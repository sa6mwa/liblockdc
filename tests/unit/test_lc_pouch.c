#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <cmocka.h>

#include "lc/lc.h"
#include "lc_pouch.h"
#include "lc_pouch_index.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"
#include "../support/lc_test_tmp.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define POUCH_UNIT_TMP_PREFIX "/tmp/liblockdc-unit-pouch-redesign-"

typedef struct pouch_value_doc {
  lonejson_int64 value;
} pouch_value_doc;

typedef struct pouch_acquire_for_update_state {
  const char *expected_snapshot;
  const char *expected_visible_during_update;
  const char *replacement;
  lc_client *observer;
  const char *key;
  int saw_snapshot;
  int saw_staged_invisible;
  int saw_staging_key_rejected;
  int fail;
} pouch_acquire_for_update_state;

typedef struct pouch_query_key_capture {
  char keys[8][128];
  char current[128];
  size_t current_len;
  size_t count;
} pouch_query_key_capture;

typedef struct pouch_query_key_counter {
  size_t count;
} pouch_query_key_counter;

static const lonejson_field pouch_value_fields[] = {
    LONEJSON_FIELD_I64(pouch_value_doc, value, "value")};

LONEJSON_MAP_DEFINE(pouch_value_map, pouch_value_doc, pouch_value_fields);

static void make_root(const char *suffix, char *root, size_t root_size) {
  char template_path[512];
  int written;

  written = snprintf(template_path, sizeof(template_path),
                     POUCH_UNIT_TMP_PREFIX "%s-XXXXXX", suffix);
  assert_true(written > 0 && (size_t)written < sizeof(template_path));
  assert_true(
      lc_test_tmp_mkdtemp(template_path, root, root_size,
                          POUCH_UNIT_TMP_PREFIX));
}

static void make_endpoint(const char *root, char *endpoint,
                          size_t endpoint_size) {
  snprintf(endpoint, endpoint_size, "pouch://%s", root);
}

static void cleanup_root(const char *root) {
  lc_test_tmp_cleanup_path(root, POUCH_UNIT_TMP_PREFIX);
}

static void cleanup_all_roots(void) {
  lc_test_tmp_cleanup_stale("/tmp", "liblockdc-unit-pouch-redesign-",
                            POUCH_UNIT_TMP_PREFIX);
}

static void test_index_docid_set_keeps_sorted_unique_docids(void **state) {
  lc_allocator allocator;
  lc_pouch_index_docid_set set;
  lc_error error;
  int added;
  int rc;

  (void)state;
  lc_allocator_init(&allocator);
  lc_error_init(&error);
  memset(&set, 0, sizeof(set));

  rc = lc_pouch_index_docid_set_append_sorted_unique(&set, 2UL, &added,
                                                     &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(added, 1);
  rc = lc_pouch_index_docid_set_append_sorted_unique(&set, 2UL, &added,
                                                     &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(added, 0);
  rc = lc_pouch_index_docid_set_append_sorted_unique(&set, 7UL, &added,
                                                     &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(added, 1);

  assert_int_equal(set.count, 2);
  assert_int_equal(set.items[0], 2UL);
  assert_int_equal(set.items[1], 7UL);

  rc = lc_pouch_index_docid_set_append_sorted_unique(&set, 3UL, &added,
                                                     &allocator, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_non_null(strstr(error.message, "requires sorted input"));

  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_pouch_index_docid_set_cleanup(&allocator, &set);

  rc = lc_pouch_index_docid_set_append_unique(&set, 7UL, &added, &allocator,
                                              &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(added, 1);
  rc = lc_pouch_index_docid_set_append_unique(&set, 2UL, &added, &allocator,
                                              &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(added, 1);
  rc = lc_pouch_index_docid_set_append_unique(&set, 7UL, &added, &allocator,
                                              &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(added, 0);
  assert_int_equal(set.count, 2);
  assert_int_equal(set.items[0], 7UL);
  assert_int_equal(set.items[1], 2UL);

  lc_pouch_index_docid_set_cleanup(&allocator, &set);
  lc_error_cleanup(&error);
}

static void test_index_docid_set_merges_sorted_sets(void **state) {
  lc_allocator allocator;
  lc_pouch_index_docid_set left;
  lc_pouch_index_docid_set right;
  lc_pouch_index_docid_set out;
  lc_error error;
  int added;
  int rc;

  (void)state;
  lc_allocator_init(&allocator);
  lc_error_init(&error);
  memset(&left, 0, sizeof(left));
  memset(&right, 0, sizeof(right));
  memset(&out, 0, sizeof(out));

  rc = lc_pouch_index_docid_set_append_sorted_unique(&left, 1UL, &added,
                                                     &allocator, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_index_docid_set_append_sorted_unique(&left, 3UL, &added,
                                                     &allocator, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_index_docid_set_append_sorted_unique(&left, 9UL, &added,
                                                     &allocator, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_index_docid_set_append_sorted_unique(&right, 3UL, &added,
                                                     &allocator, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_index_docid_set_append_sorted_unique(&right, 4UL, &added,
                                                     &allocator, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_index_docid_set_append_sorted_unique(&right, 9UL, &added,
                                                     &allocator, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_index_docid_set_append_sorted_unique(&right, 12UL, &added,
                                                     &allocator, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_pouch_index_docid_set_union_sorted(&left, &right, &out, &allocator,
                                             &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(out.count, 5);
  assert_int_equal(out.items[0], 1UL);
  assert_int_equal(out.items[1], 3UL);
  assert_int_equal(out.items[2], 4UL);
  assert_int_equal(out.items[3], 9UL);
  assert_int_equal(out.items[4], 12UL);

  lc_pouch_index_docid_set_cleanup(&allocator, &out);
  rc = lc_pouch_index_docid_set_intersect_sorted(&left, &right, &out,
                                                 &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(out.count, 2);
  assert_int_equal(out.items[0], 3UL);
  assert_int_equal(out.items[1], 9UL);

  lc_pouch_index_docid_set_cleanup(&allocator, &out);
  rc = lc_pouch_index_docid_set_subtract_sorted(&left, &right, &out,
                                                &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(out.count, 1);
  assert_int_equal(out.items[0], 1UL);

  lc_pouch_index_docid_set_cleanup(&allocator, &out);
  rc = lc_pouch_index_docid_set_append_sorted_unique(&out, 99UL, &added,
                                                     &allocator, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_index_docid_set_union_sorted(&left, &right, &out, &allocator,
                                             &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_non_null(strstr(error.message, "empty output"));

  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_pouch_index_docid_set_cleanup(&allocator, &out);
  right.items[2] = 4UL;
  rc = lc_pouch_index_docid_set_intersect_sorted(&left, &right, &out,
                                                 &allocator, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_non_null(strstr(error.message, "sorted unique"));

  lc_error_cleanup(&error);
  lc_pouch_index_docid_set_cleanup(&allocator, &out);
  lc_pouch_index_docid_set_cleanup(&allocator, &left);
  lc_pouch_index_docid_set_cleanup(&allocator, &right);
}

static void test_index_doc_table_maps_sorted_keys_to_docids(void **state) {
  lc_allocator allocator;
  lc_pouch_index_doc_table table;
  const lc_pouch_index_doc *doc;
  lc_error error;
  unsigned long doc_id;
  int found;
  int rc;

  (void)state;
  lc_allocator_init(&allocator);
  lc_error_init(&error);
  memset(&table, 0, sizeof(table));
  doc = NULL;
  doc_id = 999UL;
  found = 0;

  rc = lc_pouch_index_doc_table_append_sorted_unique(
      &table, "0a", 3UL, 11UL, 1, 0, &doc_id, &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(doc_id, 0);
  rc = lc_pouch_index_doc_table_append_sorted_unique(
      &table, "0b", 4UL, 12UL, 1, 1, &doc_id, &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(doc_id, 1);
  rc = lc_pouch_index_doc_table_append_sorted_unique(
      &table, "10", 5UL, 13UL, 0, 0, &doc_id, &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(doc_id, 2);

  rc = lc_pouch_index_doc_table_find_key_hex(&table, "0b", &doc_id, &found,
                                             &error);
  assert_int_equal(rc, LC_OK);
  assert_true(found);
  assert_int_equal(doc_id, 1);
  rc = lc_pouch_index_doc_table_get(&table, doc_id, &doc, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(doc);
  assert_string_equal(doc->key_hex, "0b");
  assert_int_equal(doc->version, 4);
  assert_int_equal(doc->bytes, 12);
  assert_true(doc->has_query_hidden);
  assert_true(doc->query_hidden);

  rc = lc_pouch_index_doc_table_find_key_hex(&table, "0c", &doc_id, &found,
                                             &error);
  assert_int_equal(rc, LC_OK);
  assert_false(found);

  rc = lc_pouch_index_doc_table_append_sorted_unique(
      &table, "10", 6UL, 14UL, 0, 0, &doc_id, &allocator, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_non_null(strstr(error.message, "duplicate"));
  lc_error_cleanup(&error);
  lc_error_init(&error);

  rc = lc_pouch_index_doc_table_append_sorted_unique(
      &table, "09", 6UL, 14UL, 0, 0, &doc_id, &allocator, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_non_null(strstr(error.message, "sorted"));
  lc_error_cleanup(&error);
  lc_error_init(&error);

  rc = lc_pouch_index_doc_table_get(&table, 99UL, &doc, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_non_null(strstr(error.message, "out of range"));

  lc_error_cleanup(&error);
  lc_pouch_index_doc_table_cleanup(&allocator, &table);
}

static void test_index_result_key_list_sorts_and_compacts_docids(
    void **state) {
  lc_allocator allocator;
  lc_pouch_index_result_key_list list;
  lc_error error;
  int rc;

  (void)state;
  lc_allocator_init(&allocator);
  lc_error_init(&error);
  memset(&list, 0, sizeof(list));

  rc = lc_pouch_index_result_key_list_add(&allocator, &list, "0c", 3UL, 30UL,
                                          300UL, 1, 1, 2U, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_index_result_key_list_add(&allocator, &list, "0a", 1UL, 10UL,
                                          100UL, 0, 0, 1U, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_index_result_key_list_add(&allocator, &list, "0b", 2UL, 20UL,
                                          200UL, 1, 0, 0U, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_index_result_key_list_add(&allocator, &list, "0b", 2UL, 21UL,
                                          201UL, 0, 1, 3U, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_index_result_key_list_add(&allocator, &list, "0d", 3UL, 31UL,
                                          301UL, 0, 0, 1U, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_pouch_index_result_key_list_sort_compact_docids(&allocator, &list,
                                                          &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 3);
  assert_int_equal(list.items[0].doc_id, 1);
  assert_string_equal(list.items[0].key_hex, "0a");
  assert_int_equal(list.items[0].version, 10);
  assert_int_equal(list.items[0].bytes, 100);
  assert_false(list.items[0].has_query_hidden);
  assert_false(list.items[0].query_hidden);
  assert_int_equal(list.items[0].value_index, 1);
  assert_int_equal(list.items[1].doc_id, 2);
  assert_string_equal(list.items[1].key_hex, "0b");
  assert_int_equal(list.items[1].version, 20);
  assert_int_equal(list.items[1].bytes, 200);
  assert_true(list.items[1].has_query_hidden);
  assert_false(list.items[1].query_hidden);
  assert_int_equal(list.items[1].value_index, 0);
  assert_int_equal(list.items[2].doc_id, 3);
  assert_string_equal(list.items[2].key_hex, "0c");
  assert_int_equal(list.items[2].value_index, 2);

  lc_pouch_index_result_key_list_cleanup(&allocator, &list);
  lc_error_cleanup(&error);
}

static void test_index_result_row_list_copies_keys_and_metadata(void **state) {
  lc_allocator allocator;
  lc_pouch_index_result_row_list list;
  lc_error error;
  char key[16];
  char key_hex[16];
  int rc;

  (void)state;
  lc_allocator_init(&allocator);
  lc_error_init(&error);
  memset(&list, 0, sizeof(list));
  strcpy(key, "alpha");
  strcpy(key_hex, "616c706861");

  rc = lc_pouch_index_result_row_list_add(&allocator, &list, key, key_hex, 7UL,
                                          11UL, 13UL, 1, 0, 2U, &error);
  assert_int_equal(rc, LC_OK);
  strcpy(key, "mutated");
  strcpy(key_hex, "6d757461746564");

  assert_int_equal(list.count, 1);
  assert_string_equal(list.items[0].key, "alpha");
  assert_string_equal(list.items[0].key_hex, "616c706861");
  assert_int_equal(list.items[0].doc_id, 7);
  assert_int_equal(list.items[0].version, 11);
  assert_int_equal(list.items[0].bytes, 13);
  assert_true(list.items[0].has_query_hidden);
  assert_false(list.items[0].query_hidden);
  assert_int_equal(list.items[0].value_index, 2);

  lc_pouch_index_result_row_list_cleanup(&allocator, &list);
  assert_null(list.items);
  assert_int_equal(list.count, 0);
  assert_int_equal(list.capacity, 0);
  lc_error_cleanup(&error);
}

static void test_index_term_fields_find_sorted_ranges(void **state) {
  lc_allocator allocator;
  lc_pouch_index_term_field *fields;
  unsigned long first;
  unsigned long count;
  int found;

  (void)state;
  lc_allocator_init(&allocator);
  fields = (lc_pouch_index_term_field *)lc_alloc_with_allocator(
      &allocator, 3U * sizeof(*fields));
  assert_non_null(fields);
  memset(fields, 0, 3U * sizeof(*fields));
  fields[0].field_hex = lc_strdup_with_allocator(&allocator, "616765");
  fields[0].first_line = 0UL;
  fields[0].line_count = 2UL;
  fields[1].field_hex = lc_strdup_with_allocator(&allocator, "6e616d65");
  fields[1].first_line = 2UL;
  fields[1].line_count = 4UL;
  fields[2].field_hex = lc_strdup_with_allocator(&allocator, "74616773");
  fields[2].first_line = 6UL;
  fields[2].line_count = 3UL;
  assert_non_null(fields[0].field_hex);
  assert_non_null(fields[1].field_hex);
  assert_non_null(fields[2].field_hex);

  first = 99UL;
  count = 88UL;
  found = lc_pouch_index_term_fields_find(fields, 3U, "6e616d65", &first,
                                          &count);
  assert_true(found);
  assert_int_equal(first, 2);
  assert_int_equal(count, 4);

  found = lc_pouch_index_term_fields_find(fields, 3U, "6f776e6572", &first,
                                          &count);
  assert_false(found);
  assert_int_equal(first, 2);
  assert_int_equal(count, 4);

  lc_pouch_index_term_fields_cleanup(&allocator, fields, 3U);
}

static void test_index_term_values_find_sorted_ranges(void **state) {
  lc_allocator allocator;
  lc_pouch_index_term_value *values;
  unsigned long first;
  unsigned long count;
  int found;

  (void)state;
  lc_allocator_init(&allocator);
  values = (lc_pouch_index_term_value *)lc_alloc_with_allocator(
      &allocator, 4U * sizeof(*values));
  assert_non_null(values);
  memset(values, 0, 4U * sizeof(*values));
  values[0].field_hex = lc_strdup_with_allocator(&allocator, "616765");
  values[0].value_hex = lc_strdup_with_allocator(&allocator, "3330");
  values[0].first_line = 0UL;
  values[0].line_count = 1UL;
  values[1].field_hex = lc_strdup_with_allocator(&allocator, "6e616d65");
  values[1].value_hex = lc_strdup_with_allocator(&allocator, "616c696365");
  values[1].first_line = 1UL;
  values[1].line_count = 2UL;
  values[2].field_hex = lc_strdup_with_allocator(&allocator, "6e616d65");
  values[2].value_hex = lc_strdup_with_allocator(&allocator, "626f62");
  values[2].first_line = 3UL;
  values[2].line_count = 1UL;
  values[3].field_hex = lc_strdup_with_allocator(&allocator, "74616773");
  values[3].value_hex = lc_strdup_with_allocator(&allocator, "706c616e");
  values[3].first_line = 4UL;
  values[3].line_count = 5UL;
  assert_non_null(values[0].field_hex);
  assert_non_null(values[0].value_hex);
  assert_non_null(values[1].field_hex);
  assert_non_null(values[1].value_hex);
  assert_non_null(values[2].field_hex);
  assert_non_null(values[2].value_hex);
  assert_non_null(values[3].field_hex);
  assert_non_null(values[3].value_hex);

  first = 99UL;
  count = 88UL;
  found = lc_pouch_index_term_values_find(
      values, 4U, "6e616d65", "626f62", &first, &count);
  assert_true(found);
  assert_int_equal(first, 3);
  assert_int_equal(count, 1);

  found = lc_pouch_index_term_values_find(
      values, 4U, "6e616d65", "6361726f6c", &first, &count);
  assert_false(found);
  assert_int_equal(first, 3);
  assert_int_equal(count, 1);

  lc_pouch_index_term_values_cleanup(&allocator, values, 4U);
}

static void test_index_term_parses_sidecar_records(void **state) {
  lc_allocator allocator;
  lc_error error;
  lc_pouch_index_term_field field;
  lc_pouch_index_term_value value;
  char field_line[] = "term_field 616765 2 4 11 77\n";
  char value_line[] = "term_value 74616773 706c616e 6 8 101 303\n";
  int rc;

  (void)state;
  lc_allocator_init(&allocator);
  lc_error_init(&error);
  memset(&field, 0, sizeof(field));
  memset(&value, 0, sizeof(value));

  rc = lc_pouch_index_term_field_parse_line(field_line, &field, &allocator,
                                            &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(field.field_hex, "616765");
  assert_int_equal(field.first_line, 2);
  assert_int_equal(field.line_count, 4);
  assert_int_equal(field.first_byte, 11);
  assert_int_equal(field.byte_count, 77);

  rc = lc_pouch_index_term_value_parse_line(value_line, &value, &allocator,
                                            &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(value.field_hex, "74616773");
  assert_string_equal(value.value_hex, "706c616e");
  assert_int_equal(value.first_line, 6);
  assert_int_equal(value.line_count, 8);
  assert_int_equal(value.first_byte, 101);
  assert_int_equal(value.byte_count, 303);

  lc_free_with_allocator(&allocator, field.field_hex);
  lc_free_with_allocator(&allocator, value.field_hex);
  lc_free_with_allocator(&allocator, value.value_hex);
  lc_error_cleanup(&error);
}

static void test_index_term_rejects_invalid_sidecar_records(void **state) {
  lc_allocator allocator;
  lc_error error;
  lc_pouch_index_term_field field;
  lc_pouch_index_term_value value;
  char bad_field_prefix[] = "field 616765 0 1 0 9\n";
  char bad_field_hex[] = "term_field 61676x 0 1 0 9\n";
  char bad_value_number[] = "term_value 74616773 706c616e nope 1 0 9\n";
  char bad_value_byte[] = "term_value 74616773 706c616e 0 1 0 nope\n";
  int rc;

  (void)state;
  lc_allocator_init(&allocator);
  lc_error_init(&error);
  memset(&field, 0, sizeof(field));
  memset(&value, 0, sizeof(value));

  rc = lc_pouch_index_term_field_parse_line(bad_field_prefix, &field,
                                            &allocator, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  rc = lc_pouch_index_term_field_parse_line(bad_field_hex, &field, &allocator,
                                            &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  rc = lc_pouch_index_term_value_parse_line(bad_value_number, &value,
                                            &allocator, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  rc = lc_pouch_index_term_value_parse_line(bad_value_byte, &value,
                                            &allocator, &error);
  assert_int_equal(rc, LC_ERR_INVALID);

  lc_free_with_allocator(&allocator, field.field_hex);
  lc_free_with_allocator(&allocator, value.field_hex);
  lc_free_with_allocator(&allocator, value.value_hex);
  lc_error_cleanup(&error);
}

static void test_index_term_keys_sort_find_and_cleanup(void **state) {
  lc_allocator allocator;
  lc_pouch_index_term_key *terms;
  size_t found_index;
  int found;

  (void)state;
  lc_allocator_init(&allocator);
  terms = (lc_pouch_index_term_key *)lc_alloc_with_allocator(
      &allocator, 4U * sizeof(*terms));
  assert_non_null(terms);
  memset(terms, 0, 4U * sizeof(*terms));
  terms[0].field_hex = lc_strdup_with_allocator(&allocator, "74616773");
  terms[0].value_hex = lc_strdup_with_allocator(&allocator, "706c616e");
  terms[0].value_type = 's';
  terms[1].field_hex = lc_strdup_with_allocator(&allocator, "616765");
  terms[1].value_hex = lc_strdup_with_allocator(&allocator, "3330");
  terms[1].value_type = 'n';
  terms[2].field_hex = lc_strdup_with_allocator(&allocator, "6e616d65");
  terms[2].value_hex = lc_strdup_with_allocator(&allocator, "626f62");
  terms[2].value_type = 's';
  terms[3].field_hex = lc_strdup_with_allocator(&allocator, "6e616d65");
  terms[3].value_hex = lc_strdup_with_allocator(&allocator, "626f62");
  terms[3].value_type = 'b';
  assert_non_null(terms[0].field_hex);
  assert_non_null(terms[0].value_hex);
  assert_non_null(terms[1].field_hex);
  assert_non_null(terms[1].value_hex);
  assert_non_null(terms[2].field_hex);
  assert_non_null(terms[2].value_hex);
  assert_non_null(terms[3].field_hex);
  assert_non_null(terms[3].value_hex);

  qsort(terms, 4U, sizeof(terms[0]), lc_pouch_index_term_key_compare);
  assert_string_equal(terms[0].field_hex, "616765");
  assert_string_equal(terms[1].field_hex, "6e616d65");
  assert_int_equal(terms[1].value_type, 'b');
  assert_string_equal(terms[2].field_hex, "6e616d65");
  assert_int_equal(terms[2].value_type, 's');
  assert_string_equal(terms[3].field_hex, "74616773");

  found_index = 99U;
  found = lc_pouch_index_term_keys_find(terms, 4U, "6e616d65", "626f62",
                                        's', &found_index);
  assert_true(found);
  assert_int_equal(found_index, 2);
  found = lc_pouch_index_term_keys_find(terms, 4U, "6e616d65", "616c696365",
                                        's', &found_index);
  assert_false(found);
  found = lc_pouch_index_term_keys_find(terms, 4U, "6e616d65", "626f62",
                                        'n', &found_index);
  assert_false(found);

  lc_pouch_index_term_keys_cleanup(&allocator, terms, 4U);
}

static void test_index_term_keys_build_exact_sorts_and_deduplicates(
    void **state) {
  lc_allocator allocator;
  lc_error error;
  lc_pouch_index_plain_term raw_terms[4];
  lc_pouch_index_term_key *terms;
  size_t term_count;
  int rc;

  (void)state;
  lc_allocator_init(&allocator);
  lc_error_init(&error);
  raw_terms[0].field = "/tags";
  raw_terms[0].value = "plan";
  raw_terms[0].value_type = 's';
  raw_terms[1].field = "/age";
  raw_terms[1].value = "30";
  raw_terms[1].value_type = 'n';
  raw_terms[2].field = "/tags";
  raw_terms[2].value = "plan";
  raw_terms[2].value_type = 's';
  raw_terms[3].field = "/name";
  raw_terms[3].value = "bob";
  raw_terms[3].value_type = 's';
  terms = NULL;
  term_count = 0U;

  rc = lc_pouch_index_term_keys_build_exact(
      raw_terms, 4U, &terms, &term_count, &allocator, &error);

  assert_int_equal(rc, LC_OK);
  assert_non_null(terms);
  assert_int_equal(term_count, 3);
  assert_string_equal(terms[0].field_hex, "2f616765");
  assert_string_equal(terms[0].value_hex, "3330");
  assert_int_equal(terms[0].value_type, 'n');
  assert_string_equal(terms[1].field_hex, "2f6e616d65");
  assert_string_equal(terms[1].value_hex, "626f62");
  assert_int_equal(terms[1].value_type, 's');
  assert_string_equal(terms[2].field_hex, "2f74616773");
  assert_string_equal(terms[2].value_hex, "706c616e");
  assert_int_equal(terms[2].value_type, 's');

  lc_pouch_index_term_keys_cleanup(&allocator, terms, 4U);
  lc_error_cleanup(&error);
}

static void test_index_term_keys_build_exact_rejects_invalid_terms(
    void **state) {
  lc_allocator allocator;
  lc_error error;
  lc_pouch_index_plain_term raw_terms[2];
  lc_pouch_index_term_key *terms;
  size_t term_count;
  int rc;

  (void)state;
  lc_allocator_init(&allocator);
  lc_error_init(&error);
  raw_terms[0].field = "/name";
  raw_terms[0].value = "bob";
  raw_terms[0].value_type = 's';
  raw_terms[1].field = "";
  raw_terms[1].value = "bad";
  raw_terms[1].value_type = 's';
  terms = (lc_pouch_index_term_key *)1;
  term_count = 99U;

  rc = lc_pouch_index_term_keys_build_exact(
      raw_terms, 2U, &terms, &term_count, &allocator, &error);

  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(terms);
  assert_int_equal(term_count, 0);
  lc_error_cleanup(&error);

  lc_error_init(&error);
  raw_terms[1].field = "/tags";
  raw_terms[1].value = NULL;
  raw_terms[1].value_type = 's';
  terms = (lc_pouch_index_term_key *)1;
  term_count = 99U;
  rc = lc_pouch_index_term_keys_build_exact(
      raw_terms, 2U, &terms, &term_count, &allocator, &error);

  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(terms);
  assert_int_equal(term_count, 0);
  lc_error_cleanup(&error);
}

static void test_index_term_keys_build_exact_for_field_values(void **state) {
  lc_allocator allocator;
  lc_error error;
  const char *values[4];
  char value_types[4];
  lc_pouch_index_term_key *terms;
  size_t term_count;
  int rc;

  (void)state;
  lc_allocator_init(&allocator);
  lc_error_init(&error);
  values[0] = "plan";
  value_types[0] = 's';
  values[1] = "finance";
  value_types[1] = 's';
  values[2] = "plan";
  value_types[2] = 's';
  values[3] = "";
  value_types[3] = 's';
  terms = NULL;
  term_count = 0U;

  rc = lc_pouch_index_term_keys_build_exact_for_field(
      "/tags", values, value_types, 4U, &terms, &term_count, &allocator,
      &error);

  assert_int_equal(rc, LC_OK);
  assert_non_null(terms);
  assert_int_equal(term_count, 3);
  assert_string_equal(terms[0].field_hex, "2f74616773");
  assert_string_equal(terms[0].value_hex, "-");
  assert_string_equal(terms[1].field_hex, "2f74616773");
  assert_string_equal(terms[1].value_hex, "66696e616e6365");
  assert_string_equal(terms[2].field_hex, "2f74616773");
  assert_string_equal(terms[2].value_hex, "706c616e");

  lc_pouch_index_term_keys_cleanup(&allocator, terms, 4U);
  lc_error_cleanup(&error);
}

static void test_index_term_keys_build_exact_for_field_rejects_invalid_values(
    void **state) {
  lc_allocator allocator;
  lc_error error;
  const char *values[2];
  char value_types[2];
  lc_pouch_index_term_key *terms;
  size_t term_count;
  int rc;

  (void)state;
  lc_allocator_init(&allocator);
  lc_error_init(&error);
  values[0] = "plan";
  value_types[0] = 's';
  values[1] = NULL;
  value_types[1] = 's';
  terms = (lc_pouch_index_term_key *)1;
  term_count = 99U;

  rc = lc_pouch_index_term_keys_build_exact_for_field(
      "/tags", values, value_types, 2U, &terms, &term_count, &allocator,
      &error);

  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(terms);
  assert_int_equal(term_count, 0);
  lc_error_cleanup(&error);

  lc_error_init(&error);
  terms = (lc_pouch_index_term_key *)1;
  term_count = 99U;
  rc = lc_pouch_index_term_keys_build_exact_for_field(
      "", values, value_types, 1U, &terms, &term_count, &allocator, &error);

  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(terms);
  assert_int_equal(term_count, 0);
  lc_error_cleanup(&error);
}

static void test_index_term_values_collect_exact_ranges(void **state) {
  lc_allocator allocator;
  lc_error error;
  lc_pouch_index_term_value values[4];
  lc_pouch_index_term_key terms[4];
  lc_pouch_index_term_range *ranges;
  size_t range_count;
  int rc;

  (void)state;
  lc_allocator_init(&allocator);
  lc_error_init(&error);
  memset(values, 0, sizeof(values));
  values[0].field_hex = "616765";
  values[0].value_hex = "3330";
  values[0].first_line = 0UL;
  values[0].line_count = 1UL;
  values[0].first_byte = 0UL;
  values[0].byte_count = 17UL;
  values[1].field_hex = "6e616d65";
  values[1].value_hex = "616c696365";
  values[1].first_line = 1UL;
  values[1].line_count = 2UL;
  values[1].first_byte = 17UL;
  values[1].byte_count = 43UL;
  values[2].field_hex = "6e616d65";
  values[2].value_hex = "626f62";
  values[2].first_line = 3UL;
  values[2].line_count = 1UL;
  values[2].first_byte = 60UL;
  values[2].byte_count = 19UL;
  values[3].field_hex = "74616773";
  values[3].value_hex = "706c616e";
  values[3].first_line = 4UL;
  values[3].line_count = 5UL;
  values[3].first_byte = 79UL;
  values[3].byte_count = 91UL;
  terms[0].field_hex = "616765";
  terms[0].value_hex = "3330";
  terms[0].value_type = 'n';
  terms[1].field_hex = "6e616d65";
  terms[1].value_hex = "626f62";
  terms[1].value_type = 's';
  terms[2].field_hex = "74616773";
  terms[2].value_hex = "706c616e";
  terms[2].value_type = 'b';
  terms[3].field_hex = "74616773";
  terms[3].value_hex = "706c616e";
  terms[3].value_type = 's';
  ranges = NULL;
  range_count = 99U;

  rc = lc_pouch_index_term_values_collect_ranges(
      values, 4U, terms, 4U, &ranges, &range_count, &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(range_count, 3);
  assert_non_null(ranges);
  assert_int_equal(ranges[0].first_line, 0);
  assert_int_equal(ranges[0].line_count, 1);
  assert_int_equal(ranges[0].first_byte, 0);
  assert_int_equal(ranges[0].byte_count, 17);
  assert_int_equal(ranges[1].first_line, 3);
  assert_int_equal(ranges[1].line_count, 1);
  assert_int_equal(ranges[1].first_byte, 60);
  assert_int_equal(ranges[1].byte_count, 19);
  assert_int_equal(ranges[2].first_line, 4);
  assert_int_equal(ranges[2].line_count, 5);
  assert_int_equal(ranges[2].first_byte, 79);
  assert_int_equal(ranges[2].byte_count, 91);

  lc_pouch_index_term_ranges_cleanup(&allocator, ranges);
  lc_error_cleanup(&error);
}

static void test_index_term_fields_select_merged_range(void **state) {
  lc_pouch_index_term_field fields[3];
  lc_pouch_index_term_key terms[2];
  unsigned long first;
  unsigned long count;
  unsigned long first_byte;
  unsigned long byte_count;
  int found;

  (void)state;
  memset(fields, 0, sizeof(fields));
  fields[0].field_hex = "616765";
  fields[0].first_line = 0UL;
  fields[0].line_count = 2UL;
  fields[0].first_byte = 0UL;
  fields[0].byte_count = 31UL;
  fields[1].field_hex = "6e616d65";
  fields[1].first_line = 2UL;
  fields[1].line_count = 4UL;
  fields[1].first_byte = 31UL;
  fields[1].byte_count = 88UL;
  fields[2].field_hex = "74616773";
  fields[2].first_line = 6UL;
  fields[2].line_count = 3UL;
  fields[2].first_byte = 119UL;
  fields[2].byte_count = 52UL;
  terms[0].field_hex = "74616773";
  terms[0].value_hex = "706c616e";
  terms[1].field_hex = "616765";
  terms[1].value_hex = "3330";
  first = 99UL;
  count = 88UL;
  first_byte = 77UL;
  byte_count = 66UL;

  found = lc_pouch_index_term_fields_select_range(
      fields, 3U, NULL, terms, 2U, 9UL, 171UL, &first, &count, &first_byte,
      &byte_count);
  assert_true(found);
  assert_int_equal(first, 0);
  assert_int_equal(count, 9);
  assert_int_equal(first_byte, 0);
  assert_int_equal(byte_count, 171);

  found = lc_pouch_index_term_fields_select_range(
      fields, 3U, "6e616d65", NULL, 0U, 9UL, 171UL, &first, &count,
      &first_byte, &byte_count);
  assert_true(found);
  assert_int_equal(first, 2);
  assert_int_equal(count, 4);
  assert_int_equal(first_byte, 31);
  assert_int_equal(byte_count, 88);
}

static void test_index_posting_roundtrips_sparse_docids(void **state) {
  lc_allocator allocator;
  lc_pouch_index_posting posting;
  lc_pouch_index_docid_set decoded;
  lc_error error;
  size_t original_length;
  int added;
  int rc;

  (void)state;
  lc_allocator_init(&allocator);
  lc_error_init(&error);
  memset(&posting, 0, sizeof(posting));
  memset(&decoded, 0, sizeof(decoded));

  rc = lc_pouch_index_posting_append_sorted_unique(&posting, 2UL, &added,
                                                   &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(added, 1);
  rc = lc_pouch_index_posting_append_sorted_unique(&posting, 7UL, &added,
                                                   &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(added, 1);
  rc = lc_pouch_index_posting_append_sorted_unique(&posting, 7UL, &added,
                                                   &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(added, 0);
  rc = lc_pouch_index_posting_append_sorted_unique(&posting, 130UL, &added,
                                                   &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(added, 1);
  assert_int_equal(posting.count, 3);
  assert_true(posting.length > 0U);

  rc = lc_pouch_index_posting_append_to_set(&posting, &decoded, &allocator,
                                            &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(decoded.count, 3);
  assert_int_equal(decoded.items[0], 2UL);
  assert_int_equal(decoded.items[1], 7UL);
  assert_int_equal(decoded.items[2], 130UL);

  rc = lc_pouch_index_posting_append_sorted_unique(&posting, 129UL, &added,
                                                   &allocator, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_non_null(strstr(error.message, "requires sorted input"));

  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_pouch_index_docid_set_cleanup(&allocator, &decoded);
  original_length = posting.length;
  assert_true(original_length > 0U);
  --posting.length;
  rc = lc_pouch_index_posting_append_to_set(&posting, &decoded, &allocator,
                                            &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_non_null(strstr(error.message, "truncated"));
  posting.length = original_length;

  lc_pouch_index_docid_set_cleanup(&allocator, &decoded);
  lc_pouch_index_posting_cleanup(&allocator, &posting);
  lc_error_cleanup(&error);
}

static void test_index_posting_roundtrips_dense_docids(void **state) {
  lc_allocator allocator;
  lc_pouch_index_dense_posting posting;
  lc_pouch_index_docid_set decoded;
  lc_error error;
  int added;
  int rc;

  (void)state;
  lc_allocator_init(&allocator);
  lc_error_init(&error);
  memset(&posting, 0, sizeof(posting));
  memset(&decoded, 0, sizeof(decoded));

  posting.length = 1U;
  rc = lc_pouch_index_dense_posting_append_to_set(&posting, &decoded,
                                                  &allocator, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_non_null(strstr(error.message, "missing bit storage"));
  lc_error_cleanup(&error);
  lc_error_init(&error);
  posting.length = 0U;

  rc = lc_pouch_index_dense_posting_append_sorted_unique(
      &posting, 1UL, &added, &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(added, 1);
  rc = lc_pouch_index_dense_posting_append_sorted_unique(
      &posting, 2UL, &added, &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(added, 1);
  rc = lc_pouch_index_dense_posting_append_sorted_unique(
      &posting, 2UL, &added, &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(added, 0);
  rc = lc_pouch_index_dense_posting_append_sorted_unique(
      &posting, 3UL, &added, &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(added, 1);
  rc = lc_pouch_index_dense_posting_append_sorted_unique(
      &posting, 10UL, &added, &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(added, 1);
  assert_int_equal(posting.count, 4);
  assert_int_equal(posting.length, 2);

  rc = lc_pouch_index_dense_posting_append_to_set(&posting, &decoded,
                                                  &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(decoded.count, 4);
  assert_int_equal(decoded.items[0], 1UL);
  assert_int_equal(decoded.items[1], 2UL);
  assert_int_equal(decoded.items[2], 3UL);
  assert_int_equal(decoded.items[3], 10UL);

  rc = lc_pouch_index_dense_posting_append_sorted_unique(
      &posting, 9UL, &added, &allocator, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_non_null(strstr(error.message, "requires sorted input"));

  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_pouch_index_docid_set_cleanup(&allocator, &decoded);
  posting.bits[1] = (unsigned char)(posting.bits[1] | (1U << 7U));
  rc = lc_pouch_index_dense_posting_append_to_set(&posting, &decoded,
                                                  &allocator, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_non_null(strstr(error.message, "exceeds max docID"));

  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_pouch_index_docid_set_cleanup(&allocator, &decoded);
  posting.bits[1] = (unsigned char)(posting.bits[1] & ~(1U << 7U));
  ++posting.count;
  rc = lc_pouch_index_dense_posting_append_to_set(&posting, &decoded,
                                                  &allocator, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_non_null(strstr(error.message, "count mismatch"));

  lc_pouch_index_docid_set_cleanup(&allocator, &decoded);
  lc_pouch_index_dense_posting_cleanup(&allocator, &posting);
  lc_error_cleanup(&error);
}

static void test_index_posting_selects_adaptive_encoding(void **state) {
  lc_allocator allocator;
  lc_pouch_index_adaptive_posting sparse;
  lc_pouch_index_adaptive_posting dense;
  lc_pouch_index_docid_set decoded;
  lc_error error;
  int added;
  int rc;

  (void)state;
  lc_allocator_init(&allocator);
  lc_error_init(&error);
  memset(&sparse, 0, sizeof(sparse));
  memset(&dense, 0, sizeof(dense));
  memset(&decoded, 0, sizeof(decoded));

  rc = lc_pouch_index_adaptive_posting_append_sorted_unique(
      &sparse, 2UL, &added, &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(added, 1);
  rc = lc_pouch_index_adaptive_posting_append_sorted_unique(
      &sparse, 130000UL, &added, &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(added, 1);
  assert_int_equal(lc_pouch_index_adaptive_posting_selected_kind(&sparse),
                   LC_POUCH_INDEX_ADAPTIVE_POSTING_SPARSE);
  rc = lc_pouch_index_adaptive_posting_append_to_set(&sparse, &decoded,
                                                     &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(decoded.count, 2);
  assert_int_equal(decoded.items[0], 2UL);
  assert_int_equal(decoded.items[1], 130000UL);

  lc_pouch_index_docid_set_cleanup(&allocator, &decoded);
  rc = lc_pouch_index_adaptive_posting_append_sorted_unique(
      &dense, 1UL, &added, &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(added, 1);
  rc = lc_pouch_index_adaptive_posting_append_sorted_unique(
      &dense, 2UL, &added, &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(added, 1);
  rc = lc_pouch_index_adaptive_posting_append_sorted_unique(
      &dense, 2UL, &added, &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(added, 0);
  rc = lc_pouch_index_adaptive_posting_append_sorted_unique(
      &dense, 3UL, &added, &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(added, 1);
  rc = lc_pouch_index_adaptive_posting_append_sorted_unique(
      &dense, 4UL, &added, &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(added, 1);
  rc = lc_pouch_index_adaptive_posting_append_sorted_unique(
      &dense, 5UL, &added, &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(added, 1);
  assert_int_equal(lc_pouch_index_adaptive_posting_selected_kind(&dense),
                   LC_POUCH_INDEX_ADAPTIVE_POSTING_DENSE);
  rc = lc_pouch_index_adaptive_posting_append_to_set(&dense, &decoded,
                                                     &allocator, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(decoded.count, 5);
  assert_int_equal(decoded.items[0], 1UL);
  assert_int_equal(decoded.items[1], 2UL);
  assert_int_equal(decoded.items[2], 3UL);
  assert_int_equal(decoded.items[3], 4UL);
  assert_int_equal(decoded.items[4], 5UL);

  rc = lc_pouch_index_adaptive_posting_append_sorted_unique(
      &dense, 4UL, &added, &allocator, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_non_null(strstr(error.message, "requires sorted input"));

  lc_pouch_index_docid_set_cleanup(&allocator, &decoded);
  lc_pouch_index_adaptive_posting_cleanup(&allocator, &sparse);
  lc_pouch_index_adaptive_posting_cleanup(&allocator, &dense);
  lc_error_cleanup(&error);
}

static void pouch_write_json_state(lc_pouch *pouch, const char *namespace_name,
                                   const char *key, const char *json,
                                   const lc_pouch_state_write_options *options,
                                   lc_error *error) {
  lc_source *source;
  lc_pouch_state_write_result write_result;
  int rc;

  source = NULL;
  memset(&write_result, 0, sizeof(write_result));
  rc = lc_source_from_memory(json, strlen(json), &source, error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, namespace_name, key, source, options,
                            &write_result, error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
}

static int pouch_query_key_begin(void *context, lc_error *error) {
  pouch_query_key_capture *capture;

  (void)error;
  capture = (pouch_query_key_capture *)context;
  capture->current_len = 0U;
  capture->current[0] = '\0';
  return 1;
}

static int pouch_query_key_chunk(void *context, const char *bytes, size_t len,
                                 lc_error *error) {
  pouch_query_key_capture *capture;

  (void)error;
  capture = (pouch_query_key_capture *)context;
  if (capture->current_len + len >= sizeof(capture->current)) {
    return 0;
  }
  memcpy(capture->current + capture->current_len, bytes, len);
  capture->current_len += len;
  capture->current[capture->current_len] = '\0';
  return 1;
}

static int pouch_query_key_end(void *context, lc_error *error) {
  pouch_query_key_capture *capture;

  (void)error;
  capture = (pouch_query_key_capture *)context;
  if (capture->count >= sizeof(capture->keys) / sizeof(capture->keys[0])) {
    return 0;
  }
  snprintf(capture->keys[capture->count], sizeof(capture->keys[capture->count]),
           "%s", capture->current);
  ++capture->count;
  return 1;
}

static int pouch_query_capture_has(const pouch_query_key_capture *capture,
                                   const char *key) {
  size_t i;

  for (i = 0U; i < capture->count; ++i) {
    if (strcmp(capture->keys[i], key) == 0) {
      return 1;
    }
  }
  return 0;
}

static int string_list_has(const lc_string_list *list, const char *value) {
  size_t i;

  for (i = 0U; i < list->count; ++i) {
    if (strcmp(list->items[i], value) == 0) {
      return 1;
    }
  }
  return 0;
}

static int pouch_query_count_begin(void *context, lc_error *error) {
  (void)context;
  (void)error;
  return 1;
}

static int pouch_query_count_chunk(void *context, const char *bytes,
                                   size_t len, lc_error *error) {
  (void)context;
  (void)bytes;
  (void)len;
  (void)error;
  return 1;
}

static int pouch_query_count_end(void *context, lc_error *error) {
  pouch_query_key_counter *counter;

  (void)error;
  counter = (pouch_query_key_counter *)context;
  ++counter->count;
  return 1;
}

static int bytes_contain_text(const void *bytes, size_t length,
                              const char *needle) {
  const unsigned char *haystack;
  size_t needle_len;
  size_t offset;

  if (bytes == NULL || needle == NULL) {
    return 0;
  }
  needle_len = strlen(needle);
  if (needle_len == 0U || needle_len > length) {
    return 0;
  }
  haystack = (const unsigned char *)bytes;
  for (offset = 0U; offset + needle_len <= length; ++offset) {
    if (memcmp(haystack + offset, needle, needle_len) == 0) {
      return 1;
    }
  }
  return 0;
}

static int setup_pouch_unit_group(void **state) {
  (void)state;
  cleanup_all_roots();
  return 0;
}

static int teardown_pouch_unit_group(void **state) {
  (void)state;
  cleanup_all_roots();
  return 0;
}

static int path_is_dir(const char *path) {
  struct stat st;

  return stat(path, &st) == 0 && S_ISDIR(st.st_mode);
}

static int path_is_file(const char *path) {
  struct stat st;

  return stat(path, &st) == 0 && S_ISREG(st.st_mode);
}

static void assert_path_dir(const char *root, const char *leaf) {
  char path[1024];

  snprintf(path, sizeof(path), "%s/%s", root, leaf);
  assert_true(path_is_dir(path));
}

static void assert_path_file(const char *root, const char *leaf) {
  char path[1024];

  snprintf(path, sizeof(path), "%s/%s", root, leaf);
  assert_true(path_is_file(path));
}

static void write_text_file(const char *path, const char *text) {
  FILE *fp;

  fp = fopen(path, "wb");
  assert_non_null(fp);
  assert_int_equal(fputs(text, fp) < 0 ? -1 : 0, 0);
  assert_int_equal(fclose(fp), 0);
}

static void append_text_file(const char *path, const char *text) {
  FILE *fp;

  fp = fopen(path, "ab");
  assert_non_null(fp);
  assert_int_equal(fputs(text, fp) < 0 ? -1 : 0, 0);
  assert_int_equal(fclose(fp), 0);
}

static void replace_text_file_first(const char *path, const char *needle,
                                    const char *replacement) {
  FILE *fp;
  char *bytes;
  char *match;
  long length;
  size_t needle_len;
  size_t replacement_len;

  fp = fopen(path, "rb");
  assert_non_null(fp);
  assert_int_equal(fseek(fp, 0L, SEEK_END), 0);
  length = ftell(fp);
  assert_true(length >= 0L);
  assert_int_equal(fseek(fp, 0L, SEEK_SET), 0);
  bytes = (char *)malloc((size_t)length + 1U);
  assert_non_null(bytes);
  assert_int_equal(fread(bytes, 1U, (size_t)length, fp), (size_t)length);
  assert_int_equal(fclose(fp), 0);
  bytes[length] = '\0';

  match = strstr(bytes, needle);
  assert_non_null(match);
  needle_len = strlen(needle);
  replacement_len = strlen(replacement);
  fp = fopen(path, "wb");
  assert_non_null(fp);
  assert_int_equal(fwrite(bytes, 1U, (size_t)(match - bytes), fp),
                   (size_t)(match - bytes));
  assert_int_equal(fwrite(replacement, 1U, replacement_len, fp),
                   replacement_len);
  assert_int_equal(
      fwrite(match + needle_len, 1U,
             (size_t)length - (size_t)(match - bytes) - needle_len, fp),
      (size_t)length - (size_t)(match - bytes) - needle_len);
  assert_int_equal(fclose(fp), 0);
  free(bytes);
}

typedef struct pouch_compaction_drift_hook_state {
  lc_pouch *peer;
  const char *segment_path;
  const char *needle;
  const char *replacement;
  int called;
} pouch_compaction_drift_hook_state;

static int pouch_compaction_drift_hook(void *context, lc_error *error) {
  pouch_compaction_drift_hook_state *state;
  lc_source *body;
  lc_pouch_state_write_result write_result;
  int rc;

  state = (pouch_compaction_drift_hook_state *)context;
  state->called = 1;
  if (state->segment_path != NULL) {
    assert_non_null(state->needle);
    assert_non_null(state->replacement);
    assert_int_equal(strlen(state->needle), strlen(state->replacement));
    replace_text_file_first(state->segment_path, state->needle,
                            state->replacement);
    return LC_OK;
  }
  body = NULL;
  memset(&write_result, 0, sizeof(write_result));
  rc = lc_source_from_memory("peer", strlen("peer"), &body, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_write(state->peer, "team/alpha", "state/peer-drift",
                              body, NULL, &write_result, error);
  }
  if (body != NULL) {
    body->close(body);
  }
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  return rc;
}

static void hex_encode_string(const char *value, char *out, size_t out_size) {
  static const char hex[] = "0123456789abcdef";
  const unsigned char *src;
  size_t offset;

  src = (const unsigned char *)value;
  offset = 0U;
  while (*src != '\0') {
    assert_true(offset + 2U < out_size);
    out[offset++] = hex[*src >> 4];
    out[offset++] = hex[*src & 0x0fU];
    ++src;
  }
  assert_true(offset < out_size);
  out[offset] = '\0';
}

static void assert_file_contains(const char *path, const char *needle) {
  FILE *fp;
  char *bytes;
  long length;
  size_t nread;

  fp = fopen(path, "rb");
  assert_non_null(fp);
  assert_int_equal(fseek(fp, 0L, SEEK_END), 0);
  length = ftell(fp);
  assert_true(length >= 0L);
  assert_int_equal(fseek(fp, 0L, SEEK_SET), 0);
  bytes = (char *)malloc((size_t)length + 1U);
  assert_non_null(bytes);
  nread = fread(bytes, 1U, (size_t)length, fp);
  assert_int_equal(nread, (size_t)length);
  assert_int_equal(fclose(fp), 0);
  bytes[nread] = '\0';
  if (strstr(bytes, needle) == NULL) {
    free(bytes);
    assert_non_null(NULL);
  }
  free(bytes);
}

static void assert_file_not_contains(const char *path, const char *needle) {
  FILE *fp;
  char *bytes;
  long length;
  size_t nread;

  fp = fopen(path, "rb");
  assert_non_null(fp);
  assert_int_equal(fseek(fp, 0L, SEEK_END), 0);
  length = ftell(fp);
  assert_true(length >= 0L);
  assert_int_equal(fseek(fp, 0L, SEEK_SET), 0);
  bytes = (char *)malloc((size_t)length + 1U);
  assert_non_null(bytes);
  nread = fread(bytes, 1U, (size_t)length, fp);
  assert_int_equal(nread, (size_t)length);
  assert_int_equal(fclose(fp), 0);
  bytes[nread] = '\0';
  if (strstr(bytes, needle) != NULL) {
    free(bytes);
    assert_null(needle);
  }
  free(bytes);
}

static void assert_path_file_contains(const char *root, const char *leaf,
                                      const char *needle) {
  char path[1024];

  snprintf(path, sizeof(path), "%s/%s", root, leaf);
  assert_file_contains(path, needle);
}

static void assert_path_file_not_contains(const char *root, const char *leaf,
                                          const char *needle) {
  char path[1024];

  snprintf(path, sizeof(path), "%s/%s", root, leaf);
  assert_file_not_contains(path, needle);
}

static void find_single_marker_path(const char *root,
                                    const char *namespace_name, char *path,
                                    size_t path_size) {
  char *namespace_path;
  char markers_path[1024];
  DIR *dir;
  struct dirent *entry;
  int found;
  int written;

  namespace_path = lc_pouch_namespace_path(NULL, root, namespace_name);
  assert_non_null(namespace_path);
  written = snprintf(markers_path, sizeof(markers_path), "%s/markers",
                     namespace_path);
  assert_true(written > 0 && (size_t)written < sizeof(markers_path));
  dir = opendir(markers_path);
  assert_non_null(dir);
  found = 0;
  while ((entry = readdir(dir)) != NULL) {
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    assert_int_equal(found, 0);
    written = snprintf(path, path_size, "%s/%s", markers_path, entry->d_name);
    assert_true(written > 0 && (size_t)written < path_size);
    found = 1;
  }
  assert_int_equal(closedir(dir), 0);
  assert_int_equal(found, 1);
  free(namespace_path);
}

static void make_peer_marker_path(const char *root, const char *namespace_name,
                                  const char *leaf, char *path,
                                  size_t path_size) {
  char *namespace_path;
  int written;

  namespace_path = lc_pouch_namespace_path(NULL, root, namespace_name);
  assert_non_null(namespace_path);
  written = snprintf(path, path_size, "%s/markers/%s", namespace_path, leaf);
  assert_true(written > 0 && (size_t)written < path_size);
  free(namespace_path);
}

static void make_queue_notify_path(const char *root,
                                   const char *namespace_name,
                                   const char *queue, char *path,
                                   size_t path_size) {
  char *namespace_path;
  char *escaped_queue;
  int written;

  namespace_path = lc_pouch_namespace_path(NULL, root, namespace_name);
  escaped_queue = lc_pouch_path_escape_name(NULL, queue);
  assert_non_null(namespace_path);
  assert_non_null(escaped_queue);
  written = snprintf(path, path_size, "%s/queue-notify/%s.notify",
                     namespace_path, escaped_queue);
  assert_true(written > 0 && (size_t)written < path_size);
  free(escaped_queue);
  free(namespace_path);
}

static unsigned long read_marker_sequence(const char *path, size_t *size) {
  FILE *fp;
  struct stat st;
  char line[256];
  unsigned long sequence;
  int found;

  assert_int_equal(stat(path, &st), 0);
  assert_true(S_ISREG(st.st_mode));
  if (size != NULL) {
    *size = (size_t)st.st_size;
  }
  fp = fopen(path, "rb");
  assert_non_null(fp);
  sequence = 0UL;
  found = 0;
  while (fgets(line, sizeof(line), fp) != NULL) {
    unsigned long parsed;

    if (sscanf(line, "sequence=%lu", &parsed) == 1) {
      sequence = parsed;
      found = 1;
      break;
    }
  }
  assert_int_equal(fclose(fp), 0);
  assert_true(found);
  return sequence;
}

static void test_marker_snapshots_detect_peer_changes(void **state) {
  lc_pouch *pouch;
  lc_pouch_namespace_marker_snapshot empty_snapshot;
  lc_pouch_namespace_marker_snapshot self_snapshot;
  lc_pouch_namespace_marker_snapshot peer_snapshot;
  lc_pouch_namespace_marker_snapshot unchanged_snapshot;
  lc_pouch_namespace_marker_snapshot changed_snapshot;
  lc_error error;
  char root[512];
  char *namespace_path;
  char peer_a_path[1024];
  char peer_b_path[1024];
  const char *peer_a;
  const char *peer_b;
  int rc;

  (void)state;
  pouch = NULL;
  namespace_path = NULL;
  memset(&empty_snapshot, 0, sizeof(empty_snapshot));
  memset(&self_snapshot, 0, sizeof(self_snapshot));
  memset(&peer_snapshot, 0, sizeof(peer_snapshot));
  memset(&unchanged_snapshot, 0, sizeof(unchanged_snapshot));
  memset(&changed_snapshot, 0, sizeof(changed_snapshot));
  lc_error_init(&error);
  make_root("marker-snapshot", root, sizeof(root));
  cleanup_root(root);

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_ensure_namespace(pouch, "default", &error);
  assert_int_equal(rc, LC_OK);
  namespace_path = lc_pouch_namespace_path(NULL, root, "default");
  assert_non_null(namespace_path);

  rc = lc_pouch_namespace_marker_snapshot_read(NULL, namespace_path, NULL,
                                               &empty_snapshot, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(empty_snapshot.marker_count, 0UL);
  assert_string_equal(empty_snapshot.fingerprint, "");

  rc = lc_pouch_namespace_touch_marker(NULL, namespace_path,
                                       "writer-self.marker", 1UL, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_namespace_marker_snapshot_read(NULL, namespace_path,
                                               "writer-self.marker",
                                               &self_snapshot, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(self_snapshot.marker_count, 0UL);
  assert_int_equal(lc_pouch_namespace_marker_snapshot_changed(
                       &empty_snapshot, &self_snapshot),
                   0);

  make_peer_marker_path(root, "default", "writer-000000000002.marker",
                        peer_b_path, sizeof(peer_b_path));
  make_peer_marker_path(root, "default", "writer-000000000001.marker",
                        peer_a_path, sizeof(peer_a_path));
  write_text_file(peer_b_path, "writer_pid=2\nsequence=1\n");
  write_text_file(peer_a_path, "writer_pid=1\nsequence=1\npad=x\n");
  rc = lc_pouch_namespace_marker_snapshot_read(NULL, namespace_path,
                                               "writer-self.marker",
                                               &peer_snapshot, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(peer_snapshot.marker_count, 2UL);
  peer_a = strstr(peer_snapshot.fingerprint, "writer-000000000001.marker");
  peer_b = strstr(peer_snapshot.fingerprint, "writer-000000000002.marker");
  assert_non_null(peer_a);
  assert_non_null(peer_b);
  assert_true(peer_a < peer_b);
  assert_int_equal(lc_pouch_namespace_marker_snapshot_changed(
                       &self_snapshot, &peer_snapshot),
                   1);

  rc = lc_pouch_namespace_marker_snapshot_read(NULL, namespace_path,
                                               "writer-self.marker",
                                               &unchanged_snapshot, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lc_pouch_namespace_marker_snapshot_changed(
                       &peer_snapshot, &unchanged_snapshot),
                   0);

  write_text_file(peer_a_path, "writer_pid=1\nsequence=2\npad=longer\n");
  rc = lc_pouch_namespace_marker_snapshot_read(NULL, namespace_path,
                                               "writer-self.marker",
                                               &changed_snapshot, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(changed_snapshot.marker_count, 2UL);
  assert_int_equal(lc_pouch_namespace_marker_snapshot_changed(
                       &unchanged_snapshot, &changed_snapshot),
                   1);

  lc_pouch_namespace_marker_snapshot_cleanup(NULL, &changed_snapshot);
  lc_pouch_namespace_marker_snapshot_cleanup(NULL, &unchanged_snapshot);
  lc_pouch_namespace_marker_snapshot_cleanup(NULL, &peer_snapshot);
  lc_pouch_namespace_marker_snapshot_cleanup(NULL, &self_snapshot);
  lc_pouch_namespace_marker_snapshot_cleanup(NULL, &empty_snapshot);
  free(namespace_path);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_marker_snapshots_treat_same_process_handles_as_peers(
    void **state) {
  lc_pouch *first;
  lc_pouch *second;
  lc_pouch_namespace_marker_snapshot first_view;
  lc_pouch_namespace_marker_snapshot second_view;
  lc_error error;
  char root[512];
  char *namespace_path;
  int rc;

  (void)state;
  first = NULL;
  second = NULL;
  namespace_path = NULL;
  memset(&first_view, 0, sizeof(first_view));
  memset(&second_view, 0, sizeof(second_view));
  lc_error_init(&error);
  make_root("marker-same-process", root, sizeof(root));
  cleanup_root(root);

  rc = lc_pouch_open(root, NULL, NULL, &first, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_open(root, NULL, NULL, &second, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(first->writer_marker_leaf);
  assert_non_null(second->writer_marker_leaf);
  assert_true(strcmp(first->writer_marker_leaf, second->writer_marker_leaf) !=
              0);
  rc = lc_pouch_ensure_namespace(first, "default", &error);
  assert_int_equal(rc, LC_OK);
  namespace_path = lc_pouch_namespace_path(NULL, root, "default");
  assert_non_null(namespace_path);

  rc = lc_pouch_namespace_touch_marker(NULL, namespace_path,
                                       first->writer_marker_leaf, 1UL, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_namespace_touch_marker(NULL, namespace_path,
                                       second->writer_marker_leaf, 1UL, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_pouch_namespace_marker_snapshot_read(NULL, namespace_path,
                                               first->writer_marker_leaf,
                                               &first_view, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(first_view.marker_count, 1UL);
  assert_non_null(strstr(first_view.fingerprint, second->writer_marker_leaf));
  assert_null(strstr(first_view.fingerprint, first->writer_marker_leaf));

  rc = lc_pouch_namespace_marker_snapshot_read(NULL, namespace_path,
                                               second->writer_marker_leaf,
                                               &second_view, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(second_view.marker_count, 1UL);
  assert_non_null(strstr(second_view.fingerprint, first->writer_marker_leaf));
  assert_null(strstr(second_view.fingerprint, second->writer_marker_leaf));

  lc_pouch_namespace_marker_snapshot_cleanup(NULL, &second_view);
  lc_pouch_namespace_marker_snapshot_cleanup(NULL, &first_view);
  free(namespace_path);
  lc_pouch_close(second);
  lc_pouch_close(first);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_marker_refresh_uses_directory_fast_path_and_force(
    void **state) {
  lc_pouch *pouch;
  lc_pouch_namespace_marker_refresh_state refresh;
  lc_pouch_namespace_marker_directory_snapshot before_dir;
  lc_pouch_namespace_marker_directory_snapshot after_dir;
  lc_error error;
  char root[512];
  char *namespace_path;
  char peer_path[1024];
  int should_scan;
  int rc;

  (void)state;
  pouch = NULL;
  namespace_path = NULL;
  memset(&refresh, 0, sizeof(refresh));
  memset(&before_dir, 0, sizeof(before_dir));
  memset(&after_dir, 0, sizeof(after_dir));
  should_scan = 0;
  lc_error_init(&error);
  make_root("marker-refresh", root, sizeof(root));
  cleanup_root(root);

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_ensure_namespace(pouch, "default", &error);
  assert_int_equal(rc, LC_OK);
  namespace_path = lc_pouch_namespace_path(NULL, root, "default");
  assert_non_null(namespace_path);

  rc = lc_pouch_namespace_marker_refresh_should_scan(
      NULL, namespace_path, "writer-self.marker", &refresh, 2UL, &should_scan,
      &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(should_scan, 1);
  assert_int_equal(refresh.initialized, 1);

  rc = lc_pouch_namespace_marker_refresh_should_scan(
      NULL, namespace_path, "writer-self.marker", &refresh, 2UL, &should_scan,
      &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(should_scan, 0);

  rc = lc_pouch_namespace_marker_directory_snapshot_read(
      NULL, namespace_path, &before_dir, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_namespace_touch_marker(NULL, namespace_path,
                                       "writer-self.marker", 1UL, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_namespace_marker_refresh_should_scan(
      NULL, namespace_path, "writer-self.marker", &refresh, 2UL, &should_scan,
      &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(should_scan, 0);

  make_peer_marker_path(root, "default", "writer-000000000009.marker",
                        peer_path, sizeof(peer_path));
  write_text_file(peer_path, "writer_pid=9\nsequence=1\n");
  rc = lc_pouch_namespace_marker_directory_snapshot_read(
      NULL, namespace_path, &after_dir, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lc_pouch_namespace_marker_directory_snapshot_changed(
                       &before_dir, &after_dir),
                   1);
  rc = lc_pouch_namespace_marker_refresh_should_scan(
      NULL, namespace_path, "writer-self.marker", &refresh, 2UL, &should_scan,
      &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(should_scan, 1);

  rc = lc_pouch_namespace_marker_directory_snapshot_read(
      NULL, namespace_path, &before_dir, &error);
  assert_int_equal(rc, LC_OK);
  write_text_file(peer_path, "writer_pid=9\nsequence=2\npad=longer\n");
  rc = lc_pouch_namespace_marker_directory_snapshot_read(
      NULL, namespace_path, &after_dir, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lc_pouch_namespace_marker_directory_snapshot_changed(
                       &before_dir, &after_dir),
                   0);
  rc = lc_pouch_namespace_marker_refresh_should_scan(
      NULL, namespace_path, "writer-self.marker", &refresh, 2UL, &should_scan,
      &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(should_scan, 1);

  rc = lc_pouch_namespace_marker_refresh_should_scan(
      NULL, namespace_path, "writer-self.marker", &refresh, 2UL, &should_scan,
      &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(should_scan, 0);
  rc = lc_pouch_namespace_marker_refresh_should_scan(
      NULL, namespace_path, "writer-self.marker", &refresh, 2UL, &should_scan,
      &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(should_scan, 0);
  rc = lc_pouch_namespace_marker_refresh_should_scan(
      NULL, namespace_path, "writer-self.marker", &refresh, 2UL, &should_scan,
      &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(should_scan, 1);

  lc_pouch_namespace_marker_refresh_state_cleanup(NULL, &refresh);
  free(namespace_path);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void read_source_to_string(lc_source *source, char *buffer,
                                  size_t buffer_size) {
  lc_error error;
  size_t offset;

  lc_error_init(&error);
  offset = 0U;
  for (;;) {
    size_t nread;

    assert_true(offset < buffer_size);
    nread = source->read(source, buffer + offset, buffer_size - offset - 1U,
                         &error);
    if (nread == 0U) {
      assert_int_equal(error.code, LC_OK);
      break;
    }
    offset += nread;
  }
  buffer[offset] = '\0';
  lc_error_cleanup(&error);
}

static void test_single_writer_state_read_uses_projection_cache(void **state) {
  lc_pouch *pouch;
  lc_source *source;
  lc_pouch_open_options options;
  lc_pouch_state_write_result write_res;
  lc_pouch_state_read_result read_res;
  lc_error error;
  char root[512];
  char *namespace_path;
  char *segment_leaf;
  char segment_path[1024];
  char buffer[64];
  int written;
  int rc;

  (void)state;
  pouch = NULL;
  source = NULL;
  namespace_path = NULL;
  segment_leaf = NULL;
  memset(&options, 0, sizeof(options));
  memset(&write_res, 0, sizeof(write_res));
  memset(&read_res, 0, sizeof(read_res));
  lc_error_init(&error);
  make_root("single-writer-cache", root, sizeof(root));
  cleanup_root(root);

  options.single_writer = 1;
  rc = lc_pouch_open(root, NULL, &options, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("{\"value\":1}", strlen("{\"value\":1}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "default", "cache/key", source, NULL,
                            &write_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_source_close(source);
  source = NULL;

  rc = lc_pouch_state_read(pouch, "default", "cache/key", &read_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_res.found);
  read_source_to_string(read_res.body, buffer, sizeof(buffer));
  assert_string_equal(buffer, "{\"value\":1}");
  lc_pouch_state_read_result_cleanup(NULL, &read_res);

  namespace_path = lc_pouch_namespace_path(NULL, root, "default");
  segment_leaf = lc_pouch_namespace_segment_leaf(NULL, 1UL);
  assert_non_null(namespace_path);
  assert_non_null(segment_leaf);
  written = snprintf(segment_path, sizeof(segment_path), "%s/segments/%s",
                     namespace_path, segment_leaf);
  assert_true(written > 0 && (size_t)written < sizeof(segment_path));
  assert_int_equal(unlink(segment_path), 0);

  rc = lc_pouch_state_read(pouch, "default", "cache/key", &read_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_res.found);
  assert_int_equal(read_res.version, write_res.version);
  read_source_to_string(read_res.body, buffer, sizeof(buffer));
  assert_string_equal(buffer, "{\"value\":1}");

  lc_pouch_state_read_result_cleanup(NULL, &read_res);
  lc_pouch_state_write_result_cleanup(NULL, &write_res);
  free(segment_leaf);
  free(namespace_path);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void
test_shared_state_projection_cache_refreshes_peer_markers(void **state) {
  lc_pouch *writer;
  lc_pouch *reader;
  lc_source *source;
  lc_pouch_state_write_result write_res;
  lc_pouch_state_read_result read_res;
  lc_error error;
  char root[512];
  char buffer[64];
  int rc;

  (void)state;
  writer = NULL;
  reader = NULL;
  source = NULL;
  memset(&write_res, 0, sizeof(write_res));
  memset(&read_res, 0, sizeof(read_res));
  lc_error_init(&error);
  make_root("shared-cache-markers", root, sizeof(root));
  cleanup_root(root);

  rc = lc_pouch_open(root, NULL, NULL, &writer, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_open(root, NULL, NULL, &reader, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory("{\"value\":1}", strlen("{\"value\":1}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(writer, "default", "cache/key", source, NULL,
                            &write_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_source_close(source);
  source = NULL;
  lc_pouch_state_write_result_cleanup(NULL, &write_res);

  rc = lc_pouch_state_read(reader, "default", "cache/key", &read_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_res.found);
  read_source_to_string(read_res.body, buffer, sizeof(buffer));
  assert_string_equal(buffer, "{\"value\":1}");
  lc_pouch_state_read_result_cleanup(NULL, &read_res);

  rc = lc_source_from_memory("{\"value\":2}", strlen("{\"value\":2}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(writer, "default", "cache/key", source, NULL,
                            &write_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_source_close(source);
  source = NULL;
  lc_pouch_state_write_result_cleanup(NULL, &write_res);

  rc = lc_pouch_state_read(reader, "default", "cache/key", &read_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_res.found);
  read_source_to_string(read_res.body, buffer, sizeof(buffer));
  assert_string_equal(buffer, "{\"value\":2}");
  lc_pouch_state_read_result_cleanup(NULL, &read_res);

  rc = lc_source_from_memory("{\"value\":3}", strlen("{\"value\":3}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(reader, "default", "cache/key", source, NULL,
                            &write_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_source_close(source);
  source = NULL;
  lc_pouch_state_write_result_cleanup(NULL, &write_res);

  rc = lc_pouch_state_read(reader, "default", "cache/key", &read_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_res.found);
  read_source_to_string(read_res.body, buffer, sizeof(buffer));
  assert_string_equal(buffer, "{\"value\":3}");

  lc_pouch_state_read_result_cleanup(NULL, &read_res);
  if (source != NULL) {
    lc_source_close(source);
  }
  lc_pouch_close(reader);
  lc_pouch_close(writer);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void open_pouch_client(const char *root, lc_client **out,
                              lc_error *error) {
  lc_client_config config;
  const char *endpoints[1];
  char endpoint[540];
  int rc;

  make_endpoint(root, endpoint, sizeof(endpoint));
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  rc = lc_client_open(&config, out, error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(*out);
}

static void open_pouch_client_endpoint(const char *endpoint, lc_client **out,
                                       lc_error *error) {
  lc_client_config config;
  const char *endpoints[1];
  int rc;

  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  rc = lc_client_open(&config, out, error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(*out);
}

static void write_client_state(lc_client *client, const char *key,
                               const char *json,
                               const char *expected_etag,
                               long expected_version,
                               int has_expected_version,
                               lc_update_res *out, lc_error *error) {
  lc_update_req update_req;
  lc_source *source;
  int rc;

  lc_update_req_init(&update_req);
  update_req.lease.key = key;
  update_req.if_state_etag = expected_etag;
  update_req.if_version = expected_version;
  update_req.has_if_version = has_expected_version;
  rc = lc_source_from_memory(json, strlen(json), &source, error);
  assert_int_equal(rc, LC_OK);
  rc = client->update(client, &update_req, source, out, error);
  source->close(source);
  assert_int_equal(rc, LC_OK);
}

static int pouch_acquire_for_update_handler(
    void *context, lc_acquire_for_update_context *update, lc_error *error) {
  pouch_acquire_for_update_state *state;
  lc_source *source;
  char snapshot[256];
  int rc;

  state = (pouch_acquire_for_update_state *)context;
  source = NULL;
  assert_non_null(update);
  assert_non_null(update->lease);
  if (state->expected_snapshot != NULL) {
    assert_true(update->state.has_state);
    assert_non_null(update->state.reader);
    read_source_to_string(update->state.reader, snapshot, sizeof(snapshot));
    assert_non_null(strstr(snapshot, state->expected_snapshot));
    state->saw_snapshot = 1;
  } else {
    assert_false(update->state.has_state);
    assert_null(update->state.reader);
  }

  rc = lc_source_from_memory(state->replacement, strlen(state->replacement),
                             &source, error);
  assert_int_equal(rc, LC_OK);
  rc = update->lease->update(update->lease, source, NULL, error);
  source->close(source);
  assert_int_equal(rc, LC_OK);
  if (state->observer != NULL) {
    lc_sink *sink;
    lc_get_res get_res;
    lc_error observer_error;
    const void *bytes;
    size_t length;

    sink = NULL;
    bytes = NULL;
    length = 0U;
    memset(&get_res, 0, sizeof(get_res));
    lc_error_init(&observer_error);
    rc = lc_sink_to_memory(&sink, &observer_error);
    assert_int_equal(rc, LC_OK);
    rc = state->observer->get(state->observer, state->key, NULL, sink,
                              &get_res, &observer_error);
    assert_int_equal(rc, LC_OK);
    if (state->expected_visible_during_update != NULL) {
      assert_false(get_res.no_content);
      rc = lc_sink_memory_bytes(sink, &bytes, &length, &observer_error);
      assert_int_equal(rc, LC_OK);
      assert_int_equal(length, strlen(state->expected_visible_during_update));
      assert_memory_equal(bytes, state->expected_visible_during_update,
                          strlen(state->expected_visible_during_update));
    } else {
      assert_true(get_res.no_content);
    }
    sink->close(sink);
    lc_get_res_cleanup(&get_res);
    {
      char staging_key[256];
      const char *stage_id;

      stage_id = update->lease->txn_id != NULL &&
                         update->lease->txn_id[0] != '\0'
                     ? update->lease->txn_id
                     : update->lease->lease_id;
      snprintf(staging_key, sizeof(staging_key), "%s/.staging/%s",
               state->key, stage_id);
      sink = NULL;
      rc = lc_sink_to_memory(&sink, &observer_error);
      assert_int_equal(rc, LC_OK);
      rc = state->observer->get(state->observer, staging_key, NULL, sink,
                                &get_res, &observer_error);
      assert_int_equal(rc, LC_ERR_INVALID);
      assert_string_equal(observer_error.message,
                          "pouch staging keys are reserved for internal state");
      sink->close(sink);
      lc_get_res_cleanup(&get_res);
      lc_error_cleanup(&observer_error);
      lc_error_init(&observer_error);
    }
    state->saw_staged_invisible = 1;
    state->saw_staging_key_rejected = 1;
    lc_error_cleanup(&observer_error);
  }
  if (state->fail) {
    if (error != NULL) {
      error->code = LC_ERR_INVALID;
      error->message = strdup("intentional pouch acquire_for_update failure");
      assert_non_null(error->message);
    }
    return LC_ERR_INVALID;
  }
  return LC_OK;
}

static void test_open_creates_segmented_root_layout(void **state) {
  lc_pouch *pouch;
  lc_pouch_status status;
  lc_pouch_open_options options;
  lc_error error;
  char root[512];
  int rc;

  (void)state;
  memset(&options, 0, sizeof(options));
  memset(&status, 0, sizeof(status));
  lc_error_init(&error);
  make_root("root-layout", root, sizeof(root));
  cleanup_root(root);

  options.segment_target_bytes = 4096UL;
  options.background_compaction_enabled = 1;
  options.compaction_interval_seconds = 30UL;
  rc = lc_pouch_open(root, NULL, &options, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(pouch);
  assert_path_dir(root, "namespaces");
  assert_path_file(root, "manifest");

  rc = lc_pouch_status_read(pouch, &status, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(status.root_path, root);
  assert_string_equal(status.layout_name, "pouch-segmented");
  assert_int_equal(status.layout_version, 1UL);
  assert_int_equal(status.segment_target_bytes, 4096UL);
  assert_int_equal(status.background_compaction_enabled, 1);
  assert_int_equal(status.compaction_interval_seconds, 30UL);

  lc_pouch_status_cleanup(NULL, &status);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_ensure_namespace_creates_per_namespace_layout(void **state) {
  lc_pouch *pouch;
  lc_error error;
  char root[512];
  char *namespace_path;
  int rc;

  (void)state;
  lc_error_init(&error);
  make_root("namespace-layout", root, sizeof(root));
  cleanup_root(root);

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_ensure_namespace(pouch, "team/alpha", &error);
  assert_int_equal(rc, LC_OK);

  namespace_path = lc_pouch_namespace_path(NULL, root, "team/alpha");
  assert_non_null(namespace_path);
  assert_true(strstr(namespace_path, "team%2falpha") != NULL);
  assert_path_dir(namespace_path, "segments");
  assert_path_dir(namespace_path, "payloads");
  assert_path_dir(namespace_path, "snapshots");
  assert_path_dir(namespace_path, "markers");
  assert_path_dir(namespace_path, "index");
  assert_path_dir(namespace_path, "queue-notify");
  assert_path_file(namespace_path, "manifest");
  assert_path_file_contains(namespace_path, "manifest",
                            "active_segment=seg-00000000000000000001.log");

  free(namespace_path);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_pouch_endpoint_opens_new_backend_without_http_engine(
    void **state) {
  lc_client_config config;
  lc_client *client;
  lc_error error;
  const char *endpoints[1];
  char root[512];
  char endpoint[540];
  int rc;

  (void)state;
  lc_error_init(&error);
  make_root("client-open", root, sizeof(root));
  cleanup_root(root);
  snprintf(endpoint, sizeof(endpoint), "pouch://%s", root);
  endpoints[0] = endpoint;

  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;

  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(client);
  assert_path_file(root, "manifest");

  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_pouch_endpoint_query_engine_routes_implicit_queries(
    void **state) {
  static const char selector[] =
      "{\"eq\":{\"field\":\"/category\",\"value\":\"planning\"}}";
  lc_client *client;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_query_key_handler handler;
  lc_update_res update_res;
  pouch_query_key_capture capture;
  lc_error error;
  char root[512];
  char endpoint[640];
  int rc;

  (void)state;
  client = NULL;
  memset(&query_res, 0, sizeof(query_res));
  memset(&handler, 0, sizeof(handler));
  memset(&update_res, 0, sizeof(update_res));
  memset(&capture, 0, sizeof(capture));
  lc_query_req_init(&query_req);
  lc_error_init(&error);
  handler.begin = pouch_query_key_begin;
  handler.chunk = pouch_query_key_chunk;
  handler.end = pouch_query_key_end;

  make_root("client-query-engine-index", root, sizeof(root));
  cleanup_root(root);
  make_endpoint(root, endpoint, sizeof(endpoint));
  open_pouch_client_endpoint(endpoint, &client, &error);
  write_client_state(client, "doc/a", "{\"category\":\"planning\"}", NULL, 0L,
                     0, &update_res, &error);
  lc_update_res_cleanup(&update_res);

  query_req.selector_json = selector;
  rc = client->query_keys(client, &query_req, &handler, &capture, &query_res,
                          &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1);
  assert_true(pouch_query_capture_has(&capture, "doc/a"));
  assert_non_null(query_res.metadata_json);
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));
  lc_query_res_cleanup(&query_res);
  lc_client_close(client);
  client = NULL;
  cleanup_root(root);

  memset(&capture, 0, sizeof(capture));
  make_root("client-query-engine-scan", root, sizeof(root));
  cleanup_root(root);
  snprintf(endpoint, sizeof(endpoint),
           "pouch://%s?query_engine=scan&query_fallback_engine=index", root);
  open_pouch_client_endpoint(endpoint, &client, &error);
  memset(&update_res, 0, sizeof(update_res));
  write_client_state(client, "doc/a", "{\"category\":\"planning\"}", NULL, 0L,
                     0, &update_res, &error);
  lc_update_res_cleanup(&update_res);

  lc_query_req_init(&query_req);
  query_req.selector_json = selector;
  rc = client->query_keys(client, &query_req, &handler, &capture, &query_res,
                          &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1);
  assert_string_equal(query_res.metadata_json, "{\"engine\":\"scan\"}");
  lc_query_res_cleanup(&query_res);

  memset(&capture, 0, sizeof(capture));
  lc_query_req_init(&query_req);
  query_req.selector_json = selector;
  query_req.refresh = "wait_for";
  rc = client->query_keys(client, &query_req, &handler, &capture, &query_res,
                          &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1);
  assert_non_null(query_res.metadata_json);
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));
  lc_query_res_cleanup(&query_res);

  memset(&capture, 0, sizeof(capture));
  lc_query_req_init(&query_req);
  query_req.selector_json = selector;
  query_req.engine = "index";
  rc = client->query_keys(client, &query_req, &handler, &capture, &query_res,
                          &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1);
  assert_non_null(query_res.metadata_json);
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));

  lc_query_res_cleanup(&query_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_pouch_namespace_config_persists_and_routes_implicit_queries(
    void **state) {
  static const char namespace_name[] = "docs/ns-config";
  static const char selector[] =
      "{\"eq\":{\"field\":\"/category\",\"value\":\"planning\"}}";
  lc_client *client;
  lc_namespace_config_req ns_req;
  lc_namespace_config_res ns_res;
  lc_update_req update_req;
  lc_update_res update_res;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_query_key_handler handler;
  pouch_query_key_capture capture;
  lc_source *source;
  lc_error error;
  char root[512];
  int rc;

  (void)state;
  client = NULL;
  source = NULL;
  memset(&ns_res, 0, sizeof(ns_res));
  memset(&update_res, 0, sizeof(update_res));
  memset(&query_res, 0, sizeof(query_res));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  lc_namespace_config_req_init(&ns_req);
  lc_update_req_init(&update_req);
  lc_query_req_init(&query_req);
  lc_error_init(&error);
  handler.begin = pouch_query_key_begin;
  handler.chunk = pouch_query_key_chunk;
  handler.end = pouch_query_key_end;

  make_root("namespace-config", root, sizeof(root));
  cleanup_root(root);
  open_pouch_client(root, &client, &error);

  ns_req.namespace_name = namespace_name;
  rc = client->get_namespace_config(client, &ns_req, &ns_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(ns_res.namespace_name, namespace_name);
  assert_string_equal(ns_res.preferred_engine, "index");
  assert_string_equal(ns_res.fallback_engine, "none");
  lc_namespace_config_res_cleanup(&ns_res);

  ns_req.preferred_engine = "scan";
  ns_req.fallback_engine = "none";
  rc = client->update_namespace_config(client, &ns_req, &ns_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(ns_res.preferred_engine, "scan");
  assert_string_equal(ns_res.fallback_engine, "none");
  lc_namespace_config_res_cleanup(&ns_res);

  update_req.lease.namespace_name = namespace_name;
  update_req.lease.key = "doc/a";
  rc = lc_source_from_memory("{\"category\":\"planning\"}",
                             strlen("{\"category\":\"planning\"}"), &source,
                             &error);
  assert_int_equal(rc, LC_OK);
  rc = client->update(client, &update_req, source, &update_res, &error);
  assert_int_equal(rc, LC_OK);
  source->close(source);
  source = NULL;
  lc_update_res_cleanup(&update_res);

  query_req.namespace_name = namespace_name;
  query_req.selector_json = selector;
  rc = client->query_keys(client, &query_req, &handler, &capture, &query_res,
                          &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1);
  assert_true(pouch_query_capture_has(&capture, "doc/a"));
  assert_string_equal(query_res.metadata_json, "{\"engine\":\"scan\"}");
  lc_query_res_cleanup(&query_res);

  memset(&capture, 0, sizeof(capture));
  lc_query_req_init(&query_req);
  query_req.namespace_name = namespace_name;
  query_req.selector_json = selector;
  query_req.engine = "index";
  rc = client->query_keys(client, &query_req, &handler, &capture, &query_res,
                          &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1);
  assert_non_null(query_res.metadata_json);
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));
  lc_query_res_cleanup(&query_res);

  ns_req.preferred_engine = NULL;
  ns_req.fallback_engine = "scan";
  rc = client->update_namespace_config(client, &ns_req, &ns_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(ns_res.preferred_engine, "scan");
  assert_string_equal(ns_res.fallback_engine, "scan");
  lc_namespace_config_res_cleanup(&ns_res);

  lc_client_close(client);
  client = NULL;
  open_pouch_client(root, &client, &error);
  lc_namespace_config_req_init(&ns_req);
  ns_req.namespace_name = namespace_name;
  rc = client->get_namespace_config(client, &ns_req, &ns_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(ns_res.preferred_engine, "scan");
  assert_string_equal(ns_res.fallback_engine, "scan");
  lc_namespace_config_res_cleanup(&ns_res);

  ns_req.preferred_engine = "linear";
  ns_req.fallback_engine = NULL;
  rc = client->update_namespace_config(client, &ns_req, &ns_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  ns_req.preferred_engine = NULL;
  ns_req.fallback_engine = "index";
  rc = client->update_namespace_config(client, &ns_req, &ns_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);

  lc_error_cleanup(&error);
  if (source != NULL) {
    source->close(source);
  }
  lc_namespace_config_res_cleanup(&ns_res);
  lc_query_res_cleanup(&query_res);
  lc_update_res_cleanup(&update_res);
  if (client != NULL) {
    lc_client_close(client);
  }
  cleanup_root(root);
}

static void test_pouch_tc_surface_persists_local_single_node_state(
    void **state) {
  lc_client *client;
  lc_tc_lease_acquire_req acquire_req;
  lc_tc_lease_acquire_res acquire_res;
  lc_tc_lease_renew_req renew_req;
  lc_tc_lease_renew_res renew_res;
  lc_tc_lease_release_req release_req;
  lc_tc_lease_release_res release_res;
  lc_tc_leader_res leader_res;
  lc_tc_cluster_announce_req cluster_req;
  lc_tc_cluster_res cluster_res;
  lc_tc_rm_register_req rm_register_req;
  lc_tc_rm_unregister_req rm_unregister_req;
  lc_tc_rm_res rm_res;
  lc_tc_rm_list_res rm_list;
  lc_error error;
  char root[512];
  int rc;

  (void)state;
  client = NULL;
  memset(&acquire_res, 0, sizeof(acquire_res));
  memset(&renew_res, 0, sizeof(renew_res));
  memset(&release_res, 0, sizeof(release_res));
  memset(&leader_res, 0, sizeof(leader_res));
  memset(&cluster_res, 0, sizeof(cluster_res));
  memset(&rm_res, 0, sizeof(rm_res));
  memset(&rm_list, 0, sizeof(rm_list));
  lc_tc_lease_acquire_req_init(&acquire_req);
  lc_tc_lease_renew_req_init(&renew_req);
  lc_tc_lease_release_req_init(&release_req);
  lc_error_init(&error);

  make_root("tc-surface", root, sizeof(root));
  cleanup_root(root);
  open_pouch_client(root, &client, &error);

  acquire_req.candidate_id = "node-a";
  acquire_req.candidate_endpoint = "pouch://node-a";
  acquire_req.term = 1UL;
  acquire_req.ttl_ms = 60000L;
  rc = client->tc_lease_acquire(client, &acquire_req, &acquire_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(acquire_res.granted);
  assert_string_equal(acquire_res.leader_id, "node-a");
  assert_string_equal(acquire_res.leader_endpoint, "pouch://node-a");
  assert_int_equal(acquire_res.term, 1UL);
  assert_true(acquire_res.expires_at_unix > 0L);
  lc_tc_lease_acquire_res_cleanup(&acquire_res);

  acquire_req.candidate_id = "node-b";
  acquire_req.candidate_endpoint = "pouch://node-b";
  rc = client->tc_lease_acquire(client, &acquire_req, &acquire_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(acquire_res.granted);
  assert_string_equal(acquire_res.leader_id, "node-a");
  lc_tc_lease_acquire_res_cleanup(&acquire_res);

  renew_req.leader_id = "node-a";
  renew_req.term = 1UL;
  renew_req.ttl_ms = 60000L;
  rc = client->tc_lease_renew(client, &renew_req, &renew_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(renew_res.renewed);
  assert_string_equal(renew_res.leader_id, "node-a");
  lc_tc_lease_renew_res_cleanup(&renew_res);

  lc_client_close(client);
  client = NULL;
  open_pouch_client(root, &client, &error);
  rc = client->tc_leader(client, &leader_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(leader_res.leader_id, "node-a");
  assert_string_equal(leader_res.leader_endpoint, "pouch://node-a");
  lc_tc_leader_res_cleanup(&leader_res);

  release_req.leader_id = "node-b";
  release_req.term = 1UL;
  rc = client->tc_lease_release(client, &release_req, &release_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(release_res.released);
  lc_tc_lease_release_res_cleanup(&release_res);

  release_req.leader_id = "node-a";
  rc = client->tc_lease_release(client, &release_req, &release_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(release_res.released);
  lc_tc_lease_release_res_cleanup(&release_res);
  rc = client->tc_leader(client, &leader_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(leader_res.leader_id, "");
  lc_tc_leader_res_cleanup(&leader_res);

  cluster_req.self_endpoint = "pouch://node-a";
  rc = client->tc_cluster_announce(client, &cluster_req, &cluster_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(cluster_res.endpoints.count, 1U);
  assert_true(string_list_has(&cluster_res.endpoints, "pouch://node-a"));
  lc_tc_cluster_res_cleanup(&cluster_res);
  lc_client_close(client);
  client = NULL;
  open_pouch_client(root, &client, &error);
  rc = client->tc_cluster_list(client, &cluster_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(cluster_res.endpoints.count, 1U);
  assert_true(string_list_has(&cluster_res.endpoints, "pouch://node-a"));
  lc_tc_cluster_res_cleanup(&cluster_res);
  rc = client->tc_cluster_leave(client, &cluster_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(cluster_res.endpoints.count, 0U);
  lc_tc_cluster_res_cleanup(&cluster_res);

  rm_register_req.backend_hash = "backend-a";
  rm_register_req.endpoint = "pouch://rm-a";
  rc = client->tc_rm_register(client, &rm_register_req, &rm_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(rm_res.backend_hash, "backend-a");
  assert_int_equal(rm_res.endpoints.count, 1U);
  assert_true(string_list_has(&rm_res.endpoints, "pouch://rm-a"));
  lc_tc_rm_res_cleanup(&rm_res);

  rm_register_req.endpoint = "pouch://rm-b";
  rc = client->tc_rm_register(client, &rm_register_req, &rm_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(rm_res.endpoints.count, 2U);
  assert_true(string_list_has(&rm_res.endpoints, "pouch://rm-a"));
  assert_true(string_list_has(&rm_res.endpoints, "pouch://rm-b"));
  lc_tc_rm_res_cleanup(&rm_res);

  rc = client->tc_rm_list(client, &rm_list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(rm_list.backend_count, 1U);
  assert_string_equal(rm_list.backends[0].backend_hash, "backend-a");
  assert_int_equal(rm_list.backends[0].endpoints.count, 2U);
  assert_true(string_list_has(&rm_list.backends[0].endpoints, "pouch://rm-a"));
  assert_true(string_list_has(&rm_list.backends[0].endpoints, "pouch://rm-b"));
  lc_tc_rm_list_res_cleanup(&rm_list);

  rm_unregister_req.backend_hash = "backend-a";
  rm_unregister_req.endpoint = "pouch://rm-a";
  rc = client->tc_rm_unregister(client, &rm_unregister_req, &rm_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(rm_res.endpoints.count, 1U);
  assert_true(string_list_has(&rm_res.endpoints, "pouch://rm-b"));
  lc_tc_rm_res_cleanup(&rm_res);

  rm_register_req.backend_hash = "";
  rm_register_req.endpoint = "pouch://rm-c";
  rc = client->tc_rm_register(client, &rm_register_req, &rm_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);

  lc_error_cleanup(&error);
  lc_tc_lease_acquire_res_cleanup(&acquire_res);
  lc_tc_lease_renew_res_cleanup(&renew_res);
  lc_tc_lease_release_res_cleanup(&release_res);
  lc_tc_leader_res_cleanup(&leader_res);
  lc_tc_cluster_res_cleanup(&cluster_res);
  lc_tc_rm_res_cleanup(&rm_res);
  lc_tc_rm_list_res_cleanup(&rm_list);
  if (client != NULL) {
    lc_client_close(client);
  }
  cleanup_root(root);
}

static void test_state_write_read_replays_segment_after_reopen(void **state) {
  lc_pouch *pouch;
  lc_source *body;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result write_result;
  lc_pouch_state_read_result read_result;
  lc_error error;
  char root[512];
  char bytes[64];
  int rc;

  (void)state;
  lc_error_init(&error);
  memset(&options, 0, sizeof(options));
  memset(&write_result, 0, sizeof(write_result));
  memset(&read_result, 0, sizeof(read_result));
  make_root("state-replay", root, sizeof(root));
  cleanup_root(root);

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("alpha-state", strlen("alpha-state"), &body,
                             &error);
  assert_int_equal(rc, LC_OK);
  options.content_type = "text/plain";
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/current", body,
                            &options, &write_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(write_result.etag, "pouch-state-1");
  assert_int_equal(write_result.version, 1UL);
  assert_int_equal(write_result.bytes, strlen("alpha-state"));
  body->close(body);
  lc_pouch_close(pouch);

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, "team/alpha", "state/current", &read_result,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_result.found);
  assert_string_equal(read_result.content_type, "text/plain");
  assert_string_equal(read_result.etag, "pouch-state-1");
  assert_int_equal(read_result.version, 1UL);
  assert_int_equal(read_result.bytes, strlen("alpha-state"));
  read_source_to_string(read_result.body, bytes, sizeof(bytes));
  assert_string_equal(bytes, "alpha-state");

  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_state_write_enforces_expected_etag(void **state) {
  lc_pouch *pouch;
  lc_source *body;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result first;
  lc_pouch_state_write_result second;
  lc_error error;
  char root[512];
  int rc;

  (void)state;
  lc_error_init(&error);
  memset(&options, 0, sizeof(options));
  memset(&first, 0, sizeof(first));
  memset(&second, 0, sizeof(second));
  make_root("state-etag", root, sizeof(root));
  cleanup_root(root);

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("one", strlen("one"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/current", body, NULL,
                            &first, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);

  lc_error_cleanup(&error);
  lc_error_init(&error);
  rc = lc_source_from_memory("two", strlen("two"), &body, &error);
  assert_int_equal(rc, LC_OK);
  options.expected_etag = "wrong-etag";
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/current", body,
                            &options, &second, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  body->close(body);

  lc_error_cleanup(&error);
  lc_error_init(&error);
  options.expected_etag = first.etag;
  rc = lc_source_from_memory("two", strlen("two"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/current", body,
                            &options, &second, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(second.etag, "pouch-state-2");
  assert_int_equal(second.version, 2UL);
  body->close(body);

  lc_pouch_state_write_result_cleanup(NULL, &first);
  lc_pouch_state_write_result_cleanup(NULL, &second);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_state_writes_roll_active_manifest_segment(void **state) {
  lc_pouch *pouch;
  lc_source *body;
  lc_pouch_open_options open_options;
  lc_pouch_state_write_result first;
  lc_pouch_state_write_result second;
  lc_pouch_state_read_result read_result;
  lc_error error;
  char root[512];
  char bytes[64];
  char *namespace_path;
  int rc;

  (void)state;
  lc_error_init(&error);
  memset(&open_options, 0, sizeof(open_options));
  memset(&first, 0, sizeof(first));
  memset(&second, 0, sizeof(second));
  memset(&read_result, 0, sizeof(read_result));
  make_root("state-rollover", root, sizeof(root));
  cleanup_root(root);

  open_options.segment_target_bytes = 128UL;
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("one", strlen("one"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/current", body, NULL,
                            &first, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);

  rc = lc_source_from_memory("two", strlen("two"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/current", body, NULL,
                            &second, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);

  namespace_path = lc_pouch_namespace_path(NULL, root, "team/alpha");
  assert_non_null(namespace_path);
  assert_path_file(namespace_path, "segments/seg-00000000000000000001.log");
  assert_path_file(namespace_path, "segments/seg-00000000000000000002.log");
  assert_path_file_contains(namespace_path, "manifest",
                            "active_segment=seg-00000000000000000002.log");

  lc_pouch_close(pouch);
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, "team/alpha", "state/current", &read_result,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_result.found);
  assert_string_equal(read_result.etag, "pouch-state-2");
  read_source_to_string(read_result.body, bytes, sizeof(bytes));
  assert_string_equal(bytes, "two");

  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_state_write_result_cleanup(NULL, &first);
  lc_pouch_state_write_result_cleanup(NULL, &second);
  free(namespace_path);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_state_scheduled_compaction_installs_snapshot(void **state) {
  lc_pouch *pouch;
  lc_source *body;
  lc_pouch_open_options open_options;
  lc_pouch_state_write_result write_a;
  lc_pouch_state_write_result delete_a;
  lc_pouch_state_write_result write_b;
  lc_pouch_state_read_result read_result;
  lc_error error;
  char root[512];
  char bytes[64];
  char *namespace_path;
  char path[1024];
  int written;
  int rc;

  (void)state;
  lc_error_init(&error);
  memset(&open_options, 0, sizeof(open_options));
  memset(&write_a, 0, sizeof(write_a));
  memset(&delete_a, 0, sizeof(delete_a));
  memset(&write_b, 0, sizeof(write_b));
  memset(&read_result, 0, sizeof(read_result));
  make_root("state-compact", root, sizeof(root));
  cleanup_root(root);

  open_options.segment_target_bytes = 1UL;
  open_options.compaction_min_segment_count = 2UL;
  open_options.compaction_min_reclaimable_bytes = 1UL;
  open_options.background_compaction_enabled = 1;
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory("one", strlen("one"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/a", body, NULL,
                            &write_a, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);

  rc = lc_pouch_state_delete(pouch, "team/alpha", "state/a", NULL, &delete_a,
                             &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(delete_a.etag, "pouch-state-2");

  namespace_path = lc_pouch_namespace_path(NULL, root, "team/alpha");
  assert_non_null(namespace_path);
  assert_path_file(namespace_path,
                   "snapshots/snapshot-00000000000000000002.log");
  assert_path_file_contains(namespace_path, "manifest",
                            "snapshot=snapshot-00000000000000000002.log");
  assert_path_file_contains(namespace_path, "manifest",
                            "active_segment=seg-00000000000000000003.log");
  written = snprintf(path, sizeof(path), "%s/segments/%s", namespace_path,
                     "seg-00000000000000000001.log");
  assert_true(written > 0 && (size_t)written < sizeof(path));
  assert_false(path_is_file(path));
  written = snprintf(path, sizeof(path), "%s/segments/%s", namespace_path,
                     "seg-00000000000000000002.log");
  assert_true(written > 0 && (size_t)written < sizeof(path));
  assert_false(path_is_file(path));

  rc = lc_source_from_memory("two", strlen("two"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/b", body, NULL,
                            &write_b, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);

  lc_pouch_close(pouch);
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, "team/alpha", "state/a", &read_result,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_false(read_result.found);
  lc_pouch_state_read_result_cleanup(NULL, &read_result);

  rc = lc_pouch_state_read(pouch, "team/alpha", "state/b", &read_result,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_result.found);
  assert_string_equal(read_result.etag, "pouch-state-3");
  read_source_to_string(read_result.body, bytes, sizeof(bytes));
  assert_string_equal(bytes, "two");

  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_state_write_result_cleanup(NULL, &write_a);
  lc_pouch_state_write_result_cleanup(NULL, &delete_a);
  lc_pouch_state_write_result_cleanup(NULL, &write_b);
  free(namespace_path);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_maintenance_reports_disabled_without_force(void **state) {
  lc_pouch *pouch;
  lc_source *body;
  lc_pouch_open_options open_options;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  lc_pouch_state_write_result write_result;
  lc_error error;
  char root[512];
  int rc;

  (void)state;
  lc_error_init(&error);
  memset(&open_options, 0, sizeof(open_options));
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  memset(&write_result, 0, sizeof(write_result));
  make_root("maintenance-disabled", root, sizeof(root));
  cleanup_root(root);

  open_options.segment_target_bytes = 1UL;
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory("one", strlen("one"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/a", body, NULL,
                            &write_result, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);

  maintenance_options.namespace_name = "team/alpha";
  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance_result.namespace_name, "team/alpha");
  assert_string_equal(maintenance_result.diagnostic, "disabled");
  assert_true(maintenance_result.skipped);
  assert_false(maintenance_result.compacted);

  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_maintenance_retention_sweep_deletes_expired_state(
    void **state) {
  lc_pouch *pouch;
  lc_source *body;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  lc_pouch_state_write_result write_result;
  lc_pouch_state_read_result read_result;
  lc_error error;
  char root[512];
  int rc;

  (void)state;
  pouch = NULL;
  body = NULL;
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  memset(&write_result, 0, sizeof(write_result));
  memset(&read_result, 0, sizeof(read_result));
  lc_error_init(&error);
  make_root("maintenance-retention", root, sizeof(root));
  cleanup_root(root);

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("one", strlen("one"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/retention", "state/a", body, NULL,
                            &write_result, &error);
  body->close(body);
  body = NULL;
  assert_int_equal(rc, LC_OK);
  assert_true(write_result.updated_at_unix > 0L);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("two", strlen("two"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/retention", "state/b", body, NULL,
                            &write_result, &error);
  body->close(body);
  body = NULL;
  assert_int_equal(rc, LC_OK);
  assert_true(write_result.updated_at_unix > 0L);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  maintenance_options.namespace_name = "team/retention";
  maintenance_options.retention_updated_before_unix = 1L;
  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance_result.diagnostic, "retention-complete");
  assert_int_equal(maintenance_result.retention_scanned_count, 2UL);
  assert_int_equal(maintenance_result.retention_expired_count, 0UL);
  assert_int_equal(maintenance_result.retention_deleted_state_count, 0UL);
  assert_int_equal(maintenance_result.retention_failed_count, 0UL);
  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);

  maintenance_options.retention_updated_before_unix = 2147483647L;
  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance_result.diagnostic, "retention-complete");
  assert_int_equal(maintenance_result.retention_scanned_count, 2UL);
  assert_int_equal(maintenance_result.retention_expired_count, 2UL);
  assert_int_equal(maintenance_result.retention_deleted_state_count, 2UL);
  assert_int_equal(maintenance_result.retention_failed_count, 0UL);
  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);

  rc = lc_pouch_state_read(pouch, "team/retention", "state/a", &read_result,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_false(read_result.found);
  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  rc = lc_pouch_state_read(pouch, "team/retention", "state/b", &read_result,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_false(read_result.found);
  lc_pouch_state_read_result_cleanup(NULL, &read_result);

  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance_result.diagnostic, "retention-complete");
  assert_int_equal(maintenance_result.retention_scanned_count, 0UL);
  assert_int_equal(maintenance_result.retention_expired_count, 0UL);
  assert_int_equal(maintenance_result.retention_deleted_state_count, 0UL);
  assert_int_equal(maintenance_result.retention_failed_count, 0UL);

  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_maintenance_creates_namespace_without_prior_writes(
    void **state) {
  lc_pouch *pouch;
  lc_pouch_open_options open_options;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  lc_error error;
  char *namespace_path;
  char root[512];
  int rc;

  (void)state;
  lc_error_init(&error);
  memset(&open_options, 0, sizeof(open_options));
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  make_root("maintenance-empty-namespace", root, sizeof(root));
  cleanup_root(root);

  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  maintenance_options.namespace_name = "team/empty";
  maintenance_options.cleanup_only = 1;
  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance_result.namespace_name, "team/empty");
  assert_string_equal(maintenance_result.diagnostic, "cleanup-complete");
  assert_true(maintenance_result.skipped);
  assert_false(maintenance_result.compacted);
  assert_int_equal(maintenance_result.cleanup_deleted_count, 0UL);
  assert_int_equal(maintenance_result.cleanup_pending_count, 0UL);

  namespace_path = lc_pouch_namespace_path(NULL, root, "team/empty");
  assert_non_null(namespace_path);
  assert_path_dir(namespace_path, "segments");
  assert_path_dir(namespace_path, "snapshots");
  assert_path_dir(namespace_path, "markers");
  assert_path_dir(namespace_path, "index");
  assert_path_file(namespace_path, "manifest");

  free(namespace_path);
  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_maintenance_reports_threshold_skip(void **state) {
  lc_pouch *pouch;
  lc_source *body;
  lc_pouch_open_options open_options;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  lc_pouch_state_write_result first;
  lc_pouch_state_write_result second;
  lc_error error;
  char root[512];
  int rc;

  (void)state;
  lc_error_init(&error);
  memset(&open_options, 0, sizeof(open_options));
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  memset(&first, 0, sizeof(first));
  memset(&second, 0, sizeof(second));
  make_root("maintenance-threshold", root, sizeof(root));
  cleanup_root(root);

  open_options.segment_target_bytes = 1UL;
  open_options.compaction_min_segment_count = 4UL;
  open_options.compaction_min_reclaimable_bytes = 1UL;
  open_options.background_compaction_enabled = 1;
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory("one", strlen("one"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/a", body, NULL,
                            &first, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);
  rc = lc_source_from_memory("two", strlen("two"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/b", body, NULL,
                            &second, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);

  maintenance_options.namespace_name = "team/alpha";
  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance_result.diagnostic,
                      "below-segment-threshold");
  assert_true(maintenance_result.skipped);
  assert_false(maintenance_result.compacted);
  assert_true(maintenance_result.candidate_segment_count < 4UL);
  assert_true(maintenance_result.candidate_bytes > 0UL);

  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  lc_pouch_state_write_result_cleanup(NULL, &first);
  lc_pouch_state_write_result_cleanup(NULL, &second);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_maintenance_force_installs_snapshot(void **state) {
  lc_pouch *pouch;
  lc_source *body;
  lc_pouch_open_options open_options;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  lc_pouch_state_write_result first;
  lc_pouch_state_write_result second;
  lc_pouch_state_read_result read_result;
  lc_error error;
  char root[512];
  char bytes[64];
  char *namespace_path;
  int rc;

  (void)state;
  lc_error_init(&error);
  memset(&open_options, 0, sizeof(open_options));
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  memset(&first, 0, sizeof(first));
  memset(&second, 0, sizeof(second));
  memset(&read_result, 0, sizeof(read_result));
  make_root("maintenance-force", root, sizeof(root));
  cleanup_root(root);

  open_options.segment_target_bytes = 1UL;
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory("one", strlen("one"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/a", body, NULL,
                            &first, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);
  rc = lc_source_from_memory("two", strlen("two"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/b", body, NULL,
                            &second, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);

  maintenance_options.namespace_name = "team/alpha";
  maintenance_options.force = 1;
  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance_result.diagnostic, "compacted");
  assert_true(maintenance_result.compacted);
  assert_false(maintenance_result.skipped);
  assert_int_equal(maintenance_result.compacted_segment_id, 2UL);

  namespace_path = lc_pouch_namespace_path(NULL, root, "team/alpha");
  assert_non_null(namespace_path);
  assert_path_file(namespace_path,
                   "snapshots/snapshot-00000000000000000002.log");
  assert_path_file_contains(namespace_path, "manifest",
                            "snapshot=snapshot-00000000000000000002.log");

  lc_pouch_close(pouch);
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, "team/alpha", "state/b", &read_result,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_result.found);
  read_source_to_string(read_result.body, bytes, sizeof(bytes));
  assert_string_equal(bytes, "two");

  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  lc_pouch_state_write_result_cleanup(NULL, &first);
  lc_pouch_state_write_result_cleanup(NULL, &second);
  free(namespace_path);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_manifest_open_ignores_unmanifested_snapshot(void **state) {
  lc_pouch *pouch;
  lc_source *body;
  lc_pouch_open_options open_options;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  lc_pouch_namespace_manifest manifest;
  lc_pouch_state_write_result first;
  lc_pouch_state_write_result second;
  lc_error error;
  char root[512];
  char *namespace_path;
  char *stray_snapshot_path;
  unsigned long cleanup_deleted_count;
  unsigned long cleanup_pending_count;
  int rc;

  (void)state;
  pouch = NULL;
  namespace_path = NULL;
  stray_snapshot_path = NULL;
  cleanup_deleted_count = 0UL;
  cleanup_pending_count = 0UL;
  lc_error_init(&error);
  memset(&open_options, 0, sizeof(open_options));
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  memset(&manifest, 0, sizeof(manifest));
  memset(&first, 0, sizeof(first));
  memset(&second, 0, sizeof(second));
  make_root("manifest-stray-snapshot", root, sizeof(root));
  cleanup_root(root);

  open_options.segment_target_bytes = 1UL;
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("one", strlen("one"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/a", body, NULL,
                            &first, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);
  rc = lc_source_from_memory("two", strlen("two"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/b", body, NULL,
                            &second, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);

  maintenance_options.namespace_name = "team/alpha";
  maintenance_options.force = 1;
  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_close(pouch);
  pouch = NULL;

  namespace_path = lc_pouch_namespace_path(NULL, root, "team/alpha");
  assert_non_null(namespace_path);
  stray_snapshot_path =
      lc_pouch_path_join(NULL, namespace_path, "snapshots/"
                         "snapshot-00000000000000000099.log");
  assert_non_null(stray_snapshot_path);
  write_text_file(stray_snapshot_path, "H 99\n");

  rc = lc_pouch_namespace_manifest_open(
      NULL, root, "team/alpha", &manifest, &cleanup_deleted_count,
      &cleanup_pending_count, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(manifest.latest_snapshot,
                      "snapshot-00000000000000000002.log");
  assert_int_equal(manifest.latest_snapshot_segment_id, 2UL);
  assert_path_file_not_contains(
      namespace_path, "manifest",
      "snapshot=snapshot-00000000000000000099.log");

  lc_pouch_namespace_manifest_cleanup(NULL, &manifest);
  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  lc_pouch_state_write_result_cleanup(NULL, &first);
  lc_pouch_state_write_result_cleanup(NULL, &second);
  free(stray_snapshot_path);
  free(namespace_path);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_maintenance_aborts_on_validation_drift(void **state) {
  lc_pouch *pouch;
  lc_pouch *peer;
  lc_source *body;
  lc_pouch_open_options open_options;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  lc_pouch_state_write_result write_result;
  pouch_compaction_drift_hook_state hook_state;
  lc_error error;
  char root[512];
  char key[128];
  char *namespace_path;
  int rc;
  unsigned int i;

  (void)state;
  pouch = NULL;
  peer = NULL;
  namespace_path = NULL;
  lc_error_init(&error);
  memset(&open_options, 0, sizeof(open_options));
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  memset(&write_result, 0, sizeof(write_result));
  memset(&hook_state, 0, sizeof(hook_state));
  make_root("maintenance-validation-drift", root, sizeof(root));
  cleanup_root(root);

  open_options.segment_target_bytes = 100000000UL;
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  for (i = 0U; i < 16U; ++i) {
    snprintf(key, sizeof(key), "state/item/%04u", i);
    rc = lc_source_from_memory("seed", strlen("seed"), &body, &error);
    assert_int_equal(rc, LC_OK);
    rc = lc_pouch_state_write(pouch, "team/alpha", key, body, NULL,
                              &write_result, &error);
    assert_int_equal(rc, LC_OK);
    body->close(body);
    lc_pouch_state_write_result_cleanup(NULL, &write_result);
    memset(&write_result, 0, sizeof(write_result));
  }

  namespace_path = lc_pouch_namespace_path(NULL, root, "team/alpha");
  assert_non_null(namespace_path);
  rc = lc_pouch_open(root, NULL, &open_options, &peer, &error);
  assert_int_equal(rc, LC_OK);
  hook_state.peer = peer;
  lc_pouch_test_after_snapshot_write_context = &hook_state;
  lc_pouch_test_after_snapshot_write_hook = pouch_compaction_drift_hook;

  maintenance_options.namespace_name = "team/alpha";
  maintenance_options.force = 1;
  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, &error);
  lc_pouch_test_after_snapshot_write_hook = NULL;
  lc_pouch_test_after_snapshot_write_context = NULL;
  assert_true(hook_state.called);
  assert_int_equal(rc, LC_ERR_PROTOCOL);
  assert_string_equal(maintenance_result.diagnostic,
                      "validation-drift-aborted");
  assert_true(maintenance_result.aborted);
  assert_false(maintenance_result.compacted);
  assert_false(maintenance_result.skipped);
  assert_path_file_not_contains(
      namespace_path, "manifest",
      "snapshot=snapshot-00000000000000000001.log");

  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  free(namespace_path);
  lc_pouch_close(peer);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_maintenance_aborts_on_same_size_segment_drift(void **state) {
  lc_pouch *pouch;
  lc_source *body;
  lc_pouch_open_options open_options;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  lc_pouch_state_write_result write_result;
  pouch_compaction_drift_hook_state hook_state;
  lc_error error;
  char root[512];
  char key[128];
  char needle_hex[256];
  char replacement_hex[256];
  char segment_path[1024];
  char *namespace_path;
  char *segment_leaf;
  int written;
  int rc;
  unsigned int i;

  (void)state;
  pouch = NULL;
  namespace_path = NULL;
  segment_leaf = NULL;
  lc_error_init(&error);
  memset(&open_options, 0, sizeof(open_options));
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  memset(&write_result, 0, sizeof(write_result));
  memset(&hook_state, 0, sizeof(hook_state));
  make_root("maintenance-same-size-validation-drift", root, sizeof(root));
  cleanup_root(root);

  open_options.segment_target_bytes = 100000000UL;
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  for (i = 0U; i < 16U; ++i) {
    snprintf(key, sizeof(key), "state/item/%04u", i);
    rc = lc_source_from_memory("seed", strlen("seed"), &body, &error);
    assert_int_equal(rc, LC_OK);
    rc = lc_pouch_state_write(pouch, "team/alpha", key, body, NULL,
                              &write_result, &error);
    assert_int_equal(rc, LC_OK);
    body->close(body);
    lc_pouch_state_write_result_cleanup(NULL, &write_result);
    memset(&write_result, 0, sizeof(write_result));
  }

  namespace_path = lc_pouch_namespace_path(NULL, root, "team/alpha");
  segment_leaf = lc_pouch_namespace_segment_leaf(NULL, 1UL);
  assert_non_null(namespace_path);
  assert_non_null(segment_leaf);
  written = snprintf(segment_path, sizeof(segment_path), "%s/segments/%s",
                     namespace_path, segment_leaf);
  assert_true(written > 0 && (size_t)written < sizeof(segment_path));
  hex_encode_string("state/item/0000", needle_hex, sizeof(needle_hex));
  hex_encode_string("state/item/9999", replacement_hex,
                    sizeof(replacement_hex));
  assert_int_equal(strlen(needle_hex), strlen(replacement_hex));

  hook_state.segment_path = segment_path;
  hook_state.needle = needle_hex;
  hook_state.replacement = replacement_hex;
  lc_pouch_test_after_snapshot_write_context = &hook_state;
  lc_pouch_test_after_snapshot_write_hook = pouch_compaction_drift_hook;

  maintenance_options.namespace_name = "team/alpha";
  maintenance_options.force = 1;
  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, &error);
  lc_pouch_test_after_snapshot_write_hook = NULL;
  lc_pouch_test_after_snapshot_write_context = NULL;
  assert_true(hook_state.called);
  assert_int_equal(rc, LC_ERR_PROTOCOL);
  assert_string_equal(maintenance_result.diagnostic,
                      "validation-drift-aborted");
  assert_true(maintenance_result.aborted);
  assert_false(maintenance_result.compacted);
  assert_false(maintenance_result.skipped);
  assert_path_file_not_contains(
      namespace_path, "manifest",
      "snapshot=snapshot-00000000000000000001.log");

  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  free(segment_leaf);
  free(namespace_path);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_maintenance_reports_snapshot_write_abort(void **state) {
  lc_pouch *pouch;
  lc_source *body;
  lc_pouch_open_options open_options;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  lc_pouch_state_write_result first;
  lc_pouch_state_write_result second;
  lc_error error;
  char root[512];
  char *namespace_path;
  char *snapshots_path;
  char *snapshot_path;
  int rc;

  (void)state;
  lc_error_init(&error);
  memset(&open_options, 0, sizeof(open_options));
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  memset(&first, 0, sizeof(first));
  memset(&second, 0, sizeof(second));
  namespace_path = NULL;
  snapshots_path = NULL;
  snapshot_path = NULL;
  make_root("maintenance-snapshot-abort", root, sizeof(root));
  cleanup_root(root);

  open_options.segment_target_bytes = 1UL;
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory("one", strlen("one"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/a", body, NULL,
                            &first, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);
  rc = lc_source_from_memory("two", strlen("two"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/b", body, NULL,
                            &second, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);

  namespace_path = lc_pouch_namespace_path(NULL, root, "team/alpha");
  assert_non_null(namespace_path);
  snapshots_path = lc_pouch_path_join(NULL, namespace_path, "snapshots");
  assert_non_null(snapshots_path);
  snapshot_path =
      lc_pouch_path_join(NULL, snapshots_path,
                         "snapshot-00000000000000000002.log");
  assert_non_null(snapshot_path);
  assert_int_equal(chmod(snapshots_path, 0555), 0);

  maintenance_options.namespace_name = "team/alpha";
  maintenance_options.force = 1;
  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, &error);
  assert_int_equal(chmod(snapshots_path, 0755), 0);
  assert_true(rc != LC_OK);
  assert_string_equal(maintenance_result.namespace_name, "team/alpha");
  assert_string_equal(maintenance_result.diagnostic, "snapshot-write-aborted");
  assert_true(maintenance_result.aborted);
  assert_false(maintenance_result.compacted);
  assert_false(maintenance_result.skipped);
  assert_int_equal(maintenance_result.candidate_segment_count, 2UL);
  assert_true(maintenance_result.candidate_bytes > 0UL);
  assert_false(path_is_file(snapshot_path));

  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  lc_pouch_state_write_result_cleanup(NULL, &first);
  lc_pouch_state_write_result_cleanup(NULL, &second);
  free(snapshot_path);
  free(snapshots_path);
  free(namespace_path);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_maintenance_reports_interval_skip(void **state) {
  lc_pouch *pouch;
  lc_source *body;
  lc_pouch_open_options open_options;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  lc_pouch_state_write_result write_result;
  lc_error error;
  char root[512];
  char *namespace_path;
  int i;
  int rc;

  (void)state;
  lc_error_init(&error);
  memset(&open_options, 0, sizeof(open_options));
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  memset(&write_result, 0, sizeof(write_result));
  make_root("maintenance-interval", root, sizeof(root));
  cleanup_root(root);

  open_options.segment_target_bytes = 1UL;
  open_options.compaction_min_segment_count = 2UL;
  open_options.compaction_min_reclaimable_bytes = 1UL;
  open_options.compaction_interval_seconds = 3600UL;
  open_options.background_compaction_enabled = 1;
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  for (i = 0; i < 4; ++i) {
    char key[32];
    char payload[32];

    snprintf(key, sizeof(key), "state/%d", i);
    snprintf(payload, sizeof(payload), "value-%d", i);
    rc = lc_source_from_memory(payload, strlen(payload), &body, &error);
    assert_int_equal(rc, LC_OK);
    rc = lc_pouch_state_write(pouch, "team/alpha", key, body, NULL,
                              &write_result, &error);
    assert_int_equal(rc, LC_OK);
    body->close(body);
    lc_pouch_state_write_result_cleanup(NULL, &write_result);
  }

  maintenance_options.namespace_name = "team/alpha";
  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance_result.diagnostic, "interval-not-elapsed");
  assert_true(maintenance_result.skipped);
  assert_false(maintenance_result.compacted);
  assert_int_equal(maintenance_result.candidate_segment_count, 2UL);

  namespace_path = lc_pouch_namespace_path(NULL, root, "team/alpha");
  assert_non_null(namespace_path);
  assert_path_file(namespace_path,
                   "snapshots/snapshot-00000000000000000002.log");

  free(namespace_path);
  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_compaction_retries_manifest_obsolete_cleanup(void **state) {
  lc_pouch *pouch;
  lc_source *body;
  lc_pouch_open_options open_options;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  lc_pouch_state_write_result first;
  lc_pouch_state_write_result second;
  lc_error error;
  char root[512];
  char *namespace_path;
  char manifest_path[2048];
  char segments_path[2048];
  char snapshots_path[2048];
  char *segment_one_path;
  char *segment_two_path;
  char *stale_snapshot_path;
  int rc;

  (void)state;
  lc_error_init(&error);
  memset(&open_options, 0, sizeof(open_options));
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  memset(&first, 0, sizeof(first));
  memset(&second, 0, sizeof(second));
  segment_one_path = NULL;
  segment_two_path = NULL;
  stale_snapshot_path = NULL;
  make_root("maintenance-obsolete-retry", root, sizeof(root));
  cleanup_root(root);

  open_options.segment_target_bytes = 1UL;
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory("one", strlen("one"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/a", body, NULL,
                            &first, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);
  rc = lc_source_from_memory("two", strlen("two"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/b", body, NULL,
                            &second, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);

  namespace_path = lc_pouch_namespace_path(NULL, root, "team/alpha");
  assert_non_null(namespace_path);
  snprintf(manifest_path, sizeof(manifest_path), "%s/manifest",
           namespace_path);
  snprintf(segments_path, sizeof(segments_path), "%s/segments",
           namespace_path);
  snprintf(snapshots_path, sizeof(snapshots_path), "%s/snapshots",
           namespace_path);
  segment_one_path =
      lc_pouch_path_join(NULL, segments_path,
                         "seg-00000000000000000001.log");
  segment_two_path =
      lc_pouch_path_join(NULL, segments_path,
                         "seg-00000000000000000002.log");
  assert_non_null(segment_one_path);
  assert_non_null(segment_two_path);
  assert_true(path_is_file(segment_one_path));
  assert_true(path_is_file(segment_two_path));
  assert_int_equal(chmod(segments_path, 0555), 0);

  maintenance_options.namespace_name = "team/alpha";
  maintenance_options.force = 1;
  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance_result.diagnostic, "compacted");
  assert_int_equal(maintenance_result.cleanup_deleted_count, 0UL);
  assert_int_equal(maintenance_result.cleanup_pending_count, 2UL);
  assert_true(path_is_file(segment_one_path));
  assert_true(path_is_file(segment_two_path));
  assert_file_contains(manifest_path,
                       "obsolete_segment=seg-00000000000000000001.log");
  assert_file_contains(manifest_path,
                       "obsolete_segment=seg-00000000000000000002.log");

  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  maintenance_options.namespace_name = "team/alpha";
  maintenance_options.cleanup_only = 1;
  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance_result.diagnostic, "cleanup-pending");
  assert_true(maintenance_result.skipped);
  assert_false(maintenance_result.compacted);
  assert_int_equal(maintenance_result.cleanup_deleted_count, 0UL);
  assert_int_equal(maintenance_result.cleanup_pending_count, 2UL);

  assert_int_equal(chmod(segments_path, 0755), 0);
  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance_result.diagnostic, "cleanup-complete");
  assert_true(maintenance_result.skipped);
  assert_false(maintenance_result.compacted);
  assert_int_equal(maintenance_result.cleanup_deleted_count, 2UL);
  assert_int_equal(maintenance_result.cleanup_pending_count, 0UL);

  assert_false(path_is_file(segment_one_path));
  assert_false(path_is_file(segment_two_path));
  assert_file_not_contains(manifest_path, "obsolete_segment=");

  stale_snapshot_path =
      lc_pouch_path_join(NULL, snapshots_path,
                         "snapshot-00000000000000000099.log");
  assert_non_null(stale_snapshot_path);
  write_text_file(stale_snapshot_path, "stale\n");
  append_text_file(
      manifest_path,
      "obsolete_snapshot=snapshot-00000000000000000099.log\n");
  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance_result.diagnostic, "cleanup-complete");
  assert_int_equal(maintenance_result.cleanup_deleted_count, 1UL);
  assert_int_equal(maintenance_result.cleanup_pending_count, 0UL);
  assert_false(path_is_file(stale_snapshot_path));
  assert_file_not_contains(manifest_path, "obsolete_snapshot=");

  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  lc_pouch_state_write_result_cleanup(NULL, &first);
  lc_pouch_state_write_result_cleanup(NULL, &second);
  free(segment_one_path);
  free(segment_two_path);
  free(stale_snapshot_path);
  free(namespace_path);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static int count_state_visit_entries(const lc_pouch_state_visit_entry *entry,
                                     void *context, lc_error *error) {
  int *count;

  (void)entry;
  (void)error;
  count = (int *)context;
  ++*count;
  return LC_OK;
}

static void test_snapshot_high_water_survives_compaction_reopen(void **state) {
  lc_pouch *pouch;
  lc_source *body;
  lc_pouch_open_options open_options;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  lc_pouch_state_write_result write_a;
  lc_pouch_state_write_result delete_a;
  lc_pouch_state_write_result write_b;
  lc_error error;
  char root[512];
  char *namespace_path;
  unsigned long index_seq;
  int visit_count;
  int rc;

  (void)state;
  lc_error_init(&error);
  memset(&open_options, 0, sizeof(open_options));
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  memset(&write_a, 0, sizeof(write_a));
  memset(&delete_a, 0, sizeof(delete_a));
  memset(&write_b, 0, sizeof(write_b));
  make_root("snapshot-high-water", root, sizeof(root));
  cleanup_root(root);

  open_options.segment_target_bytes = 1UL;
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory("one", strlen("one"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/a", body, NULL,
                            &write_a, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);
  rc = lc_pouch_state_delete(pouch, "team/alpha", "state/a", NULL, &delete_a,
                             &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("two", strlen("two"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/b", body, NULL,
                            &write_b, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);

  maintenance_options.namespace_name = "team/alpha";
  maintenance_options.force = 1;
  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance_result.diagnostic, "compacted");
  assert_int_equal(maintenance_result.compacted_segment_id, 3UL);

  namespace_path = lc_pouch_namespace_path(NULL, root, "team/alpha");
  assert_non_null(namespace_path);
  assert_path_file_contains(namespace_path,
                            "snapshots/snapshot-00000000000000000003.log",
                            "H 3\n");
  lc_pouch_close(pouch);

  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_index_seq(pouch, "team/alpha", &index_seq, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(index_seq, 3UL);

  visit_count = 0;
  rc = lc_pouch_state_visit(pouch, "team/alpha", count_state_visit_entries,
                            &visit_count, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(visit_count, 1);

  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  lc_pouch_state_write_result_cleanup(NULL, &write_a);
  lc_pouch_state_write_result_cleanup(NULL, &delete_a);
  lc_pouch_state_write_result_cleanup(NULL, &write_b);
  free(namespace_path);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_state_metadata_survives_snapshot_compaction(void **state) {
  lc_pouch *pouch;
  lc_source *body;
  lc_pouch_open_options open_options;
  lc_pouch_state_write_result write_result;
  lc_pouch_state_write_result metadata_result;
  lc_pouch_state_read_result read_result;
  lc_pouch_state_write_options metadata_options;
  lc_error error;
  char root[512];
  char bytes[64];
  char *namespace_path;
  int rc;

  (void)state;
  lc_error_init(&error);
  memset(&open_options, 0, sizeof(open_options));
  memset(&write_result, 0, sizeof(write_result));
  memset(&metadata_result, 0, sizeof(metadata_result));
  memset(&read_result, 0, sizeof(read_result));
  memset(&metadata_options, 0, sizeof(metadata_options));
  make_root("state-metadata-compact", root, sizeof(root));
  cleanup_root(root);

  open_options.segment_target_bytes = 1UL;
  open_options.compaction_min_segment_count = 2UL;
  open_options.compaction_min_reclaimable_bytes = 1UL;
  open_options.background_compaction_enabled = 1;
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory("visible", strlen("visible"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/meta", body, NULL,
                            &write_result, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);

  metadata_options.has_query_hidden = 1;
  metadata_options.query_hidden = 1;
  rc = lc_pouch_state_update_metadata(pouch, "team/alpha", "state/meta",
                                      &metadata_options, &metadata_result,
                                      &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(metadata_result.version, 2UL);
  assert_string_equal(metadata_result.etag, "pouch-state-1");
  assert_true(metadata_result.has_query_hidden);
  assert_true(metadata_result.query_hidden);

  namespace_path = lc_pouch_namespace_path(NULL, root, "team/alpha");
  assert_non_null(namespace_path);
  assert_path_file(namespace_path,
                   "snapshots/snapshot-00000000000000000002.log");
  assert_path_file_contains(namespace_path,
                            "snapshots/snapshot-00000000000000000002.log",
                            " 1 1");

  lc_pouch_close(pouch);
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, "team/alpha", "state/meta", &read_result,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_result.found);
  assert_int_equal(read_result.version, 2UL);
  assert_string_equal(read_result.etag, "pouch-state-1");
  assert_true(read_result.has_query_hidden);
  assert_true(read_result.query_hidden);
  read_source_to_string(read_result.body, bytes, sizeof(bytes));
  assert_string_equal(bytes, "visible");

  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  lc_pouch_state_write_result_cleanup(NULL, &metadata_result);
  free(namespace_path);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_namespace_manifest_repairs_from_existing_segments(
    void **state) {
  lc_pouch *pouch;
  lc_source *body;
  lc_pouch_state_write_result write_result;
  lc_pouch_state_read_result read_result;
  lc_error error;
  char root[512];
  char manifest_path[1024];
  char bytes[64];
  char *namespace_path;
  int rc;

  (void)state;
  lc_error_init(&error);
  memset(&write_result, 0, sizeof(write_result));
  memset(&read_result, 0, sizeof(read_result));
  make_root("manifest-repair", root, sizeof(root));
  cleanup_root(root);

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("repair-me", strlen("repair-me"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/current", body, NULL,
                            &write_result, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);
  lc_pouch_close(pouch);

  namespace_path = lc_pouch_namespace_path(NULL, root, "team/alpha");
  assert_non_null(namespace_path);
  snprintf(manifest_path, sizeof(manifest_path), "%s/manifest",
           namespace_path);
  write_text_file(manifest_path, "broken=true\nactive_segment=bad\n");

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, "team/alpha", "state/current", &read_result,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_result.found);
  read_source_to_string(read_result.body, bytes, sizeof(bytes));
  assert_string_equal(bytes, "repair-me");
  assert_path_file_contains(namespace_path, "manifest",
                            "active_segment=seg-00000000000000000001.log");

  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  free(namespace_path);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_staged_state_writes_durable_decision_records(void **state) {
  lc_pouch *pouch;
  lc_source *source;
  lc_pouch_state_write_result committed;
  lc_pouch_state_write_result staged_commit;
  lc_pouch_state_write_result promoted;
  lc_pouch_state_write_result staged_discard;
  lc_error error;
  char root[512];
  char *namespace_path;
  char *segment_leaf;
  char segment_path[1024];
  char committed_hex[64];
  char discarded_hex[64];
  int discarded;
  int written;
  int rc;

  (void)state;
  pouch = NULL;
  source = NULL;
  namespace_path = NULL;
  segment_leaf = NULL;
  memset(&committed, 0, sizeof(committed));
  memset(&staged_commit, 0, sizeof(staged_commit));
  memset(&promoted, 0, sizeof(promoted));
  memset(&staged_discard, 0, sizeof(staged_discard));
  lc_error_init(&error);
  make_root("state-decision-records", root, sizeof(root));
  cleanup_root(root);

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("{\"value\":1}", strlen("{\"value\":1}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/decision", source,
                            NULL, &committed, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory("{\"value\":2}", strlen("{\"value\":2}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_stage_write(pouch, "team/alpha", "state/decision",
                                  "commit-txn", source, NULL, &staged_commit,
                                  &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_promote_staged(pouch, "team/alpha", "state/decision",
                                     "commit-txn", committed.etag, &promoted,
                                     &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory("{\"value\":3}", strlen("{\"value\":3}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_stage_write(pouch, "team/alpha", "state/discard",
                                  "discard-txn", source, NULL,
                                  &staged_discard, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  discarded = 0;
  rc = lc_pouch_state_discard_staged(pouch, "team/alpha", "state/discard",
                                     "discard-txn", &discarded, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(discarded, 1);

  namespace_path = lc_pouch_namespace_path(NULL, root, "team/alpha");
  segment_leaf = lc_pouch_namespace_segment_leaf(NULL, 1UL);
  assert_non_null(namespace_path);
  assert_non_null(segment_leaf);
  written = snprintf(segment_path, sizeof(segment_path), "%s/segments/%s",
                     namespace_path, segment_leaf);
  assert_true(written > 0 && (size_t)written < sizeof(segment_path));
  hex_encode_string("committed", committed_hex, sizeof(committed_hex));
  hex_encode_string("discarded", discarded_hex, sizeof(discarded_hex));
  assert_file_contains(segment_path, "T ");
  assert_file_contains(segment_path, committed_hex);
  assert_file_contains(segment_path, discarded_hex);

  free(segment_leaf);
  free(namespace_path);
  lc_pouch_state_write_result_cleanup(NULL, &staged_discard);
  lc_pouch_state_write_result_cleanup(NULL, &promoted);
  lc_pouch_state_write_result_cleanup(NULL, &staged_commit);
  lc_pouch_state_write_result_cleanup(NULL, &committed);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_staged_decision_recovery_tombstones_interrupted_discard(
    void **state) {
  lc_pouch *pouch;
  lc_source *source;
  lc_pouch_state_write_result staged;
  lc_pouch_state_read_result read_result;
  lc_error error;
  char root[512];
  char staged_key[256];
  char staged_key_hex[512];
  char etag_hex[128];
  char decision_hex[64];
  char decision_record[1024];
  char *namespace_path;
  char *segment_leaf;
  char segment_path[1024];
  int written;
  int rc;

  (void)state;
  pouch = NULL;
  source = NULL;
  namespace_path = NULL;
  segment_leaf = NULL;
  memset(&staged, 0, sizeof(staged));
  memset(&read_result, 0, sizeof(read_result));
  lc_error_init(&error);
  make_root("state-decision-recovery", root, sizeof(root));
  cleanup_root(root);

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("{\"value\":9}", strlen("{\"value\":9}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_stage_write(pouch, "team/alpha", "state/recover",
                                  "txn-recover", source, NULL, &staged,
                                  &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_close(pouch);
  pouch = NULL;

  namespace_path = lc_pouch_namespace_path(NULL, root, "team/alpha");
  segment_leaf = lc_pouch_namespace_segment_leaf(NULL, 1UL);
  assert_non_null(namespace_path);
  assert_non_null(segment_leaf);
  written = snprintf(segment_path, sizeof(segment_path), "%s/segments/%s",
                     namespace_path, segment_leaf);
  assert_true(written > 0 && (size_t)written < sizeof(segment_path));
  written = snprintf(staged_key, sizeof(staged_key),
                     "state/recover/.staging/txn-recover");
  assert_true(written > 0 && (size_t)written < sizeof(staged_key));
  hex_encode_string(staged_key, staged_key_hex, sizeof(staged_key_hex));
  hex_encode_string(staged.etag, etag_hex, sizeof(etag_hex));
  hex_encode_string("discarded", decision_hex, sizeof(decision_hex));
  written = snprintf(decision_record, sizeof(decision_record), "T 2 %s %s %s\n",
                     staged_key_hex, etag_hex, decision_hex);
  assert_true(written > 0 && (size_t)written < sizeof(decision_record));
  append_text_file(segment_path, decision_record);

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, "team/alpha", staged_key, &read_result,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(read_result.found, 0);
  assert_file_contains(segment_path, "D 3 ");
  assert_file_contains(segment_path, staged_key_hex);

  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  free(segment_leaf);
  free(namespace_path);
  lc_pouch_state_write_result_cleanup(NULL, &staged);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_client_update_get_load_roundtrips_state(void **state) {
  lc_client *client;
  lc_client *reader;
  lc_sink *sink;
  lc_update_res update_res;
  lc_get_res get_res;
  pouch_value_doc doc;
  lc_error error;
  const void *bytes;
  size_t length;
  char root[512];
  char key[96];
  int rc;

  (void)state;
  lc_error_init(&error);
  memset(&update_res, 0, sizeof(update_res));
  memset(&get_res, 0, sizeof(get_res));
  memset(&doc, 0, sizeof(doc));
  make_root("client-state", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/client/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  write_client_state(client, key, "{\"value\":42}", NULL, 0L, 0, &update_res,
                     &error);
  assert_string_equal(update_res.new_state_etag, "pouch-state-1");
  assert_int_equal(update_res.new_version, 1L);
  assert_int_equal(update_res.bytes, strlen("{\"value\":42}"));
  lc_client_close(client);

  open_pouch_client(root, &reader, &error);
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = reader->get(reader, key, NULL, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  assert_string_equal(get_res.content_type, "application/json");
  assert_string_equal(get_res.etag, "pouch-state-1");
  assert_int_equal(get_res.version, 1L);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, strlen("{\"value\":42}"));
  assert_memory_equal(bytes, "{\"value\":42}", strlen("{\"value\":42}"));
  sink->close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);
  memset(&get_res, 0, sizeof(get_res));

  rc = reader->load(reader, key, &pouch_value_map, &doc, NULL, &get_res,
                    &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  assert_int_equal(doc.value, 42);
  assert_string_equal(get_res.etag, "pouch-state-1");

  lc_get_res_cleanup(&get_res);
  lc_update_res_cleanup(&update_res);
  lc_client_close(reader);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_client_update_enforces_state_preconditions(void **state) {
  lc_client *client;
  lc_update_res first;
  lc_update_res second;
  lc_source *source;
  lc_update_req update_req;
  lc_error error;
  char root[512];
  int rc;

  (void)state;
  lc_error_init(&error);
  memset(&first, 0, sizeof(first));
  memset(&second, 0, sizeof(second));
  make_root("client-preconditions", root, sizeof(root));
  cleanup_root(root);

  open_pouch_client(root, &client, &error);
  write_client_state(client, "state/current", "{\"value\":1}", NULL, 0L, 0,
                     &first, &error);

  lc_update_req_init(&update_req);
  update_req.lease.key = "state/current";
  update_req.if_state_etag = "wrong";
  rc = lc_source_from_memory("{\"value\":2}", strlen("{\"value\":2}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->update(client, &update_req, source, &second, &error);
  source->close(source);
  assert_int_equal(rc, LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  lc_update_req_init(&update_req);
  update_req.lease.key = "state/current";
  update_req.if_state_etag = first.new_state_etag;
  update_req.if_version = 99L;
  update_req.has_if_version = 1;
  rc = lc_source_from_memory("{\"value\":2}", strlen("{\"value\":2}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->update(client, &update_req, source, &second, &error);
  source->close(source);
  assert_int_equal(rc, LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  write_client_state(client, "state/current", "{\"value\":2}",
                     first.new_state_etag, 1L, 1, &second, &error);
  assert_string_equal(second.new_state_etag, "pouch-state-2");
  assert_int_equal(second.new_version, 2L);

  lc_update_res_cleanup(&first);
  lc_update_res_cleanup(&second);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_client_mutate_applies_plan_and_preconditions(void **state) {
  lc_client *client;
  lc_sink *sink;
  lc_update_res update_res;
  lc_mutate_op mutate_op;
  lc_mutate_res mutate_res;
  lc_get_res get_res;
  lc_error error;
  const char *mutations[3];
  const void *bytes;
  size_t length;
  char root[512];
  char key[96];
  int rc;

  (void)state;
  client = NULL;
  sink = NULL;
  memset(&update_res, 0, sizeof(update_res));
  lc_mutate_op_init(&mutate_op);
  memset(&mutate_res, 0, sizeof(mutate_res));
  memset(&get_res, 0, sizeof(get_res));
  lc_error_init(&error);
  make_root("client-mutate", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/mutate/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  write_client_state(client, key, "{\"counter\":1,\"drop\":true}", NULL, 0L,
                     0, &update_res, &error);

  mutations[0] = "/counter++";
  mutations[1] = "/status=\"ok\"";
  mutations[2] = "rm:/drop";
  mutate_op.lease.key = key;
  mutate_op.mutations = mutations;
  mutate_op.mutation_count = 3U;
  mutate_op.if_state_etag = update_res.new_state_etag;
  mutate_op.if_version = update_res.new_version;
  mutate_op.has_if_version = 1;
  rc = client->mutate(client, &mutate_op, &mutate_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(mutate_res.new_version, 2L);
  assert_string_equal(mutate_res.new_state_etag, "pouch-state-2");
  assert_true(mutate_res.bytes > 0L);

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get(client, key, NULL, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(bytes_contain_text(bytes, length, "\"counter\":2"));
  assert_true(bytes_contain_text(bytes, length, "\"status\":\"ok\""));
  assert_false(bytes_contain_text(bytes, length, "\"drop\""));
  sink->close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);

  lc_mutate_res_cleanup(&mutate_res);
  memset(&mutate_res, 0, sizeof(mutate_res));
  lc_error_cleanup(&error);
  lc_error_init(&error);
  mutate_op.if_state_etag = update_res.new_state_etag;
  mutate_op.if_version = update_res.new_version;
  rc = client->mutate(client, &mutate_op, &mutate_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_non_null(strstr(error.message, "precondition"));

  lc_mutate_res_cleanup(&mutate_res);
  lc_update_res_cleanup(&update_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_client_get_missing_and_public_state_behavior(void **state) {
  lc_client *client;
  lc_source *source;
  lc_sink *sink;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lc_update_req update_req;
  lc_update_res update_res;
  pouch_value_doc loaded;
  lc_error error;
  const void *bytes;
  size_t length;
  char root[512];
  int rc;

  (void)state;
  client = NULL;
  source = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);
  memset(&get_res, 0, sizeof(get_res));
  memset(&update_res, 0, sizeof(update_res));
  memset(&loaded, 0, sizeof(loaded));
  lc_get_opts_init(&get_opts);
  lc_update_req_init(&update_req);
  make_root("client-missing", root, sizeof(root));
  cleanup_root(root);

  open_pouch_client(root, &client, &error);
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get(client, "missing", NULL, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(get_res.no_content);
  lc_get_res_cleanup(&get_res);
  sink->close(sink);
  sink = NULL;

  memset(&get_res, 0, sizeof(get_res));
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  get_opts.public_read = 1;
  rc = client->get(client, "missing", &get_opts, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(get_res.no_content);
  sink->close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);

  update_req.lease.key = "public/state";
  rc = lc_source_from_memory("{\"value\":42}", strlen("{\"value\":42}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->update(client, &update_req, source, &update_res, &error);
  assert_int_equal(rc, LC_OK);
  source->close(source);
  source = NULL;
  lc_update_res_cleanup(&update_res);

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get(client, "public/state", &get_opts, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  assert_string_equal(get_res.content_type, "application/json");
  assert_string_equal(get_res.etag, "pouch-state-1");
  assert_int_equal(get_res.version, 1L);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(bytes_contain_text(bytes, length, "\"value\":42"));
  sink->close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);

  rc = client->load(client, "public/state", &pouch_value_map, &loaded,
                    &get_opts, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  assert_int_equal(loaded.value, 42);
  lc_get_res_cleanup(&get_res);
  if (source != NULL) {
    source->close(source);
  }
  if (sink != NULL) {
    sink->close(sink);
  }
  lc_update_res_cleanup(&update_res);
  if (client != NULL) {
    lc_client_close(client);
  }
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_client_attachments_roundtrip_and_delete(void **state) {
  lc_client *client;
  lc_source *source;
  lc_sink *sink;
  lc_attach_op attach_op;
  lc_attach_res attach_res;
  lc_attachment_list_req list_req;
  lc_attachment_list list;
  lc_attachment_get_op get_op;
  lc_attachment_get_res get_res;
  lc_attachment_delete_op delete_op;
  lc_attachment_delete_all_op delete_all_op;
  lc_error error;
  const void *bytes;
  size_t length;
  int deleted;
  int deleted_count;
  char root[512];
  char key[96];
  char beta_id[256];
  int rc;

  (void)state;
  client = NULL;
  source = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  deleted = 0;
  deleted_count = 0;
  memset(&attach_res, 0, sizeof(attach_res));
  memset(&list, 0, sizeof(list));
  memset(&get_res, 0, sizeof(get_res));
  lc_attach_op_init(&attach_op);
  lc_attachment_list_req_init(&list_req);
  lc_attachment_get_op_init(&get_op);
  lc_attachment_delete_op_init(&delete_op);
  lc_attachment_delete_all_op_init(&delete_all_op);
  lc_error_init(&error);
  make_root("client-attachments", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/attachments/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  attach_op.lease.key = key;
  attach_op.name = "alpha.txt";
  attach_op.content_type = "text/plain";
  attach_op.prevent_overwrite = 1;
  rc = lc_source_from_memory("alpha", strlen("alpha"), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->attach(client, &attach_op, source, &attach_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  assert_string_equal(attach_res.attachment.name, "alpha.txt");
  assert_string_equal(attach_res.attachment.content_type, "text/plain");
  assert_int_equal(attach_res.attachment.size, 5L);
  assert_non_null(attach_res.attachment.id);
  lc_attach_res_cleanup(&attach_res);

  rc = lc_source_from_memory("again", strlen("again"), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->attach(client, &attach_op, source, &attach_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_non_null(strstr(error.message, "already exists"));
  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_attach_res_cleanup(&attach_res);

  attach_op.name = "beta.bin";
  attach_op.content_type = "application/octet-stream";
  attach_op.prevent_overwrite = 0;
  rc = lc_source_from_memory("beta", strlen("beta"), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->attach(client, &attach_op, source, &attach_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  snprintf(beta_id, sizeof(beta_id), "%s", attach_res.attachment.id);
  lc_attach_res_cleanup(&attach_res);

  list_req.lease.key = key;
  rc = client->list_attachments(client, &list_req, &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 2U);
  assert_string_equal(list.items[0].name, "alpha.txt");
  assert_string_equal(list.items[1].name, "beta.bin");
  assert_string_equal(list.items[1].id, beta_id);
  lc_attachment_list_cleanup(&list);

  list_req.public_read = 1;
  rc = client->list_attachments(client, &list_req, &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 2U);
  assert_string_equal(list.items[0].name, "alpha.txt");
  assert_string_equal(list.items[1].name, "beta.bin");
  lc_attachment_list_cleanup(&list);

  get_op.lease.key = key;
  get_op.selector.name = "alpha.txt";
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get_attachment(client, &get_op, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(get_res.attachment.name, "alpha.txt");
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, strlen("alpha"));
  assert_memory_equal(bytes, "alpha", strlen("alpha"));
  sink->close(sink);
  sink = NULL;
  lc_attachment_get_res_cleanup(&get_res);

  get_op.public_read = 1;
  get_op.selector.name = "alpha.txt";
  get_op.selector.id = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get_attachment(client, &get_op, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(get_res.attachment.name, "alpha.txt");
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, strlen("alpha"));
  assert_memory_equal(bytes, "alpha", strlen("alpha"));
  sink->close(sink);
  sink = NULL;
  lc_attachment_get_res_cleanup(&get_res);
  get_op.public_read = 0;

  get_op.selector.name = NULL;
  get_op.selector.id = beta_id;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get_attachment(client, &get_op, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(get_res.attachment.name, "beta.bin");
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, strlen("beta"));
  assert_memory_equal(bytes, "beta", strlen("beta"));
  sink->close(sink);
  sink = NULL;
  lc_attachment_get_res_cleanup(&get_res);

  delete_op.lease.key = key;
  delete_op.selector.name = "alpha.txt";
  rc = client->delete_attachment(client, &delete_op, &deleted, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(deleted, 1);
  list_req.public_read = 1;
  rc = client->list_attachments(client, &list_req, &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 1U);
  assert_string_equal(list.items[0].name, "beta.bin");
  lc_attachment_list_cleanup(&list);
  get_op.public_read = 1;
  get_op.selector.name = "alpha.txt";
  get_op.selector.id = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get_attachment(client, &get_op, sink, &get_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_non_null(strstr(error.message, "not found"));
  sink->close(sink);
  sink = NULL;
  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_attachment_get_res_cleanup(&get_res);
  get_op.public_read = 0;
  list_req.public_read = 0;
  deleted = 0;
  rc = client->delete_attachment(client, &delete_op, &deleted, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(deleted, 0);

  delete_all_op.lease.key = key;
  rc = client->delete_all_attachments(client, &delete_all_op, &deleted_count,
                                      &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(deleted_count, 1);
  rc = client->list_attachments(client, &list_req, &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 0U);

  if (sink != NULL) {
    sink->close(sink);
  }
  if (source != NULL) {
    source->close(source);
  }
  lc_attachment_get_res_cleanup(&get_res);
  lc_attachment_list_cleanup(&list);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_client_queue_enqueue_dequeue_ack_and_nack(void **state) {
  lc_client *client;
  lc_source *source;
  lc_sink *sink;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats_res;
  lc_dequeue_req dequeue_req;
  lc_message *message;
  lc_extend_req extend_req;
  lc_nack_req nack_req;
  lc_ack_res ack_res;
  lc_error error;
  const void *bytes;
  size_t length;
  char root[512];
  int rc;

  (void)state;
  client = NULL;
  source = NULL;
  sink = NULL;
  message = NULL;
  bytes = NULL;
  length = 0U;
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_queue_stats_req_init(&stats_req);
  memset(&stats_res, 0, sizeof(stats_res));
  lc_dequeue_req_init(&dequeue_req);
  lc_extend_req_init(&extend_req);
  lc_nack_req_init(&nack_req);
  memset(&ack_res, 0, sizeof(ack_res));
  lc_error_init(&error);
  make_root("client-queue", root, sizeof(root));
  cleanup_root(root);

  open_pouch_client(root, &client, &error);
  enqueue_req.queue = "jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.max_attempts = 3;
  rc = lc_source_from_memory("job-1", strlen("job-1"), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  assert_string_equal(enqueue_res.queue, "jobs");
  assert_non_null(enqueue_res.message_id);
  assert_int_equal(enqueue_res.payload_bytes, 5L);

  stats_req.queue = "jobs";
  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats_res.available, 1);
  assert_string_equal(stats_res.head_message_id, enqueue_res.message_id);
  lc_queue_stats_res_cleanup(&stats_res);

  dequeue_req.queue = "jobs";
  dequeue_req.owner = "worker-a";
  dequeue_req.visibility_timeout_seconds = 60L;
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(message);
  assert_string_equal(message->queue, "jobs");
  assert_string_equal(message->message_id, enqueue_res.message_id);
  assert_int_equal(message->attempts, 1);
  assert_string_equal(message->payload_content_type, "text/plain");

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = message->write_payload(message, sink, NULL, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, strlen("job-1"));
  assert_memory_equal(bytes, "job-1", strlen("job-1"));
  sink->close(sink);
  sink = NULL;

  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats_res.available, 0);
  lc_queue_stats_res_cleanup(&stats_res);

  extend_req.extend_by_seconds = 90L;
  rc = message->extend(message, &extend_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(message->visibility_timeout_seconds, 90L);

  nack_req.delay_seconds = 0L;
  nack_req.intent = LC_NACK_INTENT_DEFER;
  rc = message->nack(message, &nack_req, &error);
  assert_int_equal(rc, LC_OK);
  message = NULL;

  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats_res.available, 1);
  lc_queue_stats_res_cleanup(&stats_res);

  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(message);
  assert_int_equal(message->attempts, 2);

  {
    lc_ack_op ack_op;

    memset(&ack_op, 0, sizeof(ack_op));
    ack_op.message.namespace_name = message->namespace_name;
    ack_op.message.queue = message->queue;
    ack_op.message.message_id = message->message_id;
    ack_op.message.lease_id = message->lease_id;
    ack_op.message.fencing_token = message->fencing_token;
    ack_op.message.meta_etag = message->meta_etag;
    rc = client->queue_ack(client, &ack_op, &ack_res, &error);
    assert_int_equal(rc, LC_OK);
    assert_int_equal(ack_res.acked, 1);
  }
  message->close(message);
  message = NULL;

  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats_res.available, 0);
  assert_int_equal(stats_res.pending_candidates, 0);

  lc_ack_res_cleanup(&ack_res);
  lc_queue_stats_res_cleanup(&stats_res);
  lc_enqueue_res_cleanup(&enqueue_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_txn_decisions_apply_queue_side_effects(void **state) {
  lc_client *client;
  lc_source *source;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats_res;
  lc_ack_op ack_op;
  lc_ack_res ack_res;
  lc_txn_decision_req decision_req;
  lc_txn_decision_res decision_res;
  lc_message *message;
  lc_error error;
  char root[512];
  int rc;

  (void)state;
  client = NULL;
  source = NULL;
  message = NULL;
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_dequeue_req_init(&dequeue_req);
  lc_queue_stats_req_init(&stats_req);
  memset(&stats_res, 0, sizeof(stats_res));
  memset(&ack_op, 0, sizeof(ack_op));
  memset(&ack_res, 0, sizeof(ack_res));
  lc_txn_decision_req_init(&decision_req);
  memset(&decision_res, 0, sizeof(decision_res));
  lc_error_init(&error);
  make_root("txn-queue", root, sizeof(root));
  cleanup_root(root);

  open_pouch_client(root, &client, &error);
  enqueue_req.queue = "txn-jobs";
  enqueue_req.visibility_timeout_seconds = 120L;
  rc = lc_source_from_memory("commit-job", strlen("commit-job"), &source,
                             &error);
  assert_int_equal(rc, LC_OK);
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_enqueue_res_cleanup(&enqueue_res);

  dequeue_req.queue = "txn-jobs";
  dequeue_req.owner = "worker-txn";
  dequeue_req.txn_id = "txn-queue-commit";
  dequeue_req.visibility_timeout_seconds = 120L;
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(message);
  assert_string_equal(message->txn_id, "txn-queue-commit");

  ack_op.message.namespace_name = message->namespace_name;
  ack_op.message.queue = message->queue;
  ack_op.message.message_id = message->message_id;
  ack_op.message.lease_id = message->lease_id;
  ack_op.message.txn_id = message->txn_id;
  ack_op.message.fencing_token = message->fencing_token;
  ack_op.message.meta_etag = message->meta_etag;
  rc = client->queue_ack(client, &ack_op, &ack_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(ack_res.acked, 1);
  lc_ack_res_cleanup(&ack_res);

  stats_req.queue = "txn-jobs";
  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats_res.available, 0);
  assert_int_equal(stats_res.pending_candidates, 1);
  lc_queue_stats_res_cleanup(&stats_res);

  decision_req.txn_id = "txn-queue-commit";
  rc = client->txn_commit(client, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_txn_decision_res_cleanup(&decision_res);

  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats_res.available, 0);
  assert_int_equal(stats_res.pending_candidates, 0);
  lc_queue_stats_res_cleanup(&stats_res);
  message->close(message);
  message = NULL;

  rc = lc_source_from_memory("rollback-job", strlen("rollback-job"), &source,
                             &error);
  assert_int_equal(rc, LC_OK);
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_enqueue_res_cleanup(&enqueue_res);

  dequeue_req.txn_id = "txn-queue-rollback";
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(message);
  assert_string_equal(message->txn_id, "txn-queue-rollback");

  memset(&ack_op, 0, sizeof(ack_op));
  memset(&ack_res, 0, sizeof(ack_res));
  ack_op.message.namespace_name = message->namespace_name;
  ack_op.message.queue = message->queue;
  ack_op.message.message_id = message->message_id;
  ack_op.message.lease_id = message->lease_id;
  ack_op.message.txn_id = message->txn_id;
  ack_op.message.fencing_token = message->fencing_token;
  ack_op.message.meta_etag = message->meta_etag;
  rc = client->queue_ack(client, &ack_op, &ack_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(ack_res.acked, 1);
  lc_ack_res_cleanup(&ack_res);

  decision_req.txn_id = "txn-queue-rollback";
  rc = client->txn_rollback(client, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_txn_decision_res_cleanup(&decision_res);

  ack_op.message.txn_id = NULL;
  memset(&ack_res, 0, sizeof(ack_res));
  rc = client->queue_ack(client, &ack_op, &ack_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(ack_res.acked, 1);
  lc_ack_res_cleanup(&ack_res);
  message->close(message);
  message = NULL;

  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_txn_decisions_stage_state_update_mutate_and_index_refresh(
    void **state) {
  static const char selector[] =
      "{\"eq\":{\"field\":\"/category\",\"value\":\"planning\"}}";
  const char *mutations[2];
  lc_client *client;
  lc_pouch *pouch;
  lc_source *source;
  lc_update_req update_req;
  lc_update_res update_res;
  lc_mutate_op mutate_op;
  lc_mutate_res mutate_res;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_query_key_handler handler;
  pouch_query_key_capture before_commit;
  pouch_query_key_capture after_commit;
  pouch_query_key_capture after_rollback;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats_res;
  lc_ack_op ack_op;
  lc_ack_res ack_res;
  lc_txn_participant participant;
  lc_txn_decision_req decision_req;
  lc_txn_decision_res decision_res;
  lc_pouch_state_read_result read_result;
  lc_message *message;
  lc_error error;
  char root[512];
  char state_bytes[256];
  int rc;

  (void)state;
  client = NULL;
  pouch = NULL;
  source = NULL;
  message = NULL;
  lc_update_req_init(&update_req);
  memset(&update_res, 0, sizeof(update_res));
  lc_mutate_op_init(&mutate_op);
  memset(&mutate_res, 0, sizeof(mutate_res));
  lc_query_req_init(&query_req);
  memset(&query_res, 0, sizeof(query_res));
  memset(&handler, 0, sizeof(handler));
  memset(&before_commit, 0, sizeof(before_commit));
  memset(&after_commit, 0, sizeof(after_commit));
  memset(&after_rollback, 0, sizeof(after_rollback));
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_dequeue_req_init(&dequeue_req);
  lc_queue_stats_req_init(&stats_req);
  memset(&stats_res, 0, sizeof(stats_res));
  memset(&ack_op, 0, sizeof(ack_op));
  memset(&ack_res, 0, sizeof(ack_res));
  memset(&participant, 0, sizeof(participant));
  lc_txn_decision_req_init(&decision_req);
  memset(&decision_res, 0, sizeof(decision_res));
  memset(&read_result, 0, sizeof(read_result));
  lc_error_init(&error);
  make_root("txn-state-index-mixed", root, sizeof(root));
  cleanup_root(root);

  open_pouch_client(root, &client, &error);
  update_req.lease.namespace_name = "docs/txn-index";
  update_req.lease.key = "doc/txn";
  update_req.lease.txn_id = "txn-state-index";
  update_req.content_type = "application/json";
  rc = lc_source_from_memory(
      "{\"category\":\"planning\",\"counter\":41,\"kind\":\"txn\"}",
      strlen("{\"category\":\"planning\",\"counter\":41,\"kind\":\"txn\"}"),
      &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->update(client, &update_req, source, &update_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  mutations[0] = "/counter++";
  mutations[1] = "/status=\"mutated\"";
  mutate_op.lease.namespace_name = "docs/txn-index";
  mutate_op.lease.key = "doc/txn";
  mutate_op.lease.txn_id = "txn-state-index";
  mutate_op.mutations = mutations;
  mutate_op.mutation_count = 2U;
  mutate_op.if_version = update_res.new_version;
  mutate_op.has_if_version = 1;
  rc = client->mutate(client, &mutate_op, &mutate_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_update_res_cleanup(&update_res);
  lc_mutate_res_cleanup(&mutate_res);

  enqueue_req.namespace_name = "docs/txn-index";
  enqueue_req.queue = "txn-mixed";
  enqueue_req.visibility_timeout_seconds = 120L;
  rc = lc_source_from_memory("mixed-job", strlen("mixed-job"), &source,
                             &error);
  assert_int_equal(rc, LC_OK);
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_enqueue_res_cleanup(&enqueue_res);

  dequeue_req.namespace_name = "docs/txn-index";
  dequeue_req.queue = "txn-mixed";
  dequeue_req.owner = "worker-mixed";
  dequeue_req.txn_id = "txn-state-index";
  dequeue_req.visibility_timeout_seconds = 120L;
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(message);
  assert_string_equal(message->txn_id, "txn-state-index");
  ack_op.message.namespace_name = message->namespace_name;
  ack_op.message.queue = message->queue;
  ack_op.message.message_id = message->message_id;
  ack_op.message.lease_id = message->lease_id;
  ack_op.message.txn_id = message->txn_id;
  ack_op.message.fencing_token = message->fencing_token;
  ack_op.message.meta_etag = message->meta_etag;
  rc = client->queue_ack(client, &ack_op, &ack_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(ack_res.acked, 1);
  lc_ack_res_cleanup(&ack_res);

  handler.begin = pouch_query_key_begin;
  handler.chunk = pouch_query_key_chunk;
  handler.end = pouch_query_key_end;
  query_req.namespace_name = "docs/txn-index";
  query_req.selector_json = selector;
  query_req.engine = "index";
  query_req.refresh = "wait_for";
  rc = client->query_keys(client, &query_req, &handler, &before_commit,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(before_commit.count, 0);
  lc_query_res_cleanup(&query_res);

  stats_req.namespace_name = "docs/txn-index";
  stats_req.queue = "txn-mixed";
  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats_res.pending_candidates, 1);
  lc_queue_stats_res_cleanup(&stats_res);

  participant.namespace_name = "docs/txn-index";
  participant.key = "doc/txn";
  participant.backend_hash = "backend-state";
  decision_req.txn_id = "txn-state-index";
  decision_req.participants = &participant;
  decision_req.participant_count = 1U;
  rc = client->txn_commit(client, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_txn_decision_res_cleanup(&decision_res);

  memset(&query_res, 0, sizeof(query_res));
  rc = client->query_keys(client, &query_req, &handler, &after_commit,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(after_commit.count, 1);
  assert_true(pouch_query_capture_has(&after_commit, "doc/txn"));
  lc_query_res_cleanup(&query_res);

  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats_res.pending_candidates, 0);
  lc_queue_stats_res_cleanup(&stats_res);

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, "docs/txn-index", "doc/txn", &read_result,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_result.found);
  read_source_to_string(read_result.body, state_bytes, sizeof(state_bytes));
  assert_true(bytes_contain_text(state_bytes, strlen(state_bytes),
                                 "\"counter\":42"));
  assert_true(bytes_contain_text(state_bytes, strlen(state_bytes),
                                 "\"status\":\"mutated\""));
  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_close(pouch);
  pouch = NULL;
  message->close(message);
  message = NULL;

  lc_update_req_init(&update_req);
  update_req.lease.namespace_name = "docs/txn-index";
  update_req.lease.key = "doc/rollback";
  update_req.lease.txn_id = "txn-state-rollback";
  update_req.content_type = "application/json";
  rc = lc_source_from_memory("{\"category\":\"planning\",\"counter\":7}",
                             strlen("{\"category\":\"planning\",\"counter\":7}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->update(client, &update_req, source, &update_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_update_res_cleanup(&update_res);

  participant.key = "doc/rollback";
  decision_req.txn_id = "txn-state-rollback";
  rc = client->txn_rollback(client, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_txn_decision_res_cleanup(&decision_res);

  memset(&query_res, 0, sizeof(query_res));
  rc = client->query_keys(client, &query_req, &handler, &after_rollback,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(after_rollback.count, 1);
  assert_true(pouch_query_capture_has(&after_rollback, "doc/txn"));
  assert_false(pouch_query_capture_has(&after_rollback, "doc/rollback"));
  lc_query_res_cleanup(&query_res);

  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_txn_recovery_applies_queue_side_effects(void **state) {
  lc_client *client;
  lc_pouch *pouch;
  lc_source *source;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats_res;
  lc_ack_op ack_op;
  lc_ack_res ack_res;
  lc_pouch_state_write_result write_result;
  lc_pouch_state_read_result read_result;
  lc_message *message;
  lc_error error;
  char root[512];
  const char *record;
  int rc;

  (void)state;
  client = NULL;
  pouch = NULL;
  source = NULL;
  message = NULL;
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_dequeue_req_init(&dequeue_req);
  lc_queue_stats_req_init(&stats_req);
  memset(&stats_res, 0, sizeof(stats_res));
  memset(&ack_op, 0, sizeof(ack_op));
  memset(&ack_res, 0, sizeof(ack_res));
  memset(&write_result, 0, sizeof(write_result));
  memset(&read_result, 0, sizeof(read_result));
  lc_error_init(&error);
  make_root("txn-queue-recovery", root, sizeof(root));
  cleanup_root(root);

  open_pouch_client(root, &client, &error);
  enqueue_req.queue = "txn-recover";
  enqueue_req.visibility_timeout_seconds = 120L;
  rc = lc_source_from_memory("recover-job", strlen("recover-job"), &source,
                             &error);
  assert_int_equal(rc, LC_OK);
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_enqueue_res_cleanup(&enqueue_res);

  dequeue_req.queue = "txn-recover";
  dequeue_req.owner = "worker-recover";
  dequeue_req.txn_id = "txn-queue-recover";
  dequeue_req.visibility_timeout_seconds = 120L;
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(message);

  ack_op.message.namespace_name = message->namespace_name;
  ack_op.message.queue = message->queue;
  ack_op.message.message_id = message->message_id;
  ack_op.message.lease_id = message->lease_id;
  ack_op.message.txn_id = message->txn_id;
  ack_op.message.fencing_token = message->fencing_token;
  ack_op.message.meta_etag = message->meta_etag;
  rc = client->queue_ack(client, &ack_op, &ack_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(ack_res.acked, 1);
  lc_ack_res_cleanup(&ack_res);
  message->close(message);
  message = NULL;
  lc_client_close(client);
  client = NULL;

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  record = "format pouch-txn-v1\nstate 636f6d6d6974\n"
           "expires_at_unix 0\ntc_term 1\n"
           "target_backend_hash \nparticipant_count 0\n";
  rc = lc_source_from_memory(record, strlen(record), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, ".lockd/txn", "txn/txn-queue-recover",
                            source, NULL, &write_result, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  open_pouch_client(root, &client, &error);
  stats_req.queue = "txn-recover";
  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats_res.available, 0);
  assert_int_equal(stats_res.pending_candidates, 0);
  lc_queue_stats_res_cleanup(&stats_res);
  lc_client_close(client);
  client = NULL;

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, ".lockd/txn", "txn/txn-queue-recover",
                           &read_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(read_result.found);
  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_client_queue_mutations_touch_notification_marker(
    void **state) {
  lc_client *client;
  lc_source *source;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_message *message;
  lc_error error;
  char root[512];
  char marker_path[1024];
  unsigned long enqueue_sequence;
  unsigned long dequeue_sequence;
  int rc;

  (void)state;
  client = NULL;
  source = NULL;
  message = NULL;
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_dequeue_req_init(&dequeue_req);
  lc_error_init(&error);
  make_root("client-queue-notify", root, sizeof(root));
  cleanup_root(root);

  open_pouch_client(root, &client, &error);
  enqueue_req.namespace_name = "team/notify";
  enqueue_req.queue = "jobs/main";
  enqueue_req.visibility_timeout_seconds = 30L;
  rc = lc_source_from_memory("job", strlen("job"), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  make_queue_notify_path(root, "team/notify", "jobs/main", marker_path,
                         sizeof(marker_path));
  assert_file_contains(marker_path, "queue=jobs%2fmain");
  enqueue_sequence = read_marker_sequence(marker_path, NULL);
  assert_true(enqueue_sequence > 0UL);

  dequeue_req.namespace_name = "team/notify";
  dequeue_req.queue = "jobs/main";
  dequeue_req.owner = "worker-notify";
  dequeue_req.visibility_timeout_seconds = 45L;
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(message);
  dequeue_sequence = read_marker_sequence(marker_path, NULL);
  assert_true(dequeue_sequence > enqueue_sequence);

  message->close(message);
  lc_enqueue_res_cleanup(&enqueue_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_client_queue_dequeue_batch_returns_page(void **state) {
  lc_client *client;
  lc_source *source;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_dequeue_batch_res batch;
  lc_error error;
  char root[512];
  int rc;

  (void)state;
  client = NULL;
  source = NULL;
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_dequeue_req_init(&dequeue_req);
  memset(&batch, 0, sizeof(batch));
  lc_error_init(&error);
  make_root("client-queue-batch", root, sizeof(root));
  cleanup_root(root);

  open_pouch_client(root, &client, &error);
  enqueue_req.queue = "batch";
  rc = lc_source_from_memory("one", strlen("one"), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_enqueue_res_cleanup(&enqueue_res);

  rc = lc_source_from_memory("two", strlen("two"), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  dequeue_req.queue = "batch";
  dequeue_req.page_size = 2;
  rc = client->dequeue_batch(client, &dequeue_req, &batch, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(batch.count, 2U);
  assert_non_null(batch.messages[0]);
  assert_non_null(batch.messages[1]);
  assert_string_equal(batch.messages[0]->queue, "batch");
  assert_string_equal(batch.messages[1]->queue, "batch");

  lc_dequeue_batch_cleanup(&batch);
  lc_enqueue_res_cleanup(&enqueue_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_client_queue_dequeue_with_state_uses_pouch_lease(
    void **state) {
  lc_client *client;
  lc_source *source;
  lc_sink *sink;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_message *message;
  lc_lease *state_lease;
  lc_get_res get_res;
  lc_error error;
  const void *bytes;
  size_t length;
  char root[512];
  char expected_key[256];
  int rc;

  (void)state;
  client = NULL;
  source = NULL;
  sink = NULL;
  message = NULL;
  state_lease = NULL;
  bytes = NULL;
  length = 0U;
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_dequeue_req_init(&dequeue_req);
  memset(&get_res, 0, sizeof(get_res));
  lc_error_init(&error);
  make_root("client-queue-state", root, sizeof(root));
  cleanup_root(root);

  open_pouch_client(root, &client, &error);
  enqueue_req.queue = "jobs";
  enqueue_req.content_type = "application/json";
  rc = lc_source_from_memory("{\"job\":1}", strlen("{\"job\":1}"), &source,
                             &error);
  assert_int_equal(rc, LC_OK);
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  dequeue_req.queue = "jobs";
  dequeue_req.owner = "worker-state";
  dequeue_req.visibility_timeout_seconds = 45L;
  rc = client->dequeue_with_state(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(message);
  assert_string_equal(message->message_id, enqueue_res.message_id);
  state_lease = message->state(message);
  assert_non_null(state_lease);
  snprintf(expected_key, sizeof(expected_key), "q/jobs/state/%s",
           enqueue_res.message_id);
  assert_string_equal(state_lease->key, expected_key);
  assert_string_equal(state_lease->owner, "worker-state");
  assert_int_equal(state_lease->version, 0L);
  assert_null(state_lease->state_etag);
  assert_int_equal(state_lease->lease_expires_at_unix,
                   message->not_visible_until_unix);

  rc = lc_source_from_memory("{\"handled\":true}",
                             strlen("{\"handled\":true}"), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = state_lease->update(state_lease, source, NULL, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  assert_int_equal(state_lease->version, 1L);
  assert_string_equal(state_lease->state_etag, "pouch-state-1");

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = state_lease->get(state_lease, sink, NULL, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  assert_string_equal(get_res.content_type, "application/json");
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, strlen("{\"handled\":true}"));
  assert_memory_equal(bytes, "{\"handled\":true}",
                      strlen("{\"handled\":true}"));
  sink->close(sink);
  sink = NULL;

  rc = message->ack(message, &error);
  assert_int_equal(rc, LC_OK);
  message = NULL;

  lc_get_res_cleanup(&get_res);
  lc_enqueue_res_cleanup(&enqueue_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_client_queue_ttl_and_retry_terminal_states(void **state) {
  lc_client *client;
  lc_source *source;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats_res;
  lc_message *message;
  lc_nack_op nack_op;
  lc_nack_res nack_res;
  lc_error error;
  char root[512];
  int rc;

  (void)state;
  client = NULL;
  source = NULL;
  message = NULL;
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_dequeue_req_init(&dequeue_req);
  lc_queue_stats_req_init(&stats_req);
  memset(&stats_res, 0, sizeof(stats_res));
  lc_nack_op_init(&nack_op);
  memset(&nack_res, 0, sizeof(nack_res));
  lc_error_init(&error);
  make_root("client-queue-terminal", root, sizeof(root));
  cleanup_root(root);

  open_pouch_client(root, &client, &error);

  enqueue_req.queue = "retry";
  enqueue_req.max_attempts = 1;
  rc = lc_source_from_memory("retry", strlen("retry"), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  dequeue_req.queue = "retry";
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(message);
  assert_int_equal(message->attempts, 1);

  nack_op.message.namespace_name = message->namespace_name;
  nack_op.message.queue = message->queue;
  nack_op.message.message_id = message->message_id;
  nack_op.message.lease_id = message->lease_id;
  nack_op.message.fencing_token = message->fencing_token;
  nack_op.message.meta_etag = message->meta_etag;
  nack_op.intent = LC_NACK_INTENT_FAILURE;
  rc = client->queue_nack(client, &nack_op, &nack_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(nack_res.requeued, 0);
  message->close(message);
  message = NULL;

  stats_req.queue = "retry";
  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats_res.available, 0);
  assert_int_equal(stats_res.pending_candidates, 0);
  lc_queue_stats_res_cleanup(&stats_res);

  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_OK);
  assert_null(message);

  lc_enqueue_res_cleanup(&enqueue_res);
  lc_nack_res_cleanup(&nack_res);
  lc_nack_op_init(&nack_op);
  memset(&nack_res, 0, sizeof(nack_res));

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "ttl";
  enqueue_req.ttl_seconds = 1L;
  enqueue_req.delay_seconds = 2L;
  rc = lc_source_from_memory("ttl", strlen("ttl"), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  sleep(2U);

  stats_req.queue = "ttl";
  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats_res.available, 0);
  assert_int_equal(stats_res.pending_candidates, 0);
  lc_queue_stats_res_cleanup(&stats_res);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "ttl";
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_OK);
  assert_null(message);

  lc_enqueue_res_cleanup(&enqueue_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

typedef struct pouch_subscribe_capture {
  int count;
  int saw_state;
} pouch_subscribe_capture;

static int pouch_subscribe_ack_handler(void *context, lc_message *message,
                                       lc_error *error) {
  pouch_subscribe_capture *capture;

  capture = (pouch_subscribe_capture *)context;
  assert_non_null(message);
  capture->count += 1;
  return message->ack(message, error);
}

static int pouch_subscribe_state_ack_handler(void *context, lc_message *message,
                                             lc_error *error) {
  pouch_subscribe_capture *capture;

  capture = (pouch_subscribe_capture *)context;
  assert_non_null(message);
  assert_non_null(message->state(message));
  capture->count += 1;
  capture->saw_state = 1;
  return message->ack(message, error);
}

static int pouch_subscribe_missing_terminal_handler(void *context,
                                                   lc_message *message,
                                                   lc_error *error) {
  pouch_subscribe_capture *capture;

  (void)error;
  capture = (pouch_subscribe_capture *)context;
  assert_non_null(message);
  capture->count += 1;
  return LC_OK;
}

static void test_client_queue_subscribe_polling_paths(void **state) {
  lc_client *client;
  lc_source *source;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req subscribe_req;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats_res;
  lc_consumer consumer;
  pouch_subscribe_capture capture;
  lc_error error;
  char root[512];
  int rc;

  (void)state;
  client = NULL;
  source = NULL;
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_dequeue_req_init(&subscribe_req);
  lc_queue_stats_req_init(&stats_req);
  memset(&stats_res, 0, sizeof(stats_res));
  lc_consumer_init(&consumer);
  memset(&capture, 0, sizeof(capture));
  lc_error_init(&error);
  make_root("client-queue-subscribe", root, sizeof(root));
  cleanup_root(root);

  open_pouch_client(root, &client, &error);
  enqueue_req.queue = "subscribe";
  rc = lc_source_from_memory("one", strlen("one"), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_enqueue_res_cleanup(&enqueue_res);
  rc = lc_source_from_memory("two", strlen("two"), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  subscribe_req.queue = "subscribe";
  subscribe_req.page_size = 2;
  consumer.handle = pouch_subscribe_ack_handler;
  consumer.context = &capture;
  rc = client->subscribe(client, &subscribe_req, &consumer, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 2);

  stats_req.queue = "subscribe";
  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats_res.available, 0);
  assert_int_equal(stats_res.pending_candidates, 0);
  lc_queue_stats_res_cleanup(&stats_res);
  lc_enqueue_res_cleanup(&enqueue_res);

  memset(&capture, 0, sizeof(capture));
  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "state-subscribe";
  rc = lc_source_from_memory("state", strlen("state"), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  lc_dequeue_req_init(&subscribe_req);
  subscribe_req.queue = "state-subscribe";
  consumer.handle = pouch_subscribe_state_ack_handler;
  consumer.context = &capture;
  rc = client->subscribe_with_state(client, &subscribe_req, &consumer, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1);
  assert_int_equal(capture.saw_state, 1);
  lc_enqueue_res_cleanup(&enqueue_res);

  memset(&capture, 0, sizeof(capture));
  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "missing-terminal";
  rc = lc_source_from_memory("missing", strlen("missing"), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  lc_dequeue_req_init(&subscribe_req);
  subscribe_req.queue = "missing-terminal";
  consumer.handle = pouch_subscribe_missing_terminal_handler;
  consumer.context = &capture;
  rc = client->subscribe(client, &subscribe_req, &consumer, &error);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_string_equal(
      error.message,
      "consumer callback must ack() or nack() before returning LC_OK");
  assert_int_equal(capture.count, 1);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  stats_req.queue = "missing-terminal";
  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats_res.available, 1);
  lc_queue_stats_res_cleanup(&stats_res);

  lc_enqueue_res_cleanup(&enqueue_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

typedef struct pouch_watch_capture {
  lc_client *client;
  const char *queue;
  int event_count;
  int saw_unavailable;
  int saw_available;
  char head_message_id[160];
} pouch_watch_capture;

typedef struct pouch_watch_txn_ack_capture {
  lc_client *client;
  const char *root_path;
  const char *queue;
  int fork_child;
  int event_count;
  int saw_available;
  int saw_unavailable_after_commit;
  char head_message_id[160];
} pouch_watch_txn_ack_capture;

static int pouch_watch_enqueue_on_initial_unavailable(
    void *context, const lc_watch_event *event, lc_error *error) {
  pouch_watch_capture *capture;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_source *source;
  int rc;

  capture = (pouch_watch_capture *)context;
  capture->event_count += 1;
  if (event->available) {
    capture->saw_available = 1;
    if (event->head_message_id != NULL) {
      snprintf(capture->head_message_id, sizeof(capture->head_message_id), "%s",
               event->head_message_id);
    }
    error->code = LC_ERR_TRANSPORT;
    error->message = strdup("pouch watch observed available event");
    assert_non_null(error->message);
    return 0;
  }
  capture->saw_unavailable = 1;
  if (capture->event_count > 1) {
    error->code = LC_ERR_TRANSPORT;
    error->message = strdup("pouch watch observed repeated unavailable event");
    assert_non_null(error->message);
    return 0;
  }
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  source = NULL;
  enqueue_req.queue = capture->queue;
  rc = lc_source_from_memory("watch", strlen("watch"), &source, error);
  if (rc == LC_OK) {
    rc = capture->client->enqueue(capture->client, &enqueue_req, source,
                                  &enqueue_res, error);
  }
  if (source != NULL) {
    source->close(source);
  }
  lc_enqueue_res_cleanup(&enqueue_res);
  return rc == LC_OK ? 1 : 0;
}

static int pouch_watch_commit_txn_ack_with_client(lc_client *client,
                                                  const char *queue,
                                                  const char *txn_id,
                                                  lc_error *error) {
  lc_dequeue_req dequeue_req;
  lc_ack_op ack_op;
  lc_ack_res ack_res;
  lc_txn_decision_req decision_req;
  lc_txn_decision_res decision_res;
  lc_message *message;
  int rc;

  lc_dequeue_req_init(&dequeue_req);
  memset(&ack_op, 0, sizeof(ack_op));
  memset(&ack_res, 0, sizeof(ack_res));
  lc_txn_decision_req_init(&decision_req);
  memset(&decision_res, 0, sizeof(decision_res));
  message = NULL;
  dequeue_req.queue = queue;
  dequeue_req.owner = "watch-txn-worker";
  dequeue_req.txn_id = txn_id;
  dequeue_req.visibility_timeout_seconds = 120L;
  rc = client->dequeue(client, &dequeue_req, &message, error);
  if (rc == LC_OK && message == NULL) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "pouch watch transaction dequeue returned no message",
                      NULL, NULL, NULL);
  }
  if (rc == LC_OK) {
    ack_op.message.namespace_name = message->namespace_name;
    ack_op.message.queue = message->queue;
    ack_op.message.message_id = message->message_id;
    ack_op.message.lease_id = message->lease_id;
    ack_op.message.txn_id = message->txn_id;
    ack_op.message.fencing_token = message->fencing_token;
    ack_op.message.meta_etag = message->meta_etag;
    rc = client->queue_ack(client, &ack_op, &ack_res, error);
  }
  if (rc == LC_OK && !ack_res.acked) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "pouch watch transaction ack was not accepted", NULL,
                      NULL, NULL);
  }
  if (rc == LC_OK) {
    decision_req.txn_id = txn_id;
    rc = client->txn_commit(client, &decision_req, &decision_res, error);
  }
  if (message != NULL) {
    message->close(message);
  }
  lc_ack_res_cleanup(&ack_res);
  lc_txn_decision_res_cleanup(&decision_res);
  return rc;
}

static int pouch_watch_commit_txn_ack_child(const char *root,
                                            const char *queue,
                                            const char *txn_id) {
  lc_client_config config;
  lc_client *client;
  const char *endpoints[1];
  char endpoint[540];
  lc_error error;
  int rc;

  client = NULL;
  lc_error_init(&error);
  make_endpoint(root, endpoint, sizeof(endpoint));
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  rc = lc_client_open(&config, &client, &error);
  if (rc == LC_OK) {
    rc = pouch_watch_commit_txn_ack_with_client(client, queue, txn_id, &error);
  }
  if (client != NULL) {
    lc_client_close(client);
  }
  lc_error_cleanup(&error);
  return rc;
}

static int pouch_watch_commit_txn_ack_in_child(const char *root,
                                               const char *queue,
                                               const char *txn_id,
                                               lc_error *error) {
  pid_t pid;
  int status;

  pid = fork();
  if (pid < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to fork pouch watch transaction child",
                        strerror(errno), NULL, NULL);
  }
  if (pid == 0) {
    int child_rc;

    child_rc = pouch_watch_commit_txn_ack_child(root, queue, txn_id);
    _exit(child_rc == LC_OK ? 0 : 1);
  }
  if (waitpid(pid, &status, 0) < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to wait for pouch watch transaction child",
                        strerror(errno), NULL, NULL);
  }
  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "pouch watch transaction child failed", NULL, NULL,
                        NULL);
  }
  return LC_OK;
}

static int pouch_watch_commit_txn_ack_on_initial_available(
    void *context, const lc_watch_event *event, lc_error *error) {
  pouch_watch_txn_ack_capture *capture;
  int rc;

  capture = (pouch_watch_txn_ack_capture *)context;
  capture->event_count += 1;
  if (capture->event_count == 1) {
    if (!event->available || event->head_message_id == NULL) {
      error->code = LC_ERR_TRANSPORT;
      error->message = strdup("pouch watch expected initial available event");
      assert_non_null(error->message);
      return 0;
    }
    capture->saw_available = 1;
    snprintf(capture->head_message_id, sizeof(capture->head_message_id), "%s",
             event->head_message_id);
    if (capture->fork_child) {
      rc = pouch_watch_commit_txn_ack_in_child(
          capture->root_path, capture->queue, "txn-watch-ack", error);
    } else {
      rc = pouch_watch_commit_txn_ack_with_client(
          capture->client, capture->queue, "txn-watch-ack", error);
    }
    return rc == LC_OK ? 1 : 0;
  }
  if (!event->available) {
    capture->saw_unavailable_after_commit = 1;
    error->code = LC_ERR_TRANSPORT;
    error->message = strdup("pouch watch observed transaction ack commit");
    assert_non_null(error->message);
    return 0;
  }
  error->code = LC_ERR_TRANSPORT;
  error->message = strdup("pouch watch expected unavailable event after txn");
  assert_non_null(error->message);
  return 0;
}

static void test_client_queue_watch_polling_detects_change(void **state) {
  lc_client *client;
  lc_watch_queue_req watch_req;
  lc_watch_handler handler;
  pouch_watch_capture capture;
  lc_error error;
  char root[512];
  int rc;

  (void)state;
  client = NULL;
  lc_watch_queue_req_init(&watch_req);
  lc_watch_handler_init(&handler);
  memset(&capture, 0, sizeof(capture));
  lc_error_init(&error);
  make_root("client-queue-watch", root, sizeof(root));
  cleanup_root(root);

  open_pouch_client(root, &client, &error);
  capture.client = client;
  capture.queue = "watch";
  watch_req.queue = "watch";
  handler.handle = pouch_watch_enqueue_on_initial_unavailable;
  handler.context = &capture;
  rc = client->watch_queue(client, &watch_req, &handler, &error);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_string_equal(error.message, "pouch watch observed available event");
  assert_int_equal(capture.event_count, 2);
  assert_int_equal(capture.saw_unavailable, 1);
  assert_int_equal(capture.saw_available, 1);
  assert_true(capture.head_message_id[0] != '\0');

  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_client_queue_watch_detects_transaction_ack_commit(
    void **state) {
  lc_client *client;
  lc_source *source;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_watch_queue_req watch_req;
  lc_watch_handler handler;
  pouch_watch_txn_ack_capture capture;
  lc_error error;
  char root[512];
  int rc;

  (void)state;
  client = NULL;
  source = NULL;
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_watch_queue_req_init(&watch_req);
  lc_watch_handler_init(&handler);
  memset(&capture, 0, sizeof(capture));
  lc_error_init(&error);
  make_root("client-queue-watch-txn", root, sizeof(root));
  cleanup_root(root);

  open_pouch_client(root, &client, &error);
  enqueue_req.queue = "watch-txn";
  enqueue_req.visibility_timeout_seconds = 120L;
  rc = lc_source_from_memory("watch-txn", strlen("watch-txn"), &source,
                             &error);
  assert_int_equal(rc, LC_OK);
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_enqueue_res_cleanup(&enqueue_res);

  capture.client = client;
  capture.queue = "watch-txn";
  watch_req.queue = "watch-txn";
  handler.handle = pouch_watch_commit_txn_ack_on_initial_available;
  handler.context = &capture;
  rc = client->watch_queue(client, &watch_req, &handler, &error);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_string_equal(error.message,
                      "pouch watch observed transaction ack commit");
  assert_int_equal(capture.event_count, 2);
  assert_int_equal(capture.saw_available, 1);
  assert_int_equal(capture.saw_unavailable_after_commit, 1);
  assert_true(capture.head_message_id[0] != '\0');

  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_client_queue_watch_detects_peer_transaction_ack_commit(
    void **state) {
  lc_client *watcher;
  lc_client *actor;
  lc_source *source;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_watch_queue_req watch_req;
  lc_watch_handler handler;
  pouch_watch_txn_ack_capture capture;
  lc_error error;
  char root[512];
  int rc;

  (void)state;
  watcher = NULL;
  actor = NULL;
  source = NULL;
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_watch_queue_req_init(&watch_req);
  lc_watch_handler_init(&handler);
  memset(&capture, 0, sizeof(capture));
  lc_error_init(&error);
  make_root("client-queue-watch-peer-txn", root, sizeof(root));
  cleanup_root(root);

  open_pouch_client(root, &watcher, &error);
  open_pouch_client(root, &actor, &error);
  enqueue_req.queue = "watch-peer-txn";
  enqueue_req.visibility_timeout_seconds = 120L;
  rc = lc_source_from_memory("watch-peer-txn", strlen("watch-peer-txn"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = actor->enqueue(actor, &enqueue_req, source, &enqueue_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_enqueue_res_cleanup(&enqueue_res);

  capture.client = actor;
  capture.queue = "watch-peer-txn";
  watch_req.queue = "watch-peer-txn";
  handler.handle = pouch_watch_commit_txn_ack_on_initial_available;
  handler.context = &capture;
  rc = watcher->watch_queue(watcher, &watch_req, &handler, &error);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_string_equal(error.message,
                      "pouch watch observed transaction ack commit");
  assert_int_equal(capture.event_count, 2);
  assert_int_equal(capture.saw_available, 1);
  assert_int_equal(capture.saw_unavailable_after_commit, 1);
  assert_true(capture.head_message_id[0] != '\0');

  lc_client_close(actor);
  lc_client_close(watcher);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_client_queue_watch_detects_forked_transaction_ack_commit(
    void **state) {
  lc_client *watcher;
  lc_source *source;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_watch_queue_req watch_req;
  lc_watch_handler handler;
  pouch_watch_txn_ack_capture capture;
  lc_error error;
  char root[512];
  int rc;

  (void)state;
  watcher = NULL;
  source = NULL;
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_watch_queue_req_init(&watch_req);
  lc_watch_handler_init(&handler);
  memset(&capture, 0, sizeof(capture));
  lc_error_init(&error);
  make_root("client-queue-watch-fork-txn", root, sizeof(root));
  cleanup_root(root);

  open_pouch_client(root, &watcher, &error);
  enqueue_req.queue = "watch-fork-txn";
  enqueue_req.visibility_timeout_seconds = 120L;
  rc = lc_source_from_memory("watch-fork-txn", strlen("watch-fork-txn"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = watcher->enqueue(watcher, &enqueue_req, source, &enqueue_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_enqueue_res_cleanup(&enqueue_res);

  capture.root_path = root;
  capture.queue = "watch-fork-txn";
  capture.fork_child = 1;
  watch_req.queue = "watch-fork-txn";
  handler.handle = pouch_watch_commit_txn_ack_on_initial_available;
  handler.context = &capture;
  rc = watcher->watch_queue(watcher, &watch_req, &handler, &error);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_string_equal(error.message,
                      "pouch watch observed transaction ack commit");
  assert_int_equal(capture.event_count, 2);
  assert_int_equal(capture.saw_available, 1);
  assert_int_equal(capture.saw_unavailable_after_commit, 1);
  assert_true(capture.head_message_id[0] != '\0');

  lc_client_close(watcher);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_client_remove_tombstones_state_and_enforces_preconditions(
    void **state) {
  lc_client *client;
  lc_sink *sink;
  lc_update_res update_res;
  lc_remove_op remove_op;
  lc_remove_res remove_res;
  lc_get_res get_res;
  lc_error error;
  char root[512];
  char key[96];
  int rc;

  (void)state;
  client = NULL;
  sink = NULL;
  memset(&update_res, 0, sizeof(update_res));
  memset(&remove_res, 0, sizeof(remove_res));
  memset(&get_res, 0, sizeof(get_res));
  lc_remove_op_init(&remove_op);
  lc_error_init(&error);
  make_root("client-remove", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/remove/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  write_client_state(client, key, "{\"value\":11}", NULL, 0L, 0, &update_res,
                     &error);

  remove_op.lease.key = key;
  remove_op.if_state_etag = "wrong";
  rc = client->remove(client, &remove_op, &remove_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(remove_res.removed, 0);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  lc_remove_op_init(&remove_op);
  memset(&remove_res, 0, sizeof(remove_res));
  remove_op.lease.key = key;
  remove_op.if_state_etag = update_res.new_state_etag;
  remove_op.if_version = update_res.new_version;
  remove_op.has_if_version = 1;
  rc = client->remove(client, &remove_op, &remove_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(remove_res.removed, 1);
  assert_int_equal(remove_res.new_version, 2L);

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get(client, key, NULL, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(get_res.no_content);
  sink->close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);

  lc_remove_op_init(&remove_op);
  memset(&remove_res, 0, sizeof(remove_res));
  remove_op.lease.key = "missing";
  rc = client->remove(client, &remove_op, &remove_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(remove_res.removed, 0);
  assert_int_equal(remove_res.new_version, 0L);

  lc_remove_res_cleanup(&remove_res);
  lc_update_res_cleanup(&update_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_state_mutations_touch_writer_marker(void **state) {
  lc_client *client;
  lc_sink *sink;
  lc_update_res update_res;
  lc_remove_op remove_op;
  lc_remove_res remove_res;
  lc_get_res get_res;
  lc_acquire_req acquire_req;
  lc_error error;
  pouch_acquire_for_update_state handler_state;
  const void *bytes;
  size_t length;
  size_t size1;
  size_t size2;
  size_t size3;
  char root[512];
  char key[96];
  char marker_path[1024];
  int rc;

  (void)state;
  client = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  size1 = 0U;
  size2 = 0U;
  size3 = 0U;
  memset(&update_res, 0, sizeof(update_res));
  memset(&remove_res, 0, sizeof(remove_res));
  memset(&get_res, 0, sizeof(get_res));
  lc_remove_op_init(&remove_op);
  lc_acquire_req_init(&acquire_req);
  lc_error_init(&error);
  make_root("state-marker", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/marker/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  write_client_state(client, key, "{\"value\":1}", NULL, 0L, 0, &update_res,
                     &error);
  find_single_marker_path(root, "default", marker_path, sizeof(marker_path));
  assert_int_equal(read_marker_sequence(marker_path, &size1), 1UL);
  lc_update_res_cleanup(&update_res);

  memset(&update_res, 0, sizeof(update_res));
  write_client_state(client, key, "{\"value\":2}", NULL, 0L, 0, &update_res,
                     &error);
  assert_int_equal(read_marker_sequence(marker_path, &size2), 2UL);
  assert_true(size1 != size2);
  lc_update_res_cleanup(&update_res);

  remove_op.lease.key = key;
  rc = client->remove(client, &remove_op, &remove_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(remove_res.removed, 1);
  assert_int_equal(read_marker_sequence(marker_path, &size3), 3UL);
  assert_true(size2 != size3);
  lc_remove_res_cleanup(&remove_res);

  memset(&handler_state, 0, sizeof(handler_state));
  handler_state.replacement = "{\"value\":4}";
  handler_state.observer = client;
  handler_state.key = key;
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire_for_update(client, &acquire_req,
                                  pouch_acquire_for_update_handler,
                                  &handler_state, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(handler_state.saw_staged_invisible, 1);
  assert_int_equal(read_marker_sequence(marker_path, NULL), 5UL);

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get(client, key, NULL, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, strlen("{\"value\":4}"));
  assert_memory_equal(bytes, "{\"value\":4}", strlen("{\"value\":4}"));
  sink->close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);

  lc_error_cleanup(&error);
  lc_error_init(&error);
  memset(&handler_state, 0, sizeof(handler_state));
  handler_state.expected_snapshot = "\"value\":4";
  handler_state.expected_visible_during_update = "{\"value\":4}";
  handler_state.replacement = "{\"value\":5}";
  handler_state.observer = client;
  handler_state.key = key;
  handler_state.fail = 1;
  rc = client->acquire_for_update(client, &acquire_req,
                                  pouch_acquire_for_update_handler,
                                  &handler_state, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(handler_state.saw_staged_invisible, 1);
  assert_int_equal(read_marker_sequence(marker_path, NULL), 7UL);

  lc_error_cleanup(&error);
  lc_error_init(&error);
  memset(&get_res, 0, sizeof(get_res));
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get(client, key, NULL, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, strlen("{\"value\":4}"));
  assert_memory_equal(bytes, "{\"value\":4}", strlen("{\"value\":4}"));
  sink->close(sink);

  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_lease_bound_state_update_get_and_release(void **state) {
  lc_client *client;
  lc_client *reader;
  lc_lease *lease;
  lc_source *source;
  lc_sink *sink;
  lc_acquire_req acquire_req;
  lc_release_req release_req;
  lc_get_res get_res;
  lc_error error;
  const void *bytes;
  size_t length;
  char root[512];
  char key[96];
  int rc;

  (void)state;
  client = NULL;
  reader = NULL;
  lease = NULL;
  source = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_release_req_init(&release_req);
  memset(&get_res, 0, sizeof(get_res));
  make_root("lease-state", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/lease/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);
  assert_string_equal(lease->key, key);
  assert_string_equal(lease->owner, "lc-unit-pouch");
  assert_int_equal(lease->version, 0L);

  rc = lc_source_from_memory("{\"value\":7}", strlen("{\"value\":7}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lease->update(lease, source, NULL, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 1L);
  assert_string_equal(lease->state_etag, "pouch-state-1");

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = lease->get(lease, sink, NULL, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  assert_string_equal(get_res.content_type, "application/json");
  assert_string_equal(get_res.etag, "pouch-state-1");
  assert_int_equal(get_res.version, 1L);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, strlen("{\"value\":7}"));
  assert_memory_equal(bytes, "{\"value\":7}", strlen("{\"value\":7}"));
  sink->close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);

  rc = lease->release(lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  lease = NULL;

  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 30L;
  acquire_req.if_not_exists = 1;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(lease);

  lc_client_close(client);
  client = NULL;

  lc_error_cleanup(&error);
  lc_error_init(&error);
  memset(&get_res, 0, sizeof(get_res));
  open_pouch_client(root, &reader, &error);
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = reader->get(reader, key, NULL, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  assert_int_equal(get_res.version, 1L);
  sink->close(sink);

  lc_get_res_cleanup(&get_res);
  lc_client_close(reader);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_lease_mutate_and_local_mutate_refresh_state(void **state) {
  lc_client *client;
  lc_lease *lease;
  lc_source *source;
  lc_sink *sink;
  lc_acquire_req acquire_req;
  lc_mutate_req mutate_req;
  lc_mutate_local_req local_req;
  lc_get_res get_res;
  lc_error error;
  const char *mutations[1];
  const char *local_mutations[1];
  const void *bytes;
  size_t length;
  char root[512];
  char key[96];
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  source = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_mutate_req_init(&mutate_req);
  lc_mutate_local_req_init(&local_req);
  memset(&get_res, 0, sizeof(get_res));
  make_root("lease-mutate", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/lease-mutate/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory("{\"counter\":1}", strlen("{\"counter\":1}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lease->update(lease, source, NULL, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 1L);

  mutations[0] = "/counter++";
  mutate_req.mutations = mutations;
  mutate_req.mutation_count = 1U;
  rc = lease->mutate(lease, &mutate_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 2L);
  assert_string_equal(lease->state_etag, "pouch-state-2");

  local_mutations[0] = "/label=\"local\"";
  local_req.mutations = local_mutations;
  local_req.mutation_count = 1U;
  rc = lease->mutate_local(lease, &local_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 3L);
  assert_string_equal(lease->state_etag, "pouch-state-3");

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = lease->get(lease, sink, NULL, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(bytes_contain_text(bytes, length, "\"counter\":2"));
  assert_true(bytes_contain_text(bytes, length, "\"label\":\"local\""));
  sink->close(sink);

  lc_get_res_cleanup(&get_res);
  lease->close(lease);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_lease_attachments_use_pouch_object_store(void **state) {
  lc_client *client;
  lc_lease *lease;
  lc_source *source;
  lc_sink *sink;
  lc_acquire_req acquire_req;
  lc_attach_req attach_req;
  lc_attach_res attach_res;
  lc_attachment_list list;
  lc_attachment_get_req get_req;
  lc_attachment_get_res get_res;
  lc_error error;
  const void *bytes;
  size_t length;
  int deleted_count;
  char root[512];
  char key[96];
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  source = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  deleted_count = 0;
  lc_acquire_req_init(&acquire_req);
  lc_attach_req_init(&attach_req);
  memset(&attach_res, 0, sizeof(attach_res));
  memset(&list, 0, sizeof(list));
  lc_attachment_get_req_init(&get_req);
  memset(&get_res, 0, sizeof(get_res));
  lc_error_init(&error);
  make_root("lease-attachments", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/lease-attachments/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);

  attach_req.name = "lease.txt";
  attach_req.content_type = "text/plain";
  rc = lc_source_from_memory("lease-body", strlen("lease-body"), &source,
                             &error);
  assert_int_equal(rc, LC_OK);
  rc = lease->attach(lease, &attach_req, source, &attach_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  assert_string_equal(attach_res.attachment.name, "lease.txt");
  lc_attach_res_cleanup(&attach_res);

  rc = lease->list_attachments(lease, &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 1U);
  assert_string_equal(list.items[0].name, "lease.txt");

  get_req.selector.name = "lease.txt";
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = lease->get_attachment(lease, &get_req, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, strlen("lease-body"));
  assert_memory_equal(bytes, "lease-body", strlen("lease-body"));
  sink->close(sink);

  rc = lease->delete_all_attachments(lease, &deleted_count, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(deleted_count, 1);

  lc_attachment_get_res_cleanup(&get_res);
  lc_attachment_list_cleanup(&list);
  lease->close(lease);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_lease_save_streams_mapped_json_and_replays_after_reopen(
    void **state) {
  lc_client *client;
  lc_client *reader;
  lc_lease *lease;
  lc_acquire_req acquire_req;
  lc_release_req release_req;
  lc_get_res get_res;
  lc_error error;
  pouch_value_doc saved;
  pouch_value_doc loaded;
  char root[512];
  char key[96];
  int rc;

  (void)state;
  client = NULL;
  reader = NULL;
  lease = NULL;
  memset(&get_res, 0, sizeof(get_res));
  memset(&saved, 0, sizeof(saved));
  memset(&loaded, 0, sizeof(loaded));
  lc_acquire_req_init(&acquire_req);
  lc_release_req_init(&release_req);
  lc_error_init(&error);
  make_root("lease-save", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/lease-save/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);

  saved.value = 42;
  rc = lease->save(lease, &pouch_value_map, &saved, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 1L);
  assert_string_equal(lease->state_etag, "pouch-state-1");

  rc = lease->load(lease, &pouch_value_map, &loaded, NULL, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  assert_int_equal(loaded.value, 42);
  assert_string_equal(get_res.content_type, "application/json");
  assert_string_equal(get_res.etag, "pouch-state-1");
  lc_get_res_cleanup(&get_res);

  rc = lease->release(lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  lease = NULL;
  lc_client_close(client);
  client = NULL;

  memset(&loaded, 0, sizeof(loaded));
  memset(&get_res, 0, sizeof(get_res));
  open_pouch_client(root, &reader, &error);
  rc = reader->load(reader, key, &pouch_value_map, &loaded, NULL, &get_res,
                    &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  assert_int_equal(loaded.value, 42);
  assert_string_equal(get_res.etag, "pouch-state-1");

  lc_get_res_cleanup(&get_res);
  lc_client_close(reader);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_lease_keepalive_and_release_use_local_lifecycle(
    void **state) {
  lc_client *client;
  lc_lease *lease;
  lc_source *source;
  lc_acquire_req acquire_req;
  lc_keepalive_req keepalive_req;
  lc_keepalive_op keepalive_op;
  lc_keepalive_res keepalive_res;
  lc_release_op release_op;
  lc_release_res release_res;
  lc_error error;
  char root[512];
  char key[96];
  long before;
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  source = NULL;
  memset(&keepalive_res, 0, sizeof(keepalive_res));
  memset(&release_res, 0, sizeof(release_res));
  lc_acquire_req_init(&acquire_req);
  lc_keepalive_req_init(&keepalive_req);
  lc_keepalive_op_init(&keepalive_op);
  lc_release_op_init(&release_op);
  lc_error_init(&error);
  make_root("lease-keepalive", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/lease-keepalive/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 30L;
  before = (long)time(NULL);
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(lease->lease_expires_at_unix >= before + 30L);

  rc = lc_source_from_memory("{\"value\":9}", strlen("{\"value\":9}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lease->update(lease, source, NULL, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 1L);
  assert_string_equal(lease->state_etag, "pouch-state-1");

  keepalive_req.ttl_seconds = 45L;
  before = (long)time(NULL);
  rc = lease->keepalive(lease, &keepalive_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(lease->lease_expires_at_unix >= before + 45L);
  assert_int_equal(lease->version, 1L);
  assert_string_equal(lease->state_etag, "pouch-state-1");

  keepalive_op.lease.namespace_name = lease->namespace_name;
  keepalive_op.lease.key = lease->key;
  keepalive_op.lease.lease_id = lease->lease_id;
  keepalive_op.lease.txn_id = lease->txn_id;
  keepalive_op.lease.fencing_token = lease->fencing_token;
  keepalive_op.ttl_seconds = 60L;
  before = (long)time(NULL);
  rc = client->keepalive(client, &keepalive_op, &keepalive_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(keepalive_res.lease_expires_at_unix >= before + 60L);
  assert_int_equal(keepalive_res.version, 1L);
  assert_string_equal(keepalive_res.state_etag, "pouch-state-1");
  lc_keepalive_res_cleanup(&keepalive_res);

  release_op.lease.namespace_name = lease->namespace_name;
  release_op.lease.key = lease->key;
  release_op.lease.lease_id = lease->lease_id;
  release_op.lease.txn_id = lease->txn_id;
  release_op.lease.fencing_token = lease->fencing_token;
  rc = client->release(client, &release_op, &release_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(release_res.released, 1);
  lc_release_res_cleanup(&release_res);

  rc = lease->release(lease, NULL, &error);
  assert_int_equal(rc, LC_OK);
  lease = NULL;

  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_acquire_rejects_non_positive_ttl(void **state) {
  lc_client *client;
  lc_lease *lease;
  lc_acquire_req acquire_req;
  lc_error error;
  char root[512];
  char key[96];
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  lc_acquire_req_init(&acquire_req);
  lc_error_init(&error);
  make_root("lease-invalid-ttl", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/lease-invalid-ttl/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 0L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(lease);

  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_lease_metadata_persists_query_hidden(void **state) {
  lc_client *client;
  lc_client *reader;
  lc_lease *lease;
  lc_source *source;
  lc_acquire_req acquire_req;
  lc_metadata_req metadata_req;
  lc_describe_req describe_req;
  lc_describe_res describe_res;
  lc_error error;
  char root[512];
  char key[96];
  int rc;

  (void)state;
  client = NULL;
  reader = NULL;
  lease = NULL;
  source = NULL;
  memset(&describe_res, 0, sizeof(describe_res));
  lc_acquire_req_init(&acquire_req);
  lc_metadata_req_init(&metadata_req);
  lc_describe_req_init(&describe_req);
  lc_error_init(&error);
  make_root("lease-metadata", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/lease-metadata/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("{\"value\":15}", strlen("{\"value\":15}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lease->update(lease, source, NULL, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 1L);
  assert_false(lease->has_query_hidden);

  metadata_req.has_query_hidden = 1;
  metadata_req.query_hidden = 1;
  rc = lease->metadata(lease, &metadata_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 2L);
  assert_string_equal(lease->state_etag, "pouch-state-1");
  assert_true(lease->has_query_hidden);
  assert_true(lease->query_hidden);

  rc = lc_source_from_memory("{\"value\":16}", strlen("{\"value\":16}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lease->update(lease, source, NULL, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 3L);
  assert_string_equal(lease->state_etag, "pouch-state-3");
  assert_true(lease->has_query_hidden);
  assert_true(lease->query_hidden);

  rc = lease->release(lease, NULL, &error);
  assert_int_equal(rc, LC_OK);
  lease = NULL;
  lc_client_close(client);
  client = NULL;

  open_pouch_client(root, &reader, &error);
  describe_req.key = key;
  rc = reader->describe(reader, &describe_req, &describe_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(describe_res.version, 3L);
  assert_string_equal(describe_res.state_etag, "pouch-state-3");
  assert_true(describe_res.has_query_hidden);
  assert_true(describe_res.query_hidden);
  lc_describe_res_cleanup(&describe_res);

  lc_acquire_req_init(&acquire_req);
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 30L;
  rc = reader->acquire(reader, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 3L);
  assert_true(lease->has_query_hidden);
  assert_true(lease->query_hidden);

  lc_metadata_req_init(&metadata_req);
  metadata_req.has_query_hidden = 1;
  metadata_req.query_hidden = 0;
  rc = lease->metadata(lease, &metadata_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 4L);
  assert_true(lease->has_query_hidden);
  assert_false(lease->query_hidden);

  rc = lease->release(lease, NULL, &error);
  assert_int_equal(rc, LC_OK);
  lease = NULL;
  lc_client_close(reader);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_client_metadata_enforces_version_precondition(void **state) {
  lc_client *client;
  lc_lease *lease;
  lc_source *source;
  lc_acquire_req acquire_req;
  lc_metadata_op metadata_op;
  lc_metadata_res metadata_res;
  lc_error error;
  char root[512];
  char key[96];
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  source = NULL;
  memset(&metadata_res, 0, sizeof(metadata_res));
  lc_acquire_req_init(&acquire_req);
  lc_metadata_op_init(&metadata_op);
  lc_error_init(&error);
  make_root("metadata-precondition", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/metadata-precondition/%ld",
           (long)getpid());

  open_pouch_client(root, &client, &error);
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("{\"value\":21}", strlen("{\"value\":21}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lease->update(lease, source, NULL, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  metadata_op.lease.namespace_name = lease->namespace_name;
  metadata_op.lease.key = lease->key;
  metadata_op.lease.lease_id = lease->lease_id;
  metadata_op.lease.txn_id = lease->txn_id;
  metadata_op.lease.fencing_token = lease->fencing_token;
  metadata_op.has_query_hidden = 1;
  metadata_op.query_hidden = 1;
  metadata_op.has_if_version = 1;
  metadata_op.if_version = 99L;
  rc = client->metadata(client, &metadata_op, &metadata_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_false(metadata_res.has_query_hidden);
  lc_metadata_res_cleanup(&metadata_res);

  rc = lease->release(lease, NULL, &error);
  assert_int_equal(rc, LC_OK);
  lease = NULL;
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_query_keys_scan_uses_liblql_and_query_hidden(void **state) {
  static const char selector[] =
      "{\"eq\":{\"field\":\"/category\",\"value\":\"planning\"}}";
  lc_client *client;
  lc_pouch *pouch;
  lc_source *source;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_query_key_handler handler;
  pouch_query_key_capture first_page;
  pouch_query_key_capture second_page;
  pouch_query_key_capture lql_page;
  pouch_query_key_capture conflict_page;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result write_result;
  lc_error error;
  char root[512];
  char cursor[64];
  int rc;

  (void)state;
  client = NULL;
  pouch = NULL;
  source = NULL;
  memset(&query_res, 0, sizeof(query_res));
  memset(&handler, 0, sizeof(handler));
  memset(&first_page, 0, sizeof(first_page));
  memset(&second_page, 0, sizeof(second_page));
  memset(&lql_page, 0, sizeof(lql_page));
  memset(&conflict_page, 0, sizeof(conflict_page));
  memset(&options, 0, sizeof(options));
  memset(&write_result, 0, sizeof(write_result));
  lc_query_req_init(&query_req);
  lc_error_init(&error);
  make_root("query-keys-scan", root, sizeof(root));
  cleanup_root(root);
  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory("{\"category\":\"planning\",\"n\":1}",
                             strlen("{\"category\":\"planning\",\"n\":1}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query", "doc/a", source, NULL,
                            &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"category\":\"planning\",\"n\":2}",
                             strlen("{\"category\":\"planning\",\"n\":2}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query", "doc/b", source, NULL,
                            &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"category\":\"finance\",\"n\":3}",
                             strlen("{\"category\":\"finance\",\"n\":3}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query", "doc/c", source, NULL,
                            &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  memset(&options, 0, sizeof(options));
  options.has_query_hidden = 1;
  options.query_hidden = 1;
  rc = lc_source_from_memory("{\"category\":\"planning\",\"n\":4}",
                             strlen("{\"category\":\"planning\",\"n\":4}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query", "doc/hidden", source,
                            &options, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"category\":\"planning\",\"n\":5}",
                             strlen("{\"category\":\"planning\",\"n\":5}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_stage_write(pouch, "docs/query", "doc/staged",
                                  "txn-query-hidden", source, NULL,
                                  &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  open_pouch_client(root, &client, &error);

  handler.begin = pouch_query_key_begin;
  handler.chunk = pouch_query_key_chunk;
  handler.end = pouch_query_key_end;
  query_req.namespace_name = "docs/query";
  query_req.engine = "scan";
  query_req.selector_json = selector;
  query_req.limit = 1L;
  rc = client->query_keys(client, &query_req, &handler, &first_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(first_page.count, 1);
  assert_non_null(query_res.cursor);
  assert_string_equal(query_res.return_mode, "keys");
  assert_string_equal(query_res.metadata_json, "{\"engine\":\"scan\"}");
  assert_true(query_res.index_seq > 0UL);

  snprintf(cursor, sizeof(cursor), "%s", query_res.cursor);
  query_req.cursor = cursor;
  lc_query_res_cleanup(&query_res);
  rc = client->query_keys(client, &query_req, &handler, &second_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(second_page.count, 1);
  assert_null(query_res.cursor);
  assert_true(pouch_query_capture_has(&first_page, "doc/a") ||
              pouch_query_capture_has(&second_page, "doc/a"));
  assert_true(pouch_query_capture_has(&first_page, "doc/b") ||
              pouch_query_capture_has(&second_page, "doc/b"));
  assert_false(pouch_query_capture_has(&first_page, "doc/hidden"));
  assert_false(pouch_query_capture_has(&second_page, "doc/hidden"));
  assert_false(pouch_query_capture_has(&first_page, "doc/staged"));
  assert_false(pouch_query_capture_has(&second_page, "doc/staged"));
  assert_false(pouch_query_capture_has(&first_page, "doc/c"));
  assert_false(pouch_query_capture_has(&second_page, "doc/c"));

  lc_query_res_cleanup(&query_res);

  lc_query_req_init(&query_req);
  query_req.namespace_name = "docs/query";
  query_req.engine = "scan";
  query_req.selector_lql = "eq{field=/category,value=planning}";
  query_req.limit = 10L;
  rc = client->query_keys(client, &query_req, &handler, &lql_page, &query_res,
                          &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lql_page.count, 2);
  assert_true(pouch_query_capture_has(&lql_page, "doc/a"));
  assert_true(pouch_query_capture_has(&lql_page, "doc/b"));
  assert_false(pouch_query_capture_has(&lql_page, "doc/hidden"));
  assert_false(pouch_query_capture_has(&lql_page, "doc/staged"));
  lc_query_res_cleanup(&query_res);

  lc_query_req_init(&query_req);
  query_req.namespace_name = "docs/query";
  query_req.engine = "scan";
  query_req.selector_json = selector;
  query_req.selector_lql = "eq{field=/category,value=planning}";
  rc = client->query_keys(client, &query_req, &handler, &conflict_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  lc_query_res_cleanup(&query_res);
  lc_error_cleanup(&error);
  lc_client_close(client);
  cleanup_root(root);
}

static void test_query_keys_enforces_lockd_limit_contract(void **state) {
  lc_client *client;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_query_key_handler handler;
  pouch_query_key_counter default_counter;
  pouch_query_key_counter capped_counter;
  lc_update_res update_res;
  lc_error error;
  char root[512];
  char key[64];
  char body[64];
  unsigned long index;
  int rc;

  (void)state;
  client = NULL;
  memset(&query_res, 0, sizeof(query_res));
  memset(&handler, 0, sizeof(handler));
  memset(&default_counter, 0, sizeof(default_counter));
  memset(&capped_counter, 0, sizeof(capped_counter));
  memset(&update_res, 0, sizeof(update_res));
  lc_query_req_init(&query_req);
  lc_error_init(&error);
  make_root("query-keys-limit-contract", root, sizeof(root));
  cleanup_root(root);
  open_pouch_client(root, &client, &error);

  for (index = 0UL; index < 1005UL; ++index) {
    snprintf(key, sizeof(key), "doc/%04lu", index);
    snprintf(body, sizeof(body), "{\"n\":%lu}", index);
    write_client_state(client, key, body, NULL, 0L, 0, &update_res, &error);
    lc_update_res_cleanup(&update_res);
  }

  handler.begin = pouch_query_count_begin;
  handler.chunk = pouch_query_count_chunk;
  handler.end = pouch_query_count_end;
  query_req.engine = "index";
  query_req.refresh = "wait_for";
  rc = client->query_keys(client, &query_req, &handler, &default_counter,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(default_counter.count, 100);
  assert_non_null(query_res.cursor);
  lc_query_res_cleanup(&query_res);

  lc_query_req_init(&query_req);
  query_req.engine = "index";
  query_req.refresh = "wait_for";
  query_req.limit = 2000L;
  rc = client->query_keys(client, &query_req, &handler, &capped_counter,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capped_counter.count, 1000);
  assert_non_null(query_res.cursor);

  lc_query_res_cleanup(&query_res);
  lc_error_cleanup(&error);
  lc_client_close(client);
  cleanup_root(root);
}

static void test_query_keys_index_summary_uses_sidecar_rows(void **state) {
  static const char selector[] =
      "{\"eq\":{\"field\":\"/category\",\"value\":\"planning\"}}";
  lc_client *client;
  lc_pouch *pouch;
  lc_source *source;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_query_key_handler handler;
  pouch_query_key_capture first_page;
  pouch_query_key_capture second_page;
  pouch_query_key_capture indexed_page;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result write_result;
  lc_pouch_state_write_result delete_result;
  lc_error error;
  char *namespace_path;
  char root[512];
  char cursor[64];
  int rc;

  (void)state;
  client = NULL;
  pouch = NULL;
  source = NULL;
  namespace_path = NULL;
  memset(&query_res, 0, sizeof(query_res));
  memset(&handler, 0, sizeof(handler));
  memset(&first_page, 0, sizeof(first_page));
  memset(&second_page, 0, sizeof(second_page));
  memset(&indexed_page, 0, sizeof(indexed_page));
  memset(&options, 0, sizeof(options));
  memset(&write_result, 0, sizeof(write_result));
  memset(&delete_result, 0, sizeof(delete_result));
  lc_query_req_init(&query_req);
  lc_error_init(&error);
  make_root("query-keys-index-summary", root, sizeof(root));
  cleanup_root(root);
  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory("{\"category\":\"planning\",\"n\":1}",
                             strlen("{\"category\":\"planning\",\"n\":1}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index", "doc/a", source, NULL,
                            &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"category\":\"finance\",\"n\":2}",
                             strlen("{\"category\":\"finance\",\"n\":2}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index", "doc/b", source, NULL,
                            &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  memset(&options, 0, sizeof(options));
  options.has_query_hidden = 1;
  options.query_hidden = 1;
  rc = lc_source_from_memory("{\"category\":\"planning\",\"n\":3}",
                             strlen("{\"category\":\"planning\",\"n\":3}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index", "doc/hidden", source,
                            &options, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"category\":\"planning\",\"n\":4}",
                             strlen("{\"category\":\"planning\",\"n\":4}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index", "doc/deleted", source,
                            NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  rc = lc_pouch_state_delete(pouch, "docs/query-index", "doc/deleted", NULL,
                             &delete_result, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &delete_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  open_pouch_client(root, &client, &error);
  handler.begin = pouch_query_key_begin;
  handler.chunk = pouch_query_key_chunk;
  handler.end = pouch_query_key_end;
  query_req.namespace_name = "docs/query-index";
  query_req.refresh = "wait_for";
  query_req.limit = 1L;
  rc = client->query_keys(client, &query_req, &handler, &first_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(first_page.count, 1);
  assert_non_null(query_res.cursor);
  assert_string_equal(query_res.return_mode, "keys");
  assert_non_null(query_res.metadata_json);
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index-summary\""));
  assert_true(query_res.index_seq > 0UL);
  namespace_path = lc_pouch_namespace_path(NULL, root, "docs/query-index");
  assert_non_null(namespace_path);
  assert_path_file_contains(namespace_path, "index/query.index",
                            "row_count=3");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "term_field_count=");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "term_field 2f63617465676f7279 ");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "term_field 2f6e ");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "term_index_complete=1");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "presence_index_complete=1");

  snprintf(cursor, sizeof(cursor), "%s", query_res.cursor);
  query_req.cursor = cursor;
  query_req.engine = "index";
  lc_query_res_cleanup(&query_res);
  rc = client->query_keys(client, &query_req, &handler, &second_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(second_page.count, 1);
  assert_null(query_res.cursor);
  assert_true(pouch_query_capture_has(&first_page, "doc/a") ||
              pouch_query_capture_has(&second_page, "doc/a"));
  assert_true(pouch_query_capture_has(&first_page, "doc/b") ||
              pouch_query_capture_has(&second_page, "doc/b"));
  assert_false(pouch_query_capture_has(&first_page, "doc/hidden"));
  assert_false(pouch_query_capture_has(&second_page, "doc/hidden"));
  assert_false(pouch_query_capture_has(&first_page, "doc/deleted"));
  assert_false(pouch_query_capture_has(&second_page, "doc/deleted"));
  lc_query_res_cleanup(&query_res);

  memset(&query_res, 0, sizeof(query_res));
  query_req.cursor = NULL;
  query_req.selector_json = selector;
  rc = client->query_keys(client, &query_req, &handler, &indexed_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(indexed_page.count, 1);
  assert_true(pouch_query_capture_has(&indexed_page, "doc/a"));
  assert_false(pouch_query_capture_has(&indexed_page, "doc/b"));
  assert_false(pouch_query_capture_has(&indexed_page, "doc/hidden"));
  assert_false(pouch_query_capture_has(&indexed_page, "doc/deleted"));
  assert_null(query_res.cursor);
  assert_non_null(query_res.metadata_json);
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));

  free(namespace_path);
  lc_query_res_cleanup(&query_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_query_keys_index_scalar_in_uses_array_postings(void **state) {
  static const char selector[] =
      "{\"in\":{\"field\":\"/tags[]\",\"any\":[\"planning\",\"finance\"]}}";
  lc_client *client;
  lc_pouch *pouch;
  lc_source *source;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_query_key_handler handler;
  pouch_query_key_capture first_page;
  pouch_query_key_capture second_page;
  pouch_query_key_capture exists_page;
  pouch_query_key_capture prefix_page;
  pouch_query_key_capture contains_page;
  pouch_query_key_capture iprefix_page;
  pouch_query_key_capture icontains_page;
  pouch_query_key_capture numeric_prefix_page;
  pouch_query_key_capture odd_nibble_contains_page;
  pouch_query_key_capture range_page;
  pouch_query_key_capture unsupported_page;
  lc_pouch_state_write_options hidden_options;
  lc_pouch_state_write_result write_result;
  lc_pouch_state_write_result delete_result;
  lc_error error;
  char *namespace_path;
  char root[512];
  char cursor[64];
  int rc;

  (void)state;
  client = NULL;
  pouch = NULL;
  source = NULL;
  namespace_path = NULL;
  memset(&query_res, 0, sizeof(query_res));
  memset(&handler, 0, sizeof(handler));
  memset(&first_page, 0, sizeof(first_page));
  memset(&second_page, 0, sizeof(second_page));
  memset(&exists_page, 0, sizeof(exists_page));
  memset(&prefix_page, 0, sizeof(prefix_page));
  memset(&contains_page, 0, sizeof(contains_page));
  memset(&iprefix_page, 0, sizeof(iprefix_page));
  memset(&icontains_page, 0, sizeof(icontains_page));
  memset(&numeric_prefix_page, 0, sizeof(numeric_prefix_page));
  memset(&odd_nibble_contains_page, 0, sizeof(odd_nibble_contains_page));
  memset(&range_page, 0, sizeof(range_page));
  memset(&unsupported_page, 0, sizeof(unsupported_page));
  memset(&hidden_options, 0, sizeof(hidden_options));
  memset(&write_result, 0, sizeof(write_result));
  memset(&delete_result, 0, sizeof(delete_result));
  lc_query_req_init(&query_req);
  lc_error_init(&error);
  make_root("query-keys-index-in", root, sizeof(root));
  cleanup_root(root);
  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory(
      "{\"tags\":[\"planning\",\"finance\"],\"n\":1}",
      strlen("{\"tags\":[\"planning\",\"finance\"],\"n\":1}"), &source,
      &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-in", "doc/a", source,
                            NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"tags\":[\"ops\"],\"n\":2}",
                             strlen("{\"tags\":[\"ops\"],\"n\":2}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-in", "doc/b", source,
                            NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"tags\":[\"finance\"],\"n\":3}",
                             strlen("{\"tags\":[\"finance\"],\"n\":3}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-in", "doc/c", source,
                            NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"n\":2.5}", strlen("{\"n\":2.5}"), &source,
                             &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-in", "doc/decimal",
                            source, NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"needle\":\"\\u0006\\u0016 \"}",
                             strlen("{\"needle\":\"\\u0006\\u0016 \"}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-in",
                            "doc/odd-nibble", source, NULL, &write_result,
                            &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  hidden_options.has_query_hidden = 1;
  hidden_options.query_hidden = 1;
  rc = lc_source_from_memory("{\"tags\":[\"planning\"],\"n\":4}",
                             strlen("{\"tags\":[\"planning\"],\"n\":4}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-in", "doc/hidden",
                            source, &hidden_options, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"tags\":[\"finance\"],\"n\":5}",
                             strlen("{\"tags\":[\"finance\"],\"n\":5}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-in", "doc/deleted",
                            source, NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  rc = lc_pouch_state_delete(pouch, "docs/query-index-in", "doc/deleted",
                             NULL, &delete_result, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &delete_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  open_pouch_client(root, &client, &error);
  handler.begin = pouch_query_key_begin;
  handler.chunk = pouch_query_key_chunk;
  handler.end = pouch_query_key_end;
  query_req.namespace_name = "docs/query-index-in";
  query_req.selector_json = selector;
  query_req.engine = "index";
  query_req.refresh = "wait_for";
  query_req.limit = 1L;
  rc = client->query_keys(client, &query_req, &handler, &first_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(first_page.count, 1);
  assert_non_null(query_res.cursor);
  assert_non_null(query_res.metadata_json);
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));
  namespace_path =
      lc_pouch_namespace_path(NULL, root, "docs/query-index-in");
  assert_non_null(namespace_path);
  assert_path_file_contains(namespace_path, "index/query.index",
                            "term_index_complete=1");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "presence_index_complete=1");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "2f746167732f5b5d");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "2f74616773");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "706c616e6e696e67");

  snprintf(cursor, sizeof(cursor), "%s", query_res.cursor);
  query_req.cursor = cursor;
  lc_query_res_cleanup(&query_res);
  rc = client->query_keys(client, &query_req, &handler, &second_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(second_page.count, 1);
  assert_null(query_res.cursor);
  assert_true(pouch_query_capture_has(&first_page, "doc/a") ||
              pouch_query_capture_has(&second_page, "doc/a"));
  assert_true(pouch_query_capture_has(&first_page, "doc/c") ||
              pouch_query_capture_has(&second_page, "doc/c"));
  assert_false(pouch_query_capture_has(&first_page, "doc/b"));
  assert_false(pouch_query_capture_has(&second_page, "doc/b"));
  assert_false(pouch_query_capture_has(&first_page, "doc/hidden"));
  assert_false(pouch_query_capture_has(&second_page, "doc/hidden"));
  assert_false(pouch_query_capture_has(&first_page, "doc/deleted"));
  assert_false(pouch_query_capture_has(&second_page, "doc/deleted"));
  lc_query_res_cleanup(&query_res);

  memset(&query_res, 0, sizeof(query_res));
  query_req.cursor = NULL;
  query_req.selector_json = "{\"exists\":\"/tags\"}";
  query_req.limit = 0L;
  rc = client->query_keys(client, &query_req, &handler, &exists_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(exists_page.count, 3);
  assert_null(query_res.cursor);
  assert_true(pouch_query_capture_has(&exists_page, "doc/a"));
  assert_true(pouch_query_capture_has(&exists_page, "doc/b"));
  assert_true(pouch_query_capture_has(&exists_page, "doc/c"));
  assert_false(pouch_query_capture_has(&exists_page, "doc/hidden"));
  assert_false(pouch_query_capture_has(&exists_page, "doc/deleted"));
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));
  lc_query_res_cleanup(&query_res);

  memset(&query_res, 0, sizeof(query_res));
  query_req.cursor = NULL;
  query_req.selector_json =
      "{\"contains\":{\"field\":\"/needle\",\"value\":\"ab\"}}";
  query_req.limit = 0L;
  rc = client->query_keys(client, &query_req, &handler,
                          &odd_nibble_contains_page, &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(odd_nibble_contains_page.count, 0);
  assert_null(query_res.cursor);
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));
  lc_query_res_cleanup(&query_res);

  memset(&query_res, 0, sizeof(query_res));
  query_req.cursor = NULL;
  query_req.selector_json =
      "{\"prefix\":{\"field\":\"/tags[]\",\"value\":\"fin\"}}";
  query_req.limit = 0L;
  rc = client->query_keys(client, &query_req, &handler, &prefix_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(prefix_page.count, 2);
  assert_null(query_res.cursor);
  assert_true(pouch_query_capture_has(&prefix_page, "doc/a"));
  assert_true(pouch_query_capture_has(&prefix_page, "doc/c"));
  assert_false(pouch_query_capture_has(&prefix_page, "doc/b"));
  assert_false(pouch_query_capture_has(&prefix_page, "doc/hidden"));
  assert_false(pouch_query_capture_has(&prefix_page, "doc/deleted"));
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));
  lc_query_res_cleanup(&query_res);

  memset(&query_res, 0, sizeof(query_res));
  query_req.cursor = NULL;
  query_req.selector_json =
      "{\"iprefix\":{\"field\":\"/tags[]\",\"value\":\"FIN\"}}";
  query_req.limit = 0L;
  rc = client->query_keys(client, &query_req, &handler, &iprefix_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(iprefix_page.count, 2);
  assert_null(query_res.cursor);
  assert_true(pouch_query_capture_has(&iprefix_page, "doc/a"));
  assert_true(pouch_query_capture_has(&iprefix_page, "doc/c"));
  assert_false(pouch_query_capture_has(&iprefix_page, "doc/b"));
  assert_false(pouch_query_capture_has(&iprefix_page, "doc/hidden"));
  assert_false(pouch_query_capture_has(&iprefix_page, "doc/deleted"));
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));
  lc_query_res_cleanup(&query_res);

  memset(&query_res, 0, sizeof(query_res));
  query_req.cursor = NULL;
  query_req.selector_json =
      "{\"contains\":{\"field\":\"/tags[]\",\"value\":\"nan\"}}";
  query_req.limit = 0L;
  rc = client->query_keys(client, &query_req, &handler, &contains_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(contains_page.count, 2);
  assert_null(query_res.cursor);
  assert_true(pouch_query_capture_has(&contains_page, "doc/a"));
  assert_true(pouch_query_capture_has(&contains_page, "doc/c"));
  assert_false(pouch_query_capture_has(&contains_page, "doc/b"));
  assert_false(pouch_query_capture_has(&contains_page, "doc/hidden"));
  assert_false(pouch_query_capture_has(&contains_page, "doc/deleted"));
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));
  lc_query_res_cleanup(&query_res);

  memset(&query_res, 0, sizeof(query_res));
  query_req.cursor = NULL;
  query_req.selector_json =
      "{\"icontains\":{\"field\":\"/tags[]\",\"value\":\"INA\"}}";
  query_req.limit = 0L;
  rc = client->query_keys(client, &query_req, &handler, &icontains_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(icontains_page.count, 2);
  assert_null(query_res.cursor);
  assert_true(pouch_query_capture_has(&icontains_page, "doc/a"));
  assert_true(pouch_query_capture_has(&icontains_page, "doc/c"));
  assert_false(pouch_query_capture_has(&icontains_page, "doc/b"));
  assert_false(pouch_query_capture_has(&icontains_page, "doc/hidden"));
  assert_false(pouch_query_capture_has(&icontains_page, "doc/deleted"));
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));
  lc_query_res_cleanup(&query_res);

  memset(&query_res, 0, sizeof(query_res));
  query_req.cursor = NULL;
  query_req.selector_json =
      "{\"prefix\":{\"field\":\"/n\",\"value\":\"2\"}}";
  query_req.limit = 0L;
  rc = client->query_keys(client, &query_req, &handler, &numeric_prefix_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(numeric_prefix_page.count, 0);
  assert_null(query_res.cursor);
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));
  lc_query_res_cleanup(&query_res);

  memset(&query_res, 0, sizeof(query_res));
  query_req.cursor = NULL;
  query_req.selector_json =
      "{\"range\":{\"field\":\"/n\",\"gte\":2,\"lt\":4}}";
  query_req.limit = 0L;
  rc = client->query_keys(client, &query_req, &handler, &range_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(range_page.count, 3);
  assert_null(query_res.cursor);
  assert_true(pouch_query_capture_has(&range_page, "doc/b"));
  assert_true(pouch_query_capture_has(&range_page, "doc/c"));
  assert_true(pouch_query_capture_has(&range_page, "doc/decimal"));
  assert_false(pouch_query_capture_has(&range_page, "doc/a"));
  assert_false(pouch_query_capture_has(&range_page, "doc/hidden"));
  assert_false(pouch_query_capture_has(&range_page, "doc/deleted"));
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));
  lc_query_res_cleanup(&query_res);

  memset(&query_res, 0, sizeof(query_res));
  query_req.cursor = NULL;
  query_req.selector_json =
      "{\"and\":[{\"eq\":{\"field\":\"/n\",\"value\":\"1\"}}]}";
  rc = client->query_keys(client, &query_req, &handler, &unsupported_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_true(bytes_contain_text(error.message, strlen(error.message),
                                 "supports exact scalar equality, in, exists, "
                                 "prefix, contains, range, and date"));

  free(namespace_path);
  lc_query_res_cleanup(&query_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_query_keys_index_preserves_json_scalar_types(void **state) {
  lc_client *client;
  lc_pouch *pouch;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_query_key_handler handler;
  pouch_query_key_capture scan_number_page;
  pouch_query_key_capture number_page;
  pouch_query_key_capture string_page;
  pouch_query_key_capture bool_page;
  pouch_query_key_capture root_or_page;
  pouch_query_key_capture mixed_in_page;
  lc_error error;
  char root[512];
  int rc;

  (void)state;
  client = NULL;
  pouch = NULL;
  memset(&query_res, 0, sizeof(query_res));
  memset(&handler, 0, sizeof(handler));
  memset(&scan_number_page, 0, sizeof(scan_number_page));
  memset(&number_page, 0, sizeof(number_page));
  memset(&string_page, 0, sizeof(string_page));
  memset(&bool_page, 0, sizeof(bool_page));
  memset(&root_or_page, 0, sizeof(root_or_page));
  memset(&mixed_in_page, 0, sizeof(mixed_in_page));
  lc_query_req_init(&query_req);
  lc_error_init(&error);
  make_root("query-keys-index-scalar-types", root, sizeof(root));
  cleanup_root(root);
  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  pouch_write_json_state(pouch, "docs/query-index-scalar-types",
                         "doc/number", "{\"v\":1,\"flag\":false}", NULL,
                         &error);
  pouch_write_json_state(pouch, "docs/query-index-scalar-types",
                         "doc/number-float", "{\"v\":1.0}", NULL, &error);
  pouch_write_json_state(pouch, "docs/query-index-scalar-types",
                         "doc/string-number",
                         "{\"v\":\"1\",\"flag\":\"false\"}", NULL, &error);
  pouch_write_json_state(pouch, "docs/query-index-scalar-types", "doc/bool",
                         "{\"v\":true,\"flag\":true}", NULL, &error);
  pouch_write_json_state(pouch, "docs/query-index-scalar-types",
                         "doc/string-bool",
                         "{\"v\":\"true\",\"flag\":\"true\"}", NULL, &error);
  pouch_write_json_state(pouch, "docs/query-index-scalar-types", "doc/null",
                         "{\"v\":null}", NULL, &error);
  pouch_write_json_state(pouch, "docs/query-index-scalar-types",
                         "doc/string-null", "{\"v\":\"null\"}", NULL,
                         &error);
  lc_pouch_close(pouch);
  pouch = NULL;

  open_pouch_client(root, &client, &error);
  handler.begin = pouch_query_key_begin;
  handler.chunk = pouch_query_key_chunk;
  handler.end = pouch_query_key_end;
  query_req.namespace_name = "docs/query-index-scalar-types";
  query_req.selector_json = "{\"eq\":{\"field\":\"/v\",\"value\":1}}";
  query_req.engine = "scan";
  rc = client->query_keys(client, &query_req, &handler, &scan_number_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(pouch_query_capture_has(&scan_number_page, "doc/number"));
  assert_false(pouch_query_capture_has(&scan_number_page,
                                       "doc/string-number"));
  lc_query_res_cleanup(&query_res);

  query_req.engine = "index";
  query_req.refresh = "wait_for";
  rc = client->query_keys(client, &query_req, &handler, &number_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(number_page.count, scan_number_page.count);
  assert_true(pouch_query_capture_has(&number_page, "doc/number"));
  assert_false(pouch_query_capture_has(&number_page, "doc/string-number"));
  assert_int_equal(pouch_query_capture_has(&number_page, "doc/number-float"),
                   pouch_query_capture_has(&scan_number_page,
                                           "doc/number-float"));
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"query_candidates\":2"));
  lc_query_res_cleanup(&query_res);

  memset(&string_page, 0, sizeof(string_page));
  query_req.refresh = NULL;
  query_req.selector_json = "{\"eq\":{\"field\":\"/v\",\"value\":\"1\"}}";
  rc = client->query_keys(client, &query_req, &handler, &string_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(string_page.count, 1);
  assert_true(pouch_query_capture_has(&string_page, "doc/string-number"));
  assert_false(pouch_query_capture_has(&string_page, "doc/number"));
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"query_candidates\":1"));
  lc_query_res_cleanup(&query_res);

  memset(&bool_page, 0, sizeof(bool_page));
  query_req.selector_lql = "eq{field=/flag,value=true}";
  query_req.selector_json = NULL;
  rc = client->query_keys(client, &query_req, &handler, &bool_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(bool_page.count, 1);
  assert_true(pouch_query_capture_has(&bool_page, "doc/bool"));
  assert_false(pouch_query_capture_has(&bool_page, "doc/string-bool"));
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"query_candidates\":1"));
  lc_query_res_cleanup(&query_res);

  memset(&root_or_page, 0, sizeof(root_or_page));
  query_req.selector_lql =
      "or.eq{field=/v,value=1},or.eq{field=/flag,value=true}";
  query_req.selector_json = NULL;
  rc = client->query_keys(client, &query_req, &handler, &root_or_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(root_or_page.count, 3);
  assert_true(pouch_query_capture_has(&root_or_page, "doc/number"));
  assert_true(pouch_query_capture_has(&root_or_page, "doc/number-float"));
  assert_true(pouch_query_capture_has(&root_or_page, "doc/bool"));
  assert_false(pouch_query_capture_has(&root_or_page, "doc/string-number"));
  assert_false(pouch_query_capture_has(&root_or_page, "doc/string-bool"));
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"query_candidates\":3"));
  lc_query_res_cleanup(&query_res);

  memset(&mixed_in_page, 0, sizeof(mixed_in_page));
  query_req.selector_lql = NULL;
  query_req.selector_json =
      "{\"in\":{\"field\":\"/v\",\"any\":[1,\"1\",null,\"null\"]}}";
  rc = client->query_keys(client, &query_req, &handler, &mixed_in_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(mixed_in_page.count, 5);
  assert_true(pouch_query_capture_has(&mixed_in_page, "doc/number"));
  assert_true(pouch_query_capture_has(&mixed_in_page, "doc/number-float"));
  assert_true(pouch_query_capture_has(&mixed_in_page, "doc/string-number"));
  assert_true(pouch_query_capture_has(&mixed_in_page, "doc/null"));
  assert_true(pouch_query_capture_has(&mixed_in_page, "doc/string-null"));
  assert_false(pouch_query_capture_has(&mixed_in_page, "doc/bool"));
  assert_false(pouch_query_capture_has(&mixed_in_page, "doc/string-bool"));
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"query_candidates\":5"));

  lc_query_res_cleanup(&query_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_query_keys_index_root_or_uses_scalar_union(void **state) {
  static const char selector_lql[] =
      "or.eq{field=/bucket,value=needle},or.eq{field=/flag,value=true}";
  static const char reversed_selector_lql[] =
      "or.eq{field=/flag,value=true},or.eq{field=/bucket,value=needle}";
  lc_client *client;
  lc_pouch *pouch;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_query_key_handler handler;
  pouch_query_key_capture first_page;
  pouch_query_key_capture second_page;
  pouch_query_key_capture reversed_page;
  lc_sink *documents_sink;
  lc_pouch_state_write_options hidden_options;
  lc_pouch_state_write_result delete_result;
  lc_error error;
  const void *document_bytes;
  size_t document_length;
  char root[512];
  char cursor[64];
  int rc;

  (void)state;
  client = NULL;
  pouch = NULL;
  documents_sink = NULL;
  document_bytes = NULL;
  document_length = 0U;
  memset(&query_res, 0, sizeof(query_res));
  memset(&handler, 0, sizeof(handler));
  memset(&first_page, 0, sizeof(first_page));
  memset(&second_page, 0, sizeof(second_page));
  memset(&reversed_page, 0, sizeof(reversed_page));
  memset(&hidden_options, 0, sizeof(hidden_options));
  memset(&delete_result, 0, sizeof(delete_result));
  lc_query_req_init(&query_req);
  lc_error_init(&error);
  make_root("query-keys-index-root-or", root, sizeof(root));
  cleanup_root(root);
  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  pouch_write_json_state(pouch, "docs/query-index-or", "doc/a",
                         "{\"bucket\":\"needle\",\"flag\":false,\"n\":1}",
                         NULL, &error);
  pouch_write_json_state(pouch, "docs/query-index-or", "doc/b",
                         "{\"bucket\":\"hay\",\"flag\":true,\"n\":2}", NULL,
                         &error);
  pouch_write_json_state(pouch, "docs/query-index-or", "doc/overlap",
                         "{\"bucket\":\"needle\",\"flag\":true,\"n\":3}",
                         NULL, &error);
  pouch_write_json_state(pouch, "docs/query-index-or", "doc/string-flag",
                         "{\"bucket\":\"hay\",\"flag\":\"true\",\"n\":4}",
                         NULL, &error);
  pouch_write_json_state(pouch, "docs/query-index-or", "doc/no-match",
                         "{\"bucket\":\"hay\",\"flag\":false,\"n\":5}", NULL,
                         &error);
  hidden_options.has_query_hidden = 1;
  hidden_options.query_hidden = 1;
  pouch_write_json_state(pouch, "docs/query-index-or", "doc/hidden",
                         "{\"bucket\":\"needle\",\"flag\":false,\"n\":6}",
                         &hidden_options, &error);
  pouch_write_json_state(pouch, "docs/query-index-or", "doc/deleted",
                         "{\"bucket\":\"hay\",\"flag\":true,\"n\":7}", NULL,
                         &error);
  rc = lc_pouch_state_delete(pouch, "docs/query-index-or", "doc/deleted",
                             NULL, &delete_result, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &delete_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  open_pouch_client(root, &client, &error);
  handler.begin = pouch_query_key_begin;
  handler.chunk = pouch_query_key_chunk;
  handler.end = pouch_query_key_end;
  query_req.namespace_name = "docs/query-index-or";
  query_req.selector_lql = selector_lql;
  query_req.engine = "index";
  query_req.refresh = "wait_for";
  query_req.limit = 2L;
  rc = client->query_keys(client, &query_req, &handler, &first_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(first_page.count, 2);
  assert_string_equal(first_page.keys[0], "doc/a");
  assert_string_equal(first_page.keys[1], "doc/b");
  assert_non_null(query_res.cursor);
  assert_non_null(query_res.metadata_json);
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"query_candidates\":3"));

  snprintf(cursor, sizeof(cursor), "%s", query_res.cursor);
  query_req.cursor = cursor;
  lc_query_res_cleanup(&query_res);
  rc = client->query_keys(client, &query_req, &handler, &second_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(second_page.count, 1);
  assert_string_equal(second_page.keys[0], "doc/overlap");
  assert_null(query_res.cursor);
  assert_true(pouch_query_capture_has(&first_page, "doc/a") ||
              pouch_query_capture_has(&second_page, "doc/a"));
  assert_true(pouch_query_capture_has(&first_page, "doc/b") ||
              pouch_query_capture_has(&second_page, "doc/b"));
  assert_true(pouch_query_capture_has(&first_page, "doc/overlap") ||
              pouch_query_capture_has(&second_page, "doc/overlap"));
  assert_false(pouch_query_capture_has(&first_page, "doc/string-flag"));
  assert_false(pouch_query_capture_has(&second_page, "doc/string-flag"));
  assert_false(pouch_query_capture_has(&first_page, "doc/no-match"));
  assert_false(pouch_query_capture_has(&second_page, "doc/no-match"));
  assert_false(pouch_query_capture_has(&first_page, "doc/hidden"));
  assert_false(pouch_query_capture_has(&second_page, "doc/hidden"));
  assert_false(pouch_query_capture_has(&first_page, "doc/deleted"));
  assert_false(pouch_query_capture_has(&second_page, "doc/deleted"));
  lc_query_res_cleanup(&query_res);

  memset(&query_res, 0, sizeof(query_res));
  query_req.cursor = NULL;
  query_req.limit = 0L;
  query_req.selector_lql = reversed_selector_lql;
  rc = client->query_keys(client, &query_req, &handler, &reversed_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(reversed_page.count, 3);
  assert_true(pouch_query_capture_has(&reversed_page, "doc/a"));
  assert_true(pouch_query_capture_has(&reversed_page, "doc/b"));
  assert_true(pouch_query_capture_has(&reversed_page, "doc/overlap"));
  assert_false(pouch_query_capture_has(&reversed_page, "doc/string-flag"));
  assert_false(pouch_query_capture_has(&reversed_page, "doc/no-match"));
  assert_false(pouch_query_capture_has(&reversed_page, "doc/hidden"));
  assert_false(pouch_query_capture_has(&reversed_page, "doc/deleted"));
  lc_query_res_cleanup(&query_res);

  memset(&query_res, 0, sizeof(query_res));
  query_req.cursor = NULL;
  query_req.selector_lql = selector_lql;
  query_req.limit = 0L;
  query_req.return_mode = "documents";
  query_req.fields_json = "{\"n\":true}";
  rc = lc_sink_to_memory(&documents_sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->query(client, &query_req, documents_sink, &query_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(documents_sink, &document_bytes, &document_length,
                            &error);
  assert_int_equal(rc, LC_OK);
  assert_true(document_length > 0U);
  assert_null(query_res.cursor);
  assert_true(bytes_contain_text(document_bytes, document_length, "\"n\":1"));
  assert_true(bytes_contain_text(document_bytes, document_length, "\"n\":2"));
  assert_true(bytes_contain_text(document_bytes, document_length, "\"n\":3"));
  assert_false(bytes_contain_text(document_bytes, document_length, "\"n\":4"));
  assert_false(bytes_contain_text(document_bytes, document_length, "\"n\":5"));
  assert_false(bytes_contain_text(document_bytes, document_length, "\"n\":6"));
  assert_false(bytes_contain_text(document_bytes, document_length, "\"n\":7"));
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));

  lc_sink_close(documents_sink);
  lc_query_res_cleanup(&query_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_query_keys_index_text_stops_after_target_field(void **state) {
  lc_client *client;
  lc_pouch *pouch;
  lc_source *source;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_query_key_handler handler;
  pouch_query_key_capture contains_page;
  pouch_query_key_capture prefix_page;
  lc_pouch_state_write_result write_result;
  lc_error error;
  char *namespace_path;
  char sidecar_path[1024];
  char root[512];
  int rc;

  (void)state;
  client = NULL;
  pouch = NULL;
  source = NULL;
  namespace_path = NULL;
  memset(&query_res, 0, sizeof(query_res));
  memset(&handler, 0, sizeof(handler));
  memset(&contains_page, 0, sizeof(contains_page));
  memset(&prefix_page, 0, sizeof(prefix_page));
  memset(&write_result, 0, sizeof(write_result));
  lc_query_req_init(&query_req);
  lc_error_init(&error);
  make_root("query-keys-index-text-stop", root, sizeof(root));
  cleanup_root(root);

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("{\"a\":\"alphabet timeout\",\"z\":\"later\"}",
                             strlen("{\"a\":\"alphabet timeout\",\"z\":\"later\"}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-text-stop", "doc/a",
                            source, NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  rc = lc_source_from_memory("{\"a\":\"ordinary\",\"z\":\"later\"}",
                             strlen("{\"a\":\"ordinary\",\"z\":\"later\"}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-text-stop", "doc/b",
                            source, NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  open_pouch_client(root, &client, &error);
  handler.begin = pouch_query_key_begin;
  handler.chunk = pouch_query_key_chunk;
  handler.end = pouch_query_key_end;
  query_req.namespace_name = "docs/query-index-text-stop";
  query_req.selector_json =
      "{\"contains\":{\"field\":\"/a\",\"value\":\"timeout\"}}";
  query_req.engine = "index";
  query_req.refresh = "wait_for";
  rc = client->query_keys(client, &query_req, &handler, &contains_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(contains_page.count, 1);
  assert_true(pouch_query_capture_has(&contains_page, "doc/a"));
  lc_query_res_cleanup(&query_res);

  namespace_path =
      lc_pouch_namespace_path(NULL, root, "docs/query-index-text-stop");
  assert_non_null(namespace_path);
  snprintf(sidecar_path, sizeof(sidecar_path), "%s/index/query.index",
           namespace_path);
  assert_file_contains(sidecar_path, "2f61");
  assert_file_contains(sidecar_path, "2f7a");
  replace_text_file_first(sidecar_path, "6c61746572", "6c6174657278");

  memset(&query_res, 0, sizeof(query_res));
  memset(&contains_page, 0, sizeof(contains_page));
  query_req.refresh = NULL;
  rc = client->query_keys(client, &query_req, &handler, &contains_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(contains_page.count, 1);
  assert_true(pouch_query_capture_has(&contains_page, "doc/a"));
  lc_query_res_cleanup(&query_res);

  memset(&query_res, 0, sizeof(query_res));
  query_req.selector_json =
      "{\"prefix\":{\"field\":\"/a\",\"value\":\"alph\"}}";
  rc = client->query_keys(client, &query_req, &handler, &prefix_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(prefix_page.count, 1);
  assert_true(pouch_query_capture_has(&prefix_page, "doc/a"));

  free(namespace_path);
  lc_query_res_cleanup(&query_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_query_keys_index_date_lql_filters_temporal_candidates(
    void **state) {
  lc_client *client;
  lc_pouch *pouch;
  lc_source *source;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_query_key_handler handler;
  pouch_query_key_capture date_page;
  lc_pouch_state_write_options hidden_options;
  lc_pouch_state_write_result write_result;
  lc_pouch_state_write_result delete_result;
  lc_error error;
  char root[512];
  int rc;

  (void)state;
  client = NULL;
  pouch = NULL;
  source = NULL;
  memset(&query_res, 0, sizeof(query_res));
  memset(&handler, 0, sizeof(handler));
  memset(&date_page, 0, sizeof(date_page));
  memset(&hidden_options, 0, sizeof(hidden_options));
  memset(&write_result, 0, sizeof(write_result));
  memset(&delete_result, 0, sizeof(delete_result));
  lc_query_req_init(&query_req);
  lc_error_init(&error);
  make_root("query-keys-index-date-lql", root, sizeof(root));
  cleanup_root(root);
  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory(
      "{\"created_at\":\"2026-01-01T00:00:00Z\",\"n\":1}",
      strlen("{\"created_at\":\"2026-01-01T00:00:00Z\",\"n\":1}"),
      &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-date", "doc/new",
                            source, NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory(
      "{\"created_at\":\"2025-01-01T02:00:00+01:00\",\"n\":7}",
      strlen("{\"created_at\":\"2025-01-01T02:00:00+01:00\",\"n\":7}"),
      &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-date", "doc/offset",
                            source, NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"created_at\":\"2025-01-02\",\"n\":8}",
                             strlen("{\"created_at\":\"2025-01-02\",\"n\":8}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-date", "doc/date-only",
                            source, NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory(
      "{\"created_at\":\"2025-01-01T00:00:01\",\"n\":9}",
      strlen("{\"created_at\":\"2025-01-01T00:00:01\",\"n\":9}"), &source,
      &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-date", "doc/naive",
                            source, NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory(
      "{\"created_at\":\"2025-01-01T00:00:00.500Z\",\"n\":10}",
      strlen("{\"created_at\":\"2025-01-01T00:00:00.500Z\",\"n\":10}"),
      &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-date", "doc/fractional",
                            source, NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory(
      "{\"created_at\":\"2024-01-01T00:00:00Z\",\"n\":2}",
      strlen("{\"created_at\":\"2024-01-01T00:00:00Z\",\"n\":2}"),
      &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-date", "doc/old",
                            source, NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"created_at\":\"not-a-date\",\"n\":3}",
                             strlen("{\"created_at\":\"not-a-date\",\"n\":3}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-date", "doc/invalid",
                            source, NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"category\":\"missing-date\",\"n\":4}",
                             strlen("{\"category\":\"missing-date\",\"n\":4}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-date", "doc/missing",
                            source, NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  hidden_options.has_query_hidden = 1;
  hidden_options.query_hidden = 1;
  rc = lc_source_from_memory(
      "{\"created_at\":\"2026-01-01T00:00:00Z\",\"n\":5}",
      strlen("{\"created_at\":\"2026-01-01T00:00:00Z\",\"n\":5}"),
      &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-date", "doc/hidden",
                            source, &hidden_options, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory(
      "{\"created_at\":\"2026-01-01T00:00:00Z\",\"n\":6}",
      strlen("{\"created_at\":\"2026-01-01T00:00:00Z\",\"n\":6}"),
      &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-date", "doc/deleted",
                            source, NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  rc = lc_pouch_state_delete(pouch, "docs/query-index-date", "doc/deleted",
                             NULL, &delete_result, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &delete_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  open_pouch_client(root, &client, &error);
  handler.begin = pouch_query_key_begin;
  handler.chunk = pouch_query_key_chunk;
  handler.end = pouch_query_key_end;
  query_req.namespace_name = "docs/query-index-date";
  query_req.selector_lql = "date{field=/created_at,after=2025-01-01}";
  query_req.engine = "index";
  query_req.refresh = "wait_for";
  rc = client->query_keys(client, &query_req, &handler, &date_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(date_page.count, 5);
  assert_true(pouch_query_capture_has(&date_page, "doc/new"));
  assert_true(pouch_query_capture_has(&date_page, "doc/offset"));
  assert_true(pouch_query_capture_has(&date_page, "doc/date-only"));
  assert_true(pouch_query_capture_has(&date_page, "doc/naive"));
  assert_true(pouch_query_capture_has(&date_page, "doc/fractional"));
  assert_false(pouch_query_capture_has(&date_page, "doc/old"));
  assert_false(pouch_query_capture_has(&date_page, "doc/invalid"));
  assert_false(pouch_query_capture_has(&date_page, "doc/missing"));
  assert_false(pouch_query_capture_has(&date_page, "doc/hidden"));
  assert_false(pouch_query_capture_has(&date_page, "doc/deleted"));
  assert_null(query_res.cursor);
  assert_non_null(query_res.metadata_json);
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));

  lc_query_res_cleanup(&query_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_query_keys_index_recursive_exists_uses_container_presence(
    void **state) {
  lc_client *client;
  lc_pouch *pouch;
  lc_source *source;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_query_key_handler handler;
  pouch_query_key_capture page;
  lc_pouch_state_write_result write_result;
  lc_error error;
  char root[512];
  int rc;

  (void)state;
  client = NULL;
  pouch = NULL;
  source = NULL;
  memset(&query_res, 0, sizeof(query_res));
  memset(&handler, 0, sizeof(handler));
  memset(&page, 0, sizeof(page));
  memset(&write_result, 0, sizeof(write_result));
  lc_query_req_init(&query_req);
  lc_error_init(&error);
  make_root("query-keys-index-recursive-exists", root, sizeof(root));
  cleanup_root(root);
  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory("{\"details\":{\"message\":\"alpha\"}}",
                             strlen("{\"details\":{\"message\":\"alpha\"}}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-recursive", "doc/a",
                            source, NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"details\":{\"other\":1}}",
                             strlen("{\"details\":{\"other\":1}}"), &source,
                             &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-recursive", "doc/b",
                            source, NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"other\":true}", strlen("{\"other\":true}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-recursive", "doc/c",
                            source, NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  open_pouch_client(root, &client, &error);
  handler.begin = pouch_query_key_begin;
  handler.chunk = pouch_query_key_chunk;
  handler.end = pouch_query_key_end;
  query_req.namespace_name = "docs/query-index-recursive";
  query_req.selector_json = "{\"exists\":\"/details/**\"}";
  query_req.engine = "index";
  query_req.refresh = "wait_for";
  rc = client->query_keys(client, &query_req, &handler, &page, &query_res,
                          &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(page.count, 2);
  assert_true(pouch_query_capture_has(&page, "doc/a"));
  assert_true(pouch_query_capture_has(&page, "doc/b"));
  assert_false(pouch_query_capture_has(&page, "doc/c"));
  assert_non_null(query_res.metadata_json);
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));

  lc_query_res_cleanup(&query_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_query_documents_scan_streams_rows(void **state) {
  static const char selector[] =
      "{\"eq\":{\"field\":\"/category\",\"value\":\"planning\"}}";
  lc_client *client;
  lc_pouch *pouch;
  lc_source *source;
  lc_sink *first_sink;
  lc_sink *second_sink;
  lc_sink *invalid_sink;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result write_result;
  lc_error error;
  const void *first_bytes;
  const void *second_bytes;
  size_t first_length;
  size_t second_length;
  char root[512];
  char cursor[64];
  int rc;

  (void)state;
  client = NULL;
  pouch = NULL;
  source = NULL;
  first_sink = NULL;
  second_sink = NULL;
  invalid_sink = NULL;
  first_bytes = NULL;
  second_bytes = NULL;
  first_length = 0U;
  second_length = 0U;
  memset(&query_res, 0, sizeof(query_res));
  memset(&options, 0, sizeof(options));
  memset(&write_result, 0, sizeof(write_result));
  lc_query_req_init(&query_req);
  lc_error_init(&error);
  make_root("query-documents-scan", root, sizeof(root));
  cleanup_root(root);
  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory("{\"category\":\"planning\",\"n\":1}",
                             strlen("{\"category\":\"planning\",\"n\":1}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-docs", "doc/a", source, NULL,
                            &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"category\":\"planning\",\"n\":2}",
                             strlen("{\"category\":\"planning\",\"n\":2}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-docs", "doc/b", source, NULL,
                            &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"category\":\"finance\",\"n\":3}",
                             strlen("{\"category\":\"finance\",\"n\":3}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-docs", "doc/c", source, NULL,
                            &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  memset(&options, 0, sizeof(options));
  options.has_query_hidden = 1;
  options.query_hidden = 1;
  rc = lc_source_from_memory("{\"category\":\"planning\",\"n\":4}",
                             strlen("{\"category\":\"planning\",\"n\":4}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-docs", "doc/hidden", source,
                            &options, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"category\":\"planning\",\"n\":5}",
                             strlen("{\"category\":\"planning\",\"n\":5}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_stage_write(pouch, "docs/query-docs", "doc/staged",
                                  "txn-query-docs-hidden", source, NULL,
                                  &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  open_pouch_client(root, &client, &error);
  rc = lc_sink_to_memory(&first_sink, &error);
  assert_int_equal(rc, LC_OK);

  query_req.namespace_name = "docs/query-docs";
  query_req.selector_json = selector;
  query_req.limit = 1L;
  query_req.return_mode = "documents";
  query_req.engine = "scan";
  query_req.fields_json = "{\"n\":true}";
  rc = client->query(client, &query_req, first_sink, &query_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(first_sink, &first_bytes, &first_length, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(first_length > 0U);
  assert_non_null(query_res.cursor);
  assert_string_equal(query_res.return_mode, "documents");
  assert_non_null(query_res.metadata_json);
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"scan\""));
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "query_candidates"));
  assert_true(query_res.index_seq > 0UL);

  snprintf(cursor, sizeof(cursor), "%s", query_res.cursor);
  query_req.cursor = cursor;
  lc_query_res_cleanup(&query_res);
  rc = lc_sink_to_memory(&second_sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->query(client, &query_req, second_sink, &query_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(second_sink, &second_bytes, &second_length, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(second_length > 0U);
  assert_null(query_res.cursor);
  assert_true(bytes_contain_text(first_bytes, first_length, "\"n\":1") ||
              bytes_contain_text(second_bytes, second_length, "\"n\":1"));
  assert_true(bytes_contain_text(first_bytes, first_length, "\"n\":2") ||
              bytes_contain_text(second_bytes, second_length, "\"n\":2"));
  assert_false(bytes_contain_text(first_bytes, first_length, "\"n\":3"));
  assert_false(bytes_contain_text(second_bytes, second_length, "\"n\":3"));
  assert_false(bytes_contain_text(first_bytes, first_length, "\"n\":4"));
  assert_false(bytes_contain_text(second_bytes, second_length, "\"n\":4"));
  assert_false(bytes_contain_text(first_bytes, first_length, "\"n\":5"));
  assert_false(bytes_contain_text(second_bytes, second_length, "\"n\":5"));

  query_req.cursor = NULL;
  query_req.fields_json = "[\"n\"]";
  lc_query_res_cleanup(&query_res);
  rc = lc_sink_to_memory(&invalid_sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->query(client, &query_req, invalid_sink, &query_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_non_null(strstr(error.message, "fields_json must be a JSON object"));
  lc_error_cleanup(&error);
  lc_error_init(&error);

  lc_sink_close(first_sink);
  lc_sink_close(second_sink);
  lc_sink_close(invalid_sink);
  lc_query_res_cleanup(&query_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_query_documents_index_uses_scalar_postings(void **state) {
  static const char selector[] =
      "{\"in\":{\"field\":\"/tags[]\",\"any\":[\"planning\",\"finance\"]}}";
  lc_client *client;
  lc_pouch *pouch;
  lc_source *source;
  lc_sink *first_sink;
  lc_sink *second_sink;
  lc_sink *exists_sink;
  lc_sink *prefix_sink;
  lc_sink *contains_sink;
  lc_sink *iprefix_sink;
  lc_sink *icontains_sink;
  lc_sink *range_sink;
  lc_sink *unsupported_sink;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_pouch_state_write_options hidden_options;
  lc_pouch_state_write_result write_result;
  lc_pouch_state_write_result delete_result;
  lc_error error;
  const void *first_bytes;
  const void *second_bytes;
  const void *exists_bytes;
  const void *prefix_bytes;
  const void *contains_bytes;
  const void *iprefix_bytes;
  const void *icontains_bytes;
  const void *range_bytes;
  size_t first_length;
  size_t second_length;
  size_t exists_length;
  size_t prefix_length;
  size_t contains_length;
  size_t iprefix_length;
  size_t icontains_length;
  size_t range_length;
  char root[512];
  char cursor[64];
  int rc;

  (void)state;
  client = NULL;
  pouch = NULL;
  source = NULL;
  first_sink = NULL;
  second_sink = NULL;
  exists_sink = NULL;
  prefix_sink = NULL;
  contains_sink = NULL;
  iprefix_sink = NULL;
  icontains_sink = NULL;
  range_sink = NULL;
  unsupported_sink = NULL;
  first_bytes = NULL;
  second_bytes = NULL;
  exists_bytes = NULL;
  prefix_bytes = NULL;
  contains_bytes = NULL;
  iprefix_bytes = NULL;
  icontains_bytes = NULL;
  range_bytes = NULL;
  first_length = 0U;
  second_length = 0U;
  exists_length = 0U;
  prefix_length = 0U;
  contains_length = 0U;
  iprefix_length = 0U;
  icontains_length = 0U;
  range_length = 0U;
  memset(&query_res, 0, sizeof(query_res));
  memset(&hidden_options, 0, sizeof(hidden_options));
  memset(&write_result, 0, sizeof(write_result));
  memset(&delete_result, 0, sizeof(delete_result));
  lc_query_req_init(&query_req);
  lc_error_init(&error);
  make_root("query-documents-index", root, sizeof(root));
  cleanup_root(root);
  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory(
      "{\"tags\":[\"planning\",\"finance\"],\"n\":1}",
      strlen("{\"tags\":[\"planning\",\"finance\"],\"n\":1}"), &source,
      &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-docs-index", "doc/a", source,
                            NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"tags\":[\"ops\"],\"n\":2}",
                             strlen("{\"tags\":[\"ops\"],\"n\":2}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-docs-index", "doc/b", source,
                            NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"tags\":[\"finance\"],\"n\":3}",
                             strlen("{\"tags\":[\"finance\"],\"n\":3}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-docs-index", "doc/c", source,
                            NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  hidden_options.has_query_hidden = 1;
  hidden_options.query_hidden = 1;
  rc = lc_source_from_memory("{\"tags\":[\"planning\"],\"n\":4}",
                             strlen("{\"tags\":[\"planning\"],\"n\":4}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-docs-index", "doc/hidden",
                            source, &hidden_options, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"tags\":[\"finance\"],\"n\":5}",
                             strlen("{\"tags\":[\"finance\"],\"n\":5}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-docs-index", "doc/deleted",
                            source, NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  rc = lc_pouch_state_delete(pouch, "docs/query-docs-index", "doc/deleted",
                             NULL, &delete_result, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &delete_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  open_pouch_client(root, &client, &error);
  rc = lc_sink_to_memory(&first_sink, &error);
  assert_int_equal(rc, LC_OK);
  query_req.namespace_name = "docs/query-docs-index";
  query_req.selector_json = selector;
  query_req.limit = 1L;
  query_req.return_mode = "documents";
  query_req.engine = "index";
  query_req.refresh = "wait_for";
  query_req.fields_json = "{\"n\":true}";
  rc = client->query(client, &query_req, first_sink, &query_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(first_sink, &first_bytes, &first_length, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(first_length > 0U);
  assert_non_null(query_res.cursor);
  assert_string_equal(query_res.return_mode, "documents");
  assert_non_null(query_res.metadata_json);
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));

  snprintf(cursor, sizeof(cursor), "%s", query_res.cursor);
  query_req.cursor = cursor;
  lc_query_res_cleanup(&query_res);
  rc = lc_sink_to_memory(&second_sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->query(client, &query_req, second_sink, &query_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(second_sink, &second_bytes, &second_length,
                            &error);
  assert_int_equal(rc, LC_OK);
  assert_true(second_length > 0U);
  assert_null(query_res.cursor);
  assert_true(bytes_contain_text(first_bytes, first_length, "\"n\":1") ||
              bytes_contain_text(second_bytes, second_length, "\"n\":1"));
  assert_true(bytes_contain_text(first_bytes, first_length, "\"n\":3") ||
              bytes_contain_text(second_bytes, second_length, "\"n\":3"));
  assert_false(bytes_contain_text(first_bytes, first_length, "\"n\":2"));
  assert_false(bytes_contain_text(second_bytes, second_length, "\"n\":2"));
  assert_false(bytes_contain_text(first_bytes, first_length, "\"n\":4"));
  assert_false(bytes_contain_text(second_bytes, second_length, "\"n\":4"));
  assert_false(bytes_contain_text(first_bytes, first_length, "\"n\":5"));
  assert_false(bytes_contain_text(second_bytes, second_length, "\"n\":5"));
  lc_query_res_cleanup(&query_res);

  memset(&query_res, 0, sizeof(query_res));
  query_req.cursor = NULL;
  query_req.selector_json = "{\"exists\":\"/tags\"}";
  query_req.limit = 0L;
  rc = lc_sink_to_memory(&exists_sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->query(client, &query_req, exists_sink, &query_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(exists_sink, &exists_bytes, &exists_length,
                            &error);
  assert_int_equal(rc, LC_OK);
  assert_true(exists_length > 0U);
  assert_null(query_res.cursor);
  assert_true(bytes_contain_text(exists_bytes, exists_length, "\"n\":1"));
  assert_true(bytes_contain_text(exists_bytes, exists_length, "\"n\":2"));
  assert_true(bytes_contain_text(exists_bytes, exists_length, "\"n\":3"));
  assert_false(bytes_contain_text(exists_bytes, exists_length, "\"n\":4"));
  assert_false(bytes_contain_text(exists_bytes, exists_length, "\"n\":5"));
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));
  lc_query_res_cleanup(&query_res);

  memset(&query_res, 0, sizeof(query_res));
  query_req.cursor = NULL;
  query_req.selector_json =
      "{\"range\":{\"field\":\"/n\",\"gte\":2,\"lt\":4}}";
  query_req.limit = 0L;
  rc = lc_sink_to_memory(&range_sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->query(client, &query_req, range_sink, &query_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(range_sink, &range_bytes, &range_length, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(range_length > 0U);
  assert_null(query_res.cursor);
  assert_true(bytes_contain_text(range_bytes, range_length, "\"n\":2"));
  assert_true(bytes_contain_text(range_bytes, range_length, "\"n\":3"));
  assert_false(bytes_contain_text(range_bytes, range_length, "\"n\":1"));
  assert_false(bytes_contain_text(range_bytes, range_length, "\"n\":4"));
  assert_false(bytes_contain_text(range_bytes, range_length, "\"n\":5"));
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));
  lc_query_res_cleanup(&query_res);

  memset(&query_res, 0, sizeof(query_res));
  query_req.cursor = NULL;
  query_req.selector_json =
      "{\"iprefix\":{\"field\":\"/tags[]\",\"value\":\"FIN\"}}";
  query_req.limit = 0L;
  rc = lc_sink_to_memory(&iprefix_sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->query(client, &query_req, iprefix_sink, &query_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(iprefix_sink, &iprefix_bytes, &iprefix_length,
                            &error);
  assert_int_equal(rc, LC_OK);
  assert_true(iprefix_length > 0U);
  assert_null(query_res.cursor);
  assert_true(bytes_contain_text(iprefix_bytes, iprefix_length, "\"n\":1"));
  assert_true(bytes_contain_text(iprefix_bytes, iprefix_length, "\"n\":3"));
  assert_false(bytes_contain_text(iprefix_bytes, iprefix_length, "\"n\":2"));
  assert_false(bytes_contain_text(iprefix_bytes, iprefix_length, "\"n\":4"));
  assert_false(bytes_contain_text(iprefix_bytes, iprefix_length, "\"n\":5"));
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));
  lc_query_res_cleanup(&query_res);

  memset(&query_res, 0, sizeof(query_res));
  query_req.cursor = NULL;
  query_req.selector_json =
      "{\"contains\":{\"field\":\"/tags[]\",\"value\":\"nan\"}}";
  query_req.limit = 0L;
  rc = lc_sink_to_memory(&contains_sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->query(client, &query_req, contains_sink, &query_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(contains_sink, &contains_bytes, &contains_length,
                            &error);
  assert_int_equal(rc, LC_OK);
  assert_true(contains_length > 0U);
  assert_null(query_res.cursor);
  assert_true(bytes_contain_text(contains_bytes, contains_length, "\"n\":1"));
  assert_true(bytes_contain_text(contains_bytes, contains_length, "\"n\":3"));
  assert_false(bytes_contain_text(contains_bytes, contains_length, "\"n\":2"));
  assert_false(bytes_contain_text(contains_bytes, contains_length, "\"n\":4"));
  assert_false(bytes_contain_text(contains_bytes, contains_length, "\"n\":5"));
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));
  lc_query_res_cleanup(&query_res);

  memset(&query_res, 0, sizeof(query_res));
  query_req.cursor = NULL;
  query_req.selector_json =
      "{\"icontains\":{\"field\":\"/tags[]\",\"value\":\"INA\"}}";
  query_req.limit = 0L;
  rc = lc_sink_to_memory(&icontains_sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->query(client, &query_req, icontains_sink, &query_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(icontains_sink, &icontains_bytes,
                            &icontains_length, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(icontains_length > 0U);
  assert_null(query_res.cursor);
  assert_true(bytes_contain_text(icontains_bytes, icontains_length, "\"n\":1"));
  assert_true(bytes_contain_text(icontains_bytes, icontains_length, "\"n\":3"));
  assert_false(bytes_contain_text(icontains_bytes, icontains_length, "\"n\":2"));
  assert_false(bytes_contain_text(icontains_bytes, icontains_length, "\"n\":4"));
  assert_false(bytes_contain_text(icontains_bytes, icontains_length, "\"n\":5"));
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));
  lc_query_res_cleanup(&query_res);

  memset(&query_res, 0, sizeof(query_res));
  query_req.cursor = NULL;
  query_req.selector_json =
      "{\"prefix\":{\"field\":\"/tags[]\",\"value\":\"fin\"}}";
  query_req.limit = 0L;
  rc = lc_sink_to_memory(&prefix_sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->query(client, &query_req, prefix_sink, &query_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(prefix_sink, &prefix_bytes, &prefix_length,
                            &error);
  assert_int_equal(rc, LC_OK);
  assert_true(prefix_length > 0U);
  assert_null(query_res.cursor);
  assert_true(bytes_contain_text(prefix_bytes, prefix_length, "\"n\":1"));
  assert_true(bytes_contain_text(prefix_bytes, prefix_length, "\"n\":3"));
  assert_false(bytes_contain_text(prefix_bytes, prefix_length, "\"n\":2"));
  assert_false(bytes_contain_text(prefix_bytes, prefix_length, "\"n\":4"));
  assert_false(bytes_contain_text(prefix_bytes, prefix_length, "\"n\":5"));
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));
  lc_query_res_cleanup(&query_res);

  memset(&query_res, 0, sizeof(query_res));
  query_req.cursor = NULL;
  query_req.selector_json =
      "{\"and\":[{\"eq\":{\"field\":\"/n\",\"value\":\"1\"}}]}";
  rc = lc_sink_to_memory(&unsupported_sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->query(client, &query_req, unsupported_sink, &query_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_true(bytes_contain_text(error.message, strlen(error.message),
                                 "supports exact scalar equality, in, exists, "
                                 "prefix, contains, range, and date"));

  lc_sink_close(first_sink);
  lc_sink_close(second_sink);
  lc_sink_close(exists_sink);
  lc_sink_close(prefix_sink);
  lc_sink_close(contains_sink);
  lc_sink_close(iprefix_sink);
  lc_sink_close(icontains_sink);
  lc_sink_close(range_sink);
  lc_sink_close(unsupported_sink);
  lc_query_res_cleanup(&query_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_flush_index_reports_projection_high_water(void **state) {
  lc_client *client;
  lc_pouch *writer;
  lc_source *source;
  lc_pouch_state_write_options hidden_options;
  lc_pouch_state_write_result write_result;
  lc_pouch_state_write_result live_result;
  lc_pouch_state_write_result hidden_result;
  lc_pouch_state_write_result delete_result;
  lc_index_flush_req flush_req;
  lc_index_flush_res flush_res;
  lc_error error;
  char *namespace_path;
  char sidecar_path[1024];
  char root[512];
  unsigned long delete_version;
  int rc;

  (void)state;
  client = NULL;
  writer = NULL;
  source = NULL;
  namespace_path = NULL;
  memset(&hidden_options, 0, sizeof(hidden_options));
  memset(&write_result, 0, sizeof(write_result));
  memset(&live_result, 0, sizeof(live_result));
  memset(&hidden_result, 0, sizeof(hidden_result));
  memset(&delete_result, 0, sizeof(delete_result));
  lc_index_flush_req_init(&flush_req);
  memset(&flush_res, 0, sizeof(flush_res));
  lc_error_init(&error);
  make_root("flush-index", root, sizeof(root));
  cleanup_root(root);

  open_pouch_client(root, &client, &error);
  rc = lc_pouch_open(root, NULL, NULL, &writer, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("{\"kind\":\"flush\",\"n\":1}",
                             strlen("{\"kind\":\"flush\",\"n\":1}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(writer, "docs/flush", "doc/a", source, NULL,
                            &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"kind\":\"flush\",\"n\":2}",
                             strlen("{\"kind\":\"flush\",\"n\":2}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(writer, "docs/flush", "doc/live", source, NULL,
                            &live_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &live_result);

  hidden_options.has_query_hidden = 1;
  hidden_options.query_hidden = 1;
  rc = lc_source_from_memory("{\"kind\":\"flush\",\"n\":3}",
                             strlen("{\"kind\":\"flush\",\"n\":3}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(writer, "docs/flush", "doc/hidden", source,
                            &hidden_options, &hidden_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &hidden_result);

  rc = lc_pouch_state_delete(writer, "docs/flush", "doc/a", NULL,
                             &delete_result, &error);
  assert_int_equal(rc, LC_OK);
  delete_version = delete_result.version;
  lc_pouch_state_write_result_cleanup(NULL, &delete_result);
  lc_pouch_close(writer);
  writer = NULL;

  flush_req.namespace_name = "docs/flush";
  flush_req.mode = "wait";
  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(flush_res.namespace_name, "docs/flush");
  assert_string_equal(flush_res.mode, "wait");
  assert_string_equal(flush_res.flush_id, "pouch-query-index-repair");
  assert_true(flush_res.accepted);
  assert_true(flush_res.flushed);
  assert_false(flush_res.pending);
  assert_true(flush_res.index_seq >= delete_version);
  assert_string_equal(flush_res.correlation_id, "pouch-index-flush");
  namespace_path = lc_pouch_namespace_path(NULL, root, "docs/flush");
  assert_non_null(namespace_path);
  assert_path_file_contains(namespace_path, "index/query.index",
                            "format=pouch-query-index");
  assert_path_file_contains(namespace_path, "index/query.index", "version=11");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "state_index_seq=4");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "row_count=2");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "term_count=");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "term_field_count=");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "term_value_count=");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "term_field 2f6b696e64 ");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "term_value 2f6b696e64 666c757368 ");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "666c757368 646f632f6c697665 s");
  assert_path_file_contains(namespace_path, "index/query.index",
                            " 0 2f6b696e64 ");
  assert_path_file_contains(namespace_path, "index/query.index",
                            " 1 2f6b696e64 ");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "term_field 2f6e ");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "term_index_complete=1");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "presence_count=");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "presence_index_complete=1");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "summary_hash=");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "646f632f6c697665");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "row 3 ");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "646f632f68696464656e");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "row 2 ");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "2f6b696e64");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "666c757368");
  lc_index_flush_res_cleanup(&flush_res);

  snprintf(sidecar_path, sizeof(sidecar_path), "%s/index/query.index",
           namespace_path);
  write_text_file(sidecar_path,
                  "format=pouch-query-index\nversion=2\nstate_index_seq=4\n"
                  "row_count=1\nsummary_hash=1\nrow 3 22 0 0 "
                  "646f632f6c697665 - -\n");
  flush_req.mode = "sync";
  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(flush_res.mode, "sync");
  assert_string_equal(flush_res.flush_id, "pouch-query-index-repair");
  assert_true(flush_res.index_seq >= delete_version);
  assert_path_file_contains(namespace_path, "index/query.index",
                            "format=pouch-query-index");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "state_index_seq=4");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "row_count=2");
  lc_index_flush_res_cleanup(&flush_res);

  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(flush_res.flush_id, "pouch-query-index-flush");
  lc_index_flush_res_cleanup(&flush_res);

  flush_req.mode = "eventually";
  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message, "pouch flush_index mode must be wait or sync");

  free(namespace_path);
  lc_error_cleanup(&error);
  lc_client_close(client);
  cleanup_root(root);
}

static void test_txn_decisions_persist_participant_records(void **state) {
  lc_client *client;
  lc_client *reader;
  lc_pouch *pouch;
  lc_source *source;
  lc_txn_participant participants[2];
  lc_txn_decision_req decision_req;
  lc_txn_decision_res decision_res;
  lc_txn_replay_req replay_req;
  lc_txn_replay_res replay_res;
  lc_pouch_state_write_result write_result;
  lc_pouch_state_read_result read_result;
  lc_error error;
  char root[512];
  char txn_record[1024];
  char state_bytes[128];
  char namespace_hex[128];
  char key_hex[128];
  char backend_hex[128];
  int rc;

  (void)state;
  client = NULL;
  reader = NULL;
  pouch = NULL;
  source = NULL;
  memset(participants, 0, sizeof(participants));
  memset(&decision_res, 0, sizeof(decision_res));
  memset(&replay_res, 0, sizeof(replay_res));
  memset(&write_result, 0, sizeof(write_result));
  memset(&read_result, 0, sizeof(read_result));
  lc_txn_decision_req_init(&decision_req);
  lc_txn_replay_req_init(&replay_req);
  lc_error_init(&error);
  make_root("txn-records", root, sizeof(root));
  cleanup_root(root);

  participants[0].namespace_name = "orders/eu";
  participants[0].key = "state/order-1";
  participants[0].backend_hash = "backend-a";
  participants[1].namespace_name = "orders/us";
  participants[1].key = "state/order-2";
  participants[1].backend_hash = "backend-b";
  decision_req.txn_id = "txn-pouch-records";
  decision_req.participants = participants;
  decision_req.participant_count = 2U;
  decision_req.expires_at_unix = 2147483647L;
  decision_req.tc_term = 7UL;
  decision_req.target_backend_hash = "target-backend";

  open_pouch_client(root, &client, &error);
  rc = client->txn_prepare(client, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(decision_res.txn_id, "txn-pouch-records");
  assert_string_equal(decision_res.state, "prepare");
  assert_string_equal(decision_res.correlation_id,
                      "pouch-txn-00000000000000000001");
  lc_txn_decision_res_cleanup(&decision_res);
  lc_client_close(client);
  client = NULL;

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("committed-order-1",
                             strlen("committed-order-1"), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_stage_write(pouch, "orders/eu", "state/order-1",
                                  "txn-pouch-records", source, NULL,
                                  &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("committed-order-2",
                             strlen("committed-order-2"), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_stage_write(pouch, "orders/us", "state/order-2",
                                  "txn-pouch-records", source, NULL,
                                  &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("rolled-back-order",
                             strlen("rolled-back-order"), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_stage_write(pouch, "orders/eu", "state/order-1",
                                  "txn-pouch-rollback", source, NULL,
                                  &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  open_pouch_client(root, &reader, &error);
  replay_req.txn_id = "txn-pouch-records";
  rc = reader->txn_replay(reader, &replay_req, &replay_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(replay_res.txn_id, "txn-pouch-records");
  assert_string_equal(replay_res.state, "prepare");
  assert_string_equal(replay_res.correlation_id,
                      "pouch-txn-00000000000000000001");
  lc_txn_replay_res_cleanup(&replay_res);

  rc = reader->txn_commit(reader, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(decision_res.state, "commit");
  assert_string_equal(decision_res.correlation_id,
                      "pouch-txn-00000000000000000002");
  lc_txn_decision_res_cleanup(&decision_res);

  rc = reader->txn_replay(reader, &replay_req, &replay_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(replay_res.state, "commit");
  assert_string_equal(replay_res.correlation_id,
                      "pouch-txn-00000000000000000002");
  lc_txn_replay_res_cleanup(&replay_res);
  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, "orders/eu", "state/order-1",
                           &read_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_result.found);
  read_source_to_string(read_result.body, state_bytes, sizeof(state_bytes));
  assert_string_equal(state_bytes, "committed-order-1");
  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  rc = lc_pouch_state_read(pouch, "orders/eu",
                           "state/order-1/.staging/txn-pouch-records",
                           &read_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(read_result.found);
  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  rc = lc_pouch_state_read(pouch, "orders/eu", "state/order-1",
                           &read_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_result.found);
  read_source_to_string(read_result.body, state_bytes, sizeof(state_bytes));
  assert_string_equal(state_bytes, "committed-order-1");
  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  decision_req.txn_id = "txn-pouch-rollback";
  rc = reader->txn_rollback(reader, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(decision_res.txn_id, "txn-pouch-rollback");
  assert_string_equal(decision_res.state, "rollback");
  assert_string_equal(decision_res.correlation_id,
                      "pouch-txn-00000000000000000003");
  lc_txn_decision_res_cleanup(&decision_res);

  replay_req.txn_id = "txn-pouch-rollback";
  rc = reader->txn_replay(reader, &replay_req, &replay_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(replay_res.txn_id, "txn-pouch-rollback");
  assert_string_equal(replay_res.state, "rollback");
  assert_string_equal(replay_res.correlation_id,
                      "pouch-txn-00000000000000000003");
  lc_txn_replay_res_cleanup(&replay_res);
  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, "orders/eu",
                           "state/order-1/.staging/txn-pouch-rollback",
                           &read_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(read_result.found);
  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_close(pouch);
  pouch = NULL;
  lc_client_close(reader);
  reader = NULL;

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, ".lockd/txn", "txn/txn-pouch-records",
                           &read_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_result.found);
  assert_string_equal(read_result.content_type,
                      "application/x-lockdc-pouch-txn");
  read_source_to_string(read_result.body, txn_record, sizeof(txn_record));
  hex_encode_string("orders/eu", namespace_hex, sizeof(namespace_hex));
  hex_encode_string("state/order-1", key_hex, sizeof(key_hex));
  hex_encode_string("backend-a", backend_hex, sizeof(backend_hex));
  assert_non_null(strstr(txn_record, "state 636f6d6d6974\n"));
  assert_non_null(strstr(txn_record, namespace_hex));
  assert_non_null(strstr(txn_record, key_hex));
  assert_non_null(strstr(txn_record, backend_hex));

  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  open_pouch_client(root, &reader, &error);
  lc_client_close(reader);
  reader = NULL;

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, ".lockd/txn", "txn/txn-pouch-records",
                           &read_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(read_result.found);
  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_txn_decisions_apply_attachment_side_effects(void **state) {
  lc_client *client;
  lc_source *source;
  lc_sink *sink;
  lc_txn_participant participant;
  lc_txn_decision_req decision_req;
  lc_txn_decision_res decision_res;
  lc_attach_op attach_op;
  lc_attach_res attach_res;
  lc_attachment_list_req list_req;
  lc_attachment_list list;
  lc_attachment_get_op get_op;
  lc_attachment_get_res get_res;
  lc_error error;
  const void *bytes;
  size_t length;
  char root[512];
  int rc;

  (void)state;
  client = NULL;
  source = NULL;
  sink = NULL;
  memset(&participant, 0, sizeof(participant));
  lc_txn_decision_req_init(&decision_req);
  memset(&decision_res, 0, sizeof(decision_res));
  lc_attach_op_init(&attach_op);
  memset(&attach_res, 0, sizeof(attach_res));
  lc_attachment_list_req_init(&list_req);
  memset(&list, 0, sizeof(list));
  lc_attachment_get_op_init(&get_op);
  memset(&get_res, 0, sizeof(get_res));
  lc_error_init(&error);
  bytes = NULL;
  length = 0U;
  make_root("txn-attachments", root, sizeof(root));
  cleanup_root(root);

  open_pouch_client(root, &client, &error);
  attach_op.lease.namespace_name = "objects/txn";
  attach_op.lease.key = "state/object-1";
  attach_op.lease.txn_id = "txn-attachment-commit";
  attach_op.name = "report.txt";
  attach_op.content_type = "text/plain";
  attach_op.prevent_overwrite = 1;
  rc = lc_source_from_memory("committed-object", strlen("committed-object"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->attach(client, &attach_op, source, &attach_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_attach_res_cleanup(&attach_res);

  list_req.lease.namespace_name = "objects/txn";
  list_req.lease.key = "state/object-1";
  rc = client->list_attachments(client, &list_req, &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 0U);
  lc_attachment_list_cleanup(&list);

  participant.namespace_name = "objects/txn";
  participant.key = "state/object-1";
  participant.backend_hash = "backend-object";
  decision_req.txn_id = "txn-attachment-commit";
  decision_req.participants = &participant;
  decision_req.participant_count = 1U;
  rc = client->txn_commit(client, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(decision_res.state, "commit");
  lc_txn_decision_res_cleanup(&decision_res);

  rc = client->list_attachments(client, &list_req, &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 1U);
  assert_string_equal(list.items[0].name, "report.txt");
  lc_attachment_list_cleanup(&list);

  get_op.lease.namespace_name = "objects/txn";
  get_op.lease.key = "state/object-1";
  get_op.selector.name = "report.txt";
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get_attachment(client, &get_op, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, strlen("committed-object"));
  assert_memory_equal(bytes, "committed-object", strlen("committed-object"));
  sink->close(sink);
  sink = NULL;
  lc_attachment_get_res_cleanup(&get_res);

  attach_op.lease.txn_id = "txn-attachment-rollback";
  attach_op.name = "rolled-back.txt";
  rc = lc_source_from_memory("rolled-back-object",
                             strlen("rolled-back-object"), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->attach(client, &attach_op, source, &attach_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_attach_res_cleanup(&attach_res);

  decision_req.txn_id = "txn-attachment-rollback";
  rc = client->txn_rollback(client, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(decision_res.state, "rollback");
  lc_txn_decision_res_cleanup(&decision_res);

  rc = client->list_attachments(client, &list_req, &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 1U);
  assert_string_equal(list.items[0].name, "report.txt");
  lc_attachment_list_cleanup(&list);

  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_txn_recovery_applies_attachment_side_effects(void **state) {
  lc_client *client;
  lc_pouch *pouch;
  lc_source *source;
  lc_attach_op attach_op;
  lc_attach_res attach_res;
  lc_attachment_list_req list_req;
  lc_attachment_list list;
  lc_pouch_state_write_result write_result;
  lc_pouch_state_read_result read_result;
  lc_error error;
  char root[512];
  char record[1024];
  char namespace_hex[128];
  char key_hex[128];
  char backend_hex[128];
  int written;
  int rc;

  (void)state;
  client = NULL;
  pouch = NULL;
  source = NULL;
  lc_attach_op_init(&attach_op);
  memset(&attach_res, 0, sizeof(attach_res));
  lc_attachment_list_req_init(&list_req);
  memset(&list, 0, sizeof(list));
  memset(&write_result, 0, sizeof(write_result));
  memset(&read_result, 0, sizeof(read_result));
  lc_error_init(&error);
  make_root("txn-attachment-recovery", root, sizeof(root));
  cleanup_root(root);

  open_pouch_client(root, &client, &error);
  attach_op.lease.namespace_name = "objects/recover";
  attach_op.lease.key = "state/object-2";
  attach_op.lease.txn_id = "txn-attachment-recover";
  attach_op.name = "recovered.txt";
  attach_op.content_type = "text/plain";
  rc = lc_source_from_memory("recovered-object", strlen("recovered-object"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->attach(client, &attach_op, source, &attach_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_attach_res_cleanup(&attach_res);
  lc_client_close(client);
  client = NULL;

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  hex_encode_string("objects/recover", namespace_hex, sizeof(namespace_hex));
  hex_encode_string("state/object-2", key_hex, sizeof(key_hex));
  hex_encode_string("backend-object", backend_hex, sizeof(backend_hex));
  written = snprintf(record, sizeof(record),
                     "format pouch-txn-v1\nstate 636f6d6d6974\n"
                     "expires_at_unix 0\ntc_term 1\n"
                     "target_backend_hash \nparticipant_count 1\n"
                     "participant %s %s %s\n",
                     namespace_hex, key_hex, backend_hex);
  assert_true(written > 0 && (size_t)written < sizeof(record));
  rc = lc_source_from_memory(record, strlen(record), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, ".lockd/txn",
                            "txn/txn-attachment-recover", source, NULL,
                            &write_result, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  open_pouch_client(root, &client, &error);
  list_req.lease.namespace_name = "objects/recover";
  list_req.lease.key = "state/object-2";
  rc = client->list_attachments(client, &list_req, &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 1U);
  assert_string_equal(list.items[0].name, "recovered.txt");
  lc_attachment_list_cleanup(&list);
  lc_client_close(client);
  client = NULL;

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, ".lockd/txn",
                           "txn/txn-attachment-recover", &read_result,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_false(read_result.found);
  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void pouch_attach_text(lc_client *client, const char *namespace_name,
                              const char *key, const char *txn_id,
                              const char *name, const char *body,
                              lc_error *error) {
  lc_attach_op attach_op;
  lc_attach_res attach_res;
  lc_source *source;
  int rc;

  lc_attach_op_init(&attach_op);
  memset(&attach_res, 0, sizeof(attach_res));
  source = NULL;
  attach_op.lease.namespace_name = namespace_name;
  attach_op.lease.key = key;
  attach_op.lease.txn_id = txn_id;
  attach_op.name = name;
  attach_op.content_type = "text/plain";
  rc = lc_source_from_memory(body, strlen(body), &source, error);
  assert_int_equal(rc, LC_OK);
  rc = client->attach(client, &attach_op, source, &attach_res, error);
  source->close(source);
  assert_int_equal(rc, LC_OK);
  lc_attach_res_cleanup(&attach_res);
}

static int pouch_attachment_list_has_name(const lc_attachment_list *list,
                                          const char *name) {
  size_t i;

  for (i = 0U; i < list->count; ++i) {
    if (strcmp(list->items[i].name, name) == 0) {
      return 1;
    }
  }
  return 0;
}

static void test_txn_decisions_apply_mixed_object_queue_side_effects(
    void **state) {
  lc_client *client;
  lc_source *source;
  lc_txn_participant participant;
  lc_txn_decision_req decision_req;
  lc_txn_decision_res decision_res;
  lc_attachment_list_req list_req;
  lc_attachment_list list;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats_res;
  lc_ack_op ack_op;
  lc_ack_res ack_res;
  lc_message *message;
  lc_error error;
  char root[512];
  int rc;

  (void)state;
  client = NULL;
  source = NULL;
  message = NULL;
  memset(&participant, 0, sizeof(participant));
  lc_txn_decision_req_init(&decision_req);
  memset(&decision_res, 0, sizeof(decision_res));
  lc_attachment_list_req_init(&list_req);
  memset(&list, 0, sizeof(list));
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_dequeue_req_init(&dequeue_req);
  lc_queue_stats_req_init(&stats_req);
  memset(&stats_res, 0, sizeof(stats_res));
  memset(&ack_op, 0, sizeof(ack_op));
  memset(&ack_res, 0, sizeof(ack_res));
  lc_error_init(&error);
  make_root("txn-mixed-object-queue", root, sizeof(root));
  cleanup_root(root);

  open_pouch_client(root, &client, &error);
  list_req.lease.namespace_name = "objects/mixed";
  list_req.lease.key = "state/object-queue";
  participant.namespace_name = "objects/mixed";
  participant.key = "state/object-queue";
  participant.backend_hash = "backend-object";
  decision_req.participants = &participant;
  decision_req.participant_count = 1U;
  enqueue_req.namespace_name = "objects/mixed";
  enqueue_req.queue = "mixed-q";
  enqueue_req.visibility_timeout_seconds = 120L;
  dequeue_req.namespace_name = "objects/mixed";
  dequeue_req.queue = "mixed-q";
  dequeue_req.owner = "mixed-worker";
  dequeue_req.visibility_timeout_seconds = 120L;
  stats_req.namespace_name = "objects/mixed";
  stats_req.queue = "mixed-q";

  pouch_attach_text(client, "objects/mixed", "state/object-queue",
                    "txn-mixed-commit", "commit.txt", "commit", &error);
  rc = client->list_attachments(client, &list_req, &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(pouch_attachment_list_has_name(&list, "commit.txt"));
  lc_attachment_list_cleanup(&list);

  rc = lc_source_from_memory("commit-job", strlen("commit-job"), &source,
                             &error);
  assert_int_equal(rc, LC_OK);
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_enqueue_res_cleanup(&enqueue_res);
  dequeue_req.txn_id = "txn-mixed-commit";
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(message);
  ack_op.message.namespace_name = message->namespace_name;
  ack_op.message.queue = message->queue;
  ack_op.message.message_id = message->message_id;
  ack_op.message.lease_id = message->lease_id;
  ack_op.message.txn_id = message->txn_id;
  ack_op.message.fencing_token = message->fencing_token;
  ack_op.message.meta_etag = message->meta_etag;
  rc = client->queue_ack(client, &ack_op, &ack_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(ack_res.acked, 1);
  lc_ack_res_cleanup(&ack_res);
  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats_res.pending_candidates, 1);
  lc_queue_stats_res_cleanup(&stats_res);
  decision_req.txn_id = "txn-mixed-commit";
  rc = client->txn_commit(client, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_txn_decision_res_cleanup(&decision_res);
  rc = client->list_attachments(client, &list_req, &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(pouch_attachment_list_has_name(&list, "commit.txt"));
  lc_attachment_list_cleanup(&list);
  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats_res.pending_candidates, 0);
  lc_queue_stats_res_cleanup(&stats_res);
  message->close(message);
  message = NULL;

  pouch_attach_text(client, "objects/mixed", "state/object-queue",
                    "txn-mixed-rollback", "rollback.txt", "rollback",
                    &error);
  rc = lc_source_from_memory("rollback-job", strlen("rollback-job"), &source,
                             &error);
  assert_int_equal(rc, LC_OK);
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_enqueue_res_cleanup(&enqueue_res);
  dequeue_req.txn_id = "txn-mixed-rollback";
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(message);
  memset(&ack_op, 0, sizeof(ack_op));
  memset(&ack_res, 0, sizeof(ack_res));
  ack_op.message.namespace_name = message->namespace_name;
  ack_op.message.queue = message->queue;
  ack_op.message.message_id = message->message_id;
  ack_op.message.lease_id = message->lease_id;
  ack_op.message.txn_id = message->txn_id;
  ack_op.message.fencing_token = message->fencing_token;
  ack_op.message.meta_etag = message->meta_etag;
  rc = client->queue_ack(client, &ack_op, &ack_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(ack_res.acked, 1);
  lc_ack_res_cleanup(&ack_res);
  decision_req.txn_id = "txn-mixed-rollback";
  rc = client->txn_rollback(client, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_txn_decision_res_cleanup(&decision_res);
  rc = client->list_attachments(client, &list_req, &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(pouch_attachment_list_has_name(&list, "commit.txt"));
  assert_false(pouch_attachment_list_has_name(&list, "rollback.txt"));
  lc_attachment_list_cleanup(&list);
  ack_op.message.txn_id = NULL;
  memset(&ack_res, 0, sizeof(ack_res));
  rc = client->queue_ack(client, &ack_op, &ack_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(ack_res.acked, 1);
  lc_ack_res_cleanup(&ack_res);
  message->close(message);
  message = NULL;

  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_txn_decisions_apply_attachment_delete_and_clear(
    void **state) {
  lc_client *client;
  lc_txn_participant participant;
  lc_txn_decision_req decision_req;
  lc_txn_decision_res decision_res;
  lc_attachment_delete_op delete_op;
  lc_attachment_delete_all_op delete_all_op;
  lc_attachment_list_req list_req;
  lc_attachment_list list;
  lc_error error;
  char root[512];
  int deleted;
  int deleted_count;
  int rc;

  (void)state;
  client = NULL;
  memset(&participant, 0, sizeof(participant));
  lc_txn_decision_req_init(&decision_req);
  memset(&decision_res, 0, sizeof(decision_res));
  lc_attachment_delete_op_init(&delete_op);
  lc_attachment_delete_all_op_init(&delete_all_op);
  lc_attachment_list_req_init(&list_req);
  memset(&list, 0, sizeof(list));
  lc_error_init(&error);
  make_root("txn-attachment-delete", root, sizeof(root));
  cleanup_root(root);

  open_pouch_client(root, &client, &error);
  pouch_attach_text(client, "objects/delete", "state/object-3", NULL,
                    "keep.txt", "keep", &error);
  pouch_attach_text(client, "objects/delete", "state/object-3", NULL,
                    "delete.txt", "delete", &error);
  pouch_attach_text(client, "objects/delete", "state/object-3", NULL,
                    "rollback-delete.txt", "rollback-delete", &error);

  list_req.lease.namespace_name = "objects/delete";
  list_req.lease.key = "state/object-3";
  delete_op.lease.namespace_name = "objects/delete";
  delete_op.lease.key = "state/object-3";
  delete_op.lease.txn_id = "txn-delete-commit";
  delete_op.selector.name = "delete.txt";
  deleted = 0;
  rc = client->delete_attachment(client, &delete_op, &deleted, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(deleted, 1);

  rc = client->list_attachments(client, &list_req, &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(pouch_attachment_list_has_name(&list, "delete.txt"));
  lc_attachment_list_cleanup(&list);

  participant.namespace_name = "objects/delete";
  participant.key = "state/object-3";
  participant.backend_hash = "backend-object";
  decision_req.txn_id = "txn-delete-commit";
  decision_req.participants = &participant;
  decision_req.participant_count = 1U;
  rc = client->txn_commit(client, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_txn_decision_res_cleanup(&decision_res);

  rc = client->list_attachments(client, &list_req, &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(pouch_attachment_list_has_name(&list, "delete.txt"));
  assert_true(pouch_attachment_list_has_name(&list, "keep.txt"));
  assert_true(pouch_attachment_list_has_name(&list, "rollback-delete.txt"));
  lc_attachment_list_cleanup(&list);

  delete_op.lease.txn_id = "txn-delete-rollback";
  delete_op.selector.name = "rollback-delete.txt";
  deleted = 0;
  rc = client->delete_attachment(client, &delete_op, &deleted, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(deleted, 1);
  decision_req.txn_id = "txn-delete-rollback";
  rc = client->txn_rollback(client, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_txn_decision_res_cleanup(&decision_res);
  rc = client->list_attachments(client, &list_req, &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(pouch_attachment_list_has_name(&list, "rollback-delete.txt"));
  lc_attachment_list_cleanup(&list);

  pouch_attach_text(client, "objects/delete", "state/object-3", NULL,
                    "clear-a.txt", "clear-a", &error);
  pouch_attach_text(client, "objects/delete", "state/object-3", NULL,
                    "clear-b.txt", "clear-b", &error);
  pouch_attach_text(client, "objects/delete", "state/object-3",
                    "txn-clear-commit", "staged-clear.txt", "staged-clear",
                    &error);
  delete_all_op.lease.namespace_name = "objects/delete";
  delete_all_op.lease.key = "state/object-3";
  delete_all_op.lease.txn_id = "txn-clear-commit";
  deleted_count = 0;
  rc = client->delete_all_attachments(client, &delete_all_op, &deleted_count,
                                      &error);
  assert_int_equal(rc, LC_OK);
  assert_true(deleted_count >= 4);
  rc = client->list_attachments(client, &list_req, &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(list.count >= 4U);
  lc_attachment_list_cleanup(&list);
  decision_req.txn_id = "txn-clear-commit";
  rc = client->txn_commit(client, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_txn_decision_res_cleanup(&decision_res);
  rc = client->list_attachments(client, &list_req, &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 0U);
  lc_attachment_list_cleanup(&list);

  pouch_attach_text(client, "objects/delete", "state/object-3", NULL,
                    "rollback-clear.txt", "rollback-clear", &error);
  delete_all_op.lease.txn_id = "txn-clear-rollback";
  deleted_count = 0;
  rc = client->delete_all_attachments(client, &delete_all_op, &deleted_count,
                                      &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(deleted_count, 1);
  decision_req.txn_id = "txn-clear-rollback";
  rc = client->txn_rollback(client, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_txn_decision_res_cleanup(&decision_res);
  rc = client->list_attachments(client, &list_req, &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 1U);
  assert_string_equal(list.items[0].name, "rollback-clear.txt");
  lc_attachment_list_cleanup(&list);

  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_txn_recovery_applies_decisions_on_client_open(void **state) {
  static const char committed_body[] =
      "{\"category\":\"planning\",\"value\":\"recovered-commit\"}";
  static const char selector[] =
      "{\"eq\":{\"field\":\"/category\",\"value\":\"planning\"}}";
  lc_client *client;
  lc_pouch *pouch;
  lc_source *source;
  lc_pouch_state_write_result write_result;
  lc_pouch_state_read_result read_result;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_query_key_handler handler;
  pouch_query_key_capture query_capture;
  lc_error error;
  char root[512];
  char record[1024];
  char namespace_hex[128];
  char key_hex[128];
  char backend_hex[128];
  char bytes[128];
  int written;
  int rc;

  (void)state;
  client = NULL;
  pouch = NULL;
  source = NULL;
  memset(&write_result, 0, sizeof(write_result));
  memset(&read_result, 0, sizeof(read_result));
  lc_query_req_init(&query_req);
  memset(&query_res, 0, sizeof(query_res));
  memset(&handler, 0, sizeof(handler));
  memset(&query_capture, 0, sizeof(query_capture));
  lc_error_init(&error);
  make_root("txn-recovery", root, sizeof(root));
  cleanup_root(root);

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  hex_encode_string("orders/recover", namespace_hex, sizeof(namespace_hex));
  hex_encode_string("state/recover-commit", key_hex, sizeof(key_hex));
  hex_encode_string("backend-recover", backend_hex, sizeof(backend_hex));
  written = snprintf(record, sizeof(record),
                     "format pouch-txn-v1\nstate 636f6d6d6974\n"
                     "expires_at_unix 0\ntc_term 1\n"
                     "target_backend_hash \nparticipant_count 1\n"
                     "participant %s %s %s\n",
                     namespace_hex, key_hex, backend_hex);
  assert_true(written > 0 && (size_t)written < sizeof(record));
  rc = lc_source_from_memory(committed_body, strlen(committed_body), &source,
                             &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_stage_write(pouch, "orders/recover",
                                  "state/recover-commit",
                                  "txn-recover-commit", source, NULL,
                                  &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  rc = lc_source_from_memory(record, strlen(record), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, ".lockd/txn", "txn/txn-recover-commit",
                            source, NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  hex_encode_string("state/recover-expired", key_hex, sizeof(key_hex));
  written = snprintf(record, sizeof(record),
                     "format pouch-txn-v1\nstate 70726570617265\n"
                     "expires_at_unix 1\ntc_term 1\n"
                     "target_backend_hash \nparticipant_count 1\n"
                     "participant %s %s %s\n",
                     namespace_hex, key_hex, backend_hex);
  assert_true(written > 0 && (size_t)written < sizeof(record));
  rc = lc_source_from_memory("expired-stage", strlen("expired-stage"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_stage_write(pouch, "orders/recover",
                                  "state/recover-expired",
                                  "txn-recover-expired", source, NULL,
                                  &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  rc = lc_source_from_memory(record, strlen(record), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, ".lockd/txn", "txn/txn-recover-expired",
                            source, NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  open_pouch_client(root, &client, &error);
  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, "orders/recover", "state/recover-commit",
                           &read_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_result.found);
  read_source_to_string(read_result.body, bytes, sizeof(bytes));
  assert_true(bytes_contain_text(bytes, strlen(bytes),
                                 "\"value\":\"recovered-commit\""));
  lc_pouch_state_read_result_cleanup(NULL, &read_result);

  handler.begin = pouch_query_key_begin;
  handler.chunk = pouch_query_key_chunk;
  handler.end = pouch_query_key_end;
  query_req.namespace_name = "orders/recover";
  query_req.selector_json = selector;
  query_req.engine = "index";
  query_req.refresh = "wait_for";
  rc = client->query_keys(client, &query_req, &handler, &query_capture,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(query_capture.count, 1);
  assert_true(pouch_query_capture_has(&query_capture,
                                      "state/recover-commit"));
  assert_null(query_res.cursor);
  assert_non_null(query_res.metadata_json);
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));
  lc_query_res_cleanup(&query_res);

  rc = lc_pouch_state_read(
      pouch, "orders/recover",
      "state/recover-expired/.staging/txn-recover-expired", &read_result,
      &error);
  assert_int_equal(rc, LC_OK);
  assert_false(read_result.found);
  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  rc = lc_pouch_state_read(pouch, "orders/recover", "state/recover-expired",
                           &read_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(read_result.found);
  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  rc = lc_pouch_state_read(pouch, ".lockd/txn", "txn/txn-recover-commit",
                           &read_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(read_result.found);
  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  rc = lc_pouch_state_read(pouch, ".lockd/txn", "txn/txn-recover-expired",
                           &read_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(read_result.found);
  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_lease_remove_tombstones_state_and_refreshes_view(
    void **state) {
  lc_client *client;
  lc_lease *lease;
  lc_source *source;
  lc_sink *sink;
  lc_acquire_req acquire_req;
  lc_get_res get_res;
  lc_error error;
  char root[512];
  char key[96];
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  source = NULL;
  sink = NULL;
  memset(&get_res, 0, sizeof(get_res));
  lc_acquire_req_init(&acquire_req);
  lc_error_init(&error);
  make_root("lease-remove", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/lease-remove/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("{\"value\":12}", strlen("{\"value\":12}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lease->update(lease, source, NULL, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 1L);
  assert_non_null(lease->state_etag);

  rc = lease->remove(lease, NULL, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 0L);
  assert_null(lease->state_etag);

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get(client, key, NULL, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(get_res.no_content);
  sink->close(sink);

  lc_get_res_cleanup(&get_res);
  lease->close(lease);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_acquire_for_update_success_and_rollback(void **state) {
  lc_client *client;
  lc_sink *sink;
  lc_update_res update_res;
  lc_get_res get_res;
  lc_acquire_req acquire_req;
  lc_error error;
  pouch_acquire_for_update_state handler_state;
  const void *bytes;
  size_t length;
  char root[512];
  char key[96];
  int rc;

  (void)state;
  client = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  memset(&update_res, 0, sizeof(update_res));
  memset(&get_res, 0, sizeof(get_res));
  lc_acquire_req_init(&acquire_req);
  lc_error_init(&error);
  make_root("acquire-for-update", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/afu/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  write_client_state(client, key, "{\"value\":1}", NULL, 0L, 0, &update_res,
                     &error);
  lc_update_res_cleanup(&update_res);

  memset(&handler_state, 0, sizeof(handler_state));
  handler_state.expected_snapshot = "\"value\":1";
  handler_state.expected_visible_during_update = "{\"value\":1}";
  handler_state.replacement = "{\"value\":2}";
  handler_state.observer = client;
  handler_state.key = key;
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire_for_update(client, &acquire_req,
                                  pouch_acquire_for_update_handler,
                                  &handler_state, &error);
  if (rc != LC_OK) {
    fail_msg("acquire_for_update failed: %s",
             error.message != NULL ? error.message : "(no message)");
  }
  assert_int_equal(rc, LC_OK);
  assert_int_equal(handler_state.saw_snapshot, 1);
  assert_int_equal(handler_state.saw_staged_invisible, 1);
  assert_int_equal(handler_state.saw_staging_key_rejected, 1);

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get(client, key, NULL, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, strlen("{\"value\":2}"));
  assert_memory_equal(bytes, "{\"value\":2}", strlen("{\"value\":2}"));
  sink->close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);

  lc_error_cleanup(&error);
  lc_error_init(&error);
  memset(&handler_state, 0, sizeof(handler_state));
  handler_state.expected_snapshot = "\"value\":2";
  handler_state.expected_visible_during_update = "{\"value\":2}";
  handler_state.replacement = "{\"value\":3}";
  handler_state.observer = client;
  handler_state.key = key;
  handler_state.fail = 1;
  rc = client->acquire_for_update(client, &acquire_req,
                                  pouch_acquire_for_update_handler,
                                  &handler_state, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "intentional pouch acquire_for_update failure");
  assert_int_equal(handler_state.saw_staged_invisible, 1);
  assert_int_equal(handler_state.saw_staging_key_rejected, 1);

  lc_error_cleanup(&error);
  lc_error_init(&error);
  memset(&get_res, 0, sizeof(get_res));
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get(client, key, NULL, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, strlen("{\"value\":2}"));
  assert_memory_equal(bytes, "{\"value\":2}", strlen("{\"value\":2}"));
  sink->close(sink);

  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_acquire_for_update_rollback_removes_new_state(void **state) {
  lc_client *client;
  lc_sink *sink;
  lc_get_res get_res;
  lc_acquire_req acquire_req;
  lc_error error;
  pouch_acquire_for_update_state handler_state;
  char root[512];
  char key[96];
  int rc;

  (void)state;
  client = NULL;
  sink = NULL;
  memset(&get_res, 0, sizeof(get_res));
  lc_acquire_req_init(&acquire_req);
  lc_error_init(&error);
  make_root("acquire-for-update-new-rollback", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/afu-new/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  memset(&handler_state, 0, sizeof(handler_state));
  handler_state.replacement = "{\"value\":9}";
  handler_state.observer = client;
  handler_state.key = key;
  handler_state.fail = 1;
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire_for_update(client, &acquire_req,
                                  pouch_acquire_for_update_handler,
                                  &handler_state, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "intentional pouch acquire_for_update failure");
  assert_int_equal(handler_state.saw_staged_invisible, 1);
  assert_int_equal(handler_state.saw_staging_key_rejected, 1);

  lc_error_cleanup(&error);
  lc_error_init(&error);
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get(client, key, NULL, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(get_res.no_content);
  sink->close(sink);

  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_index_docid_set_keeps_sorted_unique_docids),
      cmocka_unit_test(test_index_docid_set_merges_sorted_sets),
      cmocka_unit_test(test_index_doc_table_maps_sorted_keys_to_docids),
      cmocka_unit_test(test_index_result_key_list_sorts_and_compacts_docids),
      cmocka_unit_test(test_index_result_row_list_copies_keys_and_metadata),
      cmocka_unit_test(test_index_term_fields_find_sorted_ranges),
      cmocka_unit_test(test_index_term_values_find_sorted_ranges),
      cmocka_unit_test(test_index_term_parses_sidecar_records),
      cmocka_unit_test(test_index_term_rejects_invalid_sidecar_records),
      cmocka_unit_test(test_index_term_keys_sort_find_and_cleanup),
      cmocka_unit_test(test_index_term_keys_build_exact_sorts_and_deduplicates),
      cmocka_unit_test(test_index_term_keys_build_exact_rejects_invalid_terms),
      cmocka_unit_test(test_index_term_keys_build_exact_for_field_values),
      cmocka_unit_test(
          test_index_term_keys_build_exact_for_field_rejects_invalid_values),
      cmocka_unit_test(test_index_term_values_collect_exact_ranges),
      cmocka_unit_test(test_index_term_fields_select_merged_range),
      cmocka_unit_test(test_index_posting_roundtrips_sparse_docids),
      cmocka_unit_test(test_index_posting_roundtrips_dense_docids),
      cmocka_unit_test(test_index_posting_selects_adaptive_encoding),
      cmocka_unit_test(test_open_creates_segmented_root_layout),
      cmocka_unit_test(test_ensure_namespace_creates_per_namespace_layout),
      cmocka_unit_test(
          test_pouch_endpoint_opens_new_backend_without_http_engine),
      cmocka_unit_test(
          test_pouch_endpoint_query_engine_routes_implicit_queries),
      cmocka_unit_test(
          test_pouch_namespace_config_persists_and_routes_implicit_queries),
      cmocka_unit_test(test_pouch_tc_surface_persists_local_single_node_state),
      cmocka_unit_test(test_state_write_read_replays_segment_after_reopen),
      cmocka_unit_test(test_state_write_enforces_expected_etag),
      cmocka_unit_test(test_state_writes_roll_active_manifest_segment),
      cmocka_unit_test(test_state_scheduled_compaction_installs_snapshot),
      cmocka_unit_test(test_maintenance_reports_disabled_without_force),
      cmocka_unit_test(test_maintenance_retention_sweep_deletes_expired_state),
      cmocka_unit_test(
          test_maintenance_creates_namespace_without_prior_writes),
      cmocka_unit_test(test_maintenance_reports_threshold_skip),
      cmocka_unit_test(test_maintenance_force_installs_snapshot),
      cmocka_unit_test(test_manifest_open_ignores_unmanifested_snapshot),
      cmocka_unit_test(test_maintenance_aborts_on_validation_drift),
      cmocka_unit_test(test_maintenance_aborts_on_same_size_segment_drift),
      cmocka_unit_test(test_maintenance_reports_snapshot_write_abort),
      cmocka_unit_test(test_maintenance_reports_interval_skip),
      cmocka_unit_test(test_compaction_retries_manifest_obsolete_cleanup),
      cmocka_unit_test(test_snapshot_high_water_survives_compaction_reopen),
      cmocka_unit_test(test_state_metadata_survives_snapshot_compaction),
      cmocka_unit_test(
          test_namespace_manifest_repairs_from_existing_segments),
      cmocka_unit_test(test_staged_state_writes_durable_decision_records),
      cmocka_unit_test(
          test_staged_decision_recovery_tombstones_interrupted_discard),
      cmocka_unit_test(test_client_update_get_load_roundtrips_state),
      cmocka_unit_test(test_client_update_enforces_state_preconditions),
      cmocka_unit_test(test_client_mutate_applies_plan_and_preconditions),
      cmocka_unit_test(test_client_get_missing_and_public_state_behavior),
      cmocka_unit_test(test_client_attachments_roundtrip_and_delete),
      cmocka_unit_test(test_client_queue_enqueue_dequeue_ack_and_nack),
      cmocka_unit_test(test_txn_decisions_apply_queue_side_effects),
      cmocka_unit_test(
          test_txn_decisions_stage_state_update_mutate_and_index_refresh),
      cmocka_unit_test(test_txn_recovery_applies_queue_side_effects),
      cmocka_unit_test(
          test_client_queue_mutations_touch_notification_marker),
      cmocka_unit_test(test_client_queue_dequeue_batch_returns_page),
      cmocka_unit_test(test_client_queue_dequeue_with_state_uses_pouch_lease),
      cmocka_unit_test(test_client_queue_ttl_and_retry_terminal_states),
      cmocka_unit_test(test_client_queue_subscribe_polling_paths),
      cmocka_unit_test(test_client_queue_watch_polling_detects_change),
      cmocka_unit_test(
          test_client_queue_watch_detects_transaction_ack_commit),
      cmocka_unit_test(
          test_client_queue_watch_detects_peer_transaction_ack_commit),
      cmocka_unit_test(
          test_client_queue_watch_detects_forked_transaction_ack_commit),
      cmocka_unit_test(
          test_client_remove_tombstones_state_and_enforces_preconditions),
      cmocka_unit_test(test_state_mutations_touch_writer_marker),
      cmocka_unit_test(test_marker_snapshots_detect_peer_changes),
      cmocka_unit_test(
          test_marker_snapshots_treat_same_process_handles_as_peers),
      cmocka_unit_test(
          test_marker_refresh_uses_directory_fast_path_and_force),
      cmocka_unit_test(test_single_writer_state_read_uses_projection_cache),
      cmocka_unit_test(
          test_shared_state_projection_cache_refreshes_peer_markers),
      cmocka_unit_test(test_lease_bound_state_update_get_and_release),
      cmocka_unit_test(test_lease_mutate_and_local_mutate_refresh_state),
      cmocka_unit_test(test_lease_attachments_use_pouch_object_store),
      cmocka_unit_test(
          test_lease_save_streams_mapped_json_and_replays_after_reopen),
      cmocka_unit_test(test_lease_keepalive_and_release_use_local_lifecycle),
      cmocka_unit_test(test_acquire_rejects_non_positive_ttl),
      cmocka_unit_test(test_lease_metadata_persists_query_hidden),
      cmocka_unit_test(test_client_metadata_enforces_version_precondition),
      cmocka_unit_test(test_query_keys_scan_uses_liblql_and_query_hidden),
      cmocka_unit_test(test_query_keys_enforces_lockd_limit_contract),
      cmocka_unit_test(test_query_keys_index_summary_uses_sidecar_rows),
      cmocka_unit_test(test_query_keys_index_scalar_in_uses_array_postings),
      cmocka_unit_test(test_query_keys_index_preserves_json_scalar_types),
      cmocka_unit_test(test_query_keys_index_root_or_uses_scalar_union),
      cmocka_unit_test(test_query_keys_index_text_stops_after_target_field),
      cmocka_unit_test(
          test_query_keys_index_date_lql_filters_temporal_candidates),
      cmocka_unit_test(
          test_query_keys_index_recursive_exists_uses_container_presence),
      cmocka_unit_test(test_query_documents_scan_streams_rows),
      cmocka_unit_test(test_query_documents_index_uses_scalar_postings),
      cmocka_unit_test(test_flush_index_reports_projection_high_water),
      cmocka_unit_test(test_txn_decisions_persist_participant_records),
      cmocka_unit_test(test_txn_decisions_apply_attachment_side_effects),
      cmocka_unit_test(test_txn_recovery_applies_attachment_side_effects),
      cmocka_unit_test(
          test_txn_decisions_apply_mixed_object_queue_side_effects),
      cmocka_unit_test(test_txn_decisions_apply_attachment_delete_and_clear),
      cmocka_unit_test(test_txn_recovery_applies_decisions_on_client_open),
      cmocka_unit_test(test_lease_remove_tombstones_state_and_refreshes_view),
      cmocka_unit_test(test_acquire_for_update_success_and_rollback),
      cmocka_unit_test(test_acquire_for_update_rollback_removes_new_state),
  };

  return cmocka_run_group_tests(tests, setup_pouch_unit_group,
                                teardown_pouch_unit_group);
}

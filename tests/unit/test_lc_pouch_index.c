#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include <cmocka.h>

#include "lc_pouch_index.h"

static void assert_doc_ids(const lc_pouch_index_doc_id_set *set,
                           const lc_pouch_index_doc_id *expected,
                           size_t expected_count) {
  size_t index;

  assert_non_null(set);
  assert_int_equal(set->count, expected_count);
  for (index = 0U; index < expected_count; ++index) {
    assert_int_equal(set->items[index], expected[index]);
  }
}

static int set_from_values(lc_pouch_index_doc_id_set *set,
                           const lc_pouch_index_doc_id *values, size_t count);

static void test_doc_id_set_sort_unique(void **state) {
  lc_pouch_index_doc_id_set set;
  lc_pouch_index_doc_id expected[] = {1U, 2U, 3U, 5U, 8U};

  (void)state;
  memset(&set, 0, sizeof(set));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, &set, 5U));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, &set, 1U));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, &set, 5U));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, &set, 3U));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, &set, 2U));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, &set, 8U));

  assert_true(lc_pouch_index_doc_id_set_sort_unique(&set));
  assert_doc_ids(&set, expected, sizeof(expected) / sizeof(expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &set);
}

static void test_doc_id_set_algebra(void **state) {
  lc_pouch_index_doc_id_set left;
  lc_pouch_index_doc_id_set right;
  lc_pouch_index_doc_id_set out;
  lc_pouch_index_doc_id left_values[] = {1U, 3U, 5U, 7U, 9U};
  lc_pouch_index_doc_id right_values[] = {3U, 4U, 7U, 10U};
  lc_pouch_index_doc_id union_expected[] = {1U, 3U, 4U, 5U, 7U, 9U, 10U};
  lc_pouch_index_doc_id intersect_expected[] = {3U, 7U};
  lc_pouch_index_doc_id subtract_expected[] = {1U, 5U, 9U};
  size_t index;

  (void)state;
  memset(&left, 0, sizeof(left));
  memset(&right, 0, sizeof(right));
  memset(&out, 0, sizeof(out));
  for (index = 0U; index < sizeof(left_values) / sizeof(left_values[0]);
       ++index) {
    assert_true(
        lc_pouch_index_doc_id_set_append(NULL, &left, left_values[index]));
  }
  for (index = 0U; index < sizeof(right_values) / sizeof(right_values[0]);
       ++index) {
    assert_true(
        lc_pouch_index_doc_id_set_append(NULL, &right, right_values[index]));
  }

  assert_true(lc_pouch_index_doc_id_set_union(NULL, &out, &left, &right));
  assert_doc_ids(&out, union_expected,
                 sizeof(union_expected) / sizeof(union_expected[0]));

  assert_true(lc_pouch_index_doc_id_set_intersect(NULL, &out, &left, &right));
  assert_doc_ids(&out, intersect_expected,
                 sizeof(intersect_expected) / sizeof(intersect_expected[0]));

  assert_true(lc_pouch_index_doc_id_set_subtract(NULL, &out, &left, &right));
  assert_doc_ids(&out, subtract_expected,
                 sizeof(subtract_expected) / sizeof(subtract_expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &out);
  lc_pouch_index_doc_id_set_cleanup(NULL, &right);
  lc_pouch_index_doc_id_set_cleanup(NULL, &left);
}

static void test_doc_id_set_algebra_allows_alias_destination(void **state) {
  lc_pouch_index_doc_id_set left;
  lc_pouch_index_doc_id_set right;
  lc_pouch_index_doc_id left_values[] = {1U, 3U, 5U, 7U, 9U};
  lc_pouch_index_doc_id right_values[] = {3U, 4U, 7U, 10U};
  lc_pouch_index_doc_id union_expected[] = {1U, 3U, 4U, 5U, 7U, 9U, 10U};
  lc_pouch_index_doc_id intersect_expected[] = {3U, 7U};
  lc_pouch_index_doc_id subtract_expected[] = {1U, 5U, 9U};

  (void)state;
  assert_true(set_from_values(&left, left_values,
                              sizeof(left_values) / sizeof(left_values[0])));
  assert_true(set_from_values(&right, right_values,
                              sizeof(right_values) / sizeof(right_values[0])));

  assert_true(lc_pouch_index_doc_id_set_union(NULL, &left, &left, &right));
  assert_doc_ids(&left, union_expected,
                 sizeof(union_expected) / sizeof(union_expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &left);
  assert_true(set_from_values(&left, left_values,
                              sizeof(left_values) / sizeof(left_values[0])));
  assert_true(lc_pouch_index_doc_id_set_intersect(NULL, &right, &left, &right));
  assert_doc_ids(&right, intersect_expected,
                 sizeof(intersect_expected) / sizeof(intersect_expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &right);
  assert_true(set_from_values(&right, right_values,
                              sizeof(right_values) / sizeof(right_values[0])));
  assert_true(lc_pouch_index_doc_id_set_subtract(NULL, &left, &left, &right));
  assert_doc_ids(&left, subtract_expected,
                 sizeof(subtract_expected) / sizeof(subtract_expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &right);
  lc_pouch_index_doc_id_set_cleanup(NULL, &left);
}

static int set_from_values(lc_pouch_index_doc_id_set *set,
                           const lc_pouch_index_doc_id *values, size_t count) {
  size_t index;

  memset(set, 0, sizeof(*set));
  for (index = 0U; index < count; ++index) {
    if (!lc_pouch_index_doc_id_set_append(NULL, set, values[index])) {
      return 0;
    }
  }
  return 1;
}

static void test_posting_sparse_decodes_sorted_unique_doc_ids(void **state) {
  lc_pouch_index_posting posting;
  lc_pouch_index_doc_id_set decoded;
  lc_pouch_index_doc_id ids[] = {99U, 7U, 7U, 1024U, 31U};
  lc_pouch_index_doc_id expected[] = {7U, 31U, 99U, 1024U};

  (void)state;
  memset(&posting, 0, sizeof(posting));
  memset(&decoded, 0, sizeof(decoded));

  assert_true(lc_pouch_index_posting_build(NULL, &posting, ids,
                                           sizeof(ids) / sizeof(ids[0])));
  assert_int_equal(posting.encoding, LC_POUCH_INDEX_POSTING_SPARSE);
  assert_true(lc_pouch_index_posting_decode(NULL, &posting, &decoded));
  assert_doc_ids(&decoded, expected, sizeof(expected) / sizeof(expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &decoded);
  lc_pouch_index_posting_cleanup(NULL, &posting);
}

static void test_posting_sparse_handles_max_doc_id(void **state) {
  lc_pouch_index_posting posting;
  lc_pouch_index_doc_id_set decoded;
  lc_pouch_index_doc_id ids[] = {UINT32_MAX};
  lc_pouch_index_doc_id expected[] = {UINT32_MAX};

  (void)state;
  memset(&posting, 0, sizeof(posting));
  memset(&decoded, 0, sizeof(decoded));

  assert_true(lc_pouch_index_posting_build(NULL, &posting, ids,
                                           sizeof(ids) / sizeof(ids[0])));
  assert_int_equal(posting.encoding, LC_POUCH_INDEX_POSTING_SPARSE);
  assert_true(lc_pouch_index_posting_decode(NULL, &posting, &decoded));
  assert_doc_ids(&decoded, expected, sizeof(expected) / sizeof(expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &decoded);
  lc_pouch_index_posting_cleanup(NULL, &posting);
}

static void test_posting_dense_decodes_and_intersects(void **state) {
  lc_pouch_index_posting posting;
  lc_pouch_index_doc_id_set filter;
  lc_pouch_index_doc_id_set decoded;
  lc_pouch_index_doc_id_set intersected;
  lc_pouch_index_doc_id ids[128];
  lc_pouch_index_doc_id filter_values[] = {0U, 1U, 5U, 63U, 64U, 127U, 200U};
  lc_pouch_index_doc_id intersect_expected[] = {0U, 1U, 5U, 63U, 64U, 127U};
  size_t index;

  (void)state;
  memset(&posting, 0, sizeof(posting));
  memset(&filter, 0, sizeof(filter));
  memset(&decoded, 0, sizeof(decoded));
  memset(&intersected, 0, sizeof(intersected));
  for (index = 0U; index < sizeof(ids) / sizeof(ids[0]); ++index) {
    ids[index] = (lc_pouch_index_doc_id)index;
  }

  assert_true(lc_pouch_index_posting_build(NULL, &posting, ids,
                                           sizeof(ids) / sizeof(ids[0])));
  assert_int_equal(posting.encoding, LC_POUCH_INDEX_POSTING_DENSE);
  assert_true(lc_pouch_index_posting_decode(NULL, &posting, &decoded));
  assert_int_equal(decoded.count, 128U);
  assert_int_equal(decoded.items[0], 0U);
  assert_int_equal(decoded.items[127], 127U);

  assert_true(
      set_from_values(&filter, filter_values,
                      sizeof(filter_values) / sizeof(filter_values[0])));
  assert_true(
      lc_pouch_index_posting_intersect(NULL, &posting, &filter, &intersected));
  assert_doc_ids(&intersected, intersect_expected,
                 sizeof(intersect_expected) / sizeof(intersect_expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &intersected);
  lc_pouch_index_doc_id_set_cleanup(NULL, &decoded);
  lc_pouch_index_doc_id_set_cleanup(NULL, &filter);
  lc_pouch_index_posting_cleanup(NULL, &posting);
}

static void test_term_table_interns_sorted_terms_with_stable_ids(void **state) {
  lc_pouch_index_term_table table;
  lc_pouch_index_term_id first_id;
  lc_pouch_index_term_id second_id;
  lc_pouch_index_term_id duplicate_id;
  lc_pouch_index_term_id found_id;

  (void)state;
  memset(&table, 0, sizeof(table));

  assert_true(lc_pouch_index_term_table_find_or_add(NULL, &table, "/region",
                                                    "s:north", &first_id));
  assert_true(lc_pouch_index_term_table_find_or_add(NULL, &table, "/kind",
                                                    "s:invoice", &second_id));
  assert_true(lc_pouch_index_term_table_find_or_add(NULL, &table, "/region",
                                                    "s:north", &duplicate_id));

  assert_int_equal(first_id, 0U);
  assert_int_equal(second_id, 1U);
  assert_int_equal(duplicate_id, first_id);
  assert_int_equal(table.count, 2U);
  assert_string_equal(table.entries[0].field, "/kind");
  assert_string_equal(table.entries[0].value, "s:invoice");
  assert_int_equal(table.entries[0].id, second_id);
  assert_string_equal(table.entries[1].field, "/region");
  assert_string_equal(table.entries[1].value, "s:north");
  assert_int_equal(table.entries[1].id, first_id);

  assert_true(
      lc_pouch_index_term_table_find(&table, "/region", "s:north", &found_id));
  assert_int_equal(found_id, first_id);
  assert_false(
      lc_pouch_index_term_table_find(&table, "/region", "s:south", NULL));

  lc_pouch_index_term_table_cleanup(NULL, &table);
}

static void test_term_posting_table_decodes_by_term_id(void **state) {
  lc_pouch_index_term_posting_table table;
  lc_pouch_index_doc_id_set decoded;
  lc_pouch_index_doc_id first_ids[] = {9U, 3U, 3U, 7U};
  lc_pouch_index_doc_id second_ids[128];
  lc_pouch_index_doc_id first_expected[] = {3U, 7U, 9U};
  size_t index;

  (void)state;
  memset(&table, 0, sizeof(table));
  memset(&decoded, 0, sizeof(decoded));
  for (index = 0U; index < sizeof(second_ids) / sizeof(second_ids[0]);
       ++index) {
    second_ids[index] = (lc_pouch_index_doc_id)index;
  }

  assert_true(lc_pouch_index_term_posting_table_put(
      NULL, &table, 42U, first_ids, sizeof(first_ids) / sizeof(first_ids[0])));
  assert_true(lc_pouch_index_term_posting_table_put(
      NULL, &table, 7U, second_ids,
      sizeof(second_ids) / sizeof(second_ids[0])));

  assert_true(lc_pouch_index_term_posting_table_contains(&table, 42U));
  assert_false(lc_pouch_index_term_posting_table_contains(&table, 99U));
  assert_int_equal(table.count, 2U);
  assert_int_equal(table.entries[0].term_id, 7U);
  assert_int_equal(table.entries[0].posting.encoding,
                   LC_POUCH_INDEX_POSTING_DENSE);
  assert_int_equal(table.entries[1].term_id, 42U);
  assert_int_equal(table.entries[1].posting.encoding,
                   LC_POUCH_INDEX_POSTING_SPARSE);

  assert_true(
      lc_pouch_index_term_posting_table_decode(NULL, &table, 42U, &decoded));
  assert_doc_ids(&decoded, first_expected,
                 sizeof(first_expected) / sizeof(first_expected[0]));

  assert_true(
      lc_pouch_index_term_posting_table_decode(NULL, &table, 99U, &decoded));
  assert_doc_ids(&decoded, NULL, 0U);

  lc_pouch_index_doc_id_set_cleanup(NULL, &decoded);
  lc_pouch_index_term_posting_table_cleanup(NULL, &table);
}

static void test_term_posting_table_replaces_existing_posting(void **state) {
  lc_pouch_index_term_posting_table table;
  lc_pouch_index_doc_id_set decoded;
  lc_pouch_index_doc_id initial_ids[] = {1U, 4U, 8U};
  lc_pouch_index_doc_id replacement_ids[] = {2U, 4U, 4U};
  lc_pouch_index_doc_id expected[] = {2U, 4U};

  (void)state;
  memset(&table, 0, sizeof(table));
  memset(&decoded, 0, sizeof(decoded));

  assert_true(lc_pouch_index_term_posting_table_put(
      NULL, &table, 3U, initial_ids,
      sizeof(initial_ids) / sizeof(initial_ids[0])));
  assert_true(lc_pouch_index_term_posting_table_put(
      NULL, &table, 3U, replacement_ids,
      sizeof(replacement_ids) / sizeof(replacement_ids[0])));
  assert_int_equal(table.count, 1U);

  assert_true(
      lc_pouch_index_term_posting_table_decode(NULL, &table, 3U, &decoded));
  assert_doc_ids(&decoded, expected, sizeof(expected) / sizeof(expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &decoded);
  lc_pouch_index_term_posting_table_cleanup(NULL, &table);
}

static void test_result_cache_keys_by_generation_and_plan(void **state) {
  lc_pouch_index_result_cache cache;
  lc_pouch_index_doc_id_set source;
  lc_pouch_index_doc_id_set found;
  lc_pouch_index_doc_id source_values[] = {8U, 2U, 8U, 5U};
  lc_pouch_index_doc_id expected[] = {2U, 5U, 8U};

  (void)state;
  memset(&cache, 0, sizeof(cache));
  memset(&source, 0, sizeof(source));
  memset(&found, 0, sizeof(found));
  assert_true(
      set_from_values(&source, source_values,
                      sizeof(source_values) / sizeof(source_values[0])));

  assert_true(lc_pouch_index_result_cache_put(NULL, &cache, 11U,
                                              "eq:/region=s:north", &source));
  assert_true(lc_pouch_index_result_cache_find(NULL, &cache, 11U,
                                               "eq:/region=s:north", &found));
  assert_doc_ids(&found, expected, sizeof(expected) / sizeof(expected[0]));
  assert_false(lc_pouch_index_result_cache_find(NULL, &cache, 12U,
                                                "eq:/region=s:north", &found));
  assert_false(lc_pouch_index_result_cache_find(NULL, &cache, 11U,
                                                "eq:/region=s:south", &found));

  lc_pouch_index_doc_id_set_cleanup(NULL, &found);
  lc_pouch_index_doc_id_set_cleanup(NULL, &source);
  lc_pouch_index_result_cache_cleanup(NULL, &cache);
}

static void test_result_cache_replaces_existing_entry(void **state) {
  lc_pouch_index_result_cache cache;
  lc_pouch_index_doc_id_set first;
  lc_pouch_index_doc_id_set second;
  lc_pouch_index_doc_id_set found;
  lc_pouch_index_doc_id first_values[] = {1U, 4U};
  lc_pouch_index_doc_id second_values[] = {9U, 3U, 3U};
  lc_pouch_index_doc_id expected[] = {3U, 9U};

  (void)state;
  memset(&cache, 0, sizeof(cache));
  memset(&first, 0, sizeof(first));
  memset(&second, 0, sizeof(second));
  memset(&found, 0, sizeof(found));
  assert_true(set_from_values(&first, first_values,
                              sizeof(first_values) / sizeof(first_values[0])));
  assert_true(
      set_from_values(&second, second_values,
                      sizeof(second_values) / sizeof(second_values[0])));

  assert_true(lc_pouch_index_result_cache_put(NULL, &cache, 4U, "exists:/tags",
                                              &first));
  assert_true(lc_pouch_index_result_cache_put(NULL, &cache, 4U, "exists:/tags",
                                              &second));
  assert_int_equal(cache.count, 1U);
  assert_true(lc_pouch_index_result_cache_find(NULL, &cache, 4U, "exists:/tags",
                                               &found));
  assert_doc_ids(&found, expected, sizeof(expected) / sizeof(expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &found);
  lc_pouch_index_doc_id_set_cleanup(NULL, &second);
  lc_pouch_index_doc_id_set_cleanup(NULL, &first);
  lc_pouch_index_result_cache_cleanup(NULL, &cache);
}

typedef struct fake_exact_reader {
  size_t calls;
} fake_exact_reader;

static int fake_read_exact_doc_ids(void *context, const char *field,
                                   const char *value,
                                   lc_pouch_index_doc_id_set *doc_ids,
                                   lc_error *error) {
  fake_exact_reader *reader;

  (void)error;
  reader = (fake_exact_reader *)context;
  assert_non_null(reader);
  assert_string_equal(field, "/region");
  reader->calls++;
  if (strcmp(value, "s:north") == 0) {
    assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 7U));
    assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 3U));
  } else if (strcmp(value, "s:south") == 0) {
    assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 3U));
    assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 9U));
  } else {
    fail_msg("unexpected exact term value: %s", value);
  }
  return LC_OK;
}

static int fake_read_exists_doc_ids(void *context, const char *field,
                                    lc_pouch_index_doc_id_set *doc_ids,
                                    lc_error *error) {
  fake_exact_reader *reader;

  (void)error;
  reader = (fake_exact_reader *)context;
  assert_non_null(reader);
  assert_string_equal(field, "/tags/0");
  reader->calls++;
  assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 11U));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 5U));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 11U));
  return LC_OK;
}

static int fake_read_range_doc_ids(void *context,
                                   const lc_pouch_document_range_term *term,
                                   lc_pouch_index_doc_id_set *doc_ids,
                                   lc_error *error) {
  fake_exact_reader *reader;

  (void)error;
  reader = (fake_exact_reader *)context;
  assert_non_null(reader);
  assert_non_null(term);
  assert_string_equal(term->field, "/score");
  assert_string_equal(term->gte, "n:+:1:0");
  assert_string_equal(term->lt, "n:+:9:0");
  reader->calls++;
  assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 8U));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 2U));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 8U));
  return LC_OK;
}

static void
test_collect_in_term_doc_ids_uses_reader_and_deduplicates(void **state) {
  const char *values[3];
  lc_pouch_document_in_term term;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {3U, 7U, 9U};
  fake_exact_reader reader;
  lc_error error;
  int rc;

  (void)state;
  memset(&term, 0, sizeof(term));
  memset(&doc_ids, 0, sizeof(doc_ids));
  memset(&reader, 0, sizeof(reader));
  memset(&error, 0, sizeof(error));

  values[0] = "s:north";
  values[1] = NULL;
  values[2] = "s:south";
  term.field = "/region";
  term.values = values;
  term.value_count = 3U;

  rc = lc_pouch_index_collect_in_term_doc_ids(
      NULL, &term, fake_read_exact_doc_ids, &reader, &doc_ids, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(reader.calls, 2U);
  assert_doc_ids(&doc_ids, expected, sizeof(expected) / sizeof(expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_error_cleanup(&error);
}

static void
test_collect_eq_term_doc_ids_uses_reader_and_deduplicates(void **state) {
  lc_pouch_document_eq_term term;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {3U, 7U};
  fake_exact_reader reader;
  lc_error error;
  int rc;

  (void)state;
  memset(&term, 0, sizeof(term));
  memset(&doc_ids, 0, sizeof(doc_ids));
  memset(&reader, 0, sizeof(reader));
  memset(&error, 0, sizeof(error));

  term.field = "/region";
  term.value = "s:north";
  rc = lc_pouch_index_collect_eq_term_doc_ids(
      NULL, &term, fake_read_exact_doc_ids, &reader, &doc_ids, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(reader.calls, 1U);
  assert_doc_ids(&doc_ids, expected, sizeof(expected) / sizeof(expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_error_cleanup(&error);
}

static void
test_collect_exists_term_doc_ids_uses_reader_and_deduplicates(void **state) {
  lc_pouch_document_exists_term term;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {5U, 11U};
  fake_exact_reader reader;
  lc_error error;
  int rc;

  (void)state;
  memset(&term, 0, sizeof(term));
  memset(&doc_ids, 0, sizeof(doc_ids));
  memset(&reader, 0, sizeof(reader));
  memset(&error, 0, sizeof(error));

  term.field = "/tags/0";
  rc = lc_pouch_index_collect_exists_term_doc_ids(
      NULL, &term, fake_read_exists_doc_ids, &reader, &doc_ids, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(reader.calls, 1U);
  assert_doc_ids(&doc_ids, expected, sizeof(expected) / sizeof(expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_error_cleanup(&error);
}

static void
test_collect_range_term_doc_ids_uses_reader_and_deduplicates(void **state) {
  lc_pouch_document_range_term term;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {2U, 8U};
  fake_exact_reader reader;
  lc_error error;
  int rc;

  (void)state;
  memset(&term, 0, sizeof(term));
  memset(&doc_ids, 0, sizeof(doc_ids));
  memset(&reader, 0, sizeof(reader));
  memset(&error, 0, sizeof(error));

  term.field = "/score";
  term.gte = "n:+:1:0";
  term.lt = "n:+:9:0";
  rc = lc_pouch_index_collect_range_term_doc_ids(
      NULL, &term, fake_read_range_doc_ids, &reader, &doc_ids, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(reader.calls, 1U);
  assert_doc_ids(&doc_ids, expected, sizeof(expected) / sizeof(expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_error_cleanup(&error);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_doc_id_set_sort_unique),
      cmocka_unit_test(test_doc_id_set_algebra),
      cmocka_unit_test(test_doc_id_set_algebra_allows_alias_destination),
      cmocka_unit_test(test_posting_sparse_decodes_sorted_unique_doc_ids),
      cmocka_unit_test(test_posting_sparse_handles_max_doc_id),
      cmocka_unit_test(test_posting_dense_decodes_and_intersects),
      cmocka_unit_test(test_term_table_interns_sorted_terms_with_stable_ids),
      cmocka_unit_test(test_term_posting_table_decodes_by_term_id),
      cmocka_unit_test(test_term_posting_table_replaces_existing_posting),
      cmocka_unit_test(test_result_cache_keys_by_generation_and_plan),
      cmocka_unit_test(test_result_cache_replaces_existing_entry),
      cmocka_unit_test(
          test_collect_in_term_doc_ids_uses_reader_and_deduplicates),
      cmocka_unit_test(
          test_collect_eq_term_doc_ids_uses_reader_and_deduplicates),
      cmocka_unit_test(
          test_collect_exists_term_doc_ids_uses_reader_and_deduplicates),
      cmocka_unit_test(
          test_collect_range_term_doc_ids_uses_reader_and_deduplicates),
  };
  return cmocka_run_group_tests(tests, NULL, NULL);
}

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
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

static void test_doc_table_assigns_dense_ids_by_namespace_key(void **state) {
  lc_pouch_index_doc_table table;
  lc_pouch_index_doc_id id_alpha;
  lc_pouch_index_doc_id id_bravo;
  lc_pouch_index_doc_id id_other;
  lc_pouch_index_doc_id duplicate;
  const char *namespace_name;
  const char *key;

  (void)state;
  memset(&table, 0, sizeof(table));

  assert_true(lc_pouch_index_doc_table_find_or_add(NULL, &table, "default",
                                                   "bravo", &id_bravo));
  assert_true(lc_pouch_index_doc_table_find_or_add(NULL, &table, "default",
                                                   "alpha", &id_alpha));
  assert_true(lc_pouch_index_doc_table_find_or_add(NULL, &table, "other",
                                                   "alpha", &id_other));
  assert_true(lc_pouch_index_doc_table_find_or_add(NULL, &table, "default",
                                                   "alpha", &duplicate));

  assert_int_equal(table.count, 3U);
  assert_int_equal(id_bravo, 0U);
  assert_int_equal(id_alpha, 0U);
  assert_int_equal(duplicate, 0U);
  assert_int_equal(id_other, 2U);
  assert_true(
      lc_pouch_index_doc_table_find(&table, "default", "bravo", &id_bravo));
  assert_int_equal(id_bravo, 1U);

  namespace_name = NULL;
  key = NULL;
  assert_true(
      lc_pouch_index_doc_table_lookup(&table, 0U, &namespace_name, &key));
  assert_string_equal(namespace_name, "default");
  assert_string_equal(key, "alpha");
  assert_true(
      lc_pouch_index_doc_table_lookup(&table, 1U, &namespace_name, &key));
  assert_string_equal(namespace_name, "default");
  assert_string_equal(key, "bravo");
  assert_true(
      lc_pouch_index_doc_table_lookup(&table, 2U, &namespace_name, &key));
  assert_string_equal(namespace_name, "other");
  assert_string_equal(key, "alpha");
  assert_false(lc_pouch_index_doc_table_lookup(&table, 3U, NULL, NULL));

  lc_pouch_index_doc_table_cleanup(NULL, &table);
}

static void test_doc_generation_codec_round_trips(void **state) {
  lc_pouch_index_doc_generation generation;
  lc_pouch_index_doc_generation decoded;
  lc_pouch_index_doc_id id;
  const char *namespace_name;
  const char *key;
  unsigned char *encoded;
  size_t encoded_size;
  size_t written;

  (void)state;
  memset(&generation, 0, sizeof(generation));
  memset(&decoded, 0, sizeof(decoded));
  generation.identity.sequence = 123U;
  generation.identity.manifest_generation = 456U;
  generation.namespace_name = (char *)malloc(8U);
  assert_non_null(generation.namespace_name);
  memcpy(generation.namespace_name, "default", 8U);
  assert_true(lc_pouch_index_doc_table_find_or_add(NULL, &generation.docs,
                                                   "default", "bravo", &id));
  assert_true(lc_pouch_index_doc_table_find_or_add(NULL, &generation.docs,
                                                   "default", "alpha", &id));

  assert_true(
      lc_pouch_index_doc_generation_encoded_size(&generation, &encoded_size));
  encoded = (unsigned char *)malloc(encoded_size);
  assert_non_null(encoded);
  assert_true(lc_pouch_index_doc_generation_encode(&generation, encoded,
                                                   encoded_size, &written));
  assert_int_equal(written, encoded_size);

  assert_true(lc_pouch_index_doc_generation_decode(NULL, &decoded, encoded,
                                                   encoded_size));
  assert_int_equal(decoded.identity.sequence, 123U);
  assert_int_equal(decoded.identity.manifest_generation, 456U);
  assert_string_equal(decoded.namespace_name, "default");
  assert_int_equal(decoded.docs.count, 2U);
  assert_true(lc_pouch_index_doc_table_lookup(&decoded.docs, 0U,
                                              &namespace_name, &key));
  assert_string_equal(namespace_name, "default");
  assert_string_equal(key, "alpha");
  assert_true(lc_pouch_index_doc_table_lookup(&decoded.docs, 1U,
                                              &namespace_name, &key));
  assert_string_equal(namespace_name, "default");
  assert_string_equal(key, "bravo");
  assert_true(
      lc_pouch_index_doc_table_find(&decoded.docs, "default", "bravo", &id));
  assert_int_equal(id, 1U);

  free(encoded);
  lc_pouch_index_doc_generation_cleanup(NULL, &decoded);
  lc_pouch_index_doc_generation_cleanup(NULL, &generation);
}

static void test_doc_generation_codec_rejects_corruption(void **state) {
  lc_pouch_index_doc_generation generation;
  lc_pouch_index_doc_generation decoded;
  lc_pouch_index_doc_id id;
  unsigned char *encoded;
  size_t encoded_size;
  size_t first_key_offset;
  size_t written;

  (void)state;
  memset(&generation, 0, sizeof(generation));
  memset(&decoded, 0, sizeof(decoded));
  generation.namespace_name = (char *)malloc(8U);
  assert_non_null(generation.namespace_name);
  memcpy(generation.namespace_name, "default", 8U);
  assert_true(lc_pouch_index_doc_table_find_or_add(NULL, &generation.docs,
                                                   "default", "alpha", &id));
  assert_true(lc_pouch_index_doc_table_find_or_add(NULL, &generation.docs,
                                                   "default", "bravo", &id));
  assert_true(
      lc_pouch_index_doc_generation_encoded_size(&generation, &encoded_size));
  encoded = (unsigned char *)malloc(encoded_size);
  assert_non_null(encoded);
  assert_true(lc_pouch_index_doc_generation_encode(&generation, encoded,
                                                   encoded_size, &written));

  assert_false(lc_pouch_index_doc_generation_decode(NULL, &decoded, encoded,
                                                    encoded_size - 1U));
  encoded[0] = 'x';
  assert_false(lc_pouch_index_doc_generation_decode(NULL, &decoded, encoded,
                                                    encoded_size));
  encoded[0] = 'L';

  first_key_offset = 40U + strlen(generation.namespace_name) + 4U;
  memcpy(encoded + first_key_offset, "zebra", 5U);
  assert_false(lc_pouch_index_doc_generation_decode(NULL, &decoded, encoded,
                                                    encoded_size));

  free(encoded);
  lc_pouch_index_doc_generation_cleanup(NULL, &decoded);
  lc_pouch_index_doc_generation_cleanup(NULL, &generation);
}

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

static void test_doc_id_scratch_reuses_reset_buffers(void **state) {
  lc_pouch_index_doc_id_scratch scratch;
  size_t term_capacity;
  size_t merge_capacity;
  size_t index;

  (void)state;
  memset(&scratch, 0, sizeof(scratch));
  for (index = 0U; index < 24U; ++index) {
    assert_true(lc_pouch_index_doc_id_set_append(NULL, &scratch.term,
                                                 (lc_pouch_index_doc_id)index));
  }
  for (index = 0U; index < 9U; ++index) {
    assert_true(lc_pouch_index_doc_id_set_append(
        NULL, &scratch.merge, (lc_pouch_index_doc_id)(index + 100U)));
  }
  term_capacity = scratch.term.capacity;
  merge_capacity = scratch.merge.capacity;
  assert_true(term_capacity >= 24U);
  assert_true(merge_capacity >= 9U);

  lc_pouch_index_doc_id_set_reset(&scratch.term);
  lc_pouch_index_doc_id_set_reset(&scratch.merge);
  assert_int_equal(scratch.term.count, 0U);
  assert_int_equal(scratch.merge.count, 0U);
  assert_int_equal(scratch.term.capacity, term_capacity);
  assert_int_equal(scratch.merge.capacity, merge_capacity);

  assert_true(lc_pouch_index_doc_id_set_append(NULL, &scratch.term, 77U));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, &scratch.merge, 88U));
  assert_int_equal(scratch.term.capacity, term_capacity);
  assert_int_equal(scratch.merge.capacity, merge_capacity);

  lc_pouch_index_doc_id_scratch_cleanup(NULL, &scratch);
  assert_null(scratch.term.items);
  assert_null(scratch.merge.items);
  assert_int_equal(scratch.term.capacity, 0U);
  assert_int_equal(scratch.merge.capacity, 0U);
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

static void
test_temporal_posting_table_appends_after_bound_and_residuals(void **state) {
  lc_pouch_index_temporal_posting_table table;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {4U, 7U};

  (void)state;
  memset(&table, 0, sizeof(table));
  memset(&doc_ids, 0, sizeof(doc_ids));

  assert_true(lc_pouch_index_temporal_posting_table_add_value_doc_id(
      NULL, &table, "/created_at", 1000, 0, 9U));
  assert_true(lc_pouch_index_temporal_posting_table_add_value_doc_id(
      NULL, &table, "/created_at", 1100, 0, 3U));
  assert_true(lc_pouch_index_temporal_posting_table_add_value_doc_id(
      NULL, &table, "/created_at", 1200, 1, 7U));
  assert_true(lc_pouch_index_temporal_posting_table_add_value_doc_id(
      NULL, &table, "/other", 1300, 0, 8U));
  assert_true(lc_pouch_index_temporal_posting_table_add_residual_doc_id(
      NULL, &table, "/created_at", 4U));
  assert_true(
      lc_pouch_index_temporal_posting_table_build_postings(NULL, &table));
  assert_true(
      lc_pouch_index_temporal_posting_table_has_field(&table, "/created_at"));

  assert_true(lc_pouch_index_temporal_posting_table_append_after(
      NULL, &table, "/created_at", 1100, 0, &doc_ids));
  assert_true(lc_pouch_index_doc_id_set_sort_unique(&doc_ids));
  assert_doc_ids(&doc_ids, expected, sizeof(expected) / sizeof(expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_pouch_index_temporal_posting_table_cleanup(NULL, &table);
}

static void test_temporal_posting_table_codec_round_trips(void **state) {
  lc_pouch_index_temporal_posting_table table;
  lc_pouch_index_temporal_posting_table decoded;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {4U, 7U};
  unsigned char *encoded;
  size_t encoded_size;
  size_t written;

  (void)state;
  memset(&table, 0, sizeof(table));
  memset(&decoded, 0, sizeof(decoded));
  memset(&doc_ids, 0, sizeof(doc_ids));

  assert_true(lc_pouch_index_temporal_posting_table_add_value_doc_id(
      NULL, &table, "/created_at", 1200, 1, 7U));
  assert_true(lc_pouch_index_temporal_posting_table_add_value_doc_id(
      NULL, &table, "/created_at", 1000, 0, 9U));
  assert_true(lc_pouch_index_temporal_posting_table_add_value_doc_id(
      NULL, &table, "/created_at", 1100, 0, 3U));
  assert_true(lc_pouch_index_temporal_posting_table_add_value_doc_id(
      NULL, &table, "/created_at", 1200, 1, 7U));
  assert_true(lc_pouch_index_temporal_posting_table_add_value_doc_id(
      NULL, &table, "/other", 1300, 0, 8U));
  assert_true(lc_pouch_index_temporal_posting_table_add_residual_doc_id(
      NULL, &table, "/created_at", 4U));
  assert_true(
      lc_pouch_index_temporal_posting_table_build_postings(NULL, &table));
  assert_true(lc_pouch_index_temporal_posting_table_encoded_size(
      &table, &encoded_size));
  encoded = (unsigned char *)malloc(encoded_size);
  assert_non_null(encoded);
  assert_true(lc_pouch_index_temporal_posting_table_encode(
      &table, encoded, encoded_size, &written));
  assert_int_equal(written, encoded_size);
  assert_true(lc_pouch_index_temporal_posting_table_decode(
      NULL, &decoded, encoded, encoded_size));
  assert_true(lc_pouch_index_temporal_posting_table_append_after(
      NULL, &decoded, "/created_at", 1100, 0, &doc_ids));
  assert_true(lc_pouch_index_doc_id_set_sort_unique(&doc_ids));
  assert_doc_ids(&doc_ids, expected, sizeof(expected) / sizeof(expected[0]));

  free(encoded);
  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_pouch_index_temporal_posting_table_cleanup(NULL, &decoded);
  lc_pouch_index_temporal_posting_table_cleanup(NULL, &table);
}

static void test_temporal_posting_table_codec_rejects_corruption(void **state) {
  lc_pouch_index_temporal_posting_table table;
  lc_pouch_index_temporal_posting_table decoded;
  unsigned char *encoded;
  size_t encoded_size;
  size_t written;

  (void)state;
  memset(&table, 0, sizeof(table));
  memset(&decoded, 0, sizeof(decoded));

  assert_true(lc_pouch_index_temporal_posting_table_add_value_doc_id(
      NULL, &table, "/created_at", 1000, 0, 9U));
  assert_true(
      lc_pouch_index_temporal_posting_table_build_postings(NULL, &table));
  assert_true(lc_pouch_index_temporal_posting_table_encoded_size(
      &table, &encoded_size));
  encoded = (unsigned char *)malloc(encoded_size);
  assert_non_null(encoded);
  assert_true(lc_pouch_index_temporal_posting_table_encode(
      &table, encoded, encoded_size, &written));
  assert_int_equal(written, encoded_size);

  assert_false(lc_pouch_index_temporal_posting_table_decode(
      NULL, &decoded, encoded, encoded_size - 1U));
  encoded[0] = 'x';
  assert_false(lc_pouch_index_temporal_posting_table_decode(
      NULL, &decoded, encoded, encoded_size));

  free(encoded);
  lc_pouch_index_temporal_posting_table_cleanup(NULL, &decoded);
  lc_pouch_index_temporal_posting_table_cleanup(NULL, &table);
}

static void test_temporal_generation_codec_round_trips(void **state) {
  lc_pouch_index_temporal_generation generation;
  lc_pouch_index_temporal_generation decoded;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {4U, 7U};
  unsigned char *encoded;
  size_t encoded_size;
  size_t written;

  (void)state;
  memset(&generation, 0, sizeof(generation));
  memset(&decoded, 0, sizeof(decoded));
  memset(&doc_ids, 0, sizeof(doc_ids));

  generation.identity.sequence = 123U;
  generation.identity.manifest_generation = 456U;
  generation.namespace_name = (char *)malloc(8U);
  assert_non_null(generation.namespace_name);
  memcpy(generation.namespace_name, "default", 8U);
  assert_true(lc_pouch_index_temporal_posting_table_add_value_doc_id(
      NULL, &generation.postings, "/created_at", 1200, 1, 7U));
  assert_true(lc_pouch_index_temporal_posting_table_add_value_doc_id(
      NULL, &generation.postings, "/created_at", 1000, 0, 9U));
  assert_true(lc_pouch_index_temporal_posting_table_add_residual_doc_id(
      NULL, &generation.postings, "/created_at", 4U));
  assert_true(lc_pouch_index_temporal_posting_table_build_postings(
      NULL, &generation.postings));
  assert_true(lc_pouch_index_temporal_generation_encoded_size(&generation,
                                                              &encoded_size));
  encoded = (unsigned char *)malloc(encoded_size);
  assert_non_null(encoded);
  assert_true(lc_pouch_index_temporal_generation_encode(
      &generation, encoded, encoded_size, &written));
  assert_int_equal(written, encoded_size);

  assert_true(lc_pouch_index_temporal_generation_decode(NULL, &decoded, encoded,
                                                        encoded_size));
  assert_int_equal(decoded.identity.sequence, 123U);
  assert_int_equal(decoded.identity.manifest_generation, 456U);
  assert_string_equal(decoded.namespace_name, "default");
  assert_true(lc_pouch_index_temporal_posting_table_append_after(
      NULL, &decoded.postings, "/created_at", 1100, 0, &doc_ids));
  assert_true(lc_pouch_index_doc_id_set_sort_unique(&doc_ids));
  assert_doc_ids(&doc_ids, expected, sizeof(expected) / sizeof(expected[0]));

  free(encoded);
  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_pouch_index_temporal_generation_cleanup(NULL, &decoded);
  lc_pouch_index_temporal_generation_cleanup(NULL, &generation);
}

static void test_temporal_generation_codec_rejects_corruption(void **state) {
  lc_pouch_index_temporal_generation generation;
  lc_pouch_index_temporal_generation decoded;
  unsigned char *encoded;
  size_t encoded_size;
  size_t written;

  (void)state;
  memset(&generation, 0, sizeof(generation));
  memset(&decoded, 0, sizeof(decoded));

  generation.namespace_name = (char *)malloc(8U);
  assert_non_null(generation.namespace_name);
  memcpy(generation.namespace_name, "default", 8U);
  assert_true(lc_pouch_index_temporal_posting_table_add_value_doc_id(
      NULL, &generation.postings, "/created_at", 1000, 0, 9U));
  assert_true(lc_pouch_index_temporal_posting_table_build_postings(
      NULL, &generation.postings));
  assert_true(lc_pouch_index_temporal_generation_encoded_size(&generation,
                                                              &encoded_size));
  encoded = (unsigned char *)malloc(encoded_size);
  assert_non_null(encoded);
  assert_true(lc_pouch_index_temporal_generation_encode(
      &generation, encoded, encoded_size, &written));

  assert_false(lc_pouch_index_temporal_generation_decode(
      NULL, &decoded, encoded, encoded_size - 1U));
  encoded[0] = 'x';
  assert_false(lc_pouch_index_temporal_generation_decode(
      NULL, &decoded, encoded, encoded_size));

  free(encoded);
  lc_pouch_index_temporal_generation_cleanup(NULL, &decoded);
  lc_pouch_index_temporal_generation_cleanup(NULL, &generation);
}

static void test_text_posting_table_appends_prefix_and_contains(void **state) {
  lc_pouch_index_text_posting_table table;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id prefix_expected[] = {2U};
  lc_pouch_index_doc_id contains_expected[] = {4U, 7U};
  lc_pouch_index_doc_id icontains_expected[] = {2U, 4U, 7U};

  (void)state;
  memset(&table, 0, sizeof(table));
  memset(&doc_ids, 0, sizeof(doc_ids));

  assert_true(lc_pouch_index_text_posting_table_add_value_doc_id(
      NULL, &table, "/body", "benchmark", 4U));
  assert_true(lc_pouch_index_text_posting_table_add_value_doc_id(
      NULL, &table, "/body", "Marked", 2U));
  assert_true(lc_pouch_index_text_posting_table_add_value_doc_id(
      NULL, &table, "/body", "landmark", 7U));
  assert_true(lc_pouch_index_text_posting_table_add_value_doc_id(
      NULL, &table, "/body", "landmark", 7U));
  assert_true(lc_pouch_index_text_posting_table_add_value_doc_id(
      NULL, &table, "/other", "market", 9U));
  assert_true(lc_pouch_index_text_posting_table_build_postings(NULL, &table));
  assert_true(lc_pouch_index_text_posting_table_has_field(&table, "/body"));

  assert_true(lc_pouch_index_text_posting_table_append_prefix(
      NULL, &table, "/body", "mar", 1, &doc_ids));
  assert_true(lc_pouch_index_doc_id_set_sort_unique(&doc_ids));
  assert_doc_ids(&doc_ids, prefix_expected,
                 sizeof(prefix_expected) / sizeof(prefix_expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  memset(&doc_ids, 0, sizeof(doc_ids));
  assert_true(lc_pouch_index_text_posting_table_append_contains(
      NULL, &table, "/body", "mark", 0, &doc_ids));
  assert_true(lc_pouch_index_doc_id_set_sort_unique(&doc_ids));
  assert_doc_ids(&doc_ids, contains_expected,
                 sizeof(contains_expected) / sizeof(contains_expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  memset(&doc_ids, 0, sizeof(doc_ids));
  assert_true(lc_pouch_index_text_posting_table_append_contains(
      NULL, &table, "/body", "MARK", 1, &doc_ids));
  assert_true(lc_pouch_index_doc_id_set_sort_unique(&doc_ids));
  assert_doc_ids(&doc_ids, icontains_expected,
                 sizeof(icontains_expected) / sizeof(icontains_expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  memset(&doc_ids, 0, sizeof(doc_ids));
  assert_true(lc_pouch_index_text_posting_table_append_contains(
      NULL, &table, "/body", "zzz", 1, &doc_ids));
  assert_true(lc_pouch_index_doc_id_set_sort_unique(&doc_ids));
  assert_doc_ids(&doc_ids, NULL, 0U);

  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_pouch_index_text_posting_table_cleanup(NULL, &table);
}

static void test_text_posting_table_codec_round_trips(void **state) {
  lc_pouch_index_text_posting_table table;
  lc_pouch_index_text_posting_table decoded;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {4U, 7U};
  unsigned char *encoded;
  size_t encoded_size;
  size_t written;

  (void)state;
  memset(&table, 0, sizeof(table));
  memset(&decoded, 0, sizeof(decoded));
  memset(&doc_ids, 0, sizeof(doc_ids));

  assert_true(lc_pouch_index_text_posting_table_add_value_doc_id(
      NULL, &table, "/body", "landmark", 7U));
  assert_true(lc_pouch_index_text_posting_table_add_value_doc_id(
      NULL, &table, "/body", "benchmark", 4U));
  assert_true(lc_pouch_index_text_posting_table_add_value_doc_id(
      NULL, &table, "/body", "Marked", 2U));
  assert_true(lc_pouch_index_text_posting_table_build_postings(NULL, &table));
  assert_true(
      lc_pouch_index_text_posting_table_encoded_size(&table, &encoded_size));
  encoded = (unsigned char *)malloc(encoded_size);
  assert_non_null(encoded);
  assert_true(lc_pouch_index_text_posting_table_encode(&table, encoded,
                                                       encoded_size, &written));
  assert_int_equal(written, encoded_size);

  assert_true(lc_pouch_index_text_posting_table_decode(NULL, &decoded, encoded,
                                                       encoded_size));
  assert_true(lc_pouch_index_text_posting_table_append_contains(
      NULL, &decoded, "/body", "mark", 0, &doc_ids));
  assert_true(lc_pouch_index_doc_id_set_sort_unique(&doc_ids));
  assert_doc_ids(&doc_ids, expected, sizeof(expected) / sizeof(expected[0]));

  free(encoded);
  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_pouch_index_text_posting_table_cleanup(NULL, &decoded);
  lc_pouch_index_text_posting_table_cleanup(NULL, &table);
}

static void test_text_generation_codec_round_trips(void **state) {
  lc_pouch_index_text_generation generation;
  lc_pouch_index_text_generation decoded;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {2U, 4U, 7U};
  unsigned char *encoded;
  size_t encoded_size;
  size_t written;

  (void)state;
  memset(&generation, 0, sizeof(generation));
  memset(&decoded, 0, sizeof(decoded));
  memset(&doc_ids, 0, sizeof(doc_ids));

  generation.identity.sequence = 123U;
  generation.identity.manifest_generation = 456U;
  generation.namespace_name = (char *)malloc(8U);
  assert_non_null(generation.namespace_name);
  memcpy(generation.namespace_name, "default", 8U);
  assert_true(lc_pouch_index_text_posting_table_add_value_doc_id(
      NULL, &generation.postings, "/body", "benchmark", 4U));
  assert_true(lc_pouch_index_text_posting_table_add_value_doc_id(
      NULL, &generation.postings, "/body", "Marked", 2U));
  assert_true(lc_pouch_index_text_posting_table_add_value_doc_id(
      NULL, &generation.postings, "/body", "landmark", 7U));
  assert_true(lc_pouch_index_text_posting_table_build_postings(
      NULL, &generation.postings));
  assert_true(
      lc_pouch_index_text_generation_encoded_size(&generation, &encoded_size));
  encoded = (unsigned char *)malloc(encoded_size);
  assert_non_null(encoded);
  assert_true(lc_pouch_index_text_generation_encode(&generation, encoded,
                                                    encoded_size, &written));
  assert_int_equal(written, encoded_size);

  assert_true(lc_pouch_index_text_generation_decode(NULL, &decoded, encoded,
                                                    encoded_size));
  assert_int_equal(decoded.identity.sequence, 123U);
  assert_int_equal(decoded.identity.manifest_generation, 456U);
  assert_string_equal(decoded.namespace_name, "default");
  assert_true(lc_pouch_index_text_posting_table_append_contains(
      NULL, &decoded.postings, "/body", "MARK", 1, &doc_ids));
  assert_true(lc_pouch_index_doc_id_set_sort_unique(&doc_ids));
  assert_doc_ids(&doc_ids, expected, sizeof(expected) / sizeof(expected[0]));

  free(encoded);
  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_pouch_index_text_generation_cleanup(NULL, &decoded);
  lc_pouch_index_text_generation_cleanup(NULL, &generation);
}

static void test_text_generation_codec_rejects_corruption(void **state) {
  lc_pouch_index_text_generation generation;
  lc_pouch_index_text_generation decoded;
  unsigned char *encoded;
  size_t encoded_size;
  size_t written;

  (void)state;
  memset(&generation, 0, sizeof(generation));
  memset(&decoded, 0, sizeof(decoded));

  generation.namespace_name = (char *)malloc(8U);
  assert_non_null(generation.namespace_name);
  memcpy(generation.namespace_name, "default", 8U);
  assert_true(lc_pouch_index_text_posting_table_add_value_doc_id(
      NULL, &generation.postings, "/body", "benchmark", 4U));
  assert_true(lc_pouch_index_text_posting_table_build_postings(
      NULL, &generation.postings));
  assert_true(
      lc_pouch_index_text_generation_encoded_size(&generation, &encoded_size));
  encoded = (unsigned char *)malloc(encoded_size);
  assert_non_null(encoded);
  assert_true(lc_pouch_index_text_generation_encode(&generation, encoded,
                                                    encoded_size, &written));

  assert_false(lc_pouch_index_text_generation_decode(NULL, &decoded, encoded,
                                                     encoded_size - 1U));
  encoded[0] = 'x';
  assert_false(lc_pouch_index_text_generation_decode(NULL, &decoded, encoded,
                                                     encoded_size));

  free(encoded);
  lc_pouch_index_text_generation_cleanup(NULL, &decoded);
  lc_pouch_index_text_generation_cleanup(NULL, &generation);
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

static void test_term_posting_table_appends_by_term_id(void **state) {
  lc_pouch_index_term_posting_table table;
  lc_pouch_index_doc_id_set appended;
  lc_pouch_index_doc_id ids[] = {9U, 3U, 3U, 7U};
  lc_pouch_index_doc_id expected[] = {1U, 3U, 7U, 9U};
  lc_pouch_index_doc_id missing_expected[] = {1U, 3U, 7U, 9U};

  (void)state;
  memset(&table, 0, sizeof(table));
  memset(&appended, 0, sizeof(appended));

  assert_true(lc_pouch_index_doc_id_set_append(NULL, &appended, 1U));
  assert_true(lc_pouch_index_term_posting_table_put(
      NULL, &table, 42U, ids, sizeof(ids) / sizeof(ids[0])));

  assert_true(
      lc_pouch_index_term_posting_table_append(NULL, &table, 42U, &appended));
  assert_doc_ids(&appended, expected, sizeof(expected) / sizeof(expected[0]));

  assert_true(
      lc_pouch_index_term_posting_table_append(NULL, &table, 99U, &appended));
  assert_doc_ids(&appended, missing_expected,
                 sizeof(missing_expected) / sizeof(missing_expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &appended);
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

static void test_term_generation_codec_round_trips(void **state) {
  lc_pouch_index_term_generation generation;
  lc_pouch_index_term_generation decoded;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_term_id region_id;
  lc_pouch_index_term_id kind_id;
  lc_pouch_index_term_id found_id;
  lc_pouch_index_doc_id sparse_ids[] = {9U, 3U, 3U, 7U};
  lc_pouch_index_doc_id sparse_expected[] = {3U, 7U, 9U};
  lc_pouch_index_doc_id dense_ids[128];
  unsigned char *bytes;
  size_t size;
  size_t written;
  size_t index;

  (void)state;
  memset(&generation, 0, sizeof(generation));
  memset(&decoded, 0, sizeof(decoded));
  memset(&doc_ids, 0, sizeof(doc_ids));
  bytes = NULL;
  for (index = 0U; index < sizeof(dense_ids) / sizeof(dense_ids[0]); ++index) {
    dense_ids[index] = (lc_pouch_index_doc_id)index;
  }

  generation.identity.sequence = 33U;
  generation.identity.manifest_generation = 44U;
  generation.namespace_name = lc_pouch_strdup(NULL, "default");
  assert_non_null(generation.namespace_name);
  assert_true(lc_pouch_index_term_table_find_or_add(
      NULL, &generation.terms, "/region", "s:north", &region_id));
  assert_true(lc_pouch_index_term_table_find_or_add(
      NULL, &generation.terms, "/kind", "s:invoice", &kind_id));
  assert_true(lc_pouch_index_term_posting_table_put(
      NULL, &generation.postings, region_id, sparse_ids,
      sizeof(sparse_ids) / sizeof(sparse_ids[0])));
  assert_true(lc_pouch_index_term_posting_table_put(
      NULL, &generation.postings, kind_id, dense_ids,
      sizeof(dense_ids) / sizeof(dense_ids[0])));

  assert_true(lc_pouch_index_term_generation_encoded_size(&generation, &size));
  bytes = (unsigned char *)malloc(size);
  assert_non_null(bytes);
  assert_true(lc_pouch_index_term_generation_encode(&generation, bytes, size,
                                                    &written));
  assert_int_equal(written, size);
  assert_true(
      lc_pouch_index_term_generation_decode(NULL, &decoded, bytes, size));

  assert_int_equal(decoded.identity.sequence, 33U);
  assert_int_equal(decoded.identity.manifest_generation, 44U);
  assert_string_equal(decoded.namespace_name, "default");
  assert_true(lc_pouch_index_term_table_find(&decoded.terms, "/region",
                                             "s:north", &found_id));
  assert_int_equal(found_id, region_id);
  assert_true(lc_pouch_index_term_table_find(&decoded.terms, "/kind",
                                             "s:invoice", &found_id));
  assert_int_equal(found_id, kind_id);
  assert_true(lc_pouch_index_term_posting_table_decode(NULL, &decoded.postings,
                                                       region_id, &doc_ids));
  assert_doc_ids(&doc_ids, sparse_expected,
                 sizeof(sparse_expected) / sizeof(sparse_expected[0]));
  assert_int_equal(decoded.postings.entries[0].posting.encoding,
                   LC_POUCH_INDEX_POSTING_SPARSE);
  assert_int_equal(decoded.postings.entries[1].posting.encoding,
                   LC_POUCH_INDEX_POSTING_DENSE);

  free(bytes);
  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_pouch_index_term_generation_cleanup(NULL, &decoded);
  lc_pouch_index_term_generation_cleanup(NULL, &generation);
}

static void test_term_generation_codec_rejects_corruption(void **state) {
  lc_pouch_index_term_generation generation;
  lc_pouch_index_term_generation decoded;
  lc_pouch_index_term_id term_id;
  lc_pouch_index_doc_id ids[] = {1U, 2U, 3U};
  unsigned char *bytes;
  size_t size;
  size_t written;

  (void)state;
  memset(&generation, 0, sizeof(generation));
  memset(&decoded, 0, sizeof(decoded));
  bytes = NULL;

  generation.identity.sequence = 33U;
  generation.identity.manifest_generation = 44U;
  generation.namespace_name = lc_pouch_strdup(NULL, "default");
  assert_non_null(generation.namespace_name);
  assert_true(lc_pouch_index_term_table_find_or_add(
      NULL, &generation.terms, "/region", "s:north", &term_id));
  assert_true(lc_pouch_index_term_posting_table_put(
      NULL, &generation.postings, term_id, ids, sizeof(ids) / sizeof(ids[0])));
  assert_true(lc_pouch_index_term_generation_encoded_size(&generation, &size));
  bytes = (unsigned char *)malloc(size);
  assert_non_null(bytes);
  assert_true(lc_pouch_index_term_generation_encode(&generation, bytes, size,
                                                    &written));
  assert_int_equal(written, size);

  bytes[0] ^= 0xffU;
  assert_false(
      lc_pouch_index_term_generation_decode(NULL, &decoded, bytes, size));
  bytes[0] ^= 0xffU;
  assert_false(
      lc_pouch_index_term_generation_decode(NULL, &decoded, bytes, size - 1U));
  bytes[size - 1U] ^= 0xffU;
  assert_false(
      lc_pouch_index_term_generation_decode(NULL, &decoded, bytes, size));

  free(bytes);
  lc_pouch_index_term_generation_cleanup(NULL, &decoded);
  lc_pouch_index_term_generation_cleanup(NULL, &generation);
}

static void
test_number_posting_table_appends_range_and_residuals(void **state) {
  lc_pouch_index_number_posting_table table;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {4U, 8U, 99U};

  (void)state;
  memset(&table, 0, sizeof(table));
  memset(&doc_ids, 0, sizeof(doc_ids));

  assert_true(lc_pouch_index_number_posting_table_add_value_doc_id(
      NULL, &table, "/amount", "n:+:15:0", 8U));
  assert_true(lc_pouch_index_number_posting_table_add_value_doc_id(
      NULL, &table, "/amount", "n:-:5:0", 1U));
  assert_true(lc_pouch_index_number_posting_table_add_value_doc_id(
      NULL, &table, "/amount", "n:+:2:0", 2U));
  assert_true(lc_pouch_index_number_posting_table_add_value_doc_id(
      NULL, &table, "/amount", "n:+:1:1", 4U));
  assert_true(lc_pouch_index_number_posting_table_add_value_doc_id(
      NULL, &table, "/amount", "n:+:1:1", 4U));
  assert_true(lc_pouch_index_number_posting_table_add_residual_doc_id(
      NULL, &table, "/amount", 99U));
  assert_true(lc_pouch_index_number_posting_table_build_postings(NULL, &table));

  assert_true(lc_pouch_index_number_posting_table_has_field(&table, "/amount"));
  assert_true(lc_pouch_index_number_posting_table_append_range(
      NULL, &table, "/amount", "n:+:2:0", NULL, NULL, "n:+:15:0", &doc_ids));
  assert_true(lc_pouch_index_doc_id_set_sort_unique(&doc_ids));
  assert_doc_ids(&doc_ids, expected, sizeof(expected) / sizeof(expected[0]));
  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  memset(&doc_ids, 0, sizeof(doc_ids));

  assert_true(lc_pouch_index_number_posting_table_append_range(
      NULL, &table, "/amount", "n:+:2:0", "n:-:5:0", "n:+:15:0", "n:+:2:0",
      &doc_ids));
  assert_true(lc_pouch_index_doc_id_set_sort_unique(&doc_ids));
  assert_int_equal(doc_ids.count, 1U);
  assert_int_equal(doc_ids.items[0], 99U);

  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_pouch_index_number_posting_table_cleanup(NULL, &table);
}

static void test_number_generation_codec_round_trips(void **state) {
  lc_pouch_index_number_generation generation;
  lc_pouch_index_number_generation decoded;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {2U, 3U, 99U};
  unsigned char *bytes;
  size_t size;
  size_t written;

  (void)state;
  memset(&generation, 0, sizeof(generation));
  memset(&decoded, 0, sizeof(decoded));
  memset(&doc_ids, 0, sizeof(doc_ids));
  bytes = NULL;

  generation.identity.sequence = 55U;
  generation.identity.manifest_generation = 66U;
  generation.namespace_name = lc_pouch_strdup(NULL, "default");
  assert_non_null(generation.namespace_name);
  assert_true(lc_pouch_index_number_posting_table_add_value_doc_id(
      NULL, &generation.postings, "/amount", "n:-:5:0", 1U));
  assert_true(lc_pouch_index_number_posting_table_add_value_doc_id(
      NULL, &generation.postings, "/amount", "n:0", 2U));
  assert_true(lc_pouch_index_number_posting_table_add_value_doc_id(
      NULL, &generation.postings, "/amount", "n:+:2:0", 3U));
  assert_true(lc_pouch_index_number_posting_table_add_value_doc_id(
      NULL, &generation.postings, "/amount", "n:+:1:1", 4U));
  assert_true(lc_pouch_index_number_posting_table_add_residual_doc_id(
      NULL, &generation.postings, "/amount", 99U));
  assert_true(lc_pouch_index_number_posting_table_build_postings(
      NULL, &generation.postings));

  assert_true(
      lc_pouch_index_number_generation_encoded_size(&generation, &size));
  bytes = (unsigned char *)malloc(size);
  assert_non_null(bytes);
  assert_true(lc_pouch_index_number_generation_encode(&generation, bytes, size,
                                                      &written));
  assert_int_equal(written, size);
  assert_true(
      lc_pouch_index_number_generation_decode(NULL, &decoded, bytes, size));

  assert_int_equal(decoded.identity.sequence, 55U);
  assert_int_equal(decoded.identity.manifest_generation, 66U);
  assert_string_equal(decoded.namespace_name, "default");
  assert_true(lc_pouch_index_number_posting_table_append_range(
      NULL, &decoded.postings, "/amount", NULL, "n:0", "n:+:1:1", NULL,
      &doc_ids));
  assert_true(lc_pouch_index_doc_id_set_sort_unique(&doc_ids));
  assert_doc_ids(&doc_ids, expected, sizeof(expected) / sizeof(expected[0]));

  free(bytes);
  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_pouch_index_number_generation_cleanup(NULL, &decoded);
  lc_pouch_index_number_generation_cleanup(NULL, &generation);
}

static void test_number_generation_codec_rejects_corruption(void **state) {
  lc_pouch_index_number_generation generation;
  lc_pouch_index_number_generation decoded;
  unsigned char *bytes;
  size_t size;
  size_t written;
  size_t index;

  (void)state;
  memset(&generation, 0, sizeof(generation));
  memset(&decoded, 0, sizeof(decoded));
  bytes = NULL;

  generation.identity.sequence = 55U;
  generation.identity.manifest_generation = 66U;
  generation.namespace_name = lc_pouch_strdup(NULL, "default");
  assert_non_null(generation.namespace_name);
  assert_true(lc_pouch_index_number_posting_table_add_value_doc_id(
      NULL, &generation.postings, "/amount", "n:+:2:0", 3U));
  assert_true(lc_pouch_index_number_posting_table_build_postings(
      NULL, &generation.postings));
  assert_true(
      lc_pouch_index_number_generation_encoded_size(&generation, &size));
  bytes = (unsigned char *)malloc(size);
  assert_non_null(bytes);
  assert_true(lc_pouch_index_number_generation_encode(&generation, bytes, size,
                                                      &written));
  assert_int_equal(written, size);

  bytes[0] ^= 0xffU;
  assert_false(
      lc_pouch_index_number_generation_decode(NULL, &decoded, bytes, size));
  bytes[0] ^= 0xffU;
  assert_false(lc_pouch_index_number_generation_decode(NULL, &decoded, bytes,
                                                       size - 1U));
  for (index = 0U; index + 2U < size; ++index) {
    if (bytes[index] == 'n' && bytes[index + 1U] == ':' &&
        bytes[index + 2U] == '+') {
      bytes[index] = 'x';
      break;
    }
  }
  assert_true(index + 2U < size);
  assert_false(
      lc_pouch_index_number_generation_decode(NULL, &decoded, bytes, size));

  free(bytes);
  lc_pouch_index_number_generation_cleanup(NULL, &decoded);
  lc_pouch_index_number_generation_cleanup(NULL, &generation);
}

static void test_result_cache_keys_by_generation_and_plan(void **state) {
  lc_pouch_index_result_cache cache;
  lc_pouch_index_doc_id_set source;
  lc_pouch_index_doc_id_set found;
  lc_pouch_index_identity identity;
  lc_pouch_index_identity other_identity;
  lc_pouch_index_doc_id source_values[] = {8U, 2U, 8U, 5U};
  lc_pouch_index_doc_id expected[] = {2U, 5U, 8U};

  (void)state;
  memset(&cache, 0, sizeof(cache));
  memset(&source, 0, sizeof(source));
  memset(&found, 0, sizeof(found));
  memset(&identity, 0, sizeof(identity));
  memset(&other_identity, 0, sizeof(other_identity));
  assert_true(
      set_from_values(&source, source_values,
                      sizeof(source_values) / sizeof(source_values[0])));

  assert_true(lc_pouch_index_result_cache_put(NULL, &cache, 11U,
                                              "eq:/region=s:north", &source));
  assert_int_equal(cache.puts, 1U);
  assert_int_equal(cache.hits, 0U);
  assert_int_equal(cache.misses, 0U);
  assert_true(lc_pouch_index_result_cache_find(NULL, &cache, 11U,
                                               "eq:/region=s:north", &found));
  assert_doc_ids(&found, expected, sizeof(expected) / sizeof(expected[0]));
  assert_int_equal(cache.hits, 1U);
  assert_int_equal(cache.misses, 0U);
  assert_false(lc_pouch_index_result_cache_find(NULL, &cache, 12U,
                                                "eq:/region=s:north", &found));
  assert_false(lc_pouch_index_result_cache_find(NULL, &cache, 11U,
                                                "eq:/region=s:south", &found));
  assert_int_equal(cache.hits, 1U);
  assert_int_equal(cache.misses, 2U);

  identity.sequence = 11U;
  identity.manifest_generation = 99U;
  other_identity.sequence = 11U;
  other_identity.manifest_generation = 100U;
  assert_true(lc_pouch_index_result_cache_put_identity(
      NULL, &cache, identity, "eq:/region=s:north", &source));
  assert_true(lc_pouch_index_result_cache_find_identity(
      NULL, &cache, identity, "eq:/region=s:north", &found));
  assert_doc_ids(&found, expected, sizeof(expected) / sizeof(expected[0]));
  assert_false(lc_pouch_index_result_cache_find_identity(
      NULL, &cache, other_identity, "eq:/region=s:north", &found));
  assert_false(lc_pouch_index_result_cache_find_identity(
      NULL, &cache, identity, "eq:/region=s:south", &found));
  assert_int_equal(cache.hits, 2U);
  assert_int_equal(cache.misses, 4U);

  lc_pouch_index_doc_id_set_cleanup(NULL, &found);
  lc_pouch_index_doc_id_set_cleanup(NULL, &source);
  lc_pouch_index_result_cache_cleanup(NULL, &cache);
}

static void assert_result_plan_key(const lc_pouch_query_index_scan_req *req,
                                   lc_pouch_index_result_plan_kind kind,
                                   const char *expected) {
  char *key;

  key = lc_pouch_index_result_plan_key(NULL, req, kind);
  assert_non_null(key);
  assert_string_equal(key, expected);
  lc_pouch_free(NULL, key);
}

static void test_result_plan_keys_are_normalized_by_index(void **state) {
  lc_pouch_query_index_scan_req req;
  lc_pouch_document_eq_term eq;
  lc_pouch_document_eq_term compound_eq[3];
  lc_pouch_document_eq_term not_equal[3];
  lc_pouch_document_exists_term exists;
  lc_pouch_document_in_term in;
  lc_pouch_document_range_term range;
  lc_pouch_document_prefix_term prefix;
  lc_pouch_document_contains_term contains;
  lc_pouch_document_date_after_term date_after;
  const char *in_values[] = {"s:beta", "s:alpha", "s:alpha"};

  (void)state;
  memset(&req, 0, sizeof(req));
  req.namespace_name = "default";

  eq.field = "/value";
  eq.value = "s:alpha";
  req.document_eq_terms = &eq;
  req.document_eq_term_count = 1U;
  assert_result_plan_key(&req, LC_POUCH_INDEX_RESULT_PLAN_EQ,
                         "eq:7:default:6:/value:7:s:alpha");

  not_equal[0].field = "/kind";
  not_equal[0].value = "s:block";
  not_equal[1].field = "/bucket";
  not_equal[1].value = "s:cold";
  not_equal[2].field = "/kind";
  not_equal[2].value = "s:block";
  req.document_not_eq_terms = not_equal;
  req.document_not_eq_term_count = sizeof(not_equal) / sizeof(not_equal[0]);
  assert_result_plan_key(
      &req, LC_POUCH_INDEX_RESULT_PLAN_EQ,
      "eq:7:default:6:/value:7:s:alpha:not_eq:2:7:/bucket:6:s:cold:5:/kind:7:"
      "s:block");

  memset(&req, 0, sizeof(req));
  req.namespace_name = "default";
  exists.field = "/box";
  req.document_exists_terms = &exists;
  req.document_exists_term_count = 1U;
  assert_result_plan_key(&req, LC_POUCH_INDEX_RESULT_PLAN_EXISTS,
                         "exists:7:default:4:/box");

  compound_eq[0].field = "/kind";
  compound_eq[0].value = "s:include";
  compound_eq[1].field = "/bucket";
  compound_eq[1].value = "s:hot";
  compound_eq[2].field = "/kind";
  compound_eq[2].value = "s:include";
  req.document_eq_terms = compound_eq;
  req.document_eq_term_count = sizeof(compound_eq) / sizeof(compound_eq[0]);
  assert_result_plan_key(
      &req, LC_POUCH_INDEX_RESULT_PLAN_EXISTS,
      "exists:7:default:4:/box:eq:2:7:/bucket:5:s:hot:5:/kind:9:s:include");

  not_equal[0].field = "/kind";
  not_equal[0].value = "s:block";
  not_equal[1].field = "/bucket";
  not_equal[1].value = "s:cold";
  not_equal[2].field = "/kind";
  not_equal[2].value = "s:block";
  req.document_not_eq_terms = not_equal;
  req.document_not_eq_term_count = sizeof(not_equal) / sizeof(not_equal[0]);
  assert_result_plan_key(
      &req, LC_POUCH_INDEX_RESULT_PLAN_EXISTS,
      "exists:7:default:4:/box:eq:2:7:/bucket:5:s:hot:5:/kind:9:s:"
      "include:not_eq:2:7:/bucket:6:s:cold:5:/kind:7:s:block");

  memset(&req, 0, sizeof(req));
  req.namespace_name = "default";
  in.field = "/value";
  in.values = in_values;
  in.value_count = sizeof(in_values) / sizeof(in_values[0]);
  req.document_in_terms = &in;
  req.document_in_term_count = 1U;
  assert_result_plan_key(&req, LC_POUCH_INDEX_RESULT_PLAN_IN,
                         "in:7:default:6:/value:2:7:s:alpha:6:s:beta");

  compound_eq[0].field = "/kind";
  compound_eq[0].value = "s:include";
  compound_eq[1].field = "/bucket";
  compound_eq[1].value = "s:hot";
  compound_eq[2].field = "/kind";
  compound_eq[2].value = "s:include";
  req.document_eq_terms = compound_eq;
  req.document_eq_term_count = sizeof(compound_eq) / sizeof(compound_eq[0]);
  assert_result_plan_key(
      &req, LC_POUCH_INDEX_RESULT_PLAN_IN,
      "in:7:default:6:/value:2:7:s:alpha:6:s:beta:eq:2:7:/bucket:5:s:"
      "hot:5:/kind:9:s:include");

  not_equal[0].field = "/kind";
  not_equal[0].value = "s:block";
  not_equal[1].field = "/bucket";
  not_equal[1].value = "s:cold";
  not_equal[2].field = "/kind";
  not_equal[2].value = "s:block";
  req.document_not_eq_terms = not_equal;
  req.document_not_eq_term_count = sizeof(not_equal) / sizeof(not_equal[0]);
  assert_result_plan_key(
      &req, LC_POUCH_INDEX_RESULT_PLAN_IN,
      "in:7:default:6:/value:2:7:s:alpha:6:s:beta:eq:2:7:/bucket:5:s:"
      "hot:5:/kind:9:s:include:not_eq:2:7:/bucket:6:s:cold:5:/kind:7:s:"
      "block");

  memset(&req, 0, sizeof(req));
  req.namespace_name = "default";
  range.field = "/n";
  range.gt = "n:1";
  range.gte = "n:2";
  range.lt = "n:3";
  range.lte = "n:4";
  req.document_range_terms = &range;
  req.document_range_term_count = 1U;
  assert_result_plan_key(
      &req, LC_POUCH_INDEX_RESULT_PLAN_RANGE,
      "range:7:default:2:/n:1:3:n:1:1:3:n:2:1:3:n:3:1:3:n:4");

  compound_eq[0].field = "/kind";
  compound_eq[0].value = "s:include";
  compound_eq[1].field = "/bucket";
  compound_eq[1].value = "s:hot";
  compound_eq[2].field = "/kind";
  compound_eq[2].value = "s:include";
  req.document_eq_terms = compound_eq;
  req.document_eq_term_count = sizeof(compound_eq) / sizeof(compound_eq[0]);
  assert_result_plan_key(
      &req, LC_POUCH_INDEX_RESULT_PLAN_RANGE,
      "range:7:default:2:/n:1:3:n:1:1:3:n:2:1:3:n:3:1:3:n:4:eq:2:7:/"
      "bucket:5:s:hot:5:/kind:9:s:include");

  not_equal[0].field = "/kind";
  not_equal[0].value = "s:block";
  not_equal[1].field = "/bucket";
  not_equal[1].value = "s:cold";
  not_equal[2].field = "/kind";
  not_equal[2].value = "s:block";
  req.document_not_eq_terms = not_equal;
  req.document_not_eq_term_count = sizeof(not_equal) / sizeof(not_equal[0]);
  assert_result_plan_key(
      &req, LC_POUCH_INDEX_RESULT_PLAN_RANGE,
      "range:7:default:2:/n:1:3:n:1:1:3:n:2:1:3:n:3:1:3:n:4:eq:2:7:/"
      "bucket:5:s:hot:5:/kind:9:s:include:not_eq:2:7:/bucket:6:s:cold:5:/"
      "kind:7:s:block");

  memset(&req, 0, sizeof(req));
  req.namespace_name = "default";
  prefix.field = "/name";
  prefix.value = "al";
  prefix.ignore_case = 1;
  req.document_prefix_terms = &prefix;
  req.document_prefix_term_count = 1U;
  assert_result_plan_key(&req, LC_POUCH_INDEX_RESULT_PLAN_PREFIX,
                         "prefix:7:default:5:/name:1:2:al");

  compound_eq[0].field = "/kind";
  compound_eq[0].value = "s:include";
  compound_eq[1].field = "/bucket";
  compound_eq[1].value = "s:hot";
  compound_eq[2].field = "/kind";
  compound_eq[2].value = "s:include";
  req.document_eq_terms = compound_eq;
  req.document_eq_term_count = sizeof(compound_eq) / sizeof(compound_eq[0]);
  assert_result_plan_key(
      &req, LC_POUCH_INDEX_RESULT_PLAN_PREFIX,
      "prefix:7:default:5:/name:1:2:al:eq:2:7:/bucket:5:s:hot:5:/kind:9:s:"
      "include");

  req.document_not_eq_terms = not_equal;
  req.document_not_eq_term_count = sizeof(not_equal) / sizeof(not_equal[0]);
  assert_result_plan_key(
      &req, LC_POUCH_INDEX_RESULT_PLAN_PREFIX,
      "prefix:7:default:5:/name:1:2:al:eq:2:7:/bucket:5:s:hot:5:/kind:9:s:"
      "include:not_eq:2:7:/bucket:6:s:cold:5:/kind:7:s:block");

  memset(&req, 0, sizeof(req));
  req.namespace_name = "default";
  contains.field = "/name";
  contains.value = "pha";
  contains.ignore_case = 0;
  req.document_contains_terms = &contains;
  req.document_contains_term_count = 1U;
  assert_result_plan_key(&req, LC_POUCH_INDEX_RESULT_PLAN_CONTAINS,
                         "contains:7:default:5:/name:0:3:pha");

  req.document_eq_terms = compound_eq;
  req.document_eq_term_count = sizeof(compound_eq) / sizeof(compound_eq[0]);
  assert_result_plan_key(
      &req, LC_POUCH_INDEX_RESULT_PLAN_CONTAINS,
      "contains:7:default:5:/name:0:3:pha:eq:2:7:/bucket:5:s:hot:5:/kind:9:s:"
      "include");

  req.document_not_eq_terms = not_equal;
  req.document_not_eq_term_count = sizeof(not_equal) / sizeof(not_equal[0]);
  assert_result_plan_key(
      &req, LC_POUCH_INDEX_RESULT_PLAN_CONTAINS,
      "contains:7:default:5:/name:0:3:pha:eq:2:7:/bucket:5:s:hot:5:/kind:9:s:"
      "include:not_eq:2:7:/bucket:6:s:cold:5:/kind:7:s:block");

  memset(&req, 0, sizeof(req));
  req.namespace_name = "default";
  date_after.field = "/created_at";
  date_after.after = "2025-01-01T00:00:00Z";
  req.document_date_after_terms = &date_after;
  req.document_date_after_term_count = 1U;
  assert_result_plan_key(
      &req, LC_POUCH_INDEX_RESULT_PLAN_DATE_AFTER,
      "date_after:7:default:11:/created_at:0:20:2025-01-01T00:00:00Z");

  compound_eq[0].field = "/kind";
  compound_eq[0].value = "s:include";
  compound_eq[1].field = "/bucket";
  compound_eq[1].value = "s:hot";
  compound_eq[2].field = "/kind";
  compound_eq[2].value = "s:include";
  req.document_eq_terms = compound_eq;
  req.document_eq_term_count = sizeof(compound_eq) / sizeof(compound_eq[0]);
  assert_result_plan_key(
      &req, LC_POUCH_INDEX_RESULT_PLAN_DATE_AFTER,
      "date_after:7:default:11:/created_at:0:20:2025-01-01T00:00:00Z:eq:2:7:/"
      "bucket:5:s:hot:5:/kind:9:s:include");

  req.document_not_eq_terms = not_equal;
  req.document_not_eq_term_count = sizeof(not_equal) / sizeof(not_equal[0]);
  assert_result_plan_key(
      &req, LC_POUCH_INDEX_RESULT_PLAN_DATE_AFTER,
      "date_after:7:default:11:/created_at:0:20:2025-01-01T00:00:00Z:eq:2:7:/"
      "bucket:5:s:hot:5:/kind:9:s:include:not_eq:2:7:/bucket:6:s:cold:5:/"
      "kind:7:s:block");
}

static void test_result_plan_keys_reject_filtered_compound_views(void **state) {
  lc_pouch_query_index_scan_req req;
  lc_pouch_document_exists_term exists;
  lc_pouch_document_date_after_term date_after;
  lc_pouch_document_in_term not_in;
  lc_pouch_document_range_term range;
  const char *values[] = {"s:beta"};

  (void)state;
  memset(&req, 0, sizeof(req));
  req.namespace_name = "default";
  req.key = "alpha";
  exists.field = "/value";
  req.document_exists_terms = &exists;
  req.document_exists_term_count = 1U;
  assert_null(lc_pouch_index_result_plan_key(
      NULL, &req, LC_POUCH_INDEX_RESULT_PLAN_EXISTS));

  req.key = NULL;
  not_in.field = "/value";
  not_in.values = values;
  not_in.value_count = sizeof(values) / sizeof(values[0]);
  req.document_not_in_terms = &not_in;
  req.document_not_in_term_count = 1U;
  assert_null(lc_pouch_index_result_plan_key(
      NULL, &req, LC_POUCH_INDEX_RESULT_PLAN_EXISTS));

  memset(&req, 0, sizeof(req));
  req.namespace_name = "default";
  range.field = "/n";
  range.gte = "n:1";
  req.document_range_terms = &range;
  req.document_range_term_count = 1U;
  req.document_not_in_terms = &not_in;
  req.document_not_in_term_count = 1U;
  assert_null(lc_pouch_index_result_plan_key(NULL, &req,
                                             LC_POUCH_INDEX_RESULT_PLAN_RANGE));

  memset(&req, 0, sizeof(req));
  req.namespace_name = "default";
  date_after.field = "/created_at";
  date_after.after = "2025-01-01T00:00:00Z";
  req.document_date_after_terms = &date_after;
  req.document_date_after_term_count = 1U;
  req.document_not_in_terms = &not_in;
  req.document_not_in_term_count = 1U;
  assert_null(lc_pouch_index_result_plan_key(
      NULL, &req, LC_POUCH_INDEX_RESULT_PLAN_DATE_AFTER));
}

static void test_prepared_term_cache_refreshes_by_generation(void **state) {
  lc_pouch_index_prepared_term_cache cache;
  lc_pouch_index_doc_id_set decoded;
  lc_pouch_index_doc_id ids[] = {9U, 1U};
  lc_pouch_index_doc_id expected[] = {1U, 9U};
  lc_pouch_index_identity identity;
  lc_pouch_index_term_id term_id;

  (void)state;
  memset(&cache, 0, sizeof(cache));
  memset(&decoded, 0, sizeof(decoded));
  memset(&identity, 0, sizeof(identity));

  lc_pouch_index_prepared_term_cache_refresh(NULL, &cache, 7U);
  assert_int_equal(cache.generation, 7U);
  assert_int_equal(cache.identity.sequence, 7U);
  assert_int_equal(cache.identity.manifest_generation, 0U);
  assert_true(lc_pouch_index_term_table_find_or_add(
      NULL, &cache.terms, "default:/field", "s:value", &term_id));
  assert_true(lc_pouch_index_term_posting_table_put(
      NULL, &cache.postings, term_id, ids, sizeof(ids) / sizeof(ids[0])));

  lc_pouch_index_prepared_term_cache_refresh(NULL, &cache, 7U);
  assert_int_equal(cache.generation, 7U);
  assert_int_equal(cache.identity.sequence, 7U);
  assert_int_equal(cache.identity.manifest_generation, 0U);
  assert_int_equal(cache.terms.count, 1U);
  assert_true(lc_pouch_index_term_posting_table_decode(NULL, &cache.postings,
                                                       term_id, &decoded));
  assert_doc_ids(&decoded, expected, sizeof(expected) / sizeof(expected[0]));
  lc_pouch_index_doc_id_set_cleanup(NULL, &decoded);

  lc_pouch_index_prepared_term_cache_refresh(NULL, &cache, 8U);
  assert_int_equal(cache.generation, 8U);
  assert_int_equal(cache.identity.sequence, 8U);
  assert_int_equal(cache.identity.manifest_generation, 0U);
  assert_int_equal(cache.terms.count, 0U);
  assert_int_equal(cache.postings.count, 0U);

  identity.sequence = 8U;
  identity.manifest_generation = 99U;
  assert_true(lc_pouch_index_term_table_find_or_add(
      NULL, &cache.terms, "default:/field", "s:value", &term_id));
  assert_true(lc_pouch_index_term_posting_table_put(
      NULL, &cache.postings, term_id, ids, sizeof(ids) / sizeof(ids[0])));
  lc_pouch_index_prepared_term_cache_refresh_identity(NULL, &cache, identity);
  assert_int_equal(cache.generation, 8U);
  assert_int_equal(cache.identity.sequence, 8U);
  assert_int_equal(cache.identity.manifest_generation, 99U);
  assert_int_equal(cache.terms.count, 0U);
  assert_int_equal(cache.postings.count, 0U);

  lc_pouch_index_prepared_term_cache_cleanup(NULL, &cache);
}

static void test_prepared_temporal_cache_refreshes_by_identity(void **state) {
  lc_pouch_index_prepared_temporal_cache cache;
  lc_pouch_index_doc_id_set decoded;
  lc_pouch_index_doc_id expected[] = {5U};
  lc_pouch_index_identity identity;

  (void)state;
  memset(&cache, 0, sizeof(cache));
  memset(&decoded, 0, sizeof(decoded));
  memset(&identity, 0, sizeof(identity));

  identity.sequence = 7U;
  identity.manifest_generation = 1U;
  lc_pouch_index_prepared_temporal_cache_refresh_identity(NULL, &cache,
                                                          identity);
  assert_int_equal(cache.generation, 7U);
  assert_int_equal(cache.identity.sequence, 7U);
  assert_int_equal(cache.identity.manifest_generation, 1U);
  assert_true(lc_pouch_index_temporal_posting_table_add_value_doc_id(
      NULL, &cache.postings, "default:/created_at", 1000, 0, 5U));
  assert_true(lc_pouch_index_temporal_posting_table_build_postings(
      NULL, &cache.postings));

  lc_pouch_index_prepared_temporal_cache_refresh_identity(NULL, &cache,
                                                          identity);
  assert_true(lc_pouch_index_temporal_posting_table_append_after(
      NULL, &cache.postings, "default:/created_at", 900, 0, &decoded));
  assert_true(lc_pouch_index_doc_id_set_sort_unique(&decoded));
  assert_doc_ids(&decoded, expected, sizeof(expected) / sizeof(expected[0]));
  lc_pouch_index_doc_id_set_cleanup(NULL, &decoded);

  identity.manifest_generation = 2U;
  lc_pouch_index_prepared_temporal_cache_refresh_identity(NULL, &cache,
                                                          identity);
  assert_int_equal(cache.generation, 7U);
  assert_int_equal(cache.identity.sequence, 7U);
  assert_int_equal(cache.identity.manifest_generation, 2U);
  assert_int_equal(cache.postings.field_count, 0U);

  lc_pouch_index_prepared_temporal_cache_cleanup(NULL, &cache);
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
  assert_int_equal(cache.puts, 2U);
  assert_int_equal(cache.replacements, 1U);
  assert_true(lc_pouch_index_result_cache_find(NULL, &cache, 4U, "exists:/tags",
                                               &found));
  assert_doc_ids(&found, expected, sizeof(expected) / sizeof(expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &found);
  lc_pouch_index_doc_id_set_cleanup(NULL, &second);
  lc_pouch_index_doc_id_set_cleanup(NULL, &first);
  lc_pouch_index_result_cache_cleanup(NULL, &cache);
}

static void
test_result_page_doc_ids_applies_namespace_cursor_and_limit(void **state) {
  lc_pouch_index_doc_table table;
  lc_pouch_index_doc_id_set matches;
  lc_pouch_index_result_page page;
  lc_pouch_index_doc_id id_alpha;
  lc_pouch_index_doc_id id_bravo;
  lc_pouch_index_doc_id id_charlie;
  lc_pouch_index_doc_id id_other;
  lc_pouch_index_doc_id expected[] = {1U};
  int invalid_doc_id;

  (void)state;
  memset(&table, 0, sizeof(table));
  memset(&matches, 0, sizeof(matches));
  memset(&page, 0, sizeof(page));

  assert_true(lc_pouch_index_doc_table_find_or_add(NULL, &table, "default",
                                                   "alpha", &id_alpha));
  assert_true(lc_pouch_index_doc_table_find_or_add(NULL, &table, "default",
                                                   "bravo", &id_bravo));
  assert_true(lc_pouch_index_doc_table_find_or_add(NULL, &table, "default",
                                                   "charlie", &id_charlie));
  assert_true(lc_pouch_index_doc_table_find_or_add(NULL, &table, "other",
                                                   "alpha", &id_other));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, &matches, id_other));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, &matches, id_charlie));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, &matches, id_alpha));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, &matches, id_bravo));
  assert_true(lc_pouch_index_doc_id_set_sort_unique(&matches));

  invalid_doc_id = 1;
  assert_true(lc_pouch_index_result_page_doc_ids(
      NULL, &table, &matches, "default", "alpha", 1U, &page, &invalid_doc_id));
  assert_false(invalid_doc_id);
  assert_doc_ids(&page.doc_ids, expected,
                 sizeof(expected) / sizeof(expected[0]));
  assert_true(page.truncated);
  assert_string_equal(page.next_start_after, "bravo");

  lc_pouch_index_result_page_cleanup(NULL, &page);
  lc_pouch_index_doc_id_set_cleanup(NULL, &matches);
  lc_pouch_index_doc_table_cleanup(NULL, &table);
}

static void
test_result_page_doc_ids_does_not_truncate_at_exact_end(void **state) {
  lc_pouch_index_doc_table table;
  lc_pouch_index_doc_id_set matches;
  lc_pouch_index_result_page page;
  lc_pouch_index_doc_id id_alpha;
  lc_pouch_index_doc_id id_bravo;
  lc_pouch_index_doc_id expected[] = {0U, 1U};
  int invalid_doc_id;

  (void)state;
  memset(&table, 0, sizeof(table));
  memset(&matches, 0, sizeof(matches));
  memset(&page, 0, sizeof(page));

  assert_true(lc_pouch_index_doc_table_find_or_add(NULL, &table, "default",
                                                   "alpha", &id_alpha));
  assert_true(lc_pouch_index_doc_table_find_or_add(NULL, &table, "default",
                                                   "bravo", &id_bravo));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, &matches, id_alpha));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, &matches, id_bravo));

  invalid_doc_id = 1;
  assert_true(lc_pouch_index_result_page_doc_ids(
      NULL, &table, &matches, "default", NULL, 2U, &page, &invalid_doc_id));
  assert_false(invalid_doc_id);
  assert_doc_ids(&page.doc_ids, expected,
                 sizeof(expected) / sizeof(expected[0]));
  assert_false(page.truncated);
  assert_null(page.next_start_after);

  lc_pouch_index_result_page_cleanup(NULL, &page);
  lc_pouch_index_doc_id_set_cleanup(NULL, &matches);
  lc_pouch_index_doc_table_cleanup(NULL, &table);
}

static void
test_result_page_doc_ids_rejects_missing_doc_table_id(void **state) {
  lc_pouch_index_doc_table table;
  lc_pouch_index_doc_id_set matches;
  lc_pouch_index_result_page page;
  lc_pouch_index_doc_id id_alpha;
  int invalid_doc_id;

  (void)state;
  memset(&table, 0, sizeof(table));
  memset(&matches, 0, sizeof(matches));
  memset(&page, 0, sizeof(page));

  assert_true(lc_pouch_index_doc_table_find_or_add(NULL, &table, "default",
                                                   "alpha", &id_alpha));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, &matches, id_alpha));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, &matches, 99U));

  invalid_doc_id = 0;
  assert_false(lc_pouch_index_result_page_doc_ids(
      NULL, &table, &matches, "default", NULL, 0U, &page, &invalid_doc_id));
  assert_true(invalid_doc_id);
  assert_int_equal(page.doc_ids.count, 0U);
  assert_null(page.next_start_after);

  lc_pouch_index_result_page_cleanup(NULL, &page);
  lc_pouch_index_doc_id_set_cleanup(NULL, &matches);
  lc_pouch_index_doc_table_cleanup(NULL, &table);
}

typedef struct fake_result_collector {
  lc_pouch_index_doc_id ids[8];
  size_t count;
  size_t calls;
  int last_cacheable;
} fake_result_collector;

static int fake_collect_result_doc_ids(void *context, int cacheable,
                                       lc_pouch_index_doc_id_set *doc_ids,
                                       lc_error *error) {
  fake_result_collector *collector;
  size_t index;

  (void)error;
  collector = (fake_result_collector *)context;
  assert_non_null(collector);
  collector->calls++;
  collector->last_cacheable = cacheable;
  for (index = 0U; index < collector->count; ++index) {
    assert_true(
        lc_pouch_index_doc_id_set_append(NULL, doc_ids, collector->ids[index]));
  }
  assert_true(lc_pouch_index_doc_id_set_sort_unique(doc_ids));
  return LC_OK;
}

typedef struct fake_result_remapper {
  lc_pouch_index_doc_id source_ids[8];
  lc_pouch_index_doc_id remapped_ids[8];
  size_t count;
  size_t calls;
} fake_result_remapper;

static int fake_remap_result_doc_ids(void *context,
                                     const lc_pouch_index_doc_id_set *source,
                                     lc_pouch_index_doc_id_set *remapped,
                                     lc_error *error) {
  fake_result_remapper *mapper;
  size_t index;
  size_t map_index;

  (void)error;
  mapper = (fake_result_remapper *)context;
  assert_non_null(mapper);
  assert_non_null(source);
  assert_non_null(remapped);
  mapper->calls++;
  for (index = 0U; index < source->count; ++index) {
    for (map_index = 0U; map_index < mapper->count; ++map_index) {
      if (mapper->source_ids[map_index] == source->items[index]) {
        assert_true(lc_pouch_index_doc_id_set_append(
            NULL, remapped, mapper->remapped_ids[map_index]));
        break;
      }
    }
    assert_true(map_index < mapper->count);
  }
  assert_true(lc_pouch_index_doc_id_set_sort_unique(remapped));
  return LC_OK;
}

static void
test_cached_result_page_uses_cache_and_applies_cursor(void **state) {
  lc_pouch_index_doc_table table;
  lc_pouch_index_result_cache cache;
  lc_pouch_index_result_page page;
  lc_pouch_query_index_scan_req req;
  lc_pouch_document_eq_term eq;
  fake_result_collector collector;
  lc_pouch_index_identity identity;
  lc_pouch_index_doc_id id_alpha;
  lc_pouch_index_doc_id id_bravo;
  lc_pouch_index_doc_id id_charlie;
  lc_pouch_index_doc_id expected_bravo[] = {1U};
  lc_pouch_index_doc_id expected_charlie[] = {2U};
  int invalid_doc_id;
  int rc;

  (void)state;
  memset(&table, 0, sizeof(table));
  memset(&cache, 0, sizeof(cache));
  memset(&page, 0, sizeof(page));
  memset(&req, 0, sizeof(req));
  memset(&eq, 0, sizeof(eq));
  memset(&collector, 0, sizeof(collector));
  memset(&identity, 0, sizeof(identity));

  assert_true(lc_pouch_index_doc_table_find_or_add(NULL, &table, "default",
                                                   "alpha", &id_alpha));
  assert_true(lc_pouch_index_doc_table_find_or_add(NULL, &table, "default",
                                                   "bravo", &id_bravo));
  assert_true(lc_pouch_index_doc_table_find_or_add(NULL, &table, "default",
                                                   "charlie", &id_charlie));
  collector.ids[0] = id_charlie;
  collector.ids[1] = id_alpha;
  collector.ids[2] = id_bravo;
  collector.count = 3U;

  eq.field = "/region";
  eq.value = "s:north";
  req.namespace_name = "default";
  req.document_eq_terms = &eq;
  req.document_eq_term_count = 1U;
  req.start_after = "alpha";
  req.limit = 1U;

  invalid_doc_id = 0;
  rc = lc_pouch_index_cached_result_page(
      NULL, &table, &cache, 7U, &req, LC_POUCH_INDEX_RESULT_PLAN_EQ,
      fake_collect_result_doc_ids, &collector, &page, &invalid_doc_id, NULL);
  assert_int_equal(rc, LC_OK);
  assert_false(invalid_doc_id);
  assert_int_equal(collector.calls, 1U);
  assert_true(collector.last_cacheable);
  assert_int_equal(cache.misses, 1U);
  assert_int_equal(cache.puts, 1U);
  assert_doc_ids(&page.doc_ids, expected_bravo,
                 sizeof(expected_bravo) / sizeof(expected_bravo[0]));
  assert_true(page.truncated);
  assert_string_equal(page.next_start_after, "bravo");
  lc_pouch_index_result_page_cleanup(NULL, &page);

  req.start_after = "bravo";
  invalid_doc_id = 0;
  rc = lc_pouch_index_cached_result_page(
      NULL, &table, &cache, 7U, &req, LC_POUCH_INDEX_RESULT_PLAN_EQ,
      fake_collect_result_doc_ids, &collector, &page, &invalid_doc_id, NULL);
  assert_int_equal(rc, LC_OK);
  assert_false(invalid_doc_id);
  assert_int_equal(collector.calls, 1U);
  assert_int_equal(cache.hits, 1U);
  assert_doc_ids(&page.doc_ids, expected_charlie,
                 sizeof(expected_charlie) / sizeof(expected_charlie[0]));
  assert_false(page.truncated);
  assert_null(page.next_start_after);
  lc_pouch_index_result_page_cleanup(NULL, &page);

  req.start_after = NULL;
  invalid_doc_id = 0;
  rc = lc_pouch_index_cached_result_page(
      NULL, &table, &cache, 8U, &req, LC_POUCH_INDEX_RESULT_PLAN_EQ,
      fake_collect_result_doc_ids, &collector, &page, &invalid_doc_id, NULL);
  assert_int_equal(rc, LC_OK);
  assert_false(invalid_doc_id);
  assert_int_equal(collector.calls, 2U);
  assert_int_equal(cache.misses, 2U);
  assert_int_equal(cache.puts, 2U);
  lc_pouch_index_result_page_cleanup(NULL, &page);

  identity.sequence = 8U;
  identity.manifest_generation = 1U;
  rc = lc_pouch_index_cached_result_page_identity(
      NULL, &table, &cache, identity, &req, LC_POUCH_INDEX_RESULT_PLAN_EQ,
      fake_collect_result_doc_ids, &collector, &page, &invalid_doc_id, NULL);
  assert_int_equal(rc, LC_OK);
  assert_false(invalid_doc_id);
  assert_int_equal(collector.calls, 3U);
  assert_int_equal(cache.misses, 3U);
  assert_int_equal(cache.puts, 3U);

  lc_pouch_index_result_page_cleanup(NULL, &page);
  lc_pouch_index_result_cache_cleanup(NULL, &cache);
  lc_pouch_index_doc_table_cleanup(NULL, &table);
}

static void
test_cached_remapped_result_page_caches_local_doc_ids(void **state) {
  lc_pouch_index_doc_table table;
  lc_pouch_index_result_cache cache;
  lc_pouch_index_result_page page;
  lc_pouch_query_index_scan_req req;
  lc_pouch_document_eq_term eq;
  fake_result_collector collector;
  fake_result_remapper remapper;
  lc_pouch_index_identity identity;
  lc_pouch_index_doc_id id_alpha;
  lc_pouch_index_doc_id id_bravo;
  lc_pouch_index_doc_id id_charlie;
  lc_pouch_index_doc_id expected_bravo[] = {1U};
  lc_pouch_index_doc_id expected_charlie[] = {2U};
  int invalid_doc_id;
  int rc;

  (void)state;
  memset(&table, 0, sizeof(table));
  memset(&cache, 0, sizeof(cache));
  memset(&page, 0, sizeof(page));
  memset(&req, 0, sizeof(req));
  memset(&eq, 0, sizeof(eq));
  memset(&collector, 0, sizeof(collector));
  memset(&remapper, 0, sizeof(remapper));
  memset(&identity, 0, sizeof(identity));

  assert_true(lc_pouch_index_doc_table_find_or_add(NULL, &table, "default",
                                                   "alpha", &id_alpha));
  assert_true(lc_pouch_index_doc_table_find_or_add(NULL, &table, "default",
                                                   "bravo", &id_bravo));
  assert_true(lc_pouch_index_doc_table_find_or_add(NULL, &table, "default",
                                                   "charlie", &id_charlie));
  collector.ids[0] = 102U;
  collector.ids[1] = 100U;
  collector.ids[2] = 101U;
  collector.count = 3U;
  remapper.source_ids[0] = 100U;
  remapper.remapped_ids[0] = id_alpha;
  remapper.source_ids[1] = 101U;
  remapper.remapped_ids[1] = id_bravo;
  remapper.source_ids[2] = 102U;
  remapper.remapped_ids[2] = id_charlie;
  remapper.count = 3U;

  eq.field = "/region";
  eq.value = "s:north";
  req.namespace_name = "default";
  req.document_eq_terms = &eq;
  req.document_eq_term_count = 1U;
  req.start_after = "alpha";
  req.limit = 1U;
  identity.sequence = 7U;
  identity.manifest_generation = 3U;

  invalid_doc_id = 0;
  rc = lc_pouch_index_cached_remapped_result_page_identity(
      NULL, &table, &cache, identity, &req, LC_POUCH_INDEX_RESULT_PLAN_EQ,
      fake_collect_result_doc_ids, &collector, fake_remap_result_doc_ids,
      &remapper, &page, &invalid_doc_id, NULL);
  assert_int_equal(rc, LC_OK);
  assert_false(invalid_doc_id);
  assert_int_equal(collector.calls, 1U);
  assert_int_equal(remapper.calls, 1U);
  assert_true(collector.last_cacheable);
  assert_int_equal(cache.misses, 1U);
  assert_int_equal(cache.puts, 1U);
  assert_doc_ids(&page.doc_ids, expected_bravo,
                 sizeof(expected_bravo) / sizeof(expected_bravo[0]));
  assert_true(page.truncated);
  assert_string_equal(page.next_start_after, "bravo");
  lc_pouch_index_result_page_cleanup(NULL, &page);

  req.start_after = "bravo";
  invalid_doc_id = 0;
  rc = lc_pouch_index_cached_remapped_result_page_identity(
      NULL, &table, &cache, identity, &req, LC_POUCH_INDEX_RESULT_PLAN_EQ,
      fake_collect_result_doc_ids, &collector, fake_remap_result_doc_ids,
      &remapper, &page, &invalid_doc_id, NULL);
  assert_int_equal(rc, LC_OK);
  assert_false(invalid_doc_id);
  assert_int_equal(collector.calls, 1U);
  assert_int_equal(remapper.calls, 1U);
  assert_int_equal(cache.hits, 1U);
  assert_doc_ids(&page.doc_ids, expected_charlie,
                 sizeof(expected_charlie) / sizeof(expected_charlie[0]));
  assert_false(page.truncated);
  assert_null(page.next_start_after);

  lc_pouch_index_result_page_cleanup(NULL, &page);
  lc_pouch_index_result_cache_cleanup(NULL, &cache);
  lc_pouch_index_doc_table_cleanup(NULL, &table);
}

static void test_cached_result_page_allows_uncacheable_collect(void **state) {
  lc_pouch_index_doc_table table;
  lc_pouch_index_result_cache cache;
  lc_pouch_index_result_page page;
  lc_pouch_query_index_scan_req req;
  lc_pouch_document_eq_term eq;
  fake_result_collector collector;
  lc_pouch_index_doc_id id_alpha;
  lc_pouch_index_doc_id expected[] = {0U};
  int invalid_doc_id;
  int rc;

  (void)state;
  memset(&table, 0, sizeof(table));
  memset(&cache, 0, sizeof(cache));
  memset(&page, 0, sizeof(page));
  memset(&req, 0, sizeof(req));
  memset(&eq, 0, sizeof(eq));
  memset(&collector, 0, sizeof(collector));

  assert_true(lc_pouch_index_doc_table_find_or_add(NULL, &table, "default",
                                                   "alpha", &id_alpha));
  collector.ids[0] = id_alpha;
  collector.count = 1U;
  eq.field = "/region";
  eq.value = "s:north";
  req.namespace_name = "default";
  req.owner = "owner-filter";
  req.document_eq_terms = &eq;
  req.document_eq_term_count = 1U;

  invalid_doc_id = 0;
  rc = lc_pouch_index_cached_result_page(
      NULL, &table, &cache, 7U, &req, LC_POUCH_INDEX_RESULT_PLAN_EQ,
      fake_collect_result_doc_ids, &collector, &page, &invalid_doc_id, NULL);
  assert_int_equal(rc, LC_OK);
  assert_false(invalid_doc_id);
  assert_int_equal(collector.calls, 1U);
  assert_false(collector.last_cacheable);
  assert_int_equal(cache.count, 0U);
  assert_int_equal(cache.hits, 0U);
  assert_int_equal(cache.misses, 0U);
  assert_int_equal(cache.puts, 0U);
  assert_doc_ids(&page.doc_ids, expected,
                 sizeof(expected) / sizeof(expected[0]));

  lc_pouch_index_result_page_cleanup(NULL, &page);
  lc_pouch_index_result_cache_cleanup(NULL, &cache);
  lc_pouch_index_doc_table_cleanup(NULL, &table);
}

static void test_cached_result_page_reports_invalid_doc_id(void **state) {
  lc_pouch_index_doc_table table;
  lc_pouch_index_result_cache cache;
  lc_pouch_index_result_page page;
  lc_pouch_query_index_scan_req req;
  lc_pouch_document_eq_term eq;
  fake_result_collector collector;
  lc_pouch_index_doc_id id_alpha;
  int invalid_doc_id;
  int rc;

  (void)state;
  memset(&table, 0, sizeof(table));
  memset(&cache, 0, sizeof(cache));
  memset(&page, 0, sizeof(page));
  memset(&req, 0, sizeof(req));
  memset(&eq, 0, sizeof(eq));
  memset(&collector, 0, sizeof(collector));

  assert_true(lc_pouch_index_doc_table_find_or_add(NULL, &table, "default",
                                                   "alpha", &id_alpha));
  collector.ids[0] = id_alpha;
  collector.ids[1] = 99U;
  collector.count = 2U;
  eq.field = "/region";
  eq.value = "s:north";
  req.namespace_name = "default";
  req.document_eq_terms = &eq;
  req.document_eq_term_count = 1U;

  invalid_doc_id = 0;
  rc = lc_pouch_index_cached_result_page(
      NULL, &table, &cache, 7U, &req, LC_POUCH_INDEX_RESULT_PLAN_EQ,
      fake_collect_result_doc_ids, &collector, &page, &invalid_doc_id, NULL);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_true(invalid_doc_id);
  assert_int_equal(page.doc_ids.count, 0U);

  lc_pouch_index_result_page_cleanup(NULL, &page);
  lc_pouch_index_result_cache_cleanup(NULL, &cache);
  lc_pouch_index_doc_table_cleanup(NULL, &table);
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
  reader->calls++;
  if (strcmp(field, "/region") == 0 && strcmp(value, "s:north") == 0) {
    assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 7U));
    assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 3U));
    assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 5U));
  } else if (strcmp(field, "/region") == 0 && strcmp(value, "s:south") == 0) {
    assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 3U));
    assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 9U));
  } else if (strcmp(field, "/status") == 0 && strcmp(value, "s:paid") == 0) {
    assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 5U));
    assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 3U));
    assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 9U));
  } else {
    fail_msg("unexpected exact term: %s=%s", field, value);
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
  reader->calls++;
  if (strcmp(field, "/tags/0") == 0) {
    assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 11U));
    assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 5U));
    assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 11U));
  } else if (strcmp(field, "/tagged") == 0) {
    assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 13U));
    assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 3U));
    assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 5U));
    assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 13U));
    assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 7U));
  } else {
    fail_msg("unexpected exists term: %s", field);
  }
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
  assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 3U));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 5U));
  return LC_OK;
}

static int fake_read_prefix_doc_ids(void *context,
                                    const lc_pouch_document_prefix_term *term,
                                    lc_pouch_index_doc_id_set *doc_ids,
                                    lc_error *error) {
  fake_exact_reader *reader;

  (void)error;
  reader = (fake_exact_reader *)context;
  assert_non_null(reader);
  assert_non_null(term);
  assert_string_equal(term->field, "/owner");
  assert_string_equal(term->value, "bench-");
  assert_true(term->ignore_case);
  reader->calls++;
  assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 13U));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 4U));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 13U));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 3U));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 5U));
  return LC_OK;
}

static int fake_read_contains_doc_ids(
    void *context, const lc_pouch_document_contains_term *term,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error) {
  fake_exact_reader *reader;

  (void)error;
  reader = (fake_exact_reader *)context;
  assert_non_null(reader);
  assert_non_null(term);
  assert_string_equal(term->field, "/body");
  assert_string_equal(term->value, "needle");
  assert_false(term->ignore_case);
  reader->calls++;
  assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 21U));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 6U));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 21U));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 3U));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 5U));
  return LC_OK;
}

static int fake_read_date_after_doc_ids(void *context, const char *field,
                                        int64_t unix_seconds,
                                        int32_t nanosecond,
                                        lc_pouch_index_doc_id_set *doc_ids,
                                        lc_error *error) {
  fake_exact_reader *reader;

  (void)error;
  reader = (fake_exact_reader *)context;
  assert_non_null(reader);
  assert_string_equal(field, "/created_at");
  assert_int_equal(unix_seconds, 1735689600);
  assert_int_equal(nanosecond, 0);
  reader->calls++;
  assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 11U));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 5U));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 11U));
  assert_true(lc_pouch_index_doc_id_set_append(NULL, doc_ids, 3U));
  return LC_OK;
}

static void
test_collect_in_term_doc_ids_uses_reader_and_deduplicates(void **state) {
  const char *values[3];
  lc_pouch_document_in_term term;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {3U, 5U, 7U, 9U};
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
test_collect_in_term_with_eq_doc_ids_intersects_in_index(void **state) {
  const char *values[2];
  lc_pouch_document_in_term in;
  lc_pouch_document_eq_term eq[2];
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {3U, 5U};
  fake_exact_reader reader;
  lc_error error;
  int rc;

  (void)state;
  memset(&in, 0, sizeof(in));
  memset(&eq, 0, sizeof(eq));
  memset(&doc_ids, 0, sizeof(doc_ids));
  memset(&reader, 0, sizeof(reader));
  memset(&error, 0, sizeof(error));

  values[0] = "s:north";
  values[1] = "s:south";
  in.field = "/region";
  in.values = values;
  in.value_count = sizeof(values) / sizeof(values[0]);
  eq[0].field = "/region";
  eq[0].value = "s:north";
  eq[1].field = "/status";
  eq[1].value = "s:paid";
  rc = lc_pouch_index_collect_in_term_with_eq_doc_ids(
      NULL, &in, eq, sizeof(eq) / sizeof(eq[0]), fake_read_exact_doc_ids,
      &reader, &doc_ids, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(reader.calls, 4U);
  assert_doc_ids(&doc_ids, expected, sizeof(expected) / sizeof(expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_error_cleanup(&error);
}

static void
test_collect_in_term_with_eq_and_not_eq_doc_ids_filters_in_index(void **state) {
  const char *values[2];
  lc_pouch_document_in_term in;
  lc_pouch_document_eq_term eq;
  lc_pouch_document_eq_term not_equal;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {5U};
  fake_exact_reader reader;
  lc_error error;
  int rc;

  (void)state;
  memset(&in, 0, sizeof(in));
  memset(&eq, 0, sizeof(eq));
  memset(&not_equal, 0, sizeof(not_equal));
  memset(&doc_ids, 0, sizeof(doc_ids));
  memset(&reader, 0, sizeof(reader));
  memset(&error, 0, sizeof(error));

  values[0] = "s:north";
  values[1] = "s:south";
  in.field = "/region";
  in.values = values;
  in.value_count = sizeof(values) / sizeof(values[0]);
  eq.field = "/status";
  eq.value = "s:paid";
  not_equal.field = "/region";
  not_equal.value = "s:south";
  rc = lc_pouch_index_collect_in_term_with_eq_and_not_eq_doc_ids(
      NULL, &in, &eq, 1U, &not_equal, 1U, fake_read_exact_doc_ids, &reader,
      &doc_ids, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(reader.calls, 4U);
  assert_doc_ids(&doc_ids, expected, sizeof(expected) / sizeof(expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_error_cleanup(&error);
}

static void
test_collect_eq_term_doc_ids_uses_reader_and_deduplicates(void **state) {
  lc_pouch_document_eq_term term;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {3U, 5U, 7U};
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
test_collect_eq_term_with_not_eq_doc_ids_subtracts_in_index(void **state) {
  lc_pouch_document_eq_term term;
  lc_pouch_document_eq_term not_equal;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {7U};
  fake_exact_reader reader;
  lc_error error;
  int rc;

  (void)state;
  memset(&term, 0, sizeof(term));
  memset(&not_equal, 0, sizeof(not_equal));
  memset(&doc_ids, 0, sizeof(doc_ids));
  memset(&reader, 0, sizeof(reader));
  memset(&error, 0, sizeof(error));

  term.field = "/region";
  term.value = "s:north";
  not_equal.field = "/status";
  not_equal.value = "s:paid";
  rc = lc_pouch_index_collect_eq_term_with_not_eq_doc_ids(
      NULL, &term, &not_equal, 1U, fake_read_exact_doc_ids, &reader, &doc_ids,
      &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(reader.calls, 2U);
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
test_collect_exists_term_with_eq_doc_ids_intersects_in_index(void **state) {
  lc_pouch_document_exists_term exists;
  lc_pouch_document_eq_term eq[2];
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {3U, 5U};
  fake_exact_reader reader;
  lc_error error;
  int rc;

  (void)state;
  memset(&exists, 0, sizeof(exists));
  memset(&eq, 0, sizeof(eq));
  memset(&doc_ids, 0, sizeof(doc_ids));
  memset(&reader, 0, sizeof(reader));
  memset(&error, 0, sizeof(error));

  exists.field = "/tagged";
  eq[0].field = "/region";
  eq[0].value = "s:north";
  eq[1].field = "/status";
  eq[1].value = "s:paid";
  rc = lc_pouch_index_collect_exists_term_with_eq_doc_ids(
      NULL, &exists, eq, sizeof(eq) / sizeof(eq[0]), fake_read_exists_doc_ids,
      fake_read_exact_doc_ids, &reader, &doc_ids, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(reader.calls, 3U);
  assert_doc_ids(&doc_ids, expected, sizeof(expected) / sizeof(expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_error_cleanup(&error);
}

static void
test_collect_exists_term_with_eq_and_not_eq_doc_ids_filters_in_index(
    void **state) {
  lc_pouch_document_exists_term exists;
  lc_pouch_document_eq_term eq;
  lc_pouch_document_eq_term not_equal;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {5U};
  fake_exact_reader reader;
  lc_error error;
  int rc;

  (void)state;
  memset(&exists, 0, sizeof(exists));
  memset(&eq, 0, sizeof(eq));
  memset(&not_equal, 0, sizeof(not_equal));
  memset(&doc_ids, 0, sizeof(doc_ids));
  memset(&reader, 0, sizeof(reader));
  memset(&error, 0, sizeof(error));

  exists.field = "/tagged";
  eq.field = "/status";
  eq.value = "s:paid";
  not_equal.field = "/region";
  not_equal.value = "s:south";
  rc = lc_pouch_index_collect_exists_term_with_eq_and_not_eq_doc_ids(
      NULL, &exists, &eq, 1U, &not_equal, 1U, fake_read_exists_doc_ids,
      fake_read_exact_doc_ids, &reader, &doc_ids, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(reader.calls, 3U);
  assert_doc_ids(&doc_ids, expected, sizeof(expected) / sizeof(expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_error_cleanup(&error);
}

static void
test_collect_range_term_doc_ids_uses_reader_and_deduplicates(void **state) {
  lc_pouch_document_range_term term;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {2U, 3U, 5U, 8U};
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

static void
test_collect_range_term_with_eq_doc_ids_intersects_in_index(void **state) {
  lc_pouch_document_range_term range;
  lc_pouch_document_eq_term eq[2];
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {3U, 5U};
  fake_exact_reader reader;
  lc_error error;
  int rc;

  (void)state;
  memset(&range, 0, sizeof(range));
  memset(&eq, 0, sizeof(eq));
  memset(&doc_ids, 0, sizeof(doc_ids));
  memset(&reader, 0, sizeof(reader));
  memset(&error, 0, sizeof(error));

  range.field = "/score";
  range.gte = "n:+:1:0";
  range.lt = "n:+:9:0";
  eq[0].field = "/region";
  eq[0].value = "s:north";
  eq[1].field = "/status";
  eq[1].value = "s:paid";
  rc = lc_pouch_index_collect_range_term_with_eq_doc_ids(
      NULL, &range, eq, sizeof(eq) / sizeof(eq[0]), fake_read_range_doc_ids,
      fake_read_exact_doc_ids, &reader, &doc_ids, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(reader.calls, 3U);
  assert_doc_ids(&doc_ids, expected, sizeof(expected) / sizeof(expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_error_cleanup(&error);
}

static void test_collect_range_term_with_eq_and_not_eq_doc_ids_filters_in_index(
    void **state) {
  lc_pouch_document_range_term range;
  lc_pouch_document_eq_term eq[2];
  lc_pouch_document_eq_term not_equal;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {5U};
  fake_exact_reader reader;
  lc_error error;
  int rc;

  (void)state;
  memset(&range, 0, sizeof(range));
  memset(&eq, 0, sizeof(eq));
  memset(&not_equal, 0, sizeof(not_equal));
  memset(&doc_ids, 0, sizeof(doc_ids));
  memset(&reader, 0, sizeof(reader));
  memset(&error, 0, sizeof(error));

  range.field = "/score";
  range.gte = "n:+:1:0";
  range.lt = "n:+:9:0";
  eq[0].field = "/region";
  eq[0].value = "s:north";
  eq[1].field = "/status";
  eq[1].value = "s:paid";
  not_equal.field = "/region";
  not_equal.value = "s:south";
  rc = lc_pouch_index_collect_range_term_with_eq_and_not_eq_doc_ids(
      NULL, &range, eq, sizeof(eq) / sizeof(eq[0]), &not_equal, 1U,
      fake_read_range_doc_ids, fake_read_exact_doc_ids, &reader, &doc_ids,
      &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(reader.calls, 4U);
  assert_doc_ids(&doc_ids, expected, sizeof(expected) / sizeof(expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_error_cleanup(&error);
}

static void
test_collect_prefix_term_doc_ids_uses_reader_and_deduplicates(void **state) {
  lc_pouch_document_prefix_term term;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {3U, 4U, 5U, 13U};
  fake_exact_reader reader;
  lc_error error;
  int rc;

  (void)state;
  memset(&term, 0, sizeof(term));
  memset(&doc_ids, 0, sizeof(doc_ids));
  memset(&reader, 0, sizeof(reader));
  memset(&error, 0, sizeof(error));

  term.field = "/owner";
  term.value = "bench-";
  term.ignore_case = 1;
  rc = lc_pouch_index_collect_prefix_term_doc_ids(
      NULL, &term, fake_read_prefix_doc_ids, &reader, &doc_ids, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(reader.calls, 1U);
  assert_doc_ids(&doc_ids, expected, sizeof(expected) / sizeof(expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_error_cleanup(&error);
}

static void
test_collect_prefix_term_with_eq_doc_ids_intersects_in_index(void **state) {
  lc_pouch_document_prefix_term prefix;
  lc_pouch_document_eq_term eq[2];
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {3U, 5U};
  fake_exact_reader reader;
  lc_error error;
  int rc;

  (void)state;
  memset(&prefix, 0, sizeof(prefix));
  memset(&eq, 0, sizeof(eq));
  memset(&doc_ids, 0, sizeof(doc_ids));
  memset(&reader, 0, sizeof(reader));
  memset(&error, 0, sizeof(error));

  prefix.field = "/owner";
  prefix.value = "bench-";
  prefix.ignore_case = 1;
  eq[0].field = "/region";
  eq[0].value = "s:north";
  eq[1].field = "/status";
  eq[1].value = "s:paid";
  rc = lc_pouch_index_collect_prefix_term_with_eq_doc_ids(
      NULL, &prefix, eq, sizeof(eq) / sizeof(eq[0]), fake_read_prefix_doc_ids,
      fake_read_exact_doc_ids, &reader, &doc_ids, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(reader.calls, 3U);
  assert_doc_ids(&doc_ids, expected, sizeof(expected) / sizeof(expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_error_cleanup(&error);
}

static void
test_collect_prefix_term_with_eq_and_not_eq_doc_ids_filters_in_index(
    void **state) {
  lc_pouch_document_prefix_term prefix;
  lc_pouch_document_eq_term eq[2];
  lc_pouch_document_eq_term not_equal;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {5U};
  fake_exact_reader reader;
  lc_error error;
  int rc;

  (void)state;
  memset(&prefix, 0, sizeof(prefix));
  memset(&eq, 0, sizeof(eq));
  memset(&not_equal, 0, sizeof(not_equal));
  memset(&doc_ids, 0, sizeof(doc_ids));
  memset(&reader, 0, sizeof(reader));
  memset(&error, 0, sizeof(error));

  prefix.field = "/owner";
  prefix.value = "bench-";
  prefix.ignore_case = 1;
  eq[0].field = "/region";
  eq[0].value = "s:north";
  eq[1].field = "/status";
  eq[1].value = "s:paid";
  not_equal.field = "/region";
  not_equal.value = "s:south";
  rc = lc_pouch_index_collect_prefix_term_with_eq_and_not_eq_doc_ids(
      NULL, &prefix, eq, sizeof(eq) / sizeof(eq[0]), &not_equal, 1U,
      fake_read_prefix_doc_ids, fake_read_exact_doc_ids, &reader, &doc_ids,
      &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(reader.calls, 4U);
  assert_doc_ids(&doc_ids, expected, sizeof(expected) / sizeof(expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_error_cleanup(&error);
}

static void
test_collect_contains_term_doc_ids_uses_reader_and_deduplicates(void **state) {
  lc_pouch_document_contains_term term;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {3U, 5U, 6U, 21U};
  fake_exact_reader reader;
  lc_error error;
  int rc;

  (void)state;
  memset(&term, 0, sizeof(term));
  memset(&doc_ids, 0, sizeof(doc_ids));
  memset(&reader, 0, sizeof(reader));
  memset(&error, 0, sizeof(error));

  term.field = "/body";
  term.value = "needle";
  term.ignore_case = 0;
  rc = lc_pouch_index_collect_contains_term_doc_ids(
      NULL, &term, fake_read_contains_doc_ids, &reader, &doc_ids, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(reader.calls, 1U);
  assert_doc_ids(&doc_ids, expected, sizeof(expected) / sizeof(expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_error_cleanup(&error);
}

static void
test_collect_contains_term_with_eq_and_not_eq_doc_ids_filters_in_index(
    void **state) {
  lc_pouch_document_contains_term contains;
  lc_pouch_document_eq_term eq[2];
  lc_pouch_document_eq_term not_equal;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {5U};
  fake_exact_reader reader;
  lc_error error;
  int rc;

  (void)state;
  memset(&contains, 0, sizeof(contains));
  memset(&eq, 0, sizeof(eq));
  memset(&not_equal, 0, sizeof(not_equal));
  memset(&doc_ids, 0, sizeof(doc_ids));
  memset(&reader, 0, sizeof(reader));
  memset(&error, 0, sizeof(error));

  contains.field = "/body";
  contains.value = "needle";
  contains.ignore_case = 0;
  eq[0].field = "/region";
  eq[0].value = "s:north";
  eq[1].field = "/status";
  eq[1].value = "s:paid";
  not_equal.field = "/region";
  not_equal.value = "s:south";
  rc = lc_pouch_index_collect_contains_term_with_eq_and_not_eq_doc_ids(
      NULL, &contains, eq, sizeof(eq) / sizeof(eq[0]), &not_equal, 1U,
      fake_read_contains_doc_ids, fake_read_exact_doc_ids, &reader, &doc_ids,
      &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(reader.calls, 4U);
  assert_doc_ids(&doc_ids, expected, sizeof(expected) / sizeof(expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_error_cleanup(&error);
}

static void
test_collect_contains_term_with_eq_doc_ids_intersects_in_index(void **state) {
  lc_pouch_document_contains_term contains;
  lc_pouch_document_eq_term eq[2];
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {3U, 5U};
  fake_exact_reader reader;
  lc_error error;
  int rc;

  (void)state;
  memset(&contains, 0, sizeof(contains));
  memset(&eq, 0, sizeof(eq));
  memset(&doc_ids, 0, sizeof(doc_ids));
  memset(&reader, 0, sizeof(reader));
  memset(&error, 0, sizeof(error));

  contains.field = "/body";
  contains.value = "needle";
  contains.ignore_case = 0;
  eq[0].field = "/region";
  eq[0].value = "s:north";
  eq[1].field = "/status";
  eq[1].value = "s:paid";
  rc = lc_pouch_index_collect_contains_term_with_eq_doc_ids(
      NULL, &contains, eq, sizeof(eq) / sizeof(eq[0]),
      fake_read_contains_doc_ids, fake_read_exact_doc_ids, &reader, &doc_ids,
      &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(reader.calls, 3U);
  assert_doc_ids(&doc_ids, expected, sizeof(expected) / sizeof(expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_error_cleanup(&error);
}

static void
test_collect_date_after_doc_ids_uses_reader_and_deduplicates(void **state) {
  lc_pouch_document_date_after_term term;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_doc_id expected[] = {3U, 5U, 11U};
  fake_exact_reader reader;
  lc_error error;
  int rc;

  (void)state;
  memset(&term, 0, sizeof(term));
  memset(&doc_ids, 0, sizeof(doc_ids));
  memset(&reader, 0, sizeof(reader));
  memset(&error, 0, sizeof(error));

  term.field = "/created_at";
  term.after = "2025-01-01";
  rc = lc_pouch_index_collect_date_after_doc_ids(
      NULL, &term, fake_read_date_after_doc_ids, &reader, &doc_ids, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(reader.calls, 1U);
  assert_doc_ids(&doc_ids, expected, sizeof(expected) / sizeof(expected[0]));

  lc_pouch_index_doc_id_set_cleanup(NULL, &doc_ids);
  lc_error_cleanup(&error);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_doc_table_assigns_dense_ids_by_namespace_key),
      cmocka_unit_test(test_doc_generation_codec_round_trips),
      cmocka_unit_test(test_doc_generation_codec_rejects_corruption),
      cmocka_unit_test(test_doc_id_set_sort_unique),
      cmocka_unit_test(test_doc_id_scratch_reuses_reset_buffers),
      cmocka_unit_test(test_doc_id_set_algebra),
      cmocka_unit_test(test_doc_id_set_algebra_allows_alias_destination),
      cmocka_unit_test(test_posting_sparse_decodes_sorted_unique_doc_ids),
      cmocka_unit_test(test_posting_sparse_handles_max_doc_id),
      cmocka_unit_test(test_posting_dense_decodes_and_intersects),
      cmocka_unit_test(
          test_temporal_posting_table_appends_after_bound_and_residuals),
      cmocka_unit_test(test_temporal_posting_table_codec_round_trips),
      cmocka_unit_test(test_temporal_posting_table_codec_rejects_corruption),
      cmocka_unit_test(test_temporal_generation_codec_round_trips),
      cmocka_unit_test(test_temporal_generation_codec_rejects_corruption),
      cmocka_unit_test(test_text_posting_table_appends_prefix_and_contains),
      cmocka_unit_test(test_text_posting_table_codec_round_trips),
      cmocka_unit_test(test_text_generation_codec_round_trips),
      cmocka_unit_test(test_text_generation_codec_rejects_corruption),
      cmocka_unit_test(test_term_table_interns_sorted_terms_with_stable_ids),
      cmocka_unit_test(test_term_posting_table_decodes_by_term_id),
      cmocka_unit_test(test_term_posting_table_appends_by_term_id),
      cmocka_unit_test(test_term_posting_table_replaces_existing_posting),
      cmocka_unit_test(test_term_generation_codec_round_trips),
      cmocka_unit_test(test_term_generation_codec_rejects_corruption),
      cmocka_unit_test(test_number_posting_table_appends_range_and_residuals),
      cmocka_unit_test(test_number_generation_codec_round_trips),
      cmocka_unit_test(test_number_generation_codec_rejects_corruption),
      cmocka_unit_test(test_result_cache_keys_by_generation_and_plan),
      cmocka_unit_test(test_result_plan_keys_are_normalized_by_index),
      cmocka_unit_test(test_result_plan_keys_reject_filtered_compound_views),
      cmocka_unit_test(test_prepared_term_cache_refreshes_by_generation),
      cmocka_unit_test(test_prepared_temporal_cache_refreshes_by_identity),
      cmocka_unit_test(test_result_cache_replaces_existing_entry),
      cmocka_unit_test(
          test_result_page_doc_ids_applies_namespace_cursor_and_limit),
      cmocka_unit_test(test_result_page_doc_ids_does_not_truncate_at_exact_end),
      cmocka_unit_test(test_result_page_doc_ids_rejects_missing_doc_table_id),
      cmocka_unit_test(test_cached_result_page_uses_cache_and_applies_cursor),
      cmocka_unit_test(test_cached_remapped_result_page_caches_local_doc_ids),
      cmocka_unit_test(test_cached_result_page_allows_uncacheable_collect),
      cmocka_unit_test(test_cached_result_page_reports_invalid_doc_id),
      cmocka_unit_test(
          test_collect_in_term_doc_ids_uses_reader_and_deduplicates),
      cmocka_unit_test(
          test_collect_in_term_with_eq_doc_ids_intersects_in_index),
      cmocka_unit_test(
          test_collect_in_term_with_eq_and_not_eq_doc_ids_filters_in_index),
      cmocka_unit_test(
          test_collect_eq_term_doc_ids_uses_reader_and_deduplicates),
      cmocka_unit_test(
          test_collect_eq_term_with_not_eq_doc_ids_subtracts_in_index),
      cmocka_unit_test(
          test_collect_exists_term_doc_ids_uses_reader_and_deduplicates),
      cmocka_unit_test(
          test_collect_exists_term_with_eq_doc_ids_intersects_in_index),
      cmocka_unit_test(
          test_collect_exists_term_with_eq_and_not_eq_doc_ids_filters_in_index),
      cmocka_unit_test(
          test_collect_range_term_doc_ids_uses_reader_and_deduplicates),
      cmocka_unit_test(
          test_collect_range_term_with_eq_doc_ids_intersects_in_index),
      cmocka_unit_test(
          test_collect_range_term_with_eq_and_not_eq_doc_ids_filters_in_index),
      cmocka_unit_test(
          test_collect_prefix_term_doc_ids_uses_reader_and_deduplicates),
      cmocka_unit_test(
          test_collect_prefix_term_with_eq_doc_ids_intersects_in_index),
      cmocka_unit_test(
          test_collect_prefix_term_with_eq_and_not_eq_doc_ids_filters_in_index),
      cmocka_unit_test(
          test_collect_contains_term_doc_ids_uses_reader_and_deduplicates),
      cmocka_unit_test(
          test_collect_contains_term_with_eq_doc_ids_intersects_in_index),
      cmocka_unit_test(
          test_collect_contains_term_with_eq_and_not_eq_doc_ids_filters_in_index),
      cmocka_unit_test(
          test_collect_date_after_doc_ids_uses_reader_and_deduplicates),
  };
  return cmocka_run_group_tests(tests, NULL, NULL);
}

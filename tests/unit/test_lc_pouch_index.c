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

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_doc_id_set_sort_unique),
      cmocka_unit_test(test_doc_id_set_algebra),
      cmocka_unit_test(test_doc_id_set_algebra_allows_alias_destination),
      cmocka_unit_test(test_posting_sparse_decodes_sorted_unique_doc_ids),
      cmocka_unit_test(test_posting_sparse_handles_max_doc_id),
      cmocka_unit_test(test_posting_dense_decodes_and_intersects),
  };
  return cmocka_run_group_tests(tests, NULL, NULL);
}

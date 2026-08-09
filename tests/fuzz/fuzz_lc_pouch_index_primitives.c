#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "lc/lc.h"
#include "lc_api_internal.h"
#include "lc_pouch_index.h"

static char *fuzz_copy_bytes(const uint8_t *data, size_t size) {
  char *copy;

  copy = (char *)malloc(size + 1U);
  if (copy == NULL) {
    return NULL;
  }
  if (size > 0U) {
    memcpy(copy, data, size);
  }
  copy[size] = '\0';
  return copy;
}

static void fuzz_parse_term_lines(const uint8_t *data, size_t size,
                                  const lc_allocator *allocator) {
  lc_pouch_index_term_field field;
  lc_pouch_index_term_value value;
  lc_error error;
  char *line;

  lc_error_init(&error);
  memset(&field, 0, sizeof(field));
  memset(&value, 0, sizeof(value));
  line = fuzz_copy_bytes(data, size);
  if (line != NULL) {
    (void)lc_pouch_index_term_field_parse_line(line, &field, allocator, &error);
    lc_free_with_allocator(allocator, field.field_hex);
    free(line);
  }
  lc_error_cleanup(&error);

  lc_error_init(&error);
  line = fuzz_copy_bytes(data, size);
  if (line != NULL) {
    (void)lc_pouch_index_term_value_parse_line(line, &value, allocator, &error);
    lc_free_with_allocator(allocator, value.field_hex);
    lc_free_with_allocator(allocator, value.value_hex);
    free(line);
  }
  lc_error_cleanup(&error);
}

static void fuzz_parse_dates(const uint8_t *data, size_t size) {
  lc_pouch_index_date_bounds bounds;
  lc_pouch_index_parsed_date_bounds parsed;
  lc_pouch_index_instant instant;
  lc_error error;
  char *text;

  text = fuzz_copy_bytes(data, size);
  if (text == NULL) {
    return;
  }
  (void)lc_pouch_index_parse_lql_datetime(text, &instant);
  memset(&bounds, 0, sizeof(bounds));
  bounds.has_gt = size > 0U && (data[0] & 1U) != 0U;
  bounds.has_gte = size > 0U && (data[0] & 2U) != 0U;
  bounds.has_lt = size > 0U && (data[0] & 4U) != 0U;
  bounds.has_lte = size > 0U && (data[0] & 8U) != 0U;
  bounds.gt = text;
  bounds.gte = text;
  bounds.lt = text;
  bounds.lte = text;
  lc_error_init(&error);
  if (lc_pouch_index_parse_date_bounds(&bounds, &parsed, &error) == LC_OK) {
    (void)lc_pouch_index_date_contains_value(&parsed, &instant);
  }
  lc_error_cleanup(&error);
  free(text);
}

static void fuzz_decode_generations(const uint8_t *data, size_t size,
                                    const lc_allocator *allocator) {
  lc_pouch_index_term_generation generation;
  lc_error error;
  int valid;

  memset(&generation, 0, sizeof(generation));
  valid = 0;
  lc_error_init(&error);
  (void)lc_pouch_index_term_generation_load_bytes(allocator, (const char *)data,
                                                  size, 1UL, 0UL, 0UL,
                                                  &generation, &valid, &error);
  lc_pouch_index_term_generation_cleanup(allocator, &generation);
  lc_error_cleanup(&error);
}

static void fuzz_decode_postings(const uint8_t *data, size_t size,
                                 const lc_allocator *allocator) {
  lc_pouch_index_posting sparse;
  lc_pouch_index_dense_posting dense;
  lc_pouch_index_docid_set set;
  lc_error error;

  memset(&sparse, 0, sizeof(sparse));
  memset(&dense, 0, sizeof(dense));
  memset(&set, 0, sizeof(set));
  lc_error_init(&error);
  if (size > 0U) {
    sparse.bytes = (unsigned char *)lc_alloc_with_allocator(allocator, size);
    dense.bits = (unsigned char *)lc_alloc_with_allocator(allocator, size);
    if (sparse.bytes != NULL) {
      memcpy(sparse.bytes, data, size);
      sparse.length = size;
      sparse.capacity = size;
      sparse.count = (size_t)(data[0] & 31U);
      (void)lc_pouch_index_posting_append_to_set(&sparse, &set, allocator,
                                                 &error);
    }
    lc_pouch_index_docid_set_cleanup(allocator, &set);
    memset(&set, 0, sizeof(set));
    if (dense.bits != NULL) {
      memcpy(dense.bits, data, size);
      dense.length = size;
      dense.capacity = size;
      dense.count = (size_t)(data[0] & 31U);
      dense.max_doc_id = size > 1U ? (unsigned long)data[1] : 0UL;
      (void)lc_pouch_index_dense_posting_append_to_set(&dense, &set, allocator,
                                                       &error);
    }
  }
  lc_pouch_index_docid_set_cleanup(allocator, &set);
  lc_pouch_index_posting_cleanup(allocator, &sparse);
  lc_pouch_index_dense_posting_cleanup(allocator, &dense);
  lc_error_cleanup(&error);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  lc_allocator allocator;

  if (data == NULL) {
    return 0;
  }
  if (size > 8192U) {
    size = 8192U;
  }
  lc_allocator_init(&allocator);
  fuzz_parse_term_lines(data, size, &allocator);
  fuzz_parse_dates(data, size);
  fuzz_decode_generations(data, size, &allocator);
  fuzz_decode_postings(data, size, &allocator);
  return 0;
}

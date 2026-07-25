#include "lc_pouch_index.h"

#include <stdlib.h>
#include <string.h>

#define LC_POUCH_INDEX_DENSE_THRESHOLD_NUM 18U
#define LC_POUCH_INDEX_DENSE_THRESHOLD_DEN 100U

static int lc_pouch_index_doc_id_compare(const void *left, const void *right) {
  lc_pouch_index_doc_id left_id;
  lc_pouch_index_doc_id right_id;

  left_id = *(const lc_pouch_index_doc_id *)left;
  right_id = *(const lc_pouch_index_doc_id *)right;
  if (left_id < right_id) {
    return -1;
  }
  if (left_id > right_id) {
    return 1;
  }
  return 0;
}

static int
lc_pouch_index_doc_id_set_reserve(const lc_pouch_allocator *allocator,
                                  lc_pouch_index_doc_id_set *set,
                                  size_t needed) {
  size_t new_capacity;
  lc_pouch_index_doc_id *grown;

  if (set == NULL) {
    return 0;
  }
  if (needed <= set->capacity) {
    return 1;
  }
  new_capacity = set->capacity == 0U ? 16U : set->capacity;
  while (new_capacity < needed) {
    if (new_capacity > ((size_t)-1) / 2U) {
      return 0;
    }
    new_capacity *= 2U;
  }
  if (new_capacity > ((size_t)-1) / sizeof(set->items[0])) {
    return 0;
  }
  grown = (lc_pouch_index_doc_id *)lc_pouch_realloc(
      allocator, set->items, new_capacity * sizeof(set->items[0]));
  if (grown == NULL) {
    return 0;
  }
  set->items = grown;
  set->capacity = new_capacity;
  return 1;
}

void lc_pouch_index_doc_id_set_cleanup(const lc_pouch_allocator *allocator,
                                       lc_pouch_index_doc_id_set *set) {
  if (set == NULL) {
    return;
  }
  lc_pouch_free(allocator, set->items);
  memset(set, 0, sizeof(*set));
}

int lc_pouch_index_doc_id_set_append(const lc_pouch_allocator *allocator,
                                     lc_pouch_index_doc_id_set *set,
                                     lc_pouch_index_doc_id id) {
  if (!lc_pouch_index_doc_id_set_reserve(allocator, set, set->count + 1U)) {
    return 0;
  }
  set->items[set->count++] = id;
  return 1;
}

int lc_pouch_index_doc_id_set_sort_unique(lc_pouch_index_doc_id_set *set) {
  size_t read_index;
  size_t write_index;

  if (set == NULL || set->count < 2U) {
    return 1;
  }
  qsort(set->items, set->count, sizeof(set->items[0]),
        lc_pouch_index_doc_id_compare);
  write_index = 1U;
  for (read_index = 1U; read_index < set->count; ++read_index) {
    if (set->items[read_index] == set->items[write_index - 1U]) {
      continue;
    }
    set->items[write_index++] = set->items[read_index];
  }
  set->count = write_index;
  return 1;
}

int lc_pouch_index_doc_id_set_clone(const lc_pouch_allocator *allocator,
                                    lc_pouch_index_doc_id_set *dst,
                                    const lc_pouch_index_doc_id_set *src) {
  if (dst == NULL || src == NULL) {
    return 0;
  }
  dst->count = 0U;
  if (src->count == 0U) {
    return 1;
  }
  if (!lc_pouch_index_doc_id_set_reserve(allocator, dst, src->count)) {
    return 0;
  }
  memcpy(dst->items, src->items, src->count * sizeof(src->items[0]));
  dst->count = src->count;
  return 1;
}

int lc_pouch_index_doc_id_set_union(const lc_pouch_allocator *allocator,
                                    lc_pouch_index_doc_id_set *dst,
                                    const lc_pouch_index_doc_id_set *left,
                                    const lc_pouch_index_doc_id_set *right) {
  lc_pouch_index_doc_id_set tmp;
  size_t left_index;
  size_t right_index;
  size_t needed;

  if (dst == NULL || left == NULL || right == NULL) {
    return 0;
  }
  if (dst == left || dst == right) {
    memset(&tmp, 0, sizeof(tmp));
    if (!lc_pouch_index_doc_id_set_union(allocator, &tmp, left, right)) {
      return 0;
    }
    lc_pouch_index_doc_id_set_cleanup(allocator, dst);
    *dst = tmp;
    return 1;
  }
  dst->count = 0U;
  if (left->count > ((size_t)-1) - right->count) {
    return 0;
  }
  needed = left->count + right->count;
  if (!lc_pouch_index_doc_id_set_reserve(allocator, dst, needed)) {
    return 0;
  }
  left_index = 0U;
  right_index = 0U;
  while (left_index < left->count && right_index < right->count) {
    lc_pouch_index_doc_id left_id;
    lc_pouch_index_doc_id right_id;

    left_id = left->items[left_index];
    right_id = right->items[right_index];
    if (left_id == right_id) {
      dst->items[dst->count++] = left_id;
      left_index++;
      right_index++;
    } else if (left_id < right_id) {
      dst->items[dst->count++] = left_id;
      left_index++;
    } else {
      dst->items[dst->count++] = right_id;
      right_index++;
    }
  }
  while (left_index < left->count) {
    dst->items[dst->count++] = left->items[left_index++];
  }
  while (right_index < right->count) {
    dst->items[dst->count++] = right->items[right_index++];
  }
  return 1;
}

int lc_pouch_index_doc_id_set_intersect(
    const lc_pouch_allocator *allocator, lc_pouch_index_doc_id_set *dst,
    const lc_pouch_index_doc_id_set *left,
    const lc_pouch_index_doc_id_set *right) {
  lc_pouch_index_doc_id_set tmp;
  size_t left_index;
  size_t right_index;
  size_t max_count;

  if (dst == NULL || left == NULL || right == NULL) {
    return 0;
  }
  if (dst == left || dst == right) {
    memset(&tmp, 0, sizeof(tmp));
    if (!lc_pouch_index_doc_id_set_intersect(allocator, &tmp, left, right)) {
      return 0;
    }
    lc_pouch_index_doc_id_set_cleanup(allocator, dst);
    *dst = tmp;
    return 1;
  }
  dst->count = 0U;
  max_count = left->count < right->count ? left->count : right->count;
  if (!lc_pouch_index_doc_id_set_reserve(allocator, dst, max_count)) {
    return 0;
  }
  left_index = 0U;
  right_index = 0U;
  while (left_index < left->count && right_index < right->count) {
    lc_pouch_index_doc_id left_id;
    lc_pouch_index_doc_id right_id;

    left_id = left->items[left_index];
    right_id = right->items[right_index];
    if (left_id == right_id) {
      dst->items[dst->count++] = left_id;
      left_index++;
      right_index++;
    } else if (left_id < right_id) {
      left_index++;
    } else {
      right_index++;
    }
  }
  return 1;
}

int lc_pouch_index_doc_id_set_subtract(const lc_pouch_allocator *allocator,
                                       lc_pouch_index_doc_id_set *dst,
                                       const lc_pouch_index_doc_id_set *left,
                                       const lc_pouch_index_doc_id_set *right) {
  lc_pouch_index_doc_id_set tmp;
  size_t left_index;
  size_t right_index;

  if (dst == NULL || left == NULL || right == NULL) {
    return 0;
  }
  if (dst == left || dst == right) {
    memset(&tmp, 0, sizeof(tmp));
    if (!lc_pouch_index_doc_id_set_subtract(allocator, &tmp, left, right)) {
      return 0;
    }
    lc_pouch_index_doc_id_set_cleanup(allocator, dst);
    *dst = tmp;
    return 1;
  }
  dst->count = 0U;
  if (!lc_pouch_index_doc_id_set_reserve(allocator, dst, left->count)) {
    return 0;
  }
  left_index = 0U;
  right_index = 0U;
  while (left_index < left->count) {
    lc_pouch_index_doc_id left_id;
    lc_pouch_index_doc_id right_id;

    if (right_index >= right->count) {
      dst->items[dst->count++] = left->items[left_index++];
      continue;
    }
    left_id = left->items[left_index];
    right_id = right->items[right_index];
    if (left_id == right_id) {
      left_index++;
      right_index++;
    } else if (left_id < right_id) {
      dst->items[dst->count++] = left_id;
      left_index++;
    } else {
      right_index++;
    }
  }
  return 1;
}

static size_t lc_pouch_index_uvarint_size(lc_pouch_index_doc_id value) {
  size_t size;

  size = 1U;
  while (value >= 0x80U) {
    value >>= 7U;
    size++;
  }
  return size;
}

static size_t
lc_pouch_index_estimate_sparse_size(const lc_pouch_index_doc_id_set *ids) {
  size_t index;
  size_t total;
  lc_pouch_index_doc_id previous;

  total = 0U;
  previous = 0U;
  for (index = 0U; index < ids->count; ++index) {
    lc_pouch_index_doc_id value;

    value = ids->items[index];
    if (index > 0U) {
      value -= previous;
    }
    total += lc_pouch_index_uvarint_size(value);
    previous = ids->items[index];
  }
  return total;
}

static size_t lc_pouch_index_put_uvarint(unsigned char *dst,
                                         lc_pouch_index_doc_id value) {
  size_t written;

  written = 0U;
  while (value >= 0x80U) {
    dst[written++] = (unsigned char)((value & 0x7fU) | 0x80U);
    value >>= 7U;
  }
  dst[written++] = (unsigned char)value;
  return written;
}

static int lc_pouch_index_read_uvarint(const unsigned char **cursor,
                                       const unsigned char *end,
                                       lc_pouch_index_doc_id *out) {
  const unsigned char *ptr;
  lc_pouch_index_doc_id value;
  unsigned int shift;

  if (cursor == NULL || *cursor == NULL || out == NULL) {
    return 0;
  }
  ptr = *cursor;
  value = 0U;
  shift = 0U;
  while (ptr < end && shift < 32U) {
    unsigned char byte;

    byte = *ptr++;
    value |= ((lc_pouch_index_doc_id)(byte & 0x7fU)) << shift;
    if ((byte & 0x80U) == 0U) {
      *cursor = ptr;
      *out = value;
      return 1;
    }
    shift += 7U;
  }
  return 0;
}

void lc_pouch_index_posting_cleanup(const lc_pouch_allocator *allocator,
                                    lc_pouch_index_posting *posting) {
  if (posting == NULL) {
    return;
  }
  lc_pouch_free(allocator, posting->sparse);
  lc_pouch_free(allocator, posting->dense);
  memset(posting, 0, sizeof(*posting));
}

static int
lc_pouch_index_posting_should_use_dense(const lc_pouch_index_doc_id_set *ids,
                                        size_t sparse_size,
                                        size_t dense_word_count) {
  size_t universe;
  size_t dense_size;
  double density;

  if (ids == NULL || ids->count == 0U || dense_word_count == 0U) {
    return 0;
  }
#if SIZE_MAX <= UINT32_MAX
  if (ids->items[ids->count - 1U] == UINT32_MAX) {
    return 0;
  }
#endif
  universe = (size_t)ids->items[ids->count - 1U] + 1U;
  density = (double)ids->count / (double)universe;
  if (density < ((double)LC_POUCH_INDEX_DENSE_THRESHOLD_NUM /
                 (double)LC_POUCH_INDEX_DENSE_THRESHOLD_DEN)) {
    return 0;
  }
  if (dense_word_count > ((size_t)-1) / sizeof(uint64_t)) {
    return 0;
  }
  dense_size = dense_word_count * sizeof(uint64_t);
  return dense_size <= sparse_size;
}

int lc_pouch_index_posting_build(const lc_pouch_allocator *allocator,
                                 lc_pouch_index_posting *posting,
                                 const lc_pouch_index_doc_id *ids,
                                 size_t count) {
  lc_pouch_index_doc_id_set sorted;
  size_t index;
  size_t sparse_size;
  size_t dense_word_count;
  int use_dense;

  if (posting == NULL || (ids == NULL && count > 0U)) {
    return 0;
  }
  lc_pouch_index_posting_cleanup(allocator, posting);
  if (count == 0U) {
    return 1;
  }
  memset(&sorted, 0, sizeof(sorted));
  if (!lc_pouch_index_doc_id_set_reserve(allocator, &sorted, count)) {
    return 0;
  }
  memcpy(sorted.items, ids, count * sizeof(ids[0]));
  sorted.count = count;
  lc_pouch_index_doc_id_set_sort_unique(&sorted);
  sparse_size = lc_pouch_index_estimate_sparse_size(&sorted);
  dense_word_count = ((size_t)sorted.items[sorted.count - 1U] / 64U) + 1U;
  use_dense = lc_pouch_index_posting_should_use_dense(&sorted, sparse_size,
                                                      dense_word_count);
  posting->count = sorted.count;
  posting->max_doc_id = sorted.items[sorted.count - 1U];
  if (use_dense) {
    posting->dense = (uint64_t *)lc_pouch_calloc(allocator, dense_word_count,
                                                 sizeof(posting->dense[0]));
    if (posting->dense == NULL) {
      lc_pouch_index_doc_id_set_cleanup(allocator, &sorted);
      lc_pouch_index_posting_cleanup(allocator, posting);
      return 0;
    }
    for (index = 0U; index < sorted.count; ++index) {
      lc_pouch_index_doc_id id;

      id = sorted.items[index];
      posting->dense[id / 64U] |= ((uint64_t)1U) << (id % 64U);
    }
    posting->dense_word_count = dense_word_count;
    posting->encoding = LC_POUCH_INDEX_POSTING_DENSE;
  } else {
    unsigned char *cursor;
    lc_pouch_index_doc_id previous;

    posting->sparse = (unsigned char *)lc_pouch_alloc(allocator, sparse_size);
    if (posting->sparse == NULL) {
      lc_pouch_index_doc_id_set_cleanup(allocator, &sorted);
      lc_pouch_index_posting_cleanup(allocator, posting);
      return 0;
    }
    cursor = posting->sparse;
    previous = 0U;
    for (index = 0U; index < sorted.count; ++index) {
      lc_pouch_index_doc_id delta;

      delta = sorted.items[index];
      if (index > 0U) {
        delta -= previous;
      }
      cursor += lc_pouch_index_put_uvarint(cursor, delta);
      previous = sorted.items[index];
    }
    posting->sparse_len = sparse_size;
    posting->encoding = LC_POUCH_INDEX_POSTING_SPARSE;
  }
  lc_pouch_index_doc_id_set_cleanup(allocator, &sorted);
  return 1;
}

int lc_pouch_index_posting_decode(const lc_pouch_allocator *allocator,
                                  const lc_pouch_index_posting *posting,
                                  lc_pouch_index_doc_id_set *dst) {
  size_t index;

  if (posting == NULL || dst == NULL) {
    return 0;
  }
  dst->count = 0U;
  if (posting->count == 0U ||
      posting->encoding == LC_POUCH_INDEX_POSTING_EMPTY) {
    return 1;
  }
  if (!lc_pouch_index_doc_id_set_reserve(allocator, dst, posting->count)) {
    return 0;
  }
  if (posting->encoding == LC_POUCH_INDEX_POSTING_DENSE) {
    for (index = 0U; index < posting->dense_word_count; ++index) {
      uint64_t word;
      unsigned int bit;

      word = posting->dense[index];
      for (bit = 0U; bit < 64U; ++bit) {
        lc_pouch_index_doc_id id;

        if ((word & (((uint64_t)1U) << bit)) == 0U) {
          continue;
        }
        id = (lc_pouch_index_doc_id)(index * 64U + bit);
        if (id > posting->max_doc_id) {
          break;
        }
        dst->items[dst->count++] = id;
      }
    }
    return dst->count == posting->count;
  }
  if (posting->encoding == LC_POUCH_INDEX_POSTING_SPARSE) {
    const unsigned char *cursor;
    const unsigned char *end;
    lc_pouch_index_doc_id current;

    cursor = posting->sparse;
    end = posting->sparse + posting->sparse_len;
    current = 0U;
    for (index = 0U; index < posting->count; ++index) {
      lc_pouch_index_doc_id delta;

      if (!lc_pouch_index_read_uvarint(&cursor, end, &delta)) {
        return 0;
      }
      if (index == 0U) {
        current = delta;
      } else {
        current += delta;
      }
      dst->items[dst->count++] = current;
    }
    return cursor == end;
  }
  return 0;
}

int lc_pouch_index_posting_intersect(const lc_pouch_allocator *allocator,
                                     const lc_pouch_index_posting *posting,
                                     const lc_pouch_index_doc_id_set *filter,
                                     lc_pouch_index_doc_id_set *dst) {
  lc_pouch_index_doc_id_set decoded;
  int ok;

  if (posting == NULL || filter == NULL || dst == NULL) {
    return 0;
  }
  memset(&decoded, 0, sizeof(decoded));
  if (!lc_pouch_index_posting_decode(allocator, posting, &decoded)) {
    lc_pouch_index_doc_id_set_cleanup(allocator, &decoded);
    return 0;
  }
  ok = lc_pouch_index_doc_id_set_intersect(allocator, dst, &decoded, filter);
  lc_pouch_index_doc_id_set_cleanup(allocator, &decoded);
  return ok;
}

int lc_pouch_index_collect_eq_term_doc_ids(
    const lc_pouch_allocator *allocator, const lc_pouch_document_eq_term *term,
    lc_pouch_index_exact_term_doc_ids_fn read_exact, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error) {
  int rc;

  if (term == NULL || term->field == NULL || term->value == NULL ||
      read_exact == NULL || doc_ids == NULL) {
    return LC_OK;
  }
  rc = read_exact(read_context, term->field, term->value, doc_ids, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!lc_pouch_index_doc_id_set_sort_unique(doc_ids)) {
    return LC_ERR_NOMEM;
  }
  (void)allocator;
  (void)error;
  return LC_OK;
}

int lc_pouch_index_collect_in_term_doc_ids(
    const lc_pouch_allocator *allocator, const lc_pouch_document_in_term *term,
    lc_pouch_index_exact_term_doc_ids_fn read_exact, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error) {
  size_t value_index;
  int rc;

  if (term == NULL || term->field == NULL || term->values == NULL ||
      term->value_count == 0U || read_exact == NULL || doc_ids == NULL) {
    return LC_OK;
  }
  for (value_index = 0U; value_index < term->value_count; ++value_index) {
    if (term->values[value_index] == NULL) {
      continue;
    }
    rc = read_exact(read_context, term->field, term->values[value_index],
                    doc_ids, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  if (!lc_pouch_index_doc_id_set_sort_unique(doc_ids)) {
    return LC_ERR_NOMEM;
  }
  (void)allocator;
  (void)error;
  return LC_OK;
}

int lc_pouch_index_collect_exists_term_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_exists_term *term,
    lc_pouch_index_exists_term_doc_ids_fn read_exists, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error) {
  int rc;

  if (term == NULL || term->field == NULL || read_exists == NULL ||
      doc_ids == NULL) {
    return LC_OK;
  }
  rc = read_exists(read_context, term->field, doc_ids, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!lc_pouch_index_doc_id_set_sort_unique(doc_ids)) {
    return LC_ERR_NOMEM;
  }
  (void)allocator;
  (void)error;
  return LC_OK;
}

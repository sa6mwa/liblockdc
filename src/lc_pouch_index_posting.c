#include "lc_pouch_index.h"

#include <limits.h>
#include <stddef.h>
#include <string.h>

#define LC_POUCH_INDEX_DENSE_THRESHOLD_NUM 18U
#define LC_POUCH_INDEX_DENSE_THRESHOLD_DEN 100U

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
  for (index = 0U; index < count; ++index) {
    if (!lc_pouch_index_doc_id_set_append(allocator, &sorted, ids[index])) {
      lc_pouch_index_doc_id_set_cleanup(allocator, &sorted);
      return 0;
    }
  }
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
  if (dst == NULL) {
    return 0;
  }
  dst->count = 0U;
  return lc_pouch_index_posting_append(allocator, posting, dst);
}

int lc_pouch_index_posting_append(const lc_pouch_allocator *allocator,
                                  const lc_pouch_index_posting *posting,
                                  lc_pouch_index_doc_id_set *dst) {
  size_t original_count;
  size_t index;

  if (posting == NULL || dst == NULL) {
    return 0;
  }
  original_count = dst->count;
  if (original_count > ((size_t)-1) - posting->count) {
    return 0;
  }
  if (posting->count == 0U ||
      posting->encoding == LC_POUCH_INDEX_POSTING_EMPTY) {
    return 1;
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
        if (!lc_pouch_index_doc_id_set_append(allocator, dst, id)) {
          return 0;
        }
      }
    }
    return dst->count == original_count + posting->count;
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
      if (!lc_pouch_index_doc_id_set_append(allocator, dst, current)) {
        return 0;
      }
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

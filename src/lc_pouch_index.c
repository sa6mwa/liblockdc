#include "lc_pouch_index.h"

#include <limits.h>
#include <stdio.h>
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
lc_pouch_index_doc_table_compare_entry(const lc_pouch_index_doc_entry *entry,
                                       const char *namespace_name,
                                       const char *key) {
  int cmp;

  cmp = strcmp(entry->namespace_name, namespace_name);
  if (cmp != 0) {
    return cmp;
  }
  return strcmp(entry->key, key);
}

static int
lc_pouch_index_doc_table_search(const lc_pouch_index_doc_table *table,
                                const char *namespace_name, const char *key,
                                size_t *position_out) {
  size_t low;
  size_t high;

  if (table == NULL || namespace_name == NULL || key == NULL) {
    if (position_out != NULL) {
      *position_out = 0U;
    }
    return 0;
  }
  low = 0U;
  high = table->count;
  while (low < high) {
    size_t mid;
    int cmp;

    mid = low + ((high - low) / 2U);
    cmp = lc_pouch_index_doc_table_compare_entry(&table->entries[mid],
                                                 namespace_name, key);
    if (cmp < 0) {
      low = mid + 1U;
    } else {
      high = mid;
    }
  }
  if (position_out != NULL) {
    *position_out = low;
  }
  return low < table->count &&
         lc_pouch_index_doc_table_compare_entry(&table->entries[low],
                                                namespace_name, key) == 0;
}

static int lc_pouch_index_doc_table_reserve(const lc_pouch_allocator *allocator,
                                            lc_pouch_index_doc_table *table,
                                            size_t needed) {
  size_t new_capacity;
  lc_pouch_index_doc_entry *grown;

  if (table == NULL) {
    return 0;
  }
  if (needed <= table->capacity) {
    return 1;
  }
  new_capacity = table->capacity == 0U ? 16U : table->capacity;
  while (new_capacity < needed) {
    if (new_capacity > ((size_t)-1) / 2U) {
      return 0;
    }
    new_capacity *= 2U;
  }
  if (new_capacity > ((size_t)-1) / sizeof(table->entries[0])) {
    return 0;
  }
  grown = (lc_pouch_index_doc_entry *)lc_pouch_realloc(
      allocator, table->entries, new_capacity * sizeof(table->entries[0]));
  if (grown == NULL) {
    return 0;
  }
  memset(grown + table->capacity, 0,
         (new_capacity - table->capacity) * sizeof(grown[0]));
  table->entries = grown;
  table->capacity = new_capacity;
  return 1;
}

static void
lc_pouch_index_doc_table_assign_ids_from(lc_pouch_index_doc_table *table,
                                         size_t first) {
  size_t index;

  if (table == NULL) {
    return;
  }
  for (index = first; index < table->count; ++index) {
    table->entries[index].id = (lc_pouch_index_doc_id)index;
  }
}

void lc_pouch_index_doc_table_cleanup(const lc_pouch_allocator *allocator,
                                      lc_pouch_index_doc_table *table) {
  size_t index;

  if (table == NULL) {
    return;
  }
  for (index = 0U; index < table->count; ++index) {
    lc_pouch_free(allocator, table->entries[index].namespace_name);
    lc_pouch_free(allocator, table->entries[index].key);
  }
  lc_pouch_free(allocator, table->entries);
  memset(table, 0, sizeof(*table));
}

int lc_pouch_index_doc_table_find(const lc_pouch_index_doc_table *table,
                                  const char *namespace_name, const char *key,
                                  lc_pouch_index_doc_id *id_out) {
  size_t position;

  if (!lc_pouch_index_doc_table_search(table, namespace_name, key, &position)) {
    return 0;
  }
  if (id_out != NULL) {
    *id_out = table->entries[position].id;
  }
  return 1;
}

int lc_pouch_index_doc_table_find_or_add(const lc_pouch_allocator *allocator,
                                         lc_pouch_index_doc_table *table,
                                         const char *namespace_name,
                                         const char *key,
                                         lc_pouch_index_doc_id *id_out) {
  lc_pouch_index_doc_entry entry;
  size_t position;

  if (table == NULL || namespace_name == NULL || key == NULL ||
      table->count > (size_t)UINT32_MAX) {
    return 0;
  }
  if (lc_pouch_index_doc_table_search(table, namespace_name, key, &position)) {
    if (id_out != NULL) {
      *id_out = table->entries[position].id;
    }
    return 1;
  }
  memset(&entry, 0, sizeof(entry));
  entry.namespace_name = lc_pouch_strdup(allocator, namespace_name);
  entry.key = lc_pouch_strdup(allocator, key);
  if (entry.namespace_name == NULL || entry.key == NULL) {
    lc_pouch_free(allocator, entry.namespace_name);
    lc_pouch_free(allocator, entry.key);
    return 0;
  }
  if (!lc_pouch_index_doc_table_reserve(allocator, table, table->count + 1U)) {
    lc_pouch_free(allocator, entry.namespace_name);
    lc_pouch_free(allocator, entry.key);
    return 0;
  }
  if (position < table->count) {
    memmove(table->entries + position + 1U, table->entries + position,
            (table->count - position) * sizeof(table->entries[0]));
  }
  table->entries[position] = entry;
  table->count++;
  lc_pouch_index_doc_table_assign_ids_from(table, position);
  if (id_out != NULL) {
    *id_out = table->entries[position].id;
  }
  return 1;
}

int lc_pouch_index_doc_table_lookup(const lc_pouch_index_doc_table *table,
                                    lc_pouch_index_doc_id id,
                                    const char **namespace_name_out,
                                    const char **key_out) {
  if (table == NULL || (size_t)id >= table->count) {
    return 0;
  }
  if (namespace_name_out != NULL) {
    *namespace_name_out = table->entries[id].namespace_name;
  }
  if (key_out != NULL) {
    *key_out = table->entries[id].key;
  }
  return 1;
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

static int lc_pouch_index_term_compare(const char *left_field,
                                       const char *left_value,
                                       const char *right_field,
                                       const char *right_value) {
  int cmp;

  cmp = strcmp(left_field, right_field);
  if (cmp != 0) {
    return cmp;
  }
  return strcmp(left_value, right_value);
}

static int
lc_pouch_index_term_table_find_position(const lc_pouch_index_term_table *table,
                                        const char *field, const char *value,
                                        size_t *position_out) {
  size_t low;
  size_t high;

  if (position_out != NULL) {
    *position_out = 0U;
  }
  if (table == NULL || field == NULL || value == NULL) {
    return 0;
  }
  low = 0U;
  high = table->count;
  while (low < high) {
    size_t mid;
    int cmp;

    mid = low + ((high - low) / 2U);
    cmp = lc_pouch_index_term_compare(table->entries[mid].field,
                                      table->entries[mid].value, field, value);
    if (cmp < 0) {
      low = mid + 1U;
    } else {
      high = mid;
    }
  }
  if (position_out != NULL) {
    *position_out = low;
  }
  return low < table->count &&
         lc_pouch_index_term_compare(table->entries[low].field,
                                     table->entries[low].value, field,
                                     value) == 0;
}

static int
lc_pouch_index_term_table_reserve(const lc_pouch_allocator *allocator,
                                  lc_pouch_index_term_table *table,
                                  size_t needed) {
  size_t new_capacity;
  lc_pouch_index_term_entry *grown;

  if (table == NULL) {
    return 0;
  }
  if (needed <= table->capacity) {
    return 1;
  }
  new_capacity = table->capacity == 0U ? 16U : table->capacity;
  while (new_capacity < needed) {
    if (new_capacity > ((size_t)-1) / 2U) {
      return 0;
    }
    new_capacity *= 2U;
  }
  if (new_capacity > ((size_t)-1) / sizeof(table->entries[0])) {
    return 0;
  }
  grown = (lc_pouch_index_term_entry *)lc_pouch_realloc(
      allocator, table->entries, new_capacity * sizeof(table->entries[0]));
  if (grown == NULL) {
    return 0;
  }
  table->entries = grown;
  table->capacity = new_capacity;
  return 1;
}

void lc_pouch_index_term_table_cleanup(const lc_pouch_allocator *allocator,
                                       lc_pouch_index_term_table *table) {
  size_t index;

  if (table == NULL) {
    return;
  }
  for (index = 0U; index < table->count; ++index) {
    lc_pouch_free(allocator, table->entries[index].field);
    lc_pouch_free(allocator, table->entries[index].value);
  }
  lc_pouch_free(allocator, table->entries);
  memset(table, 0, sizeof(*table));
}

int lc_pouch_index_term_table_find(const lc_pouch_index_term_table *table,
                                   const char *field, const char *value,
                                   lc_pouch_index_term_id *id_out) {
  size_t position;

  if (!lc_pouch_index_term_table_find_position(table, field, value,
                                               &position)) {
    return 0;
  }
  if (id_out != NULL) {
    *id_out = table->entries[position].id;
  }
  return 1;
}

int lc_pouch_index_term_table_find_or_add(const lc_pouch_allocator *allocator,
                                          lc_pouch_index_term_table *table,
                                          const char *field, const char *value,
                                          lc_pouch_index_term_id *id_out) {
  lc_pouch_index_term_entry entry;
  size_t position;

  if (table == NULL || field == NULL || value == NULL) {
    return 0;
  }
  if (lc_pouch_index_term_table_find_position(table, field, value, &position)) {
    if (id_out != NULL) {
      *id_out = table->entries[position].id;
    }
    return 1;
  }
  if (table->next_id == UINT32_MAX) {
    return 0;
  }
  memset(&entry, 0, sizeof(entry));
  entry.field = lc_pouch_strdup(allocator, field);
  entry.value = lc_pouch_strdup(allocator, value);
  if (entry.field == NULL || entry.value == NULL) {
    lc_pouch_free(allocator, entry.field);
    lc_pouch_free(allocator, entry.value);
    return 0;
  }
  entry.id = table->next_id;
  if (!lc_pouch_index_term_table_reserve(allocator, table, table->count + 1U)) {
    lc_pouch_free(allocator, entry.field);
    lc_pouch_free(allocator, entry.value);
    return 0;
  }
  if (position < table->count) {
    memmove(&table->entries[position + 1U], &table->entries[position],
            (table->count - position) * sizeof(table->entries[0]));
  }
  table->entries[position] = entry;
  table->count++;
  table->next_id++;
  if (id_out != NULL) {
    *id_out = entry.id;
  }
  return 1;
}

static int lc_pouch_index_term_posting_table_find_position(
    const lc_pouch_index_term_posting_table *table,
    lc_pouch_index_term_id term_id, size_t *position_out) {
  size_t low;
  size_t high;

  if (position_out != NULL) {
    *position_out = 0U;
  }
  if (table == NULL) {
    return 0;
  }
  low = 0U;
  high = table->count;
  while (low < high) {
    size_t mid;

    mid = low + ((high - low) / 2U);
    if (table->entries[mid].term_id < term_id) {
      low = mid + 1U;
    } else {
      high = mid;
    }
  }
  if (position_out != NULL) {
    *position_out = low;
  }
  return low < table->count && table->entries[low].term_id == term_id;
}

static int lc_pouch_index_term_posting_table_reserve(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_term_posting_table *table, size_t needed) {
  size_t new_capacity;
  lc_pouch_index_term_posting_entry *grown;

  if (table == NULL) {
    return 0;
  }
  if (needed <= table->capacity) {
    return 1;
  }
  new_capacity = table->capacity == 0U ? 16U : table->capacity;
  while (new_capacity < needed) {
    if (new_capacity > ((size_t)-1) / 2U) {
      return 0;
    }
    new_capacity *= 2U;
  }
  if (new_capacity > ((size_t)-1) / sizeof(table->entries[0])) {
    return 0;
  }
  grown = (lc_pouch_index_term_posting_entry *)lc_pouch_realloc(
      allocator, table->entries, new_capacity * sizeof(table->entries[0]));
  if (grown == NULL) {
    return 0;
  }
  table->entries = grown;
  table->capacity = new_capacity;
  return 1;
}

void lc_pouch_index_term_posting_table_cleanup(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_term_posting_table *table) {
  size_t index;

  if (table == NULL) {
    return;
  }
  for (index = 0U; index < table->count; ++index) {
    lc_pouch_index_posting_cleanup(allocator, &table->entries[index].posting);
  }
  lc_pouch_free(allocator, table->entries);
  memset(table, 0, sizeof(*table));
}

int lc_pouch_index_term_posting_table_put(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_term_posting_table *table, lc_pouch_index_term_id term_id,
    const lc_pouch_index_doc_id *ids, size_t count) {
  lc_pouch_index_posting posting;
  size_t position;

  if (table == NULL || (ids == NULL && count > 0U)) {
    return 0;
  }
  memset(&posting, 0, sizeof(posting));
  if (!lc_pouch_index_posting_build(allocator, &posting, ids, count)) {
    return 0;
  }
  if (lc_pouch_index_term_posting_table_find_position(table, term_id,
                                                      &position)) {
    lc_pouch_index_posting_cleanup(allocator,
                                   &table->entries[position].posting);
    table->entries[position].posting = posting;
    return 1;
  }
  if (!lc_pouch_index_term_posting_table_reserve(allocator, table,
                                                 table->count + 1U)) {
    lc_pouch_index_posting_cleanup(allocator, &posting);
    return 0;
  }
  if (position < table->count) {
    memmove(&table->entries[position + 1U], &table->entries[position],
            (table->count - position) * sizeof(table->entries[0]));
  }
  table->entries[position].term_id = term_id;
  table->entries[position].posting = posting;
  table->count++;
  return 1;
}

int lc_pouch_index_term_posting_table_contains(
    const lc_pouch_index_term_posting_table *table,
    lc_pouch_index_term_id term_id) {
  return lc_pouch_index_term_posting_table_find_position(table, term_id, NULL);
}

int lc_pouch_index_term_posting_table_decode(
    const lc_pouch_allocator *allocator,
    const lc_pouch_index_term_posting_table *table,
    lc_pouch_index_term_id term_id, lc_pouch_index_doc_id_set *dst) {
  size_t position;

  if (dst == NULL) {
    return 0;
  }
  dst->count = 0U;
  if (!lc_pouch_index_term_posting_table_find_position(table, term_id,
                                                       &position)) {
    return 1;
  }
  return lc_pouch_index_posting_decode(allocator,
                                       &table->entries[position].posting, dst);
}

void lc_pouch_index_prepared_term_cache_cleanup(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_prepared_term_cache *cache) {
  if (cache == NULL) {
    return;
  }
  lc_pouch_index_term_posting_table_cleanup(allocator, &cache->postings);
  lc_pouch_index_term_table_cleanup(allocator, &cache->terms);
  cache->generation = 0U;
}

void lc_pouch_index_prepared_term_cache_refresh(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_prepared_term_cache *cache, uint64_t generation) {
  if (cache == NULL || cache->generation == generation) {
    return;
  }
  lc_pouch_index_prepared_term_cache_cleanup(allocator, cache);
  cache->generation = generation;
}

static int lc_pouch_index_result_plan_has_no_eq_filters(
    const lc_pouch_query_index_scan_req *req) {
  return req->document_eq_term_count == 0U &&
         req->document_not_eq_term_count == 0U &&
         req->document_or_eq_term_count == 0U;
}

static int lc_pouch_index_result_plan_has_no_range_filters(
    const lc_pouch_query_index_scan_req *req) {
  return req->document_range_term_count == 0U &&
         req->document_not_range_term_count == 0U &&
         req->document_or_range_term_count == 0U;
}

static int lc_pouch_index_result_plan_has_no_in_filters(
    const lc_pouch_query_index_scan_req *req) {
  return req->document_in_term_count == 0U &&
         req->document_not_in_term_count == 0U &&
         req->document_or_in_term_count == 0U;
}

static int lc_pouch_index_result_plan_has_no_prefix_filters(
    const lc_pouch_query_index_scan_req *req) {
  return req->document_prefix_term_count == 0U &&
         req->document_not_prefix_term_count == 0U &&
         req->document_or_prefix_term_count == 0U;
}

static int lc_pouch_index_result_plan_has_no_contains_filters(
    const lc_pouch_query_index_scan_req *req) {
  return req->document_contains_term_count == 0U &&
         req->document_not_contains_term_count == 0U &&
         req->document_or_contains_term_count == 0U;
}

static int lc_pouch_index_result_plan_has_no_exists_filters(
    const lc_pouch_query_index_scan_req *req) {
  return req->document_exists_term_count == 0U &&
         req->document_not_exists_term_count == 0U &&
         req->document_or_exists_term_count == 0U &&
         req->document_exists_path_pattern_count == 0U &&
         req->document_or_exists_path_pattern_count == 0U;
}

static int lc_pouch_index_result_plan_cacheable_common(
    const lc_pouch_query_index_scan_req *req) {
  return req != NULL && req->namespace_name != NULL && req->key == NULL &&
         req->owner == NULL;
}

static int lc_pouch_index_result_plan_eq_cacheable(
    const lc_pouch_query_index_scan_req *req) {
  return lc_pouch_index_result_plan_cacheable_common(req) &&
         req->document_eq_terms != NULL && req->document_eq_term_count == 1U &&
         req->document_eq_terms[0].field != NULL &&
         req->document_eq_terms[0].value != NULL &&
         req->document_not_eq_term_count == 0U &&
         req->document_or_eq_term_count == 0U &&
         lc_pouch_index_result_plan_has_no_range_filters(req) &&
         lc_pouch_index_result_plan_has_no_in_filters(req) &&
         lc_pouch_index_result_plan_has_no_prefix_filters(req) &&
         lc_pouch_index_result_plan_has_no_contains_filters(req) &&
         lc_pouch_index_result_plan_has_no_exists_filters(req);
}

static int lc_pouch_index_result_plan_exists_cacheable(
    const lc_pouch_query_index_scan_req *req) {
  return lc_pouch_index_result_plan_cacheable_common(req) &&
         req->document_exists_terms != NULL &&
         req->document_exists_term_count == 1U &&
         req->document_exists_terms[0].field != NULL &&
         lc_pouch_index_result_plan_has_no_eq_filters(req) &&
         lc_pouch_index_result_plan_has_no_range_filters(req) &&
         lc_pouch_index_result_plan_has_no_in_filters(req) &&
         lc_pouch_index_result_plan_has_no_prefix_filters(req) &&
         lc_pouch_index_result_plan_has_no_contains_filters(req) &&
         req->document_not_exists_term_count == 0U &&
         req->document_or_exists_term_count == 0U &&
         req->document_exists_path_pattern_count == 0U &&
         req->document_or_exists_path_pattern_count == 0U;
}

static int lc_pouch_index_result_plan_in_cacheable(
    const lc_pouch_query_index_scan_req *req) {
  return lc_pouch_index_result_plan_cacheable_common(req) &&
         req->document_in_terms != NULL && req->document_in_term_count == 1U &&
         req->document_in_terms[0].field != NULL &&
         req->document_in_terms[0].values != NULL &&
         req->document_in_terms[0].value_count > 0U &&
         strchr(req->document_in_terms[0].field, '*') == NULL &&
         lc_pouch_index_result_plan_has_no_eq_filters(req) &&
         lc_pouch_index_result_plan_has_no_range_filters(req) &&
         req->document_not_in_term_count == 0U &&
         req->document_or_in_term_count == 0U &&
         lc_pouch_index_result_plan_has_no_prefix_filters(req) &&
         lc_pouch_index_result_plan_has_no_contains_filters(req) &&
         lc_pouch_index_result_plan_has_no_exists_filters(req);
}

static int lc_pouch_index_result_plan_range_cacheable(
    const lc_pouch_query_index_scan_req *req) {
  const lc_pouch_document_range_term *term;

  if (!lc_pouch_index_result_plan_cacheable_common(req) ||
      req->document_range_terms == NULL ||
      req->document_range_term_count != 1U ||
      req->document_range_terms[0].field == NULL) {
    return 0;
  }
  term = &req->document_range_terms[0];
  return (term->gt != NULL || term->gte != NULL || term->lt != NULL ||
          term->lte != NULL) &&
         lc_pouch_index_result_plan_has_no_eq_filters(req) &&
         req->document_not_range_term_count == 0U &&
         req->document_or_range_term_count == 0U &&
         lc_pouch_index_result_plan_has_no_in_filters(req) &&
         lc_pouch_index_result_plan_has_no_prefix_filters(req) &&
         lc_pouch_index_result_plan_has_no_contains_filters(req) &&
         lc_pouch_index_result_plan_has_no_exists_filters(req);
}

static int lc_pouch_index_result_plan_prefix_cacheable(
    const lc_pouch_query_index_scan_req *req) {
  return lc_pouch_index_result_plan_cacheable_common(req) &&
         req->document_prefix_terms != NULL &&
         req->document_prefix_term_count == 1U &&
         req->document_prefix_terms[0].field != NULL &&
         req->document_prefix_terms[0].value != NULL &&
         lc_pouch_index_result_plan_has_no_eq_filters(req) &&
         lc_pouch_index_result_plan_has_no_range_filters(req) &&
         lc_pouch_index_result_plan_has_no_in_filters(req) &&
         req->document_not_prefix_term_count == 0U &&
         req->document_or_prefix_term_count == 0U &&
         lc_pouch_index_result_plan_has_no_contains_filters(req) &&
         lc_pouch_index_result_plan_has_no_exists_filters(req);
}

static int lc_pouch_index_result_plan_contains_cacheable(
    const lc_pouch_query_index_scan_req *req) {
  return lc_pouch_index_result_plan_cacheable_common(req) &&
         req->document_contains_terms != NULL &&
         req->document_contains_term_count == 1U &&
         req->document_contains_terms[0].field != NULL &&
         req->document_contains_terms[0].value != NULL &&
         lc_pouch_index_result_plan_has_no_eq_filters(req) &&
         lc_pouch_index_result_plan_has_no_range_filters(req) &&
         lc_pouch_index_result_plan_has_no_in_filters(req) &&
         lc_pouch_index_result_plan_has_no_prefix_filters(req) &&
         req->document_not_contains_term_count == 0U &&
         req->document_or_contains_term_count == 0U &&
         lc_pouch_index_result_plan_has_no_exists_filters(req);
}

static char *
lc_pouch_index_eq_result_plan_key(const lc_pouch_allocator *allocator,
                                  const lc_pouch_query_index_scan_req *req) {
  const lc_pouch_document_eq_term *term;
  size_t namespace_len;
  size_t field_len;
  size_t value_len;
  int written;
  size_t needed;
  char *key;

  if (!lc_pouch_index_result_plan_eq_cacheable(req)) {
    return NULL;
  }
  term = &req->document_eq_terms[0];
  namespace_len = strlen(req->namespace_name);
  field_len = strlen(term->field);
  value_len = strlen(term->value);
  written =
      snprintf(NULL, 0, "eq:%lu:%s:%lu:%s:%lu:%s", (unsigned long)namespace_len,
               req->namespace_name, (unsigned long)field_len, term->field,
               (unsigned long)value_len, term->value);
  if (written < 0) {
    return NULL;
  }
  needed = (size_t)written + 1U;
  key = (char *)lc_pouch_alloc(allocator, needed);
  if (key == NULL) {
    return NULL;
  }
  (void)snprintf(key, needed, "eq:%lu:%s:%lu:%s:%lu:%s",
                 (unsigned long)namespace_len, req->namespace_name,
                 (unsigned long)field_len, term->field,
                 (unsigned long)value_len, term->value);
  return key;
}

static char *lc_pouch_index_exists_result_plan_key(
    const lc_pouch_allocator *allocator,
    const lc_pouch_query_index_scan_req *req) {
  const lc_pouch_document_exists_term *term;
  size_t namespace_len;
  size_t field_len;
  int written;
  size_t needed;
  char *key;

  if (!lc_pouch_index_result_plan_exists_cacheable(req)) {
    return NULL;
  }
  term = &req->document_exists_terms[0];
  namespace_len = strlen(req->namespace_name);
  field_len = strlen(term->field);
  written =
      snprintf(NULL, 0, "exists:%lu:%s:%lu:%s", (unsigned long)namespace_len,
               req->namespace_name, (unsigned long)field_len, term->field);
  if (written < 0) {
    return NULL;
  }
  needed = (size_t)written + 1U;
  key = (char *)lc_pouch_alloc(allocator, needed);
  if (key == NULL) {
    return NULL;
  }
  (void)snprintf(key, needed, "exists:%lu:%s:%lu:%s",
                 (unsigned long)namespace_len, req->namespace_name,
                 (unsigned long)field_len, term->field);
  return key;
}

static int lc_pouch_index_in_value_ptr_compare(const void *left,
                                               const void *right) {
  const char *left_value;
  const char *right_value;

  left_value = *(const char *const *)left;
  right_value = *(const char *const *)right;
  return strcmp(left_value, right_value);
}

static char *
lc_pouch_index_in_result_plan_key(const lc_pouch_allocator *allocator,
                                  const lc_pouch_query_index_scan_req *req) {
  const lc_pouch_document_in_term *term;
  const char **values;
  size_t namespace_len;
  size_t field_len;
  size_t value_count;
  size_t value_index;
  size_t unique_count;
  size_t total_len;
  int written;
  char *key;
  char *cursor;
  size_t offset;

  if (!lc_pouch_index_result_plan_in_cacheable(req)) {
    return NULL;
  }
  term = &req->document_in_terms[0];
  value_count = 0U;
  for (value_index = 0U; value_index < term->value_count; ++value_index) {
    if (term->values[value_index] != NULL) {
      value_count++;
    }
  }
  if (value_count == 0U || value_count > ((size_t)-1) / sizeof(values[0])) {
    return NULL;
  }
  values =
      (const char **)lc_pouch_alloc(allocator, value_count * sizeof(values[0]));
  if (values == NULL) {
    return NULL;
  }
  value_count = 0U;
  for (value_index = 0U; value_index < term->value_count; ++value_index) {
    if (term->values[value_index] != NULL) {
      values[value_count++] = term->values[value_index];
    }
  }
  qsort(values, value_count, sizeof(values[0]),
        lc_pouch_index_in_value_ptr_compare);
  unique_count = 0U;
  for (value_index = 0U; value_index < value_count; ++value_index) {
    if (unique_count > 0U &&
        strcmp(values[value_index], values[unique_count - 1U]) == 0) {
      continue;
    }
    values[unique_count++] = values[value_index];
  }
  namespace_len = strlen(req->namespace_name);
  field_len = strlen(term->field);
  written =
      snprintf(NULL, 0, "in:%lu:%s:%lu:%s:%lu", (unsigned long)namespace_len,
               req->namespace_name, (unsigned long)field_len, term->field,
               (unsigned long)unique_count);
  if (written < 0) {
    lc_pouch_free(allocator, values);
    return NULL;
  }
  total_len = (size_t)written;
  for (value_index = 0U; value_index < unique_count; ++value_index) {
    size_t value_len;

    value_len = strlen(values[value_index]);
    written = snprintf(NULL, 0, ":%lu:%s", (unsigned long)value_len,
                       values[value_index]);
    if (written < 0 || total_len > ((size_t)-1) - (size_t)written) {
      lc_pouch_free(allocator, values);
      return NULL;
    }
    total_len += (size_t)written;
  }
  if (total_len == (size_t)-1) {
    lc_pouch_free(allocator, values);
    return NULL;
  }
  key = (char *)lc_pouch_alloc(allocator, total_len + 1U);
  if (key == NULL) {
    lc_pouch_free(allocator, values);
    return NULL;
  }
  cursor = key;
  offset = 0U;
  written = snprintf(cursor, (total_len + 1U) - offset, "in:%lu:%s:%lu:%s:%lu",
                     (unsigned long)namespace_len, req->namespace_name,
                     (unsigned long)field_len, term->field,
                     (unsigned long)unique_count);
  offset += (size_t)written;
  cursor = key + offset;
  for (value_index = 0U; value_index < unique_count; ++value_index) {
    size_t value_len;

    value_len = strlen(values[value_index]);
    written = snprintf(cursor, (total_len + 1U) - offset, ":%lu:%s",
                       (unsigned long)value_len, values[value_index]);
    offset += (size_t)written;
    cursor = key + offset;
  }
  lc_pouch_free(allocator, values);
  return key;
}

static char *
lc_pouch_index_range_result_plan_key(const lc_pouch_allocator *allocator,
                                     const lc_pouch_query_index_scan_req *req) {
  const lc_pouch_document_range_term *term;
  const char *gt;
  const char *gte;
  const char *lt;
  const char *lte;
  size_t namespace_len;
  size_t field_len;
  size_t gt_len;
  size_t gte_len;
  size_t lt_len;
  size_t lte_len;
  int written;
  size_t needed;
  char *key;

  if (!lc_pouch_index_result_plan_range_cacheable(req)) {
    return NULL;
  }
  term = &req->document_range_terms[0];
  gt = term->gt != NULL ? term->gt : "";
  gte = term->gte != NULL ? term->gte : "";
  lt = term->lt != NULL ? term->lt : "";
  lte = term->lte != NULL ? term->lte : "";
  namespace_len = strlen(req->namespace_name);
  field_len = strlen(term->field);
  gt_len = strlen(gt);
  gte_len = strlen(gte);
  lt_len = strlen(lt);
  lte_len = strlen(lte);
  written = snprintf(NULL, 0,
                     "range:%lu:%s:%lu:%s:%d:%lu:%s:%d:%lu:%s:%d:%lu:%s:%d:%"
                     "lu:%s",
                     (unsigned long)namespace_len, req->namespace_name,
                     (unsigned long)field_len, term->field,
                     term->gt != NULL ? 1 : 0, (unsigned long)gt_len, gt,
                     term->gte != NULL ? 1 : 0, (unsigned long)gte_len, gte,
                     term->lt != NULL ? 1 : 0, (unsigned long)lt_len, lt,
                     term->lte != NULL ? 1 : 0, (unsigned long)lte_len, lte);
  if (written < 0) {
    return NULL;
  }
  needed = (size_t)written + 1U;
  key = (char *)lc_pouch_alloc(allocator, needed);
  if (key == NULL) {
    return NULL;
  }
  (void)snprintf(key, needed,
                 "range:%lu:%s:%lu:%s:%d:%lu:%s:%d:%lu:%s:%d:%lu:%s:%d:%lu:%s",
                 (unsigned long)namespace_len, req->namespace_name,
                 (unsigned long)field_len, term->field,
                 term->gt != NULL ? 1 : 0, (unsigned long)gt_len, gt,
                 term->gte != NULL ? 1 : 0, (unsigned long)gte_len, gte,
                 term->lt != NULL ? 1 : 0, (unsigned long)lt_len, lt,
                 term->lte != NULL ? 1 : 0, (unsigned long)lte_len, lte);
  return key;
}

static char *
lc_pouch_index_text_result_plan_key(const lc_pouch_allocator *allocator,
                                    const lc_pouch_query_index_scan_req *req,
                                    const char *kind, const char *field,
                                    const char *value, int ignore_case) {
  size_t namespace_len;
  size_t field_len;
  size_t value_len;
  int written;
  size_t needed;
  char *key;

  if (req == NULL || kind == NULL || field == NULL || value == NULL) {
    return NULL;
  }
  namespace_len = strlen(req->namespace_name);
  field_len = strlen(field);
  value_len = strlen(value);
  written = snprintf(NULL, 0, "%s:%lu:%s:%lu:%s:%d:%lu:%s", kind,
                     (unsigned long)namespace_len, req->namespace_name,
                     (unsigned long)field_len, field, ignore_case ? 1 : 0,
                     (unsigned long)value_len, value);
  if (written < 0) {
    return NULL;
  }
  needed = (size_t)written + 1U;
  key = (char *)lc_pouch_alloc(allocator, needed);
  if (key == NULL) {
    return NULL;
  }
  (void)snprintf(key, needed, "%s:%lu:%s:%lu:%s:%d:%lu:%s", kind,
                 (unsigned long)namespace_len, req->namespace_name,
                 (unsigned long)field_len, field, ignore_case ? 1 : 0,
                 (unsigned long)value_len, value);
  return key;
}

static char *lc_pouch_index_prefix_result_plan_key(
    const lc_pouch_allocator *allocator,
    const lc_pouch_query_index_scan_req *req) {
  const lc_pouch_document_prefix_term *term;

  if (!lc_pouch_index_result_plan_prefix_cacheable(req)) {
    return NULL;
  }
  term = &req->document_prefix_terms[0];
  return lc_pouch_index_text_result_plan_key(
      allocator, req, "prefix", term->field, term->value, term->ignore_case);
}

static char *lc_pouch_index_contains_result_plan_key(
    const lc_pouch_allocator *allocator,
    const lc_pouch_query_index_scan_req *req) {
  const lc_pouch_document_contains_term *term;

  if (!lc_pouch_index_result_plan_contains_cacheable(req)) {
    return NULL;
  }
  term = &req->document_contains_terms[0];
  return lc_pouch_index_text_result_plan_key(
      allocator, req, "contains", term->field, term->value, term->ignore_case);
}

char *lc_pouch_index_result_plan_key(const lc_pouch_allocator *allocator,
                                     const lc_pouch_query_index_scan_req *req,
                                     lc_pouch_index_result_plan_kind kind) {
  switch (kind) {
  case LC_POUCH_INDEX_RESULT_PLAN_EQ:
    return lc_pouch_index_eq_result_plan_key(allocator, req);
  case LC_POUCH_INDEX_RESULT_PLAN_EXISTS:
    return lc_pouch_index_exists_result_plan_key(allocator, req);
  case LC_POUCH_INDEX_RESULT_PLAN_IN:
    return lc_pouch_index_in_result_plan_key(allocator, req);
  case LC_POUCH_INDEX_RESULT_PLAN_RANGE:
    return lc_pouch_index_range_result_plan_key(allocator, req);
  case LC_POUCH_INDEX_RESULT_PLAN_PREFIX:
    return lc_pouch_index_prefix_result_plan_key(allocator, req);
  case LC_POUCH_INDEX_RESULT_PLAN_CONTAINS:
    return lc_pouch_index_contains_result_plan_key(allocator, req);
  }
  return NULL;
}

static int lc_pouch_index_result_cache_compare(uint64_t left_generation,
                                               const char *left_plan_key,
                                               uint64_t right_generation,
                                               const char *right_plan_key) {
  int cmp;

  if (left_generation < right_generation) {
    return -1;
  }
  if (left_generation > right_generation) {
    return 1;
  }
  cmp = strcmp(left_plan_key, right_plan_key);
  if (cmp < 0) {
    return -1;
  }
  if (cmp > 0) {
    return 1;
  }
  return 0;
}

static int lc_pouch_index_result_cache_find_position(
    const lc_pouch_index_result_cache *cache, uint64_t generation,
    const char *plan_key, size_t *position_out) {
  size_t low;
  size_t high;

  if (position_out != NULL) {
    *position_out = 0U;
  }
  if (cache == NULL || plan_key == NULL) {
    return 0;
  }
  low = 0U;
  high = cache->count;
  while (low < high) {
    size_t mid;
    int cmp;

    mid = low + ((high - low) / 2U);
    cmp = lc_pouch_index_result_cache_compare(cache->entries[mid].generation,
                                              cache->entries[mid].plan_key,
                                              generation, plan_key);
    if (cmp < 0) {
      low = mid + 1U;
    } else {
      high = mid;
    }
  }
  if (position_out != NULL) {
    *position_out = low;
  }
  return low < cache->count &&
         lc_pouch_index_result_cache_compare(cache->entries[low].generation,
                                             cache->entries[low].plan_key,
                                             generation, plan_key) == 0;
}

static int
lc_pouch_index_result_cache_reserve(const lc_pouch_allocator *allocator,
                                    lc_pouch_index_result_cache *cache,
                                    size_t needed) {
  size_t new_capacity;
  lc_pouch_index_result_cache_entry *grown;

  if (cache == NULL) {
    return 0;
  }
  if (needed <= cache->capacity) {
    return 1;
  }
  new_capacity = cache->capacity == 0U ? 16U : cache->capacity;
  while (new_capacity < needed) {
    if (new_capacity > ((size_t)-1) / 2U) {
      return 0;
    }
    new_capacity *= 2U;
  }
  if (new_capacity > ((size_t)-1) / sizeof(cache->entries[0])) {
    return 0;
  }
  grown = (lc_pouch_index_result_cache_entry *)lc_pouch_realloc(
      allocator, cache->entries, new_capacity * sizeof(cache->entries[0]));
  if (grown == NULL) {
    return 0;
  }
  cache->entries = grown;
  cache->capacity = new_capacity;
  return 1;
}

void lc_pouch_index_result_cache_cleanup(const lc_pouch_allocator *allocator,
                                         lc_pouch_index_result_cache *cache) {
  size_t index;

  if (cache == NULL) {
    return;
  }
  for (index = 0U; index < cache->count; ++index) {
    lc_pouch_free(allocator, cache->entries[index].plan_key);
    lc_pouch_index_doc_id_set_cleanup(allocator,
                                      &cache->entries[index].doc_ids);
  }
  lc_pouch_free(allocator, cache->entries);
  memset(cache, 0, sizeof(*cache));
}

void lc_pouch_index_result_page_cleanup(const lc_pouch_allocator *allocator,
                                        lc_pouch_index_result_page *page) {
  if (page == NULL) {
    return;
  }
  lc_pouch_index_doc_id_set_cleanup(allocator, &page->doc_ids);
  lc_pouch_free(allocator, page->next_start_after);
  memset(page, 0, sizeof(*page));
}

int lc_pouch_index_result_page_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_index_doc_table *doc_table,
    const lc_pouch_index_doc_id_set *doc_ids, const char *namespace_name,
    const char *start_after, size_t limit, lc_pouch_index_result_page *page,
    int *invalid_doc_id_out) {
  size_t index;

  if (page == NULL) {
    return 0;
  }
  memset(page, 0, sizeof(*page));
  if (invalid_doc_id_out != NULL) {
    *invalid_doc_id_out = 0;
  }
  if (doc_table == NULL || doc_ids == NULL || namespace_name == NULL) {
    return 1;
  }
  for (index = 0U; index < doc_ids->count; ++index) {
    const char *doc_namespace;
    const char *key;

    if (!lc_pouch_index_doc_table_lookup(doc_table, doc_ids->items[index],
                                         &doc_namespace, &key)) {
      if (invalid_doc_id_out != NULL) {
        *invalid_doc_id_out = 1;
      }
      lc_pouch_index_result_page_cleanup(allocator, page);
      return 0;
    }
    if (strcmp(doc_namespace, namespace_name) != 0) {
      continue;
    }
    if (start_after != NULL && strcmp(key, start_after) <= 0) {
      continue;
    }
    if (limit > 0U && page->doc_ids.count == limit) {
      page->next_start_after = lc_pouch_strdup(
          allocator,
          doc_table->entries[page->doc_ids.items[page->doc_ids.count - 1U]]
              .key);
      if (page->next_start_after == NULL) {
        lc_pouch_index_result_page_cleanup(allocator, page);
        return 0;
      }
      page->truncated = 1;
      return 1;
    }
    if (!lc_pouch_index_doc_id_set_append(allocator, &page->doc_ids,
                                          doc_ids->items[index])) {
      lc_pouch_index_result_page_cleanup(allocator, page);
      return 0;
    }
  }
  return 1;
}

int lc_pouch_index_result_cache_find(const lc_pouch_allocator *allocator,
                                     const lc_pouch_index_result_cache *cache,
                                     uint64_t generation, const char *plan_key,
                                     lc_pouch_index_doc_id_set *dst) {
  size_t position;

  if (dst == NULL) {
    return 0;
  }
  dst->count = 0U;
  if (!lc_pouch_index_result_cache_find_position(cache, generation, plan_key,
                                                 &position)) {
    return 0;
  }
  return lc_pouch_index_doc_id_set_clone(allocator, dst,
                                         &cache->entries[position].doc_ids);
}

int lc_pouch_index_result_cache_put(const lc_pouch_allocator *allocator,
                                    lc_pouch_index_result_cache *cache,
                                    uint64_t generation, const char *plan_key,
                                    const lc_pouch_index_doc_id_set *doc_ids) {
  lc_pouch_index_doc_id_set copy;
  char *plan_key_copy;
  size_t position;

  if (cache == NULL || plan_key == NULL || doc_ids == NULL) {
    return 0;
  }
  memset(&copy, 0, sizeof(copy));
  if (!lc_pouch_index_doc_id_set_clone(allocator, &copy, doc_ids) ||
      !lc_pouch_index_doc_id_set_sort_unique(&copy)) {
    lc_pouch_index_doc_id_set_cleanup(allocator, &copy);
    return 0;
  }
  if (lc_pouch_index_result_cache_find_position(cache, generation, plan_key,
                                                &position)) {
    lc_pouch_index_doc_id_set_cleanup(allocator,
                                      &cache->entries[position].doc_ids);
    cache->entries[position].doc_ids = copy;
    return 1;
  }
  plan_key_copy = lc_pouch_strdup(allocator, plan_key);
  if (plan_key_copy == NULL) {
    lc_pouch_index_doc_id_set_cleanup(allocator, &copy);
    return 0;
  }
  if (!lc_pouch_index_result_cache_reserve(allocator, cache,
                                           cache->count + 1U)) {
    lc_pouch_free(allocator, plan_key_copy);
    lc_pouch_index_doc_id_set_cleanup(allocator, &copy);
    return 0;
  }
  if (position < cache->count) {
    memmove(&cache->entries[position + 1U], &cache->entries[position],
            (cache->count - position) * sizeof(cache->entries[0]));
  }
  cache->entries[position].generation = generation;
  cache->entries[position].plan_key = plan_key_copy;
  cache->entries[position].doc_ids = copy;
  cache->count++;
  return 1;
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

int lc_pouch_index_collect_range_term_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_range_term *term,
    lc_pouch_index_range_term_doc_ids_fn read_range, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error) {
  int rc;

  if (term == NULL || term->field == NULL || read_range == NULL ||
      doc_ids == NULL) {
    return LC_OK;
  }
  rc = read_range(read_context, term, doc_ids, error);
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

int lc_pouch_index_collect_prefix_term_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_prefix_term *term,
    lc_pouch_index_prefix_term_doc_ids_fn read_prefix, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error) {
  int rc;

  if (term == NULL || term->field == NULL || term->value == NULL ||
      read_prefix == NULL || doc_ids == NULL) {
    return LC_OK;
  }
  rc = read_prefix(read_context, term, doc_ids, error);
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

int lc_pouch_index_collect_contains_term_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_contains_term *term,
    lc_pouch_index_contains_term_doc_ids_fn read_contains, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error) {
  int rc;

  if (term == NULL || term->field == NULL || term->value == NULL ||
      read_contains == NULL || doc_ids == NULL) {
    return LC_OK;
  }
  rc = read_contains(read_context, term, doc_ids, error);
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

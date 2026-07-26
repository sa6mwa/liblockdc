#include "lc_pouch_index.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

#define LC_POUCH_INDEX_DOC_GENERATION_MAGIC "LCPDTG1\0"
#define LC_POUCH_INDEX_DOC_MAGIC_LEN 8U
#define LC_POUCH_INDEX_DOC_GENERATION_HEADER_SIZE 40U
#define LC_POUCH_INDEX_DOC_ENTRY_FIXED_SIZE 4U

static int lc_pouch_index_doc_add_size(size_t *total, size_t add) {
  if (total == NULL || *total > ((size_t)-1) - add) {
    return 0;
  }
  *total += add;
  return 1;
}

static void lc_pouch_index_doc_put_u32(unsigned char *dst, uint32_t value) {
  dst[0] = (unsigned char)(value & UINT32_C(0xff));
  dst[1] = (unsigned char)((value >> 8U) & UINT32_C(0xff));
  dst[2] = (unsigned char)((value >> 16U) & UINT32_C(0xff));
  dst[3] = (unsigned char)((value >> 24U) & UINT32_C(0xff));
}

static void lc_pouch_index_doc_put_u64(unsigned char *dst, uint64_t value) {
  size_t index;

  for (index = 0U; index < 8U; ++index) {
    dst[index] = (unsigned char)((value >> (index * 8U)) & UINT64_C(0xff));
  }
}

static uint32_t lc_pouch_index_doc_get_u32(const unsigned char *src) {
  return ((uint32_t)src[0]) | (((uint32_t)src[1]) << 8U) |
         (((uint32_t)src[2]) << 16U) | (((uint32_t)src[3]) << 24U);
}

static uint64_t lc_pouch_index_doc_get_u64(const unsigned char *src) {
  uint64_t value;
  size_t index;

  value = 0U;
  for (index = 0U; index < 8U; ++index) {
    value |= ((uint64_t)src[index]) << (index * 8U);
  }
  return value;
}

static int lc_pouch_index_doc_decode_require(size_t src_size, size_t offset,
                                             size_t needed) {
  return offset <= src_size && needed <= src_size - offset;
}

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

void lc_pouch_index_doc_generation_cleanup(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_doc_generation *generation) {
  if (generation == NULL) {
    return;
  }
  lc_pouch_free(allocator, generation->namespace_name);
  lc_pouch_index_doc_table_cleanup(allocator, &generation->docs);
  memset(generation, 0, sizeof(*generation));
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

int lc_pouch_index_doc_generation_encoded_size(
    const lc_pouch_index_doc_generation *generation, size_t *size_out) {
  size_t namespace_len;
  size_t total;
  size_t index;

  if (size_out != NULL) {
    *size_out = 0U;
  }
  if (generation == NULL || generation->namespace_name == NULL ||
      size_out == NULL || generation->docs.count > (size_t)UINT32_MAX) {
    return 0;
  }
  namespace_len = strlen(generation->namespace_name);
  if (namespace_len > UINT32_MAX) {
    return 0;
  }
  total = LC_POUCH_INDEX_DOC_GENERATION_HEADER_SIZE;
  if (!lc_pouch_index_doc_add_size(&total, namespace_len)) {
    return 0;
  }
  for (index = 0U; index < generation->docs.count; ++index) {
    const lc_pouch_index_doc_entry *entry;
    size_t key_len;

    entry = &generation->docs.entries[index];
    if (entry->namespace_name == NULL || entry->key == NULL ||
        strcmp(entry->namespace_name, generation->namespace_name) != 0 ||
        entry->id != (lc_pouch_index_doc_id)index) {
      return 0;
    }
    if (index > 0U && lc_pouch_index_doc_table_compare_entry(
                          &generation->docs.entries[index - 1U],
                          entry->namespace_name, entry->key) >= 0) {
      return 0;
    }
    key_len = strlen(entry->key);
    if (key_len > UINT32_MAX ||
        !lc_pouch_index_doc_add_size(&total,
                                     LC_POUCH_INDEX_DOC_ENTRY_FIXED_SIZE) ||
        !lc_pouch_index_doc_add_size(&total, key_len)) {
      return 0;
    }
  }
  *size_out = total;
  return 1;
}

int lc_pouch_index_doc_generation_encode(
    const lc_pouch_index_doc_generation *generation, unsigned char *dst,
    size_t dst_size, size_t *written_out) {
  unsigned char *cursor;
  size_t needed;
  size_t namespace_len;
  size_t index;

  if (written_out != NULL) {
    *written_out = 0U;
  }
  if (generation == NULL || dst == NULL || written_out == NULL ||
      !lc_pouch_index_doc_generation_encoded_size(generation, &needed) ||
      dst_size < needed) {
    return 0;
  }
  namespace_len = strlen(generation->namespace_name);
  cursor = dst;
  memcpy(cursor, LC_POUCH_INDEX_DOC_GENERATION_MAGIC,
         LC_POUCH_INDEX_DOC_MAGIC_LEN);
  cursor += LC_POUCH_INDEX_DOC_MAGIC_LEN;
  lc_pouch_index_doc_put_u32(cursor, 1U);
  cursor += 4U;
  lc_pouch_index_doc_put_u32(cursor, (uint32_t)namespace_len);
  cursor += 4U;
  lc_pouch_index_doc_put_u32(cursor, (uint32_t)generation->docs.count);
  cursor += 4U;
  lc_pouch_index_doc_put_u32(cursor, 0U);
  cursor += 4U;
  lc_pouch_index_doc_put_u64(cursor, generation->identity.sequence);
  cursor += 8U;
  lc_pouch_index_doc_put_u64(cursor, generation->identity.manifest_generation);
  cursor += 8U;
  memcpy(cursor, generation->namespace_name, namespace_len);
  cursor += namespace_len;
  for (index = 0U; index < generation->docs.count; ++index) {
    const lc_pouch_index_doc_entry *entry;
    size_t key_len;

    entry = &generation->docs.entries[index];
    key_len = strlen(entry->key);
    lc_pouch_index_doc_put_u32(cursor, (uint32_t)key_len);
    cursor += 4U;
    memcpy(cursor, entry->key, key_len);
    cursor += key_len;
  }
  *written_out = (size_t)(cursor - dst);
  return *written_out == needed;
}

int lc_pouch_index_doc_generation_decode(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_doc_generation *generation, const unsigned char *src,
    size_t src_size) {
  lc_pouch_index_doc_generation decoded;
  size_t offset;
  uint32_t version;
  uint32_t namespace_len;
  uint32_t doc_count;
  uint32_t index;

  if (generation == NULL || src == NULL ||
      !lc_pouch_index_doc_decode_require(
          src_size, 0U, LC_POUCH_INDEX_DOC_GENERATION_HEADER_SIZE) ||
      memcmp(src, LC_POUCH_INDEX_DOC_GENERATION_MAGIC,
             LC_POUCH_INDEX_DOC_MAGIC_LEN) != 0) {
    return 0;
  }
  memset(&decoded, 0, sizeof(decoded));
  offset = LC_POUCH_INDEX_DOC_MAGIC_LEN;
  version = lc_pouch_index_doc_get_u32(src + offset);
  offset += 4U;
  namespace_len = lc_pouch_index_doc_get_u32(src + offset);
  offset += 4U;
  doc_count = lc_pouch_index_doc_get_u32(src + offset);
  offset += 4U;
  if (lc_pouch_index_doc_get_u32(src + offset) != 0U) {
    return 0;
  }
  offset += 4U;
  decoded.identity.sequence = lc_pouch_index_doc_get_u64(src + offset);
  offset += 8U;
  decoded.identity.manifest_generation =
      lc_pouch_index_doc_get_u64(src + offset);
  offset += 8U;
  if (version != 1U ||
      !lc_pouch_index_doc_decode_require(src_size, offset, namespace_len)) {
    return 0;
  }
  decoded.namespace_name =
      (char *)lc_pouch_alloc(allocator, (size_t)namespace_len + 1U);
  if (decoded.namespace_name == NULL) {
    return 0;
  }
  memcpy(decoded.namespace_name, src + offset, namespace_len);
  decoded.namespace_name[namespace_len] = '\0';
  offset += namespace_len;
  for (index = 0U; index < doc_count; ++index) {
    lc_pouch_index_doc_id doc_id;
    char *key;
    uint32_t key_len;

    if (!lc_pouch_index_doc_decode_require(
            src_size, offset, LC_POUCH_INDEX_DOC_ENTRY_FIXED_SIZE)) {
      lc_pouch_index_doc_generation_cleanup(allocator, &decoded);
      return 0;
    }
    key_len = lc_pouch_index_doc_get_u32(src + offset);
    offset += 4U;
    if (!lc_pouch_index_doc_decode_require(src_size, offset, key_len)) {
      lc_pouch_index_doc_generation_cleanup(allocator, &decoded);
      return 0;
    }
    key = (char *)lc_pouch_alloc(allocator, (size_t)key_len + 1U);
    if (key == NULL) {
      lc_pouch_index_doc_generation_cleanup(allocator, &decoded);
      return 0;
    }
    memcpy(key, src + offset, key_len);
    key[key_len] = '\0';
    offset += key_len;
    if (!lc_pouch_index_doc_table_find_or_add(
            allocator, &decoded.docs, decoded.namespace_name, key, &doc_id) ||
        doc_id != (lc_pouch_index_doc_id)index ||
        decoded.docs.count != (size_t)index + 1U) {
      lc_pouch_free(allocator, key);
      lc_pouch_index_doc_generation_cleanup(allocator, &decoded);
      return 0;
    }
    lc_pouch_free(allocator, key);
  }
  if (offset != src_size) {
    lc_pouch_index_doc_generation_cleanup(allocator, &decoded);
    return 0;
  }
  lc_pouch_index_doc_generation_cleanup(allocator, generation);
  *generation = decoded;
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

void lc_pouch_index_doc_id_set_reset(lc_pouch_index_doc_id_set *set) {
  if (set == NULL) {
    return;
  }
  set->count = 0U;
}

void lc_pouch_index_doc_id_scratch_cleanup(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_doc_id_scratch *scratch) {
  if (scratch == NULL) {
    return;
  }
  lc_pouch_index_doc_id_set_cleanup(allocator, &scratch->term);
  lc_pouch_index_doc_id_set_cleanup(allocator, &scratch->merge);
  memset(scratch, 0, sizeof(*scratch));
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

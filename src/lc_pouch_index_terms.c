#include "lc_pouch_index.h"

#include <string.h>

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

#include "lc_pouch_index.h"

#include <stdlib.h>
#include <string.h>

static int lc_pouch_index_temporal_doc_compare_value(
    int64_t left_seconds, int32_t left_nanosecond, uint32_t left_doc_id,
    int64_t right_seconds, int32_t right_nanosecond, uint32_t right_doc_id) {
  if (left_seconds < right_seconds) {
    return -1;
  }
  if (left_seconds > right_seconds) {
    return 1;
  }
  if (left_nanosecond < right_nanosecond) {
    return -1;
  }
  if (left_nanosecond > right_nanosecond) {
    return 1;
  }
  if (left_doc_id < right_doc_id) {
    return -1;
  }
  if (left_doc_id > right_doc_id) {
    return 1;
  }
  return 0;
}

static int lc_pouch_index_temporal_doc_qsort_compare(const void *left,
                                                     const void *right) {
  const lc_pouch_index_temporal_doc_entry *a;
  const lc_pouch_index_temporal_doc_entry *b;

  a = (const lc_pouch_index_temporal_doc_entry *)left;
  b = (const lc_pouch_index_temporal_doc_entry *)right;
  return lc_pouch_index_temporal_doc_compare_value(
      a->unix_seconds, a->nanosecond, a->doc_id, b->unix_seconds,
      b->nanosecond, b->doc_id);
}

static int lc_pouch_index_temporal_doc_after_bound(
    const lc_pouch_index_temporal_doc_entry *entry, int64_t unix_seconds,
    int32_t nanosecond) {
  if (entry->unix_seconds > unix_seconds) {
    return 1;
  }
  return entry->unix_seconds == unix_seconds && entry->nanosecond > nanosecond;
}

static int lc_pouch_index_temporal_field_find_position(
    const lc_pouch_index_temporal_posting_table *table, const char *field,
    size_t *position_out) {
  size_t low;
  size_t high;

  if (position_out != NULL) {
    *position_out = 0U;
  }
  if (table == NULL || field == NULL) {
    return 0;
  }
  low = 0U;
  high = table->field_count;
  while (low < high) {
    size_t mid;
    int cmp;

    mid = low + ((high - low) / 2U);
    cmp = strcmp(table->fields[mid].field, field);
    if (cmp < 0) {
      low = mid + 1U;
    } else {
      high = mid;
    }
  }
  if (position_out != NULL) {
    *position_out = low;
  }
  return low < table->field_count &&
         strcmp(table->fields[low].field, field) == 0;
}

static int lc_pouch_index_temporal_fields_reserve(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_temporal_posting_table *table, size_t needed) {
  size_t new_capacity;
  lc_pouch_index_temporal_field_entry *grown;

  if (table == NULL) {
    return 0;
  }
  if (needed <= table->field_capacity) {
    return 1;
  }
  new_capacity = table->field_capacity == 0U ? 8U : table->field_capacity;
  while (new_capacity < needed) {
    if (new_capacity > ((size_t)-1) / 2U) {
      return 0;
    }
    new_capacity *= 2U;
  }
  if (new_capacity > ((size_t)-1) / sizeof(table->fields[0])) {
    return 0;
  }
  grown = (lc_pouch_index_temporal_field_entry *)lc_pouch_realloc(
      allocator, table->fields, new_capacity * sizeof(table->fields[0]));
  if (grown == NULL) {
    return 0;
  }
  table->fields = grown;
  table->field_capacity = new_capacity;
  return 1;
}

static int lc_pouch_index_temporal_values_reserve(
    const lc_pouch_allocator *allocator, lc_pouch_index_temporal_field_entry *field,
    size_t needed) {
  size_t new_capacity;
  lc_pouch_index_temporal_doc_entry *grown;

  if (field == NULL) {
    return 0;
  }
  if (needed <= field->value_capacity) {
    return 1;
  }
  new_capacity = field->value_capacity == 0U ? 32U : field->value_capacity;
  while (new_capacity < needed) {
    if (new_capacity > ((size_t)-1) / 2U) {
      return 0;
    }
    new_capacity *= 2U;
  }
  if (new_capacity > ((size_t)-1) / sizeof(field->values[0])) {
    return 0;
  }
  grown = (lc_pouch_index_temporal_doc_entry *)lc_pouch_realloc(
      allocator, field->values, new_capacity * sizeof(field->values[0]));
  if (grown == NULL) {
    return 0;
  }
  field->values = grown;
  field->value_capacity = new_capacity;
  return 1;
}

void lc_pouch_index_temporal_posting_table_cleanup(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_temporal_posting_table *table) {
  size_t index;

  if (table == NULL) {
    return;
  }
  for (index = 0U; index < table->field_count; ++index) {
    lc_pouch_free(allocator, table->fields[index].field);
    lc_pouch_free(allocator, table->fields[index].values);
    lc_pouch_index_doc_id_set_cleanup(allocator, &table->fields[index].doc_ids);
    lc_pouch_index_posting_cleanup(allocator, &table->fields[index].posting);
  }
  lc_pouch_free(allocator, table->fields);
  memset(table, 0, sizeof(*table));
}

int lc_pouch_index_temporal_posting_table_mark_field(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_temporal_posting_table *table, const char *field) {
  lc_pouch_index_temporal_field_entry entry;
  size_t position;

  if (table == NULL || field == NULL) {
    return 0;
  }
  if (lc_pouch_index_temporal_field_find_position(table, field, &position)) {
    return 1;
  }
  memset(&entry, 0, sizeof(entry));
  entry.field = lc_pouch_strdup(allocator, field);
  entry.values_sorted = 1;
  if (entry.field == NULL) {
    return 0;
  }
  if (!lc_pouch_index_temporal_fields_reserve(allocator, table,
                                              table->field_count + 1U)) {
    lc_pouch_free(allocator, entry.field);
    return 0;
  }
  if (position < table->field_count) {
    memmove(&table->fields[position + 1U], &table->fields[position],
            (table->field_count - position) * sizeof(table->fields[0]));
  }
  table->fields[position] = entry;
  table->field_count++;
  return 1;
}

int lc_pouch_index_temporal_posting_table_has_field(
    const lc_pouch_index_temporal_posting_table *table, const char *field) {
  return lc_pouch_index_temporal_field_find_position(table, field, NULL);
}

int lc_pouch_index_temporal_posting_table_add_value_doc_id(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_temporal_posting_table *table, const char *field,
    int64_t unix_seconds, int32_t nanosecond, lc_pouch_index_doc_id doc_id) {
  lc_pouch_index_temporal_field_entry *entry;
  size_t position;

  if (table == NULL || field == NULL) {
    return 0;
  }
  if (!lc_pouch_index_temporal_posting_table_mark_field(allocator, table,
                                                        field) ||
      !lc_pouch_index_temporal_field_find_position(table, field, &position)) {
    return 0;
  }
  entry = &table->fields[position];
  if (!lc_pouch_index_temporal_values_reserve(allocator, entry,
                                              entry->value_count + 1U)) {
    return 0;
  }
  entry->values[entry->value_count].unix_seconds = unix_seconds;
  entry->values[entry->value_count].nanosecond = nanosecond;
  entry->values[entry->value_count].doc_id = doc_id;
  entry->value_count++;
  entry->values_sorted = 0;
  return 1;
}

int lc_pouch_index_temporal_posting_table_add_residual_doc_id(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_temporal_posting_table *table, const char *field,
    lc_pouch_index_doc_id doc_id) {
  lc_pouch_index_temporal_field_entry *entry;
  size_t position;

  if (table == NULL || field == NULL) {
    return 0;
  }
  if (!lc_pouch_index_temporal_posting_table_mark_field(allocator, table,
                                                        field) ||
      !lc_pouch_index_temporal_field_find_position(table, field, &position)) {
    return 0;
  }
  entry = &table->fields[position];
  entry->posting_ready = 0;
  return lc_pouch_index_doc_id_set_append(allocator, &entry->doc_ids, doc_id);
}

static void lc_pouch_index_temporal_field_sort_values(
    lc_pouch_index_temporal_field_entry *field) {
  size_t index;
  size_t out;

  if (field == NULL || field->values_sorted) {
    return;
  }
  if (field->value_count > 1U) {
    qsort(field->values, field->value_count, sizeof(field->values[0]),
          lc_pouch_index_temporal_doc_qsort_compare);
  }
  out = 0U;
  for (index = 0U; index < field->value_count; ++index) {
    if (out > 0U &&
        lc_pouch_index_temporal_doc_compare_value(
            field->values[out - 1U].unix_seconds,
            field->values[out - 1U].nanosecond, field->values[out - 1U].doc_id,
            field->values[index].unix_seconds, field->values[index].nanosecond,
            field->values[index].doc_id) == 0) {
      continue;
    }
    if (out != index) {
      field->values[out] = field->values[index];
    }
    ++out;
  }
  field->value_count = out;
  field->values_sorted = 1;
}

int lc_pouch_index_temporal_posting_table_build_postings(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_temporal_posting_table *table) {
  size_t index;

  if (table == NULL) {
    return 0;
  }
  for (index = 0U; index < table->field_count; ++index) {
    lc_pouch_index_temporal_field_sort_values(&table->fields[index]);
    if (table->fields[index].posting_ready) {
      continue;
    }
    lc_pouch_index_posting_cleanup(allocator, &table->fields[index].posting);
    if (!lc_pouch_index_doc_id_set_sort_unique(&table->fields[index].doc_ids)) {
      return 0;
    }
    if (table->fields[index].doc_ids.count > 1U) {
      if (!lc_pouch_index_posting_build(
              allocator, &table->fields[index].posting,
              table->fields[index].doc_ids.items,
              table->fields[index].doc_ids.count)) {
        return 0;
      }
      table->fields[index].posting_ready = 1;
    }
  }
  return 1;
}

static size_t lc_pouch_index_temporal_field_upper_bound(
    const lc_pouch_index_temporal_field_entry *field, int64_t unix_seconds,
    int32_t nanosecond) {
  size_t low;
  size_t high;

  low = 0U;
  high = field != NULL ? field->value_count : 0U;
  while (low < high) {
    size_t mid;
    const lc_pouch_index_temporal_doc_entry *entry;

    mid = low + ((high - low) / 2U);
    entry = &field->values[mid];
    if (lc_pouch_index_temporal_doc_after_bound(entry, unix_seconds,
                                                nanosecond)) {
      high = mid;
    } else {
      low = mid + 1U;
    }
  }
  return low;
}

int lc_pouch_index_temporal_posting_table_append_after(
    const lc_pouch_allocator *allocator,
    const lc_pouch_index_temporal_posting_table *table, const char *field,
    int64_t unix_seconds, int32_t nanosecond,
    lc_pouch_index_doc_id_set *dst) {
  const lc_pouch_index_temporal_field_entry *entry;
  size_t position;
  size_t index;

  if (table == NULL || field == NULL || dst == NULL) {
    return 0;
  }
  if (!lc_pouch_index_temporal_field_find_position(table, field, &position)) {
    return 1;
  }
  entry = &table->fields[position];
  if (!entry->values_sorted) {
    return 0;
  }
  if (entry->posting_ready) {
    if (!lc_pouch_index_posting_append(allocator, &entry->posting, dst)) {
      return 0;
    }
  } else {
    for (index = 0U; index < entry->doc_ids.count; ++index) {
      if (!lc_pouch_index_doc_id_set_append(allocator, dst,
                                            entry->doc_ids.items[index])) {
        return 0;
      }
    }
  }
  position = lc_pouch_index_temporal_field_upper_bound(entry, unix_seconds,
                                                       nanosecond);
  for (index = position; index < entry->value_count; ++index) {
    if (!lc_pouch_index_doc_id_set_append(allocator, dst,
                                          entry->values[index].doc_id)) {
      return 0;
    }
  }
  return 1;
}

void lc_pouch_index_prepared_temporal_cache_cleanup(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_prepared_temporal_cache *cache) {
  if (cache == NULL) {
    return;
  }
  lc_pouch_index_temporal_posting_table_cleanup(allocator, &cache->postings);
  cache->generation = 0U;
  memset(&cache->identity, 0, sizeof(cache->identity));
}

void lc_pouch_index_prepared_temporal_cache_refresh_identity(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_prepared_temporal_cache *cache,
    lc_pouch_index_identity identity) {
  if (cache == NULL ||
      (cache->identity.sequence == identity.sequence &&
       cache->identity.manifest_generation == identity.manifest_generation)) {
    return;
  }
  lc_pouch_index_prepared_temporal_cache_cleanup(allocator, cache);
  cache->identity = identity;
  cache->generation = identity.sequence;
}

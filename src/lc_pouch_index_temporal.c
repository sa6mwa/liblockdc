#include "lc_pouch_index.h"

#include <stdlib.h>
#include <string.h>

#define LC_POUCH_INDEX_TEMPORAL_MAGIC_LEN 8U
#define LC_POUCH_INDEX_TEMPORAL_HEADER_SIZE 16U
#define LC_POUCH_INDEX_TEMPORAL_FIELD_FIXED_SIZE 12U
#define LC_POUCH_INDEX_TEMPORAL_VALUE_SIZE 16U
#define LC_POUCH_INDEX_TEMPORAL_MAGIC "LCPTMP1\0"

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
    const lc_pouch_allocator *allocator,
    lc_pouch_index_temporal_field_entry *field, size_t needed) {
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

static int lc_pouch_index_temporal_add_size(size_t *total, size_t add) {
  if (total == NULL || *total > ((size_t)-1) - add) {
    return 0;
  }
  *total += add;
  return 1;
}

static uint64_t lc_pouch_index_temporal_zigzag_i64(int64_t value) {
  if (value < 0) {
    return (((uint64_t)(-(value + 1))) << 1U) | UINT64_C(1);
  }
  return ((uint64_t)value) << 1U;
}

static int lc_pouch_index_temporal_unzigzag_i64(uint64_t value,
                                                int64_t *out) {
  uint64_t magnitude;

  if (out == NULL) {
    return 0;
  }
  magnitude = value >> 1U;
  if ((value & UINT64_C(1)) != 0U) {
    if (magnitude > (uint64_t)INT64_MAX) {
      return 0;
    }
    *out = -((int64_t)magnitude) - INT64_C(1);
    return 1;
  }
  if (magnitude > (uint64_t)INT64_MAX) {
    return 0;
  }
  *out = (int64_t)magnitude;
  return 1;
}

static void lc_pouch_index_temporal_put_u32(unsigned char *dst,
                                            uint32_t value) {
  dst[0] = (unsigned char)(value & 0xffU);
  dst[1] = (unsigned char)((value >> 8U) & 0xffU);
  dst[2] = (unsigned char)((value >> 16U) & 0xffU);
  dst[3] = (unsigned char)((value >> 24U) & 0xffU);
}

static void lc_pouch_index_temporal_put_u64(unsigned char *dst,
                                            uint64_t value) {
  size_t index;

  for (index = 0U; index < 8U; ++index) {
    dst[index] = (unsigned char)((value >> (index * 8U)) & UINT64_C(0xff));
  }
}

static uint32_t lc_pouch_index_temporal_get_u32(const unsigned char *src) {
  return ((uint32_t)src[0]) | (((uint32_t)src[1]) << 8U) |
         (((uint32_t)src[2]) << 16U) | (((uint32_t)src[3]) << 24U);
}

static uint64_t lc_pouch_index_temporal_get_u64(const unsigned char *src) {
  uint64_t value;
  size_t index;

  value = 0U;
  for (index = 0U; index < 8U; ++index) {
    value |= ((uint64_t)src[index]) << (index * 8U);
  }
  return value;
}

int lc_pouch_index_temporal_posting_table_encoded_size(
    const lc_pouch_index_temporal_posting_table *table, size_t *size_out) {
  size_t total;
  size_t index;

  if (size_out != NULL) {
    *size_out = 0U;
  }
  if (table == NULL || size_out == NULL || table->field_count > UINT32_MAX) {
    return 0;
  }
  total = LC_POUCH_INDEX_TEMPORAL_HEADER_SIZE;
  for (index = 0U; index < table->field_count; ++index) {
    const lc_pouch_index_temporal_field_entry *field;
    size_t field_len;
    size_t value_bytes;
    size_t residual_bytes;

    field = &table->fields[index];
    if (field->field == NULL || field->value_count > UINT32_MAX ||
        field->doc_ids.count > UINT32_MAX || !field->values_sorted) {
      return 0;
    }
    field_len = strlen(field->field);
    if (field_len > UINT32_MAX ||
        field->value_count > ((size_t)-1) / LC_POUCH_INDEX_TEMPORAL_VALUE_SIZE ||
        field->doc_ids.count > ((size_t)-1) / sizeof(uint32_t)) {
      return 0;
    }
    value_bytes = field->value_count * LC_POUCH_INDEX_TEMPORAL_VALUE_SIZE;
    residual_bytes = field->doc_ids.count * sizeof(uint32_t);
    if (!lc_pouch_index_temporal_add_size(
            &total, LC_POUCH_INDEX_TEMPORAL_FIELD_FIXED_SIZE) ||
        !lc_pouch_index_temporal_add_size(&total, field_len) ||
        !lc_pouch_index_temporal_add_size(&total, value_bytes) ||
        !lc_pouch_index_temporal_add_size(&total, residual_bytes)) {
      return 0;
    }
  }
  *size_out = total;
  return 1;
}

int lc_pouch_index_temporal_posting_table_encode(
    const lc_pouch_index_temporal_posting_table *table, unsigned char *dst,
    size_t dst_size, size_t *written_out) {
  unsigned char *cursor;
  size_t needed;
  size_t index;

  if (written_out != NULL) {
    *written_out = 0U;
  }
  if (table == NULL || dst == NULL || written_out == NULL ||
      !lc_pouch_index_temporal_posting_table_encoded_size(table, &needed) ||
      dst_size < needed) {
    return 0;
  }
  cursor = dst;
  memcpy(cursor, LC_POUCH_INDEX_TEMPORAL_MAGIC,
         LC_POUCH_INDEX_TEMPORAL_MAGIC_LEN);
  cursor += LC_POUCH_INDEX_TEMPORAL_MAGIC_LEN;
  lc_pouch_index_temporal_put_u32(cursor, 1U);
  cursor += 4U;
  lc_pouch_index_temporal_put_u32(cursor, (uint32_t)table->field_count);
  cursor += 4U;
  for (index = 0U; index < table->field_count; ++index) {
    const lc_pouch_index_temporal_field_entry *field;
    size_t value_index;
    size_t field_len;

    field = &table->fields[index];
    field_len = strlen(field->field);
    lc_pouch_index_temporal_put_u32(cursor, (uint32_t)field_len);
    cursor += 4U;
    lc_pouch_index_temporal_put_u32(cursor, (uint32_t)field->value_count);
    cursor += 4U;
    lc_pouch_index_temporal_put_u32(cursor, (uint32_t)field->doc_ids.count);
    cursor += 4U;
    memcpy(cursor, field->field, field_len);
    cursor += field_len;
    for (value_index = 0U; value_index < field->value_count; ++value_index) {
      lc_pouch_index_temporal_put_u64(
          cursor, lc_pouch_index_temporal_zigzag_i64(
                      field->values[value_index].unix_seconds));
      cursor += 8U;
      lc_pouch_index_temporal_put_u32(
          cursor, (uint32_t)field->values[value_index].nanosecond);
      cursor += 4U;
      lc_pouch_index_temporal_put_u32(cursor,
                                      field->values[value_index].doc_id);
      cursor += 4U;
    }
    for (value_index = 0U; value_index < field->doc_ids.count; ++value_index) {
      lc_pouch_index_temporal_put_u32(cursor, field->doc_ids.items[value_index]);
      cursor += 4U;
    }
  }
  *written_out = (size_t)(cursor - dst);
  return *written_out == needed;
}

static int lc_pouch_index_temporal_decode_require(size_t src_size,
                                                  size_t offset,
                                                  size_t needed) {
  return offset <= src_size && needed <= src_size - offset;
}

int lc_pouch_index_temporal_posting_table_decode(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_temporal_posting_table *table, const unsigned char *src,
    size_t src_size) {
  lc_pouch_index_temporal_posting_table decoded;
  size_t offset;
  uint32_t version;
  uint32_t field_count;
  uint32_t field_index;

  if (table == NULL || src == NULL ||
      !lc_pouch_index_temporal_decode_require(
          src_size, 0U, LC_POUCH_INDEX_TEMPORAL_HEADER_SIZE) ||
      memcmp(src, LC_POUCH_INDEX_TEMPORAL_MAGIC,
             LC_POUCH_INDEX_TEMPORAL_MAGIC_LEN) != 0) {
    return 0;
  }
  offset = LC_POUCH_INDEX_TEMPORAL_MAGIC_LEN;
  version = lc_pouch_index_temporal_get_u32(src + offset);
  offset += 4U;
  if (version != 1U) {
    return 0;
  }
  field_count = lc_pouch_index_temporal_get_u32(src + offset);
  offset += 4U;
  memset(&decoded, 0, sizeof(decoded));
  for (field_index = 0U; field_index < field_count; ++field_index) {
    char *field_name;
    uint32_t field_len;
    uint32_t value_count;
    uint32_t residual_count;
    uint32_t value_index;

    if (!lc_pouch_index_temporal_decode_require(
            src_size, offset, LC_POUCH_INDEX_TEMPORAL_FIELD_FIXED_SIZE)) {
      lc_pouch_index_temporal_posting_table_cleanup(allocator, &decoded);
      return 0;
    }
    field_len = lc_pouch_index_temporal_get_u32(src + offset);
    offset += 4U;
    value_count = lc_pouch_index_temporal_get_u32(src + offset);
    offset += 4U;
    residual_count = lc_pouch_index_temporal_get_u32(src + offset);
    offset += 4U;
    if (!lc_pouch_index_temporal_decode_require(src_size, offset, field_len)) {
      lc_pouch_index_temporal_posting_table_cleanup(allocator, &decoded);
      return 0;
    }
    field_name = (char *)lc_pouch_alloc(allocator, (size_t)field_len + 1U);
    if (field_name == NULL) {
      lc_pouch_index_temporal_posting_table_cleanup(allocator, &decoded);
      return 0;
    }
    memcpy(field_name, src + offset, field_len);
    field_name[field_len] = '\0';
    offset += field_len;
    if (!lc_pouch_index_temporal_posting_table_mark_field(allocator, &decoded,
                                                          field_name)) {
      lc_pouch_free(allocator, field_name);
      lc_pouch_index_temporal_posting_table_cleanup(allocator, &decoded);
      return 0;
    }
    for (value_index = 0U; value_index < value_count; ++value_index) {
      int64_t unix_seconds;
      int32_t nanosecond;
      uint64_t encoded_seconds;
      uint32_t raw_nanosecond;
      uint32_t doc_id;

      if (!lc_pouch_index_temporal_decode_require(
              src_size, offset, LC_POUCH_INDEX_TEMPORAL_VALUE_SIZE)) {
        lc_pouch_free(allocator, field_name);
        lc_pouch_index_temporal_posting_table_cleanup(allocator, &decoded);
        return 0;
      }
      encoded_seconds = lc_pouch_index_temporal_get_u64(src + offset);
      offset += 8U;
      raw_nanosecond = lc_pouch_index_temporal_get_u32(src + offset);
      offset += 4U;
      doc_id = lc_pouch_index_temporal_get_u32(src + offset);
      offset += 4U;
      if (raw_nanosecond > 999999999U ||
          !lc_pouch_index_temporal_unzigzag_i64(encoded_seconds,
                                                &unix_seconds)) {
        lc_pouch_free(allocator, field_name);
        lc_pouch_index_temporal_posting_table_cleanup(allocator, &decoded);
        return 0;
      }
      nanosecond = (int32_t)raw_nanosecond;
      if (!lc_pouch_index_temporal_posting_table_add_value_doc_id(
              allocator, &decoded, field_name, unix_seconds, nanosecond,
              doc_id)) {
        lc_pouch_free(allocator, field_name);
        lc_pouch_index_temporal_posting_table_cleanup(allocator, &decoded);
        return 0;
      }
    }
    for (value_index = 0U; value_index < residual_count; ++value_index) {
      uint32_t doc_id;

      if (!lc_pouch_index_temporal_decode_require(src_size, offset, 4U)) {
        lc_pouch_free(allocator, field_name);
        lc_pouch_index_temporal_posting_table_cleanup(allocator, &decoded);
        return 0;
      }
      doc_id = lc_pouch_index_temporal_get_u32(src + offset);
      offset += 4U;
      if (!lc_pouch_index_temporal_posting_table_add_residual_doc_id(
              allocator, &decoded, field_name, doc_id)) {
        lc_pouch_free(allocator, field_name);
        lc_pouch_index_temporal_posting_table_cleanup(allocator, &decoded);
        return 0;
      }
    }
    lc_pouch_free(allocator, field_name);
  }
  if (offset != src_size ||
      !lc_pouch_index_temporal_posting_table_build_postings(allocator,
                                                            &decoded)) {
    lc_pouch_index_temporal_posting_table_cleanup(allocator, &decoded);
    return 0;
  }
  lc_pouch_index_temporal_posting_table_cleanup(allocator, table);
  *table = decoded;
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

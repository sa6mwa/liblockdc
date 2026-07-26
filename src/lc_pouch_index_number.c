#include "lc_pouch_index.h"

#include "lc_pouch_number.h"

#include <stdlib.h>
#include <string.h>

#define LC_POUCH_INDEX_NUMBER_MAGIC_LEN 8U
#define LC_POUCH_INDEX_NUMBER_HEADER_SIZE 16U
#define LC_POUCH_INDEX_NUMBER_FIELD_FIXED_SIZE 12U
#define LC_POUCH_INDEX_NUMBER_VALUE_FIXED_SIZE 8U
#define LC_POUCH_INDEX_NUMBER_MAGIC "LCPNUM1\0"
#define LC_POUCH_INDEX_NUMBER_GENERATION_MAGIC "LCPNGN1\0"
#define LC_POUCH_INDEX_NUMBER_GENERATION_HEADER_SIZE 40U

static int lc_pouch_index_number_key_valid(const char *number) {
  int cmp;

  return number != NULL && lc_pouch_number_eq_key_compare(number, number, &cmp);
}

static int lc_pouch_index_number_doc_compare_value(const char *left_number,
                                                   uint32_t left_doc_id,
                                                   const char *right_number,
                                                   uint32_t right_doc_id) {
  int cmp;

  if (!lc_pouch_number_eq_key_compare(left_number, right_number, &cmp)) {
    return strcmp(left_number, right_number);
  }
  if (cmp != 0) {
    return cmp;
  }
  if (left_doc_id < right_doc_id) {
    return -1;
  }
  if (left_doc_id > right_doc_id) {
    return 1;
  }
  return 0;
}

static int lc_pouch_index_number_doc_qsort_compare(const void *left,
                                                   const void *right) {
  const lc_pouch_index_number_doc_entry *a;
  const lc_pouch_index_number_doc_entry *b;

  a = (const lc_pouch_index_number_doc_entry *)left;
  b = (const lc_pouch_index_number_doc_entry *)right;
  return lc_pouch_index_number_doc_compare_value(a->number, a->doc_id,
                                                 b->number, b->doc_id);
}

static int lc_pouch_index_number_field_find_position(
    const lc_pouch_index_number_posting_table *table, const char *field,
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

static int
lc_pouch_index_number_fields_reserve(const lc_pouch_allocator *allocator,
                                     lc_pouch_index_number_posting_table *table,
                                     size_t needed) {
  size_t new_capacity;
  lc_pouch_index_number_field_entry *grown;

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
  grown = (lc_pouch_index_number_field_entry *)lc_pouch_realloc(
      allocator, table->fields, new_capacity * sizeof(table->fields[0]));
  if (grown == NULL) {
    return 0;
  }
  table->fields = grown;
  table->field_capacity = new_capacity;
  return 1;
}

static int
lc_pouch_index_number_values_reserve(const lc_pouch_allocator *allocator,
                                     lc_pouch_index_number_field_entry *field,
                                     size_t needed) {
  size_t new_capacity;
  lc_pouch_index_number_doc_entry *grown;

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
  grown = (lc_pouch_index_number_doc_entry *)lc_pouch_realloc(
      allocator, field->values, new_capacity * sizeof(field->values[0]));
  if (grown == NULL) {
    return 0;
  }
  field->values = grown;
  field->value_capacity = new_capacity;
  return 1;
}

void lc_pouch_index_number_posting_table_cleanup(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_number_posting_table *table) {
  size_t field_index;

  if (table == NULL) {
    return;
  }
  for (field_index = 0U; field_index < table->field_count; ++field_index) {
    size_t value_index;

    lc_pouch_free(allocator, table->fields[field_index].field);
    for (value_index = 0U; value_index < table->fields[field_index].value_count;
         ++value_index) {
      lc_pouch_free(allocator,
                    table->fields[field_index].values[value_index].number);
    }
    lc_pouch_free(allocator, table->fields[field_index].values);
    lc_pouch_index_doc_id_set_cleanup(allocator,
                                      &table->fields[field_index].doc_ids);
    lc_pouch_index_posting_cleanup(allocator,
                                   &table->fields[field_index].posting);
  }
  lc_pouch_free(allocator, table->fields);
  memset(table, 0, sizeof(*table));
}

int lc_pouch_index_number_posting_table_mark_field(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_number_posting_table *table, const char *field) {
  lc_pouch_index_number_field_entry entry;
  size_t position;

  if (table == NULL || field == NULL) {
    return 0;
  }
  if (lc_pouch_index_number_field_find_position(table, field, &position)) {
    return 1;
  }
  memset(&entry, 0, sizeof(entry));
  entry.field = lc_pouch_strdup(allocator, field);
  entry.values_sorted = 1;
  if (entry.field == NULL) {
    return 0;
  }
  if (!lc_pouch_index_number_fields_reserve(allocator, table,
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

int lc_pouch_index_number_posting_table_has_field(
    const lc_pouch_index_number_posting_table *table, const char *field) {
  return lc_pouch_index_number_field_find_position(table, field, NULL);
}

int lc_pouch_index_number_posting_table_add_value_doc_id(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_number_posting_table *table, const char *field,
    const char *number, lc_pouch_index_doc_id doc_id) {
  lc_pouch_index_number_field_entry *entry;
  char *number_copy;
  size_t position;

  if (table == NULL || field == NULL ||
      !lc_pouch_index_number_key_valid(number)) {
    return 0;
  }
  if (!lc_pouch_index_number_posting_table_mark_field(allocator, table,
                                                      field) ||
      !lc_pouch_index_number_field_find_position(table, field, &position)) {
    return 0;
  }
  entry = &table->fields[position];
  if (!lc_pouch_index_number_values_reserve(allocator, entry,
                                            entry->value_count + 1U)) {
    return 0;
  }
  number_copy = lc_pouch_strdup(allocator, number);
  if (number_copy == NULL) {
    return 0;
  }
  entry->values[entry->value_count].number = number_copy;
  entry->values[entry->value_count].doc_id = doc_id;
  entry->value_count++;
  entry->values_sorted = 0;
  return 1;
}

int lc_pouch_index_number_posting_table_add_residual_doc_id(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_number_posting_table *table, const char *field,
    lc_pouch_index_doc_id doc_id) {
  lc_pouch_index_number_field_entry *entry;
  size_t position;

  if (table == NULL || field == NULL) {
    return 0;
  }
  if (!lc_pouch_index_number_posting_table_mark_field(allocator, table,
                                                      field) ||
      !lc_pouch_index_number_field_find_position(table, field, &position)) {
    return 0;
  }
  entry = &table->fields[position];
  entry->posting_ready = 0;
  return lc_pouch_index_doc_id_set_append(allocator, &entry->doc_ids, doc_id);
}

static void lc_pouch_index_number_field_sort_values(
    lc_pouch_index_number_field_entry *field,
    const lc_pouch_allocator *allocator) {
  size_t index;
  size_t out;

  if (field == NULL || field->values_sorted) {
    return;
  }
  if (field->value_count > 1U) {
    qsort(field->values, field->value_count, sizeof(field->values[0]),
          lc_pouch_index_number_doc_qsort_compare);
  }
  out = 0U;
  for (index = 0U; index < field->value_count; ++index) {
    if (out > 0U &&
        lc_pouch_index_number_doc_compare_value(
            field->values[out - 1U].number, field->values[out - 1U].doc_id,
            field->values[index].number, field->values[index].doc_id) == 0) {
      lc_pouch_free(allocator, field->values[index].number);
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

int lc_pouch_index_number_posting_table_build_postings(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_number_posting_table *table) {
  size_t index;

  if (table == NULL) {
    return 0;
  }
  for (index = 0U; index < table->field_count; ++index) {
    lc_pouch_index_number_field_sort_values(&table->fields[index], allocator);
    if (table->fields[index].posting_ready) {
      continue;
    }
    lc_pouch_index_posting_cleanup(allocator, &table->fields[index].posting);
    if (!lc_pouch_index_doc_id_set_sort_unique(&table->fields[index].doc_ids)) {
      return 0;
    }
    if (table->fields[index].doc_ids.count > 1U) {
      if (!lc_pouch_index_posting_build(allocator,
                                        &table->fields[index].posting,
                                        table->fields[index].doc_ids.items,
                                        table->fields[index].doc_ids.count)) {
        return 0;
      }
      table->fields[index].posting_ready = 1;
    }
  }
  return 1;
}

static size_t lc_pouch_index_number_field_lower_bound(
    const lc_pouch_index_number_field_entry *field, const char *number,
    int exclusive) {
  size_t low;
  size_t high;

  low = 0U;
  high = field != NULL ? field->value_count : 0U;
  while (low < high) {
    size_t mid;
    int cmp;

    mid = low + ((high - low) / 2U);
    if (!lc_pouch_number_eq_key_compare(field->values[mid].number, number,
                                        &cmp)) {
      return high;
    }
    if (cmp < 0 || (exclusive && cmp == 0)) {
      low = mid + 1U;
    } else {
      high = mid;
    }
  }
  return low;
}

static size_t lc_pouch_index_number_field_upper_bound(
    const lc_pouch_index_number_field_entry *field, const char *number,
    int inclusive) {
  size_t low;
  size_t high;

  low = 0U;
  high = field != NULL ? field->value_count : 0U;
  while (low < high) {
    size_t mid;
    int cmp;

    mid = low + ((high - low) / 2U);
    if (!lc_pouch_number_eq_key_compare(field->values[mid].number, number,
                                        &cmp)) {
      return low;
    }
    if (cmp < 0 || (inclusive && cmp == 0)) {
      low = mid + 1U;
    } else {
      high = mid;
    }
  }
  return low;
}

int lc_pouch_index_number_posting_table_append_range(
    const lc_pouch_allocator *allocator,
    const lc_pouch_index_number_posting_table *table, const char *field,
    const char *gt, const char *gte, const char *lt, const char *lte,
    lc_pouch_index_doc_id_set *dst) {
  const lc_pouch_index_number_field_entry *entry;
  const char *lower;
  const char *upper;
  size_t position;
  size_t start;
  size_t end;
  size_t index;
  int lower_exclusive;
  int upper_inclusive;

  if (table == NULL || field == NULL || dst == NULL) {
    return 0;
  }
  if ((gt != NULL && !lc_pouch_index_number_key_valid(gt)) ||
      (gte != NULL && !lc_pouch_index_number_key_valid(gte)) ||
      (lt != NULL && !lc_pouch_index_number_key_valid(lt)) ||
      (lte != NULL && !lc_pouch_index_number_key_valid(lte))) {
    return 0;
  }
  if (!lc_pouch_index_number_field_find_position(table, field, &position)) {
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
  lower = NULL;
  lower_exclusive = 0;
  if (gt != NULL) {
    lower = gt;
    lower_exclusive = 1;
  }
  if (gte != NULL) {
    int cmp;

    if (lower == NULL) {
      lower = gte;
      lower_exclusive = 0;
    } else if (!lc_pouch_number_eq_key_compare(gte, lower, &cmp)) {
      return 0;
    } else if (cmp > 0) {
      lower = gte;
      lower_exclusive = 0;
    }
  }
  upper = NULL;
  upper_inclusive = 0;
  if (lt != NULL) {
    upper = lt;
    upper_inclusive = 0;
  }
  if (lte != NULL) {
    int cmp;

    if (upper == NULL) {
      upper = lte;
      upper_inclusive = 1;
    } else if (!lc_pouch_number_eq_key_compare(lte, upper, &cmp)) {
      return 0;
    } else if (cmp < 0) {
      upper = lte;
      upper_inclusive = 1;
    }
  }
  start = lower != NULL ? lc_pouch_index_number_field_lower_bound(
                              entry, lower, lower_exclusive)
                        : 0U;
  end = upper != NULL ? lc_pouch_index_number_field_upper_bound(entry, upper,
                                                                upper_inclusive)
                      : entry->value_count;
  if (end < start) {
    end = start;
  }
  for (index = start; index < end; ++index) {
    if (!lc_pouch_index_doc_id_set_append(allocator, dst,
                                          entry->values[index].doc_id)) {
      return 0;
    }
  }
  return 1;
}

static int lc_pouch_index_number_add_size(size_t *total, size_t add) {
  if (total == NULL || *total > ((size_t)-1) - add) {
    return 0;
  }
  *total += add;
  return 1;
}

static void lc_pouch_index_number_put_u32(unsigned char *dst, uint32_t value) {
  dst[0] = (unsigned char)(value & 0xffU);
  dst[1] = (unsigned char)((value >> 8U) & 0xffU);
  dst[2] = (unsigned char)((value >> 16U) & 0xffU);
  dst[3] = (unsigned char)((value >> 24U) & 0xffU);
}

static void lc_pouch_index_number_put_u64(unsigned char *dst, uint64_t value) {
  size_t index;

  for (index = 0U; index < 8U; ++index) {
    dst[index] = (unsigned char)((value >> (index * 8U)) & UINT64_C(0xff));
  }
}

static uint32_t lc_pouch_index_number_get_u32(const unsigned char *src) {
  return ((uint32_t)src[0]) | (((uint32_t)src[1]) << 8U) |
         (((uint32_t)src[2]) << 16U) | (((uint32_t)src[3]) << 24U);
}

static uint64_t lc_pouch_index_number_get_u64(const unsigned char *src) {
  uint64_t value;
  size_t index;

  value = 0U;
  for (index = 0U; index < 8U; ++index) {
    value |= ((uint64_t)src[index]) << (index * 8U);
  }
  return value;
}

int lc_pouch_index_number_posting_table_encoded_size(
    const lc_pouch_index_number_posting_table *table, size_t *size_out) {
  size_t total;
  size_t index;

  if (size_out != NULL) {
    *size_out = 0U;
  }
  if (table == NULL || size_out == NULL || table->field_count > UINT32_MAX) {
    return 0;
  }
  total = LC_POUCH_INDEX_NUMBER_HEADER_SIZE;
  for (index = 0U; index < table->field_count; ++index) {
    const lc_pouch_index_number_field_entry *field;
    size_t field_len;
    size_t value_index;
    size_t residual_bytes;

    field = &table->fields[index];
    if (field->field == NULL || field->value_count > UINT32_MAX ||
        field->doc_ids.count > UINT32_MAX || !field->values_sorted) {
      return 0;
    }
    field_len = strlen(field->field);
    if (field_len > UINT32_MAX ||
        field->doc_ids.count > ((size_t)-1) / sizeof(uint32_t)) {
      return 0;
    }
    if (!lc_pouch_index_number_add_size(
            &total, LC_POUCH_INDEX_NUMBER_FIELD_FIXED_SIZE) ||
        !lc_pouch_index_number_add_size(&total, field_len)) {
      return 0;
    }
    for (value_index = 0U; value_index < field->value_count; ++value_index) {
      size_t number_len;

      if (!lc_pouch_index_number_key_valid(field->values[value_index].number)) {
        return 0;
      }
      number_len = strlen(field->values[value_index].number);
      if (number_len > UINT32_MAX ||
          !lc_pouch_index_number_add_size(
              &total, LC_POUCH_INDEX_NUMBER_VALUE_FIXED_SIZE) ||
          !lc_pouch_index_number_add_size(&total, number_len)) {
        return 0;
      }
    }
    residual_bytes = field->doc_ids.count * sizeof(uint32_t);
    if (!lc_pouch_index_number_add_size(&total, residual_bytes)) {
      return 0;
    }
  }
  *size_out = total;
  return 1;
}

int lc_pouch_index_number_posting_table_encode(
    const lc_pouch_index_number_posting_table *table, unsigned char *dst,
    size_t dst_size, size_t *written_out) {
  unsigned char *cursor;
  size_t needed;
  size_t index;

  if (written_out != NULL) {
    *written_out = 0U;
  }
  if (table == NULL || dst == NULL || written_out == NULL ||
      !lc_pouch_index_number_posting_table_encoded_size(table, &needed) ||
      dst_size < needed) {
    return 0;
  }
  cursor = dst;
  memcpy(cursor, LC_POUCH_INDEX_NUMBER_MAGIC, LC_POUCH_INDEX_NUMBER_MAGIC_LEN);
  cursor += LC_POUCH_INDEX_NUMBER_MAGIC_LEN;
  lc_pouch_index_number_put_u32(cursor, 1U);
  cursor += 4U;
  lc_pouch_index_number_put_u32(cursor, (uint32_t)table->field_count);
  cursor += 4U;
  for (index = 0U; index < table->field_count; ++index) {
    const lc_pouch_index_number_field_entry *field;
    size_t value_index;
    size_t field_len;

    field = &table->fields[index];
    field_len = strlen(field->field);
    lc_pouch_index_number_put_u32(cursor, (uint32_t)field_len);
    cursor += 4U;
    lc_pouch_index_number_put_u32(cursor, (uint32_t)field->value_count);
    cursor += 4U;
    lc_pouch_index_number_put_u32(cursor, (uint32_t)field->doc_ids.count);
    cursor += 4U;
    memcpy(cursor, field->field, field_len);
    cursor += field_len;
    for (value_index = 0U; value_index < field->value_count; ++value_index) {
      size_t number_len;

      number_len = strlen(field->values[value_index].number);
      lc_pouch_index_number_put_u32(cursor, (uint32_t)number_len);
      cursor += 4U;
      lc_pouch_index_number_put_u32(cursor, field->values[value_index].doc_id);
      cursor += 4U;
      memcpy(cursor, field->values[value_index].number, number_len);
      cursor += number_len;
    }
    for (value_index = 0U; value_index < field->doc_ids.count; ++value_index) {
      lc_pouch_index_number_put_u32(cursor, field->doc_ids.items[value_index]);
      cursor += 4U;
    }
  }
  *written_out = (size_t)(cursor - dst);
  return *written_out == needed;
}

static int lc_pouch_index_number_decode_require(size_t src_size, size_t offset,
                                                size_t needed) {
  return offset <= src_size && needed <= src_size - offset;
}

int lc_pouch_index_number_posting_table_decode(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_number_posting_table *table, const unsigned char *src,
    size_t src_size) {
  lc_pouch_index_number_posting_table decoded;
  size_t offset;
  uint32_t version;
  uint32_t field_count;
  uint32_t field_index;

  if (table == NULL || src == NULL ||
      !lc_pouch_index_number_decode_require(
          src_size, 0U, LC_POUCH_INDEX_NUMBER_HEADER_SIZE) ||
      memcmp(src, LC_POUCH_INDEX_NUMBER_MAGIC,
             LC_POUCH_INDEX_NUMBER_MAGIC_LEN) != 0) {
    return 0;
  }
  offset = LC_POUCH_INDEX_NUMBER_MAGIC_LEN;
  version = lc_pouch_index_number_get_u32(src + offset);
  offset += 4U;
  if (version != 1U) {
    return 0;
  }
  field_count = lc_pouch_index_number_get_u32(src + offset);
  offset += 4U;
  memset(&decoded, 0, sizeof(decoded));
  for (field_index = 0U; field_index < field_count; ++field_index) {
    char *field_name;
    uint32_t field_len;
    uint32_t value_count;
    uint32_t residual_count;
    uint32_t value_index;

    if (!lc_pouch_index_number_decode_require(
            src_size, offset, LC_POUCH_INDEX_NUMBER_FIELD_FIXED_SIZE)) {
      lc_pouch_index_number_posting_table_cleanup(allocator, &decoded);
      return 0;
    }
    field_len = lc_pouch_index_number_get_u32(src + offset);
    offset += 4U;
    value_count = lc_pouch_index_number_get_u32(src + offset);
    offset += 4U;
    residual_count = lc_pouch_index_number_get_u32(src + offset);
    offset += 4U;
    if (!lc_pouch_index_number_decode_require(src_size, offset, field_len)) {
      lc_pouch_index_number_posting_table_cleanup(allocator, &decoded);
      return 0;
    }
    field_name = (char *)lc_pouch_alloc(allocator, (size_t)field_len + 1U);
    if (field_name == NULL) {
      lc_pouch_index_number_posting_table_cleanup(allocator, &decoded);
      return 0;
    }
    memcpy(field_name, src + offset, field_len);
    field_name[field_len] = '\0';
    offset += field_len;
    if (!lc_pouch_index_number_posting_table_mark_field(allocator, &decoded,
                                                        field_name)) {
      lc_pouch_free(allocator, field_name);
      lc_pouch_index_number_posting_table_cleanup(allocator, &decoded);
      return 0;
    }
    for (value_index = 0U; value_index < value_count; ++value_index) {
      char *number;
      uint32_t number_len;
      uint32_t doc_id;

      if (!lc_pouch_index_number_decode_require(
              src_size, offset, LC_POUCH_INDEX_NUMBER_VALUE_FIXED_SIZE)) {
        lc_pouch_free(allocator, field_name);
        lc_pouch_index_number_posting_table_cleanup(allocator, &decoded);
        return 0;
      }
      number_len = lc_pouch_index_number_get_u32(src + offset);
      offset += 4U;
      doc_id = lc_pouch_index_number_get_u32(src + offset);
      offset += 4U;
      if (!lc_pouch_index_number_decode_require(src_size, offset, number_len)) {
        lc_pouch_free(allocator, field_name);
        lc_pouch_index_number_posting_table_cleanup(allocator, &decoded);
        return 0;
      }
      number = (char *)lc_pouch_alloc(allocator, (size_t)number_len + 1U);
      if (number == NULL) {
        lc_pouch_free(allocator, field_name);
        lc_pouch_index_number_posting_table_cleanup(allocator, &decoded);
        return 0;
      }
      memcpy(number, src + offset, number_len);
      number[number_len] = '\0';
      offset += number_len;
      if (!lc_pouch_index_number_posting_table_add_value_doc_id(
              allocator, &decoded, field_name, number, doc_id)) {
        lc_pouch_free(allocator, number);
        lc_pouch_free(allocator, field_name);
        lc_pouch_index_number_posting_table_cleanup(allocator, &decoded);
        return 0;
      }
      lc_pouch_free(allocator, number);
    }
    for (value_index = 0U; value_index < residual_count; ++value_index) {
      uint32_t doc_id;

      if (!lc_pouch_index_number_decode_require(src_size, offset, 4U)) {
        lc_pouch_free(allocator, field_name);
        lc_pouch_index_number_posting_table_cleanup(allocator, &decoded);
        return 0;
      }
      doc_id = lc_pouch_index_number_get_u32(src + offset);
      offset += 4U;
      if (!lc_pouch_index_number_posting_table_add_residual_doc_id(
              allocator, &decoded, field_name, doc_id)) {
        lc_pouch_free(allocator, field_name);
        lc_pouch_index_number_posting_table_cleanup(allocator, &decoded);
        return 0;
      }
    }
    lc_pouch_free(allocator, field_name);
  }
  if (offset != src_size || !lc_pouch_index_number_posting_table_build_postings(
                                allocator, &decoded)) {
    lc_pouch_index_number_posting_table_cleanup(allocator, &decoded);
    return 0;
  }
  lc_pouch_index_number_posting_table_cleanup(allocator, table);
  *table = decoded;
  return 1;
}

void lc_pouch_index_number_generation_cleanup(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_number_generation *generation) {
  if (generation == NULL) {
    return;
  }
  lc_pouch_free(allocator, generation->namespace_name);
  lc_pouch_index_number_posting_table_cleanup(allocator, &generation->postings);
  memset(generation, 0, sizeof(*generation));
}

int lc_pouch_index_number_generation_encoded_size(
    const lc_pouch_index_number_generation *generation, size_t *size_out) {
  size_t total;
  size_t namespace_len;
  size_t table_size;

  if (size_out != NULL) {
    *size_out = 0U;
  }
  if (generation == NULL || generation->namespace_name == NULL ||
      size_out == NULL ||
      !lc_pouch_index_number_posting_table_encoded_size(&generation->postings,
                                                        &table_size)) {
    return 0;
  }
  namespace_len = strlen(generation->namespace_name);
  if (namespace_len > UINT32_MAX) {
    return 0;
  }
  total = LC_POUCH_INDEX_NUMBER_GENERATION_HEADER_SIZE;
  if (!lc_pouch_index_number_add_size(&total, namespace_len) ||
      !lc_pouch_index_number_add_size(&total, table_size)) {
    return 0;
  }
  *size_out = total;
  return 1;
}

int lc_pouch_index_number_generation_encode(
    const lc_pouch_index_number_generation *generation, unsigned char *dst,
    size_t dst_size, size_t *written_out) {
  unsigned char *cursor;
  size_t needed;
  size_t namespace_len;
  size_t table_size;
  size_t table_written;

  if (written_out != NULL) {
    *written_out = 0U;
  }
  if (generation == NULL || generation->namespace_name == NULL || dst == NULL ||
      written_out == NULL ||
      !lc_pouch_index_number_generation_encoded_size(generation, &needed) ||
      dst_size < needed ||
      !lc_pouch_index_number_posting_table_encoded_size(&generation->postings,
                                                        &table_size)) {
    return 0;
  }
  namespace_len = strlen(generation->namespace_name);
  cursor = dst;
  memcpy(cursor, LC_POUCH_INDEX_NUMBER_GENERATION_MAGIC,
         LC_POUCH_INDEX_NUMBER_MAGIC_LEN);
  cursor += LC_POUCH_INDEX_NUMBER_MAGIC_LEN;
  lc_pouch_index_number_put_u32(cursor, 1U);
  cursor += 4U;
  lc_pouch_index_number_put_u64(cursor, generation->identity.sequence);
  cursor += 8U;
  lc_pouch_index_number_put_u64(cursor,
                                generation->identity.manifest_generation);
  cursor += 8U;
  lc_pouch_index_number_put_u32(cursor, (uint32_t)namespace_len);
  cursor += 4U;
  lc_pouch_index_number_put_u64(cursor, (uint64_t)table_size);
  cursor += 8U;
  memcpy(cursor, generation->namespace_name, namespace_len);
  cursor += namespace_len;
  if (!lc_pouch_index_number_posting_table_encode(
          &generation->postings, cursor, dst_size - (size_t)(cursor - dst),
          &table_written) ||
      table_written != table_size) {
    return 0;
  }
  cursor += table_written;
  *written_out = (size_t)(cursor - dst);
  return *written_out == needed;
}

int lc_pouch_index_number_generation_decode(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_number_generation *generation, const unsigned char *src,
    size_t src_size) {
  lc_pouch_index_number_generation decoded;
  size_t offset;
  uint32_t version;
  uint32_t namespace_len;
  uint64_t table_size_u64;

  if (generation == NULL || src == NULL ||
      !lc_pouch_index_number_decode_require(
          src_size, 0U, LC_POUCH_INDEX_NUMBER_GENERATION_HEADER_SIZE) ||
      memcmp(src, LC_POUCH_INDEX_NUMBER_GENERATION_MAGIC,
             LC_POUCH_INDEX_NUMBER_MAGIC_LEN) != 0) {
    return 0;
  }
  offset = LC_POUCH_INDEX_NUMBER_MAGIC_LEN;
  version = lc_pouch_index_number_get_u32(src + offset);
  offset += 4U;
  if (version != 1U) {
    return 0;
  }
  memset(&decoded, 0, sizeof(decoded));
  decoded.identity.sequence = lc_pouch_index_number_get_u64(src + offset);
  offset += 8U;
  decoded.identity.manifest_generation =
      lc_pouch_index_number_get_u64(src + offset);
  offset += 8U;
  namespace_len = lc_pouch_index_number_get_u32(src + offset);
  offset += 4U;
  table_size_u64 = lc_pouch_index_number_get_u64(src + offset);
  offset += 8U;
  if (table_size_u64 > (uint64_t)((size_t)-1) ||
      !lc_pouch_index_number_decode_require(src_size, offset, namespace_len)) {
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
  if (!lc_pouch_index_number_decode_require(src_size, offset,
                                            (size_t)table_size_u64) ||
      offset + (size_t)table_size_u64 != src_size ||
      !lc_pouch_index_number_posting_table_decode(
          allocator, &decoded.postings, src + offset, (size_t)table_size_u64)) {
    lc_pouch_index_number_generation_cleanup(allocator, &decoded);
    return 0;
  }
  lc_pouch_index_number_generation_cleanup(allocator, generation);
  *generation = decoded;
  return 1;
}

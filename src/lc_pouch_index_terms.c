#include "lc_pouch_index.h"

#include <limits.h>
#include <string.h>

#define LC_POUCH_INDEX_TERM_MAGIC_LEN 8U
#define LC_POUCH_INDEX_TERM_TABLE_MAGIC "LCPTTM1\0"
#define LC_POUCH_INDEX_TERM_GENERATION_MAGIC "LCPTTG1\0"
#define LC_POUCH_INDEX_TERM_TABLE_HEADER_SIZE 24U
#define LC_POUCH_INDEX_TERM_ENTRY_FIXED_SIZE 12U
#define LC_POUCH_INDEX_TERM_POSTING_FIXED_SIZE 24U
#define LC_POUCH_INDEX_TERM_GENERATION_HEADER_SIZE 40U

static int lc_pouch_index_term_add_size(size_t *total, size_t add) {
  if (total == NULL || *total > ((size_t)-1) - add) {
    return 0;
  }
  *total += add;
  return 1;
}

static void lc_pouch_index_term_put_u32(unsigned char *dst, uint32_t value) {
  dst[0] = (unsigned char)(value & UINT32_C(0xff));
  dst[1] = (unsigned char)((value >> 8U) & UINT32_C(0xff));
  dst[2] = (unsigned char)((value >> 16U) & UINT32_C(0xff));
  dst[3] = (unsigned char)((value >> 24U) & UINT32_C(0xff));
}

static void lc_pouch_index_term_put_u64(unsigned char *dst, uint64_t value) {
  size_t index;

  for (index = 0U; index < 8U; ++index) {
    dst[index] = (unsigned char)((value >> (index * 8U)) & UINT64_C(0xff));
  }
}

static uint32_t lc_pouch_index_term_get_u32(const unsigned char *src) {
  return ((uint32_t)src[0]) | (((uint32_t)src[1]) << 8U) |
         (((uint32_t)src[2]) << 16U) | (((uint32_t)src[3]) << 24U);
}

static uint64_t lc_pouch_index_term_get_u64(const unsigned char *src) {
  uint64_t value;
  size_t index;

  value = 0U;
  for (index = 0U; index < 8U; ++index) {
    value |= ((uint64_t)src[index]) << (index * 8U);
  }
  return value;
}

static int lc_pouch_index_term_decode_require(size_t src_size, size_t offset,
                                              size_t needed) {
  return offset <= src_size && needed <= src_size - offset;
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

int lc_pouch_index_term_posting_table_append(
    const lc_pouch_allocator *allocator,
    const lc_pouch_index_term_posting_table *table,
    lc_pouch_index_term_id term_id, lc_pouch_index_doc_id_set *dst) {
  size_t position;

  if (dst == NULL) {
    return 0;
  }
  if (!lc_pouch_index_term_posting_table_find_position(table, term_id,
                                                       &position)) {
    return 1;
  }
  return lc_pouch_index_posting_append(allocator,
                                       &table->entries[position].posting, dst);
}

static int
lc_pouch_index_term_table_contains_id(const lc_pouch_index_term_table *table,
                                      lc_pouch_index_term_id term_id) {
  size_t index;

  if (table == NULL) {
    return 0;
  }
  for (index = 0U; index < table->count; ++index) {
    if (table->entries[index].id == term_id) {
      return 1;
    }
  }
  return 0;
}

static int lc_pouch_index_term_table_add_decoded(
    const lc_pouch_allocator *allocator, lc_pouch_index_term_table *table,
    lc_pouch_index_term_id term_id, const char *field, const char *value) {
  lc_pouch_index_term_entry entry;
  size_t position;

  if (table == NULL || field == NULL || value == NULL ||
      lc_pouch_index_term_table_contains_id(table, term_id) ||
      lc_pouch_index_term_table_find_position(table, field, value, &position)) {
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
  entry.id = term_id;
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
  return 1;
}

static int
lc_pouch_index_posting_payload_size(const lc_pouch_index_posting *posting,
                                    size_t *size_out) {
  if (size_out != NULL) {
    *size_out = 0U;
  }
  if (posting == NULL || size_out == NULL) {
    return 0;
  }
  if (posting->count > UINT32_MAX) {
    return 0;
  }
  if (posting->encoding == LC_POUCH_INDEX_POSTING_EMPTY) {
    if (posting->count != 0U) {
      return 0;
    }
    *size_out = 0U;
    return 1;
  }
  if (posting->encoding == LC_POUCH_INDEX_POSTING_SPARSE) {
    if (posting->count == 0U || posting->sparse == NULL) {
      return 0;
    }
    *size_out = posting->sparse_len;
    return 1;
  }
  if (posting->encoding == LC_POUCH_INDEX_POSTING_DENSE) {
    if (posting->count == 0U || posting->dense == NULL ||
        posting->dense_word_count > ((size_t)-1) / sizeof(uint64_t)) {
      return 0;
    }
    *size_out = posting->dense_word_count * sizeof(uint64_t);
    return 1;
  }
  return 0;
}

static int lc_pouch_index_term_generation_table_encoded_size(
    const lc_pouch_index_term_generation *generation, size_t *size_out) {
  size_t total;
  size_t index;

  if (size_out != NULL) {
    *size_out = 0U;
  }
  if (generation == NULL || size_out == NULL ||
      generation->terms.count > UINT32_MAX ||
      generation->postings.count > UINT32_MAX) {
    return 0;
  }
  total = LC_POUCH_INDEX_TERM_TABLE_HEADER_SIZE;
  for (index = 0U; index < generation->terms.count; ++index) {
    const lc_pouch_index_term_entry *term;
    size_t field_len;
    size_t value_len;

    term = &generation->terms.entries[index];
    if (term->field == NULL || term->value == NULL) {
      return 0;
    }
    field_len = strlen(term->field);
    value_len = strlen(term->value);
    if (field_len > UINT32_MAX || value_len > UINT32_MAX ||
        !lc_pouch_index_term_add_size(&total,
                                      LC_POUCH_INDEX_TERM_ENTRY_FIXED_SIZE) ||
        !lc_pouch_index_term_add_size(&total, field_len) ||
        !lc_pouch_index_term_add_size(&total, value_len)) {
      return 0;
    }
  }
  for (index = 0U; index < generation->postings.count; ++index) {
    const lc_pouch_index_term_posting_entry *entry;
    size_t payload_size;

    entry = &generation->postings.entries[index];
    if (!lc_pouch_index_term_table_contains_id(&generation->terms,
                                               entry->term_id) ||
        !lc_pouch_index_posting_payload_size(&entry->posting, &payload_size) ||
        !lc_pouch_index_term_add_size(&total,
                                      LC_POUCH_INDEX_TERM_POSTING_FIXED_SIZE) ||
        !lc_pouch_index_term_add_size(&total, payload_size)) {
      return 0;
    }
  }
  *size_out = total;
  return 1;
}

static int lc_pouch_index_term_generation_table_encode(
    const lc_pouch_index_term_generation *generation, unsigned char *dst,
    size_t dst_size, size_t *written_out) {
  unsigned char *cursor;
  size_t needed;
  size_t index;

  if (written_out != NULL) {
    *written_out = 0U;
  }
  if (generation == NULL || dst == NULL || written_out == NULL ||
      !lc_pouch_index_term_generation_table_encoded_size(generation, &needed) ||
      dst_size < needed) {
    return 0;
  }
  cursor = dst;
  memcpy(cursor, LC_POUCH_INDEX_TERM_TABLE_MAGIC,
         LC_POUCH_INDEX_TERM_MAGIC_LEN);
  cursor += LC_POUCH_INDEX_TERM_MAGIC_LEN;
  lc_pouch_index_term_put_u32(cursor, 1U);
  cursor += 4U;
  lc_pouch_index_term_put_u32(cursor, (uint32_t)generation->terms.count);
  cursor += 4U;
  lc_pouch_index_term_put_u32(cursor, (uint32_t)generation->postings.count);
  cursor += 4U;
  lc_pouch_index_term_put_u32(cursor, generation->terms.next_id);
  cursor += 4U;
  for (index = 0U; index < generation->terms.count; ++index) {
    const lc_pouch_index_term_entry *term;
    size_t field_len;
    size_t value_len;

    term = &generation->terms.entries[index];
    field_len = strlen(term->field);
    value_len = strlen(term->value);
    lc_pouch_index_term_put_u32(cursor, term->id);
    cursor += 4U;
    lc_pouch_index_term_put_u32(cursor, (uint32_t)field_len);
    cursor += 4U;
    lc_pouch_index_term_put_u32(cursor, (uint32_t)value_len);
    cursor += 4U;
    memcpy(cursor, term->field, field_len);
    cursor += field_len;
    memcpy(cursor, term->value, value_len);
    cursor += value_len;
  }
  for (index = 0U; index < generation->postings.count; ++index) {
    const lc_pouch_index_term_posting_entry *entry;
    const lc_pouch_index_posting *posting;
    size_t payload_size;

    entry = &generation->postings.entries[index];
    posting = &entry->posting;
    if (!lc_pouch_index_posting_payload_size(posting, &payload_size)) {
      return 0;
    }
    lc_pouch_index_term_put_u32(cursor, entry->term_id);
    cursor += 4U;
    lc_pouch_index_term_put_u32(cursor, (uint32_t)posting->encoding);
    cursor += 4U;
    lc_pouch_index_term_put_u32(cursor, (uint32_t)posting->count);
    cursor += 4U;
    lc_pouch_index_term_put_u32(cursor, posting->max_doc_id);
    cursor += 4U;
    lc_pouch_index_term_put_u64(cursor, (uint64_t)payload_size);
    cursor += 8U;
    if (posting->encoding == LC_POUCH_INDEX_POSTING_SPARSE) {
      memcpy(cursor, posting->sparse, payload_size);
    } else if (posting->encoding == LC_POUCH_INDEX_POSTING_DENSE) {
      memcpy(cursor, posting->dense, payload_size);
    }
    cursor += payload_size;
  }
  *written_out = (size_t)(cursor - dst);
  return *written_out == needed;
}

static int lc_pouch_index_term_posting_table_add_decoded(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_term_posting_table *table, lc_pouch_index_term_id term_id,
    lc_pouch_index_posting *posting) {
  size_t position;

  if (table == NULL || posting == NULL ||
      lc_pouch_index_term_posting_table_find_position(table, term_id,
                                                      &position)) {
    return 0;
  }
  if (!lc_pouch_index_term_posting_table_reserve(allocator, table,
                                                 table->count + 1U)) {
    return 0;
  }
  if (position < table->count) {
    memmove(&table->entries[position + 1U], &table->entries[position],
            (table->count - position) * sizeof(table->entries[0]));
  }
  table->entries[position].term_id = term_id;
  table->entries[position].posting = *posting;
  memset(posting, 0, sizeof(*posting));
  table->count++;
  return 1;
}

static int lc_pouch_index_term_validate_decoded_posting(
    const lc_pouch_allocator *allocator, lc_pouch_index_posting *posting) {
  lc_pouch_index_doc_id_set decoded;
  size_t index;
  int ok;

  memset(&decoded, 0, sizeof(decoded));
  ok = lc_pouch_index_posting_append(allocator, posting, &decoded);
  if (ok && decoded.count != posting->count) {
    ok = 0;
  }
  if (ok && posting->count > 0U &&
      decoded.items[decoded.count - 1U] != posting->max_doc_id) {
    ok = 0;
  }
  for (index = 1U; ok && index < decoded.count; ++index) {
    if (decoded.items[index - 1U] >= decoded.items[index]) {
      ok = 0;
    }
  }
  lc_pouch_index_doc_id_set_cleanup(allocator, &decoded);
  return ok;
}

static int lc_pouch_index_term_decode_posting(
    const lc_pouch_allocator *allocator, lc_pouch_index_posting *posting,
    uint32_t encoding, uint32_t count, uint32_t max_doc_id,
    const unsigned char *payload, size_t payload_size) {
  if (posting == NULL || (payload == NULL && payload_size > 0U)) {
    return 0;
  }
  memset(posting, 0, sizeof(*posting));
  posting->encoding = (lc_pouch_index_posting_encoding)encoding;
  posting->count = count;
  posting->max_doc_id = max_doc_id;
  if (posting->encoding == LC_POUCH_INDEX_POSTING_EMPTY) {
    return count == 0U && max_doc_id == 0U && payload_size == 0U;
  }
  if (posting->encoding == LC_POUCH_INDEX_POSTING_SPARSE) {
    if (count == 0U || payload_size == 0U) {
      return 0;
    }
    posting->sparse = (unsigned char *)lc_pouch_alloc(allocator, payload_size);
    if (posting->sparse == NULL) {
      return 0;
    }
    memcpy(posting->sparse, payload, payload_size);
    posting->sparse_len = payload_size;
    if (!lc_pouch_index_term_validate_decoded_posting(allocator, posting)) {
      lc_pouch_index_posting_cleanup(allocator, posting);
      return 0;
    }
    return 1;
  }
  if (posting->encoding == LC_POUCH_INDEX_POSTING_DENSE) {
    if (count == 0U || payload_size == 0U ||
        payload_size % sizeof(uint64_t) != 0U) {
      return 0;
    }
    posting->dense_word_count = payload_size / sizeof(uint64_t);
    posting->dense = (uint64_t *)lc_pouch_alloc(allocator, payload_size);
    if (posting->dense == NULL) {
      return 0;
    }
    memcpy(posting->dense, payload, payload_size);
    if (!lc_pouch_index_term_validate_decoded_posting(allocator, posting)) {
      lc_pouch_index_posting_cleanup(allocator, posting);
      return 0;
    }
    return 1;
  }
  return 0;
}

static int lc_pouch_index_term_generation_table_decode(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_term_generation *decoded, const unsigned char *src,
    size_t src_size) {
  size_t offset;
  uint32_t version;
  uint32_t term_count;
  uint32_t posting_count;
  uint32_t term_index;
  uint32_t posting_index;

  if (decoded == NULL || src == NULL ||
      !lc_pouch_index_term_decode_require(
          src_size, 0U, LC_POUCH_INDEX_TERM_TABLE_HEADER_SIZE) ||
      memcmp(src, LC_POUCH_INDEX_TERM_TABLE_MAGIC,
             LC_POUCH_INDEX_TERM_MAGIC_LEN) != 0) {
    return 0;
  }
  offset = LC_POUCH_INDEX_TERM_MAGIC_LEN;
  version = lc_pouch_index_term_get_u32(src + offset);
  offset += 4U;
  if (version != 1U) {
    return 0;
  }
  term_count = lc_pouch_index_term_get_u32(src + offset);
  offset += 4U;
  posting_count = lc_pouch_index_term_get_u32(src + offset);
  offset += 4U;
  decoded->terms.next_id = lc_pouch_index_term_get_u32(src + offset);
  offset += 4U;
  for (term_index = 0U; term_index < term_count; ++term_index) {
    char *field;
    char *value;
    uint32_t term_id;
    uint32_t field_len;
    uint32_t value_len;

    if (!lc_pouch_index_term_decode_require(
            src_size, offset, LC_POUCH_INDEX_TERM_ENTRY_FIXED_SIZE)) {
      return 0;
    }
    term_id = lc_pouch_index_term_get_u32(src + offset);
    offset += 4U;
    field_len = lc_pouch_index_term_get_u32(src + offset);
    offset += 4U;
    value_len = lc_pouch_index_term_get_u32(src + offset);
    offset += 4U;
    if (term_id >= decoded->terms.next_id ||
        !lc_pouch_index_term_decode_require(src_size, offset, field_len) ||
        !lc_pouch_index_term_decode_require(src_size, offset + field_len,
                                            value_len)) {
      return 0;
    }
    field = (char *)lc_pouch_alloc(allocator, (size_t)field_len + 1U);
    value = (char *)lc_pouch_alloc(allocator, (size_t)value_len + 1U);
    if (field == NULL || value == NULL) {
      lc_pouch_free(allocator, field);
      lc_pouch_free(allocator, value);
      return 0;
    }
    memcpy(field, src + offset, field_len);
    field[field_len] = '\0';
    offset += field_len;
    memcpy(value, src + offset, value_len);
    value[value_len] = '\0';
    offset += value_len;
    if (!lc_pouch_index_term_table_add_decoded(allocator, &decoded->terms,
                                               term_id, field, value)) {
      lc_pouch_free(allocator, field);
      lc_pouch_free(allocator, value);
      return 0;
    }
    lc_pouch_free(allocator, field);
    lc_pouch_free(allocator, value);
  }
  for (posting_index = 0U; posting_index < posting_count; ++posting_index) {
    lc_pouch_index_posting posting;
    uint32_t term_id;
    uint32_t encoding;
    uint32_t count;
    uint32_t max_doc_id;
    uint64_t payload_size_u64;
    size_t payload_size;

    if (!lc_pouch_index_term_decode_require(
            src_size, offset, LC_POUCH_INDEX_TERM_POSTING_FIXED_SIZE)) {
      return 0;
    }
    term_id = lc_pouch_index_term_get_u32(src + offset);
    offset += 4U;
    encoding = lc_pouch_index_term_get_u32(src + offset);
    offset += 4U;
    count = lc_pouch_index_term_get_u32(src + offset);
    offset += 4U;
    max_doc_id = lc_pouch_index_term_get_u32(src + offset);
    offset += 4U;
    payload_size_u64 = lc_pouch_index_term_get_u64(src + offset);
    offset += 8U;
    if (payload_size_u64 > (uint64_t)((size_t)-1) ||
        !lc_pouch_index_term_table_contains_id(&decoded->terms, term_id)) {
      return 0;
    }
    payload_size = (size_t)payload_size_u64;
    if (!lc_pouch_index_term_decode_require(src_size, offset, payload_size)) {
      return 0;
    }
    if (!lc_pouch_index_term_decode_posting(allocator, &posting, encoding,
                                            count, max_doc_id, src + offset,
                                            payload_size)) {
      return 0;
    }
    if (!lc_pouch_index_term_posting_table_add_decoded(
            allocator, &decoded->postings, term_id, &posting)) {
      lc_pouch_index_posting_cleanup(allocator, &posting);
      return 0;
    }
    offset += payload_size;
  }
  return offset == src_size;
}

void lc_pouch_index_term_generation_cleanup(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_term_generation *generation) {
  if (generation == NULL) {
    return;
  }
  lc_pouch_free(allocator, generation->namespace_name);
  lc_pouch_index_term_posting_table_cleanup(allocator, &generation->postings);
  lc_pouch_index_term_table_cleanup(allocator, &generation->terms);
  memset(generation, 0, sizeof(*generation));
}

int lc_pouch_index_term_generation_encoded_size(
    const lc_pouch_index_term_generation *generation, size_t *size_out) {
  size_t total;
  size_t namespace_len;
  size_t table_size;

  if (size_out != NULL) {
    *size_out = 0U;
  }
  if (generation == NULL || generation->namespace_name == NULL ||
      size_out == NULL ||
      !lc_pouch_index_term_generation_table_encoded_size(generation,
                                                         &table_size)) {
    return 0;
  }
  namespace_len = strlen(generation->namespace_name);
  if (namespace_len > UINT32_MAX) {
    return 0;
  }
  total = LC_POUCH_INDEX_TERM_GENERATION_HEADER_SIZE;
  if (!lc_pouch_index_term_add_size(&total, namespace_len) ||
      !lc_pouch_index_term_add_size(&total, table_size)) {
    return 0;
  }
  *size_out = total;
  return 1;
}

int lc_pouch_index_term_generation_encode(
    const lc_pouch_index_term_generation *generation, unsigned char *dst,
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
      !lc_pouch_index_term_generation_encoded_size(generation, &needed) ||
      dst_size < needed ||
      !lc_pouch_index_term_generation_table_encoded_size(generation,
                                                         &table_size)) {
    return 0;
  }
  namespace_len = strlen(generation->namespace_name);
  cursor = dst;
  memcpy(cursor, LC_POUCH_INDEX_TERM_GENERATION_MAGIC,
         LC_POUCH_INDEX_TERM_MAGIC_LEN);
  cursor += LC_POUCH_INDEX_TERM_MAGIC_LEN;
  lc_pouch_index_term_put_u32(cursor, 1U);
  cursor += 4U;
  lc_pouch_index_term_put_u64(cursor, generation->identity.sequence);
  cursor += 8U;
  lc_pouch_index_term_put_u64(cursor, generation->identity.manifest_generation);
  cursor += 8U;
  lc_pouch_index_term_put_u32(cursor, (uint32_t)namespace_len);
  cursor += 4U;
  lc_pouch_index_term_put_u64(cursor, (uint64_t)table_size);
  cursor += 8U;
  memcpy(cursor, generation->namespace_name, namespace_len);
  cursor += namespace_len;
  if (!lc_pouch_index_term_generation_table_encode(
          generation, cursor, dst_size - (size_t)(cursor - dst),
          &table_written) ||
      table_written != table_size) {
    return 0;
  }
  cursor += table_written;
  *written_out = (size_t)(cursor - dst);
  return *written_out == needed;
}

int lc_pouch_index_term_generation_decode(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_term_generation *generation, const unsigned char *src,
    size_t src_size) {
  lc_pouch_index_term_generation decoded;
  size_t offset;
  uint32_t version;
  uint32_t namespace_len;
  uint64_t table_size_u64;

  if (generation == NULL || src == NULL ||
      !lc_pouch_index_term_decode_require(
          src_size, 0U, LC_POUCH_INDEX_TERM_GENERATION_HEADER_SIZE) ||
      memcmp(src, LC_POUCH_INDEX_TERM_GENERATION_MAGIC,
             LC_POUCH_INDEX_TERM_MAGIC_LEN) != 0) {
    return 0;
  }
  offset = LC_POUCH_INDEX_TERM_MAGIC_LEN;
  version = lc_pouch_index_term_get_u32(src + offset);
  offset += 4U;
  if (version != 1U) {
    return 0;
  }
  memset(&decoded, 0, sizeof(decoded));
  decoded.identity.sequence = lc_pouch_index_term_get_u64(src + offset);
  offset += 8U;
  decoded.identity.manifest_generation =
      lc_pouch_index_term_get_u64(src + offset);
  offset += 8U;
  namespace_len = lc_pouch_index_term_get_u32(src + offset);
  offset += 4U;
  table_size_u64 = lc_pouch_index_term_get_u64(src + offset);
  offset += 8U;
  if (table_size_u64 > (uint64_t)((size_t)-1) ||
      !lc_pouch_index_term_decode_require(src_size, offset, namespace_len)) {
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
  if (!lc_pouch_index_term_decode_require(src_size, offset,
                                          (size_t)table_size_u64) ||
      offset + (size_t)table_size_u64 != src_size ||
      !lc_pouch_index_term_generation_table_decode(
          allocator, &decoded, src + offset, (size_t)table_size_u64)) {
    lc_pouch_index_term_generation_cleanup(allocator, &decoded);
    return 0;
  }
  lc_pouch_index_term_generation_cleanup(allocator, generation);
  *generation = decoded;
  return 1;
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
  memset(&cache->identity, 0, sizeof(cache->identity));
}

void lc_pouch_index_prepared_term_cache_refresh(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_prepared_term_cache *cache, uint64_t generation) {
  lc_pouch_index_identity identity;

  memset(&identity, 0, sizeof(identity));
  identity.sequence = generation;
  lc_pouch_index_prepared_term_cache_refresh_identity(allocator, cache,
                                                      identity);
}

void lc_pouch_index_prepared_term_cache_refresh_identity(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_prepared_term_cache *cache,
    lc_pouch_index_identity identity) {
  if (cache == NULL ||
      (cache->identity.sequence == identity.sequence &&
       cache->identity.manifest_generation == identity.manifest_generation)) {
    return;
  }
  lc_pouch_index_prepared_term_cache_cleanup(allocator, cache);
  cache->identity = identity;
  cache->generation = identity.sequence;
}

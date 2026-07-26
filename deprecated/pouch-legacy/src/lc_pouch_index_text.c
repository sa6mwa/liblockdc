#include "lc_pouch_index.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define LC_POUCH_INDEX_TEXT_MAGIC_LEN 8U
#define LC_POUCH_INDEX_TEXT_HEADER_SIZE 16U
#define LC_POUCH_INDEX_TEXT_FIELD_FIXED_SIZE 12U
#define LC_POUCH_INDEX_TEXT_VALUE_FIXED_SIZE 8U
#define LC_POUCH_INDEX_TEXT_GRAM_BYTES 3U
#define LC_POUCH_INDEX_TEXT_MAGIC "LCPTXT1\0"
#define LC_POUCH_INDEX_TEXT_GENERATION_MAGIC "LCPTXG1\0"
#define LC_POUCH_INDEX_TEXT_GENERATION_HEADER_SIZE 40U

typedef struct lc_pouch_index_text_gram_build_entry {
  lc_pouch_index_term_id term_id;
  lc_pouch_index_doc_id_set doc_ids;
} lc_pouch_index_text_gram_build_entry;

typedef struct lc_pouch_index_text_gram_build_table {
  lc_pouch_index_text_gram_build_entry *entries;
  size_t count;
  size_t capacity;
} lc_pouch_index_text_gram_build_table;

static unsigned char lc_pouch_index_text_ascii_lower(unsigned char ch) {
  if (ch >= (unsigned char)'A' && ch <= (unsigned char)'Z') {
    return (unsigned char)(ch - (unsigned char)'A' + (unsigned char)'a');
  }
  return ch;
}

static void lc_pouch_index_text_make_gram(const char *text, size_t offset,
                                          char out[6]) {
  out[0] = 'g';
  out[1] = ':';
  out[2] = (char)lc_pouch_index_text_ascii_lower((unsigned char)text[offset]);
  out[3] =
      (char)lc_pouch_index_text_ascii_lower((unsigned char)text[offset + 1U]);
  out[4] =
      (char)lc_pouch_index_text_ascii_lower((unsigned char)text[offset + 2U]);
  out[5] = '\0';
}

static int lc_pouch_index_text_starts_with(const char *text, const char *prefix,
                                           int ignore_case) {
  size_t index;
  size_t prefix_len;

  if (text == NULL || prefix == NULL) {
    return 0;
  }
  prefix_len = strlen(prefix);
  for (index = 0U; index < prefix_len; ++index) {
    unsigned char left;
    unsigned char right;

    if (text[index] == '\0') {
      return 0;
    }
    left = (unsigned char)text[index];
    right = (unsigned char)prefix[index];
    if (ignore_case) {
      left = lc_pouch_index_text_ascii_lower(left);
      right = lc_pouch_index_text_ascii_lower(right);
    }
    if (left != right) {
      return 0;
    }
  }
  return 1;
}

static int lc_pouch_index_text_contains_text(const char *text,
                                             const char *needle,
                                             int ignore_case) {
  size_t needle_len;
  size_t pos;
  size_t index;

  if (text == NULL || needle == NULL) {
    return 0;
  }
  needle_len = strlen(needle);
  if (needle_len == 0U) {
    return 1;
  }
  if (!ignore_case) {
    return strstr(text, needle) != NULL;
  }
  for (pos = 0U; text[pos] != '\0'; ++pos) {
    for (index = 0U; index < needle_len; ++index) {
      unsigned char left;
      unsigned char right;

      if (text[pos + index] == '\0') {
        return 0;
      }
      left = lc_pouch_index_text_ascii_lower((unsigned char)text[pos + index]);
      right = lc_pouch_index_text_ascii_lower((unsigned char)needle[index]);
      if (left != right) {
        break;
      }
    }
    if (index == needle_len) {
      return 1;
    }
  }
  return 0;
}

static int lc_pouch_index_text_doc_compare_value(const char *left_text,
                                                 uint32_t left_doc_id,
                                                 const char *right_text,
                                                 uint32_t right_doc_id) {
  int cmp;

  cmp = strcmp(left_text, right_text);
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

static int lc_pouch_index_text_doc_qsort_compare(const void *left,
                                                 const void *right) {
  const lc_pouch_index_text_doc_entry *a;
  const lc_pouch_index_text_doc_entry *b;

  a = (const lc_pouch_index_text_doc_entry *)left;
  b = (const lc_pouch_index_text_doc_entry *)right;
  return lc_pouch_index_text_doc_compare_value(a->text, a->doc_id, b->text,
                                               b->doc_id);
}

static int lc_pouch_index_text_field_find_position(
    const lc_pouch_index_text_posting_table *table, const char *field,
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
lc_pouch_index_text_fields_reserve(const lc_pouch_allocator *allocator,
                                   lc_pouch_index_text_posting_table *table,
                                   size_t needed) {
  size_t new_capacity;
  lc_pouch_index_text_field_entry *grown;

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
  grown = (lc_pouch_index_text_field_entry *)lc_pouch_realloc(
      allocator, table->fields, new_capacity * sizeof(table->fields[0]));
  if (grown == NULL) {
    return 0;
  }
  table->fields = grown;
  table->field_capacity = new_capacity;
  return 1;
}

static int
lc_pouch_index_text_values_reserve(const lc_pouch_allocator *allocator,
                                   lc_pouch_index_text_field_entry *field,
                                   size_t needed) {
  size_t new_capacity;
  lc_pouch_index_text_doc_entry *grown;

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
  grown = (lc_pouch_index_text_doc_entry *)lc_pouch_realloc(
      allocator, field->values, new_capacity * sizeof(field->values[0]));
  if (grown == NULL) {
    return 0;
  }
  field->values = grown;
  field->value_capacity = new_capacity;
  return 1;
}

void lc_pouch_index_text_posting_table_cleanup(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_text_posting_table *table) {
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
                    table->fields[field_index].values[value_index].text);
    }
    lc_pouch_free(allocator, table->fields[field_index].values);
  }
  lc_pouch_free(allocator, table->fields);
  lc_pouch_index_term_table_cleanup(allocator, &table->grams);
  lc_pouch_index_term_posting_table_cleanup(allocator, &table->gram_postings);
  memset(table, 0, sizeof(*table));
}

int lc_pouch_index_text_posting_table_mark_field(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_text_posting_table *table, const char *field) {
  lc_pouch_index_text_field_entry entry;
  size_t position;

  if (table == NULL || field == NULL) {
    return 0;
  }
  if (lc_pouch_index_text_field_find_position(table, field, &position)) {
    return 1;
  }
  memset(&entry, 0, sizeof(entry));
  entry.field = lc_pouch_strdup(allocator, field);
  entry.values_sorted = 1;
  if (entry.field == NULL) {
    return 0;
  }
  if (!lc_pouch_index_text_fields_reserve(allocator, table,
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

int lc_pouch_index_text_posting_table_has_field(
    const lc_pouch_index_text_posting_table *table, const char *field) {
  return lc_pouch_index_text_field_find_position(table, field, NULL);
}

int lc_pouch_index_text_posting_table_add_value_doc_id(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_text_posting_table *table, const char *field,
    const char *text, lc_pouch_index_doc_id doc_id) {
  lc_pouch_index_text_field_entry *entry;
  char *text_copy;
  size_t position;

  if (table == NULL || field == NULL || text == NULL) {
    return 0;
  }
  if (!lc_pouch_index_text_posting_table_mark_field(allocator, table, field) ||
      !lc_pouch_index_text_field_find_position(table, field, &position)) {
    return 0;
  }
  entry = &table->fields[position];
  if (!lc_pouch_index_text_values_reserve(allocator, entry,
                                          entry->value_count + 1U)) {
    return 0;
  }
  text_copy = lc_pouch_strdup(allocator, text);
  if (text_copy == NULL) {
    return 0;
  }
  entry->values[entry->value_count].text = text_copy;
  entry->values[entry->value_count].doc_id = doc_id;
  entry->value_count++;
  entry->values_sorted = 0;
  return 1;
}

static void
lc_pouch_index_text_field_sort_values(const lc_pouch_allocator *allocator,
                                      lc_pouch_index_text_field_entry *field) {
  size_t index;
  size_t out;

  if (field == NULL || field->values_sorted) {
    return;
  }
  if (field->value_count > 1U) {
    qsort(field->values, field->value_count, sizeof(field->values[0]),
          lc_pouch_index_text_doc_qsort_compare);
  }
  out = 0U;
  for (index = 0U; index < field->value_count; ++index) {
    if (out > 0U &&
        lc_pouch_index_text_doc_compare_value(
            field->values[out - 1U].text, field->values[out - 1U].doc_id,
            field->values[index].text, field->values[index].doc_id) == 0) {
      lc_pouch_free(allocator, field->values[index].text);
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

static void lc_pouch_index_text_gram_build_table_cleanup(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_text_gram_build_table *table) {
  size_t index;

  if (table == NULL) {
    return;
  }
  for (index = 0U; index < table->count; ++index) {
    lc_pouch_index_doc_id_set_cleanup(allocator,
                                      &table->entries[index].doc_ids);
  }
  lc_pouch_free(allocator, table->entries);
  memset(table, 0, sizeof(*table));
}

static int lc_pouch_index_text_gram_build_table_reserve(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_text_gram_build_table *table, size_t needed) {
  size_t new_capacity;
  lc_pouch_index_text_gram_build_entry *grown;

  if (table == NULL) {
    return 0;
  }
  if (needed <= table->capacity) {
    return 1;
  }
  new_capacity = table->capacity == 0U ? 32U : table->capacity;
  while (new_capacity < needed) {
    if (new_capacity > ((size_t)-1) / 2U) {
      return 0;
    }
    new_capacity *= 2U;
  }
  if (new_capacity > ((size_t)-1) / sizeof(table->entries[0])) {
    return 0;
  }
  grown = (lc_pouch_index_text_gram_build_entry *)lc_pouch_realloc(
      allocator, table->entries, new_capacity * sizeof(table->entries[0]));
  if (grown == NULL) {
    return 0;
  }
  table->entries = grown;
  table->capacity = new_capacity;
  return 1;
}

static int lc_pouch_index_text_gram_build_append(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_text_gram_build_table *table, lc_pouch_index_term_id term_id,
    lc_pouch_index_doc_id doc_id) {
  size_t index;

  if (table == NULL) {
    return 0;
  }
  for (index = 0U; index < table->count; ++index) {
    if (table->entries[index].term_id == term_id) {
      return lc_pouch_index_doc_id_set_append(
          allocator, &table->entries[index].doc_ids, doc_id);
    }
  }
  if (!lc_pouch_index_text_gram_build_table_reserve(allocator, table,
                                                    table->count + 1U)) {
    return 0;
  }
  memset(&table->entries[table->count], 0,
         sizeof(table->entries[table->count]));
  table->entries[table->count].term_id = term_id;
  if (!lc_pouch_index_doc_id_set_append(
          allocator, &table->entries[table->count].doc_ids, doc_id)) {
    return 0;
  }
  table->count++;
  return 1;
}

int lc_pouch_index_text_posting_table_build_postings(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_text_posting_table *table) {
  lc_pouch_index_text_gram_build_table grams;
  size_t field_index;
  int ok;

  if (table == NULL) {
    return 0;
  }
  memset(&grams, 0, sizeof(grams));
  lc_pouch_index_term_table_cleanup(allocator, &table->grams);
  lc_pouch_index_term_posting_table_cleanup(allocator, &table->gram_postings);
  ok = 1;
  for (field_index = 0U; ok && field_index < table->field_count;
       ++field_index) {
    lc_pouch_index_text_field_entry *field;
    size_t value_index;

    field = &table->fields[field_index];
    lc_pouch_index_text_field_sort_values(allocator, field);
    for (value_index = 0U; ok && value_index < field->value_count;
         ++value_index) {
      const char *text;
      size_t text_len;
      size_t offset;

      text = field->values[value_index].text;
      text_len = strlen(text);
      if (text_len < LC_POUCH_INDEX_TEXT_GRAM_BYTES) {
        continue;
      }
      for (offset = 0U; offset + LC_POUCH_INDEX_TEXT_GRAM_BYTES <= text_len;
           ++offset) {
        lc_pouch_index_term_id term_id;
        char gram[6];

        lc_pouch_index_text_make_gram(text, offset, gram);
        if (!lc_pouch_index_term_table_find_or_add(
                allocator, &table->grams, field->field, gram, &term_id) ||
            !lc_pouch_index_text_gram_build_append(
                allocator, &grams, term_id,
                field->values[value_index].doc_id)) {
          ok = 0;
          break;
        }
      }
    }
  }
  for (field_index = 0U; ok && field_index < grams.count; ++field_index) {
    if (!lc_pouch_index_doc_id_set_sort_unique(
            &grams.entries[field_index].doc_ids) ||
        !lc_pouch_index_term_posting_table_put(
            allocator, &table->gram_postings,
            grams.entries[field_index].term_id,
            grams.entries[field_index].doc_ids.items,
            grams.entries[field_index].doc_ids.count)) {
      ok = 0;
    }
  }
  lc_pouch_index_text_gram_build_table_cleanup(allocator, &grams);
  return ok;
}

int lc_pouch_index_text_posting_table_append_prefix(
    const lc_pouch_allocator *allocator,
    const lc_pouch_index_text_posting_table *table, const char *field,
    const char *prefix, int ignore_case, lc_pouch_index_doc_id_set *dst) {
  const lc_pouch_index_text_field_entry *entry;
  size_t position;
  size_t index;

  if (table == NULL || field == NULL || prefix == NULL || dst == NULL) {
    return 0;
  }
  if (!lc_pouch_index_text_field_find_position(table, field, &position)) {
    return 1;
  }
  entry = &table->fields[position];
  if (!entry->values_sorted) {
    return 0;
  }
  for (index = 0U; index < entry->value_count; ++index) {
    if (lc_pouch_index_text_starts_with(entry->values[index].text, prefix,
                                        ignore_case) &&
        !lc_pouch_index_doc_id_set_append(allocator, dst,
                                          entry->values[index].doc_id)) {
      return 0;
    }
  }
  return 1;
}

static const lc_pouch_index_term_posting_entry *
lc_pouch_index_text_gram_posting_find(
    const lc_pouch_index_term_posting_table *table,
    lc_pouch_index_term_id term_id) {
  size_t low;
  size_t high;

  if (table == NULL) {
    return NULL;
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
  if (low < table->count && table->entries[low].term_id == term_id) {
    return &table->entries[low];
  }
  return NULL;
}

static int
lc_pouch_index_text_doc_id_set_contains(const lc_pouch_index_doc_id_set *set,
                                        lc_pouch_index_doc_id id) {
  size_t low;
  size_t high;

  if (set == NULL) {
    return 0;
  }
  low = 0U;
  high = set->count;
  while (low < high) {
    size_t mid;

    mid = low + ((high - low) / 2U);
    if (set->items[mid] < id) {
      low = mid + 1U;
    } else {
      high = mid;
    }
  }
  return low < set->count && set->items[low] == id;
}

static int lc_pouch_index_text_choose_contains_gram(
    const lc_pouch_allocator *allocator,
    const lc_pouch_index_text_posting_table *table, const char *field,
    const char *needle, lc_pouch_index_doc_id_set *candidates,
    int *has_candidates_out) {
  lc_pouch_index_doc_id_set best;
  size_t needle_len;
  size_t offset;
  int have_best;

  if (has_candidates_out != NULL) {
    *has_candidates_out = 0;
  }
  if (table == NULL || field == NULL || needle == NULL || candidates == NULL) {
    return 0;
  }
  memset(&best, 0, sizeof(best));
  needle_len = strlen(needle);
  if (needle_len < LC_POUCH_INDEX_TEXT_GRAM_BYTES) {
    return 1;
  }
  have_best = 0;
  for (offset = 0U; offset + LC_POUCH_INDEX_TEXT_GRAM_BYTES <= needle_len;
       ++offset) {
    const lc_pouch_index_term_posting_entry *posting;
    lc_pouch_index_term_id term_id;
    lc_pouch_index_doc_id_set decoded;
    char gram[6];

    memset(&decoded, 0, sizeof(decoded));
    lc_pouch_index_text_make_gram(needle, offset, gram);
    if (!lc_pouch_index_term_table_find(&table->grams, field, gram, &term_id)) {
      lc_pouch_index_doc_id_set_cleanup(allocator, &best);
      lc_pouch_index_doc_id_set_reset(candidates);
      if (has_candidates_out != NULL) {
        *has_candidates_out = 1;
      }
      return 1;
    }
    posting =
        lc_pouch_index_text_gram_posting_find(&table->gram_postings, term_id);
    if (posting == NULL || !lc_pouch_index_posting_decode(
                               allocator, &posting->posting, &decoded)) {
      lc_pouch_index_doc_id_set_cleanup(allocator, &best);
      lc_pouch_index_doc_id_set_cleanup(allocator, &decoded);
      return 0;
    }
    if (!have_best || decoded.count < best.count) {
      lc_pouch_index_doc_id_set_cleanup(allocator, &best);
      best = decoded;
      memset(&decoded, 0, sizeof(decoded));
      have_best = 1;
      if (best.count == 0U) {
        break;
      }
    }
    lc_pouch_index_doc_id_set_cleanup(allocator, &decoded);
  }
  if (have_best) {
    lc_pouch_index_doc_id_set_cleanup(allocator, candidates);
    *candidates = best;
    memset(&best, 0, sizeof(best));
    if (has_candidates_out != NULL) {
      *has_candidates_out = 1;
    }
  }
  lc_pouch_index_doc_id_set_cleanup(allocator, &best);
  return 1;
}

int lc_pouch_index_text_posting_table_append_contains(
    const lc_pouch_allocator *allocator,
    const lc_pouch_index_text_posting_table *table, const char *field,
    const char *needle, int ignore_case, lc_pouch_index_doc_id_set *dst) {
  lc_pouch_index_doc_id_set candidates;
  const lc_pouch_index_text_field_entry *entry;
  size_t position;
  size_t index;
  int has_candidates;

  if (table == NULL || field == NULL || needle == NULL || dst == NULL) {
    return 0;
  }
  if (!lc_pouch_index_text_field_find_position(table, field, &position)) {
    return 1;
  }
  entry = &table->fields[position];
  if (!entry->values_sorted) {
    return 0;
  }
  memset(&candidates, 0, sizeof(candidates));
  has_candidates = 0;
  if (!lc_pouch_index_text_choose_contains_gram(allocator, table, field, needle,
                                                &candidates, &has_candidates)) {
    lc_pouch_index_doc_id_set_cleanup(allocator, &candidates);
    return 0;
  }
  for (index = 0U; index < entry->value_count; ++index) {
    if (has_candidates && !lc_pouch_index_text_doc_id_set_contains(
                              &candidates, entry->values[index].doc_id)) {
      continue;
    }
    if (lc_pouch_index_text_contains_text(entry->values[index].text, needle,
                                          ignore_case) &&
        !lc_pouch_index_doc_id_set_append(allocator, dst,
                                          entry->values[index].doc_id)) {
      lc_pouch_index_doc_id_set_cleanup(allocator, &candidates);
      return 0;
    }
  }
  lc_pouch_index_doc_id_set_cleanup(allocator, &candidates);
  return 1;
}

static int lc_pouch_index_text_add_size(size_t *total, size_t add) {
  if (total == NULL || *total > ((size_t)-1) - add) {
    return 0;
  }
  *total += add;
  return 1;
}

static void lc_pouch_index_text_put_u32(unsigned char *dst, uint32_t value) {
  dst[0] = (unsigned char)(value & UINT32_C(0xff));
  dst[1] = (unsigned char)((value >> 8U) & UINT32_C(0xff));
  dst[2] = (unsigned char)((value >> 16U) & UINT32_C(0xff));
  dst[3] = (unsigned char)((value >> 24U) & UINT32_C(0xff));
}

static void lc_pouch_index_text_put_u64(unsigned char *dst, uint64_t value) {
  size_t index;

  for (index = 0U; index < 8U; ++index) {
    dst[index] = (unsigned char)((value >> (index * 8U)) & UINT64_C(0xff));
  }
}

static uint32_t lc_pouch_index_text_get_u32(const unsigned char *src) {
  return ((uint32_t)src[0]) | (((uint32_t)src[1]) << 8U) |
         (((uint32_t)src[2]) << 16U) | (((uint32_t)src[3]) << 24U);
}

static uint64_t lc_pouch_index_text_get_u64(const unsigned char *src) {
  uint64_t value;
  size_t index;

  value = 0U;
  for (index = 0U; index < 8U; ++index) {
    value |= ((uint64_t)src[index]) << (index * 8U);
  }
  return value;
}

static int lc_pouch_index_text_decode_require(size_t src_size, size_t offset,
                                              size_t needed) {
  return offset <= src_size && needed <= src_size - offset;
}

int lc_pouch_index_text_posting_table_encoded_size(
    const lc_pouch_index_text_posting_table *table, size_t *size_out) {
  size_t total;
  size_t field_index;

  if (size_out != NULL) {
    *size_out = 0U;
  }
  if (table == NULL || size_out == NULL || table->field_count > UINT32_MAX) {
    return 0;
  }
  total = LC_POUCH_INDEX_TEXT_HEADER_SIZE;
  for (field_index = 0U; field_index < table->field_count; ++field_index) {
    const lc_pouch_index_text_field_entry *field;
    size_t field_len;
    size_t value_index;

    field = &table->fields[field_index];
    if (field->field == NULL || field->value_count > UINT32_MAX ||
        !field->values_sorted) {
      return 0;
    }
    field_len = strlen(field->field);
    if (field_len > UINT32_MAX ||
        !lc_pouch_index_text_add_size(&total,
                                      LC_POUCH_INDEX_TEXT_FIELD_FIXED_SIZE) ||
        !lc_pouch_index_text_add_size(&total, field_len)) {
      return 0;
    }
    for (value_index = 0U; value_index < field->value_count; ++value_index) {
      size_t text_len;

      if (field->values[value_index].text == NULL) {
        return 0;
      }
      text_len = strlen(field->values[value_index].text);
      if (text_len > UINT32_MAX ||
          !lc_pouch_index_text_add_size(&total,
                                        LC_POUCH_INDEX_TEXT_VALUE_FIXED_SIZE) ||
          !lc_pouch_index_text_add_size(&total, text_len)) {
        return 0;
      }
    }
  }
  *size_out = total;
  return 1;
}

int lc_pouch_index_text_posting_table_encode(
    const lc_pouch_index_text_posting_table *table, unsigned char *dst,
    size_t dst_size, size_t *written_out) {
  unsigned char *cursor;
  size_t needed;
  size_t field_index;

  if (written_out != NULL) {
    *written_out = 0U;
  }
  if (table == NULL || dst == NULL || written_out == NULL ||
      !lc_pouch_index_text_posting_table_encoded_size(table, &needed) ||
      dst_size < needed) {
    return 0;
  }
  cursor = dst;
  memcpy(cursor, LC_POUCH_INDEX_TEXT_MAGIC, LC_POUCH_INDEX_TEXT_MAGIC_LEN);
  cursor += LC_POUCH_INDEX_TEXT_MAGIC_LEN;
  lc_pouch_index_text_put_u32(cursor, 1U);
  cursor += 4U;
  lc_pouch_index_text_put_u32(cursor, (uint32_t)table->field_count);
  cursor += 4U;
  for (field_index = 0U; field_index < table->field_count; ++field_index) {
    const lc_pouch_index_text_field_entry *field;
    size_t field_len;
    size_t value_index;

    field = &table->fields[field_index];
    field_len = strlen(field->field);
    lc_pouch_index_text_put_u32(cursor, (uint32_t)field_len);
    cursor += 4U;
    lc_pouch_index_text_put_u32(cursor, (uint32_t)field->value_count);
    cursor += 4U;
    lc_pouch_index_text_put_u32(cursor, 0U);
    cursor += 4U;
    memcpy(cursor, field->field, field_len);
    cursor += field_len;
    for (value_index = 0U; value_index < field->value_count; ++value_index) {
      size_t text_len;

      text_len = strlen(field->values[value_index].text);
      lc_pouch_index_text_put_u32(cursor, (uint32_t)text_len);
      cursor += 4U;
      lc_pouch_index_text_put_u32(cursor, field->values[value_index].doc_id);
      cursor += 4U;
      memcpy(cursor, field->values[value_index].text, text_len);
      cursor += text_len;
    }
  }
  *written_out = (size_t)(cursor - dst);
  return *written_out == needed;
}

int lc_pouch_index_text_posting_table_decode(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_text_posting_table *table, const unsigned char *src,
    size_t src_size) {
  lc_pouch_index_text_posting_table decoded;
  size_t offset;
  uint32_t version;
  uint32_t field_count;
  uint32_t field_index;

  if (table == NULL || src == NULL ||
      !lc_pouch_index_text_decode_require(src_size, 0U,
                                          LC_POUCH_INDEX_TEXT_HEADER_SIZE) ||
      memcmp(src, LC_POUCH_INDEX_TEXT_MAGIC, LC_POUCH_INDEX_TEXT_MAGIC_LEN) !=
          0) {
    return 0;
  }
  offset = LC_POUCH_INDEX_TEXT_MAGIC_LEN;
  version = lc_pouch_index_text_get_u32(src + offset);
  offset += 4U;
  if (version != 1U) {
    return 0;
  }
  field_count = lc_pouch_index_text_get_u32(src + offset);
  offset += 4U;
  memset(&decoded, 0, sizeof(decoded));
  for (field_index = 0U; field_index < field_count; ++field_index) {
    char *field_name;
    uint32_t field_len;
    uint32_t value_count;
    uint32_t reserved;
    uint32_t value_index;

    if (!lc_pouch_index_text_decode_require(
            src_size, offset, LC_POUCH_INDEX_TEXT_FIELD_FIXED_SIZE)) {
      lc_pouch_index_text_posting_table_cleanup(allocator, &decoded);
      return 0;
    }
    field_len = lc_pouch_index_text_get_u32(src + offset);
    offset += 4U;
    value_count = lc_pouch_index_text_get_u32(src + offset);
    offset += 4U;
    reserved = lc_pouch_index_text_get_u32(src + offset);
    offset += 4U;
    if (reserved != 0U ||
        !lc_pouch_index_text_decode_require(src_size, offset, field_len)) {
      lc_pouch_index_text_posting_table_cleanup(allocator, &decoded);
      return 0;
    }
    field_name = (char *)lc_pouch_alloc(allocator, (size_t)field_len + 1U);
    if (field_name == NULL) {
      lc_pouch_index_text_posting_table_cleanup(allocator, &decoded);
      return 0;
    }
    memcpy(field_name, src + offset, field_len);
    field_name[field_len] = '\0';
    offset += field_len;
    if (!lc_pouch_index_text_posting_table_mark_field(allocator, &decoded,
                                                      field_name)) {
      lc_pouch_free(allocator, field_name);
      lc_pouch_index_text_posting_table_cleanup(allocator, &decoded);
      return 0;
    }
    for (value_index = 0U; value_index < value_count; ++value_index) {
      char *text;
      uint32_t text_len;
      uint32_t doc_id;

      if (!lc_pouch_index_text_decode_require(
              src_size, offset, LC_POUCH_INDEX_TEXT_VALUE_FIXED_SIZE)) {
        lc_pouch_free(allocator, field_name);
        lc_pouch_index_text_posting_table_cleanup(allocator, &decoded);
        return 0;
      }
      text_len = lc_pouch_index_text_get_u32(src + offset);
      offset += 4U;
      doc_id = lc_pouch_index_text_get_u32(src + offset);
      offset += 4U;
      if (!lc_pouch_index_text_decode_require(src_size, offset, text_len)) {
        lc_pouch_free(allocator, field_name);
        lc_pouch_index_text_posting_table_cleanup(allocator, &decoded);
        return 0;
      }
      text = (char *)lc_pouch_alloc(allocator, (size_t)text_len + 1U);
      if (text == NULL) {
        lc_pouch_free(allocator, field_name);
        lc_pouch_index_text_posting_table_cleanup(allocator, &decoded);
        return 0;
      }
      memcpy(text, src + offset, text_len);
      text[text_len] = '\0';
      offset += text_len;
      if (!lc_pouch_index_text_posting_table_add_value_doc_id(
              allocator, &decoded, field_name, text, doc_id)) {
        lc_pouch_free(allocator, text);
        lc_pouch_free(allocator, field_name);
        lc_pouch_index_text_posting_table_cleanup(allocator, &decoded);
        return 0;
      }
      lc_pouch_free(allocator, text);
    }
    lc_pouch_free(allocator, field_name);
  }
  if (offset != src_size ||
      !lc_pouch_index_text_posting_table_build_postings(allocator, &decoded)) {
    lc_pouch_index_text_posting_table_cleanup(allocator, &decoded);
    return 0;
  }
  lc_pouch_index_text_posting_table_cleanup(allocator, table);
  *table = decoded;
  return 1;
}

void lc_pouch_index_text_generation_cleanup(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_text_generation *generation) {
  if (generation == NULL) {
    return;
  }
  lc_pouch_free(allocator, generation->namespace_name);
  lc_pouch_index_text_posting_table_cleanup(allocator, &generation->postings);
  memset(generation, 0, sizeof(*generation));
}

int lc_pouch_index_text_generation_encoded_size(
    const lc_pouch_index_text_generation *generation, size_t *size_out) {
  size_t total;
  size_t namespace_len;
  size_t table_size;

  if (size_out != NULL) {
    *size_out = 0U;
  }
  if (generation == NULL || generation->namespace_name == NULL ||
      size_out == NULL ||
      !lc_pouch_index_text_posting_table_encoded_size(&generation->postings,
                                                      &table_size)) {
    return 0;
  }
  namespace_len = strlen(generation->namespace_name);
  if (namespace_len > UINT32_MAX) {
    return 0;
  }
  total = LC_POUCH_INDEX_TEXT_GENERATION_HEADER_SIZE;
  if (!lc_pouch_index_text_add_size(&total, namespace_len) ||
      !lc_pouch_index_text_add_size(&total, table_size)) {
    return 0;
  }
  *size_out = total;
  return 1;
}

int lc_pouch_index_text_generation_encode(
    const lc_pouch_index_text_generation *generation, unsigned char *dst,
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
      !lc_pouch_index_text_generation_encoded_size(generation, &needed) ||
      dst_size < needed ||
      !lc_pouch_index_text_posting_table_encoded_size(&generation->postings,
                                                      &table_size)) {
    return 0;
  }
  namespace_len = strlen(generation->namespace_name);
  cursor = dst;
  memcpy(cursor, LC_POUCH_INDEX_TEXT_GENERATION_MAGIC,
         LC_POUCH_INDEX_TEXT_MAGIC_LEN);
  cursor += LC_POUCH_INDEX_TEXT_MAGIC_LEN;
  lc_pouch_index_text_put_u32(cursor, 1U);
  cursor += 4U;
  lc_pouch_index_text_put_u64(cursor, generation->identity.sequence);
  cursor += 8U;
  lc_pouch_index_text_put_u64(cursor, generation->identity.manifest_generation);
  cursor += 8U;
  lc_pouch_index_text_put_u32(cursor, (uint32_t)namespace_len);
  cursor += 4U;
  lc_pouch_index_text_put_u64(cursor, (uint64_t)table_size);
  cursor += 8U;
  memcpy(cursor, generation->namespace_name, namespace_len);
  cursor += namespace_len;
  if (!lc_pouch_index_text_posting_table_encode(
          &generation->postings, cursor, dst_size - (size_t)(cursor - dst),
          &table_written) ||
      table_written != table_size) {
    return 0;
  }
  cursor += table_written;
  *written_out = (size_t)(cursor - dst);
  return *written_out == needed;
}

int lc_pouch_index_text_generation_decode(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_text_generation *generation, const unsigned char *src,
    size_t src_size) {
  lc_pouch_index_text_generation decoded;
  size_t offset;
  uint32_t version;
  uint32_t namespace_len;
  uint64_t table_size_u64;

  if (generation == NULL || src == NULL ||
      !lc_pouch_index_text_decode_require(
          src_size, 0U, LC_POUCH_INDEX_TEXT_GENERATION_HEADER_SIZE) ||
      memcmp(src, LC_POUCH_INDEX_TEXT_GENERATION_MAGIC,
             LC_POUCH_INDEX_TEXT_MAGIC_LEN) != 0) {
    return 0;
  }
  offset = LC_POUCH_INDEX_TEXT_MAGIC_LEN;
  version = lc_pouch_index_text_get_u32(src + offset);
  offset += 4U;
  if (version != 1U) {
    return 0;
  }
  memset(&decoded, 0, sizeof(decoded));
  decoded.identity.sequence = lc_pouch_index_text_get_u64(src + offset);
  offset += 8U;
  decoded.identity.manifest_generation =
      lc_pouch_index_text_get_u64(src + offset);
  offset += 8U;
  namespace_len = lc_pouch_index_text_get_u32(src + offset);
  offset += 4U;
  table_size_u64 = lc_pouch_index_text_get_u64(src + offset);
  offset += 8U;
  if (table_size_u64 > (uint64_t)((size_t)-1) ||
      !lc_pouch_index_text_decode_require(src_size, offset, namespace_len)) {
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
  if (!lc_pouch_index_text_decode_require(src_size, offset,
                                          (size_t)table_size_u64) ||
      offset + (size_t)table_size_u64 != src_size ||
      !lc_pouch_index_text_posting_table_decode(
          allocator, &decoded.postings, src + offset, (size_t)table_size_u64)) {
    lc_pouch_index_text_generation_cleanup(allocator, &decoded);
    return 0;
  }
  lc_pouch_index_text_generation_cleanup(allocator, generation);
  *generation = decoded;
  return 1;
}

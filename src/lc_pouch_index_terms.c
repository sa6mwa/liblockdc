#include "lc_pouch_index.h"

#include "lc_api_internal.h"

#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define LC_POUCH_INDEX_TERM_GENERATION_FORMAT "pouch-term-generation"
#define LC_POUCH_INDEX_TERM_GENERATION_VERSION 1UL

static int lc_pouch_index_term_hex_token_valid(const char *token) {
  size_t index;
  unsigned char ch;

  if (token == NULL || token[0] == '\0') {
    return 0;
  }
  for (index = 0U; token[index] != '\0'; ++index) {
    ch = (unsigned char)token[index];
    if (!((ch >= (unsigned char)'0' && ch <= (unsigned char)'9') ||
          (ch >= (unsigned char)'a' && ch <= (unsigned char)'f') ||
          (ch >= (unsigned char)'A' && ch <= (unsigned char)'F'))) {
      return 0;
    }
  }
  return index % 2U == 0U;
}

static char *lc_pouch_index_term_next_token(char **cursor, int final_token) {
  char *start;
  char *space;

  if (cursor == NULL || *cursor == NULL) {
    return NULL;
  }
  start = *cursor;
  if (final_token) {
    space = strchr(start, '\n');
    if (space != NULL) {
      *space = '\0';
    }
    if (strchr(start, ' ') != NULL) {
      return NULL;
    }
    *cursor = NULL;
    return start;
  }
  space = strchr(start, ' ');
  if (space == NULL) {
    return NULL;
  }
  *space = '\0';
  *cursor = space + 1;
  return start;
}

static int lc_pouch_index_term_parse_ulong_token(const char *token,
                                                 unsigned long *out,
                                                 const char *message,
                                                 lc_error *error) {
  char *end;
  unsigned long parsed;

  if (token == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL, NULL);
  }
  errno = 0;
  parsed = strtoul(token, &end, 10);
  if (errno != 0 || end == token || *end != '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL, NULL);
  }
  *out = parsed;
  return LC_OK;
}

static char *lc_pouch_index_term_hex_encode_bytes(const lc_allocator *allocator,
                                                  const char *value,
                                                  size_t length) {
  static const char hex[] = "0123456789abcdef";
  const unsigned char *src;
  char *encoded;
  char *dst;

  if (value == NULL || length == 0U) {
    encoded = (char *)lc_alloc_with_allocator(allocator, 2U);
    if (encoded != NULL) {
      encoded[0] = '-';
      encoded[1] = '\0';
    }
    return encoded;
  }
  if (length > ((size_t)-1 - 1U) / 2U) {
    return NULL;
  }
  encoded = (char *)lc_alloc_with_allocator(allocator, (length * 2U) + 1U);
  if (encoded == NULL) {
    return NULL;
  }
  src = (const unsigned char *)value;
  dst = encoded;
  while (length-- > 0U) {
    *dst++ = hex[*src >> 4];
    *dst++ = hex[*src & 0x0fU];
    ++src;
  }
  *dst = '\0';
  return encoded;
}

static char *lc_pouch_index_term_hex_encode(const lc_allocator *allocator,
                                            const char *value) {
  return lc_pouch_index_term_hex_encode_bytes(
      allocator, value, value != NULL ? strlen(value) : 0U);
}

static int lc_pouch_index_term_parse_number_value(const char *text,
                                                  double *out) {
  char *endptr;
  double value;

  if (text == NULL || text[0] == '\0' || out == NULL) {
    return 0;
  }
  errno = 0;
  value = strtod(text, &endptr);
  if (errno != 0 || endptr == text || *endptr != '\0' || !isfinite(value)) {
    return 0;
  }
  *out = value;
  return 1;
}

static char *lc_pouch_index_term_value_hex_encode(const lc_allocator *allocator,
                                                  const char *value,
                                                  char value_type,
                                                  lc_error *error) {
  char canonical[64];
  double number;
  int written;

  if (value_type != 'n') {
    return lc_pouch_index_term_hex_encode(allocator, value);
  }
  if (!lc_pouch_index_term_parse_number_value(value, &number)) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch index exact numeric term requires a finite JSON "
                       "number",
                       NULL, NULL, "pouch");
    return NULL;
  }
  if (number == 0.0) {
    written = snprintf(canonical, sizeof(canonical), "0");
  } else {
    written = snprintf(canonical, sizeof(canonical), "%.17g", number);
  }
  if (written < 0 || (size_t)written >= sizeof(canonical)) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch index exact numeric term exceeds local limit",
                       NULL, NULL, "pouch");
    return NULL;
  }
  return lc_pouch_index_term_hex_encode(allocator, canonical);
}

void lc_pouch_index_term_fields_cleanup(const lc_allocator *allocator,
                                        lc_pouch_index_term_field *fields,
                                        size_t count) {
  size_t index;

  if (fields == NULL) {
    return;
  }
  for (index = 0U; index < count; ++index) {
    lc_free_with_allocator(allocator, fields[index].field_hex);
  }
  lc_free_with_allocator(allocator, fields);
}

void lc_pouch_index_term_values_cleanup(const lc_allocator *allocator,
                                        lc_pouch_index_term_value *values,
                                        size_t count) {
  size_t index;

  if (values == NULL) {
    return;
  }
  for (index = 0U; index < count; ++index) {
    lc_free_with_allocator(allocator, values[index].field_hex);
    lc_free_with_allocator(allocator, values[index].value_hex);
  }
  lc_free_with_allocator(allocator, values);
}

int lc_pouch_index_term_fields_find(const lc_pouch_index_term_field *fields,
                                    size_t count, const char *field_hex,
                                    unsigned long *first_line,
                                    unsigned long *line_count) {
  size_t low;
  size_t high;
  size_t mid;
  int cmp;

  if (fields == NULL || field_hex == NULL || first_line == NULL ||
      line_count == NULL) {
    return 0;
  }
  low = 0U;
  high = count;
  while (low < high) {
    mid = low + ((high - low) / 2U);
    cmp = strcmp(field_hex, fields[mid].field_hex);
    if (cmp == 0) {
      *first_line = fields[mid].first_line;
      *line_count = fields[mid].line_count;
      return 1;
    }
    if (cmp < 0) {
      high = mid;
    } else {
      low = mid + 1U;
    }
  }
  return 0;
}

int lc_pouch_index_term_values_find(const lc_pouch_index_term_value *values,
                                    size_t count, const char *field_hex,
                                    const char *value_hex,
                                    unsigned long *first_line,
                                    unsigned long *line_count) {
  size_t low;
  size_t high;
  size_t mid;
  int cmp;

  if (values == NULL || field_hex == NULL || value_hex == NULL ||
      first_line == NULL || line_count == NULL) {
    return 0;
  }
  low = 0U;
  high = count;
  while (low < high) {
    mid = low + ((high - low) / 2U);
    cmp = strcmp(field_hex, values[mid].field_hex);
    if (cmp == 0) {
      cmp = strcmp(value_hex, values[mid].value_hex);
    }
    if (cmp == 0) {
      *first_line = values[mid].first_line;
      *line_count = values[mid].line_count;
      return 1;
    }
    if (cmp < 0) {
      high = mid;
    } else {
      low = mid + 1U;
    }
  }
  return 0;
}

void lc_pouch_index_term_keys_cleanup(const lc_allocator *allocator,
                                      lc_pouch_index_term_key *terms,
                                      size_t count) {
  size_t index;

  if (terms == NULL) {
    return;
  }
  for (index = 0U; index < count; ++index) {
    lc_free_with_allocator(allocator, (char *)terms[index].field_hex);
    lc_free_with_allocator(allocator, (char *)terms[index].value_hex);
  }
  lc_free_with_allocator(allocator, terms);
}

int lc_pouch_index_term_key_compare_items(
    const lc_pouch_index_term_key *left, const lc_pouch_index_term_key *right) {
  int cmp;

  if (left == NULL && right == NULL) {
    return 0;
  }
  if (left == NULL) {
    return -1;
  }
  if (right == NULL) {
    return 1;
  }
  cmp = strcmp(left->field_hex, right->field_hex);
  if (cmp != 0) {
    return cmp;
  }
  cmp = strcmp(left->value_hex, right->value_hex);
  if (cmp != 0) {
    return cmp;
  }
  if (left->value_type != right->value_type) {
    return left->value_type < right->value_type ? -1 : 1;
  }
  return 0;
}

int lc_pouch_index_term_key_compare(const void *left, const void *right) {
  return lc_pouch_index_term_key_compare_items(
      (const lc_pouch_index_term_key *)left,
      (const lc_pouch_index_term_key *)right);
}

int lc_pouch_index_term_key_compare_pair(const char *field_hex,
                                         const char *value_hex,
                                         const lc_pouch_index_term_key *term) {
  int cmp;

  if (field_hex == NULL || value_hex == NULL || term == NULL) {
    return field_hex == NULL && value_hex == NULL && term == NULL ? 0 : -1;
  }
  cmp = strcmp(field_hex, term->field_hex);
  if (cmp != 0) {
    return cmp;
  }
  return strcmp(value_hex, term->value_hex);
}

int lc_pouch_index_term_keys_find(const lc_pouch_index_term_key *terms,
                                  size_t count, const char *field_hex,
                                  const char *value_hex, char value_type,
                                  size_t *index_out) {
  size_t low;
  size_t high;
  size_t mid;
  size_t scan;
  int cmp;

  if (terms == NULL || field_hex == NULL || value_hex == NULL ||
      value_type == '\0' || count == 0U) {
    return 0;
  }
  low = 0U;
  high = count;
  while (low < high) {
    mid = low + ((high - low) / 2U);
    cmp =
        lc_pouch_index_term_key_compare_pair(field_hex, value_hex, &terms[mid]);
    if (cmp == 0) {
      scan = mid;
      while (scan > 0U && lc_pouch_index_term_key_compare_pair(
                              field_hex, value_hex, &terms[scan - 1U]) == 0) {
        --scan;
      }
      for (; scan < count && lc_pouch_index_term_key_compare_pair(
                                 field_hex, value_hex, &terms[scan]) == 0;
           ++scan) {
        if (terms[scan].value_type == value_type) {
          if (index_out != NULL) {
            *index_out = scan;
          }
          return 1;
        }
      }
      return 0;
    }
    if (cmp < 0) {
      high = mid;
    } else {
      low = mid + 1U;
    }
  }
  return 0;
}

int lc_pouch_index_term_keys_build_exact(const lc_pouch_index_plain_term *terms,
                                         size_t term_count,
                                         lc_pouch_index_term_key **out_terms,
                                         size_t *out_count,
                                         const lc_allocator *allocator,
                                         lc_error *error) {
  lc_pouch_index_term_key *keys;
  size_t index;
  size_t write_index;
  int rc;

  if (out_terms == NULL || out_count == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index exact term-key build requires outputs",
                        NULL, NULL, NULL);
  }
  *out_terms = NULL;
  *out_count = 0U;
  if (terms == NULL || term_count == 0U) {
    return LC_OK;
  }
  keys = (lc_pouch_index_term_key *)lc_alloc_with_allocator(
      allocator, term_count * sizeof(*keys));
  if (keys == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index exact term keys", NULL,
                        NULL, NULL);
  }
  memset(keys, 0, term_count * sizeof(*keys));
  rc = LC_OK;
  for (index = 0U; index < term_count; ++index) {
    if (terms[index].field == NULL || terms[index].field[0] == '\0' ||
        terms[index].value == NULL ||
        (terms[index].value_type != 's' && terms[index].value_type != 'n' &&
         terms[index].value_type != 'b' && terms[index].value_type != 'z')) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index exact term-key build requires non-empty "
                        "fields, non-null values, and scalar value types",
                        NULL, NULL, NULL);
      break;
    }
    keys[index].field_hex =
        lc_pouch_index_term_hex_encode(allocator, terms[index].field);
    keys[index].value_hex = lc_pouch_index_term_value_hex_encode(
        allocator, terms[index].value, terms[index].value_type, error);
    if (keys[index].field_hex == NULL || keys[index].value_hex == NULL) {
      rc = error != NULL && error->code != LC_OK
               ? error->code
               : lc_error_set(error, LC_ERR_NOMEM, 0L,
                              "failed to allocate pouch index exact term key",
                              NULL, NULL, NULL);
      break;
    }
    keys[index].value_type = terms[index].value_type;
  }
  if (rc == LC_OK) {
    qsort(keys, term_count, sizeof(keys[0]), lc_pouch_index_term_key_compare);
    write_index = 0U;
    for (index = 0U; index < term_count; ++index) {
      if (write_index > 0U && lc_pouch_index_term_key_compare_items(
                                  &keys[write_index - 1U], &keys[index]) == 0) {
        lc_free_with_allocator(allocator, (char *)keys[index].field_hex);
        lc_free_with_allocator(allocator, (char *)keys[index].value_hex);
        memset(&keys[index], 0, sizeof(keys[index]));
        continue;
      }
      if (write_index != index) {
        keys[write_index] = keys[index];
        memset(&keys[index], 0, sizeof(keys[index]));
      }
      ++write_index;
    }
    *out_terms = keys;
    *out_count = write_index;
    return LC_OK;
  }
  lc_pouch_index_term_keys_cleanup(allocator, keys, term_count);
  return rc;
}

int lc_pouch_index_term_keys_build_exact_for_field(
    const char *field, const char *const *values, const char *value_types,
    size_t value_count, lc_pouch_index_term_key **out_terms, size_t *out_count,
    const lc_allocator *allocator, lc_error *error) {
  lc_pouch_index_plain_term *terms;
  size_t index;
  int rc;

  if (out_terms == NULL || out_count == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index exact field term-key build requires "
                        "outputs",
                        NULL, NULL, NULL);
  }
  *out_terms = NULL;
  *out_count = 0U;
  if (field == NULL || field[0] == '\0' ||
      (values == NULL && value_count > 0U) ||
      (value_types == NULL && value_count > 0U)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index exact field term-key build requires a "
                        "field, values, and value types",
                        NULL, NULL, NULL);
  }
  if (value_count == 0U) {
    return LC_OK;
  }
  terms = (lc_pouch_index_plain_term *)lc_alloc_with_allocator(
      allocator, value_count * sizeof(*terms));
  if (terms == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index exact field terms",
                        NULL, NULL, NULL);
  }
  for (index = 0U; index < value_count; ++index) {
    terms[index].field = field;
    terms[index].value = values[index];
    terms[index].value_type = value_types[index];
  }
  rc = lc_pouch_index_term_keys_build_exact(terms, value_count, out_terms,
                                            out_count, allocator, error);
  lc_free_with_allocator(allocator, terms);
  return rc;
}

static int lc_pouch_index_term_value_hex_token_valid(const char *token) {
  return token != NULL && (strcmp(token, "-") == 0 ||
                           lc_pouch_index_term_hex_token_valid(token));
}

static int lc_pouch_index_term_value_type_valid(char value_type) {
  return value_type == 's' || value_type == 'n' || value_type == 'b' ||
         value_type == 'z';
}

static int lc_pouch_index_term_entry_compare_parts(const char *left_field_hex,
                                                   const char *left_value_hex,
                                                   char left_value_type,
                                                   const char *right_field_hex,
                                                   const char *right_value_hex,
                                                   char right_value_type) {
  int cmp;

  cmp = strcmp(left_field_hex, right_field_hex);
  if (cmp != 0) {
    return cmp;
  }
  cmp = strcmp(left_value_hex, right_value_hex);
  if (cmp != 0) {
    return cmp;
  }
  if (left_value_type != right_value_type) {
    return left_value_type < right_value_type ? -1 : 1;
  }
  return 0;
}

static int lc_pouch_index_term_table_find_position(
    const lc_pouch_index_term_table *table, const char *field_hex,
    const char *value_hex, char value_type, size_t *position_out) {
  size_t low;
  size_t high;
  size_t mid;
  int cmp;

  if (position_out != NULL) {
    *position_out = 0U;
  }
  if (table == NULL || field_hex == NULL || value_hex == NULL ||
      !lc_pouch_index_term_value_type_valid(value_type)) {
    return 0;
  }
  low = 0U;
  high = table->count;
  while (low < high) {
    mid = low + ((high - low) / 2U);
    cmp = lc_pouch_index_term_entry_compare_parts(
        table->items[mid].field_hex, table->items[mid].value_hex,
        table->items[mid].value_type, field_hex, value_hex, value_type);
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
         lc_pouch_index_term_entry_compare_parts(
             table->items[low].field_hex, table->items[low].value_hex,
             table->items[low].value_type, field_hex, value_hex,
             value_type) == 0;
}

static int lc_pouch_index_term_table_reserve(const lc_allocator *allocator,
                                             lc_pouch_index_term_table *table,
                                             size_t needed, lc_error *error) {
  lc_pouch_index_term_entry *next_items;
  size_t next_capacity;

  if (table == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term table reserve requires table", NULL,
                        NULL, NULL);
  }
  if (needed <= table->capacity) {
    return LC_OK;
  }
  next_capacity = table->capacity == 0U ? 16U : table->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch index term table exceeds local limit", NULL,
                          NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_items = (lc_pouch_index_term_entry *)lc_alloc_with_allocator(
      allocator, next_capacity * sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index term table", NULL, NULL,
                        NULL);
  }
  if (table->items != NULL) {
    memcpy(next_items, table->items, table->count * sizeof(*next_items));
    lc_free_with_allocator(allocator, table->items);
  }
  memset(next_items + table->count, 0,
         (next_capacity - table->count) * sizeof(*next_items));
  table->items = next_items;
  table->capacity = next_capacity;
  return LC_OK;
}

void lc_pouch_index_term_table_cleanup(const lc_allocator *allocator,
                                       lc_pouch_index_term_table *table) {
  size_t index;

  if (table == NULL) {
    return;
  }
  for (index = 0U; index < table->count; ++index) {
    lc_free_with_allocator(allocator, table->items[index].field_hex);
    lc_free_with_allocator(allocator, table->items[index].value_hex);
  }
  lc_free_with_allocator(allocator, table->items);
  memset(table, 0, sizeof(*table));
}

int lc_pouch_index_term_table_find(const lc_pouch_index_term_table *table,
                                   const char *field_hex, const char *value_hex,
                                   char value_type,
                                   unsigned long *term_id_out) {
  size_t position;

  if (!lc_pouch_index_term_table_find_position(table, field_hex, value_hex,
                                               value_type, &position)) {
    return 0;
  }
  if (term_id_out != NULL) {
    *term_id_out = table->items[position].term_id;
  }
  return 1;
}

static int
lc_pouch_index_term_table_contains_id(const lc_pouch_index_term_table *table,
                                      unsigned long term_id) {
  size_t index;

  if (table == NULL || term_id == 0UL) {
    return 0;
  }
  for (index = 0U; index < table->count; ++index) {
    if (table->items[index].term_id == term_id) {
      return 1;
    }
  }
  return 0;
}

static int lc_pouch_index_term_table_add_entry(
    const lc_allocator *allocator, lc_pouch_index_term_table *table,
    const char *field_hex, const char *value_hex, char value_type,
    unsigned long term_id, unsigned long *term_id_out, lc_error *error) {
  lc_pouch_index_term_entry entry;
  size_t position;
  int rc;

  if (table == NULL || field_hex == NULL || value_hex == NULL ||
      !lc_pouch_index_term_hex_token_valid(field_hex) ||
      !lc_pouch_index_term_value_hex_token_valid(value_hex) ||
      !lc_pouch_index_term_value_type_valid(value_type) || term_id == 0UL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term table entry requires typed exact "
                        "hex term",
                        NULL, NULL, "pouch");
  }
  if (lc_pouch_index_term_table_contains_id(table, term_id) ||
      lc_pouch_index_term_table_find_position(table, field_hex, value_hex,
                                              value_type, &position)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term table entry must be unique", NULL,
                        NULL, "pouch");
  }
  memset(&entry, 0, sizeof(entry));
  entry.field_hex = lc_strdup_with_allocator(allocator, field_hex);
  entry.value_hex = lc_strdup_with_allocator(allocator, value_hex);
  if (entry.field_hex == NULL || entry.value_hex == NULL) {
    lc_free_with_allocator(allocator, entry.field_hex);
    lc_free_with_allocator(allocator, entry.value_hex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index term entry", NULL, NULL,
                        NULL);
  }
  entry.value_type = value_type;
  entry.term_id = term_id;
  rc = lc_pouch_index_term_table_reserve(allocator, table, table->count + 1U,
                                         error);
  if (rc != LC_OK) {
    lc_free_with_allocator(allocator, entry.field_hex);
    lc_free_with_allocator(allocator, entry.value_hex);
    return rc;
  }
  if (position < table->count) {
    memmove(&table->items[position + 1U], &table->items[position],
            (table->count - position) * sizeof(table->items[0]));
  }
  table->items[position] = entry;
  ++table->count;
  if (term_id >= table->next_term_id) {
    if (term_id == (unsigned long)-1) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index term table id exceeds local limit", NULL,
                          NULL, "pouch");
    }
    table->next_term_id = term_id + 1UL;
  }
  if (term_id_out != NULL) {
    *term_id_out = term_id;
  }
  return LC_OK;
}

int lc_pouch_index_term_table_find_or_add(
    const lc_allocator *allocator, lc_pouch_index_term_table *table,
    const char *field_hex, const char *value_hex, char value_type,
    unsigned long *term_id_out, lc_error *error) {
  unsigned long term_id;

  if (term_id_out != NULL) {
    *term_id_out = 0UL;
  }
  if (lc_pouch_index_term_table_find(table, field_hex, value_hex, value_type,
                                     term_id_out)) {
    return LC_OK;
  }
  if (table == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term table add requires table", NULL, NULL,
                        NULL);
  }
  if (table->next_term_id == 0UL) {
    table->next_term_id = 1UL;
  }
  term_id = table->next_term_id;
  return lc_pouch_index_term_table_add_entry(allocator, table, field_hex,
                                             value_hex, value_type, term_id,
                                             term_id_out, error);
}

static int lc_pouch_index_term_posting_table_find_position(
    const lc_pouch_index_term_posting_table *table, unsigned long term_id,
    size_t *position_out) {
  size_t low;
  size_t high;
  size_t mid;

  if (position_out != NULL) {
    *position_out = 0U;
  }
  if (table == NULL || term_id == 0UL) {
    return 0;
  }
  low = 0U;
  high = table->count;
  while (low < high) {
    mid = low + ((high - low) / 2U);
    if (table->items[mid].term_id < term_id) {
      low = mid + 1U;
    } else {
      high = mid;
    }
  }
  if (position_out != NULL) {
    *position_out = low;
  }
  return low < table->count && table->items[low].term_id == term_id;
}

static int lc_pouch_index_term_posting_table_reserve(
    const lc_allocator *allocator, lc_pouch_index_term_posting_table *table,
    size_t needed, lc_error *error) {
  lc_pouch_index_term_posting_entry *next_items;
  size_t next_capacity;

  if (table == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term posting table reserve requires "
                        "table",
                        NULL, NULL, NULL);
  }
  if (needed <= table->capacity) {
    return LC_OK;
  }
  next_capacity = table->capacity == 0U ? 16U : table->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch index term posting table exceeds local "
                          "limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_items = (lc_pouch_index_term_posting_entry *)lc_alloc_with_allocator(
      allocator, next_capacity * sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index term posting table",
                        NULL, NULL, NULL);
  }
  if (table->items != NULL) {
    memcpy(next_items, table->items, table->count * sizeof(*next_items));
    lc_free_with_allocator(allocator, table->items);
  }
  memset(next_items + table->count, 0,
         (next_capacity - table->count) * sizeof(*next_items));
  table->items = next_items;
  table->capacity = next_capacity;
  return LC_OK;
}

void lc_pouch_index_term_posting_table_cleanup(
    const lc_allocator *allocator, lc_pouch_index_term_posting_table *table) {
  size_t index;

  if (table == NULL) {
    return;
  }
  for (index = 0U; index < table->count; ++index) {
    lc_pouch_index_adaptive_posting_cleanup(allocator,
                                            &table->items[index].posting);
  }
  lc_free_with_allocator(allocator, table->items);
  memset(table, 0, sizeof(*table));
}

int lc_pouch_index_term_posting_table_put(
    const lc_allocator *allocator, lc_pouch_index_term_posting_table *table,
    unsigned long term_id, const unsigned long *doc_ids, size_t doc_id_count,
    lc_error *error) {
  lc_pouch_index_adaptive_posting posting;
  size_t position;
  size_t index;
  int added;
  int rc;

  if (table == NULL || term_id == 0UL ||
      (doc_ids == NULL && doc_id_count > 0U)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term posting put requires table, term, "
                        "and docIDs",
                        NULL, NULL, NULL);
  }
  memset(&posting, 0, sizeof(posting));
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < doc_id_count; ++index) {
    rc = lc_pouch_index_adaptive_posting_append_sorted_unique(
        &posting, doc_ids[index], &added, allocator, error);
  }
  if (rc != LC_OK) {
    lc_pouch_index_adaptive_posting_cleanup(allocator, &posting);
    return rc;
  }
  if (lc_pouch_index_term_posting_table_find_position(table, term_id,
                                                      &position)) {
    lc_pouch_index_adaptive_posting_cleanup(allocator,
                                            &table->items[position].posting);
    table->items[position].posting = posting;
    return LC_OK;
  }
  rc = lc_pouch_index_term_posting_table_reserve(allocator, table,
                                                 table->count + 1U, error);
  if (rc != LC_OK) {
    lc_pouch_index_adaptive_posting_cleanup(allocator, &posting);
    return rc;
  }
  if (position < table->count) {
    memmove(&table->items[position + 1U], &table->items[position],
            (table->count - position) * sizeof(table->items[0]));
  }
  table->items[position].term_id = term_id;
  table->items[position].posting = posting;
  ++table->count;
  return LC_OK;
}

int lc_pouch_index_term_posting_table_append_to_set(
    const lc_pouch_index_term_posting_table *table, unsigned long term_id,
    lc_pouch_index_docid_set *set, const lc_allocator *allocator,
    lc_error *error) {
  size_t position;

  if (set == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term posting decode requires output set",
                        NULL, NULL, NULL);
  }
  if (!lc_pouch_index_term_posting_table_find_position(table, term_id,
                                                       &position)) {
    return LC_OK;
  }
  return lc_pouch_index_adaptive_posting_append_to_set(
      &table->items[position].posting, set, allocator, error);
}

typedef struct lc_pouch_index_term_generation_buffer {
  char *bytes;
  size_t length;
  size_t capacity;
} lc_pouch_index_term_generation_buffer;

static void lc_pouch_index_term_generation_buffer_cleanup(
    const lc_allocator *allocator,
    lc_pouch_index_term_generation_buffer *buffer) {
  if (buffer == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, buffer->bytes);
  memset(buffer, 0, sizeof(*buffer));
}

static int lc_pouch_index_term_generation_buffer_append(
    const lc_allocator *allocator,
    lc_pouch_index_term_generation_buffer *buffer, const char *bytes,
    size_t length, lc_error *error) {
  char *next_bytes;
  size_t needed;
  size_t next_capacity;

  if (buffer == NULL || (bytes == NULL && length > 0U)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation append requires buffer",
                        NULL, NULL, NULL);
  }
  if (length > ((size_t)-1) - buffer->length - 1U) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch index term generation exceeds local limit", NULL,
                        NULL, NULL);
  }
  needed = buffer->length + length + 1U;
  if (needed > buffer->capacity) {
    next_capacity = buffer->capacity == 0U ? 256U : buffer->capacity;
    while (next_capacity < needed) {
      if (next_capacity > ((size_t)-1 / 2U)) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "pouch index term generation exceeds local limit",
                            NULL, NULL, NULL);
      }
      next_capacity *= 2U;
    }
    next_bytes = (char *)lc_alloc_with_allocator(allocator, next_capacity);
    if (next_bytes == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch index term generation",
                          NULL, NULL, NULL);
    }
    if (buffer->bytes != NULL) {
      memcpy(next_bytes, buffer->bytes, buffer->length);
      lc_free_with_allocator(allocator, buffer->bytes);
    }
    buffer->bytes = next_bytes;
    buffer->capacity = next_capacity;
  }
  if (length > 0U) {
    memcpy(buffer->bytes + buffer->length, bytes, length);
  }
  buffer->length += length;
  buffer->bytes[buffer->length] = '\0';
  return LC_OK;
}

static int lc_pouch_index_term_generation_buffer_append_cstr(
    const lc_allocator *allocator,
    lc_pouch_index_term_generation_buffer *buffer, const char *text,
    lc_error *error) {
  return lc_pouch_index_term_generation_buffer_append(
      allocator, buffer, text, text != NULL ? strlen(text) : 0U, error);
}

static int lc_pouch_index_term_generation_buffer_append_ulong_line(
    const lc_allocator *allocator,
    lc_pouch_index_term_generation_buffer *buffer, const char *name,
    unsigned long value, lc_error *error) {
  char line[128];
  int written;

  written = snprintf(line, sizeof(line), "%s=%lu\n", name, value);
  if (written < 0 || (size_t)written >= sizeof(line)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "failed to format pouch index term generation header",
                        NULL, NULL, NULL);
  }
  return lc_pouch_index_term_generation_buffer_append(allocator, buffer, line,
                                                      (size_t)written, error);
}

static char *lc_pouch_index_term_generation_payload_hex(
    const lc_allocator *allocator,
    const lc_pouch_index_adaptive_posting *posting,
    lc_pouch_index_adaptive_posting_kind kind) {
  if (kind == LC_POUCH_INDEX_ADAPTIVE_POSTING_DENSE) {
    return lc_pouch_index_term_hex_encode_bytes(
        allocator, (const char *)posting->dense.bits, posting->dense.length);
  }
  return lc_pouch_index_term_hex_encode_bytes(
      allocator, (const char *)posting->sparse.bytes, posting->sparse.length);
}

void lc_pouch_index_term_generation_cleanup(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation) {
  if (generation == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, generation->namespace_name);
  lc_pouch_index_term_posting_table_cleanup(allocator, &generation->postings);
  lc_pouch_index_term_table_cleanup(allocator, &generation->terms);
  memset(generation, 0, sizeof(*generation));
}

int lc_pouch_index_term_generation_encode(
    const lc_pouch_index_term_generation *generation,
    const lc_allocator *allocator, char **out_bytes, size_t *out_length,
    lc_error *error) {
  lc_pouch_index_term_generation_buffer buffer;
  char *namespace_hex;
  size_t index;
  int rc;

  if (out_bytes == NULL || out_length == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation encode requires outputs",
                        NULL, NULL, NULL);
  }
  *out_bytes = NULL;
  *out_length = 0U;
  if (generation == NULL || generation->namespace_name == NULL ||
      generation->namespace_name[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation encode requires "
                        "namespace and generation",
                        NULL, NULL, NULL);
  }
  memset(&buffer, 0, sizeof(buffer));
  namespace_hex =
      lc_pouch_index_term_hex_encode(allocator, generation->namespace_name);
  if (namespace_hex == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index term generation "
                        "namespace",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_index_term_generation_buffer_append_cstr(
      allocator, &buffer, "format=" LC_POUCH_INDEX_TERM_GENERATION_FORMAT "\n",
      error);
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_buffer_append_ulong_line(
        allocator, &buffer, "version", LC_POUCH_INDEX_TERM_GENERATION_VERSION,
        error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_buffer_append_ulong_line(
        allocator, &buffer, "index_seq", generation->index_seq, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_buffer_append_ulong_line(
        allocator, &buffer, "row_count", generation->row_count, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_buffer_append_ulong_line(
        allocator, &buffer, "row_hash", generation->row_hash, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_buffer_append_cstr(
        allocator, &buffer, "namespace_hex=", error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_buffer_append_cstr(
        allocator, &buffer, namespace_hex, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_buffer_append_cstr(allocator, &buffer,
                                                           "\n", error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_buffer_append_ulong_line(
        allocator, &buffer, "term_count", generation->terms.count, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_generation_buffer_append_ulong_line(
        allocator, &buffer, "posting_count", generation->postings.count, error);
  }
  for (index = 0U; rc == LC_OK && index < generation->terms.count; ++index) {
    const lc_pouch_index_term_entry *term;
    char line[128];
    int written;

    term = &generation->terms.items[index];
    if (term->term_id == 0UL ||
        !lc_pouch_index_term_hex_token_valid(term->field_hex) ||
        !lc_pouch_index_term_value_hex_token_valid(term->value_hex) ||
        !lc_pouch_index_term_value_type_valid(term->value_type)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation contains invalid term",
                        NULL, NULL, "pouch");
      break;
    }
    written = snprintf(line, sizeof(line), "term %lu %c ", term->term_id,
                       term->value_type);
    if (written < 0 || (size_t)written >= sizeof(line)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "failed to format pouch index term generation term",
                        NULL, NULL, NULL);
      break;
    }
    rc = lc_pouch_index_term_generation_buffer_append(allocator, &buffer, line,
                                                      (size_t)written, error);
    if (rc == LC_OK) {
      rc = lc_pouch_index_term_generation_buffer_append_cstr(
          allocator, &buffer, term->field_hex, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_index_term_generation_buffer_append_cstr(allocator, &buffer,
                                                             " ", error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_index_term_generation_buffer_append_cstr(
          allocator, &buffer, term->value_hex, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_index_term_generation_buffer_append_cstr(allocator, &buffer,
                                                             "\n", error);
    }
  }
  for (index = 0U; rc == LC_OK && index < generation->postings.count; ++index) {
    const lc_pouch_index_term_posting_entry *entry;
    lc_pouch_index_adaptive_posting_kind kind;
    const lc_pouch_index_posting *sparse;
    const lc_pouch_index_dense_posting *dense;
    char *payload_hex;
    char line[192];
    unsigned long count;
    unsigned long max_doc_id;
    size_t payload_length;
    int written;

    entry = &generation->postings.items[index];
    if (!lc_pouch_index_term_table_contains_id(&generation->terms,
                                               entry->term_id)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation posting references "
                        "unknown term",
                        NULL, NULL, "pouch");
      break;
    }
    kind = lc_pouch_index_adaptive_posting_selected_kind(&entry->posting);
    sparse = &entry->posting.sparse;
    dense = &entry->posting.dense;
    payload_hex = lc_pouch_index_term_generation_payload_hex(
        allocator, &entry->posting, kind);
    if (payload_hex == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index term generation "
                        "posting payload",
                        NULL, NULL, NULL);
      break;
    }
    if (kind == LC_POUCH_INDEX_ADAPTIVE_POSTING_DENSE) {
      count = (unsigned long)dense->count;
      max_doc_id = dense->max_doc_id;
      payload_length = dense->length;
    } else {
      count = (unsigned long)sparse->count;
      max_doc_id = sparse->has_last_doc_id ? sparse->last_doc_id : 0UL;
      payload_length = sparse->length;
    }
    written = snprintf(
        line, sizeof(line), "posting %lu %c %lu %lu %lu ", entry->term_id,
        kind == LC_POUCH_INDEX_ADAPTIVE_POSTING_DENSE ? 'd' : 's', count,
        max_doc_id, (unsigned long)payload_length);
    if (written < 0 || (size_t)written >= sizeof(line)) {
      lc_free_with_allocator(allocator, payload_hex);
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "failed to format pouch index term generation "
                        "posting",
                        NULL, NULL, NULL);
      break;
    }
    rc = lc_pouch_index_term_generation_buffer_append(allocator, &buffer, line,
                                                      (size_t)written, error);
    if (rc == LC_OK) {
      rc = lc_pouch_index_term_generation_buffer_append_cstr(
          allocator, &buffer, payload_hex, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_index_term_generation_buffer_append_cstr(allocator, &buffer,
                                                             "\n", error);
    }
    lc_free_with_allocator(allocator, payload_hex);
  }
  lc_free_with_allocator(allocator, namespace_hex);
  if (rc != LC_OK) {
    lc_pouch_index_term_generation_buffer_cleanup(allocator, &buffer);
    return rc;
  }
  *out_bytes = buffer.bytes;
  *out_length = buffer.length;
  return LC_OK;
}

static char *lc_pouch_index_term_generation_next_line(char **cursor) {
  char *line;
  char *end;

  if (cursor == NULL || *cursor == NULL) {
    return NULL;
  }
  line = *cursor;
  end = strchr(line, '\n');
  if (end == NULL) {
    *cursor = NULL;
  } else {
    *end = '\0';
    *cursor = end + 1;
    if (**cursor == '\0') {
      *cursor = NULL;
    }
  }
  return line;
}

static int lc_pouch_index_term_generation_header_ulong(char *line,
                                                       const char *name,
                                                       unsigned long *out) {
  size_t name_len;
  char *end;
  unsigned long value;

  if (line == NULL || name == NULL || out == NULL) {
    return 0;
  }
  name_len = strlen(name);
  if (strncmp(line, name, name_len) != 0 || line[name_len] != '=') {
    return 0;
  }
  errno = 0;
  value = strtoul(line + name_len + 1U, &end, 10);
  if (errno != 0 || end == line + name_len + 1U || *end != '\0') {
    return 0;
  }
  *out = value;
  return 1;
}

static int lc_pouch_index_term_generation_hex_value(unsigned char ch,
                                                    unsigned char *value) {
  if (ch >= (unsigned char)'0' && ch <= (unsigned char)'9') {
    *value = (unsigned char)(ch - (unsigned char)'0');
    return 1;
  }
  if (ch >= (unsigned char)'a' && ch <= (unsigned char)'f') {
    *value = (unsigned char)(ch - (unsigned char)'a' + 10U);
    return 1;
  }
  if (ch >= (unsigned char)'A' && ch <= (unsigned char)'F') {
    *value = (unsigned char)(ch - (unsigned char)'A' + 10U);
    return 1;
  }
  return 0;
}

static int lc_pouch_index_term_generation_hex_decode(
    const lc_allocator *allocator, const char *hex, unsigned char **out_bytes,
    size_t *out_length, lc_error *error) {
  unsigned char *bytes;
  size_t hex_length;
  size_t index;

  if (out_bytes == NULL || out_length == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation hex decode requires "
                        "outputs",
                        NULL, NULL, NULL);
  }
  *out_bytes = NULL;
  *out_length = 0U;
  if (hex == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation hex decode requires "
                        "input",
                        NULL, NULL, "pouch");
  }
  if (strcmp(hex, "-") == 0) {
    return LC_OK;
  }
  if (!lc_pouch_index_term_hex_token_valid(hex)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation has invalid hex token",
                        NULL, NULL, "pouch");
  }
  hex_length = strlen(hex);
  bytes = (unsigned char *)lc_alloc_with_allocator(allocator,
                                                   (hex_length / 2U) + 1U);
  if (bytes == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index term generation hex",
                        NULL, NULL, NULL);
  }
  for (index = 0U; index < hex_length; index += 2U) {
    unsigned char hi;
    unsigned char lo;

    if (!lc_pouch_index_term_generation_hex_value((unsigned char)hex[index],
                                                  &hi) ||
        !lc_pouch_index_term_generation_hex_value(
            (unsigned char)hex[index + 1U], &lo)) {
      lc_free_with_allocator(allocator, bytes);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index term generation has invalid hex byte",
                          NULL, NULL, "pouch");
    }
    bytes[index / 2U] = (unsigned char)((hi << 4U) | lo);
  }
  bytes[hex_length / 2U] = '\0';
  *out_bytes = bytes;
  *out_length = hex_length / 2U;
  return LC_OK;
}

static int lc_pouch_index_term_generation_decode_posting(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    unsigned long term_id, char kind, unsigned long count,
    unsigned long max_doc_id, const char *payload_hex, lc_error *error) {
  lc_pouch_index_docid_set decoded;
  lc_pouch_index_posting sparse;
  lc_pouch_index_dense_posting dense;
  unsigned char *payload;
  size_t payload_length;
  int rc;

  memset(&decoded, 0, sizeof(decoded));
  memset(&sparse, 0, sizeof(sparse));
  memset(&dense, 0, sizeof(dense));
  payload = NULL;
  payload_length = 0U;
  rc = lc_pouch_index_term_generation_hex_decode(
      allocator, payload_hex, &payload, &payload_length, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (kind == 's') {
    sparse.bytes = payload;
    sparse.length = payload_length;
    sparse.capacity = payload_length;
    sparse.count = (size_t)count;
    sparse.last_doc_id = max_doc_id;
    sparse.has_last_doc_id = count > 0UL ? 1 : 0;
    if ((count == 0UL && payload_length != 0U) ||
        (count > 0UL && payload_length == 0U)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation sparse posting is "
                        "invalid",
                        NULL, NULL, "pouch");
    } else {
      rc = lc_pouch_index_posting_append_to_set(&sparse, &decoded, allocator,
                                                error);
    }
    payload = NULL;
  } else if (kind == 'd') {
    dense.bits = payload;
    dense.length = payload_length;
    dense.capacity = payload_length;
    dense.count = (size_t)count;
    dense.max_doc_id = max_doc_id;
    dense.last_doc_id = max_doc_id;
    dense.has_last_doc_id = count > 0UL ? 1 : 0;
    if ((count == 0UL && payload_length != 0U) ||
        (count > 0UL && payload_length == 0U)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation dense posting is "
                        "invalid",
                        NULL, NULL, "pouch");
    } else {
      rc = lc_pouch_index_dense_posting_append_to_set(&dense, &decoded,
                                                      allocator, error);
    }
    payload = NULL;
  } else {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index term generation has invalid posting kind",
                      NULL, NULL, "pouch");
  }
  if (rc == LC_OK && decoded.count != (size_t)count) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index term generation posting count mismatch",
                      NULL, NULL, "pouch");
  }
  if (rc == LC_OK && count > 0UL &&
      decoded.items[decoded.count - 1U] != max_doc_id) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index term generation posting max docID "
                      "mismatch",
                      NULL, NULL, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_term_posting_table_put(allocator, &generation->postings,
                                               term_id, decoded.items,
                                               decoded.count, error);
  }
  lc_free_with_allocator(allocator, payload);
  if (sparse.bytes != NULL) {
    lc_pouch_index_posting_cleanup(allocator, &sparse);
  }
  if (dense.bits != NULL) {
    lc_pouch_index_dense_posting_cleanup(allocator, &dense);
  }
  lc_pouch_index_docid_set_cleanup(allocator, &decoded);
  return rc;
}

int lc_pouch_index_term_generation_decode(
    const lc_allocator *allocator, const char *bytes, size_t length,
    unsigned long expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, lc_pouch_index_term_generation *generation,
    lc_error *error) {
  lc_pouch_index_term_generation decoded;
  char *copy;
  char *cursor;
  char *line;
  unsigned long version;
  unsigned long term_count;
  unsigned long posting_count;
  size_t index;
  int rc;

  if (generation == NULL || bytes == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation decode requires bytes "
                        "and output",
                        NULL, NULL, NULL);
  }
  if (memchr(bytes, '\0', length) != NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation contains NUL bytes", NULL,
                        NULL, "pouch");
  }
  memset(&decoded, 0, sizeof(decoded));
  copy = (char *)lc_alloc_with_allocator(allocator, length + 1U);
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index term generation "
                        "decode buffer",
                        NULL, NULL, NULL);
  }
  memcpy(copy, bytes, length);
  copy[length] = '\0';
  cursor = copy;
  rc = LC_OK;
  line = lc_pouch_index_term_generation_next_line(&cursor);
  if (line == NULL ||
      strcmp(line, "format=" LC_POUCH_INDEX_TERM_GENERATION_FORMAT) != 0) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index term generation has invalid format", NULL,
                      NULL, "pouch");
  }
  line = rc == LC_OK ? lc_pouch_index_term_generation_next_line(&cursor) : NULL;
  if (rc == LC_OK && (!lc_pouch_index_term_generation_header_ulong(
                          line, "version", &version) ||
                      version != LC_POUCH_INDEX_TERM_GENERATION_VERSION)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index term generation has invalid version", NULL,
                      NULL, "pouch");
  }
  line = rc == LC_OK ? lc_pouch_index_term_generation_next_line(&cursor) : NULL;
  if (rc == LC_OK && !lc_pouch_index_term_generation_header_ulong(
                         line, "index_seq", &decoded.index_seq)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index term generation missing index sequence",
                      NULL, NULL, "pouch");
  }
  line = rc == LC_OK ? lc_pouch_index_term_generation_next_line(&cursor) : NULL;
  if (rc == LC_OK && !lc_pouch_index_term_generation_header_ulong(
                         line, "row_count", &decoded.row_count)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index term generation missing row count", NULL,
                      NULL, "pouch");
  }
  line = rc == LC_OK ? lc_pouch_index_term_generation_next_line(&cursor) : NULL;
  if (rc == LC_OK && !lc_pouch_index_term_generation_header_ulong(
                         line, "row_hash", &decoded.row_hash)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index term generation missing row hash", NULL,
                      NULL, "pouch");
  }
  line = rc == LC_OK ? lc_pouch_index_term_generation_next_line(&cursor) : NULL;
  if (rc == LC_OK) {
    const char prefix[] = "namespace_hex=";
    unsigned char *namespace_bytes;
    size_t namespace_length;

    namespace_bytes = NULL;
    namespace_length = 0U;
    if (line == NULL || strncmp(line, prefix, sizeof(prefix) - 1U) != 0 ||
        lc_pouch_index_term_generation_hex_decode(
            allocator, line + sizeof(prefix) - 1U, &namespace_bytes,
            &namespace_length, error) != LC_OK ||
        namespace_length == 0U) {
      rc = error != NULL && error->code != LC_OK
               ? error->code
               : lc_error_set(error, LC_ERR_INVALID, 0L,
                              "pouch index term generation missing namespace",
                              NULL, NULL, "pouch");
    } else {
      decoded.namespace_name = (char *)namespace_bytes;
      namespace_bytes = NULL;
    }
    lc_free_with_allocator(allocator, namespace_bytes);
  }
  line = rc == LC_OK ? lc_pouch_index_term_generation_next_line(&cursor) : NULL;
  if (rc == LC_OK && !lc_pouch_index_term_generation_header_ulong(
                         line, "term_count", &term_count)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index term generation missing term count", NULL,
                      NULL, "pouch");
  }
  line = rc == LC_OK ? lc_pouch_index_term_generation_next_line(&cursor) : NULL;
  if (rc == LC_OK && !lc_pouch_index_term_generation_header_ulong(
                         line, "posting_count", &posting_count)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index term generation missing posting count", NULL,
                      NULL, "pouch");
  }
  for (index = 0U; rc == LC_OK && index < term_count; ++index) {
    char *term_id_token;
    char *type_token;
    char *field_hex;
    char *value_hex;
    unsigned long term_id;

    line = lc_pouch_index_term_generation_next_line(&cursor);
    if (line == NULL || strncmp(line, "term ", sizeof("term ") - 1U) != 0) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation term is missing", NULL,
                        NULL, "pouch");
      break;
    }
    line += sizeof("term ") - 1U;
    term_id_token = lc_pouch_index_term_next_token(&line, 0);
    type_token = lc_pouch_index_term_next_token(&line, 0);
    field_hex = lc_pouch_index_term_next_token(&line, 0);
    value_hex = lc_pouch_index_term_next_token(&line, 1);
    if (lc_pouch_index_term_parse_ulong_token(
            term_id_token, &term_id,
            "pouch index term generation term has invalid id",
            error) != LC_OK ||
        type_token == NULL || type_token[0] == '\0' || type_token[1] != '\0' ||
        field_hex == NULL || value_hex == NULL) {
      rc = error != NULL && error->code != LC_OK
               ? error->code
               : lc_error_set(error, LC_ERR_INVALID, 0L,
                              "pouch index term generation term is invalid",
                              NULL, NULL, "pouch");
      break;
    }
    rc = lc_pouch_index_term_table_add_entry(
        allocator, &decoded.terms, field_hex, value_hex, type_token[0], term_id,
        NULL, error);
  }
  for (index = 0U; rc == LC_OK && index < posting_count; ++index) {
    char *term_id_token;
    char *kind_token;
    char *count_token;
    char *max_doc_id_token;
    char *payload_length_token;
    char *payload_hex;
    unsigned long term_id;
    unsigned long count;
    unsigned long max_doc_id;
    unsigned long payload_length;

    line = lc_pouch_index_term_generation_next_line(&cursor);
    if (line == NULL ||
        strncmp(line, "posting ", sizeof("posting ") - 1U) != 0) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation posting is missing", NULL,
                        NULL, "pouch");
      break;
    }
    line += sizeof("posting ") - 1U;
    term_id_token = lc_pouch_index_term_next_token(&line, 0);
    kind_token = lc_pouch_index_term_next_token(&line, 0);
    count_token = lc_pouch_index_term_next_token(&line, 0);
    max_doc_id_token = lc_pouch_index_term_next_token(&line, 0);
    payload_length_token = lc_pouch_index_term_next_token(&line, 0);
    payload_hex = lc_pouch_index_term_next_token(&line, 1);
    if (lc_pouch_index_term_parse_ulong_token(
            term_id_token, &term_id,
            "pouch index term generation posting has invalid term id",
            error) != LC_OK ||
        lc_pouch_index_term_parse_ulong_token(
            count_token, &count,
            "pouch index term generation posting has invalid count",
            error) != LC_OK ||
        lc_pouch_index_term_parse_ulong_token(
            max_doc_id_token, &max_doc_id,
            "pouch index term generation posting has invalid max docID",
            error) != LC_OK ||
        lc_pouch_index_term_parse_ulong_token(
            payload_length_token, &payload_length,
            "pouch index term generation posting has invalid payload length",
            error) != LC_OK ||
        kind_token == NULL || kind_token[0] == '\0' || kind_token[1] != '\0' ||
        payload_hex == NULL ||
        !lc_pouch_index_term_table_contains_id(&decoded.terms, term_id)) {
      rc = error != NULL && error->code != LC_OK
               ? error->code
               : lc_error_set(error, LC_ERR_INVALID, 0L,
                              "pouch index term generation posting is "
                              "invalid",
                              NULL, NULL, "pouch");
      break;
    }
    if ((strcmp(payload_hex, "-") == 0
             ? 0UL
             : (unsigned long)(strlen(payload_hex) / 2U)) != payload_length) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation posting payload length "
                        "mismatch",
                        NULL, NULL, "pouch");
      break;
    }
    rc = lc_pouch_index_term_generation_decode_posting(
        allocator, &decoded, term_id, kind_token[0], count, max_doc_id,
        payload_hex, error);
  }
  if (rc == LC_OK && cursor != NULL) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index term generation has trailing records", NULL,
                      NULL, "pouch");
  }
  if (rc == LC_OK && (decoded.index_seq != expected_index_seq ||
                      decoded.row_count != expected_row_count ||
                      decoded.row_hash != expected_row_hash)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index term generation identity mismatch", NULL,
                      NULL, "pouch");
  }
  lc_free_with_allocator(allocator, copy);
  if (rc != LC_OK) {
    lc_pouch_index_term_generation_cleanup(allocator, &decoded);
    return rc;
  }
  lc_pouch_index_term_generation_cleanup(allocator, generation);
  *generation = decoded;
  return LC_OK;
}

int lc_pouch_index_term_generation_load_bytes(
    const lc_allocator *allocator, const char *bytes, size_t length,
    unsigned long expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, lc_pouch_index_term_generation *generation,
    int *valid, lc_error *error) {
  lc_error decode_error;
  int rc;

  if (generation == NULL || valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation load requires generation "
                        "and valid output",
                        NULL, NULL, NULL);
  }
  memset(generation, 0, sizeof(*generation));
  *valid = 0;
  lc_error_init(&decode_error);
  rc = lc_pouch_index_term_generation_decode(
      allocator, bytes, length, expected_index_seq, expected_row_count,
      expected_row_hash, generation, &decode_error);
  if (rc == LC_OK) {
    *valid = 1;
  } else if (rc == LC_ERR_INVALID) {
    lc_pouch_index_term_generation_cleanup(allocator, generation);
    rc = LC_OK;
  } else if (error != NULL && decode_error.code != LC_OK) {
    rc = lc_error_set(error, decode_error.code, 0L, decode_error.message,
                      decode_error.detail, decode_error.server_code,
                      decode_error.correlation_id);
  }
  lc_error_cleanup(&decode_error);
  return rc;
}

static int lc_pouch_index_term_generation_read_file(
    const lc_allocator *allocator, const char *path, char **out_bytes,
    size_t *out_length, int *present, lc_error *error) {
  FILE *fp;
  char *bytes;
  size_t length;
  size_t capacity;
  size_t got;

  if (path == NULL || out_bytes == NULL || out_length == NULL ||
      present == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation read requires path and "
                        "outputs",
                        NULL, NULL, NULL);
  }
  *out_bytes = NULL;
  *out_length = 0U;
  *present = 0;
  fp = fopen(path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch index term generation",
                        strerror(errno), NULL, NULL);
  }
  *present = 1;
  bytes = NULL;
  length = 0U;
  capacity = 0U;
  for (;;) {
    if (capacity - length <= 1U) {
      char *next_bytes;
      size_t next_capacity;

      next_capacity = capacity == 0U ? 4096U : capacity * 2U;
      if (next_capacity <= capacity || next_capacity == (size_t)-1) {
        fclose(fp);
        lc_free_with_allocator(allocator, bytes);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "pouch index term generation exceeds local limit",
                            NULL, NULL, NULL);
      }
      next_bytes = (char *)lc_alloc_with_allocator(allocator, next_capacity);
      if (next_bytes == NULL) {
        fclose(fp);
        lc_free_with_allocator(allocator, bytes);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch index term generation",
                            NULL, NULL, NULL);
      }
      if (bytes != NULL) {
        memcpy(next_bytes, bytes, length);
        lc_free_with_allocator(allocator, bytes);
      }
      bytes = next_bytes;
      capacity = next_capacity;
    }
    got = fread(bytes + length, 1U, capacity - length - 1U, fp);
    length += got;
    if (got == 0U) {
      if (ferror(fp)) {
        fclose(fp);
        lc_free_with_allocator(allocator, bytes);
        return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                            "failed to read pouch index term generation",
                            strerror(errno), NULL, NULL);
      }
      break;
    }
  }
  if (fclose(fp) != 0) {
    lc_free_with_allocator(allocator, bytes);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch index term generation",
                        strerror(errno), NULL, NULL);
  }
  bytes[length] = '\0';
  *out_bytes = bytes;
  *out_length = length;
  return LC_OK;
}

int lc_pouch_index_term_generation_validate_file(
    const lc_allocator *allocator, const char *path,
    unsigned long expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, int *present, int *valid,
    lc_error *error) {
  FILE *fp;
  char line[256];
  unsigned long value;

  (void)allocator;
  if (valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation validation requires "
                        "valid output",
                        NULL, NULL, NULL);
  }
  *valid = 0;
  if (present != NULL) {
    *present = 0;
  }
  fp = fopen(path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, (long)errno,
                        "failed to open pouch index term generation",
                        strerror(errno), NULL, NULL);
  }
  if (present != NULL) {
    *present = 1;
  }
  if (fgets(line, sizeof(line), fp) == NULL) {
    fclose(fp);
    return LC_OK;
  }
  line[strcspn(line, "\n")] = '\0';
  if (strcmp(line, "format=" LC_POUCH_INDEX_TERM_GENERATION_FORMAT) != 0) {
    fclose(fp);
    return LC_OK;
  }
  if (fgets(line, sizeof(line), fp) == NULL) {
    fclose(fp);
    return LC_OK;
  }
  line[strcspn(line, "\n")] = '\0';
  if (!lc_pouch_index_term_generation_header_ulong(line, "version", &value) ||
      value != LC_POUCH_INDEX_TERM_GENERATION_VERSION) {
    fclose(fp);
    return LC_OK;
  }
  if (fgets(line, sizeof(line), fp) == NULL) {
    fclose(fp);
    return LC_OK;
  }
  line[strcspn(line, "\n")] = '\0';
  if (!lc_pouch_index_term_generation_header_ulong(line, "index_seq", &value) ||
      value != expected_index_seq) {
    fclose(fp);
    return LC_OK;
  }
  if (fgets(line, sizeof(line), fp) == NULL) {
    fclose(fp);
    return LC_OK;
  }
  line[strcspn(line, "\n")] = '\0';
  if (!lc_pouch_index_term_generation_header_ulong(line, "row_count", &value) ||
      value != expected_row_count) {
    fclose(fp);
    return LC_OK;
  }
  if (fgets(line, sizeof(line), fp) == NULL) {
    fclose(fp);
    return LC_OK;
  }
  line[strcspn(line, "\n")] = '\0';
  if (!lc_pouch_index_term_generation_header_ulong(line, "row_hash", &value) ||
      value != expected_row_hash) {
    fclose(fp);
    return LC_OK;
  }
  if (fclose(fp) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch index term generation",
                        strerror(errno), NULL, NULL);
  }
  *valid = 1;
  return LC_OK;
}

int lc_pouch_index_term_generation_load_file(
    const lc_allocator *allocator, const char *path,
    unsigned long expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, lc_pouch_index_term_generation *generation,
    int *present, int *valid, lc_error *error) {
  lc_error decode_error;
  char *bytes;
  size_t length;
  int rc;

  if (generation == NULL || valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term generation load requires generation "
                        "and valid output",
                        NULL, NULL, NULL);
  }
  memset(generation, 0, sizeof(*generation));
  *valid = 0;
  bytes = NULL;
  length = 0U;
  lc_error_init(&decode_error);
  rc = lc_pouch_index_term_generation_read_file(allocator, path, &bytes,
                                                &length, present, error);
  if (rc == LC_OK && present != NULL && *present) {
    rc = lc_pouch_index_term_generation_load_bytes(
        allocator, bytes, length, expected_index_seq, expected_row_count,
        expected_row_hash, generation, valid, error);
  }
  lc_error_cleanup(&decode_error);
  lc_free_with_allocator(allocator, bytes);
  return rc;
}

int lc_pouch_index_term_field_parse_line(char *line,
                                         lc_pouch_index_term_field *field,
                                         const lc_allocator *allocator,
                                         lc_error *error) {
  char *cursor;
  char *field_hex;
  char *first_token;
  char *count_token;
  char *first_byte_token;
  char *byte_count_token;
  unsigned long first_line;
  unsigned long line_count;
  unsigned long first_byte;
  unsigned long byte_count;
  int rc;

  if (line == NULL || field == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term field requires line and output", NULL,
                        NULL, NULL);
  }
  if (strncmp(line, "term_field ", sizeof("term_field ") - 1U) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term field has invalid prefix", NULL, NULL,
                        NULL);
  }
  cursor = line + sizeof("term_field ") - 1U;
  field_hex = lc_pouch_index_term_next_token(&cursor, 0);
  first_token = lc_pouch_index_term_next_token(&cursor, 0);
  count_token = lc_pouch_index_term_next_token(&cursor, 0);
  first_byte_token = lc_pouch_index_term_next_token(&cursor, 0);
  byte_count_token = lc_pouch_index_term_next_token(&cursor, 1);
  if (field_hex == NULL || strcmp(field_hex, "-") == 0 ||
      !lc_pouch_index_term_hex_token_valid(field_hex) || first_token == NULL ||
      count_token == NULL || first_byte_token == NULL ||
      byte_count_token == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term field has invalid fields", NULL, NULL,
                        NULL);
  }
  rc = lc_pouch_index_term_parse_ulong_token(
      first_token, &first_line, "pouch index term field has invalid first line",
      error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_index_term_parse_ulong_token(
      count_token, &line_count, "pouch index term field has invalid line count",
      error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_index_term_parse_ulong_token(
      first_byte_token, &first_byte,
      "pouch index term field has invalid first byte", error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_index_term_parse_ulong_token(
      byte_count_token, &byte_count,
      "pouch index term field has invalid byte count", error);
  if (rc != LC_OK) {
    return rc;
  }
  field->field_hex = lc_strdup_with_allocator(allocator, field_hex);
  if (field->field_hex == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index term field", NULL, NULL,
                        NULL);
  }
  field->first_line = first_line;
  field->line_count = line_count;
  field->first_byte = first_byte;
  field->byte_count = byte_count;
  return LC_OK;
}

int lc_pouch_index_term_value_parse_line(char *line,
                                         lc_pouch_index_term_value *value,
                                         const lc_allocator *allocator,
                                         lc_error *error) {
  char *cursor;
  char *field_hex;
  char *value_hex;
  char *first_token;
  char *count_token;
  char *first_byte_token;
  char *byte_count_token;
  unsigned long first_line;
  unsigned long line_count;
  unsigned long first_byte;
  unsigned long byte_count;
  int rc;

  if (line == NULL || value == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term value requires line and output", NULL,
                        NULL, NULL);
  }
  if (strncmp(line, "term_value ", sizeof("term_value ") - 1U) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term value has invalid prefix", NULL, NULL,
                        NULL);
  }
  cursor = line + sizeof("term_value ") - 1U;
  field_hex = lc_pouch_index_term_next_token(&cursor, 0);
  value_hex = lc_pouch_index_term_next_token(&cursor, 0);
  first_token = lc_pouch_index_term_next_token(&cursor, 0);
  count_token = lc_pouch_index_term_next_token(&cursor, 0);
  first_byte_token = lc_pouch_index_term_next_token(&cursor, 0);
  byte_count_token = lc_pouch_index_term_next_token(&cursor, 1);
  if (field_hex == NULL || value_hex == NULL || strcmp(field_hex, "-") == 0 ||
      !lc_pouch_index_term_hex_token_valid(field_hex) ||
      !lc_pouch_index_term_hex_token_valid(value_hex) || first_token == NULL ||
      count_token == NULL || first_byte_token == NULL ||
      byte_count_token == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term value has invalid fields", NULL, NULL,
                        NULL);
  }
  rc = lc_pouch_index_term_parse_ulong_token(
      first_token, &first_line, "pouch index term value has invalid first line",
      error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_index_term_parse_ulong_token(
      count_token, &line_count, "pouch index term value has invalid line count",
      error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_index_term_parse_ulong_token(
      first_byte_token, &first_byte,
      "pouch index term value has invalid first byte", error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_index_term_parse_ulong_token(
      byte_count_token, &byte_count,
      "pouch index term value has invalid byte count", error);
  if (rc != LC_OK) {
    return rc;
  }
  value->field_hex = lc_strdup_with_allocator(allocator, field_hex);
  value->value_hex = lc_strdup_with_allocator(allocator, value_hex);
  if (value->field_hex == NULL || value->value_hex == NULL) {
    lc_free_with_allocator(allocator, value->field_hex);
    lc_free_with_allocator(allocator, value->value_hex);
    memset(value, 0, sizeof(*value));
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index term value", NULL, NULL,
                        NULL);
  }
  value->first_line = first_line;
  value->line_count = line_count;
  value->first_byte = first_byte;
  value->byte_count = byte_count;
  return LC_OK;
}

void lc_pouch_index_term_ranges_cleanup(const lc_allocator *allocator,
                                        lc_pouch_index_term_range *ranges) {
  lc_free_with_allocator(allocator, ranges);
}

int lc_pouch_index_term_fields_select_range(
    const lc_pouch_index_term_field *fields, size_t field_count,
    const char *field_hex, const lc_pouch_index_term_key *terms,
    size_t term_count, unsigned long term_line_count,
    unsigned long term_byte_count, unsigned long *first_line,
    unsigned long *line_count, unsigned long *first_byte,
    unsigned long *byte_count) {
  unsigned long first;
  unsigned long count;
  unsigned long end;
  unsigned long min_first;
  unsigned long max_end;
  unsigned long min_byte;
  unsigned long max_byte_end;
  unsigned long byte_end;
  size_t index;
  size_t field_index;
  int found;

  if (first_line == NULL || line_count == NULL || first_byte == NULL ||
      byte_count == NULL) {
    return 0;
  }
  if (term_count == 0U) {
    if (!lc_pouch_index_term_fields_find(fields, field_count, field_hex,
                                         first_line, line_count)) {
      return 0;
    }
    for (index = 0U; index < field_count; ++index) {
      if (strcmp(fields[index].field_hex, field_hex) == 0) {
        *first_byte = fields[index].first_byte;
        *byte_count = fields[index].byte_count;
        return 1;
      }
    }
    return 0;
  }
  if (terms == NULL) {
    return 0;
  }
  found = 0;
  min_first = term_line_count;
  max_end = 0UL;
  min_byte = term_byte_count;
  max_byte_end = 0UL;
  for (index = 0U; index < term_count; ++index) {
    if (lc_pouch_index_term_fields_find(
            fields, field_count, terms[index].field_hex, &first, &count)) {
      end = first + count;
      if (!found || first < min_first) {
        min_first = first;
      }
      if (end > max_end) {
        max_end = end;
      }
      for (field_index = 0U; field_index < field_count; ++field_index) {
        if (strcmp(fields[field_index].field_hex, terms[index].field_hex) ==
            0) {
          byte_end =
              fields[field_index].first_byte + fields[field_index].byte_count;
          if (!found || fields[field_index].first_byte < min_byte) {
            min_byte = fields[field_index].first_byte;
          }
          if (byte_end > max_byte_end) {
            max_byte_end = byte_end;
          }
          break;
        }
      }
      found = 1;
    }
  }
  if (!found) {
    return 0;
  }
  *first_line = min_first;
  *line_count = max_end - min_first;
  *first_byte = min_byte;
  *byte_count = max_byte_end - min_byte;
  return 1;
}

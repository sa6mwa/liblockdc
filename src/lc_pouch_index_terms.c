#include "lc_pouch_index.h"

#include "lc_api_internal.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

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

static char *lc_pouch_index_term_hex_encode_bytes(
    const lc_allocator *allocator, const char *value, size_t length) {
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

void lc_pouch_index_term_fields_cleanup(
    const lc_allocator *allocator, lc_pouch_index_term_field *fields,
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

void lc_pouch_index_term_values_cleanup(
    const lc_allocator *allocator, lc_pouch_index_term_value *values,
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

void lc_pouch_index_term_keys_cleanup(
    const lc_allocator *allocator, lc_pouch_index_term_key *terms,
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
    const lc_pouch_index_term_key *left,
    const lc_pouch_index_term_key *right) {
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

int lc_pouch_index_term_key_compare_pair(
    const char *field_hex, const char *value_hex,
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
    cmp = lc_pouch_index_term_key_compare_pair(field_hex, value_hex,
                                               &terms[mid]);
    if (cmp == 0) {
      scan = mid;
      while (scan > 0U &&
             lc_pouch_index_term_key_compare_pair(field_hex, value_hex,
                                                  &terms[scan - 1U]) == 0) {
        --scan;
      }
      for (; scan < count &&
             lc_pouch_index_term_key_compare_pair(field_hex, value_hex,
                                                  &terms[scan]) == 0;
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

int lc_pouch_index_term_keys_build_exact(
    const lc_pouch_index_plain_term *terms, size_t term_count,
    lc_pouch_index_term_key **out_terms, size_t *out_count,
    const lc_allocator *allocator, lc_error *error) {
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
                        "failed to allocate pouch index exact term keys",
                        NULL, NULL, NULL);
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
    keys[index].value_hex =
        lc_pouch_index_term_hex_encode(allocator, terms[index].value);
    if (keys[index].field_hex == NULL || keys[index].value_hex == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index exact term key", NULL,
                        NULL, NULL);
      break;
    }
    keys[index].value_type = terms[index].value_type;
  }
  if (rc == LC_OK) {
    qsort(keys, term_count, sizeof(keys[0]),
          lc_pouch_index_term_key_compare);
    write_index = 0U;
    for (index = 0U; index < term_count; ++index) {
      if (write_index > 0U &&
          lc_pouch_index_term_key_compare_items(
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
    size_t value_count, lc_pouch_index_term_key **out_terms,
    size_t *out_count, const lc_allocator *allocator, lc_error *error) {
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
  rc = lc_pouch_index_term_keys_build_exact(
      terms, value_count, out_terms, out_count, allocator, error);
  lc_free_with_allocator(allocator, terms);
  return rc;
}

int lc_pouch_index_term_field_parse_line(
    char *line, lc_pouch_index_term_field *field,
    const lc_allocator *allocator, lc_error *error) {
  char *cursor;
  char *field_hex;
  char *first_token;
  char *count_token;
  unsigned long first_line;
  unsigned long line_count;
  int rc;

  if (line == NULL || field == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term field requires line and output",
                        NULL, NULL, NULL);
  }
  if (strncmp(line, "term_field ", sizeof("term_field ") - 1U) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term field has invalid prefix", NULL,
                        NULL, NULL);
  }
  cursor = line + sizeof("term_field ") - 1U;
  field_hex = lc_pouch_index_term_next_token(&cursor, 0);
  first_token = lc_pouch_index_term_next_token(&cursor, 0);
  count_token = lc_pouch_index_term_next_token(&cursor, 1);
  if (field_hex == NULL || strcmp(field_hex, "-") == 0 ||
      !lc_pouch_index_term_hex_token_valid(field_hex) ||
      first_token == NULL || count_token == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term field has invalid fields", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_index_term_parse_ulong_token(
      first_token, &first_line,
      "pouch index term field has invalid first line", error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_index_term_parse_ulong_token(
      count_token, &line_count,
      "pouch index term field has invalid line count", error);
  if (rc != LC_OK) {
    return rc;
  }
  field->field_hex = lc_strdup_with_allocator(allocator, field_hex);
  if (field->field_hex == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index term field", NULL,
                        NULL, NULL);
  }
  field->first_line = first_line;
  field->line_count = line_count;
  return LC_OK;
}

int lc_pouch_index_term_value_parse_line(
    char *line, lc_pouch_index_term_value *value,
    const lc_allocator *allocator, lc_error *error) {
  char *cursor;
  char *field_hex;
  char *value_hex;
  char *first_token;
  char *count_token;
  unsigned long first_line;
  unsigned long line_count;
  int rc;

  if (line == NULL || value == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term value requires line and output",
                        NULL, NULL, NULL);
  }
  if (strncmp(line, "term_value ", sizeof("term_value ") - 1U) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term value has invalid prefix", NULL,
                        NULL, NULL);
  }
  cursor = line + sizeof("term_value ") - 1U;
  field_hex = lc_pouch_index_term_next_token(&cursor, 0);
  value_hex = lc_pouch_index_term_next_token(&cursor, 0);
  first_token = lc_pouch_index_term_next_token(&cursor, 0);
  count_token = lc_pouch_index_term_next_token(&cursor, 1);
  if (field_hex == NULL || value_hex == NULL || strcmp(field_hex, "-") == 0 ||
      !lc_pouch_index_term_hex_token_valid(field_hex) ||
      !lc_pouch_index_term_hex_token_valid(value_hex) ||
      first_token == NULL || count_token == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term value has invalid fields", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_index_term_parse_ulong_token(
      first_token, &first_line,
      "pouch index term value has invalid first line", error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_index_term_parse_ulong_token(
      count_token, &line_count,
      "pouch index term value has invalid line count", error);
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
                        "failed to allocate pouch index term value", NULL,
                        NULL, NULL);
  }
  value->first_line = first_line;
  value->line_count = line_count;
  return LC_OK;
}

void lc_pouch_index_term_ranges_cleanup(
    const lc_allocator *allocator, lc_pouch_index_term_range *ranges) {
  lc_free_with_allocator(allocator, ranges);
}

int lc_pouch_index_term_values_collect_ranges(
    const lc_pouch_index_term_value *values, size_t value_count,
    const lc_pouch_index_term_key *terms, size_t term_count,
    lc_pouch_index_term_range **out_ranges, size_t *out_count,
    const lc_allocator *allocator, lc_error *error) {
  lc_pouch_index_term_range *ranges;
  unsigned long first;
  unsigned long count;
  size_t index;
  size_t range_count;

  if (out_ranges == NULL || out_count == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index term range collection requires outputs",
                        NULL, NULL, NULL);
  }
  *out_ranges = NULL;
  *out_count = 0U;
  if (terms == NULL || term_count == 0U) {
    return LC_OK;
  }
  ranges = (lc_pouch_index_term_range *)lc_alloc_with_allocator(
      allocator, term_count * sizeof(*ranges));
  if (ranges == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index term ranges", NULL,
                        NULL, NULL);
  }
  range_count = 0U;
  for (index = 0U; index < term_count; ++index) {
    if (index > 0U &&
        lc_pouch_index_term_key_compare_pair(terms[index].field_hex,
                                             terms[index].value_hex,
                                             &terms[index - 1U]) == 0) {
      continue;
    }
    if (lc_pouch_index_term_values_find(values, value_count,
                                        terms[index].field_hex,
                                        terms[index].value_hex, &first,
                                        &count)) {
      ranges[range_count].first_line = first;
      ranges[range_count].line_count = count;
      ++range_count;
    }
  }
  if (range_count == 0U) {
    lc_free_with_allocator(allocator, ranges);
    return LC_OK;
  }
  *out_ranges = ranges;
  *out_count = range_count;
  return LC_OK;
}

int lc_pouch_index_term_fields_select_range(
    const lc_pouch_index_term_field *fields, size_t field_count,
    const char *field_hex, const lc_pouch_index_term_key *terms,
    size_t term_count, unsigned long term_line_count,
    unsigned long *first_line, unsigned long *line_count) {
  unsigned long first;
  unsigned long count;
  unsigned long end;
  unsigned long min_first;
  unsigned long max_end;
  size_t index;
  int found;

  if (first_line == NULL || line_count == NULL) {
    return 0;
  }
  if (term_count == 0U) {
    return lc_pouch_index_term_fields_find(fields, field_count, field_hex,
                                           first_line, line_count);
  }
  if (terms == NULL) {
    return 0;
  }
  found = 0;
  min_first = term_line_count;
  max_end = 0UL;
  for (index = 0U; index < term_count; ++index) {
    if (lc_pouch_index_term_fields_find(fields, field_count,
                                        terms[index].field_hex, &first,
                                        &count)) {
      end = first + count;
      if (!found || first < min_first) {
        min_first = first;
      }
      if (end > max_end) {
        max_end = end;
      }
      found = 1;
    }
  }
  if (!found) {
    return 0;
  }
  *first_line = min_first;
  *line_count = max_end - min_first;
  return 1;
}

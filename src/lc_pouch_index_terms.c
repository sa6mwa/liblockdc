#include "lc_pouch_index.h"

#include "lc_api_internal.h"

#include <string.h>

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

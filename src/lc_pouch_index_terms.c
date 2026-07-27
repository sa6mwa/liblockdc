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

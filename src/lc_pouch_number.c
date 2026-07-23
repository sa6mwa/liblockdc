#include "lc_pouch_number.h"

#include "lc_api_internal.h"

#include <limits.h>
#include <stdio.h>
#include <string.h>

static int lc_pouch_number_is_digit(char ch) {
  return ch >= '0' && ch <= '9';
}

static int lc_pouch_number_parse_exp(const char *data, size_t len,
                                     size_t *pos, long *out) {
  int negative;
  long value;

  negative = 0;
  value = 0L;
  if (*pos >= len) {
    return 0;
  }
  if (data[*pos] == '-' || data[*pos] == '+') {
    negative = data[*pos] == '-';
    ++*pos;
  }
  if (*pos >= len || !lc_pouch_number_is_digit(data[*pos])) {
    return 0;
  }
  while (*pos < len && lc_pouch_number_is_digit(data[*pos])) {
    int digit;

    digit = data[*pos] - '0';
    if (value > (LONG_MAX - digit) / 10L) {
      return 0;
    }
    value = value * 10L + digit;
    ++*pos;
  }
  *out = negative ? -value : value;
  return 1;
}

static int lc_pouch_number_add_long(long left, long right, long *out) {
  if ((right > 0L && left > LONG_MAX - right) ||
      (right < 0L && left < LONG_MIN - right)) {
    return 0;
  }
  *out = left + right;
  return 1;
}

int lc_pouch_number_eq_key(const char *data, size_t len, char **out) {
  char *digits;
  char *key;
  size_t pos;
  size_t digit_count;
  size_t frac_len;
  size_t first;
  size_t last;
  size_t canonical_len;
  long exp_value;
  long scale;
  int negative;
  int any_nonzero;
  int written;

  if (out != NULL) {
    *out = NULL;
  }
  if (data == NULL || len == 0U || out == NULL) {
    return 0;
  }
  digits = (char *)lc_alloc_with_allocator(NULL, len + 1U);
  if (digits == NULL) {
    return 0;
  }
  pos = 0U;
  digit_count = 0U;
  frac_len = 0U;
  exp_value = 0L;
  negative = 0;
  any_nonzero = 0;
  if (data[pos] == '-') {
    negative = 1;
    ++pos;
    if (pos >= len) {
      lc_free_with_allocator(NULL, digits);
      return 0;
    }
  }
  if (data[pos] == '0') {
    digits[digit_count++] = data[pos++];
  } else if (data[pos] >= '1' && data[pos] <= '9') {
    while (pos < len && lc_pouch_number_is_digit(data[pos])) {
      if (data[pos] != '0') {
        any_nonzero = 1;
      }
      digits[digit_count++] = data[pos++];
    }
  } else {
    lc_free_with_allocator(NULL, digits);
    return 0;
  }
  if (pos < len && data[pos] == '.') {
    ++pos;
    if (pos >= len || !lc_pouch_number_is_digit(data[pos])) {
      lc_free_with_allocator(NULL, digits);
      return 0;
    }
    while (pos < len && lc_pouch_number_is_digit(data[pos])) {
      if (data[pos] != '0') {
        any_nonzero = 1;
      }
      digits[digit_count++] = data[pos++];
      ++frac_len;
    }
  }
  if (pos < len && (data[pos] == 'e' || data[pos] == 'E')) {
    ++pos;
    if (!lc_pouch_number_parse_exp(data, len, &pos, &exp_value)) {
      lc_free_with_allocator(NULL, digits);
      return 0;
    }
  }
  if (pos != len || digit_count == 0U) {
    lc_free_with_allocator(NULL, digits);
    return 0;
  }
  if (!any_nonzero) {
    key = lc_strdup_with_allocator(NULL, "n:0");
    lc_free_with_allocator(NULL, digits);
    if (key == NULL) {
      return 0;
    }
    *out = key;
    return 1;
  }
  first = 0U;
  while (first < digit_count && digits[first] == '0') {
    ++first;
  }
  last = digit_count;
  scale = exp_value;
  if (frac_len > (size_t)LONG_MAX ||
      !lc_pouch_number_add_long(scale, -(long)frac_len, &scale)) {
    lc_free_with_allocator(NULL, digits);
    return 0;
  }
  while (last > first && digits[last - 1U] == '0') {
    --last;
    if (!lc_pouch_number_add_long(scale, 1L, &scale)) {
      lc_free_with_allocator(NULL, digits);
      return 0;
    }
  }
  canonical_len = last - first;
  if (canonical_len > (size_t)INT_MAX) {
    lc_free_with_allocator(NULL, digits);
    return 0;
  }
  if (canonical_len > ((size_t)-1) - 64U) {
    lc_free_with_allocator(NULL, digits);
    return 0;
  }
  key = (char *)lc_alloc_with_allocator(NULL, canonical_len + 64U);
  if (key == NULL) {
    lc_free_with_allocator(NULL, digits);
    return 0;
  }
  written = snprintf(key, canonical_len + 64U, "n:%c:%.*s:%ld",
                     negative ? '-' : '+', (int)canonical_len, digits + first,
                     scale);
  lc_free_with_allocator(NULL, digits);
  if (written < 0 || (size_t)written >= canonical_len + 64U) {
    lc_free_with_allocator(NULL, key);
    return 0;
  }
  *out = key;
  return 1;
}

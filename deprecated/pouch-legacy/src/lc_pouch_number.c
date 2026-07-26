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

static int lc_pouch_number_parse_key(const char *key, int *negative,
                                     const char **digits, size_t *digit_count,
                                     long *scale) {
  const char *scale_text;
  char *end;
  size_t index;

  if (key == NULL || negative == NULL || digits == NULL ||
      digit_count == NULL || scale == NULL) {
    return 0;
  }
  if (strcmp(key, "n:0") == 0) {
    *negative = 0;
    *digits = "";
    *digit_count = 0U;
    *scale = 0L;
    return 1;
  }
  if (strncmp(key, "n:+:", 4U) == 0) {
    *negative = 0;
  } else if (strncmp(key, "n:-:", 4U) == 0) {
    *negative = 1;
  } else {
    return 0;
  }
  *digits = key + 4U;
  scale_text = strchr(*digits, ':');
  if (scale_text == NULL || scale_text == *digits) {
    return 0;
  }
  *digit_count = (size_t)(scale_text - *digits);
  for (index = 0U; index < *digit_count; ++index) {
    if (!lc_pouch_number_is_digit((*digits)[index])) {
      return 0;
    }
  }
  ++scale_text;
  if (*scale_text == '\0') {
    return 0;
  }
  *scale = strtol(scale_text, &end, 10);
  return end != scale_text && *end == '\0';
}

static int lc_pouch_number_compare_digit_count(long left_scale,
                                               size_t left_digits,
                                               long right_scale,
                                               size_t right_digits,
                                               int *out) {
  long left;
  long right;

  if (left_digits > (size_t)LONG_MAX || right_digits > (size_t)LONG_MAX) {
    return 0;
  }
  if (!lc_pouch_number_add_long(left_scale, (long)left_digits, &left) ||
      !lc_pouch_number_add_long(right_scale, (long)right_digits, &right)) {
    return 0;
  }
  if (left < right) {
    *out = -1;
  } else if (left > right) {
    *out = 1;
  } else {
    *out = 0;
  }
  return 1;
}

static int lc_pouch_number_compare_positive(const char *left_digits,
                                            size_t left_digit_count,
                                            long left_scale,
                                            const char *right_digits,
                                            size_t right_digit_count,
                                            long right_scale, int *out) {
  int magnitude_cmp;
  size_t compare_len;
  size_t index;

  if (!lc_pouch_number_compare_digit_count(left_scale, left_digit_count,
                                           right_scale, right_digit_count,
                                           &magnitude_cmp)) {
    return 0;
  }
  if (magnitude_cmp < 0) {
    *out = -1;
    return 1;
  }
  if (magnitude_cmp > 0) {
    *out = 1;
    return 1;
  }
  compare_len =
      left_digit_count > right_digit_count ? left_digit_count : right_digit_count;
  for (index = 0U; index < compare_len; ++index) {
    char left;
    char right;

    left = index < left_digit_count ? left_digits[index] : '0';
    right = index < right_digit_count ? right_digits[index] : '0';
    if (left < right) {
      *out = -1;
      return 1;
    }
    if (left > right) {
      *out = 1;
      return 1;
    }
  }
  *out = 0;
  return 1;
}

int lc_pouch_number_eq_key_compare(const char *left, const char *right,
                                   int *out) {
  const char *left_digits;
  const char *right_digits;
  size_t left_digit_count;
  size_t right_digit_count;
  long left_scale;
  long right_scale;
  int left_negative;
  int right_negative;
  int cmp;

  if (out != NULL) {
    *out = 0;
  }
  if (out == NULL ||
      !lc_pouch_number_parse_key(left, &left_negative, &left_digits,
                                 &left_digit_count, &left_scale) ||
      !lc_pouch_number_parse_key(right, &right_negative, &right_digits,
                                 &right_digit_count, &right_scale)) {
    return 0;
  }
  if (left_digit_count == 0U && right_digit_count == 0U) {
    *out = 0;
    return 1;
  }
  if (left_digit_count == 0U) {
    *out = right_negative ? 1 : -1;
    return 1;
  }
  if (right_digit_count == 0U) {
    *out = left_negative ? -1 : 1;
    return 1;
  }
  if (left_negative != right_negative) {
    *out = left_negative ? -1 : 1;
    return 1;
  }
  if (!lc_pouch_number_compare_positive(
          left_digits, left_digit_count, left_scale, right_digits,
          right_digit_count, right_scale, &cmp)) {
    return 0;
  }
  *out = left_negative ? -cmp : cmp;
  return 1;
}

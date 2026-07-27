#include "lc_pouch_index.h"

#include "lc_api_internal.h"

#include <string.h>

static int lc_pouch_index_parse_2digits(const char *text, int *out) {
  if (text == NULL || out == NULL || text[0] < '0' || text[0] > '9' ||
      text[1] < '0' || text[1] > '9') {
    return 0;
  }
  *out = ((text[0] - '0') * 10) + (text[1] - '0');
  return 1;
}

static int lc_pouch_index_parse_4digits(const char *text, int *out) {
  int value;
  size_t index;

  if (text == NULL || out == NULL) {
    return 0;
  }
  value = 0;
  for (index = 0U; index < 4U; ++index) {
    if (text[index] < '0' || text[index] > '9') {
      return 0;
    }
    value = (value * 10) + (text[index] - '0');
  }
  *out = value;
  return 1;
}

static int lc_pouch_index_date_is_leap(int year) {
  return (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
}

static int lc_pouch_index_date_month_days(int year, int month) {
  static const int days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

  if (month < 1 || month > 12) {
    return 0;
  }
  if (month == 2 && lc_pouch_index_date_is_leap(year)) {
    return 29;
  }
  return days[month - 1];
}

static double lc_pouch_index_days_from_civil(int year, int month, int day) {
  long y;
  long era;
  long yoe;
  long doy;
  long doe;

  y = (long)year;
  y -= month <= 2 ? 1L : 0L;
  era = y >= 0L ? y / 400L : (y - 399L) / 400L;
  yoe = y - era * 400L;
  doy =
      (153L * (long)(month + (month > 2 ? -3 : 9)) + 2L) / 5L + (long)day - 1L;
  doe = yoe * 365L + yoe / 4L - yoe / 100L + doy;
  return (double)(era * 146097L + doe - 719468L);
}

int lc_pouch_index_parse_lql_datetime(const char *text,
                                      lc_pouch_index_instant *out) {
  int year;
  int month;
  int day;
  int hour;
  int minute;
  int second;
  int offset_sign;
  int offset_hour;
  int offset_minute;
  double fraction;
  size_t length;
  size_t index;
  double days;
  double seconds;
  double offset_seconds;

  if (text == NULL || out == NULL) {
    return 0;
  }
  length = strlen(text);
  if (length < 10U) {
    return 0;
  }
  if (!lc_pouch_index_parse_4digits(text, &year) || text[4] != '-' ||
      !lc_pouch_index_parse_2digits(text + 5, &month) || text[7] != '-' ||
      !lc_pouch_index_parse_2digits(text + 8, &day)) {
    return 0;
  }
  hour = 0;
  minute = 0;
  second = 0;
  if (month < 1 || month > 12 || day < 1 ||
      day > lc_pouch_index_date_month_days(year, month)) {
    return 0;
  }
  index = 10U;
  fraction = 0.0;
  if (text[index] == '\0') {
    days = lc_pouch_index_days_from_civil(year, month, day);
    out->seconds = days * 86400.0;
    return 1;
  }
  if (text[index] != 'T' && text[index] != 't') {
    return 0;
  }
  if (length < 19U) {
    return 0;
  }
  ++index;
  if (!lc_pouch_index_parse_2digits(text + index, &hour) ||
      text[index + 2U] != ':' ||
      !lc_pouch_index_parse_2digits(text + index + 3U, &minute) ||
      text[index + 5U] != ':' ||
      !lc_pouch_index_parse_2digits(text + index + 6U, &second)) {
    return 0;
  }
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 ||
      second > 60) {
    return 0;
  }
  index += 8U;
  if (text[index] == '.') {
    double scale;
    size_t digits;

    ++index;
    scale = 0.1;
    digits = 0U;
    while (text[index] >= '0' && text[index] <= '9') {
      if (digits < 9U) {
        fraction += (double)(text[index] - '0') * scale;
        scale /= 10.0;
      }
      ++digits;
      ++index;
    }
    if (digits == 0U) {
      return 0;
    }
  }
  offset_sign = 0;
  offset_hour = 0;
  offset_minute = 0;
  if (text[index] == 'Z' || text[index] == 'z') {
    ++index;
  } else if (text[index] == '+' || text[index] == '-') {
    offset_sign = text[index] == '+' ? 1 : -1;
    ++index;
    if (!lc_pouch_index_parse_2digits(text + index, &offset_hour) ||
        text[index + 2U] != ':' ||
        !lc_pouch_index_parse_2digits(text + index + 3U, &offset_minute)) {
      return 0;
    }
    if (offset_hour > 23 || offset_minute > 59) {
      return 0;
    }
    index += 5U;
  } else if (text[index] != '\0') {
    return 0;
  }
  if (text[index] != '\0') {
    return 0;
  }
  days = lc_pouch_index_days_from_civil(year, month, day);
  seconds = days * 86400.0 + (double)hour * 3600.0 + (double)minute * 60.0 +
            (double)second + fraction;
  offset_seconds = (double)offset_sign * ((double)offset_hour * 3600.0 +
                                          (double)offset_minute * 60.0);
  seconds -= offset_seconds;
  out->seconds = seconds;
  return 1;
}

static int lc_pouch_index_instant_compare(const lc_pouch_index_instant *left,
                                          const lc_pouch_index_instant *right) {
  if (left->seconds < right->seconds) {
    return -1;
  }
  if (left->seconds > right->seconds) {
    return 1;
  }
  return 0;
}

int lc_pouch_index_date_contains_value(
    const lc_pouch_index_parsed_date_bounds *bounds,
    const lc_pouch_index_instant *value) {
  if (bounds == NULL || value == NULL ||
      (!bounds->has_gt && !bounds->has_gte && !bounds->has_lt &&
       !bounds->has_lte)) {
    return 0;
  }
  if (bounds->has_gt &&
      lc_pouch_index_instant_compare(value, &bounds->gt) <= 0) {
    return 0;
  }
  if (bounds->has_gte &&
      lc_pouch_index_instant_compare(value, &bounds->gte) < 0) {
    return 0;
  }
  if (bounds->has_lt &&
      lc_pouch_index_instant_compare(value, &bounds->lt) >= 0) {
    return 0;
  }
  if (bounds->has_lte &&
      lc_pouch_index_instant_compare(value, &bounds->lte) > 0) {
    return 0;
  }
  return 1;
}

int lc_pouch_index_parse_date_bounds(const lc_pouch_index_date_bounds *bounds,
                                     lc_pouch_index_parsed_date_bounds *out,
                                     lc_error *error) {
  if (bounds == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index date lookup requires date bounds", NULL,
                        NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  if (bounds->has_gt) {
    if (!lc_pouch_index_parse_lql_datetime(bounds->gt, &out->gt)) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index date gt bound is invalid", NULL, NULL,
                          "pouch");
    }
    out->has_gt = 1;
  }
  if (bounds->has_gte) {
    if (!lc_pouch_index_parse_lql_datetime(bounds->gte, &out->gte)) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index date gte bound is invalid", NULL, NULL,
                          "pouch");
    }
    out->has_gte = 1;
  }
  if (bounds->has_lt) {
    if (!lc_pouch_index_parse_lql_datetime(bounds->lt, &out->lt)) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index date lt bound is invalid", NULL, NULL,
                          "pouch");
    }
    out->has_lt = 1;
  }
  if (bounds->has_lte) {
    if (!lc_pouch_index_parse_lql_datetime(bounds->lte, &out->lte)) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index date lte bound is invalid", NULL, NULL,
                          "pouch");
    }
    out->has_lte = 1;
  }
  if (!out->has_gt && !out->has_gte && !out->has_lt && !out->has_lte) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index date lookup requires bounded selector",
                        NULL, NULL, "pouch");
  }
  return LC_OK;
}

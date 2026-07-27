#include "lc_pouch_temporal.h"

#include <stddef.h>
#include <string.h>

static int lc_pouch_temporal_is_digit(char ch) {
  return ch >= '0' && ch <= '9';
}

static int lc_pouch_temporal_decimal2(const char *text, size_t offset) {
  return ((int)(text[offset] - '0') * 10) + (int)(text[offset + 1U] - '0');
}

static int lc_pouch_temporal_decimal4(const char *text) {
  return ((int)(text[0] - '0') * 1000) + ((int)(text[1] - '0') * 100) +
         ((int)(text[2] - '0') * 10) + (int)(text[3] - '0');
}

static int lc_pouch_temporal_leap_year(int year) {
  return (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
}

static int lc_pouch_temporal_month_days(int year, int month) {
  static const int days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

  if (month < 1 || month > 12) {
    return 0;
  }
  if (month == 2 && lc_pouch_temporal_leap_year(year)) {
    return 29;
  }
  return days[month - 1];
}

static int64_t lc_pouch_temporal_days_from_civil(int year, int month, int day) {
  int y;
  int era;
  unsigned yoe;
  unsigned doy;
  unsigned doe;

  y = year - (month <= 2 ? 1 : 0);
  era = (y >= 0 ? y : y - 399) / 400;
  yoe = (unsigned)(y - era * 400);
  doy = (unsigned)((153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1);
  doe = yoe * 365U + yoe / 4U - yoe / 100U + doy;
  return (int64_t)era * INT64_C(146097) + (int64_t)doe - INT64_C(719468);
}

static int lc_pouch_temporal_valid_date_time(int year, int month, int day,
                                             int hour, int minute, int second) {
  return month >= 1 && month <= 12 && day >= 1 &&
         day <= lc_pouch_temporal_month_days(year, month) && hour >= 0 &&
         hour <= 23 && minute >= 0 && minute <= 59 && second >= 0 &&
         second <= 59;
}

static int lc_pouch_temporal_view_parse(const char *text, size_t len,
                                        lc_pouch_temporal *out) {
  int year;
  int month;
  int day;
  int hour;
  int minute;
  int second;
  int offset_sign;
  int offset_seconds;
  int64_t unix_seconds;
  int32_t nanosecond;
  size_t pos;
  size_t fraction_digits;

  if (text == NULL || len < 10U || text[4] != '-' || text[7] != '-' ||
      !lc_pouch_temporal_is_digit(text[0]) ||
      !lc_pouch_temporal_is_digit(text[1]) ||
      !lc_pouch_temporal_is_digit(text[2]) ||
      !lc_pouch_temporal_is_digit(text[3]) ||
      !lc_pouch_temporal_is_digit(text[5]) ||
      !lc_pouch_temporal_is_digit(text[6]) ||
      !lc_pouch_temporal_is_digit(text[8]) ||
      !lc_pouch_temporal_is_digit(text[9])) {
    return 0;
  }
  year = lc_pouch_temporal_decimal4(text);
  month = lc_pouch_temporal_decimal2(text, 5U);
  day = lc_pouch_temporal_decimal2(text, 8U);
  if (len == 10U) {
    if (!lc_pouch_temporal_valid_date_time(year, month, day, 0, 0, 0)) {
      return 0;
    }
    if (out != NULL) {
      out->unix_seconds =
          lc_pouch_temporal_days_from_civil(year, month, day) * INT64_C(86400);
      out->nanosecond = 0;
    }
    return 1;
  }
  if (len < 19U || text[10] != 'T' || text[13] != ':' || text[16] != ':' ||
      !lc_pouch_temporal_is_digit(text[11]) ||
      !lc_pouch_temporal_is_digit(text[12]) ||
      !lc_pouch_temporal_is_digit(text[14]) ||
      !lc_pouch_temporal_is_digit(text[15]) ||
      !lc_pouch_temporal_is_digit(text[17]) ||
      !lc_pouch_temporal_is_digit(text[18])) {
    return 0;
  }
  hour = lc_pouch_temporal_decimal2(text, 11U);
  minute = lc_pouch_temporal_decimal2(text, 14U);
  second = lc_pouch_temporal_decimal2(text, 17U);
  if (!lc_pouch_temporal_valid_date_time(year, month, day, hour, minute,
                                         second)) {
    return 0;
  }
  pos = 19U;
  nanosecond = 0;
  if (pos < len && text[pos] == '.') {
    ++pos;
    if (pos >= len || !lc_pouch_temporal_is_digit(text[pos])) {
      return 0;
    }
    fraction_digits = 0U;
    while (pos < len && lc_pouch_temporal_is_digit(text[pos])) {
      if (fraction_digits >= 9U) {
        return 0;
      }
      nanosecond = (int32_t)(nanosecond * 10 + (text[pos] - '0'));
      ++fraction_digits;
      ++pos;
    }
    while (fraction_digits < 9U) {
      nanosecond = (int32_t)(nanosecond * 10);
      ++fraction_digits;
    }
  }
  offset_seconds = 0;
  if (pos == len) {
    offset_seconds = 0;
  } else if (text[pos] == 'Z' && pos + 1U == len) {
    offset_seconds = 0;
  } else if ((text[pos] == '+' || text[pos] == '-') && pos + 6U == len &&
             lc_pouch_temporal_is_digit(text[pos + 1U]) &&
             lc_pouch_temporal_is_digit(text[pos + 2U]) &&
             text[pos + 3U] == ':' &&
             lc_pouch_temporal_is_digit(text[pos + 4U]) &&
             lc_pouch_temporal_is_digit(text[pos + 5U])) {
    int offset_hour;
    int offset_minute;

    offset_sign = text[pos] == '+' ? 1 : -1;
    offset_hour = lc_pouch_temporal_decimal2(text, pos + 1U);
    offset_minute = lc_pouch_temporal_decimal2(text, pos + 4U);
    if (offset_hour < 0 || offset_hour > 23 || offset_minute < 0 ||
        offset_minute > 59) {
      return 0;
    }
    offset_seconds = offset_sign * (offset_hour * 3600 + offset_minute * 60);
  } else {
    return 0;
  }
  if (out != NULL) {
    unix_seconds =
        lc_pouch_temporal_days_from_civil(year, month, day) * INT64_C(86400) +
        (int64_t)hour * INT64_C(3600) + (int64_t)minute * INT64_C(60) +
        (int64_t)second - (int64_t)offset_seconds;
    out->unix_seconds = unix_seconds;
    out->nanosecond = nanosecond;
  }
  return 1;
}

static const char *lc_pouch_temporal_trim_begin(const char *text) {
  while (*text == ' ' || *text == '\t' || *text == '\n' || *text == '\r') {
    ++text;
  }
  return text;
}

static const char *lc_pouch_temporal_trim_end(const char *begin) {
  const char *end;

  end = begin + strlen(begin);
  while (end > begin && (end[-1] == ' ' || end[-1] == '\t' || end[-1] == '\n' ||
                         end[-1] == '\r')) {
    --end;
  }
  return end;
}

int lc_pouch_temporal_parse(const char *text, lc_pouch_temporal *out) {
  const char *begin;
  const char *end;

  if (text == NULL) {
    return 0;
  }
  begin = lc_pouch_temporal_trim_begin(text);
  end = lc_pouch_temporal_trim_end(begin);
  return lc_pouch_temporal_view_parse(begin, (size_t)(end - begin), out);
}

static int lc_pouch_temporal_has_date_prefix(const char *text, size_t len) {
  return len >= 10U && text[4] == '-' && text[7] == '-' &&
         lc_pouch_temporal_is_digit(text[0]) &&
         lc_pouch_temporal_is_digit(text[1]) &&
         lc_pouch_temporal_is_digit(text[2]) &&
         lc_pouch_temporal_is_digit(text[3]) &&
         lc_pouch_temporal_is_digit(text[5]) &&
         lc_pouch_temporal_is_digit(text[6]) &&
         lc_pouch_temporal_is_digit(text[8]) &&
         lc_pouch_temporal_is_digit(text[9]);
}

static int lc_pouch_temporal_has_time_prefix(const char *text, size_t len) {
  return len >= 19U &&
         (text[10] == 'T' || text[10] == 't' || text[10] == ' ') &&
         text[13] == ':' && text[16] == ':' &&
         lc_pouch_temporal_is_digit(text[11]) &&
         lc_pouch_temporal_is_digit(text[12]) &&
         lc_pouch_temporal_is_digit(text[14]) &&
         lc_pouch_temporal_is_digit(text[15]) &&
         lc_pouch_temporal_is_digit(text[17]) &&
         lc_pouch_temporal_is_digit(text[18]);
}

static int lc_pouch_temporal_has_offset_suffix(const char *text, size_t pos,
                                               size_t len) {
  if (pos == len) {
    return 1;
  }
  if ((text[pos] == 'Z' || text[pos] == 'z') && pos + 1U == len) {
    return 1;
  }
  return (text[pos] == '+' || text[pos] == '-') && pos + 6U == len &&
         lc_pouch_temporal_is_digit(text[pos + 1U]) &&
         lc_pouch_temporal_is_digit(text[pos + 2U]) && text[pos + 3U] == ':' &&
         lc_pouch_temporal_is_digit(text[pos + 4U]) &&
         lc_pouch_temporal_is_digit(text[pos + 5U]);
}

int lc_pouch_temporal_may_match_liblql(const char *text) {
  const char *begin;
  const char *end;
  size_t len;
  size_t pos;

  if (text == NULL) {
    return 0;
  }
  begin = lc_pouch_temporal_trim_begin(text);
  end = lc_pouch_temporal_trim_end(begin);
  len = (size_t)(end - begin);
  if (lc_pouch_temporal_view_parse(begin, len, NULL)) {
    return 1;
  }
  if (!lc_pouch_temporal_has_date_prefix(begin, len)) {
    return 0;
  }
  if (len == 10U) {
    return 1;
  }
  if (!lc_pouch_temporal_has_time_prefix(begin, len)) {
    return 0;
  }
  pos = 19U;
  if (pos < len && (begin[pos] == '.' || begin[pos] == ',')) {
    ++pos;
    if (pos >= len || !lc_pouch_temporal_is_digit(begin[pos])) {
      return 0;
    }
    while (pos < len && lc_pouch_temporal_is_digit(begin[pos])) {
      ++pos;
    }
  }
  return lc_pouch_temporal_has_offset_suffix(begin, pos, len);
}

int lc_pouch_temporal_compare(const lc_pouch_temporal *left,
                              const lc_pouch_temporal *right) {
  if (left->unix_seconds < right->unix_seconds) {
    return -1;
  }
  if (left->unix_seconds > right->unix_seconds) {
    return 1;
  }
  if (left->nanosecond < right->nanosecond) {
    return -1;
  }
  if (left->nanosecond > right->nanosecond) {
    return 1;
  }
  return 0;
}

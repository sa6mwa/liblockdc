#include "lc_pouch_index.h"

#include "lc_api_internal.h"

#include <limits.h>
#include <string.h>

void lc_pouch_index_docid_set_cleanup(const lc_allocator *allocator,
                                      lc_pouch_index_docid_set *set) {
  if (set == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, set->items);
  memset(set, 0, sizeof(*set));
}

static int lc_pouch_index_docid_set_reserve(
    lc_pouch_index_docid_set *set, size_t needed,
    const lc_allocator *allocator, lc_error *error) {
  unsigned long *next_items;
  size_t next_capacity;

  if (set == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index docID set reserve requires set", NULL,
                        NULL, NULL);
  }
  if (needed <= set->capacity) {
    return LC_OK;
  }
  next_capacity = set->capacity == 0U ? 16U : set->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch index docID set exceeds local limit", NULL,
                          NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_items = (unsigned long *)lc_alloc_with_allocator(
      allocator, next_capacity * sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index docID set", NULL,
                        NULL, NULL);
  }
  if (set->items != NULL) {
    memcpy(next_items, set->items, set->count * sizeof(*next_items));
    lc_free_with_allocator(allocator, set->items);
  }
  set->items = next_items;
  set->capacity = next_capacity;
  return LC_OK;
}

int lc_pouch_index_docid_set_append_sorted_unique(
    lc_pouch_index_docid_set *set, unsigned long doc_id, int *added,
    const lc_allocator *allocator, lc_error *error) {
  int rc;

  if (set == NULL || added == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index docID set append requires set and added",
                        NULL, NULL, NULL);
  }
  *added = 0;
  if (set->count > 0U) {
    if (doc_id < set->items[set->count - 1U]) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index docID set append requires sorted input",
                          NULL, NULL, "pouch-redesign");
    }
    if (doc_id == set->items[set->count - 1U]) {
      return LC_OK;
    }
  }
  rc = lc_pouch_index_docid_set_reserve(set, set->count + 1U, allocator,
                                        error);
  if (rc != LC_OK) {
    return rc;
  }
  set->items[set->count++] = doc_id;
  *added = 1;
  return LC_OK;
}

int lc_pouch_index_docid_set_append_unique(
    lc_pouch_index_docid_set *set, unsigned long doc_id, int *added,
    const lc_allocator *allocator, lc_error *error) {
  size_t index;
  int rc;

  if (set == NULL || added == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index docID set append requires set and added",
                        NULL, NULL, NULL);
  }
  *added = 0;
  for (index = 0U; index < set->count; ++index) {
    if (set->items[index] == doc_id) {
      return LC_OK;
    }
  }
  rc = lc_pouch_index_docid_set_reserve(set, set->count + 1U, allocator,
                                        error);
  if (rc != LC_OK) {
    return rc;
  }
  set->items[set->count++] = doc_id;
  *added = 1;
  return LC_OK;
}

void lc_pouch_index_posting_cleanup(const lc_allocator *allocator,
                                    lc_pouch_index_posting *posting) {
  if (posting == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, posting->bytes);
  memset(posting, 0, sizeof(*posting));
}

static int lc_pouch_index_posting_reserve(
    lc_pouch_index_posting *posting, size_t needed,
    const lc_allocator *allocator, lc_error *error) {
  unsigned char *next_bytes;
  size_t next_capacity;

  if (posting == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index posting reserve requires posting", NULL,
                        NULL, NULL);
  }
  if (needed <= posting->capacity) {
    return LC_OK;
  }
  next_capacity = posting->capacity == 0U ? 16U : posting->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch index posting exceeds local limit", NULL,
                          NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_bytes = (unsigned char *)lc_alloc_with_allocator(allocator,
                                                        next_capacity);
  if (next_bytes == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index posting", NULL, NULL,
                        NULL);
  }
  if (posting->bytes != NULL) {
    memcpy(next_bytes, posting->bytes, posting->length);
    lc_free_with_allocator(allocator, posting->bytes);
  }
  posting->bytes = next_bytes;
  posting->capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_index_posting_append_varint(
    lc_pouch_index_posting *posting, unsigned long value,
    const lc_allocator *allocator, lc_error *error) {
  unsigned char encoded[sizeof(unsigned long) * 2U];
  size_t encoded_length;
  int rc;

  if (posting == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index posting append requires posting", NULL,
                        NULL, NULL);
  }
  encoded_length = 0U;
  do {
    unsigned char byte;

    byte = (unsigned char)(value & 0x7FUL);
    value >>= 7U;
    if (value != 0UL) {
      byte = (unsigned char)(byte | 0x80U);
    }
    encoded[encoded_length++] = byte;
  } while (value != 0UL && encoded_length < sizeof(encoded));
  if (value != 0UL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index posting varint exceeds local limit", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_index_posting_reserve(
      posting, posting->length + encoded_length, allocator, error);
  if (rc != LC_OK) {
    return rc;
  }
  memcpy(posting->bytes + posting->length, encoded, encoded_length);
  posting->length += encoded_length;
  return LC_OK;
}

int lc_pouch_index_posting_append_sorted_unique(
    lc_pouch_index_posting *posting, unsigned long doc_id, int *added,
    const lc_allocator *allocator, lc_error *error) {
  unsigned long delta;
  int rc;

  if (posting == NULL || added == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index posting append requires posting and "
                        "added",
                        NULL, NULL, NULL);
  }
  *added = 0;
  if (posting->has_last_doc_id) {
    if (doc_id < posting->last_doc_id) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index posting append requires sorted input",
                          NULL, NULL, "pouch-redesign");
    }
    if (doc_id == posting->last_doc_id) {
      return LC_OK;
    }
    delta = doc_id - posting->last_doc_id;
  } else {
    delta = doc_id;
  }
  rc = lc_pouch_index_posting_append_varint(posting, delta, allocator, error);
  if (rc != LC_OK) {
    return rc;
  }
  posting->last_doc_id = doc_id;
  posting->has_last_doc_id = 1;
  ++posting->count;
  *added = 1;
  return LC_OK;
}

static int lc_pouch_index_posting_read_varint(
    const lc_pouch_index_posting *posting, size_t *offset,
    unsigned long *out, lc_error *error) {
  unsigned long value;
  unsigned int shift;

  if (posting == NULL || offset == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index posting varint read requires inputs",
                        NULL, NULL, NULL);
  }
  value = 0UL;
  shift = 0U;
  while (*offset < posting->length) {
    unsigned char byte;

    byte = posting->bytes[*offset];
    ++*offset;
    if (shift >= (unsigned int)(sizeof(unsigned long) * CHAR_BIT)) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index posting varint is too large", NULL,
                          NULL, "pouch-redesign");
    }
    value |= ((unsigned long)(byte & 0x7FU)) << shift;
    if ((byte & 0x80U) == 0U) {
      *out = value;
      return LC_OK;
    }
    shift += 7U;
  }
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index posting varint is truncated", NULL, NULL,
                      "pouch-redesign");
}

int lc_pouch_index_posting_append_to_set(
    const lc_pouch_index_posting *posting, lc_pouch_index_docid_set *set,
    const lc_allocator *allocator, lc_error *error) {
  unsigned long doc_id;
  unsigned long delta;
  size_t offset;
  size_t index;
  int added;
  int rc;

  if (posting == NULL || set == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index posting decode requires posting and set",
                        NULL, NULL, NULL);
  }
  doc_id = 0UL;
  delta = 0UL;
  offset = 0U;
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < posting->count; ++index) {
    rc = lc_pouch_index_posting_read_varint(posting, &offset, &delta, error);
    if (rc != LC_OK) {
      break;
    }
    if (index > 0U && delta > ULONG_MAX - doc_id) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index posting docID delta overflows", NULL,
                        NULL, "pouch-redesign");
      break;
    }
    doc_id = index == 0U ? delta : doc_id + delta;
    rc = lc_pouch_index_docid_set_append_sorted_unique(
        set, doc_id, &added, allocator, error);
  }
  if (rc == LC_OK && offset != posting->length) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index posting has trailing bytes", NULL, NULL,
                      "pouch-redesign");
  }
  return rc;
}

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
  static const int days[] = {31, 28, 31, 30, 31, 30,
                             31, 31, 30, 31, 30, 31};

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
  doy = (153L * (long)(month + (month > 2 ? -3 : 9)) + 2L) / 5L +
        (long)day - 1L;
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
  seconds = days * 86400.0 + (double)hour * 3600.0 +
            (double)minute * 60.0 + (double)second + fraction;
  offset_seconds = (double)offset_sign *
                   ((double)offset_hour * 3600.0 +
                    (double)offset_minute * 60.0);
  seconds -= offset_seconds;
  out->seconds = seconds;
  return 1;
}

static int lc_pouch_index_instant_compare(
    const lc_pouch_index_instant *left,
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

int lc_pouch_index_parse_date_bounds(
    const lc_pouch_index_date_bounds *bounds,
    lc_pouch_index_parsed_date_bounds *out, lc_error *error) {
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
                          "pouch-redesign");
    }
    out->has_gt = 1;
  }
  if (bounds->has_gte) {
    if (!lc_pouch_index_parse_lql_datetime(bounds->gte, &out->gte)) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index date gte bound is invalid", NULL, NULL,
                          "pouch-redesign");
    }
    out->has_gte = 1;
  }
  if (bounds->has_lt) {
    if (!lc_pouch_index_parse_lql_datetime(bounds->lt, &out->lt)) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index date lt bound is invalid", NULL, NULL,
                          "pouch-redesign");
    }
    out->has_lt = 1;
  }
  if (bounds->has_lte) {
    if (!lc_pouch_index_parse_lql_datetime(bounds->lte, &out->lte)) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index date lte bound is invalid", NULL, NULL,
                          "pouch-redesign");
    }
    out->has_lte = 1;
  }
  if (!out->has_gt && !out->has_gte && !out->has_lt && !out->has_lte) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index date lookup requires bounded selector",
                        NULL, NULL, "pouch-redesign");
  }
  return LC_OK;
}

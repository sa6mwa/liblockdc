#include "lc_pouch_query_index.h"

#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"

#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define LC_POUCH_QUERY_INDEX_FORMAT "pouch-query-index"
#define LC_POUCH_QUERY_INDEX_VERSION 4UL
#define LC_POUCH_QUERY_INDEX_LEAF "query.index"
#define LC_POUCH_QUERY_INDEX_HASH_OFFSET 2166136261UL
#define LC_POUCH_QUERY_INDEX_HASH_PRIME 16777619UL
#define LC_POUCH_QUERY_INDEX_HASH_MASK 0xffffffffUL

typedef struct lc_pouch_query_index_row {
  char *key_hex;
  char *content_type_hex;
  char *etag_hex;
  unsigned long version;
  unsigned long bytes;
  int has_query_hidden;
  int query_hidden;
} lc_pouch_query_index_row;

typedef struct lc_pouch_query_index_term {
  char *field_hex;
  char *value_hex;
  char *key_hex;
} lc_pouch_query_index_term;

typedef struct lc_pouch_query_index_presence {
  char *field_hex;
  char *key_hex;
} lc_pouch_query_index_presence;

typedef struct lc_pouch_query_index_summary {
  const lc_allocator *allocator;
  lc_pouch *pouch;
  const char *namespace_name;
  lc_pouch_query_index_row *rows;
  size_t count;
  size_t capacity;
  lc_pouch_query_index_term *terms;
  size_t term_count;
  size_t term_capacity;
  int term_index_complete;
  lc_pouch_query_index_presence *presences;
  size_t presence_count;
  size_t presence_capacity;
  int presence_index_complete;
} lc_pouch_query_index_summary;

typedef struct lc_pouch_query_index_text {
  const lc_allocator *allocator;
  char *bytes;
  size_t length;
  size_t capacity;
} lc_pouch_query_index_text;

typedef struct lc_pouch_query_index_read_result {
  unsigned long index_seq;
  unsigned long row_count;
  unsigned long row_hash;
  unsigned long term_count;
  unsigned long term_hash;
  unsigned long presence_count;
  unsigned long presence_hash;
  int term_index_complete;
  int presence_index_complete;
  int present;
  int valid;
} lc_pouch_query_index_read_result;

typedef struct lc_pouch_query_index_row_reader {
  const lc_allocator *allocator;
  lc_pouch_query_index_row_visit_fn visit;
  void *context;
} lc_pouch_query_index_row_reader;

typedef struct lc_pouch_query_index_term_reader {
  const lc_allocator *allocator;
  const char *field_hex;
  const char *value_hex;
  const char *value_text;
  int prefix_match;
  int contains_match;
  int ignore_case;
  int range_match;
  int date_match;
  int stop;
  lc_pouch_query_index_range_bounds range_bounds;
  lc_pouch_query_index_date_bounds date_bounds;
  lc_pouch_index_parsed_date_bounds parsed_date_bounds;
  lc_pouch_query_index_key_visit_fn visit;
  void *context;
} lc_pouch_query_index_term_reader;

typedef struct lc_pouch_query_index_presence_reader {
  const lc_allocator *allocator;
  const char *field_hex;
  lc_pouch_query_index_key_visit_fn visit;
  void *context;
} lc_pouch_query_index_presence_reader;

typedef struct lc_pouch_query_index_source_reader {
  lc_source *source;
  lc_error error;
} lc_pouch_query_index_source_reader;

typedef struct lc_pouch_query_index_extract_context {
  lc_pouch_query_index_summary *summary;
  const char *key_hex;
  char *field;
  size_t field_len;
  size_t field_capacity;
  char *value;
  size_t value_len;
  size_t value_capacity;
} lc_pouch_query_index_extract_context;

static unsigned long lc_pouch_query_index_hash_init(void) {
  return LC_POUCH_QUERY_INDEX_HASH_OFFSET;
}

static void lc_pouch_query_index_hash_byte(unsigned long *hash,
                                           unsigned char byte) {
  *hash ^= (unsigned long)byte;
  *hash *= LC_POUCH_QUERY_INDEX_HASH_PRIME;
  *hash &= LC_POUCH_QUERY_INDEX_HASH_MASK;
}

static void lc_pouch_query_index_hash_bytes(unsigned long *hash,
                                            const char *bytes,
                                            size_t length) {
  size_t index;

  for (index = 0U; index < length; ++index) {
    lc_pouch_query_index_hash_byte(hash, (unsigned char)bytes[index]);
  }
}

static char *lc_pouch_query_index_hex_encode_bytes(
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

static char *lc_pouch_query_index_hex_encode(const lc_allocator *allocator,
                                             const char *value) {
  return lc_pouch_query_index_hex_encode_bytes(
      allocator, value, value != NULL ? strlen(value) : 0U);
}

static void lc_pouch_query_index_summary_cleanup(
    lc_pouch_query_index_summary *summary) {
  size_t index;

  if (summary == NULL) {
    return;
  }
  for (index = 0U; index < summary->count; ++index) {
    lc_free_with_allocator(summary->allocator, summary->rows[index].key_hex);
    lc_free_with_allocator(summary->allocator,
                           summary->rows[index].content_type_hex);
    lc_free_with_allocator(summary->allocator, summary->rows[index].etag_hex);
  }
  for (index = 0U; index < summary->term_count; ++index) {
    lc_free_with_allocator(summary->allocator, summary->terms[index].field_hex);
    lc_free_with_allocator(summary->allocator, summary->terms[index].value_hex);
    lc_free_with_allocator(summary->allocator, summary->terms[index].key_hex);
  }
  for (index = 0U; index < summary->presence_count; ++index) {
    lc_free_with_allocator(summary->allocator,
                           summary->presences[index].field_hex);
    lc_free_with_allocator(summary->allocator,
                           summary->presences[index].key_hex);
  }
  lc_free_with_allocator(summary->allocator, summary->rows);
  lc_free_with_allocator(summary->allocator, summary->terms);
  lc_free_with_allocator(summary->allocator, summary->presences);
  memset(summary, 0, sizeof(*summary));
}

static int lc_pouch_query_index_summary_reserve(
    lc_pouch_query_index_summary *summary, size_t needed, lc_error *error) {
  lc_pouch_query_index_row *next_rows;
  size_t next_capacity;

  if (needed <= summary->capacity) {
    return LC_OK;
  }
  next_capacity = summary->capacity == 0U ? 16U : summary->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch query-index summary exceeds local limit", NULL,
                          NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_rows = (lc_pouch_query_index_row *)lc_alloc_with_allocator(
      summary->allocator, next_capacity * sizeof(*next_rows));
  if (next_rows == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index summary rows",
                        NULL, NULL, NULL);
  }
  if (summary->rows != NULL) {
    memcpy(next_rows, summary->rows, summary->count * sizeof(*next_rows));
    lc_free_with_allocator(summary->allocator, summary->rows);
  }
  memset(next_rows + summary->count, 0,
         (next_capacity - summary->count) * sizeof(*next_rows));
  summary->rows = next_rows;
  summary->capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_query_index_term_reserve(
    lc_pouch_query_index_summary *summary, size_t needed, lc_error *error) {
  lc_pouch_query_index_term *next_terms;
  size_t next_capacity;

  if (needed <= summary->term_capacity) {
    return LC_OK;
  }
  next_capacity = summary->term_capacity == 0U ? 32U : summary->term_capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch query-index terms exceed local limit", NULL,
                          NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_terms = (lc_pouch_query_index_term *)lc_alloc_with_allocator(
      summary->allocator, next_capacity * sizeof(*next_terms));
  if (next_terms == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index terms", NULL,
                        NULL, NULL);
  }
  if (summary->terms != NULL) {
    memcpy(next_terms, summary->terms, summary->term_count * sizeof(*next_terms));
    lc_free_with_allocator(summary->allocator, summary->terms);
  }
  memset(next_terms + summary->term_count, 0,
         (next_capacity - summary->term_count) * sizeof(*next_terms));
  summary->terms = next_terms;
  summary->term_capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_query_index_term_add(
    lc_pouch_query_index_summary *summary, const char *field, size_t field_len,
    const char *value, size_t value_len, const char *key_hex,
    lc_error *error) {
  lc_pouch_query_index_term *term;
  int rc;

  if (field == NULL || field_len == 0U || value == NULL || key_hex == NULL) {
    return LC_OK;
  }
  rc = lc_pouch_query_index_term_reserve(summary, summary->term_count + 1U,
                                         error);
  if (rc != LC_OK) {
    return rc;
  }
  term = &summary->terms[summary->term_count];
  memset(term, 0, sizeof(*term));
  term->field_hex =
      lc_pouch_query_index_hex_encode_bytes(summary->allocator, field,
                                            field_len);
  term->value_hex =
      lc_pouch_query_index_hex_encode_bytes(summary->allocator, value,
                                            value_len);
  term->key_hex = lc_strdup_with_allocator(summary->allocator, key_hex);
  if (term->field_hex == NULL || term->value_hex == NULL ||
      term->key_hex == NULL) {
    lc_free_with_allocator(summary->allocator, term->field_hex);
    lc_free_with_allocator(summary->allocator, term->value_hex);
    lc_free_with_allocator(summary->allocator, term->key_hex);
    memset(term, 0, sizeof(*term));
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index term", NULL,
                        NULL, NULL);
  }
  ++summary->term_count;
  return LC_OK;
}

static int lc_pouch_query_index_presence_reserve(
    lc_pouch_query_index_summary *summary, size_t needed, lc_error *error) {
  lc_pouch_query_index_presence *next_presences;
  size_t next_capacity;

  if (needed <= summary->presence_capacity) {
    return LC_OK;
  }
  next_capacity =
      summary->presence_capacity == 0U ? 32U : summary->presence_capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch query-index presences exceed local limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_presences = (lc_pouch_query_index_presence *)lc_alloc_with_allocator(
      summary->allocator, next_capacity * sizeof(*next_presences));
  if (next_presences == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index presences", NULL,
                        NULL, NULL);
  }
  if (summary->presences != NULL) {
    memcpy(next_presences, summary->presences,
           summary->presence_count * sizeof(*next_presences));
    lc_free_with_allocator(summary->allocator, summary->presences);
  }
  memset(next_presences + summary->presence_count, 0,
         (next_capacity - summary->presence_count) * sizeof(*next_presences));
  summary->presences = next_presences;
  summary->presence_capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_query_index_presence_add(
    lc_pouch_query_index_summary *summary, const char *field, size_t field_len,
    const char *key_hex, lc_error *error) {
  lc_pouch_query_index_presence *presence;
  int rc;

  if (field == NULL || field_len == 0U || key_hex == NULL) {
    return LC_OK;
  }
  rc = lc_pouch_query_index_presence_reserve(
      summary, summary->presence_count + 1U, error);
  if (rc != LC_OK) {
    return rc;
  }
  presence = &summary->presences[summary->presence_count];
  memset(presence, 0, sizeof(*presence));
  presence->field_hex = lc_pouch_query_index_hex_encode_bytes(
      summary->allocator, field, field_len);
  presence->key_hex = lc_strdup_with_allocator(summary->allocator, key_hex);
  if (presence->field_hex == NULL || presence->key_hex == NULL) {
    lc_free_with_allocator(summary->allocator, presence->field_hex);
    lc_free_with_allocator(summary->allocator, presence->key_hex);
    memset(presence, 0, sizeof(*presence));
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index presence", NULL,
                        NULL, NULL);
  }
  ++summary->presence_count;
  return LC_OK;
}

static lonejson_read_result
lc_pouch_query_index_lonejson_read(void *user, unsigned char *buffer,
                                   size_t capacity) {
  lc_pouch_query_index_source_reader *reader;
  lonejson_read_result result;

  memset(&result, 0, sizeof(result));
  reader = (lc_pouch_query_index_source_reader *)user;
  if (reader == NULL || reader->source == NULL) {
    result.error_code = EINVAL;
    return result;
  }
  result.bytes_read =
      reader->source->read(reader->source, buffer, capacity, &reader->error);
  if (result.bytes_read == 0U) {
    if (reader->error.code != LC_OK) {
      result.error_code = EIO;
    } else {
      result.eof = 1;
    }
  }
  return result;
}

static int lc_pouch_query_index_extract_reserve(
    lc_pouch_query_index_extract_context *context, int field, size_t extra,
    lc_error *error) {
  char **bytes;
  size_t *length;
  size_t *capacity;
  char *next;
  size_t needed;
  size_t next_capacity;

  bytes = field ? &context->field : &context->value;
  length = field ? &context->field_len : &context->value_len;
  capacity = field ? &context->field_capacity : &context->value_capacity;
  if (extra > (size_t)-1 - *length - 1U) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch query-index extracted term exceeds local limit",
                        NULL, NULL, NULL);
  }
  needed = *length + extra + 1U;
  if (needed <= *capacity) {
    return LC_OK;
  }
  next_capacity = *capacity == 0U ? 64U : *capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch query-index extracted term exceeds local "
                          "limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next = (char *)lc_alloc_with_allocator(context->summary->allocator,
                                         next_capacity);
  if (next == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index extracted term",
                        NULL, NULL, NULL);
  }
  if (*bytes != NULL) {
    memcpy(next, *bytes, *length);
    lc_free_with_allocator(context->summary->allocator, *bytes);
  }
  *bytes = next;
  *capacity = next_capacity;
  (*bytes)[*length] = '\0';
  return LC_OK;
}

static int lc_pouch_query_index_extract_append(
    lc_pouch_query_index_extract_context *context, int field, const char *bytes,
    size_t length, lc_error *error) {
  char **target;
  size_t *target_len;
  int rc;

  rc = lc_pouch_query_index_extract_reserve(context, field, length, error);
  if (rc != LC_OK) {
    return rc;
  }
  target = field ? &context->field : &context->value;
  target_len = field ? &context->field_len : &context->value_len;
  memcpy(*target + *target_len, bytes, length);
  *target_len += length;
  (*target)[*target_len] = '\0';
  return LC_OK;
}

static int lc_pouch_query_index_path_segment_is_array_index(
    const lonejson_path_segment *segment) {
  size_t index;

  if (segment == NULL || segment->len == 0U) {
    return 0;
  }
  for (index = 0U; index < segment->len; ++index) {
    if (segment->data[index] < '0' || segment->data[index] > '9') {
      return 0;
    }
  }
  return 1;
}

static int lc_pouch_query_index_extract_set_field(
    lc_pouch_query_index_extract_context *context,
    const lonejson_value_path *path, lc_error *error) {
  size_t segment_index;

  context->field_len = 0U;
  if (context->field != NULL) {
    context->field[0] = '\0';
  }
  if (path == NULL || path->segment_count == 0U) {
    return LC_OK;
  }
  for (segment_index = 0U; segment_index < path->segment_count;
       ++segment_index) {
    const lonejson_path_segment *segment;
    size_t byte_index;
    int rc;

    segment = &path->segments[segment_index];
    rc = lc_pouch_query_index_extract_append(context, 1, "/", 1U, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (lc_pouch_query_index_path_segment_is_array_index(segment)) {
      rc = lc_pouch_query_index_extract_append(context, 1, "[]", 2U, error);
      if (rc != LC_OK) {
        return rc;
      }
      continue;
    }
    for (byte_index = 0U; byte_index < segment->len; ++byte_index) {
      const char *replacement;
      char byte;

      byte = segment->data[byte_index];
      replacement = NULL;
      if (byte == '~') {
        replacement = "~0";
      } else if (byte == '/') {
        replacement = "~1";
      }
      if (replacement != NULL) {
        rc = lc_pouch_query_index_extract_append(context, 1, replacement, 2U,
                                                 error);
      } else {
        rc = lc_pouch_query_index_extract_append(context, 1, &byte, 1U, error);
      }
      if (rc != LC_OK) {
        return rc;
      }
    }
  }
  return LC_OK;
}

static lonejson_status lc_pouch_query_index_lonejson_error(
    lonejson_error *lj_error, const lc_error *error) {
  if (lj_error != NULL) {
    lonejson_error_init(lj_error);
    lj_error->code = error != NULL && error->code == LC_ERR_NOMEM
                         ? LONEJSON_STATUS_ALLOCATION_FAILED
                         : LONEJSON_STATUS_CALLBACK_FAILED;
    snprintf(lj_error->message, sizeof(lj_error->message), "%s",
             error != NULL && error->message != NULL ? error->message
                                                     : "pouch query-index "
                                                       "callback failed");
  }
  return lj_error != NULL ? lj_error->code : LONEJSON_STATUS_CALLBACK_FAILED;
}

static lonejson_status lc_pouch_query_index_presence_value(
    void *user, const lonejson_value_path *path, lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  int rc;

  context = (lc_pouch_query_index_extract_context *)user;
  lc_error_init(&error);
  rc = lc_pouch_query_index_extract_set_field(context, path, &error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_presence_add(
        context->summary, context->field, context->field_len, context->key_hex,
        &error);
  }
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_pouch_query_index_string_begin(
    void *user, const lonejson_value_path *path, lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  int rc;

  context = (lc_pouch_query_index_extract_context *)user;
  lc_error_init(&error);
  context->value_len = 0U;
  if (context->value != NULL) {
    context->value[0] = '\0';
  }
  rc = lc_pouch_query_index_extract_set_field(context, path, &error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_presence_add(
        context->summary, context->field, context->field_len, context->key_hex,
        &error);
  }
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_pouch_query_index_string_chunk(
    void *user, const lonejson_value_path *path, const char *data, size_t len,
    lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  int rc;

  (void)path;
  context = (lc_pouch_query_index_extract_context *)user;
  lc_error_init(&error);
  rc = lc_pouch_query_index_extract_append(context, 0, data, len, &error);
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_pouch_query_index_string_end(
    void *user, const lonejson_value_path *path, lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  int rc;

  (void)path;
  context = (lc_pouch_query_index_extract_context *)user;
  lc_error_init(&error);
  rc = lc_pouch_query_index_term_add(
      context->summary, context->field, context->field_len, context->value,
      context->value_len, context->key_hex, &error);
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_pouch_query_index_number_begin(
    void *user, const lonejson_value_path *path, lonejson_error *lj_error) {
  return lc_pouch_query_index_string_begin(user, path, lj_error);
}

static lonejson_status lc_pouch_query_index_number_chunk(
    void *user, const lonejson_value_path *path, const char *data, size_t len,
    lonejson_error *lj_error) {
  return lc_pouch_query_index_string_chunk(user, path, data, len, lj_error);
}

static lonejson_status lc_pouch_query_index_number_end(
    void *user, const lonejson_value_path *path, lonejson_error *lj_error) {
  return lc_pouch_query_index_string_end(user, path, lj_error);
}

static lonejson_status lc_pouch_query_index_boolean_value(
    void *user, const lonejson_value_path *path, int value,
    lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  const char *text;
  int rc;

  context = (lc_pouch_query_index_extract_context *)user;
  text = value ? "true" : "false";
  lc_error_init(&error);
  rc = lc_pouch_query_index_extract_set_field(context, path, &error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_presence_add(
        context->summary, context->field, context->field_len, context->key_hex,
        &error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_term_add(context->summary, context->field,
                                       context->field_len, text, strlen(text),
                                       context->key_hex, &error);
  }
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_pouch_query_index_null_value(
    void *user, const lonejson_value_path *path, lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  int rc;

  context = (lc_pouch_query_index_extract_context *)user;
  lc_error_init(&error);
  rc = lc_pouch_query_index_extract_set_field(context, path, &error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_presence_add(
        context->summary, context->field, context->field_len, context->key_hex,
        &error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_term_add(context->summary, context->field,
                                       context->field_len, "null", 4U,
                                       context->key_hex, &error);
  }
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static void lc_pouch_query_index_extract_context_cleanup(
    lc_pouch_query_index_extract_context *context) {
  if (context == NULL || context->summary == NULL) {
    return;
  }
  lc_free_with_allocator(context->summary->allocator, context->field);
  lc_free_with_allocator(context->summary->allocator, context->value);
  memset(context, 0, sizeof(*context));
}

static int lc_pouch_query_index_extract_terms(
    lc_pouch_query_index_summary *summary, const char *key,
    const char *key_hex, lc_error *error) {
  lc_pouch_state_read_result read_result;
  lc_pouch_query_index_source_reader reader;
  lc_pouch_query_index_extract_context context;
  lonejson_path_value_visitor visitor;
  lonejson_error lj_error;
  lonejson *runtime;
  lonejson_status status;
  int rc;

  memset(&read_result, 0, sizeof(read_result));
  memset(&reader, 0, sizeof(reader));
  memset(&context, 0, sizeof(context));
  rc = lc_pouch_state_read(summary->pouch, summary->namespace_name, key,
                           &read_result, error);
  if (rc != LC_OK || !read_result.found || read_result.body == NULL) {
    lc_pouch_state_read_result_cleanup(summary->allocator, &read_result);
    return rc;
  }
  if (read_result.body->reset != NULL) {
    rc = read_result.body->reset(read_result.body, error);
    if (rc != LC_OK) {
      lc_pouch_state_read_result_cleanup(summary->allocator, &read_result);
      return rc;
    }
  }
  runtime = lc_thread_lonejson_runtime();
  if (runtime == NULL) {
    lc_pouch_state_read_result_cleanup(summary->allocator, &read_result);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize pouch query-index JSON runtime",
                        NULL, NULL, NULL);
  }
  reader.source = read_result.body;
  lc_error_init(&reader.error);
  context.summary = summary;
  context.key_hex = key_hex;
  visitor = lonejson_default_path_value_visitor();
  visitor.object_begin = lc_pouch_query_index_presence_value;
  visitor.array_begin = lc_pouch_query_index_presence_value;
  visitor.string_begin = lc_pouch_query_index_string_begin;
  visitor.string_chunk = lc_pouch_query_index_string_chunk;
  visitor.string_end = lc_pouch_query_index_string_end;
  visitor.number_begin = lc_pouch_query_index_number_begin;
  visitor.number_chunk = lc_pouch_query_index_number_chunk;
  visitor.number_end = lc_pouch_query_index_number_end;
  visitor.boolean_value = lc_pouch_query_index_boolean_value;
  visitor.null_value = lc_pouch_query_index_null_value;
  lonejson_error_init(&lj_error);
  status = runtime->visit_path_value_reader(
      runtime, lc_pouch_query_index_lonejson_read, &reader, &visitor, &context,
      &lj_error);
  if (reader.error.code != LC_OK) {
    rc = reader.error.code;
    if (error != NULL) {
      *error = reader.error;
      memset(&reader.error, 0, sizeof(reader.error));
    }
  } else if (status == LONEJSON_STATUS_ALLOCATION_FAILED ||
             status == LONEJSON_STATUS_CALLBACK_FAILED) {
    rc = lc_lonejson_error_from_status(
        error, status, &lj_error, "failed to build pouch query-index terms");
  } else {
    if (status != LONEJSON_STATUS_OK) {
      summary->term_index_complete = 0;
      summary->presence_index_complete = 0;
    }
    rc = LC_OK;
  }
  lc_error_cleanup(&reader.error);
  lc_pouch_query_index_extract_context_cleanup(&context);
  lc_pouch_state_read_result_cleanup(summary->allocator, &read_result);
  return rc;
}

static int lc_pouch_query_index_summary_visit(
    const lc_pouch_state_visit_entry *entry, void *context, lc_error *error) {
  lc_pouch_query_index_summary *summary;
  lc_pouch_query_index_row *row;
  int rc;

  summary = (lc_pouch_query_index_summary *)context;
  if (entry == NULL || entry->key == NULL) {
    return LC_OK;
  }
  if (strncmp(entry->key, ".staging/", sizeof(".staging/") - 1U) == 0 ||
      strstr(entry->key, "/.staging/") != NULL) {
    return LC_OK;
  }
  rc = lc_pouch_query_index_summary_reserve(summary, summary->count + 1U,
                                            error);
  if (rc != LC_OK) {
    return rc;
  }
  row = &summary->rows[summary->count];
  memset(row, 0, sizeof(*row));
  row->key_hex = lc_pouch_query_index_hex_encode(summary->allocator,
                                                 entry->key);
  row->content_type_hex =
      lc_pouch_query_index_hex_encode(summary->allocator,
                                      entry->content_type);
  row->etag_hex = lc_pouch_query_index_hex_encode(summary->allocator,
                                                  entry->etag);
  if (row->key_hex == NULL || row->content_type_hex == NULL ||
      row->etag_hex == NULL) {
    lc_free_with_allocator(summary->allocator, row->key_hex);
    lc_free_with_allocator(summary->allocator, row->content_type_hex);
    lc_free_with_allocator(summary->allocator, row->etag_hex);
    memset(row, 0, sizeof(*row));
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index summary row",
                        NULL, NULL, NULL);
  }
  row->version = entry->version;
  row->bytes = entry->bytes;
  row->has_query_hidden = entry->has_query_hidden;
  row->query_hidden = entry->query_hidden;
  ++summary->count;
  return lc_pouch_query_index_extract_terms(summary, entry->key, row->key_hex,
                                            error);
}

static int lc_pouch_query_index_row_compare(const void *left,
                                            const void *right) {
  const lc_pouch_query_index_row *a;
  const lc_pouch_query_index_row *b;
  int cmp;

  a = (const lc_pouch_query_index_row *)left;
  b = (const lc_pouch_query_index_row *)right;
  cmp = strcmp(a->key_hex, b->key_hex);
  if (cmp != 0) {
    return cmp;
  }
  if (a->version < b->version) {
    return -1;
  }
  if (a->version > b->version) {
    return 1;
  }
  return 0;
}

static int lc_pouch_query_index_term_compare(const void *left,
                                             const void *right) {
  const lc_pouch_query_index_term *a;
  const lc_pouch_query_index_term *b;
  int cmp;

  a = (const lc_pouch_query_index_term *)left;
  b = (const lc_pouch_query_index_term *)right;
  cmp = strcmp(a->field_hex, b->field_hex);
  if (cmp != 0) {
    return cmp;
  }
  cmp = strcmp(a->value_hex, b->value_hex);
  if (cmp != 0) {
    return cmp;
  }
  return strcmp(a->key_hex, b->key_hex);
}

static int lc_pouch_query_index_term_equal(
    const lc_pouch_query_index_term *left,
    const lc_pouch_query_index_term *right) {
  return left != NULL && right != NULL &&
         strcmp(left->field_hex, right->field_hex) == 0 &&
         strcmp(left->value_hex, right->value_hex) == 0 &&
         strcmp(left->key_hex, right->key_hex) == 0;
}

static int lc_pouch_query_index_presence_compare(const void *left,
                                                 const void *right) {
  const lc_pouch_query_index_presence *a;
  const lc_pouch_query_index_presence *b;
  int cmp;

  a = (const lc_pouch_query_index_presence *)left;
  b = (const lc_pouch_query_index_presence *)right;
  cmp = strcmp(a->field_hex, b->field_hex);
  if (cmp != 0) {
    return cmp;
  }
  return strcmp(a->key_hex, b->key_hex);
}

static int lc_pouch_query_index_presence_equal(
    const lc_pouch_query_index_presence *left,
    const lc_pouch_query_index_presence *right) {
  return left != NULL && right != NULL &&
         strcmp(left->field_hex, right->field_hex) == 0 &&
         strcmp(left->key_hex, right->key_hex) == 0;
}

static int lc_pouch_query_index_text_reserve(lc_pouch_query_index_text *text,
                                             size_t extra, lc_error *error) {
  char *next_bytes;
  size_t needed;
  size_t next_capacity;

  if (extra > (size_t)-1 - text->length - 1U) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch query-index text exceeds local limit", NULL,
                        NULL, NULL);
  }
  needed = text->length + extra + 1U;
  if (needed <= text->capacity) {
    return LC_OK;
  }
  next_capacity = text->capacity == 0U ? 256U : text->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch query-index text exceeds local limit", NULL,
                          NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_bytes = (char *)lc_alloc_with_allocator(text->allocator, next_capacity);
  if (next_bytes == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index text", NULL,
                        NULL, NULL);
  }
  if (text->bytes != NULL) {
    memcpy(next_bytes, text->bytes, text->length);
    lc_free_with_allocator(text->allocator, text->bytes);
  }
  text->bytes = next_bytes;
  text->capacity = next_capacity;
  text->bytes[text->length] = '\0';
  return LC_OK;
}

static int lc_pouch_query_index_text_append(lc_pouch_query_index_text *text,
                                            const char *bytes, size_t length,
                                            unsigned long *hash,
                                            lc_error *error) {
  int rc;

  rc = lc_pouch_query_index_text_reserve(text, length, error);
  if (rc != LC_OK) {
    return rc;
  }
  memcpy(text->bytes + text->length, bytes, length);
  text->length += length;
  text->bytes[text->length] = '\0';
  if (hash != NULL) {
    lc_pouch_query_index_hash_bytes(hash, bytes, length);
  }
  return LC_OK;
}

static int lc_pouch_query_index_text_append_cstr(
    lc_pouch_query_index_text *text, const char *bytes, unsigned long *hash,
    lc_error *error) {
  return lc_pouch_query_index_text_append(text, bytes, strlen(bytes), hash,
                                          error);
}

static int lc_pouch_query_index_read_line(FILE *fp,
                                          lc_pouch_query_index_text *line,
                                          int *got_line, lc_error *error) {
  int ch;

  if (line == NULL || got_line == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index line read requires outputs", NULL,
                        NULL, NULL);
  }
  line->length = 0U;
  if (line->bytes != NULL) {
    line->bytes[0] = '\0';
  }
  *got_line = 0;
  while ((ch = fgetc(fp)) != EOF) {
    int rc;
    char byte;

    byte = (char)ch;
    rc = lc_pouch_query_index_text_append(line, &byte, 1U, NULL, error);
    if (rc != LC_OK) {
      return rc;
    }
    *got_line = 1;
    if (ch == '\n') {
      return LC_OK;
    }
  }
  if (ferror(fp)) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch query-index row",
                        strerror(errno), NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_query_index_hex_value(unsigned char value) {
  if (value >= '0' && value <= '9') {
    return (int)(value - '0');
  }
  if (value >= 'a' && value <= 'f') {
    return (int)(value - 'a') + 10;
  }
  if (value >= 'A' && value <= 'F') {
    return (int)(value - 'A') + 10;
  }
  return -1;
}

static int lc_pouch_query_index_hex_token_valid(const char *token) {
  size_t index;
  size_t length;

  if (token == NULL || token[0] == '\0') {
    return 0;
  }
  if (strcmp(token, "-") == 0) {
    return 1;
  }
  length = strlen(token);
  if ((length % 2U) != 0U) {
    return 0;
  }
  for (index = 0U; index < length; ++index) {
    if (lc_pouch_query_index_hex_value((unsigned char)token[index]) < 0) {
      return 0;
    }
  }
  return 1;
}

static char *lc_pouch_query_index_hex_decode(
    const lc_allocator *allocator, const char *token, lc_error *error) {
  char *decoded;
  size_t index;
  size_t length;

  if (token == NULL || token[0] == '\0' || strcmp(token, "-") == 0) {
    return lc_strdup_with_allocator(allocator, "");
  }
  length = strlen(token);
  if ((length % 2U) != 0U) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query-index row has invalid hex token", NULL, NULL,
                 NULL);
    return NULL;
  }
  decoded = (char *)lc_alloc_with_allocator(allocator, (length / 2U) + 1U);
  if (decoded == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index decoded key", NULL,
                 NULL, NULL);
    return NULL;
  }
  for (index = 0U; index < length; index += 2U) {
    int high;
    int low;

    high = lc_pouch_query_index_hex_value((unsigned char)token[index]);
    low = lc_pouch_query_index_hex_value((unsigned char)token[index + 1U]);
    if (high < 0 || low < 0) {
      lc_free_with_allocator(allocator, decoded);
      lc_error_set(error, LC_ERR_INVALID, 0L,
                   "pouch query-index row has invalid hex token", NULL, NULL,
                   NULL);
      return NULL;
    }
    decoded[index / 2U] = (char)(((unsigned int)high << 4) | (unsigned int)low);
  }
  decoded[length / 2U] = '\0';
  return decoded;
}

static int lc_pouch_query_index_parse_ulong_token(char **cursor,
                                                  unsigned long *out) {
  char *begin;
  char *end;

  if (cursor == NULL || *cursor == NULL || out == NULL) {
    return 0;
  }
  begin = *cursor;
  if (*begin == '\0' || *begin == '\n' || *begin == ' ') {
    return 0;
  }
  errno = 0;
  *out = strtoul(begin, &end, 10);
  if (errno != 0 || end == begin || *end != ' ') {
    return 0;
  }
  *cursor = end + 1;
  return 1;
}

static int lc_pouch_query_index_parse_int_token(char **cursor, int *out) {
  unsigned long parsed;

  if (!lc_pouch_query_index_parse_ulong_token(cursor, &parsed) ||
      parsed > 1UL) {
    return 0;
  }
  *out = parsed != 0UL;
  return 1;
}

static char *lc_pouch_query_index_next_token(char **cursor, int final_token) {
  char *begin;
  char *end;

  if (cursor == NULL || *cursor == NULL) {
    return NULL;
  }
  begin = *cursor;
  if (*begin == '\0' || *begin == '\n' || *begin == ' ') {
    return NULL;
  }
  end = begin;
  while (*end != '\0' && *end != '\n' && *end != ' ') {
    ++end;
  }
  if (final_token) {
    if (*end == ' ') {
      return NULL;
    }
  } else if (*end != ' ') {
    return NULL;
  }
  if (*end != '\0') {
    *end = '\0';
    *cursor = end + 1;
  } else {
    *cursor = end;
  }
  return begin;
}

static int lc_pouch_query_index_parse_and_visit_row(
    const char *line_bytes, lc_pouch_query_index_row_reader *reader,
    lc_error *error) {
  lc_pouch_query_index_row_view row;
  char *line;
  char *cursor;
  char *key_hex;
  char *content_type_hex;
  char *etag_hex;
  char *key;
  size_t line_len;
  int rc;

  if (line_bytes == NULL || reader == NULL || reader->visit == NULL) {
    return LC_OK;
  }
  line_len = strlen(line_bytes);
  line = (char *)lc_alloc_with_allocator(reader->allocator, line_len + 1U);
  if (line == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index row parser",
                        NULL, NULL, NULL);
  }
  memcpy(line, line_bytes, line_len + 1U);
  if (strncmp(line, "row ", sizeof("row ") - 1U) != 0) {
    lc_free_with_allocator(reader->allocator, line);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index row has invalid prefix", NULL,
                        NULL, NULL);
  }
  cursor = line + sizeof("row ") - 1U;
  memset(&row, 0, sizeof(row));
  key = NULL;
  key_hex = NULL;
  content_type_hex = NULL;
  etag_hex = NULL;
  rc = LC_OK;
  if (!lc_pouch_query_index_parse_ulong_token(&cursor, &row.version) ||
      !lc_pouch_query_index_parse_ulong_token(&cursor, &row.bytes) ||
      !lc_pouch_query_index_parse_int_token(&cursor, &row.has_query_hidden) ||
      !lc_pouch_query_index_parse_int_token(&cursor, &row.query_hidden)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index row has invalid numeric fields", NULL,
                      NULL, NULL);
  }
  if (rc == LC_OK) {
    key_hex = lc_pouch_query_index_next_token(&cursor, 0);
    content_type_hex = lc_pouch_query_index_next_token(&cursor, 0);
    etag_hex = lc_pouch_query_index_next_token(&cursor, 1);
    if (key_hex == NULL || content_type_hex == NULL || etag_hex == NULL ||
        strcmp(key_hex, "-") == 0 ||
        !lc_pouch_query_index_hex_token_valid(key_hex) ||
        !lc_pouch_query_index_hex_token_valid(content_type_hex) ||
        !lc_pouch_query_index_hex_token_valid(etag_hex)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index row has invalid hex fields", NULL,
                        NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    key = lc_pouch_query_index_hex_decode(reader->allocator, key_hex, error);
    if (key == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
  }
  if (rc == LC_OK) {
    row.key = key;
    rc = reader->visit(&row, reader->context, error);
  }
  lc_free_with_allocator(reader->allocator, key);
  lc_free_with_allocator(reader->allocator, line);
  return rc;
}

static int lc_pouch_query_index_parse_number_value(const char *text,
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

static int lc_pouch_query_index_range_contains_value(
    const lc_pouch_query_index_range_bounds *bounds, double value) {
  if (bounds == NULL ||
      (!bounds->has_gt && !bounds->has_gte && !bounds->has_lt &&
       !bounds->has_lte)) {
    return 0;
  }
  if (bounds->has_gt && !(value > bounds->gt)) {
    return 0;
  }
  if (bounds->has_gte && !(value >= bounds->gte)) {
    return 0;
  }
  if (bounds->has_lt && !(value < bounds->lt)) {
    return 0;
  }
  if (bounds->has_lte && !(value <= bounds->lte)) {
    return 0;
  }
  return 1;
}

static unsigned char lc_pouch_query_index_ascii_lower(unsigned char ch) {
  if (ch >= 'A' && ch <= 'Z') {
    return (unsigned char)(ch - 'A' + 'a');
  }
  return ch;
}

static int lc_pouch_query_index_text_has_prefix(const char *value,
                                                const char *prefix,
                                                int ignore_case) {
  size_t index;

  if (value == NULL || prefix == NULL) {
    return 0;
  }
  for (index = 0U; prefix[index] != '\0'; ++index) {
    unsigned char a;
    unsigned char b;

    if (value[index] == '\0') {
      return 0;
    }
    a = (unsigned char)value[index];
    b = (unsigned char)prefix[index];
    if (ignore_case) {
      a = lc_pouch_query_index_ascii_lower(a);
      b = lc_pouch_query_index_ascii_lower(b);
    }
    if (a != b) {
      return 0;
    }
  }
  return 1;
}

static int lc_pouch_query_index_text_contains(const char *value,
                                              const char *needle,
                                              int ignore_case) {
  size_t value_index;
  size_t needle_len;

  if (value == NULL || needle == NULL) {
    return 0;
  }
  if (!ignore_case) {
    return strstr(value, needle) != NULL;
  }
  needle_len = strlen(needle);
  if (needle_len == 0U) {
    return 1;
  }
  for (value_index = 0U; value[value_index] != '\0'; ++value_index) {
    if (lc_pouch_query_index_text_has_prefix(value + value_index, needle,
                                             1)) {
      return 1;
    }
  }
  return 0;
}

static int lc_pouch_query_index_term_reader_matches_value(
    lc_pouch_query_index_term_reader *reader, const char *value_hex,
    int *matched, lc_error *error) {
  char *value_text;
  double number;
  lc_pouch_index_instant instant;

  if (matched == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term match requires output", NULL,
                        NULL, NULL);
  }
  *matched = 0;
  if (reader == NULL || value_hex == NULL) {
    return LC_OK;
  }
  if (reader->prefix_match) {
    if (reader->ignore_case) {
      value_text =
          lc_pouch_query_index_hex_decode(reader->allocator, value_hex, error);
      if (value_text == NULL) {
        return error != NULL && error->code != LC_OK ? error->code
                                                     : LC_ERR_NOMEM;
      }
      *matched = lc_pouch_query_index_text_has_prefix(
          value_text, reader->value_text, 1);
      lc_free_with_allocator(reader->allocator, value_text);
    } else {
      *matched = strncmp(value_hex, reader->value_hex,
                         strlen(reader->value_hex)) == 0;
    }
    return LC_OK;
  }
  if (reader->contains_match) {
    if (reader->ignore_case) {
      value_text =
          lc_pouch_query_index_hex_decode(reader->allocator, value_hex, error);
      if (value_text == NULL) {
        return error != NULL && error->code != LC_OK ? error->code
                                                     : LC_ERR_NOMEM;
      }
      *matched = lc_pouch_query_index_text_contains(value_text,
                                                    reader->value_text, 1);
      lc_free_with_allocator(reader->allocator, value_text);
    } else {
      *matched = strstr(value_hex, reader->value_hex) != NULL;
    }
    return LC_OK;
  }
  if (reader->range_match) {
    value_text =
        lc_pouch_query_index_hex_decode(reader->allocator, value_hex, error);
    if (value_text == NULL) {
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_NOMEM;
    }
    if (lc_pouch_query_index_parse_number_value(value_text, &number)) {
      *matched = lc_pouch_query_index_range_contains_value(
          &reader->range_bounds, number);
    }
    lc_free_with_allocator(reader->allocator, value_text);
    return LC_OK;
  }
  if (reader->date_match) {
    value_text =
        lc_pouch_query_index_hex_decode(reader->allocator, value_hex, error);
    if (value_text == NULL) {
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_NOMEM;
    }
    if (lc_pouch_index_parse_lql_datetime(value_text, &instant)) {
      *matched = lc_pouch_index_date_contains_value(
          &reader->parsed_date_bounds, &instant);
    }
    lc_free_with_allocator(reader->allocator, value_text);
    return LC_OK;
  }
  *matched = strcmp(value_hex, reader->value_hex) == 0;
  return LC_OK;
}

static int lc_pouch_query_index_parse_and_visit_term(
    const char *line_bytes, lc_pouch_query_index_term_reader *reader,
    lc_error *error) {
  lc_pouch_query_index_key_view key_view;
  char *line;
  char *cursor;
  char *field_hex;
  char *value_hex;
  char *key_hex;
  char *key;
  size_t line_len;
  int matched;
  int field_cmp;
  int value_cmp;
  int rc;

  if (line_bytes == NULL || reader == NULL || reader->visit == NULL) {
    return LC_OK;
  }
  line_len = strlen(line_bytes);
  line = (char *)lc_alloc_with_allocator(reader->allocator, line_len + 1U);
  if (line == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index term parser",
                        NULL, NULL, NULL);
  }
  memcpy(line, line_bytes, line_len + 1U);
  if (strncmp(line, "term ", sizeof("term ") - 1U) != 0) {
    lc_free_with_allocator(reader->allocator, line);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term has invalid prefix", NULL,
                        NULL, NULL);
  }
  cursor = line + sizeof("term ") - 1U;
  key = NULL;
  field_hex = lc_pouch_query_index_next_token(&cursor, 0);
  value_hex = lc_pouch_query_index_next_token(&cursor, 0);
  key_hex = lc_pouch_query_index_next_token(&cursor, 1);
  if (field_hex == NULL || value_hex == NULL || key_hex == NULL ||
      strcmp(field_hex, "-") == 0 || strcmp(key_hex, "-") == 0 ||
      !lc_pouch_query_index_hex_token_valid(field_hex) ||
      !lc_pouch_query_index_hex_token_valid(value_hex) ||
      !lc_pouch_query_index_hex_token_valid(key_hex)) {
    lc_free_with_allocator(reader->allocator, line);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term has invalid hex fields", NULL,
                        NULL, NULL);
  }
  rc = LC_OK;
  matched = 0;
  field_cmp = strcmp(field_hex, reader->field_hex);
  if ((!reader->prefix_match && !reader->contains_match &&
       !reader->range_match && !reader->date_match && !reader->ignore_case &&
       field_cmp > 0) ||
      ((reader->range_match || reader->date_match) && field_cmp > 0)) {
    reader->stop = 1;
  } else if (field_cmp == 0) {
    if (!reader->prefix_match && !reader->contains_match &&
        !reader->range_match && !reader->date_match && !reader->ignore_case) {
      value_cmp = strcmp(value_hex, reader->value_hex);
      if (value_cmp > 0) {
        reader->stop = 1;
      } else if (value_cmp == 0) {
        rc = lc_pouch_query_index_term_reader_matches_value(reader, value_hex,
                                                            &matched, error);
      }
    } else {
      rc = lc_pouch_query_index_term_reader_matches_value(reader, value_hex,
                                                          &matched, error);
    }
  }
  if (rc == LC_OK && matched) {
    key = lc_pouch_query_index_hex_decode(reader->allocator, key_hex, error);
    if (key == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    } else {
      key_view.key = key;
      rc = reader->visit(&key_view, reader->context, error);
    }
  }
  lc_free_with_allocator(reader->allocator, key);
  lc_free_with_allocator(reader->allocator, line);
  return rc;
}

static int lc_pouch_query_index_parse_and_visit_presence(
    const char *line_bytes, lc_pouch_query_index_presence_reader *reader,
    lc_error *error) {
  lc_pouch_query_index_key_view key_view;
  char *line;
  char *cursor;
  char *field_hex;
  char *key_hex;
  char *key;
  size_t line_len;
  int rc;

  if (line_bytes == NULL || reader == NULL || reader->visit == NULL) {
    return LC_OK;
  }
  line_len = strlen(line_bytes);
  line = (char *)lc_alloc_with_allocator(reader->allocator, line_len + 1U);
  if (line == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index presence parser",
                        NULL, NULL, NULL);
  }
  memcpy(line, line_bytes, line_len + 1U);
  if (strncmp(line, "present ", sizeof("present ") - 1U) != 0) {
    lc_free_with_allocator(reader->allocator, line);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index presence has invalid prefix", NULL,
                        NULL, NULL);
  }
  cursor = line + sizeof("present ") - 1U;
  key = NULL;
  field_hex = lc_pouch_query_index_next_token(&cursor, 0);
  key_hex = lc_pouch_query_index_next_token(&cursor, 1);
  if (field_hex == NULL || key_hex == NULL || strcmp(field_hex, "-") == 0 ||
      strcmp(key_hex, "-") == 0 ||
      !lc_pouch_query_index_hex_token_valid(field_hex) ||
      !lc_pouch_query_index_hex_token_valid(key_hex)) {
    lc_free_with_allocator(reader->allocator, line);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index presence has invalid hex fields",
                        NULL, NULL, NULL);
  }
  rc = LC_OK;
  if (strcmp(field_hex, reader->field_hex) == 0) {
    key = lc_pouch_query_index_hex_decode(reader->allocator, key_hex, error);
    if (key == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    } else {
      key_view.key = key;
      rc = reader->visit(&key_view, reader->context, error);
    }
  }
  lc_free_with_allocator(reader->allocator, key);
  lc_free_with_allocator(reader->allocator, line);
  return rc;
}

static char *lc_pouch_query_index_path(lc_pouch *pouch,
                                       const char *namespace_name,
                                       lc_error *error) {
  char *namespace_path;
  char *index_path;
  char *sidecar_path;

  namespace_path =
      lc_pouch_namespace_path(&pouch->allocator, pouch->root_path,
                              namespace_name);
  if (namespace_path == NULL) {
    return NULL;
  }
  index_path = lc_pouch_path_join(&pouch->allocator, namespace_path, "index");
  lc_free_with_allocator(&pouch->allocator, namespace_path);
  if (index_path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index directory path", NULL,
                 NULL, NULL);
    return NULL;
  }
  sidecar_path =
      lc_pouch_path_join(&pouch->allocator, index_path,
                         LC_POUCH_QUERY_INDEX_LEAF);
  lc_free_with_allocator(&pouch->allocator, index_path);
  if (sidecar_path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index sidecar path", NULL,
                 NULL, NULL);
  }
  return sidecar_path;
}

static int lc_pouch_query_index_parse_header_line(const char *line,
                                                  const char *name,
                                                  unsigned long *out) {
  const char *value;
  char *end;
  unsigned long parsed;
  size_t name_len;

  if (line == NULL || name == NULL || out == NULL) {
    return 0;
  }
  name_len = strlen(name);
  if (strncmp(line, name, name_len) != 0 || line[name_len] != '=') {
    return 0;
  }
  value = line + name_len + 1U;
  if (*value == '\0' || *value == '\n') {
    return 0;
  }
  errno = 0;
  parsed = strtoul(value, &end, 10);
  if (errno != 0 || end == value || (*end != '\n' && *end != '\0')) {
    return 0;
  }
  *out = parsed;
  return 1;
}

static int lc_pouch_query_index_read_rows(
    FILE *fp, unsigned long row_count, unsigned long row_hash,
    lc_pouch_query_index_row_reader *reader, int *valid, lc_error *error) {
  static const char prefix[] = "row ";
  lc_pouch_query_index_text line;
  unsigned long computed_hash;
  unsigned long actual_rows;
  int got_line;
  int rc;

  memset(&line, 0, sizeof(line));
  line.allocator = reader != NULL ? reader->allocator : NULL;
  computed_hash = lc_pouch_query_index_hash_init();
  actual_rows = 0UL;
  rc = LC_OK;
  while (actual_rows < row_count) {
    rc = lc_pouch_query_index_read_line(fp, &line, &got_line, error);
    if (rc != LC_OK || !got_line) {
      if (rc == LC_OK) {
        *valid = 0;
      }
      break;
    }
    lc_pouch_query_index_hash_bytes(&computed_hash, line.bytes, line.length);
    if (line.length <= sizeof(prefix) - 1U ||
        line.bytes[line.length - 1U] != '\n' ||
        strncmp(line.bytes, prefix, sizeof(prefix) - 1U) != 0) {
      *valid = 0;
      break;
    }
    if (reader != NULL && reader->visit != NULL) {
      rc = lc_pouch_query_index_parse_and_visit_row(line.bytes, reader, error);
      if (rc != LC_OK) {
        *valid = 0;
        break;
      }
    }
    ++actual_rows;
  }
  if (rc == LC_OK &&
      (actual_rows != row_count || computed_hash != row_hash)) {
    *valid = 0;
  } else if (rc == LC_OK) {
    *valid = 1;
  }
  lc_free_with_allocator(line.allocator, line.bytes);
  return rc;
}

static int lc_pouch_query_index_read_terms(
    FILE *fp, unsigned long term_count, unsigned long term_hash,
    lc_pouch_query_index_term_reader *reader, int *valid, lc_error *error) {
  static const char prefix[] = "term ";
  lc_pouch_query_index_text line;
  unsigned long computed_hash;
  unsigned long actual_terms;
  int got_line;
  int allow_sorted_stop;
  int rc;

  memset(&line, 0, sizeof(line));
  line.allocator = reader != NULL ? reader->allocator : NULL;
  computed_hash = lc_pouch_query_index_hash_init();
  actual_terms = 0UL;
  allow_sorted_stop =
      reader != NULL && reader->visit != NULL && !reader->prefix_match &&
      !reader->contains_match && !reader->ignore_case;
  if (allow_sorted_stop) {
    reader->stop = 0;
  }
  rc = LC_OK;
  while (actual_terms < term_count) {
    rc = lc_pouch_query_index_read_line(fp, &line, &got_line, error);
    if (rc != LC_OK || !got_line) {
      if (rc == LC_OK) {
        *valid = 0;
      }
      break;
    }
    lc_pouch_query_index_hash_bytes(&computed_hash, line.bytes, line.length);
    if (line.length <= sizeof(prefix) - 1U ||
        line.bytes[line.length - 1U] != '\n' ||
        strncmp(line.bytes, prefix, sizeof(prefix) - 1U) != 0) {
      *valid = 0;
      break;
    }
    if (reader != NULL && reader->visit != NULL) {
      rc = lc_pouch_query_index_parse_and_visit_term(line.bytes, reader, error);
      if (rc != LC_OK) {
        *valid = 0;
        break;
      }
    }
    ++actual_terms;
    if (allow_sorted_stop && reader->stop) {
      break;
    }
  }
  if (rc == LC_OK && allow_sorted_stop && reader->stop) {
    *valid = 1;
  } else if (rc == LC_OK &&
      (actual_terms != term_count || computed_hash != term_hash)) {
    *valid = 0;
  } else if (rc == LC_OK) {
    *valid = 1;
  }
  lc_free_with_allocator(line.allocator, line.bytes);
  return rc;
}

static int lc_pouch_query_index_read_presences(
    FILE *fp, unsigned long presence_count, unsigned long presence_hash,
    lc_pouch_query_index_presence_reader *reader, int *valid,
    lc_error *error) {
  static const char prefix[] = "present ";
  lc_pouch_query_index_text line;
  unsigned long computed_hash;
  unsigned long actual_presences;
  int got_line;
  int rc;

  memset(&line, 0, sizeof(line));
  line.allocator = reader != NULL ? reader->allocator : NULL;
  computed_hash = lc_pouch_query_index_hash_init();
  actual_presences = 0UL;
  rc = LC_OK;
  while (actual_presences < presence_count) {
    rc = lc_pouch_query_index_read_line(fp, &line, &got_line, error);
    if (rc != LC_OK || !got_line) {
      if (rc == LC_OK) {
        *valid = 0;
      }
      break;
    }
    lc_pouch_query_index_hash_bytes(&computed_hash, line.bytes, line.length);
    if (line.length <= sizeof(prefix) - 1U ||
        line.bytes[line.length - 1U] != '\n' ||
        strncmp(line.bytes, prefix, sizeof(prefix) - 1U) != 0) {
      *valid = 0;
      break;
    }
    if (reader != NULL && reader->visit != NULL) {
      rc = lc_pouch_query_index_parse_and_visit_presence(line.bytes, reader,
                                                         error);
      if (rc != LC_OK) {
        *valid = 0;
        break;
      }
    }
    ++actual_presences;
  }
  if (rc == LC_OK &&
      (actual_presences != presence_count ||
       computed_hash != presence_hash)) {
    *valid = 0;
  } else if (rc == LC_OK) {
    *valid = 1;
  }
  lc_free_with_allocator(line.allocator, line.bytes);
  return rc;
}

static const lc_allocator *lc_pouch_query_index_reader_allocator(
    lc_pouch_query_index_row_reader *reader,
    lc_pouch_query_index_term_reader *term_reader,
    lc_pouch_query_index_presence_reader *presence_reader) {
  if (reader != NULL) {
    return reader->allocator;
  }
  if (term_reader != NULL) {
    return term_reader->allocator;
  }
  if (presence_reader != NULL) {
    return presence_reader->allocator;
  }
  return NULL;
}

static int lc_pouch_query_index_skip_lines(
    FILE *fp, unsigned long count, const lc_allocator *allocator, int *valid,
    lc_error *error) {
  lc_pouch_query_index_text line;
  unsigned long actual;
  int got_line;
  int rc;

  if (valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index skip requires valid output", NULL,
                        NULL, NULL);
  }
  memset(&line, 0, sizeof(line));
  line.allocator = allocator;
  actual = 0UL;
  rc = LC_OK;
  *valid = 0;
  while (actual < count) {
    rc = lc_pouch_query_index_read_line(fp, &line, &got_line, error);
    if (rc != LC_OK || !got_line) {
      break;
    }
    ++actual;
  }
  if (rc == LC_OK && actual == count) {
    *valid = 1;
  }
  lc_free_with_allocator(allocator, line.bytes);
  return rc;
}

static int lc_pouch_query_index_read_with_reader(
    const char *path, lc_pouch_query_index_read_result *out,
    lc_pouch_query_index_row_reader *reader,
    lc_pouch_query_index_term_reader *term_reader,
    lc_pouch_query_index_presence_reader *presence_reader, lc_error *error) {
  char format[64];
  char line[256];
  unsigned long version;
  unsigned long parsed_seq;
  unsigned long row_count;
  unsigned long row_hash;
  unsigned long term_count;
  unsigned long term_hash;
  unsigned long term_index_complete;
  unsigned long presence_count;
  unsigned long presence_hash;
  unsigned long presence_index_complete;
  FILE *fp;
  int matched;
  int row_valid;
  int term_valid;
  int presence_valid;
  int full_read;
  int want_rows;
  int want_terms;
  int want_presences;
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index read requires an output", NULL, NULL,
                        NULL);
  }
  memset(out, 0, sizeof(*out));
  fp = fopen(path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch query-index sidecar",
                        strerror(errno), NULL, NULL);
  }
  memset(format, 0, sizeof(format));
  version = 0UL;
  parsed_seq = 0UL;
  row_count = 0UL;
  row_hash = 0UL;
  term_count = 0UL;
  term_hash = 0UL;
  term_index_complete = 0UL;
  presence_count = 0UL;
  presence_hash = 0UL;
  presence_index_complete = 0UL;
  matched = 0;
  if (fgets(line, sizeof(line), fp) != NULL &&
      sscanf(line, "format=%63s\n", format) == 1) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "version", &version)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "state_index_seq",
                                             &parsed_seq)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "row_count", &row_count)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "summary_hash",
                                             &row_hash)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_count",
                                             &term_count)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_hash", &term_hash)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_index_complete",
                                             &term_index_complete) &&
      term_index_complete <= 1UL) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "presence_count",
                                             &presence_count)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "presence_hash",
                                             &presence_hash)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "presence_index_complete",
                                             &presence_index_complete) &&
      presence_index_complete <= 1UL) {
    ++matched;
  }
  out->present = 1;
  if (matched == 11 &&
      strcmp(format, LC_POUCH_QUERY_INDEX_FORMAT) == 0 &&
      version == LC_POUCH_QUERY_INDEX_VERSION) {
    out->index_seq = parsed_seq;
    out->row_count = row_count;
    out->row_hash = row_hash;
    out->term_count = term_count;
    out->term_hash = term_hash;
    out->presence_count = presence_count;
    out->presence_hash = presence_hash;
    out->term_index_complete = term_index_complete != 0UL;
    out->presence_index_complete = presence_index_complete != 0UL;
    full_read =
        reader == NULL && term_reader == NULL && presence_reader == NULL;
    want_rows = full_read || reader != NULL;
    want_terms = full_read || term_reader != NULL;
    want_presences = full_read || presence_reader != NULL;
    row_valid = !want_rows;
    term_valid = !want_terms;
    presence_valid = !want_presences;
    if (want_rows) {
      rc = lc_pouch_query_index_read_rows(fp, row_count, row_hash, reader,
                                          &row_valid, error);
    } else {
      rc = lc_pouch_query_index_skip_lines(
          fp, row_count,
          lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                presence_reader),
          &row_valid, error);
    }
    if (rc == LC_OK && want_terms) {
      rc = lc_pouch_query_index_read_terms(fp, term_count, term_hash,
                                           term_reader, &term_valid, error);
    } else if (rc == LC_OK && want_presences) {
      rc = lc_pouch_query_index_skip_lines(
          fp, term_count,
          lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                presence_reader),
          &term_valid, error);
    }
    if (rc == LC_OK && want_presences) {
      rc = lc_pouch_query_index_read_presences(
          fp, presence_count, presence_hash, presence_reader, &presence_valid,
          error);
    }
    if (rc == LC_OK && full_read) {
      int got_extra;
      lc_pouch_query_index_text extra;

      memset(&extra, 0, sizeof(extra));
      extra.allocator = lc_pouch_query_index_reader_allocator(
          reader, term_reader, presence_reader);
      rc = lc_pouch_query_index_read_line(fp, &extra, &got_extra, error);
      if (rc == LC_OK && got_extra) {
        presence_valid = 0;
      }
      lc_free_with_allocator(extra.allocator, extra.bytes);
    }
    if (rc == LC_OK) {
      out->valid = row_valid && term_valid && presence_valid;
    }
  } else {
    rc = LC_OK;
  }
  if (fclose(fp) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch query-index sidecar",
                        strerror(errno), NULL, NULL);
  }
  return rc;
}

static int lc_pouch_query_index_read(
    const char *path, lc_pouch_query_index_read_result *out, lc_error *error) {
  return lc_pouch_query_index_read_with_reader(path, out, NULL, NULL, NULL,
                                               error);
}

static int lc_pouch_query_index_read_header(
    const char *path, lc_pouch_query_index_read_result *out, lc_error *error) {
  char format[64];
  char line[256];
  unsigned long version;
  unsigned long parsed_seq;
  unsigned long row_count;
  unsigned long row_hash;
  unsigned long term_count;
  unsigned long term_hash;
  unsigned long term_index_complete;
  unsigned long presence_count;
  unsigned long presence_hash;
  unsigned long presence_index_complete;
  FILE *fp;
  int matched;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index header read requires an output",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  fp = fopen(path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch query-index sidecar",
                        strerror(errno), NULL, NULL);
  }
  memset(format, 0, sizeof(format));
  version = 0UL;
  parsed_seq = 0UL;
  row_count = 0UL;
  row_hash = 0UL;
  term_count = 0UL;
  term_hash = 0UL;
  term_index_complete = 0UL;
  presence_count = 0UL;
  presence_hash = 0UL;
  presence_index_complete = 0UL;
  matched = 0;
  if (fgets(line, sizeof(line), fp) != NULL &&
      sscanf(line, "format=%63s\n", format) == 1) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "version", &version)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "state_index_seq",
                                             &parsed_seq)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "row_count", &row_count)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "summary_hash",
                                             &row_hash)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_count",
                                             &term_count)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_hash", &term_hash)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_index_complete",
                                             &term_index_complete) &&
      term_index_complete <= 1UL) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "presence_count",
                                             &presence_count)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "presence_hash",
                                             &presence_hash)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "presence_index_complete",
                                             &presence_index_complete) &&
      presence_index_complete <= 1UL) {
    ++matched;
  }
  out->present = 1;
  if (matched == 11 &&
      strcmp(format, LC_POUCH_QUERY_INDEX_FORMAT) == 0 &&
      version == LC_POUCH_QUERY_INDEX_VERSION) {
    out->index_seq = parsed_seq;
    out->row_count = row_count;
    out->row_hash = row_hash;
    out->term_count = term_count;
    out->term_hash = term_hash;
    out->presence_count = presence_count;
    out->presence_hash = presence_hash;
    out->term_index_complete = term_index_complete != 0UL;
    out->presence_index_complete = presence_index_complete != 0UL;
    out->valid = 1;
  }
  if (fclose(fp) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch query-index sidecar",
                        strerror(errno), NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_query_index_build_text(
    unsigned long index_seq, lc_pouch_query_index_summary *summary,
    lc_pouch_query_index_text *out, unsigned long *row_hash_out,
    unsigned long *term_count_out, unsigned long *term_hash_out,
    unsigned long *presence_count_out, unsigned long *presence_hash_out,
    lc_error *error) {
  lc_pouch_query_index_text header;
  lc_pouch_query_index_text rows;
  lc_pouch_query_index_text terms;
  lc_pouch_query_index_text presences;
  char line[256];
  unsigned long row_hash;
  unsigned long term_hash;
  unsigned long term_line_count;
  unsigned long presence_hash;
  unsigned long presence_line_count;
  size_t index;
  int written;
  int rc;

  if (summary == NULL || out == NULL || row_hash_out == NULL ||
      term_count_out == NULL || term_hash_out == NULL ||
      presence_count_out == NULL || presence_hash_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index build requires summary outputs",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  out->allocator = summary->allocator;
  *row_hash_out = lc_pouch_query_index_hash_init();
  *term_count_out = 0UL;
  *term_hash_out = lc_pouch_query_index_hash_init();
  *presence_count_out = 0UL;
  *presence_hash_out = lc_pouch_query_index_hash_init();
  if (summary->count > 1U) {
    qsort(summary->rows, summary->count, sizeof(summary->rows[0]),
          lc_pouch_query_index_row_compare);
  }
  if (summary->term_count > 1U) {
    qsort(summary->terms, summary->term_count, sizeof(summary->terms[0]),
          lc_pouch_query_index_term_compare);
  }
  if (summary->presence_count > 1U) {
    qsort(summary->presences, summary->presence_count,
          sizeof(summary->presences[0]), lc_pouch_query_index_presence_compare);
  }
  memset(&header, 0, sizeof(header));
  memset(&rows, 0, sizeof(rows));
  memset(&terms, 0, sizeof(terms));
  memset(&presences, 0, sizeof(presences));
  header.allocator = summary->allocator;
  rows.allocator = summary->allocator;
  terms.allocator = summary->allocator;
  presences.allocator = summary->allocator;
  row_hash = lc_pouch_query_index_hash_init();
  term_hash = lc_pouch_query_index_hash_init();
  term_line_count = 0UL;
  presence_hash = lc_pouch_query_index_hash_init();
  presence_line_count = 0UL;
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < summary->count; ++index) {
    lc_pouch_query_index_row *row;

    row = &summary->rows[index];
    written = snprintf(line, sizeof(line), "row %lu %lu %d %d ",
                       row->version, row->bytes,
                       row->has_query_hidden ? 1 : 0,
                       row->query_hidden ? 1 : 0);
    if (written < 0 || (size_t)written >= sizeof(line)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index row exceeds local limit", NULL,
                        NULL, NULL);
      break;
    }
    rc = lc_pouch_query_index_text_append(&rows, line, (size_t)written,
                                          &row_hash, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(&rows, row->key_hex,
                                                 &row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(&rows, " ", &row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(
          &rows, row->content_type_hex, &row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(&rows, " ", &row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(&rows, row->etag_hex,
                                                 &row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(&rows, "\n", &row_hash,
                                                 error);
    }
  }
  for (index = 0U; rc == LC_OK && index < summary->term_count; ++index) {
    lc_pouch_query_index_term *term;

    term = &summary->terms[index];
    if (index > 0U &&
        lc_pouch_query_index_term_equal(&summary->terms[index - 1U], term)) {
      continue;
    }
    rc = lc_pouch_query_index_text_append_cstr(&terms, "term ", &term_hash,
                                               error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(&terms, term->field_hex,
                                                 &term_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(&terms, " ", &term_hash,
                                                 error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(&terms, term->value_hex,
                                                 &term_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(&terms, " ", &term_hash,
                                                 error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(&terms, term->key_hex,
                                                 &term_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(&terms, "\n", &term_hash,
                                                 error);
    }
    if (rc == LC_OK) {
      ++term_line_count;
    }
  }
  for (index = 0U; rc == LC_OK && index < summary->presence_count; ++index) {
    lc_pouch_query_index_presence *presence;

    presence = &summary->presences[index];
    if (index > 0U && lc_pouch_query_index_presence_equal(
                          &summary->presences[index - 1U], presence)) {
      continue;
    }
    rc = lc_pouch_query_index_text_append_cstr(&presences, "present ",
                                               &presence_hash, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(
          &presences, presence->field_hex, &presence_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(&presences, " ",
                                                 &presence_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(
          &presences, presence->key_hex, &presence_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(&presences, "\n",
                                                 &presence_hash, error);
    }
    if (rc == LC_OK) {
      ++presence_line_count;
    }
  }
  if (rc == LC_OK) {
    written = snprintf(line, sizeof(line),
                       "format=%s\nversion=%lu\nstate_index_seq=%lu\n"
                       "row_count=%lu\nsummary_hash=%lu\n"
                       "term_count=%lu\nterm_hash=%lu\n"
                       "term_index_complete=%d\n"
                       "presence_count=%lu\npresence_hash=%lu\n"
                       "presence_index_complete=%d\n",
                       LC_POUCH_QUERY_INDEX_FORMAT,
                       LC_POUCH_QUERY_INDEX_VERSION, index_seq,
                       (unsigned long)summary->count, row_hash,
                       term_line_count, term_hash,
                       summary->term_index_complete ? 1 : 0,
                       presence_line_count, presence_hash,
                       summary->presence_index_complete ? 1 : 0);
    if (written < 0 || (size_t)written >= sizeof(line)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index header exceeds local limit", NULL,
                        NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append(&header, line, (size_t)written, NULL,
                                          error);
  }
  if (rc == LC_OK && rows.length > 0U) {
    rc = lc_pouch_query_index_text_append(&header, rows.bytes, rows.length,
                                          NULL, error);
  }
  if (rc == LC_OK && terms.length > 0U) {
    rc = lc_pouch_query_index_text_append(&header, terms.bytes, terms.length,
                                          NULL, error);
  }
  if (rc == LC_OK && presences.length > 0U) {
    rc = lc_pouch_query_index_text_append(&header, presences.bytes,
                                          presences.length, NULL, error);
  }
  if (rc == LC_OK) {
    *out = header;
    memset(&header, 0, sizeof(header));
    *row_hash_out = row_hash;
    *term_count_out = term_line_count;
    *term_hash_out = term_hash;
    *presence_count_out = presence_line_count;
    *presence_hash_out = presence_hash;
  }
  lc_free_with_allocator(summary->allocator, header.bytes);
  lc_free_with_allocator(summary->allocator, rows.bytes);
  lc_free_with_allocator(summary->allocator, terms.bytes);
  lc_free_with_allocator(summary->allocator, presences.bytes);
  return rc;
}

static int lc_pouch_query_index_write_text(const char *path,
                                           lc_pouch_query_index_text *text,
                                           lc_error *error) {
  return lc_pouch_path_write_text_file(path, text != NULL && text->bytes != NULL
                                                 ? text->bytes
                                                 : "",
                                       error);
}

int lc_pouch_query_index_flush(lc_pouch *pouch, const char *namespace_name,
                               unsigned long state_index_seq,
                               lc_pouch_query_index_flush_result *out,
                               lc_error *error) {
  lc_pouch_query_index_read_result sidecar;
  lc_pouch_query_index_summary summary;
  lc_pouch_query_index_text text;
  char *sidecar_path;
  unsigned long expected_hash;
  unsigned long expected_term_count;
  unsigned long expected_term_hash;
  unsigned long expected_presence_count;
  unsigned long expected_presence_hash;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_query_index_flush requires pouch, "
                        "namespace, and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  memset(&summary, 0, sizeof(summary));
  memset(&text, 0, sizeof(text));
  summary.allocator = &pouch->allocator;
  summary.pouch = pouch;
  summary.namespace_name = namespace_name;
  summary.term_index_complete = 1;
  summary.presence_index_complete = 1;
  text.allocator = &pouch->allocator;
  sidecar_path = lc_pouch_query_index_path(pouch, namespace_name, error);
  if (sidecar_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_query_index_read(sidecar_path, &sidecar, error);
  if (rc == LC_OK && sidecar.present && sidecar.valid &&
      sidecar.index_seq == state_index_seq) {
    out->index_seq = state_index_seq;
    lc_free_with_allocator(&pouch->allocator, sidecar_path);
    return LC_OK;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_visit(pouch, namespace_name,
                              lc_pouch_query_index_summary_visit, &summary,
                              error);
  }
  expected_hash = lc_pouch_query_index_hash_init();
  expected_term_count = 0UL;
  expected_term_hash = lc_pouch_query_index_hash_init();
  expected_presence_count = 0UL;
  expected_presence_hash = lc_pouch_query_index_hash_init();
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_build_text(state_index_seq, &summary, &text,
                                         &expected_hash, &expected_term_count,
                                         &expected_term_hash,
                                         &expected_presence_count,
                                         &expected_presence_hash, error);
  }
  if (rc == LC_OK &&
      (!sidecar.present || !sidecar.valid ||
       sidecar.index_seq != state_index_seq ||
       sidecar.row_count != (unsigned long)summary.count ||
       sidecar.row_hash != expected_hash ||
       sidecar.term_count != expected_term_count ||
       sidecar.term_hash != expected_term_hash ||
       sidecar.term_index_complete != summary.term_index_complete ||
       sidecar.presence_count != expected_presence_count ||
       sidecar.presence_hash != expected_presence_hash ||
       sidecar.presence_index_complete !=
           summary.presence_index_complete)) {
    rc = lc_pouch_query_index_write_text(sidecar_path, &text, error);
    out->repaired = 1;
  }
  if (rc == LC_OK) {
    out->index_seq = state_index_seq;
  }
  lc_free_with_allocator(&pouch->allocator, text.bytes);
  lc_pouch_query_index_summary_cleanup(&summary);
  lc_free_with_allocator(&pouch->allocator, sidecar_path);
  return rc;
}

int lc_pouch_query_index_ensure_current(
    lc_pouch *pouch, const char *namespace_name, unsigned long state_index_seq,
    lc_pouch_query_index_flush_result *out, lc_error *error) {
  lc_pouch_query_index_read_result sidecar;
  char *sidecar_path;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_query_index_ensure_current requires pouch, "
                        "namespace, and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  sidecar_path = lc_pouch_query_index_path(pouch, namespace_name, error);
  if (sidecar_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_query_index_read_header(sidecar_path, &sidecar, error);
  lc_free_with_allocator(&pouch->allocator, sidecar_path);
  if (rc != LC_OK) {
    return rc;
  }
  if (sidecar.present && sidecar.valid &&
      sidecar.index_seq == state_index_seq) {
    out->index_seq = state_index_seq;
    return LC_OK;
  }
  return lc_pouch_query_index_flush(pouch, namespace_name, state_index_seq,
                                    out, error);
}

int lc_pouch_query_index_visit(lc_pouch *pouch, const char *namespace_name,
                               lc_pouch_query_index_row_visit_fn visit,
                               void *context, unsigned long *index_seq,
                               lc_error *error) {
  lc_pouch_query_index_read_result sidecar;
  lc_pouch_query_index_row_reader reader;
  char *sidecar_path;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_query_index_visit requires pouch, "
                        "namespace, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  *index_seq = 0UL;
  sidecar_path = lc_pouch_query_index_path(pouch, namespace_name, error);
  if (sidecar_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  memset(&reader, 0, sizeof(reader));
  reader.allocator = &pouch->allocator;
  reader.visit = visit;
  reader.context = context;
  rc = lc_pouch_query_index_read_with_reader(sidecar_path, &sidecar, &reader,
                                             NULL, NULL, error);
  if (rc == LC_OK && (!sidecar.present || !sidecar.valid)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index sidecar is not readable", NULL, NULL,
                      "pouch-redesign");
  }
  if (rc == LC_OK) {
    *index_seq = sidecar.index_seq;
  }
  lc_free_with_allocator(&pouch->allocator, sidecar_path);
  return rc;
}

static int lc_pouch_query_index_visit_term_match(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *value, int prefix_match, int contains_match, int ignore_case,
    const lc_pouch_query_index_range_bounds *range_bounds,
    const lc_pouch_query_index_date_bounds *date_bounds,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error) {
  lc_pouch_query_index_read_result sidecar;
  lc_pouch_query_index_term_reader reader;
  char *sidecar_path;
  char *field_hex;
  char *value_hex;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      field == NULL || field[0] == '\0' ||
      (value == NULL && range_bounds == NULL && date_bounds == NULL) ||
      visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term lookup requires pouch, "
                        "namespace, field, value, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  *index_seq = 0UL;
  sidecar_path = lc_pouch_query_index_path(pouch, namespace_name, error);
  if (sidecar_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  field_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, field);
  value_hex = range_bounds == NULL && date_bounds == NULL
                  ? lc_pouch_query_index_hex_encode(&pouch->allocator, value)
                  : lc_strdup_with_allocator(&pouch->allocator, "");
  if (field_hex == NULL || value_hex == NULL) {
    lc_free_with_allocator(&pouch->allocator, field_hex);
    lc_free_with_allocator(&pouch->allocator, value_hex);
    lc_free_with_allocator(&pouch->allocator, sidecar_path);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index scalar lookup",
                        NULL, NULL, NULL);
  }
  memset(&reader, 0, sizeof(reader));
  reader.allocator = &pouch->allocator;
  reader.field_hex = field_hex;
  reader.value_hex = value_hex;
  reader.value_text = value;
  reader.prefix_match = prefix_match;
  reader.contains_match = contains_match;
  reader.ignore_case = ignore_case;
  if (range_bounds != NULL) {
    reader.range_match = 1;
    reader.range_bounds = *range_bounds;
  }
  if (date_bounds != NULL) {
    reader.date_match = 1;
    reader.date_bounds = *date_bounds;
    rc = lc_pouch_index_parse_date_bounds(
        date_bounds, &reader.parsed_date_bounds, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(&pouch->allocator, field_hex);
      lc_free_with_allocator(&pouch->allocator, value_hex);
      lc_free_with_allocator(&pouch->allocator, sidecar_path);
      return rc;
    }
  }
  reader.visit = visit;
  reader.context = context;
  rc = lc_pouch_query_index_read_with_reader(sidecar_path, &sidecar, NULL,
                                             &reader, NULL, error);
  if (rc == LC_OK && (!sidecar.present || !sidecar.valid)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index sidecar is not readable", NULL, NULL,
                      "pouch-redesign");
  }
  if (rc == LC_OK && !sidecar.term_index_complete) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index scalar postings are incomplete", NULL,
                      NULL, "pouch-redesign");
  }
  if (rc == LC_OK) {
    *index_seq = sidecar.index_seq;
  }
  lc_free_with_allocator(&pouch->allocator, field_hex);
  lc_free_with_allocator(&pouch->allocator, value_hex);
  lc_free_with_allocator(&pouch->allocator, sidecar_path);
  return rc;
}

int lc_pouch_query_index_visit_scalar(lc_pouch *pouch,
                                      const char *namespace_name,
                                      const char *field,
                                      const char *value,
                                      lc_pouch_query_index_key_visit_fn visit,
                                      void *context,
                                      unsigned long *index_seq,
                                      lc_error *error) {
  return lc_pouch_query_index_visit_term_match(
      pouch, namespace_name, field, value, 0, 0, 0, NULL, NULL, visit, context,
      index_seq, error);
}

int lc_pouch_query_index_visit_prefix(lc_pouch *pouch,
                                      const char *namespace_name,
                                      const char *field,
                                      const char *prefix,
                                      int ignore_case,
                                      lc_pouch_query_index_key_visit_fn visit,
                                      void *context,
                                      unsigned long *index_seq,
                                      lc_error *error) {
  if (prefix == NULL || prefix[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index prefix lookup requires non-empty "
                        "prefix",
                        NULL, NULL, NULL);
  }
  return lc_pouch_query_index_visit_term_match(
      pouch, namespace_name, field, prefix, 1, 0, ignore_case, NULL, NULL, visit,
      context,
      index_seq, error);
}

int lc_pouch_query_index_visit_contains(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *needle, int ignore_case,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error) {
  if (needle == NULL || needle[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index contains lookup requires non-empty "
                        "needle",
                        NULL, NULL, NULL);
  }
  return lc_pouch_query_index_visit_term_match(
      pouch, namespace_name, field, needle, 0, 1, ignore_case, NULL, NULL, visit,
      context,
      index_seq, error);
}

int lc_pouch_query_index_visit_range(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const lc_pouch_query_index_range_bounds *bounds,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error) {
  if (bounds == NULL ||
      (!bounds->has_gt && !bounds->has_gte && !bounds->has_lt &&
       !bounds->has_lte)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index range lookup requires numeric "
                        "bounds",
                        NULL, NULL, NULL);
  }
  return lc_pouch_query_index_visit_term_match(
      pouch, namespace_name, field, NULL, 0, 0, 0, bounds, NULL, visit, context,
      index_seq, error);
}

int lc_pouch_query_index_visit_date(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const lc_pouch_query_index_date_bounds *bounds,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error) {
  if (bounds == NULL ||
      (!bounds->has_gt && !bounds->has_gte && !bounds->has_lt &&
       !bounds->has_lte)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index date lookup requires temporal "
                        "bounds",
                        NULL, NULL, NULL);
  }
  return lc_pouch_query_index_visit_term_match(
      pouch, namespace_name, field, NULL, 0, 0, 0, NULL, bounds, visit,
      context, index_seq, error);
}

int lc_pouch_query_index_visit_exists(lc_pouch *pouch,
                                      const char *namespace_name,
                                      const char *field,
                                      lc_pouch_query_index_key_visit_fn visit,
                                      void *context,
                                      unsigned long *index_seq,
                                      lc_error *error) {
  lc_pouch_query_index_read_result sidecar;
  lc_pouch_query_index_presence_reader reader;
  char *sidecar_path;
  char *field_hex;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      field == NULL || field[0] == '\0' || visit == NULL ||
      index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_query_index_visit_exists requires pouch, "
                        "namespace, field, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  *index_seq = 0UL;
  sidecar_path = lc_pouch_query_index_path(pouch, namespace_name, error);
  if (sidecar_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  field_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, field);
  if (field_hex == NULL) {
    lc_free_with_allocator(&pouch->allocator, sidecar_path);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index exists lookup",
                        NULL, NULL, NULL);
  }
  memset(&reader, 0, sizeof(reader));
  reader.allocator = &pouch->allocator;
  reader.field_hex = field_hex;
  reader.visit = visit;
  reader.context = context;
  rc = lc_pouch_query_index_read_with_reader(sidecar_path, &sidecar, NULL,
                                             NULL, &reader, error);
  if (rc == LC_OK && (!sidecar.present || !sidecar.valid)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index sidecar is not readable", NULL, NULL,
                      "pouch-redesign");
  }
  if (rc == LC_OK && !sidecar.presence_index_complete) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index presence postings are incomplete",
                      NULL, NULL, "pouch-redesign");
  }
  if (rc == LC_OK) {
    *index_seq = sidecar.index_seq;
  }
  lc_free_with_allocator(&pouch->allocator, field_hex);
  lc_free_with_allocator(&pouch->allocator, sidecar_path);
  return rc;
}

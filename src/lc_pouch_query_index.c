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
#define LC_POUCH_QUERY_INDEX_VERSION 10UL
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
  char value_type;
  unsigned long doc_id;
  unsigned long version;
  unsigned long bytes;
  int has_query_hidden;
  int query_hidden;
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
  unsigned long term_field_count;
  unsigned long term_value_count;
  unsigned long presence_count;
  unsigned long presence_hash;
  int term_index_complete;
  int presence_index_complete;
  int present;
  int valid;
} lc_pouch_query_index_read_result;

typedef struct lc_pouch_query_index_row_reader {
  const lc_allocator *allocator;
  unsigned long row_index;
  lc_pouch_query_index_row_visit_fn visit;
  void *context;
} lc_pouch_query_index_row_reader;

typedef struct lc_pouch_query_index_term_reader {
  const lc_allocator *allocator;
  const char *field_hex;
  const char *value_hex;
  const char *const *value_hexes;
  size_t value_hex_count;
  const struct lc_pouch_query_index_exact_term *exact_terms;
  size_t exact_term_count;
  const char *value_text;
  int prefix_match;
  int contains_match;
  int ignore_case;
  int string_values_only;
  int range_match;
  int date_match;
  int stop;
  int visit_doc_ids;
  lc_pouch_query_index_range_bounds range_bounds;
  lc_pouch_query_index_date_bounds date_bounds;
  lc_pouch_index_parsed_date_bounds parsed_date_bounds;
  char *value_scratch;
  size_t value_scratch_capacity;
  lc_pouch_query_index_key_visit_fn visit;
  void *context;
} lc_pouch_query_index_term_reader;

typedef struct lc_pouch_query_index_presence_reader {
  const lc_allocator *allocator;
  const char *field_hex;
  lc_pouch_query_index_key_visit_fn visit;
  void *context;
} lc_pouch_query_index_presence_reader;

typedef struct lc_pouch_query_index_term_range {
  unsigned long first_line;
  unsigned long line_count;
} lc_pouch_query_index_term_range;

typedef struct lc_pouch_query_index_any_merge_context {
  const lc_allocator *allocator;
  lc_pouch_index_result_row_list *lists;
  size_t list_count;
} lc_pouch_query_index_any_merge_context;

typedef struct lc_pouch_query_index_docid_key_context {
  const lc_allocator *allocator;
  lc_pouch_index_result_key_list *keys;
} lc_pouch_query_index_docid_key_context;

typedef struct lc_pouch_query_index_exact_term {
  char *field_hex;
  char *value_hex;
} lc_pouch_query_index_exact_term;

typedef struct lc_pouch_query_index_source_reader {
  lc_source *source;
  lc_error error;
} lc_pouch_query_index_source_reader;

typedef struct lc_pouch_query_index_extract_context {
  lc_pouch_query_index_summary *summary;
  const char *key_hex;
  unsigned long version;
  unsigned long bytes;
  int has_query_hidden;
  int query_hidden;
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
    char value_type, unsigned long version, unsigned long bytes,
    int has_query_hidden, int query_hidden,
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
  term->value_type = value_type;
  term->version = version;
  term->bytes = bytes;
  term->has_query_hidden = has_query_hidden;
  term->query_hidden = query_hidden;
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
      context->value_len, context->key_hex, 's', context->version,
      context->bytes, context->has_query_hidden, context->query_hidden,
      &error);
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
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  int rc;

  (void)path;
  context = (lc_pouch_query_index_extract_context *)user;
  lc_error_init(&error);
  rc = lc_pouch_query_index_term_add(
      context->summary, context->field, context->field_len, context->value,
      context->value_len, context->key_hex, 'n', context->version,
      context->bytes, context->has_query_hidden, context->query_hidden,
      &error);
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
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
                                       context->key_hex, 'b',
                                       context->version, context->bytes,
                                       context->has_query_hidden,
                                       context->query_hidden, &error);
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
                                       context->key_hex, 'z',
                                       context->version, context->bytes,
                                       context->has_query_hidden,
                                       context->query_hidden, &error);
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
    const lc_pouch_query_index_row *row, lc_error *error) {
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
  context.key_hex = row->key_hex;
  context.version = row->version;
  context.bytes = row->bytes;
  context.has_query_hidden = row->has_query_hidden;
  context.query_hidden = row->query_hidden;
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
  return lc_pouch_query_index_extract_terms(summary, entry->key, row, error);
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
  if (a->value_type != b->value_type) {
    return a->value_type < b->value_type ? -1 : 1;
  }
  return strcmp(a->key_hex, b->key_hex);
}

static int lc_pouch_query_index_exact_term_compare(const void *left,
                                                   const void *right) {
  const lc_pouch_query_index_exact_term *a;
  const lc_pouch_query_index_exact_term *b;
  int cmp;

  a = (const lc_pouch_query_index_exact_term *)left;
  b = (const lc_pouch_query_index_exact_term *)right;
  cmp = strcmp(a->field_hex, b->field_hex);
  if (cmp != 0) {
    return cmp;
  }
  return strcmp(a->value_hex, b->value_hex);
}

static int lc_pouch_query_index_cstr_ptr_compare(const void *left,
                                                 const void *right) {
  const char *const *a;
  const char *const *b;

  a = (const char *const *)left;
  b = (const char *const *)right;
  return strcmp(*a, *b);
}

static int lc_pouch_query_index_term_equal(
    const lc_pouch_query_index_term *left,
    const lc_pouch_query_index_term *right) {
  return left != NULL && right != NULL &&
         strcmp(left->field_hex, right->field_hex) == 0 &&
         strcmp(left->value_hex, right->value_hex) == 0 &&
         left->value_type == right->value_type &&
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

static int lc_pouch_query_index_text_append_ulong_line(
    lc_pouch_query_index_text *text, const char *name, unsigned long value,
    lc_error *error) {
  char line[96];
  int written;

  if (text == NULL || name == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index header line requires text and name",
                        NULL, NULL, NULL);
  }
  written = snprintf(line, sizeof(line), "%s=%lu\n", name, value);
  if (written < 0 || (size_t)written >= sizeof(line)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index header line exceeds local limit",
                        NULL, NULL, NULL);
  }
  return lc_pouch_query_index_text_append(text, line, (size_t)written, NULL,
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

static char *lc_pouch_query_index_hex_decode_scratch(
    lc_pouch_query_index_term_reader *reader, const char *token,
    lc_error *error) {
  char *next;
  size_t index;
  size_t length;
  size_t decoded_length;

  if (reader == NULL || token == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query-index scratch decode requires reader and token",
                 NULL, NULL, NULL);
    return NULL;
  }
  if (token[0] == '\0' || strcmp(token, "-") == 0) {
    decoded_length = 0U;
  } else {
    length = strlen(token);
    if ((length % 2U) != 0U) {
      lc_error_set(error, LC_ERR_INVALID, 0L,
                   "pouch query-index row has invalid hex token", NULL, NULL,
                   NULL);
      return NULL;
    }
    decoded_length = length / 2U;
  }
  if (decoded_length + 1U > reader->value_scratch_capacity) {
    next = (char *)lc_alloc_with_allocator(reader->allocator,
                                           decoded_length + 1U);
    if (next == NULL) {
      lc_error_set(error, LC_ERR_NOMEM, 0L,
                   "failed to allocate pouch query-index decoded value", NULL,
                   NULL, NULL);
      return NULL;
    }
    lc_free_with_allocator(reader->allocator, reader->value_scratch);
    reader->value_scratch = next;
    reader->value_scratch_capacity = decoded_length + 1U;
  }
  if (decoded_length == 0U) {
    reader->value_scratch[0] = '\0';
    return reader->value_scratch;
  }
  length = decoded_length * 2U;
  for (index = 0U; index < length; index += 2U) {
    int high;
    int low;

    high = lc_pouch_query_index_hex_value((unsigned char)token[index]);
    low = lc_pouch_query_index_hex_value((unsigned char)token[index + 1U]);
    if (high < 0 || low < 0) {
      lc_error_set(error, LC_ERR_INVALID, 0L,
                   "pouch query-index row has invalid hex token", NULL, NULL,
                   NULL);
      return NULL;
    }
    reader->value_scratch[index / 2U] =
        (char)(((unsigned int)high << 4) | (unsigned int)low);
  }
  reader->value_scratch[decoded_length] = '\0';
  return reader->value_scratch;
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
    row.doc_id = reader->row_index;
    row.key_hex = key_hex;
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

static int lc_pouch_query_index_parse_integer_hex_value(const char *token,
                                                        double *out) {
  double value;
  size_t index;
  size_t length;
  int negative;
  int saw_digit;

  if (token == NULL || token[0] == '\0' || out == NULL) {
    return 0;
  }
  length = strlen(token);
  if ((length % 2U) != 0U) {
    return 0;
  }
  value = 0.0;
  negative = 0;
  saw_digit = 0;
  for (index = 0U; index < length; index += 2U) {
    int high;
    int low;
    unsigned char ch;

    high = lc_pouch_query_index_hex_value((unsigned char)token[index]);
    low = lc_pouch_query_index_hex_value((unsigned char)token[index + 1U]);
    if (high < 0 || low < 0) {
      return 0;
    }
    ch = (unsigned char)(((unsigned int)high << 4) | (unsigned int)low);
    if (index == 0U && ch == '-') {
      negative = 1;
      continue;
    }
    if (ch < '0' || ch > '9') {
      return 0;
    }
    saw_digit = 1;
    value = (value * 10.0) + (double)(ch - '0');
    if (!isfinite(value)) {
      return 0;
    }
  }
  if (!saw_digit) {
    return 0;
  }
  *out = negative ? -value : value;
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

static int lc_pouch_query_index_hex_value(unsigned char value);

static unsigned char lc_pouch_query_index_ascii_lower(unsigned char ch) {
  if (ch >= 'A' && ch <= 'Z') {
    return (unsigned char)(ch - 'A' + 'a');
  }
  return ch;
}

static int lc_pouch_query_index_hex_text_has_prefix(const char *value_hex,
                                                    const char *prefix,
                                                    int ignore_case) {
  size_t index;

  if (value_hex == NULL || prefix == NULL) {
    return 0;
  }
  for (index = 0U; prefix[index] != '\0'; ++index) {
    int high;
    int low;
    unsigned char actual;
    unsigned char expected;

    if (value_hex[index * 2U] == '\0' ||
        value_hex[(index * 2U) + 1U] == '\0') {
      return 0;
    }
    high = lc_pouch_query_index_hex_value(
        (unsigned char)value_hex[index * 2U]);
    low = lc_pouch_query_index_hex_value(
        (unsigned char)value_hex[(index * 2U) + 1U]);
    if (high < 0 || low < 0) {
      return 0;
    }
    actual = (unsigned char)((high << 4) | low);
    if (actual == '\0') {
      return 0;
    }
    expected = (unsigned char)prefix[index];
    if (ignore_case) {
      actual = lc_pouch_query_index_ascii_lower(actual);
      expected = lc_pouch_query_index_ascii_lower(expected);
    }
    if (actual != expected) {
      return 0;
    }
  }
  return 1;
}

static int lc_pouch_query_index_hex_text_contains(const char *value_hex,
                                                  const char *needle,
                                                  int ignore_case) {
  size_t value_index;
  size_t needle_len;
  size_t value_hex_len;
  size_t value_len;

  if (value_hex == NULL || needle == NULL) {
    return 0;
  }
  needle_len = strlen(needle);
  if (needle_len == 0U) {
    return 1;
  }
  value_hex_len = strlen(value_hex);
  value_len = value_hex_len / 2U;
  if (value_hex_len % 2U != 0U || needle_len > value_len) {
    return 0;
  }
  for (value_index = 0U; value_index + needle_len <= value_len;
       ++value_index) {
    if (lc_pouch_query_index_hex_text_has_prefix(
            value_hex + (value_index * 2U), needle, ignore_case)) {
      return 1;
    }
  }
  return 0;
}

static int lc_pouch_query_index_hex_contains_aligned(const char *value_hex,
                                                     const char *needle_hex) {
  size_t value_len;
  size_t needle_len;
  size_t index;

  if (value_hex == NULL || needle_hex == NULL) {
    return 0;
  }
  needle_len = strlen(needle_hex);
  if (needle_len == 0U) {
    return 1;
  }
  value_len = strlen(value_hex);
  if (value_len % 2U != 0U || needle_len % 2U != 0U ||
      needle_len > value_len) {
    return 0;
  }
  for (index = 0U; index + needle_len <= value_len; index += 2U) {
    if (strncmp(value_hex + index, needle_hex, needle_len) == 0) {
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
      *matched = lc_pouch_query_index_hex_text_has_prefix(
          value_hex, reader->value_text, 1);
    } else {
      *matched = strncmp(value_hex, reader->value_hex,
                         strlen(reader->value_hex)) == 0;
    }
    return LC_OK;
  }
  if (reader->contains_match) {
    if (reader->ignore_case) {
      *matched = lc_pouch_query_index_hex_text_contains(
          value_hex, reader->value_text, 1);
    } else {
      *matched = lc_pouch_query_index_hex_contains_aligned(
          value_hex, reader->value_hex);
    }
    return LC_OK;
  }
  if (reader->range_match) {
    if (lc_pouch_query_index_parse_integer_hex_value(value_hex, &number)) {
      *matched = lc_pouch_query_index_range_contains_value(
          &reader->range_bounds, number);
    } else {
      value_text = lc_pouch_query_index_hex_decode_scratch(reader, value_hex,
                                                           error);
      if (value_text == NULL) {
        return error != NULL && error->code != LC_OK ? error->code
                                                     : LC_ERR_NOMEM;
      }
      if (lc_pouch_query_index_parse_number_value(value_text, &number)) {
        *matched = lc_pouch_query_index_range_contains_value(
            &reader->range_bounds, number);
      }
    }
    return LC_OK;
  }
  if (reader->date_match) {
    value_text = lc_pouch_query_index_hex_decode_scratch(reader, value_hex,
                                                         error);
    if (value_text == NULL) {
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_NOMEM;
    }
    if (lc_pouch_index_parse_lql_datetime(value_text, &instant)) {
      *matched = lc_pouch_index_date_contains_value(
          &reader->parsed_date_bounds, &instant);
    }
    return LC_OK;
  }
  *matched = strcmp(value_hex, reader->value_hex) == 0;
  return LC_OK;
}

static int lc_pouch_query_index_term_reader_matches_any_value(
    lc_pouch_query_index_term_reader *reader, const char *value_hex,
    size_t *value_index_out) {
  size_t low;
  size_t high;
  size_t mid;
  int cmp;

  if (reader == NULL || value_hex == NULL || reader->value_hex_count == 0U) {
    return 0;
  }
  low = 0U;
  high = reader->value_hex_count;
  while (low < high) {
    mid = low + ((high - low) / 2U);
    cmp = strcmp(value_hex, reader->value_hexes[mid]);
    if (cmp == 0) {
      if (value_index_out != NULL) {
        *value_index_out = mid;
      }
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

static int lc_pouch_query_index_term_reader_exact_pair_compare(
    const char *field_hex, const char *value_hex,
    const lc_pouch_query_index_exact_term *term) {
  int cmp;

  cmp = strcmp(field_hex, term->field_hex);
  if (cmp != 0) {
    return cmp;
  }
  return strcmp(value_hex, term->value_hex);
}

static int lc_pouch_query_index_term_reader_matches_exact_term(
    lc_pouch_query_index_term_reader *reader, const char *field_hex,
    const char *value_hex, size_t *value_index_out) {
  size_t low;
  size_t high;
  size_t mid;
  int cmp;

  if (reader == NULL || field_hex == NULL || value_hex == NULL ||
      reader->exact_term_count == 0U) {
    return 0;
  }
  low = 0U;
  high = reader->exact_term_count;
  while (low < high) {
    mid = low + ((high - low) / 2U);
    cmp = lc_pouch_query_index_term_reader_exact_pair_compare(
        field_hex, value_hex, &reader->exact_terms[mid]);
    if (cmp == 0) {
      if (value_index_out != NULL) {
        *value_index_out = mid;
      }
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

static int lc_pouch_query_index_parse_and_visit_term(
    char *line, lc_pouch_query_index_term_reader *reader,
    lc_error *error) {
  lc_pouch_query_index_key_view key_view;
  char *cursor;
  char *field_hex;
  char *value_hex;
  char *key_hex;
  char *type_token;
  char *key;
  char value_type;
  size_t value_index;
  int matched;
  int field_cmp;
  int value_cmp;
  int rc;

  if (line == NULL || reader == NULL || reader->visit == NULL) {
    return LC_OK;
  }
  if (strncmp(line, "term ", sizeof("term ") - 1U) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term has invalid prefix", NULL,
                        NULL, NULL);
  }
  cursor = line + sizeof("term ") - 1U;
  key = NULL;
  rc = LC_OK;
  memset(&key_view, 0, sizeof(key_view));
  value_index = 0U;
  if (!lc_pouch_query_index_parse_ulong_token(&cursor, &key_view.version) ||
      !lc_pouch_query_index_parse_ulong_token(&cursor, &key_view.bytes) ||
      !lc_pouch_query_index_parse_int_token(&cursor,
                                            &key_view.has_query_hidden) ||
      !lc_pouch_query_index_parse_int_token(&cursor,
                                            &key_view.query_hidden) ||
      !lc_pouch_query_index_parse_ulong_token(&cursor, &key_view.doc_id)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index term has invalid numeric fields",
                      NULL, NULL, NULL);
  }
  field_hex = NULL;
  value_hex = NULL;
  key_hex = NULL;
  type_token = NULL;
  value_type = '\0';
  if (rc == LC_OK) {
    field_hex = lc_pouch_query_index_next_token(&cursor, 0);
    value_hex = lc_pouch_query_index_next_token(&cursor, 0);
    key_hex = lc_pouch_query_index_next_token(&cursor, 0);
    type_token = lc_pouch_query_index_next_token(&cursor, 1);
    if (field_hex == NULL || value_hex == NULL || key_hex == NULL ||
        type_token == NULL || type_token[0] == '\0' ||
        type_token[1] != '\0' ||
        (type_token[0] != 's' && type_token[0] != 'n' &&
         type_token[0] != 'b' && type_token[0] != 'z') ||
        strcmp(field_hex, "-") == 0 || strcmp(key_hex, "-") == 0 ||
        !lc_pouch_query_index_hex_token_valid(field_hex) ||
        !lc_pouch_query_index_hex_token_valid(value_hex) ||
        !lc_pouch_query_index_hex_token_valid(key_hex)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term has invalid hex fields", NULL,
                        NULL, NULL);
    } else {
      value_type = type_token[0];
    }
  }
  matched = 0;
  field_cmp = 0;
  if (rc == LC_OK && reader->exact_term_count > 0U) {
    field_cmp = lc_pouch_query_index_term_reader_exact_pair_compare(
        field_hex, value_hex,
        &reader->exact_terms[reader->exact_term_count - 1U]);
    if (field_cmp > 0) {
      reader->stop = 1;
    } else {
      matched = lc_pouch_query_index_term_reader_matches_exact_term(
          reader, field_hex, value_hex, &value_index);
    }
  } else if (rc == LC_OK &&
             (field_cmp = strcmp(field_hex, reader->field_hex)) > 0) {
    reader->stop = 1;
  } else if (rc == LC_OK && field_cmp == 0) {
    if (reader->string_values_only && value_type != 's') {
      matched = 0;
    } else if (!reader->prefix_match && !reader->contains_match &&
        !reader->range_match && !reader->date_match && !reader->ignore_case) {
      if (reader->value_hex_count > 0U) {
        value_cmp = strcmp(value_hex,
                           reader->value_hexes[reader->value_hex_count - 1U]);
      } else {
        value_cmp = strcmp(value_hex, reader->value_hex);
      }
      if (value_cmp > 0) {
        reader->stop = 1;
      } else if (reader->value_hex_count > 0U) {
        matched = lc_pouch_query_index_term_reader_matches_any_value(
            reader, value_hex, &value_index);
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
    key_view.key_hex = key_hex;
    key_view.value_index = value_index;
    if (reader->visit_doc_ids) {
      rc = reader->visit(&key_view, reader->context, error);
    } else {
      key = lc_pouch_query_index_hex_decode(reader->allocator, key_hex, error);
      if (key == NULL) {
        rc = error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_NOMEM;
      } else {
        key_view.key = key;
        rc = reader->visit(&key_view, reader->context, error);
      }
    }
  }
  lc_free_with_allocator(reader->allocator, key);
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
      reader->row_index = actual_rows;
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
  allow_sorted_stop = reader != NULL && reader->visit != NULL;
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

static int lc_pouch_query_index_parse_term_field(
    char *line, lc_pouch_index_term_field *field,
    const lc_allocator *allocator, lc_error *error) {
  char *cursor;
  char *field_hex;
  char *first_token;
  char *count_token;
  char *end;
  unsigned long first_line;
  unsigned long line_count;

  if (line == NULL || field == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term field requires line and "
                        "output",
                        NULL, NULL, NULL);
  }
  if (strncmp(line, "term_field ", sizeof("term_field ") - 1U) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term field has invalid prefix",
                        NULL, NULL, NULL);
  }
  cursor = line + sizeof("term_field ") - 1U;
  field_hex = lc_pouch_query_index_next_token(&cursor, 0);
  first_token = lc_pouch_query_index_next_token(&cursor, 0);
  count_token = lc_pouch_query_index_next_token(&cursor, 1);
  if (field_hex == NULL || strcmp(field_hex, "-") == 0 ||
      !lc_pouch_query_index_hex_token_valid(field_hex) ||
      first_token == NULL || count_token == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term field has invalid fields",
                        NULL, NULL, NULL);
  }
  errno = 0;
  first_line = strtoul(first_token, &end, 10);
  if (errno != 0 || end == first_token || *end != '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term field has invalid first line",
                        NULL, NULL, NULL);
  }
  errno = 0;
  line_count = strtoul(count_token, &end, 10);
  if (errno != 0 || end == count_token || *end != '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term field has invalid line count",
                        NULL, NULL, NULL);
  }
  field->field_hex = lc_strdup_with_allocator(allocator, field_hex);
  if (field->field_hex == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index term field",
                        NULL, NULL, NULL);
  }
  field->first_line = first_line;
  field->line_count = line_count;
  return LC_OK;
}

static int lc_pouch_query_index_read_terms_slice(
    FILE *fp, unsigned long term_count, lc_pouch_query_index_term_reader *reader,
    int *valid, lc_error *error) {
  static const char prefix[] = "term ";
  lc_pouch_query_index_text line;
  unsigned long actual_terms;
  int got_line;
  int rc;

  if (valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term slice read requires valid "
                        "output",
                        NULL, NULL, NULL);
  }
  memset(&line, 0, sizeof(line));
  line.allocator = reader != NULL ? reader->allocator : NULL;
  actual_terms = 0UL;
  rc = LC_OK;
  *valid = 0;
  if (reader != NULL) {
    reader->stop = 0;
  }
  while (actual_terms < term_count) {
    rc = lc_pouch_query_index_read_line(fp, &line, &got_line, error);
    if (rc != LC_OK || !got_line) {
      break;
    }
    if (line.length <= sizeof(prefix) - 1U ||
        line.bytes[line.length - 1U] != '\n' ||
        strncmp(line.bytes, prefix, sizeof(prefix) - 1U) != 0) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term has invalid prefix", NULL,
                        NULL, NULL);
      break;
    }
    if (reader != NULL && reader->visit != NULL) {
      rc = lc_pouch_query_index_parse_and_visit_term(line.bytes, reader, error);
      if (rc != LC_OK) {
        break;
      }
    }
    ++actual_terms;
    if (reader != NULL && reader->stop) {
      break;
    }
  }
  if (rc == LC_OK && (actual_terms == term_count ||
                      (reader != NULL && reader->stop))) {
    *valid = 1;
  }
  lc_free_with_allocator(line.allocator, line.bytes);
  return rc;
}

static int lc_pouch_query_index_read_term_fields(
    FILE *fp, unsigned long term_field_count, unsigned long term_count,
    const lc_allocator *allocator, lc_pouch_index_term_field **out_fields,
    size_t *out_count, int *valid, lc_error *error) {
  lc_pouch_query_index_text line;
  lc_pouch_index_term_field *fields;
  unsigned long actual_fields;
  unsigned long previous_end;
  int got_line;
  int rc;

  if (valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term field read requires valid "
                        "output",
                        NULL, NULL, NULL);
  }
  if (out_fields != NULL) {
    *out_fields = NULL;
  }
  if (out_count != NULL) {
    *out_count = 0U;
  }
  *valid = 0;
  fields = NULL;
  if (term_field_count > 0UL && out_fields != NULL) {
    fields = (lc_pouch_index_term_field *)lc_alloc_with_allocator(
        allocator, (size_t)term_field_count * sizeof(*fields));
    if (fields == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query-index term fields",
                          NULL, NULL, NULL);
    }
    memset(fields, 0, (size_t)term_field_count * sizeof(*fields));
  }
  memset(&line, 0, sizeof(line));
  line.allocator = allocator;
  actual_fields = 0UL;
  previous_end = 0UL;
  rc = LC_OK;
  while (actual_fields < term_field_count) {
    lc_pouch_index_term_field parsed;

    memset(&parsed, 0, sizeof(parsed));
    rc = lc_pouch_query_index_read_line(fp, &line, &got_line, error);
    if (rc != LC_OK || !got_line) {
      break;
    }
    rc = lc_pouch_query_index_parse_term_field(line.bytes, &parsed, allocator,
                                               error);
    if (rc != LC_OK) {
      break;
    }
    if (parsed.line_count == 0UL || parsed.first_line < previous_end ||
        parsed.first_line > term_count ||
        parsed.line_count > term_count - parsed.first_line ||
        (actual_fields > 0UL && fields != NULL &&
         strcmp(fields[actual_fields - 1UL].field_hex, parsed.field_hex) >=
             0)) {
      lc_free_with_allocator(allocator, parsed.field_hex);
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term field range is invalid",
                        NULL, NULL, NULL);
      break;
    }
    if (fields != NULL) {
      fields[actual_fields] = parsed;
    } else {
      lc_free_with_allocator(allocator, parsed.field_hex);
    }
    previous_end = parsed.first_line + parsed.line_count;
    ++actual_fields;
  }
  if (rc == LC_OK && actual_fields == term_field_count) {
    *valid = 1;
    if (out_fields != NULL) {
      *out_fields = fields;
      fields = NULL;
    }
    if (out_count != NULL) {
      *out_count = (size_t)actual_fields;
    }
  }
  lc_pouch_index_term_fields_cleanup(allocator, fields,
                                     (size_t)term_field_count);
  lc_free_with_allocator(line.allocator, line.bytes);
  return rc;
}

static int lc_pouch_query_index_parse_term_value(
    char *line, lc_pouch_index_term_value *value,
    const lc_allocator *allocator, lc_error *error) {
  char *cursor;
  char *field_hex;
  char *value_hex;
  char *first_token;
  char *count_token;
  char *end;
  unsigned long first_line;
  unsigned long line_count;

  if (line == NULL || value == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term value requires line and "
                        "output",
                        NULL, NULL, NULL);
  }
  if (strncmp(line, "term_value ", sizeof("term_value ") - 1U) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term value has invalid prefix",
                        NULL, NULL, NULL);
  }
  cursor = line + sizeof("term_value ") - 1U;
  field_hex = lc_pouch_query_index_next_token(&cursor, 0);
  value_hex = lc_pouch_query_index_next_token(&cursor, 0);
  first_token = lc_pouch_query_index_next_token(&cursor, 0);
  count_token = lc_pouch_query_index_next_token(&cursor, 1);
  if (field_hex == NULL || value_hex == NULL || strcmp(field_hex, "-") == 0 ||
      !lc_pouch_query_index_hex_token_valid(field_hex) ||
      !lc_pouch_query_index_hex_token_valid(value_hex) ||
      first_token == NULL || count_token == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term value has invalid fields",
                        NULL, NULL, NULL);
  }
  errno = 0;
  first_line = strtoul(first_token, &end, 10);
  if (errno != 0 || end == first_token || *end != '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term value has invalid first line",
                        NULL, NULL, NULL);
  }
  errno = 0;
  line_count = strtoul(count_token, &end, 10);
  if (errno != 0 || end == count_token || *end != '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term value has invalid line count",
                        NULL, NULL, NULL);
  }
  value->field_hex = lc_strdup_with_allocator(allocator, field_hex);
  value->value_hex = lc_strdup_with_allocator(allocator, value_hex);
  if (value->field_hex == NULL || value->value_hex == NULL) {
    lc_free_with_allocator(allocator, value->field_hex);
    lc_free_with_allocator(allocator, value->value_hex);
    memset(value, 0, sizeof(*value));
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index term value",
                        NULL, NULL, NULL);
  }
  value->first_line = first_line;
  value->line_count = line_count;
  return LC_OK;
}

static int lc_pouch_query_index_read_term_values(
    FILE *fp, unsigned long term_value_count, unsigned long term_count,
    const lc_allocator *allocator, lc_pouch_index_term_value **out_values,
    size_t *out_count, int *valid, lc_error *error) {
  lc_pouch_query_index_text line;
  lc_pouch_index_term_value *values;
  unsigned long actual_values;
  unsigned long previous_end;
  int got_line;
  int rc;

  if (valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term value read requires valid "
                        "output",
                        NULL, NULL, NULL);
  }
  if (out_values != NULL) {
    *out_values = NULL;
  }
  if (out_count != NULL) {
    *out_count = 0U;
  }
  *valid = 0;
  values = NULL;
  if (term_value_count > 0UL && out_values != NULL) {
    values = (lc_pouch_index_term_value *)lc_alloc_with_allocator(
        allocator, (size_t)term_value_count * sizeof(*values));
    if (values == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query-index term values",
                          NULL, NULL, NULL);
    }
    memset(values, 0, (size_t)term_value_count * sizeof(*values));
  }
  memset(&line, 0, sizeof(line));
  line.allocator = allocator;
  actual_values = 0UL;
  previous_end = 0UL;
  rc = LC_OK;
  while (actual_values < term_value_count) {
    lc_pouch_index_term_value parsed;

    memset(&parsed, 0, sizeof(parsed));
    rc = lc_pouch_query_index_read_line(fp, &line, &got_line, error);
    if (rc != LC_OK || !got_line) {
      break;
    }
    rc = lc_pouch_query_index_parse_term_value(line.bytes, &parsed, allocator,
                                               error);
    if (rc != LC_OK) {
      break;
    }
    if (parsed.line_count == 0UL || parsed.first_line < previous_end ||
        parsed.first_line > term_count ||
        parsed.line_count > term_count - parsed.first_line ||
        (actual_values > 0UL && values != NULL &&
         (strcmp(values[actual_values - 1UL].field_hex, parsed.field_hex) >
              0 ||
          (strcmp(values[actual_values - 1UL].field_hex, parsed.field_hex) ==
               0 &&
           strcmp(values[actual_values - 1UL].value_hex, parsed.value_hex) >=
               0)))) {
      lc_free_with_allocator(allocator, parsed.field_hex);
      lc_free_with_allocator(allocator, parsed.value_hex);
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term value range is invalid",
                        NULL, NULL, NULL);
      break;
    }
    if (values != NULL) {
      values[actual_values] = parsed;
    } else {
      lc_free_with_allocator(allocator, parsed.field_hex);
      lc_free_with_allocator(allocator, parsed.value_hex);
    }
    previous_end = parsed.first_line + parsed.line_count;
    ++actual_values;
  }
  if (rc == LC_OK && actual_values == term_value_count) {
    *valid = 1;
    if (out_values != NULL) {
      *out_values = values;
      values = NULL;
    }
    if (out_count != NULL) {
      *out_count = (size_t)actual_values;
    }
  }
  lc_pouch_index_term_values_cleanup(allocator, values,
                                     (size_t)term_value_count);
  lc_free_with_allocator(line.allocator, line.bytes);
  return rc;
}

static void lc_pouch_query_index_term_ranges_cleanup(
    const lc_allocator *allocator, lc_pouch_query_index_term_range *ranges) {
  lc_free_with_allocator(allocator, ranges);
}

static int lc_pouch_query_index_term_reader_exact_ranges(
    const lc_pouch_index_term_value *values, size_t value_count,
    const lc_pouch_query_index_term_reader *reader,
    lc_pouch_query_index_term_range **out_ranges, size_t *out_count,
    const lc_allocator *allocator, lc_error *error) {
  lc_pouch_query_index_term_range *ranges;
  unsigned long first;
  unsigned long count;
  size_t index;
  size_t range_count;

  if (out_ranges == NULL || out_count == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index exact ranges require outputs",
                        NULL, NULL, NULL);
  }
  *out_ranges = NULL;
  *out_count = 0U;
  if (reader == NULL || reader->exact_term_count == 0U) {
    return LC_OK;
  }
  ranges = (lc_pouch_query_index_term_range *)lc_alloc_with_allocator(
      allocator, reader->exact_term_count * sizeof(*ranges));
  if (ranges == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index exact ranges",
                        NULL, NULL, NULL);
  }
  range_count = 0U;
  for (index = 0U; index < reader->exact_term_count; ++index) {
    if (lc_pouch_index_term_values_find(
            values, value_count, reader->exact_terms[index].field_hex,
            reader->exact_terms[index].value_hex, &first, &count)) {
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

static int lc_pouch_query_index_term_reader_range(
    const lc_pouch_index_term_field *fields, size_t field_count,
    const lc_pouch_query_index_term_reader *reader, unsigned long term_count,
    unsigned long *first_line, unsigned long *line_count) {
  unsigned long first;
  unsigned long count;
  unsigned long end;
  unsigned long min_first;
  unsigned long max_end;
  size_t index;
  int found;

  if (reader == NULL || first_line == NULL || line_count == NULL) {
    return 0;
  }
  if (reader->exact_term_count == 0U) {
    return lc_pouch_index_term_fields_find(
        fields, field_count, reader->field_hex, first_line, line_count);
  }
  found = 0;
  min_first = term_count;
  max_end = 0UL;
  for (index = 0U; index < reader->exact_term_count; ++index) {
    if (lc_pouch_index_term_fields_find(
            fields, field_count, reader->exact_terms[index].field_hex, &first,
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
  unsigned long term_field_count;
  unsigned long term_value_count;
  unsigned long presence_count;
  unsigned long presence_hash;
  unsigned long presence_index_complete;
  lc_pouch_index_term_field *term_fields;
  lc_pouch_index_term_value *term_values;
  size_t term_field_table_count;
  size_t term_value_table_count;
  FILE *fp;
  int matched;
  int row_valid;
  int term_valid;
  int term_field_valid;
  int term_value_valid;
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
  term_field_count = 0UL;
  term_value_count = 0UL;
  presence_count = 0UL;
  presence_hash = 0UL;
  presence_index_complete = 0UL;
  term_fields = NULL;
  term_values = NULL;
  term_field_table_count = 0U;
  term_value_table_count = 0U;
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
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_field_count",
                                             &term_field_count)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_value_count",
                                             &term_value_count)) {
    ++matched;
  }
  out->present = 1;
  if (matched == 13 &&
      strcmp(format, LC_POUCH_QUERY_INDEX_FORMAT) == 0 &&
      version == LC_POUCH_QUERY_INDEX_VERSION) {
    out->index_seq = parsed_seq;
    out->row_count = row_count;
    out->row_hash = row_hash;
    out->term_count = term_count;
    out->term_hash = term_hash;
    out->term_field_count = term_field_count;
    out->term_value_count = term_value_count;
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
    term_field_valid = 0;
    term_value_valid = 0;
    presence_valid = !want_presences;
    rc = lc_pouch_query_index_read_term_fields(
        fp, term_field_count, term_count,
        lc_pouch_query_index_reader_allocator(reader, term_reader,
                                              presence_reader),
        want_terms ? &term_fields : NULL,
        want_terms ? &term_field_table_count : NULL, &term_field_valid, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_read_term_values(
          fp, term_value_count, term_count,
          lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                presence_reader),
          want_terms ? &term_values : NULL,
          want_terms ? &term_value_table_count : NULL, &term_value_valid,
          error);
    }
    if (rc == LC_OK && want_terms) {
      lc_pouch_query_index_term_range *exact_ranges;
      unsigned long first_line;
      unsigned long line_count;
      unsigned long current_line;
      size_t range_index;
      size_t exact_range_count;

      exact_ranges = NULL;
      first_line = 0UL;
      line_count = 0UL;
      exact_range_count = 0U;
      if (term_reader != NULL && term_reader->exact_term_count > 0U) {
        rc = lc_pouch_query_index_term_reader_exact_ranges(
            term_values, term_value_table_count, term_reader, &exact_ranges,
            &exact_range_count,
            lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                  presence_reader),
            error);
      }
      if (rc == LC_OK && term_reader != NULL && exact_range_count > 0U) {
        current_line = 0UL;
        term_valid = 1;
        for (range_index = 0U; rc == LC_OK && range_index < exact_range_count;
             ++range_index) {
          if (exact_ranges[range_index].first_line > current_line) {
            rc = lc_pouch_query_index_skip_lines(
                fp, exact_ranges[range_index].first_line - current_line,
                lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                      presence_reader),
                &term_valid, error);
            current_line = exact_ranges[range_index].first_line;
          }
          if (rc == LC_OK && term_valid) {
            rc = lc_pouch_query_index_read_terms_slice(
                fp, exact_ranges[range_index].line_count, term_reader,
                &term_valid, error);
            current_line += exact_ranges[range_index].line_count;
          }
        }
        if (rc == LC_OK && (want_rows || want_presences) && term_valid &&
            current_line < term_count) {
          rc = lc_pouch_query_index_skip_lines(
              fp, term_count - current_line,
              lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                    presence_reader),
              &term_valid, error);
        }
      } else if (rc == LC_OK && term_reader != NULL &&
                 term_reader->exact_term_count > 0U) {
        term_valid = 1;
        if (want_rows || want_presences) {
          rc = lc_pouch_query_index_skip_lines(
              fp, term_count,
              lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                    presence_reader),
              &term_valid, error);
        }
      } else if (rc == LC_OK && term_reader != NULL &&
          lc_pouch_query_index_term_reader_range(
              term_fields, term_field_table_count, term_reader, term_count,
              &first_line, &line_count)) {
        if (first_line > 0UL) {
          rc = lc_pouch_query_index_skip_lines(
              fp, first_line,
              lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                    presence_reader),
              &term_valid, error);
        } else {
          term_valid = 1;
        }
        if (rc == LC_OK && term_valid) {
          rc = lc_pouch_query_index_read_terms_slice(
              fp, line_count, term_reader, &term_valid, error);
        }
        if (rc == LC_OK && (want_rows || want_presences) && term_valid &&
            first_line + line_count < term_count) {
          rc = lc_pouch_query_index_skip_lines(
              fp, term_count - first_line - line_count,
              lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                    presence_reader),
              &term_valid, error);
        }
      } else if (term_reader != NULL) {
        term_valid = 1;
        if (want_rows || want_presences) {
          rc = lc_pouch_query_index_skip_lines(
              fp, term_count,
              lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                    presence_reader),
              &term_valid, error);
        }
      } else {
        rc = lc_pouch_query_index_read_terms(fp, term_count, term_hash,
                                             term_reader, &term_valid, error);
      }
      lc_pouch_query_index_term_ranges_cleanup(
          lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                presence_reader),
          exact_ranges);
    } else if (rc == LC_OK && (want_rows || want_presences)) {
      rc = lc_pouch_query_index_skip_lines(
          fp, term_count,
          lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                presence_reader),
          &term_valid, error);
    }
    if (want_rows) {
      if (rc == LC_OK) {
        rc = lc_pouch_query_index_read_rows(fp, row_count, row_hash, reader,
                                            &row_valid, error);
      }
    } else if (rc == LC_OK && want_presences) {
      rc = lc_pouch_query_index_skip_lines(
          fp, row_count,
          lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                presence_reader),
          &row_valid, error);
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
      out->valid = row_valid && term_valid && term_field_valid &&
                   term_value_valid &&
                   presence_valid;
    }
  } else {
    rc = LC_OK;
  }
  lc_pouch_index_term_fields_cleanup(
      lc_pouch_query_index_reader_allocator(reader, term_reader,
                                            presence_reader),
      term_fields, term_field_table_count);
  lc_pouch_index_term_values_cleanup(
      lc_pouch_query_index_reader_allocator(reader, term_reader,
                                            presence_reader),
      term_values, term_value_table_count);
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
  unsigned long term_field_count;
  unsigned long term_value_count;
  unsigned long presence_count;
  unsigned long presence_hash;
  unsigned long presence_index_complete;
  lc_pouch_index_term_field *term_fields;
  lc_pouch_index_term_value *term_values;
  size_t term_field_table_count;
  size_t term_value_table_count;
  FILE *fp;
  int matched;
  int term_field_valid;
  int term_value_valid;
  int rc;

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
  term_field_count = 0UL;
  term_value_count = 0UL;
  presence_count = 0UL;
  presence_hash = 0UL;
  presence_index_complete = 0UL;
  term_fields = NULL;
  term_values = NULL;
  term_field_table_count = 0U;
  term_value_table_count = 0U;
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
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_field_count",
                                             &term_field_count)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_value_count",
                                             &term_value_count)) {
    ++matched;
  }
  out->present = 1;
  rc = LC_OK;
  if (matched == 13 &&
      strcmp(format, LC_POUCH_QUERY_INDEX_FORMAT) == 0 &&
      version == LC_POUCH_QUERY_INDEX_VERSION) {
    term_field_valid = 0;
    term_value_valid = 0;
    rc = lc_pouch_query_index_read_term_fields(
        fp, term_field_count, term_count, NULL, &term_fields,
        &term_field_table_count, &term_field_valid, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_read_term_values(
          fp, term_value_count, term_count, NULL, &term_values,
          &term_value_table_count, &term_value_valid, error);
    }
    out->index_seq = parsed_seq;
    out->row_count = row_count;
    out->row_hash = row_hash;
    out->term_count = term_count;
    out->term_hash = term_hash;
    out->term_field_count = term_field_count;
    out->term_value_count = term_value_count;
    out->presence_count = presence_count;
    out->presence_hash = presence_hash;
    out->term_index_complete = term_index_complete != 0UL;
    out->presence_index_complete = presence_index_complete != 0UL;
    out->valid = rc == LC_OK && term_field_valid && term_value_valid;
  }
  lc_pouch_index_term_fields_cleanup(NULL, term_fields,
                                     term_field_table_count);
  lc_pouch_index_term_values_cleanup(NULL, term_values,
                                     term_value_table_count);
  if (fclose(fp) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch query-index sidecar",
                        strerror(errno), NULL, NULL);
  }
  return rc;
}

static int lc_pouch_query_index_build_text(
    unsigned long index_seq, lc_pouch_query_index_summary *summary,
    lc_pouch_query_index_text *out, unsigned long *row_hash_out,
    unsigned long *term_count_out, unsigned long *term_hash_out,
    unsigned long *term_field_count_out, unsigned long *term_value_count_out,
    unsigned long *presence_count_out, unsigned long *presence_hash_out,
    lc_error *error) {
  lc_pouch_query_index_text header;
  lc_pouch_query_index_text rows;
  lc_pouch_query_index_text term_fields;
  lc_pouch_query_index_text term_values;
  lc_pouch_query_index_text terms;
  lc_pouch_query_index_text presences;
  lc_pouch_index_doc_table doc_table;
  char line[256];
  unsigned long row_hash;
  unsigned long term_hash;
  unsigned long term_line_count;
  unsigned long term_field_line_count;
  unsigned long term_value_line_count;
  unsigned long current_field_first;
  unsigned long current_field_count;
  unsigned long current_value_first;
  unsigned long current_value_count;
  unsigned long presence_hash;
  unsigned long presence_line_count;
  unsigned long doc_id;
  const char *current_field_hex;
  const char *current_value_field_hex;
  const char *current_value_hex;
  size_t index;
  int found;
  int written;
  int rc;

  if (summary == NULL || out == NULL || row_hash_out == NULL ||
      term_count_out == NULL || term_hash_out == NULL ||
      term_field_count_out == NULL || term_value_count_out == NULL ||
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
  *term_field_count_out = 0UL;
  *term_value_count_out = 0UL;
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
  memset(&term_fields, 0, sizeof(term_fields));
  memset(&term_values, 0, sizeof(term_values));
  memset(&terms, 0, sizeof(terms));
  memset(&presences, 0, sizeof(presences));
  memset(&doc_table, 0, sizeof(doc_table));
  header.allocator = summary->allocator;
  rows.allocator = summary->allocator;
  term_fields.allocator = summary->allocator;
  term_values.allocator = summary->allocator;
  terms.allocator = summary->allocator;
  presences.allocator = summary->allocator;
  row_hash = lc_pouch_query_index_hash_init();
  term_hash = lc_pouch_query_index_hash_init();
  term_line_count = 0UL;
  term_field_line_count = 0UL;
  term_value_line_count = 0UL;
  current_field_hex = NULL;
  current_field_first = 0UL;
  current_field_count = 0UL;
  current_value_field_hex = NULL;
  current_value_hex = NULL;
  current_value_first = 0UL;
  current_value_count = 0UL;
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
      rc = lc_pouch_index_doc_table_append_sorted_unique(
          &doc_table, row->key_hex, row->version, row->bytes,
          row->has_query_hidden, row->query_hidden, &doc_id,
          summary->allocator, error);
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
    rc = lc_pouch_index_doc_table_find_key_hex(
        &doc_table, term->key_hex, &term->doc_id, &found, error);
    if (rc != LC_OK) {
      break;
    }
    if (!found) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term references missing doc table "
                        "entry",
                        NULL, NULL, NULL);
      break;
    }
    if (current_field_hex == NULL ||
        strcmp(current_field_hex, term->field_hex) != 0) {
      if (current_field_hex != NULL) {
        rc = lc_pouch_query_index_text_append_cstr(&term_fields, "term_field ",
                                                   NULL, error);
        if (rc == LC_OK) {
          rc = lc_pouch_query_index_text_append_cstr(
              &term_fields, current_field_hex, NULL, error);
        }
        if (rc == LC_OK) {
          written = snprintf(line, sizeof(line), " %lu %lu\n",
                             current_field_first, current_field_count);
          if (written < 0 || (size_t)written >= sizeof(line)) {
            rc = lc_error_set(
                error, LC_ERR_INVALID, 0L,
                "pouch query-index term field exceeds local limit", NULL,
                NULL, NULL);
          }
        }
        if (rc == LC_OK) {
          rc = lc_pouch_query_index_text_append(
              &term_fields, line, (size_t)written, NULL, error);
        }
        if (rc == LC_OK) {
          ++term_field_line_count;
        }
      }
      current_field_hex = term->field_hex;
      current_field_first = term_line_count;
      current_field_count = 0UL;
    }
    if (current_value_field_hex == NULL ||
        strcmp(current_value_field_hex, term->field_hex) != 0 ||
        strcmp(current_value_hex, term->value_hex) != 0) {
      if (current_value_field_hex != NULL) {
        rc = lc_pouch_query_index_text_append_cstr(&term_values, "term_value ",
                                                   NULL, error);
        if (rc == LC_OK) {
          rc = lc_pouch_query_index_text_append_cstr(
              &term_values, current_value_field_hex, NULL, error);
        }
        if (rc == LC_OK) {
          rc = lc_pouch_query_index_text_append_cstr(&term_values, " ", NULL,
                                                     error);
        }
        if (rc == LC_OK) {
          rc = lc_pouch_query_index_text_append_cstr(
              &term_values, current_value_hex, NULL, error);
        }
        if (rc == LC_OK) {
          written = snprintf(line, sizeof(line), " %lu %lu\n",
                             current_value_first, current_value_count);
          if (written < 0 || (size_t)written >= sizeof(line)) {
            rc = lc_error_set(
                error, LC_ERR_INVALID, 0L,
                "pouch query-index term value exceeds local limit", NULL,
                NULL, NULL);
          }
        }
        if (rc == LC_OK) {
          rc = lc_pouch_query_index_text_append(
              &term_values, line, (size_t)written, NULL, error);
        }
        if (rc == LC_OK) {
          ++term_value_line_count;
        }
      }
      current_value_field_hex = term->field_hex;
      current_value_hex = term->value_hex;
      current_value_first = term_line_count;
      current_value_count = 0UL;
    }
    written = snprintf(line, sizeof(line), "term %lu %lu %d %d %lu ",
                       term->version, term->bytes,
                       term->has_query_hidden ? 1 : 0,
                       term->query_hidden ? 1 : 0, term->doc_id);
    if (written < 0 || (size_t)written >= sizeof(line)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term exceeds local limit", NULL,
                        NULL, NULL);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append(&terms, line, (size_t)written,
                                            &term_hash, error);
    }
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
      rc = lc_pouch_query_index_text_append_cstr(&terms, " ", &term_hash,
                                                 error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append(&terms, &term->value_type, 1U,
                                            &term_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(&terms, "\n", &term_hash,
                                                 error);
    }
    if (rc == LC_OK) {
      ++term_line_count;
      ++current_field_count;
      ++current_value_count;
    }
  }
  if (rc == LC_OK && current_value_field_hex != NULL) {
    rc = lc_pouch_query_index_text_append_cstr(&term_values, "term_value ",
                                               NULL, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(
          &term_values, current_value_field_hex, NULL, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(&term_values, " ", NULL,
                                                 error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(
          &term_values, current_value_hex, NULL, error);
    }
    if (rc == LC_OK) {
      written = snprintf(line, sizeof(line), " %lu %lu\n",
                         current_value_first, current_value_count);
      if (written < 0 || (size_t)written >= sizeof(line)) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query-index term value exceeds local limit",
                          NULL, NULL, NULL);
      }
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append(&term_values, line,
                                            (size_t)written, NULL, error);
    }
    if (rc == LC_OK) {
      ++term_value_line_count;
    }
  }
  if (rc == LC_OK && current_field_hex != NULL) {
    rc = lc_pouch_query_index_text_append_cstr(&term_fields, "term_field ",
                                               NULL, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(
          &term_fields, current_field_hex, NULL, error);
    }
    if (rc == LC_OK) {
      written = snprintf(line, sizeof(line), " %lu %lu\n",
                         current_field_first, current_field_count);
      if (written < 0 || (size_t)written >= sizeof(line)) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query-index term field exceeds local limit",
                          NULL, NULL, NULL);
      }
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append(&term_fields, line,
                                            (size_t)written, NULL, error);
    }
    if (rc == LC_OK) {
      ++term_field_line_count;
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
    rc = lc_pouch_query_index_text_append_cstr(
        &header, "format=" LC_POUCH_QUERY_INDEX_FORMAT "\n", NULL, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        &header, "version", LC_POUCH_QUERY_INDEX_VERSION, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        &header, "state_index_seq", index_seq, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        &header, "row_count", (unsigned long)summary->count, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(&header, "summary_hash",
                                                    row_hash, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(&header, "term_count",
                                                    term_line_count, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(&header, "term_hash",
                                                    term_hash, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        &header, "term_index_complete",
        summary->term_index_complete ? 1UL : 0UL, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        &header, "presence_count", presence_line_count, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        &header, "presence_hash", presence_hash, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        &header, "presence_index_complete",
        summary->presence_index_complete ? 1UL : 0UL, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        &header, "term_field_count", term_field_line_count, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        &header, "term_value_count", term_value_line_count, error);
  }
  if (rc == LC_OK && term_fields.length > 0U) {
    rc = lc_pouch_query_index_text_append(&header, term_fields.bytes,
                                          term_fields.length, NULL, error);
  }
  if (rc == LC_OK && term_values.length > 0U) {
    rc = lc_pouch_query_index_text_append(&header, term_values.bytes,
                                          term_values.length, NULL, error);
  }
  if (rc == LC_OK && terms.length > 0U) {
    rc = lc_pouch_query_index_text_append(&header, terms.bytes, terms.length,
                                          NULL, error);
  }
  if (rc == LC_OK && rows.length > 0U) {
    rc = lc_pouch_query_index_text_append(&header, rows.bytes, rows.length,
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
    *term_field_count_out = term_field_line_count;
    *term_value_count_out = term_value_line_count;
    *presence_count_out = presence_line_count;
    *presence_hash_out = presence_hash;
  }
  lc_free_with_allocator(summary->allocator, header.bytes);
  lc_free_with_allocator(summary->allocator, rows.bytes);
  lc_free_with_allocator(summary->allocator, term_fields.bytes);
  lc_free_with_allocator(summary->allocator, term_values.bytes);
  lc_free_with_allocator(summary->allocator, terms.bytes);
  lc_free_with_allocator(summary->allocator, presences.bytes);
  lc_pouch_index_doc_table_cleanup(summary->allocator, &doc_table);
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
  unsigned long expected_term_field_count;
  unsigned long expected_term_value_count;
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
  expected_term_field_count = 0UL;
  expected_term_value_count = 0UL;
  expected_presence_count = 0UL;
  expected_presence_hash = lc_pouch_query_index_hash_init();
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_build_text(state_index_seq, &summary, &text,
                                         &expected_hash, &expected_term_count,
                                         &expected_term_hash,
                                         &expected_term_field_count,
                                         &expected_term_value_count,
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
       sidecar.term_field_count != expected_term_field_count ||
       sidecar.term_value_count != expected_term_value_count ||
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
  reader.string_values_only = prefix_match || contains_match;
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
      lc_free_with_allocator(&pouch->allocator, reader.value_scratch);
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
  lc_free_with_allocator(&pouch->allocator, reader.value_scratch);
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

int lc_pouch_query_index_visit_scalar_any(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *const *values, size_t value_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error) {
  lc_pouch_query_index_read_result sidecar;
  lc_pouch_query_index_term_reader reader;
  char *sidecar_path;
  char *field_hex;
  char **value_hexes;
  size_t index;
  size_t write_index;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      field == NULL || field[0] == '\0' || values == NULL ||
      value_count == 0U || visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index multi-scalar lookup requires "
                        "pouch, namespace, field, values, visitor, and "
                        "index_seq",
                        NULL, NULL, NULL);
  }
  if (value_count == 1U) {
    return lc_pouch_query_index_visit_scalar(
        pouch, namespace_name, field, values[0], visit, context, index_seq,
        error);
  }
  memset(&reader, 0, sizeof(reader));
  *index_seq = 0UL;
  sidecar_path = lc_pouch_query_index_path(pouch, namespace_name, error);
  if (sidecar_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  field_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, field);
  value_hexes = (char **)lc_alloc_with_allocator(
      &pouch->allocator, value_count * sizeof(*value_hexes));
  if (field_hex == NULL || value_hexes == NULL) {
    lc_free_with_allocator(&pouch->allocator, field_hex);
    lc_free_with_allocator(&pouch->allocator, value_hexes);
    lc_free_with_allocator(&pouch->allocator, sidecar_path);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index multi-scalar "
                        "lookup",
                        NULL, NULL, NULL);
  }
  memset(value_hexes, 0, value_count * sizeof(*value_hexes));
  rc = LC_OK;
  for (index = 0U; index < value_count; ++index) {
    if (values[index] == NULL) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index multi-scalar lookup requires "
                        "non-null values",
                        NULL, NULL, NULL);
      break;
    }
    value_hexes[index] =
        lc_pouch_query_index_hex_encode(&pouch->allocator, values[index]);
    if (value_hexes[index] == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index multi-scalar "
                        "value",
                        NULL, NULL, NULL);
      break;
    }
  }
  if (rc == LC_OK) {
    qsort(value_hexes, value_count, sizeof(value_hexes[0]),
          lc_pouch_query_index_cstr_ptr_compare);
    write_index = 0U;
    for (index = 0U; index < value_count; ++index) {
      if (write_index > 0U &&
          strcmp(value_hexes[write_index - 1U], value_hexes[index]) == 0) {
        lc_free_with_allocator(&pouch->allocator, value_hexes[index]);
        value_hexes[index] = NULL;
        continue;
      }
      if (write_index != index) {
        value_hexes[write_index] = value_hexes[index];
        value_hexes[index] = NULL;
      }
      ++write_index;
    }
    reader.allocator = &pouch->allocator;
    reader.field_hex = field_hex;
    reader.value_hexes = (const char *const *)value_hexes;
    reader.value_hex_count = write_index;
    reader.visit = visit;
    reader.context = context;
    rc = lc_pouch_query_index_read_with_reader(sidecar_path, &sidecar, NULL,
                                               &reader, NULL, error);
    if (rc == LC_OK && (!sidecar.present || !sidecar.valid)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index sidecar is not readable", NULL,
                        NULL, "pouch-redesign");
    }
    if (rc == LC_OK && !sidecar.term_index_complete) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index scalar postings are incomplete",
                        NULL, NULL, "pouch-redesign");
    }
    if (rc == LC_OK) {
      *index_seq = sidecar.index_seq;
    }
  }
  lc_free_with_allocator(&pouch->allocator, reader.value_scratch);
  for (index = 0U; index < value_count; ++index) {
    lc_free_with_allocator(&pouch->allocator, value_hexes[index]);
  }
  lc_free_with_allocator(&pouch->allocator, value_hexes);
  lc_free_with_allocator(&pouch->allocator, field_hex);
  lc_free_with_allocator(&pouch->allocator, sidecar_path);
  return rc;
}

int lc_pouch_query_index_visit_scalar_terms(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_scalar_term *terms, size_t term_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error) {
  lc_pouch_query_index_read_result sidecar;
  lc_pouch_query_index_term_reader reader;
  lc_pouch_query_index_exact_term *exact_terms;
  char *sidecar_path;
  size_t index;
  size_t write_index;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      terms == NULL || term_count == 0U || visit == NULL ||
      index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index scalar term-set lookup requires "
                        "pouch, namespace, terms, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  if (term_count == 1U) {
    return lc_pouch_query_index_visit_scalar(
        pouch, namespace_name, terms[0].field, terms[0].value, visit, context,
        index_seq, error);
  }
  memset(&reader, 0, sizeof(reader));
  *index_seq = 0UL;
  sidecar_path = lc_pouch_query_index_path(pouch, namespace_name, error);
  if (sidecar_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  exact_terms = (lc_pouch_query_index_exact_term *)lc_alloc_with_allocator(
      &pouch->allocator, term_count * sizeof(*exact_terms));
  if (exact_terms == NULL) {
    lc_free_with_allocator(&pouch->allocator, sidecar_path);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index scalar term "
                        "lookup",
                        NULL, NULL, NULL);
  }
  memset(exact_terms, 0, term_count * sizeof(*exact_terms));
  rc = LC_OK;
  for (index = 0U; index < term_count; ++index) {
    if (terms[index].field == NULL || terms[index].field[0] == '\0' ||
        terms[index].value == NULL) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index scalar term-set lookup requires "
                        "non-empty fields and non-null values",
                        NULL, NULL, NULL);
      break;
    }
    exact_terms[index].field_hex =
        lc_pouch_query_index_hex_encode(&pouch->allocator, terms[index].field);
    exact_terms[index].value_hex =
        lc_pouch_query_index_hex_encode(&pouch->allocator, terms[index].value);
    if (exact_terms[index].field_hex == NULL ||
        exact_terms[index].value_hex == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index scalar term",
                        NULL, NULL, NULL);
      break;
    }
  }
  if (rc == LC_OK) {
    qsort(exact_terms, term_count, sizeof(exact_terms[0]),
          lc_pouch_query_index_exact_term_compare);
    write_index = 0U;
    for (index = 0U; index < term_count; ++index) {
      if (write_index > 0U &&
          lc_pouch_query_index_exact_term_compare(
              &exact_terms[write_index - 1U], &exact_terms[index]) == 0) {
        lc_free_with_allocator(&pouch->allocator, exact_terms[index].field_hex);
        lc_free_with_allocator(&pouch->allocator, exact_terms[index].value_hex);
        memset(&exact_terms[index], 0, sizeof(exact_terms[index]));
        continue;
      }
      if (write_index != index) {
        exact_terms[write_index] = exact_terms[index];
        memset(&exact_terms[index], 0, sizeof(exact_terms[index]));
      }
      ++write_index;
    }
    reader.allocator = &pouch->allocator;
    reader.exact_terms = exact_terms;
    reader.exact_term_count = write_index;
    reader.visit = visit;
    reader.context = context;
    rc = lc_pouch_query_index_read_with_reader(sidecar_path, &sidecar, NULL,
                                               &reader, NULL, error);
    if (rc == LC_OK && (!sidecar.present || !sidecar.valid)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index sidecar is not readable", NULL,
                        NULL, "pouch-redesign");
    }
    if (rc == LC_OK && !sidecar.term_index_complete) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index scalar postings are incomplete",
                        NULL, NULL, "pouch-redesign");
    }
    if (rc == LC_OK) {
      *index_seq = sidecar.index_seq;
    }
  }
  lc_free_with_allocator(&pouch->allocator, reader.value_scratch);
  for (index = 0U; index < term_count; ++index) {
    lc_free_with_allocator(&pouch->allocator, exact_terms[index].field_hex);
    lc_free_with_allocator(&pouch->allocator, exact_terms[index].value_hex);
  }
  lc_free_with_allocator(&pouch->allocator, exact_terms);
  lc_free_with_allocator(&pouch->allocator, sidecar_path);
  return rc;
}

static int lc_pouch_query_index_docid_key_collect(
    const lc_pouch_query_index_key_view *key, void *context,
    lc_error *error) {
  lc_pouch_query_index_docid_key_context *doc_context;

  doc_context = (lc_pouch_query_index_docid_key_context *)context;
  if (doc_context == NULL || doc_context->keys == NULL) {
    return LC_OK;
  }
  if (key == NULL) {
    return LC_OK;
  }
  return lc_pouch_index_result_key_list_add(
      doc_context->allocator, doc_context->keys, key->key_hex, key->doc_id,
      key->version, key->bytes, key->has_query_hidden, key->query_hidden,
      key->value_index, error);
}

static int lc_pouch_query_index_docid_key_emit(
    const lc_allocator *allocator, lc_pouch_index_result_key_list *keys,
    lc_pouch_query_index_key_visit_fn visit, void *context, lc_error *error) {
  lc_pouch_query_index_key_view key_view;
  char *key;
  size_t index;
  int rc;

  if (keys == NULL || visit == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index docID key emit requires keys and "
                        "visitor",
                        NULL, NULL, NULL);
  }
  if (keys->count == 0U) {
    return LC_OK;
  }
  rc = lc_pouch_index_result_key_list_sort_compact_docids(allocator, keys,
                                                          error);
  if (rc != LC_OK) {
    return rc;
  }
  for (index = 0U; rc == LC_OK && index < keys->count; ++index) {
    memset(&key_view, 0, sizeof(key_view));
    key = lc_pouch_query_index_hex_decode(allocator, keys->items[index].key_hex,
                                          error);
    if (key == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      break;
    }
    key_view.key = key;
    key_view.key_hex = keys->items[index].key_hex;
    key_view.doc_id = keys->items[index].doc_id;
    key_view.version = keys->items[index].version;
    key_view.bytes = keys->items[index].bytes;
    key_view.has_query_hidden = keys->items[index].has_query_hidden;
    key_view.query_hidden = keys->items[index].query_hidden;
    key_view.value_index = keys->items[index].value_index;
    rc = visit(&key_view, context, error);
    lc_free_with_allocator(allocator, key);
  }
  if (rc == LC_POUCH_STATE_READ_MANY_STOP) {
    rc = LC_OK;
  }
  return rc;
}

static int lc_pouch_query_index_any_merge_collect(
    const lc_pouch_query_index_key_view *key, void *context,
    lc_error *error) {
  lc_pouch_query_index_any_merge_context *merge_context;

  merge_context = (lc_pouch_query_index_any_merge_context *)context;
  if (merge_context == NULL || key == NULL ||
      key->value_index >= merge_context->list_count) {
    return LC_OK;
  }
  return lc_pouch_index_result_row_list_add(
      merge_context->allocator, &merge_context->lists[key->value_index],
      key->key, key->key_hex, key->doc_id, key->version, key->bytes,
      key->has_query_hidden, key->query_hidden, key->value_index, error);
}

static int lc_pouch_query_index_any_merge_emit(
    lc_pouch_query_index_any_merge_context *merge_context,
    lc_pouch_query_index_key_visit_fn visit, void *context, lc_error *error) {
  size_t *positions;
  size_t index;
  size_t min_index;
  const char *min_key;
  int rc;

  if (merge_context == NULL || visit == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index merge emit requires context and "
                        "visitor",
                        NULL, NULL, NULL);
  }
  positions = (size_t *)lc_alloc_with_allocator(
      merge_context->allocator, merge_context->list_count * sizeof(*positions));
  if (positions == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index merge cursors",
                        NULL, NULL, NULL);
  }
  memset(positions, 0, merge_context->list_count * sizeof(*positions));
  rc = LC_OK;
  while (rc == LC_OK) {
    min_index = merge_context->list_count;
    min_key = NULL;
    for (index = 0U; index < merge_context->list_count; ++index) {
      lc_pouch_index_result_row_list *list;

      list = &merge_context->lists[index];
      if (positions[index] >= list->count) {
        continue;
      }
      if (min_key == NULL ||
          strcmp(list->items[positions[index]].key, min_key) < 0) {
        min_key = list->items[positions[index]].key;
        min_index = index;
      }
    }
    if (min_key == NULL || min_index >= merge_context->list_count) {
      break;
    }
    {
      lc_pouch_index_result_row *row;
      lc_pouch_query_index_key_view key_view;

      row = &merge_context->lists[min_index].items[positions[min_index]];
      memset(&key_view, 0, sizeof(key_view));
      key_view.key = row->key;
      key_view.key_hex = row->key_hex;
      key_view.doc_id = row->doc_id;
      key_view.version = row->version;
      key_view.bytes = row->bytes;
      key_view.has_query_hidden = row->has_query_hidden;
      key_view.query_hidden = row->query_hidden;
      key_view.value_index = row->value_index;
      rc = visit(&key_view, context, error);
    }
    for (index = 0U; index < merge_context->list_count; ++index) {
      lc_pouch_index_result_row_list *list;

      list = &merge_context->lists[index];
      while (positions[index] < list->count &&
             strcmp(list->items[positions[index]].key, min_key) == 0) {
        ++positions[index];
      }
    }
  }
  if (rc == LC_POUCH_STATE_READ_MANY_STOP) {
    rc = LC_OK;
  }
  lc_free_with_allocator(merge_context->allocator, positions);
  return rc;
}

int lc_pouch_query_index_visit_scalar_any_merged(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *const *values, size_t value_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error) {
  lc_pouch_query_index_any_merge_context merge_context;
  size_t index;
  int rc;

  memset(&merge_context, 0, sizeof(merge_context));
  if (pouch == NULL || value_count == 0U) {
    return lc_pouch_query_index_visit_scalar_any(
        pouch, namespace_name, field, values, value_count, visit, context,
        index_seq, error);
  }
  if (value_count == 1U) {
    return lc_pouch_query_index_visit_scalar(
        pouch, namespace_name, field, values[0], visit, context, index_seq,
        error);
  }
  merge_context.allocator = &pouch->allocator;
  merge_context.list_count = value_count;
  merge_context.lists = (lc_pouch_index_result_row_list *)lc_alloc_with_allocator(
      &pouch->allocator, value_count * sizeof(*merge_context.lists));
  if (merge_context.lists == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index merge lists",
                        NULL, NULL, NULL);
  }
  memset(merge_context.lists, 0,
         value_count * sizeof(*merge_context.lists));
  rc = lc_pouch_query_index_visit_scalar_any(
      pouch, namespace_name, field, values, value_count,
      lc_pouch_query_index_any_merge_collect, &merge_context, index_seq, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_any_merge_emit(&merge_context, visit, context,
                                             error);
  }
  for (index = 0U; index < value_count; ++index) {
    lc_pouch_index_result_row_list_cleanup(&pouch->allocator,
                                           &merge_context.lists[index]);
  }
  lc_free_with_allocator(&pouch->allocator, merge_context.lists);
  return rc;
}

int lc_pouch_query_index_visit_scalar_terms_merged(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_scalar_term *terms, size_t term_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error) {
  lc_pouch_query_index_any_merge_context merge_context;
  size_t index;
  int rc;

  memset(&merge_context, 0, sizeof(merge_context));
  if (pouch == NULL || term_count == 0U) {
    return lc_pouch_query_index_visit_scalar_terms(
        pouch, namespace_name, terms, term_count, visit, context, index_seq,
        error);
  }
  if (term_count == 1U) {
    return lc_pouch_query_index_visit_scalar(
        pouch, namespace_name, terms[0].field, terms[0].value, visit, context,
        index_seq, error);
  }
  merge_context.allocator = &pouch->allocator;
  merge_context.list_count = term_count;
  merge_context.lists = (lc_pouch_index_result_row_list *)lc_alloc_with_allocator(
      &pouch->allocator, term_count * sizeof(*merge_context.lists));
  if (merge_context.lists == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index term merge "
                        "lists",
                        NULL, NULL, NULL);
  }
  memset(merge_context.lists, 0,
         term_count * sizeof(*merge_context.lists));
  rc = lc_pouch_query_index_visit_scalar_terms(
      pouch, namespace_name, terms, term_count,
      lc_pouch_query_index_any_merge_collect, &merge_context, index_seq, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_any_merge_emit(&merge_context, visit, context,
                                             error);
  }
  for (index = 0U; index < term_count; ++index) {
    lc_pouch_index_result_row_list_cleanup(&pouch->allocator,
                                           &merge_context.lists[index]);
  }
  lc_free_with_allocator(&pouch->allocator, merge_context.lists);
  return rc;
}

int lc_pouch_query_index_visit_scalar_terms_docids(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_scalar_term *terms, size_t term_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error) {
  lc_pouch_query_index_read_result sidecar;
  lc_pouch_query_index_term_reader term_reader;
  lc_pouch_index_result_key_list keys;
  lc_pouch_query_index_docid_key_context doc_context;
  lc_pouch_query_index_exact_term *exact_terms;
  char *sidecar_path;
  size_t index;
  size_t write_index;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      terms == NULL || term_count == 0U || visit == NULL ||
      index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index scalar term docID lookup requires "
                        "pouch, namespace, terms, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  if (term_count == 1U) {
    return lc_pouch_query_index_visit_scalar(
        pouch, namespace_name, terms[0].field, terms[0].value, visit, context,
        index_seq, error);
  }
  memset(&term_reader, 0, sizeof(term_reader));
  memset(&keys, 0, sizeof(keys));
  memset(&doc_context, 0, sizeof(doc_context));
  *index_seq = 0UL;
  sidecar_path = lc_pouch_query_index_path(pouch, namespace_name, error);
  if (sidecar_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  exact_terms = (lc_pouch_query_index_exact_term *)lc_alloc_with_allocator(
      &pouch->allocator, term_count * sizeof(*exact_terms));
  if (exact_terms == NULL) {
    lc_free_with_allocator(&pouch->allocator, sidecar_path);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index scalar term "
                        "docID lookup",
                        NULL, NULL, NULL);
  }
  memset(exact_terms, 0, term_count * sizeof(*exact_terms));
  rc = LC_OK;
  for (index = 0U; index < term_count; ++index) {
    if (terms[index].field == NULL || terms[index].field[0] == '\0' ||
        terms[index].value == NULL) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index scalar term docID lookup requires "
                        "non-empty fields and non-null values",
                        NULL, NULL, NULL);
      break;
    }
    exact_terms[index].field_hex =
        lc_pouch_query_index_hex_encode(&pouch->allocator, terms[index].field);
    exact_terms[index].value_hex =
        lc_pouch_query_index_hex_encode(&pouch->allocator, terms[index].value);
    if (exact_terms[index].field_hex == NULL ||
        exact_terms[index].value_hex == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index scalar docID "
                        "term",
                        NULL, NULL, NULL);
      break;
    }
  }
  if (rc == LC_OK) {
    qsort(exact_terms, term_count, sizeof(exact_terms[0]),
          lc_pouch_query_index_exact_term_compare);
    write_index = 0U;
    for (index = 0U; index < term_count; ++index) {
      if (write_index > 0U &&
          lc_pouch_query_index_exact_term_compare(
              &exact_terms[write_index - 1U], &exact_terms[index]) == 0) {
        lc_free_with_allocator(&pouch->allocator, exact_terms[index].field_hex);
        lc_free_with_allocator(&pouch->allocator, exact_terms[index].value_hex);
        memset(&exact_terms[index], 0, sizeof(exact_terms[index]));
        continue;
      }
      if (write_index != index) {
        exact_terms[write_index] = exact_terms[index];
        memset(&exact_terms[index], 0, sizeof(exact_terms[index]));
      }
      ++write_index;
    }
    doc_context.allocator = &pouch->allocator;
    doc_context.keys = &keys;
    term_reader.allocator = &pouch->allocator;
    term_reader.exact_terms = exact_terms;
    term_reader.exact_term_count = write_index;
    term_reader.visit_doc_ids = 1;
    term_reader.visit = lc_pouch_query_index_docid_key_collect;
    term_reader.context = &doc_context;
    rc = lc_pouch_query_index_read_with_reader(sidecar_path, &sidecar, NULL,
                                               &term_reader, NULL, error);
    if (rc == LC_OK && (!sidecar.present || !sidecar.valid)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index sidecar is not readable", NULL,
                        NULL, "pouch-redesign");
    }
    if (rc == LC_OK && !sidecar.term_index_complete) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index scalar postings are incomplete",
                        NULL, NULL, "pouch-redesign");
    }
    if (rc == LC_OK) {
      *index_seq = sidecar.index_seq;
      rc = lc_pouch_query_index_docid_key_emit(
          &pouch->allocator, &keys, visit, context, error);
    }
  }
  lc_free_with_allocator(&pouch->allocator, term_reader.value_scratch);
  for (index = 0U; index < term_count; ++index) {
    lc_free_with_allocator(&pouch->allocator, exact_terms[index].field_hex);
    lc_free_with_allocator(&pouch->allocator, exact_terms[index].value_hex);
  }
  lc_free_with_allocator(&pouch->allocator, exact_terms);
  lc_pouch_index_result_key_list_cleanup(&pouch->allocator, &keys);
  lc_free_with_allocator(&pouch->allocator, sidecar_path);
  return rc;
}

int lc_pouch_query_index_visit_scalar_any_docids(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *const *values, size_t value_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error) {
  lc_pouch_query_index_scalar_term *terms;
  size_t index;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      field == NULL || field[0] == '\0' || values == NULL ||
      value_count == 0U || visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index multi-scalar docID lookup requires "
                        "pouch, namespace, field, values, visitor, and "
                        "index_seq",
                        NULL, NULL, NULL);
  }
  if (value_count == 1U) {
    return lc_pouch_query_index_visit_scalar(
        pouch, namespace_name, field, values[0], visit, context, index_seq,
        error);
  }
  terms = (lc_pouch_query_index_scalar_term *)lc_alloc_with_allocator(
      &pouch->allocator, value_count * sizeof(*terms));
  if (terms == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index multi-scalar "
                        "docID lookup",
                        NULL, NULL, NULL);
  }
  memset(terms, 0, value_count * sizeof(*terms));
  rc = LC_OK;
  for (index = 0U; index < value_count; ++index) {
    if (values[index] == NULL) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index multi-scalar docID lookup requires "
                        "non-null values",
                        NULL, NULL, NULL);
      break;
    }
    terms[index].field = field;
    terms[index].value = values[index];
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_visit_scalar_terms_docids(
        pouch, namespace_name, terms, value_count, visit, context, index_seq,
        error);
  }
  lc_free_with_allocator(&pouch->allocator, terms);
  return rc;
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

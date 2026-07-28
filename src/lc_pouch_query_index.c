#include "lc_pouch_query_index.h"

#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"

#include <errno.h>
#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define LC_POUCH_QUERY_INDEX_FORMAT "pouch-query-index"
#define LC_POUCH_QUERY_INDEX_VERSION 11UL
#define LC_POUCH_QUERY_INDEX_LEAF "query.index"
#define LC_POUCH_QUERY_INDEX_DOC_TABLE_LEAF "query.index.lcpdtg"
#define LC_POUCH_QUERY_INDEX_EXACT_TERM_LEAF "query.index.lcpttg"
#define LC_POUCH_QUERY_INDEX_PRESENCE_TERM_LEAF "query.index.lcppg"
#define LC_POUCH_QUERY_INDEX_RANGE_TERM_LEAF "query.index.lcprg"
#define LC_POUCH_QUERY_INDEX_TEXT_TERM_LEAF "query.index.lcptxg"
#define LC_POUCH_QUERY_INDEX_TRIGRAM_TERM_LEAF "query.index.lcpt3g"
#define LC_POUCH_QUERY_INDEX_TEMPORAL_TERM_LEAF "query.index.lcptdg"
#define LC_POUCH_QUERY_INDEX_CRYPTO_DESC_SUFFIX ".lcpcrypto"
#define LC_POUCH_QUERY_INDEX_ANY_TEXT_FIELD "/..."
#define LC_POUCH_QUERY_INDEX_ANY_TEXT_FIELD_HEX "2f2e2e2e"
#define LC_POUCH_QUERY_INDEX_HASH_OFFSET 2166136261UL
#define LC_POUCH_QUERY_INDEX_HASH_PRIME 16777619UL
#define LC_POUCH_QUERY_INDEX_HASH_MASK 0xffffffffUL

typedef struct lc_pouch_query_index_row {
  char *key;
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

typedef struct lc_pouch_query_index_extract_batch {
  lc_pouch_query_index_summary *summary;
  size_t index;
} lc_pouch_query_index_extract_batch;

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
  const char *value_text;
  int prefix_match;
  int contains_match;
  int ignore_case;
  int string_values_only;
  int range_match;
  int date_match;
  int stop;
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

typedef struct lc_pouch_query_index_any_merge_context {
  const lc_allocator *allocator;
  lc_pouch_index_result_row_list *lists;
  size_t list_count;
} lc_pouch_query_index_any_merge_context;

typedef struct lc_pouch_query_index_exact_generation_docids {
  unsigned long term_id;
  lc_pouch_index_docid_set docids;
} lc_pouch_query_index_exact_generation_docids;

typedef struct lc_pouch_query_index_exact_generation_accumulator {
  lc_pouch_query_index_exact_generation_docids *items;
  size_t count;
  size_t capacity;
} lc_pouch_query_index_exact_generation_accumulator;

typedef struct lc_pouch_query_index_file_signature {
  unsigned long size;
  unsigned long mtime;
  int present;
} lc_pouch_query_index_file_signature;

struct lc_pouch_query_index_prepared_exact {
  struct lc_pouch_query_index_prepared_exact *next;
  char *namespace_name;
  unsigned long index_seq;
  unsigned long row_count;
  unsigned long row_hash;
  lc_pouch_index_doc_table doc_table;
  lc_pouch_index_term_generation exact_generation;
  lc_pouch_query_index_file_signature doc_table_signature;
  lc_pouch_query_index_file_signature exact_signature;
  char *last_result_key;
  lc_pouch_index_result_docid_list last_result_docids;
  lc_pouch_index_result_page_cache page_cache;
};

struct lc_pouch_query_index_prepared_presence {
  struct lc_pouch_query_index_prepared_presence *next;
  char *namespace_name;
  unsigned long index_seq;
  unsigned long row_count;
  unsigned long row_hash;
  lc_pouch_index_doc_table doc_table;
  lc_pouch_index_term_generation presence_generation;
  lc_pouch_query_index_file_signature doc_table_signature;
  lc_pouch_query_index_file_signature presence_signature;
  char *last_result_key;
  lc_pouch_index_result_docid_list last_result_docids;
  lc_pouch_index_result_page_cache page_cache;
};

struct lc_pouch_query_index_prepared_range {
  struct lc_pouch_query_index_prepared_range *next;
  char *namespace_name;
  unsigned long index_seq;
  unsigned long row_count;
  unsigned long row_hash;
  lc_pouch_index_doc_table doc_table;
  lc_pouch_index_term_generation range_generation;
  lc_pouch_query_index_file_signature doc_table_signature;
  lc_pouch_query_index_file_signature range_signature;
  char *last_result_key;
  lc_pouch_index_result_docid_list last_result_docids;
  lc_pouch_index_result_page_cache page_cache;
};

struct lc_pouch_query_index_prepared_text {
  struct lc_pouch_query_index_prepared_text *next;
  char *namespace_name;
  unsigned long index_seq;
  unsigned long row_count;
  unsigned long row_hash;
  lc_pouch_index_doc_table doc_table;
  lc_pouch_index_term_generation text_generation;
  lc_pouch_index_term_generation trigram_generation;
  lc_pouch_query_index_file_signature doc_table_signature;
  lc_pouch_query_index_file_signature text_signature;
  lc_pouch_query_index_file_signature trigram_signature;
  char *last_result_key;
  lc_pouch_index_result_docid_list last_result_docids;
  lc_pouch_index_result_page_cache page_cache;
};

struct lc_pouch_query_index_prepared_temporal {
  struct lc_pouch_query_index_prepared_temporal *next;
  char *namespace_name;
  unsigned long index_seq;
  unsigned long row_count;
  unsigned long row_hash;
  lc_pouch_index_doc_table doc_table;
  lc_pouch_index_term_generation temporal_generation;
  lc_pouch_query_index_file_signature doc_table_signature;
  lc_pouch_query_index_file_signature temporal_signature;
  char *last_result_key;
  lc_pouch_index_result_docid_list last_result_docids;
  lc_pouch_index_result_page_cache page_cache;
};

static int lc_pouch_query_index_visit_exact_generation(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_index_term_key *exact_terms, size_t exact_term_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error);
static int lc_pouch_query_index_prepare_text_reader(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar,
    lc_pouch_query_index_prepared_text **out, lc_error *error);

typedef struct lc_pouch_query_index_source_reader {
  lc_source *source;
  lc_error error;
} lc_pouch_query_index_source_reader;

static int lc_pouch_query_index_skip_lines(FILE *fp, unsigned long count,
                                           const lc_allocator *allocator,
                                           int *valid, lc_error *error);

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
                                            const char *bytes, size_t length) {
  size_t index;

  for (index = 0U; index < length; ++index) {
    lc_pouch_query_index_hash_byte(hash, (unsigned char)bytes[index]);
  }
}

static char *
lc_pouch_query_index_hex_encode_bytes(const lc_allocator *allocator,
                                      const char *value, size_t length) {
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

static void
lc_pouch_query_index_summary_cleanup(lc_pouch_query_index_summary *summary) {
  size_t index;

  if (summary == NULL) {
    return;
  }
  for (index = 0U; index < summary->count; ++index) {
    lc_free_with_allocator(summary->allocator, summary->rows[index].key);
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

static int
lc_pouch_query_index_summary_reserve(lc_pouch_query_index_summary *summary,
                                     size_t needed, lc_error *error) {
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

static int
lc_pouch_query_index_term_reserve(lc_pouch_query_index_summary *summary,
                                  size_t needed, lc_error *error) {
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
    memcpy(next_terms, summary->terms,
           summary->term_count * sizeof(*next_terms));
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
    const char *value, size_t value_len, const char *key_hex, char value_type,
    unsigned long version, unsigned long bytes, int has_query_hidden,
    int query_hidden, lc_error *error) {
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
  term->field_hex = lc_pouch_query_index_hex_encode_bytes(summary->allocator,
                                                          field, field_len);
  term->value_hex = lc_pouch_query_index_hex_encode_bytes(summary->allocator,
                                                          value, value_len);
  term->key_hex = lc_strdup_with_allocator(summary->allocator, key_hex);
  if (term->field_hex == NULL || term->value_hex == NULL ||
      term->key_hex == NULL) {
    lc_free_with_allocator(summary->allocator, term->field_hex);
    lc_free_with_allocator(summary->allocator, term->value_hex);
    lc_free_with_allocator(summary->allocator, term->key_hex);
    memset(term, 0, sizeof(*term));
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index term", NULL, NULL,
                        NULL);
  }
  term->value_type = value_type;
  term->version = version;
  term->bytes = bytes;
  term->has_query_hidden = has_query_hidden;
  term->query_hidden = query_hidden;
  ++summary->term_count;
  return LC_OK;
}

static int
lc_pouch_query_index_presence_reserve(lc_pouch_query_index_summary *summary,
                                      size_t needed, lc_error *error) {
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

static int
lc_pouch_query_index_presence_add(lc_pouch_query_index_summary *summary,
                                  const char *field, size_t field_len,
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

static lonejson_status
lc_pouch_query_index_lonejson_error(lonejson_error *lj_error,
                                    const lc_error *error) {
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

static lonejson_status
lc_pouch_query_index_presence_value(void *user, const lonejson_value_path *path,
                                    lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  int rc;

  context = (lc_pouch_query_index_extract_context *)user;
  lc_error_init(&error);
  rc = lc_pouch_query_index_extract_set_field(context, path, &error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_presence_add(context->summary, context->field,
                                           context->field_len, context->key_hex,
                                           &error);
  }
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_query_index_string_begin(void *user, const lonejson_value_path *path,
                                  lonejson_error *lj_error) {
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
    rc = lc_pouch_query_index_presence_add(context->summary, context->field,
                                           context->field_len, context->key_hex,
                                           &error);
  }
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_query_index_string_chunk(void *user, const lonejson_value_path *path,
                                  const char *data, size_t len,
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

static lonejson_status
lc_pouch_query_index_string_end(void *user, const lonejson_value_path *path,
                                lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  int rc;

  (void)path;
  context = (lc_pouch_query_index_extract_context *)user;
  lc_error_init(&error);
  rc = lc_pouch_query_index_term_add(
      context->summary, context->field, context->field_len, context->value,
      context->value_len, context->key_hex, 's', context->version,
      context->bytes, context->has_query_hidden, context->query_hidden, &error);
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_query_index_number_begin(void *user, const lonejson_value_path *path,
                                  lonejson_error *lj_error) {
  return lc_pouch_query_index_string_begin(user, path, lj_error);
}

static lonejson_status
lc_pouch_query_index_number_chunk(void *user, const lonejson_value_path *path,
                                  const char *data, size_t len,
                                  lonejson_error *lj_error) {
  return lc_pouch_query_index_string_chunk(user, path, data, len, lj_error);
}

static lonejson_status
lc_pouch_query_index_number_end(void *user, const lonejson_value_path *path,
                                lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  int rc;

  (void)path;
  context = (lc_pouch_query_index_extract_context *)user;
  lc_error_init(&error);
  rc = lc_pouch_query_index_term_add(
      context->summary, context->field, context->field_len, context->value,
      context->value_len, context->key_hex, 'n', context->version,
      context->bytes, context->has_query_hidden, context->query_hidden, &error);
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_query_index_boolean_value(void *user, const lonejson_value_path *path,
                                   int value, lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  const char *text;
  int rc;

  context = (lc_pouch_query_index_extract_context *)user;
  text = value ? "true" : "false";
  lc_error_init(&error);
  rc = lc_pouch_query_index_extract_set_field(context, path, &error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_presence_add(context->summary, context->field,
                                           context->field_len, context->key_hex,
                                           &error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_term_add(
        context->summary, context->field, context->field_len, text,
        strlen(text), context->key_hex, 'b', context->version, context->bytes,
        context->has_query_hidden, context->query_hidden, &error);
  }
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_query_index_null_value(void *user, const lonejson_value_path *path,
                                lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  int rc;

  context = (lc_pouch_query_index_extract_context *)user;
  lc_error_init(&error);
  rc = lc_pouch_query_index_extract_set_field(context, path, &error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_presence_add(context->summary, context->field,
                                           context->field_len, context->key_hex,
                                           &error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_term_add(
        context->summary, context->field, context->field_len, "null", 4U,
        context->key_hex, 'z', context->version, context->bytes,
        context->has_query_hidden, context->query_hidden, &error);
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

static int lc_pouch_query_index_extract_terms_from_body(
    lc_pouch_query_index_summary *summary, const char *key,
    const lc_pouch_query_index_row *row, lc_source *body, lc_error *error) {
  lc_pouch_query_index_source_reader reader;
  lc_pouch_query_index_extract_context context;
  lonejson_path_value_visitor visitor;
  lonejson_error lj_error;
  lonejson *runtime;
  lonejson_status status;
  int rc;

  memset(&reader, 0, sizeof(reader));
  memset(&context, 0, sizeof(context));
  if (summary == NULL || key == NULL || row == NULL || body == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index extraction requires summary, key, "
                        "row, and body",
                        NULL, NULL, NULL);
  }
  if (body->reset != NULL) {
    rc = body->reset(body, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  runtime = lc_thread_lonejson_runtime();
  if (runtime == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize pouch query-index JSON runtime",
                        NULL, NULL, NULL);
  }
  reader.source = body;
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
  return rc;
}

static int lc_pouch_query_index_extract_batch_visit(
    const char *key, const lc_pouch_state_read_result *read_result,
    void *context, lc_error *error) {
  lc_pouch_query_index_extract_batch *batch;
  lc_pouch_query_index_row *row;

  batch = (lc_pouch_query_index_extract_batch *)context;
  if (batch == NULL || batch->summary == NULL ||
      batch->index >= batch->summary->count) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index batch extraction exceeded rows",
                        NULL, NULL, NULL);
  }
  row = &batch->summary->rows[batch->index++];
  if (read_result == NULL || !read_result->found || read_result->body == NULL) {
    return LC_OK;
  }
  return lc_pouch_query_index_extract_terms_from_body(
      batch->summary, key != NULL ? key : row->key, row, read_result->body,
      error);
}

static int
lc_pouch_query_index_summary_visit(const lc_pouch_state_visit_entry *entry,
                                   void *context, lc_error *error) {
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
  rc =
      lc_pouch_query_index_summary_reserve(summary, summary->count + 1U, error);
  if (rc != LC_OK) {
    return rc;
  }
  row = &summary->rows[summary->count];
  memset(row, 0, sizeof(*row));
  row->key = lc_strdup_with_allocator(summary->allocator, entry->key);
  row->key_hex =
      lc_pouch_query_index_hex_encode(summary->allocator, entry->key);
  row->content_type_hex =
      lc_pouch_query_index_hex_encode(summary->allocator, entry->content_type);
  row->etag_hex =
      lc_pouch_query_index_hex_encode(summary->allocator, entry->etag);
  if (row->key == NULL || row->key_hex == NULL ||
      row->content_type_hex == NULL || row->etag_hex == NULL) {
    lc_free_with_allocator(summary->allocator, row->key);
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
  return LC_OK;
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

static int
lc_pouch_query_index_term_equal(const lc_pouch_query_index_term *left,
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
                        "failed to allocate pouch query-index text", NULL, NULL,
                        NULL);
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

static int
lc_pouch_query_index_text_append_cstr(lc_pouch_query_index_text *text,
                                      const char *bytes, unsigned long *hash,
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
                        "failed to read pouch query-index row", strerror(errno),
                        NULL, NULL);
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

static char *lc_pouch_query_index_hex_decode(const lc_allocator *allocator,
                                             const char *token,
                                             lc_error *error) {
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
                 "failed to allocate pouch query-index decoded key", NULL, NULL,
                 NULL);
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
    next =
        (char *)lc_alloc_with_allocator(reader->allocator, decoded_length + 1U);
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
                        "failed to allocate pouch query-index row parser", NULL,
                        NULL, NULL);
  }
  memcpy(line, line_bytes, line_len + 1U);
  if (strncmp(line, "row ", sizeof("row ") - 1U) != 0) {
    lc_free_with_allocator(reader->allocator, line);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index row has invalid prefix", NULL, NULL,
                        NULL);
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

static int lc_pouch_query_index_exact_generation_value_hex(
    const lc_allocator *allocator, const char *value_hex, char value_type,
    char **out, lc_error *error) {
  char canonical[64];
  char *value_text;
  double number;
  int written;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index exact generation value requires "
                        "output",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  if (value_hex == NULL || (value_type != 's' && value_type != 'n' &&
                            value_type != 'b' && value_type != 'z')) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index exact generation value requires a "
                        "typed scalar",
                        NULL, NULL, "pouch");
  }
  if (value_type != 'n') {
    *out = lc_strdup_with_allocator(allocator, value_hex);
    if (*out == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch exact generation value",
                          NULL, NULL, NULL);
    }
    return LC_OK;
  }
  value_text = lc_pouch_query_index_hex_decode(allocator, value_hex, error);
  if (value_text == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  if (!lc_pouch_query_index_parse_number_value(value_text, &number)) {
    lc_free_with_allocator(allocator, value_text);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index exact generation number term is "
                        "invalid",
                        NULL, NULL, "pouch");
  }
  lc_free_with_allocator(allocator, value_text);
  if (number == 0.0) {
    written = snprintf(canonical, sizeof(canonical), "0");
  } else {
    written = snprintf(canonical, sizeof(canonical), "%.17g", number);
  }
  if (written < 0 || (size_t)written >= sizeof(canonical)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index exact generation number exceeds "
                        "local limit",
                        NULL, NULL, "pouch");
  }
  *out = lc_pouch_query_index_hex_encode(allocator, canonical);
  if (*out == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch exact numeric generation "
                        "value",
                        NULL, NULL, NULL);
  }
  return LC_OK;
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
  if (bounds == NULL || (!bounds->has_gt && !bounds->has_gte &&
                         !bounds->has_lt && !bounds->has_lte)) {
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

    if (value_hex[index * 2U] == '\0' || value_hex[(index * 2U) + 1U] == '\0') {
      return 0;
    }
    high = lc_pouch_query_index_hex_value((unsigned char)value_hex[index * 2U]);
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
  for (value_index = 0U; value_index + needle_len <= value_len; ++value_index) {
    if (lc_pouch_query_index_hex_text_has_prefix(value_hex + (value_index * 2U),
                                                 needle, ignore_case)) {
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
  if (value_len % 2U != 0U || needle_len % 2U != 0U || needle_len > value_len) {
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
      *matched =
          strncmp(value_hex, reader->value_hex, strlen(reader->value_hex)) == 0;
    }
    return LC_OK;
  }
  if (reader->contains_match) {
    if (reader->ignore_case) {
      *matched = lc_pouch_query_index_hex_text_contains(value_hex,
                                                        reader->value_text, 1);
    } else {
      *matched = lc_pouch_query_index_hex_contains_aligned(value_hex,
                                                           reader->value_hex);
    }
    return LC_OK;
  }
  if (reader->range_match) {
    if (lc_pouch_query_index_parse_integer_hex_value(value_hex, &number)) {
      *matched = lc_pouch_query_index_range_contains_value(
          &reader->range_bounds, number);
    } else {
      value_text =
          lc_pouch_query_index_hex_decode_scratch(reader, value_hex, error);
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
    value_text =
        lc_pouch_query_index_hex_decode_scratch(reader, value_hex, error);
    if (value_text == NULL) {
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    if (lc_pouch_index_parse_lql_datetime(value_text, &instant)) {
      *matched = lc_pouch_index_date_contains_value(&reader->parsed_date_bounds,
                                                    &instant);
    }
    return LC_OK;
  }
  *matched = strcmp(value_hex, reader->value_hex) == 0;
  return LC_OK;
}

static int lc_pouch_query_index_parse_and_visit_term(
    char *line, lc_pouch_query_index_term_reader *reader, lc_error *error) {
  lc_pouch_query_index_key_view key_view;
  char *cursor;
  char *field_hex;
  char *value_hex;
  char *key_hex;
  char *type_token;
  char *key;
  char value_type;
  int matched;
  int field_cmp;
  int value_cmp;
  int rc;

  if (line == NULL || reader == NULL || reader->visit == NULL) {
    return LC_OK;
  }
  if (strncmp(line, "term ", sizeof("term ") - 1U) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term has invalid prefix", NULL, NULL,
                        NULL);
  }
  cursor = line + sizeof("term ") - 1U;
  key = NULL;
  rc = LC_OK;
  memset(&key_view, 0, sizeof(key_view));
  if (!lc_pouch_query_index_parse_ulong_token(&cursor, &key_view.version) ||
      !lc_pouch_query_index_parse_ulong_token(&cursor, &key_view.bytes) ||
      !lc_pouch_query_index_parse_int_token(&cursor,
                                            &key_view.has_query_hidden) ||
      !lc_pouch_query_index_parse_int_token(&cursor, &key_view.query_hidden) ||
      !lc_pouch_query_index_parse_ulong_token(&cursor, &key_view.doc_id)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index term has invalid numeric fields", NULL,
                      NULL, NULL);
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
        type_token == NULL || type_token[0] == '\0' || type_token[1] != '\0' ||
        (type_token[0] != 's' && type_token[0] != 'n' && type_token[0] != 'b' &&
         type_token[0] != 'z') ||
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
  if (rc == LC_OK && (field_cmp = strcmp(field_hex, reader->field_hex)) > 0) {
    reader->stop = 1;
  } else if (rc == LC_OK && field_cmp == 0) {
    if (reader->string_values_only && value_type != 's') {
      matched = 0;
    } else if (!reader->prefix_match && !reader->contains_match &&
               !reader->range_match && !reader->date_match &&
               !reader->ignore_case) {
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
    key_view.key_hex = key_hex;
    key = lc_pouch_query_index_hex_decode(reader->allocator, key_hex, error);
    if (key == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    } else {
      key_view.key = key;
      rc = reader->visit(&key_view, reader->context, error);
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

  namespace_path = lc_pouch_namespace_path(&pouch->allocator, pouch->root_path,
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
  sidecar_path = lc_pouch_path_join(&pouch->allocator, index_path,
                                    LC_POUCH_QUERY_INDEX_LEAF);
  lc_free_with_allocator(&pouch->allocator, index_path);
  if (sidecar_path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index sidecar path", NULL,
                 NULL, NULL);
  }
  return sidecar_path;
}

static char *lc_pouch_query_index_doc_table_path(lc_pouch *pouch,
                                                 const char *namespace_name,
                                                 lc_error *error) {
  char *namespace_path;
  char *index_path;
  char *generation_path;

  namespace_path = lc_pouch_namespace_path(&pouch->allocator, pouch->root_path,
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
  generation_path = lc_pouch_path_join(&pouch->allocator, index_path,
                                       LC_POUCH_QUERY_INDEX_DOC_TABLE_LEAF);
  lc_free_with_allocator(&pouch->allocator, index_path);
  if (generation_path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index doc table generation "
                 "path",
                 NULL, NULL, NULL);
  }
  return generation_path;
}

static char *lc_pouch_query_index_exact_term_path(lc_pouch *pouch,
                                                  const char *namespace_name,
                                                  lc_error *error) {
  char *namespace_path;
  char *index_path;
  char *generation_path;

  namespace_path = lc_pouch_namespace_path(&pouch->allocator, pouch->root_path,
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
  generation_path = lc_pouch_path_join(&pouch->allocator, index_path,
                                       LC_POUCH_QUERY_INDEX_EXACT_TERM_LEAF);
  lc_free_with_allocator(&pouch->allocator, index_path);
  if (generation_path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index exact term generation "
                 "path",
                 NULL, NULL, NULL);
  }
  return generation_path;
}

static char *lc_pouch_query_index_presence_term_path(lc_pouch *pouch,
                                                     const char *namespace_name,
                                                     lc_error *error) {
  char *namespace_path;
  char *index_path;
  char *generation_path;

  namespace_path = lc_pouch_namespace_path(&pouch->allocator, pouch->root_path,
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
  generation_path = lc_pouch_path_join(&pouch->allocator, index_path,
                                       LC_POUCH_QUERY_INDEX_PRESENCE_TERM_LEAF);
  lc_free_with_allocator(&pouch->allocator, index_path);
  if (generation_path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index presence generation "
                 "path",
                 NULL, NULL, NULL);
  }
  return generation_path;
}

static char *lc_pouch_query_index_range_term_path(lc_pouch *pouch,
                                                  const char *namespace_name,
                                                  lc_error *error) {
  char *namespace_path;
  char *index_path;
  char *generation_path;

  namespace_path = lc_pouch_namespace_path(&pouch->allocator, pouch->root_path,
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
  generation_path = lc_pouch_path_join(&pouch->allocator, index_path,
                                       LC_POUCH_QUERY_INDEX_RANGE_TERM_LEAF);
  lc_free_with_allocator(&pouch->allocator, index_path);
  if (generation_path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index range generation path",
                 NULL, NULL, NULL);
  }
  return generation_path;
}

static char *lc_pouch_query_index_text_term_path(lc_pouch *pouch,
                                                 const char *namespace_name,
                                                 lc_error *error) {
  char *namespace_path;
  char *index_path;
  char *generation_path;

  namespace_path = lc_pouch_namespace_path(&pouch->allocator, pouch->root_path,
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
  generation_path = lc_pouch_path_join(&pouch->allocator, index_path,
                                       LC_POUCH_QUERY_INDEX_TEXT_TERM_LEAF);
  lc_free_with_allocator(&pouch->allocator, index_path);
  if (generation_path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index text generation path",
                 NULL, NULL, NULL);
  }
  return generation_path;
}

static char *lc_pouch_query_index_trigram_term_path(lc_pouch *pouch,
                                                    const char *namespace_name,
                                                    lc_error *error) {
  char *namespace_path;
  char *index_path;
  char *generation_path;

  namespace_path = lc_pouch_namespace_path(&pouch->allocator, pouch->root_path,
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
  generation_path = lc_pouch_path_join(&pouch->allocator, index_path,
                                       LC_POUCH_QUERY_INDEX_TRIGRAM_TERM_LEAF);
  lc_free_with_allocator(&pouch->allocator, index_path);
  if (generation_path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index trigram generation "
                 "path",
                 NULL, NULL, NULL);
  }
  return generation_path;
}

static char *lc_pouch_query_index_temporal_term_path(lc_pouch *pouch,
                                                     const char *namespace_name,
                                                     lc_error *error) {
  char *namespace_path;
  char *index_path;
  char *generation_path;

  namespace_path = lc_pouch_namespace_path(&pouch->allocator, pouch->root_path,
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
  generation_path = lc_pouch_path_join(&pouch->allocator, index_path,
                                       LC_POUCH_QUERY_INDEX_TEMPORAL_TERM_LEAF);
  lc_free_with_allocator(&pouch->allocator, index_path);
  if (generation_path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index temporal generation "
                 "path",
                 NULL, NULL, NULL);
  }
  return generation_path;
}

static int lc_pouch_query_index_file_present(const char *path, int *present,
                                             lc_error *error) {
  struct stat st;

  if (path == NULL || present == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index file probe requires path and "
                        "present output",
                        NULL, NULL, NULL);
  }
  *present = 0;
  if (stat(path, &st) == 0) {
    *present = S_ISREG(st.st_mode) ? 1 : 0;
    return LC_OK;
  }
  if (errno == ENOENT) {
    return LC_OK;
  }
  return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to probe pouch query-index artifact",
                      strerror(errno), NULL, "pouch");
}

static char *
lc_pouch_query_index_crypto_descriptor_path(const lc_allocator *allocator,
                                            const char *path,
                                            lc_error *error) {
  char *descriptor_path;
  size_t path_len;
  size_t suffix_len;

  if (path == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query-index crypto descriptor requires path", NULL,
                 NULL, "pouch");
    return NULL;
  }
  path_len = strlen(path);
  suffix_len = strlen(LC_POUCH_QUERY_INDEX_CRYPTO_DESC_SUFFIX);
  if (path_len > (size_t)-1 - suffix_len - 1U) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "pouch query-index crypto descriptor path exceeds local "
                 "limit",
                 NULL, NULL, NULL);
    return NULL;
  }
  descriptor_path =
      (char *)lc_alloc_with_allocator(allocator, path_len + suffix_len + 1U);
  if (descriptor_path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index crypto descriptor path",
                 NULL, NULL, NULL);
    return NULL;
  }
  memcpy(descriptor_path, path, path_len);
  memcpy(descriptor_path + path_len, LC_POUCH_QUERY_INDEX_CRYPTO_DESC_SUFFIX,
         suffix_len + 1U);
  return descriptor_path;
}

static char *lc_pouch_query_index_artifact_context(
    const lc_allocator *allocator, const char *namespace_name, const char *path,
    lc_error *error) {
  const char prefix[] = "query-index/";
  const char *leaf;
  char *context;
  size_t prefix_len;
  size_t namespace_len;
  size_t leaf_len;
  size_t length;

  if (namespace_name == NULL || path == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query-index crypto context requires namespace and "
                 "path",
                 NULL, NULL, "pouch");
    return NULL;
  }
  leaf = strrchr(path, '/');
  leaf = leaf != NULL ? leaf + 1 : path;
  prefix_len = sizeof(prefix) - 1U;
  namespace_len = strlen(namespace_name);
  leaf_len = strlen(leaf);
  if (namespace_len > (size_t)-1 - prefix_len - leaf_len - 2U) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "pouch query-index crypto context exceeds local limit", NULL,
                 NULL, NULL);
    return NULL;
  }
  length = prefix_len + namespace_len + 1U + leaf_len;
  context = (char *)lc_alloc_with_allocator(allocator, length + 1U);
  if (context == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index crypto context", NULL,
                 NULL, NULL);
    return NULL;
  }
  memcpy(context, prefix, prefix_len);
  memcpy(context + prefix_len, namespace_name, namespace_len);
  context[prefix_len + namespace_len] = '/';
  memcpy(context + prefix_len + namespace_len + 1U, leaf, leaf_len + 1U);
  return context;
}

static int lc_pouch_query_index_read_file_bytes(
    const lc_allocator *allocator, const char *path, char **out_bytes,
    size_t *out_length, int *present, lc_error *error) {
  FILE *fp;
  char *bytes;
  size_t length;
  size_t capacity;
  size_t got;

  if (path == NULL || out_bytes == NULL || out_length == NULL ||
      present == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index file read requires path and "
                        "outputs",
                        NULL, NULL, NULL);
  }
  *out_bytes = NULL;
  *out_length = 0U;
  *present = 0;
  fp = fopen(path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch query-index file",
                        strerror(errno), NULL, "pouch");
  }
  *present = 1;
  bytes = NULL;
  length = 0U;
  capacity = 0U;
  for (;;) {
    if (capacity - length <= 1U) {
      char *next_bytes;
      size_t next_capacity;

      next_capacity = capacity == 0U ? 4096U : capacity * 2U;
      if (next_capacity <= capacity || next_capacity == (size_t)-1) {
        fclose(fp);
        lc_free_with_allocator(allocator, bytes);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "pouch query-index file exceeds local limit", NULL,
                            NULL, NULL);
      }
      next_bytes = (char *)lc_alloc_with_allocator(allocator, next_capacity);
      if (next_bytes == NULL) {
        fclose(fp);
        lc_free_with_allocator(allocator, bytes);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch query-index file", NULL,
                            NULL, NULL);
      }
      if (bytes != NULL) {
        memcpy(next_bytes, bytes, length);
        lc_free_with_allocator(allocator, bytes);
      }
      bytes = next_bytes;
      capacity = next_capacity;
    }
    got = fread(bytes + length, 1U, capacity - length - 1U, fp);
    length += got;
    if (got == 0U) {
      if (ferror(fp)) {
        fclose(fp);
        lc_free_with_allocator(allocator, bytes);
        return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                            "failed to read pouch query-index file",
                            strerror(errno), NULL, "pouch");
      }
      break;
    }
  }
  if (fclose(fp) != 0) {
    lc_free_with_allocator(allocator, bytes);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch query-index file",
                        strerror(errno), NULL, "pouch");
  }
  bytes[length] = '\0';
  *out_bytes = bytes;
  *out_length = length;
  return LC_OK;
}

static int lc_pouch_query_index_source_to_bytes(
    const lc_allocator *allocator, lc_source *source, char **out_bytes,
    size_t *out_length, lc_error *error) {
  lc_sink *sink;
  const void *bytes;
  char *copy;
  size_t length;
  int rc;

  if (source == NULL || out_bytes == NULL || out_length == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index source read requires source and "
                        "outputs",
                        NULL, NULL, NULL);
  }
  *out_bytes = NULL;
  *out_length = 0U;
  sink = NULL;
  rc = lc_sink_to_memory(&sink, error);
  if (rc == LC_OK) {
    rc = lc_copy(source, sink, NULL, error);
  }
  bytes = NULL;
  length = 0U;
  if (rc == LC_OK) {
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  }
  copy = NULL;
  if (rc == LC_OK) {
    if (length == (size_t)-1) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch query-index source exceeds local limit", NULL,
                        NULL, NULL);
    } else {
      copy = (char *)lc_alloc_with_allocator(allocator, length + 1U);
      if (copy == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query-index source bytes",
                          NULL, NULL, NULL);
      } else {
        memcpy(copy, bytes, length);
        copy[length] = '\0';
      }
    }
  }
  if (sink != NULL) {
    lc_sink_close(sink);
  }
  if (rc != LC_OK) {
    lc_free_with_allocator(allocator, copy);
    return rc;
  }
  *out_bytes = copy;
  *out_length = length;
  return LC_OK;
}

static int lc_pouch_query_index_read_encrypted_artifact_bytes(
    lc_pouch *pouch, const char *namespace_name, const char *path, int strict,
    char **out_bytes, size_t *out_length, int *present, int *valid,
    lc_error *error) {
  lc_source *source;
  char *context;
  char *descriptor_path;
  char *descriptor;
  size_t descriptor_length;
  int descriptor_present;
  int rc;

  if (out_bytes == NULL || out_length == NULL || present == NULL ||
      valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted query-index artifact read requires "
                        "outputs",
                        NULL, NULL, NULL);
  }
  *out_bytes = NULL;
  *out_length = 0U;
  *present = 0;
  *valid = 0;
  rc = lc_pouch_query_index_file_present(path, present, error);
  if (rc != LC_OK || !*present) {
    return rc;
  }
  descriptor_path = lc_pouch_query_index_crypto_descriptor_path(
      &pouch->allocator, path, error);
  if (descriptor_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  descriptor = NULL;
  descriptor_length = 0U;
  descriptor_present = 0;
  rc = lc_pouch_query_index_read_file_bytes(
      &pouch->allocator, descriptor_path, &descriptor, &descriptor_length,
      &descriptor_present, error);
  lc_free_with_allocator(&pouch->allocator, descriptor_path);
  if (rc != LC_OK || !descriptor_present) {
    lc_free_with_allocator(&pouch->allocator, descriptor);
    return rc;
  }
  while (descriptor_length > 0U &&
         (descriptor[descriptor_length - 1U] == '\n' ||
          descriptor[descriptor_length - 1U] == '\r')) {
    descriptor[--descriptor_length] = '\0';
  }
  context = lc_pouch_query_index_artifact_context(
      &pouch->allocator, namespace_name, path, error);
  if (context == NULL) {
    lc_free_with_allocator(&pouch->allocator, descriptor);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  source = NULL;
  rc = lc_pouch_crypto_source_from_file(pouch->crypto, context, path,
                                        descriptor, &source, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_source_to_bytes(&pouch->allocator, source,
                                              out_bytes, out_length, error);
  }
  if (source != NULL) {
    source->close(source);
  }
  lc_free_with_allocator(&pouch->allocator, context);
  lc_free_with_allocator(&pouch->allocator, descriptor);
  if (rc != LC_OK) {
    if (!strict && rc != LC_ERR_NOMEM) {
      if (error != NULL) {
        lc_error_cleanup(error);
      }
      return LC_OK;
    }
    return rc;
  }
  *valid = 1;
  return LC_OK;
}

static int lc_pouch_query_index_open_encrypted_artifact_source(
    lc_pouch *pouch, const char *namespace_name, const char *path, int strict,
    lc_source **out, int *present, int *valid, lc_error *error) {
  char *context;
  char *descriptor_path;
  char *descriptor;
  size_t descriptor_length;
  int descriptor_present;
  int rc;

  if (out == NULL || present == NULL || valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted query-index artifact open requires "
                        "outputs",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  *present = 0;
  *valid = 0;
  rc = lc_pouch_query_index_file_present(path, present, error);
  if (rc != LC_OK || !*present) {
    return rc;
  }
  descriptor_path = lc_pouch_query_index_crypto_descriptor_path(
      &pouch->allocator, path, error);
  if (descriptor_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  descriptor = NULL;
  descriptor_length = 0U;
  descriptor_present = 0;
  rc = lc_pouch_query_index_read_file_bytes(
      &pouch->allocator, descriptor_path, &descriptor, &descriptor_length,
      &descriptor_present, error);
  lc_free_with_allocator(&pouch->allocator, descriptor_path);
  if (rc != LC_OK || !descriptor_present) {
    lc_free_with_allocator(&pouch->allocator, descriptor);
    return rc;
  }
  while (descriptor_length > 0U &&
         (descriptor[descriptor_length - 1U] == '\n' ||
          descriptor[descriptor_length - 1U] == '\r')) {
    descriptor[--descriptor_length] = '\0';
  }
  context = lc_pouch_query_index_artifact_context(
      &pouch->allocator, namespace_name, path, error);
  if (context == NULL) {
    lc_free_with_allocator(&pouch->allocator, descriptor);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_crypto_source_from_file(pouch->crypto, context, path,
                                        descriptor, out, error);
  lc_free_with_allocator(&pouch->allocator, context);
  lc_free_with_allocator(&pouch->allocator, descriptor);
  if (rc != LC_OK) {
    if (!strict && rc != LC_ERR_NOMEM) {
      if (error != NULL) {
        lc_error_cleanup(error);
      }
      return LC_OK;
    }
    return rc;
  }
  *valid = 1;
  return LC_OK;
}

static int lc_pouch_query_index_write_artifact_bytes(
    lc_pouch *pouch, const char *namespace_name, const char *path,
    const char *bytes, size_t length, lc_error *error) {
  lc_source *source;
  char *context;
  char *descriptor_path;
  char *descriptor;
  unsigned long plain_bytes;
  unsigned long cipher_bytes;
  int rc;

  if (pouch == NULL || path == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index artifact write requires pouch and "
                        "path",
                        NULL, NULL, NULL);
  }
  if (!lc_pouch_crypto_enabled(pouch->crypto)) {
    return lc_pouch_path_write_text_file(path, bytes != NULL ? bytes : "",
                                         error);
  }
  context = lc_pouch_query_index_artifact_context(
      &pouch->allocator, namespace_name, path, error);
  if (context == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  source = NULL;
  descriptor = NULL;
  rc = lc_source_from_memory(bytes != NULL ? bytes : "", length, &source,
                             error);
  if (rc == LC_OK) {
    rc = lc_pouch_crypto_stream_to_file(
        pouch->crypto, context, path, source, &plain_bytes, &cipher_bytes,
        &descriptor, error);
  }
  if (source != NULL) {
    source->close(source);
  }
  if (rc == LC_OK) {
    descriptor_path = lc_pouch_query_index_crypto_descriptor_path(
        &pouch->allocator, path, error);
    if (descriptor_path == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    } else {
      rc = lc_pouch_path_write_text_file(descriptor_path, descriptor, error);
      lc_free_with_allocator(&pouch->allocator, descriptor_path);
    }
  }
  if (rc != LC_OK) {
    unlink(path);
  }
  lc_free_with_allocator(&pouch->allocator, descriptor);
  lc_free_with_allocator(&pouch->allocator, context);
  return rc;
}

static int lc_pouch_query_index_doc_table_generation_load_artifact(
    lc_pouch *pouch, const char *namespace_name, const char *path,
    unsigned long expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, lc_pouch_index_doc_table *table,
    int *present, int *valid, lc_error *error) {
  char *bytes;
  size_t length;
  int artifact_valid;
  int rc;

  if (!lc_pouch_crypto_enabled(pouch != NULL ? pouch->crypto : NULL)) {
    return lc_pouch_index_doc_table_generation_load_file(
        &pouch->allocator, path, expected_index_seq, expected_row_count,
        expected_row_hash, table, present, valid, error);
  }
  memset(table, 0, sizeof(*table));
  *present = 0;
  *valid = 0;
  bytes = NULL;
  length = 0U;
  artifact_valid = 0;
  rc = lc_pouch_query_index_read_encrypted_artifact_bytes(
      pouch, namespace_name, path, 0, &bytes, &length, present,
      &artifact_valid, error);
  if (rc == LC_OK && *present && artifact_valid) {
    rc = lc_pouch_index_doc_table_generation_load_bytes(
        &pouch->allocator, &bytes, length, expected_index_seq,
        expected_row_count, expected_row_hash, table, valid, error);
  }
  lc_free_with_allocator(&pouch->allocator, bytes);
  return rc;
}

static int lc_pouch_query_index_doc_table_generation_validate_artifact(
    lc_pouch *pouch, const char *namespace_name, const char *path,
    unsigned long expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, int *present, int *valid,
    lc_error *error) {
  lc_pouch_index_doc_table table;
  int rc;

  if (!lc_pouch_crypto_enabled(pouch != NULL ? pouch->crypto : NULL)) {
    return lc_pouch_index_doc_table_generation_validate_file(
        &pouch->allocator, path, expected_index_seq, expected_row_count,
        expected_row_hash, present, valid, error);
  }
  memset(&table, 0, sizeof(table));
  rc = lc_pouch_query_index_doc_table_generation_load_artifact(
      pouch, namespace_name, path, expected_index_seq, expected_row_count,
      expected_row_hash, &table, present, valid, error);
  lc_pouch_index_doc_table_cleanup(&pouch->allocator, &table);
  return rc;
}

static int lc_pouch_query_index_term_generation_load_artifact(
    lc_pouch *pouch, const char *namespace_name, const char *path,
    unsigned long expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, lc_pouch_index_term_generation *generation,
    int *present, int *valid, lc_error *error) {
  char *bytes;
  size_t length;
  int artifact_valid;
  int rc;

  if (!lc_pouch_crypto_enabled(pouch != NULL ? pouch->crypto : NULL)) {
    return lc_pouch_index_term_generation_load_file(
        &pouch->allocator, path, expected_index_seq, expected_row_count,
        expected_row_hash, generation, present, valid, error);
  }
  memset(generation, 0, sizeof(*generation));
  *present = 0;
  *valid = 0;
  bytes = NULL;
  length = 0U;
  artifact_valid = 0;
  rc = lc_pouch_query_index_read_encrypted_artifact_bytes(
      pouch, namespace_name, path, 0, &bytes, &length, present,
      &artifact_valid, error);
  if (rc == LC_OK && *present && artifact_valid) {
    rc = lc_pouch_index_term_generation_load_bytes(
        &pouch->allocator, bytes, length, expected_index_seq,
        expected_row_count, expected_row_hash, generation, valid, error);
  }
  lc_free_with_allocator(&pouch->allocator, bytes);
  return rc;
}

static int lc_pouch_query_index_term_generation_validate_artifact(
    lc_pouch *pouch, const char *namespace_name, const char *path,
    unsigned long expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, int *present, int *valid,
    lc_error *error) {
  lc_pouch_index_term_generation generation;
  int rc;

  if (!lc_pouch_crypto_enabled(pouch != NULL ? pouch->crypto : NULL)) {
    return lc_pouch_index_term_generation_validate_file(
        &pouch->allocator, path, expected_index_seq, expected_row_count,
        expected_row_hash, present, valid, error);
  }
  memset(&generation, 0, sizeof(generation));
  rc = lc_pouch_query_index_term_generation_load_artifact(
      pouch, namespace_name, path, expected_index_seq, expected_row_count,
      expected_row_hash, &generation, present, valid, error);
  lc_pouch_index_term_generation_cleanup(&pouch->allocator, &generation);
  return rc;
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
  if (rc == LC_OK && (actual_rows != row_count || computed_hash != row_hash)) {
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
      if (rc == LC_POUCH_STATE_READ_MANY_STOP) {
        rc = LC_OK;
        reader->stop = 1;
      }
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

static int
lc_pouch_query_index_read_terms_slice(FILE *fp, unsigned long term_count,
                                      lc_pouch_query_index_term_reader *reader,
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
                        "pouch query-index term has invalid prefix", NULL, NULL,
                        NULL);
      break;
    }
    if (reader != NULL && reader->visit != NULL) {
      rc = lc_pouch_query_index_parse_and_visit_term(line.bytes, reader, error);
      if (rc == LC_POUCH_STATE_READ_MANY_STOP) {
        rc = LC_OK;
        reader->stop = 1;
      }
      if (rc != LC_OK) {
        break;
      }
    }
    ++actual_terms;
    if (reader != NULL && reader->stop) {
      break;
    }
  }
  if (rc == LC_OK &&
      (actual_terms == term_count || (reader != NULL && reader->stop))) {
    *valid = 1;
  }
  lc_free_with_allocator(line.allocator, line.bytes);
  return rc;
}

static int lc_pouch_query_index_seek_term_slice(FILE *fp,
                                                long term_section_start,
                                                unsigned long first_byte,
                                                const lc_allocator *allocator,
                                                int *valid, lc_error *error) {
  if (valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term seek requires valid output",
                        NULL, NULL, NULL);
  }
  *valid = 0;
  if (fp == NULL || term_section_start < 0L ||
      first_byte > (unsigned long)(LONG_MAX - term_section_start)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term byte range is invalid", NULL,
                        NULL, NULL);
  }
  if (fseek(fp, term_section_start + (long)first_byte, SEEK_SET) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to seek pouch query-index term slice",
                        strerror(errno), NULL, NULL);
  }
  (void)allocator;
  *valid = 1;
  return LC_OK;
}

static int lc_pouch_query_index_read_term_fields(
    FILE *fp, unsigned long term_field_count, unsigned long term_count,
    const lc_allocator *allocator, lc_pouch_index_term_field **out_fields,
    size_t *out_count, int *valid, lc_error *error) {
  lc_pouch_query_index_text line;
  lc_pouch_index_term_field *fields;
  unsigned long actual_fields;
  unsigned long previous_end;
  unsigned long previous_byte_end;
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
  previous_byte_end = 0UL;
  rc = LC_OK;
  while (actual_fields < term_field_count) {
    lc_pouch_index_term_field parsed;

    memset(&parsed, 0, sizeof(parsed));
    rc = lc_pouch_query_index_read_line(fp, &line, &got_line, error);
    if (rc != LC_OK || !got_line) {
      break;
    }
    rc = lc_pouch_index_term_field_parse_line(line.bytes, &parsed, allocator,
                                              error);
    if (rc != LC_OK) {
      break;
    }
    if (parsed.line_count == 0UL || parsed.byte_count == 0UL ||
        parsed.first_line < previous_end ||
        parsed.first_byte < previous_byte_end ||
        parsed.first_line > term_count ||
        parsed.line_count > term_count - parsed.first_line ||
        (actual_fields > 0UL && fields != NULL &&
         strcmp(fields[actual_fields - 1UL].field_hex, parsed.field_hex) >=
             0)) {
      lc_free_with_allocator(allocator, parsed.field_hex);
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term field range is invalid", NULL,
                        NULL, NULL);
      break;
    }
    if (fields != NULL) {
      fields[actual_fields] = parsed;
    } else {
      lc_free_with_allocator(allocator, parsed.field_hex);
    }
    previous_end = parsed.first_line + parsed.line_count;
    previous_byte_end = parsed.first_byte + parsed.byte_count;
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

static int lc_pouch_query_index_read_term_values(
    FILE *fp, unsigned long term_value_count, unsigned long term_count,
    const lc_allocator *allocator, lc_pouch_index_term_value **out_values,
    size_t *out_count, int *valid, lc_error *error) {
  lc_pouch_query_index_text line;
  lc_pouch_index_term_value *values;
  unsigned long actual_values;
  unsigned long previous_end;
  unsigned long previous_byte_end;
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
  previous_byte_end = 0UL;
  rc = LC_OK;
  while (actual_values < term_value_count) {
    lc_pouch_index_term_value parsed;

    memset(&parsed, 0, sizeof(parsed));
    rc = lc_pouch_query_index_read_line(fp, &line, &got_line, error);
    if (rc != LC_OK || !got_line) {
      break;
    }
    rc = lc_pouch_index_term_value_parse_line(line.bytes, &parsed, allocator,
                                              error);
    if (rc != LC_OK) {
      break;
    }
    if (parsed.line_count == 0UL || parsed.byte_count == 0UL ||
        parsed.first_line < previous_end ||
        parsed.first_byte < previous_byte_end ||
        parsed.first_line > term_count ||
        parsed.line_count > term_count - parsed.first_line ||
        (actual_values > 0UL && values != NULL &&
         (strcmp(values[actual_values - 1UL].field_hex, parsed.field_hex) > 0 ||
          (strcmp(values[actual_values - 1UL].field_hex, parsed.field_hex) ==
               0 &&
           strcmp(values[actual_values - 1UL].value_hex, parsed.value_hex) >=
               0)))) {
      lc_free_with_allocator(allocator, parsed.field_hex);
      lc_free_with_allocator(allocator, parsed.value_hex);
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term value range is invalid", NULL,
                        NULL, NULL);
      break;
    }
    if (values != NULL) {
      values[actual_values] = parsed;
    } else {
      lc_free_with_allocator(allocator, parsed.field_hex);
      lc_free_with_allocator(allocator, parsed.value_hex);
    }
    previous_end = parsed.first_line + parsed.line_count;
    previous_byte_end = parsed.first_byte + parsed.byte_count;
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

static int lc_pouch_query_index_term_reader_range(
    const lc_pouch_index_term_field *fields, size_t field_count,
    const lc_pouch_query_index_term_reader *reader, unsigned long term_count,
    unsigned long term_byte_count, unsigned long *first_line,
    unsigned long *line_count, unsigned long *first_byte,
    unsigned long *byte_count) {
  if (reader == NULL || first_line == NULL || line_count == NULL) {
    return 0;
  }
  return lc_pouch_index_term_fields_select_range(
      fields, field_count, reader->field_hex, NULL, 0U, term_count,
      term_byte_count, first_line, line_count, first_byte, byte_count);
}

static int lc_pouch_query_index_read_presences(
    FILE *fp, unsigned long presence_count, unsigned long presence_hash,
    lc_pouch_query_index_presence_reader *reader, int *valid, lc_error *error) {
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
      (actual_presences != presence_count || computed_hash != presence_hash)) {
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

static int lc_pouch_query_index_skip_lines(FILE *fp, unsigned long count,
                                           const lc_allocator *allocator,
                                           int *valid, lc_error *error) {
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

static int lc_pouch_query_index_read_with_reader_fp(
    FILE *fp, lc_pouch_query_index_read_result *out,
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
  unsigned long first_byte;
  unsigned long byte_count;
  unsigned long term_byte_count;
  lc_pouch_index_term_field *term_fields;
  lc_pouch_index_term_value *term_values;
  size_t term_field_table_count;
  size_t term_value_table_count;
  long term_section_start;
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

  if (fp == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index read requires a stream and output",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
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
  first_byte = 0UL;
  byte_count = 0UL;
  term_byte_count = 0UL;
  term_fields = NULL;
  term_values = NULL;
  term_field_table_count = 0U;
  term_value_table_count = 0U;
  term_section_start = -1L;
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
      lc_pouch_query_index_parse_header_line(line, "summary_hash", &row_hash)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_count", &term_count)) {
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
  if (matched == 13 && strcmp(format, LC_POUCH_QUERY_INDEX_FORMAT) == 0 &&
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
    if (rc == LC_OK) {
      term_section_start = ftell(fp);
      if (term_section_start < 0L) {
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to locate pouch query-index term section",
                          strerror(errno), NULL, NULL);
      }
    }
    if (rc == LC_OK && term_field_table_count > 0U) {
      lc_pouch_index_term_field *last_field;

      last_field = &term_fields[term_field_table_count - 1U];
      term_byte_count = last_field->first_byte + last_field->byte_count;
    }
    if (rc == LC_OK && want_terms) {
      unsigned long first_line;
      unsigned long line_count;

      first_line = 0UL;
      line_count = 0UL;
      first_byte = 0UL;
      byte_count = 0UL;
      if (rc == LC_OK && term_reader != NULL &&
          lc_pouch_query_index_term_reader_range(
              term_fields, term_field_table_count, term_reader, term_count,
              term_byte_count, &first_line, &line_count, &first_byte,
              &byte_count)) {
        rc = lc_pouch_query_index_seek_term_slice(
            fp, term_section_start, first_byte,
            lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                  presence_reader),
            &term_valid, error);
        if (rc == LC_OK && term_valid) {
          rc = lc_pouch_query_index_read_terms_slice(
              fp, line_count, term_reader, &term_valid, error);
        }
        if (rc == LC_OK && (want_rows || want_presences) && term_valid) {
          rc = lc_pouch_query_index_seek_term_slice(
              fp, term_section_start, term_byte_count,
              lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                    presence_reader),
              &term_valid, error);
        }
      } else if (rc == LC_OK && term_reader != NULL && want_rows) {
        rc = lc_pouch_query_index_seek_term_slice(
            fp, term_section_start, 0UL,
            lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                  presence_reader),
            &term_valid, error);
        if (rc == LC_OK && term_valid) {
          rc = lc_pouch_query_index_skip_lines(
              fp, term_count,
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
    } else if (rc == LC_OK && (want_rows || want_presences)) {
      rc = lc_pouch_query_index_seek_term_slice(
          fp, term_section_start, 0UL,
          lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                presence_reader),
          &term_valid, error);
    }
    if (rc == LC_OK && !want_terms && (want_rows || want_presences)) {
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
      rc = lc_pouch_query_index_read_presences(fp, presence_count,
                                               presence_hash, presence_reader,
                                               &presence_valid, error);
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
                   term_value_valid && presence_valid;
    }
  } else {
    rc = LC_OK;
  }
  lc_pouch_index_term_fields_cleanup(lc_pouch_query_index_reader_allocator(
                                         reader, term_reader, presence_reader),
                                     term_fields, term_field_table_count);
  lc_pouch_index_term_values_cleanup(lc_pouch_query_index_reader_allocator(
                                         reader, term_reader, presence_reader),
                                     term_values, term_value_table_count);
  return rc;
}

static int lc_pouch_query_index_read_with_reader_file(
    const char *path, lc_pouch_query_index_read_result *out,
    lc_pouch_query_index_row_reader *reader,
    lc_pouch_query_index_term_reader *term_reader,
    lc_pouch_query_index_presence_reader *presence_reader, lc_error *error) {
  FILE *fp;
  int rc;

  fp = fopen(path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      memset(out, 0, sizeof(*out));
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch query-index sidecar",
                        strerror(errno), NULL, NULL);
  }
  rc = lc_pouch_query_index_read_with_reader_fp(
      fp, out, reader, term_reader, presence_reader, error);
  if (fclose(fp) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch query-index sidecar",
                        strerror(errno), NULL, NULL);
  }
  return rc;
}

static int lc_pouch_query_index_read_with_reader(
    lc_pouch *pouch, const char *namespace_name, const char *path,
    lc_pouch_query_index_read_result *out,
    lc_pouch_query_index_row_reader *reader,
    lc_pouch_query_index_term_reader *term_reader,
    lc_pouch_query_index_presence_reader *presence_reader, lc_error *error) {
  char *bytes;
  size_t length;
  FILE *fp;
  int present;
  int valid;
  int rc;

  if (!lc_pouch_crypto_enabled(pouch != NULL ? pouch->crypto : NULL)) {
    return lc_pouch_query_index_read_with_reader_file(
        path, out, reader, term_reader, presence_reader, error);
  }
  bytes = NULL;
  length = 0U;
  present = 0;
  valid = 0;
  rc = lc_pouch_query_index_read_encrypted_artifact_bytes(
      pouch, namespace_name, path, 1, &bytes, &length, &present, &valid, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(out, 0, sizeof(*out));
  out->present = present;
  if (!present || !valid) {
    lc_free_with_allocator(&pouch->allocator, bytes);
    return LC_OK;
  }
  fp = fmemopen(bytes, length, "rb");
  if (fp == NULL) {
    lc_free_with_allocator(&pouch->allocator, bytes);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to open encrypted pouch query-index memory",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_query_index_read_with_reader_fp(
      fp, out, reader, term_reader, presence_reader, error);
  if (fclose(fp) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close encrypted pouch query-index memory",
                      strerror(errno), NULL, NULL);
  }
  lc_free_with_allocator(&pouch->allocator, bytes);
  return rc;
}

static int lc_pouch_query_index_read_header_fp(
    FILE *fp, lc_pouch_query_index_read_result *out, lc_error *error) {
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
  int matched;
  int rc;

  if (fp == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index header read requires a stream and "
                        "output",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
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
      lc_pouch_query_index_parse_header_line(line, "summary_hash", &row_hash)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_count", &term_count)) {
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
  if (matched == 13 && strcmp(format, LC_POUCH_QUERY_INDEX_FORMAT) == 0 &&
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
    out->valid = 1;
  }
  return rc;
}

static int lc_pouch_query_index_read_header_file(
    const char *path, lc_pouch_query_index_read_result *out, lc_error *error) {
  FILE *fp;
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
  rc = lc_pouch_query_index_read_header_fp(fp, out, error);
  if (fclose(fp) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch query-index sidecar",
                        strerror(errno), NULL, NULL);
  }
  return rc;
}

static int lc_pouch_query_index_source_header_line(lc_source *source,
                                                   char *line,
                                                   size_t line_size,
                                                   lc_error *error) {
  size_t length;

  if (source == NULL || line == NULL || line_size == 0U) {
    return 0;
  }
  length = 0U;
  for (;;) {
    unsigned char ch;
    size_t got;

    got = source->read(source, &ch, 1U, error);
    if (got == 0U) {
      if (error != NULL && error->code != LC_OK) {
        return 0;
      }
      if (length == 0U) {
        return 0;
      }
      line[length] = '\0';
      return 1;
    }
    if (length + 1U >= line_size) {
      if (error != NULL) {
        lc_error_set(error, LC_ERR_INVALID, 0L,
                     "pouch query-index header line exceeds local limit", NULL,
                     NULL, "pouch");
      }
      return 0;
    }
    line[length++] = (char)ch;
    if (ch == '\n') {
      line[length] = '\0';
      return 1;
    }
  }
}

static int lc_pouch_query_index_read_header_source(
    lc_source *source, lc_pouch_query_index_read_result *out,
    lc_error *error) {
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
  int matched;

  if (source == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index header read requires source and "
                        "output",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
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
  matched = 0;
  if (lc_pouch_query_index_source_header_line(source, line, sizeof(line),
                                             error) &&
      sscanf(line, "format=%63s\n", format) == 1) {
    ++matched;
  }
  if (lc_pouch_query_index_source_header_line(source, line, sizeof(line),
                                             error) &&
      lc_pouch_query_index_parse_header_line(line, "version", &version)) {
    ++matched;
  }
  if (lc_pouch_query_index_source_header_line(source, line, sizeof(line),
                                             error) &&
      lc_pouch_query_index_parse_header_line(line, "state_index_seq",
                                             &parsed_seq)) {
    ++matched;
  }
  if (lc_pouch_query_index_source_header_line(source, line, sizeof(line),
                                             error) &&
      lc_pouch_query_index_parse_header_line(line, "row_count", &row_count)) {
    ++matched;
  }
  if (lc_pouch_query_index_source_header_line(source, line, sizeof(line),
                                             error) &&
      lc_pouch_query_index_parse_header_line(line, "summary_hash", &row_hash)) {
    ++matched;
  }
  if (lc_pouch_query_index_source_header_line(source, line, sizeof(line),
                                             error) &&
      lc_pouch_query_index_parse_header_line(line, "term_count", &term_count)) {
    ++matched;
  }
  if (lc_pouch_query_index_source_header_line(source, line, sizeof(line),
                                             error) &&
      lc_pouch_query_index_parse_header_line(line, "term_hash", &term_hash)) {
    ++matched;
  }
  if (lc_pouch_query_index_source_header_line(source, line, sizeof(line),
                                             error) &&
      lc_pouch_query_index_parse_header_line(line, "term_index_complete",
                                             &term_index_complete) &&
      term_index_complete <= 1UL) {
    ++matched;
  }
  if (lc_pouch_query_index_source_header_line(source, line, sizeof(line),
                                             error) &&
      lc_pouch_query_index_parse_header_line(line, "presence_count",
                                             &presence_count)) {
    ++matched;
  }
  if (lc_pouch_query_index_source_header_line(source, line, sizeof(line),
                                             error) &&
      lc_pouch_query_index_parse_header_line(line, "presence_hash",
                                             &presence_hash)) {
    ++matched;
  }
  if (lc_pouch_query_index_source_header_line(source, line, sizeof(line),
                                             error) &&
      lc_pouch_query_index_parse_header_line(line, "presence_index_complete",
                                             &presence_index_complete) &&
      presence_index_complete <= 1UL) {
    ++matched;
  }
  if (lc_pouch_query_index_source_header_line(source, line, sizeof(line),
                                             error) &&
      lc_pouch_query_index_parse_header_line(line, "term_field_count",
                                             &term_field_count)) {
    ++matched;
  }
  if (lc_pouch_query_index_source_header_line(source, line, sizeof(line),
                                             error) &&
      lc_pouch_query_index_parse_header_line(line, "term_value_count",
                                             &term_value_count)) {
    ++matched;
  }
  if (error != NULL && error->code != LC_OK) {
    return error->code;
  }
  out->present = 1;
  if (matched == 13 && strcmp(format, LC_POUCH_QUERY_INDEX_FORMAT) == 0 &&
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
    out->valid = 1;
  }
  return LC_OK;
}

static int lc_pouch_query_index_read_header(
    lc_pouch *pouch, const char *namespace_name, const char *path,
    lc_pouch_query_index_read_result *out, lc_error *error) {
  lc_source *source;
  int present;
  int valid;
  int rc;

  if (!lc_pouch_crypto_enabled(pouch != NULL ? pouch->crypto : NULL)) {
    return lc_pouch_query_index_read_header_file(path, out, error);
  }
  source = NULL;
  present = 0;
  valid = 0;
  rc = lc_pouch_query_index_open_encrypted_artifact_source(
      pouch, namespace_name, path, 0, &source, &present, &valid, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(out, 0, sizeof(*out));
  out->present = present;
  if (!present || !valid) {
    return LC_OK;
  }
  rc = lc_pouch_query_index_read_header_source(source, out, error);
  if (source != NULL) {
    source->close(source);
  }
  if (rc != LC_OK && rc != LC_ERR_NOMEM) {
    if (error != NULL) {
      lc_error_cleanup(error);
    }
    memset(out, 0, sizeof(*out));
    out->present = present;
    return LC_OK;
  }
  return rc;
}

static int lc_pouch_query_index_exact_generation_flush_posting(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    unsigned long term_id, const lc_pouch_index_docid_set *docids,
    lc_error *error) {
  if (term_id == 0UL || docids == NULL || docids->count == 0U) {
    return LC_OK;
  }
  return lc_pouch_index_term_posting_table_put(allocator, &generation->postings,
                                               term_id, docids->items,
                                               docids->count, error);
}

static int lc_pouch_query_index_docid_compare(const void *left,
                                              const void *right) {
  unsigned long left_value;
  unsigned long right_value;

  left_value = *(const unsigned long *)left;
  right_value = *(const unsigned long *)right;
  if (left_value < right_value) {
    return -1;
  }
  if (left_value > right_value) {
    return 1;
  }
  return 0;
}

static void lc_pouch_query_index_exact_generation_accumulator_cleanup(
    const lc_allocator *allocator,
    lc_pouch_query_index_exact_generation_accumulator *accumulator) {
  size_t index;

  if (accumulator == NULL) {
    return;
  }
  for (index = 0U; index < accumulator->count; ++index) {
    lc_pouch_index_docid_set_cleanup(allocator,
                                     &accumulator->items[index].docids);
  }
  lc_free_with_allocator(allocator, accumulator->items);
  memset(accumulator, 0, sizeof(*accumulator));
}

static int lc_pouch_query_index_exact_generation_accumulator_reserve(
    const lc_allocator *allocator,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    size_t needed, lc_error *error) {
  lc_pouch_query_index_exact_generation_docids *next_items;
  size_t next_capacity;

  if (accumulator == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch exact generation accumulator requires "
                        "accumulator",
                        NULL, NULL, NULL);
  }
  if (needed <= accumulator->capacity) {
    return LC_OK;
  }
  next_capacity = accumulator->capacity == 0U ? 16U : accumulator->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch exact generation accumulator exceeds local "
                          "limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_items =
      (lc_pouch_query_index_exact_generation_docids *)lc_alloc_with_allocator(
          allocator, next_capacity * sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch exact generation "
                        "accumulator",
                        NULL, NULL, NULL);
  }
  if (accumulator->items != NULL) {
    memcpy(next_items, accumulator->items,
           accumulator->count * sizeof(next_items[0]));
    lc_free_with_allocator(allocator, accumulator->items);
  }
  memset(next_items + accumulator->count, 0,
         (next_capacity - accumulator->count) * sizeof(next_items[0]));
  accumulator->items = next_items;
  accumulator->capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_query_index_exact_generation_accumulator_find(
    const lc_pouch_query_index_exact_generation_accumulator *accumulator,
    unsigned long term_id, size_t *position_out) {
  size_t low;
  size_t high;
  size_t mid;

  if (position_out != NULL) {
    *position_out = 0U;
  }
  if (accumulator == NULL || term_id == 0UL) {
    return 0;
  }
  low = 0U;
  high = accumulator->count;
  while (low < high) {
    mid = low + ((high - low) / 2U);
    if (accumulator->items[mid].term_id < term_id) {
      low = mid + 1U;
    } else {
      high = mid;
    }
  }
  if (position_out != NULL) {
    *position_out = low;
  }
  return low < accumulator->count && accumulator->items[low].term_id == term_id;
}

static int lc_pouch_query_index_exact_generation_accumulator_append(
    const lc_allocator *allocator,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    unsigned long term_id, unsigned long doc_id, lc_error *error) {
  size_t position;
  int added;
  int rc;

  if (term_id == 0UL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch exact generation accumulator requires term id",
                        NULL, NULL, "pouch");
  }
  if (!lc_pouch_query_index_exact_generation_accumulator_find(
          accumulator, term_id, &position)) {
    rc = lc_pouch_query_index_exact_generation_accumulator_reserve(
        allocator, accumulator, accumulator->count + 1U, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (position < accumulator->count) {
      memmove(&accumulator->items[position + 1U], &accumulator->items[position],
              (accumulator->count - position) * sizeof(accumulator->items[0]));
      memset(&accumulator->items[position], 0,
             sizeof(accumulator->items[position]));
    }
    accumulator->items[position].term_id = term_id;
    ++accumulator->count;
  }
  return lc_pouch_index_docid_set_append_unique(
      &accumulator->items[position].docids, doc_id, &added, allocator, error);
}

static char *
lc_pouch_query_index_trigram_value_hex(const lc_allocator *allocator,
                                       const unsigned char *bytes,
                                       int ignore_case) {
  char gram[4];

  if (bytes == NULL) {
    return NULL;
  }
  gram[0] = ignore_case ? 'i' : 'c';
  gram[1] = ignore_case ? (char)lc_pouch_query_index_ascii_lower(bytes[0])
                        : (char)bytes[0];
  gram[2] = ignore_case ? (char)lc_pouch_query_index_ascii_lower(bytes[1])
                        : (char)bytes[1];
  gram[3] = ignore_case ? (char)lc_pouch_query_index_ascii_lower(bytes[2])
                        : (char)bytes[2];
  return lc_pouch_query_index_hex_encode_bytes(allocator, gram, sizeof(gram));
}

static int lc_pouch_query_index_trigram_generation_add_value(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    const char *field_hex, const char *value_hex, unsigned long doc_id,
    lc_error *error) {
  (void)allocator;
  (void)generation;
  (void)accumulator;
  (void)field_hex;
  (void)value_hex;
  (void)doc_id;
  (void)error;
  return LC_OK;
}

static char *lc_pouch_query_index_temporal_instant_value_hex(
    const lc_allocator *allocator, const lc_pouch_index_instant *instant,
    lc_error *error) {
  char value[96];
  int written;

  if (instant == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch temporal generation value requires instant", NULL, NULL,
                 NULL);
    return NULL;
  }
  written = snprintf(value, sizeof(value), "%.17g", instant->seconds);
  if (written < 0 || (size_t)written >= sizeof(value)) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch temporal generation value exceeds local limit", NULL,
                 NULL, "pouch");
    return NULL;
  }
  return lc_pouch_query_index_hex_encode(allocator, value);
}

static int lc_pouch_query_index_exact_generation_accumulator_flush(
    const lc_allocator *allocator,
    const lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_index_term_generation *generation, lc_error *error) {
  lc_pouch_index_docid_set sorted;
  size_t index;
  size_t read_index;
  size_t write_index;
  int rc;

  if (accumulator == NULL || generation == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch exact generation flush requires accumulator "
                        "and generation",
                        NULL, NULL, NULL);
  }
  memset(&sorted, 0, sizeof(sorted));
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < accumulator->count; ++index) {
    lc_pouch_index_docid_set_cleanup(allocator, &sorted);
    sorted.items = (unsigned long *)lc_alloc_with_allocator(
        allocator,
        accumulator->items[index].docids.count * sizeof(sorted.items[0]));
    if (sorted.items == NULL && accumulator->items[index].docids.count > 0U) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate sorted pouch exact generation "
                        "docIDs",
                        NULL, NULL, NULL);
      break;
    }
    sorted.count = accumulator->items[index].docids.count;
    sorted.capacity = sorted.count;
    if (sorted.count > 0U) {
      memcpy(sorted.items, accumulator->items[index].docids.items,
             sorted.count * sizeof(sorted.items[0]));
      qsort(sorted.items, sorted.count, sizeof(sorted.items[0]),
            lc_pouch_query_index_docid_compare);
      write_index = 0U;
      for (read_index = 0U; read_index < sorted.count; ++read_index) {
        if (write_index > 0U &&
            sorted.items[write_index - 1U] == sorted.items[read_index]) {
          continue;
        }
        sorted.items[write_index++] = sorted.items[read_index];
      }
      sorted.count = write_index;
    }
    rc = lc_pouch_query_index_exact_generation_flush_posting(
        allocator, generation, accumulator->items[index].term_id, &sorted,
        error);
  }
  lc_pouch_index_docid_set_cleanup(allocator, &sorted);
  return rc;
}

static int lc_pouch_query_index_build_text(
    unsigned long index_seq, lc_pouch_query_index_summary *summary,
    lc_pouch_query_index_text *out, unsigned long *row_hash_out,
    unsigned long *term_count_out, unsigned long *term_hash_out,
    unsigned long *term_field_count_out, unsigned long *term_value_count_out,
    unsigned long *presence_count_out, unsigned long *presence_hash_out,
    char **doc_table_generation_out, size_t *doc_table_generation_length_out,
    char **exact_term_generation_out, size_t *exact_term_generation_length_out,
    char **presence_term_generation_out,
    size_t *presence_term_generation_length_out,
    char **range_term_generation_out, size_t *range_term_generation_length_out,
    char **text_term_generation_out, size_t *text_term_generation_length_out,
    char **trigram_term_generation_out,
    size_t *trigram_term_generation_length_out,
    char **temporal_term_generation_out,
    size_t *temporal_term_generation_length_out, lc_error *error) {
  lc_pouch_query_index_text header;
  lc_pouch_query_index_text rows;
  lc_pouch_query_index_text term_fields;
  lc_pouch_query_index_text term_values;
  lc_pouch_query_index_text terms;
  lc_pouch_query_index_text presences;
  lc_pouch_index_doc_table doc_table;
  lc_pouch_index_term_generation exact_generation;
  lc_pouch_index_term_generation presence_generation;
  lc_pouch_index_term_generation range_generation;
  lc_pouch_index_term_generation text_generation;
  lc_pouch_index_term_generation trigram_generation;
  lc_pouch_index_term_generation temporal_generation;
  lc_pouch_query_index_exact_generation_accumulator exact_accumulator;
  lc_pouch_query_index_exact_generation_accumulator presence_accumulator;
  lc_pouch_query_index_exact_generation_accumulator range_accumulator;
  lc_pouch_query_index_exact_generation_accumulator text_accumulator;
  lc_pouch_query_index_exact_generation_accumulator trigram_accumulator;
  lc_pouch_query_index_exact_generation_accumulator temporal_accumulator;
  char line[256];
  unsigned long row_hash;
  unsigned long term_hash;
  unsigned long term_line_count;
  unsigned long term_field_line_count;
  unsigned long term_value_line_count;
  unsigned long current_field_first;
  unsigned long current_field_count;
  unsigned long current_field_first_byte;
  unsigned long current_value_first;
  unsigned long current_value_count;
  unsigned long current_value_first_byte;
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
      presence_count_out == NULL || presence_hash_out == NULL ||
      doc_table_generation_out == NULL ||
      doc_table_generation_length_out == NULL ||
      exact_term_generation_out == NULL ||
      exact_term_generation_length_out == NULL ||
      presence_term_generation_out == NULL ||
      presence_term_generation_length_out == NULL ||
      range_term_generation_out == NULL ||
      range_term_generation_length_out == NULL ||
      text_term_generation_out == NULL ||
      text_term_generation_length_out == NULL ||
      trigram_term_generation_out == NULL ||
      trigram_term_generation_length_out == NULL ||
      temporal_term_generation_out == NULL ||
      temporal_term_generation_length_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index build requires summary outputs",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  out->allocator = summary->allocator;
  *doc_table_generation_out = NULL;
  *doc_table_generation_length_out = 0U;
  *exact_term_generation_out = NULL;
  *exact_term_generation_length_out = 0U;
  *presence_term_generation_out = NULL;
  *presence_term_generation_length_out = 0U;
  *range_term_generation_out = NULL;
  *range_term_generation_length_out = 0U;
  *text_term_generation_out = NULL;
  *text_term_generation_length_out = 0U;
  *trigram_term_generation_out = NULL;
  *trigram_term_generation_length_out = 0U;
  *temporal_term_generation_out = NULL;
  *temporal_term_generation_length_out = 0U;
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
  memset(&exact_generation, 0, sizeof(exact_generation));
  memset(&presence_generation, 0, sizeof(presence_generation));
  memset(&range_generation, 0, sizeof(range_generation));
  memset(&text_generation, 0, sizeof(text_generation));
  memset(&trigram_generation, 0, sizeof(trigram_generation));
  memset(&temporal_generation, 0, sizeof(temporal_generation));
  memset(&exact_accumulator, 0, sizeof(exact_accumulator));
  memset(&presence_accumulator, 0, sizeof(presence_accumulator));
  memset(&range_accumulator, 0, sizeof(range_accumulator));
  memset(&text_accumulator, 0, sizeof(text_accumulator));
  memset(&trigram_accumulator, 0, sizeof(trigram_accumulator));
  memset(&temporal_accumulator, 0, sizeof(temporal_accumulator));
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
  current_field_first_byte = 0UL;
  current_value_field_hex = NULL;
  current_value_hex = NULL;
  current_value_first = 0UL;
  current_value_count = 0UL;
  current_value_first_byte = 0UL;
  presence_hash = lc_pouch_query_index_hash_init();
  presence_line_count = 0UL;
  rc = LC_OK;
  exact_generation.namespace_name =
      lc_strdup_with_allocator(summary->allocator, summary->namespace_name);
  if (exact_generation.namespace_name == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch exact generation namespace",
                      NULL, NULL, NULL);
  }
  if (rc == LC_OK) {
    presence_generation.namespace_name =
        lc_strdup_with_allocator(summary->allocator, summary->namespace_name);
    if (presence_generation.namespace_name == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch presence generation "
                        "namespace",
                        NULL, NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    range_generation.namespace_name =
        lc_strdup_with_allocator(summary->allocator, summary->namespace_name);
    if (range_generation.namespace_name == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch range generation namespace",
                        NULL, NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    text_generation.namespace_name =
        lc_strdup_with_allocator(summary->allocator, summary->namespace_name);
    if (text_generation.namespace_name == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch text generation namespace",
                        NULL, NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    temporal_generation.namespace_name =
        lc_strdup_with_allocator(summary->allocator, summary->namespace_name);
    if (temporal_generation.namespace_name == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch temporal generation "
                        "namespace",
                        NULL, NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    trigram_generation.namespace_name =
        lc_strdup_with_allocator(summary->allocator, summary->namespace_name);
    if (trigram_generation.namespace_name == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch trigram generation "
                        "namespace",
                        NULL, NULL, NULL);
    }
  }
  for (index = 0U; rc == LC_OK && index < summary->count; ++index) {
    lc_pouch_query_index_row *row;

    row = &summary->rows[index];
    written = snprintf(line, sizeof(line), "row %lu %lu %d %d ", row->version,
                       row->bytes, row->has_query_hidden ? 1 : 0,
                       row->query_hidden ? 1 : 0);
    if (written < 0 || (size_t)written >= sizeof(line)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index row exceeds local limit", NULL, NULL,
                        NULL);
      break;
    }
    rc = lc_pouch_query_index_text_append(&rows, line, (size_t)written,
                                          &row_hash, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(&rows, row->key_hex, &row_hash,
                                                 error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(&rows, " ", &row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(&rows, row->content_type_hex,
                                                 &row_hash, error);
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
          row->has_query_hidden, row->query_hidden, &doc_id, summary->allocator,
          error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(&rows, "\n", &row_hash, error);
    }
  }
  for (index = 0U; rc == LC_OK && index < summary->term_count; ++index) {
    lc_pouch_query_index_term *term;

    term = &summary->terms[index];
    if (index > 0U &&
        lc_pouch_query_index_term_equal(&summary->terms[index - 1U], term)) {
      continue;
    }
    rc = lc_pouch_index_doc_table_find_key_hex(&doc_table, term->key_hex,
                                               &term->doc_id, &found, error);
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
    if (rc == LC_OK) {
      char *exact_value_hex;
      unsigned long exact_term_id;

      exact_value_hex = NULL;
      exact_term_id = 0UL;
      rc = lc_pouch_query_index_exact_generation_value_hex(
          summary->allocator, term->value_hex, term->value_type,
          &exact_value_hex, error);
      if (rc == LC_OK) {
        rc = lc_pouch_index_term_table_find_or_add(
            summary->allocator, &exact_generation.terms, term->field_hex,
            exact_value_hex, term->value_type, &exact_term_id, error);
      }
      if (rc == LC_OK) {
        rc = lc_pouch_query_index_exact_generation_accumulator_append(
            summary->allocator, &exact_accumulator, exact_term_id, term->doc_id,
            error);
      }
      if (rc == LC_OK && term->value_type == 'n') {
        unsigned long range_term_id;

        range_term_id = 0UL;
        rc = lc_pouch_index_term_table_find_or_add(
            summary->allocator, &range_generation.terms, term->field_hex,
            exact_value_hex, term->value_type, &range_term_id, error);
        if (rc == LC_OK) {
          rc = lc_pouch_query_index_exact_generation_accumulator_append(
              summary->allocator, &range_accumulator, range_term_id,
              term->doc_id, error);
        }
      }
      if (rc == LC_OK && term->value_type == 's') {
        unsigned long text_term_id;
        char *date_text;
        lc_pouch_index_instant ignored_instant;

        text_term_id = 0UL;
        rc = lc_pouch_index_term_table_find_or_add(
            summary->allocator, &text_generation.terms, term->field_hex,
            exact_value_hex, term->value_type, &text_term_id, error);
        if (rc == LC_OK) {
          rc = lc_pouch_query_index_exact_generation_accumulator_append(
              summary->allocator, &text_accumulator, text_term_id, term->doc_id,
              error);
        }
        if (rc == LC_OK) {
          rc = lc_pouch_query_index_trigram_generation_add_value(
              summary->allocator, &trigram_generation, &trigram_accumulator,
              term->field_hex, exact_value_hex, term->doc_id, error);
        }
        date_text = NULL;
        if (rc == LC_OK) {
          date_text = lc_pouch_query_index_hex_decode(summary->allocator,
                                                      exact_value_hex, error);
          if (date_text == NULL) {
            rc = error != NULL && error->code != LC_OK ? error->code
                                                       : LC_ERR_NOMEM;
          }
        }
        if (rc == LC_OK &&
            lc_pouch_index_parse_lql_datetime(date_text, &ignored_instant)) {
          char *temporal_value_hex;
          unsigned long temporal_term_id;

          temporal_value_hex = lc_pouch_query_index_temporal_instant_value_hex(
              summary->allocator, &ignored_instant, error);
          if (temporal_value_hex == NULL) {
            rc = error != NULL && error->code != LC_OK ? error->code
                                                       : LC_ERR_NOMEM;
          }
          temporal_term_id = 0UL;
          if (rc == LC_OK) {
            rc = lc_pouch_index_term_table_find_or_add(
                summary->allocator, &temporal_generation.terms, term->field_hex,
                temporal_value_hex, 'n', &temporal_term_id, error);
          }
          if (rc == LC_OK) {
            rc = lc_pouch_query_index_exact_generation_accumulator_append(
                summary->allocator, &temporal_accumulator, temporal_term_id,
                term->doc_id, error);
          }
          lc_free_with_allocator(summary->allocator, temporal_value_hex);
        }
        lc_free_with_allocator(summary->allocator, date_text);
      }
      lc_free_with_allocator(summary->allocator, exact_value_hex);
    }
    if (rc != LC_OK) {
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
          written = snprintf(
              line, sizeof(line), " %lu %lu %lu %lu\n", current_field_first,
              current_field_count, current_field_first_byte,
              (unsigned long)terms.length - current_field_first_byte);
          if (written < 0 || (size_t)written >= sizeof(line)) {
            rc =
                lc_error_set(error, LC_ERR_INVALID, 0L,
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
      current_field_hex = term->field_hex;
      current_field_first = term_line_count;
      current_field_count = 0UL;
      current_field_first_byte = (unsigned long)terms.length;
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
          written = snprintf(
              line, sizeof(line), " %lu %lu %lu %lu\n", current_value_first,
              current_value_count, current_value_first_byte,
              (unsigned long)terms.length - current_value_first_byte);
          if (written < 0 || (size_t)written >= sizeof(line)) {
            rc =
                lc_error_set(error, LC_ERR_INVALID, 0L,
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
      current_value_field_hex = term->field_hex;
      current_value_hex = term->value_hex;
      current_value_first = term_line_count;
      current_value_count = 0UL;
      current_value_first_byte = (unsigned long)terms.length;
    }
    written =
        snprintf(line, sizeof(line), "term %lu %lu %d %d %lu ", term->version,
                 term->bytes, term->has_query_hidden ? 1 : 0,
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
      rc =
          lc_pouch_query_index_text_append_cstr(&terms, " ", &term_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(&terms, term->value_hex,
                                                 &term_hash, error);
    }
    if (rc == LC_OK) {
      rc =
          lc_pouch_query_index_text_append_cstr(&terms, " ", &term_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(&terms, term->key_hex,
                                                 &term_hash, error);
    }
    if (rc == LC_OK) {
      rc =
          lc_pouch_query_index_text_append_cstr(&terms, " ", &term_hash, error);
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
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        summary->allocator, &exact_accumulator, &exact_generation, error);
  }
  if (rc == LC_OK && current_value_field_hex != NULL) {
    rc = lc_pouch_query_index_text_append_cstr(&term_values, "term_value ",
                                               NULL, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(
          &term_values, current_value_field_hex, NULL, error);
    }
    if (rc == LC_OK) {
      rc =
          lc_pouch_query_index_text_append_cstr(&term_values, " ", NULL, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(
          &term_values, current_value_hex, NULL, error);
    }
    if (rc == LC_OK) {
      written = snprintf(
          line, sizeof(line), " %lu %lu %lu %lu\n", current_value_first,
          current_value_count, current_value_first_byte,
          (unsigned long)terms.length - current_value_first_byte);
      if (written < 0 || (size_t)written >= sizeof(line)) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query-index term value exceeds local limit",
                          NULL, NULL, NULL);
      }
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append(&term_values, line, (size_t)written,
                                            NULL, error);
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
      written = snprintf(
          line, sizeof(line), " %lu %lu %lu %lu\n", current_field_first,
          current_field_count, current_field_first_byte,
          (unsigned long)terms.length - current_field_first_byte);
      if (written < 0 || (size_t)written >= sizeof(line)) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query-index term field exceeds local limit",
                          NULL, NULL, NULL);
      }
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append(&term_fields, line, (size_t)written,
                                            NULL, error);
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
    rc = lc_pouch_index_doc_table_find_key_hex(&doc_table, presence->key_hex,
                                               &doc_id, &found, error);
    if (rc != LC_OK) {
      break;
    }
    if (!found) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index presence references missing doc "
                        "table entry",
                        NULL, NULL, NULL);
      break;
    }
    if (rc == LC_OK) {
      unsigned long presence_term_id;

      presence_term_id = 0UL;
      rc = lc_pouch_index_term_table_find_or_add(
          summary->allocator, &presence_generation.terms, presence->field_hex,
          "-", 'z', &presence_term_id, error);
      if (rc == LC_OK) {
        rc = lc_pouch_query_index_exact_generation_accumulator_append(
            summary->allocator, &presence_accumulator, presence_term_id, doc_id,
            error);
      }
    }
    if (rc != LC_OK) {
      break;
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
      rc = lc_pouch_query_index_text_append_cstr(&presences, presence->key_hex,
                                                 &presence_hash, error);
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
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        summary->allocator, &presence_accumulator, &presence_generation, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        summary->allocator, &range_accumulator, &range_generation, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        summary->allocator, &text_accumulator, &text_generation, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        summary->allocator, &trigram_accumulator, &trigram_generation, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        summary->allocator, &temporal_accumulator, &temporal_generation, error);
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
    rc = lc_pouch_query_index_text_append_ulong_line(&header, "state_index_seq",
                                                     index_seq, error);
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
    rc = lc_pouch_query_index_text_append_ulong_line(&header, "presence_hash",
                                                     presence_hash, error);
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
    rc = lc_pouch_index_doc_table_generation_encode(
        &doc_table, index_seq, row_hash, summary->allocator,
        doc_table_generation_out, doc_table_generation_length_out, error);
  }
  if (rc == LC_OK) {
    exact_generation.index_seq = index_seq;
    exact_generation.row_count = (unsigned long)summary->count;
    exact_generation.row_hash = row_hash;
    rc = lc_pouch_index_term_generation_encode(
        &exact_generation, summary->allocator, exact_term_generation_out,
        exact_term_generation_length_out, error);
  }
  if (rc == LC_OK) {
    presence_generation.index_seq = index_seq;
    presence_generation.row_count = (unsigned long)summary->count;
    presence_generation.row_hash = row_hash;
    rc = lc_pouch_index_term_generation_encode(
        &presence_generation, summary->allocator, presence_term_generation_out,
        presence_term_generation_length_out, error);
  }
  if (rc == LC_OK) {
    range_generation.index_seq = index_seq;
    range_generation.row_count = (unsigned long)summary->count;
    range_generation.row_hash = row_hash;
    rc = lc_pouch_index_term_generation_encode(
        &range_generation, summary->allocator, range_term_generation_out,
        range_term_generation_length_out, error);
  }
  if (rc == LC_OK) {
    text_generation.index_seq = index_seq;
    text_generation.row_count = (unsigned long)summary->count;
    text_generation.row_hash = row_hash;
    rc = lc_pouch_index_term_generation_encode(
        &text_generation, summary->allocator, text_term_generation_out,
        text_term_generation_length_out, error);
  }
  if (rc == LC_OK) {
    trigram_generation.index_seq = index_seq;
    trigram_generation.row_count = (unsigned long)summary->count;
    trigram_generation.row_hash = row_hash;
    rc = lc_pouch_index_term_generation_encode(
        &trigram_generation, summary->allocator, trigram_term_generation_out,
        trigram_term_generation_length_out, error);
  }
  if (rc == LC_OK) {
    temporal_generation.index_seq = index_seq;
    temporal_generation.row_count = (unsigned long)summary->count;
    temporal_generation.row_hash = row_hash;
    rc = lc_pouch_index_term_generation_encode(
        &temporal_generation, summary->allocator, temporal_term_generation_out,
        temporal_term_generation_length_out, error);
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
  if (rc != LC_OK) {
    lc_free_with_allocator(summary->allocator, *doc_table_generation_out);
    *doc_table_generation_out = NULL;
    *doc_table_generation_length_out = 0U;
    lc_free_with_allocator(summary->allocator, *exact_term_generation_out);
    *exact_term_generation_out = NULL;
    *exact_term_generation_length_out = 0U;
    lc_free_with_allocator(summary->allocator, *presence_term_generation_out);
    *presence_term_generation_out = NULL;
    *presence_term_generation_length_out = 0U;
    lc_free_with_allocator(summary->allocator, *range_term_generation_out);
    *range_term_generation_out = NULL;
    *range_term_generation_length_out = 0U;
    lc_free_with_allocator(summary->allocator, *text_term_generation_out);
    *text_term_generation_out = NULL;
    *text_term_generation_length_out = 0U;
    lc_free_with_allocator(summary->allocator, *trigram_term_generation_out);
    *trigram_term_generation_out = NULL;
    *trigram_term_generation_length_out = 0U;
    lc_free_with_allocator(summary->allocator, *temporal_term_generation_out);
    *temporal_term_generation_out = NULL;
    *temporal_term_generation_length_out = 0U;
  }
  lc_pouch_query_index_exact_generation_accumulator_cleanup(summary->allocator,
                                                            &exact_accumulator);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(
      summary->allocator, &presence_accumulator);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(summary->allocator,
                                                            &range_accumulator);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(summary->allocator,
                                                            &text_accumulator);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(
      summary->allocator, &trigram_accumulator);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(
      summary->allocator, &temporal_accumulator);
  lc_pouch_index_term_generation_cleanup(summary->allocator, &exact_generation);
  lc_pouch_index_term_generation_cleanup(summary->allocator,
                                         &presence_generation);
  lc_pouch_index_term_generation_cleanup(summary->allocator, &range_generation);
  lc_pouch_index_term_generation_cleanup(summary->allocator, &text_generation);
  lc_pouch_index_term_generation_cleanup(summary->allocator,
                                         &trigram_generation);
  lc_pouch_index_term_generation_cleanup(summary->allocator,
                                         &temporal_generation);
  lc_pouch_index_doc_table_cleanup(summary->allocator, &doc_table);
  return rc;
}

static int lc_pouch_query_index_write_text(lc_pouch *pouch,
                                           const char *namespace_name,
                                           const char *path,
                                           lc_pouch_query_index_text *text,
                                           lc_error *error) {
  return lc_pouch_query_index_write_artifact_bytes(
      pouch, namespace_name, path, text != NULL && text->bytes != NULL
                                       ? text->bytes
                                       : "",
      text != NULL ? text->length : 0U, error);
}

int lc_pouch_query_index_flush(lc_pouch *pouch, const char *namespace_name,
                               unsigned long state_index_seq,
                               lc_pouch_query_index_flush_result *out,
                               lc_error *error) {
  lc_pouch_query_index_read_result sidecar;
  lc_pouch_query_index_read_result warm_sidecar;
  lc_pouch_query_index_summary summary;
  lc_pouch_query_index_extract_batch extract_batch;
  lc_pouch_query_index_prepared_text *prepared_text;
  lc_pouch_query_index_text text;
  const char **extract_keys;
  char *sidecar_path;
  char *doc_table_path;
  char *exact_term_path;
  char *presence_term_path;
  char *range_term_path;
  char *text_term_path;
  char *trigram_term_path;
  char *temporal_term_path;
  char *doc_table_generation;
  char *exact_term_generation;
  char *presence_term_generation;
  char *range_term_generation;
  char *text_term_generation;
  char *trigram_term_generation;
  char *temporal_term_generation;
  size_t doc_table_generation_length;
  size_t exact_term_generation_length;
  size_t presence_term_generation_length;
  size_t range_term_generation_length;
  size_t text_term_generation_length;
  size_t trigram_term_generation_length;
  size_t temporal_term_generation_length;
  size_t extract_index;
  unsigned long expected_hash;
  unsigned long expected_term_count;
  unsigned long expected_term_hash;
  unsigned long expected_term_field_count;
  unsigned long expected_term_value_count;
  unsigned long expected_presence_count;
  unsigned long expected_presence_hash;
  int doc_table_present;
  int doc_table_valid;
  int exact_term_present;
  int exact_term_valid;
  int presence_term_present;
  int presence_term_valid;
  int range_term_present;
  int range_term_valid;
  int text_term_present;
  int text_term_valid;
  int trigram_term_present;
  int trigram_term_valid;
  int temporal_term_present;
  int temporal_term_valid;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_query_index_flush requires pouch, "
                        "namespace, and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  memset(&warm_sidecar, 0, sizeof(warm_sidecar));
  memset(&summary, 0, sizeof(summary));
  memset(&extract_batch, 0, sizeof(extract_batch));
  memset(&text, 0, sizeof(text));
  prepared_text = NULL;
  extract_keys = NULL;
  doc_table_path = NULL;
  exact_term_path = NULL;
  presence_term_path = NULL;
  range_term_path = NULL;
  text_term_path = NULL;
  trigram_term_path = NULL;
  temporal_term_path = NULL;
  doc_table_generation = NULL;
  exact_term_generation = NULL;
  presence_term_generation = NULL;
  range_term_generation = NULL;
  text_term_generation = NULL;
  trigram_term_generation = NULL;
  temporal_term_generation = NULL;
  doc_table_generation_length = 0U;
  exact_term_generation_length = 0U;
  presence_term_generation_length = 0U;
  range_term_generation_length = 0U;
  text_term_generation_length = 0U;
  trigram_term_generation_length = 0U;
  temporal_term_generation_length = 0U;
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
  doc_table_path =
      lc_pouch_query_index_doc_table_path(pouch, namespace_name, error);
  if (doc_table_path == NULL) {
    lc_free_with_allocator(&pouch->allocator, sidecar_path);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  exact_term_path =
      lc_pouch_query_index_exact_term_path(pouch, namespace_name, error);
  if (exact_term_path == NULL) {
    lc_free_with_allocator(&pouch->allocator, doc_table_path);
    lc_free_with_allocator(&pouch->allocator, sidecar_path);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  presence_term_path =
      lc_pouch_query_index_presence_term_path(pouch, namespace_name, error);
  if (presence_term_path == NULL) {
    lc_free_with_allocator(&pouch->allocator, exact_term_path);
    lc_free_with_allocator(&pouch->allocator, doc_table_path);
    lc_free_with_allocator(&pouch->allocator, sidecar_path);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  range_term_path =
      lc_pouch_query_index_range_term_path(pouch, namespace_name, error);
  if (range_term_path == NULL) {
    lc_free_with_allocator(&pouch->allocator, presence_term_path);
    lc_free_with_allocator(&pouch->allocator, exact_term_path);
    lc_free_with_allocator(&pouch->allocator, doc_table_path);
    lc_free_with_allocator(&pouch->allocator, sidecar_path);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  text_term_path =
      lc_pouch_query_index_text_term_path(pouch, namespace_name, error);
  if (text_term_path == NULL) {
    lc_free_with_allocator(&pouch->allocator, range_term_path);
    lc_free_with_allocator(&pouch->allocator, presence_term_path);
    lc_free_with_allocator(&pouch->allocator, exact_term_path);
    lc_free_with_allocator(&pouch->allocator, doc_table_path);
    lc_free_with_allocator(&pouch->allocator, sidecar_path);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  temporal_term_path =
      lc_pouch_query_index_temporal_term_path(pouch, namespace_name, error);
  if (temporal_term_path == NULL) {
    lc_free_with_allocator(&pouch->allocator, text_term_path);
    lc_free_with_allocator(&pouch->allocator, range_term_path);
    lc_free_with_allocator(&pouch->allocator, presence_term_path);
    lc_free_with_allocator(&pouch->allocator, exact_term_path);
    lc_free_with_allocator(&pouch->allocator, doc_table_path);
    lc_free_with_allocator(&pouch->allocator, sidecar_path);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  trigram_term_path =
      lc_pouch_query_index_trigram_term_path(pouch, namespace_name, error);
  if (trigram_term_path == NULL) {
    lc_free_with_allocator(&pouch->allocator, temporal_term_path);
    lc_free_with_allocator(&pouch->allocator, text_term_path);
    lc_free_with_allocator(&pouch->allocator, range_term_path);
    lc_free_with_allocator(&pouch->allocator, presence_term_path);
    lc_free_with_allocator(&pouch->allocator, exact_term_path);
    lc_free_with_allocator(&pouch->allocator, doc_table_path);
    lc_free_with_allocator(&pouch->allocator, sidecar_path);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_query_index_read_header(pouch, namespace_name, sidecar_path,
                                        &sidecar, error);
  if (rc == LC_OK && sidecar.present && sidecar.valid &&
      sidecar.index_seq == state_index_seq) {
    doc_table_present = 0;
    doc_table_valid = 0;
    rc = lc_pouch_query_index_doc_table_generation_validate_artifact(
        pouch, namespace_name, doc_table_path, sidecar.index_seq,
        sidecar.row_count, sidecar.row_hash, &doc_table_present,
        &doc_table_valid, error);
    exact_term_present = 0;
    exact_term_valid = 0;
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_term_generation_validate_artifact(
          pouch, namespace_name, exact_term_path, sidecar.index_seq,
          sidecar.row_count, sidecar.row_hash, &exact_term_present,
          &exact_term_valid, error);
    }
    presence_term_present = 0;
    presence_term_valid = 0;
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_term_generation_validate_artifact(
          pouch, namespace_name, presence_term_path, sidecar.index_seq,
          sidecar.row_count, sidecar.row_hash, &presence_term_present,
          &presence_term_valid, error);
    }
    range_term_present = 0;
    range_term_valid = 0;
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_term_generation_validate_artifact(
          pouch, namespace_name, range_term_path, sidecar.index_seq,
          sidecar.row_count, sidecar.row_hash, &range_term_present,
          &range_term_valid, error);
    }
    text_term_present = 0;
    text_term_valid = 0;
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_term_generation_validate_artifact(
          pouch, namespace_name, text_term_path, sidecar.index_seq,
          sidecar.row_count, sidecar.row_hash, &text_term_present,
          &text_term_valid, error);
    }
    temporal_term_present = 0;
    temporal_term_valid = 0;
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_term_generation_validate_artifact(
          pouch, namespace_name, temporal_term_path, sidecar.index_seq,
          sidecar.row_count, sidecar.row_hash, &temporal_term_present,
          &temporal_term_valid, error);
    }
    trigram_term_present = 0;
    trigram_term_valid = 0;
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_term_generation_validate_artifact(
          pouch, namespace_name, trigram_term_path, sidecar.index_seq,
          sidecar.row_count, sidecar.row_hash, &trigram_term_present,
          &trigram_term_valid, error);
    }
    if (rc == LC_OK && doc_table_present && doc_table_valid &&
        exact_term_present && exact_term_valid && presence_term_present &&
        presence_term_valid && range_term_present && range_term_valid &&
        text_term_present && text_term_valid && temporal_term_present &&
        temporal_term_valid && trigram_term_present && trigram_term_valid) {
      rc = lc_pouch_query_index_prepare_text_reader(
          pouch, namespace_name, &sidecar, &prepared_text, error);
    }
    if (rc == LC_OK && doc_table_present && doc_table_valid &&
        exact_term_present && exact_term_valid && presence_term_present &&
        presence_term_valid && range_term_present && range_term_valid &&
        text_term_present && text_term_valid && temporal_term_present &&
        temporal_term_valid && trigram_term_present && trigram_term_valid) {
      out->index_seq = state_index_seq;
      lc_free_with_allocator(&pouch->allocator, trigram_term_path);
      lc_free_with_allocator(&pouch->allocator, temporal_term_path);
      lc_free_with_allocator(&pouch->allocator, text_term_path);
      lc_free_with_allocator(&pouch->allocator, range_term_path);
      lc_free_with_allocator(&pouch->allocator, presence_term_path);
      lc_free_with_allocator(&pouch->allocator, exact_term_path);
      lc_free_with_allocator(&pouch->allocator, doc_table_path);
      lc_free_with_allocator(&pouch->allocator, sidecar_path);
      return LC_OK;
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_visit(pouch, namespace_name,
                              lc_pouch_query_index_summary_visit, &summary,
                              error);
  }
  if (rc == LC_OK && summary.count > 0U) {
    extract_keys = (const char **)lc_calloc_with_allocator(
        &pouch->allocator, summary.count, sizeof(*extract_keys));
    if (extract_keys == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index extraction keys",
                        NULL, NULL, NULL);
    }
  }
  if (rc == LC_OK && summary.count > 0U) {
    for (extract_index = 0U; extract_index < summary.count; ++extract_index) {
      extract_keys[extract_index] = summary.rows[extract_index].key;
    }
    extract_batch.summary = &summary;
    extract_batch.index = 0U;
    rc = lc_pouch_state_read_many(
        pouch, namespace_name, extract_keys, summary.count,
        lc_pouch_query_index_extract_batch_visit, &extract_batch, error);
  }
  lc_free_with_allocator(&pouch->allocator, extract_keys);
  extract_keys = NULL;
  expected_hash = lc_pouch_query_index_hash_init();
  expected_term_count = 0UL;
  expected_term_hash = lc_pouch_query_index_hash_init();
  expected_term_field_count = 0UL;
  expected_term_value_count = 0UL;
  expected_presence_count = 0UL;
  expected_presence_hash = lc_pouch_query_index_hash_init();
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_build_text(
        state_index_seq, &summary, &text, &expected_hash, &expected_term_count,
        &expected_term_hash, &expected_term_field_count,
        &expected_term_value_count, &expected_presence_count,
        &expected_presence_hash, &doc_table_generation,
        &doc_table_generation_length, &exact_term_generation,
        &exact_term_generation_length, &presence_term_generation,
        &presence_term_generation_length, &range_term_generation,
        &range_term_generation_length, &text_term_generation,
        &text_term_generation_length, &trigram_term_generation,
        &trigram_term_generation_length, &temporal_term_generation,
        &temporal_term_generation_length, error);
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
       sidecar.presence_index_complete != summary.presence_index_complete)) {
    rc = lc_pouch_query_index_write_text(pouch, namespace_name, sidecar_path,
                                         &text, error);
    out->repaired = 1;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_write_artifact_bytes(
        pouch, namespace_name, doc_table_path,
        doc_table_generation != NULL ? doc_table_generation : "",
        doc_table_generation_length, error);
    if (rc == LC_OK) {
      out->repaired = 1;
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_write_artifact_bytes(
        pouch, namespace_name, exact_term_path,
        exact_term_generation != NULL ? exact_term_generation : "",
        exact_term_generation_length, error);
    if (rc == LC_OK) {
      out->repaired = 1;
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_write_artifact_bytes(
        pouch, namespace_name, presence_term_path,
        presence_term_generation != NULL ? presence_term_generation : "",
        presence_term_generation_length, error);
    if (rc == LC_OK) {
      out->repaired = 1;
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_write_artifact_bytes(
        pouch, namespace_name, range_term_path,
        range_term_generation != NULL ? range_term_generation : "",
        range_term_generation_length, error);
    if (rc == LC_OK) {
      out->repaired = 1;
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_write_artifact_bytes(
        pouch, namespace_name, text_term_path,
        text_term_generation != NULL ? text_term_generation : "",
        text_term_generation_length, error);
    if (rc == LC_OK) {
      out->repaired = 1;
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_write_artifact_bytes(
        pouch, namespace_name, trigram_term_path,
        trigram_term_generation != NULL ? trigram_term_generation : "",
        trigram_term_generation_length, error);
    if (rc == LC_OK) {
      out->repaired = 1;
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_write_artifact_bytes(
        pouch, namespace_name, temporal_term_path,
        temporal_term_generation != NULL ? temporal_term_generation : "",
        temporal_term_generation_length, error);
    if (rc == LC_OK) {
      out->repaired = 1;
    }
  }
  if (rc == LC_OK) {
    out->index_seq = state_index_seq;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_read_header(
        pouch, namespace_name, sidecar_path, &warm_sidecar, error);
  }
  if (rc == LC_OK && warm_sidecar.present && warm_sidecar.valid) {
    rc = lc_pouch_query_index_prepare_text_reader(
        pouch, namespace_name, &warm_sidecar, &prepared_text, error);
  }
  lc_free_with_allocator(&pouch->allocator, exact_term_generation);
  lc_free_with_allocator(&pouch->allocator, presence_term_generation);
  lc_free_with_allocator(&pouch->allocator, range_term_generation);
  lc_free_with_allocator(&pouch->allocator, text_term_generation);
  lc_free_with_allocator(&pouch->allocator, trigram_term_generation);
  lc_free_with_allocator(&pouch->allocator, temporal_term_generation);
  lc_free_with_allocator(&pouch->allocator, doc_table_generation);
  lc_free_with_allocator(&pouch->allocator, text.bytes);
  lc_free_with_allocator(&pouch->allocator, extract_keys);
  lc_pouch_query_index_summary_cleanup(&summary);
  lc_free_with_allocator(&pouch->allocator, temporal_term_path);
  lc_free_with_allocator(&pouch->allocator, text_term_path);
  lc_free_with_allocator(&pouch->allocator, range_term_path);
  lc_free_with_allocator(&pouch->allocator, presence_term_path);
  lc_free_with_allocator(&pouch->allocator, exact_term_path);
  lc_free_with_allocator(&pouch->allocator, doc_table_path);
  lc_free_with_allocator(&pouch->allocator, trigram_term_path);
  lc_free_with_allocator(&pouch->allocator, sidecar_path);
  return rc;
}

int lc_pouch_query_index_ensure_current(lc_pouch *pouch,
                                        const char *namespace_name,
                                        unsigned long state_index_seq,
                                        lc_pouch_query_index_flush_result *out,
                                        lc_error *error) {
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
  rc = lc_pouch_query_index_read_header(pouch, namespace_name, sidecar_path,
                                        &sidecar, error);
  lc_free_with_allocator(&pouch->allocator, sidecar_path);
  if (rc != LC_OK) {
    return rc;
  }
  if (sidecar.present && sidecar.valid &&
      sidecar.index_seq == state_index_seq) {
    out->index_seq = state_index_seq;
    return LC_OK;
  }
  return lc_pouch_query_index_flush(pouch, namespace_name, state_index_seq, out,
                                    error);
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
  rc = lc_pouch_query_index_read_with_reader(
      pouch, namespace_name, sidecar_path, &sidecar, &reader, NULL, NULL,
      error);
  if (rc == LC_OK && (!sidecar.present || !sidecar.valid)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index sidecar is not readable", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK) {
    *index_seq = sidecar.index_seq;
  }
  lc_free_with_allocator(&pouch->allocator, sidecar_path);
  return rc;
}

static int lc_pouch_query_index_visit_term_match(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *value, char value_type, int prefix_match, int contains_match,
    int ignore_case, const lc_pouch_query_index_range_bounds *range_bounds,
    const lc_pouch_query_index_date_bounds *date_bounds,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error) {
  lc_pouch_query_index_read_result sidecar;
  lc_pouch_query_index_term_reader reader;
  lc_pouch_index_term_key *exact_terms;
  char *sidecar_path;
  char *field_hex;
  char *value_hex;
  size_t exact_term_count;
  int exact_scalar;
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
  exact_terms = NULL;
  exact_term_count = 0U;
  field_hex = NULL;
  value_hex = NULL;
  exact_scalar = !prefix_match && !contains_match && !ignore_case &&
                 range_bounds == NULL && date_bounds == NULL;
  if (exact_scalar && (value_type != 's' && value_type != 'n' &&
                       value_type != 'b' && value_type != 'z')) {
    lc_free_with_allocator(&pouch->allocator, sidecar_path);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index scalar lookup requires a JSON "
                        "scalar value type",
                        NULL, NULL, NULL);
  }
  if (exact_scalar) {
    char value_types[1];

    value_types[0] = value_type;
    rc = lc_pouch_index_term_keys_build_exact_for_field(
        field, &value, value_types, 1U, &exact_terms, &exact_term_count,
        &pouch->allocator, error);
  } else {
    field_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, field);
    value_hex = range_bounds == NULL && date_bounds == NULL
                    ? lc_pouch_query_index_hex_encode(&pouch->allocator, value)
                    : lc_strdup_with_allocator(&pouch->allocator, "");
    rc = field_hex != NULL && value_hex != NULL ? LC_OK : LC_ERR_NOMEM;
  }
  if (rc != LC_OK) {
    if (rc == LC_ERR_NOMEM) {
      lc_error_set(error, LC_ERR_NOMEM, 0L,
                   "failed to allocate pouch query-index scalar lookup", NULL,
                   NULL, NULL);
    }
    lc_pouch_index_term_keys_cleanup(&pouch->allocator, exact_terms, 1U);
    lc_free_with_allocator(&pouch->allocator, field_hex);
    lc_free_with_allocator(&pouch->allocator, value_hex);
    lc_free_with_allocator(&pouch->allocator, sidecar_path);
    return rc;
  }
  if (exact_scalar) {
    rc = lc_pouch_query_index_visit_exact_generation(
        pouch, namespace_name, exact_terms, exact_term_count, visit, context,
        index_seq, error);
    lc_pouch_index_term_keys_cleanup(&pouch->allocator, exact_terms, 1U);
    lc_free_with_allocator(&pouch->allocator, sidecar_path);
    return rc;
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
    rc = lc_pouch_index_parse_date_bounds(date_bounds,
                                          &reader.parsed_date_bounds, error);
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
  rc = lc_pouch_query_index_read_with_reader(
      pouch, namespace_name, sidecar_path, &sidecar, NULL, &reader, NULL,
      error);
  if (rc == LC_OK && (!sidecar.present || !sidecar.valid)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index sidecar is not readable", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK && !sidecar.term_index_complete) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index scalar postings are incomplete", NULL,
                      NULL, "pouch");
  }
  if (rc == LC_OK) {
    *index_seq = sidecar.index_seq;
  }
  lc_free_with_allocator(&pouch->allocator, reader.value_scratch);
  lc_pouch_index_term_keys_cleanup(&pouch->allocator, exact_terms, 1U);
  lc_free_with_allocator(&pouch->allocator, field_hex);
  lc_free_with_allocator(&pouch->allocator, value_hex);
  lc_free_with_allocator(&pouch->allocator, sidecar_path);
  return rc;
}

int lc_pouch_query_index_visit_scalar(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *value, char value_type, lc_pouch_query_index_key_visit_fn visit,
    void *context, unsigned long *index_seq, lc_error *error) {
  return lc_pouch_query_index_visit_term_match(
      pouch, namespace_name, field, value, value_type, 0, 0, 0, NULL, NULL,
      visit, context, index_seq, error);
}

int lc_pouch_query_index_visit_scalar_any(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *const *values, const char *value_types, size_t value_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error) {
  lc_pouch_index_term_key *exact_terms;
  size_t exact_term_count;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      field == NULL || field[0] == '\0' || values == NULL ||
      value_types == NULL || value_count == 0U || visit == NULL ||
      index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index multi-scalar lookup requires "
                        "pouch, namespace, field, values, visitor, and "
                        "index_seq",
                        NULL, NULL, NULL);
  }
  if (value_count == 1U) {
    return lc_pouch_query_index_visit_scalar(pouch, namespace_name, field,
                                             values[0], value_types[0], visit,
                                             context, index_seq, error);
  }
  *index_seq = 0UL;
  exact_terms = NULL;
  exact_term_count = 0U;
  rc = lc_pouch_index_term_keys_build_exact_for_field(
      field, values, value_types, value_count, &exact_terms, &exact_term_count,
      &pouch->allocator, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_visit_exact_generation(
        pouch, namespace_name, exact_terms, exact_term_count, visit, context,
        index_seq, error);
  }
  lc_pouch_index_term_keys_cleanup(&pouch->allocator, exact_terms,
                                   exact_term_count);
  return rc;
}

int lc_pouch_query_index_visit_scalar_terms(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_scalar_term *terms, size_t term_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error) {
  lc_pouch_index_term_key *exact_terms;
  size_t exact_term_count;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      terms == NULL || term_count == 0U || visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index scalar term-set lookup requires "
                        "pouch, namespace, terms, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  if (term_count == 1U) {
    return lc_pouch_query_index_visit_scalar(
        pouch, namespace_name, terms[0].field, terms[0].value,
        terms[0].value_type, visit, context, index_seq, error);
  }
  *index_seq = 0UL;
  exact_terms = NULL;
  exact_term_count = 0U;
  rc = lc_pouch_index_term_keys_build_exact(terms, term_count, &exact_terms,
                                            &exact_term_count,
                                            &pouch->allocator, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_visit_exact_generation(
        pouch, namespace_name, exact_terms, exact_term_count, visit, context,
        index_seq, error);
  }
  lc_pouch_index_term_keys_cleanup(&pouch->allocator, exact_terms,
                                   exact_term_count);
  return rc;
}

static int
lc_pouch_query_index_rows_emit(const lc_pouch_index_result_row_list *rows,
                               lc_pouch_query_index_key_visit_fn visit,
                               void *context, lc_error *error);
static int lc_pouch_query_index_docid_rows_build(
    const lc_allocator *allocator, lc_pouch_index_result_docid_list *docids,
    const lc_pouch_index_doc_table *doc_table,
    lc_pouch_index_result_row_list *rows, lc_error *error);
static int lc_pouch_query_index_docid_emit_cached(
    const lc_allocator *allocator, lc_pouch_index_result_page_cache *cache,
    const char *cache_key, lc_pouch_index_result_docid_list *docids,
    const lc_pouch_index_doc_table *doc_table,
    lc_pouch_query_index_key_visit_fn visit, void *context, lc_error *error);

static int lc_pouch_query_index_docid_emit(
    const lc_allocator *allocator, lc_pouch_index_result_docid_list *docids,
    const lc_pouch_index_doc_table *doc_table,
    lc_pouch_query_index_key_visit_fn visit, void *context, lc_error *error) {
  lc_pouch_index_result_row_list rows;
  int rc;

  memset(&rows, 0, sizeof(rows));
  if (docids == NULL || doc_table == NULL || visit == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index docID emit requires docIDs, doc "
                        "table, and visitor",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_query_index_docid_rows_build(allocator, docids, doc_table,
                                             &rows, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_rows_emit(&rows, visit, context, error);
  }
  lc_pouch_index_result_row_list_cleanup(allocator, &rows);
  return rc;
}

static int
lc_pouch_query_index_rows_emit(const lc_pouch_index_result_row_list *rows,
                               lc_pouch_query_index_key_visit_fn visit,
                               void *context, lc_error *error) {
  lc_pouch_query_index_key_view key_view;
  size_t index;
  int rc;

  if (rows == NULL || visit == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index row emit requires rows and "
                        "visitor",
                        NULL, NULL, NULL);
  }
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < rows->count; ++index) {
    const lc_pouch_index_result_row *row;

    row = &rows->items[index];
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
  if (rc == LC_POUCH_STATE_READ_MANY_STOP) {
    rc = LC_OK;
  }
  return rc;
}

static int lc_pouch_query_index_docid_rows_build(
    const lc_allocator *allocator, lc_pouch_index_result_docid_list *docids,
    const lc_pouch_index_doc_table *doc_table,
    lc_pouch_index_result_row_list *rows, lc_error *error) {
  const lc_pouch_index_doc *doc;
  char *key;
  size_t index;
  int rc;

  if (docids == NULL || doc_table == NULL || rows == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index docID row build requires docIDs, "
                        "doc table, and rows",
                        NULL, NULL, NULL);
  }
  if (docids->count == 0U) {
    return LC_OK;
  }
  rc = lc_pouch_index_result_docid_list_sort_compact(allocator, docids, error);
  if (rc != LC_OK) {
    return rc;
  }
  for (index = 0U; rc == LC_OK && index < docids->count; ++index) {
    doc = NULL;
    rc = lc_pouch_index_doc_table_get(doc_table, docids->items[index].doc_id,
                                      &doc, error);
    if (rc != LC_OK) {
      break;
    }
    key = lc_pouch_query_index_hex_decode(allocator, doc->key_hex, error);
    if (key == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      break;
    }
    rc = lc_pouch_index_result_row_list_add(
        allocator, rows, key, doc->key_hex, docids->items[index].doc_id,
        doc->version, doc->bytes, doc->has_query_hidden, doc->query_hidden,
        docids->items[index].value_index, error);
    lc_free_with_allocator(allocator, key);
  }
  return rc;
}

static int lc_pouch_query_index_docid_emit_cached(
    const lc_allocator *allocator, lc_pouch_index_result_page_cache *cache,
    const char *cache_key, lc_pouch_index_result_docid_list *docids,
    const lc_pouch_index_doc_table *doc_table,
    lc_pouch_query_index_key_visit_fn visit, void *context, lc_error *error) {
  const lc_pouch_index_result_row_list *cached_rows;
  lc_pouch_index_result_row_list rows;
  int hit;
  int rc;

  if (cache == NULL || cache_key == NULL) {
    return lc_pouch_query_index_docid_emit(allocator, docids, doc_table, visit,
                                           context, error);
  }
  cached_rows = NULL;
  hit = 0;
  rc = lc_pouch_index_result_page_cache_lookup(cache, cache_key, &cached_rows,
                                               &hit, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (hit) {
    return lc_pouch_query_index_rows_emit(cached_rows, visit, context, error);
  }
  memset(&rows, 0, sizeof(rows));
  rc = lc_pouch_query_index_docid_rows_build(allocator, docids, doc_table,
                                             &rows, error);
  if (rc == LC_OK) {
    rc = lc_pouch_index_result_page_cache_store(allocator, cache, cache_key,
                                                &rows, error);
  }
  lc_pouch_index_result_row_list_cleanup(allocator, &rows);
  if (rc == LC_OK) {
    rc = lc_pouch_index_result_page_cache_lookup(cache, cache_key, &cached_rows,
                                                 &hit, error);
  }
  if (rc == LC_OK && hit) {
    rc = lc_pouch_query_index_rows_emit(cached_rows, visit, context, error);
  }
  return rc;
}

static void lc_pouch_query_index_prepared_exact_cleanup(
    const lc_allocator *allocator, lc_pouch_query_index_prepared_exact *entry) {
  if (entry == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, entry->namespace_name);
  lc_pouch_index_doc_table_cleanup(allocator, &entry->doc_table);
  lc_pouch_index_term_generation_cleanup(allocator, &entry->exact_generation);
  lc_free_with_allocator(allocator, entry->last_result_key);
  lc_pouch_index_result_docid_list_cleanup(allocator,
                                           &entry->last_result_docids);
  lc_pouch_index_result_page_cache_cleanup(allocator, &entry->page_cache);
  lc_free_with_allocator(allocator, entry);
}

static void lc_pouch_query_index_prepared_presence_cleanup(
    const lc_allocator *allocator,
    lc_pouch_query_index_prepared_presence *entry) {
  if (entry == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, entry->namespace_name);
  lc_pouch_index_doc_table_cleanup(allocator, &entry->doc_table);
  lc_pouch_index_term_generation_cleanup(allocator,
                                         &entry->presence_generation);
  lc_free_with_allocator(allocator, entry->last_result_key);
  lc_pouch_index_result_docid_list_cleanup(allocator,
                                           &entry->last_result_docids);
  lc_pouch_index_result_page_cache_cleanup(allocator, &entry->page_cache);
  lc_free_with_allocator(allocator, entry);
}

static void lc_pouch_query_index_prepared_range_cleanup(
    const lc_allocator *allocator, lc_pouch_query_index_prepared_range *entry) {
  if (entry == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, entry->namespace_name);
  lc_pouch_index_doc_table_cleanup(allocator, &entry->doc_table);
  lc_pouch_index_term_generation_cleanup(allocator, &entry->range_generation);
  lc_free_with_allocator(allocator, entry->last_result_key);
  lc_pouch_index_result_docid_list_cleanup(allocator,
                                           &entry->last_result_docids);
  lc_pouch_index_result_page_cache_cleanup(allocator, &entry->page_cache);
  lc_free_with_allocator(allocator, entry);
}

static void lc_pouch_query_index_prepared_text_cleanup(
    const lc_allocator *allocator, lc_pouch_query_index_prepared_text *entry) {
  if (entry == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, entry->namespace_name);
  lc_pouch_index_doc_table_cleanup(allocator, &entry->doc_table);
  lc_pouch_index_term_generation_cleanup(allocator, &entry->text_generation);
  lc_pouch_index_term_generation_cleanup(allocator, &entry->trigram_generation);
  lc_free_with_allocator(allocator, entry->last_result_key);
  lc_pouch_index_result_docid_list_cleanup(allocator,
                                           &entry->last_result_docids);
  lc_pouch_index_result_page_cache_cleanup(allocator, &entry->page_cache);
  lc_free_with_allocator(allocator, entry);
}

static void lc_pouch_query_index_prepared_temporal_cleanup(
    const lc_allocator *allocator,
    lc_pouch_query_index_prepared_temporal *entry) {
  if (entry == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, entry->namespace_name);
  lc_pouch_index_doc_table_cleanup(allocator, &entry->doc_table);
  lc_pouch_index_term_generation_cleanup(allocator,
                                         &entry->temporal_generation);
  lc_free_with_allocator(allocator, entry->last_result_key);
  lc_pouch_index_result_docid_list_cleanup(allocator,
                                           &entry->last_result_docids);
  lc_pouch_index_result_page_cache_cleanup(allocator, &entry->page_cache);
  lc_free_with_allocator(allocator, entry);
}

void lc_pouch_query_index_prepared_cache_cleanup(lc_pouch *pouch) {
  lc_pouch_query_index_prepared_exact *entry;
  lc_pouch_query_index_prepared_exact *next;
  lc_pouch_query_index_prepared_presence *presence_entry;
  lc_pouch_query_index_prepared_presence *presence_next;
  lc_pouch_query_index_prepared_range *range_entry;
  lc_pouch_query_index_prepared_range *range_next;
  lc_pouch_query_index_prepared_text *text_entry;
  lc_pouch_query_index_prepared_text *text_next;
  lc_pouch_query_index_prepared_temporal *temporal_entry;
  lc_pouch_query_index_prepared_temporal *temporal_next;

  if (pouch == NULL) {
    return;
  }
  entry = pouch->prepared_exact_readers;
  pouch->prepared_exact_readers = NULL;
  while (entry != NULL) {
    next = entry->next;
    lc_pouch_query_index_prepared_exact_cleanup(&pouch->allocator, entry);
    entry = next;
  }
  presence_entry = pouch->prepared_presence_readers;
  pouch->prepared_presence_readers = NULL;
  while (presence_entry != NULL) {
    presence_next = presence_entry->next;
    lc_pouch_query_index_prepared_presence_cleanup(&pouch->allocator,
                                                   presence_entry);
    presence_entry = presence_next;
  }
  range_entry = pouch->prepared_range_readers;
  pouch->prepared_range_readers = NULL;
  while (range_entry != NULL) {
    range_next = range_entry->next;
    lc_pouch_query_index_prepared_range_cleanup(&pouch->allocator, range_entry);
    range_entry = range_next;
  }
  text_entry = pouch->prepared_text_readers;
  pouch->prepared_text_readers = NULL;
  while (text_entry != NULL) {
    text_next = text_entry->next;
    lc_pouch_query_index_prepared_text_cleanup(&pouch->allocator, text_entry);
    text_entry = text_next;
  }
  temporal_entry = pouch->prepared_temporal_readers;
  pouch->prepared_temporal_readers = NULL;
  while (temporal_entry != NULL) {
    temporal_next = temporal_entry->next;
    lc_pouch_query_index_prepared_temporal_cleanup(&pouch->allocator,
                                                   temporal_entry);
    temporal_entry = temporal_next;
  }
}

static int lc_pouch_query_index_prepared_exact_matches(
    const lc_pouch_query_index_prepared_exact *entry,
    const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar) {
  return entry != NULL && namespace_name != NULL && sidecar != NULL &&
         entry->namespace_name != NULL &&
         strcmp(entry->namespace_name, namespace_name) == 0 &&
         entry->index_seq == sidecar->index_seq &&
         entry->row_count == sidecar->row_count &&
         entry->row_hash == sidecar->row_hash;
}

static lc_pouch_query_index_prepared_exact *
lc_pouch_query_index_prepared_exact_find(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar) {
  lc_pouch_query_index_prepared_exact *entry;

  if (pouch == NULL) {
    return NULL;
  }
  for (entry = pouch->prepared_exact_readers; entry != NULL;
       entry = entry->next) {
    if (lc_pouch_query_index_prepared_exact_matches(entry, namespace_name,
                                                    sidecar)) {
      return entry;
    }
  }
  return NULL;
}

static void lc_pouch_query_index_prepared_exact_remove_namespace(
    lc_pouch *pouch, const char *namespace_name) {
  lc_pouch_query_index_prepared_exact **cursor;
  lc_pouch_query_index_prepared_exact *entry;

  if (pouch == NULL || namespace_name == NULL) {
    return;
  }
  cursor = &pouch->prepared_exact_readers;
  while (*cursor != NULL) {
    entry = *cursor;
    if (entry->namespace_name != NULL &&
        strcmp(entry->namespace_name, namespace_name) == 0) {
      *cursor = entry->next;
      entry->next = NULL;
      lc_pouch_query_index_prepared_exact_cleanup(&pouch->allocator, entry);
      continue;
    }
    cursor = &entry->next;
  }
}

static int lc_pouch_query_index_prepared_presence_matches(
    const lc_pouch_query_index_prepared_presence *entry,
    const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar) {
  return entry != NULL && namespace_name != NULL && sidecar != NULL &&
         entry->namespace_name != NULL &&
         strcmp(entry->namespace_name, namespace_name) == 0 &&
         entry->index_seq == sidecar->index_seq &&
         entry->row_count == sidecar->row_count &&
         entry->row_hash == sidecar->row_hash;
}

static lc_pouch_query_index_prepared_presence *
lc_pouch_query_index_prepared_presence_find(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar) {
  lc_pouch_query_index_prepared_presence *entry;

  if (pouch == NULL) {
    return NULL;
  }
  for (entry = pouch->prepared_presence_readers; entry != NULL;
       entry = entry->next) {
    if (lc_pouch_query_index_prepared_presence_matches(entry, namespace_name,
                                                       sidecar)) {
      return entry;
    }
  }
  return NULL;
}

static void lc_pouch_query_index_prepared_presence_remove_namespace(
    lc_pouch *pouch, const char *namespace_name) {
  lc_pouch_query_index_prepared_presence **cursor;
  lc_pouch_query_index_prepared_presence *entry;

  if (pouch == NULL || namespace_name == NULL) {
    return;
  }
  cursor = &pouch->prepared_presence_readers;
  while (*cursor != NULL) {
    entry = *cursor;
    if (entry->namespace_name != NULL &&
        strcmp(entry->namespace_name, namespace_name) == 0) {
      *cursor = entry->next;
      entry->next = NULL;
      lc_pouch_query_index_prepared_presence_cleanup(&pouch->allocator, entry);
      continue;
    }
    cursor = &entry->next;
  }
}

static int lc_pouch_query_index_prepared_range_matches(
    const lc_pouch_query_index_prepared_range *entry,
    const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar) {
  return entry != NULL && namespace_name != NULL && sidecar != NULL &&
         entry->namespace_name != NULL &&
         strcmp(entry->namespace_name, namespace_name) == 0 &&
         entry->index_seq == sidecar->index_seq &&
         entry->row_count == sidecar->row_count &&
         entry->row_hash == sidecar->row_hash;
}

static lc_pouch_query_index_prepared_range *
lc_pouch_query_index_prepared_range_find(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar) {
  lc_pouch_query_index_prepared_range *entry;

  if (pouch == NULL) {
    return NULL;
  }
  for (entry = pouch->prepared_range_readers; entry != NULL;
       entry = entry->next) {
    if (lc_pouch_query_index_prepared_range_matches(entry, namespace_name,
                                                    sidecar)) {
      return entry;
    }
  }
  return NULL;
}

static void lc_pouch_query_index_prepared_range_remove_namespace(
    lc_pouch *pouch, const char *namespace_name) {
  lc_pouch_query_index_prepared_range **cursor;
  lc_pouch_query_index_prepared_range *entry;

  if (pouch == NULL || namespace_name == NULL) {
    return;
  }
  cursor = &pouch->prepared_range_readers;
  while (*cursor != NULL) {
    entry = *cursor;
    if (entry->namespace_name != NULL &&
        strcmp(entry->namespace_name, namespace_name) == 0) {
      *cursor = entry->next;
      entry->next = NULL;
      lc_pouch_query_index_prepared_range_cleanup(&pouch->allocator, entry);
      continue;
    }
    cursor = &entry->next;
  }
}

static int lc_pouch_query_index_prepared_text_matches(
    const lc_pouch_query_index_prepared_text *entry, const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar) {
  return entry != NULL && namespace_name != NULL && sidecar != NULL &&
         entry->namespace_name != NULL &&
         strcmp(entry->namespace_name, namespace_name) == 0 &&
         entry->index_seq == sidecar->index_seq &&
         entry->row_count == sidecar->row_count &&
         entry->row_hash == sidecar->row_hash;
}

static lc_pouch_query_index_prepared_text *
lc_pouch_query_index_prepared_text_find(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar) {
  lc_pouch_query_index_prepared_text *entry;

  if (pouch == NULL) {
    return NULL;
  }
  for (entry = pouch->prepared_text_readers; entry != NULL;
       entry = entry->next) {
    if (lc_pouch_query_index_prepared_text_matches(entry, namespace_name,
                                                   sidecar)) {
      return entry;
    }
  }
  return NULL;
}

static void lc_pouch_query_index_prepared_text_remove_namespace(
    lc_pouch *pouch, const char *namespace_name) {
  lc_pouch_query_index_prepared_text **cursor;
  lc_pouch_query_index_prepared_text *entry;

  if (pouch == NULL || namespace_name == NULL) {
    return;
  }
  cursor = &pouch->prepared_text_readers;
  while (*cursor != NULL) {
    entry = *cursor;
    if (entry->namespace_name != NULL &&
        strcmp(entry->namespace_name, namespace_name) == 0) {
      *cursor = entry->next;
      entry->next = NULL;
      lc_pouch_query_index_prepared_text_cleanup(&pouch->allocator, entry);
      continue;
    }
    cursor = &entry->next;
  }
}

static int lc_pouch_query_index_prepared_temporal_matches(
    const lc_pouch_query_index_prepared_temporal *entry,
    const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar) {
  return entry != NULL && namespace_name != NULL && sidecar != NULL &&
         entry->namespace_name != NULL &&
         strcmp(entry->namespace_name, namespace_name) == 0 &&
         entry->index_seq == sidecar->index_seq &&
         entry->row_count == sidecar->row_count &&
         entry->row_hash == sidecar->row_hash;
}

static lc_pouch_query_index_prepared_temporal *
lc_pouch_query_index_prepared_temporal_find(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar) {
  lc_pouch_query_index_prepared_temporal *entry;

  if (pouch == NULL) {
    return NULL;
  }
  for (entry = pouch->prepared_temporal_readers; entry != NULL;
       entry = entry->next) {
    if (lc_pouch_query_index_prepared_temporal_matches(entry, namespace_name,
                                                       sidecar)) {
      return entry;
    }
  }
  return NULL;
}

static void lc_pouch_query_index_prepared_temporal_remove_namespace(
    lc_pouch *pouch, const char *namespace_name) {
  lc_pouch_query_index_prepared_temporal **cursor;
  lc_pouch_query_index_prepared_temporal *entry;

  if (pouch == NULL || namespace_name == NULL) {
    return;
  }
  cursor = &pouch->prepared_temporal_readers;
  while (*cursor != NULL) {
    entry = *cursor;
    if (entry->namespace_name != NULL &&
        strcmp(entry->namespace_name, namespace_name) == 0) {
      *cursor = entry->next;
      entry->next = NULL;
      lc_pouch_query_index_prepared_temporal_cleanup(&pouch->allocator, entry);
      continue;
    }
    cursor = &entry->next;
  }
}

static int lc_pouch_query_index_load_doc_table_generation(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar,
    lc_pouch_index_doc_table *doc_table, lc_error *error) {
  lc_pouch_query_index_flush_result flush_result;
  char *doc_table_path;
  int present;
  int valid;
  int rc;

  if (pouch == NULL || namespace_name == NULL || sidecar == NULL ||
      doc_table == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index doc table generation load requires "
                        "pouch, namespace, sidecar, and table",
                        NULL, NULL, NULL);
  }
  doc_table_path =
      lc_pouch_query_index_doc_table_path(pouch, namespace_name, error);
  if (doc_table_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  present = 0;
  valid = 0;
  rc = lc_pouch_query_index_doc_table_generation_load_artifact(
      pouch, namespace_name, doc_table_path, sidecar->index_seq,
      sidecar->row_count, sidecar->row_hash, doc_table, &present, &valid,
      error);
  if (rc == LC_OK && (!present || !valid)) {
    memset(&flush_result, 0, sizeof(flush_result));
    rc = lc_pouch_query_index_flush(pouch, namespace_name, sidecar->index_seq,
                                    &flush_result, error);
    if (rc == LC_OK) {
      lc_pouch_index_doc_table_cleanup(&pouch->allocator, doc_table);
      present = 0;
      valid = 0;
      rc = lc_pouch_query_index_doc_table_generation_load_artifact(
          pouch, namespace_name, doc_table_path, sidecar->index_seq,
          sidecar->row_count, sidecar->row_hash, doc_table, &present, &valid,
          error);
    }
  }
  if (rc == LC_OK && (!present || !valid)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index doc table generation is not readable",
                      NULL, NULL, "pouch");
  }
  lc_free_with_allocator(&pouch->allocator, doc_table_path);
  return rc;
}

static int lc_pouch_query_index_load_exact_term_generation(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar,
    lc_pouch_index_term_generation *generation, lc_error *error) {
  lc_pouch_query_index_flush_result flush_result;
  char *exact_term_path;
  int present;
  int valid;
  int rc;

  if (pouch == NULL || namespace_name == NULL || sidecar == NULL ||
      generation == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index exact generation load requires "
                        "pouch, namespace, sidecar, and generation",
                        NULL, NULL, NULL);
  }
  exact_term_path =
      lc_pouch_query_index_exact_term_path(pouch, namespace_name, error);
  if (exact_term_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  present = 0;
  valid = 0;
  rc = lc_pouch_query_index_term_generation_load_artifact(
      pouch, namespace_name, exact_term_path, sidecar->index_seq,
      sidecar->row_count, sidecar->row_hash, generation, &present, &valid,
      error);
  if (rc == LC_OK && (!present || !valid)) {
    memset(&flush_result, 0, sizeof(flush_result));
    rc = lc_pouch_query_index_flush(pouch, namespace_name, sidecar->index_seq,
                                    &flush_result, error);
    if (rc == LC_OK) {
      lc_pouch_index_term_generation_cleanup(&pouch->allocator, generation);
      present = 0;
      valid = 0;
      rc = lc_pouch_query_index_term_generation_load_artifact(
          pouch, namespace_name, exact_term_path, sidecar->index_seq,
          sidecar->row_count, sidecar->row_hash, generation, &present, &valid,
          error);
    }
  }
  if (rc == LC_OK && (!present || !valid)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index exact generation is not readable",
                      NULL, NULL, "pouch");
  }
  lc_free_with_allocator(&pouch->allocator, exact_term_path);
  return rc;
}

static int lc_pouch_query_index_file_signature_capture(
    const char *path, lc_pouch_query_index_file_signature *signature,
    lc_error *error) {
  struct stat st;

  if (path == NULL || signature == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index file signature requires path and "
                        "signature",
                        NULL, NULL, NULL);
  }
  memset(signature, 0, sizeof(*signature));
  if (stat(path, &st) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, (long)errno,
                        "failed to stat pouch query-index generation file",
                        path, NULL, "pouch");
  }
  signature->size = (unsigned long)st.st_size;
  signature->mtime = (unsigned long)st.st_mtime;
  signature->present = 1;
  return LC_OK;
}

static int lc_pouch_query_index_artifact_signature_capture(
    lc_pouch *pouch, const char *path,
    lc_pouch_query_index_file_signature *signature, lc_error *error) {
  lc_pouch_query_index_file_signature descriptor_signature;
  char *descriptor_path;
  int rc;

  rc = lc_pouch_query_index_file_signature_capture(path, signature, error);
  if (rc != LC_OK || !lc_pouch_crypto_enabled(pouch != NULL ? pouch->crypto
                                                            : NULL)) {
    return rc;
  }
  descriptor_path = lc_pouch_query_index_crypto_descriptor_path(
      &pouch->allocator, path, error);
  if (descriptor_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_query_index_file_signature_capture(descriptor_path,
                                                   &descriptor_signature,
                                                   error);
  lc_free_with_allocator(&pouch->allocator, descriptor_path);
  if (rc != LC_OK) {
    return rc;
  }
  signature->size = (signature->size * 33UL) ^ descriptor_signature.size;
  signature->mtime = (signature->mtime * 33UL) ^ descriptor_signature.mtime;
  signature->present = signature->present && descriptor_signature.present;
  return LC_OK;
}

static int lc_pouch_query_index_capture_path_signature(
    lc_pouch *pouch, const char *namespace_name,
    char *(*path_fn)(lc_pouch *, const char *, lc_error *),
    lc_pouch_query_index_file_signature *signature, lc_error *error) {
  char *path;
  int rc;

  if (pouch == NULL || namespace_name == NULL || path_fn == NULL ||
      signature == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index signature capture requires pouch, "
                        "namespace, path builder, and signature",
                        NULL, NULL, NULL);
  }
  path = path_fn(pouch, namespace_name, error);
  if (path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_query_index_artifact_signature_capture(pouch, path, signature,
                                                       error);
  lc_free_with_allocator(&pouch->allocator, path);
  return rc;
}

static int lc_pouch_query_index_signature_unchanged(
    lc_pouch *pouch, const char *path,
    const lc_pouch_query_index_file_signature *signature, int *unchanged_out,
    lc_error *error) {
  lc_pouch_query_index_file_signature current;
  int rc;

  if (unchanged_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index signature comparison requires "
                        "output",
                        NULL, NULL, NULL);
  }
  *unchanged_out = 0;
  if (path == NULL || signature == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index signature comparison requires path "
                        "and signature",
                        NULL, NULL, NULL);
  }
  if (!signature->present) {
    return LC_OK;
  }
  rc = lc_pouch_query_index_artifact_signature_capture(pouch, path, &current,
                                                       error);
  if (rc != LC_OK) {
    return rc;
  }
  *unchanged_out = current.present && current.size == signature->size &&
                   current.mtime == signature->mtime;
  return LC_OK;
}

static int lc_pouch_query_index_validate_doc_table_generation(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar, int *valid_out,
    lc_error *error) {
  char *doc_table_path;
  int present;
  int valid;
  int rc;

  if (valid_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index doc table generation validation "
                        "requires output",
                        NULL, NULL, NULL);
  }
  *valid_out = 0;
  if (pouch == NULL || namespace_name == NULL || sidecar == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index doc table generation validation "
                        "requires pouch, namespace, and sidecar",
                        NULL, NULL, NULL);
  }
  doc_table_path =
      lc_pouch_query_index_doc_table_path(pouch, namespace_name, error);
  if (doc_table_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  present = 0;
  valid = 0;
  rc = lc_pouch_query_index_doc_table_generation_validate_artifact(
      pouch, namespace_name, doc_table_path, sidecar->index_seq,
      sidecar->row_count, sidecar->row_hash, &present, &valid, error);
  if (rc == LC_OK) {
    *valid_out = present && valid;
  }
  lc_free_with_allocator(&pouch->allocator, doc_table_path);
  return rc;
}

static int lc_pouch_query_index_validate_doc_table_signature(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar,
    lc_pouch_query_index_file_signature *signature, int *valid_out,
    lc_error *error) {
  char *doc_table_path;
  int unchanged;
  int rc;

  if (valid_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index doc table signature validation "
                        "requires output",
                        NULL, NULL, NULL);
  }
  *valid_out = 0;
  doc_table_path =
      lc_pouch_query_index_doc_table_path(pouch, namespace_name, error);
  if (doc_table_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  unchanged = 0;
  rc = lc_pouch_query_index_signature_unchanged(
      pouch, doc_table_path, signature, &unchanged, error);
  lc_free_with_allocator(&pouch->allocator, doc_table_path);
  if (rc != LC_OK) {
    return rc;
  }
  if (unchanged) {
    *valid_out = 1;
    return LC_OK;
  }
  rc = lc_pouch_query_index_validate_doc_table_generation(
      pouch, namespace_name, sidecar, valid_out, error);
  if (rc == LC_OK && *valid_out) {
    rc = lc_pouch_query_index_capture_path_signature(
        pouch, namespace_name, lc_pouch_query_index_doc_table_path, signature,
        error);
  }
  return rc;
}

static int lc_pouch_query_index_validate_term_generation_path(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar,
    char *(*path_fn)(lc_pouch *, const char *, lc_error *), int *valid_out,
    lc_error *error) {
  char *term_path;
  int present;
  int valid;
  int rc;

  if (valid_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term generation validation "
                        "requires output",
                        NULL, NULL, NULL);
  }
  *valid_out = 0;
  if (pouch == NULL || namespace_name == NULL || sidecar == NULL ||
      path_fn == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term generation validation "
                        "requires pouch, namespace, sidecar, and path builder",
                        NULL, NULL, NULL);
  }
  term_path = path_fn(pouch, namespace_name, error);
  if (term_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  present = 0;
  valid = 0;
  rc = lc_pouch_query_index_term_generation_validate_artifact(
      pouch, namespace_name, term_path, sidecar->index_seq, sidecar->row_count,
      sidecar->row_hash, &present, &valid, error);
  if (rc == LC_OK) {
    *valid_out = present && valid;
  }
  lc_free_with_allocator(&pouch->allocator, term_path);
  return rc;
}

static int lc_pouch_query_index_validate_term_generation_signature(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar,
    char *(*path_fn)(lc_pouch *, const char *, lc_error *),
    lc_pouch_query_index_file_signature *signature, int *valid_out,
    lc_error *error) {
  char *term_path;
  int unchanged;
  int rc;

  if (valid_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term signature validation requires "
                        "output",
                        NULL, NULL, NULL);
  }
  *valid_out = 0;
  term_path = path_fn(pouch, namespace_name, error);
  if (term_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  unchanged = 0;
  rc = lc_pouch_query_index_signature_unchanged(
      pouch, term_path, signature, &unchanged, error);
  lc_free_with_allocator(&pouch->allocator, term_path);
  if (rc != LC_OK) {
    return rc;
  }
  if (unchanged) {
    *valid_out = 1;
    return LC_OK;
  }
  rc = lc_pouch_query_index_validate_term_generation_path(
      pouch, namespace_name, sidecar, path_fn, valid_out, error);
  if (rc == LC_OK && *valid_out) {
    rc = lc_pouch_query_index_capture_path_signature(pouch, namespace_name,
                                                     path_fn, signature, error);
  }
  return rc;
}

static int lc_pouch_query_index_load_presence_term_generation(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar,
    lc_pouch_index_term_generation *generation, lc_error *error) {
  lc_pouch_query_index_flush_result flush_result;
  char *presence_term_path;
  int present;
  int valid;
  int rc;

  if (pouch == NULL || namespace_name == NULL || sidecar == NULL ||
      generation == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index presence generation load requires "
                        "pouch, namespace, sidecar, and generation",
                        NULL, NULL, NULL);
  }
  presence_term_path =
      lc_pouch_query_index_presence_term_path(pouch, namespace_name, error);
  if (presence_term_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  present = 0;
  valid = 0;
  rc = lc_pouch_query_index_term_generation_load_artifact(
      pouch, namespace_name, presence_term_path, sidecar->index_seq,
      sidecar->row_count, sidecar->row_hash, generation, &present, &valid,
      error);
  if (rc == LC_OK && (!present || !valid)) {
    memset(&flush_result, 0, sizeof(flush_result));
    rc = lc_pouch_query_index_flush(pouch, namespace_name, sidecar->index_seq,
                                    &flush_result, error);
    if (rc == LC_OK) {
      lc_pouch_index_term_generation_cleanup(&pouch->allocator, generation);
      present = 0;
      valid = 0;
      rc = lc_pouch_query_index_term_generation_load_artifact(
          pouch, namespace_name, presence_term_path, sidecar->index_seq,
          sidecar->row_count, sidecar->row_hash, generation, &present, &valid,
          error);
    }
  }
  if (rc == LC_OK && (!present || !valid)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index presence generation is not readable",
                      NULL, NULL, "pouch");
  }
  lc_free_with_allocator(&pouch->allocator, presence_term_path);
  return rc;
}

static int lc_pouch_query_index_load_range_term_generation(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar,
    lc_pouch_index_term_generation *generation, lc_error *error) {
  lc_pouch_query_index_flush_result flush_result;
  char *range_term_path;
  int present;
  int valid;
  int rc;

  if (pouch == NULL || namespace_name == NULL || sidecar == NULL ||
      generation == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index range generation load requires "
                        "pouch, namespace, sidecar, and generation",
                        NULL, NULL, NULL);
  }
  range_term_path =
      lc_pouch_query_index_range_term_path(pouch, namespace_name, error);
  if (range_term_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  present = 0;
  valid = 0;
  rc = lc_pouch_query_index_term_generation_load_artifact(
      pouch, namespace_name, range_term_path, sidecar->index_seq,
      sidecar->row_count, sidecar->row_hash, generation, &present, &valid,
      error);
  if (rc == LC_OK && (!present || !valid)) {
    memset(&flush_result, 0, sizeof(flush_result));
    rc = lc_pouch_query_index_flush(pouch, namespace_name, sidecar->index_seq,
                                    &flush_result, error);
    if (rc == LC_OK) {
      lc_pouch_index_term_generation_cleanup(&pouch->allocator, generation);
      present = 0;
      valid = 0;
      rc = lc_pouch_query_index_term_generation_load_artifact(
          pouch, namespace_name, range_term_path, sidecar->index_seq,
          sidecar->row_count, sidecar->row_hash, generation, &present, &valid,
          error);
    }
  }
  if (rc == LC_OK && (!present || !valid)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index range generation is not readable",
                      NULL, NULL, "pouch");
  }
  lc_free_with_allocator(&pouch->allocator, range_term_path);
  return rc;
}

static int lc_pouch_query_index_load_text_term_generation(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar,
    lc_pouch_index_term_generation *generation, lc_error *error) {
  lc_pouch_query_index_flush_result flush_result;
  char *text_term_path;
  int present;
  int valid;
  int rc;

  if (pouch == NULL || namespace_name == NULL || sidecar == NULL ||
      generation == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index text generation load requires "
                        "pouch, namespace, sidecar, and generation",
                        NULL, NULL, NULL);
  }
  text_term_path =
      lc_pouch_query_index_text_term_path(pouch, namespace_name, error);
  if (text_term_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  present = 0;
  valid = 0;
  rc = lc_pouch_query_index_term_generation_load_artifact(
      pouch, namespace_name, text_term_path, sidecar->index_seq,
      sidecar->row_count, sidecar->row_hash, generation, &present, &valid,
      error);
  if (rc == LC_OK && (!present || !valid)) {
    memset(&flush_result, 0, sizeof(flush_result));
    rc = lc_pouch_query_index_flush(pouch, namespace_name, sidecar->index_seq,
                                    &flush_result, error);
    if (rc == LC_OK) {
      lc_pouch_index_term_generation_cleanup(&pouch->allocator, generation);
      present = 0;
      valid = 0;
      rc = lc_pouch_query_index_term_generation_load_artifact(
          pouch, namespace_name, text_term_path, sidecar->index_seq,
          sidecar->row_count, sidecar->row_hash, generation, &present, &valid,
          error);
    }
  }
  if (rc == LC_OK && (!present || !valid)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index text generation is not readable", NULL,
                      NULL, "pouch");
  }
  lc_free_with_allocator(&pouch->allocator, text_term_path);
  return rc;
}

static int lc_pouch_query_index_load_trigram_term_generation(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar,
    lc_pouch_index_term_generation *generation, lc_error *error) {
  lc_pouch_query_index_flush_result flush_result;
  char *trigram_term_path;
  int present;
  int valid;
  int rc;

  if (pouch == NULL || namespace_name == NULL || sidecar == NULL ||
      generation == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index trigram generation load requires "
                        "pouch, namespace, sidecar, and generation",
                        NULL, NULL, NULL);
  }
  trigram_term_path =
      lc_pouch_query_index_trigram_term_path(pouch, namespace_name, error);
  if (trigram_term_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  present = 0;
  valid = 0;
  rc = lc_pouch_query_index_term_generation_load_artifact(
      pouch, namespace_name, trigram_term_path, sidecar->index_seq,
      sidecar->row_count, sidecar->row_hash, generation, &present, &valid,
      error);
  if (rc == LC_OK && (!present || !valid)) {
    memset(&flush_result, 0, sizeof(flush_result));
    rc = lc_pouch_query_index_flush(pouch, namespace_name, sidecar->index_seq,
                                    &flush_result, error);
    if (rc == LC_OK) {
      lc_pouch_index_term_generation_cleanup(&pouch->allocator, generation);
      present = 0;
      valid = 0;
      rc = lc_pouch_query_index_term_generation_load_artifact(
          pouch, namespace_name, trigram_term_path, sidecar->index_seq,
          sidecar->row_count, sidecar->row_hash, generation, &present, &valid,
          error);
    }
  }
  if (rc == LC_OK && (!present || !valid)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index trigram generation is not readable",
                      NULL, NULL, "pouch");
  }
  lc_free_with_allocator(&pouch->allocator, trigram_term_path);
  return rc;
}

static int lc_pouch_query_index_load_temporal_term_generation(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar,
    lc_pouch_index_term_generation *generation, lc_error *error) {
  lc_pouch_query_index_flush_result flush_result;
  char *temporal_term_path;
  int present;
  int valid;
  int rc;

  if (pouch == NULL || namespace_name == NULL || sidecar == NULL ||
      generation == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index temporal generation load requires "
                        "pouch, namespace, sidecar, and generation",
                        NULL, NULL, NULL);
  }
  temporal_term_path =
      lc_pouch_query_index_temporal_term_path(pouch, namespace_name, error);
  if (temporal_term_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  present = 0;
  valid = 0;
  rc = lc_pouch_query_index_term_generation_load_artifact(
      pouch, namespace_name, temporal_term_path, sidecar->index_seq,
      sidecar->row_count, sidecar->row_hash, generation, &present, &valid,
      error);
  if (rc == LC_OK && (!present || !valid)) {
    memset(&flush_result, 0, sizeof(flush_result));
    rc = lc_pouch_query_index_flush(pouch, namespace_name, sidecar->index_seq,
                                    &flush_result, error);
    if (rc == LC_OK) {
      lc_pouch_index_term_generation_cleanup(&pouch->allocator, generation);
      present = 0;
      valid = 0;
      rc = lc_pouch_query_index_term_generation_load_artifact(
          pouch, namespace_name, temporal_term_path, sidecar->index_seq,
          sidecar->row_count, sidecar->row_hash, generation, &present, &valid,
          error);
    }
  }
  if (rc == LC_OK && (!present || !valid)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index temporal generation is not readable",
                      NULL, NULL, "pouch");
  }
  lc_free_with_allocator(&pouch->allocator, temporal_term_path);
  return rc;
}

static int lc_pouch_query_index_prepare_exact_reader(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar,
    lc_pouch_query_index_prepared_exact **out, lc_error *error) {
  lc_pouch_query_index_prepared_exact *entry;
  int generation_valid;
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index prepared reader requires output",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  if (pouch == NULL || namespace_name == NULL || sidecar == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index prepared reader requires pouch, "
                        "namespace, and sidecar",
                        NULL, NULL, NULL);
  }
  entry =
      lc_pouch_query_index_prepared_exact_find(pouch, namespace_name, sidecar);
  if (entry != NULL) {
    generation_valid = 0;
    rc = lc_pouch_query_index_validate_term_generation_signature(
        pouch, namespace_name, sidecar, lc_pouch_query_index_exact_term_path,
        &entry->exact_signature, &generation_valid, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (generation_valid) {
      rc = lc_pouch_query_index_validate_doc_table_signature(
          pouch, namespace_name, sidecar, &entry->doc_table_signature,
          &generation_valid, error);
      if (rc != LC_OK) {
        return rc;
      }
    }
    if (generation_valid) {
      *out = entry;
      return LC_OK;
    }
    lc_pouch_query_index_prepared_exact_remove_namespace(pouch, namespace_name);
    entry = NULL;
  }
  entry = (lc_pouch_query_index_prepared_exact *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*entry));
  if (entry == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch prepared exact reader", NULL,
                        NULL, NULL);
  }
  entry->namespace_name =
      lc_strdup_with_allocator(&pouch->allocator, namespace_name);
  if (entry->namespace_name == NULL) {
    lc_pouch_query_index_prepared_exact_cleanup(&pouch->allocator, entry);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch prepared reader namespace",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_query_index_load_exact_term_generation(
      pouch, namespace_name, sidecar, &entry->exact_generation, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_capture_path_signature(
        pouch, namespace_name, lc_pouch_query_index_exact_term_path,
        &entry->exact_signature, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_load_doc_table_generation(
        pouch, namespace_name, sidecar, &entry->doc_table, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_capture_path_signature(
        pouch, namespace_name, lc_pouch_query_index_doc_table_path,
        &entry->doc_table_signature, error);
  }
  if (rc != LC_OK) {
    lc_pouch_query_index_prepared_exact_cleanup(&pouch->allocator, entry);
    return rc;
  }
  entry->index_seq = sidecar->index_seq;
  entry->row_count = sidecar->row_count;
  entry->row_hash = sidecar->row_hash;
  lc_pouch_query_index_prepared_exact_remove_namespace(pouch, namespace_name);
  entry->next = pouch->prepared_exact_readers;
  pouch->prepared_exact_readers = entry;
  *out = entry;
  return LC_OK;
}

static int lc_pouch_query_index_prepare_text_reader(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar,
    lc_pouch_query_index_prepared_text **out, lc_error *error) {
  lc_pouch_query_index_prepared_text *entry;
  int generation_valid;
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index prepared text reader requires "
                        "output",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  if (pouch == NULL || namespace_name == NULL || sidecar == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index prepared text reader requires "
                        "pouch, namespace, and sidecar",
                        NULL, NULL, NULL);
  }
  entry =
      lc_pouch_query_index_prepared_text_find(pouch, namespace_name, sidecar);
  if (entry != NULL) {
    generation_valid = 0;
    rc = lc_pouch_query_index_validate_term_generation_signature(
        pouch, namespace_name, sidecar, lc_pouch_query_index_text_term_path,
        &entry->text_signature, &generation_valid, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (generation_valid) {
      rc = lc_pouch_query_index_validate_term_generation_signature(
          pouch, namespace_name, sidecar,
          lc_pouch_query_index_trigram_term_path, &entry->trigram_signature,
          &generation_valid, error);
      if (rc != LC_OK) {
        return rc;
      }
    }
    if (generation_valid) {
      rc = lc_pouch_query_index_validate_doc_table_signature(
          pouch, namespace_name, sidecar, &entry->doc_table_signature,
          &generation_valid, error);
      if (rc != LC_OK) {
        return rc;
      }
    }
    if (generation_valid) {
      *out = entry;
      return LC_OK;
    }
    lc_pouch_query_index_prepared_text_remove_namespace(pouch, namespace_name);
    entry = NULL;
  }
  entry = (lc_pouch_query_index_prepared_text *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*entry));
  if (entry == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch prepared text reader", NULL,
                        NULL, NULL);
  }
  entry->namespace_name =
      lc_strdup_with_allocator(&pouch->allocator, namespace_name);
  if (entry->namespace_name == NULL) {
    lc_pouch_query_index_prepared_text_cleanup(&pouch->allocator, entry);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch prepared text namespace",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_query_index_load_text_term_generation(
      pouch, namespace_name, sidecar, &entry->text_generation, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_capture_path_signature(
        pouch, namespace_name, lc_pouch_query_index_text_term_path,
        &entry->text_signature, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_load_trigram_term_generation(
        pouch, namespace_name, sidecar, &entry->trigram_generation, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_capture_path_signature(
        pouch, namespace_name, lc_pouch_query_index_trigram_term_path,
        &entry->trigram_signature, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_load_doc_table_generation(
        pouch, namespace_name, sidecar, &entry->doc_table, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_capture_path_signature(
        pouch, namespace_name, lc_pouch_query_index_doc_table_path,
        &entry->doc_table_signature, error);
  }
  if (rc != LC_OK) {
    lc_pouch_query_index_prepared_text_cleanup(&pouch->allocator, entry);
    return rc;
  }
  entry->index_seq = sidecar->index_seq;
  entry->row_count = sidecar->row_count;
  entry->row_hash = sidecar->row_hash;
  lc_pouch_query_index_prepared_text_remove_namespace(pouch, namespace_name);
  entry->next = pouch->prepared_text_readers;
  pouch->prepared_text_readers = entry;
  *out = entry;
  return LC_OK;
}

static int lc_pouch_query_index_prepare_range_reader(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar,
    lc_pouch_query_index_prepared_range **out, lc_error *error) {
  lc_pouch_query_index_prepared_range *entry;
  int generation_valid;
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index prepared range reader requires "
                        "output",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  if (pouch == NULL || namespace_name == NULL || sidecar == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index prepared range reader requires "
                        "pouch, namespace, and sidecar",
                        NULL, NULL, NULL);
  }
  entry =
      lc_pouch_query_index_prepared_range_find(pouch, namespace_name, sidecar);
  if (entry != NULL) {
    generation_valid = 0;
    rc = lc_pouch_query_index_validate_term_generation_signature(
        pouch, namespace_name, sidecar, lc_pouch_query_index_range_term_path,
        &entry->range_signature, &generation_valid, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (generation_valid) {
      rc = lc_pouch_query_index_validate_doc_table_signature(
          pouch, namespace_name, sidecar, &entry->doc_table_signature,
          &generation_valid, error);
      if (rc != LC_OK) {
        return rc;
      }
    }
    if (generation_valid) {
      *out = entry;
      return LC_OK;
    }
    lc_pouch_query_index_prepared_range_remove_namespace(pouch, namespace_name);
    entry = NULL;
  }
  entry = (lc_pouch_query_index_prepared_range *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*entry));
  if (entry == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch prepared range reader", NULL,
                        NULL, NULL);
  }
  entry->namespace_name =
      lc_strdup_with_allocator(&pouch->allocator, namespace_name);
  if (entry->namespace_name == NULL) {
    lc_pouch_query_index_prepared_range_cleanup(&pouch->allocator, entry);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch prepared range namespace",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_query_index_load_range_term_generation(
      pouch, namespace_name, sidecar, &entry->range_generation, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_capture_path_signature(
        pouch, namespace_name, lc_pouch_query_index_range_term_path,
        &entry->range_signature, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_load_doc_table_generation(
        pouch, namespace_name, sidecar, &entry->doc_table, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_capture_path_signature(
        pouch, namespace_name, lc_pouch_query_index_doc_table_path,
        &entry->doc_table_signature, error);
  }
  if (rc != LC_OK) {
    lc_pouch_query_index_prepared_range_cleanup(&pouch->allocator, entry);
    return rc;
  }
  entry->index_seq = sidecar->index_seq;
  entry->row_count = sidecar->row_count;
  entry->row_hash = sidecar->row_hash;
  lc_pouch_query_index_prepared_range_remove_namespace(pouch, namespace_name);
  entry->next = pouch->prepared_range_readers;
  pouch->prepared_range_readers = entry;
  *out = entry;
  return LC_OK;
}

static int lc_pouch_query_index_prepare_temporal_reader(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar,
    lc_pouch_query_index_prepared_temporal **out, lc_error *error) {
  lc_pouch_query_index_prepared_temporal *entry;
  int generation_valid;
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index prepared temporal reader requires "
                        "output",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  if (pouch == NULL || namespace_name == NULL || sidecar == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index prepared temporal reader requires "
                        "pouch, namespace, and sidecar",
                        NULL, NULL, NULL);
  }
  entry = lc_pouch_query_index_prepared_temporal_find(pouch, namespace_name,
                                                      sidecar);
  if (entry != NULL) {
    generation_valid = 0;
    rc = lc_pouch_query_index_validate_term_generation_signature(
        pouch, namespace_name, sidecar, lc_pouch_query_index_temporal_term_path,
        &entry->temporal_signature, &generation_valid, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (generation_valid) {
      rc = lc_pouch_query_index_validate_doc_table_signature(
          pouch, namespace_name, sidecar, &entry->doc_table_signature,
          &generation_valid, error);
      if (rc != LC_OK) {
        return rc;
      }
    }
    if (generation_valid) {
      *out = entry;
      return LC_OK;
    }
    lc_pouch_query_index_prepared_temporal_remove_namespace(pouch,
                                                            namespace_name);
    entry = NULL;
  }
  entry = (lc_pouch_query_index_prepared_temporal *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*entry));
  if (entry == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch prepared temporal reader",
                        NULL, NULL, NULL);
  }
  entry->namespace_name =
      lc_strdup_with_allocator(&pouch->allocator, namespace_name);
  if (entry->namespace_name == NULL) {
    lc_pouch_query_index_prepared_temporal_cleanup(&pouch->allocator, entry);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch prepared temporal namespace",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_query_index_load_temporal_term_generation(
      pouch, namespace_name, sidecar, &entry->temporal_generation, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_capture_path_signature(
        pouch, namespace_name, lc_pouch_query_index_temporal_term_path,
        &entry->temporal_signature, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_load_doc_table_generation(
        pouch, namespace_name, sidecar, &entry->doc_table, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_capture_path_signature(
        pouch, namespace_name, lc_pouch_query_index_doc_table_path,
        &entry->doc_table_signature, error);
  }
  if (rc != LC_OK) {
    lc_pouch_query_index_prepared_temporal_cleanup(&pouch->allocator, entry);
    return rc;
  }
  entry->index_seq = sidecar->index_seq;
  entry->row_count = sidecar->row_count;
  entry->row_hash = sidecar->row_hash;
  lc_pouch_query_index_prepared_temporal_remove_namespace(pouch,
                                                          namespace_name);
  entry->next = pouch->prepared_temporal_readers;
  pouch->prepared_temporal_readers = entry;
  *out = entry;
  return LC_OK;
}

static int lc_pouch_query_index_prepare_presence_reader(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_read_result *sidecar,
    lc_pouch_query_index_prepared_presence **out, lc_error *error) {
  lc_pouch_query_index_prepared_presence *entry;
  int generation_valid;
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index prepared presence reader requires "
                        "output",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  if (pouch == NULL || namespace_name == NULL || sidecar == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index prepared presence reader requires "
                        "pouch, namespace, and sidecar",
                        NULL, NULL, NULL);
  }
  entry = lc_pouch_query_index_prepared_presence_find(pouch, namespace_name,
                                                      sidecar);
  if (entry != NULL) {
    generation_valid = 0;
    rc = lc_pouch_query_index_validate_term_generation_signature(
        pouch, namespace_name, sidecar, lc_pouch_query_index_presence_term_path,
        &entry->presence_signature, &generation_valid, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (generation_valid) {
      rc = lc_pouch_query_index_validate_doc_table_signature(
          pouch, namespace_name, sidecar, &entry->doc_table_signature,
          &generation_valid, error);
      if (rc != LC_OK) {
        return rc;
      }
    }
    if (generation_valid) {
      *out = entry;
      return LC_OK;
    }
    lc_pouch_query_index_prepared_presence_remove_namespace(pouch,
                                                            namespace_name);
    entry = NULL;
  }
  entry = (lc_pouch_query_index_prepared_presence *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*entry));
  if (entry == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch prepared presence reader",
                        NULL, NULL, NULL);
  }
  entry->namespace_name =
      lc_strdup_with_allocator(&pouch->allocator, namespace_name);
  if (entry->namespace_name == NULL) {
    lc_pouch_query_index_prepared_presence_cleanup(&pouch->allocator, entry);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch prepared presence namespace",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_query_index_load_presence_term_generation(
      pouch, namespace_name, sidecar, &entry->presence_generation, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_capture_path_signature(
        pouch, namespace_name, lc_pouch_query_index_presence_term_path,
        &entry->presence_signature, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_load_doc_table_generation(
        pouch, namespace_name, sidecar, &entry->doc_table, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_capture_path_signature(
        pouch, namespace_name, lc_pouch_query_index_doc_table_path,
        &entry->doc_table_signature, error);
  }
  if (rc != LC_OK) {
    lc_pouch_query_index_prepared_presence_cleanup(&pouch->allocator, entry);
    return rc;
  }
  entry->index_seq = sidecar->index_seq;
  entry->row_count = sidecar->row_count;
  entry->row_hash = sidecar->row_hash;
  lc_pouch_query_index_prepared_presence_remove_namespace(pouch,
                                                          namespace_name);
  entry->next = pouch->prepared_presence_readers;
  pouch->prepared_presence_readers = entry;
  *out = entry;
  return LC_OK;
}

static int lc_pouch_query_index_collect_exact_generation_docids(
    const lc_allocator *allocator,
    const lc_pouch_index_term_generation *generation,
    const lc_pouch_index_term_key *exact_terms, size_t exact_term_count,
    lc_pouch_index_result_docid_list *docids, lc_error *error) {
  lc_pouch_index_docid_set set;
  unsigned long term_id;
  size_t term_index;
  size_t doc_index;
  int rc;

  if (generation == NULL || exact_terms == NULL || docids == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index exact generation collection "
                        "requires generation, terms, and docIDs",
                        NULL, NULL, NULL);
  }
  rc = LC_OK;
  memset(&set, 0, sizeof(set));
  for (term_index = 0U; rc == LC_OK && term_index < exact_term_count;
       ++term_index) {
    if (!lc_pouch_index_term_table_find(
            &generation->terms, exact_terms[term_index].field_hex,
            exact_terms[term_index].value_hex,
            exact_terms[term_index].value_type, &term_id)) {
      continue;
    }
    lc_pouch_index_docid_set_cleanup(allocator, &set);
    rc = lc_pouch_index_term_posting_table_append_to_set(
        &generation->postings, term_id, &set, allocator, error);
    for (doc_index = 0U; rc == LC_OK && doc_index < set.count; ++doc_index) {
      rc = lc_pouch_index_result_docid_list_add(
          allocator, docids, set.items[doc_index], term_index, error);
    }
  }
  lc_pouch_index_docid_set_cleanup(allocator, &set);
  return rc;
}

static char *lc_pouch_query_index_exact_result_key(
    const lc_allocator *allocator, const lc_pouch_index_term_key *exact_terms,
    size_t exact_term_count, lc_error *error) {
  char *key;
  char *cursor;
  size_t length;
  size_t index;
  size_t field_len;
  size_t value_len;

  if (exact_terms == NULL || exact_term_count == 0U) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch exact result cache key requires terms", NULL, NULL,
                 NULL);
    return NULL;
  }
  length = 0U;
  for (index = 0U; index < exact_term_count; ++index) {
    if (exact_terms[index].field_hex == NULL ||
        exact_terms[index].value_hex == NULL ||
        exact_terms[index].value_type == '\0') {
      lc_error_set(error, LC_ERR_INVALID, 0L,
                   "pouch exact result cache key requires complete terms", NULL,
                   NULL, NULL);
      return NULL;
    }
    field_len = strlen(exact_terms[index].field_hex);
    value_len = strlen(exact_terms[index].value_hex);
    if (field_len > (size_t)-1 - length ||
        value_len > (size_t)-1 - length - field_len ||
        5U > (size_t)-1 - length - field_len - value_len) {
      lc_error_set(error, LC_ERR_NOMEM, 0L,
                   "pouch exact result cache key exceeds local limit", NULL,
                   NULL, NULL);
      return NULL;
    }
    length += field_len + value_len + 4U;
  }
  key = (char *)lc_alloc_with_allocator(allocator, length + 1U);
  if (key == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch exact result cache key", NULL, NULL,
                 NULL);
    return NULL;
  }
  cursor = key;
  for (index = 0U; index < exact_term_count; ++index) {
    field_len = strlen(exact_terms[index].field_hex);
    value_len = strlen(exact_terms[index].value_hex);
    memcpy(cursor, exact_terms[index].field_hex, field_len);
    cursor += field_len;
    *cursor++ = ':';
    *cursor++ = exact_terms[index].value_type;
    *cursor++ = ':';
    memcpy(cursor, exact_terms[index].value_hex, value_len);
    cursor += value_len;
    *cursor++ = '\n';
  }
  *cursor = '\0';
  return key;
}

static int lc_pouch_query_index_docid_list_copy(
    const lc_allocator *allocator,
    const lc_pouch_index_result_docid_list *source,
    lc_pouch_index_result_docid_list *out, lc_error *error) {
  size_t index;
  int rc;

  if (source == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch docID result copy requires source and output",
                        NULL, NULL, NULL);
  }
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < source->count; ++index) {
    rc = lc_pouch_index_result_docid_list_add(
        allocator, out, source->items[index].doc_id,
        source->items[index].value_index, error);
  }
  return rc;
}

static int lc_pouch_query_index_result_cache_store(
    const lc_allocator *allocator, char **slot_key,
    lc_pouch_index_result_docid_list *slot_docids, char **result_key,
    lc_pouch_index_result_docid_list *collected,
    lc_pouch_index_result_docid_list *out, lc_error *error) {
  if (slot_key == NULL || slot_docids == NULL || result_key == NULL ||
      *result_key == NULL || collected == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch result cache store requires complete slots",
                        NULL, NULL, NULL);
  }
  lc_free_with_allocator(allocator, *slot_key);
  lc_pouch_index_result_docid_list_cleanup(allocator, slot_docids);
  *slot_key = *result_key;
  *result_key = NULL;
  *slot_docids = *collected;
  memset(collected, 0, sizeof(*collected));
  return lc_pouch_query_index_docid_list_copy(allocator, slot_docids, out,
                                              error);
}

static int lc_pouch_query_index_result_cache_hit(
    const lc_allocator *allocator, const char *slot_key,
    const lc_pouch_index_result_docid_list *slot_docids, const char *result_key,
    lc_pouch_index_result_docid_list *out, int *hit, lc_error *error) {
  if (out == NULL || hit == NULL || result_key == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch result cache lookup requires key and output",
                        NULL, NULL, NULL);
  }
  *hit = 0;
  if (slot_key == NULL || strcmp(slot_key, result_key) != 0) {
    return LC_OK;
  }
  *hit = 1;
  return lc_pouch_query_index_docid_list_copy(allocator, slot_docids, out,
                                              error);
}

static int
lc_pouch_query_index_result_key_append_number(lc_pouch_query_index_text *text,
                                              const char *name, int present,
                                              double value, lc_error *error) {
  char line[96];
  int written;

  written = snprintf(line, sizeof(line), "%s:%d:", name, present ? 1 : 0);
  if (written < 0 || (size_t)written >= sizeof(line)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch result cache key exceeds local limit", NULL,
                        NULL, NULL);
  }
  if (lc_pouch_query_index_text_append(text, line, (size_t)written, NULL,
                                       error) != LC_OK) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  if (present) {
    written = snprintf(line, sizeof(line), "%.17g", value);
    if (written < 0 || (size_t)written >= sizeof(line)) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch result cache number key exceeds local limit",
                          NULL, NULL, NULL);
    }
    if (lc_pouch_query_index_text_append(text, line, (size_t)written, NULL,
                                         error) != LC_OK) {
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
  }
  return lc_pouch_query_index_text_append_cstr(text, "\n", NULL, error);
}

static char *lc_pouch_query_index_presence_result_key(
    const lc_allocator *allocator, const char *field_hex, lc_error *error) {
  lc_pouch_query_index_text text;

  if (field_hex == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch presence result cache key requires field", NULL, NULL,
                 NULL);
    return NULL;
  }
  memset(&text, 0, sizeof(text));
  text.allocator = allocator;
  if (lc_pouch_query_index_text_append_cstr(&text, "presence\n", NULL, error) !=
          LC_OK ||
      lc_pouch_query_index_text_append_cstr(&text, field_hex, NULL, error) !=
          LC_OK ||
      lc_pouch_query_index_text_append_cstr(&text, "\n", NULL, error) !=
          LC_OK) {
    lc_free_with_allocator(allocator, text.bytes);
    return NULL;
  }
  return text.bytes;
}

static char *lc_pouch_query_index_range_result_key(
    const lc_allocator *allocator, const char *field_hex,
    const lc_pouch_query_index_range_bounds *bounds, lc_error *error) {
  lc_pouch_query_index_text text;

  if (field_hex == NULL || bounds == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch range result cache key requires field and bounds", NULL,
                 NULL, NULL);
    return NULL;
  }
  memset(&text, 0, sizeof(text));
  text.allocator = allocator;
  if (lc_pouch_query_index_text_append_cstr(&text, "range\n", NULL, error) !=
          LC_OK ||
      lc_pouch_query_index_text_append_cstr(&text, field_hex, NULL, error) !=
          LC_OK ||
      lc_pouch_query_index_text_append_cstr(&text, "\n", NULL, error) !=
          LC_OK ||
      lc_pouch_query_index_result_key_append_number(
          &text, "gt", bounds->has_gt, bounds->gt, error) != LC_OK ||
      lc_pouch_query_index_result_key_append_number(
          &text, "gte", bounds->has_gte, bounds->gte, error) != LC_OK ||
      lc_pouch_query_index_result_key_append_number(
          &text, "lt", bounds->has_lt, bounds->lt, error) != LC_OK ||
      lc_pouch_query_index_result_key_append_number(
          &text, "lte", bounds->has_lte, bounds->lte, error) != LC_OK) {
    lc_free_with_allocator(allocator, text.bytes);
    return NULL;
  }
  return text.bytes;
}

static char *lc_pouch_query_index_text_result_key(
    const lc_allocator *allocator, const char *field_hex,
    const char *needle_hex, int prefix_match, int contains_match,
    int ignore_case, lc_error *error) {
  lc_pouch_query_index_text text;
  char line[64];
  int written;

  if (field_hex == NULL || needle_hex == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch text result cache key requires field and needle", NULL,
                 NULL, NULL);
    return NULL;
  }
  memset(&text, 0, sizeof(text));
  text.allocator = allocator;
  written =
      snprintf(line, sizeof(line), "text:%d:%d:%d\n", prefix_match ? 1 : 0,
               contains_match ? 1 : 0, ignore_case ? 1 : 0);
  if (written < 0 || (size_t)written >= sizeof(line)) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch text result cache key exceeds local limit", NULL, NULL,
                 NULL);
    return NULL;
  }
  if (lc_pouch_query_index_text_append(&text, line, (size_t)written, NULL,
                                       error) != LC_OK ||
      lc_pouch_query_index_text_append_cstr(&text, field_hex, NULL, error) !=
          LC_OK ||
      lc_pouch_query_index_text_append_cstr(&text, "\n", NULL, error) !=
          LC_OK ||
      lc_pouch_query_index_text_append_cstr(&text, needle_hex, NULL, error) !=
          LC_OK ||
      lc_pouch_query_index_text_append_cstr(&text, "\n", NULL, error) !=
          LC_OK) {
    lc_free_with_allocator(allocator, text.bytes);
    return NULL;
  }
  return text.bytes;
}

static char *lc_pouch_query_index_temporal_result_key(
    const lc_allocator *allocator, const char *field_hex,
    const lc_pouch_index_parsed_date_bounds *bounds, lc_error *error) {
  lc_pouch_query_index_text text;

  if (field_hex == NULL || bounds == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch temporal result cache key requires field and bounds",
                 NULL, NULL, NULL);
    return NULL;
  }
  memset(&text, 0, sizeof(text));
  text.allocator = allocator;
  if (lc_pouch_query_index_text_append_cstr(&text, "temporal\n", NULL, error) !=
          LC_OK ||
      lc_pouch_query_index_text_append_cstr(&text, field_hex, NULL, error) !=
          LC_OK ||
      lc_pouch_query_index_text_append_cstr(&text, "\n", NULL, error) !=
          LC_OK ||
      lc_pouch_query_index_result_key_append_number(
          &text, "gt", bounds->has_gt, bounds->gt.seconds, error) != LC_OK ||
      lc_pouch_query_index_result_key_append_number(
          &text, "gte", bounds->has_gte, bounds->gte.seconds, error) != LC_OK ||
      lc_pouch_query_index_result_key_append_number(
          &text, "lt", bounds->has_lt, bounds->lt.seconds, error) != LC_OK ||
      lc_pouch_query_index_result_key_append_number(
          &text, "lte", bounds->has_lte, bounds->lte.seconds, error) != LC_OK) {
    lc_free_with_allocator(allocator, text.bytes);
    return NULL;
  }
  return text.bytes;
}

static int lc_pouch_query_index_prepared_exact_collect(
    const lc_allocator *allocator,
    lc_pouch_query_index_prepared_exact *prepared,
    const lc_pouch_index_term_key *exact_terms, size_t exact_term_count,
    lc_pouch_index_result_docid_list *docids, lc_error *error) {
  lc_pouch_index_result_docid_list collected;
  char *result_key;
  int rc;

  if (prepared == NULL || docids == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch prepared exact collect requires reader and "
                        "docIDs",
                        NULL, NULL, NULL);
  }
  memset(&collected, 0, sizeof(collected));
  result_key = lc_pouch_query_index_exact_result_key(allocator, exact_terms,
                                                     exact_term_count, error);
  if (result_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  if (prepared->last_result_key != NULL &&
      strcmp(prepared->last_result_key, result_key) == 0) {
    lc_free_with_allocator(allocator, result_key);
    return lc_pouch_query_index_docid_list_copy(
        allocator, &prepared->last_result_docids, docids, error);
  }
  rc = lc_pouch_query_index_collect_exact_generation_docids(
      allocator, &prepared->exact_generation, exact_terms, exact_term_count,
      &collected, error);
  if (rc == LC_OK) {
    lc_free_with_allocator(allocator, prepared->last_result_key);
    lc_pouch_index_result_docid_list_cleanup(allocator,
                                             &prepared->last_result_docids);
    prepared->last_result_key = result_key;
    result_key = NULL;
    prepared->last_result_docids = collected;
    memset(&collected, 0, sizeof(collected));
    rc = lc_pouch_query_index_docid_list_copy(
        allocator, &prepared->last_result_docids, docids, error);
  }
  lc_pouch_index_result_docid_list_cleanup(allocator, &collected);
  lc_free_with_allocator(allocator, result_key);
  return rc;
}

static int lc_pouch_query_index_prepared_presence_collect(
    const lc_allocator *allocator,
    lc_pouch_query_index_prepared_presence *prepared, const char *field_hex,
    lc_pouch_index_result_docid_list *docids, lc_error *error) {
  lc_pouch_index_result_docid_list collected;
  lc_pouch_index_term_key presence_term;
  char *result_key;
  int hit;
  int rc;

  if (prepared == NULL || field_hex == NULL || docids == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch prepared presence collect requires reader, "
                        "field, and docIDs",
                        NULL, NULL, NULL);
  }
  memset(&collected, 0, sizeof(collected));
  memset(&presence_term, 0, sizeof(presence_term));
  result_key =
      lc_pouch_query_index_presence_result_key(allocator, field_hex, error);
  if (result_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_query_index_result_cache_hit(
      allocator, prepared->last_result_key, &prepared->last_result_docids,
      result_key, docids, &hit, error);
  if (rc == LC_OK && !hit) {
    presence_term.field_hex = field_hex;
    presence_term.value_hex = "-";
    presence_term.value_type = 'z';
    rc = lc_pouch_query_index_collect_exact_generation_docids(
        allocator, &prepared->presence_generation, &presence_term, 1U,
        &collected, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_result_cache_store(
          allocator, &prepared->last_result_key, &prepared->last_result_docids,
          &result_key, &collected, docids, error);
    }
  }
  lc_pouch_index_result_docid_list_cleanup(allocator, &collected);
  lc_free_with_allocator(allocator, result_key);
  return rc;
}

static int lc_pouch_query_index_collect_range_generation_docids(
    const lc_allocator *allocator,
    const lc_pouch_index_term_generation *generation, const char *field_hex,
    const lc_pouch_query_index_range_bounds *bounds,
    lc_pouch_index_result_docid_list *docids, lc_error *error) {
  lc_pouch_index_docid_set set;
  const lc_pouch_index_term_entry *term;
  char *value_text;
  double number;
  size_t term_index;
  size_t doc_index;
  int rc;

  if (generation == NULL || field_hex == NULL || bounds == NULL ||
      docids == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index range generation collection "
                        "requires generation, field, bounds, and docIDs",
                        NULL, NULL, NULL);
  }
  rc = LC_OK;
  memset(&set, 0, sizeof(set));
  for (term_index = 0U; rc == LC_OK && term_index < generation->terms.count;
       ++term_index) {
    term = &generation->terms.items[term_index];
    if (term->field_hex == NULL || strcmp(term->field_hex, field_hex) != 0 ||
        term->value_type != 'n' || term->value_hex == NULL ||
        strcmp(term->value_hex, "-") == 0) {
      continue;
    }
    value_text =
        lc_pouch_query_index_hex_decode(allocator, term->value_hex, error);
    if (value_text == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      break;
    }
    if (lc_pouch_query_index_parse_number_value(value_text, &number) &&
        lc_pouch_query_index_range_contains_value(bounds, number)) {
      lc_pouch_index_docid_set_cleanup(allocator, &set);
      rc = lc_pouch_index_term_posting_table_append_to_set(
          &generation->postings, term->term_id, &set, allocator, error);
      for (doc_index = 0U; rc == LC_OK && doc_index < set.count; ++doc_index) {
        rc = lc_pouch_index_result_docid_list_add(
            allocator, docids, set.items[doc_index], term_index, error);
      }
    }
    lc_free_with_allocator(allocator, value_text);
  }
  lc_pouch_index_docid_set_cleanup(allocator, &set);
  return rc;
}

static int lc_pouch_query_index_prepared_range_collect(
    const lc_allocator *allocator,
    lc_pouch_query_index_prepared_range *prepared, const char *field_hex,
    const lc_pouch_query_index_range_bounds *bounds,
    lc_pouch_index_result_docid_list *docids, lc_error *error) {
  lc_pouch_index_result_docid_list collected;
  char *result_key;
  int hit;
  int rc;

  if (prepared == NULL || field_hex == NULL || bounds == NULL ||
      docids == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch prepared range collect requires reader, field, "
                        "bounds, and docIDs",
                        NULL, NULL, NULL);
  }
  memset(&collected, 0, sizeof(collected));
  result_key = lc_pouch_query_index_range_result_key(allocator, field_hex,
                                                     bounds, error);
  if (result_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_query_index_result_cache_hit(
      allocator, prepared->last_result_key, &prepared->last_result_docids,
      result_key, docids, &hit, error);
  if (rc == LC_OK && !hit) {
    rc = lc_pouch_query_index_collect_range_generation_docids(
        allocator, &prepared->range_generation, field_hex, bounds, &collected,
        error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_result_cache_store(
          allocator, &prepared->last_result_key, &prepared->last_result_docids,
          &result_key, &collected, docids, error);
    }
  }
  lc_pouch_index_result_docid_list_cleanup(allocator, &collected);
  lc_free_with_allocator(allocator, result_key);
  return rc;
}

static int lc_pouch_query_index_collect_temporal_generation_docids(
    const lc_allocator *allocator,
    const lc_pouch_index_term_generation *generation, const char *field_hex,
    const lc_pouch_index_parsed_date_bounds *bounds,
    lc_pouch_index_result_docid_list *docids, lc_error *error) {
  lc_pouch_index_docid_set set;
  const lc_pouch_index_term_entry *term;
  lc_pouch_index_instant instant;
  char *value_text;
  double seconds;
  size_t term_index;
  size_t doc_index;
  int rc;

  if (generation == NULL || field_hex == NULL || bounds == NULL ||
      docids == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index temporal generation collection "
                        "requires generation, field, bounds, and docIDs",
                        NULL, NULL, NULL);
  }
  rc = LC_OK;
  memset(&set, 0, sizeof(set));
  for (term_index = 0U; rc == LC_OK && term_index < generation->terms.count;
       ++term_index) {
    term = &generation->terms.items[term_index];
    if (term->field_hex == NULL || strcmp(term->field_hex, field_hex) != 0 ||
        term->value_type != 'n' || term->value_hex == NULL ||
        strcmp(term->value_hex, "-") == 0) {
      continue;
    }
    value_text =
        lc_pouch_query_index_hex_decode(allocator, term->value_hex, error);
    if (value_text == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      break;
    }
    if (lc_pouch_query_index_parse_number_value(value_text, &seconds)) {
      instant.seconds = seconds;
      if (lc_pouch_index_date_contains_value(bounds, &instant)) {
        lc_pouch_index_docid_set_cleanup(allocator, &set);
        rc = lc_pouch_index_term_posting_table_append_to_set(
            &generation->postings, term->term_id, &set, allocator, error);
        for (doc_index = 0U; rc == LC_OK && doc_index < set.count;
             ++doc_index) {
          rc = lc_pouch_index_result_docid_list_add(
              allocator, docids, set.items[doc_index], term_index, error);
        }
      }
    }
    lc_free_with_allocator(allocator, value_text);
  }
  lc_pouch_index_docid_set_cleanup(allocator, &set);
  return rc;
}

static int lc_pouch_query_index_prepared_temporal_collect(
    const lc_allocator *allocator,
    lc_pouch_query_index_prepared_temporal *prepared, const char *field_hex,
    const lc_pouch_index_parsed_date_bounds *bounds,
    lc_pouch_index_result_docid_list *docids, lc_error *error) {
  lc_pouch_index_result_docid_list collected;
  char *result_key;
  int hit;
  int rc;

  if (prepared == NULL || field_hex == NULL || bounds == NULL ||
      docids == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch prepared temporal collect requires reader, "
                        "field, bounds, and docIDs",
                        NULL, NULL, NULL);
  }
  memset(&collected, 0, sizeof(collected));
  result_key = lc_pouch_query_index_temporal_result_key(allocator, field_hex,
                                                        bounds, error);
  if (result_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_query_index_result_cache_hit(
      allocator, prepared->last_result_key, &prepared->last_result_docids,
      result_key, docids, &hit, error);
  if (rc == LC_OK && !hit) {
    rc = lc_pouch_query_index_collect_temporal_generation_docids(
        allocator, &prepared->temporal_generation, field_hex, bounds,
        &collected, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_result_cache_store(
          allocator, &prepared->last_result_key, &prepared->last_result_docids,
          &result_key, &collected, docids, error);
    }
  }
  lc_pouch_index_result_docid_list_cleanup(allocator, &collected);
  lc_free_with_allocator(allocator, result_key);
  return rc;
}

static int lc_pouch_query_index_text_generation_matches(
    const char *value_hex, const char *needle_hex, const char *needle_text,
    int prefix_match, int contains_match, int ignore_case) {
  if (value_hex == NULL) {
    return 0;
  }
  if (prefix_match) {
    if (ignore_case) {
      return lc_pouch_query_index_hex_text_has_prefix(value_hex, needle_text,
                                                      1);
    }
    return needle_hex != NULL &&
           strncmp(value_hex, needle_hex, strlen(needle_hex)) == 0;
  }
  if (contains_match) {
    if (ignore_case) {
      return lc_pouch_query_index_hex_text_contains(value_hex, needle_text, 1);
    }
    return needle_hex != NULL &&
           lc_pouch_query_index_hex_contains_aligned(value_hex, needle_hex);
  }
  return 0;
}

static int
lc_pouch_query_index_docid_set_contains(const lc_pouch_index_docid_set *set,
                                        unsigned long doc_id) {
  size_t low;
  size_t high;
  size_t mid;

  if (set == NULL) {
    return 1;
  }
  low = 0U;
  high = set->count;
  while (low < high) {
    mid = low + ((high - low) / 2U);
    if (set->items[mid] < doc_id) {
      low = mid + 1U;
    } else {
      high = mid;
    }
  }
  return low < set->count && set->items[low] == doc_id;
}

static int lc_pouch_query_index_field_hex_is_any_text(const char *field_hex) {
  return field_hex != NULL &&
         strcmp(field_hex, LC_POUCH_QUERY_INDEX_ANY_TEXT_FIELD_HEX) == 0;
}

static int lc_pouch_query_index_collect_trigram_candidate_docids(
    const lc_allocator *allocator,
    const lc_pouch_index_term_generation *generation, const char *field_hex,
    const char *needle_text, int ignore_case, lc_pouch_index_docid_set *docids,
    int *used, lc_error *error) {
  lc_pouch_index_docid_set current;
  lc_pouch_index_docid_set gram_docids;
  lc_pouch_index_docid_set posting;
  lc_pouch_index_docid_set next;
  size_t needle_length;
  size_t index;
  int have_current;
  int any_field;
  int rc;

  if (generation == NULL || field_hex == NULL || needle_text == NULL ||
      docids == NULL || used == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch trigram candidate collection requires "
                        "generation, field, needle, and output",
                        NULL, NULL, NULL);
  }
  memset(&current, 0, sizeof(current));
  memset(&gram_docids, 0, sizeof(gram_docids));
  memset(&posting, 0, sizeof(posting));
  memset(&next, 0, sizeof(next));
  *used = 0;
  needle_length = strlen(needle_text);
  if (needle_length < 3U) {
    return LC_OK;
  }
  have_current = 0;
  any_field = lc_pouch_query_index_field_hex_is_any_text(field_hex);
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index + 3U <= needle_length; ++index) {
    const unsigned char *bytes;
    char *value_hex;
    unsigned long term_id;
    int have_gram_docids;
    size_t term_index;

    bytes = (const unsigned char *)needle_text + index;
    value_hex =
        lc_pouch_query_index_trigram_value_hex(allocator, bytes, ignore_case);
    if (value_hex == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch trigram lookup key", NULL,
                        NULL, NULL);
      break;
    }
    lc_pouch_index_docid_set_cleanup(allocator, &gram_docids);
    memset(&gram_docids, 0, sizeof(gram_docids));
    have_gram_docids = 0;
    if (!any_field) {
      term_id = 0UL;
      if (lc_pouch_index_term_table_find(&generation->terms, field_hex,
                                         value_hex, 's', &term_id)) {
        rc = lc_pouch_index_term_posting_table_append_to_set(
            &generation->postings, term_id, &gram_docids, allocator, error);
        have_gram_docids = rc == LC_OK ? 1 : 0;
      }
    } else {
      for (term_index = 0U; rc == LC_OK && term_index < generation->terms.count;
           ++term_index) {
        const lc_pouch_index_term_entry *term;

        term = &generation->terms.items[term_index];
        if (term->value_type != 's' || term->value_hex == NULL ||
            strcmp(term->value_hex, value_hex) != 0) {
          continue;
        }
        lc_pouch_index_docid_set_cleanup(allocator, &posting);
        memset(&posting, 0, sizeof(posting));
        rc = lc_pouch_index_term_posting_table_append_to_set(
            &generation->postings, term->term_id, &posting, allocator, error);
        if (rc != LC_OK) {
          break;
        }
        if (!have_gram_docids) {
          gram_docids = posting;
          memset(&posting, 0, sizeof(posting));
          have_gram_docids = 1;
          continue;
        }
        lc_pouch_index_docid_set_cleanup(allocator, &next);
        memset(&next, 0, sizeof(next));
        rc = lc_pouch_index_docid_set_union_sorted(&gram_docids, &posting,
                                                   &next, allocator, error);
        lc_pouch_index_docid_set_cleanup(allocator, &gram_docids);
        lc_pouch_index_docid_set_cleanup(allocator, &posting);
        gram_docids = next;
        memset(&next, 0, sizeof(next));
      }
    }
    lc_free_with_allocator(allocator, value_hex);
    if (rc != LC_OK) {
      break;
    }
    if (!have_gram_docids) {
      lc_pouch_index_docid_set_cleanup(allocator, &current);
      memset(&current, 0, sizeof(current));
      have_current = 1;
      break;
    }
    if (!have_current) {
      current = gram_docids;
      memset(&gram_docids, 0, sizeof(gram_docids));
      have_current = 1;
      continue;
    }
    lc_pouch_index_docid_set_cleanup(allocator, &next);
    rc = lc_pouch_index_docid_set_intersect_sorted(&current, &gram_docids,
                                                   &next, allocator, error);
    lc_pouch_index_docid_set_cleanup(allocator, &current);
    lc_pouch_index_docid_set_cleanup(allocator, &gram_docids);
    current = next;
    memset(&next, 0, sizeof(next));
  }
  if (rc == LC_OK && have_current) {
    *docids = current;
    memset(&current, 0, sizeof(current));
    *used = 1;
  }
  lc_pouch_index_docid_set_cleanup(allocator, &next);
  lc_pouch_index_docid_set_cleanup(allocator, &posting);
  lc_pouch_index_docid_set_cleanup(allocator, &gram_docids);
  lc_pouch_index_docid_set_cleanup(allocator, &current);
  return rc;
}

static int lc_pouch_query_index_collect_text_generation_docids(
    const lc_allocator *allocator,
    const lc_pouch_index_term_generation *generation, const char *field_hex,
    const char *needle_hex, const char *needle_text, int prefix_match,
    int contains_match, int ignore_case,
    const lc_pouch_index_docid_set *candidate_docids,
    lc_pouch_index_result_docid_list *docids, lc_error *error) {
  lc_pouch_index_docid_set set;
  const lc_pouch_index_term_entry *term;
  size_t term_index;
  size_t doc_index;
  int any_field;
  int rc;

  if (generation == NULL || field_hex == NULL || needle_text == NULL ||
      docids == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index text generation collection "
                        "requires generation, field, needle, and docIDs",
                        NULL, NULL, NULL);
  }
  rc = LC_OK;
  any_field = lc_pouch_query_index_field_hex_is_any_text(field_hex);
  memset(&set, 0, sizeof(set));
  for (term_index = 0U; rc == LC_OK && term_index < generation->terms.count;
       ++term_index) {
    int field_cmp;

    term = &generation->terms.items[term_index];
    if (term->field_hex == NULL) {
      continue;
    }
    if (!any_field) {
      field_cmp = strcmp(term->field_hex, field_hex);
      if (field_cmp < 0) {
        continue;
      }
      if (field_cmp > 0) {
        break;
      }
    }
    if (term->value_type != 's' || term->value_hex == NULL ||
        strcmp(term->value_hex, "-") == 0 ||
        !lc_pouch_query_index_text_generation_matches(
            term->value_hex, needle_hex, needle_text, prefix_match,
            contains_match, ignore_case)) {
      continue;
    }
    lc_pouch_index_docid_set_cleanup(allocator, &set);
    rc = lc_pouch_index_term_posting_table_append_to_set(
        &generation->postings, term->term_id, &set, allocator, error);
    for (doc_index = 0U; rc == LC_OK && doc_index < set.count; ++doc_index) {
      if (lc_pouch_query_index_docid_set_contains(candidate_docids,
                                                  set.items[doc_index])) {
        rc = lc_pouch_index_result_docid_list_add(
            allocator, docids, set.items[doc_index], term_index, error);
      }
    }
  }
  lc_pouch_index_docid_set_cleanup(allocator, &set);
  return rc;
}

static int lc_pouch_query_index_prepared_text_collect(
    const lc_allocator *allocator, lc_pouch_query_index_prepared_text *prepared,
    const char *field_hex, const char *needle_hex, const char *needle_text,
    int prefix_match, int contains_match, int ignore_case,
    lc_pouch_index_result_docid_list *docids, lc_error *error) {
  lc_pouch_index_result_docid_list collected;
  lc_pouch_index_docid_set candidate_docids;
  char *result_key;
  int hit;
  int rc;
  int use_candidate_docids;
  int use_trigram_candidates;

  if (prepared == NULL || field_hex == NULL || needle_hex == NULL ||
      needle_text == NULL || docids == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch prepared text collect requires reader, field, "
                        "needle, and docIDs",
                        NULL, NULL, NULL);
  }
  memset(&collected, 0, sizeof(collected));
  memset(&candidate_docids, 0, sizeof(candidate_docids));
  use_candidate_docids = 0;
  use_trigram_candidates = 0;
  result_key = lc_pouch_query_index_text_result_key(
      allocator, field_hex, needle_hex, prefix_match, contains_match,
      ignore_case, error);
  if (result_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_query_index_result_cache_hit(
      allocator, prepared->last_result_key, &prepared->last_result_docids,
      result_key, docids, &hit, error);
  if (rc == LC_OK && !hit) {
    if (contains_match && ignore_case && use_trigram_candidates) {
      rc = lc_pouch_query_index_collect_trigram_candidate_docids(
          allocator, &prepared->trigram_generation, field_hex, needle_text,
          ignore_case, &candidate_docids, &use_candidate_docids, error);
    }
  }
  if (rc == LC_OK && !hit) {
    rc = lc_pouch_query_index_collect_text_generation_docids(
        allocator, &prepared->text_generation, field_hex, needle_hex,
        needle_text, prefix_match, contains_match, ignore_case,
        use_candidate_docids ? &candidate_docids : NULL, &collected, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_result_cache_store(
          allocator, &prepared->last_result_key, &prepared->last_result_docids,
          &result_key, &collected, docids, error);
    }
  }
  lc_pouch_index_docid_set_cleanup(allocator, &candidate_docids);
  lc_pouch_index_result_docid_list_cleanup(allocator, &collected);
  lc_free_with_allocator(allocator, result_key);
  return rc;
}

static int lc_pouch_query_index_visit_text_generation(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *needle, int prefix_match, int contains_match, int ignore_case,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error) {
  lc_pouch_query_index_read_result sidecar;
  lc_pouch_query_index_prepared_text *prepared;
  lc_pouch_index_result_docid_list docids;
  char *sidecar_path;
  char *field_hex;
  char *needle_hex;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      field == NULL || field[0] == '\0' || needle == NULL ||
      needle[0] == '\0' || visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index text lookup requires pouch, "
                        "namespace, field, needle, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  *index_seq = 0UL;
  sidecar_path = lc_pouch_query_index_path(pouch, namespace_name, error);
  if (sidecar_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  field_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, field);
  needle_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, needle);
  if (field_hex == NULL || needle_hex == NULL) {
    lc_free_with_allocator(&pouch->allocator, needle_hex);
    lc_free_with_allocator(&pouch->allocator, field_hex);
    lc_free_with_allocator(&pouch->allocator, sidecar_path);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index text lookup",
                        NULL, NULL, NULL);
  }
  memset(&sidecar, 0, sizeof(sidecar));
  memset(&docids, 0, sizeof(docids));
  prepared = NULL;
  rc = lc_pouch_query_index_read_header(pouch, namespace_name, sidecar_path,
                                        &sidecar, error);
  if (rc == LC_OK && (!sidecar.present || !sidecar.valid)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index sidecar is not readable", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK && !sidecar.term_index_complete) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index scalar postings are incomplete", NULL,
                      NULL, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_prepare_text_reader(pouch, namespace_name,
                                                  &sidecar, &prepared, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_prepared_text_collect(
        &pouch->allocator, prepared, field_hex, needle_hex, needle,
        prefix_match, contains_match, ignore_case, &docids, error);
  }
  if (rc == LC_OK) {
    *index_seq = sidecar.index_seq;
    rc = lc_pouch_query_index_docid_emit_cached(
        &pouch->allocator, &prepared->page_cache, prepared->last_result_key,
        &docids, &prepared->doc_table, visit, context, error);
  }
  lc_pouch_index_result_docid_list_cleanup(&pouch->allocator, &docids);
  lc_free_with_allocator(&pouch->allocator, needle_hex);
  lc_free_with_allocator(&pouch->allocator, field_hex);
  lc_free_with_allocator(&pouch->allocator, sidecar_path);
  return rc;
}

static int lc_pouch_query_index_visit_exact_generation(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_index_term_key *exact_terms, size_t exact_term_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error) {
  lc_pouch_query_index_read_result sidecar;
  lc_pouch_index_result_docid_list docids;
  lc_pouch_query_index_prepared_exact *prepared;
  char *sidecar_path;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      exact_terms == NULL || exact_term_count == 0U || visit == NULL ||
      index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index exact generation visit requires "
                        "pouch, namespace, terms, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  memset(&sidecar, 0, sizeof(sidecar));
  memset(&docids, 0, sizeof(docids));
  prepared = NULL;
  *index_seq = 0UL;
  sidecar_path = lc_pouch_query_index_path(pouch, namespace_name, error);
  if (sidecar_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_query_index_read_header(pouch, namespace_name, sidecar_path,
                                        &sidecar, error);
  if (rc == LC_OK && (!sidecar.present || !sidecar.valid)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index sidecar is not readable", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK && !sidecar.term_index_complete) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index scalar postings are incomplete", NULL,
                      NULL, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_prepare_exact_reader(pouch, namespace_name,
                                                   &sidecar, &prepared, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_prepared_exact_collect(
        &pouch->allocator, prepared, exact_terms, exact_term_count, &docids,
        error);
  }
  if (rc == LC_OK) {
    *index_seq = sidecar.index_seq;
    rc = lc_pouch_query_index_docid_emit_cached(
        &pouch->allocator, &prepared->page_cache, prepared->last_result_key,
        &docids, &prepared->doc_table, visit, context, error);
  }
  lc_pouch_index_result_docid_list_cleanup(&pouch->allocator, &docids);
  lc_free_with_allocator(&pouch->allocator, sidecar_path);
  return rc;
}

static int
lc_pouch_query_index_any_merge_collect(const lc_pouch_query_index_key_view *key,
                                       void *context, lc_error *error) {
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
    const char *const *values, const char *value_types, size_t value_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error) {
  lc_pouch_query_index_any_merge_context merge_context;
  size_t index;
  int rc;

  memset(&merge_context, 0, sizeof(merge_context));
  if (pouch == NULL || value_count == 0U) {
    return lc_pouch_query_index_visit_scalar_any(
        pouch, namespace_name, field, values, value_types, value_count, visit,
        context, index_seq, error);
  }
  if (value_count == 1U) {
    return lc_pouch_query_index_visit_scalar(pouch, namespace_name, field,
                                             values[0], value_types[0], visit,
                                             context, index_seq, error);
  }
  merge_context.allocator = &pouch->allocator;
  merge_context.list_count = value_count;
  merge_context.lists =
      (lc_pouch_index_result_row_list *)lc_alloc_with_allocator(
          &pouch->allocator, value_count * sizeof(*merge_context.lists));
  if (merge_context.lists == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index merge lists",
                        NULL, NULL, NULL);
  }
  memset(merge_context.lists, 0, value_count * sizeof(*merge_context.lists));
  rc = lc_pouch_query_index_visit_scalar_any(
      pouch, namespace_name, field, values, value_types, value_count,
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
    return lc_pouch_query_index_visit_scalar_terms(pouch, namespace_name, terms,
                                                   term_count, visit, context,
                                                   index_seq, error);
  }
  if (term_count == 1U) {
    return lc_pouch_query_index_visit_scalar(
        pouch, namespace_name, terms[0].field, terms[0].value,
        terms[0].value_type, visit, context, index_seq, error);
  }
  merge_context.allocator = &pouch->allocator;
  merge_context.list_count = term_count;
  merge_context.lists =
      (lc_pouch_index_result_row_list *)lc_alloc_with_allocator(
          &pouch->allocator, term_count * sizeof(*merge_context.lists));
  if (merge_context.lists == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index term merge "
                        "lists",
                        NULL, NULL, NULL);
  }
  memset(merge_context.lists, 0, term_count * sizeof(*merge_context.lists));
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
  lc_pouch_index_term_key *exact_terms;
  size_t exact_term_count;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      terms == NULL || term_count == 0U || visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index scalar term docID lookup requires "
                        "pouch, namespace, terms, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  *index_seq = 0UL;
  exact_terms = NULL;
  exact_term_count = 0U;
  rc = lc_pouch_index_term_keys_build_exact(terms, term_count, &exact_terms,
                                            &exact_term_count,
                                            &pouch->allocator, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_visit_exact_generation(
        pouch, namespace_name, exact_terms, exact_term_count, visit, context,
        index_seq, error);
  }
  lc_pouch_index_term_keys_cleanup(&pouch->allocator, exact_terms,
                                   exact_term_count);
  return rc;
}

int lc_pouch_query_index_visit_scalar_any_docids(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *const *values, const char *value_types, size_t value_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error) {
  lc_pouch_query_index_scalar_term *terms;
  size_t index;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      field == NULL || field[0] == '\0' || values == NULL ||
      value_types == NULL || value_count == 0U || visit == NULL ||
      index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index multi-scalar docID lookup requires "
                        "pouch, namespace, field, values, visitor, and "
                        "index_seq",
                        NULL, NULL, NULL);
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
    if (values[index] == NULL ||
        (value_types[index] != 's' && value_types[index] != 'n' &&
         value_types[index] != 'b' && value_types[index] != 'z')) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index multi-scalar docID lookup requires "
                        "non-null values and scalar value types",
                        NULL, NULL, NULL);
      break;
    }
    terms[index].field = field;
    terms[index].value = values[index];
    terms[index].value_type = value_types[index];
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
                                      const char *field, const char *prefix,
                                      int ignore_case,
                                      lc_pouch_query_index_key_visit_fn visit,
                                      void *context, unsigned long *index_seq,
                                      lc_error *error) {
  if (prefix == NULL || prefix[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index prefix lookup requires non-empty "
                        "prefix",
                        NULL, NULL, NULL);
  }
  return lc_pouch_query_index_visit_text_generation(
      pouch, namespace_name, field, prefix, 1, 0, ignore_case, visit, context,
      index_seq, error);
}

int lc_pouch_query_index_visit_contains(lc_pouch *pouch,
                                        const char *namespace_name,
                                        const char *field, const char *needle,
                                        int ignore_case,
                                        lc_pouch_query_index_key_visit_fn visit,
                                        void *context, unsigned long *index_seq,
                                        lc_error *error) {
  if (needle == NULL || needle[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index contains lookup requires non-empty "
                        "needle",
                        NULL, NULL, NULL);
  }
  return lc_pouch_query_index_visit_text_generation(
      pouch, namespace_name, field, needle, 0, 1, ignore_case, visit, context,
      index_seq, error);
}

int lc_pouch_query_index_visit_range(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const lc_pouch_query_index_range_bounds *bounds,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error) {
  lc_pouch_query_index_read_result sidecar;
  lc_pouch_query_index_prepared_range *prepared;
  lc_pouch_index_result_docid_list docids;
  char *sidecar_path;
  char *field_hex;
  int rc;

  if (bounds == NULL || (!bounds->has_gt && !bounds->has_gte &&
                         !bounds->has_lt && !bounds->has_lte)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index range lookup requires numeric "
                        "bounds",
                        NULL, NULL, NULL);
  }
  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      field == NULL || field[0] == '\0' || visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index range lookup requires pouch, "
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
                        "failed to allocate pouch query-index range lookup",
                        NULL, NULL, NULL);
  }
  memset(&sidecar, 0, sizeof(sidecar));
  memset(&docids, 0, sizeof(docids));
  prepared = NULL;
  rc = lc_pouch_query_index_read_header(pouch, namespace_name, sidecar_path,
                                        &sidecar, error);
  if (rc == LC_OK && (!sidecar.present || !sidecar.valid)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index sidecar is not readable", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK && !sidecar.term_index_complete) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index scalar postings are incomplete", NULL,
                      NULL, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_prepare_range_reader(pouch, namespace_name,
                                                   &sidecar, &prepared, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_prepared_range_collect(
        &pouch->allocator, prepared, field_hex, bounds, &docids, error);
  }
  if (rc == LC_OK) {
    *index_seq = sidecar.index_seq;
    rc = lc_pouch_query_index_docid_emit_cached(
        &pouch->allocator, &prepared->page_cache, prepared->last_result_key,
        &docids, &prepared->doc_table, visit, context, error);
  }
  lc_pouch_index_result_docid_list_cleanup(&pouch->allocator, &docids);
  lc_free_with_allocator(&pouch->allocator, field_hex);
  lc_free_with_allocator(&pouch->allocator, sidecar_path);
  return rc;
}

int lc_pouch_query_index_visit_date(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const lc_pouch_query_index_date_bounds *bounds,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error) {
  lc_pouch_query_index_read_result sidecar;
  lc_pouch_query_index_prepared_temporal *prepared;
  lc_pouch_index_parsed_date_bounds parsed_bounds;
  lc_pouch_index_result_docid_list docids;
  char *sidecar_path;
  char *field_hex;
  int rc;

  if (bounds == NULL || (!bounds->has_gt && !bounds->has_gte &&
                         !bounds->has_lt && !bounds->has_lte)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index date lookup requires temporal "
                        "bounds",
                        NULL, NULL, NULL);
  }
  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      field == NULL || field[0] == '\0' || visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index date lookup requires pouch, "
                        "namespace, field, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_index_parse_date_bounds(bounds, &parsed_bounds, error);
  if (rc != LC_OK) {
    return rc;
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
                        "failed to allocate pouch query-index date lookup",
                        NULL, NULL, NULL);
  }
  memset(&sidecar, 0, sizeof(sidecar));
  memset(&docids, 0, sizeof(docids));
  prepared = NULL;
  rc = lc_pouch_query_index_read_header(pouch, namespace_name, sidecar_path,
                                        &sidecar, error);
  if (rc == LC_OK && (!sidecar.present || !sidecar.valid)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index sidecar is not readable", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK && !sidecar.term_index_complete) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index scalar postings are incomplete", NULL,
                      NULL, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_prepare_temporal_reader(
        pouch, namespace_name, &sidecar, &prepared, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_prepared_temporal_collect(
        &pouch->allocator, prepared, field_hex, &parsed_bounds, &docids, error);
  }
  if (rc == LC_OK) {
    *index_seq = sidecar.index_seq;
    rc = lc_pouch_query_index_docid_emit_cached(
        &pouch->allocator, &prepared->page_cache, prepared->last_result_key,
        &docids, &prepared->doc_table, visit, context, error);
  }
  lc_pouch_index_result_docid_list_cleanup(&pouch->allocator, &docids);
  lc_free_with_allocator(&pouch->allocator, field_hex);
  lc_free_with_allocator(&pouch->allocator, sidecar_path);
  return rc;
}

int lc_pouch_query_index_visit_exists(lc_pouch *pouch,
                                      const char *namespace_name,
                                      const char *field,
                                      lc_pouch_query_index_key_visit_fn visit,
                                      void *context, unsigned long *index_seq,
                                      lc_error *error) {
  lc_pouch_query_index_read_result sidecar;
  lc_pouch_query_index_prepared_presence *prepared;
  lc_pouch_index_result_docid_list docids;
  char *sidecar_path;
  char *field_hex;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      field == NULL || field[0] == '\0' || visit == NULL || index_seq == NULL) {
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
  memset(&sidecar, 0, sizeof(sidecar));
  memset(&docids, 0, sizeof(docids));
  prepared = NULL;
  rc = lc_pouch_query_index_read_header(pouch, namespace_name, sidecar_path,
                                        &sidecar, error);
  if (rc == LC_OK && (!sidecar.present || !sidecar.valid)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index sidecar is not readable", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK && !sidecar.presence_index_complete) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index presence postings are incomplete",
                      NULL, NULL, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_prepare_presence_reader(
        pouch, namespace_name, &sidecar, &prepared, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_prepared_presence_collect(
        &pouch->allocator, prepared, field_hex, &docids, error);
  }
  if (rc == LC_OK) {
    *index_seq = sidecar.index_seq;
    rc = lc_pouch_query_index_docid_emit_cached(
        &pouch->allocator, &prepared->page_cache, prepared->last_result_key,
        &docids, &prepared->doc_table, visit, context, error);
  }
  lc_pouch_index_result_docid_list_cleanup(&pouch->allocator, &docids);
  lc_free_with_allocator(&pouch->allocator, field_hex);
  lc_free_with_allocator(&pouch->allocator, sidecar_path);
  return rc;
}

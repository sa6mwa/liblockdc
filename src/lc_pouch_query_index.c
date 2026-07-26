#include "lc_pouch_query_index.h"

#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define LC_POUCH_QUERY_INDEX_FORMAT "pouch-query-index"
#define LC_POUCH_QUERY_INDEX_VERSION 2UL
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

typedef struct lc_pouch_query_index_summary {
  const lc_allocator *allocator;
  lc_pouch_query_index_row *rows;
  size_t count;
  size_t capacity;
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
  int present;
  int valid;
} lc_pouch_query_index_read_result;

typedef struct lc_pouch_query_index_row_reader {
  const lc_allocator *allocator;
  lc_pouch_query_index_row_visit_fn visit;
  void *context;
} lc_pouch_query_index_row_reader;

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

static char *lc_pouch_query_index_hex_encode(const lc_allocator *allocator,
                                             const char *value) {
  static const char hex[] = "0123456789abcdef";
  const unsigned char *src;
  char *encoded;
  char *dst;
  size_t length;

  if (value == NULL || value[0] == '\0') {
    encoded = (char *)lc_alloc_with_allocator(allocator, 2U);
    if (encoded != NULL) {
      encoded[0] = '-';
      encoded[1] = '\0';
    }
    return encoded;
  }
  length = strlen(value);
  if (length > ((size_t)-1 - 1U) / 2U) {
    return NULL;
  }
  encoded = (char *)lc_alloc_with_allocator(allocator, (length * 2U) + 1U);
  if (encoded == NULL) {
    return NULL;
  }
  src = (const unsigned char *)value;
  dst = encoded;
  while (*src != '\0') {
    *dst++ = hex[*src >> 4];
    *dst++ = hex[*src & 0x0fU];
    ++src;
  }
  *dst = '\0';
  return encoded;
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
  lc_free_with_allocator(summary->allocator, summary->rows);
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
  for (;;) {
    rc = lc_pouch_query_index_read_line(fp, &line, &got_line, error);
    if (rc != LC_OK || !got_line) {
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

static int lc_pouch_query_index_read_with_reader(
    const char *path, lc_pouch_query_index_read_result *out,
    lc_pouch_query_index_row_reader *reader, lc_error *error) {
  char format[64];
  char line[256];
  unsigned long version;
  unsigned long parsed_seq;
  unsigned long row_count;
  unsigned long row_hash;
  FILE *fp;
  int matched;
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
  out->present = 1;
  if (matched == 5 &&
      strcmp(format, LC_POUCH_QUERY_INDEX_FORMAT) == 0 &&
      version == LC_POUCH_QUERY_INDEX_VERSION) {
    out->index_seq = parsed_seq;
    out->row_count = row_count;
    out->row_hash = row_hash;
    rc = lc_pouch_query_index_read_rows(fp, row_count, row_hash, reader,
                                        &out->valid, error);
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
  return lc_pouch_query_index_read_with_reader(path, out, NULL, error);
}

static int lc_pouch_query_index_build_text(
    unsigned long index_seq, lc_pouch_query_index_summary *summary,
    lc_pouch_query_index_text *out, unsigned long *row_hash_out,
    lc_error *error) {
  lc_pouch_query_index_text header;
  lc_pouch_query_index_text rows;
  char line[256];
  unsigned long row_hash;
  size_t index;
  int written;
  int rc;

  if (summary == NULL || out == NULL || row_hash_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index build requires summary outputs",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  out->allocator = summary->allocator;
  *row_hash_out = lc_pouch_query_index_hash_init();
  if (summary->count > 1U) {
    qsort(summary->rows, summary->count, sizeof(summary->rows[0]),
          lc_pouch_query_index_row_compare);
  }
  memset(&header, 0, sizeof(header));
  memset(&rows, 0, sizeof(rows));
  header.allocator = summary->allocator;
  rows.allocator = summary->allocator;
  row_hash = lc_pouch_query_index_hash_init();
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
  if (rc == LC_OK) {
    written = snprintf(line, sizeof(line),
                       "format=%s\nversion=%lu\nstate_index_seq=%lu\n"
                       "row_count=%lu\nsummary_hash=%lu\n",
                       LC_POUCH_QUERY_INDEX_FORMAT,
                       LC_POUCH_QUERY_INDEX_VERSION, index_seq,
                       (unsigned long)summary->count, row_hash);
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
  if (rc == LC_OK) {
    *out = header;
    memset(&header, 0, sizeof(header));
    *row_hash_out = row_hash;
  }
  lc_free_with_allocator(summary->allocator, header.bytes);
  lc_free_with_allocator(summary->allocator, rows.bytes);
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
  text.allocator = &pouch->allocator;
  sidecar_path = lc_pouch_query_index_path(pouch, namespace_name, error);
  if (sidecar_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_state_visit(pouch, namespace_name,
                            lc_pouch_query_index_summary_visit, &summary,
                            error);
  expected_hash = lc_pouch_query_index_hash_init();
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_build_text(state_index_seq, &summary, &text,
                                         &expected_hash, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_read(sidecar_path, &sidecar, error);
  }
  if (rc == LC_OK &&
      (!sidecar.present || !sidecar.valid ||
       sidecar.index_seq != state_index_seq ||
       sidecar.row_count != (unsigned long)summary.count ||
       sidecar.row_hash != expected_hash)) {
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
  rc = lc_pouch_query_index_read(sidecar_path, &sidecar, error);
  if (rc == LC_OK && (!sidecar.present || !sidecar.valid)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index sidecar is not readable", NULL, NULL,
                      "pouch-redesign");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_read_with_reader(sidecar_path, &sidecar, &reader,
                                               error);
  }
  if (rc == LC_OK) {
    *index_seq = sidecar.index_seq;
  }
  lc_free_with_allocator(&pouch->allocator, sidecar_path);
  return rc;
}

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

static int lc_pouch_query_index_read_rows(FILE *fp, unsigned long row_count,
                                          unsigned long row_hash,
                                          int *valid, lc_error *error) {
  static const char prefix[] = "row ";
  unsigned long computed_hash;
  unsigned long actual_rows;
  size_t line_pos;
  int in_line;
  int ch;

  computed_hash = lc_pouch_query_index_hash_init();
  actual_rows = 0UL;
  line_pos = 0U;
  in_line = 0;
  while ((ch = fgetc(fp)) != EOF) {
    in_line = 1;
    lc_pouch_query_index_hash_byte(&computed_hash, (unsigned char)ch);
    if (line_pos < sizeof(prefix) - 1U &&
        ch != (unsigned char)prefix[line_pos]) {
      *valid = 0;
      return LC_OK;
    }
    ++line_pos;
    if (ch == '\n') {
      if (line_pos <= sizeof(prefix) - 1U) {
        *valid = 0;
        return LC_OK;
      }
      ++actual_rows;
      line_pos = 0U;
      in_line = 0;
    }
  }
  if (ferror(fp)) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch query-index rows",
                        strerror(errno), NULL, NULL);
  }
  if (in_line || actual_rows != row_count || computed_hash != row_hash) {
    *valid = 0;
    return LC_OK;
  }
  *valid = 1;
  return LC_OK;
}

static int lc_pouch_query_index_read(
    const char *path, lc_pouch_query_index_read_result *out, lc_error *error) {
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
    rc = lc_pouch_query_index_read_rows(fp, row_count, row_hash, &out->valid,
                                        error);
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

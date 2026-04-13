#include "lc_mutate_stream.h"

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <pwd.h>
#include <strings.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#if defined(__linux__)
extern time_t timegm(struct tm *tm_value);
#endif

#define LC_MS_SCRATCH 65536U

typedef struct lc_ms_utf8_state {
  int remaining;
  unsigned int codepoint;
  unsigned int min_codepoint;
} lc_ms_utf8_state;

typedef struct lc_ms_lonejson_sink_file {
  FILE *fp;
} lc_ms_lonejson_sink_file;

typedef struct lc_ms_lonejson_scalar_sink {
  FILE *fp;
  size_t prefix_offset;
  int has_tail;
  unsigned char tail;
} lc_ms_lonejson_scalar_sink;

typedef struct lc_ms_string_value_json {
  char *value;
} lc_ms_string_value_json;

typedef struct lc_ms_text_source_json {
  lonejson_source value;
} lc_ms_text_source_json;

typedef struct lc_ms_base64_source_json {
  lonejson_source value;
} lc_ms_base64_source_json;

typedef struct lc_ms_i64_value_json {
  lonejson_int64 value;
} lc_ms_i64_value_json;

typedef struct lc_ms_f64_value_json {
  double value;
} lc_ms_f64_value_json;

static const lonejson_field lc_ms_string_value_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_ms_string_value_json, value, "v")};
static const lonejson_field lc_ms_text_source_fields[] = {
    LONEJSON_FIELD_STRING_SOURCE_REQ(lc_ms_text_source_json, value, "v")};
static const lonejson_field lc_ms_base64_source_fields[] = {
    LONEJSON_FIELD_BASE64_SOURCE_REQ(lc_ms_base64_source_json, value, "v")};
static const lonejson_field lc_ms_i64_value_fields[] = {
    LONEJSON_FIELD_I64_REQ(lc_ms_i64_value_json, value, "v")};
static const lonejson_field lc_ms_f64_value_fields[] = {
    LONEJSON_FIELD_F64_REQ(lc_ms_f64_value_json, value, "v")};
LONEJSON_MAP_DEFINE(lc_ms_string_value_map, lc_ms_string_value_json,
                    lc_ms_string_value_fields);
LONEJSON_MAP_DEFINE(lc_ms_text_source_map, lc_ms_text_source_json,
                    lc_ms_text_source_fields);
LONEJSON_MAP_DEFINE(lc_ms_base64_source_map, lc_ms_base64_source_json,
                    lc_ms_base64_source_fields);
LONEJSON_MAP_DEFINE(lc_ms_i64_value_map, lc_ms_i64_value_json,
                    lc_ms_i64_value_fields);
LONEJSON_MAP_DEFINE(lc_ms_f64_value_map, lc_ms_f64_value_json,
                    lc_ms_f64_value_fields);

static int lc_ms_set_error(lc_error *error, const char *message,
                           const char *detail) {
  return lc_error_set(error, LC_ERR_INVALID, 0L, message, detail, NULL, NULL);
}

static char *lc_ms_strdup_range(const char *start, size_t length) {
  char *copy;

  copy = (char *)malloc(length + 1U);
  if (copy == NULL) {
    return NULL;
  }
  if (length != 0U) {
    memcpy(copy, start, length);
  }
  copy[length] = '\0';
  return copy;
}

static int lc_ms_append_ptr(char ***items, size_t *count, char *value) {
  char **next_items;

  next_items = (char **)realloc(*items, (*count + 1U) * sizeof(char *));
  if (next_items == NULL) {
    return 0;
  }
  *items = next_items;
  (*items)[*count] = value;
  *count += 1U;
  return 1;
}

static void lc_ms_value_cleanup(lc_mutation_value *value) {
  if (value == NULL) {
    return;
  }
  free(value->string_value);
  free(value->file_path);
  memset(value, 0, sizeof(*value));
}

static void lc_ms_mutation_cleanup(lc_mutation *mutation) {
  size_t i;

  if (mutation == NULL) {
    return;
  }
  if (mutation->path_segments != NULL) {
    for (i = 0; i < mutation->path_segment_count; ++i) {
      free(mutation->path_segments[i]);
    }
    free(mutation->path_segments);
  }
  lc_ms_value_cleanup(&mutation->value);
  memset(mutation, 0, sizeof(*mutation));
}

void lc_mutation_plan_close(lc_mutation_plan *plan) {
  size_t i;

  if (plan == NULL) {
    return;
  }
  for (i = 0; i < plan->count; ++i) {
    lc_ms_mutation_cleanup(&plan->items[i]);
  }
  free(plan->items);
  free(plan);
}

static int lc_ms_write_bytes(FILE *out, const void *bytes, size_t length,
                             lc_error *error) {
  if (length == 0U) {
    return LC_OK;
  }
  if (out == NULL) {
    return LC_OK;
  }
  if (fwrite(bytes, 1U, length, out) != length) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to write mutate stream output", strerror(errno),
                        NULL, NULL);
  }
  return LC_OK;
}

static int lc_ms_write_char(FILE *out, int ch, lc_error *error) {
  if (out == NULL) {
    return LC_OK;
  }
  if (fputc(ch, out) == EOF) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to write mutate stream output", strerror(errno),
                        NULL, NULL);
  }
  return LC_OK;
}

static lonejson_status lc_ms_lonejson_scalar_sink_write(void *user,
                                                        const void *data,
                                                        size_t len,
                                                        lonejson_error *error) {
  static const unsigned char prefix[] = {'{', '"', 'v', '"', ':'};
  lc_ms_lonejson_scalar_sink *sink;
  const unsigned char *bytes;
  size_t offset;

  sink = (lc_ms_lonejson_scalar_sink *)user;
  bytes = (const unsigned char *)data;
  if (sink == NULL || sink->fp == NULL || (len > 0U && data == NULL)) {
    if (error != NULL) {
      lonejson_error_init(error);
    }
    return LONEJSON_STATUS_INVALID_ARGUMENT;
  }
  offset = 0U;
  while (sink->prefix_offset < sizeof(prefix) && offset < len) {
    if (bytes[offset] != prefix[sink->prefix_offset]) {
      if (error != NULL) {
        lonejson_error_init(error);
        snprintf(error->message, sizeof(error->message),
                 "unexpected lonejson scalar wrapper shape");
      }
      return LONEJSON_STATUS_INVALID_JSON;
    }
    sink->prefix_offset += 1U;
    offset += 1U;
  }
  while (offset < len) {
    if (sink->has_tail) {
      if (fputc((int)sink->tail, sink->fp) == EOF) {
        if (error != NULL) {
          lonejson_error_init(error);
          error->system_errno = errno;
          snprintf(error->message, sizeof(error->message),
                   "failed to write mutate stream output");
        }
        return LONEJSON_STATUS_IO_ERROR;
      }
    }
    sink->tail = bytes[offset++];
    sink->has_tail = 1;
  }
  return LONEJSON_STATUS_OK;
}

static int lc_ms_lonejson_status(lc_error *error, lonejson_status status,
                                 const lonejson_error *lj_error,
                                 const char *message) {
  return lc_lonejson_error_from_status(error, status, lj_error, message);
}

static int lc_ms_serialize_scalar_json(FILE *out, const lonejson_map *map,
                                       const void *src, lc_error *error,
                                       const char *message) {
  lc_ms_lonejson_scalar_sink sink;
  lonejson_error lj_error;
  lonejson_status status;
  lonejson_write_options options;

  memset(&sink, 0, sizeof(sink));
  sink.fp = out;
  memset(&lj_error, 0, sizeof(lj_error));
  options = lonejson_default_write_options();
  status = lonejson_serialize_sink(map, src, lc_ms_lonejson_scalar_sink_write,
                                   &sink, &options, &lj_error);
  if (status == LONEJSON_STATUS_OK) {
    if (sink.prefix_offset != 5U || !sink.has_tail || sink.tail != '}') {
      memset(&lj_error, 0, sizeof(lj_error));
      snprintf(lj_error.message, sizeof(lj_error.message),
               "unexpected lonejson scalar wrapper shape");
      status = LONEJSON_STATUS_INVALID_JSON;
    }
  }
  return lc_ms_lonejson_status(error, status, &lj_error, message);
}

static int lc_ms_utf8_push(lc_ms_utf8_state *state, unsigned char byte,
                           lc_error *error) {
  if (state->remaining == 0) {
    if (byte <= 0x7fU) {
      state->codepoint = byte;
      state->min_codepoint = 0U;
      return LC_OK;
    }
    if (byte >= 0xc2U && byte <= 0xdfU) {
      state->remaining = 1;
      state->codepoint = byte & 0x1fU;
      state->min_codepoint = 0x80U;
      return LC_OK;
    }
    if (byte >= 0xe0U && byte <= 0xefU) {
      state->remaining = 2;
      state->codepoint = byte & 0x0fU;
      state->min_codepoint = 0x800U;
      return LC_OK;
    }
    if (byte >= 0xf0U && byte <= 0xf4U) {
      state->remaining = 3;
      state->codepoint = byte & 0x07U;
      state->min_codepoint = 0x10000U;
      return LC_OK;
    }
    return lc_ms_set_error(error, "invalid UTF-8 in textfile mutation", NULL);
  }

  if ((byte & 0xc0U) != 0x80U) {
    return lc_ms_set_error(error, "invalid UTF-8 in textfile mutation", NULL);
  }
  state->codepoint = (state->codepoint << 6) | (byte & 0x3fU);
  state->remaining -= 1;
  if (state->remaining == 0) {
    if (state->codepoint < state->min_codepoint ||
        state->codepoint > 0x10ffffU ||
        (state->codepoint >= 0xd800U && state->codepoint <= 0xdfffU)) {
      return lc_ms_set_error(error, "invalid UTF-8 in textfile mutation", NULL);
    }
  }
  return LC_OK;
}

static int lc_ms_copy_source_bytes(FILE *out, lc_source *source,
                                   int validate_text, int strict_text,
                                   int *out_text_ok, lc_error *error) {
  unsigned char buffer[LC_MS_SCRATCH];
  size_t nread;
  size_t i;
  lc_ms_utf8_state utf8;
  int text_ok;

  memset(&utf8, 0, sizeof(utf8));
  text_ok = validate_text ? 1 : 0;
  for (;;) {
    nread = source->read(source, buffer, sizeof(buffer), error);
    if (nread == 0U) {
      break;
    }
    if (lc_ms_write_bytes(out, buffer, nread, error) != LC_OK) {
      return LC_ERR_TRANSPORT;
    }
    for (i = 0; i < nread; ++i) {
      if (validate_text && text_ok) {
        if (buffer[i] == '\0') {
          text_ok = 0;
          if (strict_text) {
            return lc_ms_set_error(error, "invalid UTF-8 in textfile mutation",
                                   NULL);
          }
          continue;
        }
        if (lc_ms_utf8_push(&utf8, buffer[i], error) != LC_OK) {
          if (strict_text) {
            return lc_ms_set_error(error, "invalid UTF-8 in textfile mutation",
                                   NULL);
          }
          lc_error_cleanup(error);
          lc_error_init(error);
          text_ok = 0;
        }
      }
    }
  }
  if (validate_text && text_ok && utf8.remaining != 0) {
    if (strict_text) {
      return lc_ms_set_error(error, "invalid UTF-8 in textfile mutation",
                             NULL);
    }
    text_ok = 0;
  }
  if (out_text_ok != NULL) {
    *out_text_ok = text_ok;
  }
  return LC_OK;
}

static int lc_ms_open_file_source(const lc_mutation_value *value,
                                  lc_source **out, lc_error *error) {
  if (value->file_value_resolver != NULL &&
      value->file_value_resolver->open != NULL) {
    return value->file_value_resolver->open(value->file_value_resolver->context,
                                            value->file_path, out, error);
  }
  return lc_source_from_file(value->file_path, out, error);
}

static int lc_ms_emit_json_string_bytes(FILE *out, const char *text,
                                        lc_error *error) {
  lc_ms_string_value_json doc;

  doc.value = (char *)text;
  return lc_ms_serialize_scalar_json(out, &lc_ms_string_value_map, &doc, error,
                                     "failed to serialize mutate string");
}

static int lc_ms_emit_file_mutation_value(FILE *out,
                                          const lc_mutation_value *value,
                                          lc_mutation_file_mode requested_mode,
                                          lc_error *error) {
  lc_source *source;
  FILE *scratch;
  int validate_text;
  int text_ok;
  lc_mutation_file_mode actual_mode;
  lonejson_source lj_source;
  lc_ms_text_source_json text_doc;
  lc_ms_base64_source_json base64_doc;
  int rc;

  source = NULL;
  scratch = NULL;
  actual_mode = requested_mode;
  if (actual_mode == LC_MUTATION_FILE_BASE64 &&
      value->file_value_resolver == NULL) {
    lonejson_source_init(&lj_source);
    base64_doc.value = lj_source;
    rc = lc_ms_lonejson_status(
        error,
        lonejson_source_set_path(&base64_doc.value, value->file_path, NULL),
        NULL, "failed to open base64file mutation");
    if (rc != LC_OK) {
      lonejson_source_cleanup(&base64_doc.value);
      return rc;
    }
    rc = lc_ms_serialize_scalar_json(out, &lc_ms_base64_source_map, &base64_doc,
                                     error,
                                     "failed to serialize base64file mutation");
    lonejson_source_cleanup(&base64_doc.value);
    return rc;
  }

  rc = lc_ms_open_file_source(value, &source, error);
  if (rc != LC_OK) {
    return rc;
  }
  scratch = tmpfile();
  if (scratch == NULL) {
    source->close(source);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create file-backed mutation spool",
                        strerror(errno), NULL, NULL);
  }
  validate_text =
      actual_mode == LC_MUTATION_FILE_TEXT || actual_mode == LC_MUTATION_FILE_AUTO;
  text_ok = 0;
  rc = lc_ms_copy_source_bytes(scratch, source, validate_text,
                               actual_mode == LC_MUTATION_FILE_TEXT, &text_ok,
                               error);
  source->close(source);
  if (rc != LC_OK) {
    fclose(scratch);
    return rc;
  }
  if (actual_mode == LC_MUTATION_FILE_AUTO) {
    actual_mode = text_ok ? LC_MUTATION_FILE_TEXT : LC_MUTATION_FILE_BASE64;
  } else if (actual_mode == LC_MUTATION_FILE_TEXT && !text_ok) {
    fclose(scratch);
    return lc_ms_set_error(error, "invalid UTF-8 in textfile mutation", NULL);
  }
  if (fflush(scratch) != 0 || fseek(scratch, 0L, SEEK_SET) != 0) {
    fclose(scratch);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to rewind file-backed mutation spool",
                        strerror(errno), NULL, NULL);
  }

  lonejson_source_init(&lj_source);
  if (actual_mode == LC_MUTATION_FILE_TEXT) {
    text_doc.value = lj_source;
    rc = lc_ms_lonejson_status(error,
                               lonejson_source_set_file(&text_doc.value,
                                                        scratch, NULL),
                               NULL, "failed to open textfile mutation");
    if (rc == LC_OK) {
      rc = lc_ms_serialize_scalar_json(out, &lc_ms_text_source_map, &text_doc,
                                       error,
                                       "failed to serialize textfile mutation");
    }
    lonejson_source_cleanup(&text_doc.value);
  } else {
    base64_doc.value = lj_source;
    rc = lc_ms_lonejson_status(
        error,
        lonejson_source_set_file(&base64_doc.value, scratch, NULL), NULL,
        "failed to open base64file mutation");
    if (rc == LC_OK) {
      rc = lc_ms_serialize_scalar_json(out, &lc_ms_base64_source_map,
                                       &base64_doc, error,
                                       "failed to serialize base64file mutation");
    }
    lonejson_source_cleanup(&base64_doc.value);
  }
  fclose(scratch);
  return rc;
}

static int lc_ms_plan_append(lc_mutation_plan *plan,
                             const lc_mutation *mutation, lc_error *error) {
  lc_mutation *next_items;

  next_items = (lc_mutation *)realloc(plan->items, (plan->count + 1U) *
                                                       sizeof(*plan->items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate mutation plan", NULL, NULL, NULL);
  }
  plan->items = next_items;
  plan->items[plan->count] = *mutation;
  plan->count += 1U;
  return LC_OK;
}

static int lc_ms_emit_number(FILE *out, double number, lc_error *error) {
  lc_ms_f64_value_json doc;

  if (!isfinite(number)) {
    return lc_ms_set_error(error, "mutation produced non-finite number", NULL);
  }
  doc.value = number;
  return lc_ms_serialize_scalar_json(out, &lc_ms_f64_value_map, &doc, error,
                                     "failed to serialize mutate number");
}

static int lc_ms_emit_mutation_value(FILE *out, const lc_mutation *mutation,
                                     double existing_value, int has_existing,
                                     lc_error *error) {
  lc_mutation_file_mode mode;

  switch (mutation->kind) {
  case LC_MUTATION_REMOVE:
    return lc_ms_write_bytes(out, "null", 4U, error);
  case LC_MUTATION_INCREMENT:
    if (has_existing) {
      return lc_ms_emit_number(out, existing_value + mutation->delta, error);
    }
    return lc_ms_emit_number(out, mutation->delta, error);
  case LC_MUTATION_SET:
    switch (mutation->value.kind) {
    case LC_MUTATION_VALUE_NULL:
      return lc_ms_write_bytes(out, "null", 4U, error);
    case LC_MUTATION_VALUE_BOOL:
      return lc_ms_write_bytes(out,
                               mutation->value.bool_value ? "true" : "false",
                               mutation->value.bool_value ? 4U : 5U, error);
    case LC_MUTATION_VALUE_LONG: {
      lc_ms_i64_value_json doc;

      doc.value = mutation->value.long_value;
      return lc_ms_serialize_scalar_json(out, &lc_ms_i64_value_map, &doc,
                                         error,
                                         "failed to serialize mutate integer");
    }
    case LC_MUTATION_VALUE_DOUBLE:
      return lc_ms_emit_number(out, mutation->value.double_value, error);
    case LC_MUTATION_VALUE_STRING:
      return lc_ms_emit_json_string_bytes(out, mutation->value.string_value,
                                          error);
    case LC_MUTATION_VALUE_FILE:
      mode = mutation->value.file_mode;
      return lc_ms_emit_file_mutation_value(out, &mutation->value, mode, error);
    default:
      return lc_ms_set_error(error, "unsupported mutation value type", NULL);
    }
  default:
    return lc_ms_set_error(error, "unsupported mutation kind", NULL);
  }
}

static int lc_ms_emit_missing_chain(FILE *out, const lc_mutation *mutation,
                                    size_t path_index, lc_error *error) {
  int rc;

  if (path_index >= mutation->path_segment_count) {
    return lc_ms_emit_mutation_value(out, mutation, 0.0, 0, error);
  }
  if (isdigit((unsigned char)mutation->path_segments[path_index][0])) {
    return lc_ms_set_error(error,
                           "local mutate cannot create missing array paths",
                           mutation->path_segments[path_index]);
  }
  rc = lc_ms_write_char(out, '{', error);
  if (rc == LC_OK) {
    rc = lc_ms_emit_json_string_bytes(out, mutation->path_segments[path_index],
                                      error);
  }
  if (rc == LC_OK) {
    rc = lc_ms_write_char(out, ':', error);
  }
  if (rc == LC_OK) {
    rc = lc_ms_emit_missing_chain(out, mutation, path_index + 1U, error);
  }
  if (rc == LC_OK) {
    rc = lc_ms_write_char(out, '}', error);
  }
  return rc;
}

typedef enum lc_ms_visit_container_kind {
  LC_MS_VISIT_CONTAINER_OBJECT = 1,
  LC_MS_VISIT_CONTAINER_ARRAY = 2
} lc_ms_visit_container_kind;

typedef struct lc_ms_visit_frame {
  lc_ms_visit_container_kind kind;
  size_t path_len;
  size_t array_index;
  int first_kept;
  int saw_target_child;
  char *pending_key;
  char index_text[32];
} lc_ms_visit_frame;

typedef struct lc_ms_visit_context {
  FILE *out;
  const lc_mutation *mutation;
  char **path;
  size_t path_len;
  lc_ms_visit_frame frames[128];
  size_t frame_count;
  size_t skip_depth;
  int capture_increment;
  char *key_buffer;
  size_t key_len;
  size_t key_cap;
  char *number_buffer;
  size_t number_len;
  size_t number_cap;
} lc_ms_visit_context;

static int lc_ms_is_prefix(const lc_mutation *mutation, char **path,
                           size_t path_count) {
  size_t i;

  if (path_count > mutation->path_segment_count) {
    return 0;
  }
  for (i = 0U; i < path_count; ++i) {
    if (strcmp(mutation->path_segments[i], path[i]) != 0) {
      return 0;
    }
  }
  return 1;
}

static int lc_ms_is_exact(const lc_mutation *mutation, char **path,
                          size_t path_count) {
  return mutation->path_segment_count == path_count &&
         lc_ms_is_prefix(mutation, path, path_count);
}

static int lc_ms_has_descendant(const lc_mutation *mutation, char **path,
                                size_t path_count) {
  return mutation->path_segment_count > path_count &&
         lc_ms_is_prefix(mutation, path, path_count);
}

static lonejson_status lc_ms_visit_fail(lonejson_error *error,
                                        lonejson_status status,
                                        const char *message) {
  if (error != NULL) {
    lonejson_error_init(error);
    snprintf(error->message, sizeof(error->message), "%s", message);
  }
  return status;
}

static lonejson_status lc_ms_visit_io_fail(lonejson_error *error,
                                           const char *message) {
  if (error != NULL) {
    lonejson_error_init(error);
    error->system_errno = errno;
    snprintf(error->message, sizeof(error->message), "%s", message);
  }
  return LONEJSON_STATUS_IO_ERROR;
}

static lonejson_status lc_ms_visit_write_bytes(FILE *out, const void *bytes,
                                               size_t len,
                                               lonejson_error *error) {
  if (len == 0U) {
    return LONEJSON_STATUS_OK;
  }
  if (out == NULL || (bytes == NULL && len != 0U)) {
    return lc_ms_visit_fail(error, LONEJSON_STATUS_INVALID_ARGUMENT,
                            "invalid mutate output stream");
  }
  if (fwrite(bytes, 1U, len, out) != len) {
    return lc_ms_visit_io_fail(error, "failed to write mutate output");
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_ms_visit_write_char(FILE *out, int ch,
                                              lonejson_error *error) {
  unsigned char byte;

  byte = (unsigned char)ch;
  return lc_ms_visit_write_bytes(out, &byte, 1U, error);
}

static lonejson_status lc_ms_visit_write_escaped_json(FILE *out,
                                                      const unsigned char *data,
                                                      size_t len,
                                                      lonejson_error *error) {
  size_t i;
  char buffer[7];
  lonejson_status status;

  for (i = 0U; i < len; ++i) {
    switch (data[i]) {
    case '"':
      status = lc_ms_visit_write_bytes(out, "\\\"", 2U, error);
      break;
    case '\\':
      status = lc_ms_visit_write_bytes(out, "\\\\", 2U, error);
      break;
    case '\b':
      status = lc_ms_visit_write_bytes(out, "\\b", 2U, error);
      break;
    case '\f':
      status = lc_ms_visit_write_bytes(out, "\\f", 2U, error);
      break;
    case '\n':
      status = lc_ms_visit_write_bytes(out, "\\n", 2U, error);
      break;
    case '\r':
      status = lc_ms_visit_write_bytes(out, "\\r", 2U, error);
      break;
    case '\t':
      status = lc_ms_visit_write_bytes(out, "\\t", 2U, error);
      break;
    default:
      if (data[i] < 0x20U) {
        snprintf(buffer, sizeof(buffer), "\\u%04x", (unsigned)data[i]);
        status = lc_ms_visit_write_bytes(out, buffer, 6U, error);
      } else {
        status = lc_ms_visit_write_bytes(out, &data[i], 1U, error);
      }
      break;
    }
    if (status != LONEJSON_STATUS_OK) {
      return status;
    }
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_ms_visit_buffer_append(char **buffer, size_t *length,
                                                 size_t *capacity,
                                                 const char *data, size_t len,
                                                 lonejson_error *error) {
  char *next;
  size_t next_capacity;

  if (len == 0U) {
    return LONEJSON_STATUS_OK;
  }
  if (*buffer == NULL && *capacity != 0U) {
    *capacity = 0U;
  }
  if (*length + len + 1U > *capacity) {
    next_capacity = *capacity == 0U ? 32U : *capacity * 2U;
    while (next_capacity < *length + len + 1U) {
      next_capacity *= 2U;
    }
    next = (char *)realloc(*buffer, next_capacity);
    if (next == NULL) {
      return lc_ms_visit_fail(error, LONEJSON_STATUS_OVERFLOW,
                              "failed to allocate local mutate buffer");
    }
    *buffer = next;
    *capacity = next_capacity;
  }
  memcpy(*buffer + *length, data, len);
  *length += len;
  (*buffer)[*length] = '\0';
  return LONEJSON_STATUS_OK;
}

static void lc_ms_visit_context_cleanup(lc_ms_visit_context *ctx) {
  size_t i;

  if (ctx == NULL) {
    return;
  }
  for (i = 0U; i < ctx->frame_count; ++i) {
    free(ctx->frames[i].pending_key);
    ctx->frames[i].pending_key = NULL;
  }
  free(ctx->key_buffer);
  ctx->key_buffer = NULL;
  free(ctx->number_buffer);
  ctx->number_buffer = NULL;
  free(ctx->path);
  ctx->path = NULL;
}

static lc_ms_visit_frame *lc_ms_visit_top(lc_ms_visit_context *ctx) {
  if (ctx->frame_count == 0U) {
    return NULL;
  }
  return &ctx->frames[ctx->frame_count - 1U];
}

static lonejson_status lc_ms_visit_push_frame(lc_ms_visit_context *ctx,
                                              lc_ms_visit_container_kind kind,
                                              lonejson_error *error) {
  lc_ms_visit_frame *frame;

  if (ctx->frame_count >= sizeof(ctx->frames) / sizeof(ctx->frames[0])) {
    return lc_ms_visit_fail(error, LONEJSON_STATUS_OVERFLOW,
                            "local mutate nesting is too deep");
  }
  frame = &ctx->frames[ctx->frame_count++];
  memset(frame, 0, sizeof(*frame));
  frame->kind = kind;
  frame->path_len = ctx->path_len;
  frame->first_kept = 1;
  return LONEJSON_STATUS_OK;
}

static void lc_ms_visit_pop_frame(lc_ms_visit_context *ctx) {
  if (ctx->frame_count != 0U) {
    ctx->frame_count -= 1U;
  }
}

static void lc_ms_visit_finalize_parent_child(lc_ms_visit_context *ctx) {
  lc_ms_visit_frame *frame;

  frame = lc_ms_visit_top(ctx);
  if (frame == NULL) {
    ctx->path_len = 0U;
    return;
  }
  if (frame->kind == LC_MS_VISIT_CONTAINER_OBJECT) {
    free(frame->pending_key);
    frame->pending_key = NULL;
    ctx->key_buffer = NULL;
    ctx->key_len = 0U;
  } else if (frame->kind == LC_MS_VISIT_CONTAINER_ARRAY) {
    frame->array_index += 1U;
  }
  ctx->path_len = frame->path_len;
}

static lonejson_status lc_ms_visit_prepare_child(lc_ms_visit_context *ctx,
                                                 lc_ms_visit_frame *frame,
                                                 lc_ms_visit_container_kind kind,
                                                 lonejson_error *error) {
  if (frame->path_len >= 127U) {
    return lc_ms_visit_fail(error, LONEJSON_STATUS_OVERFLOW,
                            "local mutate nesting is too deep");
  }
  if (kind == LC_MS_VISIT_CONTAINER_OBJECT) {
    if (frame->pending_key == NULL) {
      return lc_ms_visit_fail(error, LONEJSON_STATUS_INVALID_JSON,
                              "unexpected object visitor state");
    }
    ctx->path[frame->path_len] = frame->pending_key;
    ctx->path_len = frame->path_len + 1U;
  } else {
    snprintf(frame->index_text, sizeof(frame->index_text), "%lu",
             (unsigned long)frame->array_index);
    ctx->path[frame->path_len] = frame->index_text;
    ctx->path_len = frame->path_len + 1U;
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_ms_visit_emit_prefix(lc_ms_visit_context *ctx,
                                               lc_ms_visit_frame *frame,
                                               lonejson_error *error) {
  lonejson_status status;

  if (frame->kind == LC_MS_VISIT_CONTAINER_OBJECT) {
    if (frame->pending_key == NULL) {
      return lc_ms_visit_fail(error, LONEJSON_STATUS_INVALID_JSON,
                              "unexpected object visitor state");
    }
    if (!frame->first_kept) {
      status = lc_ms_visit_write_char(ctx->out, ',', error);
      if (status != LONEJSON_STATUS_OK) {
        return status;
      }
    } else {
      frame->first_kept = 0;
    }
    status = lc_ms_visit_write_char(ctx->out, '"', error);
    if (status != LONEJSON_STATUS_OK) {
      return status;
    }
    status = lc_ms_visit_write_escaped_json(
        ctx->out, (const unsigned char *)frame->pending_key,
        strlen(frame->pending_key), error);
    if (status != LONEJSON_STATUS_OK) {
      return status;
    }
    status = lc_ms_visit_write_char(ctx->out, '"', error);
    if (status != LONEJSON_STATUS_OK) {
      return status;
    }
    return lc_ms_visit_write_char(ctx->out, ':', error);
  }
  if (!frame->first_kept) {
    status = lc_ms_visit_write_char(ctx->out, ',', error);
    if (status != LONEJSON_STATUS_OK) {
      return status;
    }
  } else {
    frame->first_kept = 0;
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_ms_visit_emit_mutation_value_status(
    FILE *out, const lc_mutation *mutation, double existing_value,
    int has_existing, lonejson_error *error) {
  lc_error lc_error_value;
  int rc;

  lc_error_init(&lc_error_value);
  rc = lc_ms_emit_mutation_value(out, mutation, existing_value, has_existing,
                                 &lc_error_value);
  if (rc != LC_OK) {
    if (error != NULL) {
      lonejson_error_init(error);
      if (lc_error_value.message != NULL) {
        snprintf(error->message, sizeof(error->message), "%s",
                 lc_error_value.message);
      } else {
        snprintf(error->message, sizeof(error->message),
                 "failed to serialize mutation value");
      }
      if (lc_error_value.code == LC_ERR_TRANSPORT) {
        error->system_errno = errno;
      }
    }
    lc_error_cleanup(&lc_error_value);
    return lc_error_value.code == LC_ERR_TRANSPORT ? LONEJSON_STATUS_IO_ERROR
                                                   : LONEJSON_STATUS_INVALID_JSON;
  }
  lc_error_cleanup(&lc_error_value);
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_ms_visit_emit_missing_chain_status(
    FILE *out, const lc_mutation *mutation, size_t path_index,
    lonejson_error *error) {
  lc_error lc_error_value;
  int rc;

  lc_error_init(&lc_error_value);
  rc = lc_ms_emit_missing_chain(out, mutation, path_index, &lc_error_value);
  if (rc != LC_OK) {
    if (error != NULL) {
      lonejson_error_init(error);
      if (lc_error_value.message != NULL) {
        snprintf(error->message, sizeof(error->message), "%s",
                 lc_error_value.message);
      } else {
        snprintf(error->message, sizeof(error->message),
                 "failed to synthesize missing mutation chain");
      }
    }
    lc_error_cleanup(&lc_error_value);
    return LONEJSON_STATUS_INVALID_JSON;
  }
  lc_error_cleanup(&lc_error_value);
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_ms_visit_object_key_begin(void *user,
                                                   lonejson_error *error) {
  lc_ms_visit_context *ctx;
  lc_ms_visit_frame *frame;

  ctx = (lc_ms_visit_context *)user;
  if (ctx->skip_depth != 0U) {
    return LONEJSON_STATUS_OK;
  }
  frame = lc_ms_visit_top(ctx);
  if (frame == NULL || frame->kind != LC_MS_VISIT_CONTAINER_OBJECT) {
    return lc_ms_visit_fail(error, LONEJSON_STATUS_INVALID_JSON,
                            "unexpected object key");
  }
  free(frame->pending_key);
  frame->pending_key = NULL;
  free(ctx->key_buffer);
  ctx->key_buffer = NULL;
  ctx->key_len = 0U;
  ctx->key_cap = 0U;
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_ms_visit_object_key_chunk(void *user,
                                                    const char *data,
                                                    size_t len,
                                                    lonejson_error *error) {
  lc_ms_visit_context *ctx;
  lc_ms_visit_frame *frame;
  lonejson_status status;

  ctx = (lc_ms_visit_context *)user;
  if (ctx->skip_depth != 0U) {
    return LONEJSON_STATUS_OK;
  }
  frame = lc_ms_visit_top(ctx);
  if (frame == NULL || frame->kind != LC_MS_VISIT_CONTAINER_OBJECT) {
    return lc_ms_visit_fail(error, LONEJSON_STATUS_INVALID_JSON,
                            "unexpected object key chunk");
  }
  if (memchr(data, '\0', len) != NULL) {
    return lc_ms_visit_fail(error, LONEJSON_STATUS_INVALID_JSON,
                            "local mutate object keys cannot contain NUL");
  }
  status = lc_ms_visit_buffer_append(&ctx->key_buffer, &ctx->key_len,
                                     &ctx->key_cap, data, len, error);
  if (status != LONEJSON_STATUS_OK) {
    return status;
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_ms_visit_object_key_end(void *user,
                                                  lonejson_error *error) {
  lc_ms_visit_context *ctx;
  lc_ms_visit_frame *frame;

  (void)error;
  ctx = (lc_ms_visit_context *)user;
  if (ctx->skip_depth != 0U) {
    return LONEJSON_STATUS_OK;
  }
  frame = lc_ms_visit_top(ctx);
  if (frame == NULL || frame->kind != LC_MS_VISIT_CONTAINER_OBJECT ||
      ctx->key_buffer == NULL) {
    return LONEJSON_STATUS_INVALID_JSON;
  }
  frame->pending_key = ctx->key_buffer;
  ctx->key_buffer = NULL;
  ctx->key_len = 0U;
  ctx->key_cap = 0U;
  if (ctx->mutation->path_segment_count > frame->path_len &&
      strcmp(ctx->mutation->path_segments[frame->path_len], frame->pending_key) ==
          0) {
    frame->saw_target_child = 1;
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_ms_visit_object_begin(void *user,
                                                lonejson_error *error) {
  lc_ms_visit_context *ctx;
  lc_ms_visit_frame *parent;
  lonejson_status status;
  int exact;

  ctx = (lc_ms_visit_context *)user;
  if (ctx->skip_depth != 0U) {
    ctx->skip_depth += 1U;
    return LONEJSON_STATUS_OK;
  }
  if (ctx->frame_count == 0U) {
    status = lc_ms_visit_write_char(ctx->out, '{', error);
    if (status != LONEJSON_STATUS_OK) {
      return status;
    }
    return lc_ms_visit_push_frame(ctx, LC_MS_VISIT_CONTAINER_OBJECT, error);
  }

  parent = lc_ms_visit_top(ctx);
  status = lc_ms_visit_prepare_child(ctx, parent, LC_MS_VISIT_CONTAINER_OBJECT,
                                     error);
  if (status != LONEJSON_STATUS_OK) {
    return status;
  }
  exact = lc_ms_is_exact(ctx->mutation, ctx->path, ctx->path_len);
  if (exact) {
    switch (ctx->mutation->kind) {
    case LC_MUTATION_REMOVE:
      ctx->skip_depth = 1U;
      return LONEJSON_STATUS_OK;
    case LC_MUTATION_SET:
      status = lc_ms_visit_emit_prefix(ctx, parent, error);
      if (status != LONEJSON_STATUS_OK) {
        return status;
      }
      status = lc_ms_visit_emit_mutation_value_status(ctx->out, ctx->mutation,
                                                      0.0, 0, error);
      if (status != LONEJSON_STATUS_OK) {
        return status;
      }
      ctx->skip_depth = 1U;
      return LONEJSON_STATUS_OK;
    case LC_MUTATION_INCREMENT:
      return lc_ms_visit_fail(error, LONEJSON_STATUS_INVALID_JSON,
                              "increment target is not numeric");
    default:
      return lc_ms_visit_fail(error, LONEJSON_STATUS_INVALID_JSON,
                              "unsupported mutation kind");
    }
  }

  status = lc_ms_visit_emit_prefix(ctx, parent, error);
  if (status != LONEJSON_STATUS_OK) {
    return status;
  }
  status = lc_ms_visit_write_char(ctx->out, '{', error);
  if (status != LONEJSON_STATUS_OK) {
    return status;
  }
  return lc_ms_visit_push_frame(ctx, LC_MS_VISIT_CONTAINER_OBJECT, error);
}

static lonejson_status lc_ms_visit_object_end(void *user,
                                              lonejson_error *error) {
  lc_ms_visit_context *ctx;
  lc_ms_visit_frame *frame;
  lonejson_status status;

  ctx = (lc_ms_visit_context *)user;
  if (ctx->skip_depth != 0U) {
    ctx->skip_depth -= 1U;
    if (ctx->skip_depth == 0U) {
      lc_ms_visit_finalize_parent_child(ctx);
    }
    return LONEJSON_STATUS_OK;
  }
  frame = lc_ms_visit_top(ctx);
  if (frame == NULL || frame->kind != LC_MS_VISIT_CONTAINER_OBJECT) {
    return lc_ms_visit_fail(error, LONEJSON_STATUS_INVALID_JSON,
                            "unexpected object end");
  }
  if (!frame->saw_target_child &&
      lc_ms_has_descendant(ctx->mutation, ctx->path, frame->path_len) &&
      ctx->mutation->kind != LC_MUTATION_REMOVE) {
    if (!frame->first_kept) {
      status = lc_ms_visit_write_char(ctx->out, ',', error);
      if (status != LONEJSON_STATUS_OK) {
        return status;
      }
    } else {
      frame->first_kept = 0;
    }
    status = lc_ms_visit_write_char(ctx->out, '"', error);
    if (status != LONEJSON_STATUS_OK) {
      return status;
    }
    status = lc_ms_visit_write_escaped_json(
        ctx->out,
        (const unsigned char *)ctx->mutation->path_segments[frame->path_len],
        strlen(ctx->mutation->path_segments[frame->path_len]), error);
    if (status != LONEJSON_STATUS_OK) {
      return status;
    }
    status = lc_ms_visit_write_char(ctx->out, '"', error);
    if (status != LONEJSON_STATUS_OK) {
      return status;
    }
    status = lc_ms_visit_write_char(ctx->out, ':', error);
    if (status != LONEJSON_STATUS_OK) {
      return status;
    }
    status = lc_ms_visit_emit_missing_chain_status(ctx->out, ctx->mutation,
                                                   frame->path_len + 1U, error);
    if (status != LONEJSON_STATUS_OK) {
      return status;
    }
  }
  status = lc_ms_visit_write_char(ctx->out, '}', error);
  if (status != LONEJSON_STATUS_OK) {
    return status;
  }
  lc_ms_visit_pop_frame(ctx);
  lc_ms_visit_finalize_parent_child(ctx);
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_ms_visit_array_begin(void *user,
                                               lonejson_error *error) {
  lc_ms_visit_context *ctx;
  lc_ms_visit_frame *parent;
  lonejson_status status;
  int exact;

  ctx = (lc_ms_visit_context *)user;
  if (ctx->skip_depth != 0U) {
    ctx->skip_depth += 1U;
    return LONEJSON_STATUS_OK;
  }
  if (ctx->frame_count == 0U) {
    return lc_ms_visit_fail(error, LONEJSON_STATUS_INVALID_JSON,
                            "local mutate currently requires a JSON object root");
  }
  parent = lc_ms_visit_top(ctx);
  status = lc_ms_visit_prepare_child(ctx, parent, LC_MS_VISIT_CONTAINER_ARRAY,
                                     error);
  if (status != LONEJSON_STATUS_OK) {
    return status;
  }
  exact = lc_ms_is_exact(ctx->mutation, ctx->path, ctx->path_len);
  if (exact) {
    switch (ctx->mutation->kind) {
    case LC_MUTATION_REMOVE:
      ctx->skip_depth = 1U;
      return LONEJSON_STATUS_OK;
    case LC_MUTATION_SET:
      status = lc_ms_visit_emit_prefix(ctx, parent, error);
      if (status != LONEJSON_STATUS_OK) {
        return status;
      }
      status = lc_ms_visit_emit_mutation_value_status(ctx->out, ctx->mutation,
                                                      0.0, 0, error);
      if (status != LONEJSON_STATUS_OK) {
        return status;
      }
      ctx->skip_depth = 1U;
      return LONEJSON_STATUS_OK;
    case LC_MUTATION_INCREMENT:
      return lc_ms_visit_fail(error, LONEJSON_STATUS_INVALID_JSON,
                              "increment target is not numeric");
    default:
      return lc_ms_visit_fail(error, LONEJSON_STATUS_INVALID_JSON,
                              "unsupported mutation kind");
    }
  }
  status = lc_ms_visit_emit_prefix(ctx, parent, error);
  if (status != LONEJSON_STATUS_OK) {
    return status;
  }
  status = lc_ms_visit_write_char(ctx->out, '[', error);
  if (status != LONEJSON_STATUS_OK) {
    return status;
  }
  return lc_ms_visit_push_frame(ctx, LC_MS_VISIT_CONTAINER_ARRAY, error);
}

static lonejson_status lc_ms_visit_array_end(void *user,
                                             lonejson_error *error) {
  lc_ms_visit_context *ctx;
  lc_ms_visit_frame *frame;
  lonejson_status status;

  ctx = (lc_ms_visit_context *)user;
  if (ctx->skip_depth != 0U) {
    ctx->skip_depth -= 1U;
    if (ctx->skip_depth == 0U) {
      lc_ms_visit_finalize_parent_child(ctx);
    }
    return LONEJSON_STATUS_OK;
  }
  frame = lc_ms_visit_top(ctx);
  if (frame == NULL || frame->kind != LC_MS_VISIT_CONTAINER_ARRAY) {
    return lc_ms_visit_fail(error, LONEJSON_STATUS_INVALID_JSON,
                            "unexpected array end");
  }
  status = lc_ms_visit_write_char(ctx->out, ']', error);
  if (status != LONEJSON_STATUS_OK) {
    return status;
  }
  lc_ms_visit_pop_frame(ctx);
  lc_ms_visit_finalize_parent_child(ctx);
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_ms_visit_string_begin(void *user,
                                                lonejson_error *error) {
  lc_ms_visit_context *ctx;
  lc_ms_visit_frame *parent;
  lonejson_status status;
  int exact;

  ctx = (lc_ms_visit_context *)user;
  if (ctx->skip_depth != 0U) {
    ctx->skip_depth += 1U;
    return LONEJSON_STATUS_OK;
  }
  if (ctx->frame_count == 0U) {
    return lc_ms_visit_fail(error, LONEJSON_STATUS_INVALID_JSON,
                            "local mutate currently requires a JSON object root");
  }
  parent = lc_ms_visit_top(ctx);
  status = lc_ms_visit_prepare_child(ctx, parent, parent->kind, error);
  if (status != LONEJSON_STATUS_OK) {
    return status;
  }
  exact = lc_ms_is_exact(ctx->mutation, ctx->path, ctx->path_len);
  if (exact) {
    switch (ctx->mutation->kind) {
    case LC_MUTATION_REMOVE:
      ctx->skip_depth = 1U;
      return LONEJSON_STATUS_OK;
    case LC_MUTATION_SET:
      status = lc_ms_visit_emit_prefix(ctx, parent, error);
      if (status != LONEJSON_STATUS_OK) {
        return status;
      }
      status = lc_ms_visit_emit_mutation_value_status(ctx->out, ctx->mutation,
                                                      0.0, 0, error);
      if (status != LONEJSON_STATUS_OK) {
        return status;
      }
      ctx->skip_depth = 1U;
      return LONEJSON_STATUS_OK;
    case LC_MUTATION_INCREMENT:
      return lc_ms_visit_fail(error, LONEJSON_STATUS_INVALID_JSON,
                              "increment target is not numeric");
    default:
      return lc_ms_visit_fail(error, LONEJSON_STATUS_INVALID_JSON,
                              "unsupported mutation kind");
    }
  }
  status = lc_ms_visit_emit_prefix(ctx, parent, error);
  if (status != LONEJSON_STATUS_OK) {
    return status;
  }
  return lc_ms_visit_write_char(ctx->out, '"', error);
}

static lonejson_status lc_ms_visit_string_chunk(void *user, const char *data,
                                                size_t len,
                                                lonejson_error *error) {
  lc_ms_visit_context *ctx;

  ctx = (lc_ms_visit_context *)user;
  if (ctx->skip_depth != 0U) {
    return LONEJSON_STATUS_OK;
  }
  return lc_ms_visit_write_escaped_json(ctx->out,
                                        (const unsigned char *)data, len,
                                        error);
}

static lonejson_status lc_ms_visit_string_end(void *user,
                                              lonejson_error *error) {
  lc_ms_visit_context *ctx;
  lonejson_status status;

  ctx = (lc_ms_visit_context *)user;
  if (ctx->skip_depth != 0U) {
    ctx->skip_depth -= 1U;
    if (ctx->skip_depth == 0U) {
      lc_ms_visit_finalize_parent_child(ctx);
    }
    return LONEJSON_STATUS_OK;
  }
  status = lc_ms_visit_write_char(ctx->out, '"', error);
  if (status != LONEJSON_STATUS_OK) {
    return status;
  }
  lc_ms_visit_finalize_parent_child(ctx);
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_ms_visit_number_begin(void *user,
                                                lonejson_error *error) {
  lc_ms_visit_context *ctx;
  lc_ms_visit_frame *parent;
  lonejson_status status;
  int exact;

  ctx = (lc_ms_visit_context *)user;
  if (ctx->skip_depth != 0U) {
    ctx->skip_depth += 1U;
    return LONEJSON_STATUS_OK;
  }
  if (ctx->frame_count == 0U) {
    return lc_ms_visit_fail(error, LONEJSON_STATUS_INVALID_JSON,
                            "local mutate currently requires a JSON object root");
  }
  parent = lc_ms_visit_top(ctx);
  status = lc_ms_visit_prepare_child(ctx, parent, parent->kind, error);
  if (status != LONEJSON_STATUS_OK) {
    return status;
  }
  exact = lc_ms_is_exact(ctx->mutation, ctx->path, ctx->path_len);
  if (exact) {
    switch (ctx->mutation->kind) {
    case LC_MUTATION_REMOVE:
      ctx->skip_depth = 1U;
      return LONEJSON_STATUS_OK;
    case LC_MUTATION_SET:
      status = lc_ms_visit_emit_prefix(ctx, parent, error);
      if (status != LONEJSON_STATUS_OK) {
        return status;
      }
      status = lc_ms_visit_emit_mutation_value_status(ctx->out, ctx->mutation,
                                                      0.0, 0, error);
      if (status != LONEJSON_STATUS_OK) {
        return status;
      }
      ctx->skip_depth = 1U;
      return LONEJSON_STATUS_OK;
    case LC_MUTATION_INCREMENT:
      status = lc_ms_visit_emit_prefix(ctx, parent, error);
      if (status != LONEJSON_STATUS_OK) {
        return status;
      }
      free(ctx->number_buffer);
      ctx->number_buffer = NULL;
      ctx->number_len = 0U;
      ctx->number_cap = 0U;
      ctx->capture_increment = 1;
      return LONEJSON_STATUS_OK;
    default:
      return lc_ms_visit_fail(error, LONEJSON_STATUS_INVALID_JSON,
                              "unsupported mutation kind");
    }
  }
  status = lc_ms_visit_emit_prefix(ctx, parent, error);
  if (status != LONEJSON_STATUS_OK) {
    return status;
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_ms_visit_number_chunk(void *user, const char *data,
                                                size_t len,
                                                lonejson_error *error) {
  lc_ms_visit_context *ctx;

  ctx = (lc_ms_visit_context *)user;
  if (ctx->skip_depth != 0U) {
    return LONEJSON_STATUS_OK;
  }
  if (ctx->capture_increment) {
    return lc_ms_visit_buffer_append(&ctx->number_buffer, &ctx->number_len,
                                     &ctx->number_cap, data, len, error);
  }
  return lc_ms_visit_write_bytes(ctx->out, data, len, error);
}

static lonejson_status lc_ms_visit_number_end(void *user, lonejson_error *error) {
  lc_ms_visit_context *ctx;
  double existing;
  char *endptr;
  lonejson_status status;

  ctx = (lc_ms_visit_context *)user;
  if (ctx->skip_depth != 0U) {
    ctx->skip_depth -= 1U;
    if (ctx->skip_depth == 0U) {
      lc_ms_visit_finalize_parent_child(ctx);
    }
    return LONEJSON_STATUS_OK;
  }
  if (ctx->capture_increment) {
    ctx->capture_increment = 0;
    errno = 0;
    existing = strtod(ctx->number_buffer != NULL ? ctx->number_buffer : "",
                      &endptr);
    if (errno != 0 || endptr == ctx->number_buffer || *endptr != '\0') {
      return lc_ms_visit_fail(error, LONEJSON_STATUS_INVALID_JSON,
                              "increment target is not numeric");
    }
    status = lc_ms_visit_emit_mutation_value_status(ctx->out, ctx->mutation,
                                                    existing, 1, error);
    if (status != LONEJSON_STATUS_OK) {
      return status;
    }
    lc_ms_visit_finalize_parent_child(ctx);
    return LONEJSON_STATUS_OK;
  }
  lc_ms_visit_finalize_parent_child(ctx);
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_ms_visit_boolean_value(void *user, int value,
                                                 lonejson_error *error) {
  lc_ms_visit_context *ctx;
  lc_ms_visit_frame *parent;
  lonejson_status status;

  ctx = (lc_ms_visit_context *)user;
  if (ctx->skip_depth != 0U) {
    return LONEJSON_STATUS_OK;
  }
  if (ctx->frame_count == 0U) {
    return lc_ms_visit_fail(error, LONEJSON_STATUS_INVALID_JSON,
                            "local mutate currently requires a JSON object root");
  }
  parent = lc_ms_visit_top(ctx);
  status = lc_ms_visit_prepare_child(ctx, parent, parent->kind, error);
  if (status != LONEJSON_STATUS_OK) {
    return status;
  }
  if (lc_ms_is_exact(ctx->mutation, ctx->path, ctx->path_len)) {
    if (ctx->mutation->kind == LC_MUTATION_REMOVE) {
      lc_ms_visit_finalize_parent_child(ctx);
      return LONEJSON_STATUS_OK;
    }
    if (ctx->mutation->kind != LC_MUTATION_SET) {
      return lc_ms_visit_fail(error, LONEJSON_STATUS_INVALID_JSON,
                              "increment target is not numeric");
    }
    status = lc_ms_visit_emit_prefix(ctx, parent, error);
    if (status != LONEJSON_STATUS_OK) {
      return status;
    }
    status = lc_ms_visit_emit_mutation_value_status(ctx->out, ctx->mutation,
                                                    0.0, 0, error);
    if (status != LONEJSON_STATUS_OK) {
      return status;
    }
    lc_ms_visit_finalize_parent_child(ctx);
    return LONEJSON_STATUS_OK;
  }
  status = lc_ms_visit_emit_prefix(ctx, parent, error);
  if (status != LONEJSON_STATUS_OK) {
    return status;
  }
  status = lc_ms_visit_write_bytes(ctx->out, value ? "true" : "false",
                                   value ? 4U : 5U, error);
  if (status != LONEJSON_STATUS_OK) {
    return status;
  }
  lc_ms_visit_finalize_parent_child(ctx);
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_ms_visit_null_value(void *user,
                                              lonejson_error *error) {
  lc_ms_visit_context *ctx;
  lc_ms_visit_frame *parent;
  lonejson_status status;

  ctx = (lc_ms_visit_context *)user;
  if (ctx->skip_depth != 0U) {
    return LONEJSON_STATUS_OK;
  }
  if (ctx->frame_count == 0U) {
    return lc_ms_visit_fail(error, LONEJSON_STATUS_INVALID_JSON,
                            "local mutate currently requires a JSON object root");
  }
  parent = lc_ms_visit_top(ctx);
  status = lc_ms_visit_prepare_child(ctx, parent, parent->kind, error);
  if (status != LONEJSON_STATUS_OK) {
    return status;
  }
  if (lc_ms_is_exact(ctx->mutation, ctx->path, ctx->path_len)) {
    if (ctx->mutation->kind == LC_MUTATION_REMOVE) {
      lc_ms_visit_finalize_parent_child(ctx);
      return LONEJSON_STATUS_OK;
    }
    if (ctx->mutation->kind != LC_MUTATION_SET) {
      return lc_ms_visit_fail(error, LONEJSON_STATUS_INVALID_JSON,
                              "increment target is not numeric");
    }
    status = lc_ms_visit_emit_prefix(ctx, parent, error);
    if (status != LONEJSON_STATUS_OK) {
      return status;
    }
    status = lc_ms_visit_emit_mutation_value_status(ctx->out, ctx->mutation,
                                                    0.0, 0, error);
    if (status != LONEJSON_STATUS_OK) {
      return status;
    }
    lc_ms_visit_finalize_parent_child(ctx);
    return LONEJSON_STATUS_OK;
  }
  status = lc_ms_visit_emit_prefix(ctx, parent, error);
  if (status != LONEJSON_STATUS_OK) {
    return status;
  }
  status = lc_ms_visit_write_bytes(ctx->out, "null", 4U, error);
  if (status != LONEJSON_STATUS_OK) {
    return status;
  }
  lc_ms_visit_finalize_parent_child(ctx);
  return LONEJSON_STATUS_OK;
}

static int lc_ms_visit_apply_single(const lc_mutation *mutation, FILE *input,
                                    FILE *output, lc_error *error) {
  lc_ms_visit_context ctx;
  lonejson_value_visitor visitor;
  lonejson_value_limits limits;
  lonejson_error lj_error;
  lonejson_status status;

  memset(&ctx, 0, sizeof(ctx));
  ctx.out = output;
  ctx.mutation = mutation;
  ctx.path = (char **)calloc(128U, sizeof(char *));
  if (ctx.path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate local mutate path stack", NULL,
                        NULL, NULL);
  }
  visitor = lonejson_default_value_visitor();
  visitor.object_begin = lc_ms_visit_object_begin;
  visitor.object_end = lc_ms_visit_object_end;
  visitor.object_key_begin = lc_ms_visit_object_key_begin;
  visitor.object_key_chunk = lc_ms_visit_object_key_chunk;
  visitor.object_key_end = lc_ms_visit_object_key_end;
  visitor.array_begin = lc_ms_visit_array_begin;
  visitor.array_end = lc_ms_visit_array_end;
  visitor.string_begin = lc_ms_visit_string_begin;
  visitor.string_chunk = lc_ms_visit_string_chunk;
  visitor.string_end = lc_ms_visit_string_end;
  visitor.number_begin = lc_ms_visit_number_begin;
  visitor.number_chunk = lc_ms_visit_number_chunk;
  visitor.number_end = lc_ms_visit_number_end;
  visitor.boolean_value = lc_ms_visit_boolean_value;
  visitor.null_value = lc_ms_visit_null_value;
  limits = lonejson_default_value_limits();
  memset(&lj_error, 0, sizeof(lj_error));
  status = lonejson_visit_value_filep(input, &visitor, &ctx, &limits, &lj_error);
  if (status != LONEJSON_STATUS_OK) {
    lc_ms_visit_context_cleanup(&ctx);
    if (lj_error.message[0] != '\0') {
      return lc_error_set(error, LC_ERR_INVALID, 0L, lj_error.message, NULL,
                          NULL, NULL);
    }
    return lc_lonejson_error_from_status(error, status, &lj_error,
                                          "failed to transform local mutate "
                                          "document");
  }
  if (fflush(output) != 0) {
    lc_ms_visit_context_cleanup(&ctx);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to flush mutate stream output", strerror(errno),
                        NULL, NULL);
  }
  lc_ms_visit_context_cleanup(&ctx);
  return LC_OK;
}

static int lc_ms_split_path(const char *path, char ***out_segments,
                            size_t *out_count, lc_error *error) {
  const char *cursor;
  const char *segment_start;
  char **segments;
  size_t count;
  char *segment;
  size_t length;
  size_t i;
  char decoded[PATH_MAX];
  size_t decoded_length;

  if (path == NULL || path[0] != '/') {
    return lc_ms_set_error(error, "mutation path must start with '/'", path);
  }
  cursor = path + 1;
  segments = NULL;
  count = 0U;
  while (1) {
    segment_start = cursor;
    while (*cursor != '\0' && *cursor != '/') {
      cursor += 1;
    }
    length = (size_t)(cursor - segment_start);
    decoded_length = 0U;
    for (i = 0U; i < length; ++i) {
      if (segment_start[i] == '~' && i + 1U < length) {
        if (segment_start[i + 1U] == '0') {
          decoded[decoded_length++] = '~';
          i += 1U;
          continue;
        }
        if (segment_start[i + 1U] == '1') {
          decoded[decoded_length++] = '/';
          i += 1U;
          continue;
        }
      }
      decoded[decoded_length++] = segment_start[i];
    }
    decoded[decoded_length] = '\0';
    if (decoded_length == 0U) {
      while (count > 0U) {
        free(segments[--count]);
      }
      free(segments);
      return lc_ms_set_error(error, "mutation path refers to document root",
                             path);
    }
    if (strcmp(decoded, "*") == 0 || strcmp(decoded, "[]") == 0 ||
        strcmp(decoded, "**") == 0 || strcmp(decoded, "...") == 0) {
      while (count > 0U) {
        free(segments[--count]);
      }
      free(segments);
      return lc_ms_set_error(error,
                             "local mutate does not support wildcard LQL "
                             "paths; use direct JSON pointer paths",
                             decoded);
    }
    segment = lc_ms_strdup_range(decoded, decoded_length);
    if (segment == NULL || !lc_ms_append_ptr(&segments, &count, segment)) {
      free(segment);
      while (count > 0U) {
        free(segments[--count]);
      }
      free(segments);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate mutation path", NULL, NULL, NULL);
    }
    if (*cursor == '\0') {
      break;
    }
    cursor += 1;
  }
  *out_segments = segments;
  *out_count = count;
  return LC_OK;
}

static int lc_ms_split_expressions(const char *text, char ***out_parts,
                                   size_t *out_count, lc_error *error) {
  const char *cursor;
  const char *part_start;
  int depth;
  int in_single;
  int in_double;
  char **parts;
  size_t count;
  char *part;
  size_t length;
  const char *start;
  const char *end;

  parts = NULL;
  count = 0U;
  cursor = text;
  part_start = text;
  depth = 0;
  in_single = 0;
  in_double = 0;
  while (*cursor != '\0') {
    if (*cursor == '\\') {
      if (cursor[1] != '\0') {
        cursor += 2;
        continue;
      }
    }
    if (!in_double && *cursor == '\'') {
      in_single = !in_single;
    } else if (!in_single && *cursor == '"') {
      in_double = !in_double;
    } else if (!in_single && !in_double) {
      if (*cursor == '{') {
        depth += 1;
      } else if (*cursor == '}') {
        depth -= 1;
      } else if (depth == 0 && (*cursor == ',' || *cursor == '\n')) {
        start = part_start;
        end = cursor;
        while (start < end && isspace((unsigned char)*start)) {
          start += 1;
        }
        while (end > start && isspace((unsigned char)end[-1])) {
          end -= 1;
        }
        length = (size_t)(end - start);
        if (length != 0U) {
          part = lc_ms_strdup_range(start, length);
          if (part == NULL || !lc_ms_append_ptr(&parts, &count, part)) {
            free(part);
            while (count > 0U) {
              free(parts[--count]);
            }
            free(parts);
            return lc_error_set(error, LC_ERR_NOMEM, 0L,
                                "failed to allocate brace mutation", NULL, NULL,
                                NULL);
          }
        }
        part_start = cursor + 1;
      }
    }
    cursor += 1;
  }
  start = part_start;
  end = cursor;
  while (start < end && isspace((unsigned char)*start)) {
    start += 1;
  }
  while (end > start && isspace((unsigned char)end[-1])) {
    end -= 1;
  }
  length = (size_t)(end - start);
  if (length != 0U) {
    part = lc_ms_strdup_range(start, length);
    if (part == NULL || !lc_ms_append_ptr(&parts, &count, part)) {
      free(part);
      while (count > 0U) {
        free(parts[--count]);
      }
      free(parts);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate brace mutation", NULL, NULL,
                          NULL);
    }
  }
  *out_parts = parts;
  *out_count = count;
  return LC_OK;
}

static int lc_ms_resolve_home_path(const char *path, char **out,
                                   lc_error *error) {
  const char *home;
  struct passwd *pwd;
  char *resolved;
  size_t home_len;
  size_t path_len;

  if (strcmp(path, "~") == 0 || strncmp(path, "~/", 2) == 0) {
    home = getenv("HOME");
    if (home == NULL || home[0] == '\0') {
      pwd = getpwuid(getuid());
      if (pwd != NULL) {
        home = pwd->pw_dir;
      }
    }
    if (home == NULL || home[0] == '\0') {
      return lc_ms_set_error(
          error, "failed to resolve home directory for file-backed mutation",
          path);
    }
    home_len = strlen(home);
    path_len = strcmp(path, "~") == 0 ? 0U : strlen(path + 2);
    resolved = (char *)malloc(home_len + 1U + path_len + 1U);
    if (resolved == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate resolved file path", NULL, NULL,
                          NULL);
    }
    memcpy(resolved, home, home_len);
    if (path_len != 0U) {
      resolved[home_len] = '/';
      memcpy(resolved + home_len + 1U, path + 2, path_len);
      resolved[home_len + 1U + path_len] = '\0';
    } else {
      resolved[home_len] = '\0';
    }
    *out = resolved;
    return LC_OK;
  }
  *out = lc_strdup_local(path);
  if (*out == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to allocate file path",
                        NULL, NULL, NULL);
  }
  return LC_OK;
}

static int lc_ms_resolve_file_path(const char *raw_path, const char *base_dir,
                                   char **out, lc_error *error) {
  char *path;
  char *resolved;
  size_t path_len;
  size_t base_len;

  path = lc_strdup_local(raw_path);
  if (path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate file-backed mutation path", NULL,
                        NULL, NULL);
  }
  path_len = strlen(path);
  if (path_len >= 2U && ((path[0] == '"' && path[path_len - 1U] == '"') ||
                         (path[0] == '\'' && path[path_len - 1U] == '\''))) {
    memmove(path, path + 1, path_len - 2U);
    path[path_len - 2U] = '\0';
  }
  if (path[0] == '\0') {
    free(path);
    return lc_ms_set_error(error, "file-backed mutation missing file path",
                           NULL);
  }
  if (lc_ms_resolve_home_path(path, &resolved, error) != LC_OK) {
    free(path);
    return LC_ERR_INVALID;
  }
  free(path);
  if (resolved[0] == '/') {
    *out = resolved;
    return LC_OK;
  }
  if (base_dir == NULL || base_dir[0] == '\0') {
    free(resolved);
    return lc_ms_set_error(
        error,
        "relative file-backed mutation path requires file value base dir",
        raw_path);
  }
  base_len = strlen(base_dir);
  path_len = strlen(resolved);
  path = (char *)malloc(base_len + 1U + path_len + 1U);
  if (path == NULL) {
    free(resolved);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate resolved file path", NULL, NULL,
                        NULL);
  }
  memcpy(path, base_dir, base_len);
  path[base_len] = '/';
  memcpy(path + base_len + 1U, resolved, path_len + 1U);
  free(resolved);
  *out = path;
  return LC_OK;
}

static int lc_ms_build_increment(const char *path_text, double delta,
                                 lc_mutation *mutation, lc_error *error) {
  if (delta == 0.0) {
    return lc_ms_set_error(error, "increment mutation requires non-zero delta",
                           path_text);
  }
  mutation->kind = LC_MUTATION_INCREMENT;
  mutation->delta = delta;
  return lc_ms_split_path(path_text, &mutation->path_segments,
                          &mutation->path_segment_count, error);
}

static int lc_ms_is_leap_year(int year) {
  return ((year % 4) == 0 && (year % 100) != 0) || ((year % 400) == 0);
}

static int lc_ms_days_in_month(int year, int month) {
  static const int days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

  if (month == 2 && lc_ms_is_leap_year(year)) {
    return 29;
  }
  return days[month - 1];
}

static int lc_ms_parse_fixed_digits(const char *text, size_t offset,
                                    size_t digits, int *out) {
  size_t i;
  int value;

  value = 0;
  for (i = 0U; i < digits; ++i) {
    if (!isdigit((unsigned char)text[offset + i])) {
      return 0;
    }
    value = (value * 10) + (text[offset + i] - '0');
  }
  *out = value;
  return 1;
}

static int lc_ms_parse_time_literal(const char *text, struct timespec *out,
                                    lc_error *error) {
  size_t length;
  int year;
  int month;
  int day;
  int hour;
  int minute;
  int second;
  int offset_hour;
  int offset_minute;
  int offset_sign;
  int offset_seconds;
  long nanoseconds;
  size_t pos;
  size_t digits;
  struct tm tm_value;
  time_t epoch;

  if (text == NULL || out == NULL) {
    return lc_ms_set_error(error, "invalid time literal", NULL);
  }
  length = strlen(text);
  if (length < 20U) {
    return lc_ms_set_error(error, "invalid RFC3339 time literal", text);
  }
  if (!lc_ms_parse_fixed_digits(text, 0U, 4U, &year) || text[4] != '-' ||
      !lc_ms_parse_fixed_digits(text, 5U, 2U, &month) || text[7] != '-' ||
      !lc_ms_parse_fixed_digits(text, 8U, 2U, &day) ||
      (text[10] != 'T' && text[10] != 't') ||
      !lc_ms_parse_fixed_digits(text, 11U, 2U, &hour) || text[13] != ':' ||
      !lc_ms_parse_fixed_digits(text, 14U, 2U, &minute) || text[16] != ':' ||
      !lc_ms_parse_fixed_digits(text, 17U, 2U, &second)) {
    return lc_ms_set_error(error, "invalid RFC3339 time literal", text);
  }
  if (month < 1 || month > 12 || day < 1 ||
      day > lc_ms_days_in_month(year, month) || hour < 0 || hour > 23 ||
      minute < 0 || minute > 59 || second < 0 || second > 59) {
    return lc_ms_set_error(error, "invalid RFC3339 time literal", text);
  }

  nanoseconds = 0L;
  pos = 19U;
  if (pos < length && text[pos] == '.') {
    pos += 1U;
    digits = 0U;
    while (pos + digits < length &&
           isdigit((unsigned char)text[pos + digits])) {
      if (digits < 9U) {
        nanoseconds = (nanoseconds * 10L) + (long)(text[pos + digits] - '0');
      }
      digits += 1U;
    }
    if (digits == 0U || digits > 9U) {
      return lc_ms_set_error(error, "invalid RFC3339Nano time literal", text);
    }
    while (digits < 9U) {
      nanoseconds *= 10L;
      digits += 1U;
    }
    pos += digits;
  }

  offset_seconds = 0;
  if (pos == length - 1U && (text[pos] == 'Z' || text[pos] == 'z')) {
    pos += 1U;
  } else {
    if (pos + 6U != length || (text[pos] != '+' && text[pos] != '-') ||
        !lc_ms_parse_fixed_digits(text, pos + 1U, 2U, &offset_hour) ||
        text[pos + 3U] != ':' ||
        !lc_ms_parse_fixed_digits(text, pos + 4U, 2U, &offset_minute)) {
      return lc_ms_set_error(error, "invalid RFC3339 time literal", text);
    }
    if (offset_hour > 23 || offset_minute > 59) {
      return lc_ms_set_error(error, "invalid RFC3339 time literal", text);
    }
    offset_sign = text[pos] == '-' ? -1 : 1;
    offset_seconds = offset_sign * ((offset_hour * 60 + offset_minute) * 60);
    pos += 6U;
  }
  if (pos != length) {
    return lc_ms_set_error(error, "invalid RFC3339 time literal", text);
  }

  memset(&tm_value, 0, sizeof(tm_value));
  tm_value.tm_year = year - 1900;
  tm_value.tm_mon = month - 1;
  tm_value.tm_mday = day;
  tm_value.tm_hour = hour;
  tm_value.tm_min = minute;
  tm_value.tm_sec = second;
  epoch = timegm(&tm_value);
  if (epoch == (time_t)-1 &&
      !(year == 1969 && month == 12 && day == 31 && hour == 23 &&
        minute == 59 && second == 59 && offset_seconds == 0)) {
    return lc_ms_set_error(error, "failed to normalize RFC3339 time literal",
                           text);
  }
  out->tv_sec = epoch - (time_t)offset_seconds;
  out->tv_nsec = nanoseconds;
  return LC_OK;
}

static int lc_ms_format_time_literal(const struct timespec *ts, char **out_text,
                                     lc_error *error) {
  struct tm tm_value;
  char buffer[64];
  char fraction[10];
  size_t fraction_len;
  int length;
  char *text;

  if (ts == NULL || out_text == NULL) {
    return lc_ms_set_error(error, "invalid timestamp value", NULL);
  }
  if (ts->tv_nsec < 0L || ts->tv_nsec >= 1000000000L) {
    return lc_ms_set_error(error, "invalid timestamp value", NULL);
  }
  if (gmtime_r(&ts->tv_sec, &tm_value) == NULL) {
    return lc_ms_set_error(error, "failed to normalize timestamp value", NULL);
  }

  length =
      snprintf(buffer, sizeof(buffer), "%04d-%02d-%02dT%02d:%02d:%02d",
               tm_value.tm_year + 1900, tm_value.tm_mon + 1, tm_value.tm_mday,
               tm_value.tm_hour, tm_value.tm_min, tm_value.tm_sec);
  if (length < 0 || (size_t)length >= sizeof(buffer)) {
    return lc_ms_set_error(error, "failed to format timestamp value", NULL);
  }
  if (ts->tv_nsec != 0L) {
    snprintf(fraction, sizeof(fraction), "%09ld", ts->tv_nsec);
    fraction_len = 9U;
    while (fraction_len != 0U && fraction[fraction_len - 1U] == '0') {
      fraction_len -= 1U;
    }
    buffer[length++] = '.';
    memcpy(buffer + length, fraction, fraction_len);
    length += (int)fraction_len;
  }
  buffer[length++] = 'Z';
  buffer[length] = '\0';
  text = lc_strdup_local(buffer);
  if (text == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate timestamp literal", NULL, NULL,
                        NULL);
  }
  *out_text = text;
  return LC_OK;
}

static int lc_ms_parse_literal_value(const char *literal, int time_mode,
                                     const lc_mutation_parse_options *options,
                                     lc_mutation_value *value,
                                     lc_error *error) {
  char *text;
  char *endptr;
  double dbl;
  lc_i64 lng;
  struct timespec now_value;
  struct timespec parsed_time;
  size_t length;
  int rc;

  text = lc_strdup_local(literal);
  if (text == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate mutation literal", NULL, NULL,
                        NULL);
  }
  while (*text != '\0' && isspace((unsigned char)*text)) {
    memmove(text, text + 1, strlen(text));
  }
  length = strlen(text);
  while (length != 0U && isspace((unsigned char)text[length - 1U])) {
    text[--length] = '\0';
  }
  if (length >= 2U && ((text[0] == '"' && text[length - 1U] == '"') ||
                       (text[0] == '\'' && text[length - 1U] == '\''))) {
    memmove(text, text + 1, length - 2U);
    text[length - 2U] = '\0';
  }
  if (time_mode) {
    if (strcmp(text, "NOW") == 0 || strcmp(text, "now") == 0) {
      memset(&now_value, 0, sizeof(now_value));
      if (options != NULL && options->has_now) {
        now_value = options->now;
      } else if (clock_gettime(CLOCK_REALTIME, &now_value) != 0) {
        now_value.tv_sec = time(NULL);
        now_value.tv_nsec = 0L;
      }
      free(text);
      value->kind = LC_MUTATION_VALUE_STRING;
      return lc_ms_format_time_literal(&now_value, &value->string_value, error);
    }
    rc = lc_ms_parse_time_literal(text, &parsed_time, error);
    free(text);
    if (rc != LC_OK) {
      return rc;
    }
    value->kind = LC_MUTATION_VALUE_STRING;
    return lc_ms_format_time_literal(&parsed_time, &value->string_value, error);
  }
  if (strcasecmp(text, "true") == 0) {
    free(text);
    value->kind = LC_MUTATION_VALUE_BOOL;
    value->bool_value = 1;
    return LC_OK;
  }
  if (strcasecmp(text, "false") == 0) {
    free(text);
    value->kind = LC_MUTATION_VALUE_BOOL;
    value->bool_value = 0;
    return LC_OK;
  }
  if (strcasecmp(text, "null") == 0) {
    free(text);
    value->kind = LC_MUTATION_VALUE_NULL;
    return LC_OK;
  }
  if (lc_i64_parse_base10(text, &lng)) {
    free(text);
    value->kind = LC_MUTATION_VALUE_LONG;
    value->long_value = lng;
    return LC_OK;
  }
  errno = 0;
  dbl = strtod(text, &endptr);
  if (errno == 0 && endptr != text && *endptr == '\0') {
    free(text);
    value->kind = LC_MUTATION_VALUE_DOUBLE;
    value->double_value = dbl;
    return LC_OK;
  }
  value->kind = LC_MUTATION_VALUE_STRING;
  value->string_value = text;
  return LC_OK;
}

static int lc_ms_build_set(const char *path_text, const char *value_text,
                           int time_mode, lc_mutation_file_mode file_mode,
                           const lc_mutation_parse_options *options,
                           lc_mutation *mutation, lc_error *error) {
  int rc;

  mutation->kind = LC_MUTATION_SET;
  rc = lc_ms_split_path(path_text, &mutation->path_segments,
                        &mutation->path_segment_count, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (file_mode != 0) {
    if (options == NULL) {
      return lc_ms_set_error(error, "file-backed mutations are disabled", NULL);
    }
    mutation->value.kind = LC_MUTATION_VALUE_FILE;
    mutation->value.file_mode = file_mode;
    mutation->value.file_value_resolver = options->file_value_resolver;
    return lc_ms_resolve_file_path(value_text, options->file_value_base_dir,
                                   &mutation->value.file_path, error);
  }
  return lc_ms_parse_literal_value(value_text, time_mode, options,
                                   &mutation->value, error);
}

static int lc_ms_parse_expr(const char *expr,
                            const lc_mutation_parse_options *options,
                            lc_mutation_plan *plan, lc_error *error) {
  const char *cursor;
  const char *path_text;
  const char *value_text;
  lc_mutation_file_mode file_mode;
  int remove_mode;
  int time_mode;
  lc_mutation mutation;
  char **subparts = NULL;
  size_t subcount = 0U;
  size_t i;
  char *brace_text;
  double delta;
  char *copy;
  char *eq;
  size_t cursor_len;
  int rc;

  memset(&mutation, 0, sizeof(mutation));
  file_mode = 0;
  remove_mode = 0;
  time_mode = 0;
  cursor = expr;
  if (strncmp(cursor, "file:", 5) == 0) {
    file_mode = LC_MUTATION_FILE_AUTO;
    cursor += 5;
  } else if (strncmp(cursor, "textfile:", 9) == 0) {
    file_mode = LC_MUTATION_FILE_TEXT;
    cursor += 9;
  } else if (strncmp(cursor, "base64file:", 11) == 0) {
    file_mode = LC_MUTATION_FILE_BASE64;
    cursor += 11;
  }
  if (strncmp(cursor, "rm:", 3) == 0) {
    remove_mode = 1;
    cursor += 3;
  } else if (strncmp(cursor, "remove:", 7) == 0) {
    remove_mode = 1;
    cursor += 7;
  } else if (strncmp(cursor, "delete:", 7) == 0) {
    remove_mode = 1;
    cursor += 7;
  } else if (strncmp(cursor, "del:", 4) == 0) {
    remove_mode = 1;
    cursor += 4;
  }
  if (strncmp(cursor, "time:", 5) == 0) {
    if (file_mode != 0) {
      return lc_ms_set_error(
          error, "file-backed mutation cannot be combined with time:", expr);
    }
    if (remove_mode) {
      return lc_ms_set_error(
          error, "time-prefixed mutation cannot be combined with remove", expr);
    }
    time_mode = 1;
    cursor += 5;
  }
  if (remove_mode) {
    mutation.kind = LC_MUTATION_REMOVE;
    if (lc_ms_split_path(cursor, &mutation.path_segments,
                         &mutation.path_segment_count, error) != LC_OK) {
      return LC_ERR_INVALID;
    }
    rc = lc_ms_plan_append(plan, &mutation, error);
    if (rc != LC_OK) {
      lc_ms_mutation_cleanup(&mutation);
      return rc;
    }
    return LC_OK;
  }
  cursor_len = strlen(cursor);
  if (cursor_len >= 2U && strcmp(cursor + cursor_len - 2U, "++") == 0) {
    copy = lc_ms_strdup_range(cursor, cursor_len - 2U);
    if (copy == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate increment mutation", NULL, NULL,
                          NULL);
    }
    if (lc_ms_build_increment(copy, 1.0, &mutation, error) != LC_OK) {
      free(copy);
      return LC_ERR_INVALID;
    }
    free(copy);
    rc = lc_ms_plan_append(plan, &mutation, error);
    if (rc != LC_OK) {
      lc_ms_mutation_cleanup(&mutation);
      return rc;
    }
    return LC_OK;
  }
  if (cursor_len >= 2U && strcmp(cursor + cursor_len - 2U, "--") == 0) {
    copy = lc_ms_strdup_range(cursor, cursor_len - 2U);
    if (copy == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate increment mutation", NULL, NULL,
                          NULL);
    }
    if (lc_ms_build_increment(copy, -1.0, &mutation, error) != LC_OK) {
      free(copy);
      return LC_ERR_INVALID;
    }
    free(copy);
    rc = lc_ms_plan_append(plan, &mutation, error);
    if (rc != LC_OK) {
      lc_ms_mutation_cleanup(&mutation);
      return rc;
    }
    return LC_OK;
  }
  if (cursor_len > 0U && cursor[cursor_len - 1U] == '}') {
    const char *brace = strchr(cursor, '{');
    if (brace != NULL && brace > cursor) {
      brace_text = lc_ms_strdup_range(
          brace + 1, (size_t)(cursor + cursor_len - 1U - (brace + 1)));
      if (brace_text == NULL) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate brace mutation", NULL, NULL,
                            NULL);
      }
      if (lc_ms_split_expressions(brace_text, &subparts, &subcount, error) !=
          LC_OK) {
        free(brace_text);
        return LC_ERR_INVALID;
      }
      free(brace_text);
      copy = lc_ms_strdup_range(cursor, (size_t)(brace - cursor));
      if (copy == NULL) {
        for (i = 0U; i < subcount; ++i)
          free(subparts[i]);
        free(subparts);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate brace prefix", NULL, NULL,
                            NULL);
      }
      for (i = 0U; i < subcount; ++i) {
        char *combined;
        size_t combined_len;
        combined_len = strlen(copy) + strlen(subparts[i]) + 1U;
        combined = (char *)malloc(combined_len + 1U);
        if (combined == NULL) {
          free(copy);
          while (i < subcount)
            free(subparts[i++]);
          free(subparts);
          return lc_error_set(error, LC_ERR_NOMEM, 0L,
                              "failed to allocate brace mutation", NULL, NULL,
                              NULL);
        }
        strcpy(combined, copy);
        strcat(combined, subparts[i]);
        if (lc_ms_parse_expr(combined, options, plan, error) != LC_OK) {
          free(combined);
          free(copy);
          while (i < subcount)
            free(subparts[i++]);
          free(subparts);
          return LC_ERR_INVALID;
        }
        free(combined);
        free(subparts[i]);
      }
      free(subparts);
      free(copy);
      return LC_OK;
    }
  }
  copy = lc_strdup_local(cursor);
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate mutation expression", NULL, NULL,
                        NULL);
  }
  eq = strchr(copy, '=');
  if (eq == NULL) {
    free(copy);
    return lc_ms_set_error(error, "invalid mutation expression", expr);
  }
  *eq = '\0';
  path_text = copy;
  value_text = eq + 1;
  if (!time_mode && file_mode == 0) {
    char *delta_end;
    errno = 0;
    delta = strtod(value_text, &delta_end);
    if (errno == 0 && delta_end != value_text && *delta_end == '\0') {
      if (lc_ms_build_increment(path_text, delta, &mutation, error) != LC_OK) {
        lc_ms_mutation_cleanup(&mutation);
        free(copy);
        return LC_ERR_INVALID;
      }
      free(copy);
      rc = lc_ms_plan_append(plan, &mutation, error);
      if (rc != LC_OK) {
        lc_ms_mutation_cleanup(&mutation);
        return rc;
      }
      return LC_OK;
    }
  }
  if (lc_ms_build_set(path_text, value_text, time_mode, file_mode, options,
                      &mutation, error) != LC_OK) {
    lc_ms_mutation_cleanup(&mutation);
    free(copy);
    return LC_ERR_INVALID;
  }
  free(copy);
  rc = lc_ms_plan_append(plan, &mutation, error);
  if (rc != LC_OK) {
    lc_ms_mutation_cleanup(&mutation);
    return rc;
  }
  return LC_OK;
}

int lc_mutation_plan_build(const char *const *exprs, size_t expr_count,
                           const lc_mutation_parse_options *options,
                           lc_mutation_plan **out, lc_error *error) {
  lc_mutation_plan *plan;
  size_t i;
  int rc;

  if (exprs == NULL || expr_count == 0U || out == NULL) {
    return lc_ms_set_error(error, "local mutate requires mutation expressions",
                           NULL);
  }
  plan = (lc_mutation_plan *)calloc(1U, sizeof(*plan));
  if (plan == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate mutation plan", NULL, NULL, NULL);
  }
  for (i = 0U; i < expr_count; ++i) {
    if (exprs[i] == NULL || exprs[i][0] == '\0') {
      continue;
    }
    rc = lc_ms_parse_expr(exprs[i], options, plan, error);
    if (rc != LC_OK) {
      lc_mutation_plan_close(plan);
      return rc;
    }
  }
  if (plan->count == 0U) {
    lc_mutation_plan_close(plan);
    return lc_ms_set_error(error, "local mutate requires at least one mutation",
                           NULL);
  }
  *out = plan;
  return LC_OK;
}

int lc_mutation_plan_apply(const lc_mutation_plan *plan, FILE *input,
                           FILE **out_final, lc_error *error) {
  FILE *current;
  FILE *next;
  size_t i;
  int rc;

  if (plan == NULL || input == NULL || out_final == NULL) {
    return lc_ms_set_error(
        error, "mutation plan apply requires plan, input, and out_final", NULL);
  }
  current = input;
  rewind(current);
  for (i = 0U; i < plan->count; ++i) {
    next = tmpfile();
    if (next == NULL) {
      if (current != input) {
        fclose(current);
      }
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to create local mutate scratch file",
                          strerror(errno), NULL, NULL);
    }
    rewind(current);
    rc = lc_ms_visit_apply_single(&plan->items[i], current, next, error);
    if (rc != LC_OK) {
      fclose(next);
      if (current != input) {
        fclose(current);
      }
      return rc;
    }
    if (current != input) {
      fclose(current);
    }
    current = next;
  }
  rewind(current);
  *out_final = current;
  return LC_OK;
}

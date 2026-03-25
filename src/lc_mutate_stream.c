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

typedef struct lc_ms_reader {
  FILE *fp;
  int has_peek;
  int peek;
} lc_ms_reader;

typedef struct lc_ms_utf8_state {
  int remaining;
  unsigned int codepoint;
  unsigned int min_codepoint;
} lc_ms_utf8_state;

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

static int lc_ms_reader_get(lc_ms_reader *reader) {
  int ch;

  if (reader->has_peek) {
    reader->has_peek = 0;
    return reader->peek;
  }
  ch = fgetc(reader->fp);
  return ch;
}

static void lc_ms_reader_unget(lc_ms_reader *reader, int ch) {
  reader->has_peek = 1;
  reader->peek = ch;
}

static int lc_ms_skip_ws(lc_ms_reader *reader) {
  int ch;

  do {
    ch = lc_ms_reader_get(reader);
  } while (ch != EOF && isspace((unsigned char)ch));
  return ch;
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

static int lc_ms_emit_json_escaped_byte(FILE *out, unsigned char byte,
                                        lc_error *error) {
  static const char hex[] = "0123456789abcdef";
  char escaped[6];

  switch (byte) {
  case '"':
    return lc_ms_write_bytes(out, "\\\"", 2U, error);
  case '\\':
    return lc_ms_write_bytes(out, "\\\\", 2U, error);
  case '\b':
    return lc_ms_write_bytes(out, "\\b", 2U, error);
  case '\f':
    return lc_ms_write_bytes(out, "\\f", 2U, error);
  case '\n':
    return lc_ms_write_bytes(out, "\\n", 2U, error);
  case '\r':
    return lc_ms_write_bytes(out, "\\r", 2U, error);
  case '\t':
    return lc_ms_write_bytes(out, "\\t", 2U, error);
  default:
    escaped[0] = '\\';
    escaped[1] = 'u';
    escaped[2] = '0';
    escaped[3] = '0';
    escaped[4] = hex[(byte >> 4) & 0x0fU];
    escaped[5] = hex[byte & 0x0fU];
    return lc_ms_write_bytes(out, escaped, sizeof(escaped), error);
  }
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

static int lc_ms_emit_utf8_json_string(FILE *out, lc_source *source,
                                       lc_error *error) {
  unsigned char buffer[LC_MS_SCRATCH];
  size_t nread;
  size_t i;
  lc_ms_utf8_state utf8;
  int rc;

  memset(&utf8, 0, sizeof(utf8));
  rc = lc_ms_write_char(out, '"', error);
  if (rc != LC_OK) {
    return rc;
  }
  for (;;) {
    nread = source->read(source, buffer, sizeof(buffer), error);
    if (nread == 0U) {
      break;
    }
    for (i = 0; i < nread; ++i) {
      if (buffer[i] == '\0') {
        return lc_ms_set_error(
            error, "textfile mutation does not accept NUL bytes", NULL);
      }
      rc = lc_ms_utf8_push(&utf8, buffer[i], error);
      if (rc != LC_OK) {
        return rc;
      }
      if (buffer[i] < 0x20U || buffer[i] == '"' || buffer[i] == '\\') {
        rc = lc_ms_emit_json_escaped_byte(out, buffer[i], error);
      } else {
        rc = lc_ms_write_bytes(out, buffer + i, 1U, error);
      }
      if (rc != LC_OK) {
        return rc;
      }
    }
  }
  if (utf8.remaining != 0) {
    return lc_ms_set_error(error, "invalid UTF-8 in textfile mutation", NULL);
  }
  return lc_ms_write_char(out, '"', error);
}

static int lc_ms_emit_base64_string(FILE *out, lc_source *source,
                                    lc_error *error) {
  static const char base64_table[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  unsigned char buffer[LC_MS_SCRATCH];
  unsigned char tail[3];
  size_t nread;
  size_t i;
  size_t tail_len;
  int rc;
  char encoded[4];

  tail_len = 0U;
  rc = lc_ms_write_char(out, '"', error);
  if (rc != LC_OK) {
    return rc;
  }
  for (;;) {
    nread = source->read(source, buffer, sizeof(buffer), error);
    if (nread == 0U) {
      break;
    }
    i = 0U;
    if (tail_len != 0U) {
      while (tail_len < 3U && i < nread) {
        tail[tail_len++] = buffer[i++];
      }
      if (tail_len == 3U) {
        encoded[0] = base64_table[(tail[0] >> 2) & 0x3fU];
        encoded[1] =
            base64_table[((tail[0] & 0x03U) << 4) | ((tail[1] >> 4) & 0x0fU)];
        encoded[2] =
            base64_table[((tail[1] & 0x0fU) << 2) | ((tail[2] >> 6) & 0x03U)];
        encoded[3] = base64_table[tail[2] & 0x3fU];
        rc = lc_ms_write_bytes(out, encoded, sizeof(encoded), error);
        if (rc != LC_OK) {
          return rc;
        }
        tail_len = 0U;
      }
    }
    while (i + 3U <= nread) {
      encoded[0] = base64_table[(buffer[i] >> 2) & 0x3fU];
      encoded[1] = base64_table[((buffer[i] & 0x03U) << 4) |
                                ((buffer[i + 1U] >> 4) & 0x0fU)];
      encoded[2] = base64_table[((buffer[i + 1U] & 0x0fU) << 2) |
                                ((buffer[i + 2U] >> 6) & 0x03U)];
      encoded[3] = base64_table[buffer[i + 2U] & 0x3fU];
      rc = lc_ms_write_bytes(out, encoded, sizeof(encoded), error);
      if (rc != LC_OK) {
        return rc;
      }
      i += 3U;
    }
    tail_len = nread - i;
    if (tail_len != 0U) {
      memcpy(tail, buffer + i, tail_len);
    }
  }
  if (tail_len == 1U) {
    encoded[0] = base64_table[(tail[0] >> 2) & 0x3fU];
    encoded[1] = base64_table[(tail[0] & 0x03U) << 4];
    encoded[2] = '=';
    encoded[3] = '=';
    rc = lc_ms_write_bytes(out, encoded, sizeof(encoded), error);
  } else if (tail_len == 2U) {
    encoded[0] = base64_table[(tail[0] >> 2) & 0x3fU];
    encoded[1] =
        base64_table[((tail[0] & 0x03U) << 4) | ((tail[1] >> 4) & 0x0fU)];
    encoded[2] = base64_table[(tail[1] & 0x0fU) << 2];
    encoded[3] = '=';
    rc = lc_ms_write_bytes(out, encoded, sizeof(encoded), error);
  } else {
    rc = LC_OK;
  }
  if (rc != LC_OK) {
    return rc;
  }
  return lc_ms_write_char(out, '"', error);
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

static int lc_ms_detect_auto_file_mode(const lc_mutation_value *value,
                                       lc_mutation_file_mode *out_mode,
                                       lc_error *error) {
  lc_source *source;
  unsigned char buffer[LC_MS_SCRATCH];
  size_t nread;
  size_t i;
  int saw_nul;
  lc_ms_utf8_state utf8;
  int rc;

  *out_mode = LC_MUTATION_FILE_BASE64;
  saw_nul = 0;
  memset(&utf8, 0, sizeof(utf8));
  rc = lc_ms_open_file_source(value, &source, error);
  if (rc != LC_OK) {
    return rc;
  }
  for (;;) {
    nread = source->read(source, buffer, sizeof(buffer), error);
    if (nread == 0U) {
      break;
    }
    for (i = 0; i < nread; ++i) {
      if (buffer[i] == '\0') {
        saw_nul = 1;
        break;
      }
      rc = lc_ms_utf8_push(&utf8, buffer[i], error);
      if (rc != LC_OK) {
        source->close(source);
        return LC_OK;
      }
    }
    if (saw_nul) {
      break;
    }
  }
  source->close(source);
  if (!saw_nul && utf8.remaining == 0) {
    *out_mode = LC_MUTATION_FILE_TEXT;
  }
  return LC_OK;
}

static int lc_ms_emit_json_string_bytes(FILE *out, const char *text,
                                        lc_error *error) {
  const unsigned char *bytes;
  size_t i;
  int rc;

  rc = lc_ms_write_char(out, '"', error);
  if (rc != LC_OK) {
    return rc;
  }
  bytes = (const unsigned char *)text;
  for (i = 0U; bytes[i] != '\0'; ++i) {
    if (bytes[i] < 0x20U || bytes[i] == '"' || bytes[i] == '\\') {
      rc = lc_ms_emit_json_escaped_byte(out, bytes[i], error);
    } else {
      rc = lc_ms_write_bytes(out, bytes + i, 1U, error);
    }
    if (rc != LC_OK) {
      return rc;
    }
  }
  return lc_ms_write_char(out, '"', error);
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
  char buffer[64];
  int length;
  double integral_part;

  if (!isfinite(number)) {
    return lc_ms_set_error(error, "mutation produced non-finite number", NULL);
  }
  if (modf(number, &integral_part) == 0.0 &&
      integral_part >= (double)LONG_MIN && integral_part <= (double)LONG_MAX) {
    length = snprintf(buffer, sizeof(buffer), "%ld", (long)integral_part);
  } else {
    length = snprintf(buffer, sizeof(buffer), "%.17g", number);
  }
  if (length < 0) {
    return lc_ms_set_error(error, "failed to format mutation number", NULL);
  }
  return lc_ms_write_bytes(out, buffer, (size_t)length, error);
}

static int lc_ms_emit_mutation_value(FILE *out, const lc_mutation *mutation,
                                     double existing_value, int has_existing,
                                     lc_error *error) {
  lc_source *source;
  lc_mutation_file_mode mode;
  int rc;

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
      char buffer[64];
      int length;
      length = lc_i64_format_base10(mutation->value.long_value, buffer,
                                    sizeof(buffer));
      if (length < 0) {
        return lc_ms_set_error(error, "failed to format mutation integer",
                               NULL);
      }
      return lc_ms_write_bytes(out, buffer, (size_t)length, error);
    }
    case LC_MUTATION_VALUE_DOUBLE:
      return lc_ms_emit_number(out, mutation->value.double_value, error);
    case LC_MUTATION_VALUE_STRING:
      return lc_ms_emit_json_string_bytes(out, mutation->value.string_value,
                                          error);
    case LC_MUTATION_VALUE_FILE:
      mode = mutation->value.file_mode;
      if (mode == LC_MUTATION_FILE_AUTO) {
        rc = lc_ms_detect_auto_file_mode(&mutation->value, &mode, error);
        if (rc != LC_OK) {
          return rc;
        }
      }
      rc = lc_ms_open_file_source(&mutation->value, &source, error);
      if (rc != LC_OK) {
        return rc;
      }
      if (mode == LC_MUTATION_FILE_TEXT) {
        rc = lc_ms_emit_utf8_json_string(out, source, error);
      } else {
        rc = lc_ms_emit_base64_string(out, source, error);
      }
      source->close(source);
      return rc;
    default:
      return lc_ms_set_error(error, "unsupported mutation value type", NULL);
    }
  default:
    return lc_ms_set_error(error, "unsupported mutation kind", NULL);
  }
}

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

static int lc_ms_read_json_string(lc_ms_reader *reader, char **out,
                                  lc_error *error) {
  char *buffer;
  size_t length;
  size_t capacity;
  int ch;
  int hex_value;
  unsigned codepoint;

  buffer = NULL;
  length = 0U;
  capacity = 0U;
  ch = lc_ms_reader_get(reader);
  if (ch != '"') {
    return lc_ms_set_error(error, "expected JSON string", NULL);
  }
  for (;;) {
    ch = lc_ms_reader_get(reader);
    if (ch == EOF) {
      free(buffer);
      return lc_ms_set_error(error, "unexpected EOF in JSON string", NULL);
    }
    if (ch == '"') {
      break;
    }
    if (ch == '\\') {
      ch = lc_ms_reader_get(reader);
      if (ch == EOF) {
        free(buffer);
        return lc_ms_set_error(error, "unexpected EOF in JSON escape", NULL);
      }
      switch (ch) {
      case '"':
      case '\\':
      case '/':
        break;
      case 'b':
        ch = '\b';
        break;
      case 'f':
        ch = '\f';
        break;
      case 'n':
        ch = '\n';
        break;
      case 'r':
        ch = '\r';
        break;
      case 't':
        ch = '\t';
        break;
      case 'u':
        codepoint = 0U;
        for (hex_value = 0; hex_value < 4; ++hex_value) {
          ch = lc_ms_reader_get(reader);
          if (ch == EOF || !isxdigit((unsigned char)ch)) {
            free(buffer);
            return lc_ms_set_error(error, "invalid Unicode escape", NULL);
          }
          codepoint <<= 4;
          if (ch >= '0' && ch <= '9')
            codepoint |= (unsigned)(ch - '0');
          else if (ch >= 'a' && ch <= 'f')
            codepoint |= (unsigned)(10 + ch - 'a');
          else
            codepoint |= (unsigned)(10 + ch - 'A');
        }
        if (codepoint > 0x7fU) {
          free(buffer);
          return lc_ms_set_error(
              error,
              "non-ASCII object keys in local mutate are not supported yet",
              NULL);
        }
        ch = (int)codepoint;
        break;
      default:
        free(buffer);
        return lc_ms_set_error(error, "invalid JSON escape", NULL);
      }
    }
    if (length + 2U > capacity) {
      size_t next_capacity;
      char *next_buffer;

      next_capacity = capacity == 0U ? 32U : capacity * 2U;
      next_buffer = (char *)realloc(buffer, next_capacity);
      if (next_buffer == NULL) {
        free(buffer);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate JSON string", NULL, NULL, NULL);
      }
      buffer = next_buffer;
      capacity = next_capacity;
    }
    buffer[length++] = (char)ch;
  }
  if (buffer == NULL) {
    buffer = (char *)malloc(1U);
    if (buffer == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate empty JSON string", NULL, NULL,
                          NULL);
    }
  }
  buffer[length] = '\0';
  *out = buffer;
  return LC_OK;
}

static int lc_ms_copy_json_string_raw(lc_ms_reader *reader, FILE *out,
                                      lc_error *error) {
  int ch;
  int rc;

  ch = lc_ms_reader_get(reader);
  if (ch != '"') {
    return lc_ms_set_error(error, "expected JSON string", NULL);
  }
  rc = lc_ms_write_char(out, ch, error);
  if (rc != LC_OK) {
    return rc;
  }
  for (;;) {
    ch = lc_ms_reader_get(reader);
    if (ch == EOF) {
      return lc_ms_set_error(error, "unexpected EOF in JSON string", NULL);
    }
    rc = lc_ms_write_char(out, ch, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (ch == '\\') {
      ch = lc_ms_reader_get(reader);
      if (ch == EOF) {
        return lc_ms_set_error(error, "unexpected EOF in JSON escape", NULL);
      }
      rc = lc_ms_write_char(out, ch, error);
      if (rc != LC_OK) {
        return rc;
      }
      if (ch == 'u') {
        int i;
        for (i = 0; i < 4; ++i) {
          ch = lc_ms_reader_get(reader);
          if (ch == EOF) {
            return lc_ms_set_error(error, "unexpected EOF in JSON escape",
                                   NULL);
          }
          rc = lc_ms_write_char(out, ch, error);
          if (rc != LC_OK) {
            return rc;
          }
        }
      }
      continue;
    }
    if (ch == '"') {
      return LC_OK;
    }
  }
}

static int lc_ms_copy_raw_value(lc_ms_reader *reader, FILE *out,
                                lc_error *error);

static int lc_ms_copy_raw_compound(lc_ms_reader *reader, FILE *out, int open_ch,
                                   int close_ch, lc_error *error) {
  int ch;
  int first;
  int rc;

  rc = lc_ms_write_char(out, open_ch, error);
  if (rc != LC_OK) {
    return rc;
  }
  first = 1;
  for (;;) {
    ch = lc_ms_skip_ws(reader);
    if (ch == close_ch) {
      return lc_ms_write_char(out, close_ch, error);
    }
    if (!first) {
      if (ch != ',') {
        return lc_ms_set_error(error, "invalid JSON separator", NULL);
      }
      rc = lc_ms_write_char(out, ',', error);
      if (rc != LC_OK) {
        return rc;
      }
      ch = lc_ms_skip_ws(reader);
    }
    lc_ms_reader_unget(reader, ch);
    if (open_ch == '{') {
      rc = lc_ms_copy_json_string_raw(reader, out, error);
      if (rc != LC_OK) {
        return rc;
      }
      ch = lc_ms_skip_ws(reader);
      if (ch != ':') {
        return lc_ms_set_error(error, "expected object colon", NULL);
      }
      rc = lc_ms_write_char(out, ':', error);
      if (rc != LC_OK) {
        return rc;
      }
    }
    rc = lc_ms_copy_raw_value(reader, out, error);
    if (rc != LC_OK) {
      return rc;
    }
    first = 0;
  }
}

static int lc_ms_copy_raw_literal(lc_ms_reader *reader, FILE *out, int first_ch,
                                  lc_error *error) {
  int ch;
  int rc;

  rc = lc_ms_write_char(out, first_ch, error);
  if (rc != LC_OK) {
    return rc;
  }
  for (;;) {
    ch = lc_ms_reader_get(reader);
    if (ch == EOF) {
      return LC_OK;
    }
    if (isspace((unsigned char)ch) || ch == ',' || ch == ']' || ch == '}') {
      lc_ms_reader_unget(reader, ch);
      return LC_OK;
    }
    rc = lc_ms_write_char(out, ch, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
}

static int lc_ms_copy_raw_value(lc_ms_reader *reader, FILE *out,
                                lc_error *error) {
  int ch;

  ch = lc_ms_skip_ws(reader);
  if (ch == EOF) {
    return lc_ms_set_error(error, "unexpected EOF in JSON value", NULL);
  }
  switch (ch) {
  case '{':
    return lc_ms_copy_raw_compound(reader, out, '{', '}', error);
  case '[':
    return lc_ms_copy_raw_compound(reader, out, '[', ']', error);
  case '"':
    lc_ms_reader_unget(reader, ch);
    return lc_ms_copy_json_string_raw(reader, out, error);
  default:
    return lc_ms_copy_raw_literal(reader, out, ch, error);
  }
}

static int lc_ms_read_number_value(lc_ms_reader *reader, double *out,
                                   lc_error *error) {
  char buffer[128];
  size_t length;
  int ch;
  char *endptr;
  double number;

  length = 0U;
  ch = lc_ms_skip_ws(reader);
  if (ch == EOF) {
    return lc_ms_set_error(error, "unexpected EOF in numeric mutation target",
                           NULL);
  }
  do {
    if (length + 1U >= sizeof(buffer)) {
      return lc_ms_set_error(error, "numeric value too long", NULL);
    }
    buffer[length++] = (char)ch;
    ch = lc_ms_reader_get(reader);
  } while (ch != EOF && !isspace((unsigned char)ch) && ch != ',' && ch != ']' &&
           ch != '}');
  if (ch != EOF) {
    lc_ms_reader_unget(reader, ch);
  }
  buffer[length] = '\0';
  errno = 0;
  number = strtod(buffer, &endptr);
  if (errno != 0 || endptr == buffer || *endptr != '\0') {
    return lc_ms_set_error(error, "increment target is not numeric", buffer);
  }
  *out = number;
  return LC_OK;
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

static int lc_ms_transform_value(lc_ms_reader *reader, FILE *out,
                                 const lc_mutation *mutation, char **path,
                                 size_t path_count, lc_error *error);

static int lc_ms_transform_object(lc_ms_reader *reader, FILE *out,
                                  const lc_mutation *mutation, char **path,
                                  size_t path_count, lc_error *error) {
  int ch;
  int first;
  int rc;
  int saw_target_child;
  char *key;

  rc = lc_ms_write_char(out, '{', error);
  if (rc != LC_OK) {
    return rc;
  }
  first = 1;
  saw_target_child = 0;
  for (;;) {
    ch = lc_ms_skip_ws(reader);
    if (ch == '}') {
      break;
    }
    if (!first) {
      if (ch != ',') {
        return lc_ms_set_error(error, "invalid object separator", NULL);
      }
      ch = lc_ms_skip_ws(reader);
    }
    lc_ms_reader_unget(reader, ch);
    key = NULL;
    rc = lc_ms_read_json_string(reader, &key, error);
    if (rc != LC_OK) {
      return rc;
    }
    ch = lc_ms_skip_ws(reader);
    if (ch != ':') {
      free(key);
      return lc_ms_set_error(error, "expected object colon", NULL);
    }

    path[path_count] = key;
    if ((mutation->path_segment_count > path_count &&
         strcmp(mutation->path_segments[path_count], key) == 0) ||
        (mutation->path_segment_count == path_count + 1U &&
         strcmp(mutation->path_segments[path_count], key) == 0)) {
      saw_target_child = 1;
    }
    if (lc_ms_is_exact(mutation, path, path_count + 1U) &&
        mutation->kind == LC_MUTATION_REMOVE) {
      rc = lc_ms_copy_raw_value(reader, NULL, error);
      free(key);
      if (rc != LC_OK) {
        return rc;
      }
      continue;
    }

    if (!first) {
      rc = lc_ms_write_char(out, ',', error);
      if (rc != LC_OK) {
        free(key);
        return rc;
      }
    }
    rc = lc_ms_emit_json_string_bytes(out, key, error);
    if (rc == LC_OK) {
      rc = lc_ms_write_char(out, ':', error);
    }
    if (rc == LC_OK) {
      rc = lc_ms_transform_value(reader, out, mutation, path, path_count + 1U,
                                 error);
    }
    free(key);
    if (rc != LC_OK) {
      return rc;
    }
    first = 0;
  }

  if (!saw_target_child && lc_ms_has_descendant(mutation, path, path_count) &&
      mutation->kind != LC_MUTATION_REMOVE) {
    if (!first) {
      rc = lc_ms_write_char(out, ',', error);
      if (rc != LC_OK) {
        return rc;
      }
    }
    rc = lc_ms_emit_json_string_bytes(out, mutation->path_segments[path_count],
                                      error);
    if (rc == LC_OK) {
      rc = lc_ms_write_char(out, ':', error);
    }
    if (rc == LC_OK) {
      rc = lc_ms_emit_missing_chain(out, mutation, path_count + 1U, error);
    }
    if (rc != LC_OK) {
      return rc;
    }
  }

  return lc_ms_write_char(out, '}', error);
}

static int lc_ms_transform_array(lc_ms_reader *reader, FILE *out,
                                 const lc_mutation *mutation, char **path,
                                 size_t path_count, lc_error *error) {
  size_t index;
  int ch;
  int first;
  int rc;
  char index_buffer[32];

  rc = lc_ms_write_char(out, '[', error);
  if (rc != LC_OK) {
    return rc;
  }
  index = 0U;
  first = 1;
  for (;;) {
    ch = lc_ms_skip_ws(reader);
    if (ch == ']') {
      break;
    }
    if (!first) {
      if (ch != ',') {
        return lc_ms_set_error(error, "invalid array separator", NULL);
      }
      ch = lc_ms_skip_ws(reader);
    }
    lc_ms_reader_unget(reader, ch);
    if (!first) {
      rc = lc_ms_write_char(out, ',', error);
      if (rc != LC_OK) {
        return rc;
      }
    }
    snprintf(index_buffer, sizeof(index_buffer), "%lu", (unsigned long)index);
    path[path_count] = index_buffer;
    rc = lc_ms_transform_value(reader, out, mutation, path, path_count + 1U,
                               error);
    if (rc != LC_OK) {
      return rc;
    }
    first = 0;
    index += 1U;
  }
  return lc_ms_write_char(out, ']', error);
}

static int lc_ms_transform_value(lc_ms_reader *reader, FILE *out,
                                 const lc_mutation *mutation, char **path,
                                 size_t path_count, lc_error *error) {
  int ch;
  int rc;
  double existing_number = 0.0;

  if (lc_ms_is_exact(mutation, path, path_count)) {
    if (mutation->kind == LC_MUTATION_INCREMENT) {
      rc = lc_ms_read_number_value(reader, &existing_number, error);
      if (rc != LC_OK) {
        return rc;
      }
      return lc_ms_emit_mutation_value(out, mutation, existing_number, 1,
                                       error);
    }
    rc = lc_ms_copy_raw_value(reader, NULL, error);
    if (rc != LC_OK) {
      return rc;
    }
    return lc_ms_emit_mutation_value(out, mutation, 0.0, 0, error);
  }

  if (!lc_ms_has_descendant(mutation, path, path_count)) {
    return lc_ms_copy_raw_value(reader, out, error);
  }

  ch = lc_ms_skip_ws(reader);
  if (ch == EOF) {
    return lc_ms_set_error(error, "unexpected EOF in JSON value", NULL);
  }
  switch (ch) {
  case '{':
    return lc_ms_transform_object(reader, out, mutation, path, path_count,
                                  error);
  case '[':
    return lc_ms_transform_array(reader, out, mutation, path, path_count,
                                 error);
  default:
    lc_ms_reader_unget(reader, ch);
    rc = lc_ms_copy_raw_value(reader, NULL, error);
    if (rc != LC_OK) {
      return rc;
    }
    return lc_ms_emit_missing_chain(out, mutation, path_count, error);
  }
}

static int lc_ms_apply_single(const lc_mutation *mutation, FILE *input,
                              FILE *output, lc_error *error) {
  lc_ms_reader reader;
  char *path_stack[128];
  int ch;
  int rc;

  memset(&reader, 0, sizeof(reader));
  reader.fp = input;
  ch = lc_ms_skip_ws(&reader);
  if (ch == EOF) {
    return lc_ms_set_error(error, "local mutate input is empty", NULL);
  }
  if (ch != '{') {
    return lc_ms_set_error(
        error, "local mutate currently requires a JSON object root", NULL);
  }
  rc = lc_ms_transform_object(&reader, output, mutation, path_stack, 0U, error);
  if (rc != LC_OK) {
    return rc;
  }
  ch = lc_ms_skip_ws(&reader);
  if (ch != EOF) {
    return lc_ms_set_error(
        error, "local mutate input must contain exactly one JSON object", NULL);
  }
  if (fflush(output) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to flush mutate stream output", strerror(errno),
                        NULL, NULL);
  }
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
    rc = lc_ms_apply_single(&plan->items[i], current, next, error);
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

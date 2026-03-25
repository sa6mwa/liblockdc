#include "lc_internal.h"
#include "lc_log.h"

#include <ctype.h>
#include <lc/version.h>
#include <openssl/err.h>
#include <openssl/x509v3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *LC_ENGINE_VERSION_STRING = LC_VERSION_STRING;

static size_t lc_engine_curl_write_callback(void *contents, size_t size,
                                            size_t nmemb, void *userdata);
static size_t lc_engine_header_callback(char *buffer, size_t size,
                                        size_t nitems, void *userdata);
static CURLcode lc_engine_ssl_ctx_callback(CURL *curl, void *ssl_ctx,
                                           void *userdata);
static int lc_engine_append_hex_escape(lc_engine_buffer *buffer,
                                       unsigned char value);
static int lc_engine_case_equal_n(const char *left, const char *right,
                                  size_t count);
static const char *lc_engine_skip_ws(const char *value);
static const char *lc_engine_find_field(const char *json,
                                        const char *field_name);
static int lc_engine_capture_string(const char *value, char **out_value);
static int lc_engine_store_x509(X509 ***items, size_t *item_count, X509 *value);
static int lc_engine_match_private_key(X509 *certificate, EVP_PKEY *candidate);
static void lc_engine_assign_header_string(char **target, const char *value,
                                           const char *end);
static const char *lc_engine_trim_http_ows_end(const char *value,
                                               const char *end);
static size_t
lc_engine_header_numeric_parse_failed(lc_engine_http_result *result,
                                      const char *message);
static char *lc_engine_join_endpoints(const lc_engine_client *client);

const char *lc_engine_version_string(void) { return LC_ENGINE_VERSION_STRING; }

void lc_engine_client_config_init(lc_engine_client_config *config) {
  if (config == NULL) {
    return;
  }
  memset(config, 0, sizeof(*config));
  config->timeout_ms = 30000L;
  config->prefer_http_2 = 1;
}

void lc_engine_error_init(lc_engine_error *error) {
  if (error == NULL) {
    return;
  }
  memset(error, 0, sizeof(*error));
}

void lc_engine_error_reset(lc_engine_error *error) {
  if (error == NULL) {
    return;
  }
  lc_engine_free_string(&error->message);
  lc_engine_free_string(&error->server_error_code);
  lc_engine_free_string(&error->detail);
  lc_engine_free_string(&error->leader_endpoint);
  lc_engine_free_string(&error->current_etag);
  lc_engine_free_string(&error->correlation_id);
  memset(error, 0, sizeof(*error));
}

void lc_engine_error_cleanup(lc_engine_error *error) {
  lc_engine_error_reset(error);
}

void lc_engine_acquire_response_cleanup(lc_engine_acquire_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->namespace_name);
  lc_engine_free_string(&response->lease_id);
  lc_engine_free_string(&response->txn_id);
  lc_engine_free_string(&response->key);
  lc_engine_free_string(&response->owner);
  lc_engine_free_string(&response->state_etag);
  lc_engine_free_string(&response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_engine_get_response_cleanup(lc_engine_get_response *response) {
  if (response == NULL) {
    return;
  }
  free(response->body);
  lc_engine_free_string(&response->content_type);
  lc_engine_free_string(&response->etag);
  lc_engine_free_string(&response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_engine_keepalive_response_cleanup(
    lc_engine_keepalive_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->state_etag);
  lc_engine_free_string(&response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_engine_release_response_cleanup(lc_engine_release_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_engine_query_response_cleanup(lc_engine_query_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->raw_json);
  lc_engine_free_string(&response->cursor);
  lc_engine_free_string(&response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_engine_enqueue_response_cleanup(lc_engine_enqueue_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->namespace_name);
  lc_engine_free_string(&response->queue);
  lc_engine_free_string(&response->message_id);
  lc_engine_free_string(&response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_engine_dequeue_response_cleanup(lc_engine_dequeue_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->namespace_name);
  lc_engine_free_string(&response->queue);
  lc_engine_free_string(&response->message_id);
  lc_engine_free_string(&response->payload_content_type);
  free(response->payload);
  lc_engine_free_string(&response->correlation_id);
  lc_engine_free_string(&response->lease_id);
  lc_engine_free_string(&response->txn_id);
  lc_engine_free_string(&response->meta_etag);
  lc_engine_free_string(&response->state_etag);
  lc_engine_free_string(&response->state_lease_id);
  lc_engine_free_string(&response->state_txn_id);
  lc_engine_free_string(&response->next_cursor);
  memset(response, 0, sizeof(*response));
}

char *lc_engine_strdup_local(const char *value) {
  size_t length;
  char *copy;

  if (value == NULL) {
    return NULL;
  }
  length = strlen(value);
  copy = (char *)malloc(length + 1U);
  if (copy == NULL) {
    return NULL;
  }
  memcpy(copy, value, length + 1U);
  return copy;
}

char *lc_engine_strdup_range(const char *begin, const char *end) {
  size_t length;
  char *copy;

  if (begin == NULL || end == NULL || end < begin) {
    return NULL;
  }
  length = (size_t)(end - begin);
  copy = (char *)malloc(length + 1U);
  if (copy == NULL) {
    return NULL;
  }
  if (length > 0U) {
    memcpy(copy, begin, length);
  }
  copy[length] = '\0';
  return copy;
}

void lc_engine_free_string(char **value) {
  if (value == NULL || *value == NULL) {
    return;
  }
  free(*value);
  *value = NULL;
}

void lc_engine_trim_trailing_slash(char *value) {
  size_t length;

  if (value == NULL) {
    return;
  }
  length = strlen(value);
  while (length > 0U && value[length - 1U] == '/') {
    value[length - 1U] = '\0';
    --length;
  }
}

int lc_engine_set_client_error(lc_engine_error *error, int code,
                               const char *message) {
  if (error != NULL) {
    lc_engine_error_reset(error);
    error->code = code;
    error->message = lc_engine_strdup_local(message);
    if (message != NULL && error->message == NULL) {
      return LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  return code;
}

int lc_engine_set_transport_error(lc_engine_error *error, const char *message) {
  return lc_engine_set_client_error(error, LC_ENGINE_ERROR_TRANSPORT, message);
}

int lc_engine_set_protocol_error(lc_engine_error *error, const char *message) {
  return lc_engine_set_client_error(error, LC_ENGINE_ERROR_PROTOCOL, message);
}

void lc_engine_buffer_init(lc_engine_buffer *buffer) {
  if (buffer == NULL) {
    return;
  }
  memset(buffer, 0, sizeof(*buffer));
}

void lc_engine_buffer_cleanup(lc_engine_buffer *buffer) {
  if (buffer == NULL) {
    return;
  }
  free(buffer->data);
  memset(buffer, 0, sizeof(*buffer));
}

int lc_engine_buffer_append(lc_engine_buffer *buffer, const char *bytes,
                            size_t count) {
  size_t required;
  size_t capacity;
  char *next_data;

  if (buffer == NULL || (count > 0U && bytes == NULL)) {
    return LC_ENGINE_ERROR_INVALID_ARGUMENT;
  }
  required = buffer->length + count + 1U;
  if (required > buffer->capacity) {
    capacity = buffer->capacity == 0U ? 256U : buffer->capacity;
    while (capacity < required) {
      capacity *= 2U;
    }
    next_data = (char *)realloc(buffer->data, capacity);
    if (next_data == NULL) {
      return LC_ENGINE_ERROR_NO_MEMORY;
    }
    buffer->data = next_data;
    buffer->capacity = capacity;
  }
  if (count > 0U) {
    memcpy(buffer->data + buffer->length, bytes, count);
    buffer->length += count;
  }
  buffer->data[buffer->length] = '\0';
  return LC_ENGINE_OK;
}

int lc_engine_buffer_append_cstr(lc_engine_buffer *buffer, const char *value) {
  if (value == NULL) {
    return LC_ENGINE_OK;
  }
  return lc_engine_buffer_append(buffer, value, strlen(value));
}

int lc_engine_json_begin_object(lc_engine_buffer *buffer) {
  return lc_engine_buffer_append(buffer, "{", 1U);
}

int lc_engine_json_end_object(lc_engine_buffer *buffer) {
  return lc_engine_buffer_append(buffer, "}", 1U);
}

static int lc_engine_json_add_separator(lc_engine_buffer *buffer,
                                        int *first_field) {
  if (*first_field) {
    *first_field = 0;
    return LC_ENGINE_OK;
  }
  return lc_engine_buffer_append(buffer, ",", 1U);
}

static int lc_engine_json_add_escaped(lc_engine_buffer *buffer,
                                      const char *value) {
  const unsigned char *cursor;
  int rc;

  rc = lc_engine_buffer_append(buffer, "\"", 1U);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  cursor = (const unsigned char *)value;
  while (*cursor != '\0') {
    switch (*cursor) {
    case '\\':
      rc = lc_engine_buffer_append(buffer, "\\\\", 2U);
      break;
    case '"':
      rc = lc_engine_buffer_append(buffer, "\\\"", 2U);
      break;
    case '\b':
      rc = lc_engine_buffer_append(buffer, "\\b", 2U);
      break;
    case '\f':
      rc = lc_engine_buffer_append(buffer, "\\f", 2U);
      break;
    case '\n':
      rc = lc_engine_buffer_append(buffer, "\\n", 2U);
      break;
    case '\r':
      rc = lc_engine_buffer_append(buffer, "\\r", 2U);
      break;
    case '\t':
      rc = lc_engine_buffer_append(buffer, "\\t", 2U);
      break;
    default:
      if (*cursor < 0x20U) {
        rc = lc_engine_append_hex_escape(buffer, *cursor);
      } else {
        rc = lc_engine_buffer_append(buffer, (const char *)cursor, 1U);
      }
      break;
    }
    if (rc != LC_ENGINE_OK) {
      return rc;
    }
    ++cursor;
  }
  return lc_engine_buffer_append(buffer, "\"", 1U);
}

int lc_engine_json_add_string_field(lc_engine_buffer *buffer, int *first_field,
                                    const char *name, const char *value) {
  int rc;

  if (value == NULL) {
    return LC_ENGINE_OK;
  }
  rc = lc_engine_json_add_separator(buffer, first_field);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  rc = lc_engine_json_add_escaped(buffer, name);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  rc = lc_engine_buffer_append(buffer, ":", 1U);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  return lc_engine_json_add_escaped(buffer, value);
}

int lc_engine_json_add_long_field(lc_engine_buffer *buffer, int *first_field,
                                  const char *name, long value) {
  char digits[64];
  int rc;

  rc = lc_engine_json_add_separator(buffer, first_field);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  rc = lc_engine_json_add_escaped(buffer, name);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  rc = lc_engine_buffer_append(buffer, ":", 1U);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  sprintf(digits, "%ld", value);
  return lc_engine_buffer_append_cstr(buffer, digits);
}

int lc_engine_json_add_bool_field(lc_engine_buffer *buffer, int *first_field,
                                  const char *name, int value) {
  int rc;

  rc = lc_engine_json_add_separator(buffer, first_field);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  rc = lc_engine_json_add_escaped(buffer, name);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  rc = lc_engine_buffer_append(buffer, ":", 1U);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (value) {
    return lc_engine_buffer_append(buffer, "true", 4U);
  }
  return lc_engine_buffer_append(buffer, "false", 5U);
}

int lc_engine_json_add_raw_field(lc_engine_buffer *buffer, int *first_field,
                                 const char *name, const char *value) {
  int rc;

  if (value == NULL) {
    return LC_ENGINE_OK;
  }
  rc = lc_engine_json_add_separator(buffer, first_field);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  rc = lc_engine_json_add_escaped(buffer, name);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  rc = lc_engine_buffer_append(buffer, ":", 1U);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  return lc_engine_buffer_append_cstr(buffer, value);
}

static int lc_engine_append_hex_escape(lc_engine_buffer *buffer,
                                       unsigned char value) {
  char encoded[6];
  static const char hex[] = "0123456789abcdef";

  encoded[0] = '\\';
  encoded[1] = 'u';
  encoded[2] = '0';
  encoded[3] = '0';
  encoded[4] = hex[(value >> 4) & 0x0fU];
  encoded[5] = hex[value & 0x0fU];
  return lc_engine_buffer_append(buffer, encoded, sizeof(encoded));
}

static const char *lc_engine_skip_ws(const char *value) {
  while (value != NULL && *value != '\0' && isspace((unsigned char)*value)) {
    ++value;
  }
  return value;
}

static const char *lc_engine_find_field(const char *json,
                                        const char *field_name) {
  char pattern[128];
  size_t pattern_length;
  const char *match;

  pattern[0] = '"';
  pattern[1] = '\0';
  strncat(pattern, field_name, sizeof(pattern) - 3U);
  strcat(pattern, "\"");
  pattern_length = strlen(pattern);

  match = json;
  while (match != NULL) {
    match = strstr(match, pattern);
    if (match == NULL) {
      return NULL;
    }
    if ((match == json || match[-1] != '\\') && match[pattern_length] != '\0') {
      match += pattern_length;
      match = lc_engine_skip_ws(match);
      if (match != NULL && *match == ':') {
        return lc_engine_skip_ws(match + 1);
      }
    }
    ++match;
  }
  return NULL;
}

static int lc_engine_capture_string(const char *value, char **out_value) {
  const char *cursor;
  lc_engine_buffer decoded;
  int rc;

  if (value == NULL || *value != '"') {
    return LC_ENGINE_ERROR_PROTOCOL;
  }
  lc_engine_buffer_init(&decoded);
  cursor = value + 1;
  while (*cursor != '\0') {
    if (*cursor == '"') {
      *out_value = decoded.data;
      return LC_ENGINE_OK;
    }
    if (*cursor == '\\') {
      ++cursor;
      if (*cursor == '\0') {
        lc_engine_buffer_cleanup(&decoded);
        return LC_ENGINE_ERROR_PROTOCOL;
      }
      switch (*cursor) {
      case '"':
      case '\\':
      case '/':
        rc = lc_engine_buffer_append(&decoded, cursor, 1U);
        break;
      case 'b':
        rc = lc_engine_buffer_append(&decoded, "\b", 1U);
        break;
      case 'f':
        rc = lc_engine_buffer_append(&decoded, "\f", 1U);
        break;
      case 'n':
        rc = lc_engine_buffer_append(&decoded, "\n", 1U);
        break;
      case 'r':
        rc = lc_engine_buffer_append(&decoded, "\r", 1U);
        break;
      case 't':
        rc = lc_engine_buffer_append(&decoded, "\t", 1U);
        break;
      case 'u':
        if (cursor[1] == '0' && cursor[2] == '0' &&
            isxdigit((unsigned char)cursor[3]) &&
            isxdigit((unsigned char)cursor[4])) {
          unsigned int high;
          unsigned int low;
          char ch;

          high = (unsigned int)(isdigit((unsigned char)cursor[3])
                                    ? cursor[3] - '0'
                                    : (tolower((unsigned char)cursor[3]) - 'a' +
                                       10));
          low = (unsigned int)(isdigit((unsigned char)cursor[4])
                                   ? cursor[4] - '0'
                                   : (tolower((unsigned char)cursor[4]) - 'a' +
                                      10));
          ch = (char)((high << 4) | low);
          rc = lc_engine_buffer_append(&decoded, &ch, 1U);
          cursor += 4;
        } else {
          lc_engine_buffer_cleanup(&decoded);
          return LC_ENGINE_ERROR_PROTOCOL;
        }
        break;
      default:
        lc_engine_buffer_cleanup(&decoded);
        return LC_ENGINE_ERROR_PROTOCOL;
      }
      if (rc != LC_ENGINE_OK) {
        lc_engine_buffer_cleanup(&decoded);
        return rc;
      }
      ++cursor;
      continue;
    }
    rc = lc_engine_buffer_append(&decoded, cursor, 1U);
    if (rc != LC_ENGINE_OK) {
      lc_engine_buffer_cleanup(&decoded);
      return rc;
    }
    ++cursor;
  }
  lc_engine_buffer_cleanup(&decoded);
  return LC_ENGINE_ERROR_PROTOCOL;
}

int lc_engine_json_get_string(const char *json, const char *field_name,
                              char **out_value) {
  const char *value;

  if (json == NULL || field_name == NULL || out_value == NULL) {
    return LC_ENGINE_ERROR_INVALID_ARGUMENT;
  }
  value = lc_engine_find_field(json, field_name);
  if (value == NULL || strncmp(value, "null", 4U) == 0) {
    return LC_ENGINE_OK;
  }
  return lc_engine_capture_string(value, out_value);
}

int lc_engine_json_get_long(const char *json, const char *field_name,
                            long *out_value) {
  const char *value;
  const char *end;

  if (json == NULL || field_name == NULL || out_value == NULL) {
    return LC_ENGINE_ERROR_INVALID_ARGUMENT;
  }
  value = lc_engine_find_field(json, field_name);
  if (value == NULL) {
    return LC_ENGINE_OK;
  }
  end = value;
  while (*end != '\0' && *end != ',' && *end != '}' && *end != ']' &&
         *end != ' ' && *end != '\t' && *end != '\r' && *end != '\n') {
    ++end;
  }
  if (!lc_parse_long_base10_range_checked(value, (size_t)(end - value),
                                          out_value)) {
    return LC_ENGINE_ERROR_PROTOCOL;
  }
  return LC_ENGINE_OK;
}

int lc_engine_json_get_bool(const char *json, const char *field_name,
                            int *out_value) {
  const char *value;

  if (json == NULL || field_name == NULL || out_value == NULL) {
    return LC_ENGINE_ERROR_INVALID_ARGUMENT;
  }
  value = lc_engine_find_field(json, field_name);
  if (value == NULL) {
    return LC_ENGINE_OK;
  }
  if (strncmp(value, "true", 4U) == 0) {
    *out_value = 1;
    return LC_ENGINE_OK;
  }
  if (strncmp(value, "false", 5U) == 0) {
    *out_value = 0;
    return LC_ENGINE_OK;
  }
  return LC_ENGINE_ERROR_PROTOCOL;
}

static size_t lc_engine_curl_write_callback(void *contents, size_t size,
                                            size_t nmemb, void *userdata) {
  lc_engine_buffer *buffer;
  size_t total;

  buffer = (lc_engine_buffer *)userdata;
  total = size * nmemb;
  if (lc_engine_buffer_append(buffer, (const char *)contents, total) !=
      LC_ENGINE_OK) {
    return 0U;
  }
  return total;
}

static int lc_engine_case_equal_n(const char *left, const char *right,
                                  size_t count) {
  size_t index;

  for (index = 0U; index < count; ++index) {
    if (tolower((unsigned char)left[index]) !=
        tolower((unsigned char)right[index])) {
      return 0;
    }
  }
  return 1;
}

static void lc_engine_assign_header_string(char **target, const char *value,
                                           const char *end) {
  lc_engine_free_string(target);
  *target = lc_engine_strdup_range(value, end);
}

static const char *lc_engine_trim_http_ows_end(const char *value,
                                               const char *end) {
  while (end > value && (end[-1] == ' ' || end[-1] == '\t')) {
    --end;
  }
  return end;
}

static size_t
lc_engine_header_numeric_parse_failed(lc_engine_http_result *result,
                                      const char *message) {
  if (result != NULL) {
    result->header_parse_failed = 1;
    lc_engine_free_string(&result->header_parse_error_message);
    result->header_parse_error_message = lc_engine_strdup_local(message);
  }
  return 0U;
}

static char *lc_engine_join_endpoints(const lc_engine_client *client) {
  size_t total_length;
  size_t index;
  char *joined;
  char *cursor;

  if (client == NULL || client->endpoint_count == 0U) {
    return NULL;
  }
  total_length = 1U;
  for (index = 0U; index < client->endpoint_count; ++index) {
    if (client->endpoints[index] != NULL) {
      total_length += strlen(client->endpoints[index]);
    }
    if (index + 1U < client->endpoint_count) {
      total_length += 1U;
    }
  }
  joined = (char *)malloc(total_length);
  if (joined == NULL) {
    return NULL;
  }
  cursor = joined;
  for (index = 0U; index < client->endpoint_count; ++index) {
    size_t item_length;

    item_length = client->endpoints[index] != NULL
                      ? strlen(client->endpoints[index])
                      : 0U;
    if (item_length > 0U) {
      memcpy(cursor, client->endpoints[index], item_length);
      cursor += item_length;
    }
    if (index + 1U < client->endpoint_count) {
      *cursor++ = ',';
    }
  }
  *cursor = '\0';
  return joined;
}

static size_t lc_engine_header_callback(char *buffer, size_t size,
                                        size_t nitems, void *userdata) {
  lc_engine_http_result *result;
  size_t total;
  const char *value;
  const char *end;
  size_t name_length;

  result = (lc_engine_http_result *)userdata;
  total = size * nitems;

  if (total > 0U) {
    name_length = strlen("X-Correlation-Id:");
    if (total > name_length &&
        lc_engine_case_equal_n(buffer, "X-Correlation-Id:", name_length)) {
      value = buffer + name_length;
      while (*value == ' ' || *value == '\t') {
        ++value;
      }
      end = buffer + total;
      while (end > value && (end[-1] == '\r' || end[-1] == '\n')) {
        --end;
      }
      lc_engine_assign_header_string(&result->correlation_id, value, end);
      return total;
    }

    name_length = strlen("ETag:");
    if (total > name_length &&
        lc_engine_case_equal_n(buffer, "ETag:", name_length)) {
      value = buffer + name_length;
      while (*value == ' ' || *value == '\t') {
        ++value;
      }
      end = buffer + total;
      while (end > value && (end[-1] == '\r' || end[-1] == '\n')) {
        --end;
      }
      if (end > value && *value == '"' && end[-1] == '"') {
        ++value;
        --end;
      }
      lc_engine_assign_header_string(&result->etag, value, end);
      return total;
    }

    name_length = strlen("X-Key-Version:");
    if (total > name_length &&
        lc_engine_case_equal_n(buffer, "X-Key-Version:", name_length)) {
      value = buffer + name_length;
      while (*value == ' ' || *value == '\t') {
        ++value;
      }
      end = buffer + total;
      while (end > value && (end[-1] == '\r' || end[-1] == '\n')) {
        --end;
      }
      end = lc_engine_trim_http_ows_end(value, end);
      if (!lc_parse_long_base10_range_checked(value, (size_t)(end - value),
                                              &result->key_version)) {
        return lc_engine_header_numeric_parse_failed(
            result, "response X-Key-Version header is invalid");
      }
      return total;
    }

    name_length = strlen("X-Fencing-Token:");
    if (total > name_length &&
        lc_engine_case_equal_n(buffer, "X-Fencing-Token:", name_length)) {
      value = buffer + name_length;
      while (*value == ' ' || *value == '\t') {
        ++value;
      }
      end = buffer + total;
      while (end > value && (end[-1] == '\r' || end[-1] == '\n')) {
        --end;
      }
      end = lc_engine_trim_http_ows_end(value, end);
      if (!lc_parse_long_base10_range_checked(value, (size_t)(end - value),
                                              &result->fencing_token)) {
        return lc_engine_header_numeric_parse_failed(
            result, "response X-Fencing-Token header is invalid");
      }
      return total;
    }

    name_length = strlen("Content-Type:");
    if (total > name_length &&
        lc_engine_case_equal_n(buffer, "Content-Type:", name_length)) {
      value = buffer + name_length;
      while (*value == ' ' || *value == '\t') {
        ++value;
      }
      end = buffer + total;
      while (end > value && (end[-1] == '\r' || end[-1] == '\n')) {
        --end;
      }
      lc_engine_assign_header_string(&result->content_type, value, end);
      return total;
    }
  }

  return total;
}

void lc_engine_http_result_cleanup(lc_engine_http_result *result) {
  if (result == NULL) {
    return;
  }
  lc_engine_buffer_cleanup(&result->body);
  lc_engine_free_string(&result->correlation_id);
  lc_engine_free_string(&result->etag);
  lc_engine_free_string(&result->content_type);
  lc_engine_free_string(&result->header_parse_error_message);
  memset(result, 0, sizeof(*result));
}

static void lc_engine_build_url(lc_engine_client *client, const char *endpoint,
                                const char *path, lc_engine_buffer *url) {
  const char *scheme;

  scheme = strstr(endpoint, "://");
  if (scheme == NULL) {
    if (client->disable_mtls) {
      lc_engine_buffer_append_cstr(url, "http://");
    } else {
      lc_engine_buffer_append_cstr(url, "https://");
    }
  }
  lc_engine_buffer_append_cstr(url, endpoint);
  lc_engine_buffer_append_cstr(url, path);
}

static CURLcode lc_engine_ssl_ctx_callback(CURL *curl, void *ssl_ctx,
                                           void *userdata) {
  lc_engine_client *client;
  SSL_CTX *ctx;
  X509_STORE *store;
  size_t index;

  (void)curl;
  client = (lc_engine_client *)userdata;
  if (client == NULL || client->disable_mtls) {
    return CURLE_OK;
  }

  ctx = (SSL_CTX *)ssl_ctx;
  if (client->tls_bundle.client_cert == NULL ||
      client->tls_bundle.client_key == NULL) {
    return CURLE_SSL_CERTPROBLEM;
  }
  if (SSL_CTX_use_certificate(ctx, client->tls_bundle.client_cert) != 1) {
    return CURLE_SSL_CERTPROBLEM;
  }
  if (SSL_CTX_use_PrivateKey(ctx, client->tls_bundle.client_key) != 1) {
    return CURLE_SSL_CERTPROBLEM;
  }
  if (SSL_CTX_check_private_key(ctx) != 1) {
    return CURLE_SSL_CERTPROBLEM;
  }

  for (index = 0U; index < client->tls_bundle.chain_cert_count; ++index) {
    X509 *copy;

    copy = X509_dup(client->tls_bundle.chain_certs[index]);
    if (copy == NULL) {
      return CURLE_OUT_OF_MEMORY;
    }
    if (SSL_CTX_add_extra_chain_cert(ctx, copy) != 1) {
      X509_free(copy);
      return CURLE_SSL_CERTPROBLEM;
    }
  }

  store = SSL_CTX_get_cert_store(ctx);
  for (index = 0U; index < client->tls_bundle.ca_cert_count; ++index) {
    if (X509_STORE_add_cert(store, client->tls_bundle.ca_certs[index]) != 1) {
      ERR_clear_error();
    }
  }

  return CURLE_OK;
}

int lc_engine_http_request(lc_engine_client *client, const char *method,
                           const char *path, const void *body,
                           size_t body_length,
                           const lc_engine_header_pair *headers,
                           size_t header_count, lc_engine_http_result *result,
                           lc_engine_error *error) {
  size_t endpoint_index;
  char *endpoint_list;

  if (client == NULL || method == NULL || path == NULL || result == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, method, path, and result are required");
  }

  memset(result, 0, sizeof(*result));
  lc_engine_buffer_init(&result->body);
  endpoint_list = lc_engine_join_endpoints(client);
  if (endpoint_list != NULL) {
    pslog_field order_fields[3];

    order_fields[0] = lc_log_str_field("method", method);
    order_fields[1] = lc_log_str_field("path", path);
    order_fields[2] = lc_log_str_field("endpoints", endpoint_list);
    lc_log_trace(client->logger, "client.http.order", order_fields, 3U);
    free(endpoint_list);
  }

  for (endpoint_index = 0U; endpoint_index < client->endpoint_count;
       ++endpoint_index) {
    CURL *easy;
    struct curl_slist *curl_headers;
    lc_engine_buffer url;
    size_t header_index;
    char error_buffer[CURL_ERROR_SIZE];
    CURLcode curl_code;
    int should_retry;

    lc_engine_http_result_cleanup(result);
    lc_engine_buffer_init(&result->body);
    lc_engine_buffer_init(&url);
    lc_engine_build_url(client, client->endpoints[endpoint_index], path, &url);

    easy = curl_easy_init();
    curl_headers = NULL;
    memset(error_buffer, 0, sizeof(error_buffer));
    if (easy == NULL) {
      lc_engine_buffer_cleanup(&url);
      return lc_engine_set_transport_error(error, "curl_easy_init failed");
    }

    curl_easy_setopt(easy, CURLOPT_URL, url.data);
    {
      pslog_field attempt_fields[5];

      attempt_fields[0] = lc_log_str_field("method", method);
      attempt_fields[1] = lc_log_str_field("path", path);
      attempt_fields[2] =
          lc_log_str_field("endpoint", client->endpoints[endpoint_index]);
      attempt_fields[3] = lc_log_u64_field("attempt", endpoint_index + 1U);
      attempt_fields[4] = lc_log_u64_field("total", client->endpoint_count);
      lc_log_trace(client->logger, "client.http.attempt", attempt_fields, 5U);
    }
    curl_easy_setopt(easy, CURLOPT_CUSTOMREQUEST, method);
    curl_easy_setopt(easy, CURLOPT_FOLLOWLOCATION, 0L);
    curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION,
                     lc_engine_curl_write_callback);
    curl_easy_setopt(easy, CURLOPT_WRITEDATA, &result->body);
    curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION, lc_engine_header_callback);
    curl_easy_setopt(easy, CURLOPT_HEADERDATA, result);
    curl_easy_setopt(easy, CURLOPT_ERRORBUFFER, error_buffer);
    curl_easy_setopt(easy, CURLOPT_HTTP_VERSION,
                     client->prefer_http_2 ? CURL_HTTP_VERSION_2TLS
                                           : CURL_HTTP_VERSION_1_1);
    curl_easy_setopt(easy, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(easy, CURLOPT_TIMEOUT_MS,
                     client->timeout_ms > 0L ? client->timeout_ms : 30000L);
    if (client->unix_socket_path != NULL &&
        client->unix_socket_path[0] != '\0') {
      curl_easy_setopt(easy, CURLOPT_UNIX_SOCKET_PATH,
                       client->unix_socket_path);
    }

    if (!client->disable_mtls) {
      curl_easy_setopt(easy, CURLOPT_SSL_VERIFYPEER,
                       client->insecure_skip_verify ? 0L : 1L);
      curl_easy_setopt(easy, CURLOPT_SSL_VERIFYHOST,
                       client->insecure_skip_verify ? 0L : 2L);
      curl_easy_setopt(easy, CURLOPT_SSL_CTX_FUNCTION,
                       lc_engine_ssl_ctx_callback);
      curl_easy_setopt(easy, CURLOPT_SSL_CTX_DATA, client);
    }

    for (header_index = 0U; header_index < header_count; ++header_index) {
      lc_engine_buffer line;

      lc_engine_buffer_init(&line);
      lc_engine_buffer_append_cstr(&line, headers[header_index].name);
      lc_engine_buffer_append_cstr(&line, ": ");
      lc_engine_buffer_append_cstr(&line, headers[header_index].value);
      curl_headers = curl_slist_append(curl_headers, line.data);
      lc_engine_buffer_cleanup(&line);
      if (curl_headers == NULL) {
        curl_easy_cleanup(easy);
        lc_engine_buffer_cleanup(&url);
        return lc_engine_set_client_error(
            error, LC_ENGINE_ERROR_NO_MEMORY,
            "failed to allocate curl header list");
      }
    }

    if (body != NULL) {
      curl_easy_setopt(easy, CURLOPT_POSTFIELDS, body);
      curl_easy_setopt(easy, CURLOPT_POSTFIELDSIZE, (long)body_length);
    }

    if (curl_headers != NULL) {
      curl_easy_setopt(easy, CURLOPT_HTTPHEADER, curl_headers);
    }

    curl_code = curl_easy_perform(easy);
    if (curl_code != CURLE_OK) {
      pslog_field error_fields[6];
      const char *error_text;

      error_text = error_buffer[0] != '\0' ? error_buffer
                                           : curl_easy_strerror(curl_code);
      error_fields[0] = lc_log_str_field("method", method);
      error_fields[1] = lc_log_str_field("path", path);
      error_fields[2] =
          lc_log_str_field("endpoint", client->endpoints[endpoint_index]);
      error_fields[3] = lc_log_u64_field("attempt", endpoint_index + 1U);
      error_fields[4] = lc_log_u64_field("total", client->endpoint_count);
      error_fields[5] = lc_log_str_field("error", error_text);
      lc_log_trace(client->logger, "client.http.error", error_fields, 6U);
      curl_slist_free_all(curl_headers);
      curl_easy_cleanup(easy);
      lc_engine_buffer_cleanup(&url);
      if (result->header_parse_failed) {
        lc_engine_set_protocol_error(error,
                                     result->header_parse_error_message != NULL
                                         ? result->header_parse_error_message
                                         : "response header is invalid");
        lc_engine_http_result_cleanup(result);
        return error != NULL ? error->code : LC_ENGINE_ERROR_PROTOCOL;
      }
      if (endpoint_index + 1U < client->endpoint_count) {
        continue;
      }
      if (error_buffer[0] != '\0') {
        lc_engine_set_transport_error(error, error_buffer);
        lc_engine_http_result_cleanup(result);
        return error != NULL ? error->code : LC_ENGINE_ERROR_TRANSPORT;
      }
      lc_engine_set_transport_error(error, curl_easy_strerror(curl_code));
      lc_engine_http_result_cleanup(result);
      return error != NULL ? error->code : LC_ENGINE_ERROR_TRANSPORT;
    }

    curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &result->http_status);
    {
      pslog_field success_fields[7];

      success_fields[0] = lc_log_str_field("method", method);
      success_fields[1] = lc_log_str_field("path", path);
      success_fields[2] =
          lc_log_str_field("endpoint", client->endpoints[endpoint_index]);
      success_fields[3] = lc_log_u64_field("attempt", endpoint_index + 1U);
      success_fields[4] = lc_log_u64_field("total", client->endpoint_count);
      success_fields[5] = pslog_i64("status", (pslog_int64)result->http_status);
      success_fields[6] = lc_log_str_field("cid", result->correlation_id);
      lc_log_trace(client->logger, "client.http.success", success_fields, 7U);
    }
    curl_slist_free_all(curl_headers);
    curl_easy_cleanup(easy);
    lc_engine_buffer_cleanup(&url);

    should_retry = 0;
    if (result->http_status == 503L &&
        endpoint_index + 1U < client->endpoint_count) {
      char *server_code;
      server_code = NULL;
      if (result->body.data != NULL &&
          lc_engine_json_get_string(result->body.data, "error", &server_code) ==
              LC_ENGINE_OK &&
          server_code != NULL) {
        if (strcmp(server_code, "node_passive") == 0) {
          should_retry = 1;
        }
      }
      lc_engine_free_string(&server_code);
    }
    if (should_retry) {
      continue;
    }
    return LC_ENGINE_OK;
  }

  {
    pslog_field failure_fields[3];

    failure_fields[0] = lc_log_str_field("method", method);
    failure_fields[1] = lc_log_str_field("path", path);
    failure_fields[2] =
        lc_log_str_field("error", error != NULL ? error->message : NULL);
    lc_log_debug(client->logger, "client.http.unreachable", failure_fields, 3U);
  }
  return lc_engine_set_transport_error(error,
                                       "all configured endpoints failed");
}

static int lc_engine_store_x509(X509 ***items, size_t *item_count,
                                X509 *value) {
  X509 **next_items;

  next_items = (X509 **)realloc(*items, (*item_count + 1U) * sizeof(X509 *));
  if (next_items == NULL) {
    return LC_ENGINE_ERROR_NO_MEMORY;
  }
  X509_up_ref(value);
  next_items[*item_count] = value;
  *items = next_items;
  *item_count += 1U;
  return LC_ENGINE_OK;
}

static int lc_engine_match_private_key(X509 *certificate, EVP_PKEY *candidate) {
  EVP_PKEY *public_key;
  int match;

  public_key = X509_get_pubkey(certificate);
  if (public_key == NULL) {
    return 0;
  }
  match = EVP_PKEY_eq(public_key, candidate) == 1;
  EVP_PKEY_free(public_key);
  return match;
}

int lc_engine_load_bundle(lc_engine_client *client, const char *bundle_path,
                          lc_engine_error *error) {
  BIO *bio;
  STACK_OF(X509_INFO) * info_stack;
  size_t index;
  int rc;

  if (client == NULL || bundle_path == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "client and bundle path are required");
  }

  bio = BIO_new_file(bundle_path, "rb");
  if (bio == NULL) {
    return lc_engine_set_transport_error(error, "failed to open client bundle");
  }

  info_stack = PEM_X509_INFO_read_bio(bio, NULL, NULL, NULL);
  BIO_free(bio);
  if (info_stack == NULL) {
    return lc_engine_set_protocol_error(error,
                                        "failed to parse client bundle PEM");
  }

  rc = LC_ENGINE_OK;
  for (index = 0U; index < (size_t)sk_X509_INFO_num(info_stack); ++index) {
    X509_INFO *info;

    info = sk_X509_INFO_value(info_stack, (int)index);
    if (info->x509 != NULL) {
      if (X509_check_ca(info->x509)) {
        rc =
            lc_engine_store_x509(&client->tls_bundle.ca_certs,
                                 &client->tls_bundle.ca_cert_count, info->x509);
      } else if (client->tls_bundle.client_cert == NULL) {
        X509_up_ref(info->x509);
        client->tls_bundle.client_cert = info->x509;
      } else {
        rc = lc_engine_store_x509(&client->tls_bundle.chain_certs,
                                  &client->tls_bundle.chain_cert_count,
                                  info->x509);
      }
      if (rc != LC_ENGINE_OK) {
        break;
      }
    }
  }

  if (rc == LC_ENGINE_OK && client->tls_bundle.client_cert != NULL) {
    for (index = 0U; index < (size_t)sk_X509_INFO_num(info_stack); ++index) {
      X509_INFO *info;

      info = sk_X509_INFO_value(info_stack, (int)index);
      if (info->x_pkey != NULL && info->x_pkey->dec_pkey != NULL &&
          lc_engine_match_private_key(client->tls_bundle.client_cert,
                                      info->x_pkey->dec_pkey)) {
        EVP_PKEY_up_ref(info->x_pkey->dec_pkey);
        client->tls_bundle.client_key = info->x_pkey->dec_pkey;
        break;
      }
    }
  }

  sk_X509_INFO_pop_free(info_stack, X509_INFO_free);

  if (rc != LC_ENGINE_OK) {
    lc_engine_free_bundle(&client->tls_bundle);
    return lc_engine_set_client_error(error, rc,
                                      "failed to retain parsed client bundle");
  }
  if (client->tls_bundle.client_cert == NULL) {
    lc_engine_free_bundle(&client->tls_bundle);
    return lc_engine_set_protocol_error(
        error, "client bundle does not contain a client certificate");
  }
  if (client->tls_bundle.client_key == NULL) {
    lc_engine_free_bundle(&client->tls_bundle);
    return lc_engine_set_protocol_error(
        error, "client bundle does not contain a matching private key");
  }
  if (client->tls_bundle.ca_cert_count == 0U) {
    lc_engine_free_bundle(&client->tls_bundle);
    return lc_engine_set_protocol_error(
        error, "client bundle does not contain a CA certificate");
  }
  return LC_ENGINE_OK;
}

void lc_engine_free_bundle(lc_engine_tls_bundle *bundle) {
  size_t index;

  if (bundle == NULL) {
    return;
  }
  if (bundle->client_cert != NULL) {
    X509_free(bundle->client_cert);
  }
  if (bundle->client_key != NULL) {
    EVP_PKEY_free(bundle->client_key);
  }
  for (index = 0U; index < bundle->ca_cert_count; ++index) {
    X509_free(bundle->ca_certs[index]);
  }
  for (index = 0U; index < bundle->chain_cert_count; ++index) {
    X509_free(bundle->chain_certs[index]);
  }
  free(bundle->ca_certs);
  free(bundle->chain_certs);
  memset(bundle, 0, sizeof(*bundle));
}

const char *lc_engine_effective_namespace(lc_engine_client *client,
                                          const char *namespace_name) {
  if (namespace_name != NULL && namespace_name[0] != '\0') {
    return namespace_name;
  }
  if (client != NULL && client->default_namespace != NULL &&
      client->default_namespace[0] != '\0') {
    return client->default_namespace;
  }
  return "default";
}

char *lc_engine_url_encode(const char *value) {
  CURL *easy;
  char *escaped;
  char *copy;

  if (value == NULL) {
    return lc_engine_strdup_local("");
  }
  easy = curl_easy_init();
  if (easy == NULL) {
    return NULL;
  }
  escaped = curl_easy_escape(easy, value, 0);
  curl_easy_cleanup(easy);
  if (escaped == NULL) {
    return NULL;
  }
  copy = lc_engine_strdup_local(escaped);
  curl_free(escaped);
  return copy;
}

int lc_engine_set_server_error_from_result(
    lc_engine_error *error, const lc_engine_http_result *result) {
  int rc;

  rc = lc_engine_set_client_error(error, LC_ENGINE_ERROR_SERVER,
                                  "lockd returned a non-success status");
  if (rc != LC_ENGINE_OK) {
    return rc;
  }

  if (error != NULL) {
    error->http_status = result->http_status;
    if (result->correlation_id != NULL) {
      error->correlation_id = lc_engine_strdup_local(result->correlation_id);
      if (error->correlation_id == NULL) {
        return LC_ENGINE_ERROR_NO_MEMORY;
      }
    }
    if (result->body.data != NULL) {
      if (lc_engine_json_get_string(result->body.data, "error",
                                    &error->server_error_code) !=
          LC_ENGINE_OK) {
        return lc_engine_set_protocol_error(error,
                                            "failed to decode error.error");
      }
      if (lc_engine_json_get_string(result->body.data, "detail",
                                    &error->detail) != LC_ENGINE_OK) {
        return lc_engine_set_protocol_error(error,
                                            "failed to decode error.detail");
      }
      if (lc_engine_json_get_string(result->body.data, "leader_endpoint",
                                    &error->leader_endpoint) != LC_ENGINE_OK) {
        return lc_engine_set_protocol_error(
            error, "failed to decode error.leader_endpoint");
      }
      if (lc_engine_json_get_string(result->body.data, "current_etag",
                                    &error->current_etag) != LC_ENGINE_OK) {
        return lc_engine_set_protocol_error(
            error, "failed to decode error.current_etag");
      }
      if (lc_engine_json_get_long(result->body.data, "current_version",
                                  &error->current_version) != LC_ENGINE_OK) {
        return lc_engine_set_protocol_error(
            error, "failed to decode error.current_version");
      }
      if (lc_engine_json_get_long(result->body.data, "retry_after_seconds",
                                  &error->retry_after_seconds) !=
          LC_ENGINE_OK) {
        return lc_engine_set_protocol_error(
            error, "failed to decode error.retry_after_seconds");
      }
    }
  }

  return LC_ENGINE_ERROR_SERVER;
}

int lc_engine_client_open(const lc_engine_client_config *config,
                          lc_engine_client **out_client,
                          lc_engine_error *error) {
  lc_engine_client *client;
  size_t index;

  if (config == NULL || out_client == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "config and out_client are required");
  }
  if (config->endpoints == NULL || config->endpoint_count == 0U) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "at least one endpoint is required");
  }
  if (!config->disable_mtls && (config->client_bundle_path == NULL ||
                                config->client_bundle_path[0] == '\0')) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client_bundle_path is required unless disable_mtls is enabled");
  }

  curl_global_init(CURL_GLOBAL_DEFAULT);
  OPENSSL_init_ssl(0U, NULL);

  client = (lc_engine_client *)calloc(1U, sizeof(*client));
  if (client == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to allocate client");
  }

  client->endpoint_count = config->endpoint_count;
  client->endpoints = (char **)calloc(client->endpoint_count, sizeof(char *));
  if (client->endpoints == NULL) {
    lc_engine_client_close(client);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to allocate endpoint array");
  }
  for (index = 0U; index < client->endpoint_count; ++index) {
    client->endpoints[index] = lc_engine_strdup_local(config->endpoints[index]);
    if (client->endpoints[index] == NULL) {
      lc_engine_client_close(client);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to copy endpoint");
    }
    lc_engine_trim_trailing_slash(client->endpoints[index]);
  }

  client->default_namespace = lc_engine_strdup_local(
      config->default_namespace != NULL ? config->default_namespace
                                        : "default");
  if (client->default_namespace == NULL) {
    lc_engine_client_close(client);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to copy default namespace");
  }
  client->unix_socket_path = lc_engine_strdup_local(config->unix_socket_path);
  if (config->unix_socket_path != NULL && client->unix_socket_path == NULL) {
    lc_engine_client_close(client);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to copy unix_socket_path");
  }

  client->timeout_ms = config->timeout_ms > 0L ? config->timeout_ms : 30000L;
  client->disable_mtls = config->disable_mtls ? 1 : 0;
  client->insecure_skip_verify = config->insecure_skip_verify ? 1 : 0;
  client->prefer_http_2 = config->prefer_http_2 ? 1 : 0;
  client->disable_logger_sys_field = config->disable_logger_sys_field ? 1 : 0;
  client->base_logger =
      config->logger != NULL ? config->logger : lc_log_noop_logger();
  client->logger = lc_log_client_logger(client->base_logger,
                                        client->disable_logger_sys_field);
  if (client->logger == NULL) {
    lc_engine_client_close(client);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to initialize client logger");
  }
  client->owns_logger = (client->logger != client->base_logger &&
                         client->logger != lc_log_noop_logger())
                            ? 1
                            : 0;

  if (!client->disable_mtls) {
    if (lc_engine_load_bundle(client, config->client_bundle_path, error) !=
        LC_ENGINE_OK) {
      lc_engine_client_close(client);
      return error != NULL ? error->code : LC_ENGINE_ERROR_PROTOCOL;
    }
  }

  *out_client = client;
  {
    pslog_field fields[2];

    fields[0] = lc_log_u64_field("endpoint_count", client->endpoint_count);
    fields[1] =
        lc_log_str_field("default_namespace", client->default_namespace);
    lc_log_info(client->logger, "client.init", fields, 2U);
  }
  return LC_ENGINE_OK;
}

pslog_logger *lc_engine_client_logger(lc_engine_client *client) {
  if (client == NULL || client->logger == NULL) {
    return lc_log_noop_logger();
  }
  return client->logger;
}

void lc_engine_client_close(lc_engine_client *client) {
  size_t index;

  if (client == NULL) {
    return;
  }
  lc_engine_free_bundle(&client->tls_bundle);
  if (client->endpoints != NULL) {
    for (index = 0U; index < client->endpoint_count; ++index) {
      lc_engine_free_string(&client->endpoints[index]);
    }
  }
  free(client->endpoints);
  lc_engine_free_string(&client->unix_socket_path);
  lc_engine_free_string(&client->default_namespace);
  if (client->owns_logger && client->logger != NULL &&
      client->logger != lc_log_noop_logger()) {
    client->logger->destroy(client->logger);
  }
  free(client);
}

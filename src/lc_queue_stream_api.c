#include "lc_internal.h"
#include "lc_log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <curl/curl.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <yajl/yajl_tree.h>

typedef struct lc_engine_watch_state {
  lc_engine_queue_watch_handler handler;
  void *handler_context;
  lc_engine_error *error;
  lc_engine_buffer line_buffer;
  lc_engine_buffer data_buffer;
  lc_engine_buffer error_body;
  char *event_name;
  char *correlation_id;
  long http_status;
  int callback_failed;
} lc_engine_watch_state;

typedef enum lc_engine_subscribe_phase {
  LC_ENGINE_SUBSCRIBE_EXPECT_BOUNDARY = 0,
  LC_ENGINE_SUBSCRIBE_READ_HEADERS = 1,
  LC_ENGINE_SUBSCRIBE_READ_META = 2,
  LC_ENGINE_SUBSCRIBE_READ_PAYLOAD = 3,
  LC_ENGINE_SUBSCRIBE_DONE = 4
} lc_engine_subscribe_phase;

typedef struct lc_engine_subscribe_state {
  lc_engine_queue_stream_handler handler;
  void *handler_context;
  lc_engine_error *error;
  lc_engine_buffer line_buffer;
  lc_engine_buffer meta_buffer;
  lc_engine_buffer error_body;
  char *correlation_id;
  char *content_type;
  char *boundary;
  char *boundary_line;
  char *closing_boundary_line;
  char *part_name;
  char *part_content_type;
  lc_engine_dequeue_response current;
  long http_status;
  long part_content_length;
  long payload_remaining;
  lc_engine_subscribe_phase phase;
  int delivery_active;
  int callback_failed;
} lc_engine_subscribe_state;

static void lc_engine_queue_stream_log_attempt(lc_engine_client *client,
                                               const char *path,
                                               size_t endpoint_index) {
  pslog_field fields[5];

  fields[0] = lc_log_str_field("method", "POST");
  fields[1] = lc_log_str_field("path", path);
  fields[2] = lc_log_str_field("endpoint", client->endpoints[endpoint_index]);
  fields[3] = lc_log_u64_field("attempt", endpoint_index + 1U);
  fields[4] = lc_log_u64_field("total", client->endpoint_count);
  lc_log_trace(client->logger, "client.http.attempt", fields, 5U);
}

static void lc_engine_queue_stream_log_error(lc_engine_client *client,
                                             const char *path,
                                             size_t endpoint_index,
                                             const char *error_text) {
  pslog_field fields[6];

  fields[0] = lc_log_str_field("method", "POST");
  fields[1] = lc_log_str_field("path", path);
  fields[2] = lc_log_str_field("endpoint", client->endpoints[endpoint_index]);
  fields[3] = lc_log_u64_field("attempt", endpoint_index + 1U);
  fields[4] = lc_log_u64_field("total", client->endpoint_count);
  fields[5] = lc_log_str_field("error", error_text);
  lc_log_trace(client->logger, "client.http.error", fields, 6U);
}

static void lc_engine_queue_stream_log_success(lc_engine_client *client,
                                               const char *path,
                                               size_t endpoint_index,
                                               long http_status,
                                               const char *correlation_id) {
  pslog_field fields[7];

  fields[0] = lc_log_str_field("method", "POST");
  fields[1] = lc_log_str_field("path", path);
  fields[2] = lc_log_str_field("endpoint", client->endpoints[endpoint_index]);
  fields[3] = lc_log_u64_field("attempt", endpoint_index + 1U);
  fields[4] = lc_log_u64_field("total", client->endpoint_count);
  fields[5] = lc_log_i64_field("status", http_status);
  fields[6] = lc_log_str_field("cid", correlation_id);
  lc_log_trace(client->logger, "client.http.success", fields, 7U);
}

static int lc_engine_queue_stream_progress(void *client_ptr, curl_off_t dltotal,
                                           curl_off_t dlnow, curl_off_t ultotal,
                                           curl_off_t ulnow) {
  lc_engine_client *client;

  (void)dltotal;
  (void)dlnow;
  (void)ultotal;
  (void)ulnow;
  client = (lc_engine_client *)client_ptr;
  if (client != NULL && client->cancel_check != NULL &&
      client->cancel_check(client->cancel_context)) {
    return 1;
  }
  return 0;
}

static CURLcode lc_engine_queue_stream_ssl_ctx(CURL *curl, void *ssl_ctx,
                                               void *userptr) {
  lc_engine_client *client;
  SSL_CTX *ctx;
  X509_STORE *store;
  size_t index;

  (void)curl;
  client = (lc_engine_client *)userptr;
  ctx = (SSL_CTX *)ssl_ctx;
  if (client == NULL || ctx == NULL) {
    return CURLE_SSL_CERTPROBLEM;
  }
  if (client->tls_bundle.client_cert != NULL &&
      SSL_CTX_use_certificate(ctx, client->tls_bundle.client_cert) != 1) {
    return CURLE_SSL_CERTPROBLEM;
  }
  if (client->tls_bundle.client_key != NULL &&
      SSL_CTX_use_PrivateKey(ctx, client->tls_bundle.client_key) != 1) {
    return CURLE_SSL_CERTPROBLEM;
  }
  store = SSL_CTX_get_cert_store(ctx);
  if (store == NULL) {
    return CURLE_SSL_CERTPROBLEM;
  }
  for (index = 0U; index < client->tls_bundle.ca_cert_count; ++index) {
    if (client->tls_bundle.ca_certs[index] != NULL) {
      X509_STORE_add_cert(store, client->tls_bundle.ca_certs[index]);
    }
  }
  for (index = 0U; index < client->tls_bundle.chain_cert_count; ++index) {
    if (client->tls_bundle.chain_certs[index] != NULL) {
      X509_up_ref(client->tls_bundle.chain_certs[index]);
      SSL_CTX_add_extra_chain_cert(ctx, client->tls_bundle.chain_certs[index]);
    }
  }
  return CURLE_OK;
}

static int lc_engine_queue_header_name_equals(const char *begin, size_t length,
                                              const char *expected) {
  size_t index;
  size_t expected_length;

  expected_length = strlen(expected);
  if (length != expected_length) {
    return 0;
  }
  for (index = 0U; index < length; ++index) {
    char a;
    char b;

    a = begin[index];
    b = expected[index];
    if (a >= 'A' && a <= 'Z') {
      a = (char)(a - 'A' + 'a');
    }
    if (b >= 'A' && b <= 'Z') {
      b = (char)(b - 'A' + 'a');
    }
    if (a != b) {
      return 0;
    }
  }
  return 1;
}

static int lc_engine_queue_set_header_value(char **slot, const char *begin,
                                            const char *end) {
  char *copy;

  copy = lc_engine_strdup_range(begin, end);
  if (copy == NULL) {
    return 0;
  }
  lc_engine_free_string(slot);
  *slot = copy;
  return 1;
}

static size_t lc_engine_watch_header_callback(char *buffer, size_t size,
                                              size_t nitems, void *userdata) {
  lc_engine_watch_state *state;
  size_t total;
  char *colon;
  char *value;
  char *end;
  char status_line[64];
  size_t status_length;
  long parsed;

  state = (lc_engine_watch_state *)userdata;
  total = size * nitems;
  if (total == 0U) {
    return 0U;
  }
  if (total >= 5U && memcmp(buffer, "HTTP/", 5U) == 0) {
    status_length = total;
    if (status_length >= sizeof(status_line)) {
      status_length = sizeof(status_line) - 1U;
    }
    memcpy(status_line, buffer, status_length);
    status_line[status_length] = '\0';
    parsed = 0L;
    sscanf(status_line, "%*s %ld", &parsed);
    state->http_status = parsed;
    return total;
  }
  colon = (char *)memchr(buffer, ':', total);
  if (colon == NULL) {
    return total;
  }
  value = colon + 1;
  end = buffer + total;
  while (value < end && (*value == ' ' || *value == '\t')) {
    ++value;
  }
  while (end > value && (end[-1] == '\r' || end[-1] == '\n' || end[-1] == ' ' ||
                         end[-1] == '\t')) {
    --end;
  }
  if (lc_engine_queue_header_name_equals(buffer, (size_t)(colon - buffer),
                                         "X-Correlation-Id")) {
    lc_engine_free_string(&state->correlation_id);
    state->correlation_id = lc_engine_strdup_range(value, end);
    if (state->correlation_id == NULL) {
      state->callback_failed = 1;
      return 0U;
    }
  }
  return total;
}

void lc_engine_queue_watch_event_cleanup(lc_engine_queue_watch_event *event) {
  if (event == NULL) {
    return;
  }
  lc_engine_free_string(&event->namespace_name);
  lc_engine_free_string(&event->queue);
  lc_engine_free_string(&event->head_message_id);
  lc_engine_free_string(&event->correlation_id);
}

static int lc_engine_watch_event_name_is_supported(const char *event_name) {
  if (event_name == NULL || event_name[0] == '\0') {
    return 1;
  }
  return strcmp(event_name, "queue_watch") == 0;
}

static int lc_engine_watch_dispatch_event(lc_engine_watch_state *state) {
  yajl_val root;
  lc_engine_queue_watch_event event;
  char errbuf[128];
  static const char *path_namespace[] = {"namespace", NULL};
  static const char *path_queue[] = {"queue", NULL};
  static const char *path_available[] = {"available", NULL};
  static const char *path_head[] = {"head_message_id", NULL};
  static const char *path_changed[] = {"changed_at_unix", NULL};
  static const char *path_correlation[] = {"correlation_id", NULL};
  yajl_val value;

  if (state->data_buffer.length == 0U) {
    return LC_ENGINE_OK;
  }
  if (!lc_engine_watch_event_name_is_supported(state->event_name)) {
    state->data_buffer.length = 0U;
    if (state->data_buffer.data != NULL) {
      state->data_buffer.data[0] = '\0';
    }
    lc_engine_free_string(&state->event_name);
    return LC_ENGINE_OK;
  }

  errbuf[0] = '\0';
  root = yajl_tree_parse(state->data_buffer.data, errbuf, sizeof(errbuf));
  if (root == NULL) {
    return lc_engine_set_protocol_error(
        state->error,
        errbuf[0] != '\0' ? errbuf : "failed to parse queue watch event");
  }

  memset(&event, 0, sizeof(event));
  value = yajl_tree_get(root, path_namespace, yajl_t_string);
  if (value != NULL) {
    event.namespace_name = lc_engine_strdup_local(YAJL_GET_STRING(value));
    if (event.namespace_name == NULL) {
      yajl_tree_free(root);
      return lc_engine_set_client_error(
          state->error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to allocate queue watch namespace");
    }
  }
  value = yajl_tree_get(root, path_queue, yajl_t_string);
  if (value != NULL) {
    event.queue = lc_engine_strdup_local(YAJL_GET_STRING(value));
    if (event.queue == NULL) {
      yajl_tree_free(root);
      lc_engine_queue_watch_event_cleanup(&event);
      return lc_engine_set_client_error(state->error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to allocate queue watch queue");
    }
  }
  value = yajl_tree_get(root, path_head, yajl_t_string);
  if (value != NULL) {
    event.head_message_id = lc_engine_strdup_local(YAJL_GET_STRING(value));
    if (event.head_message_id == NULL) {
      yajl_tree_free(root);
      lc_engine_queue_watch_event_cleanup(&event);
      return lc_engine_set_client_error(
          state->error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to allocate queue watch head_message_id");
    }
  }
  value = yajl_tree_get(root, path_available, yajl_t_true);
  event.available = value != NULL;
  value = yajl_tree_get(root, path_changed, yajl_t_number);
  if (value != NULL) {
    if (!lc_parse_long_base10_checked(YAJL_GET_NUMBER(value),
                                      &event.changed_at_unix)) {
      yajl_tree_free(root);
      lc_engine_queue_watch_event_cleanup(&event);
      return lc_engine_set_protocol_error(
          state->error, "queue watch changed_at_unix is out of range");
    }
  }
  value = yajl_tree_get(root, path_correlation, yajl_t_string);
  if (value != NULL) {
    event.correlation_id = lc_engine_strdup_local(YAJL_GET_STRING(value));
  } else if (state->correlation_id != NULL) {
    event.correlation_id = lc_engine_strdup_local(state->correlation_id);
  }
  yajl_tree_free(root);

  if (value != NULL || state->correlation_id != NULL) {
    if (event.correlation_id == NULL) {
      lc_engine_queue_watch_event_cleanup(&event);
      return lc_engine_set_client_error(
          state->error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to allocate queue watch correlation_id");
    }
  }

  if (!state->handler(state->handler_context, &event, state->error)) {
    lc_engine_queue_watch_event_cleanup(&event);
    if (state->error->code == LC_ENGINE_OK) {
      return lc_engine_set_transport_error(state->error,
                                           "queue watch handler failed");
    }
    return state->error->code;
  }

  lc_engine_queue_watch_event_cleanup(&event);
  state->data_buffer.length = 0U;
  if (state->data_buffer.data != NULL) {
    state->data_buffer.data[0] = '\0';
  }
  lc_engine_free_string(&state->event_name);
  return LC_ENGINE_OK;
}

static int lc_engine_watch_process_line(lc_engine_watch_state *state) {
  char *line;
  const char *value;

  line = state->line_buffer.data;
  if (line == NULL) {
    return LC_ENGINE_OK;
  }
  if (line[0] == '\0') {
    return lc_engine_watch_dispatch_event(state);
  }
  if (line[0] == ':') {
    return LC_ENGINE_OK;
  }
  if (strncmp(line, "event:", 6) == 0) {
    value = line + 6;
    while (*value == ' ' || *value == '\t') {
      ++value;
    }
    lc_engine_free_string(&state->event_name);
    state->event_name = lc_engine_strdup_local(value);
    if (state->event_name == NULL) {
      return lc_engine_set_client_error(state->error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to allocate SSE event name");
    }
  } else if (strncmp(line, "data:", 5) == 0) {
    value = line + 5;
    while (*value == ' ' || *value == '\t') {
      ++value;
    }
    if (state->data_buffer.length > 0U &&
        lc_engine_buffer_append_cstr(&state->data_buffer, "\n") !=
            LC_ENGINE_OK) {
      return lc_engine_set_client_error(state->error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to append SSE newline");
    }
    if (lc_engine_buffer_append_cstr(&state->data_buffer, value) !=
        LC_ENGINE_OK) {
      return lc_engine_set_client_error(state->error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to append SSE data");
    }
  }
  return LC_ENGINE_OK;
}

static size_t lc_engine_watch_write_callback(char *ptr, size_t size,
                                             size_t nmemb, void *userdata) {
  lc_engine_watch_state *state;
  size_t total;
  size_t index;
  int rc;
  char ch;

  state = (lc_engine_watch_state *)userdata;
  total = size * nmemb;
  if (state->http_status >= 400L) {
    if (lc_engine_buffer_append(&state->error_body, ptr, total) !=
        LC_ENGINE_OK) {
      state->callback_failed = 1;
      lc_engine_set_client_error(state->error, LC_ENGINE_ERROR_NO_MEMORY,
                                 "failed to buffer watch_queue error body");
      return 0U;
    }
    return total;
  }
  for (index = 0U; index < total; ++index) {
    ch = ptr[index];
    if (ch == '\r') {
      continue;
    }
    if (ch == '\n') {
      if (lc_engine_buffer_append_cstr(&state->line_buffer, "") !=
          LC_ENGINE_OK) {
        state->callback_failed = 1;
        lc_engine_set_client_error(state->error, LC_ENGINE_ERROR_NO_MEMORY,
                                   "failed to terminate SSE line buffer");
        return 0U;
      }
      rc = lc_engine_watch_process_line(state);
      state->line_buffer.length = 0U;
      if (state->line_buffer.data != NULL) {
        state->line_buffer.data[0] = '\0';
      }
      if (rc != LC_ENGINE_OK) {
        state->callback_failed = 1;
        return 0U;
      }
    } else {
      if (lc_engine_buffer_append(&state->line_buffer, &ch, 1U) !=
          LC_ENGINE_OK) {
        state->callback_failed = 1;
        lc_engine_set_client_error(state->error, LC_ENGINE_ERROR_NO_MEMORY,
                                   "failed to append SSE line byte");
        return 0U;
      }
    }
  }
  return total;
}

static void lc_engine_watch_state_cleanup(lc_engine_watch_state *state) {
  if (state == NULL) {
    return;
  }
  lc_engine_buffer_cleanup(&state->line_buffer);
  lc_engine_buffer_cleanup(&state->data_buffer);
  lc_engine_buffer_cleanup(&state->error_body);
  lc_engine_free_string(&state->event_name);
  lc_engine_free_string(&state->correlation_id);
}

static size_t lc_engine_subscribe_header_callback(char *buffer, size_t size,
                                                  size_t nitems,
                                                  void *userdata) {
  lc_engine_subscribe_state *state;
  size_t total;
  char *colon;
  char *value;
  char *end;
  char status_line[64];
  size_t status_length;
  long parsed;

  state = (lc_engine_subscribe_state *)userdata;
  total = size * nitems;
  if (total == 0U) {
    return 0U;
  }
  if (total >= 5U && memcmp(buffer, "HTTP/", 5U) == 0) {
    status_length = total;
    if (status_length >= sizeof(status_line)) {
      status_length = sizeof(status_line) - 1U;
    }
    memcpy(status_line, buffer, status_length);
    status_line[status_length] = '\0';
    parsed = 0L;
    sscanf(status_line, "%*s %ld", &parsed);
    state->http_status = parsed;
    return total;
  }
  colon = (char *)memchr(buffer, ':', total);
  if (colon == NULL) {
    return total;
  }
  value = colon + 1;
  end = buffer + total;
  while (value < end && (*value == ' ' || *value == '\t')) {
    ++value;
  }
  while (end > value && (end[-1] == '\r' || end[-1] == '\n' || end[-1] == ' ' ||
                         end[-1] == '\t')) {
    --end;
  }

  if (lc_engine_queue_header_name_equals(buffer, (size_t)(colon - buffer),
                                         "X-Correlation-Id")) {
    if (!lc_engine_queue_set_header_value(&state->correlation_id, value, end)) {
      state->callback_failed = 1;
      return 0U;
    }
  } else if (lc_engine_queue_header_name_equals(
                 buffer, (size_t)(colon - buffer), "Content-Type")) {
    if (!lc_engine_queue_set_header_value(&state->content_type, value, end)) {
      state->callback_failed = 1;
      return 0U;
    }
  }
  return total;
}

static void lc_engine_subscribe_reset_part(lc_engine_subscribe_state *state) {
  lc_engine_free_string(&state->part_name);
  lc_engine_free_string(&state->part_content_type);
  state->part_content_length = -1L;
}

static void
lc_engine_subscribe_state_cleanup(lc_engine_subscribe_state *state) {
  if (state == NULL) {
    return;
  }
  if (state->delivery_active) {
    lc_engine_dequeue_response_cleanup(&state->current);
    state->delivery_active = 0;
  }
  lc_engine_subscribe_reset_part(state);
  lc_engine_buffer_cleanup(&state->line_buffer);
  lc_engine_buffer_cleanup(&state->meta_buffer);
  lc_engine_buffer_cleanup(&state->error_body);
  lc_engine_free_string(&state->correlation_id);
  lc_engine_free_string(&state->content_type);
  lc_engine_free_string(&state->boundary);
  lc_engine_free_string(&state->boundary_line);
  lc_engine_free_string(&state->closing_boundary_line);
}

static int
lc_engine_subscribe_extract_boundary(lc_engine_subscribe_state *state) {
  const char *needle;
  const char *value;
  const char *end;
  size_t length;

  if (state->boundary != NULL) {
    return LC_ENGINE_OK;
  }
  if (state->content_type == NULL || state->content_type[0] == '\0') {
    return lc_engine_set_protocol_error(
        state->error, "subscribe response missing Content-Type");
  }
  if (strncmp(state->content_type, "multipart/", 10) != 0 &&
      strncmp(state->content_type, "Multipart/", 10) != 0) {
    return lc_engine_set_protocol_error(state->error,
                                        "subscribe response is not multipart");
  }
  needle = strstr(state->content_type, "boundary=");
  if (needle == NULL) {
    return lc_engine_set_protocol_error(state->error,
                                        "multipart response missing boundary");
  }
  value = needle + 9;
  if (*value == '"') {
    ++value;
    end = strchr(value, '"');
    if (end == NULL) {
      return lc_engine_set_protocol_error(state->error,
                                          "invalid multipart boundary");
    }
  } else {
    end = value;
    while (*end != '\0' && *end != ';' && *end != ' ' && *end != '\t') {
      ++end;
    }
  }
  length = (size_t)(end - value);
  if (length == 0U) {
    return lc_engine_set_protocol_error(state->error,
                                        "empty multipart boundary");
  }
  state->boundary = lc_engine_strdup_range(value, end);
  if (state->boundary == NULL) {
    return lc_engine_set_client_error(state->error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to allocate multipart boundary");
  }
  state->boundary_line = (char *)malloc(length + 3U);
  if (state->boundary_line == NULL) {
    return lc_engine_set_client_error(
        state->error, LC_ENGINE_ERROR_NO_MEMORY,
        "failed to allocate multipart boundary line");
  }
  snprintf(state->boundary_line, length + 3U, "--%s", state->boundary);
  state->closing_boundary_line = (char *)malloc(length + 5U);
  if (state->closing_boundary_line == NULL) {
    return lc_engine_set_client_error(
        state->error, LC_ENGINE_ERROR_NO_MEMORY,
        "failed to allocate multipart closing boundary line");
  }
  snprintf(state->closing_boundary_line, length + 5U, "--%s--",
           state->boundary);
  return LC_ENGINE_OK;
}

static int lc_engine_subscribe_dup_string(yajl_val root,
                                          const char *const *path,
                                          char **out_value) {
  yajl_val value;

  value = yajl_tree_get(root, (const char **)path, yajl_t_string);
  if (value == NULL) {
    return 1;
  }
  *out_value = lc_engine_strdup_local(YAJL_GET_STRING(value));
  return *out_value != NULL;
}

static int lc_engine_subscribe_parse_meta(lc_engine_subscribe_state *state) {
  int rc;

  if (state->delivery_active) {
    lc_engine_dequeue_response_cleanup(&state->current);
    memset(&state->current, 0, sizeof(state->current));
    state->delivery_active = 0;
  }
  rc = lc_engine_parse_subscribe_meta_json(state->meta_buffer.data,
                                           state->correlation_id,
                                           &state->current, state->error);
  if (rc != LC_ENGINE_OK) {
    lc_engine_dequeue_response_cleanup(&state->current);
    memset(&state->current, 0, sizeof(state->current));
    return rc;
  }
  state->delivery_active = 1;
  state->meta_buffer.length = 0U;
  if (state->meta_buffer.data != NULL) {
    state->meta_buffer.data[0] = '\0';
  }
  return LC_ENGINE_OK;
}

int lc_engine_parse_subscribe_meta_json(const char *json,
                                        const char *fallback_correlation_id,
                                        lc_engine_dequeue_response *response,
                                        lc_engine_error *error) {
  yajl_val root;
  yajl_val message;
  yajl_val value;
  char errbuf[128];
  static const char *path_message[] = {"message", NULL};
  static const char *path_next_cursor[] = {"next_cursor", NULL};
  static const char *path_namespace[] = {"namespace", NULL};
  static const char *path_queue[] = {"queue", NULL};
  static const char *path_message_id[] = {"message_id", NULL};
  static const char *path_attempts[] = {"attempts", NULL};
  static const char *path_max_attempts[] = {"max_attempts", NULL};
  static const char *path_failure_attempts[] = {"failure_attempts", NULL};
  static const char *path_not_visible_until[] = {"not_visible_until_unix",
                                                 NULL};
  static const char *path_visibility_timeout[] = {"visibility_timeout_seconds",
                                                  NULL};
  static const char *path_payload_content_type[] = {"payload_content_type",
                                                    NULL};
  static const char *path_payload_bytes[] = {"payload_bytes", NULL};
  static const char *path_correlation_id[] = {"correlation_id", NULL};
  static const char *path_lease_id[] = {"lease_id", NULL};
  static const char *path_lease_expires[] = {"lease_expires_at_unix", NULL};
  static const char *path_fencing_token[] = {"fencing_token", NULL};
  static const char *path_txn_id[] = {"txn_id", NULL};
  static const char *path_meta_etag[] = {"meta_etag", NULL};
  static const char *path_state_etag[] = {"state_etag", NULL};
  static const char *path_state_lease_id[] = {"state_lease_id", NULL};
  static const char *path_state_lease_expires[] = {
      "state_lease_expires_at_unix", NULL};
  static const char *path_state_fencing_token[] = {"state_fencing_token", NULL};
  static const char *path_state_txn_id[] = {"state_txn_id", NULL};

  if (json == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "parse_subscribe_meta_json requires json, response, and error");
  }

  lc_engine_dequeue_response_cleanup(response);
  errbuf[0] = '\0';
  root = yajl_tree_parse(json, errbuf, sizeof(errbuf));
  if (root == NULL) {
    if (errbuf[0] != '\0') {
      return lc_engine_set_protocol_error(error, errbuf);
    }
    return lc_engine_set_protocol_error(error,
                                        "failed to parse subscribe meta body");
  }
  message = yajl_tree_get(root, path_message, yajl_t_object);
  if (message == NULL) {
    yajl_tree_free(root);
    return lc_engine_set_protocol_error(error,
                                        "subscribe meta body missing message");
  }

  if (!lc_engine_subscribe_dup_string(message, path_namespace,
                                      &response->namespace_name) ||
      !lc_engine_subscribe_dup_string(message, path_queue, &response->queue) ||
      !lc_engine_subscribe_dup_string(message, path_message_id,
                                      &response->message_id) ||
      !lc_engine_subscribe_dup_string(message, path_payload_content_type,
                                      &response->payload_content_type) ||
      !lc_engine_subscribe_dup_string(message, path_correlation_id,
                                      &response->correlation_id) ||
      !lc_engine_subscribe_dup_string(message, path_lease_id,
                                      &response->lease_id) ||
      !lc_engine_subscribe_dup_string(message, path_txn_id,
                                      &response->txn_id) ||
      !lc_engine_subscribe_dup_string(message, path_meta_etag,
                                      &response->meta_etag) ||
      !lc_engine_subscribe_dup_string(message, path_state_etag,
                                      &response->state_etag) ||
      !lc_engine_subscribe_dup_string(message, path_state_lease_id,
                                      &response->state_lease_id) ||
      !lc_engine_subscribe_dup_string(message, path_state_txn_id,
                                      &response->state_txn_id) ||
      !lc_engine_subscribe_dup_string(root, path_next_cursor,
                                      &response->next_cursor)) {
    yajl_tree_free(root);
    lc_engine_dequeue_response_cleanup(response);
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_NO_MEMORY,
        "failed to allocate subscribe delivery metadata");
  }

  value = yajl_tree_get(message, path_attempts, yajl_t_number);
  if (value != NULL) {
    if (!lc_parse_int_base10_checked(YAJL_GET_NUMBER(value),
                                     &response->attempts)) {
      yajl_tree_free(root);
      lc_engine_dequeue_response_cleanup(response);
      return lc_engine_set_protocol_error(error,
                                          "queue attempts is out of range");
    }
  }
  value = yajl_tree_get(message, path_max_attempts, yajl_t_number);
  if (value != NULL) {
    if (!lc_parse_int_base10_checked(YAJL_GET_NUMBER(value),
                                     &response->max_attempts)) {
      yajl_tree_free(root);
      lc_engine_dequeue_response_cleanup(response);
      return lc_engine_set_protocol_error(error,
                                          "queue max_attempts is out of range");
    }
  }
  value = yajl_tree_get(message, path_failure_attempts, yajl_t_number);
  if (value != NULL) {
    if (!lc_parse_int_base10_checked(YAJL_GET_NUMBER(value),
                                     &response->failure_attempts)) {
      yajl_tree_free(root);
      lc_engine_dequeue_response_cleanup(response);
      return lc_engine_set_protocol_error(
          error, "queue failure_attempts is out of range");
    }
  }
  value = yajl_tree_get(message, path_not_visible_until, yajl_t_number);
  if (value != NULL) {
    if (!lc_parse_long_base10_checked(YAJL_GET_NUMBER(value),
                                      &response->not_visible_until_unix)) {
      yajl_tree_free(root);
      lc_engine_dequeue_response_cleanup(response);
      return lc_engine_set_protocol_error(
          error, "queue not_visible_until_unix is out of range");
    }
  }
  value = yajl_tree_get(message, path_visibility_timeout, yajl_t_number);
  if (value != NULL) {
    if (!lc_parse_long_base10_checked(YAJL_GET_NUMBER(value),
                                      &response->visibility_timeout_seconds)) {
      yajl_tree_free(root);
      lc_engine_dequeue_response_cleanup(response);
      return lc_engine_set_protocol_error(
          error, "queue visibility_timeout_seconds is out of range");
    }
  }
  value = yajl_tree_get(message, path_payload_bytes, yajl_t_number);
  if (value != NULL) {
    if (!lc_parse_size_base10_checked(YAJL_GET_NUMBER(value),
                                      &response->payload_length)) {
      yajl_tree_free(root);
      lc_engine_dequeue_response_cleanup(response);
      return lc_engine_set_protocol_error(
          error, "queue payload_bytes is out of range");
    }
  }
  value = yajl_tree_get(message, path_lease_expires, yajl_t_number);
  if (value != NULL) {
    if (!lc_parse_long_base10_checked(YAJL_GET_NUMBER(value),
                                      &response->lease_expires_at_unix)) {
      yajl_tree_free(root);
      lc_engine_dequeue_response_cleanup(response);
      return lc_engine_set_protocol_error(
          error, "queue lease_expires_at_unix is out of range");
    }
  }
  value = yajl_tree_get(message, path_fencing_token, yajl_t_number);
  if (value != NULL) {
    if (!lc_parse_long_base10_checked(YAJL_GET_NUMBER(value),
                                      &response->fencing_token)) {
      yajl_tree_free(root);
      lc_engine_dequeue_response_cleanup(response);
      return lc_engine_set_protocol_error(
          error, "queue fencing_token is out of range");
    }
  }
  value = yajl_tree_get(message, path_state_lease_expires, yajl_t_number);
  if (value != NULL) {
    if (!lc_parse_long_base10_checked(YAJL_GET_NUMBER(value),
                                      &response->state_lease_expires_at_unix)) {
      yajl_tree_free(root);
      lc_engine_dequeue_response_cleanup(response);
      return lc_engine_set_protocol_error(
          error, "queue state_lease_expires_at_unix is out of range");
    }
  }
  value = yajl_tree_get(message, path_state_fencing_token, yajl_t_number);
  if (value != NULL) {
    if (!lc_parse_long_base10_checked(YAJL_GET_NUMBER(value),
                                      &response->state_fencing_token)) {
      yajl_tree_free(root);
      lc_engine_dequeue_response_cleanup(response);
      return lc_engine_set_protocol_error(
          error, "queue state_fencing_token is out of range");
    }
  }
  if (response->correlation_id == NULL && fallback_correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(fallback_correlation_id);
    if (response->correlation_id == NULL) {
      yajl_tree_free(root);
      lc_engine_dequeue_response_cleanup(response);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to allocate subscribe correlation id");
    }
  }

  yajl_tree_free(root);
  return LC_ENGINE_OK;
}

static int lc_engine_subscribe_begin_payload(lc_engine_subscribe_state *state) {
  if (!state->delivery_active) {
    return lc_engine_set_protocol_error(
        state->error, "subscribe payload part arrived before meta");
  }
  if (state->part_content_type != NULL &&
      state->current.payload_content_type == NULL) {
    state->current.payload_content_type =
        lc_engine_strdup_local(state->part_content_type);
    if (state->current.payload_content_type == NULL) {
      return lc_engine_set_client_error(
          state->error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to allocate payload content type");
    }
  }
  state->payload_remaining = state->part_content_length;
  if (state->payload_remaining < 0L) {
    return lc_engine_set_protocol_error(
        state->error, "subscribe payload part missing Content-Length");
  }
  if (state->handler.begin != NULL) {
    if (!state->handler.begin(state->handler_context, &state->current,
                              state->error)) {
      if (state->error->code == LC_ENGINE_OK) {
        return lc_engine_set_transport_error(state->error,
                                             "subscribe begin callback failed");
      }
      return state->error->code;
    }
  }
  if (state->payload_remaining == 0L && state->handler.end != NULL) {
    if (!state->handler.end(state->handler_context, &state->current,
                            state->error)) {
      if (state->error->code == LC_ENGINE_OK) {
        return lc_engine_set_transport_error(state->error,
                                             "subscribe end callback failed");
      }
      return state->error->code;
    }
  }
  if (state->payload_remaining == 0L) {
    lc_engine_dequeue_response_cleanup(&state->current);
    memset(&state->current, 0, sizeof(state->current));
    state->delivery_active = 0;
    state->phase = LC_ENGINE_SUBSCRIBE_EXPECT_BOUNDARY;
  } else {
    state->phase = LC_ENGINE_SUBSCRIBE_READ_PAYLOAD;
  }
  return LC_ENGINE_OK;
}

static int
lc_engine_subscribe_parse_content_disposition(lc_engine_subscribe_state *state,
                                              const char *value) {
  const char *needle;
  const char *begin;
  const char *end;

  needle = strstr(value, "name=");
  if (needle == NULL) {
    return LC_ENGINE_OK;
  }
  begin = needle + 5;
  if (*begin == '"') {
    ++begin;
    end = strchr(begin, '"');
    if (end == NULL) {
      return lc_engine_set_protocol_error(
          state->error, "invalid multipart Content-Disposition name");
    }
  } else {
    end = begin;
    while (*end != '\0' && *end != ';' && *end != ' ' && *end != '\t') {
      ++end;
    }
  }
  lc_engine_free_string(&state->part_name);
  state->part_name = lc_engine_strdup_range(begin, end);
  if (state->part_name == NULL) {
    return lc_engine_set_client_error(state->error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to allocate multipart part name");
  }
  return LC_ENGINE_OK;
}

static int lc_engine_subscribe_process_header(lc_engine_subscribe_state *state,
                                              const char *line) {
  const char *colon;
  const char *name_end;
  const char *value;
  const char *value_end;
  long parsed_length;

  colon = strchr(line, ':');
  if (colon == NULL) {
    return lc_engine_set_protocol_error(state->error,
                                        "invalid multipart header line");
  }
  name_end = colon;
  value = colon + 1;
  while (*value == ' ' || *value == '\t') {
    ++value;
  }
  value_end = value + strlen(value);
  while (value_end > value && (value_end[-1] == ' ' || value_end[-1] == '\t')) {
    --value_end;
  }
  if (lc_engine_queue_header_name_equals(line, (size_t)(name_end - line),
                                         "Content-Disposition")) {
    return lc_engine_subscribe_parse_content_disposition(state, value);
  }
  if (lc_engine_queue_header_name_equals(line, (size_t)(name_end - line),
                                         "Content-Type")) {
    lc_engine_free_string(&state->part_content_type);
    state->part_content_type = lc_engine_strdup_range(value, value_end);
    if (state->part_content_type == NULL) {
      return lc_engine_set_client_error(
          state->error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to allocate multipart content type");
    }
    return LC_ENGINE_OK;
  }
  if (lc_engine_queue_header_name_equals(line, (size_t)(name_end - line),
                                         "Content-Length")) {
    if (lc_parse_long_base10_range_checked(value, (size_t)(value_end - value),
                                           &parsed_length) &&
        parsed_length >= 0L) {
      state->part_content_length = parsed_length;
    } else {
      return lc_engine_set_protocol_error(
          state->error, "multipart content-length is out of range");
    }
    return LC_ENGINE_OK;
  }
  return LC_ENGINE_OK;
}

static int lc_engine_subscribe_process_line(lc_engine_subscribe_state *state) {
  char *line;
  int rc;

  line = state->line_buffer.data;
  if (line == NULL) {
    return LC_ENGINE_OK;
  }
  if (state->phase == LC_ENGINE_SUBSCRIBE_EXPECT_BOUNDARY) {
    if (line[0] == '\0') {
      return LC_ENGINE_OK;
    }
    if (strcmp(line, state->boundary_line) == 0) {
      state->phase = LC_ENGINE_SUBSCRIBE_READ_HEADERS;
      lc_engine_subscribe_reset_part(state);
      return LC_ENGINE_OK;
    }
    if (strcmp(line, state->closing_boundary_line) == 0) {
      state->phase = LC_ENGINE_SUBSCRIBE_DONE;
      return LC_ENGINE_OK;
    }
    return lc_engine_set_protocol_error(state->error,
                                        "unexpected multipart boundary line");
  }
  if (state->phase == LC_ENGINE_SUBSCRIBE_READ_HEADERS) {
    if (line[0] == '\0') {
      if (state->part_name == NULL) {
        return lc_engine_set_protocol_error(state->error,
                                            "multipart part missing name");
      }
      if (strcmp(state->part_name, "meta") == 0) {
        state->meta_buffer.length = 0U;
        if (state->meta_buffer.data != NULL) {
          state->meta_buffer.data[0] = '\0';
        }
        state->phase = LC_ENGINE_SUBSCRIBE_READ_META;
        return LC_ENGINE_OK;
      }
      if (strcmp(state->part_name, "payload") == 0) {
        rc = lc_engine_subscribe_begin_payload(state);
        if (rc != LC_ENGINE_OK) {
          return rc;
        }
        return LC_ENGINE_OK;
      }
      return lc_engine_set_protocol_error(state->error,
                                          "unexpected multipart part name");
    }
    return lc_engine_subscribe_process_header(state, line);
  }
  if (state->phase == LC_ENGINE_SUBSCRIBE_READ_META) {
    if (strcmp(line, state->boundary_line) == 0 ||
        strcmp(line, state->closing_boundary_line) == 0) {
      rc = lc_engine_subscribe_parse_meta(state);
      if (rc != LC_ENGINE_OK) {
        return rc;
      }
      lc_engine_subscribe_reset_part(state);
      if (strcmp(line, state->closing_boundary_line) == 0) {
        state->phase = LC_ENGINE_SUBSCRIBE_DONE;
        return LC_ENGINE_OK;
      }
      state->phase = LC_ENGINE_SUBSCRIBE_READ_HEADERS;
      return LC_ENGINE_OK;
    }
    if (lc_engine_buffer_append_cstr(&state->meta_buffer, line) !=
            LC_ENGINE_OK ||
        lc_engine_buffer_append_cstr(&state->meta_buffer, "\n") !=
            LC_ENGINE_OK) {
      return lc_engine_set_client_error(state->error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to append subscribe meta line");
    }
    return LC_ENGINE_OK;
  }
  return LC_ENGINE_OK;
}

static size_t lc_engine_subscribe_write_callback(char *ptr, size_t size,
                                                 size_t nmemb, void *userdata) {
  lc_engine_subscribe_state *state;
  size_t total;
  size_t offset;
  size_t chunk;
  int rc;
  char ch;

  state = (lc_engine_subscribe_state *)userdata;
  total = size * nmemb;
  if (state->http_status >= 400L) {
    if (lc_engine_buffer_append(&state->error_body, ptr, total) !=
        LC_ENGINE_OK) {
      state->callback_failed = 1;
      lc_engine_set_client_error(state->error, LC_ENGINE_ERROR_NO_MEMORY,
                                 "failed to buffer subscribe error body");
      return 0U;
    }
    return total;
  }
  rc = lc_engine_subscribe_extract_boundary(state);
  if (rc != LC_ENGINE_OK) {
    state->callback_failed = 1;
    return 0U;
  }

  offset = 0U;
  while (offset < total) {
    if (state->phase == LC_ENGINE_SUBSCRIBE_DONE) {
      break;
    }
    if (state->phase == LC_ENGINE_SUBSCRIBE_READ_PAYLOAD) {
      chunk = total - offset;
      if ((long)chunk > state->payload_remaining) {
        chunk = (size_t)state->payload_remaining;
      }
      if (chunk > 0U && state->handler.chunk != NULL) {
        if (!state->handler.chunk(state->handler_context, ptr + offset, chunk,
                                  state->error)) {
          state->callback_failed = 1;
          if (state->error->code == LC_ENGINE_OK) {
            lc_engine_set_transport_error(state->error,
                                          "subscribe payload callback failed");
          }
          return 0U;
        }
      }
      offset += chunk;
      state->payload_remaining -= (long)chunk;
      if (state->payload_remaining == 0L) {
        if (state->handler.end != NULL) {
          if (!state->handler.end(state->handler_context, &state->current,
                                  state->error)) {
            state->callback_failed = 1;
            if (state->error->code == LC_ENGINE_OK) {
              lc_engine_set_transport_error(state->error,
                                            "subscribe end callback failed");
            }
            return 0U;
          }
        }
        lc_engine_dequeue_response_cleanup(&state->current);
        memset(&state->current, 0, sizeof(state->current));
        state->delivery_active = 0;
        state->phase = LC_ENGINE_SUBSCRIBE_EXPECT_BOUNDARY;
      }
      continue;
    }

    ch = ptr[offset++];
    if (ch == '\r') {
      continue;
    }
    if (ch == '\n') {
      if (lc_engine_buffer_append_cstr(&state->line_buffer, "") !=
          LC_ENGINE_OK) {
        state->callback_failed = 1;
        lc_engine_set_client_error(state->error, LC_ENGINE_ERROR_NO_MEMORY,
                                   "failed to terminate multipart line buffer");
        return 0U;
      }
      rc = lc_engine_subscribe_process_line(state);
      state->line_buffer.length = 0U;
      if (state->line_buffer.data != NULL) {
        state->line_buffer.data[0] = '\0';
      }
      if (rc != LC_ENGINE_OK) {
        state->callback_failed = 1;
        return 0U;
      }
    } else {
      if (lc_engine_buffer_append(&state->line_buffer, &ch, 1U) !=
          LC_ENGINE_OK) {
        state->callback_failed = 1;
        lc_engine_set_client_error(state->error, LC_ENGINE_ERROR_NO_MEMORY,
                                   "failed to append multipart line byte");
        return 0U;
      }
    }
  }
  return total;
}

int lc_engine_client_watch_queue(lc_engine_client *client,
                                 const lc_engine_watch_queue_request *request,
                                 lc_engine_queue_watch_handler handler,
                                 void *handler_context,
                                 lc_engine_error *error) {
  lc_engine_buffer body;
  const char *namespace_name;
  struct curl_slist *headers;
  size_t endpoint_index;
  int first_field;

  if (client == NULL || request == NULL || handler == NULL || error == NULL ||
      request->queue == NULL || request->queue[0] == '\0') {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "watch_queue requires client, request, handler, error, and queue");
  }

  namespace_name =
      lc_engine_effective_namespace(client, request->namespace_name);
  lc_engine_buffer_init(&body);
  if (lc_engine_json_begin_object(&body) != LC_ENGINE_OK) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_NO_MEMORY,
        "failed to allocate watch_queue request body");
  }
  first_field = 1;
  if (namespace_name != NULL && namespace_name[0] != '\0' &&
      lc_engine_json_add_string_field(&body, &first_field, "namespace",
                                      namespace_name) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add watch_queue namespace");
  }
  if (lc_engine_json_add_string_field(&body, &first_field, "queue",
                                      request->queue) != LC_ENGINE_OK ||
      lc_engine_json_end_object(&body) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_NO_MEMORY,
        "failed to build watch_queue request body");
  }

  headers = NULL;
  headers = curl_slist_append(headers, "Content-Type: application/json");
  headers = curl_slist_append(headers, "Accept: text/event-stream");
  if (headers == NULL) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to allocate watch_queue headers");
  }

  for (endpoint_index = 0U; endpoint_index < client->endpoint_count;
       ++endpoint_index) {
    CURL *curl;
    CURLcode curl_rc;
    lc_engine_watch_state state;
    char *url;
    size_t url_length;
    int rc;
    lc_engine_http_result synthetic;

    memset(&state, 0, sizeof(state));
    state.handler = handler;
    state.handler_context = handler_context;
    state.error = error;
    lc_engine_buffer_init(&state.line_buffer);
    lc_engine_buffer_init(&state.data_buffer);
    lc_engine_buffer_init(&state.error_body);

    url_length = strlen(client->endpoints[endpoint_index]) +
                 strlen("/v1/queue/watch") + 1U;
    url = (char *)malloc(url_length);
    if (url == NULL) {
      curl_slist_free_all(headers);
      lc_engine_buffer_cleanup(&body);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to allocate watch_queue URL");
    }
    snprintf(url, url_length, "%s/v1/queue/watch",
             client->endpoints[endpoint_index]);

    curl = curl_easy_init();
    if (curl == NULL) {
      free(url);
      curl_slist_free_all(headers);
      lc_engine_buffer_cleanup(&body);
      return lc_engine_set_transport_error(
          error, "failed to initialize curl for watch_queue");
    }
    lc_engine_queue_stream_log_attempt(client, "/v1/queue/watch",
                                       endpoint_index);
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.data);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE,
                     (curl_off_t)body.length);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION,
                     lc_engine_watch_write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &state);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION,
                     lc_engine_watch_header_callback);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &state);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    if (client->timeout_ms > 0L) {
      curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, client->timeout_ms);
    }
    if (client->unix_socket_path != NULL &&
        client->unix_socket_path[0] != '\0') {
      curl_easy_setopt(curl, CURLOPT_UNIX_SOCKET_PATH,
                       client->unix_socket_path);
    }
    if (client->prefer_http_2) {
      curl_easy_setopt(curl, CURLOPT_HTTP_VERSION,
                       (long)CURL_HTTP_VERSION_2TLS);
    } else {
      curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, (long)CURL_HTTP_VERSION_1_1);
    }
    if (!client->disable_mtls) {
      curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER,
                       client->insecure_skip_verify ? 0L : 1L);
      curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST,
                       client->insecure_skip_verify ? 0L : 2L);
      curl_easy_setopt(curl, CURLOPT_SSL_CTX_FUNCTION,
                       lc_engine_queue_stream_ssl_ctx);
      curl_easy_setopt(curl, CURLOPT_SSL_CTX_DATA, client);
    }

    curl_rc = curl_easy_perform(curl);
    free(url);
    curl_easy_cleanup(curl);

    if (curl_rc == CURLE_WRITE_ERROR && state.callback_failed) {
      rc = error->code;
      lc_engine_watch_state_cleanup(&state);
      curl_slist_free_all(headers);
      lc_engine_buffer_cleanup(&body);
      return rc;
    }
    if (curl_rc != CURLE_OK) {
      lc_engine_queue_stream_log_error(client, "/v1/queue/watch",
                                       endpoint_index,
                                       curl_easy_strerror(curl_rc));
      rc = lc_engine_set_transport_error(error, curl_easy_strerror(curl_rc));
      lc_engine_watch_state_cleanup(&state);
      curl_slist_free_all(headers);
      lc_engine_buffer_cleanup(&body);
      return rc;
    }
    if (state.http_status >= 200L && state.http_status < 300L) {
      lc_engine_queue_stream_log_success(client, "/v1/queue/watch",
                                         endpoint_index, state.http_status,
                                         state.correlation_id);
      lc_engine_watch_state_cleanup(&state);
      break;
    }

    memset(&synthetic, 0, sizeof(synthetic));
    synthetic.http_status = state.http_status;
    synthetic.body = state.error_body;
    synthetic.correlation_id = state.correlation_id;
    lc_engine_queue_stream_log_error(client, "/v1/queue/watch", endpoint_index,
                                     "server returned error status");
    rc = lc_engine_set_server_error_from_result(error, &synthetic);
    lc_engine_watch_state_cleanup(&state);
    if (error->server_error_code == NULL ||
        strcmp(error->server_error_code, "node_passive") != 0) {
      curl_slist_free_all(headers);
      lc_engine_buffer_cleanup(&body);
      return rc;
    }
    lc_engine_error_reset(error);
  }

  curl_slist_free_all(headers);
  lc_engine_buffer_cleanup(&body);
  return LC_ENGINE_OK;
}

static int lc_engine_client_subscribe_internal(
    lc_engine_client *client, const lc_engine_dequeue_request *request,
    const lc_engine_queue_stream_handler *handler, void *handler_context,
    const char *path, lc_engine_error *error) {
  lc_engine_buffer body;
  const char *namespace_name;
  struct curl_slist *headers;
  int first_field;
  long wait_seconds;
  int page_size;

  if (client == NULL || request == NULL || handler == NULL ||
      handler->chunk == NULL || error == NULL || request->queue == NULL ||
      request->queue[0] == '\0' || request->owner == NULL ||
      request->owner[0] == '\0') {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "subscribe requires client, request, "
                                      "chunk handler, error, queue, and owner");
  }

  namespace_name =
      lc_engine_effective_namespace(client, request->namespace_name);
  wait_seconds = request->wait_seconds;
  if (wait_seconds < 0L) {
    wait_seconds = -1L;
  }
  page_size = request->page_size;
  if (page_size <= 0) {
    page_size = 1;
  }

  lc_engine_buffer_init(&body);
  if (lc_engine_json_begin_object(&body) != LC_ENGINE_OK) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_NO_MEMORY,
        "failed to allocate subscribe request body");
  }
  first_field = 1;
  if (namespace_name != NULL && namespace_name[0] != '\0' &&
      lc_engine_json_add_string_field(&body, &first_field, "namespace",
                                      namespace_name) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add subscribe namespace");
  }
  if (lc_engine_json_add_string_field(&body, &first_field, "queue",
                                      request->queue) != LC_ENGINE_OK ||
      lc_engine_json_add_string_field(&body, &first_field, "owner",
                                      request->owner) != LC_ENGINE_OK ||
      lc_engine_json_add_long_field(
          &body, &first_field, "visibility_timeout_seconds",
          request->visibility_timeout_seconds) != LC_ENGINE_OK ||
      lc_engine_json_add_long_field(&body, &first_field, "wait_seconds",
                                    wait_seconds) != LC_ENGINE_OK ||
      lc_engine_json_add_long_field(&body, &first_field, "page_size",
                                    (long)page_size) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to build subscribe request body");
  }
  if (request->start_after != NULL && request->start_after[0] != '\0' &&
      lc_engine_json_add_string_field(&body, &first_field, "start_after",
                                      request->start_after) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add subscribe start_after");
  }
  if (lc_engine_json_end_object(&body) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_NO_MEMORY,
        "failed to finish subscribe request body");
  }

  headers = NULL;
  headers = curl_slist_append(headers, "Content-Type: application/json");
  headers = curl_slist_append(headers, "Accept: multipart/related");
  if (headers == NULL) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to allocate subscribe headers");
  }

  for (;;) {
    size_t endpoint_index;
    int retryable;

    retryable = 0;
    for (endpoint_index = 0U; endpoint_index < client->endpoint_count;
         ++endpoint_index) {
      CURL *curl;
      CURLcode curl_rc;
      lc_engine_subscribe_state state;
      char *url;
      size_t url_length;
      int rc;
      lc_engine_http_result synthetic;

      memset(&state, 0, sizeof(state));
      state.handler = *handler;
      state.handler_context = handler_context;
      state.error = error;
      state.part_content_length = -1L;
      state.phase = LC_ENGINE_SUBSCRIBE_EXPECT_BOUNDARY;
      lc_engine_buffer_init(&state.line_buffer);
      lc_engine_buffer_init(&state.meta_buffer);
      lc_engine_buffer_init(&state.error_body);

      url_length =
          strlen(client->endpoints[endpoint_index]) + strlen(path) + 1U;
      url = (char *)malloc(url_length);
      if (url == NULL) {
        curl_slist_free_all(headers);
        lc_engine_buffer_cleanup(&body);
        return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                          "failed to allocate subscribe URL");
      }
      snprintf(url, url_length, "%s%s", client->endpoints[endpoint_index],
               path);

      curl = curl_easy_init();
      if (curl == NULL) {
        free(url);
        curl_slist_free_all(headers);
        lc_engine_buffer_cleanup(&body);
        return lc_engine_set_transport_error(
            error, "failed to initialize curl for subscribe");
      }
      lc_engine_queue_stream_log_attempt(client, path, endpoint_index);
      curl_easy_setopt(curl, CURLOPT_URL, url);
      curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
      curl_easy_setopt(curl, CURLOPT_POST, 1L);
      curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.data);
      curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE,
                       (curl_off_t)body.length);
      curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION,
                       lc_engine_subscribe_write_callback);
      curl_easy_setopt(curl, CURLOPT_WRITEDATA, &state);
      curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION,
                       lc_engine_subscribe_header_callback);
      curl_easy_setopt(curl, CURLOPT_HEADERDATA, &state);
      curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
      curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
      curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION,
                       lc_engine_queue_stream_progress);
      curl_easy_setopt(curl, CURLOPT_XFERINFODATA, client);
      if (client->timeout_ms > 0L) {
        curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, client->timeout_ms);
      }
      if (client->unix_socket_path != NULL &&
          client->unix_socket_path[0] != '\0') {
        curl_easy_setopt(curl, CURLOPT_UNIX_SOCKET_PATH,
                         client->unix_socket_path);
      }
      if (client->prefer_http_2) {
        curl_easy_setopt(curl, CURLOPT_HTTP_VERSION,
                         (long)CURL_HTTP_VERSION_2TLS);
      } else {
        curl_easy_setopt(curl, CURLOPT_HTTP_VERSION,
                         (long)CURL_HTTP_VERSION_1_1);
      }
      if (!client->disable_mtls) {
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER,
                         client->insecure_skip_verify ? 0L : 1L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST,
                         client->insecure_skip_verify ? 0L : 2L);
        curl_easy_setopt(curl, CURLOPT_SSL_CTX_FUNCTION,
                         lc_engine_queue_stream_ssl_ctx);
        curl_easy_setopt(curl, CURLOPT_SSL_CTX_DATA, client);
      }

      curl_rc = curl_easy_perform(curl);
      free(url);
      curl_easy_cleanup(curl);

      if (curl_rc == CURLE_ABORTED_BY_CALLBACK &&
          client->cancel_check != NULL &&
          client->cancel_check(client->cancel_context)) {
        lc_engine_subscribe_state_cleanup(&state);
        curl_slist_free_all(headers);
        lc_engine_buffer_cleanup(&body);
        return LC_ENGINE_OK;
      }
      if (curl_rc == CURLE_WRITE_ERROR && state.callback_failed) {
        lc_engine_queue_stream_log_error(client, path, endpoint_index,
                                         error != NULL && error->message != NULL
                                             ? error->message
                                             : "stream callback failed");
        rc = error->code;
        lc_engine_subscribe_state_cleanup(&state);
        curl_slist_free_all(headers);
        lc_engine_buffer_cleanup(&body);
        return rc;
      }
      if (curl_rc != CURLE_OK) {
        lc_engine_queue_stream_log_error(client, path, endpoint_index,
                                         curl_easy_strerror(curl_rc));
        rc = lc_engine_set_transport_error(error, curl_easy_strerror(curl_rc));
        lc_engine_subscribe_state_cleanup(&state);
        curl_slist_free_all(headers);
        lc_engine_buffer_cleanup(&body);
        return rc;
      }
      if (state.http_status >= 200L && state.http_status < 300L) {
        lc_engine_queue_stream_log_success(client, path, endpoint_index,
                                           state.http_status,
                                           state.correlation_id);
        lc_engine_subscribe_state_cleanup(&state);
        curl_slist_free_all(headers);
        lc_engine_buffer_cleanup(&body);
        return LC_ENGINE_OK;
      }

      memset(&synthetic, 0, sizeof(synthetic));
      synthetic.http_status = state.http_status;
      synthetic.body = state.error_body;
      synthetic.correlation_id = state.correlation_id;
      lc_engine_queue_stream_log_error(client, path, endpoint_index,
                                       "server returned error status");
      rc = lc_engine_set_server_error_from_result(error, &synthetic);
      lc_engine_subscribe_state_cleanup(&state);
      if (error->server_error_code != NULL &&
          (strcmp(error->server_error_code, "waiting") == 0 ||
           strcmp(error->server_error_code, "cas_mismatch") == 0)) {
        retryable = 1;
        lc_engine_error_reset(error);
        break;
      }
      if (error->server_error_code != NULL &&
          strcmp(error->server_error_code, "node_passive") == 0) {
        lc_engine_error_reset(error);
        continue;
      }
      curl_slist_free_all(headers);
      lc_engine_buffer_cleanup(&body);
      return rc;
    }

    if (!retryable) {
      break;
    }
    {
      struct timespec delay;

      delay.tv_sec = 0;
      delay.tv_nsec = 500000L;
      nanosleep(&delay, NULL);
    }
  }

  curl_slist_free_all(headers);
  lc_engine_buffer_cleanup(&body);
  return lc_engine_set_transport_error(
      error, "all endpoints rejected the subscribe request");
}

int lc_engine_client_subscribe(lc_engine_client *client,
                               const lc_engine_dequeue_request *request,
                               const lc_engine_queue_stream_handler *handler,
                               void *handler_context, lc_engine_error *error) {
  return lc_engine_client_subscribe_internal(
      client, request, handler, handler_context, "/v1/queue/subscribe", error);
}

int lc_engine_client_subscribe_with_state(
    lc_engine_client *client, const lc_engine_dequeue_request *request,
    const lc_engine_queue_stream_handler *handler, void *handler_context,
    lc_engine_error *error) {
  return lc_engine_client_subscribe_internal(
      client, request, handler, handler_context, "/v1/queue/subscribeWithState",
      error);
}

int lc_engine_client_dequeue_into(lc_engine_client *client,
                                  const lc_engine_dequeue_request *request,
                                  const lc_engine_queue_stream_handler *handler,
                                  void *handler_context,
                                  lc_engine_error *error) {
  return lc_engine_client_subscribe_internal(
      client, request, handler, handler_context, "/v1/queue/dequeue", error);
}

int lc_engine_client_dequeue_with_state_into(
    lc_engine_client *client, const lc_engine_dequeue_request *request,
    const lc_engine_queue_stream_handler *handler, void *handler_context,
    lc_engine_error *error) {
  return lc_engine_client_subscribe_internal(
      client, request, handler, handler_context, "/v1/queue/dequeueWithState",
      error);
}

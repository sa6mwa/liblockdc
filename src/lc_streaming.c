#include "lc_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <curl/curl.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

#define LC_ENGINE_STREAM_ERROR_BODY_LIMIT (8U * 1024U)

typedef struct lc_engine_stream_query_state {
  lc_engine_client *client;
  lc_engine_write_callback writer;
  void *writer_context;
  lc_engine_query_stream_response *response;
  lc_engine_error *error;
  lc_engine_buffer error_body;
  long http_status;
  int saw_status;
  int write_failed;
  int header_failed;
} lc_engine_stream_query_state;

static CURLcode lc_engine_stream_ssl_ctx(CURL *curl, void *ssl_ctx,
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
  if (client->tls_bundle.client_cert != NULL) {
    if (SSL_CTX_use_certificate(ctx, client->tls_bundle.client_cert) != 1) {
      return CURLE_SSL_CERTPROBLEM;
    }
  }
  if (client->tls_bundle.client_key != NULL) {
    if (SSL_CTX_use_PrivateKey(ctx, client->tls_bundle.client_key) != 1) {
      return CURLE_SSL_CERTPROBLEM;
    }
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

static int lc_engine_stream_capture_range(lc_engine_client *client, char **slot,
                                          const char *begin, const char *end) {
  char *copy;

  if (begin == NULL || end == NULL || end < begin) {
    return 1;
  }
  copy = lc_engine_client_strdup_range(client, begin, end);
  if (copy == NULL) {
    return 0;
  }
  lc_engine_client_free_alloc(client, *slot);
  *slot = copy;
  return 1;
}

static size_t lc_engine_stream_header_callback(char *buffer, size_t size,
                                               size_t nitems, void *userdata) {
  lc_engine_stream_query_state *state;
  size_t total;
  char *colon;
  char *value;
  char *end;
  long parsed;
  char status_line[64];
  size_t status_length;

  state = (lc_engine_stream_query_state *)userdata;
  total = size * nitems;
  if (total == 0U) {
    return 0U;
  }
  if (total >= 5U && memcmp(buffer, "HTTP/", 5U) == 0) {
    parsed = 0L;
    status_length = total;
    if (status_length >= sizeof(status_line)) {
      status_length = sizeof(status_line) - 1U;
    }
    memcpy(status_line, buffer, status_length);
    status_line[status_length] = '\0';
    sscanf(status_line, "%*s %ld", &parsed);
    state->http_status = parsed;
    state->saw_status = 1;
    if (state->response != NULL) {
      state->response->http_status = parsed;
    }
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

  if ((size_t)(colon - buffer) == 20U &&
      strncmp(buffer, "X-Lockd-Query-Cursor", 20U) == 0) {
    if (!lc_engine_stream_capture_range(state->client, &state->response->cursor,
                                        value, end)) {
      lc_engine_set_client_error(state->error, LC_ENGINE_ERROR_NO_MEMORY,
                                 "failed to capture query cursor");
      state->header_failed = 1;
      return 0U;
    }
  } else if ((size_t)(colon - buffer) == 23U &&
             strncmp(buffer, "X-Lockd-Query-Index-Seq", 23U) == 0) {
    if (!lc_parse_ulong_base10_range_checked(value, (size_t)(end - value),
                                             &state->response->index_seq)) {
      lc_engine_set_protocol_error(state->error,
                                   "failed to parse query index sequence");
      state->header_failed = 1;
      return 0U;
    }
  } else if ((size_t)(colon - buffer) == 22U &&
             strncmp(buffer, "X-Lockd-Query-Metadata", 22U) == 0) {
    if (!lc_engine_stream_capture_range(
            state->client, &state->response->metadata_json, value, end)) {
      lc_engine_set_client_error(state->error, LC_ENGINE_ERROR_NO_MEMORY,
                                 "failed to capture query metadata");
      state->header_failed = 1;
      return 0U;
    }
  } else if ((size_t)(colon - buffer) == 20U &&
             strncmp(buffer, "X-Lockd-Query-Return", 20U) == 0) {
    if (!lc_engine_stream_capture_range(
            state->client, &state->response->return_mode, value, end)) {
      lc_engine_set_client_error(state->error, LC_ENGINE_ERROR_NO_MEMORY,
                                 "failed to capture query return mode");
      state->header_failed = 1;
      return 0U;
    }
  } else if ((size_t)(colon - buffer) == 16U &&
             strncmp(buffer, "X-Correlation-Id", 16U) == 0) {
    if (!lc_engine_stream_capture_range(
            state->client, &state->response->correlation_id, value, end)) {
      lc_engine_set_client_error(state->error, LC_ENGINE_ERROR_NO_MEMORY,
                                 "failed to capture correlation id");
      state->header_failed = 1;
      return 0U;
    }
  }

  return total;
}

static size_t lc_engine_stream_write_callback(char *ptr, size_t size,
                                              size_t nmemb, void *userdata) {
  lc_engine_stream_query_state *state;
  size_t total;
  int ok;

  state = (lc_engine_stream_query_state *)userdata;
  total = size * nmemb;
  if (total == 0U) {
    return 0U;
  }
  if (state->saw_status && state->http_status >= 400L) {
    if (lc_engine_buffer_append_limited(&state->error_body, ptr, total,
                                         LC_ENGINE_STREAM_ERROR_BODY_LIMIT) !=
        LC_ENGINE_OK) {
      lc_engine_set_client_error(state->error, LC_ENGINE_ERROR_NO_MEMORY,
                                 "failed to buffer error response");
      state->write_failed = 1;
      return 0U;
    }
    return total;
  }
  ok = state->writer(state->writer_context, ptr, total, state->error);
  if (!ok) {
    if (state->error->code == LC_ENGINE_OK) {
      lc_engine_set_transport_error(state->error,
                                    "query writer callback failed");
    }
    state->write_failed = 1;
    return 0U;
  }
  return total;
}

static int lc_engine_stream_build_query_body(
    lc_engine_buffer *body, lc_engine_client *client,
    const lc_engine_query_request *request, lc_engine_error *error) {
  int first_field;
  const char *effective_namespace;

  lc_engine_buffer_init(body);
  if (lc_engine_json_begin_object(body) != LC_ENGINE_OK) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to allocate query body");
  }
  first_field = 1;
  effective_namespace =
      lc_engine_effective_namespace(client, request->namespace_name);
  if (effective_namespace != NULL && effective_namespace[0] != '\0' &&
      lc_engine_json_add_string_field(body, &first_field, "namespace",
                                      effective_namespace) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add query namespace");
  }
  if (request->selector_json != NULL && request->selector_json[0] != '\0' &&
      lc_engine_json_add_raw_field(body, &first_field, "selector",
                                   request->selector_json) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add query selector");
  }
  if (request->limit > 0L &&
      lc_engine_json_add_long_field(body, &first_field, "limit",
                                    request->limit) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add query limit");
  }
  if (request->cursor != NULL && request->cursor[0] != '\0' &&
      lc_engine_json_add_string_field(body, &first_field, "cursor",
                                      request->cursor) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add query cursor");
  }
  if (request->fields_json != NULL && request->fields_json[0] != '\0' &&
      lc_engine_json_add_raw_field(body, &first_field, "fields",
                                   request->fields_json) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add query fields");
  }
  if (request->return_mode != NULL && request->return_mode[0] != '\0' &&
      lc_engine_json_add_string_field(body, &first_field, "return",
                                      request->return_mode) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add query return");
  }
  if (lc_engine_json_end_object(body) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_protocol_error(error, "failed to close query body");
  }
  return LC_ENGINE_OK;
}

static int lc_engine_stream_perform_query(lc_engine_client *client,
                                          const char *url,
                                          const lc_engine_buffer *body,
                                          lc_engine_stream_query_state *state) {
  CURL *curl;
  CURLcode curl_rc;
  struct curl_slist *headers;

  curl = curl_easy_init();
  if (curl == NULL) {
    return lc_engine_set_transport_error(state->error,
                                         "failed to initialize curl");
  }

  headers = NULL;
  headers = curl_slist_append(headers, "Content-Type: application/json");
  headers = curl_slist_append(headers,
                              "Accept: application/x-ndjson, application/json");

  curl_easy_setopt(curl, CURLOPT_URL, url);
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_POST, 1L);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body->data);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE, (curl_off_t)body->length);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION,
                   lc_engine_stream_write_callback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, state);
  curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION,
                   lc_engine_stream_header_callback);
  curl_easy_setopt(curl, CURLOPT_HEADERDATA, state);
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
  if (client->timeout_ms > 0L) {
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, client->timeout_ms);
  }
  if (client->unix_socket_path != NULL && client->unix_socket_path[0] != '\0') {
    curl_easy_setopt(curl, CURLOPT_UNIX_SOCKET_PATH, client->unix_socket_path);
  }
  if (client->prefer_http_2) {
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, (long)CURL_HTTP_VERSION_2TLS);
  } else {
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, (long)CURL_HTTP_VERSION_1_1);
  }
  if (!client->disable_mtls) {
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER,
                     client->insecure_skip_verify ? 0L : 1L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST,
                     client->insecure_skip_verify ? 0L : 2L);
    curl_easy_setopt(curl, CURLOPT_SSL_CTX_FUNCTION, lc_engine_stream_ssl_ctx);
    curl_easy_setopt(curl, CURLOPT_SSL_CTX_DATA, client);
  }

  curl_rc = curl_easy_perform(curl);
  if (state->header_failed && state->error->code != LC_ENGINE_OK) {
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    return state->error->code;
  }
  if (curl_rc == CURLE_WRITE_ERROR &&
      (state->write_failed || state->header_failed)) {
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    return state->error->code;
  }
  if (curl_rc != CURLE_OK) {
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    return lc_engine_set_transport_error(state->error,
                                         curl_easy_strerror(curl_rc));
  }
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &state->http_status);
  state->response->http_status = state->http_status;
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);
  if (state->header_failed && state->error->code != LC_ENGINE_OK) {
    return state->error->code;
  }
  return LC_ENGINE_OK;
}

int lc_engine_client_query_into(lc_engine_client *client,
                                const lc_engine_query_request *request,
                                lc_engine_write_callback writer,
                                void *writer_context,
                                lc_engine_query_stream_response *response,
                                lc_engine_error *error) {
  lc_engine_buffer body;
  lc_engine_stream_query_state state;
  char *url;
  size_t endpoint_index;
  int rc;

  if (client == NULL || request == NULL || writer == NULL || response == NULL ||
      error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "query_into requires client, request, writer, response, and error");
  }
  if (request->selector_json == NULL || request->selector_json[0] == '\0') {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "query_into requires selector_json");
  }

  memset(response, 0, sizeof(*response));
  memset(&state, 0, sizeof(state));
  state.client = client;
  state.writer = writer;
  state.writer_context = writer_context;
  state.response = response;
  state.error = error;
  lc_engine_buffer_init(&state.error_body);

  rc = lc_engine_stream_build_query_body(&body, client, request, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }

  for (endpoint_index = 0U; endpoint_index < client->endpoint_count;
       ++endpoint_index) {
    size_t url_length;

    state.http_status = 0L;
    state.saw_status = 0;
    state.write_failed = 0;
    state.header_failed = 0;
    state.error_body.length = 0U;
    if (state.error_body.data != NULL) {
      state.error_body.data[0] = '\0';
    }
    url_length =
        strlen(client->endpoints[endpoint_index]) + strlen("/v1/query") + 1U;
    url = (char *)malloc(url_length);
    if (url == NULL) {
      lc_engine_buffer_cleanup(&body);
      lc_engine_buffer_cleanup(&state.error_body);
      lc_engine_query_stream_response_cleanup(client, response);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to allocate query URL");
    }
    snprintf(url, url_length, "%s/v1/query", client->endpoints[endpoint_index]);
    rc = lc_engine_stream_perform_query(client, url, &body, &state);
    free(url);
    if (rc != LC_ENGINE_OK) {
      lc_engine_buffer_cleanup(&body);
      lc_engine_buffer_cleanup(&state.error_body);
      lc_engine_query_stream_response_cleanup(client, response);
      return rc;
    }
    if (state.http_status >= 200L && state.http_status < 300L) {
      lc_engine_buffer_cleanup(&body);
      lc_engine_buffer_cleanup(&state.error_body);
      return LC_ENGINE_OK;
    }

    rc = lc_engine_set_server_error_from_json(error, state.http_status,
                                              response->correlation_id,
                                              state.error_body.data);
    if (error->server_error_code == NULL ||
        strcmp(error->server_error_code, "node_passive") != 0) {
      lc_engine_buffer_cleanup(&body);
      lc_engine_buffer_cleanup(&state.error_body);
      lc_engine_query_stream_response_cleanup(client, response);
      return rc;
    }
    lc_engine_query_stream_response_cleanup(client, response);
    lc_engine_error_reset(error);
  }

  lc_engine_buffer_cleanup(&body);
  lc_engine_buffer_cleanup(&state.error_body);
  lc_engine_query_stream_response_cleanup(client, response);
  return lc_engine_set_transport_error(
      error, "all endpoints rejected the streaming query request");
}

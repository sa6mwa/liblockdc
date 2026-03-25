#include "lc_internal.h"

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <yajl/yajl_gen.h>
#include <yajl/yajl_parse.h>

typedef enum lc_engine_queue_parse_kind {
  LC_ENGINE_QUEUE_PARSE_ACK = 1,
  LC_ENGINE_QUEUE_PARSE_NACK = 2,
  LC_ENGINE_QUEUE_PARSE_EXTEND = 3,
  LC_ENGINE_QUEUE_PARSE_STATS = 4
} lc_engine_queue_parse_kind;

typedef struct lc_engine_queue_parse_context {
  lc_engine_queue_parse_kind kind;
  char current_key[64];
  size_t current_key_length;
  void *response;
} lc_engine_queue_parse_context;

static int lc_engine_yajl_status_ok(yajl_gen_status status) {
  return status == yajl_gen_status_ok;
}

static int lc_engine_yajl_write_key(yajl_gen gen, const char *key) {
  return lc_engine_yajl_status_ok(
      yajl_gen_string(gen, (const unsigned char *)key, strlen(key)));
}

static int lc_engine_yajl_write_string_field(yajl_gen gen, const char *key,
                                             const char *value) {
  if (!lc_engine_yajl_write_key(gen, key)) {
    return 0;
  }
  return lc_engine_yajl_status_ok(
      yajl_gen_string(gen, (const unsigned char *)value, strlen(value)));
}

static int lc_engine_yajl_write_long_field(yajl_gen gen, const char *key,
                                           long value) {
  char scratch[32];
  int written;

  if (!lc_engine_yajl_write_key(gen, key)) {
    return 0;
  }
  written = snprintf(scratch, sizeof(scratch), "%ld", value);
  if (written < 0 || (size_t)written >= sizeof(scratch)) {
    return 0;
  }
  return lc_engine_yajl_status_ok(
      yajl_gen_number(gen, scratch, (size_t)written));
}

static int lc_engine_yajl_copy_to_buffer(yajl_gen gen, lc_engine_buffer *buffer,
                                         lc_engine_error *error) {
  const unsigned char *data;
  size_t length;

  yajl_gen_get_buf(gen, &data, &length);
  if (lc_engine_buffer_append(buffer, (const char *)data, length) !=
      LC_ENGINE_OK) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to allocate JSON request body");
  }
  return LC_ENGINE_OK;
}

static int lc_engine_key_equals(const lc_engine_queue_parse_context *context,
                                const char *value) {
  size_t expected_length;

  expected_length = strlen(value);
  if (context->current_key_length != expected_length) {
    return 0;
  }
  return memcmp(context->current_key, value, expected_length) == 0;
}

static int lc_engine_assign_json_string(char **out_value,
                                        const unsigned char *value,
                                        size_t length) {
  char *copy;

  copy = (char *)malloc(length + 1U);
  if (copy == NULL) {
    return 0;
  }
  if (length > 0U) {
    memcpy(copy, value, length);
  }
  copy[length] = '\0';
  lc_engine_free_string(out_value);
  *out_value = copy;
  return 1;
}

static int lc_engine_queue_parse_null(void *ctx) {
  (void)ctx;
  return 1;
}

static int lc_engine_queue_parse_boolean(void *ctx, int value) {
  lc_engine_queue_parse_context *context;

  context = (lc_engine_queue_parse_context *)ctx;
  if (context->kind == LC_ENGINE_QUEUE_PARSE_ACK) {
    lc_engine_queue_ack_response *response;

    response = (lc_engine_queue_ack_response *)context->response;
    if (lc_engine_key_equals(context, "acked")) {
      response->acked = value;
    }
  } else if (context->kind == LC_ENGINE_QUEUE_PARSE_NACK) {
    lc_engine_queue_nack_response *response;

    response = (lc_engine_queue_nack_response *)context->response;
    if (lc_engine_key_equals(context, "requeued")) {
      response->requeued = value;
    }
  } else if (context->kind == LC_ENGINE_QUEUE_PARSE_STATS) {
    lc_engine_queue_stats_response *response;

    response = (lc_engine_queue_stats_response *)context->response;
    if (lc_engine_key_equals(context, "has_active_watcher")) {
      response->has_active_watcher = value;
    } else if (lc_engine_key_equals(context, "available")) {
      response->available = value;
    }
  }
  return 1;
}

static int lc_engine_queue_parse_long_value(const unsigned char *value,
                                            size_t length, long *out_value) {
  char buffer[32];

  if (length == 0U || length >= sizeof(buffer) || out_value == NULL) {
    return 0;
  }
  memcpy(buffer, value, length);
  buffer[length] = '\0';
  return lc_parse_long_base10_checked(buffer, out_value);
}

static int lc_engine_queue_parse_number(void *ctx, const char *value,
                                        size_t length) {
  lc_engine_queue_parse_context *context;
  long parsed;

  context = (lc_engine_queue_parse_context *)ctx;
  if (!lc_engine_queue_parse_long_value((const unsigned char *)value, length,
                                        &parsed)) {
    return 0;
  }
  if (context->kind == LC_ENGINE_QUEUE_PARSE_EXTEND) {
    lc_engine_queue_extend_response *response;

    response = (lc_engine_queue_extend_response *)context->response;
    if (lc_engine_key_equals(context, "lease_expires_at_unix")) {
      response->lease_expires_at_unix = parsed;
    } else if (lc_engine_key_equals(context, "visibility_timeout_seconds")) {
      response->visibility_timeout_seconds = parsed;
    } else if (lc_engine_key_equals(context, "state_lease_expires_at_unix")) {
      response->state_lease_expires_at_unix = parsed;
    }
  } else if (context->kind == LC_ENGINE_QUEUE_PARSE_STATS) {
    lc_engine_queue_stats_response *response;

    response = (lc_engine_queue_stats_response *)context->response;
    if (lc_engine_key_equals(context, "waiting_consumers")) {
      if (parsed < (long)INT_MIN || parsed > (long)INT_MAX) {
        return 0;
      }
      response->waiting_consumers = (int)parsed;
    } else if (lc_engine_key_equals(context, "pending_candidates")) {
      if (parsed < (long)INT_MIN || parsed > (long)INT_MAX) {
        return 0;
      }
      response->pending_candidates = (int)parsed;
    } else if (lc_engine_key_equals(context, "total_consumers")) {
      if (parsed < (long)INT_MIN || parsed > (long)INT_MAX) {
        return 0;
      }
      response->total_consumers = (int)parsed;
    } else if (lc_engine_key_equals(context, "head_enqueued_at_unix")) {
      response->head_enqueued_at_unix = parsed;
    } else if (lc_engine_key_equals(context, "head_not_visible_until_unix")) {
      response->head_not_visible_until_unix = parsed;
    } else if (lc_engine_key_equals(context, "head_age_seconds")) {
      response->head_age_seconds = parsed;
    }
  }
  return 1;
}

static int lc_engine_queue_parse_string(void *ctx, const unsigned char *value,
                                        size_t length) {
  lc_engine_queue_parse_context *context;

  context = (lc_engine_queue_parse_context *)ctx;
  if (context->kind == LC_ENGINE_QUEUE_PARSE_ACK) {
    lc_engine_queue_ack_response *response;

    response = (lc_engine_queue_ack_response *)context->response;
    if (lc_engine_key_equals(context, "correlation_id")) {
      return lc_engine_assign_json_string(&response->correlation_id, value,
                                          length);
    }
  } else if (context->kind == LC_ENGINE_QUEUE_PARSE_NACK) {
    lc_engine_queue_nack_response *response;

    response = (lc_engine_queue_nack_response *)context->response;
    if (lc_engine_key_equals(context, "meta_etag")) {
      return lc_engine_assign_json_string(&response->meta_etag, value, length);
    }
    if (lc_engine_key_equals(context, "correlation_id")) {
      return lc_engine_assign_json_string(&response->correlation_id, value,
                                          length);
    }
  } else if (context->kind == LC_ENGINE_QUEUE_PARSE_EXTEND) {
    lc_engine_queue_extend_response *response;

    response = (lc_engine_queue_extend_response *)context->response;
    if (lc_engine_key_equals(context, "meta_etag")) {
      return lc_engine_assign_json_string(&response->meta_etag, value, length);
    }
    if (lc_engine_key_equals(context, "correlation_id")) {
      return lc_engine_assign_json_string(&response->correlation_id, value,
                                          length);
    }
  } else if (context->kind == LC_ENGINE_QUEUE_PARSE_STATS) {
    lc_engine_queue_stats_response *response;

    response = (lc_engine_queue_stats_response *)context->response;
    if (lc_engine_key_equals(context, "namespace")) {
      return lc_engine_assign_json_string(&response->namespace_name, value,
                                          length);
    }
    if (lc_engine_key_equals(context, "queue")) {
      return lc_engine_assign_json_string(&response->queue, value, length);
    }
    if (lc_engine_key_equals(context, "head_message_id")) {
      return lc_engine_assign_json_string(&response->head_message_id, value,
                                          length);
    }
    if (lc_engine_key_equals(context, "correlation_id")) {
      return lc_engine_assign_json_string(&response->correlation_id, value,
                                          length);
    }
  }
  return 1;
}

static int lc_engine_queue_parse_map_key(void *ctx, const unsigned char *value,
                                         size_t length) {
  lc_engine_queue_parse_context *context;
  size_t copy_length;

  context = (lc_engine_queue_parse_context *)ctx;
  copy_length = length;
  if (copy_length >= sizeof(context->current_key)) {
    copy_length = sizeof(context->current_key) - 1U;
  }
  if (copy_length > 0U) {
    memcpy(context->current_key, value, copy_length);
  }
  context->current_key[copy_length] = '\0';
  context->current_key_length = copy_length;
  return 1;
}

static yajl_callbacks lc_engine_queue_parse_callbacks = {
    lc_engine_queue_parse_null,
    lc_engine_queue_parse_boolean,
    0,
    0,
    lc_engine_queue_parse_number,
    lc_engine_queue_parse_string,
    0,
    lc_engine_queue_parse_map_key,
    0,
    0,
    0};

static int
lc_engine_queue_parse_response_json(const lc_engine_http_result *result,
                                    lc_engine_queue_parse_kind kind,
                                    void *response, lc_engine_error *error) {
  yajl_handle parser;
  yajl_status status;
  lc_engine_queue_parse_context context;
  unsigned char *message;

  memset(&context, 0, sizeof(context));
  context.kind = kind;
  context.response = response;

  parser = yajl_alloc(&lc_engine_queue_parse_callbacks, 0, (void *)&context);
  if (parser == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to allocate YAJL parser");
  }

  status = yajl_parse(parser, (const unsigned char *)result->body.data,
                      result->body.length);
  if (status == yajl_status_ok) {
    status = yajl_complete_parse(parser);
  }
  if (status != yajl_status_ok) {
    message =
        yajl_get_error(parser, 1, (const unsigned char *)result->body.data,
                       result->body.length);
    if (message != NULL) {
      lc_engine_set_protocol_error(error, (const char *)message);
      yajl_free_error(parser, message);
    } else {
      lc_engine_set_protocol_error(error, "failed to parse JSON response");
    }
    yajl_free(parser);
    return error->code;
  }

  yajl_free(parser);
  return LC_ENGINE_OK;
}

static int
lc_engine_queue_apply_correlation(char **out_correlation_id,
                                  const lc_engine_http_result *result) {
  if (*out_correlation_id == NULL && result->correlation_id != NULL) {
    *out_correlation_id = lc_engine_strdup_local(result->correlation_id);
    if (*out_correlation_id == NULL) {
      return 0;
    }
  }
  return 1;
}

static int lc_engine_queue_start_request(yajl_gen *out_gen,
                                         lc_engine_buffer *out_buffer,
                                         lc_engine_error *error) {
  yajl_gen gen = NULL;

  lc_engine_buffer_init(out_buffer);
  gen = yajl_gen_alloc(0);
  if (gen == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to allocate YAJL generator");
  }
  yajl_gen_config(gen, yajl_gen_beautify, 0);
  if (!lc_engine_yajl_status_ok(yajl_gen_map_open(gen))) {
    yajl_gen_free(gen);
    return lc_engine_set_protocol_error(error, "failed to start JSON object");
  }
  *out_gen = gen;
  return LC_ENGINE_OK;
}

static int lc_engine_queue_finish_request(yajl_gen gen,
                                          lc_engine_buffer *buffer,
                                          lc_engine_error *error) {
  int rc;

  if (!lc_engine_yajl_status_ok(yajl_gen_map_close(gen))) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(buffer);
    return lc_engine_set_protocol_error(error, "failed to finish JSON object");
  }
  rc = lc_engine_yajl_copy_to_buffer(gen, buffer, error);
  yajl_gen_free(gen);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(buffer);
  }
  return rc;
}

static int lc_engine_queue_add_namespace(yajl_gen gen, lc_engine_client *client,
                                         const char *namespace_name) {
  const char *effective_namespace;

  effective_namespace = lc_engine_effective_namespace(client, namespace_name);
  if (effective_namespace != NULL && effective_namespace[0] != '\0') {
    return lc_engine_yajl_write_string_field(gen, "namespace",
                                             effective_namespace);
  }
  return 1;
}

static int
lc_engine_queue_request_headers(const lc_engine_header_pair **out_headers,
                                size_t *out_header_count) {
  static const lc_engine_header_pair headers[] = {
      {"Content-Type", "application/json"}, {"Accept", "application/json"}};

  *out_headers = headers;
  *out_header_count = sizeof(headers) / sizeof(headers[0]);
  return 1;
}

static int lc_engine_build_queue_nack_body(
    lc_engine_client *client, const lc_engine_queue_nack_request *request,
    lc_engine_buffer *body, lc_engine_error *error) {
  int first_field;
  const char *effective_namespace;

  lc_engine_buffer_init(body);
  if (lc_engine_json_begin_object(body) != LC_ENGINE_OK) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_NO_MEMORY,
        "failed to allocate queue_nack request body");
  }
  first_field = 1;
  effective_namespace =
      lc_engine_effective_namespace(client, request->namespace_name);
  if (effective_namespace != NULL && effective_namespace[0] != '\0' &&
      lc_engine_json_add_string_field(body, &first_field, "namespace",
                                      effective_namespace) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add queue_nack namespace");
  }
  if (lc_engine_json_add_string_field(body, &first_field, "queue",
                                      request->queue) != LC_ENGINE_OK ||
      lc_engine_json_add_string_field(body, &first_field, "message_id",
                                      request->message_id) != LC_ENGINE_OK ||
      lc_engine_json_add_string_field(body, &first_field, "lease_id",
                                      request->lease_id) != LC_ENGINE_OK ||
      lc_engine_json_add_string_field(body, &first_field, "meta_etag",
                                      request->meta_etag) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to build queue_nack request");
  }
  if (request->txn_id != NULL && request->txn_id[0] != '\0' &&
      lc_engine_json_add_string_field(body, &first_field, "txn_id",
                                      request->txn_id) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add queue_nack txn_id");
  }
  if (request->fencing_token > 0L &&
      lc_engine_json_add_long_field(body, &first_field, "fencing_token",
                                    request->fencing_token) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add queue_nack fencing_token");
  }
  if (request->state_etag != NULL && request->state_etag[0] != '\0' &&
      lc_engine_json_add_string_field(body, &first_field, "state_etag",
                                      request->state_etag) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add queue_nack state_etag");
  }
  if (request->delay_seconds > 0L &&
      lc_engine_json_add_long_field(body, &first_field, "delay_seconds",
                                    request->delay_seconds) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add queue_nack delay_seconds");
  }
  if (request->intent != NULL && request->intent[0] != '\0' &&
      lc_engine_json_add_string_field(body, &first_field, "intent",
                                      request->intent) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add queue_nack intent");
  }
  if (request->last_error_json != NULL && request->last_error_json[0] != '\0' &&
      lc_engine_json_add_raw_field(body, &first_field, "last_error",
                                   request->last_error_json) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add queue_nack last_error");
  }
  if (request->state_lease_id != NULL && request->state_lease_id[0] != '\0') {
    if (lc_engine_json_add_string_field(body, &first_field, "state_lease_id",
                                        request->state_lease_id) !=
        LC_ENGINE_OK) {
      lc_engine_buffer_cleanup(body);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to add queue_nack state_lease_id");
    }
    if (request->state_fencing_token > 0L &&
        lc_engine_json_add_long_field(body, &first_field, "state_fencing_token",
                                      request->state_fencing_token) !=
            LC_ENGINE_OK) {
      lc_engine_buffer_cleanup(body);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to add queue_nack state_fencing_token");
    }
  }
  if (lc_engine_json_end_object(body) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_protocol_error(
        error, "failed to close queue_nack request body");
  }
  return LC_ENGINE_OK;
}

void lc_engine_queue_stats_response_cleanup(
    lc_engine_queue_stats_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->namespace_name);
  lc_engine_free_string(&response->queue);
  lc_engine_free_string(&response->head_message_id);
  lc_engine_free_string(&response->correlation_id);
}

void lc_engine_queue_ack_response_cleanup(
    lc_engine_queue_ack_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->correlation_id);
}

void lc_engine_queue_nack_response_cleanup(
    lc_engine_queue_nack_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->meta_etag);
  lc_engine_free_string(&response->correlation_id);
}

void lc_engine_queue_extend_response_cleanup(
    lc_engine_queue_extend_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->meta_etag);
  lc_engine_free_string(&response->correlation_id);
}

int lc_engine_client_queue_stats(lc_engine_client *client,
                                 const lc_engine_queue_stats_request *request,
                                 lc_engine_queue_stats_response *response,
                                 lc_engine_error *error) {
  yajl_gen gen = NULL;
  lc_engine_buffer body;
  lc_engine_http_result result;
  const lc_engine_header_pair *headers;
  size_t header_count;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "queue_stats requires client, request, response, and error");
  }
  if (request->queue == NULL || request->queue[0] == '\0') {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "queue_stats requires queue");
  }

  memset(&result, 0, sizeof(result));
  rc = lc_engine_queue_start_request(&gen, &body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (!lc_engine_queue_add_namespace(gen, client, request->namespace_name) ||
      !lc_engine_yajl_write_string_field(gen, "queue", request->queue)) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to build queue_stats request");
  }
  rc = lc_engine_queue_finish_request(gen, &body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }

  lc_engine_queue_request_headers(&headers, &header_count);
  rc = lc_engine_http_request(client, "POST", "/v1/queue/stats", body.data,
                              body.length, headers, header_count, &result,
                              error);
  lc_engine_buffer_cleanup(&body);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status < 200 || result.http_status >= 300) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }
  rc = lc_engine_queue_parse_response_json(&result, LC_ENGINE_QUEUE_PARSE_STATS,
                                           response, error);
  if (rc == LC_ENGINE_OK &&
      !lc_engine_queue_apply_correlation(&response->correlation_id, &result)) {
    rc =
        lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                   "failed to copy queue_stats correlation_id");
  }
  lc_engine_http_result_cleanup(&result);
  return rc;
}

int lc_engine_client_queue_ack(lc_engine_client *client,
                               const lc_engine_queue_ack_request *request,
                               lc_engine_queue_ack_response *response,
                               lc_engine_error *error) {
  yajl_gen gen = NULL;
  lc_engine_buffer body;
  lc_engine_http_result result;
  const lc_engine_header_pair *headers;
  size_t header_count;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "queue_ack requires client, request, response, and error");
  }
  if (request->queue == NULL || request->message_id == NULL ||
      request->lease_id == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "queue_ack requires queue, message_id, and lease_id");
  }

  memset(&result, 0, sizeof(result));
  rc = lc_engine_queue_start_request(&gen, &body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (!lc_engine_queue_add_namespace(gen, client, request->namespace_name) ||
      !lc_engine_yajl_write_string_field(gen, "queue", request->queue) ||
      !lc_engine_yajl_write_string_field(gen, "message_id",
                                         request->message_id) ||
      !lc_engine_yajl_write_string_field(gen, "lease_id", request->lease_id)) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to build queue_ack request");
  }
  if (request->txn_id != NULL && request->txn_id[0] != '\0' &&
      !lc_engine_yajl_write_string_field(gen, "txn_id", request->txn_id)) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add queue_ack txn_id");
  }
  if (request->fencing_token > 0L &&
      !lc_engine_yajl_write_long_field(gen, "fencing_token",
                                       request->fencing_token)) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add queue_ack fencing_token");
  }
  if (request->meta_etag != NULL && request->meta_etag[0] != '\0' &&
      !lc_engine_yajl_write_string_field(gen, "meta_etag",
                                         request->meta_etag)) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add queue_ack meta_etag");
  }
  if (request->state_etag != NULL && request->state_etag[0] != '\0' &&
      !lc_engine_yajl_write_string_field(gen, "state_etag",
                                         request->state_etag)) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add queue_ack state_etag");
  }
  if (request->state_lease_id != NULL && request->state_lease_id[0] != '\0') {
    if (!lc_engine_yajl_write_string_field(gen, "state_lease_id",
                                           request->state_lease_id)) {
      yajl_gen_free(gen);
      lc_engine_buffer_cleanup(&body);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to add queue_ack state_lease_id");
    }
    if (request->state_fencing_token > 0L &&
        !lc_engine_yajl_write_long_field(gen, "state_fencing_token",
                                         request->state_fencing_token)) {
      yajl_gen_free(gen);
      lc_engine_buffer_cleanup(&body);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to add queue_ack state_fencing_token");
    }
  }
  rc = lc_engine_queue_finish_request(gen, &body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }

  lc_engine_queue_request_headers(&headers, &header_count);
  rc = lc_engine_http_request(client, "POST", "/v1/queue/ack", body.data,
                              body.length, headers, header_count, &result,
                              error);
  lc_engine_buffer_cleanup(&body);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status < 200 || result.http_status >= 300) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }
  rc = lc_engine_queue_parse_response_json(&result, LC_ENGINE_QUEUE_PARSE_ACK,
                                           response, error);
  if (rc == LC_ENGINE_OK &&
      !lc_engine_queue_apply_correlation(&response->correlation_id, &result)) {
    rc = lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                    "failed to copy queue_ack correlation_id");
  }
  lc_engine_http_result_cleanup(&result);
  return rc;
}

int lc_engine_client_queue_nack(lc_engine_client *client,
                                const lc_engine_queue_nack_request *request,
                                lc_engine_queue_nack_response *response,
                                lc_engine_error *error) {
  lc_engine_buffer body;
  lc_engine_http_result result;
  const lc_engine_header_pair *headers;
  size_t header_count;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "queue_nack requires client, request, response, and error");
  }
  if (request->queue == NULL || request->message_id == NULL ||
      request->lease_id == NULL || request->meta_etag == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "queue_nack requires queue, message_id, lease_id, and meta_etag");
  }

  memset(&result, 0, sizeof(result));
  rc = lc_engine_build_queue_nack_body(client, request, &body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }

  lc_engine_queue_request_headers(&headers, &header_count);
  rc = lc_engine_http_request(client, "POST", "/v1/queue/nack", body.data,
                              body.length, headers, header_count, &result,
                              error);
  lc_engine_buffer_cleanup(&body);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status < 200 || result.http_status >= 300) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }
  rc = lc_engine_queue_parse_response_json(&result, LC_ENGINE_QUEUE_PARSE_NACK,
                                           response, error);
  if (rc == LC_ENGINE_OK &&
      !lc_engine_queue_apply_correlation(&response->correlation_id, &result)) {
    rc = lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                    "failed to copy queue_nack correlation_id");
  }
  lc_engine_http_result_cleanup(&result);
  return rc;
}

int lc_engine_client_queue_extend(lc_engine_client *client,
                                  const lc_engine_queue_extend_request *request,
                                  lc_engine_queue_extend_response *response,
                                  lc_engine_error *error) {
  yajl_gen gen = NULL;
  lc_engine_buffer body;
  lc_engine_http_result result;
  const lc_engine_header_pair *headers;
  size_t header_count;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "queue_extend requires client, request, response, and error");
  }
  if (request->queue == NULL || request->message_id == NULL ||
      request->lease_id == NULL || request->meta_etag == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "queue_extend requires queue, message_id, lease_id, and meta_etag");
  }

  memset(&result, 0, sizeof(result));
  rc = lc_engine_queue_start_request(&gen, &body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (!lc_engine_queue_add_namespace(gen, client, request->namespace_name) ||
      !lc_engine_yajl_write_string_field(gen, "queue", request->queue) ||
      !lc_engine_yajl_write_string_field(gen, "message_id",
                                         request->message_id) ||
      !lc_engine_yajl_write_string_field(gen, "lease_id", request->lease_id) ||
      !lc_engine_yajl_write_string_field(gen, "meta_etag",
                                         request->meta_etag)) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to build queue_extend request");
  }
  if (request->txn_id != NULL && request->txn_id[0] != '\0' &&
      !lc_engine_yajl_write_string_field(gen, "txn_id", request->txn_id)) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add queue_extend txn_id");
  }
  if (request->fencing_token > 0L &&
      !lc_engine_yajl_write_long_field(gen, "fencing_token",
                                       request->fencing_token)) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_NO_MEMORY,
        "failed to add queue_extend fencing_token");
  }
  if (request->extend_by_seconds > 0L &&
      !lc_engine_yajl_write_long_field(gen, "extend_by_seconds",
                                       request->extend_by_seconds)) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_NO_MEMORY,
        "failed to add queue_extend extend_by_seconds");
  }
  if (request->state_lease_id != NULL && request->state_lease_id[0] != '\0') {
    if (!lc_engine_yajl_write_string_field(gen, "state_lease_id",
                                           request->state_lease_id)) {
      yajl_gen_free(gen);
      lc_engine_buffer_cleanup(&body);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to add queue_extend state_lease_id");
    }
    if (request->state_fencing_token > 0L &&
        !lc_engine_yajl_write_long_field(gen, "state_fencing_token",
                                         request->state_fencing_token)) {
      yajl_gen_free(gen);
      lc_engine_buffer_cleanup(&body);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to add queue_extend state_fencing_token");
    }
  }
  rc = lc_engine_queue_finish_request(gen, &body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }

  lc_engine_queue_request_headers(&headers, &header_count);
  rc = lc_engine_http_request(client, "POST", "/v1/queue/extend", body.data,
                              body.length, headers, header_count, &result,
                              error);
  lc_engine_buffer_cleanup(&body);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status < 200 || result.http_status >= 300) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }
  rc = lc_engine_queue_parse_response_json(
      &result, LC_ENGINE_QUEUE_PARSE_EXTEND, response, error);
  if (rc == LC_ENGINE_OK &&
      !lc_engine_queue_apply_correlation(&response->correlation_id, &result)) {
    rc = lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_NO_MEMORY,
        "failed to copy queue_extend correlation_id");
  }
  lc_engine_http_result_cleanup(&result);
  return rc;
}

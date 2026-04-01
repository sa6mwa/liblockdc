#include "lc_internal.h"

#include <limits.h>
#include <string.h>

typedef enum lc_engine_queue_parse_kind {
  LC_ENGINE_QUEUE_PARSE_ACK = 1,
  LC_ENGINE_QUEUE_PARSE_NACK = 2,
  LC_ENGINE_QUEUE_PARSE_EXTEND = 3,
  LC_ENGINE_QUEUE_PARSE_STATS = 4
} lc_engine_queue_parse_kind;

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

static int lc_engine_queue_request_headers(
    const lc_engine_header_pair **out_headers, size_t *out_header_count) {
  static const lc_engine_header_pair headers[] = {
      {"Content-Type", "application/json"}, {"Accept", "application/json"}};

  *out_headers = headers;
  *out_header_count = sizeof(headers) / sizeof(headers[0]);
  return 1;
}

static int lc_engine_queue_add_namespace(lc_engine_buffer *body,
                                         int *first_field,
                                         lc_engine_client *client,
                                         const char *namespace_name,
                                         const char *label,
                                         lc_engine_error *error) {
  const char *effective_namespace;

  effective_namespace = lc_engine_effective_namespace(client, namespace_name);
  if (effective_namespace != NULL && effective_namespace[0] != '\0' &&
      lc_engine_json_add_string_field(body, first_field, "namespace",
                                      effective_namespace) != LC_ENGINE_OK) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY, label);
  }
  return LC_ENGINE_OK;
}

static int lc_engine_queue_start_request(lc_engine_buffer *body,
                                         lc_engine_error *error) {
  lc_engine_buffer_init(body);
  if (lc_engine_json_begin_object(body) != LC_ENGINE_OK) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to allocate JSON request body");
  }
  return LC_ENGINE_OK;
}

static int lc_engine_queue_finish_request(lc_engine_buffer *body,
                                          lc_engine_error *error) {
  if (lc_engine_json_end_object(body) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_protocol_error(error, "failed to finish JSON object");
  }
  return LC_ENGINE_OK;
}

static int
lc_engine_queue_parse_response_json(const lc_engine_http_result *result,
                                    lc_engine_queue_parse_kind kind,
                                    void *response, lc_engine_error *error) {
  int rc;
  long value;
  int bool_value;

  if (result == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "queue response parsing requires result, response, and error");
  }

  switch (kind) {
  case LC_ENGINE_QUEUE_PARSE_ACK: {
    lc_engine_queue_ack_response *ack_response;

    ack_response = (lc_engine_queue_ack_response *)response;
    rc = lc_engine_json_get_bool(result->body.data, "acked", &ack_response->acked);
    if (rc != LC_ENGINE_OK) {
      return lc_engine_set_protocol_error(error,
                                          "failed to parse queue_ack response");
    }
    break;
  }
  case LC_ENGINE_QUEUE_PARSE_NACK: {
    lc_engine_queue_nack_response *nack_response;

    nack_response = (lc_engine_queue_nack_response *)response;
    rc = lc_engine_json_get_bool(result->body.data, "requeued",
                                 &nack_response->requeued);
    if (rc != LC_ENGINE_OK) {
      return lc_engine_set_protocol_error(
          error, "failed to parse queue_nack requeued");
    }
    rc = lc_engine_json_get_string(result->body.data, "meta_etag",
                                   &nack_response->meta_etag);
    if (rc != LC_ENGINE_OK) {
      return lc_engine_set_protocol_error(error,
                                          "failed to parse queue_nack meta_etag");
    }
    break;
  }
  case LC_ENGINE_QUEUE_PARSE_EXTEND: {
    lc_engine_queue_extend_response *extend_response;

    extend_response = (lc_engine_queue_extend_response *)response;
    rc = lc_engine_json_get_long(result->body.data, "lease_expires_at_unix",
                                 &extend_response->lease_expires_at_unix);
    if (rc != LC_ENGINE_OK) {
      return lc_engine_set_protocol_error(
          error, "failed to parse queue_extend lease_expires_at_unix");
    }
    rc = lc_engine_json_get_long(result->body.data, "visibility_timeout_seconds",
                                 &extend_response->visibility_timeout_seconds);
    if (rc != LC_ENGINE_OK) {
      return lc_engine_set_protocol_error(
          error, "failed to parse queue_extend visibility_timeout_seconds");
    }
    rc = lc_engine_json_get_string(result->body.data, "meta_etag",
                                   &extend_response->meta_etag);
    if (rc != LC_ENGINE_OK) {
      return lc_engine_set_protocol_error(error,
                                          "failed to parse queue_extend meta_etag");
    }
    rc = lc_engine_json_get_long(result->body.data,
                                 "state_lease_expires_at_unix",
                                 &extend_response->state_lease_expires_at_unix);
    if (rc != LC_ENGINE_OK) {
      return lc_engine_set_protocol_error(
          error, "failed to parse queue_extend state_lease_expires_at_unix");
    }
    break;
  }
  case LC_ENGINE_QUEUE_PARSE_STATS: {
    lc_engine_queue_stats_response *stats_response;

    stats_response = (lc_engine_queue_stats_response *)response;
    rc = lc_engine_json_get_string(result->body.data, "namespace",
                                   &stats_response->namespace_name);
    if (rc != LC_ENGINE_OK) {
      return lc_engine_set_protocol_error(error,
                                          "failed to parse queue_stats namespace");
    }
    rc = lc_engine_json_get_string(result->body.data, "queue",
                                   &stats_response->queue);
    if (rc != LC_ENGINE_OK) {
      return lc_engine_set_protocol_error(error,
                                          "failed to parse queue_stats queue");
    }
    rc = lc_engine_json_get_long(result->body.data, "waiting_consumers", &value);
    if (rc != LC_ENGINE_OK || value < (long)INT_MIN || value > (long)INT_MAX) {
      return lc_engine_set_protocol_error(
          error, "queue_stats waiting_consumers is out of range");
    }
    stats_response->waiting_consumers = (int)value;
    rc = lc_engine_json_get_long(result->body.data, "pending_candidates", &value);
    if (rc != LC_ENGINE_OK || value < (long)INT_MIN || value > (long)INT_MAX) {
      return lc_engine_set_protocol_error(
          error, "queue_stats pending_candidates is out of range");
    }
    stats_response->pending_candidates = (int)value;
    rc = lc_engine_json_get_long(result->body.data, "total_consumers", &value);
    if (rc != LC_ENGINE_OK || value < (long)INT_MIN || value > (long)INT_MAX) {
      return lc_engine_set_protocol_error(
          error, "queue_stats total_consumers is out of range");
    }
    stats_response->total_consumers = (int)value;
    rc = lc_engine_json_get_bool(result->body.data, "has_active_watcher",
                                 &bool_value);
    if (rc != LC_ENGINE_OK) {
      return lc_engine_set_protocol_error(
          error, "failed to parse queue_stats has_active_watcher");
    }
    stats_response->has_active_watcher = bool_value;
    rc = lc_engine_json_get_bool(result->body.data, "available", &bool_value);
    if (rc != LC_ENGINE_OK) {
      return lc_engine_set_protocol_error(error,
                                          "failed to parse queue_stats available");
    }
    stats_response->available = bool_value;
    rc = lc_engine_json_get_string(result->body.data, "head_message_id",
                                   &stats_response->head_message_id);
    if (rc != LC_ENGINE_OK) {
      return lc_engine_set_protocol_error(
          error, "failed to parse queue_stats head_message_id");
    }
    rc = lc_engine_json_get_long(result->body.data, "head_enqueued_at_unix",
                                 &stats_response->head_enqueued_at_unix);
    if (rc != LC_ENGINE_OK) {
      return lc_engine_set_protocol_error(
          error, "failed to parse queue_stats head_enqueued_at_unix");
    }
    rc = lc_engine_json_get_long(result->body.data,
                                 "head_not_visible_until_unix",
                                 &stats_response->head_not_visible_until_unix);
    if (rc != LC_ENGINE_OK) {
      return lc_engine_set_protocol_error(
          error, "failed to parse queue_stats head_not_visible_until_unix");
    }
    rc = lc_engine_json_get_long(result->body.data, "head_age_seconds",
                                 &stats_response->head_age_seconds);
    if (rc != LC_ENGINE_OK) {
      return lc_engine_set_protocol_error(error,
                                          "failed to parse queue_stats head_age_seconds");
    }
    break;
  }
  default:
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "unknown queue parse kind");
  }

  return LC_ENGINE_OK;
}

static int lc_engine_build_queue_stats_body(lc_engine_client *client,
                                            const char *namespace_name,
                                            const char *queue,
                                            lc_engine_buffer *body,
                                            lc_engine_error *error) {
  int first_field;
  int rc;

  rc = lc_engine_queue_start_request(body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  first_field = 1;
  rc = lc_engine_queue_add_namespace(body, &first_field, client, namespace_name,
                                     "failed to add queue_stats namespace",
                                     error);
  if (rc != LC_ENGINE_OK ||
      lc_engine_json_add_string_field(body, &first_field, "queue", queue) !=
          LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    if (rc != LC_ENGINE_OK) {
      return rc;
    }
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to build queue_stats request");
  }
  return lc_engine_queue_finish_request(body, error);
}

static int
lc_engine_build_queue_ack_body(lc_engine_client *client,
                               const lc_engine_queue_ack_request *request,
                               lc_engine_buffer *body,
                               lc_engine_error *error) {
  int first_field;
  int rc;

  rc = lc_engine_queue_start_request(body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  first_field = 1;
  rc = lc_engine_queue_add_namespace(body, &first_field, client,
                                     request->namespace_name,
                                     "failed to add queue_ack namespace",
                                     error);
  if (rc != LC_ENGINE_OK ||
      lc_engine_json_add_string_field(body, &first_field, "queue",
                                      request->queue) != LC_ENGINE_OK ||
      lc_engine_json_add_string_field(body, &first_field, "message_id",
                                      request->message_id) != LC_ENGINE_OK ||
      lc_engine_json_add_string_field(body, &first_field, "lease_id",
                                      request->lease_id) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    if (rc != LC_ENGINE_OK) {
      return rc;
    }
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to build queue_ack request");
  }
  if (request->txn_id != NULL && request->txn_id[0] != '\0' &&
      lc_engine_json_add_string_field(body, &first_field, "txn_id",
                                      request->txn_id) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add queue_ack txn_id");
  }
  if (request->fencing_token > 0L &&
      lc_engine_json_add_long_field(body, &first_field, "fencing_token",
                                    request->fencing_token) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add queue_ack fencing_token");
  }
  if (request->meta_etag != NULL && request->meta_etag[0] != '\0' &&
      lc_engine_json_add_string_field(body, &first_field, "meta_etag",
                                      request->meta_etag) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add queue_ack meta_etag");
  }
  if (request->state_etag != NULL && request->state_etag[0] != '\0' &&
      lc_engine_json_add_string_field(body, &first_field, "state_etag",
                                      request->state_etag) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add queue_ack state_etag");
  }
  if (request->state_lease_id != NULL && request->state_lease_id[0] != '\0') {
    if (lc_engine_json_add_string_field(body, &first_field, "state_lease_id",
                                        request->state_lease_id) !=
        LC_ENGINE_OK) {
      lc_engine_buffer_cleanup(body);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to add queue_ack state_lease_id");
    }
    if (request->state_fencing_token > 0L &&
        lc_engine_json_add_long_field(body, &first_field, "state_fencing_token",
                                      request->state_fencing_token) !=
            LC_ENGINE_OK) {
      lc_engine_buffer_cleanup(body);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to add queue_ack state_fencing_token");
    }
  }
  return lc_engine_queue_finish_request(body, error);
}

static int lc_engine_build_queue_nack_body(
    lc_engine_client *client, const lc_engine_queue_nack_request *request,
    lc_engine_buffer *body, lc_engine_error *error) {
  int first_field;
  int rc;

  rc = lc_engine_queue_start_request(body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  first_field = 1;
  rc = lc_engine_queue_add_namespace(body, &first_field, client,
                                     request->namespace_name,
                                     "failed to add queue_nack namespace",
                                     error);
  if (rc != LC_ENGINE_OK ||
      lc_engine_json_add_string_field(body, &first_field, "queue",
                                      request->queue) != LC_ENGINE_OK ||
      lc_engine_json_add_string_field(body, &first_field, "message_id",
                                      request->message_id) != LC_ENGINE_OK ||
      lc_engine_json_add_string_field(body, &first_field, "lease_id",
                                      request->lease_id) != LC_ENGINE_OK ||
      lc_engine_json_add_string_field(body, &first_field, "meta_etag",
                                      request->meta_etag) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    if (rc != LC_ENGINE_OK) {
      return rc;
    }
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
  return lc_engine_queue_finish_request(body, error);
}

static int
lc_engine_build_queue_extend_body(lc_engine_client *client,
                                  const lc_engine_queue_extend_request *request,
                                  lc_engine_buffer *body,
                                  lc_engine_error *error) {
  int first_field;
  int rc;

  rc = lc_engine_queue_start_request(body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  first_field = 1;
  rc = lc_engine_queue_add_namespace(body, &first_field, client,
                                     request->namespace_name,
                                     "failed to add queue_extend namespace",
                                     error);
  if (rc != LC_ENGINE_OK ||
      lc_engine_json_add_string_field(body, &first_field, "queue",
                                      request->queue) != LC_ENGINE_OK ||
      lc_engine_json_add_string_field(body, &first_field, "message_id",
                                      request->message_id) != LC_ENGINE_OK ||
      lc_engine_json_add_string_field(body, &first_field, "lease_id",
                                      request->lease_id) != LC_ENGINE_OK ||
      lc_engine_json_add_string_field(body, &first_field, "meta_etag",
                                      request->meta_etag) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    if (rc != LC_ENGINE_OK) {
      return rc;
    }
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to build queue_extend request");
  }
  if (request->txn_id != NULL && request->txn_id[0] != '\0' &&
      lc_engine_json_add_string_field(body, &first_field, "txn_id",
                                      request->txn_id) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add queue_extend txn_id");
  }
  if (request->fencing_token > 0L &&
      lc_engine_json_add_long_field(body, &first_field, "fencing_token",
                                    request->fencing_token) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_NO_MEMORY,
        "failed to add queue_extend fencing_token");
  }
  if (request->extend_by_seconds > 0L &&
      lc_engine_json_add_long_field(body, &first_field, "extend_by_seconds",
                                    request->extend_by_seconds) !=
          LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_NO_MEMORY,
        "failed to add queue_extend extend_by_seconds");
  }
  if (request->state_lease_id != NULL && request->state_lease_id[0] != '\0') {
    if (lc_engine_json_add_string_field(body, &first_field, "state_lease_id",
                                        request->state_lease_id) !=
        LC_ENGINE_OK) {
      lc_engine_buffer_cleanup(body);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to add queue_extend state_lease_id");
    }
    if (request->state_fencing_token > 0L &&
        lc_engine_json_add_long_field(body, &first_field, "state_fencing_token",
                                      request->state_fencing_token) !=
            LC_ENGINE_OK) {
      lc_engine_buffer_cleanup(body);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to add queue_extend state_fencing_token");
    }
  }
  return lc_engine_queue_finish_request(body, error);
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
  rc = lc_engine_build_queue_stats_body(client, request->namespace_name,
                                        request->queue, &body, error);
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
    rc = lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                    "failed to copy queue_stats correlation_id");
  }
  lc_engine_http_result_cleanup(&result);
  return rc;
}

int lc_engine_client_queue_ack(lc_engine_client *client,
                               const lc_engine_queue_ack_request *request,
                               lc_engine_queue_ack_response *response,
                               lc_engine_error *error) {
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
  rc = lc_engine_build_queue_ack_body(client, request, &body, error);
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
  rc = lc_engine_build_queue_extend_body(client, request, &body, error);
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

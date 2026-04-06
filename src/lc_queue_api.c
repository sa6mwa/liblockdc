#include "lc_internal.h"

#include <limits.h>
#include <string.h>

typedef enum lc_engine_queue_parse_kind {
  LC_ENGINE_QUEUE_PARSE_ACK = 1,
  LC_ENGINE_QUEUE_PARSE_NACK = 2,
  LC_ENGINE_QUEUE_PARSE_EXTEND = 3,
  LC_ENGINE_QUEUE_PARSE_STATS = 4
} lc_engine_queue_parse_kind;

typedef struct lc_engine_queue_stats_response_json {
  char *namespace_name;
  char *queue;
  lonejson_int64 waiting_consumers;
  lonejson_int64 pending_candidates;
  lonejson_int64 total_consumers;
  bool has_active_watcher;
  bool available;
  char *head_message_id;
  lonejson_int64 head_enqueued_at_unix;
  lonejson_int64 head_not_visible_until_unix;
  lonejson_int64 head_age_seconds;
} lc_engine_queue_stats_response_json;

typedef struct lc_engine_queue_ack_response_json {
  bool acked;
} lc_engine_queue_ack_response_json;

typedef struct lc_engine_queue_nack_response_json {
  bool requeued;
  char *meta_etag;
} lc_engine_queue_nack_response_json;

typedef struct lc_engine_queue_extend_response_json {
  lonejson_int64 lease_expires_at_unix;
  lonejson_int64 visibility_timeout_seconds;
  char *meta_etag;
  lonejson_int64 state_lease_expires_at_unix;
} lc_engine_queue_extend_response_json;
typedef struct lc_engine_queue_nack_body_json {
  char *namespace_name;
  char *queue;
  char *message_id;
  char *lease_id;
  char *txn_id;
  lonejson_int64 fencing_token;
  char *meta_etag;
  char *state_etag;
  lonejson_int64 delay_seconds;
  lonejson_json_value last_error;
  char *intent;
  char *state_lease_id;
  lonejson_int64 state_fencing_token;
} lc_engine_queue_nack_body_json;

static const lonejson_field lc_engine_queue_stats_response_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_stats_response_json,
                                namespace_name, "namespace"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_stats_response_json, queue,
                                "queue"),
    LONEJSON_FIELD_I64(lc_engine_queue_stats_response_json, waiting_consumers,
                       "waiting_consumers"),
    LONEJSON_FIELD_I64(lc_engine_queue_stats_response_json, pending_candidates,
                       "pending_candidates"),
    LONEJSON_FIELD_I64(lc_engine_queue_stats_response_json, total_consumers,
                       "total_consumers"),
    LONEJSON_FIELD_BOOL(lc_engine_queue_stats_response_json, has_active_watcher,
                        "has_active_watcher"),
    LONEJSON_FIELD_BOOL(lc_engine_queue_stats_response_json, available,
                        "available"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_stats_response_json,
                                head_message_id, "head_message_id"),
    LONEJSON_FIELD_I64(lc_engine_queue_stats_response_json,
                       head_enqueued_at_unix, "head_enqueued_at_unix"),
    LONEJSON_FIELD_I64(lc_engine_queue_stats_response_json,
                       head_not_visible_until_unix,
                       "head_not_visible_until_unix"),
    LONEJSON_FIELD_I64(lc_engine_queue_stats_response_json, head_age_seconds,
                       "head_age_seconds")};

static const lonejson_field lc_engine_queue_ack_response_fields[] = {
    LONEJSON_FIELD_BOOL(lc_engine_queue_ack_response_json, acked, "acked")};

static const lonejson_field lc_engine_queue_nack_response_fields[] = {
    LONEJSON_FIELD_BOOL(lc_engine_queue_nack_response_json, requeued,
                        "requeued"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_nack_response_json, meta_etag,
                                "meta_etag")};

static const lonejson_field lc_engine_queue_extend_response_fields[] = {
    LONEJSON_FIELD_I64(lc_engine_queue_extend_response_json,
                       lease_expires_at_unix, "lease_expires_at_unix"),
    LONEJSON_FIELD_I64(lc_engine_queue_extend_response_json,
                       visibility_timeout_seconds,
                       "visibility_timeout_seconds"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_extend_response_json, meta_etag,
                                "meta_etag"),
    LONEJSON_FIELD_I64(lc_engine_queue_extend_response_json,
                       state_lease_expires_at_unix,
                       "state_lease_expires_at_unix")};
static const lonejson_field lc_engine_queue_stats_body_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_stats_request, namespace_name,
                                "namespace"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_stats_request, queue, "queue")};
static const lonejson_field lc_engine_queue_ack_body_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_ack_request, namespace_name,
                                "namespace"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_ack_request, queue, "queue"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_ack_request, message_id,
                                "message_id"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_ack_request, lease_id,
                                "lease_id"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_ack_request, txn_id, "txn_id"),
    LONEJSON_FIELD_I64(lc_engine_queue_ack_request, fencing_token,
                       "fencing_token"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_ack_request, meta_etag,
                                "meta_etag"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_ack_request, state_etag,
                                "state_etag"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_ack_request, state_lease_id,
                                "state_lease_id"),
    LONEJSON_FIELD_I64(lc_engine_queue_ack_request, state_fencing_token,
                       "state_fencing_token")};
static const lonejson_field lc_engine_queue_extend_body_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_extend_request, namespace_name,
                                "namespace"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_extend_request, queue, "queue"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_extend_request, message_id,
                                "message_id"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_extend_request, lease_id,
                                "lease_id"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_extend_request, txn_id,
                                "txn_id"),
    LONEJSON_FIELD_I64(lc_engine_queue_extend_request, fencing_token,
                       "fencing_token"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_extend_request, meta_etag,
                                "meta_etag"),
    LONEJSON_FIELD_I64(lc_engine_queue_extend_request, extend_by_seconds,
                       "extend_by_seconds"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_extend_request, state_lease_id,
                                "state_lease_id"),
    LONEJSON_FIELD_I64(lc_engine_queue_extend_request, state_fencing_token,
                       "state_fencing_token")};
static const lonejson_field lc_engine_queue_nack_body_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_nack_body_json, namespace_name,
                                "namespace"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_nack_body_json, queue, "queue"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_nack_body_json, message_id,
                                "message_id"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_nack_body_json, lease_id,
                                "lease_id"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_nack_body_json, txn_id,
                                "txn_id"),
    LONEJSON_FIELD_I64(lc_engine_queue_nack_body_json, fencing_token,
                       "fencing_token"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_nack_body_json, meta_etag,
                                "meta_etag"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_nack_body_json, state_etag,
                                "state_etag"),
    LONEJSON_FIELD_I64(lc_engine_queue_nack_body_json, delay_seconds,
                       "delay_seconds"),
    LONEJSON_FIELD_JSON_VALUE_REQ(lc_engine_queue_nack_body_json, last_error,
                                  "last_error"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_nack_body_json, intent,
                                "intent"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_queue_nack_body_json, state_lease_id,
                                "state_lease_id"),
    LONEJSON_FIELD_I64(lc_engine_queue_nack_body_json, state_fencing_token,
                       "state_fencing_token")};

LONEJSON_MAP_DEFINE(lc_engine_queue_stats_body_map,
                    lc_engine_queue_stats_request,
                    lc_engine_queue_stats_body_fields);
LONEJSON_MAP_DEFINE(lc_engine_queue_ack_body_map, lc_engine_queue_ack_request,
                    lc_engine_queue_ack_body_fields);
LONEJSON_MAP_DEFINE(lc_engine_queue_extend_body_map,
                    lc_engine_queue_extend_request,
                    lc_engine_queue_extend_body_fields);
LONEJSON_MAP_DEFINE(lc_engine_queue_nack_body_map,
                    lc_engine_queue_nack_body_json,
                    lc_engine_queue_nack_body_fields);

LONEJSON_MAP_DEFINE(lc_engine_queue_stats_response_map,
                    lc_engine_queue_stats_response_json,
                    lc_engine_queue_stats_response_fields);
LONEJSON_MAP_DEFINE(lc_engine_queue_ack_response_map,
                    lc_engine_queue_ack_response_json,
                    lc_engine_queue_ack_response_fields);
LONEJSON_MAP_DEFINE(lc_engine_queue_nack_response_map,
                    lc_engine_queue_nack_response_json,
                    lc_engine_queue_nack_response_fields);
LONEJSON_MAP_DEFINE(lc_engine_queue_extend_response_map,
                    lc_engine_queue_extend_response_json,
                    lc_engine_queue_extend_response_fields);

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

static int lc_engine_i64_to_int_checked(lonejson_int64 value, const char *label,
                                        int *out_value,
                                        lc_engine_error *error) {
  if (out_value == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "missing int output");
  }
  if (value < (lonejson_int64)INT_MIN || value > (lonejson_int64)INT_MAX) {
    return lc_engine_set_protocol_error(error, label);
  }
  *out_value = (int)value;
  return LC_ENGINE_OK;
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

static int lc_engine_queue_add_namespace(
    lc_engine_buffer *body, int *first_field, lc_engine_client *client,
    const char *namespace_name, const char *label, lc_engine_error *error) {
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

static int lc_engine_queue_parse_response_json(
    const void *parsed_json, const lc_engine_http_result *result,
    lc_engine_queue_parse_kind kind, void *response, lc_engine_error *error) {
  if (parsed_json == NULL || result == NULL || response == NULL ||
      error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "queue response parsing requires result, response, and error");
  }

  switch (kind) {
  case LC_ENGINE_QUEUE_PARSE_ACK: {
    lc_engine_queue_ack_response *ack_response;
    const lc_engine_queue_ack_response_json *parsed;

    ack_response = (lc_engine_queue_ack_response *)response;
    parsed = (const lc_engine_queue_ack_response_json *)parsed_json;
    ack_response->acked = parsed->acked ? 1 : 0;
    break;
  }
  case LC_ENGINE_QUEUE_PARSE_NACK: {
    lc_engine_queue_nack_response *nack_response;
    lc_engine_queue_nack_response_json *parsed;

    nack_response = (lc_engine_queue_nack_response *)response;
    parsed = (lc_engine_queue_nack_response_json *)parsed_json;
    nack_response->requeued = parsed->requeued ? 1 : 0;
    nack_response->meta_etag = lc_engine_strdup_local(parsed->meta_etag);
    if (parsed->meta_etag != NULL && nack_response->meta_etag == NULL) {
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to copy queue_nack response");
    }
    break;
  }
  case LC_ENGINE_QUEUE_PARSE_EXTEND: {
    lc_engine_queue_extend_response *extend_response;
    lc_engine_queue_extend_response_json *parsed;

    extend_response = (lc_engine_queue_extend_response *)response;
    parsed = (lc_engine_queue_extend_response_json *)parsed_json;
    extend_response->lease_expires_at_unix = parsed->lease_expires_at_unix;
    extend_response->visibility_timeout_seconds =
        parsed->visibility_timeout_seconds;
    extend_response->meta_etag = lc_engine_strdup_local(parsed->meta_etag);
    if (parsed->meta_etag != NULL && extend_response->meta_etag == NULL) {
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to copy queue_extend response");
    }
    extend_response->state_lease_expires_at_unix =
        parsed->state_lease_expires_at_unix;
    break;
  }
  case LC_ENGINE_QUEUE_PARSE_STATS: {
    lc_engine_queue_stats_response *stats_response;
    lc_engine_queue_stats_response_json *parsed;
    int value;
    int int_rc;

    stats_response = (lc_engine_queue_stats_response *)response;
    parsed = (lc_engine_queue_stats_response_json *)parsed_json;
    value = 0;
    int_rc = lc_engine_i64_to_int_checked(
        parsed->waiting_consumers,
        "queue_stats waiting_consumers is out of range", &value, error);
    if (int_rc != LC_ENGINE_OK) {
      return int_rc;
    }
    stats_response->waiting_consumers = value;
    int_rc = lc_engine_i64_to_int_checked(
        parsed->pending_candidates,
        "queue_stats pending_candidates is out of range", &value, error);
    if (int_rc != LC_ENGINE_OK) {
      return int_rc;
    }
    stats_response->pending_candidates = value;
    int_rc = lc_engine_i64_to_int_checked(
        parsed->total_consumers, "queue_stats total_consumers is out of range",
        &value, error);
    if (int_rc != LC_ENGINE_OK) {
      return int_rc;
    }
    stats_response->total_consumers = value;
    stats_response->has_active_watcher = parsed->has_active_watcher ? 1 : 0;
    stats_response->available = parsed->available ? 1 : 0;
    stats_response->namespace_name =
        lc_engine_strdup_local(parsed->namespace_name);
    stats_response->queue = lc_engine_strdup_local(parsed->queue);
    stats_response->head_message_id =
        lc_engine_strdup_local(parsed->head_message_id);
    if ((parsed->namespace_name != NULL &&
         stats_response->namespace_name == NULL) ||
        (parsed->queue != NULL && stats_response->queue == NULL) ||
        (parsed->head_message_id != NULL &&
         stats_response->head_message_id == NULL)) {
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to copy queue_stats response");
    }
    stats_response->head_enqueued_at_unix = parsed->head_enqueued_at_unix;
    stats_response->head_not_visible_until_unix =
        parsed->head_not_visible_until_unix;
    stats_response->head_age_seconds = parsed->head_age_seconds;
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
                               lc_engine_buffer *body, lc_engine_error *error) {
  int first_field;
  int rc;

  rc = lc_engine_queue_start_request(body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  first_field = 1;
  rc = lc_engine_queue_add_namespace(
      body, &first_field, client, request->namespace_name,
      "failed to add queue_ack namespace", error);
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
  rc = lc_engine_queue_add_namespace(
      body, &first_field, client, request->namespace_name,
      "failed to add queue_nack namespace", error);
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

static int lc_engine_build_queue_extend_body(
    lc_engine_client *client, const lc_engine_queue_extend_request *request,
    lc_engine_buffer *body, lc_engine_error *error) {
  int first_field;
  int rc;

  rc = lc_engine_queue_start_request(body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  first_field = 1;
  rc = lc_engine_queue_add_namespace(
      body, &first_field, client, request->namespace_name,
      "failed to add queue_extend namespace", error);
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
  lc_engine_http_result result;
  const lc_engine_header_pair *headers;
  size_t header_count;
  lc_engine_queue_stats_request body_src;
  lc_engine_queue_stats_response_json parsed;
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
  lc_engine_queue_request_headers(&headers, &header_count);
  body_src = *request;
  body_src.namespace_name =
      (char *)lc_engine_effective_namespace(client, request->namespace_name);
  rc = lc_engine_http_json_request_stream(
      client, "POST", "/v1/queue/stats", &lc_engine_queue_stats_body_map,
      &body_src, NULL, headers, header_count,
      &lc_engine_queue_stats_response_map, &parsed, &result, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status < 200 || result.http_status >= 300) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lonejson_cleanup(&lc_engine_queue_stats_response_map, &parsed);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }
  rc = lc_engine_queue_parse_response_json(
      &parsed, &result, LC_ENGINE_QUEUE_PARSE_STATS, response, error);
  if (rc == LC_ENGINE_OK &&
      !lc_engine_queue_apply_correlation(&response->correlation_id, &result)) {
    rc =
        lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                   "failed to copy queue_stats correlation_id");
  }
  lonejson_cleanup(&lc_engine_queue_stats_response_map, &parsed);
  lc_engine_http_result_cleanup(&result);
  return rc;
}

int lc_engine_client_queue_ack(lc_engine_client *client,
                               const lc_engine_queue_ack_request *request,
                               lc_engine_queue_ack_response *response,
                               lc_engine_error *error) {
  lc_engine_http_result result;
  const lc_engine_header_pair *headers;
  size_t header_count;
  lc_engine_queue_ack_request body_src;
  lc_engine_queue_ack_response_json parsed;
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
  lc_engine_queue_request_headers(&headers, &header_count);
  body_src = *request;
  body_src.namespace_name =
      (char *)lc_engine_effective_namespace(client, request->namespace_name);
  rc = lc_engine_http_json_request_stream(
      client, "POST", "/v1/queue/ack", &lc_engine_queue_ack_body_map, &body_src,
      NULL, headers, header_count, &lc_engine_queue_ack_response_map, &parsed,
      &result, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status < 200 || result.http_status >= 300) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lonejson_cleanup(&lc_engine_queue_ack_response_map, &parsed);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }
  rc = lc_engine_queue_parse_response_json(
      &parsed, &result, LC_ENGINE_QUEUE_PARSE_ACK, response, error);
  if (rc == LC_ENGINE_OK &&
      !lc_engine_queue_apply_correlation(&response->correlation_id, &result)) {
    rc = lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                    "failed to copy queue_ack correlation_id");
  }
  lonejson_cleanup(&lc_engine_queue_ack_response_map, &parsed);
  lc_engine_http_result_cleanup(&result);
  return rc;
}

int lc_engine_client_queue_nack(lc_engine_client *client,
                                const lc_engine_queue_nack_request *request,
                                lc_engine_queue_nack_response *response,
                                lc_engine_error *error) {
  lc_engine_http_result result;
  const lc_engine_header_pair *headers;
  size_t header_count;
  lc_engine_queue_nack_response_json parsed;
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
  lc_engine_queue_request_headers(&headers, &header_count);
  {
    lc_engine_queue_nack_body_json body_src;
    lc_engine_json_reader_source last_error_source;

    memset(&body_src, 0, sizeof(body_src));
    body_src.namespace_name =
        (char *)lc_engine_effective_namespace(client, request->namespace_name);
    body_src.queue = (char *)request->queue;
    body_src.message_id = (char *)request->message_id;
    body_src.lease_id = (char *)request->lease_id;
    body_src.txn_id = (char *)request->txn_id;
    body_src.fencing_token = request->fencing_token;
    body_src.meta_etag = (char *)request->meta_etag;
    body_src.state_etag = (char *)request->state_etag;
    body_src.delay_seconds = request->delay_seconds;
    body_src.intent = (char *)request->intent;
    body_src.state_lease_id = (char *)request->state_lease_id;
    body_src.state_fencing_token = request->state_fencing_token;
    rc = lc_engine_json_value_init_from_cstr(&body_src.last_error,
                                             &last_error_source,
                                             request->last_error_json, error);
    if (rc == LC_ENGINE_OK) {
      rc = lc_engine_http_json_request_stream(
          client, "POST", "/v1/queue/nack", &lc_engine_queue_nack_body_map,
          &body_src, NULL, headers, header_count,
          &lc_engine_queue_nack_response_map, &parsed, &result, error);
    }
    lonejson_json_value_cleanup(&body_src.last_error);
  }
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status < 200 || result.http_status >= 300) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lonejson_cleanup(&lc_engine_queue_nack_response_map, &parsed);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }
  rc = lc_engine_queue_parse_response_json(
      &parsed, &result, LC_ENGINE_QUEUE_PARSE_NACK, response, error);
  if (rc == LC_ENGINE_OK &&
      !lc_engine_queue_apply_correlation(&response->correlation_id, &result)) {
    rc = lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                    "failed to copy queue_nack correlation_id");
  }
  lonejson_cleanup(&lc_engine_queue_nack_response_map, &parsed);
  lc_engine_http_result_cleanup(&result);
  return rc;
}

int lc_engine_client_queue_extend(lc_engine_client *client,
                                  const lc_engine_queue_extend_request *request,
                                  lc_engine_queue_extend_response *response,
                                  lc_engine_error *error) {
  lc_engine_http_result result;
  const lc_engine_header_pair *headers;
  size_t header_count;
  lc_engine_queue_extend_request body_src;
  lc_engine_queue_extend_response_json parsed;
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
  lc_engine_queue_request_headers(&headers, &header_count);
  body_src = *request;
  body_src.namespace_name =
      (char *)lc_engine_effective_namespace(client, request->namespace_name);
  rc = lc_engine_http_json_request_stream(
      client, "POST", "/v1/queue/extend", &lc_engine_queue_extend_body_map,
      &body_src, NULL, headers, header_count,
      &lc_engine_queue_extend_response_map, &parsed, &result, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status < 200 || result.http_status >= 300) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lonejson_cleanup(&lc_engine_queue_extend_response_map, &parsed);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }
  rc = lc_engine_queue_parse_response_json(
      &parsed, &result, LC_ENGINE_QUEUE_PARSE_EXTEND, response, error);
  if (rc == LC_ENGINE_OK &&
      !lc_engine_queue_apply_correlation(&response->correlation_id, &result)) {
    rc = lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_NO_MEMORY,
        "failed to copy queue_extend correlation_id");
  }
  lonejson_cleanup(&lc_engine_queue_extend_response_map, &parsed);
  lc_engine_http_result_cleanup(&result);
  return rc;
}

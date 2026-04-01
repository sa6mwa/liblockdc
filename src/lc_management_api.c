#include "lc_internal.h"

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct lc_engine_namespace_query_json {
  char *preferred_engine;
  char *fallback_engine;
} lc_engine_namespace_query_json;

typedef struct lc_engine_namespace_config_response_json {
  char *namespace_name;
  lc_engine_namespace_query_json query;
} lc_engine_namespace_config_response_json;

typedef struct lc_engine_index_flush_response_json {
  char *namespace_name;
  char *mode;
  char *flush_id;
  bool accepted;
  bool flushed;
  bool pending;
  lonejson_int64 index_seq;
} lc_engine_index_flush_response_json;

typedef struct lc_engine_txn_response_json {
  char *txn_id;
  char *state;
} lc_engine_txn_response_json;

typedef struct lc_engine_tc_lease_response_json {
  bool granted;
  char *leader_id;
  char *leader_endpoint;
  lonejson_int64 term;
  lonejson_int64 expires_at;
} lc_engine_tc_lease_response_json;

typedef struct lc_engine_tc_release_response_json {
  bool released;
} lc_engine_tc_release_response_json;

typedef struct lc_engine_tc_cluster_response_json {
  lonejson_string_array endpoints;
  lonejson_int64 updated_at_unix;
  lonejson_int64 expires_at_unix;
} lc_engine_tc_cluster_response_json;

typedef struct lc_engine_tcrm_register_response_json {
  char *backend_hash;
  lonejson_string_array endpoints;
  lonejson_int64 updated_at_unix;
} lc_engine_tcrm_register_response_json;

typedef struct lc_engine_tcrm_backend_json {
  char *backend_hash;
  lonejson_string_array endpoints;
  lonejson_int64 updated_at_unix;
} lc_engine_tcrm_backend_json;

typedef struct lc_engine_tcrm_list_response_json {
  lonejson_object_array backends;
  lonejson_int64 updated_at_unix;
} lc_engine_tcrm_list_response_json;

static const lonejson_field lc_engine_namespace_query_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_namespace_query_json,
                                preferred_engine, "preferred_engine"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_namespace_query_json,
                                fallback_engine, "fallback_engine")};

LONEJSON_MAP_DEFINE(lc_engine_namespace_query_map, lc_engine_namespace_query_json,
                    lc_engine_namespace_query_fields);

static const lonejson_field lc_engine_namespace_config_response_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_namespace_config_response_json,
                                namespace_name, "namespace"),
    LONEJSON_FIELD_OBJECT(lc_engine_namespace_config_response_json, query,
                          "query", &lc_engine_namespace_query_map)};

LONEJSON_MAP_DEFINE(lc_engine_namespace_config_response_map,
                    lc_engine_namespace_config_response_json,
                    lc_engine_namespace_config_response_fields);

static const lonejson_field lc_engine_index_flush_response_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_index_flush_response_json,
                                namespace_name, "namespace"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_index_flush_response_json, mode,
                                "mode"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_index_flush_response_json, flush_id,
                                "flush_id"),
    LONEJSON_FIELD_BOOL(lc_engine_index_flush_response_json, accepted,
                        "accepted"),
    LONEJSON_FIELD_BOOL(lc_engine_index_flush_response_json, flushed, "flushed"),
    LONEJSON_FIELD_BOOL(lc_engine_index_flush_response_json, pending, "pending"),
    LONEJSON_FIELD_I64(lc_engine_index_flush_response_json, index_seq,
                       "index_seq")};

LONEJSON_MAP_DEFINE(lc_engine_index_flush_response_map,
                    lc_engine_index_flush_response_json,
                    lc_engine_index_flush_response_fields);

static const lonejson_field lc_engine_txn_response_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_txn_response_json, txn_id, "txn_id"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_txn_response_json, state, "state")};

LONEJSON_MAP_DEFINE(lc_engine_txn_response_map, lc_engine_txn_response_json,
                    lc_engine_txn_response_fields);

static const lonejson_field lc_engine_tc_lease_response_fields[] = {
    LONEJSON_FIELD_BOOL(lc_engine_tc_lease_response_json, granted, "granted"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_tc_lease_response_json, leader_id,
                                "leader_id"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_tc_lease_response_json,
                                leader_endpoint, "leader_endpoint"),
    LONEJSON_FIELD_I64(lc_engine_tc_lease_response_json, term, "term"),
    LONEJSON_FIELD_I64(lc_engine_tc_lease_response_json, expires_at,
                       "expires_at")};

LONEJSON_MAP_DEFINE(lc_engine_tc_lease_response_map,
                    lc_engine_tc_lease_response_json,
                    lc_engine_tc_lease_response_fields);

static const lonejson_field lc_engine_tc_release_response_fields[] = {
    LONEJSON_FIELD_BOOL(lc_engine_tc_release_response_json, released,
                        "released")};

LONEJSON_MAP_DEFINE(lc_engine_tc_release_response_map,
                    lc_engine_tc_release_response_json,
                    lc_engine_tc_release_response_fields);

static const lonejson_field lc_engine_tc_cluster_response_fields[] = {
    LONEJSON_FIELD_STRING_ARRAY(lc_engine_tc_cluster_response_json, endpoints,
                                "endpoints", LONEJSON_OVERFLOW_FAIL),
    LONEJSON_FIELD_I64(lc_engine_tc_cluster_response_json, updated_at_unix,
                       "updated_at_unix"),
    LONEJSON_FIELD_I64(lc_engine_tc_cluster_response_json, expires_at_unix,
                       "expires_at_unix")};

LONEJSON_MAP_DEFINE(lc_engine_tc_cluster_response_map,
                    lc_engine_tc_cluster_response_json,
                    lc_engine_tc_cluster_response_fields);

static const lonejson_field lc_engine_tcrm_register_response_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_tcrm_register_response_json,
                                backend_hash, "backend_hash"),
    LONEJSON_FIELD_STRING_ARRAY(lc_engine_tcrm_register_response_json,
                                endpoints, "endpoints",
                                LONEJSON_OVERFLOW_FAIL),
    LONEJSON_FIELD_I64(lc_engine_tcrm_register_response_json, updated_at_unix,
                       "updated_at_unix")};

LONEJSON_MAP_DEFINE(lc_engine_tcrm_register_response_map,
                    lc_engine_tcrm_register_response_json,
                    lc_engine_tcrm_register_response_fields);

static const lonejson_field lc_engine_tcrm_backend_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_tcrm_backend_json, backend_hash,
                                "backend_hash"),
    LONEJSON_FIELD_STRING_ARRAY(lc_engine_tcrm_backend_json, endpoints,
                                "endpoints", LONEJSON_OVERFLOW_FAIL),
    LONEJSON_FIELD_I64(lc_engine_tcrm_backend_json, updated_at_unix,
                       "updated_at_unix")};

LONEJSON_MAP_DEFINE(lc_engine_tcrm_backend_map, lc_engine_tcrm_backend_json,
                    lc_engine_tcrm_backend_fields);

static const lonejson_field lc_engine_tcrm_list_response_fields[] = {
    LONEJSON_FIELD_OBJECT_ARRAY(lc_engine_tcrm_list_response_json, backends,
                                "backends", lc_engine_tcrm_backend_json,
                                &lc_engine_tcrm_backend_map,
                                LONEJSON_OVERFLOW_FAIL),
    LONEJSON_FIELD_I64(lc_engine_tcrm_list_response_json, updated_at_unix,
                       "updated_at_unix")};

LONEJSON_MAP_DEFINE(lc_engine_tcrm_list_response_map,
                    lc_engine_tcrm_list_response_json,
                    lc_engine_tcrm_list_response_fields);

static int lc_engine_mgmt_i64_to_long_checked(lonejson_int64 value,
                                              const char *label,
                                              long *out_value,
                                              lc_engine_error *error) {
  if (out_value == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "missing long output");
  }
  if (value < (lonejson_int64)LONG_MIN || value > (lonejson_int64)LONG_MAX) {
    return lc_engine_set_protocol_error(error, label);
  }
  *out_value = (long)value;
  return LC_ENGINE_OK;
}

static int lc_engine_mgmt_i64_to_ulong_checked(lonejson_int64 value,
                                               const char *label,
                                               unsigned long *out_value,
                                               lc_engine_error *error) {
  unsigned long narrowed;

  if (out_value == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "missing unsigned long output");
  }
  if (value < 0) {
    return lc_engine_set_protocol_error(error, label);
  }
  narrowed = (unsigned long)value;
  if ((lonejson_int64)narrowed != value) {
    return lc_engine_set_protocol_error(error, label);
  }
  *out_value = narrowed;
  return LC_ENGINE_OK;
}

static int
lc_engine_mgmt_request_headers(const lc_engine_header_pair **out_headers,
                               size_t *out_header_count) {
  static const lc_engine_header_pair headers[] = {
      {"Content-Type", "application/json"}, {"Accept", "application/json"}};

  *out_headers = headers;
  *out_header_count = sizeof(headers) / sizeof(headers[0]);
  return 1;
}

static int lc_engine_mgmt_finish_body(lc_engine_buffer *buffer,
                                      lc_engine_error *error) {
  if (lc_engine_json_end_object(buffer) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(buffer);
    return lc_engine_set_protocol_error(
        error, "failed to finish management request body");
  }
  return LC_ENGINE_OK;
}

static int lc_engine_mgmt_start_body(lc_engine_buffer *buffer,
                                     lc_engine_error *error) {
  lc_engine_buffer_init(buffer);
  if (lc_engine_json_begin_object(buffer) != LC_ENGINE_OK) {
    return lc_engine_set_protocol_error(
        error, "failed to open management request body");
  }
  return LC_ENGINE_OK;
}

static int lc_engine_mgmt_call(lc_engine_client *client, const char *method,
                               const char *path, const lc_engine_buffer *body,
                               void *response, lc_engine_error *error,
                               int (*parse_fn)(const lc_engine_http_result *,
                                               void *, lc_engine_error *)) {
  lc_engine_http_result result;
  const lc_engine_header_pair *headers;
  size_t header_count;
  int rc;

  memset(&result, 0, sizeof(result));
  lc_engine_mgmt_request_headers(&headers, &header_count);
  rc = lc_engine_http_request(client, method, path,
                              body != NULL ? body->data : NULL,
                              body != NULL ? body->length : 0U, headers,
                              body != NULL ? header_count : 0U, &result, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status < 200L || result.http_status >= 300L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }
  rc = parse_fn(&result, response, error);
  lc_engine_http_result_cleanup(&result);
  return rc;
}

static void
lc_engine_mgmt_capture_correlation(const lc_engine_http_result *result,
                                   char **out_value) {
  if (result->correlation_id != NULL && *out_value == NULL) {
    *out_value = lc_engine_strdup_local(result->correlation_id);
  }
}

void lc_engine_string_array_cleanup(lc_engine_string_array *array) {
  size_t index;

  if (array == NULL) {
    return;
  }
  for (index = 0U; index < array->count; ++index) {
    lc_engine_free_string(&array->items[index]);
  }
  free(array->items);
  array->items = NULL;
  array->count = 0U;
}

void lc_engine_namespace_config_response_cleanup(
    lc_engine_namespace_config_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->namespace_name);
  lc_engine_free_string(&response->preferred_engine);
  lc_engine_free_string(&response->fallback_engine);
  lc_engine_free_string(&response->correlation_id);
}

void lc_engine_index_flush_response_cleanup(
    lc_engine_index_flush_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->namespace_name);
  lc_engine_free_string(&response->mode);
  lc_engine_free_string(&response->flush_id);
  lc_engine_free_string(&response->correlation_id);
}

void lc_engine_txn_replay_response_cleanup(
    lc_engine_txn_replay_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->txn_id);
  lc_engine_free_string(&response->state);
  lc_engine_free_string(&response->correlation_id);
}

void lc_engine_txn_decision_response_cleanup(
    lc_engine_txn_decision_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->txn_id);
  lc_engine_free_string(&response->state);
  lc_engine_free_string(&response->correlation_id);
}

void lc_engine_tc_lease_acquire_response_cleanup(
    lc_engine_tc_lease_acquire_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->leader_id);
  lc_engine_free_string(&response->leader_endpoint);
  lc_engine_free_string(&response->correlation_id);
}

void lc_engine_tc_lease_renew_response_cleanup(
    lc_engine_tc_lease_renew_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->leader_id);
  lc_engine_free_string(&response->leader_endpoint);
  lc_engine_free_string(&response->correlation_id);
}

void lc_engine_tc_lease_release_response_cleanup(
    lc_engine_tc_lease_release_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->correlation_id);
}

void lc_engine_tc_leader_response_cleanup(
    lc_engine_tc_leader_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->leader_id);
  lc_engine_free_string(&response->leader_endpoint);
  lc_engine_free_string(&response->correlation_id);
}

void lc_engine_tc_cluster_response_cleanup(
    lc_engine_tc_cluster_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_string_array_cleanup(&response->endpoints);
  lc_engine_free_string(&response->correlation_id);
}

void lc_engine_tcrm_register_response_cleanup(
    lc_engine_tcrm_register_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->backend_hash);
  lc_engine_string_array_cleanup(&response->endpoints);
  lc_engine_free_string(&response->correlation_id);
}

void lc_engine_tcrm_unregister_response_cleanup(
    lc_engine_tcrm_unregister_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->backend_hash);
  lc_engine_string_array_cleanup(&response->endpoints);
  lc_engine_free_string(&response->correlation_id);
}

void lc_engine_tcrm_list_response_cleanup(
    lc_engine_tcrm_list_response *response) {
  size_t index;

  if (response == NULL) {
    return;
  }
  for (index = 0U; index < response->backend_count; ++index) {
    lc_engine_free_string(&response->backends[index].backend_hash);
    lc_engine_string_array_cleanup(&response->backends[index].endpoints);
  }
  free(response->backends);
  response->backends = NULL;
  response->backend_count = 0U;
  lc_engine_free_string(&response->correlation_id);
}

static int
lc_engine_parse_namespace_response(const lc_engine_http_result *result,
                                   void *out_response, lc_engine_error *error) {
  lc_engine_namespace_config_response *response;
  lc_engine_namespace_config_response_json parsed;
  lonejson_error lj_error;
  lonejson_status status;
  int rc;

  response = (lc_engine_namespace_config_response *)out_response;
  memset(&parsed, 0, sizeof(parsed));
  memset(&lj_error, 0, sizeof(lj_error));
  status = lonejson_parse_cstr(&lc_engine_namespace_config_response_map, &parsed,
                               result->body.data, NULL, &lj_error);
  rc = lc_engine_lonejson_error_from_status(
      error, status, &lj_error, "failed to parse namespace response");
  if (rc != LC_ENGINE_OK) {
    lonejson_cleanup(&lc_engine_namespace_config_response_map, &parsed);
    return rc;
  }
  response->namespace_name = parsed.namespace_name;
  response->preferred_engine = parsed.query.preferred_engine;
  response->fallback_engine = parsed.query.fallback_engine;
  lc_engine_mgmt_capture_correlation(result, &response->correlation_id);
  parsed.namespace_name = NULL;
  parsed.query.preferred_engine = NULL;
  parsed.query.fallback_engine = NULL;
  lonejson_cleanup(&lc_engine_namespace_config_response_map, &parsed);
  return LC_ENGINE_OK;
}

static int
lc_engine_parse_index_flush_response(const lc_engine_http_result *result,
                                     void *out_response,
                                     lc_engine_error *error) {
  lc_engine_index_flush_response *response;
  lc_engine_index_flush_response_json parsed;
  lonejson_error lj_error;
  lonejson_status status;
  int rc;

  response = (lc_engine_index_flush_response *)out_response;
  memset(&parsed, 0, sizeof(parsed));
  memset(&lj_error, 0, sizeof(lj_error));
  status = lonejson_parse_cstr(&lc_engine_index_flush_response_map, &parsed,
                               result->body.data, NULL, &lj_error);
  rc = lc_engine_lonejson_error_from_status(
      error, status, &lj_error, "failed to parse index flush response");
  if (rc != LC_ENGINE_OK) {
    lonejson_cleanup(&lc_engine_index_flush_response_map, &parsed);
    return rc;
  }
  response->namespace_name = parsed.namespace_name;
  response->mode = parsed.mode;
  response->flush_id = parsed.flush_id;
  response->accepted = parsed.accepted ? 1 : 0;
  response->flushed = parsed.flushed ? 1 : 0;
  response->pending = parsed.pending ? 1 : 0;
  rc = lc_engine_mgmt_i64_to_ulong_checked(parsed.index_seq,
                                           "index_flush index_seq is out of range",
                                           &response->index_seq, error);
  if (rc != LC_ENGINE_OK) {
    parsed.namespace_name = NULL;
    parsed.mode = NULL;
    parsed.flush_id = NULL;
    lonejson_cleanup(&lc_engine_index_flush_response_map, &parsed);
    return rc;
  }
  lc_engine_mgmt_capture_correlation(result, &response->correlation_id);
  parsed.namespace_name = NULL;
  parsed.mode = NULL;
  parsed.flush_id = NULL;
  lonejson_cleanup(&lc_engine_index_flush_response_map, &parsed);
  return LC_ENGINE_OK;
}

static int lc_engine_parse_txn_response(const lc_engine_http_result *result,
                                        void *out_response,
                                        lc_engine_error *error) {
  lc_engine_txn_replay_response *response;
  lc_engine_txn_response_json parsed;
  lonejson_error lj_error;
  lonejson_status status;
  int rc;

  response = (lc_engine_txn_replay_response *)out_response;
  memset(&parsed, 0, sizeof(parsed));
  memset(&lj_error, 0, sizeof(lj_error));
  status = lonejson_parse_cstr(&lc_engine_txn_response_map, &parsed,
                               result->body.data, NULL, &lj_error);
  rc = lc_engine_lonejson_error_from_status(
      error, status, &lj_error, "failed to parse txn response");
  if (rc != LC_ENGINE_OK) {
    lonejson_cleanup(&lc_engine_txn_response_map, &parsed);
    return rc;
  }
  response->txn_id = parsed.txn_id;
  response->state = parsed.state;
  lc_engine_mgmt_capture_correlation(result, &response->correlation_id);
  parsed.txn_id = NULL;
  parsed.state = NULL;
  lonejson_cleanup(&lc_engine_txn_response_map, &parsed);
  return LC_ENGINE_OK;
}

static int
lc_engine_parse_tc_lease_acquire_response(const lc_engine_http_result *result,
                                          void *out_response,
                                          lc_engine_error *error) {
  lc_engine_tc_lease_acquire_response *response;
  lc_engine_tc_lease_response_json parsed;
  lonejson_error lj_error;
  lonejson_status status;
  int rc;

  response = (lc_engine_tc_lease_acquire_response *)out_response;
  memset(&parsed, 0, sizeof(parsed));
  memset(&lj_error, 0, sizeof(lj_error));
  status = lonejson_parse_cstr(&lc_engine_tc_lease_response_map, &parsed,
                               result->body.data, NULL, &lj_error);
  rc = lc_engine_lonejson_error_from_status(
      error, status, &lj_error, "failed to parse tc lease response");
  if (rc != LC_ENGINE_OK) {
    lonejson_cleanup(&lc_engine_tc_lease_response_map, &parsed);
    return rc;
  }
  response->granted = parsed.granted ? 1 : 0;
  response->leader_id = parsed.leader_id;
  response->leader_endpoint = parsed.leader_endpoint;
  rc = lc_engine_mgmt_i64_to_ulong_checked(parsed.term,
                                           "tc lease term is out of range",
                                           &response->term, error);
  if (rc == LC_ENGINE_OK) {
    rc = lc_engine_mgmt_i64_to_long_checked(
        parsed.expires_at, "tc lease expires_at is out of range",
        &response->expires_at_unix, error);
  }
  if (rc != LC_ENGINE_OK) {
    parsed.leader_id = NULL;
    parsed.leader_endpoint = NULL;
    lonejson_cleanup(&lc_engine_tc_lease_response_map, &parsed);
    return rc;
  }
  lc_engine_mgmt_capture_correlation(result, &response->correlation_id);
  parsed.leader_id = NULL;
  parsed.leader_endpoint = NULL;
  lonejson_cleanup(&lc_engine_tc_lease_response_map, &parsed);
  return LC_ENGINE_OK;
}

static int
lc_engine_parse_tc_lease_renew_response(const lc_engine_http_result *result,
                                        void *out_response,
                                        lc_engine_error *error) {
  lc_engine_tc_lease_renew_response *response;
  lc_engine_tc_lease_acquire_response temp;
  int rc;

  memset(&temp, 0, sizeof(temp));
  rc = lc_engine_parse_tc_lease_acquire_response(result, &temp, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  response = (lc_engine_tc_lease_renew_response *)out_response;
  response->renewed = temp.granted;
  response->leader_id = temp.leader_id;
  response->leader_endpoint = temp.leader_endpoint;
  response->term = temp.term;
  response->expires_at_unix = temp.expires_at_unix;
  response->correlation_id = temp.correlation_id;
  return LC_ENGINE_OK;
}

static int
lc_engine_parse_tc_lease_release_response(const lc_engine_http_result *result,
                                          void *out_response,
                                          lc_engine_error *error) {
  lc_engine_tc_lease_release_response *response;
  lc_engine_tc_release_response_json parsed;
  lonejson_error lj_error;
  lonejson_status status;
  int rc;

  response = (lc_engine_tc_lease_release_response *)out_response;
  memset(&parsed, 0, sizeof(parsed));
  memset(&lj_error, 0, sizeof(lj_error));
  status = lonejson_parse_cstr(&lc_engine_tc_release_response_map, &parsed,
                               result->body.data, NULL, &lj_error);
  rc = lc_engine_lonejson_error_from_status(
      error, status, &lj_error, "failed to parse tc lease release response");
  if (rc != LC_ENGINE_OK) {
    lonejson_cleanup(&lc_engine_tc_release_response_map, &parsed);
    return rc;
  }
  response->released = parsed.released ? 1 : 0;
  lc_engine_mgmt_capture_correlation(result, &response->correlation_id);
  lonejson_cleanup(&lc_engine_tc_release_response_map, &parsed);
  return LC_ENGINE_OK;
}

static int
lc_engine_parse_tc_leader_response(const lc_engine_http_result *result,
                                   void *out_response, lc_engine_error *error) {
  lc_engine_tc_leader_response *response;
  lc_engine_tc_lease_acquire_response temp;
  int rc;

  memset(&temp, 0, sizeof(temp));
  rc = lc_engine_parse_tc_lease_acquire_response(result, &temp, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  response = (lc_engine_tc_leader_response *)out_response;
  response->leader_id = temp.leader_id;
  response->leader_endpoint = temp.leader_endpoint;
  response->term = temp.term;
  response->expires_at_unix = temp.expires_at_unix;
  response->correlation_id = temp.correlation_id;
  return LC_ENGINE_OK;
}

static int
lc_engine_parse_tc_cluster_response(const lc_engine_http_result *result,
                                    void *out_response,
                                    lc_engine_error *error) {
  lc_engine_tc_cluster_response *response;
  lc_engine_tc_cluster_response_json parsed;
  lonejson_error lj_error;
  lonejson_status status;
  size_t index;
  int rc;

  response = (lc_engine_tc_cluster_response *)out_response;
  memset(&parsed, 0, sizeof(parsed));
  memset(&lj_error, 0, sizeof(lj_error));
  status = lonejson_parse_cstr(&lc_engine_tc_cluster_response_map, &parsed,
                               result->body.data, NULL, &lj_error);
  rc = lc_engine_lonejson_error_from_status(
      error, status, &lj_error, "failed to parse tc cluster response");
  if (rc != LC_ENGINE_OK) {
    lonejson_cleanup(&lc_engine_tc_cluster_response_map, &parsed);
    return rc;
  }
  response->endpoints.count = parsed.endpoints.count;
  if (parsed.endpoints.count > 0U) {
    response->endpoints.items =
        (char **)calloc(parsed.endpoints.count, sizeof(char *));
    if (response->endpoints.items == NULL) {
      lonejson_cleanup(&lc_engine_tc_cluster_response_map, &parsed);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to allocate tc cluster endpoints");
    }
    for (index = 0U; index < parsed.endpoints.count; ++index) {
      response->endpoints.items[index] =
          lc_engine_strdup_local(parsed.endpoints.items[index]);
      if (response->endpoints.items[index] == NULL) {
        lonejson_cleanup(&lc_engine_tc_cluster_response_map, &parsed);
        return lc_engine_set_client_error(
            error, LC_ENGINE_ERROR_NO_MEMORY,
            "failed to allocate tc cluster endpoint");
      }
    }
  }
  rc = lc_engine_mgmt_i64_to_long_checked(
      parsed.updated_at_unix, "tc cluster updated_at_unix is out of range",
      &response->updated_at_unix, error);
  if (rc == LC_ENGINE_OK) {
    rc = lc_engine_mgmt_i64_to_long_checked(
        parsed.expires_at_unix, "tc cluster expires_at_unix is out of range",
        &response->expires_at_unix, error);
  }
  if (rc != LC_ENGINE_OK) {
    lonejson_cleanup(&lc_engine_tc_cluster_response_map, &parsed);
    return rc;
  }
  lc_engine_mgmt_capture_correlation(result, &response->correlation_id);
  lonejson_cleanup(&lc_engine_tc_cluster_response_map, &parsed);
  return LC_ENGINE_OK;
}

static int
lc_engine_parse_tcrm_register_response(const lc_engine_http_result *result,
                                       void *out_response,
                                       lc_engine_error *error) {
  lc_engine_tcrm_register_response *response;
  lc_engine_tcrm_register_response_json parsed;
  lonejson_error lj_error;
  lonejson_status status;
  size_t index;
  int rc;

  response = (lc_engine_tcrm_register_response *)out_response;
  memset(&parsed, 0, sizeof(parsed));
  memset(&lj_error, 0, sizeof(lj_error));
  status = lonejson_parse_cstr(&lc_engine_tcrm_register_response_map, &parsed,
                               result->body.data, NULL, &lj_error);
  rc = lc_engine_lonejson_error_from_status(
      error, status, &lj_error, "failed to parse tcrm register response");
  if (rc != LC_ENGINE_OK) {
    lonejson_cleanup(&lc_engine_tcrm_register_response_map, &parsed);
    return rc;
  }
  response->backend_hash = parsed.backend_hash;
  response->endpoints.count = parsed.endpoints.count;
  if (parsed.endpoints.count > 0U) {
    response->endpoints.items =
        (char **)calloc(parsed.endpoints.count, sizeof(char *));
    if (response->endpoints.items == NULL) {
      parsed.backend_hash = NULL;
      lonejson_cleanup(&lc_engine_tcrm_register_response_map, &parsed);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to allocate tcrm register endpoints");
    }
    for (index = 0U; index < parsed.endpoints.count; ++index) {
      response->endpoints.items[index] =
          lc_engine_strdup_local(parsed.endpoints.items[index]);
      if (response->endpoints.items[index] == NULL) {
        parsed.backend_hash = NULL;
        lonejson_cleanup(&lc_engine_tcrm_register_response_map, &parsed);
        return lc_engine_set_client_error(
            error, LC_ENGINE_ERROR_NO_MEMORY,
            "failed to allocate tcrm register endpoint");
      }
    }
  }
  rc = lc_engine_mgmt_i64_to_long_checked(
      parsed.updated_at_unix, "tcrm register updated_at_unix is out of range",
      &response->updated_at_unix, error);
  if (rc != LC_ENGINE_OK) {
    parsed.backend_hash = NULL;
    lonejson_cleanup(&lc_engine_tcrm_register_response_map, &parsed);
    return rc;
  }
  lc_engine_mgmt_capture_correlation(result, &response->correlation_id);
  parsed.backend_hash = NULL;
  lonejson_cleanup(&lc_engine_tcrm_register_response_map, &parsed);
  return LC_ENGINE_OK;
}

static int
lc_engine_parse_tcrm_list_response(const lc_engine_http_result *result,
                                   void *out_response, lc_engine_error *error) {
  lc_engine_tcrm_list_response *response;
  lc_engine_tcrm_list_response_json parsed;
  lonejson_error lj_error;
  lonejson_status status;
  size_t index;
  int rc;

  response = (lc_engine_tcrm_list_response *)out_response;
  memset(&parsed, 0, sizeof(parsed));
  memset(&lj_error, 0, sizeof(lj_error));
  status = lonejson_parse_cstr(&lc_engine_tcrm_list_response_map, &parsed,
                               result->body.data, NULL, &lj_error);
  rc = lc_engine_lonejson_error_from_status(
      error, status, &lj_error, "failed to parse tcrm list response");
  if (rc != LC_ENGINE_OK) {
    lonejson_cleanup(&lc_engine_tcrm_list_response_map, &parsed);
    return rc;
  }
  if (parsed.backends.count > 0U) {
    response->backends = (lc_engine_tcrm_backend *)calloc(
        parsed.backends.count, sizeof(lc_engine_tcrm_backend));
    if (response->backends == NULL) {
      lonejson_cleanup(&lc_engine_tcrm_list_response_map, &parsed);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to allocate tcrm backend list");
    }
    response->backend_count = parsed.backends.count;
    for (index = 0U; index < parsed.backends.count; ++index) {
      lc_engine_tcrm_backend_json *backend;
      size_t endpoint_index;

      backend = &((lc_engine_tcrm_backend_json *)parsed.backends.items)[index];
      response->backends[index].backend_hash =
          lc_engine_strdup_local(backend->backend_hash);
      response->backends[index].endpoints.count = backend->endpoints.count;
      if (response->backends[index].backend_hash == NULL) {
        lonejson_cleanup(&lc_engine_tcrm_list_response_map, &parsed);
        return lc_engine_set_client_error(
            error, LC_ENGINE_ERROR_NO_MEMORY,
            "failed to allocate tcrm backend item");
      }
      if (backend->endpoints.count > 0U) {
        response->backends[index].endpoints.items =
            (char **)calloc(backend->endpoints.count, sizeof(char *));
        if (response->backends[index].endpoints.items == NULL) {
          lonejson_cleanup(&lc_engine_tcrm_list_response_map, &parsed);
          return lc_engine_set_client_error(
              error, LC_ENGINE_ERROR_NO_MEMORY,
              "failed to allocate tcrm backend endpoints");
        }
        for (endpoint_index = 0U; endpoint_index < backend->endpoints.count;
             ++endpoint_index) {
          response->backends[index].endpoints.items[endpoint_index] =
              lc_engine_strdup_local(backend->endpoints.items[endpoint_index]);
          if (response->backends[index].endpoints.items[endpoint_index] ==
              NULL) {
            lonejson_cleanup(&lc_engine_tcrm_list_response_map, &parsed);
            return lc_engine_set_client_error(
                error, LC_ENGINE_ERROR_NO_MEMORY,
                "failed to allocate tcrm backend endpoint");
          }
        }
      }
      rc = lc_engine_mgmt_i64_to_long_checked(
          backend->updated_at_unix, "tcrm backend updated_at_unix is out of range",
          &response->backends[index].updated_at_unix, error);
      if (rc != LC_ENGINE_OK) {
        lonejson_cleanup(&lc_engine_tcrm_list_response_map, &parsed);
        return rc;
      }
    }
  }
  rc = lc_engine_mgmt_i64_to_long_checked(
      parsed.updated_at_unix, "tcrm list updated_at_unix is out of range",
      &response->updated_at_unix, error);
  if (rc != LC_ENGINE_OK) {
    lonejson_cleanup(&lc_engine_tcrm_list_response_map, &parsed);
    return rc;
  }
  lc_engine_mgmt_capture_correlation(result, &response->correlation_id);
  lonejson_cleanup(&lc_engine_tcrm_list_response_map, &parsed);
  return LC_ENGINE_OK;
}

int lc_engine_client_get_namespace_config(
    lc_engine_client *client, const char *namespace_name,
    lc_engine_namespace_config_response *response, lc_engine_error *error) {
  lc_engine_buffer path;
  char *encoded;
  int rc;

  if (client == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "get_namespace_config requires client, response, and error");
  }
  lc_engine_buffer_init(&path);
  if (lc_engine_buffer_append_cstr(&path, "/v1/namespace") != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&path);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to allocate namespace path");
  }
  if (namespace_name != NULL && namespace_name[0] != '\0') {
    encoded = lc_engine_url_encode(namespace_name);
    if (encoded == NULL) {
      lc_engine_buffer_cleanup(&path);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to encode namespace");
    }
    lc_engine_buffer_append_cstr(&path, "?namespace=");
    lc_engine_buffer_append_cstr(&path, encoded);
    free(encoded);
  }
  rc = lc_engine_mgmt_call(client, "GET", path.data, NULL, response, error,
                           lc_engine_parse_namespace_response);
  lc_engine_buffer_cleanup(&path);
  return rc;
}

int lc_engine_client_update_namespace_config(
    lc_engine_client *client, const lc_engine_namespace_config_request *request,
    lc_engine_namespace_config_response *response, lc_engine_error *error) {
  lc_engine_buffer body;
  lc_engine_buffer query;
  int first_field;
  int query_first_field;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "update_namespace_config requires "
                                      "client, request, response, and error");
  }
  rc = lc_engine_mgmt_start_body(&body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  first_field = 1;
  if (request->namespace_name != NULL && request->namespace_name[0] != '\0' &&
      lc_engine_json_add_string_field(&body, &first_field, "namespace",
                                      request->namespace_name) !=
          LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add namespace name");
  }
  if (request->preferred_engine != NULL || request->fallback_engine != NULL) {
    lc_engine_buffer_init(&query);
    if (lc_engine_json_begin_object(&query) != LC_ENGINE_OK) {
      lc_engine_buffer_cleanup(&body);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to start namespace query config");
    }
    query_first_field = 1;
    if (request->preferred_engine != NULL &&
        request->preferred_engine[0] != '\0' &&
        lc_engine_json_add_string_field(&query, &query_first_field,
                                        "preferred_engine",
                                        request->preferred_engine) !=
            LC_ENGINE_OK) {
      lc_engine_buffer_cleanup(&query);
      lc_engine_buffer_cleanup(&body);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to add preferred engine");
    }
    if (request->fallback_engine != NULL &&
        request->fallback_engine[0] != '\0' &&
        lc_engine_json_add_string_field(&query, &query_first_field,
                                        "fallback_engine",
                                        request->fallback_engine) !=
            LC_ENGINE_OK) {
      lc_engine_buffer_cleanup(&query);
      lc_engine_buffer_cleanup(&body);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to add fallback engine");
    }
    if (lc_engine_json_end_object(&query) != LC_ENGINE_OK ||
        lc_engine_json_add_raw_field(&body, &first_field, "query",
                                     query.data) != LC_ENGINE_OK) {
      lc_engine_buffer_cleanup(&query);
      lc_engine_buffer_cleanup(&body);
      return lc_engine_set_protocol_error(
          error, "failed to build namespace query config");
    }
    lc_engine_buffer_cleanup(&query);
  }
  rc = lc_engine_mgmt_finish_body(&body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  rc = lc_engine_mgmt_call(client, "PUT", "/v1/namespace", &body, response,
                           error, lc_engine_parse_namespace_response);
  lc_engine_buffer_cleanup(&body);
  return rc;
}

int lc_engine_client_index_flush(lc_engine_client *client,
                                 const lc_engine_index_flush_request *request,
                                 lc_engine_index_flush_response *response,
                                 lc_engine_error *error) {
  lc_engine_buffer body;
  int first_field;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "index_flush requires client, request, response, and error");
  }
  rc = lc_engine_mgmt_start_body(&body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  first_field = 1;
  if (request->namespace_name != NULL && request->namespace_name[0] != '\0' &&
      lc_engine_json_add_string_field(&body, &first_field, "namespace",
                                      request->namespace_name) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add flush namespace");
  }
  if (request->mode != NULL && request->mode[0] != '\0' &&
      lc_engine_json_add_string_field(&body, &first_field, "mode",
                                      request->mode) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add flush mode");
  }
  rc = lc_engine_mgmt_finish_body(&body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  rc = lc_engine_mgmt_call(client, "POST", "/v1/index/flush", &body, response,
                           error, lc_engine_parse_index_flush_response);
  lc_engine_buffer_cleanup(&body);
  return rc;
}

int lc_engine_client_txn_replay(lc_engine_client *client,
                                const lc_engine_txn_replay_request *request,
                                lc_engine_txn_replay_response *response,
                                lc_engine_error *error) {
  lc_engine_buffer body;
  int first_field;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL ||
      request->txn_id == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "txn_replay requires client, request, response, error, and txn_id");
  }
  rc = lc_engine_mgmt_start_body(&body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  first_field = 1;
  if (lc_engine_json_add_string_field(&body, &first_field, "txn_id",
                                      request->txn_id) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add txn_id");
  }
  rc = lc_engine_mgmt_finish_body(&body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  rc = lc_engine_mgmt_call(client, "POST", "/v1/txn/replay", &body, response,
                           error, lc_engine_parse_txn_response);
  lc_engine_buffer_cleanup(&body);
  return rc;
}

static int lc_engine_mgmt_build_txn_decision_body(
    const lc_engine_txn_decision_request *request, lc_engine_buffer *body,
    lc_engine_error *error) {
  lc_engine_buffer participants;
  lc_engine_buffer participant;
  size_t index;
  int first_field;
  int participant_first_field;
  int rc;

  rc = lc_engine_mgmt_start_body(body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  first_field = 1;
  if (lc_engine_json_add_string_field(body, &first_field, "txn_id",
                                      request->txn_id) != LC_ENGINE_OK ||
      lc_engine_json_add_string_field(body, &first_field, "state",
                                      request->state) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add txn decision basics");
  }
  if (request->participant_count > 0U && request->participants != NULL) {
    lc_engine_buffer_init(&participants);
    if (lc_engine_buffer_append_cstr(&participants, "[") != LC_ENGINE_OK) {
      lc_engine_buffer_cleanup(body);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to open participants array");
    }
    for (index = 0U; index < request->participant_count; ++index) {
      lc_engine_buffer_init(&participant);
      if (lc_engine_json_begin_object(&participant) != LC_ENGINE_OK) {
        lc_engine_buffer_cleanup(&participants);
        lc_engine_buffer_cleanup(body);
        return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                          "failed to add txn participant");
      }
      participant_first_field = 1;
      if (lc_engine_json_add_string_field(
              &participant, &participant_first_field, "namespace",
              request->participants[index].namespace_name) != LC_ENGINE_OK ||
          lc_engine_json_add_string_field(&participant, &participant_first_field,
                                          "key",
                                          request->participants[index].key) !=
              LC_ENGINE_OK) {
        lc_engine_buffer_cleanup(&participant);
        lc_engine_buffer_cleanup(&participants);
        lc_engine_buffer_cleanup(body);
        return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                          "failed to add txn participant");
      }
      if (request->participants[index].backend_hash != NULL &&
          request->participants[index].backend_hash[0] != '\0' &&
          lc_engine_json_add_string_field(
              &participant, &participant_first_field, "backend_hash",
              request->participants[index].backend_hash) != LC_ENGINE_OK) {
        lc_engine_buffer_cleanup(&participant);
        lc_engine_buffer_cleanup(&participants);
        lc_engine_buffer_cleanup(body);
        return lc_engine_set_client_error(
            error, LC_ENGINE_ERROR_NO_MEMORY,
            "failed to add txn participant backend_hash");
      }
      if (lc_engine_json_end_object(&participant) != LC_ENGINE_OK) {
        lc_engine_buffer_cleanup(&participant);
        lc_engine_buffer_cleanup(&participants);
        lc_engine_buffer_cleanup(body);
        return lc_engine_set_protocol_error(error,
                                            "failed to close txn participant");
      }
      if (index > 0U &&
          lc_engine_buffer_append_cstr(&participants, ",") != LC_ENGINE_OK) {
        lc_engine_buffer_cleanup(&participant);
        lc_engine_buffer_cleanup(&participants);
        lc_engine_buffer_cleanup(body);
        return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                          "failed to separate participants");
      }
      if (lc_engine_buffer_append(&participants, participant.data,
                                  participant.length) != LC_ENGINE_OK) {
        lc_engine_buffer_cleanup(&participant);
        lc_engine_buffer_cleanup(&participants);
        lc_engine_buffer_cleanup(body);
        return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                          "failed to append participant");
      }
      lc_engine_buffer_cleanup(&participant);
    }
    if (lc_engine_buffer_append_cstr(&participants, "]") != LC_ENGINE_OK ||
        lc_engine_json_add_raw_field(body, &first_field, "participants",
                                     participants.data) != LC_ENGINE_OK) {
      lc_engine_buffer_cleanup(&participants);
      lc_engine_buffer_cleanup(body);
      return lc_engine_set_protocol_error(error,
                                          "failed to close participants array");
    }
    lc_engine_buffer_cleanup(&participants);
  }
  if (request->expires_at_unix > 0L &&
      lc_engine_json_add_long_field(body, &first_field, "expires_at_unix",
                                    request->expires_at_unix) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add txn expires_at_unix");
  }
  if (request->tc_term > 0UL &&
      request->tc_term <= (unsigned long)LONG_MAX &&
      lc_engine_json_add_long_field(body, &first_field, "tc_term",
                                    (long)request->tc_term) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add txn tc_term");
  }
  if (request->target_backend_hash != NULL &&
      request->target_backend_hash[0] != '\0' &&
      lc_engine_json_add_string_field(body, &first_field, "target_backend_hash",
                                      request->target_backend_hash) !=
          LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add txn target_backend_hash");
  }
  return lc_engine_mgmt_finish_body(body, error);
}

int lc_engine_client_txn_decide(lc_engine_client *client,
                                const lc_engine_txn_decision_request *request,
                                lc_engine_txn_decision_response *response,
                                lc_engine_error *error) {
  lc_engine_buffer body;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL ||
      request->txn_id == NULL || request->state == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "txn_decide requires client, request, "
                                      "response, error, txn_id, and state");
  }
  rc = lc_engine_mgmt_build_txn_decision_body(request, &body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  rc = lc_engine_mgmt_call(
      client, "POST", "/v1/txn/decide", &body, response, error,
      (int (*)(const lc_engine_http_result *, void *,
               lc_engine_error *))lc_engine_parse_txn_response);
  lc_engine_buffer_cleanup(&body);
  return rc;
}

int lc_engine_client_txn_commit(lc_engine_client *client,
                                const lc_engine_txn_decision_request *request,
                                lc_engine_txn_decision_response *response,
                                lc_engine_error *error) {
  lc_engine_buffer body;
  lc_engine_txn_decision_request effective_request;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "txn_commit requires client, request, response, and error");
  }
  effective_request = *request;
  effective_request.state = "commit";
  rc = lc_engine_mgmt_build_txn_decision_body(&effective_request, &body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  rc = lc_engine_mgmt_call(
      client, "POST", "/v1/txn/commit", &body, response, error,
      (int (*)(const lc_engine_http_result *, void *,
               lc_engine_error *))lc_engine_parse_txn_response);
  lc_engine_buffer_cleanup(&body);
  return rc;
}

int lc_engine_client_txn_rollback(lc_engine_client *client,
                                  const lc_engine_txn_decision_request *request,
                                  lc_engine_txn_decision_response *response,
                                  lc_engine_error *error) {
  lc_engine_buffer body;
  lc_engine_txn_decision_request effective_request;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "txn_rollback requires client, request, response, and error");
  }
  effective_request = *request;
  effective_request.state = "rollback";
  rc = lc_engine_mgmt_build_txn_decision_body(&effective_request, &body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  rc = lc_engine_mgmt_call(
      client, "POST", "/v1/txn/rollback", &body, response, error,
      (int (*)(const lc_engine_http_result *, void *,
               lc_engine_error *))lc_engine_parse_txn_response);
  lc_engine_buffer_cleanup(&body);
  return rc;
}

int lc_engine_client_tc_lease_acquire(
    lc_engine_client *client, const lc_engine_tc_lease_acquire_request *request,
    lc_engine_tc_lease_acquire_response *response, lc_engine_error *error) {
  lc_engine_buffer body;
  int first_field;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "tc_lease_acquire requires client, request, response, and error");
  }
  rc = lc_engine_mgmt_start_body(&body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  first_field = 1;
  if (lc_engine_json_add_string_field(&body, &first_field, "candidate_id",
                                      request->candidate_id) != LC_ENGINE_OK ||
      lc_engine_json_add_string_field(&body, &first_field,
                                      "candidate_endpoint",
                                      request->candidate_endpoint) !=
          LC_ENGINE_OK ||
      request->term > (unsigned long)LONG_MAX ||
      lc_engine_json_add_long_field(&body, &first_field, "term",
                                    (long)request->term) != LC_ENGINE_OK ||
      lc_engine_json_add_long_field(&body, &first_field, "ttl_ms",
                                    request->ttl_ms) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to build tc lease acquire body");
  }
  rc = lc_engine_mgmt_finish_body(&body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  rc = lc_engine_mgmt_call(client, "POST", "/v1/tc/lease/acquire", &body,
                           response, error,
                           lc_engine_parse_tc_lease_acquire_response);
  lc_engine_buffer_cleanup(&body);
  return rc;
}

int lc_engine_client_tc_lease_renew(
    lc_engine_client *client, const lc_engine_tc_lease_renew_request *request,
    lc_engine_tc_lease_renew_response *response, lc_engine_error *error) {
  lc_engine_buffer body;
  int first_field;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "tc_lease_renew requires client, request, response, and error");
  }
  rc = lc_engine_mgmt_start_body(&body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  first_field = 1;
  if (lc_engine_json_add_string_field(&body, &first_field, "leader_id",
                                      request->leader_id) != LC_ENGINE_OK ||
      request->term > (unsigned long)LONG_MAX ||
      lc_engine_json_add_long_field(&body, &first_field, "term",
                                    (long)request->term) != LC_ENGINE_OK ||
      lc_engine_json_add_long_field(&body, &first_field, "ttl_ms",
                                    request->ttl_ms) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to build tc lease renew body");
  }
  rc = lc_engine_mgmt_finish_body(&body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  rc =
      lc_engine_mgmt_call(client, "POST", "/v1/tc/lease/renew", &body, response,
                          error, lc_engine_parse_tc_lease_renew_response);
  lc_engine_buffer_cleanup(&body);
  return rc;
}

int lc_engine_client_tc_lease_release(
    lc_engine_client *client, const lc_engine_tc_lease_release_request *request,
    lc_engine_tc_lease_release_response *response, lc_engine_error *error) {
  lc_engine_buffer body;
  int first_field;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "tc_lease_release requires client, request, response, and error");
  }
  rc = lc_engine_mgmt_start_body(&body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  first_field = 1;
  if (lc_engine_json_add_string_field(&body, &first_field, "leader_id",
                                      request->leader_id) != LC_ENGINE_OK ||
      request->term > (unsigned long)LONG_MAX ||
      lc_engine_json_add_long_field(&body, &first_field, "term",
                                    (long)request->term) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to build tc lease release body");
  }
  rc = lc_engine_mgmt_finish_body(&body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  rc = lc_engine_mgmt_call(client, "POST", "/v1/tc/lease/release", &body,
                           response, error,
                           lc_engine_parse_tc_lease_release_response);
  lc_engine_buffer_cleanup(&body);
  return rc;
}

int lc_engine_client_tc_leader(lc_engine_client *client,
                               lc_engine_tc_leader_response *response,
                               lc_engine_error *error) {
  if (client == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "tc_leader requires client, response, and error");
  }
  return lc_engine_mgmt_call(client, "GET", "/v1/tc/leader", NULL, response,
                             error, lc_engine_parse_tc_leader_response);
}

int lc_engine_client_tc_cluster_announce(
    lc_engine_client *client,
    const lc_engine_tc_cluster_announce_request *request,
    lc_engine_tc_cluster_response *response, lc_engine_error *error) {
  lc_engine_buffer body;
  int first_field;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL ||
      request->self_endpoint == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "tc_cluster_announce requires client, request, response, error, and "
        "self_endpoint");
  }
  rc = lc_engine_mgmt_start_body(&body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  first_field = 1;
  if (lc_engine_json_add_string_field(&body, &first_field, "self_endpoint",
                                      request->self_endpoint) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add self_endpoint");
  }
  rc = lc_engine_mgmt_finish_body(&body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  rc =
      lc_engine_mgmt_call(client, "POST", "/v1/tc/cluster/announce", &body,
                          response, error, lc_engine_parse_tc_cluster_response);
  lc_engine_buffer_cleanup(&body);
  return rc;
}

int lc_engine_client_tc_cluster_leave(lc_engine_client *client,
                                      lc_engine_tc_cluster_response *response,
                                      lc_engine_error *error) {
  lc_engine_buffer body;

  if (client == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "tc_cluster_leave requires client, response, and error");
  }
  lc_engine_buffer_init(&body);
  return lc_engine_mgmt_call(client, "POST", "/v1/tc/cluster/leave", &body,
                             response, error,
                             lc_engine_parse_tc_cluster_response);
}

int lc_engine_client_tc_cluster_list(lc_engine_client *client,
                                     lc_engine_tc_cluster_response *response,
                                     lc_engine_error *error) {
  if (client == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "tc_cluster_list requires client, response, and error");
  }
  return lc_engine_mgmt_call(client, "GET", "/v1/tc/cluster/list", NULL,
                             response, error,
                             lc_engine_parse_tc_cluster_response);
}

int lc_engine_client_tcrm_register(
    lc_engine_client *client, const lc_engine_tcrm_register_request *request,
    lc_engine_tcrm_register_response *response, lc_engine_error *error) {
  lc_engine_buffer body;
  int first_field;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "tcrm_register requires client, request, response, and error");
  }
  rc = lc_engine_mgmt_start_body(&body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  first_field = 1;
  if (lc_engine_json_add_string_field(&body, &first_field, "backend_hash",
                                      request->backend_hash) != LC_ENGINE_OK ||
      lc_engine_json_add_string_field(&body, &first_field, "endpoint",
                                      request->endpoint) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to build tcrm register body");
  }
  rc = lc_engine_mgmt_finish_body(&body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  rc =
      lc_engine_mgmt_call(client, "POST", "/v1/tc/rm/register", &body, response,
                          error, lc_engine_parse_tcrm_register_response);
  lc_engine_buffer_cleanup(&body);
  return rc;
}

int lc_engine_client_tcrm_unregister(
    lc_engine_client *client, const lc_engine_tcrm_unregister_request *request,
    lc_engine_tcrm_unregister_response *response, lc_engine_error *error) {
  lc_engine_buffer body;
  int first_field;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "tcrm_unregister requires client, request, response, and error");
  }
  rc = lc_engine_mgmt_start_body(&body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  first_field = 1;
  if (lc_engine_json_add_string_field(&body, &first_field, "backend_hash",
                                      request->backend_hash) != LC_ENGINE_OK ||
      lc_engine_json_add_string_field(&body, &first_field, "endpoint",
                                      request->endpoint) != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to build tcrm unregister body");
  }
  rc = lc_engine_mgmt_finish_body(&body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  rc = lc_engine_mgmt_call(
      client, "POST", "/v1/tc/rm/unregister", &body, response, error,
      (int (*)(const lc_engine_http_result *, void *,
               lc_engine_error *))lc_engine_parse_tcrm_register_response);
  lc_engine_buffer_cleanup(&body);
  return rc;
}

int lc_engine_client_tcrm_list(lc_engine_client *client,
                               lc_engine_tcrm_list_response *response,
                               lc_engine_error *error) {
  if (client == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "tcrm_list requires client, response, and error");
  }
  return lc_engine_mgmt_call(client, "GET", "/v1/tc/rm/list", NULL, response,
                             error, lc_engine_parse_tcrm_list_response);
}

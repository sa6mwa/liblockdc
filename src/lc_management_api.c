#include "lc_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <yajl/yajl_gen.h>
#include <yajl/yajl_tree.h>

static int lc_engine_mgmt_gen_ok(yajl_gen_status status) {
  return status == yajl_gen_status_ok;
}

static int lc_engine_mgmt_gen_key(yajl_gen gen, const char *key) {
  return lc_engine_mgmt_gen_ok(
      yajl_gen_string(gen, (const unsigned char *)key, strlen(key)));
}

static int lc_engine_mgmt_gen_string_field(yajl_gen gen, const char *key,
                                           const char *value) {
  if (!lc_engine_mgmt_gen_key(gen, key)) {
    return 0;
  }
  return lc_engine_mgmt_gen_ok(
      yajl_gen_string(gen, (const unsigned char *)value, strlen(value)));
}

static int lc_engine_mgmt_gen_long_field(yajl_gen gen, const char *key,
                                         unsigned long value) {
  char scratch[32];
  int written;

  if (!lc_engine_mgmt_gen_key(gen, key)) {
    return 0;
  }
  written = snprintf(scratch, sizeof(scratch), "%lu", value);
  if (written < 0 || (size_t)written >= sizeof(scratch)) {
    return 0;
  }
  return lc_engine_mgmt_gen_ok(yajl_gen_number(gen, scratch, (size_t)written));
}

static int lc_engine_mgmt_parse_long_number(yajl_val value, const char *label,
                                            long *out_value,
                                            lc_engine_error *error) {
  if (value == NULL) {
    return LC_ENGINE_OK;
  }
  if (!lc_parse_long_base10_checked(YAJL_GET_NUMBER(value), out_value)) {
    return lc_engine_set_protocol_error(error, label);
  }
  return LC_ENGINE_OK;
}

static int lc_engine_mgmt_parse_ulong_number(yajl_val value, const char *label,
                                             unsigned long *out_value,
                                             lc_engine_error *error) {
  if (value == NULL) {
    return LC_ENGINE_OK;
  }
  if (!lc_parse_ulong_base10_checked(YAJL_GET_NUMBER(value), out_value)) {
    return lc_engine_set_protocol_error(error, label);
  }
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

static int lc_engine_mgmt_finish_body(yajl_gen gen, lc_engine_buffer *buffer,
                                      lc_engine_error *error) {
  const unsigned char *data;
  size_t length;

  if (!lc_engine_mgmt_gen_ok(yajl_gen_map_close(gen))) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(buffer);
    return lc_engine_set_protocol_error(
        error, "failed to finish management request body");
  }
  yajl_gen_get_buf(gen, &data, &length);
  if (lc_engine_buffer_append(buffer, (const char *)data, length) !=
      LC_ENGINE_OK) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(buffer);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to copy management request body");
  }
  yajl_gen_free(gen);
  return LC_ENGINE_OK;
}

static int lc_engine_mgmt_start_body(yajl_gen *out_gen,
                                     lc_engine_buffer *buffer,
                                     lc_engine_error *error) {
  yajl_gen gen = NULL;

  lc_engine_buffer_init(buffer);
  gen = yajl_gen_alloc(NULL);
  if (gen == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to allocate YAJL generator");
  }
  if (!lc_engine_mgmt_gen_ok(yajl_gen_map_open(gen))) {
    yajl_gen_free(gen);
    return lc_engine_set_protocol_error(
        error, "failed to open management request body");
  }
  *out_gen = gen;
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

static int lc_engine_mgmt_parse_tree(const lc_engine_http_result *result,
                                     yajl_val *out_root,
                                     lc_engine_error *error) {
  char errbuf[128];
  yajl_val root = NULL;

  errbuf[0] = '\0';
  root = yajl_tree_parse(result->body.data, errbuf, sizeof(errbuf));
  if (root == NULL) {
    if (errbuf[0] != '\0') {
      return lc_engine_set_protocol_error(error, errbuf);
    }
    return lc_engine_set_protocol_error(
        error, "failed to parse management response JSON");
  }
  *out_root = root;
  return LC_ENGINE_OK;
}

static int lc_engine_mgmt_dup_tree_string(yajl_val root,
                                          const char *const *path,
                                          char **out_value) {
  yajl_val value;

  value = yajl_tree_get(root, (const char **)path, yajl_t_string);
  if (value == NULL || YAJL_GET_STRING(value) == NULL) {
    return 1;
  }
  *out_value = lc_engine_strdup_local(YAJL_GET_STRING(value));
  return *out_value != NULL;
}

static void
lc_engine_mgmt_capture_correlation(const lc_engine_http_result *result,
                                   char **out_value) {
  if (result->correlation_id != NULL && *out_value == NULL) {
    *out_value = lc_engine_strdup_local(result->correlation_id);
  }
}

static int lc_engine_mgmt_parse_string_array(yajl_val root,
                                             const char *field_name,
                                             lc_engine_string_array *array) {
  const char *path[2];
  yajl_val value;
  size_t index;

  path[0] = field_name;
  path[1] = NULL;
  value = yajl_tree_get(root, path, yajl_t_array);
  if (value == NULL || value->u.array.len == 0U) {
    array->items = NULL;
    array->count = 0U;
    return 1;
  }
  array->items = (char **)calloc(value->u.array.len, sizeof(char *));
  if (array->items == NULL) {
    return 0;
  }
  array->count = value->u.array.len;
  for (index = 0U; index < value->u.array.len; ++index) {
    if (YAJL_IS_STRING(value->u.array.values[index])) {
      array->items[index] =
          lc_engine_strdup_local(YAJL_GET_STRING(value->u.array.values[index]));
      if (array->items[index] == NULL) {
        return 0;
      }
    }
  }
  return 1;
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
  yajl_val root = NULL;
  static const char *path_namespace[] = {"namespace", NULL};
  static const char *path_preferred[] = {"query", "preferred_engine", NULL};
  static const char *path_fallback[] = {"query", "fallback_engine", NULL};
  int rc;

  response = (lc_engine_namespace_config_response *)out_response;
  rc = lc_engine_mgmt_parse_tree(result, &root, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (!lc_engine_mgmt_dup_tree_string(root, path_namespace,
                                      &response->namespace_name) ||
      !lc_engine_mgmt_dup_tree_string(root, path_preferred,
                                      &response->preferred_engine) ||
      !lc_engine_mgmt_dup_tree_string(root, path_fallback,
                                      &response->fallback_engine)) {
    yajl_tree_free(root);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to allocate namespace response");
  }
  lc_engine_mgmt_capture_correlation(result, &response->correlation_id);
  yajl_tree_free(root);
  return LC_ENGINE_OK;
}

static int
lc_engine_parse_index_flush_response(const lc_engine_http_result *result,
                                     void *out_response,
                                     lc_engine_error *error) {
  lc_engine_index_flush_response *response;
  yajl_val root = NULL;
  static const char *path_namespace[] = {"namespace", NULL};
  static const char *path_mode[] = {"mode", NULL};
  static const char *path_flush_id[] = {"flush_id", NULL};
  static const char *path_accepted[] = {"accepted", NULL};
  static const char *path_flushed[] = {"flushed", NULL};
  static const char *path_pending[] = {"pending", NULL};
  static const char *path_index_seq[] = {"index_seq", NULL};
  yajl_val value;
  int rc;

  response = (lc_engine_index_flush_response *)out_response;
  rc = lc_engine_mgmt_parse_tree(result, &root, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (!lc_engine_mgmt_dup_tree_string(root, path_namespace,
                                      &response->namespace_name) ||
      !lc_engine_mgmt_dup_tree_string(root, path_mode, &response->mode) ||
      !lc_engine_mgmt_dup_tree_string(root, path_flush_id,
                                      &response->flush_id)) {
    yajl_tree_free(root);
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_NO_MEMORY,
        "failed to allocate index flush response");
  }
  value = yajl_tree_get(root, path_accepted, yajl_t_true);
  response->accepted = value != NULL;
  value = yajl_tree_get(root, path_flushed, yajl_t_true);
  response->flushed = value != NULL;
  value = yajl_tree_get(root, path_pending, yajl_t_true);
  response->pending = value != NULL;
  value = yajl_tree_get(root, path_index_seq, yajl_t_number);
  rc = lc_engine_mgmt_parse_ulong_number(
      value, "index_flush index_seq is out of range", &response->index_seq,
      error);
  if (rc != LC_ENGINE_OK) {
    yajl_tree_free(root);
    return rc;
  }
  lc_engine_mgmt_capture_correlation(result, &response->correlation_id);
  yajl_tree_free(root);
  return LC_ENGINE_OK;
}

static int lc_engine_parse_txn_response(const lc_engine_http_result *result,
                                        void *out_response,
                                        lc_engine_error *error) {
  lc_engine_txn_replay_response *response;
  yajl_val root = NULL;
  static const char *path_txn_id[] = {"txn_id", NULL};
  static const char *path_state[] = {"state", NULL};
  int rc;

  response = (lc_engine_txn_replay_response *)out_response;
  rc = lc_engine_mgmt_parse_tree(result, &root, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (!lc_engine_mgmt_dup_tree_string(root, path_txn_id, &response->txn_id) ||
      !lc_engine_mgmt_dup_tree_string(root, path_state, &response->state)) {
    yajl_tree_free(root);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to allocate txn response");
  }
  lc_engine_mgmt_capture_correlation(result, &response->correlation_id);
  yajl_tree_free(root);
  return LC_ENGINE_OK;
}

static int
lc_engine_parse_tc_lease_acquire_response(const lc_engine_http_result *result,
                                          void *out_response,
                                          lc_engine_error *error) {
  lc_engine_tc_lease_acquire_response *response;
  yajl_val root = NULL;
  static const char *path_granted[] = {"granted", NULL};
  static const char *path_leader_id[] = {"leader_id", NULL};
  static const char *path_leader_endpoint[] = {"leader_endpoint", NULL};
  static const char *path_term[] = {"term", NULL};
  static const char *path_expires_at[] = {"expires_at", NULL};
  yajl_val value;
  int rc;

  response = (lc_engine_tc_lease_acquire_response *)out_response;
  rc = lc_engine_mgmt_parse_tree(result, &root, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (!lc_engine_mgmt_dup_tree_string(root, path_leader_id,
                                      &response->leader_id) ||
      !lc_engine_mgmt_dup_tree_string(root, path_leader_endpoint,
                                      &response->leader_endpoint)) {
    yajl_tree_free(root);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to allocate tc lease response");
  }
  value = yajl_tree_get(root, path_granted, yajl_t_true);
  response->granted = value != NULL;
  value = yajl_tree_get(root, path_term, yajl_t_number);
  rc = lc_engine_mgmt_parse_ulong_number(value, "tc lease term is out of range",
                                         &response->term, error);
  if (rc != LC_ENGINE_OK) {
    yajl_tree_free(root);
    return rc;
  }
  value = yajl_tree_get(root, path_expires_at, yajl_t_number);
  rc = lc_engine_mgmt_parse_long_number(value,
                                        "tc lease expires_at is out of range",
                                        &response->expires_at_unix, error);
  if (rc != LC_ENGINE_OK) {
    yajl_tree_free(root);
    return rc;
  }
  lc_engine_mgmt_capture_correlation(result, &response->correlation_id);
  yajl_tree_free(root);
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
  yajl_val root = NULL;
  static const char *path_released[] = {"released", NULL};
  yajl_val value;
  int rc;

  response = (lc_engine_tc_lease_release_response *)out_response;
  rc = lc_engine_mgmt_parse_tree(result, &root, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  value = yajl_tree_get(root, path_released, yajl_t_true);
  response->released = value != NULL;
  lc_engine_mgmt_capture_correlation(result, &response->correlation_id);
  yajl_tree_free(root);
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
  yajl_val root = NULL;
  static const char *path_updated[] = {"updated_at_unix", NULL};
  static const char *path_expires[] = {"expires_at_unix", NULL};
  yajl_val value;
  int rc;

  response = (lc_engine_tc_cluster_response *)out_response;
  rc = lc_engine_mgmt_parse_tree(result, &root, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (!lc_engine_mgmt_parse_string_array(root, "endpoints",
                                         &response->endpoints)) {
    yajl_tree_free(root);
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_NO_MEMORY,
        "failed to allocate tc cluster endpoints");
  }
  value = yajl_tree_get(root, path_updated, yajl_t_number);
  rc = lc_engine_mgmt_parse_long_number(
      value, "tc cluster updated_at_unix is out of range",
      &response->updated_at_unix, error);
  if (rc != LC_ENGINE_OK) {
    yajl_tree_free(root);
    return rc;
  }
  value = yajl_tree_get(root, path_expires, yajl_t_number);
  rc = lc_engine_mgmt_parse_long_number(
      value, "tc cluster expires_at_unix is out of range",
      &response->expires_at_unix, error);
  if (rc != LC_ENGINE_OK) {
    yajl_tree_free(root);
    return rc;
  }
  lc_engine_mgmt_capture_correlation(result, &response->correlation_id);
  yajl_tree_free(root);
  return LC_ENGINE_OK;
}

static int
lc_engine_parse_tcrm_register_response(const lc_engine_http_result *result,
                                       void *out_response,
                                       lc_engine_error *error) {
  lc_engine_tcrm_register_response *response;
  yajl_val root = NULL;
  static const char *path_backend_hash[] = {"backend_hash", NULL};
  static const char *path_updated[] = {"updated_at_unix", NULL};
  yajl_val value;
  int rc;

  response = (lc_engine_tcrm_register_response *)out_response;
  rc = lc_engine_mgmt_parse_tree(result, &root, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (!lc_engine_mgmt_dup_tree_string(root, path_backend_hash,
                                      &response->backend_hash) ||
      !lc_engine_mgmt_parse_string_array(root, "endpoints",
                                         &response->endpoints)) {
    yajl_tree_free(root);
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_NO_MEMORY,
        "failed to allocate tcrm register response");
  }
  value = yajl_tree_get(root, path_updated, yajl_t_number);
  rc = lc_engine_mgmt_parse_long_number(
      value, "tcrm register updated_at_unix is out of range",
      &response->updated_at_unix, error);
  if (rc != LC_ENGINE_OK) {
    yajl_tree_free(root);
    return rc;
  }
  lc_engine_mgmt_capture_correlation(result, &response->correlation_id);
  yajl_tree_free(root);
  return LC_ENGINE_OK;
}

static int
lc_engine_parse_tcrm_list_response(const lc_engine_http_result *result,
                                   void *out_response, lc_engine_error *error) {
  lc_engine_tcrm_list_response *response;
  yajl_val root = NULL;
  static const char *path_backends[] = {"backends", NULL};
  static const char *path_updated[] = {"updated_at_unix", NULL};
  yajl_val backends;
  yajl_val value;
  size_t index;
  int rc;

  response = (lc_engine_tcrm_list_response *)out_response;
  rc = lc_engine_mgmt_parse_tree(result, &root, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  backends = yajl_tree_get(root, path_backends, yajl_t_array);
  if (backends != NULL && backends->u.array.len > 0U) {
    response->backends = (lc_engine_tcrm_backend *)calloc(
        backends->u.array.len, sizeof(lc_engine_tcrm_backend));
    if (response->backends == NULL) {
      yajl_tree_free(root);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to allocate tcrm backend list");
    }
    response->backend_count = backends->u.array.len;
    for (index = 0U; index < backends->u.array.len; ++index) {
      yajl_val backend;
      static const char *path_backend_hash[] = {"backend_hash", NULL};
      static const char *path_backend_updated[] = {"updated_at_unix", NULL};

      backend = backends->u.array.values[index];
      if (!lc_engine_mgmt_dup_tree_string(
              backend, path_backend_hash,
              &response->backends[index].backend_hash) ||
          !lc_engine_mgmt_parse_string_array(
              backend, "endpoints", &response->backends[index].endpoints)) {
        yajl_tree_free(root);
        return lc_engine_set_client_error(
            error, LC_ENGINE_ERROR_NO_MEMORY,
            "failed to allocate tcrm backend item");
      }
      value = yajl_tree_get(backend, path_backend_updated, yajl_t_number);
      rc = lc_engine_mgmt_parse_long_number(
          value, "tcrm backend updated_at_unix is out of range",
          &response->backends[index].updated_at_unix, error);
      if (rc != LC_ENGINE_OK) {
        yajl_tree_free(root);
        return rc;
      }
    }
  }
  value = yajl_tree_get(root, path_updated, yajl_t_number);
  rc = lc_engine_mgmt_parse_long_number(
      value, "tcrm list updated_at_unix is out of range",
      &response->updated_at_unix, error);
  if (rc != LC_ENGINE_OK) {
    yajl_tree_free(root);
    return rc;
  }
  lc_engine_mgmt_capture_correlation(result, &response->correlation_id);
  yajl_tree_free(root);
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
  yajl_gen gen = NULL;
  lc_engine_buffer body;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "update_namespace_config requires "
                                      "client, request, response, and error");
  }
  rc = lc_engine_mgmt_start_body(&gen, &body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (request->namespace_name != NULL && request->namespace_name[0] != '\0' &&
      !lc_engine_mgmt_gen_string_field(gen, "namespace",
                                       request->namespace_name)) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add namespace name");
  }
  if (request->preferred_engine != NULL || request->fallback_engine != NULL) {
    if (!lc_engine_mgmt_gen_key(gen, "query") ||
        !lc_engine_mgmt_gen_ok(yajl_gen_map_open(gen))) {
      yajl_gen_free(gen);
      lc_engine_buffer_cleanup(&body);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to add namespace query config");
    }
    if (request->preferred_engine != NULL &&
        request->preferred_engine[0] != '\0' &&
        !lc_engine_mgmt_gen_string_field(gen, "preferred_engine",
                                         request->preferred_engine)) {
      yajl_gen_free(gen);
      lc_engine_buffer_cleanup(&body);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to add preferred engine");
    }
    if (request->fallback_engine != NULL &&
        request->fallback_engine[0] != '\0' &&
        !lc_engine_mgmt_gen_string_field(gen, "fallback_engine",
                                         request->fallback_engine)) {
      yajl_gen_free(gen);
      lc_engine_buffer_cleanup(&body);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to add fallback engine");
    }
    if (!lc_engine_mgmt_gen_ok(yajl_gen_map_close(gen))) {
      yajl_gen_free(gen);
      lc_engine_buffer_cleanup(&body);
      return lc_engine_set_protocol_error(
          error, "failed to close namespace query config");
    }
  }
  rc = lc_engine_mgmt_finish_body(gen, &body, error);
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
  yajl_gen gen = NULL;
  lc_engine_buffer body;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "index_flush requires client, request, response, and error");
  }
  rc = lc_engine_mgmt_start_body(&gen, &body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (request->namespace_name != NULL && request->namespace_name[0] != '\0' &&
      !lc_engine_mgmt_gen_string_field(gen, "namespace",
                                       request->namespace_name)) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add flush namespace");
  }
  if (request->mode != NULL && request->mode[0] != '\0' &&
      !lc_engine_mgmt_gen_string_field(gen, "mode", request->mode)) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add flush mode");
  }
  rc = lc_engine_mgmt_finish_body(gen, &body, error);
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
  yajl_gen gen = NULL;
  lc_engine_buffer body;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL ||
      request->txn_id == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "txn_replay requires client, request, response, error, and txn_id");
  }
  rc = lc_engine_mgmt_start_body(&gen, &body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (!lc_engine_mgmt_gen_string_field(gen, "txn_id", request->txn_id)) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add txn_id");
  }
  rc = lc_engine_mgmt_finish_body(gen, &body, error);
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
  yajl_gen gen = NULL;
  size_t index;
  int rc;

  rc = lc_engine_mgmt_start_body(&gen, body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (!lc_engine_mgmt_gen_string_field(gen, "txn_id", request->txn_id) ||
      !lc_engine_mgmt_gen_string_field(gen, "state", request->state)) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add txn decision basics");
  }
  if (request->participant_count > 0U && request->participants != NULL) {
    if (!lc_engine_mgmt_gen_key(gen, "participants") ||
        !lc_engine_mgmt_gen_ok(yajl_gen_array_open(gen))) {
      yajl_gen_free(gen);
      lc_engine_buffer_cleanup(body);
      return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                        "failed to open participants array");
    }
    for (index = 0U; index < request->participant_count; ++index) {
      if (!lc_engine_mgmt_gen_ok(yajl_gen_map_open(gen)) ||
          !lc_engine_mgmt_gen_string_field(
              gen, "namespace", request->participants[index].namespace_name) ||
          !lc_engine_mgmt_gen_string_field(gen, "key",
                                           request->participants[index].key)) {
        yajl_gen_free(gen);
        lc_engine_buffer_cleanup(body);
        return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                          "failed to add txn participant");
      }
      if (request->participants[index].backend_hash != NULL &&
          request->participants[index].backend_hash[0] != '\0' &&
          !lc_engine_mgmt_gen_string_field(
              gen, "backend_hash", request->participants[index].backend_hash)) {
        yajl_gen_free(gen);
        lc_engine_buffer_cleanup(body);
        return lc_engine_set_client_error(
            error, LC_ENGINE_ERROR_NO_MEMORY,
            "failed to add txn participant backend_hash");
      }
      if (!lc_engine_mgmt_gen_ok(yajl_gen_map_close(gen))) {
        yajl_gen_free(gen);
        lc_engine_buffer_cleanup(body);
        return lc_engine_set_protocol_error(error,
                                            "failed to close txn participant");
      }
    }
    if (!lc_engine_mgmt_gen_ok(yajl_gen_array_close(gen))) {
      yajl_gen_free(gen);
      lc_engine_buffer_cleanup(body);
      return lc_engine_set_protocol_error(error,
                                          "failed to close participants array");
    }
  }
  if (request->expires_at_unix > 0L &&
      !lc_engine_mgmt_gen_long_field(gen, "expires_at_unix",
                                     (unsigned long)request->expires_at_unix)) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add txn expires_at_unix");
  }
  if (request->tc_term > 0UL &&
      !lc_engine_mgmt_gen_long_field(gen, "tc_term", request->tc_term)) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add txn tc_term");
  }
  if (request->target_backend_hash != NULL &&
      request->target_backend_hash[0] != '\0' &&
      !lc_engine_mgmt_gen_string_field(gen, "target_backend_hash",
                                       request->target_backend_hash)) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add txn target_backend_hash");
  }
  return lc_engine_mgmt_finish_body(gen, body, error);
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
  yajl_gen gen = NULL;
  lc_engine_buffer body;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "tc_lease_acquire requires client, request, response, and error");
  }
  rc = lc_engine_mgmt_start_body(&gen, &body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (!lc_engine_mgmt_gen_string_field(gen, "candidate_id",
                                       request->candidate_id) ||
      !lc_engine_mgmt_gen_string_field(gen, "candidate_endpoint",
                                       request->candidate_endpoint) ||
      !lc_engine_mgmt_gen_long_field(gen, "term", request->term) ||
      !lc_engine_mgmt_gen_long_field(gen, "ttl_ms",
                                     (unsigned long)request->ttl_ms)) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to build tc lease acquire body");
  }
  rc = lc_engine_mgmt_finish_body(gen, &body, error);
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
  yajl_gen gen = NULL;
  lc_engine_buffer body;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "tc_lease_renew requires client, request, response, and error");
  }
  rc = lc_engine_mgmt_start_body(&gen, &body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (!lc_engine_mgmt_gen_string_field(gen, "leader_id", request->leader_id) ||
      !lc_engine_mgmt_gen_long_field(gen, "term", request->term) ||
      !lc_engine_mgmt_gen_long_field(gen, "ttl_ms",
                                     (unsigned long)request->ttl_ms)) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to build tc lease renew body");
  }
  rc = lc_engine_mgmt_finish_body(gen, &body, error);
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
  yajl_gen gen = NULL;
  lc_engine_buffer body;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "tc_lease_release requires client, request, response, and error");
  }
  rc = lc_engine_mgmt_start_body(&gen, &body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (!lc_engine_mgmt_gen_string_field(gen, "leader_id", request->leader_id) ||
      !lc_engine_mgmt_gen_long_field(gen, "term", request->term)) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to build tc lease release body");
  }
  rc = lc_engine_mgmt_finish_body(gen, &body, error);
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
  yajl_gen gen = NULL;
  lc_engine_buffer body;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL ||
      request->self_endpoint == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "tc_cluster_announce requires client, request, response, error, and "
        "self_endpoint");
  }
  rc = lc_engine_mgmt_start_body(&gen, &body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (!lc_engine_mgmt_gen_string_field(gen, "self_endpoint",
                                       request->self_endpoint)) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to add self_endpoint");
  }
  rc = lc_engine_mgmt_finish_body(gen, &body, error);
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
  yajl_gen gen = NULL;
  lc_engine_buffer body;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "tcrm_register requires client, request, response, and error");
  }
  rc = lc_engine_mgmt_start_body(&gen, &body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (!lc_engine_mgmt_gen_string_field(gen, "backend_hash",
                                       request->backend_hash) ||
      !lc_engine_mgmt_gen_string_field(gen, "endpoint", request->endpoint)) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to build tcrm register body");
  }
  rc = lc_engine_mgmt_finish_body(gen, &body, error);
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
  yajl_gen gen = NULL;
  lc_engine_buffer body;
  int rc;

  if (client == NULL || request == NULL || response == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "tcrm_unregister requires client, request, response, and error");
  }
  rc = lc_engine_mgmt_start_body(&gen, &body, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (!lc_engine_mgmt_gen_string_field(gen, "backend_hash",
                                       request->backend_hash) ||
      !lc_engine_mgmt_gen_string_field(gen, "endpoint", request->endpoint)) {
    yajl_gen_free(gen);
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to build tcrm unregister body");
  }
  rc = lc_engine_mgmt_finish_body(gen, &body, error);
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

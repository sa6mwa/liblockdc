#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include <cmocka.h>

#include "lc/lc.h"
#include "lc_api_internal.h"

static char *dup_cstr(const char *value) {
  size_t len;
  char *copy;

  if (value == NULL) {
    return NULL;
  }
  len = strlen(value);
  copy = (char *)malloc(len + 1U);
  if (copy != NULL) {
    memcpy(copy, value, len + 1U);
  }
  return copy;
}

static void test_client_config_init_sets_expected_defaults(void **state) {
  lc_client_config config;

  (void)state;
  memset(&config, 0xAB, sizeof(config));
  lc_client_config_init(&config);

  assert_null(config.endpoints);
  assert_int_equal(config.endpoint_count, 0U);
  assert_null(config.unix_socket_path);
  assert_null(config.client_bundle_path);
  assert_null(config.default_namespace);
  assert_int_equal(config.timeout_ms, 30000L);
  assert_int_equal(config.disable_mtls, 0);
  assert_int_equal(config.prefer_http_2, 1);
  assert_int_equal(config.http_json_response_limit_bytes, 0U);
}

static void test_allocator_init_clears_allocator(void **state) {
  lc_allocator allocator;

  (void)state;
  memset(&allocator, 0xAB, sizeof(allocator));
  lc_allocator_init(&allocator);

  assert_null(allocator.malloc_fn);
  assert_null(allocator.realloc_fn);
  assert_null(allocator.free_fn);
  assert_null(allocator.context);
}

static void test_error_cleanup_resets_allocated_fields(void **state) {
  lc_error error;

  (void)state;
  memset(&error, 0, sizeof(error));
  error.code = LC_ERR_SERVER;
  error.http_status = 409L;
  error.message = dup_cstr("message");
  error.detail = dup_cstr("detail");
  error.server_code = dup_cstr("server_code");
  error.correlation_id = dup_cstr("corr");

  lc_error_cleanup(&error);

  assert_int_equal(error.code, 0);
  assert_int_equal(error.http_status, 0L);
  assert_null(error.message);
  assert_null(error.detail);
  assert_null(error.server_code);
  assert_null(error.correlation_id);
}

static void test_error_from_legacy_maps_transport_and_fields(void **state) {
  lc_error error;
  lc_engine_error legacy;
  int rc;

  (void)state;
  memset(&legacy, 0, sizeof(legacy));
  lc_error_init(&error);

  legacy.code = LC_ENGINE_ERROR_TRANSPORT;
  legacy.http_status = 503L;
  legacy.message = dup_cstr("transport failed");
  legacy.detail = dup_cstr("detail");
  legacy.server_error_code = dup_cstr("node_passive");
  legacy.correlation_id = dup_cstr("corr-1");

  rc = lc_error_from_legacy(&error, &legacy);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_int_equal(error.code, LC_ERR_TRANSPORT);
  assert_int_equal(error.http_status, 503L);
  assert_string_equal(error.message, "transport failed");
  assert_string_equal(error.detail, "detail");
  assert_string_equal(error.server_code, "node_passive");
  assert_string_equal(error.correlation_id, "corr-1");

  lc_engine_error_cleanup(&legacy);
  lc_error_cleanup(&error);
}

static void test_error_set_duplicates_message_fields(void **state) {
  lc_error error;
  int rc;

  (void)state;
  lc_error_init(&error);

  rc =
      lc_error_set(&error, LC_ERR_SERVER, 418L, "msg", "detail", "srv", "corr");
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.code, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 418L);
  assert_string_equal(error.message, "msg");
  assert_string_equal(error.detail, "detail");
  assert_string_equal(error.server_code, "srv");
  assert_string_equal(error.correlation_id, "corr");

  lc_error_cleanup(&error);
}

static void test_error_set_returns_code_without_error_object(void **state) {
  int rc;

  (void)state;
  rc =
      lc_error_set(NULL, LC_ERR_PROTOCOL, 400L, "msg", "detail", "srv", "corr");
  assert_int_equal(rc, LC_ERR_PROTOCOL);
}

static void
test_error_from_legacy_maps_protocol_and_server_codes(void **state) {
  lc_error error;
  lc_engine_error legacy;
  int rc;

  (void)state;
  memset(&legacy, 0, sizeof(legacy));
  lc_error_init(&error);

  legacy.code = LC_ENGINE_ERROR_PROTOCOL;
  legacy.http_status = 502L;
  legacy.message = dup_cstr("protocol failed");
  rc = lc_error_from_legacy(&error, &legacy);
  assert_int_equal(rc, LC_ERR_PROTOCOL);
  assert_int_equal(error.code, LC_ERR_PROTOCOL);
  assert_int_equal(error.http_status, 502L);
  assert_string_equal(error.message, "protocol failed");
  lc_engine_error_cleanup(&legacy);
  lc_error_cleanup(&error);

  memset(&legacy, 0, sizeof(legacy));
  lc_error_init(&error);
  legacy.code = 999;
  legacy.http_status = 500L;
  legacy.message = dup_cstr("server failed");
  rc = lc_error_from_legacy(&error, &legacy);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.code, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 500L);
  assert_string_equal(error.message, "server failed");
  lc_engine_error_cleanup(&legacy);
  lc_error_cleanup(&error);
}

static void test_dup_bytes_as_text_copies_and_terminates(void **state) {
  const char bytes[] = {'a', 'b', 'c', 'd'};
  char *copy;

  (void)state;
  copy = lc_dup_bytes_as_text(bytes, sizeof(bytes));
  assert_non_null(copy);
  assert_memory_equal(copy, bytes, sizeof(bytes));
  assert_int_equal(copy[sizeof(bytes)], '\0');
  free(copy);
}

static void test_attachment_info_copy_deep_copies_fields(void **state) {
  lc_engine_attachment_info legacy;
  lc_attachment_info pub;

  (void)state;
  memset(&legacy, 0, sizeof(legacy));
  memset(&pub, 0, sizeof(pub));
  legacy.id = dup_cstr("att-1");
  legacy.name = dup_cstr("photo.png");
  legacy.size = 42L;
  legacy.plaintext_sha256 = dup_cstr("abc123");
  legacy.content_type = dup_cstr("image/png");
  legacy.created_at_unix = 100L;
  legacy.updated_at_unix = 200L;

  lc_attachment_info_copy(&pub, &legacy);
  assert_string_equal(pub.id, "att-1");
  assert_string_equal(pub.name, "photo.png");
  assert_int_equal(pub.size, 42L);
  assert_string_equal(pub.plaintext_sha256, "abc123");
  assert_string_equal(pub.content_type, "image/png");
  assert_int_equal(pub.created_at_unix, 100L);
  assert_int_equal(pub.updated_at_unix, 200L);
  assert_ptr_not_equal(pub.id, legacy.id);
  assert_ptr_not_equal(pub.name, legacy.name);

  lc_attachment_info_cleanup(&pub);
  lc_engine_attachment_info_cleanup(&legacy);
}

static void test_client_open_rejects_missing_config(void **state) {
  lc_client *client;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  lc_error_init(&error);

  rc = lc_client_open(NULL, &client, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(error.code, LC_ERR_INVALID);
  assert_null(client);

  lc_error_cleanup(&error);
}

static void test_client_open_rejects_empty_endpoints(void **state) {
  lc_client_config config;
  lc_client *client;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  lc_client_config_init(&config);
  lc_error_init(&error);
  config.disable_mtls = 1;

  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(error.code, LC_ERR_INVALID);
  assert_null(client);

  lc_error_cleanup(&error);
}

static void
test_client_open_rejects_missing_bundle_when_mtls_enabled(void **state) {
  static const char *endpoints[] = {"https://lockd.example.invalid"};
  lc_client_config config;
  lc_client *client;
  lc_error error;
  int rc;

  (void)state;
  client = NULL;
  lc_client_config_init(&config);
  lc_error_init(&error);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.disable_mtls = 0;

  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(error.code, LC_ERR_INVALID);
  assert_null(client);

  lc_error_cleanup(&error);
}

static void
test_management_cleanup_helpers_release_nested_allocations(void **state) {
  lc_tc_rm_list_res rm_list;
  lc_tc_cluster_res cluster;
  lc_namespace_config_res ns;

  (void)state;
  memset(&rm_list, 0, sizeof(rm_list));
  memset(&cluster, 0, sizeof(cluster));
  memset(&ns, 0, sizeof(ns));

  rm_list.backends = (lc_tc_rm_backend *)calloc(2U, sizeof(*rm_list.backends));
  assert_non_null(rm_list.backends);
  rm_list.backend_count = 2U;
  rm_list.backends[0].backend_hash = dup_cstr("b1");
  rm_list.backends[0].endpoints.items = (char **)calloc(1U, sizeof(char *));
  rm_list.backends[0].endpoints.count = 1U;
  rm_list.backends[0].endpoints.items[0] = dup_cstr("e1");
  rm_list.backends[1].backend_hash = dup_cstr("b2");
  rm_list.correlation_id = dup_cstr("corr-rm");

  cluster.endpoints.items = (char **)calloc(2U, sizeof(char *));
  cluster.endpoints.count = 2U;
  cluster.endpoints.items[0] = dup_cstr("tcp://a");
  cluster.endpoints.items[1] = dup_cstr("tcp://b");
  cluster.correlation_id = dup_cstr("corr-cluster");

  ns.namespace_name = dup_cstr("default");
  ns.preferred_engine = dup_cstr("index");
  ns.fallback_engine = dup_cstr("scan");
  ns.correlation_id = dup_cstr("corr-ns");

  lc_tc_rm_list_res_cleanup(&rm_list);
  lc_tc_cluster_res_cleanup(&cluster);
  lc_namespace_config_res_cleanup(&ns);

  assert_null(rm_list.backends);
  assert_int_equal(rm_list.backend_count, 0U);
  assert_null(rm_list.correlation_id);
  assert_null(cluster.endpoints.items);
  assert_int_equal(cluster.endpoints.count, 0U);
  assert_null(cluster.correlation_id);
  assert_null(ns.namespace_name);
  assert_null(ns.preferred_engine);
  assert_null(ns.fallback_engine);
  assert_null(ns.correlation_id);
}

static void test_subscribe_meta_builds_queue_state_handle(void **state) {
  static const char json[] =
      "{\"message\":{\"namespace\":\"default\",\"queue\":\"workflow\","
      "\"message_id\":\"msg-123\",\"attempts\":1,\"max_attempts\":5,"
      "\"payload_content_type\":\"application/"
      "json\",\"lease_id\":\"lease-msg\","
      "\"txn_id\":\"txn-msg\",\"meta_etag\":\"meta-etag\","
      "\"state_etag\":\"state-etag\",\"state_lease_id\":\"lease-state\","
      "\"state_lease_expires_at_unix\":1234,"
      "\"state_fencing_token\":7,\"state_txn_id\":\"txn-state\"},"
      "\"next_cursor\":\"cursor-1\"}";
  lc_engine_dequeue_response response;
  lc_engine_error legacy_error;
  lc_source *payload;
  lc_message *message;
  lc_lease *state_lease;
  int rc;

  (void)state;
  memset(&response, 0, sizeof(response));
  memset(&legacy_error, 0, sizeof(legacy_error));
  payload = NULL;
  message = NULL;
  state_lease = NULL;

  rc = lc_engine_parse_subscribe_meta_json(json, "fallback-correlation",
                                           &response, &legacy_error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_string_equal(response.state_lease_id, "lease-state");

  rc = lc_source_from_memory("{}", 2U, &payload, NULL);
  assert_int_equal(rc, LC_OK);
  message = lc_message_new(NULL, &response, payload, NULL);
  assert_non_null(message);

  state_lease = message->state(message);
  assert_non_null(state_lease);
  assert_string_equal(state_lease->lease_id, "lease-state");
  assert_string_equal(state_lease->key, "q/workflow/state/msg-123");
  assert_null(state_lease->state_etag);

  message->close(message);
  lc_engine_dequeue_response_cleanup(&response);
  lc_engine_error_cleanup(&legacy_error);
}

static void
test_subscribe_meta_without_state_has_no_state_handle(void **state) {
  static const char json[] =
      "{\"message\":{\"namespace\":\"default\",\"queue\":\"workflow\","
      "\"message_id\":\"msg-456\",\"attempts\":1,\"max_attempts\":5,"
      "\"payload_content_type\":\"application/json\","
      "\"lease_id\":\"lease-msg\",\"txn_id\":\"txn-msg\","
      "\"meta_etag\":\"meta-etag\"}}";
  lc_engine_dequeue_response response;
  lc_engine_error legacy_error;
  lc_source *payload;
  lc_message *message;
  int rc;

  (void)state;
  memset(&response, 0, sizeof(response));
  memset(&legacy_error, 0, sizeof(legacy_error));
  payload = NULL;
  message = NULL;

  rc = lc_engine_parse_subscribe_meta_json(json, "fallback-correlation",
                                           &response, &legacy_error);
  assert_int_equal(rc, LC_ENGINE_OK);
  rc = lc_source_from_memory("{}", 2U, &payload, NULL);
  assert_int_equal(rc, LC_OK);
  message = lc_message_new(NULL, &response, payload, NULL);
  assert_non_null(message);
  assert_null(message->state(message));

  message->close(message);
  lc_engine_dequeue_response_cleanup(&response);
  lc_engine_error_cleanup(&legacy_error);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_client_config_init_sets_expected_defaults),
      cmocka_unit_test(test_allocator_init_clears_allocator),
      cmocka_unit_test(test_error_cleanup_resets_allocated_fields),
      cmocka_unit_test(test_error_from_legacy_maps_transport_and_fields),
      cmocka_unit_test(test_error_set_duplicates_message_fields),
      cmocka_unit_test(test_error_set_returns_code_without_error_object),
      cmocka_unit_test(test_error_from_legacy_maps_protocol_and_server_codes),
      cmocka_unit_test(test_dup_bytes_as_text_copies_and_terminates),
      cmocka_unit_test(test_attachment_info_copy_deep_copies_fields),
      cmocka_unit_test(test_client_open_rejects_missing_config),
      cmocka_unit_test(test_client_open_rejects_empty_endpoints),
      cmocka_unit_test(
          test_client_open_rejects_missing_bundle_when_mtls_enabled),
      cmocka_unit_test(
          test_management_cleanup_helpers_release_nested_allocations),
      cmocka_unit_test(test_subscribe_meta_builds_queue_state_handle),
      cmocka_unit_test(test_subscribe_meta_without_state_has_no_state_handle),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}

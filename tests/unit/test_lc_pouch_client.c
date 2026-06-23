#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cmocka.h>
#include <sys/types.h>
#include <unistd.h>

#include "lc/lc.h"

typedef struct pouch_value_doc {
  lonejson_int64 value;
} pouch_value_doc;

static const lonejson_field pouch_value_fields[] = {
    LONEJSON_FIELD_I64(pouch_value_doc, value, "value")};

LONEJSON_MAP_DEFINE(pouch_value_map, pouch_value_doc, pouch_value_fields);

static void test_root_path(char *buffer, size_t buffer_size,
                           const char *suffix) {
  snprintf(buffer, buffer_size, "/tmp/liblockdc-pouch-client-%ld-%s",
           (long)getpid(), suffix);
}

static void test_endpoint(char *buffer, size_t buffer_size, const char *root) {
  snprintf(buffer, buffer_size, "pouch://%s", root);
}

static void test_cleanup_root(const char *root) {
  char path[512];

  snprintf(path, sizeof(path), "%s/store.log", root);
  unlink(path);
  snprintf(path, sizeof(path), "%s/writer.lock", root);
  unlink(path);
  rmdir(root);
}

static lc_source *source_from_text(const char *text) {
  lc_source *source;
  lc_error error;
  int rc;

  source = NULL;
  memset(&error, 0, sizeof(error));
  rc = lc_source_from_memory(text, strlen(text), &source, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(source);
  lc_error_cleanup(&error);
  return source;
}

static char *memory_sink_text(lc_sink *sink) {
  const void *bytes;
  size_t length;
  char *text;
  lc_error error;
  int rc;

  memset(&error, 0, sizeof(error));
  bytes = NULL;
  length = 0U;
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  text = (char *)malloc(length + 1U);
  assert_non_null(text);
  memcpy(text, bytes, length);
  text[length] = '\0';
  lc_error_cleanup(&error);
  return text;
}

static lc_client *open_pouch_client_with_namespace(const char *endpoint,
                                                   const char *namespace_name) {
  lc_client_config config;
  lc_client *client;
  lc_error error;
  const char *endpoints[1];
  int rc;

  memset(&error, 0, sizeof(error));
  lc_client_config_init(&config);
  endpoints[0] = endpoint;
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = namespace_name;
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(client);
  lc_error_cleanup(&error);
  return client;
}

static lc_client *open_pouch_client_with_limit(const char *endpoint,
                                               size_t json_limit) {
  lc_client_config config;
  lc_client *client;
  lc_error error;
  const char *endpoints[1];
  int rc;

  memset(&error, 0, sizeof(error));
  lc_client_config_init(&config);
  endpoints[0] = endpoint;
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "default";
  config.http_json_response_limit_bytes = json_limit;
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(client);
  lc_error_cleanup(&error);
  return client;
}

static lc_client *open_pouch_client(const char *endpoint) {
  return open_pouch_client_with_namespace(endpoint, "default");
}

typedef struct subscribe_test_state {
  size_t handled;
  const char *expected[2];
} subscribe_test_state;

typedef struct consumer_service_test_state {
  lc_consumer_service *service;
  size_t handled;
  const char *expected[2];
} consumer_service_test_state;

typedef struct consumer_service_stateful_test_state {
  lc_consumer_service *service;
  size_t handled;
  const char *expected;
} consumer_service_stateful_test_state;

static int subscribe_test_handle(void *context, lc_message *message,
                                 lc_error *error) {
  subscribe_test_state *state;
  lc_sink *sink;
  char *text;
  size_t written;
  int rc;

  state = (subscribe_test_state *)context;
  assert_true(state->handled < 2U);
  sink = NULL;
  rc = lc_sink_to_memory(&sink, error);
  assert_int_equal(rc, LC_OK);
  written = 0U;
  rc = message->write_payload(message, sink, &written, error);
  assert_int_equal(rc, LC_OK);
  text = memory_sink_text(sink);
  assert_string_equal(text, state->expected[state->handled]);
  free(text);
  lc_sink_close(sink);
  ++state->handled;
  rc = message->ack(message, error);
  assert_int_equal(rc, LC_OK);
  return LC_OK;
}

static int consumer_service_test_handle(void *context,
                                        lc_consumer_message *message,
                                        lc_error *error) {
  consumer_service_test_state *state;
  lc_sink *sink;
  char *text;
  size_t written;
  int rc;

  state = (consumer_service_test_state *)context;
  assert_non_null(message);
  assert_non_null(message->message);
  assert_false(message->with_state);
  assert_null(message->state);
  assert_true(state->handled < 2U);
  sink = NULL;
  rc = lc_sink_to_memory(&sink, error);
  assert_int_equal(rc, LC_OK);
  written = 0U;
  rc = message->message->write_payload(message->message, sink, &written, error);
  assert_int_equal(rc, LC_OK);
  text = memory_sink_text(sink);
  assert_string_equal(text, state->expected[state->handled]);
  free(text);
  lc_sink_close(sink);
  ++state->handled;
  if (state->handled == 2U) {
    rc = lc_consumer_service_stop(state->service);
    assert_int_equal(rc, LC_OK);
  }
  return LC_OK;
}

static int consumer_service_stateful_test_handle(void *context,
                                                 lc_consumer_message *message,
                                                 lc_error *error) {
  consumer_service_stateful_test_state *state;
  lc_source *source;
  lc_update_opts opts;
  lc_sink *sink;
  char *text;
  size_t written;
  int rc;

  state = (consumer_service_stateful_test_state *)context;
  assert_non_null(message);
  assert_non_null(message->message);
  assert_true(message->with_state);
  assert_non_null(message->state);
  assert_int_equal(state->handled, 0U);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, error);
  assert_int_equal(rc, LC_OK);
  written = 0U;
  rc = message->message->write_payload(message->message, sink, &written, error);
  assert_int_equal(rc, LC_OK);
  text = memory_sink_text(sink);
  assert_string_equal(text, state->expected);
  free(text);
  lc_sink_close(sink);

  lc_update_opts_init(&opts);
  opts.content_type = "application/json";
  source = source_from_text("{\"managed\":true}");
  rc = message->state->update(message->state, source, &opts, error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  state->handled += 1U;
  rc = lc_consumer_service_stop(state->service);
  assert_int_equal(rc, LC_OK);
  return LC_OK;
}

static void test_pouch_endpoint_lease_state_lifecycle(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_client *second_client;
  lc_lease *lease;
  lc_acquire_req acquire;
  lc_update_opts update_opts;
  lc_get_res get_res;
  lc_describe_req describe_req;
  lc_describe_res describe_res;
  lc_keepalive_req keepalive_req;
  lc_metadata_req metadata_req;
  lc_attach_req attach_req;
  lc_attach_res attach_res;
  lc_attachment_list_req list_req;
  lc_attachment_list attachment_list;
  lc_attachment_get_op get_attachment_req;
  lc_attachment_get_res get_attachment_res;
  lc_attachment_selector attachment_selector;
  lc_attach_res alpha_attach_res;
  lc_attach_res zeta_attach_res;
  lc_attach_res duplicate_attach_res;
  lc_remove_req remove_req;
  lc_release_req release_req;
  lc_source *source;
  lc_sink *sink;
  lc_error error;
  char *text;
  int deleted;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "lifecycle");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&get_res, 0, sizeof(get_res));
  memset(&describe_res, 0, sizeof(describe_res));
  memset(&attach_res, 0, sizeof(attach_res));
  memset(&alpha_attach_res, 0, sizeof(alpha_attach_res));
  memset(&zeta_attach_res, 0, sizeof(zeta_attach_res));
  memset(&duplicate_attach_res, 0, sizeof(duplicate_attach_res));
  memset(&attachment_list, 0, sizeof(attachment_list));
  memset(&get_attachment_res, 0, sizeof(get_attachment_res));
  client = open_pouch_client(endpoint);

  lc_acquire_req_init(&acquire);
  acquire.key = "alpha";
  acquire.owner = "owner-a";
  acquire.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);
  assert_string_equal(lease->namespace_name, "default");
  assert_string_equal(lease->key, "alpha");
  assert_string_equal(lease->owner, "owner-a");
  assert_non_null(lease->lease_id);
  assert_int_equal(lease->fencing_token, 1L);

  source = source_from_text("{\"n\":1}");
  lc_update_opts_init(&update_opts);
  update_opts.content_type = "application/json";
  rc = lease->update(lease, source, &update_opts, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 1L);
  assert_non_null(lease->state_etag);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = lease->get(lease, sink, NULL, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  assert_string_equal(get_res.content_type, "application/json");
  assert_string_equal(get_res.etag, lease->state_etag);
  assert_int_equal(get_res.version, 1L);
  text = memory_sink_text(sink);
  assert_string_equal(text, "{\"n\":1}");
  free(text);
  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);

  lc_describe_req_init(&describe_req);
  describe_req.key = "alpha";
  rc = client->describe(client, &describe_req, &describe_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(describe_res.lease_id, lease->lease_id);
  assert_string_equal(describe_res.state_etag, lease->state_etag);
  assert_int_equal(describe_res.version, lease->version);
  lc_describe_res_cleanup(&describe_res);

  lc_metadata_req_init(&metadata_req);
  metadata_req.has_query_hidden = 1;
  metadata_req.query_hidden = 1;
  metadata_req.has_if_version = 1;
  metadata_req.if_version = lease->version;
  rc = lease->metadata(lease, &metadata_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(lease->has_query_hidden);
  assert_true(lease->query_hidden);

  lc_keepalive_req_init(&keepalive_req);
  keepalive_req.ttl_seconds = 120L;
  rc = lease->keepalive(lease, &keepalive_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(lease->lease_expires_at_unix > 0L);

  lc_attach_req_init(&attach_req);
  attach_req.name = "result.txt";
  attach_req.content_type = "text/plain";
  attach_req.prevent_overwrite = 1;
  attach_req.has_max_bytes = 1;
  attach_req.max_bytes = 64L;
  source = source_from_text("attachment-body");
  rc = lease->attach(lease, &attach_req, source, &attach_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 2L);
  assert_int_equal(attach_res.version, lease->version);
  assert_non_null(attach_res.attachment.id);
  assert_string_equal(attach_res.attachment.name, "result.txt");
  assert_string_equal(attach_res.attachment.content_type, "text/plain");
  assert_int_equal(attach_res.attachment.size, 15L);

  lc_attach_req_init(&attach_req);
  attach_req.name = "zeta.txt";
  attach_req.content_type = "text/plain";
  attach_req.prevent_overwrite = 1;
  source = source_from_text("zeta-body");
  rc = lease->attach(lease, &attach_req, source, &zeta_attach_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 3L);
  assert_string_equal(zeta_attach_res.attachment.name, "zeta.txt");

  lc_attach_req_init(&attach_req);
  attach_req.name = "alpha.txt";
  attach_req.content_type = "text/plain";
  attach_req.prevent_overwrite = 1;
  source = source_from_text("alpha-body");
  rc = lease->attach(lease, &attach_req, source, &alpha_attach_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 4L);
  assert_string_equal(alpha_attach_res.attachment.name, "alpha.txt");

  source = source_from_text("duplicate-body");
  rc = lease->attach(lease, &attach_req, source, &duplicate_attach_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 409L);
  assert_int_equal(lease->version, 4L);
  lc_attach_res_cleanup(&duplicate_attach_res);
  lc_error_cleanup(&error);

  second_client = open_pouch_client(endpoint);
  lc_attachment_list_req_init(&list_req);
  list_req.lease.namespace_name = lease->namespace_name;
  list_req.lease.key = lease->key;
  list_req.lease.lease_id = lease->lease_id;
  list_req.lease.txn_id = lease->txn_id;
  list_req.lease.fencing_token = lease->fencing_token;
  rc = second_client->list_attachments(second_client, &list_req,
                                       &attachment_list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(attachment_list.count, 3U);
  assert_string_equal(attachment_list.items[0].id,
                      alpha_attach_res.attachment.id);
  assert_string_equal(attachment_list.items[0].name, "alpha.txt");
  assert_string_equal(attachment_list.items[1].id, attach_res.attachment.id);
  assert_string_equal(attachment_list.items[1].name, "result.txt");
  assert_string_equal(attachment_list.items[2].id,
                      zeta_attach_res.attachment.id);
  assert_string_equal(attachment_list.items[2].name, "zeta.txt");
  lc_attachment_list_cleanup(&attachment_list);

  lc_attachment_get_op_init(&get_attachment_req);
  get_attachment_req.lease = list_req.lease;
  get_attachment_req.selector.id = alpha_attach_res.attachment.id;
  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = second_client->get_attachment(second_client, &get_attachment_req, sink,
                                     &get_attachment_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(get_attachment_res.attachment.id,
                      alpha_attach_res.attachment.id);
  text = memory_sink_text(sink);
  assert_string_equal(text, "alpha-body");
  free(text);
  lc_sink_close(sink);
  lc_attachment_get_res_cleanup(&get_attachment_res);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = second_client->get(second_client, "alpha", NULL, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  text = memory_sink_text(sink);
  assert_string_equal(text, "{\"n\":1}");
  free(text);
  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
  second_client->close(second_client);

  lc_attachment_selector_init(&attachment_selector);
  attachment_selector.name = "result.txt";
  rc = lease->delete_attachment(lease, &attachment_selector, &deleted, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(deleted);
  assert_int_equal(lease->version, 5L);

  rc = lease->delete_attachment(lease, &attachment_selector, &deleted, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(deleted);
  assert_int_equal(lease->version, 5L);

  second_client = open_pouch_client(endpoint);
  rc = second_client->list_attachments(second_client, &list_req,
                                       &attachment_list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(attachment_list.count, 2U);
  assert_string_equal(attachment_list.items[0].name, "alpha.txt");
  assert_string_equal(attachment_list.items[1].name, "zeta.txt");
  lc_attachment_list_cleanup(&attachment_list);
  second_client->close(second_client);

  rc = lease->delete_all_attachments(lease, &deleted, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(deleted, 2);
  assert_int_equal(lease->version, 6L);

  rc = lease->list_attachments(lease, &attachment_list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(attachment_list.count, 0U);
  lc_attachment_list_cleanup(&attachment_list);

  lc_remove_req_init(&remove_req);
  remove_req.if_state_etag = lease->state_etag;
  rc = lease->remove(lease, &remove_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_null(lease->state_etag);
  assert_int_equal(lease->version, 7L);

  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  client->close(client);
  lc_attach_res_cleanup(&alpha_attach_res);
  lc_attach_res_cleanup(&attach_res);
  lc_attach_res_cleanup(&zeta_attach_res);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_lease_save_uses_mapped_lonejson(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_client *reader_client;
  lc_lease *lease;
  lc_acquire_req acquire;
  lc_get_res get_res;
  lc_release_req release_req;
  lc_sink *sink;
  lc_error error;
  pouch_value_doc value_doc;
  pouch_value_doc loaded_doc;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "mapped-save");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&get_res, 0, sizeof(get_res));
  client = open_pouch_client(endpoint);
  reader_client = NULL;

  lc_acquire_req_init(&acquire);
  acquire.key = "mapped";
  acquire.owner = "owner-a";
  acquire.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  value_doc.value = 7;
  rc = lease->save(lease, &pouch_value_map, &value_doc, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 1L);
  assert_non_null(lease->state_etag);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = lease->get(lease, sink, NULL, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  assert_string_equal(get_res.content_type, "application/json");
  assert_int_equal(get_res.version, 1L);
  text = memory_sink_text(sink);
  assert_string_equal(text, "{\"value\":7}");
  free(text);
  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);

  reader_client = open_pouch_client(endpoint);
  memset(&loaded_doc, 0, sizeof(loaded_doc));
  rc = reader_client->load(reader_client, "mapped", &pouch_value_map,
                           &loaded_doc, NULL, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  assert_string_equal(get_res.content_type, "application/json");
  assert_int_equal(get_res.version, 1L);
  assert_int_equal(loaded_doc.value, 7);
  lc_get_res_cleanup(&get_res);
  reader_client->close(reader_client);
  reader_client = NULL;

  memset(&loaded_doc, 0, sizeof(loaded_doc));
  rc = lease->load(lease, &pouch_value_map, &loaded_doc, NULL, &get_res,
                   &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  assert_string_equal(get_res.content_type, "application/json");
  assert_int_equal(get_res.version, 1L);
  assert_int_equal(loaded_doc.value, 7);
  assert_int_equal(lease->version, 1L);
  lc_get_res_cleanup(&get_res);

  value_doc.value = 8;
  rc = lease->save(lease, &pouch_value_map, &value_doc, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 2L);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = lease->get(lease, sink, NULL, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(get_res.content_type, "application/json");
  assert_int_equal(get_res.version, 2L);
  text = memory_sink_text(sink);
  assert_string_equal(text, "{\"value\":8}");
  free(text);
  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);

  memset(&loaded_doc, 0, sizeof(loaded_doc));
  rc = lease->load(lease, &pouch_value_map, &loaded_doc, NULL, &get_res,
                   &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  assert_int_equal(get_res.version, 2L);
  assert_int_equal(loaded_doc.value, 8);
  assert_int_equal(lease->version, 2L);
  lc_get_res_cleanup(&get_res);

  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_rejects_missing_or_wrong_txn_id(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_acquire_req acquire;
  lc_update_req update_req;
  lc_update_res update_res;
  lc_release_req release_req;
  lc_source *source;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "txn-lease-validation");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&update_res, 0, sizeof(update_res));
  client = open_pouch_client(endpoint);

  lc_acquire_req_init(&acquire);
  acquire.key = "txn-bound";
  acquire.owner = "owner-a";
  acquire.ttl_seconds = 60L;
  acquire.txn_id = "txn-a";
  lease = NULL;
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);
  assert_string_equal(lease->txn_id, "txn-a");

  lc_update_req_init(&update_req);
  update_req.lease.namespace_name = lease->namespace_name;
  update_req.lease.key = lease->key;
  update_req.lease.lease_id = lease->lease_id;
  update_req.lease.fencing_token = lease->fencing_token;
  update_req.content_type = "application/json";
  source = source_from_text("{\"txn\":false}");
  rc = client->update(client, &update_req, source, &update_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 400L);
  assert_string_equal(error.server_code, "missing_txn");
  lc_update_res_cleanup(&update_res);
  lc_error_cleanup(&error);

  update_req.lease.txn_id = "txn-b";
  source = source_from_text("{\"txn\":\"wrong\"}");
  rc = client->update(client, &update_req, source, &update_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 409L);
  assert_string_equal(error.server_code, "txn_mismatch");
  lc_update_res_cleanup(&update_res);
  lc_error_cleanup(&error);

  update_req.lease.txn_id = lease->txn_id;
  source = source_from_text("{\"txn\":true}");
  rc = client->update(client, &update_req, source, &update_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(update_res.new_version, 1L);
  lc_update_res_cleanup(&update_res);

  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_remove_without_state_is_noop(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_lease *reacquired;
  lc_acquire_req acquire_req;
  lc_remove_op remove_op;
  lc_remove_res remove_res;
  lc_update_opts update_opts;
  lc_keepalive_req keepalive_req;
  lc_get_res get_res;
  lc_release_req release_req;
  lc_source *source;
  lc_sink *sink;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "remove-noop");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&remove_res, 0, sizeof(remove_res));
  memset(&get_res, 0, sizeof(get_res));
  client = open_pouch_client(endpoint);
  lease = NULL;
  reacquired = NULL;

  lc_acquire_req_init(&acquire_req);
  acquire_req.key = "empty";
  acquire_req.owner = "owner-a";
  acquire_req.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);
  assert_int_equal(lease->version, 0L);
  assert_null(lease->state_etag);

  lc_remove_op_init(&remove_op);
  remove_op.lease.namespace_name = lease->namespace_name;
  remove_op.lease.key = lease->key;
  remove_op.lease.lease_id = lease->lease_id;
  remove_op.lease.txn_id = lease->txn_id;
  remove_op.lease.fencing_token = lease->fencing_token;
  rc = client->remove(client, &remove_op, &remove_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(remove_res.removed);
  assert_int_equal(remove_res.new_version, 0L);
  lc_remove_res_cleanup(&remove_res);

  lc_keepalive_req_init(&keepalive_req);
  keepalive_req.ttl_seconds = 60L;
  rc = lease->keepalive(lease, &keepalive_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 0L);
  assert_null(lease->state_etag);

  lc_update_opts_init(&update_opts);
  update_opts.content_type = "application/json";
  source = source_from_text("{\"n\":1}");
  rc = lease->update(lease, source, &update_opts, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 1L);
  assert_non_null(lease->state_etag);

  lc_remove_op_init(&remove_op);
  remove_op.lease.namespace_name = lease->namespace_name;
  remove_op.lease.key = lease->key;
  remove_op.lease.lease_id = lease->lease_id;
  remove_op.lease.txn_id = lease->txn_id;
  remove_op.lease.fencing_token = lease->fencing_token;
  remove_op.if_state_etag = lease->state_etag;
  rc = client->remove(client, &remove_op, &remove_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(remove_res.removed);
  assert_int_equal(remove_res.new_version, 2L);
  lc_remove_res_cleanup(&remove_res);

  update_opts.if_state_etag = lease->state_etag;
  source = source_from_text("{\"n\":2}");
  rc = lease->update(lease, source, &update_opts, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 412L);
  lc_error_cleanup(&error);
  assert_int_equal(lease->version, 1L);

  rc = lease->keepalive(lease, &keepalive_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 1L);

  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  lease = NULL;

  acquire_req.owner = "owner-b";
  rc = client->acquire(client, &acquire_req, &reacquired, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(reacquired);
  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = reacquired->get(reacquired, sink, NULL, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(get_res.no_content);
  assert_int_equal(get_res.version, 0L);
  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
  rc = reacquired->release(reacquired, &release_req, &error);
  assert_int_equal(rc, LC_OK);

  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_release_preserves_state_for_reacquire(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *first_lease;
  lc_lease *second_lease;
  lc_acquire_req acquire_req;
  lc_update_opts update_opts;
  lc_metadata_req metadata_req;
  lc_get_res get_res;
  lc_release_req release_req;
  lc_source *source;
  lc_sink *sink;
  lc_error error;
  char *text;
  char *first_etag;
  long first_version;
  long first_fencing_token;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "release-reacquire");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&get_res, 0, sizeof(get_res));
  client = open_pouch_client(endpoint);
  first_lease = NULL;
  second_lease = NULL;
  first_etag = NULL;

  lc_acquire_req_init(&acquire_req);
  acquire_req.key = "persisted";
  acquire_req.owner = "owner-a";
  acquire_req.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire_req, &first_lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(first_lease);
  assert_int_equal(first_lease->fencing_token, 1L);

  lc_update_opts_init(&update_opts);
  update_opts.content_type = "application/json";
  source = source_from_text("{\"released\":true}");
  rc = first_lease->update(first_lease, source, &update_opts, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(first_lease->version, 1L);
  assert_non_null(first_lease->state_etag);

  lc_metadata_req_init(&metadata_req);
  metadata_req.has_query_hidden = 1;
  metadata_req.query_hidden = 1;
  metadata_req.has_if_version = 1;
  metadata_req.if_version = first_lease->version;
  rc = first_lease->metadata(first_lease, &metadata_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(first_lease->query_hidden);

  first_etag = strdup(first_lease->state_etag);
  assert_non_null(first_etag);
  first_version = first_lease->version;
  first_fencing_token = first_lease->fencing_token;

  lc_release_req_init(&release_req);
  rc = first_lease->release(first_lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  first_lease = NULL;

  lc_acquire_req_init(&acquire_req);
  acquire_req.key = "persisted";
  acquire_req.owner = "owner-b";
  acquire_req.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire_req, &second_lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(second_lease);
  assert_string_equal(second_lease->state_etag, first_etag);
  assert_int_equal(second_lease->version, first_version);
  assert_int_equal(second_lease->fencing_token, first_fencing_token + 1L);
  assert_true(second_lease->has_query_hidden);
  assert_true(second_lease->query_hidden);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = second_lease->get(second_lease, sink, NULL, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  assert_string_equal(get_res.etag, first_etag);
  assert_int_equal(get_res.version, first_version);
  text = memory_sink_text(sink);
  assert_string_equal(text, "{\"released\":true}");
  free(text);
  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);

  rc = second_lease->release(second_lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  free(first_etag);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_lease_load_respects_json_limit(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_acquire_req acquire;
  lc_get_res get_res;
  lc_release_req release_req;
  lc_error error;
  pouch_value_doc value_doc;
  pouch_value_doc loaded_doc;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "mapped-load-limit");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&get_res, 0, sizeof(get_res));
  client = open_pouch_client_with_limit(endpoint, 4U);

  lc_acquire_req_init(&acquire);
  acquire.key = "mapped-limit";
  acquire.owner = "owner-a";
  acquire.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  value_doc.value = 7;
  rc = lease->save(lease, &pouch_value_map, &value_doc, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 1L);

  memset(&loaded_doc, 0, sizeof(loaded_doc));
  rc = lease->load(lease, &pouch_value_map, &loaded_doc, NULL, &get_res,
                   &error);
  assert_int_equal(rc, LC_ERR_PROTOCOL);
  assert_int_equal(error.code, LC_ERR_PROTOCOL);
  assert_string_equal(error.message,
                      "mapped state response exceeds configured byte limit");
  assert_int_equal(loaded_doc.value, 0);
  assert_int_equal(lease->version, 1L);
  lc_get_res_cleanup(&get_res);
  lc_error_cleanup(&error);

  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_queue_lifecycle(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_client *second_client;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats_res;
  lc_extend_req extend_req;
  lc_nack_req nack_req;
  lc_message *message;
  lc_sink *sink;
  lc_source *source;
  lc_error error;
  char *text;
  size_t written;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&stats_res, 0, sizeof(stats_res));
  client = open_pouch_client(endpoint);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 60L;
  enqueue_req.ttl_seconds = 3600L;
  enqueue_req.max_attempts = 3;
  source = source_from_text("queue-body");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(enqueue_res.namespace_name, "default");
  assert_string_equal(enqueue_res.queue, "jobs");
  assert_non_null(enqueue_res.message_id);
  assert_int_equal(enqueue_res.payload_bytes, 10L);

  second_client = open_pouch_client(endpoint);
  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "jobs";
  rc =
      second_client->queue_stats(second_client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats_res.available, 1);
  assert_string_equal(stats_res.head_message_id, enqueue_res.message_id);
  lc_queue_stats_res_cleanup(&stats_res);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "jobs";
  dequeue_req.owner = "worker-a";
  dequeue_req.visibility_timeout_seconds = 30L;
  message = NULL;
  rc = second_client->dequeue(second_client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(message);
  assert_string_equal(message->message_id, enqueue_res.message_id);
  assert_string_equal(message->payload_content_type, "text/plain");
  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  written = 0U;
  rc = message->write_payload(message, sink, &written, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(written, 10U);
  text = memory_sink_text(sink);
  assert_string_equal(text, "queue-body");
  free(text);
  lc_sink_close(sink);

  lc_extend_req_init(&extend_req);
  extend_req.extend_by_seconds = 45L;
  rc = message->extend(message, &extend_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(message->visibility_timeout_seconds, 45L);

  lc_nack_req_init(&nack_req);
  nack_req.intent = LC_NACK_INTENT_DEFER;
  nack_req.delay_seconds = 0L;
  rc = message->nack(message, &nack_req, &error);
  assert_int_equal(rc, LC_OK);
  message = NULL;
  second_client->close(second_client);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "jobs";
  dequeue_req.owner = "worker-b";
  dequeue_req.visibility_timeout_seconds = 30L;
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(message);
  assert_int_equal(message->attempts, 2);
  rc = message->ack(message, &error);
  assert_int_equal(rc, LC_OK);

  memset(&stats_res, 0, sizeof(stats_res));
  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats_res.available, 0);

  lc_queue_stats_res_cleanup(&stats_res);
  lc_enqueue_res_cleanup(&enqueue_res);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_queue_visibility_handoff_rejects_stale_refs(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *first_client;
  lc_client *second_client;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_extend_req extend_req;
  lc_message *first_message;
  lc_message *second_message;
  lc_source *source;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-handoff");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  first_client = open_pouch_client(endpoint);
  second_client = open_pouch_client(endpoint);
  first_message = NULL;
  second_message = NULL;

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 1L;
  enqueue_req.ttl_seconds = 60L;
  enqueue_req.max_attempts = 3;
  source = source_from_text("handoff");
  rc = first_client->enqueue(first_client, &enqueue_req, source, &enqueue_res,
                             &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "jobs";
  dequeue_req.owner = "worker-a";
  dequeue_req.visibility_timeout_seconds = 1L;
  rc = first_client->dequeue(first_client, &dequeue_req, &first_message,
                             &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(first_message);
  assert_string_equal(first_message->message_id, enqueue_res.message_id);
  assert_int_equal(first_message->attempts, 1);

  sleep(2U);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "jobs";
  dequeue_req.owner = "worker-b";
  dequeue_req.visibility_timeout_seconds = 30L;
  rc = second_client->dequeue(second_client, &dequeue_req, &second_message,
                              &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(second_message);
  assert_string_equal(second_message->message_id, enqueue_res.message_id);
  assert_int_equal(second_message->attempts, 2);
  assert_true(second_message->fencing_token > first_message->fencing_token);

  rc = first_message->ack(first_message, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 409L);
  lc_error_cleanup(&error);

  lc_extend_req_init(&extend_req);
  extend_req.extend_by_seconds = 30L;
  rc = first_message->extend(first_message, &extend_req, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 409L);
  lc_error_cleanup(&error);

  rc = second_message->ack(second_message, &error);
  assert_int_equal(rc, LC_OK);
  second_message = NULL;
  first_message->close(first_message);

  second_client->close(second_client);
  first_client->close(first_client);
  lc_enqueue_res_cleanup(&enqueue_res);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_rejects_reserved_namespace(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_client *reserved_default_client;
  lc_acquire_req acquire_req;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_ack_op ack_req;
  lc_ack_res ack_res;
  lc_get_res get_res;
  lc_lease *lease;
  lc_source *source;
  lc_sink *sink;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "reserved-ns");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&get_res, 0, sizeof(get_res));
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&ack_res, 0, sizeof(ack_res));
  client = open_pouch_client(endpoint);

  lc_acquire_req_init(&acquire_req);
  acquire_req.namespace_name = ".lockd";
  acquire_req.key = "backend-id";
  acquire_req.owner = "owner";
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  lc_error_cleanup(&error);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.namespace_name = ".lockd";
  enqueue_req.queue = "jobs";
  source = source_from_text("reserved");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_INVALID);
  lc_enqueue_res_cleanup(&enqueue_res);
  lc_error_cleanup(&error);

  memset(&ack_req, 0, sizeof(ack_req));
  ack_req.message.namespace_name = ".lockd";
  ack_req.message.queue = "jobs";
  ack_req.message.message_id = "message";
  ack_req.message.lease_id = "lease";
  ack_req.message.fencing_token = 1L;
  rc = client->queue_ack(client, &ack_req, &ack_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  lc_ack_res_cleanup(&ack_res);
  lc_error_cleanup(&error);
  client->close(client);

  reserved_default_client = open_pouch_client_with_namespace(endpoint, ".lockd");
  lc_acquire_req_init(&acquire_req);
  acquire_req.key = "alpha";
  acquire_req.owner = "owner";
  rc = reserved_default_client->acquire(reserved_default_client, &acquire_req,
                                        &lease, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  lc_error_cleanup(&error);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = reserved_default_client->get(reserved_default_client, "alpha", NULL,
                                    sink, &get_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
  lc_error_cleanup(&error);
  reserved_default_client->close(reserved_default_client);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_dequeue_with_state_lifecycle(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_update_opts update_opts;
  lc_get_res get_res;
  lc_message *message;
  lc_lease *queue_state;
  lc_source *source;
  lc_sink *sink;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-state");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&get_res, 0, sizeof(get_res));
  client = open_pouch_client(endpoint);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 60L;
  enqueue_req.ttl_seconds = 3600L;
  source = source_from_text("stateful-body");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_non_null(enqueue_res.message_id);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "jobs";
  dequeue_req.owner = "state-worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  message = NULL;
  rc = client->dequeue_with_state(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(message);
  queue_state = message->state(message);
  assert_non_null(queue_state);
  assert_string_equal(queue_state->namespace_name, "default");
  assert_non_null(queue_state->lease_id);
  assert_true(queue_state->fencing_token > 0L);
  assert_true(queue_state->lease_expires_at_unix > 0L);

  lc_update_opts_init(&update_opts);
  update_opts.content_type = "application/json";
  source = source_from_text("{\"status\":\"ok\"}");
  rc = queue_state->update(queue_state, source, &update_opts, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_true(queue_state->version > 0L);
  assert_non_null(queue_state->state_etag);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = queue_state->get(queue_state, sink, NULL, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  assert_string_equal(get_res.content_type, "application/json");
  text = memory_sink_text(sink);
  assert_string_equal(text, "{\"status\":\"ok\"}");
  free(text);
  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);

  rc = message->ack(message, &error);
  assert_int_equal(rc, LC_OK);
  lc_enqueue_res_cleanup(&enqueue_res);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_dequeue_batch_lifecycle(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_enqueue_req enqueue_req;
  lc_dequeue_req dequeue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_batch_res batch;
  lc_source *source;
  lc_sink *sink;
  lc_error error;
  char *text;
  size_t written;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "batch");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&batch, 0, sizeof(batch));
  client = open_pouch_client(endpoint);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 60L;
  enqueue_req.ttl_seconds = 3600L;
  enqueue_req.max_attempts = 3;

  source = source_from_text("first");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_enqueue_res_cleanup(&enqueue_res);

  source = source_from_text("second");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_enqueue_res_cleanup(&enqueue_res);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "jobs";
  dequeue_req.owner = "batch-worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  dequeue_req.page_size = 2;
  rc = client->dequeue_batch(client, &dequeue_req, &batch, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(batch.count, 2U);
  assert_string_equal(batch.messages[0]->queue, "jobs");
  assert_string_equal(batch.messages[1]->queue, "jobs");

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  written = 0U;
  rc = batch.messages[0]->write_payload(batch.messages[0], sink, &written,
                                        &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(written, 5U);
  text = memory_sink_text(sink);
  assert_string_equal(text, "first");
  free(text);
  lc_sink_close(sink);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  written = 0U;
  rc = batch.messages[1]->write_payload(batch.messages[1], sink, &written,
                                        &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(written, 6U);
  text = memory_sink_text(sink);
  assert_string_equal(text, "second");
  free(text);
  lc_sink_close(sink);

  rc = batch.messages[0]->ack(batch.messages[0], &error);
  assert_int_equal(rc, LC_OK);
  batch.messages[0] = NULL;
  rc = batch.messages[1]->ack(batch.messages[1], &error);
  assert_int_equal(rc, LC_OK);
  batch.messages[1] = NULL;
  lc_dequeue_batch_cleanup(&batch);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "jobs";
  dequeue_req.owner = "batch-worker";
  dequeue_req.page_size = 2;
  rc = client->dequeue_batch(client, &dequeue_req, &batch, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(batch.count, 0U);

  lc_dequeue_batch_cleanup(&batch);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_subscribe_lifecycle(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req subscribe_req;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats_res;
  lc_consumer consumer;
  subscribe_test_state subscribe_state;
  lc_source *source;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "subscribe");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&stats_res, 0, sizeof(stats_res));
  memset(&consumer, 0, sizeof(consumer));
  memset(&subscribe_state, 0, sizeof(subscribe_state));
  subscribe_state.expected[0] = "first";
  subscribe_state.expected[1] = "second";
  client = open_pouch_client(endpoint);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 60L;
  enqueue_req.ttl_seconds = 3600L;
  enqueue_req.max_attempts = 3;
  source = source_from_text("first");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_enqueue_res_cleanup(&enqueue_res);
  source = source_from_text("second");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_enqueue_res_cleanup(&enqueue_res);

  lc_dequeue_req_init(&subscribe_req);
  subscribe_req.queue = "jobs";
  subscribe_req.owner = "subscriber";
  subscribe_req.visibility_timeout_seconds = 30L;
  subscribe_req.page_size = 2;
  consumer.handle = subscribe_test_handle;
  consumer.context = &subscribe_state;
  rc = client->subscribe(client, &subscribe_req, &consumer, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(subscribe_state.handled, 2U);

  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "jobs";
  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats_res.available, 0);

  lc_queue_stats_res_cleanup(&stats_res);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_consumer_service_auto_ack(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_consumer_config consumer_config;
  lc_consumer_service_config service_config;
  lc_consumer_service *service;
  consumer_service_test_state service_state;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats_res;
  lc_source *source;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "consumer-service");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&stats_res, 0, sizeof(stats_res));
  memset(&service_state, 0, sizeof(service_state));
  service_state.expected[0] = "first";
  service_state.expected[1] = "second";
  client = open_pouch_client(endpoint);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 60L;
  enqueue_req.ttl_seconds = 3600L;
  enqueue_req.max_attempts = 3;
  source = source_from_text("first");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_enqueue_res_cleanup(&enqueue_res);
  source = source_from_text("second");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_enqueue_res_cleanup(&enqueue_res);

  lc_consumer_config_init(&consumer_config);
  lc_consumer_service_config_init(&service_config);
  consumer_config.request.queue = "jobs";
  consumer_config.request.owner = "managed-worker";
  consumer_config.request.visibility_timeout_seconds = 30L;
  consumer_config.request.wait_seconds = 1L;
  consumer_config.handle = consumer_service_test_handle;
  consumer_config.context = &service_state;
  service_config.consumers = &consumer_config;
  service_config.consumer_count = 1U;
  service = NULL;
  rc = client->new_consumer_service(client, &service_config, &service, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(service);
  service_state.service = service;
  rc = service->run(service, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(service_state.handled, 2U);
  service->close(service);

  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "jobs";
  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats_res.available, 0);

  lc_queue_stats_res_cleanup(&stats_res);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_consumer_service_with_state(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_consumer_config consumer_config;
  lc_consumer_service_config service_config;
  lc_consumer_service *service;
  consumer_service_stateful_test_state service_state;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats_res;
  lc_source *source;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "consumer-service-state");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&stats_res, 0, sizeof(stats_res));
  memset(&service_state, 0, sizeof(service_state));
  service_state.expected = "stateful";
  client = open_pouch_client(endpoint);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 60L;
  enqueue_req.ttl_seconds = 3600L;
  enqueue_req.max_attempts = 3;
  source = source_from_text("stateful");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_enqueue_res_cleanup(&enqueue_res);

  lc_consumer_config_init(&consumer_config);
  lc_consumer_service_config_init(&service_config);
  consumer_config.request.queue = "jobs";
  consumer_config.request.owner = "managed-state-worker";
  consumer_config.request.visibility_timeout_seconds = 30L;
  consumer_config.request.wait_seconds = 1L;
  consumer_config.with_state = 1;
  consumer_config.handle = consumer_service_stateful_test_handle;
  consumer_config.context = &service_state;
  service_config.consumers = &consumer_config;
  service_config.consumer_count = 1U;
  service = NULL;
  rc = client->new_consumer_service(client, &service_config, &service, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(service);
  service_state.service = service;
  rc = service->run(service, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(service_state.handled, 1U);
  service->close(service);

  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "jobs";
  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats_res.available, 0);

  lc_queue_stats_res_cleanup(&stats_res);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_pouch_endpoint_lease_state_lifecycle),
      cmocka_unit_test(test_pouch_endpoint_lease_save_uses_mapped_lonejson),
      cmocka_unit_test(test_pouch_endpoint_rejects_missing_or_wrong_txn_id),
      cmocka_unit_test(test_pouch_endpoint_remove_without_state_is_noop),
      cmocka_unit_test(
          test_pouch_endpoint_release_preserves_state_for_reacquire),
      cmocka_unit_test(test_pouch_endpoint_lease_load_respects_json_limit),
      cmocka_unit_test(test_pouch_endpoint_queue_lifecycle),
      cmocka_unit_test(
          test_pouch_endpoint_queue_visibility_handoff_rejects_stale_refs),
      cmocka_unit_test(test_pouch_endpoint_rejects_reserved_namespace),
      cmocka_unit_test(test_pouch_endpoint_dequeue_with_state_lifecycle),
      cmocka_unit_test(test_pouch_endpoint_dequeue_batch_lifecycle),
      cmocka_unit_test(test_pouch_endpoint_subscribe_lifecycle),
      cmocka_unit_test(test_pouch_endpoint_consumer_service_auto_ack),
      cmocka_unit_test(test_pouch_endpoint_consumer_service_with_state),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}

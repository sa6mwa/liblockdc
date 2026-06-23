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

static lc_client *open_pouch_client(const char *endpoint) {
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
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(client);
  lc_error_cleanup(&error);
  return client;
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
  assert_int_equal(attachment_list.count, 1U);
  assert_string_equal(attachment_list.items[0].id, attach_res.attachment.id);
  assert_string_equal(attachment_list.items[0].name, "result.txt");
  lc_attachment_list_cleanup(&attachment_list);

  lc_attachment_get_op_init(&get_attachment_req);
  get_attachment_req.lease = list_req.lease;
  get_attachment_req.selector.name = "result.txt";
  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = second_client->get_attachment(second_client, &get_attachment_req, sink,
                                     &get_attachment_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(get_attachment_res.attachment.id,
                      attach_res.attachment.id);
  text = memory_sink_text(sink);
  assert_string_equal(text, "attachment-body");
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
  assert_int_equal(lease->version, 3L);

  lc_remove_req_init(&remove_req);
  remove_req.if_state_etag = lease->state_etag;
  rc = lease->remove(lease, &remove_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_null(lease->state_etag);
  assert_int_equal(lease->version, 4L);

  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  client->close(client);
  lc_attach_res_cleanup(&attach_res);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_pouch_endpoint_lease_state_lifecycle),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}

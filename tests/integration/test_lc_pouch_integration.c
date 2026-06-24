#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cmocka.h>

#include "lc/lc.h"

static void assert_lc_ok(int rc, lc_error *error) {
  if (rc != LC_OK) {
    fail_msg(
        "lockdc call failed: rc=%d error=%d status=%ld message=%s "
        "server_code=%s correlation=%s",
        rc, error != NULL ? error->code : -1,
        error != NULL ? error->http_status : 0L,
        error != NULL && error->message != NULL ? error->message : "(none)",
        error != NULL && error->server_code != NULL ? error->server_code
                                                    : "(none)",
        error != NULL && error->correlation_id != NULL ? error->correlation_id
                                                       : "(none)");
  }
}

static void pouch_root_path(char *root, size_t root_size, const char *suffix) {
  snprintf(root, root_size, "/tmp/liblockdc-pouch-integration-%ld-%s",
           (long)getpid(), suffix);
}

static void pouch_endpoint(char *endpoint, size_t endpoint_size,
                           const char *root) {
  snprintf(endpoint, endpoint_size, "pouch://%s", root);
}

static void cleanup_pouch_root(const char *root) {
  char path[512];

  snprintf(path, sizeof(path), "%s/store.compact.tmp", root);
  unlink(path);
  snprintf(path, sizeof(path), "%s/store.log", root);
  unlink(path);
  snprintf(path, sizeof(path), "%s/writer.lock", root);
  unlink(path);
  rmdir(root);
}

static lc_source *source_from_bytes(const void *bytes, size_t length,
                                    lc_error *error) {
  lc_source *source;
  int rc;

  source = NULL;
  rc = lc_source_from_memory(bytes, length, &source, error);
  assert_lc_ok(rc, error);
  assert_non_null(source);
  return source;
}

static lc_source *source_from_text(const char *text, lc_error *error) {
  return source_from_bytes(text, strlen(text), error);
}

static off_t pouch_log_size(const char *root) {
  char path[512];
  struct stat st;

  snprintf(path, sizeof(path), "%s/store.log", root);
  assert_int_equal(stat(path, &st), 0);
  return st.st_size;
}

static void open_pouch_client(const char *endpoint, lc_client **out,
                              lc_error *error) {
  lc_client_config config;
  const char *endpoints[1];
  int rc;

  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "default";
  rc = lc_client_open(&config, out, error);
  assert_lc_ok(rc, error);
  assert_non_null(*out);
}

static void assert_sink_text(lc_sink *sink, const char *expected,
                             lc_error *error) {
  const void *bytes;
  size_t length;
  int rc;

  bytes = NULL;
  length = 0U;
  rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  assert_lc_ok(rc, error);
  assert_int_equal(length, strlen(expected));
  assert_memory_equal(bytes, expected, length);
}

static void assert_client_state_text(lc_client *client, const char *key,
                                     const char *expected, lc_error *error) {
  lc_sink *sink;
  lc_get_res get_res;
  int rc;

  sink = NULL;
  memset(&get_res, 0, sizeof(get_res));
  rc = lc_sink_to_memory(&sink, error);
  assert_lc_ok(rc, error);
  rc = client->get(client, key, NULL, sink, &get_res, error);
  assert_lc_ok(rc, error);
  assert_false(get_res.no_content);
  assert_sink_text(sink, expected, error);
  lc_get_res_cleanup(&get_res);
  lc_sink_close(sink);
}

static void lease_ref_from_lease(lc_lease *lease, lc_lease_ref *ref) {
  lc_lease_ref_init(ref);
  ref->namespace_name = lease->namespace_name;
  ref->key = lease->key;
  ref->lease_id = lease->lease_id;
  ref->txn_id = lease->txn_id;
  ref->fencing_token = lease->fencing_token;
}

static void test_pouch_public_state_attachment_shared_handles(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *writer;
  lc_client *reader;
  lc_lease *lease;
  lc_source *source;
  lc_sink *sink;
  lc_acquire_req acquire;
  lc_release_req release_req;
  lc_attach_req attach_req;
  lc_attach_res attach_res;
  lc_attachment_list attachments;
  lc_attachment_get_req get_attachment_req;
  lc_attachment_get_res get_attachment_res;
  lc_error error;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "state-attachment");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  writer = NULL;
  reader = NULL;
  lease = NULL;
  source = NULL;
  sink = NULL;
  memset(&attach_res, 0, sizeof(attach_res));
  memset(&attachments, 0, sizeof(attachments));
  memset(&get_attachment_res, 0, sizeof(get_attachment_res));

  open_pouch_client(endpoint, &writer, &error);
  lc_acquire_req_init(&acquire);
  acquire.key = "integration/state-key";
  acquire.owner = "writer";
  acquire.ttl_seconds = 60L;
  rc = writer->acquire(writer, &acquire, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  source = source_from_text("{\"owner\":\"writer\",\"step\":1}", &error);
  rc = lease->update(lease, source, NULL, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);

  lc_attach_req_init(&attach_req);
  attach_req.name = "note.txt";
  attach_req.content_type = "text/plain";
  source = source_from_text("attachment-body", &error);
  rc = lease->attach(lease, &attach_req, source, &attach_res, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);
  assert_string_equal(attach_res.attachment.name, "note.txt");
  assert_int_equal(attach_res.attachment.size, 15L);

  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;
  writer->close(writer);
  writer = NULL;

  open_pouch_client(endpoint, &reader, &error);
  assert_client_state_text(reader, "integration/state-key",
                           "{\"owner\":\"writer\",\"step\":1}", &error);

  rc = reader->acquire(reader, &acquire, &lease, &error);
  assert_lc_ok(rc, &error);
  rc = lease->list_attachments(lease, &attachments, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(attachments.count, 1U);
  assert_string_equal(attachments.items[0].name, "note.txt");

  lc_attachment_get_req_init(&get_attachment_req);
  get_attachment_req.selector.name = "note.txt";
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = lease->get_attachment(lease, &get_attachment_req, sink,
                             &get_attachment_res, &error);
  assert_lc_ok(rc, &error);
  assert_string_equal(get_attachment_res.attachment.name, "note.txt");
  assert_sink_text(sink, "attachment-body", &error);
  lc_sink_close(sink);
  sink = NULL;

  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;
  reader->close(reader);
  reader = NULL;

  lc_attach_res_cleanup(&attach_res);
  lc_attachment_list_cleanup(&attachments);
  lc_attachment_get_res_cleanup(&get_attachment_res);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_attachment_survives_compaction_reopen(
    void **state) {
  char root[256];
  char endpoint[320];
  char payload[4096];
  lc_client *writer;
  lc_client *reader;
  lc_lease *lease;
  lc_source *source;
  lc_sink *sink;
  lc_acquire_req acquire;
  lc_update_opts update_opts;
  lc_release_req release_req;
  lc_attach_req attach_req;
  lc_attach_res attach_res;
  lc_attachment_list attachments;
  lc_attachment_get_req get_attachment_req;
  lc_attachment_get_res get_attachment_res;
  lc_error error;
  size_t index;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "attachment-compact");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  writer = NULL;
  reader = NULL;
  lease = NULL;
  source = NULL;
  sink = NULL;
  memset(&attach_res, 0, sizeof(attach_res));
  memset(&attachments, 0, sizeof(attachments));
  memset(&get_attachment_res, 0, sizeof(get_attachment_res));
  memset(payload, 'x', sizeof(payload));

  open_pouch_client(endpoint, &writer, &error);
  lc_acquire_req_init(&acquire);
  acquire.key = "integration/compact-attachment";
  acquire.owner = "writer";
  acquire.ttl_seconds = 60L;
  rc = writer->acquire(writer, &acquire, &lease, &error);
  assert_lc_ok(rc, &error);

  lc_attach_req_init(&attach_req);
  attach_req.name = "artifact.txt";
  attach_req.content_type = "text/plain";
  source = source_from_text("attachment-after-compaction", &error);
  rc = lease->attach(lease, &attach_req, source, &attach_res, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);
  assert_string_equal(attach_res.attachment.name, "artifact.txt");

  lc_update_opts_init(&update_opts);
  update_opts.content_type = "application/octet-stream";
  for (index = 0U; index < 80U; ++index) {
    source = source_from_bytes(payload, sizeof(payload), &error);
    rc = lease->update(lease, source, &update_opts, &error);
    lc_source_close(source);
    assert_lc_ok(rc, &error);
  }
  assert_true(pouch_log_size(root) < (off_t)(80U * (sizeof(payload) + 1024U)));

  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;
  writer->close(writer);
  writer = NULL;

  open_pouch_client(endpoint, &reader, &error);
  acquire.owner = "reader";
  rc = reader->acquire(reader, &acquire, &lease, &error);
  assert_lc_ok(rc, &error);

  rc = lease->list_attachments(lease, &attachments, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(attachments.count, 1U);
  assert_string_equal(attachments.items[0].name, "artifact.txt");

  lc_attachment_get_req_init(&get_attachment_req);
  get_attachment_req.selector.name = "artifact.txt";
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = lease->get_attachment(lease, &get_attachment_req, sink,
                             &get_attachment_res, &error);
  assert_lc_ok(rc, &error);
  assert_string_equal(get_attachment_res.attachment.name, "artifact.txt");
  assert_sink_text(sink, "attachment-after-compaction", &error);
  lc_sink_close(sink);
  sink = NULL;

  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;
  reader->close(reader);
  reader = NULL;

  lc_attach_res_cleanup(&attach_res);
  lc_attachment_list_cleanup(&attachments);
  lc_attachment_get_res_cleanup(&get_attachment_res);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_queue_shared_handles(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *producer;
  lc_client *consumer;
  lc_client *observer;
  lc_source *source;
  lc_sink *sink;
  lc_message *message;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats;
  lc_error error;
  size_t written;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "queue");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  producer = NULL;
  consumer = NULL;
  observer = NULL;
  source = NULL;
  sink = NULL;
  message = NULL;
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&stats, 0, sizeof(stats));

  open_pouch_client(endpoint, &producer, &error);
  open_pouch_client(endpoint, &consumer, &error);
  open_pouch_client(endpoint, &observer, &error);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "integration-jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 3600L;
  enqueue_req.max_attempts = 3;
  source = source_from_text("queued-work", &error);
  rc = producer->enqueue(producer, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);
  assert_string_equal(enqueue_res.queue, "integration-jobs");

  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "integration-jobs";
  rc = observer->queue_stats(observer, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_true(stats.available);
  assert_string_equal(stats.head_message_id, enqueue_res.message_id);
  lc_queue_stats_res_cleanup(&stats);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "integration-jobs";
  dequeue_req.owner = "worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  rc = consumer->dequeue(consumer, &dequeue_req, &message, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(message);
  assert_string_equal(message->message_id, enqueue_res.message_id);
  assert_string_equal(message->payload_content_type, "text/plain");

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  written = 0U;
  rc = message->write_payload(message, sink, &written, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(written, strlen("queued-work"));
  assert_sink_text(sink, "queued-work", &error);
  lc_sink_close(sink);
  sink = NULL;

  rc = message->ack(message, &error);
  assert_lc_ok(rc, &error);
  message = NULL;

  memset(&stats, 0, sizeof(stats));
  rc = observer->queue_stats(observer, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_false(stats.available);
  assert_null(stats.head_message_id);

  lc_queue_stats_res_cleanup(&stats);
  lc_enqueue_res_cleanup(&enqueue_res);
  producer->close(producer);
  consumer->close(consumer);
  observer->close(observer);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_cas_across_clients(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *first;
  lc_client *second;
  lc_lease *lease;
  lc_source *source;
  lc_acquire_req acquire;
  lc_update_req update;
  lc_update_res update_res;
  lc_release_req release_req;
  lc_error error;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "cas");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  first = NULL;
  second = NULL;
  lease = NULL;
  source = NULL;
  memset(&update_res, 0, sizeof(update_res));

  open_pouch_client(endpoint, &first, &error);
  open_pouch_client(endpoint, &second, &error);

  lc_acquire_req_init(&acquire);
  acquire.key = "integration/cas-key";
  acquire.owner = "first";
  acquire.ttl_seconds = 60L;
  rc = first->acquire(first, &acquire, &lease, &error);
  assert_lc_ok(rc, &error);
  source = source_from_text("{\"version\":1}", &error);
  rc = lease->update(lease, source, NULL, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);

  lc_update_req_init(&update);
  lease_ref_from_lease(lease, &update.lease);
  update.if_state_etag = "stale-etag";
  source = source_from_text("{\"version\":2}", &error);
  rc = second->update(second, &update, source, &update_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 412L);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  update.if_state_etag = lease->state_etag;
  source = source_from_text("{\"version\":2}", &error);
  rc = second->update(second, &update, source, &update_res, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);
  assert_true(update_res.new_version > 1L);
  assert_client_state_text(second, "integration/cas-key", "{\"version\":2}",
                           &error);

  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;
  first->close(first);
  second->close(second);

  lc_update_res_cleanup(&update_res);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_pouch_public_state_attachment_shared_handles),
      cmocka_unit_test(
          test_pouch_public_attachment_survives_compaction_reopen),
      cmocka_unit_test(test_pouch_public_queue_shared_handles),
      cmocka_unit_test(test_pouch_public_cas_across_clients),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}

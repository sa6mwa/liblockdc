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

typedef struct pouch_consumer_state_test {
  lc_consumer_service *service;
  size_t handled;
  char queue[64];
  char message_id[128];
  char state_key[256];
} pouch_consumer_state_test;

static int pouch_consumer_state_handle(void *context,
                                       lc_consumer_message *message,
                                       lc_error *error) {
  pouch_consumer_state_test *state;
  lc_update_opts update_opts;
  lc_source *source;
  lc_sink *sink;
  size_t written;
  int rc;

  state = (pouch_consumer_state_test *)context;
  assert_non_null(state);
  assert_non_null(message);
  assert_non_null(message->message);
  assert_true(message->with_state);
  assert_non_null(message->state);
  assert_int_equal(state->handled, 0U);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, error);
  assert_lc_ok(rc, error);
  written = 0U;
  rc = message->message->write_payload(message->message, sink, &written,
                                       error);
  assert_lc_ok(rc, error);
  assert_int_equal(written, strlen("stateful-work"));
  assert_sink_text(sink, "stateful-work", error);
  lc_sink_close(sink);

  snprintf(state->queue, sizeof(state->queue), "%s", message->queue);
  snprintf(state->message_id, sizeof(state->message_id), "%s",
           message->message->message_id);
  snprintf(state->state_key, sizeof(state->state_key), "q/%s/state/%s",
           state->queue, state->message_id);

  lc_update_opts_init(&update_opts);
  update_opts.content_type = "application/json";
  source = source_from_text("{\"consumer\":\"stateful\",\"saved\":true}",
                            error);
  rc = message->state->update(message->state, source, &update_opts, error);
  lc_source_close(source);
  assert_lc_ok(rc, error);

  state->handled += 1U;
  rc = lc_consumer_service_stop(state->service);
  assert_int_equal(rc, LC_OK);
  return LC_OK;
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

static void test_pouch_public_queue_visibility_redelivery(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *producer;
  lc_client *worker_a;
  lc_client *worker_b;
  lc_source *source;
  lc_message *first_delivery;
  lc_message *second_delivery;
  lc_message *redelivery;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats;
  lc_extend_req extend_req;
  lc_nack_req nack_req;
  lc_error error;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "queue-redelivery");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  producer = NULL;
  worker_a = NULL;
  worker_b = NULL;
  source = NULL;
  first_delivery = NULL;
  second_delivery = NULL;
  redelivery = NULL;
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&stats, 0, sizeof(stats));

  open_pouch_client(endpoint, &producer, &error);
  open_pouch_client(endpoint, &worker_a, &error);
  open_pouch_client(endpoint, &worker_b, &error);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "visibility-jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 1L;
  enqueue_req.ttl_seconds = 60L;
  enqueue_req.max_attempts = 4;
  source = source_from_text("handoff-work", &error);
  rc = producer->enqueue(producer, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "visibility-jobs";
  dequeue_req.owner = "worker-a";
  dequeue_req.visibility_timeout_seconds = 1L;
  rc = worker_a->dequeue(worker_a, &dequeue_req, &first_delivery, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(first_delivery);
  assert_string_equal(first_delivery->message_id, enqueue_res.message_id);
  assert_int_equal(first_delivery->attempts, 1);

  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "visibility-jobs";
  rc = producer->queue_stats(producer, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(stats.pending_candidates, 1);
  assert_false(stats.available);
  assert_null(stats.head_message_id);
  lc_queue_stats_res_cleanup(&stats);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "visibility-jobs";
  dequeue_req.owner = "worker-b";
  dequeue_req.visibility_timeout_seconds = 1L;
  rc = worker_b->dequeue(worker_b, &dequeue_req, &second_delivery, &error);
  assert_lc_ok(rc, &error);
  assert_null(second_delivery);

  lc_extend_req_init(&extend_req);
  extend_req.extend_by_seconds = 2L;
  rc = first_delivery->extend(first_delivery, &extend_req, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(first_delivery->visibility_timeout_seconds, 2L);

  sleep(1U);
  rc = worker_b->dequeue(worker_b, &dequeue_req, &second_delivery, &error);
  assert_lc_ok(rc, &error);
  assert_null(second_delivery);

  sleep(2U);
  rc = worker_b->dequeue(worker_b, &dequeue_req, &second_delivery, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(second_delivery);
  assert_string_equal(second_delivery->message_id, enqueue_res.message_id);
  assert_int_equal(second_delivery->attempts, 2);
  assert_true(second_delivery->fencing_token > first_delivery->fencing_token);

  rc = first_delivery->ack(first_delivery, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 409L);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  first_delivery->close(first_delivery);
  first_delivery = NULL;

  lc_nack_req_init(&nack_req);
  nack_req.intent = LC_NACK_INTENT_DEFER;
  nack_req.delay_seconds = 0L;
  rc = second_delivery->nack(second_delivery, &nack_req, &error);
  assert_lc_ok(rc, &error);
  second_delivery = NULL;

  memset(&stats, 0, sizeof(stats));
  rc = producer->queue_stats(producer, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_true(stats.available);
  assert_string_equal(stats.head_message_id, enqueue_res.message_id);
  lc_queue_stats_res_cleanup(&stats);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "visibility-jobs";
  dequeue_req.owner = "worker-c";
  dequeue_req.visibility_timeout_seconds = 30L;
  rc = producer->dequeue(producer, &dequeue_req, &redelivery, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(redelivery);
  assert_string_equal(redelivery->message_id, enqueue_res.message_id);
  assert_int_equal(redelivery->attempts, 3);
  assert_int_equal(redelivery->failure_attempts, 0);

  rc = redelivery->ack(redelivery, &error);
  assert_lc_ok(rc, &error);
  redelivery = NULL;

  memset(&stats, 0, sizeof(stats));
  rc = producer->queue_stats(producer, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_false(stats.available);
  assert_int_equal(stats.pending_candidates, 0);

  lc_queue_stats_res_cleanup(&stats);
  lc_enqueue_res_cleanup(&enqueue_res);
  producer->close(producer);
  worker_a->close(worker_a);
  worker_b->close(worker_b);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_consumer_service_with_state(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *producer;
  lc_client *verifier;
  lc_source *source;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_consumer_config consumer_config;
  lc_consumer_service_config service_config;
  lc_consumer_service *service;
  pouch_consumer_state_test consumer_state;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats;
  lc_error error;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "consumer-state");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  producer = NULL;
  verifier = NULL;
  source = NULL;
  service = NULL;
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&consumer_state, 0, sizeof(consumer_state));
  memset(&stats, 0, sizeof(stats));

  open_pouch_client(endpoint, &producer, &error);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "managed-state";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 3600L;
  enqueue_req.max_attempts = 3;
  source = source_from_text("stateful-work", &error);
  rc = producer->enqueue(producer, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);

  lc_consumer_config_init(&consumer_config);
  lc_consumer_service_config_init(&service_config);
  consumer_config.request.queue = "managed-state";
  consumer_config.request.owner = "managed-state-worker";
  consumer_config.request.visibility_timeout_seconds = 30L;
  consumer_config.request.wait_seconds = 1L;
  consumer_config.with_state = 1;
  consumer_config.handle = pouch_consumer_state_handle;
  consumer_config.context = &consumer_state;
  service_config.consumers = &consumer_config;
  service_config.consumer_count = 1U;
  rc = producer->new_consumer_service(producer, &service_config, &service,
                                      &error);
  assert_lc_ok(rc, &error);
  assert_non_null(service);
  consumer_state.service = service;
  rc = service->run(service, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(consumer_state.handled, 1U);
  service->close(service);
  service = NULL;
  assert_string_equal(consumer_state.queue, "managed-state");
  assert_string_equal(consumer_state.message_id, enqueue_res.message_id);

  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "managed-state";
  rc = producer->queue_stats(producer, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_false(stats.available);
  assert_int_equal(stats.pending_candidates, 0);
  lc_queue_stats_res_cleanup(&stats);

  open_pouch_client(endpoint, &verifier, &error);
  assert_client_state_text(
      verifier, consumer_state.state_key,
      "{\"consumer\":\"stateful\",\"saved\":true}", &error);
  verifier->close(verifier);
  verifier = NULL;

  lc_enqueue_res_cleanup(&enqueue_res);
  producer->close(producer);
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

static void test_pouch_public_remove_recreate_semantics(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_lease *reacquired;
  lc_source *source;
  lc_sink *sink;
  lc_acquire_req acquire;
  lc_remove_op remove_op;
  lc_remove_req remove_req;
  lc_remove_res remove_res;
  lc_keepalive_req keepalive_req;
  lc_update_opts update_opts;
  lc_get_res get_res;
  lc_release_req release_req;
  lc_error error;
  char *stale_state_etag;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "remove-recreate");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  client = NULL;
  lease = NULL;
  reacquired = NULL;
  source = NULL;
  sink = NULL;
  stale_state_etag = NULL;
  memset(&remove_res, 0, sizeof(remove_res));
  memset(&get_res, 0, sizeof(get_res));

  open_pouch_client(endpoint, &client, &error);

  lc_acquire_req_init(&acquire);
  acquire.key = "integration/remove-key";
  acquire.owner = "owner-a";
  acquire.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(lease->version, 0L);
  assert_null(lease->state_etag);

  lc_remove_req_init(&remove_req);
  rc = lease->remove(lease, &remove_req, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(lease->version, 0L);
  assert_null(lease->state_etag);

  lc_update_opts_init(&update_opts);
  update_opts.content_type = "application/json";
  source = source_from_text("{\"version\":1}", &error);
  rc = lease->update(lease, source, &update_opts, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);
  assert_int_equal(lease->version, 1L);
  assert_non_null(lease->state_etag);
  stale_state_etag = strdup(lease->state_etag);
  assert_non_null(stale_state_etag);

  lc_remove_op_init(&remove_op);
  lease_ref_from_lease(lease, &remove_op.lease);
  remove_op.has_if_version = 1;
  remove_op.if_version = 0L;
  remove_op.if_state_etag = stale_state_etag;
  rc = client->remove(client, &remove_op, &remove_res, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 412L);
  lc_remove_res_cleanup(&remove_res);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_client_state_text(client, "integration/remove-key", "{\"version\":1}",
                           &error);

  lc_remove_req_init(&remove_req);
  remove_req.if_state_etag = lease->state_etag;
  rc = lease->remove(lease, &remove_req, &error);
  assert_lc_ok(rc, &error);
  assert_null(lease->state_etag);
  assert_int_equal(lease->version, 2L);

  update_opts.if_state_etag = stale_state_etag;
  source = source_from_text("{\"version\":2}", &error);
  rc = lease->update(lease, source, &update_opts, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 412L);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  lc_keepalive_req_init(&keepalive_req);
  keepalive_req.ttl_seconds = 60L;
  rc = lease->keepalive(lease, &keepalive_req, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(lease->version, 2L);
  assert_null(lease->state_etag);

  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  acquire.owner = "owner-b";
  rc = client->acquire(client, &acquire, &reacquired, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(reacquired);
  assert_int_equal(reacquired->version, 2L);
  assert_null(reacquired->state_etag);

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = reacquired->get(reacquired, sink, NULL, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_true(get_res.no_content);
  assert_int_equal(get_res.version, 0L);
  lc_get_res_cleanup(&get_res);
  lc_sink_close(sink);
  sink = NULL;

  source = source_from_text("{\"version\":3}", &error);
  update_opts.if_state_etag = NULL;
  rc = reacquired->update(reacquired, source, &update_opts, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);
  assert_int_equal(reacquired->version, 3L);
  assert_client_state_text(client, "integration/remove-key", "{\"version\":3}",
                           &error);

  rc = reacquired->release(reacquired, &release_req, &error);
  assert_lc_ok(rc, &error);
  reacquired = NULL;
  client->close(client);

  lc_remove_res_cleanup(&remove_res);
  lc_get_res_cleanup(&get_res);
  free(stale_state_etag);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_pouch_public_state_attachment_shared_handles),
      cmocka_unit_test(
          test_pouch_public_attachment_survives_compaction_reopen),
      cmocka_unit_test(test_pouch_public_queue_shared_handles),
      cmocka_unit_test(test_pouch_public_queue_visibility_redelivery),
      cmocka_unit_test(test_pouch_public_consumer_service_with_state),
      cmocka_unit_test(test_pouch_public_cas_across_clients),
      cmocka_unit_test(test_pouch_public_remove_recreate_semantics),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}

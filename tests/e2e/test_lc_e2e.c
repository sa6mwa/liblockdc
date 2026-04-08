#include <pthread.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <cmocka.h>

#include "lc/lc.h"

typedef struct e2e_status_doc {
  char *status;
} e2e_status_doc;

static const lonejson_field e2e_status_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(e2e_status_doc, status, "status")};

LONEJSON_MAP_DEFINE(e2e_status_map, e2e_status_doc, e2e_status_fields);

static void assert_lc_ok(int rc, lc_error *error);

static void save_json_text_or_die(lc_lease *lease, const char *json_text,
                                  lc_error *error) {
  lc_json *json;
  int rc;

  json = NULL;
  rc = lc_json_from_string(json_text, &json, error);
  assert_lc_ok(rc, error);
  rc = lease->update(lease, json, NULL, error);
  lc_json_close(json);
  assert_lc_ok(rc, error);
}

static const char *env_or_default(const char *name, const char *fallback) {
  const char *value;

  value = getenv(name);
  if (value == NULL || value[0] == '\0') {
    return fallback;
  }
  return value;
}

static int file_exists(const char *path) {
  struct stat st;

  return path != NULL && stat(path, &st) == 0 && S_ISREG(st.st_mode);
}

static int socket_exists(const char *path) {
  struct stat st;

  return path != NULL && stat(path, &st) == 0 && S_ISSOCK(st.st_mode);
}

static void require_file_or_skip(const char *path) {
  if (!file_exists(path)) {
    skip();
  }
}

static void require_socket_or_skip(const char *path) {
  if (!socket_exists(path)) {
    skip();
  }
}

static void assert_lc_ok(int rc, lc_error *error) {
  if (rc != LC_OK) {
    print_message(
        "lc error: code=%d http=%ld message=%s detail=%s server_code=%s "
        "correlation=%s\n",
        error != NULL ? error->code : -1,
        error != NULL ? error->http_status : 0L,
        error != NULL && error->message != NULL ? error->message : "(null)",
        error != NULL && error->detail != NULL ? error->detail : "(null)",
        error != NULL && error->server_code != NULL ? error->server_code
                                                    : "(null)",
        error != NULL && error->correlation_id != NULL ? error->correlation_id
                                                       : "(null)");
  }
  assert_int_equal(rc, LC_OK);
}

static int buffer_contains(const void *bytes, size_t length,
                           const char *needle) {
  const unsigned char *cursor;
  size_t needle_length;
  size_t i;

  if (bytes == NULL || needle == NULL) {
    return 0;
  }
  needle_length = strlen(needle);
  if (needle_length == 0U || length < needle_length) {
    return 0;
  }
  cursor = (const unsigned char *)bytes;
  for (i = 0U; i + needle_length <= length; ++i) {
    if (memcmp(cursor + i, needle, needle_length) == 0) {
      return 1;
    }
  }
  return 0;
}

static void make_unique_name(const char *prefix, char *buffer,
                             size_t capacity) {
  long now;
  int pid;

  now = (long)time(NULL);
  pid = (int)getpid();
  snprintf(buffer, capacity, "%s-%ld-%d", prefix, now, pid);
}

static void open_tcp_client(const char *endpoint, const char *bundle_path,
                            lc_client **out, lc_error *error) {
  lc_client_config config;
  const char *endpoints[1];
  int rc;

  lc_client_config_init(&config);
  endpoints[0] = endpoint;
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.client_bundle_path = bundle_path;
  config.default_namespace = "default";
  config.timeout_ms = 5000L;
  config.insecure_skip_verify = 1;
  rc = lc_client_open(&config, out, error);
  assert_lc_ok(rc, error);
}

static void open_uds_client(const char *socket_path, lc_client **out,
                            lc_error *error) {
  lc_client_config config;
  const char *endpoints[1];
  int rc;

  lc_client_config_init(&config);
  endpoints[0] = "http://localhost";
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.unix_socket_path = socket_path;
  config.default_namespace = "default";
  config.disable_mtls = 1;
  config.timeout_ms = 5000L;
  rc = lc_client_open(&config, out, error);
  assert_lc_ok(rc, error);
}

static void test_disk_lease_state_roundtrip(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_get_res get_res;
  lc_get_opts get_opts;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char key[96];
  static const char json_text[] =
      "{\"kind\":\"e2e\",\"value\":1,\"tags\":[\"disk\"]}";
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./devenv/volumes/lockd-disk-a-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&get_res, 0, sizeof(get_res));
  memset(&get_opts, 0, sizeof(get_opts));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("disk-state", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  save_json_text_or_die(lease, json_text, &error);
  assert_true(lease->version >= 1L);
  assert_non_null(lease->state_etag);

  rc = lease->describe(lease, &error);
  assert_lc_ok(rc, &error);
  assert_string_equal(lease->key, key);
  assert_string_equal(lease->namespace_name, "default");
  assert_non_null(lease->owner);
  assert_true(lease->owner[0] != '\0');
  assert_non_null(lease->lease_id);

  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  get_opts.public_read = 1;
  rc = client->get(client, key, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(get_res.no_content, 0);
  assert_true(get_res.version >= 1L);

  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_true(buffer_contains(bytes, length, "\"kind\":\"e2e\""));
  assert_true(buffer_contains(bytes, length, "\"tags\":[\"disk\"]"));

  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_s3_lease_state_roundtrip(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_get_res get_res;
  lc_get_opts get_opts;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char key[96];
  static const char json_text[] =
      "{\"kind\":\"e2e\",\"value\":1,\"tags\":[\"s3\"]}";
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_S3_ENDPOINT", "https://localhost:19443");
  bundle_path = env_or_default("LOCKDC_E2E_S3_BUNDLE",
                               "./devenv/volumes/lockd-s3-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&get_res, 0, sizeof(get_res));
  memset(&get_opts, 0, sizeof(get_opts));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("s3-state", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  save_json_text_or_die(lease, json_text, &error);
  assert_true(lease->version >= 1L);
  assert_non_null(lease->state_etag);

  rc = lease->describe(lease, &error);
  assert_lc_ok(rc, &error);
  assert_string_equal(lease->key, key);
  assert_string_equal(lease->namespace_name, "default");
  assert_non_null(lease->owner);
  assert_true(lease->owner[0] != '\0');
  assert_non_null(lease->lease_id);

  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  get_opts.public_read = 1;
  rc = client->get(client, key, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(get_res.no_content, 0);
  assert_true(get_res.version >= 1L);

  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_true(buffer_contains(bytes, length, "\"kind\":\"e2e\""));
  assert_true(buffer_contains(bytes, length, "\"tags\":[\"s3\"]"));

  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_s3_attachment_roundtrip(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_source *src;
  lc_sink *sink;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_attach_req attach_req;
  lc_attach_res attach_res;
  lc_attachment_list attachment_list;
  lc_attachment_get_req get_req;
  lc_attachment_get_res get_res;
  const void *bytes;
  size_t length;
  char key[96];
  static const unsigned char payload[] = {'h', 'e', 'l', 'l',
                                          'o', '-', 's', '3'};
  int deleted;
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_S3_ENDPOINT", "https://localhost:19443");
  bundle_path = env_or_default("LOCKDC_E2E_S3_BUNDLE",
                               "./devenv/volumes/lockd-s3-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  src = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  deleted = 0;
  lc_error_init(&error);
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&attach_req, 0, sizeof(attach_req));
  memset(&attach_res, 0, sizeof(attach_res));
  memset(&attachment_list, 0, sizeof(attachment_list));
  memset(&get_req, 0, sizeof(get_req));
  memset(&get_res, 0, sizeof(get_res));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("s3-attachment", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  save_json_text_or_die(lease, "{\"kind\":\"attachment\"}", &error);

  rc = lc_source_from_memory(payload, sizeof(payload), &src, &error);
  assert_lc_ok(rc, &error);
  attach_req.name = "blob.bin";
  attach_req.content_type = "application/octet-stream";
  rc = lease->attach(lease, &attach_req, src, &attach_res, &error);
  assert_lc_ok(rc, &error);
  assert_string_equal(attach_res.attachment.name, "blob.bin");
  assert_true(attach_res.attachment.size >= (long)sizeof(payload));

  rc = lease->list_attachments(lease, &attachment_list, &error);
  assert_lc_ok(rc, &error);
  assert_true(attachment_list.count >= 1U);

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  memset(&get_req, 0, sizeof(get_req));
  get_req.selector.name = "blob.bin";
  rc = lease->get_attachment(lease, &get_req, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(length, sizeof(payload));
  assert_memory_equal(bytes, payload, sizeof(payload));

  rc = lease->delete_attachment(lease, &get_req.selector, &deleted, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(deleted, 1);

  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  lc_sink_close(sink);
  lc_source_close(src);
  lc_attachment_get_res_cleanup(&get_res);
  lc_attachment_list_cleanup(&attachment_list);
  lc_attach_res_cleanup(&attach_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_mem_uds_queue_roundtrip(void **state) {
  const char *socket_path;
  lc_client *client;
  lc_source *src;
  lc_sink *sink;
  lc_message *message;
  lc_error error;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  const void *bytes;
  size_t length;
  size_t written;
  char queue_name[96];
  static const unsigned char payload[] = {'h', 'e', 'l', 'l', 'o',
                                          '-', 'u', 'd', 's'};
  int rc;

  (void)state;
  socket_path = env_or_default("LOCKDC_E2E_MEM_SOCKET",
                               "./devenv/volumes/lockd-mem-run/lockd.sock");
  require_socket_or_skip(socket_path);

  client = NULL;
  src = NULL;
  sink = NULL;
  message = NULL;
  bytes = NULL;
  length = 0U;
  written = 0U;
  lc_error_init(&error);
  memset(&enqueue_req, 0, sizeof(enqueue_req));
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&dequeue_req, 0, sizeof(dequeue_req));

  open_uds_client(socket_path, &client, &error);
  make_unique_name("mem-queue", queue_name, sizeof(queue_name));
  rc = lc_source_from_memory(payload, sizeof(payload), &src, &error);
  assert_lc_ok(rc, &error);

  enqueue_req.queue = queue_name;
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 300L;
  enqueue_req.max_attempts = 5;
  rc = client->enqueue(client, &enqueue_req, src, &enqueue_res, &error);
  assert_lc_ok(rc, &error);
  assert_string_equal(enqueue_res.queue, queue_name);
  assert_true(enqueue_res.payload_bytes >= (long)sizeof(payload));

  dequeue_req.queue = queue_name;
  dequeue_req.owner = "lc-e2e-worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  dequeue_req.wait_seconds = 2L;
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(message);
  assert_string_equal(message->queue, queue_name);
  assert_string_equal(message->payload_content_type, "text/plain");

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = message->write_payload(message, sink, &written, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(written, sizeof(payload));
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(length, sizeof(payload));
  assert_memory_equal(bytes, payload, sizeof(payload));

  rc = message->rewind_payload(message, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(error.code, LC_ERR_INVALID);
  lc_error_cleanup(&error);
  rc = message->ack(message, &error);
  assert_lc_ok(rc, &error);
  message = NULL;

  lc_enqueue_res_cleanup(&enqueue_res);
  lc_sink_close(sink);
  lc_source_close(src);
  if (message != NULL) {
    lc_message_close(message);
  }
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_mem_uds_dequeue_batch_roundtrip(void **state) {
  const char *socket_path;
  lc_client *client;
  lc_source *src;
  lc_error error;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_dequeue_batch_res batch;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char queue_name[96];
  static const unsigned char payload_a[] = {'b', 'a', 't', 'c', 'h', '-', '1'};
  static const unsigned char payload_b[] = {'b', 'a', 't', 'c', 'h', '-', '2'};
  int rc;
  size_t i;

  (void)state;
  socket_path = env_or_default("LOCKDC_E2E_MEM_SOCKET",
                               "./devenv/volumes/lockd-mem-run/lockd.sock");
  require_socket_or_skip(socket_path);

  client = NULL;
  src = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_dequeue_req_init(&dequeue_req);
  memset(&batch, 0, sizeof(batch));

  open_uds_client(socket_path, &client, &error);
  make_unique_name("mem-queue-batch", queue_name, sizeof(queue_name));
  enqueue_req.queue = queue_name;
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 300L;
  enqueue_req.max_attempts = 5;

  rc = lc_source_from_memory(payload_a, sizeof(payload_a), &src, &error);
  assert_lc_ok(rc, &error);
  rc = client->enqueue(client, &enqueue_req, src, &enqueue_res, &error);
  assert_lc_ok(rc, &error);
  lc_source_close(src);
  src = NULL;
  lc_enqueue_res_cleanup(&enqueue_res);

  rc = lc_source_from_memory(payload_b, sizeof(payload_b), &src, &error);
  assert_lc_ok(rc, &error);
  rc = client->enqueue(client, &enqueue_req, src, &enqueue_res, &error);
  assert_lc_ok(rc, &error);
  lc_source_close(src);
  src = NULL;
  lc_enqueue_res_cleanup(&enqueue_res);

  dequeue_req.queue = queue_name;
  dequeue_req.owner = "lc-e2e-batch-worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  dequeue_req.wait_seconds = 2L;
  dequeue_req.page_size = 2;
  rc = client->dequeue_batch(client, &dequeue_req, &batch, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(batch.count, 2U);

  for (i = 0U; i < batch.count; ++i) {
    rc = lc_sink_to_memory(&sink, &error);
    assert_lc_ok(rc, &error);
    rc =
        batch.messages[i]->write_payload(batch.messages[i], sink, NULL, &error);
    assert_lc_ok(rc, &error);
    rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
    assert_lc_ok(rc, &error);
    assert_true(length == sizeof(payload_a) || length == sizeof(payload_b));
    rc = batch.messages[i]->ack(batch.messages[i], &error);
    assert_lc_ok(rc, &error);
    batch.messages[i] = NULL;
    lc_sink_close(sink);
    sink = NULL;
  }

  lc_dequeue_batch_cleanup(&batch);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_local_mutate_stream_roundtrip(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_sink *sink;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_get_res get_res;
  lc_mutate_local_req mutate_req;
  const void *bytes;
  size_t length;
  char key[96];
  char file_template[] = "/tmp/liblockdc-local-mutate-XXXXXX";
  int fd;
  FILE *fp;
  const char *mutations[2];
  char mutation_buffer[512];
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./devenv/volumes/lockd-disk-a-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  memset(&get_res, 0, sizeof(get_res));
  lc_mutate_local_req_init(&mutate_req);

  fd = mkstemp(file_template);
  assert_true(fd >= 0);
  fp = fdopen(fd, "wb");
  assert_non_null(fp);
  assert_int_equal(fwrite("hello\\nlockd", 1U, 11U, fp), 11);
  assert_int_equal(fclose(fp), 0);

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("disk-local-mutate", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  save_json_text_or_die(lease, "{\"kind\":\"e2e-local\"}", &error);

  mutations[0] = "/filename=\"blob.txt\"";
  snprintf(mutation_buffer, sizeof(mutation_buffer), "textfile:/blob=%s",
           file_template);
  mutations[1] = mutation_buffer;
  mutate_req.mutations = mutations;
  mutate_req.mutation_count = 2U;
  rc = lease->mutate_local(lease, &mutate_req, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease->state_etag);

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = lease->get(lease, sink, NULL, &get_res, &error);
  assert_lc_ok(rc, &error);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_true(buffer_contains(bytes, length, "\"filename\":\"blob.txt\""));
  assert_true(buffer_contains(bytes, length, "\"blob\":"));

  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  unlink(file_template);
  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_s3_local_mutate_stream_roundtrip(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_sink *sink;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_get_res get_res;
  lc_mutate_local_req mutate_req;
  const void *bytes;
  size_t length;
  char key[96];
  char file_template[] = "/tmp/liblockdc-s3-local-mutate-XXXXXX";
  int fd;
  FILE *fp;
  const char *mutations[2];
  char mutation_buffer[512];
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_S3_ENDPOINT", "https://localhost:19443");
  bundle_path = env_or_default("LOCKDC_E2E_S3_BUNDLE",
                               "./devenv/volumes/lockd-s3-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  memset(&get_res, 0, sizeof(get_res));
  lc_mutate_local_req_init(&mutate_req);

  fd = mkstemp(file_template);
  assert_true(fd >= 0);
  fp = fdopen(fd, "wb");
  assert_non_null(fp);
  assert_int_equal(fwrite("hello\\nlockd", 1U, 11U, fp), 11);
  assert_int_equal(fclose(fp), 0);

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("s3-local-mutate", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  save_json_text_or_die(lease, "{\"kind\":\"e2e-local\"}", &error);

  mutations[0] = "/filename=\"blob.txt\"";
  snprintf(mutation_buffer, sizeof(mutation_buffer), "textfile:/blob=%s",
           file_template);
  mutations[1] = mutation_buffer;
  mutate_req.mutations = mutations;
  mutate_req.mutation_count = 2U;
  rc = lease->mutate_local(lease, &mutate_req, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease->state_etag);

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = lease->get(lease, sink, NULL, &get_res, &error);
  assert_lc_ok(rc, &error);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_true(buffer_contains(bytes, length, "\"filename\":\"blob.txt\""));
  assert_true(buffer_contains(bytes, length, "\"blob\":"));

  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  unlink(file_template);
  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

typedef struct e2e_consumer_backend {
  const char *label;
  const char *endpoint_env;
  const char *endpoint_default;
  const char *bundle_env;
  const char *bundle_default;
  const char *socket_env;
  const char *socket_default;
  int use_uds;
} e2e_consumer_backend;

typedef struct e2e_consumer_context {
  pthread_mutex_t mutex;
  int handled;
  int error_events;
  int start_events;
  int stop_events;
  int first_delivery_mode;
  int expect_state;
  int saw_state;
  char payload[64];
  char state_json[128];
  char state_key[160];
} e2e_consumer_context;

enum {
  E2E_CONSUMER_FIRST_DELIVERY_NONE = 0,
  E2E_CONSUMER_FIRST_DELIVERY_FAIL = 1,
  E2E_CONSUMER_FIRST_DELIVERY_DEFER = 2
};

static const e2e_consumer_backend e2e_backend_disk = {
    "disk",
    "LOCKDC_E2E_DISK_A_ENDPOINT",
    "https://127.0.0.1:19441",
    "LOCKDC_E2E_DISK_A_BUNDLE",
    "./devenv/volumes/lockd-disk-a-config/client.pem",
    NULL,
    NULL,
    0};

static const e2e_consumer_backend e2e_backend_s3 = {
    "s3",
    "LOCKDC_E2E_S3_ENDPOINT",
    "https://127.0.0.1:19443",
    "LOCKDC_E2E_S3_BUNDLE",
    "./devenv/volumes/lockd-s3-config/client.pem",
    NULL,
    NULL,
    0};

static const e2e_consumer_backend e2e_backend_mem = {
    "mem",
    NULL,
    NULL,
    NULL,
    NULL,
    "LOCKDC_E2E_MEM_SOCKET",
    "./devenv/volumes/lockd-mem-run/lockd.sock",
    1};

static void open_consumer_backend_client(const e2e_consumer_backend *backend,
                                         lc_client **out, lc_error *error) {
  const char *endpoint;
  const char *bundle_path;
  const char *socket_path;

  if (backend->use_uds) {
    socket_path = env_or_default(backend->socket_env, backend->socket_default);
    require_socket_or_skip(socket_path);
    open_uds_client(socket_path, out, error);
    return;
  }

  endpoint = env_or_default(backend->endpoint_env, backend->endpoint_default);
  bundle_path = env_or_default(backend->bundle_env, backend->bundle_default);
  require_file_or_skip(bundle_path);
  open_tcp_client(endpoint, bundle_path, out, error);
}

static int e2e_consumer_record_payload(e2e_consumer_context *consumer_context,
                                       const void *bytes, size_t length) {
  pthread_mutex_lock(&consumer_context->mutex);
  memset(consumer_context->payload, 0, sizeof(consumer_context->payload));
  if (length >= sizeof(consumer_context->payload)) {
    length = sizeof(consumer_context->payload) - 1U;
  }
  memcpy(consumer_context->payload, bytes, length);
  consumer_context->handled += 1;
  pthread_mutex_unlock(&consumer_context->mutex);
  return LC_OK;
}

static int e2e_consumer_persist_state(e2e_consumer_context *consumer_context,
                                      lc_lease *state, lc_error *error) {
  e2e_status_doc doc;
  lc_get_res get_res;
  int rc;

  if (state == NULL) {
    return LC_OK;
  }

  memset(&doc, 0, sizeof(doc));
  memset(&get_res, 0, sizeof(get_res));
  doc.status = "from-consumer-service";
  rc = state->save(state, &e2e_status_map, &doc, NULL, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&doc, 0, sizeof(doc));
  rc = state->load(state, &e2e_status_map, &doc, NULL, NULL, &get_res, error);
  if (rc != LC_OK) {
    lc_get_res_cleanup(&get_res);
    return rc;
  }

  pthread_mutex_lock(&consumer_context->mutex);
  consumer_context->saw_state = 1;
  memset(consumer_context->state_json, 0, sizeof(consumer_context->state_json));
  if (doc.status != NULL) {
    strncpy(consumer_context->state_json, doc.status,
            sizeof(consumer_context->state_json) - 1U);
  }
  memset(consumer_context->state_key, 0, sizeof(consumer_context->state_key));
  if (state->key != NULL) {
    strncpy(consumer_context->state_key, state->key,
            sizeof(consumer_context->state_key) - 1U);
  }
  pthread_mutex_unlock(&consumer_context->mutex);

  lonejson_cleanup(&e2e_status_map, &doc);
  lc_get_res_cleanup(&get_res);
  return LC_OK;
}

static int e2e_consumer_handle(void *context, lc_consumer_message *delivery,
                               lc_error *error) {
  e2e_consumer_context *consumer_context;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  int handled_count;
  lc_lease *state_lease;
  int rc;
  lc_nack_req nack_req;

  consumer_context = (e2e_consumer_context *)context;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  handled_count = 0;
  rc = lc_sink_to_memory(&sink, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = delivery->message->write_payload(delivery->message, sink, NULL, error);
  if (rc != LC_OK) {
    lc_sink_close(sink);
    return rc;
  }
  rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  if (rc == LC_OK) {
    rc = e2e_consumer_record_payload(consumer_context, bytes, length);
  }
  lc_sink_close(sink);
  if (rc != LC_OK) {
    return rc;
  }

  state_lease = delivery->state;
  if (state_lease == NULL && delivery->message != NULL &&
      delivery->message->state != NULL) {
    state_lease = delivery->message->state(delivery->message);
  }
  if (consumer_context->expect_state && state_lease == NULL) {
    return LC_ERR_PROTOCOL;
  }
  rc = e2e_consumer_persist_state(consumer_context, state_lease, error);
  if (rc != LC_OK) {
    return rc;
  }

  pthread_mutex_lock(&consumer_context->mutex);
  handled_count = consumer_context->handled;
  pthread_mutex_unlock(&consumer_context->mutex);
  if (consumer_context->first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_FAIL &&
      handled_count == 1) {
    return LC_ERR_TRANSPORT;
  }
  if (consumer_context->first_delivery_mode ==
          E2E_CONSUMER_FIRST_DELIVERY_DEFER &&
      handled_count == 1) {
    lc_nack_req_init(&nack_req);
    nack_req.intent = LC_NACK_INTENT_DEFER;
    nack_req.delay_seconds = 0L;
    return delivery->message->nack(delivery->message, &nack_req, error);
  }

  return LC_OK;
}

static int e2e_consumer_on_error(void *context, const lc_consumer_error *event,
                                 lc_error *error) {
  e2e_consumer_context *consumer_context;

  (void)event;
  (void)error;
  consumer_context = (e2e_consumer_context *)context;
  pthread_mutex_lock(&consumer_context->mutex);
  consumer_context->error_events += 1;
  pthread_mutex_unlock(&consumer_context->mutex);
  return LC_OK;
}

static void e2e_consumer_on_start(void *context,
                                  const lc_consumer_lifecycle_event *event) {
  e2e_consumer_context *consumer_context;

  (void)event;
  consumer_context = (e2e_consumer_context *)context;
  pthread_mutex_lock(&consumer_context->mutex);
  consumer_context->start_events += 1;
  pthread_mutex_unlock(&consumer_context->mutex);
}

static void e2e_consumer_on_stop(void *context,
                                 const lc_consumer_lifecycle_event *event) {
  e2e_consumer_context *consumer_context;

  (void)event;
  consumer_context = (e2e_consumer_context *)context;
  pthread_mutex_lock(&consumer_context->mutex);
  consumer_context->stop_events += 1;
  pthread_mutex_unlock(&consumer_context->mutex);
}

static void wait_for_consumer_handled(e2e_consumer_context *consumer_context,
                                      int minimum_count) {
  int maximum_retries;
  int retries;

  maximum_retries = 120;
  if (minimum_count > 1) {
    maximum_retries = 600;
  }
  retries = 0;
  for (;;) {
    pthread_mutex_lock(&consumer_context->mutex);
    if (consumer_context->handled >= minimum_count) {
      pthread_mutex_unlock(&consumer_context->mutex);
      return;
    }
    pthread_mutex_unlock(&consumer_context->mutex);
    if (retries++ > maximum_retries) {
      fail_msg("consumer retry timed out: handled=%d start_events=%d "
               "stop_events=%d error_events=%d",
               consumer_context->handled, consumer_context->start_events,
               consumer_context->stop_events, consumer_context->error_events);
    }
    usleep(100000);
  }
}

static void run_consumer_service_variant(const e2e_consumer_backend *backend,
                                         const char *name_prefix,
                                         int enqueue_count,
                                         int first_delivery_mode,
                                         int expect_state,
                                         size_t worker_count) {
  lc_client *client;
  lc_consumer_service *service;
  lc_error error;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_consumer_config consumer;
  lc_consumer_service_config service_config;
  e2e_consumer_context consumer_context;
  char queue_name[96];
  static const unsigned char payload[] = {'c', 'o', 'n', 's', 'u', 'm',
                                          'e', 'r', '-', 'o', 'k'};
  int rc;
  int i;

  client = NULL;
  service = NULL;
  lc_error_init(&error);
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&service_config);
  memset(&consumer_context, 0, sizeof(consumer_context));
  consumer_context.first_delivery_mode = first_delivery_mode;
  consumer_context.expect_state = expect_state;
  assert_int_equal(pthread_mutex_init(&consumer_context.mutex, NULL), 0);

  open_consumer_backend_client(backend, &client, &error);
  make_unique_name(name_prefix, queue_name, sizeof(queue_name));

  consumer.name = backend->label;
  consumer.request.queue = queue_name;
  consumer.request.visibility_timeout_seconds =
      first_delivery_mode != E2E_CONSUMER_FIRST_DELIVERY_NONE ? 1L : 30L;
  consumer.request.wait_seconds = 1L;
  consumer.worker_count = worker_count;
  consumer.with_state = expect_state;
  consumer.handle = e2e_consumer_handle;
  consumer.on_error = e2e_consumer_on_error;
  consumer.on_start = e2e_consumer_on_start;
  consumer.on_stop = e2e_consumer_on_stop;
  consumer.context = &consumer_context;
  lc_consumer_restart_policy_init(&consumer.restart_policy);
  consumer.restart_policy.base_delay_ms = 100L;
  consumer.restart_policy.max_delay_ms = 250L;
  consumer.restart_policy.max_failures = 5;

  service_config.consumers = &consumer;
  service_config.consumer_count = 1U;
  rc = client->new_consumer_service(client, &service_config, &service, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(service);

  rc = service->start(service, &error);
  assert_lc_ok(rc, &error);

  enqueue_req.queue = queue_name;
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 300L;
  enqueue_req.max_attempts = 5;
  for (i = 0; i < enqueue_count; ++i) {
    lc_source *src;

    src = NULL;
    rc = lc_source_from_memory(payload, sizeof(payload), &src, &error);
    assert_lc_ok(rc, &error);
    rc = client->enqueue(client, &enqueue_req, src, &enqueue_res, &error);
    assert_lc_ok(rc, &error);
    lc_source_close(src);
    lc_enqueue_res_cleanup(&enqueue_res);
    memset(&enqueue_res, 0, sizeof(enqueue_res));
  }

  wait_for_consumer_handled(&consumer_context,
                            first_delivery_mode !=
                                    E2E_CONSUMER_FIRST_DELIVERY_NONE
                                ? enqueue_count + 1
                                : enqueue_count);

  rc = (service->stop)(service);
  assert_int_equal(rc, LC_OK);
  rc = service->wait(service, &error);
  assert_lc_ok(rc, &error);

  pthread_mutex_lock(&consumer_context.mutex);
  assert_true(consumer_context.start_events >= (int)worker_count);
  assert_true(consumer_context.stop_events >= (int)worker_count);
  if (first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_FAIL) {
    assert_int_equal(consumer_context.handled, enqueue_count + 1);
    assert_true(consumer_context.error_events >= 1);
    assert_true(consumer_context.start_events >= (int)worker_count + 1);
  } else if (first_delivery_mode ==
             E2E_CONSUMER_FIRST_DELIVERY_DEFER) {
    assert_int_equal(consumer_context.handled, enqueue_count + 1);
    assert_int_equal(consumer_context.error_events, 0);
    assert_int_equal(consumer_context.start_events, (int)worker_count);
  } else {
    assert_int_equal(consumer_context.handled, enqueue_count);
    assert_int_equal(consumer_context.error_events, 0);
    assert_int_equal(consumer_context.start_events, (int)worker_count);
  }
  assert_string_equal(consumer_context.payload, "consumer-ok");
  if (expect_state) {
    assert_int_equal(consumer_context.saw_state, 1);
    assert_non_null(
        strstr(consumer_context.state_json, "from-consumer-service"));
    assert_non_null(strstr(consumer_context.state_key, "/state/"));
    assert_non_null(strstr(consumer_context.state_key, queue_name));
  } else {
    assert_int_equal(consumer_context.saw_state, 0);
  }
  pthread_mutex_unlock(&consumer_context.mutex);

  pthread_mutex_destroy(&consumer_context.mutex);
  lc_enqueue_res_cleanup(&enqueue_res);
  lc_consumer_service_close(service);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_consumer_service_happy(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_disk, "disk-consumer-happy", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 0, 1U);
}

static void test_disk_consumer_service_multi_delivery(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_disk, "disk-consumer-multi", 2,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 0, 1U);
}

static void test_disk_consumer_service_with_state(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_disk, "disk-consumer-state", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 1, 1U);
}

static void
test_disk_consumer_service_restart_after_handler_failure(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_disk, "disk-consumer-retry", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_FAIL, 0, 1U);
}

static void test_disk_consumer_service_defer_then_redeliver(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_disk, "disk-consumer-defer", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_DEFER, 0, 1U);
}

static void test_s3_consumer_service_happy(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_s3, "s3-consumer-happy", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 0, 1U);
}

static void test_s3_consumer_service_multi_delivery(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_s3, "s3-consumer-multi", 2,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 0, 1U);
}

static void test_s3_consumer_service_with_state(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_s3, "s3-consumer-state", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 1, 1U);
}

static void
test_s3_consumer_service_restart_after_handler_failure(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_s3, "s3-consumer-retry", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_FAIL, 0, 1U);
}

static void test_s3_consumer_service_defer_then_redeliver(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_s3, "s3-consumer-defer", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_DEFER, 0, 1U);
}

static void test_mem_uds_consumer_service_happy(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_mem, "mem-consumer-happy", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 0, 1U);
}

static void test_mem_uds_consumer_service_multi_delivery(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_mem, "mem-consumer-multi", 2,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 0, 1U);
}

static void test_mem_uds_consumer_service_with_state(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_mem, "mem-consumer-state", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 1, 1U);
}

static void
test_mem_uds_consumer_service_restart_after_handler_failure(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_mem, "mem-consumer-retry", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_FAIL, 0, 1U);
}

static void test_mem_uds_consumer_service_defer_then_redeliver(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_mem, "mem-consumer-defer", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_DEFER, 0, 1U);
}

static void test_mem_uds_consumer_service_multi_worker(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_mem, "mem-consumer-workers", 2, 0,
                               0, 2U);
}

static void test_mem_uds_dequeue_with_state_roundtrip(void **state) {
  const char *socket_path;
  lc_client *client;
  lc_source *src;
  lc_message *message;
  lc_lease *state_lease;
  lc_error error;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_get_res get_res;
  char queue_name[96];
  char expected_state_key[160];
  static const unsigned char payload[] = {'s', 't', 'a', 't', 'e',
                                          '-', 'u', 'd', 's'};
  int rc;

  (void)state;
  socket_path = env_or_default("LOCKDC_E2E_MEM_SOCKET",
                               "./devenv/volumes/lockd-mem-run/lockd.sock");
  require_socket_or_skip(socket_path);

  client = NULL;
  src = NULL;
  message = NULL;
  state_lease = NULL;
  lc_error_init(&error);
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_dequeue_req_init(&dequeue_req);
  memset(&get_res, 0, sizeof(get_res));

  open_uds_client(socket_path, &client, &error);
  make_unique_name("mem-state-queue", queue_name, sizeof(queue_name));
  rc = lc_source_from_memory(payload, sizeof(payload), &src, &error);
  assert_lc_ok(rc, &error);

  enqueue_req.queue = queue_name;
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 300L;
  enqueue_req.max_attempts = 5;
  rc = client->enqueue(client, &enqueue_req, src, &enqueue_res, &error);
  assert_lc_ok(rc, &error);

  dequeue_req.queue = queue_name;
  dequeue_req.owner = "lc-e2e-state-worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  dequeue_req.wait_seconds = 2L;
  rc = client->dequeue_with_state(client, &dequeue_req, &message, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(message);

  state_lease = message->state(message);
  assert_non_null(state_lease);
  snprintf(expected_state_key, sizeof(expected_state_key), "q/%s/state/%s",
           queue_name, message->message_id);
  assert_string_equal(state_lease->key, expected_state_key);

  {
    e2e_status_doc doc;

    doc.status = "from-direct-state";
    rc = state_lease->save(state_lease, &e2e_status_map, &doc, NULL, &error);
  }
  assert_lc_ok(rc, &error);
  {
    e2e_status_doc doc;

    memset(&doc, 0, sizeof(doc));
    rc = state_lease->load(state_lease, &e2e_status_map, &doc, NULL, NULL,
                           &get_res, &error);
    assert_lc_ok(rc, &error);
    assert_non_null(doc.status);
    assert_string_equal(doc.status, "from-direct-state");
    lonejson_cleanup(&e2e_status_map, &doc);
  }
  assert_lc_ok(rc, &error);

  rc = message->ack(message, &error);
  assert_lc_ok(rc, &error);
  message = NULL;

  lc_get_res_cleanup(&get_res);
  lc_enqueue_res_cleanup(&enqueue_res);
  lc_source_close(src);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_s3_dequeue_with_state_roundtrip(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_source *src;
  lc_message *message;
  lc_lease *state_lease;
  lc_error error;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_get_res get_res;
  char queue_name[96];
  char expected_state_key[160];
  static const unsigned char payload[] = {'s', 't', 'a', 't', 'e',
                                          '-', 's', '3'};
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_S3_ENDPOINT", "https://localhost:19443");
  bundle_path = env_or_default("LOCKDC_E2E_S3_BUNDLE",
                               "./devenv/volumes/lockd-s3-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  src = NULL;
  message = NULL;
  state_lease = NULL;
  lc_error_init(&error);
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_dequeue_req_init(&dequeue_req);
  memset(&get_res, 0, sizeof(get_res));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("s3-state-queue", queue_name, sizeof(queue_name));
  rc = lc_source_from_memory(payload, sizeof(payload), &src, &error);
  assert_lc_ok(rc, &error);

  enqueue_req.queue = queue_name;
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 300L;
  enqueue_req.max_attempts = 5;
  rc = client->enqueue(client, &enqueue_req, src, &enqueue_res, &error);
  assert_lc_ok(rc, &error);

  dequeue_req.queue = queue_name;
  dequeue_req.owner = "lc-e2e-state-worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  dequeue_req.wait_seconds = 2L;
  rc = client->dequeue_with_state(client, &dequeue_req, &message, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(message);

  state_lease = message->state(message);
  assert_non_null(state_lease);
  snprintf(expected_state_key, sizeof(expected_state_key), "q/%s/state/%s",
           queue_name, message->message_id);
  assert_string_equal(state_lease->key, expected_state_key);

  {
    e2e_status_doc doc;

    doc.status = "from-s3-direct-state";
    rc = state_lease->save(state_lease, &e2e_status_map, &doc, NULL, &error);
  }
  assert_lc_ok(rc, &error);
  {
    e2e_status_doc doc;

    memset(&doc, 0, sizeof(doc));
    rc = state_lease->load(state_lease, &e2e_status_map, &doc, NULL, NULL,
                           &get_res, &error);
    assert_lc_ok(rc, &error);
    assert_non_null(doc.status);
    assert_string_equal(doc.status, "from-s3-direct-state");
    lonejson_cleanup(&e2e_status_map, &doc);
  }
  assert_lc_ok(rc, &error);

  rc = message->ack(message, &error);
  assert_lc_ok(rc, &error);
  message = NULL;

  lc_get_res_cleanup(&get_res);
  lc_enqueue_res_cleanup(&enqueue_res);
  lc_source_close(src);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_disk_lease_state_roundtrip),
      cmocka_unit_test(test_s3_lease_state_roundtrip),
      cmocka_unit_test(test_disk_local_mutate_stream_roundtrip),
      cmocka_unit_test(test_s3_local_mutate_stream_roundtrip),
      cmocka_unit_test(test_s3_attachment_roundtrip),
      cmocka_unit_test(test_disk_consumer_service_happy),
      cmocka_unit_test(test_disk_consumer_service_multi_delivery),
      cmocka_unit_test(test_disk_consumer_service_with_state),
      cmocka_unit_test(
          test_disk_consumer_service_restart_after_handler_failure),
      cmocka_unit_test(test_disk_consumer_service_defer_then_redeliver),
      cmocka_unit_test(test_s3_consumer_service_happy),
      cmocka_unit_test(test_s3_consumer_service_multi_delivery),
      cmocka_unit_test(test_s3_consumer_service_with_state),
      cmocka_unit_test(test_s3_consumer_service_restart_after_handler_failure),
      cmocka_unit_test(test_s3_consumer_service_defer_then_redeliver),
      cmocka_unit_test(test_mem_uds_consumer_service_happy),
      cmocka_unit_test(test_mem_uds_consumer_service_multi_delivery),
      cmocka_unit_test(test_mem_uds_consumer_service_multi_worker),
      cmocka_unit_test(test_mem_uds_consumer_service_with_state),
      cmocka_unit_test(
          test_mem_uds_consumer_service_restart_after_handler_failure),
      cmocka_unit_test(test_mem_uds_consumer_service_defer_then_redeliver),
      cmocka_unit_test(test_mem_uds_dequeue_with_state_roundtrip),
      cmocka_unit_test(test_s3_dequeue_with_state_roundtrip),
      cmocka_unit_test(test_mem_uds_dequeue_batch_roundtrip),
      cmocka_unit_test(test_mem_uds_queue_roundtrip)};

  return cmocka_run_group_tests(tests, NULL, NULL);
}

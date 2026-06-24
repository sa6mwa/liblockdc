#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <pthread.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cmocka.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "lc/lc.h"
#include "lc_api_internal.h"

#define TEST_POUCH_QUERY_INDEX_HEADER_SIZE 64U
#define TEST_POUCH_QUERY_INDEX_PAYLOAD_LENGTH_OFFSET 44U
#define TEST_POUCH_QUERY_INDEX_RECORD_VERSION_OFFSET 56U

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

static void test_endpoint_with_query(char *buffer, size_t buffer_size,
                                     const char *root, const char *query) {
  snprintf(buffer, buffer_size, "pouch://%s?%s", root, query);
}

static void test_cleanup_root(const char *root) {
  char path[512];

  snprintf(path, sizeof(path), "%s/store.compact.tmp", root);
  unlink(path);
  snprintf(path, sizeof(path), "%s/query.index.compact.tmp", root);
  unlink(path);
  snprintf(path, sizeof(path), "%s/store.log", root);
  unlink(path);
  snprintf(path, sizeof(path), "%s/query.index", root);
  unlink(path);
  snprintf(path, sizeof(path), "%s/writer.lock", root);
  unlink(path);
  rmdir(root);
}

static off_t test_query_index_size(const char *root) {
  char path[512];
  struct stat st;

  snprintf(path, sizeof(path), "%s/query.index", root);
  assert_int_equal(stat(path, &st), 0);
  return st.st_size;
}

static unsigned long test_get_u64(const unsigned char *bytes) {
  return ((unsigned long)bytes[0]) | ((unsigned long)bytes[1] << 8) |
         ((unsigned long)bytes[2] << 16) | ((unsigned long)bytes[3] << 24) |
         ((unsigned long)bytes[4] << 32) | ((unsigned long)bytes[5] << 40) |
         ((unsigned long)bytes[6] << 48) | ((unsigned long)bytes[7] << 56);
}

static void test_put_u64(unsigned char *bytes, unsigned long value) {
  bytes[0] = (unsigned char)(value & 0xffU);
  bytes[1] = (unsigned char)((value >> 8) & 0xffU);
  bytes[2] = (unsigned char)((value >> 16) & 0xffU);
  bytes[3] = (unsigned char)((value >> 24) & 0xffU);
  bytes[4] = (unsigned char)((value >> 32) & 0xffU);
  bytes[5] = (unsigned char)((value >> 40) & 0xffU);
  bytes[6] = (unsigned char)((value >> 48) & 0xffU);
  bytes[7] = (unsigned char)((value >> 56) & 0xffU);
}

static int test_bytes_contains(const unsigned char *haystack,
                               size_t haystack_len, const char *needle,
                               size_t needle_len) {
  size_t index;

  if (needle_len == 0U || needle_len > haystack_len) {
    return 0;
  }
  for (index = 0U; index + needle_len <= haystack_len; ++index) {
    if (memcmp(haystack + index, needle, needle_len) == 0) {
      return 1;
    }
  }
  return 0;
}

static void set_first_query_index_match_record_version(const char *root,
                                                       const char *needle,
                                                       unsigned long version) {
  char path[512];
  unsigned char header[TEST_POUCH_QUERY_INDEX_HEADER_SIZE];
  unsigned char *payload;
  size_t needle_len;
  unsigned long payload_len;
  off_t record_offset;
  int fd;
  int found;

  snprintf(path, sizeof(path), "%s/query.index", root);
  fd = open(path, O_RDWR);
  assert_true(fd >= 0);
  assert_int_equal(lseek(fd, 0, SEEK_SET), 0);
  needle_len = strlen(needle);
  found = 0;
  while (!found) {
    record_offset = lseek(fd, 0, SEEK_CUR);
    assert_true(record_offset >= 0);
    assert_int_equal(read(fd, header, sizeof(header)), sizeof(header));
    assert_memory_equal(header, "LCQI", 4U);
    payload_len =
        test_get_u64(header + TEST_POUCH_QUERY_INDEX_PAYLOAD_LENGTH_OFFSET);
    payload = (unsigned char *)malloc((size_t)payload_len);
    assert_non_null(payload);
    assert_int_equal(read(fd, payload, (size_t)payload_len), payload_len);
    if (test_bytes_contains(payload, (size_t)payload_len, needle,
                            needle_len)) {
      test_put_u64(header + TEST_POUCH_QUERY_INDEX_RECORD_VERSION_OFFSET,
                   version);
      assert_int_equal(lseek(fd, record_offset, SEEK_SET), record_offset);
      assert_int_equal(write(fd, header, sizeof(header)), sizeof(header));
      found = 1;
    }
    free(payload);
  }
  assert_int_equal(close(fd), 0);
  assert_true(found);
}

static void corrupt_query_index_tail(const char *root) {
  static const unsigned char garbage[] = {0x7fU, 0x55U, 0x13U};
  char path[512];
  FILE *file;

  snprintf(path, sizeof(path), "%s/query.index", root);
  file = fopen(path, "ab");
  assert_non_null(file);
  assert_int_equal(fwrite(garbage, 1U, sizeof(garbage), file),
                   sizeof(garbage));
  assert_int_equal(fclose(file), 0);
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

static void assert_pouch_client_state_text(lc_client *client, const char *key,
                                           const char *expected,
                                           lc_error *error) {
  lc_sink *sink;
  lc_get_res get_res;
  char *text;
  int rc;

  sink = NULL;
  memset(&get_res, 0, sizeof(get_res));
  rc = lc_sink_to_memory(&sink, error);
  assert_int_equal(rc, LC_OK);
  rc = client->get(client, key, NULL, sink, &get_res, error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  text = memory_sink_text(sink);
  assert_string_equal(text, expected);
  free(text);
  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
}

static void seed_pouch_staged_state(lc_client *client, lc_lease *lease,
                                    const char *body, lc_error *error) {
  lc_client_handle *handle;
  lc_source *source;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res res;
  int rc;

  handle = (lc_client_handle *)client;
  memset(&opts, 0, sizeof(opts));
  memset(&res, 0, sizeof(res));
  opts.content_type = "application/json";
  source = source_from_text(body);
  rc = handle->pouch_store->stage_state(
      handle->pouch_store, lease->namespace_name, lease->key, lease->txn_id,
      source, &opts, &res, error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&handle->pouch_allocator, &res);
}

static void partial_pouch_rollback_participant(lc_client *client,
                                               const char *namespace_name,
                                               const char *key,
                                               const char *txn_id,
                                               lc_error *error) {
  lc_client_handle *handle;
  lc_pouch_meta_record record;
  lc_pouch_store_meta_res stored;
  lc_pouch_discard_staged_opts discard_opts;
  lc_pouch_meta next_meta;
  int rc;

  handle = (lc_client_handle *)client;
  memset(&record, 0, sizeof(record));
  memset(&stored, 0, sizeof(stored));
  memset(&discard_opts, 0, sizeof(discard_opts));
  rc = handle->pouch_store->load_meta(handle->pouch_store, namespace_name, key,
                                      &record, error);
  assert_int_equal(rc, LC_OK);
  assert_true(record.found);
  assert_string_equal(record.meta.txn_id, txn_id);

  discard_opts.ignore_not_found = 1;
  rc = handle->pouch_store->discard_staged_state(
      handle->pouch_store, namespace_name, key, txn_id, &discard_opts, error);
  assert_int_equal(rc, LC_OK);

  next_meta = record.meta;
  next_meta.owner = NULL;
  next_meta.lease_id = NULL;
  next_meta.txn_id = NULL;
  next_meta.lease_expires_at_unix = 0L;
  rc = handle->pouch_store->store_meta(handle->pouch_store, namespace_name, key,
                                       &next_meta, record.etag, &stored,
                                       error);
  assert_int_equal(rc, LC_OK);

  lc_pouch_store_meta_res_cleanup(&handle->pouch_allocator, &stored);
  lc_pouch_meta_record_cleanup(&handle->pouch_allocator, &record);
}

static void expire_pouch_participant_lease(lc_client *client,
                                           const char *namespace_name,
                                           const char *key,
                                           const char *txn_id,
                                           lc_error *error) {
  lc_client_handle *handle;
  lc_pouch_meta_record record;
  lc_pouch_store_meta_res stored;
  lc_pouch_meta next_meta;
  int rc;

  handle = (lc_client_handle *)client;
  memset(&record, 0, sizeof(record));
  memset(&stored, 0, sizeof(stored));
  rc = handle->pouch_store->load_meta(handle->pouch_store, namespace_name, key,
                                      &record, error);
  assert_int_equal(rc, LC_OK);
  assert_true(record.found);
  assert_string_equal(record.meta.txn_id, txn_id);

  next_meta = record.meta;
  next_meta.lease_expires_at_unix = 1L;
  rc = handle->pouch_store->store_meta(handle->pouch_store, namespace_name, key,
                                       &next_meta, record.etag, &stored,
                                       error);
  assert_int_equal(rc, LC_OK);

  lc_pouch_store_meta_res_cleanup(&handle->pouch_allocator, &stored);
  lc_pouch_meta_record_cleanup(&handle->pouch_allocator, &record);
}

static void assert_pouch_staged_state_missing(lc_client *client,
                                              const char *namespace_name,
                                              const char *key,
                                              const char *txn_id,
                                              lc_error *error) {
  lc_client_handle *handle;
  lc_source *body;
  lc_pouch_state_info info;
  int rc;

  handle = (lc_client_handle *)client;
  body = NULL;
  memset(&info, 0, sizeof(info));
  rc = handle->pouch_store->load_staged_state(handle->pouch_store,
                                              namespace_name, key, txn_id,
                                              &body, &info, error);
  assert_int_equal(rc, LC_OK);
  assert_true(info.no_content);
  assert_null(body);
  lc_pouch_state_info_cleanup(&handle->pouch_allocator, &info);
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

static lc_client *open_pouch_client_with_query_config(
    const char *endpoint, const char *preferred_engine,
    const char *fallback_engine) {
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
  config.pouch_query_engine = preferred_engine;
  config.pouch_query_fallback_engine = fallback_engine;
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(client);
  lc_error_cleanup(&error);
  return client;
}

static lc_client *open_pouch_client(const char *endpoint) {
  return open_pouch_client_with_namespace(endpoint, "default");
}

typedef struct delayed_enqueue_context {
  const char *endpoint;
  const char *queue;
  const char *body;
  int rc;
  lc_error error;
} delayed_enqueue_context;

static void *delayed_enqueue_main(void *arg) {
  delayed_enqueue_context *ctx;
  lc_client *client;
  lc_enqueue_req req;
  lc_enqueue_res res;
  lc_source *source;

  ctx = (delayed_enqueue_context *)arg;
  memset(&ctx->error, 0, sizeof(ctx->error));
  ctx->rc = LC_ERR_TRANSPORT;
  usleep(200000U);
  client = open_pouch_client(ctx->endpoint);
  memset(&res, 0, sizeof(res));
  lc_enqueue_req_init(&req);
  req.queue = ctx->queue;
  req.content_type = "text/plain";
  req.visibility_timeout_seconds = 30L;
  req.ttl_seconds = 3600L;
  source = source_from_text(ctx->body);
  ctx->rc = client->enqueue(client, &req, source, &res, &ctx->error);
  lc_source_close(source);
  lc_enqueue_res_cleanup(&res);
  client->close(client);
  return NULL;
}

typedef struct subscribe_test_state {
  size_t handled;
  const char *expected[2];
} subscribe_test_state;

typedef struct watch_test_state {
  size_t handled;
  int available;
  char queue[64];
  char head_message_id[128];
  char correlation_id[64];
  int fail;
  int fail_without_error;
} watch_test_state;

typedef struct consumer_service_test_state {
  lc_consumer_service *service;
  size_t handled;
  const char *expected[2];
} consumer_service_test_state;

typedef struct attach_reject_store {
  lc_pouch_store pub;
  int load_meta_called;
  int put_object_called;
  int store_meta_called;
  int delete_object_called;
  char deleted_id[32];
} attach_reject_store;

static int attach_reject_load_meta(lc_pouch_store *self,
                                   const char *namespace_name,
                                   const char *key,
                                   lc_pouch_meta_record *out,
                                   lc_error *error) {
  attach_reject_store *store;
  (void)error;
  store = (attach_reject_store *)self->impl;
  store->load_meta_called = 1;
  out->found = 1;
  out->namespace_name = strdup(namespace_name);
  out->key = strdup(key);
  out->etag = strdup("meta-etag-1");
  out->meta.owner = strdup("owner-a");
  out->meta.lease_id = strdup("lease-a");
  out->meta.txn_id = strdup("txn-a");
  out->meta.version = 7L;
  out->meta.lease_expires_at_unix = 4102444800L;
  out->meta.fencing_token = 11L;
  assert_non_null(out->namespace_name);
  assert_non_null(out->key);
  assert_non_null(out->etag);
  assert_non_null(out->meta.owner);
  assert_non_null(out->meta.lease_id);
  assert_non_null(out->meta.txn_id);
  return LC_OK;
}

static int attach_reject_store_meta(lc_pouch_store *self,
                                    const char *namespace_name,
                                    const char *key,
                                    const lc_pouch_meta *meta,
                                    const char *expected_etag,
                                    lc_pouch_store_meta_res *out,
                                    lc_error *error) {
  attach_reject_store *store;
  (void)namespace_name;
  (void)key;
  (void)out;
  store = (attach_reject_store *)self->impl;
  store->store_meta_called = 1;
  assert_int_equal(meta->version, 8L);
  assert_string_equal(expected_etag, "meta-etag-1");
  return lc_error_set(error, LC_ERR_SERVER, 412L,
                      "metadata precondition failed", NULL,
                      "etag_mismatch", NULL);
}

static int attach_reject_put_object(lc_pouch_store *self,
                                    const char *namespace_name,
                                    const char *key, lc_source *body,
                                    const lc_pouch_put_object_opts *opts,
                                    lc_pouch_object_info *out,
                                    lc_error *error) {
  attach_reject_store *store;
  (void)namespace_name;
  (void)key;
  (void)body;
  (void)error;
  store = (attach_reject_store *)self->impl;
  store->put_object_called = 1;
  assert_string_equal(opts->name, "payload.txt");
  assert_string_equal(opts->content_type, "text/plain");
  out->id = strdup("object-1");
  out->name = strdup("payload.txt");
  out->content_type = strdup("text/plain");
  out->size = 7L;
  assert_non_null(out->id);
  assert_non_null(out->name);
  assert_non_null(out->content_type);
  return LC_OK;
}

static int attach_reject_delete_object(lc_pouch_store *self,
                                       const char *namespace_name,
                                       const char *key,
                                       const lc_pouch_object_selector *selector,
                                       int *deleted, lc_error *error) {
  attach_reject_store *store;
  (void)namespace_name;
  (void)key;
  (void)error;
  store = (attach_reject_store *)self->impl;
  store->delete_object_called = 1;
  snprintf(store->deleted_id, sizeof(store->deleted_id), "%s", selector->id);
  assert_string_equal(selector->id, "object-1");
  assert_null(selector->name);
  *deleted = 1;
  return LC_OK;
}

static void test_pouch_endpoint_attach_rolls_back_object_on_meta_reject(
    void **state) {
  attach_reject_store store;
  lc_client_handle client;
  lc_attach_op op;
  lc_attach_res res;
  lc_source *source;
  lc_error error;
  int rc;

  (void)state;
  memset(&store, 0, sizeof(store));
  memset(&client, 0, sizeof(client));
  memset(&op, 0, sizeof(op));
  memset(&res, 0, sizeof(res));
  memset(&error, 0, sizeof(error));
  store.pub.impl = &store;
  store.pub.load_meta = attach_reject_load_meta;
  store.pub.store_meta = attach_reject_store_meta;
  store.pub.put_object = attach_reject_put_object;
  store.pub.delete_object = attach_reject_delete_object;
  client.pouch_store = &store.pub;
  lc_pouch_allocator_from_lc(NULL, &client.pouch_allocator);
  op.lease.namespace_name = "default";
  op.lease.key = "key-a";
  op.lease.lease_id = "lease-a";
  op.lease.txn_id = "txn-a";
  op.lease.fencing_token = 11L;
  op.name = "payload.txt";
  op.content_type = "text/plain";
  source = source_from_text("payload");

  rc = lc_pouch_client_attach_method(&client.pub, &op, source, &res, &error);

  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 412L);
  assert_string_equal(error.server_code, "etag_mismatch");
  assert_true(store.load_meta_called);
  assert_true(store.put_object_called);
  assert_true(store.store_meta_called);
  assert_true(store.delete_object_called);
  assert_string_equal(store.deleted_id, "object-1");
  lc_source_close(source);
  lc_attach_res_cleanup(&res);
  lc_error_cleanup(&error);
}

typedef struct consumer_service_stateful_test_state {
  lc_consumer_service *service;
  size_t handled;
  const char *expected;
} consumer_service_stateful_test_state;

typedef struct query_key_capture_state {
  char keys[8][64];
  char current[64];
  size_t key_count;
  size_t begin_calls;
  size_t chunk_calls;
  size_t end_calls;
  size_t current_length;
  int fail_on_chunk;
} query_key_capture_state;

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

static void child_enqueue_after_delay(const char *endpoint, const char *queue,
                                      const char *payload) {
  lc_client_config config;
  lc_client *client;
  lc_enqueue_req req;
  lc_enqueue_res res;
  lc_source *source;
  lc_error error;
  const char *endpoints[1];
  int rc;

  sleep(1U);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  lc_client_config_init(&config);
  endpoints[0] = endpoint;
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "default";
  client = NULL;
  rc = lc_client_open(&config, &client, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    _exit(21);
  }
  lc_enqueue_req_init(&req);
  req.queue = queue;
  req.content_type = "text/plain";
  req.visibility_timeout_seconds = 30L;
  req.ttl_seconds = 60L;
  req.max_attempts = 3;
  source = NULL;
  rc = lc_source_from_memory(payload, strlen(payload), &source, &error);
  if (rc == LC_OK) {
    rc = client->enqueue(client, &req, source, &res, &error);
    lc_source_close(source);
  }
  lc_enqueue_res_cleanup(&res);
  client->close(client);
  lc_error_cleanup(&error);
  _exit(rc == LC_OK ? 0 : 22);
}

static int watch_test_handle(void *context, const lc_watch_event *event,
                             lc_error *error) {
  watch_test_state *state;

  state = (watch_test_state *)context;
  state->handled += 1U;
  state->available = event->available;
  if (event->queue != NULL) {
    snprintf(state->queue, sizeof(state->queue), "%s", event->queue);
  }
  if (event->head_message_id != NULL) {
    snprintf(state->head_message_id, sizeof(state->head_message_id), "%s",
             event->head_message_id);
  }
  if (event->correlation_id != NULL) {
    snprintf(state->correlation_id, sizeof(state->correlation_id), "%s",
             event->correlation_id);
  }
  if (state->fail) {
    error->code = LC_ERR_TRANSPORT;
    error->message =
        (char *)malloc(strlen("watch callback stopped") + 1U);
    assert_non_null(error->message);
    strcpy(error->message, "watch callback stopped");
    return LC_ERR_TRANSPORT;
  }
  if (state->fail_without_error) {
    return LC_ERR_TRANSPORT;
  }
  return LC_OK;
}

static int query_key_begin_unexpected(void *context, lc_error *error) {
  (void)context;
  (void)error;
  fail_msg("pouch query_keys handler must not be called");
  return LC_ERR_INVALID;
}

static int query_key_chunk_unexpected(void *context, const char *bytes,
                                      size_t len, lc_error *error) {
  (void)context;
  (void)bytes;
  (void)len;
  (void)error;
  fail_msg("pouch query_keys handler must not be called");
  return LC_ERR_INVALID;
}

static int query_key_end_unexpected(void *context, lc_error *error) {
  (void)context;
  (void)error;
  fail_msg("pouch query_keys handler must not be called");
  return LC_ERR_INVALID;
}

static int query_key_capture_begin(void *context, lc_error *error) {
  query_key_capture_state *state;

  (void)error;
  state = (query_key_capture_state *)context;
  state->begin_calls += 1U;
  state->current_length = 0U;
  state->current[0] = '\0';
  return 1;
}

static int query_key_capture_chunk(void *context, const char *bytes,
                                   size_t len, lc_error *error) {
  query_key_capture_state *state;
  size_t available;

  state = (query_key_capture_state *)context;
  state->chunk_calls += 1U;
  if (state->fail_on_chunk) {
    error->code = LC_ERR_TRANSPORT;
    error->message =
        (char *)malloc(strlen("query key capture rejected chunk") + 1U);
    assert_non_null(error->message);
    strcpy(error->message, "query key capture rejected chunk");
    return 0;
  }
  available = sizeof(state->current) - state->current_length - 1U;
  if (len > available) {
    len = available;
  }
  if (len > 0U) {
    memcpy(state->current + state->current_length, bytes, len);
    state->current_length += len;
    state->current[state->current_length] = '\0';
  }
  return 1;
}

static int query_key_capture_end(void *context, lc_error *error) {
  query_key_capture_state *state;

  (void)error;
  state = (query_key_capture_state *)context;
  assert_true(state->key_count < 8U);
  snprintf(state->keys[state->key_count], sizeof(state->keys[0]), "%s",
           state->current);
  state->key_count += 1U;
  state->end_calls += 1U;
  state->current_length = 0U;
  state->current[0] = '\0';
  return 1;
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
  assert_non_null(lease->txn_id);
  assert_true(lease->txn_id[0] != '\0');
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
  assert_string_equal(describe_res.txn_id, lease->txn_id);
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
  assert_int_equal(lease->version, 2L);
  assert_true(lease->has_query_hidden);
  assert_true(lease->query_hidden);

  lc_describe_req_init(&describe_req);
  describe_req.key = "alpha";
  rc = client->describe(client, &describe_req, &describe_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(describe_res.version, lease->version);
  assert_true(describe_res.has_query_hidden);
  assert_true(describe_res.query_hidden);
  lc_describe_res_cleanup(&describe_res);

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
  assert_int_equal(lease->version, 3L);
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
  assert_int_equal(lease->version, 4L);
  assert_string_equal(zeta_attach_res.attachment.name, "zeta.txt");

  lc_attach_req_init(&attach_req);
  attach_req.name = "alpha.txt";
  attach_req.content_type = "text/plain";
  attach_req.prevent_overwrite = 1;
  source = source_from_text("alpha-body");
  rc = lease->attach(lease, &attach_req, source, &alpha_attach_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 5L);
  assert_string_equal(alpha_attach_res.attachment.name, "alpha.txt");

  source = source_from_text("duplicate-body");
  rc = lease->attach(lease, &attach_req, source, &duplicate_attach_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 409L);
  assert_int_equal(lease->version, 5L);
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
  assert_int_equal(lease->version, 6L);

  rc = lease->delete_attachment(lease, &attachment_selector, &deleted, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(deleted);
  assert_int_equal(lease->version, 6L);

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
  assert_int_equal(lease->version, 7L);

  rc = lease->list_attachments(lease, &attachment_list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(attachment_list.count, 0U);
  lc_attachment_list_cleanup(&attachment_list);

  lc_remove_req_init(&remove_req);
  remove_req.if_state_etag = lease->state_etag;
  rc = lease->remove(lease, &remove_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_null(lease->state_etag);
  assert_int_equal(lease->version, 8L);

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

static void test_pouch_endpoint_lease_mutate_local_updates_state(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_acquire_req acquire;
  lc_mutate_local_req mutate_req;
  lc_get_res get_res;
  lc_release_req release_req;
  lc_sink *sink;
  lc_error error;
  const char *mutations[2];
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "mutate-local");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&get_res, 0, sizeof(get_res));
  client = open_pouch_client(endpoint);

  lc_acquire_req_init(&acquire);
  acquire.key = "mutable";
  acquire.owner = "owner-a";
  acquire.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);
  assert_int_equal(lease->version, 0L);
  assert_null(lease->state_etag);

  mutations[0] = "/name=\"pouch\"";
  mutations[1] = "/counter=1";
  lc_mutate_local_req_init(&mutate_req);
  mutate_req.mutations = mutations;
  mutate_req.mutation_count = 2U;
  rc = lease->mutate_local(lease, &mutate_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 1L);
  assert_non_null(lease->state_etag);

  mutations[0] = "/counter=2";
  lc_mutate_local_req_init(&mutate_req);
  mutate_req.mutations = mutations;
  mutate_req.mutation_count = 1U;
  rc = lc_lease_mutate_local(lease, &mutate_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 2L);
  assert_non_null(lease->state_etag);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = lease->get(lease, sink, NULL, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  assert_string_equal(get_res.content_type, "application/json");
  assert_int_equal(get_res.version, 2L);
  assert_string_equal(get_res.etag, lease->state_etag);
  text = memory_sink_text(sink);
  assert_true(strstr(text, "\"name\":\"pouch\"") != NULL);
  assert_true(strstr(text, "\"counter\":3") != NULL);
  free(text);
  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);

  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_rejects_missing_acquire_owner(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_acquire_req acquire;
  lc_release_req release_req;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "missing-acquire-owner");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  client = open_pouch_client(endpoint);

  lc_acquire_req_init(&acquire);
  acquire.key = "owner-required";
  acquire.ttl_seconds = 60L;
  lease = NULL;
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 400L);
  assert_string_equal(error.server_code, "missing_owner");
  assert_null(lease);
  lc_error_cleanup(&error);

  acquire.owner = "";
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 400L);
  assert_string_equal(error.server_code, "missing_owner");
  assert_null(lease);
  lc_error_cleanup(&error);

  acquire.owner = "owner-a";
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);
  assert_string_equal(lease->owner, "owner-a");

  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_generates_implicit_txn_id(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_acquire_req acquire;
  lc_update_req update_req;
  lc_update_res update_res;
  lc_release_op release_op;
  lc_release_res release_res;
  lc_source *source;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "implicit-txn");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&update_res, 0, sizeof(update_res));
  memset(&release_res, 0, sizeof(release_res));
  client = open_pouch_client(endpoint);

  lc_acquire_req_init(&acquire);
  acquire.key = "implicit-txn";
  acquire.owner = "owner-a";
  acquire.ttl_seconds = 60L;
  lease = NULL;
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);
  assert_non_null(lease->txn_id);
  assert_true(lease->txn_id[0] != '\0');
  assert_null(strchr(lease->txn_id, '/'));

  lc_update_req_init(&update_req);
  update_req.lease.namespace_name = lease->namespace_name;
  update_req.lease.key = lease->key;
  update_req.lease.lease_id = lease->lease_id;
  update_req.lease.fencing_token = lease->fencing_token;
  update_req.content_type = "application/json";
  source = source_from_text("{\"missing_txn\":true}");
  rc = client->update(client, &update_req, source, &update_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 400L);
  assert_string_equal(error.server_code, "missing_txn");
  lc_update_res_cleanup(&update_res);
  lc_error_cleanup(&error);

  update_req.lease.txn_id = lease->txn_id;
  source = source_from_text("{\"ok\":true}");
  rc = client->update(client, &update_req, source, &update_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(update_res.new_version, 1L);
  lc_update_res_cleanup(&update_res);

  lc_release_op_init(&release_op);
  release_op.lease.namespace_name = lease->namespace_name;
  release_op.lease.key = lease->key;
  release_op.lease.lease_id = lease->lease_id;
  release_op.lease.fencing_token = lease->fencing_token;
  rc = client->release(client, &release_op, &release_res, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 400L);
  assert_string_equal(error.server_code, "missing_txn");
  lc_release_res_cleanup(&release_res);
  lc_error_cleanup(&error);

  release_op.lease.txn_id = lease->txn_id;
  rc = client->release(client, &release_op, &release_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(release_res.released);
  lc_release_res_cleanup(&release_res);
  lc_lease_close(lease);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_release_is_idempotent_for_stale_refs(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *first_lease;
  lc_lease *second_lease;
  lc_acquire_req acquire;
  lc_release_op release_op;
  lc_release_res release_res;
  lc_update_req update_req;
  lc_update_res update_res;
  lc_source *source;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "release-idempotent");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&release_res, 0, sizeof(release_res));
  memset(&update_res, 0, sizeof(update_res));
  client = open_pouch_client(endpoint);

  lc_acquire_req_init(&acquire);
  acquire.key = "release-idempotent";
  acquire.owner = "owner-a";
  acquire.ttl_seconds = 60L;
  first_lease = NULL;
  rc = client->acquire(client, &acquire, &first_lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(first_lease);

  lc_release_op_init(&release_op);
  release_op.lease.namespace_name = first_lease->namespace_name;
  release_op.lease.key = first_lease->key;
  release_op.lease.lease_id = first_lease->lease_id;
  release_op.lease.txn_id = first_lease->txn_id;
  release_op.lease.fencing_token = first_lease->fencing_token;
  rc = client->release(client, &release_op, &release_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(release_res.released);
  lc_release_res_cleanup(&release_res);

  rc = client->release(client, &release_op, &release_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(release_res.released);
  lc_release_res_cleanup(&release_res);

  acquire.owner = "owner-b";
  second_lease = NULL;
  rc = client->acquire(client, &acquire, &second_lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(second_lease);
  assert_int_equal(second_lease->fencing_token, first_lease->fencing_token + 1L);

  rc = client->release(client, &release_op, &release_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(release_res.released);
  lc_release_res_cleanup(&release_res);

  lc_update_req_init(&update_req);
  update_req.lease.namespace_name = second_lease->namespace_name;
  update_req.lease.key = second_lease->key;
  update_req.lease.lease_id = second_lease->lease_id;
  update_req.lease.txn_id = second_lease->txn_id;
  update_req.lease.fencing_token = second_lease->fencing_token;
  update_req.content_type = "application/json";
  source = source_from_text("{\"still_active\":true}");
  rc = client->update(client, &update_req, source, &update_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(update_res.new_version, 1L);
  lc_update_res_cleanup(&update_res);

  release_op.lease.namespace_name = second_lease->namespace_name;
  release_op.lease.key = second_lease->key;
  release_op.lease.lease_id = second_lease->lease_id;
  release_op.lease.txn_id = second_lease->txn_id;
  release_op.lease.fencing_token = second_lease->fencing_token;
  rc = client->release(client, &release_op, &release_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(release_res.released);
  lc_release_res_cleanup(&release_res);

  lc_lease_close(first_lease);
  lc_lease_close(second_lease);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_reports_lockd_lease_validation_errors(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_acquire_req acquire;
  lc_release_op release_op;
  lc_release_res release_res;
  lc_update_req update_req;
  lc_update_res update_res;
  lc_source *source;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "lease-validation-errors");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&release_res, 0, sizeof(release_res));
  memset(&update_res, 0, sizeof(update_res));
  client = open_pouch_client(endpoint);

  lc_acquire_req_init(&acquire);
  acquire.key = "lease-validation";
  acquire.owner = "owner-a";
  acquire.ttl_seconds = 60L;
  lease = NULL;
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  lc_update_req_init(&update_req);
  update_req.lease.namespace_name = lease->namespace_name;
  update_req.lease.key = lease->key;
  update_req.lease.lease_id = lease->lease_id;
  update_req.lease.txn_id = lease->txn_id;
  update_req.lease.fencing_token = lease->fencing_token + 1L;
  update_req.content_type = "application/json";
  source = source_from_text("{\"fencing\":false}");
  rc = client->update(client, &update_req, source, &update_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 403L);
  assert_string_equal(error.server_code, "fencing_mismatch");
  lc_update_res_cleanup(&update_res);
  lc_error_cleanup(&error);

  lc_release_op_init(&release_op);
  release_op.lease.namespace_name = lease->namespace_name;
  release_op.lease.key = lease->key;
  release_op.lease.lease_id = lease->lease_id;
  release_op.lease.txn_id = lease->txn_id;
  release_op.lease.fencing_token = lease->fencing_token;
  rc = client->release(client, &release_op, &release_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(release_res.released);
  lc_release_res_cleanup(&release_res);

  update_req.lease.fencing_token = lease->fencing_token;
  source = source_from_text("{\"released\":false}");
  rc = client->update(client, &update_req, source, &update_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 403L);
  assert_string_equal(error.server_code, "lease_required");
  lc_update_res_cleanup(&update_res);
  lc_error_cleanup(&error);

  lc_lease_close(lease);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_attachment_selector_requires_matching_id_and_name(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_acquire_req acquire;
  lc_attach_req attach_req;
  lc_attach_res first_attach;
  lc_attach_res second_attach;
  lc_attachment_get_op get_op;
  lc_attachment_get_res get_res;
  lc_attachment_delete_op delete_op;
  lc_release_req release_req;
  lc_source *source;
  lc_sink *sink;
  lc_error error;
  char *text;
  int deleted;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "attachment-selector-match");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&first_attach, 0, sizeof(first_attach));
  memset(&second_attach, 0, sizeof(second_attach));
  memset(&get_res, 0, sizeof(get_res));
  client = open_pouch_client(endpoint);
  lease = NULL;
  sink = NULL;

  lc_acquire_req_init(&acquire);
  acquire.key = "attachment-selector";
  acquire.owner = "owner-a";
  acquire.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  lc_attach_req_init(&attach_req);
  attach_req.name = "current.txt";
  attach_req.content_type = "text/plain";
  source = source_from_text("old-body");
  rc = lease->attach(lease, &attach_req, source, &first_attach, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_non_null(first_attach.attachment.id);

  source = source_from_text("new-body");
  rc = lease->attach(lease, &attach_req, source, &second_attach, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_non_null(second_attach.attachment.id);
  assert_string_not_equal(first_attach.attachment.id,
                          second_attach.attachment.id);

  lc_attachment_get_op_init(&get_op);
  get_op.lease.namespace_name = lease->namespace_name;
  get_op.lease.key = lease->key;
  get_op.lease.lease_id = lease->lease_id;
  get_op.lease.txn_id = lease->txn_id;
  get_op.lease.fencing_token = lease->fencing_token;
  get_op.selector.id = first_attach.attachment.id;
  get_op.selector.name = "current.txt";
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get_attachment(client, &get_op, sink, &get_res, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 404L);
  assert_string_equal(error.server_code, "not_found");
  lc_attachment_get_res_cleanup(&get_res);
  lc_sink_close(sink);
  sink = NULL;
  lc_error_cleanup(&error);

  lc_attachment_delete_op_init(&delete_op);
  delete_op.lease = get_op.lease;
  delete_op.selector.id = first_attach.attachment.id;
  delete_op.selector.name = "current.txt";
  deleted = 1;
  rc = client->delete_attachment(client, &delete_op, &deleted, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(deleted);

  lc_attachment_get_op_init(&get_op);
  get_op.lease = delete_op.lease;
  get_op.selector.name = "current.txt";
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get_attachment(client, &get_op, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(get_res.attachment.id, second_attach.attachment.id);
  text = memory_sink_text(sink);
  assert_string_equal(text, "new-body");
  free(text);
  lc_attachment_get_res_cleanup(&get_res);
  lc_sink_close(sink);

  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  lease = NULL;

  client->close(client);
  lc_attach_res_cleanup(&first_attach);
  lc_attach_res_cleanup(&second_attach);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_public_attachment_read_after_release(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_acquire_req acquire;
  lc_attach_req attach_req;
  lc_attach_res attach_res;
  lc_attachment_list_req list_req;
  lc_attachment_list attachment_list;
  lc_attachment_get_op get_op;
  lc_attachment_get_res get_res;
  lc_release_req release_req;
  lc_lease_ref ref;
  lc_source *source;
  lc_sink *sink;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "attachment-public-read");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&attach_res, 0, sizeof(attach_res));
  memset(&attachment_list, 0, sizeof(attachment_list));
  memset(&get_res, 0, sizeof(get_res));
  memset(&ref, 0, sizeof(ref));
  client = open_pouch_client(endpoint);
  lease = NULL;
  sink = NULL;

  lc_acquire_req_init(&acquire);
  acquire.key = "attachment-public";
  acquire.owner = "owner-a";
  acquire.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  lc_attach_req_init(&attach_req);
  attach_req.name = "public.txt";
  attach_req.content_type = "text/plain";
  attach_req.prevent_overwrite = 1;
  source = source_from_text("public-body");
  rc = lease->attach(lease, &attach_req, source, &attach_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_non_null(attach_res.attachment.id);

  ref.namespace_name = strdup(lease->namespace_name);
  ref.key = strdup(lease->key);
  ref.lease_id = strdup(lease->lease_id);
  ref.txn_id = strdup(lease->txn_id);
  ref.fencing_token = lease->fencing_token;
  assert_non_null(ref.namespace_name);
  assert_non_null(ref.key);
  assert_non_null(ref.lease_id);
  assert_non_null(ref.txn_id);

  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  lease = NULL;

  lc_attachment_list_req_init(&list_req);
  list_req.lease = ref;
  list_req.public_read = 1;
  rc = client->list_attachments(client, &list_req, &attachment_list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(attachment_list.count, 1U);
  assert_string_equal(attachment_list.items[0].name, "public.txt");
  assert_string_equal(attachment_list.items[0].id, attach_res.attachment.id);
  lc_attachment_list_cleanup(&attachment_list);

  lc_attachment_get_op_init(&get_op);
  get_op.lease = ref;
  get_op.selector.name = "public.txt";
  get_op.public_read = 1;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get_attachment(client, &get_op, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(get_res.attachment.id, attach_res.attachment.id);
  assert_string_equal(get_res.attachment.content_type, "text/plain");
  text = memory_sink_text(sink);
  assert_string_equal(text, "public-body");
  free(text);
  lc_sink_close(sink);
  sink = NULL;
  lc_attachment_get_res_cleanup(&get_res);

  client->close(client);
  free((char *)ref.namespace_name);
  free((char *)ref.key);
  free((char *)ref.lease_id);
  free((char *)ref.txn_id);
  lc_attach_res_cleanup(&attach_res);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_attachment_rejects_stale_lease_refs(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_lease *fresh_lease;
  lc_acquire_req acquire;
  lc_attach_req lease_attach_req;
  lc_attach_res lease_attach_res;
  lc_release_req release_req;
  lc_lease_ref stale_ref;
  lc_attach_op attach_op;
  lc_attach_res attach_res;
  lc_attachment_list_req list_req;
  lc_attachment_list attachment_list;
  lc_attachment_get_op get_op;
  lc_attachment_get_res get_res;
  lc_attachment_delete_op delete_op;
  lc_attachment_delete_all_op delete_all_op;
  lc_attachment_selector selector;
  lc_source *source;
  lc_sink *sink;
  lc_error error;
  int deleted;
  int deleted_count;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "attachment-stale-lease");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&lease_attach_res, 0, sizeof(lease_attach_res));
  memset(&attach_res, 0, sizeof(attach_res));
  memset(&attachment_list, 0, sizeof(attachment_list));
  memset(&get_res, 0, sizeof(get_res));
  memset(&stale_ref, 0, sizeof(stale_ref));
  client = open_pouch_client(endpoint);
  lease = NULL;
  fresh_lease = NULL;
  sink = NULL;

  lc_acquire_req_init(&acquire);
  acquire.key = "attachment-stale";
  acquire.owner = "owner-a";
  acquire.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);

  stale_ref.namespace_name = strdup(lease->namespace_name);
  stale_ref.key = strdup(lease->key);
  stale_ref.lease_id = strdup(lease->lease_id);
  stale_ref.txn_id = strdup(lease->txn_id);
  stale_ref.fencing_token = lease->fencing_token;
  assert_non_null(stale_ref.namespace_name);
  assert_non_null(stale_ref.key);
  assert_non_null(stale_ref.lease_id);
  assert_non_null(stale_ref.txn_id);

  lc_attach_req_init(&lease_attach_req);
  lease_attach_req.name = "existing.txt";
  lease_attach_req.content_type = "text/plain";
  lease_attach_req.prevent_overwrite = 1;
  source = source_from_text("existing-attachment");
  rc = lease->attach(lease, &lease_attach_req, source, &lease_attach_res,
                     &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 1L);

  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  lease = NULL;

  acquire.owner = "owner-b";
  rc = client->acquire(client, &acquire, &fresh_lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(fresh_lease);
  assert_string_not_equal(fresh_lease->lease_id, stale_ref.lease_id);

  memset(&attach_op, 0, sizeof(attach_op));
  attach_op.lease = stale_ref;
  attach_op.name = "stale.txt";
  attach_op.content_type = "text/plain";
  source = source_from_text("stale-attachment");
  rc = client->attach(client, &attach_op, source, &attach_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 403L);
  assert_string_equal(error.server_code, "lease_required");
  lc_attach_res_cleanup(&attach_res);
  lc_error_cleanup(&error);

  lc_attachment_list_req_init(&list_req);
  list_req.lease = stale_ref;
  rc = client->list_attachments(client, &list_req, &attachment_list, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 403L);
  assert_string_equal(error.server_code, "lease_required");
  lc_attachment_list_cleanup(&attachment_list);
  lc_error_cleanup(&error);

  lc_attachment_get_op_init(&get_op);
  get_op.lease = stale_ref;
  get_op.selector.name = "existing.txt";
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get_attachment(client, &get_op, sink, &get_res, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 403L);
  assert_string_equal(error.server_code, "lease_required");
  lc_attachment_get_res_cleanup(&get_res);
  lc_sink_close(sink);
  sink = NULL;
  lc_error_cleanup(&error);

  lc_attachment_selector_init(&selector);
  selector.name = "existing.txt";
  lc_attachment_delete_op_init(&delete_op);
  delete_op.lease = stale_ref;
  delete_op.selector = selector;
  deleted = 1;
  rc = client->delete_attachment(client, &delete_op, &deleted, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 403L);
  assert_string_equal(error.server_code, "lease_required");
  assert_false(deleted);
  lc_error_cleanup(&error);

  lc_attachment_delete_all_op_init(&delete_all_op);
  delete_all_op.lease = stale_ref;
  deleted_count = 1;
  rc = client->delete_all_attachments(client, &delete_all_op, &deleted_count,
                                      &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 403L);
  assert_string_equal(error.server_code, "lease_required");
  assert_int_equal(deleted_count, 0);
  lc_error_cleanup(&error);

  rc = fresh_lease->list_attachments(fresh_lease, &attachment_list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(attachment_list.count, 1U);
  assert_string_equal(attachment_list.items[0].name, "existing.txt");
  lc_attachment_list_cleanup(&attachment_list);

  rc = fresh_lease->release(fresh_lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  fresh_lease = NULL;

  client->close(client);
  free((char *)stale_ref.namespace_name);
  free((char *)stale_ref.key);
  free((char *)stale_ref.lease_id);
  free((char *)stale_ref.txn_id);
  lc_attach_res_cleanup(&lease_attach_res);
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
  assert_int_equal(reacquired->version, 2L);
  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = reacquired->get(reacquired, sink, NULL, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(get_res.no_content);
  assert_int_equal(get_res.version, 2L);
  assert_int_equal(reacquired->version, 2L);
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
  assert_int_equal(first_lease->version, 2L);
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
  assert_string_equal(enqueue_res.correlation_id, "pouch-enqueue");

  second_client = open_pouch_client(endpoint);
  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "jobs";
  rc =
      second_client->queue_stats(second_client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats_res.available, 1);
  assert_string_equal(stats_res.head_message_id, enqueue_res.message_id);
  assert_string_equal(stats_res.correlation_id, "pouch-queue-stats");
  lc_queue_stats_res_cleanup(&stats_res);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "jobs";
  dequeue_req.visibility_timeout_seconds = 30L;
  message = NULL;
  rc = second_client->dequeue(second_client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 400L);
  assert_string_equal(error.server_code, "missing_owner");
  assert_null(message);
  lc_error_cleanup(&error);

  dequeue_req.owner = "";
  rc = second_client->dequeue(second_client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 400L);
  assert_string_equal(error.server_code, "missing_owner");
  assert_null(message);
  lc_error_cleanup(&error);

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
  assert_string_equal(message->correlation_id, "pouch-dequeue");
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
  assert_string_equal(message->correlation_id, "pouch-dequeue");
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

static void test_pouch_endpoint_dequeue_waits_for_later_enqueue(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_dequeue_req dequeue_req;
  lc_message *message;
  lc_sink *sink;
  lc_error error;
  delayed_enqueue_context enqueue_ctx;
  pthread_t thread;
  char *text;
  size_t written;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-dequeue-wait");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_ctx, 0, sizeof(enqueue_ctx));
  client = open_pouch_client(endpoint);

  enqueue_ctx.endpoint = endpoint;
  enqueue_ctx.queue = "jobs";
  enqueue_ctx.body = "waited-body";
  rc = pthread_create(&thread, NULL, delayed_enqueue_main, &enqueue_ctx);
  assert_int_equal(rc, 0);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "jobs";
  dequeue_req.owner = "worker-a";
  dequeue_req.visibility_timeout_seconds = 30L;
  dequeue_req.wait_seconds = 1L;
  message = NULL;
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(message);
  assert_string_equal(message->queue, "jobs");
  assert_string_equal(message->payload_content_type, "text/plain");

  assert_int_equal(pthread_join(thread, NULL), 0);
  assert_int_equal(enqueue_ctx.rc, LC_OK);
  lc_error_cleanup(&enqueue_ctx.error);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  written = 0U;
  rc = message->write_payload(message, sink, &written, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(written, strlen("waited-body"));
  text = memory_sink_text(sink);
  assert_string_equal(text, "waited-body");
  free(text);
  lc_sink_close(sink);

  rc = message->ack(message, &error);
  assert_int_equal(rc, LC_OK);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_watch_queue_snapshots(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_watch_queue_req watch_req;
  lc_watch_handler handler;
  watch_test_state watch_state;
  lc_source *source;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "watch-queue");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&handler, 0, sizeof(handler));
  memset(&watch_state, 0, sizeof(watch_state));
  client = open_pouch_client(endpoint);

  lc_watch_queue_req_init(&watch_req);
  watch_req.queue = "jobs";
  handler.handle = watch_test_handle;
  handler.context = &watch_state;
  rc = client->watch_queue(client, &watch_req, &handler, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(watch_state.handled, 1U);
  assert_false(watch_state.available);
  assert_string_equal(watch_state.queue, "jobs");
  assert_true(watch_state.head_message_id[0] == '\0');
  assert_string_equal(watch_state.correlation_id, "pouch-watch");

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 3600L;
  enqueue_req.max_attempts = 3;
  source = source_from_text("body");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  memset(&watch_state, 0, sizeof(watch_state));
  rc = client->watch_queue(client, &watch_req, &handler, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(watch_state.handled, 1U);
  assert_true(watch_state.available);
  assert_string_equal(watch_state.queue, "jobs");
  assert_string_equal(watch_state.head_message_id, enqueue_res.message_id);
  assert_string_equal(watch_state.correlation_id, "pouch-watch");

  memset(&watch_state, 0, sizeof(watch_state));
  watch_state.fail = 1;
  rc = client->watch_queue(client, &watch_req, &handler, &error);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_string_equal(error.message, "watch callback stopped");
  assert_int_equal(watch_state.handled, 1U);
  lc_error_cleanup(&error);

  memset(&watch_state, 0, sizeof(watch_state));
  watch_state.fail_without_error = 1;
  rc = client->watch_queue(client, &watch_req, &handler, &error);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_string_equal(error.message, "queue watch handler failed");
  assert_int_equal(watch_state.handled, 1U);
  lc_error_cleanup(&error);

  lc_enqueue_res_cleanup(&enqueue_res);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_queue_rejects_negative_timing_options(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats_res;
  lc_extend_req extend_req;
  lc_nack_req nack_req;
  lc_message *message;
  lc_source *source;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-negative-timing");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&stats_res, 0, sizeof(stats_res));
  client = open_pouch_client(endpoint);
  message = NULL;

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.delay_seconds = -1L;
  source = source_from_text("invalid-delay");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "enqueue_message delay_seconds must be non-negative");
  lc_error_cleanup(&error);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = -1L;
  source = source_from_text("invalid-visibility");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(
      error.message,
      "enqueue_message visibility_timeout_seconds must be non-negative");
  lc_error_cleanup(&error);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.ttl_seconds = -1L;
  source = source_from_text("invalid-ttl");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "enqueue_message ttl_seconds must be non-negative");
  lc_error_cleanup(&error);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.max_attempts = -1;
  source = source_from_text("invalid-attempts");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "enqueue_message max_attempts must be non-negative");
  lc_error_cleanup(&error);

  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "jobs";
  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(stats_res.available);
  assert_int_equal(stats_res.pending_candidates, 0);
  lc_queue_stats_res_cleanup(&stats_res);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 3600L;
  enqueue_req.max_attempts = 3;
  source = source_from_text("valid-message");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "jobs";
  dequeue_req.owner = "worker-a";
  dequeue_req.visibility_timeout_seconds = -1L;
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(
      error.message,
      "dequeue_message visibility_timeout_seconds must be non-negative");
  assert_null(message);
  lc_error_cleanup(&error);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "jobs";
  dequeue_req.owner = "worker-a";
  dequeue_req.visibility_timeout_seconds = 30L;
  dequeue_req.wait_seconds = -1L;
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "pouch dequeue wait_seconds must be non-negative");
  assert_null(message);
  lc_error_cleanup(&error);

  memset(&stats_res, 0, sizeof(stats_res));
  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(stats_res.available);
  assert_int_equal(stats_res.pending_candidates, 1);
  assert_string_equal(stats_res.head_message_id, enqueue_res.message_id);
  lc_queue_stats_res_cleanup(&stats_res);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "jobs";
  dequeue_req.owner = "worker-a";
  dequeue_req.visibility_timeout_seconds = 30L;
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(message);

  lc_nack_req_init(&nack_req);
  nack_req.intent = LC_NACK_INTENT_DEFER;
  nack_req.delay_seconds = -1L;
  rc = message->nack(message, &nack_req, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "nack_message delay_seconds must be non-negative");
  lc_error_cleanup(&error);

  lc_extend_req_init(&extend_req);
  extend_req.extend_by_seconds = -1L;
  rc = message->extend(message, &extend_req, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "extend_message extend_by_seconds must be non-negative");
  lc_error_cleanup(&error);

  rc = message->ack(message, &error);
  assert_int_equal(rc, LC_OK);
  message = NULL;

  memset(&stats_res, 0, sizeof(stats_res));
  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(stats_res.available);
  assert_int_equal(stats_res.pending_candidates, 0);
  lc_queue_stats_res_cleanup(&stats_res);

  lc_enqueue_res_cleanup(&enqueue_res);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_queue_variants_reject_missing_owner(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_dequeue_batch_res batch;
  lc_consumer consumer;
  subscribe_test_state subscribe_state;
  lc_message *message;
  lc_source *source;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-owner-variants");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&batch, 0, sizeof(batch));
  memset(&consumer, 0, sizeof(consumer));
  memset(&subscribe_state, 0, sizeof(subscribe_state));
  client = open_pouch_client(endpoint);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 3600L;
  enqueue_req.max_attempts = 3;
  source = source_from_text("body");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "jobs";
  dequeue_req.page_size = 2;
  rc = client->dequeue_batch(client, &dequeue_req, &batch, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 400L);
  assert_string_equal(error.server_code, "missing_owner");
  assert_int_equal(batch.count, 0U);
  lc_error_cleanup(&error);

  dequeue_req.owner = "";
  rc = client->dequeue_batch(client, &dequeue_req, &batch, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 400L);
  assert_string_equal(error.server_code, "missing_owner");
  assert_int_equal(batch.count, 0U);
  lc_error_cleanup(&error);

  consumer.handle = subscribe_test_handle;
  consumer.context = &subscribe_state;
  dequeue_req.owner = NULL;
  rc = client->subscribe(client, &dequeue_req, &consumer, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 400L);
  assert_string_equal(error.server_code, "missing_owner");
  assert_int_equal(subscribe_state.handled, 0U);
  lc_error_cleanup(&error);

  dequeue_req.owner = "";
  rc = client->subscribe_with_state(client, &dequeue_req, &consumer, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 400L);
  assert_string_equal(error.server_code, "missing_owner");
  assert_int_equal(subscribe_state.handled, 0U);
  lc_error_cleanup(&error);

  dequeue_req.owner = "worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  message = NULL;
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(message);
  assert_string_equal(message->message_id, enqueue_res.message_id);
  rc = message->ack(message, &error);
  assert_int_equal(rc, LC_OK);

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

static void test_pouch_endpoint_queue_rejects_expired_delivery_ref(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_message *message;
  lc_source *source;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-expired-ref");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  client = open_pouch_client(endpoint);
  message = NULL;

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 1L;
  enqueue_req.ttl_seconds = 60L;
  enqueue_req.max_attempts = 3;
  source = source_from_text("expires");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "jobs";
  dequeue_req.owner = "worker-a";
  dequeue_req.visibility_timeout_seconds = 1L;
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(message);

  sleep(2U);

  rc = message->ack(message, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 409L);
  assert_string_equal(error.server_code, "queue_lease_expired");
  lc_error_cleanup(&error);
  message->close(message);

  lc_enqueue_res_cleanup(&enqueue_res);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_queue_rejects_missing_or_wrong_txn_id(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_ack_op ack_req;
  lc_ack_res ack_res;
  lc_nack_op nack_req;
  lc_nack_res nack_res;
  lc_extend_op extend_req;
  lc_extend_res extend_res;
  lc_message *message;
  lc_source *source;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-txn-validation");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&ack_res, 0, sizeof(ack_res));
  memset(&nack_res, 0, sizeof(nack_res));
  memset(&extend_res, 0, sizeof(extend_res));
  client = open_pouch_client(endpoint);
  message = NULL;

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 60L;
  enqueue_req.ttl_seconds = 60L;
  enqueue_req.max_attempts = 3;
  source = source_from_text("txn-message");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "jobs";
  dequeue_req.owner = "worker-a";
  dequeue_req.txn_id = "txn-a";
  dequeue_req.visibility_timeout_seconds = 60L;
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(message);
  assert_string_equal(message->txn_id, "txn-a");

  memset(&ack_req, 0, sizeof(ack_req));
  ack_req.message.namespace_name = message->namespace_name;
  ack_req.message.queue = message->queue;
  ack_req.message.message_id = message->message_id;
  ack_req.message.lease_id = message->lease_id;
  ack_req.message.fencing_token = message->fencing_token;
  ack_req.message.meta_etag = message->meta_etag;
  rc = client->queue_ack(client, &ack_req, &ack_res, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 400L);
  assert_string_equal(error.server_code, "missing_txn");
  lc_ack_res_cleanup(&ack_res);
  lc_error_cleanup(&error);

  ack_req.message.txn_id = "txn-b";
  rc = client->queue_ack(client, &ack_req, &ack_res, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 409L);
  assert_string_equal(error.server_code, "txn_mismatch");
  lc_ack_res_cleanup(&ack_res);
  lc_error_cleanup(&error);

  memset(&nack_req, 0, sizeof(nack_req));
  nack_req.message.namespace_name = message->namespace_name;
  nack_req.message.queue = message->queue;
  nack_req.message.message_id = message->message_id;
  nack_req.message.lease_id = message->lease_id;
  nack_req.message.fencing_token = message->fencing_token;
  nack_req.message.meta_etag = message->meta_etag;
  nack_req.delay_seconds = 0L;
  nack_req.intent = LC_NACK_INTENT_DEFER;
  rc = client->queue_nack(client, &nack_req, &nack_res, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 400L);
  assert_string_equal(error.server_code, "missing_txn");
  lc_nack_res_cleanup(&nack_res);
  lc_error_cleanup(&error);

  nack_req.message.txn_id = "txn-b";
  rc = client->queue_nack(client, &nack_req, &nack_res, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 409L);
  assert_string_equal(error.server_code, "txn_mismatch");
  lc_nack_res_cleanup(&nack_res);
  lc_error_cleanup(&error);

  memset(&extend_req, 0, sizeof(extend_req));
  extend_req.message.namespace_name = message->namespace_name;
  extend_req.message.queue = message->queue;
  extend_req.message.message_id = message->message_id;
  extend_req.message.lease_id = message->lease_id;
  extend_req.message.fencing_token = message->fencing_token;
  extend_req.message.meta_etag = message->meta_etag;
  extend_req.extend_by_seconds = 60L;
  rc = client->queue_extend(client, &extend_req, &extend_res, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 400L);
  assert_string_equal(error.server_code, "missing_txn");
  lc_extend_res_cleanup(&extend_res);
  lc_error_cleanup(&error);

  extend_req.message.txn_id = "txn-b";
  rc = client->queue_extend(client, &extend_req, &extend_res, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 409L);
  assert_string_equal(error.server_code, "txn_mismatch");
  lc_extend_res_cleanup(&extend_res);
  lc_error_cleanup(&error);

  rc = message->ack(message, &error);
  assert_int_equal(rc, LC_OK);
  message = NULL;

  lc_enqueue_res_cleanup(&enqueue_res);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_queue_rejects_missing_or_stale_meta_etag(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_ack_op ack_req;
  lc_ack_res ack_res;
  lc_message *message;
  lc_source *source;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-meta-etag-validation");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&ack_res, 0, sizeof(ack_res));
  client = open_pouch_client(endpoint);
  message = NULL;

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 60L;
  enqueue_req.ttl_seconds = 60L;
  enqueue_req.max_attempts = 3;
  source = source_from_text("etag-message");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "jobs";
  dequeue_req.owner = "worker-a";
  dequeue_req.visibility_timeout_seconds = 60L;
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(message);
  assert_non_null(message->meta_etag);

  memset(&ack_req, 0, sizeof(ack_req));
  ack_req.message.namespace_name = message->namespace_name;
  ack_req.message.queue = message->queue;
  ack_req.message.message_id = message->message_id;
  ack_req.message.lease_id = message->lease_id;
  ack_req.message.fencing_token = message->fencing_token;
  rc = client->queue_ack(client, &ack_req, &ack_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(error.code, LC_ERR_INVALID);
  assert_false(ack_res.acked);
  lc_ack_res_cleanup(&ack_res);
  lc_error_cleanup(&error);

  ack_req.message.meta_etag = "stale-meta-etag";
  rc = client->queue_ack(client, &ack_req, &ack_res, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 409L);
  assert_string_equal(error.server_code, "queue_lease_not_active");
  assert_false(ack_res.acked);
  lc_ack_res_cleanup(&ack_res);
  lc_error_cleanup(&error);

  ack_req.message.meta_etag = message->meta_etag;
  rc = client->queue_ack(client, &ack_req, &ack_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(ack_res.acked);
  assert_string_equal(ack_res.correlation_id, "pouch-ack");
  lc_ack_res_cleanup(&ack_res);
  message->close(message);
  message = NULL;

  lc_enqueue_res_cleanup(&enqueue_res);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_queue_extend_reports_correlation(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_extend_op extend_req;
  lc_extend_res extend_res;
  lc_message *message;
  lc_source *source;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-extend-correlation");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&extend_res, 0, sizeof(extend_res));
  client = open_pouch_client(endpoint);
  message = NULL;

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 60L;
  enqueue_req.ttl_seconds = 60L;
  source = source_from_text("extend-message");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "jobs";
  dequeue_req.owner = "worker-a";
  dequeue_req.visibility_timeout_seconds = 60L;
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(message);

  lc_extend_op_init(&extend_req);
  extend_req.message.namespace_name = message->namespace_name;
  extend_req.message.queue = message->queue;
  extend_req.message.message_id = message->message_id;
  extend_req.message.lease_id = message->lease_id;
  extend_req.message.fencing_token = message->fencing_token;
  extend_req.message.meta_etag = message->meta_etag;
  extend_req.extend_by_seconds = 45L;
  rc = client->queue_extend(client, &extend_req, &extend_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(extend_res.visibility_timeout_seconds, 45L);
  assert_non_null(extend_res.meta_etag);
  assert_string_equal(extend_res.correlation_id, "pouch-extend");

  lc_extend_res_cleanup(&extend_res);
  message->close(message);
  lc_enqueue_res_cleanup(&enqueue_res);
  client->close(client);
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
  lc_query_req query_req;
  lc_query_res query_res;
  lc_query_key_handler key_handler;
  lc_namespace_config_req namespace_req;
  lc_namespace_config_res namespace_res;
  lc_index_flush_req flush_req;
  lc_index_flush_res flush_res;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats_res;
  lc_dequeue_req dequeue_req;
  lc_get_res get_res;
  lc_lease *lease;
  lc_message *message;
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
  memset(&query_res, 0, sizeof(query_res));
  memset(&namespace_res, 0, sizeof(namespace_res));
  memset(&flush_res, 0, sizeof(flush_res));
  memset(&stats_res, 0, sizeof(stats_res));
  message = NULL;
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

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  lc_query_req_init(&query_req);
  query_req.namespace_name = ".lockd";
  query_req.selector_json = "{}";
  rc = client->query(client, &query_req, sink, &query_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.server_code, "reserved_namespace");
  lc_sink_close(sink);
  lc_query_res_cleanup(&query_res);
  lc_error_cleanup(&error);

  memset(&key_handler, 0, sizeof(key_handler));
  key_handler.begin = query_key_begin_unexpected;
  key_handler.chunk = query_key_chunk_unexpected;
  key_handler.end = query_key_end_unexpected;
  rc = client->query_keys(client, &query_req, &key_handler, NULL, &query_res,
                          &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.server_code, "reserved_namespace");
  lc_query_res_cleanup(&query_res);
  lc_error_cleanup(&error);

  lc_namespace_config_req_init(&namespace_req);
  namespace_req.namespace_name = ".lockd";
  rc = client->get_namespace_config(client, &namespace_req, &namespace_res,
                                    &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  lc_namespace_config_res_cleanup(&namespace_res);
  lc_error_cleanup(&error);

  lc_index_flush_req_init(&flush_req);
  flush_req.namespace_name = ".lockd";
  flush_req.mode = "wait";
  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.server_code, "reserved_namespace");
  lc_index_flush_res_cleanup(&flush_res);
  lc_error_cleanup(&error);

  lc_queue_stats_req_init(&stats_req);
  stats_req.namespace_name = ".lockd";
  stats_req.queue = "jobs";
  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.server_code, "reserved_namespace");
  lc_queue_stats_res_cleanup(&stats_res);
  lc_error_cleanup(&error);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.namespace_name = ".lockd";
  dequeue_req.queue = "jobs";
  dequeue_req.owner = "worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.server_code, "reserved_namespace");
  assert_null(message);
  lc_error_cleanup(&error);
  client->close(client);

  client = open_pouch_client(endpoint);
  lc_acquire_req_init(&acquire_req);
  acquire_req.namespace_name = ".lockd-txn";
  acquire_req.key = "decision";
  acquire_req.owner = "owner";
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  lc_error_cleanup(&error);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.namespace_name = ".lockd-txn";
  enqueue_req.queue = "transactions";
  source = source_from_text("reserved-transaction");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_INVALID);
  lc_enqueue_res_cleanup(&enqueue_res);
  lc_error_cleanup(&error);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  lc_query_req_init(&query_req);
  query_req.namespace_name = ".lockd-txn";
  query_req.selector_json = "{}";
  rc = client->query(client, &query_req, sink, &query_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.server_code, "reserved_namespace");
  lc_sink_close(sink);
  lc_query_res_cleanup(&query_res);
  lc_error_cleanup(&error);

  memset(&key_handler, 0, sizeof(key_handler));
  key_handler.begin = query_key_begin_unexpected;
  key_handler.chunk = query_key_chunk_unexpected;
  key_handler.end = query_key_end_unexpected;
  rc = client->query_keys(client, &query_req, &key_handler, NULL, &query_res,
                          &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.server_code, "reserved_namespace");
  lc_query_res_cleanup(&query_res);
  lc_error_cleanup(&error);

  lc_namespace_config_req_init(&namespace_req);
  namespace_req.namespace_name = ".lockd-txn";
  rc = client->get_namespace_config(client, &namespace_req, &namespace_res,
                                    &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  lc_namespace_config_res_cleanup(&namespace_res);
  lc_error_cleanup(&error);

  lc_index_flush_req_init(&flush_req);
  flush_req.namespace_name = ".lockd-txn";
  flush_req.mode = "wait";
  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.server_code, "reserved_namespace");
  lc_index_flush_res_cleanup(&flush_res);
  lc_error_cleanup(&error);

  lc_queue_stats_req_init(&stats_req);
  stats_req.namespace_name = ".lockd-txn";
  stats_req.queue = "transactions";
  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.server_code, "reserved_namespace");
  lc_queue_stats_res_cleanup(&stats_res);
  lc_error_cleanup(&error);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.namespace_name = ".lockd-txn";
  dequeue_req.queue = "transactions";
  dequeue_req.owner = "worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.server_code, "reserved_namespace");
  assert_null(message);
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

  lc_namespace_config_req_init(&namespace_req);
  rc = reserved_default_client->get_namespace_config(
      reserved_default_client, &namespace_req, &namespace_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  lc_namespace_config_res_cleanup(&namespace_res);
  lc_error_cleanup(&error);
  reserved_default_client->close(reserved_default_client);

  reserved_default_client =
      open_pouch_client_with_namespace(endpoint, ".lockd-txn");
  lc_acquire_req_init(&acquire_req);
  acquire_req.key = "decision";
  acquire_req.owner = "owner";
  rc = reserved_default_client->acquire(reserved_default_client, &acquire_req,
                                        &lease, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  lc_error_cleanup(&error);
  reserved_default_client->close(reserved_default_client);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_rejects_non_normalized_identifiers(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_acquire_req acquire_req;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_lease *lease;
  lc_source *source;
  lc_sink *sink;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "pathlike-public-identifiers");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&query_res, 0, sizeof(query_res));
  client = open_pouch_client(endpoint);
  lease = NULL;

  lc_acquire_req_init(&acquire_req);
  acquire_req.key = "../bad";
  acquire_req.owner = "owner";
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "load_meta key must not contain dot path components");
  lc_error_cleanup(&error);

  lc_acquire_req_init(&acquire_req);
  acquire_req.namespace_name = "bad/ns";
  acquire_req.key = "alpha";
  acquire_req.owner = "owner";
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "load_meta namespace must not contain '/'");
  lc_error_cleanup(&error);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "jobs//bad";
  source = source_from_text("bad queue");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(
      error.message,
      "enqueue_message queue must not contain empty path components");
  lc_enqueue_res_cleanup(&enqueue_res);
  lc_error_cleanup(&error);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  lc_query_req_init(&query_req);
  query_req.namespace_name = "bad/ns";
  query_req.selector_json = "{}";
  rc = client->query(client, &query_req, sink, &query_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "query_index_scan namespace must not contain '/'");
  lc_sink_close(sink);
  lc_query_res_cleanup(&query_res);
  lc_error_cleanup(&error);

  client->close(client);
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

static void test_pouch_endpoint_dequeue_batch_honors_start_after(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res[3];
  lc_dequeue_req dequeue_req;
  lc_dequeue_batch_res batch;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats_res;
  lc_source *source;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "batch-start-after");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(enqueue_res, 0, sizeof(enqueue_res));
  memset(&batch, 0, sizeof(batch));
  memset(&stats_res, 0, sizeof(stats_res));
  client = open_pouch_client(endpoint);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 60L;
  enqueue_req.ttl_seconds = 3600L;
  enqueue_req.max_attempts = 3;

  source = source_from_text("first");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res[0], &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("second");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res[1], &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("third");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res[2], &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "jobs";
  dequeue_req.owner = "batch-worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  dequeue_req.page_size = 2;
  dequeue_req.start_after = enqueue_res[0].message_id;
  rc = client->dequeue_batch(client, &dequeue_req, &batch, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(batch.count, 2U);
  assert_string_equal(batch.messages[0]->message_id, enqueue_res[1].message_id);
  assert_string_equal(batch.messages[0]->next_cursor,
                      enqueue_res[1].message_id);
  assert_string_equal(batch.messages[1]->message_id, enqueue_res[2].message_id);
  assert_string_equal(batch.messages[1]->next_cursor,
                      enqueue_res[2].message_id);

  rc = batch.messages[0]->ack(batch.messages[0], &error);
  assert_int_equal(rc, LC_OK);
  batch.messages[0] = NULL;
  rc = batch.messages[1]->ack(batch.messages[1], &error);
  assert_int_equal(rc, LC_OK);
  batch.messages[1] = NULL;
  lc_dequeue_batch_cleanup(&batch);

  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "jobs";
  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats_res.available, 1);
  assert_string_equal(stats_res.head_message_id, enqueue_res[0].message_id);

  lc_queue_stats_res_cleanup(&stats_res);
  lc_enqueue_res_cleanup(&enqueue_res[0]);
  lc_enqueue_res_cleanup(&enqueue_res[1]);
  lc_enqueue_res_cleanup(&enqueue_res[2]);
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

static void test_pouch_endpoint_subscribe_honors_start_after(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res[3];
  lc_dequeue_req subscribe_req;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats_res;
  lc_consumer consumer;
  subscribe_test_state subscribe_state;
  lc_source *source;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "subscribe-start-after");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(enqueue_res, 0, sizeof(enqueue_res));
  memset(&stats_res, 0, sizeof(stats_res));
  memset(&consumer, 0, sizeof(consumer));
  memset(&subscribe_state, 0, sizeof(subscribe_state));
  subscribe_state.expected[0] = "second";
  subscribe_state.expected[1] = "third";
  client = open_pouch_client(endpoint);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 60L;
  enqueue_req.ttl_seconds = 3600L;
  enqueue_req.max_attempts = 3;
  source = source_from_text("first");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res[0], &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("second");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res[1], &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("third");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res[2], &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  lc_dequeue_req_init(&subscribe_req);
  subscribe_req.queue = "jobs";
  subscribe_req.owner = "subscriber";
  subscribe_req.visibility_timeout_seconds = 30L;
  subscribe_req.page_size = 2;
  subscribe_req.start_after = enqueue_res[0].message_id;
  consumer.handle = subscribe_test_handle;
  consumer.context = &subscribe_state;
  rc = client->subscribe(client, &subscribe_req, &consumer, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(subscribe_state.handled, 2U);

  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "jobs";
  rc = client->queue_stats(client, &stats_req, &stats_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats_res.available, 1);
  assert_string_equal(stats_res.head_message_id, enqueue_res[0].message_id);

  lc_queue_stats_res_cleanup(&stats_res);
  lc_enqueue_res_cleanup(&enqueue_res[0]);
  lc_enqueue_res_cleanup(&enqueue_res[1]);
  lc_enqueue_res_cleanup(&enqueue_res[2]);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_subscribe_waits_for_shared_message(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_dequeue_req subscribe_req;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats_res;
  lc_consumer consumer;
  subscribe_test_state subscribe_state;
  lc_error error;
  pid_t child;
  int child_status;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "subscribe-wait");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&stats_res, 0, sizeof(stats_res));
  memset(&consumer, 0, sizeof(consumer));
  memset(&subscribe_state, 0, sizeof(subscribe_state));
  subscribe_state.expected[0] = "delayed";
  client = open_pouch_client(endpoint);

  child = fork();
  assert_true(child >= 0);
  if (child == 0) {
    child_enqueue_after_delay(endpoint, "jobs", "delayed");
  }

  lc_dequeue_req_init(&subscribe_req);
  subscribe_req.queue = "jobs";
  subscribe_req.owner = "waiting-subscriber";
  subscribe_req.visibility_timeout_seconds = 30L;
  subscribe_req.wait_seconds = 3L;
  subscribe_req.page_size = 1;
  consumer.handle = subscribe_test_handle;
  consumer.context = &subscribe_state;
  rc = client->subscribe(client, &subscribe_req, &consumer, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(subscribe_state.handled, 1U);

  child_status = 0;
  assert_int_equal(waitpid(child, &child_status, 0), child);
  assert_true(WIFEXITED(child_status));
  assert_int_equal(WEXITSTATUS(child_status), 0);

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

static void test_pouch_endpoint_consumer_service_honors_start_after(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res[3];
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
  test_root_path(root, sizeof(root), "consumer-service-start-after");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(enqueue_res, 0, sizeof(enqueue_res));
  memset(&stats_res, 0, sizeof(stats_res));
  memset(&service_state, 0, sizeof(service_state));
  service_state.expected[0] = "second";
  service_state.expected[1] = "third";
  client = open_pouch_client(endpoint);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 60L;
  enqueue_req.ttl_seconds = 3600L;
  enqueue_req.max_attempts = 3;
  source = source_from_text("first");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res[0], &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("second");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res[1], &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("third");
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res[2], &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  lc_consumer_config_init(&consumer_config);
  lc_consumer_service_config_init(&service_config);
  consumer_config.request.queue = "jobs";
  consumer_config.request.owner = "managed-worker";
  consumer_config.request.visibility_timeout_seconds = 30L;
  consumer_config.request.wait_seconds = 1L;
  consumer_config.request.start_after = enqueue_res[0].message_id;
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
  assert_int_equal(stats_res.available, 1);
  assert_string_equal(stats_res.head_message_id, enqueue_res[0].message_id);

  lc_queue_stats_res_cleanup(&stats_res);
  lc_enqueue_res_cleanup(&enqueue_res[0]);
  lc_enqueue_res_cleanup(&enqueue_res[1]);
  lc_enqueue_res_cleanup(&enqueue_res[2]);
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

static void assert_pouch_unsupported(int rc, lc_error *error,
                                     const char *message) {
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(error->code, LC_ERR_INVALID);
  assert_string_equal(error->message, message);
  lc_error_cleanup(error);
}

static lc_lease *pouch_acquire_query_key(lc_client *client, const char *key,
                                         lc_error *error) {
  lc_acquire_req acquire;
  lc_lease *lease;
  int rc;

  lc_acquire_req_init(&acquire);
  acquire.key = key;
  acquire.owner = "query-owner";
  acquire.ttl_seconds = 60L;
  lease = NULL;
  rc = client->acquire(client, &acquire, &lease, error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);
  return lease;
}

static void pouch_save_query_json(lc_lease *lease, const char *json,
                                  lc_error *error) {
  lc_update_opts opts;
  lc_source *source;
  int rc;

  source = source_from_text(json);
  lc_update_opts_init(&opts);
  opts.content_type = "application/json";
  rc = lease->update(lease, source, &opts, error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
}

static void pouch_save_query_json_with_type(lc_lease *lease, const char *json,
                                            const char *content_type,
                                            lc_error *error) {
  lc_update_opts opts;
  lc_source *source;
  int rc;

  source = source_from_text(json);
  lc_update_opts_init(&opts);
  opts.content_type = content_type;
  rc = lease->update(lease, source, &opts, error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
}

static void pouch_hide_query_key(lc_lease *lease, lc_error *error) {
  lc_metadata_req metadata_req;
  int rc;

  lc_metadata_req_init(&metadata_req);
  metadata_req.has_query_hidden = 1;
  metadata_req.query_hidden = 1;
  metadata_req.has_if_version = 1;
  metadata_req.if_version = lease->version;
  rc = lease->metadata(lease, &metadata_req, error);
  assert_int_equal(rc, LC_OK);
}

static void test_pouch_endpoint_reports_query_mode_defaults(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_namespace_config_req req;
  lc_namespace_config_res res;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-mode-defaults");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  client = open_pouch_client(endpoint);

  lc_namespace_config_req_init(&req);
  req.namespace_name = "default";
  rc = client->get_namespace_config(client, &req, &res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(res.namespace_name, "default");
  assert_string_equal(res.preferred_engine, "index");
  assert_string_equal(res.fallback_engine, "none");

  lc_namespace_config_res_cleanup(&res);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_reports_configured_scan_mode(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_namespace_config_req req;
  lc_namespace_config_res res;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-mode-scan");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  client = open_pouch_client_with_query_config(endpoint, "scan", NULL);

  lc_namespace_config_req_init(&req);
  req.namespace_name = "default";
  rc = client->get_namespace_config(client, &req, &res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(res.preferred_engine, "scan");
  assert_string_equal(res.fallback_engine, "none");

  lc_namespace_config_res_cleanup(&res);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_reports_configured_scan_fallback(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_namespace_config_req req;
  lc_namespace_config_res res;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-mode-fallback");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  client = open_pouch_client_with_query_config(endpoint, "index", "scan");

  lc_namespace_config_req_init(&req);
  req.namespace_name = "default";
  rc = client->get_namespace_config(client, &req, &res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(res.preferred_engine, "index");
  assert_string_equal(res.fallback_engine, "scan");

  lc_namespace_config_res_cleanup(&res);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_query_options_configure_scan_mode(
    void **state) {
  char root[256];
  char endpoint[384];
  char log_path[512];
  lc_client *client;
  lc_namespace_config_req req;
  lc_namespace_config_res res;
  lc_error error;
  struct stat st;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-mode-url");
  test_cleanup_root(root);
  test_endpoint_with_query(
      endpoint, sizeof(endpoint), root,
      "query_engine=scan&query_fallback_engine=index");
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  client = open_pouch_client(endpoint);

  snprintf(log_path, sizeof(log_path), "%s/store.log", root);
  assert_int_equal(stat(log_path, &st), 0);

  lc_namespace_config_req_init(&req);
  req.namespace_name = "default";
  rc = client->get_namespace_config(client, &req, &res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(res.preferred_engine, "scan");
  assert_string_equal(res.fallback_engine, "index");

  lc_namespace_config_res_cleanup(&res);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_query_options_override_client_config(
    void **state) {
  char root[256];
  char endpoint[384];
  lc_client *client;
  lc_lease *lease;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_sink *sink;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-mode-url-overrides-config");
  test_cleanup_root(root);
  test_endpoint_with_query(endpoint, sizeof(endpoint), root,
                           "query_engine=index");
  memset(&error, 0, sizeof(error));
  memset(&query_res, 0, sizeof(query_res));
  client = open_pouch_client_with_query_config(endpoint, "scan", NULL);
  lease = pouch_acquire_query_key(client, "endpoint-index-doc", &error);
  pouch_save_query_json(lease, "{\"endpoint_index\":true}", &error);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  lc_query_req_init(&query_req);
  query_req.selector_json = "{}";
  rc = client->query(client, &query_req, sink, &query_res, &error);
  assert_int_equal(rc, LC_OK);
  text = memory_sink_text(sink);
  assert_non_null(strstr(text, "\"key\":\"endpoint-index-doc\""));
  assert_non_null(strstr(text, "\"document\":{\"endpoint_index\":true}"));
  assert_string_equal(query_res.return_mode, "documents");
  assert_true(query_res.index_seq > 0UL);

  free(text);
  lc_sink_close(sink);
  lc_query_res_cleanup(&query_res);
  lease->close(lease);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_decodes_percent_encoded_path_and_options(
    void **state) {
  char root[256];
  char endpoint[384];
  char log_path[512];
  lc_client *client;
  lc_namespace_config_req req;
  lc_namespace_config_res res;
  lc_error error;
  struct stat st;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-mode-url encoded");
  test_cleanup_root(root);
  snprintf(endpoint, sizeof(endpoint),
           "pouch:///tmp/liblockdc-pouch-client-%ld-query-mode-url%%20encoded"
           "?query_engine=sc%%61n&query_fallback_engine=in%%64ex",
           (long)getpid());
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  client = open_pouch_client(endpoint);

  snprintf(log_path, sizeof(log_path), "%s/store.log", root);
  assert_int_equal(stat(log_path, &st), 0);

  lc_namespace_config_req_init(&req);
  req.namespace_name = "default";
  rc = client->get_namespace_config(client, &req, &res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(res.preferred_engine, "scan");
  assert_string_equal(res.fallback_engine, "index");

  lc_namespace_config_res_cleanup(&res);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_rejects_invalid_query_options(void **state) {
  char root[256];
  char endpoint[384];
  lc_client_config config;
  lc_client *client;
  lc_error error;
  const char *endpoints[1];
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-mode-url-invalid");
  test_cleanup_root(root);
  memset(&error, 0, sizeof(error));
  lc_client_config_init(&config);
  test_endpoint_with_query(endpoint, sizeof(endpoint), root,
                           "query_engine=linear");
  endpoints[0] = endpoint;
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  client = NULL;
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(client);
  assert_string_equal(error.message,
                      "pouch endpoint query_engine must be index or scan");
  lc_error_cleanup(&error);

  memset(&error, 0, sizeof(error));
  test_endpoint_with_query(endpoint, sizeof(endpoint), root, "unknown=scan");
  endpoints[0] = endpoint;
  client = NULL;
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(client);
  assert_string_equal(error.message,
                      "unsupported pouch endpoint query option");
  lc_error_cleanup(&error);

  memset(&error, 0, sizeof(error));
  snprintf(endpoint, sizeof(endpoint), "pouch://%s%%XX", root);
  endpoints[0] = endpoint;
  client = NULL;
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(client);
  assert_string_equal(error.message,
                      "invalid percent escape in pouch endpoint");
  assert_string_equal(error.detail, "path");
  lc_error_cleanup(&error);

  memset(&error, 0, sizeof(error));
  test_endpoint_with_query(endpoint, sizeof(endpoint), root,
                           "query_engine=sc%XXn");
  endpoints[0] = endpoint;
  client = NULL;
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(client);
  assert_string_equal(error.message,
                      "invalid percent escape in pouch endpoint");
  assert_string_equal(error.detail, "query_engine");
  lc_error_cleanup(&error);

  memset(&error, 0, sizeof(error));
  endpoints[0] = "pouch://relative-root";
  client = NULL;
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(client);
  assert_string_equal(error.message, "pouch endpoint path must be absolute");
  lc_error_cleanup(&error);

  memset(&error, 0, sizeof(error));
  endpoints[0] = "pouch://?query_engine=scan";
  client = NULL;
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(client);
  assert_string_equal(error.message, "pouch endpoint path must be absolute");
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_rejects_mixed_endpoint_configuration(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client_config config;
  lc_client *client;
  lc_error error;
  const char *endpoints[2];
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "mixed-endpoints-invalid");
  test_cleanup_root(root);
  memset(&error, 0, sizeof(error));
  lc_client_config_init(&config);
  test_endpoint(endpoint, sizeof(endpoint), root);
  endpoints[0] = endpoint;
  endpoints[1] = "https://127.0.0.1:1";
  config.endpoints = endpoints;
  config.endpoint_count = 2U;

  client = NULL;
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(client);
  assert_string_equal(error.message,
                      "pouch endpoints must be configured alone");

  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_scan_query_keys_pages_ordered_visible_keys(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *alpha;
  lc_lease *bravo;
  lc_lease *charlie;
  lc_lease *delta;
  lc_query_req req;
  lc_query_res res;
  lc_query_key_handler handler;
  query_key_capture_state capture;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-keys-scan-pages");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  client = open_pouch_client(endpoint);

  charlie = pouch_acquire_query_key(client, "charlie", &error);
  alpha = pouch_acquire_query_key(client, "alpha", &error);
  delta = pouch_acquire_query_key(client, "delta", &error);
  bravo = pouch_acquire_query_key(client, "bravo", &error);
  pouch_hide_query_key(delta, &error);

  handler.begin = query_key_capture_begin;
  handler.chunk = query_key_capture_chunk;
  handler.end = query_key_capture_end;
  lc_query_req_init(&req);
  req.selector_json = "{}";
  req.engine = "scan";
  req.limit = 2L;
  rc = client->query_keys(client, &req, &handler, &capture, &res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.key_count, 2U);
  assert_string_equal(capture.keys[0], "alpha");
  assert_string_equal(capture.keys[1], "bravo");
  assert_string_equal(res.cursor, "bravo");
  assert_string_equal(res.return_mode, "keys");
  assert_int_equal(res.index_seq, 0UL);
  lc_query_res_cleanup(&res);

  memset(&capture, 0, sizeof(capture));
  req.cursor = "bravo";
  rc = client->query_keys(client, &req, &handler, &capture, &res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.key_count, 1U);
  assert_string_equal(capture.keys[0], "charlie");
  assert_null(res.cursor);
  assert_string_equal(res.return_mode, "keys");
  lc_query_res_cleanup(&res);

  alpha->close(alpha);
  bravo->close(bravo);
  charlie->close(charlie);
  delta->close(delta);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_scan_query_streams_documents_with_paging(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *alpha;
  lc_lease *bravo;
  lc_lease *charlie;
  lc_lease *delta;
  lc_query_req req;
  lc_query_res res;
  lc_sink *sink;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-scan-docs-pages");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  client = open_pouch_client(endpoint);

  charlie = pouch_acquire_query_key(client, "charlie", &error);
  pouch_save_query_json(charlie, "{\"value\":3}", &error);
  alpha = pouch_acquire_query_key(client, "alpha", &error);
  pouch_save_query_json(alpha, "{\"value\":1}", &error);
  delta = pouch_acquire_query_key(client, "delta", &error);
  pouch_save_query_json(delta, "{\"value\":4}", &error);
  bravo = pouch_acquire_query_key(client, "bravo", &error);
  pouch_save_query_json(bravo, "{\"value\":2}", &error);
  pouch_hide_query_key(delta, &error);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  lc_query_req_init(&req);
  req.selector_json = "{}";
  req.engine = "scan";
  req.limit = 2L;
  rc = client->query(client, &req, sink, &res, &error);
  assert_int_equal(rc, LC_OK);
  text = memory_sink_text(sink);
  assert_non_null(strstr(text, "{\"key\":\"alpha\""));
  assert_non_null(strstr(text, "\"document\":{\"value\":1}"));
  assert_non_null(strstr(text, "{\"key\":\"bravo\""));
  assert_non_null(strstr(text, "\"document\":{\"value\":2}"));
  assert_null(strstr(text, "charlie"));
  assert_null(strstr(text, "delta"));
  assert_string_equal(res.cursor, "bravo");
  assert_string_equal(res.return_mode, "documents");
  assert_string_equal(res.metadata_json, "{\"query_candidates\":2}");
  assert_int_equal(res.index_seq, 0UL);
  free(text);
  lc_sink_close(sink);
  lc_query_res_cleanup(&res);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  req.cursor = "bravo";
  rc = client->query(client, &req, sink, &res, &error);
  assert_int_equal(rc, LC_OK);
  text = memory_sink_text(sink);
  assert_non_null(strstr(text, "{\"key\":\"charlie\""));
  assert_non_null(strstr(text, "\"document\":{\"value\":3}"));
  assert_null(strstr(text, "alpha"));
  assert_null(strstr(text, "bravo"));
  assert_null(strstr(text, "delta"));
  assert_null(res.cursor);
  assert_string_equal(res.return_mode, "documents");
  assert_string_equal(res.metadata_json, "{\"query_candidates\":1}");
  free(text);
  lc_sink_close(sink);
  lc_query_res_cleanup(&res);

  alpha->close(alpha);
  bravo->close(bravo);
  charlie->close(charlie);
  delta->close(delta);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_scan_query_serializes_metadata_with_lonejson(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_query_req req;
  lc_query_res res;
  lc_sink *sink;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-scan-json-escaping");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  client = open_pouch_client(endpoint);
  lease = pouch_acquire_query_key(client, "escape\"\\key", &error);
  pouch_save_query_json_with_type(
      lease, "{\"escaped\":true}",
      "application/json; note=\"quoted\\value\"", &error);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  lc_query_req_init(&req);
  req.selector_json = "{}";
  req.engine = "scan";
  rc = client->query(client, &req, sink, &res, &error);
  assert_int_equal(rc, LC_OK);
  text = memory_sink_text(sink);
  assert_non_null(strstr(text, "\"key\":\"escape\\\"\\\\key\""));
  assert_non_null(strstr(
      text,
      "\"content_type\":\"application/json; note=\\\"quoted\\\\value\\\"\""));
  assert_non_null(strstr(text, "\"document\":{\"escaped\":true}"));

  free(text);
  lc_sink_close(sink);
  lc_query_res_cleanup(&res);
  lease->close(lease);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_default_index_query_streams_documents(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *alpha;
  lc_lease *bravo;
  lc_lease *hidden;
  lc_query_req req;
  lc_query_res res;
  lc_sink *sink;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-index-docs");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  client = open_pouch_client(endpoint);

  bravo = pouch_acquire_query_key(client, "bravo", &error);
  pouch_save_query_json(bravo, "{\"value\":2}", &error);
  hidden = pouch_acquire_query_key(client, "hidden", &error);
  pouch_save_query_json(hidden, "{\"value\":3}", &error);
  pouch_hide_query_key(hidden, &error);
  alpha = pouch_acquire_query_key(client, "alpha", &error);
  pouch_save_query_json(alpha, "{\"value\":1}", &error);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  lc_query_req_init(&req);
  req.selector_json = "{}";
  rc = client->query(client, &req, sink, &res, &error);
  assert_int_equal(rc, LC_OK);
  text = memory_sink_text(sink);
  assert_non_null(strstr(text, "{\"key\":\"alpha\""));
  assert_non_null(strstr(text, "\"document\":{\"value\":1}"));
  assert_non_null(strstr(text, "{\"key\":\"bravo\""));
  assert_non_null(strstr(text, "\"document\":{\"value\":2}"));
  assert_null(strstr(text, "hidden"));
  assert_string_equal(res.return_mode, "documents");
  assert_string_equal(res.metadata_json, "{\"query_candidates\":2}");
  assert_true(res.index_seq > 0UL);

  free(text);
  lc_sink_close(sink);
  lc_query_res_cleanup(&res);
  alpha->close(alpha);
  bravo->close(bravo);
  hidden->close(hidden);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_default_index_query_waits_for_refresh(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_query_req req;
  lc_query_res res;
  lc_sink *sink;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-index-refresh");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  client = open_pouch_client(endpoint);
  lease = pouch_acquire_query_key(client, "refreshed", &error);
  pouch_save_query_json(lease, "{\"fresh\":true}", &error);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  lc_query_req_init(&req);
  req.selector_json = "{}";
  req.refresh = "wait_for";
  rc = client->query(client, &req, sink, &res, &error);
  assert_int_equal(rc, LC_OK);
  text = memory_sink_text(sink);
  assert_non_null(strstr(text, "{\"key\":\"refreshed\""));
  assert_non_null(strstr(text, "\"document\":{\"fresh\":true}"));
  assert_string_equal(res.return_mode, "documents");
  assert_true(res.index_seq > 0UL);

  free(text);
  lc_sink_close(sink);
  lc_query_res_cleanup(&res);
  lease->close(lease);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_default_index_query_keys_pages(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *alpha;
  lc_lease *bravo;
  lc_query_req req;
  lc_query_res res;
  lc_query_key_handler handler;
  query_key_capture_state capture;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-index-keys");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  client = open_pouch_client(endpoint);
  bravo = pouch_acquire_query_key(client, "bravo", &error);
  alpha = pouch_acquire_query_key(client, "alpha", &error);

  handler.begin = query_key_capture_begin;
  handler.chunk = query_key_capture_chunk;
  handler.end = query_key_capture_end;
  lc_query_req_init(&req);
  req.selector_json = "{}";
  req.limit = 1L;
  rc = client->query_keys(client, &req, &handler, &capture, &res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.key_count, 1U);
  assert_string_equal(capture.keys[0], "alpha");
  assert_string_equal(res.cursor, "alpha");
  assert_string_equal(res.return_mode, "keys");
  assert_true(res.index_seq > 0UL);
  lc_query_res_cleanup(&res);

  memset(&capture, 0, sizeof(capture));
  req.cursor = "alpha";
  rc = client->query_keys(client, &req, &handler, &capture, &res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.key_count, 1U);
  assert_string_equal(capture.keys[0], "bravo");
  assert_null(res.cursor);
  assert_string_equal(res.return_mode, "keys");
  assert_true(res.index_seq > 0UL);

  lc_query_res_cleanup(&res);
  alpha->close(alpha);
  bravo->close(bravo);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_default_index_query_keys_waits_for_refresh(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_query_req req;
  lc_query_res res;
  lc_query_key_handler handler;
  query_key_capture_state capture;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-keys-index-refresh");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  client = open_pouch_client(endpoint);
  lease = pouch_acquire_query_key(client, "refreshed-key", &error);

  handler.begin = query_key_capture_begin;
  handler.chunk = query_key_capture_chunk;
  handler.end = query_key_capture_end;
  lc_query_req_init(&req);
  req.selector_json = "{}";
  req.refresh = "wait_for";
  rc = client->query_keys(client, &req, &handler, &capture, &res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.key_count, 1U);
  assert_string_equal(capture.keys[0], "refreshed-key");
  assert_string_equal(res.return_mode, "keys");
  assert_true(res.index_seq > 0UL);

  lc_query_res_cleanup(&res);
  lease->close(lease);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_default_index_query_rejects_unknown_refresh(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_query_req req;
  lc_query_res res;
  lc_sink *sink;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-index-bad-refresh");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  client = open_pouch_client(endpoint);
  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);

  lc_query_req_init(&req);
  req.selector_json = "{}";
  req.refresh = "later";
  rc = client->query(client, &req, sink, &res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "pouch index query refresh must be wait_for");

  lc_sink_close(sink);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_flush_index_reports_current_sequence(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_index_flush_req req;
  lc_index_flush_res res;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "index-flush");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  client = open_pouch_client(endpoint);
  lease = pouch_acquire_query_key(client, "alpha", &error);
  pouch_save_query_json(lease, "{\"value\":1}", &error);

  lc_index_flush_req_init(&req);
  req.namespace_name = "default";
  rc = client->flush_index(client, &req, &res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(res.namespace_name, "default");
  assert_string_equal(res.mode, "wait");
  assert_string_equal(res.flush_id, "local");
  assert_true(res.accepted);
  assert_true(res.flushed);
  assert_false(res.pending);
  assert_true(res.index_seq > 0UL);
  lc_index_flush_res_cleanup(&res);

  lc_index_flush_req_init(&req);
  req.mode = "now";
  rc = client->flush_index(client, &req, &res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(res.namespace_name, "default");
  assert_string_equal(res.mode, "now");
  assert_true(res.index_seq > 0UL);
  lc_index_flush_res_cleanup(&res);

  lc_index_flush_req_init(&req);
  req.mode = "later";
  rc = client->flush_index(client, &req, &res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "pouch index flush mode must be wait or now");
  lc_error_cleanup(&error);

  lease->close(lease);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_configured_scan_query_without_hint(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_query_req req;
  lc_query_res res;
  lc_sink *sink;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-configured-scan");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  client = open_pouch_client_with_query_config(endpoint, "scan", NULL);
  lease = pouch_acquire_query_key(client, "configured-doc", &error);
  pouch_save_query_json(lease, "{\"configured\":true}", &error);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  lc_query_req_init(&req);
  req.selector_json = "{}";
  rc = client->query(client, &req, sink, &res, &error);
  assert_int_equal(rc, LC_OK);
  text = memory_sink_text(sink);
  assert_non_null(strstr(text, "configured-doc"));
  assert_non_null(strstr(text, "\"document\":{\"configured\":true}"));
  assert_string_equal(res.return_mode, "documents");

  free(text);
  lc_sink_close(sink);
  lc_query_res_cleanup(&res);
  lease->close(lease);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_explicit_scan_query_bypasses_fallback(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_query_req req;
  lc_query_res res;
  lc_sink *sink;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-explicit-scan-bypasses-fallback");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  client = open_pouch_client_with_query_config(endpoint, "scan", "index");
  lease = pouch_acquire_query_key(client, "explicit-scan-doc", &error);
  pouch_save_query_json(lease, "{\"explicit_scan\":true}", &error);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  lc_query_req_init(&req);
  req.selector_json = "{}";
  req.engine = "scan";
  rc = client->query(client, &req, sink, &res, &error);
  assert_int_equal(rc, LC_OK);
  text = memory_sink_text(sink);
  assert_non_null(strstr(text, "explicit-scan-doc"));
  assert_non_null(strstr(text, "\"document\":{\"explicit_scan\":true}"));
  assert_string_equal(res.return_mode, "documents");
  assert_int_equal(res.index_seq, 0UL);

  free(text);
  lc_sink_close(sink);
  lc_query_res_cleanup(&res);

  sink = NULL;
  memset(&res, 0, sizeof(res));
  memset(&error, 0, sizeof(error));
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  lc_query_req_init(&req);
  req.selector_json = "{}";
  req.engine = "scan";
  req.refresh = "wait_for";
  rc = client->query(client, &req, sink, &res, &error);
  assert_pouch_unsupported(rc, &error,
                           "pouch scan query does not support refresh");

  lc_sink_close(sink);
  lease->close(lease);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_scan_primary_uses_index_fallback_for_refresh(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_query_req req;
  lc_query_res res;
  lc_sink *sink;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-scan-index-fallback");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  client = open_pouch_client_with_query_config(endpoint, "scan", "index");
  lease = pouch_acquire_query_key(client, "fallback-refresh-doc", &error);
  pouch_save_query_json(lease, "{\"fallback_refresh\":true}", &error);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  lc_query_req_init(&req);
  req.selector_json = "{}";
  req.refresh = "wait_for";
  rc = client->query(client, &req, sink, &res, &error);
  assert_int_equal(rc, LC_OK);
  text = memory_sink_text(sink);
  assert_non_null(strstr(text, "fallback-refresh-doc"));
  assert_non_null(strstr(text, "\"document\":{\"fallback_refresh\":true}"));
  assert_string_equal(res.return_mode, "documents");
  assert_true(res.index_seq > 0UL);

  free(text);
  lc_sink_close(sink);
  lc_query_res_cleanup(&res);
  lease->close(lease);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_configured_scan_fallback_query(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_query_req req;
  lc_query_res res;
  lc_sink *sink;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-configured-fallback");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  client = open_pouch_client_with_query_config(endpoint, "index", "scan");
  lease = pouch_acquire_query_key(client, "fallback-doc", &error);
  pouch_save_query_json(lease, "{\"fallback\":true}", &error);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  lc_query_req_init(&req);
  req.selector_json = "{}";
  rc = client->query(client, &req, sink, &res, &error);
  assert_int_equal(rc, LC_OK);
  text = memory_sink_text(sink);
  assert_non_null(strstr(text, "fallback-doc"));
  assert_non_null(strstr(text, "\"document\":{\"fallback\":true}"));
  assert_string_equal(res.return_mode, "documents");

  free(text);
  lc_sink_close(sink);
  lc_query_res_cleanup(&res);
  lease->close(lease);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_scan_query_rejects_lql_selector(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_query_req req;
  lc_query_res res;
  lc_sink *sink;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-scan-selector");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  client = open_pouch_client(endpoint);
  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);

  lc_query_req_init(&req);
  req.selector_json = "{\"key\":\"alpha\"}";
  req.engine = "scan";
  rc = client->query(client, &req, sink, &res, &error);
  assert_pouch_unsupported(rc, &error,
                           "pouch scan query supports only match-all selector");

  lc_sink_close(sink);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_configured_scan_query_keys_without_hint(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_query_req req;
  lc_query_res res;
  lc_query_key_handler handler;
  query_key_capture_state capture;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-keys-configured-scan");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  client = open_pouch_client_with_query_config(endpoint, "scan", NULL);
  lease = pouch_acquire_query_key(client, "configured", &error);

  handler.begin = query_key_capture_begin;
  handler.chunk = query_key_capture_chunk;
  handler.end = query_key_capture_end;
  lc_query_req_init(&req);
  req.selector_json = "{}";
  rc = client->query_keys(client, &req, &handler, &capture, &res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.key_count, 1U);
  assert_string_equal(capture.keys[0], "configured");
  assert_string_equal(res.return_mode, "keys");

  lc_query_res_cleanup(&res);
  lease->close(lease);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_configured_scan_ignores_corrupt_query_sidecar(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_query_req req;
  lc_query_res doc_res;
  lc_query_res key_res;
  lc_sink *sink;
  lc_query_key_handler handler;
  query_key_capture_state capture;
  lc_error error;
  char *text;
  off_t corrupt_size;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-scan-corrupt-sidecar");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&doc_res, 0, sizeof(doc_res));
  memset(&key_res, 0, sizeof(key_res));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));

  client = open_pouch_client(endpoint);
  lease = pouch_acquire_query_key(client, "corrupt-sidecar-doc", &error);
  pouch_save_query_json(lease, "{\"corrupt_sidecar\":true}", &error);
  lease->close(lease);
  client->close(client);

  corrupt_query_index_tail(root);
  corrupt_size = test_query_index_size(root);

  client = open_pouch_client_with_query_config(endpoint, "scan", NULL);
  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  lc_query_req_init(&req);
  req.selector_json = "{}";
  rc = client->query(client, &req, sink, &doc_res, &error);
  assert_int_equal(rc, LC_OK);
  text = memory_sink_text(sink);
  assert_non_null(strstr(text, "corrupt-sidecar-doc"));
  assert_non_null(strstr(text, "\"document\":{\"corrupt_sidecar\":true}"));
  assert_string_equal(doc_res.return_mode, "documents");
  assert_int_equal(doc_res.index_seq, 0UL);
  assert_int_equal(test_query_index_size(root), corrupt_size);
  free(text);
  lc_sink_close(sink);
  lc_query_res_cleanup(&doc_res);

  handler.begin = query_key_capture_begin;
  handler.chunk = query_key_capture_chunk;
  handler.end = query_key_capture_end;
  lc_query_req_init(&req);
  req.selector_json = "{}";
  rc = client->query_keys(client, &req, &handler, &capture, &key_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.key_count, 1U);
  assert_string_equal(capture.keys[0], "corrupt-sidecar-doc");
  assert_string_equal(key_res.return_mode, "keys");
  assert_int_equal(key_res.index_seq, 0UL);
  assert_int_equal(test_query_index_size(root), corrupt_size);

  lc_query_res_cleanup(&key_res);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_configured_scan_ignores_absent_or_future_sidecar(
    void **state) {
  char root[256];
  char endpoint[320];
  char index_path[512];
  lc_client *client;
  lc_lease *lease;
  lc_query_req req;
  lc_query_res doc_res;
  lc_query_res key_res;
  lc_sink *sink;
  lc_query_key_handler handler;
  query_key_capture_state capture;
  lc_error error;
  struct stat st;
  char *text;
  off_t future_size;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-scan-absent-future-sidecar");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  snprintf(index_path, sizeof(index_path), "%s/query.index", root);
  memset(&error, 0, sizeof(error));
  memset(&doc_res, 0, sizeof(doc_res));
  memset(&key_res, 0, sizeof(key_res));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));

  client = open_pouch_client(endpoint);
  lease = pouch_acquire_query_key(client, "missing-sidecar-doc", &error);
  pouch_save_query_json(lease, "{\"missing_sidecar\":true}", &error);
  lease->close(lease);
  client->close(client);
  assert_int_equal(unlink(index_path), 0);

  client = open_pouch_client_with_query_config(endpoint, "scan", NULL);
  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  lc_query_req_init(&req);
  req.selector_json = "{}";
  rc = client->query(client, &req, sink, &doc_res, &error);
  assert_int_equal(rc, LC_OK);
  text = memory_sink_text(sink);
  assert_non_null(strstr(text, "missing-sidecar-doc"));
  assert_non_null(strstr(text, "\"document\":{\"missing_sidecar\":true}"));
  assert_string_equal(doc_res.return_mode, "documents");
  assert_int_equal(doc_res.index_seq, 0UL);
  free(text);
  lc_sink_close(sink);
  lc_query_res_cleanup(&doc_res);

  handler.begin = query_key_capture_begin;
  handler.chunk = query_key_capture_chunk;
  handler.end = query_key_capture_end;
  lc_query_req_init(&req);
  req.selector_json = "{}";
  rc = client->query_keys(client, &req, &handler, &capture, &key_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.key_count, 1U);
  assert_string_equal(capture.keys[0], "missing-sidecar-doc");
  assert_string_equal(key_res.return_mode, "keys");
  assert_int_equal(key_res.index_seq, 0UL);
  assert_int_equal(stat(index_path, &st), 0);
  assert_int_equal(st.st_size, 0);
  lc_query_res_cleanup(&key_res);
  client->close(client);

  memset(&capture, 0, sizeof(capture));
  client = open_pouch_client(endpoint);
  lease = pouch_acquire_query_key(client, "future-sidecar-doc", &error);
  pouch_save_query_json(lease, "{\"future_sidecar\":true}", &error);
  lease->close(lease);
  client->close(client);

  set_first_query_index_match_record_version(root, "future-sidecar-doc", 99UL);
  future_size = test_query_index_size(root);

  client = open_pouch_client_with_query_config(endpoint, "scan", NULL);
  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  lc_query_req_init(&req);
  req.selector_json = "{}";
  rc = client->query(client, &req, sink, &doc_res, &error);
  assert_int_equal(rc, LC_OK);
  text = memory_sink_text(sink);
  assert_non_null(strstr(text, "missing-sidecar-doc"));
  assert_non_null(strstr(text, "future-sidecar-doc"));
  assert_non_null(strstr(text, "\"document\":{\"future_sidecar\":true}"));
  assert_string_equal(doc_res.return_mode, "documents");
  assert_int_equal(doc_res.index_seq, 0UL);
  assert_int_equal(test_query_index_size(root), future_size);
  free(text);
  lc_sink_close(sink);
  lc_query_res_cleanup(&doc_res);

  rc = client->query_keys(client, &req, &handler, &capture, &key_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.key_count, 2U);
  assert_string_equal(capture.keys[0], "future-sidecar-doc");
  assert_string_equal(capture.keys[1], "missing-sidecar-doc");
  assert_string_equal(key_res.return_mode, "keys");
  assert_int_equal(key_res.index_seq, 0UL);
  assert_int_equal(test_query_index_size(root), future_size);

  lc_query_res_cleanup(&key_res);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_configured_scan_refreshes_shared_log(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *scan_client;
  lc_client *writer_client;
  lc_lease *lease;
  lc_query_req req;
  lc_query_res doc_res;
  lc_query_res key_res;
  lc_sink *sink;
  lc_query_key_handler handler;
  query_key_capture_state capture;
  lc_error error;
  char *text;
  off_t corrupt_size;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-scan-shared-log-refresh");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&doc_res, 0, sizeof(doc_res));
  memset(&key_res, 0, sizeof(key_res));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));

  scan_client = open_pouch_client_with_query_config(endpoint, "scan", NULL);
  writer_client = open_pouch_client(endpoint);
  lease = pouch_acquire_query_key(writer_client, "shared-log-doc", &error);
  pouch_save_query_json(lease, "{\"shared_log\":true}", &error);
  lease->close(lease);

  corrupt_query_index_tail(root);
  corrupt_size = test_query_index_size(root);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  lc_query_req_init(&req);
  req.selector_json = "{}";
  rc = scan_client->query(scan_client, &req, sink, &doc_res, &error);
  assert_int_equal(rc, LC_OK);
  text = memory_sink_text(sink);
  assert_non_null(strstr(text, "shared-log-doc"));
  assert_non_null(strstr(text, "\"document\":{\"shared_log\":true}"));
  assert_string_equal(doc_res.return_mode, "documents");
  assert_int_equal(doc_res.index_seq, 0UL);
  assert_int_equal(test_query_index_size(root), corrupt_size);
  free(text);
  lc_sink_close(sink);
  lc_query_res_cleanup(&doc_res);

  handler.begin = query_key_capture_begin;
  handler.chunk = query_key_capture_chunk;
  handler.end = query_key_capture_end;
  lc_query_req_init(&req);
  req.selector_json = "{}";
  rc = scan_client->query_keys(scan_client, &req, &handler, &capture, &key_res,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.key_count, 1U);
  assert_string_equal(capture.keys[0], "shared-log-doc");
  assert_string_equal(key_res.return_mode, "keys");
  assert_int_equal(key_res.index_seq, 0UL);
  assert_int_equal(test_query_index_size(root), corrupt_size);

  lc_query_res_cleanup(&key_res);
  writer_client->close(writer_client);
  scan_client->close(scan_client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_explicit_scan_query_keys_bypasses_fallback(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_query_req req;
  lc_query_res res;
  lc_query_key_handler handler;
  query_key_capture_state capture;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root),
                 "query-keys-explicit-scan-bypasses-fallback");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  client = open_pouch_client_with_query_config(endpoint, "scan", "index");
  lease = pouch_acquire_query_key(client, "explicit-scan-key", &error);

  handler.begin = query_key_capture_begin;
  handler.chunk = query_key_capture_chunk;
  handler.end = query_key_capture_end;
  lc_query_req_init(&req);
  req.selector_json = "{}";
  req.engine = "scan";
  rc = client->query_keys(client, &req, &handler, &capture, &res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.key_count, 1U);
  assert_string_equal(capture.keys[0], "explicit-scan-key");
  assert_string_equal(res.return_mode, "keys");
  assert_int_equal(res.index_seq, 0UL);

  lc_query_res_cleanup(&res);
  memset(&res, 0, sizeof(res));
  memset(&capture, 0, sizeof(capture));
  memset(&error, 0, sizeof(error));
  lc_query_req_init(&req);
  req.selector_json = "{}";
  req.engine = "scan";
  req.refresh = "wait_for";
  rc = client->query_keys(client, &req, &handler, &capture, &res, &error);
  assert_pouch_unsupported(rc, &error,
                           "pouch scan query_keys does not support refresh");
  assert_int_equal(capture.key_count, 0U);

  lease->close(lease);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_configured_scan_fallback_query_keys(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_query_req req;
  lc_query_res res;
  lc_query_key_handler handler;
  query_key_capture_state capture;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-keys-configured-fallback");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  client = open_pouch_client_with_query_config(endpoint, "index", "scan");
  lease = pouch_acquire_query_key(client, "fallback", &error);

  handler.begin = query_key_capture_begin;
  handler.chunk = query_key_capture_chunk;
  handler.end = query_key_capture_end;
  lc_query_req_init(&req);
  req.selector_json = "{}";
  rc = client->query_keys(client, &req, &handler, &capture, &res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.key_count, 1U);
  assert_string_equal(capture.keys[0], "fallback");

  lc_query_res_cleanup(&res);
  lease->close(lease);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_pouch_endpoint_scan_primary_query_keys_uses_index_fallback_for_refresh(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_query_req req;
  lc_query_res res;
  lc_query_key_handler handler;
  query_key_capture_state capture;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-keys-scan-index-fallback");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  client = open_pouch_client_with_query_config(endpoint, "scan", "index");
  lease = pouch_acquire_query_key(client, "fallback-refresh-key", &error);

  handler.begin = query_key_capture_begin;
  handler.chunk = query_key_capture_chunk;
  handler.end = query_key_capture_end;
  lc_query_req_init(&req);
  req.selector_json = "{}";
  req.refresh = "wait_for";
  rc = client->query_keys(client, &req, &handler, &capture, &res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.key_count, 1U);
  assert_string_equal(capture.keys[0], "fallback-refresh-key");
  assert_string_equal(res.return_mode, "keys");
  assert_true(res.index_seq > 0UL);

  lc_query_res_cleanup(&res);
  lease->close(lease);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_explicit_index_overrides_scan_config(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_query_req req;
  lc_query_res res;
  lc_sink *sink;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-explicit-index-over-scan");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  client = open_pouch_client_with_query_config(endpoint, "scan", NULL);
  lease = pouch_acquire_query_key(client, "explicit-index-doc", &error);
  pouch_save_query_json(lease, "{\"explicit_index\":true}", &error);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  lc_query_req_init(&req);
  req.selector_json = "{}";
  req.engine = "index";
  rc = client->query(client, &req, sink, &res, &error);
  assert_int_equal(rc, LC_OK);
  text = memory_sink_text(sink);
  assert_non_null(strstr(text, "\"key\":\"explicit-index-doc\""));
  assert_non_null(strstr(text, "\"document\":{\"explicit_index\":true}"));
  assert_string_equal(res.return_mode, "documents");
  assert_true(res.index_seq > 0UL);

  free(text);
  lc_sink_close(sink);
  lc_query_res_cleanup(&res);
  lease->close(lease);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_explicit_index_keys_override_scan_config(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_query_req req;
  lc_query_res res;
  lc_query_key_handler handler;
  query_key_capture_state capture;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-keys-explicit-index-over-scan");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  client = open_pouch_client_with_query_config(endpoint, "scan", NULL);
  lease = pouch_acquire_query_key(client, "explicit-index-key", &error);

  handler.begin = query_key_capture_begin;
  handler.chunk = query_key_capture_chunk;
  handler.end = query_key_capture_end;
  lc_query_req_init(&req);
  req.selector_json = "{}";
  req.engine = "index";
  rc = client->query_keys(client, &req, &handler, &capture, &res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.key_count, 1U);
  assert_string_equal(capture.keys[0], "explicit-index-key");
  assert_string_equal(res.return_mode, "keys");
  assert_true(res.index_seq > 0UL);

  lc_query_res_cleanup(&res);
  lease->close(lease);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_scan_query_keys_rejects_lql_selector(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_query_req req;
  lc_query_res res;
  lc_query_key_handler handler;
  query_key_capture_state capture;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-keys-scan-selector");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  client = open_pouch_client(endpoint);

  handler.begin = query_key_capture_begin;
  handler.chunk = query_key_capture_chunk;
  handler.end = query_key_capture_end;
  lc_query_req_init(&req);
  req.selector_json = "{\"key\":\"alpha\"}";
  req.engine = "scan";
  rc = client->query_keys(client, &req, &handler, &capture, &res, &error);
  assert_pouch_unsupported(
      rc, &error, "pouch scan query_keys supports only match-all selector");
  assert_int_equal(capture.key_count, 0U);

  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_scan_query_keys_propagates_handler_error(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_query_req req;
  lc_query_res res;
  lc_query_key_handler handler;
  query_key_capture_state capture;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-keys-scan-handler-error");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));
  client = open_pouch_client(endpoint);
  lease = pouch_acquire_query_key(client, "handler-error", &error);

  capture.fail_on_chunk = 1;
  handler.begin = query_key_capture_begin;
  handler.chunk = query_key_capture_chunk;
  handler.end = query_key_capture_end;
  lc_query_req_init(&req);
  req.selector_json = "{}";
  req.engine = "scan";
  rc = client->query_keys(client, &req, &handler, &capture, &res, &error);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_string_equal(error.message, "query key capture rejected chunk");
  assert_int_equal(capture.begin_calls, 1U);
  assert_int_equal(capture.chunk_calls, 1U);
  assert_int_equal(capture.end_calls, 0U);

  lc_query_res_cleanup(&res);
  lease->close(lease);
  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_rejects_invalid_query_mode_config(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client_config config;
  lc_client *client;
  lc_error error;
  const char *endpoints[1];
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-mode-invalid");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  lc_client_config_init(&config);
  endpoints[0] = endpoint;
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.pouch_query_engine = "linear";
  client = NULL;
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(client);
  assert_string_equal(error.message,
                      "pouch_query_engine must be index or scan");
  lc_error_cleanup(&error);

  memset(&error, 0, sizeof(error));
  config.pouch_query_engine = "index";
  config.pouch_query_fallback_engine = "linear";
  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(client);
  assert_string_equal(
      error.message, "pouch_query_fallback_engine must be none, index, or scan");
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_rejects_invalid_query_engine_hint(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_query_req req;
  lc_query_res res;
  lc_sink *sink;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-mode-invalid-hint");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&res, 0, sizeof(res));
  client = open_pouch_client(endpoint);
  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);

  lc_query_req_init(&req);
  req.selector_json = "{}";
  req.engine = "linear";
  rc = client->query(client, &req, sink, &res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message, "pouch query engine must be index or scan");

  lc_error_cleanup(&error);
  lc_sink_close(sink);
  client->close(client);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_reports_local_unsupported_surfaces(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_query_key_handler key_handler;
  lc_namespace_config_req namespace_req;
  lc_namespace_config_res namespace_res;
  lc_index_flush_req flush_req;
  lc_index_flush_res flush_res;
  lc_txn_replay_req replay_req;
  lc_txn_replay_res replay_res;
  lc_txn_decision_req decision_req;
  lc_txn_decision_res decision_res;
  lc_tc_lease_acquire_req tc_acquire_req;
  lc_tc_lease_acquire_res tc_acquire_res;
  lc_tc_leader_res tc_leader_res;
  lc_sink *sink;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "unsupported-surfaces");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&query_res, 0, sizeof(query_res));
  memset(&namespace_res, 0, sizeof(namespace_res));
  memset(&flush_res, 0, sizeof(flush_res));
  memset(&replay_res, 0, sizeof(replay_res));
  memset(&decision_res, 0, sizeof(decision_res));
  memset(&tc_acquire_res, 0, sizeof(tc_acquire_res));
  memset(&tc_leader_res, 0, sizeof(tc_leader_res));
  client = open_pouch_client(endpoint);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  lc_query_req_init(&query_req);
  query_req.selector_json = "{\"key\":\"alpha\"}";
  rc = client->query(client, &query_req, sink, &query_res, &error);
  assert_pouch_unsupported(rc, &error,
                           "pouch index query supports only match-all selector");
  lc_sink_close(sink);

  memset(&key_handler, 0, sizeof(key_handler));
  key_handler.begin = query_key_begin_unexpected;
  key_handler.chunk = query_key_chunk_unexpected;
  key_handler.end = query_key_end_unexpected;
  rc = client->query_keys(client, &query_req, &key_handler, NULL, &query_res,
                          &error);
  assert_pouch_unsupported(rc, &error,
                           "pouch index query_keys supports only match-all "
                           "selector");

  lc_namespace_config_req_init(&namespace_req);
  namespace_req.namespace_name = "default";
  rc = client->get_namespace_config(client, &namespace_req, &namespace_res,
                                    &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(namespace_res.preferred_engine, "index");
  assert_string_equal(namespace_res.fallback_engine, "none");
  lc_namespace_config_res_cleanup(&namespace_res);
  namespace_req.preferred_engine = "index";
  rc = client->update_namespace_config(client, &namespace_req, &namespace_res,
                                       &error);
  assert_pouch_unsupported(
      rc, &error, "pouch namespace management is not supported");

  lc_index_flush_req_init(&flush_req);
  flush_req.namespace_name = "default";
  flush_req.mode = "wait";
  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(flush_res.namespace_name, "default");
  assert_string_equal(flush_res.mode, "wait");
  assert_true(flush_res.accepted);
  assert_true(flush_res.flushed);
  assert_false(flush_res.pending);
  lc_index_flush_res_cleanup(&flush_res);

  lc_txn_replay_req_init(&replay_req);
  replay_req.txn_id = "txn-1";
  rc = client->txn_replay(client, &replay_req, &replay_res, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 404L);
  lc_error_cleanup(&error);
  lc_txn_decision_req_init(&decision_req);
  decision_req.txn_id = "txn-1";
  rc = client->txn_commit(client, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "pouch transaction decision requires txn_id and "
                      "participants");
  lc_error_cleanup(&error);

  lc_tc_lease_acquire_req_init(&tc_acquire_req);
  tc_acquire_req.candidate_id = "candidate";
  tc_acquire_req.candidate_endpoint = "pouch://candidate";
  tc_acquire_req.term = 1UL;
  tc_acquire_req.ttl_ms = 1000L;
  rc = client->tc_lease_acquire(client, &tc_acquire_req, &tc_acquire_res,
                                &error);
  assert_pouch_unsupported(
      rc, &error, "pouch transaction coordinator is not supported");
  rc = client->tc_leader(client, &tc_leader_res, &error);
  assert_pouch_unsupported(
      rc, &error, "pouch transaction coordinator is not supported");

  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_txn_commit_promotes_staged_state(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_source *source;
  lc_acquire_req acquire;
  lc_release_req release_req;
  lc_txn_participant participant;
  lc_txn_decision_req decision_req;
  lc_txn_decision_res decision_res;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "txn-commit");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&decision_res, 0, sizeof(decision_res));
  client = open_pouch_client(endpoint);
  lease = NULL;

  lc_acquire_req_init(&acquire);
  acquire.key = "txn/key";
  acquire.owner = "seed";
  acquire.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("{\"value\":1}");
  rc = lease->update(lease, source, NULL, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  lease = NULL;

  acquire.owner = "txn-owner";
  acquire.txn_id = "txn-commit-1";
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(lease->txn_id, "txn-commit-1");
  seed_pouch_staged_state(client, lease, "{\"value\":2}", &error);

  memset(&participant, 0, sizeof(participant));
  participant.namespace_name = "default";
  participant.key = "txn/key";
  lc_txn_decision_req_init(&decision_req);
  decision_req.txn_id = "txn-commit-1";
  decision_req.participants = &participant;
  decision_req.participant_count = 1U;
  rc = client->txn_commit(client, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(decision_res.txn_id, "txn-commit-1");
  assert_string_equal(decision_res.state, "committed");
  lc_txn_decision_res_cleanup(&decision_res);
  lc_lease_close(lease);
  lease = NULL;

  assert_pouch_client_state_text(client, "txn/key", "{\"value\":2}", &error);
  acquire.owner = "after-commit";
  acquire.txn_id = NULL;
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_OK);
  rc = lease->release(lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);

  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_txn_rollback_discards_staged_state(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_source *source;
  lc_acquire_req acquire;
  lc_release_req release_req;
  lc_txn_participant participant;
  lc_txn_decision_req decision_req;
  lc_txn_decision_res decision_res;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "txn-rollback");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&decision_res, 0, sizeof(decision_res));
  client = open_pouch_client(endpoint);
  lease = NULL;

  lc_acquire_req_init(&acquire);
  acquire.key = "txn/rollback-key";
  acquire.owner = "seed";
  acquire.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("{\"value\":1}");
  rc = lease->update(lease, source, NULL, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  lease = NULL;

  acquire.owner = "txn-owner";
  acquire.txn_id = "txn-rollback-1";
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_OK);
  seed_pouch_staged_state(client, lease, "{\"value\":99}", &error);

  memset(&participant, 0, sizeof(participant));
  participant.namespace_name = "default";
  participant.key = "txn/rollback-key";
  lc_txn_decision_req_init(&decision_req);
  decision_req.txn_id = "txn-rollback-1";
  decision_req.participants = &participant;
  decision_req.participant_count = 1U;
  rc = client->txn_rollback(client, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(decision_res.state, "rolled_back");
  lc_txn_decision_res_cleanup(&decision_res);
  lc_lease_close(lease);
  lease = NULL;

  assert_pouch_client_state_text(client, "txn/rollback-key", "{\"value\":1}",
                                 &error);

  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_txn_recovery_expired_prepare_after_reopen(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_source *source;
  lc_acquire_req acquire;
  lc_release_req release_req;
  lc_metadata_req metadata_req;
  lc_txn_participant participant;
  lc_txn_decision_req decision_req;
  lc_txn_decision_res decision_res;
  lc_txn_replay_req replay_req;
  lc_txn_replay_res replay_res;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "txn-replay-expired");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&decision_res, 0, sizeof(decision_res));
  memset(&replay_res, 0, sizeof(replay_res));
  client = open_pouch_client(endpoint);
  lease = NULL;

  lc_acquire_req_init(&acquire);
  acquire.key = "txn/replay-key";
  acquire.owner = "seed";
  acquire.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("{\"value\":1}");
  rc = lease->update(lease, source, NULL, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  lease = NULL;

  acquire.owner = "txn-owner";
  acquire.txn_id = "txn-replay-expired-1";
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_OK);
  lc_metadata_req_init(&metadata_req);
  metadata_req.has_query_hidden = 1;
  metadata_req.query_hidden = 1;
  rc = lease->metadata(lease, &metadata_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(lease->has_query_hidden);
  assert_true(lease->query_hidden);
  seed_pouch_staged_state(client, lease, "{\"value\":99}", &error);

  memset(&participant, 0, sizeof(participant));
  participant.namespace_name = "default";
  participant.key = "txn/replay-key";
  lc_txn_decision_req_init(&decision_req);
  decision_req.txn_id = "txn-replay-expired-1";
  decision_req.participants = &participant;
  decision_req.participant_count = 1U;
  decision_req.expires_at_unix = 1L;
  rc = client->txn_prepare(client, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(decision_res.state, "prepared");
  lc_txn_decision_res_cleanup(&decision_res);
  lc_lease_close(lease);
  lease = NULL;
  client->close(client);

  client = open_pouch_client(endpoint);
  assert_pouch_client_state_text(client, "txn/replay-key", "{\"value\":1}",
                                 &error);
  lc_txn_replay_req_init(&replay_req);
  replay_req.txn_id = "txn-replay-expired-1";
  rc = client->txn_replay(client, &replay_req, &replay_res, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 404L);
  lc_error_cleanup(&error);

  acquire.owner = "after-replay";
  acquire.txn_id = NULL;
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(lease->has_query_hidden);
  assert_true(lease->query_hidden);
  rc = lease->release(lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);

  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_pouch_endpoint_txn_recovery_continues_after_partial_rollback(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *first_lease;
  lc_lease *second_lease;
  lc_source *source;
  lc_acquire_req acquire;
  lc_release_req release_req;
  lc_txn_participant participants[2];
  lc_txn_decision_req decision_req;
  lc_txn_decision_res decision_res;
  lc_txn_replay_req replay_req;
  lc_txn_replay_res replay_res;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "txn-recovery-partial-rollback");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&decision_res, 0, sizeof(decision_res));
  memset(&replay_res, 0, sizeof(replay_res));
  client = open_pouch_client(endpoint);
  first_lease = NULL;
  second_lease = NULL;

  lc_acquire_req_init(&acquire);
  acquire.owner = "seed";
  acquire.ttl_seconds = 60L;
  acquire.key = "txn/partial-a";
  rc = client->acquire(client, &acquire, &first_lease, &error);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("{\"value\":\"a1\"}");
  rc = first_lease->update(first_lease, source, NULL, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_release_req_init(&release_req);
  rc = first_lease->release(first_lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  first_lease = NULL;

  acquire.key = "txn/partial-b";
  rc = client->acquire(client, &acquire, &second_lease, &error);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("{\"value\":\"b1\"}");
  rc = second_lease->update(second_lease, source, NULL, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  rc = second_lease->release(second_lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  second_lease = NULL;

  acquire.owner = "txn-owner";
  acquire.txn_id = "txn-partial-rollback-1";
  acquire.key = "txn/partial-a";
  rc = client->acquire(client, &acquire, &first_lease, &error);
  assert_int_equal(rc, LC_OK);
  seed_pouch_staged_state(client, first_lease, "{\"value\":\"a2\"}", &error);
  acquire.key = "txn/partial-b";
  rc = client->acquire(client, &acquire, &second_lease, &error);
  assert_int_equal(rc, LC_OK);
  seed_pouch_staged_state(client, second_lease, "{\"value\":\"b2\"}", &error);

  memset(participants, 0, sizeof(participants));
  participants[0].namespace_name = "default";
  participants[0].key = "txn/partial-a";
  participants[1].namespace_name = "default";
  participants[1].key = "txn/partial-b";
  lc_txn_decision_req_init(&decision_req);
  decision_req.txn_id = "txn-partial-rollback-1";
  decision_req.participants = participants;
  decision_req.participant_count = 2U;
  decision_req.expires_at_unix = 1L;
  rc = client->txn_prepare(client, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(decision_res.state, "prepared");
  lc_txn_decision_res_cleanup(&decision_res);

  partial_pouch_rollback_participant(client, "default", "txn/partial-a",
                                     "txn-partial-rollback-1", &error);
  lc_lease_close(first_lease);
  lc_lease_close(second_lease);
  first_lease = NULL;
  second_lease = NULL;
  client->close(client);

  client = open_pouch_client(endpoint);
  assert_pouch_client_state_text(client, "txn/partial-a", "{\"value\":\"a1\"}",
                                 &error);
  assert_pouch_client_state_text(client, "txn/partial-b", "{\"value\":\"b1\"}",
                                 &error);
  lc_txn_replay_req_init(&replay_req);
  replay_req.txn_id = "txn-partial-rollback-1";
  rc = client->txn_replay(client, &replay_req, &replay_res, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 404L);
  lc_error_cleanup(&error);

  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_txn_recovery_scans_decision_objects(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_source *source;
  lc_acquire_req acquire;
  lc_release_req release_req;
  lc_txn_participant participant;
  lc_txn_decision_req decision_req;
  lc_txn_decision_res decision_res;
  lc_txn_replay_req replay_req;
  lc_txn_replay_res replay_res;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "txn-recovery-object-scan");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  memset(&decision_res, 0, sizeof(decision_res));
  memset(&replay_res, 0, sizeof(replay_res));
  client = open_pouch_client(endpoint);
  lease = NULL;

  lc_acquire_req_init(&acquire);
  acquire.key = "txn/object-scan-key";
  acquire.owner = "seed";
  acquire.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("{\"value\":1}");
  rc = lease->update(lease, source, NULL, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  lease = NULL;

  acquire.owner = "txn-owner";
  acquire.txn_id = "txn-object-scan-1";
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_OK);
  seed_pouch_staged_state(client, lease, "{\"value\":2}", &error);

  memset(&participant, 0, sizeof(participant));
  participant.namespace_name = "default";
  participant.key = "txn/object-scan-key";
  lc_txn_decision_req_init(&decision_req);
  decision_req.txn_id = "txn-object-scan-1";
  decision_req.participants = &participant;
  decision_req.participant_count = 1U;
  decision_req.expires_at_unix = 1L;
  rc = client->txn_prepare(client, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(decision_res.state, "prepared");
  lc_txn_decision_res_cleanup(&decision_res);

  partial_pouch_rollback_participant(client, "default", "txn/object-scan-key",
                                     "txn-object-scan-1", &error);
  lc_lease_close(lease);
  lease = NULL;
  client->close(client);

  client = open_pouch_client(endpoint);
  assert_pouch_client_state_text(client, "txn/object-scan-key", "{\"value\":1}",
                                 &error);
  lc_txn_replay_req_init(&replay_req);
  replay_req.txn_id = "txn-object-scan-1";
  rc = client->txn_replay(client, &replay_req, &replay_res, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 404L);
  lc_error_cleanup(&error);

  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pouch_endpoint_txn_recovery_cleans_abandoned_staged_state(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_source *source;
  lc_acquire_req acquire;
  lc_release_req release_req;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "txn-recovery-abandoned-staged");
  test_cleanup_root(root);
  test_endpoint(endpoint, sizeof(endpoint), root);
  memset(&error, 0, sizeof(error));
  client = open_pouch_client(endpoint);
  lease = NULL;

  lc_acquire_req_init(&acquire);
  acquire.key = "txn/abandoned-key";
  acquire.owner = "seed";
  acquire.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("{\"value\":1}");
  rc = lease->update(lease, source, NULL, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  lease = NULL;

  acquire.owner = "txn-owner";
  acquire.txn_id = "txn-abandoned-1";
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_OK);
  seed_pouch_staged_state(client, lease, "{\"value\":2}", &error);
  expire_pouch_participant_lease(client, "default", "txn/abandoned-key",
                                 "txn-abandoned-1", &error);
  lc_lease_close(lease);
  lease = NULL;
  client->close(client);

  client = open_pouch_client(endpoint);
  assert_pouch_client_state_text(client, "txn/abandoned-key", "{\"value\":1}",
                                 &error);
  assert_pouch_staged_state_missing(client, "default", "txn/abandoned-key",
                                    "txn-abandoned-1", &error);

  acquire.owner = "after-abandoned-cleanup";
  acquire.txn_id = NULL;
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease->txn_id);
  assert_true(strcmp(lease->txn_id, "txn-abandoned-1") != 0);
  rc = lease->release(lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);

  client->close(client);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_pouch_endpoint_lease_state_lifecycle),
      cmocka_unit_test(test_pouch_endpoint_lease_save_uses_mapped_lonejson),
      cmocka_unit_test(test_pouch_endpoint_lease_mutate_local_updates_state),
      cmocka_unit_test(test_pouch_endpoint_rejects_missing_acquire_owner),
      cmocka_unit_test(test_pouch_endpoint_generates_implicit_txn_id),
      cmocka_unit_test(
          test_pouch_endpoint_release_is_idempotent_for_stale_refs),
      cmocka_unit_test(
          test_pouch_endpoint_reports_lockd_lease_validation_errors),
      cmocka_unit_test(
          test_pouch_endpoint_attach_rolls_back_object_on_meta_reject),
      cmocka_unit_test(
          test_pouch_endpoint_attachment_selector_requires_matching_id_and_name),
      cmocka_unit_test(
          test_pouch_endpoint_public_attachment_read_after_release),
      cmocka_unit_test(
          test_pouch_endpoint_attachment_rejects_stale_lease_refs),
      cmocka_unit_test(test_pouch_endpoint_rejects_missing_or_wrong_txn_id),
      cmocka_unit_test(test_pouch_endpoint_remove_without_state_is_noop),
      cmocka_unit_test(
          test_pouch_endpoint_release_preserves_state_for_reacquire),
      cmocka_unit_test(test_pouch_endpoint_lease_load_respects_json_limit),
      cmocka_unit_test(test_pouch_endpoint_queue_lifecycle),
      cmocka_unit_test(test_pouch_endpoint_dequeue_waits_for_later_enqueue),
      cmocka_unit_test(test_pouch_endpoint_watch_queue_snapshots),
      cmocka_unit_test(
          test_pouch_endpoint_queue_rejects_negative_timing_options),
      cmocka_unit_test(
          test_pouch_endpoint_queue_variants_reject_missing_owner),
      cmocka_unit_test(
          test_pouch_endpoint_queue_visibility_handoff_rejects_stale_refs),
      cmocka_unit_test(
          test_pouch_endpoint_queue_rejects_expired_delivery_ref),
      cmocka_unit_test(
          test_pouch_endpoint_queue_rejects_missing_or_wrong_txn_id),
      cmocka_unit_test(
          test_pouch_endpoint_queue_rejects_missing_or_stale_meta_etag),
      cmocka_unit_test(
          test_pouch_endpoint_queue_extend_reports_correlation),
      cmocka_unit_test(test_pouch_endpoint_rejects_reserved_namespace),
      cmocka_unit_test(
          test_pouch_endpoint_rejects_non_normalized_identifiers),
      cmocka_unit_test(test_pouch_endpoint_dequeue_with_state_lifecycle),
      cmocka_unit_test(test_pouch_endpoint_dequeue_batch_lifecycle),
      cmocka_unit_test(test_pouch_endpoint_dequeue_batch_honors_start_after),
      cmocka_unit_test(test_pouch_endpoint_subscribe_lifecycle),
      cmocka_unit_test(test_pouch_endpoint_subscribe_honors_start_after),
      cmocka_unit_test(test_pouch_endpoint_subscribe_waits_for_shared_message),
      cmocka_unit_test(test_pouch_endpoint_consumer_service_auto_ack),
      cmocka_unit_test(
          test_pouch_endpoint_consumer_service_honors_start_after),
      cmocka_unit_test(test_pouch_endpoint_consumer_service_with_state),
      cmocka_unit_test(test_pouch_endpoint_reports_query_mode_defaults),
      cmocka_unit_test(test_pouch_endpoint_reports_configured_scan_mode),
      cmocka_unit_test(test_pouch_endpoint_reports_configured_scan_fallback),
      cmocka_unit_test(
          test_pouch_endpoint_query_options_configure_scan_mode),
      cmocka_unit_test(
          test_pouch_endpoint_query_options_override_client_config),
      cmocka_unit_test(
          test_pouch_endpoint_decodes_percent_encoded_path_and_options),
      cmocka_unit_test(test_pouch_endpoint_rejects_invalid_query_options),
      cmocka_unit_test(
          test_pouch_endpoint_rejects_mixed_endpoint_configuration),
      cmocka_unit_test(
          test_pouch_endpoint_scan_query_keys_pages_ordered_visible_keys),
      cmocka_unit_test(
          test_pouch_endpoint_scan_query_streams_documents_with_paging),
      cmocka_unit_test(
          test_pouch_endpoint_scan_query_serializes_metadata_with_lonejson),
      cmocka_unit_test(
          test_pouch_endpoint_default_index_query_streams_documents),
      cmocka_unit_test(
          test_pouch_endpoint_default_index_query_waits_for_refresh),
      cmocka_unit_test(test_pouch_endpoint_default_index_query_keys_pages),
      cmocka_unit_test(
          test_pouch_endpoint_default_index_query_keys_waits_for_refresh),
      cmocka_unit_test(
          test_pouch_endpoint_default_index_query_rejects_unknown_refresh),
      cmocka_unit_test(
          test_pouch_endpoint_flush_index_reports_current_sequence),
      cmocka_unit_test(test_pouch_endpoint_configured_scan_query_without_hint),
      cmocka_unit_test(
          test_pouch_endpoint_explicit_scan_query_bypasses_fallback),
      cmocka_unit_test(
          test_pouch_endpoint_scan_primary_uses_index_fallback_for_refresh),
      cmocka_unit_test(test_pouch_endpoint_configured_scan_fallback_query),
      cmocka_unit_test(
          test_pouch_endpoint_explicit_index_overrides_scan_config),
      cmocka_unit_test(test_pouch_endpoint_scan_query_rejects_lql_selector),
      cmocka_unit_test(
          test_pouch_endpoint_configured_scan_query_keys_without_hint),
      cmocka_unit_test(
          test_pouch_endpoint_configured_scan_ignores_corrupt_query_sidecar),
      cmocka_unit_test(
          test_pouch_endpoint_configured_scan_ignores_absent_or_future_sidecar),
      cmocka_unit_test(test_pouch_endpoint_configured_scan_refreshes_shared_log),
      cmocka_unit_test(
          test_pouch_endpoint_explicit_scan_query_keys_bypasses_fallback),
      cmocka_unit_test(
          test_pouch_endpoint_scan_primary_query_keys_uses_index_fallback_for_refresh),
      cmocka_unit_test(
          test_pouch_endpoint_explicit_index_keys_override_scan_config),
      cmocka_unit_test(
          test_pouch_endpoint_configured_scan_fallback_query_keys),
      cmocka_unit_test(
          test_pouch_endpoint_scan_query_keys_rejects_lql_selector),
      cmocka_unit_test(
          test_pouch_endpoint_scan_query_keys_propagates_handler_error),
      cmocka_unit_test(
          test_pouch_endpoint_rejects_invalid_query_mode_config),
      cmocka_unit_test(
          test_pouch_endpoint_rejects_invalid_query_engine_hint),
      cmocka_unit_test(
          test_pouch_endpoint_reports_local_unsupported_surfaces),
      cmocka_unit_test(test_pouch_endpoint_txn_commit_promotes_staged_state),
      cmocka_unit_test(
          test_pouch_endpoint_txn_rollback_discards_staged_state),
      cmocka_unit_test(
          test_pouch_endpoint_txn_recovery_expired_prepare_after_reopen),
      cmocka_unit_test(
          test_pouch_endpoint_txn_recovery_continues_after_partial_rollback),
      cmocka_unit_test(
          test_pouch_endpoint_txn_recovery_scans_decision_objects),
      cmocka_unit_test(
          test_pouch_endpoint_txn_recovery_cleans_abandoned_staged_state),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}

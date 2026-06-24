#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <pthread.h>
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

static void assert_lc_server_error(int rc, lc_error *error, long http_status) {
  if (rc == LC_OK) {
    print_message("expected pouch server error, got success\n");
  } else {
    print_message(
        "expected pouch server error: code=%d http=%ld message=%s detail=%s "
        "server_code=%s correlation=%s\n",
        error != NULL ? error->code : -1,
        error != NULL ? error->http_status : 0L,
        error != NULL && error->message != NULL ? error->message : "(null)",
        error != NULL && error->detail != NULL ? error->detail : "(null)",
        error != NULL && error->server_code != NULL ? error->server_code
                                                    : "(null)",
        error != NULL && error->correlation_id != NULL ? error->correlation_id
                                                       : "(null)");
  }
  assert_int_not_equal(rc, LC_OK);
  assert_non_null(error);
  assert_int_equal(error->code, LC_ERR_SERVER);
  if (http_status > 0L) {
    assert_int_equal(error->http_status, http_status);
  } else {
    assert_true(error->http_status >= 400L);
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

static void pouch_endpoint_with_query(char *endpoint, size_t endpoint_size,
                                      const char *root, const char *query) {
  snprintf(endpoint, endpoint_size, "pouch://%s?%s", root, query);
}

static void cleanup_pouch_root(const char *root) {
  char path[512];

  snprintf(path, sizeof(path), "%s/store.compact.tmp", root);
  unlink(path);
  snprintf(path, sizeof(path), "%s/query.index.compact.tmp", root);
  unlink(path);
  snprintf(path, sizeof(path), "%s/store.log", root);
  unlink(path);
  snprintf(path, sizeof(path), "%s/writer.lock", root);
  unlink(path);
  snprintf(path, sizeof(path), "%s/query.index", root);
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

static void open_pouch_client_with_namespace(const char *endpoint,
                                             const char *namespace_name,
                                             lc_client **out,
                                             lc_error *error) {
  lc_client_config config;
  const char *endpoints[1];
  int rc;

  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = namespace_name;
  rc = lc_client_open(&config, out, error);
  assert_lc_ok(rc, error);
  assert_non_null(*out);
}

static void open_pouch_scan_client(const char *endpoint, lc_client **out,
                                   lc_error *error) {
  lc_client_config config;
  const char *endpoints[1];
  int rc;

  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "default";
  config.pouch_query_engine = "scan";
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

static char *sink_text(lc_sink *sink, lc_error *error) {
  const void *bytes;
  size_t length;
  char *text;
  int rc;

  bytes = NULL;
  length = 0U;
  rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  assert_lc_ok(rc, error);
  text = (char *)malloc(length + 1U);
  assert_non_null(text);
  if (length > 0U) {
    memcpy(text, bytes, length);
  }
  text[length] = '\0';
  return text;
}

typedef struct query_key_capture {
  char keys[4][256];
  size_t key_count;
  size_t current_length;
  unsigned int begin_calls;
  unsigned int chunk_calls;
  unsigned int end_calls;
} query_key_capture;

static int query_key_capture_begin(void *context, lc_error *error) {
  query_key_capture *capture;

  (void)error;
  capture = (query_key_capture *)context;
  assert_non_null(capture);
  assert_true(capture->key_count < 4U);
  capture->begin_calls++;
  capture->current_length = 0U;
  capture->keys[capture->key_count][0] = '\0';
  return 1;
}

static int query_key_capture_chunk(void *context, const char *bytes,
                                   size_t len, lc_error *error) {
  query_key_capture *capture;

  (void)error;
  capture = (query_key_capture *)context;
  assert_non_null(capture);
  assert_non_null(bytes);
  assert_true(capture->key_count < 4U);
  assert_true(capture->current_length + len <
              sizeof(capture->keys[capture->key_count]));
  memcpy(capture->keys[capture->key_count] + capture->current_length, bytes,
         len);
  capture->current_length += len;
  capture->keys[capture->key_count][capture->current_length] = '\0';
  capture->chunk_calls++;
  return 1;
}

static int query_key_capture_end(void *context, lc_error *error) {
  query_key_capture *capture;

  (void)error;
  capture = (query_key_capture *)context;
  assert_non_null(capture);
  assert_true(capture->key_count < 4U);
  capture->key_count++;
  capture->end_calls++;
  return 1;
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

static void assert_client_state_empty(lc_client *client, const char *key,
                                      lc_error *error) {
  lc_sink *sink;
  lc_get_res get_res;
  int rc;

  sink = NULL;
  memset(&get_res, 0, sizeof(get_res));
  rc = lc_sink_to_memory(&sink, error);
  assert_lc_ok(rc, error);
  rc = client->get(client, key, NULL, sink, &get_res, error);
  assert_lc_ok(rc, error);
  assert_true(get_res.no_content);
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

typedef struct pouch_consumer_failure_test {
  lc_consumer_service *service;
  size_t handled;
  size_t errors;
  long first_attempts;
  long redelivery_attempts;
  long redelivery_failures;
  int saw_delivery_error;
} pouch_consumer_failure_test;

typedef struct pouch_acquire_for_update_test {
  lc_client *observer;
  const char *key;
  const char *expected_snapshot;
  const char *expected_visible_during_update;
  const char *next_state;
  int saw_snapshot;
  int checked_staged_invisible;
} pouch_acquire_for_update_test;

typedef struct pouch_subscribe_state_test {
  size_t handled;
  char queue[64];
  char message_id[128];
  char state_key[256];
} pouch_subscribe_state_test;

typedef struct pouch_subscribe_wait_test {
  const char *endpoint;
  int rc;
  lc_error error;
  size_t handled;
  int payload_ok;
  char message_id[128];
} pouch_subscribe_wait_test;

typedef struct pouch_delayed_enqueue_test {
  const char *endpoint;
  const char *queue;
  const char *payload;
  int rc;
  lc_error error;
} pouch_delayed_enqueue_test;

typedef struct pouch_watch_state {
  size_t handled;
  int available;
  char queue[64];
  char head_message_id[128];
  char correlation_id[64];
} pouch_watch_state;

static int pouch_test_error(lc_error *error, const char *message) {
  if (error != NULL) {
    error->code = LC_ERR_INVALID;
    error->message = strdup(message);
    if (error->message == NULL) {
      return LC_ERR_NOMEM;
    }
  }
  return LC_ERR_INVALID;
}

static int pouch_acquire_for_update_handler(
    void *context, lc_acquire_for_update_context *update, lc_error *error) {
  pouch_acquire_for_update_test *test;
  lc_sink *sink;
  lc_source *source;
  const void *bytes;
  size_t length;
  size_t written;
  int rc;

  test = (pouch_acquire_for_update_test *)context;
  sink = NULL;
  source = NULL;
  bytes = NULL;
  length = 0U;
  written = 0U;
  assert_non_null(test);
  assert_non_null(update);
  assert_non_null(update->lease);
  assert_true(update->state.has_state);
  assert_non_null(update->state.reader);
  assert_true(update->state.version > 0L);

  rc = lc_sink_to_memory(&sink, error);
  assert_lc_ok(rc, error);
  rc = lc_copy(update->state.reader, sink, &written, error);
  assert_lc_ok(rc, error);
  assert_true(written > 0U);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  assert_lc_ok(rc, error);
  assert_int_equal(length, strlen(test->expected_snapshot));
  assert_memory_equal(bytes, test->expected_snapshot, length);
  test->saw_snapshot = 1;
  lc_sink_close(sink);

  source = source_from_text(test->next_state, error);
  rc = update->lease->update(update->lease, source, NULL, error);
  lc_source_close(source);
  if (rc == LC_OK && test->observer != NULL &&
      test->expected_visible_during_update != NULL) {
    assert_client_state_text(test->observer, test->key,
                             test->expected_visible_during_update, error);
    test->checked_staged_invisible = 1;
  }
  return rc;
}

static int pouch_acquire_for_update_failing_handler(
    void *context, lc_acquire_for_update_context *update, lc_error *error) {
  lc_source *source;
  int rc;

  (void)context;
  assert_non_null(update);
  assert_non_null(update->lease);
  source = source_from_text("{\"value\":3,\"via\":\"rollback\"}", error);
  rc = update->lease->update(update->lease, source, NULL, error);
  lc_source_close(source);
  assert_lc_ok(rc, error);
  if (error != NULL) {
    error->code = LC_ERR_INVALID;
    error->message = strdup("intentional pouch acquire_for_update failure");
    assert_non_null(error->message);
  }
  return LC_ERR_INVALID;
}

static int pouch_acquire_for_update_empty_handler(
    void *context, lc_acquire_for_update_context *update, lc_error *error) {
  pouch_acquire_for_update_test *test;
  lc_source *source;
  int rc;

  test = (pouch_acquire_for_update_test *)context;
  source = NULL;
  assert_non_null(test);
  assert_non_null(update);
  assert_non_null(update->lease);
  assert_false(update->state.has_state);
  assert_null(update->state.reader);
  assert_int_equal(update->state.version, 0L);
  test->saw_snapshot = 1;

  source = source_from_text(test->next_state, error);
  rc = update->lease->update(update->lease, source, NULL, error);
  lc_source_close(source);
  if (rc == LC_OK && test->observer != NULL) {
    assert_client_state_empty(test->observer, test->key, error);
    test->checked_staged_invisible = 1;
  }
  return rc;
}

static int pouch_acquire_for_update_noop_handler(
    void *context, lc_acquire_for_update_context *update, lc_error *error) {
  pouch_acquire_for_update_test *test;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  size_t written;
  int rc;

  test = (pouch_acquire_for_update_test *)context;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  written = 0U;

  assert_non_null(test);
  assert_non_null(update);
  assert_non_null(update->lease);
  if (test->expected_snapshot != NULL) {
    assert_true(update->state.has_state);
    assert_non_null(update->state.reader);
    assert_true(update->state.version > 0L);
    rc = lc_sink_to_memory(&sink, error);
    assert_lc_ok(rc, error);
    rc = lc_copy(update->state.reader, sink, &written, error);
    assert_lc_ok(rc, error);
    assert_true(written > 0U);
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
    assert_lc_ok(rc, error);
    assert_int_equal(length, strlen(test->expected_snapshot));
    assert_memory_equal(bytes, test->expected_snapshot, length);
    lc_sink_close(sink);
  } else {
    assert_false(update->state.has_state);
    assert_null(update->state.reader);
    assert_int_equal(update->state.version, 0L);
  }

  test->saw_snapshot = 1;
  if (test->observer != NULL && test->key != NULL) {
    if (test->expected_visible_during_update != NULL) {
      assert_client_state_text(test->observer, test->key,
                               test->expected_visible_during_update, error);
    } else {
      assert_client_state_empty(test->observer, test->key, error);
    }
    test->checked_staged_invisible = 1;
  }
  return LC_OK;
}

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

static int pouch_consumer_failure_handle(void *context,
                                         lc_consumer_message *message,
                                         lc_error *error) {
  pouch_consumer_failure_test *state;
  lc_sink *sink;
  size_t written;
  int rc;

  (void)error;
  state = (pouch_consumer_failure_test *)context;
  assert_non_null(state);
  assert_non_null(message);
  assert_non_null(message->message);
  assert_false(message->with_state);
  assert_null(message->state);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, error);
  assert_lc_ok(rc, error);
  written = 0U;
  rc = message->message->write_payload(message->message, sink, &written,
                                       error);
  assert_lc_ok(rc, error);
  assert_int_equal(written, strlen("retry-work"));
  assert_sink_text(sink, "retry-work", error);
  lc_sink_close(sink);

  state->handled += 1U;
  if (state->handled == 1U) {
    state->first_attempts = message->message->attempts;
    assert_int_equal(message->message->attempts, 1);
    assert_int_equal(message->message->failure_attempts, 0);
    return LC_ERR_TRANSPORT;
  }

  state->redelivery_attempts = message->message->attempts;
  state->redelivery_failures = message->message->failure_attempts;
  assert_int_equal(message->message->attempts, 2);
  assert_int_equal(message->message->failure_attempts, 1);
  rc = lc_consumer_service_stop(state->service);
  assert_int_equal(rc, LC_OK);
  return LC_OK;
}

static int pouch_consumer_failure_on_error(
    void *context, const lc_consumer_error *event, lc_error *error) {
  pouch_consumer_failure_test *state;

  (void)error;
  state = (pouch_consumer_failure_test *)context;
  assert_non_null(state);
  assert_non_null(event);
  assert_string_equal(event->queue, "managed-retry");
  assert_int_equal(event->with_state, 0);
  assert_int_equal(event->attempt, 0);
  assert_int_equal(event->restart_in_ms, 0L);
  assert_non_null(event->cause);
  assert_int_equal(event->cause->code, LC_ERR_TRANSPORT);
  state->errors += 1U;
  state->saw_delivery_error = 1;
  return LC_OK;
}

static int pouch_subscribe_state_handle(void *context, lc_message *message,
                                        lc_error *error) {
  pouch_subscribe_state_test *state;
  lc_update_opts update_opts;
  lc_source *source;
  lc_lease *queue_state;
  lc_sink *sink;
  size_t written;
  int rc;

  state = (pouch_subscribe_state_test *)context;
  assert_non_null(state);
  assert_non_null(message);
  assert_int_equal(state->handled, 0U);
  assert_string_equal(message->queue, "subscribe-state");
  queue_state = message->state(message);
  assert_non_null(queue_state);

  sink = NULL;
  rc = lc_sink_to_memory(&sink, error);
  assert_lc_ok(rc, error);
  written = 0U;
  rc = message->write_payload(message, sink, &written, error);
  assert_lc_ok(rc, error);
  assert_int_equal(written, strlen("subscribe-work"));
  assert_sink_text(sink, "subscribe-work", error);
  lc_sink_close(sink);

  snprintf(state->queue, sizeof(state->queue), "%s", message->queue);
  snprintf(state->message_id, sizeof(state->message_id), "%s",
           message->message_id);
  snprintf(state->state_key, sizeof(state->state_key), "q/%s/state/%s",
           state->queue, state->message_id);

  lc_update_opts_init(&update_opts);
  update_opts.content_type = "application/json";
  source = source_from_text("{\"subscribe\":\"stateful\",\"saved\":true}",
                            error);
  rc = queue_state->update(queue_state, source, &update_opts, error);
  lc_source_close(source);
  assert_lc_ok(rc, error);

  state->handled += 1U;
  rc = message->ack(message, error);
  assert_lc_ok(rc, error);
  return LC_OK;
}

static int pouch_subscribe_wait_handle(void *context, lc_message *message,
                                       lc_error *error) {
  pouch_subscribe_wait_test *test;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  size_t written;
  int rc;

  test = (pouch_subscribe_wait_test *)context;
  if (test == NULL || message == NULL) {
    return pouch_test_error(error, "subscribe wait test missing message");
  }
  if (test->handled != 0U) {
    return pouch_test_error(error,
                            "subscribe wait test delivered more than once");
  }

  sink = NULL;
  rc = lc_sink_to_memory(&sink, error);
  if (rc != LC_OK) {
    return rc;
  }
  written = 0U;
  rc = message->write_payload(message, sink, &written, error);
  if (rc == LC_OK) {
    bytes = NULL;
    length = 0U;
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  }
  if (rc == LC_OK && written == strlen("waited-work") &&
      length == strlen("waited-work") &&
      memcmp(bytes, "waited-work", length) == 0) {
    test->payload_ok = 1;
  } else if (rc == LC_OK) {
    rc = pouch_test_error(error,
                          "subscribe wait test saw unexpected payload");
  }
  lc_sink_close(sink);
  if (rc != LC_OK) {
    return rc;
  }

  snprintf(test->message_id, sizeof(test->message_id), "%s",
           message->message_id);
  test->handled += 1U;
  return message->ack(message, error);
}

static void *pouch_subscribe_wait_main(void *context) {
  pouch_subscribe_wait_test *test;
  lc_client_config config;
  const char *endpoints[1];
  lc_client *subscriber;
  lc_dequeue_req subscribe_req;
  lc_consumer consumer;

  test = (pouch_subscribe_wait_test *)context;
  subscriber = NULL;
  lc_error_init(&test->error);

  endpoints[0] = test->endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "default";
  test->rc = lc_client_open(&config, &subscriber, &test->error);
  if (test->rc != LC_OK) {
    return NULL;
  }

  lc_dequeue_req_init(&subscribe_req);
  subscribe_req.queue = "subscribe-wait";
  subscribe_req.owner = "wait-subscriber";
  subscribe_req.visibility_timeout_seconds = 30L;
  subscribe_req.wait_seconds = 3L;
  subscribe_req.page_size = 1;
  lc_consumer_init(&consumer);
  consumer.handle = pouch_subscribe_wait_handle;
  consumer.context = test;
  test->rc = subscriber->subscribe(subscriber, &subscribe_req, &consumer,
                                   &test->error);
  subscriber->close(subscriber);
  return NULL;
}

static void *pouch_delayed_enqueue_main(void *context) {
  pouch_delayed_enqueue_test *test;
  lc_client *producer;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_source *source;

  test = (pouch_delayed_enqueue_test *)context;
  producer = NULL;
  source = NULL;
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_error_init(&test->error);
  test->rc = LC_ERR_TRANSPORT;
  usleep(200000U);

  open_pouch_client(test->endpoint, &producer, &test->error);
  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = test->queue;
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 3600L;
  enqueue_req.max_attempts = 3;
  source = source_from_text(test->payload, &test->error);
  test->rc = producer->enqueue(producer, &enqueue_req, source, &enqueue_res,
                               &test->error);
  lc_source_close(source);
  lc_enqueue_res_cleanup(&enqueue_res);
  producer->close(producer);
  return NULL;
}

static int pouch_watch_handle(void *context, const lc_watch_event *event,
                              lc_error *error) {
  pouch_watch_state *state;

  (void)error;
  state = (pouch_watch_state *)context;
  assert_non_null(state);
  assert_non_null(event);
  state->handled += 1U;
  state->available = event->available;
  if (event->queue != NULL) {
    snprintf(state->queue, sizeof(state->queue), "%s", event->queue);
  }
  if (event->head_message_id != NULL) {
    snprintf(state->head_message_id, sizeof(state->head_message_id), "%s",
             event->head_message_id);
  } else {
    state->head_message_id[0] = '\0';
  }
  if (event->correlation_id != NULL) {
    snprintf(state->correlation_id, sizeof(state->correlation_id), "%s",
             event->correlation_id);
  }
  assert_true(event->changed_at_unix > 0L);
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

static void message_ref_from_message(lc_message *message, lc_message_ref *ref) {
  lc_message_ref_init(ref);
  ref->namespace_name = message->namespace_name;
  ref->queue = message->queue;
  ref->message_id = message->message_id;
  ref->lease_id = message->lease_id;
  ref->txn_id = message->txn_id;
  ref->fencing_token = message->fencing_token;
  ref->meta_etag = message->meta_etag;
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

static void test_pouch_public_metadata_query_hidden_persists(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_client *reader;
  lc_lease *lease;
  lc_source *source;
  lc_sink *sink;
  lc_acquire_req acquire;
  lc_update_opts update_opts;
  lc_metadata_req metadata_req;
  lc_describe_req describe_req;
  lc_describe_res describe_res;
  lc_get_res get_res;
  lc_release_req release_req;
  lc_error error;
  char *state_etag;
  long state_version;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "metadata-query-hidden");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  client = NULL;
  reader = NULL;
  lease = NULL;
  source = NULL;
  sink = NULL;
  state_etag = NULL;
  memset(&describe_res, 0, sizeof(describe_res));
  memset(&get_res, 0, sizeof(get_res));

  open_pouch_client(endpoint, &client, &error);
  lc_acquire_req_init(&acquire);
  acquire.key = "integration/metadata-query-hidden";
  acquire.owner = "writer";
  acquire.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  lc_update_opts_init(&update_opts);
  update_opts.content_type = "application/json";
  source = source_from_text("{\"indexed\":true}", &error);
  rc = lease->update(lease, source, &update_opts, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);
  state_version = lease->version;
  state_etag = strdup(lease->state_etag);
  assert_non_null(state_etag);

  lc_metadata_req_init(&metadata_req);
  metadata_req.has_query_hidden = 1;
  metadata_req.query_hidden = 1;
  metadata_req.has_if_version = 1;
  metadata_req.if_version = state_version - 1L;
  rc = lease->metadata(lease, &metadata_req, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 412L);
  assert_string_equal(error.server_code, "precondition_failed");
  assert_int_equal(lease->version, state_version);
  assert_false(lease->has_query_hidden);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  metadata_req.if_version = state_version;
  rc = lease->metadata(lease, &metadata_req, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(lease->version, state_version + 1L);
  assert_string_equal(lease->state_etag, state_etag);
  assert_true(lease->has_query_hidden);
  assert_true(lease->query_hidden);

  lc_describe_req_init(&describe_req);
  describe_req.key = acquire.key;
  rc = client->describe(client, &describe_req, &describe_res, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(describe_res.version, lease->version);
  assert_string_equal(describe_res.state_etag, state_etag);
  assert_true(describe_res.has_query_hidden);
  assert_true(describe_res.query_hidden);
  lc_describe_res_cleanup(&describe_res);

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = lease->get(lease, sink, NULL, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_false(get_res.no_content);
  assert_int_equal(get_res.version, lease->version);
  assert_string_equal(get_res.etag, state_etag);
  assert_sink_text(sink, "{\"indexed\":true}", &error);
  lc_sink_close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);

  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;
  client->close(client);
  client = NULL;

  open_pouch_client(endpoint, &reader, &error);
  acquire.owner = "reader";
  rc = reader->acquire(reader, &acquire, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);
  assert_int_equal(lease->version, state_version + 1L);
  assert_string_equal(lease->state_etag, state_etag);
  assert_true(lease->has_query_hidden);
  assert_true(lease->query_hidden);

  lc_metadata_req_init(&metadata_req);
  metadata_req.has_query_hidden = 1;
  metadata_req.query_hidden = 0;
  rc = lease->metadata(lease, &metadata_req, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(lease->version, state_version + 2L);
  assert_string_equal(lease->state_etag, state_etag);
  assert_true(lease->has_query_hidden);
  assert_false(lease->query_hidden);

  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;
  reader->close(reader);
  reader = NULL;

  free(state_etag);
  lc_describe_res_cleanup(&describe_res);
  lc_get_res_cleanup(&get_res);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_scan_query_documents_replays_after_reopen(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *writer;
  lc_client *reader;
  lc_lease *alpha;
  lc_lease *bravo;
  lc_source *source;
  lc_sink *sink;
  lc_acquire_req acquire;
  lc_update_opts update_opts;
  lc_release_req release_req;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "scan-query-reopen");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  writer = NULL;
  reader = NULL;
  alpha = NULL;
  bravo = NULL;
  source = NULL;
  sink = NULL;
  text = NULL;
  memset(&query_res, 0, sizeof(query_res));

  open_pouch_client(endpoint, &writer, &error);
  lc_acquire_req_init(&acquire);
  acquire.owner = "scan-writer";
  acquire.ttl_seconds = 60L;
  lc_update_opts_init(&update_opts);
  update_opts.content_type = "application/json";

  acquire.key = "integration/query/alpha";
  rc = writer->acquire(writer, &acquire, &alpha, &error);
  assert_lc_ok(rc, &error);
  source = source_from_text("{\"kind\":\"scan\",\"ordinal\":1}", &error);
  rc = alpha->update(alpha, source, &update_opts, &error);
  lc_source_close(source);
  source = NULL;
  assert_lc_ok(rc, &error);

  acquire.key = "integration/query/bravo";
  rc = writer->acquire(writer, &acquire, &bravo, &error);
  assert_lc_ok(rc, &error);
  source = source_from_text("{\"kind\":\"scan\",\"ordinal\":2}", &error);
  rc = bravo->update(bravo, source, &update_opts, &error);
  lc_source_close(source);
  source = NULL;
  assert_lc_ok(rc, &error);

  lc_release_req_init(&release_req);
  rc = alpha->release(alpha, &release_req, &error);
  assert_lc_ok(rc, &error);
  alpha = NULL;
  rc = bravo->release(bravo, &release_req, &error);
  assert_lc_ok(rc, &error);
  bravo = NULL;
  writer->close(writer);
  writer = NULL;

  open_pouch_scan_client(endpoint, &reader, &error);
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  lc_query_req_init(&query_req);
  query_req.selector_json = "{}";
  query_req.limit = 1L;
  rc = reader->query(reader, &query_req, sink, &query_res, &error);
  assert_lc_ok(rc, &error);
  text = sink_text(sink, &error);
  assert_non_null(strstr(text, "\"key\":\"integration/query/alpha\""));
  assert_non_null(strstr(text, "\"document\":{\"kind\":\"scan\",\"ordinal\":1}"));
  assert_null(strstr(text, "integration/query/bravo"));
  assert_string_equal(query_res.cursor, "integration/query/alpha");
  assert_string_equal(query_res.return_mode, "documents");
  assert_string_equal(query_res.metadata_json, "{\"query_candidates\":1}");
  free(text);
  text = NULL;
  lc_sink_close(sink);
  sink = NULL;
  lc_query_res_cleanup(&query_res);

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  query_req.cursor = "integration/query/alpha";
  rc = reader->query(reader, &query_req, sink, &query_res, &error);
  assert_lc_ok(rc, &error);
  text = sink_text(sink, &error);
  assert_non_null(strstr(text, "\"key\":\"integration/query/bravo\""));
  assert_non_null(strstr(text, "\"document\":{\"kind\":\"scan\",\"ordinal\":2}"));
  assert_null(strstr(text, "integration/query/alpha"));
  assert_null(query_res.cursor);
  assert_string_equal(query_res.return_mode, "documents");
  free(text);
  text = NULL;

  lc_query_res_cleanup(&query_res);
  lc_sink_close(sink);
  reader->close(reader);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_scan_query_can_be_configured_by_endpoint(
    void **state) {
  char root[256];
  char writer_endpoint[320];
  char scan_endpoint[384];
  lc_client *writer;
  lc_client *reader;
  lc_lease *lease;
  lc_source *source;
  lc_sink *sink;
  lc_acquire_req acquire;
  lc_update_opts update_opts;
  lc_release_req release_req;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "scan-query-endpoint");
  pouch_endpoint(writer_endpoint, sizeof(writer_endpoint), root);
  pouch_endpoint_with_query(scan_endpoint, sizeof(scan_endpoint), root,
                            "query_engine=scan");
  cleanup_pouch_root(root);
  lc_error_init(&error);
  writer = NULL;
  reader = NULL;
  lease = NULL;
  source = NULL;
  sink = NULL;
  text = NULL;
  memset(&query_res, 0, sizeof(query_res));

  open_pouch_client(writer_endpoint, &writer, &error);
  lc_acquire_req_init(&acquire);
  acquire.owner = "scan-endpoint-writer";
  acquire.ttl_seconds = 60L;
  acquire.key = "integration/query-endpoint/alpha";
  rc = writer->acquire(writer, &acquire, &lease, &error);
  assert_lc_ok(rc, &error);

  lc_update_opts_init(&update_opts);
  update_opts.content_type = "application/json";
  source = source_from_text("{\"kind\":\"scan-endpoint\"}", &error);
  rc = lease->update(lease, source, &update_opts, &error);
  lc_source_close(source);
  source = NULL;
  assert_lc_ok(rc, &error);

  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;
  writer->close(writer);
  writer = NULL;

  open_pouch_client(scan_endpoint, &reader, &error);
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  lc_query_req_init(&query_req);
  query_req.selector_json = "{}";
  rc = reader->query(reader, &query_req, sink, &query_res, &error);
  assert_lc_ok(rc, &error);

  text = sink_text(sink, &error);
  assert_non_null(strstr(text, "\"key\":\"integration/query-endpoint/alpha\""));
  assert_non_null(strstr(text, "\"document\":{\"kind\":\"scan-endpoint\"}"));
  assert_string_equal(query_res.return_mode, "documents");
  assert_int_equal(query_res.index_seq, 0UL);
  assert_string_equal(query_res.metadata_json, "{\"query_candidates\":1}");

  free(text);
  lc_query_res_cleanup(&query_res);
  lc_sink_close(sink);
  reader->close(reader);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void
test_pouch_public_scan_query_endpoint_uses_index_fallback_for_refresh(
    void **state) {
  char root[256];
  char writer_endpoint[320];
  char scan_endpoint[384];
  lc_client *writer;
  lc_client *reader;
  lc_lease *lease;
  lc_source *source;
  lc_sink *sink;
  lc_acquire_req acquire;
  lc_update_opts update_opts;
  lc_release_req release_req;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "scan-query-endpoint-fallback");
  pouch_endpoint(writer_endpoint, sizeof(writer_endpoint), root);
  pouch_endpoint_with_query(scan_endpoint, sizeof(scan_endpoint), root,
                            "query_engine=scan&query_fallback_engine=index");
  cleanup_pouch_root(root);
  lc_error_init(&error);
  writer = NULL;
  reader = NULL;
  lease = NULL;
  source = NULL;
  sink = NULL;
  text = NULL;
  memset(&query_res, 0, sizeof(query_res));

  open_pouch_client(writer_endpoint, &writer, &error);
  lc_acquire_req_init(&acquire);
  acquire.owner = "scan-endpoint-fallback-writer";
  acquire.ttl_seconds = 60L;
  acquire.key = "integration/query-endpoint/fallback";
  rc = writer->acquire(writer, &acquire, &lease, &error);
  assert_lc_ok(rc, &error);

  lc_update_opts_init(&update_opts);
  update_opts.content_type = "application/json";
  source = source_from_text("{\"kind\":\"scan-endpoint-fallback\"}", &error);
  rc = lease->update(lease, source, &update_opts, &error);
  lc_source_close(source);
  source = NULL;
  assert_lc_ok(rc, &error);

  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;
  writer->close(writer);
  writer = NULL;

  open_pouch_client(scan_endpoint, &reader, &error);
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  lc_query_req_init(&query_req);
  query_req.selector_json = "{}";
  query_req.refresh = "wait_for";
  rc = reader->query(reader, &query_req, sink, &query_res, &error);
  assert_lc_ok(rc, &error);

  text = sink_text(sink, &error);
  assert_non_null(strstr(text,
                         "\"key\":\"integration/query-endpoint/fallback\""));
  assert_non_null(
      strstr(text, "\"document\":{\"kind\":\"scan-endpoint-fallback\"}"));
  assert_string_equal(query_res.return_mode, "documents");
  assert_true(query_res.index_seq > 0UL);
  assert_string_equal(query_res.metadata_json, "{\"query_candidates\":1}");

  free(text);
  lc_query_res_cleanup(&query_res);
  lc_sink_close(sink);
  reader->close(reader);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_scan_query_keys_can_be_configured_by_endpoint(
    void **state) {
  char root[256];
  char writer_endpoint[320];
  char scan_endpoint[384];
  lc_client *writer;
  lc_client *reader;
  lc_lease *alpha;
  lc_lease *bravo;
  lc_acquire_req acquire;
  lc_release_req release_req;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_query_key_handler handler;
  query_key_capture capture;
  lc_error error;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "scan-query-keys-endpoint");
  pouch_endpoint(writer_endpoint, sizeof(writer_endpoint), root);
  pouch_endpoint_with_query(scan_endpoint, sizeof(scan_endpoint), root,
                            "query_engine=scan");
  cleanup_pouch_root(root);
  lc_error_init(&error);
  writer = NULL;
  reader = NULL;
  alpha = NULL;
  bravo = NULL;
  memset(&query_res, 0, sizeof(query_res));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));

  open_pouch_client(writer_endpoint, &writer, &error);
  lc_acquire_req_init(&acquire);
  acquire.owner = "scan-key-endpoint-writer";
  acquire.ttl_seconds = 60L;
  acquire.key = "integration/query-keys-endpoint/bravo";
  rc = writer->acquire(writer, &acquire, &bravo, &error);
  assert_lc_ok(rc, &error);
  acquire.key = "integration/query-keys-endpoint/alpha";
  rc = writer->acquire(writer, &acquire, &alpha, &error);
  assert_lc_ok(rc, &error);

  lc_release_req_init(&release_req);
  rc = alpha->release(alpha, &release_req, &error);
  assert_lc_ok(rc, &error);
  alpha = NULL;
  rc = bravo->release(bravo, &release_req, &error);
  assert_lc_ok(rc, &error);
  bravo = NULL;
  writer->close(writer);
  writer = NULL;

  open_pouch_client(scan_endpoint, &reader, &error);
  handler.begin = query_key_capture_begin;
  handler.chunk = query_key_capture_chunk;
  handler.end = query_key_capture_end;
  lc_query_req_init(&query_req);
  query_req.selector_json = "{}";
  query_req.limit = 2L;
  rc = reader->query_keys(reader, &query_req, &handler, &capture, &query_res,
                          &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(capture.key_count, 2U);
  assert_string_equal(capture.keys[0], "integration/query-keys-endpoint/alpha");
  assert_string_equal(capture.keys[1], "integration/query-keys-endpoint/bravo");
  assert_string_equal(query_res.return_mode, "keys");
  assert_null(query_res.cursor);
  assert_int_equal(query_res.index_seq, 0UL);

  lc_query_res_cleanup(&query_res);
  reader->close(reader);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_scan_query_documents_refreshes_open_reader(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *writer;
  lc_client *reader;
  lc_lease *alpha;
  lc_lease *bravo;
  lc_lease *hidden;
  lc_source *source;
  lc_sink *sink;
  lc_acquire_req acquire;
  lc_update_opts update_opts;
  lc_release_req release_req;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_metadata_req metadata_req;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "scan-query-open-reader");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  writer = NULL;
  reader = NULL;
  alpha = NULL;
  bravo = NULL;
  source = NULL;
  sink = NULL;
  text = NULL;
  memset(&query_res, 0, sizeof(query_res));

  open_pouch_scan_client(endpoint, &reader, &error);
  open_pouch_client(endpoint, &writer, &error);

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  lc_query_req_init(&query_req);
  query_req.selector_json = "{}";
  rc = reader->query(reader, &query_req, sink, &query_res, &error);
  assert_lc_ok(rc, &error);
  text = sink_text(sink, &error);
  assert_string_equal(text, "");
  assert_null(query_res.cursor);
  assert_string_equal(query_res.return_mode, "documents");
  assert_string_equal(query_res.metadata_json, "{\"query_candidates\":0}");
  free(text);
  text = NULL;
  lc_sink_close(sink);
  sink = NULL;
  lc_query_res_cleanup(&query_res);

  lc_acquire_req_init(&acquire);
  acquire.owner = "scan-open-writer";
  acquire.ttl_seconds = 60L;
  lc_update_opts_init(&update_opts);
  update_opts.content_type = "application/json";

  acquire.key = "integration/query-open/alpha";
  rc = writer->acquire(writer, &acquire, &alpha, &error);
  assert_lc_ok(rc, &error);
  source = source_from_text("{\"kind\":\"scan-open\",\"ordinal\":1}", &error);
  rc = alpha->update(alpha, source, &update_opts, &error);
  lc_source_close(source);
  source = NULL;
  assert_lc_ok(rc, &error);

  acquire.key = "integration/query-open/bravo";
  rc = writer->acquire(writer, &acquire, &bravo, &error);
  assert_lc_ok(rc, &error);
  source = source_from_text("{\"kind\":\"scan-open\",\"ordinal\":2}", &error);
  rc = bravo->update(bravo, source, &update_opts, &error);
  lc_source_close(source);
  source = NULL;
  assert_lc_ok(rc, &error);

  acquire.key = "integration/query-open/hidden";
  rc = writer->acquire(writer, &acquire, &hidden, &error);
  assert_lc_ok(rc, &error);
  source = source_from_text("{\"kind\":\"scan-open\",\"hidden\":true}",
                            &error);
  rc = hidden->update(hidden, source, &update_opts, &error);
  lc_source_close(source);
  source = NULL;
  assert_lc_ok(rc, &error);
  lc_metadata_req_init(&metadata_req);
  metadata_req.has_query_hidden = 1;
  metadata_req.query_hidden = 1;
  rc = hidden->metadata(hidden, &metadata_req, &error);
  assert_lc_ok(rc, &error);

  lc_release_req_init(&release_req);
  rc = alpha->release(alpha, &release_req, &error);
  assert_lc_ok(rc, &error);
  alpha = NULL;
  rc = bravo->release(bravo, &release_req, &error);
  assert_lc_ok(rc, &error);
  bravo = NULL;
  rc = hidden->release(hidden, &release_req, &error);
  assert_lc_ok(rc, &error);
  hidden = NULL;

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  lc_query_req_init(&query_req);
  query_req.selector_json = "{}";
  query_req.limit = 2L;
  rc = reader->query(reader, &query_req, sink, &query_res, &error);
  assert_lc_ok(rc, &error);
  text = sink_text(sink, &error);
  assert_non_null(strstr(text, "\"key\":\"integration/query-open/alpha\""));
  assert_non_null(
      strstr(text, "\"document\":{\"kind\":\"scan-open\",\"ordinal\":1}"));
  assert_non_null(strstr(text, "\"key\":\"integration/query-open/bravo\""));
  assert_non_null(
      strstr(text, "\"document\":{\"kind\":\"scan-open\",\"ordinal\":2}"));
  assert_null(strstr(text, "integration/query-open/hidden"));
  assert_null(strstr(text, "\"hidden\":true"));
  assert_null(query_res.cursor);
  assert_string_equal(query_res.return_mode, "documents");
  assert_string_equal(query_res.metadata_json, "{\"query_candidates\":2}");
  assert_int_equal(query_res.index_seq, 0UL);

  free(text);
  lc_query_res_cleanup(&query_res);
  lc_sink_close(sink);
  writer->close(writer);
  reader->close(reader);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_scan_query_keys_refreshes_open_reader(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *writer;
  lc_client *reader;
  lc_lease *alpha;
  lc_lease *bravo;
  lc_lease *hidden;
  lc_acquire_req acquire;
  lc_release_req release_req;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_query_key_handler handler;
  query_key_capture capture;
  lc_metadata_req metadata_req;
  lc_error error;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "scan-query-keys-open-reader");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  writer = NULL;
  reader = NULL;
  alpha = NULL;
  bravo = NULL;
  hidden = NULL;
  memset(&query_res, 0, sizeof(query_res));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));

  open_pouch_scan_client(endpoint, &reader, &error);
  open_pouch_client(endpoint, &writer, &error);

  handler.begin = query_key_capture_begin;
  handler.chunk = query_key_capture_chunk;
  handler.end = query_key_capture_end;
  lc_query_req_init(&query_req);
  query_req.selector_json = "{}";
  rc = reader->query_keys(reader, &query_req, &handler, &capture, &query_res,
                          &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(capture.key_count, 0U);
  assert_int_equal(capture.begin_calls, 0U);
  assert_int_equal(capture.chunk_calls, 0U);
  assert_int_equal(capture.end_calls, 0U);
  assert_null(query_res.cursor);
  assert_string_equal(query_res.return_mode, "keys");
  assert_int_equal(query_res.index_seq, 0UL);
  lc_query_res_cleanup(&query_res);

  lc_acquire_req_init(&acquire);
  acquire.owner = "scan-key-open-writer";
  acquire.ttl_seconds = 60L;

  acquire.key = "integration/scan-key-open/bravo";
  rc = writer->acquire(writer, &acquire, &bravo, &error);
  assert_lc_ok(rc, &error);
  acquire.key = "integration/scan-key-open/hidden";
  rc = writer->acquire(writer, &acquire, &hidden, &error);
  assert_lc_ok(rc, &error);
  lc_metadata_req_init(&metadata_req);
  metadata_req.has_query_hidden = 1;
  metadata_req.query_hidden = 1;
  rc = hidden->metadata(hidden, &metadata_req, &error);
  assert_lc_ok(rc, &error);
  acquire.key = "integration/scan-key-open/alpha";
  rc = writer->acquire(writer, &acquire, &alpha, &error);
  assert_lc_ok(rc, &error);

  lc_release_req_init(&release_req);
  rc = alpha->release(alpha, &release_req, &error);
  assert_lc_ok(rc, &error);
  alpha = NULL;
  rc = bravo->release(bravo, &release_req, &error);
  assert_lc_ok(rc, &error);
  bravo = NULL;
  rc = hidden->release(hidden, &release_req, &error);
  assert_lc_ok(rc, &error);
  hidden = NULL;

  memset(&capture, 0, sizeof(capture));
  lc_query_req_init(&query_req);
  query_req.selector_json = "{}";
  query_req.limit = 2L;
  rc = reader->query_keys(reader, &query_req, &handler, &capture, &query_res,
                          &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(capture.key_count, 2U);
  assert_string_equal(capture.keys[0], "integration/scan-key-open/alpha");
  assert_string_equal(capture.keys[1], "integration/scan-key-open/bravo");
  assert_int_equal(capture.begin_calls, 2U);
  assert_int_equal(capture.end_calls, 2U);
  assert_true(capture.chunk_calls >= 2U);
  assert_null(query_res.cursor);
  assert_string_equal(query_res.return_mode, "keys");
  assert_int_equal(query_res.index_seq, 0UL);

  lc_query_res_cleanup(&query_res);
  writer->close(writer);
  reader->close(reader);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_index_query_documents_replays_after_reopen(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *writer;
  lc_client *reader;
  lc_lease *alpha;
  lc_lease *bravo;
  lc_source *source;
  lc_sink *sink;
  lc_acquire_req acquire;
  lc_update_opts update_opts;
  lc_release_req release_req;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "index-query-reopen");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  writer = NULL;
  reader = NULL;
  alpha = NULL;
  bravo = NULL;
  source = NULL;
  sink = NULL;
  text = NULL;
  memset(&query_res, 0, sizeof(query_res));

  open_pouch_client(endpoint, &writer, &error);
  lc_acquire_req_init(&acquire);
  acquire.owner = "index-writer";
  acquire.ttl_seconds = 60L;
  lc_update_opts_init(&update_opts);
  update_opts.content_type = "application/json";

  acquire.key = "integration/index-query/alpha";
  rc = writer->acquire(writer, &acquire, &alpha, &error);
  assert_lc_ok(rc, &error);
  source = source_from_text("{\"kind\":\"index\",\"ordinal\":1}", &error);
  rc = alpha->update(alpha, source, &update_opts, &error);
  lc_source_close(source);
  source = NULL;
  assert_lc_ok(rc, &error);

  acquire.key = "integration/index-query/bravo";
  rc = writer->acquire(writer, &acquire, &bravo, &error);
  assert_lc_ok(rc, &error);
  source = source_from_text("{\"kind\":\"index\",\"ordinal\":2}", &error);
  rc = bravo->update(bravo, source, &update_opts, &error);
  lc_source_close(source);
  source = NULL;
  assert_lc_ok(rc, &error);

  lc_release_req_init(&release_req);
  rc = alpha->release(alpha, &release_req, &error);
  assert_lc_ok(rc, &error);
  alpha = NULL;
  rc = bravo->release(bravo, &release_req, &error);
  assert_lc_ok(rc, &error);
  bravo = NULL;
  writer->close(writer);
  writer = NULL;

  open_pouch_client(endpoint, &reader, &error);
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  lc_query_req_init(&query_req);
  query_req.selector_json = "{}";
  query_req.limit = 2L;
  rc = reader->query(reader, &query_req, sink, &query_res, &error);
  assert_lc_ok(rc, &error);
  text = sink_text(sink, &error);
  assert_non_null(strstr(text, "\"key\":\"integration/index-query/alpha\""));
  assert_non_null(strstr(text, "\"document\":{\"kind\":\"index\",\"ordinal\":1}"));
  assert_non_null(strstr(text, "\"key\":\"integration/index-query/bravo\""));
  assert_non_null(strstr(text, "\"document\":{\"kind\":\"index\",\"ordinal\":2}"));
  assert_null(query_res.cursor);
  assert_string_equal(query_res.return_mode, "documents");
  assert_string_equal(query_res.metadata_json, "{\"query_candidates\":2}");
  assert_true(query_res.index_seq > 0UL);

  free(text);
  lc_query_res_cleanup(&query_res);
  lc_sink_close(sink);
  reader->close(reader);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_index_query_documents_refreshes_open_reader(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *writer;
  lc_client *reader;
  lc_lease *alpha;
  lc_lease *bravo;
  lc_lease *hidden;
  lc_source *source;
  lc_sink *sink;
  lc_acquire_req acquire;
  lc_update_opts update_opts;
  lc_release_req release_req;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_metadata_req metadata_req;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "index-query-open-reader");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  writer = NULL;
  reader = NULL;
  alpha = NULL;
  bravo = NULL;
  hidden = NULL;
  source = NULL;
  sink = NULL;
  text = NULL;
  memset(&query_res, 0, sizeof(query_res));

  open_pouch_client(endpoint, &reader, &error);
  open_pouch_client(endpoint, &writer, &error);

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  lc_query_req_init(&query_req);
  query_req.selector_json = "{}";
  rc = reader->query(reader, &query_req, sink, &query_res, &error);
  assert_lc_ok(rc, &error);
  text = sink_text(sink, &error);
  assert_string_equal(text, "");
  assert_null(query_res.cursor);
  assert_string_equal(query_res.return_mode, "documents");
  assert_string_equal(query_res.metadata_json, "{\"query_candidates\":0}");
  free(text);
  text = NULL;
  lc_sink_close(sink);
  sink = NULL;
  lc_query_res_cleanup(&query_res);

  lc_acquire_req_init(&acquire);
  acquire.owner = "index-open-writer";
  acquire.ttl_seconds = 60L;
  lc_update_opts_init(&update_opts);
  update_opts.content_type = "application/json";

  acquire.key = "integration/index-open/alpha";
  rc = writer->acquire(writer, &acquire, &alpha, &error);
  assert_lc_ok(rc, &error);
  source = source_from_text("{\"kind\":\"index-open\",\"ordinal\":1}", &error);
  rc = alpha->update(alpha, source, &update_opts, &error);
  lc_source_close(source);
  source = NULL;
  assert_lc_ok(rc, &error);

  acquire.key = "integration/index-open/bravo";
  rc = writer->acquire(writer, &acquire, &bravo, &error);
  assert_lc_ok(rc, &error);
  source = source_from_text("{\"kind\":\"index-open\",\"ordinal\":2}", &error);
  rc = bravo->update(bravo, source, &update_opts, &error);
  lc_source_close(source);
  source = NULL;
  assert_lc_ok(rc, &error);

  acquire.key = "integration/index-open/hidden";
  rc = writer->acquire(writer, &acquire, &hidden, &error);
  assert_lc_ok(rc, &error);
  source = source_from_text("{\"kind\":\"index-open\",\"hidden\":true}",
                            &error);
  rc = hidden->update(hidden, source, &update_opts, &error);
  lc_source_close(source);
  source = NULL;
  assert_lc_ok(rc, &error);
  lc_metadata_req_init(&metadata_req);
  metadata_req.has_query_hidden = 1;
  metadata_req.query_hidden = 1;
  rc = hidden->metadata(hidden, &metadata_req, &error);
  assert_lc_ok(rc, &error);

  lc_release_req_init(&release_req);
  rc = alpha->release(alpha, &release_req, &error);
  assert_lc_ok(rc, &error);
  alpha = NULL;
  rc = bravo->release(bravo, &release_req, &error);
  assert_lc_ok(rc, &error);
  bravo = NULL;
  rc = hidden->release(hidden, &release_req, &error);
  assert_lc_ok(rc, &error);
  hidden = NULL;

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  lc_query_req_init(&query_req);
  query_req.selector_json = "{}";
  query_req.limit = 2L;
  rc = reader->query(reader, &query_req, sink, &query_res, &error);
  assert_lc_ok(rc, &error);
  text = sink_text(sink, &error);
  assert_non_null(strstr(text, "\"key\":\"integration/index-open/alpha\""));
  assert_non_null(
      strstr(text, "\"document\":{\"kind\":\"index-open\",\"ordinal\":1}"));
  assert_non_null(strstr(text, "\"key\":\"integration/index-open/bravo\""));
  assert_non_null(
      strstr(text, "\"document\":{\"kind\":\"index-open\",\"ordinal\":2}"));
  assert_null(strstr(text, "integration/index-open/hidden"));
  assert_null(strstr(text, "\"hidden\":true"));
  assert_null(query_res.cursor);
  assert_string_equal(query_res.return_mode, "documents");
  assert_string_equal(query_res.metadata_json, "{\"query_candidates\":2}");
  assert_true(query_res.index_seq > 0UL);

  free(text);
  lc_query_res_cleanup(&query_res);
  lc_sink_close(sink);
  writer->close(writer);
  reader->close(reader);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_index_query_keys_refreshes_open_reader(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *writer;
  lc_client *reader;
  lc_lease *alpha;
  lc_lease *bravo;
  lc_lease *hidden;
  lc_acquire_req acquire;
  lc_release_req release_req;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_query_key_handler handler;
  query_key_capture capture;
  lc_metadata_req metadata_req;
  lc_error error;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "index-query-keys-open-reader");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  writer = NULL;
  reader = NULL;
  alpha = NULL;
  bravo = NULL;
  hidden = NULL;
  memset(&query_res, 0, sizeof(query_res));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));

  open_pouch_client(endpoint, &reader, &error);
  open_pouch_client(endpoint, &writer, &error);

  handler.begin = query_key_capture_begin;
  handler.chunk = query_key_capture_chunk;
  handler.end = query_key_capture_end;
  lc_query_req_init(&query_req);
  query_req.selector_json = "{}";
  rc = reader->query_keys(reader, &query_req, &handler, &capture, &query_res,
                          &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(capture.key_count, 0U);
  assert_int_equal(capture.begin_calls, 0U);
  assert_int_equal(capture.chunk_calls, 0U);
  assert_int_equal(capture.end_calls, 0U);
  assert_null(query_res.cursor);
  assert_string_equal(query_res.return_mode, "keys");
  lc_query_res_cleanup(&query_res);

  lc_acquire_req_init(&acquire);
  acquire.owner = "index-key-open-writer";
  acquire.ttl_seconds = 60L;

  acquire.key = "integration/index-key-open/bravo";
  rc = writer->acquire(writer, &acquire, &bravo, &error);
  assert_lc_ok(rc, &error);
  acquire.key = "integration/index-key-open/hidden";
  rc = writer->acquire(writer, &acquire, &hidden, &error);
  assert_lc_ok(rc, &error);
  lc_metadata_req_init(&metadata_req);
  metadata_req.has_query_hidden = 1;
  metadata_req.query_hidden = 1;
  rc = hidden->metadata(hidden, &metadata_req, &error);
  assert_lc_ok(rc, &error);
  acquire.key = "integration/index-key-open/alpha";
  rc = writer->acquire(writer, &acquire, &alpha, &error);
  assert_lc_ok(rc, &error);

  lc_release_req_init(&release_req);
  rc = alpha->release(alpha, &release_req, &error);
  assert_lc_ok(rc, &error);
  alpha = NULL;
  rc = bravo->release(bravo, &release_req, &error);
  assert_lc_ok(rc, &error);
  bravo = NULL;
  rc = hidden->release(hidden, &release_req, &error);
  assert_lc_ok(rc, &error);
  hidden = NULL;

  memset(&capture, 0, sizeof(capture));
  lc_query_req_init(&query_req);
  query_req.selector_json = "{}";
  query_req.limit = 2L;
  rc = reader->query_keys(reader, &query_req, &handler, &capture, &query_res,
                          &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(capture.key_count, 2U);
  assert_string_equal(capture.keys[0], "integration/index-key-open/alpha");
  assert_string_equal(capture.keys[1], "integration/index-key-open/bravo");
  assert_int_equal(capture.begin_calls, 2U);
  assert_int_equal(capture.end_calls, 2U);
  assert_true(capture.chunk_calls >= 2U);
  assert_null(query_res.cursor);
  assert_string_equal(query_res.return_mode, "keys");
  assert_true(query_res.index_seq > 0UL);

  lc_query_res_cleanup(&query_res);
  writer->close(writer);
  reader->close(reader);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_flush_index_refreshes_open_reader(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *writer;
  lc_client *reader;
  lc_lease *alpha;
  lc_lease *bravo;
  lc_source *source;
  lc_acquire_req acquire;
  lc_update_opts update_opts;
  lc_release_req release_req;
  lc_index_flush_req flush_req;
  lc_index_flush_res before_flush;
  lc_index_flush_res after_first_flush;
  lc_index_flush_res after_second_flush;
  lc_error error;
  unsigned long before_seq;
  unsigned long first_seq;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "flush-index-open-reader");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  writer = NULL;
  reader = NULL;
  alpha = NULL;
  bravo = NULL;
  source = NULL;
  memset(&before_flush, 0, sizeof(before_flush));
  memset(&after_first_flush, 0, sizeof(after_first_flush));
  memset(&after_second_flush, 0, sizeof(after_second_flush));

  open_pouch_client(endpoint, &reader, &error);
  open_pouch_client(endpoint, &writer, &error);

  lc_index_flush_req_init(&flush_req);
  flush_req.namespace_name = "default";
  rc = reader->flush_index(reader, &flush_req, &before_flush, &error);
  assert_lc_ok(rc, &error);
  assert_string_equal(before_flush.namespace_name, "default");
  assert_string_equal(before_flush.mode, "wait");
  assert_string_equal(before_flush.flush_id, "local");
  assert_true(before_flush.accepted);
  assert_true(before_flush.flushed);
  assert_false(before_flush.pending);
  before_seq = before_flush.index_seq;

  lc_acquire_req_init(&acquire);
  acquire.owner = "flush-index-writer";
  acquire.ttl_seconds = 60L;
  lc_update_opts_init(&update_opts);
  update_opts.content_type = "application/json";

  acquire.key = "integration/flush-index/alpha";
  rc = writer->acquire(writer, &acquire, &alpha, &error);
  assert_lc_ok(rc, &error);
  source = source_from_text("{\"flush\":1}", &error);
  rc = alpha->update(alpha, source, &update_opts, &error);
  lc_source_close(source);
  source = NULL;
  assert_lc_ok(rc, &error);
  lc_release_req_init(&release_req);
  rc = alpha->release(alpha, &release_req, &error);
  assert_lc_ok(rc, &error);
  alpha = NULL;

  lc_index_flush_req_init(&flush_req);
  flush_req.namespace_name = "default";
  rc = reader->flush_index(reader, &flush_req, &after_first_flush, &error);
  assert_lc_ok(rc, &error);
  assert_string_equal(after_first_flush.namespace_name, "default");
  assert_string_equal(after_first_flush.mode, "wait");
  assert_true(after_first_flush.accepted);
  assert_true(after_first_flush.flushed);
  assert_false(after_first_flush.pending);
  assert_true(after_first_flush.index_seq > before_seq);
  first_seq = after_first_flush.index_seq;

  acquire.key = "integration/flush-index/bravo";
  rc = writer->acquire(writer, &acquire, &bravo, &error);
  assert_lc_ok(rc, &error);
  source = source_from_text("{\"flush\":2}", &error);
  rc = bravo->update(bravo, source, &update_opts, &error);
  lc_source_close(source);
  source = NULL;
  assert_lc_ok(rc, &error);
  rc = bravo->release(bravo, &release_req, &error);
  assert_lc_ok(rc, &error);
  bravo = NULL;

  lc_index_flush_req_init(&flush_req);
  flush_req.namespace_name = "default";
  flush_req.mode = "now";
  rc = reader->flush_index(reader, &flush_req, &after_second_flush, &error);
  assert_lc_ok(rc, &error);
  assert_string_equal(after_second_flush.namespace_name, "default");
  assert_string_equal(after_second_flush.mode, "now");
  assert_true(after_second_flush.accepted);
  assert_true(after_second_flush.flushed);
  assert_false(after_second_flush.pending);
  assert_true(after_second_flush.index_seq > first_seq);

  lc_index_flush_res_cleanup(&before_flush);
  lc_index_flush_res_cleanup(&after_first_flush);
  lc_index_flush_res_cleanup(&after_second_flush);
  writer->close(writer);
  reader->close(reader);
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

static void test_pouch_public_attachment_delete_semantics(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_client *reader;
  lc_lease *lease;
  lc_source *source;
  lc_acquire_req acquire;
  lc_release_req release_req;
  lc_attach_req attach_req;
  lc_attach_res first_attach;
  lc_attach_res second_attach;
  lc_attachment_list attachments;
  lc_attachment_selector selector;
  lc_error error;
  long version_after_attach;
  long version_after_delete;
  int deleted;
  int deleted_count;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "attachment-delete");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  client = NULL;
  reader = NULL;
  lease = NULL;
  source = NULL;
  memset(&first_attach, 0, sizeof(first_attach));
  memset(&second_attach, 0, sizeof(second_attach));
  memset(&attachments, 0, sizeof(attachments));

  open_pouch_client(endpoint, &client, &error);
  lc_acquire_req_init(&acquire);
  acquire.key = "integration/delete-attachments";
  acquire.owner = "writer";
  acquire.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  lc_attach_req_init(&attach_req);
  attach_req.name = "one.txt";
  attach_req.content_type = "text/plain";
  source = source_from_text("attachment-one", &error);
  rc = lease->attach(lease, &attach_req, source, &first_attach, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);
  assert_string_equal(first_attach.attachment.name, "one.txt");

  lc_attach_req_init(&attach_req);
  attach_req.name = "two.txt";
  attach_req.content_type = "text/plain";
  source = source_from_text("attachment-two", &error);
  rc = lease->attach(lease, &attach_req, source, &second_attach, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);
  assert_string_equal(second_attach.attachment.name, "two.txt");
  version_after_attach = lease->version;

  rc = lease->list_attachments(lease, &attachments, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(attachments.count, 2U);
  lc_attachment_list_cleanup(&attachments);

  lc_attachment_selector_init(&selector);
  selector.name = "one.txt";
  deleted = 0;
  rc = lease->delete_attachment(lease, &selector, &deleted, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(deleted, 1);
  assert_int_equal(lease->version, version_after_attach + 1L);
  version_after_delete = lease->version;

  rc = lease->list_attachments(lease, &attachments, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(attachments.count, 1U);
  assert_string_equal(attachments.items[0].name, "two.txt");
  lc_attachment_list_cleanup(&attachments);

  deleted = 1;
  rc = lease->delete_attachment(lease, &selector, &deleted, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(deleted, 0);
  assert_int_equal(lease->version, version_after_delete);

  deleted_count = 0;
  rc = lease->delete_all_attachments(lease, &deleted_count, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(deleted_count, 1);
  assert_int_equal(lease->version, version_after_delete + 1L);

  rc = lease->list_attachments(lease, &attachments, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(attachments.count, 0U);
  lc_attachment_list_cleanup(&attachments);

  deleted_count = 1;
  rc = lease->delete_all_attachments(lease, &deleted_count, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(deleted_count, 0);
  assert_int_equal(lease->version, version_after_delete + 1L);

  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;
  client->close(client);
  client = NULL;

  open_pouch_client(endpoint, &reader, &error);
  acquire.owner = "reader";
  rc = reader->acquire(reader, &acquire, &lease, &error);
  assert_lc_ok(rc, &error);
  rc = lease->list_attachments(lease, &attachments, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(attachments.count, 0U);
  lc_attachment_list_cleanup(&attachments);

  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;
  reader->close(reader);
  reader = NULL;

  lc_attach_res_cleanup(&first_attach);
  lc_attach_res_cleanup(&second_attach);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_attachment_prevent_overwrite(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_source *source;
  lc_sink *sink;
  lc_acquire_req acquire;
  lc_release_req release_req;
  lc_attach_req attach_req;
  lc_attach_res first_attach;
  lc_attach_res duplicate_attach;
  lc_attach_res overwrite_attach;
  lc_attachment_list attachments;
  lc_attachment_get_req get_attachment_req;
  lc_attachment_get_res get_attachment_res;
  lc_error error;
  long version_after_first;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "attachment-prevent-overwrite");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  client = NULL;
  lease = NULL;
  source = NULL;
  sink = NULL;
  memset(&first_attach, 0, sizeof(first_attach));
  memset(&duplicate_attach, 0, sizeof(duplicate_attach));
  memset(&overwrite_attach, 0, sizeof(overwrite_attach));
  memset(&attachments, 0, sizeof(attachments));
  memset(&get_attachment_res, 0, sizeof(get_attachment_res));

  open_pouch_client(endpoint, &client, &error);
  lc_acquire_req_init(&acquire);
  acquire.key = "integration/prevent-overwrite";
  acquire.owner = "writer";
  acquire.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  lc_attach_req_init(&attach_req);
  attach_req.name = "same.txt";
  attach_req.content_type = "text/plain";
  attach_req.prevent_overwrite = 1;
  source = source_from_text("first-body", &error);
  rc = lease->attach(lease, &attach_req, source, &first_attach, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);
  assert_string_equal(first_attach.attachment.name, "same.txt");
  version_after_first = lease->version;

  source = source_from_text("duplicate-body", &error);
  rc = lease->attach(lease, &attach_req, source, &duplicate_attach, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 409L);
  assert_string_equal(error.server_code, "attachment_exists");
  assert_int_equal(lease->version, version_after_first);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  rc = lease->list_attachments(lease, &attachments, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(attachments.count, 1U);
  assert_string_equal(attachments.items[0].name, "same.txt");
  lc_attachment_list_cleanup(&attachments);

  attach_req.prevent_overwrite = 0;
  source = source_from_text("overwrite-body", &error);
  rc = lease->attach(lease, &attach_req, source, &overwrite_attach, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);
  assert_string_equal(overwrite_attach.attachment.name, "same.txt");
  assert_string_not_equal(overwrite_attach.attachment.id,
                          first_attach.attachment.id);
  assert_int_equal(lease->version, version_after_first + 1L);

  rc = lease->list_attachments(lease, &attachments, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(attachments.count, 1U);
  assert_string_equal(attachments.items[0].name, "same.txt");
  lc_attachment_list_cleanup(&attachments);

  lc_attachment_get_req_init(&get_attachment_req);
  get_attachment_req.selector.name = "same.txt";
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = lease->get_attachment(lease, &get_attachment_req, sink,
                             &get_attachment_res, &error);
  assert_lc_ok(rc, &error);
  assert_string_equal(get_attachment_res.attachment.id,
                      overwrite_attach.attachment.id);
  assert_sink_text(sink, "overwrite-body", &error);
  lc_sink_close(sink);
  sink = NULL;

  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;
  client->close(client);
  client = NULL;

  lc_attach_res_cleanup(&first_attach);
  lc_attach_res_cleanup(&duplicate_attach);
  lc_attach_res_cleanup(&overwrite_attach);
  lc_attachment_get_res_cleanup(&get_attachment_res);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_client_level_attachment_apis(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_source *source;
  lc_sink *sink;
  lc_acquire_req acquire;
  lc_release_req release_req;
  lc_attach_op attach_op;
  lc_attach_res first_attach;
  lc_attach_res second_attach;
  lc_attachment_list_req list_req;
  lc_attachment_list attachments;
  lc_attachment_get_op get_op;
  lc_attachment_get_res get_res;
  lc_attachment_delete_op delete_op;
  lc_attachment_delete_all_op delete_all_op;
  lc_error error;
  long version_after_second;
  int deleted;
  int deleted_count;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "client-attachment-apis");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  client = NULL;
  lease = NULL;
  source = NULL;
  sink = NULL;
  memset(&first_attach, 0, sizeof(first_attach));
  memset(&second_attach, 0, sizeof(second_attach));
  memset(&attachments, 0, sizeof(attachments));
  memset(&get_res, 0, sizeof(get_res));

  open_pouch_client(endpoint, &client, &error);
  lc_acquire_req_init(&acquire);
  acquire.key = "integration/client-attachments";
  acquire.owner = "writer";
  acquire.ttl_seconds = 60L;
  rc = lc_acquire(client, &acquire, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  lc_attach_op_init(&attach_op);
  lease_ref_from_lease(lease, &attach_op.lease);
  attach_op.name = "client-one.txt";
  attach_op.content_type = "text/plain";
  source = source_from_text("client-one", &error);
  rc = lc_attach(client, &attach_op, source, &first_attach, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);
  assert_string_equal(first_attach.attachment.name, "client-one.txt");
  assert_int_equal(first_attach.version, 1L);

  lc_attach_op_init(&attach_op);
  lease_ref_from_lease(lease, &attach_op.lease);
  attach_op.name = "client-two.txt";
  attach_op.content_type = "text/plain";
  source = source_from_text("client-two", &error);
  rc = lc_attach(client, &attach_op, source, &second_attach, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);
  assert_string_equal(second_attach.attachment.name, "client-two.txt");
  assert_int_equal(second_attach.version, 2L);
  version_after_second = second_attach.version;

  lc_attachment_list_req_init(&list_req);
  lease_ref_from_lease(lease, &list_req.lease);
  rc = lc_list_attachments(client, &list_req, &attachments, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(attachments.count, 2U);
  assert_string_equal(attachments.items[0].name, "client-one.txt");
  assert_string_equal(attachments.items[1].name, "client-two.txt");
  lc_attachment_list_cleanup(&attachments);

  lc_attachment_get_op_init(&get_op);
  lease_ref_from_lease(lease, &get_op.lease);
  get_op.selector.id = first_attach.attachment.id;
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = lc_get_attachment(client, &get_op, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_string_equal(get_res.attachment.name, "client-one.txt");
  assert_sink_text(sink, "client-one", &error);
  lc_sink_close(sink);
  sink = NULL;
  lc_attachment_get_res_cleanup(&get_res);

  lc_attachment_delete_op_init(&delete_op);
  lease_ref_from_lease(lease, &delete_op.lease);
  delete_op.selector.name = "client-one.txt";
  deleted = 0;
  rc = lc_delete_attachment(client, &delete_op, &deleted, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(deleted, 1);

  memset(&attachments, 0, sizeof(attachments));
  rc = lc_list_attachments(client, &list_req, &attachments, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(attachments.count, 1U);
  assert_string_equal(attachments.items[0].name, "client-two.txt");
  lc_attachment_list_cleanup(&attachments);

  lc_attachment_delete_all_op_init(&delete_all_op);
  lease_ref_from_lease(lease, &delete_all_op.lease);
  deleted_count = 0;
  rc = lc_delete_all_attachments(client, &delete_all_op, &deleted_count,
                                 &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(deleted_count, 1);

  memset(&attachments, 0, sizeof(attachments));
  rc = lc_list_attachments(client, &list_req, &attachments, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(attachments.count, 0U);
  lc_attachment_list_cleanup(&attachments);

  lc_release_req_init(&release_req);
  rc = lc_lease_release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;
  client->close(client);
  client = NULL;

  lc_attach_res_cleanup(&first_attach);
  lc_attach_res_cleanup(&second_attach);
  lc_attachment_get_res_cleanup(&get_res);
  lc_error_cleanup(&error);
  assert_int_equal(version_after_second, 2L);
  cleanup_pouch_root(root);
}

static void test_pouch_public_attachment_read_after_release(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_lease *lease;
  lc_source *source;
  lc_sink *sink;
  lc_acquire_req acquire;
  lc_attach_req attach_req;
  lc_attach_res attach_res;
  lc_attachment_list_req list_req;
  lc_attachment_list attachments;
  lc_attachment_get_op get_op;
  lc_attachment_get_res get_res;
  lc_release_req release_req;
  lc_lease_ref released_ref;
  lc_error error;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "public-attachment-after-release");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  client = NULL;
  lease = NULL;
  source = NULL;
  sink = NULL;
  memset(&attach_res, 0, sizeof(attach_res));
  memset(&attachments, 0, sizeof(attachments));
  memset(&get_res, 0, sizeof(get_res));
  memset(&released_ref, 0, sizeof(released_ref));

  open_pouch_client(endpoint, &client, &error);
  lc_acquire_req_init(&acquire);
  acquire.key = "integration/public-attachment-after-release";
  acquire.owner = "writer";
  acquire.ttl_seconds = 60L;
  rc = lc_acquire(client, &acquire, &lease, &error);
  assert_lc_ok(rc, &error);

  lc_attach_req_init(&attach_req);
  attach_req.name = "released.txt";
  attach_req.content_type = "text/plain";
  source = source_from_text("released-body", &error);
  rc = lease->attach(lease, &attach_req, source, &attach_res, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);
  assert_non_null(attach_res.attachment.id);

  released_ref.namespace_name = strdup(lease->namespace_name);
  released_ref.key = strdup(lease->key);
  released_ref.lease_id = strdup(lease->lease_id);
  released_ref.txn_id = strdup(lease->txn_id);
  released_ref.fencing_token = lease->fencing_token;
  assert_non_null(released_ref.namespace_name);
  assert_non_null(released_ref.key);
  assert_non_null(released_ref.lease_id);
  assert_non_null(released_ref.txn_id);

  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  lc_attachment_list_req_init(&list_req);
  list_req.lease = released_ref;
  list_req.public_read = 1;
  rc = lc_list_attachments(client, &list_req, &attachments, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(attachments.count, 1U);
  assert_string_equal(attachments.items[0].name, "released.txt");
  assert_string_equal(attachments.items[0].id, attach_res.attachment.id);
  lc_attachment_list_cleanup(&attachments);

  lc_attachment_get_op_init(&get_op);
  get_op.lease = released_ref;
  get_op.selector.id = attach_res.attachment.id;
  get_op.public_read = 1;
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = lc_get_attachment(client, &get_op, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_string_equal(get_res.attachment.name, "released.txt");
  assert_string_equal(get_res.attachment.content_type, "text/plain");
  assert_sink_text(sink, "released-body", &error);
  lc_sink_close(sink);
  sink = NULL;

  client->close(client);
  client = NULL;
  free((char *)released_ref.namespace_name);
  free((char *)released_ref.key);
  free((char *)released_ref.lease_id);
  free((char *)released_ref.txn_id);
  lc_attach_res_cleanup(&attach_res);
  lc_attachment_get_res_cleanup(&get_res);
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
  assert_string_equal(enqueue_res.correlation_id, "pouch-enqueue");

  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "integration-jobs";
  rc = observer->queue_stats(observer, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_true(stats.available);
  assert_string_equal(stats.head_message_id, enqueue_res.message_id);
  assert_string_equal(stats.correlation_id, "pouch-queue-stats");
  lc_queue_stats_res_cleanup(&stats);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "integration-jobs";
  dequeue_req.owner = "worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  rc = consumer->dequeue(consumer, &dequeue_req, &message, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(message);
  assert_string_equal(message->correlation_id, "pouch-dequeue");
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

static void test_pouch_public_queue_initial_delay_hides_until_visible(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *producer;
  lc_client *worker;
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
  pouch_root_path(root, sizeof(root), "queue-initial-delay");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  producer = NULL;
  worker = NULL;
  source = NULL;
  sink = NULL;
  message = NULL;
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&stats, 0, sizeof(stats));

  open_pouch_client(endpoint, &producer, &error);
  open_pouch_client(endpoint, &worker, &error);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "initial-delay";
  enqueue_req.content_type = "text/plain";
  enqueue_req.delay_seconds = 2L;
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 3600L;
  enqueue_req.max_attempts = 3;
  source = source_from_text("delayed-work", &error);
  rc = producer->enqueue(producer, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);
  assert_true(enqueue_res.not_visible_until_unix > 0L);

  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "initial-delay";
  rc = producer->queue_stats(producer, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_false(stats.available);
  assert_int_equal(stats.pending_candidates, 1);
  assert_null(stats.head_message_id);
  lc_queue_stats_res_cleanup(&stats);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "initial-delay";
  dequeue_req.owner = "delayed-worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  rc = worker->dequeue(worker, &dequeue_req, &message, &error);
  assert_lc_ok(rc, &error);
  assert_null(message);

  sleep(3U);

  memset(&stats, 0, sizeof(stats));
  rc = producer->queue_stats(producer, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_true(stats.available);
  assert_int_equal(stats.pending_candidates, 1);
  assert_string_equal(stats.head_message_id, enqueue_res.message_id);
  lc_queue_stats_res_cleanup(&stats);

  rc = worker->dequeue(worker, &dequeue_req, &message, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(message);
  assert_string_equal(message->message_id, enqueue_res.message_id);
  assert_int_equal(message->attempts, 1);
  assert_int_equal(message->failure_attempts, 0);

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  written = 0U;
  rc = message->write_payload(message, sink, &written, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(written, strlen("delayed-work"));
  assert_sink_text(sink, "delayed-work", &error);
  lc_sink_close(sink);
  sink = NULL;

  rc = message->ack(message, &error);
  assert_lc_ok(rc, &error);
  message = NULL;

  memset(&stats, 0, sizeof(stats));
  rc = producer->queue_stats(producer, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_false(stats.available);
  assert_int_equal(stats.pending_candidates, 0);
  assert_null(stats.head_message_id);

  lc_queue_stats_res_cleanup(&stats);
  lc_enqueue_res_cleanup(&enqueue_res);
  producer->close(producer);
  worker->close(worker);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_queue_ttl_expiry_removes_candidate(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *producer;
  lc_client *worker;
  lc_client *reopened;
  lc_source *source;
  lc_message *message;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats;
  lc_error error;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "queue-ttl-expiry");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  producer = NULL;
  worker = NULL;
  reopened = NULL;
  source = NULL;
  message = NULL;
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&stats, 0, sizeof(stats));

  open_pouch_client(endpoint, &producer, &error);
  open_pouch_client(endpoint, &worker, &error);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "ttl-expiry";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 1L;
  enqueue_req.max_attempts = 3;
  source = source_from_text("expires-before-delivery", &error);
  rc = producer->enqueue(producer, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);

  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "ttl-expiry";
  rc = producer->queue_stats(producer, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_true(stats.available);
  assert_int_equal(stats.pending_candidates, 1);
  assert_string_equal(stats.head_message_id, enqueue_res.message_id);
  lc_queue_stats_res_cleanup(&stats);

  sleep(2U);

  memset(&stats, 0, sizeof(stats));
  rc = producer->queue_stats(producer, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_false(stats.available);
  assert_int_equal(stats.pending_candidates, 0);
  assert_null(stats.head_message_id);
  lc_queue_stats_res_cleanup(&stats);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "ttl-expiry";
  dequeue_req.owner = "ttl-worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  rc = worker->dequeue(worker, &dequeue_req, &message, &error);
  assert_lc_ok(rc, &error);
  assert_null(message);

  producer->close(producer);
  producer = NULL;
  worker->close(worker);
  worker = NULL;

  open_pouch_client(endpoint, &reopened, &error);
  memset(&stats, 0, sizeof(stats));
  rc = reopened->queue_stats(reopened, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_false(stats.available);
  assert_int_equal(stats.pending_candidates, 0);
  assert_null(stats.head_message_id);
  lc_queue_stats_res_cleanup(&stats);

  rc = reopened->dequeue(reopened, &dequeue_req, &message, &error);
  assert_lc_ok(rc, &error);
  assert_null(message);

  lc_enqueue_res_cleanup(&enqueue_res);
  reopened->close(reopened);
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

static void test_pouch_public_queue_nack_delay_redelivery(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *producer;
  lc_client *worker_a;
  lc_client *worker_b;
  lc_source *source;
  lc_message *first_delivery;
  lc_message *redelivery;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats;
  lc_nack_req nack_req;
  lc_error error;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "queue-nack-delay");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  producer = NULL;
  worker_a = NULL;
  worker_b = NULL;
  source = NULL;
  first_delivery = NULL;
  redelivery = NULL;
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&stats, 0, sizeof(stats));

  open_pouch_client(endpoint, &producer, &error);
  open_pouch_client(endpoint, &worker_a, &error);
  open_pouch_client(endpoint, &worker_b, &error);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "delay-jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 60L;
  enqueue_req.max_attempts = 3;
  source = source_from_text("delay-work", &error);
  rc = producer->enqueue(producer, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "delay-jobs";
  dequeue_req.owner = "worker-a";
  dequeue_req.visibility_timeout_seconds = 30L;
  rc = worker_a->dequeue(worker_a, &dequeue_req, &first_delivery, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(first_delivery);
  assert_string_equal(first_delivery->message_id, enqueue_res.message_id);
  assert_int_equal(first_delivery->attempts, 1);

  lc_nack_req_init(&nack_req);
  nack_req.intent = LC_NACK_INTENT_DEFER;
  nack_req.delay_seconds = 2L;
  rc = first_delivery->nack(first_delivery, &nack_req, &error);
  assert_lc_ok(rc, &error);
  first_delivery = NULL;

  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "delay-jobs";
  rc = producer->queue_stats(producer, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_false(stats.available);
  assert_int_equal(stats.pending_candidates, 1);
  assert_null(stats.head_message_id);
  lc_queue_stats_res_cleanup(&stats);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "delay-jobs";
  dequeue_req.owner = "worker-b";
  dequeue_req.visibility_timeout_seconds = 30L;
  rc = worker_b->dequeue(worker_b, &dequeue_req, &redelivery, &error);
  assert_lc_ok(rc, &error);
  assert_null(redelivery);

  sleep(3U);

  memset(&stats, 0, sizeof(stats));
  rc = producer->queue_stats(producer, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_true(stats.available);
  assert_string_equal(stats.head_message_id, enqueue_res.message_id);
  lc_queue_stats_res_cleanup(&stats);

  rc = worker_b->dequeue(worker_b, &dequeue_req, &redelivery, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(redelivery);
  assert_string_equal(redelivery->message_id, enqueue_res.message_id);
  assert_int_equal(redelivery->attempts, 2);
  assert_int_equal(redelivery->failure_attempts, 0);

  rc = redelivery->ack(redelivery, &error);
  assert_lc_ok(rc, &error);
  redelivery = NULL;

  lc_enqueue_res_cleanup(&enqueue_res);
  producer->close(producer);
  worker_a->close(worker_a);
  worker_b->close(worker_b);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_queue_retry_exhaustion_terminal(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *producer;
  lc_client *worker;
  lc_client *reopened;
  lc_source *source;
  lc_message *first_delivery;
  lc_message *second_delivery;
  lc_message *empty_delivery;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_nack_op nack_op;
  lc_nack_res first_nack;
  lc_nack_res final_nack;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats;
  lc_error error;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "queue-retry-exhaustion");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  producer = NULL;
  worker = NULL;
  reopened = NULL;
  source = NULL;
  first_delivery = NULL;
  second_delivery = NULL;
  empty_delivery = NULL;
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&first_nack, 0, sizeof(first_nack));
  memset(&final_nack, 0, sizeof(final_nack));
  memset(&stats, 0, sizeof(stats));

  open_pouch_client(endpoint, &producer, &error);
  open_pouch_client(endpoint, &worker, &error);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "retry-terminal";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 3600L;
  enqueue_req.max_attempts = 2;
  source = source_from_text("terminal-work", &error);
  rc = producer->enqueue(producer, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "retry-terminal";
  dequeue_req.owner = "worker-a";
  dequeue_req.visibility_timeout_seconds = 30L;
  rc = worker->dequeue(worker, &dequeue_req, &first_delivery, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(first_delivery);
  assert_string_equal(first_delivery->message_id, enqueue_res.message_id);
  assert_int_equal(first_delivery->attempts, 1);
  assert_int_equal(first_delivery->failure_attempts, 0);

  lc_nack_op_init(&nack_op);
  message_ref_from_message(first_delivery, &nack_op.message);
  nack_op.intent = LC_NACK_INTENT_FAILURE;
  rc = lc_queue_nack(worker, &nack_op, &first_nack, &error);
  assert_lc_ok(rc, &error);
  assert_true(first_nack.requeued);
  assert_non_null(first_nack.meta_etag);
  assert_string_equal(first_nack.correlation_id, "pouch-nack");
  first_delivery->close(first_delivery);
  first_delivery = NULL;

  rc = worker->dequeue(worker, &dequeue_req, &second_delivery, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(second_delivery);
  assert_string_equal(second_delivery->message_id, enqueue_res.message_id);
  assert_int_equal(second_delivery->attempts, 2);
  assert_int_equal(second_delivery->failure_attempts, 1);
  assert_true(second_delivery->fencing_token > 0L);

  lc_nack_op_init(&nack_op);
  message_ref_from_message(second_delivery, &nack_op.message);
  nack_op.intent = LC_NACK_INTENT_FAILURE;
  rc = lc_queue_nack(worker, &nack_op, &final_nack, &error);
  assert_lc_ok(rc, &error);
  assert_false(final_nack.requeued);
  assert_non_null(final_nack.meta_etag);
  assert_string_equal(final_nack.correlation_id, "pouch-nack");
  second_delivery->close(second_delivery);
  second_delivery = NULL;

  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "retry-terminal";
  rc = producer->queue_stats(producer, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_false(stats.available);
  assert_int_equal(stats.pending_candidates, 0);
  assert_null(stats.head_message_id);
  lc_queue_stats_res_cleanup(&stats);

  rc = worker->dequeue(worker, &dequeue_req, &empty_delivery, &error);
  assert_lc_ok(rc, &error);
  assert_null(empty_delivery);

  producer->close(producer);
  producer = NULL;
  worker->close(worker);
  worker = NULL;

  open_pouch_client(endpoint, &reopened, &error);
  memset(&stats, 0, sizeof(stats));
  rc = reopened->queue_stats(reopened, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_false(stats.available);
  assert_int_equal(stats.pending_candidates, 0);
  assert_null(stats.head_message_id);

  lc_queue_stats_res_cleanup(&stats);
  lc_nack_res_cleanup(&first_nack);
  lc_nack_res_cleanup(&final_nack);
  lc_enqueue_res_cleanup(&enqueue_res);
  reopened->close(reopened);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_queue_stats_is_read_only(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *producer;
  lc_client *worker_a;
  lc_client *worker_b;
  lc_source *source;
  lc_message *first_delivery;
  lc_message *second_delivery;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats;
  lc_error error;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "queue-stats-read-only");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  producer = NULL;
  worker_a = NULL;
  worker_b = NULL;
  source = NULL;
  first_delivery = NULL;
  second_delivery = NULL;
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&stats, 0, sizeof(stats));

  open_pouch_client(endpoint, &producer, &error);
  open_pouch_client(endpoint, &worker_a, &error);
  open_pouch_client(endpoint, &worker_b, &error);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "stats-jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 3600L;
  enqueue_req.max_attempts = 3;
  source = source_from_text("stats-work", &error);
  rc = producer->enqueue(producer, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "stats-jobs";
  dequeue_req.owner = "worker-a";
  dequeue_req.visibility_timeout_seconds = 30L;
  rc = worker_a->dequeue(worker_a, &dequeue_req, &first_delivery, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(first_delivery);
  assert_string_equal(first_delivery->message_id, enqueue_res.message_id);
  assert_int_equal(first_delivery->attempts, 1);
  assert_int_equal(first_delivery->failure_attempts, 0);

  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "stats-jobs";
  rc = producer->queue_stats(producer, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_false(stats.available);
  assert_int_equal(stats.pending_candidates, 1);
  assert_null(stats.head_message_id);
  lc_queue_stats_res_cleanup(&stats);

  memset(&stats, 0, sizeof(stats));
  rc = producer->queue_stats(producer, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_false(stats.available);
  assert_int_equal(stats.pending_candidates, 1);
  assert_null(stats.head_message_id);
  lc_queue_stats_res_cleanup(&stats);

  dequeue_req.owner = "worker-b";
  rc = worker_b->dequeue(worker_b, &dequeue_req, &second_delivery, &error);
  assert_lc_ok(rc, &error);
  assert_null(second_delivery);

  rc = first_delivery->ack(first_delivery, &error);
  assert_lc_ok(rc, &error);
  first_delivery = NULL;

  memset(&stats, 0, sizeof(stats));
  rc = producer->queue_stats(producer, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_false(stats.available);
  assert_int_equal(stats.pending_candidates, 0);
  assert_null(stats.head_message_id);

  lc_queue_stats_res_cleanup(&stats);
  lc_enqueue_res_cleanup(&enqueue_res);
  producer->close(producer);
  worker_a->close(worker_a);
  worker_b->close(worker_b);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_queue_batch_no_duplicate_acked_delivery(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *producer;
  lc_client *worker_a;
  lc_client *worker_b;
  lc_source *source;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res[4];
  lc_dequeue_req dequeue_req;
  lc_dequeue_batch_res first_batch;
  lc_dequeue_batch_res second_batch;
  lc_dequeue_batch_res empty_batch;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats;
  lc_error error;
  const char *payloads[4];
  size_t index;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "queue-batch");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  producer = NULL;
  worker_a = NULL;
  worker_b = NULL;
  source = NULL;
  memset(enqueue_res, 0, sizeof(enqueue_res));
  memset(&first_batch, 0, sizeof(first_batch));
  memset(&second_batch, 0, sizeof(second_batch));
  memset(&empty_batch, 0, sizeof(empty_batch));
  memset(&stats, 0, sizeof(stats));
  payloads[0] = "batch-one";
  payloads[1] = "batch-two";
  payloads[2] = "batch-three";
  payloads[3] = "batch-four";

  open_pouch_client(endpoint, &producer, &error);
  open_pouch_client(endpoint, &worker_a, &error);
  open_pouch_client(endpoint, &worker_b, &error);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "batch-jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 3600L;
  enqueue_req.max_attempts = 3;
  for (index = 0U; index < 4U; ++index) {
    source = source_from_text(payloads[index], &error);
    rc = producer->enqueue(producer, &enqueue_req, source,
                           &enqueue_res[index], &error);
    lc_source_close(source);
    assert_lc_ok(rc, &error);
  }

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "batch-jobs";
  dequeue_req.owner = "worker-a";
  dequeue_req.visibility_timeout_seconds = 30L;
  dequeue_req.page_size = 2;
  rc = worker_a->dequeue_batch(worker_a, &dequeue_req, &first_batch, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(first_batch.count, 2U);
  assert_string_equal(first_batch.messages[0]->message_id,
                      enqueue_res[0].message_id);
  assert_string_equal(first_batch.messages[1]->message_id,
                      enqueue_res[1].message_id);
  assert_string_not_equal(first_batch.messages[0]->message_id,
                          first_batch.messages[1]->message_id);

  rc = first_batch.messages[0]->ack(first_batch.messages[0], &error);
  assert_lc_ok(rc, &error);
  first_batch.messages[0] = NULL;
  rc = first_batch.messages[1]->ack(first_batch.messages[1], &error);
  assert_lc_ok(rc, &error);
  first_batch.messages[1] = NULL;
  lc_dequeue_batch_cleanup(&first_batch);

  dequeue_req.owner = "worker-b";
  dequeue_req.page_size = 4;
  rc = worker_b->dequeue_batch(worker_b, &dequeue_req, &second_batch, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(second_batch.count, 2U);
  assert_string_equal(second_batch.messages[0]->message_id,
                      enqueue_res[2].message_id);
  assert_string_equal(second_batch.messages[1]->message_id,
                      enqueue_res[3].message_id);
  assert_string_not_equal(second_batch.messages[0]->message_id,
                          second_batch.messages[1]->message_id);

  rc = second_batch.messages[0]->ack(second_batch.messages[0], &error);
  assert_lc_ok(rc, &error);
  second_batch.messages[0] = NULL;
  rc = second_batch.messages[1]->ack(second_batch.messages[1], &error);
  assert_lc_ok(rc, &error);
  second_batch.messages[1] = NULL;
  lc_dequeue_batch_cleanup(&second_batch);

  rc = worker_b->dequeue_batch(worker_b, &dequeue_req, &empty_batch, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(empty_batch.count, 0U);
  lc_dequeue_batch_cleanup(&empty_batch);

  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "batch-jobs";
  rc = producer->queue_stats(producer, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_false(stats.available);
  assert_int_equal(stats.pending_candidates, 0);

  lc_queue_stats_res_cleanup(&stats);
  for (index = 0U; index < 4U; ++index) {
    lc_enqueue_res_cleanup(&enqueue_res[index]);
  }
  producer->close(producer);
  worker_a->close(worker_a);
  worker_b->close(worker_b);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_queue_batch_honors_start_after_cursor(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *producer;
  lc_client *worker;
  lc_client *observer;
  lc_source *source;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res[3];
  lc_dequeue_req dequeue_req;
  lc_dequeue_batch_res batch;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats;
  lc_error error;
  const char *payloads[3];
  size_t index;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "queue-batch-start-after");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  producer = NULL;
  worker = NULL;
  observer = NULL;
  source = NULL;
  memset(enqueue_res, 0, sizeof(enqueue_res));
  memset(&batch, 0, sizeof(batch));
  memset(&stats, 0, sizeof(stats));
  payloads[0] = "cursor-one";
  payloads[1] = "cursor-two";
  payloads[2] = "cursor-three";

  open_pouch_client(endpoint, &producer, &error);
  open_pouch_client(endpoint, &worker, &error);
  open_pouch_client(endpoint, &observer, &error);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "cursor-jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 3600L;
  enqueue_req.max_attempts = 3;
  for (index = 0U; index < 3U; ++index) {
    source = source_from_text(payloads[index], &error);
    rc = producer->enqueue(producer, &enqueue_req, source,
                           &enqueue_res[index], &error);
    lc_source_close(source);
    assert_lc_ok(rc, &error);
  }

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "cursor-jobs";
  dequeue_req.owner = "cursor-worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  dequeue_req.page_size = 2;
  dequeue_req.start_after = enqueue_res[0].message_id;
  rc = worker->dequeue_batch(worker, &dequeue_req, &batch, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(batch.count, 2U);
  assert_string_equal(batch.messages[0]->message_id,
                      enqueue_res[1].message_id);
  assert_string_equal(batch.messages[0]->next_cursor,
                      enqueue_res[1].message_id);
  assert_string_equal(batch.messages[1]->message_id,
                      enqueue_res[2].message_id);
  assert_string_equal(batch.messages[1]->next_cursor,
                      enqueue_res[2].message_id);

  rc = batch.messages[0]->ack(batch.messages[0], &error);
  assert_lc_ok(rc, &error);
  batch.messages[0] = NULL;
  rc = batch.messages[1]->ack(batch.messages[1], &error);
  assert_lc_ok(rc, &error);
  batch.messages[1] = NULL;
  lc_dequeue_batch_cleanup(&batch);

  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "cursor-jobs";
  rc = observer->queue_stats(observer, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_true(stats.available);
  assert_int_equal(stats.pending_candidates, 1);
  assert_string_equal(stats.head_message_id, enqueue_res[0].message_id);

  lc_queue_stats_res_cleanup(&stats);
  for (index = 0U; index < 3U; ++index) {
    lc_enqueue_res_cleanup(&enqueue_res[index]);
  }
  producer->close(producer);
  worker->close(worker);
  observer->close(observer);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_dequeue_with_state_honors_start_after_cursor(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *producer;
  lc_client *worker;
  lc_client *observer;
  lc_source *source;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res[3];
  lc_dequeue_req dequeue_req;
  lc_update_opts update_opts;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats;
  lc_message *message;
  lc_lease *queue_state;
  lc_error error;
  const char *payloads[3];
  size_t index;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "queue-state-start-after");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  producer = NULL;
  worker = NULL;
  observer = NULL;
  source = NULL;
  message = NULL;
  memset(enqueue_res, 0, sizeof(enqueue_res));
  memset(&stats, 0, sizeof(stats));
  payloads[0] = "state-cursor-one";
  payloads[1] = "state-cursor-two";
  payloads[2] = "state-cursor-three";

  open_pouch_client(endpoint, &producer, &error);
  open_pouch_client(endpoint, &worker, &error);
  open_pouch_client(endpoint, &observer, &error);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "state-cursor-jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 3600L;
  enqueue_req.max_attempts = 3;
  for (index = 0U; index < 3U; ++index) {
    source = source_from_text(payloads[index], &error);
    rc = producer->enqueue(producer, &enqueue_req, source,
                           &enqueue_res[index], &error);
    lc_source_close(source);
    assert_lc_ok(rc, &error);
  }

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "state-cursor-jobs";
  dequeue_req.owner = "state-cursor-worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  dequeue_req.start_after = enqueue_res[0].message_id;
  rc = worker->dequeue_with_state(worker, &dequeue_req, &message, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(message);
  assert_string_equal(message->message_id, enqueue_res[1].message_id);
  assert_string_equal(message->next_cursor, enqueue_res[1].message_id);
  queue_state = message->state(message);
  assert_non_null(queue_state);
  assert_string_equal(queue_state->namespace_name, "default");
  assert_non_null(queue_state->lease_id);

  lc_update_opts_init(&update_opts);
  update_opts.content_type = "application/json";
  source = source_from_text("{\"cursor_state\":true}", &error);
  rc = queue_state->update(queue_state, source, &update_opts, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);

  rc = message->ack(message, &error);
  assert_lc_ok(rc, &error);
  message = NULL;

  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "state-cursor-jobs";
  rc = observer->queue_stats(observer, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_true(stats.available);
  assert_int_equal(stats.pending_candidates, 2);
  assert_string_equal(stats.head_message_id, enqueue_res[0].message_id);

  lc_queue_stats_res_cleanup(&stats);
  for (index = 0U; index < 3U; ++index) {
    lc_enqueue_res_cleanup(&enqueue_res[index]);
  }
  producer->close(producer);
  worker->close(worker);
  observer->close(observer);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_watch_queue_snapshots(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *producer;
  lc_client *watcher;
  lc_client *worker;
  lc_source *source;
  lc_message *message;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_watch_queue_req watch_req;
  lc_watch_handler handler;
  lc_dequeue_req dequeue_req;
  pouch_watch_state watch_state;
  lc_error error;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "watch-queue");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  producer = NULL;
  watcher = NULL;
  worker = NULL;
  source = NULL;
  message = NULL;
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&handler, 0, sizeof(handler));
  memset(&watch_state, 0, sizeof(watch_state));

  open_pouch_client(endpoint, &producer, &error);
  open_pouch_client(endpoint, &watcher, &error);
  open_pouch_client(endpoint, &worker, &error);

  lc_watch_queue_req_init(&watch_req);
  watch_req.queue = "watch-jobs";
  handler.handle = pouch_watch_handle;
  handler.context = &watch_state;
  rc = watcher->watch_queue(watcher, &watch_req, &handler, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(watch_state.handled, 1U);
  assert_false(watch_state.available);
  assert_string_equal(watch_state.queue, "watch-jobs");
  assert_string_equal(watch_state.head_message_id, "");
  assert_string_equal(watch_state.correlation_id, "pouch-watch");

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "watch-jobs";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 3600L;
  enqueue_req.max_attempts = 3;
  source = source_from_text("watch-work", &error);
  rc = producer->enqueue(producer, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);

  memset(&watch_state, 0, sizeof(watch_state));
  rc = watcher->watch_queue(watcher, &watch_req, &handler, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(watch_state.handled, 1U);
  assert_true(watch_state.available);
  assert_string_equal(watch_state.queue, "watch-jobs");
  assert_string_equal(watch_state.head_message_id, enqueue_res.message_id);
  assert_string_equal(watch_state.correlation_id, "pouch-watch");

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "watch-jobs";
  dequeue_req.owner = "watch-worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  rc = worker->dequeue(worker, &dequeue_req, &message, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(message);
  rc = message->ack(message, &error);
  assert_lc_ok(rc, &error);
  message = NULL;

  memset(&watch_state, 0, sizeof(watch_state));
  rc = watcher->watch_queue(watcher, &watch_req, &handler, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(watch_state.handled, 1U);
  assert_false(watch_state.available);
  assert_string_equal(watch_state.queue, "watch-jobs");
  assert_string_equal(watch_state.head_message_id, "");

  lc_enqueue_res_cleanup(&enqueue_res);
  producer->close(producer);
  watcher->close(watcher);
  worker->close(worker);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_subscribe_with_state(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *producer;
  lc_client *subscriber;
  lc_client *verifier;
  lc_source *source;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req subscribe_req;
  lc_consumer consumer;
  pouch_subscribe_state_test subscribe_state;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats;
  lc_error error;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "subscribe-state");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  producer = NULL;
  subscriber = NULL;
  verifier = NULL;
  source = NULL;
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&consumer, 0, sizeof(consumer));
  memset(&subscribe_state, 0, sizeof(subscribe_state));
  memset(&stats, 0, sizeof(stats));

  open_pouch_client(endpoint, &producer, &error);
  open_pouch_client(endpoint, &subscriber, &error);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "subscribe-state";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 3600L;
  enqueue_req.max_attempts = 3;
  source = source_from_text("subscribe-work", &error);
  rc = producer->enqueue(producer, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);

  lc_dequeue_req_init(&subscribe_req);
  subscribe_req.queue = "subscribe-state";
  subscribe_req.owner = "state-subscriber";
  subscribe_req.visibility_timeout_seconds = 30L;
  subscribe_req.page_size = 1;
  consumer.handle = pouch_subscribe_state_handle;
  consumer.context = &subscribe_state;
  rc = subscriber->subscribe_with_state(subscriber, &subscribe_req, &consumer,
                                        &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(subscribe_state.handled, 1U);
  assert_string_equal(subscribe_state.message_id, enqueue_res.message_id);

  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "subscribe-state";
  rc = producer->queue_stats(producer, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_false(stats.available);
  assert_int_equal(stats.pending_candidates, 0);
  lc_queue_stats_res_cleanup(&stats);

  open_pouch_client(endpoint, &verifier, &error);
  assert_client_state_text(
      verifier, subscribe_state.state_key,
      "{\"subscribe\":\"stateful\",\"saved\":true}", &error);
  verifier->close(verifier);
  verifier = NULL;

  lc_enqueue_res_cleanup(&enqueue_res);
  producer->close(producer);
  subscriber->close(subscriber);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_subscribe_waits_for_later_enqueue(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *producer;
  lc_source *source;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats;
  pouch_subscribe_wait_test subscribe_state;
  pthread_t subscribe_thread;
  lc_error error;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "subscribe-wait");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  producer = NULL;
  source = NULL;
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&stats, 0, sizeof(stats));
  memset(&subscribe_state, 0, sizeof(subscribe_state));
  subscribe_state.endpoint = endpoint;

  assert_int_equal(
      pthread_create(&subscribe_thread, NULL, pouch_subscribe_wait_main,
                     &subscribe_state),
      0);
  usleep(200000U);

  open_pouch_client(endpoint, &producer, &error);
  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "subscribe-wait";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 3600L;
  enqueue_req.max_attempts = 3;
  source = source_from_text("waited-work", &error);
  rc = producer->enqueue(producer, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);

  assert_int_equal(pthread_join(subscribe_thread, NULL), 0);
  assert_lc_ok(subscribe_state.rc, &subscribe_state.error);
  assert_int_equal(subscribe_state.handled, 1U);
  assert_true(subscribe_state.payload_ok);
  assert_string_equal(subscribe_state.message_id, enqueue_res.message_id);

  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "subscribe-wait";
  rc = producer->queue_stats(producer, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_false(stats.available);
  assert_int_equal(stats.pending_candidates, 0);

  lc_queue_stats_res_cleanup(&stats);
  lc_enqueue_res_cleanup(&enqueue_res);
  producer->close(producer);
  lc_error_cleanup(&subscribe_state.error);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_dequeue_waits_for_later_enqueue(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *consumer;
  lc_dequeue_req dequeue_req;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats;
  lc_message *message;
  lc_sink *sink;
  pouch_delayed_enqueue_test enqueue_test;
  pthread_t enqueue_thread;
  lc_error error;
  size_t written;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "dequeue-wait");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  memset(&stats, 0, sizeof(stats));
  memset(&enqueue_test, 0, sizeof(enqueue_test));
  consumer = NULL;
  message = NULL;
  sink = NULL;

  open_pouch_client(endpoint, &consumer, &error);
  enqueue_test.endpoint = endpoint;
  enqueue_test.queue = "dequeue-wait";
  enqueue_test.payload = "waited-dequeue";
  assert_int_equal(
      pthread_create(&enqueue_thread, NULL, pouch_delayed_enqueue_main,
                     &enqueue_test),
      0);

  lc_dequeue_req_init(&dequeue_req);
  dequeue_req.queue = "dequeue-wait";
  dequeue_req.owner = "wait-worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  dequeue_req.wait_seconds = 1L;
  rc = consumer->dequeue(consumer, &dequeue_req, &message, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(message);
  assert_string_equal(message->queue, "dequeue-wait");
  assert_string_equal(message->payload_content_type, "text/plain");

  assert_int_equal(pthread_join(enqueue_thread, NULL), 0);
  assert_lc_ok(enqueue_test.rc, &enqueue_test.error);
  lc_error_cleanup(&enqueue_test.error);

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  written = 0U;
  rc = message->write_payload(message, sink, &written, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(written, strlen("waited-dequeue"));
  assert_sink_text(sink, "waited-dequeue", &error);
  lc_sink_close(sink);

  rc = message->ack(message, &error);
  assert_lc_ok(rc, &error);
  message = NULL;

  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "dequeue-wait";
  rc = consumer->queue_stats(consumer, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_false(stats.available);
  assert_int_equal(stats.pending_candidates, 0);

  lc_queue_stats_res_cleanup(&stats);
  consumer->close(consumer);
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

static void test_pouch_public_consumer_service_start_wait_with_state(
    void **state) {
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
  pouch_root_path(root, sizeof(root), "consumer-start-wait");
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
  enqueue_req.queue = "managed-start-state";
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
  consumer_config.request.queue = "managed-start-state";
  consumer_config.request.owner = "managed-start-worker";
  consumer_config.request.visibility_timeout_seconds = 30L;
  consumer_config.request.wait_seconds = 1L;
  consumer_config.with_state = 1;
  consumer_config.handle = pouch_consumer_state_handle;
  consumer_config.context = &consumer_state;
  service_config.consumers = &consumer_config;
  service_config.consumer_count = 1U;
  rc = lc_client_new_consumer_service(producer, &service_config, &service,
                                      &error);
  assert_lc_ok(rc, &error);
  assert_non_null(service);
  consumer_state.service = service;
  rc = lc_consumer_service_start(service, &error);
  assert_lc_ok(rc, &error);
  rc = lc_consumer_service_wait(service, &error);
  assert_lc_ok(rc, &error);
  service->close(service);
  service = NULL;

  assert_int_equal(consumer_state.handled, 1U);
  assert_string_equal(consumer_state.queue, "managed-start-state");
  assert_string_equal(consumer_state.message_id, enqueue_res.message_id);

  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "managed-start-state";
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

static void test_pouch_public_consumer_service_polls_later_enqueue(
    void **state) {
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
  pouch_root_path(root, sizeof(root), "consumer-poll-later");
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

  lc_consumer_config_init(&consumer_config);
  lc_consumer_service_config_init(&service_config);
  consumer_config.request.queue = "managed-later-state";
  consumer_config.request.owner = "managed-later-worker";
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
  rc = service->start(service, &error);
  assert_lc_ok(rc, &error);

  usleep(200000U);
  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "managed-later-state";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 3600L;
  enqueue_req.max_attempts = 3;
  source = source_from_text("stateful-work", &error);
  rc = producer->enqueue(producer, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);

  rc = service->wait(service, &error);
  assert_lc_ok(rc, &error);
  service->close(service);
  service = NULL;
  assert_int_equal(consumer_state.handled, 1U);
  assert_string_equal(consumer_state.queue, "managed-later-state");
  assert_string_equal(consumer_state.message_id, enqueue_res.message_id);

  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "managed-later-state";
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

static void test_pouch_public_consumer_service_failure_redelivery(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_source *source;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_consumer_config consumer_config;
  lc_consumer_service_config service_config;
  lc_consumer_service *service;
  pouch_consumer_failure_test consumer_state;
  lc_queue_stats_req stats_req;
  lc_queue_stats_res stats;
  lc_error error;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "consumer-failure");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  client = NULL;
  source = NULL;
  service = NULL;
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&consumer_state, 0, sizeof(consumer_state));
  memset(&stats, 0, sizeof(stats));

  open_pouch_client(endpoint, &client, &error);

  lc_enqueue_req_init(&enqueue_req);
  enqueue_req.queue = "managed-retry";
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 3600L;
  enqueue_req.max_attempts = 3;
  source = source_from_text("retry-work", &error);
  rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);

  lc_consumer_config_init(&consumer_config);
  lc_consumer_service_config_init(&service_config);
  consumer_config.request.queue = "managed-retry";
  consumer_config.request.owner = "managed-retry-worker";
  consumer_config.request.visibility_timeout_seconds = 30L;
  consumer_config.request.wait_seconds = 1L;
  consumer_config.handle = pouch_consumer_failure_handle;
  consumer_config.on_error = pouch_consumer_failure_on_error;
  consumer_config.context = &consumer_state;
  service_config.consumers = &consumer_config;
  service_config.consumer_count = 1U;
  rc = client->new_consumer_service(client, &service_config, &service, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(service);
  consumer_state.service = service;
  rc = service->run(service, &error);
  assert_lc_ok(rc, &error);
  service->close(service);
  service = NULL;

  assert_int_equal(consumer_state.handled, 2U);
  assert_int_equal(consumer_state.errors, 1U);
  assert_true(consumer_state.saw_delivery_error);
  assert_int_equal(consumer_state.first_attempts, 1L);
  assert_int_equal(consumer_state.redelivery_attempts, 2L);
  assert_int_equal(consumer_state.redelivery_failures, 1L);

  lc_queue_stats_req_init(&stats_req);
  stats_req.queue = "managed-retry";
  rc = client->queue_stats(client, &stats_req, &stats, &error);
  assert_lc_ok(rc, &error);
  assert_false(stats.available);
  assert_int_equal(stats.pending_candidates, 0);

  lc_queue_stats_res_cleanup(&stats);
  lc_enqueue_res_cleanup(&enqueue_res);
  client->close(client);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_acquire_for_update_stages_state(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_client *observer;
  lc_lease *lease;
  lc_source *source;
  lc_acquire_req acquire;
  lc_release_req release_req;
  lc_error error;
  pouch_acquire_for_update_test handler_state;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "acquire-for-update");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  client = NULL;
  observer = NULL;
  lease = NULL;
  source = NULL;
  memset(&handler_state, 0, sizeof(handler_state));

  open_pouch_client(endpoint, &client, &error);
  open_pouch_client(endpoint, &observer, &error);

  lc_acquire_req_init(&acquire);
  acquire.key = "integration/acquire-for-update-key";
  acquire.owner = "seed";
  acquire.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_lc_ok(rc, &error);
  source = source_from_text("{\"value\":1}", &error);
  rc = lease->update(lease, source, NULL, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);
  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  acquire.owner = "commit";
  handler_state.observer = observer;
  handler_state.key = "integration/acquire-for-update-key";
  handler_state.expected_snapshot = "{\"value\":1}";
  handler_state.expected_visible_during_update = "{\"value\":1}";
  handler_state.next_state = "{\"value\":2,\"via\":\"commit\"}";
  rc = lc_acquire_for_update(client, &acquire,
                             pouch_acquire_for_update_handler, &handler_state,
                             &error);
  assert_lc_ok(rc, &error);
  assert_true(handler_state.saw_snapshot);
  assert_true(handler_state.checked_staged_invisible);
  assert_client_state_text(client, "integration/acquire-for-update-key",
                           "{\"value\":2,\"via\":\"commit\"}", &error);

  acquire.owner = "rollback";
  rc = lc_acquire_for_update(client, &acquire,
                             pouch_acquire_for_update_failing_handler, NULL,
                             &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(error.code, LC_ERR_INVALID);
  assert_non_null(error.message);
  assert_string_equal(error.message,
                      "intentional pouch acquire_for_update failure");
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_client_state_text(client, "integration/acquire-for-update-key",
                           "{\"value\":2,\"via\":\"commit\"}", &error);

  client->close(client);
  observer->close(observer);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_acquire_for_update_creates_empty_state(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_client *observer;
  lc_acquire_req acquire;
  lc_error error;
  pouch_acquire_for_update_test handler_state;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "acquire-for-update-empty");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  client = NULL;
  observer = NULL;
  memset(&handler_state, 0, sizeof(handler_state));

  open_pouch_client(endpoint, &client, &error);
  open_pouch_client(endpoint, &observer, &error);

  lc_acquire_req_init(&acquire);
  acquire.key = "integration/acquire-for-update-empty-key";
  acquire.owner = "empty-commit";
  acquire.ttl_seconds = 60L;
  handler_state.observer = observer;
  handler_state.key = "integration/acquire-for-update-empty-key";
  handler_state.next_state = "{\"value\":1,\"via\":\"empty-commit\"}";
  rc = lc_acquire_for_update(client, &acquire,
                             pouch_acquire_for_update_empty_handler,
                             &handler_state, &error);
  assert_lc_ok(rc, &error);
  assert_true(handler_state.saw_snapshot);
  assert_true(handler_state.checked_staged_invisible);
  assert_client_state_text(client, "integration/acquire-for-update-empty-key",
                           "{\"value\":1,\"via\":\"empty-commit\"}", &error);

  memset(&handler_state, 0, sizeof(handler_state));
  acquire.key = "integration/acquire-for-update-empty-rollback-key";
  acquire.owner = "empty-rollback";
  rc = lc_acquire_for_update(client, &acquire,
                             pouch_acquire_for_update_failing_handler, NULL,
                             &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(error.code, LC_ERR_INVALID);
  assert_non_null(error.message);
  assert_string_equal(error.message,
                      "intentional pouch acquire_for_update failure");
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_client_state_empty(client,
                            "integration/acquire-for-update-empty-rollback-key",
                            &error);

  client->close(client);
  observer->close(observer);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_public_acquire_for_update_noop_releases_unchanged(
    void **state) {
  char root[256];
  char endpoint[320];
  lc_client *client;
  lc_client *observer;
  lc_lease *lease;
  lc_source *source;
  lc_acquire_req acquire;
  lc_release_req release_req;
  lc_error error;
  pouch_acquire_for_update_test handler_state;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "acquire-for-update-noop");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  client = NULL;
  observer = NULL;
  lease = NULL;
  source = NULL;
  memset(&handler_state, 0, sizeof(handler_state));

  open_pouch_client(endpoint, &client, &error);
  open_pouch_client(endpoint, &observer, &error);

  lc_acquire_req_init(&acquire);
  acquire.key = "integration/acquire-for-update-noop-key";
  acquire.owner = "seed";
  acquire.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_lc_ok(rc, &error);
  source = source_from_text("{\"value\":1}", &error);
  rc = lease->update(lease, source, NULL, &error);
  lc_source_close(source);
  assert_lc_ok(rc, &error);
  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  acquire.owner = "noop-existing";
  handler_state.observer = observer;
  handler_state.key = "integration/acquire-for-update-noop-key";
  handler_state.expected_snapshot = "{\"value\":1}";
  handler_state.expected_visible_during_update = "{\"value\":1}";
  rc = lc_acquire_for_update(client, &acquire,
                             pouch_acquire_for_update_noop_handler,
                             &handler_state, &error);
  assert_lc_ok(rc, &error);
  assert_true(handler_state.saw_snapshot);
  assert_true(handler_state.checked_staged_invisible);
  assert_client_state_text(client, "integration/acquire-for-update-noop-key",
                           "{\"value\":1}", &error);

  acquire.owner = "after-noop-existing";
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_lc_ok(rc, &error);
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  memset(&handler_state, 0, sizeof(handler_state));
  acquire.key = "integration/acquire-for-update-noop-empty-key";
  acquire.owner = "noop-empty";
  handler_state.observer = observer;
  handler_state.key = "integration/acquire-for-update-noop-empty-key";
  rc = lc_acquire_for_update(client, &acquire,
                             pouch_acquire_for_update_noop_handler,
                             &handler_state, &error);
  assert_lc_ok(rc, &error);
  assert_true(handler_state.saw_snapshot);
  assert_true(handler_state.checked_staged_invisible);
  assert_client_state_empty(client,
                            "integration/acquire-for-update-noop-empty-key",
                            &error);

  acquire.owner = "after-noop-empty";
  rc = client->acquire(client, &acquire, &lease, &error);
  assert_lc_ok(rc, &error);
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  client->close(client);
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
  assert_int_equal(get_res.version, 2L);
  assert_int_equal(reacquired->version, 2L);
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

static void test_pouch_public_mutate_local_shared_state(void **state) {
  char root[256];
  char endpoint[320];
  lc_client *writer;
  lc_client *reader;
  lc_lease *lease;
  lc_acquire_req acquire;
  lc_mutate_local_req mutate_req;
  lc_mutate_req lease_mutate_req;
  lc_mutate_op client_mutate_req;
  lc_mutate_res client_mutate_res;
  lc_get_res get_res;
  lc_release_req release_req;
  lc_sink *sink;
  lc_error error;
  const char *mutations[2];
  const char *lease_mutations[2];
  const char *client_mutations[1];
  char *text;
  int rc;

  (void)state;
  pouch_root_path(root, sizeof(root), "mutate-local");
  pouch_endpoint(endpoint, sizeof(endpoint), root);
  cleanup_pouch_root(root);
  lc_error_init(&error);
  writer = NULL;
  reader = NULL;
  lease = NULL;
  sink = NULL;
  text = NULL;
  memset(&get_res, 0, sizeof(get_res));
  memset(&client_mutate_res, 0, sizeof(client_mutate_res));

  open_pouch_client(endpoint, &writer, &error);
  lc_acquire_req_init(&acquire);
  acquire.namespace_name = "mutns";
  acquire.key = "integration/mutate-local";
  acquire.owner = "writer";
  acquire.ttl_seconds = 60L;
  rc = writer->acquire(writer, &acquire, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  mutations[0] = "/owner=\"writer\"";
  mutations[1] = "/attempts=1";
  lc_mutate_local_req_init(&mutate_req);
  mutate_req.mutations = mutations;
  mutate_req.mutation_count = 2U;
  rc = lease->mutate_local(lease, &mutate_req, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(lease->version, 1L);
  assert_non_null(lease->state_etag);

  lease_mutations[0] = "/attempts++";
  lease_mutations[1] = "/kind=\"lease-mutate\"";
  lc_mutate_req_init(&lease_mutate_req);
  lease_mutate_req.mutations = lease_mutations;
  lease_mutate_req.mutation_count = 2U;
  rc = lease->mutate(lease, &lease_mutate_req, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(lease->version, 2L);
  assert_non_null(lease->state_etag);

  lease_mutate_req.if_version = lease->version + 100L;
  lease_mutate_req.has_if_version = 1;
  rc = lease->mutate(lease, &lease_mutate_req, &error);
  assert_lc_server_error(rc, &error, 412L);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  client_mutations[0] = "/client_mutated=true";
  memset(&client_mutate_req, 0, sizeof(client_mutate_req));
  client_mutate_req.lease.namespace_name = lease->namespace_name;
  client_mutate_req.lease.key = lease->key;
  client_mutate_req.lease.lease_id = lease->lease_id;
  client_mutate_req.lease.txn_id = lease->txn_id;
  client_mutate_req.lease.fencing_token = lease->fencing_token;
  client_mutate_req.mutations = client_mutations;
  client_mutate_req.mutation_count = 1U;
  client_mutate_req.if_version = lease->version;
  client_mutate_req.has_if_version = 1;
  rc = writer->mutate(writer, &client_mutate_req, &client_mutate_res, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(client_mutate_res.new_version, 3L);
  assert_non_null(client_mutate_res.new_state_etag);
  lc_mutate_res_cleanup(&client_mutate_res);

  open_pouch_client_with_namespace(endpoint, "mutns", &reader, &error);
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = reader->get(reader, "integration/mutate-local", NULL, sink, &get_res,
                   &error);
  assert_lc_ok(rc, &error);
  assert_false(get_res.no_content);
  assert_string_equal(get_res.content_type, "application/json");
  assert_int_equal(get_res.version, 3L);
  text = sink_text(sink, &error);
  assert_non_null(strstr(text, "\"owner\":\"writer\""));
  assert_non_null(strstr(text, "\"attempts\":2"));
  assert_non_null(strstr(text, "\"kind\":\"lease-mutate\""));
  assert_non_null(strstr(text, "\"client_mutated\":true"));
  free(text);
  text = NULL;
  lc_sink_close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);

  lc_release_req_init(&release_req);
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;
  reader->close(reader);
  reader = NULL;
  writer->close(writer);
  writer = NULL;
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_pouch_public_state_attachment_shared_handles),
      cmocka_unit_test(test_pouch_public_metadata_query_hidden_persists),
      cmocka_unit_test(
          test_pouch_public_scan_query_documents_replays_after_reopen),
      cmocka_unit_test(
          test_pouch_public_scan_query_can_be_configured_by_endpoint),
      cmocka_unit_test(
          test_pouch_public_scan_query_endpoint_uses_index_fallback_for_refresh),
      cmocka_unit_test(
          test_pouch_public_scan_query_keys_can_be_configured_by_endpoint),
      cmocka_unit_test(
          test_pouch_public_scan_query_documents_refreshes_open_reader),
      cmocka_unit_test(
          test_pouch_public_scan_query_keys_refreshes_open_reader),
      cmocka_unit_test(
          test_pouch_public_index_query_documents_replays_after_reopen),
      cmocka_unit_test(
          test_pouch_public_index_query_documents_refreshes_open_reader),
      cmocka_unit_test(
          test_pouch_public_index_query_keys_refreshes_open_reader),
      cmocka_unit_test(test_pouch_public_flush_index_refreshes_open_reader),
      cmocka_unit_test(
          test_pouch_public_attachment_survives_compaction_reopen),
      cmocka_unit_test(test_pouch_public_attachment_delete_semantics),
      cmocka_unit_test(test_pouch_public_attachment_prevent_overwrite),
      cmocka_unit_test(test_pouch_public_client_level_attachment_apis),
      cmocka_unit_test(test_pouch_public_attachment_read_after_release),
      cmocka_unit_test(test_pouch_public_queue_shared_handles),
      cmocka_unit_test(
          test_pouch_public_queue_initial_delay_hides_until_visible),
      cmocka_unit_test(test_pouch_public_queue_ttl_expiry_removes_candidate),
      cmocka_unit_test(test_pouch_public_queue_visibility_redelivery),
      cmocka_unit_test(test_pouch_public_queue_nack_delay_redelivery),
      cmocka_unit_test(test_pouch_public_queue_retry_exhaustion_terminal),
      cmocka_unit_test(test_pouch_public_queue_stats_is_read_only),
      cmocka_unit_test(
          test_pouch_public_queue_batch_no_duplicate_acked_delivery),
      cmocka_unit_test(
          test_pouch_public_queue_batch_honors_start_after_cursor),
      cmocka_unit_test(
          test_pouch_public_dequeue_with_state_honors_start_after_cursor),
      cmocka_unit_test(test_pouch_public_watch_queue_snapshots),
      cmocka_unit_test(test_pouch_public_subscribe_with_state),
      cmocka_unit_test(test_pouch_public_dequeue_waits_for_later_enqueue),
      cmocka_unit_test(test_pouch_public_subscribe_waits_for_later_enqueue),
      cmocka_unit_test(test_pouch_public_consumer_service_with_state),
      cmocka_unit_test(
          test_pouch_public_consumer_service_start_wait_with_state),
      cmocka_unit_test(
          test_pouch_public_consumer_service_polls_later_enqueue),
      cmocka_unit_test(test_pouch_public_consumer_service_failure_redelivery),
      cmocka_unit_test(test_pouch_public_acquire_for_update_stages_state),
      cmocka_unit_test(
          test_pouch_public_acquire_for_update_creates_empty_state),
      cmocka_unit_test(
          test_pouch_public_acquire_for_update_noop_releases_unchanged),
      cmocka_unit_test(test_pouch_public_cas_across_clients),
      cmocka_unit_test(test_pouch_public_remove_recreate_semantics),
      cmocka_unit_test(test_pouch_public_mutate_local_shared_state),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <cmocka.h>

#include "lc/lc.h"
#include "lc_api_internal.h"
#include "lc_log.h"

typedef struct tracked_alloc_header {
  size_t size;
} tracked_alloc_header;

typedef struct tracked_allocator_state {
  pthread_mutex_t mutex;
  size_t live_allocations;
  size_t total_allocations;
} tracked_allocator_state;

typedef struct consumer_test_state {
  lc_consumer_service *service;
  pthread_mutex_t mutex;
  size_t expected_messages;
  size_t handled_messages;
  size_t subscribe_calls;
  int saw_logger;
  int fail_subscribe;
  int stop_requested;
  FILE *log_fp;
} consumer_test_state;

typedef struct fake_service {
  lc_consumer_service pub;
  int run_calls;
  int start_calls;
  int stop_calls;
  int wait_calls;
  int close_calls;
} fake_service;

typedef struct fake_client {
  lc_client pub;
  int new_consumer_service_calls;
  lc_consumer_service *service_to_return;
} fake_client;

static consumer_test_state *g_consumer_test_state = NULL;
static lc_allocator g_test_allocator;
static pslog_logger *g_test_client_logger = NULL;

struct lc_consumer_service_handle {
  lc_consumer_service pub;
  char **endpoints;
  size_t endpoint_count;
  char *unix_socket_path;
  char *client_bundle_path;
  char *default_namespace;
  long timeout_ms;
  int disable_mtls;
  int insecure_skip_verify;
  int prefer_http_2;
  int disable_logger_sys_field;
  pslog_logger *base_logger;
  pslog_logger *logger;
};

static int fake_service_run(lc_consumer_service *self, lc_error *error) {
  fake_service *service;
  (void)error;
  service = (fake_service *)self;
  service->run_calls += 1;
  return LC_OK;
}

static int fake_service_start(lc_consumer_service *self, lc_error *error) {
  fake_service *service;
  (void)error;
  service = (fake_service *)self;
  service->start_calls += 1;
  return LC_OK;
}

static int fake_service_stop(lc_consumer_service *self) {
  fake_service *service;
  service = (fake_service *)self;
  service->stop_calls += 1;
  return LC_OK;
}

static int fake_service_wait(lc_consumer_service *self, lc_error *error) {
  fake_service *service;
  (void)error;
  service = (fake_service *)self;
  service->wait_calls += 1;
  return LC_OK;
}

static void fake_service_close(lc_consumer_service *self) {
  fake_service *service;
  service = (fake_service *)self;
  service->close_calls += 1;
}

static int
fake_client_new_consumer_service(lc_client *self,
                                 const lc_consumer_service_config *config,
                                 lc_consumer_service **out, lc_error *error) {
  fake_client *client;

  (void)config;
  (void)error;
  client = (fake_client *)self;
  client->new_consumer_service_calls += 1;
  *out = client->service_to_return;
  return LC_OK;
}

static void *tracked_malloc(void *context, size_t size) {
  tracked_alloc_header *header;
  tracked_allocator_state *state;

  state = (tracked_allocator_state *)context;
  header = (tracked_alloc_header *)malloc(sizeof(*header) + size);
  if (header == NULL) {
    return NULL;
  }
  header->size = size;
  pthread_mutex_lock(&state->mutex);
  state->live_allocations += 1U;
  state->total_allocations += 1U;
  pthread_mutex_unlock(&state->mutex);
  return (void *)(header + 1);
}

static void tracked_free(void *context, void *ptr) {
  tracked_alloc_header *header;
  tracked_allocator_state *state;

  if (ptr == NULL) {
    return;
  }
  state = (tracked_allocator_state *)context;
  header = ((tracked_alloc_header *)ptr) - 1;
  pthread_mutex_lock(&state->mutex);
  assert_true(state->live_allocations > 0U);
  state->live_allocations -= 1U;
  pthread_mutex_unlock(&state->mutex);
  free(header);
}

static void *tracked_realloc(void *context, void *ptr, size_t size) {
  tracked_alloc_header *header;
  tracked_alloc_header *next;

  if (ptr == NULL) {
    return tracked_malloc(context, size);
  }
  if (size == 0U) {
    tracked_free(context, ptr);
    return NULL;
  }
  header = ((tracked_alloc_header *)ptr) - 1;
  next = (tracked_alloc_header *)realloc(header, sizeof(*header) + size);
  if (next == NULL) {
    return NULL;
  }
  next->size = size;
  return (void *)(next + 1);
}

static void tracked_allocator_state_init(tracked_allocator_state *state) {
  memset(state, 0, sizeof(*state));
  pthread_mutex_init(&state->mutex, NULL);
}

static void tracked_allocator_state_cleanup(tracked_allocator_state *state) {
  pthread_mutex_destroy(&state->mutex);
}

static void tracked_allocator_init(lc_allocator *allocator,
                                   tracked_allocator_state *state) {
  lc_allocator_init(allocator);
  allocator->malloc_fn = tracked_malloc;
  allocator->realloc_fn = tracked_realloc;
  allocator->free_fn = tracked_free;
  allocator->context = state;
}

static void fake_test_client_close(lc_client *self) {
  lc_client_handle *client;

  client = (lc_client_handle *)self;
  lc_free_with_allocator(&client->allocator, client);
}

static int fake_clone_client(lc_consumer_service_handle *service,
                             lc_client **out, lc_error *error) {
  lc_client_handle *client;
  (void)service;

  client = (lc_client_handle *)lc_calloc_with_allocator(&g_test_allocator, 1U,
                                                        sizeof(*client));
  if (client == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate fake worker client", NULL, NULL,
                        NULL);
  }
  client->allocator = g_test_allocator;
  client->base_logger = g_test_client_logger != NULL ? g_test_client_logger
                                                     : lc_log_noop_logger();
  client->logger = g_test_client_logger != NULL ? g_test_client_logger
                                                : lc_log_noop_logger();
  client->pub.close = fake_test_client_close;
  *out = &client->pub;
  return LC_OK;
}

static int handle_consumer_message(void *context, lc_consumer_message *message,
                                   lc_error *error) {
  consumer_test_state *state;
  int (*stop_fn)(lc_consumer_service *);
  int rc;

  state = (consumer_test_state *)context;
  assert_non_null(message->logger);
  pthread_mutex_lock(&state->mutex);
  state->saw_logger = 1;
  pthread_mutex_unlock(&state->mutex);
  rc = message->message->ack(message->message, error);
  if (rc != LC_OK) {
    return rc;
  }
  pthread_mutex_lock(&state->mutex);
  state->handled_messages += 1U;
  if (state->handled_messages >= state->expected_messages &&
      state->service != NULL) {
    stop_fn = state->service->stop;
    stop_fn(state->service);
  }
  pthread_mutex_unlock(&state->mutex);
  return LC_OK;
}

static pslog_logger *open_test_logger(FILE **out_fp) {
  FILE *fp;
  pslog_config config;

  fp = tmpfile();
  if (fp == NULL) {
    return NULL;
  }
  pslog_default_config(&config);
  config.mode = PSLOG_MODE_JSON;
  config.min_level = PSLOG_LEVEL_TRACE;
  config.timestamps = 0;
  config.verbose_fields = 1;
  config.output = pslog_output_from_fp(fp, 0);
  *out_fp = fp;
  return pslog_new(&config);
}

static char *read_stream_text(FILE *fp) {
  long length;
  char *buffer;

  if (fp == NULL || fflush(fp) != 0 || fseek(fp, 0L, SEEK_END) != 0) {
    return NULL;
  }
  length = ftell(fp);
  if (length < 0L || fseek(fp, 0L, SEEK_SET) != 0) {
    return NULL;
  }
  buffer = (char *)calloc((size_t)length + 1U, 1U);
  if (buffer == NULL) {
    return NULL;
  }
  if (length > 0L && fread(buffer, 1U, (size_t)length, fp) != (size_t)length) {
    free(buffer);
    return NULL;
  }
  buffer[length] = '\0';
  return buffer;
}

static int fake_subscribe(lc_consumer_service_handle *service,
                          const lc_dequeue_req *request, int with_state,
                          lc_client_handle *client,
                          const lc_engine_queue_stream_handler *handler,
                          void *handler_context,
                          lc_engine_error *engine_error) {
  consumer_test_state *state;
  lc_engine_dequeue_response delivery;
  char message_id[64];
  int accepted;
  int completed;

  (void)service;
  (void)client;
  state = g_consumer_test_state;
  pthread_mutex_lock(&state->mutex);
  state->subscribe_calls += 1U;
  snprintf(message_id, sizeof(message_id), "msg-%lu",
           (unsigned long)state->subscribe_calls);
  pthread_mutex_unlock(&state->mutex);

  if (state->fail_subscribe) {
    lc_engine_error_init(engine_error);
    engine_error->code = LC_ENGINE_ERROR_TRANSPORT;
    engine_error->message = lc_strdup_local("synthetic subscribe failure");
    return LC_ENGINE_ERROR_TRANSPORT;
  }

  memset(&delivery, 0, sizeof(delivery));
  delivery.namespace_name = (char *)request->namespace_name;
  delivery.queue = (char *)request->queue;
  delivery.message_id = message_id;
  delivery.payload_content_type = "application/json";
  delivery.lease_id = "lease-1";
  delivery.meta_etag = "meta-1";
  delivery.fencing_token = 11L;
  delivery.visibility_timeout_seconds = request->visibility_timeout_seconds;
  if (with_state) {
    delivery.state_lease_id = "state-lease-1";
    delivery.state_etag = "state-etag-1";
    delivery.state_fencing_token = 17L;
  }

  accepted = handler->begin(handler_context, &delivery, engine_error);
  if (!accepted) {
    return LC_ENGINE_ERROR_TRANSPORT;
  }
  completed =
      handler->chunk(handler_context, "{\"ok\":true}", 11U, engine_error);
  if (!completed) {
    return LC_ENGINE_ERROR_TRANSPORT;
  }
  if (!handler->end(handler_context, &delivery, engine_error)) {
    return LC_ENGINE_ERROR_TRANSPORT;
  }
  return LC_ENGINE_OK;
}

static int fake_subscribe_until_stop(
    lc_consumer_service_handle *service, const lc_dequeue_req *request,
    int with_state, lc_client_handle *client,
    const lc_engine_queue_stream_handler *handler, void *handler_context,
    lc_engine_error *engine_error) {
  consumer_test_state *state;

  (void)service;
  (void)request;
  (void)with_state;
  (void)client;
  (void)handler;
  (void)handler_context;
  (void)engine_error;
  state = g_consumer_test_state;
  pthread_mutex_lock(&state->mutex);
  state->subscribe_calls += 1U;
  pthread_mutex_unlock(&state->mutex);
  for (;;) {
    pthread_mutex_lock(&state->mutex);
    if (state->stop_requested) {
      pthread_mutex_unlock(&state->mutex);
      break;
    }
    pthread_mutex_unlock(&state->mutex);
    usleep(1000);
  }
  return LC_ENGINE_OK;
}

static void init_fake_root_client(lc_client_handle *client,
                                  const lc_allocator *allocator) {
  memset(client, 0, sizeof(*client));
  client->allocator = *allocator;
  client->default_namespace = "default";
  client->base_logger = lc_log_noop_logger();
  client->logger = lc_log_noop_logger();
}

static void test_consumer_restart_policy_init_sets_defaults(void **state) {
  lc_consumer_restart_policy policy;

  (void)state;
  memset(&policy, 0xff, sizeof(policy));
  lc_consumer_restart_policy_init(&policy);
  assert_int_equal(policy.immediate_retries, 3);
  assert_int_equal(policy.base_delay_ms, 250L);
  assert_int_equal(policy.max_delay_ms, 300000L);
  assert_true(policy.multiplier > 1.0);
  assert_int_equal(policy.jitter_ms, 0L);
  assert_int_equal(policy.max_failures, 0);
}

static void test_consumer_config_inits_are_zero_safe(void **state) {
  lc_consumer_config config;
  lc_consumer_service_config service_config;

  (void)state;
  memset(&config, 0xff, sizeof(config));
  memset(&service_config, 0xff, sizeof(service_config));
  lc_consumer_config_init(&config);
  lc_consumer_service_config_init(&service_config);
  assert_null(config.name);
  assert_null(config.request.queue);
  assert_int_equal(config.worker_count, 1U);
  assert_int_equal(config.with_state, 0);
  assert_null(config.handle);
  assert_int_equal(config.restart_policy.immediate_retries, 3);
  assert_null(service_config.consumers);
  assert_int_equal(service_config.consumer_count, 0U);
}

static void test_consumer_service_wrappers_delegate(void **state) {
  fake_service service;
  fake_client client;
  lc_consumer_service_config config;
  lc_consumer_service *out;
  lc_error error;
  int rc;

  (void)state;
  memset(&service, 0, sizeof(service));
  memset(&client, 0, sizeof(client));
  lc_consumer_service_config_init(&config);
  out = NULL;
  lc_error_init(&error);

  service.pub.run = fake_service_run;
  service.pub.start = fake_service_start;
  service.pub.stop = fake_service_stop;
  service.pub.wait = fake_service_wait;
  service.pub.close = fake_service_close;
  client.pub.new_consumer_service = fake_client_new_consumer_service;
  client.service_to_return = &service.pub;

  rc = lc_client_new_consumer_service(&client.pub, &config, &out, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(client.new_consumer_service_calls, 1);
  assert_ptr_equal(out, &service.pub);

  rc = lc_consumer_service_run(out, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_consumer_service_start(out, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_consumer_service_stop(out);
  assert_int_equal(rc, LC_OK);
  rc = lc_consumer_service_wait(out, &error);
  assert_int_equal(rc, LC_OK);
  lc_consumer_service_close(out);

  assert_int_equal(service.run_calls, 1);
  assert_int_equal(service.start_calls, 1);
  assert_int_equal(service.stop_calls, 1);
  assert_int_equal(service.wait_calls, 1);
  assert_int_equal(service.close_calls, 1);
  lc_error_cleanup(&error);
}

static void
test_consumer_service_multi_worker_releases_tracked_allocations(void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  lc_error error;
  int rc;

  (void)state;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_allocator = allocator;
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.handle = handle_consumer_message;
  consumer.worker_count = 3U;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(service);
  service->close(service);
  assert_int_equal(error.code, LC_OK);
  assert_true(alloc_state.total_allocations > 0U);
  assert_int_equal(alloc_state.live_allocations, 0U);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void
test_consumer_service_multi_worker_runs_without_transport(void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  int rc;

  (void)state;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_allocator = allocator;
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.handle = handle_consumer_message;
  consumer.worker_count = 3U;
  consumer.context = &runtime_state;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  g_consumer_test_state = &runtime_state;
  lc_consumer_service_set_test_hooks(service, fake_clone_client,
                                     fake_subscribe_until_stop);

  rc = service->start(service, &error);
  if (rc != LC_OK) {
    fail_msg("service start failed rc=%d err=%d msg=%s", rc, error.code,
             error.message != NULL ? error.message : "(null)");
  }
  usleep(10000);
  pthread_mutex_lock(&runtime_state.mutex);
  runtime_state.stop_requested = 1;
  pthread_mutex_unlock(&runtime_state.mutex);
  {
    int (*stop_fn)(lc_consumer_service *);
    stop_fn = service->stop;
    rc = stop_fn(service);
  }
  assert_int_equal(rc, LC_OK);
  rc = service->wait(service, &error);
  if (rc != LC_OK) {
    fail_msg("service wait failed rc=%d err=%d msg=%s", rc, error.code,
             error.message != NULL ? error.message : "(null)");
  }
  assert_true(runtime_state.subscribe_calls >= 3U);

  service->close(service);
  g_consumer_test_state = NULL;
  assert_true(alloc_state.total_allocations > 0U);
  assert_int_equal(alloc_state.live_allocations, 0U);

  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void
test_consumer_service_logs_restart_with_configured_logger(void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  pslog_logger *logger;
  char *logs;
  int rc;

  (void)state;
  logger = NULL;
  logs = NULL;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_allocator = allocator;
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  logger = open_test_logger(&runtime_state.log_fp);
  assert_non_null(logger);
  client.base_logger = logger;
  client.logger = lc_log_client_logger(logger, 0);
  assert_non_null(client.logger);
  g_test_client_logger = client.logger;
  runtime_state.fail_subscribe = 1;

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.handle = handle_consumer_message;
  consumer.worker_count = 1U;
  consumer.context = &runtime_state;
  consumer.restart_policy.immediate_retries = 0;
  consumer.restart_policy.base_delay_ms = 1L;
  consumer.restart_policy.max_delay_ms = 1L;
  consumer.restart_policy.max_failures = 1;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  g_consumer_test_state = &runtime_state;
  lc_consumer_service_set_test_hooks(service, fake_clone_client,
                                     fake_subscribe);

  rc = service->run(service, &error);
  assert_int_not_equal(rc, LC_OK);

  service->close(service);
  g_consumer_test_state = NULL;
  g_test_client_logger = NULL;
  client.logger->destroy(client.logger);
  logger->destroy(logger);
  logs = read_stream_text(runtime_state.log_fp);
  assert_non_null(logs);
  assert_non_null(strstr(logs, "\"message\":\"client.consumer.restart\""));
  assert_non_null(strstr(logs, "\"consumer\":\"worker-test\""));
  assert_non_null(strstr(logs, "\"queue\":\"jobs\""));
  assert_non_null(strstr(logs, "\"sys\":\"client.lockd\""));

  free(logs);
  fclose(runtime_state.log_fp);
  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void test_consumer_service_logs_subscribe_lifecycle(void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  pslog_logger *logger;
  char *logs;
  int rc;

  (void)state;
  logger = NULL;
  logs = NULL;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_allocator = allocator;
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  logger = open_test_logger(&runtime_state.log_fp);
  assert_non_null(logger);
  client.base_logger = logger;
  client.logger = lc_log_client_logger(logger, 0);
  assert_non_null(client.logger);
  g_test_client_logger = client.logger;

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.request.owner = "worker-owner";
  consumer.handle = handle_consumer_message;
  consumer.worker_count = 1U;
  consumer.context = &runtime_state;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  g_consumer_test_state = &runtime_state;
  lc_consumer_service_set_test_hooks(service, fake_clone_client,
                                     fake_subscribe_until_stop);

  rc = service->start(service, &error);
  assert_int_equal(rc, LC_OK);
  usleep(10000);
  pthread_mutex_lock(&runtime_state.mutex);
  runtime_state.stop_requested = 1;
  pthread_mutex_unlock(&runtime_state.mutex);
  {
    int (*stop_fn)(lc_consumer_service *);

    stop_fn = service->stop;
    rc = stop_fn(service);
  }
  assert_int_equal(rc, LC_OK);
  rc = service->wait(service, &error);
  assert_int_equal(rc, LC_OK);

  service->close(service);
  g_consumer_test_state = NULL;
  g_test_client_logger = NULL;
  client.logger->destroy(client.logger);
  logger->destroy(logger);
  logs = read_stream_text(runtime_state.log_fp);
  assert_non_null(logs);
  assert_non_null(strstr(logs, "\"message\":\"client.queue.subscribe.begin\""));
  assert_non_null(strstr(logs, "\"consumer\":\"worker-test\""));
  assert_non_null(strstr(logs, "\"queue\":\"jobs\""));
  assert_non_null(strstr(logs, "\"owner\":\"worker-owner\""));
  assert_non_null(strstr(logs, "\"sys\":\"client.lockd\""));

  free(logs);
  fclose(runtime_state.log_fp);
  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void test_consumer_service_retains_owned_logger_after_root_client_close(
    void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_service_config config;
  lc_consumer_config consumer;
  lc_consumer_service *service;
  lc_consumer_service_handle *service_handle;
  pslog_logger *base_logger;
  FILE *log_fp;
  char *logs;
  lc_error error;
  int rc;

  (void)state;
  service = NULL;
  base_logger = NULL;
  log_fp = NULL;
  logs = NULL;
  memset(&client, 0, sizeof(client));
  memset(&config, 0, sizeof(config));
  memset(&consumer, 0, sizeof(consumer));
  memset(&error, 0, sizeof(error));
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  init_fake_root_client(&client, &allocator);

  base_logger = open_test_logger(&log_fp);
  assert_non_null(base_logger);
  client.base_logger = base_logger;
  client.logger = lc_log_client_logger(base_logger, 0);
  assert_non_null(client.logger);

  lc_consumer_service_config_init(&config);
  lc_consumer_config_init(&consumer);
  consumer.request.queue = "jobs";
  consumer.handle = handle_consumer_message;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(service);
  service_handle = (lc_consumer_service_handle *)service;
  assert_ptr_equal(service_handle->base_logger, base_logger);
  assert_non_null(service_handle->logger);
  assert_ptr_not_equal(service_handle->logger, client.logger);

  client.logger->destroy(client.logger);
  client.logger = NULL;
  lc_log_info(service_handle->logger, "consumer.service.logger.still.valid",
              NULL, 0U);

  lc_consumer_service_close(service);
  base_logger->destroy(base_logger);
  logs = read_stream_text(log_fp);
  assert_non_null(logs);
  assert_non_null(
      strstr(logs, "\"message\":\"consumer.service.logger.still.valid\""));

  free(logs);
  fclose(log_fp);
  tracked_allocator_state_cleanup(&alloc_state);
}

static void test_consumer_service_multi_worker_failure_cleans_up(void **state) {
  tracked_allocator_state alloc_state;
  lc_allocator allocator;
  lc_client_handle client;
  lc_consumer_config consumer;
  lc_consumer_service_config config;
  lc_consumer_service *service;
  consumer_test_state runtime_state;
  lc_error error;
  int rc;

  (void)state;
  tracked_allocator_state_init(&alloc_state);
  tracked_allocator_init(&allocator, &alloc_state);
  g_test_allocator = allocator;
  g_test_client_logger = NULL;
  init_fake_root_client(&client, &allocator);
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&config);
  lc_error_init(&error);
  memset(&runtime_state, 0, sizeof(runtime_state));
  pthread_mutex_init(&runtime_state.mutex, NULL);
  runtime_state.fail_subscribe = 1;

  consumer.name = "worker-test";
  consumer.request.queue = "jobs";
  consumer.handle = handle_consumer_message;
  consumer.worker_count = 2U;
  consumer.context = &runtime_state;
  consumer.restart_policy.max_failures = 1;
  config.consumers = &consumer;
  config.consumer_count = 1U;

  rc = lc_client_new_consumer_service_method(&client.pub, &config, &service,
                                             &error);
  assert_int_equal(rc, LC_OK);
  runtime_state.service = service;
  g_consumer_test_state = &runtime_state;
  lc_consumer_service_set_test_hooks(service, fake_clone_client,
                                     fake_subscribe);

  rc = service->run(service, &error);
  assert_int_not_equal(rc, LC_OK);
  assert_true(error.code != LC_OK);

  service->close(service);
  g_consumer_test_state = NULL;
  assert_int_equal(alloc_state.live_allocations, 0U);

  pthread_mutex_destroy(&runtime_state.mutex);
  lc_error_cleanup(&error);
  tracked_allocator_state_cleanup(&alloc_state);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_consumer_restart_policy_init_sets_defaults),
      cmocka_unit_test(test_consumer_config_inits_are_zero_safe),
      cmocka_unit_test(test_consumer_service_wrappers_delegate),
      cmocka_unit_test(
          test_consumer_service_multi_worker_releases_tracked_allocations),
      cmocka_unit_test(
          test_consumer_service_multi_worker_runs_without_transport),
      cmocka_unit_test(test_consumer_service_multi_worker_failure_cleans_up),
      cmocka_unit_test(
          test_consumer_service_retains_owned_logger_after_root_client_close),
      cmocka_unit_test(
          test_consumer_service_logs_restart_with_configured_logger),
      cmocka_unit_test(test_consumer_service_logs_subscribe_lifecycle),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}

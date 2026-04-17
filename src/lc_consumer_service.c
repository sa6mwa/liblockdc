#include "lc_api_internal.h"
#include "lc_internal.h"
#include "lc_log.h"

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define LC_CONSUMER_MAX_HOST 256

typedef struct lc_consumer_worker_config {
  lc_allocator allocator;
  char *name;
  lc_dequeue_req request;
  int with_state;
  int (*handle)(void *context, lc_consumer_message *message, lc_error *error);
  int (*on_error)(void *context, const lc_consumer_error *event,
                  lc_error *error);
  void (*on_start)(void *context, const lc_consumer_lifecycle_event *event);
  void (*on_stop)(void *context, const lc_consumer_lifecycle_event *event);
  void *context;
  lc_consumer_restart_policy restart_policy;
} lc_consumer_worker_config;

typedef struct lc_consumer_worker_state lc_consumer_worker_state;

typedef enum lc_consumer_delivery_state {
  LC_CONSUMER_DELIVERY_STATE_OPEN = 0,
  LC_CONSUMER_DELIVERY_STATE_EXTENDER_STOPPED = 1,
  LC_CONSUMER_DELIVERY_STATE_TERMINAL_IN_PROGRESS = 2,
  LC_CONSUMER_DELIVERY_STATE_TERMINAL = 3
} lc_consumer_delivery_state;

typedef struct lc_consumer_delivery_bridge {
  lc_consumer_worker_state *worker;
  lc_client_handle *client;
  lc_error *error;
  lc_engine_dequeue_response meta;
  lc_stream_pipe *pipe;
  pthread_t handler_thread;
  pthread_t extend_thread;
  int handler_thread_started;
  int extend_thread_started;
  int state_mutex_initialized;
  int state_cond_initialized;
  int op_mutex_initialized;
  int handler_rc;
  int runtime_fatal;
  lc_consumer_delivery_state state;
  int handler_done;
  pthread_mutex_t state_mutex;
  pthread_mutex_t op_mutex;
  pthread_cond_t state_cond;
  lc_message *message;
  lc_message *inner_message;
} lc_consumer_delivery_bridge;

typedef struct lc_consumer_runtime_message {
  lc_message pub;
  lc_consumer_delivery_bridge *bridge;
  lc_message *inner;
} lc_consumer_runtime_message;

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
  size_t http_json_response_limit_bytes;
  int disable_logger_sys_field;
  pslog_logger *base_logger;
  pslog_logger *logger;
  int owns_logger;
  lc_allocator allocator;
  lc_consumer_worker_state *workers;
  size_t worker_count;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  size_t active_workers;
  int started_once;
  int stop_requested;
  int joined;
  int fatal_rc;
  lc_error fatal_error;
};

struct lc_consumer_worker_state {
  lc_consumer_service_handle *service;
  lc_consumer_worker_config config;
  pthread_t thread;
  int thread_started;
};

static pthread_mutex_t lc_consumer_owner_mutex = PTHREAD_MUTEX_INITIALIZER;
static long lc_consumer_owner_seq = 0L;

static void *lc_consumer_delivery_handler_main(void *context);
static void *lc_consumer_delivery_extend_main(void *context);
static int lc_consumer_is_stop_requested(lc_consumer_service_handle *service);
static void
lc_consumer_delivery_stop_extender(lc_consumer_delivery_bridge *bridge);
static lc_message *
lc_consumer_runtime_message_wrap(lc_consumer_delivery_bridge *bridge,
                                 lc_message *inner, lc_error *error);
static void lc_consumer_delivery_request_extender_stop(
    lc_consumer_delivery_bridge *bridge);
static int
lc_consumer_delivery_state_is_terminal(lc_consumer_delivery_state state);

static void lc_consumer_timespec_add_ms(struct timespec *ts, long delay_ms) {
  if (ts == NULL || delay_ms <= 0L) {
    return;
  }
  ts->tv_sec += delay_ms / 1000L;
  ts->tv_nsec += (delay_ms % 1000L) * 1000000L;
  if (ts->tv_nsec >= 1000000000L) {
    ts->tv_sec += ts->tv_nsec / 1000000000L;
    ts->tv_nsec %= 1000000000L;
  }
}

static long lc_consumer_auto_extend_delay_ms(long visibility_timeout_seconds) {
  long delay_ms;

  if (visibility_timeout_seconds <= 0L) {
    return 0L;
  }
  delay_ms = (visibility_timeout_seconds * 1000L) / 2L;
  if (delay_ms < 250L) {
    delay_ms = 250L;
  }
  if (delay_ms > 30000L) {
    delay_ms = 30000L;
  }
  return delay_ms;
}

static int
lc_consumer_delivery_state_is_terminal(lc_consumer_delivery_state state) {
  return state == LC_CONSUMER_DELIVERY_STATE_TERMINAL;
}

static int
lc_consumer_delivery_state_allows_extend(lc_consumer_delivery_state state) {
  return state == LC_CONSUMER_DELIVERY_STATE_OPEN;
}

static int lc_consumer_delivery_state_allows_terminal_action(
    lc_consumer_delivery_state state) {
  return state == LC_CONSUMER_DELIVERY_STATE_OPEN ||
         state == LC_CONSUMER_DELIVERY_STATE_EXTENDER_STOPPED;
}

static void lc_consumer_delivery_mark_terminal(lc_consumer_delivery_bridge *bridge) {
  pthread_mutex_lock(&bridge->state_mutex);
  bridge->state = LC_CONSUMER_DELIVERY_STATE_TERMINAL;
  pthread_cond_broadcast(&bridge->state_cond);
  pthread_mutex_unlock(&bridge->state_mutex);
}

static void
lc_consumer_delivery_mark_handler_done(lc_consumer_delivery_bridge *bridge) {
  pthread_mutex_lock(&bridge->state_mutex);
  bridge->handler_done = 1;
  pthread_cond_broadcast(&bridge->state_cond);
  pthread_mutex_unlock(&bridge->state_mutex);
}

static void
lc_consumer_delivery_request_extender_stop(lc_consumer_delivery_bridge *bridge) {
  pthread_mutex_lock(&bridge->state_mutex);
  if (lc_consumer_delivery_state_allows_extend(bridge->state)) {
    bridge->state = LC_CONSUMER_DELIVERY_STATE_EXTENDER_STOPPED;
  }
  pthread_cond_broadcast(&bridge->state_cond);
  pthread_mutex_unlock(&bridge->state_mutex);
}

static void
lc_consumer_delivery_stop_extender(lc_consumer_delivery_bridge *bridge) {
  pthread_t extend_thread;
  int should_join;

  if (bridge == NULL || !bridge->state_mutex_initialized) {
    return;
  }
  lc_consumer_delivery_request_extender_stop(bridge);
  pthread_mutex_lock(&bridge->state_mutex);
  should_join = bridge->extend_thread_started;
  extend_thread = bridge->extend_thread;
  bridge->extend_thread_started = 0;
  pthread_mutex_unlock(&bridge->state_mutex);
  if (should_join) {
    pthread_join(extend_thread, NULL);
  }
}

static void
lc_consumer_delivery_cleanup(lc_consumer_delivery_bridge *bridge) {
  if (bridge == NULL) {
    return;
  }
  if (bridge->handler_thread_started) {
    pthread_join(bridge->handler_thread, NULL);
    bridge->handler_thread_started = 0;
  }
  lc_consumer_delivery_stop_extender(bridge);
  if (bridge->message != NULL) {
    bridge->message->close(bridge->message);
    bridge->message = NULL;
    bridge->inner_message = NULL;
  }
  lc_engine_dequeue_response_cleanup(&bridge->meta);
  if (bridge->op_mutex_initialized) {
    pthread_mutex_destroy(&bridge->op_mutex);
    bridge->op_mutex_initialized = 0;
  }
  if (bridge->state_cond_initialized) {
    pthread_cond_destroy(&bridge->state_cond);
    bridge->state_cond_initialized = 0;
  }
  if (bridge->state_mutex_initialized) {
    pthread_mutex_destroy(&bridge->state_mutex);
    bridge->state_mutex_initialized = 0;
  }
}

static int
lc_consumer_runtime_message_begin_terminal_action(
    lc_consumer_delivery_bridge *bridge) {
  int should_skip;

  should_skip = 0;
  pthread_mutex_lock(&bridge->state_mutex);
  if (bridge->handler_done ||
      !lc_consumer_delivery_state_allows_terminal_action(bridge->state)) {
    should_skip = 1;
  } else {
    bridge->state = LC_CONSUMER_DELIVERY_STATE_TERMINAL_IN_PROGRESS;
    pthread_cond_broadcast(&bridge->state_cond);
  }
  pthread_mutex_unlock(&bridge->state_mutex);
  return should_skip;
}

static int lc_consumer_runtime_message_ack(lc_message *self, lc_error *error) {
  lc_consumer_runtime_message *runtime_message;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "message ack requires self", NULL, NULL, NULL);
  }
  runtime_message = (lc_consumer_runtime_message *)self;
  if (runtime_message->inner == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "message is no longer available", NULL, NULL, NULL);
  }
  if (lc_consumer_runtime_message_begin_terminal_action(
          runtime_message->bridge)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "message is no longer available", NULL, NULL, NULL);
  }
  pthread_mutex_lock(&runtime_message->bridge->op_mutex);
  rc = runtime_message->inner->ack(runtime_message->inner, error);
  if (rc == LC_OK) {
    runtime_message->inner = NULL;
    runtime_message->bridge->inner_message = NULL;
    lc_consumer_delivery_mark_terminal(runtime_message->bridge);
  }
  pthread_mutex_unlock(&runtime_message->bridge->op_mutex);
  return rc;
}

static int lc_consumer_runtime_message_nack(lc_message *self,
                                            const lc_nack_req *req,
                                            lc_error *error) {
  lc_consumer_runtime_message *runtime_message;
  int rc;

  if (self == NULL || req == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "message nack requires self and req", NULL, NULL,
                        NULL);
  }
  runtime_message = (lc_consumer_runtime_message *)self;
  if (runtime_message->inner == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "message is no longer available", NULL, NULL, NULL);
  }
  if (lc_consumer_runtime_message_begin_terminal_action(
          runtime_message->bridge)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "message is no longer available", NULL, NULL, NULL);
  }
  pthread_mutex_lock(&runtime_message->bridge->op_mutex);
  rc = runtime_message->inner->nack(runtime_message->inner, req, error);
  if (rc == LC_OK) {
    runtime_message->inner = NULL;
    runtime_message->bridge->inner_message = NULL;
    lc_consumer_delivery_mark_terminal(runtime_message->bridge);
  }
  pthread_mutex_unlock(&runtime_message->bridge->op_mutex);
  return rc;
}

static int lc_consumer_runtime_message_extend(lc_message *self,
                                              const lc_extend_req *req,
                                              lc_error *error) {
  lc_consumer_runtime_message *runtime_message;
  int rc;
  int skip;

  if (self == NULL || req == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "message extend requires self and req", NULL, NULL,
                        NULL);
  }
  runtime_message = (lc_consumer_runtime_message *)self;
  pthread_mutex_lock(&runtime_message->bridge->state_mutex);
  skip = runtime_message->inner == NULL ||
         !lc_consumer_delivery_state_allows_extend(
             runtime_message->bridge->state) ||
         runtime_message->bridge->handler_done ||
         lc_consumer_is_stop_requested(runtime_message->bridge->worker->service);
  pthread_mutex_unlock(&runtime_message->bridge->state_mutex);
  if (skip) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "message is no longer available", NULL, NULL, NULL);
  }
  pthread_mutex_lock(&runtime_message->bridge->op_mutex);
  if (runtime_message->inner == NULL) {
    pthread_mutex_unlock(&runtime_message->bridge->op_mutex);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "message is no longer available", NULL, NULL, NULL);
  }
  rc = runtime_message->inner->extend(runtime_message->inner, req, error);
  pthread_mutex_unlock(&runtime_message->bridge->op_mutex);
  return rc;
}

static lc_lease *lc_consumer_runtime_message_state(lc_message *self) {
  lc_consumer_runtime_message *runtime_message;

  if (self == NULL) {
    return NULL;
  }
  runtime_message = (lc_consumer_runtime_message *)self;
  return runtime_message->inner != NULL && runtime_message->inner->state != NULL
             ? runtime_message->inner->state(runtime_message->inner)
             : NULL;
}

static lc_source *lc_consumer_runtime_message_payload_reader(lc_message *self) {
  lc_consumer_runtime_message *runtime_message;

  if (self == NULL) {
    return NULL;
  }
  runtime_message = (lc_consumer_runtime_message *)self;
  return runtime_message->inner != NULL &&
                 runtime_message->inner->payload_reader != NULL
             ? runtime_message->inner->payload_reader(runtime_message->inner)
             : NULL;
}

static int lc_consumer_runtime_message_rewind_payload(lc_message *self,
                                                      lc_error *error) {
  lc_consumer_runtime_message *runtime_message;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "message rewind_payload requires self", NULL, NULL,
                        NULL);
  }
  runtime_message = (lc_consumer_runtime_message *)self;
  if (runtime_message->inner == NULL ||
      runtime_message->inner->rewind_payload == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "message has no payload", NULL, NULL, NULL);
  }
  return runtime_message->inner->rewind_payload(runtime_message->inner, error);
}

static int lc_consumer_runtime_message_write_payload(lc_message *self,
                                                     lc_sink *dst,
                                                     size_t *written,
                                                     lc_error *error) {
  lc_consumer_runtime_message *runtime_message;

  if (self == NULL || dst == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "message write_payload requires self and dst", NULL,
                        NULL, NULL);
  }
  runtime_message = (lc_consumer_runtime_message *)self;
  if (runtime_message->inner == NULL ||
      runtime_message->inner->write_payload == NULL) {
    if (written != NULL) {
      *written = 0U;
    }
    return LC_OK;
  }
  return runtime_message->inner->write_payload(runtime_message->inner, dst,
                                               written, error);
}

static void lc_consumer_runtime_message_close(lc_message *self) {
  lc_consumer_runtime_message *runtime_message;

  if (self == NULL) {
    return;
  }
  runtime_message = (lc_consumer_runtime_message *)self;
  if (runtime_message->inner != NULL) {
    runtime_message->inner->close(runtime_message->inner);
    runtime_message->inner = NULL;
    runtime_message->bridge->inner_message = NULL;
  }
  if (runtime_message->bridge->message == &runtime_message->pub) {
    runtime_message->bridge->message = NULL;
  }
  lc_client_free(runtime_message->bridge->client, runtime_message);
}

static lc_message *
lc_consumer_runtime_message_wrap(lc_consumer_delivery_bridge *bridge,
                                 lc_message *inner, lc_error *error) {
  lc_consumer_runtime_message *runtime_message;

  runtime_message = (lc_consumer_runtime_message *)lc_client_calloc(
      bridge->client, 1U, sizeof(*runtime_message));
  if (runtime_message == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate managed consumer message", NULL, NULL,
                 NULL);
    return NULL;
  }
  runtime_message->bridge = bridge;
  runtime_message->inner = inner;
  runtime_message->pub.ack = lc_consumer_runtime_message_ack;
  runtime_message->pub.nack = lc_consumer_runtime_message_nack;
  runtime_message->pub.extend = lc_consumer_runtime_message_extend;
  runtime_message->pub.state = lc_consumer_runtime_message_state;
  runtime_message->pub.payload_reader = lc_consumer_runtime_message_payload_reader;
  runtime_message->pub.rewind_payload = lc_consumer_runtime_message_rewind_payload;
  runtime_message->pub.write_payload = lc_consumer_runtime_message_write_payload;
  runtime_message->pub.close = lc_consumer_runtime_message_close;
  runtime_message->pub.namespace_name = inner->namespace_name;
  runtime_message->pub.queue = inner->queue;
  runtime_message->pub.message_id = inner->message_id;
  runtime_message->pub.attempts = inner->attempts;
  runtime_message->pub.max_attempts = inner->max_attempts;
  runtime_message->pub.failure_attempts = inner->failure_attempts;
  runtime_message->pub.not_visible_until_unix = inner->not_visible_until_unix;
  runtime_message->pub.visibility_timeout_seconds =
      inner->visibility_timeout_seconds;
  runtime_message->pub.payload_content_type = inner->payload_content_type;
  runtime_message->pub.correlation_id = inner->correlation_id;
  runtime_message->pub.lease_id = inner->lease_id;
  runtime_message->pub.lease_expires_at_unix = inner->lease_expires_at_unix;
  runtime_message->pub.fencing_token = inner->fencing_token;
  runtime_message->pub.txn_id = inner->txn_id;
  runtime_message->pub.meta_etag = inner->meta_etag;
  runtime_message->pub.next_cursor = inner->next_cursor;
  runtime_message->pub.payload = inner->payload;
  return &runtime_message->pub;
}

static void lc_consumer_request_cleanup(const lc_allocator *allocator,
                                        lc_dequeue_req *request) {
  if (request == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, (char *)request->namespace_name);
  lc_free_with_allocator(allocator, (char *)request->queue);
  lc_free_with_allocator(allocator, (char *)request->owner);
  lc_free_with_allocator(allocator, (char *)request->txn_id);
  lc_free_with_allocator(allocator, (char *)request->start_after);
  memset(request, 0, sizeof(*request));
}

static void
lc_consumer_worker_config_cleanup(lc_consumer_worker_config *config) {
  if (config == NULL) {
    return;
  }
  lc_free_with_allocator(&config->allocator, config->name);
  lc_consumer_request_cleanup(&config->allocator, &config->request);
  memset(config, 0, sizeof(*config));
}

static void lc_consumer_free_string_array(const lc_allocator *allocator,
                                          char **items, size_t count) {
  size_t i;

  if (items == NULL) {
    return;
  }
  for (i = 0U; i < count; ++i) {
    lc_free_with_allocator(allocator, items[i]);
  }
  lc_free_with_allocator(allocator, items);
}

static int lc_consumer_copy_error(lc_error *dst, const lc_error *src,
                                  int fallback_code,
                                  const char *fallback_message) {
  if (src != NULL && src->code != LC_OK) {
    return lc_error_set(dst, src->code, src->http_status, src->message,
                        src->detail, src->server_code, src->correlation_id);
  }
  return lc_error_set(dst, fallback_code, 0L, fallback_message, NULL, NULL,
                      NULL);
}

static void lc_consumer_set_fatal_error(lc_consumer_service_handle *service,
                                        int rc, const lc_error *error,
                                        const char *message) {
  pthread_mutex_lock(&service->mutex);
  if (service->fatal_rc == LC_OK) {
    service->fatal_rc = rc != LC_OK ? rc : LC_ERR_TRANSPORT;
    lc_consumer_copy_error(&service->fatal_error, error, service->fatal_rc,
                           message);
  }
  service->stop_requested = 1;
  pthread_cond_broadcast(&service->cond);
  pthread_mutex_unlock(&service->mutex);
}

static int lc_consumer_is_stop_requested(lc_consumer_service_handle *service) {
  int stop_requested;

  pthread_mutex_lock(&service->mutex);
  stop_requested = service->stop_requested;
  pthread_mutex_unlock(&service->mutex);
  return stop_requested;
}

static int lc_consumer_cancel_check(void *context) {
  return lc_consumer_is_stop_requested((lc_consumer_service_handle *)context);
}

static int lc_consumer_sanitize_token(const char *value, char *buffer,
                                      size_t capacity) {
  size_t out_len;
  int last_dash;
  unsigned char ch;

  if (buffer == NULL || capacity == 0U) {
    return 0;
  }
  out_len = 0U;
  last_dash = 1;
  buffer[0] = '\0';
  if (value == NULL) {
    return 0;
  }
  while (*value != '\0' && isspace((unsigned char)*value)) {
    value += 1;
  }
  while (*value != '\0') {
    ch = (unsigned char)*value;
    if (isalnum(ch) || ch == '-' || ch == '_' || ch == '.') {
      if (out_len + 1U >= capacity) {
        break;
      }
      buffer[out_len++] = (char)ch;
      last_dash = ch == '-';
    } else if (!last_dash) {
      if (out_len + 1U >= capacity) {
        break;
      }
      buffer[out_len++] = '-';
      last_dash = 1;
    }
    value += 1;
  }
  while (out_len != 0U && buffer[out_len - 1U] == '-') {
    out_len -= 1U;
  }
  buffer[out_len] = '\0';
  return out_len != 0U;
}

static char *lc_consumer_default_owner(const lc_allocator *allocator,
                                       const char *name) {
  char name_token[128];
  char host_token[LC_CONSUMER_MAX_HOST];
  char host_raw[LC_CONSUMER_MAX_HOST];
  char owner[512];
  long seq;

  if (!lc_consumer_sanitize_token(name, name_token, sizeof(name_token))) {
    snprintf(name_token, sizeof(name_token), "consumer");
  }
  memset(host_raw, 0, sizeof(host_raw));
  if (gethostname(host_raw, sizeof(host_raw) - 1U) != 0 ||
      !lc_consumer_sanitize_token(host_raw, host_token, sizeof(host_token))) {
    snprintf(host_token, sizeof(host_token), "unknown-host");
  }
  pthread_mutex_lock(&lc_consumer_owner_mutex);
  lc_consumer_owner_seq += 1L;
  seq = lc_consumer_owner_seq;
  pthread_mutex_unlock(&lc_consumer_owner_mutex);
  snprintf(owner, sizeof(owner), "consumer-%s-%s-%ld-%ld", name_token,
           host_token, (long)getpid(), seq);
  return lc_strdup_with_allocator(allocator, owner);
}

static void
lc_consumer_normalize_restart_policy(lc_consumer_restart_policy *policy) {
  lc_consumer_restart_policy defaults;

  lc_consumer_restart_policy_init(&defaults);
  if (policy->immediate_retries < 0) {
    policy->immediate_retries = 0;
  }
  if (policy->base_delay_ms <= 0L) {
    policy->base_delay_ms = defaults.base_delay_ms;
  }
  if (policy->max_delay_ms <= 0L) {
    policy->max_delay_ms = defaults.max_delay_ms;
  }
  if (policy->base_delay_ms > policy->max_delay_ms) {
    policy->base_delay_ms = policy->max_delay_ms;
  }
  if (policy->multiplier <= 1.0) {
    policy->multiplier = defaults.multiplier;
  }
  if (policy->jitter_ms < 0L) {
    policy->jitter_ms = 0L;
  }
  if (policy->max_failures < 0) {
    policy->max_failures = 0;
  }
}

static void
lc_consumer_log_subscribe_event(lc_consumer_service_handle *service,
                                const lc_consumer_worker_config *config,
                                const char *message) {
  pslog_field fields[5];

  fields[0] = lc_log_str_field("consumer", config->name);
  fields[1] = lc_log_str_field("queue", config->request.queue);
  fields[2] = lc_log_str_field("namespace", config->request.namespace_name);
  fields[3] = lc_log_str_field("owner", config->request.owner);
  fields[4] = lc_log_bool_field("with_state", config->with_state);
  lc_log_info(service->logger, message, fields, 5U);
}

static void
lc_consumer_log_subscribe_error(lc_consumer_service_handle *service,
                                const lc_consumer_worker_config *config,
                                const lc_error *error) {
  pslog_field fields[8];

  fields[0] = lc_log_str_field("consumer", config->name);
  fields[1] = lc_log_str_field("queue", config->request.queue);
  fields[2] = lc_log_str_field("namespace", config->request.namespace_name);
  fields[3] = lc_log_str_field("owner", config->request.owner);
  fields[4] = lc_log_bool_field("with_state", config->with_state);
  fields[5] = lc_log_code_field(error);
  fields[6] = lc_log_http_status_field(error);
  fields[7] = lc_log_error_field("error", error);
  lc_log_warn(service->logger, "client.queue.subscribe.error", fields, 8U);
}

static long
lc_consumer_restart_delay(int failures,
                          const lc_consumer_restart_policy *policy) {
  int delayed_attempt;
  int i;
  double next_delay;
  long delay;
  long jitter;
  long offset;
  long span;

  if (failures <= 0) {
    return 0L;
  }
  if (failures <= policy->immediate_retries) {
    return 0L;
  }
  delayed_attempt = failures - policy->immediate_retries;
  delay = policy->base_delay_ms;
  for (i = 1; i < delayed_attempt; ++i) {
    next_delay = (double)delay * policy->multiplier;
    if (next_delay <= (double)delay || next_delay > (double)LONG_MAX) {
      delay = policy->max_delay_ms;
      break;
    }
    delay = (long)next_delay;
    if (delay >= policy->max_delay_ms) {
      delay = policy->max_delay_ms;
      break;
    }
  }
  if (delay > policy->max_delay_ms) {
    delay = policy->max_delay_ms;
  }
  if (policy->jitter_ms <= 0L || delay <= 0L) {
    return delay;
  }
  jitter = policy->jitter_ms;
  if (delay < jitter) {
    jitter = delay;
  }
  if (jitter <= 0L) {
    return delay;
  }
  span = jitter * 2L + 1L;
  if (span <= 0L) {
    return delay;
  }
  offset = (long)(rand() % span) - jitter;
  delay += offset;
  if (delay < 0L) {
    delay = 0L;
  }
  if (delay > policy->max_delay_ms) {
    delay = policy->max_delay_ms;
  }
  return delay;
}

static int lc_consumer_wait_delay(lc_consumer_service_handle *service,
                                  long delay_ms) {
  struct timespec now;
  struct timespec deadline;

  if (delay_ms <= 0L) {
    return lc_consumer_is_stop_requested(service);
  }
  if (clock_gettime(CLOCK_REALTIME, &now) != 0) {
    return 0;
  }
  deadline.tv_sec = now.tv_sec + (time_t)(delay_ms / 1000L);
  deadline.tv_nsec = now.tv_nsec + (long)(delay_ms % 1000L) * 1000000L;
  if (deadline.tv_nsec >= 1000000000L) {
    deadline.tv_sec += 1;
    deadline.tv_nsec -= 1000000000L;
  }
  pthread_mutex_lock(&service->mutex);
  while (!service->stop_requested) {
    if (pthread_cond_timedwait(&service->cond, &service->mutex, &deadline) ==
        ETIMEDOUT) {
      break;
    }
  }
  delay_ms = service->stop_requested;
  pthread_mutex_unlock(&service->mutex);
  return delay_ms != 0L;
}

static int lc_consumer_is_non_retryable(const lc_error *error) {
  if (error == NULL) {
    return 0;
  }
  if (error->code == LC_ERR_INVALID || error->code == LC_ERR_PROTOCOL) {
    return 1;
  }
  if (error->http_status >= 400L && error->http_status < 500L &&
      error->http_status != 408L && error->http_status != 429L) {
    return 1;
  }
  return 0;
}

static int lc_consumer_copy_request(lc_dequeue_req *dst,
                                    const lc_dequeue_req *src, const char *name,
                                    const char *default_namespace,
                                    const lc_allocator *allocator,
                                    lc_error *error) {
  memset(dst, 0, sizeof(*dst));
  dst->namespace_name =
      lc_strdup_with_allocator(allocator, src->namespace_name);
  if (src->namespace_name != NULL && dst->namespace_name == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy consumer namespace", NULL, NULL, NULL);
  }
  if ((dst->namespace_name == NULL || dst->namespace_name[0] == '\0') &&
      default_namespace != NULL && default_namespace[0] != '\0') {
    lc_free_with_allocator(allocator, (char *)dst->namespace_name);
    dst->namespace_name =
        lc_strdup_with_allocator(allocator, default_namespace);
    if (dst->namespace_name == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to default consumer namespace", NULL, NULL,
                          NULL);
    }
  }
  dst->queue = lc_strdup_with_allocator(allocator, src->queue);
  if (dst->queue == NULL || dst->queue[0] == '\0') {
    lc_consumer_request_cleanup(allocator, dst);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "consumer config requires request.queue", NULL, NULL,
                        NULL);
  }
  if (src->owner != NULL && src->owner[0] != '\0') {
    dst->owner = lc_strdup_with_allocator(allocator, src->owner);
  } else {
    dst->owner = lc_consumer_default_owner(allocator, name);
  }
  if (dst->owner == NULL) {
    lc_consumer_request_cleanup(allocator, dst);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy consumer owner", NULL, NULL, NULL);
  }
  dst->txn_id = lc_strdup_with_allocator(allocator, src->txn_id);
  if (src->txn_id != NULL && dst->txn_id == NULL) {
    lc_consumer_request_cleanup(allocator, dst);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy consumer txn_id", NULL, NULL, NULL);
  }
  dst->start_after = lc_strdup_with_allocator(allocator, src->start_after);
  if (src->start_after != NULL && dst->start_after == NULL) {
    lc_consumer_request_cleanup(allocator, dst);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy consumer start_after", NULL, NULL,
                        NULL);
  }
  dst->visibility_timeout_seconds = src->visibility_timeout_seconds;
  dst->wait_seconds = src->wait_seconds;
  dst->page_size = src->page_size > 0 ? src->page_size : 1;
  return LC_OK;
}

static int lc_consumer_copy_worker_config(lc_consumer_service_handle *service,
                                          lc_consumer_worker_config *dst,
                                          const lc_consumer_config *src,
                                          lc_error *error) {
  memset(dst, 0, sizeof(*dst));
  if (src->handle == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "consumer config requires handle", NULL, NULL, NULL);
  }
  dst->allocator = service->allocator;
  if (src->name != NULL && src->name[0] != '\0') {
    dst->name = lc_strdup_with_allocator(&dst->allocator, src->name);
  } else {
    dst->name = lc_strdup_with_allocator(&dst->allocator, src->request.queue);
  }
  if (dst->name == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to copy consumer name",
                        NULL, NULL, NULL);
  }
  if (lc_consumer_copy_request(&dst->request, &src->request, dst->name,
                               service->default_namespace, &dst->allocator,
                               error) != LC_OK) {
    lc_consumer_worker_config_cleanup(dst);
    return error->code;
  }
  dst->with_state = src->with_state;
  dst->handle = src->handle;
  dst->on_error = src->on_error;
  dst->on_start = src->on_start;
  dst->on_stop = src->on_stop;
  dst->context = src->context;
  dst->restart_policy = src->restart_policy;
  lc_consumer_normalize_restart_policy(&dst->restart_policy);
  return LC_OK;
}

static int lc_consumer_copy_base_config(lc_consumer_service_handle *service,
                                        const lc_client_handle *client,
                                        lc_error *error) {
  size_t i;

  service->allocator = client->allocator;
  service->endpoint_count = client->endpoint_count;
  if (client->endpoint_count != 0U) {
    service->endpoints = (char **)lc_calloc_with_allocator(
        &service->allocator, client->endpoint_count, sizeof(char *));
    if (service->endpoints == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate consumer service endpoints", NULL,
                          NULL, NULL);
    }
    for (i = 0U; i < client->endpoint_count; ++i) {
      service->endpoints[i] =
          lc_strdup_with_allocator(&service->allocator, client->endpoints[i]);
      if (client->endpoints[i] != NULL && service->endpoints[i] == NULL) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to copy consumer service endpoint", NULL,
                            NULL, NULL);
      }
    }
  }
  service->unix_socket_path =
      lc_strdup_with_allocator(&service->allocator, client->unix_socket_path);
  if (client->unix_socket_path != NULL && service->unix_socket_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy consumer unix socket path", NULL, NULL,
                        NULL);
  }
  service->client_bundle_path =
      lc_strdup_with_allocator(&service->allocator, client->client_bundle_path);
  if (client->client_bundle_path != NULL &&
      service->client_bundle_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy consumer client bundle path", NULL,
                        NULL, NULL);
  }
  service->default_namespace =
      lc_strdup_with_allocator(&service->allocator, client->default_namespace);
  if (client->default_namespace != NULL && service->default_namespace == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy consumer default namespace", NULL, NULL,
                        NULL);
  }
  service->timeout_ms = client->timeout_ms;
  service->disable_mtls = client->disable_mtls;
  service->insecure_skip_verify = client->insecure_skip_verify;
  service->prefer_http_2 = client->prefer_http_2;
  service->http_json_response_limit_bytes =
      client->http_json_response_limit_bytes;
  service->disable_logger_sys_field = client->disable_logger_sys_field;
  service->base_logger =
      client->base_logger != NULL ? client->base_logger : lc_log_noop_logger();
  service->logger = lc_log_client_logger(service->base_logger,
                                         service->disable_logger_sys_field);
  service->owns_logger =
      (service->logger != NULL && service->logger != service->base_logger &&
       service->logger != lc_log_noop_logger())
          ? 1
          : 0;
  if (service->logger == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate consumer service logger", NULL,
                        NULL, NULL);
  }
  return LC_OK;
}

static int lc_consumer_clone_client(lc_consumer_service_handle *service,
                                    lc_client **out, lc_error *error) {
  lc_client_config config;
  const char **endpoints;
  lc_client_handle *client;
  size_t i;
  int rc;

  lc_client_config_init(&config);
  endpoints = NULL;
  if (service->endpoint_count != 0U) {
    endpoints = (const char **)lc_calloc_with_allocator(
        &service->allocator, service->endpoint_count, sizeof(char *));
    if (endpoints == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate consumer endpoint array", NULL,
                          NULL, NULL);
    }
    for (i = 0U; i < service->endpoint_count; ++i) {
      endpoints[i] = service->endpoints[i];
    }
  }
  config.endpoints = endpoints;
  config.endpoint_count = service->endpoint_count;
  config.unix_socket_path = service->unix_socket_path;
  config.client_bundle_path = service->client_bundle_path;
  config.default_namespace = service->default_namespace;
  config.timeout_ms = service->timeout_ms;
  config.disable_mtls = service->disable_mtls;
  config.insecure_skip_verify = service->insecure_skip_verify;
  config.prefer_http_2 = service->prefer_http_2;
  config.http_json_response_limit_bytes =
      service->http_json_response_limit_bytes;
  config.disable_logger_sys_field = service->disable_logger_sys_field;
  config.logger = service->base_logger;
  config.allocator = service->allocator;
  rc = lc_client_open(&config, out, error);
  lc_free_with_allocator(&service->allocator, (void *)endpoints);
  if (rc != LC_OK) {
    return rc;
  }
  client = (lc_client_handle *)(*out);
  client->engine->cancel_check = lc_consumer_cancel_check;
  client->engine->cancel_context = service;
  return LC_OK;
}

static void lc_consumer_invoke_start(const lc_consumer_worker_config *config,
                                     int attempt) {
  lc_consumer_lifecycle_event event;

  if (config->on_start == NULL) {
    return;
  }
  memset(&event, 0, sizeof(event));
  event.name = config->name;
  event.queue = config->request.queue;
  event.with_state = config->with_state;
  event.attempt = attempt;
  config->on_start(config->context, &event);
}

static void lc_consumer_invoke_stop(const lc_consumer_worker_config *config,
                                    int attempt, const lc_error *error) {
  lc_consumer_lifecycle_event event;

  if (config->on_stop == NULL) {
    return;
  }
  memset(&event, 0, sizeof(event));
  event.name = config->name;
  event.queue = config->request.queue;
  event.with_state = config->with_state;
  event.attempt = attempt;
  event.error = error;
  config->on_stop(config->context, &event);
}

static int lc_consumer_handle_failure(lc_consumer_service_handle *service,
                                      const lc_consumer_worker_config *config,
                                      int *failures, const lc_error *cause,
                                      lc_error *error) {
  lc_consumer_error event;
  long delay_ms;

  if (lc_consumer_is_stop_requested(service)) {
    return LC_OK;
  }
  *failures += 1;
  if (config->restart_policy.max_failures > 0 &&
      *failures > config->restart_policy.max_failures) {
    return lc_error_set(error, cause != NULL ? cause->code : LC_ERR_TRANSPORT,
                        cause != NULL ? cause->http_status : 0L,
                        "consumer exceeded max failures",
                        cause != NULL ? cause->message : NULL,
                        cause != NULL ? cause->server_code : NULL,
                        cause != NULL ? cause->correlation_id : NULL);
  }
  delay_ms = 0L;
  if (!lc_consumer_is_non_retryable(cause)) {
    delay_ms = lc_consumer_restart_delay(*failures, &config->restart_policy);
  }
  if (config->on_error != NULL) {
    memset(&event, 0, sizeof(event));
    event.name = config->name;
    event.queue = config->request.queue;
    event.with_state = config->with_state;
    event.attempt = *failures;
    event.restart_in_ms = delay_ms;
    event.cause = cause;
    if (config->on_error(config->context, &event, error) != LC_OK) {
      if (error->code == LC_OK) {
        lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                     "consumer error handler stopped service", NULL, NULL,
                     NULL);
      }
      return error->code;
    }
  } else if (lc_consumer_is_non_retryable(cause)) {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("consumer", config->name);
    fields[1] = lc_log_str_field("queue", config->request.queue);
    fields[2] = lc_log_bool_field("stateful", config->with_state);
    fields[3] = pslog_i64("attempt", (pslog_int64)*failures);
    fields[4] = lc_log_error_field("error", cause);
    lc_log_error(service->logger, "client.consumer.stop", fields, 5U);
  } else {
    pslog_field fields[6];

    fields[0] = lc_log_str_field("consumer", config->name);
    fields[1] = lc_log_str_field("queue", config->request.queue);
    fields[2] = lc_log_bool_field("stateful", config->with_state);
    fields[3] = pslog_i64("attempt", (pslog_int64)*failures);
    fields[4] = pslog_i64("restart_in_ms", (pslog_int64)delay_ms);
    fields[5] = lc_log_error_field("error", cause);
    lc_log_warn(service->logger, "client.consumer.restart", fields, 6U);
  }
  if (lc_consumer_is_non_retryable(cause)) {
    return lc_consumer_copy_error(error, cause, LC_ERR_TRANSPORT,
                                  "consumer encountered non-retryable error");
  }
  if (lc_consumer_wait_delay(service, delay_ms)) {
    return LC_OK;
  }
  return LC_OK;
}

static int
lc_consumer_report_service_fatal(lc_consumer_service_handle *service,
                                 const lc_consumer_worker_config *config,
                                 const lc_error *cause, lc_error *error) {
  lc_consumer_error event;

  if (config->on_error != NULL) {
    memset(&event, 0, sizeof(event));
    event.name = config->name;
    event.queue = config->request.queue;
    event.with_state = config->with_state;
    event.attempt = 1;
    event.restart_in_ms = 0L;
    event.cause = cause;
    if (config->on_error(config->context, &event, error) != LC_OK) {
      if (error->code == LC_OK) {
        lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                     "consumer error handler stopped service", NULL, NULL,
                     NULL);
      }
      return error->code;
    }
  }
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("consumer", config->name);
    fields[1] = lc_log_str_field("queue", config->request.queue);
    fields[2] = lc_log_bool_field("stateful", config->with_state);
    fields[3] = pslog_i64("attempt", (pslog_int64)1);
    fields[4] = lc_log_error_field("error", cause);
    lc_log_error(service->logger, "client.consumer.stop", fields, 5U);
  }
  return LC_OK;
}

static int
lc_consumer_report_delivery_failure(lc_consumer_service_handle *service,
                                    const lc_consumer_worker_config *config,
                                    const lc_error *cause, lc_error *error) {
  lc_consumer_error event;

  if (lc_consumer_is_stop_requested(service)) {
    return LC_OK;
  }
  if (config->on_error != NULL) {
    memset(&event, 0, sizeof(event));
    event.name = config->name;
    event.queue = config->request.queue;
    event.with_state = config->with_state;
    event.attempt = 0;
    event.restart_in_ms = 0L;
    event.cause = cause;
    if (config->on_error(config->context, &event, error) != LC_OK) {
      if (error->code == LC_OK) {
        lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                     "consumer error handler stopped service", NULL, NULL,
                     NULL);
      }
      return error->code;
    }
  }
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("consumer", config->name);
    fields[1] = lc_log_str_field("queue", config->request.queue);
    fields[2] = lc_log_bool_field("stateful", config->with_state);
    fields[3] = pslog_i64("attempt", (pslog_int64)0);
    fields[4] = lc_log_error_field("error", cause);
    lc_log_warn(service->logger, "client.consumer.delivery.error", fields, 5U);
  }
  return LC_OK;
}

static void
lc_consumer_delivery_meta_copy(lc_engine_dequeue_response *dst,
                               const lc_engine_dequeue_response *src) {
  memset(dst, 0, sizeof(*dst));
  dst->namespace_name = lc_strdup_local(src->namespace_name);
  dst->queue = lc_strdup_local(src->queue);
  dst->message_id = lc_strdup_local(src->message_id);
  dst->attempts = src->attempts;
  dst->max_attempts = src->max_attempts;
  dst->failure_attempts = src->failure_attempts;
  dst->not_visible_until_unix = src->not_visible_until_unix;
  dst->visibility_timeout_seconds = src->visibility_timeout_seconds;
  dst->payload_content_type = lc_strdup_local(src->payload_content_type);
  dst->correlation_id = lc_strdup_local(src->correlation_id);
  dst->lease_id = lc_strdup_local(src->lease_id);
  dst->lease_expires_at_unix = src->lease_expires_at_unix;
  dst->fencing_token = src->fencing_token;
  dst->txn_id = lc_strdup_local(src->txn_id);
  dst->meta_etag = lc_strdup_local(src->meta_etag);
  dst->state_etag = lc_strdup_local(src->state_etag);
  dst->state_lease_id = lc_strdup_local(src->state_lease_id);
  dst->state_lease_expires_at_unix = src->state_lease_expires_at_unix;
  dst->state_fencing_token = src->state_fencing_token;
  dst->state_txn_id = lc_strdup_local(src->state_txn_id);
  dst->next_cursor = lc_strdup_local(src->next_cursor);
}

static int
lc_consumer_delivery_begin(void *context,
                           const lc_engine_dequeue_response *delivery,
                           lc_engine_error *engine_error) {
  lc_consumer_delivery_bridge *bridge;
  lc_source *payload;
  int rc;

  (void)engine_error;
  bridge = (lc_consumer_delivery_bridge *)context;
  lc_consumer_delivery_meta_copy(&bridge->meta, delivery);
  {
    pslog_field fields[8];

    fields[0] = lc_log_str_field("consumer", bridge->worker->config.name);
    fields[1] = lc_log_str_field("queue", bridge->worker->config.request.queue);
    fields[2] = lc_log_str_field("namespace",
                                 bridge->worker->config.request.namespace_name);
    fields[3] = lc_log_str_field("message_id", delivery->message_id);
    fields[4] = pslog_i64("attempts", (pslog_int64)delivery->attempts);
    fields[5] =
        pslog_i64("failure_attempts", (pslog_int64)delivery->failure_attempts);
    fields[6] =
        lc_log_bool_field("with_state", bridge->worker->config.with_state);
    fields[7] = lc_log_str_field("cid", delivery->correlation_id);
    lc_log_debug(bridge->worker->service->logger, "client.queue.delivery.begin",
                 fields, 8U);
  }
  bridge->state = LC_CONSUMER_DELIVERY_STATE_OPEN;
  bridge->handler_rc = LC_OK;
  bridge->runtime_fatal = 0;
  bridge->handler_done = 0;
  bridge->message = NULL;
  bridge->inner_message = NULL;
  bridge->handler_thread_started = 0;
  bridge->extend_thread_started = 0;
  bridge->state_mutex_initialized = 0;
  bridge->state_cond_initialized = 0;
  bridge->op_mutex_initialized = 0;
  rc = pthread_mutex_init(&bridge->state_mutex, NULL);
  if (rc != 0) {
    bridge->runtime_fatal = 1;
    lc_error_set(bridge->error, LC_ERR_TRANSPORT, 0L,
                 "failed to initialize consumer delivery state", NULL, NULL,
                 NULL);
    lc_engine_dequeue_response_cleanup(&bridge->meta);
    return 0;
  }
  bridge->state_mutex_initialized = 1;
  rc = pthread_cond_init(&bridge->state_cond, NULL);
  if (rc != 0) {
    bridge->runtime_fatal = 1;
    pthread_mutex_destroy(&bridge->state_mutex);
    bridge->state_mutex_initialized = 0;
    lc_error_set(bridge->error, LC_ERR_TRANSPORT, 0L,
                 "failed to initialize consumer delivery state", NULL, NULL,
                 NULL);
    lc_engine_dequeue_response_cleanup(&bridge->meta);
    return 0;
  }
  bridge->state_cond_initialized = 1;
  rc = pthread_mutex_init(&bridge->op_mutex, NULL);
  if (rc != 0) {
    bridge->runtime_fatal = 1;
    pthread_cond_destroy(&bridge->state_cond);
    pthread_mutex_destroy(&bridge->state_mutex);
    bridge->state_cond_initialized = 0;
    bridge->state_mutex_initialized = 0;
    lc_error_set(bridge->error, LC_ERR_TRANSPORT, 0L,
                 "failed to initialize consumer delivery state", NULL, NULL,
                 NULL);
    lc_engine_dequeue_response_cleanup(&bridge->meta);
    return 0;
  }
  bridge->op_mutex_initialized = 1;
  rc = lc_stream_pipe_open(65536U, &bridge->client->allocator, &payload,
                           &bridge->pipe, bridge->error);
  if (rc != LC_OK) {
    bridge->runtime_fatal = 1;
    pthread_mutex_destroy(&bridge->op_mutex);
    pthread_cond_destroy(&bridge->state_cond);
    pthread_mutex_destroy(&bridge->state_mutex);
    bridge->op_mutex_initialized = 0;
    bridge->state_cond_initialized = 0;
    bridge->state_mutex_initialized = 0;
    lc_engine_dequeue_response_cleanup(&bridge->meta);
    return 0;
  }
  bridge->inner_message =
      lc_message_new(bridge->client, &bridge->meta, payload, NULL);
  if (bridge->inner_message == NULL) {
    bridge->runtime_fatal = 1;
    payload->close(payload);
    lc_stream_pipe_fail(bridge->pipe, LC_ERR_NOMEM,
                        "failed to allocate consumer message");
    bridge->pipe = NULL;
    lc_error_set(bridge->error, LC_ERR_NOMEM, 0L,
                 "failed to allocate consumer message", NULL, NULL, NULL);
    pthread_mutex_destroy(&bridge->op_mutex);
    pthread_cond_destroy(&bridge->state_cond);
    pthread_mutex_destroy(&bridge->state_mutex);
    bridge->op_mutex_initialized = 0;
    bridge->state_cond_initialized = 0;
    bridge->state_mutex_initialized = 0;
    lc_engine_dequeue_response_cleanup(&bridge->meta);
    return 0;
  }
  bridge->message =
      lc_consumer_runtime_message_wrap(bridge, bridge->inner_message,
                                       bridge->error);
  if (bridge->message == NULL) {
    bridge->runtime_fatal = 1;
    bridge->inner_message->close(bridge->inner_message);
    bridge->inner_message = NULL;
    lc_stream_pipe_fail(bridge->pipe, LC_ERR_NOMEM,
                        "failed to allocate consumer message");
    bridge->pipe = NULL;
    lc_error_set(bridge->error, LC_ERR_NOMEM, 0L,
                 "failed to allocate consumer message", NULL, NULL, NULL);
    pthread_mutex_destroy(&bridge->op_mutex);
    pthread_cond_destroy(&bridge->state_cond);
    pthread_mutex_destroy(&bridge->state_mutex);
    bridge->op_mutex_initialized = 0;
    bridge->state_cond_initialized = 0;
    bridge->state_mutex_initialized = 0;
    lc_engine_dequeue_response_cleanup(&bridge->meta);
    return 0;
  }
  rc = pthread_create(&bridge->handler_thread, NULL,
                      lc_consumer_delivery_handler_main, bridge);
  if (rc != 0) {
    bridge->runtime_fatal = 1;
    bridge->message->close(bridge->message);
    bridge->message = NULL;
    bridge->inner_message = NULL;
    lc_stream_pipe_fail(bridge->pipe, LC_ERR_TRANSPORT,
                        "failed to start consumer delivery thread");
    bridge->pipe = NULL;
    lc_error_set(bridge->error, LC_ERR_TRANSPORT, 0L,
                 "failed to start consumer delivery thread", NULL, NULL, NULL);
    pthread_mutex_destroy(&bridge->op_mutex);
    pthread_cond_destroy(&bridge->state_cond);
    pthread_mutex_destroy(&bridge->state_mutex);
    bridge->op_mutex_initialized = 0;
    bridge->state_cond_initialized = 0;
    bridge->state_mutex_initialized = 0;
    lc_engine_dequeue_response_cleanup(&bridge->meta);
    return 0;
  }
  bridge->handler_thread_started = 1;
  rc = pthread_create(&bridge->extend_thread, NULL,
                      lc_consumer_delivery_extend_main, bridge);
  if (rc != 0) {
    bridge->runtime_fatal = 1;
    if (bridge->pipe != NULL) {
      lc_stream_pipe_fail(bridge->pipe, LC_ERR_TRANSPORT,
                          "failed to start consumer delivery extender");
      bridge->pipe = NULL;
    }
    pthread_join(bridge->handler_thread, NULL);
    bridge->handler_thread_started = 0;
    if (bridge->message != NULL) {
      bridge->message->close(bridge->message);
      bridge->message = NULL;
      bridge->inner_message = NULL;
    }
    lc_error_set(bridge->error, LC_ERR_TRANSPORT, 0L,
                 "failed to start consumer delivery extender", NULL, NULL,
                 NULL);
    pthread_mutex_destroy(&bridge->op_mutex);
    pthread_cond_destroy(&bridge->state_cond);
    pthread_mutex_destroy(&bridge->state_mutex);
    bridge->op_mutex_initialized = 0;
    bridge->state_cond_initialized = 0;
    bridge->state_mutex_initialized = 0;
    lc_engine_dequeue_response_cleanup(&bridge->meta);
    return 0;
  }
  bridge->extend_thread_started = 1;
  return 1;
}

static int lc_consumer_delivery_chunk(void *context, const void *bytes,
                                      size_t count,
                                      lc_engine_error *engine_error) {
  lc_consumer_delivery_bridge *bridge;
  int rc;

  (void)engine_error;
  bridge = (lc_consumer_delivery_bridge *)context;
  rc = lc_stream_pipe_write(bridge->pipe, bytes, count, bridge->error);
  if (rc != LC_OK) {
    lc_stream_pipe_fail(bridge->pipe, bridge->error->code,
                        bridge->error->message);
    bridge->pipe = NULL;
    return 0;
  }
  return 1;
}

static int lc_consumer_delivery_end(void *context,
                                    const lc_engine_dequeue_response *delivery,
                                    lc_engine_error *engine_error) {
  lc_consumer_delivery_bridge *bridge;
  int rc;

  (void)delivery;
  (void)engine_error;
  bridge = (lc_consumer_delivery_bridge *)context;
  if (lc_consumer_is_stop_requested(bridge->worker->service)) {
    if (bridge->pipe != NULL) {
      lc_stream_pipe_fail(bridge->pipe, LC_ERR_TRANSPORT,
                          "consumer service stopping");
      bridge->pipe = NULL;
    }
    if (bridge->extend_thread_started) {
      lc_consumer_delivery_stop_extender(bridge);
    }
    if (bridge->handler_thread_started) {
      pthread_join(bridge->handler_thread, NULL);
      bridge->handler_thread_started = 0;
    }
    if (bridge->message != NULL) {
      bridge->message->close(bridge->message);
      bridge->message = NULL;
      bridge->inner_message = NULL;
    }
    lc_engine_dequeue_response_cleanup(&bridge->meta);
    pthread_mutex_destroy(&bridge->op_mutex);
    bridge->op_mutex_initialized = 0;
    return 1;
  }
  lc_stream_pipe_finish(bridge->pipe);
  bridge->pipe = NULL;
  pthread_join(bridge->handler_thread, NULL);
  bridge->handler_thread_started = 0;
  lc_consumer_delivery_stop_extender(bridge);
  rc = bridge->handler_rc;
  lc_engine_dequeue_response_cleanup(&bridge->meta);
  pthread_mutex_destroy(&bridge->op_mutex);
  bridge->op_mutex_initialized = 0;
  {
    pslog_field fields[7];

    fields[0] = lc_log_str_field("consumer", bridge->worker->config.name);
    fields[1] = lc_log_str_field("queue", bridge->worker->config.request.queue);
    fields[2] = lc_log_str_field("namespace",
                                 bridge->worker->config.request.namespace_name);
    fields[3] = lc_log_str_field(
        "message_id", delivery != NULL ? delivery->message_id : NULL);
    fields[4] = lc_log_bool_field(
        "terminal", lc_consumer_delivery_state_is_terminal(bridge->state));
    fields[5] = lc_log_i64_field("handler_rc", rc);
    fields[6] = lc_log_str_field(
        "cid", delivery != NULL ? delivery->correlation_id : NULL);
    lc_log_debug(bridge->worker->service->logger,
                 rc == LC_OK ? "client.queue.delivery.complete"
                             : "client.queue.delivery.handler_error",
                 fields, 7U);
  }
  return rc == LC_OK;
}

static void *lc_consumer_delivery_handler_main(void *context) {
  lc_consumer_delivery_bridge *bridge;
  lc_nack_req nack_req;
  lc_error terminal_error;
  lc_error handler_error;
  lc_consumer_message consumer_message;
  int rc;
  int final_rc;

  bridge = (lc_consumer_delivery_bridge *)context;
  memset(&consumer_message, 0, sizeof(consumer_message));
  consumer_message.client = &bridge->client->pub;
  consumer_message.logger = bridge->client->logger;
  consumer_message.name = bridge->worker->config.name;
  consumer_message.queue = bridge->worker->config.request.queue;
  consumer_message.with_state = bridge->worker->config.with_state;
  consumer_message.message = bridge->message;
  consumer_message.state =
      bridge->message != NULL && bridge->message->state != NULL
          ? bridge->message->state(bridge->message)
          : NULL;
  lc_error_init(&handler_error);
  rc = bridge->worker->config.handle(bridge->worker->config.context,
                                     &consumer_message, &handler_error);
  final_rc = rc;
  if (bridge->message != NULL &&
      lc_consumer_delivery_state_is_terminal(bridge->state)) {
    lc_consumer_delivery_stop_extender(bridge);
  } else if (rc == LC_OK && bridge->message != NULL &&
             !lc_consumer_delivery_state_is_terminal(bridge->state)) {
    lc_consumer_delivery_stop_extender(bridge);
  }
  if (rc == LC_OK && bridge->handler_rc != LC_OK) {
    final_rc =
        bridge->handler_rc != LC_OK ? bridge->handler_rc : LC_ERR_TRANSPORT;
    if (handler_error.code == LC_OK) {
      if (bridge->error != NULL && bridge->error->code != LC_OK) {
        lc_error_set(&handler_error, bridge->error->code,
                     bridge->error->http_status, bridge->error->message,
                     bridge->error->detail, bridge->error->server_code,
                     bridge->error->correlation_id);
      } else {
        lc_error_set(&handler_error, final_rc, 0L,
                     "consumer auto-extend failed", NULL, NULL, NULL);
      }
    }
  }
  if (final_rc == LC_OK) {
    if (bridge->message != NULL &&
        !lc_consumer_delivery_state_is_terminal(bridge->state)) {
      lc_error_init(&terminal_error);
      if (bridge->message->ack(bridge->message, &terminal_error) == LC_OK) {
      } else {
        final_rc = terminal_error.code != LC_OK ? terminal_error.code
                                                : LC_ERR_TRANSPORT;
        lc_error_set(&handler_error, final_rc, terminal_error.http_status,
                     terminal_error.message, terminal_error.detail,
                     terminal_error.server_code,
                     terminal_error.correlation_id);
      }
      lc_error_cleanup(&terminal_error);
    }
  } else {
    /* A non-OK handler return is a managed consumer failure. The runtime,
     * not the user callback, owns the failure nack and restart semantics. */
    if (handler_error.code == LC_OK) {
      lc_error_set(&handler_error, LC_ERR_TRANSPORT, 0L,
                   "consumer handler failed", NULL, NULL, NULL);
    }
    if (bridge->message != NULL &&
        !lc_consumer_delivery_state_is_terminal(bridge->state)) {
      lc_consumer_delivery_stop_extender(bridge);
      lc_nack_req_init(&nack_req);
      nack_req.intent = LC_NACK_INTENT_FAILURE;
      nack_req.delay_seconds = 0L;
      lc_error_init(&terminal_error);
      if (bridge->message->nack(bridge->message, &nack_req, &terminal_error) ==
          LC_OK) {
      } else {
        final_rc = terminal_error.code != LC_OK ? terminal_error.code
                                                : LC_ERR_TRANSPORT;
        if (bridge->error != NULL && bridge->error->code == LC_OK) {
          lc_consumer_copy_error(bridge->error, &terminal_error, final_rc,
                                 "consumer handler failed to nack delivery");
        }
      }
      lc_error_cleanup(&terminal_error);
    }
  }
  if (rc != LC_OK || (rc == LC_OK && final_rc != LC_OK)) {
    lc_consumer_copy_error(bridge->error, &handler_error,
                           handler_error.code != LC_OK ? handler_error.code
                                                       : final_rc,
                           "consumer handler failed");
  }
  lc_error_cleanup(&handler_error);
  bridge->handler_rc = final_rc;
  lc_consumer_delivery_mark_handler_done(bridge);
  return NULL;
}

static void *lc_consumer_delivery_extend_main(void *context) {
  lc_consumer_delivery_bridge *bridge;
  lc_extend_req extend_req;
  lc_error extend_error;
  long delay_ms;
  long visibility_timeout_seconds;
  struct timespec deadline;
  int rc;

  bridge = (lc_consumer_delivery_bridge *)context;
  for (;;) {
    pthread_mutex_lock(&bridge->state_mutex);
    if (bridge->handler_done ||
        !lc_consumer_delivery_state_allows_extend(bridge->state) ||
        lc_consumer_is_stop_requested(bridge->worker->service)) {
      pthread_mutex_unlock(&bridge->state_mutex);
      return NULL;
    }
    visibility_timeout_seconds =
        bridge->message != NULL ? bridge->message->visibility_timeout_seconds
                                : 0L;
    delay_ms = lc_consumer_auto_extend_delay_ms(visibility_timeout_seconds);
    if (delay_ms <= 0L) {
      pthread_mutex_unlock(&bridge->state_mutex);
      return NULL;
    }
    if (clock_gettime(CLOCK_REALTIME, &deadline) != 0) {
      pthread_mutex_unlock(&bridge->state_mutex);
      return NULL;
    }
    lc_consumer_timespec_add_ms(&deadline, delay_ms);
    rc = pthread_cond_timedwait(&bridge->state_cond, &bridge->state_mutex,
                                &deadline);
    if (bridge->handler_done ||
        !lc_consumer_delivery_state_allows_extend(bridge->state) ||
        lc_consumer_is_stop_requested(bridge->worker->service)) {
      pthread_mutex_unlock(&bridge->state_mutex);
      return NULL;
    }
    pthread_mutex_unlock(&bridge->state_mutex);
    if (rc == 0) {
      continue;
    }
    if (bridge->inner_message == NULL) {
      return NULL;
    }
    pthread_mutex_lock(&bridge->op_mutex);
    pthread_mutex_lock(&bridge->state_mutex);
    if (bridge->inner_message == NULL || bridge->handler_done ||
        !lc_consumer_delivery_state_allows_extend(bridge->state) ||
        lc_consumer_is_stop_requested(bridge->worker->service)) {
      pthread_mutex_unlock(&bridge->state_mutex);
      pthread_mutex_unlock(&bridge->op_mutex);
      return NULL;
    }
    visibility_timeout_seconds = bridge->inner_message->visibility_timeout_seconds;
    pthread_mutex_unlock(&bridge->state_mutex);
    if (visibility_timeout_seconds <= 0L) {
      pthread_mutex_unlock(&bridge->op_mutex);
      return NULL;
    }
    lc_extend_req_init(&extend_req);
    extend_req.extend_by_seconds = visibility_timeout_seconds;
    lc_error_init(&extend_error);
    rc = bridge->inner_message->extend(bridge->inner_message, &extend_req,
                                       &extend_error);
    pthread_mutex_unlock(&bridge->op_mutex);
    if (rc != LC_OK) {
      pthread_mutex_lock(&bridge->state_mutex);
      if (bridge->handler_done ||
          !lc_consumer_delivery_state_allows_extend(bridge->state) ||
          lc_consumer_is_stop_requested(bridge->worker->service)) {
        pthread_mutex_unlock(&bridge->state_mutex);
        lc_error_cleanup(&extend_error);
        return NULL;
      }
      if (bridge->error != NULL && bridge->error->code == LC_OK) {
        lc_consumer_copy_error(bridge->error, &extend_error,
                               extend_error.code != LC_OK ? extend_error.code
                                                          : LC_ERR_TRANSPORT,
                               "consumer auto-extend failed");
      }
      if (bridge->handler_rc == LC_OK) {
        bridge->handler_rc =
            extend_error.code != LC_OK ? extend_error.code : LC_ERR_TRANSPORT;
      }
      pthread_cond_broadcast(&bridge->state_cond);
      pthread_mutex_unlock(&bridge->state_mutex);
      lc_error_cleanup(&extend_error);
      return NULL;
    }
    lc_error_cleanup(&extend_error);
  }
}

static void *lc_consumer_worker_main(void *context) {
  lc_consumer_worker_state *worker;
  lc_consumer_service_handle *service;
  lc_client *client;
  lc_client_handle *client_handle;
  lc_engine_dequeue_request request;
  lc_engine_queue_stream_handler handler;
  lc_consumer_delivery_bridge bridge;
  lc_engine_error engine_error;
  lc_error error;
  int rc;
  int attempt;
  int failures;

  worker = (lc_consumer_worker_state *)context;
  service = worker->service;
  client = NULL;
  client_handle = NULL;
  attempt = 0;
  failures = 0;
  lc_error_init(&error);
  lc_engine_error_init(&engine_error);

  rc = lc_consumer_clone_client(service, &client, &error);
  if (rc != LC_OK) {
    lc_consumer_set_fatal_error(service, rc, &error,
                                "failed to open consumer worker client");
    goto done;
  }
  client_handle = (lc_client_handle *)client;

  memset(&request, 0, sizeof(request));
  request.namespace_name = worker->config.request.namespace_name;
  request.queue = worker->config.request.queue;
  request.owner = worker->config.request.owner;
  request.txn_id = worker->config.request.txn_id;
  request.visibility_timeout_seconds =
      worker->config.request.visibility_timeout_seconds;
  request.wait_seconds = worker->config.request.wait_seconds;
  request.page_size = worker->config.request.page_size;
  request.start_after = worker->config.request.start_after;

  memset(&handler, 0, sizeof(handler));
  handler.begin = lc_consumer_delivery_begin;
  handler.chunk = lc_consumer_delivery_chunk;
  handler.end = lc_consumer_delivery_end;

  while (!lc_consumer_is_stop_requested(service)) {
    attempt += 1;
    lc_consumer_invoke_start(&worker->config, attempt);
    lc_consumer_log_subscribe_event(service, &worker->config,
                                    "client.queue.subscribe.begin");
    lc_error_cleanup(&error);
    lc_error_init(&error);
    lc_engine_error_cleanup(&engine_error);
    lc_engine_error_init(&engine_error);
    memset(&bridge, 0, sizeof(bridge));
    bridge.worker = worker;
    bridge.client = client_handle;
    bridge.error = &error;

    if (worker->config.with_state) {
      rc = lc_engine_client_subscribe_with_state(
          client_handle->engine, &request, &handler, &bridge, &engine_error);
    } else {
      rc = lc_engine_client_subscribe(client_handle->engine, &request, &handler,
                                      &bridge, &engine_error);
    }
    if (bridge.pipe != NULL) {
      lc_stream_pipe_fail(bridge.pipe, LC_ERR_TRANSPORT,
                          "consumer subscribe aborted");
      bridge.pipe = NULL;
    }
    lc_consumer_delivery_cleanup(&bridge);

    if (rc == LC_ENGINE_OK && bridge.handler_rc == LC_OK &&
        error.code == LC_OK) {
      lc_consumer_log_subscribe_event(service, &worker->config,
                                      "client.queue.subscribe.complete");
      if (lc_consumer_is_stop_requested(service)) {
        lc_consumer_invoke_stop(&worker->config, attempt, NULL);
        break;
      }
      failures = 0;
      lc_consumer_invoke_stop(&worker->config, attempt, NULL);
      continue;
    }
    if (bridge.runtime_fatal) {
      lc_consumer_log_subscribe_error(service, &worker->config, &error);
      lc_consumer_invoke_stop(&worker->config, attempt, &error);
      if (lc_consumer_report_service_fatal(service, &worker->config, &error,
                                           &error) != LC_OK) {
        lc_consumer_set_fatal_error(service, error.code, &error,
                                    "consumer service stopped after failure");
        break;
      }
      lc_consumer_set_fatal_error(service, error.code, &error,
                                  "consumer service delivery runtime failed");
      break;
    }
    if (lc_consumer_is_stop_requested(service)) {
      lc_consumer_invoke_stop(&worker->config, attempt, NULL);
      break;
    }
    if (bridge.handler_rc != LC_OK || error.code != LC_OK) {
      lc_consumer_log_subscribe_error(service, &worker->config, &error);
      lc_consumer_invoke_stop(&worker->config, attempt, &error);
      if (lc_consumer_report_delivery_failure(service, &worker->config, &error,
                                              &error) != LC_OK) {
        lc_consumer_set_fatal_error(service, error.code, &error,
                                    "consumer service stopped after failure");
        break;
      }
      continue;
    }
    rc = lc_error_from_engine(&error, &engine_error);
    lc_consumer_log_subscribe_error(service, &worker->config, &error);
    lc_consumer_invoke_stop(&worker->config, attempt, &error);
    if (lc_consumer_handle_failure(service, &worker->config, &failures, &error,
                                   &error) != LC_OK) {
      lc_consumer_set_fatal_error(service, error.code, &error,
                                  "consumer service stopped after failure");
      break;
    }
  }

done:
  if (client != NULL) {
    client->close(client);
  }
  lc_engine_error_cleanup(&engine_error);
  lc_error_cleanup(&error);
  pthread_mutex_lock(&service->mutex);
  service->active_workers -= 1U;
  pthread_cond_broadcast(&service->cond);
  pthread_mutex_unlock(&service->mutex);
  return NULL;
}

static int lc_consumer_join_all(lc_consumer_service_handle *service) {
  size_t i;

  if (service->joined) {
    return LC_OK;
  }
  for (i = 0U; i < service->worker_count; ++i) {
    if (service->workers[i].thread_started) {
      pthread_join(service->workers[i].thread, NULL);
      service->workers[i].thread_started = 0;
    }
  }
  service->joined = 1;
  return LC_OK;
}

int lc_consumer_service_start_method(lc_consumer_service *self,
                                     lc_error *error) {
  lc_consumer_service_handle *service;
  size_t i;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "consumer service start requires service", NULL, NULL,
                        NULL);
  }
  service = (lc_consumer_service_handle *)self;
  pthread_mutex_lock(&service->mutex);
  if (service->started_once) {
    pthread_mutex_unlock(&service->mutex);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "consumer service can only be started once", NULL, NULL,
                        NULL);
  }
  service->started_once = 1;
  service->stop_requested = 0;
  service->joined = 0;
  service->fatal_rc = LC_OK;
  lc_error_cleanup(&service->fatal_error);
  lc_error_init(&service->fatal_error);
  pthread_mutex_unlock(&service->mutex);

  for (i = 0U; i < service->worker_count; ++i) {
    service->workers[i].service = service;
    pthread_mutex_lock(&service->mutex);
    service->active_workers += 1U;
    pthread_mutex_unlock(&service->mutex);
      rc = pthread_create(&service->workers[i].thread, NULL,
                          lc_consumer_worker_main, &service->workers[i]);
    if (rc != 0) {
      pthread_mutex_lock(&service->mutex);
      service->active_workers -= 1U;
      pthread_mutex_unlock(&service->mutex);
      lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                   "failed to start consumer worker thread", strerror(rc), NULL,
                   NULL);
      lc_consumer_set_fatal_error(service, error->code, error,
                                  "failed to start consumer worker thread");
      break;
    }
    service->workers[i].thread_started = 1;
  }

  if (i != service->worker_count) {
    lc_consumer_service_stop_method(self);
    lc_consumer_service_wait_method(self, error);
    return error->code != LC_OK ? error->code : LC_ERR_TRANSPORT;
  }
  return LC_OK;
}

int lc_consumer_service_stop_method(lc_consumer_service *self) {
  lc_consumer_service_handle *service;

  if (self == NULL) {
    return LC_ERR_INVALID;
  }
  service = (lc_consumer_service_handle *)self;
  pthread_mutex_lock(&service->mutex);
  service->stop_requested = 1;
  pthread_cond_broadcast(&service->cond);
  pthread_mutex_unlock(&service->mutex);
  return LC_OK;
}

int lc_consumer_service_wait_method(lc_consumer_service *self,
                                    lc_error *error) {
  lc_consumer_service_handle *service;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "consumer service wait requires service", NULL, NULL,
                        NULL);
  }
  service = (lc_consumer_service_handle *)self;
  pthread_mutex_lock(&service->mutex);
  if (!service->started_once) {
    pthread_mutex_unlock(&service->mutex);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "consumer service has not been started", NULL, NULL,
                        NULL);
  }
  while (service->active_workers != 0U) {
    pthread_cond_wait(&service->cond, &service->mutex);
  }
  pthread_mutex_unlock(&service->mutex);

  rc = lc_consumer_join_all(service);
  if (rc != LC_OK) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to join consumer service threads", NULL, NULL,
                        NULL);
  }
  if (service->fatal_rc != LC_OK) {
    return lc_consumer_copy_error(error, &service->fatal_error,
                                  service->fatal_rc, "consumer service failed");
  }
  return LC_OK;
}

int lc_consumer_service_run_method(lc_consumer_service *self, lc_error *error) {
  int rc;

  rc = lc_consumer_service_start_method(self, error);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_consumer_service_wait_method(self, error);
}

void lc_consumer_service_close_method(lc_consumer_service *self) {
  lc_consumer_service_handle *service;
  size_t i;

  if (self == NULL) {
    return;
  }
  service = (lc_consumer_service_handle *)self;
  if (service->started_once && !service->joined) {
    lc_consumer_service_stop_method(self);
    lc_consumer_service_wait_method(self, NULL);
  }
  for (i = 0U; i < service->worker_count; ++i) {
    lc_consumer_worker_config_cleanup(&service->workers[i].config);
  }
  lc_free_with_allocator(&service->allocator, service->workers);
  lc_consumer_free_string_array(&service->allocator, service->endpoints,
                                service->endpoint_count);
  lc_free_with_allocator(&service->allocator, service->unix_socket_path);
  lc_free_with_allocator(&service->allocator, service->client_bundle_path);
  lc_free_with_allocator(&service->allocator, service->default_namespace);
  lc_error_cleanup(&service->fatal_error);
  pthread_cond_destroy(&service->cond);
  pthread_mutex_destroy(&service->mutex);
  if (service->owns_logger && service->logger != NULL &&
      service->logger != lc_log_noop_logger()) {
    service->logger->destroy(service->logger);
  }
  lc_free_with_allocator(&service->allocator, service);
}

int lc_client_new_consumer_service_method(
    lc_client *self, const lc_consumer_service_config *config,
    lc_consumer_service **out, lc_error *error) {
  lc_client_handle *client;
  lc_consumer_service_handle *service;
  size_t i;
  size_t j;
  size_t worker_slot;
  size_t worker_count;
  int rc;

  if (self == NULL || config == NULL || out == NULL ||
      config->consumers == NULL || config->consumer_count == 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "new_consumer_service requires client, config, and out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  service = (lc_consumer_service_handle *)lc_calloc_with_allocator(
      &client->allocator, 1U, sizeof(*service));
  if (service == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate consumer service", NULL, NULL,
                        NULL);
  }
  service->allocator = client->allocator;
  lc_error_init(&service->fatal_error);
  pthread_mutex_init(&service->mutex, NULL);
  pthread_cond_init(&service->cond, NULL);
  rc = lc_consumer_copy_base_config(service, client, error);
  if (rc != LC_OK) {
    lc_consumer_service_close_method(&service->pub);
    return rc;
  }
  service->worker_count = 0U;
  for (i = 0U; i < config->consumer_count; ++i) {
    worker_count = config->consumers[i].worker_count;
    if (worker_count == 0U) {
      worker_count = 1U;
    }
    service->worker_count += worker_count;
  }
  service->workers = (lc_consumer_worker_state *)lc_calloc_with_allocator(
      &service->allocator, service->worker_count, sizeof(*service->workers));
  if (service->workers == NULL) {
    lc_consumer_service_close_method(&service->pub);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate consumer service workers", NULL,
                        NULL, NULL);
  }
  worker_slot = 0U;
  for (i = 0U; i < config->consumer_count; ++i) {
    worker_count = config->consumers[i].worker_count;
    if (worker_count == 0U) {
      worker_count = 1U;
    }
    for (j = 0U; j < worker_count; ++j) {
      rc = lc_consumer_copy_worker_config(service,
                                          &service->workers[worker_slot].config,
                                          &config->consumers[i], error);
      if (rc != LC_OK) {
        lc_consumer_service_close_method(&service->pub);
        return rc;
      }
      worker_slot += 1U;
    }
  }
  service->pub.run = lc_consumer_service_run_method;
  service->pub.start = lc_consumer_service_start_method;
  service->pub.stop = lc_consumer_service_stop_method;
  service->pub.wait = lc_consumer_service_wait_method;
  service->pub.close = lc_consumer_service_close_method;
  *out = &service->pub;
  return LC_OK;
}

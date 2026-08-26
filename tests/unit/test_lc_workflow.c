#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>

#include <cmocka.h>

#include "../support/lc_test_tmp.h"
#include "lc/lc.h"
#include "lc_api_internal.h"
#include "lc_pouch_internal.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define WORKFLOW_TMP_PREFIX "/tmp/liblockdc-unit-workflow-"
#define WORKFLOW_RECONCILIATION_RECORDS 256U
#define WORKFLOW_PREFETCH_RECORDS 3U
#define WORKFLOW_SHARED_PROCESS_RECORDS 256U
#define WORKFLOW_SHARED_PROCESS_IDLE_LIMIT 40U
#define WORKFLOW_CLEAN_REOPEN_FOREIGN_KEYS 32U
#define WORKFLOW_CLEAN_REOPEN_FOREIGN_CHURN 16U
#define WORKFLOW_CLEAN_REOPEN_PENDING_RECORDS 32U

static int workflow_fail_allocation(void *context, lc_error *error) {
  (void)context;
  return lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "forced workflow allocation failure", NULL, NULL, NULL);
}

typedef struct workflow_fail_once {
  unsigned int calls;
} workflow_fail_once;

static int workflow_fail_first_call(void *context, lc_error *error) {
  workflow_fail_once *failure = (workflow_fail_once *)context;

  if (failure != NULL && failure->calls++ == 0U) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "forced workflow transient claim failure", NULL, NULL,
                        NULL);
  }
  return LC_OK;
}

static void workflow_reset_allocation_failures(void) {
  lc_workflow_test_after_close_requested_hook = NULL;
  lc_workflow_test_after_close_requested_context = NULL;
  lc_workflow_test_before_ready_job_detach_hook = NULL;
  lc_workflow_test_before_ready_job_detach_context = NULL;
  lc_workflow_test_before_ready_job_teardown_hook = NULL;
  lc_workflow_test_before_ready_job_teardown_context = NULL;
  lc_workflow_test_before_ledger_append_hook = NULL;
  lc_workflow_test_before_ledger_append_context = NULL;
  lc_workflow_test_before_participant_allocation_hook = NULL;
  lc_workflow_test_before_participant_allocation_context = NULL;
  lc_workflow_test_before_command_receipt_copy_hook = NULL;
  lc_workflow_test_before_command_receipt_copy_context = NULL;
  lc_workflow_test_before_outbox_receipt_copy_hook = NULL;
  lc_workflow_test_before_outbox_receipt_copy_context = NULL;
  lc_workflow_test_before_notification_copy_hook = NULL;
  lc_workflow_test_before_notification_copy_context = NULL;
  lc_workflow_test_before_claim_outbox_hook = NULL;
  lc_workflow_test_before_claim_outbox_context = NULL;
}

static int workflow_bytes_contains(const void *bytes, size_t length,
                                   const char *needle) {
  size_t needle_length;
  size_t index;

  if (bytes == NULL || needle == NULL)
    return 0;
  needle_length = strlen(needle);
  if (needle_length == 0U || needle_length > length)
    return 0;
  for (index = 0U; index <= length - needle_length; ++index) {
    if (memcmp((const char *)bytes + index, needle, needle_length) == 0)
      return 1;
  }
  return 0;
}

static int workflow_slow_test_runtime(void) {
  const char *value = getenv("LOCKDC_SLOW_TEST_RUNTIME");

  return value != NULL && value[0] != '\0' && strcmp(value, "0") != 0;
}

static long workflow_claim_ttl_seconds(void) {
  return workflow_slow_test_runtime() ? 2L : 1L;
}

static long workflow_claim_next_timeout_ms(void) {
  return workflow_slow_test_runtime() ? 10000L : 5000L;
}

static unsigned int workflow_claim_expiry_wait_seconds(void) {
  return workflow_slow_test_runtime() ? 3U : 2U;
}

static long workflow_elapsed_milliseconds(const struct timespec *started,
                                          const struct timespec *finished);

typedef struct workflow_shutdown_race {
  pthread_mutex_t mutex;
  pthread_cond_t condition;
  int close_requested;
  int allow_close;
  int ready_detach_entered;
  int allow_ready_detach;
  int teardown_entered;
  int allow_teardown;
  int close_finished;
  int next_finished;
  int next_rc;
  lc_workflow *workflow;
  lc_outbox_job *job;
} workflow_shutdown_race;

static int workflow_shutdown_race_wait(workflow_shutdown_race *race,
                                       int *flag) {
  struct timespec deadline;
  int reached;
  int rc;

  if (clock_gettime(CLOCK_REALTIME, &deadline) != 0)
    return 0;
  deadline.tv_sec += 5L;
  assert_int_equal(pthread_mutex_lock(&race->mutex), 0);
  rc = 0;
  while (!*flag && rc == 0)
    rc = pthread_cond_timedwait(&race->condition, &race->mutex, &deadline);
  reached = *flag;
  assert_int_equal(pthread_mutex_unlock(&race->mutex), 0);
  return rc == 0 && reached;
}

static void workflow_shutdown_race_after_close_requested(void *context) {
  workflow_shutdown_race *race = (workflow_shutdown_race *)context;

  (void)pthread_mutex_lock(&race->mutex);
  race->close_requested = 1;
  (void)pthread_cond_broadcast(&race->condition);
  while (!race->allow_close)
    (void)pthread_cond_wait(&race->condition, &race->mutex);
  (void)pthread_mutex_unlock(&race->mutex);
}

static void workflow_shutdown_race_before_ready_detach(void *context) {
  workflow_shutdown_race *race = (workflow_shutdown_race *)context;

  (void)pthread_mutex_lock(&race->mutex);
  race->ready_detach_entered = 1;
  (void)pthread_cond_broadcast(&race->condition);
  while (!race->allow_ready_detach)
    (void)pthread_cond_wait(&race->condition, &race->mutex);
  (void)pthread_mutex_unlock(&race->mutex);
}

static void workflow_shutdown_race_before_teardown(void *context) {
  workflow_shutdown_race *race = (workflow_shutdown_race *)context;

  (void)pthread_mutex_lock(&race->mutex);
  race->teardown_entered = 1;
  (void)pthread_cond_broadcast(&race->condition);
  while (!race->allow_teardown)
    (void)pthread_cond_wait(&race->condition, &race->mutex);
  (void)pthread_mutex_unlock(&race->mutex);
}

static void *workflow_shutdown_race_close_thread(void *context) {
  workflow_shutdown_race *race = (workflow_shutdown_race *)context;

  lc_workflow_close(race->workflow);
  (void)pthread_mutex_lock(&race->mutex);
  race->close_finished = 1;
  (void)pthread_cond_broadcast(&race->condition);
  (void)pthread_mutex_unlock(&race->mutex);
  return NULL;
}

static void *workflow_shutdown_race_next_thread(void *context) {
  workflow_shutdown_race *race = (workflow_shutdown_race *)context;
  lc_error error;
  lc_outbox_job *job;
  int rc;

  lc_error_init(&error);
  job = NULL;
  rc = lc_workflow_next(race->workflow, 0L, &job, &error);
  lc_error_cleanup(&error);
  (void)pthread_mutex_lock(&race->mutex);
  race->next_rc = rc;
  race->job = job;
  race->next_finished = 1;
  (void)pthread_cond_broadcast(&race->condition);
  (void)pthread_mutex_unlock(&race->mutex);
  return NULL;
}

typedef struct workflow_query_count {
  unsigned long count;
} workflow_query_count;

typedef struct workflow_tracked_allocation {
  void *pointer;
  struct workflow_tracked_allocation *next;
} workflow_tracked_allocation;

typedef struct workflow_tracking_allocator {
  pthread_mutex_t mutex;
  workflow_tracked_allocation *allocations;
  size_t foreign_free_calls;
  size_t foreign_realloc_calls;
} workflow_tracking_allocator;

static void
workflow_tracking_allocator_init(workflow_tracking_allocator *allocator) {
  memset(allocator, 0, sizeof(*allocator));
  assert_int_equal(pthread_mutex_init(&allocator->mutex, NULL), 0);
}

static void
workflow_tracking_allocator_destroy(workflow_tracking_allocator *allocator) {
  workflow_tracked_allocation *allocation;

  assert_int_equal(pthread_mutex_lock(&allocator->mutex), 0);
  allocation = allocator->allocations;
  allocator->allocations = NULL;
  assert_int_equal(pthread_mutex_unlock(&allocator->mutex), 0);
  assert_null(allocation);
  assert_int_equal(allocator->foreign_free_calls, 0U);
  assert_int_equal(allocator->foreign_realloc_calls, 0U);
  pthread_mutex_destroy(&allocator->mutex);
}

static void *workflow_tracking_malloc(void *context, size_t size) {
  workflow_tracking_allocator *allocator =
      (workflow_tracking_allocator *)context;
  workflow_tracked_allocation *allocation;
  void *pointer;

  pointer = malloc(size == 0U ? 1U : size);
  if (pointer == NULL)
    return NULL;
  allocation = (workflow_tracked_allocation *)malloc(sizeof(*allocation));
  if (allocation == NULL) {
    free(pointer);
    return NULL;
  }
  allocation->pointer = pointer;
  assert_int_equal(pthread_mutex_lock(&allocator->mutex), 0);
  allocation->next = allocator->allocations;
  allocator->allocations = allocation;
  assert_int_equal(pthread_mutex_unlock(&allocator->mutex), 0);
  return pointer;
}

static void *workflow_tracking_realloc(void *context, void *pointer,
                                       size_t size) {
  workflow_tracking_allocator *allocator =
      (workflow_tracking_allocator *)context;
  workflow_tracked_allocation **cursor;
  workflow_tracked_allocation *allocation;
  void *resized;

  if (pointer == NULL)
    return workflow_tracking_malloc(context, size);
  assert_int_equal(pthread_mutex_lock(&allocator->mutex), 0);
  cursor = &allocator->allocations;
  while (*cursor != NULL && (*cursor)->pointer != pointer)
    cursor = &(*cursor)->next;
  if (*cursor == NULL) {
    ++allocator->foreign_realloc_calls;
    assert_int_equal(pthread_mutex_unlock(&allocator->mutex), 0);
    return NULL;
  }
  allocation = *cursor;
  if (size == 0U) {
    *cursor = allocation->next;
    assert_int_equal(pthread_mutex_unlock(&allocator->mutex), 0);
    free(allocation);
    free(pointer);
    return NULL;
  }
  resized = realloc(pointer, size);
  if (resized != NULL)
    allocation->pointer = resized;
  assert_int_equal(pthread_mutex_unlock(&allocator->mutex), 0);
  return resized;
}

static void workflow_tracking_free(void *context, void *pointer) {
  workflow_tracking_allocator *allocator =
      (workflow_tracking_allocator *)context;
  workflow_tracked_allocation **cursor;
  workflow_tracked_allocation *allocation;

  if (pointer == NULL)
    return;
  assert_int_equal(pthread_mutex_lock(&allocator->mutex), 0);
  cursor = &allocator->allocations;
  while (*cursor != NULL && (*cursor)->pointer != pointer)
    cursor = &(*cursor)->next;
  if (*cursor == NULL) {
    ++allocator->foreign_free_calls;
    assert_int_equal(pthread_mutex_unlock(&allocator->mutex), 0);
    return;
  }
  allocation = *cursor;
  *cursor = allocation->next;
  assert_int_equal(pthread_mutex_unlock(&allocator->mutex), 0);
  free(allocation);
  free(pointer);
}

typedef struct workflow_reconcile_overflow_hook {
  lc_workflow *workflow;
  int calls;
  int rc;
} workflow_reconcile_overflow_hook;

static int workflow_query_count_begin(void *context, lc_error *error) {
  (void)context;
  (void)error;
  return 1;
}

static int workflow_query_count_chunk(void *context, const char *bytes,
                                      size_t length, lc_error *error) {
  (void)context;
  (void)bytes;
  (void)length;
  (void)error;
  return 1;
}

static int workflow_query_count_end(void *context, lc_error *error) {
  workflow_query_count *count = (workflow_query_count *)context;

  (void)error;
  ++count->count;
  return 1;
}

static void seed_recovery_outbox(lc_client *client, const char *namespace_name,
                                 const char *key, lc_error *error) {
  static const char state[] =
      "{\"record_type\":\"lockdc.outbox.v1\",\"operation_id\":\"recovery-op\","
      "\"effect_id\":\"recovery-effect\",\"effect_key\":\"recovery-key\","
      "\"message_id\":\"msg_recovery\","
      "\"kind\":\"test\",\"destination\":\"recovery://target\","
      "\"content_type\":\"text/plain\",\"dispatch_state\":\"pending\","
      "\"attempt_count\":0,\"not_before_unix\":0}";
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_source *state_source;
  lc_source *payload_source;
  lc_attach_req attach;
  lc_attach_res attach_result;

  lc_acquire_req_init(&acquire);
  acquire.namespace_name = namespace_name;
  acquire.key = key;
  acquire.owner = "workflow-recovery-seed";
  acquire.ttl_seconds = 30L;
  lease = NULL;
  assert_int_equal(lc_acquire(client, &acquire, &lease, error), LC_OK);
  state_source = NULL;
  assert_int_equal(
      lc_source_from_memory(state, sizeof(state) - 1U, &state_source, error),
      LC_OK);
  assert_int_equal(lc_lease_update(lease, state_source, NULL, error), LC_OK);
  lc_source_close(state_source);
  payload_source = NULL;
  assert_int_equal(
      lc_source_from_memory("recovery-payload", 16U, &payload_source, error),
      LC_OK);
  lc_attach_req_init(&attach);
  attach.name = "payload";
  attach.content_type = "text/plain";
  attach.prevent_overwrite = 1;
  memset(&attach_result, 0, sizeof(attach_result));
  assert_int_equal(
      lc_lease_attach(lease, &attach, payload_source, &attach_result, error),
      LC_OK);
  lc_attach_res_cleanup(&attach_result);
  lc_source_close(payload_source);
  assert_int_equal(lc_lease_release(lease, NULL, error), LC_OK);
}

static void seed_terminal_workflow_outbox(lc_client *client,
                                          const char *namespace_name,
                                          const char *key, lc_error *error) {
  static const char state[] =
      "{\"record_type\":\"lockdc.outbox.v1\",\"operation_id\":\"terminal-op\","
      "\"effect_id\":\"terminal-effect\",\"effect_key\":\"terminal-key\","
      "\"message_id\":\"msg_terminal\","
      "\"kind\":\"test\",\"destination\":\"recovery://target\","
      "\"content_type\":\"text/plain\",\"dispatch_state\":\"completed\","
      "\"attempt_count\":1,\"not_before_unix\":0}";
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_source *source;

  lc_acquire_req_init(&acquire);
  acquire.namespace_name = namespace_name;
  acquire.key = key;
  acquire.owner = "workflow-terminal-seed";
  acquire.ttl_seconds = 30L;
  lease = NULL;
  source = NULL;
  assert_int_equal(lc_acquire(client, &acquire, &lease, error), LC_OK);
  assert_int_equal(
      lc_source_from_memory(state, sizeof(state) - 1U, &source, error), LC_OK);
  assert_int_equal(lc_lease_update(lease, source, NULL, error), LC_OK);
  lc_source_close(source);
  assert_int_equal(lc_lease_release(lease, NULL, error), LC_OK);
}

static void seed_foreign_workflow_state(lc_client *client,
                                        const char *namespace_name,
                                        const char *key,
                                        const char *dispatch_state,
                                        lc_error *error) {
  char state[160];
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_source *state_source;

  assert_true(snprintf(state, sizeof(state),
                       "{\"record_type\":\"foreign.v1\","
                       "\"dispatch_state\":\"%s\"}",
                       dispatch_state) > 0);
  lc_acquire_req_init(&acquire);
  acquire.namespace_name = namespace_name;
  acquire.key = key;
  acquire.owner = "workflow-foreign-seed";
  acquire.ttl_seconds = 30L;
  lease = NULL;
  assert_int_equal(lc_acquire(client, &acquire, &lease, error), LC_OK);
  state_source = NULL;
  assert_int_equal(
      lc_source_from_memory(state, strlen(state), &state_source, error), LC_OK);
  assert_int_equal(lc_lease_update(lease, state_source, NULL, error), LC_OK);
  lc_source_close(state_source);
  assert_int_equal(lc_lease_release(lease, NULL, error), LC_OK);
}

typedef struct workflow_process_result {
  int rc;
  int got_job;
  unsigned long delivered;
  char error_message[256];
  char error_detail[256];
} workflow_process_result;

static workflow_process_result
workflow_shared_process_claim(const char *root, const char *namespace_name,
                              int start_fd) {
  workflow_process_result result;
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_client *client;
  lc_workflow *workflow;
  lc_outbox_job *job;
  lc_error error;
  char start;

  memset(&result, 0, sizeof(result));
  result.rc = LC_ERR_TRANSPORT;
  if (read(start_fd, &start, 1U) != 1) {
    (void)close(start_fd);
    return result;
  }
  (void)close(start_fd);
  if (snprintf(endpoint, sizeof(endpoint), "pouch://%s?single_writer=false",
               root) < 0)
    return result;
  endpoints[0] = endpoint;
  client = NULL;
  workflow = NULL;
  job = NULL;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  result.rc = lc_client_open(&client_config, &client, &error);
  if (result.rc == LC_OK) {
    lc_workflow_config_init(&workflow_config);
    workflow_config.namespace_name = namespace_name;
    workflow_config.owner = "workflow-shared-child";
    workflow_config.recovery_interval_seconds = 1L;
    result.rc =
        lc_client_new_workflow(client, &workflow_config, &workflow, &error);
  }
  if (result.rc == LC_OK)
    result.rc = lc_workflow_next(workflow, 5000L, &job, &error);
  if (result.rc == LC_OK && job != NULL) {
    result.got_job = 1;
    result.rc = lc_outbox_job_complete(job, NULL, &error);
    if (result.rc == LC_OK)
      result.delivered = 1UL;
  }
  if (job != NULL)
    lc_outbox_job_close(job);
  if (workflow != NULL)
    lc_workflow_close(workflow);
  if (client != NULL)
    lc_client_close(client);
  if (error.message != NULL)
    (void)snprintf(result.error_message, sizeof(result.error_message), "%s",
                   error.message);
  if (error.detail != NULL)
    (void)snprintf(result.error_detail, sizeof(result.error_detail), "%s",
                   error.detail);
  lc_error_cleanup(&error);
  return result;
}

static workflow_process_result
workflow_shared_process_drain(const char *root, const char *namespace_name,
                              const char *owner, int start_fd) {
  workflow_process_result result;
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_client *client;
  lc_workflow *workflow;
  lc_error error;
  char start;
  unsigned long idle_count;

  memset(&result, 0, sizeof(result));
  result.rc = LC_ERR_TRANSPORT;
  if (read(start_fd, &start, 1U) != 1) {
    (void)close(start_fd);
    return result;
  }
  (void)close(start_fd);
  if (start != 's' ||
      snprintf(endpoint, sizeof(endpoint),
               "pouch://%s?single_writer=false&segment_target_bytes=65536",
               root) < 0) {
    result.rc = LC_ERR_INVALID;
    return result;
  }
  endpoints[0] = endpoint;
  client = NULL;
  workflow = NULL;
  idle_count = 0U;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  result.rc = lc_client_open(&client_config, &client, &error);
  if (result.rc == LC_OK) {
    lc_workflow_config_init(&workflow_config);
    workflow_config.namespace_name = namespace_name;
    workflow_config.owner = owner;
    workflow_config.notification_capacity = 16U;
    workflow_config.recovery_interval_seconds = 1L;
    result.rc =
        lc_client_new_workflow(client, &workflow_config, &workflow, &error);
  }
  while (result.rc == LC_OK &&
         idle_count < WORKFLOW_SHARED_PROCESS_IDLE_LIMIT) {
    lc_outbox_job *job;

    job = NULL;
    result.rc = lc_workflow_next(workflow, 250L, &job, &error);
    if (result.rc != LC_OK)
      break;
    if (job == NULL) {
      ++idle_count;
      continue;
    }
    idle_count = 0U;
    result.got_job = 1;
    result.rc = lc_outbox_job_complete(job, NULL, &error);
    lc_outbox_job_close(job);
    if (result.rc == LC_OK)
      ++result.delivered;
  }
  if (workflow != NULL)
    lc_workflow_close(workflow);
  if (client != NULL)
    lc_client_close(client);
  if (error.message != NULL)
    (void)snprintf(result.error_message, sizeof(result.error_message), "%s",
                   error.message);
  if (error.detail != NULL)
    (void)snprintf(result.error_detail, sizeof(result.error_detail), "%s",
                   error.detail);
  lc_error_cleanup(&error);
  return result;
}

static void workflow_assert_outbox_completed(lc_client *client, const char *key,
                                             lc_error *error) {
  lc_get_opts options;
  lc_get_res result;
  lc_sink *sink;
  const void *bytes;
  size_t length;

  lc_get_opts_init(&options);
  options.public_read = 1;
  memset(&result, 0, sizeof(result));
  sink = NULL;
  bytes = NULL;
  length = 0U;
  assert_int_equal(lc_sink_to_memory(&sink, error), LC_OK);
  assert_int_equal(lc_get(client, key, &options, sink, &result, error), LC_OK);
  assert_false(result.no_content);
  assert_int_equal(lc_sink_memory_bytes(sink, &bytes, &length, error), LC_OK);
  assert_true(workflow_bytes_contains(bytes, length,
                                      "\"dispatch_state\":\"completed\""));
  lc_get_res_cleanup(&result);
  lc_sink_close(sink);
}

static void workflow_assert_public_state_absent(lc_client *client,
                                                const char *key,
                                                lc_error *error) {
  lc_get_opts options;
  lc_get_res result;
  lc_sink *sink;

  lc_get_opts_init(&options);
  options.public_read = 1;
  memset(&result, 0, sizeof(result));
  sink = NULL;
  assert_int_equal(lc_sink_to_memory(&sink, error), LC_OK);
  assert_int_equal(lc_get(client, key, &options, sink, &result, error), LC_OK);
  assert_true(result.no_content);
  lc_get_res_cleanup(&result);
  lc_sink_close(sink);
}

static int workflow_force_pouch_txn_decision_failure(void *context,
                                                     lc_error *error) {
  int *calls = (int *)context;

  ++*calls;
  return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "forced workflow terminal decision failure", NULL, NULL,
                      "pouch");
}

static void workflow_reconcile_overflow_commit_hook(void *context) {
  workflow_reconcile_overflow_hook *hook =
      (workflow_reconcile_overflow_hook *)context;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_workflow_transaction *transaction;
  lc_source *payload;
  lc_error error;

  if (hook == NULL || hook->workflow == NULL || hook->calls != 0)
    return;
  ++hook->calls;
  hook->rc = LC_ERR_INVALID;
  lc_error_init(&error);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "reconcile-overflow-operation";
  entry.effect_id = "reconcile-overflow-effect";
  entry.effect_key = "reconcile-overflow-effect-key";
  entry.kind = "test";
  entry.destination = "reconcile://overflow";
  entry.content_type = "text/plain";
  lc_outbox_receipt_init(&receipt);
  transaction = NULL;
  payload = NULL;
  hook->rc = lc_source_from_memory("overflow", 8U, &payload, &error);
  if (hook->rc == LC_OK) {
    hook->rc = lc_workflow_append_outbox(hook->workflow, &entry, payload,
                                         &transaction, &receipt, &error);
  }
  if (hook->rc == LC_OK) {
    hook->rc = lc_workflow_transaction_commit(transaction, &error);
  }
  if (transaction != NULL)
    lc_workflow_transaction_close(transaction);
  if (payload != NULL)
    lc_source_close(payload);
  lc_outbox_receipt_cleanup(&receipt);
  lc_error_cleanup(&error);
}

static void
test_pouch_outbox_duplicate_rejects_immutable_envelope_conflicts(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_outbox_entry entry;
  lc_client *client;
  lc_workflow *workflow;
  lc_workflow_transaction *transaction;
  lc_outbox_receipt receipt;
  lc_source *payload;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "immutable-envelope-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "workflow";
  workflow_config.owner = "workflow-immutable-envelope";
  workflow = NULL;
  assert_int_equal(
      lc_client_new_workflow(client, &workflow_config, &workflow, &error),
      LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "immutable-envelope-operation";
  entry.effect_id = "immutable-envelope-effect";
  entry.effect_key = "immutable-envelope-effect-key";
  entry.causation_id = "command-immutable-1";
  entry.kind = "http";
  entry.schema_version = "v1";
  entry.destination = "https://example.invalid/immutable-envelope";
  entry.content_type = "text/plain";
  entry.headers_json = "{\"x-request-id\":\"first\"}";
  entry.trace_context = "trace-first";
  payload = NULL;
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  lc_outbox_receipt_init(&receipt);
  transaction = NULL;
  assert_int_equal(lc_workflow_append_outbox(workflow, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_OK);
  assert_non_null(transaction);
  assert_int_equal(lc_workflow_transaction_commit(transaction, &error), LC_OK);
  lc_workflow_transaction_close(transaction);
  transaction = NULL;

  entry.content_type = "application/json";
  assert_int_equal(lc_workflow_append_outbox(workflow, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_ERR_SERVER);
  assert_null(transaction);
  assert_null(receipt.outbox_key);
  assert_false(receipt.duplicate);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  entry.content_type = "text/plain";

  entry.headers_json = "{\"x-request-id\":\"second\"}";
  assert_int_equal(lc_workflow_append_outbox(workflow, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_ERR_SERVER);
  assert_null(transaction);
  assert_null(receipt.outbox_key);
  assert_false(receipt.duplicate);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  entry.headers_json = "{\"x-request-id\":\"first\"}";

  entry.causation_id = "command-immutable-2";
  assert_int_equal(lc_workflow_append_outbox(workflow, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_ERR_SERVER);
  assert_null(transaction);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  entry.causation_id = "command-immutable-1";

  entry.schema_version = "v2";
  assert_int_equal(lc_workflow_append_outbox(workflow, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_ERR_SERVER);
  assert_null(transaction);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  entry.schema_version = "v1";

  entry.trace_context = "trace-second";
  assert_int_equal(lc_workflow_append_outbox(workflow, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_ERR_SERVER);
  assert_null(transaction);
  assert_null(receipt.outbox_key);
  assert_false(receipt.duplicate);

  lc_outbox_receipt_cleanup(&receipt);
  lc_source_close(payload);
  lc_workflow_close(workflow);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void test_pouch_outbox_transaction_and_duplicate(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_outbox_entry entry;
  lc_workflow_participant_request participant_request;
  lc_client *client;
  lc_workflow *workflow;
  lc_workflow_transaction *transaction;
  lc_workflow_transaction *duplicate_transaction;
  lc_workflow_participant *participant;
  lc_outbox_receipt receipt;
  lc_outbox_receipt duplicate_receipt;
  lc_inbox_message inbox;
  lc_inbox_accept_result inbox_result;
  lc_workflow_transaction *inbox_transaction;
  lc_outbox_job *job;
  lc_source *payload;
  lc_source *state_source;
  lc_sink *payload_sink;
  const void *payload_bytes;
  size_t payload_length;
  size_t payload_written;
  lc_error error;
  char long_consumer_id[256];

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "core-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  memset(long_consumer_id, 'c', sizeof(long_consumer_id) - 1U);
  long_consumer_id[sizeof(long_consumer_id) - 1U] = '\0';
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "workflow";
  workflow_config.owner = "workflow-test";
  workflow = NULL;
  assert_int_equal(
      lc_client_new_workflow(client, &workflow_config, &workflow, &error),
      LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "operation-1";
  entry.effect_id = "effect-1";
  entry.effect_key = "foreign-idempotency-\"1\\stable";
  entry.kind = "http";
  entry.destination = "https://example.invalid/effect?target=\"primary\"";
  entry.content_type = "text/plain";
  entry.trace_context = "trace-\"quoted\"";
  payload = NULL;
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  lc_outbox_receipt_init(&receipt);
  transaction = NULL;
  assert_int_equal(lc_workflow_append_outbox(workflow, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_OK);
  assert_non_null(transaction);
  assert_non_null(receipt.outbox_key);
  lc_workflow_participant_request_init(&participant_request);
  participant_request.acquire.namespace_name = "domain";
  participant_request.acquire.key = "order-1";
  participant_request.acquire.owner = "orders";
  participant_request.acquire.ttl_seconds = 30L;
  participant = NULL;
  assert_int_equal(lc_workflow_transaction_acquire(
                       transaction, &participant_request, &participant, &error),
                   LC_OK);
  assert_non_null(participant);
  assert_non_null(participant->txn_id);
  state_source = NULL;
  assert_int_equal(lc_source_from_memory("{\"status\":\"paid\"}", 17U,
                                         &state_source, &error),
                   LC_OK);
  assert_int_equal(participant->update(participant, state_source, NULL, &error),
                   LC_OK);
  lc_source_close(state_source);
  lc_workflow_participant_close(participant);
  assert_int_equal(lc_workflow_transaction_commit(transaction, &error), LC_OK);
  lc_workflow_transaction_close(transaction);
  job = NULL;
  assert_int_equal(lc_workflow_next(workflow, 2000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_string_equal(job->effect_key, entry.effect_key);
  assert_string_equal(job->destination, entry.destination);
  assert_int_equal(job->attempt, 1);
  lc_source_close(payload);
  payload = NULL;
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  lc_outbox_receipt_init(&duplicate_receipt);
  duplicate_transaction = (lc_workflow_transaction *)1;
  assert_int_equal(lc_workflow_append_outbox(workflow, &entry, payload,
                                             &duplicate_transaction,
                                             &duplicate_receipt, &error),
                   LC_OK);
  assert_null(duplicate_transaction);
  assert_true(duplicate_receipt.duplicate);
  assert_string_equal(receipt.outbox_key, duplicate_receipt.outbox_key);
  payload_sink = NULL;
  payload_bytes = NULL;
  payload_length = 0U;
  payload_written = 0U;
  assert_int_equal(lc_sink_to_memory(&payload_sink, &error), LC_OK);
  assert_int_equal(
      lc_outbox_job_write_payload(job, payload_sink, &payload_written, &error),
      LC_OK);
  assert_int_equal(lc_sink_memory_bytes(payload_sink, &payload_bytes,
                                        &payload_length, &error),
                   LC_OK);
  assert_int_equal(payload_written, 7U);
  assert_int_equal(payload_length, 7U);
  assert_memory_equal(payload_bytes, "payload", 7U);
  lc_sink_close(payload_sink);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  lc_outbox_job_close(job);
  lc_source_close(payload);
  payload = NULL;
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_receipt_init(&receipt);
  entry.effect_id = "effect-retry";
  entry.effect_key = "foreign-idempotency-retry";
  assert_int_equal(
      lc_source_from_memory("retry-payload", 13U, &payload, &error), LC_OK);
  transaction = NULL;
  assert_int_equal(lc_workflow_append_outbox(workflow, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_OK);
  assert_non_null(transaction);
  assert_int_equal(lc_workflow_transaction_commit(transaction, &error), LC_OK);
  lc_workflow_transaction_close(transaction);
  job = NULL;
  assert_int_equal(lc_workflow_next(workflow, 2000L, &job, &error), LC_OK);
  assert_non_null(job);
  {
    lc_outbox_retry retry;
    lc_outbox_retry_init(&retry);
    retry.delay_seconds = 3601L;
    assert_int_equal(lc_outbox_job_retry(job, &retry, &error), LC_ERR_INVALID);
    lc_error_cleanup(&error);
    lc_error_init(&error);
    retry.delay_seconds = 1L;
    assert_int_equal(lc_outbox_job_retry(job, &retry, &error), LC_OK);
  }
  lc_outbox_job_close(job);
  job = NULL;
  assert_int_equal(lc_workflow_next(workflow, 3000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_string_equal(job->effect_key, "foreign-idempotency-retry");
  assert_int_equal(job->attempt, 2);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  lc_outbox_job_close(job);
  lc_source_close(payload);
  payload = NULL;
  entry.effect_id = "effect-1";
  entry.effect_key = "foreign-idempotency-\"1\\stable";
  lc_outbox_receipt_cleanup(&duplicate_receipt);
  lc_inbox_message_init(&inbox);
  inbox.consumer_id = long_consumer_id;
  inbox.source_kind = "http";
  inbox.source_id = "gateway";
  inbox.message_id = "message-1";
  inbox.payload_digest = "abc123";
  inbox.operation_id = "operation-inbox-1";
  inbox_transaction = NULL;
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_workflow_accept_inbox(workflow, &inbox,
                                            &inbox_transaction, &inbox_result,
                                            &error),
                   LC_OK);
  assert_true(inbox_result.accepted);
  assert_non_null(inbox_transaction);
  assert_int_equal(lc_workflow_transaction_commit(inbox_transaction, &error),
                   LC_OK);
  lc_workflow_transaction_close(inbox_transaction);
  inbox_transaction = (lc_workflow_transaction *)1;
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_workflow_accept_inbox(workflow, &inbox,
                                            &inbox_transaction, &inbox_result,
                                            &error),
                   LC_OK);
  assert_null(inbox_transaction);
  assert_true(inbox_result.duplicate);
  inbox.operation_id = "operation-inbox-conflict";
  inbox_transaction = (lc_workflow_transaction *)1;
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_workflow_accept_inbox(workflow, &inbox,
                                            &inbox_transaction, &inbox_result,
                                            &error),
                   LC_ERR_SERVER);
  assert_null(inbox_transaction);
  assert_false(inbox_result.duplicate);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_outbox_receipt_cleanup(&receipt);
  lc_workflow_close(workflow);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void
test_pouch_participant_cleanup_after_transaction_close(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_inbox_message inbox;
  lc_inbox_accept_result inbox_result;
  lc_workflow_participant_request participant_request;
  lc_client *client;
  lc_workflow *workflow;
  lc_workflow_transaction *transaction;
  lc_workflow_participant *participant;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "participant-lifetime-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  workflow = NULL;
  transaction = NULL;
  participant = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "participant-lifetime";
  workflow_config.owner = "participant-lifetime-test";
  assert_int_equal(
      lc_client_new_workflow(client, &workflow_config, &workflow, &error),
      LC_OK);
  lc_inbox_message_init(&inbox);
  inbox.consumer_id = "participant-lifetime-consumer";
  inbox.source_kind = "http";
  inbox.source_id = "participant-lifetime-source";
  inbox.message_id = "participant-lifetime-message";
  inbox.payload_digest = "participant-lifetime-digest";
  inbox.operation_id = "participant-lifetime-operation";
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_workflow_accept_inbox(workflow, &inbox, &transaction,
                                            &inbox_result, &error),
                   LC_OK);
  assert_non_null(transaction);
  lc_workflow_participant_request_init(&participant_request);
  participant_request.acquire.namespace_name = "participant-lifetime";
  participant_request.acquire.key = "participant-lifetime-domain";
  participant_request.acquire.owner = "participant-lifetime-test";
  participant_request.acquire.ttl_seconds = 30L;
  assert_int_equal(lc_workflow_transaction_acquire(
                       transaction, &participant_request, &participant, &error),
                   LC_OK);
  assert_non_null(participant);
  assert_string_equal(participant->key, "participant-lifetime-domain");
  assert_int_equal(lc_workflow_transaction_commit(transaction, &error), LC_OK);

  /* Terminal leases are owned by the transaction. Views must fail closed
   * rather than retain a pointer to a released lease. */
  assert_int_equal(participant->describe(participant, &error), LC_ERR_INVALID);
  assert_null(participant->key);
  assert_null(participant->txn_id);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  /* This is the normal cleanup order when a host retains a participant view:
   * close the workflow and transaction first, then close the view. */
  lc_workflow_close(workflow);
  workflow = NULL;
  lc_workflow_transaction_close(transaction);
  transaction = NULL;
  assert_int_equal(participant->describe(participant, &error), LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_workflow_participant_close(participant);
  participant = NULL;

  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void
test_pouch_participant_allocation_failure_rolls_back_enrollment(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_inbox_message inbox;
  lc_inbox_accept_result inbox_result;
  lc_workflow_participant_request participant_request;
  lc_client *client;
  lc_workflow *workflow;
  lc_workflow_transaction *transaction;
  lc_workflow_participant *participant;
  lc_error error;
  int rc;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "participant-oom-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  workflow = NULL;
  transaction = NULL;
  participant = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "participant-oom";
  workflow_config.owner = "participant-oom-test";
  assert_int_equal(
      lc_client_new_workflow(client, &workflow_config, &workflow, &error),
      LC_OK);
  lc_inbox_message_init(&inbox);
  inbox.consumer_id = "participant-oom-consumer";
  inbox.source_kind = "http";
  inbox.source_id = "participant-oom-source";
  inbox.message_id = "participant-oom-message";
  inbox.payload_digest = "participant-oom-digest";
  inbox.operation_id = "participant-oom-operation";
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_workflow_accept_inbox(workflow, &inbox, &transaction,
                                            &inbox_result, &error),
                   LC_OK);
  lc_workflow_participant_request_init(&participant_request);
  participant_request.acquire.namespace_name = "participant-oom";
  participant_request.acquire.key = "participant-oom-domain";
  participant_request.acquire.owner = "participant-oom-test";
  participant_request.acquire.ttl_seconds = 30L;

  workflow_reset_allocation_failures();
  lc_workflow_test_before_participant_allocation_hook =
      workflow_fail_allocation;
  rc = lc_workflow_transaction_acquire(transaction, &participant_request,
                                       &participant, &error);
  workflow_reset_allocation_failures();
  assert_int_equal(rc, LC_ERR_NOMEM);
  assert_null(participant);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  /* The implicit-XA rollback is terminal. A caller retries in a new workflow
   * transaction, never by committing the failed xid. */
  assert_int_equal(lc_workflow_transaction_commit(transaction, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_workflow_transaction_close(transaction);
  transaction = NULL;
  inbox.message_id = "participant-oom-retry-message";
  inbox.payload_digest = "participant-oom-retry-digest";
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_workflow_accept_inbox(workflow, &inbox, &transaction,
                                            &inbox_result, &error),
                   LC_OK);
  assert_int_equal(lc_workflow_transaction_acquire(
                       transaction, &participant_request, &participant, &error),
                   LC_OK);
  assert_non_null(participant);
  lc_workflow_participant_close(participant);
  assert_int_equal(lc_workflow_transaction_commit(transaction, &error), LC_OK);
  lc_workflow_transaction_close(transaction);
  lc_workflow_close(workflow);
  lc_client_close(client);
  workflow_reset_allocation_failures();
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void test_pouch_command_receipt_allocation_failure_rolls_back_enrollment(
    void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_inbox_message inbox;
  lc_inbox_accept_result inbox_result;
  lc_command_request command;
  lc_command_receipt receipt;
  lc_client *client;
  lc_workflow *workflow;
  lc_workflow_transaction *transaction;
  lc_error error;
  int rc;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "command-oom-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  workflow = NULL;
  transaction = NULL;
  lc_command_receipt_init(&receipt);
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "command-oom";
  workflow_config.owner = "command-oom-test";
  assert_int_equal(
      lc_client_new_workflow(client, &workflow_config, &workflow, &error),
      LC_OK);
  lc_inbox_message_init(&inbox);
  inbox.consumer_id = "command-oom-consumer";
  inbox.source_kind = "http";
  inbox.source_id = "command-oom-source";
  inbox.message_id = "command-oom-message";
  inbox.payload_digest = "command-oom-digest";
  inbox.operation_id = "command-oom-operation";
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_workflow_accept_inbox(workflow, &inbox, &transaction,
                                            &inbox_result, &error),
                   LC_OK);
  lc_command_request_init(&command);
  command.identity.scope = "command-oom-scope";
  command.identity.command_type = "command-oom-type";
  command.identity.idempotency_key = "command-oom-key";
  command.request_digest = "command-oom-digest";
  command.operation_id = "command-oom-operation";

  workflow_reset_allocation_failures();
  lc_workflow_test_before_command_receipt_copy_hook = workflow_fail_allocation;
  rc = lc_workflow_transaction_accept_command(transaction, &command, &receipt,
                                              &error);
  workflow_reset_allocation_failures();
  assert_int_equal(rc, LC_ERR_NOMEM);
  assert_null(receipt.command_id);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  assert_int_equal(lc_workflow_transaction_commit(transaction, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_workflow_transaction_close(transaction);
  transaction = NULL;
  inbox.message_id = "command-oom-retry-message";
  inbox.payload_digest = "command-oom-retry-digest";
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_workflow_accept_inbox(workflow, &inbox, &transaction,
                                            &inbox_result, &error),
                   LC_OK);
  assert_int_equal(lc_workflow_transaction_accept_command(transaction, &command,
                                                          &receipt, &error),
                   LC_OK);
  assert_int_equal(receipt.state, LC_COMMAND_PENDING);
  assert_int_equal(lc_workflow_transaction_commit(transaction, &error), LC_OK);
  lc_workflow_transaction_close(transaction);
  lc_command_receipt_cleanup(&receipt);
  assert_int_equal(lc_workflow_get_command_receipt(workflow, &command.identity,
                                                   &receipt, &error),
                   LC_OK);
  assert_false(receipt.duplicate);
  assert_int_equal(receipt.state, LC_COMMAND_PENDING);
  lc_command_receipt_cleanup(&receipt);
  lc_workflow_close(workflow);
  lc_client_close(client);
  workflow_reset_allocation_failures();
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void
test_pouch_outbox_allocation_failures_roll_back_enrollment(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_inbox_message inbox;
  lc_inbox_accept_result inbox_result;
  lc_workflow_participant_request participant_request;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_client *client;
  lc_workflow *workflow;
  lc_workflow_transaction *transaction;
  lc_workflow_participant *participant;
  lc_source *payload;
  lc_error error;
  size_t index;
  int rc;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "outbox-oom-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  workflow = NULL;
  transaction = NULL;
  participant = NULL;
  payload = NULL;
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "outbox-oom";
  workflow_config.owner = "outbox-oom-test";
  assert_int_equal(
      lc_client_new_workflow(client, &workflow_config, &workflow, &error),
      LC_OK);
  lc_inbox_message_init(&inbox);
  inbox.consumer_id = "outbox-oom-consumer";
  inbox.source_kind = "http";
  inbox.source_id = "outbox-oom-source";
  inbox.message_id = "outbox-oom-message";
  inbox.payload_digest = "outbox-oom-digest";
  inbox.operation_id = "outbox-oom-operation";
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_workflow_accept_inbox(workflow, &inbox, &transaction,
                                            &inbox_result, &error),
                   LC_OK);
  lc_workflow_participant_request_init(&participant_request);
  participant_request.acquire.namespace_name = "outbox-oom";
  participant_request.acquire.owner = "outbox-oom-test";
  participant_request.acquire.ttl_seconds = 30L;
  for (index = 0U; index < 3U; ++index) {
    char key[64];

    assert_true(snprintf(key, sizeof(key), "outbox-oom-domain-%lu",
                         (unsigned long)index) > 0);
    participant_request.acquire.key = key;
    assert_int_equal(lc_workflow_transaction_acquire(transaction,
                                                     &participant_request,
                                                     &participant, &error),
                     LC_OK);
    lc_workflow_participant_close(participant);
    participant = NULL;
  }
  lc_outbox_entry_init(&entry);
  entry.operation_id = "outbox-oom-operation";
  entry.effect_id = "outbox-oom-ledger";
  entry.effect_key = "outbox-oom-ledger-effect";
  entry.kind = "test";
  entry.destination = "outbox://oom-ledger";
  entry.content_type = "text/plain";
  assert_int_equal(lc_source_from_memory("outbox", 6U, &payload, &error),
                   LC_OK);

  /* The ledger grows after staging the outbox attachment. Its allocation
   * failure must roll that staged lease back, so the exact retry succeeds. */
  workflow_reset_allocation_failures();
  lc_workflow_test_before_ledger_append_hook = workflow_fail_allocation;
  rc = lc_workflow_transaction_append_outbox(transaction, &entry, payload,
                                             &receipt, &error);
  workflow_reset_allocation_failures();
  assert_int_equal(rc, LC_ERR_NOMEM);
  assert_null(receipt.outbox_key);
  lc_source_close(payload);
  payload = NULL;
  lc_error_cleanup(&error);
  lc_error_init(&error);

  assert_int_equal(lc_workflow_transaction_commit(transaction, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_workflow_transaction_close(transaction);
  transaction = NULL;
  inbox.message_id = "outbox-oom-ledger-retry-message";
  inbox.payload_digest = "outbox-oom-ledger-retry-digest";
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_workflow_accept_inbox(workflow, &inbox, &transaction,
                                            &inbox_result, &error),
                   LC_OK);
  assert_int_equal(lc_source_from_memory("outbox", 6U, &payload, &error),
                   LC_OK);
  assert_int_equal(lc_workflow_transaction_append_outbox(
                       transaction, &entry, payload, &receipt, &error),
                   LC_OK);
  lc_source_close(payload);
  payload = NULL;
  lc_outbox_receipt_cleanup(&receipt);
  assert_int_equal(lc_workflow_transaction_commit(transaction, &error), LC_OK);
  lc_workflow_transaction_close(transaction);
  transaction = NULL;

  /* The receipt copy happens after the outbox is enrolled. Failing it must
   * likewise remove the staged effect before the caller retries. */
  inbox.message_id = "outbox-oom-receipt-message";
  inbox.payload_digest = "outbox-oom-receipt-digest";
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_workflow_accept_inbox(workflow, &inbox, &transaction,
                                            &inbox_result, &error),
                   LC_OK);
  entry.effect_id = "outbox-oom-receipt";
  entry.effect_key = "outbox-oom-receipt-effect";
  entry.destination = "outbox://oom-receipt";
  assert_int_equal(lc_source_from_memory("receipt", 7U, &payload, &error),
                   LC_OK);
  workflow_reset_allocation_failures();
  lc_workflow_test_before_outbox_receipt_copy_hook = workflow_fail_allocation;
  rc = lc_workflow_transaction_append_outbox(transaction, &entry, payload,
                                             &receipt, &error);
  workflow_reset_allocation_failures();
  assert_int_equal(rc, LC_ERR_NOMEM);
  assert_null(receipt.outbox_key);
  lc_source_close(payload);
  payload = NULL;
  lc_error_cleanup(&error);
  lc_error_init(&error);

  assert_int_equal(lc_workflow_transaction_commit(transaction, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_workflow_transaction_close(transaction);
  transaction = NULL;
  inbox.message_id = "outbox-oom-receipt-retry-message";
  inbox.payload_digest = "outbox-oom-receipt-retry-digest";
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_workflow_accept_inbox(workflow, &inbox, &transaction,
                                            &inbox_result, &error),
                   LC_OK);
  assert_int_equal(lc_source_from_memory("receipt", 7U, &payload, &error),
                   LC_OK);
  assert_int_equal(lc_workflow_transaction_append_outbox(
                       transaction, &entry, payload, &receipt, &error),
                   LC_OK);
  lc_source_close(payload);
  lc_outbox_receipt_cleanup(&receipt);
  assert_int_equal(lc_workflow_transaction_commit(transaction, &error), LC_OK);
  lc_workflow_transaction_close(transaction);
  lc_workflow_close(workflow);
  lc_client_close(client);
  workflow_reset_allocation_failures();
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void
test_pouch_notification_allocation_failure_reconciles_committed_outbox(
    void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_client *client;
  lc_workflow *workflow;
  lc_workflow_transaction *transaction;
  lc_outbox_job *job;
  lc_source *payload;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "notify-oom-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  workflow = NULL;
  transaction = NULL;
  job = NULL;
  payload = NULL;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "notify-oom";
  workflow_config.owner = "notify-oom-test";
  assert_int_equal(
      lc_client_new_workflow(client, &workflow_config, &workflow, &error),
      LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "notify-oom-operation";
  entry.effect_id = "notify-oom-effect";
  entry.effect_key = "notify-oom-key";
  entry.kind = "test";
  entry.destination = "test://notify-oom";
  entry.content_type = "text/plain";
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  assert_int_equal(lc_workflow_append_outbox(workflow, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_OK);
  assert_non_null(transaction);

  /* The commit is durable before its best-effort direct-key signal. Losing
   * that allocation must make the private dispatcher reconcile instead. */
  lc_workflow_test_before_notification_copy_hook = workflow_fail_allocation;
  assert_int_equal(lc_workflow_transaction_commit(transaction, &error), LC_OK);
  lc_workflow_test_before_notification_copy_hook = NULL;
  lc_workflow_transaction_close(transaction);
  transaction = NULL;
  assert_int_equal(lc_workflow_next(workflow, workflow_claim_next_timeout_ms(),
                                    &job, &error),
                   LC_OK);
  assert_non_null(job);
  assert_string_equal(job->effect_key, entry.effect_key);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  lc_outbox_job_close(job);
  lc_source_close(payload);
  lc_outbox_receipt_cleanup(&receipt);
  lc_workflow_close(workflow);
  lc_client_close(client);
  workflow_reset_allocation_failures();
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void
test_pouch_retry_notification_allocation_failure_recovers_at_deadline(
    void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_outbox_retry retry;
  lc_client *client;
  lc_workflow *workflow;
  lc_workflow_transaction *transaction;
  lc_outbox_job *job;
  lc_source *payload;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "retry-oom-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  workflow = NULL;
  transaction = NULL;
  job = NULL;
  payload = NULL;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "retry-oom";
  workflow_config.owner = "retry-oom-test";
  assert_int_equal(
      lc_client_new_workflow(client, &workflow_config, &workflow, &error),
      LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "retry-oom-operation";
  entry.effect_id = "retry-oom-effect";
  entry.effect_key = "retry-oom-key";
  entry.kind = "test";
  entry.destination = "test://retry-oom";
  entry.content_type = "text/plain";
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  assert_int_equal(lc_workflow_append_outbox(workflow, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_OK);
  assert_int_equal(lc_workflow_transaction_commit(transaction, &error), LC_OK);
  lc_workflow_transaction_close(transaction);
  transaction = NULL;
  assert_int_equal(lc_workflow_next(workflow, workflow_claim_next_timeout_ms(),
                                    &job, &error),
                   LC_OK);
  assert_non_null(job);
  lc_outbox_retry_init(&retry);
  retry.delay_seconds = 1L;

  /* `retry_wait` is already durable when its delayed-key allocation fails.
   * The dispatcher must retain a recovery deadline instead of stranding it. */
  lc_workflow_test_before_notification_copy_hook = workflow_fail_allocation;
  assert_int_equal(lc_outbox_job_retry(job, &retry, &error), LC_OK);
  lc_workflow_test_before_notification_copy_hook = NULL;
  lc_outbox_job_close(job);
  job = NULL;
  assert_int_equal(lc_workflow_next(workflow, workflow_claim_next_timeout_ms(),
                                    &job, &error),
                   LC_OK);
  assert_non_null(job);
  assert_int_equal(job->attempt, 2);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  lc_outbox_job_close(job);
  lc_source_close(payload);
  lc_outbox_receipt_cleanup(&receipt);
  lc_workflow_close(workflow);
  lc_client_close(client);
  workflow_reset_allocation_failures();
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void test_pouch_transient_claim_failure_is_rescheduled(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_workflow_stats stats;
  lc_client *client;
  lc_workflow *workflow;
  lc_workflow_transaction *transaction;
  lc_outbox_job *job;
  lc_source *payload;
  lc_error error;
  workflow_fail_once failure;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "claim-retry-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  workflow = NULL;
  transaction = NULL;
  job = NULL;
  payload = NULL;
  memset(&failure, 0, sizeof(failure));
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "claim-retry";
  workflow_config.owner = "claim-retry-test";
  assert_int_equal(
      lc_client_new_workflow(client, &workflow_config, &workflow, &error),
      LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "claim-retry-operation";
  entry.effect_id = "claim-retry-effect";
  entry.effect_key = "claim-retry-key";
  entry.kind = "test";
  entry.destination = "test://claim-retry";
  entry.content_type = "text/plain";
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  assert_int_equal(lc_workflow_append_outbox(workflow, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_OK);
  lc_workflow_test_before_claim_outbox_hook = workflow_fail_first_call;
  lc_workflow_test_before_claim_outbox_context = &failure;
  assert_int_equal(lc_workflow_transaction_commit(transaction, &error), LC_OK);
  lc_workflow_transaction_close(transaction);
  transaction = NULL;
  assert_int_equal(lc_workflow_next(workflow, workflow_claim_next_timeout_ms(),
                                    &job, &error),
                   LC_OK);
  assert_non_null(job);
  assert_int_equal(failure.calls, 2U);
  lc_workflow_stats_init(&stats);
  assert_int_equal(lc_workflow_get_stats(workflow, &stats, &error), LC_OK);
  assert_true(stats.claim_losses >= 1U);
  lc_workflow_stats_cleanup(&stats);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  lc_outbox_job_close(job);
  lc_source_close(payload);
  lc_outbox_receipt_cleanup(&receipt);
  lc_workflow_close(workflow);
  lc_client_close(client);
  workflow_reset_allocation_failures();
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void
test_pouch_claim_recovery_allocation_failure_recovers_at_expiry(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_client *client;
  lc_workflow *workflow;
  lc_workflow_transaction *transaction;
  lc_outbox_job *first, *replacement;
  lc_source *payload;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "claim-recovery-oom-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  workflow = NULL;
  transaction = NULL;
  first = NULL;
  replacement = NULL;
  payload = NULL;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "claim-recovery-oom";
  workflow_config.owner = "claim-recovery-oom-test";
  workflow_config.claim_ttl_seconds = workflow_claim_ttl_seconds();
  assert_int_equal(
      lc_client_new_workflow(client, &workflow_config, &workflow, &error),
      LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "claim-recovery-oom-operation";
  entry.effect_id = "claim-recovery-oom-effect";
  entry.effect_key = "claim-recovery-oom-key";
  entry.kind = "test";
  entry.destination = "test://claim-recovery-oom";
  entry.content_type = "text/plain";
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);
  assert_int_equal(lc_workflow_append_outbox(workflow, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_OK);
  assert_int_equal(lc_workflow_transaction_commit(transaction, &error), LC_OK);
  lc_workflow_transaction_close(transaction);
  transaction = NULL;
  assert_int_equal(lc_workflow_next(workflow, workflow_claim_next_timeout_ms(),
                                    &first, &error),
                   LC_OK);
  assert_non_null(first);
  lc_workflow_test_before_notification_copy_hook = workflow_fail_allocation;
  lc_outbox_job_close(first);
  first = NULL;
  lc_workflow_test_before_notification_copy_hook = NULL;
  assert_int_equal(lc_workflow_next(workflow, workflow_claim_next_timeout_ms(),
                                    &replacement, &error),
                   LC_OK);
  assert_non_null(replacement);
  assert_int_equal(replacement->attempt, 2);
  assert_int_equal(lc_outbox_job_complete(replacement, NULL, &error), LC_OK);
  lc_outbox_job_close(replacement);
  lc_source_close(payload);
  lc_outbox_receipt_cleanup(&receipt);
  lc_workflow_close(workflow);
  lc_client_close(client);
  workflow_reset_allocation_failures();
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void
test_pouch_command_receipt_commits_with_outbox_and_result(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_command_request command;
  lc_command_receipt receipt, duplicate_receipt;
  lc_workflow_transaction *transaction, *duplicate_transaction;
  lc_workflow_participant_request participant_request;
  lc_workflow_participant *participant;
  lc_outbox_entry entry;
  lc_outbox_receipt outbox_receipt;
  lc_command_result command_result;
  lc_outbox_completion completion;
  lc_client *client;
  lc_workflow *workflow;
  lc_source *payload, *result_body, *domain_state;
  lc_sink *sink;
  const void *bytes;
  size_t length, written;
  lc_outbox_job *job;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "command-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  workflow = NULL;
  transaction = NULL;
  participant = NULL;
  payload = NULL;
  result_body = NULL;
  domain_state = NULL;
  job = NULL;
  lc_command_receipt_init(&receipt);
  lc_command_receipt_init(&duplicate_receipt);
  lc_outbox_receipt_init(&outbox_receipt);
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "command-workflow";
  workflow_config.owner = "command-owner";
  assert_int_equal(
      lc_client_new_workflow(client, &workflow_config, &workflow, &error),
      LC_OK);
  lc_command_request_init(&command);
  command.identity.scope = "tenant-a";
  command.identity.command_type = "orders.create.v1";
  command.identity.idempotency_key = "request-1";
  command.request_digest = "semantic-request-digest-1";
  command.operation_id = "order-operation-1";
  assert_int_equal(lc_workflow_accept_command(workflow, &command, &transaction,
                                              &receipt, &error),
                   LC_OK);
  assert_non_null(transaction);
  assert_int_equal(receipt.state, LC_COMMAND_PENDING);
  assert_false(receipt.duplicate);
  assert_non_null(receipt.command_id);
  lc_workflow_participant_request_init(&participant_request);
  participant_request.acquire.namespace_name = "orders";
  participant_request.acquire.key = "order-command-1";
  participant_request.acquire.owner = "orders";
  participant_request.acquire.ttl_seconds = 30L;
  assert_int_equal(lc_workflow_transaction_acquire(
                       transaction, &participant_request, &participant, &error),
                   LC_OK);
  assert_int_equal(lc_source_from_memory("{\"status\":\"created\"}", 20U,
                                         &domain_state, &error),
                   LC_OK);
  assert_int_equal(participant->update(participant, domain_state, NULL, &error),
                   LC_OK);
  lc_source_close(domain_state);
  domain_state = NULL;
  lc_workflow_participant_close(participant);
  participant = NULL;
  lc_outbox_entry_init(&entry);
  entry.operation_id = "order-operation-1";
  entry.effect_id = "order-created";
  entry.effect_key = "foreign-order-created-1";
  entry.kind = "http";
  entry.schema_version = "v1";
  entry.destination = "https://example.invalid/orders";
  entry.content_type = "text/plain";
  assert_int_equal(lc_source_from_memory("outbox", 6U, &payload, &error),
                   LC_OK);
  assert_int_equal(lc_workflow_transaction_append_outbox(
                       transaction, &entry, payload, &outbox_receipt, &error),
                   LC_OK);
  assert_int_equal(lc_source_from_memory("created", 7U, &result_body, &error),
                   LC_OK);
  lc_command_result_init(&command_result);
  command_result.result_code = "created";
  command_result.result_reference = "order-command-1";
  command_result.content_type = "text/plain";
  command_result.body = result_body;
  assert_int_equal(lc_workflow_transaction_complete_command(
                       transaction, &command_result, &error),
                   LC_OK);
  lc_source_close(result_body);
  result_body = NULL;
  assert_int_equal(lc_workflow_transaction_commit(transaction, &error), LC_OK);
  lc_workflow_transaction_close(transaction);
  transaction = NULL;
  lc_command_receipt_cleanup(&receipt);
  assert_int_equal(lc_workflow_get_command_receipt(workflow, &command.identity,
                                                   &receipt, &error),
                   LC_OK);
  assert_int_equal(receipt.state, LC_COMMAND_COMPLETED);
  assert_string_equal(receipt.result_code, "created");
  assert_true(receipt.has_result_body);
  sink = NULL;
  bytes = NULL;
  length = 0U;
  written = 0U;
  assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
  assert_int_equal(lc_workflow_write_command_result(workflow, &command.identity,
                                                    sink, &written, &error),
                   LC_OK);
  assert_int_equal(lc_sink_memory_bytes(sink, &bytes, &length, &error), LC_OK);
  assert_int_equal(written, 7U);
  assert_memory_equal(bytes, "created", 7U);
  lc_sink_close(sink);
  duplicate_transaction = (lc_workflow_transaction *)1;
  assert_int_equal(lc_workflow_accept_command(workflow, &command,
                                              &duplicate_transaction,
                                              &duplicate_receipt, &error),
                   LC_OK);
  assert_null(duplicate_transaction);
  assert_true(duplicate_receipt.duplicate);
  assert_int_equal(duplicate_receipt.state, LC_COMMAND_COMPLETED);
  command.request_digest = "conflicting-digest";
  assert_int_equal(lc_workflow_accept_command(workflow, &command,
                                              &duplicate_transaction,
                                              &duplicate_receipt, &error),
                   LC_ERR_SERVER);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  command.request_digest = "semantic-request-digest-1";
  assert_int_equal(lc_workflow_next(workflow, 2000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_non_null(job->message_id);
  assert_string_equal(job->causation_id, receipt.command_id);
  assert_string_equal(job->schema_version, "v1");
  lc_outbox_completion_init(&completion);
  completion.delivery_reference = "provider-delivery-1";
  completion.response_digest = "provider-response-digest";
  assert_int_equal(lc_outbox_job_complete(job, &completion, &error), LC_OK);
  lc_outbox_job_close(job);
  job = NULL;
  duplicate_transaction = (lc_workflow_transaction *)1;
  assert_int_equal(lc_workflow_resume_command(workflow, &command.identity,
                                              &duplicate_transaction,
                                              &duplicate_receipt, &error),
                   LC_OK);
  assert_null(duplicate_transaction);
  assert_int_equal(duplicate_receipt.state, LC_COMMAND_COMPLETED);
  lc_outbox_receipt_cleanup(&outbox_receipt);
  lc_command_receipt_cleanup(&duplicate_receipt);
  lc_command_receipt_cleanup(&receipt);
  lc_source_close(payload);
  lc_workflow_close(workflow);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void test_pouch_shared_command_resume_is_durable(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_command_request command;
  lc_command_receipt receipt;
  lc_command_result result;
  lc_workflow_transaction *transaction;
  lc_client *first_client, *second_client;
  lc_workflow *first_workflow, *second_workflow;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "command-shared-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint),
                       "pouch://%s?single_writer=false", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "command-shared";
  workflow_config.owner = "command-shared-first";
  first_client = NULL;
  first_workflow = NULL;
  second_client = NULL;
  second_workflow = NULL;
  transaction = NULL;
  lc_command_receipt_init(&receipt);
  lc_command_request_init(&command);
  command.identity.scope = "tenant-shared";
  command.identity.command_type = "orders.cancel.v1";
  command.identity.idempotency_key = "shared-request";
  command.request_digest = "shared-request-digest";
  assert_int_equal(lc_client_open(&client_config, &first_client, &error),
                   LC_OK);
  assert_int_equal(lc_client_new_workflow(first_client, &workflow_config,
                                          &first_workflow, &error),
                   LC_OK);
  assert_int_equal(lc_workflow_accept_command(first_workflow, &command,
                                              &transaction, &receipt, &error),
                   LC_OK);
  assert_int_equal(lc_workflow_transaction_commit(transaction, &error), LC_OK);
  lc_workflow_transaction_close(transaction);
  transaction = NULL;
  lc_workflow_close(first_workflow);
  lc_client_close(first_client);
  workflow_config.owner = "command-shared-second";
  assert_int_equal(lc_client_open(&client_config, &second_client, &error),
                   LC_OK);
  assert_int_equal(lc_client_new_workflow(second_client, &workflow_config,
                                          &second_workflow, &error),
                   LC_OK);
  lc_command_receipt_cleanup(&receipt);
  assert_int_equal(lc_workflow_resume_command(second_workflow,
                                              &command.identity, &transaction,
                                              &receipt, &error),
                   LC_OK);
  assert_non_null(transaction);
  lc_command_result_init(&result);
  result.failure_code = "cancelled";
  result.failure_message = "order was already cancelled";
  assert_int_equal(
      lc_workflow_transaction_fail_command(transaction, &result, &error),
      LC_OK);
  assert_int_equal(lc_workflow_transaction_commit(transaction, &error), LC_OK);
  lc_workflow_transaction_close(transaction);
  transaction = NULL;
  lc_command_receipt_cleanup(&receipt);
  assert_int_equal(lc_workflow_get_command_receipt(
                       second_workflow, &command.identity, &receipt, &error),
                   LC_OK);
  assert_int_equal(receipt.state, LC_COMMAND_FAILED);
  assert_string_equal(receipt.failure_code, "cancelled");
  lc_command_receipt_cleanup(&receipt);
  lc_workflow_close(second_workflow);
  lc_client_close(second_client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void
test_pouch_multikey_terminal_failure_publishes_nothing(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_workflow_participant_request participant_request;
  lc_client *client;
  lc_workflow *workflow;
  lc_workflow_transaction *transaction;
  lc_workflow_participant *participant;
  lc_source *payload;
  lc_source *domain_state;
  lc_error error;
  int decision_calls;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "atomic-terminal-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  workflow = NULL;
  transaction = NULL;
  participant = NULL;
  payload = NULL;
  domain_state = NULL;
  decision_calls = 0;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client_config.default_namespace = "workflow-atomic";
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "workflow-atomic";
  workflow_config.owner = "workflow-atomic-test";
  assert_int_equal(
      lc_client_new_workflow(client, &workflow_config, &workflow, &error),
      LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "atomic-operation";
  entry.effect_id = "atomic-effect";
  entry.effect_key = "atomic-effect-key";
  entry.kind = "test";
  entry.destination = "atomic://effect";
  entry.content_type = "text/plain";
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_source_from_memory("atomic", 6U, &payload, &error),
                   LC_OK);
  assert_int_equal(lc_workflow_append_outbox(workflow, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_OK);
  assert_non_null(transaction);
  lc_workflow_participant_request_init(&participant_request);
  participant_request.acquire.namespace_name = "workflow-atomic";
  participant_request.acquire.key = "domain-atomic";
  participant_request.acquire.owner = "workflow-atomic-test";
  participant_request.acquire.ttl_seconds = 30L;
  assert_int_equal(lc_workflow_transaction_acquire(
                       transaction, &participant_request, &participant, &error),
                   LC_OK);
  assert_int_equal(
      lc_source_from_memory("{\"committed\":true}", 18U, &domain_state, &error),
      LC_OK);
  assert_int_equal(participant->update(participant, domain_state, NULL, &error),
                   LC_OK);
  lc_source_close(domain_state);
  domain_state = NULL;
  lc_workflow_participant_close(participant);
  participant = NULL;

  /* A failed terminal decision must leave both the outbox intent and the
   * domain mutation private. Before the regression fix, the first implicit
   * outbox lease was committed before this second-participant decision. */
  lc_pouch_test_before_txn_decision_context = &decision_calls;
  lc_pouch_test_before_txn_decision_hook =
      workflow_force_pouch_txn_decision_failure;
  assert_int_equal(lc_workflow_transaction_commit(transaction, &error),
                   LC_ERR_TRANSPORT);
  assert_int_equal(decision_calls, 1);
  lc_pouch_test_before_txn_decision_hook = NULL;
  lc_pouch_test_before_txn_decision_context = NULL;
  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_workflow_transaction_close(transaction);
  transaction = NULL;
  workflow_assert_public_state_absent(client, receipt.outbox_key, &error);
  workflow_assert_public_state_absent(client, "domain-atomic", &error);

  lc_outbox_receipt_cleanup(&receipt);
  lc_source_close(payload);
  lc_workflow_close(workflow);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void test_pouch_reconciliation_retains_overflow_request(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  workflow_reconcile_overflow_hook hook;
  lc_workflow_stats stats;
  lc_client *client;
  lc_workflow *workflow;
  lc_outbox_job *job;
  lc_error error;
  int delivered_recovered;
  int delivered_overflow;
  size_t attempt;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "reconcile-overflow-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  client = NULL;
  workflow = NULL;
  job = NULL;
  delivered_recovered = 0;
  delivered_overflow = 0;
  memset(&hook, 0, sizeof(hook));
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client_config.default_namespace = "workflow-reconcile-overflow";
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "workflow-reconcile-overflow";
  workflow_config.owner = "workflow-reconcile-overflow-test";
  workflow_config.notification_capacity = 1U;
  assert_int_equal(
      lc_client_new_workflow(client, &workflow_config, &workflow, &error),
      LC_OK);

  /* Do not install the race hook until the mandatory empty startup sweep has
   * completed. The following explicit sweep is therefore the only one that
   * can add the second record while its notification queue is full. */
  memset(&stats, 0, sizeof(stats));
  for (attempt = 0U; attempt < 100U; ++attempt) {
    assert_int_equal(lc_workflow_get_stats(workflow, &stats, &error), LC_OK);
    if (stats.recovery_queries != 0U)
      break;
    lc_workflow_stats_cleanup(&stats);
    memset(&stats, 0, sizeof(stats));
    {
      struct timespec delay;
      delay.tv_sec = 0;
      delay.tv_nsec = 10000000L;
      (void)nanosleep(&delay, NULL);
    }
  }
  assert_true(stats.recovery_queries != 0U);
  lc_workflow_stats_cleanup(&stats);

  seed_recovery_outbox(client, "workflow-reconcile-overflow",
                       "__lockdc_io/v1/outbox/recovered", &error);
  hook.workflow = workflow;
  lc_workflow_test_after_reconcile_query_context = &hook;
  lc_workflow_test_after_reconcile_query_hook =
      workflow_reconcile_overflow_commit_hook;
  assert_int_equal(lc_workflow_reconcile(workflow, &error), LC_OK);
  for (attempt = 0U; attempt < 2U; ++attempt) {
    job = NULL;
    assert_int_equal(lc_workflow_next(workflow, 3000L, &job, &error), LC_OK);
    assert_non_null(job);
    if (strcmp(job->effect_key, "reconcile-overflow-effect-key") == 0)
      delivered_overflow = 1;
    else if (strcmp(job->effect_key, "recovery-key") == 0)
      delivered_recovered = 1;
    else
      fail_msg("unexpected reconciled outbox job");
    assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
    lc_outbox_job_close(job);
  }
  lc_workflow_test_after_reconcile_query_hook = NULL;
  lc_workflow_test_after_reconcile_query_context = NULL;
  assert_int_equal(hook.calls, 1);
  assert_int_equal(hook.rc, LC_OK);
  assert_true(delivered_recovered);
  assert_true(delivered_overflow);

  lc_workflow_close(workflow);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void test_pouch_dead_letter_operations(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_outbox_entry entry;
  lc_client *client;
  lc_workflow *workflow;
  lc_workflow_transaction *transaction;
  lc_outbox_receipt receipt;
  lc_outbox_job *job;
  lc_source *payload;
  lc_sink *sink;
  lc_dead_letter_export_opts export_options;
  lc_dead_letter_export_res export_result;
  lc_workflow_stats workflow_stats;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_attachment_list attachments;
  const void *bytes;
  size_t length;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "dead-letter-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  workflow = NULL;
  transaction = NULL;
  job = NULL;
  payload = NULL;
  sink = NULL;
  lease = NULL;
  memset(&attachments, 0, sizeof(attachments));
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "workflow-dead-letter";
  workflow_config.owner = "workflow-dead-letter-test";
  assert_int_equal(
      lc_client_new_workflow(client, &workflow_config, &workflow, &error),
      LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "dead-letter-operation";
  entry.effect_id = "dead-letter-effect";
  entry.effect_key = "foreign-dead-letter-idempotency-key";
  entry.kind = "http";
  entry.destination = "https://example.invalid/dead-letter";
  entry.content_type = "text/plain";
  assert_int_equal(lc_source_from_memory("payload-body", 12U, &payload, &error),
                   LC_OK);
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_workflow_append_outbox(workflow, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_OK);
  assert_int_equal(lc_workflow_transaction_commit(transaction, &error), LC_OK);
  lc_workflow_transaction_close(transaction);
  transaction = NULL;
  lc_source_close(payload);
  payload = NULL;
  assert_int_equal(lc_workflow_next(workflow, 3000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_int_equal(lc_outbox_job_dead_letter(job, "permanent failure", &error),
                   LC_OK);
  lc_outbox_job_close(job);
  job = NULL;
  seed_foreign_workflow_state(client, workflow_config.namespace_name,
                              "foreign-dead-letter", "dead_letter", &error);

  lc_dead_letter_export_res_init(&export_result);
  assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
  assert_int_equal(lc_workflow_export_dead_letters(workflow, NULL, sink,
                                                   &export_result, &error),
                   LC_OK);
  assert_int_equal(export_result.exported, 1U);
  bytes = NULL;
  length = 0U;
  assert_int_equal(lc_sink_memory_bytes(sink, &bytes, &length, &error), LC_OK);
  assert_true(length > 2U);
  assert_true(workflow_bytes_contains(bytes, length, "dead_letter"));
  assert_true(workflow_bytes_contains(bytes, length, entry.effect_key));
  assert_false(workflow_bytes_contains(bytes, length, "payload-body"));
  lc_sink_close(sink);
  sink = NULL;

  lc_workflow_stats_init(&workflow_stats);
  assert_int_equal(lc_workflow_get_stats(workflow, &workflow_stats, &error),
                   LC_OK);
  assert_true(workflow_stats.running);
  assert_true(workflow_stats.direct_notifications > 0U);
  lc_workflow_stats_cleanup(&workflow_stats);
  assert_int_equal(
      lc_workflow_replay_dead_letter(workflow, receipt.outbox_key, &error),
      LC_OK);
  assert_int_equal(lc_workflow_next(workflow, 3000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_string_equal(job->effect_key, entry.effect_key);
  assert_int_equal(job->attempt, 1);
  assert_int_equal(lc_outbox_job_dead_letter(job, "second failure", &error),
                   LC_OK);
  lc_outbox_job_close(job);
  job = NULL;

  lc_dead_letter_export_opts_init(&export_options);
  export_options.format = LC_DEAD_LETTER_EXPORT_JSONL;
  lc_dead_letter_export_res_init(&export_result);
  assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
  assert_int_equal(lc_workflow_export_dead_letters(
                       workflow, &export_options, sink, &export_result, &error),
                   LC_OK);
  assert_int_equal(export_result.exported, 1U);
  assert_int_equal(lc_sink_memory_bytes(sink, &bytes, &length, &error), LC_OK);
  assert_true(length > 0U && ((const char *)bytes)[length - 1U] == '\n');
  assert_true(
      workflow_bytes_contains(bytes, length, "prior_dead_letter_error"));
  assert_true(workflow_bytes_contains(bytes, length, "second failure"));
  lc_sink_close(sink);
  sink = NULL;

  assert_int_equal(
      lc_workflow_delete_dead_letter(workflow, receipt.outbox_key, &error),
      LC_OK);
  lc_acquire_req_init(&acquire);
  acquire.namespace_name = workflow_config.namespace_name;
  acquire.key = receipt.outbox_key;
  acquire.owner = "workflow-dead-letter-inspect";
  acquire.ttl_seconds = 30L;
  assert_int_equal(lc_acquire(client, &acquire, &lease, &error), LC_OK);
  assert_int_equal(lc_lease_list_attachments(lease, &attachments, &error),
                   LC_OK);
  assert_int_equal(attachments.count, 0U);
  lc_attachment_list_cleanup(&attachments);
  assert_int_equal(lc_lease_release(lease, NULL, &error), LC_OK);
  lease = NULL;

  entry.effect_id = "startup-replay-effect";
  entry.effect_key = "startup-replay-idempotency-key";
  assert_int_equal(
      lc_source_from_memory("startup-payload", 15U, &payload, &error), LC_OK);
  lc_outbox_receipt_cleanup(&receipt);
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_workflow_append_outbox(workflow, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_OK);
  assert_int_equal(lc_workflow_transaction_commit(transaction, &error), LC_OK);
  lc_workflow_transaction_close(transaction);
  transaction = NULL;
  lc_source_close(payload);
  payload = NULL;
  assert_int_equal(lc_workflow_next(workflow, 3000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_int_equal(lc_outbox_job_dead_letter(job, "startup replay", &error),
                   LC_OK);
  lc_outbox_job_close(job);
  job = NULL;
  lc_workflow_close(workflow);
  workflow = NULL;
  workflow_config.replay_dead_letters_on_startup = 1;
  assert_int_equal(
      lc_client_new_workflow(client, &workflow_config, &workflow, &error),
      LC_OK);
  assert_int_equal(lc_workflow_next(workflow, 5000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_string_equal(job->effect_key, entry.effect_key);
  assert_int_equal(job->attempt, 1);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  lc_outbox_job_close(job);
  lc_outbox_receipt_cleanup(&receipt);
  lc_workflow_close(workflow);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void test_pouch_startup_recovery_claims_seeded_outbox(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_client *client;
  lc_workflow *workflow;
  lc_outbox_job *job;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "recovery-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  seed_recovery_outbox(client, "workflow-recovery",
                       "__lockdc_io/v1/outbox/recovery", &error);
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "workflow-recovery";
  workflow_config.owner = "workflow-recovery-test";
  workflow = NULL;
  assert_int_equal(
      lc_client_new_workflow(client, &workflow_config, &workflow, &error),
      LC_OK);
  job = NULL;
  assert_int_equal(lc_workflow_next(workflow, 5000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_string_equal(job->effect_key, "recovery-key");
  assert_int_equal(job->attempt, 1);
  /* A handed-off job remains terminally usable after dispatcher shutdown. Its
   * retry is recovered durably by the replacement workflow, not lost with the
   * private in-memory scheduler. */
  lc_workflow_close(workflow);
  workflow = NULL;
  {
    lc_outbox_retry retry;
    lc_outbox_retry_init(&retry);
    retry.delay_seconds = 1L;
    assert_int_equal(lc_outbox_job_retry(job, &retry, &error), LC_OK);
  }
  lc_outbox_job_close(job);
  job = NULL;
  assert_int_equal(
      lc_client_new_workflow(client, &workflow_config, &workflow, &error),
      LC_OK);
  assert_int_equal(lc_workflow_next(workflow, 5000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_string_equal(job->effect_key, "recovery-key");
  assert_int_equal(job->attempt, 2);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  lc_outbox_job_close(job);
  lc_workflow_close(workflow);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void test_pouch_reopen_reconciles_durable_index_mode(int shared) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_index_flush_req flush_request;
  lc_index_flush_res flush_result;
  lc_query_req query_request;
  lc_query_key_handler query_handler;
  lc_query_res query_result;
  workflow_query_count query_count;
  lc_workflow_config workflow_config;
  lc_client *client;
  lc_workflow *workflow;
  lc_outbox_job *job;
  lc_error error;
  struct timespec started;
  struct timespec finished;
  size_t index;

  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "clean-reopen-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint),
                       shared ? "pouch://%s?single_writer=false" : "pouch://%s",
                       root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  for (index = 0U; index < WORKFLOW_CLEAN_REOPEN_FOREIGN_CHURN; ++index) {
    size_t key_index;

    for (key_index = 0U; key_index < WORKFLOW_CLEAN_REOPEN_FOREIGN_KEYS;
         ++key_index) {
      char key[128];

      assert_true(snprintf(key, sizeof(key), "foreign-%03lu",
                           (unsigned long)key_index) > 0);
      seed_foreign_workflow_state(client, "workflow-clean-reopen", key,
                                  "completed", &error);
    }
  }
  for (index = 0U; index < WORKFLOW_CLEAN_REOPEN_PENDING_RECORDS; ++index) {
    char key[128];

    assert_true(snprintf(key, sizeof(key),
                         "__lockdc_io/v1/outbox/clean-reopen-%03lu",
                         (unsigned long)index) > 0);
    seed_recovery_outbox(client, "workflow-clean-reopen", key, &error);
  }
  lc_index_flush_req_init(&flush_request);
  memset(&flush_result, 0, sizeof(flush_result));
  flush_request.namespace_name = "workflow-clean-reopen";
  flush_request.mode = "sync";
  assert_int_equal(
      lc_flush_index(client, &flush_request, &flush_result, &error), LC_OK);
  lc_index_flush_res_cleanup(&flush_result);
  lc_client_close(client);
  client = NULL;

  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_query_req_init(&query_request);
  memset(&query_handler, 0, sizeof(query_handler));
  memset(&query_result, 0, sizeof(query_result));
  memset(&query_count, 0, sizeof(query_count));
  query_request.namespace_name = "workflow-clean-reopen";
  query_request.selector_json =
      "{\"in\":{\"field\":\"/dispatch_state\",\"any\":[\"pending\","
      "\"retry_wait\"]}}";
  query_request.engine = "index";
  query_request.refresh = "wait_for";
  query_handler.begin = workflow_query_count_begin;
  query_handler.chunk = workflow_query_count_chunk;
  query_handler.end = workflow_query_count_end;
  assert_int_equal(lc_query_keys(client, &query_request, &query_handler,
                                 &query_count, &query_result, &error),
                   LC_OK);
  assert_int_equal(query_count.count, WORKFLOW_CLEAN_REOPEN_PENDING_RECORDS);
  lc_query_res_cleanup(&query_result);
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "workflow-clean-reopen";
  workflow_config.owner = "workflow-clean-reopen-test";
  workflow_config.notification_capacity = 1U;
  workflow = NULL;
  assert_int_equal(clock_gettime(CLOCK_MONOTONIC, &started), 0);
  assert_int_equal(
      lc_client_new_workflow(client, &workflow_config, &workflow, &error),
      LC_OK);
  job = NULL;
  assert_int_equal(lc_workflow_next(workflow, 5000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_int_equal(clock_gettime(CLOCK_MONOTONIC, &finished), 0);
  assert_true(workflow_elapsed_milliseconds(&started, &finished) < 5000L);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  lc_outbox_job_close(job);
  lc_workflow_close(workflow);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void test_pouch_clean_reopen_reconciles_durable_index(void **state) {
  (void)state;
  test_pouch_reopen_reconciles_durable_index_mode(0);
}

static void test_pouch_shared_reopen_reconciles_durable_index(void **state) {
  (void)state;
  test_pouch_reopen_reconciles_durable_index_mode(1);
}

/* Compaction is allowed to discard historical release records, but never the
 * final lease clear. Reconciliation must be able to claim every pending outbox
 * record immediately after a dense implicit-XA history is snapshotted. */
static void
test_pouch_compacted_reopen_reconciles_released_outbox(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  lc_workflow_config workflow_config;
  lc_client *client;
  lc_client_handle *client_handle;
  lc_workflow *workflow;
  lc_outbox_job *job;
  lc_error error;
  size_t index;
  size_t rewrite;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "compacted-reopen-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint),
                       "pouch://%s?segment_target_bytes=65536", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  workflow = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  for (rewrite = 0U; rewrite < 5U; ++rewrite) {
    for (index = 0U; index < 64U; ++index) {
      char key[128];

      assert_true(snprintf(key, sizeof(key),
                           "__lockdc_io/v1/outbox/compacted-terminal-%03lu",
                           (unsigned long)index) > 0);
      seed_terminal_workflow_outbox(client, "workflow-compacted-reopen", key,
                                    &error);
    }
  }
  for (index = 0U; index < 16U; ++index) {
    char key[128];

    assert_true(snprintf(key, sizeof(key),
                         "__lockdc_io/v1/outbox/compacted-pending-%03lu",
                         (unsigned long)index) > 0);
    seed_recovery_outbox(client, "workflow-compacted-reopen", key, &error);
  }
  client_handle = (lc_client_handle *)client;
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  maintenance_options.namespace_name = "workflow-compacted-reopen";
  maintenance_options.force = 1;
  assert_int_equal(lc_pouch_maintenance_run(client_handle->pouch,
                                            &maintenance_options,
                                            &maintenance_result, &error),
                   LC_OK);
  assert_true(maintenance_result.compacted);
  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  lc_client_close(client);
  client = NULL;

  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "workflow-compacted-reopen";
  workflow_config.owner = "workflow-compacted-reopen-test";
  workflow_config.notification_capacity = 16U;
  assert_int_equal(
      lc_client_new_workflow(client, &workflow_config, &workflow, &error),
      LC_OK);
  for (index = 0U; index < 16U; ++index) {
    job = NULL;
    assert_int_equal(lc_workflow_next(workflow, 5000L, &job, &error), LC_OK);
    assert_non_null(job);
    assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
    lc_outbox_job_close(job);
  }

  lc_workflow_close(workflow);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static long workflow_elapsed_milliseconds(const struct timespec *started,
                                          const struct timespec *finished) {
  long seconds = (long)(finished->tv_sec - started->tv_sec);
  long nanoseconds = (long)(finished->tv_nsec - started->tv_nsec);

  return seconds * 1000L + nanoseconds / 1000000L;
}

static void test_pouch_reconciliation_pages_large_outbox(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_client *client;
  lc_workflow *workflow;
  lc_outbox_job *job;
  lc_error error;
  struct timespec started;
  struct timespec finished;
  size_t index;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "reconcile-large-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  for (index = 0U; index < WORKFLOW_RECONCILIATION_RECORDS; ++index) {
    char key[128];

    assert_true(snprintf(key, sizeof(key), "__lockdc_io/v1/outbox/load-%03lu",
                         (unsigned long)index) > 0);
    seed_recovery_outbox(client, "workflow-reconcile-large", key, &error);
  }
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "workflow-reconcile-large";
  workflow_config.owner = "workflow-reconcile-large-test";
  workflow_config.notification_capacity = 16U;
  workflow = NULL;
  assert_int_equal(clock_gettime(CLOCK_MONOTONIC, &started), 0);
  assert_int_equal(
      lc_client_new_workflow(client, &workflow_config, &workflow, &error),
      LC_OK);
  for (index = 0U; index < WORKFLOW_RECONCILIATION_RECORDS; ++index) {
    job = NULL;
    assert_int_equal(lc_workflow_next(workflow, 30000L, &job, &error), LC_OK);
    assert_non_null(job);
    assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
    lc_outbox_job_close(job);
  }
  assert_int_equal(clock_gettime(CLOCK_MONOTONIC, &finished), 0);
  assert_true(workflow_elapsed_milliseconds(&started, &finished) < 30000L);
  fprintf(stderr, "workflow reconciliation: %lu records in %ld ms\n",
          (unsigned long)WORKFLOW_RECONCILIATION_RECORDS,
          workflow_elapsed_milliseconds(&started, &finished));
  lc_workflow_close(workflow);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void
test_pouch_reconciliation_preserves_allocator_domains(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  workflow_tracking_allocator allocator;
  lc_client *client;
  lc_workflow *workflow;
  lc_outbox_job *job;
  lc_error error;
  size_t index;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "reconcile-cursor-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  for (index = 0U; index < 3U; ++index) {
    char key[128];

    assert_true(snprintf(key, sizeof(key), "__lockdc_io/v1/outbox/cursor-%03lu",
                         (unsigned long)index) > 0);
    seed_recovery_outbox(client, "workflow-reconcile-cursor", key, &error);
  }
  lc_client_close(client);
  workflow_tracking_allocator_init(&allocator);
  client_config.allocator.malloc_fn = workflow_tracking_malloc;
  client_config.allocator.realloc_fn = workflow_tracking_realloc;
  client_config.allocator.free_fn = workflow_tracking_free;
  client_config.allocator.context = &allocator;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "workflow-reconcile-cursor";
  workflow_config.owner = "workflow-reconcile-cursor-test";
  workflow_config.notification_capacity = 1U;
  workflow = NULL;
  assert_int_equal(
      lc_client_new_workflow(client, &workflow_config, &workflow, &error),
      LC_OK);
  for (index = 0U; index < 3U; ++index) {
    job = NULL;
    assert_int_equal(lc_workflow_next(workflow, 30000L, &job, &error), LC_OK);
    assert_non_null(job);
    assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
    lc_outbox_job_close(job);
  }
  lc_workflow_close(workflow);
  lc_client_close(client);
  lc_error_cleanup(&error);
  workflow_tracking_allocator_destroy(&allocator);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void test_pouch_recovery_prefetch_is_bounded(void **state) {
  char root[256];
  char template_path[256];
  char endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_client *client;
  lc_workflow *workflow;
  lc_error error;
  struct timespec deadline;
  struct timespec now;
  size_t index;
  int found_unclaimed;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "prefetch-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  for (index = 0U; index < WORKFLOW_PREFETCH_RECORDS; ++index) {
    char key[128];

    assert_true(snprintf(key, sizeof(key),
                         "__lockdc_io/v1/outbox/prefetch-%03lu",
                         (unsigned long)index) > 0);
    seed_recovery_outbox(client, "workflow-prefetch", key, &error);
  }
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "workflow-prefetch";
  workflow_config.owner = "workflow-prefetch-test";
  workflow_config.notification_capacity = 2U;
  workflow = NULL;
  assert_int_equal(
      lc_client_new_workflow(client, &workflow_config, &workflow, &error),
      LC_OK);
  assert_int_equal(clock_gettime(CLOCK_MONOTONIC, &deadline), 0);
  deadline.tv_sec += 3L;
  found_unclaimed = 0;
  do {
    size_t locked = 0U;
    size_t available = 0U;

    for (index = 0U; index < WORKFLOW_PREFETCH_RECORDS; ++index) {
      char key[128];
      lc_acquire_req acquire;
      lc_lease *lease;
      int rc;

      assert_true(snprintf(key, sizeof(key),
                           "__lockdc_io/v1/outbox/prefetch-%03lu",
                           (unsigned long)index) > 0);
      lc_acquire_req_init(&acquire);
      acquire.namespace_name = "workflow-prefetch";
      acquire.key = key;
      acquire.owner = "workflow-prefetch-probe";
      acquire.ttl_seconds = 30L;
      lease = NULL;
      rc = lc_acquire(client, &acquire, &lease, &error);
      if (rc == LC_OK) {
        ++available;
        assert_int_equal(lc_lease_release(lease, NULL, &error), LC_OK);
      } else {
        ++locked;
        lc_error_cleanup(&error);
        lc_error_init(&error);
      }
    }
    if (locked >= 2U && available > 0U)
      found_unclaimed = 1;
    if (!found_unclaimed) {
      struct timespec delay;
      delay.tv_sec = 0;
      delay.tv_nsec = 10000000L;
      (void)nanosleep(&delay, NULL);
      assert_int_equal(clock_gettime(CLOCK_MONOTONIC, &now), 0);
    }
  } while (!found_unclaimed &&
           (now.tv_sec < deadline.tv_sec ||
            (now.tv_sec == deadline.tv_sec && now.tv_nsec < deadline.tv_nsec)));
  assert_true(found_unclaimed);
  lc_workflow_close(workflow);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void test_pouch_shared_process_dispatches_once(void **state) {
  char root[256], template_path[256], endpoint[320], start;
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_client *client;
  lc_workflow *workflow;
  lc_outbox_job *job;
  workflow_process_result child_result;
  lc_error error;
  int start_pipe[2], result_pipe[2], status, parent_got_job;
  pid_t child;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "shared-process-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint),
                       "pouch://%s?single_writer=false", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  seed_recovery_outbox(client, "workflow-shared-process",
                       "__lockdc_io/v1/outbox/shared-process", &error);
  lc_client_close(client);
  assert_int_equal(pipe(start_pipe), 0);
  assert_int_equal(pipe(result_pipe), 0);
  child = fork();
  assert_true(child >= 0);
  if (child == 0) {
    workflow_process_result result;
    (void)close(start_pipe[1]);
    (void)close(result_pipe[0]);
    result = workflow_shared_process_claim(root, "workflow-shared-process",
                                           start_pipe[0]);
    (void)write(result_pipe[1], &result, sizeof(result));
    (void)close(result_pipe[1]);
    _exit(result.rc == LC_OK ? 0 : 1);
  }
  (void)close(start_pipe[0]);
  (void)close(result_pipe[1]);
  client = NULL;
  workflow = NULL;
  job = NULL;
  parent_got_job = 0;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "workflow-shared-process";
  workflow_config.owner = "workflow-shared-parent";
  workflow_config.recovery_interval_seconds = 1L;
  assert_int_equal(
      lc_client_new_workflow(client, &workflow_config, &workflow, &error),
      LC_OK);
  start = 's';
  assert_int_equal(write(start_pipe[1], &start, 1U), 1);
  (void)close(start_pipe[1]);
  assert_int_equal(lc_workflow_next(workflow, 5000L, &job, &error), LC_OK);
  if (job != NULL) {
    parent_got_job = 1;
    assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
    lc_outbox_job_close(job);
  }
  assert_int_equal(read(result_pipe[0], &child_result, sizeof(child_result)),
                   (ssize_t)sizeof(child_result));
  (void)close(result_pipe[0]);
  assert_int_equal(waitpid(child, &status, 0), child);
  assert_true(WIFEXITED(status));
  assert_int_equal(WEXITSTATUS(status), 0);
  assert_int_equal(child_result.rc, LC_OK);
  assert_true((parent_got_job != 0) != (child_result.got_job != 0));
  lc_workflow_close(workflow);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void
test_pouch_shared_process_reconciles_each_outbox_once(void **state) {
  char root[256], template_path[256], endpoint[384];
  const char *endpoints[1];
  lc_client_config client_config;
  workflow_process_result results[2];
  lc_client *client;
  lc_error error;
  int start_pipes[2][2];
  int result_pipes[2][2];
  int status;
  pid_t children[2];
  size_t index;
  unsigned long delivered;

  (void)state;
  memset(start_pipes, -1, sizeof(start_pipes));
  memset(result_pipes, -1, sizeof(result_pipes));
  memset(children, 0, sizeof(children));
  memset(results, 0, sizeof(results));
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "shared-reconcile-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(
      snprintf(endpoint, sizeof(endpoint),
               "pouch://%s?single_writer=false&segment_target_bytes=65536",
               root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client_config.default_namespace = "workflow-shared-reconcile";
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  for (index = 0U; index < WORKFLOW_SHARED_PROCESS_RECORDS; ++index) {
    char key[160];

    assert_true(snprintf(key, sizeof(key),
                         "__lockdc_io/v1/outbox/shared-reconcile-%03lu",
                         (unsigned long)index) > 0);
    seed_recovery_outbox(client, client_config.default_namespace, key, &error);
  }
  lc_client_close(client);
  client = NULL;

  for (index = 0U; index < 2U; ++index) {
    char owner[64];

    assert_int_equal(pipe(start_pipes[index]), 0);
    assert_int_equal(pipe(result_pipes[index]), 0);
    children[index] = fork();
    assert_true(children[index] >= 0);
    if (children[index] == 0) {
      workflow_process_result child_result;

      (void)close(start_pipes[index][1]);
      (void)close(result_pipes[index][0]);
      assert_true(snprintf(owner, sizeof(owner), "workflow-shared-drain-%lu",
                           (unsigned long)index) > 0);
      child_result = workflow_shared_process_drain(
          root, client_config.default_namespace, owner, start_pipes[index][0]);
      (void)write(result_pipes[index][1], &child_result, sizeof(child_result));
      (void)close(result_pipes[index][1]);
      _exit(child_result.rc == LC_OK ? 0 : 1);
    }
    (void)close(start_pipes[index][0]);
    start_pipes[index][0] = -1;
    (void)close(result_pipes[index][1]);
    result_pipes[index][1] = -1;
  }
  for (index = 0U; index < 2U; ++index) {
    char start;

    start = 's';
    assert_int_equal(write(start_pipes[index][1], &start, 1U), 1);
    (void)close(start_pipes[index][1]);
    start_pipes[index][1] = -1;
  }
  delivered = 0UL;
  for (index = 0U; index < 2U; ++index) {
    assert_int_equal(
        read(result_pipes[index][0], &results[index], sizeof(results[index])),
        (ssize_t)sizeof(results[index]));
    (void)close(result_pipes[index][0]);
    result_pipes[index][0] = -1;
    assert_int_equal(waitpid(children[index], &status, 0), children[index]);
    assert_true(WIFEXITED(status));
    if (results[index].rc != LC_OK) {
      (void)fprintf(stderr,
                    "shared workflow drain %lu failed: rc=%d message=%s "
                    "detail=%s\n",
                    (unsigned long)index, results[index].rc,
                    results[index].error_message, results[index].error_detail);
    }
    assert_int_equal(results[index].rc, LC_OK);
    assert_int_equal(WEXITSTATUS(status), 0);
    delivered += results[index].delivered;
  }
  assert_int_equal(delivered, WORKFLOW_SHARED_PROCESS_RECORDS);

  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  for (index = 0U; index < WORKFLOW_SHARED_PROCESS_RECORDS; ++index) {
    char key[160];

    assert_true(snprintf(key, sizeof(key),
                         "__lockdc_io/v1/outbox/shared-reconcile-%03lu",
                         (unsigned long)index) > 0);
    workflow_assert_outbox_completed(client, key, &error);
  }
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void test_pouch_expired_claim_rejects_stale_terminal(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config config;
  lc_client *client;
  lc_workflow *first, *second;
  lc_outbox_job *stale, *replacement;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "stale-terminal-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  seed_recovery_outbox(client, "workflow-stale-terminal",
                       "__lockdc_io/v1/outbox/stale-terminal", &error);
  lc_workflow_config_init(&config);
  config.namespace_name = "workflow-stale-terminal";
  config.owner = "workflow-stale-first";
  config.claim_ttl_seconds = workflow_claim_ttl_seconds();
  first = NULL;
  assert_int_equal(lc_client_new_workflow(client, &config, &first, &error),
                   LC_OK);
  stale = NULL;
  assert_int_equal(
      lc_workflow_next(first, workflow_claim_next_timeout_ms(), &stale, &error),
      LC_OK);
  assert_non_null(stale);
  lc_workflow_close(first);
  sleep(workflow_claim_expiry_wait_seconds());
  config.owner = "workflow-stale-second";
  second = NULL;
  assert_int_equal(lc_client_new_workflow(client, &config, &second, &error),
                   LC_OK);
  replacement = NULL;
  assert_int_equal(lc_workflow_next(second, workflow_claim_next_timeout_ms(),
                                    &replacement, &error),
                   LC_OK);
  assert_non_null(replacement);
  assert_string_equal(replacement->effect_key, stale->effect_key);
  assert_true(lc_outbox_job_complete(stale, NULL, &error) != LC_OK);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(lc_outbox_job_complete(replacement, NULL, &error), LC_OK);
  lc_outbox_job_close(replacement);
  lc_outbox_job_close(stale);
  lc_workflow_close(second);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void
test_pouch_expired_claim_recovers_and_preserves_attempt_budget(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config config;
  lc_client *client;
  lc_workflow *workflow;
  lc_outbox_job *first, *second, *unexpected;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_sink *sink;
  lc_get_res get_result;
  const void *bytes;
  size_t length;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "expired-budget-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  seed_recovery_outbox(client, "workflow-expired-budget",
                       "__lockdc_io/v1/outbox/expired-budget", &error);
  lc_workflow_config_init(&config);
  config.namespace_name = "workflow-expired-budget";
  config.owner = "workflow-expired-budget";
  config.claim_ttl_seconds = workflow_claim_ttl_seconds();
  config.max_attempts = 2;
  workflow = NULL;
  assert_int_equal(lc_client_new_workflow(client, &config, &workflow, &error),
                   LC_OK);

  first = NULL;
  assert_int_equal(lc_workflow_next(workflow, workflow_claim_next_timeout_ms(),
                                    &first, &error),
                   LC_OK);
  assert_non_null(first);
  assert_int_equal(first->attempt, 1);
  /* Closing an unfinished job must wake the local dispatcher at lease expiry;
   * it is not dependent on a restart or an opt-in recovery poll. */
  lc_outbox_job_close(first);

  second = NULL;
  assert_int_equal(lc_workflow_next(workflow, workflow_claim_next_timeout_ms(),
                                    &second, &error),
                   LC_OK);
  assert_non_null(second);
  assert_int_equal(second->attempt, 2);
  lc_outbox_job_close(second);

  /* A second abandoned claim consumes the final durable attempt. Expiry must
   * dead-letter it rather than reset the counter and hand out attempt three. */
  unexpected = (lc_outbox_job *)1;
  assert_int_equal(lc_workflow_next(workflow, workflow_claim_next_timeout_ms(),
                                    &unexpected, &error),
                   LC_OK);
  assert_null(unexpected);

  lc_acquire_req_init(&acquire);
  acquire.namespace_name = config.namespace_name;
  acquire.key = "__lockdc_io/v1/outbox/expired-budget";
  acquire.owner = "workflow-expired-budget-inspect";
  acquire.ttl_seconds = 30L;
  lease = NULL;
  assert_int_equal(lc_acquire(client, &acquire, &lease, &error), LC_OK);
  sink = NULL;
  memset(&get_result, 0, sizeof(get_result));
  assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
  assert_int_equal(lc_lease_get(lease, sink, NULL, &get_result, &error), LC_OK);
  bytes = NULL;
  length = 0U;
  assert_int_equal(lc_sink_memory_bytes(sink, &bytes, &length, &error), LC_OK);
  assert_true(workflow_bytes_contains(bytes, length,
                                      "\"dispatch_state\":\"dead_letter\""));
  assert_true(workflow_bytes_contains(bytes, length, "\"attempt_count\":2"));
  lc_get_res_cleanup(&get_result);
  lc_sink_close(sink);
  assert_int_equal(lc_lease_release(lease, NULL, &error), LC_OK);
  lc_workflow_close(workflow);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void
test_pouch_workflow_validates_durable_input_contracts(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  char *oversized_diagnostic;
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_outbox_entry entry;
  lc_inbox_message inbox;
  lc_inbox_accept_result inbox_result;
  lc_outbox_retry retry;
  lc_outbox_receipt receipt;
  lc_client *client;
  lc_workflow *workflow;
  lc_workflow_transaction *transaction;
  lc_outbox_job *job;
  lc_source *payload;
  lc_source *second_payload;
  lc_error error;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "input-contracts-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  workflow = NULL;
  transaction = NULL;
  job = NULL;
  payload = NULL;
  second_payload = NULL;
  oversized_diagnostic = NULL;
  lc_outbox_receipt_init(&receipt);
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "workflow-input-contracts";
  workflow_config.owner = "workflow-input-contracts-test";
  assert_int_equal(
      lc_client_new_workflow(client, &workflow_config, &workflow, &error),
      LC_OK);
  lc_outbox_entry_init(&entry);
  entry.operation_id = "input-contract-operation";
  entry.effect_id = "input-contract-effect";
  entry.effect_key = "input-contract-effect-key";
  entry.kind = "test";
  entry.destination = "input-contract://destination";
  entry.content_type = "text/plain";
  assert_int_equal(lc_source_from_memory("payload", 7U, &payload, &error),
                   LC_OK);

  entry.headers_json = "{";
  assert_int_equal(lc_workflow_append_outbox(workflow, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_ERR_INVALID);
  assert_null(transaction);
  assert_null(receipt.outbox_key);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  entry.headers_json = "[]";
  assert_int_equal(lc_workflow_append_outbox(workflow, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_ERR_INVALID);
  assert_null(transaction);
  assert_null(receipt.outbox_key);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  entry.headers_json = "{\"x-request-id\":\"contract\"}";
  assert_int_equal(lc_workflow_append_outbox(workflow, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_OK);
  assert_non_null(transaction);
  assert_int_equal(lc_workflow_transaction_commit(transaction, &error), LC_OK);
  lc_workflow_transaction_close(transaction);
  transaction = NULL;

  /* Input validation precedes duplicate lookup: invalid metadata must not be
   * silently accepted just because a valid entry with the same identity exists.
   */
  entry.headers_json = "{";
  assert_int_equal(lc_workflow_append_outbox(workflow, &entry, payload,
                                             &transaction, &receipt, &error),
                   LC_ERR_INVALID);
  assert_null(transaction);
  assert_null(receipt.outbox_key);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  entry.headers_json = "{\"x-request-id\":\"contract\"}";

  assert_int_equal(lc_workflow_next(workflow, 3000L, &job, &error), LC_OK);
  assert_non_null(job);

  oversized_diagnostic = (char *)malloc(LC_WORKFLOW_MAX_DIAGNOSTIC_BYTES + 2U);
  assert_non_null(oversized_diagnostic);
  memset(oversized_diagnostic, 'x', LC_WORKFLOW_MAX_DIAGNOSTIC_BYTES + 1U);
  oversized_diagnostic[LC_WORKFLOW_MAX_DIAGNOSTIC_BYTES + 1U] = '\0';
  lc_outbox_retry_init(&retry);
  retry.diagnostic = oversized_diagnostic;
  assert_int_equal(lc_outbox_job_retry(job, &retry, &error), LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(lc_outbox_job_dead_letter(job, oversized_diagnostic, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  lc_outbox_job_close(job);
  job = NULL;
  free(oversized_diagnostic);
  oversized_diagnostic = NULL;

  /* A rejected append cannot roll back already-enrolled work. */
  entry.effect_id = "input-contract-preserve";
  entry.effect_key = "input-contract-preserve-key";
  entry.headers_json = "{\"x-request-id\":\"preserve\"}";
  assert_int_equal(
      lc_source_from_memory("preserve", 8U, &second_payload, &error), LC_OK);
  assert_int_equal(lc_workflow_append_outbox(workflow, &entry, second_payload,
                                             &transaction, &receipt, &error),
                   LC_OK);
  assert_non_null(transaction);
  entry.effect_id = "input-contract-rejected";
  entry.effect_key = "input-contract-rejected-key";
  entry.headers_json = "{";
  assert_int_equal(lc_workflow_transaction_append_outbox(
                       transaction, &entry, second_payload, &receipt, &error),
                   LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(lc_workflow_transaction_commit(transaction, &error), LC_OK);
  lc_workflow_transaction_close(transaction);
  transaction = NULL;
  assert_int_equal(lc_workflow_next(workflow, 3000L, &job, &error), LC_OK);
  assert_non_null(job);
  assert_int_equal(lc_outbox_job_complete(job, NULL, &error), LC_OK);
  lc_outbox_job_close(job);
  job = NULL;
  lc_source_close(second_payload);
  second_payload = NULL;

  lc_inbox_message_init(&inbox);
  inbox.consumer_id = "a\nb";
  inbox.source_kind = "c";
  inbox.source_id = "d";
  inbox.message_id = "e";
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_workflow_accept_inbox(workflow, &inbox, &transaction,
                                            &inbox_result, &error),
                   LC_OK);
  assert_true(inbox_result.accepted);
  assert_int_equal(lc_workflow_transaction_commit(transaction, &error), LC_OK);
  lc_workflow_transaction_close(transaction);
  transaction = NULL;

  inbox.consumer_id = "a";
  inbox.source_kind = "b";
  inbox.source_id = "c\nd";
  inbox.message_id = "e";
  memset(&inbox_result, 0, sizeof(inbox_result));
  assert_int_equal(lc_workflow_accept_inbox(workflow, &inbox, &transaction,
                                            &inbox_result, &error),
                   LC_OK);
  assert_true(inbox_result.accepted);
  assert_false(inbox_result.duplicate);
  assert_int_equal(lc_workflow_transaction_commit(transaction, &error), LC_OK);
  lc_workflow_transaction_close(transaction);
  transaction = NULL;

  lc_outbox_receipt_cleanup(&receipt);
  lc_source_close(payload);
  lc_workflow_close(workflow);
  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

static void
test_pouch_workflow_close_serializes_ready_job_detach(void **state) {
  char root[256], template_path[256], endpoint[320];
  const char *endpoints[1];
  lc_client_config client_config;
  lc_workflow_config workflow_config;
  lc_workflow_stats stats;
  workflow_shutdown_race race;
  lc_client *client;
  lc_workflow *workflow;
  lc_error error;
  pthread_t close_thread, next_thread;
  size_t attempt;
  int close_finished_before_next_detach;

  (void)state;
  assert_true(snprintf(template_path, sizeof(template_path),
                       WORKFLOW_TMP_PREFIX "shutdown-race-XXXXXX") > 0);
  assert_true(lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                                  WORKFLOW_TMP_PREFIX));
  assert_true(snprintf(endpoint, sizeof(endpoint), "pouch://%s", root) > 0);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&client_config);
  client_config.endpoints = endpoints;
  client_config.endpoint_count = 1U;
  client = NULL;
  workflow = NULL;
  assert_int_equal(lc_client_open(&client_config, &client, &error), LC_OK);
  seed_recovery_outbox(client, "workflow-shutdown-race",
                       "__lockdc_io/v1/outbox/shutdown-race", &error);
  lc_workflow_config_init(&workflow_config);
  workflow_config.namespace_name = "workflow-shutdown-race";
  workflow_config.owner = "workflow-shutdown-race-test";
  assert_int_equal(
      lc_client_new_workflow(client, &workflow_config, &workflow, &error),
      LC_OK);
  memset(&stats, 0, sizeof(stats));
  for (attempt = 0U; attempt < 500U; ++attempt) {
    assert_int_equal(lc_workflow_get_stats(workflow, &stats, &error), LC_OK);
    if (stats.ready_jobs == 1U)
      break;
    lc_workflow_stats_cleanup(&stats);
    memset(&stats, 0, sizeof(stats));
    {
      struct timespec delay;
      delay.tv_sec = 0;
      delay.tv_nsec = 10000000L;
      (void)nanosleep(&delay, NULL);
    }
  }
  assert_int_equal(stats.ready_jobs, 1U);
  lc_workflow_stats_cleanup(&stats);

  memset(&race, 0, sizeof(race));
  race.workflow = workflow;
  assert_int_equal(pthread_mutex_init(&race.mutex, NULL), 0);
  assert_int_equal(pthread_cond_init(&race.condition, NULL), 0);
  workflow_reset_allocation_failures();
  lc_workflow_test_after_close_requested_hook =
      workflow_shutdown_race_after_close_requested;
  lc_workflow_test_after_close_requested_context = &race;
  lc_workflow_test_before_ready_job_detach_hook =
      workflow_shutdown_race_before_ready_detach;
  lc_workflow_test_before_ready_job_detach_context = &race;
  lc_workflow_test_before_ready_job_teardown_hook =
      workflow_shutdown_race_before_teardown;
  lc_workflow_test_before_ready_job_teardown_context = &race;
  assert_int_equal(pthread_create(&close_thread, NULL,
                                  workflow_shutdown_race_close_thread, &race),
                   0);
  assert_true(workflow_shutdown_race_wait(&race, &race.close_requested));
  assert_int_equal(pthread_create(&next_thread, NULL,
                                  workflow_shutdown_race_next_thread, &race),
                   0);
  assert_true(workflow_shutdown_race_wait(&race, &race.ready_detach_entered));

  assert_int_equal(pthread_mutex_lock(&race.mutex), 0);
  race.allow_close = 1;
  assert_int_equal(pthread_cond_broadcast(&race.condition), 0);
  assert_int_equal(pthread_mutex_unlock(&race.mutex), 0);
  assert_true(workflow_shutdown_race_wait(&race, &race.teardown_entered));
  assert_int_equal(pthread_mutex_lock(&race.mutex), 0);
  race.allow_teardown = 1;
  assert_int_equal(pthread_cond_broadcast(&race.condition), 0);
  assert_int_equal(pthread_mutex_unlock(&race.mutex), 0);
  {
    struct timespec delay;
    delay.tv_sec = 0;
    delay.tv_nsec = 50000000L;
    (void)nanosleep(&delay, NULL);
  }
  assert_int_equal(pthread_mutex_lock(&race.mutex), 0);
  close_finished_before_next_detach = race.close_finished;
  assert_int_equal(pthread_mutex_unlock(&race.mutex), 0);

  assert_int_equal(pthread_mutex_lock(&race.mutex), 0);
  race.allow_ready_detach = 1;
  assert_int_equal(pthread_cond_broadcast(&race.condition), 0);
  assert_int_equal(pthread_mutex_unlock(&race.mutex), 0);
  assert_true(workflow_shutdown_race_wait(&race, &race.next_finished));
  assert_int_equal(pthread_join(next_thread, NULL), 0);
  assert_int_equal(race.next_rc, LC_OK);
  assert_non_null(race.job);
  lc_outbox_job_close(race.job);
  assert_int_equal(pthread_join(close_thread, NULL), 0);
  assert_false(close_finished_before_next_detach);
  workflow_reset_allocation_failures();
  pthread_cond_destroy(&race.condition);
  pthread_mutex_destroy(&race.mutex);

  lc_client_close(client);
  lc_error_cleanup(&error);
  lc_test_tmp_cleanup_path(root, WORKFLOW_TMP_PREFIX);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(
          test_pouch_outbox_duplicate_rejects_immutable_envelope_conflicts),
      cmocka_unit_test(test_pouch_outbox_transaction_and_duplicate),
      cmocka_unit_test(test_pouch_participant_cleanup_after_transaction_close),
      cmocka_unit_test(
          test_pouch_participant_allocation_failure_rolls_back_enrollment),
      cmocka_unit_test(
          test_pouch_command_receipt_allocation_failure_rolls_back_enrollment),
      cmocka_unit_test(
          test_pouch_outbox_allocation_failures_roll_back_enrollment),
      cmocka_unit_test(
          test_pouch_notification_allocation_failure_reconciles_committed_outbox),
      cmocka_unit_test(
          test_pouch_retry_notification_allocation_failure_recovers_at_deadline),
      cmocka_unit_test(test_pouch_transient_claim_failure_is_rescheduled),
      cmocka_unit_test(
          test_pouch_claim_recovery_allocation_failure_recovers_at_expiry),
      cmocka_unit_test(
          test_pouch_command_receipt_commits_with_outbox_and_result),
      cmocka_unit_test(test_pouch_shared_command_resume_is_durable),
      cmocka_unit_test(test_pouch_multikey_terminal_failure_publishes_nothing),
      cmocka_unit_test(test_pouch_reconciliation_retains_overflow_request),
      cmocka_unit_test(test_pouch_dead_letter_operations),
      cmocka_unit_test(test_pouch_startup_recovery_claims_seeded_outbox),
      cmocka_unit_test(test_pouch_clean_reopen_reconciles_durable_index),
      cmocka_unit_test(test_pouch_shared_reopen_reconciles_durable_index),
      cmocka_unit_test(test_pouch_compacted_reopen_reconciles_released_outbox),
      cmocka_unit_test(test_pouch_reconciliation_pages_large_outbox),
      cmocka_unit_test(test_pouch_reconciliation_preserves_allocator_domains),
      cmocka_unit_test(test_pouch_recovery_prefetch_is_bounded),
      cmocka_unit_test(test_pouch_shared_process_dispatches_once),
      cmocka_unit_test(test_pouch_shared_process_reconciles_each_outbox_once),
      cmocka_unit_test(test_pouch_expired_claim_rejects_stale_terminal),
      cmocka_unit_test(
          test_pouch_expired_claim_recovers_and_preserves_attempt_budget),
      cmocka_unit_test(test_pouch_workflow_validates_durable_input_contracts),
      cmocka_unit_test(test_pouch_workflow_close_serializes_ready_job_detach),
  };
  return cmocka_run_group_tests(tests, NULL, NULL);
}

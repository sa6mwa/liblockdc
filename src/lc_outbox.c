#include "lc_api_internal.h"
#include "lc_pouch_internal.h"

#include <openssl/evp.h>
#include <openssl/rand.h>

#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>

typedef struct lc_outbox_handle lc_outbox_handle;
typedef struct lc_outbox_transaction_handle lc_outbox_transaction_handle;
typedef struct lc_outbox_participant_handle lc_outbox_participant_handle;
typedef struct lc_outbox_job_handle lc_outbox_job_handle;

typedef struct lc_outbox_delayed_notification {
  char *key;
  lc_unix_seconds eligible_at_unix;
  /* A claim expiry needs the claimed-record recovery pass. A retry wake only
   * needs the ordinary pending/retry passes. Keep that distinction when a
   * bounded local wake is displaced into durable recovery state. */
  int claim_recovery;
} lc_outbox_delayed_notification;

static void lc_outbox_release(lc_outbox_handle *outbox);
static void lc_outbox_retain(lc_outbox_handle *outbox);
static int lc_outbox_notify(lc_outbox_handle *outbox, const char *key,
                            int replace_delayed);
static int lc_outbox_get_or_start_dispatcher_method(lc_outbox *self,
                                                    lc_outbox_dispatcher **out,
                                                    lc_error *error);
static void lc_outbox_dispatcher_close_method(lc_outbox_dispatcher *self);
static int lc_outbox_dispatcher_stop_method(lc_outbox_dispatcher *self,
                                            long deadline_ms, lc_error *error);
static void
lc_outbox_dispatcher_retain(lc_outbox_dispatcher_handle *dispatcher);
static void
lc_outbox_dispatcher_retain_locked(lc_outbox_dispatcher_handle *dispatcher);
static void lc_outbox_rollback_lease(lc_lease *lease);
static void lc_outbox_transaction_abort_enrolled_lease(
    lc_outbox_transaction_handle *transaction, lc_lease *lease);

#ifdef LOCKDC_TEST_BUILD
lc_outbox_test_after_reconcile_query_hook_fn
    lc_outbox_test_after_reconcile_query_hook = NULL;
void *lc_outbox_test_after_reconcile_query_context = NULL;
lc_outbox_test_after_reconcile_query_hook_fn
    lc_outbox_test_after_recovery_overflow_hook = NULL;
void *lc_outbox_test_after_recovery_overflow_context = NULL;
lc_outbox_test_after_reconcile_query_hook_fn
    lc_outbox_test_before_recovery_query_hook = NULL;
void *lc_outbox_test_before_recovery_query_context = NULL;
lc_outbox_test_after_reconcile_query_hook_fn
    lc_outbox_test_after_recovery_capacity_pause_hook = NULL;
void *lc_outbox_test_after_recovery_capacity_pause_context = NULL;
lc_outbox_test_dead_letter_replay_client_hook_fn
    lc_outbox_test_dead_letter_replay_client_hook = NULL;
void *lc_outbox_test_dead_letter_replay_client_context = NULL;
lc_outbox_test_hook_fn lc_outbox_test_after_close_requested_hook = NULL;
void *lc_outbox_test_after_close_requested_context = NULL;
lc_outbox_test_hook_fn lc_outbox_test_before_ready_job_detach_hook = NULL;
void *lc_outbox_test_before_ready_job_detach_context = NULL;
lc_outbox_test_hook_fn lc_outbox_test_before_ready_job_teardown_hook = NULL;
void *lc_outbox_test_before_ready_job_teardown_context = NULL;
lc_outbox_test_hook_fn lc_outbox_test_before_dispatcher_wait_hook = NULL;
void *lc_outbox_test_before_dispatcher_wait_context = NULL;
lc_outbox_test_hook_fn lc_outbox_test_before_next_wait_hook = NULL;
void *lc_outbox_test_before_next_wait_context = NULL;
lc_outbox_test_hook_fn lc_outbox_test_before_next_release_hook = NULL;
void *lc_outbox_test_before_next_release_context = NULL;
lc_outbox_test_hook_fn lc_outbox_test_after_dispatcher_core_retain_hook = NULL;
void *lc_outbox_test_after_dispatcher_core_retain_context = NULL;
lc_outbox_test_hook_fn lc_outbox_test_before_dead_letter_export_open_hook =
    NULL;
void *lc_outbox_test_before_dead_letter_export_open_context = NULL;
lc_outbox_test_failure_hook_fn lc_outbox_test_before_ledger_append_hook = NULL;
void *lc_outbox_test_before_ledger_append_context = NULL;
lc_outbox_test_failure_hook_fn
    lc_outbox_test_before_participant_allocation_hook = NULL;
void *lc_outbox_test_before_participant_allocation_context = NULL;
lc_outbox_test_failure_hook_fn lc_outbox_test_before_command_receipt_copy_hook =
    NULL;
void *lc_outbox_test_before_command_receipt_copy_context = NULL;
lc_outbox_test_failure_hook_fn lc_outbox_test_after_command_terminal_load_hook =
    NULL;
void *lc_outbox_test_after_command_terminal_load_context = NULL;
lc_outbox_test_failure_hook_fn lc_outbox_test_before_outbox_receipt_copy_hook =
    NULL;
void *lc_outbox_test_before_outbox_receipt_copy_context = NULL;
lc_outbox_test_failure_hook_fn
    lc_outbox_test_before_transaction_terminal_release_hook = NULL;
void *lc_outbox_test_before_transaction_terminal_release_context = NULL;
lc_outbox_test_failure_hook_fn
    lc_outbox_test_after_transaction_terminal_decision_hook = NULL;
void *lc_outbox_test_after_transaction_terminal_decision_context = NULL;
lc_outbox_test_failure_hook_fn lc_outbox_test_before_notification_copy_hook =
    NULL;
void *lc_outbox_test_before_notification_copy_context = NULL;
lc_outbox_test_failure_hook_fn lc_outbox_test_before_claim_outbox_hook = NULL;
void *lc_outbox_test_before_claim_outbox_context = NULL;
lc_outbox_test_failure_hook_fn
    lc_outbox_test_before_outbox_handoff_reacquire_hook = NULL;
void *lc_outbox_test_before_outbox_handoff_reacquire_context = NULL;
lc_outbox_test_failure_hook_fn
    lc_outbox_test_before_periodic_recovery_schedule_hook = NULL;
void *lc_outbox_test_before_periodic_recovery_schedule_context = NULL;
lc_outbox_test_failure_hook_fn
    lc_outbox_test_before_dead_letter_claim_cleanup_hook = NULL;
void *lc_outbox_test_before_dead_letter_claim_cleanup_context = NULL;
lc_outbox_test_failure_hook_fn
    lc_outbox_test_before_outbox_renew_keepalive_hook = NULL;
void *lc_outbox_test_before_outbox_renew_keepalive_context = NULL;
lc_outbox_test_failure_hook_fn
    lc_outbox_test_before_outbox_renew_deadline_publish_hook = NULL;
void *lc_outbox_test_before_outbox_renew_deadline_publish_context = NULL;
#endif

typedef struct lc_outbox_record {
  char *record_type;
  char *operation_id;
  char *effect_id;
  char *effect_key;
  char *payload_digest;
  char *message_id;
  char *command_id;
  char *causation_id;
  char *kind;
  char *schema_version;
  char *destination;
  char *content_type;
  char *headers_json;
  char *trace_context;
  char *dispatch_state;
  lonejson_int64 attempt_count;
  lonejson_int64 claim_expires_at_unix;
  lonejson_int64 not_before_unix;
  lonejson_int64 replay_count;
  lonejson_int64 dead_lettered_at_unix;
  lonejson_int64 replayed_at_unix;
  char *delivery_reference;
  char *response_digest;
  lonejson_int64 completed_at_unix;
  char *last_error;
  char *prior_dead_letter_error;
} lc_outbox_record;

/* A claim lease cannot publish an ordinary state update without taking its
 * implicit-XA terminal decision. Renewals therefore publish their current
 * expiry through this independently committed outbox record. The attempt
 * number binds a hint to one durable claim, so stale hints are ignored. */
typedef struct lc_outbox_claim_deadline_record {
  char *record_type;
  lonejson_int64 attempt_count;
  lonejson_int64 replay_count;
  lonejson_int64 claim_expires_at_unix;
} lc_outbox_claim_deadline_record;

typedef struct lc_outbox_inbox_record {
  char *record_type;
  char *consumer_id;
  char *source_kind;
  char *source_id;
  char *message_id;
  char *payload_digest;
  char *operation_id;
  char *processing_state;
} lc_outbox_inbox_record;

typedef struct lc_outbox_command_record {
  char *record_type;
  char *command_id;
  char *scope;
  char *command_type;
  char *idempotency_key;
  char *request_digest;
  char *operation_id;
  lonejson_int64 accepted_at_unix;
  char *state;
  char *result_code;
  char *result_reference;
  char *result_content_type;
  lonejson_int64 completed_at_unix;
  char *failure_code;
  char *failure_message;
  lonejson_int64 failed_at_unix;
  lonejson_int64 has_result_body;
} lc_outbox_command_record;

static const lonejson_field lc_outbox_record_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_outbox_record, record_type,
                                    "record_type"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_outbox_record, operation_id,
                                    "operation_id"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_outbox_record, effect_id, "effect_id"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_outbox_record, effect_key, "effect_key"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_outbox_record, payload_digest,
                                    "payload_digest"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_outbox_record, message_id, "message_id"),
    LONEJSON_FIELD_STRING_ALLOC(lc_outbox_record, command_id, "command_id"),
    LONEJSON_FIELD_STRING_ALLOC(lc_outbox_record, causation_id, "causation_id"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_outbox_record, kind, "kind"),
    LONEJSON_FIELD_STRING_ALLOC(lc_outbox_record, schema_version,
                                "schema_version"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_outbox_record, destination,
                                    "destination"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_outbox_record, content_type,
                                    "content_type"),
    LONEJSON_FIELD_STRING_ALLOC(lc_outbox_record, headers_json, "headers_json"),
    LONEJSON_FIELD_STRING_ALLOC(lc_outbox_record, trace_context,
                                "trace_context"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_outbox_record, dispatch_state,
                                    "dispatch_state"),
    LONEJSON_FIELD_I64(lc_outbox_record, attempt_count, "attempt_count"),
    LONEJSON_FIELD_I64(lc_outbox_record, claim_expires_at_unix,
                       "claim_expires_at_unix"),
    LONEJSON_FIELD_I64(lc_outbox_record, not_before_unix, "not_before_unix"),
    LONEJSON_FIELD_I64(lc_outbox_record, replay_count, "replay_count"),
    LONEJSON_FIELD_I64(lc_outbox_record, dead_lettered_at_unix,
                       "dead_lettered_at_unix"),
    LONEJSON_FIELD_I64(lc_outbox_record, replayed_at_unix, "replayed_at_unix"),
    LONEJSON_FIELD_STRING_ALLOC(lc_outbox_record, delivery_reference,
                                "delivery_reference"),
    LONEJSON_FIELD_STRING_ALLOC(lc_outbox_record, response_digest,
                                "response_digest"),
    LONEJSON_FIELD_I64(lc_outbox_record, completed_at_unix,
                       "completed_at_unix"),
    LONEJSON_FIELD_STRING_ALLOC(lc_outbox_record, last_error, "last_error"),
    LONEJSON_FIELD_STRING_ALLOC(lc_outbox_record, prior_dead_letter_error,
                                "prior_dead_letter_error")};

static const lonejson_field lc_outbox_claim_deadline_record_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_outbox_claim_deadline_record,
                                    record_type, "record_type"),
    LONEJSON_FIELD_I64(lc_outbox_claim_deadline_record, attempt_count,
                       "attempt_count"),
    LONEJSON_FIELD_I64(lc_outbox_claim_deadline_record, replay_count,
                       "replay_count"),
    LONEJSON_FIELD_I64(lc_outbox_claim_deadline_record, claim_expires_at_unix,
                       "claim_expires_at_unix")};

static const lonejson_field lc_outbox_inbox_record_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_outbox_inbox_record, record_type,
                                    "record_type"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_outbox_inbox_record, consumer_id,
                                    "consumer_id"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_outbox_inbox_record, source_kind,
                                    "source_kind"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_outbox_inbox_record, source_id,
                                    "source_id"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_outbox_inbox_record, message_id,
                                    "message_id"),
    LONEJSON_FIELD_STRING_ALLOC(lc_outbox_inbox_record, payload_digest,
                                "payload_digest"),
    LONEJSON_FIELD_STRING_ALLOC(lc_outbox_inbox_record, operation_id,
                                "operation_id"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_outbox_inbox_record, processing_state,
                                    "processing_state")};

static const lonejson_field lc_outbox_command_record_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_outbox_command_record, record_type,
                                    "record_type"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_outbox_command_record, command_id,
                                    "command_id"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_outbox_command_record, scope, "scope"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_outbox_command_record, command_type,
                                    "command_type"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_outbox_command_record, idempotency_key,
                                    "idempotency_key"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_outbox_command_record, request_digest,
                                    "request_digest"),
    LONEJSON_FIELD_STRING_ALLOC(lc_outbox_command_record, operation_id,
                                "operation_id"),
    LONEJSON_FIELD_I64(lc_outbox_command_record, accepted_at_unix,
                       "accepted_at_unix"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_outbox_command_record, state, "state"),
    LONEJSON_FIELD_STRING_ALLOC(lc_outbox_command_record, result_code,
                                "result_code"),
    LONEJSON_FIELD_STRING_ALLOC(lc_outbox_command_record, result_reference,
                                "result_reference"),
    LONEJSON_FIELD_STRING_ALLOC(lc_outbox_command_record, result_content_type,
                                "result_content_type"),
    LONEJSON_FIELD_I64(lc_outbox_command_record, completed_at_unix,
                       "completed_at_unix"),
    LONEJSON_FIELD_STRING_ALLOC(lc_outbox_command_record, failure_code,
                                "failure_code"),
    LONEJSON_FIELD_STRING_ALLOC(lc_outbox_command_record, failure_message,
                                "failure_message"),
    LONEJSON_FIELD_I64(lc_outbox_command_record, failed_at_unix,
                       "failed_at_unix"),
    LONEJSON_FIELD_I64(lc_outbox_command_record, has_result_body,
                       "has_result_body")};

LONEJSON_MAP_DEFINE(lc_outbox_record_map, lc_outbox_record,
                    lc_outbox_record_fields);
LONEJSON_MAP_DEFINE(lc_outbox_claim_deadline_record_map,
                    lc_outbox_claim_deadline_record,
                    lc_outbox_claim_deadline_record_fields);
LONEJSON_MAP_DEFINE(lc_outbox_inbox_record_map, lc_outbox_inbox_record,
                    lc_outbox_inbox_record_fields);
LONEJSON_MAP_DEFINE(lc_outbox_command_record_map, lc_outbox_command_record,
                    lc_outbox_command_record_fields);

struct lc_outbox_handle {
  lc_outbox pub;
  lc_client_handle *client;
  lc_client_handle *dispatcher_client;
  char *ns;
  char *owner;
  long transaction_ttl_seconds;
  long claim_ttl_seconds;
  long recovery_interval_seconds;
  long retry_initial_delay_seconds;
  long retry_max_delay_seconds;
  long host_retry_delay_max_seconds;
  long shutdown_timeout_ms;
  int max_attempts;
  int replay_dead_letters_on_startup;
  pthread_mutex_t notification_mutex;
  pthread_cond_t notification_cond;
  pthread_cond_t dispatcher_cond;
  char **notifications;
  size_t notification_count;
  size_t notification_capacity;
  lc_outbox_delayed_notification *delayed_notifications;
  size_t delayed_notification_count;
  char *recovery_cursor;
  int recovery_needed;
  int recovery_immediate;
  int recovery_resume_pending;
  int recovery_claims_pending;
  int recovery_scan_mode;
  lc_unix_seconds next_recovery_unix;
  int notification_mutex_initialized;
  int notification_cond_initialized;
  int dispatcher_cond_initialized;
  pthread_t dispatcher_thread;
  int dispatcher_started;
  int dispatcher_exited;
  uint64_t direct_notifications;
  uint64_t notification_overflows;
  uint64_t recovery_queries;
  uint64_t recovered_claims;
  uint64_t claim_losses;
  uint64_t payload_open_failures;
  size_t waiting_consumers;
  char *last_error;
  int startup_dead_letter_replay_pending;
  int startup_dead_letter_replay_flushed;
  int closed;
  int close_requested;
  size_t ref_count;
  lc_outbox_dispatcher_handle *dispatcher;
  lc_outbox_dispatcher_handle *attached_dispatcher;
};

enum {
  LC_OUTBOX_RECOVERY_SCAN_PENDING = 0,
  LC_OUTBOX_RECOVERY_SCAN_CLAIMS = 1,
  LC_OUTBOX_RECOVERY_SCAN_RETRIES = 2
};

struct lc_outbox_dispatcher_handle {
  lc_outbox_dispatcher pub;
  lc_outbox_handle *core;
  lc_client_handle *client;
  lc_allocator allocator;
  pthread_mutex_t lifecycle_mutex;
  pthread_cond_t lifecycle_cond;
  size_t ref_count;
  size_t active_jobs;
  int lifecycle_initialized;
  int stopping;
  int stop_complete;
  int worker_finished;
  /* Client close waits only for the private worker, not caller-owned durable
   * jobs. Their own client references preserve terminal usability. */
  int detach_on_client_close;
  int client_close_requested;
  lc_outbox_dispatcher_handle *registry_next;
};

long lc_outbox_dispatcher_pump_timeout(const lc_outbox_dispatcher *dispatcher) {
  const lc_outbox_dispatcher_handle *handle;

  if (dispatcher == NULL || dispatcher->impl == NULL)
    return 0L;
  handle = (const lc_outbox_dispatcher_handle *)dispatcher->impl;
  if (handle->core == NULL)
    return 0L;
  return handle->core->shutdown_timeout_ms;
}

static void
lc_outbox_dispatcher_retain(lc_outbox_dispatcher_handle *dispatcher);
static void lc_outbox_dispatcher_close_method(lc_outbox_dispatcher *self);
static int lc_outbox_dispatcher_wait_method(lc_outbox_dispatcher *self,
                                            long deadline_ms, lc_error *error);

static void
lc_outbox_replace_attached_dispatcher(lc_outbox_handle *outbox,
                                      lc_outbox_dispatcher_handle *dispatcher) {
  lc_outbox_dispatcher_handle *previous;

  /* The client lifecycle mutex serializes producer attachment replacement with
   * commit-time notification publication and producer close. */
  if (outbox == NULL || outbox->attached_dispatcher == dispatcher)
    return;
  previous = outbox->attached_dispatcher;
  outbox->attached_dispatcher = dispatcher;
  if (previous != NULL)
    lc_outbox_dispatcher_close_method(&previous->pub);
}

static int lc_outbox_dispatcher_configuration_matches(
    const lc_outbox_handle *outbox, lc_outbox_dispatcher_handle *dispatcher) {
  const lc_outbox_handle *core;

  if (outbox == NULL || dispatcher == NULL || dispatcher->core == NULL)
    return 0;
  core = dispatcher->core;
  return strcmp(outbox->ns, core->ns) == 0 &&
         strcmp(outbox->owner, core->owner) == 0 &&
         outbox->transaction_ttl_seconds == core->transaction_ttl_seconds &&
         outbox->claim_ttl_seconds == core->claim_ttl_seconds &&
         outbox->max_attempts == core->max_attempts &&
         outbox->notification_capacity == core->notification_capacity &&
         outbox->retry_initial_delay_seconds ==
             core->retry_initial_delay_seconds &&
         outbox->retry_max_delay_seconds == core->retry_max_delay_seconds &&
         outbox->host_retry_delay_max_seconds ==
             core->host_retry_delay_max_seconds &&
         outbox->recovery_interval_seconds == core->recovery_interval_seconds &&
         outbox->shutdown_timeout_ms == core->shutdown_timeout_ms &&
         outbox->replay_dead_letters_on_startup ==
             core->replay_dead_letters_on_startup;
}

static void lc_outbox_publish_committed_outbox(lc_outbox_handle *outbox,
                                               const char *key) {
  lc_outbox_dispatcher_handle *dispatcher;
  lc_outbox_handle *core;

  if (outbox == NULL || key == NULL)
    return;
  core = NULL;
  /* Attachment replacement can release the old shell's last reference. Take
   * a shell reference while the client lifecycle mutex still protects the
   * attachment pointer, then inspect its state under its own mutex. */
  pthread_mutex_lock(&outbox->client->lifecycle_mutex);
  dispatcher = outbox->attached_dispatcher;
  if (dispatcher != NULL) {
    pthread_mutex_lock(&dispatcher->lifecycle_mutex);
    lc_outbox_dispatcher_retain_locked(dispatcher);
    if (!dispatcher->stopping && dispatcher->core != NULL) {
      core = dispatcher->core;
      lc_outbox_retain(core);
    }
    pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
  }
  pthread_mutex_unlock(&outbox->client->lifecycle_mutex);
  if (core != NULL) {
    (void)lc_outbox_notify(core, key, 1);
    lc_outbox_release(core);
  }
  if (dispatcher != NULL)
    lc_outbox_dispatcher_close_method(&dispatcher->pub);
}

static int lc_outbox_dispatcher_cancel_check(void *context) {
  lc_outbox_handle *outbox = (lc_outbox_handle *)context;
  int closed;

  pthread_mutex_lock(&outbox->notification_mutex);
  closed = outbox->closed;
  pthread_mutex_unlock(&outbox->notification_mutex);
  return closed;
}

static int lc_outbox_replay_dead_letters_on_startup(lc_outbox_handle *outbox,
                                                    lc_error *error);

struct lc_outbox_transaction_handle {
  lc_outbox_transaction pub;
  lc_outbox_handle *outbox;
  lc_lease **leases;
  size_t lease_count;
  size_t lease_capacity;
  char **notification_keys;
  size_t notification_count;
  size_t notification_capacity;
  char **fresh_outbox_keys;
  char **fresh_outbox_effect_keys;
  size_t fresh_outbox_count;
  size_t fresh_outbox_capacity;
  lc_outbox_participant_handle *participants;
  lc_lease *command_lease;
  char *command_id;
  int command_causation_owned;
  char *causation_id;
  int command_terminal;
  int has_domain_participant;
  /* Once a lease has cast a terminal vote, the remaining handle set is a
   * decision retry only: callers must not mutate or enroll more work into an
   * implicit-XA transaction that is already partially decided. */
  int terminal_vote_started;
  int terminal_rollback;
  int terminal;
};

struct lc_outbox_participant_handle {
  lc_outbox_participant pub;
  lc_outbox_handle *outbox;
  lc_outbox_transaction_handle *transaction;
  lc_lease *lease;
  lc_outbox_participant_handle *next;
};

struct lc_outbox_job_handle {
  lc_outbox_job pub;
  lc_client_handle *client;
  lc_outbox_handle *outbox;
  lc_outbox_dispatcher_handle *dispatcher;
  lc_lease *lease;
  char *outbox_key;
  lc_outbox_record record;
  const char *staged_terminal_state;
  lc_unix_seconds staged_terminal_not_before_unix;
  int terminal;
};

/* The dispatcher and public next() callers wait for different predicates.
 * Keep their wakeups separate so a public waiter cannot consume the dispatch
 * signal that makes a durable outbox record eligible for a host job. */
static void lc_outbox_signal_dispatcher_locked(lc_outbox_handle *outbox) {
  if (outbox->dispatcher_cond_initialized)
    pthread_cond_signal(&outbox->dispatcher_cond);
}

static void lc_outbox_request_recovery(lc_outbox_handle *outbox,
                                       lc_unix_seconds not_before_unix) {
  if (outbox == NULL)
    return;
  pthread_mutex_lock(&outbox->notification_mutex);
  if (!outbox->closed) {
    outbox->recovery_needed = 1;
    if (not_before_unix <= 0)
      outbox->recovery_immediate = 1;
    if (not_before_unix > 0 && (outbox->next_recovery_unix == 0 ||
                                not_before_unix < outbox->next_recovery_unix)) {
      outbox->next_recovery_unix = not_before_unix;
    }
    lc_outbox_signal_dispatcher_locked(outbox);
  }
  pthread_mutex_unlock(&outbox->notification_mutex);
}

static void
lc_outbox_request_claim_recovery(lc_outbox_handle *outbox,
                                 lc_unix_seconds claim_expires_at_unix) {
  if (outbox == NULL || claim_expires_at_unix <= 0)
    return;
  pthread_mutex_lock(&outbox->notification_mutex);
  if (!outbox->closed) {
    outbox->recovery_needed = 1;
    outbox->recovery_claims_pending = 1;
    if (outbox->next_recovery_unix == 0 ||
        claim_expires_at_unix < outbox->next_recovery_unix) {
      outbox->next_recovery_unix = claim_expires_at_unix;
    }
    lc_outbox_signal_dispatcher_locked(outbox);
  }
  pthread_mutex_unlock(&outbox->notification_mutex);
}

/* A saturated reconciliation deliberately pauses instead of accumulating an
 * unbounded local queue. Any operation that frees one combined candidate or
 * delayed-wake slot must restart that paused durable sweep. */
static void
lc_outbox_resume_recovery_if_capacity_locked(lc_outbox_handle *outbox) {
  if (outbox->recovery_resume_pending &&
      outbox->notification_count + outbox->delayed_notification_count <
          outbox->notification_capacity) {
    outbox->recovery_resume_pending = 0;
    outbox->recovery_needed = 1;
    outbox->recovery_immediate = 1;
    lc_outbox_signal_dispatcher_locked(outbox);
  }
}

/* Convert one future local wake back into its durable recovery predicate. This
 * is used only under bounded-capacity pressure: direct ready work and an
 * explicit reconciliation must never be blocked indefinitely by a delayed
 * wake. The durable envelope remains authoritative, including whether the
 * displaced key was an active claim rather than a retry. */
static int
lc_outbox_evict_delayed_for_recovery_locked(lc_outbox_handle *outbox) {
  size_t evicted;
  size_t index;
  lc_unix_seconds eligible_at_unix;
  int claim_recovery;

  if (outbox->delayed_notification_count == 0U)
    return 0;
  evicted = 0U;
  for (index = 1U; index < outbox->delayed_notification_count; ++index) {
    if (outbox->delayed_notifications[index].eligible_at_unix >
        outbox->delayed_notifications[evicted].eligible_at_unix) {
      evicted = index;
    }
  }
  eligible_at_unix = outbox->delayed_notifications[evicted].eligible_at_unix;
  claim_recovery = outbox->delayed_notifications[evicted].claim_recovery;
  lc_client_free(outbox->client, outbox->delayed_notifications[evicted].key);
  --outbox->delayed_notification_count;
  if (evicted != outbox->delayed_notification_count) {
    outbox->delayed_notifications[evicted] =
        outbox->delayed_notifications[outbox->delayed_notification_count];
  }
  outbox->recovery_needed = 1;
  if (claim_recovery)
    outbox->recovery_claims_pending = 1;
  if (outbox->next_recovery_unix == 0 ||
      eligible_at_unix < outbox->next_recovery_unix) {
    outbox->next_recovery_unix = eligible_at_unix;
  }
  return 1;
}

static int lc_outbox_notify(lc_outbox_handle *outbox, const char *key,
                            int replace_delayed) {
  char *copy;
  size_t index;
  int accepted;

  if (outbox == NULL || key == NULL ||
      strncmp(key, "__lockdc_io/v1/outbox/",
              sizeof("__lockdc_io/v1/outbox/") - 1U) != 0)
    return 0;
#ifdef LOCKDC_TEST_BUILD
  if (lc_outbox_test_before_notification_copy_hook != NULL &&
      lc_outbox_test_before_notification_copy_hook(
          lc_outbox_test_before_notification_copy_context, NULL) != LC_OK) {
    lc_outbox_request_recovery(outbox, 0);
    return 0;
  }
#endif
  copy = lc_client_strdup(outbox->client, key);
  if (copy == NULL) {
    lc_outbox_request_recovery(outbox, 0);
    return 0;
  }
  accepted = 0;
  pthread_mutex_lock(&outbox->notification_mutex);
  for (index = 0U; !outbox->closed && index < outbox->notification_count;
       ++index) {
    if (strcmp(outbox->notifications[index], key) == 0) {
      accepted = 1;
      break;
    }
  }
  if (replace_delayed) {
    for (index = 0U;
         !outbox->closed && index < outbox->delayed_notification_count;
         ++index) {
      if (strcmp(outbox->delayed_notifications[index].key, key) == 0) {
        lc_client_free(outbox->client,
                       outbox->delayed_notifications[index].key);
        --outbox->delayed_notification_count;
        if (index != outbox->delayed_notification_count) {
          outbox->delayed_notifications[index] =
              outbox->delayed_notifications[outbox->delayed_notification_count];
        }
        break;
      }
    }
  } else {
    /* A retry/claim deadline is already a local durable-work wake-up. A
     * reconciliation pass that finds the same key must not also enqueue an
     * immediate candidate before the durable deadline. */
    for (index = 0U; !outbox->closed && !accepted &&
                     index < outbox->delayed_notification_count;
         ++index) {
      if (strcmp(outbox->delayed_notifications[index].key, key) == 0) {
        accepted = 1;
        break;
      }
    }
  }
  if (!outbox->closed && !accepted &&
      outbox->notification_count + outbox->delayed_notification_count >=
          outbox->notification_capacity &&
      replace_delayed && outbox->delayed_notification_count > 0U) {
    /* A future wake is recoverable from its durable envelope. Evict the
     * furthest one to admit ready work; its exact recovery predicate travels
     * with the displaced wake so a claim cannot become permanently invisible.
     */
    (void)lc_outbox_evict_delayed_for_recovery_locked(outbox);
  }
  if (!outbox->closed && !accepted &&
      outbox->notification_count + outbox->delayed_notification_count <
          outbox->notification_capacity) {
    outbox->notifications[outbox->notification_count++] = copy;
    ++outbox->direct_notifications;
    lc_outbox_signal_dispatcher_locked(outbox);
    if (outbox->notification_cond_initialized)
      pthread_cond_broadcast(&outbox->notification_cond);
    copy = NULL;
    accepted = 1;
  } else if (!outbox->closed && !accepted) {
    outbox->recovery_needed = 1;
    outbox->recovery_immediate = 1;
    ++outbox->notification_overflows;
    lc_outbox_signal_dispatcher_locked(outbox);
  }
  pthread_mutex_unlock(&outbox->notification_mutex);
  lc_client_free(outbox->client, copy);
  return accepted;
}

static void lc_outbox_record_error(lc_outbox_handle *outbox,
                                   const lc_error *error) {
  char *copy;

  if (outbox == NULL || error == NULL || error->message == NULL)
    return;
  copy = lc_client_strdup(outbox->client, error->message);
  if (copy == NULL)
    return;
  pthread_mutex_lock(&outbox->notification_mutex);
  lc_client_free(outbox->client, outbox->last_error);
  outbox->last_error = copy;
  pthread_mutex_unlock(&outbox->notification_mutex);
}

/* An acquire conflict means a different dispatcher owns the outbox lease.
 * lc_outbox_claim_outbox() has already scheduled recovery at its durable
 * claim deadline, so treating it as an ordinary transient failure would pull
 * that deadline forward and continuously poll the active claim. */
static int lc_outbox_claim_failure_is_retryable(const lc_error *error) {
  return error != NULL && error->code != LC_ERR_INVALID &&
         error->http_status != 409L;
}

#ifdef LOCKDC_TEST_BUILD
int lc_outbox_test_claim_failure_is_retryable(const lc_error *error) {
  return lc_outbox_claim_failure_is_retryable(error);
}
#endif

static void lc_outbox_schedule_retry(lc_outbox_handle *outbox, const char *key,
                                     lc_unix_seconds eligible_at_unix) {
  char *copy;
  size_t index;

  if (outbox == NULL || key == NULL || eligible_at_unix <= 0)
    return;
#ifdef LOCKDC_TEST_BUILD
  if (lc_outbox_test_before_notification_copy_hook != NULL &&
      lc_outbox_test_before_notification_copy_hook(
          lc_outbox_test_before_notification_copy_context, NULL) != LC_OK) {
    lc_outbox_request_recovery(outbox, eligible_at_unix);
    return;
  }
#endif
  copy = lc_client_strdup(outbox->client, key);
  if (copy == NULL) {
    lc_outbox_request_recovery(outbox, eligible_at_unix);
    return;
  }
  pthread_mutex_lock(&outbox->notification_mutex);
  if (!outbox->closed) {
    for (index = 0U; index < outbox->delayed_notification_count; ++index) {
      lc_outbox_delayed_notification *delayed =
          &outbox->delayed_notifications[index];
      if (strcmp(delayed->key, key) == 0) {
        if (eligible_at_unix < delayed->eligible_at_unix) {
          delayed->eligible_at_unix = eligible_at_unix;
        }
        delayed->claim_recovery = 0;
        break;
      }
    }
    if (index == outbox->delayed_notification_count) {
      if (outbox->notification_count + outbox->delayed_notification_count <
          outbox->notification_capacity) {
        lc_outbox_delayed_notification *delayed =
            &outbox
                 ->delayed_notifications[outbox->delayed_notification_count++];
        delayed->key = copy;
        delayed->eligible_at_unix = eligible_at_unix;
        delayed->claim_recovery = 0;
        copy = NULL;
      } else {
        outbox->recovery_needed = 1;
        if (outbox->next_recovery_unix == 0 ||
            eligible_at_unix < outbox->next_recovery_unix) {
          outbox->next_recovery_unix = eligible_at_unix;
        }
      }
    }
    lc_outbox_signal_dispatcher_locked(outbox);
  }
  pthread_mutex_unlock(&outbox->notification_mutex);
  lc_client_free(outbox->client, copy);
}

/* A claimed envelope remains durable if its worker disappears. Ordinary
 * observations must preserve the earliest known expiry: a duplicate
 * notification must not postpone recovery. A successful keepalive is the
 * only operation allowed to replace that deadline with a later expiry. */
static void
lc_outbox_schedule_claim_recovery_at(lc_outbox_handle *outbox, const char *key,
                                     lc_unix_seconds claim_expires_at_unix,
                                     int replace_existing) {
  char *copy;
  size_t index;

  if (outbox == NULL || key == NULL || claim_expires_at_unix <= 0)
    return;
#ifdef LOCKDC_TEST_BUILD
  if (lc_outbox_test_before_notification_copy_hook != NULL &&
      lc_outbox_test_before_notification_copy_hook(
          lc_outbox_test_before_notification_copy_context, NULL) != LC_OK) {
    lc_outbox_request_claim_recovery(outbox, claim_expires_at_unix);
    return;
  }
#endif
  copy = lc_client_strdup(outbox->client, key);
  if (copy == NULL) {
    lc_outbox_request_claim_recovery(outbox, claim_expires_at_unix);
    return;
  }
  pthread_mutex_lock(&outbox->notification_mutex);
  if (!outbox->closed) {
    for (index = 0U; index < outbox->delayed_notification_count; ++index) {
      lc_outbox_delayed_notification *delayed =
          &outbox->delayed_notifications[index];
      if (strcmp(delayed->key, key) == 0) {
        if (replace_existing ||
            claim_expires_at_unix < delayed->eligible_at_unix) {
          delayed->eligible_at_unix = claim_expires_at_unix;
        }
        delayed->claim_recovery = 1;
        break;
      }
    }
    if (index == outbox->delayed_notification_count) {
      if (outbox->notification_count + outbox->delayed_notification_count <
          outbox->notification_capacity) {
        lc_outbox_delayed_notification *delayed =
            &outbox
                 ->delayed_notifications[outbox->delayed_notification_count++];
        delayed->key = copy;
        delayed->eligible_at_unix = claim_expires_at_unix;
        delayed->claim_recovery = 1;
        copy = NULL;
      } else {
        /* The key cannot be retained locally; recover the durable claim at
         * expiry rather than letting an idle local Pouch dispatcher strand it.
         */
        outbox->recovery_needed = 1;
        outbox->recovery_claims_pending = 1;
        if (outbox->next_recovery_unix == 0 ||
            claim_expires_at_unix < outbox->next_recovery_unix) {
          outbox->next_recovery_unix = claim_expires_at_unix;
        }
      }
    }
    lc_outbox_signal_dispatcher_locked(outbox);
  }
  pthread_mutex_unlock(&outbox->notification_mutex);
  lc_client_free(outbox->client, copy);
}

static void
lc_outbox_schedule_claim_recovery(lc_outbox_handle *outbox, const char *key,
                                  lc_unix_seconds claim_expires_at_unix) {
  lc_outbox_schedule_claim_recovery_at(outbox, key, claim_expires_at_unix, 0);
}

static void
lc_outbox_refresh_claim_recovery(lc_outbox_handle *outbox, const char *key,
                                 lc_unix_seconds claim_expires_at_unix) {
  lc_outbox_schedule_claim_recovery_at(outbox, key, claim_expires_at_unix, 1);
}

#ifdef LOCKDC_TEST_BUILD
int lc_outbox_test_delayed_recovery_deadline(lc_outbox *outbox, const char *key,
                                             lc_i64 *out_deadline) {
  lc_outbox_handle *inner = (lc_outbox_handle *)outbox;
  size_t index;
  int found = 0;

  if (inner == NULL || key == NULL || out_deadline == NULL)
    return 0;
  pthread_mutex_lock(&inner->notification_mutex);
  for (index = 0U; index < inner->delayed_notification_count; ++index) {
    if (strcmp(inner->delayed_notifications[index].key, key) == 0) {
      *out_deadline =
          (lc_i64)inner->delayed_notifications[index].eligible_at_unix;
      found = 1;
      break;
    }
  }
  pthread_mutex_unlock(&inner->notification_mutex);
  return found;
}

int lc_outbox_test_dispatcher_delayed_recovery_deadline(
    lc_outbox_dispatcher *dispatcher, const char *key, lc_i64 *out_deadline) {
  lc_outbox_dispatcher_handle *handle =
      (lc_outbox_dispatcher_handle *)dispatcher;
  lc_outbox_handle *core;
  int found;

  if (handle == NULL || key == NULL || out_deadline == NULL)
    return 0;
  pthread_mutex_lock(&handle->lifecycle_mutex);
  core = handle->core;
  if (core != NULL)
    lc_outbox_retain(core);
  pthread_mutex_unlock(&handle->lifecycle_mutex);
  if (core == NULL)
    return 0;
  found =
      lc_outbox_test_delayed_recovery_deadline(&core->pub, key, out_deadline);
  lc_outbox_release(core);
  return found;
}

int lc_outbox_test_dispatcher_periodic_recovery_is_armed(
    lc_outbox_dispatcher *dispatcher) {
  lc_outbox_dispatcher_handle *handle =
      (lc_outbox_dispatcher_handle *)dispatcher;
  lc_outbox_handle *core;
  int armed;

  if (handle == NULL)
    return 0;
  pthread_mutex_lock(&handle->lifecycle_mutex);
  core = handle->core;
  if (core != NULL)
    lc_outbox_retain(core);
  pthread_mutex_unlock(&handle->lifecycle_mutex);
  if (core == NULL)
    return 0;
  pthread_mutex_lock(&core->notification_mutex);
  armed = core->next_recovery_unix != 0;
  pthread_mutex_unlock(&core->notification_mutex);
  lc_outbox_release(core);
  return armed;
}

#endif

static void lc_outbox_cancel_delayed_notification(lc_outbox_handle *outbox,
                                                  const char *key) {
  size_t index;

  if (outbox == NULL || key == NULL)
    return;
  pthread_mutex_lock(&outbox->notification_mutex);
  for (index = 0U; index < outbox->delayed_notification_count; ++index) {
    if (strcmp(outbox->delayed_notifications[index].key, key) == 0) {
      lc_client_free(outbox->client, outbox->delayed_notifications[index].key);
      --outbox->delayed_notification_count;
      if (index != outbox->delayed_notification_count) {
        outbox->delayed_notifications[index] =
            outbox->delayed_notifications[outbox->delayed_notification_count];
      }
      lc_outbox_resume_recovery_if_capacity_locked(outbox);
      break;
    }
  }
  pthread_mutex_unlock(&outbox->notification_mutex);
}

static void lc_outbox_promote_due_retries_locked(lc_outbox_handle *outbox,
                                                 lc_unix_seconds now) {
  size_t index;

  for (index = 0U; index < outbox->delayed_notification_count;) {
    lc_outbox_delayed_notification *delayed =
        &outbox->delayed_notifications[index];
    if (delayed->eligible_at_unix > now) {
      ++index;
      continue;
    }
    if (outbox->notification_count < outbox->notification_capacity) {
      outbox->notifications[outbox->notification_count++] = delayed->key;
      delayed->key = NULL;
    } else {
      lc_client_free(outbox->client, delayed->key);
      outbox->recovery_needed = 1;
      outbox->recovery_immediate = 1;
      if (delayed->claim_recovery)
        outbox->recovery_claims_pending = 1;
    }
    --outbox->delayed_notification_count;
    if (index != outbox->delayed_notification_count) {
      outbox->delayed_notifications[index] =
          outbox->delayed_notifications[outbox->delayed_notification_count];
    }
  }
  if (outbox->notification_cond_initialized && outbox->notification_count > 0U)
    pthread_cond_broadcast(&outbox->notification_cond);
}

static lc_unix_seconds
lc_outbox_next_dispatch_deadline_locked(const lc_outbox_handle *outbox) {
  lc_unix_seconds deadline = outbox->next_recovery_unix;
  size_t index;

  for (index = 0U; index < outbox->delayed_notification_count; ++index) {
    lc_unix_seconds eligible =
        outbox->delayed_notifications[index].eligible_at_unix;
    if (deadline == 0 || eligible < deadline)
      deadline = eligible;
  }
  return deadline;
}

/* A durable recovery request with a future deadline is deliberately dormant:
 * it is the bounded fallback when retaining a delayed key failed.  Treating
 * the flag alone as runnable would repeatedly scan an unchanged namespace
 * before the retry or claim expiry can make that record eligible. */
static int lc_outbox_recovery_is_due_locked(const lc_outbox_handle *outbox,
                                            lc_unix_seconds now) {
  return outbox->recovery_needed &&
         (outbox->recovery_immediate || outbox->next_recovery_unix == 0 ||
          now <= 0 || outbox->next_recovery_unix <= now);
}

/* The dispatcher timeout bounds only private cancellation and shutdown. A
 * handed-off job is host-owned and therefore keeps the root client's normal
 * request timeout for payload reads, renewal, and terminal decisions. */
static long lc_outbox_host_job_timeout(const lc_outbox_handle *outbox) {
  return outbox->client->timeout_ms;
}

#ifdef LOCKDC_TEST_BUILD
int lc_outbox_test_recovery_is_due(int recovery_needed, int recovery_immediate,
                                   lc_unix_seconds next_recovery_unix,
                                   lc_unix_seconds now) {
  lc_outbox_handle outbox;

  memset(&outbox, 0, sizeof(outbox));
  outbox.recovery_needed = recovery_needed;
  outbox.recovery_immediate = recovery_immediate;
  outbox.next_recovery_unix = next_recovery_unix;
  return lc_outbox_recovery_is_due_locked(&outbox, now);
}

long lc_outbox_test_host_job_timeout(long root_timeout_ms,
                                     long shutdown_timeout_ms) {
  lc_client_handle client;
  lc_outbox_handle outbox;

  memset(&client, 0, sizeof(client));
  memset(&outbox, 0, sizeof(outbox));
  client.timeout_ms = root_timeout_ms;
  outbox.client = &client;
  outbox.shutdown_timeout_ms = shutdown_timeout_ms;
  return lc_outbox_host_job_timeout(&outbox);
}
#endif

static int
lc_outbox_transaction_add_lease(lc_outbox_transaction_handle *transaction,
                                lc_lease *lease, lc_error *error) {
  lc_lease **grown;
  size_t capacity;

#ifdef LOCKDC_TEST_BUILD
  if (lc_outbox_test_before_ledger_append_hook != NULL) {
    int rc = lc_outbox_test_before_ledger_append_hook(
        lc_outbox_test_before_ledger_append_context, error);

    if (rc != LC_OK)
      return rc;
  }
#endif
  if (transaction->lease_count == transaction->lease_capacity) {
    capacity = transaction->lease_capacity == 0U
                   ? 4U
                   : transaction->lease_capacity * 2U;
    grown = (lc_lease **)lc_client_realloc(
        transaction->outbox->client, transaction->leases,
        capacity * sizeof(*transaction->leases));
    if (grown == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to grow outbox participant ledger", NULL,
                          NULL, NULL);
    }
    transaction->leases = grown;
    transaction->lease_capacity = capacity;
  }
  transaction->leases[transaction->lease_count++] = lease;
  return LC_OK;
}

static void
lc_outbox_transaction_remove_lease(lc_outbox_transaction_handle *transaction,
                                   lc_lease *lease) {
  size_t index;

  if (transaction == NULL || lease == NULL)
    return;
  for (index = 0U; index < transaction->lease_count; ++index) {
    if (transaction->leases[index] == lease) {
      size_t remaining = transaction->lease_count - index - 1U;

      if (remaining > 0U) {
        memmove(&transaction->leases[index], &transaction->leases[index + 1U],
                remaining * sizeof(*transaction->leases));
      }
      transaction->leases[--transaction->lease_count] = NULL;
      return;
    }
  }
}

static int lc_outbox_transaction_reserve_notification_keys(
    lc_outbox_transaction_handle *transaction, size_t additional,
    lc_error *error) {
  char **grown;
  size_t required;

  if (additional > (size_t)-1 - transaction->notification_count) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox dispatch signal count is too large", NULL, NULL,
                        NULL);
  }
  required = transaction->notification_count + additional;
  if (required <= transaction->notification_capacity)
    return LC_OK;
  grown = (char **)lc_client_realloc(transaction->outbox->client,
                                     transaction->notification_keys,
                                     required * sizeof(*grown));
  if (grown == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to retain outbox dispatch signals", NULL, NULL,
                        NULL);
  }
  transaction->notification_keys = grown;
  transaction->notification_capacity = required;
  return LC_OK;
}

static void lc_outbox_transaction_clear_notification_keys(
    lc_outbox_transaction_handle *transaction) {
  size_t index;

  if (transaction == NULL)
    return;
  for (index = 0U; index < transaction->notification_count; ++index)
    lc_client_free(transaction->outbox->client,
                   transaction->notification_keys[index]);
  lc_client_free(transaction->outbox->client, transaction->notification_keys);
  transaction->notification_keys = NULL;
  transaction->notification_count = 0U;
  transaction->notification_capacity = 0U;
}

static void lc_outbox_transaction_clear_fresh_outboxes(
    lc_outbox_transaction_handle *transaction) {
  size_t index;

  if (transaction == NULL)
    return;
  for (index = 0U; index < transaction->fresh_outbox_count; ++index) {
    lc_client_free(transaction->outbox->client,
                   transaction->fresh_outbox_keys[index]);
    lc_client_free(transaction->outbox->client,
                   transaction->fresh_outbox_effect_keys[index]);
  }
  lc_client_free(transaction->outbox->client, transaction->fresh_outbox_keys);
  lc_client_free(transaction->outbox->client,
                 transaction->fresh_outbox_effect_keys);
  transaction->fresh_outbox_keys = NULL;
  transaction->fresh_outbox_effect_keys = NULL;
  transaction->fresh_outbox_count = 0U;
  transaction->fresh_outbox_capacity = 0U;
}

static int lc_outbox_transaction_track_fresh_outbox(
    lc_outbox_transaction_handle *transaction, const char *key,
    const char *effect_key, lc_error *error) {
  char **grown_keys;
  char **grown_effect_keys;
  size_t capacity;

  if (transaction == NULL || key == NULL || effect_key == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox fresh outbox identity is required", NULL, NULL,
                        NULL);
  if (transaction->fresh_outbox_count == transaction->fresh_outbox_capacity) {
    capacity = transaction->fresh_outbox_capacity == 0U
                   ? 4U
                   : transaction->fresh_outbox_capacity * 2U;
    grown_keys = (char **)lc_client_realloc(transaction->outbox->client,
                                            transaction->fresh_outbox_keys,
                                            capacity * sizeof(*grown_keys));
    if (grown_keys == NULL)
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to retain outbox receipt", NULL, NULL, NULL);
    grown_effect_keys = (char **)lc_client_realloc(
        transaction->outbox->client, transaction->fresh_outbox_effect_keys,
        capacity * sizeof(*grown_effect_keys));
    if (grown_effect_keys == NULL) {
      transaction->fresh_outbox_keys = grown_keys;
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to retain outbox receipt", NULL, NULL, NULL);
    }
    transaction->fresh_outbox_keys = grown_keys;
    transaction->fresh_outbox_effect_keys = grown_effect_keys;
    transaction->fresh_outbox_capacity = capacity;
  }
  transaction->fresh_outbox_keys[transaction->fresh_outbox_count] =
      lc_client_strdup(transaction->outbox->client, key);
  transaction->fresh_outbox_effect_keys[transaction->fresh_outbox_count] =
      lc_client_strdup(transaction->outbox->client, effect_key);
  if (transaction->fresh_outbox_keys[transaction->fresh_outbox_count] == NULL ||
      transaction->fresh_outbox_effect_keys[transaction->fresh_outbox_count] ==
          NULL) {
    lc_client_free(
        transaction->outbox->client,
        transaction->fresh_outbox_keys[transaction->fresh_outbox_count]);
    lc_client_free(
        transaction->outbox->client,
        transaction->fresh_outbox_effect_keys[transaction->fresh_outbox_count]);
    transaction->fresh_outbox_keys[transaction->fresh_outbox_count] = NULL;
    transaction->fresh_outbox_effect_keys[transaction->fresh_outbox_count] =
        NULL;
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to retain outbox receipt", NULL, NULL, NULL);
  }
  ++transaction->fresh_outbox_count;
  return LC_OK;
}

static void lc_outbox_transaction_free_prepared_notification_keys(
    lc_outbox_transaction_handle *transaction, char **keys) {
  size_t index;

  if (transaction == NULL)
    return;
  for (index = 0U; index < transaction->lease_count; ++index) {
    lc_client_free(transaction->outbox->client,
                   keys == NULL ? NULL : keys[index]);
  }
  lc_client_free(transaction->outbox->client, keys);
}

static int lc_outbox_digest(const char *value, char out[44], lc_error *error) {
  EVP_MD_CTX *ctx;
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int length;
  static const char base64url[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  size_t i;

  if (value == NULL || value[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox identity must be non-empty", NULL, NULL, NULL);
  }
  ctx = EVP_MD_CTX_new();
  length = 0U;
  if (ctx == NULL || EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) != 1 ||
      EVP_DigestUpdate(ctx, value, strlen(value)) != 1 ||
      EVP_DigestFinal_ex(ctx, digest, &length) != 1 || length != 32U) {
    EVP_MD_CTX_free(ctx);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "failed to digest outbox identity", NULL, NULL, NULL);
  }
  EVP_MD_CTX_free(ctx);
  for (i = 0U; i < 30U; i += 3U) {
    out[(i / 3U) * 4U] = base64url[digest[i] >> 2U];
    out[(i / 3U) * 4U + 1U] =
        base64url[((digest[i] & 0x03U) << 4U) | (digest[i + 1U] >> 4U)];
    out[(i / 3U) * 4U + 2U] =
        base64url[((digest[i + 1U] & 0x0fU) << 2U) | (digest[i + 2U] >> 6U)];
    out[(i / 3U) * 4U + 3U] = base64url[digest[i + 2U] & 0x3fU];
  }
  out[40] = base64url[digest[30] >> 2U];
  out[41] = base64url[((digest[30] & 0x03U) << 4U) | (digest[31] >> 4U)];
  out[42] = base64url[(digest[31] & 0x0fU) << 2U];
  out[43] = '\0';
  return LC_OK;
}

static int lc_outbox_key(lc_outbox_handle *outbox, const lc_outbox_entry *entry,
                         char **out, lc_error *error) {
  char operation[44];
  char effect[44];
  size_t length;
  char *key;
  int rc;

  (void)outbox;

  if (entry == NULL || entry->operation_id == NULL ||
      entry->effect_id == NULL || entry->effect_key == NULL ||
      entry->payload_digest == NULL || entry->kind == NULL ||
      entry->destination == NULL || entry->operation_id[0] == '\0' ||
      entry->effect_id[0] == '\0' || entry->effect_key[0] == '\0' ||
      entry->payload_digest[0] == '\0' || entry->kind[0] == '\0' ||
      entry->destination[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox operation, effect, effect key, payload digest, "
                        "kind, and destination are required",
                        NULL, NULL, NULL);
  }
  rc = lc_outbox_digest(entry->operation_id, operation, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_outbox_digest(entry->effect_id, effect, error);
  if (rc != LC_OK) {
    return rc;
  }
  length = sizeof("__lockdc_io/v1/outbox//") - 1U + 43U + 43U + 1U;
  key = (char *)malloc(length);
  if (key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate outbox key", NULL, NULL, NULL);
  }
  snprintf(key, length, "__lockdc_io/v1/outbox/%s/%s", operation, effect);
  *out = key;
  return LC_OK;
}

static int lc_outbox_is_outbox_key(const char *key) {
  static const char prefix[] = "__lockdc_io/v1/outbox/";

  return key != NULL && strncmp(key, prefix, sizeof(prefix) - 1U) == 0;
}

static int lc_outbox_claim_deadline_key(const char *outbox_key, char **out,
                                        lc_error *error) {
  char digest[44];
  char *key;
  size_t length;
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "claim deadline key requires output", NULL, NULL, NULL);
  }
  *out = NULL;
  rc = lc_outbox_digest(outbox_key, digest, error);
  if (rc != LC_OK)
    return rc;
  length = sizeof("__lockdc_io/v1/outbox-claim/") - 1U + sizeof(digest);
  key = (char *)malloc(length);
  if (key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate outbox claim deadline key", NULL,
                        NULL, NULL);
  }
  snprintf(key, length, "__lockdc_io/v1/outbox-claim/%s", digest);
  *out = key;
  return LC_OK;
}

static int lc_outbox_digest_identity_part(EVP_MD_CTX *ctx, const char *part) {
  uint64_t length;
  unsigned char encoded_length[8];
  size_t byte;

  if (ctx == NULL || part == NULL)
    return 0;
  length = (uint64_t)strlen(part);
  for (byte = 0U; byte < sizeof(encoded_length); ++byte) {
    encoded_length[sizeof(encoded_length) - 1U - byte] =
        (unsigned char)(length & 0xffU);
    length >>= 8U;
  }
  return EVP_DigestUpdate(ctx, encoded_length, sizeof(encoded_length)) == 1 &&
         EVP_DigestUpdate(ctx, part, strlen(part)) == 1;
}

static int lc_outbox_inbox_key(lc_outbox_handle *outbox,
                               const lc_inbox_message *message, char **out,
                               lc_error *error) {
  char identity[44];
  EVP_MD_CTX *ctx;
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int length;
  static const char base64url[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  char *key;
  size_t i;
  size_t key_length;
  if (message == NULL || message->consumer_id == NULL ||
      message->source_kind == NULL || message->source_id == NULL ||
      message->message_id == NULL || message->consumer_id[0] == '\0' ||
      message->source_kind[0] == '\0' || message->source_id[0] == '\0' ||
      message->message_id[0] == '\0')
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "inbox consumer and source identity are required", NULL,
                        NULL, NULL);
  ctx = EVP_MD_CTX_new();
  length = 0U;
  if (ctx == NULL || EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) != 1 ||
      !lc_outbox_digest_identity_part(ctx, message->consumer_id) ||
      !lc_outbox_digest_identity_part(ctx, message->source_kind) ||
      !lc_outbox_digest_identity_part(ctx, message->source_id) ||
      !lc_outbox_digest_identity_part(ctx, message->message_id) ||
      EVP_DigestFinal_ex(ctx, digest, &length) != 1 || length != 32U) {
    EVP_MD_CTX_free(ctx);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "failed to digest inbox identity", NULL, NULL, NULL);
  }
  EVP_MD_CTX_free(ctx);
  for (i = 0U; i < 30U; i += 3U) {
    identity[(i / 3U) * 4U] = base64url[digest[i] >> 2U];
    identity[(i / 3U) * 4U + 1U] =
        base64url[((digest[i] & 0x03U) << 4U) | (digest[i + 1U] >> 4U)];
    identity[(i / 3U) * 4U + 2U] =
        base64url[((digest[i + 1U] & 0x0fU) << 2U) | (digest[i + 2U] >> 6U)];
    identity[(i / 3U) * 4U + 3U] = base64url[digest[i + 2U] & 0x3fU];
  }
  identity[40] = base64url[digest[30] >> 2U];
  identity[41] = base64url[((digest[30] & 0x03U) << 4U) | (digest[31] >> 4U)];
  identity[42] = base64url[(digest[31] & 0x0fU) << 2U];
  identity[43] = '\0';
  key_length = sizeof("__lockdc_io/v1/inbox/") - 1U + 43U + 1U;
  key = (char *)malloc(key_length);
  if (key == NULL)
    return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to allocate inbox key",
                        NULL, NULL, NULL);
  snprintf(key, key_length, "__lockdc_io/v1/inbox/%s", identity);
  (void)outbox;
  *out = key;
  return LC_OK;
}

static int lc_outbox_command_key(const lc_command_identity *identity,
                                 char **out, char command_id[48],
                                 lc_error *error) {
  EVP_MD_CTX *ctx;
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int digest_length;
  static const char base64url[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  const char *parts[3];
  size_t index;
  char identity_digest[44];
  char *key;
  size_t key_length;

  if (identity == NULL || identity->scope == NULL ||
      identity->command_type == NULL || identity->idempotency_key == NULL ||
      identity->scope[0] == '\0' || identity->command_type[0] == '\0' ||
      identity->idempotency_key[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "command scope, type, and idempotency key are required",
                        NULL, NULL, NULL);
  }
  parts[0] = identity->scope;
  parts[1] = identity->command_type;
  parts[2] = identity->idempotency_key;
  ctx = EVP_MD_CTX_new();
  digest_length = 0U;
  if (ctx == NULL || EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) != 1) {
    EVP_MD_CTX_free(ctx);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "failed to digest command identity", NULL, NULL, NULL);
  }
  for (index = 0U; index < 3U; ++index) {
    if (!lc_outbox_digest_identity_part(ctx, parts[index])) {
      EVP_MD_CTX_free(ctx);
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "failed to digest command identity", NULL, NULL,
                          NULL);
    }
  }
  if (EVP_DigestFinal_ex(ctx, digest, &digest_length) != 1 ||
      digest_length != 32U) {
    EVP_MD_CTX_free(ctx);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "failed to digest command identity", NULL, NULL, NULL);
  }
  EVP_MD_CTX_free(ctx);
  for (index = 0U; index < 30U; index += 3U) {
    identity_digest[(index / 3U) * 4U] = base64url[digest[index] >> 2U];
    identity_digest[(index / 3U) * 4U + 1U] =
        base64url[((digest[index] & 0x03U) << 4U) | (digest[index + 1U] >> 4U)];
    identity_digest[(index / 3U) * 4U + 2U] =
        base64url[((digest[index + 1U] & 0x0fU) << 2U) |
                  (digest[index + 2U] >> 6U)];
    identity_digest[(index / 3U) * 4U + 3U] =
        base64url[digest[index + 2U] & 0x3fU];
  }
  identity_digest[40] = base64url[digest[30] >> 2U];
  identity_digest[41] =
      base64url[((digest[30] & 0x03U) << 4U) | (digest[31] >> 4U)];
  identity_digest[42] = base64url[(digest[31] & 0x0fU) << 2U];
  identity_digest[43] = '\0';
  key_length = sizeof("__lockdc_io/v1/command/") - 1U + 43U + 1U;
  key = (char *)malloc(key_length);
  if (key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate command receipt key", NULL, NULL,
                        NULL);
  }
  snprintf(key, key_length, "__lockdc_io/v1/command/%s", identity_digest);
  snprintf(command_id, 48U, "cmd_%s", identity_digest);
  *out = key;
  return LC_OK;
}

static int lc_outbox_command_key_from_id(const char *command_id, char **out,
                                         lc_error *error) {
  const char *digest;
  size_t index;
  size_t key_length;
  char *key;

  if (command_id == NULL || strncmp(command_id, "cmd_", 4U) != 0 ||
      strlen(command_id) != 47U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "canonical command id is required", NULL, NULL, NULL);
  }
  digest = command_id + 4U;
  for (index = 0U; index < 43U; ++index) {
    unsigned char ch = (unsigned char)digest[index];

    if (!((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
          (ch >= '0' && ch <= '9') || ch == '-' || ch == '_')) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "canonical command id is required", NULL, NULL, NULL);
    }
  }
  key_length = sizeof("__lockdc_io/v1/command/") - 1U + 43U + 1U;
  key = (char *)malloc(key_length);
  if (key == NULL)
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate command receipt key", NULL, NULL,
                        NULL);
  snprintf(key, key_length, "__lockdc_io/v1/command/%s", digest);
  *out = key;
  return LC_OK;
}

typedef struct lc_outbox_headers_json_validation {
  int root_is_object;
} lc_outbox_headers_json_validation;

static lonejson_status lc_outbox_headers_json_object_begin(
    void *context, const lonejson_value_path *path, lonejson_error *error) {
  lc_outbox_headers_json_validation *validation =
      (lc_outbox_headers_json_validation *)context;

  (void)error;
  if (validation != NULL && path != NULL && path->segment_count == 0U)
    validation->root_is_object = 1;
  return LONEJSON_STATUS_OK;
}

static int lc_outbox_validate_headers_json(const char *headers_json,
                                           lc_error *error) {
  lonejson_path_value_visitor visitor;
  lonejson_error json_error;
  lonejson *runtime;
  lonejson_status status;
  lc_outbox_headers_json_validation validation;

  if (headers_json == NULL)
    return LC_OK;
  runtime = lc_thread_lonejson_runtime();
  if (runtime == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize outbox JSON runtime", NULL, NULL,
                        NULL);
  }
  memset(&validation, 0, sizeof(validation));
  visitor = lonejson_default_path_value_visitor();
  visitor.object_begin = lc_outbox_headers_json_object_begin;
  lonejson_error_init(&json_error);
  status = runtime->visit_path_value_cstr(runtime, headers_json, &visitor,
                                          &validation, &json_error);
  if (status != LONEJSON_STATUS_OK || !validation.root_is_object) {
    return lc_error_set(
        error,
        status == LONEJSON_STATUS_ALLOCATION_FAILED ? LC_ERR_NOMEM
                                                    : LC_ERR_INVALID,
        0L, "outbox headers_json must be a valid JSON object",
        status == LONEJSON_STATUS_OK ? NULL : json_error.message,
        status == LONEJSON_STATUS_OK ? NULL : lonejson_status_string(status),
        NULL);
  }
  return LC_OK;
}

static int lc_outbox_validate_outbox_envelope(const lc_outbox_entry *entry,
                                              lc_error *error) {
  const char *fields[11];
  size_t total;
  size_t index;

  if (entry == NULL)
    return LC_OK;
  fields[0] = entry->operation_id;
  fields[1] = entry->effect_id;
  fields[2] = entry->effect_key;
  fields[3] = entry->payload_digest;
  fields[4] = entry->causation_id;
  fields[5] = entry->kind;
  fields[6] = entry->schema_version;
  fields[7] = entry->destination;
  fields[8] = entry->content_type;
  fields[9] = entry->headers_json;
  fields[10] = entry->trace_context;
  total = 0U;
  for (index = 0U; index < sizeof(fields) / sizeof(fields[0]); ++index) {
    size_t length;

    if (fields[index] == NULL)
      continue;
    length = strlen(fields[index]);
    if (length > (size_t)LC_OUTBOX_MAX_ENVELOPE_BYTES - total) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "outbox envelope exceeds the retained size limit",
                          NULL, NULL, NULL);
    }
    total += length;
  }
  return lc_outbox_validate_headers_json(entry->headers_json, error);
}

#define LC_OUTBOX_RECEIPT_JSON_FIXED_BYTES 1024U

static size_t lc_outbox_receipt_input_limit(const lc_client_handle *client) {
  size_t serialized_limit = (size_t)LC_OUTBOX_MAX_RECEIPT_BYTES;

  /* Zero is the documented client default, not a zero-byte response budget. */
  if (client != NULL && !client->is_pouch &&
      client->http_json_response_limit_bytes > 0U &&
      client->http_json_response_limit_bytes < serialized_limit) {
    serialized_limit = client->http_json_response_limit_bytes;
  }
  if (serialized_limit <= LC_OUTBOX_RECEIPT_JSON_FIXED_BYTES)
    return 0U;
  /* Every input byte may need a six-byte JSON escape sequence. Reserve ample
   * fixed-record headroom so a caller never commits a receipt its own remote
   * client cannot later parse under the configured response limit. */
  return (serialized_limit - LC_OUTBOX_RECEIPT_JSON_FIXED_BYTES) / 6U;
}

static int lc_outbox_validate_receipt_fields(const lc_client_handle *client,
                                             const char *const *fields,
                                             size_t field_count,
                                             const char *kind,
                                             lc_error *error) {
  size_t limit;
  size_t total;
  size_t index;

  limit = lc_outbox_receipt_input_limit(client);
  total = 0U;
  for (index = 0U; index < field_count; ++index) {
    size_t length;

    if (fields[index] == NULL)
      continue;
    length = strlen(fields[index]);
    if (length > limit - total) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "outbox receipt metadata exceeds the retained size "
                          "limit",
                          kind, NULL, NULL);
    }
    total += length;
  }
  return LC_OK;
}

static int lc_outbox_validate_command_request(const lc_client_handle *client,
                                              const lc_command_request *request,
                                              lc_error *error) {
  const char *fields[5];

  if (request == NULL || request->request_digest == NULL ||
      request->request_digest[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "command request digest is required", NULL, NULL, NULL);
  }
  if (request->generate_idempotency_key != 0 &&
      request->generate_idempotency_key != 1) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "command idempotency-key generation flag is invalid",
                        NULL, NULL, NULL);
  }
  if (request->generate_idempotency_key) {
    if (request->identity.idempotency_key != NULL &&
        request->identity.idempotency_key[0] != '\0') {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "generated command idempotency key must be omitted",
                          NULL, NULL, NULL);
    }
  } else if (request->identity.idempotency_key == NULL ||
             request->identity.idempotency_key[0] == '\0') {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "command idempotency key is required unless generation is enabled",
        NULL, NULL, NULL);
  }
  fields[0] = request->identity.scope;
  fields[1] = request->identity.command_type;
  fields[2] = request->identity.idempotency_key;
  fields[3] = request->request_digest;
  fields[4] = request->operation_id;
  return lc_outbox_validate_receipt_fields(
      client, fields, sizeof(fields) / sizeof(fields[0]), "command", error);
}

static int lc_outbox_effective_command_request(
    const lc_command_request *request, lc_command_request *effective,
    char generated_key[LC_XID_STRING_SIZE], lc_error *error) {
  if (request == NULL || effective == NULL || generated_key == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "command request and effective request are required",
                        NULL, NULL, NULL);
  *effective = *request;
  if (request->generate_idempotency_key == 0)
    return LC_OK;
  if (request->generate_idempotency_key != 1 ||
      (request->identity.idempotency_key != NULL &&
       request->identity.idempotency_key[0] != '\0')) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "generated command idempotency key must be explicitly "
                        "requested without a supplied key",
                        NULL, NULL, NULL);
  }
  if (lc_xid_new(generated_key, error) != LC_OK)
    return error != NULL ? error->code : LC_ERR_TRANSPORT;
  effective->identity.idempotency_key = generated_key;
  effective->generate_idempotency_key = 0;
  return LC_OK;
}

/* A terminal transition adds metadata to the durable command receipt. Validate
 * the complete retained record before staging it so the same client can always
 * read the receipt it committed. */
static int lc_outbox_validate_command_terminal_metadata(
    const lc_client_handle *client, const lc_outbox_command_record *record,
    const lc_command_result *result, int failed, lc_error *error) {
  const char *fields[8];

  if (record == NULL || result == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "command terminal receipt inputs are required", NULL,
                        NULL, NULL);
  }
  fields[0] = record->scope;
  fields[1] = record->command_type;
  fields[2] = record->idempotency_key;
  fields[3] = record->request_digest;
  fields[4] = record->operation_id;
  fields[5] = failed ? result->failure_code : result->result_code;
  fields[6] = failed ? result->failure_message : result->result_reference;
  fields[7] = failed ? NULL : result->content_type;
  return lc_outbox_validate_receipt_fields(
      client, fields, sizeof(fields) / sizeof(fields[0]), "command", error);
}

#ifdef LOCKDC_TEST_BUILD
size_t lc_outbox_test_receipt_input_limit(int is_pouch,
                                          size_t response_limit_bytes) {
  lc_client_handle client;

  memset(&client, 0, sizeof(client));
  client.is_pouch = is_pouch;
  client.http_json_response_limit_bytes = response_limit_bytes;
  return lc_outbox_receipt_input_limit(&client);
}
#endif

static int lc_outbox_validate_inbox_message(const lc_client_handle *client,
                                            const lc_inbox_message *message,
                                            lc_error *error) {
  const char *fields[6];

  if (message == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L, "inbox message is required",
                        NULL, NULL, NULL);
  fields[0] = message->consumer_id;
  fields[1] = message->source_kind;
  fields[2] = message->source_id;
  fields[3] = message->message_id;
  fields[4] = message->payload_digest;
  fields[5] = message->operation_id;
  return lc_outbox_validate_receipt_fields(
      client, fields, sizeof(fields) / sizeof(fields[0]), "inbox", error);
}

/* Durable recovery must enforce the same host-facing envelope boundary as a
 * new append. The mapper establishes field presence, but an external write or
 * storage corruption can still leave required strings empty or make the
 * embedded headers value invalid. */
static int
lc_outbox_validate_durable_outbox_record(const lc_outbox_record *record,
                                         lc_error *error) {
  lc_outbox_entry entry;

  if (record == NULL || record->operation_id == NULL ||
      record->effect_id == NULL || record->effect_key == NULL ||
      record->payload_digest == NULL || record->message_id == NULL ||
      record->kind == NULL || record->destination == NULL ||
      record->content_type == NULL || record->operation_id[0] == '\0' ||
      record->effect_id[0] == '\0' || record->effect_key[0] == '\0' ||
      record->payload_digest[0] == '\0' || record->message_id[0] == '\0' ||
      record->kind[0] == '\0' || record->destination[0] == '\0' ||
      record->content_type[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "durable outbox envelope is missing required fields",
                        NULL, NULL, NULL);
  }
  lc_outbox_entry_init(&entry);
  entry.operation_id = record->operation_id;
  entry.effect_id = record->effect_id;
  entry.effect_key = record->effect_key;
  entry.payload_digest = record->payload_digest;
  entry.causation_id = record->causation_id;
  entry.kind = record->kind;
  entry.schema_version = record->schema_version;
  entry.destination = record->destination;
  entry.content_type = record->content_type;
  entry.headers_json = record->headers_json;
  entry.trace_context = record->trace_context;
  return lc_outbox_validate_outbox_envelope(&entry, error);
}

/* Mapped records are owned by the parser runtime that decoded them. Remote
 * clients decode through their engine runtime, whereas Pouch uses the caller
 * thread runtime. Keeping that distinction here both preserves allocator
 * ownership and lets outbox entry points report a Pouch runtime allocation
 * failure before they acquire or decode durable state. */
static lonejson *lc_outbox_json_runtime(lc_client_handle *client) {
  if (client != NULL && !client->is_pouch) {
    return lc_engine_lonejson_runtime(client->engine);
  }
  return lc_thread_lonejson_runtime();
}

static int lc_outbox_require_json_runtime(lc_client_handle *client,
                                          lonejson **out, lc_error *error) {
  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox JSON runtime output is required", NULL, NULL,
                        NULL);
  }
  *out = lc_outbox_json_runtime(client);
  if (*out == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize outbox JSON runtime", NULL, NULL,
                        NULL);
  }
  return LC_OK;
}

static int lc_outbox_validate_diagnostic(const char *diagnostic,
                                         lc_error *error) {
  size_t length;

  if (diagnostic == NULL)
    return LC_OK;
  for (length = 0U; length <= LC_OUTBOX_MAX_DIAGNOSTIC_BYTES; ++length) {
    if (diagnostic[length] == '\0')
      return LC_OK;
  }
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "outbox diagnostic exceeds the retained size limit", NULL,
                      NULL, NULL);
}

static int
lc_outbox_validate_completion_evidence(const lc_outbox_completion *completion,
                                       lc_error *error) {
  const char *values[2];
  const char *names[2];
  size_t index;
  size_t length;

  if (completion == NULL)
    return LC_OK;
  values[0] = completion->delivery_reference;
  values[1] = completion->response_digest;
  names[0] = "delivery reference";
  names[1] = "response digest";
  for (index = 0U; index < 2U; ++index) {
    if (values[index] == NULL)
      continue;
    for (length = 0U; length <= LC_OUTBOX_MAX_COMPLETION_EVIDENCE_BYTES;
         ++length) {
      if (values[index][length] == '\0')
        break;
    }
    if (length > LC_OUTBOX_MAX_COMPLETION_EVIDENCE_BYTES) {
      return lc_error_set(
          error, LC_ERR_INVALID, 0L,
          "outbox completion evidence exceeds the retained size "
          "limit",
          names[index], NULL, NULL);
    }
  }
  return LC_OK;
}

static int lc_outbox_validate_failure_message(const char *message,
                                              lc_error *error) {
  size_t length;

  if (message == NULL)
    return LC_OK;
  for (length = 0U; length <= LC_OUTBOX_MAX_COMPLETION_EVIDENCE_BYTES;
       ++length) {
    if (message[length] == '\0')
      return LC_OK;
  }
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "command failure message exceeds the retained size limit",
                      NULL, NULL, NULL);
}

static time_t lc_outbox_time_t_maximum(void);

static int lc_outbox_timestamp_add(lc_unix_seconds base, long delta,
                                   const char *field, lc_unix_seconds *out,
                                   lc_error *error) {
  char message[128];

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox timestamp addition requires output", NULL, NULL,
                        NULL);
  }
  if (delta < 0L) {
    snprintf(message, sizeof(message), "outbox %s must be non-negative",
             field != NULL ? field : "duration");
    return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL, NULL);
  }
  if ((uintmax_t)delta > (uintmax_t)LC_I64_MAX ||
      base > LC_I64_MAX - (lc_unix_seconds)delta) {
    snprintf(message, sizeof(message),
             "outbox %s exceeds supported timestamp range",
             field != NULL ? field : "duration");
    return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL, NULL);
  }
  *out = base + (lc_unix_seconds)delta;
  if (*out > (lc_unix_seconds)lc_outbox_time_t_maximum()) {
    snprintf(message, sizeof(message),
             "outbox %s exceeds supported wait deadline range",
             field != NULL ? field : "duration");
    return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL, NULL);
  }
  return LC_OK;
}

static long lc_outbox_timestamp_max_delay(lc_unix_seconds base) {
  lc_unix_seconds remaining;
  lc_unix_seconds wait_remaining;
  lc_unix_seconds wait_maximum;
  long maximum;

  /* A negative clock is still safely advanceable by every supported long
   * duration. Avoid subtracting it from LC_I64_MAX, which could overflow. */
  if (base < 0)
    return LONG_MAX;
  wait_maximum = (lc_unix_seconds)lc_outbox_time_t_maximum();
  if (base >= LC_I64_MAX || base >= wait_maximum)
    return 0L;
  remaining = LC_I64_MAX - base;
  wait_remaining = wait_maximum - base;
  maximum =
      (uintmax_t)remaining > (uintmax_t)LONG_MAX ? LONG_MAX : (long)remaining;
  if ((uintmax_t)wait_remaining < (uintmax_t)maximum)
    maximum = (long)wait_remaining;
  return maximum;
}

static time_t lc_outbox_time_t_maximum(void) {
  uintmax_t maximum;
  size_t bits;

  bits = sizeof(time_t) * CHAR_BIT;
  if (bits >= sizeof(uintmax_t) * CHAR_BIT)
    maximum = (uintmax_t)-1;
  else
    maximum = ((uintmax_t)1U << bits) - 1U;
  if ((time_t)-1 < (time_t)0)
    maximum >>= 1U;
  return (time_t)maximum;
}

static void
lc_outbox_timespec_add_milliseconds_saturating(struct timespec *deadline,
                                               long timeout_ms) {
  time_t maximum;
  uintmax_t seconds;
  uintmax_t remaining;
  long nanoseconds;
  int carry;

  maximum = lc_outbox_time_t_maximum();
  seconds = (uintmax_t)timeout_ms / 1000U;
  nanoseconds = deadline->tv_nsec + (timeout_ms % 1000L) * 1000000L;
  carry = nanoseconds >= 1000000000L;

  if (deadline->tv_sec >= maximum) {
    deadline->tv_sec = maximum;
    deadline->tv_nsec = 999999999L;
    return;
  }
  remaining = (uintmax_t)(maximum - deadline->tv_sec);
  if (seconds > remaining || (seconds == remaining && carry)) {
    deadline->tv_sec = maximum;
    deadline->tv_nsec = 999999999L;
    return;
  }
  deadline->tv_sec += (time_t)seconds;
  if (carry) {
    ++deadline->tv_sec;
    nanoseconds -= 1000000000L;
  }
  deadline->tv_nsec = nanoseconds;
}

static int lc_outbox_retry_is_not_eligible(lonejson_int64 not_before_unix,
                                           lc_unix_seconds now) {
  return not_before_unix > (lonejson_int64)now;
}

#ifdef LOCKDC_TEST_BUILD
int lc_outbox_test_retry_is_not_eligible(lc_unix_seconds not_before_unix,
                                         lc_unix_seconds now) {
  return lc_outbox_retry_is_not_eligible((lonejson_int64)not_before_unix, now);
}
#endif

static int
lc_outbox_command_receipt_from_record(const lc_outbox_command_record *record,
                                      lc_command_receipt *receipt,
                                      lc_error *error) {
  if (record == NULL || receipt == NULL || record->record_type == NULL ||
      record->command_id == NULL || record->scope == NULL ||
      record->command_type == NULL || record->idempotency_key == NULL ||
      record->request_digest == NULL || record->state == NULL ||
      record->command_id[0] == '\0' || record->scope[0] == '\0' ||
      record->command_type[0] == '\0' || record->idempotency_key[0] == '\0' ||
      record->request_digest[0] == '\0' || record->accepted_at_unix <= 0 ||
      strcmp(record->record_type, "lockdc.command.v1") != 0) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "command receipt record is malformed", NULL, NULL,
                        NULL);
  }
  if (strcmp(record->state, "pending") == 0) {
    if (record->result_code != NULL || record->result_reference != NULL ||
        record->result_content_type != NULL || record->completed_at_unix != 0 ||
        record->failure_code != NULL || record->failure_message != NULL ||
        record->failed_at_unix != 0 || record->has_result_body != 0) {
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pending command receipt contains terminal outcome",
                          NULL, NULL, NULL);
    }
  } else if (strcmp(record->state, "completed") == 0) {
    if (record->result_code == NULL || record->result_code[0] == '\0' ||
        record->completed_at_unix <= 0 || record->failure_code != NULL ||
        record->failure_message != NULL || record->failed_at_unix != 0 ||
        (record->has_result_body != 0 && record->has_result_body != 1) ||
        (record->has_result_body != 0 &&
         (record->result_content_type == NULL ||
          record->result_content_type[0] == '\0'))) {
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "completed command receipt is malformed", NULL, NULL,
                          NULL);
    }
  } else if (strcmp(record->state, "failed") == 0) {
    if (record->failure_code == NULL || record->failure_code[0] == '\0' ||
        record->failed_at_unix <= 0 || record->result_code != NULL ||
        record->result_reference != NULL ||
        record->result_content_type != NULL || record->completed_at_unix != 0 ||
        record->has_result_body != 0) {
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "failed command receipt is malformed", NULL, NULL,
                          NULL);
    }
  } else {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "command receipt state is invalid", NULL, NULL, NULL);
  }
  lc_command_receipt_cleanup(receipt);
  if ((receipt->command_id = lc_strdup_local(record->command_id)) == NULL ||
      (receipt->scope = lc_strdup_local(record->scope)) == NULL ||
      (receipt->command_type = lc_strdup_local(record->command_type)) == NULL ||
      (receipt->idempotency_key = lc_strdup_local(record->idempotency_key)) ==
          NULL ||
      (record->operation_id != NULL && (receipt->operation_id = lc_strdup_local(
                                            record->operation_id)) == NULL) ||
      (record->result_code != NULL &&
       (receipt->result_code = lc_strdup_local(record->result_code)) == NULL) ||
      (record->result_reference != NULL &&
       (receipt->result_reference =
            lc_strdup_local(record->result_reference)) == NULL) ||
      (record->failure_code != NULL && (receipt->failure_code = lc_strdup_local(
                                            record->failure_code)) == NULL) ||
      (record->failure_message != NULL &&
       (receipt->failure_message = lc_strdup_local(record->failure_message)) ==
           NULL)) {
    lc_command_receipt_cleanup(receipt);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy command receipt", NULL, NULL, NULL);
  }
  if (strcmp(record->state, "pending") == 0)
    receipt->state = LC_COMMAND_PENDING;
  else if (strcmp(record->state, "completed") == 0)
    receipt->state = LC_COMMAND_COMPLETED;
  else if (strcmp(record->state, "failed") == 0)
    receipt->state = LC_COMMAND_FAILED;
  receipt->has_result_body = record->has_result_body != 0;
  return LC_OK;
}

static int lc_outbox_stage_command(lc_client_handle *client, lc_lease *lease,
                                   const lc_command_request *request,
                                   const char *command_id, lc_error *error) {
  lc_outbox_command_record record;
  time_t now;

  if (lc_outbox_validate_command_request(client, request, error) != LC_OK)
    return error != NULL ? error->code : LC_ERR_INVALID;
  now = time(NULL);
  if (now == (time_t)-1)
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "failed to read command receipt clock", NULL, NULL,
                        NULL);
  memset(&record, 0, sizeof(record));
  record.record_type = "lockdc.command.v1";
  record.command_id = (char *)command_id;
  record.scope = (char *)request->identity.scope;
  record.command_type = (char *)request->identity.command_type;
  record.idempotency_key = (char *)request->identity.idempotency_key;
  record.request_digest = (char *)request->request_digest;
  record.operation_id = (char *)request->operation_id;
  record.accepted_at_unix = (lonejson_int64)now;
  record.state = "pending";
  return lc_lease_save(lease, &lc_outbox_command_record_map, &record, error);
}

static int lc_outbox_stage_inbox(lc_client_handle *client, lc_lease *lease,
                                 const lc_inbox_message *message,
                                 lc_error *error) {
  lc_outbox_inbox_record record;

  if (lc_outbox_validate_inbox_message(client, message, error) != LC_OK)
    return error != NULL ? error->code : LC_ERR_INVALID;

  memset(&record, 0, sizeof(record));
  record.record_type = "lockdc.inbox.v1";
  record.consumer_id = (char *)message->consumer_id;
  record.source_kind = (char *)message->source_kind;
  record.source_id = (char *)message->source_id;
  record.message_id = (char *)message->message_id;
  record.payload_digest = (char *)message->payload_digest;
  record.operation_id = (char *)message->operation_id;
  record.processing_state = "accepted";
  return lc_lease_save(lease, &lc_outbox_inbox_record_map, &record, error);
}

static int lc_outbox_nullable_string_equal(const char *left,
                                           const char *right) {
  return left == right ||
         (left != NULL && right != NULL && strcmp(left, right) == 0);
}

static int lc_outbox_existing_outbox(lc_outbox_handle *outbox, lc_lease *lease,
                                     const char *key,
                                     const lc_outbox_entry *entry,
                                     lc_outbox_receipt *receipt,
                                     lc_error *error) {
  lc_outbox_record record;
  lc_get_res result;
  lc_get_opts options;
  lonejson *runtime;
  const char *content_type;
  int rc;

  memset(&record, 0, sizeof(record));
  memset(&result, 0, sizeof(result));
  lc_get_opts_init(&options);
  options.public_read = 1;
  content_type = entry->content_type != NULL ? entry->content_type
                                             : "application/octet-stream";
  rc = lc_outbox_require_json_runtime(outbox->client, &runtime, error);
  if (rc != LC_OK)
    return rc;
  if (lease != NULL) {
    rc = lc_lease_load(lease, &lc_outbox_record_map, &record, NULL, &result,
                       error);
  } else {
    rc = lc_load_in_namespace(&outbox->client->pub, outbox->ns, key,
                              &lc_outbox_record_map, &record, &options, &result,
                              error);
  }
  if (rc == LC_OK &&
      (result.no_content || record.record_type == NULL ||
       record.operation_id == NULL || record.effect_id == NULL ||
       record.effect_key == NULL || record.payload_digest == NULL ||
       record.message_id == NULL || record.kind == NULL ||
       record.destination == NULL || record.content_type == NULL ||
       strcmp(record.record_type, "lockdc.outbox.v1") != 0 ||
       strcmp(record.operation_id, entry->operation_id) != 0 ||
       strcmp(record.effect_id, entry->effect_id) != 0 ||
       strcmp(record.effect_key, entry->effect_key) != 0 ||
       strcmp(record.payload_digest, entry->payload_digest) != 0 ||
       !lc_outbox_nullable_string_equal(record.causation_id,
                                        entry->causation_id) ||
       strcmp(record.kind, entry->kind) != 0 ||
       !lc_outbox_nullable_string_equal(record.schema_version,
                                        entry->schema_version) ||
       strcmp(record.destination, entry->destination) != 0 ||
       strcmp(record.content_type, content_type) != 0 ||
       !lc_outbox_nullable_string_equal(record.headers_json,
                                        entry->headers_json) ||
       !lc_outbox_nullable_string_equal(record.trace_context,
                                        entry->trace_context))) {
    rc =
        lc_error_set(error, LC_ERR_SERVER, 0L,
                     "outbox immutable fields conflict with an existing record",
                     NULL, NULL, NULL);
  }
  runtime->cleanup(runtime, &lc_outbox_record_map, &record);
  lc_get_res_cleanup(&result);
  if (rc == LC_OK) {
    receipt->outbox_key = lc_strdup_local(key);
    receipt->effect_key = lc_strdup_local(entry->effect_key);
    if (receipt->outbox_key == NULL || receipt->effect_key == NULL) {
      lc_outbox_receipt_cleanup(receipt);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate duplicate outbox receipt", NULL,
                          NULL, NULL);
    }
    receipt->duplicate = 1;
  }
  return rc;
}

static int lc_outbox_existing_inbox(lc_outbox_handle *outbox, lc_lease *lease,
                                    const char *key,
                                    const lc_inbox_message *message,
                                    lc_inbox_accept_result *result,
                                    lc_error *error) {
  lc_outbox_inbox_record record;
  lc_get_res load_result;
  lc_get_opts options;
  lonejson *runtime;
  int rc;

  memset(&record, 0, sizeof(record));
  memset(&load_result, 0, sizeof(load_result));
  lc_get_opts_init(&options);
  options.public_read = 1;
  rc = lc_outbox_require_json_runtime(outbox->client, &runtime, error);
  if (rc != LC_OK)
    return rc;
  if (lease != NULL) {
    rc = lc_lease_load(lease, &lc_outbox_inbox_record_map, &record, NULL,
                       &load_result, error);
  } else {
    rc = lc_load_in_namespace(&outbox->client->pub, outbox->ns, key,
                              &lc_outbox_inbox_record_map, &record, &options,
                              &load_result, error);
  }
  if (rc == LC_OK &&
      (load_result.no_content || record.record_type == NULL ||
       record.consumer_id == NULL || record.source_kind == NULL ||
       record.source_id == NULL || record.message_id == NULL ||
       strcmp(record.record_type, "lockdc.inbox.v1") != 0 ||
       strcmp(record.consumer_id, message->consumer_id) != 0 ||
       strcmp(record.source_kind, message->source_kind) != 0 ||
       strcmp(record.source_id, message->source_id) != 0 ||
       strcmp(record.message_id, message->message_id) != 0 ||
       ((record.payload_digest == NULL) != (message->payload_digest == NULL)) ||
       (record.payload_digest != NULL &&
        strcmp(record.payload_digest, message->payload_digest) != 0) ||
       !lc_outbox_nullable_string_equal(record.operation_id,
                                        message->operation_id))) {
    rc = lc_error_set(error, LC_ERR_SERVER, 0L,
                      "inbox immutable fields conflict with an existing record",
                      NULL, NULL, NULL);
  }
  runtime->cleanup(runtime, &lc_outbox_inbox_record_map, &record);
  lc_get_res_cleanup(&load_result);
  if (rc == LC_OK)
    result->duplicate = 1;
  return rc;
}

static int lc_outbox_existing_command(lc_outbox_handle *outbox, lc_lease *lease,
                                      const char *key,
                                      const lc_command_identity *identity,
                                      const char *command_id,
                                      const lc_command_request *request,
                                      lc_command_receipt *receipt,
                                      lc_error *error) {
  lc_outbox_command_record record;
  lc_get_res load_result;
  lc_get_opts options;
  lonejson *runtime;
  int rc;

  memset(&record, 0, sizeof(record));
  memset(&load_result, 0, sizeof(load_result));
  lc_get_opts_init(&options);
  options.public_read = 1;
  rc = lc_outbox_require_json_runtime(outbox->client, &runtime, error);
  if (rc != LC_OK)
    return rc;
  if (lease != NULL) {
    rc = lc_lease_load(lease, &lc_outbox_command_record_map, &record, NULL,
                       &load_result, error);
  } else {
    rc = lc_load_in_namespace(&outbox->client->pub, outbox->ns, key,
                              &lc_outbox_command_record_map, &record, &options,
                              &load_result, error);
  }
  if (rc == LC_OK &&
      (load_result.no_content || record.record_type == NULL ||
       record.command_id == NULL || record.scope == NULL ||
       record.command_type == NULL || record.idempotency_key == NULL ||
       record.request_digest == NULL || identity == NULL ||
       command_id == NULL || strcmp(record.command_id, command_id) != 0 ||
       strcmp(record.scope, identity->scope) != 0 ||
       strcmp(record.command_type, identity->command_type) != 0 ||
       strcmp(record.idempotency_key, identity->idempotency_key) != 0 ||
       (request != NULL &&
        (strcmp(record.request_digest, request->request_digest) != 0 ||
         !lc_outbox_nullable_string_equal(record.operation_id,
                                          request->operation_id))))) {
    rc = lc_error_set(
        error, LC_ERR_SERVER, 0L,
        "command immutable fields conflict with an existing receipt", NULL,
        NULL, NULL);
  }
  if (rc == LC_OK)
    rc = lc_outbox_command_receipt_from_record(&record, receipt, error);
  runtime->cleanup(runtime, &lc_outbox_command_record_map, &record);
  lc_get_res_cleanup(&load_result);
  if (rc == LC_OK && request != NULL)
    receipt->duplicate = 1;
  return rc;
}

/* A create-only acquire cannot distinguish an active lease from a committed
 * duplicate: both are expected rejections. Probe with an ordinary lease and
 * retain it through validation, so a duplicate is reported only from a
 * stable, currently unleased committed record. */
static int lc_outbox_probe_duplicate_barrier(lc_outbox_handle *outbox,
                                             const char *key, int acquire_rc,
                                             lc_lease **out, lc_error *error) {
  lc_acquire_req acquire;
  lc_error acquire_error;
  lc_lease *lease;
  int rc;

  if (outbox == NULL || key == NULL || out == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox duplicate probe requires outbox, key, and "
                        "lease output",
                        NULL, NULL, NULL);
  *out = NULL;
  /* Only a create-only conflict may have a durable record to inspect.  In
   * particular, do not turn a transient acquire failure into a second acquire
   * and an invented immutable-record conflict. */
  if ((outbox->client->is_pouch &&
       (acquire_rc != LC_ERR_INVALID || error == NULL ||
        error->message == NULL ||
        strcmp(error->message,
               "pouch acquire if_not_exists precondition failed") != 0)) ||
      (!outbox->client->is_pouch &&
       (acquire_rc != LC_ERR_SERVER || error == NULL ||
        error->http_status != 409L))) {
    return acquire_rc;
  }
  lc_error_init(&acquire_error);
  if (error != NULL) {
    acquire_error = *error;
    lc_error_init(error);
  }
  lc_acquire_req_init(&acquire);
  acquire.ns = outbox->ns;
  acquire.key = key;
  acquire.owner = outbox->owner;
  acquire.ttl_seconds = outbox->transaction_ttl_seconds;
  lease = NULL;
  rc = lc_acquire(&outbox->client->pub, &acquire, &lease, error);
  if (rc == LC_OK) {
    lc_error_cleanup(&acquire_error);
    *out = lease;
    return LC_OK;
  }
  if (error != NULL) {
    lc_error_cleanup(error);
    *error = acquire_error;
    lc_error_init(&acquire_error);
  }
  lc_error_cleanup(&acquire_error);
  return acquire_rc;
}

static int
lc_outbox_transaction_set_command(lc_outbox_transaction_handle *transaction,
                                  lc_lease *lease, const char *command_id,
                                  lc_error *error) {
  char *cause;
  char *retained_command_id;

  if (transaction->command_lease != NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox transaction already owns a command receipt",
                        NULL, NULL, NULL);
  }
  if (transaction->causation_id == NULL) {
    cause = lc_client_strdup(transaction->outbox->client, command_id);
    if (cause == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to retain command causation identity", NULL,
                          NULL, NULL);
    }
    transaction->causation_id = cause;
    transaction->command_causation_owned = 1;
  }
  retained_command_id =
      lc_client_strdup(transaction->outbox->client, command_id);
  if (retained_command_id == NULL) {
    if (transaction->command_causation_owned) {
      lc_client_free(transaction->outbox->client, transaction->causation_id);
      transaction->causation_id = NULL;
      transaction->command_causation_owned = 0;
    }
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to retain command identity", NULL, NULL, NULL);
  }
  transaction->command_id = retained_command_id;
  transaction->command_lease = lease;
  return LC_OK;
}

static void
lc_outbox_transaction_clear_command(lc_outbox_transaction_handle *transaction,
                                    lc_lease *lease) {
  if (transaction == NULL || transaction->command_lease != lease)
    return;
  transaction->command_lease = NULL;
  transaction->command_terminal = 0;
  lc_client_free(transaction->outbox->client, transaction->command_id);
  transaction->command_id = NULL;
  if (transaction->command_causation_owned) {
    lc_client_free(transaction->outbox->client, transaction->causation_id);
    transaction->causation_id = NULL;
  }
  transaction->command_causation_owned = 0;
}

static int
lc_outbox_stage_command_terminal(lc_outbox_transaction_handle *transaction,
                                 const lc_command_result *result, int failed,
                                 lc_error *error) {
  lc_outbox_command_record record;
  lc_outbox_command_record updated;
  lc_get_res load_result;
  lc_attach_req attach;
  lc_attach_res attach_result;
  lonejson *runtime;
  time_t now;
  int rc;

  if (transaction == NULL || transaction->terminal ||
      transaction->terminal_vote_started ||
      transaction->command_lease == NULL || transaction->command_terminal ||
      result == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "an open transaction with a pending command is required", NULL, NULL,
        NULL);
  }
  if (failed) {
    rc = lc_outbox_validate_failure_message(result->failure_message, error);
    if (rc != LC_OK)
      return rc;
  }
  if ((!failed &&
       (result->result_code == NULL || result->result_code[0] == '\0' ||
        result->failure_code != NULL || result->failure_message != NULL ||
        (result->body != NULL &&
         (result->content_type == NULL || result->content_type[0] == '\0')))) ||
      (failed &&
       (result->failure_code == NULL || result->failure_code[0] == '\0' ||
        result->result_code != NULL || result->result_reference != NULL ||
        result->content_type != NULL || result->body != NULL))) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "command terminal result contains contradictory fields",
                        NULL, NULL, NULL);
  }
  memset(&record, 0, sizeof(record));
  memset(&load_result, 0, sizeof(load_result));
  rc = lc_outbox_require_json_runtime(transaction->outbox->client, &runtime,
                                      error);
  if (rc != LC_OK)
    return rc;
  rc = lc_lease_load(transaction->command_lease, &lc_outbox_command_record_map,
                     &record, NULL, &load_result, error);
#ifdef LOCKDC_TEST_BUILD
  if (rc == LC_OK && lc_outbox_test_after_command_terminal_load_hook != NULL) {
    rc = lc_outbox_test_after_command_terminal_load_hook(
        lc_outbox_test_after_command_terminal_load_context, error);
  }
#endif
  if (rc != LC_OK) {
    runtime->cleanup(runtime, &lc_outbox_command_record_map, &record);
    lc_get_res_cleanup(&load_result);
    return rc;
  }
  if (record.state == NULL || strcmp(record.state, "pending") != 0) {
    runtime->cleanup(runtime, &lc_outbox_command_record_map, &record);
    lc_get_res_cleanup(&load_result);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "command receipt is already terminal", NULL, NULL,
                        NULL);
  }
  rc = lc_outbox_validate_command_terminal_metadata(
      transaction->outbox->client, &record, result, failed, error);
  if (rc != LC_OK) {
    runtime->cleanup(runtime, &lc_outbox_command_record_map, &record);
    lc_get_res_cleanup(&load_result);
    return rc;
  }
  now = time(NULL);
  if (now == (time_t)-1) {
    runtime->cleanup(runtime, &lc_outbox_command_record_map, &record);
    lc_get_res_cleanup(&load_result);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "failed to read command terminal clock", NULL, NULL,
                        NULL);
  }
  updated = record;
  updated.state = failed ? "failed" : "completed";
  if (failed) {
    updated.failure_code = (char *)result->failure_code;
    updated.failure_message = (char *)result->failure_message;
    updated.failed_at_unix = (lonejson_int64)now;
  } else {
    updated.result_code = (char *)result->result_code;
    updated.result_reference = (char *)result->result_reference;
    updated.result_content_type = (char *)result->content_type;
    updated.completed_at_unix = (lonejson_int64)now;
    updated.has_result_body = result->body != NULL;
  }
  rc = lc_lease_save(transaction->command_lease, &lc_outbox_command_record_map,
                     &updated, error);
  if (rc == LC_OK && result->body != NULL) {
    lc_attach_req_init(&attach);
    attach.name = "result";
    attach.content_type = result->content_type;
    attach.prevent_overwrite = 1;
    memset(&attach_result, 0, sizeof(attach_result));
    rc = lc_lease_attach(transaction->command_lease, &attach, result->body,
                         &attach_result, error);
    lc_attach_res_cleanup(&attach_result);
    if (rc != LC_OK) {
      /* The receipt was staged before its required attachment. An attachment
       * failure must decide rollback for the entire implicit-XA transaction so
       * that no caller can later publish an incomplete terminal receipt. */
      lc_outbox_transaction_abort_enrolled_lease(transaction,
                                                 transaction->command_lease);
    }
  }
  runtime->cleanup(runtime, &lc_outbox_command_record_map, &record);
  lc_get_res_cleanup(&load_result);
  if (rc == LC_OK)
    transaction->command_terminal = 1;
  return rc;
}

static int lc_outbox_stage_outbox(lc_lease *lease, const lc_outbox_entry *entry,
                                  const char *command_id, lc_source *payload,
                                  lc_error *error) {
  lc_outbox_record record;
  lc_attach_req attach;
  lc_attach_res attach_result;
  char message_digest[44];
  char message_id[48];
  int rc;

  if (payload == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox payload source is required", NULL, NULL, NULL);
  }
  rc = lc_outbox_validate_outbox_envelope(entry, error);
  if (rc != LC_OK)
    return rc;
  memset(&record, 0, sizeof(record));
  record.record_type = "lockdc.outbox.v1";
  record.operation_id = (char *)entry->operation_id;
  record.effect_id = (char *)entry->effect_id;
  record.effect_key = (char *)entry->effect_key;
  record.payload_digest = (char *)entry->payload_digest;
  rc = lc_outbox_digest(lease->key, message_digest, error);
  if (rc != LC_OK)
    return rc;
  snprintf(message_id, sizeof(message_id), "msg_%s", message_digest);
  record.message_id = message_id;
  record.command_id = (char *)command_id;
  record.causation_id = (char *)entry->causation_id;
  record.kind = (char *)entry->kind;
  record.schema_version = (char *)entry->schema_version;
  record.destination = (char *)entry->destination;
  record.content_type =
      (char *)(entry->content_type != NULL ? entry->content_type
                                           : "application/octet-stream");
  record.headers_json = (char *)entry->headers_json;
  record.trace_context = (char *)entry->trace_context;
  record.dispatch_state = "pending";
  record.attempt_count = 0;
  record.not_before_unix = 0;
  record.replay_count = 0;
  record.dead_lettered_at_unix = 0;
  record.replayed_at_unix = 0;
  rc = lc_lease_save(lease, &lc_outbox_record_map, &record, error);
  if (rc != LC_OK) {
    return rc;
  }
  lc_attach_req_init(&attach);
  attach.name = "payload";
  attach.content_type = entry->content_type != NULL
                            ? entry->content_type
                            : "application/octet-stream";
  attach.prevent_overwrite = 1;
  memset(&attach_result, 0, sizeof(attach_result));
  rc = lc_lease_attach(lease, &attach, payload, &attach_result, error);
  lc_attach_res_cleanup(&attach_result);
  return rc;
}

static void lc_outbox_record_clear(lc_client_handle *client,
                                   lc_outbox_record *record) {
  if (record == NULL)
    return;
  lc_client_free(client, record->record_type);
  lc_client_free(client, record->operation_id);
  lc_client_free(client, record->effect_id);
  lc_client_free(client, record->effect_key);
  lc_client_free(client, record->payload_digest);
  lc_client_free(client, record->message_id);
  lc_client_free(client, record->command_id);
  lc_client_free(client, record->causation_id);
  lc_client_free(client, record->kind);
  lc_client_free(client, record->schema_version);
  lc_client_free(client, record->destination);
  lc_client_free(client, record->content_type);
  lc_client_free(client, record->headers_json);
  lc_client_free(client, record->trace_context);
  lc_client_free(client, record->delivery_reference);
  lc_client_free(client, record->response_digest);
  lc_client_free(client, record->dispatch_state);
  lc_client_free(client, record->last_error);
  lc_client_free(client, record->prior_dead_letter_error);
  memset(record, 0, sizeof(*record));
}

static void lc_outbox_record_loaded_clear(lc_client_handle *client,
                                          lc_outbox_record *record) {
  lonejson *runtime;

  if (record == NULL)
    return;
  runtime = lc_outbox_json_runtime(client);
  lc_lonejson_cleanup_value(runtime, &lc_outbox_record_map, record);
}

static int lc_outbox_publish_claim_deadline(lc_outbox_job_handle *job,
                                            lc_unix_seconds expires_at_unix,
                                            lc_error *error) {
  lc_outbox_claim_deadline_record record;
  lc_acquire_req acquire;
  lc_release_req release;
  lc_lease *lease;
  char *key;
  int rc;

  if (job == NULL || job->outbox == NULL || job->client == NULL ||
      job->outbox_key == NULL || expires_at_unix <= 0) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "active outbox job is required to publish claim renewal", NULL, NULL,
        NULL);
  }
  key = NULL;
  lease = NULL;
  rc = lc_outbox_claim_deadline_key(job->outbox_key, &key, error);
  if (rc != LC_OK)
    return rc;
  lc_acquire_req_init(&acquire);
  acquire.ns = job->outbox->ns;
  acquire.key = key;
  acquire.owner = job->outbox->owner;
  acquire.ttl_seconds = job->outbox->transaction_ttl_seconds;
  rc = lc_acquire(&job->client->pub, &acquire, &lease, error);
  if (rc == LC_OK) {
    memset(&record, 0, sizeof(record));
    record.record_type = "lockdc.outbox-claim-deadline.v1";
    record.attempt_count = job->record.attempt_count;
    record.replay_count = job->record.replay_count;
    record.claim_expires_at_unix = (lonejson_int64)expires_at_unix;
    rc = lc_lease_save(lease, &lc_outbox_claim_deadline_record_map, &record,
                       error);
  }
  if (rc == LC_OK) {
    lc_release_req_init(&release);
    rc = lc_lease_release(lease, &release, error);
    if (rc == LC_OK)
      lease = NULL;
  }
  if (lease != NULL)
    lc_outbox_rollback_lease(lease);
  free(key);
  return rc;
}

static int lc_outbox_claim_deadline_matches(
    const lc_outbox_claim_deadline_record *deadline,
    const lc_outbox_record *outbox) {
  return deadline != NULL && outbox != NULL &&
         deadline->attempt_count == outbox->attempt_count &&
         deadline->replay_count == outbox->replay_count;
}

static lc_unix_seconds
lc_outbox_select_claim_deadline(const lc_outbox_claim_deadline_record *deadline,
                                const lc_outbox_record *outbox,
                                lc_unix_seconds fallback) {
  if (lc_outbox_claim_deadline_matches(deadline, outbox) &&
      deadline->claim_expires_at_unix > 0) {
    return (lc_unix_seconds)deadline->claim_expires_at_unix;
  }
  return fallback;
}

static int lc_outbox_conflicted_recovery_deadline(
    lc_unix_seconds now, lc_unix_seconds durable_deadline,
    long retry_delay_seconds, lc_unix_seconds *out, lc_error *error) {
  if (out == NULL || retry_delay_seconds < 1L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "conflicted claim recovery requires output and retry "
                        "delay",
                        NULL, NULL, NULL);
  }
  if (durable_deadline > now) {
    *out = durable_deadline;
    return LC_OK;
  }
  return lc_outbox_timestamp_add(now, retry_delay_seconds,
                                 "conflicted claim retry delay", out, error);
}

#ifdef LOCKDC_TEST_BUILD
int lc_outbox_test_claim_deadline_matches(lc_i64 deadline_attempt,
                                          lc_i64 deadline_replay,
                                          lc_i64 outbox_attempt,
                                          lc_i64 outbox_replay) {
  lc_outbox_claim_deadline_record deadline;
  lc_outbox_record outbox;

  memset(&deadline, 0, sizeof(deadline));
  memset(&outbox, 0, sizeof(outbox));
  deadline.attempt_count = (lonejson_int64)deadline_attempt;
  deadline.replay_count = (lonejson_int64)deadline_replay;
  outbox.attempt_count = (lonejson_int64)outbox_attempt;
  outbox.replay_count = (lonejson_int64)outbox_replay;
  return lc_outbox_claim_deadline_matches(&deadline, &outbox);
}

lc_i64 lc_outbox_test_select_claim_deadline(lc_i64 deadline_expires_at_unix,
                                            lc_i64 deadline_attempt,
                                            lc_i64 deadline_replay,
                                            lc_i64 outbox_attempt,
                                            lc_i64 outbox_replay,
                                            lc_i64 fallback) {
  lc_outbox_claim_deadline_record deadline;
  lc_outbox_record outbox;

  memset(&deadline, 0, sizeof(deadline));
  memset(&outbox, 0, sizeof(outbox));
  deadline.claim_expires_at_unix = (lonejson_int64)deadline_expires_at_unix;
  deadline.attempt_count = (lonejson_int64)deadline_attempt;
  deadline.replay_count = (lonejson_int64)deadline_replay;
  outbox.attempt_count = (lonejson_int64)outbox_attempt;
  outbox.replay_count = (lonejson_int64)outbox_replay;
  return (lc_i64)lc_outbox_select_claim_deadline(&deadline, &outbox,
                                                 (lc_unix_seconds)fallback);
}

int lc_outbox_test_conflicted_recovery_deadline(lc_i64 now,
                                                lc_i64 durable_deadline,
                                                long retry_delay_seconds,
                                                lc_i64 *out_deadline) {
  lc_error error;
  lc_unix_seconds deadline = 0;
  int rc;

  if (out_deadline == NULL)
    return LC_ERR_INVALID;
  lc_error_init(&error);
  rc = lc_outbox_conflicted_recovery_deadline(
      (lc_unix_seconds)now, (lc_unix_seconds)durable_deadline,
      retry_delay_seconds, &deadline, &error);
  if (rc == LC_OK)
    *out_deadline = (lc_i64)deadline;
  lc_error_cleanup(&error);
  return rc;
}

#endif

static lc_unix_seconds lc_outbox_effective_claim_deadline(
    lc_outbox_handle *outbox, const char *outbox_key,
    const lc_outbox_record *record_outbox, lc_unix_seconds fallback) {
  lc_outbox_claim_deadline_record record;
  lc_get_opts options;
  lc_get_res result;
  lc_error error;
  lonejson *runtime;
  char *key;
  int rc;

  if (outbox == NULL || outbox_key == NULL || record_outbox == NULL ||
      record_outbox->dispatch_state == NULL ||
      strcmp(record_outbox->dispatch_state, "claimed") != 0 || fallback <= 0)
    return fallback;
  memset(&record, 0, sizeof(record));
  memset(&result, 0, sizeof(result));
  runtime = NULL;
  key = NULL;
  lc_error_init(&error);
  rc = lc_outbox_claim_deadline_key(outbox_key, &key, &error);
  if (rc != LC_OK)
    goto done;
  lc_get_opts_init(&options);
  options.public_read = 1;
  runtime = lc_outbox_json_runtime(outbox->dispatcher_client);
  rc = runtime == NULL
           ? LC_ERR_NOMEM
           : lc_load_in_namespace(&outbox->dispatcher_client->pub, outbox->ns,
                                  key, &lc_outbox_claim_deadline_record_map,
                                  &record, &options, &result, &error);
  if (rc == LC_OK && !result.no_content && record.record_type != NULL &&
      strcmp(record.record_type, "lockdc.outbox-claim-deadline.v1") == 0) {
    fallback =
        lc_outbox_select_claim_deadline(&record, record_outbox, fallback);
  }
  if (runtime != NULL) {
    runtime->cleanup(runtime, &lc_outbox_claim_deadline_record_map, &record);
  }
done:
  free(key);
  lc_get_res_cleanup(&result);
  lc_error_cleanup(&error);
  return fallback;
}

static int lc_outbox_record_copy(lc_client_handle *client,
                                 lc_outbox_record *dst,
                                 const lc_outbox_record *src, lc_error *error) {
  memset(dst, 0, sizeof(*dst));
  if ((src->record_type != NULL && (dst->record_type = lc_client_strdup(
                                        client, src->record_type)) == NULL) ||
      (src->operation_id != NULL && (dst->operation_id = lc_client_strdup(
                                         client, src->operation_id)) == NULL) ||
      (src->effect_id != NULL &&
       (dst->effect_id = lc_client_strdup(client, src->effect_id)) == NULL) ||
      (src->effect_key != NULL &&
       (dst->effect_key = lc_client_strdup(client, src->effect_key)) == NULL) ||
      (src->payload_digest != NULL &&
       (dst->payload_digest = lc_client_strdup(client, src->payload_digest)) ==
           NULL) ||
      (src->message_id != NULL &&
       (dst->message_id = lc_client_strdup(client, src->message_id)) == NULL) ||
      (src->command_id != NULL &&
       (dst->command_id = lc_client_strdup(client, src->command_id)) == NULL) ||
      (src->causation_id != NULL && (dst->causation_id = lc_client_strdup(
                                         client, src->causation_id)) == NULL) ||
      (src->kind != NULL &&
       (dst->kind = lc_client_strdup(client, src->kind)) == NULL) ||
      (src->schema_version != NULL &&
       (dst->schema_version = lc_client_strdup(client, src->schema_version)) ==
           NULL) ||
      (src->destination != NULL && (dst->destination = lc_client_strdup(
                                        client, src->destination)) == NULL) ||
      (src->content_type != NULL && (dst->content_type = lc_client_strdup(
                                         client, src->content_type)) == NULL) ||
      (src->headers_json != NULL && (dst->headers_json = lc_client_strdup(
                                         client, src->headers_json)) == NULL) ||
      (src->trace_context != NULL &&
       (dst->trace_context = lc_client_strdup(client, src->trace_context)) ==
           NULL) ||
      (src->delivery_reference != NULL &&
       (dst->delivery_reference =
            lc_client_strdup(client, src->delivery_reference)) == NULL) ||
      (src->response_digest != NULL &&
       (dst->response_digest =
            lc_client_strdup(client, src->response_digest)) == NULL) ||
      (src->dispatch_state != NULL &&
       (dst->dispatch_state = lc_client_strdup(client, src->dispatch_state)) ==
           NULL) ||
      (src->last_error != NULL &&
       (dst->last_error = lc_client_strdup(client, src->last_error)) == NULL) ||
      (src->prior_dead_letter_error != NULL &&
       (dst->prior_dead_letter_error =
            lc_client_strdup(client, src->prior_dead_letter_error)) == NULL)) {
    lc_outbox_record_clear(client, dst);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy outbox envelope", NULL, NULL, NULL);
  }
  dst->attempt_count = src->attempt_count;
  dst->claim_expires_at_unix = src->claim_expires_at_unix;
  dst->not_before_unix = src->not_before_unix;
  dst->replay_count = src->replay_count;
  dst->dead_lettered_at_unix = src->dead_lettered_at_unix;
  dst->replayed_at_unix = src->replayed_at_unix;
  dst->completed_at_unix = src->completed_at_unix;
  return LC_OK;
}

static void lc_outbox_job_refresh(lc_outbox_job_handle *job) {
  job->pub.outbox_key = job->outbox_key;
  job->pub.operation_id = job->record.operation_id;
  job->pub.effect_id = job->record.effect_id;
  job->pub.effect_key = job->record.effect_key;
  job->pub.message_id = job->record.message_id;
  job->pub.command_id = job->record.command_id;
  job->pub.causation_id = job->record.causation_id;
  job->pub.kind = job->record.kind;
  job->pub.schema_version = job->record.schema_version;
  job->pub.destination = job->record.destination;
  job->pub.content_type = job->record.content_type;
  job->pub.headers_json = job->record.headers_json;
  job->pub.trace_context = job->record.trace_context;
  job->pub.attempt = (int)job->record.attempt_count;
  job->pub.lease_expires_at_unix =
      job->lease != NULL ? job->lease->lease_expires_at_unix : 0;
}

static void lc_outbox_job_close_method(lc_outbox_job *self) {
  lc_outbox_job_handle *job = (lc_outbox_job_handle *)self;
  lc_client_handle *client;
  lc_outbox_handle *outbox;
  lc_outbox_dispatcher_handle *dispatcher;

  if (job == NULL)
    return;
  client = job->client;
  outbox = job->outbox;
  dispatcher = job->dispatcher;
  job->dispatcher = NULL;
  if (job->lease != NULL) {
    /* The committed claim record is intentionally left for expiry recovery.
     * Keep the local dispatcher awake at that boundary even when its ordinary
     * Pouch recovery interval is disabled. */
    lc_outbox_schedule_claim_recovery(
        outbox, job->outbox_key,
        (lc_unix_seconds)job->lease->lease_expires_at_unix);
    job->lease->close(job->lease);
  }
  lc_outbox_record_clear(client, &job->record);
  lc_client_free(client, job->outbox_key);
  lc_client_free(client, job);
  if (outbox != NULL)
    lc_outbox_release(outbox);
  if (dispatcher != NULL) {
    pthread_mutex_lock(&dispatcher->lifecycle_mutex);
    if (dispatcher->active_jobs > 0U)
      --dispatcher->active_jobs;
    pthread_cond_broadcast(&dispatcher->lifecycle_cond);
    pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
    lc_outbox_dispatcher_close_method(&dispatcher->pub);
  }
  lc_client_handle_release(client);
}

static int lc_outbox_job_write_payload_method(lc_outbox_job *self, lc_sink *dst,
                                              size_t *written,
                                              lc_error *error) {
  lc_outbox_job_handle *job = (lc_outbox_job_handle *)self;
  lc_attachment_get_req request;
  lc_attachment_get_res result;
  int rc;

  if (job == NULL || job->terminal || job->lease == NULL || dst == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox job and destination sink are required", NULL,
                        NULL, NULL);
  }
  memset(&request, 0, sizeof(request));
  memset(&result, 0, sizeof(result));
  request.selector.name = "payload";
  rc = lc_lease_get_attachment(job->lease, &request, dst, &result, error);
  if (rc == LC_OK && written != NULL)
    *written = (size_t)result.attachment.size;
  if (rc != LC_OK) {
    pthread_mutex_lock(&job->outbox->notification_mutex);
    ++job->outbox->payload_open_failures;
    pthread_mutex_unlock(&job->outbox->notification_mutex);
    lc_outbox_record_error(job->outbox, error);
  }
  lc_attachment_get_res_cleanup(&result);
  return rc;
}

static int lc_outbox_renew_claim_lease(lc_lease *lease, long ttl_seconds,
                                       lc_error *error) {
  lc_keepalive_req request;

  if (lease == NULL || ttl_seconds < 1L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "active claim and positive renewal ttl are required",
                        NULL, NULL, NULL);
  }
  lc_keepalive_req_init(&request);
  request.ttl_seconds = ttl_seconds;
  /* The service owns lease time.  Once keepalive succeeds, its returned
   * expiry is the only authoritative recovery deadline; a client-clock
   * estimate cannot safely pre- or post-validate it. */
  return lc_lease_keepalive(lease, &request, error);
}

#ifdef LOCKDC_TEST_BUILD
int lc_outbox_test_renew_claim_lease(lc_lease *lease, long ttl_seconds,
                                     lc_error *error) {
  return lc_outbox_renew_claim_lease(lease, ttl_seconds, error);
}
#endif

static int lc_outbox_job_renew_method(lc_outbox_job *self, long ttl_seconds,
                                      lc_error *error) {
  lc_outbox_job_handle *job = (lc_outbox_job_handle *)self;
  int rc;

  if (job == NULL || job->terminal || job->lease == NULL || ttl_seconds < 1L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "active outbox job and positive claim ttl are required",
                        NULL, NULL, NULL);
  }
#ifdef LOCKDC_TEST_BUILD
  if (lc_outbox_test_before_outbox_renew_keepalive_hook != NULL) {
    rc = lc_outbox_test_before_outbox_renew_keepalive_hook(
        lc_outbox_test_before_outbox_renew_keepalive_context, error);
    if (rc != LC_OK)
      return rc;
  }
#endif
  rc = lc_outbox_renew_claim_lease(job->lease, ttl_seconds, error);
  if (rc == LC_OK) {
    lc_outbox_job_refresh(job);
#ifdef LOCKDC_TEST_BUILD
    if (lc_outbox_test_before_outbox_renew_deadline_publish_hook != NULL) {
      rc = lc_outbox_test_before_outbox_renew_deadline_publish_hook(
          lc_outbox_test_before_outbox_renew_deadline_publish_context, error);
    }
#endif
    if (rc == LC_OK) {
      rc = lc_outbox_publish_claim_deadline(
          job, (lc_unix_seconds)job->lease->lease_expires_at_unix, error);
    }
    /* Keep local recovery aligned with the successful server mutation even
     * when persisting the companion deadline fails. */
    lc_outbox_refresh_claim_recovery(
        job->outbox, job->outbox_key,
        (lc_unix_seconds)job->lease->lease_expires_at_unix);
  }
  return rc;
}

static int lc_outbox_job_terminal(lc_outbox_job *self, const char *state,
                                  lc_unix_seconds not_before_unix,
                                  const char *diagnostic,
                                  const lc_outbox_completion *completion,
                                  lc_error *error) {
  lc_outbox_job_handle *job = (lc_outbox_job_handle *)self;
  lc_outbox_record record;
  lc_release_req release;
  int rc;

  if (job == NULL || job->terminal || job->lease == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, "outbox job is closed", NULL,
                        NULL, NULL);
  }
  if (job->staged_terminal_state != NULL) {
    if (strcmp(job->staged_terminal_state, state) != 0) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "a different outbox terminal transition is already "
                          "staged; retry the original transition",
                          NULL, NULL, NULL);
    }
  } else {
    rc = lc_outbox_validate_diagnostic(diagnostic, error);
    if (rc != LC_OK)
      return rc;
    record = job->record;
    record.dispatch_state = (char *)state;
    record.claim_expires_at_unix = 0;
    record.not_before_unix = not_before_unix;
    record.last_error = (char *)diagnostic;
    if (strcmp(state, "completed") == 0) {
      time_t now = time(NULL);

      rc = lc_outbox_validate_completion_evidence(completion, error);
      if (rc != LC_OK)
        return rc;
      if (now == (time_t)-1) {
        return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                            "failed to read outbox completion clock", NULL,
                            NULL, NULL);
      }
      record.delivery_reference =
          (char *)(completion == NULL ? NULL : completion->delivery_reference);
      record.response_digest =
          (char *)(completion == NULL ? NULL : completion->response_digest);
      record.completed_at_unix = (lonejson_int64)now;
    }
    if (strcmp(state, "dead_letter") == 0) {
      time_t now = time(NULL);

      if (now == (time_t)-1) {
        return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                            "failed to read outbox dead-letter clock", NULL,
                            NULL, NULL);
      }
      record.dead_lettered_at_unix = (lonejson_int64)now;
    }
    rc = lc_lease_save(job->lease, &lc_outbox_record_map, &record, error);
    if (rc != LC_OK)
      return rc;
    job->staged_terminal_state = state;
    job->staged_terminal_not_before_unix = not_before_unix;
  }
  lc_release_req_init(&release);
  rc = lc_lease_release(job->lease, &release, error);
  if (rc != LC_OK)
    return rc;
  job->lease = NULL;
  job->terminal = 1;
  lc_outbox_job_refresh(job);
  lc_outbox_cancel_delayed_notification(job->outbox, job->outbox_key);
  if (strcmp(state, "retry_wait") == 0) {
    lc_outbox_schedule_retry(job->outbox, job->outbox_key,
                             job->staged_terminal_not_before_unix);
  }
  /* A successful terminal transition transfers no remaining ownership to the
   * caller.  Release the local job and its retained outbox/client references
   * now; the public contract deliberately makes a terminal method consuming. */
  lc_outbox_job_close_method(&job->pub);
  return LC_OK;
}

static int lc_outbox_job_complete_method(lc_outbox_job *self,
                                         const lc_outbox_completion *completion,
                                         lc_error *error) {
  return lc_outbox_job_terminal(self, "completed", 0L, NULL, completion, error);
}

static long lc_outbox_job_auto_retry_delay(const lc_outbox_job_handle *job,
                                           long timestamp_maximum) {
  long cap = job->outbox->retry_initial_delay_seconds;
  long maximum = job->outbox->retry_max_delay_seconds;
  long attempt = job->record.attempt_count;
  unsigned long random_value = 0UL;
  unsigned char random_bytes[sizeof(random_value)];

  if (maximum > timestamp_maximum)
    maximum = timestamp_maximum;
  if (cap > maximum)
    cap = maximum;
  while (attempt > 1L && cap < maximum) {
    if (cap > maximum / 2L) {
      cap = maximum;
    } else {
      cap *= 2L;
    }
    --attempt;
  }
  if (RAND_bytes(random_bytes, (int)sizeof(random_bytes)) == 1) {
    size_t index;
    for (index = 0U; index < sizeof(random_bytes); ++index) {
      random_value = (random_value << 8U) | random_bytes[index];
    }
  } else {
    random_value = (unsigned long)time(NULL) ^ (unsigned long)(uintptr_t)job;
  }
  if (cap == 0L)
    return 0L;
  /* LONG_MAX + 1 is not a valid signed long. Keep the full range available
   * without deriving a zero divisor through an unsigned-width assumption. */
  if (cap == LONG_MAX)
    return (long)(random_value & (unsigned long)LONG_MAX);
  return (long)(random_value % ((unsigned long)cap + 1UL));
}

static int lc_outbox_job_retry_method(lc_outbox_job *self,
                                      const lc_outbox_retry *request,
                                      lc_error *error) {
  lc_outbox_job_handle *job = (lc_outbox_job_handle *)self;
  long delay;
  time_t now;
  lc_unix_seconds deadline;
  int rc;

  if (job == NULL || request == NULL || request->delay_seconds < 0L ||
      request->delay_seconds > job->outbox->host_retry_delay_max_seconds) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "retry delay exceeds the configured host maximum", NULL,
                        NULL, NULL);
  }
  now = time(NULL);
  if (now == (time_t)-1) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "failed to read outbox retry clock", NULL, NULL, NULL);
  }
  delay = request->delay_seconds == 0L
              ? lc_outbox_job_auto_retry_delay(
                    job, lc_outbox_timestamp_max_delay((lc_unix_seconds)now))
              : request->delay_seconds;
  rc = lc_outbox_timestamp_add((lc_unix_seconds)now, delay, "retry delay",
                               &deadline, error);
  if (rc != LC_OK)
    return rc;
  if (job->record.attempt_count >= job->pub.max_attempts) {
    return lc_outbox_job_terminal(self, "dead_letter", 0L, request->diagnostic,
                                  NULL, error);
  }
  return lc_outbox_job_terminal(self, "retry_wait", deadline,
                                request->diagnostic, NULL, error);
}

static int lc_outbox_job_dead_letter_method(lc_outbox_job *self,
                                            const char *diagnostic,
                                            lc_error *error) {
  return lc_outbox_job_terminal(self, "dead_letter", 0L, diagnostic, NULL,
                                error);
}

/* A successful release consumes the lease. If the endpoint cannot accept the
 * rollback, this internal path still owns the local handle and must close it.
 */
static void lc_outbox_rollback_lease(lc_lease *lease) {
  lc_release_req release;

  if (lease == NULL)
    return;
  lc_release_req_init(&release);
  release.rollback = 1;
  if (lc_lease_release(lease, &release, NULL) != LC_OK)
    lease->close(lease);
}

/* Shared Pouch dispatchers can see the same recovery key at once. Once a
 * claim has been committed, read it before taking a lease so an observer does
 * not transiently acquire and roll back a claim that another dispatcher is
 * handing to its host. This is a shared-writer contention optimization only;
 * the lease path below remains the correctness authority. */
static int lc_outbox_pouch_shared_live_claim(lc_outbox_handle *outbox,
                                             const char *key) {
  lc_client_handle *client;
  lc_outbox_record record;
  lc_get_opts options;
  lc_get_res result;
  lonejson *runtime;
  lc_error load_error;
  time_t now;
  int rc;
  int active;

  client = outbox->dispatcher_client;
  if (!client->is_pouch || lc_pouch_single_writer_enabled(client->pouch))
    return 0;
  memset(&record, 0, sizeof(record));
  memset(&result, 0, sizeof(result));
  lc_get_opts_init(&options);
  options.public_read = 1;
  lc_error_init(&load_error);
  runtime = lc_outbox_json_runtime(client);
  if (runtime == NULL) {
    lc_error_cleanup(&load_error);
    return 0;
  }
  rc =
      lc_load_in_namespace(&client->pub, outbox->ns, key, &lc_outbox_record_map,
                           &record, &options, &result, &load_error);
  now = time(NULL);
  if (rc == LC_OK && !result.no_content && record.record_type != NULL &&
      strcmp(record.record_type, "lockdc.outbox.v1") == 0 &&
      record.dispatch_state != NULL &&
      strcmp(record.dispatch_state, "claimed") == 0) {
    record.claim_expires_at_unix =
        (lonejson_int64)lc_outbox_effective_claim_deadline(
            outbox, key, &record,
            (lc_unix_seconds)record.claim_expires_at_unix);
  }
  active = rc == LC_OK && now != (time_t)-1 && !result.no_content &&
           record.record_type != NULL &&
           strcmp(record.record_type, "lockdc.outbox.v1") == 0 &&
           record.dispatch_state != NULL &&
           strcmp(record.dispatch_state, "claimed") == 0 &&
           record.claim_expires_at_unix > (lonejson_int64)now;
  if (active) {
    lc_outbox_schedule_claim_recovery(
        outbox, key, (lc_unix_seconds)record.claim_expires_at_unix);
  }
  runtime->cleanup(runtime, &lc_outbox_record_map, &record);
  lc_get_res_cleanup(&result);
  lc_error_cleanup(&load_error);
  return active;
}

/* Remote acquire conflicts do not return the incumbent claim deadline. Read
 * the durable envelope only for that conflict so recovery remains tied to the
 * actual lease expiry. If that read cannot establish an active durable claim,
 * use the ordinary bounded retry delay instead of inventing a full new TTL. */
static void lc_outbox_schedule_conflicted_claim_recovery(
    lc_outbox_handle *outbox, const char *key, const lc_error *conflict) {
  lc_outbox_record record;
  lc_get_opts options;
  lc_get_res result;
  lonejson *runtime;
  lc_error load_error;
  lc_unix_seconds durable_deadline = 0;
  lc_unix_seconds retry_at_unix = 0;
  time_t now;
  int rc;

  if (conflict == NULL || conflict->http_status != 409L)
    return;
  memset(&record, 0, sizeof(record));
  memset(&result, 0, sizeof(result));
  lc_get_opts_init(&options);
  options.public_read = 1;
  lc_error_init(&load_error);
  runtime = lc_outbox_json_runtime(outbox->dispatcher_client);
  rc = runtime == NULL
           ? LC_ERR_NOMEM
           : lc_load_in_namespace(&outbox->dispatcher_client->pub, outbox->ns,
                                  key, &lc_outbox_record_map, &record, &options,
                                  &result, &load_error);
  if (rc == LC_OK && !result.no_content && record.record_type != NULL &&
      record.dispatch_state != NULL &&
      strcmp(record.record_type, "lockdc.outbox.v1") == 0 &&
      strcmp(record.dispatch_state, "claimed") == 0 &&
      record.claim_expires_at_unix > 0) {
    record.claim_expires_at_unix =
        (lonejson_int64)lc_outbox_effective_claim_deadline(
            outbox, key, &record,
            (lc_unix_seconds)record.claim_expires_at_unix);
    durable_deadline = (lc_unix_seconds)record.claim_expires_at_unix;
  }
  if (runtime != NULL)
    runtime->cleanup(runtime, &lc_outbox_record_map, &record);
  lc_get_res_cleanup(&result);
  lc_error_cleanup(&load_error);
  now = time(NULL);
  if (now != (time_t)-1) {
    lc_error retry_error;
    lc_error_init(&retry_error);
    rc = lc_outbox_conflicted_recovery_deadline(
        (lc_unix_seconds)now, durable_deadline,
        outbox->retry_initial_delay_seconds, &retry_at_unix, &retry_error);
    if (rc == LC_OK) {
      lc_outbox_schedule_claim_recovery(outbox, key, retry_at_unix);
    } else {
      lc_outbox_request_recovery(outbox, 0);
    }
    lc_error_cleanup(&retry_error);
  } else {
    lc_outbox_request_recovery(outbox, 0);
  }
}

/* A preflight read eliminates the normal shared-writer hand-off race. Keep a
 * small bounded backoff for the remaining read/commit race without using the
 * Pouch acquire API's one-second polling contract. */
static int lc_outbox_reacquire_durable_claim(lc_client_handle *client,
                                             const lc_acquire_req *acquire,
                                             lc_lease **lease,
                                             lc_error *error) {
  enum { LC_OUTBOX_SHARED_HANDOFF_RETRIES = 20 };
  int shared_pouch =
      client->is_pouch && !lc_pouch_single_writer_enabled(client->pouch);
  unsigned int attempt;
  int rc;

  for (attempt = 0U;; ++attempt) {
    rc = lc_acquire(&client->pub, acquire, lease, error);
    if (rc == LC_OK || !shared_pouch || rc != LC_ERR_INVALID ||
        attempt == LC_OUTBOX_SHARED_HANDOFF_RETRIES) {
      return rc;
    }
    lc_error_cleanup(error);
    lc_error_init(error);
    {
      struct timespec delay;

      delay.tv_sec = 0;
      delay.tv_nsec = 5L * 1000L * 1000L;
      (void)nanosleep(&delay, NULL);
    }
  }
}

static int lc_outbox_claim_outbox(lc_outbox_handle *outbox, const char *key,
                                  lc_outbox_job **out, lc_error *error) {
  lc_client_handle *client = outbox->dispatcher_client;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_outbox_record record;
  lc_outbox_record verified;
  lc_get_res result;
  lc_get_res verify_result;
  lonejson *runtime;
  lc_outbox_job_handle *job;
  lc_client_handle *job_client;
  lc_outbox_dispatcher_handle *dispatcher;
  time_t now;
  lc_unix_seconds claim_expires_at_unix = 0;
  char *original_dispatch_state;
  char *original_last_error;
  lonejson_int64 original_attempt_count;
  lonejson_int64 original_claim_expires_at_unix;
  int recovered_to_pending;
  int rc;

  *out = NULL;
  lc_acquire_req_init(&acquire);
  acquire.ns = outbox->ns;
  acquire.key = key;
  acquire.owner = outbox->owner;
  acquire.ttl_seconds = outbox->claim_ttl_seconds;
  lease = NULL;
  job_client = NULL;
  dispatcher = NULL;
  now = time(NULL);
  if (now == (time_t)-1) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "failed to read outbox claim clock", NULL, NULL, NULL);
  }
  rc = lc_outbox_timestamp_add((lc_unix_seconds)now, outbox->claim_ttl_seconds,
                               "claim ttl", &claim_expires_at_unix, error);
  if (rc != LC_OK)
    return rc;
#ifdef LOCKDC_TEST_BUILD
  if (lc_outbox_test_before_claim_outbox_hook != NULL) {
    rc = lc_outbox_test_before_claim_outbox_hook(
        lc_outbox_test_before_claim_outbox_context, error);
    if (rc != LC_OK) {
      /* The hook models an acquire result. Preserve the recovery scheduling
       * real acquire failures perform below so it exercises the dispatcher
       * failure path without inventing a second test-only scheduler rule. */
      lc_outbox_schedule_claim_recovery(outbox, key, claim_expires_at_unix);
      return rc;
    }
  }
#endif
  if (lc_outbox_pouch_shared_live_claim(outbox, key)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox candidate remains actively claimed", NULL, NULL,
                        NULL);
  }
  rc = lc_acquire(&client->pub, &acquire, &lease, error);
  if (rc != LC_OK) {
    lc_outbox_schedule_conflicted_claim_recovery(outbox, key, error);
    return rc;
  }
  memset(&record, 0, sizeof(record));
  memset(&verified, 0, sizeof(verified));
  memset(&result, 0, sizeof(result));
  memset(&verify_result, 0, sizeof(verify_result));
  rc = lc_outbox_require_json_runtime(client, &runtime, error);
  if (rc != LC_OK) {
    lc_outbox_rollback_lease(lease);
    return rc;
  }
  job = NULL;
  recovered_to_pending = 0;
  rc = lc_lease_load(lease, &lc_outbox_record_map, &record, NULL, &result,
                     error);
  if (rc == LC_OK && result.no_content) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "outbox candidate disappeared before claim", NULL, NULL,
                      NULL);
  }
  if (rc == LC_OK) {
    rc = lc_outbox_validate_durable_outbox_record(&record, error);
  }
  if (rc == LC_OK) {
    now = time(NULL);
    if (now == (time_t)-1 || record.record_type == NULL ||
        record.dispatch_state == NULL ||
        strcmp(record.record_type, "lockdc.outbox.v1") != 0 ||
        (strcmp(record.dispatch_state, "pending") != 0 &&
         strcmp(record.dispatch_state, "retry_wait") != 0 &&
         strcmp(record.dispatch_state, "claimed") != 0)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox candidate is not currently dispatchable", NULL,
                        NULL, NULL);
    } else if (record.attempt_count < 0 ||
               record.attempt_count >= (lonejson_int64)INT_MAX) {
      rc = lc_error_set(
          error, LC_ERR_INVALID, 0L,
          "outbox candidate attempt count is outside the supported "
          "range",
          NULL, NULL, NULL);
    } else if (strcmp(record.dispatch_state, "claimed") != 0 &&
               record.attempt_count >= outbox->max_attempts) {
      original_dispatch_state = record.dispatch_state;
      original_last_error = record.last_error;
      original_claim_expires_at_unix = record.claim_expires_at_unix;
      record.dispatch_state = "dead_letter";
      record.claim_expires_at_unix = 0;
      record.not_before_unix = 0;
      record.last_error = "delivery attempt budget exhausted before claim";
      record.dead_lettered_at_unix = (lonejson_int64)now;
      rc = lc_lease_save(lease, &lc_outbox_record_map, &record, error);
      if (rc == LC_OK) {
        lc_release_req release;

        lc_release_req_init(&release);
        rc = lc_lease_release(lease, &release, error);
        if (rc == LC_OK)
          lease = NULL;
      }
      record.dispatch_state = original_dispatch_state;
      record.last_error = original_last_error;
      record.claim_expires_at_unix = original_claim_expires_at_unix;
      if (rc == LC_OK) {
        rc = lc_error_set(
            error, LC_ERR_INVALID, 0L,
            "outbox candidate delivery attempt budget is exhausted", NULL, NULL,
            NULL);
      }
    } else if (strcmp(record.dispatch_state, "retry_wait") == 0 &&
               lc_outbox_retry_is_not_eligible(record.not_before_unix,
                                               (lc_unix_seconds)now)) {
      lc_outbox_schedule_retry(outbox, key,
                               (lc_unix_seconds)record.not_before_unix);
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox candidate retry is not yet eligible", NULL,
                        NULL, NULL);
    }
  }

  if (rc == LC_OK && strcmp(record.dispatch_state, "claimed") == 0) {
    record.claim_expires_at_unix =
        (lonejson_int64)lc_outbox_effective_claim_deadline(
            outbox, key, &record,
            (lc_unix_seconds)record.claim_expires_at_unix);
    if (record.claim_expires_at_unix > (lonejson_int64)now) {
      lc_outbox_schedule_claim_recovery(
          outbox, key, (lc_unix_seconds)record.claim_expires_at_unix);
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox candidate remains actively claimed", NULL, NULL,
                        NULL);
    } else {
      original_dispatch_state = record.dispatch_state;
      original_last_error = record.last_error;
      original_claim_expires_at_unix = record.claim_expires_at_unix;
      recovered_to_pending = record.attempt_count < outbox->max_attempts;
      record.dispatch_state = recovered_to_pending ? "pending" : "dead_letter";
      record.claim_expires_at_unix = 0;
      if (recovered_to_pending) {
        record.last_error = "claim expired before terminal outcome";
      } else {
        record.last_error = "delivery attempt budget exhausted by claim expiry";
        record.dead_lettered_at_unix = (lonejson_int64)now;
      }
      rc = lc_lease_save(lease, &lc_outbox_record_map, &record, error);
      if (rc == LC_OK) {
        lc_release_req release;
        lc_release_req_init(&release);
        rc = lc_lease_release(lease, &release, error);
        if (rc == LC_OK)
          lease = NULL;
      }
      record.dispatch_state = original_dispatch_state;
      record.last_error = original_last_error;
      record.claim_expires_at_unix = original_claim_expires_at_unix;
      if (rc == LC_OK && recovered_to_pending)
        lc_outbox_notify(outbox, key, 1);
      if (rc == LC_OK) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "outbox claim recovered after expiry", NULL, NULL,
                          NULL);
      }
    }
  }

  if (rc == LC_OK) {
    original_dispatch_state = record.dispatch_state;
    original_attempt_count = record.attempt_count;
    original_claim_expires_at_unix = record.claim_expires_at_unix;
    rc =
        lc_outbox_timestamp_add((lc_unix_seconds)now, outbox->claim_ttl_seconds,
                                "claim ttl", &claim_expires_at_unix, error);
    if (rc != LC_OK) {
      runtime->cleanup(runtime, &lc_outbox_record_map, &record);
      lc_get_res_cleanup(&result);
      lc_outbox_rollback_lease(lease);
      return rc;
    }
    record.dispatch_state = "claimed";
    ++record.attempt_count;
    record.claim_expires_at_unix = (lonejson_int64)claim_expires_at_unix;
    job = (lc_outbox_job_handle *)lc_client_calloc(client, 1U, sizeof(*job));
    if (job == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate outbox job", NULL, NULL, NULL);
    }
    if (rc == LC_OK)
      rc = lc_outbox_record_copy(client, &job->record, &record, error);
    if (rc == LC_OK) {
      job->outbox_key = lc_client_strdup(client, key);
      if (job->outbox_key == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to copy outbox job key", NULL, NULL, NULL);
      }
    }
    if (rc == LC_OK)
      rc = lc_lease_save(lease, &lc_outbox_record_map, &record, error);
    if (rc == LC_OK) {
      lc_release_req release;
      lc_release_req_init(&release);
      rc = lc_lease_release(lease, &release, error);
      if (rc == LC_OK)
        lease = NULL;
    }
    record.dispatch_state = original_dispatch_state;
    record.attempt_count = original_attempt_count;
    record.claim_expires_at_unix = original_claim_expires_at_unix;
  }
  runtime->cleanup(runtime, &lc_outbox_record_map, &record);
  lc_get_res_cleanup(&result);
  if (rc != LC_OK) {
    lc_outbox_rollback_lease(lease);
    if (job != NULL) {
      lc_outbox_record_clear(client, &job->record);
      lc_client_free(client, job->outbox_key);
      lc_client_free(client, job);
    }
    return rc;
  }

  /* The durable claim is now visible and has spent its attempt. A fresh lease
   * fences the host's terminal action; a crash in this hand-off recovers at
   * the durable claim deadline. */
#ifdef LOCKDC_TEST_BUILD
  if (lc_outbox_test_before_outbox_handoff_reacquire_hook != NULL) {
    rc = lc_outbox_test_before_outbox_handoff_reacquire_hook(
        lc_outbox_test_before_outbox_handoff_reacquire_context, error);
    if (rc != LC_OK) {
      lc_outbox_schedule_claim_recovery(outbox, key, claim_expires_at_unix);
      lc_outbox_record_clear(client, &job->record);
      lc_client_free(client, job->outbox_key);
      lc_client_free(client, job);
      return rc;
    }
  }
#endif
  /* A host-owned job must not share the dispatcher clone's cancellation hook:
   * close() cancels only in-flight dispatcher work, while a handed-off host
   * job remains usable through its normal terminal decision. */
  if (client->is_pouch) {
    lc_client_handle_retain(client);
    job_client = client;
  } else {
    lc_client *job_client_public = NULL;

    rc = lc_client_clone_remote_for_outbox(outbox->client,
                                           lc_outbox_host_job_timeout(outbox),
                                           &job_client_public, error);
    if (rc == LC_OK)
      job_client = (lc_client_handle *)job_client_public;
  }
  if (rc == LC_OK) {
    rc = lc_outbox_reacquire_durable_claim(job_client, &acquire, &lease, error);
  }
  if (rc != LC_OK) {
    lc_outbox_schedule_claim_recovery(outbox, key, claim_expires_at_unix);
    if (job_client != NULL)
      lc_client_handle_release(job_client);
    lc_outbox_record_clear(client, &job->record);
    lc_client_free(client, job->outbox_key);
    lc_client_free(client, job);
    return rc;
  }
  rc = lc_lease_load(lease, &lc_outbox_record_map, &verified, NULL,
                     &verify_result, error);
  if (rc == LC_OK && verify_result.no_content) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "durable outbox candidate disappeared before hand-off",
                      NULL, NULL, NULL);
  }
  if (rc == LC_OK &&
      (verified.record_type == NULL || verified.dispatch_state == NULL ||
       strcmp(verified.record_type, "lockdc.outbox.v1") != 0 ||
       strcmp(verified.dispatch_state, "claimed") != 0 ||
       verified.attempt_count != job->record.attempt_count ||
       verified.claim_expires_at_unix != job->record.claim_expires_at_unix)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "durable outbox claim changed before hand-off", NULL,
                      NULL, NULL);
  }
  runtime->cleanup(runtime, &lc_outbox_record_map, &verified);
  lc_get_res_cleanup(&verify_result);
  if (rc != LC_OK) {
    lc_outbox_rollback_lease(lease);
    lc_outbox_schedule_claim_recovery(outbox, key, claim_expires_at_unix);
    if (job_client != NULL)
      lc_client_handle_release(job_client);
    lc_outbox_record_clear(client, &job->record);
    lc_client_free(client, job->outbox_key);
    lc_client_free(client, job);
    return rc;
  }
  job->client = job_client;
  job->outbox = outbox;
  lc_outbox_retain(outbox);
  job->lease = lease;
  job->pub.write_payload = lc_outbox_job_write_payload_method;
  job->pub.renew = lc_outbox_job_renew_method;
  job->pub.complete = lc_outbox_job_complete_method;
  job->pub.retry = lc_outbox_job_retry_method;
  job->pub.dead_letter = lc_outbox_job_dead_letter_method;
  job->pub.close = lc_outbox_job_close_method;
  job->pub.max_attempts = outbox->max_attempts;
  lc_outbox_job_refresh(job);
  lc_outbox_schedule_claim_recovery(
      outbox, key, (lc_unix_seconds)lease->lease_expires_at_unix);
  dispatcher = outbox->dispatcher;
  if (dispatcher != NULL) {
    pthread_mutex_lock(&dispatcher->lifecycle_mutex);
    if (dispatcher->stopping) {
      pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
      lc_outbox_job_close_method(&job->pub);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "outbox dispatcher is stopping", NULL, NULL, NULL);
    }
    ++dispatcher->active_jobs;
    lc_outbox_dispatcher_retain_locked(dispatcher);
    job->dispatcher = dispatcher;
    pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
  }
  *out = &job->pub;
  return LC_OK;
}

typedef struct lc_outbox_recovery_capture {
  lc_outbox_handle *outbox;
  char key[129];
  size_t length;
  int filter_retry_wait;
  int overflowed;
} lc_outbox_recovery_capture;

static int lc_outbox_recovery_key_begin(void *context, lc_error *error) {
  lc_outbox_recovery_capture *capture = (lc_outbox_recovery_capture *)context;
  (void)error;
  capture->length = 0U;
  return 1;
}

static int lc_outbox_recovery_key_chunk(void *context, const char *bytes,
                                        size_t length, lc_error *error) {
  lc_outbox_recovery_capture *capture = (lc_outbox_recovery_capture *)context;
  if (length > sizeof(capture->key) - 1U - capture->length) {
    lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                 "outbox recovery received an oversized key", NULL, NULL, NULL);
    return 0;
  }
  memcpy(capture->key + capture->length, bytes, length);
  capture->length += length;
  return 1;
}

static int lc_outbox_recovery_key_end(void *context, lc_error *error) {
  lc_outbox_recovery_capture *capture = (lc_outbox_recovery_capture *)context;
  lc_outbox_record record;
  lc_get_opts options;
  lc_get_res result;
  lonejson *runtime;
  time_t now;
  int no_content;
  int replace_delayed;
  int rc;

  capture->key[capture->length] = '\0';
  if (!lc_outbox_is_outbox_key(capture->key))
    return 1;
  replace_delayed = 0;
  if (capture->filter_retry_wait) {
    /* A key index is intentionally a conservative candidate source. Inspect
     * the small outbox envelope before promoting it over a delayed retry or
     * claim wake-up; attachment payloads are not read on this path. */
    memset(&record, 0, sizeof(record));
    memset(&result, 0, sizeof(result));
    lc_get_opts_init(&options);
    options.public_read = 1;
    runtime = lc_outbox_json_runtime(capture->outbox->dispatcher_client);
    if (runtime == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "outbox recovery JSON runtime is unavailable", NULL,
                          NULL, NULL);
    }
    rc = lc_load_in_namespace(
        &capture->outbox->dispatcher_client->pub, capture->outbox->ns,
        capture->key, &lc_outbox_record_map, &record, &options, &result, error);
    no_content = result.no_content;
    if (rc == LC_OK && !result.no_content)
      rc = lc_outbox_validate_durable_outbox_record(&record, error);
    now = time(NULL);
    if (rc == LC_OK && !result.no_content &&
        strcmp(record.dispatch_state, "retry_wait") == 0 && now != (time_t)-1 &&
        lc_outbox_retry_is_not_eligible(record.not_before_unix,
                                        (lc_unix_seconds)now)) {
      lc_outbox_schedule_retry(capture->outbox, capture->key,
                               (lc_unix_seconds)record.not_before_unix);
      runtime->cleanup(runtime, &lc_outbox_record_map, &record);
      lc_get_res_cleanup(&result);
      return 1;
    }
    if (rc == LC_OK && !result.no_content &&
        (strcmp(record.dispatch_state, "pending") != 0 &&
         strcmp(record.dispatch_state, "retry_wait") != 0)) {
      runtime->cleanup(runtime, &lc_outbox_record_map, &record);
      lc_get_res_cleanup(&result);
      return 1;
    }
    runtime->cleanup(runtime, &lc_outbox_record_map, &record);
    lc_get_res_cleanup(&result);
    if (rc != LC_OK)
      return 0;
    if (no_content)
      return 1;
    replace_delayed = 1;
  }
  if (!lc_outbox_notify(capture->outbox, capture->key, replace_delayed)) {
    capture->overflowed = 1;
#ifdef LOCKDC_TEST_BUILD
    if (lc_outbox_test_after_recovery_overflow_hook != NULL) {
      lc_outbox_test_after_recovery_overflow_hook(
          lc_outbox_test_after_recovery_overflow_context);
    }
#endif
    return 1;
  }
  pthread_mutex_lock(&capture->outbox->notification_mutex);
  ++capture->outbox->recovered_claims;
  pthread_mutex_unlock(&capture->outbox->notification_mutex);
  return 1;
}

static int lc_outbox_reconcile_pending(lc_outbox_handle *outbox,
                                       lc_error *error) {
  static const char pending_selector[] =
      "{\"eq\":{\"field\":\"/dispatch_state\",\"value\":\"pending\"}}";
  static const char retry_selector[] =
      "{\"eq\":{\"field\":\"/dispatch_state\",\"value\":\"retry_wait\"}}";
  static const char claimed_selector[] =
      "{\"eq\":{\"field\":\"/dispatch_state\",\"value\":\"claimed\"}}";
  lc_query_req request;
  lc_query_key_handler handler;
  lc_query_res result;
  lc_index_flush_req flush_request;
  lc_index_flush_res flush_result;
  lc_outbox_recovery_capture capture;
  char *cursor;
  size_t available;
  int consume_claim_recovery;
  int use_index;
  int scan_mode;
  int rc;

  if (outbox->startup_dead_letter_replay_pending) {
    rc = lc_outbox_replay_dead_letters_on_startup(outbox, error);
    if (rc != LC_OK)
      return rc;
  }

  lc_query_req_init(&request);
  lc_index_flush_req_init(&flush_request);
  memset(&handler, 0, sizeof(handler));
  memset(&result, 0, sizeof(result));
  memset(&flush_result, 0, sizeof(flush_result));
  memset(&capture, 0, sizeof(capture));
  handler.begin = lc_outbox_recovery_key_begin;
  handler.chunk = lc_outbox_recovery_key_chunk;
  handler.end = lc_outbox_recovery_key_end;
  capture.outbox = outbox;
  consume_claim_recovery = 0;
  pthread_mutex_lock(&outbox->notification_mutex);
  if (outbox->notification_count + outbox->delayed_notification_count >=
      outbox->notification_capacity) {
    if (!lc_outbox_evict_delayed_for_recovery_locked(outbox)) {
      outbox->recovery_needed = 0;
      outbox->recovery_immediate = 0;
      outbox->recovery_resume_pending = 1;
#ifdef LOCKDC_TEST_BUILD
      if (lc_outbox_test_after_recovery_capacity_pause_hook != NULL) {
        lc_outbox_test_after_recovery_capacity_pause_hook(
            lc_outbox_test_after_recovery_capacity_pause_context);
      }
#endif
      pthread_mutex_unlock(&outbox->notification_mutex);
      return LC_OK;
    }
  }
  if (outbox->recovery_cursor == NULL &&
      outbox->recovery_scan_mode != LC_OUTBOX_RECOVERY_SCAN_RETRIES) {
    outbox->recovery_scan_mode = outbox->recovery_claims_pending
                                     ? LC_OUTBOX_RECOVERY_SCAN_CLAIMS
                                     : LC_OUTBOX_RECOVERY_SCAN_PENDING;
    consume_claim_recovery =
        outbox->recovery_scan_mode == LC_OUTBOX_RECOVERY_SCAN_CLAIMS;
  }
  scan_mode = outbox->recovery_scan_mode;
  capture.filter_retry_wait = scan_mode == LC_OUTBOX_RECOVERY_SCAN_RETRIES;
  available = outbox->notification_capacity - outbox->notification_count -
              outbox->delayed_notification_count;
  /* Do not consume a claim-recovery request until bounded capacity admits its
   * scan. A full queue is a transient local condition, not evidence that the
   * durable claim scan has happened. Signals raised after this point belong to
   * the next sweep. */
  if (consume_claim_recovery)
    outbox->recovery_claims_pending = 0;
  ++outbox->recovery_queries;
  pthread_mutex_unlock(&outbox->notification_mutex);
  use_index = outbox->dispatcher_client->pouch == NULL ||
              outbox->dispatcher_client->pouch->query_indexing_enabled;
  /* A recovery sweep starts from a durable index boundary. Later pages keep
   * that boundary: flushing every page would turn one large sweep into N
   * global flushes and needlessly amplify reconciliation cost. Scan-only
   * Pouch roots have no index boundary; their explicitly selected scan is
   * already current and remains a supported outbox recovery path. */
  if (use_index && outbox->recovery_cursor == NULL) {
    flush_request.ns = outbox->ns;
    flush_request.mode = "wait";
    rc = lc_flush_index(&outbox->dispatcher_client->pub, &flush_request,
                        &flush_result, error);
    lc_index_flush_res_cleanup(&flush_result);
    if (rc != LC_OK) {
      if (consume_claim_recovery) {
        pthread_mutex_lock(&outbox->notification_mutex);
        outbox->recovery_claims_pending = 1;
        pthread_mutex_unlock(&outbox->notification_mutex);
      }
      return rc;
    }
  }
  request.ns = outbox->ns;
  request.selector_json =
      scan_mode == LC_OUTBOX_RECOVERY_SCAN_CLAIMS
          ? claimed_selector
          : (scan_mode == LC_OUTBOX_RECOVERY_SCAN_RETRIES ? retry_selector
                                                          : pending_selector);
  request.limit = (long)available;
  request.cursor = outbox->recovery_cursor;
  request.engine = use_index ? "index" : "scan";
  request.refresh = use_index ? "wait_for" : NULL;
#ifdef LOCKDC_TEST_BUILD
  if (lc_outbox_test_before_recovery_query_hook != NULL) {
    lc_outbox_test_before_recovery_query_hook(
        lc_outbox_test_before_recovery_query_context);
  }
#endif
  rc = lc_query_keys(&outbox->dispatcher_client->pub, &request, &handler,
                     &capture, &result, error);
  if (rc != LC_OK) {
    lc_query_res_cleanup(&result);
    if (consume_claim_recovery) {
      pthread_mutex_lock(&outbox->notification_mutex);
      outbox->recovery_claims_pending = 1;
      pthread_mutex_unlock(&outbox->notification_mutex);
    }
    return rc;
  }
  cursor = result.cursor;
  result.cursor = NULL;
  lc_query_res_cleanup(&result);
#ifdef LOCKDC_TEST_BUILD
  if (lc_outbox_test_after_reconcile_query_hook != NULL) {
    lc_outbox_test_after_reconcile_query_hook(
        lc_outbox_test_after_reconcile_query_context);
  }
#endif
  /* Query cursors are public-result fields allocated in the libc domain; keep
   * their ownership consistent with lc_query_res_cleanup(). */
  if (capture.overflowed) {
    free(cursor);
    cursor = NULL;
  } else {
    free(outbox->recovery_cursor);
    outbox->recovery_cursor = cursor;
  }
  pthread_mutex_lock(&outbox->notification_mutex);
  /* The dispatcher clears the request before starting this sweep. Keep an
   * overflow signal raised while the query was in flight, otherwise the final
   * page could strand a newly committed durable key until a restart. */
  if (capture.overflowed) {
    if (outbox->notification_count + outbox->delayed_notification_count <
        outbox->notification_capacity) {
      /* A consumer can free a slot while the query callback is still running.
       * Its capacity-release wake precedes this completion path, so consume
       * the already available budget immediately instead of leaving durable
       * work paused until another foreground demand arrives. */
      outbox->recovery_needed = 1;
      outbox->recovery_immediate = 1;
      outbox->recovery_resume_pending = 0;
      lc_outbox_signal_dispatcher_locked(outbox);
    } else {
      outbox->recovery_needed = 0;
      outbox->recovery_immediate = 0;
      outbox->recovery_resume_pending = 1;
    }
  } else if (outbox->recovery_cursor == NULL &&
             scan_mode == LC_OUTBOX_RECOVERY_SCAN_CLAIMS) {
    outbox->recovery_scan_mode = LC_OUTBOX_RECOVERY_SCAN_PENDING;
    /* Claims are recovery preparation. Follow with the ordinary pending pass
     * without making claims part of every hot reconciliation sweep. */
    outbox->recovery_needed = 1;
    outbox->recovery_immediate = 1;
  } else if (outbox->recovery_cursor == NULL &&
             scan_mode == LC_OUTBOX_RECOVERY_SCAN_PENDING) {
    if (outbox->recovery_needed && outbox->recovery_immediate) {
      /* A foreground claim failed while this page was running. Preserve that
       * new immediate request instead of replacing it with the follow-up retry
       * sweep; its candidate may be the only remaining pending work. */
      outbox->recovery_scan_mode = LC_OUTBOX_RECOVERY_SCAN_PENDING;
    } else {
      /* Retry records need envelope inspection to preserve their not-before
       * deadline. Keep that uncommon body work out of the pending hot sweep. */
      outbox->recovery_scan_mode = LC_OUTBOX_RECOVERY_SCAN_RETRIES;
      outbox->recovery_needed = 1;
      outbox->recovery_immediate = 1;
    }
  } else if (outbox->recovery_cursor == NULL) {
    outbox->recovery_scan_mode = LC_OUTBOX_RECOVERY_SCAN_PENDING;
  } else {
    outbox->recovery_needed =
        outbox->recovery_needed || outbox->recovery_cursor != NULL;
    if (outbox->recovery_cursor != NULL)
      outbox->recovery_immediate = 1;
  }
  pthread_mutex_unlock(&outbox->notification_mutex);
  return LC_OK;
}

static void *lc_outbox_dispatcher_main(void *context) {
  lc_outbox_handle *outbox = (lc_outbox_handle *)context;
  lc_outbox_dispatcher_handle *dispatcher;

  for (;;) {
    lc_error error;
    int reconcile;

    reconcile = 0;
    pthread_mutex_lock(&outbox->notification_mutex);
    while (!outbox->closed) {
      lc_unix_seconds now = (lc_unix_seconds)time(NULL);
      lc_unix_seconds due;

      if (now > 0)
        lc_outbox_promote_due_retries_locked(outbox, now);
      /* A periodic recovery has no request flag until its interval expires.
       * Delayed fallback requests set the flag when scheduled and are instead
       * released by lc_outbox_recovery_is_due_locked() below. */
      if (!outbox->recovery_needed && now > 0 &&
          outbox->next_recovery_unix > 0 && now >= outbox->next_recovery_unix) {
        outbox->recovery_needed = 1;
        outbox->recovery_immediate = 1;
        outbox->recovery_claims_pending = 1;
      }
      if (lc_outbox_recovery_is_due_locked(outbox, now))
        break;
      due = lc_outbox_next_dispatch_deadline_locked(outbox);
      if (due > 0) {
        struct timespec deadline;
        deadline.tv_sec = due;
        deadline.tv_nsec = 0L;
        if (pthread_cond_timedwait(&outbox->dispatcher_cond,
                                   &outbox->notification_mutex,
                                   &deadline) == ETIMEDOUT) {
          now = (lc_unix_seconds)time(NULL);
          if (now > 0)
            lc_outbox_promote_due_retries_locked(outbox, now);
        }
      } else {
#ifdef LOCKDC_TEST_BUILD
        if (lc_outbox_test_before_dispatcher_wait_hook != NULL) {
          lc_outbox_test_before_dispatcher_wait_hook(
              lc_outbox_test_before_dispatcher_wait_context);
        }
#endif
        pthread_cond_wait(&outbox->dispatcher_cond,
                          &outbox->notification_mutex);
      }
    }
    if (outbox->closed) {
      pthread_mutex_unlock(&outbox->notification_mutex);
      break;
    }
    reconcile = 1;
    /* A new overflow while reconciliation runs must be distinguishable from
     * the request that selected this sweep. Direct keys remain candidates for
     * a consumer; the private thread never removes or claims them. */
    outbox->recovery_needed = 0;
    outbox->recovery_immediate = 0;
    pthread_mutex_unlock(&outbox->notification_mutex);

    if (reconcile) {
      int recovery_rc;
      int schedule_rc;
      lc_error schedule_error;
      lc_unix_seconds now;
      lc_unix_seconds periodic;

      schedule_rc = LC_OK;
      now = 0;
      periodic = 0;
      lc_error_init(&schedule_error);
      lc_error_init(&error);
      recovery_rc = lc_outbox_reconcile_pending(outbox, &error);
      if (recovery_rc != LC_OK)
        lc_outbox_record_error(outbox, &error);
      lc_error_cleanup(&error);
      if (recovery_rc == LC_OK) {
        now = (lc_unix_seconds)time(NULL);
        if (outbox->recovery_interval_seconds > 0L) {
#ifdef LOCKDC_TEST_BUILD
          if (lc_outbox_test_before_periodic_recovery_schedule_hook != NULL) {
            schedule_rc = lc_outbox_test_before_periodic_recovery_schedule_hook(
                lc_outbox_test_before_periodic_recovery_schedule_context,
                &schedule_error);
          } else
#endif
          {
            schedule_rc = lc_outbox_timestamp_add(
                now, outbox->recovery_interval_seconds, "recovery interval",
                &periodic, &schedule_error);
          }
          if (schedule_rc != LC_OK) {
            /* A clock jump beyond the representable range must not invoke
             * signed overflow or silently create a bogus timer. The initial
             * configuration path already rejects ordinary invalid intervals. */
            periodic = 0;
          }
        }
      }
      pthread_mutex_lock(&outbox->notification_mutex);
      if (recovery_rc != LC_OK) {
        lc_unix_seconds retry_at = 0;
        time_t retry_now = time(NULL);

        outbox->recovery_needed = 0;
        outbox->recovery_immediate = 0;
        if (retry_now != (time_t)-1) {
          lc_error retry_error;

          lc_error_init(&retry_error);
          if (lc_outbox_timestamp_add((lc_unix_seconds)retry_now, 1L,
                                      "recovery retry", &retry_at,
                                      &retry_error) == LC_OK) {
            outbox->next_recovery_unix = retry_at;
          } else {
            outbox->next_recovery_unix = 0;
          }
          lc_error_cleanup(&retry_error);
        } else {
          outbox->next_recovery_unix = 0;
        }
      } else {
        /* A retry whose delayed-key allocation failed has no in-memory entry,
         * so retain its durable-recovery deadline across this eager sweep. */
        if (outbox->next_recovery_unix <= now ||
            (periodic > 0 && (outbox->next_recovery_unix == 0 ||
                              periodic < outbox->next_recovery_unix))) {
          outbox->next_recovery_unix = periodic;
        }
      }
      pthread_mutex_unlock(&outbox->notification_mutex);
      if (schedule_rc != LC_OK)
        lc_outbox_record_error(outbox, &schedule_error);
      lc_error_cleanup(&schedule_error);
      continue;
    }
  }
  /* Retain before publishing worker exit: client close may detach this shell
   * concurrently, while this detached thread still needs its lifecycle state.
   */
  dispatcher = outbox->dispatcher;
  if (dispatcher != NULL)
    lc_outbox_dispatcher_retain(dispatcher);
  pthread_mutex_lock(&outbox->notification_mutex);
  outbox->dispatcher_exited = 1;
  if (outbox->dispatcher_cond_initialized)
    pthread_cond_broadcast(&outbox->dispatcher_cond);
  pthread_mutex_unlock(&outbox->notification_mutex);
  /* A detached private thread owns one reference until it has stopped using
   * the core.  The dispatcher owner or a direct construction failure releases
   * the other reference after observing dispatcher_exited. */
  lc_outbox_release(outbox);
  if (dispatcher != NULL) {
    int finalize_client_close;

    pthread_mutex_lock(&dispatcher->lifecycle_mutex);
    dispatcher->worker_finished = 1;
    finalize_client_close = dispatcher->client_close_requested;
    pthread_cond_broadcast(&dispatcher->lifecycle_cond);
    pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
    /* A client close issued from a Pouch callback cannot synchronously join
     * this thread: recovery may be waiting for the callback's namespace lock.
     * Once the callback has unwound and this worker exits, finish the deferred
     * client-close cleanup here. The worker-finished flag makes this wait
     * non-blocking and preserves the normal registry/refcount teardown. */
    if (finalize_client_close) {
      (void)lc_outbox_dispatcher_wait_method(&dispatcher->pub, -1L, NULL);
    }
    lc_outbox_dispatcher_close_method(&dispatcher->pub);
  }
  return NULL;
}

static int lc_outbox_wait_for_ready(lc_outbox_handle *outbox, long timeout_ms,
                                    lc_outbox_job **out, lc_error *error) {
  struct timespec deadline;
  int demand_recovery_requested;
  int wait_rc;
  char *key;
  int rc;

  if (timeout_ms < -1L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox next timeout must be -1 or non-negative", NULL,
                        NULL, NULL);
  }
  *out = NULL;
  demand_recovery_requested = 0;
  if (timeout_ms > 0L) {
    if (clock_gettime(CLOCK_REALTIME, &deadline) != 0) {
      return lc_error_set(error, LC_ERR_PROTOCOL, errno,
                          "failed to construct outbox wait deadline", NULL,
                          NULL, NULL);
    }
    lc_outbox_timespec_add_milliseconds_saturating(&deadline, timeout_ms);
  }
  for (;;) {
    key = NULL;
    pthread_mutex_lock(&outbox->notification_mutex);
    /* A blocking consumer is one unit of demand. Starting a dispatcher alone
     * deliberately performs no recovery I/O: a host may create one long before
     * it has a worker slot. A non-blocking probe remains a pure in-memory check
     * so polling cannot turn into a namespace scan hot path. */
    if (!outbox->closed && timeout_ms != 0L && !demand_recovery_requested &&
        outbox->notification_count == 0U) {
      outbox->recovery_needed = 1;
      outbox->recovery_immediate = 1;
      lc_outbox_signal_dispatcher_locked(outbox);
      demand_recovery_requested = 1;
    }
    while (!outbox->closed && outbox->notification_count == 0U) {
      if (timeout_ms == 0L)
        break;
#ifdef LOCKDC_TEST_BUILD
      if (lc_outbox_test_before_next_wait_hook != NULL) {
        lc_outbox_test_before_next_wait_hook(
            lc_outbox_test_before_next_wait_context);
      }
#endif
      ++outbox->waiting_consumers;
      if (timeout_ms < 0L) {
        wait_rc = pthread_cond_wait(&outbox->notification_cond,
                                    &outbox->notification_mutex);
      } else {
        wait_rc = pthread_cond_timedwait(
            &outbox->notification_cond, &outbox->notification_mutex, &deadline);
      }
      --outbox->waiting_consumers;
      if (wait_rc == ETIMEDOUT)
        break;
      if (wait_rc != 0) {
        pthread_mutex_unlock(&outbox->notification_mutex);
        return lc_error_set(error, LC_ERR_PROTOCOL, wait_rc,
                            "outbox dispatcher wait failed", NULL, NULL, NULL);
      }
    }
    if (outbox->closed) {
      pthread_mutex_unlock(&outbox->notification_mutex);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "outbox dispatcher is stopped", NULL, NULL, NULL);
    }
    if (outbox->notification_count > 0U) {
      key = outbox->notifications[0];
      if (outbox->notification_count > 1U) {
        memmove(outbox->notifications, outbox->notifications + 1U,
                (outbox->notification_count - 1U) *
                    sizeof(*outbox->notifications));
      }
      --outbox->notification_count;
      lc_outbox_resume_recovery_if_capacity_locked(outbox);
    }
    pthread_mutex_unlock(&outbox->notification_mutex);
    if (key == NULL)
      return LC_OK;
    rc = lc_outbox_claim_outbox(outbox, key, out, error);
    if (rc == LC_OK) {
      lc_client_free(outbox->client, key);
      return LC_OK;
    }
    pthread_mutex_lock(&outbox->notification_mutex);
    ++outbox->claim_losses;
    pthread_mutex_unlock(&outbox->notification_mutex);
    lc_outbox_record_error(outbox, error);
    if (lc_outbox_claim_failure_is_retryable(error)) {
      time_t now = time(NULL);
      if (now != (time_t)-1) {
        lc_unix_seconds retry_at = 0;
        lc_error retry_error;

        lc_error_init(&retry_error);
        if (lc_outbox_timestamp_add(
                (lc_unix_seconds)now, outbox->retry_initial_delay_seconds,
                "retry initial delay", &retry_at, &retry_error) == LC_OK)
          lc_outbox_schedule_retry(outbox, key, retry_at);
        else {
          lc_outbox_record_error(outbox, &retry_error);
          lc_outbox_request_recovery(outbox, 0);
          /* A retry deadline that cannot be represented has no delayed wake.
           * Re-arm this caller's demand so its durable recovery request cannot
           * be lost behind the first in-flight sweep. */
          demand_recovery_requested = 0;
        }
        lc_error_cleanup(&retry_error);
      } else {
        lc_outbox_request_recovery(outbox, 0);
        demand_recovery_requested = 0;
      }
    }
    lc_error_cleanup(error);
    lc_error_init(error);
    lc_client_free(outbox->client, key);
    if (timeout_ms == 0L)
      return LC_OK;
  }
}

#ifdef LOCKDC_TEST_BUILD
void lc_outbox_test_wake_next_waiters(lc_outbox *self) {
  lc_outbox_handle *outbox = (lc_outbox_handle *)self;

  if (outbox == NULL || !outbox->notification_mutex_initialized ||
      !outbox->notification_cond_initialized)
    return;
  pthread_mutex_lock(&outbox->notification_mutex);
  pthread_cond_broadcast(&outbox->notification_cond);
  pthread_mutex_unlock(&outbox->notification_mutex);
}

#endif

static void lc_outbox_participant_refresh(lc_outbox_participant_handle *p) {
  if (p->lease == NULL) {
    return;
  }
  p->pub.ns = p->lease->ns;
  p->pub.key = p->lease->key;
  p->pub.txn_id = p->lease->txn_id;
  p->pub.fencing_token = p->lease->fencing_token;
  p->pub.version = p->lease->version;
  p->pub.state_etag = p->lease->state_etag;
}

static void lc_outbox_participant_invalidate(lc_outbox_participant_handle *p) {
  if (p == NULL)
    return;
  p->transaction = NULL;
  p->lease = NULL;
  p->next = NULL;
  p->pub.ns = NULL;
  p->pub.key = NULL;
  p->pub.txn_id = NULL;
  p->pub.fencing_token = 0L;
  p->pub.version = 0;
  p->pub.state_etag = NULL;
}

static void lc_outbox_transaction_remove_participant(
    lc_outbox_transaction_handle *transaction,
    lc_outbox_participant_handle *participant) {
  lc_outbox_participant_handle **cursor;

  if (transaction == NULL || participant == NULL)
    return;
  cursor = &transaction->participants;
  while (*cursor != NULL) {
    if (*cursor == participant) {
      *cursor = participant->next;
      participant->transaction = NULL;
      participant->next = NULL;
      return;
    }
    cursor = &(*cursor)->next;
  }
  participant->transaction = NULL;
  participant->next = NULL;
}

static void lc_outbox_transaction_invalidate_participants(
    lc_outbox_transaction_handle *transaction) {
  lc_outbox_participant_handle *participant;

  if (transaction == NULL)
    return;
  participant = transaction->participants;
  transaction->participants = NULL;
  while (participant != NULL) {
    lc_outbox_participant_handle *next = participant->next;

    lc_outbox_participant_invalidate(participant);
    participant = next;
  }
}

static int lc_outbox_participant_describe(lc_outbox_participant *self,
                                          lc_error *error) {
  lc_outbox_participant_handle *p = (lc_outbox_participant_handle *)self;
  int rc;
  if (p == NULL || p->lease == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox participant is closed", NULL, NULL, NULL);
  rc = lc_lease_describe(p->lease, error);
  lc_outbox_participant_refresh(p);
  return rc;
}
static int lc_outbox_participant_get(lc_outbox_participant *self, lc_sink *dst,
                                     const lc_get_opts *opts, lc_get_res *out,
                                     lc_error *error) {
  lc_outbox_participant_handle *p = (lc_outbox_participant_handle *)self;
  if (p == NULL || p->lease == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox participant is closed", NULL, NULL, NULL);
  return lc_lease_get(p->lease, dst, opts, out, error);
}
static int lc_outbox_participant_update(lc_outbox_participant *self,
                                        lc_source *src,
                                        const lc_update_opts *opts,
                                        lc_error *error) {
  lc_outbox_participant_handle *p = (lc_outbox_participant_handle *)self;
  int rc;
  if (p == NULL || p->lease == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox participant is closed", NULL, NULL, NULL);
  rc = lc_lease_update(p->lease, src, opts, error);
  lc_outbox_participant_refresh(p);
  return rc;
}
static int lc_outbox_participant_mutate(lc_outbox_participant *self,
                                        const lc_mutate_req *req,
                                        lc_error *error) {
  lc_outbox_participant_handle *p = (lc_outbox_participant_handle *)self;
  int rc;

  if (p == NULL || p->lease == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox participant is closed", NULL, NULL, NULL);
  }
  rc = lc_lease_mutate(p->lease, req, error);
  lc_outbox_participant_refresh(p);
  return rc;
}
static int lc_outbox_participant_mutate_local(lc_outbox_participant *self,
                                              const lc_mutate_local_req *req,
                                              lc_error *error) {
  lc_outbox_participant_handle *p = (lc_outbox_participant_handle *)self;
  int rc;

  if (p == NULL || p->lease == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox participant is closed", NULL, NULL, NULL);
  }
  rc = lc_lease_mutate_local(p->lease, req, error);
  lc_outbox_participant_refresh(p);
  return rc;
}
static int lc_outbox_participant_metadata(lc_outbox_participant *self,
                                          const lc_metadata_req *req,
                                          lc_error *error) {
  lc_outbox_participant_handle *p = (lc_outbox_participant_handle *)self;
  int rc;
  if (p == NULL || p->lease == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox participant is closed", NULL, NULL, NULL);
  rc = lc_lease_metadata(p->lease, req, error);
  lc_outbox_participant_refresh(p);
  return rc;
}
static int lc_outbox_participant_remove(lc_outbox_participant *self,
                                        const lc_remove_req *req,
                                        lc_error *error) {
  lc_outbox_participant_handle *p = (lc_outbox_participant_handle *)self;
  int rc;
  if (p == NULL || p->lease == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox participant is closed", NULL, NULL, NULL);
  rc = lc_lease_remove(p->lease, req, error);
  lc_outbox_participant_refresh(p);
  return rc;
}
static int lc_outbox_participant_keepalive(lc_outbox_participant *self,
                                           const lc_keepalive_req *req,
                                           lc_error *error) {
  lc_outbox_participant_handle *p = (lc_outbox_participant_handle *)self;
  int rc;
  if (p == NULL || p->lease == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox participant is closed", NULL, NULL, NULL);
  rc = lc_lease_keepalive(p->lease, req, error);
  lc_outbox_participant_refresh(p);
  return rc;
}
static int lc_outbox_participant_attach(lc_outbox_participant *self,
                                        const lc_attach_req *req,
                                        lc_source *src, lc_attach_res *out,
                                        lc_error *error) {
  lc_outbox_participant_handle *p = (lc_outbox_participant_handle *)self;
  int rc;

  if (p == NULL || p->lease == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox participant is closed", NULL, NULL, NULL);
  rc = lc_lease_attach(p->lease, req, src, out, error);
  if (rc == LC_OK) {
    p->lease->version = out->version;
    lc_outbox_participant_refresh(p);
  }
  return rc;
}
static int lc_outbox_participant_list_attachments(lc_outbox_participant *self,
                                                  lc_attachment_list *out,
                                                  lc_error *error) {
  lc_outbox_participant_handle *p = (lc_outbox_participant_handle *)self;
  if (p == NULL || p->lease == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox participant is closed", NULL, NULL, NULL);
  }
  return lc_lease_list_attachments(p->lease, out, error);
}
static int lc_outbox_participant_get_attachment(
    lc_outbox_participant *self, const lc_attachment_get_req *req, lc_sink *dst,
    lc_attachment_get_res *out, lc_error *error) {
  lc_outbox_participant_handle *p = (lc_outbox_participant_handle *)self;
  if (p == NULL || p->lease == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox participant is closed", NULL, NULL, NULL);
  return lc_lease_get_attachment(p->lease, req, dst, out, error);
}
static int
lc_outbox_participant_delete_attachment(lc_outbox_participant *self,
                                        const lc_attachment_selector *selector,
                                        int *deleted, lc_error *error) {
  lc_outbox_participant_handle *p = (lc_outbox_participant_handle *)self;
  if (p == NULL || p->lease == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox participant is closed", NULL, NULL, NULL);
  }
  return lc_lease_delete_attachment(p->lease, selector, deleted, error);
}
static int lc_outbox_participant_delete_all_attachments(
    lc_outbox_participant *self, int *deleted_count, lc_error *error) {
  lc_outbox_participant_handle *p = (lc_outbox_participant_handle *)self;
  if (p == NULL || p->lease == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox participant is closed", NULL, NULL, NULL);
  }
  return lc_lease_delete_all_attachments(p->lease, deleted_count, error);
}
static void lc_outbox_participant_close_method(lc_outbox_participant *self) {
  lc_outbox_participant_handle *p = (lc_outbox_participant_handle *)self;
  if (p != NULL) {
    lc_outbox_handle *outbox = p->outbox;

    if (p->transaction != NULL)
      lc_outbox_transaction_remove_participant(p->transaction, p);
    lc_client_free(outbox->client, p);
    lc_outbox_release(outbox);
  }
}

/* A later participant is already durably enrolled in the implicit-XA xid
 * before an outbox operation can report a post-enrollment failure. Rolling
 * that participant back therefore decides rollback for the whole xid; keep
 * the local transaction equally terminal and release every retained handle. */
static void lc_outbox_transaction_abort_enrolled_lease(
    lc_outbox_transaction_handle *transaction, lc_lease *lease) {
  size_t index;

  if (transaction == NULL)
    return;
  lc_outbox_transaction_clear_command(transaction, lease);
  lc_outbox_transaction_remove_lease(transaction, lease);
  lc_outbox_transaction_invalidate_participants(transaction);
  lc_outbox_rollback_lease(lease);
  for (index = 0U; index < transaction->lease_count; ++index) {
    lc_outbox_rollback_lease(transaction->leases[index]);
    transaction->leases[index] = NULL;
  }
  lc_outbox_transaction_clear_notification_keys(transaction);
  lc_outbox_transaction_clear_fresh_outboxes(transaction);
  transaction->terminal = 1;
}

/* An outbox transaction is an explicit XA decision even when its first
 * participant is the only one. Pouch can mint an xid for an ordinary lease,
 * but that implicit single-lease path intentionally has no durable decision
 * record. Mint the outbox xid immediately before its first participating
 * acquire instead: begin() itself stays side-effect free, while a
 * domain-first transaction has the same recoverable commit semantics as an
 * outbox-first one. */
static int
lc_outbox_transaction_acquire_xid(lc_outbox_transaction_handle *transaction,
                                  char minted[LC_XID_STRING_SIZE],
                                  const char **out, lc_error *error) {
  const char *txn_id;

  if (transaction == NULL || minted == NULL || out == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "outbox transaction xid requires transaction and output", NULL, NULL,
        NULL);
  }
  if (transaction->lease_count == 0U) {
    int rc = lc_xid_new(minted, error);

    if (rc != LC_OK)
      return rc;
    *out = minted;
    return LC_OK;
  }
  txn_id =
      transaction->leases[0] == NULL ? NULL : transaction->leases[0]->txn_id;
  if (txn_id == NULL || txn_id[0] == '\0') {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "outbox transaction participant is missing its xid",
                        NULL, NULL, NULL);
  }
  *out = txn_id;
  return LC_OK;
}

static int lc_outbox_transaction_acquire_method(
    lc_outbox_transaction *self, const lc_outbox_participant_request *request,
    lc_outbox_participant **out, lc_error *error) {
  lc_outbox_transaction_handle *transaction =
      (lc_outbox_transaction_handle *)self;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_outbox_participant_handle *participant;
  char minted_txn_id[LC_XID_STRING_SIZE];
  int rc;
  if (transaction == NULL || request == NULL || out == NULL ||
      transaction->terminal || transaction->terminal_vote_started ||
      request->acquire.key == NULL || request->acquire.key[0] == '\0' ||
      request->acquire.txn_id != NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox participant requires an open transaction, "
                        "key, and no caller transaction id",
                        NULL, NULL, NULL);
  }
  acquire = request->acquire;
  rc = lc_outbox_transaction_acquire_xid(transaction, minted_txn_id,
                                         &acquire.txn_id, error);
  if (rc != LC_OK)
    return rc;
  if (acquire.ns == NULL)
    acquire.ns = transaction->outbox->ns;
  if (acquire.owner == NULL || acquire.owner[0] == '\0')
    acquire.owner = transaction->outbox->owner;
  if (acquire.ttl_seconds == 0L)
    acquire.ttl_seconds = transaction->outbox->transaction_ttl_seconds;
  lease = NULL;
  rc = lc_acquire(&transaction->outbox->client->pub, &acquire, &lease, error);
  if (rc != LC_OK)
    return rc;
  rc = lc_outbox_transaction_add_lease(transaction, lease, error);
  if (rc != LC_OK) {
    lc_outbox_transaction_abort_enrolled_lease(transaction, lease);
    return rc;
  }
  transaction->has_domain_participant = 1;
#ifdef LOCKDC_TEST_BUILD
  if (lc_outbox_test_before_participant_allocation_hook != NULL) {
    rc = lc_outbox_test_before_participant_allocation_hook(
        lc_outbox_test_before_participant_allocation_context, error);
    if (rc != LC_OK) {
      lc_outbox_transaction_abort_enrolled_lease(transaction, lease);
      return rc;
    }
  }
#endif
  participant = (lc_outbox_participant_handle *)lc_client_calloc(
      transaction->outbox->client, 1U, sizeof(*participant));
  if (participant == NULL) {
    lc_outbox_transaction_abort_enrolled_lease(transaction, lease);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate outbox participant", NULL, NULL,
                        NULL);
  }
  lc_outbox_retain(transaction->outbox);
  participant->outbox = transaction->outbox;
  participant->transaction = transaction;
  participant->lease = lease;
  participant->next = transaction->participants;
  transaction->participants = participant;
  participant->pub.describe = lc_outbox_participant_describe;
  participant->pub.get = lc_outbox_participant_get;
  participant->pub.update = lc_outbox_participant_update;
  participant->pub.mutate = lc_outbox_participant_mutate;
  participant->pub.mutate_local = lc_outbox_participant_mutate_local;
  participant->pub.metadata = lc_outbox_participant_metadata;
  participant->pub.remove = lc_outbox_participant_remove;
  participant->pub.keepalive = lc_outbox_participant_keepalive;
  participant->pub.attach = lc_outbox_participant_attach;
  participant->pub.list_attachments = lc_outbox_participant_list_attachments;
  participant->pub.get_attachment = lc_outbox_participant_get_attachment;
  participant->pub.delete_attachment = lc_outbox_participant_delete_attachment;
  participant->pub.delete_all_attachments =
      lc_outbox_participant_delete_all_attachments;
  participant->pub.close = lc_outbox_participant_close_method;
  lc_outbox_participant_refresh(participant);
  *out = &participant->pub;
  return LC_OK;
}

static int lc_outbox_transaction_accept_command_method(
    lc_outbox_transaction *self, const lc_command_request *request,
    lc_command_receipt *receipt, lc_error *error) {
  lc_outbox_transaction_handle *transaction =
      (lc_outbox_transaction_handle *)self;
  lc_acquire_req acquire;
  lc_lease *lease;
  char *key;
  char command_id[48];
  char minted_txn_id[LC_XID_STRING_SIZE];
  lc_outbox_command_record record;
  lc_command_request effective_request;
  char generated_key[LC_XID_STRING_SIZE];
  int rc;

  if (transaction == NULL || transaction->terminal ||
      transaction->terminal_vote_started || request == NULL ||
      receipt == NULL || transaction->command_lease != NULL ||
      request->request_digest == NULL || request->request_digest[0] == '\0') {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "open transaction, one command request, and digest are required", NULL,
        NULL, NULL);
  }
  rc = lc_outbox_effective_command_request(request, &effective_request,
                                           generated_key, error);
  if (rc != LC_OK)
    return rc;
  request = &effective_request;
  rc = lc_outbox_validate_command_request(transaction->outbox->client, request,
                                          error);
  if (rc != LC_OK)
    return rc;
  lc_command_receipt_cleanup(receipt);
  key = NULL;
  rc = lc_outbox_command_key(&request->identity, &key, command_id, error);
  if (rc != LC_OK)
    return rc;
  lc_acquire_req_init(&acquire);
  acquire.ns = transaction->outbox->ns;
  acquire.key = key;
  acquire.owner = transaction->outbox->owner;
  acquire.ttl_seconds = transaction->outbox->transaction_ttl_seconds;
  acquire.if_not_exists = 1;
  rc = lc_outbox_transaction_acquire_xid(transaction, minted_txn_id,
                                         &acquire.txn_id, error);
  if (rc != LC_OK) {
    free(key);
    return rc;
  }
  lease = NULL;
  rc = lc_acquire(&transaction->outbox->client->pub, &acquire, &lease, error);
  if (rc != LC_OK) {
    lc_lease *duplicate_lease;

    duplicate_lease = NULL;
    rc = lc_outbox_probe_duplicate_barrier(transaction->outbox, key, rc,
                                           &duplicate_lease, error);
    if (rc == LC_OK) {
      rc = lc_outbox_existing_command(transaction->outbox, duplicate_lease, key,
                                      &request->identity, command_id, request,
                                      receipt, error);
      lc_outbox_rollback_lease(duplicate_lease);
      if (rc == LC_OK && transaction->has_domain_participant)
        lc_outbox_transaction_abort_enrolled_lease(transaction, NULL);
    }
    free(key);
    return rc;
  }
  rc = lc_outbox_stage_command(transaction->outbox->client, lease, request,
                               command_id, error);
  if (rc == LC_OK)
    rc = lc_outbox_transaction_add_lease(transaction, lease, error);
  if (rc == LC_OK)
    rc = lc_outbox_transaction_set_command(transaction, lease, command_id,
                                           error);
  if (rc != LC_OK) {
    lc_outbox_transaction_abort_enrolled_lease(transaction, lease);
    free(key);
    return rc;
  }
  memset(&record, 0, sizeof(record));
  record.record_type = "lockdc.command.v1";
  record.command_id = command_id;
  record.scope = (char *)request->identity.scope;
  record.command_type = (char *)request->identity.command_type;
  record.idempotency_key = (char *)request->identity.idempotency_key;
  record.request_digest = (char *)request->request_digest;
  record.accepted_at_unix = 1;
  record.state = "pending";
  record.operation_id = (char *)request->operation_id;
#ifdef LOCKDC_TEST_BUILD
  if (lc_outbox_test_before_command_receipt_copy_hook != NULL) {
    rc = lc_outbox_test_before_command_receipt_copy_hook(
        lc_outbox_test_before_command_receipt_copy_context, error);
    if (rc != LC_OK) {
      free(key);
      lc_outbox_transaction_abort_enrolled_lease(transaction, lease);
      return rc;
    }
  }
#endif
  rc = lc_outbox_command_receipt_from_record(&record, receipt, error);
  free(key);
  if (rc != LC_OK) {
    lc_outbox_transaction_abort_enrolled_lease(transaction, lease);
  }
  return rc;
}

static int lc_outbox_transaction_accept_inbox_method(
    lc_outbox_transaction *self, const lc_inbox_message *message,
    lc_inbox_accept_result *result, lc_error *error) {
  lc_outbox_transaction_handle *transaction =
      (lc_outbox_transaction_handle *)self;
  lc_acquire_req acquire;
  lc_lease *lease;
  char *key;
  char minted_txn_id[LC_XID_STRING_SIZE];
  int rc;

  if (transaction == NULL || transaction->terminal ||
      transaction->terminal_vote_started || message == NULL || result == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "open transaction, inbox message, and result are "
                        "required",
                        NULL, NULL, NULL);
  }
  memset(result, 0, sizeof(*result));
  rc = lc_outbox_validate_inbox_message(transaction->outbox->client, message,
                                        error);
  if (rc != LC_OK)
    return rc;
  key = NULL;
  rc = lc_outbox_inbox_key(transaction->outbox, message, &key, error);
  if (rc != LC_OK)
    return rc;
  lc_acquire_req_init(&acquire);
  acquire.ns = transaction->outbox->ns;
  acquire.key = key;
  acquire.owner = transaction->outbox->owner;
  acquire.ttl_seconds = transaction->outbox->transaction_ttl_seconds;
  acquire.if_not_exists = 1;
  rc = lc_outbox_transaction_acquire_xid(transaction, minted_txn_id,
                                         &acquire.txn_id, error);
  if (rc != LC_OK) {
    free(key);
    return rc;
  }
  lease = NULL;
  rc = lc_acquire(&transaction->outbox->client->pub, &acquire, &lease, error);
  if (rc != LC_OK) {
    lc_lease *duplicate_lease;

    duplicate_lease = NULL;
    rc = lc_outbox_probe_duplicate_barrier(transaction->outbox, key, rc,
                                           &duplicate_lease, error);
    if (rc == LC_OK) {
      rc = lc_outbox_existing_inbox(transaction->outbox, duplicate_lease, key,
                                    message, result, error);
      lc_outbox_rollback_lease(duplicate_lease);
      if (rc == LC_OK && transaction->has_domain_participant)
        lc_outbox_transaction_abort_enrolled_lease(transaction, NULL);
    }
    free(key);
    return rc;
  }
  free(key);
  rc =
      lc_outbox_stage_inbox(transaction->outbox->client, lease, message, error);
  if (rc == LC_OK)
    rc = lc_outbox_transaction_add_lease(transaction, lease, error);
  if (rc == LC_OK && transaction->causation_id == NULL) {
    transaction->causation_id =
        lc_client_strdup(transaction->outbox->client, message->message_id);
    if (transaction->causation_id == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to retain inbox causation identity", NULL, NULL,
                        NULL);
    }
  }
  if (rc != LC_OK) {
    lc_outbox_transaction_abort_enrolled_lease(transaction, lease);
    return rc;
  }
  result->accepted = 1;
  return LC_OK;
}

static int
lc_outbox_transaction_complete_command_method(lc_outbox_transaction *self,
                                              const lc_command_result *result,
                                              lc_error *error) {
  return lc_outbox_stage_command_terminal((lc_outbox_transaction_handle *)self,
                                          result, 0, error);
}

static int
lc_outbox_transaction_fail_command_method(lc_outbox_transaction *self,
                                          const lc_command_result *result,
                                          lc_error *error) {
  return lc_outbox_stage_command_terminal((lc_outbox_transaction_handle *)self,
                                          result, 1, error);
}

static int lc_outbox_transaction_append_method(lc_outbox_transaction *self,
                                               const lc_outbox_entry *entry,
                                               lc_source *payload,
                                               lc_outbox_receipt *receipt,
                                               lc_error *error) {
  lc_outbox_transaction_handle *transaction =
      (lc_outbox_transaction_handle *)self;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_outbox_entry effective_entry;
  char *key;
  char minted_txn_id[LC_XID_STRING_SIZE];
  int rc;
  if (transaction == NULL || transaction->terminal ||
      transaction->terminal_vote_started || receipt == NULL || entry == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox transaction is closed", NULL, NULL, NULL);
  lc_outbox_receipt_cleanup(receipt);
  effective_entry = *entry;
  if (effective_entry.causation_id == NULL)
    effective_entry.causation_id = transaction->causation_id;
  rc = lc_outbox_validate_outbox_envelope(&effective_entry, error);
  if (rc != LC_OK)
    return rc;
  key = NULL;
  rc = lc_outbox_key(transaction->outbox, &effective_entry, &key, error);
  if (rc != LC_OK)
    return rc;
  lc_acquire_req_init(&acquire);
  acquire.ns = transaction->outbox->ns;
  acquire.key = key;
  acquire.owner = transaction->outbox->owner;
  acquire.ttl_seconds = transaction->outbox->transaction_ttl_seconds;
  acquire.if_not_exists = 1;
  rc = lc_outbox_transaction_acquire_xid(transaction, minted_txn_id,
                                         &acquire.txn_id, error);
  if (rc != LC_OK) {
    free(key);
    return rc;
  }
  lease = NULL;
  rc = lc_acquire(&transaction->outbox->client->pub, &acquire, &lease, error);
  if (rc != LC_OK) {
    lc_lease *duplicate_lease;

    duplicate_lease = NULL;
    rc = lc_outbox_probe_duplicate_barrier(transaction->outbox, key, rc,
                                           &duplicate_lease, error);
    if (rc == LC_OK) {
      rc = lc_outbox_existing_outbox(transaction->outbox, duplicate_lease, key,
                                     &effective_entry, receipt, error);
      lc_outbox_rollback_lease(duplicate_lease);
      if (rc == LC_OK && transaction->has_domain_participant)
        lc_outbox_transaction_abort_enrolled_lease(transaction, NULL);
    }
    free(key);
    return rc;
  }
  rc = lc_outbox_stage_outbox(lease, &effective_entry, transaction->command_id,
                              payload, error);
  if (rc != LC_OK) {
    lc_outbox_transaction_abort_enrolled_lease(transaction, lease);
    free(key);
    return rc;
  }
  rc = lc_outbox_transaction_add_lease(transaction, lease, error);
  if (rc != LC_OK) {
    lc_outbox_transaction_abort_enrolled_lease(transaction, lease);
    free(key);
    return rc;
  }
  rc = lc_outbox_transaction_track_fresh_outbox(
      transaction, key, effective_entry.effect_key, error);
  if (rc != LC_OK) {
    lc_outbox_transaction_abort_enrolled_lease(transaction, lease);
    free(key);
    return rc;
  }
#ifdef LOCKDC_TEST_BUILD
  if (lc_outbox_test_before_outbox_receipt_copy_hook != NULL) {
    rc = lc_outbox_test_before_outbox_receipt_copy_hook(
        lc_outbox_test_before_outbox_receipt_copy_context, error);
    if (rc != LC_OK) {
      lc_outbox_transaction_abort_enrolled_lease(transaction, lease);
      free(key);
      return rc;
    }
  }
#endif
  free(key);
  return LC_OK;
}

static int lc_outbox_transaction_terminal(lc_outbox_transaction *self,
                                          int rollback,
                                          lc_outbox_commit_result *out,
                                          lc_error *error) {
  lc_outbox_transaction_handle *transaction =
      (lc_outbox_transaction_handle *)self;
  lc_release_req request;
  lc_txn_replay_req replay_request;
  lc_txn_replay_res replay_result;
  char **prepared_notification_keys;
  lc_outbox_commit_result prepared_result;
  char *pouch_txn_id;
  size_t i;
  int rc;
  if (transaction == NULL || transaction->terminal)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox transaction is closed", NULL, NULL, NULL);
  if (!rollback && transaction->lease_count == 0U)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "empty outbox transaction cannot commit", NULL, NULL,
                        NULL);
  if (transaction->terminal_vote_started &&
      transaction->terminal_rollback != rollback) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox transaction decision is already in progress",
                        NULL, NULL, NULL);
  }
  rc = LC_OK;
  memset(&prepared_result, 0, sizeof(prepared_result));
  prepared_notification_keys = NULL;
  pouch_txn_id = NULL;
  /* A Pouch transaction reports an individual release as successful after it
   * casts its vote. Outbox transactions deliberately use an explicit xid
   * even for one participant, so replay the durable decision after every
   * commit. This prevents an expired single-participant transaction from
   * reporting success or publishing an outbox handoff after Pouch rolled it
   * back. */
  if (!rollback && transaction->outbox->client->is_pouch &&
      transaction->lease_count > 0U) {
    const char *txn_id = NULL;

    for (i = 0U; i < transaction->lease_count; ++i) {
      if (transaction->leases[i] != NULL) {
        txn_id = transaction->leases[i]->txn_id;
        break;
      }
    }

    if (txn_id == NULL || txn_id[0] == '\0') {
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "Pouch outbox transaction is missing its xid", NULL,
                          NULL, NULL);
    }
    pouch_txn_id = lc_client_strdup(transaction->outbox->client, txn_id);
    if (pouch_txn_id == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to preserve Pouch outbox xid", NULL, NULL,
                          NULL);
    }
  }
  if (!rollback) {
    prepared_notification_keys = (char **)lc_client_calloc(
        transaction->outbox->client, transaction->lease_count,
        sizeof(*prepared_notification_keys));
    if (prepared_notification_keys == NULL) {
      lc_client_free(transaction->outbox->client, pouch_txn_id);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to prepare outbox dispatch signals", NULL,
                          NULL, NULL);
    }
    rc = lc_outbox_transaction_reserve_notification_keys(
        transaction, transaction->lease_count, error);
    if (rc != LC_OK) {
      lc_client_free(transaction->outbox->client, prepared_notification_keys);
      lc_client_free(transaction->outbox->client, pouch_txn_id);
      return rc;
    }
    for (i = 0U; i < transaction->lease_count; ++i) {
      if (transaction->leases[i] != NULL &&
          lc_outbox_is_outbox_key(transaction->leases[i]->key)) {
        prepared_notification_keys[i] = lc_client_strdup(
            transaction->outbox->client, transaction->leases[i]->key);
        if (prepared_notification_keys[i] == NULL) {
          while (i > 0U) {
            --i;
            lc_client_free(transaction->outbox->client,
                           prepared_notification_keys[i]);
          }
          lc_client_free(transaction->outbox->client,
                         prepared_notification_keys);
          lc_client_free(transaction->outbox->client, pouch_txn_id);
          return lc_error_set(error, LC_ERR_NOMEM, 0L,
                              "failed to prepare outbox dispatch signals", NULL,
                              NULL, NULL);
        }
      }
    }
    /* Receipt storage is intentionally prepared before the first terminal
     * vote. Once the backend confirms commit, publication cannot fail because
     * of a local allocation. */
    if (transaction->fresh_outbox_count > 0U) {
      prepared_result.outbox_receipts =
          (lc_outbox_receipt *)calloc(transaction->fresh_outbox_count,
                                      sizeof(*prepared_result.outbox_receipts));
      if (prepared_result.outbox_receipts == NULL) {
        lc_outbox_transaction_free_prepared_notification_keys(
            transaction, prepared_notification_keys);
        lc_client_free(transaction->outbox->client, pouch_txn_id);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to prepare outbox commit receipts", NULL,
                            NULL, NULL);
      }
      for (i = 0U; i < transaction->fresh_outbox_count; ++i) {
        ++prepared_result.outbox_receipt_count;
        prepared_result.outbox_receipts[i].outbox_key =
            lc_strdup_local(transaction->fresh_outbox_keys[i]);
        prepared_result.outbox_receipts[i].effect_key =
            lc_strdup_local(transaction->fresh_outbox_effect_keys[i]);
        if (prepared_result.outbox_receipts[i].outbox_key == NULL ||
            prepared_result.outbox_receipts[i].effect_key == NULL) {
          lc_outbox_commit_result_cleanup(&prepared_result);
          lc_outbox_transaction_free_prepared_notification_keys(
              transaction, prepared_notification_keys);
          lc_client_free(transaction->outbox->client, pouch_txn_id);
          return lc_error_set(error, LC_ERR_NOMEM, 0L,
                              "failed to prepare outbox commit receipts", NULL,
                              NULL, NULL);
        }
      }
    }
  }
  lc_release_req_init(&request);
  request.rollback = rollback;
  for (i = 0U; i < transaction->lease_count; ++i) {
    if (transaction->leases[i] == NULL)
      continue;
#ifdef LOCKDC_TEST_BUILD
    if (lc_outbox_test_before_transaction_terminal_release_hook != NULL) {
      rc = lc_outbox_test_before_transaction_terminal_release_hook(
          lc_outbox_test_before_transaction_terminal_release_context, error);
      if (rc != LC_OK) {
        lc_outbox_transaction_free_prepared_notification_keys(
            transaction, prepared_notification_keys);
        lc_outbox_commit_result_cleanup(&prepared_result);
        lc_client_free(transaction->outbox->client, pouch_txn_id);
        return rc;
      }
    }
#endif
    /* The request may reach the resource manager even if its response never
     * reaches us. Freeze the decision immediately before the first request;
     * the pre-request hook above remains outside this indeterminate boundary.
     */
    if (!transaction->terminal_vote_started) {
      transaction->terminal_vote_started = 1;
      transaction->terminal_rollback = rollback;
      lc_outbox_transaction_invalidate_participants(transaction);
    }
#ifdef LOCKDC_TEST_BUILD
    if (lc_outbox_test_after_transaction_terminal_decision_hook != NULL) {
      rc = lc_outbox_test_after_transaction_terminal_decision_hook(
          lc_outbox_test_after_transaction_terminal_decision_context, error);
      if (rc != LC_OK) {
        lc_outbox_transaction_free_prepared_notification_keys(
            transaction, prepared_notification_keys);
        lc_outbox_commit_result_cleanup(&prepared_result);
        lc_client_free(transaction->outbox->client, pouch_txn_id);
        return rc;
      }
    }
#endif
    rc = lc_lease_release(transaction->leases[i], &request, error);
    if (rc != LC_OK) {
      lc_outbox_transaction_free_prepared_notification_keys(
          transaction, prepared_notification_keys);
      lc_outbox_commit_result_cleanup(&prepared_result);
      lc_client_free(transaction->outbox->client, pouch_txn_id);
      return rc;
    }
    if (prepared_notification_keys != NULL &&
        prepared_notification_keys[i] != NULL) {
      transaction->notification_keys[transaction->notification_count++] =
          prepared_notification_keys[i];
      prepared_notification_keys[i] = NULL;
    }
    lc_outbox_transaction_clear_command(transaction, transaction->leases[i]);
    transaction->leases[i] = NULL;
  }
  lc_client_free(transaction->outbox->client, prepared_notification_keys);
  transaction->terminal = 1;
  if (pouch_txn_id != NULL) {
    lc_txn_replay_req_init(&replay_request);
    memset(&replay_result, 0, sizeof(replay_result));
    replay_request.txn_id = pouch_txn_id;
    rc = lc_txn_replay(&transaction->outbox->client->pub, &replay_request,
                       &replay_result, error);
    if (rc != LC_OK) {
      /* Every lease has already cast its terminal vote. A failed replay is
       * therefore indeterminate, not proof of rollback: retain the direct
       * outbox handoff so a locally committed Pouch transaction cannot wait
       * for restart or an opt-in reconciliation interval. `notify()` falls
       * back to durable recovery if its bounded handoff cannot retain a key. */
      for (i = 0U; i < transaction->notification_count; ++i)
        lc_outbox_publish_committed_outbox(transaction->outbox,
                                           transaction->notification_keys[i]);
      lc_txn_replay_res_cleanup(&replay_result);
      lc_client_free(transaction->outbox->client, pouch_txn_id);
      lc_outbox_transaction_clear_notification_keys(transaction);
      lc_outbox_commit_result_cleanup(&prepared_result);
      return rc;
    }
    if (replay_result.state == NULL ||
        strcmp(replay_result.state, "commit") != 0) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox transaction was rolled back before commit",
                        replay_result.state == NULL
                            ? "Pouch transaction has no terminal state"
                            : replay_result.state,
                        NULL, NULL);
    }
    lc_txn_replay_res_cleanup(&replay_result);
    lc_client_free(transaction->outbox->client, pouch_txn_id);
    if (rc != LC_OK) {
      lc_outbox_transaction_clear_notification_keys(transaction);
      lc_outbox_commit_result_cleanup(&prepared_result);
      return rc;
    }
  }
  if (!rollback) {
    for (i = 0U; i < transaction->notification_count; ++i) {
      lc_outbox_publish_committed_outbox(transaction->outbox,
                                         transaction->notification_keys[i]);
      lc_client_free(transaction->outbox->client,
                     transaction->notification_keys[i]);
    }
    lc_client_free(transaction->outbox->client, transaction->notification_keys);
    transaction->notification_keys = NULL;
    transaction->notification_count = 0U;
    transaction->notification_capacity = 0U;
    if (out != NULL) {
      lc_outbox_commit_result_cleanup(out);
      *out = prepared_result;
      memset(&prepared_result, 0, sizeof(prepared_result));
    }
  } else {
    lc_outbox_transaction_clear_notification_keys(transaction);
  }
  lc_outbox_commit_result_cleanup(&prepared_result);
  return LC_OK;
}
static int lc_outbox_transaction_commit_method(lc_outbox_transaction *self,
                                               lc_outbox_commit_result *out,
                                               lc_error *error) {
  if (out == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox commit result is required", NULL, NULL, NULL);
  return lc_outbox_transaction_terminal(self, 0, out, error);
}
static int lc_outbox_transaction_rollback_method(lc_outbox_transaction *self,
                                                 lc_error *error) {
  return lc_outbox_transaction_terminal(self, 1, NULL, error);
}
static void lc_outbox_transaction_close_method(lc_outbox_transaction *self) {
  lc_outbox_transaction_handle *transaction =
      (lc_outbox_transaction_handle *)self;
  lc_outbox_handle *outbox;
  size_t i;
  if (transaction == NULL)
    return;
  outbox = transaction->outbox;
  if (!transaction->terminal)
    (void)lc_outbox_transaction_terminal(
        self,
        transaction->terminal_vote_started ? transaction->terminal_rollback : 1,
        NULL, NULL);
  lc_outbox_transaction_invalidate_participants(transaction);
  for (i = 0U; i < transaction->lease_count; ++i)
    lc_lease_close(transaction->leases[i]);
  lc_outbox_transaction_clear_notification_keys(transaction);
  lc_outbox_transaction_clear_fresh_outboxes(transaction);
  lc_client_free(outbox->client, transaction->leases);
  lc_client_free(outbox->client, transaction->causation_id);
  lc_client_free(outbox->client, transaction->command_id);
  lc_client_free(outbox->client, transaction);
  lc_outbox_release(outbox);
}

static lc_outbox_transaction *
lc_outbox_transaction_new(lc_outbox_handle *outbox, lc_lease *first,
                          lc_error *error) {
  lc_outbox_transaction_handle *transaction;
  transaction = (lc_outbox_transaction_handle *)lc_client_calloc(
      outbox->client, 1U, sizeof(*transaction));
  if (transaction == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate outbox transaction", NULL, NULL, NULL);
    return NULL;
  }
  transaction->outbox = outbox;
  lc_outbox_retain(outbox);
  transaction->pub.accept_command = lc_outbox_transaction_accept_command_method;
  transaction->pub.accept_inbox = lc_outbox_transaction_accept_inbox_method;
  transaction->pub.acquire = lc_outbox_transaction_acquire_method;
  transaction->pub.append = lc_outbox_transaction_append_method;
  transaction->pub.complete_command =
      lc_outbox_transaction_complete_command_method;
  transaction->pub.fail_command = lc_outbox_transaction_fail_command_method;
  transaction->pub.commit = lc_outbox_transaction_commit_method;
  transaction->pub.rollback = lc_outbox_transaction_rollback_method;
  transaction->pub.close = lc_outbox_transaction_close_method;
  if (first != NULL &&
      lc_outbox_transaction_add_lease(transaction, first, error) != LC_OK) {
    lc_client_free(outbox->client, transaction);
    lc_outbox_release(outbox);
    return NULL;
  }
  return &transaction->pub;
}

static int lc_outbox_begin_method(lc_outbox *self, lc_outbox_transaction **out,
                                  lc_error *error) {
  lc_outbox_handle *outbox = (lc_outbox_handle *)self;
  lc_outbox_transaction *transaction;

  if (outbox == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox and transaction output are required", NULL,
                        NULL, NULL);
  }
  *out = NULL;
  transaction = lc_outbox_transaction_new(outbox, NULL, error);
  if (transaction == NULL)
    return error != NULL ? error->code : LC_ERR_NOMEM;
  *out = transaction;
  return LC_OK;
}

static int
lc_outbox_append_method(lc_outbox *self, const lc_outbox_entry *entry,
                        lc_source *payload, lc_outbox_transaction **out_txn,
                        lc_outbox_receipt *receipt, lc_error *error) {
  lc_outbox_handle *outbox = (lc_outbox_handle *)self;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_outbox_transaction *transaction;
  char *key;
  char minted_txn_id[LC_XID_STRING_SIZE];
  int rc;
  if (outbox == NULL || out_txn == NULL || receipt == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox, transaction output, and receipt are required",
                        NULL, NULL, NULL);
  *out_txn = NULL;
  lc_outbox_receipt_cleanup(receipt);
  rc = lc_outbox_validate_outbox_envelope(entry, error);
  if (rc != LC_OK)
    return rc;
  key = NULL;
  rc = lc_outbox_key(outbox, entry, &key, error);
  if (rc != LC_OK)
    return rc;
  lc_acquire_req_init(&acquire);
  acquire.ns = outbox->ns;
  acquire.key = key;
  acquire.owner = outbox->owner;
  acquire.ttl_seconds = outbox->transaction_ttl_seconds;
  acquire.if_not_exists = 1;
  rc = lc_xid_new(minted_txn_id, error);
  if (rc != LC_OK) {
    free(key);
    return rc;
  }
  acquire.txn_id = minted_txn_id;
  lease = NULL;
  rc = lc_acquire(&outbox->client->pub, &acquire, &lease, error);
  if (rc != LC_OK) {
    lc_lease *duplicate_lease;

    duplicate_lease = NULL;
    rc = lc_outbox_probe_duplicate_barrier(outbox, key, rc, &duplicate_lease,
                                           error);
    if (rc == LC_OK) {
      rc = lc_outbox_existing_outbox(outbox, duplicate_lease, key, entry,
                                     receipt, error);
      lc_outbox_rollback_lease(duplicate_lease);
    }
    free(key);
    return rc;
  }
  rc = lc_outbox_stage_outbox(lease, entry, NULL, payload, error);
  if (rc != LC_OK) {
    lc_outbox_rollback_lease(lease);
    free(key);
    return rc;
  }
  transaction = lc_outbox_transaction_new(outbox, lease, error);
  if (transaction == NULL) {
    lc_outbox_rollback_lease(lease);
    free(key);
    return error != NULL ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_outbox_transaction_track_fresh_outbox(
      (lc_outbox_transaction_handle *)transaction, key, entry->effect_key,
      error);
  if (rc != LC_OK) {
    transaction->close(transaction);
    free(key);
    return rc;
  }
  free(key);
  *out_txn = transaction;
  return LC_OK;
}

static int lc_outbox_accept_inbox_method(lc_outbox *self,
                                         const lc_inbox_message *message,
                                         lc_outbox_transaction **out_txn,
                                         lc_inbox_accept_result *result,
                                         lc_error *error) {
  lc_outbox_handle *outbox = (lc_outbox_handle *)self;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_outbox_transaction *transaction;
  char *key;
  char minted_txn_id[LC_XID_STRING_SIZE];
  int rc;
  if (outbox == NULL || out_txn == NULL || result == NULL)
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "outbox, transaction output, and inbox result are required", NULL, NULL,
        NULL);
  *out_txn = NULL;
  memset(result, 0, sizeof(*result));
  rc = lc_outbox_validate_inbox_message(outbox->client, message, error);
  if (rc != LC_OK)
    return rc;
  key = NULL;
  rc = lc_outbox_inbox_key(outbox, message, &key, error);
  if (rc != LC_OK)
    return rc;
  lc_acquire_req_init(&acquire);
  acquire.ns = outbox->ns;
  acquire.key = key;
  acquire.owner = outbox->owner;
  acquire.ttl_seconds = outbox->transaction_ttl_seconds;
  acquire.if_not_exists = 1;
  rc = lc_xid_new(minted_txn_id, error);
  if (rc != LC_OK) {
    free(key);
    return rc;
  }
  acquire.txn_id = minted_txn_id;
  lease = NULL;
  rc = lc_acquire(&outbox->client->pub, &acquire, &lease, error);
  if (rc != LC_OK) {
    lc_lease *duplicate_lease;

    duplicate_lease = NULL;
    rc = lc_outbox_probe_duplicate_barrier(outbox, key, rc, &duplicate_lease,
                                           error);
    if (rc == LC_OK) {
      rc = lc_outbox_existing_inbox(outbox, duplicate_lease, key, message,
                                    result, error);
      lc_outbox_rollback_lease(duplicate_lease);
    }
    free(key);
    return rc;
  }
  free(key);
  rc = lc_outbox_stage_inbox(outbox->client, lease, message, error);
  if (rc != LC_OK) {
    lc_outbox_rollback_lease(lease);
    return rc;
  }
  transaction = lc_outbox_transaction_new(outbox, lease, error);
  if (transaction == NULL) {
    lc_outbox_rollback_lease(lease);
    return error != NULL ? error->code : LC_ERR_NOMEM;
  }
  ((lc_outbox_transaction_handle *)transaction)->causation_id =
      lc_client_strdup(outbox->client, message->message_id);
  if (((lc_outbox_transaction_handle *)transaction)->causation_id == NULL) {
    transaction->close(transaction);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to retain inbox causation identity", NULL, NULL,
                        NULL);
  }
  result->accepted = 1;
  *out_txn = transaction;
  return LC_OK;
}

static int lc_outbox_accept_command_method(lc_outbox *self,
                                           const lc_command_request *request,
                                           lc_outbox_transaction **out_txn,
                                           lc_command_receipt *receipt,
                                           lc_error *error) {
  lc_outbox_handle *outbox = (lc_outbox_handle *)self;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_outbox_transaction *transaction;
  lc_outbox_transaction_handle *handle;
  lc_outbox_command_record record;
  char *key;
  char command_id[48];
  char minted_txn_id[LC_XID_STRING_SIZE];
  lc_command_request effective_request;
  char generated_key[LC_XID_STRING_SIZE];
  int rc;

  if (outbox == NULL || request == NULL || out_txn == NULL || receipt == NULL ||
      request->request_digest == NULL || request->request_digest[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox, command request, digest, transaction "
                        "output, and receipt are required",
                        NULL, NULL, NULL);
  }
  *out_txn = NULL;
  lc_command_receipt_cleanup(receipt);
  rc = lc_outbox_effective_command_request(request, &effective_request,
                                           generated_key, error);
  if (rc != LC_OK)
    return rc;
  request = &effective_request;
  rc = lc_outbox_validate_command_request(outbox->client, request, error);
  if (rc != LC_OK)
    return rc;
  key = NULL;
  rc = lc_outbox_command_key(&request->identity, &key, command_id, error);
  if (rc != LC_OK)
    return rc;
  lc_acquire_req_init(&acquire);
  acquire.ns = outbox->ns;
  acquire.key = key;
  acquire.owner = outbox->owner;
  acquire.ttl_seconds = outbox->transaction_ttl_seconds;
  acquire.if_not_exists = 1;
  rc = lc_xid_new(minted_txn_id, error);
  if (rc != LC_OK) {
    free(key);
    return rc;
  }
  acquire.txn_id = minted_txn_id;
  lease = NULL;
  rc = lc_acquire(&outbox->client->pub, &acquire, &lease, error);
  if (rc != LC_OK) {
    lc_lease *duplicate_lease;

    duplicate_lease = NULL;
    rc = lc_outbox_probe_duplicate_barrier(outbox, key, rc, &duplicate_lease,
                                           error);
    if (rc == LC_OK) {
      rc = lc_outbox_existing_command(outbox, duplicate_lease, key,
                                      &request->identity, command_id, request,
                                      receipt, error);
      lc_outbox_rollback_lease(duplicate_lease);
    }
    free(key);
    return rc;
  }
  rc = lc_outbox_stage_command(outbox->client, lease, request, command_id,
                               error);
  if (rc != LC_OK) {
    lc_outbox_rollback_lease(lease);
    free(key);
    return rc;
  }
  transaction = lc_outbox_transaction_new(outbox, lease, error);
  if (transaction == NULL) {
    lc_outbox_rollback_lease(lease);
    free(key);
    return error != NULL ? error->code : LC_ERR_NOMEM;
  }
  handle = (lc_outbox_transaction_handle *)transaction;
  rc = lc_outbox_transaction_set_command(handle, lease, command_id, error);
  if (rc != LC_OK) {
    transaction->close(transaction);
    free(key);
    return rc;
  }
  memset(&record, 0, sizeof(record));
  record.record_type = "lockdc.command.v1";
  record.command_id = command_id;
  record.scope = (char *)request->identity.scope;
  record.command_type = (char *)request->identity.command_type;
  record.idempotency_key = (char *)request->identity.idempotency_key;
  record.request_digest = (char *)request->request_digest;
  record.accepted_at_unix = 1;
  record.operation_id = (char *)request->operation_id;
  record.state = "pending";
  rc = lc_outbox_command_receipt_from_record(&record, receipt, error);
  free(key);
  if (rc != LC_OK) {
    transaction->close(transaction);
    return rc;
  }
  *out_txn = transaction;
  return LC_OK;
}

static int lc_outbox_get_command_receipt_method(
    lc_outbox *self, const lc_command_identity *identity,
    lc_command_receipt *receipt, lc_error *error) {
  lc_outbox_handle *outbox = (lc_outbox_handle *)self;
  char *key;
  char command_id[48];
  int rc;

  if (outbox == NULL || receipt == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox and command receipt output are required", NULL,
                        NULL, NULL);
  lc_command_receipt_cleanup(receipt);
  key = NULL;
  rc = lc_outbox_command_key(identity, &key, command_id, error);
  if (rc == LC_OK)
    rc = lc_outbox_existing_command(outbox, NULL, key, identity, command_id,
                                    NULL, receipt, error);
  free(key);
  return rc;
}

static int lc_outbox_get_command_receipt_by_id_method(
    lc_outbox *self, const char *command_id, lc_command_receipt *receipt,
    lc_error *error) {
  lc_outbox_handle *outbox = (lc_outbox_handle *)self;
  lc_outbox_command_record record;
  lc_get_opts options;
  lc_get_res result;
  lonejson *runtime;
  char *key;
  int rc;

  if (outbox == NULL || receipt == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox and command receipt output are required", NULL,
                        NULL, NULL);
  lc_command_receipt_cleanup(receipt);
  memset(&record, 0, sizeof(record));
  memset(&result, 0, sizeof(result));
  runtime = NULL;
  key = NULL;
  rc = lc_outbox_command_key_from_id(command_id, &key, error);
  if (rc != LC_OK)
    return rc;
  rc = lc_outbox_require_json_runtime(outbox->client, &runtime, error);
  if (rc == LC_OK) {
    lc_get_opts_init(&options);
    options.public_read = 1;
    rc = lc_load_in_namespace(&outbox->client->pub, outbox->ns, key,
                              &lc_outbox_command_record_map, &record, &options,
                              &result, error);
  }
  if (rc == LC_OK && result.no_content) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "command receipt does not exist", NULL, NULL, NULL);
  }
  if (rc == LC_OK && (record.command_id == NULL ||
                      strcmp(record.command_id, command_id) != 0)) {
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "command receipt does not match command id", NULL, NULL,
                      NULL);
  }
  if (rc == LC_OK)
    rc = lc_outbox_command_receipt_from_record(&record, receipt, error);
  if (runtime != NULL)
    runtime->cleanup(runtime, &lc_outbox_command_record_map, &record);
  lc_get_res_cleanup(&result);
  free(key);
  return rc;
}

static int lc_outbox_wait_command_method(lc_outbox *self,
                                         const char *command_id,
                                         long timeout_ms,
                                         lc_command_receipt *receipt,
                                         lc_error *error) {
  struct timespec started;
  struct timespec now;
  long elapsed_ms;
  long delay_ms;
  int rc;

  if (self == NULL || receipt == NULL || timeout_ms < -1L)
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "outbox, receipt output, and valid timeout are required", NULL, NULL,
        NULL);
  if (clock_gettime(CLOCK_MONOTONIC, &started) != 0)
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "failed to read command wait clock", NULL, NULL, NULL);
  for (;;) {
    rc = lc_outbox_get_command_receipt_by_id_method(self, command_id, receipt,
                                                    error);
    if (rc != LC_OK || receipt->state != LC_COMMAND_PENDING)
      return rc;
    if (timeout_ms == 0L)
      return lc_error_set(error, LC_ERR_TIMEOUT, 0L, "command remains pending",
                          NULL, NULL, NULL);
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "failed to read command wait clock", NULL, NULL,
                          NULL);
    elapsed_ms = (long)((now.tv_sec - started.tv_sec) * 1000L +
                        (now.tv_nsec - started.tv_nsec) / 1000000L);
    if (timeout_ms > 0L && elapsed_ms >= timeout_ms)
      return lc_error_set(error, LC_ERR_TIMEOUT, 0L, "command remains pending",
                          NULL, NULL, NULL);
    delay_ms = 10L;
    if (timeout_ms > 0L && timeout_ms - elapsed_ms < delay_ms)
      delay_ms = timeout_ms - elapsed_ms;
    if (delay_ms > 0L) {
      struct timespec delay;

      delay.tv_sec = delay_ms / 1000L;
      delay.tv_nsec = (delay_ms % 1000L) * 1000000L;
      (void)nanosleep(&delay, NULL);
    }
  }
}

static int lc_outbox_write_command_result_method(
    lc_outbox *self, const lc_command_identity *identity, lc_sink *dst,
    size_t *written, lc_error *error) {
  lc_outbox_handle *outbox = (lc_outbox_handle *)self;
  lc_attachment_get_op request;
  lc_attachment_get_res result;
  char *key;
  char command_id[48];
  int rc;

  if (outbox == NULL || dst == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox and command result sink are required", NULL,
                        NULL, NULL);
  key = NULL;
  rc = lc_outbox_command_key(identity, &key, command_id, error);
  if (rc != LC_OK)
    return rc;
  lc_attachment_get_op_init(&request);
  memset(&result, 0, sizeof(result));
  request.lease.ns = outbox->ns;
  request.lease.key = key;
  request.selector.name = "result";
  request.public_read = 1;
  rc = lc_get_attachment(&outbox->client->pub, &request, dst, &result, error);
  if (rc == LC_OK && written != NULL)
    *written = (size_t)result.attachment.size;
  lc_attachment_get_res_cleanup(&result);
  free(key);
  return rc;
}

static int lc_outbox_resume_command_method(lc_outbox *self,
                                           const lc_command_identity *identity,
                                           lc_outbox_transaction **out_txn,
                                           lc_command_receipt *receipt,
                                           lc_error *error) {
  lc_outbox_handle *outbox = (lc_outbox_handle *)self;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_outbox_transaction *transaction;
  lc_outbox_transaction_handle *handle;
  lc_outbox_command_record record;
  lc_get_res load_result;
  lonejson *runtime;
  char *key;
  char command_id[48];
  char minted_txn_id[LC_XID_STRING_SIZE];
  int rc;

  if (outbox == NULL || out_txn == NULL || receipt == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox, transaction output, and receipt are required",
                        NULL, NULL, NULL);
  *out_txn = NULL;
  lc_command_receipt_cleanup(receipt);
  key = NULL;
  rc = lc_outbox_command_key(identity, &key, command_id, error);
  if (rc != LC_OK)
    return rc;
  rc = lc_outbox_existing_command(outbox, NULL, key, identity, command_id, NULL,
                                  receipt, error);
  if (rc != LC_OK || receipt->state != LC_COMMAND_PENDING) {
    free(key);
    return rc;
  }
  lc_acquire_req_init(&acquire);
  acquire.ns = outbox->ns;
  acquire.key = key;
  acquire.owner = outbox->owner;
  acquire.ttl_seconds = outbox->transaction_ttl_seconds;
  rc = lc_xid_new(minted_txn_id, error);
  if (rc != LC_OK) {
    free(key);
    return rc;
  }
  acquire.txn_id = minted_txn_id;
  lease = NULL;
  rc = lc_acquire(&outbox->client->pub, &acquire, &lease, error);
  if (rc != LC_OK) {
    free(key);
    return rc;
  }
  memset(&record, 0, sizeof(record));
  memset(&load_result, 0, sizeof(load_result));
  rc = lc_outbox_require_json_runtime(outbox->client, &runtime, error);
  if (rc != LC_OK) {
    lc_outbox_rollback_lease(lease);
    free(key);
    return rc;
  }
  rc = lc_lease_load(lease, &lc_outbox_command_record_map, &record, NULL,
                     &load_result, error);
  if (rc == LC_OK &&
      (record.state == NULL || strcmp(record.state, "pending") != 0)) {
    rc = lc_outbox_command_receipt_from_record(&record, receipt, error);
    if (rc == LC_OK)
      receipt->duplicate = 1;
  }
  runtime->cleanup(runtime, &lc_outbox_command_record_map, &record);
  lc_get_res_cleanup(&load_result);
  if (rc != LC_OK || receipt->state != LC_COMMAND_PENDING) {
    lc_outbox_rollback_lease(lease);
    free(key);
    return rc;
  }
  transaction = lc_outbox_transaction_new(outbox, lease, error);
  if (transaction == NULL) {
    lc_outbox_rollback_lease(lease);
    free(key);
    return error != NULL ? error->code : LC_ERR_NOMEM;
  }
  handle = (lc_outbox_transaction_handle *)transaction;
  rc = lc_outbox_transaction_set_command(handle, lease, command_id, error);
  free(key);
  if (rc != LC_OK) {
    transaction->close(transaction);
    return rc;
  }
  *out_txn = transaction;
  return LC_OK;
}

static int lc_outbox_resume_command_by_id_method(
    lc_outbox *self, const char *command_id, lc_outbox_transaction **out_txn,
    lc_command_receipt *receipt, lc_error *error) {
  lc_command_receipt current;
  lc_command_identity identity;
  int rc;

  if (out_txn == NULL || receipt == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "transaction and command receipt outputs are required",
                        NULL, NULL, NULL);
  lc_command_receipt_init(&current);
  rc = lc_outbox_get_command_receipt_by_id_method(self, command_id, &current,
                                                  error);
  if (rc != LC_OK) {
    lc_command_receipt_cleanup(&current);
    return rc;
  }
  identity.scope = current.scope;
  identity.command_type = current.command_type;
  identity.idempotency_key = current.idempotency_key;
  rc =
      lc_outbox_resume_command_method(self, &identity, out_txn, receipt, error);
  lc_command_receipt_cleanup(&current);
  return rc;
}

static int lc_outbox_next_method(lc_outbox *self, long timeout_ms,
                                 lc_outbox_job **out, lc_error *error) {
  lc_outbox_handle *outbox = (lc_outbox_handle *)self;
  int rc;

  if (outbox == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox and job output are required", NULL, NULL, NULL);
  }
  /* A blocked next() caller is part of the outbox's lifecycle. close()
   * wakes it, but must not destroy the wait primitives until this call has
   * returned to its host thread. */
  lc_outbox_retain(outbox);
  rc = lc_outbox_wait_for_ready(outbox, timeout_ms, out, error);
#ifdef LOCKDC_TEST_BUILD
  if (lc_outbox_test_before_next_release_hook != NULL) {
    lc_outbox_test_before_next_release_hook(
        lc_outbox_test_before_next_release_context);
  }
#endif
  lc_outbox_release(outbox);
  return rc;
}

static int lc_outbox_get_stats_method(lc_outbox *self, lc_outbox_stats *out,
                                      lc_error *error) {
  lc_outbox_handle *outbox = (lc_outbox_handle *)self;
  int had_last_error;

  if (outbox == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox and stats output are required", NULL, NULL,
                        NULL);
  }
  lc_outbox_stats_cleanup(out);
  pthread_mutex_lock(&outbox->notification_mutex);
  out->running = outbox->closed ? 0 : 1;
  out->pending_candidates = outbox->notification_count;
  out->delayed_wakes = outbox->delayed_notification_count;
  out->waiting_consumers = outbox->waiting_consumers;
  out->direct_notifications = outbox->direct_notifications;
  out->notification_overflows = outbox->notification_overflows;
  out->recovery_queries = outbox->recovery_queries;
  out->recovered_claims = outbox->recovered_claims;
  out->claim_losses = outbox->claim_losses;
  out->payload_open_failures = outbox->payload_open_failures;
  had_last_error = outbox->last_error != NULL;
  out->last_error = lc_strdup_local(outbox->last_error);
  pthread_mutex_unlock(&outbox->notification_mutex);
  if (had_last_error && out->last_error == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy outbox last error", NULL, NULL, NULL);
  }
  return LC_OK;
}

static int lc_outbox_reconcile_method(lc_outbox *self, lc_error *error) {
  lc_outbox_handle *outbox = (lc_outbox_handle *)self;

  if (outbox == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, "outbox is required", NULL,
                        NULL, NULL);
  }
  pthread_mutex_lock(&outbox->notification_mutex);
  if (outbox->closed) {
    pthread_mutex_unlock(&outbox->notification_mutex);
    return lc_error_set(error, LC_ERR_INVALID, 0L, "outbox is closed", NULL,
                        NULL, NULL);
  }
  outbox->recovery_needed = 1;
  outbox->recovery_immediate = 1;
  outbox->recovery_claims_pending = 1;
  lc_outbox_signal_dispatcher_locked(outbox);
  pthread_mutex_unlock(&outbox->notification_mutex);
  return LC_OK;
}

static int
lc_outbox_open_dead_letter(lc_outbox_handle *outbox, lc_client_handle *client,
                           const char *outbox_key, lc_lease **lease_out,
                           lc_outbox_record *record, lc_error *error) {
  lc_acquire_req acquire;
  lc_get_res result;
  lonejson *runtime;
  int rc;

  if (outbox == NULL || client == NULL || outbox_key == NULL ||
      outbox_key[0] == '\0' || lease_out == NULL || record == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox dead-letter key and outputs are required", NULL,
                        NULL, NULL);
  }
  if (!lc_outbox_is_outbox_key(outbox_key)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox dead-letter key is not an outbox key", NULL,
                        NULL, NULL);
  }
  *lease_out = NULL;
  memset(record, 0, sizeof(*record));
  memset(&result, 0, sizeof(result));
  lc_acquire_req_init(&acquire);
  acquire.ns = outbox->ns;
  acquire.key = outbox_key;
  acquire.owner = outbox->owner;
  acquire.ttl_seconds = outbox->transaction_ttl_seconds;
  rc = lc_acquire(&client->pub, &acquire, lease_out, error);
  if (rc != LC_OK)
    return rc;
  rc = lc_outbox_require_json_runtime(client, &runtime, error);
  if (rc != LC_OK) {
    lc_outbox_rollback_lease(*lease_out);
    *lease_out = NULL;
    return rc;
  }
  rc = lc_lease_load(*lease_out, &lc_outbox_record_map, record, NULL, &result,
                     error);
  lc_get_res_cleanup(&result);
  if (rc == LC_OK &&
      (record->record_type == NULL || record->dispatch_state == NULL ||
       strcmp(record->record_type, "lockdc.outbox.v1") != 0 ||
       strcmp(record->dispatch_state, "dead_letter") != 0)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "outbox record is not a dead letter", NULL, NULL, NULL);
  }
  if (rc != LC_OK) {
    runtime->cleanup(runtime, &lc_outbox_record_map, record);
    lc_outbox_rollback_lease(*lease_out);
    *lease_out = NULL;
  }
  return rc;
}

static int lc_outbox_replay_dead_letter_on_client(lc_outbox_handle *outbox,
                                                  lc_client_handle *client,
                                                  const char *outbox_key,
                                                  lc_error *error) {
  lc_outbox_record record;
  lc_lease *lease;
  lc_release_req release;
  char *original_dispatch_state;
  char *original_last_error;
  char *original_prior_dead_letter_error;
  time_t now;
  int rc;

  if (outbox == NULL || client == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox dead-letter replay requires a client", NULL,
                        NULL, NULL);
  }
#ifdef LOCKDC_TEST_BUILD
  if (lc_outbox_test_dead_letter_replay_client_hook != NULL) {
    lc_outbox_test_dead_letter_replay_client_hook(
        &client->pub,
        outbox->dispatcher_client == NULL ? NULL
                                          : &outbox->dispatcher_client->pub,
        lc_outbox_test_dead_letter_replay_client_context);
  }
#endif
  lease = NULL;
  rc = lc_outbox_open_dead_letter(outbox, client, outbox_key, &lease, &record,
                                  error);
  if (rc != LC_OK)
    return rc;
  rc = lc_outbox_validate_durable_outbox_record(&record, error);
  if (rc != LC_OK) {
    lc_outbox_record_loaded_clear(client, &record);
    lc_outbox_rollback_lease(lease);
    return rc;
  }
  if (record.replay_count < 0 ||
      record.replay_count >= (lonejson_int64)LC_I64_MAX) {
    lc_outbox_record_loaded_clear(client, &record);
    lc_outbox_rollback_lease(lease);
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "outbox dead-letter replay count is outside the supported range", NULL,
        NULL, NULL);
  }
  now = time(NULL);
  if (now == (time_t)-1) {
    lc_outbox_record_loaded_clear(client, &record);
    lc_outbox_rollback_lease(lease);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "failed to read outbox replay clock", NULL, NULL, NULL);
  }
  original_dispatch_state = record.dispatch_state;
  original_last_error = record.last_error;
  original_prior_dead_letter_error = record.prior_dead_letter_error;
  record.dispatch_state = "pending";
  record.last_error = "replayed";
  record.prior_dead_letter_error = original_last_error;
  record.attempt_count = 0;
  record.not_before_unix = 0;
  ++record.replay_count;
  record.replayed_at_unix = (lonejson_int64)now;
  rc = lc_lease_save(lease, &lc_outbox_record_map, &record, error);
  if (rc == LC_OK) {
    lc_release_req_init(&release);
    rc = lc_lease_release(lease, &release, error);
  }
  record.dispatch_state = original_dispatch_state;
  record.last_error = original_last_error;
  record.prior_dead_letter_error = original_prior_dead_letter_error;
  lc_outbox_record_loaded_clear(client, &record);
  if (rc != LC_OK) {
    lc_outbox_rollback_lease(lease);
    return rc;
  }
  lc_outbox_notify(outbox, outbox_key, 1);
  return LC_OK;
}

static int lc_outbox_replay_dead_letter_method(lc_outbox *self,
                                               const char *outbox_key,
                                               lc_error *error) {
  lc_outbox_handle *outbox = (lc_outbox_handle *)self;

  return lc_outbox_replay_dead_letter_on_client(
      outbox, outbox == NULL ? NULL : outbox->client, outbox_key, error);
}

typedef struct lc_outbox_dead_letter_replay_capture {
  lc_outbox_handle *outbox;
  char **keys;
  size_t key_capacity;
  char key[129];
  size_t length;
  size_t count;
} lc_outbox_dead_letter_replay_capture;

static int lc_outbox_dead_letter_replay_begin(void *context, lc_error *error) {
  lc_outbox_dead_letter_replay_capture *capture =
      (lc_outbox_dead_letter_replay_capture *)context;
  (void)error;
  capture->length = 0U;
  return 1;
}

static int lc_outbox_dead_letter_replay_chunk(void *context, const char *bytes,
                                              size_t length, lc_error *error) {
  lc_outbox_dead_letter_replay_capture *capture =
      (lc_outbox_dead_letter_replay_capture *)context;
  if (length > sizeof(capture->key) - 1U - capture->length) {
    (void)lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                       "dead-letter replay received an oversized key", NULL,
                       NULL, NULL);
    return 0;
  }
  memcpy(capture->key + capture->length, bytes, length);
  capture->length += length;
  return 1;
}

static int lc_outbox_dead_letter_replay_end(void *context, lc_error *error) {
  lc_outbox_dead_letter_replay_capture *capture =
      (lc_outbox_dead_letter_replay_capture *)context;

  capture->key[capture->length] = '\0';
  if (!lc_outbox_is_outbox_key(capture->key))
    return 1;
  if (capture->count == capture->key_capacity ||
      (capture->keys[capture->count] =
           lc_client_strdup(capture->outbox->client, capture->key)) == NULL) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to retain dead-letter replay key", NULL, NULL,
                       NULL);
    return 0;
  }
  ++capture->count;
  return 1;
}

static int lc_outbox_replay_dead_letters_on_startup(lc_outbox_handle *outbox,
                                                    lc_error *error) {
  static const char selector[] =
      "{\"eq\":{\"field\":\"/dispatch_state\",\"value\":\"dead_letter\"}}";
  lc_index_flush_req flush_request;
  lc_index_flush_res flush_result;
  lc_query_req request;
  lc_query_key_handler handler;
  lc_query_res result;
  lc_outbox_dead_letter_replay_capture capture;
  size_t index;
  int use_index;
  int rc;

  if (!outbox->startup_dead_letter_replay_pending)
    return LC_OK;
  use_index = outbox->dispatcher_client->pouch == NULL ||
              outbox->dispatcher_client->pouch->query_indexing_enabled;
  if (use_index && !outbox->startup_dead_letter_replay_flushed) {
    lc_index_flush_req_init(&flush_request);
    memset(&flush_result, 0, sizeof(flush_result));
    flush_request.ns = outbox->ns;
    flush_request.mode = "wait";
    rc = lc_flush_index(&outbox->dispatcher_client->pub, &flush_request,
                        &flush_result, error);
    lc_index_flush_res_cleanup(&flush_result);
    if (rc != LC_OK)
      return rc;
    outbox->startup_dead_letter_replay_flushed = 1;
  }
  lc_query_req_init(&request);
  memset(&handler, 0, sizeof(handler));
  memset(&result, 0, sizeof(result));
  memset(&capture, 0, sizeof(capture));
  capture.keys = (char **)lc_client_calloc(
      outbox->client, outbox->notification_capacity, sizeof(*capture.keys));
  if (capture.keys == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate dead-letter replay keys", NULL,
                        NULL, NULL);
  }
  capture.key_capacity = outbox->notification_capacity;
  request.ns = outbox->ns;
  request.selector_json = selector;
  request.limit = (long)outbox->notification_capacity;
  request.engine = use_index ? "index" : "scan";
  request.refresh = use_index ? "wait_for" : NULL;
  handler.begin = lc_outbox_dead_letter_replay_begin;
  handler.chunk = lc_outbox_dead_letter_replay_chunk;
  handler.end = lc_outbox_dead_letter_replay_end;
  capture.outbox = outbox;
  pthread_mutex_lock(&outbox->notification_mutex);
  ++outbox->recovery_queries;
  pthread_mutex_unlock(&outbox->notification_mutex);
  rc = lc_query_keys(&outbox->dispatcher_client->pub, &request, &handler,
                     &capture, &result, error);
  lc_query_res_cleanup(&result);
  for (index = 0U; rc == LC_OK && index < capture.count; ++index) {
    rc = lc_outbox_replay_dead_letter_on_client(
        outbox, outbox->dispatcher_client, capture.keys[index], error);
  }
  for (index = 0U; index < capture.count; ++index)
    lc_client_free(outbox->client, capture.keys[index]);
  lc_client_free(outbox->client, capture.keys);
  if (rc != LC_OK)
    return rc;
  if (capture.count == 0U)
    outbox->startup_dead_letter_replay_pending = 0;
  else {
    pthread_mutex_lock(&outbox->notification_mutex);
    outbox->recovery_needed = 1;
    outbox->recovery_immediate = 1;
    lc_outbox_signal_dispatcher_locked(outbox);
    pthread_mutex_unlock(&outbox->notification_mutex);
  }
  return LC_OK;
}

static int lc_outbox_delete_dead_letter_method(lc_outbox *self,
                                               const char *outbox_key,
                                               lc_error *error) {
  lc_outbox_handle *outbox = (lc_outbox_handle *)self;
  lc_outbox_record record;
  lc_lease *lease;
  lc_lease *deadline_lease;
  lc_remove_req remove;
  lc_release_req release;
  char *deadline_key;
  int deleted;
  int rc;

  lease = NULL;
  deadline_lease = NULL;
  deadline_key = NULL;
  rc = lc_outbox_open_dead_letter(outbox, outbox->client, outbox_key, &lease,
                                  &record, error);
  if (rc != LC_OK)
    return rc;
  /* The companion is only a recovery hint, but it must disappear before the
   * primary record is consumed. Otherwise a cleanup failure leaves callers no
   * retry path and can apply an old deadline to a recreated effect. */
#ifdef LOCKDC_TEST_BUILD
  if (lc_outbox_test_before_dead_letter_claim_cleanup_hook != NULL) {
    rc = lc_outbox_test_before_dead_letter_claim_cleanup_hook(
        lc_outbox_test_before_dead_letter_claim_cleanup_context, error);
  }
#endif
  if (rc == LC_OK)
    rc = lc_outbox_claim_deadline_key(outbox_key, &deadline_key, error);
  if (rc == LC_OK) {
    lc_acquire_req acquire;

    lc_acquire_req_init(&acquire);
    acquire.ns = outbox->ns;
    acquire.key = deadline_key;
    acquire.owner = outbox->owner;
    acquire.ttl_seconds = outbox->transaction_ttl_seconds;
    rc = lc_acquire(&outbox->client->pub, &acquire, &deadline_lease, error);
  }
  if (rc == LC_OK) {
    lc_remove_req_init(&remove);
    rc = lc_lease_remove(deadline_lease, &remove, error);
  }
  if (rc == LC_OK) {
    lc_release_req_init(&release);
    rc = lc_lease_release(deadline_lease, &release, error);
    if (rc == LC_OK)
      deadline_lease = NULL;
  }
  if (deadline_lease != NULL)
    lc_outbox_rollback_lease(deadline_lease);
  free(deadline_key);
  deadline_key = NULL;
  if (rc != LC_OK) {
    lc_outbox_record_loaded_clear(outbox->client, &record);
    lc_outbox_rollback_lease(lease);
    return rc;
  }
  deleted = 0;
  rc = lc_lease_delete_all_attachments(lease, &deleted, error);
  if (rc == LC_OK) {
    lc_remove_req_init(&remove);
    rc = lc_lease_remove(lease, &remove, error);
  }
  if (rc == LC_OK) {
    lc_release_req_init(&release);
    rc = lc_lease_release(lease, &release, error);
    if (rc == LC_OK)
      lease = NULL;
  }
  lc_outbox_record_loaded_clear(outbox->client, &record);
  if (rc != LC_OK)
    lc_outbox_rollback_lease(lease);
  return rc;
}

typedef struct lc_outbox_dead_letter_export_capture {
  lc_outbox_handle *outbox;
  lc_sink *dst;
  lc_dead_letter_export_res *result;
  int format;
  int first;
  char key[129];
  size_t length;
} lc_outbox_dead_letter_export_capture;

static int
lc_outbox_export_dead_letter_key(lc_outbox_dead_letter_export_capture *capture,
                                 lc_error *error) {
  lc_outbox_record record;
  lc_lease *lease;
  lc_get_res get_result;
  lc_release_req release;
  int rc;

  lease = NULL;
#ifdef LOCKDC_TEST_BUILD
  if (lc_outbox_test_before_dead_letter_export_open_hook != NULL) {
    lc_outbox_test_before_dead_letter_export_open_hook(
        lc_outbox_test_before_dead_letter_export_open_context);
  }
#endif
  rc = lc_outbox_open_dead_letter(capture->outbox, capture->outbox->client,
                                  capture->key, &lease, &record, error);
  if (rc != LC_OK) {
    /* Index selection and the direct read are separate operations. A record
     * replayed or deleted between them is no longer exportable, not an export
     * failure. Other validation and transport errors remain observable. */
    if ((rc == LC_ERR_INVALID && error != NULL && error->message != NULL &&
         (strcmp(error->message, "outbox record is not a dead letter") == 0 ||
          strcmp(error->message, "pouch lease already held") == 0)) ||
        (rc == LC_ERR_SERVER && error != NULL && error->http_status == 409L)) {
      lc_error_cleanup(error);
      lc_error_init(error);
      return LC_OK;
    }
    return rc;
  }
  memset(&get_result, 0, sizeof(get_result));
  if (capture->format == LC_DEAD_LETTER_EXPORT_JSON && !capture->first) {
    if (!capture->dst->write(capture->dst, ",", 1U, error))
      rc = LC_ERR_TRANSPORT;
  }
  if (rc == LC_OK)
    rc = lc_lease_get(lease, capture->dst, NULL, &get_result, error);
  if (rc == LC_OK && capture->format == LC_DEAD_LETTER_EXPORT_JSONL &&
      !capture->dst->write(capture->dst, "\n", 1U, error))
    rc = LC_ERR_TRANSPORT;
  lc_get_res_cleanup(&get_result);
  lc_release_req_init(&release);
  release.rollback = 1;
  if (lc_lease_release(lease, &release, NULL) != LC_OK)
    lc_lease_close(lease);
  lc_outbox_record_loaded_clear(capture->outbox->client, &record);
  if (rc == LC_OK) {
    capture->first = 0;
    ++capture->result->exported;
  }
  return rc;
}

static int lc_outbox_dead_letter_export_begin(void *context, lc_error *error) {
  lc_outbox_dead_letter_export_capture *capture =
      (lc_outbox_dead_letter_export_capture *)context;
  (void)error;
  capture->length = 0U;
  return 1;
}

static int lc_outbox_dead_letter_export_chunk(void *context, const char *bytes,
                                              size_t length, lc_error *error) {
  lc_outbox_dead_letter_export_capture *capture =
      (lc_outbox_dead_letter_export_capture *)context;
  if (length > sizeof(capture->key) - 1U - capture->length) {
    (void)lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                       "dead-letter query returned an oversized key", NULL,
                       NULL, NULL);
    return 0;
  }
  memcpy(capture->key + capture->length, bytes, length);
  capture->length += length;
  return 1;
}

static int lc_outbox_dead_letter_export_end(void *context, lc_error *error) {
  lc_outbox_dead_letter_export_capture *capture =
      (lc_outbox_dead_letter_export_capture *)context;
  capture->key[capture->length] = '\0';
  if (!lc_outbox_is_outbox_key(capture->key))
    return 1;
  return lc_outbox_export_dead_letter_key(capture, error) == LC_OK ? 1 : 0;
}

static int lc_outbox_export_dead_letters_method(
    lc_outbox *self, const lc_dead_letter_export_opts *options, lc_sink *dst,
    lc_dead_letter_export_res *out, lc_error *error) {
  static const char selector[] =
      "{\"eq\":{\"field\":\"/dispatch_state\",\"value\":\"dead_letter\"}}";
  lc_outbox_handle *outbox = (lc_outbox_handle *)self;
  lc_dead_letter_export_opts defaults;
  lc_index_flush_req flush_request;
  lc_index_flush_res flush_result;
  lc_query_req request;
  lc_query_key_handler handler;
  lc_query_res query_result;
  lc_outbox_dead_letter_export_capture capture;
  size_t limit;
  int use_index;
  int rc;

  if (outbox == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox, export sink, and export result are required",
                        NULL, NULL, NULL);
  }
  lc_dead_letter_export_res_init(out);
  lc_dead_letter_export_opts_init(&defaults);
  if (options == NULL)
    options = &defaults;
  if (options->format != LC_DEAD_LETTER_EXPORT_JSON &&
      options->format != LC_DEAD_LETTER_EXPORT_JSONL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "dead-letter export format is invalid", NULL, NULL,
                        NULL);
  }
  limit = options->limit == 0U ? outbox->notification_capacity : options->limit;
  if ((uintmax_t)limit > (uintmax_t)LONG_MAX) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "dead-letter export limit exceeds supported range",
                        NULL, NULL, NULL);
  }
  memset(&capture, 0, sizeof(capture));
  capture.outbox = outbox;
  capture.dst = dst;
  capture.result = out;
  capture.format = options->format;
  capture.first = 1;
  if (options->format == LC_DEAD_LETTER_EXPORT_JSON &&
      !dst->write(dst, "[", 1U, error))
    return LC_ERR_TRANSPORT;
  lc_query_req_init(&request);
  lc_index_flush_req_init(&flush_request);
  memset(&handler, 0, sizeof(handler));
  memset(&query_result, 0, sizeof(query_result));
  memset(&flush_result, 0, sizeof(flush_result));
  use_index = outbox->dispatcher_client->pouch == NULL ||
              outbox->dispatcher_client->pouch->query_indexing_enabled;
  /* Export starts from a durable index boundary when indexing is enabled. A
   * scan-only root has no index to flush and selects its current scan path.
   * A concurrent replay or
   * deletion can still change a selected record before it is read; that
   * benign stale-key race is ignored by the key visitor above. */
  if (use_index) {
    flush_request.ns = outbox->ns;
    flush_request.mode = "wait";
    rc = lc_flush_index(&outbox->dispatcher_client->pub, &flush_request,
                        &flush_result, error);
    lc_index_flush_res_cleanup(&flush_result);
    if (rc != LC_OK)
      return rc;
  }
  request.ns = outbox->ns;
  request.selector_json = selector;
  request.limit = (long)limit;
  request.engine = use_index ? "index" : "scan";
  request.refresh = use_index ? "wait_for" : NULL;
  handler.begin = lc_outbox_dead_letter_export_begin;
  handler.chunk = lc_outbox_dead_letter_export_chunk;
  handler.end = lc_outbox_dead_letter_export_end;
  rc = lc_query_keys(&outbox->client->pub, &request, &handler, &capture,
                     &query_result, error);
  lc_query_res_cleanup(&query_result);
  if (rc != LC_OK)
    return rc;
  if (options->format == LC_DEAD_LETTER_EXPORT_JSON &&
      !dst->write(dst, "]", 1U, error))
    return LC_ERR_TRANSPORT;
  return LC_OK;
}
static void lc_outbox_destroy(lc_outbox_handle *outbox) {
  lc_client_handle *client;
  size_t i;
  client = outbox->client;
  for (i = 0U; i < outbox->notification_count; ++i)
    lc_client_free(client, outbox->notifications[i]);
  lc_client_free(client, outbox->notifications);
  for (i = 0U; i < outbox->delayed_notification_count; ++i) {
    lc_client_free(client, outbox->delayed_notifications[i].key);
  }
  lc_client_free(client, outbox->delayed_notifications);
  free(outbox->recovery_cursor);
  lc_client_free(client, outbox->last_error);
  if (outbox->dispatcher_cond_initialized)
    pthread_cond_destroy(&outbox->dispatcher_cond);
  if (outbox->notification_cond_initialized)
    pthread_cond_destroy(&outbox->notification_cond);
  if (outbox->notification_mutex_initialized)
    pthread_mutex_destroy(&outbox->notification_mutex);
  lc_client_free(client, outbox->ns);
  lc_client_free(client, outbox->owner);
  if (outbox->dispatcher_client != NULL) {
    lc_client_handle_release(outbox->dispatcher_client);
  }
  lc_client_free(client, outbox);
  lc_client_handle_release(client);
}

static void lc_outbox_retain(lc_outbox_handle *outbox) {
  pthread_mutex_lock(&outbox->notification_mutex);
  ++outbox->ref_count;
  pthread_mutex_unlock(&outbox->notification_mutex);
}

static void lc_outbox_release(lc_outbox_handle *outbox) {
  int destroy = 0;

  pthread_mutex_lock(&outbox->notification_mutex);
  if (outbox->ref_count > 0U && --outbox->ref_count == 0U)
    destroy = 1;
  pthread_mutex_unlock(&outbox->notification_mutex);
  if (destroy)
    lc_outbox_destroy(outbox);
}

/* Request shutdown without consuming the owning reference.  Dispatcher stop
 * uses this half of close so its public deadline also bounds private recovery
 * teardown; the final owner is released only after wait observes exit. */
static void lc_outbox_request_close(lc_outbox_handle *outbox) {
#ifdef LOCKDC_TEST_BUILD
  int requested;
#endif

  if (outbox == NULL || !outbox->notification_mutex_initialized)
    return;
#ifdef LOCKDC_TEST_BUILD
  requested = 0;
#endif
  pthread_mutex_lock(&outbox->notification_mutex);
  if (!outbox->close_requested) {
    outbox->close_requested = 1;
    outbox->closed = 1;
#ifdef LOCKDC_TEST_BUILD
    requested = 1;
#endif
    if (outbox->notification_cond_initialized)
      pthread_cond_broadcast(&outbox->notification_cond);
    if (outbox->dispatcher_cond_initialized)
      pthread_cond_broadcast(&outbox->dispatcher_cond);
  }
  pthread_mutex_unlock(&outbox->notification_mutex);
#ifdef LOCKDC_TEST_BUILD
  if (requested && lc_outbox_test_after_close_requested_hook != NULL) {
    lc_outbox_test_after_close_requested_hook(
        lc_outbox_test_after_close_requested_context);
  }
#endif
}

static void lc_outbox_close_method(lc_outbox *self) {
  lc_outbox_handle *outbox = (lc_outbox_handle *)self;
  lc_outbox_dispatcher_handle *attached_dispatcher;
  int already_closed;

  if (outbox == NULL)
    return;
  pthread_mutex_lock(&outbox->client->lifecycle_mutex);
  attached_dispatcher = outbox->attached_dispatcher;
  outbox->attached_dispatcher = NULL;
  pthread_mutex_unlock(&outbox->client->lifecycle_mutex);
  if (attached_dispatcher != NULL)
    lc_outbox_dispatcher_close_method(&attached_dispatcher->pub);
  if (!outbox->notification_mutex_initialized) {
    lc_outbox_destroy(outbox);
    return;
  }
  pthread_mutex_lock(&outbox->notification_mutex);
  already_closed = outbox->close_requested;
  pthread_mutex_unlock(&outbox->notification_mutex);
  if (already_closed)
    return;
  lc_outbox_request_close(outbox);
  if (outbox->dispatcher_started) {
    pthread_mutex_lock(&outbox->notification_mutex);
    while (!outbox->dispatcher_exited)
      (void)pthread_cond_wait(&outbox->dispatcher_cond,
                              &outbox->notification_mutex);
    pthread_mutex_unlock(&outbox->notification_mutex);
  }
  lc_outbox_release(outbox);
}

static int lc_outbox_new(lc_client *self, const lc_outbox_config *config,
                         int start_dispatcher, lc_outbox **out,
                         lc_error *error) {
  lc_client_handle *client;
  lc_outbox_handle *outbox;
  lc_unix_seconds configured_claim_deadline;
  lc_unix_seconds periodic_deadline;
  pthread_attr_t dispatcher_attributes;
  time_t now;
  int attribute_rc;
  int rc;
  if (out != NULL)
    *out = NULL;
  if (self == NULL || config == NULL || out == NULL || config->ns == NULL ||
      config->ns[0] == '\0')
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "new_outbox requires client, namespace config, and output", NULL, NULL,
        NULL);
  client = (lc_client_handle *)self;
  pthread_mutex_lock(&client->lifecycle_mutex);
  if (client->close_requested) {
    pthread_mutex_unlock(&client->lifecycle_mutex);
    return lc_error_set(error, LC_ERR_INVALID, 0L, "client is closed", NULL,
                        NULL, NULL);
  }
  ++client->refcount;
  pthread_mutex_unlock(&client->lifecycle_mutex);
  outbox = (lc_outbox_handle *)lc_client_calloc(client, 1U, sizeof(*outbox));
  if (outbox == NULL) {
    lc_client_handle_release(client);
    return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to allocate outbox",
                        NULL, NULL, NULL);
  }
  outbox->client = client;
  outbox->ns = lc_client_strdup(client, config->ns);
  outbox->owner =
      lc_client_strdup(client, config->owner != NULL && config->owner[0] != '\0'
                                   ? config->owner
                                   : "lockdc-outbox");
  if (outbox->ns == NULL || outbox->owner == NULL) {
    lc_outbox_close_method(&outbox->pub);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy outbox configuration", NULL, NULL,
                        NULL);
  }
  outbox->transaction_ttl_seconds = config->transaction_ttl_seconds == 0L
                                        ? 30L
                                        : config->transaction_ttl_seconds;
  if (outbox->transaction_ttl_seconds < 1L) {
    lc_outbox_close_method(&outbox->pub);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox transaction ttl must be positive", NULL, NULL,
                        NULL);
  }
  outbox->claim_ttl_seconds =
      config->claim_ttl_seconds == 0L ? 300L : config->claim_ttl_seconds;
  outbox->max_attempts = config->max_attempts == 0 ? 100 : config->max_attempts;
  outbox->replay_dead_letters_on_startup =
      config->replay_dead_letters_on_startup != 0 ? 1 : 0;
  outbox->startup_dead_letter_replay_pending =
      outbox->replay_dead_letters_on_startup;
  outbox->retry_initial_delay_seconds =
      config->retry_initial_delay_seconds == 0L
          ? 1L
          : config->retry_initial_delay_seconds;
  outbox->retry_max_delay_seconds = config->retry_max_delay_seconds == 0L
                                        ? 900L
                                        : config->retry_max_delay_seconds;
  outbox->host_retry_delay_max_seconds =
      config->host_retry_delay_max_seconds == 0L
          ? 3600L
          : config->host_retry_delay_max_seconds;
  outbox->shutdown_timeout_ms =
      config->shutdown_timeout_ms == 0L
          ? (client->timeout_ms > 0L ? client->timeout_ms : 30000L)
          : config->shutdown_timeout_ms;
  outbox->recovery_interval_seconds = config->recovery_interval_seconds;
  if (outbox->recovery_interval_seconds == 0L && !client->is_pouch)
    outbox->recovery_interval_seconds = 300L;
  if (outbox->claim_ttl_seconds < 1L || outbox->max_attempts < 1 ||
      outbox->retry_initial_delay_seconds < 1L ||
      outbox->retry_max_delay_seconds < outbox->retry_initial_delay_seconds ||
      outbox->host_retry_delay_max_seconds < 1L ||
      outbox->shutdown_timeout_ms < 1L ||
      outbox->recovery_interval_seconds < 0L) {
    lc_outbox_close_method(&outbox->pub);
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "outbox claim, retry, and recovery configuration is invalid", NULL,
        NULL, NULL);
  }
  now = time(NULL);
  if (now == (time_t)-1) {
    lc_outbox_close_method(&outbox->pub);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "failed to read outbox configuration clock", NULL, NULL,
                        NULL);
  }
  rc = lc_outbox_timestamp_add((lc_unix_seconds)now, outbox->claim_ttl_seconds,
                               "claim ttl", &configured_claim_deadline, error);
  if (rc != LC_OK) {
    lc_outbox_close_method(&outbox->pub);
    return rc;
  }
  /* The first periodic sweep is armed only by consumer demand, but validate
   * the configured interval at construction so an impossible future deadline
   * remains a deterministic configuration error. */
  if (outbox->recovery_interval_seconds > 0L) {
    rc = lc_outbox_timestamp_add(
        (lc_unix_seconds)now, outbox->recovery_interval_seconds,
        "recovery interval", &periodic_deadline, error);
    if (rc != LC_OK) {
      lc_outbox_close_method(&outbox->pub);
      return rc;
    }
  }
  outbox->notification_capacity = config->notification_capacity == 0U
                                      ? 1024U
                                      : config->notification_capacity;
  if (pthread_mutex_init(&outbox->notification_mutex, NULL) != 0) {
    lc_outbox_close_method(&outbox->pub);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize outbox lifecycle state", NULL,
                        NULL, NULL);
  }
  outbox->notification_mutex_initialized = 1;
  outbox->ref_count = 1U;
  outbox->pub.begin = lc_outbox_begin_method;
  outbox->pub.accept_command = lc_outbox_accept_command_method;
  outbox->pub.get_command_receipt = lc_outbox_get_command_receipt_method;
  outbox->pub.get_command_receipt_by_id =
      lc_outbox_get_command_receipt_by_id_method;
  outbox->pub.wait_command = lc_outbox_wait_command_method;
  outbox->pub.write_command_result = lc_outbox_write_command_result_method;
  outbox->pub.resume_command = lc_outbox_resume_command_method;
  outbox->pub.resume_command_by_id = lc_outbox_resume_command_by_id_method;
  outbox->pub.append = lc_outbox_append_method;
  outbox->pub.accept_inbox = lc_outbox_accept_inbox_method;
  outbox->pub.get_or_start_dispatcher =
      lc_outbox_get_or_start_dispatcher_method;
  outbox->pub.close = lc_outbox_close_method;
  if (!start_dispatcher) {
    *out = &outbox->pub;
    return LC_OK;
  }
  outbox->notifications = (char **)lc_client_calloc(
      client, outbox->notification_capacity, sizeof(*outbox->notifications));
  outbox->delayed_notifications =
      (lc_outbox_delayed_notification *)lc_client_calloc(
          client, outbox->notification_capacity,
          sizeof(*outbox->delayed_notifications));
  if (outbox->notifications == NULL || outbox->delayed_notifications == NULL) {
    lc_outbox_close_method(&outbox->pub);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize outbox dispatcher state", NULL,
                        NULL, NULL);
  }
  if (pthread_cond_init(&outbox->notification_cond, NULL) != 0) {
    lc_outbox_close_method(&outbox->pub);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize outbox dispatcher state", NULL,
                        NULL, NULL);
  }
  outbox->notification_cond_initialized = 1;
  if (pthread_cond_init(&outbox->dispatcher_cond, NULL) != 0) {
    lc_outbox_close_method(&outbox->pub);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize outbox dispatcher state", NULL,
                        NULL, NULL);
  }
  outbox->dispatcher_cond_initialized = 1;
  if (client->is_pouch) {
    lc_client_handle_retain(client);
    outbox->dispatcher_client = client;
  } else {
    lc_client *dispatcher_client = NULL;
    rc = lc_client_clone_remote_for_outbox(client, outbox->shutdown_timeout_ms,
                                           &dispatcher_client, error);
    if (rc != LC_OK) {
      lc_outbox_close_method(&outbox->pub);
      return rc;
    }
    outbox->dispatcher_client = (lc_client_handle *)dispatcher_client;
    lc_engine_client_set_cancel_check(outbox->dispatcher_client->engine,
                                      lc_outbox_dispatcher_cancel_check,
                                      outbox);
  }
  /* Startup recovery is armed, not run. The first blocking consumer request
   * or explicit reconcile() starts it after the dispatcher is fully attached
   * to the host's client lifecycle. */
  outbox->recovery_claims_pending = 1;
  attribute_rc = pthread_attr_init(&dispatcher_attributes);
  if (attribute_rc == 0) {
    attribute_rc = pthread_attr_setdetachstate(&dispatcher_attributes,
                                               PTHREAD_CREATE_DETACHED);
    if (attribute_rc != 0)
      (void)pthread_attr_destroy(&dispatcher_attributes);
  }
  if (attribute_rc != 0) {
    lc_outbox_close_method(&outbox->pub);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to configure outbox dispatcher thread", NULL,
                        NULL, NULL);
  }
  /* The detached thread retains the core through its last instruction. */
  lc_outbox_retain(outbox);
  if (pthread_create(&outbox->dispatcher_thread, &dispatcher_attributes,
                     lc_outbox_dispatcher_main, outbox) != 0) {
    (void)pthread_attr_destroy(&dispatcher_attributes);
    lc_outbox_release(outbox);
    lc_outbox_close_method(&outbox->pub);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to start outbox dispatcher", NULL, NULL, NULL);
  }
  (void)pthread_attr_destroy(&dispatcher_attributes);
  outbox->dispatcher_started = 1;
  *out = &outbox->pub;
  return LC_OK;
}

int lc_client_new_outbox_method(lc_client *self, const lc_outbox_config *config,
                                lc_outbox **out, lc_error *error) {
  return lc_outbox_new(self, config, 0, out, error);
}

/* Receiver calls must retain the core while they run: stop/wait can detach the
 * dispatcher from its core concurrently, but a caller that owns a receiver
 * reference remains entitled to a deterministic stopped/not-stopped result. */
static int
lc_outbox_dispatcher_acquire_core(lc_outbox_dispatcher_handle *dispatcher,
                                  int require_running, lc_outbox_handle **out,
                                  lc_error *error) {
  lc_outbox_handle *core;

  if (out == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox dispatcher core output is required", NULL, NULL,
                        NULL);
  *out = NULL;
  if (dispatcher == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox dispatcher is required", NULL, NULL, NULL);
  pthread_mutex_lock(&dispatcher->lifecycle_mutex);
  core = dispatcher->core;
  if (core != NULL && (!require_running || !dispatcher->stopping))
    lc_outbox_retain(core);
  else
    core = NULL;
  pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
  if (core == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        require_running ? "outbox dispatcher is stopped"
                                        : "outbox dispatcher is unavailable",
                        NULL, NULL, NULL);
  }
#ifdef LOCKDC_TEST_BUILD
  if (lc_outbox_test_after_dispatcher_core_retain_hook != NULL) {
    lc_outbox_test_after_dispatcher_core_retain_hook(
        lc_outbox_test_after_dispatcher_core_retain_context);
  }
#endif
  *out = core;
  return LC_OK;
}

static int lc_outbox_dispatcher_next_method(lc_outbox_dispatcher *self,
                                            long timeout_ms,
                                            lc_outbox_job **out,
                                            lc_error *error) {
  lc_outbox_dispatcher_handle *dispatcher = (lc_outbox_dispatcher_handle *)self;
  lc_outbox_handle *core;
  int rc;

  rc = lc_outbox_dispatcher_acquire_core(dispatcher, 1, &core, error);
  if (rc != LC_OK)
    return rc;
  rc = lc_outbox_next_method(&core->pub, timeout_ms, out, error);
  lc_outbox_release(core);
  return rc;
}

static int lc_outbox_dispatcher_notify_outbox_key_method(
    lc_outbox_dispatcher *self, const char *outbox_key, lc_error *error) {
  lc_outbox_dispatcher_handle *dispatcher = (lc_outbox_dispatcher_handle *)self;
  lc_outbox_handle *core;
  int rc;

  if (outbox_key == NULL || outbox_key[0] == '\0' ||
      !lc_outbox_is_outbox_key(outbox_key)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "dispatcher requires a committed outbox key", NULL,
                        NULL, NULL);
  }
  rc = lc_outbox_dispatcher_acquire_core(dispatcher, 1, &core, error);
  if (rc != LC_OK)
    return rc;
  lc_outbox_notify(core, outbox_key, 1);
  lc_outbox_release(core);
  return LC_OK;
}

static int lc_outbox_dispatcher_get_stats_method(lc_outbox_dispatcher *self,
                                                 lc_outbox_stats *out,
                                                 lc_error *error) {
  lc_outbox_dispatcher_handle *dispatcher = (lc_outbox_dispatcher_handle *)self;
  lc_outbox_handle *core;
  int rc;

  rc = lc_outbox_dispatcher_acquire_core(dispatcher, 0, &core, error);
  if (rc != LC_OK)
    return rc;
  rc = lc_outbox_get_stats_method(&core->pub, out, error);
  lc_outbox_release(core);
  return rc;
}

static int lc_outbox_dispatcher_reconcile_method(lc_outbox_dispatcher *self,
                                                 lc_error *error) {
  lc_outbox_dispatcher_handle *dispatcher = (lc_outbox_dispatcher_handle *)self;
  lc_outbox_handle *core;
  int rc;

  rc = lc_outbox_dispatcher_acquire_core(dispatcher, 1, &core, error);
  if (rc != LC_OK)
    return rc;
  rc = lc_outbox_reconcile_method(&core->pub, error);
  lc_outbox_release(core);
  return rc;
}

static int lc_outbox_dispatcher_replay_dead_letter_method(
    lc_outbox_dispatcher *self, const char *outbox_key, lc_error *error) {
  lc_outbox_dispatcher_handle *dispatcher = (lc_outbox_dispatcher_handle *)self;
  lc_outbox_handle *core;
  int rc;

  rc = lc_outbox_dispatcher_acquire_core(dispatcher, 1, &core, error);
  if (rc != LC_OK)
    return rc;
  rc = lc_outbox_replay_dead_letter_method(&core->pub, outbox_key, error);
  lc_outbox_release(core);
  return rc;
}

static int lc_outbox_dispatcher_delete_dead_letter_method(
    lc_outbox_dispatcher *self, const char *outbox_key, lc_error *error) {
  lc_outbox_dispatcher_handle *dispatcher = (lc_outbox_dispatcher_handle *)self;
  lc_outbox_handle *core;
  int rc;

  rc = lc_outbox_dispatcher_acquire_core(dispatcher, 1, &core, error);
  if (rc != LC_OK)
    return rc;
  rc = lc_outbox_delete_dead_letter_method(&core->pub, outbox_key, error);
  lc_outbox_release(core);
  return rc;
}

static int lc_outbox_dispatcher_export_dead_letters_method(
    lc_outbox_dispatcher *self, const lc_dead_letter_export_opts *options,
    lc_sink *dst, lc_dead_letter_export_res *out, lc_error *error) {
  lc_outbox_dispatcher_handle *dispatcher = (lc_outbox_dispatcher_handle *)self;
  lc_outbox_handle *core;
  int rc;

  rc = lc_outbox_dispatcher_acquire_core(dispatcher, 0, &core, error);
  if (rc != LC_OK)
    return rc;
  rc = lc_outbox_export_dead_letters_method(&core->pub, options, dst, out,
                                            error);
  lc_outbox_release(core);
  return rc;
}

static int lc_outbox_dispatcher_stop_method(lc_outbox_dispatcher *self,
                                            long deadline_ms, lc_error *error) {
  lc_outbox_dispatcher_handle *dispatcher = (lc_outbox_dispatcher_handle *)self;
  lc_outbox_handle *core;
  int stop_now;

  if (dispatcher == NULL || deadline_ms < -1L)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox dispatcher and valid deadline are required",
                        NULL, NULL, NULL);
  stop_now = 0;
  core = NULL;
  pthread_mutex_lock(&dispatcher->lifecycle_mutex);
  if (!dispatcher->stopping && dispatcher->core != NULL) {
    dispatcher->stopping = 1;
    core = dispatcher->core;
    lc_outbox_retain(core);
    stop_now = 1;
  }
  pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
  if (stop_now) {
    /* Keep the stopping entry registered until private teardown and every
     * handed-out job have finished.  This prevents a racing acquisition from
     * creating a second live dispatcher for the same canonical configuration.
     * wait() performs the final registry removal and drops the core owner. */
    lc_outbox_request_close(core);
    lc_outbox_release(core);
  }
  return lc_outbox_dispatcher_wait_method(self, deadline_ms, error);
}

static int lc_outbox_dispatcher_wait_method(lc_outbox_dispatcher *self,
                                            long deadline_ms, lc_error *error) {
  lc_outbox_dispatcher_handle *dispatcher = (lc_outbox_dispatcher_handle *)self;
  struct timespec deadline;
  lc_outbox_handle *core;
  lc_outbox_dispatcher_handle **link;
  int complete;
  int wait_rc;

  if (dispatcher == NULL || deadline_ms < -1L)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox dispatcher and valid deadline are required",
                        NULL, NULL, NULL);
  if (deadline_ms > 0L) {
    if (clock_gettime(CLOCK_REALTIME, &deadline) != 0) {
      return lc_error_set(error, LC_ERR_PROTOCOL, errno,
                          "failed to construct dispatcher wait deadline", NULL,
                          NULL, NULL);
    }
    lc_outbox_timespec_add_milliseconds_saturating(&deadline, deadline_ms);
  }
  pthread_mutex_lock(&dispatcher->lifecycle_mutex);
  if (!dispatcher->stopping) {
    pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox dispatcher has not been stopped", NULL, NULL,
                        NULL);
  }
  core = dispatcher->core;
  if (core != NULL)
    lc_outbox_retain(core);
  pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
  if (core != NULL) {
    pthread_mutex_lock(&core->notification_mutex);
    while (!core->dispatcher_exited) {
      if (deadline_ms == 0L) {
        pthread_mutex_unlock(&core->notification_mutex);
        lc_outbox_release(core);
        return lc_error_set(error, LC_ERR_TIMEOUT, 0L,
                            "outbox dispatcher private thread is stopping",
                            NULL, NULL, NULL);
      }
      if (deadline_ms < 0L) {
        wait_rc = pthread_cond_wait(&core->dispatcher_cond,
                                    &core->notification_mutex);
      } else {
        wait_rc = pthread_cond_timedwait(&core->dispatcher_cond,
                                         &core->notification_mutex, &deadline);
      }
      if (wait_rc == ETIMEDOUT) {
        pthread_mutex_unlock(&core->notification_mutex);
        lc_outbox_release(core);
        return lc_error_set(error, LC_ERR_TIMEOUT, 0L,
                            "outbox dispatcher stop deadline elapsed", NULL,
                            NULL, NULL);
      }
      if (wait_rc != 0) {
        pthread_mutex_unlock(&core->notification_mutex);
        lc_outbox_release(core);
        return lc_error_set(error, LC_ERR_PROTOCOL, wait_rc,
                            "outbox dispatcher wait failed", NULL, NULL, NULL);
      }
    }
    pthread_mutex_unlock(&core->notification_mutex);
  }
  pthread_mutex_lock(&dispatcher->lifecycle_mutex);
  while (!dispatcher->detach_on_client_close && dispatcher->active_jobs > 0U) {
    if (deadline_ms == 0L) {
      pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
      if (core != NULL)
        lc_outbox_release(core);
      return lc_error_set(error, LC_ERR_TIMEOUT, 0L,
                          "outbox dispatcher still has active jobs", NULL, NULL,
                          NULL);
    }
    if (deadline_ms < 0L) {
      wait_rc = pthread_cond_wait(&dispatcher->lifecycle_cond,
                                  &dispatcher->lifecycle_mutex);
    } else {
      wait_rc = pthread_cond_timedwait(&dispatcher->lifecycle_cond,
                                       &dispatcher->lifecycle_mutex, &deadline);
    }
    if (wait_rc == ETIMEDOUT) {
      pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
      if (core != NULL)
        lc_outbox_release(core);
      return lc_error_set(error, LC_ERR_TIMEOUT, 0L,
                          "outbox dispatcher stop deadline elapsed", NULL, NULL,
                          NULL);
    }
    if (wait_rc != 0) {
      pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
      if (core != NULL)
        lc_outbox_release(core);
      return lc_error_set(error, LC_ERR_PROTOCOL, wait_rc,
                          "outbox dispatcher wait failed", NULL, NULL, NULL);
    }
  }
  complete =
      core != NULL && dispatcher->core == core && !dispatcher->stop_complete;
  if (complete) {
    dispatcher->core = NULL;
    dispatcher->stop_complete = 1;
    pthread_cond_broadcast(&dispatcher->lifecycle_cond);
  }
  pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
  if (complete) {
    pthread_mutex_lock(&dispatcher->client->lifecycle_mutex);
    link = &dispatcher->client->outbox_dispatchers;
    while (*link != NULL) {
      if (*link == dispatcher) {
        *link = dispatcher->registry_next;
        dispatcher->registry_next = NULL;
        break;
      }
      link = &(*link)->registry_next;
    }
    pthread_mutex_unlock(&dispatcher->client->lifecycle_mutex);
    /* Drop the core's dispatcher-owner reference only after its private
     * thread is gone and no handed-out job remains. */
    lc_outbox_release(core);
    /* The registry owns one dispatcher reference until final removal. */
    lc_outbox_dispatcher_close_method(&dispatcher->pub);
  }
  if (core != NULL)
    lc_outbox_release(core);
  pthread_mutex_lock(&dispatcher->lifecycle_mutex);
  while (!dispatcher->worker_finished) {
    if (deadline_ms == 0L) {
      pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
      return lc_error_set(error, LC_ERR_TIMEOUT, 0L,
                          "outbox dispatcher worker is stopping", NULL, NULL,
                          NULL);
    }
    if (deadline_ms < 0L) {
      wait_rc = pthread_cond_wait(&dispatcher->lifecycle_cond,
                                  &dispatcher->lifecycle_mutex);
    } else {
      wait_rc = pthread_cond_timedwait(&dispatcher->lifecycle_cond,
                                       &dispatcher->lifecycle_mutex, &deadline);
    }
    if (wait_rc == ETIMEDOUT) {
      pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
      return lc_error_set(error, LC_ERR_TIMEOUT, 0L,
                          "outbox dispatcher stop deadline elapsed", NULL, NULL,
                          NULL);
    }
    if (wait_rc != 0) {
      pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
      return lc_error_set(error, LC_ERR_PROTOCOL, wait_rc,
                          "outbox dispatcher wait failed", NULL, NULL, NULL);
    }
  }
  pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
  return LC_OK;
}

static void
lc_outbox_dispatcher_retain(lc_outbox_dispatcher_handle *dispatcher) {
  if (dispatcher == NULL)
    return;
  pthread_mutex_lock(&dispatcher->lifecycle_mutex);
  lc_outbox_dispatcher_retain_locked(dispatcher);
  pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
}

static void
lc_outbox_dispatcher_retain_locked(lc_outbox_dispatcher_handle *dispatcher) {
  if (dispatcher != NULL)
    ++dispatcher->ref_count;
}

#ifdef LOCKDC_TEST_BUILD
size_t lc_outbox_test_dispatcher_ref_count(lc_outbox_dispatcher *dispatcher) {
  lc_outbox_dispatcher_handle *handle =
      (lc_outbox_dispatcher_handle *)dispatcher;
  size_t count;

  if (handle == NULL)
    return 0U;
  pthread_mutex_lock(&handle->lifecycle_mutex);
  count = handle->ref_count;
  pthread_mutex_unlock(&handle->lifecycle_mutex);
  return count;
}
#endif

static void lc_outbox_dispatcher_close_method(lc_outbox_dispatcher *self) {
  lc_outbox_dispatcher_handle *dispatcher = (lc_outbox_dispatcher_handle *)self;
  int destroy;

  if (dispatcher == NULL)
    return;
  pthread_mutex_lock(&dispatcher->lifecycle_mutex);
  if (dispatcher->ref_count > 0U)
    --dispatcher->ref_count;
  destroy = dispatcher->ref_count == 0U && dispatcher->stopping &&
            dispatcher->stop_complete;
  pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
  if (destroy) {
    if (dispatcher->lifecycle_initialized) {
      pthread_cond_destroy(&dispatcher->lifecycle_cond);
      pthread_mutex_destroy(&dispatcher->lifecycle_mutex);
    }
    lc_free_with_allocator(&dispatcher->allocator, dispatcher);
  }
}

void lc_outbox_dispatchers_stop_for_client(lc_client_handle *client) {
  lc_outbox_dispatcher_handle *dispatcher;
  lc_outbox_handle *core;

  if (client == NULL || !client->lifecycle_mutex_initialized)
    return;
  for (;;) {
    core = NULL;
    pthread_mutex_lock(&client->lifecycle_mutex);
    dispatcher = client->outbox_dispatchers;
    while (dispatcher != NULL) {
      pthread_mutex_lock(&dispatcher->lifecycle_mutex);
      if (!dispatcher->client_close_requested) {
        dispatcher->client_close_requested = 1;
        dispatcher->detach_on_client_close = 1;
        dispatcher->stopping = 1;
        lc_outbox_dispatcher_retain_locked(dispatcher);
        core = dispatcher->core;
        if (core != NULL)
          lc_outbox_retain(core);
        pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
        break;
      }
      pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
      dispatcher = dispatcher->registry_next;
    }
    pthread_mutex_unlock(&client->lifecycle_mutex);
    if (dispatcher == NULL)
      return;
    if (core != NULL) {
      lc_outbox_request_close(core);
      lc_outbox_release(core);
    }
    /* Client close never waits on a caller-owned job. It normally joins the
     * private worker after cancellation so no thread can outlive a closing
     * embedding runtime. A Pouch source/visitor callback can hold a namespace
     * lock that recovery is waiting for; joining from that callback would form
     * a cycle. The exiting worker completes that deferred close instead. */
    if (client->pouch == NULL ||
        !lc_pouch_state_namespace_lock_held_by_current_thread(client->pouch)) {
      (void)lc_outbox_dispatcher_wait_method(&dispatcher->pub, -1L, NULL);
    }
    lc_outbox_dispatcher_close_method(&dispatcher->pub);
  }
}

static int lc_outbox_get_or_start_dispatcher_method(lc_outbox *self,
                                                    lc_outbox_dispatcher **out,
                                                    lc_error *error) {
  lc_outbox_handle *outbox = (lc_outbox_handle *)self;
  lc_outbox_dispatcher_handle *dispatcher;
  lc_outbox_config config;
  lc_outbox *core;
  int cond_rc;
  int mutex_rc;
  int rc;

  if (outbox == NULL || out == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox and dispatcher output are required", NULL, NULL,
                        NULL);
  *out = NULL;
  pthread_mutex_lock(&outbox->client->lifecycle_mutex);
  if (outbox->client->close_requested) {
    pthread_mutex_unlock(&outbox->client->lifecycle_mutex);
    return lc_error_set(error, LC_ERR_INVALID, 0L, "client is closed", NULL,
                        NULL, NULL);
  }
  if (outbox->attached_dispatcher != NULL) {
    dispatcher = outbox->attached_dispatcher;
    pthread_mutex_lock(&dispatcher->lifecycle_mutex);
    if (!dispatcher->stopping && dispatcher->core != NULL) {
      lc_outbox_dispatcher_retain_locked(dispatcher);
      pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
      pthread_mutex_unlock(&outbox->client->lifecycle_mutex);
      *out = &dispatcher->pub;
      return LC_OK;
    }
    pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
  }
  dispatcher = outbox->client->outbox_dispatchers;
  while (dispatcher != NULL) {
    pthread_mutex_lock(&dispatcher->lifecycle_mutex);
    if (lc_outbox_dispatcher_configuration_matches(outbox, dispatcher)) {
      if (dispatcher->stopping) {
        pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
        pthread_mutex_unlock(&outbox->client->lifecycle_mutex);
        return lc_error_set(error, LC_ERR_INVALID, 0L,
                            "outbox dispatcher is stopping", NULL, NULL, NULL);
      }
      lc_outbox_dispatcher_retain_locked(dispatcher);
      if (outbox->attached_dispatcher != dispatcher)
        lc_outbox_dispatcher_retain_locked(dispatcher);
      lc_outbox_replace_attached_dispatcher(outbox, dispatcher);
      pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
      pthread_mutex_unlock(&outbox->client->lifecycle_mutex);
      *out = &dispatcher->pub;
      return LC_OK;
    }
    pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
    dispatcher = dispatcher->registry_next;
  }
  pthread_mutex_unlock(&outbox->client->lifecycle_mutex);
  lc_outbox_config_init(&config);
  config.ns = outbox->ns;
  config.owner = outbox->owner;
  config.transaction_ttl_seconds = outbox->transaction_ttl_seconds;
  config.claim_ttl_seconds = outbox->claim_ttl_seconds;
  config.max_attempts = outbox->max_attempts;
  config.notification_capacity = outbox->notification_capacity;
  config.retry_initial_delay_seconds = outbox->retry_initial_delay_seconds;
  config.retry_max_delay_seconds = outbox->retry_max_delay_seconds;
  config.host_retry_delay_max_seconds = outbox->host_retry_delay_max_seconds;
  config.recovery_interval_seconds = outbox->recovery_interval_seconds;
  config.shutdown_timeout_ms = outbox->shutdown_timeout_ms;
  config.replay_dead_letters_on_startup =
      outbox->replay_dead_letters_on_startup;
  core = NULL;
  rc = lc_outbox_new(&outbox->client->pub, &config, 1, &core, error);
  if (rc != LC_OK)
    return rc;
  dispatcher = (lc_outbox_dispatcher_handle *)lc_client_calloc(
      outbox->client, 1U, sizeof(*dispatcher));
  if (dispatcher == NULL) {
    lc_outbox_close(core);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate outbox dispatcher", NULL, NULL,
                        NULL);
  }
  dispatcher->core = (lc_outbox_handle *)core;
  dispatcher->client = outbox->client;
  dispatcher->allocator = outbox->client->allocator;
  mutex_rc = pthread_mutex_init(&dispatcher->lifecycle_mutex, NULL);
  cond_rc = mutex_rc == 0 ? pthread_cond_init(&dispatcher->lifecycle_cond, NULL)
                          : mutex_rc;
  if (cond_rc != 0) {
    if (mutex_rc == 0)
      pthread_mutex_destroy(&dispatcher->lifecycle_mutex);
    lc_outbox_close(core);
    lc_client_free(outbox->client, dispatcher);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize outbox dispatcher lifecycle",
                        NULL, NULL, NULL);
  }
  dispatcher->lifecycle_initialized = 1;
  ((lc_outbox_handle *)core)->dispatcher = dispatcher;
  /* The initial reference becomes the registry's reference when published. */
  dispatcher->ref_count = 1U;
  dispatcher->pub.impl = dispatcher;
  dispatcher->pub.next = lc_outbox_dispatcher_next_method;
  dispatcher->pub.notify_outbox_key =
      lc_outbox_dispatcher_notify_outbox_key_method;
  dispatcher->pub.get_stats = lc_outbox_dispatcher_get_stats_method;
  dispatcher->pub.reconcile = lc_outbox_dispatcher_reconcile_method;
  dispatcher->pub.replay_dead_letter =
      lc_outbox_dispatcher_replay_dead_letter_method;
  dispatcher->pub.delete_dead_letter =
      lc_outbox_dispatcher_delete_dead_letter_method;
  dispatcher->pub.export_dead_letters =
      lc_outbox_dispatcher_export_dead_letters_method;
  dispatcher->pub.stop = lc_outbox_dispatcher_stop_method;
  dispatcher->pub.wait = lc_outbox_dispatcher_wait_method;
  dispatcher->pub.close = lc_outbox_dispatcher_close_method;
  pthread_mutex_lock(&outbox->client->lifecycle_mutex);
  if (outbox->client->close_requested) {
    pthread_mutex_unlock(&outbox->client->lifecycle_mutex);
    lc_outbox_dispatcher_retain(dispatcher);
    (void)lc_outbox_dispatcher_stop_method(&dispatcher->pub, -1L, NULL);
    lc_outbox_dispatcher_close_method(&dispatcher->pub);
    return lc_error_set(error, LC_ERR_INVALID, 0L, "client is closed", NULL,
                        NULL, NULL);
  }
  /* A concurrent creator may have completed while this dispatcher allocated
   * its private client. Keep one canonical live core per client/config. */
  {
    lc_outbox_dispatcher_handle *existing = outbox->client->outbox_dispatchers;
    while (existing != NULL) {
      pthread_mutex_lock(&existing->lifecycle_mutex);
      if (lc_outbox_dispatcher_configuration_matches(outbox, existing)) {
        if (existing->stopping) {
          pthread_mutex_unlock(&existing->lifecycle_mutex);
          pthread_mutex_unlock(&outbox->client->lifecycle_mutex);
          lc_outbox_dispatcher_retain(dispatcher);
          (void)lc_outbox_dispatcher_stop_method(&dispatcher->pub, -1L, NULL);
          lc_outbox_dispatcher_close_method(&dispatcher->pub);
          return lc_error_set(error, LC_ERR_INVALID, 0L,
                              "outbox dispatcher is stopping", NULL, NULL,
                              NULL);
        }
        /* One reference is returned to this caller. A second belongs to the
         * outbox attachment only when another creator has not already
         * published that exact attachment while this creator was allocating.
         */
        lc_outbox_dispatcher_retain_locked(existing);
        if (outbox->attached_dispatcher != existing)
          lc_outbox_dispatcher_retain_locked(existing);
        lc_outbox_replace_attached_dispatcher(outbox, existing);
        pthread_mutex_unlock(&existing->lifecycle_mutex);
        pthread_mutex_unlock(&outbox->client->lifecycle_mutex);
        lc_outbox_dispatcher_retain(dispatcher);
        (void)lc_outbox_dispatcher_stop_method(&dispatcher->pub, -1L, NULL);
        lc_outbox_dispatcher_close_method(&dispatcher->pub);
        *out = &existing->pub;
        return LC_OK;
      }
      pthread_mutex_unlock(&existing->lifecycle_mutex);
      existing = existing->registry_next;
    }
  }
  dispatcher->registry_next = outbox->client->outbox_dispatchers;
  outbox->client->outbox_dispatchers = dispatcher;
  pthread_mutex_lock(&dispatcher->lifecycle_mutex);
  lc_outbox_dispatcher_retain_locked(dispatcher);
  lc_outbox_dispatcher_retain_locked(dispatcher);
  pthread_mutex_unlock(&dispatcher->lifecycle_mutex);
  lc_outbox_replace_attached_dispatcher(outbox, dispatcher);
  pthread_mutex_unlock(&outbox->client->lifecycle_mutex);
  *out = &dispatcher->pub;
  return LC_OK;
}

int lc_client_new_outbox_with_dispatcher_method(
    lc_client *self, const lc_outbox_config *config,
    lc_outbox_dispatcher *dispatcher, lc_outbox **out, lc_error *error) {
  lc_outbox_dispatcher_handle *handle =
      (lc_outbox_dispatcher_handle *)dispatcher;
  lc_outbox_handle *outbox;
  int rc;

  if (out != NULL)
    *out = NULL;
  if (dispatcher == NULL || handle->client != (lc_client_handle *)self) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox dispatcher is not compatible with client", NULL,
                        NULL, NULL);
  }
  rc = lc_outbox_new(self, config, 0, out, error);
  if (rc != LC_OK)
    return rc;
  outbox = (lc_outbox_handle *)*out;
  /* Configuration comparison and attachment are one lifecycle transition, so
   * a racing stop cannot leave a producer holding a stopped dispatcher. */
  pthread_mutex_lock(&outbox->client->lifecycle_mutex);
  if (outbox->client->close_requested) {
    pthread_mutex_unlock(&outbox->client->lifecycle_mutex);
    lc_outbox_close(*out);
    *out = NULL;
    return lc_error_set(error, LC_ERR_INVALID, 0L, "client is closed", NULL,
                        NULL, NULL);
  }
  pthread_mutex_lock(&handle->lifecycle_mutex);
  if (handle->stopping ||
      !lc_outbox_dispatcher_configuration_matches(outbox, handle)) {
    pthread_mutex_unlock(&handle->lifecycle_mutex);
    pthread_mutex_unlock(&outbox->client->lifecycle_mutex);
    lc_outbox_close(*out);
    *out = NULL;
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox dispatcher configuration is incompatible", NULL,
                        NULL, NULL);
  }
  lc_outbox_dispatcher_retain_locked(handle);
  pthread_mutex_unlock(&handle->lifecycle_mutex);
  lc_outbox_replace_attached_dispatcher(outbox, handle);
  pthread_mutex_unlock(&outbox->client->lifecycle_mutex);
  return LC_OK;
}

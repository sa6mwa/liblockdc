#include "lc_api_internal.h"
#include "lc_pouch_internal.h"

#include <openssl/evp.h>
#include <openssl/rand.h>

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>

typedef struct lc_workflow_handle lc_workflow_handle;
typedef struct lc_workflow_transaction_handle lc_workflow_transaction_handle;
typedef struct lc_workflow_participant_handle lc_workflow_participant_handle;
typedef struct lc_outbox_job_handle lc_outbox_job_handle;

typedef struct lc_workflow_delayed_notification {
  char *key;
  lc_unix_seconds eligible_at_unix;
} lc_workflow_delayed_notification;

static void lc_workflow_release(lc_workflow_handle *workflow);
static void lc_workflow_retain(lc_workflow_handle *workflow);
static void lc_workflow_transaction_abort_enrolled_lease(
    lc_workflow_transaction_handle *transaction, lc_lease *lease);

#ifdef LOCKDC_TEST_BUILD
lc_workflow_test_after_reconcile_query_hook_fn
    lc_workflow_test_after_reconcile_query_hook = NULL;
void *lc_workflow_test_after_reconcile_query_context = NULL;
lc_workflow_test_hook_fn lc_workflow_test_after_close_requested_hook = NULL;
void *lc_workflow_test_after_close_requested_context = NULL;
lc_workflow_test_hook_fn lc_workflow_test_before_ready_job_detach_hook = NULL;
void *lc_workflow_test_before_ready_job_detach_context = NULL;
lc_workflow_test_hook_fn lc_workflow_test_before_ready_job_teardown_hook = NULL;
void *lc_workflow_test_before_ready_job_teardown_context = NULL;
lc_workflow_test_hook_fn lc_workflow_test_before_dispatcher_wait_hook = NULL;
void *lc_workflow_test_before_dispatcher_wait_context = NULL;
lc_workflow_test_hook_fn lc_workflow_test_before_next_wait_hook = NULL;
void *lc_workflow_test_before_next_wait_context = NULL;
lc_workflow_test_failure_hook_fn lc_workflow_test_before_ledger_append_hook =
    NULL;
void *lc_workflow_test_before_ledger_append_context = NULL;
lc_workflow_test_failure_hook_fn
    lc_workflow_test_before_participant_allocation_hook = NULL;
void *lc_workflow_test_before_participant_allocation_context = NULL;
lc_workflow_test_failure_hook_fn
    lc_workflow_test_before_command_receipt_copy_hook = NULL;
void *lc_workflow_test_before_command_receipt_copy_context = NULL;
lc_workflow_test_failure_hook_fn
    lc_workflow_test_before_outbox_receipt_copy_hook = NULL;
void *lc_workflow_test_before_outbox_receipt_copy_context = NULL;
lc_workflow_test_failure_hook_fn
    lc_workflow_test_before_notification_copy_hook = NULL;
void *lc_workflow_test_before_notification_copy_context = NULL;
lc_workflow_test_failure_hook_fn lc_workflow_test_before_claim_outbox_hook =
    NULL;
void *lc_workflow_test_before_claim_outbox_context = NULL;
#endif

typedef struct lc_workflow_outbox_record {
  char *record_type;
  char *operation_id;
  char *effect_id;
  char *effect_key;
  char *message_id;
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
} lc_workflow_outbox_record;

typedef struct lc_workflow_inbox_record {
  char *record_type;
  char *consumer_id;
  char *source_kind;
  char *source_id;
  char *message_id;
  char *payload_digest;
  char *operation_id;
  char *processing_state;
} lc_workflow_inbox_record;

typedef struct lc_workflow_command_record {
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
} lc_workflow_command_record;

static const lonejson_field lc_workflow_outbox_record_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_outbox_record, record_type,
                                    "record_type"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_outbox_record, operation_id,
                                    "operation_id"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_outbox_record, effect_id,
                                    "effect_id"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_outbox_record, effect_key,
                                    "effect_key"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_outbox_record, message_id,
                                    "message_id"),
    LONEJSON_FIELD_STRING_ALLOC(lc_workflow_outbox_record, causation_id,
                                "causation_id"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_outbox_record, kind, "kind"),
    LONEJSON_FIELD_STRING_ALLOC(lc_workflow_outbox_record, schema_version,
                                "schema_version"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_outbox_record, destination,
                                    "destination"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_outbox_record, content_type,
                                    "content_type"),
    LONEJSON_FIELD_STRING_ALLOC(lc_workflow_outbox_record, headers_json,
                                "headers_json"),
    LONEJSON_FIELD_STRING_ALLOC(lc_workflow_outbox_record, trace_context,
                                "trace_context"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_outbox_record, dispatch_state,
                                    "dispatch_state"),
    LONEJSON_FIELD_I64(lc_workflow_outbox_record, attempt_count,
                       "attempt_count"),
    LONEJSON_FIELD_I64(lc_workflow_outbox_record, claim_expires_at_unix,
                       "claim_expires_at_unix"),
    LONEJSON_FIELD_I64(lc_workflow_outbox_record, not_before_unix,
                       "not_before_unix"),
    LONEJSON_FIELD_I64(lc_workflow_outbox_record, replay_count, "replay_count"),
    LONEJSON_FIELD_I64(lc_workflow_outbox_record, dead_lettered_at_unix,
                       "dead_lettered_at_unix"),
    LONEJSON_FIELD_I64(lc_workflow_outbox_record, replayed_at_unix,
                       "replayed_at_unix"),
    LONEJSON_FIELD_STRING_ALLOC(lc_workflow_outbox_record, delivery_reference,
                                "delivery_reference"),
    LONEJSON_FIELD_STRING_ALLOC(lc_workflow_outbox_record, response_digest,
                                "response_digest"),
    LONEJSON_FIELD_I64(lc_workflow_outbox_record, completed_at_unix,
                       "completed_at_unix"),
    LONEJSON_FIELD_STRING_ALLOC(lc_workflow_outbox_record, last_error,
                                "last_error"),
    LONEJSON_FIELD_STRING_ALLOC(lc_workflow_outbox_record,
                                prior_dead_letter_error,
                                "prior_dead_letter_error")};

static const lonejson_field lc_workflow_inbox_record_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_inbox_record, record_type,
                                    "record_type"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_inbox_record, consumer_id,
                                    "consumer_id"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_inbox_record, source_kind,
                                    "source_kind"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_inbox_record, source_id,
                                    "source_id"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_inbox_record, message_id,
                                    "message_id"),
    LONEJSON_FIELD_STRING_ALLOC(lc_workflow_inbox_record, payload_digest,
                                "payload_digest"),
    LONEJSON_FIELD_STRING_ALLOC(lc_workflow_inbox_record, operation_id,
                                "operation_id"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_inbox_record, processing_state,
                                    "processing_state")};

static const lonejson_field lc_workflow_command_record_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_command_record, record_type,
                                    "record_type"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_command_record, command_id,
                                    "command_id"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_command_record, scope, "scope"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_command_record, command_type,
                                    "command_type"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_command_record, idempotency_key,
                                    "idempotency_key"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_command_record, request_digest,
                                    "request_digest"),
    LONEJSON_FIELD_STRING_ALLOC(lc_workflow_command_record, operation_id,
                                "operation_id"),
    LONEJSON_FIELD_I64(lc_workflow_command_record, accepted_at_unix,
                       "accepted_at_unix"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_command_record, state, "state"),
    LONEJSON_FIELD_STRING_ALLOC(lc_workflow_command_record, result_code,
                                "result_code"),
    LONEJSON_FIELD_STRING_ALLOC(lc_workflow_command_record, result_reference,
                                "result_reference"),
    LONEJSON_FIELD_STRING_ALLOC(lc_workflow_command_record, result_content_type,
                                "result_content_type"),
    LONEJSON_FIELD_I64(lc_workflow_command_record, completed_at_unix,
                       "completed_at_unix"),
    LONEJSON_FIELD_STRING_ALLOC(lc_workflow_command_record, failure_code,
                                "failure_code"),
    LONEJSON_FIELD_STRING_ALLOC(lc_workflow_command_record, failure_message,
                                "failure_message"),
    LONEJSON_FIELD_I64(lc_workflow_command_record, failed_at_unix,
                       "failed_at_unix"),
    LONEJSON_FIELD_I64(lc_workflow_command_record, has_result_body,
                       "has_result_body")};

LONEJSON_MAP_DEFINE(lc_workflow_outbox_record_map, lc_workflow_outbox_record,
                    lc_workflow_outbox_record_fields);
LONEJSON_MAP_DEFINE(lc_workflow_inbox_record_map, lc_workflow_inbox_record,
                    lc_workflow_inbox_record_fields);
LONEJSON_MAP_DEFINE(lc_workflow_command_record_map, lc_workflow_command_record,
                    lc_workflow_command_record_fields);

struct lc_workflow_handle {
  lc_workflow pub;
  lc_client_handle *client;
  lc_client_handle *dispatcher_client;
  char *namespace_name;
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
  lc_workflow_delayed_notification *delayed_notifications;
  size_t delayed_notification_count;
  char *recovery_cursor;
  int recovery_needed;
  int recovery_claims_pending;
  int recovery_scanning_claims;
  lc_unix_seconds next_recovery_unix;
  int notification_mutex_initialized;
  int notification_cond_initialized;
  int dispatcher_cond_initialized;
  pthread_t dispatcher_thread;
  int dispatcher_started;
  lc_outbox_job_handle *ready_head;
  lc_outbox_job_handle *ready_tail;
  size_t ready_count;
  uint64_t direct_notifications;
  uint64_t notification_overflows;
  uint64_t recovery_queries;
  uint64_t recovered_claims;
  uint64_t claim_losses;
  uint64_t payload_open_failures;
  char *last_error;
  int startup_dead_letter_replay_pending;
  int startup_dead_letter_replay_flushed;
  int closed;
  int close_requested;
  size_t ref_count;
};

static int lc_workflow_dispatcher_cancel_check(void *context) {
  lc_workflow_handle *workflow = (lc_workflow_handle *)context;
  int closed;

  pthread_mutex_lock(&workflow->notification_mutex);
  closed = workflow->closed;
  pthread_mutex_unlock(&workflow->notification_mutex);
  return closed;
}

static int
lc_workflow_replay_dead_letters_on_startup(lc_workflow_handle *workflow,
                                           lc_error *error);

struct lc_workflow_transaction_handle {
  lc_workflow_transaction pub;
  lc_workflow_handle *workflow;
  lc_lease **leases;
  size_t lease_count;
  size_t lease_capacity;
  lc_workflow_participant_handle *participants;
  lc_lease *command_lease;
  int command_causation_owned;
  char *causation_id;
  int command_terminal;
  int terminal;
};

struct lc_workflow_participant_handle {
  lc_workflow_participant pub;
  lc_workflow_handle *workflow;
  lc_workflow_transaction_handle *transaction;
  lc_lease *lease;
  lc_workflow_participant_handle *next;
};

struct lc_outbox_job_handle {
  lc_outbox_job pub;
  lc_client_handle *client;
  lc_workflow_handle *workflow;
  lc_lease *lease;
  char *outbox_key;
  lc_workflow_outbox_record record;
  int terminal;
  lc_outbox_job_handle *next;
};

/* The dispatcher and public next() callers wait for different predicates.
 * Keep their wakeups separate so a public waiter cannot consume the dispatch
 * signal that makes a durable outbox record eligible for a host job. */
static void lc_workflow_signal_dispatcher_locked(lc_workflow_handle *workflow) {
  if (workflow->dispatcher_cond_initialized)
    pthread_cond_signal(&workflow->dispatcher_cond);
}

static void lc_workflow_request_recovery(lc_workflow_handle *workflow,
                                         lc_unix_seconds not_before_unix) {
  if (workflow == NULL)
    return;
  pthread_mutex_lock(&workflow->notification_mutex);
  if (!workflow->closed) {
    workflow->recovery_needed = 1;
    if (not_before_unix > 0 &&
        (workflow->next_recovery_unix == 0 ||
         not_before_unix < workflow->next_recovery_unix)) {
      workflow->next_recovery_unix = not_before_unix;
    }
    lc_workflow_signal_dispatcher_locked(workflow);
  }
  pthread_mutex_unlock(&workflow->notification_mutex);
}

static void
lc_workflow_request_claim_recovery(lc_workflow_handle *workflow,
                                   lc_unix_seconds claim_expires_at_unix) {
  if (workflow == NULL || claim_expires_at_unix <= 0)
    return;
  pthread_mutex_lock(&workflow->notification_mutex);
  if (!workflow->closed) {
    workflow->recovery_needed = 1;
    workflow->recovery_claims_pending = 1;
    if (workflow->next_recovery_unix == 0 ||
        claim_expires_at_unix < workflow->next_recovery_unix) {
      workflow->next_recovery_unix = claim_expires_at_unix;
    }
    lc_workflow_signal_dispatcher_locked(workflow);
  }
  pthread_mutex_unlock(&workflow->notification_mutex);
}

static void lc_workflow_notify(lc_workflow_handle *workflow, const char *key) {
  char *copy;
  if (workflow == NULL || key == NULL ||
      strncmp(key, "__lockdc_io/v1/outbox/",
              sizeof("__lockdc_io/v1/outbox/") - 1U) != 0)
    return;
#ifdef LOCKDC_TEST_BUILD
  if (lc_workflow_test_before_notification_copy_hook != NULL &&
      lc_workflow_test_before_notification_copy_hook(
          lc_workflow_test_before_notification_copy_context, NULL) != LC_OK) {
    lc_workflow_request_recovery(workflow, 0);
    return;
  }
#endif
  copy = lc_client_strdup(workflow->client, key);
  if (copy == NULL) {
    lc_workflow_request_recovery(workflow, 0);
    return;
  }
  pthread_mutex_lock(&workflow->notification_mutex);
  if (!workflow->closed &&
      workflow->notification_count < workflow->notification_capacity) {
    workflow->notifications[workflow->notification_count++] = copy;
    ++workflow->direct_notifications;
    lc_workflow_signal_dispatcher_locked(workflow);
    copy = NULL;
  } else if (!workflow->closed) {
    workflow->recovery_needed = 1;
    ++workflow->notification_overflows;
    lc_workflow_signal_dispatcher_locked(workflow);
  }
  pthread_mutex_unlock(&workflow->notification_mutex);
  lc_client_free(workflow->client, copy);
}

static void lc_workflow_record_error(lc_workflow_handle *workflow,
                                     const lc_error *error) {
  char *copy;

  if (workflow == NULL || error == NULL || error->message == NULL)
    return;
  copy = lc_client_strdup(workflow->client, error->message);
  if (copy == NULL)
    return;
  pthread_mutex_lock(&workflow->notification_mutex);
  lc_client_free(workflow->client, workflow->last_error);
  workflow->last_error = copy;
  pthread_mutex_unlock(&workflow->notification_mutex);
}

static void lc_workflow_schedule_retry(lc_workflow_handle *workflow,
                                       const char *key,
                                       lc_unix_seconds eligible_at_unix) {
  char *copy;
  size_t index;

  if (workflow == NULL || key == NULL || eligible_at_unix <= 0)
    return;
#ifdef LOCKDC_TEST_BUILD
  if (lc_workflow_test_before_notification_copy_hook != NULL &&
      lc_workflow_test_before_notification_copy_hook(
          lc_workflow_test_before_notification_copy_context, NULL) != LC_OK) {
    lc_workflow_request_recovery(workflow, eligible_at_unix);
    return;
  }
#endif
  copy = lc_client_strdup(workflow->client, key);
  if (copy == NULL) {
    lc_workflow_request_recovery(workflow, eligible_at_unix);
    return;
  }
  pthread_mutex_lock(&workflow->notification_mutex);
  if (!workflow->closed) {
    for (index = 0U; index < workflow->delayed_notification_count; ++index) {
      lc_workflow_delayed_notification *delayed =
          &workflow->delayed_notifications[index];
      if (strcmp(delayed->key, key) == 0) {
        if (eligible_at_unix < delayed->eligible_at_unix) {
          delayed->eligible_at_unix = eligible_at_unix;
        }
        break;
      }
    }
    if (index == workflow->delayed_notification_count) {
      if (workflow->delayed_notification_count <
          workflow->notification_capacity) {
        lc_workflow_delayed_notification *delayed =
            &workflow->delayed_notifications
                 [workflow->delayed_notification_count++];
        delayed->key = copy;
        delayed->eligible_at_unix = eligible_at_unix;
        copy = NULL;
      } else {
        workflow->recovery_needed = 1;
        if (workflow->next_recovery_unix == 0 ||
            eligible_at_unix < workflow->next_recovery_unix) {
          workflow->next_recovery_unix = eligible_at_unix;
        }
      }
    }
    lc_workflow_signal_dispatcher_locked(workflow);
  }
  pthread_mutex_unlock(&workflow->notification_mutex);
  lc_client_free(workflow->client, copy);
}

/* A claimed envelope remains durable if its worker disappears. Unlike a retry
 * deadline, a successful keepalive must be able to move this deadline later. */
static void
lc_workflow_schedule_claim_recovery(lc_workflow_handle *workflow,
                                    const char *key,
                                    lc_unix_seconds claim_expires_at_unix) {
  char *copy;
  size_t index;

  if (workflow == NULL || key == NULL || claim_expires_at_unix <= 0)
    return;
#ifdef LOCKDC_TEST_BUILD
  if (lc_workflow_test_before_notification_copy_hook != NULL &&
      lc_workflow_test_before_notification_copy_hook(
          lc_workflow_test_before_notification_copy_context, NULL) != LC_OK) {
    lc_workflow_request_claim_recovery(workflow, claim_expires_at_unix);
    return;
  }
#endif
  copy = lc_client_strdup(workflow->client, key);
  if (copy == NULL) {
    lc_workflow_request_claim_recovery(workflow, claim_expires_at_unix);
    return;
  }
  pthread_mutex_lock(&workflow->notification_mutex);
  if (!workflow->closed) {
    for (index = 0U; index < workflow->delayed_notification_count; ++index) {
      lc_workflow_delayed_notification *delayed =
          &workflow->delayed_notifications[index];
      if (strcmp(delayed->key, key) == 0) {
        delayed->eligible_at_unix = claim_expires_at_unix;
        break;
      }
    }
    if (index == workflow->delayed_notification_count) {
      if (workflow->delayed_notification_count <
          workflow->notification_capacity) {
        lc_workflow_delayed_notification *delayed =
            &workflow->delayed_notifications
                 [workflow->delayed_notification_count++];
        delayed->key = copy;
        delayed->eligible_at_unix = claim_expires_at_unix;
        copy = NULL;
      } else {
        /* The key cannot be retained locally; recover the durable claim at
         * expiry rather than letting an idle local Pouch dispatcher strand it.
         */
        workflow->recovery_needed = 1;
        workflow->recovery_claims_pending = 1;
        if (workflow->next_recovery_unix == 0 ||
            claim_expires_at_unix < workflow->next_recovery_unix) {
          workflow->next_recovery_unix = claim_expires_at_unix;
        }
      }
    }
    lc_workflow_signal_dispatcher_locked(workflow);
  }
  pthread_mutex_unlock(&workflow->notification_mutex);
  lc_client_free(workflow->client, copy);
}

static void
lc_workflow_cancel_delayed_notification(lc_workflow_handle *workflow,
                                        const char *key) {
  size_t index;

  if (workflow == NULL || key == NULL)
    return;
  pthread_mutex_lock(&workflow->notification_mutex);
  for (index = 0U; index < workflow->delayed_notification_count; ++index) {
    if (strcmp(workflow->delayed_notifications[index].key, key) == 0) {
      lc_client_free(workflow->client,
                     workflow->delayed_notifications[index].key);
      --workflow->delayed_notification_count;
      if (index != workflow->delayed_notification_count) {
        workflow->delayed_notifications[index] =
            workflow
                ->delayed_notifications[workflow->delayed_notification_count];
      }
      break;
    }
  }
  pthread_mutex_unlock(&workflow->notification_mutex);
}

static void lc_workflow_promote_due_retries_locked(lc_workflow_handle *workflow,
                                                   lc_unix_seconds now) {
  size_t index;

  for (index = 0U; index < workflow->delayed_notification_count;) {
    lc_workflow_delayed_notification *delayed =
        &workflow->delayed_notifications[index];
    if (delayed->eligible_at_unix > now) {
      ++index;
      continue;
    }
    if (workflow->notification_count < workflow->notification_capacity) {
      workflow->notifications[workflow->notification_count++] = delayed->key;
      delayed->key = NULL;
    } else {
      lc_client_free(workflow->client, delayed->key);
      workflow->recovery_needed = 1;
    }
    --workflow->delayed_notification_count;
    if (index != workflow->delayed_notification_count) {
      workflow->delayed_notifications[index] =
          workflow->delayed_notifications[workflow->delayed_notification_count];
    }
  }
}

static lc_unix_seconds
lc_workflow_next_dispatch_deadline_locked(const lc_workflow_handle *workflow) {
  lc_unix_seconds deadline = workflow->next_recovery_unix;
  size_t index;

  for (index = 0U; index < workflow->delayed_notification_count; ++index) {
    lc_unix_seconds eligible =
        workflow->delayed_notifications[index].eligible_at_unix;
    if (deadline == 0 || eligible < deadline)
      deadline = eligible;
  }
  return deadline;
}

static int
lc_workflow_transaction_add_lease(lc_workflow_transaction_handle *transaction,
                                  lc_lease *lease, lc_error *error) {
  lc_lease **grown;
  size_t capacity;

#ifdef LOCKDC_TEST_BUILD
  if (lc_workflow_test_before_ledger_append_hook != NULL) {
    int rc = lc_workflow_test_before_ledger_append_hook(
        lc_workflow_test_before_ledger_append_context, error);

    if (rc != LC_OK)
      return rc;
  }
#endif
  if (transaction->lease_count == transaction->lease_capacity) {
    capacity = transaction->lease_capacity == 0U
                   ? 4U
                   : transaction->lease_capacity * 2U;
    grown = (lc_lease **)lc_client_realloc(
        transaction->workflow->client, transaction->leases,
        capacity * sizeof(*transaction->leases));
    if (grown == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to grow workflow participant ledger", NULL,
                          NULL, NULL);
    }
    transaction->leases = grown;
    transaction->lease_capacity = capacity;
  }
  transaction->leases[transaction->lease_count++] = lease;
  return LC_OK;
}

static void lc_workflow_transaction_remove_lease(
    lc_workflow_transaction_handle *transaction, lc_lease *lease) {
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

static int lc_workflow_digest(const char *value, char out[44],
                              lc_error *error) {
  EVP_MD_CTX *ctx;
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int length;
  static const char base64url[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  size_t i;

  if (value == NULL || value[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow identity must be non-empty", NULL, NULL,
                        NULL);
  }
  ctx = EVP_MD_CTX_new();
  length = 0U;
  if (ctx == NULL || EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) != 1 ||
      EVP_DigestUpdate(ctx, value, strlen(value)) != 1 ||
      EVP_DigestFinal_ex(ctx, digest, &length) != 1 || length != 32U) {
    EVP_MD_CTX_free(ctx);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "failed to digest workflow identity", NULL, NULL, NULL);
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

static int lc_workflow_outbox_key(lc_workflow_handle *workflow,
                                  const lc_outbox_entry *entry, char **out,
                                  lc_error *error) {
  char operation[44];
  char effect[44];
  size_t length;
  char *key;
  int rc;

  (void)workflow;

  if (entry == NULL || entry->operation_id == NULL ||
      entry->effect_id == NULL || entry->effect_key == NULL ||
      entry->kind == NULL || entry->destination == NULL ||
      entry->operation_id[0] == '\0' || entry->effect_id[0] == '\0' ||
      entry->effect_key[0] == '\0' || entry->kind[0] == '\0' ||
      entry->destination[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox operation, effect, effect key, kind, and "
                        "destination are required",
                        NULL, NULL, NULL);
  }
  rc = lc_workflow_digest(entry->operation_id, operation, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_workflow_digest(entry->effect_id, effect, error);
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

static int lc_workflow_is_outbox_key(const char *key) {
  static const char prefix[] = "__lockdc_io/v1/outbox/";

  return key != NULL && strncmp(key, prefix, sizeof(prefix) - 1U) == 0;
}

static int lc_workflow_digest_identity_part(EVP_MD_CTX *ctx, const char *part) {
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

static int lc_workflow_inbox_key(lc_workflow_handle *workflow,
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
      !lc_workflow_digest_identity_part(ctx, message->consumer_id) ||
      !lc_workflow_digest_identity_part(ctx, message->source_kind) ||
      !lc_workflow_digest_identity_part(ctx, message->source_id) ||
      !lc_workflow_digest_identity_part(ctx, message->message_id) ||
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
  (void)workflow;
  *out = key;
  return LC_OK;
}

static int lc_workflow_command_key(const lc_command_identity *identity,
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
    if (!lc_workflow_digest_identity_part(ctx, parts[index])) {
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

typedef struct lc_workflow_headers_json_validation {
  int root_is_object;
} lc_workflow_headers_json_validation;

static lonejson_status lc_workflow_headers_json_object_begin(
    void *context, const lonejson_value_path *path, lonejson_error *error) {
  lc_workflow_headers_json_validation *validation =
      (lc_workflow_headers_json_validation *)context;

  (void)error;
  if (validation != NULL && path != NULL && path->segment_count == 0U)
    validation->root_is_object = 1;
  return LONEJSON_STATUS_OK;
}

static int lc_workflow_validate_headers_json(const char *headers_json,
                                             lc_error *error) {
  lonejson_path_value_visitor visitor;
  lonejson_error json_error;
  lonejson *runtime;
  lonejson_status status;
  lc_workflow_headers_json_validation validation;

  if (headers_json == NULL)
    return LC_OK;
  runtime = lc_thread_lonejson_runtime();
  if (runtime == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize workflow JSON runtime", NULL,
                        NULL, NULL);
  }
  memset(&validation, 0, sizeof(validation));
  visitor = lonejson_default_path_value_visitor();
  visitor.object_begin = lc_workflow_headers_json_object_begin;
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

static int lc_workflow_validate_diagnostic(const char *diagnostic,
                                           lc_error *error) {
  size_t length;

  if (diagnostic == NULL)
    return LC_OK;
  for (length = 0U; length <= LC_WORKFLOW_MAX_DIAGNOSTIC_BYTES; ++length) {
    if (diagnostic[length] == '\0')
      return LC_OK;
  }
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "outbox diagnostic exceeds the retained size limit", NULL,
                      NULL, NULL);
}

static int lc_workflow_timestamp_add(lc_unix_seconds base, long delta,
                                     const char *field, lc_unix_seconds *out,
                                     lc_error *error) {
  char message[128];

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow timestamp addition requires output", NULL,
                        NULL, NULL);
  }
  if (delta < 0L) {
    snprintf(message, sizeof(message), "workflow %s must be non-negative",
             field != NULL ? field : "duration");
    return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL, NULL);
  }
  if ((uintmax_t)delta > (uintmax_t)LC_I64_MAX ||
      base > LC_I64_MAX - (lc_unix_seconds)delta) {
    snprintf(message, sizeof(message),
             "workflow %s exceeds supported timestamp range",
             field != NULL ? field : "duration");
    return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL, NULL);
  }
  *out = base + (lc_unix_seconds)delta;
  return LC_OK;
}

static int lc_workflow_command_receipt_from_record(
    const lc_workflow_command_record *record, lc_command_receipt *receipt,
    lc_error *error) {
  if (record == NULL || receipt == NULL || record->record_type == NULL ||
      record->command_id == NULL || record->scope == NULL ||
      record->command_type == NULL || record->idempotency_key == NULL ||
      record->state == NULL ||
      strcmp(record->record_type, "lockdc.command.v1") != 0) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "command receipt record is malformed", NULL, NULL,
                        NULL);
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
  else {
    lc_command_receipt_cleanup(receipt);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "command receipt state is invalid", NULL, NULL, NULL);
  }
  receipt->has_result_body = record->has_result_body != 0;
  return LC_OK;
}

static int lc_workflow_stage_command(lc_lease *lease,
                                     const lc_command_request *request,
                                     const char *command_id, lc_error *error) {
  lc_workflow_command_record record;
  time_t now;

  if (request == NULL || request->request_digest == NULL ||
      request->request_digest[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "command request digest is required", NULL, NULL, NULL);
  }
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
  return lc_lease_save(lease, &lc_workflow_command_record_map, &record, error);
}

static int lc_workflow_stage_inbox(lc_lease *lease,
                                   const lc_inbox_message *message,
                                   lc_error *error) {
  lc_workflow_inbox_record record;

  memset(&record, 0, sizeof(record));
  record.record_type = "lockdc.inbox.v1";
  record.consumer_id = (char *)message->consumer_id;
  record.source_kind = (char *)message->source_kind;
  record.source_id = (char *)message->source_id;
  record.message_id = (char *)message->message_id;
  record.payload_digest = (char *)message->payload_digest;
  record.operation_id = (char *)message->operation_id;
  record.processing_state = "accepted";
  return lc_lease_save(lease, &lc_workflow_inbox_record_map, &record, error);
}

static int lc_workflow_nullable_string_equal(const char *left,
                                             const char *right) {
  return left == right ||
         (left != NULL && right != NULL && strcmp(left, right) == 0);
}

static int lc_workflow_existing_outbox(lc_workflow_handle *workflow,
                                       const char *key,
                                       const lc_outbox_entry *entry,
                                       lc_outbox_receipt *receipt,
                                       lc_error *error) {
  lc_workflow_outbox_record record;
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
  runtime = lc_thread_lonejson_runtime();
  rc = lc_load_in_namespace(&workflow->client->pub, workflow->namespace_name,
                            key, &lc_workflow_outbox_record_map, &record,
                            &options, &result, error);
  if (rc == LC_OK && (result.no_content || record.record_type == NULL ||
                      record.operation_id == NULL || record.effect_id == NULL ||
                      record.effect_key == NULL || record.message_id == NULL ||
                      record.kind == NULL || record.destination == NULL ||
                      record.content_type == NULL ||
                      strcmp(record.record_type, "lockdc.outbox.v1") != 0 ||
                      strcmp(record.operation_id, entry->operation_id) != 0 ||
                      strcmp(record.effect_id, entry->effect_id) != 0 ||
                      strcmp(record.effect_key, entry->effect_key) != 0 ||
                      !lc_workflow_nullable_string_equal(record.causation_id,
                                                         entry->causation_id) ||
                      strcmp(record.kind, entry->kind) != 0 ||
                      !lc_workflow_nullable_string_equal(
                          record.schema_version, entry->schema_version) ||
                      strcmp(record.destination, entry->destination) != 0 ||
                      strcmp(record.content_type, content_type) != 0 ||
                      !lc_workflow_nullable_string_equal(record.headers_json,
                                                         entry->headers_json) ||
                      !lc_workflow_nullable_string_equal(
                          record.trace_context, entry->trace_context))) {
    rc =
        lc_error_set(error, LC_ERR_SERVER, 0L,
                     "outbox immutable fields conflict with an existing record",
                     NULL, NULL, NULL);
  }
  runtime->cleanup(runtime, &lc_workflow_outbox_record_map, &record);
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

static int lc_workflow_existing_inbox(lc_workflow_handle *workflow,
                                      const char *key,
                                      const lc_inbox_message *message,
                                      lc_inbox_accept_result *result,
                                      lc_error *error) {
  lc_workflow_inbox_record record;
  lc_get_res load_result;
  lc_get_opts options;
  lonejson *runtime;
  int rc;

  memset(&record, 0, sizeof(record));
  memset(&load_result, 0, sizeof(load_result));
  lc_get_opts_init(&options);
  options.public_read = 1;
  runtime = lc_thread_lonejson_runtime();
  rc = lc_load_in_namespace(&workflow->client->pub, workflow->namespace_name,
                            key, &lc_workflow_inbox_record_map, &record,
                            &options, &load_result, error);
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
       !lc_workflow_nullable_string_equal(record.operation_id,
                                          message->operation_id))) {
    rc = lc_error_set(error, LC_ERR_SERVER, 0L,
                      "inbox immutable fields conflict with an existing record",
                      NULL, NULL, NULL);
  }
  runtime->cleanup(runtime, &lc_workflow_inbox_record_map, &record);
  lc_get_res_cleanup(&load_result);
  if (rc == LC_OK)
    result->duplicate = 1;
  return rc;
}

static int lc_workflow_existing_command(lc_workflow_handle *workflow,
                                        const char *key,
                                        const lc_command_request *request,
                                        lc_command_receipt *receipt,
                                        lc_error *error) {
  lc_workflow_command_record record;
  lc_get_res load_result;
  lc_get_opts options;
  lonejson *runtime;
  int rc;

  memset(&record, 0, sizeof(record));
  memset(&load_result, 0, sizeof(load_result));
  lc_get_opts_init(&options);
  options.public_read = 1;
  runtime = lc_thread_lonejson_runtime();
  rc = lc_load_in_namespace(&workflow->client->pub, workflow->namespace_name,
                            key, &lc_workflow_command_record_map, &record,
                            &options, &load_result, error);
  if (rc == LC_OK &&
      (load_result.no_content || record.record_type == NULL ||
       record.scope == NULL || record.command_type == NULL ||
       record.idempotency_key == NULL || record.request_digest == NULL ||
       (request != NULL &&
        (strcmp(record.scope, request->identity.scope) != 0 ||
         strcmp(record.command_type, request->identity.command_type) != 0 ||
         strcmp(record.idempotency_key, request->identity.idempotency_key) !=
             0 ||
         strcmp(record.request_digest, request->request_digest) != 0 ||
         !lc_workflow_nullable_string_equal(record.operation_id,
                                            request->operation_id))))) {
    rc = lc_error_set(
        error, LC_ERR_SERVER, 0L,
        "command immutable fields conflict with an existing receipt", NULL,
        NULL, NULL);
  }
  if (rc == LC_OK)
    rc = lc_workflow_command_receipt_from_record(&record, receipt, error);
  runtime->cleanup(runtime, &lc_workflow_command_record_map, &record);
  lc_get_res_cleanup(&load_result);
  if (rc == LC_OK && request != NULL)
    receipt->duplicate = 1;
  return rc;
}

static int
lc_workflow_transaction_set_command(lc_workflow_transaction_handle *transaction,
                                    lc_lease *lease, const char *command_id,
                                    lc_error *error) {
  char *cause;

  if (transaction->command_lease != NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow transaction already owns a command receipt",
                        NULL, NULL, NULL);
  }
  if (transaction->causation_id == NULL) {
    cause = lc_client_strdup(transaction->workflow->client, command_id);
    if (cause == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to retain command causation identity", NULL,
                          NULL, NULL);
    }
    transaction->causation_id = cause;
    transaction->command_causation_owned = 1;
  }
  transaction->command_lease = lease;
  return LC_OK;
}

static void lc_workflow_transaction_clear_command(
    lc_workflow_transaction_handle *transaction, lc_lease *lease) {
  if (transaction == NULL || transaction->command_lease != lease)
    return;
  transaction->command_lease = NULL;
  transaction->command_terminal = 0;
  if (transaction->command_causation_owned) {
    lc_client_free(transaction->workflow->client, transaction->causation_id);
    transaction->causation_id = NULL;
  }
  transaction->command_causation_owned = 0;
}

static int
lc_workflow_stage_command_terminal(lc_workflow_transaction_handle *transaction,
                                   const lc_command_result *result, int failed,
                                   lc_error *error) {
  lc_workflow_command_record record;
  lc_workflow_command_record updated;
  lc_get_res load_result;
  lc_attach_req attach;
  lc_attach_res attach_result;
  time_t now;
  int rc;

  if (transaction == NULL || transaction->terminal ||
      transaction->command_lease == NULL || transaction->command_terminal ||
      result == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "an open transaction with a pending command is required", NULL, NULL,
        NULL);
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
  rc =
      lc_lease_load(transaction->command_lease, &lc_workflow_command_record_map,
                    &record, NULL, &load_result, error);
  if (rc != LC_OK)
    return rc;
  if (record.state == NULL || strcmp(record.state, "pending") != 0) {
    lc_thread_lonejson_runtime()->cleanup(
        lc_thread_lonejson_runtime(), &lc_workflow_command_record_map, &record);
    lc_get_res_cleanup(&load_result);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "command receipt is already terminal", NULL, NULL,
                        NULL);
  }
  now = time(NULL);
  if (now == (time_t)-1) {
    lc_thread_lonejson_runtime()->cleanup(
        lc_thread_lonejson_runtime(), &lc_workflow_command_record_map, &record);
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
  rc = lc_lease_save(transaction->command_lease,
                     &lc_workflow_command_record_map, &updated, error);
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
      lc_workflow_transaction_abort_enrolled_lease(transaction,
                                                   transaction->command_lease);
    }
  }
  lc_thread_lonejson_runtime()->cleanup(
      lc_thread_lonejson_runtime(), &lc_workflow_command_record_map, &record);
  lc_get_res_cleanup(&load_result);
  if (rc == LC_OK)
    transaction->command_terminal = 1;
  return rc;
}

static int lc_workflow_stage_outbox(lc_lease *lease,
                                    const lc_outbox_entry *entry,
                                    lc_source *payload, lc_error *error) {
  lc_workflow_outbox_record record;
  lc_attach_req attach;
  lc_attach_res attach_result;
  char message_digest[44];
  char message_id[48];
  int rc;

  if (payload == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox payload source is required", NULL, NULL, NULL);
  }
  rc = lc_workflow_validate_headers_json(entry->headers_json, error);
  if (rc != LC_OK)
    return rc;
  memset(&record, 0, sizeof(record));
  record.record_type = "lockdc.outbox.v1";
  record.operation_id = (char *)entry->operation_id;
  record.effect_id = (char *)entry->effect_id;
  record.effect_key = (char *)entry->effect_key;
  rc = lc_workflow_digest(lease->key, message_digest, error);
  if (rc != LC_OK)
    return rc;
  snprintf(message_id, sizeof(message_id), "msg_%s", message_digest);
  record.message_id = message_id;
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
  rc = lc_lease_save(lease, &lc_workflow_outbox_record_map, &record, error);
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

static void lc_workflow_outbox_record_clear(lc_client_handle *client,
                                            lc_workflow_outbox_record *record) {
  if (record == NULL)
    return;
  lc_client_free(client, record->record_type);
  lc_client_free(client, record->operation_id);
  lc_client_free(client, record->effect_id);
  lc_client_free(client, record->effect_key);
  lc_client_free(client, record->message_id);
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

static void
lc_workflow_outbox_record_loaded_clear(lc_workflow_outbox_record *record) {
  lonejson *runtime;

  if (record == NULL)
    return;
  runtime = lc_thread_lonejson_runtime();
  runtime->cleanup(runtime, &lc_workflow_outbox_record_map, record);
}

static int lc_workflow_outbox_record_copy(lc_client_handle *client,
                                          lc_workflow_outbox_record *dst,
                                          const lc_workflow_outbox_record *src,
                                          lc_error *error) {
  memset(dst, 0, sizeof(*dst));
  if ((src->record_type != NULL && (dst->record_type = lc_client_strdup(
                                        client, src->record_type)) == NULL) ||
      (src->operation_id != NULL && (dst->operation_id = lc_client_strdup(
                                         client, src->operation_id)) == NULL) ||
      (src->effect_id != NULL &&
       (dst->effect_id = lc_client_strdup(client, src->effect_id)) == NULL) ||
      (src->effect_key != NULL &&
       (dst->effect_key = lc_client_strdup(client, src->effect_key)) == NULL) ||
      (src->message_id != NULL &&
       (dst->message_id = lc_client_strdup(client, src->message_id)) == NULL) ||
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
    lc_workflow_outbox_record_clear(client, dst);
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
  lc_workflow_handle *workflow;

  if (job == NULL)
    return;
  client = job->client;
  workflow = job->workflow;
  if (job->lease != NULL) {
    /* The committed claim record is intentionally left for expiry recovery.
     * Keep the local dispatcher awake at that boundary even when its ordinary
     * Pouch recovery interval is disabled. */
    lc_workflow_schedule_claim_recovery(
        workflow, job->outbox_key,
        (lc_unix_seconds)job->lease->lease_expires_at_unix);
    job->lease->close(job->lease);
  }
  lc_workflow_outbox_record_clear(client, &job->record);
  lc_client_free(client, job->outbox_key);
  lc_client_free(client, job);
  if (workflow != NULL)
    lc_workflow_release(workflow);
  lc_client_close(&client->pub);
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
    pthread_mutex_lock(&job->workflow->notification_mutex);
    ++job->workflow->payload_open_failures;
    pthread_mutex_unlock(&job->workflow->notification_mutex);
    lc_workflow_record_error(job->workflow, error);
  }
  lc_attachment_get_res_cleanup(&result);
  return rc;
}

static int lc_outbox_job_renew_method(lc_outbox_job *self, long ttl_seconds,
                                      lc_error *error) {
  lc_outbox_job_handle *job = (lc_outbox_job_handle *)self;
  lc_keepalive_req request;
  int rc;

  if (job == NULL || job->terminal || job->lease == NULL || ttl_seconds < 1L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "active outbox job and positive claim ttl are required",
                        NULL, NULL, NULL);
  }
  lc_keepalive_req_init(&request);
  request.ttl_seconds = ttl_seconds;
  rc = lc_lease_keepalive(job->lease, &request, error);
  if (rc == LC_OK) {
    lc_outbox_job_refresh(job);
    lc_workflow_schedule_claim_recovery(
        job->workflow, job->outbox_key,
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
  lc_workflow_outbox_record record;
  lc_release_req release;
  int rc;

  if (job == NULL || job->terminal || job->lease == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, "outbox job is closed", NULL,
                        NULL, NULL);
  }
  rc = lc_workflow_validate_diagnostic(diagnostic, error);
  if (rc != LC_OK)
    return rc;
  record = job->record;
  record.dispatch_state = (char *)state;
  record.claim_expires_at_unix = 0;
  record.not_before_unix = not_before_unix;
  record.last_error = (char *)diagnostic;
  if (strcmp(state, "completed") == 0) {
    time_t now = time(NULL);

    if (now == (time_t)-1) {
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "failed to read workflow completion clock", NULL,
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
                          "failed to read workflow dead-letter clock", NULL,
                          NULL, NULL);
    }
    record.dead_lettered_at_unix = (lonejson_int64)now;
  }
  rc =
      lc_lease_save(job->lease, &lc_workflow_outbox_record_map, &record, error);
  if (rc != LC_OK)
    return rc;
  lc_release_req_init(&release);
  rc = lc_lease_release(job->lease, &release, error);
  if (rc != LC_OK)
    return rc;
  job->lease = NULL;
  job->terminal = 1;
  lc_outbox_job_refresh(job);
  lc_workflow_cancel_delayed_notification(job->workflow, job->outbox_key);
  if (strcmp(state, "retry_wait") == 0) {
    lc_workflow_schedule_retry(job->workflow, job->outbox_key,
                               (lc_unix_seconds)not_before_unix);
  }
  return LC_OK;
}

static int lc_outbox_job_complete_method(lc_outbox_job *self,
                                         const lc_outbox_completion *completion,
                                         lc_error *error) {
  return lc_outbox_job_terminal(self, "completed", 0L, NULL, completion, error);
}

static long lc_outbox_job_auto_retry_delay(const lc_outbox_job_handle *job) {
  long cap = job->workflow->retry_initial_delay_seconds;
  long maximum = job->workflow->retry_max_delay_seconds;
  long attempt = job->record.attempt_count;
  unsigned long random_value = 0UL;
  unsigned char random_bytes[sizeof(random_value)];

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
  return cap == 0L ? 0L : (long)(random_value % ((unsigned long)cap + 1UL));
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
      request->delay_seconds > job->workflow->host_retry_delay_max_seconds) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "retry delay exceeds the configured host maximum", NULL,
                        NULL, NULL);
  }
  delay = request->delay_seconds == 0L ? lc_outbox_job_auto_retry_delay(job)
                                       : request->delay_seconds;
  now = time(NULL);
  if (now == (time_t)-1) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "failed to read workflow retry clock", NULL, NULL,
                        NULL);
  }
  rc = lc_workflow_timestamp_add((lc_unix_seconds)now, delay, "retry delay",
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
static void lc_workflow_rollback_lease(lc_lease *lease) {
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
static int lc_workflow_pouch_shared_live_claim(lc_workflow_handle *workflow,
                                               const char *key) {
  lc_client_handle *client;
  lc_workflow_outbox_record record;
  lc_get_opts options;
  lc_get_res result;
  lonejson *runtime;
  lc_error load_error;
  time_t now;
  int rc;
  int active;

  client = workflow->dispatcher_client;
  if (!client->is_pouch || lc_pouch_single_writer_enabled(client->pouch))
    return 0;
  memset(&record, 0, sizeof(record));
  memset(&result, 0, sizeof(result));
  lc_get_opts_init(&options);
  options.public_read = 1;
  lc_error_init(&load_error);
  runtime = lc_thread_lonejson_runtime();
  rc = lc_load_in_namespace(&client->pub, workflow->namespace_name, key,
                            &lc_workflow_outbox_record_map, &record, &options,
                            &result, &load_error);
  now = time(NULL);
  active = rc == LC_OK && now != (time_t)-1 && !result.no_content &&
           record.record_type != NULL &&
           strcmp(record.record_type, "lockdc.outbox.v1") == 0 &&
           record.dispatch_state != NULL &&
           strcmp(record.dispatch_state, "claimed") == 0 &&
           record.claim_expires_at_unix > (lonejson_int64)now;
  if (active) {
    lc_workflow_schedule_claim_recovery(
        workflow, key, (lc_unix_seconds)record.claim_expires_at_unix);
  }
  runtime->cleanup(runtime, &lc_workflow_outbox_record_map, &record);
  lc_get_res_cleanup(&result);
  lc_error_cleanup(&load_error);
  return active;
}

/* A preflight read eliminates the normal shared-writer hand-off race. Keep a
 * small bounded backoff for the remaining read/commit race without using the
 * Pouch acquire API's one-second polling contract. */
static int lc_workflow_reacquire_durable_claim(lc_workflow_handle *workflow,
                                               const lc_acquire_req *acquire,
                                               lc_lease **lease,
                                               lc_error *error) {
  enum { LC_WORKFLOW_SHARED_HANDOFF_RETRIES = 20 };
  lc_client_handle *client = workflow->dispatcher_client;
  int shared_pouch =
      client->is_pouch && !lc_pouch_single_writer_enabled(client->pouch);
  unsigned int attempt;
  int rc;

  for (attempt = 0U;; ++attempt) {
    rc = lc_acquire(&client->pub, acquire, lease, error);
    if (rc == LC_OK || !shared_pouch || rc != LC_ERR_INVALID ||
        attempt == LC_WORKFLOW_SHARED_HANDOFF_RETRIES) {
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

static int lc_workflow_claim_outbox(lc_workflow_handle *workflow,
                                    const char *key, lc_outbox_job **out,
                                    lc_error *error) {
  lc_client_handle *client = workflow->dispatcher_client;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_workflow_outbox_record record;
  lc_workflow_outbox_record verified;
  lc_get_res result;
  lc_get_res verify_result;
  lonejson *runtime;
  lc_outbox_job_handle *job;
  time_t now;
  lc_unix_seconds claim_expires_at_unix = 0;
  char *original_dispatch_state;
  char *original_last_error;
  lonejson_int64 original_attempt_count;
  lonejson_int64 original_claim_expires_at_unix;
  int recovered_to_pending;
  int rc;

  *out = NULL;
#ifdef LOCKDC_TEST_BUILD
  if (lc_workflow_test_before_claim_outbox_hook != NULL) {
    rc = lc_workflow_test_before_claim_outbox_hook(
        lc_workflow_test_before_claim_outbox_context, error);
    if (rc != LC_OK)
      return rc;
  }
#endif
  lc_acquire_req_init(&acquire);
  acquire.namespace_name = workflow->namespace_name;
  acquire.key = key;
  acquire.owner = workflow->owner;
  acquire.ttl_seconds = workflow->claim_ttl_seconds;
  lease = NULL;
  now = time(NULL);
  if (now == (time_t)-1) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "failed to read workflow claim clock", NULL, NULL,
                        NULL);
  }
  rc = lc_workflow_timestamp_add((lc_unix_seconds)now,
                                 workflow->claim_ttl_seconds, "claim ttl",
                                 &claim_expires_at_unix, error);
  if (rc != LC_OK)
    return rc;
  if (lc_workflow_pouch_shared_live_claim(workflow, key)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox candidate remains actively claimed", NULL, NULL,
                        NULL);
  }
  rc = lc_acquire(&client->pub, &acquire, &lease, error);
  if (rc != LC_OK) {
    lc_workflow_schedule_claim_recovery(workflow, key, claim_expires_at_unix);
    return rc;
  }
  memset(&record, 0, sizeof(record));
  memset(&verified, 0, sizeof(verified));
  memset(&result, 0, sizeof(result));
  memset(&verify_result, 0, sizeof(verify_result));
  runtime = lc_thread_lonejson_runtime();
  job = NULL;
  recovered_to_pending = 0;
  rc = lc_lease_load(lease, &lc_workflow_outbox_record_map, &record, NULL,
                     &result, error);
  if (rc == LC_OK) {
    now = time(NULL);
    if (now == (time_t)-1 ||
        strcmp(record.record_type, "lockdc.outbox.v1") != 0 ||
        (strcmp(record.dispatch_state, "pending") != 0 &&
         strcmp(record.dispatch_state, "retry_wait") != 0 &&
         strcmp(record.dispatch_state, "claimed") != 0)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox candidate is not currently dispatchable", NULL,
                        NULL, NULL);
    } else if (strcmp(record.dispatch_state, "retry_wait") == 0 &&
               record.not_before_unix > (long)now) {
      lc_workflow_schedule_retry(workflow, key,
                                 (lc_unix_seconds)record.not_before_unix);
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox candidate retry is not yet eligible", NULL,
                        NULL, NULL);
    }
  }

  if (rc == LC_OK && strcmp(record.dispatch_state, "claimed") == 0) {
    if (record.claim_expires_at_unix > (lonejson_int64)now) {
      lc_workflow_schedule_claim_recovery(
          workflow, key, (lc_unix_seconds)record.claim_expires_at_unix);
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox candidate remains actively claimed", NULL, NULL,
                        NULL);
    } else {
      original_dispatch_state = record.dispatch_state;
      original_last_error = record.last_error;
      original_claim_expires_at_unix = record.claim_expires_at_unix;
      recovered_to_pending = record.attempt_count < workflow->max_attempts;
      record.dispatch_state = recovered_to_pending ? "pending" : "dead_letter";
      record.claim_expires_at_unix = 0;
      if (recovered_to_pending) {
        record.last_error = "claim expired before terminal outcome";
      } else {
        record.last_error = "delivery attempt budget exhausted by claim expiry";
        record.dead_lettered_at_unix = (lonejson_int64)now;
      }
      rc = lc_lease_save(lease, &lc_workflow_outbox_record_map, &record, error);
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
        lc_workflow_notify(workflow, key);
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
    rc = lc_workflow_timestamp_add((lc_unix_seconds)now,
                                   workflow->claim_ttl_seconds, "claim ttl",
                                   &claim_expires_at_unix, error);
    if (rc != LC_OK) {
      runtime->cleanup(runtime, &lc_workflow_outbox_record_map, &record);
      lc_get_res_cleanup(&result);
      lc_workflow_rollback_lease(lease);
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
      rc = lc_workflow_outbox_record_copy(client, &job->record, &record, error);
    if (rc == LC_OK) {
      job->outbox_key = lc_client_strdup(client, key);
      if (job->outbox_key == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to copy outbox job key", NULL, NULL, NULL);
      }
    }
    if (rc == LC_OK)
      rc = lc_lease_save(lease, &lc_workflow_outbox_record_map, &record, error);
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
  runtime->cleanup(runtime, &lc_workflow_outbox_record_map, &record);
  lc_get_res_cleanup(&result);
  if (rc != LC_OK) {
    lc_workflow_rollback_lease(lease);
    if (job != NULL) {
      lc_workflow_outbox_record_clear(client, &job->record);
      lc_client_free(client, job->outbox_key);
      lc_client_free(client, job);
    }
    return rc;
  }

  /* The durable claim is now visible and has spent its attempt. A fresh lease
   * fences the host's terminal action; a crash in this hand-off recovers at
   * the durable claim deadline. */
  rc = lc_workflow_reacquire_durable_claim(workflow, &acquire, &lease, error);
  if (rc != LC_OK) {
    lc_workflow_schedule_claim_recovery(workflow, key, claim_expires_at_unix);
    lc_workflow_outbox_record_clear(client, &job->record);
    lc_client_free(client, job->outbox_key);
    lc_client_free(client, job);
    return rc;
  }
  rc = lc_lease_load(lease, &lc_workflow_outbox_record_map, &verified, NULL,
                     &verify_result, error);
  if (rc == LC_OK &&
      (strcmp(verified.dispatch_state, "claimed") != 0 ||
       verified.attempt_count != job->record.attempt_count ||
       verified.claim_expires_at_unix != job->record.claim_expires_at_unix)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "durable outbox claim changed before hand-off", NULL,
                      NULL, NULL);
  }
  runtime->cleanup(runtime, &lc_workflow_outbox_record_map, &verified);
  lc_get_res_cleanup(&verify_result);
  if (rc != LC_OK) {
    lc_workflow_rollback_lease(lease);
    lc_workflow_schedule_claim_recovery(workflow, key, claim_expires_at_unix);
    lc_workflow_outbox_record_clear(client, &job->record);
    lc_client_free(client, job->outbox_key);
    lc_client_free(client, job);
    return rc;
  }
  lc_client_handle_retain(client);
  job->client = client;
  job->workflow = workflow;
  lc_workflow_retain(workflow);
  job->lease = lease;
  job->pub.write_payload = lc_outbox_job_write_payload_method;
  job->pub.renew = lc_outbox_job_renew_method;
  job->pub.complete = lc_outbox_job_complete_method;
  job->pub.retry = lc_outbox_job_retry_method;
  job->pub.dead_letter = lc_outbox_job_dead_letter_method;
  job->pub.close = lc_outbox_job_close_method;
  job->pub.max_attempts = workflow->max_attempts;
  lc_outbox_job_refresh(job);
  lc_workflow_schedule_claim_recovery(
      workflow, key, (lc_unix_seconds)lease->lease_expires_at_unix);
  *out = &job->pub;
  return LC_OK;
}

typedef struct lc_workflow_recovery_capture {
  lc_workflow_handle *workflow;
  char key[129];
  size_t length;
} lc_workflow_recovery_capture;

static int lc_workflow_recovery_key_begin(void *context, lc_error *error) {
  lc_workflow_recovery_capture *capture =
      (lc_workflow_recovery_capture *)context;
  (void)error;
  capture->length = 0U;
  return 1;
}

static int lc_workflow_recovery_key_chunk(void *context, const char *bytes,
                                          size_t length, lc_error *error) {
  lc_workflow_recovery_capture *capture =
      (lc_workflow_recovery_capture *)context;
  if (length > sizeof(capture->key) - 1U - capture->length) {
    lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                 "workflow recovery received an oversized key", NULL, NULL,
                 NULL);
    return 0;
  }
  memcpy(capture->key + capture->length, bytes, length);
  capture->length += length;
  return 1;
}

static int lc_workflow_recovery_key_end(void *context, lc_error *error) {
  lc_workflow_recovery_capture *capture =
      (lc_workflow_recovery_capture *)context;
  (void)error;
  capture->key[capture->length] = '\0';
  if (!lc_workflow_is_outbox_key(capture->key))
    return 1;
  lc_workflow_notify(capture->workflow, capture->key);
  pthread_mutex_lock(&capture->workflow->notification_mutex);
  ++capture->workflow->recovered_claims;
  pthread_mutex_unlock(&capture->workflow->notification_mutex);
  return 1;
}

static int lc_workflow_reconcile_pending(lc_workflow_handle *workflow,
                                         lc_error *error) {
  static const char dispatchable_selector[] =
      "{\"in\":{\"field\":\"/"
      "dispatch_state\",\"any\":[\"pending\",\"retry_wait\"]}}";
  static const char claimed_selector[] =
      "{\"eq\":{\"field\":\"/dispatch_state\",\"value\":\"claimed\"}}";
  lc_query_req request;
  lc_query_key_handler handler;
  lc_query_res result;
  lc_index_flush_req flush_request;
  lc_index_flush_res flush_result;
  lc_workflow_recovery_capture capture;
  char *cursor;
  int scanning_claims;
  int rc;

  if (workflow->startup_dead_letter_replay_pending) {
    rc = lc_workflow_replay_dead_letters_on_startup(workflow, error);
    if (rc != LC_OK)
      return rc;
  }

  lc_query_req_init(&request);
  lc_index_flush_req_init(&flush_request);
  memset(&handler, 0, sizeof(handler));
  memset(&result, 0, sizeof(result));
  memset(&flush_result, 0, sizeof(flush_result));
  memset(&capture, 0, sizeof(capture));
  pthread_mutex_lock(&workflow->notification_mutex);
  ++workflow->recovery_queries;
  pthread_mutex_unlock(&workflow->notification_mutex);
  handler.begin = lc_workflow_recovery_key_begin;
  handler.chunk = lc_workflow_recovery_key_chunk;
  handler.end = lc_workflow_recovery_key_end;
  capture.workflow = workflow;
  pthread_mutex_lock(&workflow->notification_mutex);
  if (workflow->recovery_cursor == NULL) {
    workflow->recovery_scanning_claims = workflow->recovery_claims_pending;
    /* A signal raised while this query runs belongs to the next sweep. */
    workflow->recovery_claims_pending = 0;
  }
  scanning_claims = workflow->recovery_scanning_claims;
  pthread_mutex_unlock(&workflow->notification_mutex);
  /* A recovery sweep starts from a durable index boundary. Later pages keep
   * that boundary: flushing every page would turn one large sweep into N
   * global flushes and needlessly amplify reconciliation cost. */
  if (workflow->recovery_cursor == NULL) {
    flush_request.namespace_name = workflow->namespace_name;
    flush_request.mode = "wait";
    rc = lc_flush_index(&workflow->dispatcher_client->pub, &flush_request,
                        &flush_result, error);
    lc_index_flush_res_cleanup(&flush_result);
    if (rc != LC_OK)
      return rc;
  }
  request.namespace_name = workflow->namespace_name;
  request.selector_json =
      scanning_claims ? claimed_selector : dispatchable_selector;
  request.limit = (long)workflow->notification_capacity;
  request.cursor = workflow->recovery_cursor;
  request.engine = "index";
  request.refresh = "wait_for";
  rc = lc_query_keys(&workflow->dispatcher_client->pub, &request, &handler,
                     &capture, &result, error);
  if (rc != LC_OK) {
    lc_query_res_cleanup(&result);
    return rc;
  }
  cursor = result.cursor;
  result.cursor = NULL;
  lc_query_res_cleanup(&result);
#ifdef LOCKDC_TEST_BUILD
  if (lc_workflow_test_after_reconcile_query_hook != NULL) {
    lc_workflow_test_after_reconcile_query_hook(
        lc_workflow_test_after_reconcile_query_context);
  }
#endif
  /* Query cursors are public-result fields allocated in the libc domain; keep
   * their ownership consistent with lc_query_res_cleanup(). */
  free(workflow->recovery_cursor);
  workflow->recovery_cursor = cursor;
  pthread_mutex_lock(&workflow->notification_mutex);
  /* The dispatcher clears the request before starting this sweep. Keep an
   * overflow signal raised while the query was in flight, otherwise the final
   * page could strand a newly committed durable key until a restart. */
  if (workflow->recovery_cursor == NULL && scanning_claims) {
    workflow->recovery_scanning_claims = 0;
    /* The startup or periodic claim pass is only recovery preparation. Follow
     * it with the ordinary dispatchable pass without making claims part of
     * every hot reconciliation sweep. */
    workflow->recovery_needed = 1;
  } else {
    workflow->recovery_needed =
        workflow->recovery_needed || workflow->recovery_cursor != NULL;
  }
  pthread_mutex_unlock(&workflow->notification_mutex);
  return LC_OK;
}

static void *lc_workflow_dispatcher_main(void *context) {
  lc_workflow_handle *workflow = (lc_workflow_handle *)context;

  for (;;) {
    char *key;
    lc_outbox_job *job;
    lc_error error;
    int reconcile;

    key = NULL;
    reconcile = 0;
    pthread_mutex_lock(&workflow->notification_mutex);
    while (
        !workflow->closed &&
        (workflow->ready_count >= workflow->notification_capacity ||
         (workflow->notification_count == 0U && !workflow->recovery_needed))) {
      lc_unix_seconds now = (lc_unix_seconds)time(NULL);
      lc_unix_seconds due;
      if (workflow->ready_count >= workflow->notification_capacity) {
        pthread_cond_wait(&workflow->dispatcher_cond,
                          &workflow->notification_mutex);
        continue;
      }
      if (now > 0)
        lc_workflow_promote_due_retries_locked(workflow, now);
      if (workflow->notification_count > 0U || workflow->recovery_needed) {
        break;
      }
      due = lc_workflow_next_dispatch_deadline_locked(workflow);
      if (due > 0) {
        struct timespec deadline;
        deadline.tv_sec = due;
        deadline.tv_nsec = 0L;
        if (pthread_cond_timedwait(&workflow->dispatcher_cond,
                                   &workflow->notification_mutex,
                                   &deadline) == ETIMEDOUT) {
          now = (lc_unix_seconds)time(NULL);
          if (now > 0)
            lc_workflow_promote_due_retries_locked(workflow, now);
          if (workflow->notification_count == 0U &&
              workflow->next_recovery_unix > 0 &&
              now >= workflow->next_recovery_unix) {
            workflow->recovery_needed = 1;
            workflow->recovery_claims_pending = 1;
          }
        }
      } else {
#ifdef LOCKDC_TEST_BUILD
        if (lc_workflow_test_before_dispatcher_wait_hook != NULL) {
          lc_workflow_test_before_dispatcher_wait_hook(
              lc_workflow_test_before_dispatcher_wait_context);
        }
#endif
        pthread_cond_wait(&workflow->dispatcher_cond,
                          &workflow->notification_mutex);
      }
    }
    if (workflow->closed) {
      pthread_mutex_unlock(&workflow->notification_mutex);
      break;
    }
    if (workflow->notification_count == 0U) {
      reconcile = 1;
      /* A new overflow while reconciliation runs must be distinguishable from
       * the request that selected this sweep. */
      workflow->recovery_needed = 0;
    } else {
      key = workflow->notifications[0];
      if (workflow->notification_count > 1U) {
        memmove(workflow->notifications, workflow->notifications + 1U,
                (workflow->notification_count - 1U) *
                    sizeof(*workflow->notifications));
      }
      --workflow->notification_count;
    }
    pthread_mutex_unlock(&workflow->notification_mutex);

    if (reconcile) {
      int recovery_rc;
      lc_error_init(&error);
      recovery_rc = lc_workflow_reconcile_pending(workflow, &error);
      if (recovery_rc != LC_OK)
        lc_workflow_record_error(workflow, &error);
      lc_error_cleanup(&error);
      pthread_mutex_lock(&workflow->notification_mutex);
      if (recovery_rc != LC_OK) {
        workflow->recovery_needed = 0;
        workflow->next_recovery_unix = (lc_unix_seconds)time(NULL) + 1L;
      } else {
        lc_unix_seconds now = (lc_unix_seconds)time(NULL);
        lc_unix_seconds periodic =
            workflow->recovery_interval_seconds > 0L
                ? now + workflow->recovery_interval_seconds
                : 0;

        /* A retry whose delayed-key allocation failed has no in-memory entry,
         * so retain its durable-recovery deadline across this eager sweep. */
        if (workflow->next_recovery_unix <= now ||
            (periodic > 0 && (workflow->next_recovery_unix == 0 ||
                              periodic < workflow->next_recovery_unix))) {
          workflow->next_recovery_unix = periodic;
        }
      }
      pthread_mutex_unlock(&workflow->notification_mutex);
      continue;
    }

    job = NULL;
    lc_error_init(&error);
    if (lc_workflow_claim_outbox(workflow, key, &job, &error) == LC_OK) {
      pthread_mutex_lock(&workflow->notification_mutex);
      if (!workflow->closed) {
        lc_outbox_job_handle *handle = (lc_outbox_job_handle *)job;
        handle->next = NULL;
        if (workflow->ready_tail != NULL)
          workflow->ready_tail->next = handle;
        else
          workflow->ready_head = handle;
        workflow->ready_tail = handle;
        ++workflow->ready_count;
        pthread_cond_broadcast(&workflow->notification_cond);
        job = NULL;
      }
      pthread_mutex_unlock(&workflow->notification_mutex);
    } else {
      pthread_mutex_lock(&workflow->notification_mutex);
      ++workflow->claim_losses;
      pthread_mutex_unlock(&workflow->notification_mutex);
      lc_workflow_record_error(workflow, &error);
      if (error.code != LC_ERR_INVALID) {
        time_t now = time(NULL);

        /* A failed foreground claim has consumed its only direct signal, but
         * has not changed the durable outbox record. Requeue it with the
         * normal bounded delay; if that key allocation fails, the scheduler's
         * durable-recovery fallback retains the deadline. */
        if (now != (time_t)-1) {
          lc_workflow_schedule_retry(workflow, key,
                                     (lc_unix_seconds)now +
                                         workflow->retry_initial_delay_seconds);
        } else {
          lc_workflow_request_recovery(workflow, 0);
        }
      }
    }
    if (job != NULL)
      job->close(job);
    lc_error_cleanup(&error);
    lc_client_free(workflow->client, key);
  }
  return NULL;
}

static int lc_workflow_wait_for_ready(lc_workflow_handle *workflow,
                                      long timeout_ms, lc_outbox_job **out,
                                      lc_error *error) {
  struct timespec deadline;
  int wait_rc;

  if (timeout_ms < -1L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow next timeout must be -1 or non-negative",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  if (timeout_ms > 0L) {
    if (clock_gettime(CLOCK_REALTIME, &deadline) != 0) {
      return lc_error_set(error, LC_ERR_PROTOCOL, errno,
                          "failed to construct workflow wait deadline", NULL,
                          NULL, NULL);
    }
    deadline.tv_sec += timeout_ms / 1000L;
    deadline.tv_nsec += (timeout_ms % 1000L) * 1000000L;
    if (deadline.tv_nsec >= 1000000000L) {
      ++deadline.tv_sec;
      deadline.tv_nsec -= 1000000000L;
    }
  }
  pthread_mutex_lock(&workflow->notification_mutex);
  while (!workflow->closed && workflow->ready_head == NULL) {
    if (timeout_ms == 0L)
      break;
#ifdef LOCKDC_TEST_BUILD
    if (lc_workflow_test_before_next_wait_hook != NULL) {
      lc_workflow_test_before_next_wait_hook(
          lc_workflow_test_before_next_wait_context);
    }
#endif
    if (timeout_ms < 0L) {
      wait_rc = pthread_cond_wait(&workflow->notification_cond,
                                  &workflow->notification_mutex);
    } else {
      wait_rc =
          pthread_cond_timedwait(&workflow->notification_cond,
                                 &workflow->notification_mutex, &deadline);
    }
    if (wait_rc == ETIMEDOUT)
      break;
    if (wait_rc != 0) {
      pthread_mutex_unlock(&workflow->notification_mutex);
      return lc_error_set(error, LC_ERR_PROTOCOL, wait_rc,
                          "workflow dispatcher wait failed", NULL, NULL, NULL);
    }
  }
  if (workflow->ready_head != NULL) {
    lc_outbox_job_handle *job = workflow->ready_head;
#ifdef LOCKDC_TEST_BUILD
    if (lc_workflow_test_before_ready_job_detach_hook != NULL) {
      lc_workflow_test_before_ready_job_detach_hook(
          lc_workflow_test_before_ready_job_detach_context);
    }
#endif
    workflow->ready_head = job->next;
    if (workflow->ready_head == NULL)
      workflow->ready_tail = NULL;
    job->next = NULL;
    --workflow->ready_count;
    pthread_cond_broadcast(&workflow->notification_cond);
    lc_workflow_signal_dispatcher_locked(workflow);
    *out = &job->pub;
  }
  pthread_mutex_unlock(&workflow->notification_mutex);
  return LC_OK;
}

#ifdef LOCKDC_TEST_BUILD
void lc_workflow_test_wake_next_waiters(lc_workflow *self) {
  lc_workflow_handle *workflow = (lc_workflow_handle *)self;

  if (workflow == NULL || !workflow->notification_mutex_initialized ||
      !workflow->notification_cond_initialized)
    return;
  pthread_mutex_lock(&workflow->notification_mutex);
  pthread_cond_broadcast(&workflow->notification_cond);
  pthread_mutex_unlock(&workflow->notification_mutex);
}
#endif

static void lc_workflow_participant_refresh(lc_workflow_participant_handle *p) {
  if (p->lease == NULL) {
    return;
  }
  p->pub.namespace_name = p->lease->namespace_name;
  p->pub.key = p->lease->key;
  p->pub.txn_id = p->lease->txn_id;
  p->pub.fencing_token = p->lease->fencing_token;
  p->pub.version = p->lease->version;
  p->pub.state_etag = p->lease->state_etag;
}

static void
lc_workflow_participant_invalidate(lc_workflow_participant_handle *p) {
  if (p == NULL)
    return;
  p->transaction = NULL;
  p->lease = NULL;
  p->next = NULL;
  p->pub.namespace_name = NULL;
  p->pub.key = NULL;
  p->pub.txn_id = NULL;
  p->pub.fencing_token = 0L;
  p->pub.version = 0;
  p->pub.state_etag = NULL;
}

static void lc_workflow_transaction_remove_participant(
    lc_workflow_transaction_handle *transaction,
    lc_workflow_participant_handle *participant) {
  lc_workflow_participant_handle **cursor;

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

static void lc_workflow_transaction_invalidate_lease_participant(
    lc_workflow_transaction_handle *transaction, lc_lease *lease) {
  lc_workflow_participant_handle **cursor;

  if (transaction == NULL || lease == NULL)
    return;
  cursor = &transaction->participants;
  while (*cursor != NULL) {
    lc_workflow_participant_handle *participant = *cursor;

    if (participant->lease == lease) {
      *cursor = participant->next;
      lc_workflow_participant_invalidate(participant);
      return;
    }
    cursor = &participant->next;
  }
}

static void lc_workflow_transaction_invalidate_participants(
    lc_workflow_transaction_handle *transaction) {
  lc_workflow_participant_handle *participant;

  if (transaction == NULL)
    return;
  participant = transaction->participants;
  transaction->participants = NULL;
  while (participant != NULL) {
    lc_workflow_participant_handle *next = participant->next;

    lc_workflow_participant_invalidate(participant);
    participant = next;
  }
}

static int lc_workflow_participant_describe(lc_workflow_participant *self,
                                            lc_error *error) {
  lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self;
  int rc;
  if (p == NULL || p->lease == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow participant is closed", NULL, NULL, NULL);
  rc = lc_lease_describe(p->lease, error);
  lc_workflow_participant_refresh(p);
  return rc;
}
static int lc_workflow_participant_get(lc_workflow_participant *self,
                                       lc_sink *dst, const lc_get_opts *opts,
                                       lc_get_res *out, lc_error *error) {
  lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self;
  if (p == NULL || p->lease == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow participant is closed", NULL, NULL, NULL);
  return lc_lease_get(p->lease, dst, opts, out, error);
}
static int lc_workflow_participant_update(lc_workflow_participant *self,
                                          lc_source *src,
                                          const lc_update_opts *opts,
                                          lc_error *error) {
  lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self;
  int rc;
  if (p == NULL || p->lease == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow participant is closed", NULL, NULL, NULL);
  rc = lc_lease_update(p->lease, src, opts, error);
  lc_workflow_participant_refresh(p);
  return rc;
}
static int lc_workflow_participant_mutate(lc_workflow_participant *self,
                                          const lc_mutate_req *req,
                                          lc_error *error) {
  lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self;
  int rc;

  if (p == NULL || p->lease == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow participant is closed", NULL, NULL, NULL);
  }
  rc = lc_lease_mutate(p->lease, req, error);
  lc_workflow_participant_refresh(p);
  return rc;
}
static int lc_workflow_participant_mutate_local(lc_workflow_participant *self,
                                                const lc_mutate_local_req *req,
                                                lc_error *error) {
  lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self;
  int rc;

  if (p == NULL || p->lease == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow participant is closed", NULL, NULL, NULL);
  }
  rc = lc_lease_mutate_local(p->lease, req, error);
  lc_workflow_participant_refresh(p);
  return rc;
}
static int lc_workflow_participant_metadata(lc_workflow_participant *self,
                                            const lc_metadata_req *req,
                                            lc_error *error) {
  lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self;
  int rc;
  if (p == NULL || p->lease == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow participant is closed", NULL, NULL, NULL);
  rc = lc_lease_metadata(p->lease, req, error);
  lc_workflow_participant_refresh(p);
  return rc;
}
static int lc_workflow_participant_remove(lc_workflow_participant *self,
                                          const lc_remove_req *req,
                                          lc_error *error) {
  lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self;
  int rc;
  if (p == NULL || p->lease == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow participant is closed", NULL, NULL, NULL);
  rc = lc_lease_remove(p->lease, req, error);
  lc_workflow_participant_refresh(p);
  return rc;
}
static int lc_workflow_participant_keepalive(lc_workflow_participant *self,
                                             const lc_keepalive_req *req,
                                             lc_error *error) {
  lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self;
  int rc;
  if (p == NULL || p->lease == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow participant is closed", NULL, NULL, NULL);
  rc = lc_lease_keepalive(p->lease, req, error);
  lc_workflow_participant_refresh(p);
  return rc;
}
static int lc_workflow_participant_attach(lc_workflow_participant *self,
                                          const lc_attach_req *req,
                                          lc_source *src, lc_attach_res *out,
                                          lc_error *error) {
  lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self;
  if (p == NULL || p->lease == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow participant is closed", NULL, NULL, NULL);
  return lc_lease_attach(p->lease, req, src, out, error);
}
static int lc_workflow_participant_list_attachments(
    lc_workflow_participant *self, lc_attachment_list *out, lc_error *error) {
  lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self;
  if (p == NULL || p->lease == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow participant is closed", NULL, NULL, NULL);
  }
  return lc_lease_list_attachments(p->lease, out, error);
}
static int lc_workflow_participant_get_attachment(
    lc_workflow_participant *self, const lc_attachment_get_req *req,
    lc_sink *dst, lc_attachment_get_res *out, lc_error *error) {
  lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self;
  if (p == NULL || p->lease == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow participant is closed", NULL, NULL, NULL);
  return lc_lease_get_attachment(p->lease, req, dst, out, error);
}
static int lc_workflow_participant_delete_attachment(
    lc_workflow_participant *self, const lc_attachment_selector *selector,
    int *deleted, lc_error *error) {
  lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self;
  if (p == NULL || p->lease == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow participant is closed", NULL, NULL, NULL);
  }
  return lc_lease_delete_attachment(p->lease, selector, deleted, error);
}
static int lc_workflow_participant_delete_all_attachments(
    lc_workflow_participant *self, int *deleted_count, lc_error *error) {
  lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self;
  if (p == NULL || p->lease == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow participant is closed", NULL, NULL, NULL);
  }
  return lc_lease_delete_all_attachments(p->lease, deleted_count, error);
}
static void
lc_workflow_participant_close_method(lc_workflow_participant *self) {
  lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self;
  if (p != NULL) {
    lc_workflow_handle *workflow = p->workflow;

    if (p->transaction != NULL)
      lc_workflow_transaction_remove_participant(p->transaction, p);
    lc_client_free(workflow->client, p);
    lc_workflow_release(workflow);
  }
}

/* A later participant is already durably enrolled in the implicit-XA xid
 * before a workflow operation can report a post-enrollment failure. Rolling
 * that participant back therefore decides rollback for the whole xid; keep
 * the local transaction equally terminal and release every retained handle. */
static void lc_workflow_transaction_abort_enrolled_lease(
    lc_workflow_transaction_handle *transaction, lc_lease *lease) {
  size_t index;

  if (transaction == NULL)
    return;
  lc_workflow_transaction_clear_command(transaction, lease);
  lc_workflow_transaction_remove_lease(transaction, lease);
  lc_workflow_transaction_invalidate_participants(transaction);
  lc_workflow_rollback_lease(lease);
  for (index = 0U; index < transaction->lease_count; ++index) {
    lc_workflow_rollback_lease(transaction->leases[index]);
    transaction->leases[index] = NULL;
  }
  transaction->terminal = 1;
}

static int lc_workflow_transaction_acquire_method(
    lc_workflow_transaction *self,
    const lc_workflow_participant_request *request,
    lc_workflow_participant **out, lc_error *error) {
  lc_workflow_transaction_handle *transaction =
      (lc_workflow_transaction_handle *)self;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_workflow_participant_handle *participant;
  int rc;
  if (transaction == NULL || request == NULL || out == NULL ||
      transaction->terminal || transaction->lease_count == 0U ||
      request->acquire.key == NULL || request->acquire.key[0] == '\0' ||
      request->acquire.txn_id != NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow participant requires an open transaction, "
                        "key, and no caller transaction id",
                        NULL, NULL, NULL);
  }
  acquire = request->acquire;
  acquire.txn_id = transaction->leases[0]->txn_id;
  if (acquire.namespace_name == NULL)
    acquire.namespace_name = transaction->workflow->namespace_name;
  if (acquire.owner == NULL || acquire.owner[0] == '\0')
    acquire.owner = transaction->workflow->owner;
  if (acquire.ttl_seconds == 0L)
    acquire.ttl_seconds = transaction->workflow->transaction_ttl_seconds;
  lease = NULL;
  rc = lc_acquire(&transaction->workflow->client->pub, &acquire, &lease, error);
  if (rc != LC_OK)
    return rc;
  rc = lc_workflow_transaction_add_lease(transaction, lease, error);
  if (rc != LC_OK) {
    lc_workflow_transaction_abort_enrolled_lease(transaction, lease);
    return rc;
  }
#ifdef LOCKDC_TEST_BUILD
  if (lc_workflow_test_before_participant_allocation_hook != NULL) {
    rc = lc_workflow_test_before_participant_allocation_hook(
        lc_workflow_test_before_participant_allocation_context, error);
    if (rc != LC_OK) {
      lc_workflow_transaction_abort_enrolled_lease(transaction, lease);
      return rc;
    }
  }
#endif
  participant = (lc_workflow_participant_handle *)lc_client_calloc(
      transaction->workflow->client, 1U, sizeof(*participant));
  if (participant == NULL) {
    lc_workflow_transaction_abort_enrolled_lease(transaction, lease);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate workflow participant", NULL, NULL,
                        NULL);
  }
  lc_workflow_retain(transaction->workflow);
  participant->workflow = transaction->workflow;
  participant->transaction = transaction;
  participant->lease = lease;
  participant->next = transaction->participants;
  transaction->participants = participant;
  participant->pub.describe = lc_workflow_participant_describe;
  participant->pub.get = lc_workflow_participant_get;
  participant->pub.update = lc_workflow_participant_update;
  participant->pub.mutate = lc_workflow_participant_mutate;
  participant->pub.mutate_local = lc_workflow_participant_mutate_local;
  participant->pub.metadata = lc_workflow_participant_metadata;
  participant->pub.remove = lc_workflow_participant_remove;
  participant->pub.keepalive = lc_workflow_participant_keepalive;
  participant->pub.attach = lc_workflow_participant_attach;
  participant->pub.list_attachments = lc_workflow_participant_list_attachments;
  participant->pub.get_attachment = lc_workflow_participant_get_attachment;
  participant->pub.delete_attachment =
      lc_workflow_participant_delete_attachment;
  participant->pub.delete_all_attachments =
      lc_workflow_participant_delete_all_attachments;
  participant->pub.close = lc_workflow_participant_close_method;
  lc_workflow_participant_refresh(participant);
  *out = &participant->pub;
  return LC_OK;
}

static int lc_workflow_transaction_accept_command_method(
    lc_workflow_transaction *self, const lc_command_request *request,
    lc_command_receipt *receipt, lc_error *error) {
  lc_workflow_transaction_handle *transaction =
      (lc_workflow_transaction_handle *)self;
  lc_acquire_req acquire;
  lc_lease *lease;
  char *key;
  char command_id[48];
  lc_workflow_command_record record;
  int rc;

  if (transaction == NULL || transaction->terminal || request == NULL ||
      receipt == NULL || transaction->lease_count == 0U ||
      transaction->command_lease != NULL || request->request_digest == NULL ||
      request->request_digest[0] == '\0') {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "open transaction, one command request, and digest are required", NULL,
        NULL, NULL);
  }
  lc_command_receipt_cleanup(receipt);
  key = NULL;
  rc = lc_workflow_command_key(&request->identity, &key, command_id, error);
  if (rc != LC_OK)
    return rc;
  lc_acquire_req_init(&acquire);
  acquire.namespace_name = transaction->workflow->namespace_name;
  acquire.key = key;
  acquire.owner = transaction->workflow->owner;
  acquire.ttl_seconds = transaction->workflow->transaction_ttl_seconds;
  acquire.if_not_exists = 1;
  acquire.txn_id = transaction->leases[0]->txn_id;
  lease = NULL;
  rc = lc_acquire(&transaction->workflow->client->pub, &acquire, &lease, error);
  if (rc != LC_OK) {
    if (error != NULL) {
      lc_error_cleanup(error);
      lc_error_init(error);
    }
    rc = lc_workflow_existing_command(transaction->workflow, key, request,
                                      receipt, error);
    free(key);
    return rc;
  }
  rc = lc_workflow_stage_command(lease, request, command_id, error);
  if (rc == LC_OK)
    rc = lc_workflow_transaction_add_lease(transaction, lease, error);
  if (rc == LC_OK)
    rc = lc_workflow_transaction_set_command(transaction, lease, command_id,
                                             error);
  if (rc != LC_OK) {
    lc_workflow_transaction_abort_enrolled_lease(transaction, lease);
    free(key);
    return rc;
  }
  memset(&record, 0, sizeof(record));
  record.record_type = "lockdc.command.v1";
  record.command_id = command_id;
  record.scope = (char *)request->identity.scope;
  record.command_type = (char *)request->identity.command_type;
  record.idempotency_key = (char *)request->identity.idempotency_key;
  record.state = "pending";
  record.operation_id = (char *)request->operation_id;
#ifdef LOCKDC_TEST_BUILD
  if (lc_workflow_test_before_command_receipt_copy_hook != NULL) {
    rc = lc_workflow_test_before_command_receipt_copy_hook(
        lc_workflow_test_before_command_receipt_copy_context, error);
    if (rc != LC_OK) {
      free(key);
      lc_workflow_transaction_abort_enrolled_lease(transaction, lease);
      return rc;
    }
  }
#endif
  rc = lc_workflow_command_receipt_from_record(&record, receipt, error);
  free(key);
  if (rc != LC_OK) {
    lc_workflow_transaction_abort_enrolled_lease(transaction, lease);
  }
  return rc;
}

static int
lc_workflow_transaction_complete_command_method(lc_workflow_transaction *self,
                                                const lc_command_result *result,
                                                lc_error *error) {
  return lc_workflow_stage_command_terminal(
      (lc_workflow_transaction_handle *)self, result, 0, error);
}

static int
lc_workflow_transaction_fail_command_method(lc_workflow_transaction *self,
                                            const lc_command_result *result,
                                            lc_error *error) {
  return lc_workflow_stage_command_terminal(
      (lc_workflow_transaction_handle *)self, result, 1, error);
}

static int lc_workflow_transaction_append_outbox_method(
    lc_workflow_transaction *self, const lc_outbox_entry *entry,
    lc_source *payload, lc_outbox_receipt *receipt, lc_error *error) {
  lc_workflow_transaction_handle *transaction =
      (lc_workflow_transaction_handle *)self;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_outbox_entry effective_entry;
  char *key;
  int rc;
  if (transaction == NULL || transaction->terminal || receipt == NULL ||
      transaction->lease_count == 0U || entry == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow transaction is closed", NULL, NULL, NULL);
  lc_outbox_receipt_cleanup(receipt);
  effective_entry = *entry;
  if (effective_entry.causation_id == NULL)
    effective_entry.causation_id = transaction->causation_id;
  rc = lc_workflow_validate_headers_json(effective_entry.headers_json, error);
  if (rc != LC_OK)
    return rc;
  key = NULL;
  rc = lc_workflow_outbox_key(transaction->workflow, &effective_entry, &key,
                              error);
  if (rc != LC_OK)
    return rc;
  lc_acquire_req_init(&acquire);
  acquire.namespace_name = transaction->workflow->namespace_name;
  acquire.key = key;
  acquire.owner = transaction->workflow->owner;
  acquire.ttl_seconds = transaction->workflow->transaction_ttl_seconds;
  acquire.if_not_exists = 1;
  acquire.txn_id = transaction->leases[0]->txn_id;
  lease = NULL;
  rc = lc_acquire(&transaction->workflow->client->pub, &acquire, &lease, error);
  if (rc != LC_OK) {
    if (error != NULL) {
      lc_error_cleanup(error);
      lc_error_init(error);
    }
    rc = lc_workflow_existing_outbox(transaction->workflow, key,
                                     &effective_entry, receipt, error);
    free(key);
    return rc;
  }
  rc = lc_workflow_stage_outbox(lease, &effective_entry, payload, error);
  if (rc != LC_OK) {
    lc_workflow_transaction_abort_enrolled_lease(transaction, lease);
    free(key);
    return rc;
  }
  rc = lc_workflow_transaction_add_lease(transaction, lease, error);
  if (rc != LC_OK) {
    lc_workflow_transaction_abort_enrolled_lease(transaction, lease);
    free(key);
    return rc;
  }
#ifdef LOCKDC_TEST_BUILD
  if (lc_workflow_test_before_outbox_receipt_copy_hook != NULL) {
    rc = lc_workflow_test_before_outbox_receipt_copy_hook(
        lc_workflow_test_before_outbox_receipt_copy_context, error);
    if (rc != LC_OK) {
      lc_workflow_transaction_abort_enrolled_lease(transaction, lease);
      free(key);
      return rc;
    }
  }
#endif
  receipt->effect_key = lc_strdup_local(entry->effect_key);
  if (receipt->effect_key == NULL) {
    lc_workflow_transaction_abort_enrolled_lease(transaction, lease);
    free(key);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate outbox receipt", NULL, NULL, NULL);
  }
  receipt->outbox_key = key;
  return LC_OK;
}

static int lc_workflow_transaction_terminal(lc_workflow_transaction *self,
                                            int rollback, lc_error *error) {
  lc_workflow_transaction_handle *transaction =
      (lc_workflow_transaction_handle *)self;
  lc_release_req request;
  char **notification_keys;
  size_t i;
  int rc;
  if (transaction == NULL || transaction->terminal)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow transaction is closed", NULL, NULL, NULL);
  rc = LC_OK;
  notification_keys = NULL;
  if (!rollback) {
    notification_keys = (char **)lc_client_calloc(transaction->workflow->client,
                                                  transaction->lease_count,
                                                  sizeof(*notification_keys));
    if (notification_keys == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to prepare workflow dispatch signals", NULL,
                          NULL, NULL);
    }
    for (i = 0U; i < transaction->lease_count; ++i) {
      if (transaction->leases[i] != NULL) {
        notification_keys[i] = lc_client_strdup(transaction->workflow->client,
                                                transaction->leases[i]->key);
        if (notification_keys[i] == NULL) {
          while (i > 0U) {
            --i;
            lc_client_free(transaction->workflow->client, notification_keys[i]);
          }
          lc_client_free(transaction->workflow->client, notification_keys);
          return lc_error_set(error, LC_ERR_NOMEM, 0L,
                              "failed to prepare workflow dispatch signals",
                              NULL, NULL, NULL);
        }
      }
    }
  }
  lc_release_req_init(&request);
  request.rollback = rollback;
  for (i = 0U; i < transaction->lease_count; ++i) {
    if (transaction->leases[i] == NULL)
      continue;
    rc = lc_lease_release(transaction->leases[i], &request, error);
    if (rc != LC_OK) {
      size_t j;
      for (j = 0U; j < transaction->lease_count; ++j) {
        lc_client_free(transaction->workflow->client,
                       notification_keys == NULL ? NULL : notification_keys[j]);
      }
      lc_client_free(transaction->workflow->client, notification_keys);
      return rc;
    }
    lc_workflow_transaction_invalidate_lease_participant(
        transaction, transaction->leases[i]);
    transaction->leases[i] = NULL;
  }
  transaction->terminal = 1;
  for (i = 0U; i < transaction->lease_count; ++i) {
    if (lc_workflow_is_outbox_key(
            notification_keys == NULL ? NULL : notification_keys[i])) {
      lc_workflow_notify(transaction->workflow, notification_keys[i]);
    }
    lc_client_free(transaction->workflow->client,
                   notification_keys == NULL ? NULL : notification_keys[i]);
  }
  lc_client_free(transaction->workflow->client, notification_keys);
  return LC_OK;
}
static int lc_workflow_transaction_commit_method(lc_workflow_transaction *self,
                                                 lc_error *error) {
  return lc_workflow_transaction_terminal(self, 0, error);
}
static int
lc_workflow_transaction_rollback_method(lc_workflow_transaction *self,
                                        lc_error *error) {
  return lc_workflow_transaction_terminal(self, 1, error);
}
static void
lc_workflow_transaction_close_method(lc_workflow_transaction *self) {
  lc_workflow_transaction_handle *transaction =
      (lc_workflow_transaction_handle *)self;
  lc_workflow_handle *workflow;
  size_t i;
  if (transaction == NULL)
    return;
  workflow = transaction->workflow;
  if (!transaction->terminal)
    (void)lc_workflow_transaction_terminal(self, 1, NULL);
  lc_workflow_transaction_invalidate_participants(transaction);
  for (i = 0U; i < transaction->lease_count; ++i)
    lc_lease_close(transaction->leases[i]);
  lc_client_free(workflow->client, transaction->leases);
  lc_client_free(workflow->client, transaction->causation_id);
  lc_client_free(workflow->client, transaction);
  lc_workflow_release(workflow);
}

static lc_workflow_transaction *
lc_workflow_transaction_new(lc_workflow_handle *workflow, lc_lease *first,
                            lc_error *error) {
  lc_workflow_transaction_handle *transaction;
  transaction = (lc_workflow_transaction_handle *)lc_client_calloc(
      workflow->client, 1U, sizeof(*transaction));
  if (transaction == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate workflow transaction", NULL, NULL, NULL);
    return NULL;
  }
  transaction->workflow = workflow;
  lc_workflow_retain(workflow);
  transaction->pub.accept_command =
      lc_workflow_transaction_accept_command_method;
  transaction->pub.acquire = lc_workflow_transaction_acquire_method;
  transaction->pub.append_outbox = lc_workflow_transaction_append_outbox_method;
  transaction->pub.complete_command =
      lc_workflow_transaction_complete_command_method;
  transaction->pub.fail_command = lc_workflow_transaction_fail_command_method;
  transaction->pub.commit = lc_workflow_transaction_commit_method;
  transaction->pub.rollback = lc_workflow_transaction_rollback_method;
  transaction->pub.close = lc_workflow_transaction_close_method;
  if (lc_workflow_transaction_add_lease(transaction, first, error) != LC_OK) {
    lc_client_free(workflow->client, transaction);
    lc_workflow_release(workflow);
    return NULL;
  }
  return &transaction->pub;
}

static int lc_workflow_append_outbox_method(lc_workflow *self,
                                            const lc_outbox_entry *entry,
                                            lc_source *payload,
                                            lc_workflow_transaction **out_txn,
                                            lc_outbox_receipt *receipt,
                                            lc_error *error) {
  lc_workflow_handle *workflow = (lc_workflow_handle *)self;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_workflow_transaction *transaction;
  char *key;
  int rc;
  if (workflow == NULL || out_txn == NULL || receipt == NULL)
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "workflow, transaction output, and receipt are required", NULL, NULL,
        NULL);
  *out_txn = NULL;
  lc_outbox_receipt_cleanup(receipt);
  rc = lc_workflow_validate_headers_json(
      entry != NULL ? entry->headers_json : NULL, error);
  if (rc != LC_OK)
    return rc;
  key = NULL;
  rc = lc_workflow_outbox_key(workflow, entry, &key, error);
  if (rc != LC_OK)
    return rc;
  lc_acquire_req_init(&acquire);
  acquire.namespace_name = workflow->namespace_name;
  acquire.key = key;
  acquire.owner = workflow->owner;
  acquire.ttl_seconds = workflow->transaction_ttl_seconds;
  acquire.if_not_exists = 1;
  lease = NULL;
  rc = lc_acquire(&workflow->client->pub, &acquire, &lease, error);
  if (rc != LC_OK) {
    if (error != NULL) {
      lc_error_cleanup(error);
      lc_error_init(error);
    }
    rc = lc_workflow_existing_outbox(workflow, key, entry, receipt, error);
    free(key);
    return rc;
  }
  rc = lc_workflow_stage_outbox(lease, entry, payload, error);
  if (rc != LC_OK) {
    lc_workflow_rollback_lease(lease);
    free(key);
    return rc;
  }
  transaction = lc_workflow_transaction_new(workflow, lease, error);
  if (transaction == NULL) {
    lc_workflow_rollback_lease(lease);
    free(key);
    return error != NULL ? error->code : LC_ERR_NOMEM;
  }
  receipt->outbox_key = key;
  receipt->effect_key = lc_strdup_local(entry->effect_key);
  if (receipt->effect_key == NULL) {
    transaction->close(transaction);
    lc_outbox_receipt_cleanup(receipt);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate outbox receipt", NULL, NULL, NULL);
  }
  *out_txn = transaction;
  return LC_OK;
}

static int lc_workflow_accept_inbox_method(lc_workflow *self,
                                           const lc_inbox_message *message,
                                           lc_workflow_transaction **out_txn,
                                           lc_inbox_accept_result *result,
                                           lc_error *error) {
  lc_workflow_handle *workflow = (lc_workflow_handle *)self;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_workflow_transaction *transaction;
  char *key;
  int rc;
  if (workflow == NULL || out_txn == NULL || result == NULL)
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "workflow, transaction output, and inbox result are required", NULL,
        NULL, NULL);
  *out_txn = NULL;
  memset(result, 0, sizeof(*result));
  key = NULL;
  rc = lc_workflow_inbox_key(workflow, message, &key, error);
  if (rc != LC_OK)
    return rc;
  lc_acquire_req_init(&acquire);
  acquire.namespace_name = workflow->namespace_name;
  acquire.key = key;
  acquire.owner = workflow->owner;
  acquire.ttl_seconds = workflow->transaction_ttl_seconds;
  acquire.if_not_exists = 1;
  lease = NULL;
  rc = lc_acquire(&workflow->client->pub, &acquire, &lease, error);
  if (rc != LC_OK) {
    if (error != NULL) {
      lc_error_cleanup(error);
      lc_error_init(error);
    }
    rc = lc_workflow_existing_inbox(workflow, key, message, result, error);
    free(key);
    return rc;
  }
  free(key);
  rc = lc_workflow_stage_inbox(lease, message, error);
  if (rc != LC_OK) {
    lc_workflow_rollback_lease(lease);
    return rc;
  }
  transaction = lc_workflow_transaction_new(workflow, lease, error);
  if (transaction == NULL) {
    lc_workflow_rollback_lease(lease);
    return error != NULL ? error->code : LC_ERR_NOMEM;
  }
  ((lc_workflow_transaction_handle *)transaction)->causation_id =
      lc_client_strdup(workflow->client, message->message_id);
  if (((lc_workflow_transaction_handle *)transaction)->causation_id == NULL) {
    transaction->close(transaction);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to retain inbox causation identity", NULL, NULL,
                        NULL);
  }
  result->accepted = 1;
  *out_txn = transaction;
  return LC_OK;
}

static int lc_workflow_accept_command_method(lc_workflow *self,
                                             const lc_command_request *request,
                                             lc_workflow_transaction **out_txn,
                                             lc_command_receipt *receipt,
                                             lc_error *error) {
  lc_workflow_handle *workflow = (lc_workflow_handle *)self;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_workflow_transaction *transaction;
  lc_workflow_transaction_handle *handle;
  lc_workflow_command_record record;
  char *key;
  char command_id[48];
  int rc;

  if (workflow == NULL || request == NULL || out_txn == NULL ||
      receipt == NULL || request->request_digest == NULL ||
      request->request_digest[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow, command request, digest, transaction "
                        "output, and receipt are required",
                        NULL, NULL, NULL);
  }
  *out_txn = NULL;
  lc_command_receipt_cleanup(receipt);
  key = NULL;
  rc = lc_workflow_command_key(&request->identity, &key, command_id, error);
  if (rc != LC_OK)
    return rc;
  lc_acquire_req_init(&acquire);
  acquire.namespace_name = workflow->namespace_name;
  acquire.key = key;
  acquire.owner = workflow->owner;
  acquire.ttl_seconds = workflow->transaction_ttl_seconds;
  acquire.if_not_exists = 1;
  lease = NULL;
  rc = lc_acquire(&workflow->client->pub, &acquire, &lease, error);
  if (rc != LC_OK) {
    if (error != NULL) {
      lc_error_cleanup(error);
      lc_error_init(error);
    }
    rc = lc_workflow_existing_command(workflow, key, request, receipt, error);
    free(key);
    return rc;
  }
  rc = lc_workflow_stage_command(lease, request, command_id, error);
  if (rc != LC_OK) {
    lc_workflow_rollback_lease(lease);
    free(key);
    return rc;
  }
  transaction = lc_workflow_transaction_new(workflow, lease, error);
  if (transaction == NULL) {
    lc_workflow_rollback_lease(lease);
    free(key);
    return error != NULL ? error->code : LC_ERR_NOMEM;
  }
  handle = (lc_workflow_transaction_handle *)transaction;
  rc = lc_workflow_transaction_set_command(handle, lease, command_id, error);
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
  record.operation_id = (char *)request->operation_id;
  record.state = "pending";
  rc = lc_workflow_command_receipt_from_record(&record, receipt, error);
  free(key);
  if (rc != LC_OK) {
    transaction->close(transaction);
    return rc;
  }
  *out_txn = transaction;
  return LC_OK;
}

static int lc_workflow_get_command_receipt_method(
    lc_workflow *self, const lc_command_identity *identity,
    lc_command_receipt *receipt, lc_error *error) {
  lc_workflow_handle *workflow = (lc_workflow_handle *)self;
  char *key;
  char command_id[48];
  int rc;

  if (workflow == NULL || receipt == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow and command receipt output are required",
                        NULL, NULL, NULL);
  lc_command_receipt_cleanup(receipt);
  key = NULL;
  rc = lc_workflow_command_key(identity, &key, command_id, error);
  if (rc == LC_OK)
    rc = lc_workflow_existing_command(workflow, key, NULL, receipt, error);
  free(key);
  return rc;
}

static int lc_workflow_write_command_result_method(
    lc_workflow *self, const lc_command_identity *identity, lc_sink *dst,
    size_t *written, lc_error *error) {
  lc_workflow_handle *workflow = (lc_workflow_handle *)self;
  lc_attachment_get_op request;
  lc_attachment_get_res result;
  char *key;
  char command_id[48];
  int rc;

  if (workflow == NULL || dst == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow and command result sink are required", NULL,
                        NULL, NULL);
  key = NULL;
  rc = lc_workflow_command_key(identity, &key, command_id, error);
  if (rc != LC_OK)
    return rc;
  lc_attachment_get_op_init(&request);
  memset(&result, 0, sizeof(result));
  request.lease.namespace_name = workflow->namespace_name;
  request.lease.key = key;
  request.selector.name = "result";
  request.public_read = 1;
  rc = lc_get_attachment(&workflow->client->pub, &request, dst, &result, error);
  if (rc == LC_OK && written != NULL)
    *written = (size_t)result.attachment.size;
  lc_attachment_get_res_cleanup(&result);
  free(key);
  return rc;
}

static int lc_workflow_resume_command_method(
    lc_workflow *self, const lc_command_identity *identity,
    lc_workflow_transaction **out_txn, lc_command_receipt *receipt,
    lc_error *error) {
  lc_workflow_handle *workflow = (lc_workflow_handle *)self;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_workflow_transaction *transaction;
  lc_workflow_transaction_handle *handle;
  lc_workflow_command_record record;
  lc_get_res load_result;
  lonejson *runtime;
  char *key;
  char command_id[48];
  int rc;

  if (workflow == NULL || out_txn == NULL || receipt == NULL)
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "workflow, transaction output, and receipt are required", NULL, NULL,
        NULL);
  *out_txn = NULL;
  lc_command_receipt_cleanup(receipt);
  key = NULL;
  rc = lc_workflow_command_key(identity, &key, command_id, error);
  if (rc != LC_OK)
    return rc;
  rc = lc_workflow_existing_command(workflow, key, NULL, receipt, error);
  if (rc != LC_OK || receipt->state != LC_COMMAND_PENDING) {
    free(key);
    return rc;
  }
  lc_acquire_req_init(&acquire);
  acquire.namespace_name = workflow->namespace_name;
  acquire.key = key;
  acquire.owner = workflow->owner;
  acquire.ttl_seconds = workflow->transaction_ttl_seconds;
  lease = NULL;
  rc = lc_acquire(&workflow->client->pub, &acquire, &lease, error);
  if (rc != LC_OK) {
    free(key);
    return rc;
  }
  memset(&record, 0, sizeof(record));
  memset(&load_result, 0, sizeof(load_result));
  runtime = lc_thread_lonejson_runtime();
  rc = lc_lease_load(lease, &lc_workflow_command_record_map, &record, NULL,
                     &load_result, error);
  if (rc == LC_OK &&
      (record.state == NULL || strcmp(record.state, "pending") != 0)) {
    rc = lc_workflow_command_receipt_from_record(&record, receipt, error);
    if (rc == LC_OK)
      receipt->duplicate = 1;
  }
  runtime->cleanup(runtime, &lc_workflow_command_record_map, &record);
  lc_get_res_cleanup(&load_result);
  if (rc != LC_OK || receipt->state != LC_COMMAND_PENDING) {
    lc_workflow_rollback_lease(lease);
    free(key);
    return rc;
  }
  transaction = lc_workflow_transaction_new(workflow, lease, error);
  if (transaction == NULL) {
    lc_workflow_rollback_lease(lease);
    free(key);
    return error != NULL ? error->code : LC_ERR_NOMEM;
  }
  handle = (lc_workflow_transaction_handle *)transaction;
  rc = lc_workflow_transaction_set_command(handle, lease, command_id, error);
  free(key);
  if (rc != LC_OK) {
    transaction->close(transaction);
    return rc;
  }
  *out_txn = transaction;
  return LC_OK;
}

static int lc_workflow_next_method(lc_workflow *self, long timeout_ms,
                                   lc_outbox_job **out, lc_error *error) {
  lc_workflow_handle *workflow = (lc_workflow_handle *)self;
  if (workflow == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow and job output are required", NULL, NULL,
                        NULL);
  }
  return lc_workflow_wait_for_ready(workflow, timeout_ms, out, error);
}

static int lc_workflow_get_stats_method(lc_workflow *self,
                                        lc_workflow_stats *out,
                                        lc_error *error) {
  lc_workflow_handle *workflow = (lc_workflow_handle *)self;
  int had_last_error;

  if (workflow == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow and stats output are required", NULL, NULL,
                        NULL);
  }
  lc_workflow_stats_cleanup(out);
  pthread_mutex_lock(&workflow->notification_mutex);
  out->running = workflow->closed ? 0 : 1;
  out->pending_notifications = workflow->notification_count;
  out->ready_jobs = workflow->ready_count;
  out->direct_notifications = workflow->direct_notifications;
  out->notification_overflows = workflow->notification_overflows;
  out->recovery_queries = workflow->recovery_queries;
  out->recovered_claims = workflow->recovered_claims;
  out->claim_losses = workflow->claim_losses;
  out->payload_open_failures = workflow->payload_open_failures;
  had_last_error = workflow->last_error != NULL;
  out->last_error = lc_strdup_local(workflow->last_error);
  pthread_mutex_unlock(&workflow->notification_mutex);
  if (had_last_error && out->last_error == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy workflow last error", NULL, NULL, NULL);
  }
  return LC_OK;
}

static int lc_workflow_reconcile_method(lc_workflow *self, lc_error *error) {
  lc_workflow_handle *workflow = (lc_workflow_handle *)self;

  if (workflow == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, "workflow is required", NULL,
                        NULL, NULL);
  }
  pthread_mutex_lock(&workflow->notification_mutex);
  if (workflow->closed) {
    pthread_mutex_unlock(&workflow->notification_mutex);
    return lc_error_set(error, LC_ERR_INVALID, 0L, "workflow is closed", NULL,
                        NULL, NULL);
  }
  workflow->recovery_needed = 1;
  workflow->recovery_claims_pending = 1;
  lc_workflow_signal_dispatcher_locked(workflow);
  pthread_mutex_unlock(&workflow->notification_mutex);
  return LC_OK;
}

static int lc_workflow_open_dead_letter(lc_workflow_handle *workflow,
                                        const char *outbox_key,
                                        lc_lease **lease_out,
                                        lc_workflow_outbox_record *record,
                                        lc_error *error) {
  lc_acquire_req acquire;
  lc_get_res result;
  lonejson *runtime;
  int rc;

  if (workflow == NULL || outbox_key == NULL || outbox_key[0] == '\0' ||
      lease_out == NULL || record == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow dead-letter key and outputs are required",
                        NULL, NULL, NULL);
  }
  *lease_out = NULL;
  memset(record, 0, sizeof(*record));
  memset(&result, 0, sizeof(result));
  lc_acquire_req_init(&acquire);
  acquire.namespace_name = workflow->namespace_name;
  acquire.key = outbox_key;
  acquire.owner = workflow->owner;
  acquire.ttl_seconds = workflow->transaction_ttl_seconds;
  rc = lc_acquire(&workflow->client->pub, &acquire, lease_out, error);
  if (rc != LC_OK)
    return rc;
  runtime = lc_thread_lonejson_runtime();
  rc = lc_lease_load(*lease_out, &lc_workflow_outbox_record_map, record, NULL,
                     &result, error);
  lc_get_res_cleanup(&result);
  if (rc == LC_OK &&
      (record->record_type == NULL || record->dispatch_state == NULL ||
       strcmp(record->record_type, "lockdc.outbox.v1") != 0 ||
       strcmp(record->dispatch_state, "dead_letter") != 0)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "outbox record is not a dead letter", NULL, NULL, NULL);
  }
  if (rc != LC_OK) {
    runtime->cleanup(runtime, &lc_workflow_outbox_record_map, record);
    lc_workflow_rollback_lease(*lease_out);
    *lease_out = NULL;
  }
  return rc;
}

static int lc_workflow_replay_dead_letter_method(lc_workflow *self,
                                                 const char *outbox_key,
                                                 lc_error *error) {
  lc_workflow_handle *workflow = (lc_workflow_handle *)self;
  lc_workflow_outbox_record record;
  lc_lease *lease;
  lc_release_req release;
  char *original_dispatch_state;
  char *original_last_error;
  char *original_prior_dead_letter_error;
  time_t now;
  int rc;

  lease = NULL;
  rc = lc_workflow_open_dead_letter(workflow, outbox_key, &lease, &record,
                                    error);
  if (rc != LC_OK)
    return rc;
  now = time(NULL);
  if (now == (time_t)-1) {
    lc_workflow_outbox_record_loaded_clear(&record);
    lc_workflow_rollback_lease(lease);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "failed to read workflow replay clock", NULL, NULL,
                        NULL);
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
  rc = lc_lease_save(lease, &lc_workflow_outbox_record_map, &record, error);
  if (rc == LC_OK) {
    lc_release_req_init(&release);
    rc = lc_lease_release(lease, &release, error);
  }
  record.dispatch_state = original_dispatch_state;
  record.last_error = original_last_error;
  record.prior_dead_letter_error = original_prior_dead_letter_error;
  lc_workflow_outbox_record_loaded_clear(&record);
  if (rc != LC_OK) {
    lc_workflow_rollback_lease(lease);
    return rc;
  }
  lc_workflow_notify(workflow, outbox_key);
  return LC_OK;
}

typedef struct lc_workflow_dead_letter_replay_capture {
  lc_workflow_handle *workflow;
  char **keys;
  size_t key_capacity;
  char key[129];
  size_t length;
  size_t count;
} lc_workflow_dead_letter_replay_capture;

static int lc_workflow_dead_letter_replay_begin(void *context,
                                                lc_error *error) {
  lc_workflow_dead_letter_replay_capture *capture =
      (lc_workflow_dead_letter_replay_capture *)context;
  (void)error;
  capture->length = 0U;
  return 1;
}

static int lc_workflow_dead_letter_replay_chunk(void *context,
                                                const char *bytes,
                                                size_t length,
                                                lc_error *error) {
  lc_workflow_dead_letter_replay_capture *capture =
      (lc_workflow_dead_letter_replay_capture *)context;
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

static int lc_workflow_dead_letter_replay_end(void *context, lc_error *error) {
  lc_workflow_dead_letter_replay_capture *capture =
      (lc_workflow_dead_letter_replay_capture *)context;

  capture->key[capture->length] = '\0';
  if (!lc_workflow_is_outbox_key(capture->key))
    return 1;
  if (capture->count == capture->key_capacity ||
      (capture->keys[capture->count] =
           lc_client_strdup(capture->workflow->client, capture->key)) == NULL) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to retain dead-letter replay key", NULL, NULL,
                       NULL);
    return 0;
  }
  ++capture->count;
  return 1;
}

static int
lc_workflow_replay_dead_letters_on_startup(lc_workflow_handle *workflow,
                                           lc_error *error) {
  static const char selector[] =
      "{\"eq\":{\"field\":\"/dispatch_state\",\"value\":\"dead_letter\"}}";
  lc_index_flush_req flush_request;
  lc_index_flush_res flush_result;
  lc_query_req request;
  lc_query_key_handler handler;
  lc_query_res result;
  lc_workflow_dead_letter_replay_capture capture;
  size_t index;
  int rc;

  if (!workflow->startup_dead_letter_replay_pending)
    return LC_OK;
  if (!workflow->startup_dead_letter_replay_flushed) {
    lc_index_flush_req_init(&flush_request);
    memset(&flush_result, 0, sizeof(flush_result));
    flush_request.namespace_name = workflow->namespace_name;
    flush_request.mode = "wait";
    rc = lc_flush_index(&workflow->dispatcher_client->pub, &flush_request,
                        &flush_result, error);
    lc_index_flush_res_cleanup(&flush_result);
    if (rc != LC_OK)
      return rc;
    workflow->startup_dead_letter_replay_flushed = 1;
  }
  lc_query_req_init(&request);
  memset(&handler, 0, sizeof(handler));
  memset(&result, 0, sizeof(result));
  memset(&capture, 0, sizeof(capture));
  capture.keys = (char **)lc_client_calloc(
      workflow->client, workflow->notification_capacity, sizeof(*capture.keys));
  if (capture.keys == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate dead-letter replay keys", NULL,
                        NULL, NULL);
  }
  capture.key_capacity = workflow->notification_capacity;
  request.namespace_name = workflow->namespace_name;
  request.selector_json = selector;
  request.limit = (long)workflow->notification_capacity;
  request.engine = "index";
  request.refresh = "wait_for";
  handler.begin = lc_workflow_dead_letter_replay_begin;
  handler.chunk = lc_workflow_dead_letter_replay_chunk;
  handler.end = lc_workflow_dead_letter_replay_end;
  capture.workflow = workflow;
  pthread_mutex_lock(&workflow->notification_mutex);
  ++workflow->recovery_queries;
  pthread_mutex_unlock(&workflow->notification_mutex);
  rc = lc_query_keys(&workflow->dispatcher_client->pub, &request, &handler,
                     &capture, &result, error);
  lc_query_res_cleanup(&result);
  for (index = 0U; rc == LC_OK && index < capture.count; ++index) {
    rc = lc_workflow_replay_dead_letter_method(&workflow->pub,
                                               capture.keys[index], error);
  }
  for (index = 0U; index < capture.count; ++index)
    lc_client_free(workflow->client, capture.keys[index]);
  lc_client_free(workflow->client, capture.keys);
  if (rc != LC_OK)
    return rc;
  if (capture.count == 0U)
    workflow->startup_dead_letter_replay_pending = 0;
  else {
    pthread_mutex_lock(&workflow->notification_mutex);
    workflow->recovery_needed = 1;
    lc_workflow_signal_dispatcher_locked(workflow);
    pthread_mutex_unlock(&workflow->notification_mutex);
  }
  return LC_OK;
}

static int lc_workflow_delete_dead_letter_method(lc_workflow *self,
                                                 const char *outbox_key,
                                                 lc_error *error) {
  lc_workflow_handle *workflow = (lc_workflow_handle *)self;
  lc_workflow_outbox_record record;
  lc_lease *lease;
  lc_remove_req remove;
  lc_release_req release;
  int deleted;
  int rc;

  lease = NULL;
  rc = lc_workflow_open_dead_letter(workflow, outbox_key, &lease, &record,
                                    error);
  if (rc != LC_OK)
    return rc;
  deleted = 0;
  rc = lc_lease_delete_all_attachments(lease, &deleted, error);
  if (rc == LC_OK) {
    lc_remove_req_init(&remove);
    rc = lc_lease_remove(lease, &remove, error);
  }
  if (rc == LC_OK) {
    lc_release_req_init(&release);
    rc = lc_lease_release(lease, &release, error);
  }
  lc_workflow_outbox_record_loaded_clear(&record);
  if (rc != LC_OK)
    lc_workflow_rollback_lease(lease);
  return rc;
}

typedef struct lc_workflow_dead_letter_export_capture {
  lc_workflow_handle *workflow;
  lc_sink *dst;
  lc_dead_letter_export_res *result;
  int format;
  int first;
  char key[129];
  size_t length;
} lc_workflow_dead_letter_export_capture;

static int lc_workflow_export_dead_letter_key(
    lc_workflow_dead_letter_export_capture *capture, lc_error *error) {
  lc_workflow_outbox_record record;
  lc_lease *lease;
  lc_get_res get_result;
  lc_release_req release;
  int rc;

  lease = NULL;
  rc = lc_workflow_open_dead_letter(capture->workflow, capture->key, &lease,
                                    &record, error);
  if (rc != LC_OK) {
    /* Index selection and the direct read are separate operations. A record
     * replayed or deleted between them is no longer exportable, not an export
     * failure. Other validation and transport errors remain observable. */
    if (rc == LC_ERR_INVALID && error != NULL && error->message != NULL &&
        strcmp(error->message, "outbox record is not a dead letter") == 0) {
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
  lc_workflow_outbox_record_loaded_clear(&record);
  if (rc == LC_OK) {
    capture->first = 0;
    ++capture->result->exported;
  }
  return rc;
}

static int lc_workflow_dead_letter_export_begin(void *context,
                                                lc_error *error) {
  lc_workflow_dead_letter_export_capture *capture =
      (lc_workflow_dead_letter_export_capture *)context;
  (void)error;
  capture->length = 0U;
  return 1;
}

static int lc_workflow_dead_letter_export_chunk(void *context,
                                                const char *bytes,
                                                size_t length,
                                                lc_error *error) {
  lc_workflow_dead_letter_export_capture *capture =
      (lc_workflow_dead_letter_export_capture *)context;
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

static int lc_workflow_dead_letter_export_end(void *context, lc_error *error) {
  lc_workflow_dead_letter_export_capture *capture =
      (lc_workflow_dead_letter_export_capture *)context;
  capture->key[capture->length] = '\0';
  if (!lc_workflow_is_outbox_key(capture->key))
    return 1;
  return lc_workflow_export_dead_letter_key(capture, error) == LC_OK ? 1 : 0;
}

static int lc_workflow_export_dead_letters_method(
    lc_workflow *self, const lc_dead_letter_export_opts *options, lc_sink *dst,
    lc_dead_letter_export_res *out, lc_error *error) {
  static const char selector[] =
      "{\"eq\":{\"field\":\"/dispatch_state\",\"value\":\"dead_letter\"}}";
  lc_workflow_handle *workflow = (lc_workflow_handle *)self;
  lc_dead_letter_export_opts defaults;
  lc_index_flush_req flush_request;
  lc_index_flush_res flush_result;
  lc_query_req request;
  lc_query_key_handler handler;
  lc_query_res query_result;
  lc_workflow_dead_letter_export_capture capture;
  int rc;

  if (workflow == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow, export sink, and export result are required",
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
  memset(&capture, 0, sizeof(capture));
  capture.workflow = workflow;
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
  /* Export starts from a durable index boundary. A concurrent replay or
   * deletion can still change a selected record before it is read; that
   * benign stale-key race is ignored by the key visitor above. */
  flush_request.namespace_name = workflow->namespace_name;
  flush_request.mode = "wait";
  rc = lc_flush_index(&workflow->dispatcher_client->pub, &flush_request,
                      &flush_result, error);
  lc_index_flush_res_cleanup(&flush_result);
  if (rc != LC_OK)
    return rc;
  request.namespace_name = workflow->namespace_name;
  request.selector_json = selector;
  request.limit = (long)(options->limit == 0U ? workflow->notification_capacity
                                              : options->limit);
  request.engine = "index";
  request.refresh = "wait_for";
  handler.begin = lc_workflow_dead_letter_export_begin;
  handler.chunk = lc_workflow_dead_letter_export_chunk;
  handler.end = lc_workflow_dead_letter_export_end;
  rc = lc_query_keys(&workflow->client->pub, &request, &handler, &capture,
                     &query_result, error);
  lc_query_res_cleanup(&query_result);
  if (rc != LC_OK)
    return rc;
  if (options->format == LC_DEAD_LETTER_EXPORT_JSON &&
      !dst->write(dst, "]", 1U, error))
    return LC_ERR_TRANSPORT;
  return LC_OK;
}
static void lc_workflow_destroy(lc_workflow_handle *workflow) {
  lc_client_handle *client;
  size_t i;
  client = workflow->client;
  for (i = 0U; i < workflow->notification_count; ++i)
    lc_client_free(client, workflow->notifications[i]);
  lc_client_free(client, workflow->notifications);
  for (i = 0U; i < workflow->delayed_notification_count; ++i) {
    lc_client_free(client, workflow->delayed_notifications[i].key);
  }
  lc_client_free(client, workflow->delayed_notifications);
  free(workflow->recovery_cursor);
  lc_client_free(client, workflow->last_error);
  if (workflow->dispatcher_cond_initialized)
    pthread_cond_destroy(&workflow->dispatcher_cond);
  if (workflow->notification_cond_initialized)
    pthread_cond_destroy(&workflow->notification_cond);
  if (workflow->notification_mutex_initialized)
    pthread_mutex_destroy(&workflow->notification_mutex);
  lc_client_free(client, workflow->namespace_name);
  lc_client_free(client, workflow->owner);
  if (workflow->dispatcher_client != NULL) {
    lc_client_close(&workflow->dispatcher_client->pub);
  }
  lc_client_free(client, workflow);
  lc_client_close(&client->pub);
}

static void lc_workflow_retain(lc_workflow_handle *workflow) {
  pthread_mutex_lock(&workflow->notification_mutex);
  ++workflow->ref_count;
  pthread_mutex_unlock(&workflow->notification_mutex);
}

static void lc_workflow_release(lc_workflow_handle *workflow) {
  int destroy = 0;

  pthread_mutex_lock(&workflow->notification_mutex);
  if (workflow->ref_count > 0U && --workflow->ref_count == 0U)
    destroy = 1;
  pthread_mutex_unlock(&workflow->notification_mutex);
  if (destroy)
    lc_workflow_destroy(workflow);
}

static void lc_workflow_close_method(lc_workflow *self) {
  lc_workflow_handle *workflow = (lc_workflow_handle *)self;
  lc_outbox_job_handle *ready_head;

  if (workflow == NULL)
    return;
  if (!workflow->notification_mutex_initialized) {
    lc_workflow_destroy(workflow);
    return;
  }
  pthread_mutex_lock(&workflow->notification_mutex);
  if (workflow->close_requested) {
    pthread_mutex_unlock(&workflow->notification_mutex);
    return;
  }
  workflow->close_requested = 1;
  workflow->closed = 1;
  if (workflow->notification_cond_initialized) {
    pthread_cond_broadcast(&workflow->notification_cond);
  }
  if (workflow->dispatcher_cond_initialized) {
    pthread_cond_broadcast(&workflow->dispatcher_cond);
  }
  pthread_mutex_unlock(&workflow->notification_mutex);
#ifdef LOCKDC_TEST_BUILD
  if (lc_workflow_test_after_close_requested_hook != NULL) {
    lc_workflow_test_after_close_requested_hook(
        lc_workflow_test_after_close_requested_context);
  }
#endif
  if (workflow->dispatcher_started) {
    (void)pthread_join(workflow->dispatcher_thread, NULL);
    workflow->dispatcher_started = 0;
  }
#ifdef LOCKDC_TEST_BUILD
  if (lc_workflow_test_before_ready_job_teardown_hook != NULL) {
    lc_workflow_test_before_ready_job_teardown_hook(
        lc_workflow_test_before_ready_job_teardown_context);
  }
#endif
  pthread_mutex_lock(&workflow->notification_mutex);
  ready_head = workflow->ready_head;
  workflow->ready_head = NULL;
  workflow->ready_tail = NULL;
  workflow->ready_count = 0U;
  pthread_mutex_unlock(&workflow->notification_mutex);
  while (ready_head != NULL) {
    lc_outbox_job_handle *job = ready_head;
    ready_head = job->next;
    job->next = NULL;
    job->pub.close(&job->pub);
  }
  lc_workflow_release(workflow);
}

int lc_client_new_workflow_method(lc_client *self,
                                  const lc_workflow_config *config,
                                  lc_workflow **out, lc_error *error) {
  lc_client_handle *client;
  lc_workflow_handle *workflow;
  lc_unix_seconds configured_claim_deadline;
  time_t now;
  int rc;
  if (self == NULL || config == NULL || out == NULL ||
      config->namespace_name == NULL || config->namespace_name[0] == '\0')
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "new_workflow requires client, namespace config, and output", NULL,
        NULL, NULL);
  client = (lc_client_handle *)self;
  lc_client_handle_retain(client);
  workflow =
      (lc_workflow_handle *)lc_client_calloc(client, 1U, sizeof(*workflow));
  if (workflow == NULL) {
    lc_client_close(&client->pub);
    return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to allocate workflow",
                        NULL, NULL, NULL);
  }
  workflow->client = client;
  workflow->namespace_name = lc_client_strdup(client, config->namespace_name);
  workflow->owner =
      lc_client_strdup(client, config->owner != NULL && config->owner[0] != '\0'
                                   ? config->owner
                                   : "lockdc-workflow");
  if (workflow->namespace_name == NULL || workflow->owner == NULL) {
    lc_workflow_close_method(&workflow->pub);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy workflow configuration", NULL, NULL,
                        NULL);
  }
  workflow->transaction_ttl_seconds = config->transaction_ttl_seconds == 0L
                                          ? 30L
                                          : config->transaction_ttl_seconds;
  if (workflow->transaction_ttl_seconds < 1L) {
    lc_workflow_close_method(&workflow->pub);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow transaction ttl must be positive", NULL, NULL,
                        NULL);
  }
  workflow->claim_ttl_seconds =
      config->claim_ttl_seconds == 0L ? 300L : config->claim_ttl_seconds;
  workflow->max_attempts =
      config->max_attempts == 0 ? 100 : config->max_attempts;
  workflow->replay_dead_letters_on_startup =
      config->replay_dead_letters_on_startup != 0 ? 1 : 0;
  workflow->startup_dead_letter_replay_pending =
      workflow->replay_dead_letters_on_startup;
  workflow->retry_initial_delay_seconds =
      config->retry_initial_delay_seconds == 0L
          ? 1L
          : config->retry_initial_delay_seconds;
  workflow->retry_max_delay_seconds = config->retry_max_delay_seconds == 0L
                                          ? 900L
                                          : config->retry_max_delay_seconds;
  workflow->host_retry_delay_max_seconds =
      config->host_retry_delay_max_seconds == 0L
          ? 3600L
          : config->host_retry_delay_max_seconds;
  workflow->shutdown_timeout_ms =
      config->shutdown_timeout_ms == 0L
          ? (client->timeout_ms > 0L ? client->timeout_ms : 30000L)
          : config->shutdown_timeout_ms;
  workflow->recovery_interval_seconds = config->recovery_interval_seconds;
  if (workflow->recovery_interval_seconds == 0L && !client->is_pouch)
    workflow->recovery_interval_seconds = 300L;
  if (workflow->claim_ttl_seconds < 1L || workflow->max_attempts < 1 ||
      workflow->retry_initial_delay_seconds < 1L ||
      workflow->retry_max_delay_seconds <
          workflow->retry_initial_delay_seconds ||
      workflow->host_retry_delay_max_seconds < 1L ||
      workflow->shutdown_timeout_ms < 1L ||
      workflow->recovery_interval_seconds < 0L) {
    lc_workflow_close_method(&workflow->pub);
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "workflow claim, retry, and recovery configuration is invalid", NULL,
        NULL, NULL);
  }
  now = time(NULL);
  if (now == (time_t)-1) {
    lc_workflow_close_method(&workflow->pub);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "failed to read workflow configuration clock", NULL,
                        NULL, NULL);
  }
  rc = lc_workflow_timestamp_add((lc_unix_seconds)now,
                                 workflow->claim_ttl_seconds, "claim ttl",
                                 &configured_claim_deadline, error);
  if (rc != LC_OK) {
    lc_workflow_close_method(&workflow->pub);
    return rc;
  }
  workflow->notification_capacity = config->notification_capacity == 0U
                                        ? 1024U
                                        : config->notification_capacity;
  workflow->notifications =
      (char **)lc_client_calloc(client, workflow->notification_capacity,
                                sizeof(*workflow->notifications));
  workflow->delayed_notifications =
      (lc_workflow_delayed_notification *)lc_client_calloc(
          client, workflow->notification_capacity,
          sizeof(*workflow->delayed_notifications));
  if (workflow->notifications == NULL ||
      workflow->delayed_notifications == NULL ||
      pthread_mutex_init(&workflow->notification_mutex, NULL) != 0) {
    lc_workflow_close_method(&workflow->pub);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize workflow dispatcher state", NULL,
                        NULL, NULL);
  }
  workflow->notification_mutex_initialized = 1;
  workflow->ref_count = 1U;
  if (pthread_cond_init(&workflow->notification_cond, NULL) != 0) {
    lc_workflow_close_method(&workflow->pub);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize workflow dispatcher state", NULL,
                        NULL, NULL);
  }
  workflow->notification_cond_initialized = 1;
  if (pthread_cond_init(&workflow->dispatcher_cond, NULL) != 0) {
    lc_workflow_close_method(&workflow->pub);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize workflow dispatcher state", NULL,
                        NULL, NULL);
  }
  workflow->dispatcher_cond_initialized = 1;
  if (client->is_pouch) {
    lc_client_handle_retain(client);
    workflow->dispatcher_client = client;
  } else {
    lc_client *dispatcher_client = NULL;
    rc = lc_client_clone_remote(client, workflow->shutdown_timeout_ms,
                                &dispatcher_client, error);
    if (rc != LC_OK) {
      lc_workflow_close_method(&workflow->pub);
      return rc;
    }
    workflow->dispatcher_client = (lc_client_handle *)dispatcher_client;
    lc_engine_client_set_cancel_check(workflow->dispatcher_client->engine,
                                      lc_workflow_dispatcher_cancel_check,
                                      workflow);
  }
  workflow->recovery_needed = 1;
  workflow->recovery_claims_pending = 1;
  if (workflow->recovery_interval_seconds > 0L) {
    workflow->next_recovery_unix =
        (lc_unix_seconds)time(NULL) + workflow->recovery_interval_seconds;
  }
  workflow->pub.accept_command = lc_workflow_accept_command_method;
  workflow->pub.get_command_receipt = lc_workflow_get_command_receipt_method;
  workflow->pub.write_command_result = lc_workflow_write_command_result_method;
  workflow->pub.resume_command = lc_workflow_resume_command_method;
  workflow->pub.append_outbox = lc_workflow_append_outbox_method;
  workflow->pub.accept_inbox = lc_workflow_accept_inbox_method;
  workflow->pub.next = lc_workflow_next_method;
  workflow->pub.get_stats = lc_workflow_get_stats_method;
  workflow->pub.reconcile = lc_workflow_reconcile_method;
  workflow->pub.replay_dead_letter = lc_workflow_replay_dead_letter_method;
  workflow->pub.delete_dead_letter = lc_workflow_delete_dead_letter_method;
  workflow->pub.export_dead_letters = lc_workflow_export_dead_letters_method;
  workflow->pub.close = lc_workflow_close_method;
  if (pthread_create(&workflow->dispatcher_thread, NULL,
                     lc_workflow_dispatcher_main, workflow) != 0) {
    lc_workflow_close_method(&workflow->pub);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to start workflow dispatcher", NULL, NULL,
                        NULL);
  }
  workflow->dispatcher_started = 1;
  *out = &workflow->pub;
  return LC_OK;
}

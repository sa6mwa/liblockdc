#include "lc_api_internal.h"

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

typedef struct lc_workflow_outbox_record {
  char *record_type;
  char *operation_id;
  char *effect_id;
  char *effect_key;
  char *kind;
  char *destination;
  char *content_type;
  char *headers_json;
  char *trace_context;
  char *dispatch_state;
  lonejson_int64 attempt_count;
  lonejson_int64 not_before_unix;
  char *last_error;
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

static const lonejson_field lc_workflow_outbox_record_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_outbox_record, record_type,
                                    "record_type"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_outbox_record, operation_id,
                                    "operation_id"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_outbox_record, effect_id,
                                    "effect_id"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_outbox_record, effect_key,
                                    "effect_key"),
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_outbox_record, kind, "kind"),
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
    LONEJSON_FIELD_I64(lc_workflow_outbox_record, not_before_unix,
                       "not_before_unix"),
    LONEJSON_FIELD_STRING_ALLOC(lc_workflow_outbox_record, last_error,
                                "last_error")};

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
    LONEJSON_FIELD_STRING_ALLOC_REQ(lc_workflow_inbox_record,
                                    processing_state, "processing_state")};

LONEJSON_MAP_DEFINE(lc_workflow_outbox_record_map, lc_workflow_outbox_record,
                    lc_workflow_outbox_record_fields);
LONEJSON_MAP_DEFINE(lc_workflow_inbox_record_map, lc_workflow_inbox_record,
                    lc_workflow_inbox_record_fields);

struct lc_workflow_handle {
  lc_workflow pub;
  lc_client_handle *client;
  char *namespace_name;
  char *owner;
  long transaction_ttl_seconds;
  long claim_ttl_seconds;
  long recovery_interval_seconds;
  long retry_initial_delay_seconds;
  long retry_max_delay_seconds;
  long host_retry_delay_max_seconds;
  int max_attempts;
  pthread_mutex_t notification_mutex;
  pthread_cond_t notification_cond;
  char **notifications;
  size_t notification_count;
  size_t notification_capacity;
  lc_workflow_delayed_notification *delayed_notifications;
  size_t delayed_notification_count;
  char *recovery_cursor;
  int recovery_needed;
  lc_unix_seconds next_recovery_unix;
  int notification_mutex_initialized;
  int notification_cond_initialized;
  pthread_t dispatcher_thread;
  int dispatcher_started;
  lc_outbox_job_handle *ready_head;
  lc_outbox_job_handle *ready_tail;
  int closed;
  int close_requested;
  size_t ref_count;
};

struct lc_workflow_transaction_handle {
  lc_workflow_transaction pub;
  lc_workflow_handle *workflow;
  lc_lease **leases;
  size_t lease_count;
  size_t lease_capacity;
  int terminal;
};

struct lc_workflow_participant_handle {
  lc_workflow_participant pub;
  lc_workflow_transaction_handle *transaction;
  lc_lease *lease;
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

static void lc_workflow_notify(lc_workflow_handle *workflow, const char *key) {
  char *copy;
  if (workflow == NULL || key == NULL || strncmp(key, "__lockdc_io/v1/outbox/", sizeof("__lockdc_io/v1/outbox/") - 1U) != 0) return;
  copy = lc_client_strdup(workflow->client, key);
  if (copy == NULL) return;
  pthread_mutex_lock(&workflow->notification_mutex);
  if (!workflow->closed && workflow->notification_count < workflow->notification_capacity) {
    workflow->notifications[workflow->notification_count++] = copy;
    pthread_cond_signal(&workflow->notification_cond);
    copy = NULL;
  } else if (!workflow->closed) {
    workflow->recovery_needed = 1;
    pthread_cond_signal(&workflow->notification_cond);
  }
  pthread_mutex_unlock(&workflow->notification_mutex);
  lc_client_free(workflow->client, copy);
}

static void lc_workflow_schedule_retry(lc_workflow_handle *workflow,
                                       const char *key,
                                       lc_unix_seconds eligible_at_unix) {
  char *copy;
  size_t index;

  if (workflow == NULL || key == NULL || eligible_at_unix <= 0) return;
  copy = lc_client_strdup(workflow->client, key);
  if (copy == NULL) return;
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
      if (workflow->delayed_notification_count < workflow->notification_capacity) {
        lc_workflow_delayed_notification *delayed =
            &workflow->delayed_notifications[workflow->delayed_notification_count++];
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
    pthread_cond_signal(&workflow->notification_cond);
  }
  pthread_mutex_unlock(&workflow->notification_mutex);
  lc_client_free(workflow->client, copy);
}

static void lc_workflow_promote_due_retries_locked(
    lc_workflow_handle *workflow, lc_unix_seconds now) {
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
    if (deadline == 0 || eligible < deadline) deadline = eligible;
  }
  return deadline;
}

static int lc_workflow_transaction_add_lease(
    lc_workflow_transaction_handle *transaction, lc_lease *lease,
    lc_error *error) {
  lc_lease **grown;
  size_t capacity;

  if (transaction->lease_count == transaction->lease_capacity) {
    capacity = transaction->lease_capacity == 0U ? 4U
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

static int lc_workflow_digest(const char *value, char out[44], lc_error *error) {
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
                        "failed to digest workflow identity", NULL, NULL,
                        NULL);
  }
  EVP_MD_CTX_free(ctx);
  for (i = 0U; i < 30U; i += 3U) {
    out[(i / 3U) * 4U] = base64url[digest[i] >> 2U];
    out[(i / 3U) * 4U + 1U] = base64url[((digest[i] & 0x03U) << 4U) | (digest[i + 1U] >> 4U)];
    out[(i / 3U) * 4U + 2U] = base64url[((digest[i + 1U] & 0x0fU) << 2U) | (digest[i + 2U] >> 6U)];
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
                        "outbox operation, effect, effect key, kind, and destination are required",
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
  if (message == NULL || message->consumer_id == NULL || message->source_kind == NULL || message->source_id == NULL || message->message_id == NULL || message->consumer_id[0] == '\0' || message->source_kind[0] == '\0' || message->source_id[0] == '\0' || message->message_id[0] == '\0') return lc_error_set(error, LC_ERR_INVALID, 0L, "inbox consumer and source identity are required", NULL, NULL, NULL);
  ctx = EVP_MD_CTX_new(); length = 0U;
  if (ctx == NULL || EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) != 1 || EVP_DigestUpdate(ctx, message->consumer_id, strlen(message->consumer_id)) != 1 || EVP_DigestUpdate(ctx, "\n", 1U) != 1 || EVP_DigestUpdate(ctx, message->source_kind, strlen(message->source_kind)) != 1 || EVP_DigestUpdate(ctx, "\n", 1U) != 1 || EVP_DigestUpdate(ctx, message->source_id, strlen(message->source_id)) != 1 || EVP_DigestUpdate(ctx, "\n", 1U) != 1 || EVP_DigestUpdate(ctx, message->message_id, strlen(message->message_id)) != 1 || EVP_DigestFinal_ex(ctx, digest, &length) != 1 || length != 32U) { EVP_MD_CTX_free(ctx); return lc_error_set(error, LC_ERR_PROTOCOL, 0L, "failed to digest inbox identity", NULL, NULL, NULL); }
  EVP_MD_CTX_free(ctx);
  for (i = 0U; i < 30U; i += 3U) {
    identity[(i / 3U) * 4U] = base64url[digest[i] >> 2U];
    identity[(i / 3U) * 4U + 1U] = base64url[((digest[i] & 0x03U) << 4U) |
                                               (digest[i + 1U] >> 4U)];
    identity[(i / 3U) * 4U + 2U] = base64url[((digest[i + 1U] & 0x0fU) << 2U) |
                                               (digest[i + 2U] >> 6U)];
    identity[(i / 3U) * 4U + 3U] = base64url[digest[i + 2U] & 0x3fU];
  }
  identity[40] = base64url[digest[30] >> 2U];
  identity[41] = base64url[((digest[30] & 0x03U) << 4U) |
                             (digest[31] >> 4U)];
  identity[42] = base64url[(digest[31] & 0x0fU) << 2U];
  identity[43] = '\0';
  key_length = sizeof("__lockdc_io/v1/inbox/") - 1U + 43U + 1U;
  key = (char *)malloc(key_length); if (key == NULL) return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to allocate inbox key", NULL, NULL, NULL);
  snprintf(key, key_length, "__lockdc_io/v1/inbox/%s", identity);
  (void)workflow; *out = key; return LC_OK;
}

static int lc_workflow_stage_inbox(lc_lease *lease, const lc_inbox_message *message, lc_error *error) {
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

static int lc_workflow_existing_outbox(lc_workflow_handle *workflow,
                                       const char *key,
                                       const lc_outbox_entry *entry,
                                       lc_outbox_receipt *receipt,
                                       lc_error *error) {
  lc_workflow_outbox_record record;
  lc_get_res result;
  lc_get_opts options;
  lonejson *runtime;
  int rc;

  memset(&record, 0, sizeof(record));
  memset(&result, 0, sizeof(result));
  lc_get_opts_init(&options);
  options.public_read = 1;
  runtime = lc_thread_lonejson_runtime();
  rc = lc_load_in_namespace(&workflow->client->pub, workflow->namespace_name,
                            key, &lc_workflow_outbox_record_map, &record,
                            &options, &result, error);
  if (rc == LC_OK &&
      (result.no_content || record.record_type == NULL ||
       record.operation_id == NULL || record.effect_id == NULL ||
       record.effect_key == NULL || record.kind == NULL ||
       record.destination == NULL ||
       strcmp(record.record_type, "lockdc.outbox.v1") != 0 ||
       strcmp(record.operation_id, entry->operation_id) != 0 ||
       strcmp(record.effect_id, entry->effect_id) != 0 ||
       strcmp(record.effect_key, entry->effect_key) != 0 ||
       strcmp(record.kind, entry->kind) != 0 ||
       strcmp(record.destination, entry->destination) != 0)) {
    rc = lc_error_set(error, LC_ERR_SERVER, 0L,
                      "outbox immutable fields conflict with an existing record",
                      NULL, NULL, NULL);
  }
  runtime->cleanup(runtime, &lc_workflow_outbox_record_map, &record);
  lc_get_res_cleanup(&result);
  if (rc == LC_OK) { receipt->outbox_key = lc_strdup_local(key); receipt->effect_key = lc_strdup_local(entry->effect_key); if (receipt->outbox_key == NULL || receipt->effect_key == NULL) { lc_outbox_receipt_cleanup(receipt); return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to allocate duplicate outbox receipt", NULL, NULL, NULL); } receipt->duplicate = 1; }
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
        strcmp(record.payload_digest, message->payload_digest) != 0))) {
    rc = lc_error_set(error, LC_ERR_SERVER, 0L,
                      "inbox immutable fields conflict with an existing record",
                      NULL, NULL, NULL);
  }
  runtime->cleanup(runtime, &lc_workflow_inbox_record_map, &record);
  lc_get_res_cleanup(&load_result);
  if (rc == LC_OK) result->duplicate = 1;
  return rc;
}

static int lc_workflow_stage_outbox(lc_lease *lease,
                                    const lc_outbox_entry *entry,
                                    lc_source *payload, lc_error *error) {
  lc_workflow_outbox_record record;
  lc_attach_req attach;
  lc_attach_res attach_result;
  int rc;

  if (payload == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox payload source is required", NULL, NULL,
                        NULL);
  }
  memset(&record, 0, sizeof(record));
  record.record_type = "lockdc.outbox.v1";
  record.operation_id = (char *)entry->operation_id;
  record.effect_id = (char *)entry->effect_id;
  record.effect_key = (char *)entry->effect_key;
  record.kind = (char *)entry->kind;
  record.destination = (char *)entry->destination;
  record.content_type = (char *)(entry->content_type != NULL
                                     ? entry->content_type
                                     : "application/octet-stream");
  record.headers_json = (char *)entry->headers_json;
  record.trace_context = (char *)entry->trace_context;
  record.dispatch_state = "pending";
  record.attempt_count = 0;
  record.not_before_unix = 0;
  rc = lc_lease_save(lease, &lc_workflow_outbox_record_map, &record, error);
  if (rc != LC_OK) {
    return rc;
  }
  lc_attach_req_init(&attach);
  attach.name = "payload";
  attach.content_type = entry->content_type != NULL ? entry->content_type
                                                     : "application/octet-stream";
  attach.prevent_overwrite = 1;
  memset(&attach_result, 0, sizeof(attach_result));
  rc = lc_lease_attach(lease, &attach, payload, &attach_result, error);
  lc_attach_res_cleanup(&attach_result);
  return rc;
}

static void lc_workflow_outbox_record_clear(lc_client_handle *client,
                                            lc_workflow_outbox_record *record) {
  if (record == NULL) return;
  lc_client_free(client, record->record_type);
  lc_client_free(client, record->operation_id);
  lc_client_free(client, record->effect_id);
  lc_client_free(client, record->effect_key);
  lc_client_free(client, record->kind);
  lc_client_free(client, record->destination);
  lc_client_free(client, record->content_type);
  lc_client_free(client, record->headers_json);
  lc_client_free(client, record->trace_context);
  lc_client_free(client, record->dispatch_state);
  lc_client_free(client, record->last_error);
  memset(record, 0, sizeof(*record));
}

static int lc_workflow_outbox_record_copy(lc_client_handle *client,
                                          lc_workflow_outbox_record *dst,
                                          const lc_workflow_outbox_record *src,
                                          lc_error *error) {
  memset(dst, 0, sizeof(*dst));
  if ((src->record_type != NULL &&
       (dst->record_type = lc_client_strdup(client, src->record_type)) == NULL) ||
      (src->operation_id != NULL &&
       (dst->operation_id = lc_client_strdup(client, src->operation_id)) == NULL) ||
      (src->effect_id != NULL &&
       (dst->effect_id = lc_client_strdup(client, src->effect_id)) == NULL) ||
      (src->effect_key != NULL &&
       (dst->effect_key = lc_client_strdup(client, src->effect_key)) == NULL) ||
      (src->kind != NULL &&
       (dst->kind = lc_client_strdup(client, src->kind)) == NULL) ||
      (src->destination != NULL &&
       (dst->destination = lc_client_strdup(client, src->destination)) == NULL) ||
      (src->content_type != NULL &&
       (dst->content_type = lc_client_strdup(client, src->content_type)) == NULL) ||
      (src->headers_json != NULL &&
       (dst->headers_json = lc_client_strdup(client, src->headers_json)) == NULL) ||
      (src->trace_context != NULL &&
       (dst->trace_context = lc_client_strdup(client, src->trace_context)) == NULL) ||
      (src->dispatch_state != NULL &&
       (dst->dispatch_state = lc_client_strdup(client, src->dispatch_state)) == NULL) ||
      (src->last_error != NULL &&
       (dst->last_error = lc_client_strdup(client, src->last_error)) == NULL)) {
    lc_workflow_outbox_record_clear(client, dst);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy outbox envelope", NULL, NULL, NULL);
  }
  dst->attempt_count = src->attempt_count;
  dst->not_before_unix = src->not_before_unix;
  return LC_OK;
}

static void lc_outbox_job_refresh(lc_outbox_job_handle *job) {
  job->pub.outbox_key = job->outbox_key;
  job->pub.operation_id = job->record.operation_id;
  job->pub.effect_id = job->record.effect_id;
  job->pub.effect_key = job->record.effect_key;
  job->pub.kind = job->record.kind;
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

  if (job == NULL) return;
  client = job->client;
  workflow = job->workflow;
  if (job->lease != NULL) job->lease->close(job->lease);
  lc_workflow_outbox_record_clear(client, &job->record);
  lc_client_free(client, job->outbox_key);
  lc_client_free(client, job);
  if (workflow != NULL) lc_workflow_release(workflow);
  lc_client_close(&client->pub);
}

static int lc_outbox_job_write_payload_method(lc_outbox_job *self,
                                               lc_sink *dst, size_t *written,
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
  if (rc == LC_OK && written != NULL) *written = (size_t)result.attachment.size;
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
  if (rc == LC_OK) lc_outbox_job_refresh(job);
  return rc;
}

static int lc_outbox_job_terminal(lc_outbox_job *self, const char *state,
                                  long not_before_unix,
                                  const char *diagnostic, lc_error *error) {
  lc_outbox_job_handle *job = (lc_outbox_job_handle *)self;
  lc_workflow_outbox_record record;
  lc_release_req release;
  int rc;

  if (job == NULL || job->terminal || job->lease == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, "outbox job is closed",
                        NULL, NULL, NULL);
  }
  record = job->record;
  record.dispatch_state = (char *)state;
  record.not_before_unix = not_before_unix;
  record.last_error = (char *)diagnostic;
  rc = lc_lease_save(job->lease, &lc_workflow_outbox_record_map, &record,
                     error);
  if (rc != LC_OK) return rc;
  lc_release_req_init(&release);
  rc = lc_lease_release(job->lease, &release, error);
  if (rc != LC_OK) return rc;
  job->lease = NULL;
  job->terminal = 1;
  lc_outbox_job_refresh(job);
  if (strcmp(state, "retry_wait") == 0) {
    lc_workflow_schedule_retry(job->workflow, job->outbox_key,
                               (lc_unix_seconds)not_before_unix);
  }
  return LC_OK;
}

static int lc_outbox_job_complete_method(lc_outbox_job *self,
                                          lc_error *error) {
  return lc_outbox_job_terminal(self, "completed", 0L, NULL, error);
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
  if (job->record.attempt_count >= job->pub.max_attempts) {
    return lc_outbox_job_terminal(self, "dead_letter", 0L,
                                  request->diagnostic, error);
  }
  return lc_outbox_job_terminal(self, "retry_wait", (long)(now + delay),
                                request->diagnostic, error);
}

static int lc_outbox_job_dead_letter_method(lc_outbox_job *self,
                                             const char *diagnostic,
                                             lc_error *error) {
  return lc_outbox_job_terminal(self, "dead_letter", 0L, diagnostic, error);
}

static int lc_workflow_claim_outbox(lc_workflow_handle *workflow,
                                    const char *key, lc_outbox_job **out,
                                    lc_error *error) {
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_workflow_outbox_record record;
  lc_get_res result;
  lonejson *runtime;
  lc_outbox_job_handle *job;
  time_t now;
  int rc;

  *out = NULL;
  lc_acquire_req_init(&acquire);
  acquire.namespace_name = workflow->namespace_name;
  acquire.key = key;
  acquire.owner = workflow->owner;
  acquire.ttl_seconds = workflow->claim_ttl_seconds;
  lease = NULL;
  rc = lc_acquire(&workflow->client->pub, &acquire, &lease, error);
  if (rc != LC_OK) return rc;
  memset(&record, 0, sizeof(record));
  memset(&result, 0, sizeof(result));
  runtime = lc_thread_lonejson_runtime();
  rc = lc_lease_load(lease, &lc_workflow_outbox_record_map, &record, NULL,
                     &result, error);
  if (rc == LC_OK) {
    now = time(NULL);
    if (now == (time_t)-1 || strcmp(record.record_type, "lockdc.outbox.v1") != 0 ||
        (strcmp(record.dispatch_state, "pending") != 0 &&
         strcmp(record.dispatch_state, "retry_wait") != 0)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox candidate is not currently dispatchable", NULL,
                        NULL, NULL);
    } else if (record.not_before_unix > (long)now) {
      lc_workflow_schedule_retry(workflow, key,
                                 (lc_unix_seconds)record.not_before_unix);
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox candidate retry is not yet eligible", NULL,
                        NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    lc_workflow_outbox_record claimed = record;
    claimed.dispatch_state = "claimed";
    ++claimed.attempt_count;
    rc = lc_lease_save(lease, &lc_workflow_outbox_record_map, &claimed, error);
    if (rc == LC_OK) record.attempt_count = claimed.attempt_count;
  }
  if (rc == LC_OK) {
    job = (lc_outbox_job_handle *)lc_client_calloc(workflow->client, 1U,
                                                    sizeof(*job));
    if (job == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate outbox job", NULL, NULL, NULL);
    }
  } else {
    job = NULL;
  }
  if (rc == LC_OK) rc = lc_workflow_outbox_record_copy(workflow->client,
                                                         &job->record, &record,
                                                         error);
  runtime->cleanup(runtime, &lc_workflow_outbox_record_map, &record);
  lc_get_res_cleanup(&result);
  if (rc != LC_OK) {
    lc_release_req release;
    lc_release_req_init(&release);
    release.rollback = 1;
    (void)lc_lease_release(lease, &release, NULL);
    if (job != NULL) lc_client_free(workflow->client, job);
    return rc;
  }
  lc_client_handle_retain(workflow->client);
  job->client = workflow->client;
  job->workflow = workflow;
  lc_workflow_retain(workflow);
  job->lease = lease;
  job->outbox_key = lc_client_strdup(workflow->client, lease->key);
  if (job->outbox_key == NULL) {
    lc_release_req release;
    lc_release_req_init(&release);
    release.rollback = 1;
    (void)lc_lease_release(lease, &release, NULL);
    lc_workflow_release(workflow);
    lc_workflow_outbox_record_clear(workflow->client, &job->record);
    lc_client_free(workflow->client, job);
    lc_client_close(&workflow->client->pub);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy outbox job key", NULL, NULL, NULL);
  }
  job->pub.write_payload = lc_outbox_job_write_payload_method;
  job->pub.renew = lc_outbox_job_renew_method;
  job->pub.complete = lc_outbox_job_complete_method;
  job->pub.retry = lc_outbox_job_retry_method;
  job->pub.dead_letter = lc_outbox_job_dead_letter_method;
  job->pub.close = lc_outbox_job_close_method;
  job->pub.max_attempts = workflow->max_attempts;
  lc_outbox_job_refresh(job);
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
  lc_workflow_notify(capture->workflow, capture->key);
  return 1;
}

static int lc_workflow_reconcile(lc_workflow_handle *workflow,
                                 lc_error *error) {
  static const char selector[] =
      "{\"in\":{\"field\":\"/dispatch_state\",\"any\":[\"pending\",\"retry_wait\"]}}";
  lc_query_req request;
  lc_query_key_handler handler;
  lc_query_res result;
  lc_index_flush_req flush_request;
  lc_index_flush_res flush_result;
  lc_workflow_recovery_capture capture;
  char *cursor;
  int rc;

  lc_query_req_init(&request);
  lc_index_flush_req_init(&flush_request);
  memset(&handler, 0, sizeof(handler));
  memset(&result, 0, sizeof(result));
  memset(&flush_result, 0, sizeof(flush_result));
  memset(&capture, 0, sizeof(capture));
  handler.begin = lc_workflow_recovery_key_begin;
  handler.chunk = lc_workflow_recovery_key_chunk;
  handler.end = lc_workflow_recovery_key_end;
  capture.workflow = workflow;
  /* A recovery sweep starts from a durable index boundary. Later pages keep
   * that boundary: flushing every page would turn one large sweep into N
   * global flushes and needlessly amplify reconciliation cost. */
  if (workflow->recovery_cursor == NULL) {
    flush_request.namespace_name = workflow->namespace_name;
    flush_request.mode = "wait";
    rc = lc_flush_index(&workflow->client->pub, &flush_request, &flush_result,
                        error);
    lc_index_flush_res_cleanup(&flush_result);
    if (rc != LC_OK) return rc;
  }
  request.namespace_name = workflow->namespace_name;
  request.selector_json = selector;
  request.limit = (long)workflow->notification_capacity;
  request.cursor = workflow->recovery_cursor;
  request.engine = "index";
  request.refresh = "wait_for";
  rc = lc_query_keys(&workflow->client->pub, &request, &handler, &capture,
                     &result, error);
  if (rc != LC_OK) {
    lc_query_res_cleanup(&result);
    return rc;
  }
  cursor = result.cursor;
  result.cursor = NULL;
  lc_query_res_cleanup(&result);
  lc_client_free(workflow->client, workflow->recovery_cursor);
  workflow->recovery_cursor = cursor;
  pthread_mutex_lock(&workflow->notification_mutex);
  workflow->recovery_needed = workflow->recovery_cursor != NULL;
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
    while (!workflow->closed && workflow->notification_count == 0U &&
           !workflow->recovery_needed) {
      lc_unix_seconds now = (lc_unix_seconds)time(NULL);
      lc_unix_seconds due;
      if (now > 0) lc_workflow_promote_due_retries_locked(workflow, now);
      if (workflow->notification_count > 0U || workflow->recovery_needed) {
        break;
      }
      due = lc_workflow_next_dispatch_deadline_locked(workflow);
      if (due > 0) {
        struct timespec deadline;
        deadline.tv_sec = due;
        deadline.tv_nsec = 0L;
        if (pthread_cond_timedwait(&workflow->notification_cond,
                                   &workflow->notification_mutex,
                                   &deadline) == ETIMEDOUT) {
          now = (lc_unix_seconds)time(NULL);
          if (now > 0) lc_workflow_promote_due_retries_locked(workflow, now);
          if (workflow->notification_count == 0U &&
              workflow->next_recovery_unix > 0 &&
              now >= workflow->next_recovery_unix) {
            workflow->recovery_needed = 1;
          }
        }
      } else {
        pthread_cond_wait(&workflow->notification_cond,
                          &workflow->notification_mutex);
      }
    }
    if (workflow->closed) {
      pthread_mutex_unlock(&workflow->notification_mutex);
      break;
    }
    if (workflow->notification_count == 0U) {
      reconcile = 1;
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
      recovery_rc = lc_workflow_reconcile(workflow, &error);
      lc_error_cleanup(&error);
      pthread_mutex_lock(&workflow->notification_mutex);
      if (recovery_rc != LC_OK) {
        workflow->recovery_needed = 0;
        workflow->next_recovery_unix = (lc_unix_seconds)time(NULL) + 1L;
      } else if (workflow->recovery_interval_seconds > 0L) {
        workflow->next_recovery_unix =
            (lc_unix_seconds)time(NULL) + workflow->recovery_interval_seconds;
      } else {
        workflow->next_recovery_unix = 0;
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
        if (workflow->ready_tail != NULL) workflow->ready_tail->next = handle;
        else workflow->ready_head = handle;
        workflow->ready_tail = handle;
        pthread_cond_broadcast(&workflow->notification_cond);
        job = NULL;
      }
      pthread_mutex_unlock(&workflow->notification_mutex);
    }
    if (job != NULL) job->close(job);
    lc_error_cleanup(&error);
    lc_client_free(workflow->client, key);
  }
  return NULL;
}

static int lc_workflow_wait_for_ready(lc_workflow_handle *workflow,
                                      long timeout_ms, lc_outbox_job **out,
                                      lc_error *error) {
  int wait_rc;

  if (timeout_ms < -1L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow next timeout must be -1 or non-negative",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  pthread_mutex_lock(&workflow->notification_mutex);
  while (!workflow->closed && workflow->ready_head == NULL) {
    if (timeout_ms == 0L) break;
    if (timeout_ms < 0L) {
      wait_rc = pthread_cond_wait(&workflow->notification_cond,
                                  &workflow->notification_mutex);
    } else {
      struct timespec deadline;
      if (clock_gettime(CLOCK_REALTIME, &deadline) != 0) {
        pthread_mutex_unlock(&workflow->notification_mutex);
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
      wait_rc = pthread_cond_timedwait(&workflow->notification_cond,
                                       &workflow->notification_mutex,
                                       &deadline);
    }
    if (wait_rc == ETIMEDOUT) break;
    if (wait_rc != 0) {
      pthread_mutex_unlock(&workflow->notification_mutex);
      return lc_error_set(error, LC_ERR_PROTOCOL, wait_rc,
                          "workflow dispatcher wait failed", NULL, NULL,
                          NULL);
    }
  }
  if (workflow->ready_head != NULL) {
    lc_outbox_job_handle *job = workflow->ready_head;
    workflow->ready_head = job->next;
    if (workflow->ready_head == NULL) workflow->ready_tail = NULL;
    job->next = NULL;
    *out = &job->pub;
  }
  pthread_mutex_unlock(&workflow->notification_mutex);
  return LC_OK;
}

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

static int lc_workflow_participant_describe(lc_workflow_participant *self,
                                            lc_error *error) {
  lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self;
  int rc;
  if (p == NULL || p->lease == NULL) return lc_error_set(error, LC_ERR_INVALID, 0L, "workflow participant is closed", NULL, NULL, NULL);
  rc = lc_lease_describe(p->lease, error); lc_workflow_participant_refresh(p); return rc;
}
static int lc_workflow_participant_get(lc_workflow_participant *self, lc_sink *dst, const lc_get_opts *opts, lc_get_res *out, lc_error *error) {
  lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self;
  if (p == NULL || p->lease == NULL) return lc_error_set(error, LC_ERR_INVALID, 0L, "workflow participant is closed", NULL, NULL, NULL);
  return lc_lease_get(p->lease, dst, opts, out, error);
}
static int lc_workflow_participant_update(lc_workflow_participant *self, lc_source *src, const lc_update_opts *opts, lc_error *error) {
  lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self; int rc;
  if (p == NULL || p->lease == NULL) return lc_error_set(error, LC_ERR_INVALID, 0L, "workflow participant is closed", NULL, NULL, NULL);
  rc = lc_lease_update(p->lease, src, opts, error); lc_workflow_participant_refresh(p); return rc;
}
static int lc_workflow_participant_mutate(lc_workflow_participant *self,
                                          const lc_mutate_req *req,
                                          lc_error *error) {
  lc_workflow_participant_handle *p =
      (lc_workflow_participant_handle *)self;
  int rc;

  if (p == NULL || p->lease == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow participant is closed", NULL, NULL, NULL);
  }
  rc = lc_lease_mutate(p->lease, req, error);
  lc_workflow_participant_refresh(p);
  return rc;
}
static int lc_workflow_participant_mutate_local(
    lc_workflow_participant *self, const lc_mutate_local_req *req,
    lc_error *error) {
  lc_workflow_participant_handle *p =
      (lc_workflow_participant_handle *)self;
  int rc;

  if (p == NULL || p->lease == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow participant is closed", NULL, NULL, NULL);
  }
  rc = lc_lease_mutate_local(p->lease, req, error);
  lc_workflow_participant_refresh(p);
  return rc;
}
static int lc_workflow_participant_metadata(lc_workflow_participant *self, const lc_metadata_req *req, lc_error *error) { lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self; int rc; if (p == NULL || p->lease == NULL) return lc_error_set(error, LC_ERR_INVALID, 0L, "workflow participant is closed", NULL, NULL, NULL); rc = lc_lease_metadata(p->lease, req, error); lc_workflow_participant_refresh(p); return rc; }
static int lc_workflow_participant_remove(lc_workflow_participant *self, const lc_remove_req *req, lc_error *error) { lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self; int rc; if (p == NULL || p->lease == NULL) return lc_error_set(error, LC_ERR_INVALID, 0L, "workflow participant is closed", NULL, NULL, NULL); rc = lc_lease_remove(p->lease, req, error); lc_workflow_participant_refresh(p); return rc; }
static int lc_workflow_participant_keepalive(lc_workflow_participant *self, const lc_keepalive_req *req, lc_error *error) { lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self; int rc; if (p == NULL || p->lease == NULL) return lc_error_set(error, LC_ERR_INVALID, 0L, "workflow participant is closed", NULL, NULL, NULL); rc = lc_lease_keepalive(p->lease, req, error); lc_workflow_participant_refresh(p); return rc; }
static int lc_workflow_participant_attach(lc_workflow_participant *self, const lc_attach_req *req, lc_source *src, lc_attach_res *out, lc_error *error) { lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self; if (p == NULL || p->lease == NULL) return lc_error_set(error, LC_ERR_INVALID, 0L, "workflow participant is closed", NULL, NULL, NULL); return lc_lease_attach(p->lease, req, src, out, error); }
static int lc_workflow_participant_list_attachments(
    lc_workflow_participant *self, lc_attachment_list *out, lc_error *error) {
  lc_workflow_participant_handle *p =
      (lc_workflow_participant_handle *)self;
  if (p == NULL || p->lease == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow participant is closed", NULL, NULL, NULL);
  }
  return lc_lease_list_attachments(p->lease, out, error);
}
static int lc_workflow_participant_get_attachment(lc_workflow_participant *self, const lc_attachment_get_req *req, lc_sink *dst, lc_attachment_get_res *out, lc_error *error) { lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self; if (p == NULL || p->lease == NULL) return lc_error_set(error, LC_ERR_INVALID, 0L, "workflow participant is closed", NULL, NULL, NULL); return lc_lease_get_attachment(p->lease, req, dst, out, error); }
static int lc_workflow_participant_delete_attachment(
    lc_workflow_participant *self, const lc_attachment_selector *selector,
    int *deleted, lc_error *error) {
  lc_workflow_participant_handle *p =
      (lc_workflow_participant_handle *)self;
  if (p == NULL || p->lease == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow participant is closed", NULL, NULL, NULL);
  }
  return lc_lease_delete_attachment(p->lease, selector, deleted, error);
}
static int lc_workflow_participant_delete_all_attachments(
    lc_workflow_participant *self, int *deleted_count, lc_error *error) {
  lc_workflow_participant_handle *p =
      (lc_workflow_participant_handle *)self;
  if (p == NULL || p->lease == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow participant is closed", NULL, NULL, NULL);
  }
  return lc_lease_delete_all_attachments(p->lease, deleted_count, error);
}
static void lc_workflow_participant_close_method(lc_workflow_participant *self) { lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self; if (p != NULL) { lc_client_free(p->transaction->workflow->client, p); } }

static int lc_workflow_transaction_acquire_method(
    lc_workflow_transaction *self, const lc_workflow_participant_request *request,
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
                        "workflow participant requires an open transaction, key, and no caller transaction id",
                        NULL, NULL, NULL);
  }
  acquire = request->acquire;
  acquire.txn_id = transaction->leases[0]->txn_id;
  if (acquire.namespace_name == NULL) acquire.namespace_name = transaction->workflow->namespace_name;
  if (acquire.owner == NULL || acquire.owner[0] == '\0') acquire.owner = transaction->workflow->owner;
  if (acquire.ttl_seconds == 0L) acquire.ttl_seconds = transaction->workflow->transaction_ttl_seconds;
  lease = NULL;
  rc = lc_acquire(&transaction->workflow->client->pub, &acquire, &lease, error);
  if (rc != LC_OK) return rc;
  rc = lc_workflow_transaction_add_lease(transaction, lease, error);
  if (rc != LC_OK) { lc_lease_close(lease); return rc; }
  participant = (lc_workflow_participant_handle *)lc_client_calloc(
      transaction->workflow->client, 1U, sizeof(*participant));
  if (participant == NULL) return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to allocate workflow participant", NULL, NULL, NULL);
  participant->transaction = transaction; participant->lease = lease;
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

static int lc_workflow_transaction_append_outbox_method(
    lc_workflow_transaction *self, const lc_outbox_entry *entry,
    lc_source *payload, lc_outbox_receipt *receipt, lc_error *error) {
  lc_workflow_transaction_handle *transaction =
      (lc_workflow_transaction_handle *)self;
  lc_acquire_req acquire;
  lc_lease *lease;
  char *key;
  int rc;
  if (transaction == NULL || transaction->terminal || receipt == NULL ||
      transaction->lease_count == 0U) return lc_error_set(error, LC_ERR_INVALID, 0L, "workflow transaction is closed", NULL, NULL, NULL);
  lc_outbox_receipt_cleanup(receipt); key = NULL;
  rc = lc_workflow_outbox_key(transaction->workflow, entry, &key, error);
  if (rc != LC_OK) return rc;
  lc_acquire_req_init(&acquire); acquire.namespace_name = transaction->workflow->namespace_name; acquire.key = key; acquire.owner = transaction->workflow->owner; acquire.ttl_seconds = transaction->workflow->transaction_ttl_seconds; acquire.if_not_exists = 1; acquire.txn_id = transaction->leases[0]->txn_id;
  lease = NULL; rc = lc_acquire(&transaction->workflow->client->pub, &acquire, &lease, error);
  if (rc != LC_OK) { if (error != NULL) { lc_error_cleanup(error); lc_error_init(error); } rc = lc_workflow_existing_outbox(transaction->workflow, key, entry, receipt, error); free(key); return rc; }
  rc = lc_workflow_stage_outbox(lease, entry, payload, error);
  if (rc != LC_OK) { lc_release_req rollback; lc_release_req_init(&rollback); rollback.rollback = 1; (void)lc_lease_release(lease, &rollback, NULL); free(key); return rc; }
  rc = lc_workflow_transaction_add_lease(transaction, lease, error);
  if (rc != LC_OK) { lc_lease_close(lease); free(key); return rc; }
  receipt->outbox_key = key; receipt->effect_key = lc_strdup_local(entry->effect_key);
  if (receipt->effect_key == NULL) return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to allocate outbox receipt", NULL, NULL, NULL);
  return LC_OK;
}

static int lc_workflow_transaction_terminal(lc_workflow_transaction *self,
                                            int rollback, lc_error *error) {
  lc_workflow_transaction_handle *transaction = (lc_workflow_transaction_handle *)self;
  lc_release_req request;
  char **notification_keys;
  size_t i;
  int rc;
  if (transaction == NULL || transaction->terminal) return lc_error_set(error, LC_ERR_INVALID, 0L, "workflow transaction is closed", NULL, NULL, NULL);
  notification_keys = NULL;
  if (!rollback) {
    notification_keys = (char **)lc_client_calloc(
        transaction->workflow->client, transaction->lease_count,
        sizeof(*notification_keys));
    if (notification_keys == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to prepare workflow dispatch signals", NULL,
                          NULL, NULL);
    }
    for (i = 0U; i < transaction->lease_count; ++i) {
      if (transaction->leases[i] != NULL) {
        notification_keys[i] = lc_client_strdup(
            transaction->workflow->client, transaction->leases[i]->key);
        if (notification_keys[i] == NULL) {
          while (i > 0U) {
            --i;
            lc_client_free(transaction->workflow->client,
                           notification_keys[i]);
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
    if (transaction->leases[i] == NULL) continue;
    rc = lc_lease_release(transaction->leases[i], &request, error);
    if (rc != LC_OK) {
      size_t j;
      for (j = 0U; j < transaction->lease_count; ++j) {
        lc_client_free(transaction->workflow->client, notification_keys == NULL
                                                       ? NULL
                                                       : notification_keys[j]);
      }
      lc_client_free(transaction->workflow->client, notification_keys);
      return rc;
    }
    transaction->leases[i] = NULL;
  }
  transaction->terminal = 1;
  for (i = 0U; i < transaction->lease_count; ++i) {
    lc_workflow_notify(transaction->workflow,
                       notification_keys == NULL ? NULL : notification_keys[i]);
    lc_client_free(transaction->workflow->client,
                   notification_keys == NULL ? NULL : notification_keys[i]);
  }
  lc_client_free(transaction->workflow->client, notification_keys);
  return LC_OK;
}
static int lc_workflow_transaction_commit_method(lc_workflow_transaction *self, lc_error *error) { return lc_workflow_transaction_terminal(self, 0, error); }
static int lc_workflow_transaction_rollback_method(lc_workflow_transaction *self, lc_error *error) { return lc_workflow_transaction_terminal(self, 1, error); }
static void lc_workflow_transaction_close_method(lc_workflow_transaction *self) { lc_workflow_transaction_handle *transaction = (lc_workflow_transaction_handle *)self; lc_workflow_handle *workflow; size_t i; if (transaction == NULL) return; workflow = transaction->workflow; if (!transaction->terminal) (void)lc_workflow_transaction_terminal(self, 1, NULL); for (i = 0U; i < transaction->lease_count; ++i) lc_lease_close(transaction->leases[i]); lc_client_free(workflow->client, transaction->leases); lc_client_free(workflow->client, transaction); lc_workflow_release(workflow); }

static lc_workflow_transaction *lc_workflow_transaction_new(lc_workflow_handle *workflow, lc_lease *first, lc_error *error) {
  lc_workflow_transaction_handle *transaction;
  transaction = (lc_workflow_transaction_handle *)lc_client_calloc(workflow->client, 1U, sizeof(*transaction));
  if (transaction == NULL) { lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to allocate workflow transaction", NULL, NULL, NULL); return NULL; }
  transaction->workflow = workflow;
  lc_workflow_retain(workflow);
  transaction->pub.acquire = lc_workflow_transaction_acquire_method;
  transaction->pub.append_outbox = lc_workflow_transaction_append_outbox_method;
  transaction->pub.commit = lc_workflow_transaction_commit_method;
  transaction->pub.rollback = lc_workflow_transaction_rollback_method;
  transaction->pub.close = lc_workflow_transaction_close_method;
  if (lc_workflow_transaction_add_lease(transaction, first, error) != LC_OK) { lc_client_free(workflow->client, transaction); lc_workflow_release(workflow); return NULL; }
  return &transaction->pub;
}

static int lc_workflow_append_outbox_method(lc_workflow *self, const lc_outbox_entry *entry, lc_source *payload, lc_workflow_transaction **out_txn, lc_outbox_receipt *receipt, lc_error *error) {
  lc_workflow_handle *workflow = (lc_workflow_handle *)self;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_workflow_transaction *transaction;
  char *key;
  int rc;
  if (workflow == NULL || out_txn == NULL || receipt == NULL) return lc_error_set(error, LC_ERR_INVALID, 0L, "workflow, transaction output, and receipt are required", NULL, NULL, NULL);
  *out_txn = NULL; lc_outbox_receipt_cleanup(receipt);
  key = NULL; rc = lc_workflow_outbox_key(workflow, entry, &key, error); if (rc != LC_OK) return rc;
  lc_acquire_req_init(&acquire); acquire.namespace_name = workflow->namespace_name; acquire.key = key; acquire.owner = workflow->owner; acquire.ttl_seconds = workflow->transaction_ttl_seconds; acquire.if_not_exists = 1;
  lease = NULL; rc = lc_acquire(&workflow->client->pub, &acquire, &lease, error);
  if (rc != LC_OK) { if (error != NULL) { lc_error_cleanup(error); lc_error_init(error); } rc = lc_workflow_existing_outbox(workflow, key, entry, receipt, error); free(key); return rc; }
  rc = lc_workflow_stage_outbox(lease, entry, payload, error);
  if (rc != LC_OK) { lc_release_req rollback; lc_release_req_init(&rollback); rollback.rollback = 1; (void)lc_lease_release(lease, &rollback, NULL); free(key); return rc; }
  transaction = lc_workflow_transaction_new(workflow, lease, error);
  if (transaction == NULL) { lc_lease_close(lease); free(key); return error != NULL ? error->code : LC_ERR_NOMEM; }
  receipt->outbox_key = key; receipt->effect_key = lc_strdup_local(entry->effect_key);
  if (receipt->effect_key == NULL) { transaction->close(transaction); lc_outbox_receipt_cleanup(receipt); return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to allocate outbox receipt", NULL, NULL, NULL); }
  *out_txn = transaction; return LC_OK;
}

static int lc_workflow_accept_inbox_method(lc_workflow *self, const lc_inbox_message *message, lc_workflow_transaction **out_txn, lc_inbox_accept_result *result, lc_error *error) {
  lc_workflow_handle *workflow = (lc_workflow_handle *)self;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_workflow_transaction *transaction;
  char *key;
  int rc;
  if (workflow == NULL || out_txn == NULL || result == NULL) return lc_error_set(error, LC_ERR_INVALID, 0L, "workflow, transaction output, and inbox result are required", NULL, NULL, NULL);
  *out_txn = NULL; memset(result, 0, sizeof(*result)); key = NULL;
  rc = lc_workflow_inbox_key(workflow, message, &key, error); if (rc != LC_OK) return rc;
  lc_acquire_req_init(&acquire); acquire.namespace_name = workflow->namespace_name; acquire.key = key; acquire.owner = workflow->owner; acquire.ttl_seconds = workflow->transaction_ttl_seconds; acquire.if_not_exists = 1;
  lease = NULL; rc = lc_acquire(&workflow->client->pub, &acquire, &lease, error);
  if (rc != LC_OK) { if (error != NULL) { lc_error_cleanup(error); lc_error_init(error); } rc = lc_workflow_existing_inbox(workflow, key, message, result, error); free(key); return rc; }
  free(key);
  rc = lc_workflow_stage_inbox(lease, message, error);
  if (rc != LC_OK) { lc_release_req rollback; lc_release_req_init(&rollback); rollback.rollback = 1; (void)lc_lease_release(lease, &rollback, NULL); return rc; }
  transaction = lc_workflow_transaction_new(workflow, lease, error);
  if (transaction == NULL) { lc_lease_close(lease); return error != NULL ? error->code : LC_ERR_NOMEM; }
  result->accepted = 1; *out_txn = transaction; return LC_OK;
}
static int lc_workflow_next_method(lc_workflow *self, long timeout_ms, lc_outbox_job **out, lc_error *error) {
  lc_workflow_handle *workflow = (lc_workflow_handle *)self;
  if (workflow == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "workflow and job output are required", NULL, NULL,
                        NULL);
  }
  return lc_workflow_wait_for_ready(workflow, timeout_ms, out, error);
}
static void lc_workflow_destroy(lc_workflow_handle *workflow) {
  lc_client_handle *client;
  size_t i;
  client = workflow->client;
  for (i = 0U; i < workflow->notification_count; ++i) lc_client_free(client, workflow->notifications[i]);
  lc_client_free(client, workflow->notifications);
  for (i = 0U; i < workflow->delayed_notification_count; ++i) {
    lc_client_free(client, workflow->delayed_notifications[i].key);
  }
  lc_client_free(client, workflow->delayed_notifications);
  lc_client_free(client, workflow->recovery_cursor);
  if (workflow->notification_cond_initialized) pthread_cond_destroy(&workflow->notification_cond);
  if (workflow->notification_mutex_initialized) pthread_mutex_destroy(&workflow->notification_mutex);
  lc_client_free(client, workflow->namespace_name);
  lc_client_free(client, workflow->owner);
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
  if (workflow->ref_count > 0U && --workflow->ref_count == 0U) destroy = 1;
  pthread_mutex_unlock(&workflow->notification_mutex);
  if (destroy) lc_workflow_destroy(workflow);
}

static void lc_workflow_close_method(lc_workflow *self) {
  lc_workflow_handle *workflow = (lc_workflow_handle *)self;

  if (workflow == NULL) return;
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
  pthread_mutex_unlock(&workflow->notification_mutex);
  if (workflow->dispatcher_started) {
    (void)pthread_join(workflow->dispatcher_thread, NULL);
    workflow->dispatcher_started = 0;
  }
  while (workflow->ready_head != NULL) {
    lc_outbox_job_handle *job = workflow->ready_head;
    workflow->ready_head = job->next;
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
  if (self == NULL || config == NULL || out == NULL ||
      config->namespace_name == NULL || config->namespace_name[0] == '\0')
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "new_workflow requires client, namespace config, and output",
                        NULL, NULL, NULL);
  client = (lc_client_handle *)self;
  lc_client_handle_retain(client);
  workflow = (lc_workflow_handle *)lc_client_calloc(client, 1U, sizeof(*workflow));
  if (workflow == NULL) { lc_client_close(&client->pub); return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to allocate workflow", NULL, NULL, NULL); }
  workflow->client = client;
  workflow->namespace_name = lc_client_strdup(client, config->namespace_name);
  workflow->owner = lc_client_strdup(client, config->owner != NULL && config->owner[0] != '\0' ? config->owner : "lockdc-workflow");
  if (workflow->namespace_name == NULL || workflow->owner == NULL) { lc_workflow_close_method(&workflow->pub); return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to copy workflow configuration", NULL, NULL, NULL); }
  workflow->transaction_ttl_seconds = config->transaction_ttl_seconds == 0L ? 30L : config->transaction_ttl_seconds;
  if (workflow->transaction_ttl_seconds < 1L) { lc_workflow_close_method(&workflow->pub); return lc_error_set(error, LC_ERR_INVALID, 0L, "workflow transaction ttl must be positive", NULL, NULL, NULL); }
  workflow->claim_ttl_seconds = config->claim_ttl_seconds == 0L ? 300L : config->claim_ttl_seconds;
  workflow->max_attempts = config->max_attempts == 0 ? 100 : config->max_attempts;
  workflow->retry_initial_delay_seconds =
      config->retry_initial_delay_seconds == 0L ? 1L
                                                : config->retry_initial_delay_seconds;
  workflow->retry_max_delay_seconds =
      config->retry_max_delay_seconds == 0L ? 900L
                                            : config->retry_max_delay_seconds;
  workflow->host_retry_delay_max_seconds =
      config->host_retry_delay_max_seconds == 0L
          ? 3600L
          : config->host_retry_delay_max_seconds;
  workflow->recovery_interval_seconds = config->recovery_interval_seconds;
  if (workflow->recovery_interval_seconds == 0L && !client->is_pouch) workflow->recovery_interval_seconds = 300L;
  if (workflow->claim_ttl_seconds < 1L || workflow->max_attempts < 1 ||
      workflow->retry_initial_delay_seconds < 1L ||
      workflow->retry_max_delay_seconds < workflow->retry_initial_delay_seconds ||
      workflow->host_retry_delay_max_seconds < 1L ||
      workflow->recovery_interval_seconds < 0L) { lc_workflow_close_method(&workflow->pub); return lc_error_set(error, LC_ERR_INVALID, 0L, "workflow claim, retry, and recovery configuration is invalid", NULL, NULL, NULL); }
  workflow->notification_capacity = config->notification_capacity == 0U ? 1024U : config->notification_capacity;
  workflow->notifications = (char **)lc_client_calloc(client, workflow->notification_capacity, sizeof(*workflow->notifications));
  workflow->delayed_notifications = (lc_workflow_delayed_notification *)lc_client_calloc(
      client, workflow->notification_capacity, sizeof(*workflow->delayed_notifications));
  if (workflow->notifications == NULL || workflow->delayed_notifications == NULL ||
      pthread_mutex_init(&workflow->notification_mutex, NULL) != 0) { lc_workflow_close_method(&workflow->pub); return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to initialize workflow dispatcher state", NULL, NULL, NULL); }
  workflow->notification_mutex_initialized = 1;
  workflow->ref_count = 1U;
  if (pthread_cond_init(&workflow->notification_cond, NULL) != 0) { lc_workflow_close_method(&workflow->pub); return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to initialize workflow dispatcher state", NULL, NULL, NULL); }
  workflow->notification_cond_initialized = 1;
  workflow->recovery_needed = 1;
  if (workflow->recovery_interval_seconds > 0L) {
    workflow->next_recovery_unix = (lc_unix_seconds)time(NULL) +
                                    workflow->recovery_interval_seconds;
  }
  workflow->pub.append_outbox = lc_workflow_append_outbox_method;
  workflow->pub.accept_inbox = lc_workflow_accept_inbox_method;
  workflow->pub.next = lc_workflow_next_method;
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

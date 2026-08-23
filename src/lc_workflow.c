#include "lc_api_internal.h"

#include <openssl/evp.h>

#include <stdio.h>

typedef struct lc_workflow_handle lc_workflow_handle;
typedef struct lc_workflow_transaction_handle lc_workflow_transaction_handle;
typedef struct lc_workflow_participant_handle lc_workflow_participant_handle;

struct lc_workflow_handle {
  lc_workflow pub;
  lc_client_handle *client;
  char *namespace_name;
  char *owner;
  long transaction_ttl_seconds;
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

static int lc_workflow_digest(const char *value, char out[65], lc_error *error) {
  EVP_MD_CTX *ctx;
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int length;
  static const char hex[] = "0123456789abcdef";
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
  for (i = 0U; i < 32U; ++i) {
    out[i * 2U] = hex[(digest[i] >> 4U) & 0x0fU];
    out[i * 2U + 1U] = hex[digest[i] & 0x0fU];
  }
  out[64] = '\0';
  return LC_OK;
}

static int lc_workflow_outbox_key(lc_workflow_handle *workflow,
                                  const lc_outbox_entry *entry, char **out,
                                  lc_error *error) {
  char operation[65];
  char effect[65];
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
  length = sizeof("__lockdc_io/v1/outbox//") - 1U + 64U + 64U + 1U;
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
  char identity[65];
  EVP_MD_CTX *ctx;
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int length;
  static const char hex[] = "0123456789abcdef";
  char *key;
  size_t i;
  size_t key_length;
  if (message == NULL || message->consumer_id == NULL || message->source_kind == NULL || message->source_id == NULL || message->message_id == NULL || message->consumer_id[0] == '\0' || message->source_kind[0] == '\0' || message->source_id[0] == '\0' || message->message_id[0] == '\0') return lc_error_set(error, LC_ERR_INVALID, 0L, "inbox consumer and source identity are required", NULL, NULL, NULL);
  ctx = EVP_MD_CTX_new(); length = 0U;
  if (ctx == NULL || EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) != 1 || EVP_DigestUpdate(ctx, message->consumer_id, strlen(message->consumer_id)) != 1 || EVP_DigestUpdate(ctx, "\n", 1U) != 1 || EVP_DigestUpdate(ctx, message->source_kind, strlen(message->source_kind)) != 1 || EVP_DigestUpdate(ctx, "\n", 1U) != 1 || EVP_DigestUpdate(ctx, message->source_id, strlen(message->source_id)) != 1 || EVP_DigestUpdate(ctx, "\n", 1U) != 1 || EVP_DigestUpdate(ctx, message->message_id, strlen(message->message_id)) != 1 || EVP_DigestFinal_ex(ctx, digest, &length) != 1 || length != 32U) { EVP_MD_CTX_free(ctx); return lc_error_set(error, LC_ERR_PROTOCOL, 0L, "failed to digest inbox identity", NULL, NULL, NULL); }
  EVP_MD_CTX_free(ctx);
  for (i = 0U; i < 32U; ++i) { identity[i * 2U] = hex[(digest[i] >> 4U) & 0x0fU]; identity[i * 2U + 1U] = hex[digest[i] & 0x0fU]; }
  identity[64] = '\0'; key_length = sizeof("__lockdc_io/v1/inbox/") - 1U + strlen(message->consumer_id) + 1U + 64U + 1U;
  key = (char *)malloc(key_length); if (key == NULL) return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to allocate inbox key", NULL, NULL, NULL);
  snprintf(key, key_length, "__lockdc_io/v1/inbox/%s/%s", message->consumer_id, identity);
  (void)workflow; *out = key; return LC_OK;
}

static int lc_workflow_stage_inbox(lc_lease *lease, const lc_inbox_message *message, lc_error *error) {
  char *json; size_t length; lc_source *source; int rc;
  length = strlen(message->consumer_id) + strlen(message->source_kind) + strlen(message->source_id) + strlen(message->message_id) + (message->payload_digest != NULL ? strlen(message->payload_digest) : 0U) + (message->operation_id != NULL ? strlen(message->operation_id) : 0U) + 160U;
  json = (char *)malloc(length); if (json == NULL) return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to allocate inbox receipt", NULL, NULL, NULL);
  snprintf(json, length, "{\"record_type\":\"lockdc.inbox.v1\",\"consumer_id\":\"%s\",\"source_kind\":\"%s\",\"source_id\":\"%s\",\"message_id\":\"%s\",\"payload_digest\":\"%s\",\"operation_id\":\"%s\",\"processing_state\":\"accepted\"}", message->consumer_id, message->source_kind, message->source_id, message->message_id, message->payload_digest != NULL ? message->payload_digest : "", message->operation_id != NULL ? message->operation_id : "");
  source = NULL; rc = lc_source_from_memory(json, strlen(json), &source, error); if (rc == LC_OK) rc = lc_lease_update(lease, source, NULL, error); lc_source_close(source); free(json); return rc;
}

static int lc_workflow_lease_json(lc_lease *lease, char **out, lc_error *error) {
  lc_sink *sink; const void *bytes; size_t length; char *json; lc_get_res result; int rc;
  sink = NULL; bytes = NULL; length = 0U; *out = NULL;
  memset(&result, 0, sizeof(result)); rc = lc_sink_to_memory(&sink, error); if (rc != LC_OK) return rc;
  rc = lc_lease_get(lease, sink, NULL, &result, error);
  if (rc == LC_OK) rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  if (rc == LC_OK) { json = (char *)malloc(length + 1U); if (json == NULL) rc = lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to copy workflow record", NULL, NULL, NULL); else { if (length > 0U) memcpy(json, bytes, length); json[length] = '\0'; *out = json; } }
  lc_get_res_cleanup(&result); lc_sink_close(sink); return rc;
}

static int lc_workflow_existing_outbox(lc_workflow_handle *workflow,
                                       const char *key,
                                       const lc_outbox_entry *entry,
                                       lc_outbox_receipt *receipt,
                                       lc_error *error) {
  lc_acquire_req acquire; lc_lease *lease; char *json; char *needle; size_t length; int rc;
  lc_acquire_req_init(&acquire); acquire.namespace_name = workflow->namespace_name; acquire.key = key; acquire.owner = workflow->owner; acquire.ttl_seconds = workflow->transaction_ttl_seconds;
  lease = NULL; rc = lc_acquire(&workflow->client->pub, &acquire, &lease, error); if (rc != LC_OK) return rc;
  json = NULL; rc = lc_workflow_lease_json(lease, &json, error);
  if (rc == LC_OK) { length = strlen(entry->operation_id) + strlen(entry->effect_id) + strlen(entry->effect_key) + strlen(entry->kind) + strlen(entry->destination) + 100U; needle = (char *)malloc(length); if (needle == NULL) rc = lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to validate outbox duplicate", NULL, NULL, NULL); else { snprintf(needle, length, "\"operation_id\":\"%s\"", entry->operation_id); if (strstr(json, needle) == NULL) rc = lc_error_set(error, LC_ERR_SERVER, 0L, "outbox identity conflicts with an existing record", NULL, NULL, NULL); snprintf(needle, length, "\"effect_id\":\"%s\"", entry->effect_id); if (rc == LC_OK && strstr(json, needle) == NULL) rc = lc_error_set(error, LC_ERR_SERVER, 0L, "outbox identity conflicts with an existing record", NULL, NULL, NULL); snprintf(needle, length, "\"effect_key\":\"%s\"", entry->effect_key); if (rc == LC_OK && strstr(json, needle) == NULL) rc = lc_error_set(error, LC_ERR_SERVER, 0L, "outbox immutable fields conflict with an existing record", NULL, NULL, NULL); free(needle); } }
  free(json); { lc_release_req rollback; lc_release_req_init(&rollback); rollback.rollback = 1; if (lc_lease_release(lease, &rollback, error) != LC_OK && rc == LC_OK) rc = error != NULL ? error->code : LC_ERR_SERVER; }
  if (rc == LC_OK) { receipt->outbox_key = lc_strdup_local(key); receipt->effect_key = lc_strdup_local(entry->effect_key); if (receipt->outbox_key == NULL || receipt->effect_key == NULL) { lc_outbox_receipt_cleanup(receipt); return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to allocate duplicate outbox receipt", NULL, NULL, NULL); } receipt->duplicate = 1; }
  return rc;
}

static int lc_workflow_existing_inbox(lc_workflow_handle *workflow,
                                      const char *key,
                                      const lc_inbox_message *message,
                                      lc_inbox_accept_result *result,
                                      lc_error *error) {
  lc_acquire_req acquire; lc_lease *lease; char *json; char *needle; size_t length; int rc;
  lc_acquire_req_init(&acquire); acquire.namespace_name = workflow->namespace_name; acquire.key = key; acquire.owner = workflow->owner; acquire.ttl_seconds = workflow->transaction_ttl_seconds;
  lease = NULL; rc = lc_acquire(&workflow->client->pub, &acquire, &lease, error); if (rc != LC_OK) return rc;
  json = NULL; rc = lc_workflow_lease_json(lease, &json, error);
  if (rc == LC_OK) { length = strlen(message->consumer_id) + strlen(message->source_kind) + strlen(message->source_id) + strlen(message->message_id) + (message->payload_digest != NULL ? strlen(message->payload_digest) : 0U) + 64U; needle = (char *)malloc(length); if (needle == NULL) rc = lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to validate inbox duplicate", NULL, NULL, NULL); else { snprintf(needle, length, "\"consumer_id\":\"%s\"", message->consumer_id); if (strstr(json, needle) == NULL) rc = lc_error_set(error, LC_ERR_SERVER, 0L, "inbox identity conflicts with an existing record", NULL, NULL, NULL); snprintf(needle, length, "\"source_kind\":\"%s\"", message->source_kind); if (rc == LC_OK && strstr(json, needle) == NULL) rc = lc_error_set(error, LC_ERR_SERVER, 0L, "inbox identity conflicts with an existing record", NULL, NULL, NULL); snprintf(needle, length, "\"source_id\":\"%s\"", message->source_id); if (rc == LC_OK && strstr(json, needle) == NULL) rc = lc_error_set(error, LC_ERR_SERVER, 0L, "inbox identity conflicts with an existing record", NULL, NULL, NULL); snprintf(needle, length, "\"message_id\":\"%s\"", message->message_id); if (rc == LC_OK && strstr(json, needle) == NULL) rc = lc_error_set(error, LC_ERR_SERVER, 0L, "inbox identity conflicts with an existing record", NULL, NULL, NULL); if (rc == LC_OK && message->payload_digest != NULL) { snprintf(needle, length, "\"payload_digest\":\"%s\"", message->payload_digest); if (strstr(json, needle) == NULL) rc = lc_error_set(error, LC_ERR_SERVER, 0L, "inbox payload digest conflicts with an existing record", NULL, NULL, NULL); } free(needle); } }
  free(json); { lc_release_req rollback; lc_release_req_init(&rollback); rollback.rollback = 1; if (lc_lease_release(lease, &rollback, error) != LC_OK && rc == LC_OK) rc = error != NULL ? error->code : LC_ERR_SERVER; }
  if (rc == LC_OK) result->duplicate = 1;
  return rc;
}

static int lc_workflow_stage_outbox(lc_lease *lease,
                                    const lc_outbox_entry *entry,
                                    lc_source *payload, lc_error *error) {
  char *json;
  size_t length;
  lc_source *state;
  lc_attach_req attach;
  lc_attach_res attach_result;
  int rc;

  if (payload == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "outbox payload source is required", NULL, NULL,
                        NULL);
  }
  length = strlen(entry->operation_id) + strlen(entry->effect_id) +
           strlen(entry->effect_key) + strlen(entry->kind) +
           strlen(entry->destination) + 160U;
  json = (char *)malloc(length);
  if (json == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate outbox envelope", NULL, NULL,
                        NULL);
  }
  snprintf(json, length,
           "{\"record_type\":\"lockdc.outbox.v1\",\"operation_id\":\"%s\",\"effect_id\":\"%s\",\"effect_key\":\"%s\",\"kind\":\"%s\",\"destination\":\"%s\",\"dispatch_state\":\"pending\",\"attempt_count\":0}",
           entry->operation_id, entry->effect_id, entry->effect_key,
           entry->kind, entry->destination);
  state = NULL;
  rc = lc_source_from_memory(json, strlen(json), &state, error);
  if (rc == LC_OK) {
    rc = lc_lease_update(lease, state, NULL, error);
  }
  lc_source_close(state);
  free(json);
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
static int lc_workflow_participant_metadata(lc_workflow_participant *self, const lc_metadata_req *req, lc_error *error) { lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self; int rc; if (p == NULL || p->lease == NULL) return lc_error_set(error, LC_ERR_INVALID, 0L, "workflow participant is closed", NULL, NULL, NULL); rc = lc_lease_metadata(p->lease, req, error); lc_workflow_participant_refresh(p); return rc; }
static int lc_workflow_participant_remove(lc_workflow_participant *self, const lc_remove_req *req, lc_error *error) { lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self; int rc; if (p == NULL || p->lease == NULL) return lc_error_set(error, LC_ERR_INVALID, 0L, "workflow participant is closed", NULL, NULL, NULL); rc = lc_lease_remove(p->lease, req, error); lc_workflow_participant_refresh(p); return rc; }
static int lc_workflow_participant_keepalive(lc_workflow_participant *self, const lc_keepalive_req *req, lc_error *error) { lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self; int rc; if (p == NULL || p->lease == NULL) return lc_error_set(error, LC_ERR_INVALID, 0L, "workflow participant is closed", NULL, NULL, NULL); rc = lc_lease_keepalive(p->lease, req, error); lc_workflow_participant_refresh(p); return rc; }
static int lc_workflow_participant_attach(lc_workflow_participant *self, const lc_attach_req *req, lc_source *src, lc_attach_res *out, lc_error *error) { lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self; if (p == NULL || p->lease == NULL) return lc_error_set(error, LC_ERR_INVALID, 0L, "workflow participant is closed", NULL, NULL, NULL); return lc_lease_attach(p->lease, req, src, out, error); }
static int lc_workflow_participant_get_attachment(lc_workflow_participant *self, const lc_attachment_get_req *req, lc_sink *dst, lc_attachment_get_res *out, lc_error *error) { lc_workflow_participant_handle *p = (lc_workflow_participant_handle *)self; if (p == NULL || p->lease == NULL) return lc_error_set(error, LC_ERR_INVALID, 0L, "workflow participant is closed", NULL, NULL, NULL); return lc_lease_get_attachment(p->lease, req, dst, out, error); }
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
  participant->pub.metadata = lc_workflow_participant_metadata;
  participant->pub.remove = lc_workflow_participant_remove;
  participant->pub.keepalive = lc_workflow_participant_keepalive;
  participant->pub.attach = lc_workflow_participant_attach;
  participant->pub.get_attachment = lc_workflow_participant_get_attachment;
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
  size_t i;
  int rc;
  if (transaction == NULL || transaction->terminal) return lc_error_set(error, LC_ERR_INVALID, 0L, "workflow transaction is closed", NULL, NULL, NULL);
  lc_release_req_init(&request); request.rollback = rollback;
  for (i = 0U; i < transaction->lease_count; ++i) {
    if (transaction->leases[i] == NULL) continue;
    rc = lc_lease_release(transaction->leases[i], &request, error);
    if (rc != LC_OK) return rc;
    transaction->leases[i] = NULL;
  }
  transaction->terminal = 1;
  return LC_OK;
}
static int lc_workflow_transaction_commit_method(lc_workflow_transaction *self, lc_error *error) { return lc_workflow_transaction_terminal(self, 0, error); }
static int lc_workflow_transaction_rollback_method(lc_workflow_transaction *self, lc_error *error) { return lc_workflow_transaction_terminal(self, 1, error); }
static void lc_workflow_transaction_close_method(lc_workflow_transaction *self) { lc_workflow_transaction_handle *transaction = (lc_workflow_transaction_handle *)self; size_t i; if (transaction == NULL) return; if (!transaction->terminal) (void)lc_workflow_transaction_terminal(self, 1, NULL); for (i = 0U; i < transaction->lease_count; ++i) lc_lease_close(transaction->leases[i]); lc_client_free(transaction->workflow->client, transaction->leases); lc_client_free(transaction->workflow->client, transaction); }

static lc_workflow_transaction *lc_workflow_transaction_new(lc_workflow_handle *workflow, lc_lease *first, lc_error *error) {
  lc_workflow_transaction_handle *transaction;
  transaction = (lc_workflow_transaction_handle *)lc_client_calloc(workflow->client, 1U, sizeof(*transaction));
  if (transaction == NULL) { lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to allocate workflow transaction", NULL, NULL, NULL); return NULL; }
  transaction->workflow = workflow;
  transaction->pub.acquire = lc_workflow_transaction_acquire_method;
  transaction->pub.append_outbox = lc_workflow_transaction_append_outbox_method;
  transaction->pub.commit = lc_workflow_transaction_commit_method;
  transaction->pub.rollback = lc_workflow_transaction_rollback_method;
  transaction->pub.close = lc_workflow_transaction_close_method;
  if (lc_workflow_transaction_add_lease(transaction, first, error) != LC_OK) { lc_client_free(workflow->client, transaction); return NULL; }
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
  (void)self; (void)timeout_ms; if (out != NULL) *out = NULL;
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "workflow dispatcher is not implemented", NULL, NULL,
                      NULL);
}
static void lc_workflow_close_method(lc_workflow *self) {
  lc_workflow_handle *workflow = (lc_workflow_handle *)self;
  lc_client_handle *client;
  if (workflow == NULL) return;
  client = workflow->client;
  lc_client_free(client, workflow->namespace_name);
  lc_client_free(client, workflow->owner);
  lc_client_free(client, workflow);
  lc_client_close(&client->pub);
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
  workflow->pub.append_outbox = lc_workflow_append_outbox_method;
  workflow->pub.accept_inbox = lc_workflow_accept_inbox_method;
  workflow->pub.next = lc_workflow_next_method;
  workflow->pub.close = lc_workflow_close_method;
  *out = &workflow->pub;
  return LC_OK;
}

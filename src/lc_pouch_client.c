#include "lc_api_internal.h"
#include "lc_internal.h"
#include "lc_mutate_stream.h"

#include <lql/lql.h>

#include <errno.h>
#include <stdint.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define LC_POUCH_RESERVED_BACKEND_NAMESPACE ".lockd"
#define LC_POUCH_RESERVED_TRANSACTION_NAMESPACE ".lockd-txn"
#define LC_POUCH_TXN_CONTENT_TYPE "application/x-lockd-pouch-txn"
#define LC_POUCH_TXN_RECORD_HEADER_SIZE 36U
#define LC_POUCH_TXN_STATE_PREPARED 1UL
#define LC_POUCH_TXN_STATE_COMMITTED 2UL
#define LC_POUCH_TXN_STATE_ROLLED_BACK 3UL

static const char *lc_pouch_default_namespace(lc_client_handle *client,
                                              const char *namespace_name) {
  if (namespace_name != NULL && namespace_name[0] != '\0') {
    return namespace_name;
  }
  if (client->default_namespace != NULL &&
      client->default_namespace[0] != '\0') {
    return client->default_namespace;
  }
  return "default";
}

static int lc_pouch_public_namespace(lc_client_handle *client,
                                     const char *namespace_name,
                                     const char **out, lc_error *error) {
  const char *resolved;

  if (client == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace resolution requires client and out",
                        NULL, NULL, NULL);
  }
  resolved = lc_pouch_default_namespace(client, namespace_name);
  if (strcmp(resolved, LC_POUCH_RESERVED_BACKEND_NAMESPACE) == 0 ||
      strcmp(resolved, LC_POUCH_RESERVED_TRANSACTION_NAMESPACE) == 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace is reserved for internal storage",
                        resolved, "reserved_namespace", NULL);
  }
  *out = resolved;
  return LC_OK;
}

static long lc_pouch_now_unix(void) { return (long)time(NULL); }

int lc_pouch_lease_update_method(lc_lease *self, lc_source *src,
                                 const lc_update_opts *opts, lc_error *error);
int lc_pouch_lease_get_method(lc_lease *self, lc_sink *dst,
                              const lc_get_opts *opts, lc_get_res *out,
                              lc_error *error);
static int lc_pouch_lease_staged_update_method(lc_lease *self, lc_source *src,
                                               const lc_update_opts *opts,
                                               lc_error *error);
static int lc_pouch_lease_staged_remove_method(lc_lease *self,
                                               const lc_remove_req *opts,
                                               lc_error *error);
static int lc_pouch_lease_mutate_staged_method(lc_lease *self,
                                               const lc_mutate_req *req,
                                               lc_error *error);
static int lc_pouch_lease_staged_attach(lc_lease_handle *lease,
                                        const lc_attach_req *req,
                                        lc_source *src, lc_attach_res *out,
                                        lc_error *error);
static int lc_pouch_lease_staged_list_attachments(lc_lease_handle *lease,
                                                  lc_attachment_list *out,
                                                  lc_error *error);
static int lc_pouch_lease_staged_get_attachment(
    lc_lease_handle *lease, const lc_attachment_get_req *req, lc_sink *dst,
    lc_attachment_get_res *out, lc_error *error);
static int
lc_pouch_lease_staged_delete_attachment(lc_lease_handle *lease,
                                        const lc_attachment_selector *selector,
                                        int *deleted, lc_error *error);
static int lc_pouch_lease_staged_delete_all_attachments(lc_lease_handle *lease,
                                                        int *deleted_count,
                                                        lc_error *error);
static int lc_pouch_refresh_lease(lc_lease_handle *lease,
                                  const lc_pouch_meta *meta, lc_error *error);
static int lc_pouch_repair_meta_state_gap(
    lc_client_handle *client, const char *namespace_name, const char *key,
    lc_pouch_meta_record *record, const lc_pouch_state_info *known_state,
    lc_error *error);
static int lc_pouch_set_lease_state(lc_lease_handle *lease,
                                    const char *state_etag, long version,
                                    lc_error *error);
static int lc_pouch_validate_active_lease(lc_client_handle *client,
                                          const lc_lease_ref *ref,
                                          lc_pouch_meta_record *record,
                                          lc_error *error);
static int64_t lc_pouch_now_millis(void);
static void lc_pouch_sleep_millis(long millis);

typedef struct lc_pouch_acquire_for_update_file_sink {
  FILE *fp;
} lc_pouch_acquire_for_update_file_sink;

typedef struct lc_pouch_file_sink {
  FILE *fp;
  const char *label;
} lc_pouch_file_sink;

typedef struct lc_pouch_txn_record {
  char *txn_id;
  unsigned long state;
  long expires_at_unix;
  unsigned long tc_term;
  lc_txn_participant *participants;
  size_t participant_count;
} lc_pouch_txn_record;

typedef struct lc_pouch_txn_recovery_list {
  lc_client_handle *client;
  char **txn_ids;
  size_t count;
  size_t capacity;
} lc_pouch_txn_recovery_list;

typedef struct lc_pouch_txn_abandoned_cleanup_context {
  lc_client_handle *client;
  const lc_pouch_txn_recovery_list *protected_txns;
  const char *namespace_name;
  long now_unix;
} lc_pouch_txn_abandoned_cleanup_context;

static int lc_pouch_file_sink_write(lc_sink *self, const void *bytes,
                                    size_t count, lc_error *error) {
  lc_pouch_file_sink *sink;
  const char *label;

  sink = self != NULL ? (lc_pouch_file_sink *)self->impl : NULL;
  label = sink != NULL && sink->label != NULL ? sink->label : "pouch file";
  if (sink == NULL || sink->fp == NULL) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "pouch file sink is closed", label, NULL, NULL);
  }
  if (count > 0U && fwrite(bytes, 1U, count, sink->fp) != count) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to write pouch file sink", label, NULL, NULL);
  }
  return 1;
}

static void lc_pouch_file_sink_close(lc_sink *self) { (void)self; }

static void lc_pouch_file_sink_init(lc_sink *sink, lc_pouch_file_sink *impl,
                                    FILE *fp, const char *label) {
  impl->fp = fp;
  impl->label = label;
  sink->write = lc_pouch_file_sink_write;
  sink->close = lc_pouch_file_sink_close;
  sink->impl = impl;
}

static int lc_pouch_acquire_for_update_sink_write(lc_sink *self,
                                                  const void *bytes,
                                                  size_t count,
                                                  lc_error *error) {
  lc_pouch_acquire_for_update_file_sink *sink;

  sink = (lc_pouch_acquire_for_update_file_sink *)self->impl;
  if (sink == NULL || sink->fp == NULL) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "pouch acquire_for_update sink is closed", NULL, NULL,
                        NULL);
  }
  if (count > 0U && fwrite(bytes, 1U, count, sink->fp) != count) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to write pouch acquire_for_update snapshot",
                        strerror(errno), NULL, NULL);
  }
  return 1;
}

static void lc_pouch_acquire_for_update_sink_close(lc_sink *self) {
  (void)self;
}

static char *lc_pouch_new_lease_id(lc_client_handle *client, const char *key,
                                   long fencing_token) {
  char stack[160];

  snprintf(stack, sizeof(stack), "pouch-%ld-%ld-%s", (long)time(NULL),
           fencing_token, key != NULL ? key : "lease");
  return lc_client_strdup(client, stack);
}

static char *lc_pouch_new_txn_id(lc_client_handle *client, const char *key,
                                 long fencing_token) {
  char stack[160];

  (void)key;
  snprintf(stack, sizeof(stack), "pouch-txn-%ld-%ld", (long)time(NULL),
           fencing_token);
  return lc_client_strdup(client, stack);
}

static int lc_pouch_txn_id_is_generated(const char *txn_id) {
  const char prefix[] = "pouch-txn-";

  return txn_id != NULL && strncmp(txn_id, prefix, sizeof(prefix) - 1U) == 0;
}

static int lc_pouch_lease_id_is_generated(const char *lease_id) {
  const char prefix[] = "pouch-";

  return lease_id != NULL &&
         strncmp(lease_id, prefix, sizeof(prefix) - 1U) == 0;
}

static int lc_pouch_lease_ref_is_explicit_txn(const lc_lease_ref *ref) {
  return ref != NULL && ref->txn_id != NULL && ref->txn_id[0] != '\0' &&
         !lc_pouch_txn_id_is_generated(ref->txn_id) &&
         lc_pouch_lease_id_is_generated(ref->lease_id);
}

static int lc_pouch_stack_lease_from_ref(lc_client_handle *client,
                                         const lc_lease_ref *ref,
                                         lc_lease_handle *lease,
                                         lc_pouch_meta_record *record,
                                         lc_error *error) {
  int rc;

  if (client == NULL || ref == NULL || lease == NULL || record == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction lease view requires client, ref, "
                        "lease, and record",
                        NULL, NULL, NULL);
  }
  memset(lease, 0, sizeof(*lease));
  memset(record, 0, sizeof(*record));
  rc = lc_pouch_validate_active_lease(client, ref, record, error);
  if (rc != LC_OK) {
    return rc;
  }
  lease->client = client;
  lease->namespace_name = record->namespace_name;
  lease->key = (char *)ref->key;
  lease->lease_id = (char *)ref->lease_id;
  lease->txn_id = (char *)ref->txn_id;
  lease->fencing_token = ref->fencing_token;
  lease->version = record->meta.version;
  lease->state_etag = record->meta.state_etag;
  lease->lease_expires_at_unix = record->meta.lease_expires_at_unix;
  lease->has_query_hidden = record->meta.has_query_hidden;
  lease->query_hidden = record->meta.query_hidden;
  lease->pouch_txn_explicit = 1;
  return LC_OK;
}

static char *lc_pouch_queue_state_key(lc_client_handle *client,
                                      const char *queue,
                                      const char *message_id) {
  size_t queue_len;
  size_t message_len;
  size_t total;
  char *key;

  if (client == NULL || queue == NULL || message_id == NULL) {
    return NULL;
  }
  queue_len = strlen(queue);
  message_len = strlen(message_id);
  total = 2U + queue_len + 7U + message_len + 1U;
  key = (char *)lc_client_alloc(client, total);
  if (key == NULL) {
    return NULL;
  }
  snprintf(key, total, "q/%s/state/%s", queue, message_id);
  return key;
}

static int lc_pouch_copy_public(char **dst, const char *src, lc_error *error,
                                const char *message) {
  *dst = lc_strdup_local(src);
  if (src != NULL && *dst == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L, message, NULL, NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_copy_client(lc_client_handle *client, char **dst,
                                const char *src, lc_error *error,
                                const char *message) {
  *dst = lc_client_strdup(client, src);
  if (src != NULL && *dst == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L, message, NULL, NULL, NULL);
  }
  return LC_OK;
}

static void lc_pouch_txn_put_u32(unsigned char *dst, unsigned long value) {
  dst[0] = (unsigned char)(value & 0xffUL);
  dst[1] = (unsigned char)((value >> 8) & 0xffUL);
  dst[2] = (unsigned char)((value >> 16) & 0xffUL);
  dst[3] = (unsigned char)((value >> 24) & 0xffUL);
}

static void lc_pouch_txn_put_u64(unsigned char *dst, unsigned long value) {
  lc_pouch_txn_put_u32(dst, value & 0xffffffffUL);
#if ULONG_MAX > 0xffffffffUL
  lc_pouch_txn_put_u32(dst + 4, (value >> 32) & 0xffffffffUL);
#else
  lc_pouch_txn_put_u32(dst + 4, 0UL);
#endif
}

static unsigned long lc_pouch_txn_get_u32(const unsigned char *src) {
  return ((unsigned long)src[0]) | (((unsigned long)src[1]) << 8) |
         (((unsigned long)src[2]) << 16) | (((unsigned long)src[3]) << 24);
}

static unsigned long lc_pouch_txn_get_u64(const unsigned char *src) {
#if ULONG_MAX > 0xffffffffUL
  return lc_pouch_txn_get_u32(src) | (lc_pouch_txn_get_u32(src + 4) << 32);
#else
  return lc_pouch_txn_get_u32(src);
#endif
}

static const char *lc_pouch_txn_state_name(unsigned long state) {
  if (state == LC_POUCH_TXN_STATE_PREPARED) {
    return "prepared";
  }
  if (state == LC_POUCH_TXN_STATE_COMMITTED) {
    return "committed";
  }
  if (state == LC_POUCH_TXN_STATE_ROLLED_BACK) {
    return "rolled_back";
  }
  return "unknown";
}

static void lc_pouch_txn_record_cleanup(lc_client_handle *client,
                                        lc_pouch_txn_record *record) {
  size_t index;

  if (record == NULL) {
    return;
  }
  lc_client_free(client, record->txn_id);
  for (index = 0U; index < record->participant_count; ++index) {
    lc_client_free(client, (char *)record->participants[index].namespace_name);
    lc_client_free(client, (char *)record->participants[index].key);
    lc_client_free(client, (char *)record->participants[index].backend_hash);
  }
  lc_client_free(client, record->participants);
  memset(record, 0, sizeof(*record));
}

static int lc_pouch_txn_copy_slice(lc_client_handle *client, char **out,
                                   const unsigned char *src, size_t length,
                                   lc_error *error, const char *message) {
  char *copy;

  copy = (char *)lc_client_alloc(client, length + 1U);
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L, message, NULL, NULL, NULL);
  }
  if (length > 0U) {
    memcpy(copy, src, length);
  }
  copy[length] = '\0';
  *out = copy;
  return LC_OK;
}

static int lc_pouch_txn_read_source_all(lc_client_handle *client,
                                        lc_source *source, unsigned char **out,
                                        size_t *out_length, lc_error *error) {
  unsigned char *buffer;
  unsigned char temp[4096];
  size_t capacity;
  size_t length;
  size_t got;

  buffer = NULL;
  capacity = 0U;
  length = 0U;
  while (1) {
    got = source->read(source, temp, sizeof(temp), error);
    if (got == 0U) {
      break;
    }
    if (got > ((size_t)-1) - length) {
      lc_client_free(client, buffer);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch transaction record is too large", NULL, NULL,
                          NULL);
    }
    if (length + got > capacity) {
      size_t new_capacity;
      unsigned char *grown;

      new_capacity = capacity == 0U ? 4096U : capacity;
      while (new_capacity < length + got) {
        if (new_capacity > ((size_t)-1) / 2U) {
          lc_client_free(client, buffer);
          return lc_error_set(error, LC_ERR_INVALID, 0L,
                              "pouch transaction record is too large", NULL,
                              NULL, NULL);
        }
        new_capacity *= 2U;
      }
      grown = (unsigned char *)lc_client_realloc(client, buffer, new_capacity);
      if (grown == NULL) {
        lc_client_free(client, buffer);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch transaction record", NULL,
                            NULL, NULL);
      }
      buffer = grown;
      capacity = new_capacity;
    }
    memcpy(buffer + length, temp, got);
    length += got;
  }
  *out = buffer;
  *out_length = length;
  return LC_OK;
}

static int lc_pouch_txn_validate_decision_req(lc_client_handle *client,
                                              const lc_txn_decision_req *req,
                                              lc_error *error) {
  char *backend_hash;
  size_t index;
  int rc;

  if (client == NULL || req == NULL || req->txn_id == NULL ||
      req->txn_id[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction decision requires txn_id", NULL,
                        NULL, NULL);
  }
  if (req->participant_count > 0U && req->participants == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction decision requires txn_id and "
                        "participants",
                        NULL, NULL, NULL);
  }
  backend_hash = NULL;
  if (req->target_backend_hash != NULL && req->target_backend_hash[0] != '\0') {
    rc = client->pouch_store->backend_hash(client->pouch_store, &backend_hash,
                                           error);
    if (rc != LC_OK) {
      return rc;
    }
    if (strcmp(req->target_backend_hash, backend_hash) != 0) {
      lc_pouch_free(&client->pouch_allocator, backend_hash);
      return lc_error_set(error, LC_ERR_SERVER, 409L,
                          "pouch transaction targets another backend", NULL,
                          "backend_mismatch", NULL);
    }
    lc_pouch_free(&client->pouch_allocator, backend_hash);
  }
  for (index = 0U; index < req->participant_count; ++index) {
    const char *namespace_name;

    if (req->participants[index].key == NULL ||
        req->participants[index].key[0] == '\0') {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch transaction participant requires key", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_public_namespace(client,
                                   req->participants[index].namespace_name,
                                   &namespace_name, error);
    if (rc != LC_OK) {
      return rc;
    }
    (void)namespace_name;
  }
  return LC_OK;
}

static int lc_pouch_txn_validate_pending_participants(
    lc_client_handle *client, const lc_txn_decision_req *req, lc_error *error) {
  size_t index;
  int rc;

  for (index = 0U; index < req->participant_count; ++index) {
    const char *namespace_name;
    lc_pouch_meta_record record;

    namespace_name = NULL;
    memset(&record, 0, sizeof(record));
    rc = lc_pouch_public_namespace(client,
                                   req->participants[index].namespace_name,
                                   &namespace_name, error);
    if (rc == LC_OK) {
      rc = client->pouch_store->load_meta(client->pouch_store, namespace_name,
                                          req->participants[index].key, &record,
                                          error);
    }
    if (rc == LC_OK && (!record.found || record.meta.txn_id == NULL ||
                        strcmp(record.meta.txn_id, req->txn_id) != 0)) {
      rc = lc_error_set(error, LC_ERR_SERVER, 409L,
                        "pouch transaction participant is not pending", NULL,
                        "txn_not_pending", NULL);
    }
    lc_pouch_meta_record_cleanup(&client->pouch_allocator, &record);
    if (rc != LC_OK) {
      return rc;
    }
  }
  return LC_OK;
}

static int lc_pouch_txn_encode(lc_client_handle *client,
                               const lc_txn_decision_req *req,
                               unsigned long state, unsigned char **out,
                               size_t *out_length, lc_error *error) {
  size_t length;
  size_t index;
  unsigned char *buffer;
  unsigned char *cursor;

  length = LC_POUCH_TXN_RECORD_HEADER_SIZE + strlen(req->txn_id);
  for (index = 0U; index < req->participant_count; ++index) {
    const char *namespace_name;
    const char *backend_hash;

    namespace_name = lc_pouch_default_namespace(
        client, req->participants[index].namespace_name);
    backend_hash = req->participants[index].backend_hash;
    if (strlen(namespace_name) > 0xffffffffUL ||
        strlen(req->participants[index].key) > 0xffffffffUL ||
        (backend_hash != NULL && strlen(backend_hash) > 0xffffffffUL)) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch transaction participant is too large", NULL,
                          NULL, NULL);
    }
    length +=
        12U + strlen(namespace_name) + strlen(req->participants[index].key);
    if (backend_hash != NULL) {
      length += strlen(backend_hash);
    }
  }
  buffer = (unsigned char *)lc_client_alloc(client, length);
  if (buffer == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch transaction record", NULL,
                        NULL, NULL);
  }
  memcpy(buffer, "LCTX", 4U);
  lc_pouch_txn_put_u32(buffer + 4, 1UL);
  lc_pouch_txn_put_u32(buffer + 8, state);
  lc_pouch_txn_put_u64(buffer + 12, (unsigned long)req->expires_at_unix);
  lc_pouch_txn_put_u64(buffer + 20, req->tc_term);
  lc_pouch_txn_put_u32(buffer + 28, (unsigned long)req->participant_count);
  lc_pouch_txn_put_u32(buffer + 32, (unsigned long)strlen(req->txn_id));
  cursor = buffer + LC_POUCH_TXN_RECORD_HEADER_SIZE;
  memcpy(cursor, req->txn_id, strlen(req->txn_id));
  cursor += strlen(req->txn_id);
  for (index = 0U; index < req->participant_count; ++index) {
    const char *namespace_name;
    const char *key;
    const char *backend_hash;
    size_t ns_len;
    size_t key_len;
    size_t hash_len;

    namespace_name = lc_pouch_default_namespace(
        client, req->participants[index].namespace_name);
    key = req->participants[index].key;
    backend_hash = req->participants[index].backend_hash;
    ns_len = strlen(namespace_name);
    key_len = strlen(key);
    hash_len = backend_hash != NULL ? strlen(backend_hash) : 0U;
    lc_pouch_txn_put_u32(cursor, (unsigned long)ns_len);
    lc_pouch_txn_put_u32(cursor + 4, (unsigned long)key_len);
    lc_pouch_txn_put_u32(cursor + 8, (unsigned long)hash_len);
    cursor += 12U;
    memcpy(cursor, namespace_name, ns_len);
    cursor += ns_len;
    memcpy(cursor, key, key_len);
    cursor += key_len;
    if (hash_len > 0U) {
      memcpy(cursor, backend_hash, hash_len);
      cursor += hash_len;
    }
  }
  *out = buffer;
  *out_length = length;
  return LC_OK;
}

static int lc_pouch_txn_decode(lc_client_handle *client,
                               const unsigned char *bytes, size_t length,
                               lc_pouch_txn_record *out, lc_error *error) {
  const unsigned char *cursor;
  size_t remaining;
  unsigned long version;
  unsigned long count;
  unsigned long txn_len;
  size_t index;

  memset(out, 0, sizeof(*out));
  if (length < LC_POUCH_TXN_RECORD_HEADER_SIZE ||
      memcmp(bytes, "LCTX", 4U) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "invalid pouch transaction record", NULL, NULL, NULL);
  }
  version = lc_pouch_txn_get_u32(bytes + 4);
  if (version != 1UL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "unsupported pouch transaction record version", NULL,
                        NULL, NULL);
  }
  out->state = lc_pouch_txn_get_u32(bytes + 8);
  out->expires_at_unix = (long)lc_pouch_txn_get_u64(bytes + 12);
  out->tc_term = lc_pouch_txn_get_u64(bytes + 20);
  count = lc_pouch_txn_get_u32(bytes + 28);
  txn_len = lc_pouch_txn_get_u32(bytes + 32);
  cursor = bytes + LC_POUCH_TXN_RECORD_HEADER_SIZE;
  remaining = length - LC_POUCH_TXN_RECORD_HEADER_SIZE;
  if ((size_t)txn_len > remaining) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "truncated pouch transaction id", NULL, NULL, NULL);
  }
  if (lc_pouch_txn_copy_slice(
          client, &out->txn_id, cursor, (size_t)txn_len, error,
          "failed to allocate pouch transaction id") != LC_OK) {
    return error != NULL ? error->code : LC_ERR_NOMEM;
  }
  cursor += txn_len;
  remaining -= (size_t)txn_len;
  if (count > ((size_t)-1) / sizeof(out->participants[0])) {
    lc_pouch_txn_record_cleanup(client, out);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "too many pouch transaction participants", NULL, NULL,
                        NULL);
  }
  out->participants = (lc_txn_participant *)lc_client_calloc(
      client, (size_t)count, sizeof(out->participants[0]));
  if (count > 0UL && out->participants == NULL) {
    lc_pouch_txn_record_cleanup(client, out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch transaction participants",
                        NULL, NULL, NULL);
  }
  out->participant_count = (size_t)count;
  for (index = 0U; index < out->participant_count; ++index) {
    unsigned long ns_len;
    unsigned long key_len;
    unsigned long hash_len;

    if (remaining < 12U) {
      lc_pouch_txn_record_cleanup(client, out);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "truncated pouch transaction participant", NULL, NULL,
                          NULL);
    }
    ns_len = lc_pouch_txn_get_u32(cursor);
    key_len = lc_pouch_txn_get_u32(cursor + 4);
    hash_len = lc_pouch_txn_get_u32(cursor + 8);
    cursor += 12U;
    remaining -= 12U;
    if ((size_t)ns_len > remaining ||
        (size_t)key_len > remaining - (size_t)ns_len ||
        (size_t)hash_len > remaining - (size_t)ns_len - (size_t)key_len) {
      lc_pouch_txn_record_cleanup(client, out);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "truncated pouch transaction participant", NULL, NULL,
                          NULL);
    }
    if (lc_pouch_txn_copy_slice(
            client, (char **)&out->participants[index].namespace_name, cursor,
            (size_t)ns_len, error,
            "failed to allocate pouch transaction namespace") != LC_OK) {
      lc_pouch_txn_record_cleanup(client, out);
      return error != NULL ? error->code : LC_ERR_NOMEM;
    }
    cursor += ns_len;
    remaining -= (size_t)ns_len;
    if (lc_pouch_txn_copy_slice(client, (char **)&out->participants[index].key,
                                cursor, (size_t)key_len, error,
                                "failed to allocate pouch transaction key") !=
        LC_OK) {
      lc_pouch_txn_record_cleanup(client, out);
      return error != NULL ? error->code : LC_ERR_NOMEM;
    }
    cursor += key_len;
    remaining -= (size_t)key_len;
    if (hash_len > 0UL &&
        lc_pouch_txn_copy_slice(
            client, (char **)&out->participants[index].backend_hash, cursor,
            (size_t)hash_len, error,
            "failed to allocate pouch transaction backend hash") != LC_OK) {
      lc_pouch_txn_record_cleanup(client, out);
      return error != NULL ? error->code : LC_ERR_NOMEM;
    }
    cursor += hash_len;
    remaining -= (size_t)hash_len;
  }
  if (remaining != 0U) {
    lc_pouch_txn_record_cleanup(client, out);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction record has trailing bytes", NULL,
                        NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_txn_store_record(lc_client_handle *client,
                                     const lc_txn_decision_req *req,
                                     unsigned long state, lc_error *error) {
  unsigned char *record;
  size_t record_len;
  lc_source *source;
  lc_pouch_put_object_opts opts;
  lc_pouch_object_info info;
  int rc;

  record = NULL;
  record_len = 0U;
  source = NULL;
  memset(&opts, 0, sizeof(opts));
  memset(&info, 0, sizeof(info));
  rc = lc_pouch_txn_encode(client, req, state, &record, &record_len, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_source_from_memory(record, record_len, &source, error);
  lc_client_free(client, record);
  if (rc != LC_OK) {
    return rc;
  }
  opts.name = "decision";
  opts.content_type = LC_POUCH_TXN_CONTENT_TYPE;
  rc = client->pouch_store->put_object(
      client->pouch_store, LC_POUCH_RESERVED_TRANSACTION_NAMESPACE, req->txn_id,
      source, &opts, &info, error);
  lc_source_close(source);
  lc_pouch_object_info_cleanup(&client->pouch_allocator, &info);
  return rc;
}

static int lc_pouch_txn_load_record(lc_client_handle *client,
                                    const char *txn_id,
                                    lc_pouch_txn_record *out, lc_error *error) {
  lc_pouch_object_selector selector;
  lc_pouch_object_info info;
  lc_source *source;
  unsigned char *payload;
  size_t payload_len;
  int rc;

  memset(&selector, 0, sizeof(selector));
  memset(&info, 0, sizeof(info));
  memset(out, 0, sizeof(*out));
  source = NULL;
  payload = NULL;
  payload_len = 0U;
  selector.name = "decision";
  rc = client->pouch_store->get_object(
      client->pouch_store, LC_POUCH_RESERVED_TRANSACTION_NAMESPACE, txn_id,
      &selector, &source, &info, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_txn_read_source_all(client, source, &payload, &payload_len,
                                    error);
  lc_source_close(source);
  lc_pouch_object_info_cleanup(&client->pouch_allocator, &info);
  if (rc == LC_OK) {
    rc = lc_pouch_txn_decode(client, payload, payload_len, out, error);
  }
  lc_client_free(client, payload);
  return rc;
}

static int lc_pouch_txn_delete_record(lc_client_handle *client,
                                      const char *txn_id, lc_error *error) {
  lc_pouch_object_selector selector;
  int deleted;

  memset(&selector, 0, sizeof(selector));
  selector.name = "decision";
  deleted = 0;
  return client->pouch_store->delete_object(
      client->pouch_store, LC_POUCH_RESERVED_TRANSACTION_NAMESPACE, txn_id,
      &selector, &deleted, error);
}

static char *lc_pouch_txn_attachment_stage_key(lc_client_handle *client,
                                               const char *key,
                                               const char *txn_id,
                                               lc_error *error) {
  const char mid[] = "/.staging/";
  const char suffix[] = "/attachments";
  size_t key_len;
  size_t txn_len;
  size_t mid_len;
  size_t suffix_len;
  size_t total;
  char *staged_key;

  if (client == NULL || key == NULL || txn_id == NULL || txn_id[0] == '\0') {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch staged attachment requires key and txn_id", NULL, NULL,
                 NULL);
    return NULL;
  }
  key_len = strlen(key);
  txn_len = strlen(txn_id);
  mid_len = strlen(mid);
  suffix_len = strlen(suffix);
  if (key_len > (size_t)-1 - mid_len ||
      key_len + mid_len > (size_t)-1 - txn_len ||
      key_len + mid_len + txn_len > (size_t)-1 - suffix_len ||
      key_len + mid_len + txn_len + suffix_len > (size_t)-1 - 1U) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch staged attachment key is too large", NULL, NULL, NULL);
    return NULL;
  }
  total = key_len + mid_len + txn_len + suffix_len;
  staged_key = (char *)lc_client_calloc(client, total + 1U, 1U);
  if (staged_key == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch staged attachment key", NULL, NULL,
                 NULL);
    return NULL;
  }
  memcpy(staged_key, key, key_len);
  memcpy(staged_key + key_len, mid, mid_len);
  memcpy(staged_key + key_len + mid_len, txn_id, txn_len);
  memcpy(staged_key + key_len + mid_len + txn_len, suffix, suffix_len);
  staged_key[total] = '\0';
  return staged_key;
}

static char *lc_pouch_txn_attachment_ops_key(lc_client_handle *client,
                                             const char *key,
                                             const char *txn_id,
                                             lc_error *error) {
  const char mid[] = "/.staging/";
  const char suffix[] = "/attachment-ops";
  size_t key_len;
  size_t txn_len;
  size_t mid_len;
  size_t suffix_len;
  size_t total;
  char *ops_key;

  if (client == NULL || key == NULL || txn_id == NULL || txn_id[0] == '\0') {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch staged attachment operation requires key and txn_id",
                 NULL, NULL, NULL);
    return NULL;
  }
  key_len = strlen(key);
  txn_len = strlen(txn_id);
  mid_len = strlen(mid);
  suffix_len = strlen(suffix);
  if (key_len > (size_t)-1 - mid_len ||
      key_len + mid_len > (size_t)-1 - txn_len ||
      key_len + mid_len + txn_len > (size_t)-1 - suffix_len ||
      key_len + mid_len + txn_len + suffix_len > (size_t)-1 - 1U) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch staged attachment operation key is too large", NULL,
                 NULL, NULL);
    return NULL;
  }
  total = key_len + mid_len + txn_len + suffix_len;
  ops_key = (char *)lc_client_calloc(client, total + 1U, 1U);
  if (ops_key == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch staged attachment operation key",
                 NULL, NULL, NULL);
    return NULL;
  }
  memcpy(ops_key, key, key_len);
  memcpy(ops_key + key_len, mid, mid_len);
  memcpy(ops_key + key_len + mid_len, txn_id, txn_len);
  memcpy(ops_key + key_len + mid_len + txn_len, suffix, suffix_len);
  ops_key[total] = '\0';
  return ops_key;
}

static char *lc_pouch_txn_attachment_op_name(lc_client_handle *client,
                                             const char *prefix,
                                             const char *value,
                                             lc_error *error) {
  size_t prefix_len;
  size_t value_len;
  size_t total;
  char *name;

  if (client == NULL || prefix == NULL || value == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch staged attachment operation requires name value", NULL,
                 NULL, NULL);
    return NULL;
  }
  prefix_len = strlen(prefix);
  value_len = strlen(value);
  if (prefix_len > (size_t)-1 - value_len ||
      prefix_len + value_len > (size_t)-1 - 1U) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch staged attachment operation name is too large", NULL,
                 NULL, NULL);
    return NULL;
  }
  total = prefix_len + value_len;
  name = (char *)lc_client_calloc(client, total + 1U, 1U);
  if (name == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch staged attachment operation name",
                 NULL, NULL, NULL);
    return NULL;
  }
  memcpy(name, prefix, prefix_len);
  memcpy(name + prefix_len, value, value_len);
  name[total] = '\0';
  return name;
}

#define LC_POUCH_TXN_ATTACHMENT_OP_CLEAR "\001pouch-attachment-clear"
#define LC_POUCH_TXN_ATTACHMENT_OP_DELETE_ID "\001pouch-attachment-delete-id:"
#define LC_POUCH_TXN_ATTACHMENT_OP_DELETE_NAME                                 \
  "\001pouch-attachment-delete-name:"

static int lc_pouch_txn_stage_attachment_op(lc_client_handle *client,
                                            const char *namespace_name,
                                            const char *key, const char *txn_id,
                                            const char *op_name,
                                            lc_error *error) {
  lc_pouch_put_object_opts opts;
  lc_pouch_object_info object;
  lc_source *source;
  char *ops_key;
  int rc;

  if (client == NULL || client->pouch_store == NULL ||
      client->pouch_store->put_object == NULL) {
    return LC_OK;
  }
  memset(&opts, 0, sizeof(opts));
  memset(&object, 0, sizeof(object));
  source = NULL;
  ops_key = lc_pouch_txn_attachment_ops_key(client, key, txn_id, error);
  if (ops_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_source_from_memory("", 0U, &source, error);
  if (rc == LC_OK) {
    opts.name = op_name;
    opts.content_type = "application/x-lockd-pouch-attachment-op";
    rc =
        client->pouch_store->put_object(client->pouch_store, namespace_name,
                                        ops_key, source, &opts, &object, error);
  }
  if (source != NULL) {
    source->close(source);
  }
  lc_pouch_object_info_cleanup(&client->pouch_allocator, &object);
  lc_client_free(client, ops_key);
  return rc;
}

static int lc_pouch_txn_discard_staged_attachments(lc_client_handle *client,
                                                   const char *namespace_name,
                                                   const char *key,
                                                   const char *txn_id,
                                                   lc_error *error) {
  char *staged_key;
  char *ops_key;
  int deleted_count;
  int rc;

  if (client == NULL || client->pouch_store == NULL ||
      client->pouch_store->delete_all_objects == NULL) {
    return LC_OK;
  }
  staged_key = lc_pouch_txn_attachment_stage_key(client, key, txn_id, error);
  if (staged_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  deleted_count = 0;
  rc = client->pouch_store->delete_all_objects(
      client->pouch_store, namespace_name, staged_key, &deleted_count, error);
  lc_client_free(client, staged_key);
  if (rc != LC_OK) {
    return rc;
  }
  ops_key = lc_pouch_txn_attachment_ops_key(client, key, txn_id, error);
  if (ops_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  deleted_count = 0;
  rc = client->pouch_store->delete_all_objects(
      client->pouch_store, namespace_name, ops_key, &deleted_count, error);
  lc_client_free(client, ops_key);
  return rc;
}

static int lc_pouch_txn_promote_staged_attachments(
    lc_client_handle *client, const char *namespace_name, const char *key,
    const char *txn_id, int *promoted, lc_error *error) {
  lc_pouch_object_list objects;
  lc_pouch_object_list ops;
  lc_pouch_copy_object_opts copy_opts;
  lc_pouch_object_info copied;
  lc_pouch_object_selector selector;
  char *staged_key;
  char *ops_key;
  size_t index;
  int deleted_count;
  int deleted;
  int saw_clear;
  int rc;
  size_t delete_id_prefix_len;
  size_t delete_name_prefix_len;

  if (promoted != NULL) {
    *promoted = 0;
  }
  if (client == NULL || client->pouch_store == NULL ||
      client->pouch_store->list_objects == NULL ||
      client->pouch_store->copy_object == NULL ||
      client->pouch_store->delete_all_objects == NULL) {
    return LC_OK;
  }
  memset(&objects, 0, sizeof(objects));
  memset(&ops, 0, sizeof(ops));
  memset(&selector, 0, sizeof(selector));
  delete_id_prefix_len = strlen(LC_POUCH_TXN_ATTACHMENT_OP_DELETE_ID);
  delete_name_prefix_len = strlen(LC_POUCH_TXN_ATTACHMENT_OP_DELETE_NAME);
  staged_key = lc_pouch_txn_attachment_stage_key(client, key, txn_id, error);
  if (staged_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  ops_key = lc_pouch_txn_attachment_ops_key(client, key, txn_id, error);
  if (ops_key == NULL) {
    lc_client_free(client, staged_key);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }

  rc = client->pouch_store->list_objects(client->pouch_store, namespace_name,
                                         ops_key, &ops, error);
  if (rc != LC_OK) {
    lc_client_free(client, ops_key);
    lc_client_free(client, staged_key);
    return rc;
  }
  saw_clear = 0;
  for (index = 0U; index < ops.count; ++index) {
    if (ops.items[index].name != NULL &&
        strcmp(ops.items[index].name, LC_POUCH_TXN_ATTACHMENT_OP_CLEAR) == 0) {
      saw_clear = 1;
      break;
    }
  }
  if (saw_clear) {
    deleted_count = 0;
    rc = client->pouch_store->delete_all_objects(
        client->pouch_store, namespace_name, key, &deleted_count, error);
    if (rc != LC_OK) {
      lc_pouch_object_list_cleanup(&client->pouch_allocator, &ops);
      lc_client_free(client, ops_key);
      lc_client_free(client, staged_key);
      return rc;
    }
    if (deleted_count > 0 && promoted != NULL) {
      *promoted = 1;
    }
  }
  for (index = 0U; index < ops.count; ++index) {
    if (ops.items[index].name == NULL) {
      continue;
    }
    memset(&selector, 0, sizeof(selector));
    if (strncmp(ops.items[index].name, LC_POUCH_TXN_ATTACHMENT_OP_DELETE_ID,
                delete_id_prefix_len) == 0) {
      selector.id = ops.items[index].name + delete_id_prefix_len;
    } else if (strncmp(ops.items[index].name,
                       LC_POUCH_TXN_ATTACHMENT_OP_DELETE_NAME,
                       delete_name_prefix_len) == 0) {
      selector.name = ops.items[index].name + delete_name_prefix_len;
    } else {
      continue;
    }
    deleted = 0;
    rc = client->pouch_store->delete_object(client->pouch_store, namespace_name,
                                            key, &selector, &deleted, error);
    if (rc != LC_OK) {
      lc_pouch_object_list_cleanup(&client->pouch_allocator, &ops);
      lc_client_free(client, ops_key);
      lc_client_free(client, staged_key);
      return rc;
    }
    if (deleted && promoted != NULL) {
      *promoted = 1;
    }
  }
  lc_pouch_object_list_cleanup(&client->pouch_allocator, &ops);

  rc = client->pouch_store->list_objects(client->pouch_store, namespace_name,
                                         staged_key, &objects, error);
  if (rc != LC_OK) {
    lc_client_free(client, ops_key);
    lc_client_free(client, staged_key);
    return rc;
  }
  for (index = 0U; index < objects.count; ++index) {
    memset(&copy_opts, 0, sizeof(copy_opts));
    memset(&copied, 0, sizeof(copied));
    copy_opts.source.id = objects.items[index].id;
    copy_opts.source.name = objects.items[index].name;
    copy_opts.name = objects.items[index].name;
    rc = client->pouch_store->copy_object(client->pouch_store, namespace_name,
                                          staged_key, key, &copy_opts, &copied,
                                          error);
    lc_pouch_object_info_cleanup(&client->pouch_allocator, &copied);
    if (rc != LC_OK) {
      lc_pouch_object_list_cleanup(&client->pouch_allocator, &objects);
      lc_client_free(client, ops_key);
      lc_client_free(client, staged_key);
      return rc;
    }
  }
  if (objects.count > 0U && promoted != NULL) {
    *promoted = 1;
  }
  lc_pouch_object_list_cleanup(&client->pouch_allocator, &objects);
  deleted_count = 0;
  rc = client->pouch_store->delete_all_objects(
      client->pouch_store, namespace_name, staged_key, &deleted_count, error);
  if (rc == LC_OK) {
    deleted_count = 0;
    rc = client->pouch_store->delete_all_objects(
        client->pouch_store, namespace_name, ops_key, &deleted_count, error);
  }
  lc_client_free(client, ops_key);
  lc_client_free(client, staged_key);
  return rc;
}

static int lc_pouch_txn_clear_participant_meta(
    lc_client_handle *client, const char *namespace_name,
    const lc_txn_participant *participant, const lc_pouch_meta_record *record,
    int update_state_etag, const char *state_etag, long state_version,
    lc_error *error) {
  lc_pouch_store_meta_res stored;
  lc_pouch_meta next_meta;
  int rc;

  memset(&stored, 0, sizeof(stored));
  next_meta = record->meta;
  next_meta.owner = NULL;
  next_meta.lease_id = NULL;
  next_meta.txn_id = NULL;
  next_meta.lease_expires_at_unix = 0L;
  if (update_state_etag) {
    next_meta.state_etag = (char *)state_etag;
  }
  if (state_version > 0L) {
    next_meta.version = state_version;
  }
  rc = client->pouch_store->store_meta(client->pouch_store, namespace_name,
                                       participant->key, &next_meta,
                                       record->etag, &stored, error);
  lc_pouch_store_meta_res_cleanup(&client->pouch_allocator, &stored);
  return rc;
}

static int lc_pouch_txn_finish_already_promoted_commit(
    lc_client_handle *client, const char *namespace_name,
    const lc_txn_participant *participant, const lc_pouch_meta_record *record,
    lc_error *error) {
  lc_pouch_state_info state;
  lc_source *body;
  int rc;

  memset(&state, 0, sizeof(state));
  body = NULL;
  rc = client->pouch_store->read_state(client->pouch_store, namespace_name,
                                       participant->key, &body, &state, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (body != NULL) {
    body->close(body);
  }
  if (state.no_content) {
    if (record->meta.state_etag == NULL ||
        state.version <= record->meta.version) {
      lc_pouch_state_info_cleanup(&client->pouch_allocator, &state);
      return lc_error_set(error, LC_ERR_SERVER, 404L,
                          "pouch staged state was not found", NULL, "not_found",
                          NULL);
    }
    rc = lc_pouch_txn_clear_participant_meta(client, namespace_name,
                                             participant, record, 1, NULL,
                                             state.version, error);
    lc_pouch_state_info_cleanup(&client->pouch_allocator, &state);
    return rc;
  }
  if (record->meta.state_etag == NULL || state.etag == NULL ||
      strcmp(record->meta.state_etag, state.etag) == 0) {
    lc_pouch_state_info_cleanup(&client->pouch_allocator, &state);
    return lc_error_set(error, LC_ERR_SERVER, 404L,
                        "pouch staged state was not found", NULL, "not_found",
                        NULL);
  }
  rc = lc_pouch_txn_clear_participant_meta(client, namespace_name, participant,
                                           record, 1, state.etag, state.version,
                                           error);
  lc_pouch_state_info_cleanup(&client->pouch_allocator, &state);
  return rc;
}

static int lc_pouch_txn_apply_participant(lc_client_handle *client,
                                          const char *txn_id,
                                          const lc_txn_participant *participant,
                                          unsigned long state,
                                          int allow_already_applied,
                                          lc_error *error) {
  const char *namespace_name;
  lc_pouch_meta_record record;
  lc_pouch_put_state_res promoted;
  lc_pouch_promote_staged_opts promote_opts;
  lc_pouch_discard_staged_opts discard_opts;
  int state_promoted;
  int attachments_promoted;
  int rc;

  memset(&record, 0, sizeof(record));
  memset(&promoted, 0, sizeof(promoted));
  memset(&promote_opts, 0, sizeof(promote_opts));
  memset(&discard_opts, 0, sizeof(discard_opts));
  state_promoted = 0;
  attachments_promoted = 0;
  rc = lc_pouch_public_namespace(client, participant->namespace_name,
                                 &namespace_name, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = client->pouch_store->load_meta(client->pouch_store, namespace_name,
                                      participant->key, &record, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!record.found || record.meta.txn_id == NULL ||
      strcmp(record.meta.txn_id, txn_id) != 0) {
    lc_pouch_meta_record_cleanup(&client->pouch_allocator, &record);
    if (allow_already_applied) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_SERVER, 409L,
                        "pouch transaction participant is not pending", NULL,
                        "txn_not_pending", NULL);
  }
  if (state == LC_POUCH_TXN_STATE_COMMITTED) {
    promote_opts.expected_head_etag = record.meta.state_etag;
    rc = client->pouch_store->promote_staged_state(
        client->pouch_store, namespace_name, participant->key, txn_id,
        &promote_opts, &promoted, error);
    if (rc == LC_ERR_SERVER && allow_already_applied && error != NULL &&
        error->http_status == 404L) {
      lc_error_cleanup(error);
      rc = lc_pouch_txn_finish_already_promoted_commit(
          client, namespace_name, participant, &record, error);
      lc_pouch_put_state_res_cleanup(&client->pouch_allocator, &promoted);
      lc_pouch_meta_record_cleanup(&client->pouch_allocator, &record);
      return rc;
    } else if (rc == LC_ERR_SERVER && error != NULL &&
               error->http_status == 404L) {
      lc_error_cleanup(error);
      rc = LC_OK;
    } else if (rc == LC_OK) {
      state_promoted = 1;
    }
    if (rc == LC_OK) {
      rc = lc_pouch_txn_promote_staged_attachments(
          client, namespace_name, participant->key, txn_id,
          &attachments_promoted, error);
    }
    if (rc == LC_OK && state_promoted) {
      rc = lc_pouch_txn_clear_participant_meta(
          client, namespace_name, participant, &record, 1,
          promoted.new_state_etag, promoted.new_version, error);
    } else if (rc == LC_OK) {
      rc = lc_pouch_txn_clear_participant_meta(
          client, namespace_name, participant, &record, 0, NULL,
          attachments_promoted ? record.meta.version + 1L : 0L, error);
    }
  } else if (state == LC_POUCH_TXN_STATE_ROLLED_BACK) {
    discard_opts.ignore_not_found = 1;
    rc = client->pouch_store->discard_staged_state(
        client->pouch_store, namespace_name, participant->key, txn_id,
        &discard_opts, error);
    if (rc == LC_OK) {
      rc = lc_pouch_txn_discard_staged_attachments(
          client, namespace_name, participant->key, txn_id, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_txn_clear_participant_meta(
          client, namespace_name, participant, &record, 0, NULL, 0L, error);
    }
  } else {
    rc = LC_OK;
  }
  lc_pouch_put_state_res_cleanup(&client->pouch_allocator, &promoted);
  lc_pouch_meta_record_cleanup(&client->pouch_allocator, &record);
  return rc;
}

static int lc_pouch_txn_apply_record(lc_client_handle *client,
                                     const lc_pouch_txn_record *record,
                                     int allow_already_applied,
                                     lc_error *error) {
  size_t index;
  int rc;

  if (record->state == LC_POUCH_TXN_STATE_PREPARED) {
    if (record->expires_at_unix > 0L &&
        record->expires_at_unix <= lc_pouch_now_unix()) {
      for (index = 0U; index < record->participant_count; ++index) {
        rc = lc_pouch_txn_apply_participant(
            client, record->txn_id, &record->participants[index],
            LC_POUCH_TXN_STATE_ROLLED_BACK, allow_already_applied, error);
        if (rc != LC_OK) {
          return rc;
        }
      }
      if (client->pouch_store->apply_queue_txn != NULL) {
        rc = client->pouch_store->apply_queue_txn(client->pouch_store,
                                                  record->txn_id, 0, error);
        if (rc != LC_OK) {
          return rc;
        }
      }
      return lc_pouch_txn_delete_record(client, record->txn_id, error);
    }
    return LC_OK;
  }
  if (record->state != LC_POUCH_TXN_STATE_COMMITTED &&
      record->state != LC_POUCH_TXN_STATE_ROLLED_BACK) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "unknown pouch transaction decision state", NULL, NULL,
                        NULL);
  }
  for (index = 0U; index < record->participant_count; ++index) {
    rc = lc_pouch_txn_apply_participant(
        client, record->txn_id, &record->participants[index], record->state,
        allow_already_applied, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  if (client->pouch_store->apply_queue_txn != NULL) {
    rc = client->pouch_store->apply_queue_txn(
        client->pouch_store, record->txn_id,
        record->state == LC_POUCH_TXN_STATE_COMMITTED, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  return lc_pouch_txn_delete_record(client, record->txn_id, error);
}

static void
lc_pouch_txn_recovery_list_cleanup(lc_pouch_txn_recovery_list *list) {
  size_t index;

  if (list == NULL) {
    return;
  }
  for (index = 0U; index < list->count; ++index) {
    lc_client_free(list->client, list->txn_ids[index]);
  }
  lc_client_free(list->client, list->txn_ids);
  memset(list, 0, sizeof(*list));
}

static int
lc_pouch_txn_recovery_list_contains(const lc_pouch_txn_recovery_list *list,
                                    const char *txn_id) {
  size_t index;

  for (index = 0U; index < list->count; ++index) {
    if (strcmp(list->txn_ids[index], txn_id) == 0) {
      return 1;
    }
  }
  return 0;
}

static int lc_pouch_txn_recovery_list_add(lc_pouch_txn_recovery_list *list,
                                          const char *txn_id, lc_error *error) {
  char **grown;
  char *copy;
  size_t new_capacity;

  if (txn_id == NULL || txn_id[0] == '\0' ||
      lc_pouch_txn_recovery_list_contains(list, txn_id)) {
    return LC_OK;
  }
  if (list->count == list->capacity) {
    new_capacity = list->capacity == 0U ? 16U : list->capacity * 2U;
    grown = (char **)lc_client_realloc(list->client, list->txn_ids,
                                       new_capacity * sizeof(list->txn_ids[0]));
    if (grown == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch transaction recovery list",
                          NULL, NULL, NULL);
    }
    list->txn_ids = grown;
    list->capacity = new_capacity;
  }
  copy = lc_client_strdup(list->client, txn_id);
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch transaction id", NULL, NULL,
                        NULL);
  }
  list->txn_ids[list->count++] = copy;
  return LC_OK;
}

static int lc_pouch_txn_recovery_visit(void *context,
                                       const lc_pouch_scan_meta_row *row,
                                       lc_error *error) {
  lc_pouch_txn_recovery_list *list;

  list = (lc_pouch_txn_recovery_list *)context;
  if (row != NULL && row->meta != NULL && row->meta->txn_id != NULL) {
    return lc_pouch_txn_recovery_list_add(list, row->meta->txn_id, error);
  }
  return LC_OK;
}

static int lc_pouch_txn_recovery_key_visit(void *context, const char *key,
                                           lc_error *error) {
  lc_pouch_txn_recovery_list *list;

  list = (lc_pouch_txn_recovery_list *)context;
  return lc_pouch_txn_recovery_list_add(list, key, error);
}

static int
lc_pouch_txn_collect_decision_objects(lc_client_handle *client,
                                      lc_pouch_txn_recovery_list *list,
                                      lc_error *error) {
  lc_pouch_scan_object_keys_req req;
  lc_pouch_scan_object_keys_res scan;
  char *cursor;
  char *next_cursor;
  int rc;

  if (client->pouch_store->scan_object_keys == NULL) {
    return LC_OK;
  }

  cursor = NULL;
  do {
    memset(&req, 0, sizeof(req));
    memset(&scan, 0, sizeof(scan));
    req.namespace_name = LC_POUCH_RESERVED_TRANSACTION_NAMESPACE;
    req.name = "decision";
    req.start_after = cursor;
    req.limit = 128U;
    next_cursor = NULL;
    rc = client->pouch_store->scan_object_keys(client->pouch_store, &req,
                                               lc_pouch_txn_recovery_key_visit,
                                               list, &scan, error);
    if (rc != LC_OK) {
      lc_client_free(client, cursor);
      return rc;
    }
    if (scan.truncated && scan.next_start_after != NULL) {
      next_cursor = lc_client_strdup(client, scan.next_start_after);
      if (next_cursor == NULL) {
        lc_pouch_scan_object_keys_res_cleanup(&client->pouch_allocator, &scan);
        lc_client_free(client, cursor);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to copy pouch transaction object cursor",
                            NULL, NULL, NULL);
      }
    }
    lc_pouch_scan_object_keys_res_cleanup(&client->pouch_allocator, &scan);
    lc_client_free(client, cursor);
    cursor = next_cursor;
  } while (cursor != NULL);
  return LC_OK;
}

static int lc_pouch_txn_collect_namespace(lc_client_handle *client,
                                          const char *namespace_name,
                                          lc_pouch_txn_recovery_list *list,
                                          lc_error *error) {
  lc_pouch_scan_meta_req req;
  lc_pouch_scan_meta_res scan;
  char *cursor;
  char *next_cursor;
  int rc;

  cursor = NULL;
  do {
    memset(&req, 0, sizeof(req));
    memset(&scan, 0, sizeof(scan));
    req.namespace_name = namespace_name;
    req.start_after = cursor;
    req.limit = 128U;
    req.include_hidden = 1;
    next_cursor = NULL;
    rc = client->pouch_store->scan_meta(client->pouch_store, &req,
                                        lc_pouch_txn_recovery_visit, list,
                                        &scan, error);
    if (rc != LC_OK) {
      lc_client_free(client, cursor);
      return rc;
    }
    if (scan.truncated && scan.next_start_after != NULL) {
      next_cursor = lc_client_strdup(client, scan.next_start_after);
      if (next_cursor == NULL) {
        lc_pouch_scan_meta_res_cleanup(&client->pouch_allocator, &scan);
        lc_client_free(client, cursor);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to copy pouch transaction recovery cursor",
                            NULL, NULL, NULL);
      }
    }
    lc_pouch_scan_meta_res_cleanup(&client->pouch_allocator, &scan);
    lc_client_free(client, cursor);
    cursor = next_cursor;
  } while (cursor != NULL);
  return LC_OK;
}

static int lc_pouch_txn_abandoned_cleanup_visit(
    void *context, const lc_pouch_scan_meta_row *row, lc_error *error) {
  lc_pouch_txn_abandoned_cleanup_context *cleanup;
  lc_txn_participant participant;

  cleanup = (lc_pouch_txn_abandoned_cleanup_context *)context;
  if (cleanup == NULL || row == NULL || row->key == NULL || row->meta == NULL ||
      row->meta->txn_id == NULL || row->meta->txn_id[0] == '\0') {
    return LC_OK;
  }
  if (lc_pouch_txn_recovery_list_contains(cleanup->protected_txns,
                                          row->meta->txn_id) ||
      row->meta->lease_expires_at_unix > cleanup->now_unix) {
    return LC_OK;
  }

  memset(&participant, 0, sizeof(participant));
  participant.namespace_name = cleanup->namespace_name;
  participant.key = row->key;
  return lc_pouch_txn_apply_participant(
      cleanup->client, row->meta->txn_id, &participant,
      LC_POUCH_TXN_STATE_ROLLED_BACK, 1, error);
}

static int lc_pouch_txn_cleanup_abandoned_namespace(
    lc_client_handle *client, const char *namespace_name,
    const lc_pouch_txn_recovery_list *protected_txns, lc_error *error) {
  lc_pouch_txn_abandoned_cleanup_context cleanup;
  lc_pouch_scan_meta_req req;
  lc_pouch_scan_meta_res scan;
  char *cursor;
  char *next_cursor;
  int rc;

  memset(&cleanup, 0, sizeof(cleanup));
  cleanup.client = client;
  cleanup.protected_txns = protected_txns;
  cleanup.namespace_name = namespace_name;
  cleanup.now_unix = lc_pouch_now_unix();

  cursor = NULL;
  do {
    memset(&req, 0, sizeof(req));
    memset(&scan, 0, sizeof(scan));
    req.namespace_name = namespace_name;
    req.start_after = cursor;
    req.limit = 128U;
    req.include_hidden = 1;
    next_cursor = NULL;
    rc = client->pouch_store->scan_meta(client->pouch_store, &req,
                                        lc_pouch_txn_abandoned_cleanup_visit,
                                        &cleanup, &scan, error);
    if (rc != LC_OK) {
      lc_client_free(client, cursor);
      return rc;
    }
    if (scan.truncated && scan.next_start_after != NULL) {
      next_cursor = lc_client_strdup(client, scan.next_start_after);
      if (next_cursor == NULL) {
        lc_pouch_scan_meta_res_cleanup(&client->pouch_allocator, &scan);
        lc_client_free(client, cursor);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to copy pouch abandoned txn cursor", NULL,
                            NULL, NULL);
      }
    }
    lc_pouch_scan_meta_res_cleanup(&client->pouch_allocator, &scan);
    lc_client_free(client, cursor);
    cursor = next_cursor;
  } while (cursor != NULL);
  return LC_OK;
}

int lc_pouch_client_recover_transactions(lc_client *self, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_namespace_list namespaces;
  lc_pouch_txn_recovery_list protected_txns;
  lc_pouch_txn_recovery_list replay_txns;
  lc_txn_replay_req req;
  lc_txn_replay_res res;
  size_t index;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch transaction recovery requires client", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  memset(&namespaces, 0, sizeof(namespaces));
  memset(&protected_txns, 0, sizeof(protected_txns));
  memset(&replay_txns, 0, sizeof(replay_txns));
  protected_txns.client = client;
  replay_txns.client = client;
  rc = lc_pouch_txn_collect_decision_objects(client, &protected_txns, error);
  if (rc != LC_OK) {
    lc_pouch_txn_recovery_list_cleanup(&protected_txns);
    return rc;
  }
  for (index = 0U; index < protected_txns.count; ++index) {
    rc = lc_pouch_txn_recovery_list_add(&replay_txns,
                                        protected_txns.txn_ids[index], error);
    if (rc != LC_OK) {
      lc_pouch_txn_recovery_list_cleanup(&protected_txns);
      lc_pouch_txn_recovery_list_cleanup(&replay_txns);
      return rc;
    }
  }
  rc = client->pouch_store->list_namespaces(client->pouch_store, &namespaces,
                                            error);
  if (rc != LC_OK) {
    lc_pouch_txn_recovery_list_cleanup(&protected_txns);
    lc_pouch_txn_recovery_list_cleanup(&replay_txns);
    return rc;
  }
  for (index = 0U; index < namespaces.count; ++index) {
    rc = lc_pouch_txn_collect_namespace(client, namespaces.names[index],
                                        &replay_txns, error);
    if (rc != LC_OK) {
      lc_pouch_namespace_list_cleanup(&client->pouch_allocator, &namespaces);
      lc_pouch_txn_recovery_list_cleanup(&protected_txns);
      lc_pouch_txn_recovery_list_cleanup(&replay_txns);
      return rc;
    }
  }

  for (index = 0U; index < replay_txns.count; ++index) {
    lc_txn_replay_req_init(&req);
    memset(&res, 0, sizeof(res));
    req.txn_id = replay_txns.txn_ids[index];
    rc = lc_pouch_client_txn_replay_method(self, &req, &res, error);
    lc_txn_replay_res_cleanup(&res);
    if (rc == LC_ERR_SERVER && error != NULL && error->http_status == 404L) {
      lc_error_cleanup(error);
      continue;
    }
    if (rc != LC_OK) {
      lc_pouch_namespace_list_cleanup(&client->pouch_allocator, &namespaces);
      lc_pouch_txn_recovery_list_cleanup(&protected_txns);
      lc_pouch_txn_recovery_list_cleanup(&replay_txns);
      return rc;
    }
    rc = lc_pouch_txn_recovery_list_add(&protected_txns,
                                        replay_txns.txn_ids[index], error);
    if (rc != LC_OK) {
      lc_pouch_namespace_list_cleanup(&client->pouch_allocator, &namespaces);
      lc_pouch_txn_recovery_list_cleanup(&protected_txns);
      lc_pouch_txn_recovery_list_cleanup(&replay_txns);
      return rc;
    }
  }
  for (index = 0U; index < namespaces.count; ++index) {
    rc = lc_pouch_txn_cleanup_abandoned_namespace(
        client, namespaces.names[index], &protected_txns, error);
    if (rc != LC_OK) {
      lc_pouch_namespace_list_cleanup(&client->pouch_allocator, &namespaces);
      lc_pouch_txn_recovery_list_cleanup(&protected_txns);
      lc_pouch_txn_recovery_list_cleanup(&replay_txns);
      return rc;
    }
  }
  lc_pouch_namespace_list_cleanup(&client->pouch_allocator, &namespaces);
  lc_pouch_txn_recovery_list_cleanup(&protected_txns);
  lc_pouch_txn_recovery_list_cleanup(&replay_txns);
  return LC_OK;
}

static int lc_pouch_copy_attachment_info(lc_attachment_info *dst,
                                         const lc_pouch_object_info *src,
                                         lc_error *error) {
  memset(dst, 0, sizeof(*dst));
  if (lc_pouch_copy_public(&dst->id, src->id, error,
                           "failed to copy pouch attachment id") != LC_OK ||
      lc_pouch_copy_public(&dst->name, src->name, error,
                           "failed to copy pouch attachment name") != LC_OK ||
      lc_pouch_copy_public(&dst->plaintext_sha256, src->plaintext_sha256, error,
                           "failed to copy pouch attachment digest") != LC_OK ||
      lc_pouch_copy_public(&dst->content_type, src->content_type, error,
                           "failed to copy pouch attachment content type") !=
          LC_OK) {
    lc_attachment_info_cleanup(dst);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  dst->size = src->size;
  dst->created_at_unix = src->created_at_unix;
  dst->updated_at_unix = src->updated_at_unix;
  return LC_OK;
}

static int lc_pouch_copy_source_to_sink(lc_source *source, lc_sink *sink,
                                        lc_error *error) {
  unsigned char buffer[8192];
  size_t got;

  while (1) {
    got = source->read(source, buffer, sizeof(buffer), error);
    if (got == 0U) {
      break;
    }
    if (!sink->write(sink, buffer, got, error)) {
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_TRANSPORT;
    }
  }
  return LC_OK;
}

static int lc_pouch_copy_source_to_file_limited(lc_source *source, FILE *fp,
                                                size_t limit, lc_error *error) {
  unsigned char buffer[8192];
  size_t got;
  size_t total;

  total = 0U;
  while (1) {
    got = source->read(source, buffer, sizeof(buffer), error);
    if (got == 0U) {
      break;
    }
    if (limit > 0U && (total > limit || got > limit - total)) {
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "mapped state response exceeds configured byte limit",
                          NULL, NULL, NULL);
    }
    if (fwrite(buffer, 1U, got, fp) != got) {
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to buffer pouch mapped state", NULL, NULL,
                          NULL);
    }
    total += got;
  }
  return LC_OK;
}

static void lc_pouch_lease_ref_from_handle(lc_lease_handle *lease,
                                           lc_lease_ref *ref) {
  memset(ref, 0, sizeof(*ref));
  ref->namespace_name = lease->namespace_name;
  ref->key = lease->key;
  ref->lease_id = lease->lease_id;
  ref->txn_id = lease->txn_id;
  ref->fencing_token = lease->fencing_token;
}

static void lc_pouch_queue_ref_from_message(const lc_message_ref *src,
                                            lc_pouch_queue_ref *dst) {
  memset(dst, 0, sizeof(*dst));
  if (src == NULL) {
    return;
  }
  dst->namespace_name = src->namespace_name;
  dst->queue = src->queue;
  dst->message_id = src->message_id;
  dst->lease_id = src->lease_id;
  dst->txn_id = src->txn_id;
  dst->fencing_token = src->fencing_token;
  dst->meta_etag = src->meta_etag;
}

static void
lc_pouch_queue_info_to_engine(const lc_pouch_queue_message_info *info,
                              lc_engine_dequeue_response *out) {
  memset(out, 0, sizeof(*out));
  out->namespace_name = (char *)info->namespace_name;
  out->queue = (char *)info->queue;
  out->message_id = (char *)info->message_id;
  out->attempts = info->attempts;
  out->max_attempts = info->max_attempts;
  out->failure_attempts = info->failure_attempts;
  out->not_visible_until_unix = info->not_visible_until_unix;
  out->visibility_timeout_seconds = info->visibility_timeout_seconds;
  out->payload_content_type = (char *)info->payload_content_type;
  out->lease_id = (char *)info->lease_id;
  out->lease_expires_at_unix = info->lease_expires_at_unix;
  out->fencing_token = info->fencing_token;
  out->txn_id = (char *)info->txn_id;
  out->meta_etag = (char *)info->meta_etag;
  out->next_cursor = (char *)info->message_id;
}

static int lc_pouch_prepare_queue_state_lease(
    lc_client_handle *client, const char *namespace_name,
    const lc_dequeue_req *req, const lc_pouch_queue_message_info *info,
    char **state_lease_id, char **state_txn_id, char **state_etag,
    long *state_fencing_token, long *state_lease_expires_at_unix,
    lc_error *error) {
  lc_pouch_meta_record existing;
  lc_pouch_store_meta_res stored;
  lc_pouch_meta meta;
  char *state_key;
  char *lease_id;
  char *txn_id;
  char *etag;
  long now_unix;
  long ttl_seconds;
  int rc;

  if (client == NULL || req == NULL || info == NULL || state_lease_id == NULL ||
      state_txn_id == NULL || state_etag == NULL ||
      state_fencing_token == NULL || state_lease_expires_at_unix == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch stateful dequeue requires state outputs", NULL,
                        NULL, NULL);
  }
  memset(&existing, 0, sizeof(existing));
  memset(&stored, 0, sizeof(stored));
  memset(&meta, 0, sizeof(meta));
  state_key = NULL;
  lease_id = NULL;
  txn_id = NULL;
  etag = NULL;
  *state_lease_id = NULL;
  *state_txn_id = NULL;
  *state_etag = NULL;
  *state_fencing_token = 0L;
  *state_lease_expires_at_unix = 0L;

  state_key = lc_pouch_queue_state_key(client, req->queue, info->message_id);
  if (state_key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue state key", NULL, NULL,
                        NULL);
  }
  rc = client->pouch_store->load_meta(client->pouch_store, namespace_name,
                                      state_key, &existing, error);
  if (rc != LC_OK) {
    lc_client_free(client, state_key);
    return rc;
  }

  now_unix = lc_pouch_now_unix();
  ttl_seconds = req->visibility_timeout_seconds > 0L
                    ? req->visibility_timeout_seconds
                    : 30L;
  meta.version = existing.found ? existing.meta.version : 0L;
  meta.state_etag = existing.meta.state_etag;
  meta.fencing_token = existing.found ? existing.meta.fencing_token + 1L : 1L;
  meta.lease_expires_at_unix = info->lease_expires_at_unix > now_unix
                                   ? info->lease_expires_at_unix
                                   : now_unix + ttl_seconds;
  meta.owner = (char *)req->owner;
  meta.txn_id = (char *)req->txn_id;
  meta.has_query_hidden = existing.meta.has_query_hidden;
  meta.query_hidden = existing.meta.query_hidden;

  lease_id = lc_pouch_new_lease_id(client, state_key, meta.fencing_token);
  if (lease_id == NULL) {
    lc_pouch_meta_record_cleanup(&client->pouch_allocator, &existing);
    lc_client_free(client, state_key);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch queue state lease id", NULL,
                        NULL, NULL);
  }
  meta.lease_id = lease_id;
  rc = client->pouch_store->store_meta(client->pouch_store, namespace_name,
                                       state_key, &meta, existing.etag, &stored,
                                       error);
  if (rc != LC_OK) {
    lc_client_free(client, lease_id);
    lc_pouch_meta_record_cleanup(&client->pouch_allocator, &existing);
    lc_client_free(client, state_key);
    return rc;
  }

  if (req->txn_id != NULL) {
    txn_id = lc_client_strdup(client, req->txn_id);
    if (txn_id == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch queue state transaction id", NULL,
                        NULL, NULL);
    }
  }
  if (rc == LC_OK && existing.meta.state_etag != NULL) {
    etag = lc_client_strdup(client, existing.meta.state_etag);
    if (etag == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch queue state etag", NULL, NULL,
                        NULL);
    }
  }
  if (rc == LC_OK) {
    *state_lease_id = lease_id;
    *state_txn_id = txn_id;
    *state_etag = etag;
    *state_fencing_token = meta.fencing_token;
    *state_lease_expires_at_unix = meta.lease_expires_at_unix;
    lease_id = NULL;
    txn_id = NULL;
    etag = NULL;
  }

  lc_client_free(client, etag);
  lc_client_free(client, txn_id);
  lc_client_free(client, lease_id);
  lc_pouch_store_meta_res_cleanup(&client->pouch_allocator, &stored);
  lc_pouch_meta_record_cleanup(&client->pouch_allocator, &existing);
  lc_client_free(client, state_key);
  return rc;
}

static int lc_pouch_lease_load_method(lc_lease *self, const lonejson_map *map,
                                      void *dst, const lc_get_opts *opts,
                                      lc_get_res *out, lc_error *error) {
  lc_lease_handle *lease;
  lc_source *body;
  lc_pouch_state_info info;
  lc_pouch_meta_record record;
  lc_pouch_allocator *allocator;
  const char *namespace_name;
  lonejson *runtime;
  FILE *fp;
  size_t limit;
  int rc;

  if (self == NULL || map == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease load requires self, map, destination, "
                        "and out",
                        NULL, NULL, NULL);
  }
  (void)opts;
  lease = (lc_lease_handle *)self;
  allocator = &lease->client->pouch_allocator;
  namespace_name = NULL;
  body = NULL;
  fp = NULL;
  memset(out, 0, sizeof(*out));
  memset(&info, 0, sizeof(info));
  memset(&record, 0, sizeof(record));

  rc = lc_pouch_public_namespace(lease->client, lease->namespace_name,
                                 &namespace_name, error);
  if (rc != LC_OK) {
    return rc;
  }
  runtime = lc_thread_lonejson_runtime();
  if (runtime == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize lonejson runtime", NULL, NULL,
                        NULL);
  }
  lc_lonejson_prepare_parse_destination(runtime, map, dst);
  rc = lease->client->pouch_store->read_state(lease->client->pouch_store,
                                              namespace_name, lease->key, &body,
                                              &info, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!info.no_content) {
    fp = tmpfile();
    if (fp == NULL) {
      if (body != NULL) {
        body->close(body);
      }
      lc_pouch_state_info_cleanup(allocator, &info);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to create pouch mapped load buffer", NULL,
                          NULL, NULL);
    }
    limit = lease->client->http_json_response_limit_bytes > 0U
                ? lease->client->http_json_response_limit_bytes
                : (size_t)LC_HTTP_JSON_RESPONSE_LIMIT_DEFAULT;
    rc = lc_pouch_copy_source_to_file_limited(body, fp, limit, error);
    body->close(body);
    body = NULL;
    if (rc != LC_OK) {
      fclose(fp);
      lc_pouch_state_info_cleanup(allocator, &info);
      return rc;
    }
    if (fflush(fp) != 0 || fseek(fp, 0L, SEEK_SET) != 0) {
      fclose(fp);
      lc_pouch_state_info_cleanup(allocator, &info);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to rewind pouch mapped load buffer", NULL,
                          NULL, NULL);
    }
    rc = lc_lonejson_parse_prepared_file(runtime, fp, map, dst, error,
                                         "failed to parse mapped lease state");
    fclose(fp);
    fp = NULL;
    if (rc != LC_OK) {
      lc_pouch_state_info_cleanup(allocator, &info);
      return rc;
    }
  } else if (body != NULL) {
    body->close(body);
    body = NULL;
  }

  out->no_content = info.no_content;
  out->version = info.version;
  if (lc_pouch_copy_public(&out->content_type,
                           info.no_content ? NULL : info.content_type, error,
                           "failed to copy pouch content type") != LC_OK ||
      lc_pouch_copy_public(&out->etag, info.no_content ? NULL : info.etag,
                           error, "failed to copy pouch etag") != LC_OK) {
    lc_get_res_cleanup(out);
    lc_pouch_state_info_cleanup(allocator, &info);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lease->client->pouch_store->load_meta(
      lease->client->pouch_store, namespace_name, lease->key, &record, error);
  if (rc != LC_OK) {
    lc_get_res_cleanup(out);
    lc_pouch_meta_record_cleanup(allocator, &record);
    lc_pouch_state_info_cleanup(allocator, &info);
    return rc;
  }
  rc = lc_pouch_repair_meta_state_gap(lease->client, namespace_name, lease->key,
                                      &record, &info, error);
  if (rc != LC_OK) {
    lc_get_res_cleanup(out);
    lc_pouch_meta_record_cleanup(allocator, &record);
    lc_pouch_state_info_cleanup(allocator, &info);
    return rc;
  }
  if (record.found) {
    rc = lc_pouch_refresh_lease(lease, &record.meta, error);
    if (rc != LC_OK) {
      lc_get_res_cleanup(out);
      lc_pouch_meta_record_cleanup(allocator, &record);
      lc_pouch_state_info_cleanup(allocator, &info);
      return rc;
    }
    out->version = record.meta.version;
    out->fencing_token = record.meta.fencing_token;
  }
  lc_pouch_meta_record_cleanup(allocator, &record);
  lc_pouch_state_info_cleanup(allocator, &info);
  return LC_OK;
}

static int lc_pouch_lease_save_method(lc_lease *self, const lonejson_map *map,
                                      const void *src, lc_error *error) {
  lc_lease_handle *lease;
  lc_source *source;
  lc_update_opts opts;
  lonejson *runtime;
  FILE *fp;
  int rc;

  if (self == NULL || map == NULL || src == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease save requires self, map, and source", NULL,
                        NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  runtime = lc_thread_lonejson_runtime();
  if (runtime == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize lonejson runtime", NULL, NULL,
                        NULL);
  }
  fp = tmpfile();
  if (fp == NULL) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create pouch mapped save buffer", NULL, NULL,
                        NULL);
  }
  rc = lc_lonejson_serialize_file(runtime, fp, map, src, error,
                                  "failed to serialize pouch mapped state");
  if (rc != LC_OK) {
    fclose(fp);
    return rc;
  }
  if (fflush(fp) != 0 || fseek(fp, 0L, SEEK_SET) != 0) {
    fclose(fp);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to rewind pouch mapped save buffer", NULL, NULL,
                        NULL);
  }
  source = lc_source_from_open_file(fp, 1);
  if (source == NULL) {
    fclose(fp);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to wrap pouch mapped save buffer", NULL, NULL,
                        NULL);
  }
  lc_update_opts_init(&opts);
  opts.content_type = "application/json";
  if (lease->version > 0L) {
    opts.if_version = lease->version;
    opts.has_if_version = 1;
  }
  rc = lc_pouch_lease_update_method(self, source, &opts, error);
  lc_source_close(source);
  return rc;
}

static int lc_pouch_mutate_write_empty_object(FILE *fp, lc_error *error) {
  if (fp == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch mutate requires scratch file", NULL, NULL, NULL);
  }
  if (fwrite("{}", 1U, 2U, fp) != 2U) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to seed pouch mutate document", strerror(errno),
                        NULL, NULL);
  }
  if (fflush(fp) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to flush pouch mutate document",
                        strerror(errno), NULL, NULL);
  }
  rewind(fp);
  return LC_OK;
}

static int
lc_pouch_apply_mutations(lc_client_handle *client, const lc_lease_ref *lease,
                         const char *const *mutations, size_t mutation_count,
                         const char *if_state_etag, long if_version,
                         int has_if_version, int default_lease_version,
                         long default_if_version, lc_update_res *out,
                         lc_error *error) {
  lc_mutation_parse_options parse_options;
  lc_mutation_plan *plan;
  lc_update_req update_req;
  lc_update_res update_res;
  lc_pouch_state_info state_info;
  lc_source *source;
  lc_source *body;
  const char *namespace_name;
  FILE *input_fp;
  FILE *final_fp;
  size_t limit;
  int rc;

  if (client == NULL || lease == NULL || lease->key == NULL ||
      mutations == NULL || mutation_count == 0U || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch mutate requires client, lease, mutations, and "
                        "output",
                        NULL, NULL, NULL);
  }

  memset(&parse_options, 0, sizeof(parse_options));
  memset(&state_info, 0, sizeof(state_info));
  memset(&update_res, 0, sizeof(update_res));
  lc_update_req_init(&update_req);
  plan = NULL;
  source = NULL;
  body = NULL;
  namespace_name = NULL;
  input_fp = NULL;
  final_fp = NULL;

  if (clock_gettime(CLOCK_REALTIME, &parse_options.now) == 0) {
    parse_options.has_now = 1;
  }

  rc = lc_mutation_plan_build(mutations, mutation_count, &parse_options, &plan,
                              error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_public_namespace(client, lease->namespace_name, &namespace_name,
                                 error);
  if (rc != LC_OK) {
    lc_mutation_plan_close(plan);
    return rc;
  }

  input_fp = tmpfile();
  if (input_fp == NULL) {
    lc_mutation_plan_close(plan);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create pouch mutate input file",
                        strerror(errno), NULL, NULL);
  }
  rc = client->pouch_store->read_state(client->pouch_store, namespace_name,
                                       lease->key, &body, &state_info, error);
  if (rc != LC_OK) {
    fclose(input_fp);
    lc_mutation_plan_close(plan);
    return rc;
  }
  if (!state_info.no_content && body != NULL) {
    limit = client->http_json_response_limit_bytes > 0U
                ? client->http_json_response_limit_bytes
                : (size_t)LC_HTTP_JSON_RESPONSE_LIMIT_DEFAULT;
    rc = lc_pouch_copy_source_to_file_limited(body, input_fp, limit, error);
    body->close(body);
    body = NULL;
    if (rc != LC_OK) {
      fclose(input_fp);
      lc_pouch_state_info_cleanup(&client->pouch_allocator, &state_info);
      lc_mutation_plan_close(plan);
      return rc;
    }
  } else if (body != NULL) {
    body->close(body);
    body = NULL;
  }
  if (fflush(input_fp) != 0) {
    fclose(input_fp);
    lc_pouch_state_info_cleanup(&client->pouch_allocator, &state_info);
    lc_mutation_plan_close(plan);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to flush pouch mutate input file",
                        strerror(errno), NULL, NULL);
  }
  rewind(input_fp);

  if (state_info.no_content) {
    rc = lc_pouch_mutate_write_empty_object(input_fp, error);
    if (rc != LC_OK) {
      fclose(input_fp);
      lc_pouch_state_info_cleanup(&client->pouch_allocator, &state_info);
      lc_mutation_plan_close(plan);
      return rc;
    }
  }

  rc = lc_mutation_plan_apply(plan, input_fp, &final_fp, error);
  fclose(input_fp);
  lc_mutation_plan_close(plan);
  if (rc != LC_OK) {
    lc_pouch_state_info_cleanup(&client->pouch_allocator, &state_info);
    return rc;
  }

  update_req.lease = *lease;
  update_req.content_type = "application/json";
  update_req.if_state_etag = if_state_etag;
  update_req.if_version = if_version;
  update_req.has_if_version = has_if_version;
  if (default_lease_version && !update_req.has_if_version &&
      default_if_version > 0L) {
    update_req.if_version = default_if_version;
    update_req.has_if_version = 1;
  }

  source = lc_source_from_open_file(final_fp, 0);
  if (source == NULL) {
    fclose(final_fp);
    lc_pouch_state_info_cleanup(&client->pouch_allocator, &state_info);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to wrap pouch mutate output stream", NULL, NULL,
                        NULL);
  }
  rc = lc_pouch_client_update_method(&client->pub, &update_req, source,
                                     &update_res, error);
  lc_source_close(source);
  fclose(final_fp);
  lc_pouch_state_info_cleanup(&client->pouch_allocator, &state_info);
  if (rc != LC_OK) {
    lc_update_res_cleanup(&update_res);
    return rc;
  }
  *out = update_res;
  memset(&update_res, 0, sizeof(update_res));
  return LC_OK;
}

static int lc_pouch_lease_mutate_method(lc_lease *self,
                                        const lc_mutate_req *req,
                                        lc_error *error) {
  lc_lease_handle *lease;
  lc_lease_ref ref;
  lc_update_res update_res;
  int rc;

  if (self == NULL || req == NULL || req->mutations == NULL ||
      req->mutation_count == 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease mutate requires self, request, and "
                        "mutations",
                        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  if (lease->pouch_stage_active || lease->pouch_txn_explicit) {
    return lc_pouch_lease_mutate_staged_method(self, req, error);
  }
  memset(&ref, 0, sizeof(ref));
  memset(&update_res, 0, sizeof(update_res));
  ref.namespace_name = lease->namespace_name;
  ref.key = lease->key;
  ref.lease_id = lease->lease_id;
  ref.txn_id = lease->txn_id;
  ref.fencing_token = lease->fencing_token;

  rc = lc_pouch_apply_mutations(lease->client, &ref, req->mutations,
                                req->mutation_count, req->if_state_etag,
                                req->if_version, req->has_if_version, 1,
                                lease->version, &update_res, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_set_lease_state(lease, update_res.new_state_etag,
                                update_res.new_version, error);
  lc_update_res_cleanup(&update_res);
  return rc;
}

static int lc_pouch_lease_mutate_staged_method(lc_lease *self,
                                               const lc_mutate_req *req,
                                               lc_error *error) {
  lc_lease_handle *lease;
  lc_mutation_parse_options parse_options;
  lc_mutation_plan *plan;
  lc_pouch_state_info state_info;
  lc_source *body;
  lc_source *source;
  lc_update_opts update_opts;
  const char *namespace_name;
  FILE *input_fp;
  FILE *final_fp;
  size_t limit;
  int rc;

  if (self == NULL || req == NULL || req->mutations == NULL ||
      req->mutation_count == 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease mutate requires self, request, and "
                        "mutations",
                        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  memset(&parse_options, 0, sizeof(parse_options));
  memset(&state_info, 0, sizeof(state_info));
  lc_update_opts_init(&update_opts);
  plan = NULL;
  body = NULL;
  source = NULL;
  namespace_name = NULL;
  input_fp = NULL;
  final_fp = NULL;

  if (clock_gettime(CLOCK_REALTIME, &parse_options.now) == 0) {
    parse_options.has_now = 1;
  }
  rc = lc_mutation_plan_build(req->mutations, req->mutation_count,
                              &parse_options, &plan, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_public_namespace(lease->client, lease->namespace_name,
                                 &namespace_name, error);
  if (rc != LC_OK) {
    lc_mutation_plan_close(plan);
    return rc;
  }

  input_fp = tmpfile();
  if (input_fp == NULL) {
    lc_mutation_plan_close(plan);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create pouch staged mutate input file",
                        strerror(errno), NULL, NULL);
  }

  if (lease->pouch_stage_dirty) {
    rc = lease->client->pouch_store->load_staged_state(
        lease->client->pouch_store, namespace_name, lease->key, lease->txn_id,
        &body, &state_info, error);
  } else {
    rc = lease->client->pouch_store->read_state(lease->client->pouch_store,
                                                namespace_name, lease->key,
                                                &body, &state_info, error);
  }
  if (rc != LC_OK) {
    fclose(input_fp);
    lc_mutation_plan_close(plan);
    return rc;
  }
  if (!state_info.no_content && body != NULL) {
    limit = lease->client->http_json_response_limit_bytes > 0U
                ? lease->client->http_json_response_limit_bytes
                : (size_t)LC_HTTP_JSON_RESPONSE_LIMIT_DEFAULT;
    rc = lc_pouch_copy_source_to_file_limited(body, input_fp, limit, error);
    body->close(body);
    body = NULL;
    if (rc != LC_OK) {
      fclose(input_fp);
      lc_pouch_state_info_cleanup(&lease->client->pouch_allocator, &state_info);
      lc_mutation_plan_close(plan);
      return rc;
    }
  } else if (body != NULL) {
    body->close(body);
    body = NULL;
  }
  if (fflush(input_fp) != 0) {
    fclose(input_fp);
    lc_pouch_state_info_cleanup(&lease->client->pouch_allocator, &state_info);
    lc_mutation_plan_close(plan);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to flush pouch staged mutate input file",
                        strerror(errno), NULL, NULL);
  }
  rewind(input_fp);

  if (state_info.no_content) {
    rc = lc_pouch_mutate_write_empty_object(input_fp, error);
    if (rc != LC_OK) {
      fclose(input_fp);
      lc_pouch_state_info_cleanup(&lease->client->pouch_allocator, &state_info);
      lc_mutation_plan_close(plan);
      return rc;
    }
  }

  rc = lc_mutation_plan_apply(plan, input_fp, &final_fp, error);
  fclose(input_fp);
  lc_mutation_plan_close(plan);
  lc_pouch_state_info_cleanup(&lease->client->pouch_allocator, &state_info);
  if (rc != LC_OK) {
    return rc;
  }

  update_opts.content_type = "application/json";
  update_opts.if_state_etag = req->if_state_etag;
  update_opts.if_version = req->if_version;
  update_opts.has_if_version = req->has_if_version;
  if (!update_opts.has_if_version && lease->version > 0L) {
    update_opts.if_version = lease->version;
    update_opts.has_if_version = 1;
  }

  source = lc_source_from_open_file(final_fp, 0);
  if (source == NULL) {
    fclose(final_fp);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to wrap pouch staged mutate output stream",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_lease_staged_update_method(self, source, &update_opts, error);
  lc_source_close(source);
  fclose(final_fp);
  return rc;
}

static int lc_pouch_lease_mutate_local_method(lc_lease *self,
                                              const lc_mutate_local_req *req,
                                              lc_error *error) {
  lc_mutation_parse_options parse_options;
  lc_mutation_plan *plan;
  lc_update_opts update_opts;
  lc_get_res get_res;
  lc_source *source;
  lc_sink sink;
  lc_pouch_file_sink sink_impl;
  FILE *input_fp;
  FILE *final_fp;
  int rc;

  if (self == NULL || req == NULL || req->mutations == NULL ||
      req->mutation_count == 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease mutate_local requires self and mutations",
                        NULL, NULL, NULL);
  }

  memset(&parse_options, 0, sizeof(parse_options));
  memset(&get_res, 0, sizeof(get_res));
  lc_update_opts_init(&update_opts);
  parse_options.file_value_base_dir = req->file_value_base_dir;
  parse_options.file_value_resolver = req->file_value_resolver;
  if (clock_gettime(CLOCK_REALTIME, &parse_options.now) == 0) {
    parse_options.has_now = 1;
  }
  plan = NULL;
  source = NULL;
  input_fp = NULL;
  final_fp = NULL;

  rc = lc_mutation_plan_build(req->mutations, req->mutation_count,
                              &parse_options, &plan, error);
  if (rc != LC_OK) {
    return rc;
  }

  input_fp = tmpfile();
  if (input_fp == NULL) {
    lc_mutation_plan_close(plan);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create pouch local mutate input file",
                        strerror(errno), NULL, NULL);
  }
  lc_pouch_file_sink_init(&sink, &sink_impl, input_fp, "local_mutate_input");
  rc = lc_pouch_lease_get_method(self, &sink, NULL, &get_res, error);
  if (rc != LC_OK) {
    fclose(input_fp);
    lc_mutation_plan_close(plan);
    return rc;
  }
  if (fflush(input_fp) != 0) {
    fclose(input_fp);
    lc_get_res_cleanup(&get_res);
    lc_mutation_plan_close(plan);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to flush pouch local mutate input file",
                        strerror(errno), NULL, NULL);
  }
  rewind(input_fp);

  if (get_res.no_content) {
    rc = lc_pouch_mutate_write_empty_object(input_fp, error);
    if (rc != LC_OK) {
      fclose(input_fp);
      lc_get_res_cleanup(&get_res);
      lc_mutation_plan_close(plan);
      return rc;
    }
  }

  rc = lc_mutation_plan_apply(plan, input_fp, &final_fp, error);
  fclose(input_fp);
  lc_mutation_plan_close(plan);
  if (rc != LC_OK) {
    lc_get_res_cleanup(&get_res);
    return rc;
  }

  update_opts = req->update;
  if (update_opts.content_type == NULL || update_opts.content_type[0] == '\0') {
    update_opts.content_type = "application/json";
  }
  if (!req->disable_fetched_cas) {
    if ((update_opts.if_state_etag == NULL ||
         update_opts.if_state_etag[0] == '\0') &&
        get_res.etag != NULL && get_res.etag[0] != '\0') {
      update_opts.if_state_etag = get_res.etag;
    }
    if (!update_opts.has_if_version && get_res.version > 0L) {
      update_opts.if_version = get_res.version;
      update_opts.has_if_version = 1;
    }
  }

  source = lc_source_from_open_file(final_fp, 0);
  if (source == NULL) {
    fclose(final_fp);
    lc_get_res_cleanup(&get_res);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to wrap pouch local mutate output stream", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_lease_update_method(self, source, &update_opts, error);
  lc_source_close(source);
  fclose(final_fp);
  lc_get_res_cleanup(&get_res);
  return rc;
}

static int lc_pouch_validate_active_lease(lc_client_handle *client,
                                          const lc_lease_ref *lease,
                                          lc_pouch_meta_record *record,
                                          lc_error *error) {
  int rc;
  long now_unix;
  const char *namespace_name;

  memset(record, 0, sizeof(*record));
  if (lease == NULL || lease->key == NULL || lease->lease_id == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch operation requires lease key and lease_id", NULL,
                        NULL, NULL);
  }
  namespace_name = NULL;
  rc = lc_pouch_public_namespace(client, lease->namespace_name, &namespace_name,
                                 error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = client->pouch_store->load_meta(client->pouch_store, namespace_name,
                                      lease->key, record, error);
  if (rc != LC_OK) {
    return rc;
  }
  now_unix = lc_pouch_now_unix();
  if (!record->found || record->meta.lease_id == NULL ||
      strcmp(record->meta.lease_id, lease->lease_id) != 0) {
    lc_pouch_meta_record_cleanup(&client->pouch_allocator, record);
    return lc_error_set(error, LC_ERR_SERVER, 403L,
                        "pouch operation requires active lease", NULL,
                        "lease_required", NULL);
  }
  if (record->meta.lease_expires_at_unix <= now_unix) {
    lc_pouch_meta_record_cleanup(&client->pouch_allocator, record);
    return lc_error_set(error, LC_ERR_SERVER, 403L, "pouch lease expired", NULL,
                        "lease_expired", NULL);
  }
  if (record->meta.fencing_token != lease->fencing_token) {
    lc_pouch_meta_record_cleanup(&client->pouch_allocator, record);
    return lc_error_set(error, LC_ERR_SERVER, 403L,
                        "pouch fencing token mismatch", NULL,
                        "fencing_mismatch", NULL);
  }
  if (record->meta.txn_id != NULL && lease->txn_id == NULL) {
    lc_pouch_meta_record_cleanup(&client->pouch_allocator, record);
    return lc_error_set(error, LC_ERR_SERVER, 400L,
                        "pouch operation requires transaction id for this "
                        "lease",
                        NULL, "missing_txn", NULL);
  }
  if (record->meta.txn_id != NULL && lease->txn_id != NULL &&
      strcmp(record->meta.txn_id, lease->txn_id) != 0) {
    lc_pouch_meta_record_cleanup(&client->pouch_allocator, record);
    return lc_error_set(error, LC_ERR_SERVER, 409L,
                        "pouch transaction id does not match active lease",
                        NULL, "txn_mismatch", NULL);
  }
  return LC_OK;
}

static int lc_pouch_repair_meta_state_gap(
    lc_client_handle *client, const char *namespace_name, const char *key,
    lc_pouch_meta_record *record, const lc_pouch_state_info *known_state,
    lc_error *error) {
  lc_pouch_state_info state;
  const lc_pouch_state_info *state_ref;
  lc_pouch_store_meta_res stored;
  lc_source *body;
  lc_pouch_meta next_meta;
  int mismatch;
  int rc;

  if (client == NULL || namespace_name == NULL || key == NULL ||
      record == NULL || !record->found) {
    return LC_OK;
  }
  memset(&state, 0, sizeof(state));
  memset(&stored, 0, sizeof(stored));
  state_ref = known_state;
  body = NULL;
  if (state_ref == NULL) {
    rc = client->pouch_store->read_state(client->pouch_store, namespace_name,
                                         key, &body, &state, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (body != NULL) {
      body->close(body);
      body = NULL;
    }
    state_ref = &state;
  }
  if (state_ref->no_content || state_ref->etag == NULL ||
      state_ref->etag[0] == '\0') {
    if (state_ref->no_content && record->meta.state_etag != NULL) {
      next_meta = record->meta;
      next_meta.state_etag = NULL;
      next_meta.version = record->meta.version + 1L;
      rc = client->pouch_store->store_meta(client->pouch_store, namespace_name,
                                           key, &next_meta, record->etag,
                                           &stored, error);
      lc_pouch_store_meta_res_cleanup(&client->pouch_allocator, &stored);
      lc_pouch_state_info_cleanup(&client->pouch_allocator, &state);
      if (rc != LC_OK) {
        return rc;
      }
      lc_pouch_meta_record_cleanup(&client->pouch_allocator, record);
      return client->pouch_store->load_meta(client->pouch_store, namespace_name,
                                            key, record, error);
    }
    lc_pouch_state_info_cleanup(&client->pouch_allocator, &state);
    return LC_OK;
  }
  mismatch = record->meta.state_etag == NULL ||
             strcmp(record->meta.state_etag, state_ref->etag) != 0;
  if (!mismatch) {
    lc_pouch_state_info_cleanup(&client->pouch_allocator, &state);
    return LC_OK;
  }
  next_meta = record->meta;
  next_meta.state_etag = state_ref->etag;
  next_meta.version = state_ref->version;
  rc =
      client->pouch_store->store_meta(client->pouch_store, namespace_name, key,
                                      &next_meta, record->etag, &stored, error);
  lc_pouch_store_meta_res_cleanup(&client->pouch_allocator, &stored);
  lc_pouch_state_info_cleanup(&client->pouch_allocator, &state);
  if (rc != LC_OK) {
    return rc;
  }
  lc_pouch_meta_record_cleanup(&client->pouch_allocator, record);
  return client->pouch_store->load_meta(client->pouch_store, namespace_name,
                                        key, record, error);
}

static int lc_pouch_refresh_lease(lc_lease_handle *lease,
                                  const lc_pouch_meta *meta, lc_error *error) {
  char *owner;
  char *lease_id;
  char *txn_id;
  char *state_etag;

  owner = NULL;
  lease_id = NULL;
  txn_id = NULL;
  state_etag = NULL;
  if (lc_pouch_copy_client(lease->client, &owner, meta->owner, error,
                           "failed to copy pouch lease owner") != LC_OK ||
      lc_pouch_copy_client(lease->client, &lease_id, meta->lease_id, error,
                           "failed to copy pouch lease id") != LC_OK ||
      lc_pouch_copy_client(lease->client, &txn_id,
                           meta->txn_id != NULL ? meta->txn_id : lease->txn_id,
                           error,
                           "failed to copy pouch transaction id") != LC_OK ||
      lc_pouch_copy_client(lease->client, &state_etag, meta->state_etag, error,
                           "failed to copy pouch state etag") != LC_OK) {
    lc_client_free(lease->client, owner);
    lc_client_free(lease->client, lease_id);
    lc_client_free(lease->client, txn_id);
    lc_client_free(lease->client, state_etag);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  lc_client_free(lease->client, lease->owner);
  lc_client_free(lease->client, lease->lease_id);
  lc_client_free(lease->client, lease->txn_id);
  lc_client_free(lease->client, lease->state_etag);
  lease->owner = owner;
  lease->lease_id = lease_id;
  lease->txn_id = txn_id;
  lease->state_etag = state_etag;
  lease->version = meta->version;
  lease->lease_expires_at_unix = meta->lease_expires_at_unix;
  lease->fencing_token = meta->fencing_token;
  lease->has_query_hidden = meta->has_query_hidden;
  lease->query_hidden = meta->query_hidden;
  lease->pub.owner = lease->owner;
  lease->pub.lease_id = lease->lease_id;
  lease->pub.txn_id = lease->txn_id;
  lease->pub.state_etag = lease->state_etag;
  lease->pub.version = lease->version;
  lease->pub.lease_expires_at_unix = lease->lease_expires_at_unix;
  lease->pub.fencing_token = lease->fencing_token;
  lease->pub.has_query_hidden = lease->has_query_hidden;
  lease->pub.query_hidden = lease->query_hidden;
  return LC_OK;
}

static int lc_pouch_set_lease_state(lc_lease_handle *lease,
                                    const char *state_etag, long version,
                                    lc_error *error) {
  char *new_state_etag;

  new_state_etag = lc_client_strdup(lease->client, state_etag);
  if (state_etag != NULL && new_state_etag == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch lease state etag", NULL, NULL,
                        NULL);
  }
  lc_client_free(lease->client, lease->state_etag);
  lease->state_etag = new_state_etag;
  lease->version = version;
  lease->pub.state_etag = lease->state_etag;
  lease->pub.version = lease->version;
  return LC_OK;
}

static void lc_pouch_install_lease_methods(lc_lease *lease) {
  if (lease == NULL) {
    return;
  }
  lease->describe = lc_pouch_lease_describe_method;
  lease->get = lc_pouch_lease_get_method;
  lease->load = lc_pouch_lease_load_method;
  lease->save = lc_pouch_lease_save_method;
  lease->update = lc_pouch_lease_update_method;
  lease->mutate = lc_pouch_lease_mutate_method;
  lease->mutate_local = lc_pouch_lease_mutate_local_method;
  lease->metadata = lc_pouch_lease_metadata_method;
  lease->remove = lc_pouch_lease_remove_method;
  lease->keepalive = lc_pouch_lease_keepalive_method;
  lease->release = lc_pouch_lease_release_method;
  lease->attach = lc_pouch_lease_attach_method;
  lease->list_attachments = lc_pouch_lease_list_attachments_method;
  lease->get_attachment = lc_pouch_lease_get_attachment_method;
  lease->delete_attachment = lc_pouch_lease_delete_attachment_method;
  lease->delete_all_attachments = lc_pouch_lease_delete_all_attachments_method;
}

int lc_pouch_client_acquire_method(lc_client *self, const lc_acquire_req *req,
                                   lc_lease **out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_meta_record existing;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_allocator *allocator;
  const char *namespace_name;
  const char *txn_id;
  char *lease_id;
  char *generated_txn_id;
  lc_lease *lease;
  int64_t deadline_ms;
  int64_t now_ms;
  int64_t remaining_ms;
  long now_unix;
  long sleep_ms;
  int rc;

  if (self == NULL || req == NULL || out == NULL || req->key == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch acquire requires self, req, key, and out", NULL,
                        NULL, NULL);
  }
  if (req->owner == NULL || req->owner[0] == '\0') {
    return lc_error_set(error, LC_ERR_SERVER, 400L,
                        "pouch acquire requires owner", NULL, "missing_owner",
                        NULL);
  }
  if (req->block_seconds < 0L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch acquire block_seconds must be non-negative",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  allocator = &client->pouch_allocator;
  namespace_name = NULL;
  rc = lc_pouch_public_namespace(client, req->namespace_name, &namespace_name,
                                 error);
  if (rc != LC_OK) {
    return rc;
  }
  deadline_ms = 0L;
  if (req->block_seconds > 0L) {
    now_ms = lc_pouch_now_millis();
    if (now_ms > 0L) {
      deadline_ms = now_ms + req->block_seconds * 1000L;
    }
  }
acquire_retry:
  txn_id = req->txn_id;
  generated_txn_id = NULL;
  lease_id = NULL;
  memset(&existing, 0, sizeof(existing));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  *out = NULL;
  rc = client->pouch_store->load_meta(client->pouch_store, namespace_name,
                                      req->key, &existing, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_repair_meta_state_gap(client, namespace_name, req->key,
                                      &existing, NULL, error);
  if (rc != LC_OK) {
    lc_pouch_meta_record_cleanup(allocator, &existing);
    return rc;
  }
  now_unix = lc_pouch_now_unix();
  if (existing.found && existing.meta.lease_expires_at_unix > now_unix) {
    if (deadline_ms > 0L) {
      now_ms = lc_pouch_now_millis();
      if (now_ms > 0L && now_ms < deadline_ms) {
        remaining_ms = deadline_ms - now_ms;
        sleep_ms = remaining_ms < 100L ? (long)remaining_ms : 100L;
        lc_pouch_meta_record_cleanup(allocator, &existing);
        lc_pouch_sleep_millis(sleep_ms);
        goto acquire_retry;
      }
    }
    lc_pouch_meta_record_cleanup(allocator, &existing);
    return lc_error_set(error, LC_ERR_SERVER, 409L,
                        "pouch lease is already active", NULL, "lease_conflict",
                        NULL);
  }
  if (req->if_not_exists && existing.found) {
    lc_pouch_meta_record_cleanup(allocator, &existing);
    return lc_error_set(error, LC_ERR_SERVER, 412L,
                        "pouch acquire if_not_exists precondition failed", NULL,
                        "precondition_failed", NULL);
  }
  meta.version = existing.found ? existing.meta.version : 0L;
  meta.state_etag = existing.meta.state_etag;
  meta.fencing_token = existing.found ? existing.meta.fencing_token + 1L : 1L;
  meta.lease_expires_at_unix =
      now_unix + (req->ttl_seconds > 0L ? req->ttl_seconds : 30L);
  if (txn_id == NULL || txn_id[0] == '\0') {
    generated_txn_id =
        lc_pouch_new_txn_id(client, req->key, meta.fencing_token);
    if (generated_txn_id == NULL) {
      lc_pouch_meta_record_cleanup(allocator, &existing);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch transaction id", NULL, NULL,
                          NULL);
    }
    txn_id = generated_txn_id;
  }
  meta.owner = (char *)req->owner;
  meta.txn_id = (char *)txn_id;
  meta.has_query_hidden = existing.meta.has_query_hidden;
  meta.query_hidden = existing.meta.query_hidden;
  lease_id = lc_pouch_new_lease_id(client, req->key, meta.fencing_token);
  if (lease_id == NULL) {
    lc_client_free(client, generated_txn_id);
    lc_pouch_meta_record_cleanup(allocator, &existing);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch lease id", NULL, NULL, NULL);
  }
  meta.lease_id = lease_id;
  rc = client->pouch_store->store_meta(client->pouch_store, namespace_name,
                                       req->key, &meta, existing.etag, &stored,
                                       error);
  if (rc != LC_OK) {
    lc_client_free(client, lease_id);
    lc_client_free(client, generated_txn_id);
    lc_pouch_meta_record_cleanup(allocator, &existing);
    if (deadline_ms > 0L && error != NULL && error->code == LC_ERR_SERVER &&
        error->http_status == 412L && error->server_code != NULL &&
        strcmp(error->server_code, "precondition_failed") == 0) {
      now_ms = lc_pouch_now_millis();
      if (now_ms > 0L && now_ms < deadline_ms) {
        remaining_ms = deadline_ms - now_ms;
        sleep_ms = remaining_ms < 100L ? (long)remaining_ms : 100L;
        lc_error_cleanup(error);
        lc_pouch_sleep_millis(sleep_ms);
        goto acquire_retry;
      }
    }
    return rc;
  }
  lease = lc_lease_new(client, namespace_name, req->key, req->owner, lease_id,
                       txn_id, meta.fencing_token, meta.version,
                       meta.state_etag, NULL);
  lc_client_free(client, lease_id);
  lc_client_free(client, generated_txn_id);
  if (lease == NULL) {
    lc_pouch_store_meta_res_cleanup(allocator, &stored);
    lc_pouch_meta_record_cleanup(allocator, &existing);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch lease handle", NULL, NULL,
                        NULL);
  }
  ((lc_lease_handle *)lease)->lease_expires_at_unix =
      meta.lease_expires_at_unix;
  ((lc_lease_handle *)lease)->has_query_hidden = meta.has_query_hidden;
  ((lc_lease_handle *)lease)->query_hidden = meta.query_hidden;
  ((lc_lease_handle *)lease)->pouch_txn_explicit =
      req->txn_id != NULL && req->txn_id[0] != '\0';
  lease->lease_expires_at_unix = meta.lease_expires_at_unix;
  lease->has_query_hidden = meta.has_query_hidden;
  lease->query_hidden = meta.query_hidden;
  lc_pouch_install_lease_methods(lease);
  *out = lease;
  lc_pouch_store_meta_res_cleanup(allocator, &stored);
  lc_pouch_meta_record_cleanup(allocator, &existing);
  return LC_OK;
}

int lc_pouch_client_acquire_for_update_method(
    lc_client *self, const lc_acquire_req *req,
    lc_acquire_for_update_handler_fn handler, void *handler_context,
    lc_error *error) {
  lc_client_handle *client;
  lc_lease *lease;
  lc_lease_handle *lease_handle;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lc_release_req release_req;
  lc_error handler_error;
  lc_error release_error;
  lc_pouch_acquire_for_update_file_sink file_sink;
  lc_sink sink;
  lc_acquire_for_update_context update;
  lc_pouch_promote_staged_opts promote_opts;
  lc_pouch_discard_staged_opts discard_opts;
  lc_pouch_put_state_res promoted;
  lc_pouch_meta_record record;
  lc_pouch_store_meta_res stored;
  lc_pouch_meta next_meta;
  lc_lease_ref ref;
  char *generated_txn_id;
  FILE *fp;
  FILE *snapshot_fp;
  int rc;
  int release_rc;

  if (self == NULL || req == NULL || handler == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch acquire_for_update requires self, req, and "
                        "handler",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  lease = NULL;
  lease_handle = NULL;
  memset(&get_res, 0, sizeof(get_res));
  lc_get_opts_init(&get_opts);
  lc_release_req_init(&release_req);
  lc_error_init(&handler_error);
  lc_error_init(&release_error);
  memset(&update, 0, sizeof(update));
  memset(&promote_opts, 0, sizeof(promote_opts));
  memset(&discard_opts, 0, sizeof(discard_opts));
  memset(&promoted, 0, sizeof(promoted));
  memset(&record, 0, sizeof(record));
  memset(&stored, 0, sizeof(stored));
  memset(&ref, 0, sizeof(ref));
  generated_txn_id = NULL;
  fp = NULL;
  snapshot_fp = NULL;
  rc = lc_pouch_client_acquire_method(self, req, &lease, error);
  if (rc != LC_OK) {
    return rc;
  }
  lease_handle = (lc_lease_handle *)lease;
  if (lease_handle->txn_id == NULL || lease_handle->txn_id[0] == '\0') {
    generated_txn_id = lc_pouch_new_txn_id(client, lease_handle->key,
                                           lease_handle->fencing_token);
    if (generated_txn_id == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch acquire_for_update "
                        "transaction id",
                        NULL, NULL, NULL);
      goto release_and_return;
    }
    lc_client_free(client, lease_handle->txn_id);
    lease_handle->txn_id = generated_txn_id;
    lease_handle->pub.txn_id = lease_handle->txn_id;
    generated_txn_id = NULL;
  }

  fp = tmpfile();
  if (fp == NULL) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to create pouch acquire_for_update snapshot file",
                      strerror(errno), NULL, NULL);
    goto release_and_return;
  }
  file_sink.fp = fp;
  sink.write = lc_pouch_acquire_for_update_sink_write;
  sink.close = lc_pouch_acquire_for_update_sink_close;
  sink.impl = &file_sink;

  rc = lc_lease_get(lease, &sink, &get_opts, &get_res, error);
  if (rc != LC_OK) {
    goto release_and_return;
  }
  if (fflush(fp) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to flush pouch acquire_for_update snapshot",
                      strerror(errno), NULL, NULL);
    goto release_and_return;
  }
  rewind(fp);

  lease_handle->pouch_stage_active = 1;
  lease_handle->pouch_stage_dirty = 0;
  lease_handle->pouch_stage_version = 0L;
  lc_client_free(client, lease_handle->pouch_stage_etag);
  lease_handle->pouch_stage_etag = NULL;
  lease->update = lc_pouch_lease_staged_update_method;

  update.lease = lease;
  update.state.has_state = !get_res.no_content;
  update.state.content_type = get_res.content_type;
  update.state.etag = get_res.etag;
  update.state.version = get_res.version;
  update.state.fencing_token = get_res.fencing_token;
  update.state.correlation_id = get_res.correlation_id;
  if (!get_res.no_content) {
    snapshot_fp = fp;
    fp = NULL;
    update.state.reader = lc_source_from_open_file(snapshot_fp, 1);
    snapshot_fp = NULL;
    if (update.state.reader == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to wrap pouch acquire_for_update snapshot "
                        "source",
                        NULL, NULL, NULL);
      goto release_and_return;
    }
  }

  rc = handler(handler_context, &update, &handler_error);
  if (update.state.reader != NULL) {
    lc_source_close(update.state.reader);
    update.state.reader = NULL;
  }
  if (rc != LC_OK) {
    discard_opts.ignore_not_found = 1;
    if (lease_handle->pouch_stage_dirty) {
      (void)client->pouch_store->discard_staged_state(
          client->pouch_store, lease_handle->namespace_name, lease_handle->key,
          lease_handle->txn_id, &discard_opts, &release_error);
    }
    release_req.rollback = 1;
    if (error != NULL) {
      *error = handler_error;
      lc_error_init(&handler_error);
    }
    goto release_and_return;
  }

  if (lease_handle->pouch_stage_dirty) {
    promote_opts.expected_head_etag = get_res.no_content ? NULL : get_res.etag;
    rc = client->pouch_store->promote_staged_state(
        client->pouch_store, lease_handle->namespace_name, lease_handle->key,
        lease_handle->txn_id, &promote_opts, &promoted, error);
    if (rc != LC_OK) {
      discard_opts.ignore_not_found = 1;
      (void)client->pouch_store->discard_staged_state(
          client->pouch_store, lease_handle->namespace_name, lease_handle->key,
          lease_handle->txn_id, &discard_opts, &release_error);
      release_req.rollback = 1;
      goto release_and_return;
    }
  }
  if (lease_handle->pouch_stage_dirty && rc == LC_OK) {
    memset(&ref, 0, sizeof(ref));
    ref.namespace_name = lease_handle->namespace_name;
    ref.key = lease_handle->key;
    ref.lease_id = lease_handle->lease_id;
    ref.txn_id = lease_handle->txn_id;
    ref.fencing_token = lease_handle->fencing_token;
    rc = lc_pouch_validate_active_lease(client, &ref, &record, error);
    if (rc == LC_OK) {
      next_meta = record.meta;
      next_meta.version = promoted.new_version;
      next_meta.state_etag = promoted.new_state_etag;
      rc = client->pouch_store->store_meta(
          client->pouch_store, record.namespace_name, lease_handle->key,
          &next_meta, record.etag, &stored, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_set_lease_state(lease_handle, promoted.new_state_etag,
                                    promoted.new_version, error);
    }
    if (rc != LC_OK) {
      release_req.rollback = 1;
    }
  }

release_and_return:
  if (update.state.reader != NULL) {
    lc_source_close(update.state.reader);
    update.state.reader = NULL;
  }
  if (fp != NULL) {
    fclose(fp);
    fp = NULL;
  }
  if (lease != NULL) {
    lc_pouch_install_lease_methods(lease);
  }
  release_rc = lc_lease_release(lease, &release_req, &release_error);
  if (release_rc != LC_OK && rc == LC_OK) {
    rc = release_rc;
    if (error != NULL) {
      *error = release_error;
      lc_error_init(&release_error);
    }
  }
  if (release_rc == LC_OK) {
    lease = NULL;
  } else if (lease != NULL) {
    lc_lease_close(lease);
  }
  lc_pouch_store_meta_res_cleanup(&client->pouch_allocator, &stored);
  lc_pouch_meta_record_cleanup(&client->pouch_allocator, &record);
  lc_pouch_put_state_res_cleanup(&client->pouch_allocator, &promoted);
  lc_client_free(client, generated_txn_id);
  lc_get_res_cleanup(&get_res);
  lc_error_cleanup(&handler_error);
  lc_error_cleanup(&release_error);
  return rc;
}

int lc_pouch_client_describe_method(lc_client *self, const lc_describe_req *req,
                                    lc_describe_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_meta_record record;
  lc_pouch_allocator *allocator;
  const char *namespace_name;
  int rc;

  if (self == NULL || req == NULL || out == NULL || req->key == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch describe requires self, req, key, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  allocator = &client->pouch_allocator;
  namespace_name = NULL;
  rc = lc_pouch_public_namespace(client, req->namespace_name, &namespace_name,
                                 error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(out, 0, sizeof(*out));
  memset(&record, 0, sizeof(record));
  rc = client->pouch_store->load_meta(client->pouch_store, namespace_name,
                                      req->key, &record, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!record.found) {
    return lc_error_set(error, LC_ERR_SERVER, 404L,
                        "pouch key metadata was not found", NULL, "not_found",
                        NULL);
  }
  if (lc_pouch_copy_public(&out->namespace_name, namespace_name, error,
                           "failed to copy pouch namespace") != LC_OK ||
      lc_pouch_copy_public(&out->key, req->key, error,
                           "failed to copy pouch key") != LC_OK ||
      lc_pouch_copy_public(&out->owner, record.meta.owner, error,
                           "failed to copy pouch owner") != LC_OK ||
      lc_pouch_copy_public(&out->lease_id, record.meta.lease_id, error,
                           "failed to copy pouch lease id") != LC_OK ||
      lc_pouch_copy_public(&out->txn_id, record.meta.txn_id, error,
                           "failed to copy pouch transaction id") != LC_OK ||
      lc_pouch_copy_public(&out->state_etag, record.meta.state_etag, error,
                           "failed to copy pouch state etag") != LC_OK) {
    lc_describe_res_cleanup(out);
    lc_pouch_meta_record_cleanup(allocator, &record);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  out->version = record.meta.version;
  out->lease_expires_at_unix = record.meta.lease_expires_at_unix;
  out->fencing_token = record.meta.fencing_token;
  out->has_query_hidden = record.meta.has_query_hidden;
  out->query_hidden = record.meta.query_hidden;
  lc_pouch_meta_record_cleanup(allocator, &record);
  return LC_OK;
}

static int lc_pouch_client_get_in_namespace(
    lc_client_handle *client, const char *namespace_name_opt, const char *key,
    const lc_get_opts *opts, lc_sink *dst, lc_get_res *out, lc_error *error) {
  lc_source *body;
  lc_pouch_state_info info;
  lc_pouch_meta_record record;
  lc_pouch_allocator *allocator;
  const char *namespace_name;
  unsigned char buffer[8192];
  size_t got;
  int rc;

  if (client == NULL || key == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch get requires self, key, dst, and out", NULL,
                        NULL, NULL);
  }
  (void)opts;
  allocator = &client->pouch_allocator;
  namespace_name = NULL;
  rc = lc_pouch_public_namespace(client, namespace_name_opt, &namespace_name,
                                 error);
  if (rc != LC_OK) {
    return rc;
  }
  body = NULL;
  memset(out, 0, sizeof(*out));
  memset(&info, 0, sizeof(info));
  memset(&record, 0, sizeof(record));
  rc = client->pouch_store->read_state(client->pouch_store, namespace_name, key,
                                       &body, &info, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!info.no_content) {
    while (1) {
      got = body->read(body, buffer, sizeof(buffer), error);
      if (got == 0U) {
        break;
      }
      if (!dst->write(dst, buffer, got, error)) {
        body->close(body);
        lc_pouch_state_info_cleanup(allocator, &info);
        return error != NULL && error->code != LC_OK ? error->code
                                                     : LC_ERR_TRANSPORT;
      }
    }
    body->close(body);
  }
  out->no_content = info.no_content;
  out->version = info.version;
  if (lc_pouch_copy_public(&out->content_type,
                           info.no_content ? NULL : info.content_type, error,
                           "failed to copy pouch content type") != LC_OK ||
      lc_pouch_copy_public(&out->etag, info.no_content ? NULL : info.etag,
                           error, "failed to copy pouch etag") != LC_OK) {
    lc_get_res_cleanup(out);
    lc_pouch_state_info_cleanup(allocator, &info);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = client->pouch_store->load_meta(client->pouch_store, namespace_name, key,
                                      &record, error);
  if (rc != LC_OK) {
    lc_get_res_cleanup(out);
    lc_pouch_meta_record_cleanup(allocator, &record);
    lc_pouch_state_info_cleanup(allocator, &info);
    return rc;
  }
  rc = lc_pouch_repair_meta_state_gap(client, namespace_name, key, &record,
                                      &info, error);
  if (rc != LC_OK) {
    lc_get_res_cleanup(out);
    lc_pouch_meta_record_cleanup(allocator, &record);
    lc_pouch_state_info_cleanup(allocator, &info);
    return rc;
  }
  if (record.found) {
    out->version = record.meta.version;
    out->fencing_token = record.meta.fencing_token;
  }
  lc_pouch_meta_record_cleanup(allocator, &record);
  lc_pouch_state_info_cleanup(allocator, &info);
  return LC_OK;
}

int lc_pouch_client_get_method(lc_client *self, const char *key,
                               const lc_get_opts *opts, lc_sink *dst,
                               lc_get_res *out, lc_error *error) {
  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch get requires self, key, dst, and out", NULL,
                        NULL, NULL);
  }
  return lc_pouch_client_get_in_namespace((lc_client_handle *)self, NULL, key,
                                          opts, dst, out, error);
}

int lc_pouch_client_load_method(lc_client *self, const char *key,
                                const lonejson_map *map, void *dst,
                                const lc_get_opts *opts, lc_get_res *out,
                                lc_error *error) {
  lc_client_handle *client;
  lc_source *body;
  lc_pouch_state_info info;
  lc_pouch_meta_record record;
  lc_pouch_allocator *allocator;
  const char *namespace_name;
  lonejson *runtime;
  FILE *fp;
  size_t limit;
  int rc;

  if (self == NULL || key == NULL || map == NULL || dst == NULL ||
      out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch load requires self, key, map, destination, "
                        "and out",
                        NULL, NULL, NULL);
  }
  (void)opts;
  client = (lc_client_handle *)self;
  allocator = &client->pouch_allocator;
  namespace_name = NULL;
  body = NULL;
  fp = NULL;
  memset(out, 0, sizeof(*out));
  memset(&info, 0, sizeof(info));
  memset(&record, 0, sizeof(record));

  rc = lc_pouch_public_namespace(client, NULL, &namespace_name, error);
  if (rc != LC_OK) {
    return rc;
  }
  runtime = lc_thread_lonejson_runtime();
  if (runtime == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize lonejson runtime", NULL, NULL,
                        NULL);
  }
  lc_lonejson_prepare_parse_destination(runtime, map, dst);
  rc = client->pouch_store->read_state(client->pouch_store, namespace_name, key,
                                       &body, &info, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!info.no_content) {
    fp = tmpfile();
    if (fp == NULL) {
      if (body != NULL) {
        body->close(body);
      }
      lc_pouch_state_info_cleanup(allocator, &info);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to create pouch mapped load buffer", NULL,
                          NULL, NULL);
    }
    limit = client->http_json_response_limit_bytes > 0U
                ? client->http_json_response_limit_bytes
                : (size_t)LC_HTTP_JSON_RESPONSE_LIMIT_DEFAULT;
    rc = lc_pouch_copy_source_to_file_limited(body, fp, limit, error);
    body->close(body);
    body = NULL;
    if (rc != LC_OK) {
      fclose(fp);
      lc_pouch_state_info_cleanup(allocator, &info);
      return rc;
    }
    if (fflush(fp) != 0 || fseek(fp, 0L, SEEK_SET) != 0) {
      fclose(fp);
      lc_pouch_state_info_cleanup(allocator, &info);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to rewind pouch mapped load buffer", NULL,
                          NULL, NULL);
    }
    rc = lc_lonejson_parse_prepared_file(runtime, fp, map, dst, error,
                                         "failed to parse mapped state");
    fclose(fp);
    fp = NULL;
    if (rc != LC_OK) {
      lc_pouch_state_info_cleanup(allocator, &info);
      return rc;
    }
  } else if (body != NULL) {
    body->close(body);
    body = NULL;
  }

  out->no_content = info.no_content;
  out->version = info.version;
  if (lc_pouch_copy_public(&out->content_type,
                           info.no_content ? NULL : info.content_type, error,
                           "failed to copy pouch content type") != LC_OK ||
      lc_pouch_copy_public(&out->etag, info.no_content ? NULL : info.etag,
                           error, "failed to copy pouch etag") != LC_OK) {
    lc_get_res_cleanup(out);
    lc_pouch_state_info_cleanup(allocator, &info);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = client->pouch_store->load_meta(client->pouch_store, namespace_name, key,
                                      &record, error);
  if (rc != LC_OK) {
    lc_get_res_cleanup(out);
    lc_pouch_meta_record_cleanup(allocator, &record);
    lc_pouch_state_info_cleanup(allocator, &info);
    return rc;
  }
  rc = lc_pouch_repair_meta_state_gap(client, namespace_name, key, &record,
                                      &info, error);
  if (rc != LC_OK) {
    lc_get_res_cleanup(out);
    lc_pouch_meta_record_cleanup(allocator, &record);
    lc_pouch_state_info_cleanup(allocator, &info);
    return rc;
  }
  if (record.found) {
    out->version = record.meta.version;
    out->fencing_token = record.meta.fencing_token;
  }
  lc_pouch_meta_record_cleanup(allocator, &record);
  lc_pouch_state_info_cleanup(allocator, &info);
  return LC_OK;
}

int lc_pouch_client_update_method(lc_client *self, const lc_update_req *req,
                                  lc_source *src, lc_update_res *out,
                                  lc_error *error) {
  lc_client_handle *client;
  lc_pouch_meta_record record;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res put;
  lc_pouch_store_meta_res stored;
  lc_pouch_allocator *allocator;
  lc_pouch_meta next_meta;
  int rc;

  if (self == NULL || req == NULL || src == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch update requires self, req, src, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  allocator = &client->pouch_allocator;
  memset(out, 0, sizeof(*out));
  memset(&record, 0, sizeof(record));
  memset(&opts, 0, sizeof(opts));
  memset(&put, 0, sizeof(put));
  memset(&stored, 0, sizeof(stored));
  rc = lc_pouch_validate_active_lease(client, &req->lease, &record, error);
  if (rc != LC_OK) {
    return rc;
  }
  opts.content_type =
      req->content_type != NULL ? req->content_type : "application/json";
  opts.if_state_etag = req->if_state_etag;
  opts.if_version = req->if_version;
  opts.has_if_version = req->has_if_version;
  rc = client->pouch_store->write_state(client->pouch_store,
                                        record.namespace_name, req->lease.key,
                                        src, &opts, &put, error);
  if (rc == LC_OK) {
    next_meta = record.meta;
    next_meta.version = put.new_version;
    next_meta.state_etag = put.new_state_etag;
    rc = client->pouch_store->store_meta(
        client->pouch_store, record.namespace_name, req->lease.key, &next_meta,
        record.etag, &stored, error);
  }
  if (rc == LC_OK) {
    out->new_version = put.new_version;
    out->bytes = put.bytes;
    rc = lc_pouch_copy_public(&out->new_state_etag, put.new_state_etag, error,
                              "failed to copy pouch update etag");
  }
  lc_pouch_store_meta_res_cleanup(allocator, &stored);
  lc_pouch_put_state_res_cleanup(allocator, &put);
  lc_pouch_meta_record_cleanup(allocator, &record);
  return rc;
}

int lc_pouch_client_mutate_method(lc_client *self, const lc_mutate_op *req,
                                  lc_mutate_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_update_res update_res;
  int rc;

  if (self == NULL || req == NULL || out == NULL || req->mutations == NULL ||
      req->mutation_count == 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch mutate requires self, request, mutations, and "
                        "output",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  memset(out, 0, sizeof(*out));
  memset(&update_res, 0, sizeof(update_res));

  rc = lc_pouch_apply_mutations(client, &req->lease, req->mutations,
                                req->mutation_count, req->if_state_etag,
                                req->if_version, req->has_if_version, 0, 0L,
                                &update_res, error);
  if (rc != LC_OK) {
    return rc;
  }
  out->new_version = update_res.new_version;
  out->bytes = update_res.bytes;
  out->new_state_etag = lc_strdup_local(update_res.new_state_etag);
  lc_update_res_cleanup(&update_res);
  if (out->new_state_etag == NULL) {
    lc_mutate_res_cleanup(out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch mutate etag", NULL, NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_lease_staged_update_method(lc_lease *self, lc_source *src,
                                               const lc_update_opts *opts,
                                               lc_error *error) {
  lc_lease_handle *lease;
  lc_pouch_meta_record record;
  lc_pouch_put_state_opts put_opts;
  lc_pouch_put_state_res put;
  lc_pouch_allocator *allocator;
  lc_lease_ref ref;
  char *stage_etag;
  int rc;

  if (self == NULL || src == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged update requires self and src", NULL, NULL,
                        NULL);
  }
  lease = (lc_lease_handle *)self;
  if (!lease->pouch_stage_active && !lease->pouch_txn_explicit) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged update requires acquire_for_update or "
                        "an explicit transaction",
                        NULL, NULL, NULL);
  }
  if (opts != NULL && opts->has_if_version &&
      opts->if_version != lease->version) {
    return lc_error_set(error, LC_ERR_SERVER, 412L,
                        "pouch staged update version precondition failed", NULL,
                        "precondition_failed", NULL);
  }
  if (opts != NULL && opts->if_state_etag != NULL &&
      (lease->state_etag == NULL ||
       strcmp(opts->if_state_etag, lease->state_etag) != 0)) {
    return lc_error_set(error, LC_ERR_SERVER, 412L,
                        "pouch staged update etag precondition failed", NULL,
                        "precondition_failed", NULL);
  }

  allocator = &lease->client->pouch_allocator;
  memset(&record, 0, sizeof(record));
  memset(&put_opts, 0, sizeof(put_opts));
  memset(&put, 0, sizeof(put));
  memset(&ref, 0, sizeof(ref));
  ref.namespace_name = lease->namespace_name;
  ref.key = lease->key;
  ref.lease_id = lease->lease_id;
  ref.txn_id = lease->txn_id;
  ref.fencing_token = lease->fencing_token;
  rc = lc_pouch_validate_active_lease(lease->client, &ref, &record, error);
  if (rc != LC_OK) {
    return rc;
  }

  put_opts.content_type = opts != NULL && opts->content_type != NULL
                              ? opts->content_type
                              : "application/json";
  if (lease->pouch_stage_dirty && lease->pouch_stage_etag != NULL) {
    put_opts.if_state_etag = lease->pouch_stage_etag;
    put_opts.if_version = lease->pouch_stage_version;
    put_opts.has_if_version = lease->pouch_stage_version > 0L;
  }

  rc = lease->client->pouch_store->stage_state(
      lease->client->pouch_store, record.namespace_name, lease->key,
      lease->txn_id, src, &put_opts, &put, error);
  if (rc == LC_OK) {
    stage_etag = lc_client_strdup(lease->client, put.new_state_etag);
    if (put.new_state_etag != NULL && stage_etag == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch staged update etag", NULL, NULL,
                        NULL);
    } else {
      lc_client_free(lease->client, lease->pouch_stage_etag);
      lease->pouch_stage_etag = stage_etag;
      lease->pouch_stage_version = put.new_version;
      lease->pouch_stage_dirty = 1;
      rc = lc_pouch_set_lease_state(lease, put.new_state_etag, put.new_version,
                                    error);
    }
  }

  lc_pouch_put_state_res_cleanup(allocator, &put);
  lc_pouch_meta_record_cleanup(allocator, &record);
  return rc;
}

static int lc_pouch_lease_staged_remove_method(lc_lease *self,
                                               const lc_remove_req *opts,
                                               lc_error *error) {
  lc_lease_handle *lease;
  lc_pouch_meta_record record;
  lc_pouch_put_state_res put;
  lc_pouch_allocator *allocator;
  lc_lease_ref ref;
  const char *expected_staged_etag;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged remove requires self", NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  if (!lease->pouch_stage_active && !lease->pouch_txn_explicit) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged remove requires acquire_for_update or "
                        "an explicit transaction",
                        NULL, NULL, NULL);
  }
  if (opts != NULL && opts->has_if_version &&
      opts->if_version != lease->version) {
    return lc_error_set(error, LC_ERR_SERVER, 412L,
                        "pouch staged remove version precondition failed", NULL,
                        "precondition_failed", NULL);
  }
  if (opts != NULL && opts->if_state_etag != NULL &&
      (lease->state_etag == NULL ||
       strcmp(opts->if_state_etag, lease->state_etag) != 0)) {
    return lc_error_set(error, LC_ERR_SERVER, 412L,
                        "pouch staged remove etag precondition failed", NULL,
                        "precondition_failed", NULL);
  }
  if (!lease->pouch_stage_dirty && lease->state_etag == NULL) {
    return LC_OK;
  }

  allocator = &lease->client->pouch_allocator;
  memset(&record, 0, sizeof(record));
  memset(&put, 0, sizeof(put));
  memset(&ref, 0, sizeof(ref));
  ref.namespace_name = lease->namespace_name;
  ref.key = lease->key;
  ref.lease_id = lease->lease_id;
  ref.txn_id = lease->txn_id;
  ref.fencing_token = lease->fencing_token;
  rc = lc_pouch_validate_active_lease(lease->client, &ref, &record, error);
  if (rc != LC_OK) {
    return rc;
  }

  expected_staged_etag =
      lease->pouch_stage_dirty ? lease->pouch_stage_etag : NULL;
  rc = lease->client->pouch_store->stage_state_remove(
      lease->client->pouch_store, record.namespace_name, lease->key,
      lease->txn_id, expected_staged_etag, &put, error);
  if (rc == LC_OK) {
    lc_client_free(lease->client, lease->pouch_stage_etag);
    lease->pouch_stage_etag = NULL;
    lease->pouch_stage_version = put.new_version;
    lease->pouch_stage_dirty = 1;
    rc = lc_pouch_set_lease_state(lease, NULL, put.new_version, error);
  }

  lc_pouch_put_state_res_cleanup(allocator, &put);
  lc_pouch_meta_record_cleanup(allocator, &record);
  return rc;
}

int lc_pouch_client_metadata_method(lc_client *self, const lc_metadata_op *req,
                                    lc_metadata_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_meta_record record;
  lc_pouch_store_meta_res stored;
  lc_pouch_allocator *allocator;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata requires self, req, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  allocator = &client->pouch_allocator;
  memset(out, 0, sizeof(*out));
  memset(&record, 0, sizeof(record));
  memset(&stored, 0, sizeof(stored));
  rc = lc_pouch_validate_active_lease(client, &req->lease, &record, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (req->has_if_version && record.meta.version != req->if_version) {
    lc_pouch_meta_record_cleanup(allocator, &record);
    return lc_error_set(error, LC_ERR_SERVER, 412L,
                        "pouch metadata version precondition failed", NULL,
                        "precondition_failed", NULL);
  }
  if (req->has_query_hidden) {
    record.meta.has_query_hidden = 1;
    record.meta.query_hidden = req->query_hidden;
  }
  record.meta.version += 1L;
  rc = client->pouch_store->store_meta(
      client->pouch_store, record.namespace_name, req->lease.key, &record.meta,
      record.etag, &stored, error);
  if (rc == LC_OK) {
    if (lc_pouch_copy_public(&out->namespace_name, record.namespace_name, error,
                             "failed to copy pouch namespace") != LC_OK ||
        lc_pouch_copy_public(&out->key, req->lease.key, error,
                             "failed to copy pouch key") != LC_OK) {
      lc_metadata_res_cleanup(out);
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    } else {
      out->version = stored.version;
      out->has_query_hidden = record.meta.has_query_hidden;
      out->query_hidden = record.meta.query_hidden;
    }
  }
  lc_pouch_store_meta_res_cleanup(allocator, &stored);
  lc_pouch_meta_record_cleanup(allocator, &record);
  return rc;
}

int lc_pouch_client_remove_method(lc_client *self, const lc_remove_op *req,
                                  lc_remove_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_meta_record record;
  lc_pouch_store_meta_res stored;
  lc_pouch_allocator *allocator;
  lc_pouch_meta next_meta;
  int removed;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch remove requires self, req, and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  allocator = &client->pouch_allocator;
  memset(out, 0, sizeof(*out));
  memset(&record, 0, sizeof(record));
  memset(&stored, 0, sizeof(stored));
  removed = 0;
  rc = lc_pouch_validate_active_lease(client, &req->lease, &record, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (req->has_if_version && record.meta.version != req->if_version) {
    lc_pouch_meta_record_cleanup(allocator, &record);
    return lc_error_set(error, LC_ERR_SERVER, 412L,
                        "pouch remove version precondition failed", NULL,
                        "precondition_failed", NULL);
  }
  rc = client->pouch_store->remove_state(client->pouch_store,
                                         record.namespace_name, req->lease.key,
                                         req->if_state_etag, &removed, error);
  if (rc == LC_OK && removed) {
    next_meta = record.meta;
    next_meta.version = record.meta.version + 1L;
    next_meta.state_etag = NULL;
    rc = client->pouch_store->store_meta(
        client->pouch_store, record.namespace_name, req->lease.key, &next_meta,
        record.etag, &stored, error);
  }
  if (rc == LC_OK) {
    out->removed = removed;
    out->new_version = removed ? next_meta.version : record.meta.version;
  }
  lc_pouch_store_meta_res_cleanup(allocator, &stored);
  lc_pouch_meta_record_cleanup(allocator, &record);
  return rc;
}

int lc_pouch_client_keepalive_method(lc_client *self,
                                     const lc_keepalive_op *req,
                                     lc_keepalive_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_meta_record record;
  lc_pouch_store_meta_res stored;
  lc_pouch_allocator *allocator;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch keepalive requires self, req, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  allocator = &client->pouch_allocator;
  memset(out, 0, sizeof(*out));
  memset(&record, 0, sizeof(record));
  memset(&stored, 0, sizeof(stored));
  rc = lc_pouch_validate_active_lease(client, &req->lease, &record, error);
  if (rc != LC_OK) {
    return rc;
  }
  record.meta.lease_expires_at_unix =
      lc_pouch_now_unix() + (req->ttl_seconds > 0L ? req->ttl_seconds : 30L);
  rc = client->pouch_store->store_meta(
      client->pouch_store, record.namespace_name, req->lease.key, &record.meta,
      record.etag, &stored, error);
  if (rc == LC_OK) {
    out->lease_expires_at_unix = record.meta.lease_expires_at_unix;
    out->version = record.meta.version;
    rc = lc_pouch_copy_public(&out->state_etag, record.meta.state_etag, error,
                              "failed to copy pouch state etag");
  }
  lc_pouch_store_meta_res_cleanup(allocator, &stored);
  lc_pouch_meta_record_cleanup(allocator, &record);
  return rc;
}

int lc_pouch_client_release_method(lc_client *self, const lc_release_op *req,
                                   lc_release_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_meta_record record;
  lc_pouch_store_meta_res stored;
  lc_pouch_allocator *allocator;
  lc_pouch_meta next_meta;
  const char *namespace_name;
  long now_unix;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch release requires self, req, and out", NULL, NULL,
                        NULL);
  }
  if (req->lease.key == NULL || req->lease.lease_id == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch release requires lease key and lease_id", NULL,
                        NULL, NULL);
  }
  if (req->lease.txn_id == NULL || req->lease.txn_id[0] == '\0') {
    return lc_error_set(error, LC_ERR_SERVER, 400L,
                        "pouch release requires transaction id", NULL,
                        "missing_txn", NULL);
  }
  client = (lc_client_handle *)self;
  allocator = &client->pouch_allocator;
  memset(out, 0, sizeof(*out));
  memset(&record, 0, sizeof(record));
  memset(&stored, 0, sizeof(stored));
  namespace_name = NULL;
  rc = lc_pouch_public_namespace(client, req->lease.namespace_name,
                                 &namespace_name, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = client->pouch_store->load_meta(client->pouch_store, namespace_name,
                                      req->lease.key, &record, error);
  if (rc != LC_OK) {
    return rc;
  }
  now_unix = lc_pouch_now_unix();
  if (!record.found || record.meta.lease_id == NULL ||
      strcmp(record.meta.lease_id, req->lease.lease_id) != 0 ||
      record.meta.fencing_token != req->lease.fencing_token) {
    out->released = 1;
    lc_pouch_meta_record_cleanup(allocator, &record);
    return LC_OK;
  }
  if (record.meta.lease_expires_at_unix <= now_unix) {
    next_meta = record.meta;
    next_meta.owner = NULL;
    next_meta.lease_id = NULL;
    next_meta.txn_id = NULL;
    next_meta.lease_expires_at_unix = 0L;
    rc = client->pouch_store->store_meta(
        client->pouch_store, record.namespace_name, req->lease.key, &next_meta,
        record.etag, &stored, error);
    if (rc == LC_OK) {
      out->released = 1;
    }
    lc_pouch_store_meta_res_cleanup(allocator, &stored);
    lc_pouch_meta_record_cleanup(allocator, &record);
    return rc;
  }
  if (record.meta.txn_id != NULL &&
      strcmp(record.meta.txn_id, req->lease.txn_id) != 0) {
    lc_pouch_meta_record_cleanup(allocator, &record);
    return lc_error_set(error, LC_ERR_SERVER, 409L,
                        "pouch transaction id does not match active lease",
                        NULL, "txn_mismatch", NULL);
  }
  next_meta = record.meta;
  next_meta.owner = NULL;
  next_meta.lease_id = NULL;
  next_meta.txn_id = NULL;
  next_meta.lease_expires_at_unix = 0L;
  rc = client->pouch_store->store_meta(client->pouch_store,
                                       record.namespace_name, req->lease.key,
                                       &next_meta, record.etag, &stored, error);
  if (rc == LC_OK) {
    out->released = 1;
  }
  lc_pouch_store_meta_res_cleanup(allocator, &stored);
  lc_pouch_meta_record_cleanup(allocator, &record);
  return rc;
}

int lc_pouch_client_attach_method(lc_client *self, const lc_attach_op *req,
                                  lc_source *src, lc_attach_res *out,
                                  lc_error *error) {
  lc_client_handle *client;
  lc_pouch_meta_record record;
  lc_pouch_store_meta_res stored;
  lc_pouch_put_object_opts opts;
  lc_pouch_object_selector rollback_selector;
  lc_pouch_object_info object;
  lc_pouch_meta next_meta;
  lc_pouch_allocator *allocator;
  lc_error rollback_error;
  lc_lease_handle txn_lease;
  lc_attach_req staged_req;
  int deleted;
  int rc;

  if (self == NULL || req == NULL || src == NULL || out == NULL ||
      req->name == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch attach requires self, req, src, out, and name",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  allocator = &client->pouch_allocator;
  memset(out, 0, sizeof(*out));
  if (lc_pouch_lease_ref_is_explicit_txn(&req->lease)) {
    memset(&txn_lease, 0, sizeof(txn_lease));
    memset(&staged_req, 0, sizeof(staged_req));
    rc = lc_pouch_stack_lease_from_ref(client, &req->lease, &txn_lease, &record,
                                       error);
    if (rc != LC_OK) {
      return rc;
    }
    staged_req.name = req->name;
    staged_req.content_type = req->content_type;
    staged_req.max_bytes = req->max_bytes;
    staged_req.has_max_bytes = req->has_max_bytes;
    staged_req.prevent_overwrite = req->prevent_overwrite;
    rc = lc_pouch_lease_staged_attach(&txn_lease, &staged_req, src, out, error);
    lc_pouch_meta_record_cleanup(allocator, &record);
    return rc;
  }
  memset(&record, 0, sizeof(record));
  memset(&stored, 0, sizeof(stored));
  memset(&opts, 0, sizeof(opts));
  memset(&rollback_selector, 0, sizeof(rollback_selector));
  memset(&object, 0, sizeof(object));
  memset(&rollback_error, 0, sizeof(rollback_error));
  rc = lc_pouch_validate_active_lease(client, &req->lease, &record, error);
  if (rc != LC_OK) {
    return rc;
  }
  opts.name = req->name;
  opts.content_type = req->content_type;
  opts.max_bytes = req->max_bytes;
  opts.has_max_bytes = req->has_max_bytes;
  opts.prevent_overwrite = req->prevent_overwrite;
  rc = client->pouch_store->put_object(client->pouch_store,
                                       record.namespace_name, req->lease.key,
                                       src, &opts, &object, error);
  if (rc == LC_OK) {
    next_meta = record.meta;
    next_meta.version = record.meta.version + 1L;
    rc = client->pouch_store->store_meta(
        client->pouch_store, record.namespace_name, req->lease.key, &next_meta,
        record.etag, &stored, error);
    if (rc == LC_ERR_SERVER && object.id != NULL) {
      rollback_selector.id = object.id;
      deleted = 0;
      (void)client->pouch_store->delete_object(
          client->pouch_store, record.namespace_name, req->lease.key,
          &rollback_selector, &deleted, &rollback_error);
      lc_error_cleanup(&rollback_error);
    }
  }
  if (rc == LC_OK) {
    out->version = next_meta.version;
    rc = lc_pouch_copy_attachment_info(&out->attachment, &object, error);
  }
  lc_pouch_object_info_cleanup(allocator, &object);
  lc_pouch_store_meta_res_cleanup(allocator, &stored);
  lc_pouch_meta_record_cleanup(allocator, &record);
  return rc;
}

static int lc_pouch_lease_staged_attach(lc_lease_handle *lease,
                                        const lc_attach_req *req,
                                        lc_source *src, lc_attach_res *out,
                                        lc_error *error) {
  lc_pouch_meta_record record;
  lc_pouch_put_object_opts opts;
  lc_pouch_object_info object;
  lc_pouch_object_list live_objects;
  lc_pouch_allocator *allocator;
  lc_lease_ref ref;
  char *staged_key;
  size_t index;
  int rc;

  if (lease == NULL || req == NULL || src == NULL || out == NULL ||
      req->name == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged attach requires lease, req, src, out, "
                        "and name",
                        NULL, NULL, NULL);
  }
  allocator = &lease->client->pouch_allocator;
  memset(out, 0, sizeof(*out));
  memset(&record, 0, sizeof(record));
  memset(&opts, 0, sizeof(opts));
  memset(&object, 0, sizeof(object));
  memset(&live_objects, 0, sizeof(live_objects));
  memset(&ref, 0, sizeof(ref));
  staged_key = NULL;

  ref.namespace_name = lease->namespace_name;
  ref.key = lease->key;
  ref.lease_id = lease->lease_id;
  ref.txn_id = lease->txn_id;
  ref.fencing_token = lease->fencing_token;
  rc = lc_pouch_validate_active_lease(lease->client, &ref, &record, error);
  if (rc != LC_OK) {
    return rc;
  }

  if (req->prevent_overwrite) {
    rc = lease->client->pouch_store->list_objects(
        lease->client->pouch_store, record.namespace_name, lease->key,
        &live_objects, error);
    if (rc != LC_OK) {
      lc_pouch_meta_record_cleanup(allocator, &record);
      return rc;
    }
    for (index = 0U; index < live_objects.count; ++index) {
      if (live_objects.items[index].name != NULL &&
          strcmp(live_objects.items[index].name, req->name) == 0) {
        lc_pouch_object_list_cleanup(allocator, &live_objects);
        lc_pouch_meta_record_cleanup(allocator, &record);
        return lc_error_set(error, LC_ERR_SERVER, 409L,
                            "pouch attachment already exists", NULL,
                            "attachment_exists", NULL);
      }
    }
    lc_pouch_object_list_cleanup(allocator, &live_objects);
  }

  staged_key = lc_pouch_txn_attachment_stage_key(lease->client, lease->key,
                                                 lease->txn_id, error);
  if (staged_key == NULL) {
    lc_pouch_meta_record_cleanup(allocator, &record);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  opts.name = req->name;
  opts.content_type = req->content_type;
  opts.max_bytes = req->max_bytes;
  opts.has_max_bytes = req->has_max_bytes;
  opts.prevent_overwrite = req->prevent_overwrite;
  rc = lease->client->pouch_store->put_object(lease->client->pouch_store,
                                              record.namespace_name, staged_key,
                                              src, &opts, &object, error);
  if (rc == LC_OK) {
    out->version = lease->version + 1L;
    rc = lc_pouch_copy_attachment_info(&out->attachment, &object, error);
  }
  lc_client_free(lease->client, staged_key);
  lc_pouch_object_info_cleanup(allocator, &object);
  lc_pouch_meta_record_cleanup(allocator, &record);
  return rc;
}

static int
lc_pouch_object_matches_selector(const lc_pouch_object_info *object,
                                 const lc_attachment_selector *selector) {
  if (object == NULL || selector == NULL) {
    return 0;
  }
  if (selector->id != NULL && object->id != NULL &&
      strcmp(object->id, selector->id) == 0) {
    return 1;
  }
  if (selector->name != NULL && object->name != NULL &&
      strcmp(object->name, selector->name) == 0) {
    return 1;
  }
  return 0;
}

static int lc_pouch_object_name_equal(const lc_pouch_object_info *object,
                                      const char *name) {
  return object != NULL && object->name != NULL && name != NULL &&
         strcmp(object->name, name) == 0;
}

static int lc_pouch_object_list_has_name(const lc_pouch_object_list *objects,
                                         const char *name) {
  size_t index;

  if (objects == NULL || name == NULL) {
    return 0;
  }
  for (index = 0U; index < objects->count; ++index) {
    if (lc_pouch_object_name_equal(&objects->items[index], name)) {
      return 1;
    }
  }
  return 0;
}

static int
lc_pouch_txn_attachment_ops_have_clear(const lc_pouch_object_list *ops) {
  size_t index;

  if (ops == NULL) {
    return 0;
  }
  for (index = 0U; index < ops->count; ++index) {
    if (ops->items[index].name != NULL &&
        strcmp(ops->items[index].name, LC_POUCH_TXN_ATTACHMENT_OP_CLEAR) == 0) {
      return 1;
    }
  }
  return 0;
}

static int
lc_pouch_txn_attachment_op_deletes_object(const char *op_name,
                                          const lc_pouch_object_info *object) {
  size_t prefix_len;

  if (op_name == NULL || object == NULL) {
    return 0;
  }
  prefix_len = strlen(LC_POUCH_TXN_ATTACHMENT_OP_DELETE_ID);
  if (strncmp(op_name, LC_POUCH_TXN_ATTACHMENT_OP_DELETE_ID, prefix_len) == 0) {
    return object->id != NULL && strcmp(object->id, op_name + prefix_len) == 0;
  }
  prefix_len = strlen(LC_POUCH_TXN_ATTACHMENT_OP_DELETE_NAME);
  if (strncmp(op_name, LC_POUCH_TXN_ATTACHMENT_OP_DELETE_NAME, prefix_len) ==
      0) {
    return object->name != NULL &&
           strcmp(object->name, op_name + prefix_len) == 0;
  }
  return 0;
}

static int
lc_pouch_txn_attachment_ops_delete_object(const lc_pouch_object_list *ops,
                                          const lc_pouch_object_info *object) {
  size_t index;

  if (ops == NULL || object == NULL) {
    return 0;
  }
  for (index = 0U; index < ops->count; ++index) {
    if (lc_pouch_txn_attachment_op_deletes_object(ops->items[index].name,
                                                  object)) {
      return 1;
    }
  }
  return 0;
}

static int lc_pouch_txn_attachment_hidden(const lc_pouch_object_info *object,
                                          const lc_pouch_object_list *staged,
                                          const lc_pouch_object_list *ops,
                                          int has_clear) {
  if (object == NULL) {
    return 1;
  }
  if (has_clear) {
    return 1;
  }
  if (lc_pouch_txn_attachment_ops_delete_object(ops, object)) {
    return 1;
  }
  if (lc_pouch_object_list_has_name(staged, object->name)) {
    return 1;
  }
  return 0;
}

static int lc_pouch_attachment_info_compare(const void *left,
                                            const void *right) {
  const lc_attachment_info *left_info;
  const lc_attachment_info *right_info;

  left_info = (const lc_attachment_info *)left;
  right_info = (const lc_attachment_info *)right;
  if (left_info->name == NULL && right_info->name == NULL) {
    return 0;
  }
  if (left_info->name == NULL) {
    return -1;
  }
  if (right_info->name == NULL) {
    return 1;
  }
  return strcmp(left_info->name, right_info->name);
}

static int lc_pouch_lease_load_staged_attachment_view(
    lc_lease_handle *lease, lc_pouch_meta_record *record,
    lc_pouch_object_list *live, lc_pouch_object_list *staged,
    lc_pouch_object_list *ops, int *has_clear, char **staged_key,
    lc_error *error) {
  lc_pouch_allocator *allocator;
  lc_lease_ref ref;
  char *ops_key;
  int rc;

  if (lease == NULL || record == NULL || live == NULL || staged == NULL ||
      ops == NULL || has_clear == NULL || staged_key == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged attachment view requires lease and "
                        "outputs",
                        NULL, NULL, NULL);
  }
  allocator = &lease->client->pouch_allocator;
  memset(record, 0, sizeof(*record));
  memset(live, 0, sizeof(*live));
  memset(staged, 0, sizeof(*staged));
  memset(ops, 0, sizeof(*ops));
  memset(&ref, 0, sizeof(ref));
  *has_clear = 0;
  *staged_key = NULL;
  ops_key = NULL;

  ref.namespace_name = lease->namespace_name;
  ref.key = lease->key;
  ref.lease_id = lease->lease_id;
  ref.txn_id = lease->txn_id;
  ref.fencing_token = lease->fencing_token;
  rc = lc_pouch_validate_active_lease(lease->client, &ref, record, error);
  if (rc != LC_OK) {
    return rc;
  }

  *staged_key = lc_pouch_txn_attachment_stage_key(lease->client, lease->key,
                                                  lease->txn_id, error);
  if (*staged_key == NULL) {
    lc_pouch_meta_record_cleanup(allocator, record);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  ops_key = lc_pouch_txn_attachment_ops_key(lease->client, lease->key,
                                            lease->txn_id, error);
  if (ops_key == NULL) {
    lc_client_free(lease->client, *staged_key);
    *staged_key = NULL;
    lc_pouch_meta_record_cleanup(allocator, record);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }

  rc = lease->client->pouch_store->list_objects(lease->client->pouch_store,
                                                record->namespace_name,
                                                lease->key, live, error);
  if (rc == LC_OK) {
    rc = lease->client->pouch_store->list_objects(lease->client->pouch_store,
                                                  record->namespace_name,
                                                  *staged_key, staged, error);
  }
  if (rc == LC_OK) {
    rc = lease->client->pouch_store->list_objects(lease->client->pouch_store,
                                                  record->namespace_name,
                                                  ops_key, ops, error);
  }
  lc_client_free(lease->client, ops_key);
  if (rc != LC_OK) {
    lc_pouch_object_list_cleanup(allocator, ops);
    lc_pouch_object_list_cleanup(allocator, staged);
    lc_pouch_object_list_cleanup(allocator, live);
    lc_client_free(lease->client, *staged_key);
    *staged_key = NULL;
    lc_pouch_meta_record_cleanup(allocator, record);
    return rc;
  }
  *has_clear = lc_pouch_txn_attachment_ops_have_clear(ops);
  return LC_OK;
}

static void lc_pouch_lease_staged_attachment_view_cleanup(
    lc_lease_handle *lease, lc_pouch_meta_record *record,
    lc_pouch_object_list *live, lc_pouch_object_list *staged,
    lc_pouch_object_list *ops, char *staged_key) {
  lc_pouch_allocator *allocator;

  if (lease == NULL) {
    return;
  }
  allocator = &lease->client->pouch_allocator;
  lc_pouch_object_list_cleanup(allocator, ops);
  lc_pouch_object_list_cleanup(allocator, staged);
  lc_pouch_object_list_cleanup(allocator, live);
  lc_pouch_meta_record_cleanup(allocator, record);
  lc_client_free(lease->client, staged_key);
}

static int lc_pouch_lease_staged_list_attachments(lc_lease_handle *lease,
                                                  lc_attachment_list *out,
                                                  lc_error *error) {
  lc_pouch_meta_record record;
  lc_pouch_object_list live;
  lc_pouch_object_list staged;
  lc_pouch_object_list ops;
  char *staged_key;
  size_t index;
  size_t out_index;
  size_t visible_count;
  int has_clear;
  int rc;

  if (lease == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged list_attachments requires lease and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  rc = lc_pouch_lease_load_staged_attachment_view(
      lease, &record, &live, &staged, &ops, &has_clear, &staged_key, error);
  if (rc != LC_OK) {
    return rc;
  }

  visible_count = staged.count;
  for (index = 0U; index < live.count; ++index) {
    if (!lc_pouch_txn_attachment_hidden(&live.items[index], &staged, &ops,
                                        has_clear)) {
      ++visible_count;
    }
  }
  if (visible_count > 0U) {
    out->items = (lc_attachment_info *)lc_calloc_local(visible_count,
                                                       sizeof(out->items[0]));
    if (out->items == NULL) {
      lc_pouch_lease_staged_attachment_view_cleanup(lease, &record, &live,
                                                    &staged, &ops, staged_key);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch staged attachment list",
                          NULL, NULL, NULL);
    }
    out->count = visible_count;
    out_index = 0U;
    for (index = 0U; index < live.count; ++index) {
      if (lc_pouch_txn_attachment_hidden(&live.items[index], &staged, &ops,
                                         has_clear)) {
        continue;
      }
      rc = lc_pouch_copy_attachment_info(&out->items[out_index],
                                         &live.items[index], error);
      if (rc != LC_OK) {
        lc_attachment_list_cleanup(out);
        lc_pouch_lease_staged_attachment_view_cleanup(
            lease, &record, &live, &staged, &ops, staged_key);
        return rc;
      }
      ++out_index;
    }
    for (index = 0U; index < staged.count; ++index) {
      rc = lc_pouch_copy_attachment_info(&out->items[out_index],
                                         &staged.items[index], error);
      if (rc != LC_OK) {
        lc_attachment_list_cleanup(out);
        lc_pouch_lease_staged_attachment_view_cleanup(
            lease, &record, &live, &staged, &ops, staged_key);
        return rc;
      }
      ++out_index;
    }
    qsort(out->items, out->count, sizeof(out->items[0]),
          lc_pouch_attachment_info_compare);
  }

  lc_pouch_lease_staged_attachment_view_cleanup(lease, &record, &live, &staged,
                                                &ops, staged_key);
  return LC_OK;
}

static int lc_pouch_staged_not_found(lc_error *error) {
  return lc_error_set(error, LC_ERR_SERVER, 404L,
                      "pouch attachment was not found", NULL, "not_found",
                      NULL);
}

static int lc_pouch_lease_staged_get_attachment(
    lc_lease_handle *lease, const lc_attachment_get_req *req, lc_sink *dst,
    lc_attachment_get_res *out, lc_error *error) {
  lc_pouch_meta_record record;
  lc_pouch_object_list live;
  lc_pouch_object_list staged;
  lc_pouch_object_list ops;
  lc_pouch_object_selector object_selector;
  lc_pouch_object_info object;
  lc_source *body;
  char *staged_key;
  size_t index;
  int has_clear;
  int rc;

  if (lease == NULL || req == NULL || dst == NULL || out == NULL ||
      (req->selector.id == NULL && req->selector.name == NULL)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged get_attachment requires lease, req, "
                        "dst, out, and selector",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  memset(&object_selector, 0, sizeof(object_selector));
  memset(&object, 0, sizeof(object));
  body = NULL;
  rc = lc_pouch_lease_load_staged_attachment_view(
      lease, &record, &live, &staged, &ops, &has_clear, &staged_key, error);
  if (rc != LC_OK) {
    return rc;
  }

  object_selector.id = req->selector.id;
  object_selector.name = req->selector.name;
  rc = lease->client->pouch_store->get_object(
      lease->client->pouch_store, record.namespace_name, staged_key,
      &object_selector, &body, &object, error);
  if (rc == LC_ERR_SERVER && error != NULL && error->http_status == 404L) {
    lc_error_cleanup(error);
    rc = LC_OK;
  } else if (rc == LC_OK) {
    rc = lc_pouch_copy_source_to_sink(body, dst, error);
    if (body != NULL) {
      body->close(body);
      body = NULL;
    }
    if (rc == LC_OK) {
      rc = lc_pouch_copy_attachment_info(&out->attachment, &object, error);
    }
    lc_pouch_object_info_cleanup(&lease->client->pouch_allocator, &object);
    lc_pouch_lease_staged_attachment_view_cleanup(lease, &record, &live,
                                                  &staged, &ops, staged_key);
    return rc;
  }
  if (body != NULL) {
    body->close(body);
    body = NULL;
  }
  lc_pouch_object_info_cleanup(&lease->client->pouch_allocator, &object);
  if (rc != LC_OK) {
    lc_pouch_lease_staged_attachment_view_cleanup(lease, &record, &live,
                                                  &staged, &ops, staged_key);
    return rc;
  }

  for (index = 0U; index < live.count; ++index) {
    if (!lc_pouch_object_matches_selector(&live.items[index], &req->selector)) {
      continue;
    }
    if (lc_pouch_txn_attachment_hidden(&live.items[index], &staged, &ops,
                                       has_clear)) {
      lc_pouch_lease_staged_attachment_view_cleanup(lease, &record, &live,
                                                    &staged, &ops, staged_key);
      return lc_pouch_staged_not_found(error);
    }
    rc = lease->client->pouch_store->get_object(
        lease->client->pouch_store, record.namespace_name, lease->key,
        &object_selector, &body, &object, error);
    if (rc == LC_OK) {
      rc = lc_pouch_copy_source_to_sink(body, dst, error);
    }
    if (body != NULL) {
      body->close(body);
      body = NULL;
    }
    if (rc == LC_OK) {
      rc = lc_pouch_copy_attachment_info(&out->attachment, &object, error);
    }
    lc_pouch_object_info_cleanup(&lease->client->pouch_allocator, &object);
    lc_pouch_lease_staged_attachment_view_cleanup(lease, &record, &live,
                                                  &staged, &ops, staged_key);
    return rc;
  }

  lc_pouch_lease_staged_attachment_view_cleanup(lease, &record, &live, &staged,
                                                &ops, staged_key);
  return lc_pouch_staged_not_found(error);
}

static int
lc_pouch_lease_staged_delete_attachment(lc_lease_handle *lease,
                                        const lc_attachment_selector *selector,
                                        int *deleted, lc_error *error) {
  lc_pouch_meta_record record;
  lc_pouch_object_list live_objects;
  lc_pouch_object_list staged_objects;
  lc_pouch_object_selector object_selector;
  lc_pouch_allocator *allocator;
  lc_lease_ref ref;
  char *staged_key;
  char *op_name;
  size_t index;
  int live_match;
  int staged_deleted;
  int rc;

  if (lease == NULL || selector == NULL || deleted == NULL ||
      (selector->id == NULL && selector->name == NULL)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged delete attachment requires lease, "
                        "selector, and deleted",
                        NULL, NULL, NULL);
  }
  allocator = &lease->client->pouch_allocator;
  *deleted = 0;
  memset(&record, 0, sizeof(record));
  memset(&live_objects, 0, sizeof(live_objects));
  memset(&staged_objects, 0, sizeof(staged_objects));
  memset(&object_selector, 0, sizeof(object_selector));
  memset(&ref, 0, sizeof(ref));
  staged_key = NULL;
  op_name = NULL;

  ref.namespace_name = lease->namespace_name;
  ref.key = lease->key;
  ref.lease_id = lease->lease_id;
  ref.txn_id = lease->txn_id;
  ref.fencing_token = lease->fencing_token;
  rc = lc_pouch_validate_active_lease(lease->client, &ref, &record, error);
  if (rc != LC_OK) {
    return rc;
  }

  staged_key = lc_pouch_txn_attachment_stage_key(lease->client, lease->key,
                                                 lease->txn_id, error);
  if (staged_key == NULL) {
    lc_pouch_meta_record_cleanup(allocator, &record);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }

  rc = lease->client->pouch_store->list_objects(
      lease->client->pouch_store, record.namespace_name, staged_key,
      &staged_objects, error);
  if (rc != LC_OK) {
    lc_client_free(lease->client, staged_key);
    lc_pouch_meta_record_cleanup(allocator, &record);
    return rc;
  }
  for (index = 0U; index < staged_objects.count; ++index) {
    if (lc_pouch_object_matches_selector(&staged_objects.items[index],
                                         selector)) {
      object_selector.id = selector->id;
      object_selector.name = selector->name;
      staged_deleted = 0;
      rc = lease->client->pouch_store->delete_object(
          lease->client->pouch_store, record.namespace_name, staged_key,
          &object_selector, &staged_deleted, error);
      if (rc != LC_OK) {
        lc_pouch_object_list_cleanup(allocator, &staged_objects);
        lc_client_free(lease->client, staged_key);
        lc_pouch_meta_record_cleanup(allocator, &record);
        return rc;
      }
      if (staged_deleted) {
        *deleted = 1;
      }
      break;
    }
  }
  lc_pouch_object_list_cleanup(allocator, &staged_objects);

  rc = lease->client->pouch_store->list_objects(
      lease->client->pouch_store, record.namespace_name, lease->key,
      &live_objects, error);
  if (rc != LC_OK) {
    lc_client_free(lease->client, staged_key);
    lc_pouch_meta_record_cleanup(allocator, &record);
    return rc;
  }
  live_match = 0;
  for (index = 0U; index < live_objects.count; ++index) {
    if (lc_pouch_object_matches_selector(&live_objects.items[index],
                                         selector)) {
      live_match = 1;
      break;
    }
  }
  lc_pouch_object_list_cleanup(allocator, &live_objects);
  if (live_match) {
    if (selector->id != NULL) {
      op_name = lc_pouch_txn_attachment_op_name(
          lease->client, LC_POUCH_TXN_ATTACHMENT_OP_DELETE_ID, selector->id,
          error);
    } else {
      op_name = lc_pouch_txn_attachment_op_name(
          lease->client, LC_POUCH_TXN_ATTACHMENT_OP_DELETE_NAME, selector->name,
          error);
    }
    if (op_name == NULL) {
      lc_client_free(lease->client, staged_key);
      lc_pouch_meta_record_cleanup(allocator, &record);
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    rc = lc_pouch_txn_stage_attachment_op(lease->client, record.namespace_name,
                                          lease->key, lease->txn_id, op_name,
                                          error);
    lc_client_free(lease->client, op_name);
    if (rc != LC_OK) {
      lc_client_free(lease->client, staged_key);
      lc_pouch_meta_record_cleanup(allocator, &record);
      return rc;
    }
    *deleted = 1;
  }

  lc_client_free(lease->client, staged_key);
  lc_pouch_meta_record_cleanup(allocator, &record);
  return LC_OK;
}

static int lc_pouch_lease_staged_delete_all_attachments(lc_lease_handle *lease,
                                                        int *deleted_count,
                                                        lc_error *error) {
  lc_pouch_meta_record record;
  lc_pouch_object_list live_objects;
  lc_pouch_allocator *allocator;
  lc_lease_ref ref;
  char *staged_key;
  int staged_deleted_count;
  int rc;

  if (lease == NULL || deleted_count == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged delete_all_attachments requires lease "
                        "and deleted_count",
                        NULL, NULL, NULL);
  }
  allocator = &lease->client->pouch_allocator;
  *deleted_count = 0;
  memset(&record, 0, sizeof(record));
  memset(&live_objects, 0, sizeof(live_objects));
  memset(&ref, 0, sizeof(ref));
  staged_key = NULL;

  ref.namespace_name = lease->namespace_name;
  ref.key = lease->key;
  ref.lease_id = lease->lease_id;
  ref.txn_id = lease->txn_id;
  ref.fencing_token = lease->fencing_token;
  rc = lc_pouch_validate_active_lease(lease->client, &ref, &record, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lease->client->pouch_store->list_objects(
      lease->client->pouch_store, record.namespace_name, lease->key,
      &live_objects, error);
  if (rc != LC_OK) {
    lc_pouch_meta_record_cleanup(allocator, &record);
    return rc;
  }
  if (live_objects.count > (size_t)2147483647L) {
    lc_pouch_object_list_cleanup(allocator, &live_objects);
    lc_pouch_meta_record_cleanup(allocator, &record);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged attachment count is too large", NULL,
                        NULL, NULL);
  }
  *deleted_count = (int)live_objects.count;
  lc_pouch_object_list_cleanup(allocator, &live_objects);

  staged_key = lc_pouch_txn_attachment_stage_key(lease->client, lease->key,
                                                 lease->txn_id, error);
  if (staged_key == NULL) {
    lc_pouch_meta_record_cleanup(allocator, &record);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  staged_deleted_count = 0;
  rc = lease->client->pouch_store->delete_all_objects(
      lease->client->pouch_store, record.namespace_name, staged_key,
      &staged_deleted_count, error);
  lc_client_free(lease->client, staged_key);
  if (rc != LC_OK) {
    lc_pouch_meta_record_cleanup(allocator, &record);
    return rc;
  }
  *deleted_count += staged_deleted_count;
  if (*deleted_count > staged_deleted_count) {
    rc = lc_pouch_txn_stage_attachment_op(
        lease->client, record.namespace_name, lease->key, lease->txn_id,
        LC_POUCH_TXN_ATTACHMENT_OP_CLEAR, error);
  }
  lc_pouch_meta_record_cleanup(allocator, &record);
  return rc;
}

int lc_pouch_client_list_attachments_method(lc_client *self,
                                            const lc_attachment_list_req *req,
                                            lc_attachment_list *out,
                                            lc_error *error) {
  lc_client_handle *client;
  lc_pouch_meta_record record;
  lc_pouch_object_list objects;
  lc_pouch_allocator *allocator;
  const char *namespace_name;
  size_t index;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch list_attachments requires self, req, and out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  allocator = &client->pouch_allocator;
  memset(out, 0, sizeof(*out));
  memset(&record, 0, sizeof(record));
  memset(&objects, 0, sizeof(objects));
  namespace_name = NULL;
  if (!req->public_read && lc_pouch_lease_ref_is_explicit_txn(&req->lease)) {
    lc_lease_handle txn_lease;

    memset(&txn_lease, 0, sizeof(txn_lease));
    rc = lc_pouch_stack_lease_from_ref(client, &req->lease, &txn_lease, &record,
                                       error);
    if (rc != LC_OK) {
      return rc;
    }
    rc = lc_pouch_lease_staged_list_attachments(&txn_lease, out, error);
    lc_pouch_meta_record_cleanup(allocator, &record);
    return rc;
  }
  if (req->public_read) {
    if (req->lease.key == NULL) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch public list_attachments requires lease key",
                          NULL, NULL, NULL);
    }
    rc = lc_pouch_public_namespace(client, req->lease.namespace_name,
                                   &namespace_name, error);
    if (rc != LC_OK) {
      return rc;
    }
  } else {
    rc = lc_pouch_validate_active_lease(client, &req->lease, &record, error);
    if (rc != LC_OK) {
      return rc;
    }
    namespace_name = record.namespace_name;
  }
  rc = client->pouch_store->list_objects(client->pouch_store, namespace_name,
                                         req->lease.key, &objects, error);
  if (rc == LC_OK && objects.count > 0U) {
    out->items = (lc_attachment_info *)lc_calloc_local(objects.count,
                                                       sizeof(out->items[0]));
    if (out->items == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch attachment list", NULL, NULL,
                        NULL);
    } else {
      out->count = objects.count;
      for (index = 0U; index < objects.count; ++index) {
        rc = lc_pouch_copy_attachment_info(&out->items[index],
                                           &objects.items[index], error);
        if (rc != LC_OK) {
          lc_attachment_list_cleanup(out);
          break;
        }
      }
    }
  }
  lc_pouch_object_list_cleanup(allocator, &objects);
  lc_pouch_meta_record_cleanup(allocator, &record);
  return rc;
}

int lc_pouch_client_get_attachment_method(lc_client *self,
                                          const lc_attachment_get_op *req,
                                          lc_sink *dst,
                                          lc_attachment_get_res *out,
                                          lc_error *error) {
  lc_client_handle *client;
  lc_pouch_meta_record record;
  lc_pouch_object_selector selector;
  lc_pouch_object_info object;
  lc_source *body;
  lc_pouch_allocator *allocator;
  const char *namespace_name;
  int rc;

  if (self == NULL || req == NULL || dst == NULL || out == NULL ||
      (req->selector.id == NULL && req->selector.name == NULL)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch get_attachment requires self, req, dst, out, "
                        "and selector id or name",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  allocator = &client->pouch_allocator;
  memset(out, 0, sizeof(*out));
  memset(&record, 0, sizeof(record));
  memset(&selector, 0, sizeof(selector));
  memset(&object, 0, sizeof(object));
  body = NULL;
  namespace_name = NULL;
  if (!req->public_read && lc_pouch_lease_ref_is_explicit_txn(&req->lease)) {
    lc_lease_handle txn_lease;
    lc_attachment_get_req staged_req;

    memset(&txn_lease, 0, sizeof(txn_lease));
    memset(&staged_req, 0, sizeof(staged_req));
    rc = lc_pouch_stack_lease_from_ref(client, &req->lease, &txn_lease, &record,
                                       error);
    if (rc != LC_OK) {
      return rc;
    }
    staged_req.selector = req->selector;
    staged_req.public_read = req->public_read;
    rc = lc_pouch_lease_staged_get_attachment(&txn_lease, &staged_req, dst, out,
                                              error);
    lc_pouch_meta_record_cleanup(allocator, &record);
    return rc;
  }
  if (req->public_read) {
    if (req->lease.key == NULL) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch public get_attachment requires lease key",
                          NULL, NULL, NULL);
    }
    rc = lc_pouch_public_namespace(client, req->lease.namespace_name,
                                   &namespace_name, error);
    if (rc != LC_OK) {
      return rc;
    }
  } else {
    rc = lc_pouch_validate_active_lease(client, &req->lease, &record, error);
    if (rc != LC_OK) {
      return rc;
    }
    namespace_name = record.namespace_name;
  }
  selector.id = req->selector.id;
  selector.name = req->selector.name;
  rc = client->pouch_store->get_object(client->pouch_store, namespace_name,
                                       req->lease.key, &selector, &body,
                                       &object, error);
  if (rc == LC_OK) {
    rc = lc_pouch_copy_source_to_sink(body, dst, error);
  }
  if (body != NULL) {
    body->close(body);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_copy_attachment_info(&out->attachment, &object, error);
  }
  lc_pouch_object_info_cleanup(allocator, &object);
  lc_pouch_meta_record_cleanup(allocator, &record);
  return rc;
}

int lc_pouch_client_delete_attachment_method(lc_client *self,
                                             const lc_attachment_delete_op *req,
                                             int *deleted, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_meta_record record;
  lc_pouch_store_meta_res stored;
  lc_pouch_object_selector selector;
  lc_pouch_meta next_meta;
  lc_pouch_allocator *allocator;
  int rc;

  if (self == NULL || req == NULL || deleted == NULL ||
      (req->selector.id == NULL && req->selector.name == NULL)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch delete_attachment requires self, req, deleted, "
                        "and selector id or name",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  allocator = &client->pouch_allocator;
  *deleted = 0;
  memset(&record, 0, sizeof(record));
  memset(&stored, 0, sizeof(stored));
  memset(&selector, 0, sizeof(selector));
  if (lc_pouch_lease_ref_is_explicit_txn(&req->lease)) {
    lc_lease_handle txn_lease;

    memset(&txn_lease, 0, sizeof(txn_lease));
    rc = lc_pouch_stack_lease_from_ref(client, &req->lease, &txn_lease, &record,
                                       error);
    if (rc != LC_OK) {
      return rc;
    }
    rc = lc_pouch_lease_staged_delete_attachment(&txn_lease, &req->selector,
                                                 deleted, error);
    lc_pouch_meta_record_cleanup(allocator, &record);
    return rc;
  }
  rc = lc_pouch_validate_active_lease(client, &req->lease, &record, error);
  if (rc != LC_OK) {
    return rc;
  }
  selector.id = req->selector.id;
  selector.name = req->selector.name;
  rc = client->pouch_store->delete_object(client->pouch_store,
                                          record.namespace_name, req->lease.key,
                                          &selector, deleted, error);
  if (rc == LC_OK && *deleted) {
    next_meta = record.meta;
    next_meta.version = record.meta.version + 1L;
    rc = client->pouch_store->store_meta(
        client->pouch_store, record.namespace_name, req->lease.key, &next_meta,
        record.etag, &stored, error);
  }
  lc_pouch_store_meta_res_cleanup(allocator, &stored);
  lc_pouch_meta_record_cleanup(allocator, &record);
  return rc;
}

int lc_pouch_client_delete_all_attachments_method(
    lc_client *self, const lc_attachment_delete_all_op *req, int *deleted_count,
    lc_error *error) {
  lc_client_handle *client;
  lc_pouch_meta_record record;
  lc_pouch_store_meta_res stored;
  lc_pouch_meta next_meta;
  lc_pouch_allocator *allocator;
  int rc;

  if (self == NULL || req == NULL || deleted_count == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch delete_all_attachments requires self, req, and "
                        "deleted_count",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  allocator = &client->pouch_allocator;
  *deleted_count = 0;
  memset(&record, 0, sizeof(record));
  memset(&stored, 0, sizeof(stored));
  if (lc_pouch_lease_ref_is_explicit_txn(&req->lease)) {
    lc_lease_handle txn_lease;

    memset(&txn_lease, 0, sizeof(txn_lease));
    rc = lc_pouch_stack_lease_from_ref(client, &req->lease, &txn_lease, &record,
                                       error);
    if (rc != LC_OK) {
      return rc;
    }
    rc = lc_pouch_lease_staged_delete_all_attachments(&txn_lease, deleted_count,
                                                      error);
    lc_pouch_meta_record_cleanup(allocator, &record);
    return rc;
  }
  rc = lc_pouch_validate_active_lease(client, &req->lease, &record, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = client->pouch_store->delete_all_objects(
      client->pouch_store, record.namespace_name, req->lease.key, deleted_count,
      error);
  if (rc == LC_OK && *deleted_count > 0) {
    next_meta = record.meta;
    next_meta.version = record.meta.version + 1L;
    rc = client->pouch_store->store_meta(
        client->pouch_store, record.namespace_name, req->lease.key, &next_meta,
        record.etag, &stored, error);
  }
  lc_pouch_store_meta_res_cleanup(allocator, &stored);
  lc_pouch_meta_record_cleanup(allocator, &record);
  return rc;
}

int lc_pouch_client_queue_stats_method(lc_client *self,
                                       const lc_queue_stats_req *req,
                                       lc_queue_stats_res *out,
                                       lc_error *error) {
  lc_client_handle *client;
  lc_pouch_queue_stats stats;
  const char *namespace_name;
  int rc;

  if (self == NULL || req == NULL || req->queue == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue_stats requires self, req, queue, and out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  namespace_name = NULL;
  rc = lc_pouch_public_namespace(client, req->namespace_name, &namespace_name,
                                 error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(out, 0, sizeof(*out));
  memset(&stats, 0, sizeof(stats));
  rc = client->pouch_store->queue_stats(client->pouch_store, namespace_name,
                                        req->queue, &stats, error);
  if (rc == LC_OK) {
    if (lc_pouch_copy_public(&out->namespace_name, namespace_name, error,
                             "failed to copy pouch namespace") != LC_OK ||
        lc_pouch_copy_public(&out->queue, req->queue, error,
                             "failed to copy pouch queue") != LC_OK ||
        lc_pouch_copy_public(&out->head_message_id, stats.head_message_id,
                             error,
                             "failed to copy pouch queue head id") != LC_OK ||
        lc_pouch_copy_public(&out->correlation_id, stats.correlation_id, error,
                             "failed to copy pouch queue stats correlation "
                             "id") != LC_OK) {
      lc_queue_stats_res_cleanup(out);
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    } else {
      out->available = stats.available;
      out->pending_candidates = stats.pending_candidates;
      out->head_enqueued_at_unix = stats.head_enqueued_at_unix;
      out->head_not_visible_until_unix = stats.head_not_visible_until_unix;
      out->head_age_seconds =
          stats.head_enqueued_at_unix > 0L
              ? lc_pouch_now_unix() - stats.head_enqueued_at_unix
              : 0L;
    }
  }
  lc_pouch_queue_stats_cleanup(&client->pouch_allocator, &stats);
  return rc;
}

int lc_pouch_client_watch_queue_method(lc_client *self,
                                       const lc_watch_queue_req *req,
                                       const lc_watch_handler *handler,
                                       lc_error *error) {
  lc_client_handle *client;
  lc_pouch_queue_stats stats;
  lc_watch_event event;
  lc_error handler_error;
  const char *namespace_name;
  int handler_rc;
  int rc;

  if (self == NULL || req == NULL || req->queue == NULL || handler == NULL ||
      handler->handle == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch watch_queue requires self, req, queue, and "
                        "handler",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  namespace_name = NULL;
  rc = lc_pouch_public_namespace(client, req->namespace_name, &namespace_name,
                                 error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&stats, 0, sizeof(stats));
  memset(&event, 0, sizeof(event));
  rc = client->pouch_store->queue_stats(client->pouch_store, namespace_name,
                                        req->queue, &stats, error);
  if (rc != LC_OK) {
    lc_pouch_queue_stats_cleanup(&client->pouch_allocator, &stats);
    return rc;
  }
  if (lc_pouch_copy_public(&event.namespace_name, namespace_name, error,
                           "failed to copy pouch watch namespace") != LC_OK ||
      lc_pouch_copy_public(&event.queue, req->queue, error,
                           "failed to copy pouch watch queue") != LC_OK ||
      lc_pouch_copy_public(&event.head_message_id, stats.head_message_id, error,
                           "failed to copy pouch watch head id") != LC_OK ||
      lc_pouch_copy_public(&event.correlation_id, "pouch-watch", error,
                           "failed to copy pouch watch correlation id") !=
          LC_OK) {
    lc_watch_event_cleanup(&event);
    lc_pouch_queue_stats_cleanup(&client->pouch_allocator, &stats);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  event.available = stats.available > 0;
  event.changed_at_unix = lc_pouch_now_unix();
  lc_error_init(&handler_error);
  handler_rc = handler->handle(handler->context, &event, &handler_error);
  lc_watch_event_cleanup(&event);
  lc_pouch_queue_stats_cleanup(&client->pouch_allocator, &stats);
  if (handler_error.code != LC_OK) {
    rc = lc_error_set(error, handler_error.code, handler_error.http_status,
                      handler_error.message, handler_error.detail,
                      handler_error.server_code, handler_error.correlation_id);
    lc_error_cleanup(&handler_error);
    return rc;
  }
  lc_error_cleanup(&handler_error);
  if (handler_rc != LC_OK) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "queue watch handler failed", NULL, NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_client_unsupported(lc_error *error, const char *message) {
  return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL, NULL);
}

static int lc_pouch_query_selector_error_or_unsupported(lc_error *error,
                                                        const char *message) {
  if (error != NULL && error->code != LC_OK && error->message != NULL) {
    return error->code;
  }
  return lc_pouch_client_unsupported(error, message);
}

static int64_t lc_pouch_now_millis(void) {
  struct timespec ts;

  if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
    return 0;
  }
  return (int64_t)ts.tv_sec * 1000 + (int64_t)ts.tv_nsec / 1000000;
}

static void lc_pouch_sleep_millis(long millis) {
  struct timespec req;

  if (millis <= 0L) {
    return;
  }
  req.tv_sec = millis / 1000L;
  req.tv_nsec = (millis % 1000L) * 1000000L;
  while (nanosleep(&req, &req) != 0 && errno == EINTR) {
  }
}

static int lc_pouch_query_engine_supported(const char *value) {
  return value == NULL || value[0] == '\0' || strcmp(value, "index") == 0 ||
         strcmp(value, "scan") == 0;
}

static int lc_pouch_validate_query_engine(const lc_query_req *req,
                                          lc_error *error) {
  if (req != NULL && !lc_pouch_query_engine_supported(req->engine)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query engine must be index or scan", NULL, NULL,
                        NULL);
  }
  return LC_OK;
}

static int lc_pouch_index_refresh_is_wait_for(const char *refresh) {
  return refresh != NULL && strcmp(refresh, "wait_for") == 0;
}

static int lc_pouch_index_refresh_supported(const char *refresh) {
  return refresh == NULL || refresh[0] == '\0' ||
         lc_pouch_index_refresh_is_wait_for(refresh);
}

static int lc_pouch_wait_for_index(lc_client_handle *client,
                                   const char *namespace_name,
                                   lc_error *error) {
  lc_pouch_index_flush_res flush_res;
  int rc;

  if (client == NULL || client->pouch_store == NULL ||
      client->pouch_store->flush_index == NULL) {
    return lc_pouch_client_unsupported(
        error, "pouch indexed query refresh is not available");
  }
  memset(&flush_res, 0, sizeof(flush_res));
  rc = client->pouch_store->flush_index(client->pouch_store, namespace_name,
                                        "wait", &flush_res, error);
  lc_pouch_index_flush_res_cleanup(&client->pouch_allocator, &flush_res);
  return rc;
}

static const char *lc_pouch_effective_query_engine(lc_client_handle *client,
                                                   const lc_query_req *req) {
  if (req != NULL && req->engine != NULL && req->engine[0] != '\0') {
    return req->engine;
  }
  if (client != NULL && client->pouch_query_engine != NULL &&
      strcmp(client->pouch_query_engine, "scan") == 0) {
    if (client->pouch_query_fallback_engine != NULL &&
        strcmp(client->pouch_query_fallback_engine, "index") == 0 &&
        req != NULL && lc_pouch_index_refresh_is_wait_for(req->refresh)) {
      return "index";
    }
    return "scan";
  }
  return "index";
}

typedef enum lc_pouch_query_selector_kind {
  LC_POUCH_QUERY_SELECTOR_UNSUPPORTED = 0,
  LC_POUCH_QUERY_SELECTOR_MATCH_ALL = 1,
  LC_POUCH_QUERY_SELECTOR_OWNER = 2,
  LC_POUCH_QUERY_SELECTOR_KEY = 3,
  LC_POUCH_QUERY_SELECTOR_KEY_OWNER = 4
} lc_pouch_query_selector_kind;

typedef struct lc_pouch_query_exact_selector_json {
  char *key;
  char *owner;
} lc_pouch_query_exact_selector_json;

static const lonejson_field lc_pouch_query_exact_selector_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_pouch_query_exact_selector_json, key, "key"),
    LONEJSON_FIELD_STRING_ALLOC(lc_pouch_query_exact_selector_json, owner,
                                "owner")};

LONEJSON_MAP_DEFINE(lc_pouch_query_exact_selector_map,
                    lc_pouch_query_exact_selector_json,
                    lc_pouch_query_exact_selector_fields);

typedef struct lc_pouch_metadata_selector_field_visit {
  int root_is_object;
  int valid;
  int top_key_active;
  int saw_top_key;
  char top_key[16];
  size_t top_key_len;
} lc_pouch_metadata_selector_field_visit;

typedef struct lc_pouch_lql_operator_field_visit {
  int root_is_object;
  int key_active;
  int saw_lql_key;
  char key[16];
  size_t key_len;
} lc_pouch_lql_operator_field_visit;

static int lc_pouch_selector_key_is_lql_operator(const char *key) {
  return strcmp(key, "all") == 0 || strcmp(key, "and") == 0 ||
         strcmp(key, "or") == 0 || strcmp(key, "not") == 0 ||
         strcmp(key, "eq") == 0 || strcmp(key, "in") == 0 ||
         strcmp(key, "exists") == 0 || strcmp(key, "gt") == 0 ||
         strcmp(key, "gte") == 0 || strcmp(key, "lt") == 0 ||
         strcmp(key, "lte") == 0 || strcmp(key, "before") == 0 ||
         strcmp(key, "after") == 0 || strcmp(key, "since") == 0;
}

static lonejson_status lc_pouch_metadata_selector_root_object_begin(
    void *user, const lonejson_value_path *path, lonejson_error *error) {
  lc_pouch_metadata_selector_field_visit *visit;

  (void)error;
  visit = (lc_pouch_metadata_selector_field_visit *)user;
  if (visit != NULL && path != NULL && path->segment_count == 0U) {
    visit->root_is_object = 1;
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_pouch_metadata_selector_root_array_begin(
    void *user, const lonejson_value_path *path, lonejson_error *error) {
  lc_pouch_metadata_selector_field_visit *visit;

  (void)error;
  visit = (lc_pouch_metadata_selector_field_visit *)user;
  if (visit != NULL && path != NULL && path->segment_count == 0U) {
    visit->valid = 0;
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_pouch_metadata_selector_scalar_at_root(
    void *user, const lonejson_value_path *path, lonejson_error *error) {
  lc_pouch_metadata_selector_field_visit *visit;

  (void)error;
  visit = (lc_pouch_metadata_selector_field_visit *)user;
  if (visit != NULL && path != NULL && path->segment_count == 0U) {
    visit->valid = 0;
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_metadata_selector_bool_at_root(void *user,
                                        const lonejson_value_path *path,
                                        int value, lonejson_error *error) {
  (void)value;
  return lc_pouch_metadata_selector_scalar_at_root(user, path, error);
}

static lonejson_status
lc_pouch_metadata_selector_key_begin(void *user,
                                     const lonejson_value_path *path,
                                     lonejson_error *error) {
  lc_pouch_metadata_selector_field_visit *visit;

  (void)error;
  visit = (lc_pouch_metadata_selector_field_visit *)user;
  if (visit != NULL && path != NULL && path->segment_count == 0U) {
    visit->top_key_active = 1;
    visit->saw_top_key = 1;
    visit->top_key_len = 0U;
    visit->top_key[0] = '\0';
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_metadata_selector_key_chunk(void *user,
                                     const lonejson_value_path *path,
                                     const char *data, size_t len,
                                     lonejson_error *error) {
  lc_pouch_metadata_selector_field_visit *visit;
  size_t index;

  (void)error;
  visit = (lc_pouch_metadata_selector_field_visit *)user;
  if (visit == NULL || path == NULL || path->segment_count != 0U ||
      !visit->top_key_active) {
    return LONEJSON_STATUS_OK;
  }
  for (index = 0U; index < len; ++index) {
    if (visit->top_key_len >= sizeof(visit->top_key) - 1U) {
      visit->valid = 0;
      continue;
    }
    visit->top_key[visit->top_key_len] = data[index];
    ++visit->top_key_len;
  }
  visit->top_key[visit->top_key_len] = '\0';
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_metadata_selector_key_end(void *user, const lonejson_value_path *path,
                                   lonejson_error *error) {
  lc_pouch_metadata_selector_field_visit *visit;

  (void)error;
  visit = (lc_pouch_metadata_selector_field_visit *)user;
  if (visit != NULL && path != NULL && path->segment_count == 0U &&
      visit->top_key_active) {
    visit->top_key_active = 0;
    if (strcmp(visit->top_key, "key") != 0 &&
        strcmp(visit->top_key, "owner") != 0) {
      visit->valid = 0;
    }
  }
  return LONEJSON_STATUS_OK;
}

static int
lc_pouch_metadata_selector_has_only_exact_fields(lonejson *runtime,
                                                 const char *selector_json) {
  lc_pouch_metadata_selector_field_visit visit;
  lonejson_path_value_visitor visitor;
  lonejson_error error;
  lonejson_status status;

  if (runtime == NULL || selector_json == NULL) {
    return 0;
  }
  memset(&visit, 0, sizeof(visit));
  visit.valid = 1;
  visitor = lonejson_default_path_value_visitor();
  visitor.object_begin = lc_pouch_metadata_selector_root_object_begin;
  visitor.array_begin = lc_pouch_metadata_selector_root_array_begin;
  visitor.object_key_begin = lc_pouch_metadata_selector_key_begin;
  visitor.object_key_chunk = lc_pouch_metadata_selector_key_chunk;
  visitor.object_key_end = lc_pouch_metadata_selector_key_end;
  visitor.string_begin = lc_pouch_metadata_selector_scalar_at_root;
  visitor.number_begin = lc_pouch_metadata_selector_scalar_at_root;
  visitor.boolean_value = lc_pouch_metadata_selector_bool_at_root;
  visitor.null_value = lc_pouch_metadata_selector_scalar_at_root;
  lonejson_error_init(&error);
  status = runtime->visit_path_value_cstr(runtime, selector_json, &visitor,
                                          &visit, &error);
  return status == LONEJSON_STATUS_OK && visit.root_is_object && visit.valid;
}

static lonejson_status lc_pouch_lql_operator_root_object_begin(
    void *user, const lonejson_value_path *path, lonejson_error *error) {
  lc_pouch_lql_operator_field_visit *visit;

  (void)error;
  visit = (lc_pouch_lql_operator_field_visit *)user;
  if (visit != NULL && path != NULL && path->segment_count == 0U) {
    visit->root_is_object = 1;
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_lql_operator_key_begin(void *user, const lonejson_value_path *path,
                                lonejson_error *error) {
  lc_pouch_lql_operator_field_visit *visit;

  (void)path;
  (void)error;
  visit = (lc_pouch_lql_operator_field_visit *)user;
  if (visit != NULL) {
    visit->key_active = 1;
    visit->key_len = 0U;
    visit->key[0] = '\0';
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_lql_operator_key_chunk(void *user, const lonejson_value_path *path,
                                const char *data, size_t len,
                                lonejson_error *error) {
  lc_pouch_lql_operator_field_visit *visit;
  size_t index;

  (void)path;
  (void)error;
  visit = (lc_pouch_lql_operator_field_visit *)user;
  if (visit == NULL || !visit->key_active) {
    return LONEJSON_STATUS_OK;
  }
  for (index = 0U; index < len; ++index) {
    if (visit->key_len >= sizeof(visit->key) - 1U) {
      continue;
    }
    visit->key[visit->key_len] = data[index];
    ++visit->key_len;
  }
  visit->key[visit->key_len] = '\0';
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_lql_operator_key_end(void *user, const lonejson_value_path *path,
                              lonejson_error *error) {
  lc_pouch_lql_operator_field_visit *visit;

  (void)path;
  (void)error;
  visit = (lc_pouch_lql_operator_field_visit *)user;
  if (visit != NULL && visit->key_active) {
    visit->key_active = 0;
    if (lc_pouch_selector_key_is_lql_operator(visit->key)) {
      visit->saw_lql_key = 1;
    }
  }
  return LONEJSON_STATUS_OK;
}

static int lc_pouch_selector_json_has_lql_operator(lonejson *runtime,
                                                   const char *selector_json) {
  lc_pouch_lql_operator_field_visit visit;
  lonejson_path_value_visitor visitor;
  lonejson_error error;
  lonejson_status status;

  if (runtime == NULL || selector_json == NULL) {
    return 0;
  }
  memset(&visit, 0, sizeof(visit));
  visitor = lonejson_default_path_value_visitor();
  visitor.object_begin = lc_pouch_lql_operator_root_object_begin;
  visitor.object_key_begin = lc_pouch_lql_operator_key_begin;
  visitor.object_key_chunk = lc_pouch_lql_operator_key_chunk;
  visitor.object_key_end = lc_pouch_lql_operator_key_end;
  lonejson_error_init(&error);
  status = runtime->visit_path_value_cstr(runtime, selector_json, &visitor,
                                          &visit, &error);
  return status == LONEJSON_STATUS_OK && visit.root_is_object &&
         visit.saw_lql_key;
}

static int lc_pouch_lql_status_to_error(lql_status status) {
  switch (status) {
  case LQL_STATUS_OK:
    return LC_OK;
  case LQL_STATUS_NO_MEMORY:
    return LC_ERR_NOMEM;
  case LQL_STATUS_INVALID_ARGUMENT:
  case LQL_STATUS_PARSE_ERROR:
  case LQL_STATUS_JSON_ERROR:
  case LQL_STATUS_UNSUPPORTED:
  default:
    return LC_ERR_INVALID;
  }
}

static int lc_pouch_lql_set_error(lc_error *error, lql_status status,
                                  const lql_error *lql_err,
                                  const char *message) {
  return lc_error_set(error, lc_pouch_lql_status_to_error(status), 0L, message,
                      lql_err != NULL ? lql_err->message : NULL, NULL, NULL);
}

static lc_pouch_query_selector_kind lc_pouch_metadata_selector_classify(
    const lc_pouch_query_exact_selector_json *selector, char **key_out,
    char **owner_out, lc_error *error) {
  char *key_copy;
  char *owner_copy;

  if (selector == NULL) {
    return LC_POUCH_QUERY_SELECTOR_UNSUPPORTED;
  }
  key_copy = NULL;
  owner_copy = NULL;
  if (selector->key != NULL) {
    key_copy = lc_strdup_local(selector->key);
    if (key_copy == NULL) {
      (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                         "failed to allocate pouch query selector value", NULL,
                         NULL, NULL);
      return LC_POUCH_QUERY_SELECTOR_UNSUPPORTED;
    }
  }
  if (selector->owner != NULL) {
    owner_copy = lc_strdup_local(selector->owner);
    if (owner_copy == NULL) {
      lc_free_with_allocator(NULL, key_copy);
      (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                         "failed to allocate pouch query selector value", NULL,
                         NULL, NULL);
      return LC_POUCH_QUERY_SELECTOR_UNSUPPORTED;
    }
  }
  if (key_out != NULL) {
    *key_out = key_copy;
    key_copy = NULL;
  }
  if (owner_out != NULL) {
    *owner_out = owner_copy;
    owner_copy = NULL;
  }
  lc_free_with_allocator(NULL, key_copy);
  lc_free_with_allocator(NULL, owner_copy);
  if (selector->key != NULL && selector->owner != NULL) {
    return LC_POUCH_QUERY_SELECTOR_KEY_OWNER;
  }
  if (selector->key != NULL) {
    return LC_POUCH_QUERY_SELECTOR_KEY;
  }
  if (selector->owner != NULL) {
    return LC_POUCH_QUERY_SELECTOR_OWNER;
  }
  return LC_POUCH_QUERY_SELECTOR_MATCH_ALL;
}

static lc_pouch_query_selector_kind
lc_pouch_query_selector_kind_parse(const char *selector_json, char **key_out,
                                   char **owner_out, lc_error *error) {
  lc_pouch_query_exact_selector_json selector;
  lql *lql_runtime;
  lql_selector *compiled_selector;
  lql_error lql_err;
  lql_status lql_status_value;
  lonejson *runtime;
  lonejson_error lj_error;
  lonejson_status status;
  lc_pouch_query_selector_kind kind;

  if (key_out != NULL) {
    *key_out = NULL;
  }
  if (owner_out != NULL) {
    *owner_out = NULL;
  }
  if (selector_json == NULL) {
    return LC_POUCH_QUERY_SELECTOR_UNSUPPORTED;
  }

  runtime = lc_thread_lonejson_runtime();
  if (runtime == NULL) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch query selector parser", NULL,
                       NULL, NULL);
    return LC_POUCH_QUERY_SELECTOR_UNSUPPORTED;
  }
  if (lc_pouch_selector_json_has_lql_operator(runtime, selector_json)) {
    lql_runtime = NULL;
    compiled_selector = NULL;
    lql_error_init(&lql_err);
    lql_status_value = lql_new(&lql_runtime, &lql_err);
    if (lql_status_value != LQL_STATUS_OK) {
      (void)lc_pouch_lql_set_error(error, lql_status_value, &lql_err,
                                   "failed to allocate pouch LQL runtime");
      return LC_POUCH_QUERY_SELECTOR_UNSUPPORTED;
    }
    lql_error_init(&lql_err);
    lql_status_value = lql_runtime->selector_parse_json(
        lql_runtime, selector_json, strlen(selector_json), &compiled_selector,
        &lql_err);
    if (lql_status_value == LQL_STATUS_OK) {
      lql_runtime->selector_destroy(lql_runtime, compiled_selector);
      lql_runtime->destroy(lql_runtime);
      return LC_POUCH_QUERY_SELECTOR_UNSUPPORTED;
    }
    lql_runtime->destroy(lql_runtime);
    (void)lc_pouch_lql_set_error(error, lql_status_value, &lql_err,
                                 "failed to parse pouch LQL selector");
    return LC_POUCH_QUERY_SELECTOR_UNSUPPORTED;
  }
  if (!lc_pouch_metadata_selector_has_only_exact_fields(runtime,
                                                        selector_json)) {
    return LC_POUCH_QUERY_SELECTOR_UNSUPPORTED;
  }
  memset(&selector, 0, sizeof(selector));
  lonejson_error_init(&lj_error);
  status =
      lc_lonejson_parse_cstr_value(runtime, &lc_pouch_query_exact_selector_map,
                                   &selector, selector_json, &lj_error);
  if (status != LONEJSON_STATUS_OK) {
    lc_lonejson_cleanup_value(runtime, &lc_pouch_query_exact_selector_map,
                              &selector);
    return LC_POUCH_QUERY_SELECTOR_UNSUPPORTED;
  }
  kind = lc_pouch_metadata_selector_classify(&selector, key_out, owner_out,
                                             error);
  lc_lonejson_cleanup_value(runtime, &lc_pouch_query_exact_selector_map,
                            &selector);
  if (kind == LC_POUCH_QUERY_SELECTOR_UNSUPPORTED) {
    if (key_out != NULL) {
      lc_free_with_allocator(NULL, *key_out);
      *key_out = NULL;
    }
    if (owner_out != NULL) {
      lc_free_with_allocator(NULL, *owner_out);
      *owner_out = NULL;
    }
  }
  return kind;
}

typedef struct lc_pouch_query_keys_scan_context {
  const lc_query_key_handler *handler;
  void *handler_context;
  struct lc_pouch_lql_document_filter *filter;
  lc_client_handle *client;
  const char *namespace_name;
} lc_pouch_query_keys_scan_context;

typedef struct lc_pouch_lql_document_filter {
  lql *runtime;
  lql_selector *selector;
  int enabled;
} lc_pouch_lql_document_filter;

typedef struct lc_pouch_query_scan_context {
  lc_client_handle *client;
  lc_sink *dst;
  lc_pouch_lql_document_filter *filter;
} lc_pouch_query_scan_context;

typedef struct lc_pouch_query_row_meta_json {
  char *key;
  char *content_type;
  char *etag;
  lonejson_int64 version;
} lc_pouch_query_row_meta_json;

static const lonejson_field lc_pouch_query_row_meta_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_pouch_query_row_meta_json, key, "key"),
    LONEJSON_FIELD_STRING_ALLOC(lc_pouch_query_row_meta_json, content_type,
                                "content_type"),
    LONEJSON_FIELD_STRING_ALLOC(lc_pouch_query_row_meta_json, etag, "etag"),
    LONEJSON_FIELD_I64(lc_pouch_query_row_meta_json, version, "version")};

LONEJSON_MAP_DEFINE(lc_pouch_query_row_meta_map, lc_pouch_query_row_meta_json,
                    lc_pouch_query_row_meta_fields);

static int lc_pouch_query_keys_callback_failed(lc_error *error,
                                               const char *fallback) {
  if (error != NULL && error->code != LC_OK) {
    return error->code;
  }
  return lc_error_set(error, LC_ERR_TRANSPORT, 0L, fallback, NULL, NULL, NULL);
}

static int lc_pouch_content_type_is_json(const char *content_type);

typedef struct lc_pouch_lql_memory_reader {
  const unsigned char *data;
  size_t len;
  size_t pos;
} lc_pouch_lql_memory_reader;

typedef struct lc_pouch_lql_decision_capture {
  int matched;
  size_t calls;
} lc_pouch_lql_decision_capture;

static lql_status lc_pouch_lql_read_memory(void *user, unsigned char *buffer,
                                           size_t capacity, size_t *out_len,
                                           lql_error *error) {
  lc_pouch_lql_memory_reader *reader;
  size_t remaining;
  size_t take;

  (void)error;
  reader = (lc_pouch_lql_memory_reader *)user;
  if (reader == NULL || buffer == NULL || out_len == NULL) {
    return LQL_STATUS_INVALID_ARGUMENT;
  }
  remaining = reader->len - reader->pos;
  take = remaining < capacity ? remaining : capacity;
  if (take > 0U) {
    memcpy(buffer, reader->data + reader->pos, take);
    reader->pos += take;
  }
  *out_len = take;
  return LQL_STATUS_OK;
}

static lql_stream_callback_result
lc_pouch_lql_capture_decision(void *user,
                              const lql_stream_decision *decision,
                              lql_error *error) {
  lc_pouch_lql_decision_capture *capture;

  (void)error;
  capture = (lc_pouch_lql_decision_capture *)user;
  if (capture == NULL || decision == NULL) {
    return LQL_STREAM_CALLBACK_ERROR;
  }
  ++capture->calls;
  if (decision->matched) {
    capture->matched = 1;
  }
  return LQL_STREAM_CALLBACK_CONTINUE;
}

static void
lc_pouch_lql_document_filter_cleanup(lc_pouch_lql_document_filter *filter) {
  if (filter == NULL) {
    return;
  }
  if (filter->runtime != NULL && filter->selector != NULL) {
    filter->runtime->selector_destroy(filter->runtime, filter->selector);
  }
  if (filter->runtime != NULL) {
    filter->runtime->destroy(filter->runtime);
  }
  memset(filter, 0, sizeof(*filter));
}

static int
lc_pouch_lql_document_filter_init(lc_pouch_lql_document_filter *filter,
                                  const char *selector_json,
                                  lc_error *error) {
  lql_error lql_err;
  lql_status status;

  if (filter == NULL || selector_json == NULL) {
    return LC_ERR_INVALID;
  }
  memset(filter, 0, sizeof(*filter));
  lql_error_init(&lql_err);
  status = lql_new(&filter->runtime, &lql_err);
  if (status != LQL_STATUS_OK) {
    return lc_pouch_lql_set_error(error, status, &lql_err,
                                  "failed to allocate pouch LQL runtime");
  }
  lql_error_init(&lql_err);
  status = filter->runtime->selector_parse_json(
      filter->runtime, selector_json, strlen(selector_json), &filter->selector,
      &lql_err);
  if (status != LQL_STATUS_OK) {
    lc_pouch_lql_document_filter_cleanup(filter);
    return lc_pouch_lql_set_error(error, status, &lql_err,
                                  "failed to parse pouch LQL selector");
  }
  filter->enabled = 1;
  return LC_OK;
}

static int lc_pouch_lql_document_filter_match_bytes(
    lc_client_handle *client, lc_pouch_lql_document_filter *filter,
    const unsigned char *payload, size_t payload_len, int *matched,
    lc_error *error) {
  unsigned char *framed;
  lc_pouch_lql_memory_reader reader;
  lc_pouch_lql_decision_capture decision;
  lql_stream_request request;
  lql_stream_result result;
  lql_error lql_err;
  lql_status status;

  if (matched != NULL) {
    *matched = 0;
  }
  if (filter == NULL || !filter->enabled) {
    if (matched != NULL) {
      *matched = 1;
    }
    return LC_OK;
  }
  if (client == NULL || (payload == NULL && payload_len > 0U) ||
      matched == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch LQL document filter requires client, payload, "
                        "and matched output",
                        NULL, NULL, NULL);
  }

  framed = (unsigned char *)lc_client_alloc(client, payload_len + 2U);
  if (framed == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch LQL document frame", NULL,
                        NULL, NULL);
  }
  if (payload_len > 0U) {
    memcpy(framed, payload, payload_len);
  }
  framed[payload_len] = '\n';
  framed[payload_len + 1U] = '\0';

  memset(&reader, 0, sizeof(reader));
  reader.data = framed;
  reader.len = payload_len + 1U;
  memset(&decision, 0, sizeof(decision));
  memset(&request, 0, sizeof(request));
  request.reader = lc_pouch_lql_read_memory;
  request.reader_user = &reader;
  request.selector = filter->selector;
  request.on_decision = lc_pouch_lql_capture_decision;
  request.decision_user = &decision;
  memset(&result, 0, sizeof(result));
  lql_error_init(&lql_err);
  status =
      filter->runtime->stream_apply_spooled(filter->runtime, &request, &result,
                                            &lql_err);
  lc_client_free(client, framed);
  if (status != LQL_STATUS_OK) {
    return lc_pouch_lql_set_error(error, status, &lql_err,
                                  "failed to evaluate pouch LQL selector");
  }
  if (result.records_seen != 1U || decision.calls != 1U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch LQL document filter expected one JSON record",
                        NULL, NULL, NULL);
  }
  *matched = decision.matched;
  return LC_OK;
}

static int lc_pouch_lql_document_filter_match(
    lc_client_handle *client, lc_pouch_lql_document_filter *filter,
    lc_source *body, int *matched, lc_error *error) {
  unsigned char *payload;
  size_t payload_len;
  int rc;

  payload = NULL;
  payload_len = 0U;
  rc = lc_pouch_txn_read_source_all(client, body, &payload, &payload_len, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_lql_document_filter_match_bytes(
      client, filter, payload, payload_len, matched, error);
  lc_client_free(client, payload);
  return rc;
}

static int lc_pouch_query_keys_scan_visit(void *context,
                                          const lc_pouch_scan_meta_row *row,
                                          lc_error *error) {
  lc_pouch_query_keys_scan_context *scan;
  lc_pouch_state_info state;
  lc_source *body;
  int embed_json;
  int matched;
  int rc;
  size_t key_len;

  scan = (lc_pouch_query_keys_scan_context *)context;
  if (scan == NULL || scan->handler == NULL || row == NULL ||
      row->key == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query_keys scan visitor requires context and "
                        "row",
                        NULL, NULL, NULL);
  }
  if (scan->filter != NULL && scan->filter->enabled) {
    memset(&state, 0, sizeof(state));
    body = NULL;
    rc = scan->client->pouch_store->read_state(scan->client->pouch_store,
                                               scan->namespace_name, row->key,
                                               &body, &state, error);
    if (rc != LC_OK) {
      return rc;
    }
    embed_json = body != NULL && !state.no_content &&
                 lc_pouch_content_type_is_json(state.content_type);
    matched = 0;
    if (embed_json) {
      rc = lc_pouch_lql_document_filter_match(scan->client, scan->filter, body,
                                              &matched, error);
    }
    if (body != NULL) {
      body->close(body);
    }
    lc_pouch_state_info_cleanup(&scan->client->pouch_allocator, &state);
    if (rc != LC_OK) {
      return rc;
    }
    if (!matched) {
      return LC_OK;
    }
  }
  if (scan->handler->begin != NULL &&
      !scan->handler->begin(scan->handler_context, error)) {
    return lc_pouch_query_keys_callback_failed(
        error, "pouch query_keys begin callback failed");
  }
  key_len = strlen(row->key);
  if (scan->handler->chunk != NULL &&
      !scan->handler->chunk(scan->handler_context, row->key, key_len, error)) {
    return lc_pouch_query_keys_callback_failed(
        error, "pouch query_keys chunk callback failed");
  }
  if (scan->handler->end != NULL &&
      !scan->handler->end(scan->handler_context, error)) {
    return lc_pouch_query_keys_callback_failed(
        error, "pouch query_keys end callback failed");
  }
  return LC_OK;
}

static int lc_pouch_query_keys_index_visit(void *context, const char *key,
                                           lc_error *error) {
  lc_pouch_query_keys_scan_context *scan;
  lc_pouch_scan_meta_row row;
  size_t key_len;

  scan = (lc_pouch_query_keys_scan_context *)context;
  if (scan == NULL || scan->handler == NULL || key == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query_keys index visitor requires context and "
                        "key",
                        NULL, NULL, NULL);
  }
  if (scan->filter != NULL && scan->filter->enabled) {
    memset(&row, 0, sizeof(row));
    row.key = (char *)key;
    return lc_pouch_query_keys_scan_visit(context, &row, error);
  }
  if (scan->handler->begin != NULL &&
      !scan->handler->begin(scan->handler_context, error)) {
    return lc_pouch_query_keys_callback_failed(
        error, "pouch query_keys begin callback failed");
  }
  key_len = strlen(key);
  if (scan->handler->chunk != NULL &&
      !scan->handler->chunk(scan->handler_context, key, key_len, error)) {
    return lc_pouch_query_keys_callback_failed(
        error, "pouch query_keys chunk callback failed");
  }
  if (scan->handler->end != NULL &&
      !scan->handler->end(scan->handler_context, error)) {
    return lc_pouch_query_keys_callback_failed(
        error, "pouch query_keys end callback failed");
  }
  return LC_OK;
}

static int lc_pouch_sink_write_all(lc_sink *dst, const void *bytes,
                                   size_t length, lc_error *error) {
  if (dst == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query write requires sink", NULL, NULL, NULL);
  }
  if (length == 0U) {
    return LC_OK;
  }
  if (!dst->write(dst, bytes, length, error)) {
    return error != NULL && error->code != LC_OK ? error->code
                                                 : LC_ERR_TRANSPORT;
  }
  return LC_OK;
}

static int lc_pouch_sink_write_cstr(lc_sink *dst, const char *text,
                                    lc_error *error) {
  return lc_pouch_sink_write_all(dst, text, strlen(text), error);
}

static int lc_pouch_content_type_is_json(const char *content_type) {
  size_t length;

  if (content_type == NULL) {
    return 0;
  }
  length = strlen("application/json");
  return strncmp(content_type, "application/json", length) == 0 &&
         (content_type[length] == '\0' || content_type[length] == ';');
}

static int lc_pouch_lonejson_error(lc_error *error, lonejson_status status,
                                   const lonejson_error *lj_error,
                                   const char *message) {
  int code;

  code = status == LONEJSON_STATUS_ALLOCATION_FAILED ||
                 status == LONEJSON_STATUS_OVERFLOW
             ? LC_ERR_NOMEM
             : LC_ERR_INVALID;
  return lc_error_set(error, code, 0L,
                      lj_error != NULL && lj_error->message[0] != '\0'
                          ? lj_error->message
                          : message,
                      NULL, NULL, NULL);
}

static int lc_pouch_query_write_row_prefix(lc_sink *dst, const char *key,
                                           const lc_pouch_state_info *state,
                                           lc_error *error) {
  lc_pouch_query_row_meta_json row;
  lonejson *runtime;
  lonejson_error lj_error;
  lonejson_owned_buffer owned;
  lonejson_status status;
  int rc;

  runtime = lc_thread_lonejson_runtime();
  if (runtime == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query JSON runtime", NULL,
                        NULL, NULL);
  }
  memset(&row, 0, sizeof(row));
  row.key = (char *)key;
  row.content_type = (char *)(state != NULL ? state->content_type : NULL);
  row.etag = (char *)(state != NULL ? state->etag : NULL);
  row.version = state != NULL ? (lonejson_int64)state->version : 0;
  owned = lonejson_default_owned_buffer();
  lonejson_error_init(&lj_error);
  status = runtime->serialize_owned(runtime, &lc_pouch_query_row_meta_map, &row,
                                    &owned, &lj_error);
  if (status != LONEJSON_STATUS_OK) {
    lonejson_owned_buffer_free(&owned);
    return lc_pouch_lonejson_error(error, status, &lj_error,
                                   "failed to serialize pouch query row");
  }
  if (owned.len == 0U || owned.data[owned.len - 1U] != '}') {
    lonejson_owned_buffer_free(&owned);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query row serializer returned invalid JSON",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_sink_write_all(dst, owned.data, owned.len - 1U, error);
  lonejson_owned_buffer_free(&owned);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_pouch_sink_write_cstr(dst, ",\"document\":", error);
}

static int lc_pouch_query_scan_write_row(lc_pouch_query_scan_context *scan,
                                         const char *namespace_name,
                                         const lc_pouch_scan_meta_row *row,
                                         lc_error *error) {
  lc_pouch_state_info state;
  lc_source *body;
  unsigned char *payload;
  size_t payload_len;
  int embed_json;
  int matched;
  int rc;

  memset(&state, 0, sizeof(state));
  body = NULL;
  payload = NULL;
  payload_len = 0U;
  rc = scan->client->pouch_store->read_state(scan->client->pouch_store,
                                             namespace_name, row->key, &body,
                                             &state, error);
  if (rc != LC_OK) {
    return rc;
  }
  embed_json = body != NULL && !state.no_content &&
               lc_pouch_content_type_is_json(state.content_type);
  if (scan->filter != NULL && scan->filter->enabled) {
    matched = 0;
    if (embed_json) {
      rc = lc_pouch_txn_read_source_all(scan->client, body, &payload,
                                        &payload_len, error);
      if (rc == LC_OK) {
        rc = lc_pouch_lql_document_filter_match_bytes(
            scan->client, scan->filter, payload, payload_len, &matched, error);
      }
    }
    if (rc != LC_OK) {
      lc_client_free(scan->client, payload);
      if (body != NULL) {
        body->close(body);
      }
      lc_pouch_state_info_cleanup(&scan->client->pouch_allocator, &state);
      return rc;
    }
    if (!matched) {
      lc_client_free(scan->client, payload);
      if (body != NULL) {
        body->close(body);
      }
      lc_pouch_state_info_cleanup(&scan->client->pouch_allocator, &state);
      return LC_OK;
    }
    rc = lc_pouch_query_write_row_prefix(scan->dst, row->key, &state, error);
    if (rc == LC_OK) {
      rc = lc_pouch_sink_write_all(scan->dst, payload, payload_len, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_sink_write_cstr(scan->dst, "}\n", error);
    }
    lc_client_free(scan->client, payload);
    if (body != NULL) {
      body->close(body);
    }
    lc_pouch_state_info_cleanup(&scan->client->pouch_allocator, &state);
    return rc;
  }

  rc = lc_pouch_query_write_row_prefix(scan->dst, row->key, &state, error);
  if (rc == LC_OK && embed_json) {
    rc = lc_pouch_copy_source_to_sink(body, scan->dst, error);
  } else if (rc == LC_OK) {
    rc = lc_pouch_sink_write_cstr(scan->dst, "null", error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_sink_write_cstr(scan->dst, "}\n", error);
  }
  if (body != NULL) {
    body->close(body);
  }
  lc_pouch_state_info_cleanup(&scan->client->pouch_allocator, &state);
  return rc;
}

typedef struct lc_pouch_query_scan_visit_context {
  lc_pouch_query_scan_context scan;
  const char *namespace_name;
} lc_pouch_query_scan_visit_context;

static int lc_pouch_query_scan_visit_row(void *context,
                                         const lc_pouch_scan_meta_row *row,
                                         lc_error *error) {
  lc_pouch_query_scan_visit_context *visit;

  visit = (lc_pouch_query_scan_visit_context *)context;
  if (visit == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query scan requires visitor context", NULL, NULL,
                        NULL);
  }
  return lc_pouch_query_scan_write_row(&visit->scan, visit->namespace_name, row,
                                       error);
}

static char *lc_pouch_query_candidates_metadata(unsigned long candidates,
                                                lc_error *error) {
  char metadata[96];
  char *copy;

  snprintf(metadata, sizeof(metadata), "{\"query_candidates\":%lu}",
           candidates);
  copy = lc_strdup_local(metadata);
  if (copy == NULL) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch query metadata", NULL, NULL,
                       NULL);
  }
  return copy;
}

static int lc_pouch_query_result_metadata_ready(const lc_query_res *out,
                                                lc_error *error) {
  if (out->return_mode != NULL && out->metadata_json != NULL &&
      out->correlation_id != NULL) {
    return LC_OK;
  }
  if (out->return_mode == NULL || out->metadata_json == NULL ||
      out->correlation_id == NULL) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch query metadata", NULL, NULL,
                       NULL);
  }
  return LC_ERR_NOMEM;
}

static int lc_pouch_query_selector_is_lql_operator_form(const char *selector_json) {
  lonejson *runtime;

  runtime = lc_thread_lonejson_runtime();
  if (runtime == NULL) {
    return 0;
  }
  return lc_pouch_selector_json_has_lql_operator(runtime, selector_json);
}

static int lc_pouch_client_query_scan(lc_client_handle *client,
                                      const lc_query_req *req, lc_sink *dst,
                                      lc_query_res *out, lc_error *error) {
  lc_pouch_scan_meta_req scan_req;
  lc_pouch_scan_meta_res scan_res;
  lc_pouch_query_scan_visit_context visit;
  lc_pouch_query_selector_kind selector_kind;
  lc_pouch_lql_document_filter filter;
  const char *namespace_name;
  char *key_selector;
  char *owner_selector;
  int rc;

  if (client == NULL || req == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "query requires self, req, dst, and out", NULL, NULL,
                        NULL);
  }
  key_selector = NULL;
  owner_selector = NULL;
  memset(&filter, 0, sizeof(filter));
  selector_kind = lc_pouch_query_selector_kind_parse(
      req->selector_json, &key_selector, &owner_selector, error);
  if (selector_kind == LC_POUCH_QUERY_SELECTOR_UNSUPPORTED) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    if (error != NULL && error->code == LC_ERR_NOMEM) {
      return LC_ERR_NOMEM;
    }
    if (lc_pouch_query_selector_is_lql_operator_form(req->selector_json)) {
      rc = lc_pouch_lql_document_filter_init(&filter, req->selector_json,
                                             error);
      if (rc != LC_OK) {
        return rc;
      }
    } else {
      return lc_pouch_query_selector_error_or_unsupported(
          error,
          "pouch scan query supports only match-all, key, owner, or key+owner "
          "selector");
    }
  }
  if (req->fields_json != NULL && req->fields_json[0] != '\0') {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    return lc_pouch_client_unsupported(
        error, "pouch scan query does not support fields");
  }
  if (req->return_mode != NULL && req->return_mode[0] != '\0' &&
      strcmp(req->return_mode, "documents") != 0) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch scan query return_mode must be documents", NULL,
                        NULL, NULL);
  }
  if (req->refresh != NULL && req->refresh[0] != '\0') {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    return lc_pouch_client_unsupported(
        error, "pouch scan query does not support refresh");
  }
  if (req->limit < 0L) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query limit must be non-negative", NULL, NULL,
                        NULL);
  }

  rc = lc_pouch_public_namespace(client, req->namespace_name, &namespace_name,
                                 error);
  if (rc != LC_OK) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    return rc;
  }

  memset(&scan_req, 0, sizeof(scan_req));
  memset(&scan_res, 0, sizeof(scan_res));
  memset(&visit, 0, sizeof(visit));
  memset(out, 0, sizeof(*out));
  scan_req.namespace_name = namespace_name;
  if (selector_kind == LC_POUCH_QUERY_SELECTOR_KEY ||
      selector_kind == LC_POUCH_QUERY_SELECTOR_KEY_OWNER) {
    scan_req.key = key_selector;
  }
  if (selector_kind == LC_POUCH_QUERY_SELECTOR_OWNER ||
      selector_kind == LC_POUCH_QUERY_SELECTOR_KEY_OWNER) {
    scan_req.owner = owner_selector;
  }
  scan_req.start_after = req->cursor;
  scan_req.limit = (size_t)req->limit;
  scan_req.exclude_deleted_state = 1;
  visit.scan.client = client;
  visit.scan.dst = dst;
  visit.scan.filter = &filter;
  visit.namespace_name = namespace_name;

  rc = client->pouch_store->scan_meta(client->pouch_store, &scan_req,
                                      lc_pouch_query_scan_visit_row, &visit,
                                      &scan_res, error);
  if (rc != LC_OK) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    lc_pouch_scan_meta_res_cleanup(&client->pouch_allocator, &scan_res);
    return rc;
  }

  if (scan_res.next_start_after != NULL) {
    out->cursor = lc_strdup_local(scan_res.next_start_after);
    if (out->cursor == NULL) {
      lc_client_free(client, key_selector);
      lc_client_free(client, owner_selector);
      lc_pouch_lql_document_filter_cleanup(&filter);
      lc_pouch_scan_meta_res_cleanup(&client->pouch_allocator, &scan_res);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query cursor", NULL, NULL,
                          NULL);
    }
  }
  out->return_mode = lc_strdup_local("documents");
  out->correlation_id = lc_strdup_local("pouch-query");
  out->metadata_json = lc_pouch_query_candidates_metadata(
      (unsigned long)scan_res.visited, error);
  if (lc_pouch_query_result_metadata_ready(out, error) != LC_OK) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    lc_query_res_cleanup(out);
    lc_pouch_scan_meta_res_cleanup(&client->pouch_allocator, &scan_res);
    return LC_ERR_NOMEM;
  }
  lc_client_free(client, key_selector);
  lc_client_free(client, owner_selector);
  lc_pouch_lql_document_filter_cleanup(&filter);
  lc_pouch_scan_meta_res_cleanup(&client->pouch_allocator, &scan_res);
  return LC_OK;
}

static int lc_pouch_client_query_index(lc_client_handle *client,
                                       const lc_query_req *req, lc_sink *dst,
                                       lc_query_res *out, lc_error *error) {
  lc_pouch_query_index_scan_req scan_req;
  lc_pouch_query_owner_scan_req owner_req;
  lc_pouch_query_index_scan_res scan_res;
  lc_pouch_query_scan_visit_context visit;
  lc_pouch_query_selector_kind selector_kind;
  lc_pouch_lql_document_filter filter;
  const char *namespace_name;
  char *key_selector;
  char *owner_selector;
  int rc;

  if (client == NULL || req == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "query requires self, req, dst, and out", NULL, NULL,
                        NULL);
  }
  if (client->pouch_store == NULL ||
      client->pouch_store->query_index_scan == NULL) {
    return lc_pouch_client_unsupported(error,
                                       "pouch indexed query is not available");
  }
  key_selector = NULL;
  owner_selector = NULL;
  memset(&filter, 0, sizeof(filter));
  selector_kind = lc_pouch_query_selector_kind_parse(
      req->selector_json, &key_selector, &owner_selector, error);
  if (selector_kind == LC_POUCH_QUERY_SELECTOR_UNSUPPORTED) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    if (error != NULL && error->code == LC_ERR_NOMEM) {
      return LC_ERR_NOMEM;
    }
    if (lc_pouch_query_selector_is_lql_operator_form(req->selector_json)) {
      rc = lc_pouch_lql_document_filter_init(&filter, req->selector_json,
                                             error);
      if (rc != LC_OK) {
        return rc;
      }
    } else {
      return lc_pouch_query_selector_error_or_unsupported(
          error,
          "pouch index query supports only match-all, key, owner, or key+owner "
          "selector");
    }
  }
  if (req->fields_json != NULL && req->fields_json[0] != '\0') {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    return lc_pouch_client_unsupported(
        error, "pouch index query does not support fields");
  }
  if (req->return_mode != NULL && req->return_mode[0] != '\0' &&
      strcmp(req->return_mode, "documents") != 0) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index query return_mode must be documents", NULL,
                        NULL, NULL);
  }
  if (!lc_pouch_index_refresh_supported(req->refresh)) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index query refresh must be wait_for", NULL,
                        NULL, NULL);
  }
  if (req->limit < 0L) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query limit must be non-negative", NULL, NULL,
                        NULL);
  }

  rc = lc_pouch_public_namespace(client, req->namespace_name, &namespace_name,
                                 error);
  if (rc != LC_OK) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    return rc;
  }
  if (lc_pouch_index_refresh_is_wait_for(req->refresh)) {
    rc = lc_pouch_wait_for_index(client, namespace_name, error);
    if (rc != LC_OK) {
      lc_client_free(client, key_selector);
      lc_client_free(client, owner_selector);
      lc_pouch_lql_document_filter_cleanup(&filter);
      return rc;
    }
  }

  memset(&scan_req, 0, sizeof(scan_req));
  memset(&owner_req, 0, sizeof(owner_req));
  memset(&scan_res, 0, sizeof(scan_res));
  memset(&visit, 0, sizeof(visit));
  memset(out, 0, sizeof(*out));
  scan_req.namespace_name = namespace_name;
  if (selector_kind == LC_POUCH_QUERY_SELECTOR_KEY ||
      selector_kind == LC_POUCH_QUERY_SELECTOR_KEY_OWNER) {
    scan_req.key = key_selector;
  }
  if (selector_kind == LC_POUCH_QUERY_SELECTOR_KEY_OWNER) {
    scan_req.owner = owner_selector;
  }
  scan_req.start_after = req->cursor;
  scan_req.limit = (size_t)req->limit;
  visit.scan.client = client;
  visit.scan.dst = dst;
  visit.scan.filter = &filter;
  visit.namespace_name = namespace_name;

  if (selector_kind == LC_POUCH_QUERY_SELECTOR_OWNER) {
    if (client->pouch_store->query_owner_scan == NULL) {
      lc_client_free(client, key_selector);
      lc_client_free(client, owner_selector);
      lc_pouch_lql_document_filter_cleanup(&filter);
      return lc_pouch_client_unsupported(
          error, "pouch indexed owner query is not available");
    }
    owner_req.namespace_name = namespace_name;
    owner_req.owner = owner_selector;
    owner_req.start_after = req->cursor;
    owner_req.limit = (size_t)req->limit;
    rc = client->pouch_store->query_owner_scan(client->pouch_store, &owner_req,
                                               lc_pouch_query_scan_visit_row,
                                               &visit, &scan_res, error);
  } else {
    rc = client->pouch_store->query_index_scan(client->pouch_store, &scan_req,
                                               lc_pouch_query_scan_visit_row,
                                               &visit, &scan_res, error);
  }
  if (rc != LC_OK) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    lc_pouch_query_index_scan_res_cleanup(&client->pouch_allocator, &scan_res);
    return rc;
  }

  if (scan_res.next_start_after != NULL) {
    out->cursor = lc_strdup_local(scan_res.next_start_after);
    if (out->cursor == NULL) {
      lc_client_free(client, key_selector);
      lc_client_free(client, owner_selector);
      lc_pouch_lql_document_filter_cleanup(&filter);
      lc_pouch_query_index_scan_res_cleanup(&client->pouch_allocator,
                                            &scan_res);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query cursor", NULL, NULL,
                          NULL);
    }
  }
  out->return_mode = lc_strdup_local("documents");
  out->correlation_id = lc_strdup_local("pouch-query");
  out->index_seq = scan_res.index_seq;
  out->metadata_json = lc_pouch_query_candidates_metadata(
      (unsigned long)scan_res.visited, error);
  if (lc_pouch_query_result_metadata_ready(out, error) != LC_OK) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    lc_query_res_cleanup(out);
    lc_pouch_query_index_scan_res_cleanup(&client->pouch_allocator, &scan_res);
    return LC_ERR_NOMEM;
  }
  lc_client_free(client, key_selector);
  lc_client_free(client, owner_selector);
  lc_pouch_lql_document_filter_cleanup(&filter);
  lc_pouch_query_index_scan_res_cleanup(&client->pouch_allocator, &scan_res);
  return LC_OK;
}

static int lc_pouch_client_query_keys_scan(lc_client_handle *client,
                                           const lc_query_req *req,
                                           const lc_query_key_handler *handler,
                                           void *context, lc_query_res *out,
                                           lc_error *error) {
  lc_pouch_scan_meta_req scan_req;
  lc_pouch_scan_meta_res scan_res;
  lc_pouch_query_keys_scan_context scan_context;
  lc_pouch_query_selector_kind selector_kind;
  lc_pouch_lql_document_filter filter;
  const char *namespace_name;
  char *key_selector;
  char *owner_selector;
  int rc;

  if (client == NULL || req == NULL || handler == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "query_keys requires self, req, handler, and out", NULL,
                        NULL, NULL);
  }
  key_selector = NULL;
  owner_selector = NULL;
  memset(&filter, 0, sizeof(filter));
  selector_kind = lc_pouch_query_selector_kind_parse(
      req->selector_json, &key_selector, &owner_selector, error);
  if (selector_kind == LC_POUCH_QUERY_SELECTOR_UNSUPPORTED) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    if (error != NULL && error->code == LC_ERR_NOMEM) {
      return LC_ERR_NOMEM;
    }
    if (lc_pouch_query_selector_is_lql_operator_form(req->selector_json)) {
      rc = lc_pouch_lql_document_filter_init(&filter, req->selector_json,
                                             error);
      if (rc != LC_OK) {
        return rc;
      }
    } else {
      return lc_pouch_query_selector_error_or_unsupported(
          error,
          "pouch scan query_keys supports only match-all, key, owner, or "
          "key+owner selector");
    }
  }
  if (req->fields_json != NULL && req->fields_json[0] != '\0') {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    return lc_pouch_client_unsupported(
        error, "pouch scan query_keys does not support fields");
  }
  if (req->return_mode != NULL && req->return_mode[0] != '\0' &&
      strcmp(req->return_mode, "keys") != 0) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query_keys return_mode must be keys", NULL, NULL,
                        NULL);
  }
  if (req->refresh != NULL && req->refresh[0] != '\0') {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    return lc_pouch_client_unsupported(
        error, "pouch scan query_keys does not support refresh");
  }
  if (req->limit < 0L) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query_keys limit must be non-negative", NULL,
                        NULL, NULL);
  }

  rc = lc_pouch_public_namespace(client, req->namespace_name, &namespace_name,
                                 error);
  if (rc != LC_OK) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    return rc;
  }

  memset(&scan_req, 0, sizeof(scan_req));
  memset(&scan_res, 0, sizeof(scan_res));
  memset(&scan_context, 0, sizeof(scan_context));
  memset(out, 0, sizeof(*out));
  scan_req.namespace_name = namespace_name;
  if (selector_kind == LC_POUCH_QUERY_SELECTOR_KEY ||
      selector_kind == LC_POUCH_QUERY_SELECTOR_KEY_OWNER) {
    scan_req.key = key_selector;
  }
  if (selector_kind == LC_POUCH_QUERY_SELECTOR_OWNER ||
      selector_kind == LC_POUCH_QUERY_SELECTOR_KEY_OWNER) {
    scan_req.owner = owner_selector;
  }
  scan_req.start_after = req->cursor;
  scan_req.limit = (size_t)req->limit;
  scan_req.exclude_deleted_state = 1;
  scan_context.handler = handler;
  scan_context.handler_context = context;
  scan_context.filter = &filter;
  scan_context.client = client;
  scan_context.namespace_name = namespace_name;

  if (client->pouch_store->scan_meta_keys != NULL) {
    rc = client->pouch_store->scan_meta_keys(client->pouch_store, &scan_req,
                                             lc_pouch_query_keys_index_visit,
                                             &scan_context, &scan_res, error);
  } else {
    rc = client->pouch_store->scan_meta(client->pouch_store, &scan_req,
                                        lc_pouch_query_keys_scan_visit,
                                        &scan_context, &scan_res, error);
  }
  if (rc != LC_OK) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    lc_pouch_scan_meta_res_cleanup(&client->pouch_allocator, &scan_res);
    return rc;
  }

  if (scan_res.next_start_after != NULL) {
    out->cursor = lc_strdup_local(scan_res.next_start_after);
    if (out->cursor == NULL) {
      lc_client_free(client, key_selector);
      lc_client_free(client, owner_selector);
      lc_pouch_lql_document_filter_cleanup(&filter);
      lc_pouch_scan_meta_res_cleanup(&client->pouch_allocator, &scan_res);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query_keys cursor", NULL,
                          NULL, NULL);
    }
  }
  out->return_mode = lc_strdup_local("keys");
  out->correlation_id = lc_strdup_local("pouch-query-keys");
  out->metadata_json = lc_pouch_query_candidates_metadata(
      (unsigned long)scan_res.visited, error);
  if (lc_pouch_query_result_metadata_ready(out, error) != LC_OK) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    lc_query_res_cleanup(out);
    lc_pouch_scan_meta_res_cleanup(&client->pouch_allocator, &scan_res);
    return LC_ERR_NOMEM;
  }
  lc_client_free(client, key_selector);
  lc_client_free(client, owner_selector);
  lc_pouch_lql_document_filter_cleanup(&filter);
  lc_pouch_scan_meta_res_cleanup(&client->pouch_allocator, &scan_res);
  return LC_OK;
}

static int lc_pouch_client_query_keys_index(lc_client_handle *client,
                                            const lc_query_req *req,
                                            const lc_query_key_handler *handler,
                                            void *context, lc_query_res *out,
                                            lc_error *error) {
  lc_pouch_query_index_scan_req scan_req;
  lc_pouch_query_owner_scan_req owner_req;
  lc_pouch_query_index_scan_res scan_res;
  lc_pouch_query_keys_scan_context scan_context;
  lc_pouch_query_selector_kind selector_kind;
  lc_pouch_lql_document_filter filter;
  const char *namespace_name;
  char *key_selector;
  char *owner_selector;
  int rc;

  if (client == NULL || req == NULL || handler == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "query_keys requires self, req, handler, and out", NULL,
                        NULL, NULL);
  }
  if (client->pouch_store == NULL ||
      client->pouch_store->query_index_keys_scan == NULL) {
    return lc_pouch_client_unsupported(
        error, "pouch indexed query_keys is not available");
  }
  key_selector = NULL;
  owner_selector = NULL;
  memset(&filter, 0, sizeof(filter));
  selector_kind = lc_pouch_query_selector_kind_parse(
      req->selector_json, &key_selector, &owner_selector, error);
  if (selector_kind == LC_POUCH_QUERY_SELECTOR_UNSUPPORTED) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    if (error != NULL && error->code == LC_ERR_NOMEM) {
      return LC_ERR_NOMEM;
    }
    if (lc_pouch_query_selector_is_lql_operator_form(req->selector_json)) {
      rc = lc_pouch_lql_document_filter_init(&filter, req->selector_json,
                                             error);
      if (rc != LC_OK) {
        return rc;
      }
    } else {
      return lc_pouch_query_selector_error_or_unsupported(
          error,
          "pouch index query_keys supports only match-all, key, owner, or "
          "key+owner selector");
    }
  }
  if (req->fields_json != NULL && req->fields_json[0] != '\0') {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    return lc_pouch_client_unsupported(
        error, "pouch index query_keys does not support fields");
  }
  if (req->return_mode != NULL && req->return_mode[0] != '\0' &&
      strcmp(req->return_mode, "keys") != 0) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query_keys return_mode must be keys", NULL, NULL,
                        NULL);
  }
  if (!lc_pouch_index_refresh_supported(req->refresh)) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index query_keys refresh must be wait_for", NULL,
                        NULL, NULL);
  }
  if (req->limit < 0L) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query_keys limit must be non-negative", NULL,
                        NULL, NULL);
  }

  rc = lc_pouch_public_namespace(client, req->namespace_name, &namespace_name,
                                 error);
  if (rc != LC_OK) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    return rc;
  }
  if (lc_pouch_index_refresh_is_wait_for(req->refresh)) {
    rc = lc_pouch_wait_for_index(client, namespace_name, error);
    if (rc != LC_OK) {
      lc_client_free(client, key_selector);
      lc_client_free(client, owner_selector);
      lc_pouch_lql_document_filter_cleanup(&filter);
      return rc;
    }
  }

  memset(&scan_req, 0, sizeof(scan_req));
  memset(&owner_req, 0, sizeof(owner_req));
  memset(&scan_res, 0, sizeof(scan_res));
  memset(&scan_context, 0, sizeof(scan_context));
  memset(out, 0, sizeof(*out));
  scan_req.namespace_name = namespace_name;
  if (selector_kind == LC_POUCH_QUERY_SELECTOR_KEY ||
      selector_kind == LC_POUCH_QUERY_SELECTOR_KEY_OWNER) {
    scan_req.key = key_selector;
  }
  if (selector_kind == LC_POUCH_QUERY_SELECTOR_KEY_OWNER) {
    scan_req.owner = owner_selector;
  }
  scan_req.start_after = req->cursor;
  scan_req.limit = (size_t)req->limit;
  scan_context.handler = handler;
  scan_context.handler_context = context;
  scan_context.filter = &filter;
  scan_context.client = client;
  scan_context.namespace_name = namespace_name;

  if (selector_kind == LC_POUCH_QUERY_SELECTOR_OWNER) {
    if (client->pouch_store->query_owner_keys_scan == NULL) {
      lc_client_free(client, key_selector);
      lc_client_free(client, owner_selector);
      lc_pouch_lql_document_filter_cleanup(&filter);
      return lc_pouch_client_unsupported(
          error, "pouch indexed owner query_keys is not available");
    }
    owner_req.namespace_name = namespace_name;
    owner_req.owner = owner_selector;
    owner_req.start_after = req->cursor;
    owner_req.limit = (size_t)req->limit;
    rc = client->pouch_store->query_owner_keys_scan(
        client->pouch_store, &owner_req, lc_pouch_query_keys_index_visit,
        &scan_context, &scan_res, error);
  } else {
    rc = client->pouch_store->query_index_keys_scan(
        client->pouch_store, &scan_req, lc_pouch_query_keys_index_visit,
        &scan_context, &scan_res, error);
  }
  if (rc != LC_OK) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    lc_pouch_query_index_scan_res_cleanup(&client->pouch_allocator, &scan_res);
    return rc;
  }

  if (scan_res.next_start_after != NULL) {
    out->cursor = lc_strdup_local(scan_res.next_start_after);
    if (out->cursor == NULL) {
      lc_client_free(client, key_selector);
      lc_client_free(client, owner_selector);
      lc_pouch_lql_document_filter_cleanup(&filter);
      lc_pouch_query_index_scan_res_cleanup(&client->pouch_allocator,
                                            &scan_res);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query_keys cursor", NULL,
                          NULL, NULL);
    }
  }
  out->return_mode = lc_strdup_local("keys");
  out->correlation_id = lc_strdup_local("pouch-query-keys");
  out->index_seq = scan_res.index_seq;
  out->metadata_json = lc_pouch_query_candidates_metadata(
      (unsigned long)scan_res.visited, error);
  if (lc_pouch_query_result_metadata_ready(out, error) != LC_OK) {
    lc_client_free(client, key_selector);
    lc_client_free(client, owner_selector);
    lc_pouch_lql_document_filter_cleanup(&filter);
    lc_query_res_cleanup(out);
    lc_pouch_query_index_scan_res_cleanup(&client->pouch_allocator, &scan_res);
    return LC_ERR_NOMEM;
  }
  lc_client_free(client, key_selector);
  lc_client_free(client, owner_selector);
  lc_pouch_lql_document_filter_cleanup(&filter);
  lc_pouch_query_index_scan_res_cleanup(&client->pouch_allocator, &scan_res);
  return LC_OK;
}

int lc_pouch_client_query_method(lc_client *self, const lc_query_req *req,
                                 lc_sink *dst, lc_query_res *out,
                                 lc_error *error) {
  lc_client_handle *client;
  if (self == NULL || req == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "query requires self, req, dst, and out", NULL, NULL,
                        NULL);
  }
  {
    int rc;

    rc = lc_pouch_validate_query_engine(req, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  client = (lc_client_handle *)self;
  if (strcmp(lc_pouch_effective_query_engine(client, req), "scan") == 0) {
    return lc_pouch_client_query_scan(client, req, dst, out, error);
  }
  return lc_pouch_client_query_index(client, req, dst, out, error);
}

int lc_pouch_client_query_keys_method(lc_client *self, const lc_query_req *req,
                                      const lc_query_key_handler *handler,
                                      void *context, lc_query_res *out,
                                      lc_error *error) {
  lc_client_handle *client;
  if (self == NULL || req == NULL || handler == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "query_keys requires self, req, handler, and out", NULL,
                        NULL, NULL);
  }
  {
    int rc;

    rc = lc_pouch_validate_query_engine(req, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  client = (lc_client_handle *)self;
  if (strcmp(lc_pouch_effective_query_engine(client, req), "scan") == 0) {
    return lc_pouch_client_query_keys_scan(client, req, handler, context, out,
                                           error);
  }
  return lc_pouch_client_query_keys_index(client, req, handler, context, out,
                                          error);
}

int lc_pouch_client_get_namespace_config_method(
    lc_client *self, const lc_namespace_config_req *req,
    lc_namespace_config_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_query_config store_config;
  const char *namespace_name;
  int rc;

  if (self == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "get_namespace_config requires self and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  memset(out, 0, sizeof(*out));
  memset(&store_config, 0, sizeof(store_config));
  namespace_name = NULL;
  rc = lc_pouch_public_namespace(
      client, req != NULL ? req->namespace_name : NULL, &namespace_name, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (client->pouch_store == NULL ||
      client->pouch_store->query_config == NULL) {
    return lc_pouch_client_unsupported(
        error, "pouch query configuration is not available");
  }
  rc = client->pouch_store->query_config(client->pouch_store, namespace_name,
                                         &store_config, error);
  if (rc != LC_OK) {
    return rc;
  }
  out->namespace_name = lc_strdup_local(namespace_name);
  out->preferred_engine = lc_strdup_local(store_config.preferred_engine);
  out->fallback_engine = lc_strdup_local(store_config.fallback_engine);
  out->correlation_id = lc_strdup_local("pouch-namespace-config");
  lc_pouch_query_config_cleanup(&client->pouch_allocator, &store_config);
  if (out->namespace_name == NULL || out->preferred_engine == NULL ||
      out->fallback_engine == NULL || out->correlation_id == NULL) {
    lc_namespace_config_res_cleanup(out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace configuration",
                        NULL, NULL, NULL);
  }
  return LC_OK;
}

int lc_pouch_client_update_namespace_config_method(
    lc_client *self, const lc_namespace_config_req *req,
    lc_namespace_config_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_unsupported(
      error, "pouch namespace management is not supported");
}

int lc_pouch_client_flush_index_method(lc_client *self,
                                       const lc_index_flush_req *req,
                                       lc_index_flush_res *out,
                                       lc_error *error) {
  lc_client_handle *client;
  lc_pouch_index_flush_res store_res;
  const char *namespace_name;
  int rc;

  if (self == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "flush_index requires self and out", NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  if (client->pouch_store == NULL || client->pouch_store->flush_index == NULL) {
    return lc_pouch_client_unsupported(error,
                                       "pouch index flush is not available");
  }
  rc = lc_pouch_public_namespace(
      client, req != NULL ? req->namespace_name : NULL, &namespace_name, error);
  if (rc != LC_OK) {
    return rc;
  }

  memset(out, 0, sizeof(*out));
  memset(&store_res, 0, sizeof(store_res));
  rc = client->pouch_store->flush_index(client->pouch_store, namespace_name,
                                        req != NULL ? req->mode : NULL,
                                        &store_res, error);
  if (rc != LC_OK) {
    lc_pouch_index_flush_res_cleanup(&client->pouch_allocator, &store_res);
    return rc;
  }

  out->namespace_name = lc_strdup_local(store_res.namespace_name);
  out->mode = lc_strdup_local(store_res.mode);
  out->flush_id = lc_strdup_local(store_res.flush_id);
  out->accepted = store_res.accepted;
  out->flushed = store_res.flushed;
  out->pending = store_res.pending;
  out->index_seq = store_res.index_seq;
  out->correlation_id = lc_strdup_local("pouch-index-flush");
  lc_pouch_index_flush_res_cleanup(&client->pouch_allocator, &store_res);
  if (out->namespace_name == NULL || out->mode == NULL ||
      out->flush_id == NULL || out->correlation_id == NULL) {
    lc_index_flush_res_cleanup(out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index flush result", NULL,
                        NULL, NULL);
  }
  return LC_OK;
}

int lc_pouch_client_txn_replay_method(lc_client *self,
                                      const lc_txn_replay_req *req,
                                      lc_txn_replay_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_txn_record record;
  int rc;

  if (self == NULL || req == NULL || out == NULL || req->txn_id == NULL ||
      req->txn_id[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch txn_replay requires self, req, out, and txn_id",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  memset(out, 0, sizeof(*out));
  memset(&record, 0, sizeof(record));
  rc = lc_pouch_txn_load_record(client, req->txn_id, &record, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_txn_apply_record(client, &record, 1, error);
  if (rc == LC_OK) {
    out->txn_id = lc_strdup_local(record.txn_id);
    out->state = lc_strdup_local(lc_pouch_txn_state_name(record.state));
    out->correlation_id = lc_strdup_local("pouch-txn-replay");
    if (out->txn_id == NULL || out->state == NULL ||
        out->correlation_id == NULL) {
      lc_txn_replay_res_cleanup(out);
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch transaction replay result",
                        NULL, NULL, NULL);
    }
  }
  lc_pouch_txn_record_cleanup(client, &record);
  return rc;
}

int lc_pouch_client_txn_prepare_method(lc_client *self,
                                       const lc_txn_decision_req *req,
                                       lc_txn_decision_res *out,
                                       lc_error *error) {
  lc_client_handle *client;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch txn_prepare requires self, req, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  memset(out, 0, sizeof(*out));
  rc = lc_pouch_txn_validate_decision_req(client, req, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_txn_validate_pending_participants(client, req, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_txn_store_record(client, req, LC_POUCH_TXN_STATE_PREPARED,
                                 error);
  if (rc != LC_OK) {
    return rc;
  }
  out->txn_id = lc_strdup_local(req->txn_id);
  out->state = lc_strdup_local("prepared");
  out->correlation_id = lc_strdup_local("pouch-txn-prepare");
  if (out->txn_id == NULL || out->state == NULL ||
      out->correlation_id == NULL) {
    lc_txn_decision_res_cleanup(out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch transaction prepare result",
                        NULL, NULL, NULL);
  }
  return LC_OK;
}

int lc_pouch_client_txn_commit_method(lc_client *self,
                                      const lc_txn_decision_req *req,
                                      lc_txn_decision_res *out,
                                      lc_error *error) {
  lc_client_handle *client;
  lc_pouch_txn_record record;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch txn_commit requires self, req, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  memset(out, 0, sizeof(*out));
  memset(&record, 0, sizeof(record));
  rc = lc_pouch_txn_validate_decision_req(client, req, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_txn_validate_pending_participants(client, req, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_txn_store_record(client, req, LC_POUCH_TXN_STATE_COMMITTED,
                                 error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_txn_load_record(client, req->txn_id, &record, error);
  if (rc == LC_OK) {
    rc = lc_pouch_txn_apply_record(client, &record, 0, error);
  }
  if (rc == LC_OK) {
    out->txn_id = lc_strdup_local(req->txn_id);
    out->state = lc_strdup_local("committed");
    out->correlation_id = lc_strdup_local("pouch-txn-commit");
    if (out->txn_id == NULL || out->state == NULL ||
        out->correlation_id == NULL) {
      lc_txn_decision_res_cleanup(out);
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch transaction commit result",
                        NULL, NULL, NULL);
    }
  }
  lc_pouch_txn_record_cleanup(client, &record);
  return rc;
}

int lc_pouch_client_txn_rollback_method(lc_client *self,
                                        const lc_txn_decision_req *req,
                                        lc_txn_decision_res *out,
                                        lc_error *error) {
  lc_client_handle *client;
  lc_pouch_txn_record record;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch txn_rollback requires self, req, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  memset(out, 0, sizeof(*out));
  memset(&record, 0, sizeof(record));
  rc = lc_pouch_txn_validate_decision_req(client, req, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_txn_validate_pending_participants(client, req, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_txn_store_record(client, req, LC_POUCH_TXN_STATE_ROLLED_BACK,
                                 error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_txn_load_record(client, req->txn_id, &record, error);
  if (rc == LC_OK) {
    rc = lc_pouch_txn_apply_record(client, &record, 0, error);
  }
  if (rc == LC_OK) {
    out->txn_id = lc_strdup_local(req->txn_id);
    out->state = lc_strdup_local("rolled_back");
    out->correlation_id = lc_strdup_local("pouch-txn-rollback");
    if (out->txn_id == NULL || out->state == NULL ||
        out->correlation_id == NULL) {
      lc_txn_decision_res_cleanup(out);
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch transaction rollback result",
                        NULL, NULL, NULL);
    }
  }
  lc_pouch_txn_record_cleanup(client, &record);
  return rc;
}

int lc_pouch_client_tc_lease_acquire_method(lc_client *self,
                                            const lc_tc_lease_acquire_req *req,
                                            lc_tc_lease_acquire_res *out,
                                            lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_unsupported(
      error, "pouch transaction coordinator is not supported");
}

int lc_pouch_client_tc_lease_renew_method(lc_client *self,
                                          const lc_tc_lease_renew_req *req,
                                          lc_tc_lease_renew_res *out,
                                          lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_unsupported(
      error, "pouch transaction coordinator is not supported");
}

int lc_pouch_client_tc_lease_release_method(lc_client *self,
                                            const lc_tc_lease_release_req *req,
                                            lc_tc_lease_release_res *out,
                                            lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_unsupported(
      error, "pouch transaction coordinator is not supported");
}

int lc_pouch_client_tc_leader_method(lc_client *self, lc_tc_leader_res *out,
                                     lc_error *error) {
  (void)self;
  (void)out;
  return lc_pouch_client_unsupported(
      error, "pouch transaction coordinator is not supported");
}

int lc_pouch_client_tc_cluster_announce_method(
    lc_client *self, const lc_tc_cluster_announce_req *req,
    lc_tc_cluster_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_unsupported(
      error, "pouch transaction coordinator is not supported");
}

int lc_pouch_client_tc_cluster_leave_method(lc_client *self,
                                            lc_tc_cluster_res *out,
                                            lc_error *error) {
  (void)self;
  (void)out;
  return lc_pouch_client_unsupported(
      error, "pouch transaction coordinator is not supported");
}

int lc_pouch_client_tc_cluster_list_method(lc_client *self,
                                           lc_tc_cluster_res *out,
                                           lc_error *error) {
  (void)self;
  (void)out;
  return lc_pouch_client_unsupported(
      error, "pouch transaction coordinator is not supported");
}

int lc_pouch_client_tc_rm_register_method(lc_client *self,
                                          const lc_tc_rm_register_req *req,
                                          lc_tc_rm_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_unsupported(
      error, "pouch transaction coordinator is not supported");
}

int lc_pouch_client_tc_rm_unregister_method(lc_client *self,
                                            const lc_tc_rm_unregister_req *req,
                                            lc_tc_rm_res *out,
                                            lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_unsupported(
      error, "pouch transaction coordinator is not supported");
}

int lc_pouch_client_tc_rm_list_method(lc_client *self, lc_tc_rm_list_res *out,
                                      lc_error *error) {
  (void)self;
  (void)out;
  return lc_pouch_client_unsupported(
      error, "pouch transaction coordinator is not supported");
}

int lc_pouch_client_enqueue_method(lc_client *self, const lc_enqueue_req *req,
                                   lc_source *src, lc_enqueue_res *out,
                                   lc_error *error) {
  lc_client_handle *client;
  lc_pouch_enqueue_opts opts;
  lc_pouch_queue_message_info info;
  const char *namespace_name;
  int rc;

  if (self == NULL || req == NULL || src == NULL || req->queue == NULL ||
      out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch enqueue requires self, req, src, queue, and out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  namespace_name = NULL;
  rc = lc_pouch_public_namespace(client, req->namespace_name, &namespace_name,
                                 error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(out, 0, sizeof(*out));
  memset(&opts, 0, sizeof(opts));
  memset(&info, 0, sizeof(info));
  opts.content_type = req->content_type;
  opts.delay_seconds = req->delay_seconds;
  opts.visibility_timeout_seconds = req->visibility_timeout_seconds;
  opts.ttl_seconds = req->ttl_seconds;
  opts.max_attempts = req->max_attempts;
  rc = client->pouch_store->enqueue_message(client->pouch_store, namespace_name,
                                            req->queue, src, &opts, &info,
                                            error);
  if (rc == LC_OK) {
    if (lc_pouch_copy_public(&out->namespace_name, info.namespace_name, error,
                             "failed to copy pouch namespace") != LC_OK ||
        lc_pouch_copy_public(&out->queue, info.queue, error,
                             "failed to copy pouch queue") != LC_OK ||
        lc_pouch_copy_public(&out->message_id, info.message_id, error,
                             "failed to copy pouch message id") != LC_OK ||
        lc_pouch_copy_public(&out->correlation_id, "pouch-enqueue", error,
                             "failed to copy pouch correlation id") != LC_OK) {
      lc_enqueue_res_cleanup(out);
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    } else {
      out->attempts = info.attempts;
      out->max_attempts = info.max_attempts;
      out->failure_attempts = info.failure_attempts;
      out->not_visible_until_unix = info.not_visible_until_unix;
      out->visibility_timeout_seconds = info.visibility_timeout_seconds;
      out->payload_bytes = info.payload_bytes;
    }
  }
  lc_pouch_queue_message_info_cleanup(&client->pouch_allocator, &info);
  return rc;
}

static int lc_pouch_client_dequeue_once(lc_client *self,
                                        const lc_dequeue_req *req,
                                        int with_state, lc_message **out,
                                        int *terminal_flag, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_dequeue_opts opts;
  lc_pouch_queue_message_info info;
  lc_engine_dequeue_response engine;
  lc_message_handle *message_handle;
  lc_lease_handle *state_handle;
  lc_source *body;
  const char *namespace_name;
  char *state_lease_id;
  char *state_txn_id;
  char *state_etag;
  long state_fencing_token;
  long state_lease_expires_at_unix;
  int rc;

  if (self == NULL || req == NULL || req->queue == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch dequeue requires self, req, queue, and out",
                        NULL, NULL, NULL);
  }
  if (req->owner == NULL || req->owner[0] == '\0') {
    return lc_error_set(error, LC_ERR_SERVER, 400L,
                        "pouch dequeue requires owner", NULL, "missing_owner",
                        NULL);
  }
  client = (lc_client_handle *)self;
  namespace_name = NULL;
  rc = lc_pouch_public_namespace(client, req->namespace_name, &namespace_name,
                                 error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&opts, 0, sizeof(opts));
  memset(&info, 0, sizeof(info));
  memset(&engine, 0, sizeof(engine));
  body = NULL;
  state_lease_id = NULL;
  state_txn_id = NULL;
  state_etag = NULL;
  state_fencing_token = 0L;
  state_lease_expires_at_unix = 0L;
  *out = NULL;
  opts.owner = req->owner;
  opts.txn_id = req->txn_id;
  opts.start_after = req->start_after;
  opts.visibility_timeout_seconds = req->visibility_timeout_seconds;
  rc = client->pouch_store->dequeue_message(client->pouch_store, namespace_name,
                                            req->queue, &opts, &body, &info,
                                            error);
  if (rc == LC_OK && body != NULL) {
    lc_pouch_queue_info_to_engine(&info, &engine);
    engine.correlation_id = "pouch-dequeue";
    if (with_state) {
      rc = lc_pouch_prepare_queue_state_lease(
          client, namespace_name, req, &info, &state_lease_id, &state_txn_id,
          &state_etag, &state_fencing_token, &state_lease_expires_at_unix,
          error);
      if (rc != LC_OK) {
        body->close(body);
        lc_pouch_queue_message_info_cleanup(&client->pouch_allocator, &info);
        return rc;
      }
      engine.state_lease_id = state_lease_id;
      engine.state_txn_id = state_txn_id;
      engine.state_etag = state_etag;
      engine.state_fencing_token = state_fencing_token;
      engine.state_lease_expires_at_unix = state_lease_expires_at_unix;
    }
    *out = lc_message_new(client, &engine, body, terminal_flag);
    if (*out == NULL) {
      body->close(body);
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch message handle", NULL, NULL,
                        NULL);
    } else if (with_state) {
      message_handle = (lc_message_handle *)(*out);
      if (message_handle->state_lease != NULL) {
        lc_pouch_install_lease_methods(message_handle->state_lease);
        state_handle = (lc_lease_handle *)message_handle->state_lease;
        state_handle->lease_expires_at_unix = state_lease_expires_at_unix;
        state_handle->pub.lease_expires_at_unix = state_lease_expires_at_unix;
        state_handle->pouch_txn_explicit =
            req->txn_id != NULL && req->txn_id[0] != '\0';
      }
    }
  }
  lc_client_free(client, state_etag);
  lc_client_free(client, state_txn_id);
  lc_client_free(client, state_lease_id);
  lc_pouch_queue_message_info_cleanup(&client->pouch_allocator, &info);
  return rc;
}

static int lc_pouch_client_dequeue_wait(lc_client *self,
                                        const lc_dequeue_req *req,
                                        int with_state, lc_message **out,
                                        int *terminal_flag, lc_error *error) {
  int64_t deadline_ms;
  int64_t now_ms;
  int64_t remaining_ms;
  long sleep_ms;
  int rc;

  if (req != NULL && req->wait_seconds < 0L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch dequeue wait_seconds must be non-negative", NULL,
                        NULL, NULL);
  }
  deadline_ms = 0L;
  if (req != NULL && req->wait_seconds > 0L) {
    now_ms = lc_pouch_now_millis();
    if (now_ms > 0L) {
      deadline_ms = now_ms + req->wait_seconds * 1000L;
    }
  }
  while (1) {
    rc = lc_pouch_client_dequeue_once(self, req, with_state, out, terminal_flag,
                                      error);
    if (rc != LC_OK || out == NULL || *out != NULL || deadline_ms <= 0L) {
      return rc;
    }
    now_ms = lc_pouch_now_millis();
    if (now_ms <= 0L || now_ms >= deadline_ms) {
      return LC_OK;
    }
    remaining_ms = deadline_ms - now_ms;
    sleep_ms = remaining_ms < 100L ? (long)remaining_ms : 100L;
    lc_pouch_sleep_millis(sleep_ms);
  }
}

int lc_pouch_client_dequeue_method(lc_client *self, const lc_dequeue_req *req,
                                   lc_message **out, lc_error *error) {
  return lc_pouch_client_dequeue_wait(self, req, 0, out, NULL, error);
}

int lc_pouch_client_dequeue_with_state_method(lc_client *self,
                                              const lc_dequeue_req *req,
                                              lc_message **out,
                                              lc_error *error) {
  return lc_pouch_client_dequeue_wait(self, req, 1, out, NULL, error);
}

int lc_pouch_client_dequeue_batch_method(lc_client *self,
                                         const lc_dequeue_req *req,
                                         lc_dequeue_batch_res *out,
                                         lc_error *error) {
  lc_client_handle *client;
  lc_dequeue_req single_req;
  lc_message *message;
  lc_message **grown;
  char *cursor;
  char *next_cursor;
  int limit;
  int index;
  int rc;

  if (self == NULL || req == NULL || req->queue == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch dequeue_batch requires self, req, queue, and "
                        "out",
                        NULL, NULL, NULL);
  }
  if (req->owner == NULL || req->owner[0] == '\0') {
    return lc_error_set(error, LC_ERR_SERVER, 400L,
                        "pouch dequeue_batch requires owner", NULL,
                        "missing_owner", NULL);
  }
  if (req->wait_seconds < 0L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch dequeue wait_seconds must be non-negative", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  memset(out, 0, sizeof(*out));
  single_req = *req;
  single_req.page_size = 1;
  cursor = NULL;
  limit = req->page_size > 0 ? req->page_size : 1;
  for (index = 0; index < limit; ++index) {
    message = NULL;
    if (index == 0) {
      rc = lc_pouch_client_dequeue_wait(self, &single_req, 0, &message, NULL,
                                        error);
      single_req.wait_seconds = 0L;
    } else {
      rc = lc_pouch_client_dequeue_once(self, &single_req, 0, &message, NULL,
                                        error);
    }
    if (rc != LC_OK) {
      lc_dequeue_batch_cleanup(out);
      lc_client_free(client, cursor);
      return rc;
    }
    if (message == NULL) {
      break;
    }
    next_cursor = lc_client_strdup(client, message->next_cursor);
    if (message->next_cursor != NULL && next_cursor == NULL) {
      message->close(message);
      lc_dequeue_batch_cleanup(out);
      lc_client_free(client, cursor);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to copy pouch dequeue cursor", NULL, NULL,
                          NULL);
    }
    grown = (lc_message **)lc_realloc_local(
        out->messages, (out->count + 1U) * sizeof(out->messages[0]));
    if (grown == NULL) {
      message->close(message);
      lc_dequeue_batch_cleanup(out);
      lc_client_free(client, next_cursor);
      lc_client_free(client, cursor);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to grow pouch dequeue batch", NULL, NULL,
                          NULL);
    }
    lc_client_free(client, cursor);
    cursor = next_cursor;
    single_req.start_after = cursor;
    out->messages = grown;
    out->messages[out->count] = message;
    out->count += 1U;
  }
  lc_client_free(client, cursor);
  return LC_OK;
}

static int lc_pouch_client_subscribe_common(lc_client *self,
                                            const lc_dequeue_req *req,
                                            const lc_consumer *consumer,
                                            int with_state, lc_error *error) {
  lc_client_handle *client;
  lc_dequeue_req single_req;
  lc_message *message;
  lc_nack_req nack_req;
  lc_error nack_error;
  char *cursor;
  char *next_cursor;
  int limit;
  int index;
  int terminal;
  int rc;
  int64_t deadline_ms;
  int64_t now_ms;
  int64_t remaining_ms;
  long sleep_ms;

  if (self == NULL || req == NULL || req->queue == NULL || consumer == NULL ||
      consumer->handle == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch subscribe requires self, req, queue, and "
                        "consumer",
                        NULL, NULL, NULL);
  }
  if (req->owner == NULL || req->owner[0] == '\0') {
    return lc_error_set(error, LC_ERR_SERVER, 400L,
                        "pouch subscribe requires owner", NULL, "missing_owner",
                        NULL);
  }
  if (req->wait_seconds < 0L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch dequeue wait_seconds must be non-negative", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  single_req = *req;
  single_req.page_size = 1;
  cursor = NULL;
  limit = req->page_size > 0 ? req->page_size : 1;
  deadline_ms = 0L;
  if (req->wait_seconds > 0L) {
    now_ms = lc_pouch_now_millis();
    if (now_ms > 0L) {
      deadline_ms = now_ms + req->wait_seconds * 1000L;
    }
  }
  index = 0;
  while (index < limit) {
    terminal = 0;
    message = NULL;
    rc = lc_pouch_client_dequeue_once(self, &single_req, with_state, &message,
                                      &terminal, error);
    if (rc != LC_OK) {
      lc_client_free(client, cursor);
      return rc;
    }
    if (message == NULL) {
      if (deadline_ms > 0L) {
        now_ms = lc_pouch_now_millis();
        if (now_ms > 0L && now_ms < deadline_ms) {
          remaining_ms = deadline_ms - now_ms;
          sleep_ms = remaining_ms < 100L ? (long)remaining_ms : 100L;
          lc_pouch_sleep_millis(sleep_ms);
          continue;
        }
      }
      lc_client_free(client, cursor);
      return LC_OK;
    }
    next_cursor = lc_client_strdup(client, message->next_cursor);
    if (message->next_cursor != NULL && next_cursor == NULL) {
      message->close(message);
      lc_client_free(client, cursor);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to copy pouch subscribe cursor", NULL, NULL,
                          NULL);
    }
    rc = consumer->handle(consumer->context, message, error);
    if (rc == LC_OK && !terminal) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "consumer callback must ack() or nack() before "
                        "returning LC_OK",
                        NULL, NULL, NULL);
    }
    if (rc != LC_OK && !terminal) {
      lc_nack_req_init(&nack_req);
      nack_req.intent = LC_NACK_INTENT_FAILURE;
      nack_req.delay_seconds = 0L;
      lc_error_init(&nack_error);
      if (message->nack(message, &nack_req, &nack_error) == LC_OK) {
        terminal = 1;
        message = NULL;
      } else if (error != NULL && error->code == LC_OK) {
        lc_error_set(error, nack_error.code, nack_error.http_status,
                     nack_error.message, nack_error.detail,
                     nack_error.server_code, nack_error.correlation_id);
      }
      lc_error_cleanup(&nack_error);
    }
    if (message != NULL && !terminal) {
      message->close(message);
    }
    if (rc != LC_OK) {
      lc_client_free(client, next_cursor);
      lc_client_free(client, cursor);
      return error != NULL && error->code != LC_OK ? error->code : rc;
    }
    lc_client_free(client, cursor);
    cursor = next_cursor;
    single_req.start_after = cursor;
    ++index;
  }
  lc_client_free(client, cursor);
  return LC_OK;
}

int lc_pouch_client_subscribe_method(lc_client *self, const lc_dequeue_req *req,
                                     const lc_consumer *consumer,
                                     lc_error *error) {
  return lc_pouch_client_subscribe_common(self, req, consumer, 0, error);
}

int lc_pouch_client_subscribe_with_state_method(lc_client *self,
                                                const lc_dequeue_req *req,
                                                const lc_consumer *consumer,
                                                lc_error *error) {
  return lc_pouch_client_subscribe_common(self, req, consumer, 1, error);
}

int lc_pouch_client_queue_ack_method(lc_client *self, const lc_ack_op *req,
                                     lc_ack_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_queue_ref ref;
  const char *namespace_name;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue_ack requires self, req, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  memset(out, 0, sizeof(*out));
  lc_pouch_queue_ref_from_message(&req->message, &ref);
  namespace_name = NULL;
  rc = lc_pouch_public_namespace(client, ref.namespace_name, &namespace_name,
                                 error);
  if (rc != LC_OK) {
    return rc;
  }
  ref.namespace_name = namespace_name;
  rc = client->pouch_store->ack_message(client->pouch_store, &ref, &out->acked,
                                        error);
  if (rc == LC_OK &&
      lc_pouch_copy_public(&out->correlation_id, "pouch-ack", error,
                           "failed to copy pouch correlation id") != LC_OK) {
    lc_ack_res_cleanup(out);
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  return rc;
}

int lc_pouch_client_queue_nack_method(lc_client *self, const lc_nack_op *req,
                                      lc_nack_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_queue_ref ref;
  lc_pouch_queue_message_info info;
  const char *namespace_name;
  int count_failure;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue_nack requires self, req, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  memset(out, 0, sizeof(*out));
  memset(&info, 0, sizeof(info));
  lc_pouch_queue_ref_from_message(&req->message, &ref);
  namespace_name = NULL;
  rc = lc_pouch_public_namespace(client, ref.namespace_name, &namespace_name,
                                 error);
  if (rc != LC_OK) {
    return rc;
  }
  ref.namespace_name = namespace_name;
  count_failure = req->intent != LC_NACK_INTENT_DEFER;
  rc = client->pouch_store->nack_message(client->pouch_store, &ref,
                                         req->delay_seconds, count_failure,
                                         &info, error);
  if (rc == LC_OK) {
    out->requeued = info.failure_attempts < info.max_attempts;
    if (lc_pouch_copy_public(&out->meta_etag, info.meta_etag, error,
                             "failed to copy pouch queue etag") != LC_OK ||
        lc_pouch_copy_public(&out->correlation_id, "pouch-nack", error,
                             "failed to copy pouch correlation id") != LC_OK) {
      lc_nack_res_cleanup(out);
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
  }
  lc_pouch_queue_message_info_cleanup(&client->pouch_allocator, &info);
  return rc;
}

int lc_pouch_client_queue_extend_method(lc_client *self,
                                        const lc_extend_op *req,
                                        lc_extend_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_queue_ref ref;
  lc_pouch_queue_message_info info;
  const char *namespace_name;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch queue_extend requires self, req, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  memset(out, 0, sizeof(*out));
  memset(&info, 0, sizeof(info));
  lc_pouch_queue_ref_from_message(&req->message, &ref);
  namespace_name = NULL;
  rc = lc_pouch_public_namespace(client, ref.namespace_name, &namespace_name,
                                 error);
  if (rc != LC_OK) {
    return rc;
  }
  ref.namespace_name = namespace_name;
  rc = client->pouch_store->extend_message(
      client->pouch_store, &ref, req->extend_by_seconds, &info, error);
  if (rc == LC_OK) {
    out->lease_expires_at_unix = info.lease_expires_at_unix;
    out->visibility_timeout_seconds = info.visibility_timeout_seconds;
    if (lc_pouch_copy_public(&out->meta_etag, info.meta_etag, error,
                             "failed to copy pouch queue etag") != LC_OK ||
        lc_pouch_copy_public(&out->correlation_id, "pouch-extend", error,
                             "failed to copy pouch correlation id") != LC_OK) {
      lc_extend_res_cleanup(out);
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
  }
  lc_pouch_queue_message_info_cleanup(&client->pouch_allocator, &info);
  return rc;
}

int lc_pouch_message_ack_method(lc_message *self, lc_error *error) {
  lc_message_handle *message;
  lc_ack_op req;
  lc_ack_res res;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch message ack requires self", NULL, NULL, NULL);
  }
  message = (lc_message_handle *)self;
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  req.message.namespace_name = message->namespace_name;
  req.message.queue = message->queue;
  req.message.message_id = message->message_id;
  req.message.lease_id = message->lease_id;
  req.message.txn_id = message->txn_id;
  req.message.fencing_token = message->fencing_token;
  req.message.meta_etag = message->meta_etag;
  rc = lc_pouch_client_queue_ack_method(&message->client->pub, &req, &res,
                                        error);
  lc_ack_res_cleanup(&res);
  if (rc == LC_OK) {
    if (message->terminal_flag != NULL) {
      *message->terminal_flag = 1;
    }
    lc_message_close_method(self);
  }
  return rc;
}

int lc_pouch_message_nack_method(lc_message *self, const lc_nack_req *opts,
                                 lc_error *error) {
  lc_message_handle *message;
  lc_nack_op req;
  lc_nack_res res;
  int rc;

  if (self == NULL || opts == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch message nack requires self and req", NULL, NULL,
                        NULL);
  }
  message = (lc_message_handle *)self;
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  req.message.namespace_name = message->namespace_name;
  req.message.queue = message->queue;
  req.message.message_id = message->message_id;
  req.message.lease_id = message->lease_id;
  req.message.txn_id = message->txn_id;
  req.message.fencing_token = message->fencing_token;
  req.message.meta_etag = message->meta_etag;
  req.delay_seconds = opts->delay_seconds;
  req.intent = opts->intent;
  req.last_error_json = opts->last_error_json;
  rc = lc_pouch_client_queue_nack_method(&message->client->pub, &req, &res,
                                         error);
  lc_nack_res_cleanup(&res);
  if (rc == LC_OK) {
    if (message->terminal_flag != NULL) {
      *message->terminal_flag = 1;
    }
    lc_message_close_method(self);
  }
  return rc;
}

int lc_pouch_message_extend_method(lc_message *self, const lc_extend_req *opts,
                                   lc_error *error) {
  lc_message_handle *message;
  lc_extend_op req;
  lc_extend_res res;
  char *meta_etag;
  int rc;

  if (self == NULL || opts == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch message extend requires self and req", NULL,
                        NULL, NULL);
  }
  message = (lc_message_handle *)self;
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  req.message.namespace_name = message->namespace_name;
  req.message.queue = message->queue;
  req.message.message_id = message->message_id;
  req.message.lease_id = message->lease_id;
  req.message.txn_id = message->txn_id;
  req.message.fencing_token = message->fencing_token;
  req.message.meta_etag = message->meta_etag;
  req.extend_by_seconds = opts->extend_by_seconds;
  rc = lc_pouch_client_queue_extend_method(&message->client->pub, &req, &res,
                                           error);
  if (rc == LC_OK) {
    meta_etag = lc_client_strdup(message->client, res.meta_etag);
    if (res.meta_etag != NULL && meta_etag == NULL) {
      lc_extend_res_cleanup(&res);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to copy pouch queue etag", NULL, NULL, NULL);
    }
    lc_client_free(message->client, message->meta_etag);
    message->meta_etag = meta_etag;
    message->lease_expires_at_unix = res.lease_expires_at_unix;
    message->visibility_timeout_seconds = res.visibility_timeout_seconds;
    message->fencing_token += 1L;
    message->pub.meta_etag = message->meta_etag;
    message->pub.lease_expires_at_unix = message->lease_expires_at_unix;
    message->pub.visibility_timeout_seconds =
        message->visibility_timeout_seconds;
    message->pub.fencing_token = message->fencing_token;
  }
  lc_extend_res_cleanup(&res);
  return rc;
}

int lc_pouch_lease_describe_method(lc_lease *self, lc_error *error) {
  lc_lease_handle *lease;
  lc_pouch_meta_record record;
  lc_lease_ref ref;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease describe requires self", NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  memset(&record, 0, sizeof(record));
  memset(&ref, 0, sizeof(ref));
  ref.namespace_name = lease->namespace_name;
  ref.key = lease->key;
  ref.lease_id = lease->lease_id;
  ref.txn_id = lease->txn_id;
  ref.fencing_token = lease->fencing_token;
  rc = lc_pouch_validate_active_lease(lease->client, &ref, &record, error);
  if (rc == LC_OK) {
    rc = lc_pouch_refresh_lease(lease, &record.meta, error);
  }
  lc_pouch_meta_record_cleanup(&lease->client->pouch_allocator, &record);
  return rc;
}

int lc_pouch_lease_get_method(lc_lease *self, lc_sink *dst,
                              const lc_get_opts *opts, lc_get_res *out,
                              lc_error *error) {
  lc_lease_handle *lease;
  char *new_state_etag;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease get requires self", NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  rc = lc_pouch_client_get_in_namespace(lease->client, lease->namespace_name,
                                        lease->key, opts, dst, out, error);
  if (rc == LC_OK && out != NULL) {
    new_state_etag = lc_client_strdup(lease->client, out->etag);
    if (out->etag != NULL && new_state_etag == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to copy pouch lease state etag", NULL, NULL,
                          NULL);
    }
    lc_client_free(lease->client, lease->state_etag);
    lease->state_etag = new_state_etag;
    lease->version = out->version;
    lease->fencing_token = out->fencing_token;
    lease->pub.state_etag = lease->state_etag;
    lease->pub.version = lease->version;
    lease->pub.fencing_token = lease->fencing_token;
  }
  return rc;
}

int lc_pouch_lease_update_method(lc_lease *self, lc_source *src,
                                 const lc_update_opts *opts, lc_error *error) {
  lc_lease_handle *lease;
  lc_update_req req;
  lc_update_res res;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease update requires self", NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  if (lease->pouch_stage_active || lease->pouch_txn_explicit) {
    return lc_pouch_lease_staged_update_method(self, src, opts, error);
  }
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  req.lease.namespace_name = lease->namespace_name;
  req.lease.key = lease->key;
  req.lease.lease_id = lease->lease_id;
  req.lease.txn_id = lease->txn_id;
  req.lease.fencing_token = lease->fencing_token;
  req.if_state_etag = opts != NULL ? opts->if_state_etag : NULL;
  req.if_version = opts != NULL ? opts->if_version : lease->version;
  req.has_if_version =
      opts != NULL ? opts->has_if_version : (lease->version > 0L);
  req.content_type = opts != NULL ? opts->content_type : NULL;
  rc = lc_pouch_client_update_method(&lease->client->pub, &req, src, &res,
                                     error);
  if (rc == LC_OK) {
    lc_client_free(lease->client, lease->state_etag);
    lease->state_etag = lc_client_strdup(lease->client, res.new_state_etag);
    if (res.new_state_etag != NULL && lease->state_etag == NULL) {
      lc_update_res_cleanup(&res);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to copy pouch lease state etag", NULL, NULL,
                          NULL);
    }
    lease->version = res.new_version;
    lease->pub.state_etag = lease->state_etag;
    lease->pub.version = lease->version;
  }
  lc_update_res_cleanup(&res);
  return rc;
}

int lc_pouch_lease_metadata_method(lc_lease *self, const lc_metadata_req *opts,
                                   lc_error *error) {
  lc_lease_handle *lease;
  lc_metadata_op req;
  lc_metadata_res res;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease metadata requires self", NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  req.lease.namespace_name = lease->namespace_name;
  req.lease.key = lease->key;
  req.lease.lease_id = lease->lease_id;
  req.lease.txn_id = lease->txn_id;
  req.lease.fencing_token = lease->fencing_token;
  if (opts != NULL) {
    req.has_query_hidden = opts->has_query_hidden;
    req.query_hidden = opts->query_hidden;
    req.if_version = opts->if_version;
    req.has_if_version = opts->has_if_version;
  }
  if (!req.has_if_version && lease->version > 0L) {
    req.if_version = lease->version;
    req.has_if_version = 1;
  }
  rc = lc_pouch_client_metadata_method(&lease->client->pub, &req, &res, error);
  if (rc == LC_OK) {
    lease->version = res.version;
    lease->has_query_hidden = res.has_query_hidden;
    lease->query_hidden = res.query_hidden;
    lease->pub.version = lease->version;
    lease->pub.has_query_hidden = lease->has_query_hidden;
    lease->pub.query_hidden = lease->query_hidden;
  }
  lc_metadata_res_cleanup(&res);
  return rc;
}

int lc_pouch_lease_remove_method(lc_lease *self, const lc_remove_req *opts,
                                 lc_error *error) {
  lc_lease_handle *lease;
  lc_remove_op req;
  lc_remove_res res;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease remove requires self", NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  if (lease->pouch_stage_active || lease->pouch_txn_explicit) {
    return lc_pouch_lease_staged_remove_method(self, opts, error);
  }
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  req.lease.namespace_name = lease->namespace_name;
  req.lease.key = lease->key;
  req.lease.lease_id = lease->lease_id;
  req.lease.txn_id = lease->txn_id;
  req.lease.fencing_token = lease->fencing_token;
  if (opts != NULL) {
    req.if_state_etag = opts->if_state_etag;
    req.if_version = opts->if_version;
    req.has_if_version = opts->has_if_version;
  }
  if (!req.has_if_version && lease->version > 0L) {
    req.if_version = lease->version;
    req.has_if_version = 1;
  }
  rc = lc_pouch_client_remove_method(&lease->client->pub, &req, &res, error);
  if (rc == LC_OK && res.removed) {
    lc_client_free(lease->client, lease->state_etag);
    lease->state_etag = NULL;
    lease->version = res.new_version;
    lease->pub.state_etag = NULL;
    lease->pub.version = lease->version;
  }
  lc_remove_res_cleanup(&res);
  return rc;
}

int lc_pouch_lease_keepalive_method(lc_lease *self,
                                    const lc_keepalive_req *opts,
                                    lc_error *error) {
  lc_lease_handle *lease;
  lc_keepalive_op req;
  lc_keepalive_res res;
  int rc;

  if (self == NULL || opts == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease keepalive requires self and req", NULL,
                        NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  req.lease.namespace_name = lease->namespace_name;
  req.lease.key = lease->key;
  req.lease.lease_id = lease->lease_id;
  req.lease.txn_id = lease->txn_id;
  req.lease.fencing_token = lease->fencing_token;
  req.ttl_seconds = opts->ttl_seconds;
  rc = lc_pouch_client_keepalive_method(&lease->client->pub, &req, &res, error);
  if (rc == LC_OK) {
    lease->lease_expires_at_unix = res.lease_expires_at_unix;
    lease->pub.lease_expires_at_unix = lease->lease_expires_at_unix;
  }
  lc_keepalive_res_cleanup(&res);
  return rc;
}

int lc_pouch_lease_release_method(lc_lease *self, const lc_release_req *opts,
                                  lc_error *error) {
  lc_lease_handle *lease;
  lc_release_op req;
  lc_release_res res;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease release requires self", NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  req.lease.namespace_name = lease->namespace_name;
  req.lease.key = lease->key;
  req.lease.lease_id = lease->lease_id;
  req.lease.txn_id = lease->txn_id;
  req.lease.fencing_token = lease->fencing_token;
  req.rollback = opts != NULL ? opts->rollback : 0;
  rc = lc_pouch_client_release_method(&lease->client->pub, &req, &res, error);
  lc_release_res_cleanup(&res);
  if (rc == LC_OK) {
    lc_lease_close_method(self);
  }
  return rc;
}

int lc_pouch_lease_attach_method(lc_lease *self, const lc_attach_req *opts,
                                 lc_source *src, lc_attach_res *out,
                                 lc_error *error) {
  lc_lease_handle *lease;
  lc_attach_op req;
  int rc;

  if (self == NULL || opts == NULL || src == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease attach requires self, req, src, and out",
                        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  memset(&req, 0, sizeof(req));
  lc_pouch_lease_ref_from_handle(lease, &req.lease);
  req.name = opts->name;
  req.content_type = opts->content_type;
  req.max_bytes = opts->max_bytes;
  req.has_max_bytes = opts->has_max_bytes;
  req.prevent_overwrite = opts->prevent_overwrite;
  if (lease->pouch_txn_explicit) {
    rc = lc_pouch_lease_staged_attach(lease, opts, src, out, error);
  } else {
    rc = lc_pouch_client_attach_method(&lease->client->pub, &req, src, out,
                                       error);
  }
  if (rc == LC_OK) {
    lease->version = out->version;
    lease->pub.version = lease->version;
  }
  return rc;
}

int lc_pouch_lease_list_attachments_method(lc_lease *self,
                                           lc_attachment_list *out,
                                           lc_error *error) {
  lc_lease_handle *lease;
  lc_attachment_list_req req;

  if (self == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease list_attachments requires self and out",
                        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  if (lease->pouch_txn_explicit) {
    return lc_pouch_lease_staged_list_attachments(lease, out, error);
  }
  memset(&req, 0, sizeof(req));
  lc_pouch_lease_ref_from_handle(lease, &req.lease);
  return lc_pouch_client_list_attachments_method(&lease->client->pub, &req, out,
                                                 error);
}

int lc_pouch_lease_get_attachment_method(lc_lease *self,
                                         const lc_attachment_get_req *opts,
                                         lc_sink *dst,
                                         lc_attachment_get_res *out,
                                         lc_error *error) {
  lc_lease_handle *lease;
  lc_attachment_get_op req;

  if (self == NULL || opts == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease get_attachment requires self, req, dst, "
                        "and out",
                        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  if (lease->pouch_txn_explicit && !opts->public_read) {
    return lc_pouch_lease_staged_get_attachment(lease, opts, dst, out, error);
  }
  memset(&req, 0, sizeof(req));
  lc_pouch_lease_ref_from_handle(lease, &req.lease);
  req.selector = opts->selector;
  req.public_read = opts->public_read;
  return lc_pouch_client_get_attachment_method(&lease->client->pub, &req, dst,
                                               out, error);
}

int lc_pouch_lease_delete_attachment_method(
    lc_lease *self, const lc_attachment_selector *selector, int *deleted,
    lc_error *error) {
  lc_lease_handle *lease;
  lc_attachment_delete_op req;
  int rc;

  if (self == NULL || selector == NULL || deleted == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease delete_attachment requires self, "
                        "selector, and deleted",
                        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  memset(&req, 0, sizeof(req));
  lc_pouch_lease_ref_from_handle(lease, &req.lease);
  req.selector = *selector;
  if (lease->pouch_txn_explicit) {
    rc = lc_pouch_lease_staged_delete_attachment(lease, selector, deleted,
                                                 error);
  } else {
    rc = lc_pouch_client_delete_attachment_method(&lease->client->pub, &req,
                                                  deleted, error);
  }
  if (rc == LC_OK && *deleted) {
    lease->version += 1L;
    lease->pub.version = lease->version;
  }
  return rc;
}

int lc_pouch_lease_delete_all_attachments_method(lc_lease *self,
                                                 int *deleted_count,
                                                 lc_error *error) {
  lc_lease_handle *lease;
  lc_attachment_delete_all_op req;
  int rc;

  if (self == NULL || deleted_count == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease delete_all_attachments requires self and "
                        "deleted_count",
                        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  memset(&req, 0, sizeof(req));
  lc_pouch_lease_ref_from_handle(lease, &req.lease);
  if (lease->pouch_txn_explicit) {
    rc = lc_pouch_lease_staged_delete_all_attachments(lease, deleted_count,
                                                      error);
  } else {
    rc = lc_pouch_client_delete_all_attachments_method(
        &lease->client->pub, &req, deleted_count, error);
  }
  if (rc == LC_OK && *deleted_count > 0) {
    lease->version += 1L;
    lease->pub.version = lease->version;
  }
  return rc;
}

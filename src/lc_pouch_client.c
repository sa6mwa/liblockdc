#include "lc_api_internal.h"
#include "lc_internal.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#define LC_POUCH_RESERVED_BACKEND_NAMESPACE ".lockd"

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
  if (strcmp(resolved, LC_POUCH_RESERVED_BACKEND_NAMESPACE) == 0) {
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
static int lc_pouch_lease_staged_update_method(lc_lease *self, lc_source *src,
                                               const lc_update_opts *opts,
                                               lc_error *error);
static int lc_pouch_refresh_lease(lc_lease_handle *lease,
                                  const lc_pouch_meta *meta, lc_error *error);

typedef struct lc_pouch_acquire_for_update_file_sink {
  FILE *fp;
} lc_pouch_acquire_for_update_file_sink;

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
                                                size_t limit,
                                                lc_error *error) {
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
  rc = lease->client->pouch_store->read_state(
      lease->client->pouch_store, namespace_name, lease->key, &body, &info,
      error);
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
    rc = lc_lonejson_parse_prepared_file(
        runtime, fp, map, dst, error, "failed to parse mapped lease state");
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
  if (lc_pouch_copy_public(&out->content_type, info.content_type, error,
                           "failed to copy pouch content type") != LC_OK ||
      lc_pouch_copy_public(&out->etag, info.etag, error,
                           "failed to copy pouch etag") != LC_OK) {
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
                        "pouch lease save requires self, map, and source",
                        NULL, NULL, NULL);
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
                        "failed to create pouch mapped save buffer", NULL,
                        NULL, NULL);
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
                        "failed to rewind pouch mapped save buffer", NULL,
                        NULL, NULL);
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

static int lc_pouch_lease_mutate_unsupported(lc_lease *self,
                                             const lc_mutate_req *req,
                                             lc_error *error) {
  (void)self;
  (void)req;
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch lease mutate requires the LQL/mutate slice", NULL,
                      NULL, NULL);
}

static int lc_pouch_lease_mutate_local_unsupported(
    lc_lease *self, const lc_mutate_local_req *req, lc_error *error) {
  (void)self;
  (void)req;
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch lease local mutate requires the LQL/mutate slice",
                      NULL, NULL, NULL);
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
  rc = client->pouch_store->load_meta(
      client->pouch_store, namespace_name, lease->key, record, error);
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
    return lc_error_set(error, LC_ERR_SERVER, 403L, "pouch lease expired",
                        NULL, "lease_expired", NULL);
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
                           meta->txn_id != NULL ? meta->txn_id
                                                : lease->txn_id,
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
  lease->mutate = lc_pouch_lease_mutate_unsupported;
  lease->mutate_local = lc_pouch_lease_mutate_local_unsupported;
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
  long now_unix;
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
  client = (lc_client_handle *)self;
  allocator = &client->pouch_allocator;
  namespace_name = NULL;
  txn_id = req->txn_id;
  generated_txn_id = NULL;
  rc = lc_pouch_public_namespace(client, req->namespace_name, &namespace_name,
                                 error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&existing, 0, sizeof(existing));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  *out = NULL;
  rc = client->pouch_store->load_meta(client->pouch_store, namespace_name,
                                      req->key, &existing, error);
  if (rc != LC_OK) {
    return rc;
  }
  now_unix = lc_pouch_now_unix();
  if (existing.found && existing.meta.lease_expires_at_unix > now_unix) {
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
                          "failed to allocate pouch transaction id", NULL,
                          NULL, NULL);
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
    generated_txn_id =
        lc_pouch_new_txn_id(client, lease_handle->key,
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

int lc_pouch_client_get_method(lc_client *self, const char *key,
                               const lc_get_opts *opts, lc_sink *dst,
                               lc_get_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_source *body;
  lc_pouch_state_info info;
  lc_pouch_meta_record record;
  lc_pouch_allocator *allocator;
  const char *namespace_name;
  unsigned char buffer[8192];
  size_t got;
  int rc;

  if (self == NULL || key == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch get requires self, key, dst, and out", NULL,
                        NULL, NULL);
  }
  (void)opts;
  client = (lc_client_handle *)self;
  allocator = &client->pouch_allocator;
  namespace_name = NULL;
  rc = lc_pouch_public_namespace(client, NULL, &namespace_name, error);
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
  if (lc_pouch_copy_public(&out->content_type, info.content_type, error,
                           "failed to copy pouch content type") != LC_OK ||
      lc_pouch_copy_public(&out->etag, info.etag, error,
                           "failed to copy pouch etag") != LC_OK) {
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
  if (record.found) {
    out->version = record.meta.version;
    out->fencing_token = record.meta.fencing_token;
  }
  lc_pouch_meta_record_cleanup(allocator, &record);
  lc_pouch_state_info_cleanup(allocator, &info);
  return LC_OK;
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
  if (lc_pouch_copy_public(&out->content_type, info.content_type, error,
                           "failed to copy pouch content type") != LC_OK ||
      lc_pouch_copy_public(&out->etag, info.etag, error,
                           "failed to copy pouch etag") != LC_OK) {
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
                        "pouch staged update requires self and src", NULL,
                        NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  if (!lease->pouch_stage_active) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged update requires acquire_for_update",
                        NULL, NULL, NULL);
  }
  if (opts != NULL && opts->has_if_version &&
      opts->if_version != lease->version) {
    return lc_error_set(error, LC_ERR_SERVER, 412L,
                        "pouch staged update version precondition failed",
                        NULL, "precondition_failed", NULL);
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

  put_opts.content_type =
      opts != NULL && opts->content_type != NULL ? opts->content_type
                                                 : "application/json";
  if (lease->pouch_stage_dirty) {
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

int lc_pouch_client_attach_method(lc_client *self, const lc_attach_op *req,
                                  lc_source *src, lc_attach_res *out,
                                  lc_error *error) {
  lc_client_handle *client;
  lc_pouch_meta_record record;
  lc_pouch_store_meta_res stored;
  lc_pouch_put_object_opts opts;
  lc_pouch_object_info object;
  lc_pouch_meta next_meta;
  lc_pouch_allocator *allocator;
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
  memset(&record, 0, sizeof(record));
  memset(&stored, 0, sizeof(stored));
  memset(&opts, 0, sizeof(opts));
  memset(&object, 0, sizeof(object));
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

int lc_pouch_client_list_attachments_method(lc_client *self,
                                            const lc_attachment_list_req *req,
                                            lc_attachment_list *out,
                                            lc_error *error) {
  lc_client_handle *client;
  lc_pouch_meta_record record;
  lc_pouch_object_list objects;
  lc_pouch_allocator *allocator;
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
  rc = lc_pouch_validate_active_lease(client, &req->lease, &record, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = client->pouch_store->list_objects(client->pouch_store,
                                         record.namespace_name, req->lease.key,
                                         &objects, error);
  if (rc == LC_OK && objects.count > 0U) {
    out->items =
        (lc_attachment_info *)calloc(objects.count, sizeof(out->items[0]));
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
  rc = lc_pouch_validate_active_lease(client, &req->lease, &record, error);
  if (rc != LC_OK) {
    return rc;
  }
  selector.id = req->selector.id;
  selector.name = req->selector.name;
  rc = client->pouch_store->get_object(client->pouch_store,
                                       record.namespace_name, req->lease.key,
                                       &selector, &body, &object, error);
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
                             "failed to copy pouch queue head id") != LC_OK) {
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
      lc_pouch_copy_public(&event.head_message_id, stats.head_message_id,
                           error,
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
                      handler_error.server_code,
                      handler_error.correlation_id);
    lc_error_cleanup(&handler_error);
    return rc;
  }
  lc_error_cleanup(&handler_error);
  (void)handler_rc;
  return LC_OK;
}

static int lc_pouch_client_unsupported(lc_error *error, const char *message) {
  return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL, NULL);
}

int lc_pouch_client_query_method(lc_client *self, const lc_query_req *req,
                                 lc_sink *dst, lc_query_res *out,
                                 lc_error *error) {
  (void)self;
  (void)req;
  (void)dst;
  (void)out;
  return lc_pouch_client_unsupported(error,
                                     "pouch query requires the LQL slice");
}

int lc_pouch_client_query_keys_method(lc_client *self,
                                      const lc_query_req *req,
                                      const lc_query_key_handler *handler,
                                      void *context, lc_query_res *out,
                                      lc_error *error) {
  (void)self;
  (void)req;
  (void)handler;
  (void)context;
  (void)out;
  return lc_pouch_client_unsupported(error,
                                     "pouch query_keys requires the LQL slice");
}

int lc_pouch_client_get_namespace_config_method(
    lc_client *self, const lc_namespace_config_req *req,
    lc_namespace_config_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_unsupported(
      error, "pouch namespace management is not supported");
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
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_unsupported(
      error, "pouch index flush requires the LQL slice");
}

int lc_pouch_client_txn_replay_method(lc_client *self,
                                      const lc_txn_replay_req *req,
                                      lc_txn_replay_res *out,
                                      lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_unsupported(
      error, "pouch public transaction control is not supported");
}

int lc_pouch_client_txn_prepare_method(lc_client *self,
                                       const lc_txn_decision_req *req,
                                       lc_txn_decision_res *out,
                                       lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_unsupported(
      error, "pouch public transaction control is not supported");
}

int lc_pouch_client_txn_commit_method(lc_client *self,
                                      const lc_txn_decision_req *req,
                                      lc_txn_decision_res *out,
                                      lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_unsupported(
      error, "pouch public transaction control is not supported");
}

int lc_pouch_client_txn_rollback_method(lc_client *self,
                                        const lc_txn_decision_req *req,
                                        lc_txn_decision_res *out,
                                        lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_unsupported(
      error, "pouch public transaction control is not supported");
}

int lc_pouch_client_tc_lease_acquire_method(
    lc_client *self, const lc_tc_lease_acquire_req *req,
    lc_tc_lease_acquire_res *out, lc_error *error) {
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

int lc_pouch_client_tc_lease_release_method(
    lc_client *self, const lc_tc_lease_release_req *req,
    lc_tc_lease_release_res *out, lc_error *error) {
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
                                          lc_tc_rm_res *out,
                                          lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_unsupported(
      error, "pouch transaction coordinator is not supported");
}

int lc_pouch_client_tc_rm_unregister_method(
    lc_client *self, const lc_tc_rm_unregister_req *req, lc_tc_rm_res *out,
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
                             "failed to copy pouch message id") != LC_OK) {
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

static int lc_pouch_client_dequeue_one(lc_client *self,
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
  opts.visibility_timeout_seconds = req->visibility_timeout_seconds;
  rc = client->pouch_store->dequeue_message(client->pouch_store, namespace_name,
                                            req->queue, &opts, &body, &info,
                                            error);
  if (rc == LC_OK && body != NULL) {
    lc_pouch_queue_info_to_engine(&info, &engine);
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
      }
    }
  }
  lc_client_free(client, state_etag);
  lc_client_free(client, state_txn_id);
  lc_client_free(client, state_lease_id);
  lc_pouch_queue_message_info_cleanup(&client->pouch_allocator, &info);
  return rc;
}

int lc_pouch_client_dequeue_method(lc_client *self, const lc_dequeue_req *req,
                                   lc_message **out, lc_error *error) {
  return lc_pouch_client_dequeue_one(self, req, 0, out, NULL, error);
}

int lc_pouch_client_dequeue_with_state_method(lc_client *self,
                                              const lc_dequeue_req *req,
                                              lc_message **out,
                                              lc_error *error) {
  return lc_pouch_client_dequeue_one(self, req, 1, out, NULL, error);
}

int lc_pouch_client_dequeue_batch_method(lc_client *self,
                                         const lc_dequeue_req *req,
                                         lc_dequeue_batch_res *out,
                                         lc_error *error) {
  lc_dequeue_req single_req;
  lc_message *message;
  lc_message **grown;
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
  memset(out, 0, sizeof(*out));
  single_req = *req;
  single_req.page_size = 1;
  limit = req->page_size > 0 ? req->page_size : 1;
  for (index = 0; index < limit; ++index) {
    message = NULL;
    rc = lc_pouch_client_dequeue_one(self, &single_req, 0, &message, NULL,
                                     error);
    if (rc != LC_OK) {
      lc_dequeue_batch_cleanup(out);
      return rc;
    }
    if (message == NULL) {
      break;
    }
    grown = (lc_message **)realloc(out->messages, (out->count + 1U) *
                                                      sizeof(out->messages[0]));
    if (grown == NULL) {
      message->close(message);
      lc_dequeue_batch_cleanup(out);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to grow pouch dequeue batch", NULL, NULL,
                          NULL);
    }
    out->messages = grown;
    out->messages[out->count] = message;
    out->count += 1U;
  }
  return LC_OK;
}

static int lc_pouch_client_subscribe_common(lc_client *self,
                                            const lc_dequeue_req *req,
                                            const lc_consumer *consumer,
                                            int with_state, lc_error *error) {
  lc_dequeue_req single_req;
  lc_message *message;
  lc_nack_req nack_req;
  lc_error nack_error;
  int limit;
  int index;
  int terminal;
  int rc;

  if (self == NULL || req == NULL || req->queue == NULL || consumer == NULL ||
      consumer->handle == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch subscribe requires self, req, queue, and "
                        "consumer",
                        NULL, NULL, NULL);
  }
  if (req->owner == NULL || req->owner[0] == '\0') {
    return lc_error_set(error, LC_ERR_SERVER, 400L,
                        "pouch subscribe requires owner", NULL,
                        "missing_owner", NULL);
  }
  single_req = *req;
  single_req.page_size = 1;
  limit = req->page_size > 0 ? req->page_size : 1;
  for (index = 0; index < limit; ++index) {
    terminal = 0;
    message = NULL;
    rc = lc_pouch_client_dequeue_one(self, &single_req, with_state, &message,
                                     &terminal, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (message == NULL) {
      return LC_OK;
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
      return error != NULL && error->code != LC_OK ? error->code : rc;
    }
  }
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
    rc = lc_pouch_copy_public(&out->meta_etag, info.meta_etag, error,
                              "failed to copy pouch queue etag");
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
    rc = lc_pouch_copy_public(&out->meta_etag, info.meta_etag, error,
                              "failed to copy pouch queue etag");
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
  rc = lc_pouch_client_get_method(&lease->client->pub, lease->key, opts, dst,
                                  out, error);
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
  rc =
      lc_pouch_client_attach_method(&lease->client->pub, &req, src, out, error);
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
  rc = lc_pouch_client_delete_attachment_method(&lease->client->pub, &req,
                                                deleted, error);
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
  rc = lc_pouch_client_delete_all_attachments_method(&lease->client->pub, &req,
                                                     deleted_count, error);
  if (rc == LC_OK && *deleted_count > 0) {
    lease->version += 1L;
    lease->pub.version = lease->version;
  }
  return rc;
}

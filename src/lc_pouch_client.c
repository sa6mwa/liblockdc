#include "lc_api_internal.h"

#include <stdio.h>
#include <string.h>
#include <time.h>

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

static long lc_pouch_now_unix(void) { return (long)time(NULL); }

static char *lc_pouch_new_lease_id(lc_client_handle *client, const char *key,
                                   long fencing_token) {
  char stack[160];

  snprintf(stack, sizeof(stack), "pouch-%ld-%ld-%s", (long)time(NULL),
           fencing_token, key != NULL ? key : "lease");
  return lc_client_strdup(client, stack);
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

static int lc_pouch_lease_load_unsupported(lc_lease *self,
                                           const lonejson_map *map, void *dst,
                                           const lc_get_opts *opts,
                                           lc_get_res *out, lc_error *error) {
  (void)self;
  (void)map;
  (void)dst;
  (void)opts;
  (void)out;
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch lease load is not implemented yet", NULL, NULL,
                      NULL);
}

static int lc_pouch_lease_save_unsupported(lc_lease *self,
                                           const lonejson_map *map,
                                           const void *src, lc_error *error) {
  (void)self;
  (void)map;
  (void)src;
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch lease save is not implemented yet", NULL, NULL,
                      NULL);
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

  memset(record, 0, sizeof(*record));
  if (lease == NULL || lease->key == NULL || lease->lease_id == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch operation requires lease key and lease_id", NULL,
                        NULL, NULL);
  }
  rc = client->pouch_store->load_meta(
      client->pouch_store,
      lc_pouch_default_namespace(client, lease->namespace_name), lease->key,
      record, error);
  if (rc != LC_OK) {
    return rc;
  }
  now_unix = lc_pouch_now_unix();
  if (!record->found || record->meta.lease_id == NULL ||
      strcmp(record->meta.lease_id, lease->lease_id) != 0 ||
      record->meta.lease_expires_at_unix <= now_unix ||
      record->meta.fencing_token != lease->fencing_token) {
    lc_pouch_meta_record_cleanup(&client->pouch_allocator, record);
    return lc_error_set(error, LC_ERR_SERVER, 409L,
                        "pouch lease is not active for this operation", NULL,
                        "lease_not_active", NULL);
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
      lc_pouch_copy_client(lease->client, &txn_id, meta->txn_id, error,
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

static void lc_pouch_install_lease_methods(lc_lease *lease) {
  if (lease == NULL) {
    return;
  }
  lease->describe = lc_pouch_lease_describe_method;
  lease->get = lc_pouch_lease_get_method;
  lease->load = lc_pouch_lease_load_unsupported;
  lease->save = lc_pouch_lease_save_unsupported;
  lease->update = lc_pouch_lease_update_method;
  lease->mutate = lc_pouch_lease_mutate_unsupported;
  lease->mutate_local = lc_pouch_lease_mutate_local_unsupported;
  lease->metadata = lc_pouch_lease_metadata_method;
  lease->remove = lc_pouch_lease_remove_method;
  lease->keepalive = lc_pouch_lease_keepalive_method;
  lease->release = lc_pouch_lease_release_method;
}

int lc_pouch_client_acquire_method(lc_client *self, const lc_acquire_req *req,
                                   lc_lease **out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_meta_record existing;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_allocator *allocator;
  const char *namespace_name;
  char *lease_id;
  lc_lease *lease;
  long now_unix;
  int rc;

  if (self == NULL || req == NULL || out == NULL || req->key == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch acquire requires self, req, key, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  allocator = &client->pouch_allocator;
  namespace_name = lc_pouch_default_namespace(client, req->namespace_name);
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
  meta.owner = (char *)req->owner;
  meta.txn_id = (char *)req->txn_id;
  meta.has_query_hidden = existing.meta.has_query_hidden;
  meta.query_hidden = existing.meta.query_hidden;
  lease_id = lc_pouch_new_lease_id(client, req->key, meta.fencing_token);
  if (lease_id == NULL) {
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
    lc_pouch_meta_record_cleanup(allocator, &existing);
    return rc;
  }
  lease = lc_lease_new(client, namespace_name, req->key, req->owner, lease_id,
                       req->txn_id, meta.fencing_token, meta.version,
                       meta.state_etag, NULL);
  lc_client_free(client, lease_id);
  if (lease == NULL) {
    lc_pouch_store_meta_res_cleanup(allocator, &stored);
    lc_pouch_meta_record_cleanup(allocator, &existing);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch lease handle", NULL, NULL,
                        NULL);
  }
  ((lc_lease_handle *)lease)->lease_expires_at_unix =
      meta.lease_expires_at_unix;
  lease->lease_expires_at_unix = meta.lease_expires_at_unix;
  lc_pouch_install_lease_methods(lease);
  *out = lease;
  lc_pouch_store_meta_res_cleanup(allocator, &stored);
  lc_pouch_meta_record_cleanup(allocator, &existing);
  return LC_OK;
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
  namespace_name = lc_pouch_default_namespace(client, req->namespace_name);
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
  namespace_name = lc_pouch_default_namespace(client, NULL);
  body = NULL;
  memset(out, 0, sizeof(*out));
  memset(&info, 0, sizeof(info));
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
      out->version = record.meta.version;
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
  rc = lc_pouch_validate_active_lease(client, &req->lease, &record, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = client->pouch_store->remove_state(client->pouch_store,
                                         record.namespace_name, req->lease.key,
                                         req->if_state_etag, error);
  if (rc == LC_OK) {
    next_meta = record.meta;
    next_meta.version = record.meta.version + 1L;
    next_meta.state_etag = NULL;
    rc = client->pouch_store->store_meta(
        client->pouch_store, record.namespace_name, req->lease.key, &next_meta,
        record.etag, &stored, error);
  }
  if (rc == LC_OK) {
    out->removed = 1;
    out->new_version = next_meta.version;
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
  lc_pouch_allocator *allocator;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch release requires self, req, and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  allocator = &client->pouch_allocator;
  memset(out, 0, sizeof(*out));
  memset(&record, 0, sizeof(record));
  rc = lc_pouch_validate_active_lease(client, &req->lease, &record, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = client->pouch_store->delete_meta(client->pouch_store,
                                        record.namespace_name, req->lease.key,
                                        record.etag, error);
  if (rc == LC_OK) {
    out->released = 1;
  }
  lc_pouch_meta_record_cleanup(allocator, &record);
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

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease get requires self", NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  return lc_pouch_client_get_method(&lease->client->pub, lease->key, opts, dst,
                                    out, error);
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
  rc = lc_pouch_client_metadata_method(&lease->client->pub, &req, &res, error);
  if (rc == LC_OK) {
    lease->has_query_hidden = res.has_query_hidden;
    lease->query_hidden = res.query_hidden;
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

#include "lc_api_internal.h"
#include "lc_pouch.h"

#include "lc_internal.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct lc_pouch_acquire_for_update_file {
  FILE *fp;
} lc_pouch_acquire_for_update_file;

typedef struct lc_pouch_lonejson_source {
  lonejson_generator generator;
  int initialized;
} lc_pouch_lonejson_source;

static size_t lc_pouch_lonejson_source_read(void *context, void *buffer,
                                            size_t count, lc_error *error) {
  lc_pouch_lonejson_source *source;
  lonejson_status status;
  size_t out_len;
  int out_eof;

  source = (lc_pouch_lonejson_source *)context;
  if (source == NULL || !source->initialized) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch JSON generator source is closed", NULL, NULL, NULL);
    return 0U;
  }
  out_len = 0U;
  out_eof = 0;
  status = lonejson_generator_read(&source->generator,
                                   (unsigned char *)buffer, count, &out_len,
                                   &out_eof);
  if (status != LONEJSON_STATUS_OK) {
    (void)lc_lonejson_error_from_status(error, status, NULL,
                                        "failed to stream pouch JSON value");
    return 0U;
  }
  (void)out_eof;
  return out_len;
}

static void lc_pouch_lonejson_source_close(void *context) {
  lc_pouch_lonejson_source *source;

  source = (lc_pouch_lonejson_source *)context;
  if (source == NULL) {
    return;
  }
  if (source->initialized) {
    lonejson_generator_cleanup(&source->generator);
  }
  free(source);
}

static int lc_pouch_lonejson_source_open(const lonejson_map *map,
                                         const void *src, lc_source **out,
                                         lc_error *error) {
  lc_pouch_lonejson_source *context;
  lonejson *runtime;
  lonejson_status status;
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch JSON source requires output storage", NULL,
                        NULL, NULL);
  }
  *out = NULL;
  runtime = lc_thread_lonejson_runtime();
  if (runtime == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize pouch JSON runtime", NULL, NULL,
                        NULL);
  }
  context = (lc_pouch_lonejson_source *)calloc(1U, sizeof(*context));
  if (context == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch JSON source", NULL, NULL,
                        NULL);
  }
  status = lonejson_generator_init(runtime, &context->generator, map, src);
  if (status != LONEJSON_STATUS_OK) {
    free(context);
    return lc_lonejson_error_from_status(
        error, status, NULL, "failed to initialize pouch JSON generator");
  }
  context->initialized = 1;
  rc = lc_source_from_callbacks(lc_pouch_lonejson_source_read, NULL,
                                lc_pouch_lonejson_source_close, context, out,
                                error);
  if (rc != LC_OK) {
    lc_pouch_lonejson_source_close(context);
  }
  return rc;
}

static int lc_pouch_acquire_for_update_sink_write(lc_sink *self,
                                                  const void *bytes,
                                                  size_t count,
                                                  lc_error *error) {
  lc_pouch_acquire_for_update_file *file;

  file = (lc_pouch_acquire_for_update_file *)self->impl;
  if (file == NULL || file->fp == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch acquire_for_update sink is closed", NULL, NULL,
                        NULL);
  }
  if (count > 0U && fwrite(bytes, 1U, count, file->fp) != count) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to write pouch acquire_for_update snapshot",
                        strerror(errno), NULL, NULL);
  }
  return 1;
}

static void lc_pouch_acquire_for_update_sink_close(lc_sink *self) {
  (void)self;
}

static size_t lc_pouch_acquire_for_update_source_read(void *context,
                                                      void *buffer,
                                                      size_t count,
                                                      lc_error *error) {
  lc_pouch_acquire_for_update_file *file;
  size_t nread;

  file = (lc_pouch_acquire_for_update_file *)context;
  if (file == NULL || file->fp == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch acquire_for_update source is closed", NULL, NULL,
                 NULL);
    return 0U;
  }
  nread = fread(buffer, 1U, count, file->fp);
  if (nread == 0U && ferror(file->fp)) {
    lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                 "failed to read pouch acquire_for_update snapshot",
                 strerror(errno), NULL, NULL);
  }
  return nread;
}

static int lc_pouch_acquire_for_update_source_reset(void *context,
                                                    lc_error *error) {
  lc_pouch_acquire_for_update_file *file;

  file = (lc_pouch_acquire_for_update_file *)context;
  if (file == NULL || file->fp == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch acquire_for_update source is closed", NULL,
                        NULL, NULL);
  }
  clearerr(file->fp);
  if (fseek(file->fp, 0L, SEEK_SET) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to rewind pouch acquire_for_update snapshot",
                        strerror(errno), NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_client_rebuilding(lc_error *error) {
  return lc_error_set(
      error, LC_ERR_INVALID, 0L,
      "pouch operation is not implemented in the redesigned pouch backend",
      "storage, index, search, queue, object, and transaction subsystems are "
      "being rebuilt on the new pouch architecture",
      NULL, "pouch-redesign");
}

static const char *lc_pouch_client_namespace(lc_client_handle *client,
                                             const char *namespace_name) {
  if (namespace_name != NULL && namespace_name[0] != '\0') {
    return namespace_name;
  }
  if (client->default_namespace != NULL && client->default_namespace[0] != '\0') {
    return client->default_namespace;
  }
  return "default";
}

static int lc_pouch_client_public_read_unsupported(lc_error *error) {
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch public state reads are not implemented yet", NULL,
                      NULL, "pouch-redesign");
}

static int lc_pouch_client_validate_public_key(const char *key,
                                               lc_error *error) {
  if (key == NULL || key[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch operation requires a non-empty key", NULL,
                        NULL, NULL);
  }
  if (strncmp(key, ".staging/", sizeof(".staging/") - 1U) == 0 ||
      strstr(key, "/.staging/") != NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staging keys are reserved for internal state",
                        NULL, NULL, "pouch-redesign");
  }
  return LC_OK;
}

static int lc_pouch_lease_rebuilding(lc_error *error) {
  return lc_error_set(
      error, LC_ERR_INVALID, 0L,
      "pouch lease operation is not implemented in the redesigned pouch backend",
      "the bound lease state path is available; metadata, mutation, attachment, "
      "keepalive, and removal subsystems are still being rebuilt",
      NULL, "pouch-redesign");
}

static int lc_pouch_lease_refresh_state(lc_lease_handle *lease,
                                        const char *etag, long version,
                                        lc_error *error) {
  char *etag_copy;

  etag_copy = lc_client_strdup(lease->client, etag);
  if (etag != NULL && etag_copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch lease state etag", NULL,
                        NULL, NULL);
  }
  lc_client_free(lease->client, lease->state_etag);
  lease->state_etag = etag_copy;
  lease->version = version;
  lease->pub.state_etag = lease->state_etag;
  lease->pub.version = lease->version;
  return LC_OK;
}

static void lc_pouch_lease_refresh_query_metadata(lc_lease_handle *lease,
                                                  int has_query_hidden,
                                                  int query_hidden) {
  if (lease == NULL) {
    return;
  }
  lease->has_query_hidden = has_query_hidden;
  lease->query_hidden = query_hidden;
  lease->pub.has_query_hidden = lease->has_query_hidden;
  lease->pub.query_hidden = lease->query_hidden;
}

static int lc_pouch_now_unix(long *out, lc_error *error) {
  time_t now;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch time read requires output storage", NULL, NULL,
                        NULL);
  }
  now = time(NULL);
  if (now == (time_t)-1) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch wall clock", NULL, NULL, NULL);
  }
  *out = (long)now;
  return LC_OK;
}

static int lc_pouch_expiration_from_ttl(long ttl_seconds, long *out,
                                        lc_error *error) {
  long now;
  int rc;

  if (ttl_seconds <= 0L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch ttl_seconds must be positive", NULL, NULL,
                        NULL);
  }
  rc = lc_pouch_now_unix(&now, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (ttl_seconds > LONG_MAX - now) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch ttl_seconds exceeds supported range", NULL,
                        NULL, NULL);
  }
  *out = now + ttl_seconds;
  return LC_OK;
}

static void lc_pouch_lease_refresh_expiration(lc_lease_handle *lease,
                                              long lease_expires_at_unix) {
  if (lease == NULL) {
    return;
  }
  lease->lease_expires_at_unix = lease_expires_at_unix;
  lease->pub.lease_expires_at_unix = lease_expires_at_unix;
}

static int lc_pouch_client_copy_state_metadata(
    const lc_pouch_state_read_result *read_result, lc_get_res *out,
    lc_error *error) {
  char *content_type;
  char *etag;

  content_type = lc_strdup_local(read_result->content_type);
  etag = lc_strdup_local(read_result->etag);
  if ((read_result->content_type != NULL && content_type == NULL) ||
      (read_result->etag != NULL && etag == NULL)) {
    free(content_type);
    free(etag);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch get metadata", NULL, NULL,
                        NULL);
  }
  out->content_type = content_type;
  out->etag = etag;
  out->version = (long)read_result->version;
  out->no_content = !read_result->found;
  return LC_OK;
}

static int lc_pouch_client_copy_update_metadata(
    const lc_pouch_state_write_result *write_result, lc_update_res *out,
    lc_error *error) {
  char *etag;

  etag = lc_strdup_local(write_result->etag);
  if (write_result->etag != NULL && etag == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch update metadata", NULL, NULL,
                        NULL);
  }
  out->new_state_etag = etag;
  out->new_version = (long)write_result->version;
  out->bytes = (long)write_result->bytes;
  return LC_OK;
}

static int lc_pouch_client_get_namespace(lc_client_handle *client,
                                         const char *namespace_name,
                                         const char *key,
                                         const lc_get_opts *opts, lc_sink *dst,
                                         lc_get_res *out, lc_error *error) {
  lc_pouch_state_read_result read_result;
  int rc;

  if (opts != NULL && opts->public_read) {
    return lc_pouch_client_public_read_unsupported(error);
  }
  memset(out, 0, sizeof(*out));
  memset(&read_result, 0, sizeof(read_result));
  rc = lc_pouch_state_read(client->pouch,
                           lc_pouch_client_namespace(client, namespace_name),
                           key, &read_result, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!read_result.found) {
    out->no_content = 1;
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
    return LC_OK;
  }
  rc = lc_copy(read_result.body, dst, NULL, error);
  if (rc == LC_OK) {
    rc = lc_pouch_client_copy_state_metadata(&read_result, out, error);
  }
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  return rc;
}

static int lc_pouch_client_load_namespace(lc_client_handle *client,
                                          const char *namespace_name,
                                          const char *key,
                                          const lonejson_map *map, void *dst,
                                          const lc_get_opts *opts,
                                          lc_get_res *out, lc_error *error) {
  lc_pouch_state_read_result read_result;
  lc_sink *memory_sink;
  const void *bytes;
  size_t length;
  char *json;
  lonejson *runtime;
  lonejson_error lj_error;
  lonejson_status status;
  int rc;

  if (opts != NULL && opts->public_read) {
    return lc_pouch_client_public_read_unsupported(error);
  }
  memset(out, 0, sizeof(*out));
  memset(&read_result, 0, sizeof(read_result));
  memory_sink = NULL;
  json = NULL;
  rc = lc_pouch_state_read(client->pouch,
                           lc_pouch_client_namespace(client, namespace_name),
                           key, &read_result, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!read_result.found) {
    out->no_content = 1;
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
    return LC_OK;
  }
  rc = lc_sink_to_memory(&memory_sink, error);
  if (rc == LC_OK) {
    rc = lc_copy(read_result.body, memory_sink, NULL, error);
  }
  if (rc == LC_OK) {
    rc = lc_sink_memory_bytes(memory_sink, &bytes, &length, error);
  }
  if (rc == LC_OK) {
    json = (char *)malloc(length + 1U);
    if (json == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch mapped load buffer", NULL,
                        NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    memcpy(json, bytes, length);
    json[length] = '\0';
    runtime = lc_thread_lonejson_runtime();
    lc_lonejson_prepare_parse_destination(runtime, map, dst);
    memset(&lj_error, 0, sizeof(lj_error));
    status = lc_lonejson_parse_cstr_value(runtime, map, dst, json, &lj_error);
    if (status != LONEJSON_STATUS_OK) {
      rc = lc_lonejson_error_from_status(error, status, &lj_error,
                                         "failed to parse pouch mapped state");
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_client_copy_state_metadata(&read_result, out, error);
  }
  free(json);
  if (memory_sink != NULL) {
    memory_sink->close(memory_sink);
  }
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  return rc;
}

static int lc_pouch_lease_load_method(lc_lease *self, const lonejson_map *map,
                                      void *dst, const lc_get_opts *opts,
                                      lc_get_res *out, lc_error *error);
static int lc_pouch_lease_save_method(lc_lease *self, const lonejson_map *map,
                                      const void *src, lc_error *error);
static int lc_pouch_lease_staged_update_method(lc_lease *self, lc_source *src,
                                               const lc_update_opts *opts,
                                               lc_error *error);
static int lc_pouch_lease_mutate_method(lc_lease *self,
                                        const lc_mutate_req *req,
                                        lc_error *error);
static int lc_pouch_lease_mutate_local_method(lc_lease *self,
                                              const lc_mutate_local_req *req,
                                              lc_error *error);

static void lc_pouch_patch_lease_methods(lc_lease *lease) {
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
  lc_pouch_state_read_result read_result;
  const char *namespace_name;
  char lease_id[128];
  unsigned long version;
  long lease_expires_at_unix;
  int has_query_hidden;
  int query_hidden;
  lc_lease *lease;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch acquire requires self, req with key, and out",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_public_key(req->key, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_expiration_from_ttl(req->ttl_seconds, &lease_expires_at_unix,
                                    error);
  if (rc != LC_OK) {
    return rc;
  }
  namespace_name = lc_pouch_client_namespace(client, req->namespace_name);
  memset(&read_result, 0, sizeof(read_result));
  rc = lc_pouch_state_read(client->pouch, namespace_name, req->key,
                           &read_result, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (req->if_not_exists && read_result.found) {
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch acquire if_not_exists precondition failed",
                        NULL, NULL, NULL);
  }
  version = read_result.found ? read_result.version : 0UL;
  has_query_hidden = read_result.has_query_hidden;
  query_hidden = read_result.query_hidden;
  snprintf(lease_id, sizeof(lease_id), "pouch-lease-%020lu",
           version + 1UL);
  lease = lc_lease_new(client, namespace_name, req->key, req->owner, lease_id,
                       req->txn_id, 1L, (long)version,
                       read_result.found ? read_result.etag : NULL, NULL);
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  if (lease == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch lease", NULL, NULL, NULL);
  }
  lc_pouch_lease_refresh_expiration((lc_lease_handle *)lease,
                                    lease_expires_at_unix);
  lc_pouch_lease_refresh_query_metadata((lc_lease_handle *)lease,
                                        has_query_hidden, query_hidden);
  lc_pouch_patch_lease_methods(lease);
  *out = lease;
  return LC_OK;
}

int lc_pouch_client_acquire_for_update_method(
    lc_client *self, const lc_acquire_req *req,
    lc_acquire_for_update_handler_fn handler, void *context,
    lc_error *error) {
  lc_client_handle *client;
  lc_lease *lease;
  lc_lease_handle *lease_handle;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lc_release_req release_req;
  lc_error handler_error;
  lc_error release_error;
  lc_pouch_acquire_for_update_file file;
  lc_sink sink;
  lc_acquire_for_update_context update;
  lc_pouch_state_write_result promote_result;
  FILE *fp;
  int (*original_update)(lc_lease *, lc_source *, const lc_update_opts *,
                         lc_error *);
  const char *stage_txn_id;
  int discarded;
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
  fp = NULL;
  original_update = NULL;
  stage_txn_id = NULL;
  discarded = 0;
  memset(&get_res, 0, sizeof(get_res));
  lc_get_opts_init(&get_opts);
  lc_release_req_init(&release_req);
  lc_error_init(&handler_error);
  lc_error_init(&release_error);
  memset(&file, 0, sizeof(file));
  memset(&sink, 0, sizeof(sink));
  memset(&update, 0, sizeof(update));
  memset(&promote_result, 0, sizeof(promote_result));

  rc = lc_pouch_client_acquire_method(self, req, &lease, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  lease_handle = (lc_lease_handle *)lease;
  stage_txn_id = lease_handle->txn_id != NULL && lease_handle->txn_id[0] != '\0'
                     ? lease_handle->txn_id
                     : lease_handle->lease_id;
  original_update = lease->update;

  fp = tmpfile();
  if (fp == NULL) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to create pouch acquire_for_update snapshot",
                      strerror(errno), NULL, NULL);
    goto release_and_cleanup;
  }
  file.fp = fp;
  sink.write = lc_pouch_acquire_for_update_sink_write;
  sink.close = lc_pouch_acquire_for_update_sink_close;
  sink.impl = &file;
  rc = lease->get(lease, &sink, &get_opts, &get_res, error);
  if (rc != LC_OK) {
    goto release_and_cleanup;
  }
  if (fflush(fp) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to flush pouch acquire_for_update snapshot",
                      strerror(errno), NULL, NULL);
    goto release_and_cleanup;
  }
  if (fseek(fp, 0L, SEEK_SET) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to rewind pouch acquire_for_update snapshot",
                      strerror(errno), NULL, NULL);
    goto release_and_cleanup;
  }

  update.lease = lease;
  update.state.has_state = !get_res.no_content;
  update.state.content_type = get_res.content_type;
  update.state.etag = get_res.etag;
  update.state.version = get_res.version;
  update.state.fencing_token = get_res.fencing_token;
  update.state.correlation_id = get_res.correlation_id;
  if (!get_res.no_content) {
    rc = lc_source_from_callbacks(
        lc_pouch_acquire_for_update_source_read,
        lc_pouch_acquire_for_update_source_reset, NULL, &file,
        &update.state.reader, error);
    if (rc != LC_OK) {
      goto release_and_cleanup;
    }
  }

  lease_handle->pouch_stage_active = 1;
  lease_handle->pouch_stage_dirty = 0;
  lc_client_free(client, lease_handle->pouch_stage_etag);
  lease_handle->pouch_stage_etag = NULL;
  lease_handle->pouch_stage_version = 0L;
  lease->update = lc_pouch_lease_staged_update_method;
  rc = handler(context, &update, &handler_error);
  lease->update = original_update;
  original_update = NULL;
  lease_handle->pouch_stage_active = 0;
  if (update.state.reader != NULL) {
    lc_source_close(update.state.reader);
    update.state.reader = NULL;
  }
  if (rc != LC_OK) {
    if (error != NULL) {
      *error = handler_error;
      lc_error_init(&handler_error);
    }
    release_req.rollback = 1;
    if (lease_handle->pouch_stage_dirty) {
      lc_error discard_error;

      lc_error_init(&discard_error);
      rc = lc_pouch_state_discard_staged(client->pouch,
                                         lease_handle->namespace_name,
                                         lease_handle->key, stage_txn_id,
                                         &discarded, &discard_error);
      if (rc != LC_OK && error != NULL && error->code == LC_OK) {
        *error = discard_error;
        lc_error_init(&discard_error);
      }
      lc_error_cleanup(&discard_error);
    }
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_INVALID;
  } else if (lease_handle->pouch_stage_dirty) {
    rc = lc_pouch_state_promote_staged(
        client->pouch, lease_handle->namespace_name, lease_handle->key,
        stage_txn_id, get_res.no_content ? NULL : get_res.etag,
        &promote_result, error);
    if (rc == LC_OK) {
      rc = lc_pouch_lease_refresh_state(lease_handle, promote_result.etag,
                                        (long)promote_result.version, error);
    }
    if (rc != LC_OK) {
      lc_error discard_error;

      release_req.rollback = 1;
      lc_error_init(&discard_error);
      (void)lc_pouch_state_discard_staged(client->pouch,
                                          lease_handle->namespace_name,
                                          lease_handle->key, stage_txn_id,
                                          &discarded, &discard_error);
      lc_error_cleanup(&discard_error);
    }
  }

release_and_cleanup:
  if (lease != NULL && original_update != NULL) {
    lease->update = original_update;
  }
  if (lease != NULL) {
    ((lc_lease_handle *)lease)->pouch_stage_active = 0;
  }
  release_rc = lease != NULL ? lc_pouch_lease_release_method(
                                   lease, &release_req, &release_error)
                             : LC_OK;
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
    lease = NULL;
  }

cleanup:
  if (update.state.reader != NULL) {
    lc_source_close(update.state.reader);
  }
  if (fp != NULL) {
    fclose(fp);
  }
  lc_pouch_state_write_result_cleanup(&client->allocator, &promote_result);
  lc_get_res_cleanup(&get_res);
  lc_error_cleanup(&handler_error);
  lc_error_cleanup(&release_error);
  return rc;
}

int lc_pouch_client_describe_method(lc_client *self, const lc_describe_req *req,
                                    lc_describe_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_state_read_result read_result;
  const char *namespace_name;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch describe requires self, req with key, and out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  memset(out, 0, sizeof(*out));
  rc = lc_pouch_client_validate_public_key(req->key, error);
  if (rc != LC_OK) {
    return rc;
  }
  namespace_name = lc_pouch_client_namespace(client, req->namespace_name);
  memset(&read_result, 0, sizeof(read_result));
  rc = lc_pouch_state_read(client->pouch, namespace_name, req->key,
                           &read_result, error);
  if (rc != LC_OK) {
    return rc;
  }
  out->namespace_name = lc_strdup_local(namespace_name);
  out->key = lc_strdup_local(req->key);
  out->state_etag =
      read_result.found ? lc_strdup_local(read_result.etag) : NULL;
  if (out->namespace_name == NULL || out->key == NULL ||
      (read_result.found && out->state_etag == NULL)) {
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
    lc_describe_res_cleanup(out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch describe response", NULL,
                        NULL, NULL);
  }
  out->version = read_result.found ? (long)read_result.version : 0L;
  out->fencing_token = 1L;
  out->has_query_hidden = read_result.has_query_hidden;
  out->query_hidden = read_result.query_hidden;
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  return LC_OK;
}

int lc_pouch_client_get_method(lc_client *self, const char *key,
                               const lc_get_opts *opts, lc_sink *dst,
                               lc_get_res *out, lc_error *error) {
  lc_client_handle *client;

  if (self == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch get requires self, key, dst, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  {
    int rc;

    rc = lc_pouch_client_validate_public_key(key, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  return lc_pouch_client_get_namespace(client, NULL, key, opts, dst, out,
                                       error);
}

int lc_pouch_client_load_method(lc_client *self, const char *key,
                                const lonejson_map *map, void *dst,
                                const lc_get_opts *opts, lc_get_res *out,
                                lc_error *error) {
  lc_client_handle *client;

  if (self == NULL || map == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch load requires self, key, map, destination, "
                        "and out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  {
    int rc;

    rc = lc_pouch_client_validate_public_key(key, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  return lc_pouch_client_load_namespace(client, NULL, key, map, dst, opts, out,
                                        error);
}

int lc_pouch_client_update_method(lc_client *self, const lc_update_req *req,
                                  lc_source *src, lc_update_res *out,
                                  lc_error *error) {
  lc_client_handle *client;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result write_result;
  int rc;

  if (self == NULL || req == NULL || src == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch update requires self, req with key, src, and "
                        "out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_public_key(req->lease.key, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(out, 0, sizeof(*out));
  memset(&options, 0, sizeof(options));
  memset(&write_result, 0, sizeof(write_result));
  options.content_type =
      req->content_type != NULL ? req->content_type : "application/json";
  options.expected_etag = req->if_state_etag;
  if (req->has_if_version) {
    if (req->if_version < 0L) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch update if_version must be non-negative", NULL,
                          NULL, NULL);
    }
    options.expected_version = (unsigned long)req->if_version;
    options.has_expected_version = 1;
  }
  rc = lc_pouch_state_write(
      client->pouch,
      lc_pouch_client_namespace(client, req->lease.namespace_name),
      req->lease.key, src, &options, &write_result, error);
  if (rc == LC_OK) {
    rc = lc_pouch_client_copy_update_metadata(&write_result, out, error);
  }
  lc_pouch_state_write_result_cleanup(&client->allocator, &write_result);
  return rc;
}

int lc_pouch_client_mutate_method(lc_client *self, const lc_mutate_op *req,
                                  lc_mutate_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_metadata_method(lc_client *self,
                                    const lc_metadata_op *req,
                                    lc_metadata_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  const char *namespace_name;
  char *namespace_copy;
  char *key_copy;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata requires self, req, and out", NULL,
                        NULL, NULL);
  }
  if (!req->has_query_hidden) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata requires query_hidden", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_public_key(req->lease.key, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(out, 0, sizeof(*out));
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  options.has_query_hidden = 1;
  options.query_hidden = req->query_hidden;
  if (req->has_if_version) {
    if (req->if_version < 0L) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch metadata if_version must be non-negative",
                          NULL, NULL, NULL);
    }
    options.expected_version = (unsigned long)req->if_version;
    options.has_expected_version = 1;
  }
  namespace_name = lc_pouch_client_namespace(client, req->lease.namespace_name);
  rc = lc_pouch_state_update_metadata(client->pouch, namespace_name,
                                      req->lease.key, &options, &result,
                                      error);
  if (rc != LC_OK) {
    lc_pouch_state_write_result_cleanup(&client->allocator, &result);
    return rc;
  }
  namespace_copy = lc_strdup_local(namespace_name);
  key_copy = lc_strdup_local(req->lease.key);
  if (namespace_copy == NULL || key_copy == NULL) {
    free(namespace_copy);
    free(key_copy);
    lc_pouch_state_write_result_cleanup(&client->allocator, &result);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch metadata response", NULL,
                        NULL, NULL);
  }
  out->namespace_name = namespace_copy;
  out->key = key_copy;
  out->version = (long)result.version;
  out->has_query_hidden = result.has_query_hidden;
  out->query_hidden = result.query_hidden;
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  return LC_OK;
}

int lc_pouch_client_remove_method(lc_client *self, const lc_remove_op *req,
                                  lc_remove_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch remove requires self, req with key, and out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_public_key(req->lease.key, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(out, 0, sizeof(*out));
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  options.expected_etag = req->if_state_etag;
  if (req->has_if_version) {
    if (req->if_version < 0L) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch remove if_version must be non-negative",
                          NULL, NULL, NULL);
    }
    options.expected_version = (unsigned long)req->if_version;
    options.has_expected_version = 1;
  }
  rc = lc_pouch_state_delete(
      client->pouch,
      lc_pouch_client_namespace(client, req->lease.namespace_name),
      req->lease.key, &options, &result, error);
  if (rc == LC_OK) {
    out->removed = result.version > 0UL;
    out->new_version = (long)result.version;
  }
  lc_pouch_state_write_result_cleanup(&client->allocator, &result);
  return rc;
}

int lc_pouch_client_keepalive_method(lc_client *self,
                                     const lc_keepalive_op *req,
                                     lc_keepalive_res *out,
                                     lc_error *error) {
  lc_client_handle *client;
  lc_pouch_state_read_result read_result;
  const char *namespace_name;
  long lease_expires_at_unix;
  char *state_etag;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch keepalive requires self, req, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_public_key(req->lease.key, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_expiration_from_ttl(req->ttl_seconds, &lease_expires_at_unix,
                                    error);
  if (rc != LC_OK) {
    return rc;
  }
  namespace_name = lc_pouch_client_namespace(client, req->lease.namespace_name);
  memset(&read_result, 0, sizeof(read_result));
  rc = lc_pouch_state_read(client->pouch, namespace_name, req->lease.key,
                           &read_result, error);
  if (rc != LC_OK) {
    return rc;
  }
  state_etag = read_result.etag != NULL ? lc_strdup_local(read_result.etag)
                                        : NULL;
  if (read_result.etag != NULL && state_etag == NULL) {
    lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch keepalive state etag", NULL,
                        NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  out->lease_expires_at_unix = lease_expires_at_unix;
  out->version = read_result.found ? (long)read_result.version : 0L;
  out->state_etag = state_etag;
  lc_pouch_state_read_result_cleanup(&client->allocator, &read_result);
  return LC_OK;
}

int lc_pouch_client_release_method(lc_client *self, const lc_release_op *req,
                                   lc_release_res *out, lc_error *error) {
  lc_client_handle *client;
  int discarded;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch release requires self, req, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  rc = lc_pouch_client_validate_public_key(req->lease.key, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (req->rollback && req->lease.txn_id != NULL &&
      req->lease.txn_id[0] != '\0') {
    rc = lc_pouch_state_discard_staged(
        client->pouch,
        lc_pouch_client_namespace(client, req->lease.namespace_name),
        req->lease.key, req->lease.txn_id, &discarded, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  memset(out, 0, sizeof(*out));
  out->released = 1;
  return LC_OK;
}

int lc_pouch_client_attach_method(lc_client *self, const lc_attach_op *req,
                                  lc_source *src, lc_attach_res *out,
                                  lc_error *error) {
  (void)self;
  (void)req;
  (void)src;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_list_attachments_method(
    lc_client *self, const lc_attachment_list_req *req,
    lc_attachment_list *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_get_attachment_method(
    lc_client *self, const lc_attachment_get_op *req, lc_sink *dst,
    lc_attachment_get_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)dst;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_delete_attachment_method(
    lc_client *self, const lc_attachment_delete_op *req, int *deleted,
    lc_error *error) {
  (void)self;
  (void)req;
  if (deleted != NULL) {
    *deleted = 0;
  }
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_delete_all_attachments_method(
    lc_client *self, const lc_attachment_delete_all_op *req,
    int *deleted_count, lc_error *error) {
  (void)self;
  (void)req;
  if (deleted_count != NULL) {
    *deleted_count = 0;
  }
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_queue_stats_method(lc_client *self,
                                       const lc_queue_stats_req *req,
                                       lc_queue_stats_res *out,
                                       lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_queue_ack_method(lc_client *self, const lc_ack_op *req,
                                     lc_ack_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_queue_nack_method(lc_client *self, const lc_nack_op *req,
                                      lc_nack_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_queue_extend_method(lc_client *self,
                                        const lc_extend_op *req,
                                        lc_extend_res *out,
                                        lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_enqueue_method(lc_client *self, const lc_enqueue_req *req,
                                   lc_source *src, lc_enqueue_res *out,
                                   lc_error *error) {
  (void)self;
  (void)req;
  (void)src;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_dequeue_method(lc_client *self, const lc_dequeue_req *req,
                                   lc_message **out, lc_error *error) {
  (void)self;
  (void)req;
  if (out != NULL) {
    *out = NULL;
  }
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_dequeue_with_state_method(lc_client *self,
                                              const lc_dequeue_req *req,
                                              lc_message **out,
                                              lc_error *error) {
  (void)self;
  (void)req;
  if (out != NULL) {
    *out = NULL;
  }
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_dequeue_batch_method(lc_client *self,
                                         const lc_dequeue_req *req,
                                         lc_dequeue_batch_res *out,
                                         lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_subscribe_method(lc_client *self,
                                     const lc_dequeue_req *req,
                                     const lc_consumer *consumer,
                                     lc_error *error) {
  (void)self;
  (void)req;
  (void)consumer;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_subscribe_with_state_method(lc_client *self,
                                                const lc_dequeue_req *req,
                                                const lc_consumer *consumer,
                                                lc_error *error) {
  (void)self;
  (void)req;
  (void)consumer;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_watch_queue_method(lc_client *self,
                                       const lc_watch_queue_req *req,
                                       const lc_watch_handler *handler,
                                       lc_error *error) {
  (void)self;
  (void)req;
  (void)handler;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_query_method(lc_client *self, const lc_query_req *req,
                                 lc_sink *dst, lc_query_res *out,
                                 lc_error *error) {
  (void)self;
  (void)req;
  (void)dst;
  (void)out;
  return lc_pouch_client_rebuilding(error);
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
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_get_namespace_config_method(
    lc_client *self, const lc_namespace_config_req *req,
    lc_namespace_config_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_update_namespace_config_method(
    lc_client *self, const lc_namespace_config_req *req,
    lc_namespace_config_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_flush_index_method(lc_client *self,
                                       const lc_index_flush_req *req,
                                       lc_index_flush_res *out,
                                       lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_txn_replay_method(lc_client *self,
                                      const lc_txn_replay_req *req,
                                      lc_txn_replay_res *out,
                                      lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_txn_prepare_method(lc_client *self,
                                       const lc_txn_decision_req *req,
                                       lc_txn_decision_res *out,
                                       lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_txn_commit_method(lc_client *self,
                                      const lc_txn_decision_req *req,
                                      lc_txn_decision_res *out,
                                      lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_txn_rollback_method(lc_client *self,
                                        const lc_txn_decision_req *req,
                                        lc_txn_decision_res *out,
                                        lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_recover_transactions(lc_client *self, lc_error *error) {
  (void)self;
  (void)error;
  return LC_OK;
}

int lc_pouch_client_tc_lease_acquire_method(
    lc_client *self, const lc_tc_lease_acquire_req *req,
    lc_tc_lease_acquire_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_tc_lease_renew_method(lc_client *self,
                                          const lc_tc_lease_renew_req *req,
                                          lc_tc_lease_renew_res *out,
                                          lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_tc_lease_release_method(
    lc_client *self, const lc_tc_lease_release_req *req,
    lc_tc_lease_release_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_tc_leader_method(lc_client *self, lc_tc_leader_res *out,
                                     lc_error *error) {
  (void)self;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_tc_cluster_announce_method(
    lc_client *self, const lc_tc_cluster_announce_req *req,
    lc_tc_cluster_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_tc_cluster_leave_method(lc_client *self,
                                            lc_tc_cluster_res *out,
                                            lc_error *error) {
  (void)self;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_tc_cluster_list_method(lc_client *self,
                                           lc_tc_cluster_res *out,
                                           lc_error *error) {
  (void)self;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_tc_rm_register_method(
    lc_client *self, const lc_tc_rm_register_req *req,
    lc_tc_rm_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_tc_rm_unregister_method(
    lc_client *self, const lc_tc_rm_unregister_req *req,
    lc_tc_rm_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_tc_rm_list_method(lc_client *self, lc_tc_rm_list_res *out,
                                      lc_error *error) {
  (void)self;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_message_ack_method(lc_message *self, lc_error *error) {
  (void)self;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_message_nack_method(lc_message *self, const lc_nack_req *req,
                                 lc_error *error) {
  (void)self;
  (void)req;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_message_extend_method(lc_message *self, const lc_extend_req *req,
                                   lc_error *error) {
  (void)self;
  (void)req;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_lease_describe_method(lc_lease *self, lc_error *error) {
  lc_lease_handle *lease;
  lc_pouch_state_read_result read_result;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease describe requires self", NULL, NULL,
                        NULL);
  }
  lease = (lc_lease_handle *)self;
  memset(&read_result, 0, sizeof(read_result));
  rc = lc_pouch_state_read(lease->client->pouch, lease->namespace_name,
                           lease->key, &read_result, error);
  if (rc == LC_OK) {
    rc = lc_pouch_lease_refresh_state(
        lease, read_result.found ? read_result.etag : NULL,
        read_result.found ? (long)read_result.version : 0L, error);
  }
  lc_pouch_state_read_result_cleanup(&lease->client->allocator, &read_result);
  return rc;
}

int lc_pouch_lease_get_method(lc_lease *self, lc_sink *dst,
                              const lc_get_opts *opts, lc_get_res *out,
                              lc_error *error) {
  lc_lease_handle *lease;
  int rc;

  if (self == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease get requires self, dst, and out", NULL,
                        NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  rc = lc_pouch_client_get_namespace(lease->client, lease->namespace_name,
                                     lease->key, opts, dst, out, error);
  if (rc == LC_OK && !out->no_content) {
    rc = lc_pouch_lease_refresh_state(lease, out->etag, out->version, error);
  }
  return rc;
}

static int lc_pouch_lease_load_method(lc_lease *self, const lonejson_map *map,
                                      void *dst, const lc_get_opts *opts,
                                      lc_get_res *out, lc_error *error) {
  lc_lease_handle *lease;
  int rc;

  if (self == NULL || map == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease load requires self, map, destination, "
                        "and out",
                        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  rc = lc_pouch_client_load_namespace(lease->client, lease->namespace_name,
                                      lease->key, map, dst, opts, out, error);
  if (rc == LC_OK && !out->no_content) {
    rc = lc_pouch_lease_refresh_state(lease, out->etag, out->version, error);
  }
  return rc;
}

static int lc_pouch_lease_save_method(lc_lease *self, const lonejson_map *map,
                                      const void *src, lc_error *error) {
  lc_source *source;
  lc_update_opts opts;
  int rc;

  if (self == NULL || map == NULL || src == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease save requires self, map, and source",
                        NULL, NULL, NULL);
  }
  source = NULL;
  rc = lc_pouch_lonejson_source_open(map, src, &source, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&opts, 0, sizeof(opts));
  opts.content_type = "application/json";
  rc = lc_pouch_lease_update_method(self, source, &opts, error);
  lc_source_close(source);
  return rc;
}

static int lc_pouch_lease_staged_update_method(lc_lease *self, lc_source *src,
                                               const lc_update_opts *opts,
                                               lc_error *error) {
  lc_lease_handle *lease;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  const char *stage_txn_id;
  char *etag_copy;
  int rc;

  if (self == NULL || src == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged lease update requires self and src",
                        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  if (!lease->pouch_stage_active) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged lease update used outside staging scope",
                        NULL, NULL, NULL);
  }
  stage_txn_id = lease->txn_id != NULL && lease->txn_id[0] != '\0'
                     ? lease->txn_id
                     : lease->lease_id;
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  options.content_type = "application/json";
  options.has_query_hidden = lease->has_query_hidden;
  options.query_hidden = lease->query_hidden;
  if (opts != NULL) {
    options.content_type =
        opts->content_type != NULL ? opts->content_type : options.content_type;
    options.expected_etag = opts->if_state_etag;
    if (opts->has_if_version) {
      if (opts->if_version < 0L) {
        return lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch staged update if_version must be "
                            "non-negative",
                            NULL, NULL, NULL);
      }
      options.expected_version = (unsigned long)opts->if_version;
      options.has_expected_version = 1;
    }
  } else if (lease->pouch_stage_dirty) {
    options.expected_etag = lease->pouch_stage_etag;
    options.expected_version = (unsigned long)lease->pouch_stage_version;
    options.has_expected_version = 1;
  }
  rc = lc_pouch_state_stage_write(lease->client->pouch, lease->namespace_name,
                                  lease->key, stage_txn_id, src, &options,
                                  &result, error);
  if (rc == LC_OK) {
    etag_copy = lc_client_strdup(lease->client, result.etag);
    if (etag_copy == NULL) {
      lc_pouch_state_write_result_cleanup(&lease->client->allocator, &result);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch staged state etag", NULL,
                          NULL, NULL);
    }
    lc_client_free(lease->client, lease->pouch_stage_etag);
    lease->pouch_stage_etag = etag_copy;
    lease->pouch_stage_version = (long)result.version;
    lease->pouch_stage_dirty = 1;
    rc = lc_pouch_lease_refresh_state(lease, result.etag,
                                      (long)result.version, error);
  }
  lc_pouch_state_write_result_cleanup(&lease->client->allocator, &result);
  return rc;
}

int lc_pouch_lease_update_method(lc_lease *self, lc_source *src,
                                 const lc_update_opts *opts,
                                 lc_error *error) {
  lc_lease_handle *lease;
  lc_update_req req;
  lc_update_res res;
  int rc;

  if (self == NULL || src == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease update requires self and src", NULL,
                        NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  lc_update_req_init(&req);
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
    req.content_type = opts->content_type;
  }
  if (!req.has_if_version && lease->version > 0L) {
    req.if_version = lease->version;
    req.has_if_version = 1;
  }
  if (req.content_type == NULL) {
    req.content_type = "application/json";
  }
  rc = lc_pouch_client_update_method(&lease->client->pub, &req, src, &res,
                                     error);
  if (rc == LC_OK) {
    rc = lc_pouch_lease_refresh_state(lease, res.new_state_etag,
                                      res.new_version, error);
  }
  lc_update_res_cleanup(&res);
  return rc;
}

static int lc_pouch_lease_mutate_method(lc_lease *self,
                                        const lc_mutate_req *req,
                                        lc_error *error) {
  (void)self;
  (void)req;
  return lc_pouch_lease_rebuilding(error);
}

static int lc_pouch_lease_mutate_local_method(lc_lease *self,
                                              const lc_mutate_local_req *req,
                                              lc_error *error) {
  (void)self;
  (void)req;
  return lc_pouch_lease_rebuilding(error);
}

int lc_pouch_lease_metadata_method(lc_lease *self, const lc_metadata_req *req,
                                   lc_error *error) {
  lc_lease_handle *lease;
  lc_metadata_op op;
  lc_metadata_res res;
  int rc;

  if (self == NULL || req == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease metadata requires self and req", NULL,
                        NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  lc_metadata_op_init(&op);
  memset(&res, 0, sizeof(res));
  op.lease.namespace_name = lease->namespace_name;
  op.lease.key = lease->key;
  op.lease.lease_id = lease->lease_id;
  op.lease.txn_id = lease->txn_id;
  op.lease.fencing_token = lease->fencing_token;
  op.has_query_hidden = req->has_query_hidden;
  op.query_hidden = req->query_hidden;
  op.if_version = req->if_version;
  op.has_if_version = req->has_if_version;
  if (!op.has_if_version && lease->version > 0L) {
    op.if_version = lease->version;
    op.has_if_version = 1;
  }
  rc = lc_pouch_client_metadata_method(&lease->client->pub, &op, &res,
                                       error);
  if (rc == LC_OK) {
    lease->version = res.version;
    lease->pub.version = lease->version;
    lc_pouch_lease_refresh_query_metadata(lease, res.has_query_hidden,
                                          res.query_hidden);
  }
  lc_metadata_res_cleanup(&res);
  return rc;
}

int lc_pouch_lease_remove_method(lc_lease *self, const lc_remove_req *req,
                                 lc_error *error) {
  lc_lease_handle *lease;
  lc_remove_op op;
  lc_remove_res res;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease remove requires self", NULL, NULL,
                        NULL);
  }
  lease = (lc_lease_handle *)self;
  lc_remove_op_init(&op);
  memset(&res, 0, sizeof(res));
  op.lease.namespace_name = lease->namespace_name;
  op.lease.key = lease->key;
  op.lease.lease_id = lease->lease_id;
  op.lease.txn_id = lease->txn_id;
  op.lease.fencing_token = lease->fencing_token;
  if (req != NULL) {
    op.if_state_etag = req->if_state_etag;
    op.if_version = req->if_version;
    op.has_if_version = req->has_if_version;
  }
  if (!op.has_if_version && lease->version > 0L) {
    op.if_version = lease->version;
    op.has_if_version = 1;
  }
  rc = lc_pouch_client_remove_method(&lease->client->pub, &op, &res, error);
  if (rc == LC_OK && res.removed) {
    rc = lc_pouch_lease_refresh_state(lease, NULL, 0L, error);
  }
  lc_remove_res_cleanup(&res);
  return rc;
}

int lc_pouch_lease_keepalive_method(lc_lease *self,
                                    const lc_keepalive_req *req,
                                    lc_error *error) {
  lc_lease_handle *lease;
  lc_keepalive_op op;
  lc_keepalive_res res;
  int rc;

  if (self == NULL || req == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease keepalive requires self and req", NULL,
                        NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  lc_keepalive_op_init(&op);
  memset(&res, 0, sizeof(res));
  op.lease.namespace_name = lease->namespace_name;
  op.lease.key = lease->key;
  op.lease.lease_id = lease->lease_id;
  op.lease.txn_id = lease->txn_id;
  op.lease.fencing_token = lease->fencing_token;
  op.ttl_seconds = req->ttl_seconds;
  rc = lc_pouch_client_keepalive_method(&lease->client->pub, &op, &res,
                                        error);
  if (rc == LC_OK) {
    rc = lc_pouch_lease_refresh_state(lease, res.state_etag, res.version,
                                      error);
  }
  if (rc == LC_OK) {
    lc_pouch_lease_refresh_expiration(lease, res.lease_expires_at_unix);
  }
  lc_keepalive_res_cleanup(&res);
  return rc;
}

int lc_pouch_lease_release_method(lc_lease *self, const lc_release_req *req,
                                  lc_error *error) {
  lc_lease_handle *lease;
  lc_release_op op;
  lc_release_res res;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch lease release requires self", NULL, NULL,
                        NULL);
  }
  lease = (lc_lease_handle *)self;
  lc_release_op_init(&op);
  memset(&res, 0, sizeof(res));
  op.lease.namespace_name = lease->namespace_name;
  op.lease.key = lease->key;
  op.lease.lease_id = lease->lease_id;
  op.lease.txn_id = lease->txn_id;
  op.lease.fencing_token = lease->fencing_token;
  op.rollback = req != NULL ? req->rollback : 0;
  rc = lc_pouch_client_release_method(&lease->client->pub, &op, &res, error);
  lc_release_res_cleanup(&res);
  if (rc != LC_OK) {
    return rc;
  }
  self->close(self);
  return LC_OK;
}

int lc_pouch_lease_attach_method(lc_lease *self, const lc_attach_req *req,
                                 lc_source *src, lc_attach_res *out,
                                 lc_error *error) {
  (void)self;
  (void)req;
  (void)src;
  (void)out;
  return lc_pouch_lease_rebuilding(error);
}

int lc_pouch_lease_list_attachments_method(lc_lease *self,
                                           lc_attachment_list *out,
                                           lc_error *error) {
  (void)self;
  (void)out;
  return lc_pouch_lease_rebuilding(error);
}

int lc_pouch_lease_get_attachment_method(lc_lease *self,
                                         const lc_attachment_get_req *req,
                                         lc_sink *dst,
                                         lc_attachment_get_res *out,
                                         lc_error *error) {
  (void)self;
  (void)req;
  (void)dst;
  (void)out;
  return lc_pouch_lease_rebuilding(error);
}

int lc_pouch_lease_delete_attachment_method(
    lc_lease *self, const lc_attachment_selector *selector, int *deleted,
    lc_error *error) {
  (void)self;
  (void)selector;
  if (deleted != NULL) {
    *deleted = 0;
  }
  return lc_pouch_lease_rebuilding(error);
}

int lc_pouch_lease_delete_all_attachments_method(lc_lease *self,
                                                 int *deleted_count,
                                                 lc_error *error) {
  (void)self;
  if (deleted_count != NULL) {
    *deleted_count = 0;
  }
  return lc_pouch_lease_rebuilding(error);
}

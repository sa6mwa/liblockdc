#include "lc_api_internal.h"
#include "lc_internal.h"
#include "lc_log.h"

static void lc_lease_log_error(lc_lease_handle *lease, pslog_level level,
                               const char *message, const pslog_field *fields,
                               size_t count, const lc_error *error) {
  pslog_field combined[16];
  size_t index;

  if (count + 3U > sizeof(combined) / sizeof(combined[0])) {
    count = sizeof(combined) / sizeof(combined[0]) - 3U;
  }
  for (index = 0U; index < count; ++index) {
    combined[index] = fields[index];
  }
  combined[count + 0U] = lc_log_code_field(error);
  combined[count + 1U] = lc_log_http_status_field(error);
  combined[count + 2U] = lc_log_error_field("error", error);
  switch (level) {
  case PSLOG_LEVEL_DEBUG:
    lc_log_debug(lease->client->logger, message, combined, count + 3U);
    break;
  case PSLOG_LEVEL_WARN:
    lc_log_warn(lease->client->logger, message, combined, count + 3U);
    break;
  default:
    lc_log_error(lease->client->logger, message, combined, count + 3U);
    break;
  }
}

typedef struct lc_lease_lonejson_load_bridge {
  lc_stream_pipe *pipe;
  lonejson_status status;
  lonejson_error lj_error;
} lc_lease_lonejson_load_bridge;

static int lc_lease_lonejson_load_write_callback(void *context,
                                                 const void *bytes,
                                                 size_t count,
                                                 lc_engine_error *error) {
  lc_lease_lonejson_load_bridge *bridge;

  bridge = (lc_lease_lonejson_load_bridge *)context;
  if (bridge == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "mapped load bridge is required");
  }
  if (lc_stream_pipe_write(bridge->pipe, bytes, count, NULL) != LC_OK) {
    return lc_engine_set_protocol_error(
        error, "failed to stream mapped lease state response");
  }
  return 1;
}

typedef struct lc_lease_lonejson_load_thread {
  lc_lease_lonejson_load_bridge *bridge;
  const lonejson_map *map;
  void *dst;
  const lonejson_parse_options *parse_options;
  lc_source *source;
} lc_lease_lonejson_load_thread;

static lonejson_read_result lc_lease_lonejson_load_reader(void *context,
                                                          unsigned char *buffer,
                                                          size_t capacity) {
  lc_source *source;
  lc_error public_error;
  lonejson_read_result result;
  size_t nread;

  source = (lc_source *)context;
  result = lonejson_default_read_result();
  lc_error_init(&public_error);
  nread = source->read(source, buffer, capacity, &public_error);
  result.bytes_read = nread;
  result.eof = nread == 0U && public_error.code == LC_OK ? 1 : 0;
  result.would_block = 0;
  result.error_code = public_error.code;
  lc_error_cleanup(&public_error);
  return result;
}

static void *lc_lease_lonejson_load_thread_main(void *context) {
  lc_lease_lonejson_load_thread *thread;

  thread = (lc_lease_lonejson_load_thread *)context;
  lonejson_error_init(&thread->bridge->lj_error);
  thread->bridge->status = lonejson_parse_reader(
      thread->map, thread->dst, lc_lease_lonejson_load_reader, thread->source,
      thread->parse_options, &thread->bridge->lj_error);
  if (thread->bridge->status != LONEJSON_STATUS_OK) {
    lc_stream_pipe_fail(thread->bridge->pipe, LC_ERR_PROTOCOL,
                        thread->bridge->lj_error.message);
  }
  return NULL;
}

typedef struct lc_lease_lonejson_save_bridge {
  lc_stream_pipe *pipe;
  lc_error thread_error;
} lc_lease_lonejson_save_bridge;

typedef struct lc_lease_lonejson_save_thread {
  lc_lease_lonejson_save_bridge *bridge;
  const lonejson_map *map;
  const void *src;
  const lonejson_write_options *write_options;
  lonejson_status status;
  lonejson_error lj_error;
} lc_lease_lonejson_save_thread;

static lonejson_status lc_lease_lonejson_save_sink(void *context,
                                                   const void *data,
                                                   size_t len,
                                                   lonejson_error *error) {
  lc_lease_lonejson_save_bridge *bridge;

  bridge = (lc_lease_lonejson_save_bridge *)context;
  if (bridge == NULL) {
    if (error != NULL) {
      error->code = LONEJSON_STATUS_INVALID_ARGUMENT;
      snprintf(error->message, sizeof(error->message),
               "mapped save bridge is required");
    }
    return LONEJSON_STATUS_INVALID_ARGUMENT;
  }
  if (len == 0U) {
    return LONEJSON_STATUS_OK;
  }
  if (lc_stream_pipe_write(bridge->pipe, data, len, &bridge->thread_error) !=
      LC_OK) {
    if (error != NULL) {
      error->code = LONEJSON_STATUS_CALLBACK_FAILED;
      snprintf(error->message, sizeof(error->message), "%s",
               bridge->thread_error.message != NULL
                   ? bridge->thread_error.message
                   : "failed to write mapped save stream");
    }
    return LONEJSON_STATUS_CALLBACK_FAILED;
  }
  return LONEJSON_STATUS_OK;
}

static void *lc_lease_lonejson_save_thread_main(void *context) {
  lc_lease_lonejson_save_thread *thread;

  thread = (lc_lease_lonejson_save_thread *)context;
  lc_error_init(&thread->bridge->thread_error);
  thread->status = lonejson_serialize_sink(thread->map, thread->src,
                                           lc_lease_lonejson_save_sink,
                                           thread->bridge, thread->write_options,
                                           &thread->lj_error);
  if (thread->status != LONEJSON_STATUS_OK) {
    lc_stream_pipe_fail(thread->bridge->pipe,
                        thread->bridge->thread_error.code != LC_OK
                            ? thread->bridge->thread_error.code
                            : LC_ERR_TRANSPORT,
                        thread->bridge->thread_error.message != NULL
                            ? thread->bridge->thread_error.message
                            : thread->lj_error.message);
  } else {
    lc_stream_pipe_finish(thread->bridge->pipe);
  }
  return NULL;
}

static void lc_lease_refresh_state_view(lc_lease_handle *lease,
                                        const char *state_etag, long version,
                                        long fencing_token,
                                        long lease_expires_at_unix) {
  if (lease == NULL) {
    return;
  }
  lc_client_free(lease->client, lease->state_etag);
  lease->state_etag = lc_client_strdup(lease->client, state_etag);
  lease->version = version;
  if (fencing_token > 0L) {
    lease->fencing_token = fencing_token;
  }
  if (lease_expires_at_unix > 0L) {
    lease->lease_expires_at_unix = lease_expires_at_unix;
  }
  lease->pub.state_etag = lease->state_etag;
  lease->pub.version = lease->version;
  lease->pub.fencing_token = lease->fencing_token;
  lease->pub.lease_expires_at_unix = lease->lease_expires_at_unix;
}

int lc_lease_describe_method(lc_lease *self, lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_describe_request legacy_req;
  lc_engine_describe_response legacy_res;
  lc_engine_error legacy_error;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lease describe requires self", NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    lc_log_trace(lease->client->logger, "client.describe.start", fields, 3U);
  }
  memset(&legacy_req, 0, sizeof(legacy_req));
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  legacy_req.namespace_name = lease->namespace_name;
  legacy_req.key = lease->key;
  rc = lc_engine_client_describe(lease->client->legacy, &legacy_req,
                                 &legacy_res, &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      lc_lease_log_error(lease,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? PSLOG_LEVEL_ERROR
                             : PSLOG_LEVEL_WARN,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? "client.describe.transport_error"
                             : "client.describe.error",
                         fields, 3U, error);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  lc_client_free(lease->client, lease->owner);
  lease->owner = lc_client_strdup(lease->client, legacy_res.owner);
  lc_client_free(lease->client, lease->state_etag);
  lease->state_etag = lc_client_strdup(lease->client, legacy_res.state_etag);
  lease->lease_expires_at_unix = legacy_res.expires_at_unix;
  lease->version = legacy_res.version;
  lease->has_query_hidden = legacy_res.has_query_hidden;
  lease->query_hidden = legacy_res.query_hidden;
  lease->pub.owner = lease->owner;
  lease->pub.state_etag = lease->state_etag;
  lease->pub.lease_expires_at_unix = lease->lease_expires_at_unix;
  lease->pub.version = lease->version;
  lease->pub.has_query_hidden = lease->has_query_hidden;
  lease->pub.query_hidden = lease->query_hidden;
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_i64_field("version", lease->version);
    fields[3] = lc_log_str_field("state_etag", lease->state_etag);
    fields[4] = lc_log_str_field("cid", legacy_res.correlation_id);
    lc_log_trace(lease->client->logger, "client.describe.success", fields, 5U);
  }
  lc_engine_describe_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_lease_get_method(lc_lease *self, lc_sink *dst, const lc_get_opts *opts,
                        lc_get_res *out, lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_get_request legacy_req;
  lc_engine_get_stream_response legacy_res;
  lc_engine_error legacy_error;
  lc_write_bridge bridge;
  int rc;

  if (self == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lease get requires self, dst, and out", NULL, NULL,
                        NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] =
        lc_log_bool_field("public", opts != NULL ? opts->public_read : 0);
    lc_log_trace(lease->client->logger, "client.get.start", fields, 4U);
  }
  memset(&legacy_req, 0, sizeof(legacy_req));
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  bridge.sink = dst;
  legacy_req.namespace_name = lease->namespace_name;
  legacy_req.key = lease->key;
  legacy_req.lease_id = lease->lease_id;
  legacy_req.fencing_token = lease->fencing_token;
  legacy_req.public_read = opts != NULL ? opts->public_read : 0;
  rc = lc_engine_client_get_into(lease->client->legacy, &legacy_req,
                                 lc_legacy_write_bridge, &bridge, &legacy_res,
                                 &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[4];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      fields[3] =
          lc_log_bool_field("public", opts != NULL ? opts->public_read : 0);
      lc_lease_log_error(lease,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? PSLOG_LEVEL_ERROR
                             : PSLOG_LEVEL_WARN,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? "client.get.transport_error"
                             : "client.get.error",
                         fields, 4U, error);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  out->no_content = legacy_res.no_content;
  out->content_type = lc_strdup_local(legacy_res.content_type);
  out->etag = lc_strdup_local(legacy_res.etag);
  out->version = legacy_res.version;
  out->fencing_token = legacy_res.fencing_token;
  out->correlation_id = lc_strdup_local(legacy_res.correlation_id);
  lc_lease_refresh_state_view(lease, legacy_res.etag, legacy_res.version,
                              legacy_res.fencing_token, 0L);
  if (legacy_res.no_content) {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] =
        lc_log_bool_field("public", opts != NULL ? opts->public_read : 0);
    fields[3] = lc_log_str_field("cid", legacy_res.correlation_id);
    lc_log_debug(lease->client->logger, "client.get.empty", fields, 4U);
  } else {
    pslog_field fields[6];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] =
        lc_log_bool_field("public", opts != NULL ? opts->public_read : 0);
    fields[4] = lc_log_str_field("etag", legacy_res.etag);
    fields[5] = lc_log_str_field("cid", legacy_res.correlation_id);
    lc_log_trace(lease->client->logger, "client.get.success", fields, 6U);
  }
  lc_engine_get_stream_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_lease_load_method(lc_lease *self, const lonejson_map *map, void *dst,
                         const lonejson_parse_options *parse_options,
                         const lc_get_opts *opts, lc_get_res *out,
                         lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_get_request legacy_req;
  lc_engine_get_stream_response legacy_res;
  lc_engine_error legacy_error;
  lc_lease_lonejson_load_bridge bridge;
  lc_lease_lonejson_load_thread load_thread;
  lc_source *source;
  pthread_t thread;
  int no_content;
  char *content_type;
  char *etag;
  char *correlation_id;
  long version;
  long fencing_token;
  int rc;

  if (self == NULL || map == NULL || dst == NULL || out == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "lease load requires self, map, destination, and out", NULL, NULL,
        NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] =
        lc_log_bool_field("public", opts != NULL ? opts->public_read : 0);
    lc_log_trace(lease->client->logger, "client.get.start", fields, 4U);
  }
  memset(&legacy_req, 0, sizeof(legacy_req));
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  memset(&bridge, 0, sizeof(bridge));
  rc = lc_stream_pipe_open(65536U, &lease->client->allocator, &source,
                           &bridge.pipe, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&load_thread, 0, sizeof(load_thread));
  load_thread.bridge = &bridge;
  load_thread.map = map;
  load_thread.dst = dst;
  load_thread.parse_options = parse_options;
  load_thread.source = source;
  rc = pthread_create(&thread, NULL, lc_lease_lonejson_load_thread_main,
                      &load_thread);
  if (rc != 0) {
    source->close(source);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to start mapped lease load parser thread",
                        NULL, NULL, NULL);
  }
  legacy_req.namespace_name = lease->namespace_name;
  legacy_req.key = lease->key;
  legacy_req.lease_id = lease->lease_id;
  legacy_req.fencing_token = lease->fencing_token;
  legacy_req.public_read = opts != NULL ? opts->public_read : 0;
  rc = lc_engine_client_get_into(lease->client->legacy, &legacy_req,
                                 lc_lease_lonejson_load_write_callback,
                                 &bridge,
                                 &legacy_res, &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[7];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      fields[3] =
          lc_log_bool_field("public", opts != NULL ? opts->public_read : 0);
      lc_lease_log_error(lease,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? PSLOG_LEVEL_ERROR
                             : PSLOG_LEVEL_WARN,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? "client.get.transport_error"
                             : "client.get.error",
                         fields, 4U, error);
    }
    lc_engine_get_stream_response_cleanup(&legacy_res);
    lc_stream_pipe_fail(bridge.pipe,
                        error != NULL ? error->code : LC_ERR_TRANSPORT,
                        error != NULL ? error->message
                                        : "failed to load mapped lease state");
    pthread_join(thread, NULL);
    source->close(source);
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  lc_lease_refresh_state_view(lease, legacy_res.etag, legacy_res.version,
                              legacy_res.fencing_token, 0L);
  if (legacy_res.no_content) {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] =
        lc_log_bool_field("public", opts != NULL ? opts->public_read : 0);
    fields[3] = lc_log_str_field("cid", legacy_res.correlation_id);
    lc_log_debug(lease->client->logger, "client.get.empty", fields, 4U);
  } else {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] =
        lc_log_bool_field("public", opts != NULL ? opts->public_read : 0);
    fields[3] = lc_log_str_field("etag", legacy_res.etag);
    fields[4] = lc_log_str_field("cid", legacy_res.correlation_id);
    lc_log_trace(lease->client->logger, "client.get.success", fields, 5U);
  }
  no_content = legacy_res.no_content;
  version = legacy_res.version;
  fencing_token = legacy_res.fencing_token;
  lc_stream_pipe_finish(bridge.pipe);
  pthread_join(thread, NULL);
  source->close(source);
  if (bridge.status != LONEJSON_STATUS_OK) {
    lc_engine_get_stream_response_cleanup(&legacy_res);
    lc_engine_error_cleanup(&legacy_error);
    return lc_lonejson_error_from_status(
        error, bridge.status, &bridge.lj_error,
        "failed to parse mapped lease state");
  }
  content_type = lc_strdup_local(legacy_res.content_type);
  etag = lc_strdup_local(legacy_res.etag);
  correlation_id = lc_strdup_local(legacy_res.correlation_id);
  out->no_content = no_content;
  out->content_type = content_type;
  out->etag = etag;
  out->version = version;
  out->fencing_token = fencing_token;
  out->correlation_id = correlation_id;
  lc_engine_get_stream_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_lease_save_method(lc_lease *self, const lonejson_map *map,
                         const void *src,
                         const lonejson_write_options *write_options,
                         lc_error *error) {
  lc_lease_handle *lease;
  lc_lease_lonejson_save_bridge save_bridge;
  lc_lease_lonejson_save_thread save_thread;
  lc_source *source;
  lc_json *json;
  lc_update_opts update_opts;
  pthread_t thread;
  int rc;

  if (self == NULL || map == NULL || src == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lease save requires self, map, and source", NULL,
                        NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  memset(&save_bridge, 0, sizeof(save_bridge));
  rc = lc_stream_pipe_open(65536U, &lease->client->allocator, &source,
                           &save_bridge.pipe, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_json_from_source(source, &json, error);
  if (rc != LC_OK) {
    source->close(source);
    return rc;
  }
  memset(&save_thread, 0, sizeof(save_thread));
  save_thread.bridge = &save_bridge;
  save_thread.map = map;
  save_thread.src = src;
  save_thread.write_options = write_options;
  save_bridge.thread_error.code = LC_OK;
  save_bridge.thread_error.message = NULL;
  rc = pthread_create(&thread, NULL, lc_lease_lonejson_save_thread_main,
                      &save_thread);
  if (rc != 0) {
    json->close(json);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to start mapped lease save stream", NULL, NULL,
                        NULL);
  }
  source = NULL;
  memset(&update_opts, 0, sizeof(update_opts));
  update_opts.content_type = "application/json";
  rc = lc_lease_update_method(self, json, &update_opts, error);
  json->close(json);
  pthread_join(thread, NULL);
  if (rc != LC_OK) {
    return rc;
  }
  if (save_thread.status != LONEJSON_STATUS_OK) {
    return lc_lonejson_error_from_status(
        error, save_thread.status, &save_thread.lj_error,
        "failed to serialize mapped lease state");
  }
  return LC_OK;
}

int lc_lease_update_method(lc_lease *self, lc_json *json,
                           const lc_update_opts *opts, lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_update_request legacy_req;
  lc_engine_update_response legacy_res;
  lc_engine_error legacy_error;
  lc_read_bridge bridge;
  int rc;

  if (self == NULL || json == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lease update requires self and json", NULL, NULL,
                        NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] = lc_log_str_field("content_type",
                                 opts != NULL ? opts->content_type : NULL);
    lc_log_trace(lease->client->logger, "client.update.start", fields, 4U);
  }
  memset(&legacy_req, 0, sizeof(legacy_req));
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  bridge.source = (lc_source *)json;
  legacy_req.namespace_name = lease->namespace_name;
  legacy_req.key = lease->key;
  legacy_req.lease_id = lease->lease_id;
  legacy_req.txn_id = lease->txn_id;
  legacy_req.fencing_token = lease->fencing_token;
  legacy_req.if_state_etag = opts != NULL ? opts->if_state_etag : NULL;
  legacy_req.if_version = opts != NULL ? opts->if_version : 0L;
  legacy_req.has_if_version = opts != NULL ? opts->has_if_version : 0;
  if (!legacy_req.has_if_version && lease->version > 0L) {
    legacy_req.if_version = lease->version;
    legacy_req.has_if_version = 1;
  }
  legacy_req.content_type =
      opts != NULL ? opts->content_type : "application/json";
  rc = lc_engine_client_update_from(lease->client->legacy, &legacy_req,
                                    lc_legacy_read_bridge, &bridge, &legacy_res,
                                    &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[7];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      fields[3] = lc_log_str_field("content_type",
                                   opts != NULL ? opts->content_type : NULL);
      fields[4] = lc_log_code_field(error);
      fields[5] = lc_log_http_status_field(error);
      fields[6] = lc_log_error_field("error", error);
      lc_log_warn(lease->client->logger, "client.update.error", fields, 7U);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  lc_client_free(lease->client, lease->state_etag);
  lease->state_etag =
      lc_client_strdup(lease->client, legacy_res.new_state_etag);
  lease->version = legacy_res.new_version;
  lease->pub.state_etag = lease->state_etag;
  lease->pub.version = lease->version;
  {
    pslog_field fields[6];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] = lc_log_str_field("new_etag", legacy_res.new_state_etag);
    fields[4] = lc_log_i64_field("version", legacy_res.new_version);
    fields[5] = lc_log_str_field("cid", legacy_res.correlation_id);
    lc_log_trace(lease->client->logger, "client.update.success", fields, 6U);
  }
  lc_engine_update_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_lease_mutate_method(lc_lease *self, const lc_mutate_req *req,
                           lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_mutate_request legacy_req;
  lc_engine_mutate_response legacy_res;
  lc_engine_error legacy_error;
  int rc;

  if (self == NULL || req == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lease mutate requires self and req", NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] = lc_log_i64_field("mutation_count", (long)req->mutation_count);
    lc_log_trace(lease->client->logger, "client.mutate.start", fields, 4U);
  }
  memset(&legacy_req, 0, sizeof(legacy_req));
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  legacy_req.namespace_name = lease->namespace_name;
  legacy_req.key = lease->key;
  legacy_req.lease_id = lease->lease_id;
  legacy_req.txn_id = lease->txn_id;
  legacy_req.fencing_token = lease->fencing_token;
  legacy_req.if_state_etag = req->if_state_etag;
  legacy_req.if_version = req->if_version;
  legacy_req.has_if_version = req->has_if_version;
  if (!legacy_req.has_if_version && lease->version > 0L) {
    legacy_req.if_version = lease->version;
    legacy_req.has_if_version = 1;
  }
  legacy_req.mutations = req->mutations;
  legacy_req.mutation_count = req->mutation_count;
  rc = lc_engine_client_mutate(lease->client->legacy, &legacy_req, &legacy_res,
                               &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[4];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      fields[3] = lc_log_i64_field("mutation_count", (long)req->mutation_count);
      lc_lease_log_error(lease,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? PSLOG_LEVEL_ERROR
                             : PSLOG_LEVEL_WARN,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? "client.mutate.transport_error"
                             : "client.mutate.error",
                         fields, 4U, error);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  lc_client_free(lease->client, lease->state_etag);
  lease->state_etag =
      lc_client_strdup(lease->client, legacy_res.new_state_etag);
  lease->version = legacy_res.new_version;
  lease->pub.state_etag = lease->state_etag;
  lease->pub.version = lease->version;
  {
    pslog_field fields[6];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] = lc_log_i64_field("new_version", legacy_res.new_version);
    fields[4] = lc_log_str_field("new_etag", legacy_res.new_state_etag);
    fields[5] = lc_log_str_field("cid", legacy_res.correlation_id);
    lc_log_trace(lease->client->logger, "client.mutate.success", fields, 6U);
  }
  lc_engine_mutate_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_lease_metadata_method(lc_lease *self, const lc_metadata_req *req,
                             lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_metadata_request legacy_req;
  lc_engine_metadata_response legacy_res;
  lc_engine_error legacy_error;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lease metadata requires self", NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    lc_log_trace(lease->client->logger, "client.metadata.start", fields, 3U);
  }
  memset(&legacy_req, 0, sizeof(legacy_req));
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  legacy_req.namespace_name = lease->namespace_name;
  legacy_req.key = lease->key;
  legacy_req.lease_id = lease->lease_id;
  legacy_req.txn_id = lease->txn_id;
  legacy_req.fencing_token = lease->fencing_token;
  if (req != NULL) {
    legacy_req.if_version = req->if_version;
    legacy_req.has_if_version = req->has_if_version;
    legacy_req.has_query_hidden = req->has_query_hidden;
    legacy_req.query_hidden = req->query_hidden;
  }
  if (!legacy_req.has_if_version && lease->version > 0L) {
    legacy_req.if_version = lease->version;
    legacy_req.has_if_version = 1;
  }
  rc = lc_engine_client_update_metadata(lease->client->legacy, &legacy_req,
                                        &legacy_res, &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      lc_lease_log_error(lease,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? PSLOG_LEVEL_ERROR
                             : PSLOG_LEVEL_WARN,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? "client.metadata.transport_error"
                             : "client.metadata.error",
                         fields, 3U, error);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  lease->version = legacy_res.version;
  lease->has_query_hidden = legacy_res.has_query_hidden;
  lease->query_hidden = legacy_res.query_hidden;
  lease->pub.version = lease->version;
  lease->pub.has_query_hidden = lease->has_query_hidden;
  lease->pub.query_hidden = lease->query_hidden;
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_i64_field("version", legacy_res.version);
    fields[3] = lc_log_bool_field("query_hidden", legacy_res.query_hidden);
    fields[4] = lc_log_str_field("cid", legacy_res.correlation_id);
    lc_log_trace(lease->client->logger, "client.metadata.success", fields, 5U);
  }
  lc_engine_metadata_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_lease_remove_method(lc_lease *self, const lc_remove_req *req,
                           lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_remove_request legacy_req;
  lc_engine_remove_response legacy_res;
  lc_engine_error legacy_error;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, "lease remove requires self",
                        NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    lc_log_trace(lease->client->logger, "client.remove.start", fields, 3U);
  }
  memset(&legacy_req, 0, sizeof(legacy_req));
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  legacy_req.namespace_name = lease->namespace_name;
  legacy_req.key = lease->key;
  legacy_req.lease_id = lease->lease_id;
  legacy_req.txn_id = lease->txn_id;
  legacy_req.fencing_token = lease->fencing_token;
  legacy_req.if_state_etag = req != NULL ? req->if_state_etag : NULL;
  legacy_req.if_version = req != NULL ? req->if_version : 0L;
  legacy_req.has_if_version = req != NULL ? req->has_if_version : 0;
  if (!legacy_req.has_if_version && lease->version > 0L) {
    legacy_req.if_version = lease->version;
    legacy_req.has_if_version = 1;
  }
  rc = lc_engine_client_remove(lease->client->legacy, &legacy_req, &legacy_res,
                               &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      lc_lease_log_error(lease,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? PSLOG_LEVEL_ERROR
                             : PSLOG_LEVEL_WARN,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? "client.remove.transport_error"
                             : "client.remove.error",
                         fields, 3U, error);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  if (legacy_res.removed) {
    lc_client_free(lease->client, lease->state_etag);
    lease->state_etag = NULL;
    lease->version = legacy_res.new_version;
    lease->pub.state_etag = lease->state_etag;
    lease->pub.version = lease->version;
  }
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_bool_field("removed", legacy_res.removed);
    fields[3] = lc_log_i64_field("new_version", legacy_res.new_version);
    fields[4] = lc_log_str_field("cid", legacy_res.correlation_id);
    lc_log_trace(lease->client->logger, "client.remove.success", fields, 5U);
  }
  lc_engine_remove_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_lease_keepalive_method(lc_lease *self, const lc_keepalive_req *req,
                              lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_keepalive_request legacy_req;
  lc_engine_keepalive_response legacy_res;
  lc_engine_error legacy_error;
  int rc;

  if (self == NULL || req == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lease keepalive requires self and req", NULL, NULL,
                        NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] = lc_log_i64_field("ttl_seconds", req->ttl_seconds);
    lc_log_trace(lease->client->logger, "client.keepalive.start", fields, 4U);
  }
  memset(&legacy_req, 0, sizeof(legacy_req));
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  legacy_req.namespace_name = lease->namespace_name;
  legacy_req.key = lease->key;
  legacy_req.lease_id = lease->lease_id;
  legacy_req.txn_id = lease->txn_id;
  legacy_req.ttl_seconds = req->ttl_seconds;
  legacy_req.fencing_token = lease->fencing_token;
  rc = lc_engine_client_keepalive(lease->client->legacy, &legacy_req,
                                  &legacy_res, &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[4];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      fields[3] = lc_log_i64_field("ttl_seconds", req->ttl_seconds);
      lc_lease_log_error(lease, PSLOG_LEVEL_ERROR, "client.keepalive.error",
                         fields, 4U, error);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  lc_client_free(lease->client, lease->state_etag);
  lease->state_etag = lc_client_strdup(lease->client, legacy_res.state_etag);
  lease->lease_expires_at_unix = legacy_res.lease_expires_at_unix;
  lease->version = legacy_res.version;
  lease->pub.state_etag = lease->state_etag;
  lease->pub.lease_expires_at_unix = lease->lease_expires_at_unix;
  lease->pub.version = lease->version;
  {
    pslog_field fields[6];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] = lc_log_i64_field("version", legacy_res.version);
    fields[4] =
        lc_log_i64_field("expires_at", legacy_res.lease_expires_at_unix);
    fields[5] = lc_log_str_field("cid", legacy_res.correlation_id);
    lc_log_trace(lease->client->logger, "client.keepalive.success", fields, 6U);
  }
  lc_engine_keepalive_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_lease_release_method(lc_lease *self, const lc_release_req *req,
                            lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_release_request legacy_req;
  lc_engine_release_response legacy_res;
  lc_engine_error legacy_error;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lease release requires self", NULL, NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] = lc_log_bool_field("rollback", req != NULL ? req->rollback : 0);
    lc_log_trace(lease->client->logger, "client.release.start", fields, 4U);
  }
  memset(&legacy_req, 0, sizeof(legacy_req));
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  legacy_req.namespace_name = lease->namespace_name;
  legacy_req.key = lease->key;
  legacy_req.lease_id = lease->lease_id;
  legacy_req.txn_id = lease->txn_id;
  legacy_req.fencing_token = lease->fencing_token;
  legacy_req.rollback = req != NULL ? req->rollback : 0;
  rc = lc_engine_client_release(lease->client->legacy, &legacy_req, &legacy_res,
                                &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[4];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      fields[3] =
          lc_log_bool_field("rollback", req != NULL ? req->rollback : 0);
      lc_lease_log_error(lease, PSLOG_LEVEL_ERROR, "client.release.error",
                         fields, 4U, error);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] = lc_log_bool_field("released", legacy_res.released);
    fields[4] = lc_log_str_field("cid", legacy_res.correlation_id);
    lc_log_trace(lease->client->logger, "client.release.success", fields, 5U);
  }
  lc_engine_release_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  lc_lease_close_method(self);
  return LC_OK;
}

int lc_lease_attach_method(lc_lease *self, const lc_attach_req *req,
                           lc_source *src, lc_attach_res *out,
                           lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_attach_request legacy_req;
  lc_engine_attach_response legacy_res;
  lc_engine_error legacy_error;
  lc_read_bridge bridge;
  int rc;

  if (self == NULL || req == NULL || src == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lease attach requires self, req, src, and out", NULL,
                        NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] = lc_log_str_field("name", req->name);
    lc_log_trace(lease->client->logger, "client.attachment.put.start", fields,
                 4U);
  }
  memset(&legacy_req, 0, sizeof(legacy_req));
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  bridge.source = src;
  legacy_req.namespace_name = lease->namespace_name;
  legacy_req.key = lease->key;
  legacy_req.lease_id = lease->lease_id;
  legacy_req.txn_id = lease->txn_id;
  legacy_req.fencing_token = lease->fencing_token;
  legacy_req.name = req->name;
  legacy_req.content_type = req->content_type;
  legacy_req.max_bytes = req->max_bytes;
  legacy_req.has_max_bytes = req->has_max_bytes;
  legacy_req.prevent_overwrite = req->prevent_overwrite;
  rc = lc_engine_client_attach_from(lease->client->legacy, &legacy_req,
                                    lc_legacy_read_bridge, &bridge, &legacy_res,
                                    &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[4];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      fields[3] = lc_log_str_field("name", req->name);
      lc_lease_log_error(lease,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? PSLOG_LEVEL_ERROR
                             : PSLOG_LEVEL_WARN,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? "client.attachment.put.transport_error"
                             : "client.attachment.put.error",
                         fields, 4U, error);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  lc_attachment_info_copy(&out->attachment, &legacy_res.attachment);
  out->noop = legacy_res.noop;
  out->version = legacy_res.version;
  out->correlation_id = lc_strdup_local(legacy_res.correlation_id);
  {
    pslog_field fields[6];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("name", legacy_res.attachment.name);
    fields[3] = lc_log_str_field("attachment_id", legacy_res.attachment.id);
    fields[4] = lc_log_bool_field("noop", legacy_res.noop);
    fields[5] = lc_log_str_field("cid", legacy_res.correlation_id);
    lc_log_trace(lease->client->logger, "client.attachment.put.success", fields,
                 6U);
  }
  lc_engine_attach_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_lease_list_attachments_method(lc_lease *self, lc_attachment_list *out,
                                     lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_list_attachments_request legacy_req;
  lc_engine_list_attachments_response legacy_res;
  lc_engine_error legacy_error;
  size_t index;
  int rc;

  if (self == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "list_attachments requires self and out", NULL, NULL,
                        NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[2];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    lc_log_trace(lease->client->logger, "client.attachment.list.start", fields,
                 2U);
  }
  memset(&legacy_req, 0, sizeof(legacy_req));
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  legacy_req.namespace_name = lease->namespace_name;
  legacy_req.key = lease->key;
  legacy_req.lease_id = lease->lease_id;
  legacy_req.txn_id = lease->txn_id;
  legacy_req.fencing_token = lease->fencing_token;
  rc = lc_engine_client_list_attachments(lease->client->legacy, &legacy_req,
                                         &legacy_res, &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      lc_lease_log_error(lease,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? PSLOG_LEVEL_ERROR
                             : PSLOG_LEVEL_WARN,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? "client.attachment.list.transport_error"
                             : "client.attachment.list.error",
                         fields, 2U, error);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  out->count = legacy_res.attachment_count;
  if (out->count > 0U) {
    out->items =
        (lc_attachment_info *)calloc(out->count, sizeof(lc_attachment_info));
    if (out->items == NULL) {
      lc_engine_list_attachments_response_cleanup(&legacy_res);
      lc_engine_error_cleanup(&legacy_error);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate attachment list", NULL, NULL,
                          NULL);
    }
    for (index = 0U; index < out->count; ++index) {
      lc_attachment_info_copy(&out->items[index],
                              &legacy_res.attachments[index]);
    }
  }
  out->correlation_id = lc_strdup_local(legacy_res.correlation_id);
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_u64_field("count", out->count);
    fields[3] = lc_log_str_field("cid", legacy_res.correlation_id);
    lc_log_trace(lease->client->logger, "client.attachment.list.success",
                 fields, 4U);
  }
  lc_engine_list_attachments_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_lease_get_attachment_method(lc_lease *self,
                                   const lc_attachment_get_req *req,
                                   lc_sink *dst, lc_attachment_get_res *out,
                                   lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_get_attachment_request legacy_req;
  lc_engine_get_attachment_response legacy_res;
  lc_engine_error legacy_error;
  lc_write_bridge bridge;
  int rc;

  if (self == NULL || req == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "get_attachment requires self, req, dst, and out", NULL,
                        NULL, NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("attachment_id", req->selector.id);
    fields[3] = lc_log_str_field("name", req->selector.name);
    lc_log_trace(lease->client->logger, "client.attachment.get.start", fields,
                 4U);
  }
  memset(&legacy_req, 0, sizeof(legacy_req));
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  bridge.sink = dst;
  legacy_req.namespace_name = lease->namespace_name;
  legacy_req.key = lease->key;
  legacy_req.lease_id = lease->lease_id;
  legacy_req.txn_id = lease->txn_id;
  legacy_req.fencing_token = lease->fencing_token;
  legacy_req.public_read = req->public_read;
  legacy_req.selector.id = req->selector.id;
  legacy_req.selector.name = req->selector.name;
  rc = lc_engine_client_get_attachment_into(lease->client->legacy, &legacy_req,
                                            lc_legacy_write_bridge, &bridge,
                                            &legacy_res, &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[4];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("attachment_id", req->selector.id);
      fields[3] = lc_log_str_field("name", req->selector.name);
      lc_lease_log_error(lease,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? PSLOG_LEVEL_ERROR
                             : PSLOG_LEVEL_WARN,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? "client.attachment.get.transport_error"
                             : "client.attachment.get.error",
                         fields, 4U, error);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  lc_attachment_info_copy(&out->attachment, &legacy_res.attachment);
  out->correlation_id = lc_strdup_local(legacy_res.correlation_id);
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("attachment_id", legacy_res.attachment.id);
    fields[3] = lc_log_str_field("name", legacy_res.attachment.name);
    fields[4] = lc_log_str_field("cid", legacy_res.correlation_id);
    lc_log_trace(lease->client->logger, "client.attachment.get.success", fields,
                 5U);
  }
  lc_engine_get_attachment_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_lease_delete_attachment_method(lc_lease *self,
                                      const lc_attachment_selector *selector,
                                      int *deleted, lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_delete_attachment_request legacy_req;
  lc_engine_delete_attachment_response legacy_res;
  lc_engine_error legacy_error;
  int rc;

  if (self == NULL || selector == NULL || deleted == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "delete_attachment requires self, selector, and deleted", NULL, NULL,
        NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("attachment_id", selector->id);
    fields[3] = lc_log_str_field("name", selector->name);
    lc_log_trace(lease->client->logger, "client.attachment.delete.start",
                 fields, 4U);
  }
  memset(&legacy_req, 0, sizeof(legacy_req));
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  legacy_req.namespace_name = lease->namespace_name;
  legacy_req.key = lease->key;
  legacy_req.lease_id = lease->lease_id;
  legacy_req.txn_id = lease->txn_id;
  legacy_req.fencing_token = lease->fencing_token;
  legacy_req.selector.id = selector->id;
  legacy_req.selector.name = selector->name;
  rc = lc_engine_client_delete_attachment(lease->client->legacy, &legacy_req,
                                          &legacy_res, &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[4];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("attachment_id", selector->id);
      fields[3] = lc_log_str_field("name", selector->name);
      lc_lease_log_error(lease,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? PSLOG_LEVEL_ERROR
                             : PSLOG_LEVEL_WARN,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? "client.attachment.delete.transport_error"
                             : "client.attachment.delete.error",
                         fields, 4U, error);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  *deleted = legacy_res.deleted;
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("attachment_id", selector->id);
    fields[3] = lc_log_bool_field("deleted", legacy_res.deleted);
    fields[4] = lc_log_str_field("cid", legacy_res.correlation_id);
    lc_log_trace(lease->client->logger, "client.attachment.delete.success",
                 fields, 5U);
  }
  lc_engine_delete_attachment_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_lease_delete_all_attachments_method(lc_lease *self, int *deleted_count,
                                           lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_delete_all_attachments_request legacy_req;
  lc_engine_delete_all_attachments_response legacy_res;
  lc_engine_error legacy_error;
  int rc;

  if (self == NULL || deleted_count == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "delete_all_attachments requires self and deleted_count", NULL, NULL,
        NULL);
  }
  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[2];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    lc_log_trace(lease->client->logger, "client.attachment.delete_all.start",
                 fields, 2U);
  }
  memset(&legacy_req, 0, sizeof(legacy_req));
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  legacy_req.namespace_name = lease->namespace_name;
  legacy_req.key = lease->key;
  legacy_req.lease_id = lease->lease_id;
  legacy_req.txn_id = lease->txn_id;
  legacy_req.fencing_token = lease->fencing_token;
  rc = lc_engine_client_delete_all_attachments(
      lease->client->legacy, &legacy_req, &legacy_res, &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      lc_lease_log_error(lease,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? PSLOG_LEVEL_ERROR
                             : PSLOG_LEVEL_WARN,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? "client.attachment.delete_all.transport_error"
                             : "client.attachment.delete_all.error",
                         fields, 2U, error);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  *deleted_count = legacy_res.deleted;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_i64_field("deleted", legacy_res.deleted);
    fields[3] = lc_log_str_field("cid", legacy_res.correlation_id);
    lc_log_trace(lease->client->logger, "client.attachment.delete_all.success",
                 fields, 4U);
  }
  lc_engine_delete_all_attachments_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

void lc_lease_close_method(lc_lease *self) {
  lc_lease_handle *lease;

  if (self == NULL) {
    return;
  }
  lease = (lc_lease_handle *)self;
  lc_client_free(lease->client, lease->namespace_name);
  lc_client_free(lease->client, lease->key);
  lc_client_free(lease->client, lease->owner);
  lc_client_free(lease->client, lease->lease_id);
  lc_client_free(lease->client, lease->txn_id);
  lc_client_free(lease->client, lease->state_etag);
  lc_client_free(lease->client, lease->queue_state_etag);
  lc_client_free(lease->client, lease);
}

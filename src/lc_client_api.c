#include "lc_api_internal.h"
#include "lc_internal.h"
#include "lc_log.h"

static void *lc_subscribe_handler_main(void *context);

static void lc_client_log_operation_error(lc_client_handle *client,
                                          pslog_level level,
                                          const char *message,
                                          const pslog_field *fields,
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
    lc_log_debug(client->logger, message, combined, count + 3U);
    break;
  case PSLOG_LEVEL_WARN:
    lc_log_warn(client->logger, message, combined, count + 3U);
    break;
  default:
    lc_log_error(client->logger, message, combined, count + 3U);
    break;
  }
}

typedef struct lc_client_lonejson_load_state {
  lonejson_curl_parse parse;
  size_t byte_limit;
  size_t bytes_received;
} lc_client_lonejson_load_state;

static int lc_client_lonejson_load_write_callback(void *context,
                                                  const void *bytes,
                                                  size_t count,
                                                  lc_engine_error *error) {
  lc_client_lonejson_load_state *state;
  size_t written;

  state = (lc_client_lonejson_load_state *)context;
  if (state == NULL) {
    lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                               "mapped load state is required");
    return 0U;
  }
  if (count > 0U && state->byte_limit > 0U &&
      count > state->byte_limit - state->bytes_received) {
    lc_engine_set_protocol_error(
        error, "mapped state response exceeds configured byte limit");
    return 0U;
  }
  if (count == 0U) {
    return 1;
  }
  written =
      lonejson_curl_write_callback((char *)bytes, 1U, count, &state->parse);
  if (written != count) {
    lc_engine_lonejson_error_from_status(
        error, state->parse.error.code, &state->parse.error,
        "failed to parse mapped state response");
    return 0U;
  }
  state->bytes_received += count;
  return 1;
}

int lc_client_acquire_method(lc_client *self, const lc_acquire_req *req,
                             lc_lease **out, lc_error *error) {
  lc_client_handle *client;
  lc_engine_acquire_request engine_req;
  lc_engine_acquire_response engine_res;
  lc_engine_error engine_error;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "acquire requires self, req, and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("key", req->key);
    fields[1] = lc_log_str_field("owner", req->owner);
    fields[2] = pslog_i64("ttl_seconds", (pslog_int64)req->ttl_seconds);
    lc_log_trace(client->logger, "client.acquire.start", fields, 3U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.namespace_name = req->namespace_name;
  engine_req.key = req->key;
  engine_req.owner = req->owner;
  engine_req.ttl_seconds = req->ttl_seconds;
  engine_req.block_seconds = req->block_seconds;
  engine_req.if_not_exists = req->if_not_exists;
  engine_req.txn_id = req->txn_id;
  rc = lc_engine_client_acquire(client->engine, &engine_req, &engine_res,
                                &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("key", req->key);
      fields[1] = lc_log_str_field("owner", req->owner);
      lc_client_log_operation_error(client, PSLOG_LEVEL_ERROR,
                                    "client.acquire.error", fields, 2U, error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  *out = lc_lease_new(client, engine_res.namespace_name, engine_res.key,
                      engine_res.owner, engine_res.lease_id, engine_res.txn_id,
                      engine_res.fencing_token, engine_res.version,
                      engine_res.state_etag, NULL);
  if (*out == NULL) {
    lc_engine_acquire_response_cleanup(&engine_res);
    lc_engine_error_cleanup(&engine_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate lease handle", NULL, NULL, NULL);
  }
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", req->key);
    fields[1] = lc_log_str_field("lease_id", engine_res.lease_id);
    fields[2] = lc_log_str_field("txn_id", engine_res.txn_id);
    fields[3] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_info(client->logger, "client.acquire.success", fields, 4U);
  }
  lc_engine_acquire_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_client_describe_method(lc_client *self, const lc_describe_req *req,
                              lc_describe_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_engine_describe_request engine_req;
  lc_engine_describe_response engine_res;
  lc_engine_error engine_error;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "describe requires self, req, and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[1];

    fields[0] = lc_log_str_field("key", req->key);
    lc_log_trace(client->logger, "client.describe.start", fields, 1U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.namespace_name = req->namespace_name;
  engine_req.key = req->key;
  rc = lc_engine_client_describe(client->engine, &engine_req, &engine_res,
                                 &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[1];

      fields[0] = lc_log_str_field("key", req->key);
      lc_client_log_operation_error(client, PSLOG_LEVEL_WARN,
                                    "client.describe.error", fields, 1U, error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  out->namespace_name = lc_strdup_local(engine_res.namespace_name);
  out->key = lc_strdup_local(engine_res.key);
  out->owner = lc_strdup_local(engine_res.owner);
  out->lease_id = lc_strdup_local(engine_res.lease_id);
  out->lease_expires_at_unix = engine_res.expires_at_unix;
  out->version = engine_res.version;
  out->state_etag = lc_strdup_local(engine_res.state_etag);
  out->has_query_hidden = engine_res.has_query_hidden;
  out->query_hidden = engine_res.query_hidden;
  out->correlation_id = lc_strdup_local(engine_res.correlation_id);
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", req->key);
    fields[1] = pslog_i64("version", (pslog_int64)engine_res.version);
    fields[2] = lc_log_str_field("state_etag", engine_res.state_etag);
    fields[3] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(client->logger, "client.describe.success", fields, 4U);
  }
  lc_engine_describe_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_client_get_method(lc_client *self, const char *key,
                         const lc_get_opts *opts, lc_sink *dst, lc_get_res *out,
                         lc_error *error) {
  lc_client_handle *client;
  lc_engine_get_request engine_req;
  lc_engine_get_stream_response engine_res;
  lc_engine_error engine_error;
  lc_write_bridge bridge;
  int rc;

  if (self == NULL || key == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "get requires self, key, dst, and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[2];

    fields[0] = lc_log_str_field("key", key);
    fields[1] =
        lc_log_bool_field("public", opts != NULL ? opts->public_read : 0);
    lc_log_trace(client->logger, "client.get.start", fields, 2U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  bridge.sink = dst;
  engine_req.key = key;
  engine_req.public_read = opts != NULL ? opts->public_read : 0;
  rc = lc_engine_client_get_into(client->engine, &engine_req,
                                 lc_engine_write_bridge, &bridge, &engine_res,
                                 &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("key", key);
      fields[1] =
          lc_log_bool_field("public", opts != NULL ? opts->public_read : 0);
      lc_client_log_operation_error(
          client,
          error != NULL && error->code == LC_ERR_TRANSPORT ? PSLOG_LEVEL_ERROR
                                                           : PSLOG_LEVEL_WARN,
          error != NULL && error->code == LC_ERR_TRANSPORT
              ? "client.get.transport_error"
              : "client.get.error",
          fields, 2U, error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  out->no_content = engine_res.no_content;
  out->content_type = lc_strdup_local(engine_res.content_type);
  out->etag = lc_strdup_local(engine_res.etag);
  out->version = engine_res.version;
  out->fencing_token = engine_res.fencing_token;
  out->correlation_id = lc_strdup_local(engine_res.correlation_id);
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("key", key);
    fields[1] =
        lc_log_bool_field("public", opts != NULL ? opts->public_read : 0);
    fields[2] = pslog_i64("version", (pslog_int64)engine_res.version);
    fields[3] =
        pslog_i64("fencing_token", (pslog_int64)engine_res.fencing_token);
    fields[4] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(client->logger, "client.get.success", fields, 5U);
  }
  lc_engine_get_stream_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_client_load_method(lc_client *self, const char *key,
                          const lonejson_map *map, void *dst,
                          const lonejson_parse_options *parse_options,
                          const lc_get_opts *opts, lc_get_res *out,
                          lc_error *error) {
  lc_client_handle *client;
  lc_engine_get_request engine_req;
  lc_engine_get_stream_response engine_res;
  lc_engine_error engine_error;
  lc_client_lonejson_load_state load_state;
  lonejson_parse_options options;
  int no_content;
  char *content_type;
  char *etag;
  char *correlation_id;
  long version;
  long fencing_token;
  int rc;

  if (self == NULL || key == NULL || map == NULL || dst == NULL ||
      out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "load requires self, key, map, destination, and out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[2];

    fields[0] = lc_log_str_field("key", key);
    fields[1] =
        lc_log_bool_field("public", opts != NULL ? opts->public_read : 0);
    lc_log_trace(client->logger, "client.get.start", fields, 2U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  memset(&load_state, 0, sizeof(load_state));
  options =
      parse_options != NULL ? *parse_options : lonejson_default_parse_options();
  load_state.byte_limit = client->http_json_response_limit_bytes > 0U
                              ? client->http_json_response_limit_bytes
                              : (size_t)LC_HTTP_JSON_RESPONSE_LIMIT_DEFAULT;
  lonejson_init(map, dst);
  rc = lonejson_curl_parse_init(&load_state.parse, map, dst, &options);
  if (rc != LONEJSON_STATUS_OK) {
    lonejson_cleanup(map, dst);
    return lc_lonejson_error_from_status(
        error, rc, &load_state.parse.error,
        "failed to initialize mapped load parser");
  }
  engine_req.key = key;
  engine_req.public_read = opts != NULL ? opts->public_read : 0;
  rc = lc_engine_client_get_into(client->engine, &engine_req,
                                 lc_client_lonejson_load_write_callback,
                                 &load_state, &engine_res, &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("key", key);
      fields[1] =
          lc_log_bool_field("public", opts != NULL ? opts->public_read : 0);
      lc_client_log_operation_error(
          client,
          error != NULL && error->code == LC_ERR_TRANSPORT ? PSLOG_LEVEL_ERROR
                                                           : PSLOG_LEVEL_WARN,
          error != NULL && error->code == LC_ERR_TRANSPORT
              ? "client.get.transport_error"
              : "client.get.error",
          fields, 2U, error);
    }
    lonejson_curl_parse_cleanup(&load_state.parse);
    lonejson_cleanup(map, dst);
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("key", key);
    fields[1] =
        lc_log_bool_field("public", opts != NULL ? opts->public_read : 0);
    fields[2] = pslog_i64("version", (pslog_int64)engine_res.version);
    fields[3] =
        pslog_i64("fencing_token", (pslog_int64)engine_res.fencing_token);
    fields[4] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(client->logger, "client.get.success", fields, 5U);
  }
  no_content = engine_res.no_content;
  version = engine_res.version;
  fencing_token = engine_res.fencing_token;
  if (!engine_res.no_content) {
    rc = lonejson_curl_parse_finish(&load_state.parse);
    if (rc != LONEJSON_STATUS_OK) {
      lc_engine_get_stream_response_cleanup(&engine_res);
      lc_engine_error_cleanup(&engine_error);
      lonejson_curl_parse_cleanup(&load_state.parse);
      lonejson_cleanup(map, dst);
      return lc_lonejson_error_from_status(error, rc, &load_state.parse.error,
                                           "failed to parse mapped state");
    }
  }
  lonejson_curl_parse_cleanup(&load_state.parse);
  content_type = lc_strdup_local(engine_res.content_type);
  etag = lc_strdup_local(engine_res.etag);
  correlation_id = lc_strdup_local(engine_res.correlation_id);
  out->no_content = no_content;
  out->content_type = content_type;
  out->etag = etag;
  out->version = version;
  out->fencing_token = fencing_token;
  out->correlation_id = correlation_id;
  lc_engine_get_stream_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_client_update_method(lc_client *self, const lc_update_req *req,
                            lc_json *json, lc_update_res *out,
                            lc_error *error) {
  lc_client_handle *client;
  lc_engine_update_request engine_req;
  lc_engine_update_response engine_res;
  lc_engine_error engine_error;
  lc_read_bridge bridge;
  int rc;

  if (self == NULL || req == NULL || json == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "update requires self, req, json, and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", req->lease.key);
    fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
    fields[2] = lc_log_str_field("txn_id", req->lease.txn_id);
    fields[3] =
        pslog_i64("fencing_token", (pslog_int64)req->lease.fencing_token);
    lc_log_trace(client->logger, "client.update.start", fields, 4U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  bridge.source = (lc_source *)json;
  engine_req.namespace_name = req->lease.namespace_name;
  engine_req.key = req->lease.key;
  engine_req.lease_id = req->lease.lease_id;
  engine_req.txn_id = req->lease.txn_id;
  engine_req.fencing_token = req->lease.fencing_token;
  engine_req.if_state_etag = req->if_state_etag;
  engine_req.if_version = req->if_version;
  engine_req.has_if_version = req->has_if_version;
  engine_req.content_type =
      req->content_type != NULL ? req->content_type : "application/json";
  rc = lc_engine_client_update_from(client->engine, &engine_req,
                                    lc_engine_read_bridge, &bridge, &engine_res,
                                    &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[4];

      fields[0] = lc_log_str_field("key", req->lease.key);
      fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
      fields[2] = lc_log_str_field("txn_id", req->lease.txn_id);
      fields[3] =
          pslog_i64("fencing_token", (pslog_int64)req->lease.fencing_token);
      lc_client_log_operation_error(
          client,
          error != NULL && error->code == LC_ERR_TRANSPORT ? PSLOG_LEVEL_ERROR
                                                           : PSLOG_LEVEL_WARN,
          error != NULL && error->code == LC_ERR_TRANSPORT
              ? "client.update.transport_error"
              : "client.update.error",
          fields, 4U, error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  out->new_version = engine_res.new_version;
  out->new_state_etag = lc_strdup_local(engine_res.new_state_etag);
  out->bytes = engine_res.bytes;
  out->correlation_id = lc_strdup_local(engine_res.correlation_id);
  {
    pslog_field fields[6];

    fields[0] = lc_log_str_field("key", req->lease.key);
    fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
    fields[2] = lc_log_str_field("txn_id", req->lease.txn_id);
    fields[3] = pslog_i64("new_version", (pslog_int64)engine_res.new_version);
    fields[4] = lc_log_str_field("new_etag", engine_res.new_state_etag);
    fields[5] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(client->logger, "client.update.success", fields, 6U);
  }
  lc_engine_update_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_client_mutate_method(lc_client *self, const lc_mutate_op *req,
                            lc_mutate_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_engine_mutate_request engine_req;
  lc_engine_mutate_response engine_res;
  lc_engine_error engine_error;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "mutate requires self, req, and out", NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", req->lease.key);
    fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
    fields[2] = lc_log_str_field("txn_id", req->lease.txn_id);
    fields[3] =
        pslog_i64("fencing_token", (pslog_int64)req->lease.fencing_token);
    lc_log_trace(client->logger, "client.mutate.start", fields, 4U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.namespace_name = req->lease.namespace_name;
  engine_req.key = req->lease.key;
  engine_req.lease_id = req->lease.lease_id;
  engine_req.txn_id = req->lease.txn_id;
  engine_req.fencing_token = req->lease.fencing_token;
  engine_req.if_state_etag = req->if_state_etag;
  engine_req.if_version = req->if_version;
  engine_req.has_if_version = req->has_if_version;
  engine_req.mutations = req->mutations;
  engine_req.mutation_count = req->mutation_count;
  rc = lc_engine_client_mutate(client->engine, &engine_req, &engine_res,
                               &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[4];

      fields[0] = lc_log_str_field("key", req->lease.key);
      fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
      fields[2] = lc_log_str_field("txn_id", req->lease.txn_id);
      fields[3] =
          pslog_i64("fencing_token", (pslog_int64)req->lease.fencing_token);
      lc_client_log_operation_error(
          client,
          error != NULL && error->code == LC_ERR_TRANSPORT ? PSLOG_LEVEL_ERROR
                                                           : PSLOG_LEVEL_WARN,
          error != NULL && error->code == LC_ERR_TRANSPORT
              ? "client.mutate.transport_error"
              : "client.mutate.error",
          fields, 4U, error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  out->new_version = engine_res.new_version;
  out->new_state_etag = lc_strdup_local(engine_res.new_state_etag);
  out->bytes = engine_res.bytes;
  out->correlation_id = lc_strdup_local(engine_res.correlation_id);
  {
    pslog_field fields[6];

    fields[0] = lc_log_str_field("key", req->lease.key);
    fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
    fields[2] = lc_log_str_field("txn_id", req->lease.txn_id);
    fields[3] = pslog_i64("new_version", (pslog_int64)engine_res.new_version);
    fields[4] = lc_log_str_field("new_etag", engine_res.new_state_etag);
    fields[5] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(client->logger, "client.mutate.success", fields, 6U);
  }
  lc_engine_mutate_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_client_metadata_method(lc_client *self, const lc_metadata_op *req,
                              lc_metadata_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_engine_metadata_request engine_req;
  lc_engine_metadata_response engine_res;
  lc_engine_error engine_error;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "metadata requires self, req, and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("key", req->lease.key);
    fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
    fields[2] = lc_log_str_field("txn_id", req->lease.txn_id);
    lc_log_trace(client->logger, "client.metadata.start", fields, 3U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.namespace_name = req->lease.namespace_name;
  engine_req.key = req->lease.key;
  engine_req.lease_id = req->lease.lease_id;
  engine_req.txn_id = req->lease.txn_id;
  engine_req.fencing_token = req->lease.fencing_token;
  engine_req.if_version = req->if_version;
  engine_req.has_if_version = req->has_if_version;
  engine_req.has_query_hidden = req->has_query_hidden;
  engine_req.query_hidden = req->query_hidden;
  rc = lc_engine_client_update_metadata(client->engine, &engine_req,
                                        &engine_res, &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("key", req->lease.key);
      fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
      fields[2] = lc_log_str_field("txn_id", req->lease.txn_id);
      lc_client_log_operation_error(
          client,
          error != NULL && error->code == LC_ERR_TRANSPORT ? PSLOG_LEVEL_ERROR
                                                           : PSLOG_LEVEL_WARN,
          error != NULL && error->code == LC_ERR_TRANSPORT
              ? "client.metadata.transport_error"
              : "client.metadata.error",
          fields, 3U, error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  out->namespace_name = lc_strdup_local(engine_res.namespace_name);
  out->key = lc_strdup_local(engine_res.key);
  out->version = engine_res.version;
  out->has_query_hidden = engine_res.has_query_hidden;
  out->query_hidden = engine_res.query_hidden;
  out->correlation_id = lc_strdup_local(engine_res.correlation_id);
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", req->lease.key);
    fields[1] = pslog_i64("version", (pslog_int64)engine_res.version);
    fields[2] = lc_log_bool_field("query_hidden", engine_res.query_hidden);
    fields[3] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(client->logger, "client.metadata.success", fields, 4U);
  }
  lc_engine_metadata_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_client_remove_method(lc_client *self, const lc_remove_op *req,
                            lc_remove_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_engine_remove_request engine_req;
  lc_engine_remove_response engine_res;
  lc_engine_error engine_error;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "remove requires self, req, and out", NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", req->lease.key);
    fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
    fields[2] = lc_log_str_field("txn_id", req->lease.txn_id);
    fields[3] =
        pslog_i64("fencing_token", (pslog_int64)req->lease.fencing_token);
    lc_log_trace(client->logger, "client.remove.start", fields, 4U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.namespace_name = req->lease.namespace_name;
  engine_req.key = req->lease.key;
  engine_req.lease_id = req->lease.lease_id;
  engine_req.txn_id = req->lease.txn_id;
  engine_req.fencing_token = req->lease.fencing_token;
  engine_req.if_state_etag = req->if_state_etag;
  engine_req.if_version = req->if_version;
  engine_req.has_if_version = req->has_if_version;
  rc = lc_engine_client_remove(client->engine, &engine_req, &engine_res,
                               &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[4];

      fields[0] = lc_log_str_field("key", req->lease.key);
      fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
      fields[2] = lc_log_str_field("txn_id", req->lease.txn_id);
      fields[3] =
          pslog_i64("fencing_token", (pslog_int64)req->lease.fencing_token);
      lc_client_log_operation_error(
          client,
          error != NULL && error->code == LC_ERR_TRANSPORT ? PSLOG_LEVEL_ERROR
                                                           : PSLOG_LEVEL_WARN,
          error != NULL && error->code == LC_ERR_TRANSPORT
              ? "client.remove.transport_error"
              : "client.remove.error",
          fields, 4U, error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  out->removed = engine_res.removed;
  out->new_version = engine_res.new_version;
  out->correlation_id = lc_strdup_local(engine_res.correlation_id);
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("key", req->lease.key);
    fields[1] = lc_log_bool_field("removed", engine_res.removed);
    fields[2] = pslog_i64("new_version", (pslog_int64)engine_res.new_version);
    fields[3] = lc_log_str_field("lease_id", req->lease.lease_id);
    fields[4] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(client->logger, "client.remove.success", fields, 5U);
  }
  lc_engine_remove_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_client_keepalive_method(lc_client *self, const lc_keepalive_op *req,
                               lc_keepalive_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_engine_keepalive_request engine_req;
  lc_engine_keepalive_response engine_res;
  lc_engine_error engine_error;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "keepalive requires self, req, and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", req->lease.key);
    fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
    fields[2] = pslog_i64("ttl_seconds", (pslog_int64)req->ttl_seconds);
    fields[3] =
        pslog_i64("fencing_token", (pslog_int64)req->lease.fencing_token);
    lc_log_trace(client->logger, "client.keepalive.start", fields, 4U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.namespace_name = req->lease.namespace_name;
  engine_req.key = req->lease.key;
  engine_req.lease_id = req->lease.lease_id;
  engine_req.txn_id = req->lease.txn_id;
  engine_req.fencing_token = req->lease.fencing_token;
  engine_req.ttl_seconds = req->ttl_seconds;
  rc = lc_engine_client_keepalive(client->engine, &engine_req, &engine_res,
                                  &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("key", req->lease.key);
      fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
      fields[2] =
          pslog_i64("fencing_token", (pslog_int64)req->lease.fencing_token);
      lc_client_log_operation_error(client, PSLOG_LEVEL_ERROR,
                                    "client.keepalive.error", fields, 3U,
                                    error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  out->lease_expires_at_unix = engine_res.lease_expires_at_unix;
  out->version = engine_res.version;
  out->state_etag = lc_strdup_local(engine_res.state_etag);
  out->correlation_id = lc_strdup_local(engine_res.correlation_id);
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", req->lease.key);
    fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
    fields[2] =
        pslog_i64("expires_at", (pslog_int64)engine_res.lease_expires_at_unix);
    fields[3] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(client->logger, "client.keepalive.success", fields, 4U);
  }
  lc_engine_keepalive_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_client_release_method(lc_client *self, const lc_release_op *req,
                             lc_release_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_engine_release_request engine_req;
  lc_engine_release_response engine_res;
  lc_engine_error engine_error;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "release requires self, req, and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("key", req->lease.key);
    fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
    fields[2] = lc_log_str_field("txn_id", req->lease.txn_id);
    lc_log_trace(client->logger, "client.release.start", fields, 3U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.namespace_name = req->lease.namespace_name;
  engine_req.key = req->lease.key;
  engine_req.lease_id = req->lease.lease_id;
  engine_req.txn_id = req->lease.txn_id;
  engine_req.fencing_token = req->lease.fencing_token;
  engine_req.rollback = req->rollback;
  rc = lc_engine_client_release(client->engine, &engine_req, &engine_res,
                                &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("key", req->lease.key);
      fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
      fields[2] = lc_log_str_field("txn_id", req->lease.txn_id);
      lc_client_log_operation_error(client, PSLOG_LEVEL_ERROR,
                                    "client.release.error", fields, 3U, error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  out->released = engine_res.released;
  out->correlation_id = lc_strdup_local(engine_res.correlation_id);
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", req->lease.key);
    fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
    fields[2] = lc_log_bool_field("released", engine_res.released);
    fields[3] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(client->logger, "client.release.success", fields, 4U);
  }
  lc_engine_release_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_client_attach_method(lc_client *self, const lc_attach_op *req,
                            lc_source *src, lc_attach_res *out,
                            lc_error *error) {
  lc_client_handle *client;
  lc_engine_attach_request engine_req;
  lc_engine_attach_response engine_res;
  lc_engine_error engine_error;
  lc_read_bridge bridge;
  int rc;

  if (self == NULL || req == NULL || src == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "attach requires self, req, src, and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", req->lease.key);
    fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
    fields[2] = lc_log_str_field("name", req->name);
    fields[3] = lc_log_str_field("content_type", req->content_type);
    lc_log_trace(client->logger, "client.attachment.attach.start", fields, 4U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  bridge.source = src;
  engine_req.namespace_name = req->lease.namespace_name;
  engine_req.key = req->lease.key;
  engine_req.lease_id = req->lease.lease_id;
  engine_req.txn_id = req->lease.txn_id;
  engine_req.fencing_token = req->lease.fencing_token;
  engine_req.name = req->name;
  engine_req.content_type = req->content_type;
  engine_req.max_bytes = req->max_bytes;
  engine_req.has_max_bytes = req->has_max_bytes;
  engine_req.prevent_overwrite = req->prevent_overwrite;
  rc = lc_engine_client_attach_from(client->engine, &engine_req,
                                    lc_engine_read_bridge, &bridge, &engine_res,
                                    &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("key", req->lease.key);
      fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
      fields[2] = lc_log_str_field("name", req->name);
      lc_client_log_operation_error(client, PSLOG_LEVEL_WARN,
                                    "client.attachment.attach.error", fields,
                                    3U, error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  lc_attachment_info_copy(&out->attachment, &engine_res.attachment);
  out->noop = engine_res.noop;
  out->version = engine_res.version;
  out->correlation_id = lc_strdup_local(engine_res.correlation_id);
  {
    pslog_field fields[6];

    fields[0] = lc_log_str_field("key", req->lease.key);
    fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
    fields[2] = lc_log_str_field("name", engine_res.attachment.name);
    fields[3] = lc_log_bool_field("noop", engine_res.noop);
    fields[4] = pslog_i64("version", (pslog_int64)engine_res.version);
    fields[5] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(client->logger, "client.attachment.attach.success", fields,
                 6U);
  }
  lc_engine_attach_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_client_list_attachments_method(lc_client *self,
                                      const lc_attachment_list_req *req,
                                      lc_attachment_list *out,
                                      lc_error *error) {
  lc_client_handle *client;
  lc_engine_list_attachments_request engine_req;
  lc_engine_list_attachments_response engine_res;
  lc_engine_error engine_error;
  size_t index;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "list_attachments requires self, req, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("key", req->lease.key);
    fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
    fields[2] = lc_log_bool_field("public", req->public_read);
    lc_log_trace(client->logger, "client.attachment.list.start", fields, 3U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.namespace_name = req->lease.namespace_name;
  engine_req.key = req->lease.key;
  engine_req.lease_id = req->lease.lease_id;
  engine_req.txn_id = req->lease.txn_id;
  engine_req.fencing_token = req->lease.fencing_token;
  engine_req.public_read = req->public_read;
  rc = lc_engine_client_list_attachments(client->engine, &engine_req,
                                         &engine_res, &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("key", req->lease.key);
      fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
      fields[2] = lc_log_bool_field("public", req->public_read);
      lc_client_log_operation_error(client, PSLOG_LEVEL_WARN,
                                    "client.attachment.list.error", fields, 3U,
                                    error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  out->count = engine_res.attachment_count;
  if (out->count > 0U) {
    out->items =
        (lc_attachment_info *)calloc(out->count, sizeof(lc_attachment_info));
    if (out->items == NULL) {
      lc_engine_list_attachments_response_cleanup(&engine_res);
      lc_engine_error_cleanup(&engine_error);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate attachment list", NULL, NULL,
                          NULL);
    }
    for (index = 0U; index < out->count; ++index) {
      lc_attachment_info_copy(&out->items[index],
                              &engine_res.attachments[index]);
    }
  }
  out->correlation_id = lc_strdup_local(engine_res.correlation_id);
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", req->lease.key);
    fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
    fields[2] = lc_log_u64_field("count", out->count);
    fields[3] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(client->logger, "client.attachment.list.success", fields, 4U);
  }
  lc_engine_list_attachments_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_client_get_attachment_method(lc_client *self,
                                    const lc_attachment_get_op *req,
                                    lc_sink *dst, lc_attachment_get_res *out,
                                    lc_error *error) {
  lc_client_handle *client;
  lc_engine_get_attachment_request engine_req;
  lc_engine_get_attachment_response engine_res;
  lc_engine_error engine_error;
  lc_write_bridge bridge;
  int rc;

  if (self == NULL || req == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "get_attachment requires self, req, dst, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", req->lease.key);
    fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
    fields[2] = lc_log_str_field("attachment_id", req->selector.id);
    fields[3] = lc_log_str_field("name", req->selector.name);
    lc_log_trace(client->logger, "client.attachment.get.start", fields, 4U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  bridge.sink = dst;
  engine_req.namespace_name = req->lease.namespace_name;
  engine_req.key = req->lease.key;
  engine_req.lease_id = req->lease.lease_id;
  engine_req.txn_id = req->lease.txn_id;
  engine_req.fencing_token = req->lease.fencing_token;
  engine_req.public_read = req->public_read;
  engine_req.selector.id = req->selector.id;
  engine_req.selector.name = req->selector.name;
  rc = lc_engine_client_get_attachment_into(client->engine, &engine_req,
                                            lc_engine_write_bridge, &bridge,
                                            &engine_res, &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[4];

      fields[0] = lc_log_str_field("key", req->lease.key);
      fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
      fields[2] = lc_log_str_field("attachment_id", req->selector.id);
      fields[3] = lc_log_str_field("name", req->selector.name);
      lc_client_log_operation_error(client, PSLOG_LEVEL_WARN,
                                    "client.attachment.get.error", fields, 4U,
                                    error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  lc_attachment_info_copy(&out->attachment, &engine_res.attachment);
  out->correlation_id = lc_strdup_local(engine_res.correlation_id);
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("key", req->lease.key);
    fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
    fields[2] = lc_log_str_field("attachment_id", engine_res.attachment.id);
    fields[3] = lc_log_str_field("name", engine_res.attachment.name);
    fields[4] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(client->logger, "client.attachment.get.success", fields, 5U);
  }
  lc_engine_get_attachment_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_client_delete_attachment_method(lc_client *self,
                                       const lc_attachment_delete_op *req,
                                       int *deleted, lc_error *error) {
  lc_client_handle *client;
  lc_engine_delete_attachment_request engine_req;
  lc_engine_delete_attachment_response engine_res;
  lc_engine_error engine_error;
  int rc;

  if (self == NULL || req == NULL || deleted == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "delete_attachment requires self, req, and deleted",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", req->lease.key);
    fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
    fields[2] = lc_log_str_field("attachment_id", req->selector.id);
    fields[3] = lc_log_str_field("name", req->selector.name);
    lc_log_trace(client->logger, "client.attachment.delete.start", fields, 4U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.namespace_name = req->lease.namespace_name;
  engine_req.key = req->lease.key;
  engine_req.lease_id = req->lease.lease_id;
  engine_req.txn_id = req->lease.txn_id;
  engine_req.fencing_token = req->lease.fencing_token;
  engine_req.selector.id = req->selector.id;
  engine_req.selector.name = req->selector.name;
  rc = lc_engine_client_delete_attachment(client->engine, &engine_req,
                                          &engine_res, &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[4];

      fields[0] = lc_log_str_field("key", req->lease.key);
      fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
      fields[2] = lc_log_str_field("attachment_id", req->selector.id);
      fields[3] = lc_log_str_field("name", req->selector.name);
      lc_client_log_operation_error(client, PSLOG_LEVEL_WARN,
                                    "client.attachment.delete.error", fields,
                                    4U, error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  *deleted = engine_res.deleted;
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("key", req->lease.key);
    fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
    fields[2] = lc_log_str_field("attachment_id", req->selector.id);
    fields[3] = lc_log_bool_field("deleted", engine_res.deleted);
    fields[4] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(client->logger, "client.attachment.delete.success", fields,
                 5U);
  }
  lc_engine_delete_attachment_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_client_delete_all_attachments_method(
    lc_client *self, const lc_attachment_delete_all_op *req, int *deleted_count,
    lc_error *error) {
  lc_client_handle *client;
  lc_engine_delete_all_attachments_request engine_req;
  lc_engine_delete_all_attachments_response engine_res;
  lc_engine_error engine_error;
  int rc;

  if (self == NULL || req == NULL || deleted_count == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "delete_all_attachments requires self, req, and deleted_count", NULL,
        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[2];

    fields[0] = lc_log_str_field("key", req->lease.key);
    fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
    lc_log_trace(client->logger, "client.attachment.delete_all.start", fields,
                 2U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.namespace_name = req->lease.namespace_name;
  engine_req.key = req->lease.key;
  engine_req.lease_id = req->lease.lease_id;
  engine_req.txn_id = req->lease.txn_id;
  engine_req.fencing_token = req->lease.fencing_token;
  rc = lc_engine_client_delete_all_attachments(client->engine, &engine_req,
                                               &engine_res, &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("key", req->lease.key);
      fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
      lc_client_log_operation_error(client, PSLOG_LEVEL_WARN,
                                    "client.attachment.delete_all.error",
                                    fields, 2U, error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  *deleted_count = engine_res.deleted;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", req->lease.key);
    fields[1] = lc_log_str_field("lease_id", req->lease.lease_id);
    fields[2] = pslog_i64("deleted", (pslog_int64)engine_res.deleted);
    fields[3] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(client->logger, "client.attachment.delete_all.success", fields,
                 4U);
  }
  lc_engine_delete_all_attachments_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_client_queue_stats_method(lc_client *self, const lc_queue_stats_req *req,
                                 lc_queue_stats_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_engine_queue_stats_request engine_req;
  lc_engine_queue_stats_response engine_res;
  lc_engine_error engine_error;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "queue_stats requires self, req, and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[2];

    fields[0] = lc_log_str_field("queue", req->queue);
    fields[1] = lc_log_str_field("namespace", req->namespace_name);
    lc_log_trace(client->logger, "client.queue.stats.start", fields, 2U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.namespace_name = req->namespace_name;
  engine_req.queue = req->queue;
  rc = lc_engine_client_queue_stats(client->engine, &engine_req, &engine_res,
                                    &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("queue", req->queue);
      fields[1] = lc_log_str_field("namespace", req->namespace_name);
      lc_client_log_operation_error(client, PSLOG_LEVEL_WARN,
                                    "client.queue.stats.error", fields, 2U,
                                    error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  out->namespace_name = lc_strdup_local(engine_res.namespace_name);
  out->queue = lc_strdup_local(engine_res.queue);
  out->waiting_consumers = engine_res.waiting_consumers;
  out->pending_candidates = engine_res.pending_candidates;
  out->total_consumers = engine_res.total_consumers;
  out->has_active_watcher = engine_res.has_active_watcher;
  out->available = engine_res.available;
  out->head_message_id = lc_strdup_local(engine_res.head_message_id);
  out->head_enqueued_at_unix = engine_res.head_enqueued_at_unix;
  out->head_not_visible_until_unix = engine_res.head_not_visible_until_unix;
  out->head_age_seconds = engine_res.head_age_seconds;
  out->correlation_id = lc_strdup_local(engine_res.correlation_id);
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("queue", req->queue);
    fields[1] = lc_log_str_field("namespace", req->namespace_name);
    fields[2] = pslog_i64("available", (pslog_int64)engine_res.available);
    fields[3] = lc_log_str_field("head_message_id", engine_res.head_message_id);
    fields[4] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(client->logger, "client.queue.stats.success", fields, 5U);
  }
  lc_engine_queue_stats_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_client_queue_ack_method(lc_client *self, const lc_ack_op *req,
                               lc_ack_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_engine_queue_ack_request engine_req;
  lc_engine_queue_ack_response engine_res;
  lc_engine_error engine_error;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "queue_ack requires self, req, and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("queue", req->message.queue);
    fields[1] = lc_log_str_field("namespace", req->message.namespace_name);
    fields[2] = lc_log_str_field("message_id", req->message.message_id);
    fields[3] = lc_log_str_field("lease_id", req->message.lease_id);
    fields[4] = lc_log_str_field("txn_id", req->message.txn_id);
    lc_log_trace(client->logger, "client.queue.ack.start", fields, 5U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.namespace_name = req->message.namespace_name;
  engine_req.queue = req->message.queue;
  engine_req.message_id = req->message.message_id;
  engine_req.lease_id = req->message.lease_id;
  engine_req.txn_id = req->message.txn_id;
  engine_req.fencing_token = req->message.fencing_token;
  engine_req.meta_etag = req->message.meta_etag;
  engine_req.state_etag = req->message.state_etag;
  engine_req.state_lease_id = req->message.state_lease_id;
  engine_req.state_fencing_token = req->message.state_fencing_token;
  rc = lc_engine_client_queue_ack(client->engine, &engine_req, &engine_res,
                                  &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[5];

      fields[0] = lc_log_str_field("queue", req->message.queue);
      fields[1] = lc_log_str_field("namespace", req->message.namespace_name);
      fields[2] = lc_log_str_field("message_id", req->message.message_id);
      fields[3] = lc_log_str_field("lease_id", req->message.lease_id);
      fields[4] = lc_log_str_field("txn_id", req->message.txn_id);
      lc_client_log_operation_error(client, PSLOG_LEVEL_WARN,
                                    "client.queue.ack.error", fields, 5U,
                                    error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  out->acked = engine_res.acked;
  out->correlation_id = lc_strdup_local(engine_res.correlation_id);
  {
    pslog_field fields[6];

    fields[0] = lc_log_str_field("queue", req->message.queue);
    fields[1] = lc_log_str_field("namespace", req->message.namespace_name);
    fields[2] = lc_log_str_field("message_id", req->message.message_id);
    fields[3] = lc_log_str_field("lease_id", req->message.lease_id);
    fields[4] = lc_log_bool_field("acked", engine_res.acked);
    fields[5] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(client->logger, "client.queue.ack.success", fields, 6U);
  }
  lc_engine_queue_ack_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_client_queue_nack_method(lc_client *self, const lc_nack_op *req,
                                lc_nack_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_engine_queue_nack_request engine_req;
  lc_engine_queue_nack_response engine_res;
  lc_engine_error engine_error;
  const char *wire_intent;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "queue_nack requires self, req, and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  wire_intent = NULL;
  rc = lc_nack_intent_to_wire_string(req->intent, &wire_intent, error);
  if (rc != LC_OK) {
    return rc;
  }
  {
    pslog_field fields[6];

    fields[0] = lc_log_str_field("queue", req->message.queue);
    fields[1] = lc_log_str_field("namespace", req->message.namespace_name);
    fields[2] = lc_log_str_field("message_id", req->message.message_id);
    fields[3] = lc_log_str_field("lease_id", req->message.lease_id);
    fields[4] = pslog_i64("delay_seconds", (pslog_int64)req->delay_seconds);
    fields[5] =
        lc_log_str_field("intent", lc_nack_intent_to_string(req->intent));
    lc_log_trace(client->logger, "client.queue.nack.start", fields, 6U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.namespace_name = req->message.namespace_name;
  engine_req.queue = req->message.queue;
  engine_req.message_id = req->message.message_id;
  engine_req.lease_id = req->message.lease_id;
  engine_req.txn_id = req->message.txn_id;
  engine_req.fencing_token = req->message.fencing_token;
  engine_req.meta_etag = req->message.meta_etag;
  engine_req.state_etag = req->message.state_etag;
  engine_req.state_lease_id = req->message.state_lease_id;
  engine_req.state_fencing_token = req->message.state_fencing_token;
  engine_req.delay_seconds = req->delay_seconds;
  engine_req.intent = wire_intent;
  engine_req.last_error_json = req->last_error_json;
  rc = lc_engine_client_queue_nack(client->engine, &engine_req, &engine_res,
                                   &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[6];

      fields[0] = lc_log_str_field("queue", req->message.queue);
      fields[1] = lc_log_str_field("namespace", req->message.namespace_name);
      fields[2] = lc_log_str_field("message_id", req->message.message_id);
      fields[3] = lc_log_str_field("lease_id", req->message.lease_id);
      fields[4] = pslog_i64("delay_seconds", (pslog_int64)req->delay_seconds);
      fields[5] =
          lc_log_str_field("intent", lc_nack_intent_to_string(req->intent));
      lc_client_log_operation_error(client, PSLOG_LEVEL_WARN,
                                    "client.queue.nack.error", fields, 6U,
                                    error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  out->requeued = engine_res.requeued;
  out->meta_etag = lc_strdup_local(engine_res.meta_etag);
  out->correlation_id = lc_strdup_local(engine_res.correlation_id);
  {
    pslog_field fields[7];

    fields[0] = lc_log_str_field("queue", req->message.queue);
    fields[1] = lc_log_str_field("namespace", req->message.namespace_name);
    fields[2] = lc_log_str_field("message_id", req->message.message_id);
    fields[3] = lc_log_str_field("lease_id", req->message.lease_id);
    fields[4] = lc_log_bool_field("requeued", engine_res.requeued);
    fields[5] = lc_log_str_field("meta_etag", engine_res.meta_etag);
    fields[6] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(client->logger, "client.queue.nack.success", fields, 7U);
  }
  lc_engine_queue_nack_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_client_queue_extend_method(lc_client *self, const lc_extend_op *req,
                                  lc_extend_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_engine_queue_extend_request engine_req;
  lc_engine_queue_extend_response engine_res;
  lc_engine_error engine_error;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "queue_extend requires self, req, and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("queue", req->message.queue);
    fields[1] = lc_log_str_field("namespace", req->message.namespace_name);
    fields[2] = lc_log_str_field("message_id", req->message.message_id);
    fields[3] = lc_log_str_field("lease_id", req->message.lease_id);
    fields[4] =
        pslog_i64("extend_by_seconds", (pslog_int64)req->extend_by_seconds);
    lc_log_trace(client->logger, "client.queue.extend.start", fields, 5U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.namespace_name = req->message.namespace_name;
  engine_req.queue = req->message.queue;
  engine_req.message_id = req->message.message_id;
  engine_req.lease_id = req->message.lease_id;
  engine_req.txn_id = req->message.txn_id;
  engine_req.fencing_token = req->message.fencing_token;
  engine_req.meta_etag = req->message.meta_etag;
  engine_req.state_lease_id = req->message.state_lease_id;
  engine_req.state_fencing_token = req->message.state_fencing_token;
  engine_req.extend_by_seconds = req->extend_by_seconds;
  rc = lc_engine_client_queue_extend(client->engine, &engine_req, &engine_res,
                                     &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[5];

      fields[0] = lc_log_str_field("queue", req->message.queue);
      fields[1] = lc_log_str_field("namespace", req->message.namespace_name);
      fields[2] = lc_log_str_field("message_id", req->message.message_id);
      fields[3] = lc_log_str_field("lease_id", req->message.lease_id);
      fields[4] =
          pslog_i64("extend_by_seconds", (pslog_int64)req->extend_by_seconds);
      lc_client_log_operation_error(client, PSLOG_LEVEL_WARN,
                                    "client.queue.extend.error", fields, 5U,
                                    error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  out->lease_expires_at_unix = engine_res.lease_expires_at_unix;
  out->visibility_timeout_seconds = engine_res.visibility_timeout_seconds;
  out->meta_etag = lc_strdup_local(engine_res.meta_etag);
  out->state_lease_expires_at_unix = engine_res.state_lease_expires_at_unix;
  out->correlation_id = lc_strdup_local(engine_res.correlation_id);
  {
    pslog_field fields[7];

    fields[0] = lc_log_str_field("queue", req->message.queue);
    fields[1] = lc_log_str_field("namespace", req->message.namespace_name);
    fields[2] = lc_log_str_field("message_id", req->message.message_id);
    fields[3] = lc_log_str_field("lease_id", req->message.lease_id);
    fields[4] = pslog_i64("lease_expires_at",
                          (pslog_int64)engine_res.lease_expires_at_unix);
    fields[5] = pslog_i64("visibility_timeout_seconds",
                          (pslog_int64)engine_res.visibility_timeout_seconds);
    fields[6] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(client->logger, "client.queue.extend.success", fields, 7U);
  }
  lc_engine_queue_extend_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_client_query_method(lc_client *self, const lc_query_req *req,
                           lc_sink *dst, lc_query_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_engine_query_request engine_req;
  lc_engine_query_stream_response engine_res;
  lc_engine_error engine_error;
  lc_write_bridge bridge;
  int rc;

  if (self == NULL || req == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "query requires self, req, dst, and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("namespace", req->namespace_name);
    fields[1] = lc_log_str_field("return_mode", req->return_mode);
    fields[2] = lc_log_i64_field("limit", req->limit);
    fields[3] = lc_log_str_field("cursor", req->cursor);
    lc_log_trace(client->logger, "client.query.start", fields, 4U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  bridge.sink = dst;
  engine_req.namespace_name = req->namespace_name;
  engine_req.selector_json = req->selector_json;
  engine_req.limit = req->limit;
  engine_req.cursor = req->cursor;
  engine_req.fields_json = req->fields_json;
  engine_req.return_mode = req->return_mode;
  rc = lc_engine_client_query_into(client->engine, &engine_req,
                                   lc_engine_write_bridge, &bridge, &engine_res,
                                   &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[4];

      fields[0] = lc_log_str_field("namespace", req->namespace_name);
      fields[1] = lc_log_str_field("return_mode", req->return_mode);
      fields[2] = lc_log_i64_field("limit", req->limit);
      fields[3] = lc_log_str_field("cursor", req->cursor);
      lc_client_log_operation_error(
          client,
          error != NULL && error->code == LC_ERR_TRANSPORT ? PSLOG_LEVEL_ERROR
                                                           : PSLOG_LEVEL_WARN,
          error != NULL && error->code == LC_ERR_TRANSPORT
              ? "client.query.transport_error"
              : "client.query.error",
          fields, 4U, error);
    }
    lc_engine_query_stream_response_cleanup(client->engine, &engine_res);
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  out->cursor = lc_strdup_local(engine_res.cursor);
  out->return_mode = lc_strdup_local(engine_res.return_mode);
  out->index_seq = engine_res.index_seq;
  out->correlation_id = lc_strdup_local(engine_res.correlation_id);
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("namespace", req->namespace_name);
    fields[1] = lc_log_str_field("return_mode", out->return_mode);
    fields[2] = lc_log_str_field("cursor", out->cursor);
    fields[3] = pslog_i64("index_seq", (pslog_int64)out->index_seq);
    fields[4] = lc_log_str_field("cid", out->correlation_id);
    lc_log_trace(client->logger, "client.query.success", fields, 5U);
  }
  lc_engine_query_stream_response_cleanup(client->engine, &engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_client_enqueue_method(lc_client *self, const lc_enqueue_req *req,
                             lc_source *src, lc_enqueue_res *out,
                             lc_error *error) {
  lc_client_handle *client;
  lc_engine_enqueue_request engine_req;
  lc_engine_enqueue_response engine_res;
  lc_engine_error engine_error;
  lc_read_bridge bridge;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "enqueue requires self, req, and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[2];

    fields[0] = lc_log_str_field("queue", req->queue);
    fields[1] = lc_log_str_field("namespace", req->namespace_name);
    lc_log_info(client->logger, "client.queue.enqueue.begin", fields, 2U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  bridge.source = src;
  engine_req.namespace_name = req->namespace_name;
  engine_req.queue = req->queue;
  engine_req.delay_seconds = req->delay_seconds;
  engine_req.visibility_timeout_seconds = req->visibility_timeout_seconds;
  engine_req.ttl_seconds = req->ttl_seconds;
  engine_req.max_attempts = req->max_attempts;
  engine_req.payload_content_type = req->content_type;
  rc = lc_engine_client_enqueue_from(client->engine, &engine_req,
                                     lc_engine_read_bridge, &bridge,
                                     &engine_res, &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("queue", req->queue);
      fields[1] = lc_log_str_field("namespace", req->namespace_name);
      lc_client_log_operation_error(
          client,
          error != NULL && error->code == LC_ERR_TRANSPORT ? PSLOG_LEVEL_ERROR
                                                           : PSLOG_LEVEL_WARN,
          error != NULL && error->code == LC_ERR_TRANSPORT
              ? "client.queue.enqueue.transport_error"
              : "client.queue.enqueue.error",
          fields, 2U, error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  out->namespace_name = lc_strdup_local(engine_res.namespace_name);
  out->queue = lc_strdup_local(engine_res.queue);
  out->message_id = lc_strdup_local(engine_res.message_id);
  out->attempts = engine_res.attempts;
  out->max_attempts = engine_res.max_attempts;
  out->failure_attempts = engine_res.failure_attempts;
  out->not_visible_until_unix = engine_res.not_visible_until_unix;
  out->visibility_timeout_seconds = engine_res.visibility_timeout_seconds;
  out->payload_bytes = engine_res.payload_bytes;
  out->correlation_id = lc_strdup_local(engine_res.correlation_id);
  {
    pslog_field fields[6];

    fields[0] = lc_log_str_field("queue", req->queue);
    fields[1] = lc_log_str_field("namespace", req->namespace_name);
    fields[2] = lc_log_str_field("message_id", engine_res.message_id);
    fields[3] =
        pslog_i64("payload_bytes", (pslog_int64)engine_res.payload_bytes);
    fields[4] = pslog_i64("visibility_timeout_seconds",
                          (pslog_int64)engine_res.visibility_timeout_seconds);
    fields[5] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_info(client->logger, "client.queue.enqueue.success", fields, 6U);
  }
  lc_engine_enqueue_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

static void lc_delivery_meta_copy(lc_engine_dequeue_response *dst,
                                  const lc_engine_dequeue_response *src) {
  memset(dst, 0, sizeof(*dst));
  dst->namespace_name = lc_strdup_local(src->namespace_name);
  dst->queue = lc_strdup_local(src->queue);
  dst->message_id = lc_strdup_local(src->message_id);
  dst->attempts = src->attempts;
  dst->max_attempts = src->max_attempts;
  dst->failure_attempts = src->failure_attempts;
  dst->not_visible_until_unix = src->not_visible_until_unix;
  dst->visibility_timeout_seconds = src->visibility_timeout_seconds;
  dst->payload_content_type = lc_strdup_local(src->payload_content_type);
  dst->correlation_id = lc_strdup_local(src->correlation_id);
  dst->lease_id = lc_strdup_local(src->lease_id);
  dst->lease_expires_at_unix = src->lease_expires_at_unix;
  dst->fencing_token = src->fencing_token;
  dst->txn_id = lc_strdup_local(src->txn_id);
  dst->meta_etag = lc_strdup_local(src->meta_etag);
  dst->state_etag = lc_strdup_local(src->state_etag);
  dst->state_lease_id = lc_strdup_local(src->state_lease_id);
  dst->state_lease_expires_at_unix = src->state_lease_expires_at_unix;
  dst->state_fencing_token = src->state_fencing_token;
  dst->state_txn_id = lc_strdup_local(src->state_txn_id);
  dst->next_cursor = lc_strdup_local(src->next_cursor);
}

static int lc_dequeue_begin(void *context,
                            const lc_engine_dequeue_response *delivery,
                            lc_engine_error *error) {
  lc_single_delivery_bridge *bridge;
  int rc;

  (void)error;
  bridge = (lc_single_delivery_bridge *)context;
  lc_delivery_meta_copy(&bridge->meta, delivery);
  rc = lc_stream_pipe_open(65536U, &bridge->client->allocator, &bridge->payload,
                           &bridge->pipe, bridge->error);
  return rc == LC_OK;
}

static int lc_dequeue_chunk(void *context, const void *bytes, size_t count,
                            lc_engine_error *error) {
  lc_single_delivery_bridge *bridge;
  int rc;

  (void)error;
  bridge = (lc_single_delivery_bridge *)context;
  if (bridge->pipe == NULL) {
    lc_error_set(bridge->error, LC_ERR_INVALID, 0L,
                 "missing dequeue payload stream", NULL, NULL, NULL);
    return 0;
  }
  if (count == 0U) {
    return 1;
  }
  rc = lc_stream_pipe_write(bridge->pipe, bytes, count, bridge->error);
  return rc == LC_OK;
}

static int lc_dequeue_end(void *context,
                          const lc_engine_dequeue_response *delivery,
                          lc_engine_error *error) {
  lc_single_delivery_bridge *bridge;
  lc_source *payload;
  lc_message *message;
  lc_message **next;
  size_t next_count;

  (void)delivery;
  (void)error;
  bridge = (lc_single_delivery_bridge *)context;
  if (bridge->pipe == NULL || bridge->payload == NULL) {
    lc_error_set(bridge->error, LC_ERR_INVALID, 0L,
                 "missing dequeue payload stream", NULL, NULL, NULL);
    return 0;
  }
  lc_stream_pipe_finish(bridge->pipe);
  bridge->pipe = NULL;
  payload = bridge->payload;
  bridge->payload = NULL;
  message = lc_message_new(bridge->client, &bridge->meta, payload, NULL);
  lc_engine_dequeue_response_cleanup(&bridge->meta);
  if (message == NULL) {
    if (payload != NULL) {
      payload->close(payload);
    }
    return 0;
  }
  if (bridge->mode_batch) {
    next_count = bridge->batch->count + 1U;
    next = (lc_message **)realloc(bridge->batch->messages,
                                  next_count * sizeof(*next));
    if (next == NULL) {
      message->close(message);
      lc_error_set(bridge->error, LC_ERR_NOMEM, 0L,
                   "failed to grow dequeue batch", NULL, NULL, NULL);
      return 0;
    }
    bridge->batch->messages = next;
    bridge->batch->messages[bridge->batch->count] = message;
    bridge->batch->count = next_count;
  } else {
    *bridge->out = message;
  }
  return 1;
}

static int lc_client_dequeue_common(lc_client *self, const lc_dequeue_req *req,
                                    lc_message **out, lc_error *error,
                                    int with_state) {
  lc_client_handle *client;
  lc_engine_dequeue_request engine_req;
  lc_engine_queue_stream_handler handler;
  lc_single_delivery_bridge bridge;
  lc_engine_error engine_error;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "dequeue requires self, req, and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("queue", req->queue);
    fields[1] = lc_log_str_field("namespace", req->namespace_name);
    fields[2] = lc_log_str_field("owner", req->owner);
    fields[3] = lc_log_bool_field("with_state", with_state);
    lc_log_info(client->logger, "client.queue.dequeue.begin", fields, 4U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&handler, 0, sizeof(handler));
  memset(&bridge, 0, sizeof(bridge));
  lc_engine_error_init(&engine_error);
  bridge.client = client;
  bridge.out = out;
  bridge.batch = NULL;
  bridge.error = error;
  *out = NULL;
  engine_req.namespace_name = req->namespace_name;
  engine_req.queue = req->queue;
  engine_req.owner = req->owner;
  engine_req.txn_id = req->txn_id;
  engine_req.visibility_timeout_seconds = req->visibility_timeout_seconds;
  engine_req.wait_seconds = req->wait_seconds;
  engine_req.page_size = 1;
  engine_req.start_after = req->start_after;
  handler.begin = lc_dequeue_begin;
  handler.chunk = lc_dequeue_chunk;
  handler.end = lc_dequeue_end;
  if (with_state) {
    rc = lc_engine_client_dequeue_with_state_into(
        client->engine, &engine_req, &handler, &bridge, &engine_error);
  } else {
    rc = lc_engine_client_dequeue_into(client->engine, &engine_req, &handler,
                                       &bridge, &engine_error);
  }
  if (rc != LC_ENGINE_OK) {
    if (bridge.pipe != NULL) {
      lc_stream_pipe_fail(bridge.pipe, LC_ERR_TRANSPORT,
                          "failed to dequeue payload");
      bridge.pipe = NULL;
    }
    if (bridge.payload != NULL) {
      bridge.payload->close(bridge.payload);
      bridge.payload = NULL;
    }
    lc_engine_dequeue_response_cleanup(&bridge.meta);
    if (error != NULL && error->code != LC_OK) {
      lc_engine_error_cleanup(&engine_error);
      {
        pslog_field fields[4];

        fields[0] = lc_log_str_field("queue", req->queue);
        fields[1] = lc_log_str_field("namespace", req->namespace_name);
        fields[2] = lc_log_str_field("owner", req->owner);
        fields[3] = lc_log_bool_field("with_state", with_state);
        lc_client_log_operation_error(client, PSLOG_LEVEL_WARN,
                                      "client.queue.dequeue.error", fields, 4U,
                                      error);
      }
      return error->code;
    }
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[4];

      fields[0] = lc_log_str_field("queue", req->queue);
      fields[1] = lc_log_str_field("namespace", req->namespace_name);
      fields[2] = lc_log_str_field("owner", req->owner);
      fields[3] = lc_log_bool_field("with_state", with_state);
      lc_client_log_operation_error(client, PSLOG_LEVEL_WARN,
                                    "client.queue.dequeue.error", fields, 4U,
                                    error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  if (*out != NULL) {
    pslog_field fields[6];

    fields[0] = lc_log_str_field("queue", req->queue);
    fields[1] = lc_log_str_field("namespace", req->namespace_name);
    fields[2] = lc_log_str_field("owner", req->owner);
    fields[3] = lc_log_str_field("message_id", (*out)->message_id);
    fields[4] = pslog_i64("fencing_token", (pslog_int64)(*out)->fencing_token);
    fields[5] = lc_log_str_field("cid", (*out)->correlation_id);
    lc_log_info(client->logger, "client.queue.dequeue.success", fields, 6U);
  }
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_client_dequeue_batch_method(lc_client *self, const lc_dequeue_req *req,
                                   lc_dequeue_batch_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_engine_dequeue_request engine_req;
  lc_engine_queue_stream_handler handler;
  lc_single_delivery_bridge bridge;
  lc_engine_error engine_error;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "dequeue_batch requires self, req, and out", NULL, NULL,
                        NULL);
  }
  memset(out, 0, sizeof(*out));
  client = (lc_client_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("queue", req->queue);
    fields[1] = lc_log_str_field("namespace", req->namespace_name);
    fields[2] = lc_log_str_field("owner", req->owner);
    fields[3] = pslog_i64(
        "page_size", (pslog_int64)(req->page_size > 0 ? req->page_size : 1));
    lc_log_info(client->logger, "client.queue.dequeue.begin", fields, 4U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&handler, 0, sizeof(handler));
  memset(&bridge, 0, sizeof(bridge));
  lc_engine_error_init(&engine_error);
  bridge.client = client;
  bridge.batch = out;
  bridge.error = error;
  bridge.mode_batch = 1;
  engine_req.namespace_name = req->namespace_name;
  engine_req.queue = req->queue;
  engine_req.owner = req->owner;
  engine_req.txn_id = req->txn_id;
  engine_req.visibility_timeout_seconds = req->visibility_timeout_seconds;
  engine_req.wait_seconds = req->wait_seconds;
  engine_req.page_size = req->page_size > 0 ? req->page_size : 1;
  engine_req.start_after = req->start_after;
  handler.begin = lc_dequeue_begin;
  handler.chunk = lc_dequeue_chunk;
  handler.end = lc_dequeue_end;
  rc = lc_engine_client_dequeue_into(client->engine, &engine_req, &handler,
                                     &bridge, &engine_error);
  if (rc != LC_ENGINE_OK) {
    if (bridge.pipe != NULL) {
      lc_stream_pipe_fail(bridge.pipe, LC_ERR_TRANSPORT,
                          "failed to dequeue payload");
      bridge.pipe = NULL;
    }
    if (bridge.payload != NULL) {
      bridge.payload->close(bridge.payload);
      bridge.payload = NULL;
    }
    lc_engine_dequeue_response_cleanup(&bridge.meta);
    lc_dequeue_batch_cleanup(out);
    if (error != NULL && error->code != LC_OK) {
      lc_engine_error_cleanup(&engine_error);
      {
        pslog_field fields[3];

        fields[0] = lc_log_str_field("queue", req->queue);
        fields[1] = lc_log_str_field("namespace", req->namespace_name);
        fields[2] = lc_log_str_field("owner", req->owner);
        lc_client_log_operation_error(client, PSLOG_LEVEL_WARN,
                                      "client.queue.dequeue.error", fields, 3U,
                                      error);
      }
      return error->code;
    }
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("queue", req->queue);
      fields[1] = lc_log_str_field("namespace", req->namespace_name);
      fields[2] = lc_log_str_field("owner", req->owner);
      lc_client_log_operation_error(client, PSLOG_LEVEL_WARN,
                                    "client.queue.dequeue.error", fields, 3U,
                                    error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("queue", req->queue);
    fields[1] = lc_log_str_field("namespace", req->namespace_name);
    fields[2] = lc_log_str_field("owner", req->owner);
    fields[3] = lc_log_u64_field("count", out->count);
    lc_log_info(client->logger, "client.queue.dequeue.success", fields, 4U);
  }
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_client_dequeue_method(lc_client *self, const lc_dequeue_req *req,
                             lc_message **out, lc_error *error) {
  return lc_client_dequeue_common(self, req, out, error, 0);
}

int lc_client_dequeue_with_state_method(lc_client *self,
                                        const lc_dequeue_req *req,
                                        lc_message **out, lc_error *error) {
  return lc_client_dequeue_common(self, req, out, error, 1);
}

static int lc_subscribe_begin(void *context,
                              const lc_engine_dequeue_response *delivery,
                              lc_engine_error *error) {
  lc_subscribe_bridge *bridge;
  lc_source *payload;
  int rc;

  (void)error;
  bridge = (lc_subscribe_bridge *)context;
  lc_delivery_meta_copy(&bridge->meta, delivery);
  bridge->terminal = 0;
  bridge->handler_rc = LC_OK;
  bridge->message = NULL;
  bridge->handler_thread_started = 0;
  rc = lc_stream_pipe_open(65536U, &bridge->client->allocator, &payload,
                           &bridge->pipe, bridge->error);
  if (rc != LC_OK) {
    return 0;
  }
  bridge->message =
      lc_message_new(bridge->client, &bridge->meta, payload, &bridge->terminal);
  if (bridge->message == NULL) {
    payload->close(payload);
    lc_stream_pipe_fail(bridge->pipe, LC_ERR_NOMEM,
                        "failed to allocate message handle");
    bridge->pipe = NULL;
    return 0;
  }
  rc = pthread_create(&bridge->handler_thread, NULL, lc_subscribe_handler_main,
                      bridge);
  if (rc != 0) {
    bridge->message->close(bridge->message);
    bridge->message = NULL;
    lc_stream_pipe_fail(bridge->pipe, LC_ERR_TRANSPORT,
                        "failed to start subscribe handler thread");
    bridge->pipe = NULL;
    lc_error_set(bridge->error, LC_ERR_TRANSPORT, 0L,
                 "failed to start subscribe handler thread", NULL, NULL, NULL);
    return 0;
  }
  bridge->handler_thread_started = 1;
  return 1;
}

static int lc_subscribe_chunk(void *context, const void *bytes, size_t count,
                              lc_engine_error *error) {
  lc_subscribe_bridge *bridge;
  int rc;

  (void)error;
  bridge = (lc_subscribe_bridge *)context;
  rc = lc_stream_pipe_write(bridge->pipe, bytes, count, bridge->error);
  if (rc != LC_OK) {
    lc_stream_pipe_fail(bridge->pipe, bridge->error->code,
                        bridge->error->message);
    bridge->pipe = NULL;
    return 0;
  }
  return 1;
}

static int lc_subscribe_end(void *context,
                            const lc_engine_dequeue_response *delivery,
                            lc_engine_error *error) {
  lc_subscribe_bridge *bridge;
  int handler_rc;

  (void)delivery;
  (void)error;
  bridge = (lc_subscribe_bridge *)context;
  lc_stream_pipe_finish(bridge->pipe);
  bridge->pipe = NULL;
  pthread_join(bridge->handler_thread, NULL);
  bridge->handler_thread_started = 0;
  lc_engine_dequeue_response_cleanup(&bridge->meta);
  handler_rc = bridge->handler_rc;
  if (bridge->message != NULL) {
    bridge->message->close(bridge->message);
    bridge->message = NULL;
  }
  if (handler_rc != LC_OK) {
    return 0;
  }
  return 1;
}

static void *lc_subscribe_handler_main(void *context) {
  lc_subscribe_bridge *bridge;
  lc_nack_req nack_req;
  lc_error nack_error;
  int rc;

  bridge = (lc_subscribe_bridge *)context;
  rc = bridge->consumer->handle(bridge->consumer->context, bridge->message,
                                bridge->error);
  if (rc == LC_OK && !bridge->terminal) {
    rc = lc_error_set(bridge->error, LC_ERR_TRANSPORT, 0L,
                      "consumer callback must ack() or nack() before returning "
                      "LC_OK",
                      NULL, NULL, NULL);
  } else if (rc != LC_OK && bridge->error->code == LC_OK) {
    rc = lc_error_set(bridge->error, LC_ERR_TRANSPORT, 0L,
                      "consumer callback failed", NULL, NULL, NULL);
  }
  if (rc != LC_OK && bridge->message != NULL && !bridge->terminal) {
    lc_nack_req_init(&nack_req);
    nack_req.intent = LC_NACK_INTENT_FAILURE;
    nack_req.delay_seconds = 0L;
    lc_error_init(&nack_error);
    if (bridge->message->nack(bridge->message, &nack_req, &nack_error) ==
        LC_OK) {
      bridge->terminal = 1;
      bridge->message = NULL;
    } else {
      lc_error_set(bridge->error, nack_error.code, nack_error.http_status,
                   nack_error.message, nack_error.detail,
                   nack_error.server_code, nack_error.correlation_id);
      rc = bridge->error->code;
    }
    lc_error_cleanup(&nack_error);
  }
  if (bridge->message != NULL && !bridge->terminal) {
    bridge->message->close(bridge->message);
    bridge->message = NULL;
  } else if (bridge->terminal) {
    bridge->message = NULL;
  }
  bridge->handler_rc = rc;
  return NULL;
}

static int lc_client_subscribe_common(lc_client *self,
                                      const lc_dequeue_req *req,
                                      const lc_consumer *consumer,
                                      lc_error *error, int with_state) {
  lc_client_handle *client;
  lc_engine_dequeue_request engine_req;
  lc_engine_queue_stream_handler handler;
  lc_subscribe_bridge bridge;
  lc_engine_error engine_error;
  int rc;

  if (self == NULL || req == NULL || consumer == NULL ||
      consumer->handle == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "subscribe requires self, req, and consumer", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&handler, 0, sizeof(handler));
  memset(&bridge, 0, sizeof(bridge));
  bridge.client = client;
  bridge.consumer = consumer;
  bridge.error = error;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("queue", req->queue);
    fields[1] = lc_log_str_field("namespace", req->namespace_name);
    fields[2] = lc_log_str_field("owner", req->owner);
    fields[3] = lc_log_bool_field("with_state", with_state);
    lc_log_info(client->logger, "client.queue.subscribe.begin", fields, 4U);
  }
  lc_engine_error_init(&engine_error);
  engine_req.namespace_name = req->namespace_name;
  engine_req.queue = req->queue;
  engine_req.owner = req->owner;
  engine_req.txn_id = req->txn_id;
  engine_req.visibility_timeout_seconds = req->visibility_timeout_seconds;
  engine_req.wait_seconds = req->wait_seconds;
  engine_req.page_size = req->page_size > 0 ? req->page_size : 1;
  engine_req.start_after = req->start_after;
  handler.begin = lc_subscribe_begin;
  handler.chunk = lc_subscribe_chunk;
  handler.end = lc_subscribe_end;
  if (with_state) {
    rc = lc_engine_client_subscribe_with_state(
        client->engine, &engine_req, &handler, &bridge, &engine_error);
  } else {
    rc = lc_engine_client_subscribe(client->engine, &engine_req, &handler,
                                    &bridge, &engine_error);
  }
  if (rc != LC_ENGINE_OK) {
    if (bridge.pipe != NULL) {
      lc_stream_pipe_fail(bridge.pipe, LC_ERR_TRANSPORT,
                          "subscribe stream aborted");
      bridge.pipe = NULL;
    }
    if (bridge.handler_thread_started) {
      pthread_join(bridge.handler_thread, NULL);
      bridge.handler_thread_started = 0;
    }
    if (bridge.message != NULL) {
      bridge.message->close(bridge.message);
      bridge.message = NULL;
    }
    lc_engine_dequeue_response_cleanup(&bridge.meta);
    if (error != NULL && error->code != LC_OK) {
      lc_engine_error_cleanup(&engine_error);
      {
        pslog_field fields[4];

        fields[0] = lc_log_str_field("queue", req->queue);
        fields[1] = lc_log_str_field("namespace", req->namespace_name);
        fields[2] = lc_log_str_field("owner", req->owner);
        fields[3] = lc_log_bool_field("with_state", with_state);
        lc_client_log_operation_error(client, PSLOG_LEVEL_WARN,
                                      "client.queue.subscribe.error", fields,
                                      4U, error);
      }
      return error->code;
    }
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[4];

      fields[0] = lc_log_str_field("queue", req->queue);
      fields[1] = lc_log_str_field("namespace", req->namespace_name);
      fields[2] = lc_log_str_field("owner", req->owner);
      fields[3] = lc_log_bool_field("with_state", with_state);
      lc_client_log_operation_error(client, PSLOG_LEVEL_WARN,
                                    "client.queue.subscribe.error", fields, 4U,
                                    error);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("queue", req->queue);
    fields[1] = lc_log_str_field("namespace", req->namespace_name);
    fields[2] = lc_log_str_field("owner", req->owner);
    fields[3] = lc_log_bool_field("with_state", with_state);
    lc_log_info(client->logger, "client.queue.subscribe.complete", fields, 4U);
  }
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_client_subscribe_method(lc_client *self, const lc_dequeue_req *req,
                               const lc_consumer *consumer, lc_error *error) {
  return lc_client_subscribe_common(self, req, consumer, error, 0);
}

int lc_client_subscribe_with_state_method(lc_client *self,
                                          const lc_dequeue_req *req,
                                          const lc_consumer *consumer,
                                          lc_error *error) {
  return lc_client_subscribe_common(self, req, consumer, error, 1);
}

static int lc_watch_adapter(void *context,
                            const lc_engine_queue_watch_event *event,
                            lc_engine_error *error) {
  lc_watch_bridge *bridge;
  lc_watch_event public_event;
  lc_error public_error;
  int rc;

  bridge = (lc_watch_bridge *)context;
  memset(&public_event, 0, sizeof(public_event));
  lc_error_init(&public_error);
  public_event.namespace_name = lc_strdup_local(event->namespace_name);
  public_event.queue = lc_strdup_local(event->queue);
  public_event.available = event->available;
  public_event.head_message_id = lc_strdup_local(event->head_message_id);
  public_event.changed_at_unix = event->changed_at_unix;
  public_event.correlation_id = lc_strdup_local(event->correlation_id);
  rc = bridge->handler->handle(bridge->handler->context, &public_event,
                               &public_error);
  lc_watch_event_cleanup(&public_event);
  if (!rc && public_error.code != LC_OK) {
    error->code = LC_ENGINE_ERROR_TRANSPORT;
    error->message = lc_strdup_local(public_error.message);
  }
  lc_error_cleanup(&public_error);
  return rc;
}

int lc_client_watch_queue_method(lc_client *self, const lc_watch_queue_req *req,
                                 const lc_watch_handler *handler,
                                 lc_error *error) {
  lc_client_handle *client;
  lc_engine_watch_queue_request engine_req;
  lc_engine_error engine_error;
  lc_watch_bridge bridge;
  int rc;

  if (self == NULL || req == NULL || handler == NULL ||
      handler->handle == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "watch_queue requires self, req, and handler", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  memset(&engine_req, 0, sizeof(engine_req));
  bridge.handler = handler;
  lc_engine_error_init(&engine_error);
  engine_req.namespace_name = req->namespace_name;
  engine_req.queue = req->queue;
  rc = lc_engine_client_watch_queue(client->engine, &engine_req,
                                    lc_watch_adapter, &bridge, &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

void lc_client_close_method(lc_client *self) {
  lc_client_handle *client;
  size_t i;

  if (self == NULL) {
    return;
  }
  client = (lc_client_handle *)self;
  if (client->engine != NULL) {
    lc_engine_client_close(client->engine);
  }
  if (client->endpoints != NULL) {
    for (i = 0U; i < client->endpoint_count; ++i) {
      lc_client_free(client, client->endpoints[i]);
    }
    lc_client_free(client, client->endpoints);
  }
  lc_client_free(client, client->unix_socket_path);
  lc_client_free(client, client->client_bundle_path);
  lc_client_free(client, client->default_namespace);
  lc_client_free(client, client);
}

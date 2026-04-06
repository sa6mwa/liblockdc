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

typedef struct lc_lease_lonejson_load_state {
  lonejson_curl_parse parse;
  size_t byte_limit;
  size_t bytes_received;
} lc_lease_lonejson_load_state;

static int lc_lease_lonejson_load_write_callback(void *context,
                                                 const void *bytes,
                                                 size_t count,
                                                 lc_engine_error *error) {
  lc_lease_lonejson_load_state *state;
  size_t written;

  state = (lc_lease_lonejson_load_state *)context;
  if (state == NULL) {
    lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                               "mapped lease load state is required");
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
        "failed to parse mapped lease state response");
    return 0U;
  }
  state->bytes_received += count;
  return 1;
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
  lc_lease_lonejson_load_state load_state;
  lonejson_parse_options options;
  int no_content;
  char *content_type;
  char *etag;
  char *correlation_id;
  long version;
  long fencing_token;
  int rc;

  if (self == NULL || map == NULL || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lease load requires self, map, destination, and out",
                        NULL, NULL, NULL);
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
  memset(&load_state, 0, sizeof(load_state));
  options =
      parse_options != NULL ? *parse_options : lonejson_default_parse_options();
  load_state.byte_limit = lease->client->http_json_response_limit_bytes > 0U
                              ? lease->client->http_json_response_limit_bytes
                              : (size_t)LC_HTTP_JSON_RESPONSE_LIMIT_DEFAULT;
  lonejson_init(map, dst);
  rc = lonejson_curl_parse_init(&load_state.parse, map, dst, &options);
  if (rc != LONEJSON_STATUS_OK) {
    lonejson_cleanup(map, dst);
    return lc_lonejson_error_from_status(
        error, rc, &load_state.parse.error,
        "failed to initialize mapped lease load parser");
  }
  legacy_req.namespace_name = lease->namespace_name;
  legacy_req.key = lease->key;
  legacy_req.lease_id = lease->lease_id;
  legacy_req.fencing_token = lease->fencing_token;
  legacy_req.public_read = opts != NULL ? opts->public_read : 0;
  rc = lc_engine_client_get_into(lease->client->legacy, &legacy_req,
                                 lc_lease_lonejson_load_write_callback,
                                 &load_state, &legacy_res, &legacy_error);
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
    lonejson_curl_parse_cleanup(&load_state.parse);
    lonejson_cleanup(map, dst);
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
  if (!legacy_res.no_content) {
    rc = lonejson_curl_parse_finish(&load_state.parse);
    if (rc != LONEJSON_STATUS_OK) {
      lc_engine_get_stream_response_cleanup(&legacy_res);
      lc_engine_error_cleanup(&legacy_error);
      lonejson_curl_parse_cleanup(&load_state.parse);
      lonejson_cleanup(map, dst);
      return lc_lonejson_error_from_status(
          error, rc, &load_state.parse.error,
          "failed to parse mapped lease state");
    }
  }
  lonejson_curl_parse_cleanup(&load_state.parse);
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
  lc_engine_update_request legacy_req;
  lc_engine_update_response legacy_res;
  lc_engine_error legacy_error;
  lc_update_opts update_opts;
  int rc;

  if (self == NULL || map == NULL || src == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lease save requires self, map, and source", NULL, NULL,
                        NULL);
  }
  lease = (lc_lease_handle *)self;
  memset(&legacy_req, 0, sizeof(legacy_req));
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  memset(&update_opts, 0, sizeof(update_opts));
  update_opts.content_type = "application/json";
  legacy_req.namespace_name = lease->namespace_name;
  legacy_req.key = lease->key;
  legacy_req.lease_id = lease->lease_id;
  legacy_req.txn_id = lease->txn_id;
  legacy_req.fencing_token = lease->fencing_token;
  legacy_req.if_state_etag = update_opts.if_state_etag;
  legacy_req.if_version = update_opts.if_version;
  legacy_req.has_if_version = update_opts.has_if_version;
  if (!legacy_req.has_if_version && lease->version > 0L) {
    legacy_req.if_version = lease->version;
    legacy_req.has_if_version = 1;
  }
  legacy_req.content_type = update_opts.content_type;
  rc = lc_engine_client_update_stream(lease->client->legacy, &legacy_req, map,
                                      src, write_options, &legacy_res,
                                      &legacy_error);
  if (rc != LC_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[4];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      fields[3] = lc_log_str_field("content_type", update_opts.content_type);
      lc_lease_log_error(lease,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? PSLOG_LEVEL_ERROR
                             : PSLOG_LEVEL_WARN,
                         error != NULL && error->code == LC_ERR_TRANSPORT
                             ? "client.update.transport_error"
                             : "client.update.error",
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
    fields[2] = lc_log_i64_field("version", lease->version);
    fields[3] = lc_log_str_field("state_etag", lease->state_etag);
    fields[4] = lc_log_str_field("cid", legacy_res.correlation_id);
    fields[5] = lc_log_str_field("content_type", update_opts.content_type);
    lc_log_trace(lease->client->logger, "client.update.success", fields, 6U);
  }
  lc_engine_update_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
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

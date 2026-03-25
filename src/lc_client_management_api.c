#include "lc_api_internal.h"
#include "lc_log.h"

static void
lc_client_log_management_error(lc_client_handle *client, pslog_level level,
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

static void lc_client_log_management_failure(lc_client_handle *client,
                                             const char *transport_message,
                                             const char *protocol_message,
                                             const pslog_field *fields,
                                             size_t count,
                                             const lc_error *error) {
  lc_client_log_management_error(
      client,
      error != NULL && error->code == LC_ERR_TRANSPORT ? PSLOG_LEVEL_ERROR
                                                       : PSLOG_LEVEL_WARN,
      error != NULL && error->code == LC_ERR_TRANSPORT ? transport_message
                                                       : protocol_message,
      fields, count, error);
}

static int lc_copy_string_list(lc_string_list *dst,
                               const lc_engine_string_array *src) {
  size_t index;

  memset(dst, 0, sizeof(*dst));
  if (src == NULL || src->count == 0U) {
    return 1;
  }
  dst->items = (char **)calloc(src->count, sizeof(char *));
  if (dst->items == NULL) {
    return 0;
  }
  dst->count = src->count;
  for (index = 0U; index < src->count; ++index) {
    dst->items[index] = lc_strdup_local(src->items[index]);
    if (src->items[index] != NULL && dst->items[index] == NULL) {
      lc_string_list_cleanup(dst);
      return 0;
    }
  }
  return 1;
}

static int lc_copy_tc_rm_res(lc_tc_rm_res *out,
                             const lc_engine_tcrm_register_response *legacy) {
  memset(out, 0, sizeof(*out));
  out->backend_hash = lc_strdup_local(legacy->backend_hash);
  if (legacy->backend_hash != NULL && out->backend_hash == NULL) {
    return 0;
  }
  if (!lc_copy_string_list(&out->endpoints, &legacy->endpoints)) {
    free(out->backend_hash);
    memset(out, 0, sizeof(*out));
    return 0;
  }
  out->updated_at_unix = legacy->updated_at_unix;
  out->correlation_id = lc_strdup_local(legacy->correlation_id);
  if (legacy->correlation_id != NULL && out->correlation_id == NULL) {
    lc_tc_rm_res_cleanup(out);
    return 0;
  }
  return 1;
}

static int
lc_copy_txn_participants(const lc_txn_decision_req *req,
                         lc_engine_txn_participant **out_participants) {
  lc_engine_txn_participant *participants;
  size_t index;

  *out_participants = NULL;
  if (req->participant_count == 0U || req->participants == NULL) {
    return 1;
  }
  participants = (lc_engine_txn_participant *)calloc(req->participant_count,
                                                     sizeof(*participants));
  if (participants == NULL) {
    return 0;
  }
  for (index = 0U; index < req->participant_count; ++index) {
    participants[index].namespace_name =
        req->participants[index].namespace_name;
    participants[index].key = req->participants[index].key;
    participants[index].backend_hash = req->participants[index].backend_hash;
  }
  *out_participants = participants;
  return 1;
}

static int lc_fill_txn_request(const lc_txn_decision_req *req,
                               const char *state,
                               lc_engine_txn_decision_request *legacy_req,
                               lc_engine_txn_participant **participants,
                               lc_error *error) {
  if (req == NULL || legacy_req == NULL || participants == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "txn request requires req, legacy_req, and participants", NULL, NULL,
        NULL);
  }
  memset(legacy_req, 0, sizeof(*legacy_req));
  if (!lc_copy_txn_participants(req, participants)) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate txn participants", NULL, NULL,
                        NULL);
  }
  legacy_req->txn_id = req->txn_id;
  legacy_req->state = state;
  legacy_req->participants = *participants;
  legacy_req->participant_count = req->participant_count;
  legacy_req->expires_at_unix = req->expires_at_unix;
  legacy_req->tc_term = req->tc_term;
  legacy_req->target_backend_hash = req->target_backend_hash;
  return LC_OK;
}

static int lc_copy_namespace_config_res(
    lc_namespace_config_res *out,
    const lc_engine_namespace_config_response *legacy) {
  memset(out, 0, sizeof(*out));
  out->namespace_name = lc_strdup_local(legacy->namespace_name);
  out->preferred_engine = lc_strdup_local(legacy->preferred_engine);
  out->fallback_engine = lc_strdup_local(legacy->fallback_engine);
  out->correlation_id = lc_strdup_local(legacy->correlation_id);
  if ((legacy->namespace_name != NULL && out->namespace_name == NULL) ||
      (legacy->preferred_engine != NULL && out->preferred_engine == NULL) ||
      (legacy->fallback_engine != NULL && out->fallback_engine == NULL) ||
      (legacy->correlation_id != NULL && out->correlation_id == NULL)) {
    lc_namespace_config_res_cleanup(out);
    return 0;
  }
  return 1;
}

static int
lc_copy_index_flush_res(lc_index_flush_res *out,
                        const lc_engine_index_flush_response *legacy) {
  memset(out, 0, sizeof(*out));
  out->namespace_name = lc_strdup_local(legacy->namespace_name);
  out->mode = lc_strdup_local(legacy->mode);
  out->flush_id = lc_strdup_local(legacy->flush_id);
  out->accepted = legacy->accepted;
  out->flushed = legacy->flushed;
  out->pending = legacy->pending;
  out->index_seq = legacy->index_seq;
  out->correlation_id = lc_strdup_local(legacy->correlation_id);
  if ((legacy->namespace_name != NULL && out->namespace_name == NULL) ||
      (legacy->mode != NULL && out->mode == NULL) ||
      (legacy->flush_id != NULL && out->flush_id == NULL) ||
      (legacy->correlation_id != NULL && out->correlation_id == NULL)) {
    lc_index_flush_res_cleanup(out);
    return 0;
  }
  return 1;
}

static int lc_copy_txn_replay_res(lc_txn_replay_res *out,
                                  const lc_engine_txn_replay_response *legacy) {
  memset(out, 0, sizeof(*out));
  out->txn_id = lc_strdup_local(legacy->txn_id);
  out->state = lc_strdup_local(legacy->state);
  out->correlation_id = lc_strdup_local(legacy->correlation_id);
  if ((legacy->txn_id != NULL && out->txn_id == NULL) ||
      (legacy->state != NULL && out->state == NULL) ||
      (legacy->correlation_id != NULL && out->correlation_id == NULL)) {
    lc_txn_replay_res_cleanup(out);
    return 0;
  }
  return 1;
}

static int
lc_copy_txn_decision_res(lc_txn_decision_res *out,
                         const lc_engine_txn_decision_response *legacy) {
  memset(out, 0, sizeof(*out));
  out->txn_id = lc_strdup_local(legacy->txn_id);
  out->state = lc_strdup_local(legacy->state);
  out->correlation_id = lc_strdup_local(legacy->correlation_id);
  if ((legacy->txn_id != NULL && out->txn_id == NULL) ||
      (legacy->state != NULL && out->state == NULL) ||
      (legacy->correlation_id != NULL && out->correlation_id == NULL)) {
    lc_txn_decision_res_cleanup(out);
    return 0;
  }
  return 1;
}

static int lc_copy_tc_lease_acquire_res(
    lc_tc_lease_acquire_res *out,
    const lc_engine_tc_lease_acquire_response *legacy) {
  memset(out, 0, sizeof(*out));
  out->granted = legacy->granted;
  out->leader_id = lc_strdup_local(legacy->leader_id);
  out->leader_endpoint = lc_strdup_local(legacy->leader_endpoint);
  out->term = legacy->term;
  out->expires_at_unix = legacy->expires_at_unix;
  out->correlation_id = lc_strdup_local(legacy->correlation_id);
  if ((legacy->leader_id != NULL && out->leader_id == NULL) ||
      (legacy->leader_endpoint != NULL && out->leader_endpoint == NULL) ||
      (legacy->correlation_id != NULL && out->correlation_id == NULL)) {
    lc_tc_lease_acquire_res_cleanup(out);
    return 0;
  }
  return 1;
}

static int
lc_copy_tc_lease_renew_res(lc_tc_lease_renew_res *out,
                           const lc_engine_tc_lease_renew_response *legacy) {
  memset(out, 0, sizeof(*out));
  out->renewed = legacy->renewed;
  out->leader_id = lc_strdup_local(legacy->leader_id);
  out->leader_endpoint = lc_strdup_local(legacy->leader_endpoint);
  out->term = legacy->term;
  out->expires_at_unix = legacy->expires_at_unix;
  out->correlation_id = lc_strdup_local(legacy->correlation_id);
  if ((legacy->leader_id != NULL && out->leader_id == NULL) ||
      (legacy->leader_endpoint != NULL && out->leader_endpoint == NULL) ||
      (legacy->correlation_id != NULL && out->correlation_id == NULL)) {
    lc_tc_lease_renew_res_cleanup(out);
    return 0;
  }
  return 1;
}

static int lc_copy_tc_lease_release_res(
    lc_tc_lease_release_res *out,
    const lc_engine_tc_lease_release_response *legacy) {
  memset(out, 0, sizeof(*out));
  out->released = legacy->released;
  out->correlation_id = lc_strdup_local(legacy->correlation_id);
  if (legacy->correlation_id != NULL && out->correlation_id == NULL) {
    lc_tc_lease_release_res_cleanup(out);
    return 0;
  }
  return 1;
}

static int lc_copy_tc_leader_res(lc_tc_leader_res *out,
                                 const lc_engine_tc_leader_response *legacy) {
  memset(out, 0, sizeof(*out));
  out->leader_id = lc_strdup_local(legacy->leader_id);
  out->leader_endpoint = lc_strdup_local(legacy->leader_endpoint);
  out->term = legacy->term;
  out->expires_at_unix = legacy->expires_at_unix;
  out->correlation_id = lc_strdup_local(legacy->correlation_id);
  if ((legacy->leader_id != NULL && out->leader_id == NULL) ||
      (legacy->leader_endpoint != NULL && out->leader_endpoint == NULL) ||
      (legacy->correlation_id != NULL && out->correlation_id == NULL)) {
    lc_tc_leader_res_cleanup(out);
    return 0;
  }
  return 1;
}

static int lc_copy_tc_cluster_res(lc_tc_cluster_res *out,
                                  const lc_engine_tc_cluster_response *legacy) {
  memset(out, 0, sizeof(*out));
  if (!lc_copy_string_list(&out->endpoints, &legacy->endpoints)) {
    return 0;
  }
  out->updated_at_unix = legacy->updated_at_unix;
  out->expires_at_unix = legacy->expires_at_unix;
  out->correlation_id = lc_strdup_local(legacy->correlation_id);
  if (legacy->correlation_id != NULL && out->correlation_id == NULL) {
    lc_tc_cluster_res_cleanup(out);
    return 0;
  }
  return 1;
}

int lc_client_get_namespace_config_method(lc_client *self,
                                          const lc_namespace_config_req *req,
                                          lc_namespace_config_res *out,
                                          lc_error *error) {
  lc_client_handle *client;
  lc_engine_namespace_config_response legacy_res;
  lc_engine_error legacy_error;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "get_namespace_config requires self, req, and out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[1];

    fields[0] = lc_log_str_field("namespace", req->namespace_name);
    lc_log_trace(client->logger, "client.namespace.get.start", fields, 1U);
  }
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  rc = lc_engine_client_get_namespace_config(
      client->legacy, req->namespace_name, &legacy_res, &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[1];

      fields[0] = lc_log_str_field("namespace", req->namespace_name);
      lc_client_log_management_error(
          client,
          error != NULL && error->code == LC_ERR_TRANSPORT ? PSLOG_LEVEL_ERROR
                                                           : PSLOG_LEVEL_WARN,
          error != NULL && error->code == LC_ERR_TRANSPORT
              ? "client.namespace.get.transport_error"
              : "client.namespace.get.error",
          fields, 1U, error);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  if (!lc_copy_namespace_config_res(out, &legacy_res)) {
    lc_engine_namespace_config_response_cleanup(&legacy_res);
    lc_engine_error_cleanup(&legacy_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate namespace config response", NULL,
                        NULL, NULL);
  }
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("namespace", out->namespace_name);
    fields[1] = lc_log_str_field("preferred_engine", out->preferred_engine);
    fields[2] = lc_log_str_field("fallback_engine", out->fallback_engine);
    fields[3] = lc_log_str_field("cid", out->correlation_id);
    lc_log_trace(client->logger, "client.namespace.get.success", fields, 4U);
  }
  lc_engine_namespace_config_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_client_update_namespace_config_method(lc_client *self,
                                             const lc_namespace_config_req *req,
                                             lc_namespace_config_res *out,
                                             lc_error *error) {
  lc_client_handle *client;
  lc_engine_namespace_config_request legacy_req;
  lc_engine_namespace_config_response legacy_res;
  lc_engine_error legacy_error;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "update_namespace_config requires self, req, and out",
                        NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("namespace", req->namespace_name);
    fields[1] = lc_log_str_field("preferred_engine", req->preferred_engine);
    fields[2] = lc_log_str_field("fallback_engine", req->fallback_engine);
    lc_log_trace(client->logger, "client.namespace.set.start", fields, 3U);
  }
  memset(&legacy_req, 0, sizeof(legacy_req));
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  legacy_req.namespace_name = req->namespace_name;
  legacy_req.preferred_engine = req->preferred_engine;
  legacy_req.fallback_engine = req->fallback_engine;
  rc = lc_engine_client_update_namespace_config(client->legacy, &legacy_req,
                                                &legacy_res, &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("namespace", req->namespace_name);
      fields[1] = lc_log_str_field("preferred_engine", req->preferred_engine);
      fields[2] = lc_log_str_field("fallback_engine", req->fallback_engine);
      lc_client_log_management_error(
          client,
          error != NULL && error->code == LC_ERR_TRANSPORT ? PSLOG_LEVEL_ERROR
                                                           : PSLOG_LEVEL_WARN,
          error != NULL && error->code == LC_ERR_TRANSPORT
              ? "client.namespace.set.transport_error"
              : "client.namespace.set.error",
          fields, 3U, error);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  if (!lc_copy_namespace_config_res(out, &legacy_res)) {
    lc_engine_namespace_config_response_cleanup(&legacy_res);
    lc_engine_error_cleanup(&legacy_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate namespace config response", NULL,
                        NULL, NULL);
  }
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("namespace", out->namespace_name);
    fields[1] = lc_log_str_field("preferred_engine", out->preferred_engine);
    fields[2] = lc_log_str_field("fallback_engine", out->fallback_engine);
    fields[3] = lc_log_str_field("cid", out->correlation_id);
    lc_log_trace(client->logger, "client.namespace.set.success", fields, 4U);
  }
  lc_engine_namespace_config_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_client_flush_index_method(lc_client *self, const lc_index_flush_req *req,
                                 lc_index_flush_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_engine_index_flush_request legacy_req;
  lc_engine_index_flush_response legacy_res;
  lc_engine_error legacy_error;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "flush_index requires self, req, and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[2];

    fields[0] = lc_log_str_field("namespace", req->namespace_name);
    fields[1] = lc_log_str_field("mode", req->mode);
    lc_log_trace(client->logger, "client.index.flush.start", fields, 2U);
  }
  memset(&legacy_req, 0, sizeof(legacy_req));
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  legacy_req.namespace_name = req->namespace_name;
  legacy_req.mode = req->mode;
  rc = lc_engine_client_index_flush(client->legacy, &legacy_req, &legacy_res,
                                    &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("namespace", req->namespace_name);
      fields[1] = lc_log_str_field("mode", req->mode);
      lc_client_log_management_error(
          client,
          error != NULL && error->code == LC_ERR_TRANSPORT ? PSLOG_LEVEL_ERROR
                                                           : PSLOG_LEVEL_WARN,
          error != NULL && error->code == LC_ERR_TRANSPORT
              ? "client.index.flush.transport_error"
              : "client.index.flush.error",
          fields, 2U, error);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  if (!lc_copy_index_flush_res(out, &legacy_res)) {
    lc_engine_index_flush_response_cleanup(&legacy_res);
    lc_engine_error_cleanup(&legacy_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate index flush response", NULL, NULL,
                        NULL);
  }
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("namespace", out->namespace_name);
    fields[1] = lc_log_str_field("mode", out->mode);
    fields[2] = lc_log_bool_field("accepted", out->accepted);
    fields[3] = lc_log_bool_field("flushed", out->flushed);
    fields[4] = lc_log_str_field("cid", out->correlation_id);
    lc_log_trace(client->logger, "client.index.flush.success", fields, 5U);
  }
  lc_engine_index_flush_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_client_txn_replay_method(lc_client *self, const lc_txn_replay_req *req,
                                lc_txn_replay_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_engine_txn_replay_request legacy_req;
  lc_engine_txn_replay_response legacy_res;
  lc_engine_error legacy_error;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "txn_replay requires self, req, and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[1];

    fields[0] = lc_log_str_field("txn_id", req->txn_id);
    lc_log_trace(client->logger, "client.txn.replay.start", fields, 1U);
  }
  memset(&legacy_req, 0, sizeof(legacy_req));
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  legacy_req.txn_id = req->txn_id;
  rc = lc_engine_client_txn_replay(client->legacy, &legacy_req, &legacy_res,
                                   &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[1];

      fields[0] = lc_log_str_field("txn_id", req->txn_id);
      lc_client_log_management_failure(
          client, "client.txn.replay.transport_error",
          "client.txn.replay.error", fields, 1U, error);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  if (!lc_copy_txn_replay_res(out, &legacy_res)) {
    lc_engine_txn_replay_response_cleanup(&legacy_res);
    lc_engine_error_cleanup(&legacy_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate txn replay response", NULL, NULL,
                        NULL);
  }
  {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("txn_id", out->txn_id);
    fields[1] = lc_log_str_field("state", out->state);
    fields[2] = lc_log_str_field("cid", out->correlation_id);
    lc_log_trace(client->logger, "client.txn.replay.success", fields, 3U);
  }
  lc_engine_txn_replay_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_client_txn_prepare_method(lc_client *self,
                                 const lc_txn_decision_req *req,
                                 lc_txn_decision_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_engine_txn_decision_request legacy_req;
  lc_engine_txn_decision_response legacy_res;
  lc_engine_txn_participant *participants;
  lc_engine_error legacy_error;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "txn_prepare requires self, req, and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("txn_id", req->txn_id);
    fields[1] = lc_log_u64_field("participants", req->participant_count);
    fields[2] = lc_log_u64_field("tc_term", req->tc_term);
    lc_log_trace(client->logger, "client.txn.prepare.start", fields, 3U);
  }
  participants = NULL;
  rc = lc_fill_txn_request(req, "prepare", &legacy_req, &participants, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  rc = lc_engine_client_txn_decide(client->legacy, &legacy_req, &legacy_res,
                                   &legacy_error);
  free(participants);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("txn_id", req->txn_id);
      fields[1] = lc_log_u64_field("participants", req->participant_count);
      fields[2] = lc_log_u64_field("tc_term", req->tc_term);
      lc_client_log_management_failure(
          client, "client.txn.prepare.transport_error",
          "client.txn.prepare.error", fields, 3U, error);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  if (!lc_copy_txn_decision_res(out, &legacy_res)) {
    lc_engine_txn_decision_response_cleanup(&legacy_res);
    lc_engine_error_cleanup(&legacy_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate txn prepare response", NULL, NULL,
                        NULL);
  }
  {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("txn_id", out->txn_id);
    fields[1] = lc_log_str_field("state", out->state);
    fields[2] = lc_log_str_field("cid", out->correlation_id);
    lc_log_trace(client->logger, "client.txn.prepare.success", fields, 3U);
  }
  lc_engine_txn_decision_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_client_txn_commit_method(lc_client *self, const lc_txn_decision_req *req,
                                lc_txn_decision_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_engine_txn_decision_request legacy_req;
  lc_engine_txn_decision_response legacy_res;
  lc_engine_txn_participant *participants;
  lc_engine_error legacy_error;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "txn_commit requires self, req, and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[2];

    fields[0] = lc_log_str_field("txn_id", req->txn_id);
    fields[1] = lc_log_u64_field("participants", req->participant_count);
    lc_log_trace(client->logger, "client.txn.commit.start", fields, 2U);
  }
  participants = NULL;
  rc = lc_fill_txn_request(req, NULL, &legacy_req, &participants, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  rc = lc_engine_client_txn_commit(client->legacy, &legacy_req, &legacy_res,
                                   &legacy_error);
  free(participants);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("txn_id", req->txn_id);
      fields[1] = lc_log_u64_field("participants", req->participant_count);
      lc_client_log_management_failure(
          client, "client.txn.commit.transport_error",
          "client.txn.commit.error", fields, 2U, error);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  if (!lc_copy_txn_decision_res(out, &legacy_res)) {
    lc_engine_txn_decision_response_cleanup(&legacy_res);
    lc_engine_error_cleanup(&legacy_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate txn commit response", NULL, NULL,
                        NULL);
  }
  {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("txn_id", out->txn_id);
    fields[1] = lc_log_str_field("state", out->state);
    fields[2] = lc_log_str_field("cid", out->correlation_id);
    lc_log_trace(client->logger, "client.txn.commit.success", fields, 3U);
  }
  lc_engine_txn_decision_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_client_txn_rollback_method(lc_client *self,
                                  const lc_txn_decision_req *req,
                                  lc_txn_decision_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_engine_txn_decision_request legacy_req;
  lc_engine_txn_decision_response legacy_res;
  lc_engine_txn_participant *participants;
  lc_engine_error legacy_error;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "txn_rollback requires self, req, and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[2];

    fields[0] = lc_log_str_field("txn_id", req->txn_id);
    fields[1] = lc_log_u64_field("participants", req->participant_count);
    lc_log_trace(client->logger, "client.txn.rollback.start", fields, 2U);
  }
  participants = NULL;
  rc = lc_fill_txn_request(req, NULL, &legacy_req, &participants, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  rc = lc_engine_client_txn_rollback(client->legacy, &legacy_req, &legacy_res,
                                     &legacy_error);
  free(participants);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("txn_id", req->txn_id);
      fields[1] = lc_log_u64_field("participants", req->participant_count);
      lc_client_log_management_failure(
          client, "client.txn.rollback.transport_error",
          "client.txn.rollback.error", fields, 2U, error);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  if (!lc_copy_txn_decision_res(out, &legacy_res)) {
    lc_engine_txn_decision_response_cleanup(&legacy_res);
    lc_engine_error_cleanup(&legacy_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate txn rollback response", NULL, NULL,
                        NULL);
  }
  {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("txn_id", out->txn_id);
    fields[1] = lc_log_str_field("state", out->state);
    fields[2] = lc_log_str_field("cid", out->correlation_id);
    lc_log_trace(client->logger, "client.txn.rollback.success", fields, 3U);
  }
  lc_engine_txn_decision_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_client_tc_lease_acquire_method(lc_client *self,
                                      const lc_tc_lease_acquire_req *req,
                                      lc_tc_lease_acquire_res *out,
                                      lc_error *error) {
  lc_client_handle *client;
  lc_engine_tc_lease_acquire_request legacy_req;
  lc_engine_tc_lease_acquire_response legacy_res;
  lc_engine_error legacy_error;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "tc_lease_acquire requires self, req, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("candidate_id", req->candidate_id);
    fields[1] = lc_log_str_field("candidate_endpoint", req->candidate_endpoint);
    fields[2] = lc_log_u64_field("term", req->term);
    lc_log_trace(client->logger, "client.tc.lease.acquire.start", fields, 3U);
  }
  memset(&legacy_req, 0, sizeof(legacy_req));
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  legacy_req.candidate_id = req->candidate_id;
  legacy_req.candidate_endpoint = req->candidate_endpoint;
  legacy_req.term = req->term;
  legacy_req.ttl_ms = req->ttl_ms;
  rc = lc_engine_client_tc_lease_acquire(client->legacy, &legacy_req,
                                         &legacy_res, &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("candidate_id", req->candidate_id);
      fields[1] =
          lc_log_str_field("candidate_endpoint", req->candidate_endpoint);
      fields[2] = lc_log_u64_field("term", req->term);
      lc_client_log_management_failure(
          client, "client.tc.lease.acquire.transport_error",
          "client.tc.lease.acquire.error", fields, 3U, error);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  if (!lc_copy_tc_lease_acquire_res(out, &legacy_res)) {
    lc_engine_tc_lease_acquire_response_cleanup(&legacy_res);
    lc_engine_error_cleanup(&legacy_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate tc lease acquire response", NULL,
                        NULL, NULL);
  }
  {
    pslog_field fields[5];

    fields[0] = lc_log_bool_field("granted", out->granted);
    fields[1] = lc_log_str_field("leader_id", out->leader_id);
    fields[2] = lc_log_str_field("leader_endpoint", out->leader_endpoint);
    fields[3] = lc_log_u64_field("term", out->term);
    fields[4] = lc_log_str_field("cid", out->correlation_id);
    lc_log_trace(client->logger, "client.tc.lease.acquire.success", fields, 5U);
  }
  lc_engine_tc_lease_acquire_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_client_tc_lease_renew_method(lc_client *self,
                                    const lc_tc_lease_renew_req *req,
                                    lc_tc_lease_renew_res *out,
                                    lc_error *error) {
  lc_client_handle *client;
  lc_engine_tc_lease_renew_request legacy_req;
  lc_engine_tc_lease_renew_response legacy_res;
  lc_engine_error legacy_error;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "tc_lease_renew requires self, req, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[2];

    fields[0] = lc_log_str_field("leader_id", req->leader_id);
    fields[1] = lc_log_u64_field("term", req->term);
    lc_log_trace(client->logger, "client.tc.lease.renew.start", fields, 2U);
  }
  memset(&legacy_req, 0, sizeof(legacy_req));
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  legacy_req.leader_id = req->leader_id;
  legacy_req.term = req->term;
  legacy_req.ttl_ms = req->ttl_ms;
  rc = lc_engine_client_tc_lease_renew(client->legacy, &legacy_req, &legacy_res,
                                       &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("leader_id", req->leader_id);
      fields[1] = lc_log_u64_field("term", req->term);
      lc_client_log_management_failure(
          client, "client.tc.lease.renew.transport_error",
          "client.tc.lease.renew.error", fields, 2U, error);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  if (!lc_copy_tc_lease_renew_res(out, &legacy_res)) {
    lc_engine_tc_lease_renew_response_cleanup(&legacy_res);
    lc_engine_error_cleanup(&legacy_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate tc lease renew response", NULL,
                        NULL, NULL);
  }
  {
    pslog_field fields[5];

    fields[0] = lc_log_bool_field("renewed", out->renewed);
    fields[1] = lc_log_str_field("leader_id", out->leader_id);
    fields[2] = lc_log_str_field("leader_endpoint", out->leader_endpoint);
    fields[3] = lc_log_u64_field("term", out->term);
    fields[4] = lc_log_str_field("cid", out->correlation_id);
    lc_log_trace(client->logger, "client.tc.lease.renew.success", fields, 5U);
  }
  lc_engine_tc_lease_renew_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_client_tc_lease_release_method(lc_client *self,
                                      const lc_tc_lease_release_req *req,
                                      lc_tc_lease_release_res *out,
                                      lc_error *error) {
  lc_client_handle *client;
  lc_engine_tc_lease_release_request legacy_req;
  lc_engine_tc_lease_release_response legacy_res;
  lc_engine_error legacy_error;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "tc_lease_release requires self, req, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[2];

    fields[0] = lc_log_str_field("leader_id", req->leader_id);
    fields[1] = lc_log_u64_field("term", req->term);
    lc_log_trace(client->logger, "client.tc.lease.release.start", fields, 2U);
  }
  memset(&legacy_req, 0, sizeof(legacy_req));
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  legacy_req.leader_id = req->leader_id;
  legacy_req.term = req->term;
  rc = lc_engine_client_tc_lease_release(client->legacy, &legacy_req,
                                         &legacy_res, &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("leader_id", req->leader_id);
      fields[1] = lc_log_u64_field("term", req->term);
      lc_client_log_management_failure(
          client, "client.tc.lease.release.transport_error",
          "client.tc.lease.release.error", fields, 2U, error);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  if (!lc_copy_tc_lease_release_res(out, &legacy_res)) {
    lc_engine_tc_lease_release_response_cleanup(&legacy_res);
    lc_engine_error_cleanup(&legacy_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate tc lease release response", NULL,
                        NULL, NULL);
  }
  {
    pslog_field fields[3];

    fields[0] = lc_log_bool_field("released", out->released);
    fields[1] = lc_log_u64_field("term", req->term);
    fields[2] = lc_log_str_field("cid", out->correlation_id);
    lc_log_trace(client->logger, "client.tc.lease.release.success", fields, 3U);
  }
  lc_engine_tc_lease_release_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_client_tc_leader_method(lc_client *self, lc_tc_leader_res *out,
                               lc_error *error) {
  lc_client_handle *client;
  lc_engine_tc_leader_response legacy_res;
  lc_engine_error legacy_error;
  int rc;

  if (self == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "tc_leader requires self and out", NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  lc_log_trace(client->logger, "client.tc.leader.start", NULL, 0U);
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  rc = lc_engine_client_tc_leader(client->legacy, &legacy_res, &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    lc_client_log_management_failure(client, "client.tc.leader.transport_error",
                                     "client.tc.leader.error", NULL, 0U, error);
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  if (!lc_copy_tc_leader_res(out, &legacy_res)) {
    lc_engine_tc_leader_response_cleanup(&legacy_res);
    lc_engine_error_cleanup(&legacy_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate tc leader response", NULL, NULL,
                        NULL);
  }
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("leader_id", out->leader_id);
    fields[1] = lc_log_str_field("leader_endpoint", out->leader_endpoint);
    fields[2] = lc_log_u64_field("term", out->term);
    fields[3] = lc_log_str_field("cid", out->correlation_id);
    lc_log_trace(client->logger, "client.tc.leader.success", fields, 4U);
  }
  lc_engine_tc_leader_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_client_tc_cluster_announce_method(lc_client *self,
                                         const lc_tc_cluster_announce_req *req,
                                         lc_tc_cluster_res *out,
                                         lc_error *error) {
  lc_client_handle *client;
  lc_engine_tc_cluster_announce_request legacy_req;
  lc_engine_tc_cluster_response legacy_res;
  lc_engine_error legacy_error;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "tc_cluster_announce requires self, req, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[1];

    fields[0] = lc_log_str_field("self_endpoint", req->self_endpoint);
    lc_log_trace(client->logger, "client.tc.cluster.announce.start", fields,
                 1U);
  }
  memset(&legacy_req, 0, sizeof(legacy_req));
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  legacy_req.self_endpoint = req->self_endpoint;
  rc = lc_engine_client_tc_cluster_announce(client->legacy, &legacy_req,
                                            &legacy_res, &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[1];

      fields[0] = lc_log_str_field("self_endpoint", req->self_endpoint);
      lc_client_log_management_failure(
          client, "client.tc.cluster.announce.transport_error",
          "client.tc.cluster.announce.error", fields, 1U, error);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  if (!lc_copy_tc_cluster_res(out, &legacy_res)) {
    lc_engine_tc_cluster_response_cleanup(&legacy_res);
    lc_engine_error_cleanup(&legacy_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate tc cluster response", NULL, NULL,
                        NULL);
  }
  {
    pslog_field fields[3];

    fields[0] = lc_log_u64_field("count", out->endpoints.count);
    fields[1] = lc_log_i64_field("expires_at", out->expires_at_unix);
    fields[2] = lc_log_str_field("cid", out->correlation_id);
    lc_log_trace(client->logger, "client.tc.cluster.announce.success", fields,
                 3U);
  }
  lc_engine_tc_cluster_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_client_tc_cluster_leave_method(lc_client *self, lc_tc_cluster_res *out,
                                      lc_error *error) {
  lc_client_handle *client;
  lc_engine_tc_cluster_response legacy_res;
  lc_engine_error legacy_error;
  int rc;

  if (self == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "tc_cluster_leave requires self and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  lc_log_trace(client->logger, "client.tc.cluster.leave.start", NULL, 0U);
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  rc = lc_engine_client_tc_cluster_leave(client->legacy, &legacy_res,
                                         &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    lc_client_log_management_failure(
        client, "client.tc.cluster.leave.transport_error",
        "client.tc.cluster.leave.error", NULL, 0U, error);
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  if (!lc_copy_tc_cluster_res(out, &legacy_res)) {
    lc_engine_tc_cluster_response_cleanup(&legacy_res);
    lc_engine_error_cleanup(&legacy_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate tc cluster response", NULL, NULL,
                        NULL);
  }
  {
    pslog_field fields[3];

    fields[0] = lc_log_u64_field("count", out->endpoints.count);
    fields[1] = lc_log_i64_field("expires_at", out->expires_at_unix);
    fields[2] = lc_log_str_field("cid", out->correlation_id);
    lc_log_trace(client->logger, "client.tc.cluster.leave.success", fields, 3U);
  }
  lc_engine_tc_cluster_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_client_tc_cluster_list_method(lc_client *self, lc_tc_cluster_res *out,
                                     lc_error *error) {
  lc_client_handle *client;
  lc_engine_tc_cluster_response legacy_res;
  lc_engine_error legacy_error;
  int rc;

  if (self == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "tc_cluster_list requires self and out", NULL, NULL,
                        NULL);
  }
  client = (lc_client_handle *)self;
  lc_log_trace(client->logger, "client.tc.cluster.list.start", NULL, 0U);
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  rc = lc_engine_client_tc_cluster_list(client->legacy, &legacy_res,
                                        &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    lc_client_log_management_failure(
        client, "client.tc.cluster.list.transport_error",
        "client.tc.cluster.list.error", NULL, 0U, error);
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  if (!lc_copy_tc_cluster_res(out, &legacy_res)) {
    lc_engine_tc_cluster_response_cleanup(&legacy_res);
    lc_engine_error_cleanup(&legacy_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate tc cluster response", NULL, NULL,
                        NULL);
  }
  {
    pslog_field fields[3];

    fields[0] = lc_log_u64_field("count", out->endpoints.count);
    fields[1] = lc_log_i64_field("expires_at", out->expires_at_unix);
    fields[2] = lc_log_str_field("cid", out->correlation_id);
    lc_log_trace(client->logger, "client.tc.cluster.list.success", fields, 3U);
  }
  lc_engine_tc_cluster_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_client_tc_rm_register_method(lc_client *self,
                                    const lc_tc_rm_register_req *req,
                                    lc_tc_rm_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_engine_tcrm_register_request legacy_req;
  lc_engine_tcrm_register_response legacy_res;
  lc_engine_error legacy_error;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "tc_rm_register requires self, req, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[2];

    fields[0] = lc_log_str_field("backend_hash", req->backend_hash);
    fields[1] = lc_log_str_field("endpoint", req->endpoint);
    lc_log_trace(client->logger, "client.rm.register.start", fields, 2U);
  }
  memset(&legacy_req, 0, sizeof(legacy_req));
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  legacy_req.backend_hash = req->backend_hash;
  legacy_req.endpoint = req->endpoint;
  rc = lc_engine_client_tcrm_register(client->legacy, &legacy_req, &legacy_res,
                                      &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("backend_hash", req->backend_hash);
      fields[1] = lc_log_str_field("endpoint", req->endpoint);
      lc_client_log_management_failure(
          client, "client.rm.register.transport_error",
          "client.rm.register.error", fields, 2U, error);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  if (!lc_copy_tc_rm_res(out, &legacy_res)) {
    lc_engine_tcrm_register_response_cleanup(&legacy_res);
    lc_engine_error_cleanup(&legacy_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate tc rm register response", NULL,
                        NULL, NULL);
  }
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("backend_hash", out->backend_hash);
    fields[1] = lc_log_u64_field("endpoints", out->endpoints.count);
    fields[2] = lc_log_i64_field("updated_at", out->updated_at_unix);
    fields[3] = lc_log_str_field("cid", out->correlation_id);
    lc_log_trace(client->logger, "client.rm.register.success", fields, 4U);
  }
  lc_engine_tcrm_register_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_client_tc_rm_unregister_method(lc_client *self,
                                      const lc_tc_rm_unregister_req *req,
                                      lc_tc_rm_res *out, lc_error *error) {
  lc_client_handle *client;
  lc_engine_tcrm_unregister_request legacy_req;
  lc_engine_tcrm_unregister_response legacy_res;
  lc_engine_error legacy_error;
  int rc;

  if (self == NULL || req == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "tc_rm_unregister requires self, req, and out", NULL,
                        NULL, NULL);
  }
  client = (lc_client_handle *)self;
  {
    pslog_field fields[2];

    fields[0] = lc_log_str_field("backend_hash", req->backend_hash);
    fields[1] = lc_log_str_field("endpoint", req->endpoint);
    lc_log_trace(client->logger, "client.rm.unregister.start", fields, 2U);
  }
  memset(&legacy_req, 0, sizeof(legacy_req));
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  legacy_req.backend_hash = req->backend_hash;
  legacy_req.endpoint = req->endpoint;
  rc = lc_engine_client_tcrm_unregister(client->legacy, &legacy_req,
                                        &legacy_res, &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("backend_hash", req->backend_hash);
      fields[1] = lc_log_str_field("endpoint", req->endpoint);
      lc_client_log_management_failure(
          client, "client.rm.unregister.transport_error",
          "client.rm.unregister.error", fields, 2U, error);
    }
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  memset(out, 0, sizeof(*out));
  out->backend_hash = lc_strdup_local(legacy_res.backend_hash);
  if (!lc_copy_string_list(&out->endpoints, &legacy_res.endpoints)) {
    lc_engine_tcrm_unregister_response_cleanup(&legacy_res);
    lc_engine_error_cleanup(&legacy_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate tc rm unregister response", NULL,
                        NULL, NULL);
  }
  out->updated_at_unix = legacy_res.updated_at_unix;
  out->correlation_id = lc_strdup_local(legacy_res.correlation_id);
  if ((legacy_res.backend_hash != NULL && out->backend_hash == NULL) ||
      (legacy_res.correlation_id != NULL && out->correlation_id == NULL)) {
    lc_tc_rm_res_cleanup(out);
    lc_engine_tcrm_unregister_response_cleanup(&legacy_res);
    lc_engine_error_cleanup(&legacy_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate tc rm unregister response", NULL,
                        NULL, NULL);
  }
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("backend_hash", out->backend_hash);
    fields[1] = lc_log_u64_field("endpoints", out->endpoints.count);
    fields[2] = lc_log_i64_field("updated_at", out->updated_at_unix);
    fields[3] = lc_log_str_field("cid", out->correlation_id);
    lc_log_trace(client->logger, "client.rm.unregister.success", fields, 4U);
  }
  lc_engine_tcrm_unregister_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

int lc_client_tc_rm_list_method(lc_client *self, lc_tc_rm_list_res *out,
                                lc_error *error) {
  lc_client_handle *client;
  lc_engine_tcrm_list_response legacy_res;
  lc_engine_error legacy_error;
  size_t index;
  int rc;

  if (self == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "tc_rm_list requires self and out", NULL, NULL, NULL);
  }
  client = (lc_client_handle *)self;
  lc_log_trace(client->logger, "client.rm.list.start", NULL, 0U);
  memset(out, 0, sizeof(*out));
  memset(&legacy_res, 0, sizeof(legacy_res));
  lc_engine_error_init(&legacy_error);
  rc = lc_engine_client_tcrm_list(client->legacy, &legacy_res, &legacy_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_legacy(error, &legacy_error);
    lc_client_log_management_failure(client, "client.rm.list.transport_error",
                                     "client.rm.list.error", NULL, 0U, error);
    lc_engine_error_cleanup(&legacy_error);
    return rc;
  }
  if (legacy_res.backend_count > 0U) {
    out->backends = (lc_tc_rm_backend *)calloc(legacy_res.backend_count,
                                               sizeof(*out->backends));
    if (out->backends == NULL) {
      lc_engine_tcrm_list_response_cleanup(&legacy_res);
      lc_engine_error_cleanup(&legacy_error);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate tc rm list backends", NULL, NULL,
                          NULL);
    }
    out->backend_count = legacy_res.backend_count;
    for (index = 0U; index < legacy_res.backend_count; ++index) {
      out->backends[index].backend_hash =
          lc_strdup_local(legacy_res.backends[index].backend_hash);
      out->backends[index].updated_at_unix =
          legacy_res.backends[index].updated_at_unix;
      if ((legacy_res.backends[index].backend_hash != NULL &&
           out->backends[index].backend_hash == NULL) ||
          !lc_copy_string_list(&out->backends[index].endpoints,
                               &legacy_res.backends[index].endpoints)) {
        lc_tc_rm_list_res_cleanup(out);
        lc_engine_tcrm_list_response_cleanup(&legacy_res);
        lc_engine_error_cleanup(&legacy_error);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate tc rm backend list", NULL, NULL,
                            NULL);
      }
    }
  }
  out->updated_at_unix = legacy_res.updated_at_unix;
  out->correlation_id = lc_strdup_local(legacy_res.correlation_id);
  if (legacy_res.correlation_id != NULL && out->correlation_id == NULL) {
    lc_tc_rm_list_res_cleanup(out);
    lc_engine_tcrm_list_response_cleanup(&legacy_res);
    lc_engine_error_cleanup(&legacy_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate tc rm list correlation_id", NULL,
                        NULL, NULL);
  }
  {
    pslog_field fields[3];

    fields[0] = lc_log_u64_field("backends", out->backend_count);
    fields[1] = lc_log_i64_field("updated_at", out->updated_at_unix);
    fields[2] = lc_log_str_field("cid", out->correlation_id);
    lc_log_trace(client->logger, "client.rm.list.success", fields, 3U);
  }
  lc_engine_tcrm_list_response_cleanup(&legacy_res);
  lc_engine_error_cleanup(&legacy_error);
  return LC_OK;
}

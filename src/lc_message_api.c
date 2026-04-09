#include "lc_api_internal.h"
#include "lc_log.h"

int lc_message_ack_method(lc_message *self, lc_error *error) {
  lc_message_handle *message;
  lc_engine_queue_ack_request engine_req;
  lc_engine_queue_ack_response engine_res;
  lc_engine_error engine_error;
  int rc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, "message ack requires self",
                        NULL, NULL, NULL);
  }
  message = (lc_message_handle *)self;
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("queue", message->queue);
    fields[1] = lc_log_str_field("namespace", message->namespace_name);
    fields[2] = lc_log_str_field("message_id", message->message_id);
    fields[3] = lc_log_str_field("lease_id", message->lease_id);
    fields[4] = lc_log_str_field("txn_id", message->txn_id);
    lc_log_trace(message->client->logger, "client.queue.ack.start", fields, 5U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.namespace_name = message->namespace_name;
  engine_req.queue = message->queue;
  engine_req.message_id = message->message_id;
  engine_req.lease_id = message->lease_id;
  engine_req.txn_id = message->txn_id;
  engine_req.fencing_token = message->fencing_token;
  engine_req.meta_etag = message->meta_etag;
  engine_req.state_etag = message->state_etag;
  engine_req.state_lease_id = message->state_lease_id;
  engine_req.state_fencing_token = message->state_fencing_token;
  rc = lc_engine_client_queue_ack(message->client->engine, &engine_req,
                                  &engine_res, &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[8];

      fields[0] = lc_log_str_field("queue", message->queue);
      fields[1] = lc_log_str_field("namespace", message->namespace_name);
      fields[2] = lc_log_str_field("message_id", message->message_id);
      fields[3] = lc_log_str_field("lease_id", message->lease_id);
      fields[4] = lc_log_str_field("txn_id", message->txn_id);
      fields[5] = lc_log_code_field(error);
      fields[6] = lc_log_http_status_field(error);
      fields[7] = lc_log_error_field("error", error);
      lc_log_warn(message->client->logger, "client.queue.ack.error", fields,
                  8U);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  {
    pslog_field fields[6];

    fields[0] = lc_log_str_field("queue", message->queue);
    fields[1] = lc_log_str_field("namespace", message->namespace_name);
    fields[2] = lc_log_str_field("message_id", message->message_id);
    fields[3] = lc_log_str_field("lease_id", message->lease_id);
    fields[4] = lc_log_bool_field("acked", engine_res.acked);
    fields[5] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(message->client->logger, "client.queue.ack.success", fields,
                 6U);
  }
  if (message->terminal_flag != NULL) {
    *message->terminal_flag = 1;
  }
  lc_engine_queue_ack_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  lc_message_close_method(self);
  return LC_OK;
}

int lc_message_nack_method(lc_message *self, const lc_nack_req *req,
                           lc_error *error) {
  lc_message_handle *message;
  lc_engine_queue_nack_request engine_req;
  lc_engine_queue_nack_response engine_res;
  lc_engine_error engine_error;
  const char *wire_intent;
  int rc;

  if (self == NULL || req == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "message nack requires self and req", NULL, NULL, NULL);
  }
  message = (lc_message_handle *)self;
  wire_intent = NULL;
  rc = lc_nack_intent_to_wire_string(req->intent, &wire_intent, error);
  if (rc != LC_OK) {
    return rc;
  }
  {
    pslog_field fields[6];

    fields[0] = lc_log_str_field("queue", message->queue);
    fields[1] = lc_log_str_field("namespace", message->namespace_name);
    fields[2] = lc_log_str_field("message_id", message->message_id);
    fields[3] = lc_log_str_field("lease_id", message->lease_id);
    fields[4] = pslog_i64("delay_seconds", (pslog_int64)req->delay_seconds);
    fields[5] =
        lc_log_str_field("intent", lc_nack_intent_to_string(req->intent));
    lc_log_trace(message->client->logger, "client.queue.nack.start", fields,
                 6U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.namespace_name = message->namespace_name;
  engine_req.queue = message->queue;
  engine_req.message_id = message->message_id;
  engine_req.lease_id = message->lease_id;
  engine_req.txn_id = message->txn_id;
  engine_req.fencing_token = message->fencing_token;
  engine_req.meta_etag = message->meta_etag;
  engine_req.state_etag = message->state_etag;
  engine_req.delay_seconds = req->delay_seconds;
  engine_req.intent = wire_intent;
  engine_req.last_error_json = req->last_error_json;
  engine_req.state_lease_id = message->state_lease_id;
  engine_req.state_fencing_token = message->state_fencing_token;
  rc = lc_engine_client_queue_nack(message->client->engine, &engine_req,
                                   &engine_res, &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[9];

      fields[0] = lc_log_str_field("queue", message->queue);
      fields[1] = lc_log_str_field("namespace", message->namespace_name);
      fields[2] = lc_log_str_field("message_id", message->message_id);
      fields[3] = lc_log_str_field("lease_id", message->lease_id);
      fields[4] = pslog_i64("delay_seconds", (pslog_int64)req->delay_seconds);
      fields[5] =
          lc_log_str_field("intent", lc_nack_intent_to_string(req->intent));
      fields[6] = lc_log_code_field(error);
      fields[7] = lc_log_http_status_field(error);
      fields[8] = lc_log_error_field("error", error);
      lc_log_warn(message->client->logger, "client.queue.nack.error", fields,
                  9U);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  {
    pslog_field fields[7];

    fields[0] = lc_log_str_field("queue", message->queue);
    fields[1] = lc_log_str_field("namespace", message->namespace_name);
    fields[2] = lc_log_str_field("message_id", message->message_id);
    fields[3] = lc_log_str_field("lease_id", message->lease_id);
    fields[4] = lc_log_bool_field("requeued", engine_res.requeued);
    fields[5] = lc_log_str_field("meta_etag", engine_res.meta_etag);
    fields[6] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(message->client->logger, "client.queue.nack.success", fields,
                 7U);
  }
  if (message->terminal_flag != NULL) {
    *message->terminal_flag = 1;
  }
  lc_engine_queue_nack_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  lc_message_close_method(self);
  return LC_OK;
}

int lc_message_extend_method(lc_message *self, const lc_extend_req *req,
                             lc_error *error) {
  lc_message_handle *message;
  lc_engine_queue_extend_request engine_req;
  lc_engine_queue_extend_response engine_res;
  lc_engine_error engine_error;
  int rc;

  if (self == NULL || req == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "message extend requires self and req", NULL, NULL,
                        NULL);
  }
  message = (lc_message_handle *)self;
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("queue", message->queue);
    fields[1] = lc_log_str_field("namespace", message->namespace_name);
    fields[2] = lc_log_str_field("message_id", message->message_id);
    fields[3] = lc_log_str_field("lease_id", message->lease_id);
    fields[4] =
        pslog_i64("extend_by_seconds", (pslog_int64)req->extend_by_seconds);
    lc_log_trace(message->client->logger, "client.queue.extend.start", fields,
                 5U);
  }
  memset(&engine_req, 0, sizeof(engine_req));
  memset(&engine_res, 0, sizeof(engine_res));
  lc_engine_error_init(&engine_error);
  engine_req.namespace_name = message->namespace_name;
  engine_req.queue = message->queue;
  engine_req.message_id = message->message_id;
  engine_req.lease_id = message->lease_id;
  engine_req.txn_id = message->txn_id;
  engine_req.fencing_token = message->fencing_token;
  engine_req.meta_etag = message->meta_etag;
  engine_req.extend_by_seconds = req->extend_by_seconds;
  engine_req.state_lease_id = message->state_lease_id;
  engine_req.state_fencing_token = message->state_fencing_token;
  rc = lc_engine_client_queue_extend(message->client->engine, &engine_req,
                                     &engine_res, &engine_error);
  if (rc != LC_ENGINE_OK) {
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[8];

      fields[0] = lc_log_str_field("queue", message->queue);
      fields[1] = lc_log_str_field("namespace", message->namespace_name);
      fields[2] = lc_log_str_field("message_id", message->message_id);
      fields[3] = lc_log_str_field("lease_id", message->lease_id);
      fields[4] =
          pslog_i64("extend_by_seconds", (pslog_int64)req->extend_by_seconds);
      fields[5] = lc_log_code_field(error);
      fields[6] = lc_log_http_status_field(error);
      fields[7] = lc_log_error_field("error", error);
      lc_log_warn(message->client->logger, "client.queue.extend.error", fields,
                  8U);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  lc_client_free(message->client, message->meta_etag);
  message->meta_etag = lc_client_strdup(message->client, engine_res.meta_etag);
  message->lease_expires_at_unix = engine_res.lease_expires_at_unix;
  message->visibility_timeout_seconds = engine_res.visibility_timeout_seconds;
  message->state_lease_expires_at_unix = engine_res.state_lease_expires_at_unix;
  message->pub.meta_etag = message->meta_etag;
  message->pub.lease_expires_at_unix = message->lease_expires_at_unix;
  message->pub.visibility_timeout_seconds = message->visibility_timeout_seconds;
  if (message->state_lease != NULL) {
    ((lc_lease_handle *)message->state_lease)->lease_expires_at_unix =
        engine_res.state_lease_expires_at_unix;
    message->state_lease->lease_expires_at_unix =
        engine_res.state_lease_expires_at_unix;
  }
  {
    pslog_field fields[7];

    fields[0] = lc_log_str_field("queue", message->queue);
    fields[1] = lc_log_str_field("namespace", message->namespace_name);
    fields[2] = lc_log_str_field("message_id", message->message_id);
    fields[3] = lc_log_str_field("lease_id", message->lease_id);
    fields[4] = pslog_i64("lease_expires_at",
                          (pslog_int64)engine_res.lease_expires_at_unix);
    fields[5] = pslog_i64("visibility_timeout_seconds",
                          (pslog_int64)engine_res.visibility_timeout_seconds);
    fields[6] = lc_log_str_field("cid", engine_res.correlation_id);
    lc_log_trace(message->client->logger, "client.queue.extend.success", fields,
                 7U);
  }
  lc_engine_queue_extend_response_cleanup(&engine_res);
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

lc_lease *lc_message_state_method(lc_message *self) {
  lc_message_handle *message;

  if (self == NULL) {
    return NULL;
  }
  message = (lc_message_handle *)self;
  return message->state_lease;
}

lc_source *lc_message_payload_reader_method(lc_message *self) {
  lc_message_handle *message;

  if (self == NULL) {
    return NULL;
  }
  message = (lc_message_handle *)self;
  return message->payload;
}

int lc_message_payload_json_method(lc_message *self, lc_json **out,
                                   lc_error *error) {
  lc_message_handle *message;
  lc_source *payload;

  if (self == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "message payload_json requires self and out", NULL,
                        NULL, NULL);
  }
  message = (lc_message_handle *)self;
  payload = message->payload;
  if (payload == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, "message has no payload",
                        NULL, NULL, NULL);
  }
  message->payload = NULL;
  message->pub.payload = NULL;
  return lc_json_from_source(payload, out, error);
}

int lc_message_rewind_payload_method(lc_message *self, lc_error *error) {
  lc_message_handle *message;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "message rewind_payload requires self", NULL, NULL,
                        NULL);
  }
  message = (lc_message_handle *)self;
  if (message->payload == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, "message has no payload",
                        NULL, NULL, NULL);
  }
  return message->payload->reset(message->payload, error);
}

int lc_message_write_payload_method(lc_message *self, lc_sink *dst,
                                    size_t *written, lc_error *error) {
  lc_message_handle *message;

  if (self == NULL || dst == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "message write_payload requires self and dst", NULL,
                        NULL, NULL);
  }
  message = (lc_message_handle *)self;
  if (message->payload == NULL) {
    if (written != NULL) {
      *written = 0U;
    }
    return LC_OK;
  }
  return lc_copy(message->payload, dst, written, error);
}

void lc_message_close_method(lc_message *self) {
  lc_message_handle *message;

  if (self == NULL) {
    return;
  }
  message = (lc_message_handle *)self;
  lc_client_free(message->client, message->namespace_name);
  lc_client_free(message->client, message->queue);
  lc_client_free(message->client, message->message_id);
  lc_client_free(message->client, message->payload_content_type);
  lc_client_free(message->client, message->correlation_id);
  lc_client_free(message->client, message->lease_id);
  lc_client_free(message->client, message->txn_id);
  lc_client_free(message->client, message->meta_etag);
  lc_client_free(message->client, message->next_cursor);
  lc_client_free(message->client, message->state_etag);
  lc_client_free(message->client, message->state_lease_id);
  lc_client_free(message->client, message->state_txn_id);
  if (message->payload != NULL) {
    message->payload->close(message->payload);
  }
  if (message->state_lease != NULL) {
    message->state_lease->close(message->state_lease);
  }
  lc_client_free(message->client, message);
}

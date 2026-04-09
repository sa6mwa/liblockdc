#include "lc_log.h"
#include "lc_mutate_stream.h"

#include <errno.h>

static int lc_local_mutate_writer(void *context, const void *bytes,
                                  size_t count, lc_engine_error *error) {
  FILE *fp;
  (void)error;

  fp = (FILE *)context;
  if (count == 0U) {
    return 1;
  }
  if (fwrite(bytes, 1U, count, fp) != count) {
    return LC_ENGINE_ERROR_TRANSPORT;
  }
  return 1;
}

static int lc_local_mutate_write_empty_object(FILE *fp, lc_error *error) {
  if (fwrite("{}", 1U, 2U, fp) != 2U) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to seed empty local mutate document",
                        strerror(errno), NULL, NULL);
  }
  if (fflush(fp) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to flush empty local mutate document",
                        strerror(errno), NULL, NULL);
  }
  rewind(fp);
  return LC_OK;
}

int lc_lease_mutate_local_method(lc_lease *self, const lc_mutate_local_req *req,
                                 lc_error *error) {
  lc_lease_handle *lease;
  lc_engine_get_request get_req;
  lc_engine_get_stream_response get_res;
  lc_engine_error engine_error;
  lc_mutation_parse_options parse_options;
  lc_mutation_plan *plan;
  lc_update_opts update_opts;
  lc_source *source;
  lc_json *json;
  FILE *input_fp;
  FILE *final_fp;
  int rc;

  if (self == NULL || req == NULL || req->mutations == NULL ||
      req->mutation_count == 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lease mutate_local requires self and mutations", NULL,
                        NULL, NULL);
  }

  lease = (lc_lease_handle *)self;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] = lc_log_i64_field("mutation_count", (long)req->mutation_count);
    lc_log_trace(lease->client->logger, "client.mutate_local.start", fields,
                 4U);
  }
  memset(&get_req, 0, sizeof(get_req));
  memset(&get_res, 0, sizeof(get_res));
  memset(&parse_options, 0, sizeof(parse_options));
  lc_update_opts_init(&update_opts);
  lc_engine_error_init(&engine_error);
  plan = NULL;
  source = NULL;
  json = NULL;
  final_fp = NULL;

  parse_options.file_value_base_dir = req->file_value_base_dir;
  parse_options.file_value_resolver = req->file_value_resolver;
  if (clock_gettime(CLOCK_REALTIME, &parse_options.now) == 0) {
    parse_options.has_now = 1;
  }
  rc = lc_mutation_plan_build(req->mutations, req->mutation_count,
                              &parse_options, &plan, error);
  if (rc != LC_OK) {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] = lc_log_i64_field("mutation_count", (long)req->mutation_count);
    fields[4] = lc_log_error_field("error", error);
    lc_log_warn(lease->client->logger, "client.mutate_local.error", fields, 5U);
    return rc;
  }

  input_fp = tmpfile();
  if (input_fp == NULL) {
    lc_mutation_plan_close(plan);
    {
      pslog_field fields[5];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      fields[3] = lc_log_i64_field("mutation_count", (long)req->mutation_count);
      fields[4] = lc_log_str_field("step", "tmpfile");
      lc_log_error(lease->client->logger, "client.mutate_local.error", fields,
                   5U);
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create local mutate input scratch file",
                        strerror(errno), NULL, NULL);
  }

  get_req.namespace_name = lease->namespace_name;
  get_req.key = lease->key;
  get_req.lease_id = lease->lease_id;
  get_req.fencing_token = lease->fencing_token;
  get_req.public_read = 0;
  rc = lc_engine_client_get_into(lease->client->engine, &get_req,
                                 lc_local_mutate_writer, input_fp, &get_res,
                                 &engine_error);
  if (rc != LC_ENGINE_OK) {
    fclose(input_fp);
    lc_mutation_plan_close(plan);
    rc = lc_error_from_engine(error, &engine_error);
    {
      pslog_field fields[6];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      fields[3] = lc_log_i64_field("mutation_count", (long)req->mutation_count);
      fields[4] = lc_log_str_field("step", "get");
      fields[5] = lc_log_error_field("error", error);
      lc_log_warn(lease->client->logger, "client.mutate_local.error", fields,
                  6U);
    }
    lc_engine_error_cleanup(&engine_error);
    return rc;
  }
  if (fflush(input_fp) != 0) {
    fclose(input_fp);
    lc_mutation_plan_close(plan);
    lc_engine_get_stream_response_cleanup(&get_res);
    lc_engine_error_cleanup(&engine_error);
    {
      pslog_field fields[5];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      fields[3] = lc_log_i64_field("mutation_count", (long)req->mutation_count);
      fields[4] = lc_log_str_field("step", "flush_input");
      lc_log_error(lease->client->logger, "client.mutate_local.error", fields,
                   5U);
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to flush local mutate input scratch file",
                        strerror(errno), NULL, NULL);
  }
  rewind(input_fp);

  if (get_res.no_content) {
    if (lc_local_mutate_write_empty_object(input_fp, error) != LC_OK) {
      fclose(input_fp);
      lc_mutation_plan_close(plan);
      lc_engine_get_stream_response_cleanup(&get_res);
      lc_engine_error_cleanup(&engine_error);
      {
        pslog_field fields[5];

        fields[0] = lc_log_str_field("key", lease->key);
        fields[1] = lc_log_str_field("lease_id", lease->lease_id);
        fields[2] = lc_log_str_field("txn_id", lease->txn_id);
        fields[3] =
            lc_log_i64_field("mutation_count", (long)req->mutation_count);
        fields[4] = lc_log_str_field("step", "seed_empty");
        lc_log_error(lease->client->logger, "client.mutate_local.error", fields,
                     5U);
      }
      return LC_ERR_TRANSPORT;
    }
  }

  rc = lc_mutation_plan_apply(plan, input_fp, &final_fp, error);
  fclose(input_fp);
  lc_mutation_plan_close(plan);
  if (rc != LC_OK) {
    lc_engine_get_stream_response_cleanup(&get_res);
    lc_engine_error_cleanup(&engine_error);
    {
      pslog_field fields[6];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      fields[3] = lc_log_i64_field("mutation_count", (long)req->mutation_count);
      fields[4] = lc_log_str_field("step", "apply");
      fields[5] = lc_log_error_field("error", error);
      lc_log_warn(lease->client->logger, "client.mutate_local.error", fields,
                  6U);
    }
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
    lc_engine_get_stream_response_cleanup(&get_res);
    lc_engine_error_cleanup(&engine_error);
    {
      pslog_field fields[5];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      fields[3] = lc_log_i64_field("mutation_count", (long)req->mutation_count);
      fields[4] = lc_log_str_field("step", "source_wrap");
      lc_log_error(lease->client->logger, "client.mutate_local.error", fields,
                   5U);
    }
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to wrap local mutate output stream", NULL, NULL,
                        NULL);
  }
  rc = lc_json_from_source(source, &json, error);
  if (rc != LC_OK) {
    source->close(source);
    fclose(final_fp);
    lc_engine_get_stream_response_cleanup(&get_res);
    lc_engine_error_cleanup(&engine_error);
    {
      pslog_field fields[6];

      fields[0] = lc_log_str_field("key", lease->key);
      fields[1] = lc_log_str_field("lease_id", lease->lease_id);
      fields[2] = lc_log_str_field("txn_id", lease->txn_id);
      fields[3] = lc_log_i64_field("mutation_count", (long)req->mutation_count);
      fields[4] = lc_log_str_field("step", "json_wrap");
      fields[5] = lc_log_error_field("error", error);
      lc_log_warn(lease->client->logger, "client.mutate_local.error", fields,
                  6U);
    }
    return rc;
  }
  rc = lc_lease_update_method(self, json, &update_opts, error);
  json->close(json);
  fclose(final_fp);
  lc_engine_get_stream_response_cleanup(&get_res);
  lc_engine_error_cleanup(&engine_error);
  if (rc != LC_OK) {
    pslog_field fields[6];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] = lc_log_i64_field("mutation_count", (long)req->mutation_count);
    fields[4] = lc_log_str_field("step", "update");
    fields[5] = lc_log_error_field("error", error);
    lc_log_warn(lease->client->logger, "client.mutate_local.error", fields, 6U);
    return rc;
  }
  {
    pslog_field fields[6];

    fields[0] = lc_log_str_field("key", lease->key);
    fields[1] = lc_log_str_field("lease_id", lease->lease_id);
    fields[2] = lc_log_str_field("txn_id", lease->txn_id);
    fields[3] = lc_log_i64_field("mutation_count", (long)req->mutation_count);
    fields[4] = lc_log_i64_field("new_version", lease->version);
    fields[5] = lc_log_str_field("new_etag", lease->state_etag);
    lc_log_trace(lease->client->logger, "client.mutate_local.success", fields,
                 6U);
  }
  return rc;
}

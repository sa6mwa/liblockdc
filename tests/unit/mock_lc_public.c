#include "mock_lc_public.h"

#include <string.h>

static void lc_public_mock_record(lc_public_mock_call *call, const void *arg1,
                                  const void *arg2, const void *arg3,
                                  const void *arg4, const void *arg5,
                                  const void *arg6) {
  call->count += 1;
  call->arg1 = arg1;
  call->arg2 = arg2;
  call->arg3 = arg3;
  call->arg4 = arg4;
  call->arg5 = arg5;
  call->arg6 = arg6;
}

static size_t mock_source_read(lc_source *self, void *buffer, size_t count,
                               lc_error *error) {
  lc_public_mock_source *mock;
  size_t remaining;
  size_t copied;

  (void)error;
  mock = (lc_public_mock_source *)self;
  mock->read_calls += 1;
  if (mock->read_bytes == NULL || mock->read_offset >= mock->read_length) {
    return 0U;
  }
  remaining = mock->read_length - mock->read_offset;
  copied = remaining < count ? remaining : count;
  memcpy(buffer, (const unsigned char *)mock->read_bytes + mock->read_offset,
         copied);
  mock->read_offset += copied;
  return copied;
}

static int mock_source_reset(lc_source *self, lc_error *error) {
  lc_public_mock_source *mock;

  (void)error;
  mock = (lc_public_mock_source *)self;
  mock->reset_calls += 1;
  mock->read_offset = 0U;
  return LC_OK;
}

static void mock_source_close(lc_source *self) {
  lc_public_mock_source *mock;

  mock = (lc_public_mock_source *)self;
  mock->close_calls += 1;
}

static int mock_sink_write(lc_sink *self, const void *bytes, size_t count,
                           lc_error *error) {
  lc_public_mock_sink *mock;

  (void)error;
  mock = (lc_public_mock_sink *)self;
  mock->write_calls += 1;
  mock->last_write_bytes = bytes;
  mock->last_write_count = count;
  return 1;
}

static void mock_sink_close(lc_sink *self) {
  lc_public_mock_sink *mock;

  mock = (lc_public_mock_sink *)self;
  mock->close_calls += 1;
}

static int mock_consumer_service_run(lc_consumer_service *self,
                                     lc_error *error) {
  lc_public_mock_consumer_service *mock;

  mock = (lc_public_mock_consumer_service *)self;
  lc_public_mock_record(&mock->run_call, self, error, NULL, NULL, NULL, NULL);
  return mock->rc;
}

static int mock_consumer_service_start(lc_consumer_service *self,
                                       lc_error *error) {
  lc_public_mock_consumer_service *mock;

  mock = (lc_public_mock_consumer_service *)self;
  lc_public_mock_record(&mock->start_call, self, error, NULL, NULL, NULL, NULL);
  return mock->rc;
}

static int mock_consumer_service_stop(lc_consumer_service *self) {
  lc_public_mock_consumer_service *mock;

  mock = (lc_public_mock_consumer_service *)self;
  lc_public_mock_record(&mock->stop_call, self, NULL, NULL, NULL, NULL, NULL);
  return mock->rc;
}

static int mock_consumer_service_wait(lc_consumer_service *self,
                                      lc_error *error) {
  lc_public_mock_consumer_service *mock;

  mock = (lc_public_mock_consumer_service *)self;
  lc_public_mock_record(&mock->wait_call, self, error, NULL, NULL, NULL, NULL);
  return mock->rc;
}

static void mock_consumer_service_close(lc_consumer_service *self) {
  lc_public_mock_consumer_service *mock;

  mock = (lc_public_mock_consumer_service *)self;
  mock->close_calls += 1;
}

static int mock_lease_describe(lc_lease *self, lc_error *error) {
  lc_public_mock_lease *mock;

  mock = (lc_public_mock_lease *)self;
  lc_public_mock_record(&mock->describe_call, self, error, NULL, NULL, NULL,
                        NULL);
  return mock->rc;
}

static int mock_lease_get(lc_lease *self, lc_sink *dst, const lc_get_opts *opts,
                          lc_get_res *out, lc_error *error) {
  lc_public_mock_lease *mock;

  mock = (lc_public_mock_lease *)self;
  lc_public_mock_record(&mock->get_call, self, dst, opts, out, error, NULL);
  return mock->rc;
}

static int mock_lease_load(lc_lease *self, const lonejson_map *map, void *dst,
                           const lonejson_parse_options *parse_options,
                           const lc_get_opts *opts, lc_get_res *out,
                           lc_error *error) {
  lc_public_mock_lease *mock;

  mock = (lc_public_mock_lease *)self;
  lc_public_mock_record(&mock->load_call, self, map, dst, parse_options, opts,
                        out);
  mock->load_call.arg6 = error;
  return mock->rc;
}

static int mock_lease_save(lc_lease *self, const lonejson_map *map,
                           const void *src,
                           const lonejson_write_options *write_options,
                           lc_error *error) {
  lc_public_mock_lease *mock;

  mock = (lc_public_mock_lease *)self;
  lc_public_mock_record(&mock->save_call, self, map, src, write_options, error,
                        NULL);
  return mock->rc;
}

static int mock_lease_update(lc_lease *self, lc_source *src,
                             const lc_update_opts *opts, lc_error *error) {
  lc_public_mock_lease *mock;

  mock = (lc_public_mock_lease *)self;
  lc_public_mock_record(&mock->update_call, self, src, opts, error, NULL, NULL);
  return mock->rc;
}

static int mock_lease_mutate(lc_lease *self, const lc_mutate_req *req,
                             lc_error *error) {
  lc_public_mock_lease *mock;

  mock = (lc_public_mock_lease *)self;
  lc_public_mock_record(&mock->mutate_call, self, req, error, NULL, NULL, NULL);
  return mock->rc;
}

static int mock_lease_mutate_local(lc_lease *self,
                                   const lc_mutate_local_req *req,
                                   lc_error *error) {
  lc_public_mock_lease *mock;

  mock = (lc_public_mock_lease *)self;
  lc_public_mock_record(&mock->mutate_local_call, self, req, error, NULL, NULL,
                        NULL);
  return mock->rc;
}

static int mock_lease_metadata(lc_lease *self, const lc_metadata_req *req,
                               lc_error *error) {
  lc_public_mock_lease *mock;

  mock = (lc_public_mock_lease *)self;
  lc_public_mock_record(&mock->metadata_call, self, req, error, NULL, NULL,
                        NULL);
  return mock->rc;
}

static int mock_lease_remove(lc_lease *self, const lc_remove_req *req,
                             lc_error *error) {
  lc_public_mock_lease *mock;

  mock = (lc_public_mock_lease *)self;
  lc_public_mock_record(&mock->remove_call, self, req, error, NULL, NULL, NULL);
  return mock->rc;
}

static int mock_lease_keepalive(lc_lease *self, const lc_keepalive_req *req,
                                lc_error *error) {
  lc_public_mock_lease *mock;

  mock = (lc_public_mock_lease *)self;
  lc_public_mock_record(&mock->keepalive_call, self, req, error, NULL, NULL,
                        NULL);
  return mock->rc;
}

static int mock_lease_release(lc_lease *self, const lc_release_req *req,
                              lc_error *error) {
  lc_public_mock_lease *mock;

  mock = (lc_public_mock_lease *)self;
  lc_public_mock_record(&mock->release_call, self, req, error, NULL, NULL,
                        NULL);
  return mock->rc;
}

static int mock_lease_attach(lc_lease *self, const lc_attach_req *req,
                             lc_source *src, lc_attach_res *out,
                             lc_error *error) {
  lc_public_mock_lease *mock;

  mock = (lc_public_mock_lease *)self;
  lc_public_mock_record(&mock->attach_call, self, req, src, out, error, NULL);
  return mock->rc;
}

static int mock_lease_list_attachments(lc_lease *self, lc_attachment_list *out,
                                       lc_error *error) {
  lc_public_mock_lease *mock;

  mock = (lc_public_mock_lease *)self;
  lc_public_mock_record(&mock->list_attachments_call, self, out, error, NULL,
                        NULL, NULL);
  return mock->rc;
}

static int mock_lease_get_attachment(lc_lease *self,
                                     const lc_attachment_get_req *req,
                                     lc_sink *dst, lc_attachment_get_res *out,
                                     lc_error *error) {
  lc_public_mock_lease *mock;

  mock = (lc_public_mock_lease *)self;
  lc_public_mock_record(&mock->get_attachment_call, self, req, dst, out, error,
                        NULL);
  return mock->rc;
}

static int mock_lease_delete_attachment(lc_lease *self,
                                        const lc_attachment_selector *selector,
                                        int *deleted, lc_error *error) {
  lc_public_mock_lease *mock;

  mock = (lc_public_mock_lease *)self;
  lc_public_mock_record(&mock->delete_attachment_call, self, selector, deleted,
                        error, NULL, NULL);
  if (deleted != NULL) {
    *deleted = mock->deleted_value;
  }
  return mock->rc;
}

static int mock_lease_delete_all_attachments(lc_lease *self, int *deleted_count,
                                             lc_error *error) {
  lc_public_mock_lease *mock;

  mock = (lc_public_mock_lease *)self;
  lc_public_mock_record(&mock->delete_all_attachments_call, self, deleted_count,
                        error, NULL, NULL, NULL);
  if (deleted_count != NULL) {
    *deleted_count = mock->deleted_count_value;
  }
  return mock->rc;
}

static void mock_lease_close(lc_lease *self) {
  lc_public_mock_lease *mock;

  mock = (lc_public_mock_lease *)self;
  mock->close_calls += 1;
}

static int mock_message_ack(lc_message *self, lc_error *error) {
  lc_public_mock_message *mock;

  mock = (lc_public_mock_message *)self;
  lc_public_mock_record(&mock->ack_call, self, error, NULL, NULL, NULL, NULL);
  return mock->rc;
}

static int mock_message_nack(lc_message *self, const lc_nack_req *req,
                             lc_error *error) {
  lc_public_mock_message *mock;

  mock = (lc_public_mock_message *)self;
  lc_public_mock_record(&mock->nack_call, self, req, error, NULL, NULL, NULL);
  return mock->rc;
}

static int mock_message_extend(lc_message *self, const lc_extend_req *req,
                               lc_error *error) {
  lc_public_mock_message *mock;

  mock = (lc_public_mock_message *)self;
  lc_public_mock_record(&mock->extend_call, self, req, error, NULL, NULL, NULL);
  return mock->rc;
}

static lc_lease *mock_message_state(lc_message *self) {
  lc_public_mock_message *mock;

  mock = (lc_public_mock_message *)self;
  lc_public_mock_record(&mock->state_call, self, NULL, NULL, NULL, NULL, NULL);
  return mock->state_to_return;
}

static lc_source *mock_message_payload_reader(lc_message *self) {
  lc_public_mock_message *mock;

  mock = (lc_public_mock_message *)self;
  lc_public_mock_record(&mock->payload_reader_call, self, NULL, NULL, NULL,
                        NULL, NULL);
  return mock->payload_reader_to_return;
}

static int mock_message_rewind_payload(lc_message *self, lc_error *error) {
  lc_public_mock_message *mock;

  mock = (lc_public_mock_message *)self;
  lc_public_mock_record(&mock->rewind_payload_call, self, error, NULL, NULL,
                        NULL, NULL);
  return mock->rc;
}

static int mock_message_write_payload(lc_message *self, lc_sink *dst,
                                      size_t *written, lc_error *error) {
  lc_public_mock_message *mock;

  mock = (lc_public_mock_message *)self;
  lc_public_mock_record(&mock->write_payload_call, self, dst, written, error,
                        NULL, NULL);
  if (written != NULL) {
    *written = mock->payload_written;
  }
  return mock->rc;
}

static void mock_message_close(lc_message *self) {
  lc_public_mock_message *mock;

  mock = (lc_public_mock_message *)self;
  mock->close_calls += 1;
}

static int mock_client_acquire(lc_client *self, const lc_acquire_req *req,
                               lc_lease **out, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->acquire_call, self, req, out, error, NULL, NULL);
  if (out != NULL) {
    *out = mock->lease_to_return;
  }
  return mock->rc;
}

static int mock_client_describe(lc_client *self, const lc_describe_req *req,
                                lc_describe_res *out, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->describe_call, self, req, out, error, NULL,
                        NULL);
  return mock->rc;
}

static int mock_client_get(lc_client *self, const char *key,
                           const lc_get_opts *opts, lc_sink *dst,
                           lc_get_res *out, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->get_call, self, key, opts, dst, out, error);
  return mock->rc;
}

static int mock_client_load(lc_client *self, const char *key,
                            const lonejson_map *map, void *dst,
                            const lonejson_parse_options *parse_options,
                            const lc_get_opts *opts, lc_get_res *out,
                            lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->load_call, self, key, map, dst, parse_options,
                        opts);
  mock->load_call.arg6 = out;
  return mock->rc;
}

static int mock_client_update(lc_client *self, const lc_update_req *req,
                              lc_source *src, lc_update_res *out,
                              lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->update_call, self, req, src, out, error, NULL);
  return mock->rc;
}

static int mock_client_mutate(lc_client *self, const lc_mutate_op *req,
                              lc_mutate_res *out, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->mutate_call, self, req, out, error, NULL, NULL);
  return mock->rc;
}

static int mock_client_metadata(lc_client *self, const lc_metadata_op *req,
                                lc_metadata_res *out, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->metadata_call, self, req, out, error, NULL,
                        NULL);
  return mock->rc;
}

static int mock_client_remove(lc_client *self, const lc_remove_op *req,
                              lc_remove_res *out, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->remove_call, self, req, out, error, NULL, NULL);
  return mock->rc;
}

static int mock_client_keepalive(lc_client *self, const lc_keepalive_op *req,
                                 lc_keepalive_res *out, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->keepalive_call, self, req, out, error, NULL,
                        NULL);
  return mock->rc;
}

static int mock_client_release(lc_client *self, const lc_release_op *req,
                               lc_release_res *out, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->release_call, self, req, out, error, NULL, NULL);
  return mock->rc;
}

static int mock_client_attach(lc_client *self, const lc_attach_op *req,
                              lc_source *src, lc_attach_res *out,
                              lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->attach_call, self, req, src, out, error, NULL);
  return mock->rc;
}

static int mock_client_list_attachments(lc_client *self,
                                        const lc_attachment_list_req *req,
                                        lc_attachment_list *out,
                                        lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->list_attachments_call, self, req, out, error,
                        NULL, NULL);
  return mock->rc;
}

static int mock_client_get_attachment(lc_client *self,
                                      const lc_attachment_get_op *req,
                                      lc_sink *dst, lc_attachment_get_res *out,
                                      lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->get_attachment_call, self, req, dst, out, error,
                        NULL);
  return mock->rc;
}

static int mock_client_delete_attachment(lc_client *self,
                                         const lc_attachment_delete_op *req,
                                         int *deleted, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->delete_attachment_call, self, req, deleted,
                        error, NULL, NULL);
  if (deleted != NULL) {
    *deleted = mock->deleted_value;
  }
  return mock->rc;
}

static int
mock_client_delete_all_attachments(lc_client *self,
                                   const lc_attachment_delete_all_op *req,
                                   int *deleted_count, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->delete_all_attachments_call, self, req,
                        deleted_count, error, NULL, NULL);
  if (deleted_count != NULL) {
    *deleted_count = mock->deleted_count_value;
  }
  return mock->rc;
}

static int mock_client_queue_stats(lc_client *self,
                                   const lc_queue_stats_req *req,
                                   lc_queue_stats_res *out, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->queue_stats_call, self, req, out, error, NULL,
                        NULL);
  return mock->rc;
}

static int mock_client_queue_ack(lc_client *self, const lc_ack_op *req,
                                 lc_ack_res *out, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->queue_ack_call, self, req, out, error, NULL,
                        NULL);
  return mock->rc;
}

static int mock_client_queue_nack(lc_client *self, const lc_nack_op *req,
                                  lc_nack_res *out, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->queue_nack_call, self, req, out, error, NULL,
                        NULL);
  return mock->rc;
}

static int mock_client_queue_extend(lc_client *self, const lc_extend_op *req,
                                    lc_extend_res *out, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->queue_extend_call, self, req, out, error, NULL,
                        NULL);
  return mock->rc;
}

static int mock_client_query(lc_client *self, const lc_query_req *req,
                             lc_sink *dst, lc_query_res *out, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->query_call, self, req, dst, out, error, NULL);
  return mock->rc;
}

static int mock_client_query_keys(lc_client *self, const lc_query_req *req,
                                  const lc_query_key_handler *handler,
                                  void *context, lc_query_res *out,
                                  lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->query_keys_call, self, req, handler, context,
                        out, error);
  return mock->rc;
}

static int mock_client_get_namespace_config(lc_client *self,
                                            const lc_namespace_config_req *req,
                                            lc_namespace_config_res *out,
                                            lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->get_namespace_config_call, self, req, out, error,
                        NULL, NULL);
  return mock->rc;
}

static int mock_client_update_namespace_config(
    lc_client *self, const lc_namespace_config_req *req,
    lc_namespace_config_res *out, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->update_namespace_config_call, self, req, out,
                        error, NULL, NULL);
  return mock->rc;
}

static int mock_client_flush_index(lc_client *self,
                                   const lc_index_flush_req *req,
                                   lc_index_flush_res *out, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->flush_index_call, self, req, out, error, NULL,
                        NULL);
  return mock->rc;
}

static int mock_client_txn_replay(lc_client *self, const lc_txn_replay_req *req,
                                  lc_txn_replay_res *out, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->txn_replay_call, self, req, out, error, NULL,
                        NULL);
  return mock->rc;
}

static int mock_client_txn_prepare(lc_client *self,
                                   const lc_txn_decision_req *req,
                                   lc_txn_decision_res *out, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->txn_prepare_call, self, req, out, error, NULL,
                        NULL);
  return mock->rc;
}

static int mock_client_txn_commit(lc_client *self,
                                  const lc_txn_decision_req *req,
                                  lc_txn_decision_res *out, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->txn_commit_call, self, req, out, error, NULL,
                        NULL);
  return mock->rc;
}

static int mock_client_txn_rollback(lc_client *self,
                                    const lc_txn_decision_req *req,
                                    lc_txn_decision_res *out, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->txn_rollback_call, self, req, out, error, NULL,
                        NULL);
  return mock->rc;
}

static int mock_client_tc_lease_acquire(lc_client *self,
                                        const lc_tc_lease_acquire_req *req,
                                        lc_tc_lease_acquire_res *out,
                                        lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->tc_lease_acquire_call, self, req, out, error,
                        NULL, NULL);
  return mock->rc;
}

static int mock_client_tc_lease_renew(lc_client *self,
                                      const lc_tc_lease_renew_req *req,
                                      lc_tc_lease_renew_res *out,
                                      lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->tc_lease_renew_call, self, req, out, error, NULL,
                        NULL);
  return mock->rc;
}

static int mock_client_tc_lease_release(lc_client *self,
                                        const lc_tc_lease_release_req *req,
                                        lc_tc_lease_release_res *out,
                                        lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->tc_lease_release_call, self, req, out, error,
                        NULL, NULL);
  return mock->rc;
}

static int mock_client_tc_leader(lc_client *self, lc_tc_leader_res *out,
                                 lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->tc_leader_call, self, out, error, NULL, NULL,
                        NULL);
  return mock->rc;
}

static int
mock_client_tc_cluster_announce(lc_client *self,
                                const lc_tc_cluster_announce_req *req,
                                lc_tc_cluster_res *out, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->tc_cluster_announce_call, self, req, out, error,
                        NULL, NULL);
  return mock->rc;
}

static int mock_client_tc_cluster_leave(lc_client *self, lc_tc_cluster_res *out,
                                        lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->tc_cluster_leave_call, self, out, error, NULL,
                        NULL, NULL);
  return mock->rc;
}

static int mock_client_tc_cluster_list(lc_client *self, lc_tc_cluster_res *out,
                                       lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->tc_cluster_list_call, self, out, error, NULL,
                        NULL, NULL);
  return mock->rc;
}

static int mock_client_tc_rm_register(lc_client *self,
                                      const lc_tc_rm_register_req *req,
                                      lc_tc_rm_res *out, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->tc_rm_register_call, self, req, out, error, NULL,
                        NULL);
  return mock->rc;
}

static int mock_client_tc_rm_unregister(lc_client *self,
                                        const lc_tc_rm_unregister_req *req,
                                        lc_tc_rm_res *out, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->tc_rm_unregister_call, self, req, out, error,
                        NULL, NULL);
  return mock->rc;
}

static int mock_client_tc_rm_list(lc_client *self, lc_tc_rm_list_res *out,
                                  lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->tc_rm_list_call, self, out, error, NULL, NULL,
                        NULL);
  return mock->rc;
}

static int mock_client_enqueue(lc_client *self, const lc_enqueue_req *req,
                               lc_source *src, lc_enqueue_res *out,
                               lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->enqueue_call, self, req, src, out, error, NULL);
  return mock->rc;
}

static int mock_client_dequeue(lc_client *self, const lc_dequeue_req *req,
                               lc_message **out, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->dequeue_call, self, req, out, error, NULL, NULL);
  if (out != NULL) {
    *out = mock->message_to_return;
  }
  return mock->rc;
}

static int mock_client_dequeue_batch(lc_client *self, const lc_dequeue_req *req,
                                     lc_dequeue_batch_res *out,
                                     lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->dequeue_batch_call, self, req, out, error, NULL,
                        NULL);
  return mock->rc;
}

static int mock_client_dequeue_with_state(lc_client *self,
                                          const lc_dequeue_req *req,
                                          lc_message **out, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->dequeue_with_state_call, self, req, out, error,
                        NULL, NULL);
  if (out != NULL) {
    *out = mock->message_to_return;
  }
  return mock->rc;
}

static int mock_client_subscribe(lc_client *self, const lc_dequeue_req *req,
                                 const lc_consumer *consumer, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->subscribe_call, self, req, consumer, error, NULL,
                        NULL);
  return mock->rc;
}

static int mock_client_subscribe_with_state(lc_client *self,
                                            const lc_dequeue_req *req,
                                            const lc_consumer *consumer,
                                            lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->subscribe_with_state_call, self, req, consumer,
                        error, NULL, NULL);
  return mock->rc;
}

static int
mock_client_new_consumer_service(lc_client *self,
                                 const lc_consumer_service_config *config,
                                 lc_consumer_service **out, lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->new_consumer_service_call, self, config, out,
                        error, NULL, NULL);
  if (out != NULL) {
    *out = mock->service_to_return;
  }
  return mock->rc;
}

static int mock_client_watch_queue(lc_client *self,
                                   const lc_watch_queue_req *req,
                                   const lc_watch_handler *handler,
                                   lc_error *error) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  lc_public_mock_record(&mock->watch_queue_call, self, req, handler, error,
                        NULL, NULL);
  return mock->rc;
}

static void mock_client_close(lc_client *self) {
  lc_public_mock_client *mock;

  mock = (lc_public_mock_client *)self;
  mock->close_calls += 1;
}

void lc_public_mock_source_init(lc_public_mock_source *mock) {
  memset(mock, 0, sizeof(*mock));
  mock->pub.read = mock_source_read;
  mock->pub.reset = mock_source_reset;
  mock->pub.close = mock_source_close;
}

void lc_public_mock_sink_init(lc_public_mock_sink *mock) {
  memset(mock, 0, sizeof(*mock));
  mock->pub.write = mock_sink_write;
  mock->pub.close = mock_sink_close;
}

void lc_public_mock_client_init(lc_public_mock_client *mock) {
  memset(mock, 0, sizeof(*mock));
  mock->rc = LC_OK;
  mock->deleted_value = 1;
  mock->deleted_count_value = 2;
  mock->pub.acquire = mock_client_acquire;
  mock->pub.describe = mock_client_describe;
  mock->pub.get = mock_client_get;
  mock->pub.load = mock_client_load;
  mock->pub.update = mock_client_update;
  mock->pub.mutate = mock_client_mutate;
  mock->pub.metadata = mock_client_metadata;
  mock->pub.remove = mock_client_remove;
  mock->pub.keepalive = mock_client_keepalive;
  mock->pub.release = mock_client_release;
  mock->pub.attach = mock_client_attach;
  mock->pub.list_attachments = mock_client_list_attachments;
  mock->pub.get_attachment = mock_client_get_attachment;
  mock->pub.delete_attachment = mock_client_delete_attachment;
  mock->pub.delete_all_attachments = mock_client_delete_all_attachments;
  mock->pub.queue_stats = mock_client_queue_stats;
  mock->pub.queue_ack = mock_client_queue_ack;
  mock->pub.queue_nack = mock_client_queue_nack;
  mock->pub.queue_extend = mock_client_queue_extend;
  mock->pub.query = mock_client_query;
  mock->pub.query_keys = mock_client_query_keys;
  mock->pub.get_namespace_config = mock_client_get_namespace_config;
  mock->pub.update_namespace_config = mock_client_update_namespace_config;
  mock->pub.flush_index = mock_client_flush_index;
  mock->pub.txn_replay = mock_client_txn_replay;
  mock->pub.txn_prepare = mock_client_txn_prepare;
  mock->pub.txn_commit = mock_client_txn_commit;
  mock->pub.txn_rollback = mock_client_txn_rollback;
  mock->pub.tc_lease_acquire = mock_client_tc_lease_acquire;
  mock->pub.tc_lease_renew = mock_client_tc_lease_renew;
  mock->pub.tc_lease_release = mock_client_tc_lease_release;
  mock->pub.tc_leader = mock_client_tc_leader;
  mock->pub.tc_cluster_announce = mock_client_tc_cluster_announce;
  mock->pub.tc_cluster_leave = mock_client_tc_cluster_leave;
  mock->pub.tc_cluster_list = mock_client_tc_cluster_list;
  mock->pub.tc_rm_register = mock_client_tc_rm_register;
  mock->pub.tc_rm_unregister = mock_client_tc_rm_unregister;
  mock->pub.tc_rm_list = mock_client_tc_rm_list;
  mock->pub.enqueue = mock_client_enqueue;
  mock->pub.dequeue = mock_client_dequeue;
  mock->pub.dequeue_batch = mock_client_dequeue_batch;
  mock->pub.dequeue_with_state = mock_client_dequeue_with_state;
  mock->pub.subscribe = mock_client_subscribe;
  mock->pub.subscribe_with_state = mock_client_subscribe_with_state;
  mock->pub.new_consumer_service = mock_client_new_consumer_service;
  mock->pub.watch_queue = mock_client_watch_queue;
  mock->pub.close = mock_client_close;
}

void lc_public_mock_lease_init(lc_public_mock_lease *mock) {
  memset(mock, 0, sizeof(*mock));
  mock->rc = LC_OK;
  mock->deleted_value = 1;
  mock->deleted_count_value = 2;
  mock->pub.describe = mock_lease_describe;
  mock->pub.get = mock_lease_get;
  mock->pub.load = mock_lease_load;
  mock->pub.save = mock_lease_save;
  mock->pub.update = mock_lease_update;
  mock->pub.mutate = mock_lease_mutate;
  mock->pub.mutate_local = mock_lease_mutate_local;
  mock->pub.metadata = mock_lease_metadata;
  mock->pub.remove = mock_lease_remove;
  mock->pub.keepalive = mock_lease_keepalive;
  mock->pub.release = mock_lease_release;
  mock->pub.attach = mock_lease_attach;
  mock->pub.list_attachments = mock_lease_list_attachments;
  mock->pub.get_attachment = mock_lease_get_attachment;
  mock->pub.delete_attachment = mock_lease_delete_attachment;
  mock->pub.delete_all_attachments = mock_lease_delete_all_attachments;
  mock->pub.close = mock_lease_close;
}

void lc_public_mock_message_init(lc_public_mock_message *mock) {
  memset(mock, 0, sizeof(*mock));
  mock->rc = LC_OK;
  mock->payload_written = 7U;
  mock->pub.ack = mock_message_ack;
  mock->pub.nack = mock_message_nack;
  mock->pub.extend = mock_message_extend;
  mock->pub.state = mock_message_state;
  mock->pub.payload_reader = mock_message_payload_reader;
  mock->pub.rewind_payload = mock_message_rewind_payload;
  mock->pub.write_payload = mock_message_write_payload;
  mock->pub.close = mock_message_close;
}

void lc_public_mock_consumer_service_init(
    lc_public_mock_consumer_service *mock) {
  memset(mock, 0, sizeof(*mock));
  mock->rc = LC_OK;
  mock->pub.run = mock_consumer_service_run;
  mock->pub.start = mock_consumer_service_start;
  mock->pub.stop = mock_consumer_service_stop;
  mock->pub.wait = mock_consumer_service_wait;
  mock->pub.close = mock_consumer_service_close;
}

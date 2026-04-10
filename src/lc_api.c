#include "lc_api_internal.h"

int lc_acquire(lc_client *client, const lc_acquire_req *req, lc_lease **out,
               lc_error *error) {
  return client->acquire(client, req, out, error);
}

int lc_describe(lc_client *client, const lc_describe_req *req,
                lc_describe_res *out, lc_error *error) {
  return client->describe(client, req, out, error);
}

int lc_get(lc_client *client, const char *key, const lc_get_opts *opts,
           lc_sink *dst, lc_get_res *out, lc_error *error) {
  return client->get(client, key, opts, dst, out, error);
}

int lc_load(lc_client *client, const char *key, const lonejson_map *map,
            void *dst, const lonejson_parse_options *parse_options,
            const lc_get_opts *opts, lc_get_res *out, lc_error *error) {
  return client->load(client, key, map, dst, parse_options, opts, out, error);
}

int lc_update(lc_client *client, const lc_update_req *req, lc_source *src,
              lc_update_res *out, lc_error *error) {
  return client->update(client, req, src, out, error);
}

int lc_mutate(lc_client *client, const lc_mutate_op *req, lc_mutate_res *out,
              lc_error *error) {
  return client->mutate(client, req, out, error);
}

int lc_metadata(lc_client *client, const lc_metadata_op *req,
                lc_metadata_res *out, lc_error *error) {
  return client->metadata(client, req, out, error);
}

int lc_remove(lc_client *client, const lc_remove_op *req, lc_remove_res *out,
              lc_error *error) {
  return client->remove(client, req, out, error);
}

int lc_keepalive(lc_client *client, const lc_keepalive_op *req,
                 lc_keepalive_res *out, lc_error *error) {
  return client->keepalive(client, req, out, error);
}

int lc_release(lc_client *client, const lc_release_op *req, lc_release_res *out,
               lc_error *error) {
  return client->release(client, req, out, error);
}

int lc_attach(lc_client *client, const lc_attach_op *req, lc_source *src,
              lc_attach_res *out, lc_error *error) {
  return client->attach(client, req, src, out, error);
}

int lc_list_attachments(lc_client *client, const lc_attachment_list_req *req,
                        lc_attachment_list *out, lc_error *error) {
  return client->list_attachments(client, req, out, error);
}

int lc_get_attachment(lc_client *client, const lc_attachment_get_op *req,
                      lc_sink *dst, lc_attachment_get_res *out,
                      lc_error *error) {
  return client->get_attachment(client, req, dst, out, error);
}

int lc_delete_attachment(lc_client *client, const lc_attachment_delete_op *req,
                         int *deleted, lc_error *error) {
  return client->delete_attachment(client, req, deleted, error);
}

int lc_delete_all_attachments(lc_client *client,
                              const lc_attachment_delete_all_op *req,
                              int *deleted_count, lc_error *error) {
  return client->delete_all_attachments(client, req, deleted_count, error);
}

int lc_queue_stats(lc_client *client, const lc_queue_stats_req *req,
                   lc_queue_stats_res *out, lc_error *error) {
  return client->queue_stats(client, req, out, error);
}

int lc_queue_ack(lc_client *client, const lc_ack_op *req, lc_ack_res *out,
                 lc_error *error) {
  return client->queue_ack(client, req, out, error);
}

int lc_queue_nack(lc_client *client, const lc_nack_op *req, lc_nack_res *out,
                  lc_error *error) {
  return client->queue_nack(client, req, out, error);
}

int lc_queue_extend(lc_client *client, const lc_extend_op *req,
                    lc_extend_res *out, lc_error *error) {
  return client->queue_extend(client, req, out, error);
}

int lc_query(lc_client *client, const lc_query_req *req, lc_sink *dst,
             lc_query_res *out, lc_error *error) {
  return client->query(client, req, dst, out, error);
}

int lc_get_namespace_config(lc_client *client,
                            const lc_namespace_config_req *req,
                            lc_namespace_config_res *out, lc_error *error) {
  return client->get_namespace_config(client, req, out, error);
}

int lc_update_namespace_config(lc_client *client,
                               const lc_namespace_config_req *req,
                               lc_namespace_config_res *out, lc_error *error) {
  return client->update_namespace_config(client, req, out, error);
}

int lc_flush_index(lc_client *client, const lc_index_flush_req *req,
                   lc_index_flush_res *out, lc_error *error) {
  return client->flush_index(client, req, out, error);
}

int lc_txn_replay(lc_client *client, const lc_txn_replay_req *req,
                  lc_txn_replay_res *out, lc_error *error) {
  return client->txn_replay(client, req, out, error);
}

int lc_txn_prepare(lc_client *client, const lc_txn_decision_req *req,
                   lc_txn_decision_res *out, lc_error *error) {
  return client->txn_prepare(client, req, out, error);
}

int lc_txn_commit(lc_client *client, const lc_txn_decision_req *req,
                  lc_txn_decision_res *out, lc_error *error) {
  return client->txn_commit(client, req, out, error);
}

int lc_txn_rollback(lc_client *client, const lc_txn_decision_req *req,
                    lc_txn_decision_res *out, lc_error *error) {
  return client->txn_rollback(client, req, out, error);
}

int lc_tc_lease_acquire(lc_client *client, const lc_tc_lease_acquire_req *req,
                        lc_tc_lease_acquire_res *out, lc_error *error) {
  return client->tc_lease_acquire(client, req, out, error);
}

int lc_tc_lease_renew(lc_client *client, const lc_tc_lease_renew_req *req,
                      lc_tc_lease_renew_res *out, lc_error *error) {
  return client->tc_lease_renew(client, req, out, error);
}

int lc_tc_lease_release(lc_client *client, const lc_tc_lease_release_req *req,
                        lc_tc_lease_release_res *out, lc_error *error) {
  return client->tc_lease_release(client, req, out, error);
}

int lc_tc_leader(lc_client *client, lc_tc_leader_res *out, lc_error *error) {
  return client->tc_leader(client, out, error);
}

int lc_tc_cluster_announce(lc_client *client,
                           const lc_tc_cluster_announce_req *req,
                           lc_tc_cluster_res *out, lc_error *error) {
  return client->tc_cluster_announce(client, req, out, error);
}

int lc_tc_cluster_leave(lc_client *client, lc_tc_cluster_res *out,
                        lc_error *error) {
  return client->tc_cluster_leave(client, out, error);
}

int lc_tc_cluster_list(lc_client *client, lc_tc_cluster_res *out,
                       lc_error *error) {
  return client->tc_cluster_list(client, out, error);
}

int lc_tc_rm_register(lc_client *client, const lc_tc_rm_register_req *req,
                      lc_tc_rm_res *out, lc_error *error) {
  return client->tc_rm_register(client, req, out, error);
}

int lc_tc_rm_unregister(lc_client *client, const lc_tc_rm_unregister_req *req,
                        lc_tc_rm_res *out, lc_error *error) {
  return client->tc_rm_unregister(client, req, out, error);
}

int lc_tc_rm_list(lc_client *client, lc_tc_rm_list_res *out, lc_error *error) {
  return client->tc_rm_list(client, out, error);
}

int lc_enqueue(lc_client *client, const lc_enqueue_req *req, lc_source *src,
               lc_enqueue_res *out, lc_error *error) {
  return client->enqueue(client, req, src, out, error);
}

int lc_dequeue(lc_client *client, const lc_dequeue_req *req, lc_message **out,
               lc_error *error) {
  return client->dequeue(client, req, out, error);
}

int lc_dequeue_batch(lc_client *client, const lc_dequeue_req *req,
                     lc_dequeue_batch_res *out, lc_error *error) {
  return client->dequeue_batch(client, req, out, error);
}

int lc_dequeue_with_state(lc_client *client, const lc_dequeue_req *req,
                          lc_message **out, lc_error *error) {
  return client->dequeue_with_state(client, req, out, error);
}

int lc_subscribe(lc_client *client, const lc_dequeue_req *req,
                 const lc_consumer *consumer, lc_error *error) {
  return client->subscribe(client, req, consumer, error);
}

int lc_subscribe_with_state(lc_client *client, const lc_dequeue_req *req,
                            const lc_consumer *consumer, lc_error *error) {
  return client->subscribe_with_state(client, req, consumer, error);
}

int lc_client_new_consumer_service(lc_client *client,
                                   const lc_consumer_service_config *config,
                                   lc_consumer_service **out, lc_error *error) {
  return client->new_consumer_service(client, config, out, error);
}

int lc_watch_queue(lc_client *client, const lc_watch_queue_req *req,
                   const lc_watch_handler *handler, lc_error *error) {
  return client->watch_queue(client, req, handler, error);
}

int lc_lease_describe(lc_lease *lease, lc_error *error) {
  return lease->describe(lease, error);
}

int lc_lease_get(lc_lease *lease, lc_sink *dst, const lc_get_opts *opts,
                 lc_get_res *out, lc_error *error) {
  return lease->get(lease, dst, opts, out, error);
}

int lc_lease_load(lc_lease *lease, const lonejson_map *map, void *dst,
                  const lonejson_parse_options *parse_options,
                  const lc_get_opts *opts, lc_get_res *out, lc_error *error) {
  return lease->load(lease, map, dst, parse_options, opts, out, error);
}

int lc_lease_save(lc_lease *lease, const lonejson_map *map, const void *src,
                  const lonejson_write_options *write_options,
                  lc_error *error) {
  return lease->save(lease, map, src, write_options, error);
}

int lc_lease_update(lc_lease *lease, lc_source *src,
                    const lc_update_opts *opts,
                    lc_error *error) {
  return lease->update(lease, src, opts, error);
}

int lc_lease_mutate(lc_lease *lease, const lc_mutate_req *req,
                    lc_error *error) {
  return lease->mutate(lease, req, error);
}

int lc_lease_mutate_local(lc_lease *lease, const lc_mutate_local_req *req,
                          lc_error *error) {
  return lease->mutate_local(lease, req, error);
}

int lc_lease_metadata(lc_lease *lease, const lc_metadata_req *req,
                      lc_error *error) {
  return lease->metadata(lease, req, error);
}

int lc_lease_remove(lc_lease *lease, const lc_remove_req *req,
                    lc_error *error) {
  return lease->remove(lease, req, error);
}

int lc_lease_keepalive(lc_lease *lease, const lc_keepalive_req *req,
                       lc_error *error) {
  return lease->keepalive(lease, req, error);
}

int lc_lease_release(lc_lease *lease, const lc_release_req *req,
                     lc_error *error) {
  return lease->release(lease, req, error);
}

int lc_lease_attach(lc_lease *lease, const lc_attach_req *req, lc_source *src,
                    lc_attach_res *out, lc_error *error) {
  return lease->attach(lease, req, src, out, error);
}

int lc_lease_list_attachments(lc_lease *lease, lc_attachment_list *out,
                              lc_error *error) {
  return lease->list_attachments(lease, out, error);
}

int lc_lease_get_attachment(lc_lease *lease, const lc_attachment_get_req *req,
                            lc_sink *dst, lc_attachment_get_res *out,
                            lc_error *error) {
  return lease->get_attachment(lease, req, dst, out, error);
}

int lc_lease_delete_attachment(lc_lease *lease,
                               const lc_attachment_selector *selector,
                               int *deleted, lc_error *error) {
  return lease->delete_attachment(lease, selector, deleted, error);
}

int lc_lease_delete_all_attachments(lc_lease *lease, int *deleted_count,
                                    lc_error *error) {
  return lease->delete_all_attachments(lease, deleted_count, error);
}

int lc_message_ack(lc_message *message, lc_error *error) {
  return message->ack(message, error);
}

int lc_message_nack(lc_message *message, const lc_nack_req *req,
                    lc_error *error) {
  return message->nack(message, req, error);
}

int lc_message_extend(lc_message *message, const lc_extend_req *req,
                      lc_error *error) {
  return message->extend(message, req, error);
}

lc_lease *lc_message_state(lc_message *message) {
  if (message == NULL || message->state == NULL) {
    return NULL;
  }
  return message->state(message);
}

lc_source *lc_message_payload(lc_message *message) {
  if (message == NULL) {
    return NULL;
  }
  return message->payload_reader(message);
}

int lc_message_rewind_payload(lc_message *message, lc_error *error) {
  if (message == NULL || message->rewind_payload == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "message rewind_payload requires message", NULL, NULL,
                        NULL);
  }
  return message->rewind_payload(message, error);
}

int lc_message_write_payload(lc_message *message, lc_sink *dst, size_t *written,
                             lc_error *error) {
  if (message == NULL || dst == NULL || message->write_payload == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "message write_payload requires message and dst", NULL,
                        NULL, NULL);
  }
  return message->write_payload(message, dst, written, error);
}

int lc_consumer_service_run(lc_consumer_service *service, lc_error *error) {
  return service->run(service, error);
}

int lc_consumer_service_start(lc_consumer_service *service, lc_error *error) {
  return service->start(service, error);
}

int lc_consumer_service_stop(lc_consumer_service *service) {
  return service->stop(service);
}

int lc_consumer_service_wait(lc_consumer_service *service, lc_error *error) {
  return service->wait(service, error);
}

void lc_client_close(lc_client *client) {
  if (client != NULL) {
    client->close(client);
  }
}

void lc_lease_close(lc_lease *lease) {
  if (lease != NULL) {
    lease->close(lease);
  }
}

void lc_message_close(lc_message *message) {
  if (message != NULL) {
    message->close(message);
  }
}

void lc_source_close(lc_source *source) {
  if (source != NULL) {
    source->close(source);
  }
}

void lc_sink_close(lc_sink *sink) {
  if (sink != NULL) {
    sink->close(sink);
  }
}

void lc_consumer_service_close(lc_consumer_service *service) {
  if (service != NULL) {
    service->close(service);
  }
}

#include "lc_api_internal.h"
#include "lc_pouch.h"

static int lc_pouch_client_rebuilding(lc_error *error) {
  return lc_error_set(
      error, LC_ERR_INVALID, 0L,
      "pouch operation is not implemented in the redesigned pouch backend",
      "storage, index, search, queue, object, and transaction subsystems are "
      "being rebuilt on the new pouch architecture",
      NULL, "pouch-redesign");
}

int lc_pouch_client_acquire_method(lc_client *self, const lc_acquire_req *req,
                                   lc_lease **out, lc_error *error) {
  (void)self;
  (void)req;
  if (out != NULL) {
    *out = NULL;
  }
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_acquire_for_update_method(
    lc_client *self, const lc_acquire_req *req,
    lc_acquire_for_update_handler_fn handler, void *context,
    lc_error *error) {
  (void)self;
  (void)req;
  (void)handler;
  (void)context;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_describe_method(lc_client *self, const lc_describe_req *req,
                                    lc_describe_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_get_method(lc_client *self, const char *key,
                               const lc_get_opts *opts, lc_sink *dst,
                               lc_get_res *out, lc_error *error) {
  (void)self;
  (void)key;
  (void)opts;
  (void)dst;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_load_method(lc_client *self, const char *key,
                                const lonejson_map *map, void *dst,
                                const lc_get_opts *opts, lc_get_res *out,
                                lc_error *error) {
  (void)self;
  (void)key;
  (void)map;
  (void)dst;
  (void)opts;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_update_method(lc_client *self, const lc_update_req *req,
                                  lc_source *src, lc_update_res *out,
                                  lc_error *error) {
  (void)self;
  (void)req;
  (void)src;
  (void)out;
  return lc_pouch_client_rebuilding(error);
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
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_remove_method(lc_client *self, const lc_remove_op *req,
                                  lc_remove_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_keepalive_method(lc_client *self,
                                     const lc_keepalive_op *req,
                                     lc_keepalive_res *out,
                                     lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_client_release_method(lc_client *self, const lc_release_op *req,
                                   lc_release_res *out, lc_error *error) {
  (void)self;
  (void)req;
  (void)out;
  return lc_pouch_client_rebuilding(error);
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
  (void)self;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_lease_get_method(lc_lease *self, lc_sink *dst,
                              const lc_get_opts *opts, lc_get_res *out,
                              lc_error *error) {
  (void)self;
  (void)dst;
  (void)opts;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_lease_update_method(lc_lease *self, lc_source *src,
                                 const lc_update_opts *opts,
                                 lc_error *error) {
  (void)self;
  (void)src;
  (void)opts;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_lease_metadata_method(lc_lease *self, const lc_metadata_req *req,
                                   lc_error *error) {
  (void)self;
  (void)req;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_lease_remove_method(lc_lease *self, const lc_remove_req *req,
                                 lc_error *error) {
  (void)self;
  (void)req;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_lease_keepalive_method(lc_lease *self,
                                    const lc_keepalive_req *req,
                                    lc_error *error) {
  (void)self;
  (void)req;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_lease_release_method(lc_lease *self, const lc_release_req *req,
                                  lc_error *error) {
  (void)self;
  (void)req;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_lease_attach_method(lc_lease *self, const lc_attach_req *req,
                                 lc_source *src, lc_attach_res *out,
                                 lc_error *error) {
  (void)self;
  (void)req;
  (void)src;
  (void)out;
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_lease_list_attachments_method(lc_lease *self,
                                           lc_attachment_list *out,
                                           lc_error *error) {
  (void)self;
  (void)out;
  return lc_pouch_client_rebuilding(error);
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
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_lease_delete_attachment_method(
    lc_lease *self, const lc_attachment_selector *selector, int *deleted,
    lc_error *error) {
  (void)self;
  (void)selector;
  if (deleted != NULL) {
    *deleted = 0;
  }
  return lc_pouch_client_rebuilding(error);
}

int lc_pouch_lease_delete_all_attachments_method(lc_lease *self,
                                                 int *deleted_count,
                                                 lc_error *error) {
  (void)self;
  if (deleted_count != NULL) {
    *deleted_count = 0;
  }
  return lc_pouch_client_rebuilding(error);
}

#ifndef TESTS_UNIT_MOCK_LC_PUBLIC_H
#define TESTS_UNIT_MOCK_LC_PUBLIC_H

#include "lc/lc.h"

typedef struct lc_public_mock_call {
  int count;
  const void *arg1;
  const void *arg2;
  const void *arg3;
  const void *arg4;
  const void *arg5;
  const void *arg6;
} lc_public_mock_call;

typedef struct lc_public_mock_source {
  lc_source pub;
  int close_calls;
  int reset_calls;
  int read_calls;
  const void *read_bytes;
  size_t read_length;
  size_t read_offset;
} lc_public_mock_source;

typedef struct lc_public_mock_sink {
  lc_sink pub;
  int close_calls;
  int write_calls;
  const void *last_write_bytes;
  size_t last_write_count;
} lc_public_mock_sink;

typedef struct lc_public_mock_consumer_service {
  lc_consumer_service pub;
  int rc;
  lc_public_mock_call run_call;
  lc_public_mock_call start_call;
  lc_public_mock_call stop_call;
  lc_public_mock_call wait_call;
  int close_calls;
} lc_public_mock_consumer_service;

typedef struct lc_public_mock_lease {
  lc_lease pub;
  int rc;
  int deleted_value;
  int deleted_count_value;
  lc_public_mock_call describe_call;
  lc_public_mock_call get_call;
  lc_public_mock_call load_call;
  lc_public_mock_call save_call;
  lc_public_mock_call update_call;
  lc_public_mock_call mutate_call;
  lc_public_mock_call mutate_local_call;
  lc_public_mock_call metadata_call;
  lc_public_mock_call remove_call;
  lc_public_mock_call keepalive_call;
  lc_public_mock_call release_call;
  lc_public_mock_call attach_call;
  lc_public_mock_call list_attachments_call;
  lc_public_mock_call get_attachment_call;
  lc_public_mock_call delete_attachment_call;
  lc_public_mock_call delete_all_attachments_call;
  int close_calls;
} lc_public_mock_lease;

typedef struct lc_public_mock_message {
  lc_message pub;
  int rc;
  lc_lease *state_to_return;
  lc_source *payload_reader_to_return;
  size_t payload_written;
  lc_public_mock_call ack_call;
  lc_public_mock_call nack_call;
  lc_public_mock_call extend_call;
  lc_public_mock_call state_call;
  lc_public_mock_call payload_reader_call;
  lc_public_mock_call rewind_payload_call;
  lc_public_mock_call write_payload_call;
  int close_calls;
} lc_public_mock_message;

typedef struct lc_public_mock_client {
  lc_client pub;
  int rc;
  lc_lease *lease_to_return;
  lc_message *message_to_return;
  lc_consumer_service *service_to_return;
  int deleted_value;
  int deleted_count_value;
  lc_public_mock_call acquire_call;
  lc_public_mock_call describe_call;
  lc_public_mock_call get_call;
  lc_public_mock_call load_call;
  lc_public_mock_call update_call;
  lc_public_mock_call mutate_call;
  lc_public_mock_call metadata_call;
  lc_public_mock_call remove_call;
  lc_public_mock_call keepalive_call;
  lc_public_mock_call release_call;
  lc_public_mock_call attach_call;
  lc_public_mock_call list_attachments_call;
  lc_public_mock_call get_attachment_call;
  lc_public_mock_call delete_attachment_call;
  lc_public_mock_call delete_all_attachments_call;
  lc_public_mock_call queue_stats_call;
  lc_public_mock_call queue_ack_call;
  lc_public_mock_call queue_nack_call;
  lc_public_mock_call queue_extend_call;
  lc_public_mock_call query_call;
  lc_public_mock_call query_keys_call;
  lc_public_mock_call get_namespace_config_call;
  lc_public_mock_call update_namespace_config_call;
  lc_public_mock_call flush_index_call;
  lc_public_mock_call txn_replay_call;
  lc_public_mock_call txn_prepare_call;
  lc_public_mock_call txn_commit_call;
  lc_public_mock_call txn_rollback_call;
  lc_public_mock_call tc_lease_acquire_call;
  lc_public_mock_call tc_lease_renew_call;
  lc_public_mock_call tc_lease_release_call;
  lc_public_mock_call tc_leader_call;
  lc_public_mock_call tc_cluster_announce_call;
  lc_public_mock_call tc_cluster_leave_call;
  lc_public_mock_call tc_cluster_list_call;
  lc_public_mock_call tc_rm_register_call;
  lc_public_mock_call tc_rm_unregister_call;
  lc_public_mock_call tc_rm_list_call;
  lc_public_mock_call enqueue_call;
  lc_public_mock_call dequeue_call;
  lc_public_mock_call dequeue_batch_call;
  lc_public_mock_call dequeue_with_state_call;
  lc_public_mock_call subscribe_call;
  lc_public_mock_call subscribe_with_state_call;
  lc_public_mock_call new_consumer_service_call;
  lc_public_mock_call watch_queue_call;
  int close_calls;
} lc_public_mock_client;

void lc_public_mock_source_init(lc_public_mock_source *mock);
void lc_public_mock_sink_init(lc_public_mock_sink *mock);
void lc_public_mock_client_init(lc_public_mock_client *mock);
void lc_public_mock_lease_init(lc_public_mock_lease *mock);
void lc_public_mock_message_init(lc_public_mock_message *mock);
void lc_public_mock_consumer_service_init(
    lc_public_mock_consumer_service *mock);

#endif

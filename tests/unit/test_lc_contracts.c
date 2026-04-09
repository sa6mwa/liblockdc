#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <string.h>

#include <cmocka.h>

#include "lc/lc.h"
#include "mock_lc_public.h"

static void test_client_wrappers_delegate_full_public_surface(void **state) {
  lc_public_mock_client client;
  lc_public_mock_lease lease;
  lc_public_mock_message message;
  lc_public_mock_consumer_service service;
  lc_public_mock_source source;
  lc_public_mock_sink sink;
  lc_acquire_req acquire_req;
  lc_describe_req describe_req;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lc_update_req update_req;
  lc_update_res update_res;
  lc_mutate_op mutate_op;
  lc_mutate_res mutate_res;
  lc_metadata_op metadata_op;
  lc_metadata_res metadata_res;
  lc_remove_op remove_op;
  lc_remove_res remove_res;
  lc_keepalive_op keepalive_op;
  lc_keepalive_res keepalive_res;
  lc_release_op release_op;
  lc_release_res release_res;
  lc_attach_op attach_op;
  lc_attach_res attach_res;
  lc_attachment_list_req attachment_list_req;
  lc_attachment_list attachment_list;
  lc_attachment_get_op attachment_get_op;
  lc_attachment_get_res attachment_get_res;
  lc_attachment_delete_op attachment_delete_op;
  lc_attachment_delete_all_op attachment_delete_all_op;
  lc_queue_stats_req queue_stats_req;
  lc_queue_stats_res queue_stats_res;
  lc_ack_op ack_op;
  lc_ack_res ack_res;
  lc_nack_op nack_op;
  lc_nack_res nack_res;
  lc_extend_op extend_op;
  lc_extend_res extend_res;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_namespace_config_req namespace_req;
  lc_namespace_config_res namespace_res;
  lc_index_flush_req index_flush_req;
  lc_index_flush_res index_flush_res;
  lc_txn_replay_req txn_replay_req;
  lc_txn_replay_res txn_replay_res;
  lc_txn_decision_req txn_decision_req;
  lc_txn_decision_res txn_decision_res;
  lc_tc_lease_acquire_req tc_lease_acquire_req;
  lc_tc_lease_acquire_res tc_lease_acquire_res;
  lc_tc_lease_renew_req tc_lease_renew_req;
  lc_tc_lease_renew_res tc_lease_renew_res;
  lc_tc_lease_release_req tc_lease_release_req;
  lc_tc_lease_release_res tc_lease_release_res;
  lc_tc_leader_res tc_leader_res;
  lc_tc_cluster_announce_req tc_cluster_announce_req;
  lc_tc_cluster_res tc_cluster_res;
  lc_tc_rm_register_req tc_rm_register_req;
  lc_tc_rm_unregister_req tc_rm_unregister_req;
  lc_tc_rm_res tc_rm_res;
  lc_tc_rm_list_res tc_rm_list_res;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_dequeue_batch_res dequeue_batch_res;
  lc_consumer consumer;
  lc_consumer_service_config consumer_service_config;
  lc_watch_queue_req watch_queue_req;
  lc_watch_handler watch_handler;
  lc_json json;
  lc_lease *lease_out;
  lc_message *message_out;
  lc_consumer_service *service_out;
  int deleted;
  int deleted_count;
  int rc;
  lc_error error;

  (void)state;
  lc_public_mock_client_init(&client);
  lc_public_mock_lease_init(&lease);
  lc_public_mock_message_init(&message);
  lc_public_mock_consumer_service_init(&service);
  lc_public_mock_source_init(&source);
  lc_public_mock_sink_init(&sink);
  memset(&json, 0, sizeof(json));
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&describe_req, 0, sizeof(describe_req));
  memset(&get_opts, 0, sizeof(get_opts));
  memset(&get_res, 0, sizeof(get_res));
  memset(&update_req, 0, sizeof(update_req));
  memset(&update_res, 0, sizeof(update_res));
  memset(&mutate_op, 0, sizeof(mutate_op));
  memset(&mutate_res, 0, sizeof(mutate_res));
  memset(&metadata_op, 0, sizeof(metadata_op));
  memset(&metadata_res, 0, sizeof(metadata_res));
  memset(&remove_op, 0, sizeof(remove_op));
  memset(&remove_res, 0, sizeof(remove_res));
  memset(&keepalive_op, 0, sizeof(keepalive_op));
  memset(&keepalive_res, 0, sizeof(keepalive_res));
  memset(&release_op, 0, sizeof(release_op));
  memset(&release_res, 0, sizeof(release_res));
  memset(&attach_op, 0, sizeof(attach_op));
  memset(&attach_res, 0, sizeof(attach_res));
  memset(&attachment_list_req, 0, sizeof(attachment_list_req));
  memset(&attachment_list, 0, sizeof(attachment_list));
  memset(&attachment_get_op, 0, sizeof(attachment_get_op));
  memset(&attachment_get_res, 0, sizeof(attachment_get_res));
  memset(&attachment_delete_op, 0, sizeof(attachment_delete_op));
  memset(&attachment_delete_all_op, 0, sizeof(attachment_delete_all_op));
  memset(&queue_stats_req, 0, sizeof(queue_stats_req));
  memset(&queue_stats_res, 0, sizeof(queue_stats_res));
  memset(&ack_op, 0, sizeof(ack_op));
  memset(&ack_res, 0, sizeof(ack_res));
  memset(&nack_op, 0, sizeof(nack_op));
  memset(&nack_res, 0, sizeof(nack_res));
  memset(&extend_op, 0, sizeof(extend_op));
  memset(&extend_res, 0, sizeof(extend_res));
  memset(&query_req, 0, sizeof(query_req));
  memset(&query_res, 0, sizeof(query_res));
  memset(&namespace_req, 0, sizeof(namespace_req));
  memset(&namespace_res, 0, sizeof(namespace_res));
  memset(&index_flush_req, 0, sizeof(index_flush_req));
  memset(&index_flush_res, 0, sizeof(index_flush_res));
  memset(&txn_replay_req, 0, sizeof(txn_replay_req));
  memset(&txn_replay_res, 0, sizeof(txn_replay_res));
  memset(&txn_decision_req, 0, sizeof(txn_decision_req));
  memset(&txn_decision_res, 0, sizeof(txn_decision_res));
  memset(&tc_lease_acquire_req, 0, sizeof(tc_lease_acquire_req));
  memset(&tc_lease_acquire_res, 0, sizeof(tc_lease_acquire_res));
  memset(&tc_lease_renew_req, 0, sizeof(tc_lease_renew_req));
  memset(&tc_lease_renew_res, 0, sizeof(tc_lease_renew_res));
  memset(&tc_lease_release_req, 0, sizeof(tc_lease_release_req));
  memset(&tc_lease_release_res, 0, sizeof(tc_lease_release_res));
  memset(&tc_leader_res, 0, sizeof(tc_leader_res));
  memset(&tc_cluster_announce_req, 0, sizeof(tc_cluster_announce_req));
  memset(&tc_cluster_res, 0, sizeof(tc_cluster_res));
  memset(&tc_rm_register_req, 0, sizeof(tc_rm_register_req));
  memset(&tc_rm_unregister_req, 0, sizeof(tc_rm_unregister_req));
  memset(&tc_rm_res, 0, sizeof(tc_rm_res));
  memset(&tc_rm_list_res, 0, sizeof(tc_rm_list_res));
  memset(&enqueue_req, 0, sizeof(enqueue_req));
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&dequeue_req, 0, sizeof(dequeue_req));
  memset(&dequeue_batch_res, 0, sizeof(dequeue_batch_res));
  memset(&consumer, 0, sizeof(consumer));
  memset(&consumer_service_config, 0, sizeof(consumer_service_config));
  memset(&watch_queue_req, 0, sizeof(watch_queue_req));
  memset(&watch_handler, 0, sizeof(watch_handler));
  lc_error_init(&error);

  client.lease_to_return = &lease.pub;
  client.message_to_return = &message.pub;
  client.service_to_return = &service.pub;
  lease_out = NULL;
  message_out = NULL;
  service_out = NULL;
  deleted = 0;
  deleted_count = 0;

  rc = lc_acquire(&client.pub, &acquire_req, &lease_out, &error);
  assert_int_equal(rc, LC_OK);
  assert_ptr_equal(lease_out, &lease.pub);
  assert_int_equal(client.acquire_call.count, 1);

  rc = lc_describe(&client.pub, &describe_req, NULL, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(client.describe_call.count, 1);

  rc = lc_get(&client.pub, "key-1", &get_opts, &sink.pub, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_ptr_equal(client.get_call.arg2, "key-1");

  rc = lc_load(&client.pub, "key-2", NULL, NULL, NULL, &get_opts, &get_res,
               &error);
  assert_int_equal(rc, LC_OK);
  assert_ptr_equal(client.load_call.arg2, "key-2");

  rc = lc_update(&client.pub, &update_req, &json, &update_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_mutate(&client.pub, &mutate_op, &mutate_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_metadata(&client.pub, &metadata_op, &metadata_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_remove(&client.pub, &remove_op, &remove_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_keepalive(&client.pub, &keepalive_op, &keepalive_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_release(&client.pub, &release_op, &release_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_attach(&client.pub, &attach_op, &source.pub, &attach_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_list_attachments(&client.pub, &attachment_list_req, &attachment_list,
                           &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_get_attachment(&client.pub, &attachment_get_op, &sink.pub,
                         &attachment_get_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_delete_attachment(&client.pub, &attachment_delete_op, &deleted,
                            &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(deleted, 1);
  rc = lc_delete_all_attachments(&client.pub, &attachment_delete_all_op,
                                 &deleted_count, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(deleted_count, 2);
  rc = lc_queue_stats(&client.pub, &queue_stats_req, &queue_stats_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_queue_ack(&client.pub, &ack_op, &ack_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_queue_nack(&client.pub, &nack_op, &nack_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_queue_extend(&client.pub, &extend_op, &extend_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_query(&client.pub, &query_req, &sink.pub, &query_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_get_namespace_config(&client.pub, &namespace_req, &namespace_res,
                               &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_update_namespace_config(&client.pub, &namespace_req, &namespace_res,
                                  &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_flush_index(&client.pub, &index_flush_req, &index_flush_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_txn_replay(&client.pub, &txn_replay_req, &txn_replay_res, &error);
  assert_int_equal(rc, LC_OK);
  rc =
      lc_txn_prepare(&client.pub, &txn_decision_req, &txn_decision_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_txn_commit(&client.pub, &txn_decision_req, &txn_decision_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_txn_rollback(&client.pub, &txn_decision_req, &txn_decision_res,
                       &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_tc_lease_acquire(&client.pub, &tc_lease_acquire_req,
                           &tc_lease_acquire_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_tc_lease_renew(&client.pub, &tc_lease_renew_req, &tc_lease_renew_res,
                         &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_tc_lease_release(&client.pub, &tc_lease_release_req,
                           &tc_lease_release_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_tc_leader(&client.pub, &tc_leader_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_tc_cluster_announce(&client.pub, &tc_cluster_announce_req,
                              &tc_cluster_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_tc_cluster_leave(&client.pub, &tc_cluster_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_tc_cluster_list(&client.pub, &tc_cluster_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_tc_rm_register(&client.pub, &tc_rm_register_req, &tc_rm_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_tc_rm_unregister(&client.pub, &tc_rm_unregister_req, &tc_rm_res,
                           &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_tc_rm_list(&client.pub, &tc_rm_list_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_enqueue(&client.pub, &enqueue_req, &source.pub, &enqueue_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_dequeue(&client.pub, &dequeue_req, &message_out, &error);
  assert_int_equal(rc, LC_OK);
  assert_ptr_equal(message_out, &message.pub);
  rc = lc_dequeue_batch(&client.pub, &dequeue_req, &dequeue_batch_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_dequeue_with_state(&client.pub, &dequeue_req, &message_out, &error);
  assert_int_equal(rc, LC_OK);
  assert_ptr_equal(message_out, &message.pub);
  rc = lc_subscribe(&client.pub, &dequeue_req, &consumer, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_subscribe_with_state(&client.pub, &dequeue_req, &consumer, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_client_new_consumer_service(&client.pub, &consumer_service_config,
                                      &service_out, &error);
  assert_int_equal(rc, LC_OK);
  assert_ptr_equal(service_out, &service.pub);
  rc = lc_watch_queue(&client.pub, &watch_queue_req, &watch_handler, &error);
  assert_int_equal(rc, LC_OK);

  assert_int_equal(client.acquire_call.count, 1);
  assert_int_equal(client.describe_call.count, 1);
  assert_int_equal(client.get_call.count, 1);
  assert_int_equal(client.load_call.count, 1);
  assert_int_equal(client.update_call.count, 1);
  assert_int_equal(client.mutate_call.count, 1);
  assert_int_equal(client.metadata_call.count, 1);
  assert_int_equal(client.remove_call.count, 1);
  assert_int_equal(client.keepalive_call.count, 1);
  assert_int_equal(client.release_call.count, 1);
  assert_int_equal(client.attach_call.count, 1);
  assert_int_equal(client.list_attachments_call.count, 1);
  assert_int_equal(client.get_attachment_call.count, 1);
  assert_int_equal(client.delete_attachment_call.count, 1);
  assert_int_equal(client.delete_all_attachments_call.count, 1);
  assert_int_equal(client.queue_stats_call.count, 1);
  assert_int_equal(client.queue_ack_call.count, 1);
  assert_int_equal(client.queue_nack_call.count, 1);
  assert_int_equal(client.queue_extend_call.count, 1);
  assert_int_equal(client.query_call.count, 1);
  assert_int_equal(client.get_namespace_config_call.count, 1);
  assert_int_equal(client.update_namespace_config_call.count, 1);
  assert_int_equal(client.flush_index_call.count, 1);
  assert_int_equal(client.txn_replay_call.count, 1);
  assert_int_equal(client.txn_prepare_call.count, 1);
  assert_int_equal(client.txn_commit_call.count, 1);
  assert_int_equal(client.txn_rollback_call.count, 1);
  assert_int_equal(client.tc_lease_acquire_call.count, 1);
  assert_int_equal(client.tc_lease_renew_call.count, 1);
  assert_int_equal(client.tc_lease_release_call.count, 1);
  assert_int_equal(client.tc_leader_call.count, 1);
  assert_int_equal(client.tc_cluster_announce_call.count, 1);
  assert_int_equal(client.tc_cluster_leave_call.count, 1);
  assert_int_equal(client.tc_cluster_list_call.count, 1);
  assert_int_equal(client.tc_rm_register_call.count, 1);
  assert_int_equal(client.tc_rm_unregister_call.count, 1);
  assert_int_equal(client.tc_rm_list_call.count, 1);
  assert_int_equal(client.enqueue_call.count, 1);
  assert_int_equal(client.dequeue_call.count, 1);
  assert_int_equal(client.dequeue_batch_call.count, 1);
  assert_int_equal(client.dequeue_with_state_call.count, 1);
  assert_int_equal(client.subscribe_call.count, 1);
  assert_int_equal(client.subscribe_with_state_call.count, 1);
  assert_int_equal(client.new_consumer_service_call.count, 1);
  assert_int_equal(client.watch_queue_call.count, 1);

  lc_client_close(&client.pub);
  assert_int_equal(client.close_calls, 1);
  lc_error_cleanup(&error);
}

static void test_lease_wrappers_delegate_full_public_surface(void **state) {
  lc_public_mock_lease lease;
  lc_public_mock_source source;
  lc_public_mock_sink sink;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lc_update_opts update_opts;
  lc_mutate_req mutate_req;
  lc_mutate_local_req mutate_local_req;
  lc_metadata_req metadata_req;
  lc_remove_req remove_req;
  lc_keepalive_req keepalive_req;
  lc_release_req release_req;
  lc_attach_req attach_req;
  lc_attach_res attach_res;
  lc_attachment_list attachment_list;
  lc_attachment_get_req attachment_get_req;
  lc_attachment_get_res attachment_get_res;
  lc_attachment_selector attachment_selector;
  lc_json json;
  lc_error error;
  int deleted;
  int deleted_count;
  int rc;

  (void)state;
  lc_public_mock_lease_init(&lease);
  lc_public_mock_source_init(&source);
  lc_public_mock_sink_init(&sink);
  memset(&get_opts, 0, sizeof(get_opts));
  memset(&get_res, 0, sizeof(get_res));
  memset(&update_opts, 0, sizeof(update_opts));
  memset(&mutate_req, 0, sizeof(mutate_req));
  memset(&mutate_local_req, 0, sizeof(mutate_local_req));
  memset(&metadata_req, 0, sizeof(metadata_req));
  memset(&remove_req, 0, sizeof(remove_req));
  memset(&keepalive_req, 0, sizeof(keepalive_req));
  memset(&release_req, 0, sizeof(release_req));
  memset(&attach_req, 0, sizeof(attach_req));
  memset(&attach_res, 0, sizeof(attach_res));
  memset(&attachment_list, 0, sizeof(attachment_list));
  memset(&attachment_get_req, 0, sizeof(attachment_get_req));
  memset(&attachment_get_res, 0, sizeof(attachment_get_res));
  memset(&attachment_selector, 0, sizeof(attachment_selector));
  memset(&json, 0, sizeof(json));
  lc_error_init(&error);
  deleted = 0;
  deleted_count = 0;

  rc = lc_lease_describe(&lease.pub, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_lease_get(&lease.pub, &sink.pub, &get_opts, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_lease_load(&lease.pub, NULL, NULL, NULL, &get_opts, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_lease_save(&lease.pub, NULL, NULL, NULL, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_lease_update(&lease.pub, &json, &update_opts, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_lease_mutate(&lease.pub, &mutate_req, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_lease_mutate_local(&lease.pub, &mutate_local_req, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_lease_metadata(&lease.pub, &metadata_req, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_lease_remove(&lease.pub, &remove_req, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_lease_keepalive(&lease.pub, &keepalive_req, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_lease_release(&lease.pub, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_lease_attach(&lease.pub, &attach_req, &source.pub, &attach_res,
                       &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_lease_list_attachments(&lease.pub, &attachment_list, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_lease_get_attachment(&lease.pub, &attachment_get_req, &sink.pub,
                               &attachment_get_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_lease_delete_attachment(&lease.pub, &attachment_selector, &deleted,
                                  &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(deleted, 1);
  rc = lc_lease_delete_all_attachments(&lease.pub, &deleted_count, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(deleted_count, 2);

  assert_int_equal(lease.describe_call.count, 1);
  assert_int_equal(lease.get_call.count, 1);
  assert_int_equal(lease.load_call.count, 1);
  assert_int_equal(lease.save_call.count, 1);
  assert_int_equal(lease.update_call.count, 1);
  assert_int_equal(lease.mutate_call.count, 1);
  assert_int_equal(lease.mutate_local_call.count, 1);
  assert_int_equal(lease.metadata_call.count, 1);
  assert_int_equal(lease.remove_call.count, 1);
  assert_int_equal(lease.keepalive_call.count, 1);
  assert_int_equal(lease.release_call.count, 1);
  assert_int_equal(lease.attach_call.count, 1);
  assert_int_equal(lease.list_attachments_call.count, 1);
  assert_int_equal(lease.get_attachment_call.count, 1);
  assert_int_equal(lease.delete_attachment_call.count, 1);
  assert_int_equal(lease.delete_all_attachments_call.count, 1);

  lc_lease_close(&lease.pub);
  assert_int_equal(lease.close_calls, 1);
  lc_error_cleanup(&error);
}

static void
test_message_and_service_wrappers_delegate_full_public_surface(void **state) {
  lc_public_mock_message message;
  lc_public_mock_lease lease;
  lc_public_mock_source source;
  lc_public_mock_sink sink;
  lc_public_mock_json json;
  lc_public_mock_consumer_service service;
  lc_nack_req nack_req;
  lc_extend_req extend_req;
  lc_json *json_out;
  lc_lease *state_out;
  lc_source *payload_out;
  size_t written;
  lc_error error;
  int rc;

  (void)state;
  lc_public_mock_message_init(&message);
  lc_public_mock_lease_init(&lease);
  lc_public_mock_source_init(&source);
  lc_public_mock_sink_init(&sink);
  lc_public_mock_json_init(&json);
  lc_public_mock_consumer_service_init(&service);
  memset(&nack_req, 0, sizeof(nack_req));
  memset(&extend_req, 0, sizeof(extend_req));
  lc_error_init(&error);

  message.state_to_return = &lease.pub;
  message.payload_reader_to_return = &source.pub;
  message.payload_json_to_return = &json.pub;
  json_out = NULL;
  written = 0U;

  rc = lc_message_ack(&message.pub, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_message_nack(&message.pub, &nack_req, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_message_extend(&message.pub, &extend_req, &error);
  assert_int_equal(rc, LC_OK);
  state_out = lc_message_state(&message.pub);
  assert_ptr_equal(state_out, &lease.pub);
  payload_out = lc_message_payload(&message.pub);
  assert_ptr_equal(payload_out, &source.pub);
  rc = lc_message_payload_json(&message.pub, &json_out, &error);
  assert_int_equal(rc, LC_OK);
  assert_ptr_equal(json_out, &json.pub);
  rc = lc_message_rewind_payload(&message.pub, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_message_write_payload(&message.pub, &sink.pub, &written, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(written, 7U);

  assert_int_equal(message.ack_call.count, 1);
  assert_int_equal(message.nack_call.count, 1);
  assert_int_equal(message.extend_call.count, 1);
  assert_int_equal(message.state_call.count, 1);
  assert_int_equal(message.payload_reader_call.count, 1);
  assert_int_equal(message.payload_json_call.count, 1);
  assert_int_equal(message.rewind_payload_call.count, 1);
  assert_int_equal(message.write_payload_call.count, 1);

  lc_message_close(&message.pub);
  assert_int_equal(message.close_calls, 1);

  rc = lc_consumer_service_start(&service.pub, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_consumer_service_run(&service.pub, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_consumer_service_stop(&service.pub);
  assert_int_equal(rc, LC_OK);
  rc = lc_consumer_service_wait(&service.pub, &error);
  assert_int_equal(rc, LC_OK);

  assert_int_equal(service.start_call.count, 1);
  assert_int_equal(service.run_call.count, 1);
  assert_int_equal(service.stop_call.count, 1);
  assert_int_equal(service.wait_call.count, 1);

  lc_consumer_service_close(&service.pub);
  assert_int_equal(service.close_calls, 1);
  lc_error_cleanup(&error);
}

static void test_stream_and_json_close_wrappers_delegate(void **state) {
  lc_public_mock_source source;
  lc_public_mock_sink sink;
  lc_public_mock_json json;

  (void)state;
  lc_public_mock_source_init(&source);
  lc_public_mock_sink_init(&sink);
  lc_public_mock_json_init(&json);

  lc_source_close(&source.pub);
  lc_sink_close(&sink.pub);
  lc_json_close(&json.pub);

  assert_int_equal(source.close_calls, 1);
  assert_int_equal(sink.close_calls, 1);
  assert_int_equal(json.close_calls, 1);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_client_wrappers_delegate_full_public_surface),
      cmocka_unit_test(test_lease_wrappers_delegate_full_public_surface),
      cmocka_unit_test(
          test_message_and_service_wrappers_delegate_full_public_surface),
      cmocka_unit_test(test_stream_and_json_close_wrappers_delegate),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}

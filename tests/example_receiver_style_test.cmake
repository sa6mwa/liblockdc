if(NOT DEFINED LOCKDC_ROOT)
  message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

set(lockdc_examples_dir "${LOCKDC_ROOT}/examples")
if(NOT IS_DIRECTORY "${lockdc_examples_dir}")
  message(FATAL_ERROR "examples directory not found: ${lockdc_examples_dir}")
endif()

set(lockdc_receiver_function_names
  lc_acquire
  lc_describe
  lc_get
  lc_load
  lc_update
  lc_mutate
  lc_metadata
  lc_remove
  lc_keepalive
  lc_release
  lc_attach
  lc_list_attachments
  lc_get_attachment
  lc_delete_attachment
  lc_delete_all_attachments
  lc_queue_stats
  lc_queue_ack
  lc_queue_nack
  lc_queue_extend
  lc_query
  lc_query_keys
  lc_get_namespace_config
  lc_update_namespace_config
  lc_flush_index
  lc_txn_replay
  lc_txn_prepare
  lc_txn_commit
  lc_txn_rollback
  lc_tc_lease_acquire
  lc_tc_lease_renew
  lc_tc_lease_release
  lc_tc_leader
  lc_tc_cluster_announce
  lc_tc_cluster_leave
  lc_tc_cluster_list
  lc_tc_rm_register
  lc_tc_rm_unregister
  lc_tc_rm_list
  lc_enqueue
  lc_dequeue
  lc_dequeue_batch
  lc_dequeue_with_state
  lc_subscribe
  lc_subscribe_with_state
  lc_client_new_consumer_service
  lc_watch_queue
  lc_lease_describe
  lc_lease_get
  lc_lease_load
  lc_lease_save
  lc_lease_update
  lc_lease_mutate
  lc_lease_mutate_local
  lc_lease_metadata
  lc_lease_remove
  lc_lease_keepalive
  lc_lease_release
  lc_lease_attach
  lc_lease_list_attachments
  lc_lease_get_attachment
  lc_lease_delete_attachment
  lc_lease_delete_all_attachments
  lc_message_ack
  lc_message_nack
  lc_message_extend
  lc_message_rewind_payload
  lc_message_write_payload
  lc_consumer_service_run
  lc_consumer_service_start
  lc_consumer_service_stop
  lc_consumer_service_wait
)

file(GLOB lockdc_example_sources "${lockdc_examples_dir}/*.c")
if(NOT lockdc_example_sources)
  message(FATAL_ERROR "no example sources found in ${lockdc_examples_dir}")
endif()

set(lockdc_violations "")
foreach(lockdc_example IN LISTS lockdc_example_sources)
  file(READ "${lockdc_example}" lockdc_example_content)
  foreach(lockdc_function IN LISTS lockdc_receiver_function_names)
    string(REGEX MATCHALL
      "(^|[^A-Za-z0-9_])${lockdc_function}[ \t\r\n]*\\("
      lockdc_matches
      "${lockdc_example_content}"
    )
    if(lockdc_matches)
      list(APPEND lockdc_violations
        "${lockdc_example}: uses ${lockdc_function}(...) instead of the receiver method")
    endif()
  endforeach()
endforeach()

if(lockdc_violations)
  list(JOIN lockdc_violations "\n" lockdc_violation_text)
  message(FATAL_ERROR
    "examples must prefer receiver-style handle operations:\n${lockdc_violation_text}")
endif()

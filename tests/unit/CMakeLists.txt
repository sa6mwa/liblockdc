function(lc_add_unit_test target_name)
  add_executable(${target_name} ${ARGN})
  target_link_libraries(${target_name} PRIVATE ${lc_test_link_target} lc::cmocka)
  target_include_directories(${target_name} PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
    ${LOCKDC_EXTERNAL_ROOT}/curl/install/include
    ${LOCKDC_EXTERNAL_ROOT}/openssl/install/include
    ${LOCKDC_EXTERNAL_ROOT}/nghttp2/install/include
    ${LOCKDC_EXTERNAL_ROOT}/libssh2/install/include
    ${LOCKDC_EXTERNAL_ROOT}/zlib/install/include
  )
  lc_configure_c_test_target(${target_name})
  if(LOCKDC_ENABLE_COVERAGE)
    target_compile_options(${target_name} PRIVATE
      $<$<COMPILE_LANG_AND_ID:C,GNU,Clang>:--coverage -O0 -g>
    )
    target_link_options(${target_name} PRIVATE
      $<$<LINK_LANGUAGE:C>:--coverage>
    )
  endif()
  add_test(NAME ${target_name} COMMAND ${target_name})
  # The QEMU release suite exercises every portable core unit target.  Keep
  # protocol edge-case permutations on the native sanitizer and host-release
  # suites, where they provide the same coverage without emulator overhead.
  set_tests_properties(${target_name} PROPERTIES
    LABELS "unit;cross-runtime"
    TIMEOUT 60)
  if(LOCKDC_C_TEST_ENVIRONMENT)
    set_tests_properties(${target_name} PROPERTIES
      ENVIRONMENT "${LOCKDC_C_TEST_ENVIRONMENT}")
  endif()
endfunction()

function(lc_add_https_unit_test_case target_name case_macro)
  add_executable(${target_name}
    test_lc_transport_https.c
    ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c
  )
  target_link_libraries(${target_name} PRIVATE
    ${lc_test_link_target}
    lc::cmocka
    lc::openssl_ssl_shared
    lc::openssl_crypto_shared
    Threads::Threads
  )
  target_link_options(${target_name} PRIVATE
    $<$<LINK_LANGUAGE:C>:-Wl,--wrap=lc_strdup_local>
    $<$<LINK_LANGUAGE:C>:-Wl,--wrap=lc_client_strdup>
  )
  target_include_directories(${target_name} PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
  )
  target_compile_definitions(${target_name} PRIVATE ${case_macro})
  lc_configure_c_test_target(${target_name})
  if(LOCKDC_ENABLE_COVERAGE)
    target_compile_options(${target_name} PRIVATE
      $<$<COMPILE_LANG_AND_ID:C,GNU,Clang>:--coverage -O0 -g>
    )
    target_link_options(${target_name} PRIVATE
      $<$<LINK_LANGUAGE:C>:--coverage>
    )
  endif()
  add_test(NAME ${target_name} COMMAND ${target_name})
  set_tests_properties(${target_name} PROPERTIES TIMEOUT 60)
  if(LOCKDC_C_TEST_ENVIRONMENT)
    set_tests_properties(${target_name} PROPERTIES
      ENVIRONMENT "${LOCKDC_C_TEST_ENVIRONMENT}")
  endif()
endfunction()

function(lc_add_cross_runtime_https_unit_test_case target_name case_macro)
  lc_add_https_unit_test_case(${target_name} ${case_macro})
  set_tests_properties(${target_name} PROPERTIES
    LABELS "unit;transport;cross-runtime")
endfunction()

lc_add_unit_test(lc_unit_streams test_lc_streams.c)
add_executable(lc_unit_tmp_cleanup_probe
  test_lc_tmp_cleanup.c
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c
)
lc_configure_c_test_target(lc_unit_tmp_cleanup_probe)
set(lc_unit_tmp_cleanup_probe_command "$<TARGET_FILE:lc_unit_tmp_cleanup_probe>")
if(CMAKE_CROSSCOMPILING AND CMAKE_CROSSCOMPILING_EMULATOR)
  set(lc_unit_tmp_cleanup_probe_command
    ${CMAKE_CROSSCOMPILING_EMULATOR}
    "$<TARGET_FILE:lc_unit_tmp_cleanup_probe>"
  )
endif()
add_test(NAME lc_unit_tmp_cleanup_signal
  COMMAND "${CMAKE_COMMAND}"
    "-DLOCKDC_TMP_CLEANUP_PROBE_COMMAND=${lc_unit_tmp_cleanup_probe_command}"
    -DLOCKDC_TMP_PATH_FILE=${CMAKE_BINARY_DIR}/tmp-cleanup-signal.path
    -P "${CMAKE_SOURCE_DIR}/tests/tmp_cleanup_signal_test.cmake"
)
set_tests_properties(lc_unit_tmp_cleanup_signal PROPERTIES
  LABELS "unit;local"
  TIMEOUT 15
)
add_test(NAME lc_unit_tmp_cleanup_abort
  COMMAND "${CMAKE_COMMAND}"
    "-DLOCKDC_TMP_CLEANUP_PROBE_COMMAND=${lc_unit_tmp_cleanup_probe_command}"
    -DLOCKDC_TMP_PATH_FILE=${CMAKE_BINARY_DIR}/tmp-cleanup-abort.path
    -DLOCKDC_TMP_CLEANUP_SIGNAL=ABRT
    -P "${CMAKE_SOURCE_DIR}/tests/tmp_cleanup_signal_test.cmake"
)
set_tests_properties(lc_unit_tmp_cleanup_abort PROPERTIES
  LABELS "unit;local"
  TIMEOUT 15
)
add_test(NAME lc_unit_tmp_cleanup_auto_stale
  COMMAND "${CMAKE_COMMAND}" -E env
    "LOCKDC_TMP_CLEANUP_MODE=auto-stale"
    ${lc_unit_tmp_cleanup_probe_command}
)
set_tests_properties(lc_unit_tmp_cleanup_auto_stale PROPERTIES
  LABELS "unit;local"
  TIMEOUT 15
)
add_test(NAME lc_unit_tmp_cleanup_explicit_global_stale
  COMMAND "${CMAKE_COMMAND}" -E env
    "LOCKDC_TMP_CLEANUP_MODE=explicit-global-stale"
    ${lc_unit_tmp_cleanup_probe_command}
)
set_tests_properties(lc_unit_tmp_cleanup_explicit_global_stale PROPERTIES
  LABELS "unit;local"
  TIMEOUT 15
)
add_test(NAME lc_unit_tmp_cleanup_orphan_owner_marker
  COMMAND "${CMAKE_COMMAND}" -E env
    "LOCKDC_TMP_CLEANUP_MODE=orphan-owner-marker"
    ${lc_unit_tmp_cleanup_probe_command}
)
set_tests_properties(lc_unit_tmp_cleanup_orphan_owner_marker PROPERTIES
  LABELS "unit;local"
  TIMEOUT 15
)
add_test(NAME lc_unit_tmp_cleanup_partial_owner_marker
  COMMAND "${CMAKE_COMMAND}" -E env
    "LOCKDC_TMP_CLEANUP_MODE=partial-owner-marker"
    ${lc_unit_tmp_cleanup_probe_command}
)
set_tests_properties(lc_unit_tmp_cleanup_partial_owner_marker PROPERTIES
  LABELS "unit;local"
  TIMEOUT 15
)
add_test(NAME lc_unit_tmp_cleanup_unmarked_global_prefix
  COMMAND "${CMAKE_COMMAND}" -E env
    "LOCKDC_TMP_CLEANUP_MODE=unmarked-global-prefix"
    ${lc_unit_tmp_cleanup_probe_command}
)
set_tests_properties(lc_unit_tmp_cleanup_unmarked_global_prefix PROPERTIES
  LABELS "unit;local"
  TIMEOUT 15
)
lc_add_unit_test(lc_unit_engine_allocator test_lc_engine_allocator.c)
lc_add_unit_test(lc_unit_contracts test_lc_contracts.c mock_lc_public.c)
lc_add_unit_test(lc_unit_xid_atfork test_lc_xid_atfork.c)
target_link_options(lc_unit_xid_atfork PRIVATE
  $<$<LINK_LANGUAGE:C>:-Wl,--wrap=pthread_atfork>
)
lc_add_unit_test(lc_unit_public_mock_usage test_lc_public_mock_usage.c mock_lc_public.c)
lc_add_unit_test(lc_unit_consumer_service test_lc_consumer_service.c)
lc_add_unit_test(lc_unit_workflow test_lc_workflow.c
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
target_compile_definitions(lc_unit_workflow PRIVATE LOCKDC_TEST_BUILD=1)
# The workflow suite intentionally exercises durable expiry/recovery paths.
# Its bounded waits can exceed the generic 60-second unit cap; use the
# project-wide 120-second default rather than a 90-second near-boundary cutoff.
set_tests_properties(lc_unit_workflow PROPERTIES TIMEOUT 120)
target_link_options(lc_unit_consumer_service PRIVATE
  $<$<LINK_LANGUAGE:C>:-Wl,--wrap=lc_client_open>
  $<$<LINK_LANGUAGE:C>:-Wl,--wrap=lc_engine_client_subscribe>
  $<$<LINK_LANGUAGE:C>:-Wl,--wrap=lc_engine_client_subscribe_with_state>
  $<$<LINK_LANGUAGE:C>:-Wl,--wrap=lc_message_new>
  $<$<LINK_LANGUAGE:C>:-Wl,--wrap=pthread_create>
  $<$<LINK_LANGUAGE:C>:-Wl,--wrap=pthread_join>
  $<$<LINK_LANGUAGE:C>:-Wl,--wrap=pthread_mutex_init>
  $<$<LINK_LANGUAGE:C>:-Wl,--wrap=pthread_mutex_destroy>
  $<$<LINK_LANGUAGE:C>:-Wl,--wrap=pthread_cond_init>
  $<$<LINK_LANGUAGE:C>:-Wl,--wrap=pthread_cond_destroy>
)
lc_add_unit_test(lc_unit_runtime test_lc_runtime.c)
target_link_options(lc_unit_runtime PRIVATE
  $<$<LINK_LANGUAGE:C>:-Wl,--wrap=lonejson_new>
)
lc_add_unit_test(lc_unit_mutate_stream test_lc_mutate_stream.c)
target_sources(lc_unit_mutate_stream PRIVATE
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c
)
lc_add_unit_test(lc_unit_pouch test_lc_pouch.c)
if(CMAKE_CROSSCOMPILING AND CMAKE_CROSSCOMPILING_EMULATOR)
  set(lc_pouch_cross_emulator ${CMAKE_CROSSCOMPILING_EMULATOR})
  list(GET lc_pouch_cross_emulator 0 lc_pouch_cross_emulator_program)
  if(NOT IS_ABSOLUTE "${lc_pouch_cross_emulator_program}")
    find_program(lc_pouch_cross_emulator_program_path
      NAMES "${lc_pouch_cross_emulator_program}"
      REQUIRED
    )
    list(REMOVE_AT lc_pouch_cross_emulator 0)
    list(INSERT lc_pouch_cross_emulator 0
      "${lc_pouch_cross_emulator_program_path}"
    )
  endif()
  string(REPLACE ";" "|" lc_pouch_cross_emulator_spec
    "${lc_pouch_cross_emulator}")
  target_compile_definitions(lc_unit_pouch PRIVATE
    "LOCKDC_TEST_CROSS_EMULATOR_SPEC=\"${lc_pouch_cross_emulator_spec}\""
  )
endif()
# Pouch combines logstore recovery and durability cases in one debug target;
# its debug execution exceeds the default unit-test window.
set_tests_properties(lc_unit_pouch PROPERTIES TIMEOUT 300)
target_sources(lc_unit_pouch PRIVATE
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c
)
target_compile_definitions(lc_unit_pouch PRIVATE LOCKDC_TEST_BUILD=1)
target_sources(lc_unit_streams PRIVATE
  ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c
)

lc_add_https_unit_test_case(
  lc_unit_transport_https_client_open_rejects_bundle_without_ca
  LC_HTTPS_CASE_CLIENT_OPEN_REJECTS_BUNDLE_WITHOUT_CA
)
lc_add_cross_runtime_https_unit_test_case(
  lc_unit_transport_https_client_open_accepts_memory_bundle_source
  LC_HTTPS_CASE_CLIENT_OPEN_ACCEPTS_MEMORY_BUNDLE_SOURCE
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_client_open_propagates_callback_bundle_source_failure
  LC_HTTPS_CASE_CLIENT_OPEN_PROPAGATES_CALLBACK_BUNDLE_SOURCE_FAILURE
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_client_open_accepts_fd_bundle_source
  LC_HTTPS_CASE_CLIENT_OPEN_ACCEPTS_FD_BUNDLE_SOURCE
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_client_open_accepts_chunked_callback_bundle_source
  LC_HTTPS_CASE_PUBLIC_CLIENT_OPEN_ACCEPTS_CHUNKED_CALLBACK_BUNDLE_SOURCE
)
lc_add_cross_runtime_https_unit_test_case(
  lc_unit_transport_https_public_client_namespaced_load_rejects_empty_namespace
  LC_HTTPS_CASE_PUBLIC_CLIENT_NAMESPACED_LOAD_REJECTS_EMPTY_NAMESPACE
)
lc_add_cross_runtime_https_unit_test_case(
  lc_unit_transport_https_state_paths_use_mtls
  LC_HTTPS_CASE_STATE_PATHS_USE_MTLS
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_state_transport_honors_client_cancel
  LC_HTTPS_CASE_STATE_TRANSPORT_HONORS_CLIENT_CANCEL
)
lc_add_cross_runtime_https_unit_test_case(
  lc_unit_transport_https_state_parses_buffered_typed_json_response
  LC_HTTPS_CASE_STATE_PARSES_BUFFERED_TYPED_JSON_RESPONSE
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_state_accepts_numeric_headers_with_trailing_ows
  LC_HTTPS_CASE_STATE_ACCEPTS_NUMERIC_HEADERS_WITH_TRAILING_OWS
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_state_rejects_invalid_numeric_headers_as_protocol
  LC_HTTPS_CASE_STATE_REJECTS_INVALID_NUMERIC_HEADERS_AS_PROTOCOL
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_management_paths_use_mtls
  LC_HTTPS_CASE_MANAGEMENT_PATHS_USE_MTLS
)
lc_add_cross_runtime_https_unit_test_case(
  lc_unit_transport_https_queue_paths_use_mtls
  LC_HTTPS_CASE_QUEUE_PATHS_USE_MTLS
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_queue_rejects_oversized_error_body
  LC_HTTPS_CASE_QUEUE_REJECTS_OVERSIZED_ERROR_BODY
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_watch_rejects_oversized_error_body
  LC_HTTPS_CASE_WATCH_REJECTS_OVERSIZED_ERROR_BODY
)
lc_add_cross_runtime_https_unit_test_case(
  lc_unit_transport_https_watch_filters_and_finishes_trailing_event
  LC_HTTPS_CASE_WATCH_FILTERS_AND_FINISHES_TRAILING_EVENT
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_watch_rejects_malformed_selected_event
  LC_HTTPS_CASE_WATCH_REJECTS_MALFORMED_SELECTED_EVENT
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_watch_rejects_oversized_line
  LC_HTTPS_CASE_WATCH_REJECTS_OVERSIZED_LINE
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_watch_rejects_oversized_event_data_after_prior_event
  LC_HTTPS_CASE_WATCH_REJECTS_OVERSIZED_EVENT_DATA_AFTER_PRIOR_EVENT
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_queue_rejects_overflowing_numeric_fields
  LC_HTTPS_CASE_QUEUE_REJECTS_OVERFLOWING_NUMERIC_FIELDS
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_queue_preserves_typed_json_parse_errors
  LC_HTTPS_CASE_QUEUE_PRESERVES_TYPED_JSON_PARSE_ERRORS
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_subscribe_accepts_content_length_with_trailing_ows
  LC_HTTPS_CASE_SUBSCRIBE_ACCEPTS_CONTENT_LENGTH_WITH_TRAILING_OWS
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_subscribe_rejects_oversized_header_line
  LC_HTTPS_CASE_SUBSCRIBE_REJECTS_OVERSIZED_HEADER_LINE
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_subscribe_rejects_oversized_boundary
  LC_HTTPS_CASE_SUBSCRIBE_REJECTS_OVERSIZED_BOUNDARY
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_subscribe_rejects_too_many_headers
  LC_HTTPS_CASE_SUBSCRIBE_REJECTS_TOO_MANY_HEADERS
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_subscribe_rejects_oversized_header_after_completed_delivery
  LC_HTTPS_CASE_SUBSCRIBE_REJECTS_OVERSIZED_HEADER_AFTER_COMPLETED_DELIVERY
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_subscribe_respects_client_meta_limit
  LC_HTTPS_CASE_SUBSCRIBE_RESPECTS_CLIENT_META_LIMIT
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_subscribe_rejects_default_meta_overflow
  LC_HTTPS_CASE_SUBSCRIBE_REJECTS_DEFAULT_META_OVERFLOW
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_subscribe_rejects_payload_without_content_length
  LC_HTTPS_CASE_SUBSCRIBE_REJECTS_PAYLOAD_WITHOUT_CONTENT_LENGTH
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_subscribe_rejects_missing_closing_boundary
  LC_HTTPS_CASE_SUBSCRIBE_REJECTS_MISSING_CLOSING_BOUNDARY
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_subscribe_propagates_payload_callback_failure
  LC_HTTPS_CASE_SUBSCRIBE_PROPAGATES_PAYLOAD_CALLBACK_FAILURE
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_subscribe_treats_cancelled_callback_failure_as_ok
  LC_HTTPS_CASE_SUBSCRIBE_TREATS_CANCELLED_CALLBACK_FAILURE_AS_OK
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_subscribe_treats_cancelled_finish_failure_as_ok
  LC_HTTPS_CASE_SUBSCRIBE_TREATS_CANCELLED_FINISH_FAILURE_AS_OK
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_client_emits_pslog_messages
  LC_HTTPS_CASE_PUBLIC_CLIENT_EMITS_PSLOG_MESSAGES
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_client_can_disable_sdk_sys_field
  LC_HTTPS_CASE_PUBLIC_CLIENT_CAN_DISABLE_SDK_SYS_FIELD
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_bound_lease_methods_emit_logs
  LC_HTTPS_CASE_PUBLIC_BOUND_LEASE_METHODS_EMIT_LOGS
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_lease_attach_rejects_malformed_json_response
  LC_HTTPS_CASE_PUBLIC_LEASE_ATTACH_REJECTS_MALFORMED_JSON_RESPONSE
)
lc_add_cross_runtime_https_unit_test_case(
  lc_unit_transport_https_public_lease_attach_retries_node_passive_and_cleans_parser_state
  LC_HTTPS_CASE_PUBLIC_LEASE_ATTACH_RETRIES_NODE_PASSIVE_AND_CLEANS_PARSER_STATE
)
lc_add_cross_runtime_https_unit_test_case(
  lc_unit_transport_https_public_attachment_get_preserves_i64_timestamp_headers
  LC_HTTPS_CASE_PUBLIC_ATTACHMENT_GET_PRESERVES_I64_TIMESTAMP_HEADERS
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_lease_attach_rejects_non_rewindable_retry_source
  LC_HTTPS_CASE_PUBLIC_LEASE_ATTACH_REJECTS_NON_REWINDABLE_RETRY_SOURCE
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_client_update_rejects_non_rewindable_retry_source
  LC_HTTPS_CASE_PUBLIC_CLIENT_UPDATE_REJECTS_NON_REWINDABLE_RETRY_SOURCE
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_lease_update_rejects_non_rewindable_retry_source
  LC_HTTPS_CASE_PUBLIC_LEASE_UPDATE_REJECTS_NON_REWINDABLE_RETRY_SOURCE
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_queue_retries_node_passive_and_cleans_parser_state
  LC_HTTPS_CASE_QUEUE_RETRIES_NODE_PASSIVE_AND_CLEANS_PARSER_STATE
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_enqueue_from_retries_node_passive_and_cleans_parser_state
  LC_HTTPS_CASE_ENQUEUE_FROM_RETRIES_NODE_PASSIVE_AND_CLEANS_PARSER_STATE
)
lc_add_cross_runtime_https_unit_test_case(
  lc_unit_transport_https_enqueue_from_checks_public_metadata_range
  LC_HTTPS_CASE_ENQUEUE_FROM_CHECKS_PUBLIC_METADATA_RANGE
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_enqueue_from_rejects_non_rewindable_retry_source
  LC_HTTPS_CASE_ENQUEUE_FROM_REJECTS_NON_REWINDABLE_RETRY_SOURCE
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_lease_save_uses_mapped_lonejson_upload
  LC_HTTPS_CASE_PUBLIC_LEASE_SAVE_USES_MAPPED_LONEJSON_UPLOAD
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_lease_save_ignores_response_limit_for_request_body
  LC_HTTPS_CASE_PUBLIC_LEASE_SAVE_IGNORES_RESPONSE_LIMIT_FOR_REQUEST_BODY
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_lease_get_refreshes_state_view
  LC_HTTPS_CASE_PUBLIC_LEASE_GET_REFRESHES_STATE_VIEW
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_client_load_preserves_preinitialized_json_value_capture
  LC_HTTPS_CASE_PUBLIC_CLIENT_LOAD_PRESERVES_PREINITIALIZED_JSON_VALUE_CAPTURE
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_client_load_clears_mixed_json_value_mapped_destination
  LC_HTTPS_CASE_PUBLIC_CLIENT_LOAD_CLEARS_MIXED_JSON_VALUE_MAPPED_DESTINATION
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_client_load_clears_reused_mapped_destination
  LC_HTTPS_CASE_PUBLIC_CLIENT_LOAD_CLEARS_REUSED_MAPPED_DESTINATION
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_client_load_fails_on_metadata_allocation
  LC_HTTPS_CASE_PUBLIC_CLIENT_LOAD_FAILS_ON_METADATA_ALLOCATION
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_lease_load_respects_configured_json_response_limit
  LC_HTTPS_CASE_PUBLIC_LEASE_LOAD_RESPECTS_CONFIGURED_JSON_RESPONSE_LIMIT
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_client_load_respects_configured_spooled_response_limit
  LC_HTTPS_CASE_PUBLIC_CLIENT_LOAD_RESPECTS_CONFIGURED_SPOOLED_RESPONSE_LIMIT
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_lease_load_parse_failure_does_not_refresh_state_view
  LC_HTTPS_CASE_PUBLIC_LEASE_LOAD_PARSE_FAILURE_DOES_NOT_REFRESH_STATE_VIEW
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_lease_load_empty_does_not_refresh_state_view
  LC_HTTPS_CASE_PUBLIC_LEASE_LOAD_EMPTY_DOES_NOT_REFRESH_STATE_VIEW
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_lease_load_preserves_preinitialized_json_value_capture
  LC_HTTPS_CASE_PUBLIC_LEASE_LOAD_PRESERVES_PREINITIALIZED_JSON_VALUE_CAPTURE
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_lease_load_fails_on_metadata_allocation
  LC_HTTPS_CASE_PUBLIC_LEASE_LOAD_FAILS_ON_METADATA_ALLOCATION
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_client_load_parse_failure_does_not_log_success
  LC_HTTPS_CASE_PUBLIC_CLIENT_LOAD_PARSE_FAILURE_DOES_NOT_LOG_SUCCESS
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_lease_save_fails_on_state_etag_allocation
  LC_HTTPS_CASE_PUBLIC_LEASE_SAVE_FAILS_ON_STATE_ETAG_ALLOCATION
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_lease_mutate_local_covers_no_content_path
  LC_HTTPS_CASE_PUBLIC_LEASE_MUTATE_LOCAL_COVERS_NO_CONTENT_PATH
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_management_methods_emit_logs
  LC_HTTPS_CASE_PUBLIC_MANAGEMENT_METHODS_EMIT_LOGS
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_enqueue_emits_logs
  LC_HTTPS_CASE_PUBLIC_ENQUEUE_EMITS_LOGS
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_enqueue_streams_payload_from_source
  LC_HTTPS_CASE_PUBLIC_ENQUEUE_STREAMS_PAYLOAD_FROM_SOURCE
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_dequeue_emits_stream_transport_logs
  LC_HTTPS_CASE_PUBLIC_DEQUEUE_EMITS_STREAM_TRANSPORT_LOGS
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_query_stream_captures_headers_and_body
  LC_HTTPS_CASE_PUBLIC_QUERY_STREAM_CAPTURES_HEADERS_AND_BODY
)
lc_add_cross_runtime_https_unit_test_case(
  lc_unit_transport_https_public_query_keys_streams_chunks_and_headers
  LC_HTTPS_CASE_PUBLIC_QUERY_KEYS_STREAMS_CHUNKS_AND_HEADERS
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_query_keys_captures_body_metadata
  LC_HTTPS_CASE_PUBLIC_QUERY_KEYS_CAPTURES_BODY_METADATA
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_query_keys_streams_large_response_without_client_alloc
  LC_HTTPS_CASE_PUBLIC_QUERY_KEYS_STREAMS_LARGE_RESPONSE_WITHOUT_CLIENT_ALLOC
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_query_keys_propagates_chunk_callback_failure
  LC_HTTPS_CASE_PUBLIC_QUERY_KEYS_PROPAGATES_CHUNK_CALLBACK_FAILURE
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_query_keys_propagates_end_callback_failure
  LC_HTTPS_CASE_PUBLIC_QUERY_KEYS_PROPAGATES_END_CALLBACK_FAILURE
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_query_keys_rejects_malformed_keys_json
  LC_HTTPS_CASE_PUBLIC_QUERY_KEYS_REJECTS_MALFORMED_KEYS_JSON
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_query_stream_captures_trailers_after_body
  LC_HTTPS_CASE_PUBLIC_QUERY_STREAM_CAPTURES_TRAILERS_AFTER_BODY
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_query_stream_trailers_override_headers
  LC_HTTPS_CASE_PUBLIC_QUERY_STREAM_TRAILERS_OVERRIDE_HEADERS
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_query_stream_retries_node_passive_and_cleans_trailers
  LC_HTTPS_CASE_PUBLIC_QUERY_STREAM_RETRIES_NODE_PASSIVE_AND_CLEANS_TRAILERS
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_query_stream_rejects_invalid_index_seq
  LC_HTTPS_CASE_PUBLIC_QUERY_STREAM_REJECTS_INVALID_INDEX_SEQ
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_query_stream_rejects_invalid_trailer_index_seq
  LC_HTTPS_CASE_PUBLIC_QUERY_STREAM_REJECTS_INVALID_TRAILER_INDEX_SEQ
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_queue_nack_maps_enum_intents
  LC_HTTPS_CASE_PUBLIC_QUEUE_NACK_MAPS_ENUM_INTENTS
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_queue_nack_rejects_invalid_intent
  LC_HTTPS_CASE_PUBLIC_QUEUE_NACK_REJECTS_INVALID_INTENT
)

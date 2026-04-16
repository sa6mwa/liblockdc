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
  set_target_properties(${target_name}
    PROPERTIES
      C_STANDARD 99
      C_STANDARD_REQUIRED ON
      C_EXTENSIONS ON
  )
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
endfunction()

function(lc_add_https_unit_test_case target_name case_macro)
  add_executable(${target_name} test_lc_transport_https.c)
  target_link_libraries(${target_name} PRIVATE
    ${lc_test_link_target}
    lc::cmocka
    lc::openssl_ssl_shared
    lc::openssl_crypto_shared
    Threads::Threads
  )
  target_include_directories(${target_name} PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
  )
  target_compile_definitions(${target_name} PRIVATE ${case_macro})
  set_target_properties(${target_name}
    PROPERTIES
      C_STANDARD 99
      C_STANDARD_REQUIRED ON
      C_EXTENSIONS ON
  )
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
endfunction()

lc_add_unit_test(lc_unit_streams test_lc_streams.c)
lc_add_unit_test(lc_unit_engine_allocator test_lc_engine_allocator.c)
lc_add_unit_test(lc_unit_contracts test_lc_contracts.c mock_lc_public.c)
lc_add_unit_test(lc_unit_public_mock_usage test_lc_public_mock_usage.c mock_lc_public.c)
lc_add_unit_test(lc_unit_consumer_service test_lc_consumer_service.c)
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
lc_add_unit_test(lc_unit_mutate_stream test_lc_mutate_stream.c)

lc_add_https_unit_test_case(
  lc_unit_transport_https_client_open_rejects_bundle_without_ca
  LC_HTTPS_CASE_CLIENT_OPEN_REJECTS_BUNDLE_WITHOUT_CA
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_state_paths_use_mtls
  LC_HTTPS_CASE_STATE_PATHS_USE_MTLS
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
lc_add_https_unit_test_case(
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
  lc_unit_transport_https_subscribe_respects_client_meta_limit
  LC_HTTPS_CASE_SUBSCRIBE_RESPECTS_CLIENT_META_LIMIT
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_subscribe_rejects_default_meta_overflow
  LC_HTTPS_CASE_SUBSCRIBE_REJECTS_DEFAULT_META_OVERFLOW
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
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_lease_attach_retries_node_passive_and_cleans_parser_state
  LC_HTTPS_CASE_PUBLIC_LEASE_ATTACH_RETRIES_NODE_PASSIVE_AND_CLEANS_PARSER_STATE
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_lease_attach_rejects_non_rewindable_retry_source
  LC_HTTPS_CASE_PUBLIC_LEASE_ATTACH_REJECTS_NON_REWINDABLE_RETRY_SOURCE
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_queue_retries_node_passive_and_cleans_parser_state
  LC_HTTPS_CASE_QUEUE_RETRIES_NODE_PASSIVE_AND_CLEANS_PARSER_STATE
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_enqueue_from_retries_node_passive_and_cleans_parser_state
  LC_HTTPS_CASE_ENQUEUE_FROM_RETRIES_NODE_PASSIVE_AND_CLEANS_PARSER_STATE
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
  lc_unit_transport_https_public_lease_get_refreshes_state_view
  LC_HTTPS_CASE_PUBLIC_LEASE_GET_REFRESHES_STATE_VIEW
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_client_load_preserves_preinitialized_json_value_capture
  LC_HTTPS_CASE_PUBLIC_CLIENT_LOAD_PRESERVES_PREINITIALIZED_JSON_VALUE_CAPTURE
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_lease_load_respects_configured_json_response_limit
  LC_HTTPS_CASE_PUBLIC_LEASE_LOAD_RESPECTS_CONFIGURED_JSON_RESPONSE_LIMIT
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
  lc_unit_transport_https_public_client_load_parse_failure_does_not_log_success
  LC_HTTPS_CASE_PUBLIC_CLIENT_LOAD_PARSE_FAILURE_DOES_NOT_LOG_SUCCESS
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
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_query_stream_rejects_invalid_index_seq
  LC_HTTPS_CASE_PUBLIC_QUERY_STREAM_REJECTS_INVALID_INDEX_SEQ
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_queue_nack_maps_enum_intents
  LC_HTTPS_CASE_PUBLIC_QUEUE_NACK_MAPS_ENUM_INTENTS
)
lc_add_https_unit_test_case(
  lc_unit_transport_https_public_queue_nack_rejects_invalid_intent
  LC_HTTPS_CASE_PUBLIC_QUEUE_NACK_REJECTS_INVALID_INTENT
)

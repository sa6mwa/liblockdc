function(lc_add_unit_test target_name)
  add_executable(${target_name} ${ARGN})
  target_link_libraries(${target_name} PRIVATE ${lc_test_link_target} lc::cmocka)
  target_include_directories(${target_name} PRIVATE ${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR}/src)
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

function(lc_add_cmocka_filtered_test test_name target_name filter)
  add_test(
    NAME ${test_name}
    COMMAND ${CMAKE_COMMAND} -E env CMOCKA_TEST_FILTER=${filter} $<TARGET_FILE:${target_name}>
  )
  set_tests_properties(${test_name} PROPERTIES TIMEOUT 60)
endfunction()

lc_add_unit_test(lc_unit_streams test_lc_streams.c)
lc_add_unit_test(lc_unit_engine_allocator test_lc_engine_allocator.c)
lc_add_unit_test(lc_unit_contracts test_lc_contracts.c mock_lc_public.c)
lc_add_unit_test(lc_unit_public_mock_usage test_lc_public_mock_usage.c mock_lc_public.c)
lc_add_unit_test(lc_unit_consumer_service test_lc_consumer_service.c)
lc_add_unit_test(lc_unit_runtime test_lc_runtime.c)
lc_add_unit_test(lc_unit_mutate_stream test_lc_mutate_stream.c)

add_executable(lc_unit_transport_https test_lc_transport_https.c)
target_link_libraries(lc_unit_transport_https PRIVATE
  ${lc_test_link_target}
  lc::cmocka
  lc::openssl_ssl_shared
  lc::openssl_crypto_shared
  Threads::Threads
)
target_include_directories(lc_unit_transport_https PRIVATE
  ${CMAKE_SOURCE_DIR}/include
  ${CMAKE_SOURCE_DIR}/src
)
set_target_properties(lc_unit_transport_https
  PROPERTIES
    C_STANDARD 99
    C_STANDARD_REQUIRED ON
    C_EXTENSIONS ON
)
if(LOCKDC_ENABLE_COVERAGE)
  target_compile_options(lc_unit_transport_https PRIVATE
    $<$<COMPILE_LANG_AND_ID:C,GNU,Clang>:--coverage -O0 -g>
  )
  target_link_options(lc_unit_transport_https PRIVATE
    $<$<LINK_LANGUAGE:C>:--coverage>
  )
endif()
lc_add_cmocka_filtered_test(
  lc_unit_transport_https_client_open
  lc_unit_transport_https
  test_client_open_*
)
lc_add_cmocka_filtered_test(
  lc_unit_transport_https_state
  lc_unit_transport_https
  test_state_transport_*
)
lc_add_cmocka_filtered_test(
  lc_unit_transport_https_management
  lc_unit_transport_https
  test_management_transport_*
)
lc_add_cmocka_filtered_test(
  lc_unit_transport_https_queue
  lc_unit_transport_https
  test_queue_transport_*
)
lc_add_cmocka_filtered_test(
  lc_unit_transport_https_subscribe
  lc_unit_transport_https
  test_subscribe_*
)
lc_add_cmocka_filtered_test(
  lc_unit_transport_https_public_client
  lc_unit_transport_https
  test_public_client_*
)
lc_add_cmocka_filtered_test(
  lc_unit_transport_https_public_bound_lease
  lc_unit_transport_https
  test_public_bound_lease_*
)
lc_add_cmocka_filtered_test(
  lc_unit_transport_https_public_lease
  lc_unit_transport_https
  test_public_lease_*
)
lc_add_cmocka_filtered_test(
  lc_unit_transport_https_public_management
  lc_unit_transport_https
  test_public_management_*
)
lc_add_cmocka_filtered_test(
  lc_unit_transport_https_public_enqueue
  lc_unit_transport_https
  test_public_enqueue_*
)
lc_add_cmocka_filtered_test(
  lc_unit_transport_https_public_dequeue
  lc_unit_transport_https
  test_public_dequeue_*
)
lc_add_cmocka_filtered_test(
  lc_unit_transport_https_public_query_stream
  lc_unit_transport_https
  test_public_query_stream_*
)
lc_add_cmocka_filtered_test(
  lc_unit_transport_https_public_queue_nack
  lc_unit_transport_https
  test_public_queue_nack_*
)

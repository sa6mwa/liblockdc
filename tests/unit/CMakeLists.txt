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

function(lc_add_https_unit_test_group target_name group_macro)
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
  target_compile_definitions(${target_name} PRIVATE ${group_macro})
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
lc_add_unit_test(lc_unit_runtime test_lc_runtime.c)
lc_add_unit_test(lc_unit_mutate_stream test_lc_mutate_stream.c)

lc_add_https_unit_test_group(
  lc_unit_transport_https_client_open
  LC_HTTPS_TEST_GROUP_CLIENT_OPEN
)
lc_add_https_unit_test_group(
  lc_unit_transport_https_state
  LC_HTTPS_TEST_GROUP_STATE
)
lc_add_https_unit_test_group(
  lc_unit_transport_https_management
  LC_HTTPS_TEST_GROUP_MANAGEMENT
)
lc_add_https_unit_test_group(
  lc_unit_transport_https_queue
  LC_HTTPS_TEST_GROUP_QUEUE
)
lc_add_https_unit_test_group(
  lc_unit_transport_https_subscribe
  LC_HTTPS_TEST_GROUP_SUBSCRIBE
)
lc_add_https_unit_test_group(
  lc_unit_transport_https_public_client
  LC_HTTPS_TEST_GROUP_PUBLIC_CLIENT
)
lc_add_https_unit_test_group(
  lc_unit_transport_https_public_bound_lease
  LC_HTTPS_TEST_GROUP_PUBLIC_BOUND_LEASE
)
lc_add_https_unit_test_group(
  lc_unit_transport_https_public_lease
  LC_HTTPS_TEST_GROUP_PUBLIC_LEASE
)
lc_add_https_unit_test_group(
  lc_unit_transport_https_public_management
  LC_HTTPS_TEST_GROUP_PUBLIC_MANAGEMENT
)
lc_add_https_unit_test_group(
  lc_unit_transport_https_public_enqueue
  LC_HTTPS_TEST_GROUP_PUBLIC_ENQUEUE
)
lc_add_https_unit_test_group(
  lc_unit_transport_https_public_dequeue
  LC_HTTPS_TEST_GROUP_PUBLIC_DEQUEUE
)
lc_add_https_unit_test_group(
  lc_unit_transport_https_public_query_stream
  LC_HTTPS_TEST_GROUP_PUBLIC_QUERY_STREAM
)
lc_add_https_unit_test_group(
  lc_unit_transport_https_public_queue_nack
  LC_HTTPS_TEST_GROUP_PUBLIC_QUEUE_NACK
)

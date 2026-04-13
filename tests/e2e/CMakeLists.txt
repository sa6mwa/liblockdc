function(lc_add_e2e_shard target_name group_macro)
  add_executable(${target_name} test_lc_e2e.c)
  target_link_libraries(${target_name} PRIVATE ${lc_test_link_target} lc::cmocka)
  target_include_directories(${target_name} PRIVATE ${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR}/src)
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
  set_tests_properties(${target_name} PROPERTIES
    LABELS "e2e"
    RUN_SERIAL TRUE
    TIMEOUT 300
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  )
endfunction()

lc_add_e2e_shard(lc_e2e_disk_direct LC_E2E_GROUP_DISK_DIRECT)
lc_add_e2e_shard(lc_e2e_s3_direct LC_E2E_GROUP_S3_DIRECT)
lc_add_e2e_shard(lc_e2e_mem_direct LC_E2E_GROUP_MEM_DIRECT)
lc_add_e2e_shard(lc_e2e_disk_consumer LC_E2E_GROUP_DISK_CONSUMER)
lc_add_e2e_shard(lc_e2e_s3_consumer LC_E2E_GROUP_S3_CONSUMER)
lc_add_e2e_shard(lc_e2e_mem_consumer LC_E2E_GROUP_MEM_CONSUMER)

if(TARGET lc_example_acquire_lease_lifecycle)
  add_test(NAME lc_example_smoke_acquire_lease_lifecycle
    COMMAND $<TARGET_FILE:lc_example_acquire_lease_lifecycle>
  )
  set_tests_properties(lc_example_smoke_acquire_lease_lifecycle PROPERTIES
    LABELS "e2e;examples"
    RUN_SERIAL TRUE
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  )
endif()

if(TARGET lc_example_client_and_lease_methods)
  add_test(NAME lc_example_smoke_client_and_lease_methods
    COMMAND $<TARGET_FILE:lc_example_client_and_lease_methods>
  )
  set_tests_properties(lc_example_smoke_client_and_lease_methods PROPERTIES
    LABELS "e2e;examples"
    RUN_SERIAL TRUE
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  )
endif()

if(TARGET lc_example_management_admin)
  add_test(NAME lc_example_smoke_management_admin
    COMMAND $<TARGET_FILE:lc_example_management_admin>
  )
  set_tests_properties(lc_example_smoke_management_admin PROPERTIES
    LABELS "e2e;examples"
    RUN_SERIAL TRUE
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  )
endif()

if(TARGET lc_example_curl_fetch_into_lockd)
  add_test(NAME lc_example_smoke_curl_fetch_into_lockd
    COMMAND $<TARGET_FILE:lc_example_curl_fetch_into_lockd>
  )
  set_tests_properties(lc_example_smoke_curl_fetch_into_lockd PROPERTIES
    LABELS "e2e;examples"
    RUN_SERIAL TRUE
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  )
endif()

if(TARGET lc_example_local_mutate)
  add_test(NAME lc_example_smoke_local_mutate
    COMMAND $<TARGET_FILE:lc_example_local_mutate>
      -k lc-e2e-local-mutate
      -m /kind="example-local-mutate"
      -m /counter=1
      -m time:/ts=NOW
      -m textfile:/note=${CMAKE_SOURCE_DIR}/examples/data/state_stream_input.json
  )
  set_tests_properties(lc_example_smoke_local_mutate PROPERTIES
    LABELS "e2e;examples"
    PASS_REGULAR_EXPRESSION "\"kind\":\"example-local-mutate\""
    RUN_SERIAL TRUE
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  )
endif()

if(TARGET lc_example_startconsumer)
  add_test(NAME lc_example_smoke_startconsumer
    COMMAND $<TARGET_FILE:lc_example_startconsumer>
  )
  set_tests_properties(lc_example_smoke_startconsumer PROPERTIES
    LABELS "e2e;examples"
    PASS_REGULAR_EXPRESSION "\\{\"counter\":3\\}"
    RUN_SERIAL TRUE
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  )
endif()

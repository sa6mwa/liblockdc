add_executable(lc_e2e test_lc_e2e.c)
target_link_libraries(lc_e2e PRIVATE ${lc_test_link_target} lc::cmocka)
target_include_directories(lc_e2e PRIVATE ${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR}/src)
set_target_properties(lc_e2e
  PROPERTIES
    C_STANDARD 99
    C_STANDARD_REQUIRED ON
    C_EXTENSIONS ON
)
if(LOCKDC_ENABLE_COVERAGE)
  target_compile_options(lc_e2e PRIVATE
    $<$<COMPILE_LANG_AND_ID:C,GNU,Clang>:--coverage -O0 -g>
  )
  target_link_options(lc_e2e PRIVATE
    $<$<LINK_LANGUAGE:C>:--coverage>
  )
endif()
add_test(NAME lc_e2e COMMAND lc_e2e)
set_tests_properties(lc_e2e PROPERTIES
  LABELS "e2e"
  RUN_SERIAL TRUE
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

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

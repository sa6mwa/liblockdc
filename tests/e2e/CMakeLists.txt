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

function(lc_add_lua_e2e_test test_name script_path)
  if(NOT TARGET lockdc_lua_core)
    return()
  endif()
  if(NOT LOCKDC_LUA_BIN OR NOT LOCKDC_LUAROCKS_BIN)
    return()
  endif()

  set(options)
  set(one_value_args ENV_OVERRIDES)
  cmake_parse_arguments(LUA_E2E "${options}" "${one_value_args}" "" ${ARGN})

  set(lua_e2e_command
    "${CMAKE_COMMAND}"
      -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
      -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
      -DLOCKDC_TEST_NAME=${test_name}
      -DLOCKDC_LUA_TEST_SCRIPT=${script_path}
      -P "${CMAKE_SOURCE_DIR}/tests/lua_rock_install_and_run_test.cmake"
  )
  if(LUA_E2E_ENV_OVERRIDES)
    list(APPEND lua_e2e_command "-DLOCKDC_LUA_TEST_ENV=${LUA_E2E_ENV_OVERRIDES}")
  endif()

  add_test(NAME ${test_name} COMMAND ${lua_e2e_command})
  set_tests_properties(${test_name} PROPERTIES
    LABELS "e2e;lua"
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

find_program(LOCKDC_LUA_BIN NAMES lua lua5.5)
find_program(LOCKDC_LUAROCKS_BIN NAMES luarocks)

lc_add_lua_e2e_test(
  lc_e2e_lua_example_acquire_update_json
  "${CMAKE_SOURCE_DIR}/examples/lua/acquire_update_json.lua"
  ENV_OVERRIDES "LOCKDC_KEY=tests/lua/e2e-acquire-update-json;LOCKDC_OWNER=lua-e2e-acquire-update-json"
)

lc_add_lua_e2e_test(
  lc_e2e_lua_example_queue_roundtrip
  "${CMAKE_SOURCE_DIR}/examples/lua/queue_roundtrip.lua"
  ENV_OVERRIDES "LOCKDC_QUEUE=tests-lua-e2e-queue-roundtrip;LOCKDC_OWNER=lua-e2e-queue-roundtrip"
)

lc_add_lua_e2e_test(
  lc_e2e_lua_example_namespace_config
  "${CMAKE_SOURCE_DIR}/examples/lua/namespace_config.lua"
)

lc_add_lua_e2e_test(
  lc_e2e_lua_consumer_single_message
  "${CMAKE_SOURCE_DIR}/tests/e2e/test_lockdc_lua_consumer.lua"
  ENV_OVERRIDES "LOCKDC_QUEUE=tests-lua-e2e-consumer;LOCKDC_OWNER=lua-e2e-consumer"
)

lc_add_lua_e2e_test(
  lc_e2e_lua_consumer_handler_error
  "${CMAKE_SOURCE_DIR}/tests/e2e/test_lockdc_lua_consumer_handler_error.lua"
  ENV_OVERRIDES "LOCKDC_QUEUE=tests-lua-e2e-consumer-handler-error;LOCKDC_OWNER=lua-e2e-consumer-handler-error"
)

lc_add_lua_e2e_test(
  lc_e2e_lua_state_after_ack
  "${CMAKE_SOURCE_DIR}/tests/e2e/test_lockdc_lua_state_after_ack.lua"
  ENV_OVERRIDES "LOCKDC_QUEUE=tests-lua-e2e-state-after-ack;LOCKDC_OWNER=lua-e2e-state-after-ack"
)

lc_add_lua_e2e_test(
  lc_e2e_lua_bundle_sources
  "${CMAKE_SOURCE_DIR}/tests/e2e/test_lockdc_lua_bundle_sources.lua"
  ENV_OVERRIDES "LOCKDC_KEY_PREFIX=tests/lua/e2e-bundle-sources;LOCKDC_OWNER=lua-e2e-bundle-sources"
)

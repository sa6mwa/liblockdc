function(lc_add_integration_test target_name)
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
  set_tests_properties(${target_name} PROPERTIES
    LABELS "integration;offline"
    RUN_SERIAL TRUE
    TIMEOUT 120)
  if(LOCKDC_C_TEST_ENVIRONMENT)
    set_tests_properties(${target_name} PROPERTIES
      ENVIRONMENT "${LOCKDC_C_TEST_ENVIRONMENT}")
  endif()
endfunction()

if(TARGET lc_static AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
  add_executable(lc_pouch_compaction_stack_test test_pouch_compaction_stack.c
    ${CMAKE_SOURCE_DIR}/tests/support/lc_test_tmp.c)
  target_link_libraries(lc_pouch_compaction_stack_test PRIVATE lc_static)
  target_include_directories(lc_pouch_compaction_stack_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  lc_configure_c_test_target(lc_pouch_compaction_stack_test)
  if(LOCKDC_TARGET_LIBC STREQUAL "musl")
    target_link_options(lc_pouch_compaction_stack_test PRIVATE -static)
  endif()
  foreach(mode background small-stack)
    add_test(NAME pouch_compaction_${mode}_test COMMAND lc_pouch_compaction_stack_test ${mode})
    set_tests_properties(pouch_compaction_${mode}_test PROPERTIES
      LABELS "integration;offline;pouch;cross-runtime" RUN_SERIAL TRUE TIMEOUT 60)
    if(LOCKDC_C_TEST_ENVIRONMENT)
      set_tests_properties(pouch_compaction_${mode}_test PROPERTIES
        ENVIRONMENT "${LOCKDC_C_TEST_ENVIRONMENT}")
    endif()
  endforeach()
  set_property(GLOBAL APPEND PROPERTY LOCKDC_CROSS_RUNTIME_TEST_TARGETS
    lc_pouch_compaction_stack_test)
endif()

if(LOCKDC_BUILD_POSTGRES AND LOCKDC_TARGET_ID STREQUAL "x86_64-linux-gnu"
   AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
  set(postgres_xa_executables)
  foreach(variant static shared)
    if(NOT TARGET lc_postgres_${variant})
      continue()
    endif()
    add_executable(lc_integration_postgres_${variant} test_postgres_xa.c)
    target_link_libraries(lc_integration_postgres_${variant} PRIVATE lc_postgres_${variant})
    lc_configure_c_test_target(lc_integration_postgres_${variant})
    if(variant STREQUAL "static")
      target_compile_definitions(lc_integration_postgres_${variant} PRIVATE LC_POSTGRES_FAULT_TEST=1)
      target_link_options(lc_integration_postgres_${variant} PRIVATE
        "LINKER:--wrap=cpkt_postgres_send_query_params"
        "LINKER:--wrap=cpkt_postgres_get_result_checked"
        "LINKER:--wrap=lc_xa_store_progress_blocking")
    else()
      target_link_libraries(lc_integration_postgres_${variant} PRIVATE
        lc::pslog_shared lc::lonejson_shared lc::liblql_shared)
      target_link_options(lc_integration_postgres_${variant} PRIVATE
        "LINKER:-rpath,${LOCKDC_EXTERNAL_ROOT}/cpktdb/install/lib")
    endif()
    list(APPEND postgres_xa_executables "$<TARGET_FILE:lc_integration_postgres_${variant}>")
  endforeach()
  if(TARGET lc_lua_xa_test)
    foreach(lua_mode embedded module static)
      if(lua_mode STREQUAL "static" AND NOT TARGET lc_postgres_static)
        continue()
      endif()
      set(lua_target lc_integration_lua_postgres)
      if(NOT lua_mode STREQUAL "embedded")
        string(APPEND lua_target "_${lua_mode}")
      endif()
      add_executable(${lua_target}
        test_lua_postgres_xa.c
        ${CMAKE_SOURCE_DIR}/src/lua/lockdc_lua.c
        ${CMAKE_SOURCE_DIR}/src/lua/lockdc_xa_lua.c
        ${CMAKE_SOURCE_DIR}/src/lua/lockdc_xa_resource_lua.c
        ${CMAKE_SOURCE_DIR}/src/lua/lockdc_xa_provider_lua.c
        ${CMAKE_SOURCE_DIR}/src/lua/lockdc_postgres_lua.c)
      target_include_directories(${lua_target} PRIVATE ${CMAKE_SOURCE_DIR}/src/lua)
      target_include_directories(${lua_target} SYSTEM PRIVATE ${LUA_INCLUDE_DIR})
      target_compile_definitions(${lua_target} PRIVATE
        LOCKDC_SOURCE_ROOT="${CMAKE_SOURCE_DIR}" _POSIX_C_SOURCE=200809L)
      if(lua_mode STREQUAL "static")
        target_link_libraries(${lua_target} PRIVATE lc_postgres_static Lua::Lua)
      else()
        target_link_libraries(${lua_target} PRIVATE lc_postgres_shared Lua::Lua
          lc::curl_shared lc::openssl_ssl_shared lc::openssl_crypto_shared
          lc::nghttp2_shared lc::pslog_shared lc::lonejson_shared
          lc::liblql_shared lc::libssh2_shared lc::zlib_shared)
        target_link_options(${lua_target} PRIVATE "LINKER:--no-as-needed"
          "LINKER:-rpath,${LOCKDC_EXTERNAL_ROOT}/cpktdb/install/lib")
      endif()
      if(lua_mode STREQUAL "module")
        target_compile_definitions(${lua_target} PRIVATE LOCKDC_TEST_LOAD_MODULES=1
          LOCKDC_TEST_LUA_MODULE_PATH="${CMAKE_BINARY_DIR}/lua/?.so")
        target_link_options(${lua_target} PRIVATE "LINKER:--export-dynamic")
        add_dependencies(${lua_target} lockdc_lua_core lockdc_lua_postgres)
      endif()
      lc_configure_c_test_target(${lua_target})
      set_target_properties(${lua_target} PROPERTIES
        C_STANDARD 90 C_STANDARD_REQUIRED ON C_EXTENSIONS OFF)
      list(APPEND postgres_xa_executables "$<TARGET_FILE:${lua_target}>")
    endforeach()
  endif()
  add_test(NAME postgres_xa_native_test
    COMMAND bash "${CMAKE_SOURCE_DIR}/scripts/test-postgres-xa.sh" ${postgres_xa_executables})
  set_tests_properties(postgres_xa_native_test PROPERTIES
    LABELS "integration;postgres;podman" RUN_SERIAL TRUE TIMEOUT 240)
  foreach(server_version 15 16 17 18)
    add_test(NAME postgres_xa_pg${server_version}_test
      COMMAND bash "${CMAKE_SOURCE_DIR}/scripts/test-postgres-xa.sh"
        --server-version ${server_version} ${postgres_xa_executables})
    set_tests_properties(postgres_xa_pg${server_version}_test PROPERTIES
      LABELS "integration;postgres;podman" RUN_SERIAL TRUE TIMEOUT 240)
  endforeach()
endif()

lc_apply_test_defaults()

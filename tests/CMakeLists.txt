if(TARGET lc_test_static)
  set(lc_test_link_target lc_test_static)
elseif(TARGET lc_static)
  set(lc_test_link_target lc_static)
elseif(TARGET lc_shared)
  set(lc_test_link_target lc_shared)
else()
  return()
endif()

if(LOCKDC_EXTERNAL_ROOT)
  add_test(NAME lonejson_dependency_interface
    COMMAND "${CMAKE_COMMAND}"
      -DLOCKDC_EXTERNAL_ROOT=${LOCKDC_EXTERNAL_ROOT}
      -DLOCKDC_DEPENDENCY_BUILD_ROOT=${LOCKDC_DEPENDENCY_BUILD_ROOT}
      -P "${CMAKE_SOURCE_DIR}/tests/lonejson_dependency_interface_test.cmake"
  )
  add_test(NAME zlib_dependency_interface
    COMMAND "${CMAKE_COMMAND}"
      -DLOCKDC_EXTERNAL_ROOT=${LOCKDC_EXTERNAL_ROOT}
      -DLOCKDC_DEPENDENCY_BUILD_ROOT=${LOCKDC_DEPENDENCY_BUILD_ROOT}
      -P "${CMAKE_SOURCE_DIR}/tests/zlib_dependency_interface_test.cmake"
  )
  add_test(NAME dependency_public_surface_test
    COMMAND "${CMAKE_COMMAND}"
      -DLOCKDC_EXTERNAL_ROOT=${LOCKDC_EXTERNAL_ROOT}
      -P "${CMAKE_SOURCE_DIR}/tests/dependency_public_surface_test.cmake"
  )
endif()

add_test(NAME public_api_boundary
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/public_api_boundary_test.cmake"
)

add_test(NAME world_target_clang_guard_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/world_target_clang_guard_test.cmake"
)

add_test(NAME lua_binding_source_layout_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/lua_binding_source_layout_test.cmake"
)

add_test(NAME lua_external_sdk_contract_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/lua_external_sdk_contract_test.cmake"
)

add_test(NAME release_package_matrix_targeting_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/release_package_matrix_targeting_test.cmake"
)

add_test(NAME release_targeting_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/release_targeting_test.cmake"
)

add_test(NAME packaging_portability_source_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/packaging_portability_source_test.cmake"
)

set(LOCKDC_LUA_TEST_ENV_COMMON "")
set(lockdc_lua_test_sanitizer_flags "${CMAKE_C_FLAGS}")
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  string(APPEND lockdc_lua_test_sanitizer_flags " ${CMAKE_C_FLAGS_DEBUG}")
endif()
if(lockdc_lua_test_sanitizer_flags MATCHES "(^|[ \t])-fsanitize=([^ \t,]+,)*address([, ][^ \t,]+)*($|[ \t])")
  execute_process(
    COMMAND "${CMAKE_C_COMPILER}" -print-file-name=libasan.so
    OUTPUT_VARIABLE lockdc_lua_asan_runtime
    RESULT_VARIABLE lockdc_lua_asan_runtime_result
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  if(lockdc_lua_asan_runtime_result EQUAL 0
     AND NOT lockdc_lua_asan_runtime STREQUAL ""
     AND NOT lockdc_lua_asan_runtime STREQUAL "libasan.so")
    list(APPEND LOCKDC_LUA_TEST_ENV_COMMON "LD_PRELOAD=${lockdc_lua_asan_runtime}")
  endif()
endif()

find_program(LOCKDC_LUA_BIN NAMES lua lua5.5)
if(LOCKDC_LUA_BIN)
  add_test(NAME lua_facade_unit_test
    COMMAND "${LOCKDC_LUA_BIN}" "${CMAKE_SOURCE_DIR}/tests/lua/test_lockdc_facade_unit.lua"
  )
  set_tests_properties(lua_facade_unit_test PROPERTIES
    ENVIRONMENT "LOCKDC_ROOT=${CMAKE_SOURCE_DIR}"
  )
endif()
if(TARGET lockdc_lua_core AND LOCKDC_LUA_BIN AND NOT CMAKE_CROSSCOMPILING AND NOT LOCKDC_TARGET_LIBC STREQUAL "musl")
  add_test(NAME lua_binding_core_smoke_test
    COMMAND "${LOCKDC_LUA_BIN}" "${CMAKE_SOURCE_DIR}/tests/lua/test_lockdc_core_smoke.lua"
  )
  set(lockdc_lua_core_smoke_env
    "LOCKDC_LUA_CORE_DIR=${CMAKE_BINARY_DIR}/lua"
    ${LOCKDC_LUA_TEST_ENV_COMMON}
  )
  set_tests_properties(lua_binding_core_smoke_test PROPERTIES
    ENVIRONMENT "${lockdc_lua_core_smoke_env}"
  )
endif()

add_subdirectory(unit)
if(LOCKDC_BUILD_E2E_TESTS)
  add_subdirectory(e2e)
endif()

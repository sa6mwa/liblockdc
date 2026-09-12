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
      -DLOCKDC_LONEJSON_VERSION=${LOCKDC_LONEJSON_VERSION}
      -DLOCKDC_LONEJSON_ABI_VERSION=${LOCKDC_LONEJSON_ABI_VERSION}
      -P "${CMAKE_SOURCE_DIR}/tests/lonejson_dependency_interface_test.cmake"
  )
  add_test(NAME liblql_dependency_interface
    COMMAND "${CMAKE_COMMAND}"
      -DLOCKDC_EXTERNAL_ROOT=${LOCKDC_EXTERNAL_ROOT}
      -DLOCKDC_DEPENDENCY_BUILD_ROOT=${LOCKDC_DEPENDENCY_BUILD_ROOT}
      -DLOCKDC_LIBLQL_VERSION=${LOCKDC_LIBLQL_VERSION}
      -DLOCKDC_LIBLQL_ABI_VERSION=${LOCKDC_LIBLQL_ABI_VERSION}
      -DLOCKDC_TARGET_ARCH=${LOCKDC_TARGET_ARCH}
      -DLOCKDC_TARGET_OS=${LOCKDC_TARGET_OS}
      -DLOCKDC_TARGET_LIBC=${LOCKDC_TARGET_LIBC}
      -DLOCKDC_C_COMPILER=${CMAKE_C_COMPILER}
      -DLOCKDC_CROSSCOMPILING=${CMAKE_CROSSCOMPILING}
      -P "${CMAKE_SOURCE_DIR}/tests/liblql_dependency_interface_test.cmake"
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

add_test(NAME example_receiver_style
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/example_receiver_style_test.cmake"
)

add_test(NAME pouch_liblql_storage_boundary
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/pouch_liblql_storage_boundary_test.cmake"
)

add_test(NAME release_fuzz_targeting_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/release_fuzz_targeting_test.cmake"
)

add_test(NAME benchmark_go_timeout_contract_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/benchmark_go_timeout_contract_test.cmake"
)
set_tests_properties(benchmark_go_timeout_contract_test PROPERTIES
  LABELS "unit;local;benchmark"
  TIMEOUT 10
)

add_test(NAME lua_binding_source_layout_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/lua_binding_source_layout_test.cmake"
)

add_test(NAME lua_workflow_numeric_contract_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/lua_workflow_numeric_contract_test.cmake"
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

add_test(NAME release_werror_contract_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/release_werror_contract_test.cmake"
)
set_tests_properties(release_werror_contract_test PROPERTIES
  LABELS "unit;local"
  TIMEOUT 10
)

add_test(NAME release_compile_werror_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
    -DLOCKDC_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -P "${CMAKE_SOURCE_DIR}/tests/release_compile_werror_test.cmake"
)
set_tests_properties(release_compile_werror_test PROPERTIES
  LABELS "unit;local"
  TIMEOUT 10
)

add_test(NAME release_privacy_recursive_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/release_privacy_recursive_test.cmake"
)
set_tests_properties(release_privacy_recursive_test PROPERTIES
  LABELS "unit;local;packaging"
  TIMEOUT 10
)

add_test(NAME darwin_macho_lifecycle_contract_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/darwin_macho_lifecycle_contract_test.cmake"
)
set_tests_properties(darwin_macho_lifecycle_contract_test PROPERTIES
  LABELS "unit;local;packaging"
  TIMEOUT 10
)

add_test(NAME bootlin_toolchain_contract_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/bootlin_toolchain_contract_test.cmake"
)
set_tests_properties(bootlin_toolchain_contract_test PROPERTIES
  LABELS "unit;local"
  TIMEOUT 10
)

add_test(NAME pouch_storage_allocator_contract_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/pouch_storage_allocator_contract_test.cmake"
)
set_tests_properties(pouch_storage_allocator_contract_test PROPERTIES
  LABELS "unit;local"
  TIMEOUT 10
)

add_test(NAME packaging_portability_source_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/packaging_portability_source_test.cmake"
)

add_test(NAME package_source_dist_dir_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/package_source_dist_dir_test.cmake"
)

add_test(NAME coverage_pkgconfig_metadata_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/coverage_pkgconfig_metadata_test.cmake"
)

add_test(NAME coverage_gcov_toolchain_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_BINARY_DIR=${CMAKE_BINARY_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/coverage_gcov_toolchain_test.cmake"
)

add_test(NAME lonejson_upload_rewind_transport_test
  COMMAND "${CMAKE_COMMAND}"
    -DLOCKDC_ROOT=${CMAKE_SOURCE_DIR}
    -P "${CMAKE_SOURCE_DIR}/tests/lonejson_upload_rewind_transport_test.cmake"
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
    list(APPEND LOCKDC_LUA_TEST_ENV_COMMON "ASAN_OPTIONS=detect_leaks=0")
  endif()
endif()

find_program(LOCKDC_LUA_BIN NAMES lua5.5)
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
  if(UNIX AND NOT APPLE)
    list(APPEND lockdc_lua_core_smoke_env
      "LD_LIBRARY_PATH=${LOCKDC_EXTERNAL_ROOT}/curl/install/lib:${LOCKDC_EXTERNAL_ROOT}/openssl/install/lib:${LOCKDC_EXTERNAL_ROOT}/nghttp2/install/lib:${LOCKDC_EXTERNAL_ROOT}/pslog/install/lib:${LOCKDC_EXTERNAL_ROOT}/lonejson/install/lib:${LOCKDC_EXTERNAL_ROOT}/liblql/install/lib:${LOCKDC_EXTERNAL_ROOT}/libssh2/install/lib:${LOCKDC_EXTERNAL_ROOT}/zlib/install/lib"
    )
  endif()
  set_tests_properties(lua_binding_core_smoke_test PROPERTIES
    ENVIRONMENT "${lockdc_lua_core_smoke_env}"
  )
endif()

add_subdirectory(unit)
add_subdirectory(integration)
# Bootlin sets CMAKE_CROSSCOMPILING even for a host-architecture Linux build.
# The Python driver launches the binary directly, without a QEMU wrapper.
if(TARGET lockdc_pouch_replay_probe
   AND CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux"
   AND CMAKE_SYSTEM_PROCESSOR STREQUAL CMAKE_HOST_SYSTEM_PROCESSOR
   AND LOCKDC_TARGET_LIBC STREQUAL "gnu")
  find_program(LOCKDC_REPLAY_PYTHON NAMES python3 REQUIRED)
  foreach(replay_transform IN ITEMS plain encrypted)
    set(replay_args)
    if(replay_transform STREQUAL "encrypted")
      list(APPEND replay_args --encrypted)
    endif()
    add_test(NAME pouch_replay_${replay_transform}
      COMMAND "${LOCKDC_REPLAY_PYTHON}"
        "${CMAKE_SOURCE_DIR}/tests/e2e/pouch_replay.py"
        $<TARGET_FILE:lockdc_pouch_replay_probe>
        --keys 1 --updates 256 --shared --unclean --live-staged
        --segment-bytes 65536 ${replay_args})
    set_tests_properties(pouch_replay_${replay_transform} PROPERTIES
      LABELS "integration;offline;pouch;e2e"
      RUN_SERIAL TRUE TIMEOUT 60
      WORKING_DIRECTORY "${CMAKE_BINARY_DIR}")
  endforeach()
endif()
if(LOCKDC_BUILD_E2E_TESTS)
  add_subdirectory(e2e)
endif()
lc_apply_default_test_timeouts()

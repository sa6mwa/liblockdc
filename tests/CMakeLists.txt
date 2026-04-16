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

find_program(LOCKDC_LUA_BIN NAMES lua lua5.5)
if(TARGET lockdc_lua_core AND LOCKDC_LUA_BIN)
  add_test(NAME lua_binding_core_smoke_test
    COMMAND "${LOCKDC_LUA_BIN}" "${CMAKE_SOURCE_DIR}/tests/lua/test_lockdc_core_smoke.lua"
  )
  set_tests_properties(lua_binding_core_smoke_test PROPERTIES
    ENVIRONMENT "LOCKDC_LUA_CORE_DIR=${CMAKE_BINARY_DIR}/lua"
  )
endif()

add_subdirectory(unit)
if(LOCKDC_BUILD_E2E_TESTS)
  add_subdirectory(e2e)
endif()

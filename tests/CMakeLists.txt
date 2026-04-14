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

add_subdirectory(unit)
if(LOCKDC_BUILD_E2E_TESTS)
  add_subdirectory(e2e)
endif()

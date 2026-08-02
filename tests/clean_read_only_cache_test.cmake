if(NOT DEFINED LOCKDC_ROOT OR LOCKDC_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()
if(NOT DEFINED LOCKDC_BINARY_DIR OR LOCKDC_BINARY_DIR STREQUAL "")
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

set(test_root "${LOCKDC_BINARY_DIR}/clean-read-only-cache")
set(fake_root "${test_root}/repo")
set(read_only_module "${fake_root}/.cache/go/pkg/mod/example.test/module@v1.0.0")
set(lua_build_root "${fake_root}/.luarocks-build/lockdc")
set(shared_cache_sentinel "${test_root}/shared-cache/sentinel")

file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY
    "${fake_root}/cmake"
    "${fake_root}/scripts"
    "${fake_root}/build"
    "${fake_root}/dist"
    "${read_only_module}/nested"
    "${lua_build_root}"
    "${test_root}/shared-cache")
file(COPY_FILE "${LOCKDC_ROOT}/scripts/clean.sh" "${fake_root}/scripts/clean.sh")
file(COPY_FILE "${LOCKDC_ROOT}/scripts/assert_generated_path.sh" "${fake_root}/scripts/assert_generated_path.sh")
file(COPY_FILE "${LOCKDC_ROOT}/cmake/package_clean_dist.cmake" "${fake_root}/cmake/package_clean_dist.cmake")
file(COPY_FILE "${LOCKDC_ROOT}/cmake/LcGeneratedPath.cmake" "${fake_root}/cmake/LcGeneratedPath.cmake")
file(WRITE "${fake_root}/scripts/dev-reset.sh" "#!/usr/bin/env bash\nset -euo pipefail\n")
file(CHMOD "${fake_root}/scripts/dev-reset.sh"
    PERMISSIONS
        OWNER_READ OWNER_WRITE OWNER_EXECUTE
        GROUP_READ GROUP_EXECUTE
        WORLD_READ WORLD_EXECUTE)
file(WRITE "${read_only_module}/nested/payload.txt" "module cache payload\n")
file(WRITE "${lua_build_root}/core.so" "generated Lua module\n")
file(WRITE "${shared_cache_sentinel}" "shared cache must survive\n")

execute_process(
    COMMAND chmod -R a-w "${read_only_module}"
    RESULT_VARIABLE chmod_result
    OUTPUT_VARIABLE chmod_output
    ERROR_VARIABLE chmod_error)
if(NOT chmod_result EQUAL 0)
    message(FATAL_ERROR
        "could not create read-only module cache fixture\n"
        "stdout:\n${chmod_output}\n"
        "stderr:\n${chmod_error}")
endif()

execute_process(
    COMMAND bash "${fake_root}/scripts/clean.sh"
    RESULT_VARIABLE clean_result
    OUTPUT_VARIABLE clean_output
    ERROR_VARIABLE clean_error)
if(NOT clean_result EQUAL 0)
    message(FATAL_ERROR
        "clean did not remove a read-only local module cache\n"
        "stdout:\n${clean_output}\n"
        "stderr:\n${clean_error}")
endif()

foreach(removed_path "${fake_root}/build" "${fake_root}/dist" "${fake_root}/.cache" "${fake_root}/.luarocks-build")
    if(EXISTS "${removed_path}")
        message(FATAL_ERROR "clean left generated state behind: ${removed_path}")
    endif()
endforeach()

if(NOT EXISTS "${shared_cache_sentinel}")
    message(FATAL_ERROR "clean removed state outside the repository-local cache")
endif()

file(WRITE "${fake_root}/dist/package.txt" "generated release artifact\n")
execute_process(
    COMMAND "${CMAKE_COMMAND}"
        "-DLOCKDC_ROOT=${fake_root}"
        "-DLOCKDC_DIST_DIR=${fake_root}/dist"
        -P "${fake_root}/cmake/package_clean_dist.cmake"
    RESULT_VARIABLE clean_dist_result
    OUTPUT_VARIABLE clean_dist_output
    ERROR_VARIABLE clean_dist_error)
if(NOT clean_dist_result EQUAL 0)
    message(FATAL_ERROR
        "clean-dist rejected its repository-local generated root\n"
        "stdout:\n${clean_dist_output}\n"
        "stderr:\n${clean_dist_error}")
endif()
if(NOT EXISTS "${fake_root}/dist" OR EXISTS "${fake_root}/dist/package.txt")
    message(FATAL_ERROR "clean-dist did not reset the repository-local dist root")
endif()

set(external_sentinel "${test_root}/outside/sentinel")
file(MAKE_DIRECTORY "${test_root}/outside")
file(WRITE "${external_sentinel}" "must survive\n")
execute_process(
    COMMAND "${CMAKE_COMMAND}"
        "-DLOCKDC_ROOT=${fake_root}"
        "-DLOCKDC_DIST_DIR=${test_root}/outside"
        -P "${fake_root}/cmake/package_clean_dist.cmake"
    RESULT_VARIABLE unsafe_clean_dist_result
    OUTPUT_VARIABLE unsafe_clean_dist_output
    ERROR_VARIABLE unsafe_clean_dist_error)
if(unsafe_clean_dist_result EQUAL 0)
    message(FATAL_ERROR "clean-dist accepted an external path")
endif()
if(NOT EXISTS "${external_sentinel}")
    message(FATAL_ERROR "clean-dist removed an external path before rejecting it")
endif()

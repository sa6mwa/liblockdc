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
    "${fake_root}/scripts"
    "${fake_root}/build"
    "${fake_root}/dist"
    "${read_only_module}/nested"
    "${lua_build_root}"
    "${test_root}/shared-cache")
file(COPY_FILE "${LOCKDC_ROOT}/scripts/clean.sh" "${fake_root}/scripts/clean.sh")
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

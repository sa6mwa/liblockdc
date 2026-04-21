if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

set(test_root "${LOCKDC_BINARY_DIR}/package-script-targeting-test")
set(log_path "${test_root}/invocations.log")
set(fake_cmake "${test_root}/fake-cmake.sh")
set(fake_build "${test_root}/fake-build.sh")

file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY "${test_root}")

file(WRITE "${fake_cmake}" [=[
#!/usr/bin/env bash
set -eu
{
  printf 'cmake|'
  for arg in "$@"; do
    printf '%s|' "$arg"
  done
  printf '\n'
} >> "${LOCKDC_TEST_LOG}"
]=])
file(CHMOD "${fake_cmake}"
    PERMISSIONS
        OWNER_READ OWNER_WRITE OWNER_EXECUTE
        GROUP_READ GROUP_EXECUTE
        WORLD_READ WORLD_EXECUTE
)

file(WRITE "${fake_build}" [=[
#!/usr/bin/env bash
set -eu
{
  printf 'build|'
  for arg in "$@"; do
    printf '%s|' "$arg"
  done
  printf '\n'
} >> "${LOCKDC_TEST_LOG}"
]=])
file(CHMOD "${fake_build}"
    PERMISSIONS
        OWNER_READ OWNER_WRITE OWNER_EXECUTE
        GROUP_READ GROUP_EXECUTE
        WORLD_READ WORLD_EXECUTE
)

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        LOCKDC_TEST_LOG=${log_path}
        CMAKE=${fake_cmake}
        LOCKDC_BUILD_SCRIPT=${fake_build}
        LOCKDC_VERSION=1.2.3
        "${LOCKDC_ROOT}/scripts/package.sh" aarch64-linux-musl
    WORKING_DIRECTORY "${LOCKDC_ROOT}"
    RESULT_VARIABLE package_result
    OUTPUT_VARIABLE package_stdout
    ERROR_VARIABLE package_stderr
)
if(NOT package_result EQUAL 0)
    message(FATAL_ERROR
        "expected package.sh targeted packaging to succeed\n"
        "stdout:\n${package_stdout}\n"
        "stderr:\n${package_stderr}")
endif()

file(READ "${log_path}" log_contents)

function(assert_log_contains pattern description)
    if(NOT log_contents MATCHES "${pattern}")
        message(FATAL_ERROR "missing ${description} in package.sh invocation log:\n${log_contents}")
    endif()
endfunction()

function(assert_log_not_contains pattern description)
    if(log_contents MATCHES "${pattern}")
        message(FATAL_ERROR "unexpected ${description} in package.sh invocation log:\n${log_contents}")
    endif()
endfunction()

assert_log_contains("cmake\\|-DLOCKDC_ROOT=.*cmake/package_clean_dist\\.cmake\\|" "clean-dist script invocation")
assert_log_contains("build\\|aarch64-linux-musl-release\\|" "requested release preset")
assert_log_contains("cmake\\|-DLOCKDC_BINARY_DIR=.*/build/aarch64-linux-musl-release\\|-DLOCKDC_ROOT=.*-DLOCKDC_DIST_DIR=.*/dist\\|-P\\|.*/cmake/package_archive\\.cmake\\|" "requested package script")
assert_log_contains("cmake\\|-DLOCKDC_ROOT=.*-DLOCKDC_DIST_DIR=.*/dist\\|-DLOCKDC_VERSION=1\\.2\\.3\\|-P\\|.*/cmake/package_checksums\\.cmake\\|" "checksums script invocation")

assert_log_not_contains("build\\|x86_64-linux-gnu-release\\|" "x86_64-linux-gnu release preset build")
assert_log_not_contains("cmake\\|--preset\\|release\\|" "generic release configure preset")
assert_log_not_contains("cmake\\|-DLOCKDC_BINARY_DIR=.*/build/x86_64-linux-gnu-release\\|.*cmake/package_archive\\.cmake\\|" "x86_64-linux-gnu package script")
assert_log_not_contains("cmake\\|-DLOCKDC_BINARY_DIR=.*/build/x86_64-linux-gnu-release\\|.*cmake/package_lua_rock\\.cmake\\|" "x86_64-linux-gnu Lua package script for musl-only package")

set(log_path "${test_root}/invocations-gnu.log")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        LOCKDC_TEST_LOG=${log_path}
        CMAKE=${fake_cmake}
        LOCKDC_BUILD_SCRIPT=${fake_build}
        LOCKDC_VERSION=1.2.3
        "${LOCKDC_ROOT}/scripts/package.sh" gnu
    WORKING_DIRECTORY "${LOCKDC_ROOT}"
    RESULT_VARIABLE package_gnu_result
    OUTPUT_VARIABLE package_gnu_stdout
    ERROR_VARIABLE package_gnu_stderr
)
if(NOT package_gnu_result EQUAL 0)
    message(FATAL_ERROR
        "expected package.sh gnu packaging to succeed\n"
        "stdout:\n${package_gnu_stdout}\n"
        "stderr:\n${package_gnu_stderr}")
endif()

file(READ "${log_path}" gnu_log_contents)
if(NOT gnu_log_contents MATCHES "cmake\\|-DLOCKDC_BINARY_DIR=.*/build/x86_64-linux-gnu-release\\|-DLOCKDC_ROOT=.*-DLOCKDC_DIST_DIR=.*/dist\\|-P\\|.*/cmake/package_lua_rock\\.cmake\\|")
    message(FATAL_ERROR "missing Lua packaging invocation for package.sh gnu:\n${gnu_log_contents}")
endif()

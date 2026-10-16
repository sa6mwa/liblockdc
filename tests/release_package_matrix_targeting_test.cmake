if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

set(test_root "${LOCKDC_ROOT}/build/debug/release-package-matrix-targeting-test")
set(log_path "${test_root}/invocations.log")
set(fake_bin_dir "${test_root}/bin")
set(fake_home "${test_root}/home")
set(fake_root "${test_root}/fake-root")
set(fake_script_dir "${fake_root}/scripts")
set(fake_cmake "${fake_bin_dir}/cmake")

file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY "${test_root}")
file(MAKE_DIRECTORY "${fake_bin_dir}")
file(MAKE_DIRECTORY "${fake_home}")
file(MAKE_DIRECTORY "${fake_script_dir}")

foreach(preset
    x86_64-linux-gnu-release
    x86_64-linux-musl-release
    aarch64-linux-gnu-release
    aarch64-linux-musl-release
    armhf-linux-gnu-release
    armhf-linux-musl-release
)
    file(MAKE_DIRECTORY "${fake_root}/build/${preset}")
    file(WRITE "${fake_root}/build/${preset}/CMakeCache.txt" "# fake cache for ${preset}\n")
    file(WRITE "${fake_root}/build/${preset}/package-metadata.cmake" "set(LOCKDC_VERSION \"0.0.0-test\")\n")
endforeach()

file(COPY "${LOCKDC_ROOT}/scripts/run_linux_package_matrix.sh" DESTINATION "${fake_script_dir}")
file(COPY "${LOCKDC_ROOT}/scripts/run_timed.sh" DESTINATION "${fake_script_dir}")
file(COPY "${LOCKDC_ROOT}/scripts/osxcross_available.sh" DESTINATION "${fake_script_dir}")
file(CHMOD "${fake_script_dir}/run_timed.sh"
    PERMISSIONS
        OWNER_READ OWNER_WRITE OWNER_EXECUTE
        GROUP_READ GROUP_EXECUTE
        WORLD_READ WORLD_EXECUTE
)

set(fake_release_source_script "${fake_script_dir}/test_release_from_source.sh")
file(WRITE "${fake_release_source_script}" [=[
#!/usr/bin/env bash
set -eu
{
  printf 'test_release_from_source|'
  for arg in "$@"; do
    printf '%s|' "$arg"
  done
  printf '\n'
} >> "${LOCKDC_TEST_LOG}"
]=])
file(CHMOD "${fake_release_source_script}"
    PERMISSIONS
        OWNER_READ OWNER_WRITE OWNER_EXECUTE
        GROUP_READ GROUP_EXECUTE
        WORLD_READ WORLD_EXECUTE
)

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

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        LOCKDC_TEST_LOG=${log_path}
        PATH=${fake_bin_dir}:$ENV{PATH}
        HOME=${fake_home}
        bash "${fake_root}/scripts/run_linux_package_matrix.sh"
    WORKING_DIRECTORY "${fake_root}"
    RESULT_VARIABLE script_result
    OUTPUT_VARIABLE script_stdout
    ERROR_VARIABLE script_stderr
)

if(NOT script_result EQUAL 0)
    message(FATAL_ERROR
        "expected run_linux_package_matrix.sh to succeed\n"
        "stdout:\n${script_stdout}\n"
        "stderr:\n${script_stderr}")
endif()

file(READ "${log_path}" log_contents)

function(assert_log_contains pattern description)
    if(NOT log_contents MATCHES "${pattern}")
        message(FATAL_ERROR "missing ${description} in release package log:\n${log_contents}")
    endif()
endfunction()

function(assert_log_not_contains pattern description)
    if(log_contents MATCHES "${pattern}")
        message(FATAL_ERROR "unexpected ${description} in release package log:\n${log_contents}")
    endif()
endfunction()

assert_log_contains("cmake\\|-DLOCKDC_ROOT=.*cmake/package_clean_dist\\.cmake\\|" "clean-dist invocation")
assert_log_contains("cmake\\|-DLOCKDC_BINARY_DIR=.*/build/x86_64-linux-gnu-release\\|-DLOCKDC_ROOT=.*cmake/package_archive\\.cmake\\|" "x86_64-linux-gnu archive packaging")
assert_log_contains("cmake\\|-DLOCKDC_BINARY_DIR=.*/build/x86_64-linux-musl-release\\|-DLOCKDC_ROOT=.*cmake/package_archive\\.cmake\\|" "x86_64-linux-musl archive packaging")
assert_log_contains("cmake\\|-DLOCKDC_BINARY_DIR=.*/build/aarch64-linux-gnu-release\\|-DLOCKDC_ROOT=.*cmake/package_archive\\.cmake\\|" "aarch64-linux-gnu archive packaging")
assert_log_contains("cmake\\|-DLOCKDC_BINARY_DIR=.*/build/aarch64-linux-musl-release\\|-DLOCKDC_ROOT=.*cmake/package_archive\\.cmake\\|" "aarch64-linux-musl archive packaging")
assert_log_contains("cmake\\|-DLOCKDC_BINARY_DIR=.*/build/armhf-linux-gnu-release\\|-DLOCKDC_ROOT=.*cmake/package_archive\\.cmake\\|" "armhf-linux-gnu archive packaging")
assert_log_contains("cmake\\|-DLOCKDC_BINARY_DIR=.*/build/armhf-linux-musl-release\\|-DLOCKDC_ROOT=.*cmake/package_archive\\.cmake\\|" "armhf-linux-musl archive packaging")
assert_log_contains("cmake\\|-DLOCKDC_BINARY_DIR=.*/build/x86_64-linux-gnu-release\\|-DLOCKDC_ROOT=.*-DLOCKDC_DIST_DIR=.*/dist\\|-P\\|.*/cmake/package_lua_rock\\.cmake\\|" "Lua packaging invocation")
assert_log_contains("cmake\\|-DLOCKDC_ROOT=.*-DLOCKDC_BINARY_DIR=.*/build/x86_64-linux-gnu-release\\|-DLOCKDC_DIST_DIR=.*/dist\\|-P\\|.*/cmake/package_checksums\\.cmake\\|" "checksums invocation")
assert_log_contains("test_release_from_source\\|.*/fake-root\\|.*/dist/liblockdc-0\\.0\\.0-test\\.tar\\.gz\\|" "source release verification invocation")
assert_log_contains("cmake\\|-DLOCKDC_ROOT=.*-DLOCKDC_DIST_DIR=.*/dist\\|-DLOCKDC_USE_EXISTING_ARCHIVE=ON\\|-DLOCKDC_RELEASE_PRESETS=x86_64-linux-gnu-release;x86_64-linux-musl-release;aarch64-linux-gnu-release;aarch64-linux-musl-release;armhf-linux-gnu-release;armhf-linux-musl-release\\|-P\\|.*/tests/release_tarball_sdk_matrix_test\\.cmake\\|" "read-only release tarball SDK matrix verification invocation")
assert_log_not_contains("/tests/release_tarball_sdk_test\\.cmake\\|" "duplicate host SDK verification outside the matrix")
assert_log_contains("cmake\\|-DLOCKDC_ROOT=.*-DLOCKDC_BINARY_DIR=.*/build/x86_64-linux-gnu-release\\|-DLOCKDC_DIST_DIR=.*/dist\\|-DLOCKDC_USE_EXISTING_ARCHIVE=ON\\|-DLOCKDC_LUA_BIN=.*/build/x86_64-linux-gnu-release/lockdc_lua_runner\\|-P\\|.*/tests/lua_release_package_test\\.cmake\\|" "read-only Lua release package verification invocation")
assert_log_contains("cmake\\|-DLOCKDC_ROOT=.*-DLOCKDC_DIST_DIR=.*/dist\\|-DLOCKDC_VERIFY_WORK_DIR=.*/build/release-matrix-verify\\|-DLOCKDC_RELEASE_PRESETS=x86_64-linux-gnu-release;x86_64-linux-musl-release;aarch64-linux-gnu-release;aarch64-linux-musl-release;armhf-linux-gnu-release;armhf-linux-musl-release\\|-P\\|.*/tests/release_matrix_archives_test\\.cmake\\|" "final archive verification invocation")
string(REGEX MATCHALL "package_archive\\.cmake" archive_package_invocations "${log_contents}")
list(LENGTH archive_package_invocations archive_package_count)
if(NOT archive_package_count EQUAL 6)
    message(FATAL_ERROR
        "release package matrix should create exactly six binary archives, got ${archive_package_count}:\n${log_contents}")
endif()
string(REGEX MATCHALL "package_lua_rock\\.cmake" lua_package_invocations "${log_contents}")
list(LENGTH lua_package_invocations lua_package_count)
if(NOT lua_package_count EQUAL 1)
    message(FATAL_ERROR
        "release package matrix should create exactly one Lua source rock, got ${lua_package_count}:\n${log_contents}")
endif()
assert_log_not_contains("ctest\\|" "ctest invocation")

# Exercise the real matrix driver with tiny consumer fixtures. Native targets
# must build/run in one invocation; cross targets must never execute here.
file(MAKE_DIRECTORY "${fake_root}/tests")
file(WRITE "${fake_root}/tests/release_tarball_sdk_test.cmake" [=[
file(APPEND "$ENV{LOCKDC_MATRIX_TEST_LOG}"
    "${LOCKDC_BINARY_DIR}|${LOCKDC_RUN_DOWNSTREAM_BINARIES}|${LOCKDC_VERIFIED_CONFIG_DIGESTS}\n")
if(LOCKDC_BINARY_DIR MATCHES "$ENV{LOCKDC_MATRIX_FAIL_PRESET}$")
    message(FATAL_ERROR "consumer fixture failure")
endif()
set(config_dir "${LOCKDC_BINARY_DIR}/release-tarball-sdk-test/release/sdk/lib/cmake/lockdc")
file(MAKE_DIRECTORY "${config_dir}")
if(LOCKDC_BINARY_DIR MATCHES "darwin-release$")
    file(WRITE "${config_dir}/lockdcConfig.cmake" "Darwin package logic")
else()
    file(WRITE "${config_dir}/lockdcConfig.cmake" "Linux package logic")
endif()
]=])
set(matrix_presets
    x86_64-linux-gnu-release x86_64-linux-musl-release
    aarch64-linux-gnu-release aarch64-linux-musl-release
    armhf-linux-gnu-release armhf-linux-musl-release arm64-apple-darwin-release)
file(MAKE_DIRECTORY "${fake_root}/build/arm64-apple-darwin-release")
file(WRITE "${fake_root}/build/arm64-apple-darwin-release/package-metadata.cmake" "")
foreach(fail_preset none x86_64-linux-musl-release aarch64-linux-gnu-release)
    set(matrix_log "${test_root}/matrix-${fail_preset}.log")
    file(WRITE "${matrix_log}" "")
    execute_process(COMMAND "${CMAKE_COMMAND}" -E env
        "LOCKDC_MATRIX_TEST_LOG=${matrix_log}" "LOCKDC_MATRIX_FAIL_PRESET=${fail_preset}"
        "${CMAKE_COMMAND}" -DLOCKDC_ROOT=${fake_root}
        "-DLOCKDC_RELEASE_PRESETS=${matrix_presets}"
        -P "${LOCKDC_ROOT}/tests/release_tarball_sdk_matrix_test.cmake"
        RESULT_VARIABLE rc OUTPUT_VARIABLE output ERROR_VARIABLE error)
    if(fail_preset STREQUAL "none")
        if(NOT rc EQUAL 0)
            message(FATAL_ERROR "SDK matrix failed: ${output}\n${error}")
        endif()
    elseif(rc EQUAL 0 OR NOT error MATCHES "consumer fixture failure")
        message(FATAL_ERROR "SDK matrix did not propagate failure: ${output}\n${error}")
    endif()
    file(READ "${matrix_log}" calls)
    set(expected "")
    set(digests)
    string(SHA256 linux_digest "Linux package logic")
    foreach(preset IN LISTS matrix_presets)
        set(run OFF)
        if(preset MATCHES "^x86_64-linux-")
            set(run ON)
        endif()
        string(APPEND expected "${fake_root}/build/${preset}|${run}|${digests}\n")
        if(preset STREQUAL fail_preset)
            break()
        endif()
        list(APPEND digests "${linux_digest}")
    endforeach()
    if(NOT calls STREQUAL expected)
        message(FATAL_ERROR "SDK matrix repeated, omitted or misrouted a target:\n${calls}")
    endif()
endforeach()

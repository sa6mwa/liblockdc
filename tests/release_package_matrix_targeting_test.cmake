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
endforeach()

file(COPY "${LOCKDC_ROOT}/scripts/run_linux_package_matrix.sh" DESTINATION "${fake_script_dir}")

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
assert_log_contains("cmake\\|-DLOCKDC_ROOT=.*-DLOCKDC_BINARY_DIR=.*/build/x86_64-linux-gnu-release\\|-DLOCKDC_DIST_DIR=.*/dist\\|-P\\|.*/cmake/package_checksums\\.cmake\\|" "checksums invocation")
assert_log_contains("cmake\\|-DLOCKDC_ROOT=.*-DLOCKDC_DIST_DIR=.*/dist\\|-DLOCKDC_RELEASE_PRESETS=x86_64-linux-gnu-release;x86_64-linux-musl-release;aarch64-linux-gnu-release;aarch64-linux-musl-release;armhf-linux-gnu-release;armhf-linux-musl-release\\|-P\\|.*/tests/release_matrix_archives_test\\.cmake\\|" "archive verification invocation")
assert_log_contains("cmake\\|-DLOCKDC_ROOT=.*-DLOCKDC_DIST_DIR=.*/dist\\|-DLOCKDC_RELEASE_PRESETS=x86_64-linux-gnu-release;x86_64-linux-musl-release;aarch64-linux-gnu-release;aarch64-linux-musl-release;armhf-linux-gnu-release;armhf-linux-musl-release\\|-P\\|.*/tests/release_tarball_sdk_matrix_test\\.cmake\\|" "release tarball SDK matrix verification invocation")
assert_log_contains("cmake\\|-DLOCKDC_ROOT=.*-DLOCKDC_BINARY_DIR=.*/build/x86_64-linux-gnu-release\\|-DLOCKDC_DIST_DIR=.*/dist\\|-P\\|.*/tests/release_tarball_sdk_test\\.cmake\\|" "host release tarball SDK verification invocation")
assert_log_not_contains("ctest\\|" "ctest invocation")

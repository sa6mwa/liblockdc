if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

set(test_root "${LOCKDC_BINARY_DIR}/cross-test-targeting-test")
set(script_dir "${test_root}/scripts")
set(bin_dir "${test_root}/bin")
set(log_path "${test_root}/invocations.log")
set(fake_build "${script_dir}/build.sh")
set(fake_ctest "${bin_dir}/ctest")

file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY "${script_dir}")
file(MAKE_DIRECTORY "${bin_dir}")

file(COPY "${LOCKDC_ROOT}/scripts/cross_test.sh" DESTINATION "${script_dir}")
file(CHMOD "${script_dir}/cross_test.sh"
    PERMISSIONS
        OWNER_READ OWNER_WRITE OWNER_EXECUTE
        GROUP_READ GROUP_EXECUTE
        WORLD_READ WORLD_EXECUTE
)

foreach(preset
        aarch64-linux-gnu-release
        aarch64-linux-musl-release
        armhf-linux-gnu-release
        armhf-linux-musl-release)
    file(MAKE_DIRECTORY "${test_root}/build/${preset}")
    file(WRITE "${test_root}/build/${preset}/CMakeCache.txt" "# fake cache for ${preset}\n")
endforeach()

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
exit 77
]=])
file(CHMOD "${fake_build}"
    PERMISSIONS
        OWNER_READ OWNER_WRITE OWNER_EXECUTE
        GROUP_READ GROUP_EXECUTE
        WORLD_READ WORLD_EXECUTE
)

file(WRITE "${fake_ctest}" [=[
#!/usr/bin/env bash
set -eu
{
  printf 'ctest|'
  for arg in "$@"; do
    printf '%s|' "$arg"
  done
  printf '\n'
} >> "${LOCKDC_TEST_LOG}"
exit 0
]=])
file(CHMOD "${fake_ctest}"
    PERMISSIONS
        OWNER_READ OWNER_WRITE OWNER_EXECUTE
        GROUP_READ GROUP_EXECUTE
        WORLD_READ WORLD_EXECUTE
)

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        PATH=${bin_dir}:$ENV{PATH}
        LOCKDC_TEST_LOG=${log_path}
        "${script_dir}/cross_test.sh" release
    WORKING_DIRECTORY "${LOCKDC_BINARY_DIR}"
    RESULT_VARIABLE cross_test_result
    OUTPUT_VARIABLE cross_test_stdout
    ERROR_VARIABLE cross_test_stderr
)
if(NOT cross_test_result EQUAL 0)
    message(FATAL_ERROR
        "expected cross_test.sh release to succeed without rebuilding\n"
        "stdout:\n${cross_test_stdout}\n"
        "stderr:\n${cross_test_stderr}")
endif()

file(READ "${log_path}" log_contents)

function(assert_log_contains pattern description)
    if(NOT log_contents MATCHES "${pattern}")
        message(FATAL_ERROR "missing ${description} in cross_test.sh invocation log:\n${log_contents}")
    endif()
endfunction()

function(assert_log_not_contains pattern description)
    if(log_contents MATCHES "${pattern}")
        message(FATAL_ERROR "unexpected ${description} in cross_test.sh invocation log:\n${log_contents}")
    endif()
endfunction()

assert_log_contains("ctest\\|--preset\\|aarch64-linux-gnu-release\\|--output-on-failure\\|" "aarch64 gnu ctest invocation")
assert_log_contains("ctest\\|--preset\\|aarch64-linux-musl-release\\|--output-on-failure\\|" "aarch64 musl ctest invocation")
assert_log_contains("ctest\\|--preset\\|armhf-linux-gnu-release\\|--output-on-failure\\|" "armhf gnu ctest invocation")
assert_log_contains("ctest\\|--preset\\|armhf-linux-musl-release\\|--output-on-failure\\|" "armhf musl ctest invocation")
assert_log_not_contains("build\\|" "release preset build invocation")

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
exit 0
]=])
file(WRITE "${log_path}" "")

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        PATH=${bin_dir}:$ENV{PATH}
        LOCKDC_TEST_LOG=${log_path}
        "${script_dir}/cross_test.sh" preset
    WORKING_DIRECTORY "${LOCKDC_BINARY_DIR}"
    RESULT_VARIABLE cross_test_preset_result
    OUTPUT_VARIABLE cross_test_preset_stdout
    ERROR_VARIABLE cross_test_preset_stderr
)
if(NOT cross_test_preset_result EQUAL 0)
    message(FATAL_ERROR
        "expected cross_test.sh preset to succeed\n"
        "stdout:\n${cross_test_preset_stdout}\n"
        "stderr:\n${cross_test_preset_stderr}")
endif()

file(READ "${log_path}" log_contents)

assert_log_contains("build\\|debug\\|" "debug preset build invocation")
assert_log_contains("ctest\\|--preset\\|debug\\|--output-on-failure\\|" "debug preset ctest invocation")
assert_log_not_contains("build\\|asan\\|" "asan compatibility alias build invocation")
assert_log_not_contains("ctest\\|--preset\\|asan\\|" "asan compatibility alias ctest invocation")
assert_log_not_contains("ctest\\|--preset\\|armhf-linux-musl-release\\|" "release ctest invocation during preset-only mode")

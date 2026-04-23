if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(NOT DEFINED LOCKDC_C_COMPILER)
    message(FATAL_ERROR "LOCKDC_C_COMPILER is required")
endif()

set(test_root "${LOCKDC_BINARY_DIR}/dependency-download-timeout-test")
set(configure_build_dir "${test_root}/build")
set(external_root "${test_root}/deps")
set(dependency_build_root "${test_root}/deps-build")
set(download_root "${test_root}/downloads")
set(zlib_download_script "${dependency_build_root}/zlib/stamp/download-lc_zlib_project.cmake")

file(REMOVE_RECURSE "${test_root}")

execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -S "${LOCKDC_ROOT}"
        -B "${configure_build_dir}"
        -G Ninja
        -DCMAKE_C_COMPILER=${LOCKDC_C_COMPILER}
        -DCMAKE_BUILD_TYPE=Release
        -DLOCKDC_BUILD_DEPENDENCIES=ON
        -DLOCKDC_BUILD_STATIC=ON
        -DLOCKDC_BUILD_SHARED=ON
        -DLOCKDC_BUILD_TESTS=OFF
        -DLOCKDC_BUILD_EXAMPLES=OFF
        -DLOCKDC_BUILD_E2E_TESTS=OFF
        -DLOCKDC_BUILD_BENCHMARKS=OFF
        -DLOCKDC_BUILD_FUZZERS=OFF
        -DLOCKDC_INSTALL=OFF
        -DLOCKDC_TARGET_ARCH=x86_64
        -DLOCKDC_TARGET_OS=linux
        -DLOCKDC_TARGET_LIBC=gnu
        -DLOCKDC_EXTERNAL_ROOT=${external_root}
        -DLOCKDC_DEPENDENCY_BUILD_ROOT=${dependency_build_root}
        -DLOCKDC_DOWNLOAD_ROOT=${download_root}
    RESULT_VARIABLE configure_result
    OUTPUT_VARIABLE configure_stdout
    ERROR_VARIABLE configure_stderr
)
if(NOT configure_result EQUAL 0)
    message(FATAL_ERROR
        "failed to configure dependency timeout probe\n"
        "stdout:\n${configure_stdout}\n"
        "stderr:\n${configure_stderr}")
endif()

if(NOT EXISTS "${zlib_download_script}")
    message(FATAL_ERROR "missing generated zlib download script: ${zlib_download_script}")
endif()

file(READ "${zlib_download_script}" zlib_download_contents)

function(assert_contains needle description)
    string(FIND "${zlib_download_contents}" "${needle}" found_at)
    if(found_at EQUAL -1)
        message(FATAL_ERROR "missing ${description} in zlib download script")
    endif()
endfunction()

assert_contains("timeout='300 seconds'" "download timeout message")
assert_contains("inactivity timeout='60 seconds'" "download inactivity-timeout message")
assert_contains("TIMEOUT;300" "download timeout setting")
assert_contains("INACTIVITY_TIMEOUT;60" "download inactivity-timeout setting")
assert_contains("https://www.zlib.net/zlib-1.3.2.tar.gz" "primary zlib URL")
assert_contains("https://zlib.net/fossils/zlib-1.3.2.tar.gz" "fallback zlib URL")
assert_contains("${download_root}/zlib-1.3.2.tar.gz" "shared download-root path")

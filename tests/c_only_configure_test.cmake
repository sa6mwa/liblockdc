if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

if(NOT DEFINED LOCKDC_C_COMPILER OR LOCKDC_C_COMPILER STREQUAL "")
    message(FATAL_ERROR "LOCKDC_C_COMPILER is required")
endif()

set(test_root "${LOCKDC_BINARY_DIR}/c-only-configure-test")
set(build_dir "${test_root}/build")
set(fake_cxx "${test_root}/definitely-no-cxx")

file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY "${test_root}")

execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -S "${LOCKDC_ROOT}"
        -B "${build_dir}"
        -DLOCKDC_BUILD_DEPENDENCIES=ON
        -DLOCKDC_BUILD_TESTS=OFF
        -DLOCKDC_BUILD_E2E_TESTS=OFF
        -DLOCKDC_BUILD_EXAMPLES=OFF
        -DLOCKDC_BUILD_BENCHMARKS=OFF
        -DLOCKDC_BUILD_FUZZERS=OFF
        -DCMAKE_C_COMPILER=${LOCKDC_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${fake_cxx}
    RESULT_VARIABLE configure_result
    OUTPUT_VARIABLE configure_stdout
    ERROR_VARIABLE configure_stderr
)
if(NOT configure_result EQUAL 0)
    message(FATAL_ERROR
        "expected a plain C-only configure to succeed without a working C++ compiler\n"
        "stdout:\n${configure_stdout}\n"
        "stderr:\n${configure_stderr}")
endif()

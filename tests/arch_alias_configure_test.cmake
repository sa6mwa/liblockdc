if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

if(NOT DEFINED LOCKDC_C_COMPILER OR LOCKDC_C_COMPILER STREQUAL "")
    message(FATAL_ERROR "LOCKDC_C_COMPILER is required")
endif()

if(NOT DEFINED LOCKDC_EXTERNAL_ROOT OR LOCKDC_EXTERNAL_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_EXTERNAL_ROOT is required")
endif()

function(assert_arch_alias alias canonical)
    set(test_root "${LOCKDC_BINARY_DIR}/arch-alias-configure-test/${alias}")
    set(build_dir "${test_root}/build")
    set(dependency_build_root "${test_root}/deps-build")
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
            -DLOCKDC_EXTERNAL_ROOT=${LOCKDC_EXTERNAL_ROOT}
            -DLOCKDC_DEPENDENCY_BUILD_ROOT=${dependency_build_root}
            -DLOCKDC_TARGET_ARCH=${alias}
            -DCMAKE_C_COMPILER=${LOCKDC_C_COMPILER}
            -DCMAKE_CXX_COMPILER=${fake_cxx}
        RESULT_VARIABLE configure_result
        OUTPUT_VARIABLE configure_stdout
        ERROR_VARIABLE configure_stderr
    )
    if(NOT configure_result EQUAL 0)
        message(FATAL_ERROR
            "expected alias configure for ${alias} to succeed\n"
            "stdout:\n${configure_stdout}\n"
            "stderr:\n${configure_stderr}")
    endif()

    file(STRINGS "${build_dir}/CMakeCache.txt" cached_arch REGEX "^LOCKDC_TARGET_ARCH:STRING=")
    if(NOT cached_arch STREQUAL "LOCKDC_TARGET_ARCH:STRING=${canonical}")
        message(FATAL_ERROR
            "expected alias ${alias} to normalize to ${canonical}, got '${cached_arch}'")
    endif()
endfunction()

assert_arch_alias(amd64 x86_64)
assert_arch_alias(arm64 aarch64)
assert_arch_alias(arm armhf)

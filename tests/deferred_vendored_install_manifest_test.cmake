if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

if(NOT DEFINED LOCKDC_C_COMPILER OR LOCKDC_C_COMPILER STREQUAL "")
    message(FATAL_ERROR "LOCKDC_C_COMPILER is required")
endif()

set(test_root "${LOCKDC_ROOT}/build/deferred-vendored-install-manifest-test")
set(dependency_build_root "${test_root}/deps-build")
set(configure_build_dir "${test_root}/build")

file(REMOVE_RECURSE "${test_root}")

execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -S "${LOCKDC_ROOT}"
        -B "${configure_build_dir}"
        "-DCMAKE_C_COMPILER=${LOCKDC_C_COMPILER}"
        -DCMAKE_BUILD_TYPE=Release
        -DLOCKDC_BUILD_DEPENDENCIES=ON
        -DLOCKDC_INSTALL=ON
        -DLOCKDC_BUILD_TESTS=OFF
        -DLOCKDC_BUILD_E2E_TESTS=OFF
        -DLOCKDC_BUILD_EXAMPLES=OFF
        -DLOCKDC_BUILD_BENCHMARKS=OFF
        -DLOCKDC_BUILD_FUZZERS=OFF
        "-DLOCKDC_DEPENDENCY_BUILD_ROOT=${dependency_build_root}"
    RESULT_VARIABLE configure_result
    OUTPUT_VARIABLE configure_stdout
    ERROR_VARIABLE configure_stderr
)
if(NOT configure_result EQUAL 0)
    message(FATAL_ERROR
        "failed to configure deferred vendored install manifest test\n"
        "stdout:\n${configure_stdout}\n"
        "stderr:\n${configure_stderr}")
endif()

set(install_manifest_path "${configure_build_dir}/cmake_install.cmake")
if(NOT EXISTS "${install_manifest_path}")
    message(FATAL_ERROR "missing generated install manifest: ${install_manifest_path}")
endif()

foreach(forbidden_path
    "${configure_build_dir}/install_vendored_sdk_dev_artifacts.cmake"
    "${configure_build_dir}/install_vendored_sdk_runtime_artifacts.cmake")
    if(EXISTS "${forbidden_path}")
        message(FATAL_ERROR "generated obsolete vendored install helper: ${forbidden_path}")
    endif()
endforeach()

file(READ "${install_manifest_path}" install_manifest_text)
foreach(forbidden_snippet
    "install_vendored_sdk_dev_artifacts.cmake"
    "install_vendored_sdk_runtime_artifacts.cmake"
)
    string(FIND "${install_manifest_text}" "${forbidden_snippet}" snippet_index)
    if(NOT snippet_index EQUAL -1)
        message(FATAL_ERROR
            "install manifest still references obsolete vendored install helper '${forbidden_snippet}'\n"
            "manifest:\n${install_manifest_text}")
    endif()
endforeach()

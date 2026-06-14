if(NOT DEFINED LOCKDC_ROOT OR LOCKDC_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

set(test_root "${LOCKDC_ROOT}/build/package-source-dist-dir-test")
set(fake_build_dir "${test_root}/build-tree")
set(requested_dist_dir "${test_root}/requested-dist")
set(metadata_dist_dir "${test_root}/metadata-dist")
set(package_version "9.8.7-test")

file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY "${fake_build_dir}")
file(MAKE_DIRECTORY "${requested_dist_dir}")
file(MAKE_DIRECTORY "${metadata_dist_dir}")
file(WRITE "${fake_build_dir}/package-metadata.cmake"
    "set(LOCKDC_VERSION \"${package_version}\")\n"
    "set(LOCKDC_DIST_DIR \"${metadata_dist_dir}\")\n")

execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -DLOCKDC_ROOT=${LOCKDC_ROOT}
        -DLOCKDC_BINARY_DIR=${fake_build_dir}
        -DLOCKDC_DIST_DIR=${requested_dist_dir}
        -P "${LOCKDC_ROOT}/cmake/package_source.cmake"
    RESULT_VARIABLE package_source_result
    OUTPUT_VARIABLE package_source_stdout
    ERROR_VARIABLE package_source_stderr
)
if(NOT package_source_result EQUAL 0)
    message(FATAL_ERROR
        "package_source.cmake failed\n"
        "stdout:\n${package_source_stdout}\n"
        "stderr:\n${package_source_stderr}")
endif()

set(expected_archive "${requested_dist_dir}/liblockdc-${package_version}.tar.gz")
set(unexpected_archive "${metadata_dist_dir}/liblockdc-${package_version}.tar.gz")

if(NOT EXISTS "${expected_archive}")
    message(FATAL_ERROR
        "package_source.cmake ignored caller-provided LOCKDC_DIST_DIR.\n"
        "Missing expected source archive: ${expected_archive}\n"
        "stdout:\n${package_source_stdout}\n"
        "stderr:\n${package_source_stderr}")
endif()

if(EXISTS "${unexpected_archive}")
    message(FATAL_ERROR
        "package_source.cmake wrote to metadata LOCKDC_DIST_DIR instead of caller-provided LOCKDC_DIST_DIR: "
        "${unexpected_archive}")
endif()

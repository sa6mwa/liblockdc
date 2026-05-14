if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

if(NOT DEFINED LOCKDC_VERSION)
    message(FATAL_ERROR "LOCKDC_VERSION is required")
endif()

if(NOT DEFINED LOCKDC_TARGET_ID)
    message(FATAL_ERROR "LOCKDC_TARGET_ID is required")
endif()

set(test_root "${LOCKDC_BINARY_DIR}/runtime-license-install-tree-test")
set(dist_dir "${test_root}/dist")
set(missing_dependency_build_root "${test_root}/missing-deps-build")
set(release_archive "${dist_dir}/liblockdc-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}.tar.gz")
set(archive_prefix "liblockdc-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}")
string(REPLACE "." "\\." archive_prefix_regex "${archive_prefix}")

file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY "${dist_dir}")

execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -DLOCKDC_BINARY_DIR=${LOCKDC_BINARY_DIR}
        -DLOCKDC_ROOT=${LOCKDC_ROOT}
        -DLOCKDC_DIST_DIR=${dist_dir}
        -DLOCKDC_DEPENDENCY_BUILD_ROOT=${missing_dependency_build_root}
        -P "${LOCKDC_ROOT}/cmake/package_archive.cmake"
    RESULT_VARIABLE package_result
    OUTPUT_VARIABLE package_stdout
    ERROR_VARIABLE package_stderr
)
if(NOT package_result EQUAL 0)
    message(FATAL_ERROR
        "expected install-tree packaging to succeed without deps-build fallback\n"
        "stdout:\n${package_stdout}\n"
        "stderr:\n${package_stderr}")
endif()

if(NOT EXISTS "${release_archive}")
    message(FATAL_ERROR "expected runtime archive to exist: ${release_archive}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E tar tf "${release_archive}"
    RESULT_VARIABLE tar_result
    OUTPUT_VARIABLE archive_listing
    ERROR_VARIABLE tar_stderr
)
if(NOT tar_result EQUAL 0)
    message(FATAL_ERROR
        "failed to list package archive contents\n"
        "stderr:\n${tar_stderr}")
endif()

function(assert_archive_contains pattern description)
    if(NOT archive_listing MATCHES "${pattern}")
        message(FATAL_ERROR
            "expected archive to contain ${description}\n"
            "archive contents:\n${archive_listing}")
    endif()
endfunction()

function(assert_archive_not_contains pattern description)
    if(archive_listing MATCHES "${pattern}")
        message(FATAL_ERROR
            "archive unexpectedly contains ${description}\n"
            "archive contents:\n${archive_listing}")
    endif()
endfunction()

assert_archive_contains("(^|\n)${archive_prefix_regex}/(\n|$)" "archive root directory")
assert_archive_not_contains("(^|\n)${archive_prefix_regex}/share/doc/liblockdc/third_party(/|\n|$)" "third-party license directory")

if(archive_listing MATCHES "(^|\n)\\./")
    message(FATAL_ERROR
        "archive contains invalid ./-prefixed entries\n"
        "archive contents:\n${archive_listing}")
endif()

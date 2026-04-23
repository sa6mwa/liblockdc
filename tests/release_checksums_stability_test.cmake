if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

if(NOT EXISTS "${LOCKDC_BINARY_DIR}/package-metadata.cmake")
    message(FATAL_ERROR "missing package metadata: ${LOCKDC_BINARY_DIR}/package-metadata.cmake")
endif()
include("${LOCKDC_BINARY_DIR}/package-metadata.cmake")

set(lockdc_test_root "${LOCKDC_BINARY_DIR}/release-checksums-stability-test")
set(lockdc_dist_dir "${lockdc_test_root}/dist")
set(lockdc_checksums_path "${lockdc_dist_dir}/liblockdc-${LOCKDC_VERSION}-CHECKSUMS")

file(REMOVE_RECURSE "${lockdc_test_root}")
file(MAKE_DIRECTORY "${lockdc_dist_dir}")

foreach(script_path
    "${LOCKDC_ROOT}/cmake/package_archive.cmake"
    "${LOCKDC_ROOT}/cmake/package_lua_rock.cmake"
    "${LOCKDC_ROOT}/cmake/package_checksums.cmake"
)
    execute_process(
        COMMAND "${CMAKE_COMMAND}"
            -DLOCKDC_BINARY_DIR=${LOCKDC_BINARY_DIR}
            -DLOCKDC_ROOT=${LOCKDC_ROOT}
            -DLOCKDC_DIST_DIR=${lockdc_dist_dir}
            -P "${script_path}"
        RESULT_VARIABLE lockdc_package_result
        OUTPUT_VARIABLE lockdc_package_stdout
        ERROR_VARIABLE lockdc_package_stderr
    )
    if(NOT lockdc_package_result EQUAL 0)
        message(FATAL_ERROR
            "failed while preparing shared release dist with ${script_path}\n"
            "stdout:\n${lockdc_package_stdout}\n"
            "stderr:\n${lockdc_package_stderr}")
    endif()
endforeach()

foreach(test_script
    "${LOCKDC_ROOT}/tests/release_tarball_sdk_test.cmake"
    "${LOCKDC_ROOT}/tests/lua_release_package_test.cmake"
)
    execute_process(
        COMMAND "${CMAKE_COMMAND}"
            -DLOCKDC_BINARY_DIR=${LOCKDC_BINARY_DIR}
            -DLOCKDC_ROOT=${LOCKDC_ROOT}
            -DLOCKDC_DIST_DIR=${lockdc_dist_dir}
            -P "${test_script}"
        RESULT_VARIABLE lockdc_test_result
        OUTPUT_VARIABLE lockdc_test_stdout
        ERROR_VARIABLE lockdc_test_stderr
    )
    if(NOT lockdc_test_result EQUAL 0)
        message(FATAL_ERROR
            "verification script failed during checksum stability test: ${test_script}\n"
            "stdout:\n${lockdc_test_stdout}\n"
            "stderr:\n${lockdc_test_stderr}")
    endif()
endforeach()

execute_process(
    COMMAND sha256sum -c "liblockdc-${LOCKDC_VERSION}-CHECKSUMS"
    WORKING_DIRECTORY "${lockdc_dist_dir}"
    RESULT_VARIABLE lockdc_checksum_result
    OUTPUT_VARIABLE lockdc_checksum_stdout
    ERROR_VARIABLE lockdc_checksum_stderr
)
if(NOT lockdc_checksum_result EQUAL 0)
    message(FATAL_ERROR
        "release verification scripts mutated shared dist artifacts after checksums were written\n"
        "stdout:\n${lockdc_checksum_stdout}\n"
        "stderr:\n${lockdc_checksum_stderr}")
endif()

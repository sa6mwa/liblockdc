if(NOT DEFINED LOCKDC_ROOT OR LOCKDC_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()
if(NOT DEFINED LOCKDC_BINARY_DIR OR LOCKDC_BINARY_DIR STREQUAL "")
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

find_program(LOCKDC_TAR_BIN NAMES tar)
if(NOT LOCKDC_TAR_BIN)
    message(FATAL_ERROR "tar is required for recursive release privacy coverage")
endif()

set(test_root "${LOCKDC_BINARY_DIR}/release-privacy-recursive-test")
set(inner_root "${test_root}/inner")
set(inner_archive "${test_root}/lockdc-inner.tar.gz")
set(outer_archive "${test_root}/lockdc-0.0.0-1.src.rock")
file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY "${inner_root}")
file(WRITE "${inner_root}/payload.txt" "private source=${LOCKDC_ROOT}\n")

execute_process(
    COMMAND "${LOCKDC_TAR_BIN}" -czf "${inner_archive}" inner
    WORKING_DIRECTORY "${test_root}"
    RESULT_VARIABLE inner_archive_result
    ERROR_VARIABLE inner_archive_error)
if(NOT inner_archive_result EQUAL 0)
    message(FATAL_ERROR "failed to create nested privacy fixture: ${inner_archive_error}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E tar cf "${outer_archive}" --format=zip "lockdc-inner.tar.gz"
    WORKING_DIRECTORY "${test_root}"
    RESULT_VARIABLE outer_archive_result
    ERROR_VARIABLE outer_archive_error)
if(NOT outer_archive_result EQUAL 0)
    message(FATAL_ERROR "failed to create source-rock privacy fixture: ${outer_archive_error}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -DLOCKDC_ROOT=${LOCKDC_ROOT}
        -DLOCKDC_SCAN_PATHS=${outer_archive}
        -P "${LOCKDC_ROOT}/tests/release_privacy_scan.cmake"
    RESULT_VARIABLE privacy_result
    OUTPUT_VARIABLE privacy_output
    ERROR_VARIABLE privacy_error)
if(privacy_result EQUAL 0)
    message(FATAL_ERROR
        "recursive release privacy scan accepted a private path in a nested archive\n"
        "stdout:\n${privacy_output}\n"
        "stderr:\n${privacy_error}")
endif()
if(NOT privacy_error MATCHES "private trace LOCKDC_ROOT")
    message(FATAL_ERROR
        "recursive release privacy scan failed without identifying the nested private trace\n"
        "stdout:\n${privacy_output}\n"
        "stderr:\n${privacy_error}")
endif()

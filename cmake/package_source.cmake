if(NOT DEFINED LOCKDC_ROOT OR "${LOCKDC_ROOT}" STREQUAL "")
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()
include("${CMAKE_CURRENT_LIST_DIR}/LcGeneratedPath.cmake")
if(DEFINED LOCKDC_DIST_DIR AND NOT "${LOCKDC_DIST_DIR}" STREQUAL "")
    set(lockdc_input_dist_dir "${LOCKDC_DIST_DIR}")
endif()
if(NOT DEFINED LOCKDC_DIST_DIR OR "${LOCKDC_DIST_DIR}" STREQUAL "")
    set(LOCKDC_DIST_DIR "${LOCKDC_ROOT}/dist")
endif()
if((NOT DEFINED LOCKDC_VERSION OR "${LOCKDC_VERSION}" STREQUAL "")
   AND DEFINED LOCKDC_BINARY_DIR
   AND EXISTS "${LOCKDC_BINARY_DIR}/package-metadata.cmake")
    include("${LOCKDC_BINARY_DIR}/package-metadata.cmake")
endif()
if(DEFINED lockdc_input_dist_dir)
    set(LOCKDC_DIST_DIR "${lockdc_input_dist_dir}")
endif()
if(NOT DEFINED LOCKDC_VERSION OR "${LOCKDC_VERSION}" STREQUAL "")
    set(LOCKDC_VERSION "0.0.0")
endif()

find_program(LOCKDC_TAR_BIN NAMES tar REQUIRED)
find_program(LOCKDC_GZIP_BIN NAMES gzip REQUIRED)

set(source_root_name "liblockdc-${LOCKDC_VERSION}")
set(archive_base "${LOCKDC_DIST_DIR}/${source_root_name}.tar")
set(archive_path "${archive_base}.gz")
set(stage_root "${LOCKDC_DIST_DIR}/.source-pack")
set(stage_dir "${stage_root}/${source_root_name}")

lockdc_assert_generated_path("${LOCKDC_ROOT}" "${LOCKDC_DIST_DIR}")
lockdc_assert_generated_path("${LOCKDC_ROOT}" "${stage_root}")
file(MAKE_DIRECTORY "${LOCKDC_DIST_DIR}")
file(REMOVE_RECURSE "${stage_root}")
file(REMOVE "${archive_base}" "${archive_path}")
file(MAKE_DIRECTORY "${stage_root}")

execute_process(
    COMMAND "${LOCKDC_ROOT}/scripts/stage_release_sources.sh"
            "${LOCKDC_ROOT}" "${stage_dir}" "${LOCKDC_VERSION}"
    RESULT_VARIABLE stage_result
)
if(NOT stage_result EQUAL 0)
    file(REMOVE_RECURSE "${stage_root}")
    message(FATAL_ERROR "failed to stage source archive tree")
endif()

execute_process(
    COMMAND "${LOCKDC_TAR_BIN}" -cf "${archive_base}" --format=gnu --owner=0 --group=0
            "${source_root_name}"
    WORKING_DIRECTORY "${stage_root}"
    RESULT_VARIABLE archive_result
)
if(NOT archive_result EQUAL 0)
    file(REMOVE_RECURSE "${stage_root}")
    message(FATAL_ERROR "failed to create source archive")
endif()

execute_process(
    COMMAND "${LOCKDC_GZIP_BIN}" -9 -f -n "${archive_base}"
    RESULT_VARIABLE gzip_result
)
if(NOT gzip_result EQUAL 0)
    file(REMOVE_RECURSE "${stage_root}")
    message(FATAL_ERROR "failed to gzip source archive")
endif()

file(REMOVE_RECURSE "${stage_root}")
message(STATUS "Wrote ${archive_path}")

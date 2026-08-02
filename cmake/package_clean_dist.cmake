if(NOT DEFINED LOCKDC_ROOT OR "${LOCKDC_ROOT}" STREQUAL "")
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()
include("${CMAKE_CURRENT_LIST_DIR}/LcGeneratedPath.cmake")

if(DEFINED LOCKDC_DIST_DIR AND NOT "${LOCKDC_DIST_DIR}" STREQUAL "")
    set(dist_dir "${LOCKDC_DIST_DIR}")
else()
    set(dist_dir "${LOCKDC_ROOT}/dist")
endif()

lockdc_assert_generated_path("${LOCKDC_ROOT}" "${dist_dir}")
file(REMOVE_RECURSE "${dist_dir}")
file(MAKE_DIRECTORY "${dist_dir}")

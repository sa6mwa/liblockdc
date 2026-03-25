if(DEFINED LOCKDC_DIST_DIR AND NOT "${LOCKDC_DIST_DIR}" STREQUAL "")
    set(dist_dir "${LOCKDC_DIST_DIR}")
else()
    set(dist_dir "${LOCKDC_ROOT}/dist")
endif()

file(REMOVE_RECURSE "${dist_dir}")
file(MAKE_DIRECTORY "${dist_dir}")

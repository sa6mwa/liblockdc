if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()
if(NOT DEFINED LOCKDC_VERSION_SOURCE_DIR)
    message(FATAL_ERROR "LOCKDC_VERSION_SOURCE_DIR is required")
endif()
if(NOT DEFINED LOCKDC_VERSION_PROBE_OUTPUT)
    message(FATAL_ERROR "LOCKDC_VERSION_PROBE_OUTPUT is required")
endif()

include("${LOCKDC_ROOT}/cmake/LcVersion.cmake")

lc_detect_version(lockdc_resolved_version)
string(REPLACE "." ";" lockdc_version_parts "${lockdc_resolved_version}")
list(GET lockdc_version_parts 0 lockdc_version_major)
list(GET lockdc_version_parts 1 lockdc_version_minor)
list(GET lockdc_version_parts 2 lockdc_version_patch)

file(WRITE "${LOCKDC_VERSION_PROBE_OUTPUT}"
    "${lockdc_resolved_version}|${lockdc_version_major}|${lockdc_version_minor}|${lockdc_version_patch}\n")

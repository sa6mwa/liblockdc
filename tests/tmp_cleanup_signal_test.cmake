if(NOT DEFINED LOCKDC_TMP_CLEANUP_PROBE_COMMAND)
    if(DEFINED LOCKDC_TMP_CLEANUP_PROBE)
        set(LOCKDC_TMP_CLEANUP_PROBE_COMMAND "${LOCKDC_TMP_CLEANUP_PROBE}")
    else()
        message(FATAL_ERROR "LOCKDC_TMP_CLEANUP_PROBE_COMMAND is required")
    endif()
endif()
if(NOT DEFINED LOCKDC_TMP_PATH_FILE)
    message(FATAL_ERROR "LOCKDC_TMP_PATH_FILE is required")
endif()

file(REMOVE "${LOCKDC_TMP_PATH_FILE}")
if(NOT DEFINED LOCKDC_TMP_CLEANUP_SIGNAL)
    set(LOCKDC_TMP_CLEANUP_SIGNAL "TERM")
endif()
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
            "LOCKDC_TMP_CLEANUP_PATH_FILE=${LOCKDC_TMP_PATH_FILE}"
            "LOCKDC_TMP_CLEANUP_SIGNAL=${LOCKDC_TMP_CLEANUP_SIGNAL}"
            ${LOCKDC_TMP_CLEANUP_PROBE_COMMAND}
    RESULT_VARIABLE probe_result
)

if(probe_result STREQUAL "0")
    message(FATAL_ERROR "tmp cleanup probe exited successfully; expected signal ${LOCKDC_TMP_CLEANUP_SIGNAL}")
endif()
if(NOT EXISTS "${LOCKDC_TMP_PATH_FILE}")
    message(FATAL_ERROR "tmp cleanup probe did not write its tracked path")
endif()

file(READ "${LOCKDC_TMP_PATH_FILE}" tracked_path)
string(STRIP "${tracked_path}" tracked_path)
if(tracked_path STREQUAL "")
    message(FATAL_ERROR "tmp cleanup probe wrote an empty tracked path")
endif()
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
            "LOCKDC_TMP_CLEANUP_MODE=signal-stale"
            ${LOCKDC_TMP_CLEANUP_PROBE_COMMAND}
    RESULT_VARIABLE cleanup_result
)
if(NOT cleanup_result STREQUAL "0")
    message(FATAL_ERROR "stale signal cleanup probe failed: ${cleanup_result}")
endif()
if(EXISTS "${tracked_path}")
    message(FATAL_ERROR "tracked temp path survived stale cleanup: ${tracked_path}")
endif()
file(REMOVE "${LOCKDC_TMP_PATH_FILE}")

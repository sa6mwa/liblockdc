if(NOT DEFINED LOCKDC_STRIP_TARGET OR LOCKDC_STRIP_TARGET STREQUAL "")
  message(FATAL_ERROR "LOCKDC_STRIP_TARGET is required")
endif()

if(NOT EXISTS "${LOCKDC_STRIP_TARGET}" OR IS_SYMLINK "${LOCKDC_STRIP_TARGET}")
  return()
endif()

if(NOT DEFINED LOCKDC_STRIP_BIN OR LOCKDC_STRIP_BIN STREQUAL "")
  return()
endif()

if(NOT EXISTS "${LOCKDC_STRIP_BIN}")
  return()
endif()

if(LOCKDC_TARGET_ID MATCHES "apple-darwin$" AND LOCKDC_STRIP_TARGET MATCHES "\\.a$")
  return()
endif()

execute_process(
  COMMAND "${LOCKDC_STRIP_BIN}" -S "${LOCKDC_STRIP_TARGET}"
  RESULT_VARIABLE _strip_result
  ERROR_VARIABLE _strip_error
)
if(NOT _strip_result EQUAL 0)
  message(FATAL_ERROR "failed to strip release artifact ${LOCKDC_STRIP_TARGET}\n${_strip_error}")
endif()

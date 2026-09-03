if(NOT DEFINED RUNNER OR NOT DEFINED HANG_PROBE OR NOT DEFINED SUCCESS_PROBE OR
   NOT DEFINED TEST_ROOT)
  message(FATAL_ERROR "Pouch integration mutation runner regression test requires its targets and test root.")
endif()

execute_process(
  COMMAND "${CMAKE_COMMAND}" -E env
    "LOCKDC_POUCH_INTEGRATION_TARGET_TIMEOUT_SECONDS=1"
    "${RUNNER}" "${HANG_PROBE}" "${CORPUS}" 0
  RESULT_VARIABLE hang_result
  ERROR_VARIABLE hang_error
  TIMEOUT 10
)
if(hang_result EQUAL 0 OR hang_result STREQUAL "Process terminated due to timeout")
  message(FATAL_ERROR "Pouch integration mutation runner did not return after a hung child: ${hang_result}")
endif()
if(NOT hang_error MATCHES "pouch integration mutation timeout:.*artifact=([^\n\r]+)")
  message(FATAL_ERROR "Pouch integration mutation runner did not report a timeout artifact: ${hang_error}")
endif()
set(hang_artifact "${CMAKE_MATCH_1}")
get_filename_component(hang_root "${hang_artifact}" DIRECTORY)
if(NOT hang_root MATCHES "^/tmp/liblockdc-pouch-integration-mutation-")
  message(FATAL_ERROR "Pouch integration mutation runner returned an unsafe artifact root: ${hang_root}")
endif()
file(REMOVE_RECURSE "${hang_root}")

file(REMOVE_RECURSE "${TEST_ROOT}")
file(MAKE_DIRECTORY "${TEST_ROOT}/oversized-corpus")
string(REPEAT x 65537 oversized_seed)
file(WRITE "${TEST_ROOT}/oversized-corpus/seed" "${oversized_seed}")
execute_process(
  COMMAND "${RUNNER}" "${SUCCESS_PROBE}" "${TEST_ROOT}/oversized-corpus" 0
  RESULT_VARIABLE oversized_result
  ERROR_VARIABLE oversized_error
)
file(REMOVE_RECURSE "${TEST_ROOT}")
if(NOT oversized_result EQUAL 1)
  message(FATAL_ERROR "Pouch integration mutation runner returned ${oversized_result} for an oversized corpus seed: ${oversized_error}")
endif()
if(NOT oversized_error MATCHES "failed to read Pouch integration corpus seed")
  message(FATAL_ERROR "Pouch integration mutation runner did not report the oversized corpus seed: ${oversized_error}")
endif()

foreach(invalid_count -1 5x 18446744073709551615)
  execute_process(
    COMMAND "${RUNNER}" "${SUCCESS_PROBE}" "${CORPUS}" "${invalid_count}"
    RESULT_VARIABLE invalid_result
    ERROR_VARIABLE invalid_error
  )
  if(NOT invalid_result EQUAL 2 OR NOT invalid_error MATCHES "usage:")
    message(FATAL_ERROR "Pouch integration mutation runner accepted invalid count ${invalid_count}: ${invalid_result} ${invalid_error}")
  endif()
endforeach()

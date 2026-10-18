set(root "${BINARY_DIR}/tracking-gate")
file(REMOVE_RECURSE "${root}")
file(MAKE_DIRECTORY "${root}")
file(WRITE "${root}/CTestTestfile.cmake"
  "add_test(clean \"${PROBE}\" clean)\n"
  "add_test(leak \"${PROBE}\" leak)\n"
  "add_test(after_leak \"${CMAKE_COMMAND}\" -E touch \"${root}/continued\")\n"
  "set_tests_properties(clean leak PROPERTIES ENVIRONMENT \"LOCKDC_TEST_ALLOCATOR_TRACKING=1;ASAN_OPTIONS=detect_leaks=0\" FAIL_REGULAR_EXPRESSION \"liblockdc allocator:\")\n")
execute_process(COMMAND "${CMAKE_CTEST_COMMAND}" --test-dir "${root}"
    --no-tests=error --output-on-failure -R "^clean$"
  RESULT_VARIABLE clean_rc OUTPUT_VARIABLE clean_output ERROR_VARIABLE clean_error)
if(NOT clean_rc EQUAL 0)
  message(FATAL_ERROR "tracked clean consumer failed: ${clean_output}\n${clean_error}")
endif()
execute_process(COMMAND "${CMAKE_CTEST_COMMAND}" --test-dir "${root}"
    --no-tests=error --stop-on-failure --output-on-failure
  RESULT_VARIABLE leak_rc OUTPUT_VARIABLE leak_output ERROR_VARIABLE leak_error)
if(leak_rc EQUAL 0 OR EXISTS "${root}/continued" OR
   NOT leak_output MATCHES "liblockdc allocator: 1 live allocations")
  message(FATAL_ERROR "CTest did not stop on a successful consumer's leak: ${leak_output}\n${leak_error}")
endif()

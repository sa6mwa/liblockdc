if(NOT DEFINED LOCKDC_ROOT)
  message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()
if(NOT DEFINED LOCKDC_BINARY_DIR)
  message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

set(test_root "${LOCKDC_BINARY_DIR}/run-timed-log-test")
set(timing_dir "${test_root}/timings")
set(run_timed "${LOCKDC_ROOT}/scripts/run_timed.sh")
file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY "${timing_dir}")

execute_process(
  COMMAND "${CMAKE_COMMAND}" -E env
    "LOCKDC_TIMING_DIR=${timing_dir}"
    "LOCKDC_TIMING_LOG="
    "LOCKDC_TIMING_DEPTH=0"
    bash "${run_timed}" "timing-contract" bash -c "exit 0"
  RESULT_VARIABLE timed_result
  OUTPUT_VARIABLE timed_stdout
  ERROR_VARIABLE timed_stderr
)
if(NOT timed_result EQUAL 0)
  message(FATAL_ERROR
    "run_timed.sh failed\nstdout:\n${timed_stdout}\nstderr:\n${timed_stderr}")
endif()
if(NOT timed_stderr MATCHES "\\[timing [0-9T:Z-]+\\] start timing-contract")
  message(FATAL_ERROR "run_timed.sh did not emit its timestamped start event:\n${timed_stderr}")
endif()
if(NOT timed_stdout MATCHES "timing-contract completed in")
  message(FATAL_ERROR "run_timed.sh did not emit its completion event:\n${timed_stdout}")
endif()

# The lifecycle clean step removes the default timing directory while its
# outer timer is still active. That diagnostic append must not override the
# successful command status or make `make release` fail before its proof graph.
execute_process(
  COMMAND "${CMAKE_COMMAND}" -E env
    "LOCKDC_TIMING_DIR=${test_root}/timing-clean"
    "LOCKDC_TIMING_LOG="
    "LOCKDC_TIMING_DEPTH=0"
    bash "${run_timed}" "timing-clean-contract"
      bash -c "rm -rf \"$LOCKDC_TIMING_DIR\"; exit 0"
  RESULT_VARIABLE clean_result
  OUTPUT_VARIABLE clean_stdout
  ERROR_VARIABLE clean_stderr
)
if(NOT clean_result EQUAL 0)
  message(FATAL_ERROR
    "run_timed.sh changed a successful clean command into failure\n"
    "stdout:\n${clean_stdout}\nstderr:\n${clean_stderr}")
endif()

file(GLOB timing_logs "${timing_dir}/*.tsv")
list(LENGTH timing_logs timing_log_count)
if(NOT timing_log_count EQUAL 1)
  message(FATAL_ERROR "expected one timing TSV, got ${timing_log_count}")
endif()
list(GET timing_logs 0 timing_log)
file(READ "${timing_log}" timing_contents)
if(NOT timing_contents MATCHES "started_at.*elapsed_seconds.*status.*depth")
  message(FATAL_ERROR "timing TSV is missing its schema header:\n${timing_contents}")
endif()
string(REPLACE "\t" "|" timing_records "${timing_contents}")
if(NOT timing_records MATCHES "timing-contract\\|[0-9]+\\|0\\|0")
  message(FATAL_ERROR "timing TSV is missing the successful root event:\n${timing_contents}")
endif()

file(REMOVE_RECURSE "${test_root}")

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

# A caller-selected timing directory may disappear during a command. That
# diagnostic append must not override the successful command status.
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

# A pre-existing directory can be searchable but not writable. Initial timing
# log creation is diagnostic only and must not prevent the wrapped command.
set(unwritable_timing_dir "${test_root}/timing-unwritable")
set(unwritable_marker "${test_root}/timing-unwritable-command-ran")
file(MAKE_DIRECTORY "${unwritable_timing_dir}")
execute_process(COMMAND chmod 500 "${unwritable_timing_dir}"
  RESULT_VARIABLE chmod_result)
if(NOT chmod_result EQUAL 0)
  message(FATAL_ERROR "failed to make timing directory unwritable")
endif()
execute_process(
  COMMAND "${CMAKE_COMMAND}" -E env
    "LOCKDC_TIMING_DIR=${unwritable_timing_dir}"
    "LOCKDC_TIMING_LOG="
    "LOCKDC_TIMING_DEPTH=0"
    bash "${run_timed}" "timing-unwritable-contract"
      "${CMAKE_COMMAND}" -E touch "${unwritable_marker}"
  RESULT_VARIABLE unwritable_result
  OUTPUT_VARIABLE unwritable_stdout
  ERROR_VARIABLE unwritable_stderr
)
execute_process(COMMAND chmod 700 "${unwritable_timing_dir}")
if(NOT unwritable_result EQUAL 0 OR NOT EXISTS "${unwritable_marker}")
  message(FATAL_ERROR
    "run_timed.sh did not run a command with an unwritable timing directory\n"
    "stdout:\n${unwritable_stdout}\nstderr:\n${unwritable_stderr}")
endif()

# The default log is deliberately in the system temporary area, so a lifecycle
# clean keeps the enclosing timer's record. Run an isolated copy because this
# test must not remove the real build directory.
set(default_root "${test_root}/default-root")
set(default_script_dir "${default_root}/scripts")
file(MAKE_DIRECTORY "${default_script_dir}")
file(COPY_FILE "${run_timed}" "${default_script_dir}/run_timed.sh")
execute_process(
  COMMAND env -u LOCKDC_TIMING_DIR -u LOCKDC_TIMING_LOG -u LOCKDC_TIMING_DEPTH
    "TMPDIR=${default_root}/tmp"
    bash "${default_script_dir}/run_timed.sh" "timing-default-clean"
      bash -c "rm -rf \"$PWD/build\"; exit 0"
  WORKING_DIRECTORY "${default_root}"
  RESULT_VARIABLE default_result
  OUTPUT_VARIABLE default_stdout
  ERROR_VARIABLE default_stderr
)
if(NOT default_result EQUAL 0)
  message(FATAL_ERROR
    "run_timed.sh default timing clean failed\n"
    "stdout:\n${default_stdout}\nstderr:\n${default_stderr}")
endif()
file(GLOB default_logs "${default_root}/tmp/liblockdc-timings/*.tsv")
list(LENGTH default_logs default_log_count)
if(NOT default_log_count EQUAL 1)
  message(FATAL_ERROR
    "expected one default timing TSV after clean, got ${default_log_count}")
endif()
list(GET default_logs 0 default_log)
file(READ "${default_log}" default_contents)
string(REPLACE "\t" "|" default_records "${default_contents}")
if(NOT default_records MATCHES "timing-default-clean\\|[0-9]+\\|0\\|0")
  message(FATAL_ERROR
    "default timing TSV is missing the clean event:\n${default_contents}")
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

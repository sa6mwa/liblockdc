if(NOT DEFINED LOCKDC_BINARY_DIR OR LOCKDC_BINARY_DIR STREQUAL "")
  message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()
if(NOT DEFINED LOCKDC_ROOT OR LOCKDC_ROOT STREQUAL "")
  message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()
if(NOT DEFINED LOCKDC_OUTBOX_PRODUCER OR
   LOCKDC_OUTBOX_PRODUCER STREQUAL "")
  message(FATAL_ERROR "LOCKDC_OUTBOX_PRODUCER is required")
endif()
if(NOT EXISTS "${LOCKDC_OUTBOX_PRODUCER}")
  message(FATAL_ERROR
    "outbox producer example is missing: ${LOCKDC_OUTBOX_PRODUCER}")
endif()

set(lockdc_outbox_root
  "${LOCKDC_BINARY_DIR}/outbox-example-e2e/lc_e2e_outbox_examples")
file(MAKE_DIRECTORY "${lockdc_outbox_root}")
file(REAL_PATH "${LOCKDC_BINARY_DIR}" lockdc_binary_dir)
file(REAL_PATH "${lockdc_outbox_root}" lockdc_outbox_root BASE_DIRECTORY
  "${LOCKDC_BINARY_DIR}")
string(FIND "${lockdc_outbox_root}" "${lockdc_binary_dir}/"
  lockdc_outbox_root_prefix)
if(NOT lockdc_outbox_root_prefix EQUAL 0)
  message(FATAL_ERROR "outbox example root must be below LOCKDC_BINARY_DIR")
endif()

file(REMOVE_RECURSE "${lockdc_outbox_root}")
file(MAKE_DIRECTORY "${lockdc_outbox_root}")
file(CHMOD "${lockdc_outbox_root}" PERMISSIONS
  OWNER_READ OWNER_WRITE OWNER_EXECUTE)

execute_process(
  COMMAND "${CMAKE_COMMAND}" -E env
    "LOCKDC_POUCH_ROOT=${lockdc_outbox_root}"
    "${LOCKDC_OUTBOX_PRODUCER}"
  RESULT_VARIABLE producer_result
  OUTPUT_VARIABLE producer_stdout
  ERROR_VARIABLE producer_stderr
)
if(NOT producer_result EQUAL 0)
  message(FATAL_ERROR
    "outbox producer example failed\nstdout:\n${producer_stdout}\nstderr:\n${producer_stderr}")
endif()
if(NOT producer_stdout MATCHES "committed outbox key:")
  message(FATAL_ERROR
    "outbox producer did not expose a commit-published receipt\n${producer_stdout}")
endif()

set(LOCKDC_TEST_NAME "lc_e2e_outbox_examples")
set(LOCKDC_LUA_TEST_SCRIPT
  "${LOCKDC_ROOT}/examples/lua/outbox_dispatcher.lua")
set(LOCKDC_LUA_TEST_ENV
  "LOCKDC_POUCH_ROOT=${lockdc_outbox_root}|LOCKDC_OUTBOX_ONCE=1")
include("${LOCKDC_ROOT}/tests/lua_rock_install_and_run_test.cmake")

file(REMOVE_RECURSE "${lockdc_outbox_root}")

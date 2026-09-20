if(NOT DEFINED LOCKDC_BINARY_DIR OR LOCKDC_BINARY_DIR STREQUAL "")
  message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()
if(NOT DEFINED LOCKDC_ROOT OR LOCKDC_ROOT STREQUAL "")
  message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()
if(NOT DEFINED LOCKDC_WORKFLOW_PRODUCER OR
   LOCKDC_WORKFLOW_PRODUCER STREQUAL "")
  message(FATAL_ERROR "LOCKDC_WORKFLOW_PRODUCER is required")
endif()
if(NOT EXISTS "${LOCKDC_WORKFLOW_PRODUCER}")
  message(FATAL_ERROR
    "workflow producer example is missing: ${LOCKDC_WORKFLOW_PRODUCER}")
endif()

set(lockdc_workflow_root
  "${LOCKDC_BINARY_DIR}/workflow-example-e2e/lc_e2e_workflow_examples")
file(REAL_PATH "${LOCKDC_BINARY_DIR}" lockdc_binary_dir)
file(REAL_PATH "${lockdc_workflow_root}" lockdc_workflow_root BASE_DIRECTORY
  "${LOCKDC_BINARY_DIR}")
string(FIND "${lockdc_workflow_root}" "${lockdc_binary_dir}/"
  lockdc_workflow_root_prefix)
if(NOT lockdc_workflow_root_prefix EQUAL 0)
  message(FATAL_ERROR "workflow example root must be below LOCKDC_BINARY_DIR")
endif()

file(REMOVE_RECURSE "${lockdc_workflow_root}")
file(MAKE_DIRECTORY "${lockdc_workflow_root}")
file(CHMOD "${lockdc_workflow_root}" PERMISSIONS
  OWNER_READ OWNER_WRITE OWNER_EXECUTE)

execute_process(
  COMMAND "${CMAKE_COMMAND}" -E env
    "LOCKDC_POUCH_ROOT=${lockdc_workflow_root}"
    "${LOCKDC_WORKFLOW_PRODUCER}"
  RESULT_VARIABLE producer_result
  OUTPUT_VARIABLE producer_stdout
  ERROR_VARIABLE producer_stderr
)
if(NOT producer_result EQUAL 0)
  message(FATAL_ERROR
    "workflow producer example failed\nstdout:\n${producer_stdout}\nstderr:\n${producer_stderr}")
endif()
if(NOT producer_stdout MATCHES "committed outbox key:")
  message(FATAL_ERROR
    "workflow producer did not expose a commit-published receipt\n${producer_stdout}")
endif()

set(LOCKDC_TEST_NAME "lc_e2e_workflow_examples")
set(LOCKDC_LUA_TEST_SCRIPT
  "${LOCKDC_ROOT}/examples/lua/workflow_dispatcher.lua")
set(LOCKDC_LUA_TEST_ENV
  "LOCKDC_POUCH_ROOT=${lockdc_workflow_root}|LOCKDC_WORKFLOW_ONCE=1")
include("${LOCKDC_ROOT}/tests/lua_rock_install_and_run_test.cmake")

file(REMOVE_RECURSE "${lockdc_workflow_root}")

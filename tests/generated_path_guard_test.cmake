if(NOT DEFINED LOCKDC_ROOT OR LOCKDC_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

set(guard_script "${LOCKDC_ROOT}/scripts/assert_generated_path.sh")
if(NOT EXISTS "${guard_script}")
    message(FATAL_ERROR "missing generated-path guard: ${guard_script}")
endif()

function(assert_guard path should_pass)
    execute_process(
        COMMAND bash -c "source \"${guard_script}\"; lockdc_assert_generated_path \"${LOCKDC_ROOT}\" \"${path}\""
        RESULT_VARIABLE guard_result
        ERROR_VARIABLE guard_error)
    if(should_pass AND NOT guard_result EQUAL 0)
        message(FATAL_ERROR "generated-path guard rejected ${path}: ${guard_error}")
    endif()
    if(NOT should_pass AND guard_result EQUAL 0)
        message(FATAL_ERROR "generated-path guard accepted unsafe path: ${path}")
    endif()
endfunction()

assert_guard("${LOCKDC_ROOT}/build/generated-path-guard-test" TRUE)
assert_guard("${LOCKDC_ROOT}/.cache/generated-path-guard-test" TRUE)
assert_guard("${LOCKDC_ROOT}" FALSE)
assert_guard("/" FALSE)
if(NOT "$ENV{HOME}" STREQUAL "")
    assert_guard("$ENV{HOME}" FALSE)
endif()

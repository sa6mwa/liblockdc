if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        "LOCKDC_WORLD_DRY_RUN=1"
        "LOCKDC_WORLD_CLANG_BIN=__missing_clang__"
        bash "${LOCKDC_ROOT}/scripts/world.sh"
    WORKING_DIRECTORY "${LOCKDC_ROOT}"
    RESULT_VARIABLE world_result
    OUTPUT_VARIABLE world_stdout
    ERROR_VARIABLE world_stderr
)

if(NOT world_result EQUAL 0)
    message(FATAL_ERROR
        "Dry-run world script failed with exit code ${world_result}\n"
        "stdout:\n${world_stdout}\n"
        "stderr:\n${world_stderr}")
endif()

foreach(expected
    "[world] __clean"
    "[world] __test-debug"
    "[world] __test-host"
    "[world] __cross-test"
    "[world] __asan"
    "[world] __coverage"
    "[world] skipping __fuzz: clang not available (__missing_clang__)"
    "[world] __test-e2e"
    "[world] __benchmarks"
    "[world] __release"
)
    string(FIND "${world_stdout}" "${expected}" match_index)
    if(match_index EQUAL -1)
        message(FATAL_ERROR
            "Expected world dry-run output to contain '${expected}'\n"
            "stdout:\n${world_stdout}\n"
            "stderr:\n${world_stderr}")
    endif()
endforeach()

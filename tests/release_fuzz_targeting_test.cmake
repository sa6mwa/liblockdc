if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        "LOCKDC_RELEASE_DRY_RUN=1"
        bash "${LOCKDC_ROOT}/scripts/release.sh"
    WORKING_DIRECTORY "${LOCKDC_ROOT}"
    RESULT_VARIABLE release_result
    OUTPUT_VARIABLE release_stdout
    ERROR_VARIABLE release_stderr
)

if(NOT release_result EQUAL 0)
    message(FATAL_ERROR
        "Dry-run release script failed with exit code ${release_result}\n"
        "stdout:\n${release_stdout}\n"
        "stderr:\n${release_stderr}")
endif()

foreach(expected
    "[release] __lifecycle-version-contract"
    "[release] __clean"
    "[release] __release-pipeline"
)
    string(FIND "${release_stdout}" "${expected}" match_index)
    if(match_index EQUAL -1)
        message(FATAL_ERROR
            "Expected release dry-run output to contain '${expected}'\n"
            "stdout:\n${release_stdout}\n"
            "stderr:\n${release_stderr}")
    endif()
endforeach()

execute_process(COMMAND make -n __prerelease
    WORKING_DIRECTORY "${LOCKDC_ROOT}"
    RESULT_VARIABLE plan_result OUTPUT_VARIABLE plan ERROR_VARIABLE plan_error)
if(NOT plan_result EQUAL 0)
    message(FATAL_ERROR "Cannot inspect prerelease order: ${plan_error}")
endif()
string(FIND "${plan}" "prerelease compiler preflight" compile_step)
string(FIND "${plan}" "prerelease test-all" test_step)
string(FIND "${plan}" "prerelease valgrind" memory_step)
string(FIND "${plan}" "prerelease fuzz" fuzz_step)
if(compile_step LESS 0 OR test_step LESS compile_step OR memory_step LESS test_step OR fuzz_step LESS memory_step)
    message(FATAL_ERROR "Prerelease must compile AFL before service/memory tests, then retain all gates: ${plan}")
endif()

foreach(unexpected
    "[release] __test-debug"
    "[release] __test-host"
    "[release] __cross-test"
    "[release] __test-e2e"
    "[release] __benchmarks"
)
    string(FIND "${release_stdout}" "${unexpected}" unexpected_match_index)
    if(NOT unexpected_match_index EQUAL -1)
        message(FATAL_ERROR
            "Did not expect release.sh to duplicate release-pipeline step '${unexpected}'\n"
            "stdout:\n${release_stdout}\n"
            "stderr:\n${release_stderr}")
    endif()
endforeach()

string(FIND "${release_stdout}" "[release] __lifecycle-version-contract\n[release] __clean\n[release] __release-pipeline" release_order_match)
if(release_order_match EQUAL -1)
    message(FATAL_ERROR
        "Expected release dry-run to run lifecycle-version-contract, clean, then release-pipeline\n"
        "stdout:\n${release_stdout}\n"
        "stderr:\n${release_stderr}")
endif()

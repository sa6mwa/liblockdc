if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

execute_process(
    COMMAND make -n __release
    WORKING_DIRECTORY "${LOCKDC_ROOT}"
    RESULT_VARIABLE release_result
    OUTPUT_VARIABLE release_stdout
    ERROR_VARIABLE release_stderr
)

if(NOT release_result EQUAL 0)
    message(FATAL_ERROR
        "Expected make -n __release to succeed\n"
        "stdout:\n${release_stdout}\n"
        "stderr:\n${release_stderr}")
endif()

string(FIND "${release_stdout}" "bash ./scripts/run_linux_release_matrix.sh" release_match)
if(release_match EQUAL -1)
    message(FATAL_ERROR
        "Expected __release to invoke run_linux_release_matrix.sh\n"
        "stdout:\n${release_stdout}\n"
        "stderr:\n${release_stderr}")
endif()

string(FIND "${release_stdout}" "bash ./scripts/run_linux_package_matrix.sh" package_match)
if(NOT package_match EQUAL -1)
    message(FATAL_ERROR
        "Did not expect __release to invoke run_linux_package_matrix.sh\n"
        "stdout:\n${release_stdout}\n"
        "stderr:\n${release_stderr}")
endif()

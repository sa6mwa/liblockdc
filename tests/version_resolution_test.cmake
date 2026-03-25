if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

find_program(LOCKDC_GIT_BIN NAMES git)
if(NOT LOCKDC_GIT_BIN)
    message(FATAL_ERROR "git is required for version resolution tests")
endif()

set(test_root "${LOCKDC_BINARY_DIR}/version-resolution-test")
set(repo_dir "${test_root}/repo")
set(tagged_output "${test_root}/tagged.txt")
set(untagged_output "${test_root}/untagged.txt")

file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY "${repo_dir}")
file(WRITE "${repo_dir}/README.md" "version test\n")

execute_process(
    COMMAND "${LOCKDC_GIT_BIN}" init
    WORKING_DIRECTORY "${repo_dir}"
    RESULT_VARIABLE git_init_result
    OUTPUT_QUIET
    ERROR_QUIET
)
if(NOT git_init_result EQUAL 0)
    message(FATAL_ERROR "failed to initialize temporary git repository")
endif()

execute_process(
    COMMAND "${LOCKDC_GIT_BIN}" config user.email test@example.com
    WORKING_DIRECTORY "${repo_dir}"
    RESULT_VARIABLE git_email_result
    OUTPUT_QUIET
    ERROR_QUIET
)
if(NOT git_email_result EQUAL 0)
    message(FATAL_ERROR "failed to configure temporary git email")
endif()

execute_process(
    COMMAND "${LOCKDC_GIT_BIN}" config user.name "liblockdc version test"
    WORKING_DIRECTORY "${repo_dir}"
    RESULT_VARIABLE git_name_result
    OUTPUT_QUIET
    ERROR_QUIET
)
if(NOT git_name_result EQUAL 0)
    message(FATAL_ERROR "failed to configure temporary git name")
endif()

execute_process(
    COMMAND "${LOCKDC_GIT_BIN}" add README.md
    WORKING_DIRECTORY "${repo_dir}"
    RESULT_VARIABLE git_add_result
    OUTPUT_QUIET
    ERROR_QUIET
)
if(NOT git_add_result EQUAL 0)
    message(FATAL_ERROR "failed to stage temporary repository contents")
endif()

execute_process(
    COMMAND "${LOCKDC_GIT_BIN}" commit -m "initial"
    WORKING_DIRECTORY "${repo_dir}"
    RESULT_VARIABLE git_commit_result
    OUTPUT_QUIET
    ERROR_QUIET
)
if(NOT git_commit_result EQUAL 0)
    message(FATAL_ERROR "failed to create initial temporary repository commit")
endif()

execute_process(
    COMMAND "${LOCKDC_GIT_BIN}" tag v1.2.3
    WORKING_DIRECTORY "${repo_dir}"
    RESULT_VARIABLE git_tag_result
    OUTPUT_QUIET
    ERROR_QUIET
)
if(NOT git_tag_result EQUAL 0)
    message(FATAL_ERROR "failed to create temporary repository tag")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -DLOCKDC_ROOT=${LOCKDC_ROOT}
        -DLOCKDC_VERSION_SOURCE_DIR=${repo_dir}
        -DLOCKDC_VERSION_PROBE_OUTPUT=${tagged_output}
        -P ${LOCKDC_ROOT}/tests/version_resolution_probe.cmake
    RESULT_VARIABLE tagged_probe_result
)
if(NOT tagged_probe_result EQUAL 0)
    message(FATAL_ERROR "failed to probe tagged version resolution")
endif()

file(READ "${tagged_output}" tagged_probe_contents)
string(STRIP "${tagged_probe_contents}" tagged_probe_contents)
if(NOT tagged_probe_contents STREQUAL "1.2.3|1|2|3")
    message(FATAL_ERROR
        "expected exact v-tag to resolve to 1.2.3, got '${tagged_probe_contents}'")
endif()

file(APPEND "${repo_dir}/README.md" "next commit\n")
execute_process(
    COMMAND "${LOCKDC_GIT_BIN}" add README.md
    WORKING_DIRECTORY "${repo_dir}"
    RESULT_VARIABLE git_add_second_result
    OUTPUT_QUIET
    ERROR_QUIET
)
if(NOT git_add_second_result EQUAL 0)
    message(FATAL_ERROR "failed to stage second temporary repository commit")
endif()

execute_process(
    COMMAND "${LOCKDC_GIT_BIN}" commit -m "second"
    WORKING_DIRECTORY "${repo_dir}"
    RESULT_VARIABLE git_second_commit_result
    OUTPUT_QUIET
    ERROR_QUIET
)
if(NOT git_second_commit_result EQUAL 0)
    message(FATAL_ERROR "failed to create second temporary repository commit")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -DLOCKDC_ROOT=${LOCKDC_ROOT}
        -DLOCKDC_VERSION_SOURCE_DIR=${repo_dir}
        -DLOCKDC_VERSION_PROBE_OUTPUT=${untagged_output}
        -P ${LOCKDC_ROOT}/tests/version_resolution_probe.cmake
    RESULT_VARIABLE untagged_probe_result
)
if(NOT untagged_probe_result EQUAL 0)
    message(FATAL_ERROR "failed to probe untagged version resolution")
endif()

file(READ "${untagged_output}" untagged_probe_contents)
string(STRIP "${untagged_probe_contents}" untagged_probe_contents)
if(NOT untagged_probe_contents STREQUAL "0.0.0|0|0|0")
    message(FATAL_ERROR
        "expected untagged commit to resolve to 0.0.0, got '${untagged_probe_contents}'")
endif()

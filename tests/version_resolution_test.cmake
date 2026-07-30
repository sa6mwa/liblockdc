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
set(override_output "${test_root}/override.txt")
set(nested_output "${test_root}/nested.txt")

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

file(WRITE "${repo_dir}/VERSION" "7.8.9\n")
execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -DLOCKDC_ROOT=${LOCKDC_ROOT}
        -DLOCKDC_VERSION_SOURCE_DIR=${repo_dir}
        -DLOCKDC_VERSION_PROBE_OUTPUT=${untagged_output}
        -P ${LOCKDC_ROOT}/tests/version_resolution_probe.cmake
    RESULT_VARIABLE git_version_file_probe_result
)
if(NOT git_version_file_probe_result EQUAL 0)
    message(FATAL_ERROR "failed to probe git worktree VERSION fallback behavior")
endif()
file(READ "${untagged_output}" git_version_file_probe_contents)
string(STRIP "${git_version_file_probe_contents}" git_version_file_probe_contents)
if(NOT git_version_file_probe_contents STREQUAL "0.0.0|0|0|0")
    message(FATAL_ERROR
        "expected git worktree VERSION file to be ignored, got '${git_version_file_probe_contents}'")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -DLOCKDC_ROOT=${LOCKDC_ROOT}
        -DLOCKDC_VERSION_SOURCE_DIR=${repo_dir}
        -DLOCKDC_VERSION_OVERRIDE=3.4.5
        -DLOCKDC_VERSION_PROBE_OUTPUT=${override_output}
        -P ${LOCKDC_ROOT}/tests/version_resolution_probe.cmake
    RESULT_VARIABLE override_probe_result
)
if(NOT override_probe_result EQUAL 0)
    message(FATAL_ERROR "failed to probe version override resolution")
endif()
file(READ "${override_output}" override_probe_contents)
string(STRIP "${override_probe_contents}" override_probe_contents)
if(NOT override_probe_contents STREQUAL "3.4.5|3|4|5")
    message(FATAL_ERROR
        "expected untagged override to resolve to 3.4.5, got '${override_probe_contents}'")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        LOCKDC_VERSION_SOURCE_DIR=${repo_dir}
        bash "${LOCKDC_ROOT}/scripts/release_version.sh"
    RESULT_VARIABLE make_version_result
    OUTPUT_VARIABLE make_version_stdout
    ERROR_VARIABLE make_version_stderr
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
if(NOT make_version_result EQUAL 0)
    message(FATAL_ERROR
        "Make-owned version surface failed\nstdout:\n${make_version_stdout}\nstderr:\n${make_version_stderr}")
endif()
if(NOT make_version_stdout STREQUAL "0.0.0")
    message(FATAL_ERROR
        "expected Make-owned untagged version to be 0.0.0, got '${make_version_stdout}'")
endif()

set(nested_source_dir "${repo_dir}/build/source-archive")
file(MAKE_DIRECTORY "${nested_source_dir}")
file(WRITE "${nested_source_dir}/VERSION" "8.9.10\n")
execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -DLOCKDC_ROOT=${LOCKDC_ROOT}
        -DLOCKDC_VERSION_SOURCE_DIR=${nested_source_dir}
        -DLOCKDC_VERSION_PROBE_OUTPUT=${nested_output}
        -P ${LOCKDC_ROOT}/tests/version_resolution_probe.cmake
    RESULT_VARIABLE nested_probe_result
)
if(NOT nested_probe_result EQUAL 0)
    message(FATAL_ERROR "failed to probe nested source archive version behavior")
endif()
file(READ "${nested_output}" nested_probe_contents)
string(STRIP "${nested_probe_contents}" nested_probe_contents)
if(NOT nested_probe_contents STREQUAL "8.9.10|8|9|10")
    message(FATAL_ERROR
        "expected nested source archive VERSION to resolve to 8.9.10, got '${nested_probe_contents}'")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        LOCKDC_VERSION_SOURCE_DIR=${nested_source_dir}
        bash "${LOCKDC_ROOT}/scripts/release_version.sh"
    RESULT_VARIABLE nested_make_result
    OUTPUT_VARIABLE nested_make_stdout
    ERROR_VARIABLE nested_make_stderr
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
if(NOT nested_make_result EQUAL 0)
    message(FATAL_ERROR
        "Make-owned nested source version failed\nstdout:\n${nested_make_stdout}\nstderr:\n${nested_make_stderr}")
endif()
if(NOT nested_make_stdout STREQUAL "8.9.10")
    message(FATAL_ERROR
        "expected Make-owned nested source version to be 8.9.10, got '${nested_make_stdout}'")
endif()

execute_process(
    COMMAND "${LOCKDC_GIT_BIN}" -c tag.gpgSign=false tag -a v2.3.4 -m "annotated"
    WORKING_DIRECTORY "${repo_dir}"
    RESULT_VARIABLE annotated_tag_result
    OUTPUT_QUIET
    ERROR_QUIET
)
if(NOT annotated_tag_result EQUAL 0)
    message(FATAL_ERROR "failed to create temporary annotated repository tag")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -DLOCKDC_ROOT=${LOCKDC_ROOT}
        -DLOCKDC_VERSION_SOURCE_DIR=${repo_dir}
        -DLOCKDC_VERSION_PROBE_OUTPUT=${untagged_output}
        -P ${LOCKDC_ROOT}/tests/version_resolution_probe.cmake
    RESULT_VARIABLE annotated_probe_result
    OUTPUT_QUIET
    ERROR_QUIET
)
if(annotated_probe_result EQUAL 0)
    message(FATAL_ERROR "expected annotated exact release tag to be rejected")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        LOCKDC_VERSION_SOURCE_DIR=${repo_dir}
        bash "${LOCKDC_ROOT}/scripts/release_version.sh"
    RESULT_VARIABLE annotated_make_result
    OUTPUT_QUIET
    ERROR_QUIET
)
if(annotated_make_result EQUAL 0)
    message(FATAL_ERROR "expected Make-owned version surface to reject annotated exact release tag")
endif()

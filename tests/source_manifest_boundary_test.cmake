if(NOT DEFINED LOCKDC_ROOT OR LOCKDC_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()
if(NOT DEFINED LOCKDC_BINARY_DIR OR LOCKDC_BINARY_DIR STREQUAL "")
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

find_program(LOCKDC_GIT_BIN NAMES git REQUIRED)
set(test_root "${LOCKDC_BINARY_DIR}/source-manifest-boundary-test")
set(repo_root "${test_root}/repo")
set(stage_dir "${repo_root}/build/stage")

file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY "${repo_root}/scripts")
file(COPY_FILE "${LOCKDC_ROOT}/scripts/assert_generated_path.sh"
    "${repo_root}/scripts/assert_generated_path.sh")
file(COPY_FILE "${LOCKDC_ROOT}/scripts/stage_release_sources.sh"
    "${repo_root}/scripts/stage_release_sources.sh")
file(WRITE "${repo_root}/tracked.txt" "tracked payload\n")
file(WRITE "${repo_root}/untracked.txt" "untracked payload\n")

execute_process(
    COMMAND "${LOCKDC_GIT_BIN}" init
    WORKING_DIRECTORY "${repo_root}"
    RESULT_VARIABLE init_result
    OUTPUT_VARIABLE init_output
    ERROR_VARIABLE init_error)
if(NOT init_result EQUAL 0)
    message(FATAL_ERROR "could not initialize source-manifest fixture:\n${init_output}\n${init_error}")
endif()
execute_process(
    COMMAND "${LOCKDC_GIT_BIN}" add tracked.txt scripts/assert_generated_path.sh scripts/stage_release_sources.sh
    WORKING_DIRECTORY "${repo_root}"
    RESULT_VARIABLE add_result
    OUTPUT_VARIABLE add_output
    ERROR_VARIABLE add_error)
if(NOT add_result EQUAL 0)
    message(FATAL_ERROR "could not stage source-manifest fixture:\n${add_output}\n${add_error}")
endif()

execute_process(
    COMMAND bash "${repo_root}/scripts/stage_release_sources.sh" "${repo_root}" "${stage_dir}" 1.2.3
    RESULT_VARIABLE stage_result
    OUTPUT_VARIABLE stage_output
    ERROR_VARIABLE stage_error)
if(NOT stage_result EQUAL 0)
    message(FATAL_ERROR "source staging failed:\n${stage_output}\n${stage_error}")
endif()
if(NOT EXISTS "${stage_dir}/tracked.txt")
    message(FATAL_ERROR "tracked source file is absent from the staged manifest")
endif()
if(EXISTS "${stage_dir}/untracked.txt")
    message(FATAL_ERROR "untracked source file leaked into the staged manifest")
endif()
file(STRINGS "${stage_dir}/RELEASE_MANIFEST" release_manifest)
foreach(expected_path tracked.txt scripts/assert_generated_path.sh scripts/stage_release_sources.sh VERSION RELEASE_MANIFEST)
    list(FIND release_manifest "${expected_path}" expected_index)
    if(expected_index EQUAL -1)
        message(FATAL_ERROR "staged manifest is missing ${expected_path}")
    endif()
endforeach()
list(FIND release_manifest untracked.txt untracked_index)
if(NOT untracked_index EQUAL -1)
    message(FATAL_ERROR "staged manifest lists an untracked source file")
endif()

if(NOT DEFINED LOCKDC_ROOT OR LOCKDC_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()
if(NOT DEFINED LOCKDC_BINARY_DIR OR LOCKDC_BINARY_DIR STREQUAL "")
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

set(test_root "${LOCKDC_BINARY_DIR}/dependency-prune-path-guard")
set(fake_root "${test_root}/repo")
set(safe_external_root "${fake_root}/.cache/deps/x86_64-linux-gnu")
set(safe_payload "${safe_external_root}/curl/install/bin/curl")
set(external_root "${test_root}/outside")
set(external_sentinel "${external_root}/sentinel")

file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY "${fake_root}/cmake" "${safe_external_root}/curl/install/bin" "${external_root}")
file(COPY_FILE "${LOCKDC_ROOT}/cmake/LcGeneratedPath.cmake"
    "${fake_root}/cmake/LcGeneratedPath.cmake")
file(COPY_FILE "${LOCKDC_ROOT}/cmake/prune_dependency_install_tree.cmake"
    "${fake_root}/cmake/prune_dependency_install_tree.cmake")
file(WRITE "${safe_payload}" "generated dependency executable\n")
file(WRITE "${external_sentinel}" "must survive\n")

execute_process(
    COMMAND "${CMAKE_COMMAND}"
        "-DLOCKDC_ROOT=${fake_root}"
        "-DLOCKDC_EXTERNAL_ROOT=${safe_external_root}"
        -P "${fake_root}/cmake/prune_dependency_install_tree.cmake"
    RESULT_VARIABLE safe_result
    OUTPUT_VARIABLE safe_output
    ERROR_VARIABLE safe_error)
if(NOT safe_result EQUAL 0)
    message(FATAL_ERROR
        "dependency pruning rejected its generated root\n"
        "stdout:\n${safe_output}\n"
        "stderr:\n${safe_error}")
endif()
if(EXISTS "${safe_payload}")
    message(FATAL_ERROR "dependency pruning left generated payload behind")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}"
        "-DLOCKDC_ROOT=${fake_root}"
        "-DLOCKDC_EXTERNAL_ROOT=${external_root}"
        -P "${fake_root}/cmake/prune_dependency_install_tree.cmake"
    RESULT_VARIABLE external_result
    OUTPUT_VARIABLE external_output
    ERROR_VARIABLE external_error)
if(external_result EQUAL 0)
    message(FATAL_ERROR "dependency pruning accepted an external root")
endif()
if(NOT EXISTS "${external_sentinel}")
    message(FATAL_ERROR "dependency pruning removed an external root before rejecting it")
endif()

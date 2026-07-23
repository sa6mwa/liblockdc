if(NOT DEFINED LOCKDC_ROOT OR LOCKDC_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()
if(NOT DEFINED LOCKDC_BINARY_DIR OR LOCKDC_BINARY_DIR STREQUAL "")
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

set(helper "${LOCKDC_ROOT}/cmake/acquire_verified_archive.cmake")
set(clean_script "${LOCKDC_ROOT}/scripts/clean.sh")
set(privacy_scan "${LOCKDC_ROOT}/tests/release_privacy_scan.cmake")
set(test_root "${LOCKDC_BINARY_DIR}/dependency-archive-cache-contract")
set(cache_root "${test_root}/shared-cache")
set(local_root "${test_root}/local")
set(source_dir "${test_root}/source")
set(source_archive "${source_dir}/fake-dependency-1.0.tar.gz")
set(local_archive "${local_root}/fake-dependency-1.0.tar.gz")
set(local_archive_offline "${local_root}/fake-dependency-1.0-offline.tar.gz")

file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY "${source_dir}" "${local_root}")
file(WRITE "${source_archive}" "verified archive payload\n")
file(SHA256 "${source_archive}" expected_sha256)

function(run_acquire output_path result_var output_var error_var)
    execute_process(
        COMMAND "${CMAKE_COMMAND}"
            -DLOCKDC_ARCHIVE_COMPONENT=fake-dependency
            "-DLOCKDC_ARCHIVE_URL=file://${source_archive}"
            -DLOCKDC_ARCHIVE_SHA256=${expected_sha256}
            -DLOCKDC_ARCHIVE_NAME=fake-dependency-1.0.tar.gz
            -DLOCKDC_ARCHIVE_OUTPUT=${output_path}
            -DLOCKDC_ARCHIVE_ALLOW_FILE_URL=ON
            -DCPKT_DEPENDENCY_CACHE=${cache_root}
            -P "${helper}"
        RESULT_VARIABLE acquire_result
        OUTPUT_VARIABLE acquire_output
        ERROR_VARIABLE acquire_error)
    set(${result_var} "${acquire_result}" PARENT_SCOPE)
    set(${output_var} "${acquire_output}" PARENT_SCOPE)
    set(${error_var} "${acquire_error}" PARENT_SCOPE)
endfunction()

run_acquire("${local_archive}" first_result first_output first_error)
if(NOT first_result EQUAL 0)
    message(FATAL_ERROR
        "initial verified archive acquisition failed\n"
        "stdout:\n${first_output}\n"
        "stderr:\n${first_error}")
endif()

set(global_archive "${cache_root}/archives/sha256/${expected_sha256}/fake-dependency-1.0.tar.gz")
set(global_lock "${cache_root}/locks/${expected_sha256}.lock")
foreach(required_path "${global_archive}" "${global_lock}" "${local_archive}")
    if(NOT EXISTS "${required_path}")
        message(FATAL_ERROR "verified archive acquisition did not create ${required_path}")
    endif()
endforeach()
file(SHA256 "${local_archive}" local_sha256)
if(NOT local_sha256 STREQUAL expected_sha256)
    message(FATAL_ERROR "local verified archive has wrong hash after initial acquisition")
endif()

file(REMOVE "${source_archive}" "${local_archive}")
run_acquire("${local_archive_offline}" offline_result offline_output offline_error)
if(NOT offline_result EQUAL 0)
    message(FATAL_ERROR
        "offline verified archive cache hit failed\n"
        "stdout:\n${offline_output}\n"
        "stderr:\n${offline_error}")
endif()
file(SHA256 "${local_archive_offline}" offline_sha256)
if(NOT offline_sha256 STREQUAL expected_sha256)
    message(FATAL_ERROR "offline verified archive cache hit produced wrong hash")
endif()

file(WRITE "${global_archive}" "corrupt archive payload\n")
file(REMOVE "${local_archive_offline}")
run_acquire("${local_archive_offline}" corrupt_result corrupt_output corrupt_error)
if(corrupt_result EQUAL 0)
    message(FATAL_ERROR "corrupt verified archive cache entry was accepted")
endif()
if(EXISTS "${local_archive_offline}")
    message(FATAL_ERROR "corrupt verified archive cache entry was copied to local output")
endif()
if(EXISTS "${global_archive}")
    file(SHA256 "${global_archive}" corrupt_sha256)
    if(corrupt_sha256 STREQUAL expected_sha256)
        message(FATAL_ERROR "corrupt cache test unexpectedly restored a valid archive")
    endif()
endif()

set(cache_path_leak "/tmp/liblockdc-cpkt-dependency-cache-sentinel")
set(leak_root "${test_root}/privacy-leak")
file(MAKE_DIRECTORY "${leak_root}")
file(WRITE "${leak_root}/leak.txt" "cache path: ${cache_path_leak}\n")
execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -DLOCKDC_ROOT=${LOCKDC_ROOT}
        -DCPKT_DEPENDENCY_CACHE=${cache_path_leak}
        -DLOCKDC_SCAN_LABEL=cache-leak-fixture
        "-DLOCKDC_SCAN_PATHS=${leak_root}"
        -P "${privacy_scan}"
    RESULT_VARIABLE privacy_result
    OUTPUT_VARIABLE privacy_output
    ERROR_VARIABLE privacy_error)
if(privacy_result EQUAL 0)
    message(FATAL_ERROR "release privacy scan accepted a CPKT_DEPENDENCY_CACHE path leak")
endif()

file(READ "${clean_script}" clean_script_text)
foreach(forbidden_snippet
    "CPKT_DEPENDENCY_CACHE"
    ".cache/c.pkt.systems/deps"
    "c.pkt.systems/deps")
    string(FIND "${clean_script_text}" "${forbidden_snippet}" forbidden_index)
    if(NOT forbidden_index EQUAL -1)
        message(FATAL_ERROR "clean script must not remove the shared dependency archive cache")
    endif()
endforeach()

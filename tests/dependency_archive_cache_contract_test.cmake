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

set(concurrent_source_archive "${source_dir}/fake-concurrent-1.0.tar.gz")
set(concurrent_name "fake-concurrent-1.0.tar.gz")
set(concurrent_local_root "${test_root}/concurrent-local")
set(concurrent_done_marker "${test_root}/concurrent.done")
file(MAKE_DIRECTORY "${concurrent_local_root}")
file(WRITE "${concurrent_source_archive}" "verified concurrent archive payload\n")
file(SHA256 "${concurrent_source_archive}" concurrent_sha256)
set(concurrent_global_archive
    "${cache_root}/archives/sha256/${concurrent_sha256}/${concurrent_name}")
set(concurrent_observed_tmp_marker "${test_root}/concurrent-observed-tmp")
set(concurrent_observer_script "${test_root}/concurrent-observer.cmake")
file(WRITE "${concurrent_observer_script}" "
if(NOT DEFINED LOCKDC_FINAL_ARCHIVE OR LOCKDC_FINAL_ARCHIVE STREQUAL \"\")
    message(FATAL_ERROR \"LOCKDC_FINAL_ARCHIVE is required\")
endif()
if(NOT DEFINED LOCKDC_EXPECTED_SHA256 OR LOCKDC_EXPECTED_SHA256 STREQUAL \"\")
    message(FATAL_ERROR \"LOCKDC_EXPECTED_SHA256 is required\")
endif()
if(NOT DEFINED LOCKDC_DONE_MARKER OR LOCKDC_DONE_MARKER STREQUAL \"\")
    message(FATAL_ERROR \"LOCKDC_DONE_MARKER is required\")
endif()
if(NOT DEFINED LOCKDC_OBSERVED_TMP_MARKER OR LOCKDC_OBSERVED_TMP_MARKER STREQUAL \"\")
    message(FATAL_ERROR \"LOCKDC_OBSERVED_TMP_MARKER is required\")
endif()
get_filename_component(observer_archive_dir \"\${LOCKDC_FINAL_ARCHIVE}\" DIRECTORY)
set(observer_seen_tmp OFF)
while(NOT EXISTS \"\${LOCKDC_DONE_MARKER}\")
    if(EXISTS \"\${LOCKDC_FINAL_ARCHIVE}\")
        file(SHA256 \"\${LOCKDC_FINAL_ARCHIVE}\" observer_sha256)
        string(TOLOWER \"\${observer_sha256}\" observer_sha256)
        if(NOT observer_sha256 STREQUAL LOCKDC_EXPECTED_SHA256)
            message(FATAL_ERROR
                \"concurrent cache observer saw unverified final archive: \${LOCKDC_FINAL_ARCHIVE}\")
        endif()
    endif()
    file(GLOB observer_tmp_paths \"\${observer_archive_dir}/*.tmp.*\")
    if(observer_tmp_paths)
        set(observer_seen_tmp ON)
    endif()
    execute_process(COMMAND \"${CMAKE_COMMAND}\" -E sleep 0.05)
endwhile()
if(observer_seen_tmp)
    file(WRITE \"\${LOCKDC_OBSERVED_TMP_MARKER}\" \"observed temporary archive\\n\")
endif()
")

set(concurrent_runner "${test_root}/run-concurrent-acquire.sh")
file(WRITE "${concurrent_runner}" "#!/bin/sh
set -eu
\"${CMAKE_COMMAND}\" \\
  -DLOCKDC_FINAL_ARCHIVE=\"${concurrent_global_archive}\" \\
  -DLOCKDC_EXPECTED_SHA256=\"${concurrent_sha256}\" \\
  -DLOCKDC_DONE_MARKER=\"${concurrent_done_marker}\" \\
  -DLOCKDC_OBSERVED_TMP_MARKER=\"${concurrent_observed_tmp_marker}\" \\
  -P \"${concurrent_observer_script}\" > \"${test_root}/concurrent-observer.log\" 2>&1 &
observer_pid=$!
pids=\"\"
i=1
while [ \"$i\" -le 6 ]; do
  \"${CMAKE_COMMAND}\" \\
    -DLOCKDC_ARCHIVE_COMPONENT=fake-concurrent \\
    -DLOCKDC_ARCHIVE_URL=\"file://${concurrent_source_archive}\" \\
    -DLOCKDC_ARCHIVE_SHA256=\"${concurrent_sha256}\" \\
    -DLOCKDC_ARCHIVE_NAME=\"${concurrent_name}\" \\
    -DLOCKDC_ARCHIVE_OUTPUT=\"${concurrent_local_root}/worker-$i/${concurrent_name}\" \\
    -DLOCKDC_ARCHIVE_ALLOW_FILE_URL=ON \\
    -DLOCKDC_ARCHIVE_TEST_DELAY_BEFORE_PUBLISH=1 \\
    -DCPKT_DEPENDENCY_CACHE=\"${cache_root}\" \\
    -P \"${helper}\" > \"${test_root}/concurrent-worker-$i.log\" 2>&1 &
  pids=\"$pids $!\"
  i=$((i + 1))
done
worker_status=0
for pid in $pids; do
  if ! wait \"$pid\"; then
    worker_status=1
  fi
done
printf 'done\\n' > \"${concurrent_done_marker}\"
if ! wait \"$observer_pid\"; then
  worker_status=1
fi
exit \"$worker_status\"
")
execute_process(COMMAND chmod +x "${concurrent_runner}")
execute_process(
    COMMAND "${concurrent_runner}"
    RESULT_VARIABLE concurrent_result
    OUTPUT_VARIABLE concurrent_output
    ERROR_VARIABLE concurrent_error)
if(NOT concurrent_result EQUAL 0)
    message(FATAL_ERROR
        "concurrent verified archive acquisition stress failed\n"
        "stdout:\n${concurrent_output}\n"
        "stderr:\n${concurrent_error}")
endif()
if(NOT EXISTS "${concurrent_global_archive}")
    message(FATAL_ERROR "concurrent verified archive acquisition did not publish the global archive")
endif()
file(SHA256 "${concurrent_global_archive}" concurrent_global_sha256)
if(NOT concurrent_global_sha256 STREQUAL concurrent_sha256)
    message(FATAL_ERROR "concurrent verified archive acquisition published the wrong hash")
endif()
foreach(worker_index RANGE 1 6)
    set(worker_archive "${concurrent_local_root}/worker-${worker_index}/${concurrent_name}")
    if(NOT EXISTS "${worker_archive}")
        message(FATAL_ERROR "concurrent worker ${worker_index} did not receive a local archive")
    endif()
    file(SHA256 "${worker_archive}" worker_sha256)
    if(NOT worker_sha256 STREQUAL concurrent_sha256)
        message(FATAL_ERROR "concurrent worker ${worker_index} received the wrong archive hash")
    endif()
endforeach()
if(NOT EXISTS "${concurrent_observed_tmp_marker}")
    message(FATAL_ERROR "concurrent cache observer did not observe a pre-publish temporary archive")
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

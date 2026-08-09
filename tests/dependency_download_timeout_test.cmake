if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

file(READ "${LOCKDC_ROOT}/scripts/deps.sh" deps_script)
file(READ "${LOCKDC_ROOT}/cmake/acquire_verified_archive.cmake" acquire_helper)

function(assert_contains needle description)
    string(FIND "${deps_script}" "${needle}" found_at)
    if(found_at EQUAL -1)
        message(FATAL_ERROR "missing ${description} in dependency provisioning script")
    endif()
endfunction()

assert_contains([=[download_timeout=${LOCKDC_DEPENDENCY_DOWNLOAD_TIMEOUT:-300}]=] "configurable download timeout")
assert_contains([=[local_download_root=${LOCKDC_DOWNLOAD_ROOT:-$repo_root/.cache/downloads}]=] "repository-local archive staging root")
assert_contains([=[acquire_verified_archive()]=] "verified archive acquisition wrapper")
assert_contains([=[-DLOCKDC_ARCHIVE_TIMEOUT="$download_timeout"]=] "download timeout passed to archive helper")
assert_contains([=[https://github.com/sa6mwa/c.pkt.systems/releases/download/v$cpkt_version/$cpkt_asset_name]=] "c.pkt.systems release URL")
assert_contains([=[$local_download_root/$cpkt_asset_name]=] "local c.pkt.systems archive staging path")

foreach(helper_snippet
    [=[${CPKT_DEPENDENCY_CACHE}/archives/sha256/${lockdc_archive_sha256}]=]
    [=[file(LOCK "${lockdc_lock_path}"]=]
    [=[file(DOWNLOAD]=]
    [=[TLS_VERIFY ON]=]
    [=[file(RENAME "${lockdc_tmp_path}" "${lockdc_archive_path}")]=])
    string(FIND "${acquire_helper}" "${helper_snippet}" helper_index)
    if(helper_index EQUAL -1)
        message(FATAL_ERROR "verified archive helper is missing ${helper_snippet}")
    endif()
endforeach()

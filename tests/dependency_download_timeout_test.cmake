if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

file(READ "${LOCKDC_ROOT}/scripts/deps.sh" deps_script)

function(assert_contains needle description)
    string(FIND "${deps_script}" "${needle}" found_at)
    if(found_at EQUAL -1)
        message(FATAL_ERROR "missing ${description} in dependency provisioning script")
    endif()
endfunction()

assert_contains([=[download_timeout=${LOCKDC_DEPENDENCY_DOWNLOAD_TIMEOUT:-300}]=] "configurable download timeout")
assert_contains([=[curl -fL --connect-timeout "$download_timeout"]=] "curl connection timeout")
assert_contains([=[--max-time "$download_timeout"]=] "curl maximum download time")
assert_contains([=[https://github.com/sa6mwa/c.pkt.systems/releases/download/v$cpkt_version/$cpkt_asset_name]=] "c.pkt.systems release URL")
assert_contains([=[$repo_root/.cache/downloads/$cpkt_asset_name]=] "shared c.pkt.systems download-root path")

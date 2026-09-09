if(NOT DEFINED LOCKDC_ROOT OR LOCKDC_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

file(READ "${LOCKDC_ROOT}/src/lc_transport.c" transport_source)

function(assert_contains needle description)
    string(FIND "${transport_source}" "${needle}" found_at)
    if(found_at EQUAL -1)
        message(FATAL_ERROR "missing ${description}")
    endif()
endfunction()

assert_contains("CURLOPT_SEEKFUNCTION, lonejson_curl_seek_callback"
    "LoneJSON upload seek callback")
assert_contains("CURLOPT_SEEKDATA, &body_upload"
    "LoneJSON upload seek callback context")

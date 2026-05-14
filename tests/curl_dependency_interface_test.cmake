if(NOT DEFINED LOCKDC_EXTERNAL_ROOT OR LOCKDC_EXTERNAL_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_EXTERNAL_ROOT is required")
endif()

set(curl_root "${LOCKDC_EXTERNAL_ROOT}/curl/install")
set(curl_header "${curl_root}/include/curl/curl.h")
set(curl_static_archive "${curl_root}/lib/libcurl.a")
set(curl_shared_library "${curl_root}/lib/libcurl.so")
set(cpkt_manifest "${LOCKDC_EXTERNAL_ROOT}/c.pkt.systems/install/share/c.pkt.systems/manifest.txt")

function(assert_literal_contains text needle description)
    string(FIND "${text}" "${needle}" found_at)
    if(found_at EQUAL -1)
        message(FATAL_ERROR "curl dependency metadata is missing ${description}")
    endif()
endfunction()

foreach(path IN ITEMS "${curl_header}" "${curl_static_archive}" "${curl_shared_library}")
    if(NOT EXISTS "${path}")
        message(FATAL_ERROR "missing curl public dependency artifact: ${path}")
    endif()
endforeach()

foreach(path IN ITEMS
    "${curl_root}/bin"
    "${curl_root}/lib/pkgconfig"
    "${curl_root}/lib/cmake")
    if(EXISTS "${path}")
        message(FATAL_ERROR "curl install tree still exposes non-public artifact: ${path}")
    endif()
endforeach()

if(NOT EXISTS "${cpkt_manifest}")
    message(FATAL_ERROR "missing c.pkt.systems manifest: ${cpkt_manifest}")
endif()

file(READ "${cpkt_manifest}" cpkt_manifest_text)
assert_literal_contains("${cpkt_manifest_text}" "bundle_version=0.1.0" "c.pkt.systems bundle version")
assert_literal_contains("${cpkt_manifest_text}" "curl_version=8.20.0" "curl bundle version")
assert_literal_contains("${cpkt_manifest_text}" "libssh2_version=1.11.1" "libssh2 bundle version")
assert_literal_contains("${cpkt_manifest_text}" "nghttp2_version=1.69.0" "nghttp2 bundle version")

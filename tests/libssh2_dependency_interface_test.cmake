if(NOT DEFINED LOCKDC_EXTERNAL_ROOT OR LOCKDC_EXTERNAL_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_EXTERNAL_ROOT is required")
endif()

set(libssh2_root "${LOCKDC_EXTERNAL_ROOT}/libssh2/install")
set(cpkt_manifest "${LOCKDC_EXTERNAL_ROOT}/c.pkt.systems/install/share/c.pkt.systems/manifest.txt")

function(assert_literal_contains text needle description)
    string(FIND "${text}" "${needle}" found_at)
    if(found_at EQUAL -1)
        message(FATAL_ERROR "libssh2 dependency metadata is missing ${description}")
    endif()
endfunction()

foreach(path IN ITEMS
    "${libssh2_root}/lib/libssh2.a"
    "${libssh2_root}/lib/libssh2.so"
    "${libssh2_root}/include/libssh2.h")
    if(NOT EXISTS "${path}")
        message(FATAL_ERROR "missing libssh2 dependency artifact: ${path}")
    endif()
endforeach()

foreach(path IN ITEMS
    "${libssh2_root}/lib/pkgconfig"
    "${libssh2_root}/lib/cmake"
    "${libssh2_root}/bin")
    if(EXISTS "${path}")
        message(FATAL_ERROR "libssh2 install tree still exposes non-public artifact: ${path}")
    endif()
endforeach()

if(NOT EXISTS "${cpkt_manifest}")
    message(FATAL_ERROR "missing c.pkt.systems manifest: ${cpkt_manifest}")
endif()

file(READ "${cpkt_manifest}" cpkt_manifest_text)
assert_literal_contains("${cpkt_manifest_text}" "bundle_version=0.1.0" "c.pkt.systems bundle version")
assert_literal_contains("${cpkt_manifest_text}" "libssh2_version=1.11.1" "libssh2 bundle version")
assert_literal_contains("${cpkt_manifest_text}" "zlib_version=1.3.2" "zlib bundle version")

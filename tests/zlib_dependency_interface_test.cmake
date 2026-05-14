if(NOT DEFINED LOCKDC_EXTERNAL_ROOT OR LOCKDC_EXTERNAL_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_EXTERNAL_ROOT is required")
endif()

set(zlib_root "${LOCKDC_EXTERNAL_ROOT}/zlib/install")
set(zlib_static_archive "${zlib_root}/lib/libz.a")
set(zlib_shared_library "${zlib_root}/lib/libz.so")
set(zlib_header "${zlib_root}/include/zlib.h")
set(cpkt_manifest "${LOCKDC_EXTERNAL_ROOT}/c.pkt.systems/install/share/c.pkt.systems/manifest.txt")

foreach(path IN ITEMS
    "${zlib_static_archive}"
    "${zlib_shared_library}"
    "${zlib_header}")
    if(NOT EXISTS "${path}")
        message(FATAL_ERROR "missing zlib dependency artifact: ${path}")
    endif()
endforeach()

foreach(path IN ITEMS
    "${zlib_root}/lib/pkgconfig"
    "${zlib_root}/lib/cmake"
    "${zlib_root}/share/man")
    if(EXISTS "${path}")
        message(FATAL_ERROR "zlib install tree still exposes non-public artifact: ${path}")
    endif()
endforeach()

if(NOT EXISTS "${cpkt_manifest}")
    message(FATAL_ERROR "missing c.pkt.systems manifest: ${cpkt_manifest}")
endif()

file(READ "${cpkt_manifest}" cpkt_manifest_text)
string(FIND "${cpkt_manifest_text}" "bundle_version=0.1.0" bundle_version_at)
if(bundle_version_at EQUAL -1)
    message(FATAL_ERROR "c.pkt.systems manifest is missing bundle version")
endif()
string(FIND "${cpkt_manifest_text}" "zlib_version=1.3.2" zlib_version_at)
if(zlib_version_at EQUAL -1)
    message(FATAL_ERROR "c.pkt.systems manifest is missing zlib version")
endif()

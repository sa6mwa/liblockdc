if(NOT DEFINED LOCKDC_EXTERNAL_ROOT OR LOCKDC_EXTERNAL_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_EXTERNAL_ROOT is required")
endif()

if(NOT DEFINED LOCKDC_DEPENDENCY_BUILD_ROOT OR LOCKDC_DEPENDENCY_BUILD_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_DEPENDENCY_BUILD_ROOT is required")
endif()

set(zlib_root "${LOCKDC_EXTERNAL_ROOT}/zlib/install")
set(zlib_build_root "${LOCKDC_DEPENDENCY_BUILD_ROOT}/zlib/build")
set(zlib_static_archive "${zlib_root}/lib/libz.a")
set(zlib_shared_library "${zlib_root}/lib/libz.so")
set(zlib_header "${zlib_root}/include/zlib.h")

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

set(zlib_ninja "${zlib_build_root}/build.ninja")
if(NOT EXISTS "${zlib_ninja}")
    message(FATAL_ERROR "missing zlib build graph: ${zlib_ninja}")
endif()

file(READ "${zlib_ninja}" zlib_ninja_text)
if(NOT zlib_ninja_text MATCHES "CMakeFiles/zlib_object\\.dir/")
    message(FATAL_ERROR "zlib dependency build is not using a shared object library")
endif()

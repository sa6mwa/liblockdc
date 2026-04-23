if(NOT DEFINED LOCKDC_EXTERNAL_ROOT OR LOCKDC_EXTERNAL_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_EXTERNAL_ROOT is required")
endif()

if(NOT DEFINED LOCKDC_DEPENDENCY_BUILD_ROOT OR LOCKDC_DEPENDENCY_BUILD_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_DEPENDENCY_BUILD_ROOT is required")
endif()

set(libssh2_root "${LOCKDC_EXTERNAL_ROOT}/libssh2/install")
set(libssh2_build_ninja "${LOCKDC_DEPENDENCY_BUILD_ROOT}/libssh2/build/build.ninja")
set(libssh2_pc "${LOCKDC_DEPENDENCY_BUILD_ROOT}/libssh2/build/src/libssh2.pc")

function(assert_contains text pattern description)
    if(NOT text MATCHES "${pattern}")
        message(FATAL_ERROR "libssh2 dependency metadata is missing ${description}")
    endif()
endfunction()

foreach(path IN ITEMS
    "${libssh2_root}/lib/libssh2.a"
    "${libssh2_root}/lib/libssh2.so"
    "${libssh2_root}/include/libssh2.h"
    "${libssh2_pc}"
    "${libssh2_build_ninja}")
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

file(READ "${libssh2_pc}" libssh2_pc_text)
file(READ "${libssh2_build_ninja}" libssh2_build_ninja_text)

assert_contains("${libssh2_pc_text}" "zlib" "zlib dependency in libssh2.pc")
assert_contains("${libssh2_build_ninja_text}" "src/CMakeFiles/libssh2_object.dir/" "libssh2 object-library build outputs")

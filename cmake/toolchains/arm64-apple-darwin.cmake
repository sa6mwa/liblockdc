set(CMAKE_SYSTEM_NAME Darwin)
set(CMAKE_SYSTEM_PROCESSOR arm64)
set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)

if(DEFINED ENV{OSXCROSS_ROOT} AND NOT "$ENV{OSXCROSS_ROOT}" STREQUAL "")
    set(LOCKDC_OSXCROSS_ROOT "$ENV{OSXCROSS_ROOT}")
elseif(DEFINED ENV{HOME} AND NOT "$ENV{HOME}" STREQUAL "")
    set(LOCKDC_OSXCROSS_ROOT "$ENV{HOME}/.local/cross/osxcross")
else()
    message(FATAL_ERROR "OSXCROSS_ROOT is not set and HOME is unavailable")
endif()

set(LOCKDC_OSXCROSS_HOST "arm64-apple-darwin25" CACHE STRING "osxcross target host triple")
set(LOCKDC_MACOS_DEPLOYMENT_TARGET "15.0" CACHE STRING "Minimum macOS deployment target")
set(CMAKE_OSX_DEPLOYMENT_TARGET "${LOCKDC_MACOS_DEPLOYMENT_TARGET}" CACHE STRING "" FORCE)

set(LOCKDC_OSXCROSS_BIN_DIR "${LOCKDC_OSXCROSS_ROOT}/bin")
set(ENV{PATH} "${LOCKDC_OSXCROSS_BIN_DIR}:$ENV{PATH}")
set(CMAKE_C_COMPILER "${LOCKDC_OSXCROSS_BIN_DIR}/${LOCKDC_OSXCROSS_HOST}-clang" CACHE FILEPATH "")
set(CMAKE_CXX_COMPILER "${LOCKDC_OSXCROSS_BIN_DIR}/${LOCKDC_OSXCROSS_HOST}-clang++" CACHE FILEPATH "")
set(CMAKE_AR "${LOCKDC_OSXCROSS_BIN_DIR}/${LOCKDC_OSXCROSS_HOST}-ar" CACHE FILEPATH "")
set(CMAKE_RANLIB "${LOCKDC_OSXCROSS_BIN_DIR}/${LOCKDC_OSXCROSS_HOST}-ranlib" CACHE FILEPATH "")
set(CMAKE_LINKER "${LOCKDC_OSXCROSS_BIN_DIR}/${LOCKDC_OSXCROSS_HOST}-ld" CACHE FILEPATH "")
set(CMAKE_INSTALL_NAME_TOOL "${LOCKDC_OSXCROSS_BIN_DIR}/${LOCKDC_OSXCROSS_HOST}-install_name_tool" CACHE FILEPATH "")
set(LOCKDC_OTOOL "${LOCKDC_OSXCROSS_BIN_DIR}/${LOCKDC_OSXCROSS_HOST}-otool" CACHE FILEPATH "")

foreach(_lockdc_required_tool
        CMAKE_C_COMPILER
        CMAKE_AR
        CMAKE_RANLIB
        CMAKE_LINKER
        CMAKE_INSTALL_NAME_TOOL
        LOCKDC_OTOOL)
    if(NOT EXISTS "${${_lockdc_required_tool}}")
        message(FATAL_ERROR
            "The arm64 Apple Darwin osxcross toolchain is missing ${_lockdc_required_tool}: "
            "${${_lockdc_required_tool}}. Set OSXCROSS_ROOT or install osxcross under $HOME/.local/cross/osxcross.")
    endif()
endforeach()

set(_lockdc_darwin_linker_flag "-fuse-ld=${CMAKE_LINKER}")
foreach(_lockdc_linker_flags
        CMAKE_EXE_LINKER_FLAGS
        CMAKE_SHARED_LINKER_FLAGS
        CMAKE_MODULE_LINKER_FLAGS)
    if(NOT "${${_lockdc_linker_flags}}" MATCHES "(^| )-fuse-ld=")
        set(${_lockdc_linker_flags} "${_lockdc_darwin_linker_flag} ${${_lockdc_linker_flags}}" CACHE STRING "" FORCE)
    endif()
endforeach()
unset(_lockdc_linker_flags)
unset(_lockdc_darwin_linker_flag)

file(GLOB _lockdc_osxcross_sdks LIST_DIRECTORIES true "${LOCKDC_OSXCROSS_ROOT}/SDK/MacOSX*.sdk")
if(NOT _lockdc_osxcross_sdks)
    message(FATAL_ERROR "failed to locate a usable osxcross macOS SDK under ${LOCKDC_OSXCROSS_ROOT}/SDK")
endif()
list(SORT _lockdc_osxcross_sdks)
list(REVERSE _lockdc_osxcross_sdks)
list(GET _lockdc_osxcross_sdks 0 LOCKDC_OSXCROSS_SDK)
if(NOT EXISTS "${LOCKDC_OSXCROSS_SDK}/usr/include")
    message(FATAL_ERROR "failed to locate a usable osxcross macOS SDK under ${LOCKDC_OSXCROSS_ROOT}/SDK")
endif()

set(CMAKE_OSX_SYSROOT "${LOCKDC_OSXCROSS_SDK}" CACHE PATH "" FORCE)
set(CMAKE_FIND_ROOT_PATH "${LOCKDC_OSXCROSS_SDK}")
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)

set(LOCKDC_TARGET_ARCH arm64 CACHE STRING "" FORCE)
set(LOCKDC_TARGET_OS darwin CACHE STRING "" FORCE)
set(LOCKDC_TARGET_LIBC "" CACHE STRING "" FORCE)

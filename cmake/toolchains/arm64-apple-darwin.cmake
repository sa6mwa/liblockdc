set(CMAKE_SYSTEM_NAME Darwin)
set(CMAKE_SYSTEM_PROCESSOR arm64)
set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)

get_filename_component(_lockdc_toolchain_resolver
    "${CMAKE_CURRENT_LIST_DIR}/../../scripts/cpkt-toolchains.sh" ABSOLUTE)
set(_lockdc_discovery_command "${CMAKE_COMMAND}" -E env)
if(DEFINED LOCKDC_OSXCROSS_HOST AND NOT LOCKDC_OSXCROSS_HOST STREQUAL "")
    list(APPEND _lockdc_discovery_command "CPKT_OSXCROSS_HOST=${LOCKDC_OSXCROSS_HOST}")
elseif(NOT DEFINED ENV{CPKT_OSXCROSS_HOST} OR "$ENV{CPKT_OSXCROSS_HOST}" STREQUAL "")
    if(DEFINED ENV{LOCKDC_OSXCROSS_HOST} AND NOT "$ENV{LOCKDC_OSXCROSS_HOST}" STREQUAL "")
        list(APPEND _lockdc_discovery_command "CPKT_OSXCROSS_HOST=$ENV{LOCKDC_OSXCROSS_HOST}")
    endif()
endif()
execute_process(
    COMMAND ${_lockdc_discovery_command} "${_lockdc_toolchain_resolver}" discover arm64-apple-darwin
    RESULT_VARIABLE _lockdc_discovery_result
    OUTPUT_VARIABLE _lockdc_toolchain_description
    ERROR_VARIABLE _lockdc_discovery_error)
if(NOT _lockdc_discovery_result EQUAL 0 OR
   NOT _lockdc_toolchain_description MATCHES "(^|\n)status=ready(\n|$)")
    message(FATAL_ERROR
        "Darwin osxcross and pinned host MIG are not ready. Run scripts/cpkt-toolchains.sh ensure arm64-apple-darwin and reconfigure. ${_lockdc_discovery_error}")
endif()
foreach(_lockdc_key root prefix mig migcom mig_revision)
    string(REGEX MATCH "(^|\n)${_lockdc_key}=([^\r\n]+)" _lockdc_match "${_lockdc_toolchain_description}")
    if(NOT _lockdc_match)
        message(FATAL_ERROR "Darwin resolver did not report ${_lockdc_key}")
    endif()
    set(_lockdc_resolved_${_lockdc_key} "${CMAKE_MATCH_2}")
endforeach()
set(LOCKDC_OSXCROSS_ROOT "${_lockdc_resolved_root}")
set(LOCKDC_OSXCROSS_HOST "${_lockdc_resolved_prefix}" CACHE STRING "osxcross target host triple" FORCE)
set(LOCKDC_HOST_MIG "${_lockdc_resolved_mig}" CACHE FILEPATH "Pinned Linux host MIG" FORCE)
set(LOCKDC_HOST_MIGCOM "${_lockdc_resolved_migcom}" CACHE FILEPATH "Pinned Linux host MIG compiler" FORCE)
set(LOCKDC_HOST_MIG_REVISION "${_lockdc_resolved_mig_revision}" CACHE STRING "Pinned host MIG revision" FORCE)
set(LOCKDC_MACOS_DEPLOYMENT_TARGET "15.0" CACHE STRING "Minimum macOS deployment target")
set(CMAKE_OSX_DEPLOYMENT_TARGET "${LOCKDC_MACOS_DEPLOYMENT_TARGET}" CACHE STRING "" FORCE)

set(LOCKDC_OSXCROSS_BIN_DIR "${LOCKDC_OSXCROSS_ROOT}/bin")
set(ENV{PATH} "${LOCKDC_OSXCROSS_BIN_DIR}:$ENV{PATH}")
set(CMAKE_C_COMPILER "${LOCKDC_OSXCROSS_BIN_DIR}/${LOCKDC_OSXCROSS_HOST}-clang" CACHE FILEPATH "")
set(CMAKE_CXX_COMPILER "${LOCKDC_OSXCROSS_BIN_DIR}/${LOCKDC_OSXCROSS_HOST}-clang++" CACHE FILEPATH "")
set(CMAKE_AR "${LOCKDC_OSXCROSS_BIN_DIR}/${LOCKDC_OSXCROSS_HOST}-ar" CACHE FILEPATH "")
set(CMAKE_RANLIB "${LOCKDC_OSXCROSS_BIN_DIR}/${LOCKDC_OSXCROSS_HOST}-ranlib" CACHE FILEPATH "")
set(CMAKE_LINKER "${LOCKDC_OSXCROSS_BIN_DIR}/${LOCKDC_OSXCROSS_HOST}-ld" CACHE FILEPATH "")
set(CMAKE_NM "${LOCKDC_OSXCROSS_BIN_DIR}/${LOCKDC_OSXCROSS_HOST}-nm" CACHE FILEPATH "")
set(CMAKE_STRIP "${LOCKDC_OSXCROSS_BIN_DIR}/${LOCKDC_OSXCROSS_HOST}-strip" CACHE FILEPATH "")
set(CMAKE_INSTALL_NAME_TOOL "${LOCKDC_OSXCROSS_BIN_DIR}/${LOCKDC_OSXCROSS_HOST}-install_name_tool" CACHE FILEPATH "")
set(LOCKDC_OTOOL "${LOCKDC_OSXCROSS_BIN_DIR}/${LOCKDC_OSXCROSS_HOST}-otool" CACHE FILEPATH "")

foreach(_lockdc_required_tool
        CMAKE_C_COMPILER
        CMAKE_CXX_COMPILER
        CMAKE_AR
        CMAKE_RANLIB
        CMAKE_LINKER
        CMAKE_NM
        CMAKE_STRIP
        CMAKE_INSTALL_NAME_TOOL
        LOCKDC_OTOOL)
    if(NOT EXISTS "${${_lockdc_required_tool}}")
        message(FATAL_ERROR
            "The arm64 Apple Darwin osxcross toolchain is missing ${_lockdc_required_tool}: "
            "${${_lockdc_required_tool}}. Set OSXCROSS_ROOT or install osxcross under $HOME/.local/cross/osxcross.")
    endif()
endforeach()

set(_lockdc_darwin_linker_flag "--ld-path=${CMAKE_LINKER}")
foreach(_lockdc_linker_flags
        CMAKE_EXE_LINKER_FLAGS
        CMAKE_SHARED_LINKER_FLAGS
        CMAKE_MODULE_LINKER_FLAGS)
    if(NOT "${${_lockdc_linker_flags}}" MATCHES "(^| )--ld-path=")
        set(${_lockdc_linker_flags} "${_lockdc_darwin_linker_flag} ${${_lockdc_linker_flags}}" CACHE STRING "" FORCE)
    endif()
endforeach()
unset(_lockdc_linker_flags)
unset(_lockdc_darwin_linker_flag)

file(GLOB _lockdc_osxcross_sdks LIST_DIRECTORIES true "${LOCKDC_OSXCROSS_ROOT}/SDK/MacOSX*.sdk")
if(NOT _lockdc_osxcross_sdks)
    message(FATAL_ERROR "failed to locate a usable osxcross macOS SDK under ${LOCKDC_OSXCROSS_ROOT}/SDK")
endif()
list(SORT _lockdc_osxcross_sdks COMPARE NATURAL)
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

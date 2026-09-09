cmake_minimum_required(VERSION 3.21)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(LcVersion)

lc_detect_version(LOCKDC_RESOLVED_VERSION)

project(lockdc VERSION ${LOCKDC_RESOLVED_VERSION} LANGUAGES C)

include(GNUInstallDirs)
include(CMakeDependentOption)
include(CMakePackageConfigHelpers)
include(ProcessorCount)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
find_library(LOCKDC_ATOMIC_LIBRARY NAMES atomic)
set(LOCKDC_EXPORTED_ATOMIC_LIBRARY "")
set(LOCKDC_PC_PRIVATE_EXTRA_LIBS "")
set(LOCKDC_PC_SYSTEM_LIBS "")
set(LOCKDC_EXPORTED_DARWIN_FRAMEWORKS "")
if(LOCKDC_ATOMIC_LIBRARY AND NOT LOCKDC_ATOMIC_LIBRARY MATCHES "-NOTFOUND$")
    set(LOCKDC_EXPORTED_ATOMIC_LIBRARY "atomic")
endif()
if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(LOCKDC_EXPORTED_DARWIN_FRAMEWORKS "-framework CoreFoundation;-framework SystemConfiguration")
    list(APPEND LOCKDC_PC_SYSTEM_LIBS "-framework CoreFoundation" "-framework SystemConfiguration")
endif()

set(LOCKDC_COVERAGE_RUNTIME_LIBRARY "")
if(LOCKDC_ENABLE_COVERAGE AND CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
    set(LOCKDC_COVERAGE_RUNTIME_LIBRARY "gcov")
endif()
foreach(_lockdc_pc_system_lib IN LISTS CMAKE_DL_LIBS LOCKDC_EXPORTED_ATOMIC_LIBRARY LOCKDC_COVERAGE_RUNTIME_LIBRARY)
    if(_lockdc_pc_system_lib STREQUAL "")
        continue()
    endif()
    if(IS_ABSOLUTE "${_lockdc_pc_system_lib}" OR _lockdc_pc_system_lib MATCHES "^-")
        list(APPEND LOCKDC_PC_SYSTEM_LIBS "${_lockdc_pc_system_lib}")
    else()
        list(APPEND LOCKDC_PC_SYSTEM_LIBS "-l${_lockdc_pc_system_lib}")
    endif()
endforeach()
if(LOCKDC_PC_SYSTEM_LIBS)
    string(JOIN " " LOCKDC_PC_PRIVATE_EXTRA_LIBS ${LOCKDC_PC_SYSTEM_LIBS})
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(LOCKDC_BUILD_STATIC "Build the static lockdc library." ON)
option(LOCKDC_BUILD_SHARED "Build the shared lockdc library." ON)
option(LOCKDC_BUILD_DEPENDENCIES "Provision vendored dependency artifacts needed by this preset." OFF)
option(LOCKDC_BUILD_EXAMPLES "Build example programs." ON)
option(LOCKDC_BUILD_TESTS "Build unit tests." ON)
option(LOCKDC_BUILD_LUA_BINDINGS "Build the optional Lua extension when Lua headers are available." ON)
cmake_dependent_option(LOCKDC_BUILD_E2E_TESTS
    "Build end-to-end tests that target a real lockd environment."
    OFF
    "LOCKDC_BUILD_TESTS"
    OFF
)
option(LOCKDC_BUILD_BENCHMARKS "Build local benchmark targets." ON)
option(LOCKDC_BUILD_FUZZERS "Build AFL++ deterministic unit fuzz targets." OFF)
option(LOCKDC_BUILD_POUCH_INTEGRATION_FUZZERS
    "Build production-code Pouch integration mutation targets." OFF)
option(LOCKDC_ENABLE_COVERAGE "Build test targets with gcov coverage instrumentation." OFF)
option(LOCKDC_INSTALL "Enable install rules." ON)

set(LOCKDC_OPENSSL_VERSION "3.6.2" CACHE STRING "Pinned OpenSSL release supplied by c.pkt.systems.")
set(LOCKDC_ZLIB_VERSION "1.3.2" CACHE STRING "Pinned zlib release supplied by c.pkt.systems.")
set(LOCKDC_CURL_VERSION "8.20.0" CACHE STRING "Pinned curl release supplied by c.pkt.systems.")
set(LOCKDC_NGHTTP2_VERSION "1.69.0" CACHE STRING "Pinned nghttp2 release supplied by c.pkt.systems.")
set(LOCKDC_LIBSSH2_VERSION "1.11.1" CACHE STRING "Pinned libssh2 release supplied by c.pkt.systems.")
set(LOCKDC_LONEJSON_VERSION "0.43.0" CACHE STRING "Pinned lonejson release used for mapped JSON load/save and internal JSON parsing helpers." FORCE)
set(LOCKDC_LONEJSON_ABI_VERSION "26" CACHE STRING "Pinned lonejson shared-library ABI/SOVERSION." FORCE)
set(LOCKDC_LIBLQL_VERSION "0.2.0" CACHE STRING "Pinned liblql release used for pouch LQL query support." FORCE)
set(LOCKDC_LIBLQL_ABI_VERSION "0" CACHE STRING "Pinned liblql shared-library ABI/SOVERSION." FORCE)
set(LOCKDC_CMOCKA_VERSION "2.0.2" CACHE STRING "Pinned cmocka release used for unit tests.")
set(LOCKDC_PSLOG_VERSION "0.9.0" CACHE STRING "Pinned libpslog release used for SDK logging." FORCE)
set(LOCKDC_CPKT_VERSION "0.9.0" CACHE STRING "Pinned c.pkt.systems dependency bundle version." FORCE)
set(LOCKDC_MIN_ABI_VERSION "3")
set(LOCKDC_ABI_VERSION "${LOCKDC_MIN_ABI_VERSION}" CACHE STRING "Shared-library ABI/SOVERSION for liblockdc.")
if(LOCKDC_ABI_VERSION MATCHES "^[0-9]+$" AND LOCKDC_ABI_VERSION LESS LOCKDC_MIN_ABI_VERSION)
    set(LOCKDC_ABI_VERSION "${LOCKDC_MIN_ABI_VERSION}" CACHE STRING "Shared-library ABI/SOVERSION for liblockdc." FORCE)
endif()
if(DEFINED CPKT_DEPENDENCY_CACHE AND NOT "${CPKT_DEPENDENCY_CACHE}" STREQUAL "")
    set(LOCKDC_DEFAULT_CPKT_DEPENDENCY_CACHE "${CPKT_DEPENDENCY_CACHE}")
elseif(DEFINED ENV{CPKT_DEPENDENCY_CACHE} AND NOT "$ENV{CPKT_DEPENDENCY_CACHE}" STREQUAL "")
    set(LOCKDC_DEFAULT_CPKT_DEPENDENCY_CACHE "$ENV{CPKT_DEPENDENCY_CACHE}")
elseif(DEFINED ENV{XDG_CACHE_HOME} AND NOT "$ENV{XDG_CACHE_HOME}" STREQUAL "")
    set(LOCKDC_DEFAULT_CPKT_DEPENDENCY_CACHE "$ENV{XDG_CACHE_HOME}/c.pkt.systems/deps")
elseif(DEFINED ENV{HOME} AND NOT "$ENV{HOME}" STREQUAL "")
    set(LOCKDC_DEFAULT_CPKT_DEPENDENCY_CACHE "$ENV{HOME}/.cache/c.pkt.systems/deps")
else()
    message(FATAL_ERROR
        "CPKT_DEPENDENCY_CACHE, XDG_CACHE_HOME, or HOME is required for the shared verified dependency archive cache. Set -DCPKT_DEPENDENCY_CACHE=<path> when running without a user cache environment.")
endif()
set(CPKT_DEPENDENCY_CACHE "${LOCKDC_DEFAULT_CPKT_DEPENDENCY_CACHE}" CACHE PATH "Shared verified c.pkt.systems dependency archive cache.")
set(LOCKDC_DOWNLOAD_ROOT "${CMAKE_SOURCE_DIR}/.cache/downloads" CACHE PATH "Repository-local verified dependency archive staging root.")
set(LOCKDC_CMOCKA_ARCHIVE_PATH "" CACHE FILEPATH "Verified local cmocka release archive path.")
set(LOCKDC_PSLOG_ARCHIVE_PATH "" CACHE FILEPATH "Verified local libpslog release archive path.")
set(LOCKDC_DIST_DIR "${CMAKE_BINARY_DIR}/dist" CACHE PATH "Output directory for generated package archives and checksum manifests.")
set(LOCKDC_VERSION_OVERRIDE "" CACHE STRING "Override the detected liblockdc package version with an explicit semantic version.")
set(LOCKDC_TARGET_ARCH "${CMAKE_SYSTEM_PROCESSOR}" CACHE STRING "Artifact architecture name")
set(LOCKDC_TARGET_OS "${CMAKE_SYSTEM_NAME}" CACHE STRING "Artifact operating system name")
set(LOCKDC_TARGET_LIBC "" CACHE STRING "Artifact libc suffix for Linux targets (gnu, musl, or empty)")
set(LOCKDC_DEPENDENCY_BUILD_TYPE "Release")
ProcessorCount(LOCKDC_DEPENDENCY_BUILD_JOBS_DEFAULT)
if(NOT LOCKDC_DEPENDENCY_BUILD_JOBS_DEFAULT OR LOCKDC_DEPENDENCY_BUILD_JOBS_DEFAULT LESS 1)
    set(LOCKDC_DEPENDENCY_BUILD_JOBS_DEFAULT 1)
endif()
set(LOCKDC_DEPENDENCY_BUILD_JOBS "${LOCKDC_DEPENDENCY_BUILD_JOBS_DEFAULT}" CACHE STRING "Parallel job count for vendored dependency builds.")
set(LOCKDC_DEPENDENCY_DOWNLOAD_TIMEOUT "300" CACHE STRING "Maximum time in seconds for a vendored dependency download attempt.")
set(LOCKDC_DEPENDENCY_DOWNLOAD_INACTIVITY_TIMEOUT "60" CACHE STRING "Maximum idle time in seconds for a vendored dependency download attempt.")

if(NOT LOCKDC_BUILD_STATIC AND NOT LOCKDC_BUILD_SHARED)
    message(FATAL_ERROR "Enable at least one of LOCKDC_BUILD_STATIC or LOCKDC_BUILD_SHARED.")
endif()
if(NOT LOCKDC_ABI_VERSION MATCHES "^[0-9]+$")
    message(FATAL_ERROR "LOCKDC_ABI_VERSION must be a non-negative integer.")
endif()

if(LOCKDC_BUILD_FUZZERS)
    if(NOT DEFINED LOCKDC_AFLPP_ROOT OR LOCKDC_AFLPP_ROOT STREQUAL "")
        message(FATAL_ERROR "LOCKDC_BUILD_FUZZERS=ON requires the pinned AFL++ lifecycle toolchain")
    endif()
    if(NOT LOCKDC_BUILD_STATIC)
        message(FATAL_ERROR "LOCKDC_BUILD_FUZZERS=ON requires LOCKDC_BUILD_STATIC=ON")
    endif()
endif()

string(TOLOWER "${LOCKDC_TARGET_OS}" LOCKDC_TARGET_OS_LOWER)
string(TOLOWER "${LOCKDC_TARGET_ARCH}" LOCKDC_TARGET_ARCH_LOWER)

if(LOCKDC_TARGET_ARCH_LOWER STREQUAL "amd64")
    set(LOCKDC_TARGET_ARCH_LOWER "x86_64")
endif()
if(LOCKDC_TARGET_ARCH_LOWER STREQUAL "arm64" AND NOT LOCKDC_TARGET_OS_LOWER STREQUAL "darwin")
    set(LOCKDC_TARGET_ARCH_LOWER "aarch64")
endif()
if(LOCKDC_TARGET_ARCH_LOWER STREQUAL "arm")
    set(LOCKDC_TARGET_ARCH_LOWER "armhf")
endif()
set(LOCKDC_TARGET_ARCH "${LOCKDC_TARGET_ARCH_LOWER}" CACHE STRING "Artifact architecture name" FORCE)

if(LOCKDC_TARGET_OS_LOWER STREQUAL "linux" AND NOT LOCKDC_TARGET_LIBC)
    execute_process(
        COMMAND "${CMAKE_C_COMPILER}" -dumpmachine
        OUTPUT_VARIABLE lockdc_compiler_machine
        RESULT_VARIABLE lockdc_compiler_machine_result
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )
    if(lockdc_compiler_machine_result EQUAL 0 AND lockdc_compiler_machine MATCHES "musl")
        set(LOCKDC_TARGET_LIBC "musl")
    else()
        set(LOCKDC_TARGET_LIBC "gnu")
    endif()
endif()

if(LOCKDC_TARGET_OS_LOWER STREQUAL "darwin")
    set(LOCKDC_TARGET_ID "${LOCKDC_TARGET_ARCH_LOWER}-apple-darwin")
elseif(LOCKDC_TARGET_OS_LOWER STREQUAL "freebsd")
    set(LOCKDC_TARGET_ID "${LOCKDC_TARGET_ARCH_LOWER}-freebsd")
elseif(LOCKDC_TARGET_OS_LOWER STREQUAL "linux")
    if(LOCKDC_TARGET_LIBC)
        set(LOCKDC_TARGET_ID "${LOCKDC_TARGET_ARCH_LOWER}-linux-${LOCKDC_TARGET_LIBC}")
    else()
        set(LOCKDC_TARGET_ID "${LOCKDC_TARGET_ARCH_LOWER}-linux")
    endif()
else()
    set(LOCKDC_TARGET_ID "${LOCKDC_TARGET_ARCH_LOWER}-${LOCKDC_TARGET_OS_LOWER}")
endif()

if(NOT DEFINED LOCKDC_EXTERNAL_ROOT)
    set(LOCKDC_EXTERNAL_ROOT "${CMAKE_SOURCE_DIR}/.cache/deps/${LOCKDC_TARGET_ID}" CACHE PATH "Stable install root for downloaded third-party dependencies.")
endif()
if(NOT DEFINED LOCKDC_DEPENDENCY_BUILD_ROOT)
    set(LOCKDC_DEPENDENCY_BUILD_ROOT "${CMAKE_SOURCE_DIR}/.cache/deps-build/${LOCKDC_TARGET_ID}" CACHE PATH "Stable build root for downloaded third-party dependencies.")
endif()
if(LOCKDC_BUILD_DEPENDENCIES)
    include(LcGeneratedPath)
    lockdc_assert_generated_path("${CMAKE_SOURCE_DIR}" "${LOCKDC_DOWNLOAD_ROOT}")
    lockdc_assert_generated_path("${CMAKE_SOURCE_DIR}" "${LOCKDC_EXTERNAL_ROOT}")
    lockdc_assert_generated_path("${CMAKE_SOURCE_DIR}" "${LOCKDC_DEPENDENCY_BUILD_ROOT}")
endif()

set(LOCKDC_C_TEST_ENVIRONMENT "")
if(CMAKE_CROSSCOMPILING)
    list(APPEND LOCKDC_C_TEST_ENVIRONMENT "LOCKDC_SLOW_TEST_RUNTIME=1")
endif()
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND CMAKE_SYSROOT AND LOCKDC_TARGET_LIBC STREQUAL "musl")
    set(lockdc_c_test_runtime_dirs)
    foreach(lockdc_runtime_dir
            "${CMAKE_SYSROOT}/lib"
            "${CMAKE_SYSROOT}/usr/lib"
            "${CMAKE_SYSROOT}/lib64"
            "${CMAKE_SYSROOT}/usr/lib64")
        if(IS_DIRECTORY "${lockdc_runtime_dir}")
            list(APPEND lockdc_c_test_runtime_dirs "${lockdc_runtime_dir}")
        endif()
    endforeach()
    execute_process(
        COMMAND "${CMAKE_C_COMPILER}" -print-file-name=libatomic.so.1
        OUTPUT_VARIABLE lockdc_libatomic_path
        RESULT_VARIABLE lockdc_libatomic_result
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET)
    if(lockdc_libatomic_result EQUAL 0
       AND NOT lockdc_libatomic_path STREQUAL "libatomic.so.1"
       AND EXISTS "${lockdc_libatomic_path}")
        get_filename_component(lockdc_libatomic_dir "${lockdc_libatomic_path}" DIRECTORY)
        list(APPEND lockdc_c_test_runtime_dirs "${lockdc_libatomic_dir}")
    endif()
    list(REMOVE_DUPLICATES lockdc_c_test_runtime_dirs)
    if(lockdc_c_test_runtime_dirs)
        string(JOIN ":" lockdc_c_test_runtime_path ${lockdc_c_test_runtime_dirs})
        list(APPEND LOCKDC_C_TEST_ENVIRONMENT "LD_LIBRARY_PATH=${lockdc_c_test_runtime_path}")
    endif()
endif()

function(lc_configure_fatal_linker_warnings target)
    get_target_property(target_type ${target} TYPE)
    if(NOT target_type STREQUAL "EXECUTABLE" AND
       NOT target_type STREQUAL "SHARED_LIBRARY" AND
       NOT target_type STREQUAL "MODULE_LIBRARY")
        return()
    endif()

    if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
        target_link_options(${target} PRIVATE "LINKER:-fatal_warnings")
    elseif(UNIX OR CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
        target_link_options(${target} PRIVATE "LINKER:--fatal-warnings")
    endif()
endfunction()

function(lc_configure_c_target target)
    if(UNIX OR APPLE OR CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
        target_compile_definitions(${target} PRIVATE _POSIX_C_SOURCE=200809L)
    endif()
    if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
        target_compile_definitions(${target} PRIVATE _DARWIN_C_SOURCE=1)
    endif()
    set_target_properties(${target} PROPERTIES
        C_EXTENSIONS OFF
        POSITION_INDEPENDENT_CODE ON
        VISIBILITY_INLINES_HIDDEN NO
    )
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(${target} PRIVATE
            -std=c89
            -Wall
            -Wextra
            -Wpedantic
            -pedantic-errors
            -Werror
        )
    endif()
    lc_configure_fatal_linker_warnings(${target})
endfunction()

function(lc_configure_c_test_target target)
    set_target_properties(${target} PROPERTIES
        C_STANDARD 99
        C_STANDARD_REQUIRED ON
        C_EXTENSIONS ON
    )
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(${target} PRIVATE
            -Wall
            -Wextra
            -Wpedantic
            -Wno-missing-field-initializers
            -Wno-unused-function
            -Werror
        )
    endif()
    lc_configure_fatal_linker_warnings(${target})
endfunction()

function(lc_enable_coverage_if_requested target)
    get_target_property(target_type ${target} TYPE)
    if(LOCKDC_ENABLE_COVERAGE AND CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_definitions(${target} PRIVATE LOCKDC_COVERAGE_BUILD=1)
        target_compile_options(${target} PRIVATE --coverage -O0)
        if(NOT target_type STREQUAL "STATIC_LIBRARY")
            target_link_options(${target} PRIVATE --coverage)
        endif()
    endif()
endfunction()

function(lc_assert_fuzz_toolchain_supported)
    if(NOT DEFINED LOCKDC_AFLPP_ROOT OR LOCKDC_AFLPP_ROOT STREQUAL "")
        message(FATAL_ERROR "LOCKDC_BUILD_FUZZERS=ON requires the pinned AFL++ lifecycle toolchain")
    endif()
    if(NOT EXISTS "${LOCKDC_AFLPP_ROOT}/bin/afl-fuzz")
        message(FATAL_ERROR "Pinned AFL++ afl-fuzz is missing: ${LOCKDC_AFLPP_ROOT}/bin/afl-fuzz")
    endif()
endfunction()

find_program(CLANG_FORMAT_BIN NAMES clang-format)

file(GLOB_RECURSE LOCKDC_FORMAT_SOURCES CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/include/*.h
    ${CMAKE_CURRENT_SOURCE_DIR}/src/*.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/*.h
    ${CMAKE_CURRENT_SOURCE_DIR}/bench/*.c
    ${CMAKE_CURRENT_SOURCE_DIR}/bench/*.h
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.c
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.h
    ${CMAKE_CURRENT_SOURCE_DIR}/examples/*.c
    ${CMAKE_CURRENT_SOURCE_DIR}/examples/*.h
)

if(CLANG_FORMAT_BIN)
    add_custom_target(clang-format
        COMMAND ${CLANG_FORMAT_BIN} -i -style=file ${LOCKDC_FORMAT_SOURCES}
        VERBATIM
    )
else()
    add_custom_target(clang-format
        COMMAND ${CMAKE_COMMAND} -E echo "clang-format not found"
        COMMAND ${CMAKE_COMMAND} -E false
        VERBATIM
    )
endif()

add_custom_target(format DEPENDS clang-format)

set(lc_generated_include_dir "${CMAKE_CURRENT_BINARY_DIR}/generated/include")
file(MAKE_DIRECTORY "${lc_generated_include_dir}/lc")
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/lc_version.h.in"
    "${lc_generated_include_dir}/lc/version.h"
    @ONLY
)
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/lockdc.pc.in"
    "${CMAKE_CURRENT_BINARY_DIR}/lockdc.pc"
    @ONLY
)
set(LOCKDC_SYSTEM_LIBRARIES Threads::Threads ${CMAKE_DL_LIBS})
if(LOCKDC_ATOMIC_LIBRARY AND NOT LOCKDC_ATOMIC_LIBRARY MATCHES "-NOTFOUND$")
    list(APPEND LOCKDC_SYSTEM_LIBRARIES "${LOCKDC_ATOMIC_LIBRARY}")
endif()
if(NOT LOCKDC_COVERAGE_RUNTIME_LIBRARY STREQUAL "")
    list(APPEND LOCKDC_SYSTEM_LIBRARIES "${LOCKDC_COVERAGE_RUNTIME_LIBRARY}")
endif()
set(LOCKDC_PUBLIC_HEADER "${CMAKE_CURRENT_SOURCE_DIR}/include/lc/lc.h")
set(LOCKDC_PUBLIC_VERSION_HEADER "${lc_generated_include_dir}/lc/version.h")

include(LcDependencies)
lc_configure_dependencies()
lc_get_pslog_asset_info(LOCKDC_PSLOG_ASSET_NAME LOCKDC_PSLOG_ASSET_HASH)

set(lc_sources
    src/lc_api.c
    src/lc_allocator.c
    src/lc_attachment_api.c
    src/lc_client_api.c
    src/lc_client_management_api.c
    src/lc_consumer_service.c
    src/lc_core.c
    src/lc_intcompat.c
    src/lc_lease_api.c
    src/lc_lonejson.c
    src/lc_local_mutate_api.c
    src/lc_management_api.c
    src/lc_message_api.c
    src/lc_mutate_stream.c
    src/lc_pouch_control_migration.c
    src/lc_pouch_client.c
    src/lc_pouch.c
    src/lc_pouch_crypto.c
    src/lc_pouch_index.c
    src/lc_pouch_index_doc.c
    src/lc_pouch_index_posting.c
    src/lc_pouch_index_result.c
    src/lc_pouch_index_terms.c
    src/lc_pouch_query_index.c
    src/lc_pouch_namespace.c
    src/lc_pouch_path.c
    src/lc_pouch_record.c
    src/lc_pouch_state.c
    src/lc_queue_api.c
    src/lc_queue_stream_api.c
    src/lc_workflow.c
    src/lc_log.c
    src/lc_state_api.c
    src/lc_streaming.c
    src/lc_transport.c
    src/lc_xid.c
)

set(LOCKDC_PSLOG_STATIC_INCLUDE_DIR "${LOCKDC_EXTERNAL_ROOT}/pslog/install/include")
set(LOCKDC_PSLOG_SHARED_INCLUDE_DIR "${LOCKDC_EXTERNAL_ROOT}/pslog/install/include")
set(LOCKDC_LONEJSON_STATIC_INCLUDE_DIR "${LOCKDC_EXTERNAL_ROOT}/lonejson/install/include")
set(LOCKDC_LONEJSON_SHARED_INCLUDE_DIR "${LOCKDC_EXTERNAL_ROOT}/lonejson/install/include")
set(LOCKDC_LIBLQL_STATIC_INCLUDE_DIR "${LOCKDC_EXTERNAL_ROOT}/liblql/install/include")
set(LOCKDC_LIBLQL_SHARED_INCLUDE_DIR "${LOCKDC_EXTERNAL_ROOT}/liblql/install/include")
set(LOCKDC_PSLOG_INCLUDE_DIR "")
set(LOCKDC_LONEJSON_INCLUDE_DIR "")
set(LOCKDC_LIBLQL_INCLUDE_DIR "")
foreach(lockdc_pslog_include_candidate
    "${LOCKDC_PSLOG_STATIC_INCLUDE_DIR}"
    "${LOCKDC_PSLOG_SHARED_INCLUDE_DIR}"
)
    if(EXISTS "${lockdc_pslog_include_candidate}/pslog.h")
        set(LOCKDC_PSLOG_INCLUDE_DIR "${lockdc_pslog_include_candidate}")
        break()
    endif()
endforeach()
foreach(lockdc_liblql_include_candidate
    "${LOCKDC_LIBLQL_STATIC_INCLUDE_DIR}"
    "${LOCKDC_LIBLQL_SHARED_INCLUDE_DIR}"
)
    if(EXISTS "${lockdc_liblql_include_candidate}/lql/lql.h")
        set(LOCKDC_LIBLQL_INCLUDE_DIR "${lockdc_liblql_include_candidate}")
        break()
    endif()
endforeach()
foreach(lockdc_lonejson_include_candidate
    "${LOCKDC_LONEJSON_STATIC_INCLUDE_DIR}"
    "${LOCKDC_LONEJSON_SHARED_INCLUDE_DIR}"
)
    if(EXISTS "${lockdc_lonejson_include_candidate}/lonejson.h")
        set(LOCKDC_LONEJSON_INCLUDE_DIR "${lockdc_lonejson_include_candidate}")
        break()
    endif()
endforeach()
if(LOCKDC_PSLOG_INCLUDE_DIR STREQUAL "")
    if(LOCKDC_BUILD_SHARED AND NOT LOCKDC_BUILD_STATIC)
        set(LOCKDC_PSLOG_INCLUDE_DIR "${LOCKDC_PSLOG_SHARED_INCLUDE_DIR}")
    else()
        set(LOCKDC_PSLOG_INCLUDE_DIR "${LOCKDC_PSLOG_STATIC_INCLUDE_DIR}")
    endif()
endif()
if(LOCKDC_LONEJSON_INCLUDE_DIR STREQUAL "")
    if(LOCKDC_BUILD_SHARED AND NOT LOCKDC_BUILD_STATIC)
        set(LOCKDC_LONEJSON_INCLUDE_DIR "${LOCKDC_LONEJSON_SHARED_INCLUDE_DIR}")
    else()
        set(LOCKDC_LONEJSON_INCLUDE_DIR "${LOCKDC_LONEJSON_STATIC_INCLUDE_DIR}")
    endif()
endif()
if(LOCKDC_LIBLQL_INCLUDE_DIR STREQUAL "")
    if(LOCKDC_BUILD_SHARED AND NOT LOCKDC_BUILD_STATIC)
        set(LOCKDC_LIBLQL_INCLUDE_DIR "${LOCKDC_LIBLQL_SHARED_INCLUDE_DIR}")
    else()
        set(LOCKDC_LIBLQL_INCLUDE_DIR "${LOCKDC_LIBLQL_STATIC_INCLUDE_DIR}")
    endif()
endif()

function(lc_configure_library_target target)
    target_sources(${target} PRIVATE ${lc_sources})
    target_include_directories(${target}
        PUBLIC
            $<BUILD_INTERFACE:${lc_generated_include_dir}>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<BUILD_INTERFACE:${LOCKDC_PSLOG_INCLUDE_DIR}>
            $<BUILD_INTERFACE:${LOCKDC_LONEJSON_INCLUDE_DIR}>
            $<BUILD_INTERFACE:${LOCKDC_LIBLQL_INCLUDE_DIR}>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/src
    )
    target_compile_definitions(${target}
        PUBLIC
            LONEJSON_WITH_JWT
            LONEJSON_WITH_OIDC
        PRIVATE
            LC_VERSION_STRING="${LOCKDC_RESOLVED_VERSION}"
    )
    target_link_libraries(${target} PUBLIC ${LOCKDC_SYSTEM_LIBRARIES})
    lc_configure_c_target(${target})
    lc_enable_coverage_if_requested(${target})
endfunction()

function(lc_add_release_privacy_scan target)
    if(NOT CMAKE_BUILD_TYPE STREQUAL "Release")
        return()
    endif()
    if(LOCKDC_SANITIZER_INSTRUMENTED)
        return()
    endif()
    add_custom_command(TARGET ${target} POST_BUILD
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_STRIP_BIN=${CMAKE_STRIP}
            -DLOCKDC_STRIP_TARGET=$<TARGET_FILE:${target}>
            -DLOCKDC_TARGET_ID=${LOCKDC_TARGET_ID}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/strip_release_target.cmake
        VERBATIM
    )
    add_custom_command(TARGET ${target} POST_BUILD
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -DLOCKDC_SCAN_LABEL=$<TARGET_FILE_NAME:${target}>
            -DLOCKDC_SCAN_PATHS=$<TARGET_FILE:${target}>
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/release_privacy_scan.cmake
        VERBATIM
    )
endfunction()

function(lc_add_build_tree_shared_dependency_lookup target)
    if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux" AND NOT CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
        return()
    endif()
    foreach(lockdc_external_library_dir
        "${LOCKDC_EXTERNAL_ROOT}/curl/install/lib"
        "${LOCKDC_EXTERNAL_ROOT}/openssl/install/lib"
        "${LOCKDC_EXTERNAL_ROOT}/nghttp2/install/lib"
        "${LOCKDC_EXTERNAL_ROOT}/pslog/install/lib"
        "${LOCKDC_EXTERNAL_ROOT}/lonejson/install/lib"
        "${LOCKDC_EXTERNAL_ROOT}/liblql/install/lib"
        "${LOCKDC_EXTERNAL_ROOT}/libssh2/install/lib"
        "${LOCKDC_EXTERNAL_ROOT}/zlib/install/lib"
    )
        if(EXISTS "${lockdc_external_library_dir}")
            target_link_options(${target} INTERFACE
                "$<BUILD_INTERFACE:LINKER:-rpath-link,${lockdc_external_library_dir}>"
            )
        endif()
    endforeach()
endfunction()

if(LOCKDC_BUILD_STATIC)
    add_library(lc_static STATIC)
    add_library(lockdc::static ALIAS lc_static)
    lc_configure_library_target(lc_static)
    target_link_libraries(lc_static PUBLIC
        lc::curl_static
        lc::openssl_ssl_static
        lc::openssl_crypto_static
        lc::nghttp2_static
        lc::pslog_static
        lc::lonejson_static
        lc::liblql_static
    )
    set_target_properties(lc_static PROPERTIES
        OUTPUT_NAME lockdc
    )
    lc_add_release_privacy_scan(lc_static)
endif()

if(LOCKDC_BUILD_SHARED)
    add_library(lc_shared SHARED)
    add_library(lockdc::shared ALIAS lc_shared)
    lc_configure_library_target(lc_shared)
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        target_link_options(lc_shared PRIVATE "LINKER:--enable-new-dtags")
        set(lockdc_shared_install_rpath "$ORIGIN")
    elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
        set(lockdc_shared_install_rpath "@loader_path")
    else()
        set(lockdc_shared_install_rpath "")
    endif()
    target_link_libraries(lc_shared PRIVATE
        lc::curl_shared
        lc::openssl_ssl_shared
        lc::openssl_crypto_shared
        lc::nghttp2_shared
        lc::pslog_shared
        lc::lonejson_shared
        lc::liblql_shared
    )
    lc_add_build_tree_shared_dependency_lookup(lc_shared)
    set_target_properties(lc_shared PROPERTIES
        OUTPUT_NAME lockdc
        VERSION ${PROJECT_VERSION}
        SOVERSION ${LOCKDC_ABI_VERSION}
        BUILD_RPATH "${lockdc_shared_install_rpath}"
        BUILD_RPATH_USE_ORIGIN ON
        BUILD_WITH_INSTALL_RPATH ON
        INSTALL_RPATH "${lockdc_shared_install_rpath}"
        INSTALL_NAME_DIR "@rpath"
        INSTALL_RPATH_USE_LINK_PATH OFF
    )
    lc_add_release_privacy_scan(lc_shared)
endif()

if(LOCKDC_BUILD_TESTS)
    add_library(lc_test_static STATIC)
    lc_configure_library_target(lc_test_static)
    target_compile_definitions(lc_test_static PRIVATE LOCKDC_TEST_BUILD=1)
    target_link_libraries(lc_test_static PRIVATE
        lc::curl_static
        lc::openssl_ssl_static
        lc::openssl_crypto_static
        lc::nghttp2_static
        lc::pslog_static
        lc::lonejson_static
        lc::liblql_static
    )
    set_target_properties(lc_test_static PROPERTIES
        OUTPUT_NAME lockdc_test
    )
endif()

set(LOCKDC_SHARED_LIB_NAME "")
set(LOCKDC_SHARED_SONAME "")
set(LOCKDC_SHARED_LINK_NAME "")
set(LOCKDC_PACKAGE_METADATA_SHARED_LIB_NAME "")
set(LOCKDC_PACKAGE_METADATA_SHARED_SONAME "")
set(LOCKDC_PACKAGE_METADATA_SHARED_LINK_NAME "")
set(LOCKDC_ACTIVE_C_FLAGS "${CMAKE_C_FLAGS}")
set(LOCKDC_SANITIZER_INSTRUMENTED 0)

if(DEFINED CMAKE_BUILD_TYPE AND NOT "${CMAKE_BUILD_TYPE}" STREQUAL "")
    string(TOUPPER "${CMAKE_BUILD_TYPE}" LOCKDC_CMAKE_BUILD_TYPE_UPPER)
    string(APPEND LOCKDC_ACTIVE_C_FLAGS " ${CMAKE_C_FLAGS_${LOCKDC_CMAKE_BUILD_TYPE_UPPER}}")
endif()
if(LOCKDC_ACTIVE_C_FLAGS MATCHES "(^|[ \t])-fsanitize=([^ \t,]+,)*(address|undefined)(,[^ \t,]+)*($|[ \t])")
    set(LOCKDC_SANITIZER_INSTRUMENTED 1)
endif()

if(TARGET lc_shared)
    set(LOCKDC_SHARED_LINK_NAME "${CMAKE_SHARED_LIBRARY_PREFIX}lockdc${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(LOCKDC_PACKAGE_METADATA_SHARED_LIB_NAME "$<TARGET_FILE_NAME:lc_shared>")
    set(LOCKDC_PACKAGE_METADATA_SHARED_SONAME "$<TARGET_SONAME_FILE_NAME:lc_shared>")
    set(LOCKDC_PACKAGE_METADATA_SHARED_LINK_NAME "$<TARGET_LINKER_FILE_NAME:lc_shared>")
endif()

configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/lockdcConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/lockdcConfig.cmake"
    INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/lockdc"
)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/lockdcConfigVersion.cmake"
    VERSION "${PROJECT_VERSION}"
    COMPATIBILITY SameMajorVersion
)

file(READ "${CMAKE_CURRENT_SOURCE_DIR}/cmake/package_metadata.cmake.in" LOCKDC_PACKAGE_METADATA_TEMPLATE)
string(CONFIGURE "${LOCKDC_PACKAGE_METADATA_TEMPLATE}" LOCKDC_PACKAGE_METADATA_CONTENT @ONLY)
file(GENERATE
    OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/package-metadata.cmake"
    CONTENT "${LOCKDC_PACKAGE_METADATA_CONTENT}"
)

set(SOURCE_URL "git+https://github.com/sa6mwa/liblockdc.git")
set(SOURCE_TAG_LINE "  tag = \"v${LOCKDC_RESOLVED_VERSION}\",")
set(LOCKDC_LUA_ROCKSPEC_PATH "${CMAKE_CURRENT_BINARY_DIR}/lockdc-${PROJECT_VERSION}-1.rockspec")
file(GLOB LOCKDC_STALE_LUA_ROCKSPEC_PATHS "${CMAKE_CURRENT_BINARY_DIR}/lockdc-*-1.rockspec")
foreach(_lockdc_stale_lua_rockspec_path IN LISTS LOCKDC_STALE_LUA_ROCKSPEC_PATHS)
    if(NOT "${_lockdc_stale_lua_rockspec_path}" STREQUAL "${LOCKDC_LUA_ROCKSPEC_PATH}")
        file(REMOVE "${_lockdc_stale_lua_rockspec_path}")
    endif()
endforeach()
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/lockdc.rockspec.in"
    "${LOCKDC_LUA_ROCKSPEC_PATH}"
    @ONLY
)

set(LOCKDC_LUA_INCLUDE_DIR "")
set(LOCKDC_LUA_LIBRARY "")
if(LOCKDC_BUILD_LUA_BINDINGS)
    find_path(LOCKDC_LUA_INCLUDE_DIR
        NAMES lua.h
        PATH_SUFFIXES include include/lua5.5 include/lua-5.5 lua5.5 lua-5.5
        PATHS /usr/local /usr /usr/local/include /usr/include
    )
    if("${LOCKDC_LUA_INCLUDE_DIR}" STREQUAL "" AND EXISTS "/usr/local/include/lua.h")
        set(LOCKDC_LUA_INCLUDE_DIR "/usr/local/include")
    endif()
    if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
        find_library(LOCKDC_LUA_LIBRARY
            NAMES lua5.5 lua-5.5 lua55
            PATH_SUFFIXES lib lib64
            PATHS /usr/local /usr /opt/local
        )
    endif()
    if(LOCKDC_BUILD_SHARED AND NOT "${LOCKDC_LUA_INCLUDE_DIR}" STREQUAL "")
        if(CMAKE_SYSTEM_NAME STREQUAL "Linux" OR
           CMAKE_SYSTEM_NAME STREQUAL "Darwin" OR
           NOT "${LOCKDC_LUA_LIBRARY}" STREQUAL "")
            add_library(lockdc_lua_core MODULE src/lua/lockdc_lua.c)
            target_include_directories(lockdc_lua_core SYSTEM PRIVATE "${LOCKDC_LUA_INCLUDE_DIR}")
            target_include_directories(lockdc_lua_core PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src")
            target_link_libraries(lockdc_lua_core PRIVATE lc_shared)
            if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
                target_compile_options(lockdc_lua_core PRIVATE -Werror=long-long)
            endif()
            if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
                target_link_options(lockdc_lua_core PRIVATE "LINKER:--allow-shlib-undefined")
            elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
                target_link_options(lockdc_lua_core PRIVATE "LINKER:-undefined,dynamic_lookup")
            else()
                target_link_libraries(lockdc_lua_core PRIVATE "${LOCKDC_LUA_LIBRARY}")
            endif()
            lc_configure_c_target(lockdc_lua_core)
            set_target_properties(lockdc_lua_core PROPERTIES
                PREFIX ""
                OUTPUT_NAME core
                LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/lua/lockdc"
            )
        else()
            message(STATUS
                "Lua bindings requested but no Lua runtime library was found for ${CMAKE_SYSTEM_NAME}; "
                "skipping lockdc_lua_core")
        endif()
    endif()
endif()

if(LOCKDC_INSTALL)

    install(FILES ${LOCKDC_PUBLIC_HEADER} ${LOCKDC_PUBLIC_VERSION_HEADER}
        DESTINATION include/lc
        COMPONENT dev
    )
    if(TARGET lc_shared)
        install(TARGETS lc_shared

            LIBRARY DESTINATION lib
            COMPONENT runtime
            NAMELINK_COMPONENT dev
            RUNTIME DESTINATION bin
            COMPONENT runtime
        )
    endif()
    if(TARGET lc_static)
        install(TARGETS lc_static
            ARCHIVE DESTINATION lib
            COMPONENT dev
        )
    endif()
    install(FILES
        "${CMAKE_CURRENT_BINARY_DIR}/lockdc.pc"
        DESTINATION lib/pkgconfig
        COMPONENT dev
    )
    install(FILES
        "${CMAKE_CURRENT_BINARY_DIR}/lockdcConfig.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/lockdcConfigVersion.cmake"
        DESTINATION lib/cmake/lockdc
        COMPONENT dev
    )

    foreach(_lc_component runtime dev)
        install(FILES
            "${CMAKE_CURRENT_SOURCE_DIR}/README.md"
            "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE"
            DESTINATION share/doc/liblockdc
            COMPONENT "${_lc_component}"
        )
    endforeach()
endif()

if(LOCKDC_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if(LOCKDC_BUILD_BENCHMARKS)
    add_subdirectory(bench)
endif()

if(LOCKDC_BUILD_TESTS OR LOCKDC_BUILD_FUZZERS OR
   LOCKDC_BUILD_POUCH_INTEGRATION_FUZZERS)
    enable_testing()
endif()

macro(lc_apply_default_test_timeouts)
    get_property(lockdc_registered_tests DIRECTORY PROPERTY TESTS)
    foreach(lockdc_registered_test IN LISTS lockdc_registered_tests)
        get_property(lockdc_timeout_is_set
            TEST "${lockdc_registered_test}" PROPERTY TIMEOUT SET)
        if(NOT lockdc_timeout_is_set)
            set_tests_properties("${lockdc_registered_test}" PROPERTIES TIMEOUT 120)
        endif()
    endforeach()
endmacro()

if(LOCKDC_BUILD_TESTS)
    add_subdirectory(tests)

    if(TARGET lockdc_bench)
        add_test(NAME lockdc_bench_help COMMAND lockdc_bench --help)
        set_tests_properties(lockdc_bench_help PROPERTIES
            LABELS "local;smoke"
            TIMEOUT 10
            PASS_REGULAR_EXPRESSION "usage: .*lockdc_bench"
            FAIL_REGULAR_EXPRESSION "benchmark[ ]+iterations|failed during"
        )
        if(LOCKDC_C_TEST_ENVIRONMENT)
            set_tests_properties(lockdc_bench_help PROPERTIES
                ENVIRONMENT "${LOCKDC_C_TEST_ENVIRONMENT}")
        endif()

        add_test(NAME lockdc_bench_short_help COMMAND lockdc_bench -h)
        set_tests_properties(lockdc_bench_short_help PROPERTIES
            LABELS "local;smoke"
            TIMEOUT 10
            PASS_REGULAR_EXPRESSION "usage: .*lockdc_bench"
            FAIL_REGULAR_EXPRESSION "benchmark[ ]+iterations|failed during"
        )
        if(LOCKDC_C_TEST_ENVIRONMENT)
            set_tests_properties(lockdc_bench_short_help PROPERTIES
                ENVIRONMENT "${LOCKDC_C_TEST_ENVIRONMENT}")
        endif()
    endif()

    if(TARGET lc_shared AND TARGET lc_static)
        add_test(NAME package_archives_test
            COMMAND ${CMAKE_COMMAND}
                -DLOCKDC_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}
                -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
                -DLOCKDC_VERSION=${LOCKDC_RESOLVED_VERSION}
                -DLOCKDC_TARGET_ID=${LOCKDC_TARGET_ID}
                -DLOCKDC_DIST_DIR=${LOCKDC_DIST_DIR}
                -DLOCKDC_SHARED_LIB_NAME=$<TARGET_FILE_NAME:lc_shared>
                -DLOCKDC_SHARED_SONAME=$<TARGET_SONAME_FILE_NAME:lc_shared>
                -DLOCKDC_SHARED_LINK_NAME=$<TARGET_LINKER_FILE_NAME:lc_shared>
                -DLOCKDC_SANITIZER_INSTRUMENTED=${LOCKDC_SANITIZER_INSTRUMENTED}
                -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/package_archives_test.cmake
        )
        set_tests_properties(package_archives_test PROPERTIES RUN_SERIAL TRUE)
    endif()

    if(TARGET lc_shared AND TARGET lc_static
       AND NOT CMAKE_CROSSCOMPILING
       AND CMAKE_BUILD_TYPE STREQUAL "Release")
        add_test(NAME release_tarball_sdk_test
            COMMAND ${CMAKE_COMMAND}
                -DLOCKDC_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}
                -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
                -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/release_tarball_sdk_test.cmake
        )
        set_tests_properties(release_tarball_sdk_test PROPERTIES RUN_SERIAL TRUE)

        add_test(NAME lua_release_package_test
            COMMAND ${CMAKE_COMMAND}
                -DLOCKDC_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}
                -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
                -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/lua_release_package_test.cmake
        )
        set_tests_properties(lua_release_package_test PROPERTIES RUN_SERIAL TRUE)

        add_test(NAME release_checksums_stability_test
            COMMAND ${CMAKE_COMMAND}
                -DLOCKDC_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}
                -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
                -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/release_checksums_stability_test.cmake
        )
        set_tests_properties(release_checksums_stability_test PROPERTIES RUN_SERIAL TRUE)
    endif()

    add_test(NAME version_resolution_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/version_resolution_test.cmake
    )
    set_tests_properties(version_resolution_test PROPERTIES RUN_SERIAL TRUE)

    add_test(NAME dist_dir_configure_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/dist_dir_configure_test.cmake
    )
    set_tests_properties(dist_dir_configure_test PROPERTIES RUN_SERIAL TRUE)

    add_test(NAME c_only_configure_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -DLOCKDC_C_COMPILER=${CMAKE_C_COMPILER}
            -DLOCKDC_EXTERNAL_ROOT=${LOCKDC_EXTERNAL_ROOT}
            -DLOCKDC_DEPENDENCY_BUILD_ROOT=${LOCKDC_DEPENDENCY_BUILD_ROOT}
            -DLOCKDC_TARGET_ARCH=${LOCKDC_TARGET_ARCH}
            -DLOCKDC_TARGET_OS=${LOCKDC_TARGET_OS}
            -DLOCKDC_TARGET_LIBC=${LOCKDC_TARGET_LIBC}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/c_only_configure_test.cmake
    )
    set_tests_properties(c_only_configure_test PROPERTIES RUN_SERIAL TRUE)

    add_test(NAME deferred_vendored_install_manifest_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -DLOCKDC_C_COMPILER=${CMAKE_C_COMPILER}
            -DLOCKDC_EXTERNAL_ROOT=${LOCKDC_EXTERNAL_ROOT}
            -DLOCKDC_TARGET_ARCH=${LOCKDC_TARGET_ARCH}
            -DLOCKDC_TARGET_OS=${LOCKDC_TARGET_OS}
            -DLOCKDC_TARGET_LIBC=${LOCKDC_TARGET_LIBC}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/deferred_vendored_install_manifest_test.cmake
    )
    set_tests_properties(deferred_vendored_install_manifest_test PROPERTIES RUN_SERIAL TRUE)

    add_test(NAME arch_alias_configure_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -DLOCKDC_C_COMPILER=${CMAKE_C_COMPILER}
            -DLOCKDC_EXTERNAL_ROOT=${LOCKDC_EXTERNAL_ROOT}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/arch_alias_configure_test.cmake
    )
    set_tests_properties(arch_alias_configure_test PROPERTIES RUN_SERIAL TRUE)

    add_test(NAME cross_test_targeting_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/cross_test_targeting_test.cmake
    )
    set_tests_properties(cross_test_targeting_test PROPERTIES RUN_SERIAL TRUE)

    add_test(NAME runtime_license_install_tree_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -DLOCKDC_VERSION=${LOCKDC_RESOLVED_VERSION}
            -DLOCKDC_TARGET_ID=${LOCKDC_TARGET_ID}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/runtime_license_install_tree_test.cmake
    )
    set_tests_properties(runtime_license_install_tree_test PROPERTIES RUN_SERIAL TRUE)

    add_test(NAME install_tree_sdk_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -DLOCKDC_C_COMPILER=${CMAKE_C_COMPILER}
            -DLOCKDC_C_FLAGS_DEBUG=${CMAKE_C_FLAGS_DEBUG}
            -DLOCKDC_ACTIVE_C_FLAGS=${LOCKDC_ACTIVE_C_FLAGS}
            -DLOCKDC_EXTERNAL_ROOT=${LOCKDC_EXTERNAL_ROOT}
            $<$<BOOL:${CMAKE_BUILD_TYPE}>:-DLOCKDC_BUILD_TYPE=${CMAKE_BUILD_TYPE}>
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/install_tree_sdk_test.cmake
    )
    set_tests_properties(install_tree_sdk_test PROPERTIES RUN_SERIAL TRUE)

    add_test(NAME devenv_compose_config_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/devenv_compose_config_test.cmake
    )
    set_tests_properties(devenv_compose_config_test PROPERTIES RUN_SERIAL TRUE)

    add_test(NAME pouch_boundary_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/pouch_boundary_test.cmake
    )
    set_tests_properties(pouch_boundary_test PROPERTIES RUN_SERIAL TRUE)

    add_test(NAME pslog_build_modes_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -DLOCKDC_C_COMPILER=${CMAKE_C_COMPILER}
            $<$<BOOL:${CMAKE_TOOLCHAIN_FILE}>:-DLOCKDC_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}>
            -DLOCKDC_EXTERNAL_ROOT=${LOCKDC_EXTERNAL_ROOT}
            -DLOCKDC_DEPENDENCY_BUILD_ROOT=${LOCKDC_DEPENDENCY_BUILD_ROOT}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/pslog_build_modes_test.cmake
    )
    set_tests_properties(pslog_build_modes_test PROPERTIES RUN_SERIAL TRUE)

    add_test(NAME curl_dependency_interface_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -DLOCKDC_EXTERNAL_ROOT=${LOCKDC_EXTERNAL_ROOT}
            -DLOCKDC_DEPENDENCY_BUILD_ROOT=${LOCKDC_DEPENDENCY_BUILD_ROOT}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/curl_dependency_interface_test.cmake
    )
    set_tests_properties(curl_dependency_interface_test PROPERTIES RUN_SERIAL TRUE)

    add_test(NAME libssh2_dependency_interface_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_EXTERNAL_ROOT=${LOCKDC_EXTERNAL_ROOT}
            -DLOCKDC_DEPENDENCY_BUILD_ROOT=${LOCKDC_DEPENDENCY_BUILD_ROOT}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/libssh2_dependency_interface_test.cmake
    )
    set_tests_properties(libssh2_dependency_interface_test PROPERTIES RUN_SERIAL TRUE)

    add_test(NAME pslog_dependency_download_name_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/pslog_dependency_download_name_test.cmake
    )
    set_tests_properties(pslog_dependency_download_name_test PROPERTIES RUN_SERIAL TRUE)

    add_test(NAME host_dependency_root_layout_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/host_dependency_root_layout_test.cmake
    )
    set_tests_properties(host_dependency_root_layout_test PROPERTIES RUN_SERIAL TRUE)

    add_test(NAME dependency_download_timeout_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -DLOCKDC_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}
            -DLOCKDC_C_COMPILER=${CMAKE_C_COMPILER}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/dependency_download_timeout_test.cmake
    )
    set_tests_properties(dependency_download_timeout_test PROPERTIES RUN_SERIAL TRUE)

    add_test(NAME dependency_archive_cache_contract_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -DLOCKDC_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/dependency_archive_cache_contract_test.cmake
    )
    set_tests_properties(dependency_archive_cache_contract_test PROPERTIES
        LABELS "unit;local"
        RUN_SERIAL TRUE
        TIMEOUT 20)

    add_test(NAME clean_read_only_cache_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -DLOCKDC_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/clean_read_only_cache_test.cmake
    )
    set_tests_properties(clean_read_only_cache_test PROPERTIES
        LABELS "unit;local"
        RUN_SERIAL TRUE
        TIMEOUT 10)

    add_test(NAME generated_path_guard_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/generated_path_guard_test.cmake
    )
    set_tests_properties(generated_path_guard_test PROPERTIES
        LABELS "unit;local"
        RUN_SERIAL TRUE
        TIMEOUT 10)

    add_test(NAME dependency_prune_path_guard_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -DLOCKDC_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/dependency_prune_path_guard_test.cmake
    )
    set_tests_properties(dependency_prune_path_guard_test PROPERTIES
        LABELS "unit;local;packaging"
        RUN_SERIAL TRUE
        TIMEOUT 10)

    add_test(NAME ctest_timeout_contract_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/ctest_timeout_contract_test.cmake
    )
    set_tests_properties(ctest_timeout_contract_test PROPERTIES
        LABELS "unit;local"
        RUN_SERIAL TRUE
        TIMEOUT 10)

    add_test(NAME source_manifest_boundary_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -DLOCKDC_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/source_manifest_boundary_test.cmake
    )
    set_tests_properties(source_manifest_boundary_test PROPERTIES
        LABELS "unit;local;packaging"
        RUN_SERIAL TRUE
        TIMEOUT 10)

    add_test(NAME lifecycle_command_surface_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/lifecycle_command_surface_test.cmake
    )
    set_tests_properties(lifecycle_command_surface_test PROPERTIES
        LABELS "unit;local;lifecycle-host"
        RUN_SERIAL TRUE
        TIMEOUT 10)

    add_test(NAME lifecycle_preset_contract_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/lifecycle_preset_contract_test.cmake
    )
    set_tests_properties(lifecycle_preset_contract_test PROPERTIES
        LABELS "unit;local;lifecycle-host"
        RUN_SERIAL TRUE
        TIMEOUT 10)

    add_test(NAME discover_target_tools_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -DLOCKDC_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/discover_target_tools_test.cmake
    )
    set_tests_properties(discover_target_tools_test PROPERTIES
        LABELS "unit;local;packaging;lifecycle-host"
        RUN_SERIAL TRUE
        TIMEOUT 10)

    lc_apply_default_test_timeouts()

endif()

if(LOCKDC_BUILD_FUZZERS)
    lc_assert_fuzz_toolchain_supported()
endif()

if(LOCKDC_BUILD_FUZZERS OR LOCKDC_BUILD_POUCH_INTEGRATION_FUZZERS)
    add_subdirectory(tests/fuzz)
    lc_apply_default_test_timeouts()
endif()

if(CMAKE_C_COMPILER_ID STREQUAL "GNU")
    get_filename_component(_lockdc_gcc_dir "${CMAKE_C_COMPILER}" DIRECTORY)
    get_filename_component(_lockdc_gcc_name "${CMAKE_C_COMPILER}" NAME)
    string(REGEX REPLACE "gcc$" "gcov" _lockdc_gcov_name "${_lockdc_gcc_name}")
    if(NOT _lockdc_gcov_name STREQUAL _lockdc_gcc_name)
        find_program(LOCKDC_GCOV_BIN
            NAMES "${_lockdc_gcov_name}"
            HINTS "${_lockdc_gcc_dir}"
            NO_DEFAULT_PATH)
    endif()
endif()
if(NOT LOCKDC_GCOV_BIN)
    find_program(LOCKDC_GCOV_BIN NAMES gcov)
endif()

if(LOCKDC_BUILD_TESTS AND LOCKDC_ENABLE_COVERAGE AND LOCKDC_GCOV_BIN)
    add_custom_target(coverage-report
        COMMAND ${CMAKE_CTEST_COMMAND} --test-dir ${CMAKE_CURRENT_BINARY_DIR} --output-on-failure
        COMMAND ${CMAKE_COMMAND} -E env GCOV=${LOCKDC_GCOV_BIN}
                ${CMAKE_CURRENT_SOURCE_DIR}/scripts/coverage_report.sh
                ${CMAKE_CURRENT_BINARY_DIR}
                ${CMAKE_CURRENT_BINARY_DIR}/coverage-report
        DEPENDS lc_test_static
        USES_TERMINAL
        VERBATIM
    )
endif()

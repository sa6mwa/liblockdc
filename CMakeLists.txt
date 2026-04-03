cmake_minimum_required(VERSION 3.21)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(LcVersion)

lc_detect_version(LOCKDC_RESOLVED_VERSION)

project(lockdc VERSION ${LOCKDC_RESOLVED_VERSION} LANGUAGES C)

include(GNUInstallDirs)
include(CMakeDependentOption)
include(CMakePackageConfigHelpers)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
find_library(LOCKDC_ATOMIC_LIBRARY NAMES atomic)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(LOCKDC_BUILD_STATIC "Build the static lockdc library." ON)
option(LOCKDC_BUILD_SHARED "Build the shared lockdc library." ON)
option(LOCKDC_BUILD_DEPENDENCIES "Download and build OpenSSL, nghttp2, libcurl, and libssh2." OFF)
option(LOCKDC_BUILD_EXAMPLES "Build example programs." ON)
option(LOCKDC_BUILD_TESTS "Build unit tests." ON)
cmake_dependent_option(LOCKDC_BUILD_E2E_TESTS
    "Build end-to-end tests that target a real lockd environment."
    OFF
    "LOCKDC_BUILD_TESTS"
    OFF
)
option(LOCKDC_BUILD_BENCHMARKS "Build local benchmark targets." ON)
option(LOCKDC_BUILD_FUZZERS "Build clang/libFuzzer targets for parser and stream hardening." OFF)
option(LOCKDC_ENABLE_COVERAGE "Build test targets with gcov coverage instrumentation." OFF)
option(LOCKDC_INSTALL "Enable install rules." ON)

set(LOCKDC_OPENSSL_VERSION "3.6.1" CACHE STRING "Pinned OpenSSL release used for CMake-managed dependency builds.")
set(LOCKDC_CURL_VERSION "8.18.0" CACHE STRING "Pinned curl release used for CMake-managed dependency builds.")
set(LOCKDC_NGHTTP2_VERSION "1.68.0" CACHE STRING "Pinned nghttp2 release used to enable libcurl HTTP/2 support.")
set(LOCKDC_LIBSSH2_VERSION "1.11.1" CACHE STRING "Pinned libssh2 release used for SSH and SFTP support.")
set(LOCKDC_LONEJSON_VERSION "0.3.0" CACHE STRING "Pinned lonejson release used for mapped JSON load/save and internal JSON parsing helpers.")
set(LOCKDC_CMOCKA_VERSION "2.0.2" CACHE STRING "Pinned cmocka release used for unit tests.")
set(LOCKDC_PSLOG_VERSION "0.3.1" CACHE STRING "Pinned libpslog release used for SDK logging.")
set(LOCKDC_EXTERNAL_ROOT "${CMAKE_SOURCE_DIR}/.cache/deps/host-debug" CACHE PATH "Stable install root for downloaded third-party dependencies.")
set(LOCKDC_DEPENDENCY_BUILD_ROOT "${CMAKE_SOURCE_DIR}/.cache/deps-build/host-debug" CACHE PATH "Stable build root for downloaded third-party dependencies.")
set(LOCKDC_DIST_DIR "${CMAKE_BINARY_DIR}/dist" CACHE PATH "Output directory for generated package archives and checksum manifests.")
set(LOCKDC_VERSION_OVERRIDE "" CACHE STRING "Override the detected liblockdc package version with an explicit semantic version.")
set(LOCKDC_TARGET_ARCH "${CMAKE_SYSTEM_PROCESSOR}" CACHE STRING "Artifact architecture name")
set(LOCKDC_TARGET_OS "${CMAKE_SYSTEM_NAME}" CACHE STRING "Artifact operating system name")
set(LOCKDC_TARGET_LIBC "" CACHE STRING "Artifact libc suffix for Linux targets (gnu, musl, or empty)")

if(NOT LOCKDC_BUILD_STATIC AND NOT LOCKDC_BUILD_SHARED)
    message(FATAL_ERROR "Enable at least one of LOCKDC_BUILD_STATIC or LOCKDC_BUILD_SHARED.")
endif()

if(LOCKDC_BUILD_FUZZERS)
    if(NOT CMAKE_C_COMPILER_ID MATCHES "Clang")
        message(FATAL_ERROR "LOCKDC_BUILD_FUZZERS=ON requires Clang as the C compiler")
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
if(LOCKDC_TARGET_ARCH_LOWER STREQUAL "arm64")
    set(LOCKDC_TARGET_ARCH_LOWER "aarch64")
endif()
if(LOCKDC_TARGET_ARCH_LOWER STREQUAL "arm")
    set(LOCKDC_TARGET_ARCH_LOWER "armhf")
endif()
set(LOCKDC_TARGET_ARCH "${LOCKDC_TARGET_ARCH_LOWER}" CACHE STRING "Artifact architecture name" FORCE)

if(LOCKDC_TARGET_OS_LOWER STREQUAL "linux" AND NOT LOCKDC_TARGET_LIBC)
    set(LOCKDC_TARGET_LIBC "gnu")
endif()

if(LOCKDC_TARGET_OS_LOWER STREQUAL "darwin")
    set(LOCKDC_TARGET_ID "${LOCKDC_TARGET_ARCH_LOWER}-darwin")
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

function(lc_configure_c_target target)
    if(UNIX OR APPLE OR CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
        target_compile_definitions(${target} PRIVATE _POSIX_C_SOURCE=200809L)
    endif()
    set_target_properties(${target} PROPERTIES
        C_STANDARD 90
        C_STANDARD_REQUIRED ON
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
        )
    endif()
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

function(lc_enable_fuzz_instrumentation_if_requested target)
    get_target_property(target_type ${target} TYPE)
    if(LOCKDC_BUILD_FUZZERS AND CMAKE_C_COMPILER_ID MATCHES "Clang")
        target_compile_definitions(${target} PRIVATE FUZZING_BUILD_MODE_UNSAFE_FOR_PRODUCTION=1)
        target_compile_options(${target} PRIVATE
            -fsanitize=fuzzer-no-link,address,undefined
            -fno-omit-frame-pointer
            -g
        )
        if(NOT target_type STREQUAL "STATIC_LIBRARY")
            target_link_options(${target} PRIVATE -fsanitize=fuzzer-no-link,address,undefined)
        endif()
    endif()
endfunction()

function(lc_assert_fuzz_toolchain_supported)
    set(fuzz_probe_dir "${CMAKE_CURRENT_BINARY_DIR}/CMakeFiles/fuzz-toolchain-probe")
    set(fuzz_probe_source "${fuzz_probe_dir}/probe.c")
    set(fuzz_probe_binary "${fuzz_probe_dir}/probe")

    file(MAKE_DIRECTORY "${fuzz_probe_dir}")
    file(WRITE "${fuzz_probe_source}" [=[
int LLVMFuzzerTestOneInput(const unsigned char *data, unsigned long size) {
    (void)data;
    (void)size;
    return 0;
}
]=])

    execute_process(
        COMMAND "${CMAKE_C_COMPILER}"
            -v
            -fsanitize=fuzzer,address
            -fno-omit-frame-pointer
            "${fuzz_probe_source}"
            -o "${fuzz_probe_binary}"
        RESULT_VARIABLE fuzz_probe_result
        OUTPUT_VARIABLE fuzz_probe_stdout
        ERROR_VARIABLE fuzz_probe_stderr
    )

    if(NOT fuzz_probe_result EQUAL 0)
        message(FATAL_ERROR
            "LOCKDC_BUILD_FUZZERS=ON requires a C toolchain that can link -fsanitize=fuzzer,address. "
            "The selected compiler is '${CMAKE_C_COMPILER}'. Install the matching compiler-rt/libFuzzer runtime "
            "for this Clang toolchain or use a Clang toolchain that provides those libraries.\n"
            "clang output:\n${fuzz_probe_stdout}\n${fuzz_probe_stderr}")
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
configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/lockdcConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/lockdcConfig.cmake"
    INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/lockdc"
)

set(LOCKDC_SYSTEM_LIBRARIES Threads::Threads ${CMAKE_DL_LIBS})
if(LOCKDC_ATOMIC_LIBRARY)
    list(APPEND LOCKDC_SYSTEM_LIBRARIES "${LOCKDC_ATOMIC_LIBRARY}")
endif()
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/lockdcConfigVersion.cmake"
    VERSION "${PROJECT_VERSION}"
    COMPATIBILITY SameMajorVersion
)

set(LOCKDC_PUBLIC_HEADER "${CMAKE_CURRENT_SOURCE_DIR}/include/lc/lc.h")
set(LOCKDC_PUBLIC_VERSION_HEADER "${lc_generated_include_dir}/lc/version.h")

include(LcDependencies)
lc_configure_dependencies()

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
    src/lc_queue_api.c
    src/lc_queue_stream_api.c
    src/lc_log.c
    src/lc_state_api.c
    src/lc_streaming.c
    src/lc_transport.c
)

set(LOCKDC_PSLOG_STATIC_INCLUDE_DIR "${LOCKDC_EXTERNAL_ROOT}/pslog-static/install/include")
set(LOCKDC_PSLOG_SHARED_INCLUDE_DIR "${LOCKDC_EXTERNAL_ROOT}/pslog-shared/install/include")
set(LOCKDC_LONEJSON_STATIC_INCLUDE_DIR "${LOCKDC_EXTERNAL_ROOT}/lonejson-static/install/include")
set(LOCKDC_LONEJSON_SHARED_INCLUDE_DIR "${LOCKDC_EXTERNAL_ROOT}/lonejson-shared/install/include")
set(LOCKDC_PSLOG_INCLUDE_DIR "")
set(LOCKDC_LONEJSON_INCLUDE_DIR "")
foreach(lockdc_pslog_include_candidate
    "${LOCKDC_PSLOG_STATIC_INCLUDE_DIR}"
    "${LOCKDC_PSLOG_SHARED_INCLUDE_DIR}"
)
    if(EXISTS "${lockdc_pslog_include_candidate}/pslog.h")
        set(LOCKDC_PSLOG_INCLUDE_DIR "${lockdc_pslog_include_candidate}")
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

function(lc_configure_library_target target)
    target_sources(${target} PRIVATE ${lc_sources})
    target_include_directories(${target}
        PUBLIC
            $<BUILD_INTERFACE:${lc_generated_include_dir}>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<BUILD_INTERFACE:${LOCKDC_PSLOG_INCLUDE_DIR}>
            $<BUILD_INTERFACE:${LOCKDC_LONEJSON_INCLUDE_DIR}>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/src
    )
    target_compile_definitions(${target} PRIVATE LC_VERSION_STRING="${LOCKDC_RESOLVED_VERSION}")
    target_link_libraries(${target} PUBLIC ${LOCKDC_SYSTEM_LIBRARIES})
    lc_configure_c_target(${target})
    lc_enable_coverage_if_requested(${target})
    lc_enable_fuzz_instrumentation_if_requested(${target})
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
    )
    set_target_properties(lc_static PROPERTIES
        OUTPUT_NAME lockdc
    )
endif()

if(LOCKDC_BUILD_SHARED)
    add_library(lc_shared SHARED)
    add_library(lockdc::shared ALIAS lc_shared)
    lc_configure_library_target(lc_shared)
    target_link_options(lc_shared PRIVATE "LINKER:--enable-new-dtags")
    target_link_libraries(lc_shared PRIVATE
        lc::curl_shared
        lc::openssl_ssl_shared
        lc::openssl_crypto_shared
        lc::nghttp2_shared
        lc::pslog_shared
        lc::lonejson_shared
    )
    set_target_properties(lc_shared PROPERTIES
        OUTPUT_NAME lockdc
        VERSION ${PROJECT_VERSION}
        SOVERSION ${PROJECT_VERSION_MAJOR}
        INSTALL_RPATH "$ORIGIN"
        INSTALL_RPATH_USE_LINK_PATH OFF
    )
endif()

if(LOCKDC_BUILD_TESTS)
    add_library(lc_test_static STATIC)
    lc_configure_library_target(lc_test_static)
    target_link_libraries(lc_test_static PRIVATE
        lc::curl_static
        lc::openssl_ssl_static
        lc::openssl_crypto_static
        lc::nghttp2_static
        lc::pslog_static
        lc::lonejson_static
    )
    set_target_properties(lc_test_static PROPERTIES
        OUTPUT_NAME lockdc_test
    )
endif()

set(LOCKDC_SHARED_LIB_PATH "")
set(LOCKDC_SHARED_LIB_NAME "")
set(LOCKDC_SHARED_SONAME "")
set(LOCKDC_SHARED_LINK_NAME "")
set(LOCKDC_STATIC_LIB_PATH "")

if(TARGET lc_shared)
    set(LOCKDC_SHARED_LIB_PATH "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}lockdc${CMAKE_SHARED_LIBRARY_SUFFIX}.${PROJECT_VERSION}")
    set(LOCKDC_SHARED_LIB_NAME "${CMAKE_SHARED_LIBRARY_PREFIX}lockdc${CMAKE_SHARED_LIBRARY_SUFFIX}.${PROJECT_VERSION}")
    set(LOCKDC_SHARED_SONAME "${CMAKE_SHARED_LIBRARY_PREFIX}lockdc${CMAKE_SHARED_LIBRARY_SUFFIX}.${PROJECT_VERSION_MAJOR}")
    set(LOCKDC_SHARED_LINK_NAME "${CMAKE_SHARED_LIBRARY_PREFIX}lockdc${CMAKE_SHARED_LIBRARY_SUFFIX}")
endif()

if(TARGET lc_static)
    set(LOCKDC_STATIC_LIB_PATH "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}lockdc${CMAKE_STATIC_LIBRARY_SUFFIX}")
endif()

configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/package_metadata.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/package-metadata.cmake"
    @ONLY
)

if(LOCKDC_INSTALL)
    install(FILES ${LOCKDC_PUBLIC_HEADER} ${LOCKDC_PUBLIC_VERSION_HEADER} DESTINATION include/lc)
    if(EXISTS "${LOCKDC_PSLOG_INCLUDE_DIR}/pslog.h" AND EXISTS "${LOCKDC_PSLOG_INCLUDE_DIR}/pslog_version.h")
        install(FILES
            "${LOCKDC_PSLOG_INCLUDE_DIR}/pslog.h"
            "${LOCKDC_PSLOG_INCLUDE_DIR}/pslog_version.h"
            DESTINATION include
            COMPONENT dev
        )
    endif()
    if(TARGET lc_static AND EXISTS "${LOCKDC_EXTERNAL_ROOT}/pslog-static/install/lib/libpslog.a")
        install(FILES
            "${LOCKDC_EXTERNAL_ROOT}/pslog-static/install/lib/libpslog.a"
            DESTINATION lib
            COMPONENT dev
        )
    endif()
    if(TARGET lc_shared
       AND EXISTS "${LOCKDC_EXTERNAL_ROOT}/pslog-shared/install/lib/libpslog.so.0"
       AND EXISTS "${LOCKDC_EXTERNAL_ROOT}/pslog-shared/install/lib/libpslog.so.${LOCKDC_PSLOG_VERSION}")
        install(FILES
            "${LOCKDC_EXTERNAL_ROOT}/pslog-shared/install/lib/libpslog.so.0"
            "${LOCKDC_EXTERNAL_ROOT}/pslog-shared/install/lib/libpslog.so.${LOCKDC_PSLOG_VERSION}"
            DESTINATION lib
            COMPONENT runtime
        )
    endif()
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

if(LOCKDC_BUILD_TESTS OR LOCKDC_BUILD_FUZZERS)
    enable_testing()
endif()

if(LOCKDC_BUILD_TESTS)
    add_subdirectory(tests)

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
                -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/package_archives_test.cmake
        )
        set_tests_properties(package_archives_test PROPERTIES RUN_SERIAL TRUE)
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
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/c_only_configure_test.cmake
    )
    set_tests_properties(c_only_configure_test PROPERTIES RUN_SERIAL TRUE)

    add_test(NAME arch_alias_configure_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -DLOCKDC_C_COMPILER=${CMAKE_C_COMPILER}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/arch_alias_configure_test.cmake
    )
    set_tests_properties(arch_alias_configure_test PROPERTIES RUN_SERIAL TRUE)

    add_test(NAME package_script_targeting_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/package_script_targeting_test.cmake
    )
    set_tests_properties(package_script_targeting_test PROPERTIES RUN_SERIAL TRUE)

    add_test(NAME runtime_license_fallback_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/runtime_license_fallback_test.cmake
    )
    set_tests_properties(runtime_license_fallback_test PROPERTIES RUN_SERIAL TRUE)

    add_test(NAME devenv_compose_config_test
        COMMAND ${CMAKE_COMMAND}
            -DLOCKDC_ROOT=${CMAKE_CURRENT_SOURCE_DIR}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/devenv_compose_config_test.cmake
    )
    set_tests_properties(devenv_compose_config_test PROPERTIES RUN_SERIAL TRUE)

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
endif()

if(LOCKDC_BUILD_FUZZERS)
    lc_assert_fuzz_toolchain_supported()
    add_subdirectory(tests/fuzz)
endif()

find_program(GCOV_BIN NAMES gcov)

if(LOCKDC_BUILD_TESTS AND LOCKDC_ENABLE_COVERAGE AND GCOV_BIN)
    add_custom_target(coverage-report
        COMMAND ${CMAKE_CTEST_COMMAND} --test-dir ${CMAKE_CURRENT_BINARY_DIR} --output-on-failure
        COMMAND ${CMAKE_COMMAND} -E env GCOV=${GCOV_BIN}
                ${CMAKE_CURRENT_SOURCE_DIR}/scripts/coverage_report.sh
                ${CMAKE_CURRENT_BINARY_DIR}
                ${CMAKE_CURRENT_BINARY_DIR}/coverage-report
        DEPENDS lc_test_static
        USES_TERMINAL
        VERBATIM
    )
endif()

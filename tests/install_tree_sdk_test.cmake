if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

if(EXISTS "${LOCKDC_BINARY_DIR}/package-metadata.cmake")
    include("${LOCKDC_BINARY_DIR}/package-metadata.cmake")
endif()

if(NOT DEFINED LOCKDC_C_COMPILER OR LOCKDC_C_COMPILER STREQUAL "")
    message(FATAL_ERROR "LOCKDC_C_COMPILER is required")
endif()

if(NOT DEFINED LOCKDC_EXTERNAL_ROOT OR LOCKDC_EXTERNAL_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_EXTERNAL_ROOT is required")
endif()

include("${LOCKDC_ROOT}/tests/package_config_dependency_provenance.cmake")

set(test_root "${LOCKDC_BINARY_DIR}/install-tree-sdk-test")
set(install_prefix "${test_root}/prefix")
set(consumer_src_dir "${test_root}/consumer")
set(consumer_bin_dir "${test_root}/consumer-build")

file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY "${consumer_src_dir}" "${consumer_bin_dir}")

execute_process(
    COMMAND "${CMAKE_COMMAND}" --install "${LOCKDC_BINARY_DIR}" --prefix "${install_prefix}"
    RESULT_VARIABLE install_result
    OUTPUT_VARIABLE install_stdout
    ERROR_VARIABLE install_stderr
)
if(NOT install_result EQUAL 0)
    message(FATAL_ERROR
        "failed to install lockdc test prefix\n"
        "stdout:\n${install_stdout}\n"
        "stderr:\n${install_stderr}")
endif()

foreach(required_path
    "${install_prefix}/include/lc/lc.h"
    "${install_prefix}/include/lc/version.h"
    "${install_prefix}/lib/liblockdc.a"
    "${install_prefix}/lib/cmake/lockdc/lockdcConfig.cmake"
)
    if(NOT EXISTS "${required_path}")
        message(FATAL_ERROR "installed SDK is missing required artifact: ${required_path}")
    endif()
endforeach()

foreach(forbidden_path
    "${install_prefix}/include/lonejson.h"
    "${install_prefix}/include/lql"
    "${install_prefix}/include/pslog.h"
    "${install_prefix}/include/curl"
    "${install_prefix}/include/openssl"
    "${install_prefix}/include/nghttp2"
    "${install_prefix}/include/libssh2.h"
    "${install_prefix}/include/zlib.h"
    "${install_prefix}/lib/liblonejson.a"
    "${install_prefix}/lib/liblql.a"
    "${install_prefix}/lib/libpslog.a"
    "${install_prefix}/lib/libcurl.a"
    "${install_prefix}/lib/libssl.a"
    "${install_prefix}/lib/libcrypto.a"
    "${install_prefix}/lib/libnghttp2.a"
    "${install_prefix}/lib/libssh2.a"
    "${install_prefix}/lib/libz.a"
    "${install_prefix}/share/lua/5.5/lockdc/init.lua"
    "${install_prefix}/share/lockdc/luarocks"
    "${install_prefix}/lib/lua/5.5/lockdc"
)
    if(EXISTS "${forbidden_path}")
        message(FATAL_ERROR "installed C SDK unexpectedly includes Lua artifact: ${forbidden_path}")
    endif()
endforeach()

file(READ "${install_prefix}/lib/cmake/lockdc/lockdcConfig.cmake" lockdc_config_text)
file(READ "${install_prefix}/lib/pkgconfig/lockdc.pc" lockdc_pkgconfig_text)
foreach(forbidden_archive_name libssh2.a libssl.a libcrypto.a libz.a libcurl.a libnghttp2.a libpslog.a liblonejson.a liblql.a)
    string(FIND "${lockdc_config_text}" "${forbidden_archive_name}" forbidden_archive_index)
    if(NOT forbidden_archive_index EQUAL -1)
        message(FATAL_ERROR
            "installed static package config still references bundled dependency archive ${forbidden_archive_name}\n"
            "config:\n${lockdc_config_text}")
    endif()
endforeach()

if(lockdc_config_text MATCHES "atomic")
    string(FIND "${lockdc_pkgconfig_text}" "-latomic" lockdc_pkgconfig_atomic_index)
    if(lockdc_pkgconfig_atomic_index EQUAL -1)
        message(FATAL_ERROR
            "installed pkg-config metadata is missing -latomic even though the CMake package exports it\n"
            "pkg-config:\n${lockdc_pkgconfig_text}\n"
            "config:\n${lockdc_config_text}")
    endif()
endif()

string(REGEX MATCH "(^|\n)Requires: pslog lonejson libcurl libssh2 openssl libnghttp2 zlib liblql(\n|$)"
    lockdc_pkgconfig_public_requires_match "${lockdc_pkgconfig_text}")
if(NOT lockdc_pkgconfig_public_requires_match)
    message(FATAL_ERROR
        "installed pkg-config metadata must expose shared-link dependency packages through Requires\n"
        "pkg-config:\n${lockdc_pkgconfig_text}")
endif()

string(REGEX MATCH "(^|\n)Libs: ([^\n]*)" lockdc_pkgconfig_public_libs_line
    "${lockdc_pkgconfig_text}")
if(NOT lockdc_pkgconfig_public_libs_line)
    message(FATAL_ERROR
        "installed pkg-config metadata is missing Libs\n"
        "pkg-config:\n${lockdc_pkgconfig_text}")
endif()
foreach(forbidden_pkgconfig_public_lib
    -lpslog
    -llonejson
    -llql
    -lcurl
    -lssh2
    -lz
    -lssl
    -lcrypto
    -lnghttp2
    -lpthread
    -ldl
    -latomic)
    string(FIND "${lockdc_pkgconfig_public_libs_line}"
        "${forbidden_pkgconfig_public_lib}" forbidden_pkgconfig_public_lib_index)
    if(NOT forbidden_pkgconfig_public_lib_index EQUAL -1)
        message(FATAL_ERROR
            "installed pkg-config Libs overexposes '${forbidden_pkgconfig_public_lib}'\n"
            "Libs line: ${lockdc_pkgconfig_public_libs_line}\n"
            "pkg-config:\n${lockdc_pkgconfig_text}")
    endif()
endforeach()

string(REGEX MATCH "(^|\n)Libs\\.private: ([^\n]*)"
    lockdc_pkgconfig_private_libs_line "${lockdc_pkgconfig_text}")
foreach(forbidden_pkgconfig_private_lib
    -lpslog
    -llonejson
    -llql
    -lcurl
    -lssh2
    -lz
    -lssl
    -lcrypto
    -lnghttp2)
    string(FIND "${lockdc_pkgconfig_private_libs_line}"
        "${forbidden_pkgconfig_private_lib}" forbidden_pkgconfig_private_lib_index)
    if(NOT forbidden_pkgconfig_private_lib_index EQUAL -1)
        message(FATAL_ERROR
            "installed pkg-config Libs.private must not duplicate dependency packages represented by Requires: '${forbidden_pkgconfig_private_lib}'\n"
            "Libs.private line: ${lockdc_pkgconfig_private_libs_line}\n"
            "pkg-config:\n${lockdc_pkgconfig_text}")
    endif()
endforeach()

lockdc_assert_config_accepts_non_cpkt_curl(
    "${install_prefix}"
    "${LOCKDC_EXTERNAL_ROOT}"
    "${test_root}")
lockdc_assert_config_rejects_missing_curl_target(
    "${install_prefix}"
    "${LOCKDC_EXTERNAL_ROOT}"
    "${test_root}")
lockdc_assert_config_accepts_shared_only_dependency_targets(
    "${install_prefix}"
    "${LOCKDC_EXTERNAL_ROOT}"
    "${test_root}")
lockdc_assert_config_supports_repeated_required_discovery(
    "${install_prefix}"
    "${LOCKDC_EXTERNAL_ROOT}"
    "${test_root}")
lockdc_assert_config_rejects_static_component_without_static_dependency_targets(
    "${install_prefix}"
    "${LOCKDC_EXTERNAL_ROOT}"
    "${test_root}")
lockdc_assert_config_accepts_optional_static_component_without_static_dependency_targets(
    "${install_prefix}"
    "${LOCKDC_EXTERNAL_ROOT}"
    "${test_root}")
lockdc_assert_config_accepts_optional_shared_component_without_shared_library(
    "${install_prefix}"
    "${LOCKDC_EXTERNAL_ROOT}"
    "${test_root}")
lockdc_assert_config_supports_quiet_optional_discovery(
    "${install_prefix}"
    "${LOCKDC_EXTERNAL_ROOT}"
    "${test_root}")

file(WRITE "${consumer_src_dir}/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.21)
project(lockdc_install_tree_consumer C)

find_package(lockdc CONFIG REQUIRED)

function(lockdc_assert_no_raw_dependency_links target_name)
    get_target_property(link_items "${target_name}" INTERFACE_LINK_LIBRARIES)
    foreach(link_item IN LISTS link_items)
        if(link_item MATCHES "^-l(curl|pslog|nghttp2|ssh2|ssl|crypto|z|lonejson|lql)$" OR
           link_item MATCHES "^(curl|pslog|nghttp2|ssh2|ssl|crypto|z|lonejson|lql)$")
            message(FATAL_ERROR
                "${target_name} exposes raw dependency link item '${link_item}' instead of an imported CMake target")
        endif()
    endforeach()
endfunction()

function(lockdc_assert_target_links target_name)
    get_target_property(link_items "${target_name}" INTERFACE_LINK_LIBRARIES)
    foreach(expected_item IN LISTS ARGN)
        list(FIND link_items "${expected_item}" expected_index)
        if(expected_index EQUAL -1)
            message(FATAL_ERROR
                "${target_name} is missing expected dependency target '${expected_item}'\n"
                "INTERFACE_LINK_LIBRARIES=${link_items}")
        endif()
    endforeach()
endfunction()

if(TARGET lockdc::static)
    lockdc_assert_no_raw_dependency_links(lockdc::static)
    lockdc_assert_target_links(lockdc::static
        CURL::libcurl
        OpenSSL::SSL
        OpenSSL::Crypto
        nghttp2::nghttp2
        Libssh2::libssh2
        ZLIB::ZLIB
        pslog::pslog_static
        lonejson::lonejson_static
        liblql::lql_static)
endif()

if(TARGET lockdc::shared)
    lockdc_assert_no_raw_dependency_links(lockdc::shared)
    lockdc_assert_target_links(lockdc::shared
        cpkt::curl_shared
        cpkt::openssl_ssl_shared
        cpkt::openssl_crypto_shared
        cpkt::nghttp2_shared
        cpkt::libssh2_shared
        cpkt::zlib_shared
        pslog::pslog_shared
        lonejson::lonejson
        liblql::lql_shared)
endif()

add_executable(lockdc_install_tree_consumer_static main.c)
target_link_libraries(lockdc_install_tree_consumer_static PRIVATE lockdc::static)

add_executable(lockdc_install_tree_consumer_shared main.c)
target_link_libraries(lockdc_install_tree_consumer_shared PRIVATE lockdc::shared)
]=])

file(WRITE "${consumer_src_dir}/main.c" [=[
#include <lc/lc.h>

int main(void) {
    lc_client_config config;
    lonejson_int64 value;

    value = 0;
    lc_client_config_init(&config);
    return value == 0 && lc_version_string() != 0 ? 0 : 1;
}
]=])

file(WRITE "${consumer_src_dir}/pkgconfig_static_main.c" [=[
#include <lc/lc.h>

int main(void) {
    lonejson_int64 value;

    value = 0;
    return lc_version_string() != 0 && value == 0 ? 0 : 1;
}
]=])

file(WRITE "${consumer_src_dir}/pkgconfig_shared_main.c" [=[
#include <lc/lc.h>

int main(void) {
    return lc_version_string() != 0 ? 0 : 1;
}
]=])

set(lockdc_consumer_prefix_path
    "${install_prefix}"
    "${LOCKDC_EXTERNAL_ROOT}/c.pkt.systems/install"
    "${LOCKDC_EXTERNAL_ROOT}/pslog/install"
    "${LOCKDC_EXTERNAL_ROOT}/lonejson/install"
    "${LOCKDC_EXTERNAL_ROOT}/liblql/install")
string(REPLACE ";" "\\;" lockdc_consumer_prefix_path_arg "${lockdc_consumer_prefix_path}")

set(lockdc_consumer_configure_command
    "${CMAKE_COMMAND}"
    -S "${consumer_src_dir}"
    -B "${consumer_bin_dir}"
    "-DCMAKE_C_COMPILER=${LOCKDC_C_COMPILER}"
    "-DCMAKE_C_FLAGS_DEBUG=${LOCKDC_C_FLAGS_DEBUG}"
    "-DCMAKE_PREFIX_PATH=${lockdc_consumer_prefix_path_arg}"
    "-DCURL_DIR=${LOCKDC_EXTERNAL_ROOT}/c.pkt.systems/install/lib/cmake/CURL"
    "-DOpenSSL_DIR=${LOCKDC_EXTERNAL_ROOT}/c.pkt.systems/install/lib/cmake/OpenSSL"
    "-DZLIB_DIR=${LOCKDC_EXTERNAL_ROOT}/c.pkt.systems/install/lib/cmake/zlib"
    "-Dnghttp2_DIR=${LOCKDC_EXTERNAL_ROOT}/c.pkt.systems/install/lib/cmake/nghttp2"
    "-DLibssh2_DIR=${LOCKDC_EXTERNAL_ROOT}/c.pkt.systems/install/lib/cmake/libssh2"
    "-Dpslog_DIR=${LOCKDC_EXTERNAL_ROOT}/pslog/install/lib/cmake/pslog"
    "-Dlonejson_DIR=${LOCKDC_EXTERNAL_ROOT}/lonejson/install/lib/cmake/lonejson"
    "-Dliblql_DIR=${LOCKDC_EXTERNAL_ROOT}/liblql/install/lib/cmake/liblql"
)
if(DEFINED LOCKDC_BUILD_TYPE AND NOT LOCKDC_BUILD_TYPE STREQUAL "")
    list(APPEND lockdc_consumer_configure_command
        "-DCMAKE_BUILD_TYPE=${LOCKDC_BUILD_TYPE}"
    )
endif()

execute_process(
    COMMAND ${lockdc_consumer_configure_command}
    RESULT_VARIABLE configure_result
    OUTPUT_VARIABLE configure_stdout
    ERROR_VARIABLE configure_stderr
)
if(NOT configure_result EQUAL 0)
    message(FATAL_ERROR
        "failed to configure install-tree consumer\n"
        "stdout:\n${configure_stdout}\n"
        "stderr:\n${configure_stderr}")
endif()

find_program(LOCKDC_PKG_CONFIG_BIN NAMES pkg-config)
find_program(LOCKDC_FILE_BIN NAMES file)
if(NOT LOCKDC_PKG_CONFIG_BIN)
    message(FATAL_ERROR "pkg-config is required for install-tree static SDK validation")
endif()

# Direct pkg-config consumers bypass CMake, so preserve the configured flags
# required to compile and link against an instrumented installed library.
set(lockdc_consumer_compile_and_link_flags "")
if(DEFINED LOCKDC_ACTIVE_C_FLAGS AND NOT LOCKDC_ACTIVE_C_FLAGS STREQUAL "")
    separate_arguments(lockdc_consumer_compile_and_link_flags NATIVE_COMMAND
        "${LOCKDC_ACTIVE_C_FLAGS}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" --build "${consumer_bin_dir}"
    RESULT_VARIABLE build_result
    OUTPUT_VARIABLE build_stdout
    ERROR_VARIABLE build_stderr
)
if(NOT build_result EQUAL 0)
    message(FATAL_ERROR
        "failed to build install-tree consumer\n"
        "stdout:\n${build_stdout}\n"
        "stderr:\n${build_stderr}")
endif()

set(lockdc_pkgconfig_path_entries
    "${install_prefix}/lib/pkgconfig"
    "${LOCKDC_EXTERNAL_ROOT}/c.pkt.systems/install/lib/pkgconfig"
    "${LOCKDC_EXTERNAL_ROOT}/pslog/install/lib/pkgconfig"
    "${LOCKDC_EXTERNAL_ROOT}/lonejson/install/lib/pkgconfig"
    "${LOCKDC_EXTERNAL_ROOT}/liblql/install/lib/pkgconfig")
string(JOIN ":" lockdc_pkgconfig_path ${lockdc_pkgconfig_path_entries})

set(lockdc_pkgconfig_shared_consumer "${consumer_bin_dir}/lockdc_install_tree_pkgconfig_shared")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        "PKG_CONFIG_PATH=${lockdc_pkgconfig_path}"
        "${LOCKDC_PKG_CONFIG_BIN}" --cflags lockdc
    RESULT_VARIABLE lockdc_pkgconfig_shared_cflags_result
    OUTPUT_VARIABLE lockdc_pkgconfig_shared_cflags
    ERROR_VARIABLE lockdc_pkgconfig_shared_cflags_stderr
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
if(NOT lockdc_pkgconfig_shared_cflags_result EQUAL 0)
    message(FATAL_ERROR
        "failed to resolve pkg-config cflags for shared install-tree consumer\n"
        "stdout:\n${lockdc_pkgconfig_shared_cflags}\n"
        "stderr:\n${lockdc_pkgconfig_shared_cflags_stderr}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        "PKG_CONFIG_PATH=${lockdc_pkgconfig_path}"
        "${LOCKDC_PKG_CONFIG_BIN}" --libs lockdc
    RESULT_VARIABLE lockdc_pkgconfig_shared_libs_result
    OUTPUT_VARIABLE lockdc_pkgconfig_shared_libs
    ERROR_VARIABLE lockdc_pkgconfig_shared_libs_stderr
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
if(NOT lockdc_pkgconfig_shared_libs_result EQUAL 0)
    message(FATAL_ERROR
        "failed to resolve pkg-config libs for shared install-tree consumer\n"
        "stdout:\n${lockdc_pkgconfig_shared_libs}\n"
        "stderr:\n${lockdc_pkgconfig_shared_libs_stderr}")
endif()

foreach(expected_pkgconfig_shared_lib
    -llockdc
    -lpslog
    -llonejson
    -lcurl
    -lssh2
    -lssl
    -lcrypto
    -lnghttp2
    -lz
    -llql)
    string(FIND "${lockdc_pkgconfig_shared_libs}"
        "${expected_pkgconfig_shared_lib}" expected_pkgconfig_shared_lib_index)
    if(expected_pkgconfig_shared_lib_index EQUAL -1)
        message(FATAL_ERROR
            "pkg-config --libs lockdc is missing shared-link dependency '${expected_pkgconfig_shared_lib}'\n"
            "libs: ${lockdc_pkgconfig_shared_libs}")
    endif()
endforeach()

separate_arguments(lockdc_pkgconfig_shared_cflags_list UNIX_COMMAND "${lockdc_pkgconfig_shared_cflags}")
separate_arguments(lockdc_pkgconfig_shared_libs_list UNIX_COMMAND "${lockdc_pkgconfig_shared_libs}")

execute_process(
    COMMAND "${LOCKDC_C_COMPILER}"
        ${lockdc_consumer_compile_and_link_flags}
        ${lockdc_pkgconfig_shared_cflags_list}
        "${consumer_src_dir}/pkgconfig_shared_main.c"
        -Wl,-rpath,${install_prefix}/lib
        -o "${lockdc_pkgconfig_shared_consumer}"
        ${lockdc_pkgconfig_shared_libs_list}
    RESULT_VARIABLE lockdc_pkgconfig_shared_build_result
    OUTPUT_VARIABLE lockdc_pkgconfig_shared_build_stdout
    ERROR_VARIABLE lockdc_pkgconfig_shared_build_stderr
)
if(NOT lockdc_pkgconfig_shared_build_result EQUAL 0)
    message(FATAL_ERROR
        "failed to build install-tree pkg-config shared consumer\n"
        "cflags: ${lockdc_pkgconfig_shared_cflags}\n"
        "libs: ${lockdc_pkgconfig_shared_libs}\n"
        "stdout:\n${lockdc_pkgconfig_shared_build_stdout}\n"
        "stderr:\n${lockdc_pkgconfig_shared_build_stderr}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        "PKG_CONFIG_PATH=${lockdc_pkgconfig_path}"
        "${LOCKDC_PKG_CONFIG_BIN}" --static --cflags lockdc
    RESULT_VARIABLE lockdc_pkgconfig_cflags_result
    OUTPUT_VARIABLE lockdc_pkgconfig_cflags
    ERROR_VARIABLE lockdc_pkgconfig_cflags_stderr
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
if(NOT lockdc_pkgconfig_cflags_result EQUAL 0)
    message(FATAL_ERROR
        "failed to resolve pkg-config cflags for static install-tree consumer\n"
        "stdout:\n${lockdc_pkgconfig_cflags}\n"
        "stderr:\n${lockdc_pkgconfig_cflags_stderr}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        "PKG_CONFIG_PATH=${lockdc_pkgconfig_path}"
        "${LOCKDC_PKG_CONFIG_BIN}" --static --libs lockdc
    RESULT_VARIABLE lockdc_pkgconfig_libs_result
    OUTPUT_VARIABLE lockdc_pkgconfig_libs
    ERROR_VARIABLE lockdc_pkgconfig_libs_stderr
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
if(NOT lockdc_pkgconfig_libs_result EQUAL 0)
    message(FATAL_ERROR
        "failed to resolve pkg-config libs for static install-tree consumer\n"
        "stdout:\n${lockdc_pkgconfig_libs}\n"
        "stderr:\n${lockdc_pkgconfig_libs_stderr}")
endif()

separate_arguments(lockdc_pkgconfig_cflags_list UNIX_COMMAND "${lockdc_pkgconfig_cflags}")
separate_arguments(lockdc_pkgconfig_libs_list UNIX_COMMAND "${lockdc_pkgconfig_libs}")

if(NOT DEFINED LOCKDC_SANITIZER_INSTRUMENTED OR LOCKDC_SANITIZER_INSTRUMENTED STREQUAL "" OR
   LOCKDC_SANITIZER_INSTRUMENTED STREQUAL "0")
    set(lockdc_pkgconfig_static_consumer "${consumer_bin_dir}/lockdc_install_tree_pkgconfig_static")
    execute_process(
        COMMAND "${LOCKDC_C_COMPILER}"
            ${lockdc_consumer_compile_and_link_flags}
            ${lockdc_pkgconfig_cflags_list}
            -static
            "${consumer_src_dir}/pkgconfig_static_main.c"
            -o "${lockdc_pkgconfig_static_consumer}"
            ${lockdc_pkgconfig_libs_list}
        RESULT_VARIABLE lockdc_pkgconfig_build_result
        OUTPUT_VARIABLE lockdc_pkgconfig_build_stdout
        ERROR_VARIABLE lockdc_pkgconfig_build_stderr
    )
    if(NOT lockdc_pkgconfig_build_result EQUAL 0)
        message(FATAL_ERROR
            "failed to build install-tree pkg-config static consumer\n"
            "cflags: ${lockdc_pkgconfig_cflags}\n"
            "libs: ${lockdc_pkgconfig_libs}\n"
            "stdout:\n${lockdc_pkgconfig_build_stdout}\n"
            "stderr:\n${lockdc_pkgconfig_build_stderr}")
    endif()

    if(DEFINED LOCKDC_TARGET_ID AND LOCKDC_TARGET_ID MATCHES "musl" AND LOCKDC_FILE_BIN)
        execute_process(
            COMMAND "${LOCKDC_FILE_BIN}" "${lockdc_pkgconfig_static_consumer}"
            RESULT_VARIABLE lockdc_pkgconfig_file_result
            OUTPUT_VARIABLE lockdc_pkgconfig_file_output
            ERROR_VARIABLE lockdc_pkgconfig_file_stderr
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        if(NOT lockdc_pkgconfig_file_result EQUAL 0)
            message(FATAL_ERROR
                "failed to inspect install-tree pkg-config static consumer\n"
                "stdout:\n${lockdc_pkgconfig_file_output}\n"
                "stderr:\n${lockdc_pkgconfig_file_stderr}")
        endif()
        if(NOT lockdc_pkgconfig_file_output MATCHES "statically linked")
            message(FATAL_ERROR
                "musl install-tree pkg-config static consumer is not fully static\n"
                "file:\n${lockdc_pkgconfig_file_output}")
        endif()
    endif()
endif()

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
    "${install_prefix}/include/lonejson.h"
    "${install_prefix}/lib/liblockdc.a"
    "${install_prefix}/lib/liblonejson.a"
    "${install_prefix}/lib/cmake/lockdc/lockdcConfig.cmake"
)
    if(NOT EXISTS "${required_path}")
        message(FATAL_ERROR "installed SDK is missing required artifact: ${required_path}")
    endif()
endforeach()

foreach(forbidden_path
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
string(FIND "${lockdc_config_text}" "libssh2.a" lockdc_ssh2_index)
string(FIND "${lockdc_config_text}" "libssl.a" lockdc_ssl_index)
string(FIND "${lockdc_config_text}" "libcrypto.a" lockdc_crypto_index)
string(FIND "${lockdc_config_text}" "libz.a" lockdc_z_index)

if(lockdc_ssh2_index EQUAL -1 OR lockdc_ssl_index EQUAL -1 OR
   lockdc_crypto_index EQUAL -1 OR lockdc_z_index EQUAL -1)
    message(FATAL_ERROR
        "installed static package config is missing expected static dependency archives\n"
        "config:\n${lockdc_config_text}")
endif()

if(lockdc_ssh2_index GREATER lockdc_ssl_index OR
   lockdc_ssh2_index GREATER lockdc_crypto_index OR
   lockdc_ssh2_index GREATER lockdc_z_index)
    message(FATAL_ERROR
        "installed static package config exports libssh2 after one of its dependencies\n"
        "config:\n${lockdc_config_text}")
endif()

if(lockdc_config_text MATCHES "atomic")
    string(FIND "${lockdc_pkgconfig_text}" "-latomic" lockdc_pkgconfig_atomic_index)
    if(lockdc_pkgconfig_atomic_index EQUAL -1)
        message(FATAL_ERROR
            "installed pkg-config metadata is missing -latomic even though the CMake package exports it\n"
            "pkg-config:\n${lockdc_pkgconfig_text}\n"
            "config:\n${lockdc_config_text}")
    endif()
endif()

file(WRITE "${consumer_src_dir}/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.21)
project(lockdc_install_tree_consumer C)

find_package(lockdc CONFIG REQUIRED)

add_executable(lockdc_install_tree_consumer main.c)
target_link_libraries(lockdc_install_tree_consumer PRIVATE lockdc::static)
]=])

file(WRITE "${consumer_src_dir}/main.c" [=[
#include <lc/lc.h>

int main(void) {
    lc_client_config config;
    lonejson_int64 value;

    value = 0;
    lc_client_config_init(&config);
    return value == 0 ? 0 : 1;
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

set(lockdc_consumer_configure_command
    "${CMAKE_COMMAND}"
    -S "${consumer_src_dir}"
    -B "${consumer_bin_dir}"
    "-DCMAKE_C_COMPILER=${LOCKDC_C_COMPILER}"
    "-DCMAKE_C_FLAGS_DEBUG=${LOCKDC_C_FLAGS_DEBUG}"
    "-DCMAKE_PREFIX_PATH=${install_prefix}"
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

find_program(LOCKDC_PKG_CONFIG_BIN NAMES pkg-config)
find_program(LOCKDC_FILE_BIN NAMES file)
if(NOT LOCKDC_PKG_CONFIG_BIN)
    message(FATAL_ERROR "pkg-config is required for install-tree static SDK validation")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        "PKG_CONFIG_PATH=${install_prefix}/lib/pkgconfig"
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
        "PKG_CONFIG_PATH=${install_prefix}/lib/pkgconfig"
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

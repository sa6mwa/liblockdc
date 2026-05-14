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
    "${install_prefix}/include/pslog.h"
    "${install_prefix}/include/curl"
    "${install_prefix}/include/openssl"
    "${install_prefix}/include/nghttp2"
    "${install_prefix}/include/libssh2.h"
    "${install_prefix}/include/zlib.h"
    "${install_prefix}/lib/liblonejson.a"
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
foreach(forbidden_archive_name libssh2.a libssl.a libcrypto.a libz.a libcurl.a libnghttp2.a libpslog.a liblonejson.a)
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

file(WRITE "${consumer_src_dir}/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.21)
project(lockdc_install_tree_consumer C)

find_package(lockdc CONFIG REQUIRED)

set(LOCKDC_EXTERNAL_ROOT "${LOCKDC_EXTERNAL_ROOT}")
set(LOCKDC_EXTERNAL_INCLUDE_DIRS
    "${LOCKDC_EXTERNAL_ROOT}/curl/install/include"
    "${LOCKDC_EXTERNAL_ROOT}/openssl/install/include"
    "${LOCKDC_EXTERNAL_ROOT}/nghttp2/install/include"
    "${LOCKDC_EXTERNAL_ROOT}/pslog/install/include"
    "${LOCKDC_EXTERNAL_ROOT}/lonejson/install/include"
    "${LOCKDC_EXTERNAL_ROOT}/libssh2/install/include"
    "${LOCKDC_EXTERNAL_ROOT}/zlib/install/include")
set(LOCKDC_EXTERNAL_LIBRARY_DIRS
    "${LOCKDC_EXTERNAL_ROOT}/curl/install/lib"
    "${LOCKDC_EXTERNAL_ROOT}/openssl/install/lib"
    "${LOCKDC_EXTERNAL_ROOT}/nghttp2/install/lib"
    "${LOCKDC_EXTERNAL_ROOT}/pslog/install/lib"
    "${LOCKDC_EXTERNAL_ROOT}/lonejson/install/lib"
    "${LOCKDC_EXTERNAL_ROOT}/libssh2/install/lib"
    "${LOCKDC_EXTERNAL_ROOT}/zlib/install/lib")

add_executable(lockdc_install_tree_consumer_static main.c)
target_include_directories(lockdc_install_tree_consumer_static PRIVATE ${LOCKDC_EXTERNAL_INCLUDE_DIRS})
target_link_directories(lockdc_install_tree_consumer_static PRIVATE ${LOCKDC_EXTERNAL_LIBRARY_DIRS})
target_link_libraries(lockdc_install_tree_consumer_static PRIVATE lockdc::static)

add_executable(lockdc_install_tree_consumer_shared main.c)
target_include_directories(lockdc_install_tree_consumer_shared PRIVATE ${LOCKDC_EXTERNAL_INCLUDE_DIRS})
target_link_directories(lockdc_install_tree_consumer_shared PRIVATE ${LOCKDC_EXTERNAL_LIBRARY_DIRS})
target_link_libraries(lockdc_install_tree_consumer_shared PRIVATE lockdc::shared)
]=])

file(WRITE "${consumer_src_dir}/main.c" [=[
#include <lc/lc.h>

int main(void) {
    lc_client_config config;
    lonejson_parse_options parse_options;
    lonejson_int64 value;

    value = 0;
    lc_client_config_init(&config);
    parse_options = lonejson_default_parse_options();
    return value == 0 && parse_options.max_depth > 0 ? 0 : 1;
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
    lonejson_parse_options parse_options;

    parse_options = lonejson_default_parse_options();
    return lc_version_string() != 0 && parse_options.max_depth > 0 ? 0 : 1;
}
]=])

set(lockdc_consumer_configure_command
    "${CMAKE_COMMAND}"
    -S "${consumer_src_dir}"
    -B "${consumer_bin_dir}"
    "-DCMAKE_C_COMPILER=${LOCKDC_C_COMPILER}"
    "-DCMAKE_C_FLAGS_DEBUG=${LOCKDC_C_FLAGS_DEBUG}"
    "-DCMAKE_PREFIX_PATH=${install_prefix}"
    "-DLOCKDC_EXTERNAL_ROOT=${LOCKDC_EXTERNAL_ROOT}"
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

set(lockdc_pkgconfig_shared_consumer "${consumer_bin_dir}/lockdc_install_tree_pkgconfig_shared")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        "PKG_CONFIG_PATH=${install_prefix}/lib/pkgconfig"
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
        "PKG_CONFIG_PATH=${install_prefix}/lib/pkgconfig"
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

separate_arguments(lockdc_pkgconfig_shared_cflags_list UNIX_COMMAND "${lockdc_pkgconfig_shared_cflags}")
separate_arguments(lockdc_pkgconfig_shared_libs_list UNIX_COMMAND "${lockdc_pkgconfig_shared_libs}")
list(APPEND lockdc_pkgconfig_shared_cflags_list
    "-I${LOCKDC_EXTERNAL_ROOT}/curl/install/include"
    "-I${LOCKDC_EXTERNAL_ROOT}/openssl/install/include"
    "-I${LOCKDC_EXTERNAL_ROOT}/nghttp2/install/include"
    "-I${LOCKDC_EXTERNAL_ROOT}/pslog/install/include"
    "-I${LOCKDC_EXTERNAL_ROOT}/lonejson/install/include"
    "-I${LOCKDC_EXTERNAL_ROOT}/libssh2/install/include"
    "-I${LOCKDC_EXTERNAL_ROOT}/zlib/install/include")
list(APPEND lockdc_pkgconfig_shared_libs_list
    "-L${LOCKDC_EXTERNAL_ROOT}/curl/install/lib"
    "-L${LOCKDC_EXTERNAL_ROOT}/openssl/install/lib"
    "-L${LOCKDC_EXTERNAL_ROOT}/nghttp2/install/lib"
    "-L${LOCKDC_EXTERNAL_ROOT}/lonejson/install/lib"
    "-L${LOCKDC_EXTERNAL_ROOT}/pslog/install/lib"
    "-L${LOCKDC_EXTERNAL_ROOT}/libssh2/install/lib"
    "-L${LOCKDC_EXTERNAL_ROOT}/zlib/install/lib")
if(NOT CMAKE_HOST_SYSTEM_NAME STREQUAL "Darwin")
    foreach(lockdc_external_library_dir
        "${LOCKDC_EXTERNAL_ROOT}/curl/install/lib"
        "${LOCKDC_EXTERNAL_ROOT}/openssl/install/lib"
        "${LOCKDC_EXTERNAL_ROOT}/nghttp2/install/lib"
        "${LOCKDC_EXTERNAL_ROOT}/lonejson/install/lib"
        "${LOCKDC_EXTERNAL_ROOT}/pslog/install/lib"
        "${LOCKDC_EXTERNAL_ROOT}/libssh2/install/lib"
        "${LOCKDC_EXTERNAL_ROOT}/zlib/install/lib")
        list(APPEND lockdc_pkgconfig_shared_libs_list
            "-Wl,-rpath-link,${lockdc_external_library_dir}")
    endforeach()
endif()

execute_process(
    COMMAND "${LOCKDC_C_COMPILER}"
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
list(APPEND lockdc_pkgconfig_cflags_list
    "-I${LOCKDC_EXTERNAL_ROOT}/curl/install/include"
    "-I${LOCKDC_EXTERNAL_ROOT}/openssl/install/include"
    "-I${LOCKDC_EXTERNAL_ROOT}/nghttp2/install/include"
    "-I${LOCKDC_EXTERNAL_ROOT}/pslog/install/include"
    "-I${LOCKDC_EXTERNAL_ROOT}/lonejson/install/include"
    "-I${LOCKDC_EXTERNAL_ROOT}/libssh2/install/include"
    "-I${LOCKDC_EXTERNAL_ROOT}/zlib/install/include")
list(APPEND lockdc_pkgconfig_libs_list
    "-L${LOCKDC_EXTERNAL_ROOT}/curl/install/lib"
    "-L${LOCKDC_EXTERNAL_ROOT}/openssl/install/lib"
    "-L${LOCKDC_EXTERNAL_ROOT}/nghttp2/install/lib"
    "-L${LOCKDC_EXTERNAL_ROOT}/pslog/install/lib"
    "-L${LOCKDC_EXTERNAL_ROOT}/lonejson/install/lib"
    "-L${LOCKDC_EXTERNAL_ROOT}/libssh2/install/lib"
    "-L${LOCKDC_EXTERNAL_ROOT}/zlib/install/lib")

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

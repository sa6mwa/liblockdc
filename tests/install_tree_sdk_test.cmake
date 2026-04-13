if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

if(NOT DEFINED LOCKDC_C_COMPILER OR LOCKDC_C_COMPILER STREQUAL "")
    message(FATAL_ERROR "LOCKDC_C_COMPILER is required")
endif()

if(NOT DEFINED LOCKDC_BUILD_TYPE OR LOCKDC_BUILD_TYPE STREQUAL "")
    message(FATAL_ERROR "LOCKDC_BUILD_TYPE is required")
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
)
    if(NOT EXISTS "${required_path}")
        message(FATAL_ERROR "installed SDK is missing required artifact: ${required_path}")
    endif()
endforeach()

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

execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -S "${consumer_src_dir}"
        -B "${consumer_bin_dir}"
        "-DCMAKE_C_COMPILER=${LOCKDC_C_COMPILER}"
        "-DCMAKE_BUILD_TYPE=${LOCKDC_BUILD_TYPE}"
        "-DCMAKE_C_FLAGS_DEBUG=${LOCKDC_C_FLAGS_DEBUG}"
        "-DCMAKE_PREFIX_PATH=${install_prefix}"
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

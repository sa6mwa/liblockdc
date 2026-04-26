if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()
if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()
get_filename_component(LOCKDC_BINARY_DIR "${LOCKDC_BINARY_DIR}" ABSOLUTE)
get_filename_component(LOCKDC_ROOT "${LOCKDC_ROOT}" ABSOLUTE)
if(DEFINED LOCKDC_DIST_DIR AND NOT "${LOCKDC_DIST_DIR}" STREQUAL "")
    set(lockdc_dist_dir "${LOCKDC_DIST_DIR}")
else()
    set(lockdc_dist_dir "${LOCKDC_ROOT}/dist")
endif()

function(lockdc_import_cache_path var_name)
    if(DEFINED ${var_name} AND NOT "${${var_name}}" STREQUAL "")
        return()
    endif()
    file(STRINGS "${LOCKDC_BINARY_DIR}/CMakeCache.txt" cache_line
         REGEX "^${var_name}(:[^=]+)?=" LIMIT_COUNT 1)
    if(cache_line)
        string(REGEX REPLACE "^[^=]*=" "" cache_value "${cache_line}")
        set(${var_name} "${cache_value}" PARENT_SCOPE)
    endif()
endfunction()

lockdc_import_cache_path(CMAKE_C_COMPILER)
lockdc_import_cache_path(CMAKE_TOOLCHAIN_FILE)
lockdc_import_cache_path(CMAKE_BUILD_TYPE)
lockdc_import_cache_path(LOCKDC_OTOOL)

include("${LOCKDC_BINARY_DIR}/package-metadata.cmake")
if(NOT LOCKDC_TARGET_ID STREQUAL "arm64-apple-darwin")
    message(FATAL_ERROR "Darwin smoke bundle requires arm64-apple-darwin, got ${LOCKDC_TARGET_ID}")
endif()

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

set(bundle_root "${LOCKDC_BINARY_DIR}/darwin-smoke-bundle")
set(bundle_dist "${bundle_root}/dist")
set(extract_root "${bundle_root}/release")
set(consumer_src_dir "${bundle_root}/consumer")
set(consumer_bin_dir "${bundle_root}/consumer-build")
set(stage_name "liblockdc-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}-smoke")
set(stage_root "${bundle_root}/${stage_name}")
set(release_archive "${bundle_dist}/liblockdc-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}.tar.gz")
set(release_prefix "${extract_root}/liblockdc-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}")
set(smoke_archive "${lockdc_dist_dir}/${stage_name}.zip")

file(REMOVE_RECURSE "${bundle_root}")
file(MAKE_DIRECTORY "${bundle_dist}" "${extract_root}" "${consumer_src_dir}" "${consumer_bin_dir}")

execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -DLOCKDC_BINARY_DIR=${LOCKDC_BINARY_DIR}
        -DLOCKDC_ROOT=${LOCKDC_ROOT}
        -DLOCKDC_DIST_DIR=${bundle_dist}
        -P "${LOCKDC_ROOT}/cmake/package_archive.cmake"
    RESULT_VARIABLE package_result
    OUTPUT_VARIABLE package_stdout
    ERROR_VARIABLE package_stderr
)
if(NOT package_result EQUAL 0)
    message(FATAL_ERROR
        "failed to build Darwin release archive for smoke bundle\n"
        "stdout:\n${package_stdout}\n"
        "stderr:\n${package_stderr}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E tar xf "${release_archive}"
    WORKING_DIRECTORY "${extract_root}"
    RESULT_VARIABLE extract_result
    OUTPUT_VARIABLE extract_stdout
    ERROR_VARIABLE extract_stderr
)
if(NOT extract_result EQUAL 0)
    message(FATAL_ERROR
        "failed to extract Darwin release archive for smoke bundle\n"
        "stdout:\n${extract_stdout}\n"
        "stderr:\n${extract_stderr}")
endif()

file(WRITE "${consumer_src_dir}/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.21)
project(lockdc_darwin_smoke C)

find_package(lockdc CONFIG REQUIRED)

add_executable(lockdc_static_smoke smoke.c)
target_link_libraries(lockdc_static_smoke PRIVATE lockdc::static)

add_executable(lockdc_shared_smoke smoke.c)
target_link_libraries(lockdc_shared_smoke PRIVATE lockdc::shared)
set_target_properties(lockdc_shared_smoke PROPERTIES
  BUILD_RPATH "@executable_path/../lib"
  INSTALL_RPATH "@executable_path/../lib"
)
]=])

file(WRITE "${consumer_src_dir}/smoke.c" [=[
#include <lc/lc.h>

#include <stdio.h>
#include <string.h>

int main(void) {
    lc_client_config config;
    lonejson_parse_options parse_options;
    const char *version;

    lc_client_config_init(&config);
    parse_options = lonejson_default_parse_options();
    version = lc_version_string();
    if (version == NULL || strcmp(version, LC_VERSION_STRING) != 0) {
        return 10;
    }
    if (parse_options.max_depth <= 0) {
        return 11;
    }

    puts(version);
    return 0;
}
]=])

set(configure_args
    -S "${consumer_src_dir}"
    -B "${consumer_bin_dir}"
    "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
    "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
    "-DCMAKE_PREFIX_PATH=${release_prefix}"
    "-Dlockdc_DIR=${release_prefix}/lib/cmake/lockdc"
    "-DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF"
    "-DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=OFF"
    "-DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON"
)
if(CMAKE_TOOLCHAIN_FILE)
    list(APPEND configure_args "-DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" ${configure_args}
    RESULT_VARIABLE configure_result
    OUTPUT_VARIABLE configure_stdout
    ERROR_VARIABLE configure_stderr
)
if(NOT configure_result EQUAL 0)
    message(FATAL_ERROR
        "failed to configure Darwin smoke bundle consumer\n"
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
        "failed to build Darwin smoke bundle consumer\n"
        "stdout:\n${build_stdout}\n"
        "stderr:\n${build_stderr}")
endif()

file(MAKE_DIRECTORY "${stage_root}/bin" "${stage_root}/lib")
file(COPY
    "${consumer_bin_dir}/lockdc_static_smoke"
    "${consumer_bin_dir}/lockdc_shared_smoke"
    DESTINATION "${stage_root}/bin"
)
file(GLOB smoke_dylibs LIST_DIRECTORIES false "${release_prefix}/lib/*.dylib")
if(NOT smoke_dylibs)
    message(FATAL_ERROR "Darwin smoke bundle has no dylibs to package from ${release_prefix}/lib")
endif()
file(COPY ${smoke_dylibs} DESTINATION "${stage_root}/lib")
file(WRITE "${stage_root}/run-smoke.sh" [=[
#!/bin/sh
set -eu
DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
"$DIR/bin/lockdc_static_smoke"
"$DIR/bin/lockdc_shared_smoke"
]=])
file(CHMOD "${stage_root}/run-smoke.sh"
    PERMISSIONS
        OWNER_READ OWNER_WRITE OWNER_EXECUTE
        GROUP_READ GROUP_EXECUTE
        WORLD_READ WORLD_EXECUTE
)

if(LOCKDC_OTOOL AND EXISTS "${LOCKDC_OTOOL}")
    foreach(smoke_binary lockdc_static_smoke lockdc_shared_smoke)
        execute_process(
            COMMAND "${LOCKDC_OTOOL}" -hv "${stage_root}/bin/${smoke_binary}"
            RESULT_VARIABLE otool_result
            OUTPUT_VARIABLE otool_output
            ERROR_VARIABLE otool_error
        )
        if(NOT otool_result EQUAL 0 OR NOT otool_output MATCHES "ARM64")
            message(FATAL_ERROR
                "Darwin smoke binary is not an arm64 Mach-O executable: ${smoke_binary}\n"
                "${otool_output}${otool_error}")
        endif()
    endforeach()
endif()

file(MAKE_DIRECTORY "${lockdc_dist_dir}")
file(REMOVE "${smoke_archive}")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E tar cf "${smoke_archive}" --format=zip "${stage_name}"
    WORKING_DIRECTORY "${bundle_root}"
    RESULT_VARIABLE zip_result
    OUTPUT_VARIABLE zip_stdout
    ERROR_VARIABLE zip_stderr
)
if(NOT zip_result EQUAL 0)
    message(FATAL_ERROR
        "failed to create Darwin smoke bundle zip\n"
        "stdout:\n${zip_stdout}\n"
        "stderr:\n${zip_stderr}")
endif()

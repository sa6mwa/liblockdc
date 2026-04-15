if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

function(lockdc_import_cache_value var_name)
    if(DEFINED ${var_name} AND NOT "${${var_name}}" STREQUAL "")
        return()
    endif()

    file(STRINGS "${LOCKDC_BINARY_DIR}/CMakeCache.txt" cache_line
        REGEX "^${var_name}(:[^=]+)?="
        LIMIT_COUNT 1)
    if(cache_line)
        string(REGEX REPLACE "^[^=]*=" "" cache_value "${cache_line}")
        set(${var_name} "${cache_value}" PARENT_SCOPE)
    endif()
endfunction()

lockdc_import_cache_value(CMAKE_C_COMPILER)
lockdc_import_cache_value(CMAKE_BUILD_TYPE)

if(NOT DEFINED CMAKE_C_COMPILER OR CMAKE_C_COMPILER STREQUAL "")
    message(FATAL_ERROR "CMAKE_C_COMPILER is required")
endif()

if(NOT DEFINED CMAKE_BUILD_TYPE OR CMAKE_BUILD_TYPE STREQUAL "")
    set(CMAKE_BUILD_TYPE "Release")
endif()

if(NOT DEFINED LOCKDC_RUN_DOWNSTREAM_BINARIES)
    set(LOCKDC_RUN_DOWNSTREAM_BINARIES ON)
endif()

if(NOT EXISTS "${LOCKDC_BINARY_DIR}/package-metadata.cmake")
    message(FATAL_ERROR "missing package metadata: ${LOCKDC_BINARY_DIR}/package-metadata.cmake")
endif()
include("${LOCKDC_BINARY_DIR}/package-metadata.cmake")

set(test_root "${LOCKDC_BINARY_DIR}/release-tarball-sdk-test")
set(dist_dir "${LOCKDC_DIST_DIR}")
set(extract_root "${test_root}/release")
set(consumer_src_dir "${test_root}/consumer")
set(consumer_bin_dir "${test_root}/consumer-build")

if(NOT DEFINED LOCKDC_DIST_DIR OR LOCKDC_DIST_DIR STREQUAL "")
    set(dist_dir "${test_root}/dist")
endif()

set(release_archive "${dist_dir}/liblockdc-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}.tar.gz")
set(release_prefix "${extract_root}/liblockdc-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}")

file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY "${extract_root}" "${consumer_src_dir}" "${consumer_bin_dir}" "${dist_dir}")

if(NOT EXISTS "${release_archive}")
    execute_process(
        COMMAND "${CMAKE_COMMAND}"
            -DLOCKDC_BINARY_DIR=${LOCKDC_BINARY_DIR}
            -DLOCKDC_ROOT=${LOCKDC_ROOT}
            -DLOCKDC_DIST_DIR=${dist_dir}
            -P "${LOCKDC_ROOT}/cmake/package_archive.cmake"
        RESULT_VARIABLE package_result
        OUTPUT_VARIABLE package_stdout
        ERROR_VARIABLE package_stderr
    )
    if(NOT package_result EQUAL 0)
        message(FATAL_ERROR
            "failed to build release archive for SDK test\n"
            "stdout:\n${package_stdout}\n"
            "stderr:\n${package_stderr}")
    endif()
endif()

if(NOT EXISTS "${release_archive}")
    message(FATAL_ERROR "missing release archive for SDK test: ${release_archive}")
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
        "failed to extract release archive for SDK test\n"
        "stdout:\n${extract_stdout}\n"
        "stderr:\n${extract_stderr}")
endif()

if(NOT EXISTS "${release_prefix}")
    message(FATAL_ERROR "release tarball missing expected prefix directory: ${release_prefix}")
endif()

foreach(required_path
    "${release_prefix}/include/lc/lc.h"
    "${release_prefix}/include/lc/version.h"
    "${release_prefix}/include/lonejson.h"
    "${release_prefix}/lib/liblockdc.a"
    "${release_prefix}/lib/liblockdc.so"
    "${release_prefix}/lib/pkgconfig/lockdc.pc"
    "${release_prefix}/lib/cmake/lockdc/lockdcConfig.cmake"
)
    if(NOT EXISTS "${required_path}")
        message(FATAL_ERROR "release tarball is missing required SDK artifact: ${required_path}")
    endif()
endforeach()

file(READ "${release_prefix}/lib/cmake/lockdc/lockdcConfig.cmake" lockdc_config_text)
file(READ "${release_prefix}/lib/pkgconfig/lockdc.pc" lockdc_pkgconfig_text)
foreach(forbidden_reference "${LOCKDC_ROOT}" "${LOCKDC_BINARY_DIR}" "${LOCKDC_ROOT}/.cache")
    string(REPLACE "\\" "\\\\" escaped_reference "${forbidden_reference}")
    string(REPLACE "." "\\." escaped_reference "${escaped_reference}")
    if(lockdc_config_text MATCHES "${escaped_reference}")
        message(FATAL_ERROR
            "release tarball CMake package leaks build-tree path '${forbidden_reference}'\n"
            "config:\n${lockdc_config_text}")
    endif()
    if(lockdc_pkgconfig_text MATCHES "${escaped_reference}")
        message(FATAL_ERROR
            "release tarball pkg-config metadata leaks build-tree path '${forbidden_reference}'\n"
            "pkg-config:\n${lockdc_pkgconfig_text}")
    endif()
endforeach()

file(WRITE "${consumer_src_dir}/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.21)
project(lockdc_release_tarball_consumer C)

find_package(lockdc CONFIG REQUIRED)

add_library(api_surface OBJECT api_surface.c)
target_link_libraries(api_surface PRIVATE lockdc::static)

add_executable(example_static example.c $<TARGET_OBJECTS:api_surface>)
target_link_libraries(example_static PRIVATE lockdc::static)

add_executable(test_static test.c $<TARGET_OBJECTS:api_surface>)
target_link_libraries(test_static PRIVATE lockdc::static)

add_executable(example_shared example.c $<TARGET_OBJECTS:api_surface>)
target_link_libraries(example_shared PRIVATE lockdc::shared)
set_target_properties(example_shared PROPERTIES BUILD_RPATH "${LOCKDC_RELEASE_PREFIX}/lib")

add_executable(test_shared test.c $<TARGET_OBJECTS:api_surface>)
target_link_libraries(test_shared PRIVATE lockdc::shared)
set_target_properties(test_shared PROPERTIES BUILD_RPATH "${LOCKDC_RELEASE_PREFIX}/lib")
]=])

file(WRITE "${consumer_src_dir}/example.c" [=[
#include <lc/lc.h>

#include <stdio.h>
#include <string.h>

int main(void) {
    const char *version = lc_version_string();

    if (version == NULL) {
        return 10;
    }
    if (strcmp(version, LC_VERSION_STRING) != 0) {
        return 11;
    }

    puts(version);
    return 0;
}
]=])

file(WRITE "${consumer_src_dir}/test.c" [=[
#include <lc/lc.h>

#include <string.h>

int main(void) {
    lc_client_config config;
    lonejson_int64 marker;
    const char *version;

    marker = 42;
    lc_client_config_init(&config);
    version = lc_version_string();
    if (marker != 42) {
        return 20;
    }
    if (version == NULL) {
        return 21;
    }
    if (strcmp(version, LC_VERSION_STRING) != 0) {
        return 22;
    }

    return 0;
}
]=])

file(WRITE "${consumer_src_dir}/api_surface.c" [=[
#include <lc/lc.h>

int lockdc_release_tarball_touch_client_api(lc_client *client, lc_lease **lease,
                                            lc_source *src, lc_sink *dst,
                                            const lonejson_map *map) {
    lc_error error;
    lc_acquire_req acquire;
    lc_get_opts get_opts;
    lc_get_res get_res;
    lc_update_req update;
    lc_update_res update_res;
    lc_mutate_op mutate;
    lc_mutate_res mutate_res;
    lc_metadata_op metadata;
    lc_metadata_res metadata_res;
    lc_remove_op remove;
    lc_remove_res remove_res;
    lc_keepalive_op keepalive;
    lc_keepalive_res keepalive_res;
    lc_release_op release;
    lc_release_res release_res;
    lc_query_req query;
    lc_query_res query_res;
    lc_enqueue_req enqueue;
    lc_enqueue_res enqueue_res;
    lc_dequeue_req dequeue;
    lc_queue_stats_req stats;
    lc_queue_stats_res stats_res;
    lc_message *message;
    lc_consumer consumer;
    lc_watch_queue_req watch_req;
    lc_watch_handler watch_handler;
    lc_consumer_service_config service_config;
    lc_consumer_service *service;

    lc_error_init(&error);
    lc_acquire_req_init(&acquire);
    lc_get_opts_init(&get_opts);
    lc_update_req_init(&update);
    lc_mutate_op_init(&mutate);
    lc_metadata_op_init(&metadata);
    lc_remove_op_init(&remove);
    lc_keepalive_op_init(&keepalive);
    lc_release_op_init(&release);
    lc_query_req_init(&query);
    lc_enqueue_req_init(&enqueue);
    lc_dequeue_req_init(&dequeue);
    lc_queue_stats_req_init(&stats);
    lc_consumer_init(&consumer);
    lc_watch_queue_req_init(&watch_req);
    lc_watch_handler_init(&watch_handler);
    lc_consumer_service_config_init(&service_config);

    message = (lc_message *)0;
    service = (lc_consumer_service *)0;

    return lc_acquire(client, &acquire, lease, &error) +
           lc_get(client, "key", &get_opts, dst, &get_res, &error) +
           lc_load(client, "key", map, (void *)0, (const lonejson_parse_options *)0,
                   &get_opts, &get_res, &error) +
           lc_update(client, &update, src, &update_res, &error) +
           lc_mutate(client, &mutate, &mutate_res, &error) +
           lc_metadata(client, &metadata, &metadata_res, &error) +
           lc_remove(client, &remove, &remove_res, &error) +
           lc_keepalive(client, &keepalive, &keepalive_res, &error) +
           lc_release(client, &release, &release_res, &error) +
           lc_query(client, &query, dst, &query_res, &error) +
           lc_enqueue(client, &enqueue, src, &enqueue_res, &error) +
           lc_dequeue(client, &dequeue, &message, &error) +
           lc_queue_stats(client, &stats, &stats_res, &error) +
           lc_subscribe(client, &dequeue, &consumer, &error) +
           lc_subscribe_with_state(client, &dequeue, &consumer, &error) +
           lc_client_new_consumer_service(client, &service_config, &service, &error) +
           lc_watch_queue(client, &watch_req, &watch_handler, &error);
}

int lockdc_release_tarball_touch_lease_api(lc_lease *lease, lc_source *src,
                                           lc_sink *dst,
                                           const lonejson_map *map) {
    lc_error error;
    lc_get_opts get_opts;
    lc_get_res get_res;
    lc_update_opts update_opts;
    lc_mutate_req mutate;
    lc_mutate_local_req mutate_local;
    lc_metadata_req metadata;
    lc_remove_req remove;
    lc_keepalive_req keepalive;
    lc_release_req release;
    lc_attach_req attach;
    lc_attach_res attach_res;
    lc_attachment_list attachments;
    lc_attachment_get_req attachment_get;
    lc_attachment_get_res attachment_get_res;
    lc_attachment_selector selector;
    int deleted;
    int deleted_count;

    lc_error_init(&error);
    lc_get_opts_init(&get_opts);
    lc_update_opts_init(&update_opts);
    lc_mutate_req_init(&mutate);
    lc_mutate_local_req_init(&mutate_local);
    lc_metadata_req_init(&metadata);
    lc_remove_req_init(&remove);
    lc_keepalive_req_init(&keepalive);
    lc_release_req_init(&release);
    lc_attach_req_init(&attach);
    lc_attachment_get_req_init(&attachment_get);
    lc_attachment_selector_init(&selector);
    deleted = 0;
    deleted_count = 0;

    return lc_lease_describe(lease, &error) +
           lc_lease_get(lease, dst, &get_opts, &get_res, &error) +
           lc_lease_load(lease, map, (void *)0, (const lonejson_parse_options *)0,
                         &get_opts, &get_res, &error) +
           lc_lease_save(lease, map, (const void *)0,
                         (const lonejson_write_options *)0, &error) +
           lc_lease_update(lease, src, &update_opts, &error) +
           lc_lease_mutate(lease, &mutate, &error) +
           lc_lease_mutate_local(lease, &mutate_local, &error) +
           lc_lease_metadata(lease, &metadata, &error) +
           lc_lease_remove(lease, &remove, &error) +
           lc_lease_keepalive(lease, &keepalive, &error) +
           lc_lease_release(lease, &release, &error) +
           lc_lease_attach(lease, &attach, src, &attach_res, &error) +
           lc_lease_list_attachments(lease, &attachments, &error) +
           lc_lease_get_attachment(lease, &attachment_get, dst, &attachment_get_res, &error) +
           lc_lease_delete_attachment(lease, &selector, &deleted, &error) +
           lc_lease_delete_all_attachments(lease, &deleted_count, &error);
}
]=])

execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -S "${consumer_src_dir}"
        -B "${consumer_bin_dir}"
        "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
        "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
        "-DCMAKE_PREFIX_PATH=${release_prefix}"
        "-DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF"
        "-DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=OFF"
        "-DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON"
        "-DLOCKDC_RELEASE_PREFIX=${release_prefix}"
    RESULT_VARIABLE configure_result
    OUTPUT_VARIABLE configure_stdout
    ERROR_VARIABLE configure_stderr
)
if(NOT configure_result EQUAL 0)
    message(FATAL_ERROR
        "failed to configure release-tarball consumer\n"
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
        "failed to build release-tarball consumer\n"
        "stdout:\n${build_stdout}\n"
        "stderr:\n${build_stderr}")
endif()

if(LOCKDC_RUN_DOWNSTREAM_BINARIES)
    foreach(binary_name example_static test_static example_shared test_shared)
        execute_process(
            COMMAND "${consumer_bin_dir}/${binary_name}"
            RESULT_VARIABLE run_result
            OUTPUT_VARIABLE run_stdout
            ERROR_VARIABLE run_stderr
        )
        if(NOT run_result EQUAL 0)
            message(FATAL_ERROR
                "release-tarball consumer binary failed: ${binary_name}\n"
                "stdout:\n${run_stdout}\n"
                "stderr:\n${run_stderr}")
        endif()
    endforeach()
endif()

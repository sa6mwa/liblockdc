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
        string(FIND "${cache_line}" "=" cache_value_offset)
        if(cache_value_offset LESS 0)
            message(FATAL_ERROR "malformed cache value for ${var_name}: ${cache_line}")
        endif()
        math(EXPR cache_value_offset "${cache_value_offset} + 1")
        string(SUBSTRING "${cache_line}" ${cache_value_offset} -1 cache_value)
        set(${var_name} "${cache_value}" PARENT_SCOPE)
    endif()
endfunction()

lockdc_import_cache_value(CMAKE_C_COMPILER)
lockdc_import_cache_value(CMAKE_C_FLAGS)
lockdc_import_cache_value(CMAKE_C_FLAGS_DEBUG)
lockdc_import_cache_value(CMAKE_BUILD_TYPE)

if(NOT DEFINED LOCKDC_TEST_NAME OR LOCKDC_TEST_NAME STREQUAL "")
    message(FATAL_ERROR "LOCKDC_TEST_NAME is required")
endif()

if(NOT DEFINED LOCKDC_LUA_TEST_SCRIPT OR LOCKDC_LUA_TEST_SCRIPT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_LUA_TEST_SCRIPT is required")
endif()

if(NOT DEFINED CMAKE_C_COMPILER OR CMAKE_C_COMPILER STREQUAL "")
    message(FATAL_ERROR "CMAKE_C_COMPILER is required for LuaRocks validation")
endif()

if(EXISTS "${LOCKDC_BINARY_DIR}/package-metadata.cmake")
    include("${LOCKDC_BINARY_DIR}/package-metadata.cmake")
endif()

set(LOCKDC_RUN_LUAROCKS_VALIDATION ON)
if(NOT DEFINED LOCKDC_RUN_LUA_SMOKE)
    set(LOCKDC_RUN_LUA_SMOKE ON)
endif()

if(DEFINED LOCKDC_TARGET_ID AND NOT LOCKDC_TARGET_ID STREQUAL "")
    execute_process(
        COMMAND uname -m
        OUTPUT_VARIABLE lockdc_host_arch_raw
        RESULT_VARIABLE lockdc_host_arch_result
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    if(lockdc_host_arch_result EQUAL 0)
        string(TOLOWER "${lockdc_host_arch_raw}" lockdc_host_arch)
        if(lockdc_host_arch STREQUAL "amd64")
            set(lockdc_host_arch "x86_64")
        elseif(lockdc_host_arch STREQUAL "arm64")
            set(lockdc_host_arch "aarch64")
        elseif(lockdc_host_arch MATCHES "^armv[0-9]+l$")
            set(lockdc_host_arch "armhf")
        endif()
    endif()

    string(REGEX MATCH "^[^-]+" lockdc_target_arch "${LOCKDC_TARGET_ID}")
    if(LOCKDC_TARGET_ID MATCHES "musl"
       OR (DEFINED lockdc_host_arch AND NOT lockdc_host_arch STREQUAL "" AND NOT lockdc_target_arch STREQUAL lockdc_host_arch))
        set(LOCKDC_RUN_LUA_SMOKE OFF)
        set(LOCKDC_RUN_LUAROCKS_VALIDATION OFF)
    endif()
endif()

find_program(LOCKDC_BASH_BIN NAMES bash)
find_program(LOCKDC_LUA_BIN NAMES lua lua5.5)
find_program(LOCKDC_LUAROCKS_BIN NAMES luarocks)

if(NOT LOCKDC_RUN_LUAROCKS_VALIDATION)
    message(STATUS "Skipping LuaRocks validation for ${LOCKDC_TEST_NAME}: target ${LOCKDC_TARGET_ID} is not buildable/loadable with the host Lua VM")
    return()
endif()

if(NOT LOCKDC_BASH_BIN)
    message(FATAL_ERROR "bash is required for LuaRocks validation")
endif()
if(NOT LOCKDC_LUA_BIN)
    message(FATAL_ERROR "lua is required for LuaRocks validation")
endif()
if(NOT LOCKDC_LUAROCKS_BIN)
    message(FATAL_ERROR "luarocks is required for LuaRocks validation")
endif()

if(NOT DEFINED LOCKDC_SDK_PREFIX OR LOCKDC_SDK_PREFIX STREQUAL "")
    set(LOCKDC_SDK_PREFIX "${LOCKDC_BINARY_DIR}/lua-rock-tests/${LOCKDC_TEST_NAME}/prefix")
    file(REMOVE_RECURSE "${LOCKDC_SDK_PREFIX}")
    execute_process(
        COMMAND "${CMAKE_COMMAND}" --install "${LOCKDC_BINARY_DIR}" --prefix "${LOCKDC_SDK_PREFIX}"
        RESULT_VARIABLE install_result
        OUTPUT_VARIABLE install_stdout
        ERROR_VARIABLE install_stderr
    )
    if(NOT install_result EQUAL 0)
        message(FATAL_ERROR
            "failed to stage SDK prefix for LuaRocks test ${LOCKDC_TEST_NAME}\n"
            "stdout:\n${install_stdout}\n"
            "stderr:\n${install_stderr}")
    endif()
endif()

if(DEFINED LOCKDC_ROCKSPEC_PATH AND NOT LOCKDC_ROCKSPEC_PATH STREQUAL "")
    set(LOCKDC_LUA_PACKAGE_PATH "${LOCKDC_ROCKSPEC_PATH}")
elseif(DEFINED LOCKDC_ROCK_PATH AND NOT LOCKDC_ROCK_PATH STREQUAL "")
    set(LOCKDC_LUA_PACKAGE_PATH "${LOCKDC_ROCK_PATH}")
else()
    file(GLOB generated_rockspec "${LOCKDC_BINARY_DIR}/lockdc-*-1.rockspec")
    list(LENGTH generated_rockspec generated_rockspec_count)
    if(NOT generated_rockspec_count EQUAL 1)
        message(FATAL_ERROR "expected one generated lockdc rockspec in ${LOCKDC_BINARY_DIR}")
    endif()
    list(GET generated_rockspec 0 LOCKDC_LUA_PACKAGE_PATH)
endif()

set(lonejson_cache_dir "${LOCKDC_BINARY_DIR}/lua-rock-cache")
set(lonejson_src_rock "${lonejson_cache_dir}/lonejson-0.12.0-1.src.rock")
set(lonejson_src_rock_url "https://github.com/sa6mwa/lonejson/releases/download/v0.12.0/lonejson-0.12.0-1.src.rock")
set(lua_tree_dir "${LOCKDC_BINARY_DIR}/lua-rock-tests/${LOCKDC_TEST_NAME}/tree")
set(lua_rock_workdir "${LOCKDC_ROOT}")

file(MAKE_DIRECTORY "${lonejson_cache_dir}")
if(NOT EXISTS "${lonejson_src_rock}")
    file(DOWNLOAD
        "${lonejson_src_rock_url}"
        "${lonejson_src_rock}"
        STATUS download_status
        SHOW_PROGRESS
    )
    list(GET download_status 0 download_code)
    list(GET download_status 1 download_message)
    if(NOT download_code EQUAL 0)
        file(REMOVE "${lonejson_src_rock}")
        message(FATAL_ERROR
            "failed to download lonejson Lua rock from ${lonejson_src_rock_url}: ${download_message}")
    endif()
endif()

set(lockdc_sanitizer_flags "${CMAKE_C_FLAGS}")
if(DEFINED CMAKE_BUILD_TYPE AND NOT CMAKE_BUILD_TYPE STREQUAL "")
    string(TOUPPER "${CMAKE_BUILD_TYPE}" lockdc_build_type_upper)
    if(lockdc_build_type_upper STREQUAL "DEBUG")
        string(APPEND lockdc_sanitizer_flags " ${CMAKE_C_FLAGS_DEBUG}")
    endif()
endif()

set(lockdc_asan_runtime "")
if(lockdc_sanitizer_flags MATCHES "(^|[ 	])-fsanitize=([^ 	,]+,)*address([, ][^ 	,]+)*($|[ 	])")
    execute_process(
        COMMAND "${CMAKE_C_COMPILER}" -print-file-name=libasan.so
        OUTPUT_VARIABLE lockdc_asan_runtime_raw
        RESULT_VARIABLE lockdc_asan_runtime_result
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    if(lockdc_asan_runtime_result EQUAL 0
       AND NOT lockdc_asan_runtime_raw STREQUAL ""
       AND NOT lockdc_asan_runtime_raw STREQUAL "libasan.so")
        set(lockdc_asan_runtime "${lockdc_asan_runtime_raw}")
    endif()
endif()

set(lua_build_root "${lua_rock_workdir}/.luarocks-build")

set(test_env
    "CC=${CMAKE_C_COMPILER}"
    "LOCKDC_LUA_BIN=${LOCKDC_LUA_BIN}"
    "LOCKDC_LUAROCKS_BIN=${LOCKDC_LUAROCKS_BIN}"
    "LOCKDC_LUA_VERSION=5.5"
    "LOCKDC_LONEJSON_SRC_ROCK=${lonejson_src_rock}"
)
if(NOT lockdc_asan_runtime STREQUAL "")
    list(APPEND test_env "LOCKDC_LD_PRELOAD=${lockdc_asan_runtime}")
    list(APPEND test_env "ASAN_OPTIONS=detect_leaks=0")
endif()

if(DEFINED LOCKDC_LUA_TEST_ENV AND NOT LOCKDC_LUA_TEST_ENV STREQUAL "")
    string(REPLACE "|" ";" LOCKDC_LUA_TEST_ENV_LIST "${LOCKDC_LUA_TEST_ENV}")
    foreach(env_pair IN LISTS LOCKDC_LUA_TEST_ENV_LIST)
        list(APPEND test_env "${env_pair}")
    endforeach()
endif()

if(LOCKDC_RUN_LUA_SMOKE)
    set(lockdc_run_lua_smoke_env "ON")
else()
    set(lockdc_run_lua_smoke_env "OFF")
endif()

list(APPEND test_env "LOCKDC_LUAROCKS_WORKDIR=${lua_rock_workdir}")
if(LOCKDC_LUA_PACKAGE_PATH MATCHES "[.]rockspec$")
    list(APPEND test_env "LOCKDC_LUAROCKS_BUILD_ROOT=${lua_build_root}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        ${test_env}
        "LOCKDC_RUN_LUA_SMOKE=${lockdc_run_lua_smoke_env}"
        "${LOCKDC_BASH_BIN}" "${LOCKDC_ROOT}/scripts/validate_lockdc_luarocks.sh"
        "${lua_tree_dir}"
        "${LOCKDC_SDK_PREFIX}"
        "${LOCKDC_LUA_PACKAGE_PATH}"
        "${LOCKDC_LUA_TEST_SCRIPT}"
    RESULT_VARIABLE run_result
    OUTPUT_VARIABLE run_stdout
    ERROR_VARIABLE run_stderr
)
if(NOT run_result EQUAL 0)
    message(FATAL_ERROR
        "LuaRocks validation failed for ${LOCKDC_TEST_NAME}\n"
        "stdout:\n${run_stdout}\n"
        "stderr:\n${run_stderr}")
endif()

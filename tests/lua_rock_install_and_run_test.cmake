if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

if(NOT DEFINED LOCKDC_TEST_NAME OR LOCKDC_TEST_NAME STREQUAL "")
    message(FATAL_ERROR "LOCKDC_TEST_NAME is required")
endif()

if(NOT DEFINED LOCKDC_LUA_TEST_SCRIPT OR LOCKDC_LUA_TEST_SCRIPT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_LUA_TEST_SCRIPT is required")
endif()

find_program(LOCKDC_BASH_BIN NAMES bash)
find_program(LOCKDC_LUA_BIN NAMES lua lua5.5)
find_program(LOCKDC_LUAROCKS_BIN NAMES luarocks)

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

if(NOT DEFINED LOCKDC_ROCKSPEC_PATH OR LOCKDC_ROCKSPEC_PATH STREQUAL "")
    file(GLOB generated_rockspec "${LOCKDC_BINARY_DIR}/lockdc-*-1.rockspec")
    list(LENGTH generated_rockspec generated_rockspec_count)
    if(NOT generated_rockspec_count EQUAL 1)
        message(FATAL_ERROR "expected one generated lockdc rockspec in ${LOCKDC_BINARY_DIR}")
    endif()
    list(GET generated_rockspec 0 LOCKDC_ROCKSPEC_PATH)
endif()

set(lonejson_cache_dir "${LOCKDC_BINARY_DIR}/lua-rock-cache")
set(lonejson_src_rock "${lonejson_cache_dir}/lonejson-0.4.1-1.src.rock")
set(lonejson_src_rock_url "https://github.com/sa6mwa/lonejson/releases/download/v0.4.1/lonejson-0.4.1-1.src.rock")
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

set(test_env
    "LOCKDC_LUA_BIN=${LOCKDC_LUA_BIN}"
    "LOCKDC_LUAROCKS_BIN=${LOCKDC_LUAROCKS_BIN}"
    "LOCKDC_LUA_VERSION=5.5"
    "LOCKDC_LONEJSON_SRC_ROCK=${lonejson_src_rock}"
    "LOCKDC_LUAROCKS_BUILD_ROOT=${lua_build_root}"
)

if(DEFINED LOCKDC_LUA_TEST_ENV AND NOT LOCKDC_LUA_TEST_ENV STREQUAL "")
    string(REPLACE "|" ";" LOCKDC_LUA_TEST_ENV_LIST "${LOCKDC_LUA_TEST_ENV}")
    foreach(env_pair IN LISTS LOCKDC_LUA_TEST_ENV_LIST)
        list(APPEND test_env "${env_pair}")
    endforeach()
endif()

if(DEFINED LOCKDC_SDK_PREFIX AND NOT LOCKDC_SDK_PREFIX STREQUAL ""
   AND EXISTS "${LOCKDC_SDK_PREFIX}/share/lockdc/luarocks")
    file(REAL_PATH "${LOCKDC_SDK_PREFIX}/share/lockdc/luarocks" lua_rock_workdir)
endif()

set(lua_build_root "${lua_rock_workdir}/.luarocks-build")

list(APPEND test_env "LOCKDC_LUAROCKS_WORKDIR=${lua_rock_workdir}")
list(APPEND test_env "LOCKDC_LUAROCKS_BUILD_ROOT=${lua_build_root}")

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        ${test_env}
        "${LOCKDC_BASH_BIN}" "${LOCKDC_ROOT}/scripts/validate_lockdc_luarocks.sh"
        "${lua_tree_dir}"
        "${LOCKDC_SDK_PREFIX}"
        "${LOCKDC_ROCKSPEC_PATH}"
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

if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

if(DEFINED LOCKDC_DIST_DIR AND NOT "${LOCKDC_DIST_DIR}" STREQUAL "")
    set(lockdc_dist_dir "${LOCKDC_DIST_DIR}")
else()
    set(lockdc_dist_dir "${LOCKDC_ROOT}/dist")
endif()

if(NOT EXISTS "${LOCKDC_BINARY_DIR}/package-metadata.cmake")
    message(FATAL_ERROR "missing package metadata: ${LOCKDC_BINARY_DIR}/package-metadata.cmake")
endif()
include("${LOCKDC_BINARY_DIR}/package-metadata.cmake")

set(lockdc_lua_rockspec_path "${lockdc_dist_dir}/lockdc-${LOCKDC_VERSION}-1.rockspec")
set(lockdc_lua_src_rock_path "${lockdc_dist_dir}/lockdc-${LOCKDC_VERSION}-1.src.rock")
set(lockdc_release_archive "${lockdc_dist_dir}/liblockdc-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}.tar.gz")
set(lockdc_extract_root "${LOCKDC_BINARY_DIR}/lua-release-package-test")
set(lockdc_release_prefix "${lockdc_extract_root}/liblockdc-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}")

if(NOT EXISTS "${lockdc_release_archive}")
    execute_process(
        COMMAND "${CMAKE_COMMAND}"
            -DLOCKDC_BINARY_DIR=${LOCKDC_BINARY_DIR}
            -DLOCKDC_ROOT=${LOCKDC_ROOT}
            -DLOCKDC_DIST_DIR=${lockdc_dist_dir}
            -P "${LOCKDC_ROOT}/cmake/package_archive.cmake"
        RESULT_VARIABLE package_archive_result
        OUTPUT_VARIABLE package_archive_stdout
        ERROR_VARIABLE package_archive_stderr
    )
    if(NOT package_archive_result EQUAL 0)
        message(FATAL_ERROR
            "failed to create C release archive for Lua package test
"
            "stdout:
${package_archive_stdout}
"
            "stderr:
${package_archive_stderr}")
    endif()
endif()

if(NOT EXISTS "${lockdc_lua_rockspec_path}" OR NOT EXISTS "${lockdc_lua_src_rock_path}")
    execute_process(
        COMMAND "${CMAKE_COMMAND}"
            -DLOCKDC_BINARY_DIR=${LOCKDC_BINARY_DIR}
            -DLOCKDC_ROOT=${LOCKDC_ROOT}
            -DLOCKDC_DIST_DIR=${lockdc_dist_dir}
            -P "${LOCKDC_ROOT}/cmake/package_lua_rock.cmake"
        RESULT_VARIABLE package_lua_result
        OUTPUT_VARIABLE package_lua_stdout
        ERROR_VARIABLE package_lua_stderr
    )
    if(NOT package_lua_result EQUAL 0)
        message(FATAL_ERROR
            "failed to create standalone Lua release package
"
            "stdout:
${package_lua_stdout}
"
            "stderr:
${package_lua_stderr}")
    endif()
endif()

foreach(required_path
    "${lockdc_lua_rockspec_path}"
    "${lockdc_lua_src_rock_path}"
    "${lockdc_release_archive}"
)
    if(NOT EXISTS "${required_path}")
        message(FATAL_ERROR "missing Lua release artifact: ${required_path}")
    endif()
endforeach()

file(READ "${lockdc_lua_rockspec_path}" lockdc_lua_rockspec_text)
foreach(required_snippet
    "package = \"lockdc\""
    "version = \"${LOCKDC_VERSION}-1\""
    "url = \"git+https://github.com/sa6mwa/liblockdc.git\""
    "tag = \"v${LOCKDC_VERSION}\""
    "scripts/build_lockdc_lua_rock.sh"
)
    string(FIND "${lockdc_lua_rockspec_text}" "${required_snippet}" snippet_index)
    if(snippet_index EQUAL -1)
        message(FATAL_ERROR
            "standalone Lua rockspec is missing expected snippet '${required_snippet}'\n"
            "rockspec:\n${lockdc_lua_rockspec_text}")
    endif()
endforeach()

file(REMOVE_RECURSE "${lockdc_extract_root}")
file(MAKE_DIRECTORY "${lockdc_extract_root}")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E tar xf "${lockdc_release_archive}"
    WORKING_DIRECTORY "${lockdc_extract_root}"
    RESULT_VARIABLE extract_result
    OUTPUT_VARIABLE extract_stdout
    ERROR_VARIABLE extract_stderr
)
if(NOT extract_result EQUAL 0)
    message(FATAL_ERROR
        "failed to extract C release archive for Lua package test\n"
        "stdout:\n${extract_stdout}\n"
        "stderr:\n${extract_stderr}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -DLOCKDC_BINARY_DIR=${LOCKDC_BINARY_DIR}
        -DLOCKDC_ROOT=${LOCKDC_ROOT}
        -DLOCKDC_TEST_NAME=lua-release-package-smoke
        -DLOCKDC_SDK_PREFIX=${lockdc_release_prefix}
        -DLOCKDC_ROCK_PATH=${lockdc_lua_src_rock_path}
        -DLOCKDC_LUA_TEST_SCRIPT=${LOCKDC_ROOT}/tests/lua/test_lockdc_luarocks_smoke.lua
        -P "${LOCKDC_ROOT}/tests/lua_rock_install_and_run_test.cmake"
    RESULT_VARIABLE lua_result
    OUTPUT_VARIABLE lua_stdout
    ERROR_VARIABLE lua_stderr
)
if(NOT lua_result EQUAL 0)
    message(FATAL_ERROR
        "standalone Lua release package validation failed\n"
        "stdout:\n${lua_stdout}\n"
        "stderr:\n${lua_stderr}")
endif()

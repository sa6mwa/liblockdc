if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

set(lockdc_test_root "${LOCKDC_BINARY_DIR}/lua-release-package-test-artifacts")
set(lockdc_dist_dir "${lockdc_test_root}/dist")

if(NOT EXISTS "${LOCKDC_BINARY_DIR}/package-metadata.cmake")
    message(FATAL_ERROR "missing package metadata: ${LOCKDC_BINARY_DIR}/package-metadata.cmake")
endif()
include("${LOCKDC_BINARY_DIR}/package-metadata.cmake")

function(assert_tar_numeric_owner_group archive_path)
    find_program(LOCKDC_TAR_BIN NAMES tar)
    if(NOT LOCKDC_TAR_BIN)
        message(FATAL_ERROR "tar is required for archive ownership validation")
    endif()

    execute_process(
        COMMAND "${LOCKDC_TAR_BIN}" --numeric-owner -tvzf "${archive_path}"
        RESULT_VARIABLE tar_result
        OUTPUT_VARIABLE archive_metadata
        ERROR_VARIABLE archive_metadata_error
    )
    if(NOT tar_result EQUAL 0)
        message(FATAL_ERROR
            "failed to inspect archive ownership metadata: ${archive_path}\n${archive_metadata}${archive_metadata_error}")
    endif()

    string(REGEX REPLACE "\n$" "" archive_metadata_trimmed "${archive_metadata}")
    string(REPLACE "\n" ";" archive_metadata_lines "${archive_metadata_trimmed}")
    foreach(archive_metadata_line IN LISTS archive_metadata_lines)
        if(archive_metadata_line STREQUAL "")
            continue()
        endif()
        if(NOT archive_metadata_line MATCHES "^[^ ]+[ ]+0/0([ ]+|$)")
            message(FATAL_ERROR
                "archive entry does not use numeric owner/group 0/0: ${archive_path}\n${archive_metadata_line}")
        endif()
    endforeach()
endfunction()

set(lockdc_lua_rockspec_path "${lockdc_dist_dir}/lockdc-${LOCKDC_VERSION}-1.rockspec")
set(lockdc_lua_src_rock_path "${lockdc_dist_dir}/lockdc-${LOCKDC_VERSION}-1.src.rock")
set(lockdc_release_archive "${lockdc_dist_dir}/liblockdc-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}.tar.gz")
set(lockdc_extract_root "${LOCKDC_BINARY_DIR}/lua-release-package-test")
set(lockdc_release_prefix "${lockdc_extract_root}/liblockdc-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}")
set(lockdc_lua_rock_extract_root "${LOCKDC_BINARY_DIR}/lua-release-package-src-rock")
set(lockdc_lua_inner_archive_path "${lockdc_lua_rock_extract_root}/lockdc-${LOCKDC_VERSION}-1.tar.gz")
set(lockdc_lua_inner_rockspec_path "${lockdc_lua_rock_extract_root}/lockdc-${LOCKDC_VERSION}-1.rockspec")

file(REMOVE_RECURSE "${lockdc_test_root}")
file(MAKE_DIRECTORY "${lockdc_dist_dir}")

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

foreach(required_path
    "${lockdc_lua_rockspec_path}"
    "${lockdc_lua_src_rock_path}"
    "${lockdc_release_archive}"
)
    if(NOT EXISTS "${required_path}")
        message(FATAL_ERROR "missing Lua release artifact: ${required_path}")
    endif()
endforeach()

file(REMOVE_RECURSE "${lockdc_lua_rock_extract_root}")
file(MAKE_DIRECTORY "${lockdc_lua_rock_extract_root}")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E tar xf "${lockdc_lua_src_rock_path}"
    WORKING_DIRECTORY "${lockdc_lua_rock_extract_root}"
    RESULT_VARIABLE rock_extract_result
    OUTPUT_VARIABLE rock_extract_stdout
    ERROR_VARIABLE rock_extract_stderr
)
if(NOT rock_extract_result EQUAL 0)
    message(FATAL_ERROR
        "failed to extract Lua source rock\n"
        "stdout:\n${rock_extract_stdout}\n"
        "stderr:\n${rock_extract_stderr}")
endif()

if(NOT EXISTS "${lockdc_lua_inner_archive_path}")
    message(FATAL_ERROR "Lua source rock is missing embedded source archive: ${lockdc_lua_inner_archive_path}")
endif()
if(NOT EXISTS "${lockdc_lua_inner_rockspec_path}")
    message(FATAL_ERROR "Lua source rock is missing embedded rockspec: ${lockdc_lua_inner_rockspec_path}")
endif()

assert_tar_numeric_owner_group("${lockdc_lua_inner_archive_path}")

file(READ "${lockdc_lua_inner_rockspec_path}" lockdc_lua_inner_rockspec_text)
foreach(disallowed_path "${LOCKDC_ROOT}" "$ENV{HOME}")
    if(disallowed_path STREQUAL "")
        continue()
    endif()
    string(FIND "${lockdc_lua_inner_rockspec_text}" "${disallowed_path}" disallowed_index)
    if(NOT disallowed_index EQUAL -1)
        message(FATAL_ERROR
            "Lua source rock embedded rockspec contains local path '${disallowed_path}'\n"
            "rockspec:\n${lockdc_lua_inner_rockspec_text}")
    endif()
endforeach()
string(FIND "${lockdc_lua_inner_rockspec_text}" "url = \"lockdc-${LOCKDC_VERSION}-1.tar.gz\"" inner_source_index)
if(inner_source_index EQUAL -1)
    message(FATAL_ERROR
        "Lua source rock embedded rockspec should reference the embedded source archive by relative name\n"
        "rockspec:\n${lockdc_lua_inner_rockspec_text}")
endif()

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
        "-DLOCKDC_LUA_TEST_ENV=LOCKDC_CFLAGS_EXTRA=-I${LOCKDC_EXTERNAL_ROOT}/curl/install/include -I${LOCKDC_EXTERNAL_ROOT}/openssl/install/include -I${LOCKDC_EXTERNAL_ROOT}/nghttp2/install/include -I${LOCKDC_EXTERNAL_ROOT}/pslog/install/include -I${LOCKDC_EXTERNAL_ROOT}/lonejson/install/include -I${LOCKDC_EXTERNAL_ROOT}/libssh2/install/include -I${LOCKDC_EXTERNAL_ROOT}/zlib/install/include|LOCKDC_LIBS_EXTRA=-L${LOCKDC_EXTERNAL_ROOT}/curl/install/lib -L${LOCKDC_EXTERNAL_ROOT}/openssl/install/lib -L${LOCKDC_EXTERNAL_ROOT}/nghttp2/install/lib -L${LOCKDC_EXTERNAL_ROOT}/pslog/install/lib -L${LOCKDC_EXTERNAL_ROOT}/lonejson/install/lib -L${LOCKDC_EXTERNAL_ROOT}/libssh2/install/lib -L${LOCKDC_EXTERNAL_ROOT}/zlib/install/lib -Wl,-rpath,${LOCKDC_EXTERNAL_ROOT}/curl/install/lib -Wl,-rpath,${LOCKDC_EXTERNAL_ROOT}/openssl/install/lib -Wl,-rpath,${LOCKDC_EXTERNAL_ROOT}/nghttp2/install/lib -Wl,-rpath,${LOCKDC_EXTERNAL_ROOT}/pslog/install/lib -Wl,-rpath,${LOCKDC_EXTERNAL_ROOT}/lonejson/install/lib -Wl,-rpath,${LOCKDC_EXTERNAL_ROOT}/libssh2/install/lib -Wl,-rpath,${LOCKDC_EXTERNAL_ROOT}/zlib/install/lib|LD_LIBRARY_PATH=${LOCKDC_EXTERNAL_ROOT}/curl/install/lib:${LOCKDC_EXTERNAL_ROOT}/openssl/install/lib:${LOCKDC_EXTERNAL_ROOT}/nghttp2/install/lib:${LOCKDC_EXTERNAL_ROOT}/pslog/install/lib:${LOCKDC_EXTERNAL_ROOT}/lonejson/install/lib:${LOCKDC_EXTERNAL_ROOT}/libssh2/install/lib:${LOCKDC_EXTERNAL_ROOT}/zlib/install/lib"
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

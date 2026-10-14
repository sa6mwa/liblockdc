include("${LOCKDC_ROOT}/tests/bootlin_runtime_test_support.cmake")
include("${LOCKDC_BINARY_DIR}/package-metadata.cmake")
lockdc_import_cmake_cache_value("${LOCKDC_BINARY_DIR}" CMAKE_TOOLCHAIN_FILE)
set(root "${LOCKDC_BINARY_DIR}/lua-postgres-package-test")
set(dist "${root}/dist")
set(sdk "${root}/sdk")
file(REMOVE_RECURSE "${root}")
file(MAKE_DIRECTORY "${dist}" "${root}/unpacked" "${root}/tmp")
function(checked label)
    execute_process(COMMAND ${ARGN} RESULT_VARIABLE rc OUTPUT_VARIABLE output ERROR_VARIABLE error)
    if(NOT rc EQUAL 0)
        message(FATAL_ERROR "${label} failed (${rc})\n${output}\n${error}")
    endif()
endfunction()
checked("Lua source package" "${CMAKE_COMMAND}" -DLOCKDC_ROOT=${LOCKDC_ROOT}
    -DLOCKDC_BINARY_DIR=${LOCKDC_BINARY_DIR} -DLOCKDC_DIST_DIR=${dist}
    -P "${LOCKDC_ROOT}/cmake/package_lua_rock.cmake")
set(rock "${dist}/lockdc-postgres-${LOCKDC_VERSION}-1.src.rock")
if(NOT EXISTS "${rock}" OR NOT EXISTS "${dist}/lockdc-postgres-${LOCKDC_VERSION}-1.rockspec")
    message(FATAL_ERROR "Missing selected PostgreSQL Lua release artifacts")
endif()
execute_process(COMMAND "${CMAKE_COMMAND}" -E tar xf "${rock}"
    WORKING_DIRECTORY "${root}/unpacked" RESULT_VARIABLE rc)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR "Cannot extract PostgreSQL Lua source rock")
endif()
set(source_name "liblockdc-lua-${LOCKDC_VERSION}.tar.gz")
file(SHA256 "${dist}/${source_name}" expected)
file(SHA256 "${root}/unpacked/${source_name}" actual)
if(NOT actual STREQUAL expected)
    message(FATAL_ERROR "PostgreSQL Lua rock contains a different source archive")
endif()
file(READ "${root}/unpacked/lockdc-postgres-${LOCKDC_VERSION}-1.rockspec" spec)
string(FIND "${spec}" "url = \"${source_name}\"" local_source)
string(FIND "${spec}" "${LOCKDC_ROOT}" private_path)
if(local_source EQUAL -1 OR NOT private_path EQUAL -1)
    message(FATAL_ERROR "PostgreSQL source rock has invalid/private source metadata")
endif()
foreach(component runtime dev postgres-runtime postgres-dev)
    checked("Installed ${component} SDK" "${CMAKE_COMMAND}" --install "${LOCKDC_BINARY_DIR}"
        --prefix "${sdk}" --component ${component})
endforeach()
set(prefixes "${sdk}")
foreach(component cpkt cpktdb pslog lonejson liblql)
    string(APPEND prefixes ":${LOCKDC_EXTERNAL_ROOT}/${component}/install")
endforeach()
set(link_probe "/lockdc-postgres-lua-link-flags-test")
set(environment "LOCKDC_POSTGRES_ROCK=${rock}|LOCKDC_POSTGRES_PREFIX=${sdk}|CMAKE_PREFIX_PATH=${prefixes}|LOCKDC_LUA_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}|TMPDIR=${root}/tmp|LOCKDC_TEST_POUCH_ROOT=${root}/pouch|LDFLAGS=-Wl,-rpath,${link_probe}")
checked("Installed PostgreSQL Lua rock" "${CMAKE_COMMAND}"
    -DLOCKDC_BINARY_DIR=${LOCKDC_BINARY_DIR} -DLOCKDC_ROOT=${LOCKDC_ROOT}
    -DLOCKDC_TEST_NAME=lua-postgres-package -DLOCKDC_SDK_PREFIX=${sdk}
    -DLOCKDC_ROCK_PATH=${dist}/lockdc-${LOCKDC_VERSION}-1.src.rock
    -DLOCKDC_LUA_TEST_SCRIPT=${LOCKDC_ROOT}/tests/lua/test_lockdc_postgres_rock.lua
    -DLOCKDC_LUA_BIN=${LOCKDC_LUA_BIN} "-DLOCKDC_LUA_TEST_ENV=${environment}"
    -P "${LOCKDC_ROOT}/tests/lua_rock_install_and_run_test.cmake")
# Inspect the installed bytes, not a generated cache setting: both ELF and
# Mach-O keep the requested runtime search path in their module string table.
set(module "${LOCKDC_BINARY_DIR}/lua-rock-tests/lua-postgres-package/tree/lib/lua/5.5/lockdc/postgres.so")
file(STRINGS "${module}" linked_paths REGEX "${link_probe}")
if(NOT linked_paths)
    message(FATAL_ERROR "Installed PostgreSQL Lua module discarded downstream LDFLAGS")
endif()
# Rebuilding a rockspec reuses its CMake build directory. A changed LDFLAGS
# must affect the next link, rather than retaining CMake's initial environment.
lockdc_import_cmake_cache_value("${LOCKDC_BINARY_DIR}" CMAKE_C_COMPILER)
set(consumer "${root}/consumer")
file(MAKE_DIRECTORY "${consumer}")
execute_process(COMMAND "${CMAKE_COMMAND}" -E tar xf "${dist}/${source_name}"
    WORKING_DIRECTORY "${consumer}" RESULT_VARIABLE rc)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR "Cannot extract the companion rock builder")
endif()
set(source "${consumer}/liblockdc-lua-${LOCKDC_VERSION}")
foreach(pass first second)
    checked("Rebuilt companion rock (${pass})" "${CMAKE_COMMAND}" -E env
        "LOCKDC_PREFIX=${sdk}" "LOCKDC_POSTGRES_PREFIX=${sdk}"
        "CMAKE_PREFIX_PATH=${prefixes}" "LOCKDC_LUA_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}"
        "LDFLAGS=-Wl,-rpath,${link_probe}-${pass}"
        bash "${source}/scripts/build_postgres_lua_rock.sh"
        "${CMAKE_C_COMPILER}" "-O0" "-shared" o so
        "${LOCKDC_EXTERNAL_ROOT}/cpkt/install/include" "${LOCKDC_VERSION}")
endforeach()
file(STRINGS "${source}/.luarocks-build/lockdc/postgres.so" linked_paths
    REGEX "${link_probe}-second")
if(NOT linked_paths)
    message(FATAL_ERROR "Rebuilt PostgreSQL Lua module ignored changed downstream LDFLAGS")
endif()
checked("Lua checksums" "${CMAKE_COMMAND}" -DLOCKDC_ROOT=${LOCKDC_ROOT}
    -DLOCKDC_BINARY_DIR=${LOCKDC_BINARY_DIR} -DLOCKDC_DIST_DIR=${dist}
    -P "${LOCKDC_ROOT}/cmake/package_checksums.cmake")
file(READ "${dist}/liblockdc-${LOCKDC_VERSION}-CHECKSUMS" checksums)
foreach(suffix rockspec src.rock)
    string(FIND "${checksums}" "lockdc-postgres-${LOCKDC_VERSION}-1.${suffix}" member)
    if(member EQUAL -1)
        message(FATAL_ERROR "PostgreSQL Lua artifact missing from checksum inventory")
    endif()
endforeach()
checked("Lua privacy" "${CMAKE_COMMAND}" -E env "LOCKDC_DIST_DIR=${dist}"
    bash "${LOCKDC_ROOT}/scripts/verify_release_privacy.sh")

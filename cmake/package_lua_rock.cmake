if(DEFINED LOCKDC_DIST_DIR AND NOT "${LOCKDC_DIST_DIR}" STREQUAL "")
    set(lockdc_input_dist_dir "${LOCKDC_DIST_DIR}")
endif()
if(EXISTS "${LOCKDC_BINARY_DIR}/package-metadata.cmake")
    include("${LOCKDC_BINARY_DIR}/package-metadata.cmake")
endif()
if(DEFINED lockdc_input_dist_dir)
    set(LOCKDC_DIST_DIR "${lockdc_input_dist_dir}")
elseif(NOT DEFINED LOCKDC_DIST_DIR OR "${LOCKDC_DIST_DIR}" STREQUAL "")
    set(LOCKDC_DIST_DIR "${LOCKDC_ROOT}/dist")
endif()

find_program(LOCKDC_LUAROCKS_BIN NAMES luarocks)
find_program(LOCKDC_TAR_BIN NAMES tar)
find_program(LOCKDC_GZIP_BIN NAMES gzip)
if(NOT LOCKDC_LUAROCKS_BIN)
    message(FATAL_ERROR "failed to find luarocks for Lua source rock packaging")
endif()
if(NOT LOCKDC_TAR_BIN)
    message(FATAL_ERROR "failed to find tar for Lua source rock packaging")
endif()
if(NOT LOCKDC_GZIP_BIN)
    message(FATAL_ERROR "failed to find gzip for Lua source rock packaging")
endif()

file(GLOB lockdc_generated_rockspec "${LOCKDC_BINARY_DIR}/lockdc-*-1.rockspec")
list(LENGTH lockdc_generated_rockspec lockdc_generated_rockspec_count)
if(NOT lockdc_generated_rockspec_count EQUAL 1)
    message(FATAL_ERROR "expected one generated lockdc rockspec in ${LOCKDC_BINARY_DIR}")
endif()
list(GET lockdc_generated_rockspec 0 lockdc_generated_rockspec_path)
get_filename_component(lockdc_generated_rockspec_name "${lockdc_generated_rockspec_path}" NAME)

set(lockdc_lua_stage_root "${LOCKDC_BINARY_DIR}/lua-package")
set(lockdc_lua_stage_workdir "${lockdc_lua_stage_root}/work")
set(lockdc_lua_source_root_name "lockdc-${LOCKDC_VERSION}-1")
set(lockdc_lua_source_root "${lockdc_lua_stage_root}/${lockdc_lua_source_root_name}")
set(lockdc_lua_rockspec_name "lockdc-${LOCKDC_VERSION}-1.rockspec")
set(lockdc_lua_src_rock_name "lockdc-${LOCKDC_VERSION}-1.src.rock")
set(lockdc_lua_source_archive_base "${lockdc_lua_stage_root}/${lockdc_lua_source_root_name}.tar")
set(lockdc_lua_source_archive_path "${lockdc_lua_source_archive_base}.gz")
get_filename_component(lockdc_lua_source_archive_name "${lockdc_lua_source_archive_path}" NAME)

file(REMOVE_RECURSE "${lockdc_lua_stage_root}")
file(MAKE_DIRECTORY "${lockdc_lua_stage_workdir}")
file(MAKE_DIRECTORY "${lockdc_lua_source_root}/lua/lockdc")
file(MAKE_DIRECTORY "${lockdc_lua_source_root}/src/lua")
file(MAKE_DIRECTORY "${lockdc_lua_source_root}/scripts")
file(MAKE_DIRECTORY "${LOCKDC_DIST_DIR}")

file(COPY "${lockdc_generated_rockspec_path}" DESTINATION "${lockdc_lua_stage_workdir}")
if(NOT lockdc_generated_rockspec_name STREQUAL lockdc_lua_rockspec_name)
    file(RENAME
        "${lockdc_lua_stage_workdir}/${lockdc_generated_rockspec_name}"
        "${lockdc_lua_stage_workdir}/${lockdc_lua_rockspec_name}"
    )
endif()
file(COPY "${LOCKDC_ROOT}/lua/lockdc/init.lua" DESTINATION "${lockdc_lua_source_root}/lua/lockdc")
file(COPY "${LOCKDC_ROOT}/src/lua/lockdc_lua.c" DESTINATION "${lockdc_lua_source_root}/src/lua")
file(COPY "${LOCKDC_ROOT}/src/lc_api_internal.h" DESTINATION "${lockdc_lua_source_root}/src")
file(COPY "${LOCKDC_ROOT}/src/lc_engine_api.h" DESTINATION "${lockdc_lua_source_root}/src")
file(COPY "${LOCKDC_ROOT}/scripts/build_lockdc_lua_rock.sh" DESTINATION "${lockdc_lua_source_root}/scripts")

file(REMOVE "${lockdc_lua_source_archive_base}" "${lockdc_lua_source_archive_path}")
execute_process(
    COMMAND "${LOCKDC_TAR_BIN}" -cf "${lockdc_lua_source_archive_base}" --format=gnu --owner 0 --group 0 "${lockdc_lua_source_root_name}"
    WORKING_DIRECTORY "${lockdc_lua_stage_root}"
    RESULT_VARIABLE lockdc_lua_tar_result
    OUTPUT_VARIABLE lockdc_lua_tar_stdout
    ERROR_VARIABLE lockdc_lua_tar_stderr
)
if(NOT lockdc_lua_tar_result EQUAL 0)
    message(FATAL_ERROR
        "failed to create Lua source archive\n"
        "stdout:\n${lockdc_lua_tar_stdout}\n"
        "stderr:\n${lockdc_lua_tar_stderr}")
endif()
execute_process(
    COMMAND "${LOCKDC_GZIP_BIN}" -9 -f "${lockdc_lua_source_archive_base}"
    RESULT_VARIABLE lockdc_lua_gzip_result
    OUTPUT_VARIABLE lockdc_lua_gzip_stdout
    ERROR_VARIABLE lockdc_lua_gzip_stderr
)
if(NOT lockdc_lua_gzip_result EQUAL 0)
    message(FATAL_ERROR
        "failed to gzip Lua source archive\n"
        "stdout:\n${lockdc_lua_gzip_stdout}\n"
        "stderr:\n${lockdc_lua_gzip_stderr}")
endif()

file(COPY "${lockdc_lua_source_archive_path}" DESTINATION "${lockdc_lua_stage_workdir}")

file(READ "${lockdc_lua_stage_workdir}/${lockdc_lua_rockspec_name}" lockdc_lua_pack_rockspec_text)
string(REGEX REPLACE "source = \\{[^\\}]*\\}" "source = {\n  url = \"${lockdc_lua_source_archive_path}\",\n}" lockdc_lua_pack_rockspec_text "${lockdc_lua_pack_rockspec_text}")
file(WRITE "${lockdc_lua_stage_workdir}/${lockdc_lua_rockspec_name}" "${lockdc_lua_pack_rockspec_text}")

execute_process(
    COMMAND "${LOCKDC_LUAROCKS_BIN}" pack "${lockdc_lua_rockspec_name}"
    WORKING_DIRECTORY "${lockdc_lua_stage_workdir}"
    RESULT_VARIABLE lockdc_lua_pack_result
    OUTPUT_VARIABLE lockdc_lua_pack_stdout
    ERROR_VARIABLE lockdc_lua_pack_stderr
)
if(NOT lockdc_lua_pack_result EQUAL 0)
    message(FATAL_ERROR
        "failed to create Lua source rock\n"
        "stdout:\n${lockdc_lua_pack_stdout}\n"
        "stderr:\n${lockdc_lua_pack_stderr}")
endif()

if(NOT EXISTS "${lockdc_lua_stage_workdir}/${lockdc_lua_src_rock_name}")
    message(FATAL_ERROR "luarocks pack did not produce ${lockdc_lua_src_rock_name}")
endif()

set(lockdc_lua_src_rock_rewrite_root "${lockdc_lua_stage_root}/src-rock-rewrite")
file(REMOVE_RECURSE "${lockdc_lua_src_rock_rewrite_root}")
file(MAKE_DIRECTORY "${lockdc_lua_src_rock_rewrite_root}")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E tar xf "${lockdc_lua_stage_workdir}/${lockdc_lua_src_rock_name}"
    WORKING_DIRECTORY "${lockdc_lua_src_rock_rewrite_root}"
    RESULT_VARIABLE lockdc_lua_unpack_result
    OUTPUT_VARIABLE lockdc_lua_unpack_stdout
    ERROR_VARIABLE lockdc_lua_unpack_stderr
)
if(NOT lockdc_lua_unpack_result EQUAL 0)
    message(FATAL_ERROR
        "failed to unpack Lua source rock for local-path rewrite\n"
        "stdout:\n${lockdc_lua_unpack_stdout}\n"
        "stderr:\n${lockdc_lua_unpack_stderr}")
endif()

set(lockdc_lua_embedded_rockspec "${lockdc_lua_src_rock_rewrite_root}/${lockdc_lua_rockspec_name}")
if(NOT EXISTS "${lockdc_lua_embedded_rockspec}")
    message(FATAL_ERROR "Lua source rock is missing embedded rockspec: ${lockdc_lua_embedded_rockspec}")
endif()
file(READ "${lockdc_lua_embedded_rockspec}" lockdc_lua_embedded_rockspec_text)
string(REGEX REPLACE "source = \\{[^\\}]*\\}" "source = {\n  url = \"${lockdc_lua_source_archive_name}\",\n}" lockdc_lua_embedded_rockspec_text "${lockdc_lua_embedded_rockspec_text}")
file(WRITE "${lockdc_lua_embedded_rockspec}" "${lockdc_lua_embedded_rockspec_text}")

file(REMOVE "${lockdc_lua_stage_workdir}/${lockdc_lua_src_rock_name}")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E tar cf "${lockdc_lua_stage_workdir}/${lockdc_lua_src_rock_name}" --format=zip
        "${lockdc_lua_rockspec_name}"
        "${lockdc_lua_source_archive_name}"
    WORKING_DIRECTORY "${lockdc_lua_src_rock_rewrite_root}"
    RESULT_VARIABLE lockdc_lua_repack_result
    OUTPUT_VARIABLE lockdc_lua_repack_stdout
    ERROR_VARIABLE lockdc_lua_repack_stderr
)
if(NOT lockdc_lua_repack_result EQUAL 0)
    message(FATAL_ERROR
        "failed to repack Lua source rock after local-path rewrite\n"
        "stdout:\n${lockdc_lua_repack_stdout}\n"
        "stderr:\n${lockdc_lua_repack_stderr}")
endif()

file(COPY "${lockdc_generated_rockspec_path}" DESTINATION "${LOCKDC_DIST_DIR}")
file(COPY "${lockdc_lua_stage_workdir}/${lockdc_lua_src_rock_name}" DESTINATION "${LOCKDC_DIST_DIR}")

if(NOT DEFINED LOCKDC_BINARY_DIR OR LOCKDC_BINARY_DIR STREQUAL "")
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

file(STRINGS "${LOCKDC_BINARY_DIR}/CMakeCache.txt" lockdc_readelf_cache
    REGEX "^CMAKE_READELF:FILEPATH=")
file(STRINGS "${LOCKDC_BINARY_DIR}/CMakeCache.txt" lockdc_sysroot_cache
    REGEX "^CMAKE_SYSROOT:PATH=")
if(NOT lockdc_readelf_cache OR NOT lockdc_sysroot_cache)
    message(FATAL_ERROR "Bootlin readelf and sysroot are required")
endif()
string(REGEX REPLACE "^[^=]*=" "" LOCKDC_READELF "${lockdc_readelf_cache}")
string(REGEX REPLACE "^[^=]*=" "" LOCKDC_SYSROOT "${lockdc_sysroot_cache}")

foreach(binary_path IN ITEMS
    "${LOCKDC_BINARY_DIR}/tests/unit/lc_unit_streams"
    "${LOCKDC_BINARY_DIR}/lockdc_lua_runner")
    if(NOT EXISTS "${binary_path}")
        message(FATAL_ERROR "missing development executable: ${binary_path}")
    endif()
    execute_process(
        COMMAND "${LOCKDC_READELF}" -l "${binary_path}"
        RESULT_VARIABLE interpreter_result
        OUTPUT_VARIABLE interpreter_output
        ERROR_VARIABLE interpreter_error)
    if(NOT interpreter_result EQUAL 0)
        message(FATAL_ERROR "failed to inspect ${binary_path}\n${interpreter_error}")
    endif()
    string(FIND "${interpreter_output}" "Requesting program interpreter" interpreter_at)
    string(FIND "${interpreter_output}" "${LOCKDC_SYSROOT}" sysroot_at)
    if(interpreter_at EQUAL -1 OR sysroot_at EQUAL -1)
        message(FATAL_ERROR
            "development executable does not use the selected Bootlin interpreter: ${binary_path}\n"
            "${interpreter_output}")
    endif()

    execute_process(
        COMMAND "${LOCKDC_READELF}" -d "${binary_path}"
        RESULT_VARIABLE dynamic_result
        OUTPUT_VARIABLE dynamic_output
        ERROR_VARIABLE dynamic_error)
    if(NOT dynamic_result EQUAL 0)
        message(FATAL_ERROR "failed to inspect dynamic section for ${binary_path}\n${dynamic_error}")
    endif()
    string(FIND "${dynamic_output}" "(RPATH)" rpath_at)
    string(FIND "${dynamic_output}" "(RUNPATH)" runpath_at)
    if(rpath_at EQUAL -1 OR NOT runpath_at EQUAL -1)
        message(FATAL_ERROR
            "development executable must use transitive DT_RPATH, not DT_RUNPATH: ${binary_path}\n"
            "${dynamic_output}")
    endif()
    if(dynamic_output MATCHES "(:|\\[)/lib(:|\\])")
        message(FATAL_ERROR
            "development executable must not fall back to the host /lib runtime: ${binary_path}\n"
            "${dynamic_output}")
    endif()
endforeach()

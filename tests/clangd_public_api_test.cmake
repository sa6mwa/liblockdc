if(NOT LOCKDC_ROOT OR NOT LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "Source and native build directories are required")
endif()
find_program(LOCKDC_CLANGD NAMES clangd REQUIRED)
set(log_root "${LOCKDC_BINARY_DIR}/clangd-public-api-test")
file(MAKE_DIRECTORY "${log_root}")

set(sources include/lc/lc.h src/lc_collections.c)
if(LOCKDC_BUILD_POSTGRES)
    list(PREPEND sources include/lc/postgres.h)
    list(APPEND sources src/lc_postgres.c)
endif()
foreach(source IN LISTS sources)
    # Use the editor's normal discovery path. Per-token refactoring self-tests
    # add no include/configuration coverage and are costly on large C files.
    execute_process(
        COMMAND "${LOCKDC_CLANGD}" "--check=${LOCKDC_ROOT}/${source}"
            --check-locations=false
        WORKING_DIRECTORY "${LOCKDC_ROOT}"
        RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error
        TIMEOUT 20)
    string(REPLACE "/" "-" name "${source}")
    file(WRITE "${log_root}/${name}.log" "${output}${error}")
    if(NOT result EQUAL 0)
        message(FATAL_ERROR "clangd failed for ${source}: ${result}\n${output}${error}")
    endif()
    string(FIND "${output}${error}"
        "Loaded compilation database from ${LOCKDC_BINARY_DIR}/compile_commands.json"
        database_at)
    if(database_at EQUAL -1)
        message(FATAL_ERROR "clangd did not discover the native database for ${source}\n${output}${error}")
    endif()
endforeach()

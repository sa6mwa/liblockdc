if(NOT DEFINED LOCKDC_ROOT OR LOCKDC_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()
if(NOT DEFINED LOCKDC_SHARED_LIBRARY OR LOCKDC_SHARED_LIBRARY STREQUAL "")
    message(FATAL_ERROR "LOCKDC_SHARED_LIBRARY is required")
endif()
if(NOT DEFINED LOCKDC_NM OR LOCKDC_NM STREQUAL "")
    message(FATAL_ERROR "LOCKDC_NM is required")
endif()
if(NOT EXISTS "${LOCKDC_SHARED_LIBRARY}")
    message(FATAL_ERROR
        "shared library does not exist: ${LOCKDC_SHARED_LIBRARY}")
endif()
if(DEFINED LOCKDC_LUA_MODULE AND NOT LOCKDC_LUA_MODULE STREQUAL "" AND
   NOT EXISTS "${LOCKDC_LUA_MODULE}")
    message(FATAL_ERROR "Lua module does not exist: ${LOCKDC_LUA_MODULE}")
endif()

# The installed header is the complete dynamic lc_* contract.  Extract direct
# function references rather than receiver fields or typedefs; duplicate names
# in documentation are harmless and are removed below.
file(READ "${LOCKDC_ROOT}/include/lc/lc.h" public_header)
string(REGEX MATCHALL "lc_[A-Za-z0-9_]+[ \t\r\n]*\\(" public_matches
       "${public_header}")
set(public_symbols)
foreach(public_match IN LISTS public_matches)
    string(REGEX REPLACE "[ \t\r\n]*\\($" "" public_symbol
           "${public_match}")
    list(APPEND public_symbols "${public_symbol}")
endforeach()
list(REMOVE_DUPLICATES public_symbols)
list(SORT public_symbols)

if(public_symbols STREQUAL "")
    message(FATAL_ERROR "did not find public lc_* function declarations")
endif()

if(LOCKDC_SYSTEM_NAME STREQUAL "Darwin")
    set(nm_args -gU)
else()
    set(nm_args -D --defined-only)
endif()
execute_process(
    COMMAND "${LOCKDC_NM}" ${nm_args} "${LOCKDC_SHARED_LIBRARY}"
    RESULT_VARIABLE nm_result
    OUTPUT_VARIABLE nm_output
    ERROR_VARIABLE nm_stderr
)
if(NOT nm_result EQUAL 0)
    message(FATAL_ERROR
        "failed to inspect shared library exports\n"
        "stderr:\n${nm_stderr}")
endif()

string(REGEX MATCHALL "lc_[A-Za-z0-9_]+" exported_symbols "${nm_output}")
list(REMOVE_DUPLICATES exported_symbols)
list(SORT exported_symbols)

set(missing_exports)
foreach(public_symbol IN LISTS public_symbols)
    list(FIND exported_symbols "${public_symbol}" public_export_index)
    if(public_export_index EQUAL -1)
        list(APPEND missing_exports "${public_symbol}")
    endif()
endforeach()

set(private_exports)
foreach(exported_symbol IN LISTS exported_symbols)
    list(FIND public_symbols "${exported_symbol}" public_symbol_index)
    if(public_symbol_index EQUAL -1)
        list(APPEND private_exports "${exported_symbol}")
    endif()
endforeach()

set(private_lua_imports)
if(DEFINED LOCKDC_LUA_MODULE AND NOT LOCKDC_LUA_MODULE STREQUAL "")
    if(LOCKDC_SYSTEM_NAME STREQUAL "Darwin")
        set(lua_nm_args -u)
    else()
        set(lua_nm_args -D --undefined-only)
    endif()
    execute_process(
        COMMAND "${LOCKDC_NM}" ${lua_nm_args} "${LOCKDC_LUA_MODULE}"
        RESULT_VARIABLE lua_nm_result
        OUTPUT_VARIABLE lua_nm_output
        ERROR_VARIABLE lua_nm_stderr
    )
    if(NOT lua_nm_result EQUAL 0)
        message(FATAL_ERROR
            "failed to inspect Lua module imports\n"
            "stderr:\n${lua_nm_stderr}")
    endif()
    string(REGEX MATCHALL "lc_[A-Za-z0-9_]+" lua_imports "${lua_nm_output}")
    list(REMOVE_DUPLICATES lua_imports)
    list(SORT lua_imports)
    foreach(lua_import IN LISTS lua_imports)
        list(FIND public_symbols "${lua_import}" public_import_index)
        if(public_import_index EQUAL -1)
            list(APPEND private_lua_imports "${lua_import}")
        endif()
    endforeach()
endif()

if(missing_exports OR private_exports OR private_lua_imports)
    string(JOIN "\n  " missing_text ${missing_exports})
    string(JOIN "\n  " private_text ${private_exports})
    string(JOIN "\n  " private_lua_import_text ${private_lua_imports})
    message(FATAL_ERROR
        "shared liblockdc dynamic API differs from include/lc/lc.h\n"
        "missing public exports:\n  ${missing_text}\n"
        "unexpected private lc_* exports:\n  ${private_text}\n"
        "unexpected private lc_* Lua imports:\n  ${private_lua_import_text}")
endif()

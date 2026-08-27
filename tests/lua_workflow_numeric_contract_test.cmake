if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

file(READ "${LOCKDC_ROOT}/src/lua/lockdc_lua.c" lua_binding)

foreach(required_snippet
    "static long lcdc_check_long(lua_State *L, int index, const char *name)"
    "lc_i64_to_long_checked((lc_i64)value, &result)"
    "timeout_ms = lcdc_check_long(L, 2, \"workflow next timeout\")"
    "long ttl_seconds = lcdc_check_long(L, 2, \"outbox renewal ttl\")")
    string(FIND "${lua_binding}" "${required_snippet}" snippet_index)
    if(snippet_index EQUAL -1)
        message(FATAL_ERROR
            "Lua workflow numeric conversion contract is missing: ${required_snippet}")
    endif()
endforeach()

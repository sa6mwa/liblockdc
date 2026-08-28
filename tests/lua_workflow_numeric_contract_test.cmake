if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

file(READ "${LOCKDC_ROOT}/src/lua/lockdc_lua.c" lua_binding)

foreach(required_snippet
    "static long lcdc_check_long(lua_State *L, int index, const char *name)"
    "lc_i64_to_long_checked((lc_i64)value, &result)"
    "timeout_ms = lcdc_check_long(L, 2, \"workflow next timeout\")"
    "long ttl_seconds = lcdc_check_long(L, 2, \"outbox renewal ttl\")"
    "static int lcdc_set_size_field(lua_State *L, const char *name, size_t value,"
    "static int lcdc_set_uint64_field(lua_State *L, const char *name,"
    "workflow statistic exceeds Lua integer range"
    "lcdc_set_uint64_field(L, \"direct_notifications\","
    "stats->direct_notifications, error)")
    string(FIND "${lua_binding}" "${required_snippet}" snippet_index)
    if(snippet_index EQUAL -1)
        message(FATAL_ERROR
            "Lua workflow numeric conversion contract is missing: ${required_snippet}")
    endif()
endforeach()

foreach(forbidden_snippet
    "(long)stats->pending_notifications"
    "(long)stats->ready_jobs"
    "(long)stats->direct_notifications"
    "(long)stats->notification_overflows"
    "(long)stats->recovery_queries"
    "(long)stats->recovered_claims"
    "(long)stats->claim_losses"
    "(long)stats->payload_open_failures")
    string(FIND "${lua_binding}" "${forbidden_snippet}" snippet_index)
    if(NOT snippet_index EQUAL -1)
        message(FATAL_ERROR
            "Lua workflow statistics must not narrow through C long: ${forbidden_snippet}")
    endif()
endforeach()

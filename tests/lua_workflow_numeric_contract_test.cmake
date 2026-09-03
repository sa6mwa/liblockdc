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
    "static int lcdc_set_int64_field(lua_State *L, const char *name, lc_i64 value,"
    "workflow statistic exceeds Lua integer range"
    "workflow value exceeds Lua integer range"
    "lcdc_set_uint64_field(L, \"direct_notifications\","
    "stats->direct_notifications, error)"
    "lcdc_set_int64_field(L, \"version\", participant->version, error)"
    "lcdc_set_int64_field(L, \"version\", result.version, &error)"
    "lcdc_set_int64_field(L, \"lease_expires_at_unix\","
    "job->lease_expires_at_unix, error)")
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
    "(long)stats->payload_open_failures"
    "lcdc_set_integer_field(L, \"version\", participant->version)"
    "lcdc_set_integer_field(L, \"lease_expires_at_unix\",
                         job->lease_expires_at_unix)")
    string(FIND "${lua_binding}" "${forbidden_snippet}" snippet_index)
    if(NOT snippet_index EQUAL -1)
        message(FATAL_ERROR
            "Lua workflow statistics must not narrow through C long: ${forbidden_snippet}")
    endif()
endforeach()

string(FIND "${lua_binding}"
    "static int lcdc_workflow_participant_get(lua_State *L)" participant_get_start)
string(FIND "${lua_binding}"
    "static int lcdc_workflow_participant_update(lua_State *L)" participant_get_end)
if(participant_get_start EQUAL -1 OR participant_get_end EQUAL -1 OR
   participant_get_end LESS participant_get_start)
    message(FATAL_ERROR "Lua workflow participant get binding is missing")
endif()
math(EXPR participant_get_length "${participant_get_end} - ${participant_get_start}")
string(SUBSTRING "${lua_binding}" ${participant_get_start}
    ${participant_get_length} participant_get_binding)
string(FIND "${participant_get_binding}"
    "lcdc_set_integer_field(L, \"version\", result.version)"
    participant_get_narrowing_index)
if(NOT participant_get_narrowing_index EQUAL -1)
    message(FATAL_ERROR
        "Lua workflow participant get must not narrow version through C long")
endif()

string(FIND "${lua_binding}"
    "static int lcdc_workflow_participant_attach(lua_State *L)"
    participant_attach_start)
string(FIND "${lua_binding}"
    "static int lcdc_workflow_participant_get_attachment(lua_State *L)"
    participant_attach_end)
if(participant_attach_start EQUAL -1 OR participant_attach_end EQUAL -1 OR
   participant_attach_end LESS participant_attach_start)
    message(FATAL_ERROR "Lua workflow participant attach binding is missing")
endif()
math(EXPR participant_attach_length
    "${participant_attach_end} - ${participant_attach_start}")
string(SUBSTRING "${lua_binding}" ${participant_attach_start}
    ${participant_attach_length} participant_attach_binding)
string(FIND "${participant_attach_binding}"
    "lcdc_set_int64_field(L, \"version\", result.version, &error)"
    participant_attach_wide_index)
if(participant_attach_wide_index EQUAL -1)
    message(FATAL_ERROR
        "Lua workflow participant attach must preserve 64-bit versions")
endif()
string(FIND "${participant_attach_binding}"
    "lcdc_set_integer_field(L, \"version\", result.version)"
    participant_attach_narrowing_index)
if(NOT participant_attach_narrowing_index EQUAL -1)
    message(FATAL_ERROR
        "Lua workflow participant attach must not narrow version through C long")
endif()

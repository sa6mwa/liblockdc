if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(NOT DEFINED LOCKDC_BUILD_TYPE)
    message(FATAL_ERROR "LOCKDC_BUILD_TYPE is required")
endif()

if(NOT LOCKDC_BUILD_TYPE STREQUAL "Release")
    message(STATUS "Skipping release compile warning contract for ${LOCKDC_BUILD_TYPE}")
    return()
endif()

set(compile_commands "${LOCKDC_BINARY_DIR}/compile_commands.json")
if(NOT EXISTS "${compile_commands}")
    message(FATAL_ERROR "release compile_commands.json not found: ${compile_commands}")
endif()

file(READ "${compile_commands}" compile_commands_json)

set(lockdc_benchmarks_required TRUE)
file(STRINGS "${LOCKDC_BINARY_DIR}/CMakeCache.txt" lockdc_build_benchmarks_cache
    REGEX "^LOCKDC_BUILD_BENCHMARKS(:[^=]+)?="
    LIMIT_COUNT 1)
if(lockdc_build_benchmarks_cache)
    string(REGEX REPLACE "^[^=]+=" "" lockdc_build_benchmarks_value "${lockdc_build_benchmarks_cache}")
    if(NOT lockdc_build_benchmarks_value)
        set(lockdc_benchmarks_required FALSE)
    endif()
endif()

function(assert_release_output_has_werror output_regex label required)
    string(REGEX MATCH
        "\"command\": [^\n]*\n  \"file\": [^\n]*\n  \"output\": \"${output_regex}\""
        entry
        "${compile_commands_json}"
    )
    if(entry STREQUAL "")
        string(REGEX MATCH
            "\"command\": [^\n]*\n  \"file\": [^\n]*\n  \"output\": \"[^\"]*/${output_regex}\""
            entry
            "${compile_commands_json}"
        )
    endif()
    if(entry STREQUAL "")
        if(required)
            message(FATAL_ERROR "Expected release compile command for ${label}")
        endif()
        return()
    endif()
    if(NOT entry MATCHES "(^|[ \t])-Werror([ \t\"]|$)")
        message(FATAL_ERROR
            "Expected release compile command for ${label} to include -Werror:\n${entry}")
    endif()
endfunction()

assert_release_output_has_werror(
    "CMakeFiles/lc_static.dir/src/lc_api.c.o"
    "lc_static"
    TRUE
)
assert_release_output_has_werror(
    "CMakeFiles/lc_shared.dir/src/lc_api.c.o"
    "lc_shared"
    TRUE
)
assert_release_output_has_werror(
    "tests/unit/CMakeFiles/lc_unit_pouch_disk.dir/test_lc_pouch_disk.c.o"
    "release unit tests"
    TRUE
)
assert_release_output_has_werror(
    "bench/CMakeFiles/lockdc_bench.dir/bench_main.c.o"
    "release benchmarks"
    ${lockdc_benchmarks_required}
)
assert_release_output_has_werror(
    "CMakeFiles/lockdc_lua_core.dir/src/lua/lockdc_lua.c.o"
    "Lua binding"
    FALSE
)

if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(NOT DEFINED LOCKDC_BUILD_TYPE)
    message(FATAL_ERROR "LOCKDC_BUILD_TYPE is required")
endif()

set(compile_commands "${LOCKDC_BINARY_DIR}/compile_commands.json")
if(NOT EXISTS "${compile_commands}")
    message(FATAL_ERROR "compile_commands.json not found: ${compile_commands}")
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

function(assert_output_has_werror output_regex label required c89_required)
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
            message(FATAL_ERROR "Expected compile command for ${label}")
        endif()
        return()
    endif()
    if(NOT entry MATCHES "(^|[ \t])-Werror([ \t\"]|$)")
        message(FATAL_ERROR
            "Expected compile command for ${label} to include -Werror:\n${entry}")
    endif()
    if(c89_required)
        foreach(required_option
                -std=c89
                -Wall
                -Wextra
                -Wpedantic
                -pedantic-errors)
            string(FIND "${entry}" "${required_option}" required_option_index)
            if(required_option_index EQUAL -1)
                message(FATAL_ERROR
                    "Expected compile command for ${label} to include ${required_option}:\n${entry}")
            endif()
        endforeach()
    endif()
endfunction()

assert_output_has_werror(
    "CMakeFiles/lc_static.dir/src/lc_api.c.o"
    "lc_static"
    TRUE
    TRUE
)
assert_output_has_werror(
    "CMakeFiles/lc_shared.dir/src/lc_api.c.o"
    "lc_shared"
    TRUE
    TRUE
)
assert_output_has_werror(
    "tests/unit/CMakeFiles/lc_unit_pouch.dir/test_lc_pouch.c.o"
    "release unit tests"
    TRUE
    FALSE
)
assert_output_has_werror(
    "bench/CMakeFiles/lockdc_bench.dir/bench_main.c.o"
    "release benchmarks"
    ${lockdc_benchmarks_required}
    FALSE
)
assert_output_has_werror(
    "CMakeFiles/lockdc_lua_core.dir/src/lua/lockdc_lua.c.o"
    "Lua binding"
    FALSE
    TRUE
)

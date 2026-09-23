function(read_paths path cpath output)
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E env
            --unset=LUA_PATH --unset=LUA_CPATH
            --unset=LUA_PATH_5_5 --unset=LUA_CPATH_5_5
            ${ARGN} "${path}" "${cpath}"
            "${LOCKDC_LUA_BIN}" -e "io.write(package.path, '\\n', package.cpath)"
        RESULT_VARIABLE result OUTPUT_VARIABLE paths ERROR_VARIABLE error)
    if(NOT result EQUAL 0)
        message(FATAL_ERROR "Lua runner failed: ${error}")
    endif()
    set(${output} "${paths}" PARENT_SCOPE)
endfunction()

read_paths("--unset=LUA_PATH" "--unset=LUA_CPATH" defaults)
read_paths("LUA_PATH=;;" "LUA_CPATH=;;" expanded)
if(NOT expanded STREQUAL defaults)
    message(FATAL_ERROR "Double-semicolon paths did not expand to Lua defaults")
endif()
read_paths("LUA_PATH=custom/?.lua" "LUA_CPATH=custom/?.so" custom)
if(NOT custom STREQUAL "custom/?.lua\ncustom/?.so")
    message(FATAL_ERROR "Explicit Lua search paths were not preserved")
endif()
read_paths("LUA_PATH=ignored" "LUA_CPATH=ignored" versioned
    "LUA_PATH_5_5=versioned/?.lua" "LUA_CPATH_5_5=versioned/?.so")
if(NOT versioned STREQUAL "versioned/?.lua\nversioned/?.so")
    message(FATAL_ERROR "Version-specific Lua paths did not take precedence")
endif()

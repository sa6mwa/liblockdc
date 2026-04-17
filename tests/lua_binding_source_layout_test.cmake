if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(EXISTS "${LOCKDC_BINARY_DIR}/package-metadata.cmake")
    include("${LOCKDC_BINARY_DIR}/package-metadata.cmake")
endif()

file(GLOB generated_rockspec "${LOCKDC_BINARY_DIR}/lockdc-*-1.rockspec")
list(LENGTH generated_rockspec generated_rockspec_count)
if(NOT generated_rockspec_count EQUAL 1)
    message(FATAL_ERROR "expected one generated lockdc rockspec in ${LOCKDC_BINARY_DIR}")
endif()
list(GET generated_rockspec 0 generated_rockspec_path)

foreach(required_path
    "${LOCKDC_ROOT}/lua/lockdc/init.lua"
    "${LOCKDC_ROOT}/src/lua/lockdc_lua.c"
    "${LOCKDC_ROOT}/scripts/build_lockdc_lua_rock.sh"
    "${LOCKDC_ROOT}/lockdc.rockspec.in"
    "${generated_rockspec_path}"
)
    if(NOT EXISTS "${required_path}")
        message(FATAL_ERROR "missing Lua binding artifact: ${required_path}")
    endif()
endforeach()

file(READ "${generated_rockspec_path}" rockspec_text)
foreach(required_snippet
    "package = \"lockdc\""
    "\"lonejson == 0.4.1-1\""
    "url = \"git+https://github.com/sa6mwa/liblockdc.git\""
    "tag = \"v"
    "scripts/build_lockdc_lua_rock.sh"
    "\\"$(LUA_INCDIR)\\" \\"${LOCKDC_VERSION}\\""
    "[\"lockdc.init\"] = \"lua/lockdc/init.lua\""
    "[\"lockdc.core\"] = \".luarocks-build/lockdc/core."
)
    string(FIND "${rockspec_text}" "${required_snippet}" snippet_index)
    if(snippet_index EQUAL -1)
        message(FATAL_ERROR
            "generated rockspec is missing expected snippet '${required_snippet}'\n"
            "rockspec:\n${rockspec_text}")
    endif()
endforeach()

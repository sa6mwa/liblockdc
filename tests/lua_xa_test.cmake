if(NOT LOCKDC_BINARY_DIR OR NOT LOCKDC_LUA_XA_BINARY OR NOT LOCKDC_ROOT)
    message(FATAL_ERROR "Lua XA test requires binary and source roots")
endif()
set(root "${LOCKDC_BINARY_DIR}/lua-xa-test")
file(REMOVE_RECURSE "${root}")
file(MAKE_DIRECTORY "${root}")
set(runner)
if("$ENV{LOCKDC_LUA_XA_VALGRIND}" STREQUAL "1")
    set(runner valgrind --error-exitcode=99 --leak-check=full
        --errors-for-leak-kinds=definite,indirect
        "--log-file=${LOCKDC_BINARY_DIR}/lua-xa-valgrind.log")
endif()
execute_process(
    COMMAND ${runner} "${LOCKDC_LUA_XA_BINARY}"
        "${LOCKDC_ROOT}/tests/lua/test_lockdc_xa.lua" "${root}/pouch"
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
file(REMOVE_RECURSE "${root}")
if(NOT result EQUAL 0)
    message(FATAL_ERROR "Lua XA failed (${result})\n${output}\n${error}")
endif()
message(STATUS "${output}")

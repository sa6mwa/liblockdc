set(LOCKDC_TEST_NAME lua-core-package)
set(LOCKDC_LUA_TEST_SCRIPT "${LOCKDC_ROOT}/tests/lua/test_lockdc_luarocks_smoke.lua")
include("${LOCKDC_ROOT}/tests/lua_rock_install_and_run_test.cmake")

# The base rock must work from its core SDK, including when the producer has
# PostgreSQL enabled. Installing provider components would require binaries
# outside the core-only E2E build target's closure.
foreach(path include/lc/lc.h lib/pkgconfig/lockdc.pc lib/cmake/lockdc/lockdcConfig.cmake)
    if(NOT EXISTS "${LOCKDC_SDK_PREFIX}/${path}")
        message(FATAL_ERROR "Base Lua SDK is missing ${path}")
    endif()
endforeach()
foreach(path include/lc/postgres.h lib/pkgconfig/lockdc-postgres.pc
    lib/cmake/lockdc_postgres share/doc/liblockdc_postgres)
    if(EXISTS "${LOCKDC_SDK_PREFIX}/${path}")
        message(FATAL_ERROR "Base Lua SDK unexpectedly installed provider component ${path}")
    endif()
endforeach()

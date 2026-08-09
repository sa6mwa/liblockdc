if(NOT DEFINED LOCKDC_POUCH_ROOT OR LOCKDC_POUCH_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_POUCH_ROOT is required")
endif()

file(REAL_PATH "${LOCKDC_BINARY_DIR}" lockdc_pouch_binary_dir)
file(REAL_PATH "${LOCKDC_POUCH_ROOT}" lockdc_pouch_root)
string(FIND "${lockdc_pouch_root}" "${lockdc_pouch_binary_dir}/"
    lockdc_pouch_root_prefix)
if(NOT lockdc_pouch_root_prefix EQUAL 0)
    message(FATAL_ERROR
        "LOCKDC_POUCH_ROOT must be a child of LOCKDC_BINARY_DIR for safe cleanup")
endif()

file(REMOVE_RECURSE "${lockdc_pouch_root}")
file(MAKE_DIRECTORY "${lockdc_pouch_root}")
file(CHMOD "${lockdc_pouch_root}"
    PERMISSIONS
        OWNER_READ
        OWNER_WRITE
        OWNER_EXECUTE)

if(DEFINED LOCKDC_LUA_TEST_ENV AND NOT LOCKDC_LUA_TEST_ENV STREQUAL "")
    string(APPEND LOCKDC_LUA_TEST_ENV "|LOCKDC_POUCH_ROOT=${lockdc_pouch_root}")
else()
    set(LOCKDC_LUA_TEST_ENV "LOCKDC_POUCH_ROOT=${lockdc_pouch_root}")
endif()

include("${LOCKDC_ROOT}/tests/lua_rock_install_and_run_test.cmake")

file(REMOVE_RECURSE "${lockdc_pouch_root}")

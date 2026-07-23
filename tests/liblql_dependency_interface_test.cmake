if(NOT DEFINED LOCKDC_EXTERNAL_ROOT OR LOCKDC_EXTERNAL_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_EXTERNAL_ROOT is required")
endif()

if(NOT DEFINED LOCKDC_DEPENDENCY_BUILD_ROOT OR LOCKDC_DEPENDENCY_BUILD_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_DEPENDENCY_BUILD_ROOT is required")
endif()

if(NOT DEFINED LOCKDC_LIBLQL_VERSION OR LOCKDC_LIBLQL_VERSION STREQUAL "")
    message(FATAL_ERROR "LOCKDC_LIBLQL_VERSION is required")
endif()

if(NOT DEFINED LOCKDC_LIBLQL_ABI_VERSION OR LOCKDC_LIBLQL_ABI_VERSION STREQUAL "")
    message(FATAL_ERROR "LOCKDC_LIBLQL_ABI_VERSION is required")
endif()

set(liblql_root "${LOCKDC_EXTERNAL_ROOT}/liblql/install")
set(liblql_build_root "${LOCKDC_DEPENDENCY_BUILD_ROOT}/liblql/build")
set(liblql_static_archive "${liblql_root}/lib/liblql.a")
set(liblql_shared_library "${liblql_root}/lib/liblql.so.${LOCKDC_LIBLQL_ABI_VERSION}")
set(liblql_header "${liblql_root}/include/lql/lql.h")
set(liblql_version_header "${liblql_root}/include/lql/version.h")

foreach(path IN ITEMS
    "${liblql_static_archive}"
    "${liblql_shared_library}"
    "${liblql_header}"
    "${liblql_version_header}")
    if(NOT EXISTS "${path}")
        message(FATAL_ERROR "missing liblql dependency artifact: ${path}")
    endif()
endforeach()

foreach(path IN ITEMS
    "${liblql_root}/bin")
    if(EXISTS "${path}")
        message(FATAL_ERROR "liblql install tree still exposes non-public artifact: ${path}")
    endif()
endforeach()

foreach(path IN ITEMS
    "${liblql_root}/lib/pkgconfig/liblql.pc"
    "${liblql_root}/lib/cmake/liblql/liblqlConfig.cmake"
    "${liblql_root}/lib/cmake/liblql/liblqlConfigVersion.cmake")
    if(NOT EXISTS "${path}")
        message(FATAL_ERROR "missing liblql consumer metadata: ${path}")
    endif()
endforeach()

file(READ "${liblql_root}/lib/pkgconfig/liblql.pc" liblql_pc_text)
if(NOT liblql_pc_text MATCHES "(^|\n)Version: ${LOCKDC_LIBLQL_VERSION}(\n|$)")
    message(FATAL_ERROR
        "liblql pkg-config metadata does not match configured version "
        "${LOCKDC_LIBLQL_VERSION}")
endif()
file(READ "${liblql_root}/lib/cmake/liblql/liblqlConfigVersion.cmake" liblql_cmake_version_text)
if(NOT liblql_cmake_version_text MATCHES "PACKAGE_VERSION \"${LOCKDC_LIBLQL_VERSION}\"")
    message(FATAL_ERROR
        "liblql CMake package metadata does not match configured version "
        "${LOCKDC_LIBLQL_VERSION}")
endif()

if(EXISTS "${liblql_build_root}")
    message(FATAL_ERROR
        "liblql dependency unexpectedly has a local build tree: ${liblql_build_root}")
endif()

find_program(NM_BIN NAMES nm REQUIRED)

execute_process(
    COMMAND "${NM_BIN}" -g --defined-only "${liblql_static_archive}"
    RESULT_VARIABLE static_nm_result
    OUTPUT_VARIABLE static_symbols
    ERROR_VARIABLE static_nm_stderr
)
if(NOT static_nm_result EQUAL 0)
    message(FATAL_ERROR
        "failed to inspect liblql static archive\n"
        "stderr:\n${static_nm_stderr}")
endif()

execute_process(
    COMMAND "${NM_BIN}" -D --defined-only "${liblql_shared_library}"
    RESULT_VARIABLE shared_nm_result
    OUTPUT_VARIABLE shared_symbols
    ERROR_VARIABLE shared_nm_stderr
)
if(NOT shared_nm_result EQUAL 0)
    message(FATAL_ERROR
        "failed to inspect liblql shared library\n"
        "stderr:\n${shared_nm_stderr}")
endif()

function(assert_contains text pattern description)
    if(NOT text MATCHES "${pattern}")
        message(FATAL_ERROR "liblql dependency is missing ${description}")
    endif()
endfunction()

foreach(symbol IN ITEMS
    lql_new
    lql_error_init
    lql_status_string
    lql_stream_apply
    lql_stream_apply_spooled
    lql_stream_value_size
    lql_stream_value_write_to
    lql_filter_file_spooled
    lql_rewrite_file_inline_spooled)
    assert_contains("${static_symbols}" "${symbol}" "${symbol} in liblql.a")
    assert_contains("${shared_symbols}" "${symbol}" "${symbol} in liblql.so.${LOCKDC_LIBLQL_ABI_VERSION}")
endforeach()

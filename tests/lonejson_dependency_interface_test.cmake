if(NOT DEFINED LOCKDC_EXTERNAL_ROOT OR LOCKDC_EXTERNAL_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_EXTERNAL_ROOT is required")
endif()

if(NOT DEFINED LOCKDC_DEPENDENCY_BUILD_ROOT OR LOCKDC_DEPENDENCY_BUILD_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_DEPENDENCY_BUILD_ROOT is required")
endif()

set(lonejson_root "${LOCKDC_EXTERNAL_ROOT}/lonejson/install")
set(lonejson_build_root "${LOCKDC_DEPENDENCY_BUILD_ROOT}/lonejson/build")
set(lonejson_static_archive "${lonejson_root}/lib/liblonejson.a")
set(lonejson_shared_library "${lonejson_root}/lib/liblonejson.so.0")
set(lonejson_header "${lonejson_root}/include/lonejson.h")

foreach(path IN ITEMS
    "${lonejson_static_archive}"
    "${lonejson_shared_library}"
    "${lonejson_header}")
    if(NOT EXISTS "${path}")
        message(FATAL_ERROR "missing lonejson dependency artifact: ${path}")
    endif()
endforeach()

foreach(path IN ITEMS
    "${lonejson_root}/lib/pkgconfig"
    "${lonejson_root}/lib/cmake"
    "${lonejson_root}/bin")
    if(EXISTS "${path}")
        message(FATAL_ERROR "lonejson install tree still exposes non-public artifact: ${path}")
    endif()
endforeach()

set(lonejson_ninja "${lonejson_build_root}/build.ninja")
if(NOT EXISTS "${lonejson_ninja}")
    message(FATAL_ERROR "missing lonejson build graph: ${lonejson_ninja}")
endif()

file(READ "${lonejson_ninja}" lonejson_ninja_text)
if(NOT lonejson_ninja_text MATCHES "CMakeFiles/lonejson_object\\.dir/")
    message(FATAL_ERROR "lonejson dependency build is not using a shared object library")
endif()

find_program(NM_BIN NAMES nm REQUIRED)

execute_process(
    COMMAND "${NM_BIN}" -g --defined-only "${lonejson_static_archive}"
    RESULT_VARIABLE static_nm_result
    OUTPUT_VARIABLE static_symbols
    ERROR_VARIABLE static_nm_stderr
)
if(NOT static_nm_result EQUAL 0)
    message(FATAL_ERROR
        "failed to inspect lonejson static archive\n"
        "stderr:\n${static_nm_stderr}")
endif()

execute_process(
    COMMAND "${NM_BIN}" -D --defined-only "${lonejson_shared_library}"
    RESULT_VARIABLE shared_nm_result
    OUTPUT_VARIABLE shared_symbols
    ERROR_VARIABLE shared_nm_stderr
)
if(NOT shared_nm_result EQUAL 0)
    message(FATAL_ERROR
        "failed to inspect lonejson shared library\n"
        "stderr:\n${shared_nm_stderr}")
endif()

function(assert_contains text pattern description)
    if(NOT text MATCHES "${pattern}")
        message(FATAL_ERROR "lonejson dependency is missing ${description}")
    endif()
endfunction()

function(assert_not_contains text pattern description)
    if(text MATCHES "${pattern}")
        message(FATAL_ERROR "lonejson dependency unexpectedly contains ${description}")
    endif()
endfunction()

foreach(symbol IN ITEMS
    lonejson_curl_parse_init
    lonejson_curl_write_callback
    lonejson_curl_parse_finish
    lonejson_curl_parse_cleanup
    lonejson_curl_upload_init
    lonejson_curl_read_callback
    lonejson_curl_upload_size
    lonejson_curl_upload_cleanup
    lonejson_generator_init
    lonejson_generator_read
    lonejson_generator_cleanup)
    assert_contains("${static_symbols}" "${symbol}" "${symbol} in liblonejson.a")
    assert_contains("${shared_symbols}" "${symbol}" "${symbol} in liblonejson.so.0")
endforeach()

foreach(alias_symbol IN ITEMS
    lj_curl_parse_init
    lj_curl_write_callback
    lj_curl_parse_finish
    lj_curl_parse_cleanup
    lj_curl_upload_init
    lj_curl_read_callback
    lj_curl_upload_size
    lj_curl_upload_cleanup
    lj_generator_init
    lj_generator_read
    lj_generator_cleanup)
    assert_not_contains("${static_symbols}" "${alias_symbol}" "${alias_symbol} in liblonejson.a")
    assert_not_contains("${shared_symbols}" "${alias_symbol}" "${alias_symbol} in liblonejson.so.0")
endforeach()

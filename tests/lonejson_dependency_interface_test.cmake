if(NOT DEFINED LOCKDC_EXTERNAL_ROOT OR LOCKDC_EXTERNAL_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_EXTERNAL_ROOT is required")
endif()

if(NOT DEFINED LOCKDC_DEPENDENCY_BUILD_ROOT OR LOCKDC_DEPENDENCY_BUILD_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_DEPENDENCY_BUILD_ROOT is required")
endif()

set(lonejson_root "${LOCKDC_EXTERNAL_ROOT}/lonejson/install")
set(lonejson_build_root "${LOCKDC_DEPENDENCY_BUILD_ROOT}/lonejson/build")
set(lonejson_static_archive "${lonejson_root}/lib/liblonejson.a")
set(lonejson_shared_library "${lonejson_root}/lib/liblonejson.so.4")
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
    "${lonejson_root}/bin")
    if(EXISTS "${path}")
        message(FATAL_ERROR "lonejson install tree still exposes non-public artifact: ${path}")
    endif()
endforeach()

foreach(path IN ITEMS
    "${lonejson_root}/lib/pkgconfig/lonejson.pc"
    "${lonejson_root}/lib/cmake/lonejson/lonejsonConfig.cmake"
    "${lonejson_root}/lib/cmake/lonejson/lonejsonConfigVersion.cmake")
    if(NOT EXISTS "${path}")
        message(FATAL_ERROR "missing lonejson consumer metadata: ${path}")
    endif()
endforeach()

if(EXISTS "${lonejson_build_root}")
    message(FATAL_ERROR
        "lonejson dependency unexpectedly has a local build tree: ${lonejson_build_root}")
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
    lonejson_generator_cleanup
    lonejson_default_sse_options
    lonejson_sse_open
    lonejson_sse_push
    lonejson_sse_finish
    lonejson_sse_push_json
    lonejson_sse_finish_json
    lonejson_sse_close
    lonejson_default_multipart_options
    lonejson_multipart_open
    lonejson_multipart_push
    lonejson_multipart_finish
    lonejson_multipart_close)
    assert_contains("${static_symbols}" "${symbol}" "${symbol} in liblonejson.a")
    assert_contains("${shared_symbols}" "${symbol}" "${symbol} in liblonejson.so.4")
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
    lj_generator_cleanup
    lj_default_sse_options
    lj_sse_open
    lj_sse_push
    lj_sse_finish
    lj_sse_push_json
    lj_sse_finish_json
    lj_sse_close
    lj_default_multipart_options
    lj_multipart_open
    lj_multipart_push
    lj_multipart_finish
    lj_multipart_close)
    assert_not_contains("${static_symbols}" "${alias_symbol}" "${alias_symbol} in liblonejson.a")
    assert_not_contains("${shared_symbols}" "${alias_symbol}" "${alias_symbol} in liblonejson.so.4")
endforeach()

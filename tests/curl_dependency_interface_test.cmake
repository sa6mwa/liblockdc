if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(NOT DEFINED LOCKDC_EXTERNAL_ROOT OR LOCKDC_EXTERNAL_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_EXTERNAL_ROOT is required")
endif()

set(curl_static_root "${LOCKDC_EXTERNAL_ROOT}/curl-static/install")
set(curl_shared_root "${LOCKDC_EXTERNAL_ROOT}/curl-shared-cmake/install")
set(curl_static_pc "${curl_static_root}/lib/pkgconfig/libcurl.pc")
set(curl_shared_pc "${curl_shared_root}/lib/pkgconfig/libcurl.pc")
set(curl_static_config "${curl_static_root}/bin/curl-config")
set(curl_shared_config "${curl_shared_root}/bin/curl-config")

function(assert_not_contains text pattern description)
    if(text MATCHES "${pattern}")
        message(FATAL_ERROR "curl dependency metadata unexpectedly contains ${description}")
    endif()
endfunction()

function(assert_contains text pattern description)
    if(NOT text MATCHES "${pattern}")
        message(FATAL_ERROR "curl dependency metadata is missing ${description}")
    endif()
endfunction()

function(assert_not_empty value description)
    if("${value}" STREQUAL "")
        message(FATAL_ERROR "expected non-empty ${description}")
    endif()
endfunction()

foreach(path IN ITEMS "${curl_static_pc}" "${curl_shared_pc}" "${curl_static_config}" "${curl_shared_config}")
    if(NOT EXISTS "${path}")
        message(FATAL_ERROR "missing curl dependency artifact: ${path}")
    endif()
endforeach()

file(READ "${curl_static_pc}" curl_static_pc_text)
file(READ "${curl_shared_pc}" curl_shared_pc_text)

execute_process(
    COMMAND "${curl_static_config}" --static-libs
    RESULT_VARIABLE static_libs_result
    OUTPUT_VARIABLE curl_static_libs
    ERROR_VARIABLE static_libs_stderr
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
if(NOT static_libs_result EQUAL 0)
    message(FATAL_ERROR
        "failed to query static curl libs\n"
        "stderr:\n${static_libs_stderr}")
endif()

execute_process(
    COMMAND "${curl_static_config}" --protocols
    RESULT_VARIABLE protocols_result
    OUTPUT_VARIABLE curl_protocols
    ERROR_VARIABLE protocols_stderr
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
if(NOT protocols_result EQUAL 0)
    message(FATAL_ERROR
        "failed to query curl protocols\n"
        "stderr:\n${protocols_stderr}")
endif()

assert_contains("${curl_static_pc_text}" "libssh2" "libssh2 dependency in static libcurl.pc")
assert_contains("${curl_shared_pc_text}" "libssh2" "libssh2 dependency in shared libcurl.pc")
assert_contains("${curl_static_libs}" "ssh2" "libssh2 link flags in curl-config --static-libs")
assert_contains("${curl_protocols}" "SCP" "SCP protocol support")
assert_contains("${curl_protocols}" "SFTP" "SFTP protocol support")

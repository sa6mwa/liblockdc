if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

set(public_header "${LOCKDC_ROOT}/include/lc/lc.h")
set(scan_paths
    "${LOCKDC_ROOT}/examples"
    "${LOCKDC_ROOT}/tests/e2e"
)

set(disallowed_internal_includes
    "#include \"lc_api_internal.h\""
    "#include \"lc_engine_api.h\""
    "#include \"lc_internal.h\""
)

set(disallowed_public_tokens
    "lc_engine_"
    "lc_client_handle"
    "lc_lease_handle"
    "lc_message_handle"
    "lc_consumer_service_handle"
)

file(READ "${public_header}" public_header_contents)

foreach(pattern IN LISTS disallowed_internal_includes)
    string(FIND "${public_header_contents}" "${pattern}" hit)
    if(NOT hit EQUAL -1)
        message(FATAL_ERROR "public header leaks internal include '${pattern}'")
    endif()
endforeach()

foreach(pattern IN LISTS disallowed_public_tokens)
    string(FIND "${public_header_contents}" "${pattern}" hit)
    if(NOT hit EQUAL -1)
        message(FATAL_ERROR "public header leaks internal symbol '${pattern}'")
    endif()
endforeach()

set(scan_files)
foreach(scan_path IN LISTS scan_paths)
    file(GLOB_RECURSE current_files
        "${scan_path}/*.c"
        "${scan_path}/*.h"
        "${scan_path}/*.cc"
        "${scan_path}/*.hh"
    )
    list(APPEND scan_files ${current_files})
endforeach()

foreach(scan_file IN LISTS scan_files)
    file(READ "${scan_file}" scan_contents)
    foreach(pattern IN LISTS disallowed_internal_includes)
        string(FIND "${scan_contents}" "${pattern}" hit)
        if(NOT hit EQUAL -1)
            message(FATAL_ERROR
                "public-facing example/e2e file '${scan_file}' includes private header '${pattern}'")
        endif()
    endforeach()
    foreach(pattern IN LISTS disallowed_public_tokens)
        string(FIND "${scan_contents}" "${pattern}" hit)
        if(NOT hit EQUAL -1)
            message(FATAL_ERROR
                "public-facing example/e2e file '${scan_file}' references private symbol '${pattern}'")
        endif()
    endforeach()
endforeach()

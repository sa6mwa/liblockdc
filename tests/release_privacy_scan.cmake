function(lockdc_private_trace_needles out_var)
    set(_needles "")

    foreach(_entry
        "LOCKDC_ROOT=${LOCKDC_ROOT}"
        "HOME=$ENV{HOME}"
    )
        if(NOT _entry MATCHES "^([^=]+)=(.*)$")
            continue()
        endif()
        set(_label "${CMAKE_MATCH_1}")
        set(_candidate "${CMAKE_MATCH_2}")
        if(_candidate STREQUAL "")
            continue()
        endif()
        string(REPLACE "\\" "/" _candidate "${_candidate}")
        list(FIND _needles "${_candidate}" _existing_needle_index)
        if(_existing_needle_index EQUAL -1)
            list(APPEND _needles "${_candidate}")
            list(APPEND _needles "${_label}")
        endif()
    endforeach()

    set(${out_var} "${_needles}" PARENT_SCOPE)
endfunction()

function(lockdc_sanitize_private_trace_text out_var text)
    set(_sanitized "${text}")
    lockdc_private_trace_needles(_needles)
    if(_needles)
        list(LENGTH _needles _needle_count)
        math(EXPR _needle_last "${_needle_count} - 1")
        foreach(_pair_index RANGE 0 ${_needle_last} 2)
            math(EXPR _label_index "${_pair_index} + 1")
            list(GET _needles ${_pair_index} _needle)
            list(GET _needles ${_label_index} _needle_label)
            if(NOT _needle STREQUAL "")
                string(REPLACE "${_needle}" "\${${_needle_label}}" _sanitized "${_sanitized}")
            endif()
        endforeach()
    endif()
    set(${out_var} "${_sanitized}" PARENT_SCOPE)
endfunction()

function(lockdc_assert_text_has_no_private_traces text source_label)
    lockdc_private_trace_needles(_needles)
    string(TOLOWER "${text}" _lower_text)
    lockdc_sanitize_private_trace_text(_source_label "${source_label}")

    if(NOT _needles)
        return()
    endif()

    list(LENGTH _needles _needle_count)
    math(EXPR _needle_last "${_needle_count} - 1")
    foreach(_pair_index RANGE 0 ${_needle_last} 2)
        math(EXPR _label_index "${_pair_index} + 1")
        list(GET _needles ${_pair_index} _needle)
        list(GET _needles ${_label_index} _needle_label)
        if(_needle STREQUAL "")
            continue()
        endif()
        string(TOLOWER "${_needle}" _lower_needle)
        string(FIND "${_lower_text}" "${_lower_needle}" _match_index)
        if(NOT _match_index EQUAL -1)
            message(FATAL_ERROR
                "release artifact contains private trace ${_needle_label} in ${_source_label}")
        endif()
    endforeach()
endfunction()

function(lockdc_assert_file_has_no_private_traces file_path source_label)
    find_program(LOCKDC_STRINGS_BIN NAMES strings)
    if(NOT LOCKDC_STRINGS_BIN)
        message(FATAL_ERROR "strings is required for release privacy validation")
    endif()

    if(IS_SYMLINK "${file_path}")
        file(READ_SYMLINK "${file_path}" _symlink_target)
        lockdc_assert_text_has_no_private_traces(
            "${_symlink_target}"
            "${source_label} symlink target ${file_path}"
        )
        return()
    endif()
    if(NOT EXISTS "${file_path}" OR IS_DIRECTORY "${file_path}")
        return()
    endif()

    execute_process(
        COMMAND "${LOCKDC_STRINGS_BIN}" "${file_path}"
        RESULT_VARIABLE _strings_result
        OUTPUT_VARIABLE _strings_output
        ERROR_VARIABLE _strings_error
    )
    if(NOT _strings_result EQUAL 0)
        lockdc_sanitize_private_trace_text(_file_path "${file_path}")
        message(FATAL_ERROR
            "failed to inspect release artifact for private traces: ${_file_path}\n${_strings_error}")
    endif()

    lockdc_assert_text_has_no_private_traces("${_strings_output}" "${source_label} file ${file_path}")
endfunction()

function(lockdc_assert_tree_has_no_private_traces tree_root source_label)
    if(NOT EXISTS "${tree_root}")
        lockdc_sanitize_private_trace_text(_tree_root "${tree_root}")
        message(FATAL_ERROR "missing release privacy scan root: ${_tree_root}")
    endif()

    file(GLOB_RECURSE _entries LIST_DIRECTORIES true "${tree_root}/*")
    foreach(_entry IN LISTS _entries)
        if(IS_DIRECTORY "${_entry}")
            continue()
        endif()
        lockdc_assert_file_has_no_private_traces("${_entry}" "${source_label}")
    endforeach()
endfunction()

function(lockdc_assert_archive_has_no_private_traces archive_path source_label)
    if(NOT EXISTS "${archive_path}")
        lockdc_sanitize_private_trace_text(_archive_path "${archive_path}")
        message(FATAL_ERROR "missing release privacy scan archive: ${_archive_path}")
    endif()

    string(RANDOM LENGTH 12 ALPHABET 0123456789abcdef _extract_suffix)
    get_filename_component(_archive_name "${archive_path}" NAME_WE)
    set(_extract_root "${CMAKE_CURRENT_BINARY_DIR}/privacy-scan-${_archive_name}-${_extract_suffix}")
    file(REMOVE_RECURSE "${_extract_root}")
    file(MAKE_DIRECTORY "${_extract_root}")
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E tar xf "${archive_path}"
        WORKING_DIRECTORY "${_extract_root}"
        RESULT_VARIABLE _extract_result
        ERROR_VARIABLE _extract_error
    )
    if(NOT _extract_result EQUAL 0)
        lockdc_sanitize_private_trace_text(_archive_path "${archive_path}")
        message(FATAL_ERROR
            "failed to extract release artifact for privacy scan: ${_archive_path}\n${_extract_error}")
    endif()

    lockdc_assert_tree_has_no_private_traces("${_extract_root}" "${source_label} archive ${archive_path}")
    file(REMOVE_RECURSE "${_extract_root}")
endfunction()

function(lockdc_assert_release_artifact_has_no_private_traces artifact_path source_label)
    if(IS_DIRECTORY "${artifact_path}")
        lockdc_assert_tree_has_no_private_traces("${artifact_path}" "${source_label}")
        return()
    endif()
    get_filename_component(_artifact_name "${artifact_path}" NAME)
    if(_artifact_name MATCHES "\\.(tar\\.gz|tgz|rock)$")
        lockdc_assert_archive_has_no_private_traces("${artifact_path}" "${source_label}")
    else()
        lockdc_assert_file_has_no_private_traces("${artifact_path}" "${source_label}")
    endif()
endfunction()

if(DEFINED LOCKDC_SCAN_PATHS AND NOT LOCKDC_SCAN_PATHS STREQUAL "")
    set(_scan_label "release artifact")
    if(DEFINED LOCKDC_SCAN_LABEL AND NOT LOCKDC_SCAN_LABEL STREQUAL "")
        set(_scan_label "${LOCKDC_SCAN_LABEL}")
    endif()
    foreach(_scan_path IN LISTS LOCKDC_SCAN_PATHS)
        lockdc_assert_release_artifact_has_no_private_traces("${_scan_path}" "${_scan_label}")
    endforeach()
endif()

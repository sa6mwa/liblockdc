foreach(target lc_unit_pouch lc_unit_outbox lc_unit_collections)
    if(target STREQUAL "lc_unit_pouch")
        set(case test_state_write_enforces_expected_etag)
    elseif(target STREQUAL "lc_unit_outbox")
        set(case test_outbox_public_rejections_clear_handle_outputs)
    else()
        set(case test_selection_filters_after_input_count)
    endif()
    foreach(selection valid unknown duplicate)
        set(args ${case})
        if(selection STREQUAL "unknown")
            list(APPEND args no_such_test)
        elseif(selection STREQUAL "duplicate")
            list(APPEND args ${case})
        endif()
        execute_process(COMMAND "${CMAKE_COMMAND}" -E env
            CMOCKA_TEST_FILTER=no_such_test "CMOCKA_SKIP_FILTER=*"
            CMOCKA_MESSAGE_OUTPUT=STANDARD LOCKDC_TEST_ALLOCATOR_TRACKING=1
            ${LOCKDC_TEST_RUNNER} "${LOCKDC_UNIT_DIR}/${target}" ${args}
            RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
        if(selection STREQUAL "valid")
            if(NOT result EQUAL 0 OR NOT "${output}${error}" MATCHES "\\[  PASSED  \\] 1 test" OR
               "${output}${error}" MATCHES "liblockdc allocator:")
                message(FATAL_ERROR "${target}: explicit case did not execute exactly once\n${output}\n${error}")
            endif()
        elseif(result EQUAL 0 OR NOT error MATCHES "Unknown or duplicate test case" OR
               output MATCHES "RUN")
            message(FATAL_ERROR "${target}: invalid selection did not fail before running\n${output}\n${error}")
        endif()
    endforeach()
endforeach()

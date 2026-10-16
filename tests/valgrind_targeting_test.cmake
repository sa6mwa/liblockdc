set(root "${LOCKDC_BINARY_DIR}/valgrind-targeting-test")
file(REMOVE_RECURSE "${root}")
file(MAKE_DIRECTORY "${root}/scripts" "${root}/bin")
file(COPY "${LOCKDC_ROOT}/scripts/valgrind.sh"
    "${LOCKDC_ROOT}/scripts/run_timed.sh" DESTINATION "${root}/scripts")
file(WRITE "${root}/bin/cmake" [=[#!/usr/bin/env bash
set -euo pipefail
printf 'cmake|%s\n' "$*" >> "$LOCKDC_TEST_LOG"
if [ "$1" = --build ]; then
  [ "${4:-}" = --target ] || exit 52
  [ "$LOCKDC_TEST_MODE" != build-fail ] || exit 17
  shift 4
  mkdir -p build/valgrind/tests/unit
  for target in "$@"; do
    [ "$LOCKDC_TEST_MODE" != missing ] || continue
    cat > "build/valgrind/tests/unit/$target" <<'EOF'
#!/usr/bin/env bash
set -eu
[ "$LOCKDC_UNDER_VALGRIND" = 1 ]
[ "$LOCKDC_TEST_MODE" != test-fail ] || exit 23
EOF
    chmod +x "build/valgrind/tests/unit/$target"
  done
fi
]=])
file(WRITE "${root}/bin/valgrind" [=[#!/usr/bin/env bash
set -euo pipefail
binary="${@: -1}"
printf 'run|%s|%s\n' "${binary##*/}" "$CMOCKA_TEST_FILTER" >> "$LOCKDC_TEST_LOG"
exec "$binary"
]=])
file(CHMOD "${root}/bin/cmake" "${root}/bin/valgrind"
    PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE)

function(check_case label selected mode expected)
    file(REMOVE_RECURSE "${root}/build")
    set(log "${root}/${label}.log")
    file(WRITE "${log}" "")
    execute_process(COMMAND "${CMAKE_COMMAND}" -E env
        "PATH=${root}/bin:$ENV{PATH}" "VALGRIND=${root}/bin/valgrind"
        LOCKDC_VALGRIND_PRESET=valgrind LOCKDC_VALGRIND_DRY_RUN=0
        "LOCKDC_VALGRIND_TESTS=${selected}" "LOCKDC_TEST_MODE=${mode}"
        "CMOCKA_TEST_FILTER=${ARGV4}"
        "LOCKDC_TEST_LOG=${log}" "LOCKDC_TIMING_DIR=${root}/timings"
        bash "${root}/scripts/valgrind.sh"
        RESULT_VARIABLE rc OUTPUT_VARIABLE output ERROR_VARIABLE error)
    if(NOT rc EQUAL expected)
        message(FATAL_ERROR "${label}: expected ${expected}, got ${rc}\n${output}\n${error}")
    endif()
    file(READ "${log}" calls)
    if(NOT calls MATCHES "cmake\\|--preset valgrind" OR calls MATCHES "--fresh")
        message(FATAL_ERROR "${label}: expected incremental configure: ${calls}")
    endif()
    if(selected STREQUAL "")
        set(targets lc_unit_streams lc_unit_engine_allocator lc_unit_contracts
            lc_unit_public_mock_usage lc_unit_runtime lc_unit_mutate_stream lc_unit_pouch lc_unit_outbox)
    else()
        separate_arguments(targets UNIX_COMMAND "${selected}")
    endif()
    list(JOIN targets " " target_args)
    string(FIND "${calls}" "cmake|--build --preset valgrind --target ${target_args}\n" found)
    if(found EQUAL -1)
        message(FATAL_ERROR "${label}: build must select exactly its test targets: ${calls}")
    endif()
    string(REGEX MATCHALL "run\\|[^\n]+" runs "${calls}")
    if(mode STREQUAL "ok")
        set(expected_runs)
        foreach(target IN LISTS targets)
            if(NOT "${ARGV4}" STREQUAL "")
                set(cases "${ARGV4}")
            elseif(target STREQUAL "lc_unit_outbox")
                set(cases test_pouch_dispatcher_wait_includes_worker_release
                    test_pouch_reconciliation_preserves_allocator_domains
                    test_pouch_dispatcher_stop_retains_inflight_receiver
                    test_pouch_client_close_from_source_callback
                    test_pouch_dispatcher_stop_timeout_preserves_handed_out_job)
            else()
                set(cases "*")
            endif()
            foreach(case IN LISTS cases)
                list(APPEND expected_runs "run|${target}|${case}")
            endforeach()
        endforeach()
        if(NOT runs STREQUAL expected_runs)
            message(FATAL_ERROR "${label}: wrong execution order or inventory: ${calls}")
        endif()
    elseif(mode STREQUAL "test-fail")
        list(GET targets 0 first)
        if(NOT runs STREQUAL "run|${first}|*")
            message(FATAL_ERROR "${label}: continued after failure: ${calls}")
        endif()
    elseif(runs)
        message(FATAL_ERROR "${label}: executed tests after failed/missing build: ${calls}")
    endif()
endfunction()
check_case(default "" ok 0)
check_case(custom "lc_unit_streams lc_unit_runtime" ok 0)
check_case(outbox "lc_unit_outbox" ok 0)
check_case(outbox-filter "lc_unit_outbox" ok 0 test_pouch_shared_process_dispatches_once)
check_case(build-failure "lc_unit_streams" build-fail 17)
check_case(missing-executable "lc_unit_streams" missing 2)
check_case(test-failure "lc_unit_streams lc_unit_runtime" test-fail 23)

if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

set(makefile_path "${LOCKDC_ROOT}/Makefile")
set(ledger_path "${LOCKDC_ROOT}/docs/lifecycle-migration.md")

file(READ "${makefile_path}" root_makefile)
file(READ "${ledger_path}" lifecycle_ledger)

function(assert_contains haystack needle description)
    string(FIND "${${haystack}}" "${needle}" found_at)
    if(found_at EQUAL -1)
        message(FATAL_ERROR "missing ${description}")
    endif()
endfunction()

foreach(target
        finalize-slice
        valgrind
        prerelease
        prerelease-live
        prerelease-hardening
        release-matrix)
    assert_contains(root_makefile "make ${target}" "make help entry for ${target}")
    assert_contains(root_makefile "${target}:" "make target ${target}")
endforeach()

assert_contains(root_makefile "bash ./scripts/valgrind.sh" "Valgrind runner wiring")
assert_contains(root_makefile "LOCKDC_PRERELEASE_LIVE=1" "live prerelease opt-in diagnostic")
assert_contains(root_makefile "__finalize-slice: __format __test-debug" "ordinary slice gate graph")
assert_contains(root_makefile "__test-all: __test-debug __test-host" "bounded test-all graph")
assert_contains(root_makefile "__prerelease: __finalize-slice __valgrind __fuzz-smoke __test-e2e __lua-test" "deterministic prerelease graph")
assert_contains(root_makefile "__prerelease-hardening: __prerelease __bench-gate __benchmark-pouch-go-parity-gate __release-matrix" "hardening prerelease graph")

execute_process(
    COMMAND
        "${CMAKE_COMMAND}" -E env
        LOCKDC_VALGRIND_DRY_RUN=1
        bash "${LOCKDC_ROOT}/scripts/valgrind.sh"
    WORKING_DIRECTORY "${LOCKDC_ROOT}"
    RESULT_VARIABLE valgrind_dry_run_result
    OUTPUT_VARIABLE valgrind_dry_run_stdout
    ERROR_VARIABLE valgrind_dry_run_stderr
)
if(NOT valgrind_dry_run_result EQUAL 0)
    message(FATAL_ERROR
        "Valgrind runner dry-run failed\nstdout:\n${valgrind_dry_run_stdout}\nstderr:\n${valgrind_dry_run_stderr}")
endif()
string(FIND "${valgrind_dry_run_stdout}" "preset=valgrind" valgrind_preset_match)
if(valgrind_preset_match EQUAL -1)
    message(FATAL_ERROR "Valgrind dry-run did not report preset=valgrind\nstdout:\n${valgrind_dry_run_stdout}")
endif()
string(FIND "${valgrind_dry_run_stdout}" "test=lc_unit_pouch_client" valgrind_test_match)
if(valgrind_test_match EQUAL -1)
    message(FATAL_ERROR "Valgrind dry-run did not include the pouch client unit target\nstdout:\n${valgrind_dry_run_stdout}")
endif()

execute_process(
    COMMAND make help
    WORKING_DIRECTORY "${LOCKDC_ROOT}"
    RESULT_VARIABLE help_result
    OUTPUT_VARIABLE help_stdout
    ERROR_VARIABLE help_stderr
)
if(NOT help_result EQUAL 0)
    message(FATAL_ERROR "make help failed\nstdout:\n${help_stdout}\nstderr:\n${help_stderr}")
endif()

foreach(target
        finalize-slice
        valgrind
        prerelease
        prerelease-live
        prerelease-hardening
        release-matrix)
    string(FIND "${help_stdout}" "make ${target}" help_match)
    if(help_match EQUAL -1)
        message(FATAL_ERROR "make help output did not include ${target}\nstdout:\n${help_stdout}")
    endif()
endforeach()

assert_contains(lifecycle_ledger "## Command Surface" "migration ledger command surface section")
assert_contains(lifecycle_ledger "make finalize-slice" "migration ledger finalize-slice entry")
assert_contains(lifecycle_ledger "make valgrind" "migration ledger valgrind entry")
assert_contains(lifecycle_ledger "## Decisions Still Required" "migration ledger decisions section")

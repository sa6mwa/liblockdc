if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

set(makefile_path "${LOCKDC_ROOT}/Makefile")
set(release_script_path "${LOCKDC_ROOT}/scripts/release.sh")
set(release_matrix_script_path "${LOCKDC_ROOT}/scripts/run_linux_release_matrix.sh")
set(ledger_path "${LOCKDC_ROOT}/docs/lifecycle-migration.md")

file(READ "${makefile_path}" root_makefile)
file(READ "${release_script_path}" release_script)
file(READ "${release_matrix_script_path}" release_matrix_script)
file(READ "${ledger_path}" lifecycle_ledger)

function(assert_contains haystack needle description)
    string(FIND "${${haystack}}" "${needle}" found_at)
    if(found_at EQUAL -1)
        message(FATAL_ERROR "missing ${description}")
    endif()
endfunction()

function(assert_not_contains haystack needle description)
    string(FIND "${${haystack}}" "${needle}" found_at)
    if(NOT found_at EQUAL -1)
        message(FATAL_ERROR "unexpected ${description}")
    endif()
endfunction()

foreach(obsolete_script
        scripts/dev-e2e.sh
        scripts/print-release-version.sh
        scripts/test_release_source.sh
        scripts/build_lockdc_lua_rock.sh
        scripts/validate_lockdc_luarocks.sh)
    if(EXISTS "${LOCKDC_ROOT}/${obsolete_script}")
        message(FATAL_ERROR "obsolete compatibility script still exists: ${obsolete_script}")
    endif()
endforeach()

foreach(target
        finalize-slice
        valgrind
        prerelease
        prerelease-live
        prerelease-hardening
        lifecycle-version-contract
        print-release-version
        verify-release-privacy
        fuzz-long
        bench
        bench-check
        benchmarks-go
        perf-gate
        dev-ps
        dev-logs
        release-lua-artifacts
        release-matrix)
    assert_contains(root_makefile "make ${target}" "make help entry for ${target}")
    assert_contains(root_makefile "${target}:" "make target ${target}")
endforeach()

assert_contains(root_makefile "bash ./scripts/valgrind.sh" "Valgrind runner wiring")
assert_contains(root_makefile "LOCKDC_PRERELEASE_LIVE=1" "live prerelease opt-in diagnostic")
assert_contains(root_makefile "__finalize-slice: __format __test-debug" "ordinary slice gate graph")
assert_contains(root_makefile "__test-all: __test-debug __test-host" "bounded test-all graph")
assert_contains(root_makefile "__prerelease: __release-pipeline" "deterministic prerelease graph")
assert_contains(root_makefile "__prerelease-hardening: __prerelease __fuzz __benchmark-pouch-go-parity-gate" "hardening prerelease graph")
assert_contains(root_makefile "__lifecycle-version-contract:" "lifecycle version contract target")
assert_contains(root_makefile "bash ./scripts/lifecycle-version-contract.sh" "lifecycle version contract runner")
assert_contains(root_makefile "print-release-version:" "Make-owned release version surface")
assert_contains(root_makefile "bash ./scripts/release_version.sh" "standard release version printer runner")
assert_contains(root_makefile "bash ./scripts/test-e2e.sh" "standard e2e runner")
assert_contains(root_makefile "bash ./scripts/test_release_from_source.sh" "standard source archive smoke runner")
assert_contains(root_makefile "bash ./scripts/verify_release_privacy.sh" "standard release privacy runner")
assert_contains(root_makefile "bash ./scripts/run_linux_release_matrix.sh" "standard release matrix runner")
assert_not_contains(root_makefile "build-asan:" "non-standard build-asan compatibility target")
assert_not_contains(root_makefile "test-asan:" "non-standard test-asan compatibility target")
assert_not_contains(root_makefile "asan:" "non-standard asan compatibility target")
assert_not_contains(root_makefile "__release-package-only" "obsolete release package-only internal target")
assert_contains(root_makefile "__release-pipeline: __finalize-slice __valgrind __fuzz-smoke __test-e2e __lua-test __bench-gate __release-matrix" "shared release pipeline graph")
assert_contains(release_script "run_step __lifecycle-version-contract\nrun_step __clean\nrun_step __release-pipeline" "release version contract, clean, shared pipeline order")
assert_contains(release_matrix_script "\"$make_bin\" __build-release" "standard release matrix build step")
assert_contains(release_matrix_script "\"$make_bin\" __test-host" "standard release matrix host test step")
assert_contains(release_matrix_script "bash \"$script_dir/cross_test.sh\" release" "standard release matrix cross test step")
assert_contains(release_matrix_script "bash \"$script_dir/run_linux_package_matrix.sh\"" "standard release matrix package step")

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
        lifecycle-version-contract
        print-release-version
        verify-release-privacy
        fuzz-long
        bench
        bench-check
        benchmarks-go
        perf-gate
        dev-ps
        dev-logs
        release-lua-artifacts
        release-matrix)
    string(FIND "${help_stdout}" "make ${target}" help_match)
    if(help_match EQUAL -1)
        message(FATAL_ERROR "make help output did not include ${target}\nstdout:\n${help_stdout}")
    endif()
endforeach()

assert_contains(lifecycle_ledger "## Command Surface" "migration ledger command surface section")
assert_contains(lifecycle_ledger "make finalize-slice" "migration ledger finalize-slice entry")
assert_contains(lifecycle_ledger "make valgrind" "migration ledger valgrind entry")
assert_contains(lifecycle_ledger "make lifecycle-version-contract" "migration ledger version contract entry")
assert_contains(lifecycle_ledger "## Decisions Still Required" "migration ledger decisions section")

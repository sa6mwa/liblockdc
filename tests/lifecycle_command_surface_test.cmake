if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

set(makefile_path "${LOCKDC_ROOT}/Makefile")
set(root_cmake_path "${LOCKDC_ROOT}/CMakeLists.txt")
set(release_script_path "${LOCKDC_ROOT}/scripts/release.sh")
set(release_matrix_script_path "${LOCKDC_ROOT}/scripts/run_linux_release_matrix.sh")
set(valgrind_script_path "${LOCKDC_ROOT}/scripts/valgrind.sh")
set(fuzz_script_path "${LOCKDC_ROOT}/scripts/fuzz.sh")
set(linux_build_matrix_script_path "${LOCKDC_ROOT}/scripts/run_linux_build_matrix.sh")
set(cross_test_script_path "${LOCKDC_ROOT}/scripts/cross_test.sh")
set(package_matrix_script_path "${LOCKDC_ROOT}/scripts/run_linux_package_matrix.sh")
set(source_smoke_script_path "${LOCKDC_ROOT}/scripts/test_release_from_source.sh")
set(clean_script_path "${LOCKDC_ROOT}/scripts/clean.sh")
set(e2e_script_path "${LOCKDC_ROOT}/scripts/test-e2e.sh")
set(ledger_path "${LOCKDC_ROOT}/docs/lifecycle-migration.md")

file(READ "${makefile_path}" root_makefile)
file(READ "${root_cmake_path}" root_cmake)
file(READ "${release_script_path}" release_script)
file(READ "${release_matrix_script_path}" release_matrix_script)
file(READ "${valgrind_script_path}" valgrind_script)
file(READ "${fuzz_script_path}" fuzz_script)
file(READ "${linux_build_matrix_script_path}" linux_build_matrix_script)
file(READ "${cross_test_script_path}" cross_test_script)
file(READ "${package_matrix_script_path}" package_matrix_script)
file(READ "${source_smoke_script_path}" source_smoke_script)
file(READ "${clean_script_path}" clean_script)
file(READ "${e2e_script_path}" e2e_script)
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
        scripts/test.sh
        scripts/build_lockdc_lua_rock.sh
        scripts/validate_lockdc_luarocks.sh
        scripts/package.sh
        scripts/package-verify.sh)
    if(EXISTS "${LOCKDC_ROOT}/${obsolete_script}")
        message(FATAL_ERROR "obsolete compatibility script still exists: ${obsolete_script}")
    endif()
endforeach()

foreach(target
        build-host
        test-install-tree
        example-smoke-local
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

foreach(script
        scripts/dev-logs.sh
        scripts/dev-ps.sh
        scripts/dev-reset.sh
        scripts/dev-up.sh
        scripts/dev-down.sh)
    if(NOT EXISTS "${LOCKDC_ROOT}/${script}")
        message(FATAL_ERROR "missing lifecycle runner: ${script}")
    endif()
endforeach()

assert_contains(root_makefile "bash ./scripts/valgrind.sh" "Valgrind runner wiring")
assert_contains(root_makefile ".NOTPARALLEL:" "serialized lifecycle command graph")
assert_contains(root_makefile "LOCKDC_PRERELEASE_LIVE=1" "live prerelease opt-in diagnostic")
assert_contains(root_makefile "MAKE_RECURSE := $(MAKE)" "dry-run-safe recursive Make variable")
assert_contains(root_makefile "$(TIMED) 'finalize-slice format' $(MAKE_RECURSE) __format" "ordinary slice format timing")
assert_contains(root_makefile "$(TIMED) 'finalize-slice test-debug' $(MAKE_RECURSE) __test-debug" "ordinary slice test timing")
assert_contains(root_makefile "__test-all: __test-debug __test-host __test-cross __valgrind __fuzz-smoke __test-e2e __bench-gate" "complete test-all graph")
assert_contains(root_makefile "$(TIMED) 'prerelease finalize-slice' $(MAKE_RECURSE) __finalize-slice" "ordinary prerelease slice timing")
assert_contains(root_makefile "$(TIMED) 'prerelease valgrind' $(MAKE_RECURSE) __valgrind" "ordinary prerelease Valgrind timing")
assert_contains(root_makefile "$(TIMED) 'prerelease fuzz-smoke' $(MAKE_RECURSE) __fuzz-smoke" "ordinary prerelease fuzz timing")
assert_contains(root_makefile "$(TIMED) 'prerelease e2e' $(MAKE_RECURSE) __test-e2e" "ordinary prerelease e2e timing")
assert_contains(root_makefile "$(TIMED) 'prerelease bench-gate' $(MAKE_RECURSE) __bench-gate" "ordinary prerelease benchmark timing")
assert_contains(root_makefile "__prerelease: __prerelease-ordinary" "deterministic prerelease graph")
assert_contains(root_makefile "$(TIMED) 'bench-gate benchmarks' $(MAKE_RECURSE) __benchmarks" "benchmark suite timing")
assert_contains(root_makefile "$(TIMED) 'bench-gate perf-gate' $(MAKE_RECURSE) __perf-gate" "performance gate timing")
assert_contains(root_makefile "$(TIMED) 'perf-gate pouch-go-parity' $(MAKE_RECURSE) __benchmark-pouch-go-parity-gate" "default parity benchmark timing")
assert_contains(root_makefile "$(TIMED) 'perf-gate pouch-go-durable-parity' $(MAKE_RECURSE) __benchmark-pouch-go-durable-gate" "durable parity benchmark timing")
assert_contains(valgrind_script "\"$timed_bin\" \"valgrind $test_name\"" "per-test Valgrind timing")
assert_contains(fuzz_script "\"$timed_bin\" \"fuzz $name\"" "per-target fuzz timing")
assert_contains(linux_build_matrix_script "\"$timed_bin\" \"release-matrix build $preset\"" "per-preset release build timing")
assert_contains(cross_test_script "\"$timed_bin\" \"release-matrix test $preset\"" "per-preset release test timing")
assert_contains(cross_test_script "-L cross-runtime" "curated QEMU runtime test selection")
assert_contains(package_matrix_script "release-matrix package source-smoke" "per-artifact package timing")
assert_contains(root_makefile "__prerelease-hardening: __prerelease __pouch-core-hardening __fuzz __release-matrix" "hardening prerelease graph")
assert_contains(root_makefile "pouch-core-hardening soak" "hardening core soak timing")
assert_contains(root_makefile "pouch-core-hardening reclaim" "hardening reclaim timing")
assert_contains(root_makefile "pouch-core-hardening shared-root" "hardening shared-root timing")
assert_contains(root_makefile "__lifecycle-version-contract:" "lifecycle version contract target")
assert_contains(root_makefile "bash ./scripts/lifecycle-version-contract.sh" "lifecycle version contract runner")
assert_contains(root_makefile "print-release-version:" "Make-owned release version surface")
assert_contains(root_makefile "bash ./scripts/release_version.sh" "standard release version printer runner")
assert_contains(root_makefile "bash ./scripts/test-e2e.sh" "standard e2e runner")
assert_contains(root_makefile "bash ./scripts/host_test.sh build" "standard host build runner")
assert_contains(root_makefile "'^install_tree_sdk_test$$'" "installed SDK test filter")
assert_contains(root_makefile "bash ./scripts/test-e2e.sh examples" "example smoke runner")
assert_contains(root_makefile "$(CTEST) --preset debug-lua" "standard Lua test preset runner")
assert_contains(root_makefile "bash ./scripts/test_release_from_source.sh" "standard source archive smoke runner")
assert_contains(root_makefile "bash ./scripts/verify_release_privacy.sh" "standard release privacy runner")
assert_contains(root_makefile "bash ./scripts/run_linux_release_matrix.sh" "standard release matrix runner")
assert_contains(root_makefile "__package-verify: __release-matrix __verify-release-privacy" "complete package verification graph")
assert_not_contains(root_makefile "build-asan:" "non-standard build-asan compatibility target")
assert_not_contains(root_makefile "test-asan:" "non-standard test-asan compatibility target")
assert_not_contains(root_makefile "asan:" "non-standard asan compatibility target")
assert_not_contains(root_makefile "__release-package-only" "obsolete release package-only internal target")
assert_contains(root_makefile "__release-pipeline: __prerelease __release-matrix" "complete release pipeline graph")
assert_contains(root_cmake "lifecycle-host" "target-independent lifecycle test label")
assert_contains(release_script "run_step __lifecycle-version-contract\nrun_step __clean\nrun_step __release-pipeline" "release version contract, clean, shared pipeline order")
assert_contains(release_script "run_timed.sh" "per-phase release timing")
string(REGEX MATCHALL "run_step __clean" release_clean_steps "${release_script}")
list(LENGTH release_clean_steps release_clean_step_count)
if(NOT release_clean_step_count EQUAL 1)
    message(FATAL_ERROR "release must invoke the repository clean exactly once, got ${release_clean_step_count}")
endif()
assert_contains(release_matrix_script "\"$make_bin\" __build-release" "standard release matrix build step")
assert_contains(release_matrix_script "x86_64-linux-gnu-release x86_64-linux-musl-release" "standard release matrix x86 test targets")
assert_contains(release_matrix_script "-LE lifecycle-host" "release matrix lifecycle-test exclusion")
assert_contains(source_smoke_script "host_target_id=x86_64-linux-gnu" "Bootlin source-smoke target")
assert_contains(source_smoke_script "-DCMAKE_TOOLCHAIN_FILE=\"$toolchain_file\"" "Bootlin source-smoke toolchain")
assert_contains(source_smoke_script "--target lc_unit_contracts" "bounded source-archive build target")
assert_contains(source_smoke_script "-R '^lc_unit_contracts$'" "bounded source-archive unit smoke")
assert_not_contains(source_smoke_script "detect_host_release_preset" "ambient source-smoke target detection")
assert_contains(release_matrix_script "bash \"$script_dir/cross_test.sh\" release" "standard release matrix cross test step")
assert_contains(release_matrix_script "bash \"$script_dir/run_linux_package_matrix.sh\"" "standard release matrix package step")
assert_contains(clean_script "remove_if_present \"$repo_root/.luarocks-build\"" "LuaRocks build state cleanup")
assert_contains(clean_script "lockdc_assert_generated_path" "generated-path cleanup guard")
assert_not_contains(e2e_script "test.sh" "deleted e2e dispatcher reference")
assert_contains(e2e_script "-L examples" "example-only e2e filter")
assert_contains(e2e_script "unset LD_LIBRARY_PATH" "isolated e2e runtime loader path")

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
assert_contains(valgrind_dry_run_stdout "test=lc_unit_pouch\n"
    "unified Pouch Valgrind target")
assert_not_contains(valgrind_dry_run_stdout "test=lc_unit_pouch_client"
    "retired Pouch client Valgrind target")

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
        build-host
        test-install-tree
        example-smoke-local
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
assert_contains(lifecycle_ledger "## Optional Extensions" "lifecycle ledger optional extensions section")

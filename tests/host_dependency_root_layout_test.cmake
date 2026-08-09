if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

file(READ "${LOCKDC_ROOT}/CMakePresets.json" presets_json)
file(READ "${LOCKDC_ROOT}/CMakeLists.txt" root_cmake)
file(READ "${LOCKDC_ROOT}/Makefile" root_makefile)
file(READ "${LOCKDC_ROOT}/scripts/deps.sh" deps_script)
file(READ "${LOCKDC_ROOT}/scripts/host_test.sh" host_test_script)

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

assert_not_contains(presets_json ".cache/deps/host-debug" "host-debug dependency root in presets")
assert_not_contains(presets_json ".cache/deps-build/host-debug" "host-debug dependency build root in presets")

assert_contains(root_cmake "set(LOCKDC_EXTERNAL_ROOT \"\${CMAKE_SOURCE_DIR}/.cache/deps/\${LOCKDC_TARGET_ID}\"" "target-aware default external root")
assert_contains(root_cmake "set(LOCKDC_DEPENDENCY_BUILD_ROOT \"\${CMAKE_SOURCE_DIR}/.cache/deps-build/\${LOCKDC_TARGET_ID}\"" "target-aware default dependency build root")
assert_contains(root_cmake "set(CPKT_DEPENDENCY_CACHE \"\${LOCKDC_DEFAULT_CPKT_DEPENDENCY_CACHE}\" CACHE PATH" "shared dependency archive cache variable")
assert_contains(root_cmake "if(DEFINED CPKT_DEPENDENCY_CACHE AND NOT \"\${CPKT_DEPENDENCY_CACHE}\" STREQUAL \"\")" "CMake cache dependency cache override precedence")
assert_contains(root_cmake "CPKT_DEPENDENCY_CACHE, XDG_CACHE_HOME, or HOME is required for the shared verified dependency archive cache" "actionable missing dependency cache environment error")
assert_not_contains(root_cmake [=[${CMAKE_SOURCE_DIR}/.cache/c.pkt.systems/deps]=] "cleanable repository-local dependency cache fallback")
assert_contains(root_cmake "set(LOCKDC_DOWNLOAD_ROOT \"\${CMAKE_SOURCE_DIR}/.cache/downloads\"" "repo-local download staging root")
assert_contains(root_cmake "set(LOCKDC_DEPENDENCY_BUILD_TYPE \"Release\")" "release-only dependency build type")
assert_contains(root_makefile "__deps-debug:\n\tbash ./scripts/deps.sh deps-x86_64-linux-gnu" "debug dependency target uses pinned Bootlin target")
assert_contains(root_makefile "__test-host:\n\tbash ./scripts/host_test.sh" "host test target delegates to host_test.sh")
assert_not_contains(deps_script "deps-host-debug" "retired ambient-host dependency alias")
assert_not_contains(deps_script "resolve_host_debug_preset" "ambient-host dependency resolver")
assert_contains(deps_script "deps-x86_64-linux-gnu" "x86_64 GNU dependency mapping")
assert_contains(deps_script "deps-x86_64-linux-musl" "x86_64 musl dependency mapping")
assert_contains(deps_script "LOCKDC_DEPS_DRY_RUN" "deps dry-run support")
assert_contains(deps_script "acquire_verified_archive()" "shared verified archive acquisition wrapper")
assert_contains(deps_script "CPKT_DEPENDENCY_CACHE" "shared dependency archive cache support")
assert_contains(deps_script [=["$repo_root/scripts/cpkt-toolchains.sh" ensure "$dependency_target_id"]=] "Bootlin dependency toolchain provisioning")
assert_contains(deps_script [=["$repo_root/scripts/cpkt-toolchains.sh" discover "$dependency_target_id"]=] "Bootlin dependency toolchain discovery")
assert_contains(deps_script "toolchain_target_id=$dependency_target_id" "dependency manifest target toolchain identity")
assert_contains(deps_script "toolchain_source=$toolchain_source" "dependency manifest toolchain source")
assert_contains(deps_script "toolchain_archive=$toolchain_archive" "dependency manifest toolchain archive")
assert_contains(deps_script "toolchain_root=$toolchain_root" "dependency manifest toolchain root")
assert_contains(deps_script "toolchain_sysroot=$toolchain_sysroot" "dependency manifest toolchain sysroot")
assert_contains(deps_script "toolchain_target_triple=$toolchain_target_triple" "dependency manifest toolchain target triple")
assert_contains(deps_script [=[lonejson_abi_version=${LOCKDC_LONEJSON_ABI_VERSION:-25}]=] "lonejson ABI readiness check knob")
assert_contains(deps_script [=[liblql_abi_version=${LOCKDC_LIBLQL_ABI_VERSION:-0}]=] "liblql ABI readiness check knob")
assert_contains(deps_script [=[liblonejson.${lonejson_abi_version}.${shared_ext}]=] "Darwin lonejson ABI readiness path")
assert_contains(deps_script [=[liblonejson.so.${lonejson_abi_version}]=] "Linux lonejson ABI readiness path")
assert_contains(deps_script [=[liblql.${liblql_abi_version}.${shared_ext}]=] "Darwin liblql ABI readiness path")
assert_contains(deps_script [=[liblql.so.${liblql_abi_version}]=] "Linux liblql ABI readiness path")
assert_not_contains(deps_script "liblonejson.4.${shared_ext}" "stale Darwin lonejson ABI readiness path")
assert_not_contains(deps_script "liblonejson.so.4" "stale Linux lonejson ABI readiness path")
assert_contains(host_test_script "LOCKDC_HOST_TEST_DRY_RUN" "host test dry-run support")
assert_contains(host_test_script "deps-x86_64-linux-gnu" "host test provisions Bootlin GNU dependencies")
assert_contains(host_test_script "deps-x86_64-linux-musl" "host test provisions Bootlin musl dependencies")
assert_contains(host_test_script "x86_64-linux-gnu-release" "host test covers Bootlin GNU release")
assert_contains(host_test_script "x86_64-linux-musl-release" "host test covers Bootlin musl release")
assert_contains(host_test_script "-LE lifecycle-host" "host test excludes target-independent lifecycle tests")
assert_not_contains(host_test_script "resolve_host_arch" "ambient-host architecture resolver")
assert_not_contains(host_test_script "have_native_musl_toolchain" "ambient-host musl resolver")
assert_contains(root_cmake [=[list(APPEND LOCKDC_C_TEST_ENVIRONMENT "LOCKDC_SLOW_TEST_RUNTIME=1")]=] "C test cross slow-runtime environment")
assert_contains(root_cmake [=[list(APPEND LOCKDC_C_TEST_ENVIRONMENT "LD_LIBRARY_PATH=${lockdc_c_test_runtime_path}")]=] "C test Bootlin runtime environment")
assert_contains(root_cmake "-print-file-name=libatomic.so.1" "C test libatomic runtime discovery")
assert_contains(root_cmake [=[set_tests_properties(lockdc_bench_help PROPERTIES
                ENVIRONMENT "${LOCKDC_C_TEST_ENVIRONMENT}")]=] "benchmark help test Bootlin runtime environment")

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        "PATH=/usr/bin:/bin"
        "LOCKDC_HOST_TEST_DRY_RUN=1"
        /bin/bash "${LOCKDC_ROOT}/scripts/host_test.sh"
    WORKING_DIRECTORY "${LOCKDC_ROOT}"
    RESULT_VARIABLE dry_run_result
    OUTPUT_VARIABLE dry_run_stdout
    ERROR_VARIABLE dry_run_stderr
)
if(NOT dry_run_result EQUAL 0)
    message(FATAL_ERROR
        "host_test dry-run failed\nstdout:\n${dry_run_stdout}\nstderr:\n${dry_run_stderr}")
endif()
set(expected_host_test_stdout [=[deps_preset=deps-x86_64-linux-gnu
deps_preset=deps-x86_64-linux-musl
preset=x86_64-linux-gnu-release
preset=x86_64-linux-musl-release
]=])
if(NOT "${dry_run_stdout}" STREQUAL "${expected_host_test_stdout}")
    message(FATAL_ERROR
        "host_test did not use the fixed Bootlin host matrix\n"
        "expected:\n${expected_host_test_stdout}\nactual:\n${dry_run_stdout}")
endif()

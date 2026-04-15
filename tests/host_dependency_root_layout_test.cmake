if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

file(READ "${LOCKDC_ROOT}/CMakePresets.json" presets_json)
file(READ "${LOCKDC_ROOT}/CMakeLists.txt" root_cmake)
file(READ "${LOCKDC_ROOT}/Makefile" root_makefile)
file(READ "${LOCKDC_ROOT}/scripts/deps.sh" deps_script)
set(fake_compiler_dir "${LOCKDC_ROOT}/build/test-host-root-layout-fake-bin")

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
assert_contains(root_cmake "set(LOCKDC_DOWNLOAD_ROOT \"\${CMAKE_SOURCE_DIR}/.cache/downloads\"" "shared download root")
assert_contains(root_cmake "set(LOCKDC_DEPENDENCY_BUILD_TYPE \"Release\")" "release-only dependency build type")
assert_contains(root_makefile "__deps-debug:\n\tbash ./scripts/deps.sh deps-host-debug" "debug dependency target uses host-native alias")
assert_contains(deps_script "resolve_host_debug_preset()" "host-native dependency preset resolver")
assert_contains(deps_script "deps-aarch64-linux-gnu" "aarch64 host-native dependency mapping")
assert_contains(deps_script "deps-armhf-linux-gnu" "armhf host-native dependency mapping")
assert_contains(deps_script "deps-x86_64-linux-gnu" "x86_64 host-native dependency mapping")
assert_contains(deps_script "LOCKDC_DEPS_DRY_RUN" "deps dry-run support")

file(REMOVE_RECURSE "${fake_compiler_dir}")
file(MAKE_DIRECTORY "${fake_compiler_dir}")

function(assert_host_debug_resolution triple expected_preset)
    set(fake_compiler "${fake_compiler_dir}/cc-${expected_preset}.sh")
    file(WRITE "${fake_compiler}" "#!/usr/bin/env bash\nif [ \"$1\" = \"-dumpmachine\" ]; then\n  printf '%s\\n' \"${triple}\"\n  exit 0\nfi\nprintf 'unexpected compiler invocation: %s\\n' \"$*\" >&2\nexit 1\n")
    execute_process(COMMAND chmod +x "${fake_compiler}")
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E env
            "CC=${fake_compiler}"
            "LOCKDC_DEPS_DRY_RUN=1"
            bash "${LOCKDC_ROOT}/scripts/deps.sh" deps-host-debug
        WORKING_DIRECTORY "${LOCKDC_ROOT}"
        RESULT_VARIABLE dry_run_result
        OUTPUT_VARIABLE dry_run_stdout
        ERROR_VARIABLE dry_run_stderr
    )
    if(NOT dry_run_result EQUAL 0)
        message(FATAL_ERROR
            "deps-host-debug dry-run failed for ${triple}\nstdout:\n${dry_run_stdout}\nstderr:\n${dry_run_stderr}")
    endif()
    string(FIND "${dry_run_stdout}" "preset=${expected_preset}" preset_match)
    if(preset_match EQUAL -1)
        message(FATAL_ERROR
            "deps-host-debug resolved wrong preset for ${triple}\nstdout:\n${dry_run_stdout}\nstderr:\n${dry_run_stderr}")
    endif()
endfunction()

assert_host_debug_resolution("x86_64-linux-gnu" "deps-x86_64-linux-gnu")
assert_host_debug_resolution("aarch64-unknown-linux-gnu" "deps-aarch64-linux-gnu")
assert_host_debug_resolution("armv7l-unknown-linux-gnueabihf" "deps-armhf-linux-gnu")

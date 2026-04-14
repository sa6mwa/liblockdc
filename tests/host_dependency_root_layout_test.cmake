if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

file(READ "${LOCKDC_ROOT}/CMakePresets.json" presets_json)
file(READ "${LOCKDC_ROOT}/CMakeLists.txt" root_cmake)
file(READ "${LOCKDC_ROOT}/Makefile" root_makefile)
file(READ "${LOCKDC_ROOT}/scripts/deps.sh" deps_script)
set(target_cache_dir "${LOCKDC_ROOT}/build/test-host-root-layout")

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

file(REMOVE_RECURSE "${target_cache_dir}")
execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -S "${LOCKDC_ROOT}"
        -B "${target_cache_dir}"
        -G Ninja
        -DLOCKDC_BUILD_TESTS=OFF
        -DLOCKDC_BUILD_EXAMPLES=OFF
        -DLOCKDC_BUILD_BENCHMARKS=OFF
        -DLOCKDC_BUILD_FUZZERS=OFF
        -DLOCKDC_TARGET_ARCH=aarch64
        -DLOCKDC_TARGET_OS=linux
        -DLOCKDC_TARGET_LIBC=gnu
    RESULT_VARIABLE configure_result
    OUTPUT_VARIABLE configure_stdout
    ERROR_VARIABLE configure_stderr
)
if(NOT configure_result EQUAL 0)
    message(FATAL_ERROR
        "target-aware configure failed\nstdout:\n${configure_stdout}\nstderr:\n${configure_stderr}")
endif()

file(READ "${target_cache_dir}/CMakeCache.txt" target_cache)
assert_contains(target_cache "LOCKDC_EXTERNAL_ROOT:PATH=${LOCKDC_ROOT}/.cache/deps/aarch64-linux-gnu" "configured aarch64 external root")
assert_contains(target_cache "LOCKDC_DEPENDENCY_BUILD_ROOT:PATH=${LOCKDC_ROOT}/.cache/deps-build/aarch64-linux-gnu" "configured aarch64 dependency build root")

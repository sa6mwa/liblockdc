if(NOT DEFINED LOCKDC_ROOT OR LOCKDC_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()
if(NOT DEFINED LOCKDC_BINARY_DIR OR LOCKDC_BINARY_DIR STREQUAL "")
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

set(resolver "${LOCKDC_ROOT}/scripts/cpkt-toolchains.sh")
if(NOT EXISTS "${resolver}")
    message(FATAL_ERROR "missing Bootlin resolver: ${resolver}")
endif()

execute_process(
    COMMAND bash -n "${resolver}"
    RESULT_VARIABLE syntax_result
    ERROR_VARIABLE syntax_error)
if(NOT syntax_result EQUAL 0)
    message(FATAL_ERROR "Bootlin resolver shell syntax failed\n${syntax_error}")
endif()

set(test_root "${LOCKDC_BINARY_DIR}/bootlin-toolchain-contract")
set(cache_root "${test_root}/cache")
file(REMOVE_RECURSE "${test_root}")

function(write_fake_tool path body)
    get_filename_component(parent "${path}" DIRECTORY)
    file(MAKE_DIRECTORY "${parent}")
    file(WRITE "${path}" "${body}")
    file(CHMOD "${path}"
        PERMISSIONS
            OWNER_READ OWNER_WRITE OWNER_EXECUTE
            GROUP_READ GROUP_EXECUTE
            WORLD_READ WORLD_EXECUTE)
endfunction()

function(create_fake_bootlin_root target_id root_name prefix sysroot_rel)
    set(toolchain_root "${cache_root}/roots/${root_name}")
    set(sysroot "${toolchain_root}/${sysroot_rel}")
    set(bin_dir "${toolchain_root}/bin")
    set(runtime_dir "${toolchain_root}/lib/gcc/${target_id}")
    file(MAKE_DIRECTORY
        "${bin_dir}"
        "${sysroot}/usr/include"
        "${sysroot}/usr/lib"
        "${runtime_dir}")
    file(WRITE "${sysroot}/usr/include/stdio.h" "")
    file(WRITE "${sysroot}/usr/lib/libc.so" "")
    file(WRITE "${runtime_dir}/libstdc++.a" "")
    file(WRITE "${runtime_dir}/libgcc.a" "")

    foreach(tool
            gcc
            ld
            ar
            ranlib
            strip
            nm
            objcopy
            objdump
            addr2line
            gdb
            readelf)
        write_fake_tool("${bin_dir}/${prefix}-${tool}" "#!/usr/bin/env bash\nexit 0\n")
    endforeach()
    write_fake_tool("${bin_dir}/${prefix}-g++"
        "#!/usr/bin/env bash\ncase \"$1\" in\n  -print-file-name=libstdc++.a) printf '%s\\n' '${runtime_dir}/libstdc++.a' ;;\n  -print-file-name=libgcc.a) printf '%s\\n' '${runtime_dir}/libgcc.a' ;;\n  *) exit 0 ;;\nesac\n")

    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E env
            "CPKT_TOOLCHAIN_CACHE=${cache_root}"
            "${resolver}" discover "${target_id}"
        RESULT_VARIABLE discover_result
        OUTPUT_VARIABLE discover_output
        ERROR_VARIABLE discover_error)
    if(NOT discover_result EQUAL 0)
        message(FATAL_ERROR "Bootlin resolver discover failed for ${target_id}\n${discover_error}")
    endif()
    if(NOT discover_output MATCHES "(^|\n)status=ready(\n|$)")
        message(FATAL_ERROR "fake Bootlin cache was not reported ready for ${target_id}\n${discover_output}")
    endif()
endfunction()

function(assert_bootlin_toolchain_import target_id prefix sysroot_rel)
    set(toolchain_file "${LOCKDC_ROOT}/cmake/toolchains/${target_id}.cmake")
    set(toolchain_root "${cache_root}/roots/${ARGN}")
    set(sysroot "${toolchain_root}/${sysroot_rel}")
    set(bin_dir "${toolchain_root}/bin")
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E env
            "CPKT_TOOLCHAIN_CACHE=${cache_root}"
            "${CMAKE_COMMAND}"
                -DLOCKDC_TOOLCHAIN_FILE=${toolchain_file}
                -DLOCKDC_EXPECTED_CC=${bin_dir}/${prefix}-gcc
                -DLOCKDC_EXPECTED_LINKER=${bin_dir}/${prefix}-ld
                -DLOCKDC_EXPECTED_AR=${bin_dir}/${prefix}-ar
                -DLOCKDC_EXPECTED_RANLIB=${bin_dir}/${prefix}-ranlib
                -DLOCKDC_EXPECTED_SYSROOT=${sysroot}
                -P "${LOCKDC_ROOT}/tests/bootlin_toolchain_import_assert.cmake"
        RESULT_VARIABLE import_result
        OUTPUT_VARIABLE import_output
        ERROR_VARIABLE import_error)
    if(NOT import_result EQUAL 0)
        message(FATAL_ERROR
            "Bootlin toolchain import failed for ${target_id}\nstdout:\n${import_output}\nstderr:\n${import_error}")
    endif()
endfunction()

create_fake_bootlin_root(
    x86_64-linux-gnu
    x86-64--glibc--stable-2026.08-1
    x86_64-linux
    x86_64-buildroot-linux-gnu/sysroot)
create_fake_bootlin_root(
    x86_64-linux-musl
    x86-64--musl--stable-2026.08-1
    x86_64-linux
    x86_64-buildroot-linux-musl/sysroot)
create_fake_bootlin_root(
    aarch64-linux-gnu
    aarch64--glibc--stable-2026.08-1
    aarch64-linux
    aarch64-buildroot-linux-gnu/sysroot)
create_fake_bootlin_root(
    aarch64-linux-musl
    aarch64--musl--stable-2026.08-1
    aarch64-linux
    aarch64-buildroot-linux-musl/sysroot)
create_fake_bootlin_root(
    armhf-linux-gnu
    armv7-eabihf--glibc--stable-2026.08-1
    arm-linux
    arm-buildroot-linux-gnueabihf/sysroot)
create_fake_bootlin_root(
    armhf-linux-musl
    armv7-eabihf--musl--stable-2026.08-1
    arm-linux
    arm-buildroot-linux-musleabihf/sysroot)

assert_bootlin_toolchain_import(
    x86_64-linux-gnu
    x86_64-linux
    x86_64-buildroot-linux-gnu/sysroot
    x86-64--glibc--stable-2026.08-1)
assert_bootlin_toolchain_import(
    x86_64-linux-musl
    x86_64-linux
    x86_64-buildroot-linux-musl/sysroot
    x86-64--musl--stable-2026.08-1)
assert_bootlin_toolchain_import(
    aarch64-linux-gnu
    aarch64-linux
    aarch64-buildroot-linux-gnu/sysroot
    aarch64--glibc--stable-2026.08-1)
assert_bootlin_toolchain_import(
    aarch64-linux-musl
    aarch64-linux
    aarch64-buildroot-linux-musl/sysroot
    aarch64--musl--stable-2026.08-1)
assert_bootlin_toolchain_import(
    armhf-linux-gnu
    arm-linux
    arm-buildroot-linux-gnueabihf/sysroot
    armv7-eabihf--glibc--stable-2026.08-1)
assert_bootlin_toolchain_import(
    armhf-linux-musl
    arm-linux
    arm-buildroot-linux-musleabihf/sysroot
    armv7-eabihf--musl--stable-2026.08-1)

# Exercise Darwin discovery without Apple tools, downloads, or host cache state.
set(osxcross_root "${test_root}/osxcross")
foreach(sdk_version 16 26)
    file(MAKE_DIRECTORY "${osxcross_root}/SDK/MacOSX${sdk_version}.sdk/usr/include")
endforeach()
foreach(prefix arm64-apple-darwin25.4 arm64-apple-darwin25.10)
    foreach(tool clang clang++ ld ar ranlib strip nm otool install_name_tool)
        write_fake_tool("${osxcross_root}/bin/${prefix}-${tool}" "#!/bin/sh\nexit 0\n")
    endforeach()
endforeach()
# A newer but incomplete collection must not displace the complete one.
write_fake_tool("${osxcross_root}/bin/arm64-apple-darwin25.20-clang" "#!/bin/sh\nexit 0\n")
set(mig_root "${cache_root}/roots/host-mig-puredarwin-88753c478c97b9a08bcdb66cecc68ba5881ff3af-x86_64-linux-gnu")
set(darwin_assert "${test_root}/darwin-import-assert.cmake")
file(WRITE "${darwin_assert}" [=[
include("${LOCKDC_ROOT}/cmake/toolchains/arm64-apple-darwin.cmake")
foreach(pair "CMAKE_C_COMPILER|clang" "CMAKE_CXX_COMPILER|clang++"
             "CMAKE_LINKER|ld" "CMAKE_AR|ar" "CMAKE_RANLIB|ranlib"
             "CMAKE_NM|nm" "CMAKE_STRIP|strip"
             "CMAKE_INSTALL_NAME_TOOL|install_name_tool" "LOCKDC_OTOOL|otool")
    string(REPLACE "|" ";" parts "${pair}")
    list(GET parts 0 variable)
    list(GET parts 1 suffix)
    if(NOT "${${variable}}" STREQUAL "$ENV{OSXCROSS_ROOT}/bin/${EXPECTED_PREFIX}-${suffix}")
        message(FATAL_ERROR "incorrect Darwin ${variable}: ${${variable}}")
    endif()
endforeach()
if(NOT CMAKE_OSX_SYSROOT STREQUAL "$ENV{OSXCROSS_ROOT}/SDK/MacOSX26.sdk")
    message(FATAL_ERROR "incorrect Darwin SDK selection: ${CMAKE_OSX_SYSROOT}")
endif()
if(NOT LOCKDC_HOST_MIG STREQUAL "${EXPECTED_MIG_ROOT}/bin/mig" OR
   NOT LOCKDC_HOST_MIGCOM STREQUAL "${EXPECTED_MIG_ROOT}/libexec/migcom" OR
   NOT LOCKDC_HOST_MIG_REVISION STREQUAL "88753c478c97b9a08bcdb66cecc68ba5881ff3af")
    message(FATAL_ERROR "incorrect Darwin host MIG selection")
endif()
]=])

function(assert_darwin_tools pin expected_prefix expect_ready)
    set(darwin_env "${CMAKE_COMMAND}" -E env
        "CPKT_TOOLCHAIN_CACHE=${cache_root}"
        "OSXCROSS_ROOT=${osxcross_root}"
        "CPKT_OSXCROSS_HOST=${pin}"
        "LOCKDC_OSXCROSS_HOST=")
    execute_process(COMMAND ${darwin_env} bash "${LOCKDC_ROOT}/scripts/osxcross_available.sh"
        RESULT_VARIABLE available_result)
    if(NOT available_result EQUAL 0)
        message(FATAL_ERROR "complete osxcross collection was incorrectly skipped for pin ${pin}")
    endif()
    execute_process(COMMAND ${darwin_env} "${CMAKE_COMMAND}"
        "-DLOCKDC_ROOT=${LOCKDC_ROOT}" "-DEXPECTED_PREFIX=${expected_prefix}"
        "-DEXPECTED_MIG_ROOT=${mig_root}" ${ARGN} -P "${darwin_assert}"
        RESULT_VARIABLE import_result OUTPUT_VARIABLE import_output ERROR_VARIABLE import_error)
    if(expect_ready AND NOT import_result EQUAL 0)
        message(FATAL_ERROR "Darwin import failed: ${import_output}\n${import_error}")
    elseif(NOT expect_ready)
        if(import_result EQUAL 0 OR NOT import_error MATCHES "pinned host MIG are not ready")
            message(FATAL_ERROR "Darwin configure accepted a missing host MIG: ${import_error}")
        endif()
    endif()
endfunction()

assert_darwin_tools("" arm64-apple-darwin25.10 FALSE)
foreach(path bin/mig bin/mig-upstream libexec/migcom)
    write_fake_tool("${mig_root}/${path}" "#!/bin/sh\nexit 0\n")
endforeach()
file(WRITE "${mig_root}/TOOLCHAIN" "component=host-mig\n")
assert_darwin_tools("" arm64-apple-darwin25.10 TRUE)
assert_darwin_tools(arm64-apple-darwin25.4 arm64-apple-darwin25.4 TRUE)
assert_darwin_tools(arm64-apple-darwin25.10 arm64-apple-darwin25.4 TRUE
    -DLOCKDC_OSXCROSS_HOST=arm64-apple-darwin25.4)
file(REMOVE "${osxcross_root}/bin/arm64-apple-darwin25.10-install_name_tool")
assert_darwin_tools("" arm64-apple-darwin25.4 TRUE)
execute_process(COMMAND "${CMAKE_COMMAND}" -E env
    "CPKT_TOOLCHAIN_CACHE=${cache_root}" "OSXCROSS_ROOT=${osxcross_root}"
    "CPKT_OSXCROSS_HOST=arm64-apple-darwin25.10"
    bash "${LOCKDC_ROOT}/scripts/osxcross_available.sh"
    RESULT_VARIABLE missing_packaging_tool_result)
if(missing_packaging_tool_result EQUAL 0)
    message(FATAL_ERROR "Darwin pin missing install_name_tool was accepted")
endif()
execute_process(COMMAND "${CMAKE_COMMAND}" -E env
    "CPKT_TOOLCHAIN_CACHE=${cache_root}" "OSXCROSS_ROOT=${osxcross_root}"
    "CPKT_OSXCROSS_HOST=arm64-apple-darwin25.20"
    bash "${LOCKDC_ROOT}/scripts/osxcross_available.sh"
    RESULT_VARIABLE incomplete_result)
if(incomplete_result EQUAL 0)
    message(FATAL_ERROR "incomplete pinned osxcross collection was accepted")
endif()
file(REMOVE_RECURSE "${osxcross_root}/SDK")
execute_process(COMMAND "${CMAKE_COMMAND}" -E env
    "CPKT_TOOLCHAIN_CACHE=${cache_root}" "OSXCROSS_ROOT=${osxcross_root}"
    "CPKT_OSXCROSS_HOST=" "LOCKDC_OSXCROSS_HOST="
    bash "${LOCKDC_ROOT}/scripts/osxcross_available.sh"
    RESULT_VARIABLE missing_sdk_result)
if(missing_sdk_result EQUAL 0)
    message(FATAL_ERROR "osxcross without an SDK was accepted")
endif()

# Prove ordinary fuzz entry points prepare AFL++ before CMake discovery.
set(fuzz_fixture "${test_root}/fuzz-bootstrap")
set(fuzz_log "${fuzz_fixture}/calls.log")
set(fuzz_ready "${fuzz_fixture}/afl-ready")
file(MAKE_DIRECTORY "${fuzz_fixture}/scripts" "${fuzz_fixture}/bin")
file(COPY "${LOCKDC_ROOT}/scripts/build.sh" DESTINATION "${fuzz_fixture}/scripts")
write_fake_tool("${fuzz_fixture}/scripts/cpkt-aflpp.sh" [=[#!/bin/sh
set -eu
[ "$1" = ensure ]
printf 'ensure\n' >> "$LOCKDC_FUZZ_LOG"
[ "${LOCKDC_FAIL_ENSURE:-0}" != 1 ] || exit 19
: > "$LOCKDC_FUZZ_READY"
]=])
write_fake_tool("${fuzz_fixture}/bin/cmake" [=[#!/bin/sh
set -eu
[ -f "$LOCKDC_FUZZ_READY" ] || exit 23
printf 'cmake %s\n' "$*" >> "$LOCKDC_FUZZ_LOG"
]=])
set(fuzz_env "${CMAKE_COMMAND}" -E env
    "PATH=${fuzz_fixture}/bin:$ENV{PATH}"
    "LOCKDC_FUZZ_LOG=${fuzz_log}" "LOCKDC_FUZZ_READY=${fuzz_ready}"
    "LOCKDC_FAIL_ENSURE=0")
execute_process(COMMAND ${fuzz_env} bash "${fuzz_fixture}/scripts/build.sh" fuzz lc_fuzz_streams
    WORKING_DIRECTORY "${fuzz_fixture}" RESULT_VARIABLE fuzz_build_result)
file(READ "${fuzz_log}" fuzz_calls)
if(NOT fuzz_build_result EQUAL 0 OR NOT fuzz_calls STREQUAL
   "ensure\ncmake --preset fuzz\ncmake --build --preset fuzz --target lc_fuzz_streams\n")
    message(FATAL_ERROR "fuzz build did not prepare before configure: ${fuzz_calls}")
endif()
file(REMOVE "${fuzz_log}" "${fuzz_ready}")
find_program(lockdc_test_make NAMES make REQUIRED)
execute_process(COMMAND ${fuzz_env} "${lockdc_test_make}" -f "${LOCKDC_ROOT}/Makefile"
    -o __deps-debug __build-fuzz "CMAKE=${fuzz_fixture}/bin/cmake"
    WORKING_DIRECTORY "${fuzz_fixture}" RESULT_VARIABLE fuzz_make_result
    OUTPUT_VARIABLE fuzz_make_output ERROR_VARIABLE fuzz_make_error)
file(READ "${fuzz_log}" fuzz_calls)
if(NOT fuzz_make_result EQUAL 0 OR NOT fuzz_calls STREQUAL
   "ensure\ncmake --preset fuzz\ncmake --build --preset fuzz\n")
    message(FATAL_ERROR "Make fuzz build did not prepare before configure: ${fuzz_calls}\n${fuzz_make_output}\n${fuzz_make_error}")
endif()
file(REMOVE "${fuzz_log}" "${fuzz_ready}")
execute_process(COMMAND ${fuzz_env} "${CMAKE_COMMAND}" -E env LOCKDC_FAIL_ENSURE=1
    bash "${fuzz_fixture}/scripts/build.sh" fuzz
    WORKING_DIRECTORY "${fuzz_fixture}" RESULT_VARIABLE failed_fuzz_build_result)
file(READ "${fuzz_log}" fuzz_calls)
if(failed_fuzz_build_result EQUAL 0 OR NOT fuzz_calls STREQUAL "ensure\n")
    message(FATAL_ERROR "fuzz build configured after failed preparation: ${fuzz_calls}")
endif()
file(REMOVE "${fuzz_log}" "${fuzz_ready}")
execute_process(COMMAND ${fuzz_env} "${CMAKE_COMMAND}" -E env LOCKDC_FAIL_ENSURE=1
    "${lockdc_test_make}" -f "${LOCKDC_ROOT}/Makefile"
    -o __deps-debug __build-fuzz "CMAKE=${fuzz_fixture}/bin/cmake"
    WORKING_DIRECTORY "${fuzz_fixture}" RESULT_VARIABLE failed_fuzz_make_result
    OUTPUT_QUIET ERROR_QUIET)
file(READ "${fuzz_log}" fuzz_calls)
if(failed_fuzz_make_result EQUAL 0 OR NOT fuzz_calls STREQUAL "ensure\n")
    message(FATAL_ERROR "Make fuzz build configured after failed preparation: ${fuzz_calls}")
endif()

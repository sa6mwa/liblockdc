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

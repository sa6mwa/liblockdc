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
set(toolchain_root "${cache_root}/roots/x86-64--glibc--stable-2025.08-1")
set(sysroot "${toolchain_root}/x86_64-buildroot-linux-gnu/sysroot")
set(bin_dir "${toolchain_root}/bin")
file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY "${bin_dir}" "${sysroot}/usr/include" "${sysroot}/usr/lib" "${toolchain_root}/lib/gcc")
file(WRITE "${sysroot}/usr/include/stdio.h" "")
file(WRITE "${sysroot}/usr/lib/libc.so" "")
file(WRITE "${toolchain_root}/lib/gcc/libstdc++.a" "")
file(WRITE "${toolchain_root}/lib/gcc/libgcc.a" "")

function(write_fake_tool name)
    set(path "${bin_dir}/${name}")
    if(name STREQUAL "x86_64-linux-g++")
        file(WRITE "${path}" "#!/usr/bin/env bash\ncase \"$1\" in\n  -print-file-name=libstdc++.a) printf '%s\\n' '${toolchain_root}/lib/gcc/libstdc++.a' ;;\n  -print-file-name=libgcc.a) printf '%s\\n' '${toolchain_root}/lib/gcc/libgcc.a' ;;\n  *) exit 0 ;;\nesac\n")
    else()
        file(WRITE "${path}" "#!/usr/bin/env bash\nexit 0\n")
    endif()
    file(CHMOD "${path}"
        PERMISSIONS
            OWNER_READ OWNER_WRITE OWNER_EXECUTE
            GROUP_READ GROUP_EXECUTE
            WORLD_READ WORLD_EXECUTE)
endfunction()

foreach(tool
        gcc
        g++
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
    write_fake_tool("x86_64-linux-${tool}")
endforeach()

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        "CPKT_TOOLCHAIN_CACHE=${cache_root}"
        "${resolver}" discover x86_64-linux-gnu
    RESULT_VARIABLE discover_result
    OUTPUT_VARIABLE discover_output
    ERROR_VARIABLE discover_error)
if(NOT discover_result EQUAL 0)
    message(FATAL_ERROR "Bootlin resolver discover failed\n${discover_error}")
endif()
if(NOT discover_output MATCHES "(^|\n)status=ready(\n|$)")
    message(FATAL_ERROR "fake Bootlin cache was not reported ready\n${discover_output}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        "CPKT_TOOLCHAIN_CACHE=${cache_root}"
        "${CMAKE_COMMAND}"
            -DLOCKDC_TOOLCHAIN_FILE=${LOCKDC_ROOT}/cmake/toolchains/x86_64-linux-gnu.cmake
            -DLOCKDC_EXPECTED_CC=${bin_dir}/x86_64-linux-gcc
            -DLOCKDC_EXPECTED_AR=${bin_dir}/x86_64-linux-ar
            -DLOCKDC_EXPECTED_SYSROOT=${sysroot}
            -P "${LOCKDC_ROOT}/tests/bootlin_toolchain_import_assert.cmake"
    RESULT_VARIABLE import_result
    OUTPUT_VARIABLE import_output
    ERROR_VARIABLE import_error)
if(NOT import_result EQUAL 0)
    message(FATAL_ERROR
        "Bootlin toolchain import failed\nstdout:\n${import_output}\nstderr:\n${import_error}")
endif()

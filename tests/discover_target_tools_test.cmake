if(NOT DEFINED LOCKDC_ROOT OR LOCKDC_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()
if(NOT DEFINED LOCKDC_BINARY_DIR OR LOCKDC_BINARY_DIR STREQUAL "")
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

set(discover_script "${LOCKDC_ROOT}/scripts/discover_target_tools.sh")
if(NOT EXISTS "${discover_script}")
    message(FATAL_ERROR "missing target-tool discovery helper: ${discover_script}")
endif()

set(test_root "${LOCKDC_BINARY_DIR}/discover-target-tools-test")
file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY "${test_root}")

function(write_fake_tool path)
    get_filename_component(parent "${path}" DIRECTORY)
    file(MAKE_DIRECTORY "${parent}")
    file(WRITE "${path}" "#!/bin/sh\nexit 0\n")
    file(CHMOD "${path}"
        PERMISSIONS
            OWNER_READ OWNER_WRITE OWNER_EXECUTE
            GROUP_READ GROUP_EXECUTE
            WORLD_READ WORLD_EXECUTE)
endfunction()

function(write_cache build_dir contents)
    file(MAKE_DIRECTORY "${build_dir}")
    file(WRITE "${build_dir}/CMakeCache.txt" "${contents}")
endfunction()

function(run_discover build_dir tool out_var)
    execute_process(
        COMMAND "${discover_script}"
            --build-dir "${build_dir}"
            --target-id arm64-apple-darwin
            --tool "${tool}"
        RESULT_VARIABLE result
        OUTPUT_VARIABLE output
        ERROR_VARIABLE error
        OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(NOT result EQUAL 0)
        message(FATAL_ERROR "discover ${tool} failed\n${error}")
    endif()
    set(${out_var} "${output}" PARENT_SCOPE)
endfunction()

function(assert_equal actual expected description)
    if(NOT "${actual}" STREQUAL "${expected}")
        message(FATAL_ERROR
            "expected ${description}\nexpected: ${expected}\nactual:   ${actual}")
    endif()
endfunction()

set(explicit_dir "${test_root}/explicit/bin")
write_fake_tool("${explicit_dir}/configured-otool")
write_fake_tool("${explicit_dir}/configured-ld")
set(explicit_build "${test_root}/explicit-build")
write_cache("${explicit_build}"
    "LOCKDC_OTOOL:FILEPATH=${explicit_dir}/configured-otool\nCMAKE_LINKER:FILEPATH=${explicit_dir}/configured-ld\n")
run_discover("${explicit_build}" otool explicit_otool)
run_discover("${explicit_build}" ld explicit_ld)
assert_equal("${explicit_otool}" "${explicit_dir}/configured-otool" "configured otool cache value")
assert_equal("${explicit_ld}" "${explicit_dir}/configured-ld" "configured linker cache value")

set(prefixed_bin "${test_root}/prefixed/bin")
write_fake_tool("${prefixed_bin}/arm64-apple-darwin25-clang")
write_fake_tool("${prefixed_bin}/arm64-apple-darwin25-otool")
set(prefixed_build "${test_root}/prefixed-build")
write_cache("${prefixed_build}"
    "CMAKE_C_COMPILER:FILEPATH=${prefixed_bin}/arm64-apple-darwin25-clang\n")
run_discover("${prefixed_build}" otool prefixed_otool)
assert_equal("${prefixed_otool}" "${prefixed_bin}/arm64-apple-darwin25-otool" "target-prefixed compiler sibling")

set(unprefixed_bin "${test_root}/unprefixed/bin")
write_fake_tool("${unprefixed_bin}/arm64-apple-darwin25-clang")
write_fake_tool("${unprefixed_bin}/otool")
set(unprefixed_build "${test_root}/unprefixed-build")
write_cache("${unprefixed_build}"
    "CMAKE_C_COMPILER:FILEPATH=${unprefixed_bin}/arm64-apple-darwin25-clang\n")
run_discover("${unprefixed_build}" otool unprefixed_otool)
assert_equal("${unprefixed_otool}" "${unprefixed_bin}/otool" "unprefixed compiler sibling")

set(path_bin "${test_root}/path/bin")
write_fake_tool("${path_bin}/arm64-apple-darwin25-strip")
set(path_build "${test_root}/path-build")
write_cache("${path_build}" "")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        "OSXCROSS_ROOT=${test_root}/missing-osxcross"
        "PATH=${path_bin}:$ENV{PATH}"
        "${discover_script}"
            --build-dir "${path_build}"
            --target-id arm64-apple-darwin
            --tool strip
    RESULT_VARIABLE path_result
    OUTPUT_VARIABLE path_strip
    ERROR_VARIABLE path_error
    OUTPUT_STRIP_TRAILING_WHITESPACE)
if(NOT path_result EQUAL 0)
    message(FATAL_ERROR "PATH fallback discovery failed\n${path_error}")
endif()
assert_equal("${path_strip}" "${path_bin}/arm64-apple-darwin25-strip" "target-prefixed PATH fallback")

set(host_bin "${test_root}/host/bin")
write_fake_tool("${host_bin}/ld")
set(host_build "${test_root}/host-build")
write_cache("${host_build}" "")
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
        "OSXCROSS_ROOT=${test_root}/missing-osxcross"
        "PATH=${host_bin}:/usr/bin:/bin"
        "${discover_script}"
            --build-dir "${host_build}"
            --target-id arm64-apple-darwin
            --tool ld
    RESULT_VARIABLE host_ld_result
    OUTPUT_VARIABLE host_ld_output
    ERROR_VARIABLE host_ld_error)
if(host_ld_result EQUAL 0)
    message(FATAL_ERROR
        "Darwin linker discovery selected an unprefixed host linker\n${host_ld_output}")
endif()
if(NOT host_ld_error MATCHES "external-tool-unavailable")
    message(FATAL_ERROR
        "Darwin linker discovery did not report external-tool-unavailable\n${host_ld_error}")
endif()

set(assign_build "${test_root}/assign-build")
write_cache("${assign_build}"
    "LOCKDC_OTOOL:FILEPATH=${explicit_dir}/configured-otool\nCMAKE_LINKER:FILEPATH=${explicit_dir}/configured-ld\n")
execute_process(
    COMMAND "${discover_script}"
        --build-dir "${assign_build}"
        --target-id arm64-apple-darwin
    RESULT_VARIABLE assign_result
    OUTPUT_VARIABLE assignments
    ERROR_VARIABLE assign_error)
if(NOT assign_result EQUAL 0)
    message(FATAL_ERROR "assignment discovery failed\n${assign_error}")
endif()
foreach(required_assignment
        "TARGET_ID=arm64-apple-darwin"
        "TARGET_HOST=arm64-apple-darwin25"
        "LD=${explicit_dir}/configured-ld"
        "LINKER=${explicit_dir}/configured-ld"
        "OTOOL=${explicit_dir}/configured-otool")
    if(NOT assignments MATCHES "(^|\n)${required_assignment}(\n|$)")
        message(FATAL_ERROR
            "missing target-tool assignment: ${required_assignment}\n${assignments}")
    endif()
endforeach()

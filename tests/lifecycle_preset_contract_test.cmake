if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

file(READ "${LOCKDC_ROOT}/CMakePresets.json" presets_json)
file(READ "${LOCKDC_ROOT}/Makefile" root_makefile)
file(READ "${LOCKDC_ROOT}/docs/lifecycle-migration.md" lifecycle_ledger)

function(assert_contains haystack needle description)
    string(FIND "${${haystack}}" "${needle}" found_at)
    if(found_at EQUAL -1)
        message(FATAL_ERROR "missing ${description}")
    endif()
endfunction()

function(json_length out)
    string(JSON length_value ERROR_VARIABLE json_error LENGTH "${presets_json}" ${ARGN})
    if(json_error)
        message(FATAL_ERROR "unable to read JSON length at ${ARGN}: ${json_error}")
    endif()
    set(${out} "${length_value}" PARENT_SCOPE)
endfunction()

function(json_get out)
    string(JSON value ERROR_VARIABLE json_error GET "${presets_json}" ${ARGN})
    if(json_error)
        message(FATAL_ERROR "unable to read JSON value at ${ARGN}: ${json_error}")
    endif()
    string(REGEX REPLACE "^\"(.*)\"$" "\\1" value "${value}")
    set(${out} "${value}" PARENT_SCOPE)
endfunction()

function(assert_configure_cache_absent name key)
    find_named_object_index(index configurePresets "${name}")
    string(JSON value ERROR_VARIABLE json_error GET "${presets_json}"
        configurePresets ${index} cacheVariables ${key})
    if(NOT json_error)
        message(FATAL_ERROR
            "configure preset ${name} must not set cache variable ${key}, got ${value}")
    endif()
endfunction()

function(find_named_object_index out array_path wanted_name)
    json_length(array_length "${array_path}")
    math(EXPR last_index "${array_length} - 1")
    foreach(index RANGE 0 ${last_index})
        json_get(object_name "${array_path}" ${index} name)
        if(object_name STREQUAL wanted_name)
            set(${out} "${index}" PARENT_SCOPE)
            return()
        endif()
    endforeach()
    message(FATAL_ERROR "missing ${array_path} entry named ${wanted_name}")
endfunction()

function(assert_configure_preset name)
    find_named_object_index(index configurePresets "${name}")
endfunction()

function(assert_build_preset name expected_configure)
    find_named_object_index(index buildPresets "${name}")
    json_get(configure_preset buildPresets ${index} configurePreset)
    if(NOT configure_preset STREQUAL expected_configure)
        message(FATAL_ERROR
            "build preset ${name} should use configure preset ${expected_configure}, got ${configure_preset}")
    endif()
endfunction()

function(assert_test_preset name expected_configure)
    find_named_object_index(index testPresets "${name}")
    json_get(configure_preset testPresets ${index} configurePreset)
    if(NOT configure_preset STREQUAL expected_configure)
        message(FATAL_ERROR
            "test preset ${name} should use configure preset ${expected_configure}, got ${configure_preset}")
    endif()
endfunction()

function(assert_configure_cache name key expected)
    find_named_object_index(index configurePresets "${name}")
    json_get(actual configurePresets ${index} cacheVariables ${key})
    if(NOT actual STREQUAL expected)
        message(FATAL_ERROR
            "configure preset ${name} cache variable ${key} should be ${expected}, got ${actual}")
    endif()
endfunction()

function(assert_configure_toolchain name expected)
    find_named_object_index(index configurePresets "${name}")
    json_get(actual configurePresets ${index} toolchainFile)
    if(NOT actual STREQUAL expected)
        message(FATAL_ERROR
            "configure preset ${name} toolchainFile should be ${expected}, got ${actual}")
    endif()
endfunction()

foreach(name
        base
        debug
        debug-lua
        valgrind
        fuzz
        x86_64-linux-gnu-release
        x86_64-linux-musl-release
        aarch64-linux-gnu-release
        aarch64-linux-musl-release
        armhf-linux-gnu-release
        armhf-linux-musl-release
        arm64-apple-darwin-release)
    assert_configure_preset("${name}")
endforeach()

foreach(name
        debug
        debug-lua
        valgrind
        fuzz
        x86_64-linux-gnu-release
        x86_64-linux-musl-release
        aarch64-linux-gnu-release
        aarch64-linux-musl-release
        armhf-linux-gnu-release
        armhf-linux-musl-release
        arm64-apple-darwin-release)
    assert_build_preset("${name}" "${name}")
    assert_test_preset("${name}" "${name}")
endforeach()

assert_configure_cache(base CMAKE_EXPORT_COMPILE_COMMANDS ON)
assert_configure_cache(base LOCKDC_BUILD_DEPENDENCIES OFF)
assert_configure_cache(base LOCKDC_BUILD_TESTS ON)
assert_configure_cache(base LOCKDC_BUILD_FUZZERS OFF)
assert_configure_cache(base LOCKDC_INSTALL ON)

assert_configure_cache(debug CMAKE_BUILD_TYPE Debug)
assert_configure_cache(debug LOCKDC_BUILD_EXAMPLES ON)
assert_configure_cache(debug-lua LOCKDC_BUILD_LUA_BINDINGS ON)
assert_configure_cache(debug-lua LOCKDC_BUILD_BENCHMARKS OFF)
assert_configure_cache(valgrind CMAKE_C_FLAGS_DEBUG "-O1 -g -fno-omit-frame-pointer")
assert_configure_cache(valgrind LOCKDC_BUILD_FUZZERS OFF)
assert_configure_cache(fuzz LOCKDC_BUILD_FUZZERS ON)
assert_configure_cache(fuzz LOCKDC_TARGET_ARCH x86_64)
assert_configure_cache(fuzz LOCKDC_TARGET_OS linux)
assert_configure_cache(fuzz LOCKDC_TARGET_LIBC gnu)
assert_configure_cache_absent(fuzz CMAKE_C_COMPILER)
assert_configure_cache_absent(fuzz CMAKE_CXX_COMPILER)

assert_configure_toolchain(x86_64-linux-gnu-release "$\{sourceDir\}/cmake/toolchains/x86_64-linux-gnu.cmake")
assert_configure_toolchain(x86_64-linux-musl-release "$\{sourceDir\}/cmake/toolchains/x86_64-linux-musl.cmake")
assert_configure_toolchain(aarch64-linux-gnu-release "$\{sourceDir\}/cmake/toolchains/aarch64-linux-gnu.cmake")
assert_configure_toolchain(aarch64-linux-musl-release "$\{sourceDir\}/cmake/toolchains/aarch64-linux-musl.cmake")
assert_configure_toolchain(armhf-linux-gnu-release "$\{sourceDir\}/cmake/toolchains/armhf-linux-gnu.cmake")
assert_configure_toolchain(armhf-linux-musl-release "$\{sourceDir\}/cmake/toolchains/armhf-linux-musl.cmake")

foreach(name
        x86_64-linux-gnu-release
        x86_64-linux-musl-release
        aarch64-linux-gnu-release
        aarch64-linux-musl-release
        armhf-linux-gnu-release
        armhf-linux-musl-release
        arm64-apple-darwin-release)
    assert_configure_cache("${name}" LOCKDC_DIST_DIR "$\{sourceDir\}/dist")
endforeach()

assert_contains(root_makefile "make valgrind" "make help Valgrind surface")
assert_contains(root_makefile "scripts/valgrind.sh" "Valgrind runner command")
assert_contains(lifecycle_ledger "Preset Surface" "migration ledger preset section")
assert_contains(lifecycle_ledger "`debug-lua`" "migration ledger debug-lua entry")
assert_contains(lifecycle_ledger "`valgrind`" "migration ledger valgrind entry")

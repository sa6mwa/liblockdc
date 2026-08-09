if(NOT DEFINED LOCKDC_ROOT OR LOCKDC_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

function(read_required path out_var)
    if(NOT EXISTS "${path}")
        message(FATAL_ERROR "missing required lifecycle file: ${path}")
    endif()
    file(READ "${path}" contents)
    set(${out_var} "${contents}" PARENT_SCOPE)
endfunction()

function(assert_contains haystack needle description)
    string(FIND "${haystack}" "${needle}" found_index)
    if(found_index EQUAL -1)
        message(FATAL_ERROR "expected ${description}")
    endif()
endfunction()

function(assert_not_contains haystack needle description)
    string(FIND "${haystack}" "${needle}" found_index)
    if(NOT found_index EQUAL -1)
        message(FATAL_ERROR "unexpected ${description}")
    endif()
endfunction()

read_required("${LOCKDC_ROOT}/cmake/package_archive.cmake" package_archive)
read_required("${LOCKDC_ROOT}/cmake/strip_release_target.cmake" strip_release_target)
read_required("${LOCKDC_ROOT}/tests/package_archive_assertions.cmake" package_assertions)
read_required("${LOCKDC_ROOT}/CMakeLists.txt" cmake_lists)
read_required("${LOCKDC_ROOT}/cmake/toolchains/arm64-apple-darwin.cmake" darwin_toolchain)
read_required("${LOCKDC_ROOT}/cmake/package_darwin_smoke_bundle.cmake" darwin_smoke_bundle)
read_required("${LOCKDC_ROOT}/scripts/discover_target_tools.sh" discover_target_tools)

assert_contains(
    "${package_archive}"
    "function(lockdc_verify_darwin_macho_metadata package_root)"
    "Darwin package verification function")
assert_contains(
    "${package_archive}"
    "Darwin package dylib contains non-system absolute dependency path"
    "Darwin non-system absolute dependency rejection")
assert_contains(
    "${package_archive}"
    "Darwin package dylib contains absolute rpath"
    "Darwin absolute rpath rejection")
assert_not_contains(
    "${package_archive}"
    "CMAKE_INSTALL_NAME_TOOL"
    "final package install_name_tool mutation")
assert_contains(
    "${strip_release_target}"
    "if(LOCKDC_TARGET_ID MATCHES \"apple-darwin$\")"
    "Darwin final artifact strip skip")
assert_contains(
    "${package_assertions}"
    "archive Darwin shared library has an unversioned install name"
    "Darwin ABI-versioned install-name assertion")
assert_contains(
    "${package_assertions}"
    "archive Darwin shared library contains non-system absolute dependency path"
    "Darwin archive dependency-path assertion")
assert_contains(
    "${cmake_lists}"
    "INSTALL_NAME_DIR \"@rpath\""
    "explicit Darwin install-name directory")
assert_contains(
    "${darwin_toolchain}"
    [=[set(ENV{PATH} "${LOCKDC_OSXCROSS_BIN_DIR}:$ENV{PATH}")]=]
    "osxcross bin directory prepended to configure PATH")
assert_contains(
    "${darwin_toolchain}"
    [=[set(CMAKE_LINKER "${LOCKDC_OSXCROSS_BIN_DIR}/${LOCKDC_OSXCROSS_HOST}-ld"]=]
    "explicit osxcross Darwin linker")
assert_contains(
    "${darwin_toolchain}"
    [=[set(CMAKE_NM "${LOCKDC_OSXCROSS_BIN_DIR}/${LOCKDC_OSXCROSS_HOST}-nm"]=]
    "explicit osxcross Darwin nm")
assert_contains(
    "${darwin_toolchain}"
    [=[set(CMAKE_STRIP "${LOCKDC_OSXCROSS_BIN_DIR}/${LOCKDC_OSXCROSS_HOST}-strip"]=]
    "explicit osxcross Darwin strip")
assert_contains(
    "${darwin_toolchain}"
    [=[set(_lockdc_darwin_linker_flag "--ld-path=${CMAKE_LINKER}")]=]
    "absolute Darwin --ld-path linker route")
assert_not_contains(
    "${darwin_toolchain}"
    "-fuse-ld=${CMAKE_LINKER}"
    "deprecated Darwin absolute -fuse-ld linker route")
assert_contains(
    "${darwin_smoke_bundle}"
    [=["PATH=${lockdc_darwin_tool_path}"]=]
    "Darwin smoke build PATH uses configured linker directory")
assert_contains(
    "${package_archive}"
    "scripts/discover_target_tools.sh"
    "Darwin package verification uses shared target-tool discovery")
assert_contains(
    "${package_assertions}"
    "scripts/discover_target_tools.sh"
    "Darwin archive assertions use shared target-tool discovery")
assert_contains(
    "${discover_target_tools}"
    "external-tool-unavailable"
    "target-tool discovery reports missing tools explicitly")
assert_contains(
    "${discover_target_tools}"
    "CMAKE_LINKER"
    "target-tool discovery can resolve configured linker")
assert_contains(
    "${discover_target_tools}"
    "TARGET_HOST"
    "target-tool discovery emits machine-readable assignments")
assert_contains(
    "${discover_target_tools}"
    "ld|linker)"
    "target-tool discovery has Darwin linker route coverage")

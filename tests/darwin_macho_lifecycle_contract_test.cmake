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

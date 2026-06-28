if(DEFINED LOCKDC_DIST_DIR AND NOT "${LOCKDC_DIST_DIR}" STREQUAL "")
    set(lockdc_input_dist_dir "${LOCKDC_DIST_DIR}")
endif()
if(EXISTS "${LOCKDC_BINARY_DIR}/package-metadata.cmake")
    include("${LOCKDC_BINARY_DIR}/package-metadata.cmake")
endif()
if(DEFINED lockdc_input_dist_dir)
    set(LOCKDC_DIST_DIR "${lockdc_input_dist_dir}")
elseif(NOT DEFINED LOCKDC_DIST_DIR OR "${LOCKDC_DIST_DIR}" STREQUAL "")
    set(LOCKDC_DIST_DIR "${LOCKDC_ROOT}/dist")
endif()

set(package_stage_root "${LOCKDC_BINARY_DIR}/package")
set(package_prefix_name "liblockdc-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}")
set(package_root "${package_stage_root}/${package_prefix_name}")

function(lockdc_import_cache_path var_name)
    if(DEFINED ${var_name} AND NOT "${${var_name}}" STREQUAL "")
        return()
    endif()
    file(STRINGS "${LOCKDC_BINARY_DIR}/CMakeCache.txt" cache_line
         REGEX "^${var_name}(:[^=]+)?=" LIMIT_COUNT 1)
    if(cache_line)
        string(REGEX REPLACE "^[^=]*=" "" cache_value "${cache_line}")
        set(${var_name} "${cache_value}" PARENT_SCOPE)
    endif()
endfunction()

lockdc_import_cache_path(LOCKDC_EXTERNAL_ROOT)
lockdc_import_cache_path(LOCKDC_DEPENDENCY_BUILD_ROOT)
lockdc_import_cache_path(CMAKE_STRIP)
lockdc_import_cache_path(LOCKDC_OTOOL)

file(REMOVE_RECURSE "${package_root}")
file(MAKE_DIRECTORY "${package_root}/include/lc")
file(MAKE_DIRECTORY "${package_root}/include")
file(MAKE_DIRECTORY "${package_root}/lib")
file(MAKE_DIRECTORY "${package_root}/lib/pkgconfig")
file(MAKE_DIRECTORY "${package_root}/lib/cmake/lockdc")
file(MAKE_DIRECTORY "${package_root}/share/doc/liblockdc")

if(NOT EXISTS "${LOCKDC_BINARY_DIR}/cmake_install.cmake")
    message(FATAL_ERROR
        "package generation requires a real install-enabled build tree; missing ${LOCKDC_BINARY_DIR}/cmake_install.cmake")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" --install "${LOCKDC_BINARY_DIR}" --prefix "${package_root}" --component runtime
    RESULT_VARIABLE lockdc_runtime_install_result
)
if(NOT lockdc_runtime_install_result EQUAL 0)
    message(FATAL_ERROR "failed to install runtime package payload")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" --install "${LOCKDC_BINARY_DIR}" --prefix "${package_root}" --component dev
    RESULT_VARIABLE lockdc_dev_install_result
)
if(NOT lockdc_dev_install_result EQUAL 0)
    message(FATAL_ERROR "failed to install development package payload")
endif()

function(lockdc_find_darwin_otool)
    if(NOT LOCKDC_TARGET_ID MATCHES "apple-darwin$")
        return()
    endif()
    if(NOT LOCKDC_OTOOL OR NOT EXISTS "${LOCKDC_OTOOL}")
        if(DEFINED ENV{OSXCROSS_ROOT} AND NOT "$ENV{OSXCROSS_ROOT}" STREQUAL "")
            set(_lockdc_osxcross_bin_hint "$ENV{OSXCROSS_ROOT}/bin")
        elseif(DEFINED ENV{HOME} AND NOT "$ENV{HOME}" STREQUAL "")
            set(_lockdc_osxcross_bin_hint "$ENV{HOME}/.local/cross/osxcross/bin")
        else()
            set(_lockdc_osxcross_bin_hint "")
        endif()
        find_program(LOCKDC_OTOOL NAMES arm64-apple-darwin25-otool otool HINTS "${_lockdc_osxcross_bin_hint}")
    endif()
    if(NOT LOCKDC_OTOOL OR NOT EXISTS "${LOCKDC_OTOOL}")
        message(FATAL_ERROR "otool is required for Darwin package verification")
    endif()
    set(LOCKDC_OTOOL "${LOCKDC_OTOOL}" PARENT_SCOPE)
endfunction()

function(lockdc_assert_darwin_dependency_path dylib dependency_path otool_output)
    if(NOT dependency_path MATCHES "^/")
        return()
    endif()
    if(dependency_path MATCHES "^/usr/lib/" OR dependency_path MATCHES "^/System/Library/")
        return()
    endif()
    message(FATAL_ERROR
        "Darwin package dylib contains non-system absolute dependency path "
        "'${dependency_path}' in ${dylib}\n${otool_output}")
endfunction()

function(lockdc_verify_darwin_macho_metadata package_root)
    if(NOT LOCKDC_TARGET_ID MATCHES "apple-darwin$")
        return()
    endif()

    lockdc_find_darwin_otool()

    file(GLOB _lockdc_dylibs LIST_DIRECTORIES false "${package_root}/lib/*.dylib")
    foreach(_lockdc_dylib IN LISTS _lockdc_dylibs)
        if(IS_SYMLINK "${_lockdc_dylib}")
            continue()
        endif()
        execute_process(
            COMMAND "${LOCKDC_OTOOL}" -D "${_lockdc_dylib}"
            RESULT_VARIABLE _lockdc_id_result
            OUTPUT_VARIABLE _lockdc_id_output
            ERROR_VARIABLE _lockdc_id_error
        )
        if(NOT _lockdc_id_result EQUAL 0)
            message(FATAL_ERROR "failed to inspect Darwin dylib install name ${_lockdc_dylib}\n${_lockdc_id_error}")
        endif()
        if(NOT _lockdc_id_output MATCHES "\n@rpath/")
            message(FATAL_ERROR
                "Darwin package dylib install name is not @rpath-relative: ${_lockdc_dylib}\n"
                "${_lockdc_id_output}")
        endif()

        execute_process(
            COMMAND "${LOCKDC_OTOOL}" -L "${_lockdc_dylib}"
            RESULT_VARIABLE _lockdc_otool_result
            OUTPUT_VARIABLE _lockdc_otool_output
            ERROR_VARIABLE _lockdc_otool_error
        )
        if(NOT _lockdc_otool_result EQUAL 0)
            message(FATAL_ERROR "failed to inspect Darwin dylib ${_lockdc_dylib}\n${_lockdc_otool_error}")
        endif()
        string(REGEX REPLACE "\n$" "" _lockdc_otool_output "${_lockdc_otool_output}")
        string(REPLACE "\n" ";" _lockdc_otool_lines "${_lockdc_otool_output}")
        foreach(_lockdc_otool_line IN LISTS _lockdc_otool_lines)
            string(STRIP "${_lockdc_otool_line}" _lockdc_dependency_line)
            if(_lockdc_dependency_line MATCHES ":$")
                continue()
            endif()
            if(NOT _lockdc_dependency_line MATCHES "^/")
                continue()
            endif()
            string(REGEX MATCH "^[^ \t]+" _lockdc_dependency_path "${_lockdc_dependency_line}")
            if(_lockdc_dependency_path STREQUAL "")
                continue()
            endif()
            lockdc_assert_darwin_dependency_path(
                "${_lockdc_dylib}" "${_lockdc_dependency_path}" "${_lockdc_otool_output}")
        endforeach()

        execute_process(
            COMMAND "${LOCKDC_OTOOL}" -l "${_lockdc_dylib}"
            RESULT_VARIABLE _lockdc_load_result
            OUTPUT_VARIABLE _lockdc_load_output
            ERROR_VARIABLE _lockdc_load_error
        )
        if(NOT _lockdc_load_result EQUAL 0)
            message(FATAL_ERROR "failed to inspect Darwin dylib load commands ${_lockdc_dylib}\n${_lockdc_load_error}")
        endif()
        if(_lockdc_load_output MATCHES "path /")
            message(FATAL_ERROR
                "Darwin package dylib contains absolute rpath: ${_lockdc_dylib}\n${_lockdc_load_output}")
        endif()
    endforeach()
endfunction()

lockdc_verify_darwin_macho_metadata("${package_root}")

function(lockdc_strip_packaged_artifact artifact_path)
    if(NOT CMAKE_STRIP OR NOT EXISTS "${CMAKE_STRIP}")
        return()
    endif()
    if(NOT EXISTS "${artifact_path}" OR IS_SYMLINK "${artifact_path}")
        return()
    endif()
    if(LOCKDC_TARGET_ID MATCHES "apple-darwin$")
        return()
    endif()

    execute_process(
        COMMAND "${CMAKE_STRIP}" -S "${artifact_path}"
        RESULT_VARIABLE _lockdc_strip_result
        ERROR_VARIABLE _lockdc_strip_error
    )
    if(NOT _lockdc_strip_result EQUAL 0)
        message(FATAL_ERROR "failed to strip ${artifact_path}\n${_lockdc_strip_error}")
    endif()
endfunction()

file(GLOB _lockdc_owned_library_artifacts
    LIST_DIRECTORIES false
    "${package_root}/lib/liblockdc.a"
    "${package_root}/lib/liblockdc.so*"
    "${package_root}/lib/liblockdc.*.dylib"
)
foreach(_lockdc_owned_library_artifact IN LISTS _lockdc_owned_library_artifacts)
    lockdc_strip_packaged_artifact("${_lockdc_owned_library_artifact}")
endforeach()

file(MAKE_DIRECTORY "${LOCKDC_DIST_DIR}")
set(archive_base "${LOCKDC_DIST_DIR}/liblockdc-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}.tar")
set(archive "${archive_base}.gz")
find_program(LOCKDC_TAR_BIN NAMES tar)
find_program(LOCKDC_GZIP_BIN NAMES gzip)
if(NOT LOCKDC_TAR_BIN)
    message(FATAL_ERROR "failed to find tar for archive creation")
endif()
if(NOT LOCKDC_GZIP_BIN)
    message(FATAL_ERROR "failed to find gzip for archive creation")
endif()
file(REMOVE "${archive_base}" "${archive}")
execute_process(
    COMMAND "${LOCKDC_TAR_BIN}" -cf "${archive_base}" --format=gnu --owner 0 --group 0 "${package_prefix_name}"
    WORKING_DIRECTORY "${package_stage_root}"
    RESULT_VARIABLE tar_result
)
if(NOT tar_result EQUAL 0)
    message(FATAL_ERROR "failed to create package archive")
endif()
file(REMOVE "${archive}")
execute_process(
    COMMAND "${LOCKDC_GZIP_BIN}" -9 -f "${archive_base}"
    RESULT_VARIABLE gzip_result
)
if(NOT gzip_result EQUAL 0)
    message(FATAL_ERROR "failed to gzip package archive")
endif()

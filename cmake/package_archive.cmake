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
lockdc_import_cache_path(CMAKE_INSTALL_NAME_TOOL)
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

function(lockdc_fix_darwin_install_names package_root)
    if(NOT LOCKDC_TARGET_ID MATCHES "apple-darwin$")
        return()
    endif()
    if(NOT CMAKE_INSTALL_NAME_TOOL OR NOT EXISTS "${CMAKE_INSTALL_NAME_TOOL}")
        message(FATAL_ERROR "CMAKE_INSTALL_NAME_TOOL is required for Darwin package fixups")
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
        message(FATAL_ERROR "otool is required for Darwin package fixups")
    endif()

    file(GLOB _lockdc_dylibs LIST_DIRECTORIES false "${package_root}/lib/*.dylib")
    foreach(_lockdc_dylib IN LISTS _lockdc_dylibs)
        get_filename_component(_lockdc_dylib_name "${_lockdc_dylib}" NAME)
        execute_process(
            COMMAND "${CMAKE_INSTALL_NAME_TOOL}" -id "@rpath/${_lockdc_dylib_name}" "${_lockdc_dylib}"
            RESULT_VARIABLE _lockdc_id_result
            ERROR_VARIABLE _lockdc_id_error
        )
        if(NOT _lockdc_id_result EQUAL 0)
            message(FATAL_ERROR "failed to rewrite install name for ${_lockdc_dylib}\n${_lockdc_id_error}")
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
            if(NOT _lockdc_dependency_line MATCHES "^/")
                continue()
            endif()
            string(REGEX MATCH "^[^ \t]+" _lockdc_dependency_path "${_lockdc_dependency_line}")
            if(_lockdc_dependency_path STREQUAL "")
                continue()
            endif()
            if(_lockdc_dependency_path MATCHES "^/usr/lib/" OR
               _lockdc_dependency_path MATCHES "^/System/Library/")
                continue()
            endif()
            get_filename_component(_lockdc_dependency_name "${_lockdc_dependency_path}" NAME)
            if(EXISTS "${package_root}/lib/${_lockdc_dependency_name}")
                execute_process(
                    COMMAND "${CMAKE_INSTALL_NAME_TOOL}"
                        -change "${_lockdc_dependency_path}" "@rpath/${_lockdc_dependency_name}" "${_lockdc_dylib}"
                    RESULT_VARIABLE _lockdc_change_result
                    ERROR_VARIABLE _lockdc_change_error
                )
                if(NOT _lockdc_change_result EQUAL 0)
                    message(FATAL_ERROR
                        "failed to rewrite Darwin dependency ${_lockdc_dependency_path} in ${_lockdc_dylib}\n"
                        "${_lockdc_change_error}")
                endif()
            endif()
        endforeach()
    endforeach()
endfunction()

lockdc_fix_darwin_install_names("${package_root}")

function(lockdc_strip_packaged_artifact artifact_path)
    if(NOT CMAKE_STRIP OR NOT EXISTS "${CMAKE_STRIP}")
        return()
    endif()
    if(NOT EXISTS "${artifact_path}" OR IS_SYMLINK "${artifact_path}")
        return()
    endif()
    if(LOCKDC_TARGET_ID MATCHES "apple-darwin$" AND artifact_path MATCHES "\\.a$")
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

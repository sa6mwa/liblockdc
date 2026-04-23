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

function(lockdc_copy_required_license_named package_name destination_dir output_name)
    set(candidates ${ARGN})

    foreach(candidate IN LISTS candidates)
        if(EXISTS "${candidate}")
            file(MAKE_DIRECTORY "${destination_dir}")
            file(COPY "${candidate}" DESTINATION "${destination_dir}")
            get_filename_component(source_name "${candidate}" NAME)
            if(NOT source_name STREQUAL output_name)
                file(RENAME "${destination_dir}/${source_name}" "${destination_dir}/${output_name}")
            endif()
            return()
        endif()
    endforeach()

    list(JOIN candidates "\n  " candidate_list)
    message(FATAL_ERROR
        "missing required ${package_name} package license; looked in:\n  ${candidate_list}")
endfunction()

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

lockdc_copy_required_license_named(
    "libpslog"
    "${package_root}/share/doc/liblockdc/third_party/libpslog"
    "LICENSE"
    "${LOCKDC_EXTERNAL_ROOT}/pslog/install/share/doc/libpslog/LICENSE"
    "${LOCKDC_EXTERNAL_ROOT}/pslog/install/share/doc/liblockdc-third-party/libpslog/LICENSE.txt"
)
lockdc_copy_required_license_named(
    "openssl"
    "${package_root}/share/doc/liblockdc/third_party/openssl"
    "LICENSE.txt"
    "${LOCKDC_EXTERNAL_ROOT}/openssl/install/share/doc/liblockdc-third-party/openssl/LICENSE.txt"
    "${LOCKDC_DEPENDENCY_BUILD_ROOT}/openssl/src/LICENSE.txt"
)
lockdc_copy_required_license_named(
    "curl"
    "${package_root}/share/doc/liblockdc/third_party/curl"
    "LICENSE.txt"
    "${LOCKDC_EXTERNAL_ROOT}/curl/install/share/doc/liblockdc-third-party/curl/LICENSE.txt"
    "${LOCKDC_DEPENDENCY_BUILD_ROOT}/curl/src/COPYING"
)
lockdc_copy_required_license_named(
    "nghttp2"
    "${package_root}/share/doc/liblockdc/third_party/nghttp2"
    "LICENSE.txt"
    "${LOCKDC_EXTERNAL_ROOT}/nghttp2/install/share/doc/liblockdc-third-party/nghttp2/LICENSE.txt"
    "${LOCKDC_DEPENDENCY_BUILD_ROOT}/nghttp2/src/COPYING"
)
lockdc_copy_required_license_named(
    "libssh2"
    "${package_root}/share/doc/liblockdc/third_party/libssh2"
    "LICENSE.txt"
    "${LOCKDC_EXTERNAL_ROOT}/libssh2/install/share/doc/liblockdc-third-party/libssh2/LICENSE.txt"
    "${LOCKDC_EXTERNAL_ROOT}/libssh2/install/share/doc/libssh2/COPYING"
    "${LOCKDC_DEPENDENCY_BUILD_ROOT}/libssh2/src/COPYING"
)
lockdc_copy_required_license_named(
    "zlib"
    "${package_root}/share/doc/liblockdc/third_party/zlib"
    "LICENSE.txt"
    "${LOCKDC_EXTERNAL_ROOT}/zlib/install/share/doc/liblockdc-third-party/zlib/LICENSE.txt"
    "${LOCKDC_DEPENDENCY_BUILD_ROOT}/zlib/src/LICENSE"
)
lockdc_copy_required_license_named(
    "lonejson"
    "${package_root}/share/doc/liblockdc/third_party/lonejson"
    "LICENSE"
    "${LOCKDC_EXTERNAL_ROOT}/lonejson/install/share/doc/liblonejson/LICENSE"
    "${LOCKDC_EXTERNAL_ROOT}/lonejson/install/share/doc/liblockdc-third-party/lonejson/LICENSE.txt"
)

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

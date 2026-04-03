set(package_root "${LOCKDC_BINARY_DIR}/package/archive")
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

function(lockdc_copy_if_exists source_path destination_dir)
    if(EXISTS "${source_path}")
        file(COPY "${source_path}" DESTINATION "${destination_dir}")
    endif()
endfunction()

function(lockdc_copy_directory_if_exists source_path destination_dir)
    if(EXISTS "${source_path}")
        file(COPY "${source_path}" DESTINATION "${destination_dir}")
    endif()
endfunction()

function(lockdc_copy_glob_follow pattern destination_dir)
    file(GLOB matches "${pattern}")
    if(matches)
        file(COPY ${matches} DESTINATION "${destination_dir}" FOLLOW_SYMLINK_CHAIN)
    endif()
endfunction()

function(lockdc_copy_license_if_exists source_path destination_dir)
    if(EXISTS "${source_path}")
        file(MAKE_DIRECTORY "${destination_dir}")
        get_filename_component(source_name "${source_path}" NAME)
        file(COPY "${source_path}" DESTINATION "${destination_dir}")
        if(source_name STREQUAL "COPYING")
            file(RENAME "${destination_dir}/COPYING" "${destination_dir}/LICENSE.txt")
        endif()
    endif()
endfunction()

function(lockdc_copy_required_license package_name destination_dir)
    set(candidates ${ARGN})

    foreach(candidate IN LISTS candidates)
        if(EXISTS "${candidate}")
            lockdc_copy_license_if_exists("${candidate}" "${destination_dir}")
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

file(COPY "${LOCKDC_PUBLIC_HEADER}" DESTINATION "${package_root}/include/lc")
file(COPY "${LOCKDC_PUBLIC_VERSION_HEADER}" DESTINATION "${package_root}/include/lc")
lockdc_copy_if_exists("${LOCKDC_EXTERNAL_ROOT}/pslog-static/install/include/pslog.h" "${package_root}/include")
lockdc_copy_if_exists("${LOCKDC_EXTERNAL_ROOT}/pslog-static/install/include/pslog_version.h" "${package_root}/include")
lockdc_copy_directory_if_exists("${LOCKDC_EXTERNAL_ROOT}/curl-static/install/include/curl" "${package_root}/include")
lockdc_copy_directory_if_exists("${LOCKDC_EXTERNAL_ROOT}/openssl-static/install/include/openssl" "${package_root}/include")
lockdc_copy_directory_if_exists("${LOCKDC_EXTERNAL_ROOT}/nghttp2-static/install/include/nghttp2" "${package_root}/include")
lockdc_copy_if_exists("${LOCKDC_EXTERNAL_ROOT}/libssh2/install/include/libssh2.h" "${package_root}/include")
lockdc_copy_if_exists("${LOCKDC_EXTERNAL_ROOT}/libssh2/install/include/libssh2_publickey.h" "${package_root}/include")
lockdc_copy_if_exists("${LOCKDC_EXTERNAL_ROOT}/libssh2/install/include/libssh2_sftp.h" "${package_root}/include")
lockdc_copy_if_exists("${LOCKDC_EXTERNAL_ROOT}/lonejson-static/install/include/lonejson.h" "${package_root}/include")

file(COPY "${LOCKDC_STATIC_LIB}" DESTINATION "${package_root}/lib")
file(COPY "${LOCKDC_SHARED_LIB}" DESTINATION "${package_root}/lib")
file(COPY "${LOCKDC_EXTERNAL_ROOT}/pslog-static/install/lib/libpslog.a" DESTINATION "${package_root}/lib")
lockdc_copy_if_exists("${LOCKDC_EXTERNAL_ROOT}/curl-static/install/lib/libcurl.a" "${package_root}/lib")
lockdc_copy_if_exists("${LOCKDC_EXTERNAL_ROOT}/openssl-static/install/lib/libssl.a" "${package_root}/lib")
lockdc_copy_if_exists("${LOCKDC_EXTERNAL_ROOT}/openssl-static/install/lib/libcrypto.a" "${package_root}/lib")
lockdc_copy_if_exists("${LOCKDC_EXTERNAL_ROOT}/nghttp2-static/install/lib/libnghttp2.a" "${package_root}/lib")
lockdc_copy_if_exists("${LOCKDC_EXTERNAL_ROOT}/libssh2/install/lib/libssh2.a" "${package_root}/lib")
lockdc_copy_if_exists("${LOCKDC_EXTERNAL_ROOT}/lonejson-static/install/lib/liblonejson.a" "${package_root}/lib")

if(DEFINED LOCKDC_SHARED_SONAME
   AND DEFINED LOCKDC_SHARED_LIB_NAME
   AND NOT LOCKDC_SHARED_SONAME STREQUAL ""
   AND NOT LOCKDC_SHARED_SONAME STREQUAL LOCKDC_SHARED_LIB_NAME)
    file(CREATE_LINK "${LOCKDC_SHARED_LIB_NAME}"
         "${package_root}/lib/${LOCKDC_SHARED_SONAME}"
         SYMBOLIC)
endif()
if(DEFINED LOCKDC_SHARED_LINK_NAME
   AND NOT LOCKDC_SHARED_LINK_NAME STREQUAL ""
   AND DEFINED LOCKDC_SHARED_SONAME
   AND NOT LOCKDC_SHARED_SONAME STREQUAL "")
    file(CREATE_LINK "${LOCKDC_SHARED_SONAME}"
         "${package_root}/lib/${LOCKDC_SHARED_LINK_NAME}"
         SYMBOLIC)
endif()

lockdc_copy_glob_follow("${LOCKDC_EXTERNAL_ROOT}/curl-shared-cmake/install/lib/libcurl.so*" "${package_root}/lib")
lockdc_copy_glob_follow("${LOCKDC_EXTERNAL_ROOT}/pslog-shared/install/lib/libpslog.so*" "${package_root}/lib")
lockdc_copy_glob_follow("${LOCKDC_EXTERNAL_ROOT}/openssl-shared/install/lib/libssl.so*" "${package_root}/lib")
lockdc_copy_glob_follow("${LOCKDC_EXTERNAL_ROOT}/openssl-shared/install/lib/libcrypto.so*" "${package_root}/lib")
lockdc_copy_glob_follow("${LOCKDC_EXTERNAL_ROOT}/nghttp2-shared/install/lib/libnghttp2.so*" "${package_root}/lib")
lockdc_copy_glob_follow("${LOCKDC_EXTERNAL_ROOT}/libssh2/install/lib/libssh2.so*" "${package_root}/lib")
lockdc_copy_glob_follow("${LOCKDC_EXTERNAL_ROOT}/lonejson-shared/install/lib/liblonejson.so*" "${package_root}/lib")

file(COPY "${LOCKDC_PUBLIC_PKGCONFIG}" DESTINATION "${package_root}/lib/pkgconfig")
file(COPY "${LOCKDC_PUBLIC_CMAKE_CONFIG}" DESTINATION "${package_root}/lib/cmake/lockdc")
file(COPY "${LOCKDC_PUBLIC_CMAKE_CONFIG_VERSION}" DESTINATION "${package_root}/lib/cmake/lockdc")
lockdc_copy_if_exists("${LOCKDC_ROOT}/LICENSE" "${package_root}/share/doc/liblockdc")
lockdc_copy_if_exists("${LOCKDC_ROOT}/README.md" "${package_root}/share/doc/liblockdc")

lockdc_copy_required_license(
    "libpslog"
    "${package_root}/share/doc/liblockdc/third_party/libpslog"
    "${LOCKDC_EXTERNAL_ROOT}/pslog-shared/install/share/doc/libpslog/LICENSE"
    "${LOCKDC_EXTERNAL_ROOT}/pslog-shared/install/share/doc/liblockdc-third-party/libpslog/LICENSE.txt"
    "${LOCKDC_EXTERNAL_ROOT}/pslog-static/install/share/doc/libpslog/LICENSE"
    "${LOCKDC_EXTERNAL_ROOT}/pslog-static/install/share/doc/liblockdc-third-party/libpslog/LICENSE.txt"
)
lockdc_copy_required_license(
    "openssl"
    "${package_root}/share/doc/liblockdc/third_party/openssl"
    "${LOCKDC_EXTERNAL_ROOT}/openssl-shared/install/share/doc/liblockdc-third-party/openssl/LICENSE.txt"
    "${LOCKDC_DEPENDENCY_BUILD_ROOT}/openssl-shared/src/LICENSE.txt"
    "${LOCKDC_EXTERNAL_ROOT}/openssl-static/install/share/doc/liblockdc-third-party/openssl/LICENSE.txt"
    "${LOCKDC_DEPENDENCY_BUILD_ROOT}/openssl-static/src/LICENSE.txt"
)
lockdc_copy_required_license(
    "curl"
    "${package_root}/share/doc/liblockdc/third_party/curl"
    "${LOCKDC_EXTERNAL_ROOT}/curl-shared-cmake/install/share/doc/liblockdc-third-party/curl/LICENSE.txt"
    "${LOCKDC_DEPENDENCY_BUILD_ROOT}/curl-shared-cmake/src/COPYING"
    "${LOCKDC_EXTERNAL_ROOT}/curl-static/install/share/doc/liblockdc-third-party/curl/LICENSE.txt"
    "${LOCKDC_DEPENDENCY_BUILD_ROOT}/curl-static/src/COPYING"
)
lockdc_copy_required_license(
    "nghttp2"
    "${package_root}/share/doc/liblockdc/third_party/nghttp2"
    "${LOCKDC_EXTERNAL_ROOT}/nghttp2-shared/install/share/doc/liblockdc-third-party/nghttp2/LICENSE.txt"
    "${LOCKDC_DEPENDENCY_BUILD_ROOT}/nghttp2-shared/src/COPYING"
    "${LOCKDC_EXTERNAL_ROOT}/nghttp2-static/install/share/doc/liblockdc-third-party/nghttp2/LICENSE.txt"
    "${LOCKDC_DEPENDENCY_BUILD_ROOT}/nghttp2-static/src/COPYING"
)
lockdc_copy_required_license(
    "libssh2"
    "${package_root}/share/doc/liblockdc/third_party/libssh2"
    "${LOCKDC_EXTERNAL_ROOT}/libssh2/install/share/doc/liblockdc-third-party/libssh2/LICENSE.txt"
    "${LOCKDC_EXTERNAL_ROOT}/libssh2/install/share/doc/libssh2/COPYING"
    "${LOCKDC_DEPENDENCY_BUILD_ROOT}/libssh2/src/COPYING"
)
lockdc_copy_required_license(
    "lonejson"
    "${package_root}/share/doc/liblockdc/third_party/lonejson"
    "${LOCKDC_EXTERNAL_ROOT}/lonejson-shared/install/share/doc/liblonejson/LICENSE"
    "${LOCKDC_EXTERNAL_ROOT}/lonejson-shared/install/share/doc/liblockdc-third-party/lonejson/LICENSE.txt"
    "${LOCKDC_EXTERNAL_ROOT}/lonejson-static/install/share/doc/liblonejson/LICENSE"
    "${LOCKDC_EXTERNAL_ROOT}/lonejson-static/install/share/doc/liblockdc-third-party/lonejson/LICENSE.txt"
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
    COMMAND "${LOCKDC_TAR_BIN}" -cf "${archive_base}" --format=gnu .
    WORKING_DIRECTORY "${package_root}"
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

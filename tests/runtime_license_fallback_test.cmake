if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

set(test_root "${LOCKDC_BINARY_DIR}/runtime-license-fallback-test")
set(fake_repo_root "${test_root}/repo")
set(external_root "${test_root}/external")
set(dependency_build_root "${test_root}/deps-build")
set(binary_dir "${test_root}/binary")
set(public_include_dir "${test_root}/include/lc")
set(shared_lib_dir "${test_root}/lib")
set(runtime_archive "${fake_repo_root}/dist/liblockdc-1.2.3-x86_64-linux-gnu.tar.gz")

file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY
    "${fake_repo_root}"
    "${binary_dir}"
    "${public_include_dir}"
    "${shared_lib_dir}"
    "${external_root}/pslog-static/install/include"
    "${external_root}/pslog-static/install/lib"
    "${external_root}/curl-shared-cmake/install/lib"
    "${external_root}/libssh2/install/include"
    "${external_root}/libssh2/install/lib"
    "${external_root}/pslog-shared/install/lib"
    "${external_root}/openssl-shared/install/lib"
    "${external_root}/nghttp2-shared/install/lib"
    "${external_root}/lonejson-shared/install/lib"
    "${external_root}/curl-shared-cmake/install/share/doc/liblockdc-third-party/curl"
    "${external_root}/libssh2/install/share/doc/liblockdc-third-party/libssh2"
    "${external_root}/pslog-shared/install/share/doc/libpslog"
    "${external_root}/openssl-shared/install/share/doc/liblockdc-third-party/openssl"
    "${external_root}/nghttp2-shared/install/share/doc/liblockdc-third-party/nghttp2"
    "${external_root}/lonejson-shared/install/share/doc/liblonejson"
)

file(WRITE "${fake_repo_root}/LICENSE" "liblockdc test license\n")
file(WRITE "${fake_repo_root}/README.md" "liblockdc test readme\n")
file(WRITE "${public_include_dir}/lc.h" "/* public header */\n")
file(WRITE "${public_include_dir}/version.h" "/* version header */\n")
file(WRITE "${shared_lib_dir}/liblockdc.a" "static library placeholder\n")
file(WRITE "${shared_lib_dir}/liblockdc.so.1.2.3" "shared library placeholder\n")
file(WRITE "${binary_dir}/lockdc.pc" "prefix=/tmp\n")
file(WRITE "${binary_dir}/lockdcConfig.cmake" "set(lockdc_FOUND TRUE)\n")
file(WRITE "${binary_dir}/lockdcConfigVersion.cmake" "set(PACKAGE_VERSION 1.2.3)\n")
file(WRITE "${external_root}/pslog-static/install/include/pslog.h" "/* pslog header */\n")
file(WRITE "${external_root}/pslog-static/install/include/pslog_version.h" "/* pslog version header */\n")
file(WRITE "${external_root}/pslog-static/install/lib/libpslog.a" "pslog static\n")
file(WRITE "${external_root}/libssh2/install/include/libssh2.h" "/* libssh2 header */\n")
file(WRITE "${external_root}/libssh2/install/include/libssh2_publickey.h" "/* libssh2 publickey header */\n")
file(WRITE "${external_root}/libssh2/install/include/libssh2_sftp.h" "/* libssh2 sftp header */\n")
file(WRITE "${external_root}/libssh2/install/lib/libssh2.a" "libssh2 static\n")
file(WRITE "${external_root}/libssh2/install/lib/libssh2.so.1.0.1" "libssh2 shared version\n")
file(CREATE_LINK "libssh2.so.1.0.1" "${external_root}/libssh2/install/lib/libssh2.so.1" SYMBOLIC)
file(CREATE_LINK "libssh2.so.1" "${external_root}/libssh2/install/lib/libssh2.so" SYMBOLIC)

file(WRITE "${external_root}/curl-shared-cmake/install/lib/libcurl.so.4" "curl shared\n")
file(WRITE "${external_root}/pslog-shared/install/lib/libpslog.so.0" "pslog shared\n")
file(WRITE "${external_root}/openssl-shared/install/lib/libssl.so.3" "openssl ssl shared\n")
file(WRITE "${external_root}/openssl-shared/install/lib/libcrypto.so.3" "openssl crypto shared\n")
file(WRITE "${external_root}/nghttp2-shared/install/lib/libnghttp2.so.14" "nghttp2 shared\n")
file(WRITE "${external_root}/lonejson-shared/install/lib/liblonejson.so.0" "lonejson shared\n")

file(WRITE "${external_root}/curl-shared-cmake/install/share/doc/liblockdc-third-party/curl/LICENSE.txt" "curl license\n")
file(WRITE "${external_root}/libssh2/install/share/doc/liblockdc-third-party/libssh2/LICENSE.txt" "libssh2 license\n")
file(WRITE "${external_root}/pslog-shared/install/share/doc/libpslog/LICENSE" "pslog license\n")
file(WRITE "${external_root}/openssl-shared/install/share/doc/liblockdc-third-party/openssl/LICENSE.txt" "openssl license\n")
file(WRITE "${external_root}/nghttp2-shared/install/share/doc/liblockdc-third-party/nghttp2/LICENSE.txt" "nghttp2 license\n")
file(WRITE "${external_root}/lonejson-shared/install/share/doc/liblonejson/LICENSE" "lonejson license\n")

execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -DLOCKDC_BINARY_DIR=${binary_dir}
        -DLOCKDC_ROOT=${fake_repo_root}
        -DLOCKDC_EXTERNAL_ROOT=${external_root}
        -DLOCKDC_DEPENDENCY_BUILD_ROOT=${dependency_build_root}
        -DLOCKDC_PUBLIC_HEADER=${public_include_dir}/lc.h
        -DLOCKDC_PUBLIC_VERSION_HEADER=${public_include_dir}/version.h
        -DLOCKDC_STATIC_LIB=${shared_lib_dir}/liblockdc.a
        -DLOCKDC_SHARED_LIB=${shared_lib_dir}/liblockdc.so.1.2.3
        -DLOCKDC_SHARED_LIB_NAME=liblockdc.so.1.2.3
        -DLOCKDC_SHARED_SONAME=liblockdc.so.1
        -DLOCKDC_SHARED_LINK_NAME=liblockdc.so
        -DLOCKDC_PUBLIC_PKGCONFIG=${binary_dir}/lockdc.pc
        -DLOCKDC_PUBLIC_CMAKE_CONFIG=${binary_dir}/lockdcConfig.cmake
        -DLOCKDC_PUBLIC_CMAKE_CONFIG_VERSION=${binary_dir}/lockdcConfigVersion.cmake
        -DLOCKDC_VERSION=1.2.3
        -DLOCKDC_TARGET_ID=x86_64-linux-gnu
        -P "${LOCKDC_ROOT}/cmake/package_archive.cmake"
    RESULT_VARIABLE package_result
    OUTPUT_VARIABLE package_stdout
    ERROR_VARIABLE package_stderr
)
if(NOT package_result EQUAL 0)
    message(FATAL_ERROR
        "expected combined packaging with install-tree licenses to succeed\n"
        "stdout:\n${package_stdout}\n"
        "stderr:\n${package_stderr}")
endif()

if(NOT EXISTS "${runtime_archive}")
    message(FATAL_ERROR "expected runtime archive to exist: ${runtime_archive}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" -E tar tf "${runtime_archive}"
    RESULT_VARIABLE tar_result
    OUTPUT_VARIABLE archive_listing
    ERROR_VARIABLE tar_stderr
)
if(NOT tar_result EQUAL 0)
    message(FATAL_ERROR
        "failed to list package archive contents\n"
        "stderr:\n${tar_stderr}")
endif()

function(assert_archive_contains pattern description)
    if(NOT archive_listing MATCHES "${pattern}")
        message(FATAL_ERROR
            "expected combined archive to contain ${description}\n"
            "archive contents:\n${archive_listing}")
    endif()
endfunction()

assert_archive_contains("(^|\n)(\\./)?share/doc/liblockdc/third_party/openssl/LICENSE\\.txt(\n|$)" "OpenSSL staged license")
assert_archive_contains("(^|\n)(\\./)?share/doc/liblockdc/third_party/curl/LICENSE\\.txt(\n|$)" "curl staged license")
assert_archive_contains("(^|\n)(\\./)?share/doc/liblockdc/third_party/libssh2/LICENSE\\.txt(\n|$)" "libssh2 staged license")
assert_archive_contains("(^|\n)(\\./)?share/doc/liblockdc/third_party/libpslog/LICENSE(\n|$)" "libpslog staged license")
assert_archive_contains("(^|\n)(\\./)?share/doc/liblockdc/third_party/nghttp2/LICENSE\\.txt(\n|$)" "nghttp2 staged license")
assert_archive_contains("(^|\n)(\\./)?share/doc/liblockdc/third_party/lonejson/LICENSE(\n|$)" "lonejson staged license")
assert_archive_contains("(^|\n)(\\./)?lib/liblockdc\\.a(\n|$)" "static library")
assert_archive_contains("(^|\n)(\\./)?lib/liblockdc\\.so\\.1\\.2\\.3(\n|$)" "versioned shared library")
assert_archive_contains("(^|\n)(\\./)?lib/liblockdc\\.so\\.1(\n|$)" "shared library SONAME symlink")
assert_archive_contains("(^|\n)(\\./)?lib/liblockdc\\.so(\n|$)" "shared library linker symlink")

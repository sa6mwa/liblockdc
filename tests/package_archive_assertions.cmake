function(assert_contains archive_listing archive_path pattern description)
    if(NOT archive_listing MATCHES "${pattern}")
        message(FATAL_ERROR "archive missing ${description}: ${archive_path}")
    endif()
endfunction()

function(assert_not_contains archive_listing archive_path pattern description)
    if(archive_listing MATCHES "${pattern}")
        message(FATAL_ERROR "archive unexpectedly contains ${description}: ${archive_path}")
    endif()
endfunction()

function(assert_symlink_target extract_root archive_path symlink_path)
    if(NOT IS_SYMLINK "${extract_root}/${symlink_path}")
        message(FATAL_ERROR "expected symlink entry ${symlink_path} in ${archive_path}")
    endif()

    file(READ_SYMLINK "${extract_root}/${symlink_path}" symlink_target)
    get_filename_component(symlink_dir "${extract_root}/${symlink_path}" DIRECTORY)
    if(NOT EXISTS "${symlink_dir}/${symlink_target}")
        message(FATAL_ERROR
            "archive contains broken symlink ${symlink_path} -> ${symlink_target}: ${archive_path}")
    endif()
endfunction()

function(assert_archive_layout archive_path version target_id shared_lib_name shared_soname shared_link_name)
    if(NOT EXISTS "${archive_path}")
        message(FATAL_ERROR "missing archive: ${archive_path}")
    endif()

    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E tar tf "${archive_path}"
        RESULT_VARIABLE tar_result
        OUTPUT_VARIABLE archive_listing
    )
    if(NOT tar_result EQUAL 0)
        message(FATAL_ERROR "failed to list archive contents: ${archive_path}")
    endif()

    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?share/doc/liblockdc/LICENSE(\n|$)" "share/doc/liblockdc/LICENSE")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?share/doc/liblockdc/README.md(\n|$)" "share/doc/liblockdc/README.md")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?include/lc/lc\\.h(\n|$)" "include/lc/lc.h")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?include/lc/version\\.h(\n|$)" "include/lc/version.h")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?include/pslog\\.h(\n|$)" "include/pslog.h")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?include/pslog_version\\.h(\n|$)" "include/pslog_version.h")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?include/curl/curl\\.h(\n|$)" "curl headers")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?include/openssl/ssl\\.h(\n|$)" "OpenSSL headers")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?include/nghttp2/nghttp2\\.h(\n|$)" "nghttp2 headers")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?include/lonejson\\.h(\n|$)" "lonejson header")
    assert_not_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?share/liblockdc(/|\n|$)" "legacy share/liblockdc path")
    assert_not_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?liblockdc-[^/]+/" "legacy top-level archive prefix")
    string(REPLACE "." "\\." shared_name_regex "${shared_lib_name}")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?lib/${shared_name_regex}(\n|$)" "versioned shared library")
    if(DEFINED shared_soname
       AND NOT "${shared_soname}" STREQUAL ""
       AND NOT "${shared_soname}" STREQUAL "${shared_lib_name}")
        string(REPLACE "." "\\." shared_soname_regex "${shared_soname}")
        assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?lib/${shared_soname_regex}(\n|$)" "shared-library SONAME symlink")
    endif()
    if(DEFINED shared_link_name AND NOT "${shared_link_name}" STREQUAL "")
        string(REPLACE "." "\\." shared_link_regex "${shared_link_name}")
        assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?lib/${shared_link_regex}(\n|$)" "shared-library linker symlink")
    endif()

    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?lib/liblockdc\\.a(\n|$)" "static library")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?lib/libpslog\\.a(\n|$)" "libpslog static archive")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?lib/libcurl\\.a(\n|$)" "libcurl static archive")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?lib/libssl\\.a(\n|$)" "OpenSSL ssl static archive")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?lib/libcrypto\\.a(\n|$)" "OpenSSL crypto static archive")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?lib/libnghttp2\\.a(\n|$)" "nghttp2 static archive")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?lib/liblonejson\\.a(\n|$)" "lonejson static archive")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?lib/pkgconfig/lockdc\\.pc(\n|$)" "pkg-config metadata")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?lib/cmake/lockdc/lockdcConfig\\.cmake(\n|$)" "CMake package config")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?lib/cmake/lockdc/lockdcConfigVersion\\.cmake(\n|$)" "CMake package version file")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?share/doc/liblockdc/third_party/libpslog/LICENSE(\n|$)" "libpslog license")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?share/doc/liblockdc/third_party/openssl/LICENSE\\.txt(\n|$)" "OpenSSL license")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?share/doc/liblockdc/third_party/curl/LICENSE\\.txt(\n|$)" "curl license")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?share/doc/liblockdc/third_party/nghttp2/LICENSE\\.txt(\n|$)" "nghttp2 license")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?share/doc/liblockdc/third_party/lonejson/LICENSE(\n|$)" "lonejson license")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?lib/libcurl\\.so" "bundled curl shared library")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)(\\./)?lib/libpslog\\.so" "bundled libpslog shared library")

    string(RANDOM LENGTH 12 ALPHABET 0123456789abcdef extract_suffix)
    get_filename_component(archive_name "${archive_path}" NAME_WE)
    set(extract_root "${CMAKE_CURRENT_BINARY_DIR}/archive-assert-${archive_name}-${extract_suffix}")
    file(REMOVE_RECURSE "${extract_root}")
    file(MAKE_DIRECTORY "${extract_root}")
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E tar xf "${archive_path}"
        WORKING_DIRECTORY "${extract_root}"
        RESULT_VARIABLE extract_result
    )
    if(NOT extract_result EQUAL 0)
        message(FATAL_ERROR "failed to extract archive for validation: ${archive_path}")
    endif()
    if(DEFINED shared_soname
       AND NOT "${shared_soname}" STREQUAL ""
       AND NOT "${shared_soname}" STREQUAL "${shared_lib_name}")
        assert_symlink_target("${extract_root}" "${archive_path}" "lib/${shared_soname}")
    endif()
    if(DEFINED shared_link_name AND NOT "${shared_link_name}" STREQUAL "")
        assert_symlink_target("${extract_root}" "${archive_path}" "lib/${shared_link_name}")
    endif()
    file(REMOVE_RECURSE "${extract_root}")

    file(READ "${archive_path}" archive_xfl HEX OFFSET 8 LIMIT 1)
    string(TOLOWER "${archive_xfl}" archive_xfl)
    if(NOT archive_xfl STREQUAL "02")
        message(FATAL_ERROR "archive is not using gzip maximum compression header: ${archive_path}")
    endif()
endfunction()

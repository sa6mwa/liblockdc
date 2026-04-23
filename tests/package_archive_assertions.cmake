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

function(assert_shared_library_runpath extract_root archive_path shared_lib_name)
    find_program(LOCKDC_READELF_BIN NAMES readelf)
    if(NOT LOCKDC_READELF_BIN)
        message(FATAL_ERROR "readelf is required for archive shared-library validation")
    endif()

    execute_process(
        COMMAND "${LOCKDC_READELF_BIN}" -d "${extract_root}/lib/${shared_lib_name}"
        RESULT_VARIABLE readelf_result
        OUTPUT_VARIABLE readelf_output
        ERROR_VARIABLE readelf_error
    )
    if(NOT readelf_result EQUAL 0)
        message(FATAL_ERROR
            "failed to inspect shared library dynamic tags in ${archive_path}\n${readelf_output}${readelf_error}")
    endif()

    if(readelf_output MATCHES "\\(RUNPATH\\).*\\[/")
        message(FATAL_ERROR
            "archive contains absolute RUNPATH in lib/${shared_lib_name}: ${archive_path}\n${readelf_output}")
    endif()

    if(NOT readelf_output MATCHES "\\(RUNPATH\\).*\\[\\$ORIGIN\\]")
        message(FATAL_ERROR
            "archive shared library is missing relocatable $ORIGIN RUNPATH in lib/${shared_lib_name}: ${archive_path}\n${readelf_output}")
    endif()
endfunction()

function(assert_archive_numeric_owner_group archive_path)
    find_program(LOCKDC_TAR_BIN NAMES tar)
    if(NOT LOCKDC_TAR_BIN)
        message(FATAL_ERROR "tar is required for archive ownership validation")
    endif()

    execute_process(
        COMMAND "${LOCKDC_TAR_BIN}" --numeric-owner -tvf "${archive_path}"
        RESULT_VARIABLE tar_result
        OUTPUT_VARIABLE archive_metadata
        ERROR_VARIABLE archive_metadata_error
    )
    if(NOT tar_result EQUAL 0)
        message(FATAL_ERROR
            "failed to inspect archive ownership metadata: ${archive_path}\n${archive_metadata}${archive_metadata_error}")
    endif()

    string(REGEX REPLACE "\n$" "" archive_metadata_trimmed "${archive_metadata}")
    string(REPLACE "\n" ";" archive_metadata_lines "${archive_metadata_trimmed}")
    foreach(archive_metadata_line IN LISTS archive_metadata_lines)
        if(archive_metadata_line STREQUAL "")
            continue()
        endif()
        if(NOT archive_metadata_line MATCHES "^[^ ]+[ ]+0/0([ ]+|$)")
            message(FATAL_ERROR
                "archive entry does not use numeric owner/group 0/0: ${archive_path}\n${archive_metadata_line}")
        endif()
    endforeach()
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

    assert_archive_numeric_owner_group("${archive_path}")

    set(archive_prefix "liblockdc-${version}-${target_id}")
    string(REPLACE "." "\\." archive_prefix_regex "${archive_prefix}")
    string(REGEX REPLACE "\n$" "" archive_listing_trimmed "${archive_listing}")
    string(REPLACE "\n" ";" archive_entries "${archive_listing_trimmed}")
    foreach(archive_entry IN LISTS archive_entries)
        if(archive_entry STREQUAL "")
            continue()
        endif()
        if(archive_entry MATCHES "^\\./")
            message(FATAL_ERROR "archive contains invalid ./-prefixed entry '${archive_entry}': ${archive_path}")
        endif()
        if(NOT archive_entry MATCHES "^${archive_prefix_regex}(/|$)")
            message(FATAL_ERROR "archive contains entry outside release prefix '${archive_prefix}/': ${archive_entry} in ${archive_path}")
        endif()
    endforeach()

    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/(\n|$)" "archive root directory")

    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/share/doc/liblockdc/LICENSE(\n|$)" "share/doc/liblockdc/LICENSE")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/share/doc/liblockdc/README.md(\n|$)" "share/doc/liblockdc/README.md")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/include/lc/lc\\.h(\n|$)" "include/lc/lc.h")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/include/lc/version\\.h(\n|$)" "include/lc/version.h")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/include/pslog\\.h(\n|$)" "include/pslog.h")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/include/pslog_version\\.h(\n|$)" "include/pslog_version.h")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/include/curl/curl\\.h(\n|$)" "curl headers")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/include/openssl/ssl\\.h(\n|$)" "OpenSSL headers")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/include/nghttp2/nghttp2\\.h(\n|$)" "nghttp2 headers")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/include/libssh2\\.h(\n|$)" "libssh2 header")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/include/libssh2_publickey\\.h(\n|$)" "libssh2 publickey header")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/include/libssh2_sftp\\.h(\n|$)" "libssh2 sftp header")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/include/zlib\\.h(\n|$)" "zlib header")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/include/zconf\\.h(\n|$)" "zlib configuration header")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/include/lonejson\\.h(\n|$)" "lonejson header")
    assert_not_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/share/liblockdc(/|\n|$)" "engine share/liblockdc path")
    assert_not_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/share/lockdc/luarocks(/|\n|$)" "embedded LuaRocks payload")
    assert_not_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/share/lua/5\\.5/lockdc(/|\n|$)" "embedded Lua runtime wrapper")
    assert_not_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/lua/5\\.5/lockdc(/|\n|$)" "embedded Lua native module")
    string(REPLACE "." "\\." shared_name_regex "${shared_lib_name}")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/${shared_name_regex}(\n|$)" "versioned shared library")
    if(DEFINED shared_soname
       AND NOT "${shared_soname}" STREQUAL ""
       AND NOT "${shared_soname}" STREQUAL "${shared_lib_name}")
        string(REPLACE "." "\\." shared_soname_regex "${shared_soname}")
        assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/${shared_soname_regex}(\n|$)" "shared-library SONAME symlink")
    endif()
    if(DEFINED shared_link_name AND NOT "${shared_link_name}" STREQUAL "")
        string(REPLACE "." "\\." shared_link_regex "${shared_link_name}")
        assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/${shared_link_regex}(\n|$)" "shared-library linker symlink")
    endif()

    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/liblockdc\\.a(\n|$)" "static library")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libpslog\\.a(\n|$)" "libpslog static archive")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libcurl\\.a(\n|$)" "libcurl static archive")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libssl\\.a(\n|$)" "OpenSSL ssl static archive")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libcrypto\\.a(\n|$)" "OpenSSL crypto static archive")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libnghttp2\\.a(\n|$)" "nghttp2 static archive")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libssh2\\.a(\n|$)" "libssh2 static archive")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libz\\.a(\n|$)" "zlib static archive")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/liblonejson\\.a(\n|$)" "lonejson static archive")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/pkgconfig/lockdc\\.pc(\n|$)" "pkg-config metadata")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/cmake/lockdc/lockdcConfig\\.cmake(\n|$)" "CMake package config")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/cmake/lockdc/lockdcConfigVersion\\.cmake(\n|$)" "CMake package version file")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/share/doc/liblockdc/third_party/libpslog/LICENSE(\n|$)" "libpslog license")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/share/doc/liblockdc/third_party/openssl/LICENSE\\.txt(\n|$)" "OpenSSL license")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/share/doc/liblockdc/third_party/curl/LICENSE\\.txt(\n|$)" "curl license")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/share/doc/liblockdc/third_party/nghttp2/LICENSE\\.txt(\n|$)" "nghttp2 license")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/share/doc/liblockdc/third_party/libssh2/LICENSE\\.txt(\n|$)" "libssh2 license")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/share/doc/liblockdc/third_party/zlib/LICENSE\\.txt(\n|$)" "zlib license")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/share/doc/liblockdc/third_party/lonejson/LICENSE(\n|$)" "lonejson license")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libcurl\\.so" "bundled curl shared library")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libpslog\\.so" "bundled libpslog shared library")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libssh2\\.so" "bundled libssh2 shared library")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libssh2\\.so\\.1" "libssh2 shared-library SONAME symlink")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libssh2\\.so\\.1\\.0\\.1" "libssh2 versioned shared library")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libz\\.so" "bundled zlib shared library")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libz\\.so\\.1" "zlib shared-library SONAME symlink")
    string(REPLACE "." "\\." zlib_version_regex "${LOCKDC_ZLIB_VERSION}")
    assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libz\\.so\\.${zlib_version_regex}" "zlib versioned shared library")

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
        assert_symlink_target("${extract_root}" "${archive_path}" "${archive_prefix}/lib/${shared_soname}")
    endif()
    if(DEFINED shared_link_name AND NOT "${shared_link_name}" STREQUAL "")
        assert_symlink_target("${extract_root}" "${archive_path}" "${archive_prefix}/lib/${shared_link_name}")
    endif()
    assert_shared_library_runpath("${extract_root}/${archive_prefix}" "${archive_path}" "${shared_lib_name}")
    assert_symlink_target("${extract_root}" "${archive_path}" "${archive_prefix}/lib/libssh2.so")
    assert_symlink_target("${extract_root}" "${archive_path}" "${archive_prefix}/lib/libssh2.so.1")
    assert_symlink_target("${extract_root}" "${archive_path}" "${archive_prefix}/lib/libz.so")
    assert_symlink_target("${extract_root}" "${archive_path}" "${archive_prefix}/lib/libz.so.1")
    file(REMOVE_RECURSE "${extract_root}")

    file(READ "${archive_path}" archive_xfl HEX OFFSET 8 LIMIT 1)
    string(TOLOWER "${archive_xfl}" archive_xfl)
    if(NOT archive_xfl STREQUAL "02")
        message(FATAL_ERROR "archive is not using gzip maximum compression header: ${archive_path}")
    endif()
endfunction()
if(NOT DEFINED LOCKDC_ZLIB_VERSION OR LOCKDC_ZLIB_VERSION STREQUAL "")
  set(LOCKDC_ZLIB_VERSION "1.3.2")
endif()

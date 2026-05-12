include("${CMAKE_CURRENT_LIST_DIR}/release_privacy_scan.cmake")

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
    if(shared_lib_name MATCHES "\\.dylib$")
        if(DEFINED ENV{OSXCROSS_ROOT} AND NOT "$ENV{OSXCROSS_ROOT}" STREQUAL "")
            set(_lockdc_osxcross_bin_hint "$ENV{OSXCROSS_ROOT}/bin")
        elseif(DEFINED ENV{HOME} AND NOT "$ENV{HOME}" STREQUAL "")
            set(_lockdc_osxcross_bin_hint "$ENV{HOME}/.local/cross/osxcross/bin")
        else()
            set(_lockdc_osxcross_bin_hint "")
        endif()
        find_program(LOCKDC_OTOOL_BIN
            NAMES arm64-apple-darwin25-otool otool
            HINTS "${_lockdc_osxcross_bin_hint}"
        )
        if(NOT LOCKDC_OTOOL_BIN)
            message(FATAL_ERROR "otool is required for Darwin archive shared-library validation")
        endif()

        execute_process(
            COMMAND "${LOCKDC_OTOOL_BIN}" -l "${extract_root}/lib/${shared_lib_name}"
            RESULT_VARIABLE otool_result
            OUTPUT_VARIABLE otool_output
            ERROR_VARIABLE otool_error
        )
        if(NOT otool_result EQUAL 0)
            message(FATAL_ERROR
                "failed to inspect Darwin shared library load commands in ${archive_path}\n${otool_output}${otool_error}")
        endif()
        if(otool_output MATCHES "path /")
            message(FATAL_ERROR
                "archive contains absolute Darwin rpath in lib/${shared_lib_name}: ${archive_path}\n${otool_output}")
        endif()
        if(NOT otool_output MATCHES "path @loader_path")
            message(FATAL_ERROR
                "archive Darwin shared library is missing relocatable @loader_path rpath in lib/${shared_lib_name}: ${archive_path}\n${otool_output}")
        endif()
        return()
    endif()

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

function(assert_lockdc_owned_binaries_have_no_local_paths extract_root archive_path)
    find_program(LOCKDC_STRINGS_BIN NAMES strings)
    if(NOT LOCKDC_STRINGS_BIN)
        message(FATAL_ERROR "strings is required for archive local-path validation")
    endif()

    set(disallowed_paths "${LOCKDC_ROOT}" "$ENV{HOME}")
    file(GLOB lockdc_owned_binaries
        LIST_DIRECTORIES false
        "${extract_root}/lib/liblockdc.a"
        "${extract_root}/lib/liblockdc.so*"
        "${extract_root}/lib/liblockdc.*.dylib"
    )
    foreach(lockdc_owned_binary IN LISTS lockdc_owned_binaries)
        if(IS_SYMLINK "${lockdc_owned_binary}")
            continue()
        endif()
        execute_process(
            COMMAND "${LOCKDC_STRINGS_BIN}" "${lockdc_owned_binary}"
            RESULT_VARIABLE strings_result
            OUTPUT_VARIABLE strings_output
            ERROR_VARIABLE strings_error
        )
        if(NOT strings_result EQUAL 0)
            message(FATAL_ERROR
                "failed to inspect local paths in ${lockdc_owned_binary}: ${archive_path}\n${strings_error}")
        endif()
        foreach(disallowed_path IN LISTS disallowed_paths)
            if(disallowed_path STREQUAL "")
                continue()
            endif()
            string(FIND "${strings_output}" "${disallowed_path}" disallowed_index)
            if(NOT disallowed_index EQUAL -1)
                message(FATAL_ERROR
                    "archive liblockdc-owned binary contains local path '${disallowed_path}': "
                    "${lockdc_owned_binary} in ${archive_path}")
            endif()
        endforeach()
    endforeach()
endfunction()

function(assert_packaged_binaries_have_no_sanitizer_runtime extract_root archive_path)
    find_program(LOCKDC_STRINGS_BIN NAMES strings)
    if(NOT LOCKDC_STRINGS_BIN)
        message(FATAL_ERROR "strings is required for archive sanitizer validation")
    endif()

    file(GLOB packaged_binaries
        LIST_DIRECTORIES false
        "${extract_root}/lib/*.a"
        "${extract_root}/lib/*.so"
        "${extract_root}/lib/*.so.*"
        "${extract_root}/lib/*.dylib"
        "${extract_root}/lib/*.dll"
    )
    foreach(packaged_binary IN LISTS packaged_binaries)
        if(IS_SYMLINK "${packaged_binary}")
            continue()
        endif()
        execute_process(
            COMMAND "${LOCKDC_STRINGS_BIN}" "${packaged_binary}"
            RESULT_VARIABLE strings_result
            OUTPUT_VARIABLE strings_output
            ERROR_VARIABLE strings_error
        )
        if(NOT strings_result EQUAL 0)
            message(FATAL_ERROR
                "failed to inspect sanitizer references in ${packaged_binary}: ${archive_path}\n${strings_error}")
        endif()
        foreach(disallowed_sanitizer_reference
            "libasan"
            "libubsan"
            "__asan_"
            "__ubsan_"
            "AddressSanitizer"
            "UndefinedBehaviorSanitizer"
            "ASAN_OPTIONS"
            "UBSAN_OPTIONS"
            "-fsanitize=address"
            "-fsanitize=undefined"
        )
            string(FIND "${strings_output}" "${disallowed_sanitizer_reference}" sanitizer_reference_index)
            if(NOT sanitizer_reference_index EQUAL -1)
                message(FATAL_ERROR
                    "release archive binary contains sanitizer reference '${disallowed_sanitizer_reference}': "
                    "${packaged_binary} in ${archive_path}")
            endif()
        endforeach()
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
    if(target_id MATCHES "apple-darwin$")
        assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libcurl\\.dylib" "bundled curl dylib")
        assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libpslog\\.dylib" "bundled libpslog dylib")
        assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libpslog\\.0\\.dylib" "libpslog versioned dylib")
        assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libssh2\\.dylib" "bundled libssh2 dylib")
        assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libssh2\\.1\\.dylib" "libssh2 versioned dylib")
        assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libz\\.dylib" "bundled zlib dylib")
        assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libz\\.1\\.dylib" "zlib dylib SONAME symlink")
        string(REPLACE "." "\\." zlib_version_regex "${LOCKDC_ZLIB_VERSION}")
        assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libz\\.${zlib_version_regex}\\.dylib" "zlib versioned dylib")
    else()
        assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libcurl\\.so" "bundled curl shared library")
        assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libpslog\\.so" "bundled libpslog shared library")
        assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libssh2\\.so" "bundled libssh2 shared library")
        assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libssh2\\.so\\.1" "libssh2 shared-library SONAME symlink")
        assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libssh2\\.so\\.1\\.0\\.1" "libssh2 versioned shared library")
        assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libz\\.so" "bundled zlib shared library")
        assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libz\\.so\\.1" "zlib shared-library SONAME symlink")
        string(REPLACE "." "\\." zlib_version_regex "${LOCKDC_ZLIB_VERSION}")
        assert_contains("${archive_listing}" "${archive_path}" "(^|\n)${archive_prefix_regex}/lib/libz\\.so\\.${zlib_version_regex}" "zlib versioned shared library")
    endif()

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
    if(NOT DEFINED LOCKDC_SANITIZER_INSTRUMENTED OR LOCKDC_SANITIZER_INSTRUMENTED STREQUAL "" OR
       LOCKDC_SANITIZER_INSTRUMENTED STREQUAL "0")
        lockdc_assert_tree_has_no_private_traces("${extract_root}/${archive_prefix}" "${archive_path}")
        assert_packaged_binaries_have_no_sanitizer_runtime("${extract_root}/${archive_prefix}" "${archive_path}")
    endif()
    if(target_id MATCHES "apple-darwin$")
        assert_symlink_target("${extract_root}" "${archive_path}" "${archive_prefix}/lib/libpslog.dylib")
        assert_symlink_target("${extract_root}" "${archive_path}" "${archive_prefix}/lib/libssh2.dylib")
        assert_symlink_target("${extract_root}" "${archive_path}" "${archive_prefix}/lib/libz.dylib")
        assert_symlink_target("${extract_root}" "${archive_path}" "${archive_prefix}/lib/libz.1.dylib")
    else()
        assert_symlink_target("${extract_root}" "${archive_path}" "${archive_prefix}/lib/libssh2.so")
        assert_symlink_target("${extract_root}" "${archive_path}" "${archive_prefix}/lib/libssh2.so.1")
        assert_symlink_target("${extract_root}" "${archive_path}" "${archive_prefix}/lib/libz.so")
        assert_symlink_target("${extract_root}" "${archive_path}" "${archive_prefix}/lib/libz.so.1")
    endif()
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

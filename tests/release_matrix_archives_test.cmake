if(DEFINED LOCKDC_DIST_DIR AND NOT "${LOCKDC_DIST_DIR}" STREQUAL "")
    set(lockdc_dist_dir "${LOCKDC_DIST_DIR}")
else()
    set(lockdc_dist_dir "${LOCKDC_ROOT}/dist")
endif()

if(DEFINED LOCKDC_RELEASE_PRESETS AND NOT "${LOCKDC_RELEASE_PRESETS}" STREQUAL "")
    set(lockdc_release_presets ${LOCKDC_RELEASE_PRESETS})
else()
    set(lockdc_release_presets
        x86_64-linux-gnu-release
        x86_64-linux-musl-release
        aarch64-linux-gnu-release
        aarch64-linux-musl-release
        armhf-linux-gnu-release
        armhf-linux-musl-release
    )
endif()

include("${LOCKDC_ROOT}/tests/package_archive_assertions.cmake")

if(DEFINED LOCKDC_VERIFY_WORK_DIR AND NOT "${LOCKDC_VERIFY_WORK_DIR}" STREQUAL "")
    set(lockdc_verify_work_dir "${LOCKDC_VERIFY_WORK_DIR}")
else()
    set(lockdc_verify_work_dir "${LOCKDC_ROOT}/build/release-matrix-verify")
endif()
file(MAKE_DIRECTORY "${lockdc_verify_work_dir}")

set(lockdc_expected_artifacts "")
set(lockdc_expected_checksum_artifacts "")
set(lockdc_release_version "")

foreach(lockdc_preset IN LISTS lockdc_release_presets)
    set(lockdc_build_dir "${LOCKDC_ROOT}/build/${lockdc_preset}")
    set(lockdc_metadata "${lockdc_build_dir}/package-metadata.cmake")

    if(NOT EXISTS "${lockdc_metadata}")
        message(FATAL_ERROR "missing package metadata for release preset ${lockdc_preset}: ${lockdc_metadata}")
    endif()

    unset(LOCKDC_VERSION)
    unset(LOCKDC_TARGET_ID)
    unset(LOCKDC_SHARED_LIB_NAME)
    unset(LOCKDC_SHARED_SONAME)
    unset(LOCKDC_SHARED_LINK_NAME)
    unset(LOCKDC_SANITIZER_INSTRUMENTED)
    include("${lockdc_metadata}")

    if(DEFINED LOCKDC_SANITIZER_INSTRUMENTED
       AND NOT LOCKDC_SANITIZER_INSTRUMENTED STREQUAL ""
       AND NOT LOCKDC_SANITIZER_INSTRUMENTED STREQUAL "0")
        message(FATAL_ERROR
            "release preset ${lockdc_preset} is sanitizer-instrumented; "
            "release artifacts must not be built with ASan/UBSan")
    endif()

    if(lockdc_release_version STREQUAL "")
        set(lockdc_release_version "${LOCKDC_VERSION}")
    elseif(NOT lockdc_release_version STREQUAL LOCKDC_VERSION)
        message(FATAL_ERROR
            "release preset ${lockdc_preset} resolved version ${LOCKDC_VERSION}, expected ${lockdc_release_version}")
    endif()

    set(lockdc_release_archive "${lockdc_dist_dir}/liblockdc-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}.tar.gz")

    assert_archive_layout("${lockdc_release_archive}" "${LOCKDC_VERSION}" "${LOCKDC_TARGET_ID}"
        "${LOCKDC_SHARED_LIB_NAME}" "${LOCKDC_SHARED_SONAME}" "${LOCKDC_SHARED_LINK_NAME}")

    list(APPEND lockdc_expected_artifacts
        "liblockdc-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}.tar.gz"
    )
    list(APPEND lockdc_expected_checksum_artifacts
        "liblockdc-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}.tar.gz"
    )
endforeach()

if(lockdc_release_version STREQUAL "")
    message(FATAL_ERROR "no release presets were provided for archive verification")
endif()

list(APPEND lockdc_expected_artifacts
    "liblockdc-${lockdc_release_version}.tar.gz"
    "liblockdc-lua-${lockdc_release_version}.tar.gz"
    "lockdc-${lockdc_release_version}-1.rockspec"
    "lockdc-${lockdc_release_version}-1.src.rock"
)
list(APPEND lockdc_expected_checksum_artifacts
    "liblockdc-${lockdc_release_version}.tar.gz"
    "liblockdc-lua-${lockdc_release_version}.tar.gz"
    "lockdc-${lockdc_release_version}-1.rockspec"
    "lockdc-${lockdc_release_version}-1.src.rock"
)
list(SORT lockdc_expected_artifacts)
list(SORT lockdc_expected_checksum_artifacts)
list(LENGTH lockdc_expected_checksum_artifacts lockdc_expected_checksum_artifact_count)

file(GLOB lockdc_actual_artifacts RELATIVE "${lockdc_dist_dir}" "${lockdc_dist_dir}/*")
list(FILTER lockdc_actual_artifacts EXCLUDE REGEX "^liblockdc-${lockdc_release_version}-CHECKSUMS$")
list(SORT lockdc_actual_artifacts)
if(NOT lockdc_actual_artifacts STREQUAL lockdc_expected_artifacts)
    list(JOIN lockdc_expected_artifacts "\n  " lockdc_expected_artifact_list)
    list(JOIN lockdc_actual_artifacts "\n  " lockdc_actual_artifact_list)
    message(FATAL_ERROR
        "release artifact set mismatch for ${lockdc_release_version}\nexpected:\n  ${lockdc_expected_artifact_list}\nactual:\n  ${lockdc_actual_artifact_list}")
endif()

set(lockdc_lua_rockspec_path "${lockdc_dist_dir}/lockdc-${lockdc_release_version}-1.rockspec")
set(lockdc_lua_src_rock_path "${lockdc_dist_dir}/lockdc-${lockdc_release_version}-1.src.rock")
set(lockdc_lua_source_archive_path "${lockdc_dist_dir}/liblockdc-lua-${lockdc_release_version}.tar.gz")
foreach(required_path
    "${lockdc_lua_rockspec_path}"
    "${lockdc_lua_src_rock_path}"
    "${lockdc_lua_source_archive_path}"
)
    if(NOT EXISTS "${required_path}")
        message(FATAL_ERROR "missing standalone Lua release artifact: ${required_path}")
    endif()
endforeach()

file(READ "${lockdc_lua_rockspec_path}" lockdc_lua_rockspec_text)
foreach(required_snippet
    "package = \"lockdc\""
    "version = \"${lockdc_release_version}-1\""
    "tag = \"v${lockdc_release_version}\""
)
    string(FIND "${lockdc_lua_rockspec_text}" "${required_snippet}" snippet_index)
    if(snippet_index EQUAL -1)
        message(FATAL_ERROR
            "standalone Lua rockspec is missing expected snippet '${required_snippet}'\n"
            "rockspec:\n${lockdc_lua_rockspec_text}")
    endif()
endforeach()

set(lockdc_checksums_name "liblockdc-${lockdc_release_version}-CHECKSUMS")
set(lockdc_checksums_path "${lockdc_dist_dir}/${lockdc_checksums_name}")
if(NOT EXISTS "${lockdc_checksums_path}")
    message(FATAL_ERROR "missing checksum manifest: ${lockdc_checksums_path}")
endif()

foreach(lockdc_release_artifact IN LISTS lockdc_actual_artifacts)
    lockdc_assert_release_artifact_has_no_private_traces(
        "${lockdc_dist_dir}/${lockdc_release_artifact}"
        "dist release artifact"
    )
endforeach()
lockdc_assert_file_has_no_private_traces(
    "${lockdc_checksums_path}"
    "dist release artifact"
)

set(lockdc_source_archive_path "${lockdc_dist_dir}/liblockdc-${lockdc_release_version}.tar.gz")
set(lockdc_source_archive_root "liblockdc-${lockdc_release_version}")
if(NOT EXISTS "${lockdc_source_archive_path}")
    message(FATAL_ERROR "missing source archive: ${lockdc_source_archive_path}")
endif()
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E tar tf "${lockdc_source_archive_path}"
    OUTPUT_VARIABLE lockdc_source_listing
    RESULT_VARIABLE lockdc_source_listing_result
)
if(NOT lockdc_source_listing_result EQUAL 0)
    message(FATAL_ERROR "failed to list source archive: ${lockdc_source_archive_path}")
endif()
string(REGEX REPLACE "\n$" "" lockdc_source_listing_trimmed "${lockdc_source_listing}")
string(REPLACE "\n" ";" lockdc_source_entries "${lockdc_source_listing_trimmed}")
set(lockdc_source_actual_manifest "")
foreach(lockdc_source_entry IN LISTS lockdc_source_entries)
    if(lockdc_source_entry STREQUAL "")
        continue()
    endif()
    if(lockdc_source_entry MATCHES "^\\./")
        message(FATAL_ERROR "source archive contains invalid ./-prefixed entry: ${lockdc_source_entry}")
    endif()
    if(NOT lockdc_source_entry MATCHES "^${lockdc_source_archive_root}(/|$)")
        message(FATAL_ERROR
            "source archive contains entry outside ${lockdc_source_archive_root}/: ${lockdc_source_entry}")
    endif()
    if(lockdc_source_entry MATCHES "/$")
        continue()
    endif()
    string(REGEX REPLACE "^${lockdc_source_archive_root}/" "" lockdc_source_relative_entry "${lockdc_source_entry}")
    if(NOT lockdc_source_relative_entry STREQUAL "${lockdc_source_archive_root}")
        list(APPEND lockdc_source_actual_manifest "${lockdc_source_relative_entry}")
    endif()
endforeach()
list(SORT lockdc_source_actual_manifest)
foreach(lockdc_required_source_entry
    "VERSION"
    "RELEASE_MANIFEST"
    "CMakeLists.txt"
    "include/lc/lc.h"
    "cmake/LcVersion.cmake"
    "scripts/stage_release_sources.sh"
)
    list(FIND lockdc_source_actual_manifest "${lockdc_required_source_entry}" lockdc_required_source_index)
    if(lockdc_required_source_index EQUAL -1)
        message(FATAL_ERROR
            "source archive is missing required entry ${lockdc_required_source_entry}: ${lockdc_source_archive_path}")
    endif()
endforeach()
foreach(lockdc_forbidden_source_entry
    ".git"
    ".cache"
    "build"
    "dist"
)
    foreach(lockdc_source_actual_entry IN LISTS lockdc_source_actual_manifest)
        if(lockdc_source_actual_entry MATCHES "^${lockdc_forbidden_source_entry}(/|$)")
            message(FATAL_ERROR
                "source archive contains forbidden entry ${lockdc_source_actual_entry}: ${lockdc_source_archive_path}")
        endif()
    endforeach()
endforeach()
foreach(lockdc_source_actual_entry IN LISTS lockdc_source_actual_manifest)
    if(lockdc_source_actual_entry MATCHES "^devenv/volumes/"
       AND NOT lockdc_source_actual_entry MATCHES "/\\.gitkeep$")
        message(FATAL_ERROR
            "source archive contains forbidden generated devenv entry ${lockdc_source_actual_entry}: ${lockdc_source_archive_path}")
    endif()
endforeach()
if(EXISTS "${LOCKDC_ROOT}/.git")
    set(lockdc_expected_source_manifest_path "${lockdc_verify_work_dir}/lockdc-source-expected-manifest.txt")
    set(lockdc_ignored_source_manifest_path "${lockdc_verify_work_dir}/lockdc-source-ignored-manifest.txt")
    set(lockdc_actual_source_manifest_path "${lockdc_verify_work_dir}/lockdc-source-actual-manifest.txt")
    execute_process(
        COMMAND git -C "${LOCKDC_ROOT}" ls-files --cached --modified --others --exclude-standard
        OUTPUT_FILE "${lockdc_expected_source_manifest_path}"
        RESULT_VARIABLE lockdc_git_ls_result
    )
    if(lockdc_git_ls_result EQUAL 0)
        execute_process(
            COMMAND git -C "${LOCKDC_ROOT}" check-ignore --no-index --stdin
            INPUT_FILE "${lockdc_expected_source_manifest_path}"
            OUTPUT_FILE "${lockdc_ignored_source_manifest_path}"
            RESULT_VARIABLE lockdc_check_ignore_result
            ERROR_QUIET
        )
        file(READ "${lockdc_expected_source_manifest_path}" lockdc_expected_source_manifest_text)
        string(REGEX REPLACE "\n$" "" lockdc_expected_source_manifest_text "${lockdc_expected_source_manifest_text}")
        string(REPLACE "\n" ";" lockdc_expected_source_manifest "${lockdc_expected_source_manifest_text}")
        if(EXISTS "${lockdc_ignored_source_manifest_path}")
            file(READ "${lockdc_ignored_source_manifest_path}" lockdc_ignored_source_manifest_text)
            string(REGEX REPLACE "\n$" "" lockdc_ignored_source_manifest_text "${lockdc_ignored_source_manifest_text}")
            string(REPLACE "\n" ";" lockdc_ignored_source_manifest "${lockdc_ignored_source_manifest_text}")
            foreach(lockdc_ignored_source_entry IN LISTS lockdc_ignored_source_manifest)
                if(NOT lockdc_ignored_source_entry STREQUAL "")
                    list(REMOVE_ITEM lockdc_expected_source_manifest "${lockdc_ignored_source_entry}")
                endif()
            endforeach()
        endif()
        foreach(lockdc_expected_source_entry IN LISTS lockdc_expected_source_manifest)
            if(NOT EXISTS "${LOCKDC_ROOT}/${lockdc_expected_source_entry}")
                list(REMOVE_ITEM lockdc_expected_source_manifest "${lockdc_expected_source_entry}")
            endif()
        endforeach()
        list(APPEND lockdc_expected_source_manifest "VERSION" "RELEASE_MANIFEST")
        list(REMOVE_DUPLICATES lockdc_expected_source_manifest)
        list(SORT lockdc_expected_source_manifest)
        file(WRITE "${lockdc_expected_source_manifest_path}" "")
        foreach(lockdc_expected_source_entry IN LISTS lockdc_expected_source_manifest)
            file(APPEND "${lockdc_expected_source_manifest_path}" "${lockdc_expected_source_entry}\n")
        endforeach()
        file(WRITE "${lockdc_actual_source_manifest_path}" "")
        foreach(lockdc_actual_source_entry IN LISTS lockdc_source_actual_manifest)
            file(APPEND "${lockdc_actual_source_manifest_path}" "${lockdc_actual_source_entry}\n")
        endforeach()
        execute_process(
            COMMAND "${CMAKE_COMMAND}" -E compare_files
                "${lockdc_expected_source_manifest_path}"
                "${lockdc_actual_source_manifest_path}"
            RESULT_VARIABLE lockdc_manifest_compare_result
        )
        if(NOT lockdc_manifest_compare_result EQUAL 0)
            message(FATAL_ERROR
                "source archive does not match git-tracked non-ignored manifest\n"
                "expected: ${lockdc_expected_source_manifest_path}\n"
                "actual: ${lockdc_actual_source_manifest_path}")
        endif()
    endif()
endif()

execute_process(
    COMMAND sha256sum --check "${lockdc_checksums_name}"
    WORKING_DIRECTORY "${lockdc_dist_dir}"
    RESULT_VARIABLE lockdc_checksum_verify_result
    OUTPUT_VARIABLE lockdc_checksum_verify_output
    ERROR_VARIABLE lockdc_checksum_verify_error
)
if(NOT lockdc_checksum_verify_result EQUAL 0)
    message(FATAL_ERROR
        "checksum verification failed for ${lockdc_checksums_path}\n${lockdc_checksum_verify_output}${lockdc_checksum_verify_error}")
endif()

file(STRINGS "${lockdc_checksums_path}" lockdc_checksum_lines)
list(LENGTH lockdc_checksum_lines lockdc_checksum_line_count)
if(NOT lockdc_checksum_line_count EQUAL lockdc_expected_checksum_artifact_count)
    message(FATAL_ERROR
        "checksum manifest line count mismatch for ${lockdc_checksums_path}: expected ${lockdc_expected_checksum_artifact_count}, got ${lockdc_checksum_line_count}")
endif()

foreach(lockdc_expected_artifact IN LISTS lockdc_expected_checksum_artifacts)
    set(lockdc_found_checksum 0)
    foreach(lockdc_checksum_line IN LISTS lockdc_checksum_lines)
        if(lockdc_checksum_line MATCHES "^[0-9a-f]+[ \t]+\\*?${lockdc_expected_artifact}$")
            set(lockdc_found_checksum 1)
            break()
        endif()
    endforeach()
    if(NOT lockdc_found_checksum)
        message(FATAL_ERROR
            "checksum manifest is missing entry for ${lockdc_expected_artifact}: ${lockdc_checksums_path}")
    endif()
endforeach()

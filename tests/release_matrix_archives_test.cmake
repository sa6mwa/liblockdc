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

set(lockdc_expected_archives "")
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
    include("${lockdc_metadata}")

    if(lockdc_release_version STREQUAL "")
        set(lockdc_release_version "${LOCKDC_VERSION}")
    elseif(NOT lockdc_release_version STREQUAL LOCKDC_VERSION)
        message(FATAL_ERROR
            "release preset ${lockdc_preset} resolved version ${LOCKDC_VERSION}, expected ${lockdc_release_version}")
    endif()

    set(lockdc_release_archive "${lockdc_dist_dir}/liblockdc-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}.tar.gz")

    assert_archive_layout("${lockdc_release_archive}" "${LOCKDC_VERSION}" "${LOCKDC_TARGET_ID}"
        "${LOCKDC_SHARED_LIB_NAME}" "${LOCKDC_SHARED_SONAME}" "${LOCKDC_SHARED_LINK_NAME}")

    list(APPEND lockdc_expected_archives
        "liblockdc-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}.tar.gz"
    )
endforeach()

if(lockdc_release_version STREQUAL "")
    message(FATAL_ERROR "no release presets were provided for archive verification")
endif()

list(SORT lockdc_expected_archives)
list(LENGTH lockdc_expected_archives lockdc_expected_archive_count)

file(GLOB lockdc_actual_archives RELATIVE "${lockdc_dist_dir}" "${lockdc_dist_dir}/liblockdc-${lockdc_release_version}-*.tar.gz")
list(SORT lockdc_actual_archives)
if(NOT lockdc_actual_archives STREQUAL lockdc_expected_archives)
    list(JOIN lockdc_expected_archives "\n  " lockdc_expected_archive_list)
    list(JOIN lockdc_actual_archives "\n  " lockdc_actual_archive_list)
    message(FATAL_ERROR
        "release archive set mismatch for ${lockdc_release_version}\nexpected:\n  ${lockdc_expected_archive_list}\nactual:\n  ${lockdc_actual_archive_list}")
endif()

set(lockdc_checksums_name "liblockdc-${lockdc_release_version}-CHECKSUMS")
set(lockdc_checksums_path "${lockdc_dist_dir}/${lockdc_checksums_name}")
if(NOT EXISTS "${lockdc_checksums_path}")
    message(FATAL_ERROR "missing checksum manifest: ${lockdc_checksums_path}")
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
if(NOT lockdc_checksum_line_count EQUAL lockdc_expected_archive_count)
    message(FATAL_ERROR
        "checksum manifest line count mismatch for ${lockdc_checksums_path}: expected ${lockdc_expected_archive_count}, got ${lockdc_checksum_line_count}")
endif()

foreach(lockdc_expected_archive IN LISTS lockdc_expected_archives)
    set(lockdc_found_checksum 0)
    foreach(lockdc_checksum_line IN LISTS lockdc_checksum_lines)
        if(lockdc_checksum_line MATCHES "^[0-9a-f]+[ \t]+\\*?${lockdc_expected_archive}$")
            set(lockdc_found_checksum 1)
            break()
        endif()
    endforeach()
    if(NOT lockdc_found_checksum)
        message(FATAL_ERROR
            "checksum manifest is missing entry for ${lockdc_expected_archive}: ${lockdc_checksums_path}")
    endif()
endforeach()

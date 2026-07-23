if(NOT DEFINED LOCKDC_ARCHIVE_COMPONENT OR LOCKDC_ARCHIVE_COMPONENT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_ARCHIVE_COMPONENT is required")
endif()
if(NOT DEFINED LOCKDC_ARCHIVE_URL OR LOCKDC_ARCHIVE_URL STREQUAL "")
    message(FATAL_ERROR "LOCKDC_ARCHIVE_URL is required")
endif()
if(NOT DEFINED LOCKDC_ARCHIVE_SHA256 OR LOCKDC_ARCHIVE_SHA256 STREQUAL "")
    message(FATAL_ERROR "LOCKDC_ARCHIVE_SHA256 is required")
endif()
if(NOT DEFINED LOCKDC_ARCHIVE_NAME OR LOCKDC_ARCHIVE_NAME STREQUAL "")
    message(FATAL_ERROR "LOCKDC_ARCHIVE_NAME is required")
endif()
if(NOT DEFINED LOCKDC_ARCHIVE_OUTPUT OR LOCKDC_ARCHIVE_OUTPUT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_ARCHIVE_OUTPUT is required")
endif()

string(TOLOWER "${LOCKDC_ARCHIVE_SHA256}" lockdc_archive_sha256)
string(LENGTH "${lockdc_archive_sha256}" lockdc_archive_sha256_length)
if(NOT lockdc_archive_sha256_length EQUAL 64 OR NOT lockdc_archive_sha256 MATCHES "^[0-9a-f]+$")
    message(FATAL_ERROR
        "${LOCKDC_ARCHIVE_COMPONENT} archive SHA-256 is invalid: ${LOCKDC_ARCHIVE_SHA256}")
endif()

if(NOT LOCKDC_ARCHIVE_URL MATCHES "^https://")
    if(NOT DEFINED LOCKDC_ARCHIVE_ALLOW_FILE_URL OR NOT LOCKDC_ARCHIVE_ALLOW_FILE_URL)
        message(FATAL_ERROR
            "${LOCKDC_ARCHIVE_COMPONENT} archive URL must be an HTTPS release asset: ${LOCKDC_ARCHIVE_URL}")
    endif()
    if(NOT LOCKDC_ARCHIVE_URL MATCHES "^file://")
        message(FATAL_ERROR
            "${LOCKDC_ARCHIVE_COMPONENT} test archive URL must use file:// when LOCKDC_ARCHIVE_ALLOW_FILE_URL is set: ${LOCKDC_ARCHIVE_URL}")
    endif()
endif()

if(NOT DEFINED CPKT_DEPENDENCY_CACHE OR CPKT_DEPENDENCY_CACHE STREQUAL "")
    if(DEFINED ENV{CPKT_DEPENDENCY_CACHE} AND NOT "$ENV{CPKT_DEPENDENCY_CACHE}" STREQUAL "")
        set(CPKT_DEPENDENCY_CACHE "$ENV{CPKT_DEPENDENCY_CACHE}")
    elseif(DEFINED ENV{XDG_CACHE_HOME} AND NOT "$ENV{XDG_CACHE_HOME}" STREQUAL "")
        set(CPKT_DEPENDENCY_CACHE "$ENV{XDG_CACHE_HOME}/c.pkt.systems/deps")
    elseif(DEFINED ENV{HOME} AND NOT "$ENV{HOME}" STREQUAL "")
        set(CPKT_DEPENDENCY_CACHE "$ENV{HOME}/.cache/c.pkt.systems/deps")
    else()
        message(FATAL_ERROR
            "unable to resolve CPKT_DEPENDENCY_CACHE; set CPKT_DEPENDENCY_CACHE, XDG_CACHE_HOME, or HOME")
    endif()
endif()

if(NOT DEFINED LOCKDC_ARCHIVE_TIMEOUT OR LOCKDC_ARCHIVE_TIMEOUT STREQUAL "")
    set(LOCKDC_ARCHIVE_TIMEOUT 300)
endif()
if(NOT DEFINED LOCKDC_ARCHIVE_LOCK_TIMEOUT OR LOCKDC_ARCHIVE_LOCK_TIMEOUT STREQUAL "")
    set(LOCKDC_ARCHIVE_LOCK_TIMEOUT 600)
endif()

get_filename_component(CPKT_DEPENDENCY_CACHE "${CPKT_DEPENDENCY_CACHE}" ABSOLUTE)
set(lockdc_archive_dir
    "${CPKT_DEPENDENCY_CACHE}/archives/sha256/${lockdc_archive_sha256}")
set(lockdc_archive_path "${lockdc_archive_dir}/${LOCKDC_ARCHIVE_NAME}")
set(lockdc_lock_dir "${CPKT_DEPENDENCY_CACHE}/locks")
set(lockdc_lock_path "${lockdc_lock_dir}/${lockdc_archive_sha256}.lock")

file(MAKE_DIRECTORY "${lockdc_archive_dir}" "${lockdc_lock_dir}")
file(LOCK "${lockdc_lock_path}"
    TIMEOUT "${LOCKDC_ARCHIVE_LOCK_TIMEOUT}"
    RESULT_VARIABLE lockdc_lock_result)
if(NOT lockdc_lock_result EQUAL 0)
    message(FATAL_ERROR
        "timed out waiting for ${LOCKDC_ARCHIVE_COMPONENT} archive cache lock: ${lockdc_lock_path}")
endif()

set(lockdc_cache_hit OFF)
if(EXISTS "${lockdc_archive_path}")
    file(SHA256 "${lockdc_archive_path}" lockdc_existing_sha256)
    string(TOLOWER "${lockdc_existing_sha256}" lockdc_existing_sha256)
    if(lockdc_existing_sha256 STREQUAL lockdc_archive_sha256)
        set(lockdc_cache_hit ON)
    else()
        file(REMOVE "${lockdc_archive_path}")
    endif()
endif()

if(NOT lockdc_cache_hit)
    string(RANDOM LENGTH 16 ALPHABET 0123456789abcdef lockdc_tmp_suffix)
    set(lockdc_tmp_path "${lockdc_archive_path}.tmp.${lockdc_tmp_suffix}")
    file(REMOVE "${lockdc_tmp_path}")
    file(DOWNLOAD
        "${LOCKDC_ARCHIVE_URL}"
        "${lockdc_tmp_path}"
        TIMEOUT "${LOCKDC_ARCHIVE_TIMEOUT}"
        INACTIVITY_TIMEOUT 60
        TLS_VERIFY ON
        STATUS lockdc_download_status
        LOG lockdc_download_log)
    list(GET lockdc_download_status 0 lockdc_download_code)
    list(GET lockdc_download_status 1 lockdc_download_message)
    if(NOT lockdc_download_code EQUAL 0)
        file(REMOVE "${lockdc_tmp_path}")
        message(FATAL_ERROR
            "failed to acquire ${LOCKDC_ARCHIVE_COMPONENT} archive\n"
            "url=${LOCKDC_ARCHIVE_URL}\n"
            "expected_sha256=${lockdc_archive_sha256}\n"
            "cache_path=${lockdc_archive_path}\n"
            "status=${lockdc_download_code}: ${lockdc_download_message}\n"
            "${lockdc_download_log}")
    endif()

    file(SHA256 "${lockdc_tmp_path}" lockdc_tmp_sha256)
    string(TOLOWER "${lockdc_tmp_sha256}" lockdc_tmp_sha256)
    if(NOT lockdc_tmp_sha256 STREQUAL lockdc_archive_sha256)
        file(REMOVE "${lockdc_tmp_path}")
        message(FATAL_ERROR
            "${LOCKDC_ARCHIVE_COMPONENT} archive checksum mismatch\n"
            "url=${LOCKDC_ARCHIVE_URL}\n"
            "expected=${lockdc_archive_sha256}\n"
            "actual=${lockdc_tmp_sha256}\n"
            "cache_path=${lockdc_archive_path}")
    endif()

    file(RENAME "${lockdc_tmp_path}" "${lockdc_archive_path}")
endif()

get_filename_component(lockdc_output_dir "${LOCKDC_ARCHIVE_OUTPUT}" DIRECTORY)
file(MAKE_DIRECTORY "${lockdc_output_dir}")
file(COPY_FILE "${lockdc_archive_path}" "${LOCKDC_ARCHIVE_OUTPUT}" ONLY_IF_DIFFERENT)
file(SHA256 "${LOCKDC_ARCHIVE_OUTPUT}" lockdc_output_sha256)
string(TOLOWER "${lockdc_output_sha256}" lockdc_output_sha256)
if(NOT lockdc_output_sha256 STREQUAL lockdc_archive_sha256)
    message(FATAL_ERROR
        "${LOCKDC_ARCHIVE_COMPONENT} local archive copy checksum mismatch\n"
        "expected=${lockdc_archive_sha256}\n"
        "actual=${lockdc_output_sha256}\n"
        "output=${LOCKDC_ARCHIVE_OUTPUT}")
endif()

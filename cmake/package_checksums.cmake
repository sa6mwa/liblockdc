if(DEFINED LOCKDC_DIST_DIR AND NOT "${LOCKDC_DIST_DIR}" STREQUAL "")
    set(dist_dir "${LOCKDC_DIST_DIR}")
else()
    set(dist_dir "${LOCKDC_ROOT}/dist")
endif()
if((NOT DEFINED LOCKDC_VERSION OR "${LOCKDC_VERSION}" STREQUAL "")
   AND DEFINED LOCKDC_BINARY_DIR
   AND EXISTS "${LOCKDC_BINARY_DIR}/package-metadata.cmake")
    include("${LOCKDC_BINARY_DIR}/package-metadata.cmake")
endif()
set(checksums_name "liblockdc-${LOCKDC_VERSION}-CHECKSUMS")
set(checksums_path "${dist_dir}/${checksums_name}")

file(MAKE_DIRECTORY "${dist_dir}")
file(GLOB release_entries RELATIVE "${dist_dir}" "${dist_dir}/liblockdc-${LOCKDC_VERSION}-*")

set(checksum_inputs "")
foreach(entry IN LISTS release_entries)
    if(NOT entry STREQUAL "${checksums_name}")
        list(APPEND checksum_inputs "${entry}")
    endif()
endforeach()

list(LENGTH checksum_inputs checksum_input_count)
if(checksum_input_count EQUAL 0)
    message(FATAL_ERROR "no release artifacts found in ${dist_dir} for version ${LOCKDC_VERSION}")
endif()

list(SORT checksum_inputs)

execute_process(
    COMMAND sha256sum ${checksum_inputs}
    WORKING_DIRECTORY "${dist_dir}"
    OUTPUT_FILE "${checksums_path}"
    RESULT_VARIABLE checksum_result
)

if(NOT checksum_result EQUAL 0)
    message(FATAL_ERROR "failed to create ${checksums_name}")
endif()

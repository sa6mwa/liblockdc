string(RANDOM LENGTH 12 ALPHABET 0123456789abcdef lockdc_test_suffix)
set(lockdc_test_dist_dir "${LOCKDC_BINARY_DIR}/test-artifacts/package-archives-test-${lockdc_test_suffix}")

set(release_archive "${lockdc_test_dist_dir}/liblockdc-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}.tar.gz")

file(REMOVE_RECURSE "${lockdc_test_dist_dir}")
file(MAKE_DIRECTORY "${lockdc_test_dist_dir}")

execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -DLOCKDC_BINARY_DIR=${LOCKDC_BINARY_DIR}
        -DLOCKDC_ROOT=${LOCKDC_ROOT}
        -DLOCKDC_DIST_DIR=${lockdc_test_dist_dir}
        -P "${LOCKDC_ROOT}/cmake/package_archive.cmake"
    RESULT_VARIABLE package_build_result
)
if(NOT package_build_result EQUAL 0)
    message(FATAL_ERROR "failed to build release package archive")
endif()

include("${LOCKDC_ROOT}/tests/package_archive_assertions.cmake")

assert_archive_layout("${release_archive}" "${LOCKDC_VERSION}" "${LOCKDC_TARGET_ID}"
    "${LOCKDC_SHARED_LIB_NAME}" "${LOCKDC_SHARED_SONAME}" "${LOCKDC_SHARED_LINK_NAME}")

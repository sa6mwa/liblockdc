if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

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

foreach(lockdc_preset IN LISTS lockdc_release_presets)
    set(lockdc_build_dir "${LOCKDC_ROOT}/build/${lockdc_preset}")
    if(NOT EXISTS "${lockdc_build_dir}/package-metadata.cmake")
        message(FATAL_ERROR
            "missing package metadata for release tarball SDK verification: "
            "${lockdc_build_dir}/package-metadata.cmake")
    endif()

    execute_process(
        COMMAND "${CMAKE_COMMAND}"
            -DLOCKDC_BINARY_DIR=${lockdc_build_dir}
            -DLOCKDC_ROOT=${LOCKDC_ROOT}
            -DLOCKDC_DIST_DIR=${lockdc_dist_dir}
            -DLOCKDC_RUN_DOWNSTREAM_BINARIES=OFF
            -P "${LOCKDC_ROOT}/tests/release_tarball_sdk_test.cmake"
        RESULT_VARIABLE verify_result
        OUTPUT_VARIABLE verify_stdout
        ERROR_VARIABLE verify_stderr
    )
    if(NOT verify_result EQUAL 0)
        message(FATAL_ERROR
            "release tarball SDK verification failed for ${lockdc_preset}\n"
            "stdout:\n${verify_stdout}\n"
            "stderr:\n${verify_stderr}")
    endif()
endforeach()

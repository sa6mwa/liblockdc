function(lockdc_assert_generated_path root candidate)
    if("${root}" STREQUAL "" OR "${candidate}" STREQUAL "")
        message(FATAL_ERROR "generated path validation requires a repository root and candidate path")
    endif()

    file(REAL_PATH "${root}" lockdc_generated_root)
    cmake_path(ABSOLUTE_PATH candidate
        BASE_DIRECTORY "${lockdc_generated_root}"
        NORMALIZE
        OUTPUT_VARIABLE lockdc_generated_absolute_candidate)
    set(lockdc_generated_existing_parent "${lockdc_generated_absolute_candidate}")
    set(lockdc_generated_missing_suffix "")
    while(NOT EXISTS "${lockdc_generated_existing_parent}" AND
          NOT IS_SYMLINK "${lockdc_generated_existing_parent}")
        get_filename_component(lockdc_generated_component
            "${lockdc_generated_existing_parent}" NAME)
        set(lockdc_generated_missing_suffix
            "/${lockdc_generated_component}${lockdc_generated_missing_suffix}")
        get_filename_component(lockdc_generated_existing_parent
            "${lockdc_generated_existing_parent}" DIRECTORY)
    endwhile()
    file(REAL_PATH "${lockdc_generated_existing_parent}"
        lockdc_generated_real_parent)
    set(lockdc_generated_candidate
        "${lockdc_generated_real_parent}${lockdc_generated_missing_suffix}")
    if(lockdc_generated_candidate STREQUAL lockdc_generated_root)
        message(FATAL_ERROR "refusing repository root as generated path: ${candidate}")
    endif()

    file(RELATIVE_PATH lockdc_generated_relative
        "${lockdc_generated_root}" "${lockdc_generated_candidate}")
    if(IS_ABSOLUTE "${lockdc_generated_relative}" OR
       lockdc_generated_relative MATCHES "^\\.\\.(/|$)")
        message(FATAL_ERROR "refusing generated path outside repository root: ${candidate}")
    endif()
    if(NOT lockdc_generated_relative MATCHES
       "^(build|dist|\\.cache|\\.luarocks-build|devenv/volumes)(/|$)")
        message(FATAL_ERROR "refusing path outside generated roots: ${candidate}")
    endif()
endfunction()

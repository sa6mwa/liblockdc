function(lc_detect_version out_var)
  set(_lc_version "0.0.0")
  if(DEFINED LOCKDC_VERSION_SOURCE_DIR AND NOT "${LOCKDC_VERSION_SOURCE_DIR}" STREQUAL "")
    set(_lc_version_source_dir "${LOCKDC_VERSION_SOURCE_DIR}")
  else()
    set(_lc_version_source_dir "${CMAKE_CURRENT_SOURCE_DIR}")
  endif()

  file(REAL_PATH "${_lc_version_source_dir}" _lc_version_source_real)
  execute_process(
    COMMAND git -C "${_lc_version_source_dir}" rev-parse --show-toplevel
    RESULT_VARIABLE _lc_git_worktree_result
    OUTPUT_VARIABLE _lc_git_toplevel
    ERROR_QUIET
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  set(_lc_in_git_worktree 0)
  if(_lc_git_worktree_result EQUAL 0 AND NOT "${_lc_git_toplevel}" STREQUAL "")
    file(REAL_PATH "${_lc_git_toplevel}" _lc_git_toplevel_real)
    if(_lc_git_toplevel_real STREQUAL _lc_version_source_real)
      set(_lc_in_git_worktree 1)
    endif()
  endif()

  if(_lc_in_git_worktree)
    execute_process(
      COMMAND git -C "${_lc_version_source_dir}" tag --points-at HEAD --list "v[0-9]*.[0-9]*.[0-9]*"
      RESULT_VARIABLE _lc_git_result
      OUTPUT_VARIABLE _lc_git_tags
      ERROR_QUIET
      OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    set(_lc_exact_tag_count 0)
    set(_lc_exact_tag "")
    if(_lc_git_result EQUAL 0 AND NOT "${_lc_git_tags}" STREQUAL "")
      string(REPLACE "\n" ";" _lc_git_tag_list "${_lc_git_tags}")
      foreach(_lc_git_tag IN LISTS _lc_git_tag_list)
        if(_lc_git_tag MATCHES "^v[0-9]+\\.[0-9]+\\.[0-9]+$")
          execute_process(
            COMMAND git -C "${_lc_version_source_dir}" cat-file -t "${_lc_git_tag}"
            RESULT_VARIABLE _lc_tag_type_result
            OUTPUT_VARIABLE _lc_tag_type
            ERROR_QUIET
            OUTPUT_STRIP_TRAILING_WHITESPACE
          )
          if(NOT _lc_tag_type_result EQUAL 0 OR NOT _lc_tag_type STREQUAL "commit")
            message(FATAL_ERROR
              "exact release tag ${_lc_git_tag} must be a lightweight tag that resolves directly to a commit")
          endif()
          math(EXPR _lc_exact_tag_count "${_lc_exact_tag_count} + 1")
          set(_lc_exact_tag "${_lc_git_tag}")
        endif()
      endforeach()
    endif()
    if(_lc_exact_tag_count GREATER 1)
      message(FATAL_ERROR "multiple exact lightweight release tags point at HEAD")
    endif()
    if(_lc_exact_tag_count EQUAL 1)
      string(REGEX REPLACE "^v" "" _lc_version "${_lc_exact_tag}")
    elseif(DEFINED LOCKDC_VERSION_OVERRIDE AND NOT "${LOCKDC_VERSION_OVERRIDE}" STREQUAL "")
      if(NOT LOCKDC_VERSION_OVERRIDE MATCHES "^[0-9]+\\.[0-9]+\\.[0-9]+$")
        message(FATAL_ERROR "LOCKDC_VERSION_OVERRIDE must be a semantic version like 0.1.0.")
      endif()
      set(_lc_version "${LOCKDC_VERSION_OVERRIDE}")
    endif()
  elseif(DEFINED LOCKDC_VERSION_OVERRIDE AND NOT "${LOCKDC_VERSION_OVERRIDE}" STREQUAL "")
    if(NOT LOCKDC_VERSION_OVERRIDE MATCHES "^[0-9]+\\.[0-9]+\\.[0-9]+$")
      message(FATAL_ERROR "LOCKDC_VERSION_OVERRIDE must be a semantic version like 0.1.0.")
    endif()
    set(_lc_version "${LOCKDC_VERSION_OVERRIDE}")
  elseif(EXISTS "${_lc_version_source_dir}/VERSION")
      file(READ "${_lc_version_source_dir}/VERSION" _lc_version_file)
      string(STRIP "${_lc_version_file}" _lc_version_file)
      if(NOT _lc_version_file MATCHES "^[0-9]+\\.[0-9]+\\.[0-9]+$")
        message(FATAL_ERROR "VERSION must contain a semantic version like 0.1.0.")
      endif()
      set(_lc_version "${_lc_version_file}")
  endif()

  set(${out_var} "${_lc_version}" PARENT_SCOPE)
endfunction()

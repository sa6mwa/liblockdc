function(lc_detect_version out_var)
  set(_lc_version "0.0.0")
  if(DEFINED LOCKDC_VERSION_SOURCE_DIR AND NOT "${LOCKDC_VERSION_SOURCE_DIR}" STREQUAL "")
    set(_lc_version_source_dir "${LOCKDC_VERSION_SOURCE_DIR}")
  else()
    set(_lc_version_source_dir "${CMAKE_CURRENT_SOURCE_DIR}")
  endif()

  if(DEFINED LOCKDC_VERSION_OVERRIDE AND NOT "${LOCKDC_VERSION_OVERRIDE}" STREQUAL "")
    if(NOT LOCKDC_VERSION_OVERRIDE MATCHES "^[0-9]+\\.[0-9]+\\.[0-9]+$")
      message(FATAL_ERROR "LOCKDC_VERSION_OVERRIDE must be a semantic version like 0.1.0.")
    endif()
    set(_lc_version "${LOCKDC_VERSION_OVERRIDE}")
  else()
    execute_process(
      COMMAND git -C "${_lc_version_source_dir}" describe --tags --exact-match --match "v[0-9]*.[0-9]*.[0-9]*" HEAD
      RESULT_VARIABLE _lc_git_result
      OUTPUT_VARIABLE _lc_git_tag
      ERROR_QUIET
      OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    if(_lc_git_result EQUAL 0 AND NOT "${_lc_git_tag}" STREQUAL "")
      string(REGEX REPLACE "^v" "" _lc_version "${_lc_git_tag}")
    endif()
  endif()

  set(${out_var} "${_lc_version}" PARENT_SCOPE)
endfunction()

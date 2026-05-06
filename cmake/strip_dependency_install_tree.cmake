if(NOT DEFINED LOCKDC_STRIP_ROOT OR LOCKDC_STRIP_ROOT STREQUAL "")
  message(FATAL_ERROR "LOCKDC_STRIP_ROOT is required")
endif()

if(NOT EXISTS "${LOCKDC_STRIP_ROOT}")
  message(FATAL_ERROR "dependency install tree does not exist: ${LOCKDC_STRIP_ROOT}")
endif()

if(NOT DEFINED LOCKDC_STRIP_BIN OR LOCKDC_STRIP_BIN STREQUAL "")
  message(FATAL_ERROR "LOCKDC_STRIP_BIN is required")
endif()

if(NOT EXISTS "${LOCKDC_STRIP_BIN}")
  message(FATAL_ERROR "strip tool does not exist: ${LOCKDC_STRIP_BIN}")
endif()

if(NOT DEFINED LOCKDC_STRIP_STATIC_ARCHIVES)
  set(LOCKDC_STRIP_STATIC_ARCHIVES ON)
endif()

file(GLOB_RECURSE _lockdc_strip_candidates LIST_DIRECTORIES false
  "${LOCKDC_STRIP_ROOT}/lib/*.a"
  "${LOCKDC_STRIP_ROOT}/lib/*.dylib"
  "${LOCKDC_STRIP_ROOT}/lib/*.so"
  "${LOCKDC_STRIP_ROOT}/lib/*.so.*"
)

function(lockdc_fix_darwin_dylib install_dylib)
  if(NOT DEFINED LOCKDC_DARWIN_DEPENDENCY_ROOT OR LOCKDC_DARWIN_DEPENDENCY_ROOT STREQUAL "")
    return()
  endif()
  if(NOT DEFINED LOCKDC_INSTALL_NAME_TOOL OR NOT EXISTS "${LOCKDC_INSTALL_NAME_TOOL}")
    message(FATAL_ERROR "LOCKDC_INSTALL_NAME_TOOL is required for Darwin dependency fixups")
  endif()
  if(NOT DEFINED LOCKDC_OTOOL OR NOT EXISTS "${LOCKDC_OTOOL}")
    message(FATAL_ERROR "LOCKDC_OTOOL is required for Darwin dependency fixups")
  endif()

  get_filename_component(_dylib_name "${install_dylib}" NAME)
  execute_process(
    COMMAND "${LOCKDC_INSTALL_NAME_TOOL}" -id "@rpath/${_dylib_name}" "${install_dylib}"
    RESULT_VARIABLE _id_result
    ERROR_VARIABLE _id_error
  )
  if(NOT _id_result EQUAL 0)
    message(FATAL_ERROR "failed to rewrite Darwin install name for ${install_dylib}\n${_id_error}")
  endif()

  execute_process(
    COMMAND "${LOCKDC_OTOOL}" -L "${install_dylib}"
    RESULT_VARIABLE _otool_result
    OUTPUT_VARIABLE _otool_output
    ERROR_VARIABLE _otool_error
  )
  if(NOT _otool_result EQUAL 0)
    message(FATAL_ERROR "failed to inspect Darwin dependency ${install_dylib}\n${_otool_error}")
  endif()

  string(REGEX REPLACE "([][+.*()^$?{}|\\])" "\\\\\\1" _dependency_root_re "${LOCKDC_DARWIN_DEPENDENCY_ROOT}")
  string(REPLACE "\n" ";" _otool_lines "${_otool_output}")
  foreach(_otool_line IN LISTS _otool_lines)
    string(STRIP "${_otool_line}" _dependency_line)
    if(NOT _dependency_line MATCHES "^/")
      continue()
    endif()
    string(REGEX MATCH "^[^ \t]+" _dependency_path "${_dependency_line}")
    if(NOT _dependency_path MATCHES "^${_dependency_root_re}/")
      continue()
    endif()
    get_filename_component(_dependency_name "${_dependency_path}" NAME)
    execute_process(
      COMMAND "${LOCKDC_INSTALL_NAME_TOOL}"
        -change "${_dependency_path}" "@rpath/${_dependency_name}" "${install_dylib}"
      RESULT_VARIABLE _change_result
      ERROR_VARIABLE _change_error
    )
    if(NOT _change_result EQUAL 0)
      message(FATAL_ERROR
        "failed to rewrite Darwin dependency ${_dependency_path} in ${install_dylib}\n${_change_error}")
    endif()
  endforeach()
endfunction()

foreach(_candidate IN LISTS _lockdc_strip_candidates)
  if(IS_SYMLINK "${_candidate}" OR IS_DIRECTORY "${_candidate}")
    continue()
  endif()
  if(NOT LOCKDC_STRIP_STATIC_ARCHIVES AND _candidate MATCHES "\\.a$")
    continue()
  endif()
  if(_candidate MATCHES "\\.dylib$")
    lockdc_fix_darwin_dylib("${_candidate}")
  endif()

  execute_process(
    COMMAND "${LOCKDC_STRIP_BIN}" -S "${_candidate}"
    RESULT_VARIABLE _strip_result
    ERROR_VARIABLE _strip_error
  )
  if(NOT _strip_result EQUAL 0)
    message(FATAL_ERROR "failed to strip dependency artifact ${_candidate}\n${_strip_error}")
  endif()
endforeach()

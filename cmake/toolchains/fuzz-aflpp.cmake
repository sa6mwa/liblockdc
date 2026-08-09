set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR x86_64)
set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)

include("${CMAKE_CURRENT_LIST_DIR}/../CpktBootlinToolchain.cmake")
cpkt_configure_bootlin_toolchain(x86_64-linux-gnu)

set(_lockdc_aflpp_resolver "${CMAKE_CURRENT_LIST_DIR}/../../scripts/cpkt-aflpp.sh")
get_filename_component(_lockdc_aflpp_resolver "${_lockdc_aflpp_resolver}" ABSOLUTE)
execute_process(
    COMMAND "${_lockdc_aflpp_resolver}" discover
    RESULT_VARIABLE _lockdc_aflpp_result
    OUTPUT_VARIABLE _lockdc_aflpp_description
    ERROR_VARIABLE _lockdc_aflpp_error)
if(NOT _lockdc_aflpp_result EQUAL 0)
    message(FATAL_ERROR
        "Unable to inspect pinned AFL++ toolchain: ${_lockdc_aflpp_error}")
endif()

foreach(_lockdc_aflpp_key cc cxx helper root)
    string(REGEX MATCH "(^|\n)${_lockdc_aflpp_key}=([^\r\n]+)" _lockdc_aflpp_match "${_lockdc_aflpp_description}")
    if(NOT _lockdc_aflpp_match)
        message(FATAL_ERROR "AFL++ resolver did not report ${_lockdc_aflpp_key}")
    endif()
    set(_lockdc_aflpp_${_lockdc_aflpp_key} "${CMAKE_MATCH_2}")
endforeach()

set(ENV{AFL_PATH} "${_lockdc_aflpp_helper}")
set(CMAKE_C_COMPILER "${_lockdc_aflpp_cc}" CACHE FILEPATH "" FORCE)
set(CMAKE_CXX_COMPILER "${_lockdc_aflpp_cxx}" CACHE FILEPATH "" FORCE)
set(LOCKDC_AFLPP_ROOT "${_lockdc_aflpp_root}" CACHE PATH "Pinned AFL++ lifecycle root." FORCE)
set(LOCKDC_AFLPP_HELPER "${_lockdc_aflpp_helper}" CACHE PATH "Pinned AFL++ helper root." FORCE)

set(LOCKDC_TARGET_ARCH x86_64 CACHE STRING "" FORCE)
set(LOCKDC_TARGET_OS linux CACHE STRING "" FORCE)
set(LOCKDC_TARGET_LIBC gnu CACHE STRING "" FORCE)

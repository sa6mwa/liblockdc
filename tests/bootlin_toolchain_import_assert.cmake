foreach(required
        LOCKDC_TOOLCHAIN_FILE
        LOCKDC_EXPECTED_CC
        LOCKDC_EXPECTED_AR
        LOCKDC_EXPECTED_SYSROOT)
    if(NOT DEFINED ${required} OR "${${required}}" STREQUAL "")
        message(FATAL_ERROR "${required} is required")
    endif()
endforeach()

include("${LOCKDC_TOOLCHAIN_FILE}")

if(NOT CMAKE_C_COMPILER STREQUAL LOCKDC_EXPECTED_CC)
    message(FATAL_ERROR
        "expected CMAKE_C_COMPILER=${LOCKDC_EXPECTED_CC}, got ${CMAKE_C_COMPILER}")
endif()
if(NOT CMAKE_AR STREQUAL LOCKDC_EXPECTED_AR)
    message(FATAL_ERROR
        "expected CMAKE_AR=${LOCKDC_EXPECTED_AR}, got ${CMAKE_AR}")
endif()
if(NOT CMAKE_SYSROOT STREQUAL LOCKDC_EXPECTED_SYSROOT)
    message(FATAL_ERROR
        "expected CMAKE_SYSROOT=${LOCKDC_EXPECTED_SYSROOT}, got ${CMAKE_SYSROOT}")
endif()
if(CMAKE_C_COMPILER MATCHES "^/usr/bin/" OR CMAKE_AR MATCHES "^/usr/bin/")
    message(FATAL_ERROR "Bootlin toolchain imported a host /usr/bin tool")
endif()

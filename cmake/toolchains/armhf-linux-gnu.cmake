set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR arm)
set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)
set(CMAKE_SYSROOT "" CACHE PATH "" FORCE)

set(CMAKE_C_COMPILER /usr/bin/arm-linux-gnueabihf-gcc CACHE FILEPATH "")
set(CMAKE_AR /usr/bin/arm-linux-gnueabihf-ar CACHE FILEPATH "")
set(CMAKE_RANLIB /usr/bin/arm-linux-gnueabihf-ranlib CACHE FILEPATH "")
set(LOCKDC_GNU_ROOT /usr/arm-linux-gnueabihf)

if(NOT EXISTS "${LOCKDC_GNU_ROOT}/include/stdio.h")
    message(FATAL_ERROR
        "The armhf GNU cross toolchain is missing target libc headers under ${LOCKDC_GNU_ROOT}/include. "
        "Install the Debian/Ubuntu development sysroot package libc6-dev-armhf-cross.")
endif()

if(DEFINED CMAKE_C_FLAGS)
    set(_lockdc_armhf_c_flags "${CMAKE_C_FLAGS}")
else()
    set(_lockdc_armhf_c_flags "")
endif()
if(NOT _lockdc_armhf_c_flags MATCHES "(^| )-isystem ${LOCKDC_GNU_ROOT}/include($| )")
    string(APPEND _lockdc_armhf_c_flags " -isystem ${LOCKDC_GNU_ROOT}/include")
endif()
string(STRIP "${_lockdc_armhf_c_flags}" _lockdc_armhf_c_flags)
set(CMAKE_C_FLAGS "${_lockdc_armhf_c_flags}" CACHE STRING "" FORCE)
unset(_lockdc_armhf_c_flags)

set(CMAKE_FIND_ROOT_PATH ${LOCKDC_GNU_ROOT})
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)

set(CMAKE_CROSSCOMPILING_EMULATOR /usr/bin/qemu-arm;-L;${LOCKDC_GNU_ROOT} CACHE STRING "")

set(LOCKDC_TARGET_ARCH armhf CACHE STRING "" FORCE)
set(LOCKDC_TARGET_OS linux CACHE STRING "" FORCE)
set(LOCKDC_TARGET_LIBC gnu CACHE STRING "" FORCE)

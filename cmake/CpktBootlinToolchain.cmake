set(CPKT_BOOTLIN_TOOLCHAIN_MODULE_DIR "${CMAKE_CURRENT_LIST_DIR}")

function(cpkt_read_resolver_value out key description)
    string(REGEX MATCH "(^|\n)${key}=([^\r\n]+)" match "${description}")
    if(NOT match)
        message(FATAL_ERROR
            "Bootlin resolver did not report ${key}")
    endif()
    set(${out} "${CMAKE_MATCH_2}" PARENT_SCOPE)
endfunction()

function(cpkt_configure_bootlin_toolchain target_id)
    set(resolver "${CPKT_BOOTLIN_TOOLCHAIN_MODULE_DIR}/../scripts/cpkt-toolchains.sh")
    get_filename_component(resolver "${resolver}" ABSOLUTE)
    if(NOT EXISTS "${resolver}")
        message(FATAL_ERROR
            "Missing lifecycle Bootlin resolver: ${resolver}")
    endif()

    execute_process(
        COMMAND "${resolver}" discover "${target_id}"
        RESULT_VARIABLE result
        OUTPUT_VARIABLE description
        ERROR_VARIABLE error)
    if(NOT result EQUAL 0)
        message(FATAL_ERROR
            "Unable to inspect pinned Bootlin toolchain ${target_id}: ${error}")
    endif()
    if(NOT description MATCHES "(^|\n)status=ready(\n|$)")
        message(FATAL_ERROR
            "Pinned Bootlin toolchain ${target_id} is not ready. Run scripts/cpkt-toolchains.sh ensure ${target_id} and reconfigure.")
    endif()

    foreach(key cc cxx ld ar ranlib strip nm objcopy objdump addr2line readelf sysroot root)
        cpkt_read_resolver_value(bootlin_${key} "${key}" "${description}")
    endforeach()

    set(CMAKE_C_COMPILER "${bootlin_cc}" CACHE FILEPATH "" FORCE)
    set(CMAKE_CXX_COMPILER "${bootlin_cxx}" CACHE FILEPATH "" FORCE)
    set(CMAKE_LINKER "${bootlin_ld}" CACHE FILEPATH "" FORCE)
    set(CMAKE_AR "${bootlin_ar}" CACHE FILEPATH "" FORCE)
    set(CMAKE_RANLIB "${bootlin_ranlib}" CACHE FILEPATH "" FORCE)
    set(CMAKE_STRIP "${bootlin_strip}" CACHE FILEPATH "" FORCE)
    set(CMAKE_NM "${bootlin_nm}" CACHE FILEPATH "" FORCE)
    set(CMAKE_OBJCOPY "${bootlin_objcopy}" CACHE FILEPATH "" FORCE)
    set(CMAKE_OBJDUMP "${bootlin_objdump}" CACHE FILEPATH "" FORCE)
    set(CMAKE_ADDR2LINE "${bootlin_addr2line}" CACHE FILEPATH "" FORCE)
    set(CMAKE_READELF "${bootlin_readelf}" CACHE FILEPATH "" FORCE)
    set(CMAKE_SYSROOT "${bootlin_sysroot}" CACHE PATH "" FORCE)
    set(CMAKE_FIND_ROOT_PATH "${bootlin_sysroot}" "${bootlin_root}" CACHE STRING "" FORCE)
    set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER CACHE STRING "" FORCE)
    set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY CACHE STRING "" FORCE)
    set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY CACHE STRING "" FORCE)
    set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY CACHE STRING "" FORCE)
endfunction()

function(lockdc_import_cmake_cache_value binary_dir var_name)
    if(DEFINED ${var_name} AND NOT "${${var_name}}" STREQUAL "")
        set(${var_name} "${${var_name}}" PARENT_SCOPE)
        return()
    endif()

    file(STRINGS "${binary_dir}/CMakeCache.txt" cache_line
        REGEX "^${var_name}(:[^=]+)?="
        LIMIT_COUNT 1)
    if(cache_line)
        string(REGEX REPLACE "^[^=]*=" "" cache_value "${cache_line}")
        set(${var_name} "${cache_value}" PARENT_SCOPE)
    endif()
endfunction()

function(lockdc_resolve_bootlin_runtime sysroot external_root primary_lib_dir
         output_loader output_dirs)
    set(runtime_loader "")
    set(runtime_dirs "${primary_lib_dir}")
    if(NOT "${sysroot}" STREQUAL "")
        file(GLOB runtime_loaders
            "${sysroot}/lib/ld-linux*.so.*"
            "${sysroot}/lib64/ld-linux*.so.*"
            "${sysroot}/lib/ld-musl-*.so.*"
            "${sysroot}/lib64/ld-musl-*.so.*")
        list(LENGTH runtime_loaders runtime_loader_count)
        if(runtime_loader_count EQUAL 0)
            message(FATAL_ERROR "Bootlin sysroot has no dynamic loader: ${sysroot}")
        endif()
        list(GET runtime_loaders 0 runtime_loader)
        list(APPEND runtime_dirs
            "${sysroot}/lib"
            "${sysroot}/usr/lib"
            "${sysroot}/lib64"
            "${sysroot}/usr/lib64")
    endif()

    foreach(runtime_dir
        "${external_root}/c.pkt.systems/install/lib"
        "${external_root}/curl/install/lib"
        "${external_root}/openssl/install/lib"
        "${external_root}/nghttp2/install/lib"
        "${external_root}/pslog/install/lib"
        "${external_root}/lonejson/install/lib"
        "${external_root}/liblql/install/lib"
        "${external_root}/libssh2/install/lib"
        "${external_root}/zlib/install/lib")
        list(APPEND runtime_dirs "${runtime_dir}")
    endforeach()
    list(REMOVE_DUPLICATES runtime_dirs)
    set(${output_loader} "${runtime_loader}" PARENT_SCOPE)
    set(${output_dirs} "${runtime_dirs}" PARENT_SCOPE)
endfunction()

function(lockdc_bootlin_runtime_link_flags runtime_loader runtime_dirs output)
    set(runtime_flags)
    if(NOT "${runtime_loader}" STREQUAL "")
        list(APPEND runtime_flags
            "-Wl,--dynamic-linker,${runtime_loader}"
            "-Wl,--disable-new-dtags")
        foreach(runtime_dir IN LISTS runtime_dirs)
            list(APPEND runtime_flags "-Wl,-rpath,${runtime_dir}")
        endforeach()
    endif()
    set(${output} "${runtime_flags}" PARENT_SCOPE)
endfunction()

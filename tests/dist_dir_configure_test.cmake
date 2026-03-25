if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

function(read_cache_value var_name out_var)
    file(STRINGS "${LOCKDC_BINARY_DIR}/CMakeCache.txt" cache_line
         REGEX "^${var_name}(:[^=]+)?=" LIMIT_COUNT 1)
    if(NOT cache_line)
        message(FATAL_ERROR "missing ${var_name} in ${LOCKDC_BINARY_DIR}/CMakeCache.txt")
    endif()
    string(REGEX REPLACE "^[^=]*=" "" cache_value "${cache_line}")
    set(${out_var} "${cache_value}" PARENT_SCOPE)
endfunction()

read_cache_value(CMAKE_BUILD_TYPE lockdc_build_type)
read_cache_value(LOCKDC_DIST_DIR lockdc_dist_dir)

cmake_path(NORMAL_PATH lockdc_dist_dir OUTPUT_VARIABLE lockdc_dist_dir)
cmake_path(NORMAL_PATH LOCKDC_BINARY_DIR OUTPUT_VARIABLE lockdc_binary_dir)
cmake_path(NORMAL_PATH LOCKDC_ROOT OUTPUT_VARIABLE lockdc_root)

if(lockdc_build_type STREQUAL "Release")
    set(expected_dist_dir "${lockdc_root}/dist")
else()
    set(expected_dist_dir "${lockdc_binary_dir}/dist")
endif()

if(NOT lockdc_dist_dir STREQUAL expected_dist_dir)
    message(FATAL_ERROR
        "unexpected LOCKDC_DIST_DIR for ${lockdc_build_type} build\n"
        "expected: ${expected_dist_dir}\n"
        "actual:   ${lockdc_dist_dir}")
endif()

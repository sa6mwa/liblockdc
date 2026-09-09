if(NOT DEFINED LOCKDC_BINARY_DIR OR LOCKDC_BINARY_DIR STREQUAL "")
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

set(cache_file "${LOCKDC_BINARY_DIR}/CMakeCache.txt")
if(NOT EXISTS "${cache_file}")
    message(FATAL_ERROR "missing CMake cache: ${cache_file}")
endif()

function(lockdc_read_cache_value name out_var)
    file(STRINGS "${cache_file}" cache_line
        REGEX "^${name}(:[^=]+)?="
        LIMIT_COUNT 1)
    if(NOT cache_line)
        message(FATAL_ERROR "missing ${name} in ${cache_file}")
    endif()
    string(FIND "${cache_line}" "=" equals_index)
    math(EXPR value_index "${equals_index} + 1")
    string(SUBSTRING "${cache_line}" ${value_index} -1 value)
    set(${out_var} "${value}" PARENT_SCOPE)
endfunction()

lockdc_read_cache_value(CMAKE_C_COMPILER compiler)
lockdc_read_cache_value(LOCKDC_GCOV_BIN gcov)
get_filename_component(compiler_name "${compiler}" NAME)
if(NOT compiler_name MATCHES "gcc$")
    return()
endif()

get_filename_component(compiler_dir "${compiler}" DIRECTORY)
string(REGEX REPLACE "gcc$" "gcov" expected_gcov_name "${compiler_name}")
set(expected_gcov "${compiler_dir}/${expected_gcov_name}")
if(NOT EXISTS "${expected_gcov}")
    return()
endif()
if(NOT gcov STREQUAL expected_gcov)
    message(FATAL_ERROR
        "coverage must use the gcov sibling of the selected compiler\n"
        "compiler: ${compiler}\nexpected: ${expected_gcov}\nactual: ${gcov}")
endif()

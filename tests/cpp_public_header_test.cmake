if(NOT DEFINED LOCKDC_BINARY_DIR OR LOCKDC_BINARY_DIR STREQUAL "")
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()
if(NOT DEFINED LOCKDC_ROOT OR LOCKDC_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()
if(NOT DEFINED LOCKDC_C_COMPILER OR LOCKDC_C_COMPILER STREQUAL "")
    message(FATAL_ERROR "LOCKDC_C_COMPILER is required")
endif()
if(NOT DEFINED LOCKDC_NM OR LOCKDC_NM STREQUAL "")
    message(FATAL_ERROR "LOCKDC_NM is required")
endif()
if(NOT DEFINED LOCKDC_EXTERNAL_ROOT OR LOCKDC_EXTERNAL_ROOT STREQUAL "")
    message(FATAL_ERROR "LOCKDC_EXTERNAL_ROOT is required")
endif()

get_filename_component(cxx_dir "${LOCKDC_C_COMPILER}" DIRECTORY)
get_filename_component(c_compiler_name "${LOCKDC_C_COMPILER}" NAME)
string(REGEX REPLACE "gcc$" "c++" cxx_compiler_name "${c_compiler_name}")
set(cxx_compiler "${cxx_dir}/${cxx_compiler_name}")
if(NOT EXISTS "${cxx_compiler}")
    message(FATAL_ERROR "selected Bootlin C++ compiler is missing: ${cxx_compiler}")
endif()

set(test_root "${LOCKDC_BINARY_DIR}/cpp-public-header-test")
set(source_file "${test_root}/consumer.cpp")
set(object_file "${test_root}/consumer.o")
file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY "${test_root}")
file(WRITE "${source_file}" [=[
#include <lc/lc.h>

int main() {
  lc_acquire_req request = {};
  request.ns = 0;
  return lc_version_string() == 0 ? 0 : 0;
}
]=])

set(sysroot_arg)
if(DEFINED LOCKDC_SYSROOT AND NOT LOCKDC_SYSROOT STREQUAL "")
    list(APPEND sysroot_arg "--sysroot=${LOCKDC_SYSROOT}")
endif()
execute_process(
    COMMAND "${cxx_compiler}" ${sysroot_arg} -std=c++98 -Werror -c
        "${source_file}" -o "${object_file}"
        -I "${LOCKDC_BINARY_DIR}/generated/include"
        -I "${LOCKDC_ROOT}/include"
        -isystem "${LOCKDC_EXTERNAL_ROOT}/lonejson/install/include"
        -isystem "${LOCKDC_EXTERNAL_ROOT}/pslog/install/include"
    RESULT_VARIABLE compile_result
    OUTPUT_VARIABLE compile_stdout
    ERROR_VARIABLE compile_stderr
)
if(NOT compile_result EQUAL 0)
    message(FATAL_ERROR
        "public C header does not compile as C++98 with the selected Bootlin toolchain\n"
        "stdout:\n${compile_stdout}\nstderr:\n${compile_stderr}")
endif()

execute_process(
    COMMAND "${LOCKDC_NM}" -u "${object_file}"
    RESULT_VARIABLE nm_result
    OUTPUT_VARIABLE nm_stdout
    ERROR_VARIABLE nm_stderr
)
if(NOT nm_result EQUAL 0 OR NOT nm_stdout MATCHES "lc_version_string")
    message(FATAL_ERROR
        "public C header does not preserve C linkage for C++ consumers\n"
        "stdout:\n${nm_stdout}\nstderr:\n${nm_stderr}")
endif()

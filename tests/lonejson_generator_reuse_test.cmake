if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

if(NOT DEFINED LOCKDC_BINARY_DIR)
    message(FATAL_ERROR "LOCKDC_BINARY_DIR is required")
endif()

if(NOT DEFINED LOCKDC_C_COMPILER)
    message(FATAL_ERROR "LOCKDC_C_COMPILER is required")
endif()

set(test_root "${LOCKDC_BINARY_DIR}/lonejson-generator-reuse-test")
set(configure_build_dir "${test_root}/build")
set(external_root "${test_root}/deps")
set(dependency_build_root "${test_root}/deps-build")
set(download_root "${test_root}/downloads")
set(configure_script "${dependency_build_root}/lonejson/src/configure.cmake")
set(lonejson_build_dir "${dependency_build_root}/lonejson/build")
set(lonejson_cache "${lonejson_build_dir}/CMakeCache.txt")
set(lonejson_asset "${download_root}/lonejson-0.7.0.h.gz")
set(lonejson_header "${test_root}/lonejson.h")

file(REMOVE_RECURSE "${test_root}")
file(MAKE_DIRECTORY "${download_root}")

find_program(LOCKDC_TEST_GZIP_BIN NAMES gzip REQUIRED)
file(WRITE "${lonejson_header}" "#ifndef LONEJSON_H\n#define LONEJSON_H\n#endif\n")
execute_process(
    COMMAND "${LOCKDC_TEST_GZIP_BIN}" -c "${lonejson_header}"
    OUTPUT_FILE "${lonejson_asset}"
    RESULT_VARIABLE gzip_result
    OUTPUT_VARIABLE gzip_stdout
    ERROR_VARIABLE gzip_stderr
)
if(NOT gzip_result EQUAL 0)
    message(FATAL_ERROR
        "failed to seed lonejson header archive\n"
        "stdout:\n${gzip_stdout}\n"
        "stderr:\n${gzip_stderr}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -S "${LOCKDC_ROOT}"
        -B "${configure_build_dir}"
        -G Ninja
        -DCMAKE_C_COMPILER=${LOCKDC_C_COMPILER}
        -DCMAKE_BUILD_TYPE=Release
        -DLOCKDC_BUILD_DEPENDENCIES=ON
        -DLOCKDC_BUILD_STATIC=ON
        -DLOCKDC_BUILD_SHARED=ON
        -DLOCKDC_BUILD_TESTS=OFF
        -DLOCKDC_BUILD_EXAMPLES=OFF
        -DLOCKDC_BUILD_E2E_TESTS=OFF
        -DLOCKDC_BUILD_BENCHMARKS=OFF
        -DLOCKDC_BUILD_FUZZERS=OFF
        -DLOCKDC_INSTALL=OFF
        -DLOCKDC_TARGET_ARCH=x86_64
        -DLOCKDC_TARGET_OS=linux
        -DLOCKDC_TARGET_LIBC=gnu
        -DLOCKDC_EXTERNAL_ROOT=${external_root}
        -DLOCKDC_DEPENDENCY_BUILD_ROOT=${dependency_build_root}
        -DLOCKDC_DOWNLOAD_ROOT=${download_root}
    RESULT_VARIABLE configure_result
    OUTPUT_VARIABLE configure_stdout
    ERROR_VARIABLE configure_stderr
)
if(NOT configure_result EQUAL 0)
    message(FATAL_ERROR
        "failed to configure lonejson generator reuse probe\n"
        "stdout:\n${configure_stdout}\n"
        "stderr:\n${configure_stderr}")
endif()

if(NOT EXISTS "${configure_script}")
    message(FATAL_ERROR "missing generated lonejson configure script: ${configure_script}")
endif()

file(MAKE_DIRECTORY "${lonejson_build_dir}")
file(WRITE "${lonejson_cache}" "CMAKE_GENERATOR:INTERNAL=Unix Makefiles\n")

execute_process(
    COMMAND "${CMAKE_COMMAND}"
        -DLOCKDC_LONEJSON_GENERATOR=Ninja
        -P "${configure_script}"
    RESULT_VARIABLE lonejson_configure_result
    OUTPUT_VARIABLE lonejson_configure_stdout
    ERROR_VARIABLE lonejson_configure_stderr
)
if(NOT lonejson_configure_result EQUAL 0)
    message(FATAL_ERROR
        "generated lonejson configure step failed\n"
        "stdout:\n${lonejson_configure_stdout}\n"
        "stderr:\n${lonejson_configure_stderr}")
endif()

file(READ "${lonejson_cache}" lonejson_cache_contents)
string(FIND "${lonejson_cache_contents}" "CMAKE_GENERATOR:INTERNAL=Ninja" generator_match)
if(generator_match EQUAL -1)
    message(FATAL_ERROR
        "lonejson configure did not recreate the build tree with the expected generator\n"
        "cache:\n${lonejson_cache_contents}")
endif()

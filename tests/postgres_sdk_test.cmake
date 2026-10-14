include("${LOCKDC_ROOT}/tests/bootlin_runtime_test_support.cmake")
include("${LOCKDC_ROOT}/cmake/LcGeneratedPath.cmake")
lockdc_import_cmake_cache_value("${LOCKDC_BINARY_DIR}" CMAKE_TOOLCHAIN_FILE)
lockdc_import_cmake_cache_value("${LOCKDC_BINARY_DIR}" CMAKE_C_COMPILER)
lockdc_import_cmake_cache_value("${LOCKDC_BINARY_DIR}" CMAKE_SYSROOT)
foreach(flag CMAKE_BUILD_TYPE CMAKE_C_FLAGS CMAKE_C_FLAGS_DEBUG
    CMAKE_C_FLAGS_RELEASE CMAKE_C_FLAGS_RELWITHDEBINFO CMAKE_C_FLAGS_MINSIZEREL
    CMAKE_EXE_LINKER_FLAGS CMAKE_CXX_FLAGS CMAKE_CXX_FLAGS_DEBUG)
  lockdc_import_cmake_cache_value("${LOCKDC_BINARY_DIR}" ${flag})
endforeach()
include("${LOCKDC_BINARY_DIR}/package-metadata.cmake")
string(REGEX MATCHALL "-fsanitize=[^ ]+" sanitizer_options
  "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_DEBUG} ${CMAKE_EXE_LINKER_FLAGS}")
if(NOT LOCKDC_SANITIZER_INSTRUMENTED)
  set(sanitizer_options)
endif()
set(root "${LOCKDC_BINARY_DIR}/postgres-sdk-test")
lockdc_assert_generated_path("${LOCKDC_ROOT}" "${root}")
file(REMOVE_RECURSE "${root}")
set(sdk "${root}/sdk")
foreach(component runtime dev)
  execute_process(COMMAND "${CMAKE_COMMAND}" --install "${LOCKDC_BINARY_DIR}"
    --prefix "${sdk}" --component ${component}
    RESULT_VARIABLE rc OUTPUT_VARIABLE output ERROR_VARIABLE error)
  if(NOT rc EQUAL 0)
    message(FATAL_ERROR "Core install failed: ${output}\n${error}")
  endif()
endforeach()
if(EXISTS "${sdk}/include/lc/postgres.h" OR EXISTS "${sdk}/lib/liblockdc_postgres.a"
   OR EXISTS "${sdk}/lib/cmake/lockdc_postgres" OR EXISTS "${sdk}/lib/pkgconfig/lockdc-postgres.pc")
  message(FATAL_ERROR "Optional PostgreSQL payload leaked into core install components")
endif()
foreach(component postgres-runtime postgres-dev)
  execute_process(COMMAND "${CMAKE_COMMAND}" --install "${LOCKDC_BINARY_DIR}"
    --prefix "${sdk}" --component ${component}
    RESULT_VARIABLE rc OUTPUT_VARIABLE output ERROR_VARIABLE error)
  if(NOT rc EQUAL 0)
    message(FATAL_ERROR "PostgreSQL install failed: ${output}\n${error}")
  endif()
endforeach()
file(RENAME "${sdk}" "${root}/relocated")
set(sdk "${root}/relocated")
foreach(provider core db)
  if(provider STREQUAL "core")
    set(bundle cpkt)
  else()
    set(bundle cpktdb)
  endif()
  file(SHA256 "${LOCKDC_EXTERNAL_ROOT}/${bundle}/install/share/cpkt/packages/${provider}.json" expected)
  file(SHA256 "${sdk}/share/doc/liblockdc_postgres/sdk-provenance/${provider}.json" actual)
  if(NOT expected STREQUAL actual)
    message(FATAL_ERROR "Installed ${provider} SDK provenance differs from the consumed SDK")
  endif()
endforeach()
foreach(license cpkt cpktdb postgresql openldap cyrus-sasl krb5 curl openssl zlib nghttp2 libssh2)
  if(NOT EXISTS "${sdk}/share/doc/liblockdc_postgres/licenses/${license}/LICENSE")
    message(FATAL_ERROR "Provider license missing: ${license}")
  endif()
endforeach()
set(dist "${root}/dist")
execute_process(COMMAND "${CMAKE_COMMAND}" -DLOCKDC_ROOT=${LOCKDC_ROOT}
    -DLOCKDC_BINARY_DIR=${LOCKDC_BINARY_DIR} -DLOCKDC_DIST_DIR=${dist}
    -P "${LOCKDC_ROOT}/cmake/package_archive.cmake"
    RESULT_VARIABLE rc OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(NOT rc EQUAL 0)
  message(FATAL_ERROR "PostgreSQL archive assembly failed\n${output}\n${error}")
endif()
include("${LOCKDC_BINARY_DIR}/package-metadata.cmake")
set(core_name "liblockdc-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}")
set(provider_name "liblockdc-postgres-${LOCKDC_VERSION}-${LOCKDC_TARGET_ID}")
set(extract "${root}/archive-extract")
file(MAKE_DIRECTORY "${extract}")
foreach(package ${core_name} ${provider_name})
  if(NOT EXISTS "${dist}/${package}.tar.gz")
    message(FATAL_ERROR "Missing selected archive: ${package}")
  endif()
  execute_process(COMMAND "${CMAKE_COMMAND}" -E tar xf "${dist}/${package}.tar.gz"
    WORKING_DIRECTORY "${extract}" RESULT_VARIABLE rc)
  if(NOT rc EQUAL 0)
    message(FATAL_ERROR "Cannot extract ${package}")
  endif()
endforeach()
file(RENAME "${extract}/${core_name}" "${root}/relocated-core-archive")
file(RENAME "${extract}/${provider_name}" "${root}/relocated-provider-archive")
set(core_sdk "${root}/relocated-core-archive")
set(sdk "${root}/relocated-provider-archive")
foreach(public_doc CHANGELOG.md docs/postgres.md docs/xa.md docs/lua.md)
  get_filename_component(doc_name "${public_doc}" NAME)
  set(packaged_doc "${sdk}/share/doc/liblockdc_postgres/${doc_name}")
  if(NOT EXISTS "${packaged_doc}")
    message(FATAL_ERROR "Provider archive is missing documentation: ${doc_name}")
  endif()
  file(SHA256 "${LOCKDC_ROOT}/${public_doc}" expected_doc_digest)
  file(SHA256 "${packaged_doc}" packaged_doc_digest)
  if(NOT expected_doc_digest STREQUAL packaged_doc_digest)
    message(FATAL_ERROR "Provider archive contains stale documentation: ${doc_name}")
  endif()
endforeach()
foreach(provider core db)
  if(provider STREQUAL "core")
    set(bundle cpkt)
  else()
    set(bundle cpktdb)
  endif()
  file(SHA256 "${LOCKDC_EXTERNAL_ROOT}/${bundle}/install/share/cpkt/packages/${provider}.json" expected)
  file(SHA256 "${sdk}/share/doc/liblockdc_postgres/sdk-provenance/${provider}.json" actual)
  if(NOT expected STREQUAL actual)
    message(FATAL_ERROR "Archived ${provider} SDK provenance changed")
  endif()
endforeach()
foreach(license cpkt cpktdb postgresql openldap cyrus-sasl krb5 curl openssl zlib nghttp2 libssh2)
  if(NOT EXISTS "${sdk}/share/doc/liblockdc_postgres/licenses/${license}/LICENSE")
    message(FATAL_ERROR "Archived provider license missing: ${license}")
  endif()
endforeach()
if(EXISTS "${sdk}/include/lc/lc.h" OR EXISTS "${sdk}/lib/liblockdc.a" OR
   EXISTS "${sdk}/lib/libpq.a" OR EXISTS "${sdk}/lib/cmake/lockdc")
  message(FATAL_ERROR "Provider archive bundles core/native SDK payload")
endif()
if(EXISTS "${core_sdk}/include/lc/postgres.h" OR EXISTS "${core_sdk}/lib/liblockdc_postgres.a")
  message(FATAL_ERROR "Core archive bundles optional provider payload")
endif()
foreach(required lib/liblockdc_postgres.a lib/liblockdc_postgres.so
    include/lc/postgres.h lib/cmake/lockdc_postgres/lockdc_postgresConfig.cmake
    lib/pkgconfig/lockdc-postgres.pc share/lockdc_postgres/package-metadata.cmake)
  if(NOT EXISTS "${sdk}/${required}")
    message(FATAL_ERROR "Provider archive is missing ${required}")
  endif()
endforeach()
execute_process(COMMAND "${CMAKE_COMMAND}" -DLOCKDC_ROOT=${LOCKDC_ROOT}
    -DLOCKDC_BINARY_DIR=${LOCKDC_BINARY_DIR} -DLOCKDC_DIST_DIR=${dist}
    -P "${LOCKDC_ROOT}/cmake/package_checksums.cmake"
    RESULT_VARIABLE rc OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(NOT rc EQUAL 0)
  message(FATAL_ERROR "Provider checksum generation failed\n${output}\n${error}")
endif()
file(READ "${dist}/liblockdc-${LOCKDC_VERSION}-CHECKSUMS" checksums)
string(FIND "${checksums}" "${provider_name}.tar.gz" selected_checksum)
if(selected_checksum EQUAL -1)
  message(FATAL_ERROR "Optional provider archive is absent from release checksums")
endif()
set(source "${root}/consumer")
file(MAKE_DIRECTORY "${source}")
file(WRITE "${source}/main.c" [=[
#include <lc/postgres.h>
int main(void) {
  lc_xa_connection_view view;
  lc_postgres *resource = (lc_postgres *)1;
  lc_postgres_config config;
  lc_xa_coordinator *coordinator = (lc_xa_coordinator *)1;
  const lc_xa_driver *driver = lc_postgres_driver();
  config.resource_id = "invalid";
  config.connection_info = "host=/invalid";
  lc_xa_connection_view_init(&view);
  return driver->size != sizeof(*driver) || driver->abi_version != LC_XA_DRIVER_ABI ||
         view.size != sizeof(view) || view.abi_version != LC_XA_CONNECTION_VIEW_ABI ||
         lc_postgres_open(0, &config, &resource, 0) != LC_ERR_INVALID ||
         resource != 0 || lc_xa_coordinator_open(0, 0, &coordinator, 0) != LC_ERR_INVALID ||
         coordinator != 0 || cpkt_postgres_library_version() < 180000;
}
]=])
file(WRITE "${source}/main.cpp" [=[
#include <lc/postgres.h>
int main() {
  lc_xa_connection_view view = {};
  lc_postgres_wait wait = {};
  lc_xa_coordinator *coordinator = 0;
  const lc_xa_driver *driver = lc_postgres_driver();
  lc_xa_connection_view_init(&view);
  return driver->size != sizeof(*driver) || driver->abi_version != LC_XA_DRIVER_ABI ||
         view.size != sizeof(view) || view.abi_version != LC_XA_CONNECTION_VIEW_ABI ||
         lc_postgres_operation_wait(0, &wait, 0) != LC_ERR_INVALID ||
         lc_xa_coordinator_open(0, 0, &coordinator, 0) != LC_ERR_INVALID ||
         cpkt_postgres_library_version() < 180000;
}
]=])
file(WRITE "${source}/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.21)
project(postgres_sdk_consumer C CXX)
list(APPEND CMAKE_FIND_ROOT_PATH ${CMAKE_PREFIX_PATH})
find_package(lockdc_postgres CONFIG REQUIRED COMPONENTS static shared)
foreach(variant static shared)
  foreach(language c cpp)
    add_executable(consumer_${variant}_${language} main.${language})
    set_target_properties(consumer_${variant}_${language} PROPERTIES
      C_STANDARD 90 C_STANDARD_REQUIRED YES C_EXTENSIONS NO
      CXX_STANDARD 98 CXX_STANDARD_REQUIRED YES CXX_EXTENSIONS NO
      SKIP_BUILD_RPATH TRUE)
    target_compile_options(consumer_${variant}_${language} PRIVATE -Wall -Wextra -Werror -pedantic-errors)
    target_link_libraries(consumer_${variant}_${language} PRIVATE lockdc::postgres_${variant})
    target_link_options(consumer_${variant}_${language} PRIVATE ${PROBE_RUNTIME_FLAGS})
    target_link_options(consumer_${variant}_${language} PRIVATE ${SANITIZER_OPTIONS})
  endforeach()
endforeach()
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND NOT SANITIZER_OPTIONS)
  add_executable(consumer_fully_static main.c)
  set_target_properties(consumer_fully_static PROPERTIES
    C_STANDARD 90 C_STANDARD_REQUIRED YES C_EXTENSIONS NO)
  target_compile_options(consumer_fully_static PRIVATE -Wall -Wextra -Werror -pedantic-errors)
  target_link_libraries(consumer_fully_static PRIVATE lockdc::postgres_static)
  target_link_options(consumer_fully_static PRIVATE -static)
endif()
]=])
get_filename_component(compiler_dir "${CMAKE_C_COMPILER}" DIRECTORY)
get_filename_component(compiler_name "${CMAKE_C_COMPILER}" NAME)
string(REGEX REPLACE "gcc$" "c++" cxx_name "${compiler_name}")
set(prefixes "${sdk};${core_sdk}")
foreach(component cpkt cpktdb pslog lonejson liblql)
  list(APPEND prefixes "${LOCKDC_EXTERNAL_ROOT}/${component}/install")
endforeach()
lockdc_resolve_bootlin_runtime("${CMAKE_SYSROOT}" "${LOCKDC_EXTERNAL_ROOT}"
    "${sdk}/lib" loader runtime_dirs)
list(APPEND runtime_dirs "${core_sdk}/lib")
lockdc_bootlin_runtime_link_flags("${loader}" "${runtime_dirs}" runtime_flags)
execute_process(COMMAND "${CMAKE_COMMAND}" -S "${source}" -B "${root}/build"
    "-DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}"
    "-DCMAKE_CXX_COMPILER=${compiler_dir}/${cxx_name}"
    "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
    "-DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}"
    "-DCMAKE_C_FLAGS_DEBUG=${CMAKE_C_FLAGS_DEBUG}"
    "-DCMAKE_C_FLAGS_RELEASE=${CMAKE_C_FLAGS_RELEASE}"
    "-DCMAKE_C_FLAGS_RELWITHDEBINFO=${CMAKE_C_FLAGS_RELWITHDEBINFO}"
    "-DCMAKE_C_FLAGS_MINSIZEREL=${CMAKE_C_FLAGS_MINSIZEREL}"
    "-DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}"
    "-DCMAKE_CXX_FLAGS_DEBUG=${CMAKE_CXX_FLAGS_DEBUG}"
    "-DCMAKE_EXE_LINKER_FLAGS=${CMAKE_EXE_LINKER_FLAGS}"
    "-DSANITIZER_OPTIONS=${sanitizer_options}"
    "-DCMAKE_PREFIX_PATH=${prefixes}" "-DPROBE_RUNTIME_FLAGS=${runtime_flags}"
    RESULT_VARIABLE rc OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(NOT rc EQUAL 0)
  message(FATAL_ERROR "Relocated PostgreSQL configure failed\n${output}\n${error}")
endif()
execute_process(COMMAND "${CMAKE_COMMAND}" --build "${root}/build" --parallel 1
    RESULT_VARIABLE rc OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(NOT rc EQUAL 0)
  message(FATAL_ERROR "Relocated PostgreSQL link failed\n${output}\n${error}")
endif()
foreach(variant static shared)
  foreach(language c cpp)
    execute_process(COMMAND "${root}/build/consumer_${variant}_${language}"
        RESULT_VARIABLE rc OUTPUT_VARIABLE output ERROR_VARIABLE error)
    if(NOT rc EQUAL 0)
      message(FATAL_ERROR "${variant}/${language} consumer failed\n${output}\n${error}")
    endif()
  endforeach()
endforeach()
if(NOT LOCKDC_SANITIZER_INSTRUMENTED)
  if(NOT EXISTS "${root}/build/consumer_fully_static")
    message(FATAL_ERROR "Required fully static consumer was not produced")
  endif()
  execute_process(COMMAND "${root}/build/consumer_fully_static"
    RESULT_VARIABLE rc OUTPUT_VARIABLE output ERROR_VARIABLE error)
  if(NOT rc EQUAL 0)
    message(FATAL_ERROR "Fully static PostgreSQL consumer failed\n${output}\n${error}")
  endif()
  lockdc_import_cmake_cache_value("${LOCKDC_BINARY_DIR}" CMAKE_READELF)
  execute_process(COMMAND "${CMAKE_READELF}" -l -d "${root}/build/consumer_fully_static"
    RESULT_VARIABLE rc OUTPUT_VARIABLE elf ERROR_VARIABLE error)
  if(NOT rc EQUAL 0 OR elf MATCHES "INTERP|NEEDED")
    message(FATAL_ERROR "Fully static consumer has loader dependencies\n${elf}\n${error}")
  endif()
endif()
# The optional provider imports public core APIs only, never private helpers or
# native PostgreSQL symbols into the core's ABI. Existing core export test is separate.
execute_process(COMMAND "${CMAKE_NM}" -D --undefined-only
    "${sdk}/lib/liblockdc_postgres.so"
    RESULT_VARIABLE rc OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(NOT rc EQUAL 0 OR output MATCHES "lc_pouch_|lc_client_handle_|lc_engine_")
  message(FATAL_ERROR "Provider imports a private core API\n${output}\n${error}")
endif()

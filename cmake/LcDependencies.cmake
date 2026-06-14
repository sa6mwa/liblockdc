include(ExternalProject)

function(lc_record_dependency_target target_name)
  set_property(GLOBAL APPEND PROPERTY LOCKDC_DEPENDENCY_TARGETS "${target_name}")
endfunction()

function(lc_require_dependency_file path label)
  if(NOT EXISTS "${path}")
    message(FATAL_ERROR
      "${label} was not found at ${path}\n"
      "Provide the dependency tree for this preset under LOCKDC_EXTERNAL_ROOT.\n"
      "You can provision it explicitly with scripts/deps.sh, but normal build/test/release entry points do not do that for you.")
  endif()
endfunction()

function(lc_normalize_prefix var path)
  file(TO_CMAKE_PATH "${path}" _normalized)
  set(${var} "${_normalized}" PARENT_SCOPE)
endfunction()

function(lc_require_cpkt_config package_name config_path)
  if(NOT EXISTS "${config_path}")
    message(FATAL_ERROR
      "${package_name} CMake package metadata was not found at ${config_path}\n"
      "liblockdc requires c.pkt.systems ${LOCKDC_CPKT_VERSION} or newer dependency bundles with CMake metadata.\n"
      "Run scripts/deps.sh for the target preset or clean stale dependency roots with make clean.")
  endif()
endfunction()

function(lc_add_interface_alias alias_target upstream_target)
  if(TARGET "${alias_target}")
    return()
  endif()
  if(NOT TARGET "${upstream_target}")
    message(FATAL_ERROR "Cannot create ${alias_target}; missing upstream target ${upstream_target}")
  endif()
  add_library(${alias_target} INTERFACE IMPORTED GLOBAL)
  set_target_properties(${alias_target}
    PROPERTIES
      INTERFACE_LINK_LIBRARIES "${upstream_target}"
  )
endfunction()

function(lc_configure_cpkt_package_roots)
  set(cpkt_root "${LOCKDC_EXTERNAL_ROOT}/c.pkt.systems/install")

  lc_require_cpkt_config("OpenSSL" "${cpkt_root}/lib/cmake/OpenSSL/OpenSSLConfig.cmake")
  lc_require_cpkt_config("ZLIB" "${cpkt_root}/lib/cmake/zlib/ZLIBConfig.cmake")
  lc_require_cpkt_config("nghttp2" "${cpkt_root}/lib/cmake/nghttp2/nghttp2Config.cmake")
  lc_require_cpkt_config("libssh2" "${cpkt_root}/lib/cmake/libssh2/libssh2-config.cmake")
  lc_require_cpkt_config("CURL" "${cpkt_root}/lib/cmake/CURL/CURLConfig.cmake")

  set(OpenSSL_DIR "${cpkt_root}/lib/cmake/OpenSSL" CACHE PATH "c.pkt.systems OpenSSL CMake package directory." FORCE)
  set(ZLIB_DIR "${cpkt_root}/lib/cmake/zlib" CACHE PATH "c.pkt.systems zlib CMake package directory." FORCE)
  set(nghttp2_DIR "${cpkt_root}/lib/cmake/nghttp2" CACHE PATH "c.pkt.systems nghttp2 CMake package directory." FORCE)
  set(Libssh2_DIR "${cpkt_root}/lib/cmake/libssh2" CACHE PATH "c.pkt.systems libssh2 CMake package directory." FORCE)
  set(CURL_DIR "${cpkt_root}/lib/cmake/CURL" CACHE PATH "c.pkt.systems CURL CMake package directory." FORCE)

  set(OpenSSL_DIR "${OpenSSL_DIR}" PARENT_SCOPE)
  set(ZLIB_DIR "${ZLIB_DIR}" PARENT_SCOPE)
  set(nghttp2_DIR "${nghttp2_DIR}" PARENT_SCOPE)
  set(Libssh2_DIR "${Libssh2_DIR}" PARENT_SCOPE)
  set(CURL_DIR "${CURL_DIR}" PARENT_SCOPE)
endfunction()

function(lc_get_external_c_flags out_var)
  set(_flags "-O2 -DNDEBUG -g0")
  if(CMAKE_C_COMPILER_ID MATCHES "^(AppleClang|Clang|GNU)$")
    string(APPEND _flags
      " -fmacro-prefix-map=${LOCKDC_DEPENDENCY_BUILD_ROOT}=deps-build"
      " -fmacro-prefix-map=${LOCKDC_EXTERNAL_ROOT}=deps"
    )
  endif()
  if(NOT "${CMAKE_C_FLAGS}" STREQUAL "")
    set(_flags "${CMAKE_C_FLAGS} ${_flags}")
  endif()
  string(STRIP "${_flags}" _flags)
  set(${out_var} "${_flags}" PARENT_SCOPE)
endfunction()

function(lc_get_strip_dependency_install_command out_var install_dir)
  if(NOT CMAKE_STRIP)
    message(FATAL_ERROR "CMAKE_STRIP is required when building release dependencies")
  endif()

  set(_strip_static_archives ON)
  set(_darwin_fixup_args "")
  if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(_strip_static_archives OFF)
    if(NOT CMAKE_INSTALL_NAME_TOOL)
      message(FATAL_ERROR "CMAKE_INSTALL_NAME_TOOL is required when building Darwin dependencies")
    endif()
    if(NOT LOCKDC_OTOOL)
      message(FATAL_ERROR "LOCKDC_OTOOL is required when building Darwin dependencies")
    endif()
    set(_darwin_fixup_args
      -DLOCKDC_DARWIN_DEPENDENCY_ROOT=${LOCKDC_EXTERNAL_ROOT}
      -DLOCKDC_INSTALL_NAME_TOOL=${CMAKE_INSTALL_NAME_TOOL}
      -DLOCKDC_OTOOL=${LOCKDC_OTOOL}
    )
  endif()

  set(_command
    ${CMAKE_COMMAND}
      -DLOCKDC_STRIP_BIN=${CMAKE_STRIP}
      -DLOCKDC_STRIP_ROOT=${install_dir}
      -DLOCKDC_STRIP_STATIC_ARCHIVES=${_strip_static_archives}
      ${_darwin_fixup_args}
      -P ${CMAKE_SOURCE_DIR}/cmake/strip_dependency_install_tree.cmake
  )
  set(${out_var} "${_command}" PARENT_SCOPE)
endfunction()

function(lc_append_common_external_cmake_args out_var)
  set(_args
    -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
    -DCMAKE_AR=${CMAKE_AR}
    -DCMAKE_RANLIB=${CMAKE_RANLIB}
    -Wno-dev
  )

  if(CMAKE_TOOLCHAIN_FILE)
    list(APPEND _args -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE})
  endif()

  lc_get_external_c_flags(_lockdc_external_c_flags)
  list(APPEND _args -DCMAKE_C_FLAGS=${_lockdc_external_c_flags})

  set(${out_var} "${_args}" PARENT_SCOPE)
endfunction()

function(lc_add_openssl)
  find_package(OpenSSL ${LOCKDC_OPENSSL_VERSION} CONFIG REQUIRED)
  lc_add_interface_alias(lc::openssl_crypto_static OpenSSL::Crypto)
  lc_add_interface_alias(lc::openssl_ssl_static OpenSSL::SSL)
  lc_add_interface_alias(lc::openssl_crypto_shared cpkt::openssl_crypto_shared)
  lc_add_interface_alias(lc::openssl_ssl_shared cpkt::openssl_ssl_shared)
endfunction()

function(lc_add_nghttp2)
  find_package(nghttp2 ${LOCKDC_NGHTTP2_VERSION} CONFIG REQUIRED)
  lc_add_interface_alias(lc::nghttp2_static nghttp2::nghttp2)
  lc_add_interface_alias(lc::nghttp2_shared cpkt::nghttp2_shared)
endfunction()

function(lc_add_zlib)
  find_package(ZLIB ${LOCKDC_ZLIB_VERSION} CONFIG REQUIRED)
  lc_add_interface_alias(lc::zlib_static ZLIB::ZLIB)
  lc_add_interface_alias(lc::zlib_shared cpkt::zlib_shared)
endfunction()

function(lc_add_libssh2)
  find_package(Libssh2 ${LOCKDC_LIBSSH2_VERSION} CONFIG REQUIRED)
  lc_add_interface_alias(lc::libssh2_static Libssh2::libssh2)
  lc_add_interface_alias(lc::libssh2_shared cpkt::libssh2_shared)
endfunction()

function(lc_add_curl)
  find_package(CURL ${LOCKDC_CURL_VERSION} CONFIG REQUIRED)
  lc_add_interface_alias(lc::curl_static CURL::libcurl)
  lc_add_interface_alias(lc::curl_shared cpkt::curl_shared)
endfunction()

function(lc_get_lonejson_asset_info out_name out_hash)
  set(asset_name "liblonejson-${LOCKDC_LONEJSON_VERSION}-${LOCKDC_TARGET_ID}.tar.gz")

  if(asset_name STREQUAL "liblonejson-0.31.0-x86_64-linux-gnu.tar.gz")
    set(asset_hash "f3f1de0f04b4d9491dacc856de38cd1ecde488fb8befc539bf615ac4d9490b54")
  elseif(asset_name STREQUAL "liblonejson-0.31.0-x86_64-linux-musl.tar.gz")
    set(asset_hash "967c797ca17040960ee0fc3339abda040ab3f52c6f9b8d79e729284f433ad1af")
  elseif(asset_name STREQUAL "liblonejson-0.31.0-aarch64-linux-gnu.tar.gz")
    set(asset_hash "1594b8da68a5acda24addcad19a65f4001a75b2fac26a86e8455229352732ca8")
  elseif(asset_name STREQUAL "liblonejson-0.31.0-aarch64-linux-musl.tar.gz")
    set(asset_hash "81f045090cb81b727bc390cfcc9bf46b9ee8a0ed80fcf75fd1b9cdced3f787e1")
  elseif(asset_name STREQUAL "liblonejson-0.31.0-armhf-linux-gnu.tar.gz")
    set(asset_hash "34d03906e00888a2aa9fb01073668b398c271faef01e9f70232523bf0b4be481")
  elseif(asset_name STREQUAL "liblonejson-0.31.0-armhf-linux-musl.tar.gz")
    set(asset_hash "c7fbeda5ba8318c01fda0fabe65903f8dce39d6f96564afa7efd1aca80230644")
  elseif(asset_name STREQUAL "liblonejson-0.31.0-arm64-apple-darwin.tar.gz")
    set(asset_hash "b833e8b2f385294ba085d12dd5d576dd2d8ac271abcbabdf36c81237b27fa14a")
  else()
    message(FATAL_ERROR "Unsupported liblonejson release asset: ${asset_name}")
  endif()

  set(${out_name} "${asset_name}" PARENT_SCOPE)
  set(${out_hash} "${asset_hash}" PARENT_SCOPE)
endfunction()

function(lc_add_lonejson)
  set(project_name "lc_lonejson_project")
  set(prefix_dir "${LOCKDC_DEPENDENCY_BUILD_ROOT}/lonejson")
  set(source_dir "${prefix_dir}/src")
  set(build_dir "${prefix_dir}/build")
  set(install_dir "${LOCKDC_EXTERNAL_ROOT}/lonejson/install")
  set(stamp_dir "${prefix_dir}/stamp")
  set(tmp_dir "${prefix_dir}/tmp")
  set(download_dir "${LOCKDC_DOWNLOAD_ROOT}")
  lc_get_lonejson_asset_info(asset_name asset_hash)
  lc_get_strip_dependency_install_command(strip_install_command "${install_dir}")

  file(MAKE_DIRECTORY "${install_dir}/include" "${install_dir}/lib")

  if(LOCKDC_BUILD_DEPENDENCIES)
    ExternalProject_Add(${project_name}
      URL "https://github.com/sa6mwa/lonejson/releases/download/v${LOCKDC_LONEJSON_VERSION}/${asset_name}"
      URL_HASH "SHA256=${asset_hash}"
      DOWNLOAD_NAME "${asset_name}"
      PREFIX "${prefix_dir}"
      DOWNLOAD_DIR "${download_dir}"
      SOURCE_DIR "${source_dir}"
      STAMP_DIR "${stamp_dir}"
      TMP_DIR "${tmp_dir}"
      TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_TIMEOUT}
      INACTIVITY_TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_INACTIVITY_TIMEOUT}
      CONFIGURE_COMMAND ""
      BUILD_COMMAND ""
      INSTALL_COMMAND
        ${CMAKE_COMMAND} -E rm -rf "${install_dir}"
        COMMAND ${CMAKE_COMMAND} -E copy_directory "${source_dir}" "${install_dir}"
        COMMAND ${strip_install_command}
      BUILD_IN_SOURCE 1
      DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
  endif()

  add_library(lc::lonejson_static STATIC IMPORTED GLOBAL)
  set_target_properties(lc::lonejson_static
    PROPERTIES
      IMPORTED_LOCATION "${install_dir}/lib/liblonejson${CMAKE_STATIC_LIBRARY_SUFFIX}"
      INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
  )
  if(LOCKDC_BUILD_DEPENDENCIES)
    add_dependencies(lc::lonejson_static ${project_name})
    lc_record_dependency_target(${project_name})
  else()
    lc_require_dependency_file("${install_dir}/lib/liblonejson${CMAKE_STATIC_LIBRARY_SUFFIX}" "lonejson (static)")
    lc_require_dependency_file("${install_dir}/include/lonejson.h" "lonejson header")
  endif()

  if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(lonejson_shared_library "${install_dir}/lib/liblonejson.${LOCKDC_LONEJSON_ABI_VERSION}${CMAKE_SHARED_LIBRARY_SUFFIX}")
  else()
    set(lonejson_shared_library "${install_dir}/lib/liblonejson${CMAKE_SHARED_LIBRARY_SUFFIX}.${LOCKDC_LONEJSON_ABI_VERSION}")
  endif()

  add_library(lc::lonejson_shared SHARED IMPORTED GLOBAL)
  set_target_properties(lc::lonejson_shared
    PROPERTIES
      IMPORTED_LOCATION "${lonejson_shared_library}"
      INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
  )
  if(LOCKDC_BUILD_DEPENDENCIES)
    add_dependencies(lc::lonejson_shared ${project_name})
  else()
    lc_require_dependency_file("${lonejson_shared_library}" "lonejson (shared)")
  endif()
endfunction()

function(lc_add_cmocka)
  set(project_name "lc_cmocka_project")
  set(prefix_dir "${LOCKDC_DEPENDENCY_BUILD_ROOT}/cmocka")
  set(source_dir "${prefix_dir}/src")
  set(build_dir "${prefix_dir}/build")
  set(install_dir "${LOCKDC_EXTERNAL_ROOT}/cmocka/install")
  set(stamp_dir "${prefix_dir}/stamp")
  set(tmp_dir "${prefix_dir}/tmp")
  lc_append_common_external_cmake_args(common_cmake_args)
  lc_get_strip_dependency_install_command(strip_install_command "${install_dir}")
  file(MAKE_DIRECTORY "${install_dir}/include" "${install_dir}/lib")

  if(LOCKDC_BUILD_DEPENDENCIES)
    ExternalProject_Add(${project_name}
      URL "https://cmocka.org/files/2.0/cmocka-${LOCKDC_CMOCKA_VERSION}.tar.xz"
      URL_HASH "SHA256=39f92f366bdf3f1a02af4da75b4a5c52df6c9f7e736c7d65de13283f9f0ef416"
      PREFIX "${prefix_dir}"
      DOWNLOAD_DIR "${LOCKDC_DOWNLOAD_ROOT}"
      SOURCE_DIR "${source_dir}"
      BINARY_DIR "${build_dir}"
      STAMP_DIR "${stamp_dir}"
      TMP_DIR "${tmp_dir}"
      TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_TIMEOUT}
      INACTIVITY_TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_INACTIVITY_TIMEOUT}
      CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${install_dir}
        -DCMAKE_BUILD_TYPE=${LOCKDC_DEPENDENCY_BUILD_TYPE}
        -DBUILD_SHARED_LIBS=OFF
        -DBUILD_TESTING=OFF
        -DWITH_EXAMPLES=OFF
        -DPICKY_DEVELOPER=OFF
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        ${common_cmake_args}
      BUILD_COMMAND ${CMAKE_COMMAND} --build . --parallel ${LOCKDC_DEPENDENCY_BUILD_JOBS}
      INSTALL_COMMAND ${CMAKE_COMMAND} --install .
        COMMAND ${strip_install_command}
      DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
  endif()

  add_library(lc::cmocka STATIC IMPORTED GLOBAL)
  set_target_properties(lc::cmocka
    PROPERTIES
      IMPORTED_LOCATION "${install_dir}/lib/libcmocka${CMAKE_STATIC_LIBRARY_SUFFIX}"
      INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
  )
  if(LOCKDC_BUILD_DEPENDENCIES)
    add_dependencies(lc::cmocka ${project_name})
    lc_record_dependency_target(${project_name})
  else()
    lc_require_dependency_file("${install_dir}/lib/libcmocka${CMAKE_STATIC_LIBRARY_SUFFIX}" "cmocka")
  endif()
endfunction()

function(lc_get_pslog_asset_info out_name out_hash)
  set(asset_name "libpslog-${LOCKDC_PSLOG_VERSION}-${LOCKDC_TARGET_ID}.tar.gz")

  if(asset_name STREQUAL "libpslog-0.4.1-x86_64-linux-gnu.tar.gz")
    set(asset_hash "91d2f93bc07bc66cf83d6a27a80cb6439c384d56bf84a2d11cd903215430d1d8")
  elseif(asset_name STREQUAL "libpslog-0.4.1-x86_64-linux-musl.tar.gz")
    set(asset_hash "b628d32f9207e5102c9a8ae3f7ad32ce36e61178c7db67e6aa4548eb9cae567d")
  elseif(asset_name STREQUAL "libpslog-0.4.1-aarch64-linux-gnu.tar.gz")
    set(asset_hash "d936ae9416f539c4f40aeaa023b9147cbd568bc87b7a3c3b091adfd217d935bb")
  elseif(asset_name STREQUAL "libpslog-0.4.1-aarch64-linux-musl.tar.gz")
    set(asset_hash "638725174cf39f3c5337fc6f118bc88c2a41d385a01be98170ff4bef3d57fcae")
  elseif(asset_name STREQUAL "libpslog-0.4.1-armhf-linux-gnu.tar.gz")
    set(asset_hash "bc8530a3773666deb6d551263c7dd59a64c92629fa56d1e89c278d637472f2dc")
  elseif(asset_name STREQUAL "libpslog-0.4.1-armhf-linux-musl.tar.gz")
    set(asset_hash "503d2bd882c053dc8f34dbfe718a328303a4973789bbb8fb37261e4822b3babe")
  elseif(asset_name STREQUAL "libpslog-0.4.1-arm64-apple-darwin.tar.gz")
    set(asset_hash "f8f4e18810ecad7278eb341fbfe7e3f9d85eb654891c4d08149f425f3a4c9b3d")
  else()
    message(FATAL_ERROR "Unsupported libpslog asset: ${asset_name}")
  endif()

  set(${out_name} "${asset_name}" PARENT_SCOPE)
  set(${out_hash} "${asset_hash}" PARENT_SCOPE)
endfunction()

function(lc_add_pslog)
  set(project_name "lc_pslog_project")
  set(prefix_dir "${LOCKDC_DEPENDENCY_BUILD_ROOT}/pslog")
  set(source_dir "${prefix_dir}/src")
  set(build_dir "${prefix_dir}/build")
  set(install_dir "${LOCKDC_EXTERNAL_ROOT}/pslog/install")
  set(stamp_dir "${prefix_dir}/stamp")
  set(tmp_dir "${prefix_dir}/tmp")
  lc_get_strip_dependency_install_command(strip_install_command "${install_dir}")
  lc_get_pslog_asset_info(asset_name asset_hash)

  file(MAKE_DIRECTORY "${install_dir}/include" "${install_dir}/lib")

  if(LOCKDC_BUILD_DEPENDENCIES)
    ExternalProject_Add(${project_name}
      URL "https://github.com/sa6mwa/libpslog/releases/download/v${LOCKDC_PSLOG_VERSION}/${asset_name}"
      URL_HASH "SHA256=${asset_hash}"
      DOWNLOAD_NAME "${asset_name}"
      PREFIX "${prefix_dir}"
      DOWNLOAD_DIR "${LOCKDC_DOWNLOAD_ROOT}"
      SOURCE_DIR "${source_dir}"
      STAMP_DIR "${stamp_dir}"
      TMP_DIR "${tmp_dir}"
      TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_TIMEOUT}
      INACTIVITY_TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_INACTIVITY_TIMEOUT}
      CONFIGURE_COMMAND ""
      BUILD_COMMAND ""
      INSTALL_COMMAND
        ${CMAKE_COMMAND} -E rm -rf "${install_dir}"
        COMMAND ${CMAKE_COMMAND} -E copy_directory "${source_dir}" "${install_dir}"
        COMMAND ${strip_install_command}
      BUILD_IN_SOURCE 1
      DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
  endif()

  add_library(lc::pslog_static STATIC IMPORTED GLOBAL)
  set_target_properties(lc::pslog_static
    PROPERTIES
      IMPORTED_LOCATION "${install_dir}/lib/libpslog${CMAKE_STATIC_LIBRARY_SUFFIX}"
      INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
  )
  if(LOCKDC_BUILD_DEPENDENCIES)
    add_dependencies(lc::pslog_static ${project_name})
    lc_record_dependency_target(${project_name})
  else()
    lc_require_dependency_file("${install_dir}/lib/libpslog${CMAKE_STATIC_LIBRARY_SUFFIX}" "libpslog (static)")
    lc_require_dependency_file("${install_dir}/include/pslog.h" "libpslog header")
  endif()

  if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(pslog_shared_library "${install_dir}/lib/libpslog.0${CMAKE_SHARED_LIBRARY_SUFFIX}")
  else()
    set(pslog_shared_library "${install_dir}/lib/libpslog${CMAKE_SHARED_LIBRARY_SUFFIX}.0")
  endif()

  add_library(lc::pslog_shared SHARED IMPORTED GLOBAL)
  set_target_properties(lc::pslog_shared
    PROPERTIES
      IMPORTED_LOCATION "${pslog_shared_library}"
      INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
  )
  if(LOCKDC_BUILD_DEPENDENCIES)
    add_dependencies(lc::pslog_shared ${project_name})
  else()
    lc_require_dependency_file("${pslog_shared_library}" "libpslog (shared)")
  endif()
endfunction()

function(lc_configure_dependencies)
  if(LOCKDC_BUILD_STATIC OR LOCKDC_BUILD_SHARED)
    lc_configure_cpkt_package_roots()
    lc_add_openssl()
  endif()

  lc_add_zlib()
  lc_add_libssh2()

  if(LOCKDC_BUILD_STATIC OR LOCKDC_BUILD_SHARED)
    lc_add_pslog()
    lc_add_nghttp2()
    lc_add_curl()
    lc_add_lonejson()
  endif()

  if(LOCKDC_BUILD_TESTS)
    lc_add_cmocka()
  endif()

  if(LOCKDC_BUILD_DEPENDENCIES)
    get_property(dep_targets GLOBAL PROPERTY LOCKDC_DEPENDENCY_TARGETS)
    if(dep_targets)
      add_custom_target(lc_deps DEPENDS ${dep_targets})
    endif()
  endif()
endfunction()

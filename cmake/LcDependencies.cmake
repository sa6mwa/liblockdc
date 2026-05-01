include(ExternalProject)

function(lc_record_dependency_target target_name)
  set_property(GLOBAL APPEND PROPERTY LOCKDC_DEPENDENCY_TARGETS "${target_name}")
endfunction()

function(lc_require_dependency_file path label)
  if(NOT EXISTS "${path}")
    message(FATAL_ERROR
      "${label} was not found at ${path}\n"
      "Provide the dependency tree for this preset under LOCKDC_EXTERNAL_ROOT.\n"
      "You can prebuild it explicitly with scripts/deps.sh, but normal build/test/release entry points do not do that for you.")
  endif()
endfunction()

function(lc_normalize_prefix var path)
  file(TO_CMAKE_PATH "${path}" _normalized)
  set(${var} "${_normalized}" PARENT_SCOPE)
endfunction()

function(lc_get_target_triple out_var)
  string(TOLOWER "${LOCKDC_TARGET_OS}" _lockdc_target_os_lower)

  if(_lockdc_target_os_lower STREQUAL "darwin")
    if(LOCKDC_TARGET_ARCH STREQUAL "arm64")
      set(_triple "aarch64-apple-darwin")
    else()
      message(FATAL_ERROR "Unsupported Darwin LOCKDC_TARGET_ARCH: ${LOCKDC_TARGET_ARCH}")
    endif()
  elseif(LOCKDC_TARGET_ARCH STREQUAL "x86_64")
    if(LOCKDC_TARGET_LIBC STREQUAL "musl")
      set(_triple "x86_64-linux-musl")
    else()
      set(_triple "x86_64-linux-gnu")
    endif()
  elseif(LOCKDC_TARGET_ARCH STREQUAL "aarch64")
    if(LOCKDC_TARGET_LIBC STREQUAL "musl")
      set(_triple "aarch64-linux-musl")
    else()
      set(_triple "aarch64-linux-gnu")
    endif()
  elseif(LOCKDC_TARGET_ARCH STREQUAL "armhf")
    if(LOCKDC_TARGET_LIBC STREQUAL "musl")
      set(_triple "arm-linux-musleabihf")
    else()
      set(_triple "arm-linux-gnueabihf")
    endif()
  else()
    message(FATAL_ERROR "Unsupported LOCKDC_TARGET_ARCH: ${LOCKDC_TARGET_ARCH}")
  endif()

  set(${out_var} "${_triple}" PARENT_SCOPE)
endfunction()

function(lc_get_openssl_config_target out_var)
  string(TOLOWER "${LOCKDC_TARGET_OS}" _lockdc_target_os_lower)

  if(_lockdc_target_os_lower STREQUAL "darwin")
    if(LOCKDC_TARGET_ARCH STREQUAL "arm64")
      set(_target "darwin64-arm64")
    else()
      message(FATAL_ERROR "Unsupported Darwin LOCKDC_TARGET_ARCH for OpenSSL: ${LOCKDC_TARGET_ARCH}")
    endif()
  elseif(LOCKDC_TARGET_ARCH STREQUAL "x86_64")
    set(_target "linux-x86_64")
  elseif(LOCKDC_TARGET_ARCH STREQUAL "aarch64")
    set(_target "linux-aarch64")
  elseif(LOCKDC_TARGET_ARCH STREQUAL "armhf")
    set(_target "linux-armv4")
  else()
    message(FATAL_ERROR "Unsupported LOCKDC_TARGET_ARCH for OpenSSL: ${LOCKDC_TARGET_ARCH}")
  endif()

  set(${out_var} "${_target}" PARENT_SCOPE)
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

  set(${out_var} "${_args}" PARENT_SCOPE)
endfunction()

function(lc_add_openssl)
  set(project_name "lc_openssl_project")
  set(prefix_dir "${LOCKDC_DEPENDENCY_BUILD_ROOT}/openssl")
  set(source_dir "${prefix_dir}/src")
  set(build_dir "${prefix_dir}/build")
  set(install_dir "${LOCKDC_EXTERNAL_ROOT}/openssl/install")
  set(stamp_dir "${prefix_dir}/stamp")
  set(tmp_dir "${prefix_dir}/tmp")
  lc_get_openssl_config_target(openssl_config_target)
  set(config_args ${openssl_config_target} no-tests no-docs no-module no-apps no-makedepend)
  if(LOCKDC_TARGET_LIBC STREQUAL "musl")
    list(APPEND config_args no-secure-memory no-afalgeng)
  endif()
  if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    list(APPEND config_args shared "-Wl,--enable-new-dtags,-rpath,\\$$ORIGIN")
  elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    list(APPEND config_args shared "-Wl,-rpath,@loader_path")
  else()
    list(APPEND config_args shared)
  endif()

  lc_normalize_prefix(env_prefix "${install_dir}")
  file(MAKE_DIRECTORY "${install_dir}/include" "${install_dir}/lib")
  set(build_command make -j${LOCKDC_DEPENDENCY_BUILD_JOBS})
  set(install_command make -j${LOCKDC_DEPENDENCY_BUILD_JOBS} install_sw)
  set(openssl_env_args
    CC=${CMAKE_C_COMPILER}
    AR=${CMAKE_AR}
    RANLIB=${CMAKE_RANLIB}
  )
  if(CMAKE_SYSTEM_NAME STREQUAL "Darwin" AND CMAKE_LINKER)
    list(APPEND openssl_env_args LDFLAGS=-fuse-ld=${CMAKE_LINKER})
  endif()

  if(LOCKDC_BUILD_DEPENDENCIES)
    ExternalProject_Add(${project_name}
      URL "https://github.com/openssl/openssl/releases/download/openssl-${LOCKDC_OPENSSL_VERSION}/openssl-${LOCKDC_OPENSSL_VERSION}.tar.gz"
      URL_HASH "SHA256=b1bfedcd5b289ff22aee87c9d600f515767ebf45f77168cb6d64f231f518a82e"
      DOWNLOAD_NAME "openssl-${LOCKDC_OPENSSL_VERSION}.tar.gz"
      PREFIX "${prefix_dir}"
      DOWNLOAD_DIR "${LOCKDC_DOWNLOAD_ROOT}"
      SOURCE_DIR "${source_dir}"
      STAMP_DIR "${stamp_dir}"
      TMP_DIR "${tmp_dir}"
      TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_TIMEOUT}
      INACTIVITY_TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_INACTIVITY_TIMEOUT}
      CONFIGURE_COMMAND
        ${CMAKE_COMMAND} -E env
          ${openssl_env_args}
          "${source_dir}/Configure"
          ${config_args}
          --prefix=${env_prefix}
          --openssldir=${env_prefix}/ssl
          --libdir=lib
      BUILD_COMMAND ${build_command}
      INSTALL_COMMAND ${install_command}
      BUILD_IN_SOURCE 1
      DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
  endif()

  file(MAKE_DIRECTORY "${install_dir}/include" "${install_dir}/lib")

  add_library(lc::openssl_crypto_static STATIC IMPORTED GLOBAL)
  set_target_properties(lc::openssl_crypto_static
    PROPERTIES
      IMPORTED_LOCATION "${install_dir}/lib/libcrypto${CMAKE_STATIC_LIBRARY_SUFFIX}"
      INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
  )
  if(LOCKDC_BUILD_DEPENDENCIES)
    add_dependencies(lc::openssl_crypto_static ${project_name})
    lc_record_dependency_target(${project_name})
  else()
    lc_require_dependency_file("${install_dir}/lib/libcrypto${CMAKE_STATIC_LIBRARY_SUFFIX}" "OpenSSL crypto (static)")
  endif()

  add_library(lc::openssl_ssl_static STATIC IMPORTED GLOBAL)
  set_target_properties(lc::openssl_ssl_static
    PROPERTIES
      IMPORTED_LOCATION "${install_dir}/lib/libssl${CMAKE_STATIC_LIBRARY_SUFFIX}"
      INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
      INTERFACE_LINK_LIBRARIES "lc::openssl_crypto_static;${CMAKE_DL_LIBS};Threads::Threads"
  )
  if(LOCKDC_BUILD_DEPENDENCIES)
    add_dependencies(lc::openssl_ssl_static ${project_name})
  else()
    lc_require_dependency_file("${install_dir}/lib/libssl${CMAKE_STATIC_LIBRARY_SUFFIX}" "OpenSSL ssl (static)")
  endif()

  add_library(lc::openssl_crypto_shared SHARED IMPORTED GLOBAL)
  set_target_properties(lc::openssl_crypto_shared
    PROPERTIES
      IMPORTED_LOCATION "${install_dir}/lib/libcrypto${CMAKE_SHARED_LIBRARY_SUFFIX}"
      INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
  )
  if(LOCKDC_BUILD_DEPENDENCIES)
    add_dependencies(lc::openssl_crypto_shared ${project_name})
  else()
    lc_require_dependency_file("${install_dir}/lib/libcrypto${CMAKE_SHARED_LIBRARY_SUFFIX}" "OpenSSL crypto (shared)")
  endif()

  add_library(lc::openssl_ssl_shared SHARED IMPORTED GLOBAL)
  set_target_properties(lc::openssl_ssl_shared
    PROPERTIES
      IMPORTED_LOCATION "${install_dir}/lib/libssl${CMAKE_SHARED_LIBRARY_SUFFIX}"
      INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
      INTERFACE_LINK_LIBRARIES "lc::openssl_crypto_shared;${CMAKE_DL_LIBS};Threads::Threads"
  )
  if(LOCKDC_BUILD_DEPENDENCIES)
    add_dependencies(lc::openssl_ssl_shared ${project_name})
  else()
    lc_require_dependency_file("${install_dir}/lib/libssl${CMAKE_SHARED_LIBRARY_SUFFIX}" "OpenSSL ssl (shared)")
  endif()

  set(LOCKDC_OPENSSL_static_PREFIX "${install_dir}" PARENT_SCOPE)
  set(LOCKDC_OPENSSL_shared_PREFIX "${install_dir}" PARENT_SCOPE)
endfunction()

function(lc_add_nghttp2)
  set(project_name "lc_nghttp2_project")
  set(prefix_dir "${LOCKDC_DEPENDENCY_BUILD_ROOT}/nghttp2")
  set(source_dir "${prefix_dir}/src")
  set(build_dir "${prefix_dir}/build")
  set(install_dir "${LOCKDC_EXTERNAL_ROOT}/nghttp2/install")
  set(stamp_dir "${prefix_dir}/stamp")
  set(tmp_dir "${prefix_dir}/tmp")
  lc_get_target_triple(autotools_host)
  file(MAKE_DIRECTORY "${install_dir}/include" "${install_dir}/lib")
  set(nghttp2_env_args
    CC=${CMAKE_C_COMPILER}
    AR=${CMAKE_AR}
    RANLIB=${CMAKE_RANLIB}
  )
  if(CMAKE_SYSTEM_NAME STREQUAL "Darwin" AND CMAKE_LINKER)
    list(APPEND nghttp2_env_args
      PATH=${LOCKDC_OSXCROSS_BIN_DIR}:$ENV{PATH}
      LDFLAGS=-fuse-ld=${CMAKE_LINKER}
    )
  endif()

  if(LOCKDC_BUILD_DEPENDENCIES)
    ExternalProject_Add(${project_name}
      URL "https://github.com/nghttp2/nghttp2/releases/download/v${LOCKDC_NGHTTP2_VERSION}/nghttp2-${LOCKDC_NGHTTP2_VERSION}.tar.gz"
      URL_HASH "SHA256=2c16ffc588ad3f9e2613c3fad72db48ecb5ce15bc362fcc85b342e48daf51013"
      DOWNLOAD_NAME "nghttp2-${LOCKDC_NGHTTP2_VERSION}.tar.gz"
      PREFIX "${prefix_dir}"
      DOWNLOAD_DIR "${LOCKDC_DOWNLOAD_ROOT}"
      SOURCE_DIR "${source_dir}"
      BINARY_DIR "${build_dir}"
      STAMP_DIR "${stamp_dir}"
      TMP_DIR "${tmp_dir}"
      TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_TIMEOUT}
      INACTIVITY_TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_INACTIVITY_TIMEOUT}
      CONFIGURE_COMMAND
        ${CMAKE_COMMAND} -E env
        ${nghttp2_env_args}
        "${source_dir}/configure"
        --prefix=${install_dir}
        --host=${autotools_host}
        --enable-shared
        --enable-static
        --enable-lib-only
      BUILD_COMMAND make -C lib -j${LOCKDC_DEPENDENCY_BUILD_JOBS}
      INSTALL_COMMAND make -C lib install
      BUILD_IN_SOURCE 0
      DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
  endif()

  add_library(lc::nghttp2_static STATIC IMPORTED GLOBAL)
  set_target_properties(lc::nghttp2_static
    PROPERTIES
      IMPORTED_LOCATION "${install_dir}/lib/libnghttp2${CMAKE_STATIC_LIBRARY_SUFFIX}"
      INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
  )
  if(LOCKDC_BUILD_DEPENDENCIES)
    add_dependencies(lc::nghttp2_static ${project_name})
    lc_record_dependency_target(${project_name})
  else()
    lc_require_dependency_file("${install_dir}/lib/libnghttp2${CMAKE_STATIC_LIBRARY_SUFFIX}" "nghttp2 (static)")
  endif()

  add_library(lc::nghttp2_shared SHARED IMPORTED GLOBAL)
  set_target_properties(lc::nghttp2_shared
    PROPERTIES
      IMPORTED_LOCATION "${install_dir}/lib/libnghttp2${CMAKE_SHARED_LIBRARY_SUFFIX}"
      INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
  )
  if(LOCKDC_BUILD_DEPENDENCIES)
    add_dependencies(lc::nghttp2_shared ${project_name})
  else()
    lc_require_dependency_file("${install_dir}/lib/libnghttp2${CMAKE_SHARED_LIBRARY_SUFFIX}" "nghttp2 (shared)")
  endif()

  set(LOCKDC_NGHTTP2_static_PREFIX "${install_dir}" PARENT_SCOPE)
  set(LOCKDC_NGHTTP2_shared_PREFIX "${install_dir}" PARENT_SCOPE)
endfunction()

function(lc_add_zlib)
  set(project_name "lc_zlib_project")
  set(prefix_dir "${LOCKDC_DEPENDENCY_BUILD_ROOT}/zlib")
  set(source_dir "${prefix_dir}/src")
  set(build_dir "${prefix_dir}/build")
  set(install_dir "${LOCKDC_EXTERNAL_ROOT}/zlib/install")
  set(stamp_dir "${prefix_dir}/stamp")
  set(tmp_dir "${prefix_dir}/tmp")
  lc_append_common_external_cmake_args(common_cmake_args)
  file(MAKE_DIRECTORY "${install_dir}/include" "${install_dir}/lib")

  if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(zlib_shared_library "${install_dir}/lib/libz.${LOCKDC_ZLIB_VERSION}${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(zlib_shared_soname "${install_dir}/lib/libz.1${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(zlib_shared_link "${install_dir}/lib/libz${CMAKE_SHARED_LIBRARY_SUFFIX}")
  else()
    set(zlib_shared_library "${install_dir}/lib/libz${CMAKE_SHARED_LIBRARY_SUFFIX}.${LOCKDC_ZLIB_VERSION}")
    set(zlib_shared_soname "${install_dir}/lib/libz${CMAKE_SHARED_LIBRARY_SUFFIX}.1")
    set(zlib_shared_link "${install_dir}/lib/libz${CMAKE_SHARED_LIBRARY_SUFFIX}")
  endif()
  set(zlib_static_library "${install_dir}/lib/libz${CMAKE_STATIC_LIBRARY_SUFFIX}")

  if(LOCKDC_BUILD_DEPENDENCIES)
    ExternalProject_Add(${project_name}
      URL
        "https://www.zlib.net/zlib-${LOCKDC_ZLIB_VERSION}.tar.gz"
        "https://zlib.net/fossils/zlib-${LOCKDC_ZLIB_VERSION}.tar.gz"
      URL_HASH "SHA256=bb329a0a2cd0274d05519d61c667c062e06990d72e125ee2dfa8de64f0119d16"
      DOWNLOAD_NAME "zlib-${LOCKDC_ZLIB_VERSION}.tar.gz"
      PREFIX "${prefix_dir}"
      DOWNLOAD_DIR "${LOCKDC_DOWNLOAD_ROOT}"
      SOURCE_DIR "${source_dir}"
      BINARY_DIR "${build_dir}"
      STAMP_DIR "${stamp_dir}"
      TMP_DIR "${tmp_dir}"
      TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_TIMEOUT}
      INACTIVITY_TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_INACTIVITY_TIMEOUT}
      PATCH_COMMAND ${CMAKE_COMMAND}
        -DLOCKDC_ZLIB_SOURCE_DIR=<SOURCE_DIR>
        -P ${CMAKE_SOURCE_DIR}/cmake/patch_zlib_single_pass.cmake
      CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${install_dir}
        -DCMAKE_INSTALL_LIBDIR=lib
        -DCMAKE_BUILD_TYPE=${LOCKDC_DEPENDENCY_BUILD_TYPE}
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DZLIB_BUILD_SHARED=ON
        -DZLIB_BUILD_STATIC=ON
        -DZLIB_BUILD_TESTING=OFF
        -DZLIB_INSTALL=ON
        ${common_cmake_args}
      BUILD_COMMAND ${CMAKE_COMMAND} --build . --parallel ${LOCKDC_DEPENDENCY_BUILD_JOBS}
      INSTALL_COMMAND ${CMAKE_COMMAND} --install .
      BUILD_IN_SOURCE 0
      DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
  endif()

  add_library(lc::zlib_static STATIC IMPORTED GLOBAL)
  set_target_properties(lc::zlib_static
    PROPERTIES
      IMPORTED_LOCATION "${zlib_static_library}"
      INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
  )

  add_library(lc::zlib_shared SHARED IMPORTED GLOBAL)
  set_target_properties(lc::zlib_shared
    PROPERTIES
      IMPORTED_LOCATION "${zlib_shared_library}"
      INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
  )

  if(LOCKDC_BUILD_DEPENDENCIES)
    add_dependencies(lc::zlib_static ${project_name})
    add_dependencies(lc::zlib_shared ${project_name})
    lc_record_dependency_target(${project_name})
  else()
    lc_require_dependency_file("${zlib_static_library}" "zlib static library")
    lc_require_dependency_file("${zlib_shared_library}" "zlib shared library")
    lc_require_dependency_file("${zlib_shared_soname}" "zlib shared-library SONAME")
    lc_require_dependency_file("${zlib_shared_link}" "zlib shared-library linker symlink")
    lc_require_dependency_file("${install_dir}/include/zlib.h" "zlib header")
    lc_require_dependency_file("${install_dir}/include/zconf.h" "zlib configuration header")
  endif()

  set(LOCKDC_ZLIB_PREFIX "${install_dir}" PARENT_SCOPE)
  set(LOCKDC_ZLIB_SHARED_LIBRARY "${zlib_shared_library}" PARENT_SCOPE)
endfunction()

function(lc_add_libssh2)
  set(project_name "lc_libssh2_project")
  set(openssl_project "")
  set(prefix_dir "${LOCKDC_DEPENDENCY_BUILD_ROOT}/libssh2")
  set(source_dir "${prefix_dir}/src")
  set(build_dir "${prefix_dir}/build")
  set(install_dir "${LOCKDC_EXTERNAL_ROOT}/libssh2/install")
  set(stamp_dir "${prefix_dir}/stamp")
  set(tmp_dir "${prefix_dir}/tmp")
  lc_append_common_external_cmake_args(common_cmake_args)
  file(MAKE_DIRECTORY "${install_dir}/include" "${install_dir}/lib")

  set(libssh2_shared_library "${install_dir}/lib/libssh2${CMAKE_SHARED_LIBRARY_SUFFIX}")
  if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(libssh2_shared_library "${install_dir}/lib/libssh2.1${CMAKE_SHARED_LIBRARY_SUFFIX}")
  endif()
  set(libssh2_static_library "${install_dir}/lib/libssh2${CMAKE_STATIC_LIBRARY_SUFFIX}")
  if(NOT DEFINED LOCKDC_ZLIB_PREFIX OR "${LOCKDC_ZLIB_PREFIX}" STREQUAL "")
    message(FATAL_ERROR "libssh2 requires zlib to be configured first")
  endif()
  if(DEFINED LOCKDC_OPENSSL_shared_PREFIX AND NOT "${LOCKDC_OPENSSL_shared_PREFIX}" STREQUAL "")
    set(libssh2_openssl_prefix "${LOCKDC_OPENSSL_shared_PREFIX}")
    set(libssh2_openssl_build_variant "shared")
    set(openssl_project "lc_openssl_project")
    set(libssh2_openssl_ssl_library "${libssh2_openssl_prefix}/lib/libssl${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(libssh2_openssl_crypto_library "${libssh2_openssl_prefix}/lib/libcrypto${CMAKE_SHARED_LIBRARY_SUFFIX}")
  elseif(DEFINED LOCKDC_OPENSSL_static_PREFIX AND NOT "${LOCKDC_OPENSSL_static_PREFIX}" STREQUAL "")
    set(libssh2_openssl_prefix "${LOCKDC_OPENSSL_static_PREFIX}")
    set(libssh2_openssl_build_variant "static")
    set(openssl_project "lc_openssl_project")
    set(libssh2_openssl_ssl_library "${libssh2_openssl_prefix}/lib/libssl${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(libssh2_openssl_crypto_library "${libssh2_openssl_prefix}/lib/libcrypto${CMAKE_STATIC_LIBRARY_SUFFIX}")
  else()
    message(FATAL_ERROR "libssh2 requires OpenSSL to be configured first")
  endif()
  if(DEFINED LOCKDC_OPENSSL_static_PREFIX AND NOT "${LOCKDC_OPENSSL_static_PREFIX}" STREQUAL "")
    set(libssh2_openssl_link_variant "static")
  else()
    set(libssh2_openssl_link_variant "${libssh2_openssl_build_variant}")
  endif()

  if(LOCKDC_BUILD_DEPENDENCIES)
    ExternalProject_Add(${project_name}
      URL "https://libssh2.org/download/libssh2-${LOCKDC_LIBSSH2_VERSION}.tar.gz"
      URL_HASH "SHA256=d9ec76cbe34db98eec3539fe2c899d26b0c837cb3eb466a56b0f109cabf658f7"
      DOWNLOAD_NAME "libssh2-${LOCKDC_LIBSSH2_VERSION}.tar.gz"
      PREFIX "${prefix_dir}"
      DOWNLOAD_DIR "${LOCKDC_DOWNLOAD_ROOT}"
      SOURCE_DIR "${source_dir}"
      BINARY_DIR "${build_dir}"
      STAMP_DIR "${stamp_dir}"
      TMP_DIR "${tmp_dir}"
      TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_TIMEOUT}
      INACTIVITY_TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_INACTIVITY_TIMEOUT}
      PATCH_COMMAND ${CMAKE_COMMAND}
        -DLOCKDC_LIBSSH2_SOURCE_DIR=<SOURCE_DIR>
        -P ${CMAKE_SOURCE_DIR}/cmake/patch_libssh2_single_pass.cmake
      CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${install_dir}
        -DCMAKE_INSTALL_LIBDIR=lib
        -DCMAKE_BUILD_TYPE=${LOCKDC_DEPENDENCY_BUILD_TYPE}
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_FIND_PACKAGE_PREFER_CONFIG=ON
        -DBUILD_STATIC_LIBS=ON
        -DBUILD_SHARED_LIBS=ON
        -DBUILD_EXAMPLES=OFF
        -DBUILD_TESTING=OFF
        -DENABLE_ZLIB_COMPRESSION=ON
        -DCRYPTO_BACKEND=OpenSSL
        -DOpenSSL_DIR=${libssh2_openssl_prefix}/lib/cmake/OpenSSL
        -DZLIB_ROOT=${LOCKDC_ZLIB_PREFIX}
        -DZLIB_DIR=${LOCKDC_ZLIB_PREFIX}/lib/cmake/zlib
        -DZLIB_INCLUDE_DIRS=${LOCKDC_ZLIB_PREFIX}/include
        -DZLIB_LIBRARIES=${LOCKDC_ZLIB_SHARED_LIBRARY}
        ${common_cmake_args}
      DEPENDS
        ${openssl_project}
        lc_zlib_project
      BUILD_COMMAND ${CMAKE_COMMAND} --build . --parallel ${LOCKDC_DEPENDENCY_BUILD_JOBS}
      INSTALL_COMMAND ${CMAKE_COMMAND} --install .
      BUILD_IN_SOURCE 0
      DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
  endif()

  add_library(lc::libssh2_static STATIC IMPORTED GLOBAL)
  set_target_properties(lc::libssh2_static
    PROPERTIES
      IMPORTED_LOCATION "${libssh2_static_library}"
      INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
      INTERFACE_LINK_LIBRARIES "lc::openssl_crypto_${libssh2_openssl_link_variant};lc::zlib_static"
  )

  add_library(lc::libssh2_shared SHARED IMPORTED GLOBAL)
  set_target_properties(lc::libssh2_shared
    PROPERTIES
      IMPORTED_LOCATION "${libssh2_shared_library}"
      INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
      INTERFACE_LINK_LIBRARIES "lc::zlib_shared"
  )

  if(LOCKDC_BUILD_DEPENDENCIES)
    add_dependencies(lc::libssh2_static ${project_name})
    add_dependencies(lc::libssh2_shared ${project_name})
    lc_record_dependency_target(${project_name})
  else()
    lc_require_dependency_file("${libssh2_static_library}" "libssh2 static library")
    lc_require_dependency_file("${libssh2_shared_library}" "libssh2 shared library")
    lc_require_dependency_file("${install_dir}/include/libssh2.h" "libssh2 header")
    lc_require_dependency_file("${install_dir}/include/libssh2_publickey.h" "libssh2 publickey header")
    lc_require_dependency_file("${install_dir}/include/libssh2_sftp.h" "libssh2 sftp header")
  endif()

  set(LOCKDC_LIBSSH2_PREFIX "${install_dir}" PARENT_SCOPE)
endfunction()

function(lc_add_curl)
  set(project_name "lc_curl_project")
  set(openssl_project "lc_openssl_project")
  set(nghttp2_project "lc_nghttp2_project")
  set(libssh2_project "lc_libssh2_project")
  set(zlib_project "lc_zlib_project")
  set(prefix_dir "${LOCKDC_DEPENDENCY_BUILD_ROOT}/curl")
  set(install_dir "${LOCKDC_EXTERNAL_ROOT}/curl/install")
  set(openssl_prefix "${LOCKDC_OPENSSL_shared_PREFIX}")
  set(nghttp2_prefix "${LOCKDC_NGHTTP2_shared_PREFIX}")
  set(libssh2_prefix "${LOCKDC_LIBSSH2_PREFIX}")
  set(curl_download_name "curl-${LOCKDC_CURL_VERSION}.tar.xz")
  set(curl_openssl_ssl_library "${openssl_prefix}/lib/libssl${CMAKE_SHARED_LIBRARY_SUFFIX}")
  set(curl_openssl_crypto_library "${openssl_prefix}/lib/libcrypto${CMAKE_SHARED_LIBRARY_SUFFIX}")
  set(curl_nghttp2_library "${nghttp2_prefix}/lib/libnghttp2${CMAKE_SHARED_LIBRARY_SUFFIX}")
  set(curl_libssh2_library "${libssh2_prefix}/lib/libssh2${CMAKE_SHARED_LIBRARY_SUFFIX}")
  set(curl_zlib_library "${LOCKDC_ZLIB_PREFIX}/lib/libz${CMAKE_SHARED_LIBRARY_SUFFIX}")
  set(source_dir "${prefix_dir}/src")
  set(build_dir "${prefix_dir}/build")
  set(stamp_dir "${prefix_dir}/stamp")
  set(tmp_dir "${prefix_dir}/tmp")
  lc_append_common_external_cmake_args(common_cmake_args)
  file(MAKE_DIRECTORY "${install_dir}/include" "${install_dir}/lib")
  if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(curl_install_rpath "@loader_path")
    set(curl_platform_linker_flags "")
  elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(curl_install_rpath "$ORIGIN")
    set(curl_platform_linker_flags "-DCMAKE_SHARED_LINKER_FLAGS=-Wl,--enable-new-dtags")
  else()
    set(curl_install_rpath "")
    set(curl_platform_linker_flags "")
  endif()

  if(LOCKDC_BUILD_DEPENDENCIES)
    ExternalProject_Add(${project_name}
      URL "https://curl.se/download/curl-${LOCKDC_CURL_VERSION}.tar.xz"
      URL_HASH "SHA256=40df79166e74aa20149365e11ee4c798a46ad57c34e4f68fd13100e2c9a91946"
      DOWNLOAD_NAME "${curl_download_name}"
      PREFIX "${prefix_dir}"
      DOWNLOAD_DIR "${LOCKDC_DOWNLOAD_ROOT}"
      SOURCE_DIR "${source_dir}"
      BINARY_DIR "${build_dir}"
      STAMP_DIR "${stamp_dir}"
      TMP_DIR "${tmp_dir}"
      TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_TIMEOUT}
      INACTIVITY_TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_INACTIVITY_TIMEOUT}
      DEPENDS
        ${zlib_project}
        ${openssl_project}
        ${nghttp2_project}
        ${libssh2_project}
      CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${install_dir}
        -DCMAKE_INSTALL_LIBDIR=lib
        -DCMAKE_DEBUG_POSTFIX=
        -DCMAKE_BUILD_TYPE=${LOCKDC_DEPENDENCY_BUILD_TYPE}
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_INSTALL_RPATH=${curl_install_rpath}
        -DCMAKE_INSTALL_RPATH_USE_LINK_PATH=OFF
        -DCMAKE_BUILD_RPATH=
        -DCMAKE_SKIP_INSTALL_RPATH=OFF
        ${curl_platform_linker_flags}
        -DBUILD_SHARED_LIBS=ON
        -DBUILD_STATIC_LIBS=ON
        -DSHARE_LIB_OBJECT=ON
        -DBUILD_CURL_EXE=OFF
        -DBUILD_EXAMPLES=OFF
        -DBUILD_LIBCURL_DOCS=OFF
        -DBUILD_MISC_DOCS=OFF
        -DBUILD_TESTING=OFF
        -DCURL_DISABLE_INSTALL=OFF
        -DCURL_USE_PKGCONFIG=OFF
        -DCURL_USE_OPENSSL=ON
        -DCURL_USE_LIBSSH2=ON
        -DCURL_USE_LIBSSH=OFF
        -DUSE_NGHTTP2=ON
        -DCURL_DISABLE_LDAP=ON
        -DCURL_DISABLE_LDAPS=ON
        -DCURL_ZLIB=ON
        -DCURL_BROTLI=OFF
        -DCURL_ZSTD=OFF
        -DCURL_USE_LIBPSL=OFF
        -DUSE_LIBRTMP=OFF
        -DUSE_LIBIDN2=OFF
        -DZLIB_ROOT=${LOCKDC_ZLIB_PREFIX}
        -DZLIB_INCLUDE_DIR=${LOCKDC_ZLIB_PREFIX}/include
        -DZLIB_LIBRARY=${curl_zlib_library}
        -DOPENSSL_ROOT_DIR=${openssl_prefix}
        -DOPENSSL_INCLUDE_DIR=${openssl_prefix}/include
        -DOPENSSL_SSL_LIBRARY=${curl_openssl_ssl_library}
        -DOPENSSL_CRYPTO_LIBRARY=${curl_openssl_crypto_library}
        -DNGHTTP2_INCLUDE_DIR=${nghttp2_prefix}/include
        -DNGHTTP2_LIBRARY=${curl_nghttp2_library}
        -DLIBSSH2_INCLUDE_DIR=${libssh2_prefix}/include
        -DLIBSSH2_LIBRARY=${curl_libssh2_library}
        ${common_cmake_args}
      BUILD_COMMAND ${CMAKE_COMMAND} --build . --parallel ${LOCKDC_DEPENDENCY_BUILD_JOBS}
      INSTALL_COMMAND ${CMAKE_COMMAND} --install .
      BUILD_IN_SOURCE 0
      DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
  endif()

  add_library(lc::curl_static STATIC IMPORTED GLOBAL)
  set_target_properties(lc::curl_static
    PROPERTIES
      IMPORTED_LOCATION "${install_dir}/lib/libcurl${CMAKE_STATIC_LIBRARY_SUFFIX}"
      INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
      INTERFACE_LINK_LIBRARIES "lc::openssl_ssl_static;lc::openssl_crypto_static;lc::nghttp2_static;lc::libssh2_static;lc::zlib_static;${CMAKE_DL_LIBS};Threads::Threads"
  )
  if(LOCKDC_BUILD_DEPENDENCIES)
    add_dependencies(lc::curl_static ${project_name})
    lc_record_dependency_target(${project_name})
  else()
    lc_require_dependency_file("${install_dir}/lib/libcurl${CMAKE_STATIC_LIBRARY_SUFFIX}" "curl (static)")
  endif()

  add_library(lc::curl_shared SHARED IMPORTED GLOBAL)
  set_target_properties(lc::curl_shared
    PROPERTIES
      IMPORTED_LOCATION "${install_dir}/lib/libcurl${CMAKE_SHARED_LIBRARY_SUFFIX}"
      INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
      INTERFACE_LINK_LIBRARIES "lc::openssl_ssl_shared;lc::openssl_crypto_shared;lc::nghttp2_shared;lc::libssh2_shared;lc::zlib_shared;${CMAKE_DL_LIBS};Threads::Threads"
  )
  if(LOCKDC_BUILD_DEPENDENCIES)
    add_dependencies(lc::curl_shared ${project_name})
  else()
    lc_require_dependency_file("${install_dir}/lib/libcurl${CMAKE_SHARED_LIBRARY_SUFFIX}" "curl (shared)")
  endif()
endfunction()

function(lc_get_lonejson_header_info out_name out_hash)
  set(asset_name "lonejson-${LOCKDC_LONEJSON_VERSION}.h.gz")

  if(asset_name STREQUAL "lonejson-0.5.0.h.gz")
    set(asset_hash "b91427a66b72cf0c8a4f4d3bddef5f7fbebfdaf332f50be30b4fb282442ccceb")
  else()
    message(FATAL_ERROR "Unsupported lonejson header asset: ${asset_name}")
  endif()

  set(${out_name} "${asset_name}" PARENT_SCOPE)
  set(${out_hash} "${asset_hash}" PARENT_SCOPE)
endfunction()

function(lc_write_lonejson_license license_path)
  file(WRITE "${license_path}" [=[
MIT License

Copyright (c) 2026 Michel Blomgren <mike@pkt.systems>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is furnished
to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
]=])
endfunction()

function(lc_prepare_lonejson_source source_dir install_dir
         curl_include_dir download_dir asset_name gzip_bin)
  file(MAKE_DIRECTORY
    "${source_dir}"
    "${install_dir}/include"
    "${install_dir}/lib"
  )

  file(WRITE "${source_dir}/lonejson_impl.c" [=[
#include "lonejson.h"
]=])

  lc_write_lonejson_license("${source_dir}/LICENSE")

  file(WRITE "${source_dir}/CMakeLists.txt" "cmake_minimum_required(VERSION 3.21)\n"
    "\n"
    "project(lonejson_bundle VERSION ${LOCKDC_LONEJSON_VERSION} LANGUAGES C)\n"
    "\n"
    "add_library(lonejson_object OBJECT lonejson_impl.c)\n"
    "target_include_directories(lonejson_object PRIVATE \"${curl_include_dir}\" \"${source_dir}\")\n"
    "target_compile_definitions(lonejson_object PRIVATE\n"
    "  _POSIX_C_SOURCE=200809L\n"
    "  LONEJSON_IMPLEMENTATION=1\n"
    "  LONEJSON_WITH_CURL=1\n"
    "  LONEJSON_DISABLE_SHORT_NAMES=1\n"
    ")\n"
    "set_target_properties(lonejson_object PROPERTIES POSITION_INDEPENDENT_CODE ON)\n"
    "add_library(lonejson_shared SHARED $<TARGET_OBJECTS:lonejson_object>)\n"
    "add_library(lonejson_static STATIC $<TARGET_OBJECTS:lonejson_object>)\n"
    "set_target_properties(lonejson_shared\n"
    "  PROPERTIES\n"
    "    OUTPUT_NAME lonejson\n"
    "    POSITION_INDEPENDENT_CODE ON\n"
    "    VERSION ${LOCKDC_LONEJSON_VERSION}\n"
    "    SOVERSION 0\n"
    ")\n"
    "set_target_properties(lonejson_static\n"
    "  PROPERTIES\n"
    "    OUTPUT_NAME lonejson\n"
    "    POSITION_INDEPENDENT_CODE ON\n"
    ")\n"
    "\n"
    "if(CMAKE_C_COMPILER_ID MATCHES \"Clang|GNU\")\n"
    "  target_compile_options(lonejson_object PRIVATE\n"
    "    -std=c89\n"
    "    -Wall\n"
    "    -Wextra\n"
    "    -Wpedantic\n"
    "  )\n"
    "endif()\n"
    "\n"
    "install(FILES \"${source_dir}/lonejson.h\" DESTINATION include)\n"
    "install(TARGETS lonejson_shared lonejson_static\n"
    "  ARCHIVE DESTINATION lib\n"
    "  LIBRARY DESTINATION lib\n"
    ")\n"
    "install(FILES \"${source_dir}/LICENSE\"\n"
    "  DESTINATION share/doc/liblonejson\n"
    ")\n"
    "install(FILES \"${source_dir}/LICENSE\"\n"
    "  DESTINATION share/doc/liblockdc-third-party/lonejson\n"
    "  RENAME LICENSE.txt\n"
    ")\n")

  file(WRITE "${source_dir}/configure.cmake" "if(NOT EXISTS \"${download_dir}/${asset_name}\")\n"
    "  message(FATAL_ERROR \"missing downloaded lonejson header archive: ${download_dir}/${asset_name}\")\n"
    "endif()\n"
    "set(lockdc_lonejson_generator \"${CMAKE_GENERATOR}\")\n"
    "if(DEFINED LOCKDC_LONEJSON_GENERATOR AND NOT LOCKDC_LONEJSON_GENERATOR STREQUAL \"\")\n"
    "  set(lockdc_lonejson_generator \"\${LOCKDC_LONEJSON_GENERATOR}\")\n"
    "endif()\n"
    "execute_process(\n"
    "  COMMAND \"${gzip_bin}\" -dc \"${download_dir}/${asset_name}\"\n"
    "  OUTPUT_FILE \"${source_dir}/lonejson.h\"\n"
    "  RESULT_VARIABLE gzip_result\n"
    ")\n"
    "if(NOT gzip_result EQUAL 0)\n"
    "  message(FATAL_ERROR \"failed to decompress lonejson header archive: ${download_dir}/${asset_name}\")\n"
    "endif()\n"
    "if(EXISTS \"${build_dir}/CMakeCache.txt\")\n"
    "  file(STRINGS \"${build_dir}/CMakeCache.txt\" lonejson_cache_generator_line\n"
    "    REGEX \"^CMAKE_GENERATOR:INTERNAL=\")\n"
    "  if(lonejson_cache_generator_line)\n"
    "    string(REPLACE \"CMAKE_GENERATOR:INTERNAL=\" \"\" lonejson_cache_generator\n"
    "      \"\${lonejson_cache_generator_line}\")\n"
    "    if(NOT lonejson_cache_generator STREQUAL lockdc_lonejson_generator)\n"
    "      file(REMOVE_RECURSE \"${build_dir}\")\n"
    "    endif()\n"
    "  endif()\n"
    "endif()\n"
    "execute_process(\n"
    "  COMMAND \"${CMAKE_COMMAND}\" -S \"${source_dir}\" -B \"${build_dir}\"\n"
    "          -G \"\${lockdc_lonejson_generator}\"\n"
    "          \"-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}\"\n"
    "          \"-DCMAKE_AR=${CMAKE_AR}\"\n"
    "          \"-DCMAKE_RANLIB=${CMAKE_RANLIB}\"\n")
  if(CMAKE_TOOLCHAIN_FILE)
    file(APPEND "${source_dir}/configure.cmake"
      "          \"-DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}\"\n")
  endif()
  file(APPEND "${source_dir}/configure.cmake"
    "          \"-DCMAKE_BUILD_TYPE=${LOCKDC_DEPENDENCY_BUILD_TYPE}\"\n")
  file(APPEND "${source_dir}/configure.cmake"
    "          \"-DCMAKE_INSTALL_PREFIX=${install_dir}\"\n"
    "          -Wno-dev\n"
    "  RESULT_VARIABLE configure_result\n"
    ")\n"
    "if(NOT configure_result EQUAL 0)\n"
    "  message(FATAL_ERROR \"failed to configure lonejson source build\")\n"
    "endif()\n")
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
  lc_get_lonejson_header_info(asset_name asset_hash)
  if(LOCKDC_BUILD_DEPENDENCIES)
    find_program(LOCKDC_GZIP_BIN NAMES gzip REQUIRED)
  endif()
  set(curl_include_dir "${LOCKDC_EXTERNAL_ROOT}/curl/install/include")
  set(curl_project "lc_curl_project")

  if(LOCKDC_BUILD_DEPENDENCIES)
    lc_prepare_lonejson_source("${source_dir}" "${install_dir}" "${curl_include_dir}"
                               "${download_dir}" "${asset_name}"
                               "${LOCKDC_GZIP_BIN}")
    ExternalProject_Add(${project_name}
      URL "https://github.com/sa6mwa/lonejson/releases/download/v${LOCKDC_LONEJSON_VERSION}/${asset_name}"
      URL_HASH "SHA256=${asset_hash}"
      DOWNLOAD_NAME "${asset_name}"
      PREFIX "${prefix_dir}"
      DOWNLOAD_DIR "${download_dir}"
      SOURCE_DIR "${source_dir}"
      BINARY_DIR "${build_dir}"
      STAMP_DIR "${stamp_dir}"
      TMP_DIR "${tmp_dir}"
      TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_TIMEOUT}
      INACTIVITY_TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_INACTIVITY_TIMEOUT}
      DOWNLOAD_NO_EXTRACT TRUE
      DEPENDS ${curl_project}
      CONFIGURE_COMMAND
        ${CMAKE_COMMAND}
          "-DLOCKDC_LONEJSON_GENERATOR=${CMAKE_GENERATOR}"
          -P "${source_dir}/configure.cmake"
      BUILD_COMMAND ${CMAKE_COMMAND} --build "${build_dir}" --parallel ${LOCKDC_DEPENDENCY_BUILD_JOBS}
      INSTALL_COMMAND ${CMAKE_COMMAND} --install "${build_dir}"
      BUILD_IN_SOURCE 0
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
    set(lonejson_shared_library "${install_dir}/lib/liblonejson.0${CMAKE_SHARED_LIBRARY_SUFFIX}")
  else()
    set(lonejson_shared_library "${install_dir}/lib/liblonejson${CMAKE_SHARED_LIBRARY_SUFFIX}.0")
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

function(lc_get_pslog_asset_info out_name out_hash shared_flag)
  set(asset_name "libpslog-${LOCKDC_PSLOG_VERSION}-${LOCKDC_TARGET_ID}.tar.gz")

  if(asset_name STREQUAL "libpslog-0.3.1-x86_64-linux-gnu.tar.gz")
    set(asset_hash "4113d490dfd0c7706a518d32cef5d8acfc6d8bae8b42488edb76fd64e55dd14d")
  elseif(asset_name STREQUAL "libpslog-0.3.1-x86_64-linux-musl.tar.gz")
    set(asset_hash "f71f325de984677594da1bea589ab154a14b363ad4d3fa6125443a6a8fb0e448")
  elseif(asset_name STREQUAL "libpslog-0.3.1-aarch64-linux-gnu.tar.gz")
    set(asset_hash "f47808e9d87cb3368888c40329f8370e6ebd1a4782f2709da135441cdf4d8239")
  elseif(asset_name STREQUAL "libpslog-0.3.1-aarch64-linux-musl.tar.gz")
    set(asset_hash "fdfac09bf98dcc0bfdec3bcd55478d7b9b1446ff85d14dbad2fd9cdad166a3cb")
  elseif(asset_name STREQUAL "libpslog-0.3.1-armhf-linux-gnu.tar.gz")
    set(asset_hash "57b22d018220754e8d2824df15faebca123103d20921b8b1bd73187f866bdeff")
  elseif(asset_name STREQUAL "libpslog-0.3.1-armhf-linux-musl.tar.gz")
    set(asset_hash "a2cd382af639c8cf923e317f21604e20c866e6cc00ac9d30b20dbabb39d7c489")
  else()
    message(FATAL_ERROR "Unsupported libpslog asset: ${asset_name}")
  endif()

  set(${out_name} "${asset_name}" PARENT_SCOPE)
  set(${out_hash} "${asset_hash}" PARENT_SCOPE)
endfunction()

function(lc_get_pslog_header_info out_name out_hash)
  set(asset_name "pslog-${LOCKDC_PSLOG_VERSION}.h.gz")

  if(asset_name STREQUAL "pslog-0.3.1.h.gz")
    set(asset_hash "3fd34c48c7851692e7a851714c1af8b2fb880c2858b4c4c45c3424a45c77b094")
  else()
    message(FATAL_ERROR "Unsupported libpslog single-header asset: ${asset_name}")
  endif()

  set(${out_name} "${asset_name}" PARENT_SCOPE)
  set(${out_hash} "${asset_hash}" PARENT_SCOPE)
endfunction()

function(lc_write_pslog_license license_path)
  file(WRITE "${license_path}" [=[
MIT License

Copyright (c) 2026 Michel Blomgren mike@pkt.systems <https://pkt.systems>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is furnished
to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
]=])
endfunction()

function(lc_prepare_pslog_single_header_source source_dir install_dir
         download_dir asset_name gzip_bin)
  file(MAKE_DIRECTORY
    "${source_dir}"
    "${install_dir}/include"
    "${install_dir}/lib"
  )

  file(WRITE "${source_dir}/pslog_impl.c" [=[
#define PSLOG_IMPLEMENTATION
#include "pslog.h"
]=])
  file(WRITE "${source_dir}/pslog_version.h" [=[
#ifndef PSLOG_VERSION_H
#define PSLOG_VERSION_H
#include "pslog.h"
#endif
]=])
  lc_write_pslog_license("${source_dir}/LICENSE")

  file(WRITE "${source_dir}/CMakeLists.txt" "cmake_minimum_required(VERSION 3.21)\n"
    "\n"
    "project(pslog_bundle VERSION ${LOCKDC_PSLOG_VERSION} LANGUAGES C)\n"
    "\n"
    "find_package(Threads REQUIRED)\n"
    "add_library(pslog_object OBJECT pslog_impl.c)\n"
    "target_include_directories(pslog_object PRIVATE \"${source_dir}\")\n"
    "target_compile_definitions(pslog_object PRIVATE _POSIX_C_SOURCE=200809L)\n"
    "set_target_properties(pslog_object PROPERTIES POSITION_INDEPENDENT_CODE ON)\n"
    "add_library(pslog_shared SHARED $<TARGET_OBJECTS:pslog_object>)\n"
    "add_library(pslog_static STATIC $<TARGET_OBJECTS:pslog_object>)\n"
    "target_link_libraries(pslog_shared PRIVATE Threads::Threads)\n"
    "set_target_properties(pslog_shared\n"
    "  PROPERTIES\n"
    "    OUTPUT_NAME pslog\n"
    "    POSITION_INDEPENDENT_CODE ON\n"
    "    VERSION ${LOCKDC_PSLOG_VERSION}\n"
    "    SOVERSION 0\n"
    ")\n"
    "set_target_properties(pslog_static\n"
    "  PROPERTIES\n"
    "    OUTPUT_NAME pslog\n"
    "    POSITION_INDEPENDENT_CODE ON\n"
    ")\n"
    "if(CMAKE_C_COMPILER_ID MATCHES \"Clang|GNU\")\n"
    "  target_compile_options(pslog_object PRIVATE -std=c89 -Wall -Wextra -Wpedantic)\n"
    "endif()\n"
    "install(FILES \"${source_dir}/pslog.h\" \"${source_dir}/pslog_version.h\" DESTINATION include)\n"
    "install(TARGETS pslog_shared pslog_static ARCHIVE DESTINATION lib LIBRARY DESTINATION lib)\n"
    "install(FILES \"${source_dir}/LICENSE\" DESTINATION share/doc/libpslog)\n"
    "install(FILES \"${source_dir}/LICENSE\" DESTINATION share/doc/liblockdc-third-party/libpslog RENAME LICENSE.txt)\n")

  file(WRITE "${source_dir}/configure.cmake" "if(NOT EXISTS \"${download_dir}/${asset_name}\")\n"
    "  message(FATAL_ERROR \"missing downloaded pslog header archive: ${download_dir}/${asset_name}\")\n"
    "endif()\n"
    "set(lockdc_pslog_generator \"${CMAKE_GENERATOR}\")\n"
    "if(DEFINED LOCKDC_PSLOG_GENERATOR AND NOT LOCKDC_PSLOG_GENERATOR STREQUAL \"\")\n"
    "  set(lockdc_pslog_generator \"\${LOCKDC_PSLOG_GENERATOR}\")\n"
    "endif()\n"
    "execute_process(\n"
    "  COMMAND \"${gzip_bin}\" -dc \"${download_dir}/${asset_name}\"\n"
    "  OUTPUT_FILE \"${source_dir}/pslog.h\"\n"
    "  RESULT_VARIABLE gzip_result\n"
    ")\n"
    "if(NOT gzip_result EQUAL 0)\n"
    "  message(FATAL_ERROR \"failed to decompress pslog header archive: ${download_dir}/${asset_name}\")\n"
    "endif()\n"
    "if(EXISTS \"${build_dir}/CMakeCache.txt\")\n"
    "  file(STRINGS \"${build_dir}/CMakeCache.txt\" pslog_cache_generator_line REGEX \"^CMAKE_GENERATOR:INTERNAL=\")\n"
    "  if(pslog_cache_generator_line)\n"
    "    string(REPLACE \"CMAKE_GENERATOR:INTERNAL=\" \"\" pslog_cache_generator \"\${pslog_cache_generator_line}\")\n"
    "    if(NOT pslog_cache_generator STREQUAL lockdc_pslog_generator)\n"
    "      file(REMOVE_RECURSE \"${build_dir}\")\n"
    "    endif()\n"
    "  endif()\n"
    "endif()\n"
    "execute_process(\n"
    "  COMMAND \"${CMAKE_COMMAND}\" -S \"${source_dir}\" -B \"${build_dir}\"\n"
    "          -G \"\${lockdc_pslog_generator}\"\n"
    "          \"-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}\"\n"
    "          \"-DCMAKE_AR=${CMAKE_AR}\"\n"
    "          \"-DCMAKE_RANLIB=${CMAKE_RANLIB}\"\n")
  if(CMAKE_TOOLCHAIN_FILE)
    file(APPEND "${source_dir}/configure.cmake"
      "          \"-DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}\"\n")
  endif()
  file(APPEND "${source_dir}/configure.cmake"
    "          \"-DCMAKE_BUILD_TYPE=${LOCKDC_DEPENDENCY_BUILD_TYPE}\"\n"
    "          \"-DCMAKE_INSTALL_PREFIX=${install_dir}\"\n"
    "          -Wno-dev\n"
    "  RESULT_VARIABLE configure_result\n"
    ")\n"
    "if(NOT configure_result EQUAL 0)\n"
    "  message(FATAL_ERROR \"failed to configure pslog source build\")\n"
    "endif()\n")
endfunction()

function(lc_add_pslog)
  set(project_name "lc_pslog_project")
  set(prefix_dir "${LOCKDC_DEPENDENCY_BUILD_ROOT}/pslog")
  set(source_dir "${prefix_dir}/src")
  set(build_dir "${prefix_dir}/build")
  set(install_dir "${LOCKDC_EXTERNAL_ROOT}/pslog/install")
  set(stamp_dir "${prefix_dir}/stamp")
  set(tmp_dir "${prefix_dir}/tmp")
  set(download_dir "${LOCKDC_DOWNLOAD_ROOT}")
  string(TOLOWER "${LOCKDC_TARGET_OS}" _lockdc_target_os_lower)
  if(_lockdc_target_os_lower STREQUAL "darwin")
    lc_get_pslog_header_info(asset_name asset_hash)
    if(LOCKDC_BUILD_DEPENDENCIES)
      find_program(LOCKDC_GZIP_BIN NAMES gzip REQUIRED)
    endif()
  else()
    lc_get_pslog_asset_info(asset_name asset_hash TRUE)
  endif()

  file(MAKE_DIRECTORY "${install_dir}/include" "${install_dir}/lib")

  if(LOCKDC_BUILD_DEPENDENCIES AND _lockdc_target_os_lower STREQUAL "darwin")
    lc_prepare_pslog_single_header_source("${source_dir}" "${install_dir}"
                                          "${download_dir}" "${asset_name}"
                                          "${LOCKDC_GZIP_BIN}")
    ExternalProject_Add(${project_name}
      URL "https://github.com/sa6mwa/libpslog/releases/download/v${LOCKDC_PSLOG_VERSION}/${asset_name}"
      URL_HASH "SHA256=${asset_hash}"
      DOWNLOAD_NAME "${asset_name}"
      PREFIX "${prefix_dir}"
      DOWNLOAD_DIR "${download_dir}"
      SOURCE_DIR "${source_dir}"
      BINARY_DIR "${build_dir}"
      STAMP_DIR "${stamp_dir}"
      TMP_DIR "${tmp_dir}"
      TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_TIMEOUT}
      INACTIVITY_TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_INACTIVITY_TIMEOUT}
      DOWNLOAD_NO_EXTRACT TRUE
      CONFIGURE_COMMAND
        ${CMAKE_COMMAND}
          "-DLOCKDC_PSLOG_GENERATOR=${CMAKE_GENERATOR}"
          -P "${source_dir}/configure.cmake"
      BUILD_COMMAND ${CMAKE_COMMAND} --build "${build_dir}" --parallel ${LOCKDC_DEPENDENCY_BUILD_JOBS}
      INSTALL_COMMAND ${CMAKE_COMMAND} --install "${build_dir}"
      BUILD_IN_SOURCE 0
    )
  elseif(LOCKDC_BUILD_DEPENDENCIES)
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

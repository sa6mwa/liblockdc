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
  if(LOCKDC_TARGET_ARCH STREQUAL "x86_64")
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
  if(LOCKDC_TARGET_ARCH STREQUAL "x86_64")
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

function(lc_add_openssl variant shared_flag)
  set(project_name "lc_openssl_${variant}_project")
  set(prefix_dir "${LOCKDC_DEPENDENCY_BUILD_ROOT}/openssl-${variant}")
  set(source_dir "${prefix_dir}/src")
  set(build_dir "${prefix_dir}/build")
  set(install_dir "${LOCKDC_EXTERNAL_ROOT}/openssl-${variant}/install")
  set(stamp_dir "${prefix_dir}/stamp")
  set(tmp_dir "${prefix_dir}/tmp")
  lc_get_openssl_config_target(openssl_config_target)
  set(config_args ${openssl_config_target} no-tests no-docs no-module no-apps no-makedepend)
  if(LOCKDC_TARGET_LIBC STREQUAL "musl")
    list(APPEND config_args no-secure-memory no-afalgeng)
  endif()
  if(shared_flag)
    list(APPEND config_args shared "-Wl,--enable-new-dtags,-rpath,\\$$ORIGIN")
    set(openssl_make_rpath "")
  else()
    list(APPEND config_args no-shared)
    set(openssl_make_rpath "")
  endif()

  lc_normalize_prefix(env_prefix "${install_dir}")
  file(MAKE_DIRECTORY "${install_dir}/include" "${install_dir}/lib")
  if(shared_flag)
    set(build_command make -j1 "${openssl_make_rpath}")
    set(install_command make -j1 "${openssl_make_rpath}" install_sw)
  else()
    set(build_command make -j1)
    set(install_command make -j1 install_sw)
  endif()

  if(LOCKDC_BUILD_DEPENDENCIES)
    ExternalProject_Add(${project_name}
      URL "https://github.com/openssl/openssl/releases/download/openssl-${LOCKDC_OPENSSL_VERSION}/openssl-${LOCKDC_OPENSSL_VERSION}.tar.gz"
      URL_HASH "SHA256=b1bfedcd5b289ff22aee87c9d600f515767ebf45f77168cb6d64f231f518a82e"
      DOWNLOAD_NAME "openssl-${LOCKDC_OPENSSL_VERSION}-${variant}.tar.gz"
      PREFIX "${prefix_dir}"
      DOWNLOAD_DIR "${LOCKDC_EXTERNAL_ROOT}/downloads"
      SOURCE_DIR "${source_dir}"
      STAMP_DIR "${stamp_dir}"
      TMP_DIR "${tmp_dir}"
      TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_TIMEOUT}
      INACTIVITY_TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_INACTIVITY_TIMEOUT}
      CONFIGURE_COMMAND
        ${CMAKE_COMMAND} -E env
          CC=${CMAKE_C_COMPILER}
          AR=${CMAKE_AR}
          RANLIB=${CMAKE_RANLIB}
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

  if(shared_flag)
    set(crypto_type SHARED)
    set(ssl_type SHARED)
    set(crypto_suffix "${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(ssl_suffix "${CMAKE_SHARED_LIBRARY_SUFFIX}")
  else()
    set(crypto_type STATIC)
    set(ssl_type STATIC)
    set(crypto_suffix "${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(ssl_suffix "${CMAKE_STATIC_LIBRARY_SUFFIX}")
  endif()
  file(MAKE_DIRECTORY "${install_dir}/include" "${install_dir}/lib")

  add_library(lc::openssl_crypto_${variant} ${crypto_type} IMPORTED GLOBAL)
  set_target_properties(lc::openssl_crypto_${variant}
    PROPERTIES
      IMPORTED_LOCATION "${install_dir}/lib/libcrypto${crypto_suffix}"
      INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
  )
  if(LOCKDC_BUILD_DEPENDENCIES)
    add_dependencies(lc::openssl_crypto_${variant} ${project_name})
    lc_record_dependency_target(${project_name})
  else()
    lc_require_dependency_file("${install_dir}/lib/libcrypto${crypto_suffix}" "OpenSSL crypto (${variant})")
  endif()

  add_library(lc::openssl_ssl_${variant} ${ssl_type} IMPORTED GLOBAL)
  set_target_properties(lc::openssl_ssl_${variant}
    PROPERTIES
      IMPORTED_LOCATION "${install_dir}/lib/libssl${ssl_suffix}"
      INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
      INTERFACE_LINK_LIBRARIES "lc::openssl_crypto_${variant};${CMAKE_DL_LIBS};Threads::Threads"
  )
  if(LOCKDC_BUILD_DEPENDENCIES)
    add_dependencies(lc::openssl_ssl_${variant} ${project_name})
  else()
    lc_require_dependency_file("${install_dir}/lib/libssl${ssl_suffix}" "OpenSSL ssl (${variant})")
  endif()

  set(LOCKDC_OPENSSL_${variant}_PREFIX "${install_dir}" PARENT_SCOPE)
endfunction()

function(lc_add_nghttp2 variant shared_flag)
  set(project_name "lc_nghttp2_${variant}_project")
  set(prefix_dir "${LOCKDC_DEPENDENCY_BUILD_ROOT}/nghttp2-${variant}")
  set(source_dir "${prefix_dir}/src")
  set(build_dir "${prefix_dir}/build")
  set(install_dir "${LOCKDC_EXTERNAL_ROOT}/nghttp2-${variant}/install")
  set(stamp_dir "${prefix_dir}/stamp")
  set(tmp_dir "${prefix_dir}/tmp")
  if(shared_flag)
    set(enable_shared --enable-shared)
    set(enable_static --disable-static)
    set(lib_type SHARED)
    set(lib_suffix "${CMAKE_SHARED_LIBRARY_SUFFIX}")
  else()
    set(enable_shared --disable-shared)
    set(enable_static --enable-static)
    set(lib_type STATIC)
    set(lib_suffix "${CMAKE_STATIC_LIBRARY_SUFFIX}")
  endif()
  lc_get_target_triple(autotools_host)
  file(MAKE_DIRECTORY "${install_dir}/include" "${install_dir}/lib")

  if(LOCKDC_BUILD_DEPENDENCIES)
    ExternalProject_Add(${project_name}
      URL "https://github.com/nghttp2/nghttp2/releases/download/v${LOCKDC_NGHTTP2_VERSION}/nghttp2-${LOCKDC_NGHTTP2_VERSION}.tar.gz"
      URL_HASH "SHA256=2c16ffc588ad3f9e2613c3fad72db48ecb5ce15bc362fcc85b342e48daf51013"
      DOWNLOAD_NAME "nghttp2-${LOCKDC_NGHTTP2_VERSION}-${variant}.tar.gz"
      PREFIX "${prefix_dir}"
      DOWNLOAD_DIR "${LOCKDC_EXTERNAL_ROOT}/downloads"
      SOURCE_DIR "${source_dir}"
      BINARY_DIR "${build_dir}"
      STAMP_DIR "${stamp_dir}"
      TMP_DIR "${tmp_dir}"
      TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_TIMEOUT}
      INACTIVITY_TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_INACTIVITY_TIMEOUT}
      CONFIGURE_COMMAND
        ${CMAKE_COMMAND} -E env
        CC=${CMAKE_C_COMPILER}
        AR=${CMAKE_AR}
        RANLIB=${CMAKE_RANLIB}
        "${source_dir}/configure"
        --prefix=${install_dir}
        --host=${autotools_host}
        ${enable_shared}
        ${enable_static}
        --enable-lib-only
      BUILD_COMMAND make
      INSTALL_COMMAND make install
      BUILD_IN_SOURCE 0
      DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
  endif()

  add_library(lc::nghttp2_${variant} ${lib_type} IMPORTED GLOBAL)
  set_target_properties(lc::nghttp2_${variant}
    PROPERTIES
      IMPORTED_LOCATION "${install_dir}/lib/libnghttp2${lib_suffix}"
      INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
  )
  if(LOCKDC_BUILD_DEPENDENCIES)
    add_dependencies(lc::nghttp2_${variant} ${project_name})
    lc_record_dependency_target(${project_name})
  else()
    lc_require_dependency_file("${install_dir}/lib/libnghttp2${lib_suffix}" "nghttp2 (${variant})")
  endif()

  set(LOCKDC_NGHTTP2_${variant}_PREFIX "${install_dir}" PARENT_SCOPE)
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

  set(zlib_shared_library "${install_dir}/lib/libz${CMAKE_SHARED_LIBRARY_SUFFIX}.${LOCKDC_ZLIB_VERSION}")
  set(zlib_shared_soname "${install_dir}/lib/libz${CMAKE_SHARED_LIBRARY_SUFFIX}.1")
  set(zlib_shared_link "${install_dir}/lib/libz${CMAKE_SHARED_LIBRARY_SUFFIX}")
  set(zlib_static_library "${install_dir}/lib/libz${CMAKE_STATIC_LIBRARY_SUFFIX}")

  if(LOCKDC_BUILD_DEPENDENCIES)
    ExternalProject_Add(${project_name}
      URL
        "https://www.zlib.net/zlib-${LOCKDC_ZLIB_VERSION}.tar.gz"
        "https://zlib.net/fossils/zlib-${LOCKDC_ZLIB_VERSION}.tar.gz"
      URL_HASH "SHA256=bb329a0a2cd0274d05519d61c667c062e06990d72e125ee2dfa8de64f0119d16"
      DOWNLOAD_NAME "zlib-${LOCKDC_ZLIB_VERSION}.tar.gz"
      PREFIX "${prefix_dir}"
      DOWNLOAD_DIR "${LOCKDC_EXTERNAL_ROOT}/downloads"
      SOURCE_DIR "${source_dir}"
      BINARY_DIR "${build_dir}"
      STAMP_DIR "${stamp_dir}"
      TMP_DIR "${tmp_dir}"
      TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_TIMEOUT}
      INACTIVITY_TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_INACTIVITY_TIMEOUT}
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
      BUILD_COMMAND ${CMAKE_COMMAND} --build .
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
  set(libssh2_static_library "${install_dir}/lib/libssh2${CMAKE_STATIC_LIBRARY_SUFFIX}")
  if(NOT DEFINED LOCKDC_ZLIB_PREFIX OR "${LOCKDC_ZLIB_PREFIX}" STREQUAL "")
    message(FATAL_ERROR "libssh2 requires zlib to be configured first")
  endif()
  if(DEFINED LOCKDC_OPENSSL_shared_PREFIX AND NOT "${LOCKDC_OPENSSL_shared_PREFIX}" STREQUAL "")
    set(libssh2_openssl_prefix "${LOCKDC_OPENSSL_shared_PREFIX}")
    set(libssh2_openssl_build_variant "shared")
    set(openssl_project "lc_openssl_shared_project")
    set(libssh2_openssl_ssl_library "${libssh2_openssl_prefix}/lib/libssl${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(libssh2_openssl_crypto_library "${libssh2_openssl_prefix}/lib/libcrypto${CMAKE_SHARED_LIBRARY_SUFFIX}")
  elseif(DEFINED LOCKDC_OPENSSL_static_PREFIX AND NOT "${LOCKDC_OPENSSL_static_PREFIX}" STREQUAL "")
    set(libssh2_openssl_prefix "${LOCKDC_OPENSSL_static_PREFIX}")
    set(libssh2_openssl_build_variant "static")
    set(openssl_project "lc_openssl_static_project")
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
      DOWNLOAD_DIR "${LOCKDC_EXTERNAL_ROOT}/downloads"
      SOURCE_DIR "${source_dir}"
      BINARY_DIR "${build_dir}"
      STAMP_DIR "${stamp_dir}"
      TMP_DIR "${tmp_dir}"
      TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_TIMEOUT}
      INACTIVITY_TIMEOUT ${LOCKDC_DEPENDENCY_DOWNLOAD_INACTIVITY_TIMEOUT}
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
        -DZLIB_LIBRARIES=${LOCKDC_ZLIB_PREFIX}/lib/libz${CMAKE_SHARED_LIBRARY_SUFFIX}.${LOCKDC_ZLIB_VERSION}
        ${common_cmake_args}
      DEPENDS
        ${openssl_project}
        lc_zlib_project
      BUILD_COMMAND ${CMAKE_COMMAND} --build .
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

function(lc_add_curl variant shared_flag)
  set(project_name "lc_curl_${variant}_project")
  set(openssl_project "lc_openssl_${variant}_project")
  set(nghttp2_project "lc_nghttp2_${variant}_project")
  set(libssh2_project "lc_libssh2_project")
  set(zlib_project "lc_zlib_project")

  if(shared_flag)
    set(prefix_dir "${LOCKDC_DEPENDENCY_BUILD_ROOT}/curl-shared-cmake")
    set(install_dir "${LOCKDC_EXTERNAL_ROOT}/curl-shared-cmake/install")
    set(lib_type SHARED)
    set(lib_suffix "${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(openssl_prefix "${LOCKDC_OPENSSL_shared_PREFIX}")
    set(nghttp2_prefix "${LOCKDC_NGHTTP2_shared_PREFIX}")
    set(libssh2_prefix "${LOCKDC_LIBSSH2_PREFIX}")
    set(curl_download_name "curl-${LOCKDC_CURL_VERSION}-${variant}-cmake.tar.xz")
    set(curl_openssl_ssl_library "${openssl_prefix}/lib/libssl${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(curl_openssl_crypto_library "${openssl_prefix}/lib/libcrypto${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(curl_nghttp2_library "${nghttp2_prefix}/lib/libnghttp2${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(curl_libssh2_library "${libssh2_prefix}/lib/libssh2${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(curl_zlib_library "${LOCKDC_ZLIB_PREFIX}/lib/libz${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(curl_build_shared_libs ON)
    set(curl_build_static_libs OFF)
    set(curl_install_rpath "$ORIGIN")
    set(curl_shared_linker_flags "-Wl,--enable-new-dtags")
  else()
    set(prefix_dir "${LOCKDC_DEPENDENCY_BUILD_ROOT}/curl-static")
    set(install_dir "${LOCKDC_EXTERNAL_ROOT}/curl-static/install")
    set(lib_type STATIC)
    set(lib_suffix "${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(openssl_prefix "${LOCKDC_OPENSSL_static_PREFIX}")
    set(nghttp2_prefix "${LOCKDC_NGHTTP2_static_PREFIX}")
    set(libssh2_prefix "${LOCKDC_LIBSSH2_PREFIX}")
    set(curl_download_name "curl-${LOCKDC_CURL_VERSION}-${variant}-cmake.tar.xz")
    set(curl_openssl_ssl_library "${openssl_prefix}/lib/libssl${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(curl_openssl_crypto_library "${openssl_prefix}/lib/libcrypto${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(curl_nghttp2_library "${nghttp2_prefix}/lib/libnghttp2${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(curl_libssh2_library "${libssh2_prefix}/lib/libssh2${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(curl_zlib_library "${LOCKDC_ZLIB_PREFIX}/lib/libz${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(curl_build_shared_libs OFF)
    set(curl_build_static_libs ON)
    set(curl_install_rpath "")
    set(curl_shared_linker_flags "")
  endif()
  set(source_dir "${prefix_dir}/src")
  set(build_dir "${prefix_dir}/build")
  set(stamp_dir "${prefix_dir}/stamp")
  set(tmp_dir "${prefix_dir}/tmp")
  lc_append_common_external_cmake_args(common_cmake_args)
  file(MAKE_DIRECTORY "${install_dir}/include" "${install_dir}/lib")

  if(LOCKDC_BUILD_DEPENDENCIES)
    ExternalProject_Add(${project_name}
      URL "https://curl.se/download/curl-${LOCKDC_CURL_VERSION}.tar.xz"
      URL_HASH "SHA256=40df79166e74aa20149365e11ee4c798a46ad57c34e4f68fd13100e2c9a91946"
      DOWNLOAD_NAME "${curl_download_name}"
      PREFIX "${prefix_dir}"
      DOWNLOAD_DIR "${LOCKDC_EXTERNAL_ROOT}/downloads"
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
        -DCMAKE_SHARED_LINKER_FLAGS=${curl_shared_linker_flags}
        -DBUILD_SHARED_LIBS=${curl_build_shared_libs}
        -DBUILD_STATIC_LIBS=${curl_build_static_libs}
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
      BUILD_COMMAND ${CMAKE_COMMAND} --build .
      INSTALL_COMMAND ${CMAKE_COMMAND} --install .
      BUILD_IN_SOURCE 0
      DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
  endif()

  add_library(lc::curl_${variant} ${lib_type} IMPORTED GLOBAL)
  set_target_properties(lc::curl_${variant}
    PROPERTIES
      IMPORTED_LOCATION "${install_dir}/lib/libcurl${lib_suffix}"
      INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
      INTERFACE_LINK_LIBRARIES "lc::openssl_ssl_${variant};lc::openssl_crypto_${variant};lc::nghttp2_${variant};lc::libssh2_${variant};lc::zlib_${variant};${CMAKE_DL_LIBS};Threads::Threads"
  )
  if(LOCKDC_BUILD_DEPENDENCIES)
    add_dependencies(lc::curl_${variant} ${project_name})
    lc_record_dependency_target(${project_name})
  else()
    lc_require_dependency_file("${install_dir}/lib/libcurl${lib_suffix}" "curl (${variant})")
  endif()
endfunction()

function(lc_get_lonejson_header_info out_name out_hash)
  set(asset_name "lonejson-${LOCKDC_LONEJSON_VERSION}.h.gz")

  if(asset_name STREQUAL "lonejson-0.4.1.h.gz")
    set(asset_hash "e96d160e54e9132b62cebeb349358b1b2ccf4ba00283ee3c96dab1a8512d3d45")
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

function(lc_prepare_lonejson_source variant shared_flag source_dir install_dir
         curl_include_dir download_dir asset_name gzip_bin)
  if(shared_flag)
    set(lib_type SHARED)
  else()
    set(lib_type STATIC)
  endif()

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
    "add_library(lonejson ${lib_type} lonejson_impl.c)\n"
    "target_include_directories(lonejson PRIVATE \"${curl_include_dir}\" \"${source_dir}\")\n"
    "target_compile_definitions(lonejson PRIVATE\n"
    "  _POSIX_C_SOURCE=200809L\n"
    "  LONEJSON_IMPLEMENTATION=1\n"
    "  LONEJSON_WITH_CURL=1\n"
    "  LONEJSON_DISABLE_SHORT_NAMES=1\n"
    ")\n"
    "set_target_properties(lonejson\n"
    "  PROPERTIES\n"
    "    OUTPUT_NAME lonejson\n"
    "    POSITION_INDEPENDENT_CODE ON\n"
    "    VERSION ${LOCKDC_LONEJSON_VERSION}\n"
    "    SOVERSION 0\n"
    ")\n"
    "\n"
    "if(CMAKE_C_COMPILER_ID MATCHES \"Clang|GNU\")\n"
    "  target_compile_options(lonejson PRIVATE\n"
    "    -std=c89\n"
    "    -Wall\n"
    "    -Wextra\n"
    "    -Wpedantic\n"
    "  )\n"
    "endif()\n"
    "\n"
    "install(FILES \"${source_dir}/lonejson.h\" DESTINATION include)\n"
    "install(TARGETS lonejson\n"
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
    "execute_process(\n"
    "  COMMAND \"${gzip_bin}\" -dc \"${download_dir}/${asset_name}\"\n"
    "  OUTPUT_FILE \"${source_dir}/lonejson.h\"\n"
    "  RESULT_VARIABLE gzip_result\n"
    ")\n"
    "if(NOT gzip_result EQUAL 0)\n"
    "  message(FATAL_ERROR \"failed to decompress lonejson header archive: ${download_dir}/${asset_name}\")\n"
    "endif()\n"
    "execute_process(\n"
    "  COMMAND \"${CMAKE_COMMAND}\" -S \"${source_dir}\" -B \"${build_dir}\"\n"
    "          -G \"${CMAKE_GENERATOR}\"\n"
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

function(lc_add_lonejson variant shared_flag)
  set(project_name "lc_lonejson_${variant}_project")
  set(prefix_dir "${LOCKDC_DEPENDENCY_BUILD_ROOT}/lonejson-${variant}")
  set(source_dir "${prefix_dir}/src")
  set(build_dir "${prefix_dir}/build")
  set(install_dir "${LOCKDC_EXTERNAL_ROOT}/lonejson-${variant}/install")
  set(stamp_dir "${prefix_dir}/stamp")
  set(tmp_dir "${prefix_dir}/tmp")
  set(download_dir "${LOCKDC_EXTERNAL_ROOT}/downloads")
  lc_get_lonejson_header_info(asset_name asset_hash)
  if(LOCKDC_BUILD_DEPENDENCIES)
    find_program(LOCKDC_GZIP_BIN NAMES gzip REQUIRED)
  endif()

  if(shared_flag)
    set(lib_type SHARED)
    set(imported_location "${install_dir}/lib/liblonejson${CMAKE_SHARED_LIBRARY_SUFFIX}.0")
    set(curl_include_dir "${LOCKDC_EXTERNAL_ROOT}/curl-shared-cmake/install/include")
    set(curl_project "lc_curl_shared_project")
  else()
    set(lib_type STATIC)
    set(imported_location "${install_dir}/lib/liblonejson${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(curl_include_dir "${LOCKDC_EXTERNAL_ROOT}/curl-static/install/include")
    set(curl_project "lc_curl_static_project")
  endif()

  if(LOCKDC_BUILD_DEPENDENCIES)
    lc_prepare_lonejson_source("${variant}" "${shared_flag}" "${source_dir}"
                               "${install_dir}" "${curl_include_dir}"
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
        ${CMAKE_COMMAND} -P "${source_dir}/configure.cmake"
      BUILD_COMMAND ${CMAKE_COMMAND} --build "${build_dir}"
      INSTALL_COMMAND ${CMAKE_COMMAND} --install "${build_dir}"
      BUILD_IN_SOURCE 0
    )
  endif()

  add_library(lc::lonejson_${variant} ${lib_type} IMPORTED GLOBAL)
  set_target_properties(lc::lonejson_${variant}
    PROPERTIES
      IMPORTED_LOCATION "${imported_location}"
      INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
  )
  if(LOCKDC_BUILD_DEPENDENCIES)
    add_dependencies(lc::lonejson_${variant} ${project_name})
    lc_record_dependency_target(${project_name})
  else()
    lc_require_dependency_file("${imported_location}" "lonejson (${variant})")
    lc_require_dependency_file("${install_dir}/include/lonejson.h" "lonejson header (${variant})")
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
      DOWNLOAD_DIR "${LOCKDC_EXTERNAL_ROOT}/downloads"
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
        -DPICKY_DEVELOPER=OFF
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        ${common_cmake_args}
      BUILD_COMMAND ${CMAKE_COMMAND} --build .
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

function(lc_add_pslog variant shared_flag)
  set(project_name "lc_pslog_${variant}_project")
  set(prefix_dir "${LOCKDC_DEPENDENCY_BUILD_ROOT}/pslog-${variant}")
  set(source_dir "${prefix_dir}/src")
  set(install_dir "${LOCKDC_EXTERNAL_ROOT}/pslog-${variant}/install")
  set(stamp_dir "${prefix_dir}/stamp")
  set(tmp_dir "${prefix_dir}/tmp")

  lc_get_pslog_asset_info(asset_name asset_hash ${shared_flag})

  if(shared_flag)
    set(lib_type SHARED)
    set(imported_location "${install_dir}/lib/libpslog${CMAKE_SHARED_LIBRARY_SUFFIX}.0")
  else()
    set(lib_type STATIC)
    set(imported_location "${install_dir}/lib/libpslog${CMAKE_STATIC_LIBRARY_SUFFIX}")
  endif()

  file(MAKE_DIRECTORY "${install_dir}/include" "${install_dir}/lib")

  if(LOCKDC_BUILD_DEPENDENCIES)
    ExternalProject_Add(${project_name}
      URL "https://github.com/sa6mwa/libpslog/releases/download/v${LOCKDC_PSLOG_VERSION}/${asset_name}"
      URL_HASH "SHA256=${asset_hash}"
      # Keep static/shared downloads isolated even though they come from the same
      # upstream asset. Sharing one local download path lets one ExternalProject
      # observe a partially written or replaced file from the other.
      DOWNLOAD_NAME "${variant}-${asset_name}"
      PREFIX "${prefix_dir}"
      DOWNLOAD_DIR "${LOCKDC_EXTERNAL_ROOT}/downloads"
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

  add_library(lc::pslog_${variant} ${lib_type} IMPORTED GLOBAL)
  set_target_properties(lc::pslog_${variant}
    PROPERTIES
      IMPORTED_LOCATION "${imported_location}"
      INTERFACE_INCLUDE_DIRECTORIES "${install_dir}/include"
  )
  if(LOCKDC_BUILD_DEPENDENCIES)
    add_dependencies(lc::pslog_${variant} ${project_name})
    lc_record_dependency_target(${project_name})
  else()
    lc_require_dependency_file("${imported_location}" "libpslog (${variant})")
    lc_require_dependency_file("${install_dir}/include/pslog.h" "libpslog header (${variant})")
  endif()
endfunction()

function(lc_configure_dependencies)
  if(LOCKDC_BUILD_STATIC)
    lc_add_openssl(static FALSE)
  endif()

  if(LOCKDC_BUILD_SHARED)
    lc_add_openssl(shared TRUE)
  endif()

  lc_add_zlib()
  lc_add_libssh2()

  if(LOCKDC_BUILD_STATIC)
    lc_add_pslog(static FALSE)
    lc_add_nghttp2(static FALSE)
    lc_add_curl(static FALSE)
    lc_add_lonejson(static FALSE)
  endif()

  if(LOCKDC_BUILD_SHARED)
    lc_add_pslog(shared TRUE)
    lc_add_nghttp2(shared TRUE)
    lc_add_curl(shared TRUE)
    lc_add_lonejson(shared TRUE)
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

#!/usr/bin/env bash
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
preset=${1:-deps-x86_64-linux-gnu}

unset LD_LIBRARY_PATH
dry_run=${LOCKDC_DEPS_DRY_RUN:-0}
download_timeout=${LOCKDC_DEPENDENCY_DOWNLOAD_TIMEOUT:-300}
lonejson_abi_version=${LOCKDC_LONEJSON_ABI_VERSION:-19}

resolve_host_debug_preset() {
  local compiler triple

  compiler=${CC:-cc}
  triple=$("$compiler" -dumpmachine 2>/dev/null || true)

  case "$triple" in
    x86_64*-linux-musl*)
      printf '%s\n' "deps-x86_64-linux-musl"
      ;;
    x86_64*-linux-gnu*|x86_64*-linux)
      printf '%s\n' "deps-x86_64-linux-gnu"
      ;;
    aarch64*-linux-musl*)
      printf '%s\n' "deps-aarch64-linux-musl"
      ;;
    aarch64*-linux-gnu*|aarch64*-linux)
      printf '%s\n' "deps-aarch64-linux-gnu"
      ;;
    arm*-linux-musleabihf*|armv7*-linux-musleabihf*|arm*-linux-musl*|armv7*-linux-musl*)
      printf '%s\n' "deps-armhf-linux-musl"
      ;;
    arm*-linux-gnueabihf*|armv7*-linux-gnueabihf*|arm*-linux-gnu*|armv7*-linux-gnu*)
      printf '%s\n' "deps-armhf-linux-gnu"
      ;;
    *)
      printf 'unsupported native host compiler triple for deps-host-debug: %s\n' "${triple:-unknown}" >&2
      exit 1
      ;;
  esac
}

case "$preset" in
  deps-host-debug)
    preset=$(resolve_host_debug_preset)
    ;&
  deps-x86_64-linux-gnu)
    cmake_preset="x86_64-linux-gnu-release"
    deps_root="$repo_root/.cache/deps/x86_64-linux-gnu"
    deps_build_root="$repo_root/.cache/deps-build/x86_64-linux-gnu"
    cmake_extra_args=(
      -DLOCKDC_BUILD_DEPENDENCIES=ON
      -DLOCKDC_BUILD_E2E_TESTS=OFF
      -DLOCKDC_BUILD_EXAMPLES=OFF
      -DLOCKDC_BUILD_BENCHMARKS=OFF
      -DLOCKDC_BUILD_FUZZERS=OFF
    )
    ;;
  deps-x86_64-linux-musl)
    cmake_preset="x86_64-linux-musl-release"
    deps_root="$repo_root/.cache/deps/x86_64-linux-musl"
    deps_build_root="$repo_root/.cache/deps-build/x86_64-linux-musl"
    cmake_extra_args=(
      -DLOCKDC_BUILD_DEPENDENCIES=ON
      -DLOCKDC_BUILD_E2E_TESTS=OFF
      -DLOCKDC_BUILD_EXAMPLES=OFF
      -DLOCKDC_BUILD_BENCHMARKS=OFF
      -DLOCKDC_BUILD_FUZZERS=OFF
    )
    ;;
  deps-aarch64-linux-gnu)
    cmake_preset="aarch64-linux-gnu-release"
    deps_root="$repo_root/.cache/deps/aarch64-linux-gnu"
    deps_build_root="$repo_root/.cache/deps-build/aarch64-linux-gnu"
    cmake_extra_args=(
      -DLOCKDC_BUILD_DEPENDENCIES=ON
      -DLOCKDC_BUILD_E2E_TESTS=OFF
      -DLOCKDC_BUILD_EXAMPLES=OFF
      -DLOCKDC_BUILD_BENCHMARKS=OFF
      -DLOCKDC_BUILD_FUZZERS=OFF
    )
    ;;
  deps-aarch64-linux-musl)
    cmake_preset="aarch64-linux-musl-release"
    deps_root="$repo_root/.cache/deps/aarch64-linux-musl"
    deps_build_root="$repo_root/.cache/deps-build/aarch64-linux-musl"
    cmake_extra_args=(
      -DLOCKDC_BUILD_DEPENDENCIES=ON
      -DLOCKDC_BUILD_E2E_TESTS=OFF
      -DLOCKDC_BUILD_EXAMPLES=OFF
      -DLOCKDC_BUILD_BENCHMARKS=OFF
      -DLOCKDC_BUILD_FUZZERS=OFF
    )
    ;;
  deps-armhf-linux-gnu)
    cmake_preset="armhf-linux-gnu-release"
    deps_root="$repo_root/.cache/deps/armhf-linux-gnu"
    deps_build_root="$repo_root/.cache/deps-build/armhf-linux-gnu"
    cmake_extra_args=(
      -DLOCKDC_BUILD_DEPENDENCIES=ON
      -DLOCKDC_BUILD_E2E_TESTS=OFF
      -DLOCKDC_BUILD_EXAMPLES=OFF
      -DLOCKDC_BUILD_BENCHMARKS=OFF
      -DLOCKDC_BUILD_FUZZERS=OFF
    )
    ;;
  deps-armhf-linux-musl)
    cmake_preset="armhf-linux-musl-release"
    deps_root="$repo_root/.cache/deps/armhf-linux-musl"
    deps_build_root="$repo_root/.cache/deps-build/armhf-linux-musl"
    cmake_extra_args=(
      -DLOCKDC_BUILD_DEPENDENCIES=ON
      -DLOCKDC_BUILD_E2E_TESTS=OFF
      -DLOCKDC_BUILD_EXAMPLES=OFF
      -DLOCKDC_BUILD_BENCHMARKS=OFF
      -DLOCKDC_BUILD_FUZZERS=OFF
    )
    ;;
  deps-arm64-apple-darwin)
    cmake_preset="arm64-apple-darwin-release"
    deps_root="$repo_root/.cache/deps/arm64-apple-darwin"
    deps_build_root="$repo_root/.cache/deps-build/arm64-apple-darwin"
    cmake_extra_args=(
      -DLOCKDC_BUILD_DEPENDENCIES=ON
      -DLOCKDC_BUILD_TESTS=OFF
      -DLOCKDC_BUILD_E2E_TESTS=OFF
      -DLOCKDC_BUILD_EXAMPLES=OFF
      -DLOCKDC_BUILD_BENCHMARKS=OFF
      -DLOCKDC_BUILD_FUZZERS=OFF
      -DLOCKDC_BUILD_LUA_BINDINGS=OFF
    )
    ;;
  *)
    echo "usage: scripts/deps.sh [deps-x86_64-linux-gnu|deps-host-debug|deps-x86_64-linux-musl|deps-aarch64-linux-gnu|deps-aarch64-linux-musl|deps-armhf-linux-gnu|deps-armhf-linux-musl|deps-arm64-apple-darwin]" >&2
    exit 2
    ;;
esac

if [ "$dry_run" = "1" ]; then
  printf 'preset=%s\n' "$preset"
  printf 'cmake_preset=%s\n' "$cmake_preset"
  printf 'deps_root=%s\n' "$deps_root"
  printf 'deps_build_root=%s\n' "$deps_build_root"
  exit 0
fi

manifest_path="$deps_root/manifest.txt"
mkdir -p "$deps_root"

resolve_cmake_cache_string() {
  local var_name=$1
  local override_value=$2
  local resolved_value=

  if [ -n "$override_value" ]; then
    printf '%s\n' "$override_value"
    return 0
  fi

  resolved_value=$(
    sed -n "s/^set(${var_name} \"\\(.*\\)\" CACHE STRING.*/\\1/p" \
      "$repo_root/CMakeLists.txt" | head -n1
  )
  if [ -z "$resolved_value" ]; then
    printf 'failed to resolve %s from CMakeLists.txt\n' "$var_name" >&2
    exit 1
  fi

  printf '%s\n' "$resolved_value"
}

cpkt_version=$(resolve_cmake_cache_string LOCKDC_CPKT_VERSION "${LOCKDC_CPKT_VERSION:-}")
openssl_version=$(resolve_cmake_cache_string LOCKDC_OPENSSL_VERSION "${LOCKDC_OPENSSL_VERSION:-}")
zlib_version=$(resolve_cmake_cache_string LOCKDC_ZLIB_VERSION "${LOCKDC_ZLIB_VERSION:-}")
curl_version=$(resolve_cmake_cache_string LOCKDC_CURL_VERSION "${LOCKDC_CURL_VERSION:-}")
nghttp2_version=$(resolve_cmake_cache_string LOCKDC_NGHTTP2_VERSION "${LOCKDC_NGHTTP2_VERSION:-}")
libssh2_version=$(resolve_cmake_cache_string LOCKDC_LIBSSH2_VERSION "${LOCKDC_LIBSSH2_VERSION:-}")
lonejson_version=$(resolve_cmake_cache_string LOCKDC_LONEJSON_VERSION "${LOCKDC_LONEJSON_VERSION:-}")
cmocka_version=$(resolve_cmake_cache_string LOCKDC_CMOCKA_VERSION "${LOCKDC_CMOCKA_VERSION:-}")
pslog_version=$(resolve_cmake_cache_string LOCKDC_PSLOG_VERSION "${LOCKDC_PSLOG_VERSION:-}")

cpkt_asset_name="c.pkt.systems-$cpkt_version-${preset#deps-}.tar.gz"
cpkt_download_url="https://github.com/sa6mwa/c.pkt.systems/releases/download/v$cpkt_version/$cpkt_asset_name"
case "$cpkt_asset_name" in
  c.pkt.systems-0.5.0-x86_64-linux-gnu.tar.gz)
    cpkt_asset_hash=737e1bc3992f00e9d4c71c6cae87db8dce669ce795b42ba769e686ff263cfaea
    ;;
  c.pkt.systems-0.5.0-x86_64-linux-musl.tar.gz)
    cpkt_asset_hash=435c25dfa05e831c5183ed0628730749625aa68804e21a3615937d36d11b7384
    ;;
  c.pkt.systems-0.5.0-aarch64-linux-gnu.tar.gz)
    cpkt_asset_hash=e7163bea53ec8b72bd88b6387289eddc6d11383e8903ae756b06aea03853ee9d
    ;;
  c.pkt.systems-0.5.0-aarch64-linux-musl.tar.gz)
    cpkt_asset_hash=ee19c298dffe5b9b3d304d416de95a897c547f9489eb52493500576c0d36830f
    ;;
  c.pkt.systems-0.5.0-armhf-linux-gnu.tar.gz)
    cpkt_asset_hash=b6f12112207232ef16123d19535d36d73c9a0c75fdd9c1566684f2f36b384d94
    ;;
  c.pkt.systems-0.5.0-armhf-linux-musl.tar.gz)
    cpkt_asset_hash=2de19d4fb76379a7585382ac6b77d3d9a70c505f8ef9334a5ff531336c84b368
    ;;
  c.pkt.systems-0.5.0-arm64-apple-darwin.tar.gz)
    cpkt_asset_hash=ebf1caf096df8c17067df2bd02efe65793ad98f65dafb5f29384ce6a6269a829
    ;;
  *)
    printf 'unsupported c.pkt.systems release asset: %s\n' "$cpkt_asset_name" >&2
    exit 1
    ;;
esac

lonejson_asset_name="liblonejson-$lonejson_version-${preset#deps-}.tar.gz"
lonejson_download_url="https://github.com/sa6mwa/lonejson/releases/download/v$lonejson_version/$lonejson_asset_name"
case "$lonejson_asset_name" in
  liblonejson-0.35.0-x86_64-linux-gnu.tar.gz)
    lonejson_asset_hash=f328684b58c89a3c04d76af919e59ae9b31ee56e0d14d01eaa59fb8126aaec85
    ;;
  liblonejson-0.35.0-x86_64-linux-musl.tar.gz)
    lonejson_asset_hash=5216796e1192cc7aa18b59f809380ee2631e270fce6d1149b6fa5d2e9c95ad7d
    ;;
  liblonejson-0.35.0-aarch64-linux-gnu.tar.gz)
    lonejson_asset_hash=f69a39fc80ac9d0d40c6b0d6c2eec46d5c00aeff1696f983526cbba7d0e6f6db
    ;;
  liblonejson-0.35.0-aarch64-linux-musl.tar.gz)
    lonejson_asset_hash=dd16709052eeff1797e775907db554e7805c9d1bed655a07e44094108d6b68c2
    ;;
  liblonejson-0.35.0-armhf-linux-gnu.tar.gz)
    lonejson_asset_hash=6eb99f6683283b872245608488125f9ac2b05673e9416791fb2ff89d5319601a
    ;;
  liblonejson-0.35.0-armhf-linux-musl.tar.gz)
    lonejson_asset_hash=4ac1e5d844de02548d6e23f06dacc8be44eded0a8f686c7600cc8ee718f648e2
    ;;
  liblonejson-0.35.0-arm64-apple-darwin.tar.gz)
    lonejson_asset_hash=6cbee12c36e43eda14ae5a96265067a99a2a3dd36fe1baffd5fd35e3a8583370
    ;;
  *)
    printf 'unsupported lonejson release asset: %s\n' "$lonejson_asset_name" >&2
    exit 1
    ;;
esac

compiler=${CC:-cc}
compiler_machine=$("$compiler" -dumpmachine 2>/dev/null || echo unknown)
compiler_version=$("$compiler" --version 2>/dev/null | head -n1 || echo unknown)

fingerprint=$(
  {
    cat "$repo_root/CMakePresets.json"
    cat "$repo_root/cmake/LcDependencies.cmake"
    if [ -f "$repo_root/cmake/prune_dependency_install_tree.cmake" ]; then
      cat "$repo_root/cmake/prune_dependency_install_tree.cmake"
    fi
    cat "$repo_root/scripts/deps.sh"
    if [ -d "$repo_root/cmake/toolchains" ]; then
      find "$repo_root/cmake/toolchains" -maxdepth 1 -type f | sort | while read -r file; do
        cat "$file"
      done
    fi
  } | sha256sum | awk '{print $1}'
)

manifest="compiler=$compiler
machine=$compiler_machine
version=$compiler_version
fingerprint=$fingerprint
preset=$preset
cpkt_version=$cpkt_version
cpkt_asset_name=$cpkt_asset_name
cpkt_asset_hash=$cpkt_asset_hash
openssl_version=$openssl_version
zlib_version=$zlib_version
curl_version=$curl_version
nghttp2_version=$nghttp2_version
libssh2_version=$libssh2_version
lonejson_version=$lonejson_version
lonejson_asset_name=$lonejson_asset_name
lonejson_asset_hash=$lonejson_asset_hash
cmocka_version=$cmocka_version
pslog_version=$pslog_version"

manifest_value() {
  local key=$1
  local manifest_file=$2
  sed -n "s/^${key}=//p" "$manifest_file" | head -n1
}

stage_dependency_license() {
  local install_subdir=$1
  local package_name=$2
  local source_path=$3
  local destination_path="$deps_root/$install_subdir/install/share/doc/liblockdc-third-party/$package_name/LICENSE.txt"

  if [ ! -f "$source_path" ]; then
    printf 'missing license file for %s: %s\n' "$package_name" "$source_path" >&2
    exit 1
  fi

  mkdir -p "$(dirname "$destination_path")"
  cp "$source_path" "$destination_path"
}

copy_matching_files() {
  local destination=$1
  shift
  local copied=0
  local candidate

  mkdir -p "$destination"
  for candidate in "$@"; do
    if [ -e "$candidate" ] || [ -L "$candidate" ]; then
      cp -a "$candidate" "$destination/"
      copied=1
    fi
  done
  if [ "$copied" -eq 0 ]; then
    printf 'no c.pkt.systems files matched for destination: %s\n' "$destination" >&2
    exit 1
  fi
}

stage_cpkt_license() {
  local package_name=$1
  local source_path="$deps_root/c.pkt.systems/install/share/doc/c.pkt.systems/third_party/$package_name/LICENSE"
  local destination_dir="$deps_root/$package_name/install/share/doc/liblockdc-third-party/$package_name"

  if [ ! -f "$source_path" ]; then
    printf 'missing c.pkt.systems license for %s: %s\n' "$package_name" "$source_path" >&2
    exit 1
  fi

  mkdir -p "$destination_dir"
  cp "$source_path" "$destination_dir/LICENSE.txt"
}

download_cpkt_bundle() {
  local archive_path="$repo_root/.cache/downloads/$cpkt_asset_name"
  local extract_root="$deps_root/c.pkt.systems/install"
  local actual_hash

  mkdir -p "$repo_root/.cache/downloads" "$extract_root"
  if [ ! -f "$archive_path" ]; then
    curl -fL --connect-timeout "$download_timeout" \
      --max-time "$download_timeout" \
      -o "$archive_path" "$cpkt_download_url"
  fi

  actual_hash=$(sha256sum "$archive_path" | awk '{print $1}')
  if [ "$actual_hash" != "$cpkt_asset_hash" ]; then
    printf 'c.pkt.systems checksum mismatch for %s\nexpected %s\nactual   %s\n' \
      "$cpkt_asset_name" "$cpkt_asset_hash" "$actual_hash" >&2
    exit 1
  fi

  rm -rf "$extract_root"
  mkdir -p "$extract_root"
  tar -xzf "$archive_path" -C "$extract_root" --strip-components=1
}

download_lonejson_bundle() {
  local archive_path="$repo_root/.cache/downloads/$lonejson_asset_name"
  local extract_root="$deps_root/lonejson/install"
  local actual_hash

  mkdir -p "$repo_root/.cache/downloads" "$extract_root"
  if [ ! -f "$archive_path" ]; then
    curl -fL --connect-timeout "$download_timeout" \
      --max-time "$download_timeout" \
      -o "$archive_path" "$lonejson_download_url"
  fi

  actual_hash=$(sha256sum "$archive_path" | awk '{print $1}')
  if [ "$actual_hash" != "$lonejson_asset_hash" ]; then
    printf 'lonejson checksum mismatch for %s\nexpected %s\nactual   %s\n' \
      "$lonejson_asset_name" "$lonejson_asset_hash" "$actual_hash" >&2
    exit 1
  fi

  rm -rf "$extract_root"
  mkdir -p "$extract_root"
  tar -xzf "$archive_path" -C "$extract_root" --strip-components=1
}

stage_cpkt_component_layout() {
  local cpkt_root="$deps_root/c.pkt.systems/install"

  download_cpkt_bundle
  rm -rf \
    "$deps_root/openssl/install" \
    "$deps_root/curl/install" \
    "$deps_root/nghttp2/install" \
    "$deps_root/libssh2/install" \
    "$deps_root/zlib/install"

  copy_matching_files "$deps_root/openssl/install/include" "$cpkt_root/include/openssl"
  copy_matching_files "$deps_root/openssl/install/lib" "$cpkt_root"/lib/libssl* "$cpkt_root"/lib/libcrypto*
  copy_matching_files "$deps_root/openssl/install/lib/pkgconfig" "$cpkt_root"/lib/pkgconfig/libssl.pc "$cpkt_root"/lib/pkgconfig/libcrypto.pc "$cpkt_root"/lib/pkgconfig/openssl.pc
  copy_matching_files "$deps_root/openssl/install/lib/cmake/OpenSSL" "$cpkt_root"/lib/cmake/OpenSSL/*.cmake
  stage_cpkt_license openssl

  copy_matching_files "$deps_root/curl/install/include" "$cpkt_root/include/curl"
  copy_matching_files "$deps_root/curl/install/lib" "$cpkt_root"/lib/libcurl*
  copy_matching_files "$deps_root/curl/install/lib/pkgconfig" "$cpkt_root"/lib/pkgconfig/libcurl.pc
  copy_matching_files "$deps_root/curl/install/lib/cmake/CURL" "$cpkt_root"/lib/cmake/CURL/*.cmake
  stage_cpkt_license curl

  copy_matching_files "$deps_root/nghttp2/install/include" "$cpkt_root/include/nghttp2"
  copy_matching_files "$deps_root/nghttp2/install/lib" "$cpkt_root"/lib/libnghttp2*
  copy_matching_files "$deps_root/nghttp2/install/lib/pkgconfig" "$cpkt_root"/lib/pkgconfig/libnghttp2.pc
  copy_matching_files "$deps_root/nghttp2/install/lib/cmake/nghttp2" "$cpkt_root"/lib/cmake/nghttp2/*.cmake
  stage_cpkt_license nghttp2

  copy_matching_files "$deps_root/libssh2/install/include" \
    "$cpkt_root/include/libssh2.h" \
    "$cpkt_root/include/libssh2_publickey.h" \
    "$cpkt_root/include/libssh2_sftp.h"
  copy_matching_files "$deps_root/libssh2/install/lib" "$cpkt_root"/lib/libssh2*
  copy_matching_files "$deps_root/libssh2/install/lib/pkgconfig" "$cpkt_root"/lib/pkgconfig/libssh2.pc
  copy_matching_files "$deps_root/libssh2/install/lib/cmake/libssh2" "$cpkt_root"/lib/cmake/libssh2/*.cmake
  stage_cpkt_license libssh2

  copy_matching_files "$deps_root/zlib/install/include" \
    "$cpkt_root/include/zlib.h" \
    "$cpkt_root/include/zconf.h"
  copy_matching_files "$deps_root/zlib/install/lib" "$cpkt_root"/lib/libz*
  copy_matching_files "$deps_root/zlib/install/lib/pkgconfig" "$cpkt_root"/lib/pkgconfig/zlib.pc
  copy_matching_files "$deps_root/zlib/install/lib/cmake/zlib" "$cpkt_root"/lib/cmake/zlib/*.cmake
  stage_cpkt_license zlib
}

prune_dependency_install_trees() {
  cmake -DLOCKDC_EXTERNAL_ROOT="$deps_root" -P "$repo_root/cmake/prune_dependency_install_tree.cmake"
}

assert_dependency_install_tree_privacy() {
  cmake \
    -DLOCKDC_ROOT="$repo_root" \
    -DLOCKDC_SCAN_LABEL="dependency install tree $deps_root" \
    -DLOCKDC_SCAN_PATHS="$deps_root" \
    -P "$repo_root/tests/release_privacy_scan.cmake"
}

reset_dependency_build_root() {
  local attempt=1

  while [ "$attempt" -le 5 ]; do
    rm -rf "$deps_build_root"
    if [ ! -e "$deps_build_root" ]; then
      return 0
    fi
    sleep 1
    attempt=$((attempt + 1))
  done

  printf 'failed to reset dependency build root: %s\n' "$deps_build_root" >&2
  exit 1
}

case "$preset" in
  deps-arm64-apple-darwin)
    shared_ext=dylib
    libssh2_shared_path="$deps_root/libssh2/install/lib/libssh2.1.${shared_ext}"
    libssh2_shared_soname_path="$deps_root/libssh2/install/lib/libssh2.1.${shared_ext}"
    libssh2_shared_versioned_path="$deps_root/libssh2/install/lib/libssh2.1.${shared_ext}"
    zlib_shared_path="$deps_root/zlib/install/lib/libz.${shared_ext}"
    zlib_shared_soname_path="$deps_root/zlib/install/lib/libz.1.${shared_ext}"
    zlib_shared_versioned_path="$deps_root/zlib/install/lib/libz.$zlib_version.${shared_ext}"
    pslog_shared_path="$deps_root/pslog/install/lib/libpslog.0.${shared_ext}"
    lonejson_shared_path="$deps_root/lonejson/install/lib/liblonejson.${lonejson_abi_version}.${shared_ext}"
    ;;
  *)
    shared_ext=so
    libssh2_shared_path="$deps_root/libssh2/install/lib/libssh2.so"
    libssh2_shared_soname_path="$deps_root/libssh2/install/lib/libssh2.so.1"
    libssh2_shared_versioned_path="$deps_root/libssh2/install/lib/libssh2.so.1.0.1"
    zlib_shared_path="$deps_root/zlib/install/lib/libz.so"
    zlib_shared_soname_path="$deps_root/zlib/install/lib/libz.so.1"
    zlib_shared_versioned_path="$deps_root/zlib/install/lib/libz.so.$zlib_version"
    pslog_shared_path="$deps_root/pslog/install/lib/libpslog.so.0"
    lonejson_shared_path="$deps_root/lonejson/install/lib/liblonejson.so.${lonejson_abi_version}"
    ;;
esac
curl_shared_path="$deps_root/curl/install/lib/libcurl.${shared_ext}"
openssl_ssl_shared_path="$deps_root/openssl/install/lib/libssl.${shared_ext}"
openssl_crypto_shared_path="$deps_root/openssl/install/lib/libcrypto.${shared_ext}"
nghttp2_shared_path="$deps_root/nghttp2/install/lib/libnghttp2.${shared_ext}"

deps_ready=1
required_paths=(
  "$deps_root/openssl/install/lib/libssl.a"
  "$deps_root/openssl/install/lib/libcrypto.a"
  "$openssl_ssl_shared_path"
  "$openssl_crypto_shared_path"
  "$deps_root/nghttp2/install/lib/libnghttp2.a"
  "$nghttp2_shared_path"
  "$deps_root/libssh2/install/lib/libssh2.a"
  "$libssh2_shared_path"
  "$libssh2_shared_soname_path"
  "$libssh2_shared_versioned_path"
  "$deps_root/libssh2/install/include/libssh2.h"
  "$deps_root/libssh2/install/include/libssh2_publickey.h"
  "$deps_root/libssh2/install/include/libssh2_sftp.h"
  "$deps_root/zlib/install/lib/libz.a"
  "$zlib_shared_path"
  "$zlib_shared_soname_path"
  "$zlib_shared_versioned_path"
  "$deps_root/zlib/install/include/zlib.h"
  "$deps_root/zlib/install/include/zconf.h"
  "$deps_root/curl/install/lib/libcurl.a"
  "$curl_shared_path"
  "$deps_root/c.pkt.systems/install/lib/cmake/CURL/CURLConfig.cmake"
  "$deps_root/c.pkt.systems/install/lib/cmake/OpenSSL/OpenSSLConfig.cmake"
  "$deps_root/c.pkt.systems/install/lib/cmake/libssh2/libssh2-config.cmake"
  "$deps_root/c.pkt.systems/install/lib/cmake/nghttp2/nghttp2Config.cmake"
  "$deps_root/c.pkt.systems/install/lib/cmake/zlib/ZLIBConfig.cmake"
  "$deps_root/c.pkt.systems/install/lib/pkgconfig/libcurl.pc"
  "$deps_root/c.pkt.systems/install/lib/pkgconfig/openssl.pc"
  "$deps_root/c.pkt.systems/install/lib/pkgconfig/libssh2.pc"
  "$deps_root/c.pkt.systems/install/lib/pkgconfig/libnghttp2.pc"
  "$deps_root/c.pkt.systems/install/lib/pkgconfig/zlib.pc"
  "$deps_root/pslog/install/lib/libpslog.a"
  "$deps_root/pslog/install/include/pslog.h"
  "$pslog_shared_path"
  "$deps_root/lonejson/install/lib/liblonejson.a"
  "$deps_root/lonejson/install/include/lonejson.h"
  "$lonejson_shared_path"
  "$deps_root/lonejson/install/lib/pkgconfig/lonejson.pc"
  "$deps_root/lonejson/install/lib/cmake/lonejson/lonejsonConfig.cmake"
  "$deps_root/lonejson/install/lib/cmake/lonejson/lonejsonConfigVersion.cmake"
)
if [ "$preset" != "deps-arm64-apple-darwin" ]; then
  required_paths+=("$deps_root/cmocka/install/lib/libcmocka.a")
fi

for path in "${required_paths[@]}"; do
  if [ ! -f "$path" ]; then
    deps_ready=0
    break
  fi
done

if [ "$deps_ready" -eq 1 ] && [ -f "$manifest_path" ]; then
  existing_manifest=$(cat "$manifest_path")
  if [ "$existing_manifest" = "$manifest" ]; then
    prune_dependency_install_trees
    assert_dependency_install_tree_privacy
    exit 0
  fi

  if ! grep -q '^openssl_version=' "$manifest_path"; then
    if [ "$(manifest_value compiler "$manifest_path")" = "$compiler" ] \
      && [ "$(manifest_value machine "$manifest_path")" = "$compiler_machine" ] \
      && [ "$(manifest_value version "$manifest_path")" = "$compiler_version" ] \
      && [ "$(manifest_value preset "$manifest_path")" = "$preset" ] \
      && [ "$(manifest_value zlib_version "$manifest_path")" = "$zlib_version" ]; then
      prune_dependency_install_trees
      assert_dependency_install_tree_privacy
      printf '%s\n' "$manifest" > "$manifest_path"
      exit 0
    fi
  fi
fi

reset_dependency_build_root
stage_cpkt_component_layout
download_lonejson_bundle
cmake_extra_args+=("-DLOCKDC_ZLIB_VERSION=$zlib_version")
cmake_extra_args+=("-DLOCKDC_CPKT_VERSION=$cpkt_version")
cmake --preset "$cmake_preset" --fresh "${cmake_extra_args[@]}"
cmake --build --preset "$cmake_preset" --target lc_deps
stage_dependency_license "pslog" "libpslog" "$deps_root/pslog/install/share/doc/libpslog/LICENSE"
stage_dependency_license "lonejson" "lonejson" "$deps_root/lonejson/install/share/doc/liblonejson/LICENSE"
prune_dependency_install_trees
assert_dependency_install_tree_privacy
printf '%s\n' "$manifest" > "$manifest_path"

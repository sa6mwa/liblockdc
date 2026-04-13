#!/usr/bin/env bash
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
preset=${1:-deps-host-debug}

unset LD_LIBRARY_PATH

case "$preset" in
  deps-host-debug)
    cmake_preset="debug"
    deps_root="$repo_root/.cache/deps/host-debug"
    deps_build_root="$repo_root/.cache/deps-build/host-debug"
    cmake_extra_args=(
      -DLOCKDC_BUILD_DEPENDENCIES=ON
      -DLOCKDC_BUILD_EXAMPLES=OFF
      -DLOCKDC_BUILD_BENCHMARKS=OFF
      -DLOCKDC_BUILD_FUZZERS=OFF
    )
    ;;
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
  *)
    echo "usage: scripts/deps.sh [deps-host-debug|deps-x86_64-linux-gnu|deps-x86_64-linux-musl|deps-aarch64-linux-gnu|deps-aarch64-linux-musl|deps-armhf-linux-gnu|deps-armhf-linux-musl]" >&2
    exit 2
    ;;
esac

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

openssl_version=$(resolve_cmake_cache_string LOCKDC_OPENSSL_VERSION "${LOCKDC_OPENSSL_VERSION:-}")
zlib_version=$(resolve_cmake_cache_string LOCKDC_ZLIB_VERSION "${LOCKDC_ZLIB_VERSION:-}")
curl_version=$(resolve_cmake_cache_string LOCKDC_CURL_VERSION "${LOCKDC_CURL_VERSION:-}")
nghttp2_version=$(resolve_cmake_cache_string LOCKDC_NGHTTP2_VERSION "${LOCKDC_NGHTTP2_VERSION:-}")
libssh2_version=$(resolve_cmake_cache_string LOCKDC_LIBSSH2_VERSION "${LOCKDC_LIBSSH2_VERSION:-}")
lonejson_version=$(resolve_cmake_cache_string LOCKDC_LONEJSON_VERSION "${LOCKDC_LONEJSON_VERSION:-}")
cmocka_version=$(resolve_cmake_cache_string LOCKDC_CMOCKA_VERSION "${LOCKDC_CMOCKA_VERSION:-}")
pslog_version=$(resolve_cmake_cache_string LOCKDC_PSLOG_VERSION "${LOCKDC_PSLOG_VERSION:-}")

compiler=${CC:-cc}
compiler_machine=$("$compiler" -dumpmachine 2>/dev/null || echo unknown)
compiler_version=$("$compiler" --version 2>/dev/null | head -n1 || echo unknown)

fingerprint=$(
  {
    cat "$repo_root/CMakePresets.json"
    cat "$repo_root/cmake/LcDependencies.cmake"
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
openssl_version=$openssl_version
zlib_version=$zlib_version
curl_version=$curl_version
nghttp2_version=$nghttp2_version
libssh2_version=$libssh2_version
lonejson_version=$lonejson_version
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

shared_ext=so
if [[ "$preset" == *musl ]] || [ "$preset" = "deps-host-debug" ]; then
  curl_shared_path="$deps_root/curl-shared-cmake/install/lib/libcurl.${shared_ext}"
  openssl_ssl_shared_path="$deps_root/openssl-shared/install/lib/libssl.${shared_ext}"
  openssl_crypto_shared_path="$deps_root/openssl-shared/install/lib/libcrypto.${shared_ext}"
  nghttp2_shared_path="$deps_root/nghttp2-shared/install/lib/libnghttp2.${shared_ext}"
else
  curl_shared_path="$deps_root/curl-shared-cmake/install/lib/libcurl.${shared_ext}"
  openssl_ssl_shared_path="$deps_root/openssl-shared/install/lib/libssl.${shared_ext}"
  openssl_crypto_shared_path="$deps_root/openssl-shared/install/lib/libcrypto.${shared_ext}"
  nghttp2_shared_path="$deps_root/nghttp2-shared/install/lib/libnghttp2.${shared_ext}"
fi

deps_ready=1
required_paths=(
  "$deps_root/openssl-static/install/lib/libssl.a"
  "$deps_root/openssl-static/install/lib/libcrypto.a"
  "$openssl_ssl_shared_path"
  "$openssl_crypto_shared_path"
  "$deps_root/nghttp2-static/install/lib/libnghttp2.a"
  "$nghttp2_shared_path"
  "$deps_root/libssh2/install/lib/libssh2.a"
  "$deps_root/libssh2/install/lib/libssh2.so"
  "$deps_root/libssh2/install/lib/libssh2.so.1"
  "$deps_root/libssh2/install/lib/libssh2.so.1.0.1"
  "$deps_root/libssh2/install/include/libssh2.h"
  "$deps_root/libssh2/install/include/libssh2_publickey.h"
  "$deps_root/libssh2/install/include/libssh2_sftp.h"
  "$deps_root/zlib/install/lib/libz.a"
  "$deps_root/zlib/install/lib/libz.so"
  "$deps_root/zlib/install/lib/libz.so.1"
  "$deps_root/zlib/install/lib/libz.so.$zlib_version"
  "$deps_root/zlib/install/include/zlib.h"
  "$deps_root/zlib/install/include/zconf.h"
  "$deps_root/curl-static/install/lib/libcurl.a"
  "$curl_shared_path"
  "$deps_root/pslog-static/install/lib/libpslog.a"
  "$deps_root/pslog-static/install/include/pslog.h"
  "$deps_root/pslog-shared/install/lib/libpslog.so.0"
  "$deps_root/lonejson-static/install/lib/liblonejson.a"
  "$deps_root/lonejson-static/install/include/lonejson.h"
  "$deps_root/lonejson-shared/install/lib/liblonejson.so.0"
  "$deps_root/cmocka/install/lib/libcmocka.a"
)

for path in "${required_paths[@]}"; do
  if [ ! -f "$path" ]; then
    deps_ready=0
    break
  fi
done

if [ "$deps_ready" -eq 1 ] && [ -f "$manifest_path" ]; then
  existing_manifest=$(cat "$manifest_path")
  if [ "$existing_manifest" = "$manifest" ]; then
    exit 0
  fi

  if ! grep -q '^openssl_version=' "$manifest_path"; then
    if [ "$(manifest_value compiler "$manifest_path")" = "$compiler" ] \
      && [ "$(manifest_value machine "$manifest_path")" = "$compiler_machine" ] \
      && [ "$(manifest_value version "$manifest_path")" = "$compiler_version" ] \
      && [ "$(manifest_value preset "$manifest_path")" = "$preset" ] \
      && [ "$(manifest_value zlib_version "$manifest_path")" = "$zlib_version" ]; then
      printf '%s\n' "$manifest" > "$manifest_path"
      exit 0
    fi
  fi
fi

reset_dependency_build_root
cmake_extra_args+=("-DLOCKDC_ZLIB_VERSION=$zlib_version")
cmake --preset "$cmake_preset" --fresh "${cmake_extra_args[@]}"
cmake --build --preset "$cmake_preset" --target lc_deps
stage_dependency_license "openssl-shared" "openssl" "$deps_build_root/openssl-shared/src/LICENSE.txt"
stage_dependency_license "curl-shared-cmake" "curl" "$deps_build_root/curl-shared-cmake/src/COPYING"
stage_dependency_license "libssh2" "libssh2" "$deps_build_root/libssh2/src/COPYING"
stage_dependency_license "zlib" "zlib" "$deps_build_root/zlib/src/LICENSE"
stage_dependency_license "pslog-shared" "libpslog" "$deps_root/pslog-shared/install/share/doc/libpslog/LICENSE"
stage_dependency_license "nghttp2-shared" "nghttp2" "$deps_build_root/nghttp2-shared/src/COPYING"
stage_dependency_license "lonejson-shared" "lonejson" "$deps_root/lonejson-shared/install/share/doc/liblonejson/LICENSE"
printf '%s\n' "$manifest" > "$manifest_path"

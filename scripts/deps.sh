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

compiler=${CC:-cc}
compiler_machine=$("$compiler" -dumpmachine 2>/dev/null || echo unknown)
compiler_version=$("$compiler" --version 2>/dev/null | head -n1 || echo unknown)

fingerprint=$(
  {
    cat "$repo_root/CMakeLists.txt"
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
preset=$preset"

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
  "$deps_root/curl-static/install/lib/libcurl.a"
  "$curl_shared_path"
  "$deps_root/pslog-static/install/lib/libpslog.a"
  "$deps_root/pslog-static/install/include/pslog.h"
  "$deps_root/pslog-shared/install/lib/libpslog.so.0.1.0"
  "$deps_root/yajl-static/install/lib/libyajl_s.a"
  "$deps_root/yajl-shared/install/lib/libyajl.so.2.1.0"
  "$deps_root/cmocka/install/lib/libcmocka.a"
)

for path in "${required_paths[@]}"; do
  if [ ! -f "$path" ]; then
    deps_ready=0
    break
  fi
done

if [ "$deps_ready" -eq 1 ] && [ -f "$manifest_path" ] && [ "$(cat "$manifest_path")" = "$manifest" ]; then
  exit 0
fi

reset_dependency_build_root
cmake --preset "$cmake_preset" --fresh "${cmake_extra_args[@]}"
cmake --build --preset "$cmake_preset" --target lc_deps
stage_dependency_license "openssl-shared" "openssl" "$deps_build_root/openssl-shared/src/LICENSE.txt"
stage_dependency_license "curl-shared-cmake" "curl" "$deps_build_root/curl-shared-cmake/src/COPYING"
stage_dependency_license "pslog-shared" "libpslog" "$deps_root/pslog-shared/install/share/doc/libpslog/LICENSE"
stage_dependency_license "nghttp2-shared" "nghttp2" "$deps_build_root/nghttp2-shared/src/COPYING"
stage_dependency_license "yajl-shared" "yajl" "$deps_build_root/yajl-shared/src/COPYING"
printf '%s\n' "$manifest" > "$manifest_path"

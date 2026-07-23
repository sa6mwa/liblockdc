#!/usr/bin/env bash
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
preset=${1:-deps-x86_64-linux-gnu}

unset LD_LIBRARY_PATH
dry_run=${LOCKDC_DEPS_DRY_RUN:-0}
download_timeout=${LOCKDC_DEPENDENCY_DOWNLOAD_TIMEOUT:-300}
local_download_root=${LOCKDC_DOWNLOAD_ROOT:-$repo_root/.cache/downloads}
lonejson_abi_version=${LOCKDC_LONEJSON_ABI_VERSION:-25}
liblql_abi_version=${LOCKDC_LIBLQL_ABI_VERSION:-0}

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

cmocka_asset_name="cmocka-$cmocka_version.tar.xz"
cmocka_download_url="https://cmocka.org/files/2.0/$cmocka_asset_name"
case "$cmocka_asset_name" in
  cmocka-2.0.2.tar.xz)
    cmocka_asset_hash=39f92f366bdf3f1a02af4da75b4a5c52df6c9f7e736c7d65de13283f9f0ef416
    ;;
  *)
    printf 'unsupported cmocka release asset: %s\n' "$cmocka_asset_name" >&2
    exit 1
    ;;
esac

cpkt_asset_name="c.pkt.systems-$cpkt_version-${preset#deps-}.tar.gz"
cpkt_download_url="https://github.com/sa6mwa/c.pkt.systems/releases/download/v$cpkt_version/$cpkt_asset_name"
case "$cpkt_asset_name" in
  c.pkt.systems-0.9.0-x86_64-linux-gnu.tar.gz)
    cpkt_asset_hash=0bbb1cbaf60b0a94fb5a6b3756123088b45e2bef9e38079038f22e3c07febb2e
    ;;
  c.pkt.systems-0.9.0-x86_64-linux-musl.tar.gz)
    cpkt_asset_hash=e867e7d8649bba6d6c4bed254f3a666faa090f8ccb31a3eb10b1323b694f2f21
    ;;
  c.pkt.systems-0.9.0-aarch64-linux-gnu.tar.gz)
    cpkt_asset_hash=3fb1fdeb83bfd58da48a3319dfc2c6d35384265b2db2074b63216240bf0fe2ad
    ;;
  c.pkt.systems-0.9.0-aarch64-linux-musl.tar.gz)
    cpkt_asset_hash=a915993c294e96c9a84b072bed45384c23f0058c9e18cd5caeba344aaa9b5d39
    ;;
  c.pkt.systems-0.9.0-armhf-linux-gnu.tar.gz)
    cpkt_asset_hash=18738e2d8e9661ebdcc0b54f4f292f0571d04218fceb1f281e051a50928d1694
    ;;
  c.pkt.systems-0.9.0-armhf-linux-musl.tar.gz)
    cpkt_asset_hash=7f5365014ef2222cb95c08525c0b123afb30b4f220f4edcd669f354a9af4ccab
    ;;
  c.pkt.systems-0.9.0-arm64-apple-darwin.tar.gz)
    cpkt_asset_hash=8bc25d47d30cb40b24eb5d07c2aad7850150fdea680eccadd1c819ce945901af
    ;;
  *)
    printf 'unsupported c.pkt.systems release asset: %s\n' "$cpkt_asset_name" >&2
    exit 1
    ;;
esac

lonejson_asset_name="liblonejson-$lonejson_version-${preset#deps-}.tar.gz"
lonejson_download_url="https://github.com/sa6mwa/lonejson/releases/download/v$lonejson_version/$lonejson_asset_name"
case "$lonejson_asset_name" in
  liblonejson-0.42.0-x86_64-linux-gnu.tar.gz)
    lonejson_asset_hash=e04f80b907d92f7e38f825fbd339297e85372fc1ce110abb9a93715ee450ece3
    ;;
  liblonejson-0.42.0-x86_64-linux-musl.tar.gz)
    lonejson_asset_hash=ca0811bd920f6cf59f82d45e04525b562bba238564e5c5b9a00aa18331b5a5ca
    ;;
  liblonejson-0.42.0-aarch64-linux-gnu.tar.gz)
    lonejson_asset_hash=d7f9c700be6f9af7e46b18d59a0be14a42bd19644a30684b5a1135f96ee2daed
    ;;
  liblonejson-0.42.0-aarch64-linux-musl.tar.gz)
    lonejson_asset_hash=813950b50620cfa48e01ae0c5b30ae338b79066e750c45cefeb9a86076466903
    ;;
  liblonejson-0.42.0-armhf-linux-gnu.tar.gz)
    lonejson_asset_hash=3aeff1901078917a4430dc945c253cf4cec193311f35245b4ef1c62056d181c1
    ;;
  liblonejson-0.42.0-armhf-linux-musl.tar.gz)
    lonejson_asset_hash=37ba738c675b41c563b1b03ea322ad1e65dbe76749ebfd294809b50abafb2d32
    ;;
  liblonejson-0.42.0-arm64-apple-darwin.tar.gz)
    lonejson_asset_hash=b351df4221e16d62b7b86940a6a6a6a4d38fffb850b2d118fca2f9f5a9bb5488
    ;;
  *)
    printf 'unsupported lonejson release asset: %s\n' "$lonejson_asset_name" >&2
    exit 1
    ;;
esac

liblql_version=$(resolve_cmake_cache_string LOCKDC_LIBLQL_VERSION "${LOCKDC_LIBLQL_VERSION:-}")
liblql_asset_name="liblql-$liblql_version-${preset#deps-}.tar.gz"
liblql_download_url="https://github.com/sa6mwa/liblql/releases/download/v$liblql_version/$liblql_asset_name"
case "$liblql_asset_name" in
  liblql-0.1.0-x86_64-linux-gnu.tar.gz)
    liblql_asset_hash=4296c0072f76c7a53b53c2ae9f0cbd0c5b20c2955790eae84a75a76b75786146
    ;;
  liblql-0.1.0-x86_64-linux-musl.tar.gz)
    liblql_asset_hash=88b8606fee305755e194a6a0d6ef5ecffe3e0a9890451683da20cce0ebe3b1f1
    ;;
  liblql-0.1.0-aarch64-linux-gnu.tar.gz)
    liblql_asset_hash=79fbfebeed6968a88ce58cda37b17ed3a39f2543a177c7d0bbbc3024dcb6ea5a
    ;;
  liblql-0.1.0-aarch64-linux-musl.tar.gz)
    liblql_asset_hash=407b3a0157d9646eb515a14ea25e45ef1fe1311d4319f34a47e98baf852705db
    ;;
  liblql-0.1.0-armhf-linux-gnu.tar.gz)
    liblql_asset_hash=2d991728cd46b33013300fcd5675047d15ea0b3bb9bcde8ace6dae720fa64c33
    ;;
  liblql-0.1.0-armhf-linux-musl.tar.gz)
    liblql_asset_hash=49fd62676f58c37d8d54e5a050280882326b94bf80642a59bc3c88f4d990c4e6
    ;;
  liblql-0.1.0-arm64-apple-darwin.tar.gz)
    liblql_asset_hash=75630a993625902481dc5422d68994bf1fe5ba07cac160a4b2fa3e64f89d75dc
    ;;
  *)
    printf 'unsupported liblql release asset: %s\n' "$liblql_asset_name" >&2
    exit 1
    ;;
esac

pslog_asset_name="libpslog-$pslog_version-${preset#deps-}.tar.gz"
pslog_download_url="https://github.com/sa6mwa/libpslog/releases/download/v$pslog_version/$pslog_asset_name"
case "$pslog_asset_name" in
  libpslog-0.9.0-x86_64-linux-gnu.tar.gz)
    pslog_asset_hash=7981ce7e60f6f1e144042e7a9192bb661472756ae34336fb0c2ed8316b31945f
    ;;
  libpslog-0.9.0-x86_64-linux-musl.tar.gz)
    pslog_asset_hash=d05e59e8d88018a2e78e0941d2db211f3c08e4fd7539065ed2de79ce7e371055
    ;;
  libpslog-0.9.0-aarch64-linux-gnu.tar.gz)
    pslog_asset_hash=38bb08ca6646cf186925a724b61fb534fa49ec0d5e77ca95953dd7a5b18f76e1
    ;;
  libpslog-0.9.0-aarch64-linux-musl.tar.gz)
    pslog_asset_hash=fce3c4f95b317563427437313ef2eb1987dc43973b0b0bf5169763d0a2705f69
    ;;
  libpslog-0.9.0-armhf-linux-gnu.tar.gz)
    pslog_asset_hash=eff69fe9223cd2ad56572ad6acd768b560ac3e863e379c65367ad6338dbfffef
    ;;
  libpslog-0.9.0-armhf-linux-musl.tar.gz)
    pslog_asset_hash=19eeadacfb82b7eba4187b1fc405225bf85a8866ea81939e2eaa841a23d3785c
    ;;
  libpslog-0.9.0-arm64-apple-darwin.tar.gz)
    pslog_asset_hash=ff5d2106bcbc5ea5bce8dfdbca54d21650f350e50fd214a4b52ac65b4f834073
    ;;
  *)
    printf 'unsupported libpslog release asset: %s\n' "$pslog_asset_name" >&2
    exit 1
    ;;
esac

dependency_target_id=${preset#deps-}

toolchain_value() {
  local key=$1
  printf '%s\n' "$toolchain_description" | sed -n "s/^${key}=//p" | head -n1
}

case "$dependency_target_id" in
  *-linux-*)
    "$repo_root/scripts/cpkt-toolchains.sh" ensure "$dependency_target_id" >/dev/null
    toolchain_description=$("$repo_root/scripts/cpkt-toolchains.sh" discover "$dependency_target_id")
    if ! printf '%s\n' "$toolchain_description" | grep -q '^status=ready$'; then
      printf 'Bootlin toolchain is not ready for %s\n%s\n' "$dependency_target_id" "$toolchain_description" >&2
      exit 1
    fi
    toolchain_source=$(toolchain_value source)
    toolchain_archive=$(toolchain_value archive)
    toolchain_root=$(toolchain_value root)
    toolchain_prefix=$(toolchain_value prefix)
    toolchain_sysroot=$(toolchain_value sysroot)
    toolchain_target_triple=$(toolchain_value target_triple)
    compiler_path=$(toolchain_value cc)
    compiler_machine=$("$compiler_path" -dumpmachine 2>/dev/null || echo unknown)
    compiler_version=$("$compiler_path" --version 2>/dev/null | head -n1 || echo unknown)
    compiler=$(basename -- "$compiler_path")
    ;;
  *)
    toolchain_description=$("$repo_root/scripts/cpkt-toolchains.sh" discover "$dependency_target_id")
    toolchain_source=$(toolchain_value source)
    toolchain_archive=$(toolchain_value archive)
    toolchain_root=$(toolchain_value root)
    toolchain_prefix=$(toolchain_value prefix)
    toolchain_sysroot=$(toolchain_value sysroot)
    toolchain_target_triple=$(toolchain_value target_triple)
    compiler_path=$(toolchain_value cc)
    if [ -z "$compiler_path" ]; then
      compiler_path=${CC:-cc}
    fi
    compiler_machine=$("$compiler_path" -dumpmachine 2>/dev/null || echo unknown)
    compiler_version=$("$compiler_path" --version 2>/dev/null | head -n1 || echo unknown)
    compiler=$(basename -- "$compiler_path")
    ;;
esac
toolchain_root_identity=$(basename -- "$toolchain_root")
toolchain_sysroot_identity=$toolchain_sysroot
case "$toolchain_sysroot_identity" in
  "$toolchain_root"/*)
    toolchain_sysroot_identity=${toolchain_sysroot_identity#"$toolchain_root"/}
    ;;
esac

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
toolchain_target_id=$dependency_target_id
toolchain_source=$toolchain_source
toolchain_archive=$toolchain_archive
toolchain_root=$toolchain_root_identity
toolchain_prefix=$toolchain_prefix
toolchain_sysroot=$toolchain_sysroot_identity
toolchain_target_triple=$toolchain_target_triple
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
liblql_version=$liblql_version
liblql_asset_name=$liblql_asset_name
liblql_asset_hash=$liblql_asset_hash
cmocka_version=$cmocka_version
cmocka_asset_name=$cmocka_asset_name
cmocka_asset_hash=$cmocka_asset_hash
pslog_version=$pslog_version
pslog_asset_name=$pslog_asset_name
pslog_asset_hash=$pslog_asset_hash"

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

acquire_verified_archive() {
  local component=$1
  local url=$2
  local expected_hash=$3
  local archive_name=$4
  local output_path=$5
  local cache_args=()

  if [ "${CPKT_DEPENDENCY_CACHE+x}" ]; then
    cache_args=(-DCPKT_DEPENDENCY_CACHE="$CPKT_DEPENDENCY_CACHE")
  fi

  cmake \
    -DLOCKDC_ARCHIVE_COMPONENT="$component" \
    -DLOCKDC_ARCHIVE_URL="$url" \
    -DLOCKDC_ARCHIVE_SHA256="$expected_hash" \
    -DLOCKDC_ARCHIVE_NAME="$archive_name" \
    -DLOCKDC_ARCHIVE_OUTPUT="$output_path" \
    -DLOCKDC_ARCHIVE_TIMEOUT="$download_timeout" \
    "${cache_args[@]}" \
    -P "$repo_root/cmake/acquire_verified_archive.cmake"
}

download_cpkt_bundle() {
  local archive_path="$local_download_root/$cpkt_asset_name"
  local extract_root="$deps_root/c.pkt.systems/install"
  local actual_hash

  mkdir -p "$local_download_root" "$extract_root"
  acquire_verified_archive "c.pkt.systems" "$cpkt_download_url" "$cpkt_asset_hash" "$cpkt_asset_name" "$archive_path"

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
  local archive_path="$local_download_root/$lonejson_asset_name"
  local extract_root="$deps_root/lonejson/install"
  local actual_hash

  mkdir -p "$local_download_root" "$extract_root"
  acquire_verified_archive "lonejson" "$lonejson_download_url" "$lonejson_asset_hash" "$lonejson_asset_name" "$archive_path"

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

download_liblql_bundle() {
  local archive_path="$local_download_root/$liblql_asset_name"
  local extract_root="$deps_root/liblql/install"
  local actual_hash

  mkdir -p "$local_download_root" "$extract_root"
  acquire_verified_archive "liblql" "$liblql_download_url" "$liblql_asset_hash" "$liblql_asset_name" "$archive_path"

  actual_hash=$(sha256sum "$archive_path" | awk '{print $1}')
  if [ "$actual_hash" != "$liblql_asset_hash" ]; then
    printf 'liblql checksum mismatch for %s\nexpected %s\nactual   %s\n' \
      "$liblql_asset_name" "$liblql_asset_hash" "$actual_hash" >&2
    exit 1
  fi

  rm -rf "$extract_root"
  mkdir -p "$extract_root"
  tar -xzf "$archive_path" -C "$extract_root" --strip-components=1
}

download_cmocka_bundle() {
  local archive_path="$local_download_root/$cmocka_asset_name"

  mkdir -p "$local_download_root"
  acquire_verified_archive "cmocka" "$cmocka_download_url" "$cmocka_asset_hash" "$cmocka_asset_name" "$archive_path"
}

download_pslog_bundle() {
  local archive_path="$local_download_root/$pslog_asset_name"

  mkdir -p "$local_download_root"
  acquire_verified_archive "libpslog" "$pslog_download_url" "$pslog_asset_hash" "$pslog_asset_name" "$archive_path"
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
    liblql_shared_path="$deps_root/liblql/install/lib/liblql.${liblql_abi_version}.${shared_ext}"
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
    liblql_shared_path="$deps_root/liblql/install/lib/liblql.so.${liblql_abi_version}"
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
  "$deps_root/liblql/install/lib/liblql.a"
  "$deps_root/liblql/install/include/lql/lql.h"
  "$liblql_shared_path"
  "$deps_root/liblql/install/lib/pkgconfig/liblql.pc"
  "$deps_root/liblql/install/lib/cmake/liblql/liblqlConfig.cmake"
  "$deps_root/liblql/install/lib/cmake/liblql/liblqlConfigVersion.cmake"
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
      printf '%s\n' "$manifest" > "$manifest_path"
      assert_dependency_install_tree_privacy
      exit 0
    fi
  fi
fi

reset_dependency_build_root
stage_cpkt_component_layout
download_lonejson_bundle
download_liblql_bundle
download_pslog_bundle
cmake_extra_args+=("-DLOCKDC_PSLOG_ARCHIVE_PATH=$local_download_root/$pslog_asset_name")
if [ "$preset" != "deps-arm64-apple-darwin" ]; then
  download_cmocka_bundle
  cmake_extra_args+=("-DLOCKDC_CMOCKA_ARCHIVE_PATH=$local_download_root/$cmocka_asset_name")
fi
cmake_extra_args+=("-DLOCKDC_ZLIB_VERSION=$zlib_version")
cmake_extra_args+=("-DLOCKDC_CPKT_VERSION=$cpkt_version")
cmake --preset "$cmake_preset" --fresh "${cmake_extra_args[@]}"
cmake --build --preset "$cmake_preset" --target lc_deps
stage_dependency_license "pslog" "libpslog" "$deps_root/pslog/install/share/doc/libpslog/LICENSE"
stage_dependency_license "lonejson" "lonejson" "$deps_root/lonejson/install/share/doc/liblonejson/LICENSE"
stage_dependency_license "liblql" "liblql" "$deps_root/liblql/install/share/doc/liblql/LICENSE"
prune_dependency_install_trees
printf '%s\n' "$manifest" > "$manifest_path"
assert_dependency_install_tree_privacy

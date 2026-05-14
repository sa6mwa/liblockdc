#!/usr/bin/env bash
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
requested_abi=${1:-all}
cmake_bin=${CMAKE:-cmake}
build_script=${LOCKDC_BUILD_SCRIPT:-$script_dir/build.sh}
deps_script=${LOCKDC_DEPS_SCRIPT:-$script_dir/deps.sh}
lockdc_version=${LOCKDC_VERSION:-}

unset LD_LIBRARY_PATH

resolve_version() {
  local version_file

  version_file=$(mktemp)
  trap 'rm -f "$version_file"' EXIT
  "$cmake_bin" \
    -DLOCKDC_ROOT="$repo_root" \
    -DLOCKDC_VERSION_SOURCE_DIR="$repo_root" \
    -DLOCKDC_VERSION_PROBE_OUTPUT="$version_file" \
    -P "$repo_root/tests/version_resolution_probe.cmake"
  IFS='|' read -r lockdc_version _ < "$version_file"
  rm -f "$version_file"
  trap - EXIT
}

run_cmake_script() {
  "$cmake_bin" "$@"
}

run_target() {
  local preset="$1"
  local build_dir="$repo_root/build/$preset"
  local deps_preset

  deps_preset="deps-${preset%-release}"
  "$deps_script" "$deps_preset"
  "$build_script" "$preset"
  "$cmake_bin" \
    -DLOCKDC_BINARY_DIR="$build_dir" \
    -DLOCKDC_ROOT="$repo_root" \
    -DLOCKDC_DIST_DIR="$repo_root/dist" \
    -P "$repo_root/cmake/package_archive.cmake"
}

run_target_if_osxcross() {
  local preset="$1"

  if "$script_dir/osxcross_available.sh"; then
    run_target "$preset"
    "$cmake_bin" \
      -DLOCKDC_BINARY_DIR="$repo_root/build/$preset" \
      -DLOCKDC_ROOT="$repo_root" \
      -DLOCKDC_DIST_DIR="$repo_root/dist" \
      -P "$repo_root/cmake/package_darwin_smoke_bundle.cmake"
  else
    printf '[package] skipping %s: osxcross toolchain not available\n' "$preset"
  fi
}

run_cmake_script -DLOCKDC_ROOT="$repo_root" -DLOCKDC_DIST_DIR="$repo_root/dist" -P "$repo_root/cmake/package_clean_dist.cmake"

package_lua=0

case "$requested_abi" in
  all)
    run_target x86_64-linux-gnu-release
    run_target x86_64-linux-musl-release
    run_target aarch64-linux-gnu-release
    run_target aarch64-linux-musl-release
    run_target armhf-linux-gnu-release
    run_target armhf-linux-musl-release
    run_target_if_osxcross arm64-apple-darwin-release
    package_lua=1
    ;;
  gnu)
    run_target x86_64-linux-gnu-release
    run_target aarch64-linux-gnu-release
    run_target armhf-linux-gnu-release
    package_lua=1
    ;;
  musl)
    run_target x86_64-linux-musl-release
    run_target aarch64-linux-musl-release
    run_target armhf-linux-musl-release
    ;;
  x86_64-linux-gnu)
    run_target x86_64-linux-gnu-release
    package_lua=1
    ;;
  x86_64-linux-musl)
    run_target x86_64-linux-musl-release
    ;;
  aarch64-linux-gnu)
    run_target aarch64-linux-gnu-release
    ;;
  aarch64-linux-musl)
    run_target aarch64-linux-musl-release
    ;;
  armhf-linux-gnu)
    run_target armhf-linux-gnu-release
    ;;
  armhf-linux-musl)
    run_target armhf-linux-musl-release
    ;;
  arm64-apple-darwin)
    if ! "$script_dir/osxcross_available.sh"; then
      echo "missing arm64 Apple Darwin osxcross toolchain; set OSXCROSS_ROOT or install it under \$HOME/.local/cross/osxcross" >&2
      exit 1
    fi
    run_target_if_osxcross arm64-apple-darwin-release
    ;;
  *)
    echo "usage: scripts/package.sh [all|gnu|musl|x86_64-linux-gnu|x86_64-linux-musl|aarch64-linux-gnu|aarch64-linux-musl|armhf-linux-gnu|armhf-linux-musl|arm64-apple-darwin]" >&2
    exit 2
    ;;
esac

if [ -z "$lockdc_version" ]; then
  resolve_version
fi

if [ "$package_lua" -eq 1 ]; then
  run_cmake_script \
    -DLOCKDC_BINARY_DIR="$repo_root/build/x86_64-linux-gnu-release" \
    -DLOCKDC_ROOT="$repo_root" \
    -DLOCKDC_DIST_DIR="$repo_root/dist" \
    -P "$repo_root/cmake/package_lua_rock.cmake"
fi

run_cmake_script \
  -DLOCKDC_ROOT="$repo_root" \
  -DLOCKDC_DIST_DIR="$repo_root/dist" \
  -DLOCKDC_VERSION="$lockdc_version" \
  -P "$repo_root/cmake/package_checksums.cmake"

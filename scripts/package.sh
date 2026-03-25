#!/usr/bin/env bash
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
requested_abi=${1:-all}
cmake_bin=${CMAKE:-cmake}
build_script=${LOCKDC_BUILD_SCRIPT:-$script_dir/build.sh}
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

  "$build_script" "$preset"
  "$cmake_bin" \
    -DLOCKDC_BINARY_DIR="$build_dir" \
    -DLOCKDC_ROOT="$repo_root" \
    -DLOCKDC_DIST_DIR="$repo_root/dist" \
    -P "$repo_root/cmake/package_runtime.cmake"
  "$cmake_bin" \
    -DLOCKDC_BINARY_DIR="$build_dir" \
    -DLOCKDC_ROOT="$repo_root" \
    -DLOCKDC_DIST_DIR="$repo_root/dist" \
    -P "$repo_root/cmake/package_dev.cmake"
}

run_cmake_script -DLOCKDC_ROOT="$repo_root" -DLOCKDC_DIST_DIR="$repo_root/dist" -P "$repo_root/cmake/package_clean_dist.cmake"

case "$requested_abi" in
  all)
    run_target x86_64-linux-gnu-release
    run_target x86_64-linux-musl-release
    run_target aarch64-linux-gnu-release
    run_target aarch64-linux-musl-release
    run_target armhf-linux-gnu-release
    run_target armhf-linux-musl-release
    ;;
  gnu)
    run_target x86_64-linux-gnu-release
    run_target aarch64-linux-gnu-release
    run_target armhf-linux-gnu-release
    ;;
  musl)
    run_target x86_64-linux-musl-release
    run_target aarch64-linux-musl-release
    run_target armhf-linux-musl-release
    ;;
  x86_64-linux-gnu)
    run_target x86_64-linux-gnu-release
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
  *)
    echo "usage: scripts/package.sh [all|gnu|musl|x86_64-linux-gnu|x86_64-linux-musl|aarch64-linux-gnu|aarch64-linux-musl|armhf-linux-gnu|armhf-linux-musl]" >&2
    exit 2
    ;;
esac

if [ -z "$lockdc_version" ]; then
  resolve_version
fi

run_cmake_script \
  -DLOCKDC_ROOT="$repo_root" \
  -DLOCKDC_DIST_DIR="$repo_root/dist" \
  -DLOCKDC_VERSION="$lockdc_version" \
  -P "$repo_root/cmake/package_checksums.cmake"

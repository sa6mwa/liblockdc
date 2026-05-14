#!/usr/bin/env bash
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
mode=${1:-all}

cross_release_presets=(
  aarch64-linux-gnu-release
  aarch64-linux-musl-release
  armhf-linux-gnu-release
  armhf-linux-musl-release
)

cross_preset_package_regex='dist_dir_configure_test|package_archives_test|package_script_targeting_test|runtime_license_install_tree_test|pslog_build_modes_test'

unset LD_LIBRARY_PATH

ctest_timeout=${LOCKDC_CTEST_TIMEOUT:-300}

require_release_build_tree() {
  local preset="$1"
  local build_dir="$repo_root/build/$preset"
  local cache_file="$build_dir/CMakeCache.txt"

  if [ ! -f "$cache_file" ]; then
    printf '%s\n' "missing build tree for $preset at $build_dir; run scripts/cross_build.sh first" >&2
    return 1
  fi
}

run_cross_preset_package_isolation() {
  "$script_dir/build.sh" debug
  ctest --preset debug --output-on-failure --progress --stop-on-failure --timeout "$ctest_timeout" -R "$cross_preset_package_regex"
}

run_cross_release_matrix() {
  local preset

  for preset in "${cross_release_presets[@]}"; do
    require_release_build_tree "$preset"
    ctest --preset "$preset" --output-on-failure --progress --stop-on-failure --timeout "$ctest_timeout"
  done
}

case "$mode" in
  preset)
    run_cross_preset_package_isolation
    ;;
  release)
    run_cross_release_matrix
    ;;
  all)
    run_cross_preset_package_isolation
    run_cross_release_matrix
    ;;
  *)
    printf '%s\n' "usage: scripts/cross_test.sh [preset|release|all]" >&2
    exit 2
    ;;
esac

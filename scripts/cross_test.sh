#!/usr/bin/env bash
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
mode=${1:-all}
ctest_parallel_level=${CTEST_PARALLEL_LEVEL:-$(nproc)}

cross_release_presets=(
  aarch64-linux-gnu-release
  aarch64-linux-musl-release
  armhf-linux-gnu-release
  armhf-linux-musl-release
)

cross_preset_package_regex='dist_dir_configure_test|package_archives_test|package_script_targeting_test|runtime_license_fallback_test|pslog_build_modes_test'

unset LD_LIBRARY_PATH

run_ctest_background() {
  local out_var="$1"
  local preset="$2"
  local regex="$3"

  ctest --preset "$preset" --output-on-failure --parallel "$ctest_parallel_level" -R "$regex" &
  printf -v "$out_var" '%s' "$!"
}

wait_for_pid() {
  local pid="$1"
  local label="$2"

  if ! wait "$pid"; then
    printf '%s\n' "${label} failed" >&2
    return 1
  fi
}

run_cross_preset_package_isolation() {
  local debug_pid
  local asan_pid
  local rc=0

  "$script_dir/build.sh" debug
  "$script_dir/build.sh" asan

  run_ctest_background debug_pid debug "$cross_preset_package_regex"
  run_ctest_background asan_pid asan "$cross_preset_package_regex"

  wait_for_pid "$debug_pid" "debug cross-preset package isolation test" || rc=1
  wait_for_pid "$asan_pid" "asan cross-preset package isolation test" || rc=1

  return "$rc"
}

run_cross_release_matrix() {
  local preset

  for preset in "${cross_release_presets[@]}"; do
    "$script_dir/build.sh" "$preset"
    ctest --preset "$preset" --output-on-failure --parallel "$ctest_parallel_level"
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

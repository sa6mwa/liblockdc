#!/usr/bin/env bash
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)

presets=(
  x86_64-linux-gnu-release
  x86_64-linux-musl-release
)

unset LD_LIBRARY_PATH

ctest_timeout=${LOCKDC_CTEST_TIMEOUT:-300}

cd "$repo_root"

for preset in "${presets[@]}"; do
  "$script_dir/build.sh" "$preset"
  ctest --preset "$preset" --output-on-failure --progress --stop-on-failure --timeout "$ctest_timeout"
done

#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)

# Host-executable release testing always uses the pinned Bootlin x86_64 Linux
# targets. Do not select a compiler or dependency root from the ambient host.
presets=(
  x86_64-linux-gnu-release
  x86_64-linux-musl-release
)
deps_presets=(
  deps-x86_64-linux-gnu
  deps-x86_64-linux-musl
)

unset LD_LIBRARY_PATH

ctest_timeout=${LOCKDC_CTEST_TIMEOUT:-300}

cd "$repo_root"

if [ "${LOCKDC_HOST_TEST_DRY_RUN:-0}" = "1" ]; then
  for deps_preset in "${deps_presets[@]}"; do
    printf 'deps_preset=%s\n' "$deps_preset"
  done
  for preset in "${presets[@]}"; do
    printf 'preset=%s\n' "$preset"
  done
  exit 0
fi

for deps_preset in "${deps_presets[@]}"; do
  "$script_dir/deps.sh" "$deps_preset"
done

for preset in "${presets[@]}"; do
  "$script_dir/build.sh" "$preset"
  ctest --preset "$preset" --output-on-failure --progress --stop-on-failure \
    --timeout "$ctest_timeout" -LE lifecycle-host
done

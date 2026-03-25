#!/usr/bin/env bash
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

presets=(
  aarch64-linux-gnu-release
  aarch64-linux-musl-release
  armhf-linux-gnu-release
  armhf-linux-musl-release
)

unset LD_LIBRARY_PATH

for preset in "${presets[@]}"; do
  "$script_dir/build.sh" "$preset"
done

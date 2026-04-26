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

if "$script_dir/osxcross_available.sh"; then
  presets+=(arm64-apple-darwin-release)
else
  printf '[cross-build] skipping arm64-apple-darwin-release: osxcross toolchain not available\n'
fi

for preset in "${presets[@]}"; do
  "$script_dir/build.sh" "$preset"
done

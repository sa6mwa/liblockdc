#!/usr/bin/env bash
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
preset=${1:-debug}

unset LD_LIBRARY_PATH

case "$preset" in
  dev)
    preset=debug
    ;;
  test)
    preset=debug
    ;;
esac

case "$preset" in
  debug|e2e|release|x86_64-linux-gnu-release|x86_64-linux-musl-release|aarch64-linux-gnu-release|aarch64-linux-musl-release|armhf-linux-gnu-release|armhf-linux-musl-release|arm64-apple-darwin-release|asan|coverage|fuzz)
    ;;
  *)
    echo "usage: scripts/build.sh [debug|e2e|release|x86_64-linux-gnu-release|x86_64-linux-musl-release|aarch64-linux-gnu-release|aarch64-linux-musl-release|armhf-linux-gnu-release|armhf-linux-musl-release|arm64-apple-darwin-release|asan|coverage|fuzz]" >&2
    exit 2
    ;;
esac

cmake --preset "$preset"
cmake --build --preset "$preset"

#!/usr/bin/env bash

set -eu

script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
repo_root="$(CDPATH= cd -- "$script_dir/.." && pwd)"

require_command() {
    if ! command -v "$1" >/dev/null 2>&1; then
        printf 'missing required command: %s\n' "$1" >&2
        exit 1
    fi
}

require_file() {
    if [ ! -e "$1" ]; then
        printf 'missing required file: %s\n' "$1" >&2
        exit 1
    fi
}

run_target() {
    preset="$1"
    build_dir="$repo_root/build/$preset"

    printf '\n== %s ==\n' "$preset"
    cmake --preset "$preset"
    cmake --build --preset "$preset"
    ctest --preset "$preset"
    cmake -DLOCKDC_BINARY_DIR="$build_dir" -DLOCKDC_ROOT="$repo_root" -DLOCKDC_DIST_DIR="$repo_root/dist" -P "$repo_root/cmake/package_archive.cmake"
}

require_command cmake
require_command ctest
require_command musl-gcc
require_command aarch64-linux-gnu-gcc
require_command arm-linux-gnueabihf-gcc
require_command aarch64-linux-musl-gcc
require_command arm-linux-musleabihf-gcc
require_command qemu-aarch64
require_command qemu-arm

require_file "$HOME/.local/cross/aarch64-linux-musl/aarch64-linux-musl/lib/ld-musl-aarch64.so.1"
require_file "$HOME/.local/cross/arm-linux-musleabihf/arm-linux-musleabihf/lib/ld-musl-armhf.so.1"

cd "$repo_root"

cmake --preset x86_64-linux-gnu-release
cmake -DLOCKDC_ROOT="$repo_root" -DLOCKDC_DIST_DIR="$repo_root/dist" -P "$repo_root/cmake/package_clean_dist.cmake"

run_target x86_64-linux-gnu-release
run_target x86_64-linux-musl-release
run_target aarch64-linux-gnu-release
run_target aarch64-linux-musl-release
run_target armhf-linux-gnu-release
run_target armhf-linux-musl-release

cmake     -DLOCKDC_BINARY_DIR="$repo_root/build/x86_64-linux-gnu-release"     -DLOCKDC_ROOT="$repo_root"     -DLOCKDC_DIST_DIR="$repo_root/dist"     -P "$repo_root/cmake/package_lua_rock.cmake"

cmake -DLOCKDC_ROOT="$repo_root" -DLOCKDC_BINARY_DIR="$repo_root/build/x86_64-linux-gnu-release" -DLOCKDC_DIST_DIR="$repo_root/dist" -P "$repo_root/cmake/package_checksums.cmake"
cmake \
    -DLOCKDC_ROOT="$repo_root" \
    -DLOCKDC_DIST_DIR="$repo_root/dist" \
    -DLOCKDC_RELEASE_PRESETS="x86_64-linux-gnu-release;x86_64-linux-musl-release;aarch64-linux-gnu-release;aarch64-linux-musl-release;armhf-linux-gnu-release;armhf-linux-musl-release" \
    -P "$repo_root/tests/release_matrix_archives_test.cmake"

printf '\nLinux release matrix completed successfully.\n'

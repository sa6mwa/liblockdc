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

require_build_tree() {
    local preset="$1"
    local build_dir="$repo_root/build/$preset"
    local cache_file="$build_dir/CMakeCache.txt"

    if [ ! -f "$cache_file" ]; then
        printf 'missing required build tree for %s at %s\n' "$preset" "$build_dir" >&2
        exit 1
    fi
}

package_target() {
    local preset="$1"
    local build_dir="$repo_root/build/$preset"

    printf '\n== %s ==\n' "$preset"
    require_build_tree "$preset"
    cmake \
        -DLOCKDC_BINARY_DIR="$build_dir" \
        -DLOCKDC_ROOT="$repo_root" \
        -DLOCKDC_DIST_DIR="$repo_root/dist" \
        -P "$repo_root/cmake/package_archive.cmake"
}

require_command cmake

cd "$repo_root"

cmake -DLOCKDC_ROOT="$repo_root" -DLOCKDC_DIST_DIR="$repo_root/dist" -P "$repo_root/cmake/package_clean_dist.cmake"

package_target x86_64-linux-gnu-release
package_target x86_64-linux-musl-release
package_target aarch64-linux-gnu-release
package_target aarch64-linux-musl-release
package_target armhf-linux-gnu-release
package_target armhf-linux-musl-release

cmake -DLOCKDC_ROOT="$repo_root" -DLOCKDC_BINARY_DIR="$repo_root/build/x86_64-linux-gnu-release" -DLOCKDC_DIST_DIR="$repo_root/dist" -P "$repo_root/cmake/package_checksums.cmake"
cmake \
    -DLOCKDC_ROOT="$repo_root" \
    -DLOCKDC_DIST_DIR="$repo_root/dist" \
    -DLOCKDC_RELEASE_PRESETS="x86_64-linux-gnu-release;x86_64-linux-musl-release;aarch64-linux-gnu-release;aarch64-linux-musl-release;armhf-linux-gnu-release;armhf-linux-musl-release" \
    -P "$repo_root/tests/release_matrix_archives_test.cmake"

printf '\nLinux release package matrix completed successfully.\n'

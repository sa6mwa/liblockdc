#!/usr/bin/env bash

set -euo pipefail

script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
repo_root="$(CDPATH= cd -- "$script_dir/.." && pwd)"
timed_bin="$script_dir/run_timed.sh"

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
    "$timed_bin" "release-matrix package ${preset}" cmake \
        -DLOCKDC_BINARY_DIR="$build_dir" \
        -DLOCKDC_ROOT="$repo_root" \
        -DLOCKDC_DIST_DIR="$repo_root/dist" \
        -P "$repo_root/cmake/package_archive.cmake"
}

require_command cmake

cd "$repo_root"

release_presets=(
    x86_64-linux-gnu-release
    x86_64-linux-musl-release
    aarch64-linux-gnu-release
    aarch64-linux-musl-release
    armhf-linux-gnu-release
    armhf-linux-musl-release
)
if "$script_dir/osxcross_available.sh"; then
    release_presets+=(arm64-apple-darwin-release)
else
    printf '[package] skipping arm64-apple-darwin-release: osxcross toolchain not available\n'
fi
release_preset_list=$(IFS=';'; printf '%s' "${release_presets[*]}")

"$timed_bin" "release-matrix package clean-dist" cmake \
    -DLOCKDC_ROOT="$repo_root" -DLOCKDC_DIST_DIR="$repo_root/dist" \
    -P "$repo_root/cmake/package_clean_dist.cmake"

for release_preset in "${release_presets[@]}"; do
    package_target "$release_preset"
    if [ "$release_preset" = "arm64-apple-darwin-release" ]; then
        "$timed_bin" "release-matrix package ${release_preset} smoke" cmake \
            -DLOCKDC_BINARY_DIR="$repo_root/build/$release_preset" \
            -DLOCKDC_ROOT="$repo_root" \
            -DLOCKDC_DIST_DIR="$repo_root/dist" \
            -P "$repo_root/cmake/package_darwin_smoke_bundle.cmake"
    fi
done

# Source and Lua artifacts are produced from the native, pinned Bootlin GNU
# build; never select release artifacts from an ambient host compiler.
host_release_preset=x86_64-linux-gnu-release
"$timed_bin" "release-matrix package source" cmake \
    -DLOCKDC_ROOT="$repo_root" \
    -DLOCKDC_BINARY_DIR="$repo_root/build/$host_release_preset" \
    -DLOCKDC_DIST_DIR="$repo_root/dist" \
    -P "$repo_root/cmake/package_source.cmake"
"$timed_bin" "release-matrix package lua" cmake \
    -DLOCKDC_ROOT="$repo_root" \
    -DLOCKDC_BINARY_DIR="$repo_root/build/$host_release_preset" \
    -DLOCKDC_DIST_DIR="$repo_root/dist" \
    -P "$repo_root/cmake/package_lua_rock.cmake"
"$timed_bin" "release-matrix package checksums" cmake \
    -DLOCKDC_ROOT="$repo_root" \
    -DLOCKDC_BINARY_DIR="$repo_root/build/$host_release_preset" \
    -DLOCKDC_DIST_DIR="$repo_root/dist" \
    -P "$repo_root/cmake/package_checksums.cmake"
source_archive="$repo_root/dist/liblockdc-$(sed -n 's/^set(LOCKDC_VERSION "\(.*\)")$/\1/p' "$repo_root/build/$host_release_preset/package-metadata.cmake").tar.gz"
"$timed_bin" "release-matrix package source-smoke" \
    bash "$repo_root/scripts/test_release_from_source.sh" "$repo_root" "$source_archive"
"$timed_bin" "release-matrix package tarball-sdk-matrix" cmake \
    -DLOCKDC_ROOT="$repo_root" \
    -DLOCKDC_DIST_DIR="$repo_root/dist" \
    -DLOCKDC_USE_EXISTING_ARCHIVE=ON \
    -DLOCKDC_RELEASE_PRESETS="$release_preset_list" \
    -P "$repo_root/tests/release_tarball_sdk_matrix_test.cmake"
"$timed_bin" "release-matrix package tarball-sdk" cmake \
    -DLOCKDC_ROOT="$repo_root" \
    -DLOCKDC_BINARY_DIR="$repo_root/build/$host_release_preset" \
    -DLOCKDC_DIST_DIR="$repo_root/dist" \
    -DLOCKDC_USE_EXISTING_ARCHIVE=ON \
    -P "$repo_root/tests/release_tarball_sdk_test.cmake"
"$timed_bin" "release-matrix package lua-verify" cmake \
    -DLOCKDC_ROOT="$repo_root" \
    -DLOCKDC_BINARY_DIR="$repo_root/build/$host_release_preset" \
    -DLOCKDC_DIST_DIR="$repo_root/dist" \
    -DLOCKDC_USE_EXISTING_ARCHIVE=ON \
    -P "$repo_root/tests/lua_release_package_test.cmake"
"$timed_bin" "release-matrix package archive-verify" cmake \
    -DLOCKDC_ROOT="$repo_root" \
    -DLOCKDC_DIST_DIR="$repo_root/dist" \
    -DLOCKDC_VERIFY_WORK_DIR="$repo_root/build/release-matrix-verify" \
    -DLOCKDC_RELEASE_PRESETS="$release_preset_list" \
    -P "$repo_root/tests/release_matrix_archives_test.cmake"

printf '\nLinux release package matrix completed successfully.\n'

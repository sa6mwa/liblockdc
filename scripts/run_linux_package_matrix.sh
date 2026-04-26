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

detect_host_release_preset() {
    local cc_bin="${CC:-cc}"
    local triple

    if ! triple="$($cc_bin -dumpmachine 2>/dev/null)"; then
        printf 'failed to resolve native compiler triple with %s -dumpmachine\n' "$cc_bin" >&2
        exit 1
    fi

    case "$triple" in
      x86_64*-linux-musl*)
        printf '%s\n' "x86_64-linux-musl-release"
        ;;
      x86_64*-linux-gnu*|x86_64*-linux)
        printf '%s\n' "x86_64-linux-gnu-release"
        ;;
      aarch64*-linux-musl*)
        printf '%s\n' "aarch64-linux-musl-release"
        ;;
      aarch64*-linux-gnu*|aarch64*-linux)
        printf '%s\n' "aarch64-linux-gnu-release"
        ;;
      arm*-linux-musleabihf*|armv7*-linux-musleabihf*|arm*-linux-musl*|armv7*-linux-musl*)
        printf '%s\n' "armhf-linux-musl-release"
        ;;
      arm*-linux-gnueabihf*|armv7*-linux-gnueabihf*|arm*-linux-gnu*|armv7*-linux-gnu*)
        printf '%s\n' "armhf-linux-gnu-release"
        ;;
      *)
        printf 'unsupported native compiler triple for release tarball SDK test: %s\n' "$triple" >&2
        exit 1
        ;;
    esac
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

cmake -DLOCKDC_ROOT="$repo_root" -DLOCKDC_DIST_DIR="$repo_root/dist" -P "$repo_root/cmake/package_clean_dist.cmake"

for release_preset in "${release_presets[@]}"; do
    package_target "$release_preset"
    if [ "$release_preset" = "arm64-apple-darwin-release" ]; then
        cmake \
            -DLOCKDC_BINARY_DIR="$repo_root/build/$release_preset" \
            -DLOCKDC_ROOT="$repo_root" \
            -DLOCKDC_DIST_DIR="$repo_root/dist" \
            -P "$repo_root/cmake/package_darwin_smoke_bundle.cmake"
    fi
done

host_release_preset="$(detect_host_release_preset)"
cmake \
    -DLOCKDC_ROOT="$repo_root" \
    -DLOCKDC_BINARY_DIR="$repo_root/build/$host_release_preset" \
    -DLOCKDC_DIST_DIR="$repo_root/dist" \
    -P "$repo_root/cmake/package_lua_rock.cmake"
cmake \
    -DLOCKDC_ROOT="$repo_root" \
    -DLOCKDC_BINARY_DIR="$repo_root/build/$host_release_preset" \
    -DLOCKDC_DIST_DIR="$repo_root/dist" \
    -P "$repo_root/cmake/package_checksums.cmake"
cmake \
    -DLOCKDC_ROOT="$repo_root" \
    -DLOCKDC_DIST_DIR="$repo_root/dist" \
    -DLOCKDC_RELEASE_PRESETS="$release_preset_list" \
    -P "$repo_root/tests/release_matrix_archives_test.cmake"
cmake \
    -DLOCKDC_ROOT="$repo_root" \
    -DLOCKDC_DIST_DIR="$repo_root/dist" \
    -DLOCKDC_RELEASE_PRESETS="$release_preset_list" \
    -P "$repo_root/tests/release_tarball_sdk_matrix_test.cmake"
cmake \
    -DLOCKDC_ROOT="$repo_root" \
    -DLOCKDC_BINARY_DIR="$repo_root/build/$host_release_preset" \
    -DLOCKDC_DIST_DIR="$repo_root/dist" \
    -P "$repo_root/tests/release_tarball_sdk_test.cmake"
cmake \
    -DLOCKDC_ROOT="$repo_root" \
    -DLOCKDC_BINARY_DIR="$repo_root/build/$host_release_preset" \
    -DLOCKDC_DIST_DIR="$repo_root/dist" \
    -P "$repo_root/tests/lua_release_package_test.cmake"

printf '\nLinux release package matrix completed successfully.\n'

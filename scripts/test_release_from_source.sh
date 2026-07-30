#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd -P)

if [[ $# -ne 2 ]]; then
    printf 'usage: %s <repo-root> <source-tarball>\n' "$0" >&2
    exit 1
fi

repo_root_arg=$1
source_tarball=$2
smoke_root="$repo_root_arg/build/release-source-smoke"
extract_root="$smoke_root/extract"
build_root="$smoke_root/build"

detect_host_release_preset() {
    local cc_bin="${CC:-cc}"
    local triple

    if ! triple="$($cc_bin -dumpmachine 2>/dev/null)"; then
        printf 'test_release_from_source.sh: failed to resolve native compiler triple with %s -dumpmachine\n' "$cc_bin" >&2
        exit 1
    fi

    case "$triple" in
        x86_64*-linux-musl*)
            printf '%s\n' "x86_64-linux-musl"
            ;;
        x86_64*-linux-gnu*|x86_64*-linux)
            printf '%s\n' "x86_64-linux-gnu"
            ;;
        aarch64*-linux-musl*)
            printf '%s\n' "aarch64-linux-musl"
            ;;
        aarch64*-linux-gnu*|aarch64*-linux)
            printf '%s\n' "aarch64-linux-gnu"
            ;;
        arm*-linux-musleabihf*|armv7*-linux-musleabihf*|arm*-linux-musl*|armv7*-linux-musl*)
            printf '%s\n' "armhf-linux-musl"
            ;;
        arm*-linux-gnueabihf*|armv7*-linux-gnueabihf*|arm*-linux-gnu*|armv7*-linux-gnu*)
            printf '%s\n' "armhf-linux-gnu"
            ;;
        *)
            printf 'test_release_from_source.sh: unsupported native compiler triple: %s\n' "$triple" >&2
            exit 1
            ;;
    esac
}

if [[ ! -f "$source_tarball" ]]; then
    printf 'test_release_from_source.sh: source tarball not found: %s\n' \
        "$source_tarball" >&2
    exit 1
fi

case "$smoke_root" in
    /|"")
        printf 'test_release_from_source.sh: refusing to use unsafe smoke root\n' >&2
        exit 1
        ;;
esac

rm -rf "$smoke_root"
mkdir -p "$extract_root" "$build_root"

tar -xzf "$source_tarball" -C "$extract_root"

source_root=$(find "$extract_root" -mindepth 1 -maxdepth 1 -type d | sort | head -n 1)
if [[ -z "$source_root" ]]; then
    printf 'test_release_from_source.sh: failed to locate extracted source tree\n' >&2
    exit 1
fi
if [[ ! -f "$source_root/VERSION" ]]; then
    printf 'test_release_from_source.sh: extracted source tree is missing VERSION\n' >&2
    exit 1
fi
release_version=$(tr -d '[:space:]' <"$source_root/VERSION")
if [[ ! "$release_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    printf 'test_release_from_source.sh: invalid VERSION value: %s\n' \
        "$release_version" >&2
    exit 1
fi

host_target_id="$(detect_host_release_preset)"
external_root="$repo_root_arg/.cache/deps/$host_target_id"
dependency_build_root="$repo_root_arg/.cache/deps-build/$host_target_id"
if [[ ! -d "$external_root" ]]; then
    printf 'test_release_from_source.sh: dependency root is missing: %s\n' "$external_root" >&2
    printf 'Run scripts/deps.sh deps-%s before source archive smoke validation.\n' "$host_target_id" >&2
    exit 1
fi

cmake -S "$source_root" -B "$build_root" -G Ninja \
    -DLOCKDC_BUILD_DEPENDENCIES=OFF \
    -DLOCKDC_BUILD_E2E_TESTS=OFF \
    -DLOCKDC_BUILD_FUZZERS=OFF \
    -DLOCKDC_EXTERNAL_ROOT="$external_root" \
    -DLOCKDC_DEPENDENCY_BUILD_ROOT="$dependency_build_root"
configured_version=$(sed -n 's/^#define LC_VERSION_STRING "\(.*\)"/\1/p' \
    "$build_root/generated/include/lc/version.h")
if [[ "$configured_version" != "$release_version" ]]; then
    printf 'test_release_from_source.sh: configured version %s != VERSION %s\n' \
        "$configured_version" "$release_version" >&2
    exit 1
fi
pc_version=$(sed -n 's/^Version: //p' "$build_root/lockdc.pc")
if [[ "$pc_version" != "$release_version" ]]; then
    printf 'test_release_from_source.sh: pkg-config version %s != VERSION %s\n' \
        "$pc_version" "$release_version" >&2
    exit 1
fi
cmake --build "$build_root"
ctest --test-dir "$build_root" --output-on-failure --stop-on-failure -R '^lc_unit_'

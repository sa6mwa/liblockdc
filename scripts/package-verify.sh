#!/usr/bin/env bash
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
requested_abi=${1:-all}

unset LD_LIBRARY_PATH

run_verify() {
  preset="$1"

  "$script_dir/build.sh" "$preset"
  ctest --preset "$preset" -R '^(package_archives_test|runtime_license_fallback_test|version_resolution_test|c_only_configure_test)$'
}

"$script_dir/package.sh" "$requested_abi"

case "$requested_abi" in
  all)
    run_verify x86_64-linux-gnu-release
    run_verify x86_64-linux-musl-release
    run_verify aarch64-linux-gnu-release
    run_verify aarch64-linux-musl-release
    run_verify armhf-linux-gnu-release
    run_verify armhf-linux-musl-release
    ;;
  gnu)
    run_verify x86_64-linux-gnu-release
    run_verify aarch64-linux-gnu-release
    run_verify armhf-linux-gnu-release
    ;;
  musl)
    run_verify x86_64-linux-musl-release
    run_verify aarch64-linux-musl-release
    run_verify armhf-linux-musl-release
    ;;
  x86_64-linux-gnu)
    run_verify x86_64-linux-gnu-release
    ;;
  x86_64-linux-musl)
    run_verify x86_64-linux-musl-release
    ;;
  aarch64-linux-gnu)
    run_verify aarch64-linux-gnu-release
    ;;
  aarch64-linux-musl)
    run_verify aarch64-linux-musl-release
    ;;
  armhf-linux-gnu)
    run_verify armhf-linux-gnu-release
    ;;
  armhf-linux-musl)
    run_verify armhf-linux-musl-release
    ;;
  *)
    echo "usage: scripts/package-verify.sh [all|gnu|musl|x86_64-linux-gnu|x86_64-linux-musl|aarch64-linux-gnu|aarch64-linux-musl|armhf-linux-gnu|armhf-linux-musl]" >&2
    exit 2
    ;;
esac

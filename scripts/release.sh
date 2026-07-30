#!/usr/bin/env bash

set -eu

script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
repo_root="$(CDPATH= cd -- "$script_dir/.." && pwd)"
make_bin="${MAKE:-make}"
dry_run="${LOCKDC_RELEASE_DRY_RUN:-0}"

run_step() {
    step="$1"

    if [ "$dry_run" = "1" ]; then
        printf '[release] %s\n' "$step"
        return 0
    fi

    "$make_bin" "$step"
}

cd "$repo_root"

run_step __lifecycle-version-contract
run_step __clean
run_step __format
run_step __test-debug
run_step __test-host
run_step __cross-test
run_step __fuzz
run_step __benchmarks
run_step __release-package-only

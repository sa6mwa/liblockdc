#!/usr/bin/env bash

set -eu

script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
repo_root="$(CDPATH= cd -- "$script_dir/.." && pwd)"
make_bin="${MAKE:-make}"
dry_run="${LOCKDC_WORLD_DRY_RUN:-0}"
clang_bin="${LOCKDC_WORLD_CLANG_BIN:-clang}"

run_step() {
    step="$1"

    if [ "$dry_run" = "1" ]; then
        printf '[world] %s\n' "$step"
        return 0
    fi

    "$make_bin" "$step"
}

should_run_fuzz() {
    if command -v "$clang_bin" >/dev/null 2>&1; then
        return 0
    fi
    return 1
}

cd "$repo_root"

run_step __clean
run_step __test-debug
run_step __test-host
run_step __cross-test
if should_run_fuzz; then
    run_step __fuzz
else
    printf '[world] skipping __fuzz: clang not available (%s)\n' "$clang_bin"
fi
run_step __test-e2e
run_step __benchmarks
run_step __release-package-only

#!/usr/bin/env bash
set -euo pipefail

script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
repo_root="$(CDPATH= cd -- "$script_dir/.." && pwd -P)"
make_bin="${MAKE:-make}"
dry_run="${LOCKDC_RELEASE_DRY_RUN:-0}"
timed_bin="$repo_root/scripts/run_timed.sh"

run_step() {
    step="$1"

    if [ "$dry_run" = "1" ]; then
        printf '[release] %s\n' "$step"
        return 0
    fi

    "$timed_bin" "release $step" "$make_bin" "$step"
}

cd "$repo_root"

run_step __lifecycle-version-contract
run_step __clean
run_step __release-pipeline

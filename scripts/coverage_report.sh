#!/usr/bin/env bash

set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    printf 'usage: %s <build-dir> [report-dir]\n' "$0" >&2
    exit 1
fi

build_dir="$1"
report_dir="${2:-$build_dir/coverage-report}"

gcov_bin="${GCOV:-gcov}"

if ! command -v "$gcov_bin" >/dev/null 2>&1; then
    printf 'gcov not found: %s\n' "$gcov_bin" >&2
    exit 1
fi

mkdir -p "$report_dir"
find "$report_dir" -maxdepth 1 -type f -name '*.gcov' -delete

set -- "$build_dir"/CMakeFiles/lc_test_static.dir/src/*.gcda
if [ ! -e "$1" ]; then
    printf 'no coverage data found in %s\n' \
        "$build_dir/CMakeFiles/lc_test_static.dir/src" >&2
    printf 'run the coverage-instrumented tests first\n' >&2
    exit 1
fi

summary_file="$report_dir/summary.txt"

(
    cd "$report_dir"
    "$gcov_bin" -b "$@"
) | tee "$summary_file"

printf '\nCoverage summary written to %s\n' "$summary_file"
printf 'Per-file gcov reports written to %s\n' "$report_dir"

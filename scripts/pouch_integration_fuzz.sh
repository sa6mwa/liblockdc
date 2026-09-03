#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
timed_bin="$script_dir/run_timed.sh"
build_dir="$repo_root/build/pouch-integration-fuzz/tests/fuzz"
corpus_root="$repo_root/tests/fuzz/corpus"
mutation_budget=${1:-30}

case "$mutation_budget" in
  ''|*[!0-9]*)
    echo "usage: scripts/pouch_integration_fuzz.sh [non-negative-mutation-count]" >&2
    exit 2
    ;;
esac

unset LD_LIBRARY_PATH

"$timed_bin" "pouch integration fuzz build" "$script_dir/build.sh" pouch-integration-fuzz
"$timed_bin" "pouch integration fuzz runner regression" \
  ctest --preset pouch-integration-fuzz \
  -R '^lc_pouch_integration_mutation_runner_regression$'
"$timed_bin" "pouch integration fuzz lql" \
  "$build_dir/lc_pouch_integration_mutation_runner" \
  "$build_dir/lc_pouch_integration_lql_plan" \
  "$corpus_root/pouch_lql_plan" "$mutation_budget"
"$timed_bin" "pouch integration fuzz lifecycle" \
  "$build_dir/lc_pouch_integration_mutation_runner" \
  "$build_dir/lc_pouch_integration_lifecycle" \
  "$corpus_root/pouch_lifecycle" "$mutation_budget"

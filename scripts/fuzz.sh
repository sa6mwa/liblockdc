#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
timed_bin="$script_dir/run_timed.sh"
build_dir="$repo_root/build/fuzz/tests/fuzz"
source_corpus_root="$repo_root/tests/fuzz/corpus"
work_corpus_root="$repo_root/build/fuzz/corpus"
findings_root="$repo_root/build/fuzz/findings"
max_total_time=${1:-30}

# shellcheck source=assert_generated_path.sh
source "$script_dir/assert_generated_path.sh"

unset LD_LIBRARY_PATH

prepare_corpus() {
    local name=$1
    local src_dir="$source_corpus_root/$name"
    local dst_dir="$work_corpus_root/$name"

    lockdc_assert_generated_path "$repo_root" "$dst_dir"
    rm -rf "$dst_dir"
    mkdir -p "$dst_dir"
    cp -R "$src_dir/." "$dst_dir/"

    printf '%s\n' "$dst_dir"
}

"$timed_bin" "fuzz build" "$script_dir/build.sh" fuzz
"$timed_bin" "fuzz ctest" ctest --preset fuzz

afl_fuzz=$("$script_dir/cpkt-aflpp.sh" discover | sed -n 's/^afl_fuzz=//p')

run_fuzzer() {
    local name=$1
    local target=$2
    local corpus_dir
    local findings_dir

    corpus_dir=$(prepare_corpus "$name")
    findings_dir="$findings_root/$name"
    lockdc_assert_generated_path "$repo_root" "$findings_dir"
    rm -rf "$findings_dir"
    mkdir -p "$findings_dir"
    "$timed_bin" "fuzz $name" env \
        AFL_SKIP_CPUFREQ=1 AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1 AFL_NO_AFFINITY=1 \
        "$afl_fuzz" -V "$max_total_time" -i "$corpus_dir" -o "$findings_dir" -- "$build_dir/$target" @@
}

run_fuzzer streams lc_fuzz_streams
run_fuzzer bundle_open lc_fuzz_bundle_open
run_fuzzer attachment_decode lc_fuzz_attachment_decode
run_fuzzer queue_meta lc_fuzz_queue_meta
run_fuzzer query_keys_stream lc_fuzz_query_keys_stream
run_fuzzer mutate_parse lc_fuzz_mutate_parse
run_fuzzer mutate_apply lc_fuzz_mutate_apply
run_fuzzer pouch_record_header lc_fuzz_pouch_record_header
run_fuzzer pouch_index_primitives lc_fuzz_pouch_index_primitives

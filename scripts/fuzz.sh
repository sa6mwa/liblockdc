#!/usr/bin/env bash
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
build_dir="$repo_root/build/fuzz/tests/fuzz"
source_corpus_root="$repo_root/tests/fuzz/corpus"
work_corpus_root="$repo_root/build/fuzz/corpus"
max_total_time=${1:-30}

unset LD_LIBRARY_PATH

prepare_corpus() {
    local name=$1
    local src_dir="$source_corpus_root/$name"
    local dst_dir="$work_corpus_root/$name"

    rm -rf "$dst_dir"
    mkdir -p "$dst_dir"
    cp -R "$src_dir/." "$dst_dir/"

    printf '%s\n' "$dst_dir"
}

"$script_dir/build.sh" fuzz
ctest --preset fuzz

"$build_dir/lc_fuzz_streams" -max_total_time="$max_total_time" "$(prepare_corpus streams)"
"$build_dir/lc_fuzz_bundle_open" -max_total_time="$max_total_time" "$(prepare_corpus bundle_open)"
ASAN_OPTIONS=detect_leaks=0 "$build_dir/lc_fuzz_attachment_decode" -max_total_time="$max_total_time" "$(prepare_corpus attachment_decode)"
ASAN_OPTIONS=detect_leaks=0 "$build_dir/lc_fuzz_queue_meta" -max_total_time="$max_total_time" "$(prepare_corpus queue_meta)"
"$build_dir/lc_fuzz_mutate_parse" -max_total_time="$max_total_time" "$(prepare_corpus mutate_parse)"
"$build_dir/lc_fuzz_mutate_apply" -max_total_time="$max_total_time" "$(prepare_corpus mutate_apply)"

#!/usr/bin/env bash
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
mode=${1:-measure}
storage=${2:-"$root/build/collection-layout"}
runs=${COLLECTION_LAYOUT_RUNS:-3}
case "$storage" in /*) ;; *) echo 'storage directory must be absolute' >&2; exit 2 ;; esac
case "$runs" in 1|3|5|7|9) ;; *) echo 'COLLECTION_LAYOUT_RUNS must be 1, 3, 5, 7, or 9' >&2; exit 2 ;; esac
case "$mode" in
  smoke|pull-smoke) preset=debug ;;
  measure|pull) preset=x86_64-linux-gnu-release ;;
  *) echo 'usage: compare_collection_layouts.sh [measure|smoke|pull|pull-smoke] [DIRECTORY]' >&2; exit 2 ;;
esac
cd "$root"
cmake --preset "$preset"
cmake --build --preset "$preset" --target lockdc_collection_layout_probe
mkdir -p -- "$storage"
probe="$root/build/$preset/bench/lockdc_collection_layout_probe"
source_chunk=16384
source_delay_us=0
compact=1

run_probe() {
  local name=$1 variant=$2 rows=$3 bytes=$4 limit=$5 writers=$6 sync=$7 index=$8 sample=$9
  local log="$storage/$name-$variant-$sample.log"
  printf 'profile=%s mode=%s sample=%s\n' "$name" "$variant" "$sample"
  if ! LOCKDC_TEST_ALLOCATOR_TRACKING=$tracking \
      LOCKDC_LAYOUT_SOURCE_CHUNK=$source_chunk LOCKDC_LAYOUT_SOURCE_DELAY_US=$source_delay_us \
      LOCKDC_LAYOUT_COMPACT=$compact "$probe" "$variant" "$rows" "$bytes" \
      "$limit" "$writers" "$sync" "$index" "$storage" > "$log" 2>&1; then
    cat "$log" >&2
    exit 1
  fi
  if rg -q 'liblockdc allocator:|ERROR: AddressSanitizer|runtime error:' "$log"; then cat "$log" >&2; exit 1; fi
}

if [ "$mode" = pull-smoke ]; then
  tracking=1
  compact=0
  for variant in direct-pull staged-pull; do
    run_probe memory "$variant" 16 1024 65536 1 0 0 1
    run_probe boundary "$variant" 16 65536 65536 4 1 0 1
    run_probe spill "$variant" 16 131072 65536 4 0 0 1
    source_chunk=997; source_delay_us=50
    run_probe delayed "$variant" 16 8193 65536 2 0 0 1
    source_chunk=1; source_delay_us=0
    run_probe fragments "$variant" 64 64 65536 4 0 1 1
    source_chunk=16384
  done
  echo 'All ten pull comparison smoke cases passed, with allocation tracking.'
  exit 0
fi

if [ "$mode" = smoke ]; then
  tracking=1
  for variant in raw pull push buffered; do
    run_probe smoke "$variant" 16 1024 65536 1 0 0 1
  done
  run_probe spill buffered 32 4096 1024 4 1 0 1
  run_probe boundary buffered 64 1024 1024 4 0 1 1
  run_probe above-boundary buffered 16 1025 1024 4 0 0 1
  run_probe disk-only buffered 16 1024 0 1 0 0 1
  echo 'All eight layout smoke cases passed, with allocation tracking.'
  exit 0
fi

tracking=0
if [ "$mode" = pull ]; then
  compact=0
  pull_profile() {
    local name=$1 rows=$2 bytes=$3 writers=$4 sync=$5 sample=$8 variant
    source_chunk=$6; source_delay_us=$7
    for variant in "${variants[@]}"; do
      run_probe "$name" "$variant" "$rows" "$bytes" 65536 "$writers" "$sync" 0 "$sample"
    done
  }
  for sample in $(seq 1 "$runs"); do
    variants=(direct-pull staged-pull)
    if ((sample % 2 == 0)); then variants=(staged-pull direct-pull); fi
    pull_profile small-fast-1w 1024 1024 1 0 16384 0 "$sample"
    pull_profile small-fast-4w 1024 1024 4 0 16384 0 "$sample"
    pull_profile small-durable-4w 256 1024 4 1 16384 0 "$sample"
    pull_profile large-fast-1w 256 131072 1 0 16384 0 "$sample"
    pull_profile large-fast-4w 256 131072 4 0 16384 0 "$sample"
    pull_profile large-durable-4w 128 131072 4 1 16384 0 "$sample"
    pull_profile slow-small-1w 128 1024 1 0 256 100 "$sample"
    pull_profile slow-small-4w 128 1024 4 0 256 100 "$sample"
    pull_profile slow-large-1w 128 131072 1 0 4096 100 "$sample"
    pull_profile slow-large-4w 128 131072 4 0 4096 100 "$sample"
  done
else
  for sample in $(seq 1 "$runs"); do
    # Alternate order to avoid consistently giving one layout the first run.
    variants=(raw pull push buffered)
    if ((sample % 2 == 0)); then variants=(buffered push pull raw); fi
    for variant in "${variants[@]}"; do
      run_probe small-nosync "$variant" 256 1024 65536 1 0 0 "$sample"
      run_probe small-durable "$variant" 256 1024 65536 4 1 0 "$sample"
      run_probe spill-nosync "$variant" 256 131072 65536 4 0 0 "$sample"
    done
    for variant in raw pull buffered; do
      run_probe read-scale "$variant" 8192 4096 65536 1 0 0 "$sample"
    done
  done
  for variant in pull buffered; do
    run_probe indexed "$variant" 4096 1024 65536 1 0 1 1
  done
fi

metric() {
  awk -v phase="$2" -v key="$3" '
    $1 == "phase=" phase {
      for (i=2; i<=NF; ++i) {
        split($i, item, "=")
        if (item[1] == key) { print item[2]; found=1; exit }
      }
    }
    END { if (!found) exit 2 }
  ' "$1"
}
median() {
  local profile=$1 variant=$2 phase=$3 key=$4 count=$runs sample value
  if [ "$profile" = indexed ]; then count=1; fi
  for sample in $(seq 1 "$count"); do
    value=$(metric "$storage/$profile-$variant-$sample.log" "$phase" "$key") || return
    printf '%s\n' "$value"
  done | sort -n | awk '{ x[NR]=$1 } END { print x[(NR+1)/2] }'
}
if [ "$mode" = pull ]; then
  summary="$storage/pull-summary.tsv"
  printf 'profile\tmode\tappend_us\tappend_cpu_s\tp50_us\tp95_us\tp99_us\tappend_kernel_write_bytes\tcanonical\n' > "$summary"
  for profile in small-fast-1w small-fast-4w small-durable-4w large-fast-1w large-fast-4w large-durable-4w slow-small-1w slow-small-4w slow-large-1w slow-large-4w; do
    for variant in direct-pull staged-pull; do
      append=$(median "$profile" "$variant" append per_op_us)
      cpu=$(median "$profile" "$variant" append cpu_seconds)
      p50=$(median "$profile" "$variant" latency p50_us)
      p95=$(median "$profile" "$variant" latency p95_us)
      p99=$(median "$profile" "$variant" latency p99_us)
      writes=$(median "$profile" "$variant" append_io kernel_write_bytes)
      canonical=$(median "$profile" "$variant" files_before canonical)
      printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$profile" "$variant" "$append" "$cpu" "$p50" "$p95" "$p99" "$writes" "$canonical" >> "$summary"
    done
  done
  cat "$summary"
  printf 'Results: %s\n' "$summary"
  exit 0
fi
summary="$storage/summary.tsv"
printf 'profile\tmode\tappend_us\tappend_cpu_s\tcold_MiBps\twarm_MiBps\tfiles\tcanonical\tquery_pages\tcompaction_ms\tpacked_cold_MiBps\tappend_kernel_write_bytes\n' > "$summary"
for profile in small-nosync small-durable spill-nosync read-scale indexed; do
  variants=(raw pull push buffered)
  if [ "$profile" = read-scale ]; then variants=(raw pull buffered); fi
  if [ "$profile" = indexed ]; then variants=(pull buffered); fi
  for variant in "${variants[@]}"; do
    log="$storage/$profile-$variant-1.log"
    compact=NA; packed=NA
    if [ "$variant" != raw ]; then
      compact=$(median "$profile" "$variant" forced_compaction seconds | awk '{printf "%.3f", $1*1000}')
      packed=$(median "$profile" "$variant" read_packed_advised_cold MiBps)
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$profile" "$variant" \
      "$(median "$profile" "$variant" append per_op_us)" \
      "$(median "$profile" "$variant" append cpu_seconds)" \
      "$(median "$profile" "$variant" read_advised_cold MiBps)" \
      "$(median "$profile" "$variant" read_warm MiBps)" \
      "$(metric "$log" files_before files)" \
      "$(metric "$log" files_before canonical)" \
      "$(metric "$log" files_before query_pages)" "$compact" "$packed" \
      "$(median "$profile" "$variant" append_io kernel_write_bytes)" >> "$summary"
  done
done
cat "$summary"
printf 'Results: %s\n' "$summary"

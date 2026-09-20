#!/usr/bin/env bash

set -euo pipefail

if [ "$#" -lt 2 ]; then
    printf 'usage: %s <label> <command> [args...]\n' "$0" >&2
    exit 2
fi

label="$1"
shift

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd -P)
timing_depth=${LOCKDC_TIMING_DEPTH:-0}
timing_log=${LOCKDC_TIMING_LOG:-}
timing_root=0

if [ -z "$timing_log" ]; then
    # Lifecycle clean removes every repository-local generated directory.
    # Keep the outer record in the system temporary area so it survives the
    # complete command without leaving repository scratch behind.
    timing_dir=${LOCKDC_TIMING_DIR:-"${TMPDIR:-/tmp}/liblockdc-timings"}
    if mkdir -p "$timing_dir" 2>/dev/null; then
        timing_log="$timing_dir/run-$(date -u +%Y%m%dT%H%M%SZ)-$$.tsv"
        printf 'started_at\tfinished_at\tlabel\telapsed_seconds\tstatus\tdepth\n' > "$timing_log"
        timing_root=1
    else
        timing_log=""
        printf '[timing] unable to create timing log directory: %s\n' "$timing_dir" >&2
    fi
fi

start_time="$(date +%s)"
start_timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf '[timing %s] start %s\n' "$start_timestamp" "$label" >&2

if [ -n "$timing_log" ]; then
    if LOCKDC_TIMING_LOG="$timing_log" LOCKDC_TIMING_DEPTH="$((timing_depth + 1))" "$@"; then
        status=0
    else
        status=$?
    fi
elif "$@"; then
    status=0
else
    status=$?
fi

end_time="$(date +%s)"
end_timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
elapsed="$((end_time - start_time))"
hours="$((elapsed / 3600))"
minutes="$(((elapsed % 3600) / 60))"
seconds="$((elapsed % 60))"

if [ "$hours" -gt 0 ]; then
    duration="$(printf '%dh%02dm%02ds' "$hours" "$minutes" "$seconds")"
elif [ "$minutes" -gt 0 ]; then
    duration="$(printf '%dm%02ds' "$minutes" "$seconds")"
else
    duration="$(printf '%ds' "$seconds")"
fi

if [ -n "$timing_log" ]; then
    # `clean` is itself timed and legitimately removes the default build/
    # timing directory. Logging is diagnostic only: never let a vanished log
    # turn an otherwise successful wrapped command into a failed lifecycle
    # step.
    if ! printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$start_timestamp" "$end_timestamp" "$label" "$elapsed" "$status" "$timing_depth" \
        >> "$timing_log"; then
        printf '[timing] log disappeared before completion: %s\n' "$timing_log" >&2
        timing_log=""
        timing_root=0
    fi
fi

if [ "$status" -eq 0 ]; then
    printf '[timing %s] %s completed in %s\n' "$end_timestamp" "$label" "$duration"
else
    printf '[timing %s] %s failed after %s\n' "$end_timestamp" "$label" "$duration" >&2
fi

if [ "$timing_root" -eq 1 ]; then
    printf '[timing] log: %s\n' "$timing_log"
fi

exit "$status"

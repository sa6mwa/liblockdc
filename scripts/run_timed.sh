#!/usr/bin/env bash

set -eu

if [ "$#" -lt 2 ]; then
    printf 'usage: %s <label> <command> [args...]\n' "$0" >&2
    exit 2
fi

label="$1"
shift

start_time="$(date +%s)"

if "$@"; then
    status=0
else
    status=$?
fi

end_time="$(date +%s)"
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

if [ "$status" -eq 0 ]; then
    printf '[timing] %s completed in %s\n' "$label" "$duration"
else
    printf '[timing] %s failed after %s\n' "$label" "$duration" >&2
fi

exit "$status"

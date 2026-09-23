#!/usr/bin/env bash

set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=${1:-$(CDPATH= cd -- "$script_dir/.." && pwd)}
formatter=${CLANG_FORMAT:-clang-format}

if [ ! -d "$repo_root" ]; then
  printf 'format root is not a directory: %s\n' "$repo_root" >&2
  exit 2
fi

if [[ "$formatter" == */* ]]; then
  if [ ! -x "$formatter" ]; then
    printf 'clang-format is not executable: %s\n' "$formatter" >&2
    exit 2
  fi
elif ! command -v "$formatter" >/dev/null 2>&1; then
  printf 'clang-format is not available: %s\n' "$formatter" >&2
  exit 2
fi

format_dir=$(mktemp -d "${TMPDIR:-/tmp}/lockdc-format.XXXXXX")
trap 'rm -rf "$format_dir"' EXIT

index=0
while IFS= read -r source; do
  index=$((index + 1))
  formatted="$format_dir/$index"
  "$formatter" "$repo_root/$source" > "$formatted"
  if ! cmp -s "$repo_root/$source" "$formatted"; then
    cp "$formatted" "$repo_root/$source"
  fi
done < <(cd "$repo_root" && rg --files -g '*.c' -g '*.h')

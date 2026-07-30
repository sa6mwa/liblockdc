#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd -P)
dist_dir=${LOCKDC_DIST_DIR:-$repo_root/dist}

if [ ! -d "$dist_dir" ]; then
  printf 'verify_release_privacy: dist directory not found: %s\n' "$dist_dir" >&2
  exit 1
fi

manifest=$(find "$dist_dir" -maxdepth 1 -type f -name 'liblockdc-*-CHECKSUMS' | sort | tail -n 1)
if [ -z "$manifest" ]; then
  printf 'verify_release_privacy: no liblockdc-*-CHECKSUMS manifest found in %s\n' "$dist_dir" >&2
  exit 1
fi

scan_paths=$manifest
while read -r _hash artifact_name; do
  if [ -z "${artifact_name:-}" ]; then
    continue
  fi
  case "$artifact_name" in
    /*)
      artifact_path=$artifact_name
      ;;
    *)
      artifact_path="$dist_dir/$artifact_name"
      ;;
  esac
  if [ ! -e "$artifact_path" ]; then
    printf 'verify_release_privacy: checksum-listed artifact is missing: %s\n' "$artifact_path" >&2
    exit 1
  fi
  scan_paths="${scan_paths};${artifact_path}"
done <"$manifest"

cmake \
  -DLOCKDC_ROOT="$repo_root" \
  -DLOCKDC_SCAN_PATHS="$scan_paths" \
  -DLOCKDC_SCAN_LABEL='checksum-listed release artifact' \
  -P "$repo_root/tests/release_privacy_scan.cmake"

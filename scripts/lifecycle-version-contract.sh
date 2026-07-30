#!/usr/bin/env bash
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
cmake_bin=${CMAKE:-cmake}
reserved_tag=v99.99.99
dry_run=${LOCKDC_LIFECYCLE_VERSION_CONTRACT_DRY_RUN:-0}

if [ "$dry_run" = "1" ]; then
  printf '[lifecycle-version-contract] reserved_tag=%s\n' "$reserved_tag"
  printf '[lifecycle-version-contract] source_dir=%s\n' "$repo_root"
  exit 0
fi

cleanup_reserved_tag() {
  git -C "$repo_root" tag -d "$reserved_tag" >/dev/null 2>&1 || true
}

version_probe() {
  output_file=$1
  "$cmake_bin" \
    -DLOCKDC_ROOT="$repo_root" \
    -DLOCKDC_VERSION_SOURCE_DIR="$repo_root" \
    -DLOCKDC_VERSION_PROBE_OUTPUT="$output_file" \
    -P "$repo_root/tests/version_resolution_probe.cmake"
}

cleanup_reserved_tag

exact_release_tags=$(git -C "$repo_root" tag --points-at HEAD --list 'v[0-9]*.[0-9]*.[0-9]*' || true)
exact_lightweight_count=0
exact_lightweight_tag=

if [ -n "$exact_release_tags" ]; then
  while IFS= read -r tag; do
    case "$tag" in
      v[0-9]*.[0-9]*.[0-9]*)
        ;;
      *)
        continue
        ;;
    esac
    if ! printf '%s\n' "$tag" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+$'; then
      continue
    fi
    tag_type=$(git -C "$repo_root" cat-file -t "$tag")
    if [ "$tag_type" != "commit" ]; then
      printf 'lifecycle-version-contract: exact release tag %s must be lightweight, got %s\n' "$tag" "$tag_type" >&2
      exit 1
    fi
    exact_lightweight_count=$((exact_lightweight_count + 1))
    exact_lightweight_tag=$tag
  done <<EOF
$exact_release_tags
EOF
fi

if [ "$exact_lightweight_count" -gt 1 ]; then
  printf 'lifecycle-version-contract: multiple exact lightweight release tags point at HEAD\n' >&2
  exit 1
fi

if [ "$exact_lightweight_count" -eq 1 ]; then
  version_file=$(mktemp)
  trap 'rm -f "$version_file"' EXIT
  version_probe "$version_file"
  expected_version=${exact_lightweight_tag#v}
  actual_version=$(cut -d '|' -f 1 <"$version_file")
  if [ "$actual_version" != "$expected_version" ]; then
    printf 'lifecycle-version-contract: version probe resolved %s, expected %s\n' "$actual_version" "$expected_version" >&2
    exit 1
  fi
  exit 0
fi

trap 'cleanup_reserved_tag' EXIT HUP INT TERM
git -C "$repo_root" -c tag.gpgSign=false tag "$reserved_tag"
tag_type=$(git -C "$repo_root" cat-file -t "$reserved_tag")
if [ "$tag_type" != "commit" ]; then
  printf 'lifecycle-version-contract: reserved tag %s must be lightweight, got %s\n' "$reserved_tag" "$tag_type" >&2
  exit 1
fi

version_file=$(mktemp)
trap 'rm -f "$version_file"; cleanup_reserved_tag' EXIT HUP INT TERM
version_probe "$version_file"
actual_version=$(cut -d '|' -f 1 <"$version_file")
if [ "$actual_version" != "99.99.99" ]; then
  printf 'lifecycle-version-contract: reserved tag probe resolved %s, expected 99.99.99\n' "$actual_version" >&2
  exit 1
fi

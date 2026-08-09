#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd -P)
make_bin=${MAKE:-make}
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

print_release_version() {
  "$make_bin" -s -C "$repo_root" print-release-version
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
  expected_version=${exact_lightweight_tag#v}
  actual_version=$(print_release_version)
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

actual_version=$(print_release_version)
if [ "$actual_version" != "99.99.99" ]; then
  printf 'lifecycle-version-contract: reserved tag probe resolved %s, expected 99.99.99\n' "$actual_version" >&2
  exit 1
fi

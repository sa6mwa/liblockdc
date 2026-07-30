#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd -P)
source_dir=${LOCKDC_VERSION_SOURCE_DIR:-$repo_root}
override=${LOCKDC_VERSION_OVERRIDE:-}

fail() {
  printf 'release_version: %s\n' "$*" >&2
  exit 1
}

validate_semver() {
  printf '%s\n' "$1" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$'
}

resolve_git_version() {
  local exact_tags exact_count exact_tag tag tag_type

  exact_tags=$(git -C "$source_dir" tag --points-at HEAD --list 'v[0-9]*.[0-9]*.[0-9]*' || true)
  exact_count=0
  exact_tag=
  if [ -n "$exact_tags" ]; then
    while IFS= read -r tag; do
      if ! printf '%s\n' "$tag" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+$'; then
        continue
      fi
      tag_type=$(git -C "$source_dir" cat-file -t "$tag")
      if [ "$tag_type" != "commit" ]; then
        fail "exact release tag $tag must be a lightweight tag that resolves directly to a commit"
      fi
      exact_count=$((exact_count + 1))
      exact_tag=$tag
    done <<EOF
$exact_tags
EOF
  fi

  if [ "$exact_count" -gt 1 ]; then
    fail "multiple exact lightweight release tags point at HEAD"
  fi
  if [ "$exact_count" -eq 1 ]; then
    printf '%s\n' "${exact_tag#v}"
    return 0
  fi
  if [ -n "$override" ]; then
    validate_semver "$override" || fail 'LOCKDC_VERSION_OVERRIDE must be a semantic version like 0.1.0'
    printf '%s\n' "$override"
    return 0
  fi
  printf '%s\n' '0.0.0'
}

source_real=$(CDPATH= cd -- "$source_dir" && pwd -P)
git_toplevel=
if git_toplevel=$(git -C "$source_dir" rev-parse --show-toplevel 2>/dev/null); then
  git_toplevel=$(CDPATH= cd -- "$git_toplevel" && pwd -P)
fi

if [ -n "$git_toplevel" ] && [ "$git_toplevel" = "$source_real" ]; then
  resolve_git_version
elif [ -n "$override" ]; then
  validate_semver "$override" || fail 'LOCKDC_VERSION_OVERRIDE must be a semantic version like 0.1.0'
  printf '%s\n' "$override"
elif [ -f "$source_dir/VERSION" ]; then
  version=$(tr -d '[:space:]' <"$source_dir/VERSION")
  validate_semver "$version" || fail 'VERSION must contain a semantic version like 0.1.0'
  printf '%s\n' "$version"
else
  printf '%s\n' '0.0.0'
fi

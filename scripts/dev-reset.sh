#!/usr/bin/env bash
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
compose_project=liblockdc-e2e

container_engine() {
  if command -v nerdctl >/dev/null 2>&1; then
    printf '%s\n' nerdctl
    return 0
  fi
  if command -v docker >/dev/null 2>&1; then
    printf '%s\n' docker
    return 0
  fi
  return 1
}

remove_if_present() {
  if [ -e "$1" ]; then
    rm -rf -- "$1"
  fi
}

"$script_dir/dev-down.sh" >/dev/null 2>&1 || true

engine=$(container_engine || true)
if [ -n "${engine:-}" ]; then
  ids=$("$engine" ps -aq --filter "label=com.docker.compose.project=$compose_project" 2>/dev/null || true)
  if [ -n "$ids" ]; then
    # shellcheck disable=SC2086
    "$engine" rm -f $ids >/dev/null 2>&1 || true
  fi
  "$engine" network rm "${compose_project}_default" >/dev/null 2>&1 || true
fi

remove_if_present "$repo_root/devenv/volumes/lockd-disk-a-config"
remove_if_present "$repo_root/devenv/volumes/lockd-disk-b-config"
remove_if_present "$repo_root/devenv/volumes/lockd-disk-shared"
remove_if_present "$repo_root/devenv/volumes/lockd-mem-config"
remove_if_present "$repo_root/devenv/volumes/lockd-mem-run"
remove_if_present "$repo_root/devenv/volumes/lockd-s3-config"
remove_if_present "$repo_root/devenv/volumes/minio-data/.minio.sys"
remove_if_present "$repo_root/devenv/volumes/minio-data/lockd-client-s3/liblockdc"

mkdir -p "$repo_root/devenv/volumes/minio-data/lockd-client-s3"

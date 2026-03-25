#!/usr/bin/env bash
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)

remove_if_present() {
  if [ -e "$1" ]; then
    rm -rf -- "$1"
  fi
}

"$script_dir/dev-down.sh" >/dev/null 2>&1 || true

remove_if_present "$repo_root/devenv/volumes/lockd-disk-a-config"
remove_if_present "$repo_root/devenv/volumes/lockd-disk-b-config"
remove_if_present "$repo_root/devenv/volumes/lockd-disk-shared"
remove_if_present "$repo_root/devenv/volumes/lockd-mem-config"
remove_if_present "$repo_root/devenv/volumes/lockd-mem-run"
remove_if_present "$repo_root/devenv/volumes/lockd-s3-config"
remove_if_present "$repo_root/devenv/volumes/minio-data/.minio.sys"
remove_if_present "$repo_root/devenv/volumes/minio-data/lockd-client-s3/liblockdc"

mkdir -p "$repo_root/devenv/volumes/minio-data/lockd-client-s3"

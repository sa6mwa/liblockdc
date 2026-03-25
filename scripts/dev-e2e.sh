#!/usr/bin/env bash
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)

export LOCKDC_E2E_DISK_ENDPOINT=${LOCKDC_E2E_DISK_ENDPOINT:-https://localhost:${LOCKDC_DISK_A_PORT:-19441}}
export LOCKDC_E2E_DISK_BUNDLE=${LOCKDC_E2E_DISK_BUNDLE:-$repo_root/devenv/volumes/lockd-disk-a-config/client.pem}
export LOCKDC_E2E_S3_ENDPOINT=${LOCKDC_E2E_S3_ENDPOINT:-https://localhost:${LOCKDC_S3_PORT:-19443}}
export LOCKDC_E2E_S3_BUNDLE=${LOCKDC_E2E_S3_BUNDLE:-$repo_root/devenv/volumes/lockd-s3-config/client.pem}
export LOCKDC_E2E_MEM_SOCKET=${LOCKDC_E2E_MEM_SOCKET:-$repo_root/devenv/volumes/lockd-mem-run/lockd.sock}

"$script_dir/test.sh" e2e

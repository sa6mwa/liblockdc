#!/usr/bin/env bash
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
compose="$script_dir/compose.sh"
disk_a_port=${LOCKDC_DISK_A_PORT:-19441}
disk_b_port=${LOCKDC_DISK_B_PORT:-19442}
s3_port=${LOCKDC_S3_PORT:-19443}
minio_api_port=${LOCKDC_MINIO_API_PORT:-19000}
minio_console_port=${LOCKDC_MINIO_CONSOLE_PORT:-19001}

mkdir -p \
  "$repo_root/devenv/volumes/lockd-disk-a-config" \
  "$repo_root/devenv/volumes/lockd-s3-config" \
  "$repo_root/devenv/volumes/lockd-mem-config" \
  "$repo_root/devenv/volumes/lockd-disk-shared" \
  "$repo_root/devenv/volumes/lockd-mem-run" \
  "$repo_root/devenv/volumes/minio-data/lockd-client-s3"

wait_for_file() {
  path=$1
  label=$2
  count=0
  while [ ! -s "$path" ]; do
    count=$((count + 1))
    if [ "$count" -ge 60 ]; then
      printf '%s\n' "Timed out waiting for $label at $path" >&2
      exit 1
    fi
    sleep 1
  done
}

wait_for_socket() {
  path=$1
  label=$2
  count=0
  while [ ! -S "$path" ]; do
    count=$((count + 1))
    if [ "$count" -ge 60 ]; then
      printf '%s\n' "Timed out waiting for $label at $path" >&2
      exit 1
    fi
    sleep 1
  done
}

wait_for_http() {
  url=$1
  label=$2
  count=0
  while ! curl -fsS "$url" >/dev/null 2>&1; do
    count=$((count + 1))
    if [ "$count" -ge 60 ]; then
      printf '%s\n' "Timed out waiting for $label at $url" >&2
      exit 1
    fi
    sleep 1
  done
}

"$compose" up -d --remove-orphans

wait_for_http "http://127.0.0.1:$minio_api_port/minio/health/live" "minio"

wait_for_file "$repo_root/devenv/volumes/lockd-disk-a-config/client.pem" "disk-a client bundle"
wait_for_file "$repo_root/devenv/volumes/lockd-s3-config/client.pem" "s3 client bundle"
wait_for_socket "$repo_root/devenv/volumes/lockd-mem-run/lockd.sock" "mem uds socket"

cat <<EOF
liblockdc devenv is up.

HTTP endpoints:
  disk-a: https://127.0.0.1:$disk_a_port
  disk-b: https://127.0.0.1:$disk_b_port
  s3:     https://127.0.0.1:$s3_port

UDS endpoint:
  mem:    unix://./devenv/volumes/lockd-mem-run/lockd.sock

Generated client bundles:
  disk-a: ./devenv/volumes/lockd-disk-a-config/client.pem
  s3:     ./devenv/volumes/lockd-s3-config/client.pem

MinIO:
  API:     http://127.0.0.1:$minio_api_port
  Console: http://127.0.0.1:$minio_console_port
  Bucket:  lockd-client-s3
EOF

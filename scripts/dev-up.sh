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

bring_up_services() {
  "$compose" up -d --no-recreate "$@"
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

wait_for_service_exited_success() {
  service=$1
  label=$2
  count=0
  while :; do
    if "$compose" ps -a | grep -E "[[:space:]]$service[[:space:]]+Exited \\(0\\)" >/dev/null 2>&1; then
      return 0
    fi
    count=$((count + 1))
    if [ "$count" -ge 60 ]; then
      printf '%s\n' "Timed out waiting for $label to exit successfully" >&2
      exit 1
    fi
    sleep 1
  done
}

"$compose" up -d --remove-orphans minio
wait_for_http "http://127.0.0.1:$minio_api_port/minio/health/live" "minio"

bring_up_services \
  minio-init \
  lockd-disk-a-ca \
  lockd-s3-ca \
  lockd-mem-ca

wait_for_service_exited_success minio-init "minio-init"

wait_for_file "$repo_root/devenv/volumes/lockd-disk-a-config/ca.pem" "disk-a ca bundle"
wait_for_file "$repo_root/devenv/volumes/lockd-s3-config/ca.pem" "s3 ca bundle"
wait_for_file "$repo_root/devenv/volumes/lockd-mem-config/ca.pem" "mem ca bundle"

bring_up_services \
  lockd-disk-a-server-cert \
  lockd-disk-a-client-cert \
  lockd-disk-a-tc-client-cert \
  lockd-s3-server-cert \
  lockd-s3-client-cert \
  lockd-s3-tc-client-cert \
  lockd-mem-server-cert

wait_for_file "$repo_root/devenv/volumes/lockd-disk-a-config/client.pem" "disk-a client bundle"
wait_for_file "$repo_root/devenv/volumes/lockd-disk-a-config/server.pem" "disk-a server bundle"
wait_for_file "$repo_root/devenv/volumes/lockd-s3-config/client.pem" "s3 client bundle"
wait_for_file "$repo_root/devenv/volumes/lockd-s3-config/server.pem" "s3 server bundle"
wait_for_file "$repo_root/devenv/volumes/lockd-mem-config/server.pem" "mem server bundle"

bring_up_services \
  lockd-disk-a \
  lockd-disk-b \
  lockd-s3 \
  lockd-mem

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

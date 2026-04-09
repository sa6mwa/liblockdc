#!/usr/bin/env bash
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
kind=${1:-unit}

unset LD_LIBRARY_PATH

ctest_timeout=${LOCKDC_CTEST_TIMEOUT:-300}

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

wait_for_tcp() {
  host=$1
  port=$2
  label=$3
  count=0
  while ! (exec 3<>"/dev/tcp/$host/$port") >/dev/null 2>&1; do
    count=$((count + 1))
    if [ "$count" -ge 60 ]; then
      printf '%s\n' "Timed out waiting for $label at $host:$port" >&2
      exit 1
    fi
    sleep 1
  done
  exec 3>&-
}

probe_lockd_endpoint() {
  endpoint=$1
  bundle=$2
  example_bin=$3

  env \
    LOCKDC_URL="$endpoint" \
    LOCKDC_CLIENT_PEM="$bundle" \
    LOCKDC_KEY="e2e-readiness-$$-$(date +%s)" \
    LOCKDC_OWNER="e2e-readiness" \
    "$example_bin" >/dev/null 2>&1
}

wait_for_lockd_probe() {
  endpoint=$1
  bundle=$2
  label=$3
  example_bin=$4
  count=0

  while :; do
    count=$((count + 1))
    if probe_lockd_endpoint "$endpoint" "$bundle" "$example_bin"; then
      return 0
    fi
    if [ "$count" -ge 30 ]; then
      printf '%s\n' "Timed out waiting for $label lockd API readiness at $endpoint" >&2
      exit 1
    fi
    sleep 1
  done
}

select_active_disk_endpoint() {
  disk_a_endpoint=$1
  disk_b_endpoint=$2
  bundle=$3
  example_bin=$4
  count=0

  while :; do
    count=$((count + 1))
    if probe_lockd_endpoint "$disk_a_endpoint" "$bundle" "$example_bin"; then
      printf '%s\n' "$disk_a_endpoint"
      return 0
    fi
    if probe_lockd_endpoint "$disk_b_endpoint" "$bundle" "$example_bin"; then
      printf '%s\n' "$disk_b_endpoint"
      return 0
    fi
    if [ "$count" -ge 30 ]; then
      printf '%s\n' "Timed out waiting for an active disk lockd endpoint" >&2
      exit 1
    fi
    sleep 1
  done
}

case "$kind" in
  unit|debug)
    "$script_dir/build.sh" debug
    ctest --preset debug --progress --timeout "$ctest_timeout"
    ;;
  e2e)
    "$script_dir/dev-reset.sh"
    "$script_dir/dev-up.sh"
    disk_a_endpoint=${LOCKDC_E2E_DISK_ENDPOINT:-https://localhost:${LOCKDC_DISK_A_PORT:-19441}}
    disk_b_endpoint=${LOCKDC_E2E_DISK_B_ENDPOINT:-https://localhost:${LOCKDC_DISK_B_PORT:-19442}}
    export LOCKDC_E2E_DISK_ENDPOINT="$disk_a_endpoint"
    export LOCKDC_E2E_DISK_BUNDLE=${LOCKDC_E2E_DISK_BUNDLE:-$repo_root/devenv/volumes/lockd-disk-a-config/client.pem}
    export LOCKDC_E2E_S3_ENDPOINT=${LOCKDC_E2E_S3_ENDPOINT:-https://localhost:${LOCKDC_S3_PORT:-19443}}
    export LOCKDC_E2E_S3_BUNDLE=${LOCKDC_E2E_S3_BUNDLE:-$repo_root/devenv/volumes/lockd-s3-config/client.pem}
    export LOCKDC_E2E_MEM_SOCKET=${LOCKDC_E2E_MEM_SOCKET:-$repo_root/devenv/volumes/lockd-mem-run/lockd.sock}
    wait_for_file "$LOCKDC_E2E_DISK_BUNDLE" "disk client bundle"
    wait_for_file "$LOCKDC_E2E_S3_BUNDLE" "s3 client bundle"
    wait_for_socket "$LOCKDC_E2E_MEM_SOCKET" "mem uds socket"
    wait_for_tcp "127.0.0.1" "${LOCKDC_DISK_A_PORT:-19441}" "disk lockd tcp listener"
    wait_for_tcp "127.0.0.1" "${LOCKDC_DISK_B_PORT:-19442}" "disk-b lockd tcp listener"
    wait_for_tcp "127.0.0.1" "${LOCKDC_S3_PORT:-19443}" "s3 lockd tcp listener"
    "$script_dir/build.sh" e2e
    export LOCKDC_E2E_DISK_ENDPOINT="$(select_active_disk_endpoint \
      "$disk_a_endpoint" \
      "$disk_b_endpoint" \
      "$LOCKDC_E2E_DISK_BUNDLE" \
      "$repo_root/build/e2e/examples/lc_example_acquire_lease_lifecycle")"
    export LOCKDC_E2E_DISK_A_ENDPOINT="$LOCKDC_E2E_DISK_ENDPOINT"
    export LOCKDC_E2E_DISK_A_BUNDLE="$LOCKDC_E2E_DISK_BUNDLE"
    export LOCKDC_URL="$LOCKDC_E2E_DISK_ENDPOINT"
    export LOCKDC_CLIENT_PEM="$LOCKDC_E2E_DISK_BUNDLE"
    wait_for_lockd_probe \
      "$LOCKDC_E2E_DISK_ENDPOINT" \
      "$LOCKDC_E2E_DISK_BUNDLE" \
      "disk" \
      "$repo_root/build/e2e/examples/lc_example_acquire_lease_lifecycle"
    wait_for_lockd_probe \
      "$LOCKDC_E2E_S3_ENDPOINT" \
      "$LOCKDC_E2E_S3_BUNDLE" \
      "s3" \
      "$repo_root/build/e2e/examples/lc_example_acquire_lease_lifecycle"
    ctest --preset e2e --progress --timeout "$ctest_timeout"
    ;;
  release)
    "$script_dir/build.sh" release
    ctest --preset x86_64-linux-gnu-release --progress --timeout "$ctest_timeout"
    ;;
  asan)
    "$script_dir/build.sh" asan
    ctest --preset asan --progress --timeout "$ctest_timeout"
    ;;
  coverage)
    "$script_dir/build.sh" coverage
    ctest --preset coverage --progress --timeout "$ctest_timeout"
    cmake --build --preset coverage-report
    ;;
  fuzz)
    "$script_dir/fuzz.sh"
    ;;
  all)
    "$script_dir/test.sh" unit
    "$script_dir/test.sh" e2e
    ;;
  *)
    echo "usage: scripts/test.sh [unit|debug|e2e|release|asan|coverage|fuzz|all]" >&2
    exit 2
    ;;
esac

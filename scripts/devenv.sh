#!/usr/bin/env bash
set -euo pipefail
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd -P)
workspace="$repo_root/build/devenv"
manifest="$workspace/devenv.yaml"
command=${1:-}
case "$command" in up|down|reset|render|ps|logs|running) ;; *) echo 'usage: scripts/devenv.sh up|down|reset|render|ps|logs|running' >&2; exit 2;; esac
source "$script_dir/assert_generated_path.sh"
lockdc_assert_generated_path "$repo_root" "$workspace"
[ ! -L "$workspace" ] || { echo 'build/devenv must not be a symlink' >&2; exit 1; }
require_podman() {
  command -v podman >/dev/null || { echo 'Rootless Podman is required for local E2E services' >&2; return 1; }
  [ "$(podman info --format '{{.Host.Security.Rootless}}')" = true ] || {
    echo 'Local E2E services require rootless Podman; do not use sudo' >&2; return 1;
  }
}
render() { python3 "$script_dir/render-devenv.py"; }
load_env() { source "$workspace/env.sh"; }
diagnostics() {
  echo "manifest: $manifest" >&2
  if [ -f "$workspace/env.sh" ]; then
    load_env
    podman pod ps --filter "name=^${LOCKDC_DEVENV_POD}$" >&2 || true
    podman pod logs "$LOCKDC_DEVENV_POD" > "$workspace/logs/startup-services.log" 2>&1 || true
    tail -200 "$workspace/logs/startup-services.log" >&2
    echo "Full service logs: $workspace/logs/startup-services.log" >&2
  fi
}
wait_ready() {
  local label=$1; shift
  local attempts=${LOCKDC_DEVENV_READY_ATTEMPTS:-60}
  [[ "$attempts" =~ ^[1-9][0-9]*$ ]] || { echo 'LOCKDC_DEVENV_READY_ATTEMPTS must be positive' >&2; return 1; }
  for ((i=0; i<attempts; i++)); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "Timed out waiting for $label: $*" >&2
  return 1
}
tcp_ready() { (exec 3<>"/dev/tcp/127.0.0.1/$1") >/dev/null 2>&1; }
active_disk_ready() {
  local primary
  primary=$(python3 "$script_dir/probe-devenv-disk.py" "$LOCKDC_DISK_A_PORT" "$LOCKDC_DISK_B_PORT") || return 1
  python3 "$script_dir/render-devenv.py" --disk-primary "$primary" || return 1
  load_env
}
show() {
  printf 'Pod: %s\nManifest: %s\nState: %s/state\n' "$LOCKDC_DEVENV_POD" "$manifest" "$workspace"
  printf 'disk-a: https://localhost:%s\ndisk-b: https://localhost:%s\nactive disk: %s\ns3: %s\nmem: unix://%s\n' "$LOCKDC_DISK_A_PORT" "$LOCKDC_DISK_B_PORT" "$LOCKDC_E2E_DISK_ENDPOINT" "$LOCKDC_E2E_S3_ENDPOINT" "$LOCKDC_E2E_MEM_SOCKET"
  printf 'Client bundles: %s and %s\n' "$LOCKDC_E2E_DISK_BUNDLE" "$LOCKDC_E2E_S3_BUNDLE"
  printf 'MinIO: http://127.0.0.1:%s (console :%s), bucket lockd-client-s3\n' "$LOCKDC_MINIO_API_PORT" "$LOCKDC_MINIO_CONSOLE_PORT"
  printf 'Local MinIO credentials: lockddev / lockddevpass\nEnvironment: source %q\n' "$workspace/env.sh"
  printf 'Images: lockd:v0.8.1, minio:RELEASE.2025-09-07T16-13-09Z, mc:RELEASE.2025-08-13T08-35-41Z\n'
}
down() {
  if [ -f "$manifest" ]; then
    require_podman
    load_env
    if podman pod exists "$LOCKDC_DEVENV_POD"; then
      podman pod stop --time 60 "$LOCKDC_DEVENV_POD"
    fi
    podman kube down "$manifest"
    # Verify the entire declared graph, including exited bootstrap containers.
    # A successful kube down must not leave any checkout-owned resources.
    local resource kind name status
    resource=$(python3 - "$manifest" <<'PY'
import json
import sys
with open(sys.argv[1]) as source:
    pod = json.load(source)
name = pod['metadata']['name']
print('pod', name)
for container in pod['spec'].get('initContainers', []) + pod['spec']['containers']:
    print('container', name + '-' + container['name'])
PY
    ) || return 1
    while read -r kind name; do
      if podman "$kind" exists "$name"; then
        echo "E2E teardown left $kind $name behind" >&2
        return 1
      else
        status=$?
        [ "$status" = 1 ] || {
          echo "Cannot verify E2E teardown for $kind $name (Podman status $status)" >&2
          return 1
        }
      fi
    done <<< "$resource"
    echo "E2E pod and declared containers removed: $LOCKDC_DEVENV_POD"
  fi
}
case "$command" in
  render) render ;;
  running)
    require_podman
    [ -f "$manifest" ] || exit 1
    load_env
    # One-shot bootstrap containers may have exited successfully. Require
    # every long-lived service instead of relying on the pod's aggregate state.
    for service in minio lockd-disk-a lockd-disk-b lockd-s3 lockd-mem; do
      [ "$(podman container inspect --format '{{.State.Running}}' "$LOCKDC_DEVENV_POD-$service" 2>/dev/null)" = true ] || exit 1
    done
    ;;
  down) down ;;
  reset)
    down
    rm -rf -- "$workspace"
    ;;
  ps|logs)
    require_podman
    [ -f "$manifest" ] || { echo 'No rendered devenv; run make dev-up' >&2; exit 1; }
    load_env
    if [ "$command" = ps ]; then podman pod ps --filter "name=^${LOCKDC_DEVENV_POD}$";
    else podman pod logs "$LOCKDC_DEVENV_POD"; fi
    ;;
  up)
    require_podman
    render
    load_env
    trap 'rc=$?; if [ "$rc" -ne 0 ]; then diagnostics; fi' EXIT
    if ! podman pod exists "$LOCKDC_DEVENV_POD"; then
      # Create the declared graph first. Bootstrap MinIO before lockd starts;
      # every container remains declared in the same pod-local network.
      podman kube play --network pasta --start=false "$manifest"
    fi
    # Creation and startup are separate: retry initialization even when an
    # earlier attempt left a created or stopped pod behind.
    # Use the exact declared lockd image's static CLI in shell-capable init
    # containers, preserving existing CA/server keys across down/up.
    podman cp --archive=false "$LOCKDC_DEVENV_POD-lockd-disk-a:/opt/lockd/bin/lockd" "$workspace/state/bootstrap/lockd"
    chmod u+x "$workspace/state/bootstrap/lockd"
    podman start "$LOCKDC_DEVENV_POD-minio"
    wait_ready 'MinIO HTTP health' curl --max-time 2 -fsS "http://127.0.0.1:$LOCKDC_MINIO_API_PORT/minio/health/live"
    podman start "$LOCKDC_DEVENV_POD-minio-init"
    result=$(timeout 60 podman wait "$LOCKDC_DEVENV_POD-minio-init")
    [ "$result" = 0 ] || { echo "MinIO bucket bootstrap failed: $result" >&2; exit 1; }
    podman pod start "$LOCKDC_DEVENV_POD"
    wait_ready 'disk-a TCP' tcp_ready "$LOCKDC_DISK_A_PORT"
    wait_ready 'disk-b TCP' tcp_ready "$LOCKDC_DISK_B_PORT"
    wait_ready 's3 TCP' tcp_ready "$LOCKDC_S3_PORT"
    wait_ready 'disk client bundle' test -s "$LOCKDC_E2E_DISK_BUNDLE"
    wait_ready 's3 client bundle' test -s "$LOCKDC_E2E_S3_BUNDLE"
    wait_ready 'memory Unix socket' test -S "$LOCKDC_E2E_MEM_SOCKET"
    wait_ready 'active disk API' active_disk_ready
    show
    ;;
esac

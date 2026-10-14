#!/usr/bin/env bash
set -euo pipefail
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
server_version=14
if [ "${1:-}" = --server-version ]; then
  server_version=${2:-}
  [ "$#" -ge 2 ] || { echo '--server-version needs a version' >&2; exit 2; }
  shift 2
fi
case "$server_version" in
  14) image_digest=4ea9e5ed06591da7ea23eb65465e8d3187fe79f4d5ec3ae976d29a33b013e77a ;;
  15) image_digest=d695f99707f902238afc4e65edb89e778158a7ab862ba49ba27b377b735fc1a8 ;;
  16) image_digest=621a761097839bdb50207afd6b87a72f38e2d718dd46c3d744828d8917c4f1e0 ;;
  17) image_digest=9ea644da56f1a82283b173fa38a316636fe7c538991a8ae41998cd3765434991 ;;
  18) image_digest=885cf05d376c7cf27afef02073e6bdac3841252537f16e244fd1c1e6a7c99fb1 ;;
  *) echo 'Supported test server versions: 14, 15, 16, 17, 18' >&2; exit 2 ;;
esac
[ "$#" -gt 0 ] || { echo 'At least one XA test executable is required' >&2; exit 2; }
workspace="$repo_root/build/devenv/postgres-xa"
checkout_id=$(printf '%s' "$repo_root" | sha256sum | cut -c1-10)
pod="liblockdc-postgres-xa-$checkout_id"
containers=("$pod-pg-first" "$pod-pg-second")
manifest="$workspace/postgres.yaml"
mkdir -p "$workspace"
chmod 700 "$workspace"
exec 9>"$workspace/run.lock"
flock -n 9 || { echo 'Another PostgreSQL XA test run owns this checkout' >&2; exit 1; }
if podman pod exists "$pod"; then
  echo "Refusing to start a duplicate PostgreSQL test stack: $pod" >&2
  exit 1
fi
cleanup() {
  local result=$? kind name code container
  trap - EXIT
  if podman pod exists "$pod"; then
    for container in "${containers[@]}"; do
      podman logs "$container" > "$workspace/$container.log" 2>&1 || true
    done
    podman pod stop --time 10 "$pod" > "$workspace/stop.log" 2>&1 || result=1
  fi
  podman kube down "$manifest" > "$workspace/down.log" 2>&1 || result=1
  for name in "$pod" "${containers[@]}"; do
    kind=container
    if [ "$name" = "$pod" ]; then kind=pod; fi
    if podman "$kind" exists "$name"; then
      echo "PostgreSQL teardown left $kind $name" >&2
      result=1
    else
      code=$?
      [ "$code" = 1 ] || result=1
    fi
  done
  if [ "$result" = 0 ]; then echo 'TEARDOWN: PostgreSQL pod and both containers absent'; fi
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
rm -rf -- "$workspace/state" "$workspace/socket" "$workspace/pouch"
mkdir -p "$workspace/state/first" "$workspace/socket/first" "$workspace/state/second" "$workspace/socket/second" "$workspace/pouch"
password=$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')
sed -e "s|@POD@|$pod|g" -e "s|@PASSWORD@|$password|g" -e "s|@WORKSPACE@|$workspace|g" \
  -e "s|@IMAGE_DIGEST@|$image_digest|g" \
  "$repo_root/tests/e2e/postgres.yaml.in" > "$manifest"
chmod 600 "$manifest"
podman kube play --network=none --userns=keep-id:uid=70,gid=70 "$manifest" > "$workspace/up.log" 2>&1
for index in 0 1; do
  container=${containers[$index]}
  port=$((5432 + index))
  ready=0
  for ((attempt=0; attempt<45; attempt++)); do
    if [ "$(podman exec "$container" cat /proc/1/comm 2>/dev/null)" = postgres ] &&
       podman exec "$container" pg_isready -p "$port" -U xa -d xa >/dev/null 2>&1; then ready=1; break; fi
    sleep 1
  done
  if [ "$ready" != 1 ]; then echo "PostgreSQL $container readiness failed; see service logs" >&2; exit 1; fi
  podman exec "$container" psql -p "$port" -X -qAt -v ON_ERROR_STOP=1 -U xa -d xa \
    -c "SELECT current_setting('server_version_num')::int / 10000" | rg -x "$server_version"
  podman exec -i "$container" psql -p "$port" -X -q -v ON_ERROR_STOP=1 -U xa -d xa <<'SQL'
CREATE ROLE xa_participant LOGIN;
CREATE ROLE xa_borrow_role;
GRANT xa_borrow_role TO xa_participant;
GRANT EXECUTE ON FUNCTION pg_control_system() TO xa_participant;
CREATE TABLE xa_work (name text PRIMARY KEY);
GRANT SELECT, INSERT ON xa_work TO xa_participant;
SQL
done
first="host=$workspace/socket/first port=5432 dbname=xa user=xa_participant connect_timeout=5"
second="host=$workspace/socket/second port=5433 dbname=xa user=xa_participant connect_timeout=5"
index=0
for binary in "$@"; do
  index=$((index + 1))
  test -x "$binary"
  mkdir -p "$workspace/pouch/$index"
  runner=()
  if [ "${LOCKDC_POSTGRES_VALGRIND:-0}" = 1 ]; then
    runner=(valgrind --error-exitcode=99 --leak-check=full --errors-for-leak-kinds=definite,indirect --log-file="$workspace/valgrind-$index.log")
  fi
  "${runner[@]}" "$binary" "$first" "$workspace/pouch/$index" "${containers[0]}" "$second" "${containers[1]}"
  for service in 0 1; do
    podman exec "${containers[$service]}" psql -p "$((5432 + service))" -X -qAt -v ON_ERROR_STOP=1 -U xa -d xa \
      -c 'SELECT count(*) FROM pg_prepared_xacts' | rg -x '0'
  done
done
test -w "$workspace/state/first/pgdata/PG_VERSION"
test -w "$workspace/state/second/pgdata/PG_VERSION"

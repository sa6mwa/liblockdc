#!/usr/bin/env bash
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)

preset=${LOCKDC_VALGRIND_PRESET:-valgrind}
build_dir="$repo_root/build/$preset"
valgrind_bin=${VALGRIND:-valgrind}

if [ -n "${LOCKDC_VALGRIND_TESTS:-}" ]; then
  read -r -a valgrind_tests <<< "$LOCKDC_VALGRIND_TESTS"
else
  valgrind_tests=(
    lc_unit_streams
    lc_unit_engine_allocator
    lc_unit_contracts
    lc_unit_public_mock_usage
    lc_unit_runtime
    lc_unit_mutate_stream
    lc_unit_pouch_client
  )
fi

unset LD_LIBRARY_PATH

cd "$repo_root"

if [ "${LOCKDC_VALGRIND_DRY_RUN:-0}" = "1" ]; then
  printf 'preset=%s\n' "$preset"
  printf 'build_dir=%s\n' "$build_dir"
  printf 'valgrind=%s\n' "$valgrind_bin"
  for test_name in "${valgrind_tests[@]}"; do
    printf 'test=%s\n' "$test_name"
  done
  exit 0
fi

if ! command -v "$valgrind_bin" >/dev/null 2>&1; then
  printf '%s\n' \
    'PKT_DIAGNOSTIC_BEGIN' \
    'surface=make valgrind' \
    'phase=tool-discovery' \
    'status=failed' \
    'class=external-tool-unavailable' \
    'reason=valgrind-not-found' \
    'artifact=valgrind' \
    'next=install host Valgrind or set VALGRIND=/path/to/valgrind, then rerun make valgrind' \
    'PKT_DIAGNOSTIC_END' >&2
  exit 2
fi

cmake --preset "$preset"
cmake --build --preset "$preset"

for test_name in "${valgrind_tests[@]}"; do
  test_path="$build_dir/tests/unit/$test_name"
  if [ ! -x "$test_path" ]; then
    printf '%s\n' \
      'PKT_DIAGNOSTIC_BEGIN' \
      'surface=make valgrind' \
      'phase=test-discovery' \
      'status=failed' \
      'class=build-artifact-missing' \
      "reason=valgrind-test-not-built" \
      "artifact=$test_path" \
      'next=inspect the valgrind preset build output and ensure the unit test target is enabled' \
      'PKT_DIAGNOSTIC_END' >&2
    exit 2
  fi

  "$valgrind_bin" \
    --leak-check=full \
    --show-leak-kinds=definite,indirect \
    --track-origins=yes \
    --error-exitcode=97 \
    "$test_path"
done

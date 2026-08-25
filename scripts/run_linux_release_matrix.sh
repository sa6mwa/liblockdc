#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd -P)
make_bin=${MAKE:-make}

cd "$repo_root"
unset LD_LIBRARY_PATH

"$script_dir/run_timed.sh" "release-matrix build" "$make_bin" __build-release

for preset in x86_64-linux-gnu-release x86_64-linux-musl-release; do
  "$script_dir/run_timed.sh" "release-matrix test $preset" \
    ctest --preset "$preset" --output-on-failure --progress --stop-on-failure \
      --timeout "${LOCKDC_CTEST_TIMEOUT:-300}" --parallel "${LOCKDC_CTEST_PARALLEL_LEVEL:-4}" -LE lifecycle-host
done

"$script_dir/run_timed.sh" "release-matrix cross tests" \
  bash "$script_dir/cross_test.sh" release
"$script_dir/run_timed.sh" "release-matrix package" \
  bash "$script_dir/run_linux_package_matrix.sh"

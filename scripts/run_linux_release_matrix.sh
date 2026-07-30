#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd -P)
make_bin=${MAKE:-make}

cd "$repo_root"

"$make_bin" __build-release
"$make_bin" __test-host
bash "$script_dir/cross_test.sh" release
bash "$script_dir/run_linux_package_matrix.sh"

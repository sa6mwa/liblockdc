#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)

remove_if_present() {
  if [ -e "$1" ]; then
    # Go makes cached module directories read-only. Restore owner access on
    # directories only so rm can remove the repository-local generated tree.
    # -P keeps a cache symlink from changing permissions outside this checkout.
    find -P "$1" -type d -exec chmod u+rwx -- {} +
    rm -rf -- "$1"
  fi
}

"$script_dir/dev-reset.sh"

remove_if_present "$repo_root/build"
remove_if_present "$repo_root/dist"
remove_if_present "$repo_root/.cache"
remove_if_present "$repo_root/.luarocks-build"

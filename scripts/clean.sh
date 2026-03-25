#!/usr/bin/env bash
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)

remove_if_present() {
  if [ -e "$1" ]; then
    rm -rf -- "$1"
  fi
}

"$script_dir/dev-reset.sh"

remove_if_present "$repo_root/build"
remove_if_present "$repo_root/dist"
remove_if_present "$repo_root/.cache"

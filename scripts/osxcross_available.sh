#!/usr/bin/env bash
set -eu

if [ -n "${OSXCROSS_ROOT:-}" ]; then
  osxcross_root=$OSXCROSS_ROOT
else
  osxcross_root=${HOME:?HOME is required when OSXCROSS_ROOT is not set}/.local/cross/osxcross
fi

host=${LOCKDC_OSXCROSS_HOST:-arm64-apple-darwin25}
bin_dir="$osxcross_root/bin"

for tool in clang ar ranlib install_name_tool otool; do
  if [ ! -x "$bin_dir/$host-$tool" ]; then
    exit 1
  fi
done

if ! compgen -G "$osxcross_root/SDK/MacOSX*.sdk" >/dev/null; then
  exit 1
fi

#!/usr/bin/env bash
set -euo pipefail
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
osxcross_root=${OSXCROSS_ROOT:-${HOME:?HOME is required when OSXCROSS_ROOT is not set}/.local/cross/osxcross}
compgen -G "$osxcross_root/SDK/MacOSX*.sdk" >/dev/null || exit 1
# Probe the Apple collection only. Dependency preparation ensures host MIG;
# CMake requires the complete resolver status before configuring Darwin.
if [ -z "${CPKT_OSXCROSS_HOST:-}" ] && [ -n "${LOCKDC_OSXCROSS_HOST:-}" ]; then
  export CPKT_OSXCROSS_HOST=$LOCKDC_OSXCROSS_HOST
fi
description=$("$script_dir/cpkt-toolchains.sh" discover arm64-apple-darwin)
host=$(printf '%s\n' "$description" | sed -n 's/^prefix=//p')
[ -n "$host" ] || exit 1
[ -x "$osxcross_root/bin/$host-install_name_tool" ] || exit 1

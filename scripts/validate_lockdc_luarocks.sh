#!/usr/bin/env bash

set -eu

if [ "$#" -ne 4 ]; then
  printf 'usage: %s TREE_DIR SDK_PREFIX LUA_PACKAGE_PATH LUA_SCRIPT\n' "$0" >&2
  exit 1
fi

tree_dir="$1"
sdk_prefix="$2"
lua_package_path="$3"
lua_script="$4"

lua_bin="${LOCKDC_LUA_BIN:-lua}"
luarocks_bin="${LOCKDC_LUAROCKS_BIN:-luarocks}"
lua_version="${LOCKDC_LUA_VERSION:-5.5}"
lonejson_src_rock="${LOCKDC_LONEJSON_SRC_ROCK:-https://github.com/sa6mwa/lonejson/releases/download/v0.7.0/lonejson-0.7.0-1.src.rock}"
luarocks_build_root="${LOCKDC_LUAROCKS_BUILD_ROOT:-${tree_dir}/.luarocks-build}"
luarocks_workdir="${LOCKDC_LUAROCKS_WORKDIR:-$PWD}"
run_lua_smoke="${LOCKDC_RUN_LUA_SMOKE:-1}"
lockdc_ld_preload="${LOCKDC_LD_PRELOAD:-}"

require_path() {
  if [ ! -e "$1" ]; then
    printf 'missing required path: %s\n' "$1" >&2
    exit 1
  fi
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'missing required command: %s\n' "$1" >&2
    exit 1
  fi
}

require_command "$lua_bin"
require_command "$luarocks_bin"
require_path "$sdk_prefix"
require_path "$lua_package_path"
require_path "$lua_script"
require_path "$luarocks_workdir"

export LD_LIBRARY_PATH="$sdk_prefix/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export LOCKDC_PREFIX="$sdk_prefix"
if [ -n "$lockdc_ld_preload" ]; then
  export LD_PRELOAD="$lockdc_ld_preload${LD_PRELOAD:+:$LD_PRELOAD}"
fi

rm -rf "$tree_dir" "$luarocks_build_root"
mkdir -p "$tree_dir"
cd "$luarocks_workdir"

"$luarocks_bin" --tree "$tree_dir" --lua-version "$lua_version" install "$lonejson_src_rock"
case "$lua_package_path" in
  *.rockspec)
    LOCKDC_LUAROCKS_BUILD_ROOT="$luarocks_build_root" \
      "$luarocks_bin" --tree "$tree_dir" --lua-version "$lua_version" make "$lua_package_path"
    ;;
  *.src.rock|*.rock)
    "$luarocks_bin" --tree "$tree_dir" --lua-version "$lua_version" install "$lua_package_path"
    ;;
  *)
    printf 'unsupported Lua package path: %s\n' "$lua_package_path" >&2
    exit 1
    ;;
esac

eval "$("$luarocks_bin" --tree "$tree_dir" path --lua-version "$lua_version")"

if [ "$run_lua_smoke" = "0" ] || [ "$run_lua_smoke" = "OFF" ] || [ "$run_lua_smoke" = "FALSE" ]; then
  printf '%s\n' "Skipping Lua runtime smoke: target module is not loadable by the host Lua VM"
  exit 0
fi

"$lua_bin" "$lua_script"

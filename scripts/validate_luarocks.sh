#!/usr/bin/env bash

set -euo pipefail

if [ "$#" -ne 4 ]; then
  printf 'usage: %s TREE_DIR SDK_PREFIX LUA_PACKAGE_PATH LUA_SCRIPT\n' "$0" >&2
  exit 1
fi

tree_dir="$1"
sdk_prefix="$2"
lua_package_path="$3"
lua_script="$4"

lua_bin="${LOCKDC_LUA_BIN:-lua5.5}"
luarocks_bin="${LOCKDC_LUAROCKS_BIN:-luarocks}"
lua_version="${LOCKDC_LUA_VERSION:-5.5}"
lonejson_src_rock="${LOCKDC_LONEJSON_SRC_ROCK:-https://github.com/sa6mwa/lonejson/releases/download/v0.42.0/lonejson-0.42.0-1.src.rock}"
luarocks_build_root="${LOCKDC_LUAROCKS_BUILD_ROOT:-${tree_dir}/.luarocks-build}"
luarocks_workdir="${LOCKDC_LUAROCKS_WORKDIR:-$PWD}"
run_lua_smoke="${LOCKDC_RUN_LUA_SMOKE:-1}"
lockdc_ld_preload="${LOCKDC_LD_PRELOAD:-}"

# shellcheck source=assert_generated_path.sh
source "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)/assert_generated_path.sh"

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
lockdc_assert_generated_path "$luarocks_workdir" "$tree_dir"
lockdc_assert_generated_path "$luarocks_workdir" "$luarocks_build_root"

lua_runtime_version="$($lua_bin -e 'io.write(_VERSION)' 2>/dev/null || true)"
if [ "$lua_runtime_version" != "Lua 5.5" ]; then
  printf 'Lua 5.5 is required, got %s from %s\n' "${lua_runtime_version:-unknown}" "$lua_bin" >&2
  exit 1
fi

install_lonejson_dependency() {
  case "$lonejson_src_rock" in
    *.src.rock)
      if [ -f "$lonejson_src_rock" ]; then
        require_command unzip
        require_command tar

        lonejson_unpack_dir="${tree_dir}-lonejson-src"
        rm -rf "$lonejson_unpack_dir"
        mkdir -p "$lonejson_unpack_dir"
        (
          cd "$lonejson_unpack_dir"
          unzip -q "$lonejson_src_rock"
          lonejson_archive="$(find . -maxdepth 1 -type f -name '*.tar.gz' | sed 's#^\./##' | head -n 1)"
          lonejson_rockspec="$(find . -maxdepth 1 -type f -name '*.rockspec' | sed 's#^\./##' | head -n 1)"
          if [ -z "$lonejson_archive" ] || [ -z "$lonejson_rockspec" ]; then
            printf 'invalid lonejson source rock layout: %s\n' "$lonejson_src_rock" >&2
            exit 1
          fi
          tar -xzf "$lonejson_archive"
          lonejson_source_dir="$(find . -mindepth 1 -maxdepth 1 -type d | sed 's#^\./##' | head -n 1)"
          if [ -z "$lonejson_source_dir" ]; then
            printf 'lonejson source rock has no extracted source directory: %s\n' "$lonejson_src_rock" >&2
            exit 1
          fi
          cp "$lonejson_rockspec" "$lonejson_source_dir/"
          cd "$lonejson_source_dir"
          "$luarocks_bin" --tree "$tree_dir" --lua-version "$lua_version" make "$lonejson_rockspec"
        )
        return 0
      fi
      ;;
  esac

  "$luarocks_bin" --tree "$tree_dir" --lua-version "$lua_version" install "$lonejson_src_rock"
}

export LD_LIBRARY_PATH="$sdk_prefix/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export LOCKDC_PREFIX="$sdk_prefix"
: "${LONEJSON_LIBDIR:=$sdk_prefix/lib}"
export LONEJSON_LIBDIR
if [ -n "$lockdc_ld_preload" ]; then
  export LD_PRELOAD="$lockdc_ld_preload${LD_PRELOAD:+:$LD_PRELOAD}"
fi

rm -rf "$tree_dir" "$luarocks_build_root"
mkdir -p "$tree_dir"
cd "$luarocks_workdir"

install_lonejson_dependency
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

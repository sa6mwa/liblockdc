#!/usr/bin/env bash
set -eu

usage() {
  echo "usage: scripts/discover_target_tools.sh --build-dir DIR --target-id TARGET --tool TOOL" >&2
  exit 2
}

build_dir=
target_id=
tool=

while [ "$#" -gt 0 ]; do
  case "$1" in
    --build-dir)
      [ "$#" -ge 2 ] || usage
      build_dir=$2
      shift 2
      ;;
    --target-id)
      [ "$#" -ge 2 ] || usage
      target_id=$2
      shift 2
      ;;
    --tool)
      [ "$#" -ge 2 ] || usage
      tool=$2
      shift 2
      ;;
    *)
      usage
      ;;
  esac
done

[ -n "$build_dir" ] || usage
[ -n "$target_id" ] || usage
[ -n "$tool" ] || usage

case "$target_id" in
  *apple-darwin) ;;
  *)
    echo "target-tool discovery currently supports Darwin targets only: $target_id" >&2
    exit 2
    ;;
esac

cache_file=$build_dir/CMakeCache.txt
[ -f "$cache_file" ] || {
  echo "missing CMake cache: $cache_file" >&2
  exit 1
}

cache_value() {
  awk -F= -v key="$1" '$1 ~ "^" key "(:[^=]+)?$" { print substr($0, index($0, "=") + 1); exit }' "$cache_file"
}

is_executable() {
  [ -n "$1" ] && [ -x "$1" ]
}

compiler=$(cache_value CMAKE_C_COMPILER || true)
compiler_dir=
compiler_base=
if [ -n "$compiler" ]; then
  compiler_dir=$(dirname -- "$compiler")
  compiler_base=$(basename -- "$compiler")
fi

host=${CPKT_OSXCROSS_HOST:-${LOCKDC_OSXCROSS_HOST:-$(cache_value LOCKDC_OSXCROSS_HOST || true)}}
if [ -z "$host" ] && [ -n "$compiler_base" ]; then
  case "$compiler_base" in
    *-clang) host=${compiler_base%-clang} ;;
    *-cc) host=${compiler_base%-cc} ;;
  esac
fi
if [ -z "$host" ]; then
  host=arm64-apple-darwin25
fi

if [ -n "${OSXCROSS_ROOT:-}" ]; then
  osxcross_root=$OSXCROSS_ROOT
elif [ -n "${HOME:-}" ]; then
  osxcross_root=$HOME/.local/cross/osxcross
else
  osxcross_root=
fi
osxcross_bin=
if [ -n "$osxcross_root" ]; then
  osxcross_bin=$osxcross_root/bin
fi

case "$tool" in
  otool)
    explicit=$(cache_value LOCKDC_OTOOL || true)
    explicit_alt=$(cache_value CPKT_OTOOL || true)
    cmake_tool=$(cache_value CMAKE_OTOOL || true)
    tool_name=otool
    ;;
  install_name_tool)
    explicit=
    explicit_alt=
    cmake_tool=$(cache_value CMAKE_INSTALL_NAME_TOOL || true)
    tool_name=install_name_tool
    ;;
  strip)
    explicit=
    explicit_alt=
    cmake_tool=$(cache_value CMAKE_STRIP || true)
    tool_name=strip
    ;;
  ld|linker)
    explicit=
    explicit_alt=
    cmake_tool=$(cache_value CMAKE_LINKER || true)
    tool_name=ld
    ;;
  *)
    echo "unsupported target tool: $tool" >&2
    exit 2
    ;;
esac

candidates=
for candidate in \
  "$explicit" \
  "$explicit_alt" \
  "$cmake_tool" \
  "${compiler_dir:+$compiler_dir/$host-$tool_name}" \
  "${compiler_dir:+$compiler_dir/$tool_name}" \
  "${osxcross_bin:+$osxcross_bin/$host-$tool_name}" \
  "${osxcross_bin:+$osxcross_bin/$tool_name}"
do
  if [ -n "$candidate" ]; then
    candidates="$candidates
$candidate"
  fi
done

old_ifs=$IFS
IFS='
'
for candidate in $candidates; do
  if is_executable "$candidate"; then
    printf '%s\n' "$candidate"
    IFS=$old_ifs
    exit 0
  fi
done
IFS=$old_ifs

if command -v "$host-$tool_name" >/dev/null 2>&1; then
  command -v "$host-$tool_name"
  exit 0
fi
if command -v "$tool_name" >/dev/null 2>&1; then
  command -v "$tool_name"
  exit 0
fi

echo "external-tool-unavailable: failed to discover $tool for $target_id from $cache_file" >&2
exit 1

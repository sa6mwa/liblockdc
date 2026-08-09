#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: scripts/discover_target_tools.sh --build-dir DIR --target-id TARGET [--tool TOOL]" >&2
  exit 2
}

build_dir=
target_id=
requested_tool=

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
      requested_tool=$2
      shift 2
      ;;
    *)
      usage
      ;;
  esac
done

[ -n "$build_dir" ] || usage
[ -n "$target_id" ] || usage

case "$target_id" in
  x86_64-linux-gnu|x86_64-linux-musl)
    target_host=x86_64-linux
    target_family=linux
    ;;
  aarch64-linux-gnu|aarch64-linux-musl)
    target_host=aarch64-linux
    target_family=linux
    ;;
  armhf-linux-gnu|armhf-linux-musl)
    target_host=arm-linux
    target_family=linux
    ;;
  *apple-darwin)
    target_host=
    target_family=darwin
    ;;
  *)
    echo "unsupported target ID for target-tool discovery: $target_id" >&2
    exit 2
    ;;
esac

cache_file=$build_dir/CMakeCache.txt
[ -f "$cache_file" ] || {
  echo "external-tool-unavailable: missing CMake cache: $cache_file" >&2
  exit 1
}

cache_value() {
  awk -F= -v key="$1" '$1 ~ "^" key "(:[^=]+)?$" { print substr($0, index($0, "=") + 1); exit }' "$cache_file"
}

is_executable() {
  [ -n "$1" ] && [ -x "$1" ]
}

find_executable() {
  local candidate
  for candidate in "$@"; do
    if is_executable "$candidate"; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  return 1
}

find_path_executable() {
  local name=$1
  if command -v "$name" >/dev/null 2>&1; then
    command -v "$name"
    return 0
  fi
  return 1
}

compiler=$(cache_value CMAKE_C_COMPILER || true)
compiler_dir=
if [ -n "$compiler" ]; then
  compiler_dir=$(dirname -- "$compiler")
fi

if [ "$target_family" = darwin ]; then
  configured_host=$(cache_value LOCKDC_OSXCROSS_HOST || true)
  compiler_base=$(basename -- "$compiler")
  if [ -z "$configured_host" ]; then
    case "$compiler_base" in
      *-clang) configured_host=${compiler_base%-clang} ;;
      *-cc) configured_host=${compiler_base%-cc} ;;
    esac
  fi
  target_host=${CPKT_OSXCROSS_HOST:-${LOCKDC_OSXCROSS_HOST:-${configured_host:-arm64-apple-darwin25}}}
fi

osxcross_bin=
if [ "$target_family" = darwin ]; then
  if [ -n "${OSXCROSS_ROOT:-}" ]; then
    osxcross_root=$OSXCROSS_ROOT
  elif [ -n "${HOME:-}" ]; then
    osxcross_root=$HOME/.local/cross/osxcross
  else
    osxcross_root=
  fi
  if [ -n "$osxcross_root" ]; then
    osxcross_bin=$osxcross_root/bin
  fi
fi

discover_tool() {
  local logical_tool=$1
  local tool_name=
  local explicit=
  local explicit_alt=
  local cmake_tool=
  local resolved=

  case "$logical_tool" in
    cc)
      tool_name=cc
      cmake_tool=$(cache_value CMAKE_C_COMPILER || true)
      ;;
    otool)
      [ "$target_family" = darwin ] || return 1
      tool_name=otool
      explicit=$(cache_value LOCKDC_OTOOL || true)
      explicit_alt=$(cache_value CPKT_OTOOL || true)
      cmake_tool=$(cache_value CMAKE_OTOOL || true)
      ;;
    install_name_tool)
      [ "$target_family" = darwin ] || return 1
      tool_name=install_name_tool
      cmake_tool=$(cache_value CMAKE_INSTALL_NAME_TOOL || true)
      ;;
    strip)
      tool_name=strip
      cmake_tool=$(cache_value CMAKE_STRIP || true)
      ;;
    readelf)
      [ "$target_family" = linux ] || return 1
      tool_name=readelf
      explicit=$(cache_value LOCKDC_READELF || true)
      explicit_alt=$(cache_value CPKT_READELF || true)
      cmake_tool=$(cache_value CMAKE_READELF || true)
      ;;
    ld|linker)
      tool_name=ld
      cmake_tool=$(cache_value CMAKE_LINKER || true)
      ;;
    *)
      echo "unsupported target tool: $logical_tool" >&2
      exit 2
      ;;
  esac

  resolved=$(find_executable \
    "$explicit" \
    "$explicit_alt" \
    "$cmake_tool" \
    "${compiler_dir:+$compiler_dir/$target_host-$tool_name}" \
    "${compiler_dir:+$compiler_dir/$tool_name}" \
    "${osxcross_bin:+$osxcross_bin/$target_host-$tool_name}" \
    "${osxcross_bin:+$osxcross_bin/$tool_name}" || true)
  if [ -n "$resolved" ]; then
    printf '%s\n' "$resolved"
    return 0
  fi

  if resolved=$(find_path_executable "$target_host-$tool_name" || true); then
    if [ -n "$resolved" ]; then
      printf '%s\n' "$resolved"
      return 0
    fi
  fi

  # A cross target must never inspect or link an artifact through an ambient
  # host tool. Native x86_64 Bootlin paths are still cache-resolved above.
  if [ "$target_family" = darwin ] && [ "$logical_tool" != ld ] && [ "$logical_tool" != linker ]; then
    if resolved=$(find_path_executable "$tool_name" || true); then
      if [ -n "$resolved" ]; then
        printf '%s\n' "$resolved"
        return 0
      fi
    fi
  fi

  return 1
}

print_assignment() {
  local key=$1
  local value=$2
  printf '%s=%s\n' "$key" "$value"
}

if [ -n "$requested_tool" ]; then
  if resolved_tool=$(discover_tool "$requested_tool"); then
    printf '%s\n' "$resolved_tool"
    exit 0
  fi
  echo "external-tool-unavailable: failed to discover $requested_tool for $target_id from $cache_file" >&2
  exit 1
fi

cc=$(discover_tool cc || true)
ld=$(discover_tool ld || true)
otool=$(discover_tool otool || true)
install_name_tool=$(discover_tool install_name_tool || true)
strip=$(discover_tool strip || true)
readelf=$(discover_tool readelf || true)

print_assignment TARGET_ID "$target_id"
print_assignment TARGET_HOST "$target_host"
print_assignment CC "$cc"
print_assignment LD "$ld"
print_assignment LINKER "$ld"
print_assignment OTOOL "$otool"
print_assignment INSTALL_NAME_TOOL "$install_name_tool"
print_assignment STRIP "$strip"
print_assignment READELF "$readelf"

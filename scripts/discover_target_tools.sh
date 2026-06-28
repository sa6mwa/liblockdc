#!/usr/bin/env bash
set -eu

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
  *apple-darwin) ;;
  *)
    echo "target-tool discovery currently supports Darwin targets only: $target_id" >&2
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

discover_tool() {
  local logical_tool=$1
  local tool_name=
  local explicit=
  local explicit_alt=
  local cmake_tool=
  local resolved=

  case "$logical_tool" in
    cc)
      tool_name=clang
      cmake_tool=$(cache_value CMAKE_C_COMPILER || true)
      ;;
    otool)
      tool_name=otool
      explicit=$(cache_value LOCKDC_OTOOL || true)
      explicit_alt=$(cache_value CPKT_OTOOL || true)
      cmake_tool=$(cache_value CMAKE_OTOOL || true)
      ;;
    install_name_tool)
      tool_name=install_name_tool
      cmake_tool=$(cache_value CMAKE_INSTALL_NAME_TOOL || true)
      ;;
    strip)
      tool_name=strip
      cmake_tool=$(cache_value CMAKE_STRIP || true)
      ;;
    readelf)
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
    "${compiler_dir:+$compiler_dir/$host-$tool_name}" \
    "${compiler_dir:+$compiler_dir/$tool_name}" \
    "${osxcross_bin:+$osxcross_bin/$host-$tool_name}" \
    "${osxcross_bin:+$osxcross_bin/$tool_name}" || true)

  if [ -n "$resolved" ]; then
    printf '%s\n' "$resolved"
    return 0
  fi

  if resolved=$(find_path_executable "$host-$tool_name" || true); then
    if [ -n "$resolved" ]; then
      printf '%s\n' "$resolved"
      return 0
    fi
  fi

  case "$logical_tool" in
    ld|linker)
      return 1
      ;;
    *)
      if resolved=$(find_path_executable "$tool_name" || true); then
        if [ -n "$resolved" ]; then
          printf '%s\n' "$resolved"
          return 0
        fi
      fi
      ;;
  esac

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
print_assignment TARGET_HOST "$host"
print_assignment CC "$cc"
print_assignment LD "$ld"
print_assignment LINKER "$ld"
print_assignment OTOOL "$otool"
print_assignment INSTALL_NAME_TOOL "$install_name_tool"
print_assignment STRIP "$strip"
print_assignment READELF "$readelf"

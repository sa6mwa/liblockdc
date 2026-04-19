#!/usr/bin/env bash

set -eu

if [ "$#" -ne 7 ]; then
  printf 'usage: %s CC CFLAGS LIBFLAG OBJ_EXTENSION LIB_EXTENSION LUA_INCDIR EXPECTED_LOCKDC_VERSION\n' "$0" >&2
  exit 1
fi

cc="$1"
cflags="$2"
libflag="$3"
obj_ext="$4"
lib_ext="$5"
lua_incdir="$6"
expected_lockdc_version="$7"

repo_root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
build_root="${LOCKDC_LUAROCKS_BUILD_ROOT:-${repo_root}/.luarocks-build}"
module_dir="${build_root}/lockdc"
object_path="${build_root}/lockdc_lua.${obj_ext}"
module_path="${module_dir}/core.${lib_ext}"
lockdc_release_base_url="https://github.com/sa6mwa/liblockdc/releases/download/v${expected_lockdc_version}"

if [ -z "${cc}" ]; then
  printf 'compiler command is empty\n' >&2
  exit 1
fi

detect_lockdc_release_target() {
  arch="$(uname -m 2>/dev/null || printf 'unknown')"
  os="$(uname -s 2>/dev/null || printf 'unknown')"

  case "$arch" in
    amd64) arch="x86_64" ;;
    arm64) arch="aarch64" ;;
    armv6l|armv7l) arch="armhf" ;;
  esac

  case "$os" in
    Linux)
      libc="gnu"
      if command -v ldd >/dev/null 2>&1 && ldd --version 2>&1 | grep -qi 'musl'; then
        libc="musl"
      fi
      printf '%s-linux-%s' "$arch" "$libc"
      ;;
    *)
      return 1
      ;;
  esac
}

lockdc_release_url() {
  if lockdc_target_id="$(detect_lockdc_release_target)"; then
    printf '%s/liblockdc-%s-%s.tar.gz' \
      "$lockdc_release_base_url" \
      "$expected_lockdc_version" \
      "$lockdc_target_id"
  else
    printf 'https://github.com/sa6mwa/liblockdc/releases/tag/v%s' "$expected_lockdc_version"
  fi
}

lockdc_dependency_error() {
  reason="$1"
  release_url="$(lockdc_release_url)"
  printf '%s\n' \
    "$reason" \
    "liblockdc ${expected_lockdc_version} is required by this Lua rock." \
    "Install the matching liblockdc SDK release tarball from:" \
    "  ${release_url}" \
    "Then set LOCKDC_PREFIX=/path/to/liblockdc-${expected_lockdc_version}-<target> or install lockdc.pc into pkg-config's search path." >&2
  exit 1
}

lockdc_shared_sdk_error() {
  lockdc_dependency_error \
    "normal LuaRocks builds require a shared liblockdc SDK; static-only SDKs are for vectis or in-tree embedded Lua builds"
}

lockdc_sdk_has_shared_library() {
  libdir="$1"
  if [ -z "$libdir" ] || [ ! -d "$libdir" ]; then
    return 1
  fi
  find "$libdir" -maxdepth 1 \
    \( -name 'liblockdc.so' -o -name 'liblockdc.so.*' -o -name 'liblockdc.dylib' -o -name 'lockdc.dll' -o -name 'liblockdc.dll' \) \
    | grep -q .
}

extract_lockdc_pc_version() {
  pc_path="$1"
  if [ ! -f "$pc_path" ]; then
    return 1
  fi
  sed -n 's/^Version:[[:space:]]*//p' "$pc_path" | head -n 1
}

resolve_lockdc_from_prefix() {
  prefix="$1"
  libdir="${prefix}/lib"
  pc_path="${libdir}/pkgconfig/lockdc.pc"
  version="$(extract_lockdc_pc_version "$pc_path" || true)"
  if [ -z "$version" ]; then
    lockdc_dependency_error "unable to determine liblockdc version from ${pc_path}"
  fi
  if ! lockdc_sdk_has_shared_library "$libdir"; then
    lockdc_shared_sdk_error
  fi

  lockdc_resolved_version="$version"
  lockdc_cflags="-I${prefix}/include"
  lockdc_libs="-L${libdir} -Wl,-rpath,${libdir} -llockdc"
}

resolve_lockdc_from_pkg_config() {
  if ! command -v pkg-config >/dev/null 2>&1 || ! pkg-config --exists lockdc; then
    return 1
  fi

  lockdc_pkgconfig_libdir="$(pkg-config --variable=libdir lockdc 2>/dev/null || true)"
  if ! lockdc_sdk_has_shared_library "$lockdc_pkgconfig_libdir"; then
    lockdc_shared_sdk_error
  fi

  lockdc_resolved_version="$(pkg-config --modversion lockdc)"
  lockdc_cflags="$(pkg-config --cflags lockdc)"
  lockdc_libs="$(pkg-config --libs lockdc)"
}

run_cc() {
  if [ -x "${cc}" ]; then
    "${cc}" "$@"
    return "$?"
  fi
  CC_LOCKDC="${cc}" sh -c '
    eval "set -- ${CC_LOCKDC} \"\$@\""
    exec "$@"
  ' sh "$@"
}

mkdir -p "${module_dir}"
rm -f "${object_path}" "${module_path}"

common_cflags="${cflags} -I${repo_root}/include -I${repo_root}/src -I${lua_incdir}"
linkflags="${LDFLAGS:-}"
lockdc_cflags=""
lockdc_libs=""
lockdc_resolved_version=""

if [ "$(uname -s)" = "Linux" ]; then
  linkflags="${linkflags} -Wl,--allow-shlib-undefined"
fi

if [ -n "${LOCKDC_PREFIX:-}" ]; then
  resolve_lockdc_from_prefix "${LOCKDC_PREFIX}"
elif [ -n "${LOCKDC_DIR:-}" ]; then
  resolve_lockdc_from_prefix "${LOCKDC_DIR}"
elif resolve_lockdc_from_pkg_config; then
  :
else
  lockdc_dependency_error "unable to resolve a liblockdc SDK installation"
fi

if [ "${lockdc_resolved_version}" != "${expected_lockdc_version}" ]; then
  lockdc_dependency_error "found liblockdc ${lockdc_resolved_version}, but this Lua rock requires liblockdc ${expected_lockdc_version}"
fi

run_cc ${common_cflags} ${lockdc_cflags} -c "${repo_root}/src/lua/lockdc_lua.c" -o "${object_path}"
run_cc ${libflag} -o "${module_path}" "${object_path}" ${linkflags} ${lockdc_libs}

#!/usr/bin/env bash

set -eu

if [ "$#" -ne 6 ]; then
  printf 'usage: %s CC CFLAGS LIBFLAG OBJ_EXTENSION LIB_EXTENSION LUA_INCDIR\n' "$0" >&2
  exit 1
fi

cc="$1"
cflags="$2"
libflag="$3"
obj_ext="$4"
lib_ext="$5"
lua_incdir="$6"

repo_root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
build_root="${LOCKDC_LUAROCKS_BUILD_ROOT:-${repo_root}/.luarocks-build}"
module_dir="${build_root}/lockdc"
object_path="${build_root}/lockdc_lua.${obj_ext}"
module_path="${module_dir}/core.${lib_ext}"

if [ -z "${cc}" ]; then
  printf 'compiler command is empty\n' >&2
  exit 1
fi

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

common_cflags="${cflags} -I${repo_root}/include -I${lua_incdir}"
linkflags="${LDFLAGS:-}"
lockdc_cflags=""
lockdc_libs=""

if [ "$(uname -s)" = "Linux" ]; then
  linkflags="${linkflags} -Wl,--allow-shlib-undefined"
fi

if command -v pkg-config >/dev/null 2>&1 && pkg-config --exists lockdc; then
  lockdc_cflags="$(pkg-config --cflags lockdc)"
  lockdc_libs="$(pkg-config --libs lockdc)"
elif [ -n "${LOCKDC_PREFIX:-}" ]; then
  lockdc_cflags="-I${LOCKDC_PREFIX}/include"
  lockdc_libs="-L${LOCKDC_PREFIX}/lib -Wl,-rpath,${LOCKDC_PREFIX}/lib -llockdc"
elif [ -n "${LOCKDC_DIR:-}" ]; then
  lockdc_cflags="-I${LOCKDC_DIR}/include"
  lockdc_libs="-L${LOCKDC_DIR}/lib -Wl,-rpath,${LOCKDC_DIR}/lib -llockdc"
else
  printf 'unable to resolve liblockdc install prefix; set LOCKDC_PREFIX or install lockdc.pc\n' >&2
  exit 1
fi

run_cc ${common_cflags} ${lockdc_cflags} -c "${repo_root}/src/lua/lockdc_lua.c" -o "${object_path}"
run_cc ${libflag} -o "${module_path}" "${object_path}" ${linkflags} ${lockdc_libs}

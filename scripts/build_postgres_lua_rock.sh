#!/usr/bin/env bash
set -euo pipefail
if [ "$#" -ne 7 ]; then
  echo 'usage: build_postgres_lua_rock.sh CC CFLAGS LIBFLAG OBJ_EXTENSION LIB_EXTENSION LUA_INCDIR VERSION' >&2
  exit 2
fi
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
build_root="$repo_root/.luarocks-build/postgres"
module_dir="$repo_root/.luarocks-build/lockdc"
source "$repo_root/scripts/assert_generated_path.sh"
lockdc_assert_generated_path "$repo_root" "$build_root"
lockdc_assert_generated_path "$repo_root" "$module_dir"
mkdir -p "$build_root/source" "$module_dir"
cat > "$build_root/source/CMakeLists.txt" <<'CMAKE'
cmake_minimum_required(VERSION 3.20)
project(lockdc_postgres_lua C)
# SDK prefixes are explicit host paths; the selected toolchain still governs
# compilation and the SDK packages validate their target identities.
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE BOTH)
find_package(lockdc_postgres ${EXPECTED_LOCKDC_VERSION} EXACT CONFIG REQUIRED COMPONENTS shared)
add_library(postgres MODULE
  "${LOCKDC_SOURCE_ROOT}/src/lua/lockdc_postgres_lua.c"
  "${LOCKDC_SOURCE_ROOT}/src/lua/lockdc_xa_provider_lua.c")
target_include_directories(postgres PRIVATE "${LOCKDC_SOURCE_ROOT}/src/lua")
target_include_directories(postgres SYSTEM PRIVATE "${LUA_INCLUDE_DIR}")
target_link_libraries(postgres PRIVATE lockdc::postgres_shared)
set_target_properties(postgres PROPERTIES C_STANDARD 90 C_STANDARD_REQUIRED ON C_EXTENSIONS OFF
  PREFIX "" OUTPUT_NAME postgres SUFFIX ".so" LIBRARY_OUTPUT_DIRECTORY "${LOCKDC_MODULE_DIR}")
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  target_compile_definitions(postgres PRIVATE _POSIX_C_SOURCE=200809L _FILE_OFFSET_BITS=64)
  target_link_options(postgres PRIVATE "LINKER:--allow-shlib-undefined" "LINKER:--disable-new-dtags")
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  target_link_options(postgres PRIVATE "LINKER:-undefined,dynamic_lookup")
endif()
CMAKE
prefixes="${LOCKDC_PREFIX:-${LOCKDC_DIR:-}};${LOCKDC_POSTGRES_PREFIX:-}"
toolchain_args=()
if [ -n "${LOCKDC_LUA_TOOLCHAIN_FILE:-}" ]; then
  toolchain_args=(-DCMAKE_TOOLCHAIN_FILE="$LOCKDC_LUA_TOOLCHAIN_FILE")
fi
if ! cmake -S "$build_root/source" -B "$build_root/build" \
  -DCMAKE_C_COMPILER="$1" -DCMAKE_C_FLAGS="$2" \
  -DCMAKE_MODULE_LINKER_FLAGS="${LDFLAGS:-}" \
  -DCMAKE_PREFIX_PATH="$prefixes" -DEXPECTED_LOCKDC_VERSION="$7" \
  -DLUA_INCLUDE_DIR="$6" -DLOCKDC_SOURCE_ROOT="$repo_root" \
  -DLOCKDC_MODULE_DIR="$module_dir" "${toolchain_args[@]}"; then
  printf '%s\n' "lockdc-postgres requires the shared liblockdc_postgres $7 SDK and compatible cpkt/cpktdb SDKs." \
    'Set LOCKDC_PREFIX, LOCKDC_POSTGRES_PREFIX and CMAKE_PREFIX_PATH to their installed prefixes.' >&2
  exit 1
fi
cmake --build "$build_root/build" --target postgres

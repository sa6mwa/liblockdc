#!/usr/bin/env bash
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)

resolve_host_arch() {
  local compiler triple

  compiler=${CC:-cc}
  triple=$("$compiler" -dumpmachine 2>/dev/null || true)

  case "$triple" in
    x86_64*-linux-musl*|x86_64*-linux-gnu*|x86_64*-linux)
      printf '%s\n' x86_64
      ;;
    aarch64*-linux-musl*|aarch64*-linux-gnu*|aarch64*-linux)
      printf '%s\n' aarch64
      ;;
    arm*-linux-musleabihf*|armv7*-linux-musleabihf*|arm*-linux-musl*|armv7*-linux-musl*|arm*-linux-gnueabihf*|armv7*-linux-gnueabihf*|arm*-linux-gnu*|armv7*-linux-gnu*)
      printf '%s\n' armhf
      ;;
    *)
      printf 'unsupported native host compiler triple for host tests: %s\n' "${triple:-unknown}" >&2
      exit 1
      ;;
  esac
}

have_native_musl_toolchain() {
  case "$1" in
    x86_64)
      command -v musl-gcc >/dev/null 2>&1
      ;;
    aarch64)
      [ -x "$HOME/.local/cross/aarch64-linux-musl/bin/aarch64-linux-musl-gcc" ]
      ;;
    armhf)
      [ -x "$HOME/.local/cross/arm-linux-musleabihf/bin/arm-linux-musleabihf-gcc" ]
      ;;
    *)
      return 1
      ;;
  esac
}

arch=$(resolve_host_arch)
presets=("${arch}-linux-gnu-release")
deps_presets=(deps-host-debug)

if have_native_musl_toolchain "$arch"; then
  presets+=("${arch}-linux-musl-release")
  deps_presets+=("deps-${arch}-linux-musl")
fi

unset LD_LIBRARY_PATH

ctest_timeout=${LOCKDC_CTEST_TIMEOUT:-300}

cd "$repo_root"

if [ "${LOCKDC_HOST_TEST_DRY_RUN:-0}" = "1" ]; then
  for deps_preset in "${deps_presets[@]}"; do
    printf 'deps_preset=%s\n' "$deps_preset"
  done
  for preset in "${presets[@]}"; do
    printf 'preset=%s\n' "$preset"
  done
  exit 0
fi

for deps_preset in "${deps_presets[@]}"; do
  "$script_dir/deps.sh" "$deps_preset"
done

for preset in "${presets[@]}"; do
  "$script_dir/build.sh" "$preset"
  ctest --preset "$preset" --output-on-failure --progress --stop-on-failure --timeout "$ctest_timeout"
done

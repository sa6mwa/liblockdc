SHELL := bash
.DEFAULT_GOAL := help
MAKEFLAGS += --no-builtin-rules

ROOT := $(CURDIR)
CMAKE := cmake
CTEST := ctest
CLANG_FORMAT := clang-format
TIMED := bash ./scripts/run_timed.sh

DEBUG_PRESET := debug
E2E_PRESET := e2e
X86_64_GNU_RELEASE_PRESET := x86_64-linux-gnu-release
ASAN_PRESET := asan
COVERAGE_PRESET := coverage
FUZZ_PRESET := fuzz

DEBUG_BUILD_DIR := $(ROOT)/build/$(DEBUG_PRESET)
E2E_BUILD_DIR := $(ROOT)/build/$(E2E_PRESET)
X86_64_GNU_RELEASE_BUILD_DIR := $(ROOT)/build/$(X86_64_GNU_RELEASE_PRESET)
ASAN_BUILD_DIR := $(ROOT)/build/$(ASAN_PRESET)
COVERAGE_BUILD_DIR := $(ROOT)/build/$(COVERAGE_PRESET)

DIST_DIR := $(ROOT)/dist
BENCH_ITERS ?= 0
FUZZ_TIME ?= 30

.PHONY: \
	help \
	__deps-debug __deps-release __deps-cross \
	__build-debug __build-x86_64-linux-gnu-release __build-release __build-e2e __build-asan __build-coverage __build-fuzz \
	__test-debug __test-host __test-cross __test-e2e __test-all __test-asan __test-coverage \
	__format \
	__finalize-slice __valgrind __asan __coverage __fuzz __fuzz-smoke __benchmarks __bench-gate \
	__package __package-source __package-source-smoke __package-checksums __package-verify __clean-dist \
	__lua-rock __lua-test __lua-env \
	__dev-up __dev-down __dev-reset __cross-build __cross-preset-test __cross-test \
	__prerelease __prerelease-live __prerelease-hardening __release __release-matrix __release-package-only __clean \
	deps-debug deps-release deps-cross \
	build build-debug build-release build-e2e build-asan build-coverage build-fuzz \
	test test-debug test-host test-cross test-e2e test-all test-asan test-coverage \
	format \
	finalize-slice valgrind asan coverage fuzz fuzz-smoke benchmarks bench-gate \
	package package-source package-source-smoke package-checksums package-verify verify-release-archives clean-dist \
	lua-rock lua-test lua-env \
	dev-up dev-down dev-reset cross-build cross-preset-test cross-test \
	prerelease prerelease-live prerelease-hardening release release-matrix clean

help:
	@printf '%s\n' \
		'make build              Configure and build the ASan/UBSan debug preset.' \
		'make build-debug        Configure and build the ASan/UBSan debug preset.' \
		'make build-release      Configure and build the full shipped Linux release matrix.' \
		'make build-e2e          Configure and build the e2e preset.' \
		'make build-asan         Compatibility alias for the ASan/UBSan debug build.' \
		'make build-coverage     Configure and build the coverage preset.' \
		'make build-fuzz         Configure and build the fuzz preset.' \
		'make deps-debug         Provision the host-native release dependency tree used by debug/e2e/asan/coverage/fuzz.' \
		'make deps-release       Provision the shipped x86_64 GNU/musl release dependency trees.' \
		'make deps-cross         Provision all non-host cross release dependency trees.' \
		'make test-debug         Run the ASan/UBSan debug preset test suite.' \
		'make test               Run the host-native release suite (GNU plus musl when the native musl toolchain is available).' \
		'make test-host          Run the host-native release suite (GNU plus musl when the native musl toolchain is available).' \
		'make test-cross         Run the non-host cross release suites.' \
		'make test-e2e           Run the mTLS/libcurl e2e preset against the local devenv.' \
		'make test-all           Run ASan/UBSan debug first, then host release and non-host cross release suites.' \
		'make test-asan          Compatibility alias for test-debug.' \
		'make test-coverage      Run the coverage preset test suite and build the coverage report.' \
		'make dev-up             Start the local compose-backed devenv and wait for generated client bundles.' \
		'make dev-down           Stop and remove the local compose-backed devenv.' \
		'make dev-reset          Stop the local compose-backed devenv and remove its generated state.' \
		'make format             Run clang-format over repo .c and .h files.' \
		'make finalize-slice     Run formatting plus the host release test gate for an ordinary implementation slice.' \
		'make valgrind           Native Valgrind lifecycle gate; currently fails with an actionable migration diagnostic until the Bootlin Valgrind preset lands.' \
		'make asan               Compatibility alias for test-debug.' \
		'make coverage           Run the coverage preset and generate coverage-report.' \
		'make fuzz               Build fuzz targets and run bounded corpus passes.' \
		'make fuzz-smoke         Build fuzz targets and run short bounded corpus passes (FUZZ_TIME=5).' \
		'make benchmarks         Build the shipped x86_64-linux-gnu release preset and run the local benchmark matrix (BENCH_ITERS=$(BENCH_ITERS)).' \
		'make bench-gate         Compatibility alias for benchmarks.' \
		'make package            Build the shipped x86_64-linux-gnu release preset and write the combined release archive, source archive, and Lua source rock to dist/.' \
		'make package-source     Build the source-only release archive.' \
		'make package-source-smoke  Build and verify the source-only release archive.' \
		'make package-checksums  Refresh the dist/ checksum manifest.' \
		'make package-verify     Build release packages and run package verification.' \
		'make verify-release-archives  Assert the complete shipped Linux release archive set and checksums.' \
		'make lua-rock           Build the Lua release package and source rock artifacts.' \
		'make lua-test           Run local Lua layout, SDK, facade, and binding smoke tests.' \
		'make lua-env            Print shell exports for the repo-local Lua rock tree.' \
		'make clean-dist         Reset dist/ release artifacts.' \
		'make cross-build        Build all non-host cross release presets.' \
		'make cross-preset-test  Run the host ASan/UBSan debug cross-preset packaging-isolation check.' \
		'make cross-test         Run the host cross-preset isolation check plus all non-host cross release preset tests against existing build trees.' \
		'make prerelease         Run deterministic local prerelease confidence: finalize-slice, test-all, package-verify, and lua-test.' \
		'make prerelease-live    Refuse without LOCKDC_PRERELEASE_LIVE=1; no live-provider checks are currently defined.' \
		'make prerelease-hardening  Run prerelease plus fuzz smoke, benchmark gate, and release matrix.' \
		'make release            Run the clean-slate final release workflow: tests, e2e, benchmarks, package generation, and final release verification; fuzz is included when Clang/libFuzzer is available.' \
		'make release-matrix     Rebuild, test, package, and verify the release matrix while reusing existing build and dependency caches.' \
		'make clean              Remove generated build, cache, dist, and devenv state.'

build: build-debug

deps-debug:
	$(TIMED) deps-debug $(MAKE) __deps-debug

__deps-debug:
	bash ./scripts/deps.sh deps-host-debug

deps-release:
	$(TIMED) deps-release $(MAKE) __deps-release

__deps-release:
	bash ./scripts/deps.sh deps-x86_64-linux-gnu
	bash ./scripts/deps.sh deps-x86_64-linux-musl

deps-cross:
	$(TIMED) deps-cross $(MAKE) __deps-cross

__deps-cross:
	bash ./scripts/deps.sh deps-aarch64-linux-gnu
	bash ./scripts/deps.sh deps-aarch64-linux-musl
	bash ./scripts/deps.sh deps-armhf-linux-gnu
	bash ./scripts/deps.sh deps-armhf-linux-musl
	if bash ./scripts/osxcross_available.sh; then bash ./scripts/deps.sh deps-arm64-apple-darwin; else printf '[deps] skipping deps-arm64-apple-darwin: osxcross toolchain not available\n'; fi

build-debug:
	$(TIMED) build-debug $(MAKE) __build-debug

__build-debug: __deps-debug
	$(CMAKE) --preset $(DEBUG_PRESET)
	$(CMAKE) --build --preset $(DEBUG_PRESET)

build-release:
	$(TIMED) build-release $(MAKE) __build-release

__build-x86_64-linux-gnu-release: __deps-release
	$(CMAKE) --preset $(X86_64_GNU_RELEASE_PRESET)
	$(CMAKE) --build --preset $(X86_64_GNU_RELEASE_PRESET)

__build-release: __deps-release __deps-cross
	bash ./scripts/run_linux_build_matrix.sh

build-e2e:
	$(TIMED) build-e2e $(MAKE) __build-e2e

__build-e2e: __deps-debug
	$(CMAKE) --preset $(E2E_PRESET)
	$(CMAKE) --build --preset $(E2E_PRESET)

build-asan:
	$(TIMED) build-asan $(MAKE) __build-asan

__build-asan: __deps-debug
	$(CMAKE) --preset $(ASAN_PRESET)
	$(CMAKE) --build --preset $(ASAN_PRESET)

build-coverage:
	$(TIMED) build-coverage $(MAKE) __build-coverage

__build-coverage: __deps-debug
	$(CMAKE) --preset $(COVERAGE_PRESET)
	$(CMAKE) --build --preset $(COVERAGE_PRESET)

build-fuzz:
	$(TIMED) build-fuzz $(MAKE) __build-fuzz

__build-fuzz: __deps-debug
	$(CMAKE) --preset $(FUZZ_PRESET)
	$(CMAKE) --build --preset $(FUZZ_PRESET)

test: test-host

test-debug:
	$(TIMED) test-debug $(MAKE) __test-debug

__test-debug: __build-debug
	$(CTEST) --preset $(DEBUG_PRESET)

test-host:
	$(TIMED) test-host $(MAKE) __test-host

__test-host:
	bash ./scripts/host_test.sh

test-cross:
	$(TIMED) test-cross $(MAKE) __test-cross

__test-cross: __cross-build
	bash ./scripts/cross_test.sh release

test-e2e:
	$(TIMED) test-e2e $(MAKE) __test-e2e

__test-e2e:
	bash ./scripts/test.sh e2e

test-all:
	$(TIMED) test-all $(MAKE) __test-all

__test-all: __test-debug __test-host __test-cross

dev-up:
	$(TIMED) dev-up $(MAKE) __dev-up

__dev-up:
	bash ./scripts/dev-up.sh

dev-down:
	$(TIMED) dev-down $(MAKE) __dev-down

__dev-down:
	bash ./scripts/dev-down.sh

dev-reset:
	$(TIMED) dev-reset $(MAKE) __dev-reset

__dev-reset:
	bash ./scripts/dev-reset.sh

format:
	$(TIMED) format $(MAKE) __format

__format:
	rg --files -g '*.c' -g '*.h' | xargs $(CLANG_FORMAT) -i

finalize-slice:
	$(TIMED) finalize-slice $(MAKE) __finalize-slice

__finalize-slice: __format __test-host

valgrind:
	$(TIMED) valgrind $(MAKE) __valgrind

__valgrind:
	@printf '%s\n' \
		'PKT_DIAGNOSTIC_BEGIN' \
		'surface=make valgrind' \
		'phase=lifecycle-migration' \
		'status=failed' \
		'class=external-tool-unavailable' \
		'reason=valgrind-preset-not-implemented' \
		'artifact=CMakePresets.json' \
		'next=add the lifecycle valgrind preset backed by the pinned native x86_64 Bootlin toolchain, then run host Valgrind Memcheck' \
		'PKT_DIAGNOSTIC_END' >&2
	@exit 2

test-asan:
	$(TIMED) test-asan $(MAKE) __test-asan

__test-asan: __test-debug

asan:
	$(TIMED) asan $(MAKE) __asan

__asan: __test-debug

test-coverage:
	$(TIMED) test-coverage $(MAKE) __test-coverage

__test-coverage: __build-coverage
	$(CTEST) --preset $(COVERAGE_PRESET)
	$(CMAKE) --build --preset coverage-report

coverage:
	$(TIMED) coverage $(MAKE) __coverage

__coverage: __test-coverage

fuzz:
	$(TIMED) fuzz $(MAKE) __fuzz

__fuzz:
	bash ./scripts/fuzz.sh $(FUZZ_TIME)

fuzz-smoke:
	$(TIMED) fuzz-smoke $(MAKE) __fuzz-smoke

__fuzz-smoke:
	bash ./scripts/fuzz.sh 5

benchmarks:
	$(TIMED) benchmarks $(MAKE) __benchmarks

__benchmarks: __build-x86_64-linux-gnu-release
	./build/$(X86_64_GNU_RELEASE_PRESET)/bench/lockdc_bench $(BENCH_ITERS) all

bench-gate:
	$(TIMED) bench-gate $(MAKE) __bench-gate

__bench-gate: __benchmarks

package:
	$(TIMED) package $(MAKE) __package

__package: __build-x86_64-linux-gnu-release
	$(CMAKE) -DLOCKDC_BINARY_DIR=$(X86_64_GNU_RELEASE_BUILD_DIR) -DLOCKDC_ROOT=$(ROOT) -DLOCKDC_DIST_DIR=$(DIST_DIR) -P $(ROOT)/cmake/package_archive.cmake
	$(CMAKE) -DLOCKDC_BINARY_DIR=$(X86_64_GNU_RELEASE_BUILD_DIR) -DLOCKDC_ROOT=$(ROOT) -DLOCKDC_DIST_DIR=$(DIST_DIR) -P $(ROOT)/cmake/package_source.cmake
	$(CMAKE) -DLOCKDC_BINARY_DIR=$(X86_64_GNU_RELEASE_BUILD_DIR) -DLOCKDC_ROOT=$(ROOT) -DLOCKDC_DIST_DIR=$(DIST_DIR) -P $(ROOT)/cmake/package_lua_rock.cmake

package-source:
	$(TIMED) package-source $(MAKE) __package-source

__package-source: __build-x86_64-linux-gnu-release
	$(CMAKE) -DLOCKDC_BINARY_DIR=$(X86_64_GNU_RELEASE_BUILD_DIR) -DLOCKDC_ROOT=$(ROOT) -DLOCKDC_DIST_DIR=$(DIST_DIR) -P $(ROOT)/cmake/package_source.cmake

package-source-smoke:
	$(TIMED) package-source-smoke $(MAKE) __package-source-smoke

__package-source-smoke: __package-source
	bash ./scripts/test_release_source.sh $(ROOT) $$(ls -t $(DIST_DIR)/liblockdc-*.tar.gz | grep -v -- 'liblockdc-lua-' | grep -v -- '-linux-' | grep -v -- '-apple-darwin' | head -n1)

package-checksums:
	$(TIMED) package-checksums $(MAKE) __package-checksums

__package-checksums: __deps-release
	$(CMAKE) --preset $(X86_64_GNU_RELEASE_PRESET)
	$(CMAKE) -DLOCKDC_BINARY_DIR=$(X86_64_GNU_RELEASE_BUILD_DIR) -DLOCKDC_ROOT=$(ROOT) -DLOCKDC_DIST_DIR=$(DIST_DIR) -P $(ROOT)/cmake/package_source.cmake
	$(CMAKE) -DLOCKDC_BINARY_DIR=$(X86_64_GNU_RELEASE_BUILD_DIR) -DLOCKDC_ROOT=$(ROOT) -DLOCKDC_DIST_DIR=$(DIST_DIR) -P $(ROOT)/cmake/package_lua_rock.cmake
	$(CMAKE) -DLOCKDC_BINARY_DIR=$(X86_64_GNU_RELEASE_BUILD_DIR) -DLOCKDC_ROOT=$(ROOT) -DLOCKDC_DIST_DIR=$(DIST_DIR) -P $(ROOT)/cmake/package_checksums.cmake

package-verify:
	$(TIMED) package-verify $(MAKE) __package-verify

__package-verify:
	bash ./scripts/package-verify.sh

lua-rock:
	$(TIMED) lua-rock $(MAKE) __lua-rock

__lua-rock: __build-x86_64-linux-gnu-release
	$(CMAKE) -DLOCKDC_BINARY_DIR=$(X86_64_GNU_RELEASE_BUILD_DIR) -DLOCKDC_ROOT=$(ROOT) -DLOCKDC_DIST_DIR=$(DIST_DIR) -P $(ROOT)/cmake/package_lua_rock.cmake

lua-test:
	$(TIMED) lua-test $(MAKE) __lua-test

__lua-test: __build-debug
	$(CTEST) --test-dir $(DEBUG_BUILD_DIR) -R '^lua_(binding_source_layout|external_sdk_contract|facade_unit|binding_core_smoke)_test$$' --output-on-failure

lua-env:
	$(TIMED) lua-env $(MAKE) __lua-env

__lua-env:
	@printf 'export LOCKDC_PREFIX=%s\n' '$(X86_64_GNU_RELEASE_BUILD_DIR)/package/liblockdc-$$(sed -n '"'"'s/^set(LOCKDC_VERSION "\(.*\)")$$/\1/p'"'"' $(X86_64_GNU_RELEASE_BUILD_DIR)/package-metadata.cmake)-x86_64-linux-gnu'
	@printf 'export LUA_PATH=%s\n' '$(ROOT)/lua/?.lua;$(ROOT)/lua/?/init.lua;;'
	@printf 'export LUA_CPATH=%s\n' '$(ROOT)/build/luarocks/lib/lua/5.5/?.so;;'

verify-release-archives:
	release_presets='x86_64-linux-gnu-release;x86_64-linux-musl-release;aarch64-linux-gnu-release;aarch64-linux-musl-release;armhf-linux-gnu-release;armhf-linux-musl-release'; \
	if bash ./scripts/osxcross_available.sh; then release_presets="$$release_presets;arm64-apple-darwin-release"; fi; \
	$(CMAKE) -DLOCKDC_ROOT=$(ROOT) -DLOCKDC_DIST_DIR=$(DIST_DIR) -DLOCKDC_RELEASE_PRESETS="$$release_presets" -P $(ROOT)/tests/release_matrix_archives_test.cmake

clean-dist:
	$(TIMED) clean-dist $(MAKE) __clean-dist

__clean-dist:
	$(CMAKE) -DLOCKDC_ROOT=$(ROOT) -DLOCKDC_DIST_DIR=$(DIST_DIR) -P $(ROOT)/cmake/package_clean_dist.cmake

cross-build:
	$(TIMED) cross-build $(MAKE) __cross-build

__cross-build: __deps-cross
	bash ./scripts/cross_build.sh

cross-preset-test:
	$(TIMED) cross-preset-test $(MAKE) __cross-preset-test

__cross-preset-test:
	bash ./scripts/cross_test.sh preset

cross-test:
	$(TIMED) cross-test $(MAKE) __cross-test

__cross-test: __cross-build
	bash ./scripts/cross_test.sh all

release:
	$(TIMED) release $(MAKE) __release

prerelease:
	$(TIMED) prerelease $(MAKE) __prerelease

__prerelease: __finalize-slice __test-all __package-verify __lua-test

prerelease-live:
	$(TIMED) prerelease-live $(MAKE) __prerelease-live

__prerelease-live:
	@if [ "$${LOCKDC_PRERELEASE_LIVE:-}" != "1" ]; then \
		printf '%s\n' \
			'PKT_DIAGNOSTIC_BEGIN' \
			'surface=make prerelease-live' \
			'phase=opt-in' \
			'status=failed' \
			'class=external-tool-unavailable' \
			'reason=live-prerelease-not-enabled' \
			'artifact=LOCKDC_PRERELEASE_LIVE' \
			'next=set LOCKDC_PRERELEASE_LIVE=1 only when live provider checks are defined and credentials are available' \
			'PKT_DIAGNOSTIC_END' >&2; \
		exit 2; \
	fi
	@printf '%s\n' '[prerelease-live] no live-provider checks are currently defined'

prerelease-hardening:
	$(TIMED) prerelease-hardening $(MAKE) __prerelease-hardening

__prerelease-hardening: __prerelease __fuzz-smoke __bench-gate __release-matrix

__release:
	bash ./scripts/release.sh

release-matrix:
	$(TIMED) release-matrix $(MAKE) __release-matrix

__release-matrix: __build-release
	$(MAKE) __test-host
	bash ./scripts/cross_test.sh release
	bash ./scripts/run_linux_package_matrix.sh

__release-package-only: __build-release
	bash ./scripts/run_linux_package_matrix.sh

clean:
	$(TIMED) clean $(MAKE) __clean

__clean:
	bash ./scripts/clean.sh

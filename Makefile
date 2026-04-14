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
BENCH_ITERS ?= 200000
FUZZ_TIME ?= 30

.PHONY: \
	help \
	__deps-debug __deps-release __deps-cross \
	__build-debug __build-x86_64-linux-gnu-release __build-release __build-e2e __build-asan __build-coverage __build-fuzz \
	__test-debug __test-host __test-cross __test-e2e __test-all __test-asan __test-coverage \
	__asan __coverage __fuzz __benchmarks \
	__package __package-checksums __clean-dist \
	__dev-up __dev-down __dev-reset __cross-build __cross-preset-test __cross-test __release __clean __world \
	deps-debug deps-release deps-cross \
	build build-debug build-release build-e2e build-asan build-coverage build-fuzz \
	test test-debug test-host test-cross test-e2e test-all test-asan test-coverage \
	format \
	asan coverage fuzz benchmarks \
	package package-checksums verify-release-archives clean-dist \
	dev-up dev-down dev-reset cross-build cross-preset-test cross-test release clean world

help:
	@printf '%s\n' \
		'make build              Configure and build the debug preset.' \
		'make build-release      Configure and build the full shipped Linux release matrix.' \
		'make build-e2e          Configure and build the e2e preset.' \
		'make build-asan         Configure and build the ASan/UBSan preset.' \
		'make deps-release       Provision the shipped x86_64 GNU/musl release dependency trees.' \
		'make deps-cross         Provision all non-host cross release dependency trees.' \
		'make test               Run the host release suite (x86_64 GNU + musl).' \
		'make test-cross         Run the non-host cross release suites.' \
		'make test-e2e           Run the mTLS/libcurl e2e preset against the local devenv.' \
		'make test-all           Run host release suites plus non-host cross release suites.' \
		'make dev-up             Start the local compose-backed devenv and wait for generated client bundles.' \
		'make dev-down           Stop and remove the local compose-backed devenv.' \
		'make dev-reset          Stop the local compose-backed devenv and remove its generated state.' \
		'make format             Run clang-format over repo .c and .h files.' \
		'make asan               Run the ASan/UBSan preset test suite.' \
		'make coverage           Run the coverage preset and generate coverage-report.' \
		'make fuzz               Build fuzz targets and run bounded corpus passes.' \
		'make benchmarks         Build the shipped x86_64-linux-gnu release preset and run the local benchmark matrix (BENCH_ITERS=$(BENCH_ITERS)).' \
		'make package            Build the shipped x86_64-linux-gnu release preset and write the combined release archive to dist/.' \
		'make package-checksums  Refresh the dist/ checksum manifest.' \
		'make verify-release-archives  Assert the complete shipped Linux release archive set and checksums.' \
		'make clean-dist         Reset dist/ release artifacts.' \
		'make cross-build        Build all non-host cross release presets.' \
		'make cross-preset-test  Run the host debug/asan cross-preset packaging-isolation check.' \
		'make cross-test         Run the host cross-preset isolation check plus all non-host cross release preset tests against existing build trees.' \
		'make release            Run the full Linux release matrix and package generation.' \
		'make world              Run the full clean-slate workflow: clean, builds, tests, coverage, e2e, benchmarks, and final release verification; fuzz is included when Clang/libFuzzer is available.' \
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

__test-host: __deps-release
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

__test-all: __test-host __test-cross

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
	rg --files -g '*.c' -g '*.h' | xargs $(CLANG_FORMAT) -i

test-asan:
	$(TIMED) test-asan $(MAKE) __test-asan

__test-asan: __build-asan
	$(CTEST) --preset $(ASAN_PRESET)

asan:
	$(TIMED) asan $(MAKE) __asan

__asan: __test-asan

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

benchmarks:
	$(TIMED) benchmarks $(MAKE) __benchmarks

__benchmarks: __build-x86_64-linux-gnu-release
	./build/$(X86_64_GNU_RELEASE_PRESET)/bench/lockdc_bench $(BENCH_ITERS) all

package:
	$(TIMED) package $(MAKE) __package

__package: __build-x86_64-linux-gnu-release
	$(CMAKE) -DLOCKDC_BINARY_DIR=$(X86_64_GNU_RELEASE_BUILD_DIR) -DLOCKDC_ROOT=$(ROOT) -DLOCKDC_DIST_DIR=$(DIST_DIR) -P $(ROOT)/cmake/package_archive.cmake

package-checksums:
	$(TIMED) package-checksums $(MAKE) __package-checksums

__package-checksums: __deps-release
	$(CMAKE) --preset $(X86_64_GNU_RELEASE_PRESET)
	$(CMAKE) -DLOCKDC_BINARY_DIR=$(X86_64_GNU_RELEASE_BUILD_DIR) -DLOCKDC_ROOT=$(ROOT) -DLOCKDC_DIST_DIR=$(DIST_DIR) -P $(ROOT)/cmake/package_checksums.cmake

verify-release-archives:
	$(CMAKE) -DLOCKDC_ROOT=$(ROOT) -DLOCKDC_DIST_DIR=$(DIST_DIR) -DLOCKDC_RELEASE_PRESETS='x86_64-linux-gnu-release;x86_64-linux-musl-release;aarch64-linux-gnu-release;aarch64-linux-musl-release;armhf-linux-gnu-release;armhf-linux-musl-release' -P $(ROOT)/tests/release_matrix_archives_test.cmake

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

__release: __deps-release __deps-cross
	bash ./scripts/run_linux_release_matrix.sh

world:
	$(TIMED) world $(MAKE) __world

__world:
	bash ./scripts/world.sh

clean:
	$(TIMED) clean $(MAKE) __clean

__clean:
	bash ./scripts/clean.sh

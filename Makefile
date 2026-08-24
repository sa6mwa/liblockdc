SHELL := bash
.DEFAULT_GOAL := help
MAKEFLAGS += --no-builtin-rules
.NOTPARALLEL:

ROOT := $(CURDIR)
CMAKE := cmake
CTEST := ctest
CLANG_FORMAT := clang-format
GO := go
TIMED := bash ./scripts/run_timed.sh
# Keep recursive calls print-only under `make -n`. GNU Make executes recipe
# lines that directly reference $(MAKE), even in dry-run mode.
MAKE_RECURSE := $(MAKE)

DEBUG_PRESET := debug
E2E_PRESET := e2e
X86_64_GNU_RELEASE_PRESET := x86_64-linux-gnu-release
COVERAGE_PRESET := coverage
FUZZ_PRESET := fuzz

DEBUG_BUILD_DIR := $(ROOT)/build/$(DEBUG_PRESET)
E2E_BUILD_DIR := $(ROOT)/build/$(E2E_PRESET)
X86_64_GNU_RELEASE_BUILD_DIR := $(ROOT)/build/$(X86_64_GNU_RELEASE_PRESET)
COVERAGE_BUILD_DIR := $(ROOT)/build/$(COVERAGE_PRESET)

DIST_DIR := $(ROOT)/dist
BENCH_ITERS ?= 0
POUCH_GO_BENCH ?= .
POUCH_GO_BENCHTIME ?= 3s
POUCH_GO_BENCH_COUNT ?= 1
POUCH_GO_TEST_RUN ?= ^$$
POUCH_GO_SEED_ROWS ?= 10000
POUCH_GO_TEST_TIMEOUT ?= 10m
LOCKD_GO_VERSION ?= v0.9.0
POUCH_GO_FAST_BENCH ?= Fast
POUCH_GO_FAST_BENCHTIME ?= 1x
POUCH_GO_FAST_SEED_ROWS ?= 64
POUCH_GO_FAST_TIMEOUT ?= 30s
POUCH_GO_MEDIUM_BENCH ?= Medium
POUCH_GO_MEDIUM_BENCHTIME ?= 1x
POUCH_GO_MEDIUM_SEED_ROWS ?= 64
POUCH_GO_MEDIUM_SCALE_ROWS ?= 64,1024
POUCH_GO_MEDIUM_SCALE_SCENARIOS ?= EqSparse,EqDense,RangeHalf,InRegionSingle,InTags,ContainsMessage,IprefixTags,IcontainsTags,OrSparseOrFlag,DateAfter,RecursiveExists,TenantEnterprise,WorkflowEscalated,AmountBand,RiskSignal,NarrativeSummary,NarrativeDescription,FullTextAny
POUCH_GO_MEDIUM_TIMEOUT ?= 3m
POUCH_GO_ACCEPTANCE_BENCH ?= Medium(LQL|LockdDisk)(Documents|Keys)/Docs4096/index/(EqSparse|RangeHalf|InTags|ContainsMessage|DateAfter|OrSparseOrFlag|TenantEnterprise|WorkflowEscalated|AmountBand|RiskSignal|NarrativeSummary|NarrativeDescription|FullTextAny)|MediumLQL(Documents|Keys)/Docs4096/scan/(EqSparse|RangeHalf|InTags|ContainsMessage|DateAfter|OrSparseOrFlag)
POUCH_GO_ACCEPTANCE_BENCHTIME ?= 1x
POUCH_GO_ACCEPTANCE_SEED_ROWS ?= 64
POUCH_GO_ACCEPTANCE_SCALE_ROWS ?= 4096
POUCH_GO_ACCEPTANCE_SCALE_SCENARIOS ?= EqSparse,RangeHalf,InTags,ContainsMessage,DateAfter,OrSparseOrFlag,TenantEnterprise,WorkflowEscalated,AmountBand,RiskSignal,NarrativeSummary,NarrativeDescription,FullTextAny
POUCH_GO_ACCEPTANCE_TIMEOUT ?= 3m
POUCH_GO_PRODUCTION_BENCH ?= Production(PouchPT|PouchCrypto|PouchCompression|PouchCryptoCompression|LockdDiskNoCrypto|LockdDiskCrypto)
POUCH_GO_PRODUCTION_BENCHTIME ?= 1x
# Preserve the first-segment end anchor through the three recursive benchmark
# make invocations. It keeps the PouchCrypto compression variants out of the
# transform-equivalent release comparison.
POUCH_GO_PARITY_BENCH ?= ^BenchmarkProduction(PouchPT|PouchCrypto|LockdDiskNoCrypto|LockdDiskCrypto)$$$$$$$$/.*
POUCH_GO_PARITY_BENCHTIME ?= 1x
POUCH_GO_PARITY_COUNT ?= 3
POUCH_GO_PARITY_MIN_SPEEDUP ?= 1.25
POUCH_GO_PARITY_TIMEOUT ?= 15m
POUCH_GO_DURABLE_BENCH ?= Production(PouchDurablePT|PouchDurableCrypto|LockdDiskDurableNoCrypto|LockdDiskDurableCrypto)
POUCH_GO_DURABLE_BENCHTIME ?= 1x
POUCH_GO_DURABLE_COUNT ?= 1
POUCH_GO_DURABLE_ROWS ?= 12
POUCH_GO_DURABLE_UPDATES ?= 2
POUCH_GO_DURABLE_PAYLOAD_BYTES ?= 131072
POUCH_GO_DURABLE_SEGMENT_TARGET_BYTES ?= 16384
POUCH_GO_DURABLE_TIMEOUT ?= 90s
POUCH_GO_DURABLE_GATE_TIMEOUT ?= 3m
POUCH_GO_CORE_SOAK_BENCH ?= ProductionPouch(PT|Crypto|Compression|CryptoCompression)
POUCH_GO_CORE_SOAK_BENCHTIME ?= 4x
POUCH_GO_CORE_SOAK_ROWS ?= 96
POUCH_GO_CORE_SOAK_UPDATES ?= 8
POUCH_GO_CORE_SOAK_PAYLOAD_BYTES ?= 32768
POUCH_GO_CORE_SOAK_SEGMENT_TARGET_BYTES ?= 262144
POUCH_GO_CORE_SOAK_TIMEOUT ?= 10m
POUCH_GO_PRODUCTION_ROWS ?=
POUCH_GO_PRODUCTION_UPDATES ?=
POUCH_GO_PRODUCTION_PAYLOAD_BYTES ?=
POUCH_GO_PRODUCTION_SEGMENT_TARGET_BYTES ?=
POUCH_GO_PRODUCTION_TIMEOUT ?= 10m
POUCH_GO_BOUNDED_PRODUCTION_ROWS ?= 12
POUCH_GO_BOUNDED_PRODUCTION_UPDATES ?= 2
POUCH_GO_BOUNDED_PRODUCTION_PAYLOAD_BYTES ?= 131072
POUCH_GO_BOUNDED_PRODUCTION_SEGMENT_TARGET_BYTES ?= 16384
POUCH_GO_BOUNDED_PRODUCTION_TIMEOUT ?= 90s
POUCH_GO_COMPACTION_BENCH ?= CompactionPouch
POUCH_GO_COMPACTION_BENCHTIME ?= 1x
POUCH_GO_COMPACTION_ROWS ?=
POUCH_GO_COMPACTION_UPDATES ?=
POUCH_GO_COMPACTION_PAYLOAD_BYTES ?=
POUCH_GO_COMPACTION_SEGMENT_TARGET_BYTES ?=
POUCH_GO_COMPACTION_MIN_SEGMENTS ?=
POUCH_GO_COMPACTION_MIN_RECLAIMABLE_BYTES ?=
POUCH_GO_COMPACTION_TIMEOUT ?= 10m
POUCH_GO_CONCURRENCY_BENCH ?= Concurrency(Pouch|LockdDisk)
POUCH_GO_CONCURRENCY_BENCHTIME ?= 1x
POUCH_GO_CONCURRENCY_WRITERS ?= 2
POUCH_GO_CONCURRENCY_WRITES_PER_WRITER ?= 32
POUCH_GO_CONCURRENCY_PAYLOAD_BYTES ?= 256
POUCH_GO_CONCURRENCY_TIMEOUT ?= 2m
POUCH_GO_HARDENING_COMPACTION_TIMEOUT ?= 3m
POUCH_GO_HARDENING_COMPACTION_ROWS ?= 96
POUCH_GO_HARDENING_COMPACTION_UPDATES ?= 3
POUCH_GO_HARDENING_COMPACTION_PAYLOAD_BYTES ?= 32768
POUCH_GO_HARDENING_COMPACTION_SEGMENT_TARGET_BYTES ?= 65536
POUCH_GO_HARDENING_CONCURRENCY_TIMEOUT ?= 3m
POUCH_GO_HARDENING_CONCURRENCY_WRITERS ?= 4
POUCH_GO_HARDENING_CONCURRENCY_WRITES_PER_WRITER ?= 48
POUCH_GO_HARDENING_CONCURRENCY_PAYLOAD_BYTES ?= 4096
POUCH_GO_ROUTINE_TIMEOUT ?= 90s
POUCH_GO_ROUTINE_CONCURRENCY_WRITERS ?= 2
POUCH_GO_ROUTINE_CONCURRENCY_WRITES_PER_WRITER ?= 8
POUCH_GO_ROUTINE_CONCURRENCY_PAYLOAD_BYTES ?= 256
POUCH_PERF_CASE ?= pouch-perf-index-docs
POUCH_PERF_ROWS ?= 128
POUCH_PERF_PAYLOAD_BYTES ?= 4096
POUCH_PERF_CRYPTO ?= 0
POUCH_PERF_TIMEOUT ?= 60s
POUCH_PERF_ROUTINE_ROWS ?= 12
POUCH_PERF_ROUTINE_PAYLOAD_BYTES ?= 131072
WORKFLOW_BENCH_ROWS ?= 256
WORKFLOW_BENCH_TERMINAL_ROWS ?= 1024
WORKFLOW_BENCH_CHURN_UPDATES ?= 4
WORKFLOW_BENCH_PAYLOAD_BYTES ?= 4096
WORKFLOW_BENCH_PAGE_CAPACITY ?= 16
WORKFLOW_BENCH_TIMEOUT ?= 10m
WORKFLOW_BENCH_REMOTE_ENDPOINT ?= https://localhost:19441
WORKFLOW_BENCH_REMOTE_FAILOVER_ENDPOINT ?= https://localhost:19442
WORKFLOW_BENCH_REMOTE_BUNDLE ?= $(ROOT)/devenv/volumes/lockd-disk-a-config/client.pem
FUZZ_TIME ?= 30
FUZZ_LONG_TIME ?= 300
POUCH_GO_BENCH_CFLAGS := \
	-I$(ROOT)/include \
	-I$(ROOT)/src \
	-I$(X86_64_GNU_RELEASE_BUILD_DIR)/generated/include \
	-I$(ROOT)/.cache/deps/x86_64-linux-gnu/lonejson/install/include \
	-I$(ROOT)/.cache/deps/x86_64-linux-gnu/liblql/install/include \
	-I$(ROOT)/.cache/deps/x86_64-linux-gnu/pslog/install/include \
	-I$(ROOT)/.cache/deps/x86_64-linux-gnu/curl/install/include \
	-I$(ROOT)/.cache/deps/x86_64-linux-gnu/libssh2/install/include \
	-I$(ROOT)/.cache/deps/x86_64-linux-gnu/zlib/install/include \
	-I$(ROOT)/.cache/deps/x86_64-linux-gnu/openssl/install/include \
	-I$(ROOT)/.cache/deps/x86_64-linux-gnu/nghttp2/install/include
POUCH_GO_BENCH_LDFLAGS := \
	$(X86_64_GNU_RELEASE_BUILD_DIR)/liblockdc.a \
	$(ROOT)/.cache/deps/x86_64-linux-gnu/pslog/install/lib/libpslog.a \
	$(ROOT)/.cache/deps/x86_64-linux-gnu/lonejson/install/lib/liblonejson.a \
	$(ROOT)/.cache/deps/x86_64-linux-gnu/liblql/install/lib/liblql.a \
	$(ROOT)/.cache/deps/x86_64-linux-gnu/curl/install/lib/libcurl.a \
	$(ROOT)/.cache/deps/x86_64-linux-gnu/libssh2/install/lib/libssh2.a \
	$(ROOT)/.cache/deps/x86_64-linux-gnu/zlib/install/lib/libz.a \
	$(ROOT)/.cache/deps/x86_64-linux-gnu/openssl/install/lib/libssl.a \
	$(ROOT)/.cache/deps/x86_64-linux-gnu/openssl/install/lib/libcrypto.a \
	$(ROOT)/.cache/deps/x86_64-linux-gnu/nghttp2/install/lib/libnghttp2.a \
	-pthread -ldl -latomic
LOCKD_GO_CACHE_ENV := GOMODCACHE=$(ROOT)/.cache/go/pkg/mod GOCACHE=$(ROOT)/.cache/go/build GOBIN=$(ROOT)/.cache/go/bin
LOCKD_GO_MODULE_DIR := $(ROOT)/.cache/go/pkg/mod/pkt.systems/lockd@$(LOCKD_GO_VERSION)

.PHONY: \
	help \
	__deps-debug __deps-release __deps-cross \
	__build-debug __build-host __build-x86_64-linux-gnu-release __build-release __build-e2e __build-coverage __build-fuzz \
	__test-debug __test-pouch-workflow-preflight __test-host __test-cross __test-e2e __test-install-tree __example-smoke-local __test-all __test-coverage \
	__format \
	__finalize-slice __valgrind __coverage __fuzz __fuzz-smoke __fuzz-long __bench __benchmarks __bench-check __bench-gate __benchmarks-go __perf-gate __benchmark-pouch-perf-prepare __benchmark-pouch-perf __benchmark-workflow-prepare __benchmark-workflow-pouch __benchmark-workflow-remote __benchmark-pouch-routine __benchmark-pouch-go-prepare __benchmark-pouch-go __benchmark-pouch-go-run __benchmark-pouch-go-fast __benchmark-pouch-go-medium __benchmark-pouch-go-acceptance __benchmark-pouch-go-production __benchmark-pouch-go-durable __benchmark-pouch-go-compaction __benchmark-pouch-go-concurrency __benchmark-pouch-go-parity-gate __benchmark-pouch-go-durable-gate __benchmark-pouch-go-core-soak __pouch-core-hardening \
	__package __package-source __package-source-smoke __package-checksums __package-verify __verify-release-privacy __clean-dist \
	__lua-rock __lua-test __lua-env __release-lua-artifacts \
	__dev-up __dev-down __dev-reset __dev-ps __dev-logs __cross-build __cross-preset-test __cross-test \
	__prerelease __prerelease-ordinary __prerelease-live __prerelease-hardening __lifecycle-version-contract __release __release-pipeline __release-matrix __clean \
	deps-debug deps-release deps-cross \
	build build-debug build-host build-release build-e2e build-coverage build-fuzz \
	test test-debug test-pouch-workflow-preflight test-host test-cross test-e2e test-install-tree example-smoke-local test-all test-coverage \
	format \
	finalize-slice valgrind coverage fuzz fuzz-smoke fuzz-long bench benchmarks bench-check bench-gate benchmarks-go perf-gate benchmark-pouch-perf benchmark-workflow-pouch benchmark-workflow-remote benchmark-pouch-routine benchmark-pouch-perf-index-docs benchmark-pouch-perf-full-text-keys benchmark-pouch-perf-full-text-reopen-keys benchmark-pouch-perf-scan-keys benchmark-pouch-perf-flush-intermediate benchmark-pouch-perf-flush-reopen benchmark-pouch-go benchmark-pouch-go-fast benchmark-pouch-go-medium benchmark-pouch-go-acceptance benchmark-pouch-go-production benchmark-pouch-go-durable benchmark-pouch-go-compaction benchmark-pouch-go-concurrency benchmark-pouch-go-parity-gate benchmark-pouch-go-durable-gate benchmark-pouch-go-core-soak \
	package package-source package-source-smoke package-checksums package-verify verify-release-archives verify-release-privacy clean-dist \
	lua-rock lua-test lua-env release-lua-artifacts \
	dev-up dev-down dev-reset dev-ps dev-logs cross-build cross-preset-test cross-test \
	prerelease prerelease-live prerelease-hardening lifecycle-version-contract print-release-version release release-matrix clean

help:
	@printf '%s\n' \
		'make build              Configure and build the ASan/UBSan debug preset.' \
		'make build-debug        Configure and build the ASan/UBSan debug preset.' \
		'make build-host         Configure and build the pinned Bootlin host-executable GNU and musl release presets.' \
		'make build-release      Configure and build the full shipped Linux release matrix.' \
		'make build-e2e          Configure and build the e2e preset.' \
		'make build-coverage     Configure and build the coverage preset.' \
		'make build-fuzz         Configure and build the fuzz preset.' \
		'make deps-debug         Provision the pinned x86_64 GNU Bootlin dependency tree used by debug/e2e/coverage/fuzz.' \
		'make deps-release       Provision the shipped x86_64 GNU/musl release dependency trees.' \
		'make deps-cross         Provision all non-host cross release dependency trees.' \
		'make test-debug         Run the ASan/UBSan debug preset test suite.' \
		'make test-pouch-workflow-preflight Run fast clean-restart and shared-dispatcher Pouch regressions.' \
		'make test               Run the pinned Bootlin host-executable GNU and musl release suites.' \
		'make test-host          Run the pinned Bootlin host-executable GNU and musl release suites.' \
		'make test-cross         Run the non-host cross release suites.' \
		'make test-e2e           Run the mTLS/libcurl e2e preset against the local devenv.' \
		'make test-install-tree  Validate CMake and pkg-config consumers against the installed native SDK.' \
		'make example-smoke-local Run local-service example smoke tests.' \
		'make test-all           Run debug, host and QEMU cross tests, Valgrind, fuzz smoke, local e2e, and Pouch-vs-disk performance gates.' \
		'make test-coverage      Run the coverage preset test suite and build the coverage report.' \
		'make dev-up             Start the local compose-backed devenv and wait for generated client bundles.' \
		'make dev-down           Stop and remove the local compose-backed devenv.' \
		'make dev-reset          Stop the local compose-backed devenv and remove its generated state.' \
		'make dev-ps             Show the local compose-backed devenv service state.' \
		'make dev-logs           Show local compose-backed devenv logs.' \
		'make format             Run clang-format over repo .c and .h files.' \
		'make finalize-slice     Run formatting plus the narrow debug test gate for an ordinary implementation slice.' \
		'make valgrind           Build the valgrind preset and run the native Valgrind Memcheck subset.' \
		'make coverage           Run the coverage preset and generate coverage-report.' \
		'make fuzz               Build fuzz targets and run bounded corpus passes.' \
		'make fuzz-smoke         Build fuzz targets and run short bounded corpus passes (FUZZ_TIME=5).' \
		'make fuzz-long          Build fuzz targets and run longer bounded corpus passes (FUZZ_LONG_TIME=$(FUZZ_LONG_TIME)).' \
		'make bench              Standard short name for benchmarks.' \
		'make benchmarks         Build the shipped x86_64-linux-gnu release preset and run the local benchmark matrix (BENCH_ITERS=$(BENCH_ITERS)).' \
		'make bench-check        Run native benchmarks and enforce Pouch-vs-disk parity.' \
		'make bench-gate         Run native benchmarks and enforce Pouch-vs-disk parity.' \
		'make benchmarks-go      Run Go parity benchmarks through the standard lifecycle name.' \
		'make perf-gate          Enforce Pouch-vs-disk core-operation parity for default and strict-durable I/O.' \
		'make benchmark-pouch-perf Prepare native artifacts, then run one sub-minute pouch perf case (POUCH_PERF_CASE=$(POUCH_PERF_CASE), POUCH_PERF_ROWS=$(POUCH_PERF_ROWS), POUCH_PERF_CRYPTO=$(POUCH_PERF_CRYPTO)).' \
		'make benchmark-workflow-pouch Run the large-outbox indexed-reconciliation benchmark against a fresh, un-compacted Pouch root.' \
		'make benchmark-workflow-remote Start the compose devenv, then run the same workflow reconciliation workload through the disk lockd endpoint.' \
		'make benchmark-pouch-routine Prepare benchmark artifacts, then run all bounded native phase probes plus production and shared-root concurrency comparison in at most $(POUCH_GO_ROUTINE_TIMEOUT).' \
		'make benchmark-pouch-perf-index-docs Run the isolated public-API indexed narrative document query perf case.' \
		'make benchmark-pouch-perf-full-text-keys Run the isolated public-API full-text key query perf case.' \
		'make benchmark-pouch-perf-full-text-reopen-keys Run the isolated public-API reopened full-text key query perf case.' \
		'make benchmark-pouch-perf-scan-keys Run the isolated public-API scan key query perf case.' \
		'make benchmark-pouch-perf-flush-intermediate Run the isolated public-API changed-row index flush perf case.' \
		'make benchmark-pouch-perf-flush-reopen Run the isolated public-API reopen index flush perf case.' \
		'make benchmark-pouch-go Run opt-in Go e2e lockd-disk vs pouch perf/stress benchmarks outside release gates (POUCH_GO_BENCH=$(POUCH_GO_BENCH), POUCH_GO_BENCHTIME=$(POUCH_GO_BENCHTIME), POUCH_GO_SEED_ROWS=$(POUCH_GO_SEED_ROWS)).' \
		'make benchmark-pouch-go-fast Prepare release and Go artifacts, then run the bounded pouch-vs-disk iteration suite (measurement timeout $(POUCH_GO_FAST_TIMEOUT), seed rows $(POUCH_GO_FAST_SEED_ROWS)).' \
		'make benchmark-pouch-go-medium Prepare release and Go artifacts, then run the bounded 3m pouch-vs-disk scan/index scale suite (rows $(POUCH_GO_MEDIUM_SCALE_ROWS)).' \
		'make benchmark-pouch-go-acceptance Prepare release and Go artifacts, then run the bounded 4096-doc pouch-vs-disk acceptance matrix (measurement timeout $(POUCH_GO_ACCEPTANCE_TIMEOUT)).' \
		'make benchmark-pouch-go-production Run production-like pouch-vs-disk segmented write/read/replay/queue/attachment benchmark matrix; set POUCH_GO_PRODUCTION_ROWS/UPDATES/PAYLOAD_BYTES/SEGMENT_TARGET_BYTES for one custom profile.' \
		'make benchmark-pouch-go-durable Run strict-durability Pouch fdatasync versus Go disk auto-HA production metrics.' \
		'make benchmark-pouch-go-production-bounded Run the bounded full production matrix with a shared small segment target.' \
		'make benchmark-pouch-go-compaction Run opt-in pouch forced/scheduled compaction benchmarks; set POUCH_GO_COMPACTION_* to tune early compaction.' \
		'make benchmark-pouch-go-concurrency Run bounded pouch-vs-disk key-lock contention and shared-root concurrency matrix with crypto off/on.' \
		'make benchmark-pouch-go-parity-gate Run production pouch-vs-disk benchmarks and require at least $(POUCH_GO_PARITY_MIN_SPEEDUP)x Pouch speedup on each comparable core metric.' \
		'make benchmark-pouch-go-durable-gate Run strict-durability production metrics and require the same Pouch speedup policy.' \
		'make benchmark-pouch-go-core-soak Run the bounded, long-duration Pouch core-operation churn soak used by prerelease hardening.' \
		'make package            Build a clean native Bootlin release package with source, Lua, and checksums under dist/.' \
		'make package-source     Build the source-only release archive.' \
		'make package-source-smoke  Build and verify the source-only release archive.' \
		'make package-checksums  Refresh the dist/ checksum manifest.' \
		'make package-verify     Run the full release matrix, checksum, source/SDK/Lua, and recursive privacy verification.' \
		'make verify-release-archives  Assert the complete shipped Linux release archive set and checksums.' \
		'make verify-release-privacy  Scan checksum-listed release artifacts for local private traces.' \
		'make lua-rock           Build the Lua release package and source rock artifacts.' \
		'make lua-test           Run local Lua layout, SDK, facade, and binding smoke tests.' \
		'make lua-env            Print shell exports for the repo-local Lua 5.5 rock tree.' \
		'make release-lua-artifacts  Build Lua release artifacts under dist/.' \
		'make clean-dist         Reset dist/ release artifacts.' \
		'make cross-build        Build all non-host cross release presets.' \
		'make cross-preset-test  Run the host ASan/UBSan debug cross-preset packaging-isolation check.' \
		'make cross-test         Run the host cross-preset isolation check plus all non-host cross release preset tests against existing build trees.' \
		'make prerelease         Run deterministic pre-release confidence without an initial clean.' \
		'make prerelease-live    Refuse without LOCKDC_PRERELEASE_LIVE=1; no live-provider checks are currently defined.' \
		'make prerelease-hardening  Run prerelease plus full fuzzing and the release matrix.' \
		'make lifecycle-version-contract  Verify exact release tag semantics before clean release work.' \
		'make print-release-version  Print the release version resolved by the Make-owned release surface.' \
		'make release            Run the clean-slate final release workflow: version contract, clean, then the shared release proof graph.' \
		'make release-matrix     Rebuild, test, package, and verify the release matrix while reusing existing build and dependency caches.' \
		'make clean              Remove generated build, cache, dist, and devenv state.'

build: build-debug

deps-debug:
	$(TIMED) deps-debug $(MAKE_RECURSE) __deps-debug

__deps-debug:
	bash ./scripts/deps.sh deps-x86_64-linux-gnu

deps-release:
	$(TIMED) deps-release $(MAKE_RECURSE) __deps-release

__deps-release:
	bash ./scripts/deps.sh deps-x86_64-linux-gnu
	bash ./scripts/deps.sh deps-x86_64-linux-musl

deps-cross:
	$(TIMED) deps-cross $(MAKE_RECURSE) __deps-cross

__deps-cross:
	bash ./scripts/deps.sh deps-aarch64-linux-gnu
	bash ./scripts/deps.sh deps-aarch64-linux-musl
	bash ./scripts/deps.sh deps-armhf-linux-gnu
	bash ./scripts/deps.sh deps-armhf-linux-musl
	if bash ./scripts/osxcross_available.sh; then bash ./scripts/deps.sh deps-arm64-apple-darwin; else printf '[deps] skipping deps-arm64-apple-darwin: osxcross toolchain not available\n'; fi

build-debug:
	$(TIMED) build-debug $(MAKE_RECURSE) __build-debug

__build-debug: __deps-debug
	$(CMAKE) --preset $(DEBUG_PRESET)
	$(CMAKE) --build --preset $(DEBUG_PRESET)

build-host:
	$(TIMED) build-host $(MAKE_RECURSE) __build-host

__build-host:
	bash ./scripts/host_test.sh build

build-release:
	$(TIMED) build-release $(MAKE_RECURSE) __build-release

__build-x86_64-linux-gnu-release: __deps-release
	$(CMAKE) --preset $(X86_64_GNU_RELEASE_PRESET)
	$(CMAKE) --build --preset $(X86_64_GNU_RELEASE_PRESET)

__build-release: __deps-release __deps-cross
	bash ./scripts/run_linux_build_matrix.sh

build-e2e:
	$(TIMED) build-e2e $(MAKE_RECURSE) __build-e2e

__build-e2e: __deps-debug
	$(CMAKE) --preset $(E2E_PRESET)
	$(CMAKE) --build --preset $(E2E_PRESET)

build-coverage:
	$(TIMED) build-coverage $(MAKE_RECURSE) __build-coverage

__build-coverage: __deps-debug
	$(CMAKE) --preset $(COVERAGE_PRESET)
	$(CMAKE) --build --preset $(COVERAGE_PRESET)

build-fuzz:
	$(TIMED) build-fuzz $(MAKE_RECURSE) __build-fuzz

__build-fuzz: __deps-debug
	$(CMAKE) --preset $(FUZZ_PRESET)
	$(CMAKE) --build --preset $(FUZZ_PRESET)

test: test-host

test-debug:
	$(TIMED) test-debug $(MAKE_RECURSE) __test-debug

__test-debug: __build-debug
	$(CTEST) --preset $(DEBUG_PRESET)

test-pouch-workflow-preflight:
	$(TIMED) test-pouch-workflow-preflight $(MAKE_RECURSE) __test-pouch-workflow-preflight

__test-pouch-workflow-preflight: __build-debug
	CMOCKA_TEST_FILTER=test_pouch_multikey_terminal_failure_publishes_nothing $(DEBUG_BUILD_DIR)/tests/unit/lc_unit_workflow
	CMOCKA_TEST_FILTER=test_pouch_reconciliation_retains_overflow_request $(DEBUG_BUILD_DIR)/tests/unit/lc_unit_workflow
	CMOCKA_TEST_FILTER=test_pouch_clean_reopen_reconciles_durable_index $(DEBUG_BUILD_DIR)/tests/unit/lc_unit_workflow
	CMOCKA_TEST_FILTER=test_pouch_shared_reopen_reconciles_durable_index $(DEBUG_BUILD_DIR)/tests/unit/lc_unit_workflow
	CMOCKA_TEST_FILTER=test_pouch_shared_process_reconciles_each_outbox_once $(DEBUG_BUILD_DIR)/tests/unit/lc_unit_workflow

test-host:
	$(TIMED) test-host $(MAKE_RECURSE) __test-host

__test-host:
	bash ./scripts/host_test.sh

test-cross:
	$(TIMED) test-cross $(MAKE_RECURSE) __test-cross

__test-cross: __cross-build
	bash ./scripts/cross_test.sh release

test-e2e:
	$(TIMED) test-e2e $(MAKE_RECURSE) __test-e2e

__test-e2e:
	bash ./scripts/test-e2e.sh

test-install-tree:
	$(TIMED) test-install-tree $(MAKE_RECURSE) __test-install-tree

__test-install-tree: __build-x86_64-linux-gnu-release
	$(CTEST) --preset $(X86_64_GNU_RELEASE_PRESET) --output-on-failure \
		--progress --stop-on-failure -R '^install_tree_sdk_test$$'

example-smoke-local:
	$(TIMED) example-smoke-local $(MAKE_RECURSE) __example-smoke-local

__example-smoke-local:
	bash ./scripts/test-e2e.sh examples

test-all:
	$(TIMED) test-all $(MAKE_RECURSE) __test-all

__test-all: __test-pouch-workflow-preflight __test-debug __test-host __test-cross __valgrind __fuzz-smoke __test-e2e __bench-gate

dev-up:
	$(TIMED) dev-up $(MAKE_RECURSE) __dev-up

__dev-up:
	bash ./scripts/dev-up.sh

dev-down:
	$(TIMED) dev-down $(MAKE_RECURSE) __dev-down

__dev-down:
	bash ./scripts/dev-down.sh

dev-reset:
	$(TIMED) dev-reset $(MAKE_RECURSE) __dev-reset

__dev-reset:
	bash ./scripts/dev-reset.sh

dev-ps:
	$(TIMED) dev-ps $(MAKE_RECURSE) __dev-ps

__dev-ps:
	bash ./scripts/dev-ps.sh

dev-logs:
	$(TIMED) dev-logs $(MAKE_RECURSE) __dev-logs

__dev-logs:
	bash ./scripts/dev-logs.sh

format:
	$(TIMED) format $(MAKE_RECURSE) __format

__format:
	rg --files -g '*.c' -g '*.h' | xargs $(CLANG_FORMAT) -i

finalize-slice:
	$(TIMED) finalize-slice $(MAKE_RECURSE) __finalize-slice

__finalize-slice:
	$(TIMED) 'finalize-slice format' $(MAKE_RECURSE) __format
	$(TIMED) 'finalize-slice test-debug' $(MAKE_RECURSE) __test-debug

valgrind:
	$(TIMED) valgrind $(MAKE_RECURSE) __valgrind

__valgrind:
	bash ./scripts/valgrind.sh

test-coverage:
	$(TIMED) test-coverage $(MAKE_RECURSE) __test-coverage

__test-coverage: __build-coverage
	$(CTEST) --preset $(COVERAGE_PRESET)
	$(CMAKE) --build --preset coverage-report

coverage:
	$(TIMED) coverage $(MAKE_RECURSE) __coverage

__coverage: __test-coverage

fuzz:
	$(TIMED) fuzz $(MAKE_RECURSE) __fuzz

__fuzz:
	bash ./scripts/fuzz.sh $(FUZZ_TIME)

fuzz-smoke:
	$(TIMED) fuzz-smoke $(MAKE_RECURSE) __fuzz-smoke

__fuzz-smoke:
	bash ./scripts/fuzz.sh 5

fuzz-long:
	$(TIMED) fuzz-long $(MAKE_RECURSE) __fuzz-long

__fuzz-long:
	bash ./scripts/fuzz.sh $(FUZZ_LONG_TIME)

bench:
	$(TIMED) bench $(MAKE_RECURSE) __bench

__bench: __benchmarks

benchmarks:
	$(TIMED) benchmarks $(MAKE_RECURSE) __benchmarks

__benchmarks: __build-x86_64-linux-gnu-release
	./build/$(X86_64_GNU_RELEASE_PRESET)/bench/lockdc_bench $(BENCH_ITERS) all

bench-gate:
	$(TIMED) bench-gate $(MAKE_RECURSE) __bench-gate

__bench-gate:
	$(TIMED) 'bench-gate benchmarks' $(MAKE_RECURSE) __benchmarks
	$(TIMED) 'bench-gate perf-gate' $(MAKE_RECURSE) __perf-gate

bench-check:
	$(TIMED) bench-check $(MAKE_RECURSE) __bench-check

__bench-check: __bench-gate

benchmarks-go:
	$(TIMED) benchmarks-go $(MAKE_RECURSE) __benchmarks-go

__benchmarks-go: __benchmark-pouch-go

perf-gate:
	$(TIMED) perf-gate $(MAKE_RECURSE) __perf-gate

__perf-gate:
	$(TIMED) 'perf-gate benchmark-prepare' $(MAKE_RECURSE) __benchmark-pouch-go-prepare
	$(TIMED) 'perf-gate pouch-go-parity' $(MAKE_RECURSE) __benchmark-pouch-go-parity-gate
	$(TIMED) 'perf-gate pouch-go-durable-parity' $(MAKE_RECURSE) __benchmark-pouch-go-durable-gate

benchmark-pouch-perf: __benchmark-pouch-perf-prepare
	$(TIMED) benchmark-pouch-perf timeout --kill-after=5s \
	  '$(POUCH_PERF_TIMEOUT)' $(MAKE_RECURSE) __benchmark-pouch-perf

__benchmark-pouch-perf-prepare:
	$(CMAKE) --preset $(X86_64_GNU_RELEASE_PRESET)
	$(CMAKE) --build --preset $(X86_64_GNU_RELEASE_PRESET) --target lockdc_bench

__benchmark-pouch-perf:
	LOCKDC_POUCH_PERF_PAYLOAD_BYTES='$(POUCH_PERF_PAYLOAD_BYTES)' \
	  LOCKDC_POUCH_PERF_CRYPTO='$(POUCH_PERF_CRYPTO)' \
	  ./build/$(X86_64_GNU_RELEASE_PRESET)/bench/lockdc_bench \
	    $(POUCH_PERF_ROWS) $(POUCH_PERF_CASE)

benchmark-workflow-pouch: __benchmark-workflow-prepare
	$(TIMED) benchmark-workflow-pouch timeout --kill-after=5s \
	  '$(WORKFLOW_BENCH_TIMEOUT)' $(MAKE_RECURSE) __benchmark-workflow-pouch

__benchmark-workflow-prepare:
	$(CMAKE) --preset $(X86_64_GNU_RELEASE_PRESET)
	$(CMAKE) --build --preset $(X86_64_GNU_RELEASE_PRESET) --target lockdc_bench

__benchmark-workflow-pouch:
	LOCKDC_WORKFLOW_BENCH_TERMINAL_ROWS='$(WORKFLOW_BENCH_TERMINAL_ROWS)' \
	  LOCKDC_WORKFLOW_BENCH_CHURN_UPDATES='$(WORKFLOW_BENCH_CHURN_UPDATES)' \
	  LOCKDC_WORKFLOW_BENCH_PAYLOAD_BYTES='$(WORKFLOW_BENCH_PAYLOAD_BYTES)' \
	  LOCKDC_WORKFLOW_BENCH_PAGE_CAPACITY='$(WORKFLOW_BENCH_PAGE_CAPACITY)' \
	  ./build/$(X86_64_GNU_RELEASE_PRESET)/bench/lockdc_bench \
	    $(WORKFLOW_BENCH_ROWS) workflow-reconcile

benchmark-workflow-remote: __benchmark-workflow-prepare
	$(MAKE_RECURSE) __dev-reset
	$(MAKE_RECURSE) __dev-up
	$(TIMED) benchmark-workflow-remote timeout --kill-after=5s \
	  '$(WORKFLOW_BENCH_TIMEOUT)' $(MAKE_RECURSE) __benchmark-workflow-remote

__benchmark-workflow-remote:
	LOCKDC_WORKFLOW_BENCH_ENDPOINT='$(WORKFLOW_BENCH_REMOTE_ENDPOINT)' \
	  LOCKDC_WORKFLOW_BENCH_FAILOVER_ENDPOINT='$(WORKFLOW_BENCH_REMOTE_FAILOVER_ENDPOINT)' \
	  LOCKDC_WORKFLOW_BENCH_CLIENT_BUNDLE='$(WORKFLOW_BENCH_REMOTE_BUNDLE)' \
	  LOCKDC_WORKFLOW_BENCH_TERMINAL_ROWS='$(WORKFLOW_BENCH_TERMINAL_ROWS)' \
	  LOCKDC_WORKFLOW_BENCH_CHURN_UPDATES='$(WORKFLOW_BENCH_CHURN_UPDATES)' \
	  LOCKDC_WORKFLOW_BENCH_PAYLOAD_BYTES='$(WORKFLOW_BENCH_PAYLOAD_BYTES)' \
	  LOCKDC_WORKFLOW_BENCH_PAGE_CAPACITY='$(WORKFLOW_BENCH_PAGE_CAPACITY)' \
	  ./build/$(X86_64_GNU_RELEASE_PRESET)/bench/lockdc_bench \
	    $(WORKFLOW_BENCH_ROWS) workflow-reconcile

benchmark-pouch-perf-index-docs:
	$(MAKE_RECURSE) benchmark-pouch-perf POUCH_PERF_CASE=pouch-perf-index-docs

benchmark-pouch-perf-full-text-keys:
	$(MAKE_RECURSE) benchmark-pouch-perf POUCH_PERF_CASE=pouch-perf-full-text-keys

benchmark-pouch-perf-full-text-reopen-keys:
	$(MAKE_RECURSE) benchmark-pouch-perf POUCH_PERF_CASE=pouch-perf-full-text-reopen-keys

benchmark-pouch-perf-scan-keys:
	$(MAKE_RECURSE) benchmark-pouch-perf POUCH_PERF_CASE=pouch-perf-scan-keys

benchmark-pouch-perf-flush-intermediate:
	$(MAKE_RECURSE) benchmark-pouch-perf POUCH_PERF_CASE=pouch-perf-flush-intermediate

benchmark-pouch-perf-flush-reopen:
	$(MAKE_RECURSE) benchmark-pouch-perf POUCH_PERF_CASE=pouch-perf-flush-reopen

benchmark-pouch-routine: __benchmark-pouch-perf-prepare __benchmark-pouch-go-prepare
	$(TIMED) benchmark-pouch-routine timeout --kill-after=5s \
	  '$(POUCH_GO_ROUTINE_TIMEOUT)' $(MAKE_RECURSE) __benchmark-pouch-routine

__benchmark-pouch-routine:
	$(MAKE_RECURSE) __benchmark-pouch-perf POUCH_PERF_CASE=pouch-perf-index-docs POUCH_PERF_ROWS='$(POUCH_PERF_ROUTINE_ROWS)' POUCH_PERF_PAYLOAD_BYTES='$(POUCH_PERF_ROUTINE_PAYLOAD_BYTES)'
	$(MAKE_RECURSE) __benchmark-pouch-perf POUCH_PERF_CASE=pouch-perf-full-text-keys POUCH_PERF_ROWS='$(POUCH_PERF_ROUTINE_ROWS)' POUCH_PERF_PAYLOAD_BYTES='$(POUCH_PERF_ROUTINE_PAYLOAD_BYTES)'
	$(MAKE_RECURSE) __benchmark-pouch-perf POUCH_PERF_CASE=pouch-perf-full-text-reopen-keys POUCH_PERF_ROWS='$(POUCH_PERF_ROUTINE_ROWS)' POUCH_PERF_PAYLOAD_BYTES='$(POUCH_PERF_ROUTINE_PAYLOAD_BYTES)'
	$(MAKE_RECURSE) __benchmark-pouch-perf POUCH_PERF_CASE=pouch-perf-scan-keys POUCH_PERF_ROWS='$(POUCH_PERF_ROUTINE_ROWS)' POUCH_PERF_PAYLOAD_BYTES='$(POUCH_PERF_ROUTINE_PAYLOAD_BYTES)'
	$(MAKE_RECURSE) __benchmark-pouch-perf POUCH_PERF_CASE=pouch-perf-flush-intermediate POUCH_PERF_ROWS='$(POUCH_PERF_ROUTINE_ROWS)' POUCH_PERF_PAYLOAD_BYTES='$(POUCH_PERF_ROUTINE_PAYLOAD_BYTES)'
	$(MAKE_RECURSE) __benchmark-pouch-perf POUCH_PERF_CASE=pouch-perf-flush-reopen POUCH_PERF_ROWS='$(POUCH_PERF_ROUTINE_ROWS)' POUCH_PERF_PAYLOAD_BYTES='$(POUCH_PERF_ROUTINE_PAYLOAD_BYTES)'
	$(MAKE_RECURSE) __benchmark-pouch-go-production-bounded POUCH_GO_BOUNDED_PRODUCTION_TIMEOUT='$(POUCH_GO_ROUTINE_TIMEOUT)'
	$(MAKE_RECURSE) __benchmark-pouch-go-concurrency POUCH_GO_CONCURRENCY_TIMEOUT='$(POUCH_GO_ROUTINE_TIMEOUT)' POUCH_GO_CONCURRENCY_WRITERS='$(POUCH_GO_ROUTINE_CONCURRENCY_WRITERS)' POUCH_GO_CONCURRENCY_WRITES_PER_WRITER='$(POUCH_GO_ROUTINE_CONCURRENCY_WRITES_PER_WRITER)' POUCH_GO_CONCURRENCY_PAYLOAD_BYTES='$(POUCH_GO_ROUTINE_CONCURRENCY_PAYLOAD_BYTES)'

__benchmark-pouch-go-prepare: __build-x86_64-linux-gnu-release
	mkdir -p $(ROOT)/.cache/go/pkg/mod $(ROOT)/.cache/go/build $(ROOT)/.cache/go/bin
	cd benchmark && $(LOCKD_GO_CACHE_ENV) $(GO) mod download
	cd $(LOCKD_GO_MODULE_DIR) && $(LOCKD_GO_CACHE_ENV) $(GO) build -o $(ROOT)/.cache/go/bin/lockd ./cmd/lockd

benchmark-pouch-go: __benchmark-pouch-go-prepare
	$(TIMED) benchmark-pouch-go $(MAKE_RECURSE) __benchmark-pouch-go-run

__benchmark-pouch-go: __benchmark-pouch-go-prepare
	$(MAKE_RECURSE) __benchmark-pouch-go-run

__benchmark-pouch-go-run:
	cd benchmark && \
	  set -- $$(cksum "$(X86_64_GNU_RELEASE_BUILD_DIR)/liblockdc.a"); \
	  $(LOCKD_GO_CACHE_ENV) \
	  LOCKDC_BENCH_LOCKD_BIN="$(ROOT)/.cache/go/bin/lockd" \
	  LOCKDC_BENCH_SEED_ROWS="$(POUCH_GO_SEED_ROWS)" \
	  LOCKDC_BENCH_SCALE_ROWS="$(POUCH_GO_MEDIUM_SCALE_ROWS)" \
	  LOCKDC_BENCH_SCALE_SCENARIOS="$(POUCH_GO_MEDIUM_SCALE_SCENARIOS)" \
	  LOCKDC_BENCH_PRODUCTION_ROWS="$(LOCKDC_BENCH_PRODUCTION_ROWS)" \
	  LOCKDC_BENCH_PRODUCTION_UPDATES="$(LOCKDC_BENCH_PRODUCTION_UPDATES)" \
	  LOCKDC_BENCH_PRODUCTION_PAYLOAD_BYTES="$(LOCKDC_BENCH_PRODUCTION_PAYLOAD_BYTES)" \
	  LOCKDC_BENCH_PRODUCTION_SEGMENT_TARGET_BYTES="$(LOCKDC_BENCH_PRODUCTION_SEGMENT_TARGET_BYTES)" \
	  LOCKDC_BENCH_COMPACTION_ROWS="$(LOCKDC_BENCH_COMPACTION_ROWS)" \
	  LOCKDC_BENCH_COMPACTION_UPDATES="$(LOCKDC_BENCH_COMPACTION_UPDATES)" \
	  LOCKDC_BENCH_COMPACTION_PAYLOAD_BYTES="$(LOCKDC_BENCH_COMPACTION_PAYLOAD_BYTES)" \
	  LOCKDC_BENCH_COMPACTION_SEGMENT_TARGET_BYTES="$(LOCKDC_BENCH_COMPACTION_SEGMENT_TARGET_BYTES)" \
	  LOCKDC_BENCH_COMPACTION_MIN_SEGMENTS="$(LOCKDC_BENCH_COMPACTION_MIN_SEGMENTS)" \
	  LOCKDC_BENCH_COMPACTION_MIN_RECLAIMABLE_BYTES="$(LOCKDC_BENCH_COMPACTION_MIN_RECLAIMABLE_BYTES)" \
	  LOCKDC_BENCH_CONCURRENCY_WRITERS="$(LOCKDC_BENCH_CONCURRENCY_WRITERS)" \
	  LOCKDC_BENCH_CONCURRENCY_WRITES_PER_WRITER="$(LOCKDC_BENCH_CONCURRENCY_WRITES_PER_WRITER)" \
	  LOCKDC_BENCH_CONCURRENCY_PAYLOAD_BYTES="$(LOCKDC_BENCH_CONCURRENCY_PAYLOAD_BYTES)" \
	  CGO_CFLAGS="$(POUCH_GO_BENCH_CFLAGS) -DLOCKDC_BENCH_ARCHIVE_BUILD_ID=$$1" \
	  CGO_LDFLAGS="$(POUCH_GO_BENCH_LDFLAGS)" \
	  timeout --kill-after=5s '$(POUCH_GO_TEST_TIMEOUT)' \
	    $(GO) test -run '$(POUCH_GO_TEST_RUN)' -bench '$(POUCH_GO_BENCH)' -benchtime '$(POUCH_GO_BENCHTIME)' -count '$(POUCH_GO_BENCH_COUNT)' -timeout '$(POUCH_GO_TEST_TIMEOUT)'

benchmark-pouch-go-fast: __benchmark-pouch-go-prepare
	$(TIMED) benchmark-pouch-go-fast timeout --kill-after=5s \
	  '$(POUCH_GO_FAST_TIMEOUT)' $(MAKE_RECURSE) __benchmark-pouch-go-fast

__benchmark-pouch-go-fast:
	$(MAKE_RECURSE) __benchmark-pouch-go-run \
	  POUCH_GO_BENCH='$(POUCH_GO_FAST_BENCH)' \
	  POUCH_GO_BENCHTIME='$(POUCH_GO_FAST_BENCHTIME)' \
	  POUCH_GO_SEED_ROWS='$(POUCH_GO_FAST_SEED_ROWS)' \
	  POUCH_GO_TEST_TIMEOUT='$(POUCH_GO_FAST_TIMEOUT)'

benchmark-pouch-go-medium: __benchmark-pouch-go-prepare
	$(TIMED) benchmark-pouch-go-medium timeout --kill-after=5s \
	  '$(POUCH_GO_MEDIUM_TIMEOUT)' $(MAKE_RECURSE) __benchmark-pouch-go-medium

__benchmark-pouch-go-medium:
	$(MAKE_RECURSE) __benchmark-pouch-go-run \
	  POUCH_GO_BENCH='$(POUCH_GO_MEDIUM_BENCH)' \
	  POUCH_GO_BENCHTIME='$(POUCH_GO_MEDIUM_BENCHTIME)' \
	  POUCH_GO_SEED_ROWS='$(POUCH_GO_MEDIUM_SEED_ROWS)' \
	  POUCH_GO_MEDIUM_SCALE_ROWS='$(POUCH_GO_MEDIUM_SCALE_ROWS)' \
	  POUCH_GO_MEDIUM_SCALE_SCENARIOS='$(POUCH_GO_MEDIUM_SCALE_SCENARIOS)' \
	  POUCH_GO_TEST_TIMEOUT='$(POUCH_GO_MEDIUM_TIMEOUT)'

benchmark-pouch-go-acceptance: __benchmark-pouch-go-prepare
	$(TIMED) benchmark-pouch-go-acceptance timeout --kill-after=5s \
	  '$(POUCH_GO_ACCEPTANCE_TIMEOUT)' $(MAKE_RECURSE) __benchmark-pouch-go-acceptance

__benchmark-pouch-go-acceptance:
	$(MAKE_RECURSE) __benchmark-pouch-go-run \
	  POUCH_GO_BENCH='$(POUCH_GO_ACCEPTANCE_BENCH)' \
	  POUCH_GO_BENCHTIME='$(POUCH_GO_ACCEPTANCE_BENCHTIME)' \
	  POUCH_GO_SEED_ROWS='$(POUCH_GO_ACCEPTANCE_SEED_ROWS)' \
	  POUCH_GO_MEDIUM_SCALE_ROWS='$(POUCH_GO_ACCEPTANCE_SCALE_ROWS)' \
	  POUCH_GO_MEDIUM_SCALE_SCENARIOS='$(POUCH_GO_ACCEPTANCE_SCALE_SCENARIOS)' \
	  POUCH_GO_TEST_TIMEOUT='$(POUCH_GO_ACCEPTANCE_TIMEOUT)'

benchmark-pouch-go-production: __benchmark-pouch-go-prepare
	$(TIMED) benchmark-pouch-go-production timeout --kill-after=5s \
	  '$(POUCH_GO_PRODUCTION_TIMEOUT)' $(MAKE_RECURSE) __benchmark-pouch-go-production

__benchmark-pouch-go-production:
	$(MAKE_RECURSE) __benchmark-pouch-go-run \
	  POUCH_GO_BENCH='$(POUCH_GO_PRODUCTION_BENCH)' \
	  POUCH_GO_BENCHTIME='$(POUCH_GO_PRODUCTION_BENCHTIME)' \
	  POUCH_GO_SEED_ROWS='$(POUCH_GO_FAST_SEED_ROWS)' \
	  POUCH_GO_TEST_TIMEOUT='$(POUCH_GO_PRODUCTION_TIMEOUT)' \
	  LOCKDC_BENCH_PRODUCTION_ROWS='$(POUCH_GO_PRODUCTION_ROWS)' \
	  LOCKDC_BENCH_PRODUCTION_UPDATES='$(POUCH_GO_PRODUCTION_UPDATES)' \
	  LOCKDC_BENCH_PRODUCTION_PAYLOAD_BYTES='$(POUCH_GO_PRODUCTION_PAYLOAD_BYTES)' \
	  LOCKDC_BENCH_PRODUCTION_SEGMENT_TARGET_BYTES='$(POUCH_GO_PRODUCTION_SEGMENT_TARGET_BYTES)'

benchmark-pouch-go-durable: __benchmark-pouch-go-prepare
	$(TIMED) benchmark-pouch-go-durable timeout --kill-after=5s \
	  '$(POUCH_GO_DURABLE_TIMEOUT)' $(MAKE_RECURSE) __benchmark-pouch-go-durable

__benchmark-pouch-go-durable:
	$(MAKE_RECURSE) __benchmark-pouch-go-run \
	  POUCH_GO_BENCH='$(POUCH_GO_DURABLE_BENCH)' \
	  POUCH_GO_BENCHTIME='$(POUCH_GO_DURABLE_BENCHTIME)' \
	  POUCH_GO_BENCH_COUNT='$(POUCH_GO_DURABLE_COUNT)' \
	  POUCH_GO_SEED_ROWS='$(POUCH_GO_FAST_SEED_ROWS)' \
	  POUCH_GO_TEST_TIMEOUT='$(POUCH_GO_DURABLE_TIMEOUT)' \
	  LOCKDC_BENCH_PRODUCTION_ROWS='$(POUCH_GO_DURABLE_ROWS)' \
	  LOCKDC_BENCH_PRODUCTION_UPDATES='$(POUCH_GO_DURABLE_UPDATES)' \
	  LOCKDC_BENCH_PRODUCTION_PAYLOAD_BYTES='$(POUCH_GO_DURABLE_PAYLOAD_BYTES)' \
	  LOCKDC_BENCH_PRODUCTION_SEGMENT_TARGET_BYTES='$(POUCH_GO_DURABLE_SEGMENT_TARGET_BYTES)'

benchmark-pouch-go-production-bounded: __benchmark-pouch-go-prepare
	$(TIMED) benchmark-pouch-go-production-bounded timeout --kill-after=5s \
	  '$(POUCH_GO_BOUNDED_PRODUCTION_TIMEOUT)' $(MAKE_RECURSE) __benchmark-pouch-go-production-bounded

__benchmark-pouch-go-production-bounded:
	$(MAKE_RECURSE) __benchmark-pouch-go-production \
	  POUCH_GO_PRODUCTION_BENCHTIME='1x' \
	  POUCH_GO_PRODUCTION_ROWS='$(POUCH_GO_BOUNDED_PRODUCTION_ROWS)' \
	  POUCH_GO_PRODUCTION_UPDATES='$(POUCH_GO_BOUNDED_PRODUCTION_UPDATES)' \
	  POUCH_GO_PRODUCTION_PAYLOAD_BYTES='$(POUCH_GO_BOUNDED_PRODUCTION_PAYLOAD_BYTES)' \
	  POUCH_GO_PRODUCTION_SEGMENT_TARGET_BYTES='$(POUCH_GO_BOUNDED_PRODUCTION_SEGMENT_TARGET_BYTES)' \
	  POUCH_GO_PRODUCTION_TIMEOUT='$(POUCH_GO_BOUNDED_PRODUCTION_TIMEOUT)'

benchmark-pouch-go-compaction: __benchmark-pouch-go-prepare
	$(TIMED) benchmark-pouch-go-compaction timeout --kill-after=5s \
	  '$(POUCH_GO_COMPACTION_TIMEOUT)' $(MAKE_RECURSE) __benchmark-pouch-go-compaction

__benchmark-pouch-go-compaction:
	$(MAKE_RECURSE) __benchmark-pouch-go-run \
	  POUCH_GO_BENCH='$(POUCH_GO_COMPACTION_BENCH)' \
	  POUCH_GO_BENCHTIME='$(POUCH_GO_COMPACTION_BENCHTIME)' \
	  POUCH_GO_SEED_ROWS='$(POUCH_GO_FAST_SEED_ROWS)' \
	  POUCH_GO_TEST_TIMEOUT='$(POUCH_GO_COMPACTION_TIMEOUT)' \
	  LOCKDC_BENCH_COMPACTION_ROWS='$(POUCH_GO_COMPACTION_ROWS)' \
	  LOCKDC_BENCH_COMPACTION_UPDATES='$(POUCH_GO_COMPACTION_UPDATES)' \
	  LOCKDC_BENCH_COMPACTION_PAYLOAD_BYTES='$(POUCH_GO_COMPACTION_PAYLOAD_BYTES)' \
	  LOCKDC_BENCH_COMPACTION_SEGMENT_TARGET_BYTES='$(POUCH_GO_COMPACTION_SEGMENT_TARGET_BYTES)' \
	  LOCKDC_BENCH_COMPACTION_MIN_SEGMENTS='$(POUCH_GO_COMPACTION_MIN_SEGMENTS)' \
	  LOCKDC_BENCH_COMPACTION_MIN_RECLAIMABLE_BYTES='$(POUCH_GO_COMPACTION_MIN_RECLAIMABLE_BYTES)'

benchmark-pouch-go-concurrency: __benchmark-pouch-go-prepare
	$(TIMED) benchmark-pouch-go-concurrency timeout --kill-after=5s \
	  '$(POUCH_GO_CONCURRENCY_TIMEOUT)' $(MAKE_RECURSE) __benchmark-pouch-go-concurrency

__benchmark-pouch-go-concurrency:
	$(MAKE_RECURSE) __benchmark-pouch-go-run \
	  POUCH_GO_BENCH='$(POUCH_GO_CONCURRENCY_BENCH)' \
	  POUCH_GO_BENCHTIME='$(POUCH_GO_CONCURRENCY_BENCHTIME)' \
	  POUCH_GO_TEST_TIMEOUT='$(POUCH_GO_CONCURRENCY_TIMEOUT)' \
	  LOCKDC_BENCH_CONCURRENCY_WRITERS='$(POUCH_GO_CONCURRENCY_WRITERS)' \
	  LOCKDC_BENCH_CONCURRENCY_WRITES_PER_WRITER='$(POUCH_GO_CONCURRENCY_WRITES_PER_WRITER)' \
	  LOCKDC_BENCH_CONCURRENCY_PAYLOAD_BYTES='$(POUCH_GO_CONCURRENCY_PAYLOAD_BYTES)'

benchmark-pouch-go-parity-gate: __benchmark-pouch-go-prepare
	$(TIMED) benchmark-pouch-go-parity-gate timeout --kill-after=5s \
	  '$(POUCH_GO_PARITY_TIMEOUT)' $(MAKE_RECURSE) __benchmark-pouch-go-parity-gate

__benchmark-pouch-go-parity-gate:
	mkdir -p $(ROOT)/build
	set -o pipefail; \
	$(MAKE_RECURSE) __benchmark-pouch-go-production \
	    POUCH_GO_PRODUCTION_BENCH='$(POUCH_GO_PARITY_BENCH)' \
	    POUCH_GO_PRODUCTION_BENCHTIME='$(POUCH_GO_PARITY_BENCHTIME)' \
	    POUCH_GO_BENCH_COUNT='$(POUCH_GO_PARITY_COUNT)' \
	    POUCH_GO_PRODUCTION_TIMEOUT='$(POUCH_GO_PARITY_TIMEOUT)' \
	    2>&1 | tee $(ROOT)/build/pouch-go-production.bench.txt
	python3 scripts/pouch_benchmark_parity.py --min-speedup $(POUCH_GO_PARITY_MIN_SPEEDUP) $(ROOT)/build/pouch-go-production.bench.txt

benchmark-pouch-go-durable-gate: __benchmark-pouch-go-prepare
	$(TIMED) benchmark-pouch-go-durable-gate timeout --kill-after=5s \
	  '$(POUCH_GO_DURABLE_GATE_TIMEOUT)' $(MAKE_RECURSE) __benchmark-pouch-go-durable-gate

__benchmark-pouch-go-durable-gate:
	mkdir -p $(ROOT)/build
	set -o pipefail; \
	  $(MAKE_RECURSE) __benchmark-pouch-go-durable \
	    POUCH_GO_DURABLE_BENCHTIME='$(POUCH_GO_PARITY_BENCHTIME)' \
	    POUCH_GO_DURABLE_COUNT='$(POUCH_GO_PARITY_COUNT)' \
	    POUCH_GO_DURABLE_TIMEOUT='$(POUCH_GO_DURABLE_GATE_TIMEOUT)' \
	  2>&1 | tee $(ROOT)/build/pouch-go-durable.bench.txt
	python3 scripts/pouch_benchmark_parity.py --mode durable --min-speedup $(POUCH_GO_PARITY_MIN_SPEEDUP) $(ROOT)/build/pouch-go-durable.bench.txt

benchmark-pouch-go-core-soak: __benchmark-pouch-go-prepare
	$(TIMED) benchmark-pouch-go-core-soak timeout --kill-after=5s \
	  '$(POUCH_GO_CORE_SOAK_TIMEOUT)' $(MAKE_RECURSE) __benchmark-pouch-go-core-soak

__benchmark-pouch-go-core-soak:
	$(MAKE_RECURSE) __benchmark-pouch-go-run \
	  POUCH_GO_BENCH='$(POUCH_GO_CORE_SOAK_BENCH)' \
	  POUCH_GO_BENCHTIME='$(POUCH_GO_CORE_SOAK_BENCHTIME)' \
	  POUCH_GO_TEST_TIMEOUT='$(POUCH_GO_CORE_SOAK_TIMEOUT)' \
	  LOCKDC_BENCH_PRODUCTION_ROWS='$(POUCH_GO_CORE_SOAK_ROWS)' \
	  LOCKDC_BENCH_PRODUCTION_UPDATES='$(POUCH_GO_CORE_SOAK_UPDATES)' \
	  LOCKDC_BENCH_PRODUCTION_PAYLOAD_BYTES='$(POUCH_GO_CORE_SOAK_PAYLOAD_BYTES)' \
	  LOCKDC_BENCH_PRODUCTION_SEGMENT_TARGET_BYTES='$(POUCH_GO_CORE_SOAK_SEGMENT_TARGET_BYTES)'

__pouch-core-hardening: __benchmark-pouch-go-prepare
	$(TIMED) 'pouch-core-hardening soak' timeout --kill-after=5s \
	  '$(POUCH_GO_CORE_SOAK_TIMEOUT)' $(MAKE_RECURSE) __benchmark-pouch-go-core-soak
	$(TIMED) 'pouch-core-hardening reclaim' timeout --kill-after=5s \
	  '$(POUCH_GO_HARDENING_COMPACTION_TIMEOUT)' $(MAKE_RECURSE) __benchmark-pouch-go-compaction \
	  POUCH_GO_COMPACTION_TIMEOUT='$(POUCH_GO_HARDENING_COMPACTION_TIMEOUT)' \
	  POUCH_GO_COMPACTION_ROWS='$(POUCH_GO_HARDENING_COMPACTION_ROWS)' \
	  POUCH_GO_COMPACTION_UPDATES='$(POUCH_GO_HARDENING_COMPACTION_UPDATES)' \
	  POUCH_GO_COMPACTION_PAYLOAD_BYTES='$(POUCH_GO_HARDENING_COMPACTION_PAYLOAD_BYTES)' \
	  POUCH_GO_COMPACTION_SEGMENT_TARGET_BYTES='$(POUCH_GO_HARDENING_COMPACTION_SEGMENT_TARGET_BYTES)'
	$(TIMED) 'pouch-core-hardening shared-root' timeout --kill-after=5s \
	  '$(POUCH_GO_HARDENING_CONCURRENCY_TIMEOUT)' $(MAKE_RECURSE) __benchmark-pouch-go-concurrency \
	  POUCH_GO_CONCURRENCY_TIMEOUT='$(POUCH_GO_HARDENING_CONCURRENCY_TIMEOUT)' \
	  POUCH_GO_CONCURRENCY_WRITERS='$(POUCH_GO_HARDENING_CONCURRENCY_WRITERS)' \
	  POUCH_GO_CONCURRENCY_WRITES_PER_WRITER='$(POUCH_GO_HARDENING_CONCURRENCY_WRITES_PER_WRITER)' \
	  POUCH_GO_CONCURRENCY_PAYLOAD_BYTES='$(POUCH_GO_HARDENING_CONCURRENCY_PAYLOAD_BYTES)'

package:
	$(TIMED) package $(MAKE_RECURSE) __package

__package: __build-x86_64-linux-gnu-release
	$(MAKE_RECURSE) __clean-dist
	$(CMAKE) -DLOCKDC_BINARY_DIR=$(X86_64_GNU_RELEASE_BUILD_DIR) -DLOCKDC_ROOT=$(ROOT) -DLOCKDC_DIST_DIR=$(DIST_DIR) -P $(ROOT)/cmake/package_archive.cmake
	$(CMAKE) -DLOCKDC_BINARY_DIR=$(X86_64_GNU_RELEASE_BUILD_DIR) -DLOCKDC_ROOT=$(ROOT) -DLOCKDC_DIST_DIR=$(DIST_DIR) -P $(ROOT)/cmake/package_source.cmake
	$(CMAKE) -DLOCKDC_BINARY_DIR=$(X86_64_GNU_RELEASE_BUILD_DIR) -DLOCKDC_ROOT=$(ROOT) -DLOCKDC_DIST_DIR=$(DIST_DIR) -P $(ROOT)/cmake/package_lua_rock.cmake
	$(CMAKE) -DLOCKDC_BINARY_DIR=$(X86_64_GNU_RELEASE_BUILD_DIR) -DLOCKDC_ROOT=$(ROOT) -DLOCKDC_DIST_DIR=$(DIST_DIR) -P $(ROOT)/cmake/package_checksums.cmake

package-source:
	$(TIMED) package-source $(MAKE_RECURSE) __package-source

__package-source: __build-x86_64-linux-gnu-release
	$(CMAKE) -DLOCKDC_BINARY_DIR=$(X86_64_GNU_RELEASE_BUILD_DIR) -DLOCKDC_ROOT=$(ROOT) -DLOCKDC_DIST_DIR=$(DIST_DIR) -P $(ROOT)/cmake/package_source.cmake

package-source-smoke:
	$(TIMED) package-source-smoke $(MAKE_RECURSE) __package-source-smoke

__package-source-smoke: __package-source
	bash ./scripts/test_release_from_source.sh $(ROOT) $$(ls -t $(DIST_DIR)/liblockdc-*.tar.gz | grep -v -- 'liblockdc-lua-' | grep -v -- '-linux-' | grep -v -- '-apple-darwin' | head -n1)

package-checksums:
	$(TIMED) package-checksums $(MAKE_RECURSE) __package-checksums

__package-checksums: __package

package-verify:
	$(TIMED) package-verify $(MAKE_RECURSE) __package-verify

__package-verify: __release-matrix __verify-release-privacy

verify-release-privacy:
	$(TIMED) verify-release-privacy $(MAKE_RECURSE) __verify-release-privacy

__verify-release-privacy:
	bash ./scripts/verify_release_privacy.sh

lua-rock:
	$(TIMED) lua-rock $(MAKE_RECURSE) __lua-rock

__lua-rock: __build-x86_64-linux-gnu-release
	$(CMAKE) -DLOCKDC_BINARY_DIR=$(X86_64_GNU_RELEASE_BUILD_DIR) -DLOCKDC_ROOT=$(ROOT) -DLOCKDC_DIST_DIR=$(DIST_DIR) -P $(ROOT)/cmake/package_lua_rock.cmake

lua-test:
	$(TIMED) lua-test $(MAKE_RECURSE) __lua-test

__lua-test: __build-debug
	$(CTEST) --preset debug-lua

lua-env:
	$(TIMED) lua-env $(MAKE_RECURSE) __lua-env

__lua-env:
	@printf 'export LOCKDC_PREFIX=%s\n' '$(X86_64_GNU_RELEASE_BUILD_DIR)/package/liblockdc-$$(sed -n '"'"'s/^set(LOCKDC_VERSION "\(.*\)")$$/\1/p'"'"' $(X86_64_GNU_RELEASE_BUILD_DIR)/package-metadata.cmake)-x86_64-linux-gnu'
	@printf 'export LUA_PATH=%s\n' '$(ROOT)/lua/?.lua;$(ROOT)/lua/?/init.lua;;'
	@printf 'export LUA_CPATH=%s\n' '$(ROOT)/.luarocks-build/lockdc/?.so;;'

release-lua-artifacts:
	$(TIMED) release-lua-artifacts $(MAKE_RECURSE) __release-lua-artifacts

__release-lua-artifacts: __lua-rock

verify-release-archives:
	release_presets='x86_64-linux-gnu-release;x86_64-linux-musl-release;aarch64-linux-gnu-release;aarch64-linux-musl-release;armhf-linux-gnu-release;armhf-linux-musl-release'; \
	if bash ./scripts/osxcross_available.sh; then release_presets="$$release_presets;arm64-apple-darwin-release"; fi; \
	$(CMAKE) -DLOCKDC_ROOT=$(ROOT) -DLOCKDC_DIST_DIR=$(DIST_DIR) -DLOCKDC_RELEASE_PRESETS="$$release_presets" -P $(ROOT)/tests/release_matrix_archives_test.cmake

clean-dist:
	$(TIMED) clean-dist $(MAKE_RECURSE) __clean-dist

__clean-dist:
	$(CMAKE) -DLOCKDC_ROOT=$(ROOT) -DLOCKDC_DIST_DIR=$(DIST_DIR) -P $(ROOT)/cmake/package_clean_dist.cmake

cross-build:
	$(TIMED) cross-build $(MAKE_RECURSE) __cross-build

__cross-build: __deps-cross
	bash ./scripts/cross_build.sh

cross-preset-test:
	$(TIMED) cross-preset-test $(MAKE_RECURSE) __cross-preset-test

__cross-preset-test:
	bash ./scripts/cross_test.sh preset

cross-test:
	$(TIMED) cross-test $(MAKE_RECURSE) __cross-test

__cross-test: __cross-build
	bash ./scripts/cross_test.sh all

release:
	$(TIMED) release $(MAKE_RECURSE) __release

prerelease:
	$(TIMED) prerelease $(MAKE_RECURSE) __prerelease

__prerelease-ordinary:
	$(TIMED) 'prerelease finalize-slice' $(MAKE_RECURSE) __finalize-slice
	$(TIMED) 'prerelease valgrind' $(MAKE_RECURSE) __valgrind
	$(TIMED) 'prerelease fuzz-smoke' $(MAKE_RECURSE) __fuzz-smoke
	$(TIMED) 'prerelease e2e' $(MAKE_RECURSE) __test-e2e
	$(TIMED) 'prerelease bench-gate' $(MAKE_RECURSE) __bench-gate

__prerelease: __prerelease-ordinary

prerelease-live:
	$(TIMED) prerelease-live $(MAKE_RECURSE) __prerelease-live

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
	$(TIMED) prerelease-hardening $(MAKE_RECURSE) __prerelease-hardening

__prerelease-hardening: __prerelease __pouch-core-hardening __fuzz __release-matrix

lifecycle-version-contract:
	$(TIMED) lifecycle-version-contract $(MAKE_RECURSE) __lifecycle-version-contract

__lifecycle-version-contract:
	bash ./scripts/lifecycle-version-contract.sh

print-release-version:
	@bash ./scripts/release_version.sh

__release:
	bash ./scripts/release.sh

__release-pipeline: __prerelease __release-matrix

release-matrix:
	$(TIMED) release-matrix $(MAKE_RECURSE) __release-matrix

__release-matrix:
	bash ./scripts/run_linux_release_matrix.sh

clean:
	$(TIMED) clean $(MAKE_RECURSE) __clean

__clean:
	bash ./scripts/clean.sh

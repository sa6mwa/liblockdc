if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

file(READ "${LOCKDC_ROOT}/Makefile" root_makefile)
file(READ "${LOCKDC_ROOT}/scripts/pouch_benchmark_parity.py" parity_script)

foreach(snippet
        "POUCH_GO_TEST_TIMEOUT ?= 10m"
        "POUCH_GO_TEST_RUN ?= ^$$"
        "POUCH_GO_PARITY_BENCH ?= ^BenchmarkProduction(PouchPT|PouchCrypto|LockdDiskNoCrypto|LockdDiskCrypto)$$$$$$$$/.*"
        "POUCH_GO_PARITY_TIMEOUT ?= 90s"
        "POUCH_GO_DURABLE_GATE_TIMEOUT ?= 90s"
        "POUCH_GO_CORE_SOAK_TIMEOUT ?= 10m"
        "timeout --kill-after=5s '$(POUCH_GO_TEST_TIMEOUT)'"
        "$(GO) test -run '$(POUCH_GO_TEST_RUN)' -bench '$(POUCH_GO_BENCH)'"
        "-timeout '$(POUCH_GO_TEST_TIMEOUT)'"
        "POUCH_GO_TEST_TIMEOUT='$(POUCH_GO_ACCEPTANCE_TIMEOUT)'"
        "benchmark-pouch-go-production:"
        "POUCH_GO_TEST_TIMEOUT='$(POUCH_GO_PRODUCTION_TIMEOUT)'"
        "POUCH_GO_PRODUCTION_BENCH='$(POUCH_GO_PARITY_BENCH)'"
        "POUCH_GO_PRODUCTION_TIMEOUT='$(POUCH_GO_PARITY_TIMEOUT)'"
        "POUCH_GO_BENCH='$(POUCH_GO_CORE_SOAK_BENCH)'"
        "POUCH_GO_BENCHTIME='$(POUCH_GO_CORE_SOAK_BENCHTIME)'"
        "POUCH_GO_TEST_TIMEOUT='$(POUCH_GO_CORE_SOAK_TIMEOUT)'"
        "'$(POUCH_GO_CORE_SOAK_TIMEOUT)' $(MAKE_RECURSE) __benchmark-pouch-go-core-soak"
        "LOCKDC_BENCH_PRODUCTION_PAYLOAD_BYTES='$(POUCH_GO_PRODUCTION_PAYLOAD_BYTES)'")
    string(FIND "${root_makefile}" "${snippet}" snippet_index)
    if(snippet_index EQUAL -1)
        message(FATAL_ERROR "Makefile is missing Go benchmark timeout contract snippet: ${snippet}")
    endif()
endforeach()

# Check this before any dry run: GNU Make executes recipes containing a direct
# $(MAKE) reference even under -n.
string(REGEX MATCH "\n\t[^\n]*\\$\\(MAKE\\)"
                   direct_recursive_make_recipe "${root_makefile}")
if(NOT direct_recursive_make_recipe STREQUAL "")
    message(FATAL_ERROR
        "Makefile contains a direct $(MAKE) recipe reference; make -n would execute it")
endif()

# Check the gate's expanded command includes the bounded profile.
# Dry runs must never execute the workload.
find_program(make_program NAMES gmake make REQUIRED)
execute_process(
    COMMAND "${make_program}" -n __benchmark-pouch-go-parity-gate
    WORKING_DIRECTORY "${LOCKDC_ROOT}"
    RESULT_VARIABLE dry_run_result
    OUTPUT_VARIABLE gate_command
    ERROR_VARIABLE dry_run_error)
if(NOT dry_run_result EQUAL 0)
    message(FATAL_ERROR "Gate dry run failed: ${dry_run_error}")
endif()
foreach(argument
        "POUCH_GO_PRODUCTION_ROWS='12'"
        "POUCH_GO_PRODUCTION_UPDATES='2'"
        "POUCH_GO_PRODUCTION_PAYLOAD_BYTES='131072'"
        "POUCH_GO_PRODUCTION_SEGMENT_TARGET_BYTES='16384'"
        "POUCH_GO_BENCH_COUNT='5'"
        "POUCH_GO_PRODUCTION_TIMEOUT='90s'")
    string(FIND "${gate_command}" "${argument}" argument_index)
    if(argument_index EQUAL -1)
        message(FATAL_ERROR "Gate command is missing ${argument}")
    endif()
endforeach()

execute_process(
    COMMAND "${make_program}" -n __benchmark-pouch-go-prepare
    WORKING_DIRECTORY "${LOCKDC_ROOT}"
    RESULT_VARIABLE prepare_result
    OUTPUT_VARIABLE prepare_command
    ERROR_VARIABLE prepare_error)
if(NOT prepare_result EQUAL 0)
    message(FATAL_ERROR "Benchmark preparation dry run failed: ${prepare_error}")
endif()
string(FIND "${prepare_command}" "--target lc_static" static_target_index)
if(static_target_index EQUAL -1 OR prepare_command MATCHES "deps-x86_64-linux-musl")
    message(FATAL_ERROR "Benchmark preparation must build only its native static input")
endif()

# Every production speed claim is a core lockd invariant. Keep the parser
# allowlist explicit so a benchmark refactor cannot silently drop one from the
# release gate while leaving only aggregate timing behind.
foreach(metric
        "acquire-one-ns/op"
        "update-one-ns/op"
        "release-one-ns/op"
        "stale-ns/op"
        "get-public-ns/op"
        "get-lease-ns/op"
        "attachment-write-ns/op"
        "attachment-read-ns/op"
        "queue-one-ns/op"
        "flush-intermediate-ns/op"
        "flush-final-ns/op"
        "flush-noop-ns/op"
        "flush-reopen-ns/op"
        "index-query-keys-ns/op"
        "index-query-keys-warm-ns/op"
        "index-query-docs-ns/op"
        "scan-query-keys-ns/op"
        "scan-query-docs-ns/op"
        "full-text-index-keys-ns/op"
        "full-text-scan-docs-ns/op"
        "restart-recovery-ns/op")
    string(FIND "${parity_script}" "\"${metric}\"" metric_index)
    if(metric_index EQUAL -1)
        message(FATAL_ERROR
            "Pouch parity gate is missing required core metric: ${metric}")
    endif()
endforeach()

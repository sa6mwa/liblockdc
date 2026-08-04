if(NOT DEFINED LOCKDC_ROOT)
    message(FATAL_ERROR "LOCKDC_ROOT is required")
endif()

file(READ "${LOCKDC_ROOT}/Makefile" root_makefile)

foreach(snippet
        "POUCH_GO_TEST_TIMEOUT ?= 10m"
        "POUCH_GO_TEST_RUN ?= ^$$"
        "POUCH_GO_PARITY_BENCH ?= ^BenchmarkProduction(PouchPT|PouchCrypto|LockdDiskNoCrypto|LockdDiskCrypto)$$$$$$$$/.*"
        "POUCH_GO_PARITY_TIMEOUT ?= 15m"
        "timeout --kill-after=5s '$(POUCH_GO_TEST_TIMEOUT)'"
        "$(GO) test -run '$(POUCH_GO_TEST_RUN)' -bench '$(POUCH_GO_BENCH)'"
        "-timeout '$(POUCH_GO_TEST_TIMEOUT)'"
        "POUCH_GO_TEST_TIMEOUT='$(POUCH_GO_ACCEPTANCE_TIMEOUT)'"
        "benchmark-pouch-go-production:"
        "POUCH_GO_TEST_TIMEOUT='$(POUCH_GO_PRODUCTION_TIMEOUT)'"
        "POUCH_GO_PRODUCTION_BENCH='$(POUCH_GO_PARITY_BENCH)'"
        "POUCH_GO_PRODUCTION_TIMEOUT='$(POUCH_GO_PARITY_TIMEOUT)'"
        "LOCKDC_BENCH_PRODUCTION_PAYLOAD_BYTES='$(POUCH_GO_PRODUCTION_PAYLOAD_BYTES)'")
    string(FIND "${root_makefile}" "${snippet}" snippet_index)
    if(snippet_index EQUAL -1)
        message(FATAL_ERROR "Makefile is missing Go benchmark timeout contract snippet: ${snippet}")
    endif()
endforeach()

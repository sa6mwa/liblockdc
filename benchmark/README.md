# liblockdc pouch comparison benchmarks

This module is intentionally outside the CMake release gate. It is used by the
root `benchmark-pouch-go*` Makefile targets for opt-in pouch performance and
stress work.

The pouch benchmark path calls into C through cgo and reports C-measured query
time as `c-ns/op`, so the cgo bridge is not the metric used for pouch query
latency. The Go benchmark `ns/op` still includes harness/setup work and is only
useful as a coarse runner signal.

`BenchmarkFastLockdDisk` and `BenchmarkMediumLockdDisk*` launch a real pinned
`pkt.systems/lockd` binary with a disk backend rooted in an automatically
removed `/tmp/liblockdc-lockd-disk-bench-*` directory. The disk side uses the
exported Go client and full-form LQL expressions. The pouch side measures the
C query path directly through `selector_lql` and reports C-side query time so
the cgo bridge is excluded from pouch latency metrics.

`make benchmark-pouch-go-production` runs the production workload for three
explicit variants by default: `ProductionPouchPT`, `ProductionPouchCrypto`, and
`ProductionLockdDiskNoCrypto`. Pouch crypto is enabled through the public
`pouch_crypto_key` endpoint option; the Go lockd disk server is started with
`--disable-storage-encryption`. The production target reports split flush
metrics so index flush work can be attributed to intermediate write-churn
flushes, final flush, no-op flush, and post-reopen flush.

`make benchmark-pouch-go-medium` mirrors the Go lockd disk comparison shape over
both key and document result modes for scan and indexed engines. The default
matrix includes sparse/dense equality, numeric range, `in`, array membership,
case-sensitive and case-insensitive text, date, root OR, recursive exists,
tenant/workflow/amount predicates, narrative text, and full-document text search
over production-shaped nested documents. `make benchmark-pouch-go-acceptance`
keeps the same 4096-document cap and runs the broader indexed comparison plus a
bounded scan subset.

`make benchmark-pouch-go-compaction` runs opt-in pouch-only compaction
benchmarks. It reports forced maintenance compaction time as `compaction-ns/op`
and scheduled compaction impact through `write-ns/op` plus `max-write-ns/op`,
because scheduled compaction runs synchronously inside the write that crosses
the threshold. Forced compaction keeps both default profiles; scheduled
compaction uses a bounded default profile so the target is usable as a routine
gate while still exercising an early compaction threshold. Setting any
`POUCH_GO_COMPACTION_*` value replaces the defaults with a single env-driven
scenario. Use these environment knobs to simulate different schedules:

- `POUCH_GO_COMPACTION_ROWS`
- `POUCH_GO_COMPACTION_UPDATES`
- `POUCH_GO_COMPACTION_PAYLOAD_BYTES`
- `POUCH_GO_COMPACTION_SEGMENT_TARGET_BYTES`
- `POUCH_GO_COMPACTION_MIN_SEGMENTS`
- `POUCH_GO_COMPACTION_MIN_RECLAIMABLE_BYTES`

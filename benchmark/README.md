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
`--disable-storage-encryption`.

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

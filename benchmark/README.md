# liblockdc pouch comparison benchmarks

This module is intentionally outside the CMake release gate. It is used by the
root `benchmark-pouch-go*` Makefile targets for opt-in pouch performance and
stress work.

The pouch benchmark path calls into C through cgo and reports C-measured query
time as `c-ns/op`, so the cgo bridge is not the metric used for pouch query
latency. The Go benchmark `ns/op` still includes harness/setup work and is only
useful as a coarse runner signal.

For pouch-only performance iteration, use the native `bench/lockdc_bench`
surface through the root `benchmark-pouch-perf*` targets instead of the Go
comparison matrix. These targets rebuild only `bench/lockdc_bench`, seed a
bounded public-API pouch fixture, warm the index when needed, then print one
isolated `metric=...` line for the phase under repair. The default timeout is
60 seconds including rebuild. The fast cases are:

- `make benchmark-pouch-perf-index-docs`
- `make benchmark-pouch-perf-full-text-keys`
- `make benchmark-pouch-perf-scan-keys`
- `make benchmark-pouch-perf-flush-intermediate`
- `make benchmark-pouch-perf-flush-reopen`

Set `POUCH_PERF_ROWS`, `POUCH_PERF_PAYLOAD_BYTES`, or `POUCH_PERF_CRYPTO=1` to
adjust the local fixture without switching to the broad parity suite.

`make benchmark-pouch-routine` is the bounded development suite. It runs the
six native phase probes at 12 rows and 128 KiB payloads, the 12-row segmented
production matrix, and the two-writer/eight-write shared-root concurrency
matrix. A single 90-second outer timeout covers the complete command;
`POUCH_GO_ROUTINE_TIMEOUT`, `POUCH_PERF_ROUTINE_ROWS`,
`POUCH_PERF_ROUTINE_PAYLOAD_BYTES`, and the
`POUCH_GO_ROUTINE_CONCURRENCY_*` variables adjust its profile. The broader
medium, acceptance, compaction, and parity-gate commands remain opt-in.

`BenchmarkFastLockdDisk` and `BenchmarkMediumLockdDisk*` launch a real pinned
`pkt.systems/lockd` binary with a disk backend rooted in an automatically
removed `/tmp/liblockdc-lockd-disk-bench-*` directory. The disk side uses the
exported Go client and full-form LQL expressions. The pouch side measures the
C query path directly through `selector_lql` and reports C-side query time so
the cgo bridge is excluded from pouch latency metrics.

`make benchmark-pouch-go-production` runs the production workload for five
explicit variants by default: `ProductionPouchPT`, `ProductionPouchCrypto`,
`ProductionPouchCompression`, `ProductionPouchCryptoCompression`, and
`ProductionLockdDiskNoCrypto`. Pouch crypto is enabled through the public
`pouch_crypto_key` endpoint option; the Go lockd disk server is started with
`--disable-storage-encryption`. The production target reports split flush
metrics so index flush work can be attributed to intermediate write-churn
flushes, final flush, no-op flush, and post-reopen flush.

`make benchmark-pouch-go-production-bounded` is the routine full-matrix
coverage target. It uses one iteration of 12 rows, two updates per key, a
128 KiB historical payload, and a shared 16 KiB segment target. The target
forces rolling and still validates stale ETag rejection, attachments, queue
round trips, replay, indexed and scan queries, full-text queries, and public
reads. The common target is passed to Pouch through `segment_target_bytes` and
to Go disk through `--logstore-segment-size`; this is necessary because Pouch
compression changes stored bytes and therefore rollover frequency. Override
the bounded values with the `POUCH_GO_BOUNDED_PRODUCTION_*` variables, or use
the corresponding `POUCH_GO_PRODUCTION_*` variables for a custom production
profile.

Production output reports `restart-recovery-ns/op`, the complete close/reopen
plus first post-reopen index-flush path. This is the cross-engine recovery
metric: lockd disk eagerly restores state during server startup, while Pouch
loads the namespace lazily when the first operation needs it. The existing
`reopen-ns/op` and `flush-reopen-ns/op` metrics remain diagnostic sub-phases and
must not be compared independently across the two implementations.

`make benchmark-pouch-go-concurrency` is the bounded lock and shared-root
comparison matrix. It runs Pouch and Go lockd disk with crypto disabled and
enabled, over a contended single key and independent keys. Pouch opens the
requested number of distinct liblockdc clients with `pouch_single_writer=false`
against one root. The same-key case uses public acquire/update/release calls and
asserts the final version equals every completed write; the independent-key case
reads back every key at version one. This confirms both key-lock serialization
and multi-client shared-root mutation behavior.

Go lockd disk is measured through its supported topology: the same number of
independent Go clients connect to one disk-backed lockd server. The Go disk
backend itself remains a single disk writer and does not support several disk
server instances mutating one root. The target reports `write-wall-ns/op`,
`write-mean-ns/op`, `max-write-ns/op`, and `writes/s`; readback validation is
outside the timed region. Defaults are two writers, 32 writes per writer, and a
256-byte payload. Override them with `POUCH_GO_CONCURRENCY_WRITERS`,
`POUCH_GO_CONCURRENCY_WRITES_PER_WRITER`, and
`POUCH_GO_CONCURRENCY_PAYLOAD_BYTES`.

The Go harness does not pass `--ha`, so lockd uses its default `failover` mode.
Go disk rejects `concurrent` for disk roots, and Go core marks failover writes
`NoSync`. Pouch defaults to `durable_sync=false`, so this matrix uses the same
power-loss durability boundary while comparing contention, shared-root,
key-lock, and observed throughput. Run `durable_sync=true` separately when a
stronger Pouch `fdatasync` group-commit boundary is required; do not compare
that strict mode directly to the default Go failover numbers.

Crypto mode measures the operational overhead of each engine's enabled
at-rest-encryption configuration, not a byte-for-byte cryptographic-format
comparison: Pouch uses one generated `pouch_crypto_key` across its clients, and
the bootstrapped Go lockd disk server enables its storage encryption.

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
and background-scheduled compaction impact through `write-ns/op` plus
`max-write-ns/op`. A Pouch pthread worker performs scheduled work after a
mutation signals it, so foreground writes do not execute the compaction pass.
Forced compaction keeps both default profiles; scheduled compaction uses a
bounded default profile so the target is usable as a routine gate while still
exercising an early compaction threshold. Setting any
`POUCH_GO_COMPACTION_*` value replaces the defaults with a single env-driven
scenario. Use these environment knobs to simulate different schedules:

- `POUCH_GO_COMPACTION_ROWS`
- `POUCH_GO_COMPACTION_UPDATES`
- `POUCH_GO_COMPACTION_PAYLOAD_BYTES`
- `POUCH_GO_COMPACTION_SEGMENT_TARGET_BYTES`
- `POUCH_GO_COMPACTION_MIN_SEGMENTS`
- `POUCH_GO_COMPACTION_MIN_RECLAIMABLE_BYTES`

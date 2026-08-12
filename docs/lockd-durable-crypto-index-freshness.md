# lockd durable encrypted disk index freshness

Status: upstream bug report candidate

Observed during the liblockdc release gate while preparing the patch release after `v0.13.0`, against `pkt.systems/lockd v0.9.0` as pulled by the Go benchmark module.

## Summary

The `lockd` disk backend can intermittently report a successful durable encrypted index flush after restart while the first indexed query returns an empty result set.

The observed failure was in the Go `lockd` comparison benchmark, not in liblockdc's C Pouch backend and not in the CMake/pkg-config packaging change under release.

## Failing liblockdc gate

The release gate reached:

```text
make release
```

During the benchmark phase:

```text
perf-gate pouch-go-durable-parity
make __benchmark-pouch-go-durable-gate
```

The benchmark command was:

```text
go test -run '^$' -bench 'Production(PouchDurablePT|PouchDurableCrypto|LockdDiskDurableNoCrypto|LockdDiskDurableCrypto)' -benchtime '1x' -count '3' -timeout '3m'
```

The failing benchmark case was:

```text
BenchmarkProductionLockdDiskDurableCrypto/Env/Rows12/Updates2/Payload131072/SegmentTarget16384
```

Captured failure:

```text
--- FAIL: BenchmarkProductionLockdDiskDurableCrypto/Env/Rows12/Updates2/Payload131072/SegmentTarget16384
    production_bench_test.go:1075: lockd disk production RangeHalf index query matched 0 rows, want 12
--- FAIL: BenchmarkProductionLockdDiskDurableCrypto/Env/Rows12/Updates2/Payload131072/SegmentTarget16384-4
    production_bench_test.go:1075: lockd disk production RangeHalf index query matched 0 rows, want 12
```

The log file from the interrupted release run was:

```text
build/pouch-go-durable.bench.txt
```

## Scenario details

The benchmark starts a `lockd` server from the Go module dependency:

```text
pkt.systems/lockd v0.9.0
```

The server is launched through the benchmark harness with:

```text
--store disk://<temp data root>
--ha auto
--indexer-flush-docs 64
--indexer-flush-interval 1s
```

For `BenchmarkProductionLockdDiskDurableCrypto`, storage encryption is enabled. The non-crypto durable benchmark in the same release-gate run completed before this failure.

The failing sequence inside the benchmark is:

1. Write 12 JSON documents, with 2 updates per key.
2. Flush the index with `FlushIndex(wait)`.
3. Issue a second no-op `FlushIndex(wait)`.
4. Restart the `lockd` process against the same disk root.
5. Issue a post-restart `FlushIndex(wait)`.
6. Query with the index engine for the `RangeHalf` selector.
7. Observe 0 matched keys instead of 12.

That means committed state survived the test flow, the post-restart flush returned success, but the indexed query did not observe the expected committed documents.

## Reproduction notes

The failure is intermittent.

The release-gate durable benchmark log contained:

```text
bench_samples=2 fail_markers=2 rangehalf_zero=2 pass_lines=1 ok_lines=1
```

Follow-up focused reproduction did not trigger the same miss:

```text
make __benchmark-pouch-go-durable \
  POUCH_GO_DURABLE_BENCH='ProductionLockdDiskDurableCrypto' \
  POUCH_GO_DURABLE_BENCHTIME='1x' \
  POUCH_GO_DURABLE_COUNT='10' \
  POUCH_GO_DURABLE_TIMEOUT='5m'
```

Result:

```text
10/10 focused iterations passed
```

Longer focused run:

```text
make __benchmark-pouch-go-durable \
  POUCH_GO_DURABLE_BENCH='ProductionLockdDiskDurableCrypto' \
  POUCH_GO_DURABLE_BENCHTIME='1x' \
  POUCH_GO_DURABLE_COUNT='50' \
  POUCH_GO_DURABLE_TIMEOUT='8m'
```

Result:

```text
50/50 focused iterations passed
```

The focused run log was:

```text
build/lockd-durable-crypto-count50.log
```

This supports classifying the data miss as a low-frequency freshness/recovery race or harness-sensitive timing issue.

## Likely upstream area

The benchmark drives Go `lockd` through `pkt.systems/lockd/client`, not through liblockdc's C Pouch implementation.

The relevant upstream components appear to be:

- `internal/search/index.Manager.FlushNamespace`
- `internal/search/index.Writer.Flush`
- `internal/search/index.Store.LoadManifestReadOnly`
- `internal/search/index.Store.SaveManifest`
- encrypted disk object reads/writes under `internal/storage/disk`

`FlushNamespace` forces an in-process namespace writer flush. After a process restart, there may be no writer instance for that namespace yet, so the flush path can rely entirely on already-persisted manifest and segment artifacts. The observed symptom is consistent with a post-restart path where the flush request returns success but the subsequent index reader sees a structurally valid index state that does not contain the expected `RangeHalf` postings.

This report does not prove whether the root cause is:

- stale or incomplete manifest visibility after restart;
- stale or incomplete segment visibility after encrypted disk writes;
- an index writer/visibility writer interaction in `--ha auto`;
- a benchmark harness assumption that `FlushIndex(wait)` is stronger than the upstream contract after restart.

It does prove that a release-gate run observed `FlushIndex(wait)` followed by an indexed query returning an empty result for a dataset that should match all 12 rows.

## Secondary local gate issue

The release gate did not stop automatically even though the Go benchmark emitted `--- FAIL`.

Two local benchmark-gate issues explain that:

1. The benchmark code ignores the boolean returned by `testing.B.Run`, even though Go documents that return value as reporting sub-benchmark failures.
2. `scripts/pouch_benchmark_parity.py` parses metric lines only and does not fail when the benchmark output contains `--- FAIL`.

As a result, this failed sub-benchmark still produced package-level `PASS` / `ok`, and the parity parser returned success because it had enough successful metric samples to compare.

That local gate-hardening issue should be fixed in liblockdc independently, so any future benchmark assertion failure stops the release immediately.

## Suggested upstream investigation

Suggested upstream checks:

- Add a focused `lockd` test for disk + encryption + `--ha auto` that writes indexed documents, flushes, restarts, flushes again, and asserts indexed query parity with scan.
- Run the same test with high iteration count and randomized short sleeps around flush, restart, and first query.
- Log manifest ETag/sequence, segment IDs, and index result counts around `FlushNamespace`, `LoadManifestReadOnly`, and `Query`.
- Distinguish explicitly between "flush pending in-process writer buffers" and "validate/rebuild index readability after restart" in `/v1/index/flush` semantics.

The important contract to settle upstream is whether `FlushIndex(wait)` after restart must make the latest committed state readable through the index engine, or whether callers must request a stronger refresh/repair mode before relying on indexed results.

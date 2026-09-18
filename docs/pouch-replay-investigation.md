# Pouch replay read amplification

## Finding (2026-09-11)

A public-API, fresh-process reproduction on v0.16.0 code found quadratic
logical reads during the **first acquire**, not `lc_client_open` itself.
The trigger is a surviving staged update alongside historical decisions for
already completed updates. Recovery scanned the full namespace log separately
for every historical decision to discover whether its staged key still existed.

The fix uses the existing mode-aware namespace projection lookup. It retains
mutation serialization, shared-writer refresh, and cache-invalidation handling.
No API, storage format, cancellation contract, or lease policy changes.
Recovery still traverses historical decisions; this is not a bounded-time or
cancellable-open implementation.

The supplied Vectis reproduction completed and released every update before
stopping its writer. That does not exercise this trigger. Earlier optimization
in v0.14.0 skips decision recovery when no live staged records survive, but the
conservative recovery path still had the per-decision full scan.

This demonstrates a current defect consistent with the incident's read
amplification. It does **not** establish the deployed binary's revision or the
contents of its store, so attribution of the production incident remains open.

## Reproduction

On Linux, using the normal debug build:

```sh
make build
python3 tests/e2e/pouch_replay.py build/debug/bench/lockdc_pouch_replay_probe \
  --keys 1 --updates 256 --encrypted --shared --unclean --live-staged
ctest --test-dir build/debug -R '^pouch_replay_' --output-on-failure --parallel 1 --no-tests=error
```

The driver creates disposable roots underneath the working directory, seeds
through the public client/lease API, and starts two independent reader
processes. It checks every committed JSON value and logical version. Reads use
acquire/get/release, so probes append lease records; the second open observes
that slightly larger root. `--live-staged` leaves an additional uncommitted
update on a different key. `_exit` prevents clean-close projection creation;
it does not simulate a torn write, power loss, or cold page cache.

Measurements separate open, first acquire/get/release, remaining keys, and
close. Linux `/proc/self/io` provides logical bytes and read calls, including
page-cache hits; these are process-wide, not segment-exclusive. The probe also
reports current and peak resident bytes from `/proc/self/status` for every
phase. Wall and CPU times are diagnostic only. The assertion bounds startup
reads by 64 times the pre-open store size plus 1 MiB metadata allowance. It is
a regression detector, not a promised storage complexity limit. The CTest
watchdog is only a hang guard; neither fixture creation nor assertions use
sleeps or timing thresholds.

Two registered offline e2e cases exercise plain/encrypted shared roots with
64 KiB segment targets. The Pouch unit recovery test separately verifies that
a decided staged key is discarded while an undecided staged key retains its
version and etag.

## Observed results

Bootlin GCC stable-2026.08, ASan/UBSan debug build, warm filesystem cache.
Sizes below are decimal MB. Times are local measurements, not performance
guarantees.

| Fixture | Open reads | First acquire/get/release reads | First-operation time |
| --- | ---: | ---: | ---: |
| 256 completed updates + one staged, before fix (0.823 MB root) | 0.843 MB | 213.156 MB | 3.640 s |
| Same fixture, after fix | 0.843 MB | 0.840 MB | 0.023 s |
| 1,024 keys × 4 generations + one staged, after fix (13.36 MB root) | 13.531 MB | 12.771 MB | 0.591 s |

Before fixing, the small fixture failed the logical-read assertion (214 MB
startup reads against a 53.7 MB bound). After fixing, both successive opens
passed. The larger triggering fixture also passed both opens with every value
and version intact.

Control workloads without surviving staged updates did not reproduce:

- Encrypted/shared/unclean, 256 keys × 4 generations: approximately 3.2 MB
  open reads for a 3.2 MB root.
- Encrypted/shared/unclean, 1,024 keys × 4 generations: approximately 13.5 MB
  open reads for a 13.4 MB root.
- Encrypted/shared/unclean, one key × 20,000 updates: approximately 65.3 MB
  open reads for a 64.9 MB root; open took 1.84–2.05 seconds.
- Plain/clean/exclusive, four namespaces × 256 keys × 4 generations:
  approximately 1.16 MB open reads for an 11.9 MB root (clean projection).

These controls were measured before the fix. The triggering regression cases
and full debug suite were run after it. Historical binaries, production data,
cold-cache behavior, and power-loss recovery were not tested here.

## Captured C89 metrics diagnosis (2026-09-18)

The captured `c89-systems` executable is Vectis 0.15.1 statically linked with
liblockdc 0.13.1. Its metrics worker persists one checkpoint through a new
public liblockdc client at most once every five minutes: it opens the client,
acquires and updates the checkpoint, then closes the client. The Vectis
process is long-lived, but the Pouch client used for this operation is not.

The encrypted metrics namespace has one 21,653,369-byte active segment after
migration, with 34,753 frames:

| Frame | Count |
| --- | ---: |
| state put | 4,687 |
| state delete | 4,687 |
| state link | 4,687 |
| state metadata | 16,005 |
| transaction decision | 4,687 |

There are 4,688 logical keys in the terminal projection: 4,687 deleted
checkpoint/staging keys and one metadata key. Legacy `LPL1` records describe
those canonical keys; they are not independent state-cache keys. The root has
no query-index artifacts, and no live payload body large enough to account for
the reported resident memory.

This explains the periodic CPU report. On every fresh client, 0.13.1 runs
staged-decision recovery before the checkpoint acquire. It enumerates all
4,687 durable decisions and calls the full-log state scan once for each one,
even though their staging keys are already absent. For this 21.65 MB encrypted
segment that is about 101.5 GB of repeated logical traversal for one periodic
checkpoint. A single core busy for minutes is therefore expected behavior from
the deployed binary, not evidence of a compaction loop.

The current implementation replaces the per-decision scan with a lookup in
the namespace projection. The existing offline e2e uses separate processes,
retains live staging, and bounds logical reads instead of elapsed time. A
capture-scale fixture with 4,687 historical decisions completed its first
recovery operation in about 0.17 CPU seconds. On a disposable migrated copy of
the actual metrics root, subsequent encrypted opens read one 21.7 MB segment
and used about 0.054 CPU seconds with a 10.5 MB RSS in a non-sanitized build.

The current root is below the default 64 MiB active-segment rollover, so normal
compaction has no inactive segment to reclaim. That is a storage-growth policy
question, but it was not the source of this incident. The separately identified
`lc_pouch_state_cache_warm_transformed_bodies` policy can still materialize
every eligible live encrypted or compressed body at open (up to its cache
limit). This capture has no such live bodies, so it is a distinct issue that
requires its own bounded-cache design and regression coverage.

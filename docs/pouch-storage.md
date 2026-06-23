# Pouch Storage Design

`pouch` is the embedded storage runtime for `liblockdc`. It gives the C client a
local `lockd`-compatible backend selected by `pouch://` endpoints, without
requiring a local server process. The storage layer must be usable by the client
adapter first and remain suitable for a future C server stack.

This document specifies the storage architecture before implementation. It is
based on the proven shape of the existing server-side disk store: a storage
backend interface, a disk-log backend, per-namespace append logs, in-memory
indexes rebuilt from durable records, advisory locks for cross-process writers,
and compaction snapshots. The C implementation must be idiomatic C89 and must
not copy runtime assumptions from the original implementation language, such as
managed allocation, channels, or built-in maps.

## Goals

- Provide lockd-compatible local storage for state, leases, attachments, queues,
  and consumer-service workflows.
- Support multiple independent client instances pointed at the same store root,
  including roots shared over NFSv4, CephFS, or similar filesystems that support
  advisory byte-range locks.
- Keep the backend single-writer at the mutation level while allowing readers to
  refresh their indexes from committed log records.
- Make LQL/query integration a first-class v1 requirement, but keep the storage
  and index boundary independent from the unfinished C LQL library.
- Avoid hidden memory allocation. Storage code must allocate only through a
  pouch allocator interface.
- Add benchmarks and diagnostics from the start so write latency, read latency,
  compaction cost, allocator behavior, and index rebuild cost are measurable.
- Bump the public ABI only once during the pouch feature release.

## Non-Goals

- Pouch v1 does not implement management APIs.
- Pouch v1 does not implement authentication, authorization, permissions, TLS,
  or remote networking concerns.
- Pouch is not a fake HTTP server. The client adapter may preserve the public
  `lc_client` API, but storage calls should reach the backend through a typed
  internal interface.
- Pouch must not depend on filesystem notification for correctness. Polling and
  explicit refresh are required. Filesystem notifications may be an optimization
  later.

## Layering

Pouch should be split into three layers:

1. `lc_pouch_store`
   A backend interface and common value types. This layer knows lockd storage
   semantics but not URI parsing or HTTP transport.

2. `lc_pouch_disk`
   The first backend implementation. It persists records in a log-structured
   directory, maintains per-namespace indexes, coordinates cross-process writers
   with advisory locks, and compacts old segments.

3. `lc_pouch_client`
   The internal adapter from `lc_engine_client` operations to `lc_pouch_store`.
   This is selected when a configured endpoint uses the `pouch://` scheme.

The existing curl transport remains the HTTP backend. The engine should grow a
small internal backend vtable instead of scattering `pouch://` branches through
HTTP request helpers.

## Backend Interface

The storage backend should be represented as a receiver-function struct. Names
below are illustrative; final names should match local conventions.

```c
typedef struct lc_pouch_store lc_pouch_store;

struct lc_pouch_store {
  void *impl;

  int (*load_meta)(lc_pouch_store *self, const char *namespace_name,
                   const char *key, lc_pouch_meta_record *out,
                   lc_error *error);
  int (*store_meta)(lc_pouch_store *self, const char *namespace_name,
                    const char *key, const lc_pouch_meta *meta,
                    const char *expected_etag, char **new_etag,
                    lc_error *error);
  int (*delete_meta)(lc_pouch_store *self, const char *namespace_name,
                     const char *key, const char *expected_etag,
                     lc_error *error);
  int (*scan_meta)(lc_pouch_store *self, const lc_pouch_scan_meta_req *req,
                   lc_pouch_scan_meta_visit_fn visit, void *visit_ctx,
                   lc_pouch_scan_meta_res *out, lc_error *error);

  int (*read_state)(lc_pouch_store *self, const char *namespace_name,
                    const char *key, lc_source **body,
                    lc_pouch_state_info *out, lc_error *error);
  int (*write_state)(lc_pouch_store *self, const char *namespace_name,
                     const char *key, lc_source *body,
                     const lc_pouch_put_state_opts *opts,
                     lc_pouch_put_state_res *out, lc_error *error);
  int (*remove_state)(lc_pouch_store *self, const char *namespace_name,
                      const char *key, const char *expected_etag,
                      lc_error *error);

  int (*list_objects)(lc_pouch_store *self, const lc_pouch_list_objects_req *req,
                      lc_pouch_list_objects_res *out, lc_error *error);
  int (*get_object)(lc_pouch_store *self, const char *namespace_name,
                    const char *key, lc_source **body,
                    lc_pouch_object_info *out, lc_error *error);
  int (*put_object)(lc_pouch_store *self, const char *namespace_name,
                    const char *key, lc_source *body,
                    const lc_pouch_put_object_opts *opts,
                    lc_pouch_object_info *out, lc_error *error);
  int (*delete_object)(lc_pouch_store *self, const char *namespace_name,
                       const char *key,
                       const lc_pouch_delete_object_opts *opts,
                       lc_error *error);
  int (*copy_object)(lc_pouch_store *self, const char *namespace_name,
                     const char *src_key, const char *dst_key,
                     const lc_pouch_copy_object_opts *opts,
                     lc_pouch_object_info *out, lc_error *error);

  int (*stage_state)(lc_pouch_store *self, const char *namespace_name,
                     const char *key, const char *txn_id, lc_source *body,
                     const lc_pouch_put_state_opts *opts,
                     lc_pouch_put_state_res *out, lc_error *error);
  int (*load_staged_state)(lc_pouch_store *self, const char *namespace_name,
                           const char *key, const char *txn_id,
                           lc_source **body, lc_pouch_state_info *out,
                           lc_error *error);
  int (*promote_staged_state)(lc_pouch_store *self,
                              const char *namespace_name, const char *key,
                              const char *txn_id,
                              const lc_pouch_promote_staged_opts *opts,
                              lc_pouch_put_state_res *out, lc_error *error);
  int (*discard_staged_state)(lc_pouch_store *self,
                              const char *namespace_name, const char *key,
                              const char *txn_id,
                              const lc_pouch_discard_staged_opts *opts,
                              lc_error *error);
  int (*list_staged_state)(lc_pouch_store *self,
                           const lc_pouch_list_staged_req *req,
                           lc_pouch_list_objects_res *out, lc_error *error);

  int (*backend_hash)(lc_pouch_store *self, char **out, lc_error *error);
  int (*close)(lc_pouch_store *self, lc_error *error);
  int (*abort)(lc_pouch_store *self, lc_error *error);
};
```

The interface intentionally separates metadata, state blobs, and arbitrary
objects. Queue messages and attachments can use the object plane while lease and
state coordination use the metadata/state plane. Query hot paths need a summary
scanner so LQL can avoid loading full metadata and payloads for every key.

The interface should also expose optional capability functions or flags:

- namespace listing
- queue change polling hints
- exclusive writer probing
- single-writer optimization mode
- fsync statistics
- compaction statistics and explicit compaction trigger
- index flush/default tuning for LQL
- retention/janitor sweep for expired metadata and state

## Performance Model

The disk store is conceived as an append-first system where expensive work is
moved out of the foreground path whenever correctness allows it:

- writes append compact binary records instead of rewriting object files;
- readers use in-memory indexes and only decode full metadata or payloads when
  needed;
- query hot paths scan metadata summaries rather than full documents;
- small payloads are encoded into bounded inline records for fewer syscalls;
- large payloads stream directly to the segment while hashes and CRCs are
  computed incrementally;
- fsync calls can be grouped so multiple logical commits pay for one physical
  sync;
- compaction is snapshot-based and validates that live refs did not drift before
  installing a new snapshot;
- all hot allocations use pouch-owned buffers, pools, or slabs so allocation
  behavior is measurable and controllable.

C makes the allocation side easier to control, but it does not remove the need
for allocation discipline. The pouch implementation should be written so a
benchmark can prove how many allocations each operation performs and a
fault-injection allocator can exercise every error branch.

## Allocator Contract

Storage implementation code must not call `malloc`, `calloc`, `realloc`, or
`free` directly. It must use a pouch allocator:

```c
typedef struct lc_pouch_allocator {
  void *(*malloc_fn)(void *context, size_t size);
  void *(*calloc_fn)(void *context, size_t count, size_t size);
  void *(*realloc_fn)(void *context, void *ptr, size_t size);
  void (*free_fn)(void *context, void *ptr);
  void *context;
} lc_pouch_allocator;
```

`lc_pouch_allocator` can be derived from `lc_allocator` or
`lc_engine_allocator`, but it should be explicit at the pouch boundary. This
keeps allocation ownership clear and allows storage tests to fail on accidental
platform allocation.

Required allocator-backed structures:

- growable byte buffers
- path buffers
- decoded metadata records
- sorted key arrays
- hash/index tables
- read-file cache nodes
- lock-file cache nodes
- pending commit records
- compaction capture records
- benchmark instrumentation allocations

The disk-log backend should also provide fixed-size pools or slab allocators for
hot objects:

- record references
- pending commit entries
- short path/key copies
- inline record buffers
- metadata encode/decode buffers
- scanner result rows

Large payload bytes should stream through caller-provided `lc_source` and
`lc_sink` abstractions or bounded copy buffers. Full payload materialization is
allowed only for explicitly bounded inline records.

## Disk Layout

The store root is the decoded path from `pouch:///path/to/root`.

Suggested layout:

```text
root/
  .pouch/
    format
    backend-id
    exclusive-writers/
  .lockd/
    backend-id
  <namespace>/
    logstore/
      manifest/
        manifest.log
      markers/
        writer-<id>.marker
      segments/
        seg-<writer-id>-00000001.log
        seg-<writer-id>-00000002.log
      snapshots/
        snap-<writer-id>-<id>.log
      queue-notify/
        <queue>.notify
    locks/
      <escaped-key>.lock
```

Namespaces and keys must be normalized. Empty namespaces and empty keys are
invalid. Keys should be cleaned to prevent absolute paths, parent traversal, and
ambiguous separators. Path components used for lock files must be URL/path
escaped or otherwise encoded so key bytes cannot affect the directory layout.

Reserved internal namespaces must be explicit. At minimum, transaction decision
records need a reserved transaction namespace and backend identity needs a
reserved backend namespace. Public acquire/update/query paths must reject user
requests that target those namespaces.

The backend identity should be persisted as an object with create-if-absent CAS.
If two processes initialize the same empty root concurrently, exactly one writes
the backend id and the loser rereads it. When a stable path-derived descriptor is
available, the identity may use its hash as the deterministic fallback.

## Log Record Format

Records are append-only. Each record contains:

- fixed header with magic, version, record type, key length, metadata length,
  payload length, and payload CRC
- normalized key bytes
- compact binary metadata
- optional payload bytes

Initial record types:

- metadata put
- metadata delete
- state put
- state delete
- object put
- object delete
- state link, used when compaction needs a live record to reference payload
  bytes still protected in another segment

The record header should stay compact: a 24-byte little-endian header with a
magic, version, type, lengths, and CRC is sufficient for v1. The C
implementation must define its own constants in the local pouch code and
document them beside the encoder/decoder tests.

Metadata records need generation numbers. Index replay should accept a record
only when its generation is greater than or equal to the current indexed record
for that key. This makes replay idempotent and keeps stale records from
resurrecting older state.

State and object ETags should be content hashes of the stored plaintext where
that is the lockd-visible behavior. Metadata ETags can be generated opaque
identifiers. The implementation must preserve lockd CAS semantics:

- `expected_etag` mismatch returns CAS mismatch
- create-without-existing fails when current metadata exists
- if-not-exists fails when current state/object exists
- delete with an expected ETag fails on mismatch

Replay must be conservative:

- a short read at the end of a segment is a trailing partial record and stops
  replay for that segment;
- a header magic or version mismatch stops replay at that point;
- a payload CRC mismatch stops replay at that point;
- no record after the first invalid/truncated record may be applied;
- already-applied records must remain valid when a later record is corrupt;
- unread offsets advance only after a whole record has been validated and
  applied.

The reader must support linked payload records. A state-link record points at a
payload span in another segment or snapshot. Link targets are live dependencies
and must be protected from deletion until no current record references them.

## Metadata Model

The metadata document is the lockd coordination record. It must preserve:

- active lease id, owner, expiry time, fencing token, transaction id, and
  whether the transaction was explicitly requested;
- version and published version;
- state ETag, state descriptor, plaintext byte count, and update timestamp;
- metadata attributes, including the query-hidden attribute;
- attachment metadata;
- staged transaction overlay fields: staged transaction id, staged version,
  staged state ETag, staged descriptor, staged plaintext bytes, staged remove
  flag, staged attributes, staged attachments, staged attachment deletes, and
  staged attachment clear.

The query summary path must decode only the fields needed by query planning and
filtering: version, published version, state ETag, descriptor, plaintext byte
count, and query-hidden status. Full metadata decoding should be avoided in
large scans unless a query actually needs it.

Remove semantics are part of the visible lockd contract:

- removing an existing state deletes the state head and advances the lockd
  version;
- removing a key with no state returns removed=false and version 0;
- keepalive after remove must still work for the active lease;
- stale update/remove CAS after remove must fail with a version or ETag
  conflict;
- reacquire after remove sees empty state at the new version.

## Indexes

Each opened namespace maintains in-memory indexes:

- metadata key to latest metadata record reference
- state key to latest state record reference
- object key to latest object record reference
- sorted metadata key list
- sorted object key list
- cached decoded metadata summary for query hot paths

Indexes are projections. They must be rebuildable from manifest, snapshots, and
segments. No index file is authoritative in v1.

LQL integration should consume a storage summary scan API. Until `liblql` is
available, pouch can expose a narrow internal predicate/query boundary and a
full ordered scan. The persistent format should not encode LQL-specific query
plans.

## Refresh and Shared Filesystems

Correctness must not require `fsnotify`. A store instance observes other writers
by refreshing namespace state before reads and writes.

Refresh should:

1. Ensure namespace directories exist.
2. Read manifest changes since the last manifest offset.
3. Scan snapshots and segments for new files.
4. Replay unread committed records in deterministic order.
5. Track writer marker snapshots to skip unnecessary scans.

Writer markers are small files touched after commit. They are an optimization:
if no marker changed, a reader can often skip segment scanning. The marker
mechanism must tolerate filesystems with coarse mtimes by also considering file
size or periodic full scans.

Single-writer mode may skip marker checks for the owning process, but shared
roots must default to safe refresh behavior.

Refresh must also handle history written by older store versions or interrupted
processes:

- sealed segment files without manifest entries are still valid history;
- manifests that contain only segment-open entries must be upgraded by marking
  closed historical segments sealed;
- after a crash-style restart, the latest open tail segment must remain outside
  compaction snapshots while sealed history can still compact;
- snapshot plus tail replay must prefer newer tail records over older snapshot
  records;
- a forced refresh is required after metadata or payload decode failure before
  returning the error, because another writer may have committed a newer record.

Read paths should cache open segment files with a bounded LRU, but must resolve
linked payload spans against the current segment or snapshot path. Compaction
cleanup must not close or delete files still needed by active readers.

## Locking

The disk backend is single-writer per key mutation, not globally single-process.
Writers must coordinate with:

- an in-process striped mutex keyed by namespace and key
- a process-wide global striped mutex to serialize multiple store handles in one
  process
- an advisory file lock per namespace/key for cross-process and shared
  filesystem coordination

Multi-key operations, especially staged promotion, must sort and deduplicate
keys before acquiring locks. If two keys map to the same lock stripe, the stripe
must be acquired once. Unlocking happens in reverse acquisition order. This is a
hard deadlock-avoidance invariant.

On POSIX systems, prefer `fcntl` byte-range locks over BSD `flock` for NFSv4
portability. The lock implementation should be isolated behind a small platform
file. If a platform cannot provide usable advisory locks, pouch must fail
opening a shared-write disk backend unless explicitly configured for unsafe
single-process test mode.

Lock-file descriptors should be cached with an LRU to avoid open/close overhead
on hot keys. The cache must be bounded and allocator-backed.

Exclusive writer presence is advisory crash detection, separate from per-key
locks. A process in single-writer mode should write a heartbeat payload and
touch a presence marker. Probes should prefer the heartbeat payload over file
mtime and fall back to mtime for legacy markers. Abrupt abort must stop
background work without removing the marker so peers can observe the stale
writer until its TTL expires.

## Commit and Fsync

Durability must be explicit. The default should be safe: a successful mutation
has reached the log and passed the configured commit policy.

The disk-log backend should support:

- immediate fsync
- batched fsync with a small delay and maximum operation count
- no-sync mode only for tests or explicitly accepted performance tradeoffs

The append path should distinguish:

- inline records for small payloads
- streaming records for large payloads
- commit groups so multiple records can share one fsync result

For inline records, the backend can encode the complete record into a bounded
buffer and batch contiguous appends into one write. For large payloads, it should
write the prefix, stream the payload while computing CRC and content hash, then
rewrite the finalized prefix with final lengths, CRC, and ETag metadata.

Pending records must not become visible in indexes until their commit group has
succeeded. Failed fsync must fail all pending records in that group and leave
indexes unchanged.

Pending visibility has several invariants:

- pending maps are tracked separately for metadata, state, and object records;
- a pending map stores the latest pending record for each key;
- reads and CAS writes outside the same commit group must wait for the pending
  record to commit or fail;
- reads and writes inside the same commit group may observe their own pending
  record;
- commit groups may contain multiple records and multiple keys;
- pending records apply in append order only after their group is committed;
- a later committed group must not let earlier uncommitted records become
  visible accidentally;
- failed groups must clear pending maps and notify waiters without changing
  indexes.

No-sync writes still create pending records and update indexes only after their
commit group is marked complete. They advance a no-sync epoch so an active
segment is not sealed before the required later sync boundary has caught up.

## Segment Lifecycle

Segments are append files. An active segment is sealed when:

- configured segment size is reached,
- no writes are in flight,
- no pending commits depend on it,
- any required fsync epoch has completed.

The manifest records segment lifecycle:

- segment opened
- segment sealed
- snapshot installed
- segment obsolete
- snapshot obsolete

Manifest append operations must be protected by an advisory lock. Manifest
parsing must be tolerant of trailing partial lines and unknown/incomplete stale
entries where safe.

Segment names must contain a writer id and monotonically increasing per-writer
sequence so records from multiple writers sort deterministically. Snapshot names
must be unique and must not collide with segment names.

## Compaction

Compaction creates a snapshot segment containing the current live records from
candidate sealed segments and the installed prior snapshot. It never mutates
existing segment contents.

Compaction flow:

1. Refresh namespace indexes.
2. Select sealed, non-active, non-obsolete candidates.
3. Exclude candidates protected by live state-link references.
4. Check thresholds: minimum candidate count and minimum reclaimable bytes.
5. Capture current live metadata/state/object record refs.
6. Build a temporary snapshot with compacted live records.
7. Fsync the snapshot.
8. Re-lock namespace state and validate captured refs still match current
   indexes.
9. Rename the snapshot into place.
10. Append manifest entries installing the snapshot and marking old segments or
    snapshots obsolete.
11. Delete obsolete files after a grace period.

If validation detects drift, compaction must abandon the temporary snapshot and
leave the live store unchanged.

The compactor should support an I/O bytes-per-second limit so it does not
destroy latency for foreground client work.

Additional compaction invariants:

- compaction disabled must still perform obsolete-file cleanup when requested;
- below-threshold compaction must log/return a skip reason without changing
  state;
- build failure leaves indexes, manifest, and installed snapshot unchanged;
- manifest install failure removes the just-built snapshot and leaves indexes
  unchanged;
- cleanup failure must keep obsolete entries tracked so a later pass can retry;
- a protected snapshot that is the target of a live link must not be marked
  obsolete;
- a second successful snapshot generation should obsolete the prior snapshot
  only when no live link still targets it;
- foreground reads must continue successfully while background compaction is
  copying payloads.

## Staging and Transactions

Pouch must implement transactional staging as a storage primitive, not as a
client-side convention.

Staged state keys use a per-key staging suffix:

```text
<key>/.staging/<txn-id>
.staging/<txn-id>
```

The root-level form is used when the logical key is empty or generated later.
Listing staged state must include only direct staged state entries and exclude
nested staged attachment objects.

Promotion must be atomic with respect to readers and CAS writers:

- lock both the committed key and staged key using sorted multi-key locking;
- enforce the expected committed-head ETag when provided;
- if no expected head ETag is provided, promotion behaves like if-not-exists for
  the committed head;
- wait for pending committed-head or staged records from other commit groups;
- fail if the staged record is missing or deleted;
- append a state-link record for the committed key that references the staged
  payload span, preserving ETag, descriptor, plaintext bytes, and cipher bytes;
- append a staged-state delete record so the staging key disappears after
  promotion.

Transaction decision records live in the reserved transaction namespace as
objects. Recovery must support:

- commit records that promote staged state for every participant;
- rollback records that discard staged state and clear staged metadata;
- expired pending records that roll back;
- participants in different namespaces;
- explicit replay by transaction id;
- replay after restart;
- cleanup of decided transaction records after replay completes.

## Queue and Consumer Support

Queues should use the object plane plus metadata conventions, but the storage
backend must still expose operations needed by the client adapter:

- enqueue message payload and metadata
- list/dequeue visible messages in deterministic order
- claim visibility lease
- ack
- nack
- extend
- expose cursor/next-cursor behavior expected by existing client calls
- support polling consumer loops without filesystem notification
- support stateful subscribe where message and state handles preserve the same
  correlation id
- support transaction commit and rollback for queued messages and queue-coupled
  state changes
- support restart/replay wakeups so transaction decisions made on one process
  are observed by another process
- tolerate multi-consumer contention and high fan-in/fan-out without duplicate
  acked delivery

Queue change notification files may be touched after enqueue/nack/visibility
changes as a future optimization. Correctness must come from polling and log
refresh.

The consumer service should be able to run against pouch without special public
API changes. Internally, its wait loop should use the pouch queue polling
strategy when the client backend is pouch.

Queue polling has a latency/CPU contract. Idle enqueue must wake dispatch
without requiring a continuous tight polling loop. Poll interval, jitter, and a
slower resilient polling interval should be tunable. Filesystem notification, if
enabled and supported, is only a wake optimization and must be disabled on
filesystems where it is unreliable.

## Attachments

Attachments should map to the object plane. Attachment metadata lives in the
lockd metadata document, including staged attachment updates when transactions
are used. Attachment payloads are immutable object records addressed by stable
object keys. Deletes are log records.

The backend must support:

- put attachment with content type and descriptor
- get attachment as a streaming `lc_source`
- list attachments from metadata
- delete attachment
- staged attachment puts/deletes/clear for transaction release or rollback
- public attachment listing and retrieval after lease release
- object copy that preserves content type and descriptor

Attachment and object metadata must preserve content type, descriptor,
plaintext-size hints, ETag, size, and modified time. Queue payload objects use
the same object plane and therefore share these invariants.

## Retention and Cleanup

The disk backend should support an optional retention sweep. The sweep scans
metadata, decodes records, and removes metadata plus state when `UpdatedAtUnix`
is older than the configured retention. Sweep failures for individual keys
should not abort the whole pass. The logstore directories themselves remain; the
deletion is represented by append-log records and later reclaimed by compaction.

Cleanup tasks must be restartable and idempotent:

- staged-state cleanup may be repeated after crash;
- obsolete segment/snapshot cleanup may be repeated after delete failure;
- transaction record cleanup may be repeated after replay;
- backend-id creation may race and recover through CAS reread.

## Crypto and Descriptors

Pouch may initially run without storage encryption, but the data model must
preserve descriptor fields and plaintext byte counts. If encryption is added or
shared with a future server stack:

- metadata records may be encrypted as a whole;
- state writes mint or reuse state descriptors;
- state reads can accept descriptor and plaintext-size hints from metadata;
- staged promotion must not re-encrypt payloads unnecessarily; linking the
  staged payload preserves descriptor and byte metadata;
- object payloads may carry descriptors and plaintext-size hints.

## ABI Plan

Pouch will require a public ABI bump only when the final public surface is
introduced. To avoid multiple bumps:

- Keep the first implementation under private headers and internal symbols.
- Do not expose experimental pouch structs in `include/lc/lc.h` until the
  endpoint behavior and any public config fields are settled.
- Add public config only once, likely endpoint-compatible with
  `config.endpoints = { "pouch:///..." }`.
- If diagnostics are needed publicly, group them into one stable stats API
  rather than adding fields piecemeal.

Internal symbols may churn during implementation. Public exported symbols must
not.

## Benchmarks

Benchmarks are required alongside implementation. Initial benchmark targets:

- open existing store and rebuild indexes from segments
- acquire/update/release on one hot key
- acquire/update/release across many keys
- write state payloads: 1 KiB, 64 KiB, 1 MiB, 16 MiB
- read state payloads: 1 KiB, 64 KiB, 1 MiB, 16 MiB
- metadata summary scan over 1k, 100k, and 1M keys where feasible
- enqueue/dequeue/ack throughput
- consumer polling latency at idle and under load
- attachment put/get/delete throughput
- compaction throughput and foreground write latency during compaction
- cross-process CAS contention on the same root
- NFS-like mode with marker polling and advisory locks enabled
- allocator counts, peak bytes, pool hit ratio, and allocation failures

Benchmarks should report:

- operations per second
- p50/p95/p99 latency where practical
- bytes written and read
- fsync batch size distribution
- compaction reclaim bytes
- open file cache hit/miss counts
- allocation count and peak outstanding bytes

## Verification

Unit tests:

- record encode/decode round trips
- truncated/corrupt record rejection
- payload CRC mismatch handling
- metadata generation replay ordering
- CAS success/failure
- if-not-exists behavior
- key normalization and lock path escaping
- manifest parsing and append
- marker snapshot change detection
- sorted scan pagination
- allocator failure paths

Integration tests:

- crash after append before fsync does not expose uncommitted records
- crash after fsync rebuilds indexes correctly
- two store handles contend on the same key
- two processes contend on the same root
- compaction preserves live metadata/state/object records
- compaction abandoned on validation drift
- obsolete files deleted only after grace period
- queue dequeue visibility and ack/nack/extend semantics
- consumer service runs against `pouch://`
- attachments survive reopen and compaction
- staged promotion preserves small and large payloads
- staged promotion cleans the staging key after commit
- multi-key staging locks do not deadlock on lock-stripe collisions
- transaction commit/rollback replays after restart and across namespaces
- expired pending transactions roll back
- query-hidden metadata is excluded from scans
- query pagination, namespace isolation, public-read results, and streamed
  document responses work against disk summaries
- remove semantics: empty remove, remove version bump, keepalive after remove,
  stale remove/update CAS failures, remove then recreate
- queue polling: ack removal, nack redelivery, visibility timeout handoff,
  multi-consumer contention, failover dequeue/ack, subscribe with state, and
  start-consumer auto-ack/state-save/failure paths
- exclusive writer marker: close removes marker, abort preserves marker until
  TTL expiry, heartbeat payload overrides misleading future mtimes

The storage tests should use fault-injection allocators and fault-injection file
operations where practical. Correctness should be demonstrated by reopening a
fresh store and rebuilding indexes from disk, not only by observing in-memory
state after writes.

## Implementation Order

1. Private pouch allocator, buffers, error helpers, and test allocator.
2. Backend interface and no-op/fake backend used by tests.
3. Disk layout, key normalization, file locking, and lock cache.
4. Record encoder/decoder with tests and fuzz target.
5. Namespace open/refresh/replay and in-memory indexes.
6. Metadata and state operations with CAS and fsync.
7. Object operations for attachments and queues.
8. Segment sealing, manifest, and reopen recovery tests.
9. Compaction snapshots and cleanup.
10. Queue semantics and consumer-service adapter.
11. LQL scan/query integration boundary, then `liblql` integration when ready.
12. `pouch://` endpoint selection in the client engine.
13. Benchmarks, diagnostics, packaging, and final ABI bump.

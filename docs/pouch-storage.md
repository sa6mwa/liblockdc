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

## Source Design Reading

The server-side disk backend was designed as a storage contract first and a
filesystem implementation second. The important shape to preserve is not the
implementation language or goroutine structure; it is the separation between:

- a backend contract for metadata, state, objects, staging, namespace listing,
  queue wake capabilities, backend identity, close, and crash-style abort;
- optional capabilities such as metadata-summary scans, queue change feeds,
  single-writer controls, concurrent-write reporting, exclusive-writer probing,
  query backend mode, and indexer flush defaults;
- a disk-log namespace runtime that owns segment replay, marker refresh,
  pending commit visibility, open-file caches, and compaction;
- higher lockd semantics implemented above the backend by composing metadata,
  state, object, staging, transaction, queue, and query primitives.

Pouch should mimic that architecture, not the exact call graph. In C this means
explicit vtables, explicit ownership, explicit allocator plumbing, and no hidden
runtime scheduler assumptions. Any background work must have a synchronous test
entry point so restart and crash windows can be verified without timing games.

The disk implementation is also shaped by tests that exercise the storage
backend through the full lockd API. Those tests are important because they make
storage-visible invariants out of behavior that may look like server logic:
queue consumers, start-consumer, transactional replay, query refresh, HA
promotion, remove semantics, attachment persistence, and crash recovery all
depend on storage ordering. Pouch can defer management and permissions, but it
cannot defer these storage invariants if it is to be a lockd-compatible client
backend.

The key lesson from the server disk backend is that it is not a directory store
with a log bolted on. It is a log-indexed coordination substrate. The append log
is the durability boundary, in-memory maps are only rebuilt projections, marker
files are invalidation hints, transaction records are durable decisions, and
query/queue wakeups are derived from committed storage state. Pouch must be
specified and tested at that level, because otherwise it will pass simple
get/update cases while failing the workflows that depend on lockd's ordering
model.

Additional parity details from the storage contract:

- Backend identity is a storage primitive. A backend hash must be stable for a
  store root, survive reopen, tolerate create races, and preserve the identity
  value if a later encrypted rewrite is needed.
- Namespace listing, when supported, returns sorted namespace roots and filters
  non-directories. Reserved internal namespaces remain usable by storage
  recovery but unavailable to public lock operations.
- The disk-log backend must report that it does not support unrestricted
  concurrent writers. Per-key locks make same-root mutation safe, but the store
  is still append-serialized storage, not a consensus or multi-primary
  database.
- `Abort` is not `Close`: it intentionally leaves crash-detectable writer
  presence behind while stopping in-process background loops.
- Metadata summary loading and scanning are optional fast paths, but the
  fallback contract matters: scans are lexical, support `start_after` and
  `limit`, and may skip rows that disappear or become transient while scanning.

## Disk Backend Deep-Dive Addendum

The server disk implementation has several subtle correctness boundaries that
must be treated as pouch design requirements rather than implementation
accidents.

Storage contract boundaries:

- The backend interface is lower level than the public lockd API. It stores
  metadata documents, state blobs, arbitrary objects, staged state, namespace
  lists, backend identity, and optional capabilities. Leases, queues,
  transactions, query summaries, and attachments are built by composing those
  primitives.
- Metadata is the coordination document. State and object records carry payload
  heads, but public versioning, lease ownership, staged overlays, attachment
  lists, and query-hidden state live in metadata. Any implementation that treats
  a state payload record as the whole object will get CAS, remove, query, and
  attachment behavior wrong.
- Object storage is not merely attachment storage. Queue payloads, transaction
  decision records, staged attachment payloads, backend identity, and future
  internal objects all share the object plane and therefore share object CAS,
  ETag, listing, copying, and compaction semantics.
- Optional fast paths must preserve fallback semantics. Metadata summary scans
  are optimized, but the fallback is still sorted metadata-key listing plus
  summary loading with the same pagination and not-found behavior.

Record and replay boundaries:

- Replay validation is strictly linear. After a short read, bad header, bad
  version, bad CRC, truncated type-specific metadata, or malformed state-link,
  the segment tail is considered invalid and no later bytes are examined.
- Snapshot replay is not a separate database. It is the first segment in a
  deterministic replay order, followed by non-obsolete segment tails. Newer tail
  records can override snapshot records through generation comparison.
- Installed snapshots and obsolete sets change replay history. When manifest
  changes install a snapshot or mark files obsolete, the in-memory replay
  projection must reset and rebuild from the new ordered history.
- The manifest is an accelerator and lifecycle journal. It is not the sole
  authority for valid committed records: segment and snapshot directory scans
  repair missing, legacy, or crash-incomplete manifest state.
- Legacy open-only manifests need a repair path. Historical segments are
  backfilled as sealed while the current open tail remains active, otherwise
  old data becomes a permanent uncompacted tail.

Commit and visibility boundaries:

- Pending records are visible only to their own commit group. Readers and CAS
  writers outside that group wait for the group outcome, then re-read current
  indexes before deciding.
- Applying pending records is independent from advancing replay offsets. A
  later group may become visible before an earlier group, but the segment
  `read_offset` advances only across the contiguous applied prefix. This avoids
  rereading an uncommitted gap as durable history after a later refresh.
- Failed append or fsync must wake waiters and clear the matching pending maps.
  Waiters must see the error rather than a half-visible record or a permanent
  block.
- `no_sync` still participates in pending visibility and segment lifecycle. It
  can mark a group logically complete, but an active segment cannot be sealed
  before a later sync boundary covers prior no-sync epochs.
- Marker touch is after durable commit and is only a wake/invalidation hint.
  A failed marker touch cannot roll back the committed write, and a missing
  marker cannot hide data from forced refresh or segment scans.

Shared-root boundaries:

- Same-key mutation is protected by three layers: per-store striped mutex,
  process-global striped mutex for multiple handles in one process, and
  advisory per-key file lock for other processes or hosts.
- Single-writer mode is an optimization mode, not a different correctness
  model. It skips per-key file locks for the owner but publishes exclusive
  writer presence so peers can fence, wait, or detect stale writers.
- `Close` and `Abort` intentionally differ. Close removes live writer presence;
  abort stops background work without removing crash-detectable presence, so HA
  and single-writer tests can observe stale ownership until TTL expiry.
- Writer presence probes prefer the heartbeat payload over file mtime and fall
  back to mtime for legacy markers. This avoids false positives from
  misleading future mtimes.
- Filesystem notification is never a durability or visibility requirement.
  Polling and refresh are the baseline, including on NFS-like filesystems.

Compaction boundaries:

- Compaction captures live refs under lock, copies payloads outside the lock,
  then revalidates that every captured ref is still current before installing a
  snapshot. Validation drift means the temporary snapshot is abandoned.
- Active segments, in-flight writes, pending commits, and no-sync epochs keep a
  segment out of sealing and compaction.
- State-link records preserve staged promotion without copying payloads in the
  foreground. Compaction may rewrite a live link into a normal state-put record
  only when the payload source is being compacted safely.
- Any current state-link target outside the compaction result protects its
  source segment or snapshot from obsolete cleanup.
- Cleanup is retryable. Delete failures keep obsolete entries in memory and in
  manifest-derived state so later passes can remove them.

Integration-derived compatibility boundaries:

- Remove is not a blind delete. Existing removes advance version and clear
  state; empty removes report no removal; keepalive can still succeed after
  remove while stale update/remove CAS fails; reacquire sees empty state at the
  new version.
- Queue dequeue is a lease/CAS protocol over storage. Only one contender may
  claim a visible message, delayed nack hides until the delay expires, visibility
  timeout handoff fences stale acks, retry exhaustion remains terminal after
  reopen, and observability calls must not mutate queue state.
- Start-consumer is storage-facing because handler success, explicit ack/nack,
  handler failure, auto-ack, and state-save all persist through the same queue
  and state primitives.
- Query is storage-facing because flush-wait, refresh-wait, pagination,
  namespace isolation, public-read results, streaming documents, and index
  rebuild all depend on ordered summary visibility.
- Transaction replay is storage-facing because commit and rollback records are
  durable objects. Replay after restart must promote or discard staged state,
  clean decision records, and wake query/queue observers without relying on an
  in-memory transaction manager.

These addendum points are the minimum parity checklist for the segmented pouch
backend. The implementation must preserve the same observable boundaries:
linear replay, pending-before-visible, forced refresh on uncertainty, streaming
payload copy, conservative compaction, and queue/query visibility through
committed storage records.

## Design Invariants From The Existing Disk Store

The existing disk store is best understood as a durable projection system:
segments and snapshots are the source of truth; in-memory indexes are rebuilt
views; marker files are invalidation hints; file locks serialize mutation; and
commit groups decide when appended records become visible.

Required invariants:

- Every visible metadata, state, or object head is represented by a record ref
  containing record type, key, generation, ETag, modified time, payload span,
  and optional state-link target.
- Replay is append-order within a deterministic segment order: installed
  snapshot first, then non-obsolete segment names lexically. Tail records must
  override older snapshot records through generation comparison.
- Indexes accept a record only when its generation is greater than or equal to
  the current generation for that key. Delete records remove the key when their
  generation wins.
- Sorted metadata and object key arrays are part of the hot path. Insert/delete
  must maintain lexical order without deriving order from filesystem traversal.
- Pending append records are not visible until their commit group succeeds.
  Pending maps are separate from committed indexes and are tracked per
  metadata/state/object key.
- Pending maps hold the latest pending record for a key, but the pending queue
  preserves append order. When one commit group completes before an earlier
  group, records from the completed group may become visible, while segment
  replay offsets may advance only across the contiguous committed/applied
  prefix. This prevents later refreshes from rereading an uncommitted gap as
  durable history.
- A reader or CAS writer that encounters another commit group's pending record
  waits for that group, refreshes, and then re-evaluates. It must not observe
  the half-committed value.
- A reader or writer in the same commit group may observe its own pending
  records. This is required for multi-record logical operations.
- Group commit is a correctness boundary, not only a performance optimization:
  all records in the group share one durability result, and finalizers run only
  after durable commit succeeds.
- The append path has two modes: bounded inline records for small payloads and
  streaming records for large payloads. Inline records can be batched into one
  contiguous write. Streaming records must not materialize the whole payload.
- Large streaming records write a provisional prefix, stream payload bytes while
  computing CRC and content hash, then rewrite the prefix with final lengths,
  CRC, and ETag. A crash before the prefix rewrite must leave a trailing invalid
  record that replay ignores.
- Metadata ETags are opaque UUID-style tokens. State and object ETags are
  content-derived SHA-256 hex strings over the plaintext bytes used for lockd
  CAS and attachment/object validation. Replaying a record must not regenerate a
  different ETag.
- Segment `read_offset` advances only after a complete record has passed
  header, metadata, payload, and CRC validation. Any malformed or partial record
  stops replay for that segment.
- Replay treats corrupt tails as an end-of-valid-history condition, not as an
  opportunity to skip forward. This applies equally to short headers, short
  keys, short metadata, short state-link payloads, short state/object payloads,
  bad magic, unsupported versions, bad CRCs, unknown record types, and
  truncated type-specific metadata.
- State-link records are first-class state heads. They are used for staged
  promotion and can also arise during compaction/replay. Reads must resolve the
  linked payload span before opening the payload reader.
- Any segment or snapshot that is the target of a live state-link is protected
  from obsolete cleanup, even if it otherwise looks compactable.
- Compaction installs a new snapshot only after validating that all captured
  refs still match the current live indexes. Validation drift abandons the
  snapshot and leaves indexes, manifest, and obsolete sets unchanged.
- Manifest state accelerates lifecycle tracking but is not authoritative for
  payload correctness. Missing or legacy manifest information must be repaired
  by scanning segment and snapshot directories.
- Writer markers are optimization hints. A missing marker update must not hide
  committed data because forced refresh and segment scans remain authoritative.
- Single-writer mode can skip peer-marker scans for the owning process, but it
  must publish an exclusive-writer heartbeat so other processes can fence or
  wait safely.
- Shared-root safety is a layered contract: process-local stripe locks avoid
  in-process races, global process stripe locks avoid two handles in one process
  racing each other, advisory per-key file locks coordinate other processes or
  hosts, and refresh/marker scans make other committed records visible after
  locks are released.
- `Close` performs graceful cleanup of background work and writer presence.
  `Abort` simulates process loss: it stops background work without removing
  crash-detectable writer presence.

These invariants should be documented beside C tests as they land. They are the
reason pouch needs a real disk-log implementation instead of a simple directory
of rewritten JSON files.

## Goals

- Provide lockd-compatible local storage for state, leases, attachments, queues,
  and consumer-service workflows.
- Support multiple independent client instances pointed at the same store root,
  including roots shared over NFSv4, CephFS, or similar filesystems that support
  advisory byte-range locks.
- Keep the backend single-writer at the mutation level while allowing readers to
  refresh their indexes from committed log records.
- Make indexed query support a first-class v1 storage requirement. `liblql`
  supplies the query language/parser/evaluator layer, but pouch must maintain
  storage-owned indexes and expose indexed scan primitives before `liblql` is
  ready.
- Avoid hidden memory allocation. Storage code must allocate only through a
  pouch allocator interface.
- Add benchmarks and diagnostics from the start so write latency, read latency,
  compaction cost, allocator behavior, and index rebuild cost are measurable.
- Bump the public ABI only once during the pouch feature release.

## Non-Goals

- Pouch v1 does not implement management APIs.
- Public client surfaces that pouch does not yet implement, including public
  LQL query calls before `liblql`, namespace/index mutation management, and TC
  cluster/resource-manager calls, must return deterministic local unsupported
  errors. Pouch may report the locally configured query engine defaults through
  namespace configuration reads. Public transaction prepare/commit/rollback and
  explicit replay are local pouch operations backed by durable decision objects;
  open-time recovery scans durable decision objects directly and also scans
  pending transactional metadata as a compatibility fallback before replaying
  matching decisions. A `pouch://` client must never fall through to HTTP
  transport for an unimplemented server-side surface. Internal indexed metadata
  scans are not optional: they are part of the storage engine even before the
  public LQL surface is enabled.
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
  int (*scan_meta_keys)(lc_pouch_store *self,
                        const lc_pouch_scan_meta_req *req,
                        lc_pouch_query_index_key_visit_fn visit,
                        void *visit_ctx, lc_pouch_scan_meta_res *out,
                        lc_error *error);

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

  int (*compact)(lc_pouch_store *self, const char *mode,
                 lc_pouch_compaction_res *out, lc_error *error);
  int (*backend_hash)(lc_pouch_store *self, char **out, lc_error *error);
  int (*close)(lc_pouch_store *self, lc_error *error);
  int (*abort)(lc_pouch_store *self, lc_error *error);
};
```

The interface intentionally separates metadata, state blobs, and arbitrary
objects. Queue messages and attachments can use the object plane while lease and
state coordination use the metadata/state plane. Query hot paths need
storage-owned summary and posting scans so LQL, or the temporary pre-LQL query
adapter, can avoid loading full metadata and payloads for every key.

The interface should also expose optional capability functions or flags:

- namespace listing
- queue change polling hints
- exclusive writer probing
- single-writer optimization mode
- whether the backend is safe for concurrent writers to the same root
- fsync statistics; the current private disk hook reports attempted and failed
  fsync calls plus successful log, query-index, root-directory,
  writer-marker, and queue-wake marker fsync counts
- compaction statistics and explicit compaction trigger; the current private
  disk hook supports `force` and threshold-gated `if_needed`, returning
  before/after log and query-index byte counts, before/after record counts,
  live-record count, and a skip reason such as `below-min-log-size` or
  `below-obsolete-threshold`
- index flush/default tuning for the storage indexer and later LQL integration
- query backend mode/defaults: indexed is preferred, full log-backed ordered
  scan is supported when explicitly configured, and fallback policy is explicit
- retention/janitor sweep for expired metadata and state; the current private
  disk hook accepts an `updated_before_unix` cutoff and reports scanned,
  expired, deleted metadata, deleted state, and failed-key counts

The disk implementation should report that it is not a general concurrent
writer backend, even though it safely serializes same-root mutations with
per-key advisory locks. That distinction matters for HA and embedded pouch:
multiple client instances can coordinate through the same store, but the log is
still append-serialized by key-level critical sections and commit ordering, not
by a multi-writer database protocol.

Current disk backend milestone: the private backend capability hook reports a
`disk-log` backend, advisory file-lock write coordination, same-root writer
serialization support, crash-abort marker support, backend identity support,
and explicitly reports that it is not a general concurrent-writer database.
The private storage vtable also exposes namespace listing over the disk
backend's live projections; it reports unique sorted namespaces with active
state, metadata, object, or queue entries after an authoritative replay refresh.
Writer-presence diagnostics report advisory file-lock marker mode, this
handle's heartbeat sequence, whether its marker is present, how many active peer
markers are currently visible in the store root, and how many markers are stale.
New marker files carry an `updated_at_unix` heartbeat; legacy marker files
without that field fall back to mtime.
Lock diagnostics report the current `key-striped-fcntl` mode, lock path,
whether the implementation still uses the root-level append writer lock, whether
per-key lock caching/striping is active, and counters for lock acquisitions,
releases, replay refreshes, and log reopens. The private disk vtable also
exposes the lock-key path normalizer and a nonblocking per-key advisory lock
primitive: lock paths live under `locks/<namespace>/<key>`, with key bytes
percent-escaped so slash-separated user keys do not become filesystem path
components. The current per-key primitive creates those lock files, acquires
per-store and process-wide striped mutexes before touching the lock file, uses
`fcntl` byte-range locks for cross-process contention, and tracks process-local
held locks so two store handles in the same process contend before filesystem
locking. Released key-lock descriptors are retained in a bounded process-wide
LRU cache with at most one cached descriptor for a lock path, so hot keys avoid
open/close churn without violating classic `fcntl` lock lifetime rules. Every
cached descriptor is unlocked before reuse and closed when evicted or when a
store closes. State
write/remove, metadata store/delete, and single-key object
put/delete paths acquire the per-key guard before the global append-log lock.
Multi-key object copy acquires source and destination key guards in
lexicographic order before the global append-log lock. Queue enqueue, dequeue,
ack, nack, and extend acquire the queue-name key guard before the global
append-log lock. Lock diagnostics now report stripe count, active process-held
locks, same-process contention counts, and the process-wide lock descriptor
cache exposes hits, misses, evictions, closes, current size, and capacity so hot
key contention and descriptor reuse are observable.
Read diagnostics expose a bounded process-wide read-file descriptor cache for
segmented payload files. State reads, object reads, and queue dequeue payload
reads borrow an idle descriptor for the recorded body path when possible, seek
it to the live payload span, and return it to the cache when the `lc_source`
closes. Active read sources keep their own descriptor and are never closed by
cache eviction. Before reusing a cached descriptor, the backend validates that
its device/inode still matches the recorded body path, so compaction or
obsolete-file cleanup cannot route a new reader to unexpected bytes while older
active readers continue to read from their original descriptor. A read source
may outlive the store handle that created it; store close marks the cache owner
closed, and later source close must close its descriptor directly instead of
reinserting it into the cache through freed store state.

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

Current implementation milestone: the C disk backend writes authoritative
records to per-namespace logstore segments and installs compacted live heads as
manifested namespace snapshots. Compaction runs under the writer lock, backs up
the active segment set, streams live payload spans with bounded buffers into
fresh segment files, installs those files as snapshots, and marks superseded
segments or snapshots obsolete in the manifest. The root `store.log` file is a
non-authoritative placeholder and is not replaced during compaction. Monotonic
tokens survive compaction through a private high-water record in the internal
backend namespace logstore.
Idle read descriptors are cached separately from active read sources. The cache
is a performance artifact only: entries are bounded, allocator-backed, reusable
across state/object/queue payload reads, and discarded when their descriptor no
longer names the recorded body file.
Opening a store also removes stale `store.compact.tmp`, internal-logstore
`query.index.compact.tmp`, and earlier root-level query-index temp files while
holding the writer lock, so crash leftovers from older or interrupted
compaction attempts do not accumulate or confuse later runs.
The private backend control surface now exposes explicit compaction diagnostics:
`force` runs the same live-head rewrite immediately, while `if_needed` applies
the auto-compaction thresholds and returns a concrete skip reason without
rewriting below-threshold stores. Both modes report before/after log bytes,
query-index bytes, record counts, and live-record counts so tests and future
management tooling can treat compaction as observable behavior rather than an
implicit side effect.

Segmented storage alone is not the v1 search-performance shape. A searchable
pouch store must not use full-history scanning as the preferred indexed-query
path. Before public query/LQL support ships, the disk backend must grow
append-friendly index storage: compactable index segments or equivalent
Lucene-style sidecar files with term/range postings, per-field summary columns,
deleted/live filters, and stable key ordering. The authoritative object state
still comes from the log, but the preferred query path should touch index data
first and load full metadata or payload bytes only for candidate rows that
survive the index predicates.

Full log-backed ordered scanning remains a supported backend mode, just not the
preferred default. Pouch configuration must be able to select indexed mode, scan
mode, and fallback policy when a store/client instance is opened. This is an
instance-level choice with the same operational role as the server disk store's
query backend mode: a caller can intentionally run a pouch instance in scan
mode even though indexed mode is the normal production route. The public client
sets this directly on a pouch endpoint with
`pouch:///path?query_engine=scan&query_fallback_engine=index`. The same endpoint
query parser accepts `single_writer=true` for deployments that can promise one
active writer and want the marker-synced refresh fast path. The disk backend
also exposes these choices through `lc_pouch_disk_open_with_options`, where
`query_engine=index` is the default, `query_engine=scan` forces the log-backed
ordered scan route, `query_fallback_engine` is explicit rather than implicit,
and `single_writer` is disabled by default. Scan mode is useful for tiny stores,
diagnostics, index rebuild validation, and early deployments before a particular
index feature exists.

This configuration is part of pouch setup, not just a per-request hint. A pouch
instance opened with scan as the preferred engine must route ordinary match-all
`query` and `query_keys` calls through scan mode when the request does not name
an engine. A request-level `engine` value is still honored as an override, and
fallback is considered only for implicit routing from the configured preferred
engine. That makes scan mode an operationally testable mode in its own right:
operators can deliberately run a store in full log-backed scan mode, compare it
with indexed mode, or keep it available while a new index generation is being
rebuilt.

The scan route must remain a true configured route, not a compatibility shim
inside indexed search. A pouch instance opened in scan mode must be able to
serve match-all key and document queries even if `query.index` is absent,
obsolete, corrupt, or for a future format version. Conversely, indexed mode must
not silently degrade into a full-log scan for ordinary predicate execution,
because that would hide the performance cliff that indexed search is intended
to avoid.

Scan mode is a real full log-backed scan route, not a synonym for the indexed
path with fewer predicates and not a hidden fallback inside indexed search.
Before serving a scan page, the disk backend must refresh from the authoritative
log state, including the equivalent of a forced snapshot/segment/log scan when
marker state, open-file replacement, missing sidecars, corrupt sidecars, future
sidecar versions, or configured refresh intervals make cached projections
uncertain. After that authoritative refresh it may serve the page from the
rebuilt ordered metadata-summary projection, rather than rereading every payload
record for every page. The scan result must preserve stable lexical key
ordering, honor `start_after` and `limit`, skip query-hidden rows, and avoid
payload materialization until a surviving query row needs its document body.
This keeps full log-scan correctness available for pouch instances that request
it while keeping indexed search as the default and preferred performance path.
Fallback policy applies only to configured default routing, not to explicit
per-request engine hints. For example, a store configured with
`query_engine=scan&query_fallback_engine=index` should route `refresh=wait_for`
match-all queries through the indexed path because scan mode does not consult a
durable index and therefore cannot honor refresh hints.

Scan mode acceptance criteria:

- Store-open configuration must select scan mode without requiring every query
  request to set `engine=scan`.
- Endpoint query options must override client defaults for the opened pouch
  instance.
- A scan-mode instance must serve match-all document and key queries from the
  authoritative log projection when the query sidecar is corrupt, obsolete,
  absent, or unopened.
- A long-lived scan-mode reader must see records committed by another client
  instance after forcing a log refresh; correctness must not depend on
  filesystem notifications.
- Explicit request-level `engine=scan` must bypass configured fallback policy.
- Configured fallback may route an implicit scan-preferred request to indexed
  mode only when the requested semantics require index machinery, such as
  `refresh=wait_for`.
- Scan mode must report no index sequence for results served by the scan route;
  indexed mode must report the durable query-index sequence.

The first public query surfaces are `query_keys` and `query` with the match-all
selector `{}`. In indexed mode these calls route through a storage-owned index
scan primitive and return the current `index_seq`. Before `liblql` integration,
indexed mode also accepts exact key selector form `{"key":"..."}`, exact owner
selector form `{"owner":"..."}`, and their exact conjunction
`{"key":"...","owner":"..."}`. Exact keys route through the sorted
query-summary projection and can validate owner as a post-filter; exact owners
route through storage-owned owner postings. Scan mode accepts the same pre-LQL
selector set, but resolves equality by walking ordered full-summary or metadata
scan routes instead of consulting postings. The current disk backend keeps the
sorted metadata projection as the authoritative in-memory index for match-all
and exact-key scans, while
the internal `.lockd` namespace logstore `query.index` remains a durable
sidecar accelerator that can be validated against current metadata and rebuilt
from authoritative namespace segments/snapshots. Indexed scans must never trust
sidecar rows that do not match current metadata, and key-only scans must agree
with document scans even after a sidecar tail fault. Later field postings and
`liblql` predicates must extend this boundary with storage-owned index
segments/postings and candidate iteration, rather than falling back to a single
full-log scan. Scan-mode key-only queries use a storage key-scan primitive when
the backend provides one, so configured scan mode does not copy full metadata
rows for `query_keys`. Key-only scan and indexed-scan primitives copy only
visible keys before invoking callbacks. The current disk backend serves both
primitives from the query-summary projection rather than the full metadata row
array, so `query_keys` does not pay for metadata row
copies.
Indexed match-all document scans also page over the query-summary projection
and copy only the row fields currently required by query callbacks: key, ETag,
owner, version, update timestamp, and query-hidden state. Document payloads are
still loaded only after a summary row survives pagination and visibility
filtering. The current disk backend also maintains an internal owner posting
index over query-summary rows. That pre-LQL predicate boundary can return
document rows or key-only candidates for one owner in stable key order without
walking unrelated owners in the namespace. The `query.index` sidecar records
written by the current implementation carry the owner column; older sidecar
records without that column remain replayable because the authoritative
metadata log can repopulate the summary owner before candidate scans run.
Large-namespace low-match indexed searches must be able to walk the relevant
posting/candidate sets without loading every metadata summary or every document
payload in the namespace.
The first durable LQL posting slices index exact equality candidates for strict
JSON Pointer document fields with string, boolean, null, and numeric values,
plus numeric range candidate checks, string `in` candidates, text `prefix` /
`iprefix` and `contains` / `icontains` candidates, and `exists` presence checks
over those field postings.
Numeric equality and range bounds use canonical numeric keys so equivalent JSON
number spellings compare consistently instead of relying on raw token text. Text
predicate postings store the liblql string-predicate view for JSON strings,
booleans, and number source text; JSON null still has no text-predicate posting.
Object and array field containers also emit presence postings so `exists` can
narrow candidates for structured values without requiring a scalar leaf.
Indexed mode also recognizes top-level full-form `and` conjunctions made only
of supported equality, range, string `in`, prefix, contains, and exists terms
and intersects their storage-owned postings before loading candidate documents.
Values inside one `in` term are unioned before that term is intersected with
other supported predicates. Range-only selectors preserve existing key/cursor
ordering by checking numeric postings from the key-ordered summary scan rather
than using value-sorted postings as the primary result order. The client
extracts those shapes only as candidate hints; final predicate acceptance still
runs through `liblql`. Mixed equality/range/in/prefix/contains/exists `and`
selectors intersect equality posting candidates with numeric range, string
membership, text prefix/substring, and presence postings when every hinted
child is supported.
The `query.index` sidecar starts with a format/version record so incompatible
posting layouts rebuild from authoritative namespace segments/snapshots instead
of being trusted; the text-predicate posting slice increments that format
version.
In explicit scan mode, calls route through the ordered scan path and emit no
index sequence because no durable query index is consulted. `query_keys` streams
keys, excludes `query_hidden=true` metadata, uses `cursor` as `start_after`, and
returns `keys` as the return mode. Exact key, owner, and key+owner selectors
filter that same ordered scan before limits and cursors are applied. Both
`query_keys` and
`query` report local metadata such as `query_candidates`. `query` streams NDJSON
document rows in the same ordered page, embeds JSON state payloads as
`document`, emits `null` for non-JSON or empty state payloads, and returns
`documents`. Pouch `flush_index` is synchronous for the current local
projection: it returns accepted/flushed/not-pending and the latest index
sequence. That sequence is a logical monotonic token derived from the storage
high-water mark, not a physical log record count, so compaction and reopen
cannot make query tokens move backwards. Indexed match-all queries accept
`refresh=wait_for` by performing the same synchronous local index flush before
scanning the indexed projection. Explicit scan mode remains available for
full-log/full-summary scanning through the ordered metadata summary API, but it
does not accept refresh hints because no durable query index is consulted.
Non-empty field selection, non-document scan return modes, and LQL selectors
beyond exact key/owner equality and their conjunction remain unsupported until
the indexed/LQL query slice lands.

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

This is enforced by a local CTest source contract over the storage interface and
disk-log backend. The allocator implementation itself is the only intentional
place that may wrap platform allocation functions for the default allocator.

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

Namespace listing must filter out files and blank directory names, sort results
lexically, and treat reserved internal namespaces as implementation-owned.
Queue/object scans and metadata scans must also return stable lexical order;
pagination cursors are string keys, not byte offsets into mutable files.

Reserved internal namespaces must be explicit. Backend identity uses `.lockd`
and transaction decision records use `.lockd-txn`. Public acquire/update/query
and queue paths must reject user requests that target those namespaces, while
the disk backend remains free to use them internally.

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
- state link, used by staged promotion and by compaction/replay when a live
  state head needs to reference payload bytes protected in another segment or
  snapshot

The record header should stay compact: a 24-byte little-endian header with a
magic, version, type, lengths, and CRC is sufficient for v1. The C
implementation must define its own constants in the local pouch code and
document them beside the encoder/decoder tests.

Record metadata is intentionally type-specific and compact:

- metadata put/delete records carry generation, modified time, and an opaque
  metadata ETag for puts;
- state put/link records carry generation, modified time, plaintext size,
  cipher size, state ETag, and descriptor bytes;
- state delete records carry generation and modified time;
- object put records carry generation, modified time, payload size, object
  ETag, content type, and descriptor bytes;
- object delete records carry generation and modified time.

Metadata ETag encodings and payload ETag encodings are deliberately different.
Metadata records use an opaque generated identifier, while state/object ETags
are hashes of the bytes that lockd exposes for CAS. The decoder must validate
the expected byte length for each record family and must reject invalid or
truncated descriptors before a record can update an index.

State-link payloads are compact references: target segment or snapshot name,
payload offset, and payload length. The segment name length must be bounded, the
payload must be CRC-validated like other payloads, and resolution must choose a
snapshot path when the target name is currently installed as a snapshot. Link
targets must not accept absolute paths or path traversal.

The C encoder must reject malformed descriptor/content-type lengths before
writing. The decoder must reject truncated metadata for every record type.

Header lengths are 32-bit fields in the disk-log format. Pouch v1 should treat
records whose key, metadata, or payload would overflow those fields as an
explicit unsupported-size error before writing any bytes. If larger payloads are
required later, they should be chunked into multiple records or moved to a new
record version rather than silently wrapping lengths.

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

Object put semantics need one caveat: object sizes and ETags are based on the
stored object bytes. State semantics may report plaintext size and cipher size
separately when encryption exists. The interface must keep both fields so
future encryption does not require a format break.

Replay must be conservative:

- a short read at the end of a segment is a trailing partial record and stops
  replay for that segment;
- a header magic or version mismatch stops replay at that point;
- a payload CRC mismatch stops replay at that point;
- no record after the first invalid/truncated record may be applied;
- already-applied records must remain valid when a later record is corrupt;
- unread offsets advance only after a whole record has been validated and
  applied.

Replay must not allocate unbounded buffers from on-disk lengths. Key, metadata,
payload, descriptor, content-type, and state-link segment lengths must be
checked against explicit implementation limits before allocation. Length fields
are untrusted even in files written by a previous pouch version.

The reader must support linked payload records. A state-link record points at a
payload span in another segment or snapshot. Link targets are live dependencies
and must be protected from deletion until no current record references them.

Large streaming records are written in two phases: write a provisional prefix,
stream the payload while computing content hash and CRC, then rewrite the prefix
with the final lengths, CRC, and ETag metadata. A crash before the final prefix
rewrite must leave an unreadable trailing record, not a visible corrupt record.
Replay must stop at that point and keep all prior validated records.

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

Summary rows use `published_version` when present and fall back to `version`.
The query-hidden attribute must be reflected in the summary without requiring a
full metadata decode. Hidden rows are excluded from normal query scans, but they
still remain visible to direct key lookup and lease/metadata operations. A
cached summary is valid only while the corresponding metadata record ref remains
the current head for that key; any newer metadata record must invalidate or
replace the cached summary.

Metadata generation is the public lockd object version. Metadata-only updates,
such as changing `query_hidden`, must append a new metadata record and advance
that version even when the state payload and state ETag are unchanged. Public
state reads must therefore return the current metadata version, not merely the
generation of the last state payload record. The state ETag remains the state
content validator; the metadata version remains the object CAS token used by
lease metadata, update, remove, attachment, queue-state, and reacquire flows.

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
- owner postings over cached query summaries
- later field/term/range postings for additional indexed metadata fields
- live/deleted filters for compacted index segments
- segment-level min/max and cardinality hints for fast negative matches

Indexes are projections. They must be rebuildable from manifest, snapshots, and
segments. No index file is authoritative in v1, but index files are still
durable performance artifacts. They may be rebuilt after corruption or version
upgrade, yet normal indexed query execution must use them rather than falling
back to a full log scan.

LQL integration should consume storage query APIs, not raw log scans. Until
`liblql` is available, pouch exposes a narrow internal predicate/query boundary
over indexed summaries, owner postings, stable ordering, limits, and cursors.
That same boundary must also support explicit scan mode over ordered metadata
summaries that were refreshed from authoritative log state. Additional
term/range postings should extend this boundary rather than bypass it.
Indexed mode is the preferred default; scan mode is a configured backend mode
or configured fallback. The persistent format should not encode LQL-specific
query plans. Pre-LQL scan support is intentionally limited to match-all, exact
key/owner equality, and key+owner conjunction for `query_keys` and document
`query`; richer predicate evaluation requires the later query/index
integration.

Query refresh contracts are storage-visible. A query that waits for a flush or
refresh target must observe committed summary records without requiring a full
payload read. Query pagination must be stable over lexical metadata keys,
namespace-isolated, and able to resume from `start_after` even when some keys
are concurrently deleted or become transiently unreadable.

## Refresh and Shared Filesystems

Correctness must not require `fsnotify`. A store instance observes other writers
by refreshing namespace state before reads and writes.

Refresh should:

1. Ensure namespace directories exist.
2. Read manifest changes since the last manifest offset.
3. Scan snapshots and segments for new files.
4. Replay unread committed records in deterministic order.
5. Track writer marker snapshots to skip unnecessary scans.

The current segmented implementation compares an identity-based logstore
generation derived from active snapshot and segment files. Size alone is not a
sufficient generation signal because compaction can replace one history with
different files of the same total byte length.

Writer markers are small files touched after commit. They are an optimization:
if no marker changed, a reader can often skip segment scanning. The marker
mechanism must tolerate filesystems with coarse mtimes by also considering file
size or periodic full scans.

The marker payload should intentionally alternate or otherwise change size when
mtime granularity is unreliable. Marker snapshots must ignore the current
writer's own marker and compare other writers by name, size, and modification
time. The current C backend uses peer-marker snapshots to avoid repeated
manifest/segment scans after an independent handle has refreshed from a peer,
uses marker-directory mtime and size as a fast path to avoid full marker
directory scans, stats cached peer markers when directory metadata is unchanged,
and periodically falls back to full marker scans plus segment validation so
marker hints cannot hide external rewrites indefinitely.

Refresh needs two modes:

- normal refresh may skip segment scans when peer markers and marker directory
  state are unchanged;
- single-writer refresh may skip marker and segment scans after the handle has
  synced once, because the mode promises that no peer writer is mutating the
  store;
- forced refresh always scans manifest, snapshots, and segments.

Forced refresh is required before concluding that a CAS target does not exist
after a miss, after a pending wait completes, and after a metadata/payload decode
failure. These extra refreshes are what make stale process-local indexes safe on
shared roots.

Decode failure handling is intentionally optimistic once. If a metadata payload
or summary cannot be decoded from the current ref, the store must force refresh
and retry the current head before returning the decode error. Another writer may
have committed a newer record after this process indexed the stale or corrupt
head. The retry must still fail if the refreshed current head decodes badly.

Single-writer mode may skip marker checks for the owning process, but shared
roots must default to safe refresh behavior.

On known NFS-style mounts, the backend should prefer close-after-commit or an
equivalent conservative mode so readers on other clients see sealed data
promptly. Queue filesystem watchers are an optional optimization and must be
reported as disabled or polling-only on filesystems where watch correctness is
unknown.

Close-after-commit must still respect pending writes, in-flight writes, fsync
batch drainage, and no-sync epochs. A segment must not be sealed merely because
a commit returned; it can be sealed only when no foreground write can still
extend it and no later sync boundary is needed to cover prior no-sync records.

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

Legacy/open-tail handling is not optional. A store must tolerate:

- segment files with no manifest because the manifest was introduced later or
  was not written before a crash;
- manifest files that contain `open` entries but no `seal` entries;
- a graceful restart where historical segments are sealed and compactable;
- a crash-style restart where the most recent tail remains open and must stay
  out of snapshots until it is no longer the active tail;
- current snapshots that are superseded by a newer tail record for the same
  key.

Open-only manifests need a one-time repair path. If the manifest contains
segment-open entries but no segment-seal entries, historical segments should be
backfilled as sealed while preserving the current active tail as open. After
the repair, those historical segments must become eligible for compaction
rather than remaining as a permanent uncompactable tail.

Read paths should cache open segment files with a bounded LRU, but must resolve
linked payload spans against the current segment or snapshot path. Compaction
cleanup must not close or delete files still needed by active readers.

Refresh is also the recovery path for restart and failover. A newly opened store
must rebuild namespace state from manifest, installed snapshot, and tail
segments without relying on any in-memory dispatcher state left by a prior
process. Queue transaction decisions and ordinary state writes must become
visible through this same replay path.

## Locking

The disk backend is single-writer per key mutation, not globally single-process.
Writers must coordinate with:

- an in-process striped mutex keyed by namespace and key
- a process-wide global striped mutex to serialize multiple store handles in one
  process
- an advisory file lock per namespace/key for cross-process and shared
  filesystem coordination

All three layers are required in pouch. A process may hold two client/store
handles against the same root; those handles must contend locally before
touching the same lock file. Two processes or hosts must then contend through
the advisory lock. Skipping the process-wide layer would make same-process
multi-handle tests pass accidentally under low contention and fail under CAS
races.

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

The storage backend must still report that disk-log pouch is not a general
multi-writer database. It can safely serialize same-root mutations with locks,
but higher HA logic must know it is append-serialized and not a consensus
backend.

NFS-style mounts need conservative defaults. Detection is necessarily
platform-specific, so it must be isolated from core storage logic. When a root
is known or configured as NFS-like, the backend should favor close-after-commit
or an equivalent mode that seals history promptly for remote readers, while
still relying on advisory byte-range locks and refresh scans for correctness.

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

Inline batching must compute each record ref's absolute record offset and
payload offset before releasing the caller, and it must release pooled inline
buffers exactly once on success or failure. Large streaming writes must hold the
append write mutex while writing and finalizing the prefix so no other writer
can interleave bytes into the same segment.

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

The fsync batcher should deduplicate segment files inside one batch: several
logical commit requests against the same open segment should pay at most one
physical sync while every waiter receives the same batch result. Diagnostics
must expose batch count, request count, max batch size, sync latency, and bucket
counts so benchmarks can prove whether batching is actually happening.

Commit-group behavior is part of the storage contract, not only an optimization.
When a logical operation writes multiple records, all records in the group share
one durability result and all waiters see the same success or failure. Finalizer
work that depends on durable records must run only after commit waiters succeed.
If a commit group is already committed when a new committer is registered, that
committer must still run and be drained so pending state cannot be stranded.

Commit waiters must be notified on both success and failure. A failed fsync or
append must clear pending maps for that group, mark the pending refs applied for
drain purposes, and wake readers/CAS writers that were blocked on the group.
Those waiters must receive an error rather than observing partially applied
indexes.

Crash windows to test explicitly:

- crash before append write: no new record appears;
- crash after partial record write: replay stops at the partial record;
- crash after full write but before fsync: safe mode may lose the record after
  filesystem recovery and must not expose it as committed before the commit
  group succeeds;
- crash after fsync before marker touch: refresh by segment scan must still see
  the record;
- crash after marker touch before another reader refreshes: marker is only a
  wake/invalidation hint and must not be treated as a durability barrier.

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

Manifest entries are append-only text lifecycle records. Pouch should preserve
the same semantic operations even if the C encoding differs:

- `open <segment>`
- `seal <segment>`
- `snapshot-install <snapshot>`
- `obsolete-segment <segment> <unix-time>`
- `obsolete-snapshot <snapshot> <unix-time>`

The manifest parser must be restartable from a saved byte offset for steady
state, but any snapshot install or obsolete-set change requires replay state to
reset because the ordered history has changed.

Segment names must contain a writer id and monotonically increasing per-writer
sequence so records from multiple writers sort deterministically. Snapshot names
must be unique and must not collide with segment names.

An opened segment should be recorded in the manifest when possible, but replay
must not depend on that manifest entry to find valid history. A missing manifest
or legacy manifest can be repaired by scanning segment files. The manifest is a
compact lifecycle index, not the only source of truth for committed records.

## Compaction

Compaction creates a snapshot segment containing the current live records from
candidate sealed segments and the installed prior snapshot. It never mutates
existing segment contents.

When the durable segmented history is large enough and the replayed record
count is more than twice the compacted live-head count, the writer builds
manifested snapshot files containing a private high-water record for
`next_version`, the latest non-deleted state heads, live metadata, live
attachments, and live queue messages. Superseded records and deletion
tombstones are omitted because observable state, CAS failures on deleted heads,
and queue/attachment absence are preserved without them. If snapshot
construction or manifest install fails, active segment backups are restored and
indexes are rebuilt from the still-authoritative segmented history before
returning.

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

Compaction snapshot records should materialize live linked state as normal state
put records when their source segment is being compacted. If the live state-link
record itself is outside the candidate set but points into a candidate segment,
that target segment is protected instead. This avoids deleting payload bytes
that are still reachable through a current committed state head.

Cleanup is deliberately conservative. Obsolete files are eligible only after a
grace period, delete failures keep the manifest obsolete entries intact for a
later retry, and open read handles may outlive the namespace index entry that
made the file obsolete.

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

Promotion is intentionally link-based. It must not read the staged payload into
memory, copy the payload into a new state record, or re-encrypt the bytes. The
committed state head should point at the staged payload span, then compaction may
later materialize it into a normal state-put record if doing so is safe.
The current disk backend implements this promotion path with sorted committed
key and staged-key guards before the append-log lock. Staged writes are ordinary
state writes against the generated staged key, and staged discard acquires the
staged-key guard before deleting the staged record.

The staging listing contract is narrower than generic object listing. It must
include direct staged state objects only and exclude nested staged attachment
objects such as `.staging/<txn>/attachments/...`.

Client `acquire_for_update` must route handler state writes through staged state
instead of the ordinary update path. A release request with `rollback` is not
itself a rollback mechanism if the handler has already committed visible state.
The pouch client adapter therefore installs a staged update receiver only for
the duration of the handler callback. Handler success promotes the staged state,
then updates the active lease metadata to the promoted state version and ETag
before release. Handler failure discards the staged key and releases with
rollback. Reads by other clients must never observe a handler write before
promotion, and a failing handler must leave the previous committed state
reachable.

Transaction decision records live in the reserved transaction namespace as
objects. Recovery must support:

- commit records that promote staged state for every participant;
- rollback records that discard staged state and clear staged metadata;
- expired pending records that roll back;
- participants in different namespaces;
- explicit replay by transaction id;
- replay after restart;
- cleanup of decided transaction records after replay completes.

Transaction decision objects must be ordinary durable object records with a
content type and ETag, but their namespace is reserved and unavailable to public
lock operations. The replay worker must be able to scan these records after a
fresh open, decode pending/commit/rollback decisions, apply all participants,
and then delete or mark the decision complete through normal object-delete log
records. Recovery correctness must not depend on an in-memory transaction
manager surviving restart.

Participants may span namespaces. Replay must normalize and lock each
participant by namespace/key in a deterministic order before modifying staged
state or metadata, otherwise cross-namespace transactions can deadlock or
publish only a subset of their decision.

Transaction replay must wake dependent queue and query paths after restart.
This means replaying a decision record has to update the same metadata/state and
queue object records that an online commit or rollback would have produced. A
consumer or query client polling a pouch root must not require a separate
server-owned wake channel to notice the result.

Restart recovery must distinguish abandoned staged state from undecided staged
state. Staged payloads associated with a durable pending transaction record must
remain available until the transaction is committed, rolled back, or expires.
Staged payloads whose lease/transaction has expired with no durable commit
decision must roll back and be cleaned. Query and queue tests depend on this
because they restart the process between staging and replay.

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

Queue notification files are per queue and are not the queue state. A failed
notification touch after a committed queue mutation must not roll back the
mutation, and a missing notification file must not prevent pollers from
discovering committed queue objects during their next refresh.

Current disk backend milestone: the private storage interface exposes queue
wake status. The disk backend reports `polling`, marks queue marker files as
best-effort hints, and reports filesystem notifications disabled. This is an
internal diagnostic/capability surface, not a durability contract.

The consumer service should be able to run against pouch without special public
API changes. Internally, its wait loop should use the pouch queue polling
strategy when the client backend is pouch.

Queue polling has a latency/CPU contract. Idle enqueue must wake dispatch
without requiring a continuous tight polling loop. Poll interval, jitter, and a
slower resilient polling interval should be tunable. Filesystem notification, if
enabled and supported, is only a wake optimization and must be disabled on
filesystems where it is unreliable.

The queue wake API must expose status, not just behavior. A caller should be
able to learn whether queue wake mode is polling or filesystem notification and
why. Pouch can initially expose this only internally for diagnostics and tests.

Queue delivery invariants:

- only one competing consumer may claim a visible message;
- message TTL expiry removes an unclaimed or in-flight candidate from available
  and pending views after refresh/replay;
- retry exhaustion makes a message terminal rather than immediately visible
  again, and restart/replay must preserve that terminal state;
- nack with a delay must hide the message until that delay expires and then
  redeliver it with an incremented attempt count;
- nack without a delay must make the message immediately eligible for
  redelivery with a fresh delivery token;
- visibility timeout handoff must reject a stale ack from the old owner after a
  different owner successfully claims and acks the message;
- ack removes the message from available and in-flight views;
- dequeue returns a payload stream pinned to the log file that was indexed while
  the queue lease was claimed; the backend must open and position that stream
  before releasing the storage lock so a concurrent compaction/rename cannot
  make the queued payload offset refer to a different file generation;
- observability/stat calls must be read-only and must not perturb delivery
  state;
- subscribe and start-consumer must preserve auto-ack, explicit ack/nack,
  handler-failure nack, and state-save behavior;
- polling mode must pass the same semantics as watch mode.

Queue transaction invariants:

- transaction commit publishes queued messages and stateful queue state as a
  single logical decision;
- rollback removes staged queue effects and staged state;
- stateful commit/rollback must work for queue message state handles as well as
  normal lock state;
- mixed-key transactions may involve queue objects and ordinary state keys;
- fanout across nodes and replay after restart must be observable by pollers
  even without filesystem notifications.

Start-consumer is part of the v1 storage-facing contract. The pouch client path
must preserve the same observable behavior as the server-backed client:
auto-ack on handler success, explicit ack/nack support, nack on handler
failure, state-save visibility for stateful handlers, and no duplicate acked
delivery under contention.

The polling-mode start-consumer path is the baseline, not a fallback after
watch mode. Tests must run with filesystem notification disabled and prove that
auto-ack, explicit ack/nack, deferred nack, handler failure, and state-save
paths all complete within bounded polling intervals.

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

Object copy is required for parity with attachment and transaction flows. A
copy must preserve content type and descriptor and enforce expected-ETag and
if-not-exists semantics on the destination.

Copy must stream from the source object reader into the destination append path.
It must not materialize large objects in memory, and it must re-run destination
CAS/if-not-exists checks after acquiring the destination lock. Source read
failure must leave the destination unchanged.

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

Retention sweep is metadata-driven. It must decode current metadata records,
check `updated_at_unix`, append metadata/state delete records for expired keys,
and continue when individual keys fail to decode or delete. It must not remove
log files directly. The current private disk hook implements the append-record
path and idempotent reruns; future public scheduling/configuration can sit above
that hook without changing the log semantics.

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

Encrypted state reads need descriptor and plaintext-size override hooks because
the authoritative metadata document may carry the descriptor that unlocks a
state payload. The storage layer should keep those hooks internal, but the data
model must not assume the state record alone always contains every decryption
hint needed by higher lockd logic.

Backend identity also needs a crypto migration path. If the backend-id marker
was previously stored unencrypted and encryption is enabled later, the store may
rewrite the same identity encrypted under an expected ETag. The identity value
itself must not change during that migration.

## Failure Mode Matrix

The following failure modes are derived from the disk implementation and its
regression/integration coverage. Pouch tests should cover them directly or via
observable client behavior:

- partial header, key, metadata, link payload, or object/state payload at the
  tail: replay stops at the incomplete record and preserves all prior records;
- bad magic, unsupported record version, metadata decode failure, or CRC
  mismatch: replay stops at that record and applies nothing after it;
- unsupported future record versions are corruption boundaries for the current
  reader. They must stop replay rather than being skipped, because later records
  may depend on semantics the current implementation does not understand;
- append write failure before pending registration: no index changes and the
  caller receives the write error;
- fsync failure after pending registration: all pending refs in the group are
  failed, pending maps are cleared, committed indexes are unchanged, waiters are
  woken with the error;
- marker touch failure after fsync: the write may still be durable; later forced
  refresh or segment scan must find it;
- process stop with graceful close: writer presence marker is removed and
  historical segments may become compactable;
- process abort/crash: writer presence remains until TTL expiry and the latest
  tail is replayed conservatively;
- manifest append failure during compaction install: the temporary snapshot is
  removed and live indexes/obsolete sets remain unchanged;
- snapshot build failure: no manifest entries are appended and no index refs
  move;
- cleanup delete failure: obsolete entries stay tracked for a later retry;
- compaction validation drift: temporary snapshot is abandoned and foreground
  writes win;
- live state-link into a candidate segment/snapshot: the target is excluded or
  protected from obsolete cleanup;
- stale CAS during concurrent writes from another process: exactly one writer
  succeeds and the loser sees CAS mismatch or not-found according to the
  operation;
- passive HA node attempts mutation: the public API reports passive/fenced
  status, while storage itself remains refreshable for reads/replay;
- restart with commit decision object: staged participants eventually promote
  and the decision record is cleaned;
- restart with expired pending decision object: staged participants roll back
  and the decision record is cleaned;
- restart with staged state and no valid decision: expired staging is rolled
  back and staging objects are removed;
- acquire_for_update handler failure after a state write: the staged state is
  discarded, the active lease is released with rollback, and the last committed
  state remains visible;
- acquire_for_update promotion success followed by metadata store failure:
  callers see an error and the lease is released with rollback, but recovery
  must treat the promoted state and stale metadata as a repairable consistency
  gap because promotion cannot be unlinked after it is durably appended;
- queue ack after visibility handoff: stale owner ack is rejected after another
  owner claims and acks;
- queue TTL expiry, delayed nack, immediate nack, retry exhaustion, and replay
  after each of those transitions preserve available/pending/failed counts;
- idle queue enqueue: dispatcher wakes through polling/watch hints without a
  tight busy loop.

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

Current native benchmark coverage in `bench/lockdc_bench` includes pouch
state roundtrip, explicit state payload write/read sizes at 1 KiB, 64 KiB,
1 MiB, and 16 MiB, staged promotion, public mutate throughput, attachment
put/get, enqueue/dequeue/ack, transactional queue ack rollback/redelivery, and
transactional queue ack commit/removal, hot-key compaction churn, metadata
summary scans, open/replay index rebuild,
direct indexed summary scans, direct key-only indexed scans, public indexed
document query streaming, public scan-mode document query streaming, public
indexed key streaming, public scan-mode key streaming, exact-key indexed
summary/key scans, exact-key public document and key queries in both scan and
indexed modes, exact-owner and key+owner indexed summary/key scans, exact-owner
and key+owner public document and key queries in both scan and indexed modes,
owner document and key queries with removed-state candidates filtered out in
both scan and indexed modes, low-match strict JSON Pointer LQL field selectors
over public document and key query paths in both scan and indexed modes, and
retention sweep throughput over metadata/state rows. The
native harness also includes a hot-key contention case that repeatedly contends
two store handles on one key and verifies that lock-contention diagnostics
advance under that workload.
The same executable exposes opt-in live Go lockd disk comparison cases for
low-match strict JSON Pointer field selectors:
`lockd-disk-query-field-low-match` and
`lockd-disk-query-keys-field-low-match`. They use
`LOCKDC_BENCH_DISK_ENDPOINT` / `LOCKDC_BENCH_DISK_BUNDLE`, falling back to the
disk e2e endpoint and bundle defaults, and are excluded from `all` unless
`LOCKDC_BENCH_LIVE=1` is set. This keeps default benchmark runs local while
making pouch-versus-Go disk query comparisons reproducible against the same
public client surface.
The benchmark output includes allocation/free counts and peak outstanding bytes
for cases that run through the benchmark allocator. These are smoke-sized local
benchmarks rather than performance gates; the larger matrix above remains the
target coverage for regression thresholds.

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
- decode rejection for each metadata variant when payload bytes are truncated
- state-link encode/decode, replay, read, and compaction protection
- payload CRC mismatch handling
- metadata generation replay ordering
- CAS success/failure
- if-not-exists behavior
- key normalization and lock path escaping
- manifest parsing and append
- marker snapshot change detection
- sorted scan pagination
- allocator failure paths
- pending commit application order when commit groups complete out of append
  order
- pending maps track only the latest pending record per key and are cleared on
  success or failure
- marker snapshot change detection when directory mtime is unchanged but marker
  size or marker mtime changes
- read-file cache eviction does not close files with active readers
- lock-file cache eviction does not unlock or close active locks
- state-link payload resolution validates the referenced key and payload span
- no-sync epoch prevents segment sealing before a later sync boundary catches up
- manifest offset replay plus replay reset after snapshot/obsolete changes
- forced refresh after CAS miss, pending wait, and decode failure
- backend-id create race and encrypted rewrite without identity change

Integration tests:

- crash after append before fsync does not expose uncommitted records
- crash after fsync rebuilds indexes correctly
- two store handles contend on the same key
- two processes contend on the same root
- compaction preserves live metadata/state/object records
- compaction abandoned on validation drift
- obsolete files deleted only after grace period
- queue dequeue visibility and ack/nack/extend semantics
- queue TTL expiry and retry exhaustion across reopen/replay
- consumer service runs against `pouch://`
- attachments survive reopen and compaction
- staged promotion preserves small and large payloads
- staged promotion cleans the staging key after commit
- multi-key staging locks do not deadlock on lock-stripe collisions
- transaction commit/rollback replays after restart and across namespaces
- expired pending transactions roll back
- transaction replay wakes queue and query observers after restart in both
  polling and watcher modes
- query-hidden metadata is excluded from scans
- query backend mode configuration selects indexed mode by default, explicit
  scan mode when requested, and only falls back according to configured policy
- scan-mode `query_keys` and `query` force authoritative log refresh before
  using ordered metadata summaries with stable pagination, stream results,
  exclude query-hidden metadata, and load state payloads only for document rows
- query pagination, namespace isolation, public-read results, and streamed
  document responses work against disk summaries
- query flush-wait and refresh-wait contracts observe committed summary rows
  without full payload reads
- query index rebuild/upgrade handles existing disk state
- large-namespace low-match queries do not require loading every payload
- remove semantics: empty remove, remove version bump, keepalive after remove,
  stale remove/update CAS failures, remove then recreate
- queue polling: ack removal, nack redelivery, visibility timeout handoff,
  delayed nack hiding, retry exhaustion, TTL expiry, multi-consumer contention,
  failover dequeue/ack, subscribe with state, and start-consumer
  auto-ack/state-save/failure paths
- queue high fan-in/fan-out has no duplicate acked delivery
- queue transaction decision commit/rollback, stateful commit/rollback, mixed
  key commit/rollback, fanout across nodes, and replay after restart
- queue idle enqueue wakes dispatch without a continuous polling loop
- exclusive writer marker: close removes marker, abort preserves marker until
  TTL expiry, heartbeat payload overrides misleading future mtimes
- HA mode expectations: single mode does not create HA lease metadata, auto mode
  promotes to failover when a peer exists, single mode fences auto peers, and a
  stale aborted single-writer marker eventually lets auto mode activate
- compaction restart matrix: snapshot plus tail replay, sealed history after
  graceful restart, sealed history after crash-style restart, manifestless
  legacy history, and legacy open-only manifest upgrade
- compaction generations: disabled compaction creates no snapshot,
  below-threshold compaction skips, second-generation snapshots obsolete prior
  snapshots only when no live link protects them, and background compaction
  preserves foreground reads
- staged promotion preserves small and large encrypted payloads and cleans the
  staging key
- lock-state multi-key acquisition cannot deadlock when two logical keys collide
  on the same lock stripe
- backend verification opens two independent store handles, runs concurrent
  metadata and state CAS updates, and proves exactly one contender succeeds
- reserved internal namespaces are rejected by public lock operations while
  internal transaction/backend-id records remain usable

Current default local integration coverage in `tests/integration` exercises
public `pouch://` clients sharing one disk root for state and attachment
persistence, query-hidden metadata versioning and persistence, attachment
prevent-overwrite and overwrite semantics, attachment delete/delete-all
semantics, client-level attachment APIs, namespace query-engine configuration
reporting, endpoint query-engine/fallback override for document and key
queries, scan-mode corrupt/future/absent
query-index sidecars, indexed document and key-query replay after reopen,
indexed document and key-query namespace isolation,
indexed document and key-query pagination,
indexed owner-selector document and key-query pagination,
indexed owner-selector query-hidden suppression and unhide visibility,
indexed owner-selector removal suppression,
scan and indexed key/key+owner-selector removal suppression,
scan and indexed row/key pagination across removed-state candidates,
indexed document and key-query `refresh=wait_for` against open readers,
indexed owner low-match queries over larger namespaces with candidate counts,
transaction attachment
replay commit/rollback, transaction attachment-delete replay commit/rollback,
reserved internal namespace rejection through public state, queue, query,
flush, config, and default-namespace paths while internal transaction records
remain usable,
scan-engine fallback and explicit bypass for refresh requests, queue visibility
handoff, stale delivery fencing, nack redelivery, delayed nack redelivery,
abandoned delivery handle failover, client-level nack retry exhaustion terminal
behavior across reopen, read-only queue stats while a delivery is invisible,
explicit handler
acknowledgement, handler-success auto-ack, managed-consumer lifecycle
callbacks, watch snapshots, direct subscribe-with-state, direct
dequeue-wait polling for later enqueues, batch dequeue without duplicate acked
delivery, queue transaction commit/rollback including mixed state plus queue
decisions, prepared mixed replay/commit, and expired mixed recovery after
reopen, cross-namespace transaction commit/rollback and prepared replay after
reopen, expired cross-namespace prepared transaction rollback on reopen,
transaction target-backend mismatch fencing through the public endpoint,
managed consumer state-save through both blocking `run()` and asynchronous
`start()`/`wait()`, managed failure redelivery, managed explicit
acknowledgement and defer redelivery, and cross-client CAS. The optional e2e
shard still covers broader consumer-service behavior.

Current pouch client unit coverage includes deterministic local unsupported
errors for LQL-shaped selectors, namespace mutation management, and every
transaction-coordinator cluster/resource-manager method that pouch v1 defers.

Current disk unit coverage includes queue nack and extend allocator-failure
paths, TTL expiry, retry-exhaustion replay paths that prove failed
redelivery-control mutations leave the active lease ackable and terminal queue
states remain terminal after reopen, forked cross-process CAS contention that
proves exactly one independent process can update a stale state ETag,
retention sweep replay idempotence after reopen, and forked queue dequeue
contention that proves a single message is leased to only one independent
process, plus `if_needed` compaction skip reasons for both below-min-log-size
and below-obsolete-threshold stores. A source-level allocator contract also
rejects raw platform allocation calls in the storage interface and disk-log
backend.

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
9. Query backend mode configuration with indexed default and explicit scan mode.
10. Durable summary/posting index segments with rebuild and compaction tests.
11. Compaction snapshots and cleanup.
12. Queue semantics and consumer-service adapter.
13. Storage indexed-query boundary, then `liblql` integration when ready.
14. `pouch://` endpoint selection in the client engine.
15. Benchmarks, diagnostics, packaging, and final ABI bump.

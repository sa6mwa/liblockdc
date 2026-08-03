# liblockdc TODO

## Pouch Exclusive-Writer Cutover

### Status

Pouch is not fully aligned with Go lockd disk. The cutover aligned the durable
record model, keyspace, lease semantics, queue/attachment behavior,
transactions, recovery, compaction safety, and the normal exclusive-mode
resident append path. It must not be described as fully aligned until this
document's remaining correctness, lifecycle, shared-root, and performance work
is complete and verified.

Pouch remains unreleased. Delete rejected pre-release implementations rather
than adding migration readers, format dispatch, compatibility modes, or legacy
tests. This cutover changes runtime ownership and in-memory architecture, not
the authoritative Pouch record format.

### Standing Format Invariants

These are deliberate Pouch divergences and are not refactor targets:

- Pouch records and metadata use the C-native binary format. Pouch does not
  use protobuf or Go's `LOGD` bytes and does not promise cross-engine file
  interoperability.
- Durable payload sizes, cipher sizes, record offsets, payload offsets,
  segment/snapshot IDs, generations, and index sequences use fixed-width
  `uint64_t`-backed Pouch types on every supported ABI. Narrowing at a public
  C API boundary must be range-checked before mutation commits.
- Pouch retains its public names, C structs, errors, logging, and derived query
  artifact format. Those choices must preserve the corresponding Go disk
  semantics and durability property.
- The authoritative durable state remains namespace-local rolling Pouch
  segments and snapshots. In-memory indexes, query artifacts, and queues are
  derived accelerators, never a second authority.

`docs/pouch-storage.md` is the storage authority. Go lockd disk remains the
semantic and operational reference. Any future divergence needs a written
reason, its correctness/durability impact, and targeted proof in the same
change.

## Product Contract

### Default: Exclusive Writer

Pouch defaults to one logical writer per root. A successful exclusive open
holds root ownership for the lifetime of the writer and uses a resident
namespace logstore: an in-memory projection, active segment identity and
offset, reusable active descriptor, and bounded append/commit pipeline.

Normal exclusive-mode acquire, update, release, queue, attachment, and object
mutations must not rescan a namespace, reopen the active segment, reread the
manifest, or acquire a cross-process append lock. Recovery is performed at
open, writer takeover, segment rotation, compaction installation, or an I/O
failure, not on every mutation.

If another exclusive writer owns the root, open must fail with an actionable
ownership error. It must never silently downgrade to shared-root mode.

Default-mode HA is active/passive: a standby takes ownership only after clean
handoff or validated writer expiry/failure. It is not active-active writing.

### Optional: Shared Root

Shared-root writing remains a supported explicit mode. Multiple Pouch clients
or processes may write one root, but it is an extension beyond Go disk's public
single-writer capability and is allowed a lower throughput target.

Each shared writer keeps a local resident projection and appender. It obtains
cross-process append authority once for a bounded batch, incrementally replays
only the committed tail since its cursor, appends and publishes the batch,
syncs when required, advances its cursor, and releases authority. Segment
rotation, writer epochs, manifest changes, maintenance, and handoff invalidate
or refresh affected cursors. Shared mode must never rescan the entire namespace
for an ordinary uncontended mutation.

The durable format, public API semantics, fencing rules, crash behavior, and
compaction rules are identical in both modes. Runtime mode is not encoded into
user data records.

## Refactor Program

### 1. Establish The Common Namespace Logstore Core

- [ ] Design one internal namespace-logstore owner used by all state, object,
  metadata, lease, queue, attachment, transaction, and index mutations.
- [ ] Keep a resident logical record index keyed by normalized key, durable
  index high-water sequence, active segment leaf, append offset, and manifest
  generation/lifecycle state.
- [ ] Hold reusable descriptors for the active append segment and bounded read
  sources. Close and invalidate them only at rotation, recovery, maintenance,
  mode transition, I/O failure, or Pouch close/abort.
- [ ] Replace duplicated client pre-read plus state-layer reread paths with one
  mutation authority that reads cached metadata, evaluates CAS/lease state,
  appends, commits, and publishes the updated projection atomically.
- [x] Preserve real streaming. Large and all non-memory values flow from source
  through compression/encryption, hashing, CRC, pending-record finalization,
  and the active descriptor without whole-value materialization. Only the
  unread bytes of a bounded `lc_source_from_memory` value may use the explicit
  complete-record materialized fast path.

Acceptance:

- [ ] The record format and replay results are unchanged for all supported
  record families.
- [ ] A successful mutation is visible only at the documented finalized-record
  and durable-sync boundary.
- [ ] A failed append or commit leaves no published cache/index entry and uses
  the existing crash-tail recovery rules.

### 2. Cut Over Default Exclusive Writer Mode

- [x] Make exclusive root ownership the default direct-open and endpoint
  behavior. Shared-root is an explicit opt-in and incompatible mode owners
  fail before ordinary root setup can mutate the store.
- [x] Establish a local writer-mode epoch and lazily construct each exclusive
  namespace's resident projection, active descriptor, and cursor on first use.
- [x] Route state, lease, object, attachment, queue, and staged-transaction
  mutations through the resident append path. Default-exclusive metadata-only
  state mutations use a bounded ordered append worker; bounded SDK memory
  bodies use one complete-record append while all streaming bodies remain
  direct, and public completion still waits for its own finalized commit
  result.
- [x] Match Go disk group-commit scheduling: a durable group waits at most two
  milliseconds or until its maximum request count, deduplicates file syncs,
  and propagates the shared result to every waiting operation.
- [ ] Measure durable-sync throughput and latency against Go disk under the
  standard concurrency matrix.
- [x] Route cached public reads, lease metadata reads, direct query reads, and
  scan-oriented query views through the resident projection in exclusive mode.
- [x] Rotate without reopening healthy normal append descriptors; publish the
  new manifest and replace only the affected resident descriptor/cursor.

Acceptance:

- [x] Normal exclusive state-core mutations do not perform namespace directory
  scans, manifest parsing, tail repair, cross-process lock acquisition, or
  active segment open/close work.
- [ ] Acquire, get, update, release, queue, attachment, and query operations
  preserve the existing observable contract under plaintext, crypto,
  compression, and crypto+compression roots.
- [ ] A crash or explicit abort followed by reopen recovers the last published
  record and rejects malformed sealed data exactly as specified.

### 3. Retain Shared-Root As An Explicit Mode

- [ ] Keep separate Pouch instances against one root correct for independent
  and conflicting keys, lease fencing, queue delivery, transactions, segment
  rotation, compaction, and failover.
- [x] Retain each shared writer's verified projection cursor after a successful
  local append; under append authority, replay only peer bytes beyond that
  cursor before the next local append. First materialization of the selected
  active leaf and repair of an incomplete unseen suffix retain that cursor.
  Historical-byte validation remains a recovery or manifest-invalidation
  operation, not a healthy hot-path scan.
- [x] Batch independent metadata-only shared mutations per local writer while
  holding each request's exact key ownership; tail only the delta while the
  shared append gate is held.
- [ ] Extend bounded shared append-gate batching to eligible body and
  multi-record mutations without materializing streaming payloads or widening
  exact-key ownership.
- [x] Fence shared/exclusive ownership with the root-wide process lock: every
  live shared writer holds its read lock for its lifetime, and exclusive mode
  requires the conflicting write lock. A crashed process loses that lock before
  takeover, while the per-handle mode epoch invalidates local descriptors.
- [x] Cover fork-safe shared-process state writes, conflicting lease rejection,
  fencing-token handoff, and root-lock crash handoff. Process-level queue,
  rotation, compaction, and maintenance contention coverage remains required.
- [x] Make an in-process mode transition quiesce append-capable operations
  through durable completion, then advance the local epoch so active append
  descriptors are closed and projections validate/replay only when required.
  Cross-process takeover remains covered by the durable writer-epoch item
  above.

Acceptance:

- [ ] Two or more explicit shared-root clients pass process-level contention,
  key-conflict, lease-fencing, and crash-handoff tests.
- [ ] Shared mode refresh work is proportional to the unseen committed tail,
  not the total namespace history.
- [x] Opening two default exclusive clients against one root fails
  deterministically and does not mutate the root, including independent
  processes rather than only two handles in one process.

### 4. Maintenance, Recovery, And Resource Lifecycle

- [ ] Make compaction/retention take a writer epoch or maintenance barrier,
  drain affected append work, capture/validate/install, then invalidate only
  changed namespace resources.
- [ ] Tail repair is a takeover/recovery operation. Do not invoke it on a
  healthy resident exclusive append path.
- [ ] Preserve sync foreground operations and the existing background janitor
  contract: maintenance must never delay a completed foreground mutation.
- [ ] Close descriptors, stop workers, release ownership, and clean all cache
  state correctly on close, abort, failed open, and fork-sensitive test paths.

Acceptance:

- [ ] Rotation, compaction, retention, close, abort, and writer handoff do not
  leak descriptors or leave stale cache references.
- [ ] Compaction cannot remove a span reachable by state, object, staged, or
  attachment references in either writer mode.

### 5. Remove The Superseded Hot Path

- [ ] Delete per-mutation manifest scan/reopen/tail-repair behavior from the
  exclusive path.
- [ ] Delete duplicate lease validation reads where mutation authority already
  holds the target-key lock and cached projection.
- [ ] Delete tests, comments, benchmark assumptions, and diagnostics that
  define shared-root work as the ordinary default write path.
- [ ] Do not keep an unused compatibility implementation after the cutover.

## Verification And Performance Evidence

### Behavioural Proof

- [ ] Add focused unit tests before or with each irreversible state transition:
  ownership default, explicit shared mode, writer takeover, cached mutation
  freshness, crash tails, rotation, grouped durable failure, and descriptor
  lifecycle.
- [ ] Add public API coverage for acquire, keepalive, release, get, update,
  mutate, attachments, queue lifecycle, transactions, scan, indexed query,
  full-text query, crypto, and compression in exclusive mode.
- [ ] Add multi-process shared-root tests for non-conflicting writes,
  conflicting CAS/lease writes, queue delivery ownership, active append tail
  refresh, rotation, maintenance, and stale-writer rejection.
- [ ] Run only focused checks while refactoring. Run the configured full test
  and release gates after the coherent cutover is complete.

### Benchmark Contract

- [x] Use deterministic JSON-safe high-entropy production payloads and fixed
  profiles large enough to retain the multi-segment rollover invariant under
  compression. Custom profiles remain responsible for exceeding their target.
- [x] Compare only the explicit semantically comparable end-to-end core metric
  allowlist with Go disk: acquire, lease/public get, update, release, queue,
  attachment, scan/index/full-text query, and restart recovery.
- [x] Keep `reopen`, `flush-reopen`, aggregate `ns/op`, and Pouch-only C timing
  diagnostic only; they are not independent cross-engine comparison metrics.
- [x] Record matching cold/warm indexed-key query metrics instead of allowing
  total benchmark time to hide a slow query path.
- [x] Report independent comparable attachment write and retrieve metrics;
  retain the combined roundtrip only as a diagnostic.
- [x] Provide bounded development commands: isolated exclusive Pouch probes,
  the segmented production matrix, and explicit shared-root concurrency, with
  the combined routine limited by one 90-second outer timeout.
- [ ] Set and document the numeric exclusive-mode release budget from a stable
  baseline before claiming completion. The intended outcome is a substantial
  Pouch advantage on every core feature, not merely aggregate parity.

## Completion Criteria

Do not claim full Pouch/Go-disk alignment until all of the following are true:

- Exclusive writer is the documented default and follows the resident Go-disk
  operational model.
- Shared root is explicit, correct, and independently tested.
- The Pouch binary format and portable `uint64_t` accounting remain intact.
- No ordinary exclusive mutation uses shared-root discovery work.
- Focused behavioural and failure tests cover both modes, and the configured
  full verification/release gates pass.
- The benchmark gate has valid fixtures and evaluates only comparable core
  metrics, with the exclusive performance budget met for every supported Pouch
  transform configuration.
- `docs/pouch-storage.md` contains the final divergence register and no stale
  statement that the pre-cutover implementation is fully aligned.

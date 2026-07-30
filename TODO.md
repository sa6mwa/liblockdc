# liblockdc TODO

## Pouch Go-Disk Alignment Cutover

Pouch is an unreleased initial storage engine. There is no compatibility
contract for rejected pre-release layouts. Do not add migration readers,
compatibility branches, old/new dispatch, or durable version bumps for the
current broken pouch layout.

The implementation authority is `docs/pouch-storage.md`. Go lockd disk is the
semantic reference. Pouch may diverge only where the divergence is explicitly
documented, preserves the same correctness and durability property, and is a
better C-native implementation. Accepted standing Pouch divergences:

- C-native binary record and metadata formats instead of Go protobuf payloads.
- Pouch record magic/format names instead of Go `LOGD` byte compatibility.
- `uint64_t` payload and file-size accounting, even where Go disk uses smaller
  physical fields.
- Pouch errors, structs, source files, and file names use Pouch terminology.
- Query-index artifacts may remain C-native derived artifacts as long as they
  are rebuildable from the authoritative namespace logstore.

Everything else must be treated as suspect until compared against Go disk.
The current global `.lockd/queue`, `.lockd/attachments`, `.lockd/leases`, and
`.lockd/namespace-config` model is rejected.

## Work Rules For This Correction

- [ ] Update the spec before implementation whenever Go disk behavior is
  discovered that changes the plan.
- [ ] Implement the storage/keyspace correction as one coherent cutover, not
  as compatibility-preserving micro-fixes.
- [ ] Do not run benchmarks before the full representation is aligned.
- [ ] Do not run review before the full representation is aligned.
- [ ] Run `test-all` only after all alignment items in this TODO are
  implemented.
- [ ] After implementation and `test-all`, re-analyze pouch against Go disk and
  document every remaining divergence with a reason.

## Reference Files To Walk Before Coding

For each item below, inspect the Go implementation and map the behavior to
Pouch before editing the corresponding C code.

- [ ] `../lockd/internal/storage/storage.go`
  - `storage.Meta`, `MetaSummary`, `Lease`, `StateInfo`, attachment metadata,
    staged attachment metadata, state/object options, commit groups, and
    query-exclusion semantics.
  - Pouch obligation: preserve the same logical metadata fields in binary C
    metadata and projections. Metadata needed by hot paths must be available
    without reading payload bytes.

- [ ] `../lockd/internal/storage/disk/logstore_record.go`
  - Record families: meta put/delete, state put/delete, object put/delete,
    state link.
  - Record metadata: generation, modified time, etag, content type,
    descriptor, plaintext size, cipher/stored size, CRC.
  - State link payload: segment identity plus payload offset/length.
  - Pouch obligation: keep C-native binary format, but preserve each logical
    fact, validation rule, and replay purpose. Use `uint64_t` for payload
    lengths and offsets.

- [ ] `../lockd/internal/storage/disk/logstore.go`
  - Per-namespace logstore, per-namespace projections, read-file LRU,
    append loop, pending refs, commit groups, segment rolling, writer markers,
    single-writer refresh optimization, fsync batching.
  - Pouch obligation: namespace is the storage boundary. Do not encode
    namespace into keys under global side namespaces.

- [ ] `../lockd/internal/storage/disk/disk.go`
  - `StoreMeta`, `LoadMeta`, `WriteState`, `ReadState`, `Remove`,
    `PutObject`, `GetObject`, `DeleteObject`, `ListObjects`, CAS behavior,
    pending same-group visibility, key/file locks, queue notification hooks.
  - Pouch obligation: public pouch operations must map to these same storage
    API semantics at the boundary.

- [ ] `../lockd/internal/storage/disk/staging.go`
  - Staged state key shape `<key>/.staging/<txn>`, ordered multi-key locking,
    staged promotion by state link, staged tombstone cleanup, staged listing.
  - Pouch obligation: no staged payload copy on promotion; preserve link
    metadata and protect linked spans during compaction.

- [ ] `../lockd/internal/storage/disk/logstore_compaction.go`
  - Candidate selection, active segment exclusion, snapshot install, live link
    protection, validation drift, obsolete cleanup, delete grace.
  - Pouch obligation: compact by copying current stored spans and validating
    captured refs; never rewrite a full cache dump.

- [ ] `../lockd/internal/storage/attachments.go`
  - Attachment object keys and staged attachment object keys.
  - Pouch obligation: committed attachment payloads are namespace-local under
    `state/<key>/attachments/<id>`; staged payloads are namespace-local under
    `state/<key>/.staging/<txn>/attachments/<id>`.

- [ ] `../lockd/internal/queue/service.go`
  - Queue key normalization, queue message meta/payload paths, state paths,
    metadata-before/after payload behavior, ready cache, payload cleanup on
    metadata failure, crypto contexts.
  - Pouch obligation: queue metadata, payloads, leases, state, retry, ack/nack,
    DLQ where supported, and notification behavior must be namespace-local.

- [ ] `../lockd/internal/queue/keys.go`
  - Message and state lease key parsing for `q/<queue>/msg/<id>` and
    `q/<queue>/state/<id>`.
  - Pouch obligation: use the same relative key model so queue transaction
    marker application can pair message and state leases.

- [ ] `../lockd/internal/core/locks.go`
  - Acquire, keepalive, release, lease expiration, fencing token, transaction
    id, if-not-exists behavior, staged commit/rollback, and metadata mutation.
  - Pouch obligation: leases must be target-key metadata in the same namespace,
    not separate global objects.

- [ ] `../lockd/internal/core/update.go`
  - Update and mutation behavior, staged metadata fields, state descriptors,
    query/index-visible state changes.
  - Pouch obligation: update must preserve Go disk state/meta/staged invariants
    through pouch binary metadata.

- [ ] `../lockd/internal/core/txn*.go`
  - Transaction records, decision markers, participant application, queue
    marker coupling, rollback/commit semantics.
  - Pouch obligation: keep true transaction decision/control namespaces only
    where Go disk uses them; do not use reserved namespaces for queue,
    attachment, namespace config, or leases.

- [ ] `../lockd/namespaces/config_store.go`
  - Namespace config key `config/namespace.pb`, cache TTL, load/save CAS and
    crypto behavior.
  - Pouch obligation: store pouch namespace config in the configured namespace
    under `config/namespace` or an explicitly documented C-native suffix, never
    in `.lockd/namespace-config`.

## Slice 1: Keyspace Cutover

The first implementation slice removes the rejected global side namespaces.
This is the highest-priority correctness issue.

- [ ] Delete the `.lockd/queue` durable model.
  - Queue message metadata object key: `q/<queue>/msg/<id>.meta`.
  - Queue payload object key: `q/<queue>/msg/<id>.bin`.
  - Queue workflow state object key: `q/<queue>/state/<id>.json`.
  - Queue message lease metadata key: `q/<queue>/msg/<id>`.
  - Queue workflow state lease metadata key: `q/<queue>/state/<id>`.
  - Queue DLQ keys, if implemented, follow Go shape under
    `q/<queue>/dlq/msg/<id>.meta`, `q/<queue>/dlq/msg/<id>.bin`, and
    `q/<queue>/dlq/state/<id>.json`.
  - All queue records are written in the caller namespace.
  - Namespace must be an argument to the logstore call, not encoded into the
    storage key.
  - Queue rows are internal/query-hidden and excluded from state scans,
    indexed document queries, full-text indexing, and get-public.

- [ ] Delete the `.lockd/attachments` durable model.
  - Committed attachment key: `state/<key>/attachments/<id>`.
  - Staged attachment key:
    `state/<key>/.staging/<txn>/attachments/<id>`.
  - All attachment payloads are written in the caller namespace.
  - Attachment metadata must remain attached to the state key metadata model:
    committed attachments in current metadata, staged attachments and staged
    deletes in staged metadata.
  - Direct public state reads must not expose internal attachment rows.

- [ ] Delete the `.lockd/namespace-config` durable model.
  - Config key is namespace-local: `config/namespace`.
  - If a C suffix is chosen, it must be documented and consistently hidden.
  - Config rows are query-hidden and excluded from public scans/indexes.
  - Config cache behavior must match Go intent: default fallback on missing
    config, short cache TTL if present, CAS on update.

- [ ] Delete the `.lockd/leases` durable model.
  - Active lease state belongs to the target key's metadata in the target
    namespace.
  - Pouch metadata must store at least lease id, owner, expiry, fencing token,
    txn id, explicit-transaction flag, and any staged fields needed by release.
  - Acquire/keepalive/release/update must read and mutate the same target
    metadata record used for state versioning and staged state.
  - Queue message leases use target key `q/<queue>/msg/<id>`.
  - Queue workflow state leases use target key `q/<queue>/state/<id>`.
  - Message/state lease pairing for transaction marker application must follow
    Go `queue.ParseMessageLeaseKey` / `ParseStateLeaseKey` semantics.

- [ ] Keep only true control namespaces.
  - Transaction records and decision markers may use reserved control
    namespaces when the Go reference does so.
  - Reserved namespaces must not contain user namespace queue records,
    attachment payloads, namespace config, or lease tables.

Acceptance:

- [ ] `rg` finds no durable uses of `.lockd/queue`,
  `.lockd/attachments`, `.lockd/leases`, or `.lockd/namespace-config`.
- [ ] Public APIs cannot create user state keys that collide with internal
  queue/config/attachment/staging/lease key families.
- [ ] Public scans, indexed queries, full-text queries, and get-public exclude
  every internal row by metadata and key policy.
- [ ] Reopen rebuilds queue, attachment, config, and lease projections from the
  caller namespace logstore records.

## Slice 2: Metadata Model Alignment

- [ ] Replace separate lease records with target-key metadata fields.
  - Persist lease fields in binary metadata or in a metadata payload attached
    to the target key record.
  - Cache hot lease fields in projections so acquire/release does not parse a
    user JSON payload.
  - CAS metadata by etag/generation with the same observable conflict behavior
    as Go disk.

- [ ] Preserve Go `storage.Meta` logical fields in Pouch metadata.
  - `Version`, `PublishedVersion`, `StateETag`, `UpdatedAtUnix`,
    `FencingToken`, `StateDescriptor`, `StatePlaintextBytes`, attributes,
    attachments, staged txn id, staged version, staged state etag, staged
    descriptor, staged plaintext bytes, staged attributes, staged remove,
    staged attachments, staged attachment deletes, staged clear flag.
  - Pouch may encode them as C-native binary fields instead of protobuf/JSON.

- [ ] Preserve hot summary behavior.
  - Query summaries must expose effective version, state etag, descriptor,
    plaintext byte count, and query exclusion without opening payloads.
  - Hidden/internal rows must be query-excluded independently of user metadata.

- [ ] Preserve byte accounting.
  - Plaintext bytes, stored bytes, cipher/transformed bytes, descriptor length,
    and payload CRC must be available from refs/metadata.
  - Use `uint64_t` for payload and file-size fields.

Acceptance:

- [ ] Acquire, keepalive, release, update, staged commit, rollback, get-public,
  list, scan, and query all read required metadata without parsing state JSON.
- [ ] Lease and staged metadata survive reopen.
- [ ] Metadata CAS behavior matches Go disk outcomes for missing, existing,
  stale etag, same-group pending, and different-group pending cases.

## Slice 3: State/Object/Attachment/Queue Record Family Alignment

- [ ] Keep logical meta/state/object distinctions even if Pouch stores them in
  one C-native segment record container.
  - State records hold JSON state payloads.
  - Object records hold attachments, queue payloads, namespace config, queue
    metadata payloads, transaction payloads, and any other object-like blobs.
  - Metadata records hold key metadata and hot fields.
  - State link records promote staged state without copying payload bytes.

- [ ] Queue enqueue must match Go semantics.
  - Normalize namespace and queue name.
  - Generate message id.
  - Write payload object in caller namespace first.
  - Count payload bytes while streaming.
  - Write metadata object in caller namespace second.
  - If metadata write fails, delete the payload object.
  - Ready-cache/update-notify behavior must be preserved or documented as a
    C-local equivalent.

- [ ] Queue delivery and stateful workflow must match Go semantics.
  - Dequeue observes visibility, TTL, max attempts, attempts, failure attempts,
    and FIFO ordering.
  - Ack/nack/extend validate active message lease id, fencing token, txn id,
    status, and expiry.
  - Stateful queue operations maintain paired message and state leases under
    `q/<queue>/msg/<id>` and `q/<queue>/state/<id>`.
  - Transaction decision markers apply to both paired lease keys.

- [ ] Attachment operations must match Go semantics.
  - Put/list/get/delete attachment APIs operate through object records under
    the state key attachment prefix.
  - Staged attachments are attached to the target key's staged metadata and
    promoted or discarded with release/transaction outcome.
  - Attachment bytes compare exactly across write/read.

Acceptance:

- [ ] Queue, attachment, and object payloads stream through the logstore; no
  full payload materialization is hidden behind streaming APIs.
- [ ] Public queue and attachment tests use public pouch APIs only.
- [ ] Multi-segment reopen preserves queue, attachment, and object behavior.

## Slice 4: Staging, Transactions, And Decision Markers

- [ ] Preserve Go staged state key shape `<key>/.staging/<txn>` for state.
- [ ] Preserve Go staged attachment key shape
  `state/<key>/.staging/<txn>/attachments/<id>`.
- [ ] Promote staged state by state link to the staged payload span.
- [ ] Tombstone staged state after successful promotion.
- [ ] Discard staged state by delete/tombstone.
- [ ] Use ordered multi-key locking for destination and staged keys.
- [ ] Transaction decision records remain in true control namespaces only.
- [ ] Transaction participant application commits or rolls back staged state,
  staged removes, staged attachments, attachment deletes, queue ack/nack
  effects, and metadata changes consistently.
- [ ] Queue message/state paired marker application follows Go disk.

Acceptance:

- [ ] No staged promotion copies large payload bytes.
- [ ] Live links protect source segment/snapshot spans from compaction cleanup.
- [ ] Transaction commit/rollback behavior matches Go disk for state,
  attachments, queue message leases, and queue state leases.

## Slice 5: Logstore Lifecycle And Performance-Critical Mechanics

- [ ] Keep per-namespace segment/snapshot/manifest ownership.
- [ ] Keep active segment rolling at configured default segment size.
- [ ] Replay installed snapshot first, then non-obsolete segments in order.
- [ ] Track last good offsets and repair crash-truncated active tails.
- [ ] Use writer markers and single-writer refresh optimization.
- [ ] Use bounded read-file LRU for segment/snapshot readers.
- [ ] Use append batching and cross-operation commit groups.
- [ ] Publish refs only after grouped sync succeeds, except documented
  same-operation pending visibility.
- [ ] Propagate group fsync failure to every affected operation.

Acceptance:

- [ ] No user-data surface bypasses the namespace logstore lifecycle.
- [ ] The implementation has no per-mutation fsync-only hot path where a Go
  disk equivalent batches syncs.
- [ ] Reopen after crash-tail scenarios either recovers to last good record or
  fails closed according to spec.

## Slice 6: Crypto And Compression Placement

- [ ] Crypto and compression apply at the log payload boundary.
- [ ] Compression runs before encryption; decompression runs after decryption.
- [ ] Root mode cannot switch between plaintext, crypto, compression, and
  crypto+compression without an explicit future migration feature.
- [ ] Encrypt production data at rest: state payloads, object/attachment
  payloads, queue payloads, queue metadata, namespace config, and transaction
  payloads containing production data.
- [ ] Preserve plaintext byte count, stored byte count, descriptor, etag, and
  CRC in metadata/refs.
- [ ] Compaction copies stored transformed bytes when descriptors remain valid.
- [ ] Tiny hot metadata records may skip compression, but must still be
  encrypted on crypto roots if they contain production data.

Acceptance:

- [ ] Wrong key, tampered descriptor, bad ciphertext, bad compression stream,
  and mixed root mode all fail closed.
- [ ] No plaintext production payload bytes appear in segment/snapshot files on
  crypto roots.

## Slice 7: Scan, Query, Full Text, And Derived Indexes

- [ ] Scans walk namespace-local metadata summaries and skip deleted, hidden,
  staged, reserved, and internal-prefix rows before opening payloads.
- [ ] Scans stream selector evaluation and document emission; no full corpus
  materialization.
- [ ] Indexed query never silently falls back to scan when the selected engine
  is index.
- [ ] Index flush is incremental and generation-aware.
- [ ] Query artifacts are derived, rebuildable, and never authoritative.
- [ ] Full-text indexes nested fields and whole-document aggregate text,
  including long summary/description/body fields.
- [ ] Internal queue/config/attachment/staging rows are excluded from exact,
  range, presence, text, trigram, temporal, and full-text indexes.

Acceptance:

- [ ] Query keys, query documents, scan, indexed selectors, and full-text
  selectors use public APIs.
- [ ] Indexed flush work scales with changed generations, not total corpus
  size.
- [ ] Query results match Go lockd disk by fields/content, while payload bytes
  for attachments and queue payloads match exactly.

## Slice 8: Compaction

- [ ] Candidate files are installed snapshot plus sealed non-obsolete segments.
- [ ] Active segment is excluded.
- [ ] Candidate refs are captured in deterministic key order from meta, state,
  object, queue, transaction, lease/metadata, and query-visible projections.
- [ ] Live state links into candidate files protect those files.
- [ ] Snapshot build streams stored spans; it does not materialize payloads.
- [ ] Captured refs are revalidated before install.
- [ ] Snapshot install is rename plus manifest update.
- [ ] Obsolete files are deleted only after grace and live-ref checks.

Acceptance:

- [ ] Compaction preserves state, metadata, leases, staged links, attachments,
  queue records, transaction records, byte counts, etags, descriptors, and
  query visibility.
- [ ] Validation drift abandons the temp snapshot.

## Slice 9: Dead Code And Terminology Cleanup

- [ ] Remove global side namespace helpers and call sites for queue,
  attachments, leases, and namespace config.
- [ ] Remove compatibility readers and old-layout branches for unreleased
  pouch formats.
- [ ] Remove external payload durability helpers.
- [ ] Remove string refs as durable or in-memory authority.
- [ ] Remove text hot-metadata parsers for storage facts.
- [ ] Remove full-cache compaction dump logic.
- [ ] Remove index rebuild-on-every-flush paths.
- [ ] Remove stale tests/fixtures/benchmarks that validate the rejected layout.
- [ ] Audit for `pouch-redesign`, `compat`, `company`, and disk-conflated
  terminology.

Acceptance:

- [ ] `rg` confirms rejected terminology and rejected `.lockd/*` globals are
  absent except in docs that explicitly name them as rejected.
- [ ] Pouch has one durable representation.

## Slice 10: Verification After Full Alignment

Run only after Slices 1-9 are implemented.

- [ ] Run `test-all`.
- [ ] If `test-all` fails, fix failures without weakening the storage
  invariants above.
- [ ] Re-run `test-all` until it passes.
- [ ] Re-analyze pouch against Go disk and update `docs/pouch-storage.md` with
  every accepted divergence.
- [ ] Only after passing functional verification, run benchmarks and parity
  gates in a later performance phase.
- [ ] Only after functional alignment and relevant verification, run review
  using the lifecycle-aligned command. Never run `review --uncommitted`.

## Later Performance Phase

- [ ] Restore/expand production benchmarks for Pouch plaintext, Pouch crypto,
  Pouch compression, Pouch crypto+compression, and Go lockd disk without
  crypto.
- [ ] Use realistic deep nested JSON documents with long text fields.
- [ ] Ensure datasets naturally produce many default-sized segments.
- [ ] Benchmark writes, reads, read-many, acquire/release/update, get public,
  staged promotion, queue roundtrips, attachments/objects, scan, indexed query,
  full-text query, index flush, compaction, reopen, and replay.
- [ ] Include abusive overcapacity workloads with churn, deletes, stale
  history, and mixed payload sizes.
- [ ] Add fast targeted benchmark commands under one minute for each
  performance-critical subsystem.
- [ ] Run full benchmark parity gates only after functional alignment passes.

Acceptance:

- [ ] Pouch plaintext beats Go disk without crypto on every production metric,
  unless an exception is explicitly accepted.
- [ ] Pouch crypto beats Go disk without crypto on every production metric,
  unless an exception is explicitly accepted.
- [ ] Any slower metric has root-cause analysis and either a fix or an
  explicit accepted exception.

## Logging Contract Follow-Up

- [ ] Preserve `docs/logging.md` as the liblockdc pslog contract.
- [ ] All Pouch logs use `sys=storage.pouch`.
- [ ] Public liblockdc client logs use `sys=client.lockd`.
- [ ] Pouch event names identify internal areas, for example
  `logstore.append`, `index.flush`, `compaction.start`, `queue.enqueue`.
- [ ] Logs use short readable fields such as `ns`, `key`, `msg_id`,
  `cur_*`, `lease_id`, `txn_id`, `attachment_id`, `fencing_token`,
  `payload_bytes`, `stored_bytes`, `plaintext_bytes`, and `elapsed_ms`.
- [ ] Logs do not contain state JSON, queue payloads, attachment/object bodies,
  full document text, crypto key material, transform secrets, or credentials.

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

The current divergence register, including operational/API and portability
differences that do not change the durable logstore model, is maintained in
`docs/pouch-storage.md#divergence-register`.

Everything else must be treated as suspect until compared against Go disk.
The current global `.lockd/queue`, `.lockd/attachments`, `.lockd/leases`, and
`.lockd/namespace-config` model is rejected.

## Hard Alignment Contract

This section is the production contract for the rest of this cutover.

- [x] Do not claim Pouch is aligned because the broad file layout or API shape
  resembles Go disk. Alignment means the same storage semantics, failure
  behavior, metadata authority, transaction participant model, and compaction
  safety properties.
- [x] Do not keep compatibility readers, old/new dispatch, legacy record
  variants, migration paths, or rejected pre-release representations. Pouch has
  not shipped.
- [x] Implement remaining representation corrections as one coherent cutover
  across the full storage surface. Do not land partial compatibility layers or
  small semantic stopgaps.
- [x] Treat Go disk as the default behavior. Any C-native divergence must be
  documented before or in the same implementation change and must explain why
  correctness, durability, and performance intent are preserved.
- [x] A TODO item may be checked only when the implementation, spec, and
  verification evidence all agree. Code shape alone is not proof.
- [x] Do not benchmark or run review loops until the full semantic cutover is in
  place. Functional verification happens after the whole implementation pass.

## Blocking Divergences From The Latest Audit

These items invalidate a claim of full Go-disk alignment until fixed.

- [x] Add distinct Pouch object record families.
  - Go disk has meta put/delete, state put/delete, object put/delete, and state
    link records.
  - Pouch currently represents object-like rows through state records.
  - Queue payloads, queue metadata objects, attachments, staged attachments, and
    namespace/config objects that map to Go `PutObject`/`DeleteObject`
    semantics must use object put/delete records.
  - Object metadata must preserve generation, modified time, etag, content type,
    transform descriptor, plaintext bytes, stored bytes, payload span, and CRC.

- [x] Align queue key and lease semantics exactly with Go disk.
  - Message metadata object: `q/<queue>/msg/<id>.meta`.
  - Message payload object: `q/<queue>/msg/<id>.bin`.
  - Message lease metadata target key: `q/<queue>/msg/<id>`.
  - Workflow state object: `q/<queue>/state/<id>.json`.
  - Workflow state lease metadata target key: `q/<queue>/state/<id>`.
  - Do not hex-encode queue names or message IDs unless the same logical key can
    still be parsed and paired exactly like Go `queue.ParseMessageLeaseKey` and
    `ParseStateLeaseKey`.
  - Queue message delivery must acquire and validate target-key lease metadata,
    not only fields embedded in the queue message document.
  - Single and batch dequeue paths must use the same target-key lease
    acquisition semantics and return a real fencing token that ack/nack/extend
    can validate.
  - Fencing tokens must advance and validate through the target-key metadata
    model, not by assuming a constant token.
  - Queue message/state participant parsers must accept only exact Go-shaped
    lease keys: `q/<queue>/msg/<id>` and `q/<queue>/state/<id>`.
  - Queue ack must delete message metadata/payload and state objects where Go
    disk deletes them; a durable live `acked` metadata row is rejected.

- [x] Align queue transaction participant application with Go disk.
  - Transaction decision application must interpret participant keys as queue
    message/state lease keys and apply commit/rollback with the same pairing and
    validation model as Go disk.
  - Message commit/rollback must validate and clear message lease metadata.
  - State commit/rollback must validate and clear state lease metadata.
  - Do not scan staged queue bases as a substitute for participant semantics.

- [x] Align compaction with Go disk safety semantics.
  - Candidate files are installed snapshots plus sealed non-obsolete segments.
  - The active segment is excluded.
  - Live links into candidate files protect those files.
  - Snapshot build streams stored spans and records the same hot metadata facts.
  - Validation drift aborts install.
  - Obsolete files are deleted only after grace/live-ref checks.
  - Immediate unlink of all segments through `max_segment_id` is rejected.
  - Compaction capture must store exact current record refs for payload,
    metadata-only, object, and tombstone records; file fingerprints or
    whole-cache dumps are not sufficient validation.

- [x] Align commit grouping/fsync batching with Go disk intent.
  - Public mutating operations that write multiple records must run inside a
    namespace commit group.
  - A commit group must avoid redundant fsyncs for the same physical file.
  - Queue enqueue, queue delivery, transaction decision application, attachment
    mutation, metadata mutation, and staged promotion must not regress to
    per-record sync behavior when they are one logical storage operation.
  - Default Pouch mutations use Go disk failover's `NoSync` boundary;
    `durable_sync=1` explicitly enables the root-scoped grouped `fdatasync`
    boundary and diagnostics.

## Work Rules For This Correction

- [x] Update the spec before implementation whenever Go disk behavior is
  discovered that changes the plan.
- [x] Implement the storage/keyspace correction as one coherent cutover, not
  as compatibility-preserving micro-fixes.
- [x] Do not run benchmarks before the full representation is aligned.
- [x] Do not run review before the full representation is aligned.
- [x] Run `test-all` only after all alignment items in this TODO are
  implemented.
- [x] Re-analyze Pouch against Go disk and document every remaining divergence
  with a reason.
  - 2026-07-31 follow-up source audit removed the superseded writer-scoped
    segment reader, uses fixed-width durable scalars on every supported ABI,
    publishes finalized active records only after their key/metadata bytes are
    complete, and takes client lease mutations through exact-key locks. The
    remaining divergence register entries are intentional representation or
    public-API differences. Default Pouch and the benchmark now use Go
    `failover`/`NoSync`; Pouch's stricter `durable_sync=1` policy is an
    explicit opt-in rather than a comparison divergence.
  - Broad verification remains deliberately deferred to the later functional
    verification phase; the focused checks and bounded concurrency benchmark
    do not replace `test-all`.

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
    namespace into keys under global side namespaces. Pouch deliberately uses
    its rolling active-segment tail, rather than Go's writer-marker polling, as
    the shared-writer refresh source.

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
  - Latest audit result: ack deletion and DLQ object movement are aligned at
    the durable keyspace level. Pouch performs DLQ movement synchronously on
    terminal non-transactional nack because it does not have Go's ready-cache
    worker; this is documented as an accepted C-local timing divergence.

- [x] `../lockd/internal/queue/keys.go`
  - Message and state lease key parsing for `q/<queue>/msg/<id>` and
    `q/<queue>/state/<id>`.
  - Pouch obligation: use the same relative key model so queue transaction
    marker application can pair message and state leases.
  - Latest audit result: Pouch now rejects malformed queue-looking participant
    keys that contain extra path components and reserves every direct public
    `q/` state key.

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

- [x] `../lockd/internal/core/txn*.go`
  - Transaction records, decision markers, participant application, queue
    marker coupling, rollback/commit semantics.
  - Pouch obligation: keep true transaction decision/control namespaces only
    where Go disk uses them (`.txns` / `.txn-decisions`); do not use reserved
    namespaces for queue, attachment, namespace config, or leases.

- [x] `../lockd/internal/tccluster/store.go`
  - Cluster lease namespace/key topology, endpoint normalization, active
    membership response shape, and hidden object storage.
  - Pouch obligation: use Go's `.lockd` namespace and
    `tc-cluster/leases/` key prefix. Because the current public C API exposes
    only `self_endpoint` and no explicit identity or TTL, Pouch stores the
    singleton local lease at `tc-cluster/leases/self` with no expiry. Do not
    reintroduce `.lockd/tc-cluster`.

- [x] `../lockd/internal/tcrm/store.go`
  - RM membership registry namespace/key topology, trim/dedupe/sort merge
    behavior, and delete-on-empty semantics.
  - Pouch obligation: use a single hidden object in `.lockd` at
    `tc-rm-members` with C-native binary payload and CAS by Pouch version. Do
    not store one object per endpoint and do not reintroduce `.lockd/tc-rm`.

- [x] `../lockd/namespaces/config_store.go`
  - Namespace config key `config/namespace.pb`, cache TTL, load/save CAS and
    crypto behavior.
  - Pouch obligation: store pouch namespace config in the configured namespace
    under `config/namespace` or an explicitly documented C-native suffix, never
    in `.lockd/namespace-config`.
  - Latest audit result: Pouch uses namespace-local `config/namespace` as a
    query-hidden object, returns empty etag for default/missing config, returns
    object etag for stored config, and enforces `if_etag` CAS on updates.
    Pouch does not use Go's protobuf suffix or cache implementation; those are
    documented C-native/API divergences.

## Slice 1: Keyspace Cutover

The first implementation slice removes the rejected global side namespaces.
This is the highest-priority correctness issue.

- [x] Delete the `.lockd/queue` durable model.
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

- [x] Delete the `.lockd/attachments` durable model.
  - Committed attachment key: `state/<key>/attachments/<id>`.
  - Staged attachment key:
    `state/<key>/.staging/<txn>/attachments/<id>`.
  - All attachment payloads are written in the caller namespace.
  - Attachment metadata must remain attached to the state key metadata model:
    committed attachments in current metadata, staged attachments and staged
    deletes in staged metadata.
  - Direct public state reads must not expose internal attachment rows.

- [x] Delete the `.lockd/namespace-config` durable model.
  - Config key is namespace-local: `config/namespace`.
  - If a C suffix is chosen, it must be documented and consistently hidden.
  - Config rows are query-hidden and excluded from public scans/indexes.
  - Config cache behavior must match Go intent: default fallback on missing
    config, short cache TTL if present, CAS on update.

- [x] Delete the `.lockd/leases` durable model.
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

- [x] Keep only true control namespaces.
  - Transaction records and decision markers may use reserved control
    namespaces when the Go reference does so.
  - Transaction-coordinator leader, cluster, and RM membership records use
    Go-aligned `.lockd` control object keys:
    `tc/leader`, `tc-cluster/leases/self`, and `tc-rm-members`.
  - Reserved namespaces must not contain user namespace queue records,
    attachment payloads, namespace config, or lease tables.

Acceptance:

- [x] `rg` finds no durable uses of `.lockd/queue`,
  `.lockd/attachments`, `.lockd/leases`, or `.lockd/namespace-config`.
- [x] `rg` finds no durable uses of Pouch-only `.lockd/tc`,
  `.lockd/tc-cluster`, or `.lockd/tc-rm`; TC control records use `.lockd`
  object keys matching the Go reference topology.
- [x] Public APIs cannot create user state keys that collide with internal
  queue/config/attachment/staging/lease key families.
  - Direct public state APIs reject all `q/` keys.
  - Direct public state APIs reject `state/<key>/attachments/<id>` object key
    shapes.
  - Queue state lease keys are accepted only through queue/state lease-ref
    validation, not as public user document keys.
- [x] Public scans, indexed queries, full-text queries, and get-public exclude
  every internal row by metadata and key policy.
- [x] Reopen rebuilds queue, attachment, config, and lease projections from the
  caller namespace logstore records.

## Slice 2: Metadata Model Alignment

- [x] Replace separate lease records with target-key metadata fields.
  - Persist lease fields in binary metadata or in a metadata payload attached
    to the target key record.
  - Cache hot lease fields in projections so acquire/release does not parse a
    user JSON payload.
  - CAS metadata by etag/generation with the same observable conflict behavior
    as Go disk.

- [x] Preserve Go `storage.Meta` logical fields in Pouch metadata.
  - `Version`, `PublishedVersion`, `StateETag`, `UpdatedAtUnix`,
    `FencingToken`, `StateDescriptor`, `StatePlaintextBytes`, attributes,
    attachments, staged txn id, staged version, staged state etag, staged
    descriptor, staged plaintext bytes, staged attributes, staged remove,
    staged attachments, staged attachment deletes, staged clear flag.
  - Pouch may encode them as C-native binary fields instead of protobuf/JSON.

- [x] Preserve hot summary behavior.
  - Query summaries must expose effective version, state etag, descriptor,
    plaintext byte count, and query exclusion without opening payloads.
  - Hidden/internal rows must be query-excluded independently of user metadata.

- [x] Preserve byte accounting.
  - Plaintext bytes, stored bytes, cipher/transformed bytes, descriptor length,
    and payload CRC must be available from refs/metadata.
  - Use `uint64_t` for payload and file-size fields.

Acceptance:

- [x] Acquire, keepalive, release, update, staged commit, rollback, get-public,
  list, scan, and query all read required metadata without parsing state JSON.
- [x] Lease and staged metadata survive reopen.
- [x] Metadata CAS behavior matches Go disk outcomes for missing, existing,
  stale etag, same-group pending, and different-group pending cases.

## Slice 3: State/Object/Attachment/Queue Record Family Alignment

- [x] Keep logical meta/state/object distinctions even if Pouch stores them in
  one C-native segment record container.
  - State records hold JSON state payloads.
  - Object records hold attachments, queue payloads, namespace config, queue
    metadata payloads, transaction payloads, and any other object-like blobs.
  - Metadata records hold key metadata and hot fields.
  - State link records promote staged state without copying payload bytes.

- [x] Queue enqueue must match Go semantics.
  - Normalize namespace and queue name.
  - Generate message id.
  - Write payload object in caller namespace first.
  - Count payload bytes while streaming.
  - Write metadata object in caller namespace second.
  - If metadata write fails, delete the payload object.
  - Ready-cache/update-notify behavior must be preserved or documented as a
    C-local equivalent.

- [x] Queue delivery and stateful workflow must match Go semantics.
  - Dequeue observes visibility, TTL, max attempts, attempts, failure attempts,
    and FIFO ordering.
  - Ack/nack/extend validate active message lease id, fencing token, txn id,
    status, and expiry.
  - Stateful queue operations maintain paired message and state leases under
    `q/<queue>/msg/<id>` and `q/<queue>/state/<id>`.
  - Transaction decision markers apply to both paired lease keys.

- [x] Attachment operations must match Go semantics.
  - Put/list/get/delete attachment APIs operate through object records under
    the state key attachment prefix.
  - Staged attachments are attached to the target key's staged metadata and
    promoted or discarded with release/transaction outcome.
  - Attachment bytes compare exactly across write/read.

Acceptance:

- [x] Queue, attachment, and object payloads stream through the logstore; no
  full payload materialization is hidden behind streaming APIs.
- [x] Public queue and attachment tests use public pouch APIs only.
- [x] Multi-segment reopen preserves queue, attachment, and object behavior.

## Slice 4: Staging, Transactions, And Decision Markers

- [x] Preserve Go staged state key shape `<key>/.staging/<txn>` for state.
- [x] Preserve Go staged attachment key shape
  `state/<key>/.staging/<txn>/attachments/<id>`.
- [x] Promote staged state by state link to the staged payload span.
- [x] Tombstone staged state after successful promotion.
- [x] Discard staged state by delete/tombstone.
- [x] Use ordered multi-key locking for destination and staged keys.
- [x] Transaction decision records remain in true control namespaces only.
- [x] Transaction participant application commits or rolls back staged state,
  staged removes, staged attachments, attachment deletes, queue ack/nack
  effects, and metadata changes consistently.
- [x] Queue message/state paired marker application follows Go disk.

Acceptance:

- [x] No staged promotion copies large payload bytes.
- [x] Live links protect source segment/snapshot spans from compaction cleanup.
- [x] Transaction commit/rollback behavior matches Go disk for state,
  attachments, queue message leases, and queue state leases.

## Slice 5: Logstore Lifecycle And Performance-Critical Mechanics

- [x] Keep per-namespace segment/snapshot/manifest ownership.
- [x] Keep active segment rolling at configured default segment size.
- [x] Replay installed snapshot first, then non-obsolete segments in order.
- [x] Track last good offsets and repair crash-truncated active tails.
- [x] Refresh shared-writer projections from the verified active-segment tail;
  single-writer mode reuses its local projection.
- [x] Use bounded read-file LRU for segment/snapshot readers.
- [x] Use append batching and cross-operation commit groups.
- [x] Publish refs only after grouped sync succeeds, except documented
  same-operation pending visibility.
- [x] Propagate group fsync failure to every affected operation.

Acceptance:

- [x] No user-data surface bypasses the namespace logstore lifecycle.
- [x] The implementation has no per-mutation fsync-only hot path where a Go
  disk equivalent batches syncs.
- [x] Reopen after crash-tail scenarios either recovers to last good record or
  fails closed according to spec.

## Slice 6: Crypto And Compression Placement

- [x] Crypto and compression apply at the log payload boundary.
- [x] Compression runs before encryption; decompression runs after decryption.
- [x] Root mode cannot switch between plaintext, crypto, compression, and
  crypto+compression without an explicit future migration feature.
- [x] Encrypt production data at rest: state payloads, object/attachment
  payloads, queue payloads, queue metadata, namespace config, and transaction
  payloads containing production data.
- [x] Preserve plaintext byte count, stored byte count, descriptor, etag, and
  CRC in metadata/refs.
- [x] Compaction copies stored transformed bytes when descriptors remain valid.
- [x] Tiny hot metadata records may skip compression, but must still be
  encrypted on crypto roots if they contain production data.

Acceptance:

- [x] Wrong key, tampered descriptor, bad ciphertext, bad compression stream,
  and mixed root mode all fail closed.
- [x] No plaintext production payload bytes appear in segment/snapshot files on
  crypto roots.

## Slice 7: Scan, Query, Full Text, And Derived Indexes

- [x] Scans walk namespace-local metadata summaries and skip deleted, hidden,
  staged, reserved, and internal-prefix rows before opening payloads.
- [x] Scans stream selector evaluation and document emission; no full corpus
  materialization.
- [x] Indexed query never silently falls back to scan when the selected engine
  is index.
- [x] Index flush is incremental and generation-aware.
- [x] Query artifacts are derived, rebuildable, and never authoritative.
- [x] Full-text indexes nested fields and whole-document aggregate text,
  including long summary/description/body fields.
- [x] Internal queue/config/attachment/staging rows are excluded from exact,
  range, presence, text, trigram, temporal, and full-text indexes.

Acceptance:

- [x] Query keys, query documents, scan, indexed selectors, and full-text
  selectors use public APIs.
- [x] Indexed flush work scales with changed generations, not total corpus
  size.
- [x] Query results match Go lockd disk by fields/content, while payload bytes
  for attachments and queue payloads match exactly.

## Slice 8: Compaction

- [x] Candidate files are installed snapshot plus sealed non-obsolete segments.
- [x] Active segment is excluded.
- [x] Candidate refs are captured in deterministic key order from meta, state,
  object, queue, transaction, lease/metadata, and query-visible projections.
- [x] Live state links into candidate files protect those files.
- [x] Snapshot build streams stored spans; it does not materialize payloads.
- [x] Captured refs are revalidated before install.
- [x] Snapshot install is rename plus manifest update.
- [x] Obsolete files are deleted only after grace and live-ref checks.

Acceptance:

- [x] Compaction preserves state, metadata, leases, staged links, attachments,
  queue records, transaction records, byte counts, etags, descriptors, and
  query visibility.
- [x] Validation drift abandons the temp snapshot.

## Slice 9: Dead Code And Terminology Cleanup

- [x] Remove global side namespace helpers and call sites for queue,
  attachments, leases, and namespace config.
- [x] Remove compatibility readers and old-layout branches for unreleased
  pouch formats.
- [x] Remove external payload durability helpers.
- [x] Remove string refs as durable or in-memory authority.
- [x] Remove text hot-metadata parsers for storage facts.
- [x] Remove full-cache compaction dump logic.
- [x] Remove index rebuild-on-every-flush paths.
- [x] Remove stale tests/fixtures/benchmarks that validate the rejected layout.
- [x] Audit for `pouch-redesign`, `compat`, `company`, and disk-conflated
  terminology.

Acceptance:

- [x] `rg` confirms rejected terminology and rejected `.lockd/*` globals are
  absent except in docs that explicitly name them as rejected.
- [x] Pouch has one durable representation.

## Slice 10: Verification After Full Alignment

Run only after Slices 1-9 are implemented.

- [x] Run `test-all`.
- [x] If `test-all` fails, fix failures without weakening the storage
  invariants above.
- [x] Re-run `test-all` until it passes.
- [x] Re-analyze pouch against Go disk and update `docs/pouch-storage.md` with
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

- [x] Preserve `docs/logging.md` as the liblockdc pslog contract.
- [x] All Pouch logs use `sys=storage.pouch`.
- [x] Public liblockdc client logs use `sys=client.lockd`.
- [x] Pouch event names identify internal areas, for example
  `logstore.write`, `index.flush`, `compaction.start`, `queue.enqueue`.
- [x] Logs use short readable fields such as `ns`, `key`, `msg_id`,
  `cur_*`, `lease_id`, `txn_id`, `attachment_id`, `fencing_token`,
  `payload_bytes`, `stored_bytes`, `plaintext_bytes`, and `elapsed_ms`.
- [x] Logs do not contain state JSON, queue payloads, attachment/object bodies,
  full document text, crypto key material, transform secrets, or credentials.
  - The Pouch logger test captures write/read events and asserts that a supplied
    state body never reaches pslog; all Pouch durable counters now preserve
    their `uint64_t` value through the shared field helper.

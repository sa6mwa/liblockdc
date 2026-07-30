# liblockdc TODO

## Pouch Proper Logstore Cutover

Pouch is an unreleased initial storage engine. There is no compatibility
contract for rejected pre-release layouts. The accepted target is documented in
`docs/pouch-storage.md`: a real segmented logstore with binary records,
binary hot metadata, structured refs, manifest lifecycle, Go-disk-equivalent
append/replay/compaction semantics, and C-native implementation choices.

This TODO is an implementation guide. Each slice says what to build and why.
The cutover implementation should be done as one coherent storage slice before
major test/benchmark iteration. Tests may be added with the implementation, but
do not enter micro-iteration until the full representation is in place.

## Slice 0: Reference Mapping And Design Authority

- [x] Replace the Pouch storage spec with an implementation-grade document that
  maps Go lockd disk semantics to Pouch C obligations.
  Why: the previous spec described the high-level target but left room for
  simplified structures that would underperform or diverge from Go disk.

- [x] Replace TODO with implementation slices that force a full proper
  logstore cutover and dead-code cleanup.
  Why: the old TODO treated partial current behavior as mostly complete even
  where it still missed core logstore mechanics.

- [x] During implementation, keep this doc and `docs/pouch-storage.md` current
  when an intentional C-local divergence is accepted.
  Why: divergence must be explicit and defensible, not accidental.

## Slice 1: Private Logstore Core

- [x] Introduce private logstore modules, or equivalent boundaries:
  `lc_pouch_namespace.[ch]`, `lc_pouch_record.[ch]`,
  `lc_pouch_state.c`, `lc_pouch_crypto.[ch]`, and query/index helpers.
  Why: public `lc_pouch` should become a receiver shell over one durable
  storage core, not a set of features each inventing storage behavior.

- [x] Move namespace lifecycle, segment selection, projection ownership, refs,
  replay, and segment read sources into the logstore core.
  Why: scan, query, state, objects, queue, transactions, crypto, compression,
  and compaction must all share the same source of truth.

- [x] Remove any naming that implies transition work, compatibility, company
  layers, or Go disk identity.
  Why: this is the initial Pouch implementation, not a redesign layer and not
  the Go disk engine.

Acceptance:

- [x] Public Pouch APIs compile against the new core boundary.
- [x] No public API or durable file name exposes Go disk terminology.
- [x] There is one Pouch storage representation in the codebase.

## Slice 2: Binary Record And Metadata Format

- [x] Define the Pouch record header in `lc_pouch_record.[ch]`:
  magic, initial format discriminator, record type, flags, `uint32_t` key
  length, `uint32_t` metadata length, `uint64_t` stored payload length,
  `uint32_t` payload CRC, and either `uint32_t` header CRC or reserved field.
  Why: physical traversal and corruption detection must be O(1), binary,
  bounded, and fuzzable.

- [x] Define record families: state put/delete, state link, state metadata,
  decision/control, and high-water records.
  Why: this captures the durable behavior Pouch needs without binding Pouch to
  Go's byte format, protobuf, or a separate object-record family.

- [x] Define binary metadata for every family:
  generation, modified timestamp, etag/id, content type where needed,
  plaintext byte count, stored byte count, descriptor bytes, query visibility,
  object class, queue class, transaction/lease markers, tombstone markers, and
  link facts where applicable.
  Why: replay, scan, list, CAS, compaction, and query flush must not parse JSON
  or text payloads to discover hot storage facts.

- [x] Implement strict encode/decode validation with overflow checks.
  Why: record decoding is a trust boundary and must be fuzzable.

Acceptance:

- [x] All durable storage facts currently encoded in text payloads or ref
  strings have binary metadata/ref equivalents.
- [x] No hot metadata path parses user JSON or diagnostic strings.
- [x] Header and metadata decode failures are actionable and covered by unit
  tests/fuzz targets after implementation is complete.

## Slice 3: Structured Refs And Bounded Readers

- [x] Replace durable/in-memory string refs such as
  `container@offset:length` with structured refs.
  Why: string refs are slow, overflow-prone, and too easy to treat as paths.

- [x] Store ref fields: family, key, segment/snapshot identity, record offset,
  payload offset, stored payload length, plaintext length, stored length, CRC,
  generation, etag, descriptor, flags, and optional validated link target.
  Why: reads, compaction, scan, and query need all of this without opening the
  payload.

- [x] Implement bounded segment/snapshot readers with an open-file LRU.
  Why: reads and scans must avoid repeated open/close cost and must never read
  outside a valid payload span.

- [x] Restrict link targets to manifested segment/snapshot names.
  Why: staged promotion and compaction links must not accept absolute paths,
  traversal, obsolete unknown files, or integer overflow.

Acceptance:

- [x] State, object, queue, and attachment reads stream through bounded
  logstore refs.
- [x] No state/object durability path depends on external payload files.
- [x] Link validation rejects malformed, unmanifested, and overflowed targets.

## Slice 4: Manifest, Markers, Segments, And Replay

- [x] Implement a namespace manifest with open, snapshot-install,
  obsolete-segment, obsolete-snapshot, and active-segment repair state.
  Why: active segment lifecycle and compaction install must be recoverable and
  incremental.

- [x] Implement writer markers and single-writer refresh behavior.
  Why: Pouch needs Go-disk-equivalent refresh semantics without unnecessary
  marker scans in single-writer mode.

- [x] Replay installed snapshot first, then non-obsolete segments in order.
  Why: projections must rebuild deterministically from durable logstore state.

- [x] Track last good read offsets and handle crash-truncated active tails.
  Why: crashes must not make a namespace unreadable, but partial records must
  never be applied.

- [x] Apply records by generation and family-specific semantics.
  Why: stale records must not resurrect older state, metadata, or object heads.

Acceptance:

- [x] Reopen rebuilds metadata, state, object, queue, transaction, lease,
  retention, and query-visible projections from records.
- [x] Crash-tail tests prove partial final records are ignored safely.
- [x] Bad CRC, impossible length, and invalid middle-record corruption fail
  according to documented policy.

## Slice 5: Append Coordinator, Commit Groups, And Fsync Batching

- [x] Replace external payload writes and ad hoc text records with the namespace
  state/logstore writer, structured spans, binary record prefixes, and
  operation-sized append batches.
  Why: every durable feature must share one segment/snapshot write path.

- [x] Batch small inline records into grouped writes.
  Why: write amplification must be closer to Go disk's optimized path.

- [x] Stream large payload writes directly from caller reader through transforms,
  hash/etag, and CRC into the segment.
  Why: Pouch must support large documents and objects without materializing
  payloads in memory.

- [x] Rewrite header/metadata prefix after final stored length, descriptor,
  etag, and CRC are known.
  Why: transforms can change stored size and descriptors are not known before
  streaming completes.

- [x] Implement pending record prefixes and staged same-operation visibility.
  Why: projection visibility must reflect durability, while staged operations
  can still promote known committed spans by link.

- [x] Add cross-operation commit groups and fsync-delay batching.
  Why: Go disk's durable group commit is part of the performance model for
  core write/acquire/update/release workloads. Pouch must not postpone that
  mechanism behind benchmark evidence.

Acceptance:

- [x] Write/update/acquire/release paths share the state/logstore append path.
- [x] A failed write/fsync fails every affected operation without publishing
  committed refs.
- [ ] Fast targeted benchmarks prove core write/acquire/update loops are not
  dominated by per-mutation fsync and are faster than Go disk.

## Slice 6: State, Metadata, Object, Attachment, Queue, Lease, And Transaction Cutover

- [x] Reattach public state APIs to logstore state records.
  Why: state JSON is durable log payload, not an external file.

- [x] Reattach public metadata APIs to metadata records.
  Why: list/public-state summary behavior must not require parsing payloads.

- [x] Reattach attachments and object payloads to logstore records.
  Why: binary production data needs the same append/replay/crypto/compaction
  semantics as state.

- [x] Reattach queue payloads and queue hot metadata to object/state records
  with binary queue metadata.
  Why: queue claim/list/retry/dead-letter paths must not parse arbitrary
  payload bytes and must be encrypted at rest when the root is encrypted.

- [x] Reattach lease and transaction metadata to binary metadata/state/object
  records.
  Why: locking and transaction correctness must survive replay without helper
  side formats.

- [x] Implement staged state promotion with state link records.
  Why: promotion of large staged payloads must be O(metadata) instead of copying
  the payload.

Acceptance:

- [x] Public acquire, release, update, mutate, get, get-public, attachment,
  queue, transaction, and staged-state APIs operate through the logstore core.
- [x] Reopen preserves all public API observable behavior.
- [x] No feature has a private durable side format outside the logstore unless
  explicitly documented as a derived artifact.

## Slice 7: Crypto And Compression At The Log Payload Boundary

- [x] Move crypto/compression to streaming payload transforms in the append/read
  pipeline.
  Why: transforms belong at rest inside segment/snapshot payload spans.

- [x] Use stable logical AAD/material context: record class, namespace, key,
  generation, and transform metadata.
  Why: compaction must be able to copy stored bytes without decrypt/re-encrypt
  when descriptors remain valid; physical offsets are the wrong default
  context.

- [x] Enforce root-mode invariants for plaintext, crypto, compression, and
  crypto+compression.
  Why: mixing plaintext and transformed records is not supported in the initial
  release.

- [x] Preserve plaintext byte count, stored byte count, descriptor, etag, and
  CRC in metadata/refs.
  Why: hot paths, compaction, and public result metadata need these values
  without reading payloads.

- [x] Apply compression before encryption and decompression after decryption.
  Why: this is the only order that can both compress plaintext effectively and
  encrypt stored bytes.

- [x] Make compaction copy stored transformed bytes when descriptors remain
  valid.
  Why: compaction must not scale with crypto/decompression cost for every live
  payload.

Acceptance:

- [x] Wrong key, tampered descriptor, tampered ciphertext, and corrupt zlib data
  fail closed.
- [x] Encrypted roots do not expose plaintext user payload bytes in segment or
  snapshot files.
- [x] Compaction of crypto/compressed data preserves data while avoiding
  unnecessary transform reminting.

## Slice 8: Scan, Query Index, Full Text, And Flush

- [x] Attach scan summaries to logstore projections.
  Why: scans should skip hidden/staged/reserved/deleted rows before opening
  payloads.

- [x] Keep scan document validation streaming over bounded payload refs.
  Why: large documents must not be accumulated during scan.

- [x] Keep indexed execution indexed when the selected engine is index.
  Why: falling back to scan violates the engine contract and hides performance
  bugs.

- [x] Implement generation-aware incremental index flush.
  Why: rebuilding whole sidecars on every flush is the exact failure mode Go
  disk avoided.

- [x] Ensure full-text search indexes text across the entire JSON document,
  including nested fields and long summary/description/body fields.
  Why: benchmarks and production use need realistic document shapes.

- [x] Make index sidecars derived and rebuildable from logstore projections.
  Why: corruption should trigger rebuild, not data loss.

Acceptance:

- [x] Query keys, query documents, scan, indexed selectors, and full-text
  selectors all use public APIs.
- [x] Index flush work is proportional to changed generations, not total corpus
  size.
- [ ] Benchmarks show indexed query/flush faster than Go disk on equivalent
  workloads, including crypto roots.

## Slice 9: Compaction

- [x] Implement Go-disk-shaped compaction capture.
  Why: compaction needs a stable candidate set, not a blind full-cache rewrite.

- [x] Candidate files include installed snapshot plus sealed non-obsolete
  segments and exclude the active segment.
  Why: active writes and compaction must not race over mutable tails.

- [x] Protect candidate files targeted by live state links.
  Why: a link can keep a segment/snapshot live even if its own head record is
  elsewhere.

- [x] Capture live metadata/state/object refs in deterministic key order.
  Why: snapshots should be reproducible and validation should be precise.

- [x] Build temp snapshots by streaming stored payload spans.
  Why: compaction must support large payloads and transformed bytes without
  materialization.

- [x] Validate that captured refs are still current before install.
  Why: foreground writes during compaction must not be lost.

- [x] Install snapshot by rename plus atomic manifest update, then mark obsolete
  files.
  Why: crash recovery must find either old state or new installed state.

- [x] Cleanup obsolete files after live-ref checks.
  Why: file deletion must be retryable and must not break linked payloads.

Acceptance:

- [x] Compaction preserves state, objects, attachments, queues, transactions,
  links, etags, byte counts, descriptors, and query visibility.
- [x] Validation drift abandons a temp snapshot without installing it.
- [x] Large multi-segment compaction benchmark is present and configurable.

## Slice 10: Dead Code And Terminology Cleanup

- [x] Delete rejected external payload durability code.
  Why: the initial Pouch release must have one storage representation.

- [x] Delete compatibility readers, old layout branches, stale version lineage,
  and transition docs.
  Why: unreleased baggage makes the implementation harder to reason about and
  test.

- [x] Delete or rewrite stale fixtures and benchmarks that target the rejected
  design.
  Why: tests must describe the accepted product, not old intermediate states.

- [x] Audit names for `pouch-redesign`, `compat`, `company`, disk-conflated
  terminology, and other temporary labels.
  Why: naming is part of the API and maintenance surface.

Acceptance:

- [x] `rg` confirms no rejected terminology remains except in historical commit
  messages or explicitly intentional docs.
- [x] Pouch code has no alternate old/new storage branches.
- [x] Worktree diff contains no unrelated drive-by refactors.

## Slice 11: Targeted Verification After Full Cutover

Run targeted verification only after Slices 1-10 are implemented.

- [x] Build the relevant Pouch unit target.
- [x] Run targeted state write/read/reopen/delete tests.
- [x] Run targeted public acquire/release/update/get/get-public tests.
- [x] Run targeted object/attachment tests.
- [x] Run targeted queue tests.
- [x] Run targeted transaction/staged-state tests.
- [x] Run targeted scan/query/index/full-text/flush tests.
- [x] Run targeted crypto/compression/crypto+compression tests.
- [x] Run targeted compaction tests.
- [x] Run targeted failure-mode tests for bad CRC, bad lengths, invalid refs,
  wrong key, corrupt descriptor, corrupt compression stream, and crash tails.

Why: this phase proves the full storage cutover before broader iteration.

## Slice 12: Fuzzing

- [ ] Build fuzz corpora for record headers, metadata, refs, state links,
  manifests, transform descriptors, scan/query selectors, sidecar rebuild, and
  lifecycle operations.
- [ ] Run fuzzing for plaintext roots.
- [ ] Run fuzzing for crypto roots.
- [ ] Run fuzzing for compression and crypto+compression roots where enabled.
- [ ] Run at least 30 seconds per fuzz unit before claiming coverage, and
  longer campaign runs before release.

Why: the new binary surface is a parser and storage trust boundary.

## Slice 13: Benchmarks And Parity Gates

- [x] Restore/expand production benchmarks for Pouch plaintext, Pouch crypto,
  Pouch compression where relevant, Pouch crypto+compression where relevant,
  and Go lockd disk without crypto.
- [x] Use realistic deep nested JSON documents with long text fields.
- [x] Ensure datasets naturally produce many default-sized segments.
- [x] Benchmark writes, reads, read-many, acquire/release/update, get public,
  staged promotion, queue roundtrips, attachments/objects, scan, indexed query,
  full-text query, index flush, compaction, reopen, and replay.
- [x] Include abusive overcapacity workloads with churn, deletes, stale history,
  and mixed payload sizes.
- [x] Add fast targeted benchmark commands that complete in under one minute,
  including rebuild time, for each performance-critical subsystem.
- [x] Add full benchmark parity gates after implementation stabilization.

Why: Pouch is required to beat Go lockd disk on production metrics, not just
selected read-only query cases.

Acceptance:

- [ ] Pouch plaintext beats Go disk without crypto on every production metric,
  unless an exception is explicitly accepted.
- [ ] Pouch crypto beats Go disk without crypto on every production metric,
  unless an exception is explicitly accepted.
- [ ] Pouch crypto stays near Pouch plaintext for metadata/index-heavy paths.
- [ ] Any slower metric has a root-cause analysis and an accepted fix or
  explicit exception.

## Slice 14: Full Verification, Review, Commit

- [ ] Run the full relevant liblockdc test suite.
- [ ] Run the full Pouch benchmark comparison.
- [ ] Run fuzz campaigns for the required minimum.
- [ ] Run the same review command used by the lc lifecycle skill.
- [ ] Resolve relevant review findings without weakening storage invariants.
- [ ] Run final benchmark parity gates.
- [ ] Inspect the worktree and classify dirty changes.
- [ ] Commit the coherent cutover with a Conventional Commit message.

Why: completion requires evidence: implementation, verification, benchmark
comparison, review, and clean commit discipline.

## Slice 15: pslog Logging Contract Alignment

- [x] Treat `docs/logging.md` as the liblockdc pslog event and field contract.
  Why: logging must be stable enough for operators and tests to rely on.

- [x] Align public liblockdc client logging to `sys=client.lockd`.
  Why: liblockdc is not the Go SDK and should not emit SDK subsystem identity.

- [x] Align all Pouch logging to `sys=storage.pouch`.
  Why: logstore, manifest, scan, index, crypto, compression, compaction,
  queue-backed storage, attachment/object storage, and maintenance internals
  are internal areas of one Pouch storage engine.

- [x] Remove `component` and `subsystem` fields from liblockdc-managed logs.
  Why: `sys` is the subsystem field and duplicate subsystem dimensions make
  log queries inconsistent.

- [x] Remove redundant event prefixes that duplicate `sys`.
  Why: events should be scoped below the subsystem, for example
  `acquire.start` under `sys=client.lockd` and `compaction.start` under
  `sys=storage.pouch`.

- [x] Standardize field names to the approved short-readable layout:
  `ns`, `msg_id`, `cur_*`, `lease_id`, `txn_id`, `attachment_id`,
  `fencing_token`, `payload_bytes`, `stored_bytes`, `plaintext_bytes`,
  `elapsed_ms`, and explicit unit suffixes such as `_bytes`, `_s`, and `_ms`.
  Why: fields should be compact but understandable without a legend.

- [x] Add or update logging tests that assert subsystem fields, event names,
  field names, and production-data redaction.
  Why: the logging contract is observable behavior and should not drift.

Acceptance:

- [x] All liblockdc-managed client logs use `sys=client.lockd`.
- [x] All Pouch logs use `sys=storage.pouch`.
- [x] No liblockdc-managed log emits `component` or `subsystem`.
- [x] No liblockdc-managed log event repeats its `sys` identity.
- [x] No logs contain state JSON, queue payloads, attachment/object bodies,
  full document text, crypto key material, transform secrets, or credentials.

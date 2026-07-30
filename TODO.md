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

- [ ] During implementation, keep this doc and `docs/pouch-storage.md` current
  when an intentional C-local divergence is accepted.
  Why: divergence must be explicit and defensible, not accidental.

## Slice 1: Private Logstore Core

- [ ] Introduce private logstore modules, or equivalent boundaries:
  `lc_pouch_logstore.[ch]`, `lc_pouch_record.[ch]`,
  `lc_pouch_manifest.[ch]`, `lc_pouch_compaction.[ch]`, and
  transform helpers.
  Why: public `lc_pouch` should become a receiver shell over one durable
  storage core, not a set of features each inventing storage behavior.

- [ ] Move namespace lifecycle, segment selection, projection ownership, refs,
  replay, and segment read sources into the logstore core.
  Why: scan, query, state, objects, queue, transactions, crypto, compression,
  and compaction must all share the same source of truth.

- [ ] Remove any naming that implies transition work, compatibility, company
  layers, or Go disk identity.
  Why: this is the initial Pouch implementation, not a redesign layer and not
  the Go disk engine.

Acceptance:

- [ ] Public Pouch APIs compile against the new core boundary.
- [ ] No public API or durable file name exposes Go disk terminology.
- [ ] There is one Pouch storage representation in the codebase.

## Slice 2: Binary Record And Metadata Format

- [ ] Define the Pouch record header in `lc_pouch_record.[ch]`:
  magic, initial format discriminator, record type, flags, `uint32_t` key
  length, `uint32_t` metadata length, `uint64_t` stored payload length,
  `uint32_t` payload CRC, and either `uint32_t` header CRC or reserved field.
  Why: physical traversal and corruption detection must be O(1), binary,
  bounded, and fuzzable.

- [ ] Define record families: metadata put/delete, state put/delete, state
  link, object put/delete.
  Why: this matches Go disk's durable storage families without binding Pouch to
  Go's byte format or protobuf.

- [ ] Define binary metadata for every family:
  generation, modified timestamp, etag/id, content type where needed,
  plaintext byte count, stored byte count, descriptor bytes, query visibility,
  object class, queue class, transaction/lease markers, tombstone markers, and
  link facts where applicable.
  Why: replay, scan, list, CAS, compaction, and query flush must not parse JSON
  or text payloads to discover hot storage facts.

- [ ] Implement strict encode/decode validation with overflow checks.
  Why: record decoding is a trust boundary and must be fuzzable.

Acceptance:

- [ ] All durable storage facts currently encoded in text payloads or ref
  strings have binary metadata/ref equivalents.
- [ ] No hot metadata path parses user JSON or diagnostic strings.
- [ ] Header and metadata decode failures are actionable and covered by unit
  tests/fuzz targets after implementation is complete.

## Slice 3: Structured Refs And Bounded Readers

- [ ] Replace durable/in-memory string refs such as
  `container@offset:length` with structured refs.
  Why: string refs are slow, overflow-prone, and too easy to treat as paths.

- [ ] Store ref fields: family, key, segment/snapshot identity, record offset,
  payload offset, stored payload length, plaintext length, stored length, CRC,
  generation, etag, descriptor, flags, and optional validated link target.
  Why: reads, compaction, scan, and query need all of this without opening the
  payload.

- [ ] Implement bounded segment/snapshot readers with an open-file LRU.
  Why: reads and scans must avoid repeated open/close cost and must never read
  outside a valid payload span.

- [ ] Restrict link targets to manifested segment/snapshot names.
  Why: staged promotion and compaction links must not accept absolute paths,
  traversal, obsolete unknown files, or integer overflow.

Acceptance:

- [ ] State, object, queue, and attachment reads stream through bounded
  logstore refs.
- [ ] No state/object durability path depends on external payload files.
- [ ] Link validation rejects malformed, unmanifested, and overflowed targets.

## Slice 4: Manifest, Markers, Segments, And Replay

- [ ] Implement a namespace manifest with open, seal, snapshot-install,
  obsolete-segment, and obsolete-snapshot entries.
  Why: active segment lifecycle and compaction install must be recoverable and
  incremental.

- [ ] Implement writer markers and single-writer refresh behavior.
  Why: Pouch needs Go-disk-equivalent refresh semantics without unnecessary
  marker scans in single-writer mode.

- [ ] Replay installed snapshot first, then non-obsolete segments in order.
  Why: projections must rebuild deterministically from durable logstore state.

- [ ] Track last good read offsets and handle crash-truncated active tails.
  Why: crashes must not make a namespace unreadable, but partial records must
  never be applied.

- [ ] Apply records by generation and family-specific semantics.
  Why: stale records must not resurrect older state, metadata, or object heads.

Acceptance:

- [ ] Reopen rebuilds metadata, state, object, queue, transaction, lease,
  retention, and query-visible projections from records.
- [ ] Crash-tail tests prove partial final records are ignored safely.
- [ ] Bad CRC, impossible length, and invalid middle-record corruption fail
  according to documented policy.

## Slice 5: Append Coordinator, Commit Groups, And Fsync Batching

- [ ] Replace per-mutation ad hoc writes with a namespace append coordinator.
  Why: core lockd operations cannot pay one full fsync per logical mutation.

- [ ] Batch small inline records into grouped writes.
  Why: write amplification must be closer to Go disk's optimized path.

- [ ] Stream large payload writes directly from caller reader through transforms,
  hash/etag, and CRC into the segment.
  Why: Pouch must support large documents and objects without materializing
  payloads in memory.

- [ ] Rewrite header/metadata prefix after final stored length, descriptor,
  etag, and CRC are known.
  Why: transforms can change stored size and descriptors are not known before
  streaming completes.

- [ ] Implement commit groups with pending refs and fsync batching.
  Why: projection visibility must reflect durability, while same-group CAS and
  staged operations can still reason about pending writes.

- [ ] Make batching parameters configurable: commit max operations, fsync delay,
  segment target size, and no-sync behavior where supported.
  Why: benchmark and production tuning must not require code changes.

Acceptance:

- [ ] Write/update/acquire/release paths share grouped append/commit behavior.
- [ ] A failed write/fsync fails every affected operation without publishing
  committed refs.
- [ ] Fast targeted benchmarks prove core write/acquire/update loops are not
  dominated by per-mutation fsync.

## Slice 6: State, Metadata, Object, Attachment, Queue, Lease, And Transaction Cutover

- [ ] Reattach public state APIs to logstore state records.
  Why: state JSON is durable log payload, not an external file.

- [ ] Reattach public metadata APIs to metadata records.
  Why: list/public-state summary behavior must not require parsing payloads.

- [ ] Reattach attachments and object payloads to object records.
  Why: binary production data needs the same append/replay/crypto/compaction
  semantics as state.

- [ ] Reattach queue payloads and queue hot metadata to object/state records
  with binary queue metadata.
  Why: queue claim/list/retry/dead-letter paths must not parse arbitrary
  payload bytes and must be encrypted at rest when the root is encrypted.

- [ ] Reattach lease and transaction metadata to binary metadata/state/object
  records.
  Why: locking and transaction correctness must survive replay without helper
  side formats.

- [ ] Implement staged state promotion with state link records.
  Why: promotion of large staged payloads must be O(metadata) instead of copying
  the payload.

Acceptance:

- [ ] Public acquire, release, update, mutate, get, get-public, attachment,
  queue, transaction, and staged-state APIs operate through the logstore core.
- [ ] Reopen preserves all public API observable behavior.
- [ ] No feature has a private durable side format outside the logstore unless
  explicitly documented as a derived artifact.

## Slice 7: Crypto And Compression At The Log Payload Boundary

- [ ] Move crypto/compression to streaming payload transforms in the append/read
  pipeline.
  Why: transforms belong at rest inside segment/snapshot payload spans.

- [ ] Use stable logical AAD/material context: record class, namespace, key,
  generation, and transform metadata.
  Why: compaction must be able to copy stored bytes without decrypt/re-encrypt
  when descriptors remain valid; physical offsets are the wrong default
  context.

- [ ] Enforce root-mode invariants for plaintext, crypto, compression, and
  crypto+compression.
  Why: mixing plaintext and transformed records is not supported in the initial
  release.

- [ ] Preserve plaintext byte count, stored byte count, descriptor, etag, and
  CRC in metadata/refs.
  Why: hot paths, compaction, and public result metadata need these values
  without reading payloads.

- [ ] Apply compression before encryption and decompression after decryption.
  Why: this is the only order that can both compress plaintext effectively and
  encrypt stored bytes.

- [ ] Make compaction copy stored transformed bytes when descriptors remain
  valid.
  Why: compaction must not scale with crypto/decompression cost for every live
  payload.

Acceptance:

- [ ] Wrong key, tampered descriptor, tampered ciphertext, and corrupt zlib data
  fail closed.
- [ ] Encrypted roots do not expose plaintext user payload bytes in segment or
  snapshot files.
- [ ] Compaction of crypto/compressed data preserves data while avoiding
  unnecessary transform reminting.

## Slice 8: Scan, Query Index, Full Text, And Flush

- [ ] Attach scan summaries to logstore projections.
  Why: scans should skip hidden/staged/reserved/deleted rows before opening
  payloads.

- [ ] Keep scan document validation streaming over bounded payload refs.
  Why: large documents must not be accumulated during scan.

- [ ] Keep indexed execution indexed when the selected engine is index.
  Why: falling back to scan violates the engine contract and hides performance
  bugs.

- [ ] Implement generation-aware incremental index flush.
  Why: rebuilding whole sidecars on every flush is the exact failure mode Go
  disk avoided.

- [ ] Ensure full-text search indexes text across the entire JSON document,
  including nested fields and long summary/description/body fields.
  Why: benchmarks and production use need realistic document shapes.

- [ ] Make index sidecars derived and rebuildable from logstore projections.
  Why: corruption should trigger rebuild, not data loss.

Acceptance:

- [ ] Query keys, query documents, scan, indexed selectors, and full-text
  selectors all use public APIs.
- [ ] Index flush work is proportional to changed generations, not total corpus
  size.
- [ ] Benchmarks show indexed query/flush faster than Go disk on equivalent
  workloads, including crypto roots.

## Slice 9: Compaction

- [ ] Implement Go-disk-shaped compaction capture.
  Why: compaction needs a stable candidate set, not a blind full-cache rewrite.

- [ ] Candidate files include installed snapshot plus sealed non-obsolete
  segments and exclude the active segment.
  Why: active writes and compaction must not race over mutable tails.

- [ ] Protect candidate files targeted by live state links.
  Why: a link can keep a segment/snapshot live even if its own head record is
  elsewhere.

- [ ] Capture live metadata/state/object refs in deterministic key order.
  Why: snapshots should be reproducible and validation should be precise.

- [ ] Build temp snapshots by streaming stored payload spans.
  Why: compaction must support large payloads and transformed bytes without
  materialization.

- [ ] Validate that captured refs are still current before install.
  Why: foreground writes during compaction must not be lost.

- [ ] Install snapshot by rename plus manifest append, then mark obsolete files.
  Why: crash recovery must find either old state or new installed state.

- [ ] Cleanup obsolete files after delete grace and live-ref checks.
  Why: file deletion must be retryable and must not break linked payloads.

Acceptance:

- [ ] Compaction preserves state, objects, attachments, queues, transactions,
  links, etags, byte counts, descriptors, and query visibility.
- [ ] Validation drift abandons a temp snapshot without installing it.
- [ ] Large multi-segment compaction benchmark is present and configurable.

## Slice 10: Dead Code And Terminology Cleanup

- [ ] Delete rejected external payload durability code.
  Why: the initial Pouch release must have one storage representation.

- [ ] Delete compatibility readers, old layout branches, stale version lineage,
  and transition docs.
  Why: unreleased baggage makes the implementation harder to reason about and
  test.

- [ ] Delete or rewrite stale fixtures and benchmarks that target the rejected
  design.
  Why: tests must describe the accepted product, not old intermediate states.

- [ ] Audit names for `pouch-redesign`, `compat`, `company`, disk-conflated
  terminology, and other temporary labels.
  Why: naming is part of the API and maintenance surface.

Acceptance:

- [ ] `rg` confirms no rejected terminology remains except in historical commit
  messages or explicitly intentional docs.
- [ ] Pouch code has no alternate old/new storage branches.
- [ ] Worktree diff contains no unrelated drive-by refactors.

## Slice 11: Targeted Verification After Full Cutover

Run targeted verification only after Slices 1-10 are implemented.

- [ ] Build the relevant Pouch unit target.
- [ ] Run targeted state write/read/reopen/delete tests.
- [ ] Run targeted public acquire/release/update/get/get-public tests.
- [ ] Run targeted object/attachment tests.
- [ ] Run targeted queue tests.
- [ ] Run targeted transaction/staged-state tests.
- [ ] Run targeted scan/query/index/full-text/flush tests.
- [ ] Run targeted crypto/compression/crypto+compression tests.
- [ ] Run targeted compaction tests.
- [ ] Run targeted failure-mode tests for bad CRC, bad lengths, invalid refs,
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

- [ ] Restore/expand production benchmarks for Pouch plaintext, Pouch crypto,
  Pouch compression where relevant, Pouch crypto+compression where relevant,
  and Go lockd disk without crypto.
- [ ] Use realistic deep nested JSON documents with long text fields.
- [ ] Ensure datasets naturally produce many default-sized segments.
- [ ] Benchmark writes, reads, read-many, acquire/release/update, get public,
  staged promotion, queue roundtrips, attachments/objects, scan, indexed query,
  full-text query, index flush, compaction, reopen, and replay.
- [ ] Include abusive overcapacity workloads with churn, deletes, stale history,
  and mixed payload sizes.
- [ ] Add fast targeted benchmark commands that complete in under one minute,
  including rebuild time, for each performance-critical subsystem.
- [ ] Add full benchmark parity gates after implementation stabilization.

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

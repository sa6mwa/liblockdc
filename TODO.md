# liblockdc TODO

## Pouch Initial Logstore Cutover

Pouch is an initial, unreleased storage engine. There is no compatibility
contract for rejected pre-release layouts and no migration path in this release.
The implementation must remain a single representation: segmented namespace
logs plus installed snapshots, with projections and query artifacts rebuilt from
durable logstore state.

## Design Approval Gate

- [ ] Review the current hard-cut implementation before major testing.
- [ ] Confirm accepted differences from Go lockd disk are intentional C/Pouch
  implementation details, not storage-model divergences.
- [ ] Confirm there are no live compatibility readers, version-lineage decoders,
  old payload-file durability paths, or parallel old/new pouch implementations.

## Implemented Initial Shape To Verify

- [x] State payload bytes are stored as bounded spans inside segment/snapshot
  files and live refs carry container identity, offset, length, plaintext byte
  count, stored byte count, and transform descriptor metadata.
- [x] Segment records use a compact Pouch-native binary envelope: magic,
  format version, enum record type, key length, metadata length, `u64` stored
  payload length, and stored-payload CRC.
- [x] Record metadata is binary and type-specific. Logical metadata needed for
  projections, including plaintext size, stored size, ETag, content type,
  query-hidden state, referenced spans, and descriptors, is available without
  opening payload streams.
- [x] State reads open bounded sources over segment/snapshot spans and do not
  depend on external payload files.
- [x] Replay rebuilds state projections from segment/snapshot records.
- [x] Compaction rewrites live state records into snapshot segments and removes
  obsolete segment files only after the installed projection no longer refers to
  them.
- [x] Crypto is optional, disabled by default, and root-mode invariant.
- [x] Compression is optional, disabled by default, and root-mode invariant.
- [x] Crypto/compression transforms are streaming wrappers over stored spans and
  preserve plaintext counts, stored counts, descriptors, ETags, and content
  metadata.
- [x] Query/index input is attached to logstore projections; indexed execution
  remains indexed and scan execution remains explicit scan.
- [x] Deprecated pouch implementation files and stale cutover audit docs are
  removed from the repository.

## Verification After Approval

- [x] Build the pouch unit target.
- [ ] Run targeted pouch unit tests for state write/read/reopen, public state,
  staged state, query index, compaction, crypto, and compression.
- [x] Run the full pouch unit test set.
- [ ] Run public API e2e coverage for acquire, get, get public, update, mutate,
  attachments, queues, transactions, query keys, query documents, flush index,
  maintenance, reopen, scan, and indexed queries.
- [ ] Run corruption/truncation tests for segment tails, invalid record lengths,
  invalid refs, bad transform descriptors, and crypto authentication failures.
- [ ] Run fuzzing for record decode, replay, state refs, scan/query,
  compaction metadata, and crypto/compression descriptors/material.
- [ ] Run production benchmarks for Pouch plaintext, Pouch crypto, Pouch
  compression/crypto where applicable, and Go lockd disk without crypto.
- [ ] Run the project review command and resolve relevant findings without
  weakening the storage invariants.
- [ ] Commit one coherent verified cutover on `feat/pouch`.

## Performance Gate

- [ ] Pouch plaintext and Pouch crypto must beat Go lockd disk without crypto on
  production metrics unless a specific exception is explicitly accepted.
- [ ] Benchmarks must cover repeated writes, reads, acquire/release/update,
  queue roundtrips, attachment/object operations, scan queries, indexed
  queries, full-text queries, compaction, reopen, multi-segment replay, and
  production-like overcapacity scenarios.

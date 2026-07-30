# liblockdc TODO

## Pouch Logstore Redesign Reset

The active Pouch implementation is the wrong physical storage design. It is a
metadata/state-history log with sidecar payload files. That is not an acceptable
Pouch storage engine. Pouch must be a proper segmented logstore, using Go lockd
disk as the reference implementation for physical storage shape, replay,
compaction, encryption boundaries, and production performance.

There is no released Pouch compatibility contract. Do not preserve the current
payload-sidecar representation. Do not add a compatibility layer, migration
path, or dual read/write mode for the current implementation. The cutover is a
single clean representation replacement.

## Hard Requirements

- [ ] Pouch is a real segmented logstore: state/object payload bytes are stored
  in segment record payload areas, not in `payloads/` sidecar files.
- [ ] Go lockd disk is the reference for log record shape, segment lifecycle,
  replay projections, scan summaries, compaction, staged state links, and
  crypto placement.
- [ ] The authoritative data model is append-only namespace segments plus
  installed snapshots/compaction outputs. In-memory projections and indexes are
  rebuildable accelerators only.
- [ ] The current Pouch sidecar payload model is removed in the cutover. No
  compatibility reads, no mixed roots, no old/new branching.
- [ ] Crypto is applied at the same storage boundary as Go lockd disk: encrypted
  at-rest log payload streams/records, not as separately encrypted document
  sidecar files.
- [ ] Queue message payloads and attachments/objects remain arbitrary binary
  payload records with metadata, following Go lockd disk semantics. They are
  not searchable JSON state documents.
- [ ] Query/index behavior must operate over the logstore projections and open
  payload spans only when required for scan validation or document emission.
- [ ] Performance acceptance remains: Pouch plaintext and Pouch crypto must beat
  Go lockd disk without crypto on production benchmark metrics unless a specific
  exception is documented and accepted.

## Reference Study

- [ ] Read and summarize the relevant Go lockd disk files before writing Pouch
  code:
  - `internal/storage/disk/logstore.go`
  - `internal/storage/disk/logstore_record.go`
  - `internal/storage/disk/logstore_compaction.go`
  - `internal/storage/disk/staging.go`
  - `internal/storage/disk/disk.go`
  - Go disk crypto writer/reader material handling
- [ ] Document exact Pouch equivalence points:
  - record header fields and validation
  - inline payload threshold and streaming payload path
  - segment active/sealed lifecycle
  - payload refs as segment path/name plus offset/length
  - state link records for staged promotion and compaction
  - compaction output and obsolete segment cleanup
  - scan summary/index hot paths
  - encryption metadata and AAD/material boundaries

## Clean Cutover Implementation

- [ ] Replace Pouch record persistence with true log records:
  - fixed record header
  - record type
  - normalized key
  - compact type-specific metadata
  - inline or streamed payload bytes appended inside the segment
  - CRC/checksum validation for record payloads
- [ ] Remove state payload sidecar paths and helpers from live code.
- [ ] Remove any code that treats `payloads/` as the state/object durability
  store.
- [ ] Implement segment refs for live payloads:
  - segment/snapshot identity
  - payload offset
  - payload length
  - plaintext and cipher byte counts
  - descriptor/material metadata where required
- [ ] Implement read sources over segment spans:
  - seek/open by segment ref
  - bounded read over payload length
  - checksum/authentication failure surfaces as storage corruption
  - no full-payload materialization for ordinary reads/scans
- [ ] Implement append paths equivalent to Go disk:
  - bounded inline record append for small payloads
  - streamed record append for large payloads
  - hashing and byte counts while streaming
  - atomic active segment size accounting
  - active segment rollover
- [ ] Implement replay from segments/snapshots:
  - state put/delete/link
  - object put/delete
  - metadata put/delete
  - queue and transaction records
  - generation/version rejection of stale records
  - truncated/corrupt tail handling consistent with Go disk behavior
- [ ] Implement compaction as logstore compaction:
  - capture live refs
  - rewrite live records into a new compacted segment/snapshot
  - preserve payload bytes and metadata
  - support links when a live payload must remain protected in another segment
  - install compaction output atomically through manifest/lifecycle records
  - obsolete old segments only after safe install
- [ ] Remove all dead code left by the cutover:
  - sidecar payload writers/readers/path helpers
  - payload-directory manifest assumptions
  - old record/ref structs that only supported metadata-only segments
  - compatibility branches for rejected Pouch roots
  - stale tests, fixtures, benchmarks, docs, and names that describe the old
    physical model
- [ ] Audit module boundaries after removal so no `pouch-redesign`,
  compatibility, company-layer, disk-conflated, or temporary transition
  terminology remains in public or internal Pouch APIs.

## Crypto Cutover

- [ ] Re-read Go lockd disk crypto boundaries and reproduce the same storage
  class in C.
- [ ] Encrypt log payload streams/records at rest, not separate state-document
  files.
- [ ] Preserve plaintext byte counts, cipher byte counts, descriptors/material,
  and state/object ETags.
- [ ] Bind record context with AAD/material in the same spirit as Go disk.
- [ ] Ensure plaintext roots and encrypted roots cannot mix.
- [ ] Remove any crypto code whose purpose is only supporting the current
  sidecar-payload implementation.
- [ ] Cover state, public state, staged state, queue payloads, transaction
  payloads, objects, attachments, and index artifacts according to the corrected
  logstore model.

## Query And Index

- [ ] Preserve the corrected incremental query-index flush mechanism.
- [ ] Reattach query index input to the new logstore projection, not sidecar
  payload files.
- [ ] Scan paths must follow Go disk shape:
  - page sorted metadata summaries
  - filter hidden/staged/reserved rows from metadata
  - open one payload span only when selector validation or document emission
    requires it
  - never materialize all candidate payloads
- [ ] Indexed paths must remain indexed. Do not fall back to scan for indexed
  selectors unless the selected engine is explicitly scan.
- [ ] Full-text and nested-field behavior must continue to work over streamed
  JSON payload spans.

## Tests

- [ ] Replace sidecar-layout assertions with logstore-layout assertions:
  - no state/object payload sidecar files
  - segment files contain state/object record payload spans
  - refs point to valid segment offsets/lengths
  - replay after reopen reconstructs live state from segment records
- [ ] Add end-to-end multi-segment tests using default segment sizing.
- [ ] Add staged state promotion/link tests matching Go disk semantics.
- [ ] Add compaction tests proving live payloads survive segment cleanup.
- [ ] Add corruption/truncation tests:
  - truncated segment tail
  - bad payload CRC
  - invalid record length
  - bad state link target
  - crypto authentication failure
- [ ] Add scan tests proving large payloads are streamed and not buffered.
- [ ] Add crypto tests proving encrypted roots do not leak payload bytes in
  segment files.
- [ ] Keep public API coverage: acquire, get, get public, update, mutate,
  attachments, queues, transactions, query keys, query documents, flush index,
  maintenance, reopen.
- [ ] Expand fuzzing around log record decode, replay, state links, scan/query,
  compaction metadata, and crypto descriptors/material.

## Benchmarks And Gates

- [ ] Update production benchmarks to assert physical logstore behavior:
  multiple segments with default segment size, repeated updates, mixed state,
  queue, attachment/object, transaction, scan, index, and compaction pressure.
- [ ] Keep benchmark variants:
  - Pouch plaintext
  - Pouch crypto
  - Go lockd disk without crypto
- [ ] Add targeted sub-minute benchmarks for:
  - append/write
  - read by key
  - scan keys
  - scan documents
  - indexed keys
  - indexed documents
  - acquire/release/update
  - queue roundtrip
  - attachment/object put/get
  - compaction
- [ ] Only after the full clean cutover is implemented, run full tests,
  fuzzing, production benchmarks, review, and parity gates.
- [ ] Acceptance requires Pouch to beat Go lockd disk on every production metric
  unless a specific exception is explicitly accepted.

## Explicit Non-Goals

- [ ] No compatibility with the current Pouch sidecar payload representation.
- [ ] No parallel old/new Pouch implementations.
- [ ] No hidden migration path in open/replay.
- [ ] No per-line or micro-iteration implementation strategy for the cutover.
- [ ] No performance work that optimizes the wrong sidecar design instead of
  replacing it with the proper logstore.

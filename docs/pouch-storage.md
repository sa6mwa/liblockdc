# Pouch Storage Implementation Specification

This document is the implementation authority for the initial Pouch storage
engine. Pouch has not shipped, so there is no compatibility obligation for any
pre-release layout, record format, sidecar format, crypto descriptor, benchmark
fixture, or transition name.

The implementation target is a real segmented logstore. The current partial
Pouch implementation is useful only as source material where it already matches
this specification. It must not be preserved through compatibility layers,
dual readers, legacy version branches, or "redesign" terminology.

## Reference Principle

Go lockd disk is the reference for storage semantics because it has already
solved the hard operational problems: append-only segment lifecycle, replay,
manifest state, staged state promotion, payload refs, fsync grouping, scan
summaries, compaction, and encryption placement.

Pouch is not Go lockd disk. It must remain a C storage engine with Pouch names,
C-native structs, C error handling, and the dependencies available in
liblockdc. It does not use protobuf and does not need byte-compatible `LOGD`
records. Divergence is acceptable only when it preserves the same storage
feature, durability property, and performance intent, and is a better local C
implementation.

Relevant Go disk files:

- `../lockd/internal/storage/disk/logstore_record.go`
- `../lockd/internal/storage/disk/logstore.go`
- `../lockd/internal/storage/disk/logstore_support.go`
- `../lockd/internal/storage/disk/logstore_compaction.go`
- `../lockd/internal/storage/disk/staging.go`
- `../lockd/internal/storage/disk/disk.go`

## Non-Negotiable Model

Pouch stores production data inside namespace log records. It is not a
metadata-only log with separate durable payload files.

Each namespace owns append-only segment files and installed snapshot files.
Every durable mutation appends a typed binary record to the active segment or
to a compaction snapshot. Records contain:

- a fixed binary header for physical navigation and integrity;
- a normalized key;
- compact binary type-specific metadata;
- an optional payload span stored in the same segment or snapshot file.

In-memory projections and query indexes are derived accelerators. They are
rebuilt from the durable logstore. They must never become the authoritative
storage format.

There must be no `payloads/` directory for state or object durability. Any
remaining payload-file implementation is rejected code, not an alternate Pouch
format.

## Module Boundaries

The implementation should introduce a private logstore core and make the
existing public Pouch API a receiver shell over that core.

Expected private boundaries:

- `lc_pouch_logstore.[ch]`: namespace lifecycle, segment/snapshot management,
  append coordinator, replay, projections, refs, and read sources.
- `lc_pouch_record.[ch]`: binary record header, binary metadata encoders,
  decoders, validation, CRC, and fuzzable decode helpers.
- `lc_pouch_manifest.[ch]`: append-only namespace manifest, incremental
  refresh offsets, marker files, open/seal/snapshot/obsolete operations.
- `lc_pouch_compaction.[h]`: candidate capture, protected link filtering,
  snapshot build/install, obsolete cleanup, IO throttling.
- `lc_pouch_transform.[h]` or equivalent: streaming crypto and compression
  wrappers plus descriptor encode/decode.
- Existing `lc_pouch_state.c`, query, queue, attachment, lease, and transaction
  modules must call the core through explicit storage operations. They must not
  parse log files directly or create alternate durable formats.

If the final file names differ, the same separation must still exist.

## Storage Layout

Representative root layout:

```text
root/
  pouch.meta
  namespaces/
    <escaped-namespace>/
      manifest.log
      markers/
        writers/
      segments/
        seg-<writer>-<monotonic-or-uuid>.log
      snapshots/
        snap-<writer>-<monotonic-or-uuid>.log
      index/
      queue-notify/
  locks/
```

`pouch.meta` records the root mode: plaintext, crypto, compression, and
crypto+compression. Plaintext and transformed roots cannot mix. Opening a root
with a different mode must fail. A future migration tool may deliberately
rewrite a root, but the initial release does not contain that path.

## Binary Record Header

Pouch uses a C-native binary header. It is not `LOGD`, not protobuf, and not a
version lineage for rejected layouts.

Required fields:

- `magic`: fixed Pouch log magic, for example `PCHL`;
- `format`: initial Pouch log format discriminator;
- `type`: record family enum;
- `flags`: record-local flags, currently reserved except for documented
  transform or link bits if needed;
- `key_len`: `uint32_t`;
- `meta_len`: `uint32_t`;
- `stored_payload_len`: `uint64_t`;
- `payload_crc32`: CRC of stored payload bytes after compression/encryption;
- `header_crc32` or reserved `uint32_t`: choose one deliberately and document
  it in `lc_pouch_record.[ch]`.

The header is for physical traversal and corruption detection. Logical facts
belong in record metadata, where replay can inspect them without opening the
payload.

Record decoding must reject impossible lengths, overflow, unknown record
types, short reads, bad CRC, invalid metadata length, invalid descriptor length,
and invalid link payloads. Crash-tail handling is defined in the replay
section.

## Record Families

Pouch must support the same core durable families as Go disk:

- metadata put/delete;
- state put/delete;
- state link;
- object put/delete.

Higher-level Pouch features map onto those families:

- attachments are object records with object metadata;
- queue message payloads are object or state-backed records with binary queue
  metadata sufficient for replay/list/claim without parsing user payloads;
- leases, transaction decisions, participants, retention markers, tombstones,
  and namespace control records use metadata/state/object records as
  appropriate, but their hot operational metadata must be binary and
  projection-visible.

Do not create text pseudo-records for hot storage facts. Human-readable strings
are allowed for content type, key names, and diagnostics, not as the primary
format for generation, byte counts, refs, queue state, transaction state, or
crypto/compression descriptors.

## Binary Metadata

Metadata is type-specific and binary. Pouch does not use protobuf.

All metadata that affects replay, scan, query, list, CAS, compaction, crypto,
or cleanup must be available without reading or parsing the payload.

Required metadata fields by family:

- metadata put/delete: generation `uint64_t`, modified timestamp, metadata
  etag/id, visibility bits as required by public state.
- state put/link: generation `uint64_t`, modified timestamp, state etag,
  content type or state kind if needed, plaintext byte count `uint64_t`,
  stored byte count `uint64_t`, transform descriptor length and bytes,
  query-hidden flag, staged/internal flag, and any summary fields needed to
  scan without opening hidden rows.
- state delete: generation `uint64_t`, modified timestamp, tombstone marker.
- object put: generation `uint64_t`, modified timestamp, object etag, content
  type, plaintext byte count `uint64_t`, stored byte count `uint64_t`,
  transform descriptor length and bytes, object class bits for attachment,
  queue payload, or internal object.
- object delete: generation `uint64_t`, modified timestamp, tombstone marker.
- state link: the state put metadata above plus a binary link payload or link
  metadata containing segment/snapshot identity, payload offset `uint64_t`,
  payload length `uint64_t`, and stored-payload CRC or equivalent validation.

Go disk keeps plaintext/cipher sizes, descriptor bytes, generation, modified
time, etag, and content type in metadata because those values are needed by hot
paths. Pouch must preserve that property.

## Payload Refs

Live projections store typed refs, not strings.

A ref contains:

- record family and key;
- segment or snapshot identity;
- record offset `uint64_t`;
- payload offset `uint64_t`;
- stored payload length `uint64_t`;
- plaintext byte count `uint64_t`;
- stored byte count `uint64_t`;
- payload CRC;
- generation, modified timestamp, etag, content type, flags, and descriptor;
- optional link target, represented as a validated structured ref.

String forms such as `container@offset:length` are allowed only for diagnostics
or tests. Durable refs and in-memory refs must be structured and overflow-safe.

Link targets must be restricted to manifested segment/snapshot names. Absolute
paths, traversal, unknown files, stale obsolete files, negative values, and
integer overflow must fail.

## Append And Commit Pipeline

Pouch writes through a namespace append coordinator, not per-mutation ad hoc
file writes.

Required behavior:

- hold the namespace writer lock while selecting the active segment and
  allocating record offsets;
- roll the active segment at the configured target size;
- append small records inline in batches;
- stream large payload records directly from the caller-provided reader through
  transforms, hash/etag, and CRC into the segment without full materialization;
- rewrite the header and metadata prefix only after final stored lengths,
  descriptor, hash/etag, and CRC are known;
- fsync in commit groups, deduplicating the same file within a group;
- make refs visible in projections only after the commit group succeeds, except
  for explicit no-sync modes with documented durability semantics;
- make pending refs visible to the same logical commit group when needed for
  CAS and staged promotion, matching Go disk's pending visibility semantics;
- propagate fsync/write failure to every operation in the group.

Go disk batches appends and fsync requests because per-write fsync dominates
core lockd workloads. Pouch must implement the same performance intent in C.
If the chosen C design uses a condition variable and append queue, it must have
bounded memory and deterministic shutdown. If it uses synchronous opportunistic
batching instead, benchmarks must prove it achieves the same class of behavior.

Initial constants should mirror Go disk unless profiling proves a C-local
change is better:

- inline payload threshold: 1 MiB;
- payload streaming buffer: 128 KiB;
- append batch buffer cap: 1 MiB;
- read file cache: 64 open segment/snapshot files;
- fsync batch delay: approximately 2 ms;
- commit max operations: configurable.

## Streaming Requirement

Streaming means real producer-to-consumer flow. Pouch must not serialize,
encrypt, compress, or concatenate a complete large document into memory behind
a streaming-looking API.

Bounded chunk buffers are acceptable. Full-message buffering, temporary files
as an implicit staging substitute, and whole-payload materialization are not.

This applies to:

- state writes;
- object/attachment writes;
- queue payload writes;
- reads;
- scan/query document emission;
- compaction;
- crypto;
- compression.

## Replay And Refresh

Replay rebuilds projections from installed snapshots and non-obsolete segments.

Required behavior:

- read namespace manifest incrementally using a persisted in-memory offset;
- use marker files to detect other writers where multi-writer refresh is
  supported;
- allow single-writer mode to skip unnecessary marker scans;
- order installed snapshot first, then live non-obsolete segments;
- apply records by generation so stale writes cannot resurrect older state;
- track each segment's last good read offset and avoid reading incomplete
  active segment tails;
- tolerate crash-truncated tails by stopping at the last complete valid record;
- never apply a partial record or bad-CRC payload;
- validate link targets against manifested segment/snapshot state before
  installing linked refs;
- rebuild metadata, state, object, queue, transaction, lease, retention, and
  query-visible projections from log records.

Open/corrupt-tail semantics must be explicit in tests. The implementation
should follow Go disk's operational intent: a crash tail must not make the
whole namespace unreadable, but corrupt data must not be silently applied.

## Manifest And Markers

Each namespace has an append-only manifest. It records at least:

- active segment open;
- active segment seal;
- snapshot install;
- obsolete segment;
- obsolete snapshot.

Manifest refresh consumes only new entries where possible. Snapshot install or
obsolete changes reset the affected replay state so projections cannot keep
stale refs.

Malformed manifest entries are ignored only where Go disk intentionally treats
them as non-authoritative append noise. Any behavior here must be tested and
documented in code comments because manifest policy is a durability decision.

Writer marker files must be scoped under the namespace and must not leak
storage-engine terminology from Go disk into the public Pouch API.

## Read Path

Reads open bounded sources over segment/snapshot payload spans. The read path
must use an LRU cache for open segment/snapshot files and `pread`-style bounded
reads where available.

Required behavior:

- read by structured ref, not by reparsing a string path;
- verify span bounds against the known segment/snapshot size;
- expose payload readers that stream transforms in the correct order;
- return metadata from projections without opening payload bytes;
- avoid opening hidden/staged/reserved rows for scan summaries;
- keep public state, private state, attachment/object, queue, and transaction
  reads on the same logstore primitives.

## Scan And Query

Scan and indexed query operate over logstore projections and payload spans.

Scan requirements:

- walk sorted metadata summaries;
- skip hidden, staged, reserved, and deleted rows before opening payloads;
- open a payload span only when selector evaluation or document emission
  requires it;
- stream selector evaluation through `liblql`;
- prove `has_more` cursor state by checking whether another matching row exists;
- never materialize all candidate documents.

Indexed query requirements:

- indexable selectors use indexes when the selected engine is index;
- indexed execution must not silently fall back to scan;
- index sidecars are derived artifacts rebuilt from logstore projections;
- index flush must be incremental and generation-aware, matching Go disk's
  performance intent rather than rebuilding entire sidecars on each flush;
- full-text search must cover text in the full JSON document, including nested
  fields and long text fields, through the selected indexed engine.

## Staged State

Staged state uses logstore records, not separate payload files.

Promotion must follow Go disk's solved shape:

- staged payload writes create normal state records under a staging key;
- promotion CAS-checks the destination and staged keys;
- promotion appends a state link at the destination pointing at the staged
  payload span;
- promotion appends a delete/tombstone for the staging key;
- compaction treats live links as protected until they are rewritten safely;
- link refs preserve etag, descriptor, plaintext byte count, stored byte count,
  and transform state.

This avoids copying staged payload bytes during promotion and preserves
large-payload performance.

## Crypto And Compression

Crypto is optional and disabled by default. Compression is optional and disabled
by default. A transformed root cannot be reopened in plaintext mode, and a
plaintext root cannot be reopened as transformed without a future explicit
migration tool.

Transforms belong at the log payload storage boundary. They are streaming
wrappers around payload bytes written into segment/snapshot records.

Required behavior:

- derive per-record material from the root key and a stable logical context;
- do not include physical segment offset in the authenticated context unless
  compaction deliberately remints descriptors;
- preserve plaintext byte count, stored byte count, descriptor bytes, etag, and
  payload CRC;
- authenticate record class, namespace, key, generation, and transform metadata
  as associated data where the provider supports it;
- fail closed on descriptor corruption, wrong key, tampered ciphertext, or
  invalid transform ordering;
- encrypt production data at rest: state payloads, attachment/object payloads,
  queue message payloads, and transaction payloads where they contain user or
  production data;
- keep searchable metadata and query artifacts free of plaintext user payloads
  unless the root mode explicitly defines and accepts that leakage.

Compression runs before encryption on writes and after decryption on reads.
zlib may be used. Compression must be streaming and bounded. Small payloads may
skip compression when the descriptor records that no compression was applied.

Compaction must copy stored payload bytes when the descriptor remains valid.
It must not decrypt/re-encrypt or decompress/recompress every live record merely
because its physical segment changes. If a future transform requires reminting
descriptors during compaction, that is a deliberate new behavior with its own
benchmarks and tests.

## Compaction

Compaction is a namespace lifecycle operation. It is not a full-cache dump.

Required behavior:

- load manifest, snapshots, and segments before capture;
- choose candidates from installed snapshot plus sealed non-obsolete segments;
- exclude the active segment;
- compute reclaimable bytes;
- enforce configurable `min_segments`, `min_reclaimable_bytes`, interval,
  delete grace, and optional IO throttle;
- detect live state links that point into candidate files and protect those
  files until the links can be rewritten safely;
- capture current refs from meta, state, and object projections in deterministic
  key order;
- build a temp snapshot from captured refs using streaming payload readers;
- copy stored payload bytes where transform descriptors remain valid;
- fsync the snapshot file;
- validate that captured refs are still current before install;
- rename temp snapshot into place;
- append manifest entries for snapshot install and obsolete files;
- update projections to new refs only after manifest install succeeds;
- delete obsolete files only after delete grace and only if no live refs or
  protected links remain.

Validation drift must abandon the snapshot without installing it. Cleanup must
be retryable and idempotent.

## Public API Coverage

Every Pouch behavior is exercised through the public Pouch API or public
storage API boundary, matching how Go lockd disk is benchmarked and tested.
Tests and benchmarks must not use background magic or private mutation helpers
to make Pouch look faster or more correct than the public engine.

Coverage must include:

- acquire, release, update, mutate, get, and get public;
- state write/read/reopen/delete;
- staged state promote/discard;
- attachments and object payloads;
- queue publish/claim/ack/retry/dead-letter where supported by liblockdc;
- transaction prepare/commit/discard paths;
- query keys, query documents, scan, indexed query, full-text query, and index
  flush;
- maintenance and compaction;
- crypto, compression, and crypto+compression roots;
- multi-segment production-size datasets using default segment size.

## Benchmarks

Production benchmarks compare:

- Pouch plaintext;
- Pouch crypto;
- Pouch compression where relevant;
- Pouch crypto+compression where relevant;
- Go lockd disk without crypto.

Benchmarks must include realistic and abusive workloads:

- deep nested JSON documents;
- long summary/description/body text fields;
- mixed small, medium, and large state payloads;
- repeated acquire/release/update loops;
- get/get-public/read-many;
- scan selectors;
- indexed selectors;
- full-text search over entire documents;
- queue roundtrips;
- attachment/object writes and reads;
- staged state promotion;
- compaction over many default-sized segments;
- reopen and multi-segment replay;
- overcapacity patterns with churn, deletes, updates, and stale history.

Acceptance target: Pouch plaintext and Pouch crypto beat Go lockd disk without
crypto on every production metric unless a specific exception is explicitly
accepted with evidence. Crypto overhead within Pouch should stay near the
plaintext baseline for indexed and metadata-heavy operations.

## Fuzzing And Failure Modes

Fuzzing and failure tests must cover:

- record header decode;
- metadata decode for every family;
- state link decode and target validation;
- manifest replay;
- crash-truncated segment tails;
- bad CRC and malformed lengths;
- replay generation ordering;
- compaction capture/install/cleanup metadata;
- transform descriptor decode;
- crypto authentication failure;
- compression corruption;
- scan/query selector paths;
- index sidecar corruption and rebuild.

Fuzzing must run with plaintext and transformed roots.

## Cleanup Requirements

The implementation cutover must remove rejected-code paths in the same slice:

- external state/object payload durability helpers;
- text hot-metadata parsers for storage facts;
- string payload refs as durable/in-memory authority;
- full-cache compaction dump logic;
- per-mutation fsync-only append paths where batching is required;
- hidden scan materialization paths;
- index rebuild-on-every-flush paths;
- compatibility branches for unreleased Pouch layouts;
- stale tests, fixtures, benchmarks, docs, and names such as
  `pouch-redesign`, compatibility layers, company layers, and disk-conflated
  terminology.

Pouch may mention Go disk in docs and comments only as a reference. Public API,
file names, errors, and durable Pouch metadata must use Pouch terminology.

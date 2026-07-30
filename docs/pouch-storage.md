# Pouch Storage Implementation Specification

This document is the implementation authority for the initial Pouch storage
engine. Pouch has not shipped, so there is no compatibility obligation for any
pre-release layout, record format, manifest, transform descriptor, benchmark
fixture, or transition name.

The implementation target is a real segmented logstore. The current partial
Pouch implementation is useful only as source material where it already matches
this specification. It must not be preserved through compatibility layers,
dual readers, legacy version branches, or "redesign" terminology.

## Reference Principle

Go lockd disk is the reference for storage semantics because it has already
solved the hard operational problems: append-only segment lifecycle, replay,
manifest state, staged state promotion, payload span ownership, fsync grouping, scan
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
- `../lockd/internal/storage/storage.go`
- `../lockd/internal/storage/attachments.go`
- `../lockd/internal/queue/service.go`
- `../lockd/internal/queue/keys.go`
- `../lockd/internal/core/locks.go`
- `../lockd/internal/core/update.go`
- `../lockd/internal/core/txn_marker_apply.go`
- `../lockd/namespaces/config_store.go`

## Go Disk Alignment Contract

This section is a mandatory implementation checklist. Every item is either
copied from Go disk semantics or is an explicitly accepted Pouch divergence.

### Storage API Boundary

Go disk exposes the storage backend in terms of `(namespace, key)`. The
namespace selects a per-namespace logstore. The key is relative within that
namespace. Go disk does not encode user namespace names into object keys under
a global storage namespace for queue, attachments, leases, or namespace
configuration.

Pouch must preserve that boundary:

- namespace selection happens before segment/snapshot/projection lookup;
- per-namespace manifests, segments, snapshots, markers, locks, and projections
  are independent;
- user namespace surfaces never move to a global `.lockd/*` durable namespace;
- if a logical object is internal, it is hidden by metadata and key policy
  inside the caller namespace, not hidden by being stored globally.

Rejected Pouch layout:

- `.lockd/queue`;
- `.lockd/attachments`;
- `.lockd/leases`;
- `.lockd/namespace-config`.

The only reserved namespaces that remain acceptable are true control stores
where Go disk also uses out-of-band control, such as transaction records and
decision markers. Any Pouch-only reserved control store must be named here
before implementation.

### Logstore Records

Go disk record families are:

- meta put;
- meta delete;
- state put;
- state delete;
- object put;
- object delete;
- state link.

Pouch may use different C enum values and a different binary header, but it
must preserve those logical families. Flattening all features into ordinary
state rows is not accepted when it loses object/meta/state distinction,
metadata hot-path behavior, or compaction semantics.

Go disk record metadata carries:

- generation;
- modified timestamp;
- etag;
- content type for object records;
- transform descriptor bytes;
- plaintext size;
- cipher or stored size where relevant;
- payload CRC in the physical header.

Pouch must carry the same logical facts in C-native binary metadata and refs.
Pouch additionally uses `uint64_t` payload lengths and offsets. This is an
accepted divergence from Go's 32-bit physical payload length because Pouch's
large-payload/file-size invariant is stronger.

Pouch must not use protobuf for durable metadata. Pouch must not use Go's
`LOGD` byte format. Those are accepted divergences; the semantic payload of the
metadata is not optional.

### Per-Key Metadata And Leases

Go disk stores active lease state in `storage.Meta` for the target key. The
lease fields are part of metadata mutation and are persisted through
`StoreMeta(namespace, key, meta, expectedETag)`. Important fields include:

- `Lease.ID`;
- `Lease.Owner`;
- `Lease.ExpiresAtUnix`;
- `Lease.FencingToken`;
- `Lease.TxnID`;
- `Lease.TxnExplicit`;
- key `FencingToken`;
- staged transaction fields;
- state etag/version/published version;
- state descriptor and plaintext byte count;
- attachment metadata.

Pouch must do the same at the logical model level. The active lease for
`(namespace, key)` is stored with that key's metadata in that namespace.
A separate global lease table keyed by encoded namespace/key is rejected.

Implementation obligations:

- acquire reads the target key metadata, validates expiry/if-not-exists, mints
  lease id/txn id/fencing token, and writes updated target-key metadata;
- keepalive reads and updates the same metadata record;
- release reads the same metadata, validates lease id/txn id/fencing token,
  commits or rolls back staged fields, clears the lease, and writes the same
  target metadata record;
- update/mutate validates the target metadata lease before staging or writing
  state;
- metadata CAS must use etag/generation behavior matching Go disk for missing
  keys, stale etags, pending same-group writes, and conflicting pending writes;
- projections cache hot lease and summary fields so lock paths do not parse
  state JSON.

If a C helper struct internally separates hot lease fields from other metadata,
that is an in-memory organization detail only. It must replay from target-key
metadata records and must not be a separate durable namespace.

### State Records

Go disk stores JSON state payloads as state records under the caller namespace
and relative key. `WriteState` performs key normalization, per-key locking,
refresh, pending/CAS checks, generation increment, streaming append, etag
generation, byte counting, and projection publication after commit.

Pouch obligations:

- state payloads are segment/snapshot payload spans, never external payload
  files;
- writes stream from caller reader through transforms and hashing;
- expected-etag and if-not-exists behavior match Go disk outcomes;
- generation increments from the current live state ref;
- read returns metadata from projections without opening payload bytes;
- payload readers are bounded to the recorded span;
- delete appends a state delete/tombstone record and advances generation.

### Object Records

Go disk uses object records for non-state payloads: attachments, queue payloads,
queue metadata objects, namespace config, transaction payloads, and other
object-like data. Object records carry content type, etag, descriptor,
plaintext size, and generation.

Pouch may encode object-class records in its state/logstore module, but it must
preserve object semantics:

- object keys are relative to the caller namespace;
- list order is lexical by key;
- `GetObject` streams a bounded payload span;
- `PutObject` supports CAS/if-not-exists and content type;
- `DeleteObject` supports CAS/ignore-not-found;
- object metadata is available without payload reads;
- object rows used by higher-level internal features are query-hidden.

### Attachments

Go disk attachment helpers define exact relative object keys:

- committed: `state/<key>/attachments/<attachment_id>`;
- staged: `state/<key>/.staging/<txn_id>/attachments/<attachment_id>`;
- committed prefix: `state/<key>/attachments/`;
- staged prefix: `state/<key>/.staging/<txn_id>/attachments/`.

Pouch must use this namespace-local key model. The attachment id may remain a
Pouch-generated id if public liblockdc APIs require that, but the durable
relative key must live under the Go-shaped attachment prefix.

Implementation obligations:

- attachment payload writes call the namespace logstore with the caller
  namespace;
- committed attachment metadata is stored in the target key metadata;
- staged attachment metadata is stored in the target key staged metadata;
- staged attachment promotion/discard follows release/transaction outcome;
- attachment bytes are exact on readback;
- attachment rows are hidden from public state scans/query/index/get-public.

### Queue

Go disk queue relative keys are:

- queue base: `q/<queue>`;
- message metadata: `q/<queue>/msg/<id>.pb`;
- message payload: `q/<queue>/msg/<id>.bin`;
- workflow state: `q/<queue>/state/<id>.json`;
- DLQ metadata: `q/<queue>/dlq/msg/<id>.pb`;
- DLQ payload: `q/<queue>/dlq/msg/<id>.bin`;
- DLQ state: `q/<queue>/dlq/state/<id>.json`.

Go queue lease keys use relative keys without file extensions:

- message lease: `q/<queue>/msg/<id>`;
- workflow state lease: `q/<queue>/state/<id>`.

Pouch must use the same namespace-local layout family. Because Pouch does not
use protobuf, the C-native queue metadata object key is
`q/<queue>/msg/<id>.meta` instead of Go's `.pb`. Payload and workflow state
keys stay Go-shaped: `q/<queue>/msg/<id>.bin` and
`q/<queue>/state/<id>.json`. Message and state lease metadata keys remain
extensionless: `q/<queue>/msg/<id>` and `q/<queue>/state/<id>`.

Queue enqueue obligations:

- normalize namespace and queue name;
- generate a message id;
- write the payload object in the caller namespace first;
- count payload bytes while streaming;
- build queue message metadata with queue, id, enqueue time, updated time,
  attempts, failure attempts, visibility, TTL, attributes, content type,
  descriptors, correlation id, and lease fields;
- write metadata object in the caller namespace second;
- if metadata write fails, delete the payload object;
- update ready cache or equivalent notification state only after metadata is
  durable;
- notify watchers through namespace/queue scoped notification behavior.

Queue delivery obligations:

- candidate ordering is by enqueue time and deterministic sequence/id
  tie-breaker, not by payload content;
- visibility timeout, TTL, max attempts, attempts, failure attempts, last
  error, and DLQ behavior match the supported Go queue behavior;
- ack/nack/extend validate active lease id, fencing token, txn id, status, and
  expiry;
- stateful queue operations acquire paired message and state leases;
- transaction marker application pairs `q/<queue>/msg/<id>` and
  `q/<queue>/state/<id>` the same way Go `queue.ParseMessageLeaseKey` and
  `ParseStateLeaseKey` do.

### Namespace Configuration

Go disk namespace config is an object in the configured namespace:

- key: `config/namespace.pb`;
- missing object returns the default config;
- load/save use the backend object API;
- save uses expected etag/CAS;
- cache TTL is short and invalidated on save;
- crypto encrypts metadata when enabled.

Pouch must store namespace config in the configured namespace, not in a global
namespace. The Pouch key is `config/namespace` unless a C-native suffix is
explicitly selected before implementation. The object is internal/query-hidden.

### Staging

Go disk staged state key shape is `<key>/.staging/<txn>`.
Promotion:

- locks destination and staged keys in sorted order;
- refreshes namespace projections;
- verifies destination CAS or if-not-exists;
- verifies staged ref exists and is not deleted;
- appends a state link at the destination pointing at the staged payload span;
- appends a state delete for the staged key;
- returns staged etag, descriptor, and plaintext bytes without copying payload.

Pouch must preserve this. For staged attachments, use the Go attachment helper
shape `state/<key>/.staging/<txn>/attachments/<id>`, not the generic staged
state key helper unless the resulting key is exactly the same.

### Transactions

Go disk uses reserved control namespaces for transaction records/decision
markers, but transaction participants are ordinary `(namespace, key)` pairs.
Applying a decision mutates the participant's namespace-local metadata/state.
Queue participant lease markers are recognized by the queue message/state key
shape and applied to the paired key.

Pouch obligations:

- keep transaction decision/control records only in documented control
  namespaces;
- do not use transaction control namespaces to hide queue or attachment data;
- transaction commit/rollback mutates participant metadata in participant
  namespaces;
- queue message/state paired marker application is preserved;
- transaction records containing production data are transformed at rest on
  crypto/compression roots.

### Compaction

Go disk compaction captures live refs from per-namespace projections, excludes
the active segment, protects live links into candidate files, streams payload
spans into a temp snapshot, validates captured refs before install, renames the
snapshot, updates manifest state, and deletes obsolete files only after grace
and live-ref checks.

Pouch must compact with the same lifecycle. It must not compact by dumping an
entire cache, reparsing payload JSON, or materializing large values. Object,
state, metadata, queue, attachment, transaction, and lease metadata refs must
all be represented in capture/validation.

### Query And Scan Visibility

Go disk keeps internal object surfaces out of public state/query results
through storage API boundaries and metadata/query-exclusion behavior. Pouch's
single C logstore representation must preserve that result:

- internal queue rows are hidden;
- attachment object rows are hidden;
- namespace config rows are hidden;
- staged rows are hidden;
- transaction control rows are hidden from user namespace scans;
- lease metadata is metadata, not a query-visible state document.

Hidden means all of the following:

- skipped by state scans before payload open;
- excluded from indexed document tables;
- excluded from exact/presence/range/text/trigram/temporal/full-text postings;
- excluded from get-public;
- rejected or protected against direct public state-key collision where a user
  key could address an internal row.

## Divergence Register

Every divergence from Go disk belongs in one of these two buckets.

### Accepted Pouch Divergences

- Physical record bytes:
  Go disk uses `LOGD` records. Pouch uses Pouch-specific binary record magic,
  C structs, and Pouch file naming. Reason: Pouch is a separate C storage
  engine and must not imply byte compatibility with Go disk.

- Metadata encoding:
  Go disk serializes rich metadata payloads through Go/protobuf-supported
  structures. Pouch uses C-native binary metadata. Reason: liblockdc does not
  use protobuf for pouch, and binary metadata is faster and easier to fuzz in C.

- Payload length width:
  Go disk's physical record header uses a smaller payload length field. Pouch
  uses `uint64_t` stored payload length, plaintext length, stored length, and
  offsets. Reason: Pouch must preserve large-payload and large-file invariants.

- Query-index artifact format:
  Go disk query/index internals are Go-native. Pouch uses C-native packed
  derived artifacts. Reason: indexes are derived from the authoritative
  logstore and may use a local format as long as query semantics, rebuild, and
  performance intent are preserved.

- Public names and diagnostics:
  Pouch uses Pouch terminology in files, errors, events, and durable metadata.
  Reason: Pouch is not disk and must not expose Go disk identity.

### Unaccepted Current Pouch Gaps To Remove

- Global queue namespace:
  Any durable use of `.lockd/queue` is rejected. Queue records must move to the
  caller namespace under `q/<queue>/...`.

- Global attachment namespace:
  Any durable use of `.lockd/attachments` is rejected. Attachment records must
  move to the caller namespace under `state/<key>/attachments/...` and
  `state/<key>/.staging/<txn>/attachments/...`.

- Global namespace-config namespace:
  Any durable use of `.lockd/namespace-config` is rejected. Namespace config
  must move to the configured namespace under `config/namespace` or a
  documented C-native key with the same locality.

- Global lease namespace:
  Any durable use of `.lockd/leases` is rejected. Active lease state must move
  into target-key metadata in the target namespace.

- Namespace-encoded side keys:
  Any key helper that encodes namespace into a global key is rejected for
  queue, attachment, namespace config, and lease data. Namespace is a storage
  argument, not part of those durable keys.

- State-backed object flattening:
  Any implementation that treats attachments, queue payloads, queue metadata,
  or namespace config as public state documents is rejected. They are
  object/internal records with query-hidden visibility.

- Lease hot path parsing:
  Any acquire/release/keepalive path that must parse user state JSON or a
  separate global lease payload to determine target-key lease state is
  rejected. Lease facts are metadata hot fields.

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

The implementation keeps Pouch as a receiver shell over private storage
boundaries. The current C boundaries are:

- `lc_pouch_namespace.[ch]`: namespace lifecycle, root/namespace manifest
  handling, marker files, active segment selection, snapshot install, obsolete
  cleanup, and repair from discovered segments/snapshots.
- `lc_pouch_record.[ch]`: binary record header, validation, CRC, and fuzzable
  decode helpers.
- `lc_pouch_state.c`: the namespace logstore writer/replay/projection core for
  metadata/state/object records, structured refs, scan summaries, staged links,
  and compaction capture/install.
- `lc_pouch_crypto.[ch]`: streaming transform wrappers plus descriptor
  encode/decode for crypto and compression.
- `lc_pouch_query_index.[ch]` and index helpers: derived query/full-text
  artifacts rebuilt from state projections.

Queue, attachment, lease, namespace configuration, and transaction modules call
this storage core through state operations and the same logical keyspace model
as Go lockd disk. They must not parse log files directly or create alternate
durable formats.

Reserved namespaces are not a general escape hatch. Pouch may use reserved
namespaces only for storage-engine control data whose Go reference equivalent
is also out-of-band control data, or where this document explicitly accepts a
C-local control artifact. User-namespace surfaces must remain anchored in the
caller namespace:

- queue message metadata and payload records live under the caller namespace
  with `q/<queue>/msg/<id>`-style keys;
- queue workflow state lives under the caller namespace with
  `q/<queue>/state/<id>`-style keys;
- attachments live under the caller namespace with
  `state/<key>/attachments/<id>` and staged attachment keys under
  `state/<key>/.staging/<txn>/attachments/<id>`;
- namespace configuration lives inside the configured namespace under
  `config/namespace`;
- lease state is metadata for the target logical key in the target namespace,
  not a separate global lease namespace keyed by encoded namespace/key.

Any internal row stored in a user namespace must be explicitly marked
query-hidden and must be excluded from public scans, indexed queries, and
document emission. This preserves Go disk's locality and visibility model
without exposing internal storage rows as user state.

## Logging

Pouch logs through pslog using the liblockdc logging contract in
`docs/logging.md`.

Every Pouch log uses `sys=storage.pouch`, including logstore, manifest, scan,
index, crypto, compression, compaction, queue-backed storage,
attachment/object storage, and maintenance internals. Internal areas are
expressed in event names such as `logstore.append`, `index.flush`, and
`compaction.start`; they must not be represented by changing `sys`.

Pouch logs must use short, readable fields such as `ns`, `key`, `segment`,
`record_offset`, `payload_len`, `stored_bytes`, `plaintext_bytes`,
`generation`, `reason`, and `elapsed_ms`. Pouch logs must never include state
JSON, queue payloads, attachment/object bodies, full document text, crypto key
material, transform secrets, or credentials.

## Storage Layout

Representative root layout:

```text
root/
  manifest
  namespaces/
    <escaped-namespace>/
      manifest
      markers/
        <writer-marker>
      segments/
        seg-<monotonic>.log
      snapshots/
        snapshot-<monotonic>.log
      index/
      queue-notify/
  locks/
```

`pouch.meta` records the root mode: plaintext, crypto, compression, and
crypto+compression. Plaintext and transformed roots cannot mix. Opening a root
with a different mode must fail. The initial release does not contain a
plaintext/transformed migration path.

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
- `header_crc32`: CRC32 over the first 28 header bytes.

The header is for physical traversal and corruption detection. Logical facts
belong in record metadata, where replay can inspect them without opening the
payload.

`flags` currently defines `LC_POUCH_RECORD_FLAG_PENDING`. Payload writes first
append a pending prefix, stream the payload, then rewrite the prefix with final
metadata and flags cleared. Replay never applies pending records. A pending
record at the active segment tail is treated as crash residue and repaired by
truncating to the last good offset; a pending record in a snapshot or sealed
segment is corruption.

Record decoding must reject impossible lengths, overflow, unknown record
types, short reads, bad CRC, invalid metadata length, invalid descriptor length,
and invalid link payloads. Crash-tail handling is defined in the replay
section.

## Record Families

Pouch supports the same durable behavior as Go disk through C-local record
families:

- metadata put/delete;
- state put/delete;
- object put/delete;
- state link;
- decision/control records;
- high-water records.

Higher-level Pouch features map onto those families:

- attachments are object records in the caller namespace under
  `state/<key>/attachments/<id>`-style keys;
- queue message metadata and payloads are object records in the caller
  namespace under `q/<queue>/msg/<id>`-style keys, with documented Pouch
  extension choices if C-native records do not use `.pb`/`.bin`;
- queue workflow state is an object record or metadata-backed queue state
  record in the caller namespace under `q/<queue>/state/<id>`-style keys;
- lease state is target-key metadata in the caller namespace;
- transaction decisions, participants, retention markers, tombstones, and
  explicit Pouch control records use reserved namespaces only where the
  reference model also uses reserved control stores or the C-local divergence is
  documented.

Pouch has one segment/snapshot physical container, but it must preserve the
logical record-family distinction that Go disk relies on. Object-like payloads
must not be flattened into user state documents. The accepted C-local
divergence is physical encoding, not loss of meta/state/object semantics and
not logical namespace/key placement.

Queue records persist enqueue seconds, enqueue nanoseconds, and an enqueue
sequence in binary metadata. Dequeue and stats ordering is FIFO by that tuple,
then message id as a deterministic final tie-breaker. Queue scans must not sort
same-second bursts by variable-width textual ids or by payload content.

Do not create text pseudo-records for hot storage facts. Human-readable strings
are allowed for content type, key names, and diagnostics, not as the primary
format for generation, byte counts, refs, queue state, transaction state, or
crypto/compression descriptors.

## Binary Metadata

Metadata is type-specific and binary. Pouch does not use protobuf.

All metadata that affects replay, scan, query, list, CAS, compaction, crypto,
or cleanup must be available without reading or parsing the payload.

Required metadata fields by family:

- state put/link: generation `uint64_t`, modified timestamp, state etag,
  content type or state kind if needed, plaintext byte count `uint64_t`,
  stored byte count `uint64_t`, transform descriptor length and bytes,
  query-hidden flag, staged/internal flag, and any summary fields needed to
  scan without opening hidden rows.
- state delete: generation `uint64_t`, modified timestamp, tombstone marker.
- state metadata: generation `uint64_t`, modified timestamp, current etag,
  byte counts, descriptor, content type, and query visibility without replacing
  payload bytes.
- decision/control: generation `uint64_t`, etag where applicable, decision
  code, and high-water sequence data.
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

Pouch writes through the namespace state/logstore writer. The current C
implementation has a single-writer namespace lock, segment rolling, batched
small control appends, streaming payload appends, final prefix rewrite, and a
required cross-operation commit-group/fsync-delay batching layer.

Required behavior:

- hold the namespace writer lock while selecting the active segment and
  allocating record offsets;
- roll the active segment at the configured target size;
- append small records inline in batches;
- stream large payload records directly from the caller-provided reader through
  transforms, hash/etag, and CRC into the segment without full materialization;
- rewrite the header and metadata prefix only after final stored lengths,
  descriptor, hash/etag, and CRC are known;
- group independent commit requests through a bounded fsync-delay batcher so
  the same segment file is synced once per batch, mirroring Go disk's durable
  group commit model;
- defer hot segment syncs through duplicate segment fds held by the current
  state commit group, then drain the group at the public mutation boundary;
- publish writer markers only after the grouped segment sync succeeds, so
  shared readers are not signaled to refresh from unsynced segment data;
- make refs visible in projections only after the commit group succeeds at the
  public boundary, except for explicit same-operation staged visibility;
- make staged/pending refs visible only through explicit same-operation
  promotion paths, matching the supported C-local state-link semantics;
- propagate fsync/write failure to every operation in the group.

Go disk batches appends and fsync requests because per-write fsync can dominate
core lockd workloads. Pouch must carry the same performance invariant: append
ordering is deterministic, commit publication waits for durable sync, syncs are
deduplicated per file within a batch, and shutdown drains or fails outstanding
commit requests deterministically. On Linux, hot segment commits use
`fdatasync`, matching Go disk's Linux sync path.

Initial constants should mirror Go disk unless profiling proves a C-local
change is better:

- inline payload threshold: 1 MiB;
- payload streaming buffer: 128 KiB;
- read file cache: 64 open segment/snapshot files;
- append batch buffer cap for grouped inline records: 1 MiB.

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
  query-visible projections from log records without depending on global side
  namespaces for user data surfaces.

Open/corrupt-tail semantics must be explicit in tests. The implementation
should follow Go disk's operational intent: a crash tail must not make the
whole namespace unreadable, but corrupt data must not be silently applied.

## Manifest And Markers

Each namespace has a manifest recording at least:

- active segment open;
- active segment seal;
- snapshot install;
- obsolete segment;
- obsolete snapshot.

The current C manifest is a small rewritten text file with active segment,
latest snapshot, and obsolete file sets. Snapshot install or obsolete changes
reset the affected replay state so projections cannot keep stale refs. The
manifest is namespace lifecycle metadata; hot state/object/queue/lease facts
remain binary log records.

Malformed manifest entries are ignored only where Go disk intentionally treats
them as non-authoritative append noise. Any behavior here must be tested and
documented in code comments because manifest policy is a durability decision.

Writer marker files must be scoped under the namespace and must not leak
storage-engine terminology from Go disk into the public Pouch API.

## Read Path

Reads open bounded sources over segment/snapshot payload spans. The read path
uses an LRU cache for open segment/snapshot file descriptors and dup-backed
bounded sources for independent reader lifetimes.

Required behavior:

- read by structured ref, not by reparsing a string path;
- never read past the stored payload span;
- expose payload readers that stream transforms in the correct order;
- return metadata from projections without opening payload bytes;
- avoid opening hidden, staged, reserved, and internal-prefix rows for scan
  summaries;
- keep public state, private state, attachment/object, queue, and transaction
  reads on the same logstore primitives.

Public state scans must also exclude internal user-namespace key prefixes such
as `q/`, `state/<key>/attachments/`, `state/<key>/.staging/`, and
`config/namespace` unless a future public API deliberately exposes those
surfaces. Hiding must be enforced by metadata and prefix policy, not merely by
placing records in a different namespace.

## Scan And Query

Scan and indexed query operate over logstore projections and payload spans.

Scan requirements:

- walk sorted metadata summaries;
- skip hidden, staged, reserved, internal-prefix, and deleted rows before
  opening payloads;
- open a payload span only when selector evaluation or document emission
  requires it;
- stream selector evaluation through `liblql`;
- prove `has_more` cursor state by checking whether another matching row exists;
- never materialize all candidate documents.

Indexed query requirements:

- indexable selectors use indexes when the selected engine is index;
- indexed execution must not silently fall back to scan;
- query-index artifacts are derived from logstore projections and are
  rebuildable after corruption or loss;
- query-index segment headers are plaintext metadata. They contain format,
  sequence, row counts, and hashes only;
- every non-header query-index segment component is stored in one packed binary
  artifact named `query.<segment>.query.index.lcpseg`. The packed artifact
  contains the document table, exact/presence/range/text/trigram/temporal term
  generations, and the delete set. The logical component paths remain
  in-memory identifiers for manifest signatures and parser routing only;
- encrypted packed query-index artifacts store ciphertext followed by
  descriptor bytes and a fixed binary footer. Pouch reads the footer, bounds
  decryption to the ciphertext span, and does not create separate descriptor
  companion files for derived query-index artifacts;
- an empty delete set is represented by a zero-length delete component inside
  the packed artifact. The manifest's `delete_count=0` and empty-set hash are
  the authoritative empty value; non-empty delete components must match the
  manifest count and hash;
- query-index segment artifacts and their manifest may be written directly
  without fsync because they are derived files. Recovery validates the manifest,
  header, and packed artifact signatures and rebuilds from the logstore if any
  derived write was interrupted or torn;
- normal append flushes do not sweep the index directory for orphaned derived
  artifacts. Full rebuild, repair/validated flush, and retired-segment cleanup
  paths perform orphan cleanup, so foreground append flush latency is not tied
  to directory size;
- indexed queries may reuse per-client artifact-cache trust for segment headers
  after the current manifest has validated the same path, signature, sequence,
  row count, and row hash. If the signature changes, pouch rereads the header
  and validates it normally;
- a successful manifest sequence read records per-client manifest trust for
  that namespace/index sequence. A later non-validating ensure-current call may
  skip rereading the manifest when the state index sequence is unchanged;
- indexed document queries with a discard sink may bulk-count exact candidates
  only when there is no input cursor and all matches fit below the requested
  limit, so cursor behavior and candidate verification semantics remain
  unchanged;
- packed query-index artifacts are cached per client by file signature so one
  physical read/decrypt can serve document table, delete set, and term
  generation components for the same segment;
- field-specific `contains` lookups report whether all text generations for
  the field are complete while collecting indexed candidates. The planner must
  not perform a second completeness-only artifact walk after a successful
  field-specific contains lookup;
- index flush must be incremental and generation-aware, matching Go disk's
  performance intent rather than rebuilding the full corpus on each flush;
- full-text search must cover text in the full JSON document, including nested
  fields and long text fields, through the selected indexed engine;
- `/...` full-text token and trigram terms are synthetic aggregate postings.
  Whole-document `icontains` candidate selection must resolve those aggregate
  postings directly, not scan concrete field-specific term dictionaries.

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
plaintext root cannot be reopened as transformed.

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
  unless the root mode explicitly defines and accepts that leakage. Query-index
  segment headers may be plaintext because they contain only non-secret format,
  sequence, count, and hash metadata.

Compression runs before encryption on writes and after decryption on reads.
zlib may be used. Compression must be streaming and bounded. Small payloads may
skip compression when the descriptor records that no compression was applied,
or may omit the descriptor only when no transform was applied. Encrypted records
are always descriptor-required. Tiny control records, including lease records,
are compression-ineligible on compression-enabled roots because zlib overhead
dominates those hot paths; on crypto roots they are still encrypted and still
preserve plaintext and stored byte counts.

Compaction must copy stored payload bytes when the descriptor remains valid.
It must not decrypt/re-encrypt or decompress/recompress every live record merely
because its physical segment changes.

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

Document-returning production query metrics measure steady-state public API
throughput. The benchmark first runs the same document query once outside the
timed region and verifies the exact match count for both Pouch and Go lockd
disk, then records the timed query. Key-only query metrics are not warmed this
way because they do not exercise document streaming.

Acceptance target: Pouch plaintext and Pouch crypto beat Go lockd disk without
crypto on every production metric unless a specific exception is explicitly
accepted with evidence. Crypto overhead within Pouch should stay near the
plaintext baseline for indexed and metadata-heavy operations. The
`benchmark-pouch-go-parity-gate` target runs the production comparison matrix
and fails when Pouch plaintext, crypto, compression, or crypto+compression is
slower than Go disk on any reported production performance metric.

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
- query-index artifact corruption and rebuild.

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

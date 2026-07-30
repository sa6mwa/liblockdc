# Pouch Storage Technical Specification

> Current status: this specification defines the initial Pouch storage
> implementation. Pouch has no released compatibility contract and no migration
> obligation for rejected pre-release layouts.

## Decision

Pouch must be a real segmented logstore.

The rejected pre-release design, where namespace segments were effectively
metadata/history records and state/object bytes lived in `payloads/` files, is
not a valid Pouch format. Pouch stores durable payload bytes in log record
payload areas inside segment/snapshot files, with refs expressed as segment
identity plus payload offset and length.

No compatibility layer is allowed for rejected layouts. Pouch is
single-representation: no dual readers, no mixed roots, no open-time migration,
and no branching between old and new Pouch record formats.

## Reference

Go lockd disk is the reference implementation for physical storage shape,
durability semantics, replay, compaction, scan summaries, staged state links,
and encryption placement. The relevant reference files are:

- `../lockd/internal/storage/disk/logstore.go`
- `../lockd/internal/storage/disk/logstore_record.go`
- `../lockd/internal/storage/disk/logstore_compaction.go`
- `../lockd/internal/storage/disk/staging.go`
- `../lockd/internal/storage/disk/disk.go`

Pouch remains a C storage engine and should use Pouch module names and C-local
interfaces, but its physical behavior must match the proven logstore model.

## Storage Model

Each namespace owns append-only segment files. A mutation appends a typed record
to the active segment. Records contain:

- fixed binary header with magic/version/type/lengths/checksum fields;
- normalized key bytes;
- compact binary type-specific metadata;
- optional payload bytes stored inline in the segment record.

State documents, object payloads, queue payload records, transaction records,
and attachment/object data are all represented as log records or object records
with payload spans in segment/snapshot files. Metadata records may be small and
fully inline, but they are still log records.

In-memory projections are accelerators only. They are rebuilt by replaying
segments/snapshots and must never be the authoritative source. Query indexes are
derived artifacts and must be rebuildable from logstore state.

## Payload Refs

Live state/object projections must point to payload spans, not external payload
files.
A payload ref records:

- segment or snapshot identity;
- payload offset;
- payload length;
- plaintext byte count;
- cipher byte count when encrypted;
- descriptor/material metadata when required;
- state/object ETag and version metadata.

The fixed record header carries only physical record navigation and integrity:
magic, format version, record type, flags/reserved bits, key length, metadata
length, `u64` stored payload length, and stored-payload CRC. Logical storage
facts such as generation/version, updated time, plaintext byte count, stored
byte count, ETag, content type, query-hidden state, referenced-span metadata,
and transform descriptor live in binary metadata so projections can be rebuilt
without opening JSON or binary payload streams.

Reads open a bounded source over the referenced segment span. Ordinary reads,
scan validation, and document emission must stream over that source. Full
payload materialization is allowed only for explicitly bounded inline helper
cases.

## Record Families

Initial record families:

- metadata put/delete;
- state put/delete;
- state link;
- object put/delete;
- queue records;
- transaction decision/participant records;
- retention/tombstone records as needed by the public API.

State link records are required for staged promotion and compaction cases where
a live state head must refer to a payload span protected in another segment or
snapshot. Links must validate segment identity, offset, length, and checksum.
They must not accept absolute paths, traversal, or unmanifested targets.

## Segments And Replay

Segments are append-only. Writers append to an active segment and roll over to a
new segment when size thresholds require it. Sealed historical segments are not
mutated by foreground writes.

Replay must:

- tolerate a crash-truncated tail according to Go disk semantics;
- reject invalid record headers, impossible lengths, bad CRC/checksum, and bad
  link targets;
- apply generation/version rules so stale records cannot resurrect older state;
- rebuild metadata, state, object, queue, transaction, retention, and query
  projections from durable records;
- keep reserved/internal namespaces isolated from public operations.

## Compaction

Compaction rewrites live records into a new compacted segment/snapshot and then
installs it through the namespace lifecycle/manifest. Old segments become
obsolete only after safe install.

Compaction must preserve payload bytes and metadata, including descriptors,
plaintext/cipher byte counts, ETags, versions, content types, query-hidden
state, queue state, and transaction state. It must abort on validation drift
instead of installing a snapshot built from stale segment inputs.

Compaction cleanup is retryable and idempotent. Cleanup removes obsolete segment
or snapshot files only after they are no longer referenced by the installed
manifest/projection.

## Crypto

Pouch storage encryption is optional and disabled by default.

When crypto is enabled, encryption belongs at the same storage boundary as Go
lockd disk: log payload streams/records are encrypted at rest as they are
written into the logstore. Pouch must not model encryption as separately
encrypted state-document files outside the logstore.

Crypto requirements:

- plaintext and encrypted roots cannot mix;
- opening an encrypted root without the correct key fails;
- opening a plaintext root with crypto enabled fails unless an explicit future
  migration tool exists;
- plaintext byte counts, cipher byte counts, descriptors/material, and ETags
  are preserved;
- record context is authenticated as AAD/material in the same spirit as Go disk;
- queue payloads, transaction payloads, attachment/object payloads, and state
  payloads are covered according to their log record/object semantics.

Queue message payloads and attachments are arbitrary binary data. They are not
searchable JSON state documents, but they are still production data and must be
encrypted at rest when the root is encrypted.

## Query And Scan

Searchable state is the logical JSON state payload referenced by the live state
projection. Scan and indexed query code must operate over logstore projections,
not external payload files.

Scan behavior must follow Go disk shape:

- page sorted metadata summaries;
- filter hidden/staged/reserved rows from metadata before opening payloads;
- open one payload span only when selector validation or document emission
  requires it;
- stream selector evaluation through `liblql`;
- never materialize all candidate payloads;
- preserve cursor semantics by proving whether another matching row exists.

Indexed query behavior must remain indexed. If the selected engine is index,
indexable selectors use postings/summary rows and must not silently fall back to
scan. Derived index artifacts are rebuildable from logstore segments/snapshots.

## Layout

The exact C layout may differ from Go disk names, but it must reflect a real
logstore. A representative layout is:

```text
root/
  manifest
  namespaces/
    <escaped-namespace>/
      manifest
      markers/
      segments/
        seg-00000000000000000001.log
        seg-00000000000000000002.log
      snapshots/
        snapshot-00000000000000000002.log
      index/
      queue-notify/
  locks/
```

There must be no `payloads/` directory for state/object durability.

## Verification Requirements

Tests must prove the physical storage model:

- state/object bytes are present as segment record payload spans;
- no external state/object payload files are created;
- refs point to valid segment/snapshot offsets and lengths;
- reopen replay reconstructs live state from segment records;
- large state/object payloads stream through segment spans;
- multiple default-sized segments are produced under production-like load;
- compaction preserves live payloads and removes only obsolete history;
- crypto roots do not expose plaintext payload bytes in segment files;
- corruption and truncation failures are detected.

Public API coverage must remain end-to-end through Pouch public APIs: acquire,
get, get public, update, mutate, attachments, queues, transactions,
query-keys, query-documents, flush-index, maintenance, reopen, scan, and indexed
queries.

Fuzzing must cover record decode, replay, state links, scan/query paths,
compaction metadata, and crypto descriptors/material.

## Benchmark Requirements

Production benchmarks must compare:

- Pouch plaintext;
- Pouch crypto;
- Go lockd disk without crypto.

Benchmarks must exercise repeated writes, reads, acquire/release/update, queue
roundtrips, attachment/object operations, scan queries, indexed queries,
full-text queries, compaction, reopen, multi-segment replay, and abusive
production-like overcapacity scenarios.

Acceptance requires Pouch to beat Go lockd disk on every production metric
unless a specific exception is explicitly accepted.

## Implementation Order

1. Study and map Go disk logstore semantics to Pouch C module boundaries.
2. Replace Pouch persistence with true segment record append/read/replay.
3. Remove external payload-file state/object durability code.
4. Rebuild payload refs, reads, staged links, and compaction around segment
   spans.
5. Reattach metadata, queues, transactions, attachments/objects, and retention
   to the logstore.
6. Reattach query/index/scan to logstore projections and segment-span reads.
7. Rebuild crypto at the log payload boundary.
8. Delete dead code left by the cutover, including external payload helpers,
   old metadata-only segment refs, compatibility branches, stale fixtures,
   stale benchmarks, and rejected-design terminology.
9. Audit Pouch names and boundaries so no `pouch-redesign`, compatibility,
   company-layer, disk-conflated, or temporary transition terminology remains.
10. Run focused tests and benchmarks only after the full representation cutover
   is implemented.
11. Run full tests, fuzzing, benchmarks, review, and parity gates after the
   clean cutover is complete.

## Non-Goals

- No compatibility with rejected Pouch payload-file layouts.
- No migration tool in the first corrected implementation.
- No parallel old/new Pouch code paths.
- No optimizing rejected payload-file designs.
- No hidden fallback from index to scan when the selected engine is indexed.

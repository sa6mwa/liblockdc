# Incremental collection appends

Implementation contract extending [ordered collections](collections.md).
The initial backend is Pouch, with matching C and Lua surfaces. Remote
collections remain unsupported. Version 0.24.0 uses core ABI 7, advancing from
released ABI 6 in v0.23.0.

## Scope and publication

A bound append writer accepts pushed JSON fragments without buffering a whole
item, replaying a temporary value, or introducing a producer thread. It writes
the eventual canonical pending record as data arrives. Pull append and push
append share validation, compact-byte hashing, codecs and record publication.

This does not enlist collections in XA or add append batches. A downstream
repository may publish immutable collection-position references through its
existing commit marker. Its readers must select committed references before
forwarding body bytes. Physically committed but unreferenced entries are not a
downstream publication and may require downstream garbage collection.

## C and Lua API

`lc_collection_append_begin(collection, request, &writer, error)` returns an
owned `lc_collection_writer`. It borrows no request strings after returning.
The request is the same initialized `lc_collection_append_req` used by pull
append, including the required append ID and optional entry/parent retention.

The writer has matching receiver methods and free functions:

- `write(bytes, count, error)` accepts arbitrary fragments of one JSON object.
  Bytes are borrowed only for that call; zero bytes do not signal EOF.
- `commit(result, error)` finishes validation and publishes one complete entry.
  Its result and retry rules match pull append. Position is assigned at
  publication, so concurrent writers are ordered by commit rather than begin.
- `abort(error)` discards unpublished storage. It is idempotent and cannot
  undo a committed or uncertain published append.
- `close()` releases the writer, aborting any unfinished append. Cleanup never
  publishes. Callers close successful and failed writers as well.

Lua exposes `collection:append_begin(request)` and writer `write`, `commit`,
`abort`, and `close`, without decoding or encoding application JSON. `write`
and `abort` return true or the normal structured SDK error; `commit` returns
the append result or an error. C returns normal `LC_OK`/SDK status codes.

A writer accepts sequential calls from different threads; overlapping calls
are rejected. Closing an active writer defers reclamation until that operation
unwinds. It retains its collection/client independently, so closing their
public receivers does not invalidate the owned writer. A write/validation/I/O
failure during encoding or publication leaves a terminal writer that can always
be aborted and closed. Argument checks before starting an operation need not
consume the writer. A
successful commit is terminal too; retry recovery uses append status or a new
writer/source with the same append ID. Cancellation is explicit abort between
fragment calls, or close. No cancellation worker is created.

## Pending storage and concurrency

Public push writers keep no root, namespace, catalog, append, commit or metadata
publication lock while waiting for the producer. Each new writer owns a
canonical pending file protected by its open file lock. On commit, its same
inode becomes a canonical collection segment; there is no payload copy or
second pass. This costs a segment per incremental commit. Existing synchronous
pull appends retain the shared active-segment fast path.

Both paths use the same incremental JSON validator and compression/encryption
encoder. Fixed chunk/parser scratch limits remain independent of item size.
Compression precedes encryption. Plaintext fingerprints cover compact JSON,
not compressed bytes or semantic key ordering.
The validator coalesces tiny parser emissions into at most 4096 bytes before
hashing/encoding, flushing the tail before each public write returns. This is
bounded chunk scratch, not a whole-value buffer or a body cache.

Commit obtains normal collection authority, rechecks the incarnation and append
identity, assigns position/generation, finalizes metadata, and publishes through
the existing head/projection/commit-barrier protocol. Duplicate identity and
fingerprint return the original binding without renewing retention; conflicts
fail. A peer may publish the same ID while a writer is producing, so identity
is checked again at commit.

Unpublished pending files are invisible to normal reads and recovery. Abort
removes them. Maintenance reclaims abandoned files after process exit releases
their locks. It skips live writers. Recovery also handles interruption between
canonical segment naming and head publication; such an orphan cannot block the
next rotation or become an entry merely because its bytes exist.

The selected NoSync or durable-sync policy still applies. Durable commits pin
their exact canonical inode and use the existing root fsync batcher and commit
barrier. Push commit holds the exclusive capture fence through synchronization
because it changes canonical segment topology. Pull append retains its shared
commit pin, allowing same-collection callers to join a durable group. Compare
push and pull costs separately, especially for many small entries.
A slow producer must not block expiry/drop or writes to another
collection. If collection deletion wins before commit, the writer fails without
reviving that incarnation. Captured readers retain their existing protection.

## Atomic parent-retention renewal

Append requests additionally accept `has_collection_retention_seconds` and
`collection_retention_seconds`; Lua uses `collection_retention_seconds`.
These are separate from the entry's `retention_seconds`.

Omission preserves the current parent deadline. Selected zero clears it.
Positive seconds replace it with the exact append commit timestamp plus the
checked duration. Only a newly published append applies the directive. Aborted,
conflicting, identical duplicate and failed-before-publication appends do not
renew the parent. An error after publication can still have applied it; status
reconciles this uncertain outcome.

The directive and resolved absolute deadline belong to the canonical entry's
metadata. Deletion, metadata-only entry changes and compaction preserve this
small publication fact along with the existing retry binding. Recovery applies
surviving canonical facts without granting another lifetime. The derived order
projection tracks the latest renewal; it is not another transaction authority.

Explicit collection expiry updates record the current collection generation as
their override boundary. Earlier append directives cannot override that update
during replay; a later append directive can. Count-only metadata changes do not
renew expiry or turn unsynced append facts into durable authority. Losing an
unsynced append must also lose its renewal, even if newer projections survived.

Append results/status expose the original append's parent deadline when that
append selected a directive. This is a retry observation, not necessarily the
current parent deadline; `describe` reports the latter. Parent renewal and entry
expiry remain independent. Reads/listing do not renew either.

Generic liblockdc collections have no default expiry. A 90-day inactivity policy
is a downstream choice: configure creation and send 7,776,000 seconds on new
appends. Expiry remains lazy in Pouch: objects behave normally until maintenance
actually removes them. A downstream logical API may enforce its own visibility
policy, without changing native read semantics.

## Verification

Prove fragmented keys, escapes, UTF-8 and nesting; invalid/truncated/trailing
input; bounded memory for multi-megabyte strings; and canonical writes before
producer EOF. Cover codecs, abort/close/owner-close, allocation/I/O failure,
concurrent writers, duplicate/conflicting IDs, captured readers, rotation,
restart and compaction. Use controlled clocks for renewal, overflow, clearing,
duplicate/failed append, and janitor races. Simulate loss of unsynced canonical
tails while preserving projections. Verify C89/C++ headers, ABI/export, Lua
parity and installed consumers. Keep performance measurements separate from
host-sensitive correctness assertions.

# Pouch compression changes on open

New roots default to `compression=none`. On an existing root, an omitted
compression option inherits the root manifest. Typed C settings and Lua's
`pouch.compression`, the legacy C field, and the endpoint query retain their
existing precedence. An explicit `none` is a request, not omission.

A matching mode opens normally. A differing explicit mode requests a synchronous
whole-root conversion before the client becomes available. Compression uses
zlib before encryption; decompression preserves the root's encryption policy
and key identity. This operation never changes state bytes, ETags, versions,
fencing tokens, transaction decisions, retention deadlines, collection IDs or
positions, history boundaries, or valid collection continuations.

Conversion requires the existing root writer lock exclusively, including
same-process clients. A conflicting writer prevents conversion: warn and adopt
the stored mode for this open. Normal single-writer admission rules still
apply. Once conversion has begun publishing, another opener cannot bypass its
exclusive ownership or an unfinished recovery by falling back.

The source remains authoritative while a separate namespace tree is built.
Stream each canonical payload through the existing decoder and encoder with
bounded chunk scratch. Preserve retained canonical history and staging, and
relocate payload references without copying the same body for every metadata
update. Rebuild derived indexes whose file offsets changed. Reclaim obsolete
files under the existing canonical manifest/head boundaries. Structural
metadata is not a compressed archive of the directory.

Ordinary state conversion excludes manifest-listed obsolete segments, including
files still present during the deletion grace period. Only the latest snapshot
and non-obsolete segments are converted; retired references to earlier snapshots
are not replayed. Live references and metadata after compaction remain intact.

The collection name catalog determines which partitions are retained. Unbound
partitions left by interrupted creation or reclamation are omitted from the
replacement, even when their head is missing. Missing or corrupt heads of
catalog-bound collections remain errors and prevent publication.

Collection retention updates retain the converted payload's exact descriptor
when their immutable binding is unchanged. This preserves metadata/body identity
for encrypted entry reads and later compaction; changing the binding still
requires descriptor authentication and rebinding.

Conversion follows collection replay's publication rules: incomplete active
headers and PENDING tails can be discarded, preserving the committed prefix.
A finalized record with a truncated key, metadata, or payload is corruption and
fails conversion before publication. The source tree and stored mode remain
available for repair and retry.

Synchronize the complete replacement tree before publishing a small durable
conversion intent. Move the original namespaces aside, install the replacement,
and persist the new root `compression=` setting. Reclaim the old tree and remove
the intent only after publication is durable. A crash during publication is
completed at the next open under exclusive ownership, even when that open omits
compression. A partially built tree without a published intent never changes
the configured mode; a later conversion may discard and rebuild it.

The staging root is created with owner-only permissions before copying any
contents. Directory permissions are restored from descendants upward, with the
staging root restored last. Until that completes, failures leave the staging
tree private, including catalogs whose readable file modes depend on a private
namespace ancestor. Permission-restoration errors fail conversion.

I/O, allocation, integrity, key-validation, or space failures fail the open with an
actionable SDK error. They never masquerade as a successful conversion. Work
already published by the conversion protocol remains recoverable. Conversion
temporarily needs room for both the retained source and replacement data.

Use libpslog warning events for conversion start and writer-conflict refusal,
and an info event after durable completion. Include path, source/target modes,
elapsed time, and useful byte counts without payloads or key material. Ordinary
matching/inherited opens do not scan or rewrite the root for conversion.

Verification covers both directions, omission versus explicit none, empty
roots, all codecs, retained history and staging, ordinary states, attachments,
collections and cursors, same-process and process writer conflicts, publication
crashes, failed conversion/retry, and bounded multi-megabyte payload handling.

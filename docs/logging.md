# liblockdc Logging Contract

This document defines the pslog event and field contract for liblockdc. It is
the authority for client-side liblockdc logging and for Pouch storage logging.

## Base Logger Ownership

The caller owns the base logger and its deployment context, including fields
such as `app`. liblockdc must not force or rewrite the caller's application
identity.

liblockdc may derive subsystem loggers by adding `sys`. The `sys` value is the
stable subsystem identity. Event names must not duplicate that identity.

## Subsystems

`sys=client.lockd` is used for public liblockdc client operations.

`sys=storage.pouch` is used for every log emitted by the Pouch storage engine,
including logstore, manifest, scan, index, crypto, compression, compaction,
queue-backed storage, attachment/object storage, and maintenance internals.
Pouch must not vary `sys` by internal area.

Do not emit `component` or `subsystem` fields. `sys` is the subsystem field.

## Event Names

Event names are short, stable, and scoped below `sys`.

Client examples:

- `acquire.start`
- `acquire.success`
- `http.attempt`
- `message.ack`
- `consumer.restart`

Pouch examples:

- `open`, `close`, `abort`
- `logstore.write`, `logstore.read`, `logstore.tail.repair`
- `logstore.fsync`
- `manifest.ensure_namespace`
- `scan.summaries`
- `index.flush`
- `compaction.start`
- `compaction.complete`

Use trace for successful hot-path storage flow and request-detail records;
debug for lifecycle, cache, recovery, and maintenance facts; sparse info for
operator-visible lifecycle or mode changes; warn for repaired corruption and
non-fatal operational problems; and error when an operation fails. Do not emit
an additional success event at info merely because a trace event already
describes the same storage operation.

Event names are a liblockdc contract, not a mirror of Go lockd disk. New event
families must describe a local public operation or a Pouch storage transition,
use the existing `noun.verb[.outcome]` pattern where applicable, and be added
to this document with their stable fields.

## Field Names

Fields must be short, stable, and readable without an external legend. Use
common abbreviations only where the abbreviation is already conventional in
this codebase or domain.

Accepted common abbreviations:

- `ns` for namespace
- `msg_id` for message id
- `cur_*` for current values, such as `cur_version` and `cur_etag`

Keep readable names where abbreviation would be ambiguous:

- `lease_id`
- `txn_id`
- `attachment_id`
- `fencing_token`
- `queue`
- `consumer`
- `owner`
- `cid`
- `endpoint`
- `path`
- `method`
- `status`
- `error`
- `attempt`
- `attempts`
- `failures`
- `version`
- `new_version`
- `etag`
- `state_etag`
- `new_etag`
- `meta_etag`
- `payload_bytes`
- `stored_bytes`
- `plaintext_bytes`
- `content_type`
- `page_size`
- `limit`
- `count`
- `public`
- `with_state`
- `return_mode`
- `query_hidden`

Use unit suffixes for numeric fields where the unit is not otherwise obvious:

- `_bytes`
- `_s`
- `_ms`

Examples: `retry_after_s`, `ttl_s`, `visibility_s`, `extend_s`,
`restart_ms`, `elapsed_ms`.

HTTP-specific events use `status`; do not introduce a parallel `http_status`
field unless the same event also has a non-HTTP status value.

## Pouch Storage Fields

Pouch storage logs may use these storage-specific fields when relevant:

- `ns`
- `key`
- `segment`
- `snapshot`
- `record_offset`
- `payload_offset`
- `record_bytes`
- `payload_bytes`
- `stored_bytes`
- `plaintext_bytes`
- `generation`
- `crc`
- `record_type`
- `flags`
- `descriptor_len`
- `records`
- `bytes`
- `reclaim_bytes`
- `pending_count`
- `reason`
- `elapsed_ms`
- `error`

## Data Safety

Logs must never include production payload data: no state JSON, queue payload,
attachment body, object body, full document text, crypto key material, transform
secret, or bearer credential. A field that represents a payload must contain
only its byte count, checksum, descriptor, or other bounded durable metadata.
Log identifiers, byte counts, statuses, timings, bounded names, and durable
metadata facts only.

All durable counters and offsets are emitted as exact-width signed or unsigned
64-bit pslog values. Do not narrow a persistent Pouch scalar through `size_t`,
`long`, or a formatting conversion before it reaches the logger.

# Lua SDK

`liblockdc` ships a Lua frontend for the public `liblockdc` client surface.
The Lua package namespace is `lockdc`.

This binding is intended to be the Lua-facing `lockd` client used by downstream
components such as `vectis`. `vectis` should consume the Lua modules shipped by
`liblockdc` rather than carrying a separate `lockd` Lua client.

## Distribution model

The Lua binding is distributed as a LuaRocks package that links against an
already-built `liblockdc` SDK installation.

- the Lua rock builds only the Lua extension module
- the C SDK and bundled native dependencies come from the installed
  `liblockdc` release
- the required `liblockdc` version is an exact match for the Lua rock version
- supported runtime architectures are the same ones shipped by the `liblockdc`
  binary SDK bundle

Normal LuaRocks installs are expected to be a two-step flow:

1. install or unpack the matching `liblockdc` SDK release for the target host
2. install the `lockdc` Lua rock against that SDK

The rock build checks the installed `liblockdc` version and fails early if it is
missing or does not match. Normal LuaRocks installs also require a shared
`liblockdc` SDK; static-only SDK bundles are reserved for `vectis` or other
in-tree embedded-Lua builds.

The failure message points at the matching GitHub release tarball URL for the
current rock version and tells the user to set `LOCKDC_PREFIX` to the extracted
SDK root or make `lockdc.pc` visible to `pkg-config`.

The generated rockspec expects:

- package `lockdc`
- `lonejson == 0.44.0-1`
- Lua `>= 5.5, < 5.6`

The lifecycle executes and packages only Lua 5.5. Each release includes the
standalone `liblockdc-lua-<version>.tar.gz` source package, the rendered
`lockdc-<version>-1.rockspec`, and the matching `.src.rock`. The source rock
embeds that exact standalone archive; all three artifacts are checksum-listed
and recursively privacy-scanned before release.

For local examples and test modules, use the `LOCKDC_LUA_BIN` value printed by
`make lua-env`. It names the project-built Lua 5.5 runner linked to the selected
Bootlin runtime. LuaRocks remains a packaging tool; it is not used as the
interpreter for a Bootlin-built native module.

## Local runner and search paths

`make lua-env` prints shell-evaluable exports for the project-built Bootlin Lua
runner and the repository's local Lua module paths. It does not install either
Lua rock. After installing `lockdc` and `lonejson` into a local LuaRocks tree,
load that tree's complete paths after the project exports, then run examples
with the Bootlin runner:

```bash
eval "$(make -s lua-env)"
eval "$(luarocks --tree /path/to/lua-tree --lua-version 5.5 path)"
"$LOCKDC_LUA_BIN" examples/lua/acquire_update_json.lua
```

The LuaRocks path export supplies the installed `lonejson` module as well as
`lockdc`. Do not replace this setup with `LD_LIBRARY_PATH` or a host Lua
interpreter: prefix-selected `lockdc` modules carry their SDK runtime path and
the lifecycle checks them through the Bootlin runner.

The C SDK is pinned to the matching `lonejson 0.44.0` native dependency for
mapped state load/save and internal typed JSON parsing. The Lua rock declares
the corresponding Lua-facing `lonejson` rock so Lua JSON behavior and the C
SDK JSON boundary stay in the same release line.

## Dependency ownership

`liblockdc` owns the Lua dependency boundary for the APIs it exposes.

That means:

- `liblockdc` ships the `lockdc` Lua module
- the `lockdc` rock declares the supported `lonejson` Lua rock version
- downstream consumers such as `vectis` should use the same `lonejson` Lua
  dependency declared by `lockdc` instead of bundling a competing copy

This keeps the Lua dependency graph coherent:

- one SDK owner for the public lock client and JSON contract
- one Lua JSON binding version aligned with the public `liblockdc` API surface
- one installation/import path for downstream outbox runtimes

The Lua binding does not expose `pslog` configuration. Native logging is a
`liblockdc` SDK concern; normal Lua rock users get the SDK no-op logger
default. Embedded hosts that need a shared framework logger should create the
`lc_client` in C with their own `pslog_logger *` and wrap that client for Lua
through a host-side integration layer.

## Public entrypoints

The top-level module is:

```lua
local lockdc = require("lockdc")
```

`lockdc` is the supported package. `lockdc.core` is an internal native module
used to implement it; applications must not require it or depend on its
methods, result shapes, or convenience behavior.

Primary entrypoints:

- `lockdc.open(config)`
- `lockdc.version_string()`
- `lockdc.xid_new()`
- `lockdc.encode_json(value)`
- `lockdc.decode_json(payload)`
- `lockdc.json_null`
- `lockdc.OK`, `lockdc.ERR_*`, and `lockdc.NACK_*` C status/intent constants
- `lockdc.pouch_crypto_generate_key()`
- `lockdc.pouch_crypto_default_key_file()`
- `lockdc.pouch_crypto_generate_key_file(path[, overwrite])`

Primary handle types:

- `Client`
- `Lease`
- `Message`
- `Outbox`
- `OutboxDispatcher`
- `OutboxTransaction`
- `OutboxParticipant`
- `OutboxJob`
- `HistoryConsumer`
- `Service`

## Byte sources, sinks, and explicit materialization

The Lua façade keeps the C `lc_source`/`lc_sink` distinction visible. Methods
that send bytes accept a source string, `{ bytes = ... }`, `{ path = ... }`,
`{ fd = ... }`, or a callback source with `read(max_bytes)`, optional `reset`,
and optional `close`. A callback source returns a byte string, `nil` at EOF,
or `nil, message` on failure.

Methods that receive bytes require a sink. A sink is a path string, file
descriptor number, `{ path = ... }`, `{ fd = ... }`, or a callback table:

```lua
local streamed, written_or_err = job:write_payload({
  write = function(chunk)
    foreign_request:write(chunk) -- receives bounded chunks
    return true                  -- nil/no return is also accepted
  end,
  close = function()
    foreign_request:finish()
  end,
})
assert(streamed == nil)
assert(type(written_or_err) == "number")
```

`write` runs synchronously and must either accept the complete supplied chunk
or return `false, message` / `nil, message` (or raise) to fail the operation.
There are no partial writes. `close` is optional, runs once when liblockdc
releases the sink, and cannot report an operation error. Do not call methods
on the receiver that started a callback-sink operation; that receiver is
deliberately protected against re-entry until streaming returns. In particular,
terminal job methods are unavailable from a job payload sink.

Streaming methods never silently buffer their output. Their first result is
`nil` and their second result reports bytes written or operation metadata. The
matching `read_*` method deliberately materializes bytes into a Lua string;
`read_*_json` additionally decodes that string. Use a `read_*` method only
when the value is deliberately bounded in process memory.

## Client API

Open a Lua client:

```lua
local client, err = lockdc.open({
  endpoints = { "https://localhost:19441" },
  client_bundle_source = { path = "./client.pem" },
  default_namespace = "default",
})
```

`client_bundle_source` accepts the same source-shaped values used by payload
uploads: a PEM string, `{ bytes = pem }`, `{ path = "./client.pem" }`,
`{ fd = fd }`, or a callback source:

```lua
local client, err = lockdc.open({
  endpoints = { "https://localhost:19441" },
  client_bundle_source = {
    read = function(max_bytes)
      return next_pem_chunk(max_bytes) -- return nil for EOF
    end,
  },
})
```

`client_bundle_path` is not a Lua API. Use `client_bundle_source` for every
Lua configuration, including a path-shaped source.

### Local Pouch storage

Use exactly one absolute, option-free `pouch://` endpoint for local storage.
Put all root-open policy in the nested `pouch` table; it includes crypto,
compression, writer, durability, maintenance, queue-watch, and query-index
settings. Presence is significant: `false` and `0` are explicit settings.
Top-level `pouch_crypto_*`/`pouch_compression` fields and Pouch endpoint query
options are not accepted by the Lua façade.

```lua
local client, err = lockdc.open({
  endpoints = { "pouch:///var/lib/my-service/lockd-root" },
  default_namespace = "default",
  pouch = {
    crypto_key_file = "/var/lib/my-service/lockd-root/pouch.key",
    crypto_generate_key_file = true,
    compression = "zlib",
    query_indexing = false,
    query_engine = "scan",
  },
})
```

The supported compression values are `"none"` and `"zlib"`. Pouch remains
exclusive single-writer by default; opening another writer for the same root
returns the normal structured `lockdc.open` error. Set
`pouch = { single_writer = false }` where shared writers are explicitly
required.

Typed Pouch settings are local-only: supplying a non-empty `pouch` table to a
remote or Unix-socket client returns the normal structured open error. See
[typed Pouch open settings](pouch-open-settings-api.md) for every key and its
precedence rule.

The explicit Pouch crypto helpers mirror the safe C utility workflows:
`lockdc.pouch_crypto_generate_key()` returns a new key string,
`lockdc.pouch_crypto_default_key_file()` returns the platform default path, and
`lockdc.pouch_crypto_generate_key_file(path[, overwrite])` creates a mode-0600
key file and returns its generated key. Treat returned keys as secrets and use
the nested `pouch.crypto_key_file` setting for long-lived process configuration.

Common client methods:

- `client:info()`
- `client:close()`
- `client:acquire(req)`
- `client:acquire_for_update(req, handler)`
- `client:describe(req)`
- `client:get(req, sink)` and `client:read(req)` / `client:read_json(req)`
- `client:update(req, body)`
- `client:update_json(req, value)`
- `client:mutate(req)`
- `client:metadata(req)`
- `client:remove(req)`
- `client:keepalive(req)`
- `client:release(req)`
- `client:attach(req, body)`
- `client:list_attachments(req)`
- `client:get_attachment(req, sink)` and `client:read_attachment(req)`
- `client:delete_attachment(req)`
- `client:delete_all_attachments(req)`
- `client:queue_stats(req)`
- `client:enqueue(req, body)`
- `client:dequeue(req)`
- `client:dequeue_batch(req)`
- `client:dequeue_with_state(req)`
- `client:queue_ack(message_or_req)`
- `client:queue_nack(req)`
- `client:queue_extend(req)`
- `client:query(req, sink)` and `client:read_query(req)` (`req.engine` and
  `req.refresh` may select the query engine/refresh mode; document-query
  trailer metadata is returned as `metadata_json`)
- `client:query_keys(req, handler)` streams decoded key bytes to `handler`
- `client:get_namespace_config(req)`
- `client:update_namespace_config(req)`
- `client:flush_index(req)`
- `client:txn_replay(req)`
- `client:txn_prepare(req)`
- `client:txn_commit(req)`
- `client:txn_rollback(req)`
- `client:tc_lease_acquire(req)`
- `client:tc_lease_renew(req)`
- `client:tc_lease_release(req)`
- `client:tc_leader()`
- `client:tc_cluster_announce(req)`
- `client:tc_cluster_leave()`
- `client:tc_cluster_list()`
- `client:tc_rm_register(req)`
- `client:tc_rm_unregister(req)`
- `client:tc_rm_list()`
- `client:new_outbox(config)`
- `client:new_history_consumer(config)`
- `client:subscribe(req, handler)` and `client:subscribe_with_state(req, handler)`
- `client:watch_queue(req, handler)`
- `client:new_consumer_service(config)`

## Pouch durable-history consumers

`client:new_history_consumer(config)` registers or reopens a durable Pouch
retention cursor. It is a compaction-safety boundary, not a history-query API:
the application remains responsible for obtaining and applying records through
its own replication, backup, or resume protocol. Remote lockd clients reject
this Pouch-only operation.

`config.namespace` and `config.consumer_id` form the
stable durable identity. A new identity begins at
`initial_acknowledged_index_seq` (zero by default), or set
`start_at_current = true` to retain only future history. Existing identities
always keep their recorded acknowledgement; the supplied initial position is
ignored. `start_at_current` and `initial_acknowledged_index_seq` are mutually
exclusive.

The returned receiver provides:

- `history:position()` → `{ acknowledged_index_seq, current_index_seq }`
- `history:advance(acknowledged_index_seq)` → the updated position; it may
  only move forward and cannot exceed the current sequence.
- `history:unregister()` → removes the durable retention pin.
- `history:close()` → releases only the local handle; it deliberately leaves
  the durable pin in place.

Use a stable consumer ID and register it before producing history that must be
retained. A slow cursor can retain more on-disk history, but normal Pouch open,
read, query, and mutation paths never enumerate consumer records.

## Inbox/outboxes

`client:new_outbox(config)` creates a threadless inbox/outbox producer. Its
`namespace` contains both durable inbox and outbox keys.
It never starts a dispatcher, claims a job, performs recovery, or invokes a
foreign effect. Use `outbox:dispatcher()` only in the dedicated worker or
service domain that owns delivery. The complete lifecycle and host-integration
contract is in [the outbox dispatch architecture](outbox-dispatch-architecture.md).

```lua
-- Request/producer domain: this is safe to construct without creating a
-- background dispatcher.
local outbox = assert(client:new_outbox({
  namespace = "orders-outbox",
  owner = "orders-api",
  max_attempts = 100,
}))

local txn = assert(outbox:append({
  operation_id = order_id,
  effect_id = "charge-card",
  effect_key = "charge:" .. order_id,
  payload_digest = payload_digest,
  kind = "http",
  destination = "https://payments.example/charges",
  headers_json = lockdc.encode_json({ ["idempotency-key"] = "charge:" .. order_id }),
}, payload_source))

local order = assert(txn:acquire({ namespace = "orders", key = order_id }))
assert(order:update_json({ status = "payment_pending" }))
order:close()
local commit = assert(txn:commit())
txn:close()

-- `commit.outbox_receipts` exists only after the durable commit. A host with a
-- bounded worker/supervisor wake channel may copy those keys now, never before.

-- worker.lua, in a dedicated worker/service process. This opens its own client
-- and outbox, then returns the one compatible local dispatcher or starts it
-- lazily. It does not run Lua on its private C thread.
-- For a Pouch root shared with the producer process, both client configurations
-- must explicitly select the supported `single_writer=false` shared-root mode.
local worker_client = assert(lockdc.open(worker_client_config))
local worker_outbox = assert(worker_client:new_outbox({
  namespace = "orders-outbox",
  owner = "orders-api",
  max_attempts = 100,
}))
local dispatcher = assert(worker_outbox:dispatcher())
assert(dispatcher:run({
  handlers = {
    http = function(job)
      assert(job:write_payload(foreign_request_body_sink))
      return job:complete()
    end,
  },
}))
```

`headers_json` is the durable serialized header envelope. Build it explicitly
with `lockdc.encode_json` when the headers originate as a Lua table.
`outbox:accept_inbox(message)`
returns `nil, result` on an accepted duplicate, where `result.duplicate` is
true. Otherwise it returns a `OutboxTransaction` and `result.accepted` is
true.

Outbox `payload_digest` is required immutable metadata binding the supplied
payload bytes. It is host-generated and opaque to liblockdc: use the same value
for an identical retry and a different value for changed bytes. This preserves
one-pass streaming, because the binding does not require the façade to consume
or buffer the payload before it stages the attachment.

`max_attempts` must fit the C API's signed 32-bit integer range; values outside
that range are rejected instead of being narrowed or defaulted.

Outbox receivers are explicit and owned. Parent acceptance and append
operations return a new `OutboxTransaction` only when they created fresh
durable work:

- `outbox:append(entry, payload)` returns `txn, nil` for a fresh
  append. Its receipt appears only in the successful `txn:commit()` result. A
  matching committed effect returns `nil, receipt` with `receipt.duplicate`.
- `outbox:accept_inbox(message)` returns `txn, result`; a matching source
  message returns `nil, result` with `result.duplicate`.
- `outbox:accept_command(request)` returns `txn, receipt`; a matching
  command returns `nil, receipt` with `receipt.duplicate`. Use
  `outbox:get_command_receipt(identity)` for a direct durable status read,
  `outbox:write_command_result(identity, sink)` to stream a completed result
  body, `outbox:read_command_result(identity)` to materialize it, and
  `outbox:resume_command(identity)` to obtain a transaction for a pending
  command. A terminal command resumes as `nil, receipt`.
  `outbox:get_command_receipt_by_id(command_id)` is suitable for an
  authenticated status resource. `outbox:wait_command(command_id, timeout_ms)`
  only waits for that durable receipt; it never runs a dispatcher handler or
  starts a worker. Use `0` for one read, `-1` for no deadline, or a positive
  monotonic deadline that also bounds each remote receipt read. On timeout it
  returns `receipt, error`, where receipt
  remains pending and `error.code` is `ERR_TIMEOUT`. Set
  `generate_idempotency_key = true` and omit
  `idempotency_key` in `accept_command` only when returning the generated key
  or status reference to the caller.
  Invalid or unknown command IDs use the normal Lua failure result
  `nil, error, code`; only a timeout has a pending receipt to return.
- `outbox:transaction(fn)` provides a lazy transaction proxy. Its first
  participant may be `acquire`, `append`, `accept_inbox`, or
  `accept_command`; it commits on normal callback return and rolls back on an
  error or any failed staged operation, even when the callback elects to
  inspect and return normally from that structured error. It starts no durable
  marker by itself, so an empty transaction is invalid. Its successful result
  includes `outbox_receipts`, so only that result contains fresh keys safe to
  forward. A pre-existing duplicate found after a domain participant is staged
  makes the transaction rollback-only, so no domain change can commit without
  its outbox/idempotency boundary. A duplicate first record returns its durable
  receipt even when the callback does not return it; a duplicate before a
  domain participant is enrolled may still allow an independently fresh
  outbox receipt to commit.
- `outbox:begin()` returns the same lazy transaction receiver for advanced
  code that needs explicit commit/rollback control. Its first participant has
  the same domain-acquire/command/inbox/outbox choices as `transaction(fn)`.
- `outbox:dispatcher()` acquires the compatible local dispatcher. It has no
  configuration argument because dispatch policy comes from the outbox's
  canonical configuration.
- `client:new_outbox(config, { dispatcher = dispatcher })` creates another
  threadless producer attached to an already-compatible dispatcher. It is the
  local fast wake path only: a committed receipt remains the durable source of
  truth and may still be forwarded to a supervisor.
- `dispatcher:next(timeout_ms)` returns a claimed job for advanced pull-based
  consumers and claims only on that demand. `dispatcher:next(0)` is strictly an
  in-memory, non-query probe; a blocking call may request durable recovery.
  `dispatcher:run({handlers = ...})` is the blocking dedicated worker loop;
  bounded `dispatcher:pump(options)` is for hosts that own their event loop;
  its `timeout_ms` is non-negative and no greater than the configured
  `shutdown_timeout_ms`.
  The first consumption choice binds a dispatcher to either raw pull or its
  caller Lua state's handler table; the other mode, another handler table, or
  another Lua state is rejected. A handler cannot recursively call `pump()` or
  `run()` on that dispatcher (including through an alias), nor call blocking
  `stop()` or `wait()` while it owns the active job. It must return its terminal
  outcome before another job is consumed. Neither should run foreign effects in
  an HTTP route.
- `dispatcher:notify_outbox_key(outbox_key)` accepts a key from a successful
  commit receipt as a bounded, process-local latency hint. It does not query
  the durable namespace; duplicate, unavailable, and overflowed hints are
  repaired by ordinary durable reconciliation.
- `dispatcher:stats()` returns process-local counters;
  `dispatcher:reconcile()` requests durable recovery.
- `dispatcher:replay_dead_letter(outbox_key)` returns one dead-lettered effect
  to pending; `dispatcher:delete_dead_letter(outbox_key)` permanently deletes
  it and its payload. `dispatcher:export_dead_letters(options, sink)` exports
  envelopes as `"json"` or `"jsonl"` to a required sink;
  `dispatcher:read_dead_letters(options)` is the explicit materializer.
- `outbox:close()` releases only the producer. `dispatcher:stop()` and
  `dispatcher:wait()` control the explicitly acquired dispatcher;
  `dispatcher:close()` releases a handle and does not stop shared work.
- `client:close()` stops and joins private dispatcher workers but does not wait
  for a job already returned to Lua. That job remains usable for its terminal
  decision or close; stopped dispatcher wrappers reject new work and remain
  closeable.

Transactions provide `acquire`, `append`, `accept_command`, `accept_inbox`,
`complete_command`, `fail_command`, `commit`, `rollback`, and `close`.
`accept_command` permits at most one command receipt per transaction.
`txn:commit()` returns `{ outbox_receipts = { ... } }` only after a durable
commit; it returns no fresh outbox key on failure or rollback.
`txn:append()` may return an existing duplicate receipt, which is
already safe to forward; fresh appended keys remain commit-published.
Participants provide `info`, `describe`, `get`, `read`, `read_json`, `update`,
`update_json`, `mutate`, `mutate_local`, `metadata`, `remove`, `keepalive`, and
the full attachment surface. Jobs provide `info`, `write_payload`,
`read_payload`, `read_payload_json`, `renew`, `complete`, `retry`,
`dead_letter`, and `close`. `write_payload` requires a sink, including a
callback sink; `read_payload_json` deliberately materializes and decodes the
complete body, so use it only when that size is deliberately bounded.

Participants deliberately have no release or terminal-decision method. Closing
or deciding a transaction invalidates its participants, so close each view when
finished. A dispatcher job is the only handoff to host effect execution; keep
its claim alive with `job:renew()` for longer foreign operations, then select
exactly one terminal operation. A successful `complete`, `retry`, or
`dead_letter` consumes the job. If a terminal operation returns an error, the
job remains active and the same terminal operation may be retried until its
claim expires. Use `job:close()` only when abandoning a non-terminal local
handle; it does not retry or complete the durable job.

Inside a `dispatcher:run()` or `dispatcher:pump()` handler,
`job:complete()`, `job:retry(options)`, and `job:dead_letter(diagnostic)`
construct an outcome that the façade applies after the handler returns. The
same methods on a job returned by raw `dispatcher:next()` perform the direct
terminal operation. `retry({ delay_seconds = n, diagnostic = message })`
durably reschedules the job within the configured retry bounds. `retry()` takes
no argument or that options table; a diagnostic string by itself is invalid.

## Raw XA and transaction-coordinator APIs

The outbox API is the normal Lua transactional-outbox path. It creates and
recovers durable decisions without requiring the application to manage an XID.
For a coordinator, resource manager, or recovery tool that must operate at the
same level as the C API, the raw XA surface is also available on `client`.

Use `lockdc.xid_new()` to mint a valid transaction identifier for raw XA work.

`txn_prepare`, `txn_commit`, and `txn_rollback` accept a table with `txn_id`,
`participants`, optional `expires_at_unix`, optional `tc_term`, and optional
`target_backend_hash`. Each participant has `namespace`, `key`, and an
optional `backend_hash`. `txn_replay` accepts `{ txn_id = ... }`. Decision and
replay results include `txn_id`, `state`, and `correlation_id`.

```lua
local txn_id = assert(lockdc.xid_new())
local participant = { namespace = "orders", key = "order-42" }

assert(client:txn_prepare({
  txn_id = txn_id,
  participants = { participant },
  expires_at_unix = 2147483647,
  tc_term = 1,
}))
assert(client:txn_commit({
  txn_id = txn_id,
  participants = { participant },
  tc_term = 1,
}))
```

The `tc_lease_*`, `tc_leader`, `tc_cluster_*`, and `tc_rm_*` methods map their
C requests and results directly to Lua tables. Terms must fit Lua's
non-negative integer range. Result lists are ordinary dense Lua arrays in
`endpoints` or `backends`.

`client:query_keys(req, handler)` has the same query request fields as
`client:query`, but sends each decoded key directly to Lua instead of materializing
the query response. Selectors are optional, so `engine = "scan"` with a cursor
can enumerate every key in a namespace. Pass either a function for key chunks
or a table with a required `chunk(bytes)` function and optional `begin()` and
`finish()` hooks.
A key can arrive in multiple chunks, including inside a UTF-8 sequence; collect
only the current key between `begin` and `finish` if complete strings are needed.

`client:acquire_for_update(req, handler)` wraps the common acquire, snapshot,
update, release outbox. The handler receives a context table with:

- `lease`
- `state`
- `state_meta`
- `load_json()`
- `update(body, req)`
- `update_json(value, req)`
- `mutate(req)`
- `mutate_local(req)`
- `metadata(req)`
- `remove(req)`
- `keepalive(req)`

The helper always attempts to release the lease after the handler returns.
Handler success commits staged changes; handler failure releases with rollback
so partial callback updates are not published. Return `nil` from the handler
for success, or return an error value to report failure and roll back:

```lua
local ok, err = client:acquire_for_update({
  key = "orders/42",
  owner = "worker-1",
  ttl_seconds = 60,
}, function(af)
  local state, meta = af:load_json()
  if state == nil and (meta == nil or not meta.no_content) then
    return "failed to load state"
  end
  state = state or {}
  state.status = "processing"
  return af:update_json(state)
end)
```

## Lease API

Returned by `client:acquire(...)` and by `message:state()`.

Common lease methods:

- `lease:info()`
- `lease:close()`
- `lease:describe()`
- `lease:get(req, sink)` and `lease:read(req)` / `lease:read_json(req)`
- `lease:update(body, req)`
- `lease:update_json(value, req)`
- `lease:mutate(req)`
- `lease:mutate_local(req)`
- `lease:metadata(req)`
- `lease:remove(req)`
- `lease:keepalive(req)`
- `lease:release(req)`
- `lease:attach(req, body)`
- `lease:list_attachments()`
- `lease:get_attachment(req, sink)` and `lease:read_attachment(req)`
- `lease:delete_attachment(selector)`
- `lease:delete_all_attachments()`

## Message API

Returned by dequeue operations.

Common message methods:

- `message:info()`
- `message:is_open()`
- `message:close()`
- `message:ack()`
- `message:nack(req)`
- `message:extend(req)`
- `message:state()`
- `message:rewind_payload()`
- `message:write_payload(sink)`
- `message:read_payload()` / `message:read_payload_json()`

## JSON helpers

The binding uses `lonejson` internally for JSON encode/decode helpers.

Use:

- `lockdc.encode_json(value)`
- `lockdc.decode_json(payload)`
- `lease:read_json()`
- `lease:update_json(value, req)`
- `message:read_payload_json()`

These helpers are for idiomatic Lua outbox code. They do not replace the
mapped `lonejson` APIs in the C SDK; those C APIs use caller-defined
`LONEJSON_FIELD_*` maps for typed state load/save. The Lua helpers sit on top
of the public `liblockdc` JSON transport surface and use the `lonejson`
dependency version declared by the `lockdc` rock.

Top-level JSON `null` is returned as `lockdc.json_null`. That keeps successful
JSON `null` payloads distinct from the binding's existing `nil, err` and
`nil, meta` return conventions.

## Consumer model

`client:subscribe(req, handler)` and `client:subscribe_with_state(req, handler)`
are direct bindings to the C streaming subscription operations. Their handlers
run synchronously on the calling Lua state and receive the normal public
`Message` and optional `Lease` receivers, not raw native userdata. A subscription handler must
explicitly `ack()` or `nack()` its borrowed message before returning; returning
`false, message`, `nil, message`, or raising stops the subscription with a
structured error. A message (and the optional state lease) is invalid once its
handler returns, even if Lua retains the wrapper. The client remains usable in
the handler, matching C subscription semantics, but it cannot be closed until
the native callback returns.

`client:watch_queue(req, handler)` is likewise a direct C queue watch, not a
polling loop. The handler receives a borrowed event table; return `false` to
stop cleanly, return `nil, message` or `false, message` to fail, and otherwise
return normally to continue.

`client:new_consumer_service(config)` is the Lua-specific threadless managed
consumer adaptation. Native `lc_consumer_service` workers cannot invoke a Lua
state from their private threads, so this surface deliberately supports one
blocking consumer at a time:

```lua
local service = assert(client:new_consumer_service({
  name = "orders-worker",
  request = { namespace = "orders", queue = "events", owner = "orders-worker" },
  with_state = true,
  handle = function(message, state)
    -- Return normally to acknowledge. Return nil, err (or false, err) to nack.
    return nil
  end,
}))
assert(service:run())
```

It uses the C-shaped `name`, `request`, and `with_state` configuration fields.
`handle` is the necessary Lua callback spelling. `run()` blocks; `stop()` is
safe from that handler and `wait()` reports after `run()` returns, whether it
succeeded or failed. There is no Lua
`start()` or `start_consumer()` alias and no multi-worker/multi-config service
because neither can safely call one Lua VM concurrently. Start separate Lua
states or processes when concurrency is needed. A normal handler return
acknowledges an open message. A handler that has already acknowledged,
negatively acknowledged, or closed its message may also return normally; the
service does not apply a second terminal operation. `nil, err`, `false, err`,
or a raised error nacks a still-open message and returns that failure from
`run()`; a failed nack is returned instead.

## Examples

Pedagogic examples live in:

- `examples/lua/acquire_update_json.lua`
- `examples/lua/acquire_for_update.lua`
- `examples/lua/queue_roundtrip.lua`
- `examples/lua/namespace_config.lua`
- `examples/lua/pouch_local_storage.lua`
- `examples/lua/consumer_handler.lua`

See also:

- [examples/lua/README.md](../examples/lua/README.md)

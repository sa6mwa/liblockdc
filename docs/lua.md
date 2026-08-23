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
- `lonejson == 0.42.0-1`
- Lua `>= 5.5, < 5.6`

The lifecycle executes and packages only Lua 5.5. Each release includes the
standalone `liblockdc-lua-<version>.tar.gz` source package, the rendered
`lockdc-<version>-1.rockspec`, and the matching `.src.rock`. The source rock
embeds that exact standalone archive; all three artifacts are checksum-listed
and recursively privacy-scanned before release.

The C SDK is pinned to the matching `lonejson 0.42.0` native dependency for
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
- one installation/import path for downstream workflow runtimes

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

Primary entrypoints:

- `lockdc.open(config)`
- `lockdc.version_string()`
- `lockdc.encode_json(value)`
- `lockdc.decode_json(payload)`
- `lockdc.json_null`

Primary handle types:

- `Client`
- `Lease`
- `Message`
- `Workflow`
- `WorkflowTransaction`
- `WorkflowParticipant`
- `OutboxJob`
- `Service`

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

`client_bundle_path` remains available for compatibility, but new Lua code
should prefer `client_bundle_source`.

### Local Pouch storage

Use exactly one absolute `pouch://` endpoint for local storage. Lua exposes the
same Pouch client configuration fields as `lc_client_config`: `pouch_crypto_key`,
`pouch_crypto_key_file`, `pouch_crypto_generate_key_file`, and
`pouch_compression`. Explicit Lua configuration takes the same precedence over
endpoint query options as the C client configuration.

```lua
local client, err = lockdc.open({
  endpoints = { "pouch:///var/lib/my-service/lockd-root" },
  default_namespace = "default",
  pouch_crypto_key_file = "/var/lib/my-service/lockd-root/pouch.key",
  pouch_crypto_generate_key_file = true,
  pouch_compression = "zlib",
})
```

The supported compression values are `"none"` and `"zlib"`. Pouch remains
exclusive single-writer by default; opening another writer for the same root
returns the normal structured `lockdc.open` error. Endpoint query options stay
supported for compatibility, including `?single_writer=false` where shared
writers are explicitly required.

Common client methods:

- `client:info()`
- `client:close()`
- `client:acquire(req)`
- `client:acquire_for_update(req, handler)`
- `client:describe(req)`
- `client:get_raw(req, dest)`
- `client:get_json(req)`
- `client:update_raw(req, body)`
- `client:update_json(req, value)`
- `client:mutate(req)`
- `client:metadata(req)`
- `client:remove(req)`
- `client:keepalive(req)`
- `client:release(req)`
- `client:attach(req, body)`
- `client:list_attachments(req)`
- `client:get_attachment(req, dest)`
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
- `client:query_raw(req, dest)` (`req.engine` and `req.refresh` may select the
  query engine/refresh mode; document-query trailer metadata is returned as
  `metadata_json`)
- `client:get_namespace_config(req)`
- `client:update_namespace_config(req)`
- `client:flush_index(req)`
- `client:new_workflow(config)`
- `client:subscribe(req, handler)`
- `client:subscribe_with_state(req, handler)`
- `client:watch_queue(req, handler)`
- `client:new_consumer_service(...)`
- `client:start_consumer(...)`

The Lua binding intentionally excludes the TC/XA administrative APIs.

## Inbox/outbox workflows

`client:new_workflow(config)` creates the inbox/outbox parent receiver. Its
`namespace` (or `namespace_name`) contains both durable inbox and outbox keys;
the private dispatcher signals committed keys locally and performs recovery
internally. Lua code never supplies an xid, manages the dispatcher, or receives
a callback from its thread.

```lua
local workflow = assert(client:new_workflow({
  namespace = "orders-workflow",
  owner = "orders-api",
  max_attempts = 100,
}))

local txn, receipt = assert(workflow:append_outbox({
  operation_id = order_id,
  effect_id = "charge-card",
  effect_key = "charge:" .. order_id,
  kind = "http",
  destination = "https://payments.example/charges",
  headers = { ["idempotency-key"] = "charge:" .. order_id },
}, payload_source))

local order = assert(txn:acquire({ namespace_name = "orders", key = order_id }))
assert(order:update_json({ status = "payment_pending" }))
order:close()
assert(txn:commit())
txn:close()

local job = assert(workflow:next(1000))
-- Stream arbitrary bytes to a host-owned request-body sink; no dispatcher
-- thread enters the Lua VM.
assert(job:write_payload(foreign_request_body_sink))
assert(job:complete())
job:close()
```

`headers` is the façade convenience form and is JSON-encoded into the durable
`headers_json` envelope. Pass `headers_json` directly when it is already
serialized; supplying both is an error. `workflow:accept_inbox(message)`
returns `nil, result` on an accepted duplicate, where `result.duplicate` is
true. Otherwise it returns a `WorkflowTransaction` and `result.accepted` is
true.

Workflow receivers are explicit and owned:

- `workflow:append_outbox(entry, payload)`
- `workflow:accept_inbox(message)`
- `workflow:next(timeout_ms)`
- `workflow:close()`
- `txn:acquire(req)`, `txn:append_outbox(entry, payload)`, `txn:commit()`,
  `txn:rollback()`, `txn:close()`
- participant `describe`, `get_raw`, `get_json`, `update_raw`, `update_json`,
  metadata, remove, keepalive, and attachment methods
- job `info`, `write_payload` (also `payload`), `payload_json`, `renew`,
  `complete`, `retry`, `dead_letter`, and `close`

Participants deliberately have no release or terminal-decision method. A
workflow job is the only handoff to host effect execution; keep its claim
alive with `job:renew()` for longer foreign operations, then select exactly one
terminal operation.

`client:acquire_for_update(req, handler)` wraps the common acquire, snapshot,
update, release workflow. The handler receives a context table with:

- `lease`
- `state`
- `state_meta`
- `load_json()`
- `update_raw(body, req)`
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
- `lease:get_raw(req, dest)`
- `lease:get_json(req)`
- `lease:update_raw(body, req)`
- `lease:update_json(value, req)`
- `lease:mutate(req)`
- `lease:mutate_local(req)`
- `lease:metadata(req)`
- `lease:remove(req)`
- `lease:keepalive(req)`
- `lease:release(req)`
- `lease:attach(req, body)`
- `lease:list_attachments()`
- `lease:get_attachment(req, dest)`
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
- `message:payload(dest)`
- `message:payload_json()`

## JSON helpers

The binding uses `lonejson` internally for JSON encode/decode helpers.

Use:

- `lockdc.encode_json(value)`
- `lockdc.decode_json(payload)`
- `lease:get_json()`
- `lease:update_json(value, req)`
- `message:payload_json()`

These helpers are for idiomatic Lua workflow code. They do not replace the
mapped `lonejson` APIs in the C SDK; those C APIs use caller-defined
`LONEJSON_FIELD_*` maps for typed state load/save. The Lua helpers sit on top
of the public `liblockdc` JSON transport surface and use the `lonejson`
dependency version declared by the `lockdc` rock.

Top-level JSON `null` is returned as `lockdc.json_null`. That keeps successful
JSON `null` payloads distinct from the binding's existing `nil, err` and
`nil, meta` return conventions.

## Consumer model

The Lua consumer API is intentionally blocking and single-threaded.

- handlers run on the calling Lua state
- one message is processed at a time
- after the handler completes, the next dequeue happens
- the binding does not expose native threaded callback dispatch into a shared
  Lua VM

This is deliberate. Native worker threads calling back into the same Lua state
would require a separate synchronization and dispatch model.

The supported Lua consumer paths are:

- `client:start_consumer(...)`
- `client:new_consumer_service(...):run()`
- `client:new_consumer_service(...):start()`

`Service:start()` is a blocking alias for `run()`.
Each blocking Lua consumer service supports exactly one consumer config. Start
separate blocking consumers if you need to consume separate queues from Lua.

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

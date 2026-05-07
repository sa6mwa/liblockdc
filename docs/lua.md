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
- `lonejson == 0.8.0-1`
- Lua `>= 5.5, < 5.6`

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
- `client:query_raw(req, dest)`
- `client:get_namespace_config(req)`
- `client:update_namespace_config(req)`
- `client:flush_index(req)`
- `client:subscribe(req, handler)`
- `client:subscribe_with_state(req, handler)`
- `client:watch_queue(req, handler)`
- `client:new_consumer_service(...)`
- `client:start_consumer(...)`

The Lua binding intentionally excludes the TC/XA administrative APIs.

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
mapped `lonejson` APIs in the C SDK; they sit on top of the public `liblockdc`
JSON transport surface.

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
- `examples/lua/consumer_handler.lua`

See also:

- [examples/lua/README.md](../examples/lua/README.md)

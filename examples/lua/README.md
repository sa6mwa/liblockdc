# Lua Examples

These examples are written to show the intended Lua DX directly. They do not
hide request shaping, result handling, or JSON/state operations behind reusable
helpers.

## Prerequisites

Install the `lockdc` and `lonejson` Lua rocks against a released or staged
`liblockdc` SDK bundle.

For normal LuaRocks use, install the matching `liblockdc` binary SDK release
first, then install the Lua rock. The `lockdc` rock does not build the full C
SDK for you; it builds only the Lua binding and links it against the matching
installed `liblockdc` release.

`liblockdc` owns this Lua dependency layout. Downstream consumers such as
`vectis` should use the `lockdc` and `lonejson` Lua modules shipped with the
`liblockdc` SDK bundle rather than repackaging a separate `lockd` Lua client.

When running from this repository after building and staging the SDK locally,
one workable path is:

```bash
eval "$(luarocks --tree ./build/luarocks path --lua-version 5.5)"
export LD_LIBRARY_PATH="$PWD/build/install-tree-sdk-test/prefix/lib:${LD_LIBRARY_PATH:-}"
```

## Common environment

The examples read these variables when present:

- `LOCKDC_URL`
- `LOCKDC_CLIENT_PEM`
- `LOCKDC_NAMESPACE`
- `LOCKDC_KEY`
- `LOCKDC_OWNER`
- `LOCKDC_QUEUE`

Defaults assume the local development environment from this repository.

Start it with:

```bash
make dev-up
```

## Examples

- `acquire_update_json.lua`
  acquire a lease, read JSON state, update JSON state, and release
- `queue_roundtrip.lua`
  enqueue a JSON payload, dequeue it, inspect it, and acknowledge it
- `namespace_config.lua`
  read namespace engine configuration and trigger an index flush
- `consumer_handler.lua`
  run a stateful queue consumer with explicit message and lease handling

## Consumer model

The Lua consumer path is intentionally simple and single-threaded.

- `client:start_consumer(...)` is blocking
- `client:new_consumer_service(...):run()` is blocking
- `client:new_consumer_service(...):start()` is also blocking
- one message is consumed at a time
- the Lua handler runs to completion on the calling Lua state
- after the handler completes, the next message is consumed

This is deliberate. The Lua binding does not expose the native threaded C
consumer callback model because calling back into the same Lua state from
multiple native threads would be unsafe.

In practical terms, the intended Lua DX is:

1. start one blocking consumer loop
2. handle one message
3. update state or attachments if needed
4. ack or nack
5. continue to the next message

For the broader Lua SDK surface and distribution policy, see `docs/lua.md`.

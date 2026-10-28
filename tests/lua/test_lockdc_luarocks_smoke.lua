local lockdc = require("lockdc")
assert(lockdc.allocator_tracking_configure(true))

assert(type(lockdc.version_string()) == "string")

local payload = lockdc.encode_json({
  kind = "luarocks-smoke",
  answer = 42,
  ok = true,
})

local decoded = lockdc.decode_json(payload)

assert(decoded.kind == "luarocks-smoke")
assert(decoded.answer == 42)
assert(decoded.ok == true)

assert(lockdc.encode_json(lockdc.json_null) == "null")
assert(lockdc.decode_json("null") == lockdc.json_null)

local with_null = lockdc.decode_json('{"value":null,"items":[null]}')
assert(with_null.value == lockdc.json_null)
assert(with_null.items[1] == lockdc.json_null)

local client, err = lockdc.open({})

assert(client == nil)
assert(type(err) == "table")
assert(type(err.message) == "string")

-- Exercise the installed native module and facade against the installed SDK.
local root = assert(os.getenv("LOCKDC_LUA_SMOKE_ROOT"))
local embedded = assert(lockdc.open({ endpoints = { "pouch://" .. root },
  pouch = { background_compaction = false } }))
local collection = assert(embedded:create_collection({ name = "installed-writer" }))
local writer = assert(collection:append_begin({ append_id = "event", collection_retention_seconds = 7776000 }))
assert(writer:write('{ "installed" : '))
assert(writer:write('true }'))
local result = assert(writer:commit())
assert(result.position == "1" and result.collection_retention_expires_at_unix > os.time())
writer:close()
local payload = {}
local _, progress = collection:read({}, { write = function(chunk) payload[#payload + 1] = chunk end })
assert(progress.complete and table.concat(payload) == '{"installed":true}\n')
assert(collection:drop())
collection:close()
embedded:close()
collectgarbage("collect")

local core = require("lockdc.core")
assert(core.allocator_tracking_configure(true))
local key = assert(core.pouch_crypto_generate_key())
local function rejected(value, err, code)
  assert(value == nil and err and err.message and code == core.ERR_INVALID,
    err and err.message or "expected structured failure")
end
local function read(collection)
  local bytes = 0
  local value, result, code = collection:read(nil, { write = function(chunk)
    bytes = bytes + #chunk
  end })
  return value, result, code, bytes
end
for mode = 0, 3 do
  local client = assert(core.open({
    endpoints = { "pouch://" .. lockdc_test_root .. "-leases-" .. mode },
    default_namespace = "journal", pouch = { background_compaction = false,
      compression = mode >= 2 and "zlib" or "none", durable_sync = mode % 2 == 0,
      crypto_key = mode % 2 == 1 and key or nil } }))
  local caps = assert(client:collection_capabilities())
  assert(caps.leases and caps.lease_takeover and caps.private_reads)
  local owner = assert(client:create_collection({ name = "events",
    lease = { owner = "cai", ttl_seconds = 60 } }))
  local public = assert(client:open_collection({ name = "events" }))
  local info = assert(public:describe())
  assert(info.lease.active and info.lease.owner == "cai")
  assert(info.lease.fencing_token == "1" and not info.lease.private_read)
  rejected(public:acquire({ owner = "cai" }))
  rejected(public:append({ append_id = "denied" }, "{}"))
  assert(owner:append({ append_id = "first" }, "{}"))
  local value, result, code, bytes = read(public)
  assert(value == nil and code == nil and result.completed_records == "1" and bytes == 3)
  local renewed = assert(owner:keepalive({ ttl_seconds = 90 }))
  assert(renewed.active and renewed.lease_id == info.lease.lease_id)
  assert(owner:release())
  rejected(owner:append({ append_id = "closed" }, "{}"))
  owner = assert(public:acquire({ owner = "private", private_read = true }))
  local writer = assert(owner:append_begin({ append_id = "pending" }))
  assert(writer:write("{}"))
  value, result, code, bytes = read(public)
  rejected(value, result, code)
  assert(bytes == 0)
  info = assert(public:describe())
  local replacement = assert(public:takeover({ owner = "recovery", private_read = true }, info.lease.lease_id))
  rejected(public:takeover({ owner = "another" }, info.lease.lease_id))
  rejected(writer:commit())
  rejected(owner:keepalive({ ttl_seconds = 60 }))
  rejected(owner:release())
  assert(public:append_status("pending").state == "absent")
  assert(replacement:append({ append_id = "pending" }, "{}"))
  value, result, code, bytes = read(replacement)
  assert(value == nil and code == nil and bytes == 6)
  replacement:close() -- Local close retains durable ownership.
  rejected(public:acquire({ owner = "blocked" }))
  info = assert(public:describe())
  replacement = assert(public:takeover({ owner = "last" }, info.lease.lease_id))
  assert(replacement:release())
  assert(public:append({ append_id = "multiwriter-again" }, "{}"))
  writer:close()
  owner:close()
  public:close()
  client:close()
end

-- The facade exposes the same receiver lifecycle and raw byte source/sink API.
package.preload.lonejson = function()
  return { json_null = {}, encode_json = function() error("unexpected encoding") end,
    decode_json = function() error("unexpected decoding") end }
end
local file = debug.getinfo(1, "S").source:sub(2)
local repo = assert(file:match("^(.*)/tests/lua/[^/]+$"))
package.path = repo .. "/lua/?.lua;" .. repo .. "/lua/?/init.lua;" .. package.path
local facade = require("lockdc")
local client = assert(facade.open({ endpoints = { "pouch://" .. lockdc_test_root .. "-facade" },
  default_namespace = "journal", pouch = { background_compaction = false } }))
local public = assert(client:create_collection({ name = "events" }))
local owner = assert(public:acquire({ owner = "facade" }))
assert(owner:append({ append_id = "entry" }, "{}"))
local info = assert(public:describe())
local replacement = assert(public:takeover({ owner = "replacement" }, info.lease.lease_id))
assert(replacement:keepalive({ ttl_seconds = 30 }))
assert(replacement:release())
owner:close()
public:close()
client:close()
collectgarbage("collect")
print("collection lease C/Lua parity passed")

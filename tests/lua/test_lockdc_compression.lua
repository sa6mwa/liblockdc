local core = require("lockdc.core")
assert(core.allocator_tracking_configure(true))
package.preload.lonejson = function()
  return { json_null = {}, encode_json = function() error("unexpected JSON encoding") end,
    decode_json = function() error("unexpected JSON decoding") end }
end
local repo = debug.getinfo(1, "S").source:sub(2):match("^(.*)/tests/lua/[^/]+$")
assert(repo)
package.path = repo .. "/lua/?.lua;" .. repo .. "/lua/?/init.lua;" .. package.path
local lockdc = require("lockdc")
local key = assert(core.pouch_crypto_generate_key())
for _, encrypted in ipairs({ false, true }) do
  local function open(mode)
    return assert(lockdc.open({
      endpoints = { "pouch://" .. lockdc_test_root .. (encrypted and "-encrypted" or "-plain") },
      default_namespace = "journal",
      pouch = { single_writer = false, background_compaction = false,
        compression = mode, query_indexing = false, crypto_key = encrypted and key or nil },
    }))
  end
  local client = open("none")
  local collection = assert(client:create_collection({ name = "events" }))
  local entry = assert(collection:append({ append_id = "event-1" }, '{ "n": 1 }'))
  assert(entry.position == "1")
  local changed = assert(collection:entry_metadata("1", { retention_seconds = 7776000 }))
  assert(changed.etag == entry.etag and changed.retention_expires_at_unix > 0)
  collection:close()
  client:close()
  for _, mode in ipairs({ "zlib", "inherit", "none" }) do
    client = open(mode ~= "inherit" and mode or nil)
    collection = assert(client:open_collection({ name = "events" }))
    local chunks = {}
    local value, result = collection:read({}, { write = function(bytes)
      chunks[#chunks + 1] = bytes
    end })
    assert(value == nil and result.complete, result and result.message)
    assert(table.concat(chunks) == '{"n":1}\n')
    local status = assert(collection:append_status("event-1"))
    assert(status.state == "committed" and status.entry.etag == entry.etag)
    assert(status.entry.retention_expires_at_unix == changed.retention_expires_at_unix)
    changed = assert(collection:entry_metadata("1", { retention_seconds = 7776000 }))
    collection:close()
    client:close()
  end
end
collectgarbage("collect")
print("Lua compression conversion passed")

local core = require("lockdc.core")
assert(core.allocator_tracking_configure(true))
local key = assert(core.pouch_crypto_generate_key())
local function rejected(value, err, code, expected)
  assert(value == nil and err and err.message and code == (expected or core.ERR_INVALID),
    err and err.message or "expected structured failure")
end
local function read(collection, options)
  local count, first = 0
  local value, result, code = collection:read(options, { write = function(bytes)
    count = count + #bytes
    first = first or bytes
  end })
  assert(value == nil and code == nil, result and result.message)
  return count, result, first
end
for mode = 0, 3 do
  local config = { endpoints = { "pouch://" .. lockdc_test_root .. "-mode-" .. mode },
    default_namespace = "journal", pouch = { background_compaction = false,
      compression = mode >= 2 and "zlib" or "none", crypto_key = mode % 2 == 1 and key or nil,
      durable_sync = true } }
  local client = assert(core.open(config))
  local capabilities = assert(client:collection_capabilities())
  assert(capabilities.append_writer and capabilities.append_collection_retention)
  local collection = assert(client:create_collection({ name = "events", retention_seconds = 7776000 }))
  local writer = assert(collection:append_begin({ append_id = "large", collection_retention_seconds = 7776000 }))
  assert(writer:write('{ "value" : "'))
  local chunk = string.rep("x", 4096)
  lockdc_test_lua_heap_limit(65536)
  for _ = 1, 1024 do assert(writer:write(chunk)) end
  lockdc_test_lua_heap_limit(0)
  assert(collection:describe().live_count == "0")
  assert(writer:write('" }'))
  local result = assert(writer:commit())
  assert(result.position == "1" and result.bytes == tostring(4096 * 1024 + 12))
  assert(result.collection_retention_expires_at_unix >= os.time() + 7775990)
  writer:close()
  local bytes, progress = read(collection)
  assert(bytes == tonumber(result.bytes) + 1 and progress.completed_records == "1")
  local deadline = collection:describe().retention_expires_at_unix
  local retry = assert(collection:append_begin({ append_id = "large", collection_retention_seconds = 1 }))
  assert(retry:write('{"value":"'))
  for _ = 1, 1024 do assert(retry:write(chunk)) end
  assert(retry:write('"}'))
  local duplicate = assert(retry:commit())
  assert(duplicate.duplicate and duplicate.etag == result.etag)
  assert(duplicate.collection_retention_expires_at_unix == result.collection_retention_expires_at_unix)
  assert(collection:describe().retention_expires_at_unix == deadline)
  retry:close()
  -- Force canonical compaction in every codec mode, then stream the rebound
  -- payload again. This must preserve upload AAD and the original retry fact.
  assert(lockdc_test_maintain_collections(client, true))
  local compact_bytes, compact_progress = read(collection)
  assert(compact_bytes == bytes and compact_progress.completed_records == "1")
  assert(collection:append_status("large").entry.etag == result.etag)
  local bad = assert(collection:append_begin({ append_id = "bad", collection_retention_seconds = 1 }))
  rejected(bad:write("[]"))
  rejected(bad:commit())
  assert(bad:abort() and bad:abort())
  bad:close()
  assert(collection:append_status("bad").state == "absent")
  local abandoned = assert(collection:append_begin({ append_id = "abandoned" }))
  assert(abandoned:write('{"unfinished":'))
  abandoned:close()
  assert(collection:append_status("abandoned").state == "absent")
  local gc = assert(collection:append_begin({ append_id = "gc" }))
  assert(gc:write("{}"))
  gc = nil
  collectgarbage("collect")
  assert(collection:append_status("gc").state == "absent")
  local uncertain = assert(collection:append_begin({ append_id = "uncertain", collection_retention_seconds = 7776000 }))
  assert(uncertain:write("{}"))
  lockdc_test_fail_collection_result()
  local value, err, code = uncertain:commit()
  rejected(value, err, code, core.ERR_NOMEM)
  local committed = assert(collection:append_status("uncertain"))
  assert(committed.state == "committed" and committed.entry.position == "2")
  assert(committed.entry.collection_retention_expires_at_unix > os.time())
  uncertain:close()
  local closing = assert(client:open_collection({ name = "events" }))
  rejected(closing:append_begin(setmetatable({ append_id = "closed-option" }, {
    __index = function(_, field)
      if field == "retention_seconds" then closing:close() end
    end,
  })))
  collection:close()
  client:close()
  client = assert(core.open(config))
  collection = assert(client:open_collection({ name = "events" }))
  local reopened_bytes, reopened_progress = read(collection)
  assert(reopened_bytes == bytes + 3 and reopened_progress.completed_records == "2")
  assert(collection:append_status("large").entry.etag == result.etag)
  assert(collection:describe().retention_expires_at_unix ==
    committed.entry.collection_retention_expires_at_unix)
  collection:close()
  client:close()
end
-- Exercise the facade itself while forbidding implicit JSON conversion.
package.preload.lonejson = function()
  return { json_null = {}, encode_json = function() error("unexpected encoding") end,
    decode_json = function() error("unexpected decoding") end }
end
local file = debug.getinfo(1, "S").source:sub(2)
local repo = assert(file:match("^(.*)/tests/lua/[^/]+$"))
package.path = repo .. "/lua/?.lua;" .. repo .. "/lua/?/init.lua;" .. package.path
local lockdc = require("lockdc")
local config = { endpoints = { "pouch://" .. lockdc_test_root .. "-facade" },
  default_namespace = "journal", pouch = { background_compaction = false, compression = "zlib" } }
local client = assert(lockdc.open(config))
local collection = assert(client:create_collection({ name = "facade" }))
local writer = assert(collection:append_begin({ append_id = "first", collection_retention_seconds = 0 }))
collection:close()
client:close()
assert(writer:write('{ "raw" : true }'))
local result = assert(writer:commit())
assert(result.position == "1" and result.collection_retention_expires_at_unix == 0)
writer:close()
client = assert(lockdc.open(config))
collection = assert(client:open_collection({ name = "facade" }))
local bytes, progress, first = read(collection)
assert(bytes == 13 and progress.completed_records == "1" and first == '{"raw":true}')
collection:close()
client:close()
collectgarbage("collect")
print("Lua incremental collection writers: codecs, bounded chunks, retries and cleanup passed")

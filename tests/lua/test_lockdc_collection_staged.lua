local core = require("lockdc.core")
assert(core.allocator_tracking_configure(true))
local key = assert(core.pouch_crypto_generate_key())
local chunk = string.rep("x", 16384)
local function rejected(value, err, code, expected)
  assert(value == nil and err and err.message and code == (expected or core.ERR_INVALID),
    err and err.message or "expected structured failure")
end
local function source(fail)
  local part, offset, closed = 1, 1, 0
  local src = {
    read = function(n)
      if fail == "early" or (fail == "late" and part == 18) then
        error("injected staged source failure")
      end
      local bytes = part == 1 and '{"v":"' or part <= 17 and chunk or part == 18 and '"}'
      if not bytes then return nil end
      local result = bytes:sub(offset, offset + n - 1)
      offset = offset + #result
      if offset > #bytes then part, offset = part + 1, 1 end
      return result
    end,
    reset = function() error("unexpected staged source reset") end,
    close = function() closed = closed + 1 end,
  }
  return src, function() return closed end
end

for mode = 0, 3 do
  local config = { endpoints = { "pouch://" .. lockdc_test_root .. "-staged-" .. mode },
    default_namespace = "journal", pouch = { background_compaction = false,
      compression = mode >= 2 and "zlib" or "none", crypto_key = mode % 2 == 1 and key or nil,
      durable_sync = mode % 2 == 0 } }
  local client = assert(core.open(config))
  assert(client:collection_capabilities().append_staged)
  local collection = assert(client:create_collection({ name = "events" }))
  local peer = assert(client:open_collection({ name = "events" }))
  local src, closed = source()
  local first = assert(collection:append_staged({ append_id = "large", retention_seconds = 60,
    collection_retention_seconds = 120 }, src))
  assert(first.position == "1" and first.bytes == tostring(16 * #chunk + 8) and closed() == 1)
  local deadline = collection:describe().retention_expires_at_unix
  src, closed = source()
  local duplicate = assert(collection:append_staged({ append_id = "large",
    collection_retention_seconds = 1 }, src))
  assert(duplicate.duplicate and duplicate.position == "1" and duplicate.etag == first.etag)
  assert(closed() == 1 and collection:describe().retention_expires_at_unix == deadline)
  local writer = assert(collection:append_begin({ append_id = "large" }))
  assert(writer:write('{"v":"'))
  for _ = 1, 16 do assert(writer:write(chunk)) end
  assert(writer:write('"}'))
  assert(writer:commit().duplicate)
  writer:close()
  for _, failure in ipairs({"early", "late"}) do
    src, closed = source(failure)
    local value, err, code = collection:append_staged({ append_id = failure,
      collection_retention_seconds = 1 }, src)
    rejected(value, err, code)
    assert(err.message:find("injected staged source failure", 1, true))
    assert(closed() == 1 and collection:append_status(failure).state == "absent")
    assert(collection:describe().retention_expires_at_unix == deadline)
  end
  local close_count = 0
  rejected(collection:append_staged({ append_id = "oversized" }, {
    read = function(n) return string.rep("x", n + 1) end,
    close = function() close_count = close_count + 1 end,
  }))
  assert(close_count == 1 and collection:append_status("oversized").state == "absent")
  rejected(collection:append_staged({ append_id = "truncated" }, '{"v":'))
  rejected(collection:append_staged({ append_id = "conflict" }, "[]"))
  rejected(collection:append_staged({ append_id = "large" }, "{}"))
  lockdc_test_fail_collection_result()
  local value, err, code = collection:append_staged({ append_id = "uncertain" }, "{}")
  rejected(value, err, code, core.ERR_NOMEM)
  -- The assertion below verifies committed identity even when Lua could not
  -- allocate the success result. Use the ordinary error convention explicitly.
  local status = assert(collection:append_status("uncertain"))
  assert(status.state == "committed" and status.entry.position == "2")
  local calls, source_closed = 0, 0
  local result = assert(collection:append_staged({ append_id = "closed-owners" }, {
    read = function()
      calls = calls + 1
      if calls == 1 then
        rejected(collection:describe())
        assert(peer:describe().live_count == "2")
        rejected(peer:append_staged({ append_id = "nested" }, "{}"))
        return '{"closed":true}'
      end
      peer:close()
      collection:close()
      client:close()
      return nil
    end,
    reset = function() error("unexpected reset after close") end,
    close = function() source_closed = source_closed + 1 end,
  }))
  assert(result.position == "3" and calls == 2 and source_closed == 1)
  client = assert(core.open(config))
  collection = assert(client:open_collection({ name = "events" }))
  local count = 0
  local value, progress, code = collection:read(nil, { write = function(bytes) count = count + #bytes end })
  assert(value == nil and code == nil and progress.completed_records == "3")
  assert(count == tonumber(first.bytes) + 1 + 3 + 16)
  collection:close()
  client:close()
end

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
local collection = assert(client:create_collection({ name = "events" }))
assert(client:collection_capabilities().append_staged)
assert(collection:append_staged({ append_id = "raw" }, '{ "raw" : true }').bytes == "12")
assert(collection:append_staged({ append_id = "raw" }, '{"raw":true}').duplicate)
collection:close()
client:close()
collectgarbage("collect")
print("Staged pull C/Lua parity: codecs, sources, errors, retry and owner closure passed")

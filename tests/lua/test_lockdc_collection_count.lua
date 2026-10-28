local core = require("lockdc.core")
assert(core.allocator_tracking_configure(true))
local client = assert(core.open({ endpoints = { "pouch://" .. lockdc_test_root },
  default_namespace = "journal", pouch = { background_compaction = false } }))
local function rejected(value, err, code)
  assert(value == nil and err and err.message and code == core.ERR_INVALID)
end
local function read(collection)
  local chunks = {}
  local value, result, code = collection:read({}, {
    write = function(chunk) chunks[#chunks + 1] = chunk end,
  })
  assert(value == nil and code == nil, result and result.message)
  return table.concat(chunks), result
end
  local limited = assert(client:create_collection({ name = "count-limited", max_entries = "2" }))
  assert(limited:describe().max_entries == "2")
  assert(limited:metadata({}).max_entries == "2")
  for n = 1, 5 do
    assert(limited:append({ append_id = "event-" .. n }, '{"n":' .. n .. '}'))
  end
  assert(limited:describe().live_count == "5") -- Lazy, never trim on append.
  assert(lockdc_test_maintain_collections(client))
  local payload, result = read(limited)
  assert(payload == '{"n":4}\n{"n":5}\n' and result.completed_records == "2")
  assert(limited:append_status("event-1").state == "deleted")
  local retry = assert(limited:append({ append_id = "event-1" }, '{"n":1}'))
  assert(retry.duplicate and retry.deleted and retry.position == "1")
  for _, invalid in ipairs({ 0, 2, -1, true, false, {}, "", "01", "-1", "1.5", "18446744073709551616" }) do
    rejected(limited:metadata({ max_entries = invalid }))
    rejected(client:create_collection({ name = "bad-limit", max_entries = invalid }))
  end
  rejected(limited:entry_metadata("4", { max_entries = "0" }))
  rejected(client:open_collection({ name = "count-limited", max_entries = "0" }))
  assert(limited:metadata({ max_entries = "18446744073709551615" }).max_entries == "18446744073709551615")
  assert(limited:metadata({ max_entries = "0" }).max_entries == "0")
  assert(limited:append({ append_id = "six" }, '{"n":6}').position == "6")
  assert(lockdc_test_maintain_collections(client))
  assert(limited:describe().live_count == "3")
  local peer = assert(client:open_collection({ name = "count-limited" }))
  assert(peer:describe().max_entries == "0")
  assert(peer:metadata({ max_entries = "1" }).max_entries == "1")
  assert(lockdc_test_maintain_collections(client))
  payload = read(limited)
  assert(payload == '{"n":6}\n')
  assert(peer:drop())
  peer:close()
  limited:close()
client:close()
collectgarbage("collect")
print("Lua count retention: lazy trim, policy changes, uint64 validation, retry identity passed")

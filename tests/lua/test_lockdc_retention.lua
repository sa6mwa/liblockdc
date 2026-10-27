local core = require("lockdc.core")
assert(core.allocator_tracking_configure(true))
local client = assert(core.open({
  endpoints = { "pouch://" .. lockdc_test_root },
  pouch = { background_compaction = false },
  disable_mtls = true,
}))
local function acquire(key)
  return assert(client:acquire({ namespace = "orders", key = key, owner = "lua-retention", ttl_seconds = 60 }))
end
local function committed(key)
  local body, meta = client:get({ namespace = "orders", key = key, public_read = true })
  assert(body ~= nil, meta and meta.message)
  assert(meta.retention_support == "supported")
  return meta
end
local lease = acquire("state")
local info = assert(lease:info())
assert(info.retention_support == "supported" and info.retention_expires_at_unix == nil)

-- Rejection happens before mutation-array allocation or source invocation.
for _, invalid in ipairs({ -1, 0.5, true, false, "30", {}, math.huge }) do
  local function rejected(result, err, code)
    assert(result == nil and code == core.ERR_INVALID and err.message)
  end
  rejected(lease:update("{}", { retention_seconds = invalid }))
  rejected(lease:mutate({ mutations = { "/value=1" }, retention_seconds = invalid }))
  rejected(lease:metadata({ retention_seconds = invalid }))
  rejected(lease:mutate_local({ mutations = { "/value=1" }, retention_seconds = invalid }))
end

info = assert(lease:update('{"value":1}', { retention_seconds = 120 }))
assert(info.retention_support == "supported" and info.retention_expires_at_unix == nil)
local _, private = lease:get()
assert(private.retention_support == "supported" and private.retention_expires_at_unix == nil)
assert(lease:update('{"value":2}')) -- Preserve the private directive.
assert(lease:release())
local retained = committed("state")
assert(retained.retention_expires_at_unix > 0)
local deadline = retained.retention_expires_at_unix

lease = acquire("state")
local _, read = lease:get()
assert(read.retention_expires_at_unix == deadline)
assert(lease:mutate({ mutations = { "/value=3" } }))
assert(lease:metadata({ query_hidden = false }))
assert(lease:release())
assert(committed("state").retention_expires_at_unix == deadline)

lease = acquire("state")
info = assert(lease:metadata({ retention_seconds = 0 }))
assert(info.retention_expires_at_unix == 0)
assert(lease:mutate_local({ mutations = { "/value=4" } }))
assert(lease:release())
assert(committed("state").retention_expires_at_unix == 0)

lease = acquire("state")
local request = lease:info()
request.retention_seconds = 120
info = assert(client:update(request, '{"value":5}'))
assert(info.retention_support == "supported" and info.retention_expires_at_unix == nil)
assert(lease:describe())
request = lease:info()
request.retention_seconds = 0
info = assert(client:metadata(request))
assert(info.retention_expires_at_unix == 0)
request.mutations = { "/value=6" }
request.retention_seconds = nil
info = assert(client:mutate(request))
assert(info.retention_expires_at_unix == 0)
assert(lease:release())
assert(committed("state").retention_expires_at_unix == 0)

-- Callback snapshots expose the same metadata and accept retention in writes.
assert(client:acquire_for_update({ namespace = "orders", key = "state", owner = "lua", ttl_seconds = 60 },
  function(update)
    assert(update.state_meta.retention_support == "supported")
    assert(update.state_meta.retention_expires_at_unix == 0)
    return update.lease:update('{"value":7}', { retention_seconds = 120 })
  end))
assert(committed("state").retention_expires_at_unix > 0)

local outbox = assert(client:new_outbox({ namespace = "effects" }))
local txn = assert(outbox:begin())
local participant = assert(txn:acquire({ namespace = "orders", key = "participant" }))
info = assert(participant:update('{"value":1}', { retention_seconds = 120 }))
assert(info.retention_support == "supported" and info.retention_expires_at_unix == nil)
info = assert(participant:metadata({ retention_seconds = 0 }))
assert(info.retention_expires_at_unix == 0)
assert(participant:mutate_local({ mutations = { "/value=2" } }))
participant:close()
assert(txn:commit())
txn:close()
assert(committed("participant").retention_expires_at_unix == 0)
outbox:close()
client:close()
collectgarbage("collect")
print("Lua state retention passed")

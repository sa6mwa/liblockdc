local lockdc = require("lockdc")

local root = assert(os.getenv("LOCKDC_POUCH_ROOT"), "LOCKDC_POUCH_ROOT is required")
local endpoint = "pouch://" .. root
local key_file = root .. "/pouch.key"
local explicit_false_key_file = root .. "/explicit-false/pouch.key"
local namespace_name = "lua-pouch"

local invalid_client, invalid_err = lockdc.open({
  endpoints = { endpoint },
  pouch_compression = "invalid",
})
if invalid_client ~= nil then
  invalid_client:close()
  error("invalid Pouch compression unexpectedly opened")
end
if type(invalid_err) ~= "table" or not (invalid_err.message or ""):match("compression") then
  error("invalid Pouch compression did not return a structured error")
end

local disabled_generation_client, disabled_generation_err = lockdc.open({
  endpoints = {
    endpoint .. "?crypto_key_file=" .. explicit_false_key_file
      .. "&crypto_generate_key_file=true",
  },
  pouch_crypto_generate_key_file = false,
})
if disabled_generation_client ~= nil or disabled_generation_err == nil then
  error("explicit Lua false did not override endpoint key-file generation")
end
local explicit_false_key = io.open(explicit_false_key_file, "rb")
if explicit_false_key ~= nil then
  explicit_false_key:close()
  error("endpoint key-file generation ignored explicit Lua false")
end

local client, err = lockdc.open({
  endpoints = { endpoint },
  default_namespace = namespace_name,
  pouch_crypto_key_file = key_file,
  pouch_crypto_generate_key_file = true,
  pouch_compression = "zlib",
})
if client == nil then
  error(("encrypted Pouch open failed: %s"):format(err and err.message or tostring(err)))
end

local second_client, second_err = lockdc.open({
  endpoints = { endpoint },
  pouch_crypto_key_file = key_file,
  pouch_compression = "zlib",
})
if second_client ~= nil then
  second_client:close()
  client:close()
  error("second exclusive Pouch writer unexpectedly opened")
end
if type(second_err) ~= "table" or not (second_err.message or ""):match("writer") then
  client:close()
  error("second Pouch writer did not return a structured writer error")
end

local lease, acquire_err = client:acquire({
  key = "state",
  owner = "lua-pouch-e2e",
  ttl_seconds = 30,
})
if lease == nil then
  client:close()
  error(("Pouch acquire failed: %s"):format(acquire_err and acquire_err.message or tostring(acquire_err)))
end

local updated, update_err = lease:update_json({ source = "lua-pouch", value = 1 })
if updated == nil then
  lease:close()
  client:close()
  error(("Pouch state update failed: %s"):format(update_err and update_err.message or tostring(update_err)))
end
assert(lease:release())

local state, state_meta_or_err = client:get_json({ key = "state" })
if state == nil or state.source ~= "lua-pouch" or state.value ~= 1 then
  client:close()
  error(("Pouch state read failed: %s"):format(state_meta_or_err and state_meta_or_err.message or tostring(state_meta_or_err)))
end

local enqueued, enqueue_err = client:enqueue({
  queue = "work",
  content_type = "application/json",
  visibility_timeout_seconds = 30,
  ttl_seconds = 30,
}, lockdc.encode_json({ source = "lua-pouch" }))
if enqueued == nil then
  client:close()
  error(("Pouch enqueue failed: %s"):format(enqueue_err and enqueue_err.message or tostring(enqueue_err)))
end

local message, dequeue_err = client:dequeue({
  queue = "work",
  owner = "lua-pouch-e2e",
  visibility_timeout_seconds = 30,
  wait_seconds = 0,
})
if message == nil then
  client:close()
  error(("Pouch dequeue failed: %s"):format(dequeue_err and dequeue_err.message or tostring(dequeue_err)))
end
local payload, written_or_err = message:payload_json()
if payload == nil or payload.source ~= "lua-pouch" or type(written_or_err) ~= "number" then
  message:close()
  client:close()
  error("Pouch queue payload did not round-trip through the Lua facade")
end
assert(message:ack())

local function assert_ok(operation, value, value_err)
  if value == nil then
    error(("%s failed: %s"):format(operation,
      value_err and value_err.message or tostring(value_err)))
  end
  return value
end

-- Transaction participants are parsed before any C-owned participant array is
-- allocated.  This exercises Lua's longjmp argument-error path in the real
-- binding rather than merely checking a returned lockdc error.
local malformed_txn_ok, malformed_txn_err = pcall(function()
  client:txn_commit({
    txn_id = assert(lockdc.xid_new()),
    participants = {
      "not a participant table",
    },
  })
end)
if malformed_txn_ok or not tostring(malformed_txn_err):match("table") then
  client:close()
  error("Lua XA participant validation did not reject a non-table participant")
end

local raw_txn_id = assert(lockdc.xid_new())
local raw_txn_participant = {
  namespace_name = namespace_name,
  key = "xa-state",
}
local raw_txn_lease = assert_ok("Lua raw XA acquire", client:acquire({
  key = raw_txn_participant.key,
  owner = "lua-pouch-xa",
  ttl_seconds = 30,
  txn_id = raw_txn_id,
}))
assert_ok("Lua raw XA stage",
          raw_txn_lease:update_json({ source = "lua-pouch-xa", committed = true }))
local prepared = assert_ok("Lua raw XA prepare", client:txn_prepare({
  txn_id = raw_txn_id,
  participants = { raw_txn_participant },
  expires_at_unix = 2147483647,
  tc_term = 1,
}))
if prepared.state ~= "prepare" then
  raw_txn_lease:close()
  client:close()
  error("Lua raw XA prepare did not return prepare state")
end
local committed = assert_ok("Lua raw XA commit", client:txn_commit({
  txn_id = raw_txn_id,
  participants = { raw_txn_participant },
  tc_term = 1,
}))
raw_txn_lease:close()
if committed.state ~= "commit" then
  client:close()
  error("Lua raw XA commit did not return commit state")
end
local replayed = assert_ok("Lua raw XA replay",
                           client:txn_replay({ txn_id = raw_txn_id }))
if replayed.state ~= "commit" then
  client:close()
  error("Lua raw XA replay did not preserve commit state")
end
local xa_state, xa_state_err = client:get_json({ key = raw_txn_participant.key })
if xa_state == nil or not xa_state.committed then
  client:close()
  error(("Lua raw XA state was not committed: %s"):format(
    xa_state_err and xa_state_err.message or tostring(xa_state_err)))
end
local query_keys = {}
local current_key
local query_result = assert_ok("Lua query_keys", client:query_keys({
  namespace_name = namespace_name,
  selector_json = '{"eq":{"field":"/source","value":"lua-pouch-xa"}}',
  engine = "scan",
}, {
  begin = function()
    current_key = ""
  end,
  chunk = function(bytes)
    current_key = current_key .. bytes
  end,
  finish = function()
    table.insert(query_keys, current_key)
  end,
}))
if #query_keys ~= 1 or query_keys[1] ~= raw_txn_participant.key or
    query_result.return_mode ~= "keys" then
  client:close()
  error("Lua query_keys did not stream the matching Pouch key")
end
local callback_result, callback_err = client:query_keys({
  namespace_name = namespace_name,
  selector_json = '{"eq":{"field":"/source","value":"lua-pouch-xa"}}',
  engine = "scan",
}, function()
  error("query key handler failure")
end)
if callback_result ~= nil or type(callback_err) ~= "table" or
    not (callback_err.message or ""):match("query key handler failure", 1, true) then
  client:close()
  error("Lua query_keys did not propagate handler failure")
end

local rollback_txn_id = assert(lockdc.xid_new())
local rollback_participant = {
  namespace_name = namespace_name,
  key = "xa-rollback-state",
}
local rollback_lease = assert_ok("Lua raw XA rollback acquire", client:acquire({
  key = rollback_participant.key,
  owner = "lua-pouch-xa-rollback",
  ttl_seconds = 30,
  txn_id = rollback_txn_id,
}))
assert_ok("Lua raw XA rollback stage", rollback_lease:update_json({ rolled_back = true }))
local rolled_back = assert_ok("Lua raw XA rollback", client:txn_rollback({
  txn_id = rollback_txn_id,
  participants = { rollback_participant },
  tc_term = 1,
}))
rollback_lease:close()
if rolled_back.state ~= "rollback" then
  client:close()
  error("Lua raw XA rollback did not return rollback state")
end

local tc_lease = assert_ok("Lua TC lease acquire", client:tc_lease_acquire({
  candidate_id = "lua-pouch-node",
  candidate_endpoint = "pouch://lua-pouch-node",
  term = 1,
  ttl_ms = 60000,
}))
if not tc_lease.granted then
  client:close()
  error("Lua TC lease was not granted")
end
local tc_renewed = assert_ok("Lua TC lease renew", client:tc_lease_renew({
  leader_id = "lua-pouch-node",
  term = tc_lease.term,
  ttl_ms = 60000,
}))
if not tc_renewed.renewed then
  client:close()
  error("Lua TC lease was not renewed")
end
local tc_leader = assert_ok("Lua TC leader", client:tc_leader())
if tc_leader.leader_id ~= "lua-pouch-node" then
  client:close()
  error("Lua TC leader did not return the current leader")
end
assert_ok("Lua TC lease release", client:tc_lease_release({
  leader_id = "lua-pouch-node",
  term = tc_renewed.term,
}))

local announced = assert_ok("Lua TC cluster announce", client:tc_cluster_announce({
  self_endpoint = "pouch://lua-pouch-node",
}))
if #announced.endpoints ~= 1 or announced.endpoints[1] ~= "pouch://lua-pouch-node" then
  client:close()
  error("Lua TC cluster announce did not return its endpoint")
end
local listed_cluster = assert_ok("Lua TC cluster list", client:tc_cluster_list())
if #listed_cluster.endpoints ~= 1 then
  client:close()
  error("Lua TC cluster list did not preserve membership")
end
local left_cluster = assert_ok("Lua TC cluster leave", client:tc_cluster_leave())
if #left_cluster.endpoints ~= 0 then
  client:close()
  error("Lua TC cluster leave did not remove membership")
end

local registered = assert_ok("Lua TC RM register", client:tc_rm_register({
  backend_hash = "lua-pouch-backend",
  endpoint = "pouch://lua-pouch-rm",
}))
if #registered.endpoints ~= 1 then
  client:close()
  error("Lua TC RM register did not return its endpoint")
end
local registered_rms = assert_ok("Lua TC RM list", client:tc_rm_list())
if #registered_rms.backends ~= 1 or
    registered_rms.backends[1].backend_hash ~= "lua-pouch-backend" then
  client:close()
  error("Lua TC RM list did not return the registered backend")
end
local unregistered = assert_ok("Lua TC RM unregister", client:tc_rm_unregister({
  backend_hash = "lua-pouch-backend",
  endpoint = "pouch://lua-pouch-rm",
}))
if #unregistered.endpoints ~= 0 then
  client:close()
  error("Lua TC RM unregister did not remove its endpoint")
end

local flush, flush_err = client:flush_index({ namespace_name = namespace_name, mode = "wait" })
if flush == nil then
  client:close()
  error(("Pouch index flush failed: %s"):format(flush_err and flush_err.message or tostring(flush_err)))
end
client:close()

local missing_key_client, missing_key_err = lockdc.open({
  endpoints = { endpoint },
  pouch_crypto_key_file = root .. "/missing.key",
  pouch_compression = "zlib",
})
if missing_key_client ~= nil then
  missing_key_client:close()
  error("Pouch open with a missing configured key file unexpectedly succeeded")
end
if type(missing_key_err) ~= "table" or not (missing_key_err.message or ""):match("key file") then
  error("missing Pouch key file did not return a structured error")
end

local reopened, reopen_err = lockdc.open({
  endpoints = { endpoint },
  default_namespace = namespace_name,
  pouch_crypto_key_file = key_file,
  pouch_compression = "zlib",
})
if reopened == nil then
  error(("encrypted Pouch reopen failed: %s"):format(reopen_err and reopen_err.message or tostring(reopen_err)))
end
local reopened_state, reopened_meta_or_err = reopened:get_json({ key = "state" })
if reopened_state == nil or reopened_state.value ~= 1 then
  reopened:close()
  error(("encrypted Pouch persistence read failed: %s"):format(reopened_meta_or_err and reopened_meta_or_err.message or tostring(reopened_meta_or_err)))
end
reopened:close()

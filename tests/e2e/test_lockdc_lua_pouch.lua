local lockdc = require("lockdc")

local root = assert(os.getenv("LOCKDC_POUCH_ROOT"), "LOCKDC_POUCH_ROOT is required")
local endpoint = "pouch://" .. root
local key_file = root .. "/pouch.key"
local explicit_false_key_file = root .. "/explicit-false/pouch.key"
local namespace_name = "lua-pouch"

local typed_invalid_client, typed_invalid_err = lockdc.open({
  endpoints = { endpoint .. "-typed-invalid" },
  pouch = { compression = "invalid" },
})
if typed_invalid_client ~= nil then
  typed_invalid_client:close()
  error("invalid typed Pouch compression unexpectedly opened")
end
if type(typed_invalid_err) ~= "table" or not (typed_invalid_err.message or ""):match("compression") then
  error("invalid typed Pouch compression did not return a structured error")
end

local remote_settings_client, remote_settings_err = lockdc.open({
  endpoints = { "https://lockd.invalid" },
  pouch = { durable_sync = true },
})
if remote_settings_client ~= nil then
  remote_settings_client:close()
  error("remote client unexpectedly accepted typed Pouch settings")
end
if type(remote_settings_err) ~= "table" or
    not (remote_settings_err.message or ""):match("require exactly one pouch endpoint") then
  error("remote typed Pouch settings did not return a structured local-only error")
end

local unknown_pouch_client, unknown_pouch_err = lockdc.open({
  endpoints = { endpoint .. "-typed-unknown" },
  pouch = { unexpected = true },
})
if unknown_pouch_client ~= nil then
  unknown_pouch_client:close()
  error("unknown typed Pouch setting unexpectedly opened")
end
if type(unknown_pouch_err) ~= "table" or
    not (unknown_pouch_err.message or ""):match("unknown pouch setting") then
  error("unknown typed Pouch setting did not return a structured error")
end

local wrong_type_client, wrong_type_err = lockdc.open({
  endpoints = { endpoint .. "-typed-wrong-type" },
  pouch = { query_indexing = "false" },
})
if wrong_type_client ~= nil then
  wrong_type_client:close()
  error("wrong typed Pouch value unexpectedly opened")
end
if type(wrong_type_err) ~= "table" or
    not (wrong_type_err.message or ""):match("must be a boolean") then
  error("wrong typed Pouch value did not return a structured error")
end

local typed_root = root .. "-typed"
local typed_client, typed_err = lockdc.open({
  endpoints = { "pouch://" .. typed_root .. "?compression=zlib&query_indexing=true" },
  pouch_compression = "zlib",
  pouch = {
    single_writer = false,
    durable_sync = false,
    fsync_batch_max_ops = 0,
    segment_target_bytes = 4096,
    indexer_flush_docs = 64,
    indexer_flush_interval_seconds = 1,
    background_compaction = false,
    disable_compaction_throttling = true,
    terminal_reclaim_min_bytes = 8192,
    queue_watch = false,
    query_engine = "scan",
    query_fallback_engine = "index",
    query_indexing = false,
    compression = "none",
  },
})
if typed_client == nil then
  error(("typed Pouch open failed: %s"):format(typed_err and typed_err.message or tostring(typed_err)))
end
typed_client:close()
local typed_manifest = assert(io.open(typed_root .. "/manifest", "rb"))
local typed_manifest_text = assert(typed_manifest:read("*a"))
typed_manifest:close()
if not typed_manifest_text:match("compression=none") then
  error("nested typed Pouch compression did not override compatibility configuration")
end

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
local malformed_term_ok, malformed_term_err = pcall(function()
  client:txn_commit({
    txn_id = assert(lockdc.xid_new()),
    participants = {
      { namespace_name = namespace_name, key = "invalid-term" },
    },
    tc_term = -1,
  })
end)
if malformed_term_ok or not tostring(malformed_term_err):match("tc_term") then
  client:close()
  error("Lua XA validation did not reject a negative coordinator term")
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
  expires_at_unix = 2147483648,
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
local selector_free_keys = {}
local selector_free_cursor
repeat
  local page_keys = {}
  local current_page_key
  local selector_free_result = assert_ok("Lua selector-free query_keys",
                                         client:query_keys({
    namespace_name = namespace_name,
    engine = "scan",
    limit = 1,
    cursor = selector_free_cursor,
  }, {
    begin = function()
      current_page_key = ""
    end,
    chunk = function(bytes)
      current_page_key = current_page_key .. bytes
    end,
    finish = function()
      table.insert(page_keys, current_page_key)
    end,
  }))
  if #page_keys ~= 1 then
    client:close()
    error("Lua selector-free query_keys did not stream one paginated key")
  end
  table.insert(selector_free_keys, page_keys[1])
  selector_free_cursor = selector_free_result.cursor
until selector_free_cursor == nil
local found_raw_txn_key = false
for _, key in ipairs(selector_free_keys) do
  if key == raw_txn_participant.key then
    found_raw_txn_key = true
    break
  end
end
if #selector_free_keys < 2 or not found_raw_txn_key then
  client:close()
  error("Lua selector-free query_keys did not enumerate all Pouch keys")
end

local conflicting_history, conflicting_history_err = client:new_history_consumer({
  namespace_name = namespace_name,
  consumer_id = "lua-history-invalid",
  initial_acknowledged_index_seq = 0,
  start_at_current = true,
})
if conflicting_history ~= nil or type(conflicting_history_err) ~= "table" or
    not tostring(conflicting_history_err.message):match("cannot both be supplied") then
  client:close()
  error("Lua history consumer accepted conflicting initial positions")
end

local history_consumer_id = assert(lockdc.xid_new())
local history, history_err = client:new_history_consumer({
  namespace = namespace_name,
  consumer_id = history_consumer_id,
  initial_acknowledged_index_seq = 0,
})
history = assert_ok("Lua history consumer create", history, history_err)
local history_position = assert_ok("Lua history consumer position",
                                   history:position())
if history_position.acknowledged_index_seq ~= 0 or
    type(history_position.current_index_seq) ~= "number" or
    history_position.current_index_seq < 1 then
  history:close()
  client:close()
  error("Lua history consumer did not report its durable initial position")
end
local advanced_history_position = assert_ok("Lua history consumer advance",
                                            history:advance(
  history_position.current_index_seq))
if advanced_history_position.acknowledged_index_seq ~=
    advanced_history_position.current_index_seq then
  history:close()
  client:close()
  error("Lua history consumer did not persist its acknowledgement")
end
local backward_history, backward_history_err = history:advance(
  advanced_history_position.current_index_seq - 1)
if backward_history ~= nil or type(backward_history_err) ~= "table" then
  history:close()
  client:close()
  error("Lua history consumer accepted a backward acknowledgement")
end
history:close()

local reopened_history = assert_ok("Lua history consumer reopen",
                                   client:new_history_consumer({
  namespace_name = namespace_name,
  consumer_id = history_consumer_id,
  initial_acknowledged_index_seq = 0,
}))
local reopened_history_position = assert_ok("Lua history consumer persisted position",
                                            reopened_history:position())
if reopened_history_position.acknowledged_index_seq ~=
    advanced_history_position.acknowledged_index_seq then
  reopened_history:close()
  client:close()
  error("Lua history consumer did not retain its durable acknowledgement")
end
assert_ok("Lua history consumer unregister", reopened_history:unregister())
local unregistered_history, unregistered_history_err = reopened_history:position()
if unregistered_history ~= nil or type(unregistered_history_err) ~= "table" then
  reopened_history:close()
  client:close()
  error("Lua history consumer remained usable after unregister")
end
reopened_history:close()

local current_history = assert_ok("Lua history consumer start current",
                                  client:new_history_consumer({
  namespace_name = namespace_name,
  consumer_id = assert(lockdc.xid_new()),
  start_at_current = true,
}))
local current_history_position = assert_ok("Lua history consumer current position",
                                           current_history:position())
if current_history_position.acknowledged_index_seq ~=
    current_history_position.current_index_seq then
  current_history:close()
  client:close()
  error("Lua history consumer start_at_current did not select current sequence")
end
assert_ok("Lua history consumer current unregister", current_history:unregister())
current_history:close()
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
local retained_callbacks = setmetatable({}, { __mode = "v" })
do
  local callback_payload = {}
  local function begin()
    return callback_payload
  end
  retained_callbacks[1] = callback_payload
  local invalid_callback_ok, invalid_callback_err = pcall(function()
    client:query_keys({
      namespace_name = namespace_name,
      selector_json = '{"eq":{"field":"/source","value":"lua-pouch-xa"}}',
      engine = "scan",
    }, {
      begin = begin,
      chunk = false,
    })
  end)
  if invalid_callback_ok or not tostring(invalid_callback_err):match("function") then
    client:close()
    error("Lua query_keys did not reject an invalid callback")
  end
end
do
  local callback_payload = {}
  local function begin()
    return callback_payload
  end
  local function chunk()
    return callback_payload
  end
  retained_callbacks[2] = callback_payload
  local invalid_callback_ok, invalid_callback_err = pcall(function()
    client:query_keys({
      namespace_name = namespace_name,
      selector_json = '{"eq":{"field":"/source","value":"lua-pouch-xa"}}',
      engine = "scan",
    }, {
      begin = begin,
      chunk = chunk,
      finish = false,
    })
  end)
  if invalid_callback_ok or not tostring(invalid_callback_err):match("function") then
    client:close()
    error("Lua query_keys did not reject an invalid finish callback")
  end
end
collectgarbage("collect")
collectgarbage("collect")
if retained_callbacks[1] ~= nil or retained_callbacks[2] ~= nil then
  client:close()
  error("Lua query_keys retained a callback after argument validation failed")
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

-- Request tables may have metamethods.  Closing the client from one must
-- yield a normal API error, rather than dereferencing the released receiver.
local close_during_request_xid = assert(lockdc.xid_new())
local close_during_request = setmetatable({}, {
  __index = function(_, key)
    if key == "txn_id" then
      client:close()
      return close_during_request_xid
    end
    return nil
  end,
})
local close_during_request_result, close_during_request_err =
  client:txn_replay(close_during_request)
if close_during_request_result ~= nil or type(close_during_request_err) ~= "table" or
    not tostring(close_during_request_err.message):find("closed while preparing request", 1, true) then
  error("Lua transaction replay did not reject a client closed by request parsing")
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

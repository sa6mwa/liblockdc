package.preload.lonejson = function()
  return { json_null = {}, encode_json = function() error("unexpected encoding") end }
end
package.path = lockdc_test_source_root .. "/lua/?.lua;" ..
  lockdc_test_source_root .. "/lua/?/init.lua;" .. package.path
local lockdc = require("lockdc")
local postgres = require("lockdc.postgres")
assert(postgres.OUTCOME_COMMITTED == lockdc.XA_OUTCOME_COMMITTED)
assert(postgres.OUTCOME_ABORTED == lockdc.XA_OUTCOME_ABORTED)
assert(postgres.register("postgres-alias"))
local invalid, invalid_error, invalid_code = postgres.register("bad\0name")
assert(invalid == nil and invalid_code == lockdc.ERR_INVALID and invalid_error.message)
local client = assert(lockdc.open({ endpoints = { "pouch://" .. lockdc_test_root },
  pouch = { single_writer = true } }))
local function await(operation)
  for _ = 1, 500 do
    local done, err, code = operation:advance()
    if done == nil then return nil, err, code end
    if done then return operation:result() end
    lockdc_test_wait(assert(operation:wait()))
  end
  error("test operation exceeded its bounded advancement count")
end
local function complete(operation)
  local result, error_value = await(operation)
  assert(result, error_value and error_value.message)
  assert(operation:close_blocking())
  return result
end
local coordinator = assert(client:xa_coordinator({ await = await }))
local resources = { assert(lockdc.xid_new()), assert(lockdc.xid_new()) }
for index, connection in ipairs({ lockdc_test_first, lockdc_test_second }) do
  -- Synthesized strings survive a later lookup/GC because the helper copies
  -- both fields before the provider starts native connection setup.
  local configuration = setmetatable({}, { __index = function(_, name)
    if name == "resource_id" then return resources[index] .. "" end
    if name == "connection_info" then collectgarbage("collect"); return connection .. "" end
  end })
  local operation = assert(coordinator:register_start("postgres", configuration))
  local result, err = await(operation)
  assert(result, err and err.message)
  assert(operation:close_blocking())
end
-- Coordinator-owned manual receivers expose the same diagnostics and retain
-- the native coordinator independently of its original Lua wrapper.
local retained_resource = assert(coordinator:resource(resources[1]))
assert(retained_resource:identity().resource_id == resources[1])
assert(coordinator:resource(assert(lockdc.xid_new())) == nil)
assert(complete(assert(retained_resource:inventory_start(nil, 0))).row_count == 0)
assert(retained_resource:close_blocking())
local outbox = assert(client:new_outbox({ namespace = "lua-effects" }))
local escaped
local function adapter(borrow)
  escaped = borrow
  return { query_blocking = function(_, sql)
      return lockdc_test_query_blocking(borrow, sql)
    end,
    close = function() return true end }
end
for mode = 0, 2 do
  local name, key
  local receipt, err, code, state = coordinator:transaction(function(tx)
    name = "lua-" .. tx:id()
    key = name
    for _, resource in ipairs(resources) do assert(tx:enroll(resource)) end
    local box = assert(tx:outbox(outbox))
    local participant = assert(box:acquire({ key = key, namespace = "orders" }))
    assert(participant:update('{"value":42}'))
    assert(box:append({ operation_id = tx:id(), effect_id = "effect", effect_key = name,
      payload_digest = "sha256:test", kind = "test", destination = "test://sink" }, "payload"))
    for index, resource in ipairs(resources) do
      tx:with_connection(resource, adapter, function(session)
        local first_xid = session:query_blocking("SELECT pg_current_xact_id()::text")
        assert(type(first_xid) == "string")
        collectgarbage("collect")
        assert(session:query_blocking("SELECT pg_current_xact_id()::text") == first_xid)
        assert(session:query_blocking("INSERT INTO xa_work(name) VALUES ('" .. name .. "')"))
        if mode == 2 and index == 2 then session:query_blocking("SELECT 1/0") end
      end)
    end
    if mode == 1 then error("intentional callback abort") end
    return name
  end)
  if mode == 0 then
    assert(receipt, err and err.message)
    assert(receipt.applied and receipt.decision == 1 and #receipt.outboxes == 1)
    assert(#receipt.outboxes[1].outbox_receipts == 1)
  else
    assert(receipt == nil and code == lockdc.ERR_INVALID and state.applied and state.decision == 2)
  end
  assert(escaped:view() == nil) -- Scoped cleanup never leaves a native pointer usable.
  assert(escaped:close())
  assert(escaped:close()) -- Provider cleanup errors still consumed the token.
  collectgarbage("collect")
  for _, connection in ipairs({ lockdc_test_first, lockdc_test_second }) do
    assert(lockdc_test_count_blocking(connection, name) == (mode == 0 and 1 or 0))
  end
  local lease = assert(client:acquire({ key = key, namespace = "orders", owner = "verify", ttl_seconds = 30 }))
  local payload, metadata = lease:read()
  if mode == 0 then assert(payload == '{"value":42}', "committed Pouch readback: " .. tostring(payload))
  else assert(metadata.no_content and (payload == nil or payload == ""),
    "aborted Pouch readback: " .. tostring(payload)) end
  lease:close()
end
outbox:close()
local diagnostic = assert(coordinator:resource(resources[1]))
assert(coordinator:close_blocking())
assert(complete(assert(diagnostic:inventory_start())).row_count == 0)
assert(diagnostic:close_blocking())

-- Exercise the complete public participant lifecycle through Lua on both
-- servers: prepare, inventory/status, immutable decision, resolve and recovery.
local store = assert(client:xa_store())
local manual, connections = {}, { lockdc_test_first, lockdc_test_second }
for index, connection in ipairs(connections) do
  local resource_id = assert(lockdc.xid_new())
  manual[index] = assert(store:resource("postgres-alias", {
    resource_id = resource_id, connection_info = connection }))
  local registered = complete(assert(manual[index]:register_start()))
  assert(registered.done and manual[index]:identity().resource_id == resource_id)
  local expired, e, c = manual[index]:branch_start(assert(lockdc.xid_new()), 1)
  assert(expired == nil and c == lockdc.ERR_INVALID and e.message)
  for _, bad_limit in ipairs({ -1, 65, 1.5 }) do
    local op, e, c = manual[index]:inventory_start(nil, bad_limit)
    assert(op == nil and c == lockdc.ERR_INVALID and e.message)
  end
end
-- Native resources retain the journal after its Lua receiver closes.
assert(store:close())
for _, commit in ipairs({ false, true }) do
  for _, recover in ipairs({ false, true }) do
    local id = assert(lockdc.xid_new())
    local name, branches, records = "lua-manual-" .. id, {}, {}
    -- The unvoted Pouch member keeps implicit XA undecided while SQL prepares.
    local lease = assert(client:acquire({ namespace = "lua-manual", key = name,
      owner = "lua", ttl_seconds = 30, txn_id = id }))
    assert(lease:update('{"value":42}'))
    for index, resource in ipairs(manual) do
      local op = assert(resource:branch_start(id))
      assert(op:result().done == false and op:take_branch() == nil)
      local started, start_error, start_code = await(op)
      assert(started, "branch " .. index .. " failed: " .. tostring(start_code) ..
        " sqlstate=" .. tostring(op:result().sqlstate) .. " " .. tostring(start_error and start_error.message))
      branches[index] = assert(op:take_branch())
      assert(op:take_branch() == nil)
      assert(op:close_blocking())
      local branch = branches[index]
      local borrow = assert(branch:borrow())
      local xid = lockdc_test_query_blocking(borrow, "SELECT pg_current_xact_id()::text")
      assert(type(xid) == "string" and borrow:view().txn_id == id)
      assert(type(borrow:view().owner_process) == "string" and type(borrow:view().generation) == "string")
      assert(lockdc_test_query_blocking(borrow, "INSERT INTO xa_work(name) VALUES ('" .. name .. "')"))
      local held, held_error, held_code = branch:prepare_start()
      assert(held == nil and held_code == lockdc.ERR_INVALID and held_error.message)
      assert(branch:record().phase == lockdc.XA_ENROLLED)
      assert(lockdc_test_query_blocking(borrow, "SELECT pg_current_xact_id()::text") == xid)
      assert(borrow:close())
      assert(borrow:view() == nil)
      records[index] = branch:record()
      assert(records[index].full_xid == xid)
    end
    if commit then
      local voted, vote_error, vote_code = lease:release()
      assert(voted or vote_code == lockdc.ERR_PENDING, vote_error and vote_error.message)
    end
    -- All participants must enroll before the first prepare seals the set.
    for index, resource in ipairs(manual) do
      local branch = branches[index]
      local prepared = complete(assert(branch:prepare_start()))
      records[index] = branch:record()
      assert(prepared.record.phase == lockdc.XA_PREPARED)
      assert(type(records[index].system_id) == "string")
      local inventory = complete(assert(resource:inventory_start(nil, 1)))
      assert(inventory.row_count == 1 and #inventory.rows == 1)
      assert(inventory.rows[1].gid == records[index].gid and type(inventory.rows[1].xid) == "string")
      assert(complete(assert(resource:inventory_start(inventory.rows[1].gid, 64))).row_count == 0)
      assert(complete(assert(resource:status_start(id, records[index].branch_id))).native_outcome == lockdc.XA_OUTCOME_IN_PROGRESS)
    end
    local decided, e, c, state
    if commit then decided, e, c, state = client:txn_commit({ txn_id = id })
    else decided, e, c, state = client:txn_rollback({ txn_id = id }) end
    assert(decided == nil and c == lockdc.ERR_PENDING and state and state.txn_id == id,
      "manual decision: value=" .. tostring(decided) .. " code=" .. tostring(c) ..
      " state=" .. tostring(state and state.state) .. " error=" .. tostring(e and e.message))
    assert(state.state == (commit and "commit" or "rollback"))
    local pending, pe, pc, ps = client:txn_replay({ txn_id = id })
    assert(pending == nil and pc == lockdc.ERR_PENDING and ps.txn_id == id)
    for index, resource in ipairs(manual) do
      local branch = branches[index]
      if recover then
        -- Cancel before transmitting phase two, with an immutable decision
        -- already durable. Closing must hand the prepared branch to recovery.
        local cancelled = assert(branch:resolve_start(commit))
        local done, cancel_error, cancel_code = cancelled:cancel()
        assert(done == nil and cancel_code == lockdc.ERR_PENDING and cancel_error.message)
        assert(not cancelled:result().done and cancelled:take_branch() == nil)
        assert(cancelled:close_blocking())
        local value, replay_error, replay_code, replay_state = client:txn_replay({ txn_id = id })
        assert(value == nil and replay_code == lockdc.ERR_PENDING and replay_error.message)
        assert(replay_state.state == (commit and "commit" or "rollback"))
        assert(branch:close())
        local op = assert(resource:recover_start(id, records[index].branch_id, commit))
        assert(await(op))
        branch = assert(op:take_branch())
        assert(op:close_blocking())
      else
        complete(assert(branch:resolve_start(commit)))
      end
      assert(branch:record().phase == (commit and lockdc.XA_COMMITTED or lockdc.XA_ABORTED))
      assert(branch:close())
      assert(branch:close())
      assert(branch:record() == nil)
      if recover then
        local repeated = assert(resource:recover_start(id, records[index].branch_id, commit))
        assert(await(repeated))
        local completed_branch = assert(repeated:take_branch())
        assert(completed_branch:record().phase == (commit and lockdc.XA_COMMITTED or lockdc.XA_ABORTED))
        assert(repeated:close_blocking())
        assert(completed_branch:close())
        local opposite = assert(resource:recover_start(id, records[index].branch_id, not commit))
        local value, opposite_error, opposite_code = await(opposite)
        assert(value == nil and opposite_code == lockdc.ERR_PROTOCOL and opposite_error.message)
        assert(opposite:result().done and opposite:take_branch() == nil)
        assert(opposite:close_blocking())
      end
      local outcome = complete(assert(resource:status_start(id, records[index].branch_id)))
      assert(outcome.native_outcome == (commit and lockdc.XA_OUTCOME_COMMITTED or lockdc.XA_OUTCOME_ABORTED))
      assert(lockdc_test_count_blocking(connections[index], name) == (commit and 1 or 0))
      assert(complete(assert(resource:inventory_start())).row_count == 0)
    end
    assert(client:txn_replay({ txn_id = id }).state == (commit and "commit" or "rollback"))
    lease:close()
    local verify = assert(client:acquire({ namespace = "lua-manual", key = name,
      owner = "verify", ttl_seconds = 30 }))
    local payload, metadata = verify:read()
    if commit then assert(payload == '{"value":42}')
    else assert(metadata.no_content) end
    verify:close()
  end
end
-- Taking a branch must preserve the creating operation's native lifetime pin.
local early_id = assert(lockdc.xid_new())
local creating = assert(manual[1]:branch_start(early_id))
assert(await(creating))
local early_branch = assert(creating:take_branch())
local early_record = early_branch:record()
assert(early_branch:close())
assert(creating:close_blocking())
local observed = complete(assert(manual[1]:status_start(early_id, early_record.branch_id)))
assert(observed.native_outcome == lockdc.XA_OUTCOME_ABORTED,
  "early branch close left its native transaction open: " .. observed.native_outcome)
local _, early_error, early_code, early_state = client:txn_rollback({ txn_id = early_id })
assert(early_code == lockdc.ERR_PENDING and early_error.message and early_state.state == "rollback")
local recovered = assert(manual[1]:recover_start(early_id, early_record.branch_id, false))
assert(await(recovered))
local recovered_branch = assert(recovered:take_branch())
assert(recovered:close_blocking())
assert(recovered_branch:close())
assert(client:txn_replay({ txn_id = early_id }).state == "rollback")
-- Cancellation produces a terminal error, never a successful branch transfer.
local cancelled = assert(manual[1]:branch_start(assert(lockdc.xid_new())))
assert(cancelled:cancel())
assert(cancelled:result().done and cancelled:take_branch() == nil)
assert(cancelled:close_blocking())
for _, resource in ipairs(manual) do
  assert(resource:close_blocking())
  assert(resource:close_blocking())
  assert(resource:identity() == nil)
end
client:close()
collectgarbage("collect")
print("Lua XA committed/aborted both PostgreSQL instances, Pouch and outbox")

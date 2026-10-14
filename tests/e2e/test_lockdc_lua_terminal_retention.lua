local lockdc = require("lockdc")
local function assert_ok(value, err)
  if not value and type(err) == "table" then error(err.message, 2) end
  return assert(value, err)
end
local root = assert(os.getenv("LOCKDC_POUCH_ROOT"))
local client = assert(lockdc.open({
  endpoints = { "pouch://" .. root },
  pouch = { single_writer = false, query_indexing = false },
}))

-- Native time validation and Lua count parsing must agree for every policy.
for _, kind in ipairs({ "completed", "inbox", "command" }) do
  for _, value in ipairs({ -2, -100 }) do
    local invalid, err = client:new_outbox({
      namespace = "invalid-" .. kind,
      [kind .. "_retention_seconds"] = value,
    })
    assert(invalid == nil and err.code == lockdc.ERR_INVALID)
  end
  for _, value in ipairs({ true, -1, 1.5 }) do
    assert(not pcall(function()
      return client:new_outbox({
        namespace = "invalid-count-" .. kind,
        [kind .. "_max_count"] = value,
      })
    end))
  end
end

-- Constructor capability checks need no running remote server.
local remote, remote_err = lockdc.open({
  endpoints = { "https://127.0.0.1:1" }, disable_mtls = true,
})
assert(remote, remote_err and remote_err.message)
for _, kind in ipairs({ "completed", "inbox", "command" }) do
  for _, field in ipairs({ "_retention_seconds", "_max_count" }) do
    local invalid, err = remote:new_outbox({
      namespace = "remote-retention",
      [kind .. field] = 1,
    })
    assert(invalid == nil and err.code == lockdc.ERR_INVALID)
  end
end
remote:close()

local outbox = assert(client:new_outbox({
  namespace = "lua-terminal-retention",
  completed_retention_seconds = 1,
  inbox_retention_seconds = 2,
  command_retention_seconds = 3,
  completed_max_count = false,
  inbox_max_count = 0,
  command_max_count = false,
  dead_letter_retention_seconds = -1,
}))
local message = {
  consumer_id = "consumer", source_kind = "queue", source_id = "source",
  message_id = "message",
}
local inbox_txn, accepted = outbox:accept_inbox(message)
assert(inbox_txn and accepted.accepted)
assert(inbox_txn:commit())
inbox_txn:close()
local duplicate, duplicate_result = outbox:accept_inbox(message)
assert(duplicate == nil and duplicate_result.duplicate)

local identity = { scope = "tenant", command_type = "test", idempotency_key = "key" }
local command_txn = assert(outbox:accept_command({
  scope = identity.scope, command_type = identity.command_type,
  idempotency_key = identity.idempotency_key, request_digest = "digest",
}))
assert(command_txn:complete_command({
  result_code = "ok", content_type = "text/plain", body = "owned result",
}))
assert(command_txn:commit())
command_txn:close()
assert(outbox:get_command_receipt(identity).state == lockdc.COMMAND_COMPLETED)

local entry = {
  operation_id = "operation", effect_id = "effect", effect_key = "key",
  payload_digest = "digest", kind = "test", destination = "test://target",
}
local effect_txn = assert(outbox:append(entry, "payload"))
assert(effect_txn:commit())
effect_txn:close()
local dispatcher = assert(outbox:dispatcher())
local job = assert(dispatcher:next(3000))
assert(job:complete())
job:close()

-- Deadline-bounded dispatcher pulls yield while private maintenance runs.
local deadline = os.time() + 8
local stats
repeat
  stats = assert(dispatcher:stats())
  assert(type(stats.terminal_reclaims) == "number")
  assert(stats.terminal_reclaim_failures == 0)
  if stats.terminal_reclaims >= 3 then break end
  local no_job, err = dispatcher:next(50)
  assert(no_job == nil and (err == nil or err.code == lockdc.ERR_TIMEOUT))
until os.time() >= deadline
assert(stats.terminal_reclaims == 3, "automatic terminal retention did not finish")
local missing, missing_err = outbox:get_command_receipt(identity)
assert(missing == nil and missing_err ~= nil)
local fresh_inbox, fresh_result = outbox:accept_inbox(message)
assert(fresh_inbox and fresh_result.accepted and not fresh_result.duplicate)
assert(fresh_inbox:rollback())
fresh_inbox:close()
local fresh_effect = assert(outbox:append(entry, "payload"))
assert(fresh_effect:rollback())
fresh_effect:close()
assert(dispatcher:stop(-1))
dispatcher:close()
outbox:close()

-- Each Lua count field must reach only its corresponding native policy. Keep
-- every deadline in the future so these removals prove capacity reclamation.
for _, kind in ipairs({ "completed", "inbox", "command" }) do
  local config = {
    namespace = "lua-terminal-cap-" .. kind,
    completed_retention_seconds = -1,
    inbox_retention_seconds = -1,
    command_retention_seconds = -1,
    dead_letter_retention_seconds = -1,
    [kind .. "_retention_seconds"] = 86400,
    [kind .. "_max_count"] = 1,
  }
  local capped = assert_ok(client:new_outbox(config))
  local cap_dispatcher
  if kind == "completed" then cap_dispatcher = assert_ok(capped:dispatcher()) end
  local identities = {}
  for index = 1, 2 do
    local txn
    if kind == "completed" then
      identities[index] = {
        operation_id = "cap-operation", effect_id = tostring(index),
        effect_key = "cap-key-" .. index, payload_digest = "digest",
        kind = "test", destination = "test://target",
      }
      txn = assert_ok(capped:append(identities[index], "payload"))
    elseif kind == "inbox" then
      identities[index] = {
        consumer_id = "cap-consumer", source_kind = "queue", source_id = "source",
        message_id = tostring(index),
      }
      txn = assert_ok(capped:accept_inbox(identities[index]))
    else
      identities[index] = {
        scope = "tenant", command_type = "test", idempotency_key = tostring(index),
      }
      txn = assert_ok(capped:accept_command({
        scope = "tenant", command_type = "test", idempotency_key = tostring(index),
        request_digest = "digest",
      }))
      if index == 1 then
        assert_ok(txn:fail_command({ failure_code = "declined" }))
      else
        assert_ok(txn:complete_command({
          result_code = "ok", content_type = "text/plain", body = "owned result",
        }))
      end
    end
    assert_ok(txn:commit())
    txn:close()
    if kind == "completed" then
      local cap_job = assert_ok(cap_dispatcher:next(3000))
      assert_ok(cap_job:complete())
      cap_job:close()
    end
  end
  -- Startup discovers producer-owned receipts immediately. An already idle
  -- worker may take up to the documented minute to discover new shared work.
  if cap_dispatcher == nil then cap_dispatcher = assert_ok(capped:dispatcher()) end
  local cap_deadline = os.time() + 5
  local cap_stats
  repeat
    cap_stats = assert(cap_dispatcher:stats())
    assert(cap_stats.terminal_reclaim_failures == 0)
    if cap_stats.terminal_reclaims == 1 then break end
    local no_job, err = cap_dispatcher:next(50)
    assert(no_job == nil and (err == nil or err.code == lockdc.ERR_TIMEOUT))
  until os.time() >= cap_deadline
  assert(cap_stats.terminal_reclaims == 1, "Lua count cap did not reclaim " .. kind)
  -- Freeze maintenance before probing duplicate identities: a fresh inbox
  -- acceptance publishes provisional work and can itself exceed the cap.
  assert(cap_dispatcher:stop(-1))
  cap_dispatcher:close()
  local missing_count = 0
  for _, identity in ipairs(identities) do
    if kind == "command" then
      local receipt, err = capped:get_command_receipt(identity)
      if receipt == nil then
        assert(err ~= nil)
        missing_count = missing_count + 1
      else
        assert(receipt.state ~= lockdc.COMMAND_PENDING)
      end
    else
      local fresh, receipt
      if kind == "completed" then
        fresh, receipt = capped:append(identity, "payload")
      else
        fresh, receipt = capped:accept_inbox(identity)
      end
      if fresh then
        missing_count = missing_count + 1
        assert(fresh:rollback())
        fresh:close()
      else
        assert(receipt.duplicate)
      end
    end
  end
  assert(missing_count == 1, "count cap must preserve one " .. kind .. " receipt")
  capped:close()
end
client:close()

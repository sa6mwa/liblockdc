local lockdc = require("lockdc")

local root = assert(os.getenv("LOCKDC_POUCH_ROOT"), "LOCKDC_POUCH_ROOT is required")
local client, open_err = lockdc.open({
  endpoints = { "pouch://" .. root },
  default_namespace = "lua-workflow-domain",
})

if client == nil then
  error(("Pouch workflow client open failed: %s"):format(
    open_err and open_err.message or tostring(open_err)))
end

local workflow, workflow_err = client:new_workflow({
  namespace = "lua-workflow-records",
  owner = "lua-workflow-worker",
  transaction_ttl_seconds = 30,
  claim_ttl_seconds = 30,
  recovery_interval_seconds = 0,
})
if workflow == nil then
  client:close()
  error(("Lua workflow creation failed: %s"):format(
    workflow_err and workflow_err.message or tostring(workflow_err)))
end

local function assert_ok(value, err, operation)
  if value == nil then
    error(("%s failed: %s"):format(operation, err and err.message or tostring(err)))
  end
  return value
end

local function append_effect(effect_id, payload)
  local txn, receipt_or_err = workflow:append_outbox({
    operation_id = "lua-order-1",
    effect_id = effect_id,
    effect_key = "lua-effect:" .. effect_id,
    kind = "http",
    destination = "https://example.test/effects/" .. effect_id,
    content_type = "application/json",
    headers = { ["x-workflow"] = "lua" },
  }, lockdc.encode_json(payload))
  if txn == nil then
    workflow:close()
    client:close()
    error(("Lua outbox append failed: %s"):format(
      receipt_or_err and receipt_or_err.message or tostring(receipt_or_err)))
  end
  if receipt_or_err.duplicate then
    workflow:close()
    client:close()
    error("fresh Lua outbox append unexpectedly reported duplicate")
  end
  return txn, receipt_or_err
end

local txn = append_effect("first", { sequence = 1 })
local participant, participant_err = txn:acquire({
  namespace_name = "lua-workflow-domain",
  key = "order-1",
  owner = "lua-workflow-worker",
  ttl_seconds = 30,
})
participant = assert_ok(participant, participant_err, "Lua workflow participant acquire")
assert_ok(participant:update_json({ status = "paid", revision = 1 }), nil,
          "Lua workflow participant update")
assert_ok(participant:mutate({ mutations = { "/revision++" } }), nil,
          "Lua workflow participant mutation")
local attachment = assert_ok(participant:attach({
  name = "workflow-proof",
  content_type = "text/plain",
}, "proof"), nil, "Lua workflow participant attachment")
if attachment.attachment.name ~= "workflow-proof" then
  participant:close()
  txn:close()
  workflow:close()
  client:close()
  error("Lua workflow participant attachment did not return its name")
end
local attachments = assert_ok(participant:list_attachments(), nil,
                              "Lua workflow participant attachment list")
if #attachments.items ~= 1 or attachments.items[1].name ~= "workflow-proof" then
  participant:close()
  txn:close()
  workflow:close()
  client:close()
  error("Lua workflow participant attachment list lost staged attachment")
end
assert_ok(participant:delete_attachment({ name = "workflow-proof" }), nil,
          "Lua workflow participant attachment delete")
local described, describe_err = participant:describe()
assert_ok(described, describe_err, "Lua workflow participant describe")
if described.version < 2 then
  participant:close()
  txn:close()
  workflow:close()
  client:close()
  error("Lua workflow participant describe lost staged version")
end
participant:close()
assert_ok(txn:commit(), nil, "Lua workflow transaction commit")
txn:close()

local state, state_err = client:get_json({
  namespace_name = "lua-workflow-domain",
  key = "order-1",
})
if state == nil or state.status ~= "paid" or state.revision ~= 2 then
  workflow:close()
  client:close()
  error(("Lua workflow committed state was not visible: %s"):format(
    state_err and state_err.message or tostring(state_err)))
end

local job, next_err = workflow:next(3000)
job = assert_ok(job, next_err, "Lua workflow first job")
if job:info().effect_key ~= "lua-effect:first" then
  job:close()
  workflow:close()
  client:close()
  error("Lua workflow returned the wrong first outbox job")
end
local payload, written_or_err = job:payload_json()
if payload == nil or payload.sequence ~= 1 or type(written_or_err) ~= "number" then
  job:close()
  workflow:close()
  client:close()
  error("Lua workflow outbox payload did not stream through the façade")
end
assert_ok(job:complete(), nil, "Lua workflow first job completion")
job:close()

local inbound_txn, accepted_or_err = workflow:accept_inbox({
  consumer_id = "lua-workflow-consumer",
  source_kind = "http",
  source_id = "orders",
  message_id = "message-1",
  payload_digest = "digest-1",
})
if inbound_txn == nil or not accepted_or_err.accepted then
  workflow:close()
  client:close()
  error(("Lua inbox first acceptance failed: %s"):format(
    accepted_or_err and accepted_or_err.message or tostring(accepted_or_err)))
end
assert_ok(inbound_txn:append_outbox({
  operation_id = "lua-order-1",
  effect_id = "inbound-effect",
  effect_key = "lua-effect:inbound",
  kind = "http",
  destination = "https://example.test/effects/inbound",
}, "inbound-payload"), nil, "Lua inbox transaction outbox append")
assert_ok(inbound_txn:commit(), nil, "Lua inbox transaction commit")
inbound_txn:close()

local duplicate_txn, duplicate_or_err = workflow:accept_inbox({
  consumer_id = "lua-workflow-consumer",
  source_kind = "http",
  source_id = "orders",
  message_id = "message-1",
  payload_digest = "digest-1",
})
if duplicate_txn ~= nil or duplicate_or_err == nil or not duplicate_or_err.duplicate then
  workflow:close()
  client:close()
  error("Lua inbox redelivery was not reported as a duplicate")
end

job = assert_ok(workflow:next(3000), nil, "Lua workflow inbound job")
if job:info().effect_key ~= "lua-effect:inbound" then
  job:close()
  workflow:close()
  client:close()
  error("Lua workflow returned the wrong inbox-triggered job")
end
assert_ok(job:complete(), nil, "Lua workflow inbound job completion")
job:close()

txn = append_effect("retry", { sequence = 2 })
assert_ok(txn:commit(), nil, "Lua workflow retry transaction commit")
txn:close()
job = assert_ok(workflow:next(3000), nil, "Lua workflow retry first job")
assert_ok(job:retry({ delay_seconds = 1, diagnostic = "temporary" }), nil,
          "Lua workflow retry scheduling")
job:close()
job = assert_ok(workflow:next(3000), nil, "Lua workflow retry redelivery")
if job:info().attempt ~= 2 then
  job:close()
  workflow:close()
  client:close()
  error("Lua workflow retry did not increment the attempt")
end
local dead_letter_key = job:info().outbox_key
assert_ok(job:dead_letter("permanent"), nil, "Lua workflow dead letter")
job:close()

local stats = assert_ok(workflow:stats(), nil, "Lua workflow stats")
if not stats.running then
  workflow:close()
  client:close()
  error("Lua workflow stats did not report a running dispatcher")
end
local exported, export_result = workflow:export_dead_letters({ format = "jsonl" })
if exported == nil or export_result == nil or export_result.exported ~= 1 or
    not exported:find("dead_letter", 1, true) or
    exported:find("retry-payload", 1, true) then
  workflow:close()
  client:close()
  error("Lua workflow dead-letter export did not return a metadata-only envelope")
end
assert_ok(workflow:replay_dead_letter(dead_letter_key), nil,
          "Lua workflow dead-letter replay")
job = assert_ok(workflow:next(3000), nil, "Lua workflow replayed job")
if job:info().effect_key ~= "lua-effect:retry" or job:info().attempt ~= 1 then
  job:close()
  workflow:close()
  client:close()
  error("Lua workflow dead-letter replay did not reset the delivery attempt")
end
assert_ok(job:complete(), nil, "Lua workflow replayed completion")
job:close()
assert_ok(workflow:reconcile(), nil, "Lua workflow reconciliation signal")

workflow:close()
client:close()

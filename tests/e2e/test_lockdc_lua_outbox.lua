local lockdc = require("lockdc")

local root = assert(os.getenv("LOCKDC_POUCH_ROOT"), "LOCKDC_POUCH_ROOT is required")
local client, open_err = lockdc.open({
  endpoints = { "pouch://" .. root },
  default_namespace = "lua-outbox-domain",
})

if client == nil then
  error(("Pouch outbox client open failed: %s"):format(
    open_err and open_err.message or tostring(open_err)))
end

local oversized_attempts_ok = pcall(function()
  return client:new_outbox({
    namespace = "lua-outbox-invalid-max-attempts",
    max_attempts = 4294967296,
  })
end)
if oversized_attempts_ok then
  client:close()
  error("Lua outbox accepted max_attempts outside the C int range")
end

local negative_notification_capacity_ok = pcall(function()
  return client:new_outbox({
    namespace = "lua-outbox-invalid-notification-capacity",
    notification_capacity = -1,
  })
end)
if negative_notification_capacity_ok then
  client:close()
  error("Lua outbox accepted a negative notification_capacity")
end

local oversized_queue_attempts_ok = pcall(function()
  return client:enqueue({
    namespace = "lua-outbox-invalid-max-attempts",
    queue = "invalid-attempts",
    max_attempts = 4294967296,
  }, "ignored")
end)
if oversized_queue_attempts_ok then
  client:close()
  error("Lua enqueue accepted max_attempts outside the C int range")
end

local legacy_namespace_ok = pcall(function()
  return client:new_outbox({ namespace_name = "lua-outbox-legacy-namespace" })
end)
if legacy_namespace_ok then
  client:close()
  error("Lua outbox accepted the retired namespace_name field")
end

local outbox, outbox_err = client:new_outbox({
  namespace = "lua-outbox-records",
  owner = "lua-outbox-worker",
  transaction_ttl_seconds = 30,
  claim_ttl_seconds = 30,
  recovery_interval_seconds = 0,
})
if outbox == nil then
  client:close()
  error(("Lua outbox creation failed: %s"):format(
    outbox_err and outbox_err.message or tostring(outbox_err)))
end

-- Producer construction is threadless.  Consumption is an explicit lifecycle
-- decision and remains separate from the request-domain outbox handle.
local dispatcher, dispatcher_err = outbox:dispatcher()
if dispatcher == nil then
  outbox:close()
  client:close()
  error(("Lua outbox dispatcher creation failed: %s"):format(
    dispatcher_err and dispatcher_err.message or tostring(dispatcher_err)))
end
local invalid_handlers, invalid_handlers_err = dispatcher:pump({
  handlers = { [""] = function() end },
})
if invalid_handlers ~= nil or invalid_handlers_err == nil then
  dispatcher:close()
  outbox:close()
  client:close()
  error("Lua dispatcher accepted an invalid handler map")
end
local empty_handlers, empty_handlers_err = dispatcher:pump({ handlers = {} })
if empty_handlers ~= nil or empty_handlers_err == nil then
  dispatcher:close()
  outbox:close()
  client:close()
  error("Lua dispatcher accepted an empty handler map")
end
local numeric_kind_handlers, numeric_kind_handlers_err = dispatcher:pump({
  handlers = { [1] = function() end },
})
if numeric_kind_handlers ~= nil or numeric_kind_handlers_err == nil then
  dispatcher:close()
  outbox:close()
  client:close()
  error("Lua dispatcher accepted a non-string handler kind")
end
local nul_kind_handlers, nul_kind_handlers_err = dispatcher:pump({
  handlers = { ["http\0invalid"] = function() end },
})
if nul_kind_handlers ~= nil or nul_kind_handlers_err == nil then
  dispatcher:close()
  outbox:close()
  client:close()
  error("Lua dispatcher accepted an embedded-NUL handler kind")
end

local function assert_ok(value, err, operation)
  if value == nil then
    error(("%s failed: %s"):format(operation, err and err.message or tostring(err)))
  end
  return value
end

-- Participant source/sink callbacks can re-enter the explicit transaction.
-- They must not be allowed to release the lease that the suspended native I/O
-- operation still owns.
local reentrant_entry = {
  operation_id = "lua-streaming-duplicate",
  effect_id = "lua-streaming-duplicate",
  effect_key = "lua-streaming-duplicate",
  payload_digest = "sha256:lua-streaming-duplicate",
  kind = "http",
  destination = "https://example.test/lua-streaming-duplicate",
  content_type = "text/plain",
}
local streaming_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-streaming-records",
  owner = "lua-outbox-streaming-worker",
  transaction_ttl_seconds = 30,
}), nil, "Lua streaming outbox creation")
local reentrant_seed, reentrant_seed_receipt = streaming_outbox:append(
    reentrant_entry, "seed")
reentrant_seed = assert_ok(reentrant_seed, reentrant_seed_receipt,
                           "Lua streaming duplicate seed")
assert_ok(reentrant_seed:commit(), nil,
          "Lua streaming duplicate seed commit")
reentrant_seed:close()
local streaming_txn = assert_ok(streaming_outbox:begin(), nil,
                                "Lua streaming transaction begin")
local streaming_participant = assert_ok(streaming_txn:acquire({
  namespace = "lua-outbox-streaming",
  key = "state",
  owner = "lua-outbox-streaming",
}), nil, "Lua streaming transaction acquire")
local function assert_streaming_terminal_guard()
  for _, terminal in ipairs({ "close", "commit", "rollback" }) do
    local terminal_ok, terminal_err = pcall(function()
      return streaming_txn[terminal](streaming_txn)
    end)
    if terminal_ok or not tostring(terminal_err):find("participant I/O is streaming", 1, true) then
      error("Lua transaction terminal operation escaped a participant streaming callback")
    end
  end
  local staged_ok, staged_err = pcall(function()
    return streaming_txn:append(reentrant_entry, "duplicate")
  end)
  if staged_ok or not tostring(staged_err):find("participant I/O is streaming", 1, true) then
    error("Lua transaction staging escaped a participant streaming callback")
  end
end
local source_sent = false
assert_ok(streaming_participant:update({
  read = function()
    if source_sent then
      return nil
    end
    source_sent = true
    assert_streaming_terminal_guard()
    return "streamed participant state"
  end,
}), nil, "Lua streaming transaction state update")
local streaming_get_output, streaming_get_result = streaming_participant:get({}, {
  write = function()
    assert_streaming_terminal_guard()
    return true
  end,
})
if streaming_get_output ~= nil or type(streaming_get_result) ~= "table" then
  error("Lua streaming transaction state read did not preserve sink semantics")
end
assert_ok(streaming_txn:rollback(), nil,
          "Lua streaming transaction rollback after read")
streaming_participant:close()
streaming_txn:close()
streaming_outbox:close()

local function append_effect(effect_id, payload)
  local txn, receipt_or_err = outbox:append({
    operation_id = "lua-order-1",
    effect_id = effect_id,
    effect_key = "lua-effect:" .. effect_id,
    payload_digest = "sha256:lua-effect:" .. effect_id,
    kind = "http",
    destination = "https://example.test/effects/" .. effect_id,
    content_type = "application/json",
    headers_json = lockdc.encode_json({ ["x-outbox"] = "lua" }),
  }, lockdc.encode_json(payload))
  if txn == nil then
    outbox:close()
    client:close()
    error(("Lua outbox append failed: %s"):format(
      receipt_or_err and receipt_or_err.message or tostring(receipt_or_err)))
  end
  if receipt_or_err.duplicate then
    outbox:close()
    client:close()
    error("fresh Lua outbox append unexpectedly reported duplicate")
  end
  return txn, receipt_or_err
end

local legacy_headers_ok, legacy_headers_err = pcall(function()
  return outbox:append({
    operation_id = "lua-legacy-headers",
    effect_id = "legacy-headers",
    effect_key = "lua-effect:legacy-headers",
    payload_digest = "sha256:lua-legacy-headers",
    kind = "http",
    destination = "https://example.test/effects/legacy-headers",
    headers = { ["x-outbox"] = "legacy" },
  }, "legacy")
end)
if legacy_headers_ok or
    not tostring(legacy_headers_err):find("headers_json", 1, true) then
  dispatcher:close()
  outbox:close()
  client:close()
  error("Lua outbox retained the headers compatibility field")
end

local txn = append_effect("first", { sequence = 1 })
local participant, participant_err = txn:acquire({
  namespace = "lua-outbox-domain",
  key = "order-1",
  owner = "lua-outbox-worker",
  ttl_seconds = 30,
})
participant = assert_ok(participant, participant_err, "Lua outbox participant acquire")
assert_ok(participant:update_json({ status = "paid", revision = 1 }), nil,
          "Lua outbox participant update")
assert_ok(participant:mutate({ mutations = { "/revision++" } }), nil,
          "Lua outbox participant mutation")
local attachment = assert_ok(participant:attach({
  name = "outbox-proof",
  content_type = "text/plain",
}, "proof"), nil, "Lua outbox participant attachment")
if attachment.attachment.name ~= "outbox-proof" then
  participant:close()
  txn:close()
  outbox:close()
  client:close()
  error("Lua outbox participant attachment did not return its name")
end
local attachments = assert_ok(participant:list_attachments(), nil,
                              "Lua outbox participant attachment list")
if #attachments.items ~= 1 or attachments.items[1].name ~= "outbox-proof" then
  participant:close()
  txn:close()
  outbox:close()
  client:close()
  error("Lua outbox participant attachment list lost staged attachment")
end
assert_ok(participant:delete_attachment({ name = "outbox-proof" }), nil,
          "Lua outbox participant attachment delete")
local described, describe_err = participant:describe()
assert_ok(described, describe_err, "Lua outbox participant describe")
if described.version < 2 then
  participant:close()
  txn:close()
  outbox:close()
  client:close()
  error("Lua outbox participant describe lost staged version")
end
participant:close()
assert_ok(txn:commit(), nil, "Lua outbox transaction commit")
txn:close()

local state, state_err = client:read_json({
  namespace = "lua-outbox-domain",
  key = "order-1",
})
if state == nil or state.status ~= "paid" or state.revision ~= 2 then
  outbox:close()
  client:close()
  error(("Lua outbox committed state was not visible: %s"):format(
    state_err and state_err.message or tostring(state_err)))
end

local job, next_err = dispatcher:next(3000)
job = assert_ok(job, next_err, "Lua outbox first job")
if job:info().effect_key ~= "lua-effect:first" then
  job:close()
  outbox:close()
  client:close()
  error("Lua outbox returned the wrong first outbox job")
end
local mixed_mode, mixed_mode_err = dispatcher:pump({
  handlers = { http = function() end },
})
if mixed_mode ~= nil or mixed_mode_err == nil then
  job:close()
  dispatcher:close()
  outbox:close()
  client:close()
  error("Lua dispatcher allowed handler mode after raw pull activation")
end
local second_dispatcher = assert_ok(outbox:dispatcher(), nil,
                                    "Lua outbox second dispatcher wrapper")
mixed_mode, mixed_mode_err = second_dispatcher:pump({
  handlers = { http = function() end },
})
if mixed_mode ~= nil or mixed_mode_err == nil then
  job:close()
  second_dispatcher:close()
  dispatcher:close()
  outbox:close()
  client:close()
  error("Lua dispatcher wrapper bypassed shared raw pull ownership")
end
second_dispatcher:close()
if job.payload ~= nil or job.payload_json ~= nil then
  job:close()
  outbox:close()
  client:close()
  error("Lua outbox retained payload materializer aliases")
end
local streamed_bytes = 0
local streamed_chunks = 0
local sink_closed = 0
local streamed, stream_written = job:write_payload({
  write = function(chunk)
    streamed_chunks = streamed_chunks + 1
    streamed_bytes = streamed_bytes + #chunk
    local closed, close_err = pcall(function() job:close() end)
    if closed or not tostring(close_err):find("not allowed while payload is streaming", 1, true) then
      error("Lua payload sink re-entered and closed its active job")
    end
    return true
  end,
  close = function()
    sink_closed = sink_closed + 1
  end,
})
if streamed ~= nil or type(stream_written) ~= "number" or
    streamed_bytes ~= stream_written or streamed_chunks == 0 or sink_closed ~= 1 then
  job:close()
  outbox:close()
  client:close()
  error("Lua outbox callback sink did not stream and close exactly once")
end
local failed_sink_closed = 0
local failed_stream, failed_stream_err, failed_stream_code = job:write_payload({
  write = function()
    return nil, "injected Lua sink failure"
  end,
  close = function()
    failed_sink_closed = failed_sink_closed + 1
  end,
})
if failed_stream ~= nil or failed_stream_err == nil or failed_stream_code == nil or
    not tostring(failed_stream_err.message):find("injected Lua sink failure", 1, true) or
    failed_sink_closed ~= 1 then
  job:close()
  outbox:close()
  client:close()
  error("Lua outbox callback sink failure was not a structured stream error")
end
local payload, written_or_err = job:read_payload_json()
if payload == nil or payload.sequence ~= 1 or type(written_or_err) ~= "number" then
  job:close()
  outbox:close()
  client:close()
  error("Lua outbox payload did not stream through the façade")
end
assert_ok(job:complete(), nil, "Lua outbox first job completion")

local inbound_txn, accepted_or_err = outbox:accept_inbox({
  consumer_id = "lua-outbox-consumer",
  source_kind = "http",
  source_id = "orders",
  message_id = "message-1",
  payload_digest = "digest-1",
})
if inbound_txn == nil or not accepted_or_err.accepted then
  outbox:close()
  client:close()
  error(("Lua inbox first acceptance failed: %s"):format(
    accepted_or_err and accepted_or_err.message or tostring(accepted_or_err)))
end
assert_ok(inbound_txn:append({
  operation_id = "lua-order-1",
  effect_id = "inbound-effect",
  effect_key = "lua-effect:inbound",
  payload_digest = "sha256:lua-inbound-payload",
  kind = "http",
  destination = "https://example.test/effects/inbound",
}, "inbound-payload"), nil, "Lua inbox transaction outbox append")
assert_ok(inbound_txn:commit(), nil, "Lua inbox transaction commit")
inbound_txn:close()

local duplicate_txn, duplicate_or_err = outbox:accept_inbox({
  consumer_id = "lua-outbox-consumer",
  source_kind = "http",
  source_id = "orders",
  message_id = "message-1",
  payload_digest = "digest-1",
})
if duplicate_txn ~= nil or duplicate_or_err == nil or not duplicate_or_err.duplicate then
  outbox:close()
  client:close()
  error("Lua inbox redelivery was not reported as a duplicate")
end

job = assert_ok(dispatcher:next(3000), nil, "Lua outbox inbound job")
if job:info().effect_key ~= "lua-effect:inbound" then
  job:close()
  outbox:close()
  client:close()
  error("Lua outbox returned the wrong inbox-triggered job")
end
assert_ok(job:complete(), nil, "Lua outbox inbound job completion")

txn = append_effect("retry", { sequence = 2 })
assert_ok(txn:commit(), nil, "Lua outbox retry transaction commit")
txn:close()
job = assert_ok(dispatcher:next(3000), nil, "Lua outbox retry first job")
assert_ok(job:retry({ delay_seconds = 1, diagnostic = "temporary" }), nil,
          "Lua outbox retry scheduling")
job = assert_ok(dispatcher:next(3000), nil, "Lua outbox retry redelivery")
if job:info().attempt ~= 2 then
  job:close()
  outbox:close()
  client:close()
  error("Lua outbox retry did not increment the attempt")
end
local dead_letter_key = job:info().outbox_key
assert_ok(job:dead_letter("permanent"), nil, "Lua outbox dead letter")

local stats = assert_ok(dispatcher:stats(), nil, "Lua outbox dispatcher stats")
if not stats.running then
  outbox:close()
  client:close()
  error("Lua outbox stats did not report a running dispatcher")
end
local exported, export_result = dispatcher:read_dead_letters({ format = "jsonl" })
if exported == nil or export_result == nil or export_result.exported ~= 1 or
    not exported:find("dead_letter", 1, true) or
    exported:find("retry-payload", 1, true) then
  outbox:close()
  client:close()
  error("Lua outbox dead-letter export did not return a metadata-only envelope")
end
assert_ok(dispatcher:replay_dead_letter(dead_letter_key), nil,
          "Lua outbox dead-letter replay")
job = assert_ok(dispatcher:next(3000), nil, "Lua outbox replayed job")
if job:info().effect_key ~= "lua-effect:retry" or job:info().attempt ~= 1 then
  job:close()
  outbox:close()
  client:close()
  error("Lua outbox dead-letter replay did not reset the delivery attempt")
end
assert_ok(job:complete(), nil, "Lua outbox replayed completion")
assert_ok(dispatcher:reconcile(), nil, "Lua outbox reconciliation signal")

-- A delayed retry must leave one delayed wake-up, not repeatedly trigger
-- durable recovery while this single next() call waits for its deadline.
txn = append_effect("retry-spin", { sequence = 3 })
assert_ok(txn:commit(), nil, "Lua outbox retry-spin transaction commit")
txn:close()
job = assert_ok(dispatcher:next(3000), nil, "Lua outbox retry-spin first job")
assert_ok(job:retry({ delay_seconds = 30, diagnostic = "delayed" }), nil,
          "Lua outbox retry-spin scheduling")
local retry_spin_before = assert_ok(dispatcher:stats(), nil,
                                   "Lua outbox retry-spin stats before")
local retry_spin_job, retry_spin_err = dispatcher:next(250)
if retry_spin_job ~= nil or retry_spin_err ~= nil then
  error("Lua outbox retry-spin returned work before its retry deadline")
end
local retry_spin_after = assert_ok(dispatcher:stats(), nil,
                                  "Lua outbox retry-spin stats after")
if retry_spin_after.recovery_queries - retry_spin_before.recovery_queries > 4 then
  error(("Lua outbox delayed retry repeatedly scanned durable outbox state (%d queries; %s)"):
      format(retry_spin_after.recovery_queries - retry_spin_before.recovery_queries,
             tostring(retry_spin_after.last_error)))
end

assert_ok(dispatcher:stop(-1), nil, "Lua outbox dispatcher stop")
dispatcher:close()
outbox:close()

-- Handler mode has its own dispatcher.  It deliberately shares neither a
-- wrapper nor consumption mode with the raw-pull dispatcher above.
local handler_outbox, handler_outbox_err = client:new_outbox({
  namespace = "lua-outbox-handler-records",
  owner = "lua-outbox-handler-worker",
  transaction_ttl_seconds = 30,
  claim_ttl_seconds = 30,
  recovery_interval_seconds = 0,
})
handler_outbox = assert_ok(handler_outbox, handler_outbox_err,
                             "Lua handler outbox creation")
local handler_dispatcher, handler_dispatcher_err = handler_outbox:dispatcher()
handler_dispatcher = assert_ok(handler_dispatcher, handler_dispatcher_err,
                               "Lua handler dispatcher creation")

local function append_handler_effect(effect_id, kind, payload)
  local handler_txn, handler_receipt_or_err = handler_outbox:append({
    operation_id = "lua-handler-order",
    effect_id = effect_id,
    effect_key = "lua-handler-effect:" .. effect_id,
    payload_digest = "sha256:lua-handler:" .. effect_id,
    kind = kind,
    destination = "https://example.test/handler/" .. effect_id,
    content_type = "application/json",
  }, lockdc.encode_json(payload))
  handler_txn = assert_ok(handler_txn, handler_receipt_or_err,
                          "Lua handler outbox append")
  if handler_receipt_or_err.duplicate then
    handler_txn:close()
    error("fresh Lua handler outbox append unexpectedly reported duplicate")
  end
  assert_ok(handler_txn:commit(), nil, "Lua handler transaction commit")
  handler_txn:close()
end

-- A valid Lua handler map must not be remembered until native option
-- validation has accepted the pump.  Otherwise a rejected call would make the
-- following valid call invoke a stale snapshot rather than the corrected
-- application handler.
local handler_calls = {}
local rejected_handler_map = {
  http = function()
    handler_calls.stale_after_rejection = true
  end,
}
local rejected_pump_ok, rejected_pump_err = pcall(function()
  return handler_dispatcher:pump({
    handlers = rejected_handler_map,
    max_jobs = 0,
  })
end)
if rejected_pump_ok or not tostring(rejected_pump_err):find("pump limits are invalid", 1, true) then
  handler_dispatcher:close()
  handler_outbox:close()
  client:close()
  error("Lua dispatcher accepted an invalid native pump option")
end

local handlers = rejected_handler_map
handlers.http = function(handler_job)
    local handler_info = handler_job:info()
    handler_calls.started = (handler_calls.started or 0) + 1
    local payload, payload_err = handler_job:read_payload_json()
    payload = assert_ok(payload, payload_err, "Lua handler payload decode")
    if payload.effect ~= handler_info.effect_key then
      error("Lua handler payload did not match its claimed job")
    end
    handler_calls[handler_info.effect_key] =
      (handler_calls[handler_info.effect_key] or 0) + 1
    assert_ok(handler_job:renew(30), nil, "Lua handler explicit claim renewal")
    if payload.mode == "no-outcome" and handler_info.attempt == 1 then
      return
    end
    if payload.mode == "exception" and handler_info.attempt == 1 then
      error("intentional Lua handler failure")
    end
    if payload.mode == "long-exception" and handler_info.attempt == 1 then
      error(string.rep("x", 5000))
    end
    if payload.mode == "stop" and handler_info.attempt == 1 then
      assert_ok(handler_job:complete(), nil,
                "Lua handler stop-rejection deferred completion")
      local stopped, stop_err, stop_code = handler_dispatcher:stop(0)
      if stopped ~= nil or stop_err == nil or stop_code == nil or
          not tostring(stop_err.message):find("not allowed inside its handler", 1, true) then
        error("Lua outbox handler was allowed to block its own dispatcher stop")
      end
      return
    end
    if payload.mode == "close" and handler_info.attempt == 1 then
      local closed, close_err = pcall(function()
        handler_job:close()
      end)
      if closed or not tostring(close_err):find("not allowed inside its dispatcher handler", 1, true) then
        error("Lua handler was allowed to close its dispatcher-owned job")
      end
      return
    end
    if payload.mode == "reentrant" and handler_info.attempt == 1 then
      for nested_attempt = 1, 2 do
        local nested, nested_err = handler_dispatcher:pump({
          handlers = handlers,
          max_jobs = 1,
          timeout_ms = 0,
        })
        if nested ~= nil or nested_err == nil or
            not tostring(nested_err.message):find("cannot consume recursively", 1, true) then
          error("Lua dispatcher allowed recursive handler consumption")
        end
      end
    end
    if payload.mode == "invalid-outcome" and handler_info.attempt == 1 then
      return handler_job:retry({ delay_seconds = "invalid" })
    end
    if payload.mode == "dead-letter" then
      assert_ok(handler_job:dead_letter("handler permanent failure"), nil,
                "Lua handler dead letter")
      return
    end
    assert_ok(handler_job:complete({ delivery_reference = "lua-handler" }), nil,
              "Lua handler completion")
end

append_handler_effect("complete", "http", {
  effect = "lua-handler-effect:complete",
  mode = "complete",
})
local pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler completion pump")
if pumped ~= 1 or handler_calls["lua-handler-effect:complete"] ~= 1 or
    handler_calls.stale_after_rejection then
  error("Lua handler completion did not execute exactly once")
end

append_handler_effect("reentrant", "http", {
  effect = "lua-handler-effect:reentrant",
  mode = "reentrant",
})
append_handler_effect("reentrant-followup", "http", {
  effect = "lua-handler-effect:reentrant-followup",
  mode = "complete",
})
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler reentrant pump rejection")
if pumped ~= 1 or handler_calls["lua-handler-effect:reentrant"] ~= 1 then
  error("Lua handler recursive dispatcher call was not rejected safely")
end
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler reentrant followup pump")
if pumped ~= 1 or handler_calls["lua-handler-effect:reentrant-followup"] ~= 1 then
  error("Lua handler recursive dispatcher call consumed a second job")
end

local raw_after_handler_ok = pcall(function()
  return handler_dispatcher:next(0)
end)
if raw_after_handler_ok then
  error("Lua dispatcher allowed raw pull after handler activation")
end

append_handler_effect("no-outcome", "http", {
  effect = "lua-handler-effect:no-outcome",
  mode = "no-outcome",
})
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler no-outcome retry pump")
if pumped ~= 1 then
  error("Lua handler no-outcome did not schedule a retry")
end
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler no-outcome redelivery pump")
if pumped ~= 1 or handler_calls["lua-handler-effect:no-outcome"] ~= 2 then
  error("Lua handler no-outcome was not retried exactly once")
end

append_handler_effect("exception", "http", {
  effect = "lua-handler-effect:exception",
  mode = "exception",
})
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler exception retry pump")
if pumped ~= 1 then
  error("Lua handler exception did not schedule a retry")
end
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler exception redelivery pump")
if pumped ~= 1 or handler_calls["lua-handler-effect:exception"] ~= 2 then
  error("Lua handler exception was not retried exactly once")
end

append_handler_effect("long-exception", "http", {
  effect = "lua-handler-effect:long-exception",
  mode = "long-exception",
})
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler long exception retry pump")
if pumped ~= 1 then
  error("Lua handler long exception did not schedule a bounded retry")
end
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler long exception redelivery pump")
if pumped ~= 1 or handler_calls["lua-handler-effect:long-exception"] ~= 2 then
  error("Lua handler long exception stranded its claimed job")
end

append_handler_effect("stop", "http", {
  effect = "lua-handler-effect:stop",
  mode = "stop",
})
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler stop rejection pump")
if pumped ~= 1 then
  error("Lua handler stop rejection did not preserve its deferred outcome")
end

append_handler_effect("invalid-outcome", "http", {
  effect = "lua-handler-effect:invalid-outcome",
  mode = "invalid-outcome",
})
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler invalid-outcome retry pump")
if pumped ~= 1 then
  error("Lua handler invalid outcome did not follow the retry path")
end
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler invalid-outcome redelivery pump")
if pumped ~= 1 or handler_calls["lua-handler-effect:invalid-outcome"] ~= 2 then
  error("Lua handler invalid outcome stranded its claimed job")
end

append_handler_effect("close", "http", {
  effect = "lua-handler-effect:close",
  mode = "close",
})
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler close rejection pump")
if pumped ~= 1 then
  error("Lua handler close rejection did not retry safely")
end
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler close redelivery pump")
if pumped ~= 1 or handler_calls["lua-handler-effect:close"] ~= 2 then
  error("Lua handler close rejection was not safely redelivered")
end

append_handler_effect("dead-letter", "http", {
  effect = "lua-handler-effect:dead-letter",
  mode = "dead-letter",
})
pumped = assert_ok(handler_dispatcher:pump({
  handlers = handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua handler dead-letter pump")
if pumped ~= 1 then
  error("Lua handler dead-letter outcome was not applied")
end
local handler_export, handler_export_res = handler_dispatcher:read_dead_letters({
  format = "jsonl",
})
if handler_export == nil or handler_export_res == nil or
    handler_export_res.exported ~= 1 or
    not handler_export:find("handler permanent failure", 1, true) then
  error("Lua handler dead-letter export lost its terminal diagnostic")
end

assert_ok(handler_dispatcher:stop(-1), nil, "Lua handler dispatcher stop")
handler_dispatcher:close()
handler_outbox:close()

-- A failed C terminal operation must release the handler-owned claim before
-- pump returns. Dispatcher shutdown must not depend on a later Lua GC pass.
local terminal_failure_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-terminal-failure",
  recovery_interval_seconds = 0,
}), nil, "Lua terminal-failure outbox creation")
local terminal_failure_dispatcher = assert_ok(terminal_failure_outbox:dispatcher(), nil,
                                              "Lua terminal-failure dispatcher creation")
local terminal_failure_txn, terminal_failure_receipt_or_err =
  terminal_failure_outbox:append({
    operation_id = "lua-terminal-failure-order",
    effect_id = "terminal-failure",
    effect_key = "lua-terminal-failure-effect",
    payload_digest = "sha256:lua-terminal-failure",
    kind = "http",
    destination = "https://example.test/terminal-failure",
  }, "terminal-failure-payload")
terminal_failure_txn = assert_ok(terminal_failure_txn, terminal_failure_receipt_or_err,
                                 "Lua terminal-failure append")
assert_ok(terminal_failure_txn:commit(), nil, "Lua terminal-failure commit")
terminal_failure_txn:close()
local terminal_failure_pumped, terminal_failure_err = terminal_failure_dispatcher:pump({
  handlers = {
    http = function(job)
      return job:retry({ delay_seconds = -1 })
    end,
  },
  max_jobs = 1,
  timeout_ms = 3000,
})
if terminal_failure_pumped ~= nil or terminal_failure_err == nil then
  error("Lua terminal-failure handler did not surface its invalid retry")
end
assert_ok(terminal_failure_dispatcher:stop(100), nil,
          "Lua terminal-failure dispatcher immediate stop")
terminal_failure_dispatcher:close()
terminal_failure_outbox:close()

-- A handler exception after selecting an outcome must release the registry
-- reference, including a value that closes over the handler-owned job.
local outcome_failure_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-outcome-failure",
  recovery_interval_seconds = 0,
}), nil, "Lua outcome-failure outbox creation")
local outcome_failure_dispatcher = assert_ok(outcome_failure_outbox:dispatcher(), nil,
                                             "Lua outcome-failure dispatcher creation")
local outcome_failure_txn, outcome_failure_receipt_or_err =
  outcome_failure_outbox:append({
    operation_id = "lua-outcome-failure-order",
    effect_id = "outcome-failure",
    effect_key = "lua-outcome-failure-effect",
    payload_digest = "sha256:lua-outcome-failure",
    kind = "http",
    destination = "https://example.test/outcome-failure",
  }, "outcome-failure-payload")
outcome_failure_txn = assert_ok(outcome_failure_txn, outcome_failure_receipt_or_err,
                                "Lua outcome-failure append")
assert_ok(outcome_failure_txn:commit(), nil, "Lua outcome-failure commit")
outcome_failure_txn:close()
local weak_outcomes = setmetatable({}, { __mode = "v" })
assert_ok(outcome_failure_dispatcher:pump({
  handlers = {
    http = function(job)
      local outcome = { context = job }
      weak_outcomes[1] = outcome
      assert_ok(job:complete(outcome), nil, "Lua deferred outcome selection")
      error("intentional Lua deferred outcome failure")
    end,
  },
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua deferred outcome fallback retry")
collectgarbage("collect")
collectgarbage("collect")
if weak_outcomes[1] ~= nil then
  error("Lua deferred outcome failure retained its outcome graph")
end
assert_ok(outcome_failure_dispatcher:stop(100), nil,
          "Lua outcome-failure dispatcher stop")
outcome_failure_dispatcher:close()
outcome_failure_outbox:close()

local missing_outbox, missing_outbox_err = client:new_outbox({
  namespace = "lua-outbox-missing-handler-records",
  owner = "lua-outbox-missing-handler-worker",
  transaction_ttl_seconds = 30,
  claim_ttl_seconds = 1,
  recovery_interval_seconds = 0,
})
missing_outbox = assert_ok(missing_outbox, missing_outbox_err,
                             "Lua missing-handler outbox creation")
local missing_dispatcher, missing_dispatcher_err = missing_outbox:dispatcher()
missing_dispatcher = assert_ok(missing_dispatcher, missing_dispatcher_err,
                               "Lua missing-handler dispatcher creation")
local missing_txn, missing_receipt_or_err = missing_outbox:append({
  operation_id = "lua-missing-handler-order",
  effect_id = "missing-handler",
  effect_key = "lua-missing-handler-effect",
  payload_digest = "sha256:lua-missing-handler",
  kind = "unhandled-kind",
  destination = "https://example.test/handler/missing",
}, "missing-handler-payload")
missing_txn = assert_ok(missing_txn, missing_receipt_or_err,
                        "Lua missing-handler outbox append")
assert_ok(missing_txn:commit(), nil, "Lua missing-handler transaction commit")
missing_txn:close()
local missing_handlers = setmetatable({
  http = function()
    error("Lua missing-handler test invoked an unrelated handler")
  end,
}, {
  __index = function()
    error("Lua dispatcher invoked a handler-map metamethod")
  end,
})
local missing_pumped, missing_pump_err = missing_dispatcher:pump({
  handlers = missing_handlers,
  max_jobs = 1,
  timeout_ms = 3000,
})
if missing_pumped ~= nil or missing_pump_err == nil or
    not tostring(missing_pump_err.message):find("no handler", 1, true) then
  error("Lua handler dispatcher did not return a structured missing-handler error")
end
assert_ok(missing_dispatcher:stop(-1), nil, "Lua missing-handler dispatcher stop")
missing_dispatcher:close()
missing_outbox:close()

-- Separate façade wrappers for one native dispatcher must be able to reuse the
-- same application handler table. The binding owns a stable wrapped map.
local alias_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-dispatcher-alias",
  owner = "lua-outbox-dispatcher-alias-worker",
  recovery_interval_seconds = 0,
}), nil, "Lua dispatcher alias outbox creation")
local alias_dispatcher = assert_ok(alias_outbox:dispatcher(), nil,
                                   "Lua first dispatcher alias")
local alias_dispatcher_again = assert_ok(alias_outbox:dispatcher(), nil,
                                         "Lua second dispatcher alias")
local alias_calls = 0
local alias_handlers = {
  http = function(alias_job)
    alias_calls = alias_calls + 1
    assert_ok(alias_job:complete(), nil, "Lua alias handler completion")
  end,
}
local alias_txn, alias_receipt_or_err = alias_outbox:append({
  operation_id = "lua-alias-order",
  effect_id = "alias",
  effect_key = "lua-alias-effect",
  payload_digest = "sha256:lua-alias",
  kind = "http",
  destination = "https://example.test/alias",
}, "alias-payload")
alias_txn = assert_ok(alias_txn, alias_receipt_or_err,
                      "Lua dispatcher alias outbox append")
assert_ok(alias_txn:commit(), nil, "Lua dispatcher alias transaction commit")
alias_txn:close()
local alias_pumped = assert_ok(alias_dispatcher:pump({
  handlers = alias_handlers,
  max_jobs = 1,
  timeout_ms = 3000,
}), nil, "Lua first dispatcher alias pump")
if alias_pumped ~= 1 or alias_calls ~= 1 then
  error("Lua first dispatcher alias did not run its shared handler")
end
alias_dispatcher:close()
alias_dispatcher = nil
collectgarbage("collect")
collectgarbage("collect")
alias_pumped = assert_ok(alias_dispatcher_again:pump({
  handlers = alias_handlers,
  max_jobs = 1,
  timeout_ms = 0,
}), nil, "Lua second dispatcher alias pump")
if alias_pumped ~= 0 then
  error("Lua second dispatcher alias unexpectedly consumed another job")
end
alias_dispatcher_again:close()
local alias_reopened = assert_ok(alias_outbox:dispatcher(), nil,
                                 "Lua reopened dispatcher alias")
local alias_rebound, alias_rebound_err = alias_reopened:pump({
  handlers = { http = function() end },
  max_jobs = 1,
  timeout_ms = 0,
})
if alias_rebound ~= 0 or alias_rebound_err ~= nil then
  error("Lua dispatcher did not release handler ownership after every wrapper closed")
end
assert_ok(alias_reopened:stop(-1), nil, "Lua dispatcher alias stop")
alias_reopened:close()
alias_outbox:close()

-- Closing the wrapper that entered pump() from its own handler must not leave
-- the shared binding in recursive-consumption state. A surviving alias owns
-- the same native dispatcher and must continue with the same handler map.
local closing_alias_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-dispatcher-close-alias",
  owner = "lua-outbox-dispatcher-close-alias-worker",
  recovery_interval_seconds = 0,
}), nil, "Lua closing dispatcher alias outbox creation")
local closing_alias_first = assert_ok(closing_alias_outbox:dispatcher(), nil,
                                     "Lua closing dispatcher first alias")
local closing_alias_second = assert_ok(closing_alias_outbox:dispatcher(), nil,
                                      "Lua closing dispatcher second alias")
local closing_alias_calls = 0
local closing_alias_handlers = {
  http = function(closing_alias_job)
    closing_alias_calls = closing_alias_calls + 1
    if closing_alias_calls == 1 then
      closing_alias_first:close()
    end
    assert_ok(closing_alias_job:complete(), nil,
              "Lua closing dispatcher alias handler completion")
  end,
}
for closing_alias_index = 1, 2 do
  local closing_alias_txn, closing_alias_receipt_or_err =
      closing_alias_outbox:append({
        operation_id = "lua-closing-alias-order-" .. closing_alias_index,
        effect_id = "closing-alias-" .. closing_alias_index,
        effect_key = "lua-closing-alias-effect-" .. closing_alias_index,
        payload_digest = "sha256:lua-closing-alias-" .. closing_alias_index,
        kind = "http",
        destination = "https://example.test/closing-alias",
      }, "closing-alias-payload")
  closing_alias_txn = assert_ok(closing_alias_txn, closing_alias_receipt_or_err,
                                "Lua closing dispatcher alias append")
  assert_ok(closing_alias_txn:commit(), nil,
            "Lua closing dispatcher alias transaction commit")
  closing_alias_txn:close()
  local closing_alias_pumped = assert_ok(
      (closing_alias_index == 1 and closing_alias_first or closing_alias_second):pump({
        handlers = closing_alias_handlers,
        max_jobs = 1,
        timeout_ms = 3000,
      }), nil, "Lua closing dispatcher alias pump")
  if closing_alias_pumped ~= 1 then
    error("Lua closing dispatcher alias did not consume its effect")
  end
end
if closing_alias_calls ~= 2 then
  error("Lua closing dispatcher alias retained recursive consumption state")
end
assert_ok(closing_alias_second:stop(-1), nil,
          "Lua closing dispatcher alias stop")
closing_alias_second:close()
closing_alias_outbox:close()

-- Options-table metamethod failures happen before consumption begins. After a
-- caller catches the Lua error, a valid handler mode must still be usable.
local options_failure_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-dispatcher-options-failure",
  recovery_interval_seconds = 0,
}), nil, "Lua dispatcher options-failure outbox creation")
local options_failure_dispatcher = assert_ok(options_failure_outbox:dispatcher(), nil,
                                             "Lua dispatcher options-failure creation")
local bad_pump_options = setmetatable({ max_jobs = 1, timeout_ms = 0 }, {
  __index = function(_, key)
    if key == "handlers" then
      error("intentional Lua outbox handlers lookup failure")
    end
  end,
})
local bad_pump_ok = pcall(function()
  options_failure_dispatcher:pump(bad_pump_options)
end)
if bad_pump_ok then
  error("Lua dispatcher options metamethod failure did not escape pump")
end
local options_failure_pumped = assert_ok(options_failure_dispatcher:pump({
  handlers = { http = function() end },
  max_jobs = 1,
  timeout_ms = 0,
}), nil, "Lua dispatcher remained usable after options failure")
if options_failure_pumped ~= 0 then
  error("Lua dispatcher options-failure probe unexpectedly consumed work")
end
assert_ok(options_failure_dispatcher:stop(-1), nil,
          "Lua dispatcher options-failure stop")
options_failure_dispatcher:close()
options_failure_outbox:close()

-- The scoped callback owns transaction completion. Explicit close is rejected
-- so the binding cannot commit or roll back a freed native transaction.
local callback_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-callback-close",
}), nil, "Lua callback outbox creation")
local callback_close_ok, callback_close_err = pcall(function()
  callback_outbox:transaction(function(callback_txn)
    callback_txn:close()
  end)
end)
if callback_close_ok or
    not tostring(callback_close_err):find("not allowed inside its callback", 1, true) then
  error("Lua scoped transaction allowed callback-driven close")
end
local callback_terminal_result, callback_terminal_err = callback_outbox:transaction(function(callback_txn)
  local committed, commit_err = pcall(function() callback_txn:commit() end)
  if committed or not tostring(commit_err):find("not allowed inside its callback", 1, true) then
    error("Lua scoped transaction allowed callback-driven commit")
  end
  local rolled_back, rollback_err = pcall(function() callback_txn:rollback() end)
  if rolled_back or not tostring(rollback_err):find("not allowed inside its callback", 1, true) then
    error("Lua scoped transaction allowed callback-driven rollback")
  end
  assert_ok(callback_txn:append({
    operation_id = "lua-callback-terminal-order",
    effect_id = "callback-terminal",
    effect_key = "lua-callback-terminal-effect",
    payload_digest = "sha256:lua-callback-terminal",
    kind = "http",
    destination = "https://example.test/callback-terminal",
  }, "callback-terminal-payload"), nil, "Lua callback terminal outbox append")
  -- Callback values are not transaction results. In particular, an explicit
  -- nil must not precede the commit result and make normal result/error callers
  -- interpret this durable success as failure.
  return nil
end)
if callback_terminal_result == nil or callback_terminal_err ~= nil or
    #callback_terminal_result.outbox_receipts ~= 1 then
  error("Lua callback façade did not retain exclusive transaction completion")
end

-- A callback participant mutation returns one Lua result on success. Its
-- return count is not an lc_status and must never make the callback rollback.
local callback_participant_result, callback_participant_err =
    callback_outbox:transaction(function(callback_txn)
  local participant = assert_ok(callback_txn:acquire({
    key = "callback-successful-participant-domain",
    owner = "lua-callback-successful-participant",
    ttl_seconds = 30,
  }), nil, "Lua callback successful participant acquire")
  assert_ok(participant:update_json({ committed = true }), nil,
            "Lua callback successful participant update")
  participant:close()
end)
if callback_participant_result == nil or callback_participant_err ~= nil then
  error("Lua callback rolled back a successful participant update")
end
-- Client:read_json addresses the client's default namespace. Read the distinct
-- outbox namespace through a fresh lease, which also proves the state was
-- committed rather than merely remaining visible to the callback participant.
local callback_participant_reader = assert_ok(client:acquire({
  namespace = "lua-outbox-callback-close",
  key = "callback-successful-participant-domain",
  owner = "lua-callback-successful-participant-reader",
  ttl_seconds = 30,
}), nil, "Lua callback successful participant durable read acquire")
local callback_participant_state, callback_participant_meta =
    callback_participant_reader:read_json()
assert_ok(callback_participant_reader:release({ rollback = true }), nil,
          "Lua callback successful participant durable read release")
if callback_participant_state == nil or callback_participant_meta == nil or
    callback_participant_state.committed ~= true then
  error("Lua callback did not persist a successful participant update")
end

local callback_seed_txn = assert_ok(callback_outbox:append({
  operation_id = "lua-callback-duplicate-order",
  effect_id = "callback-duplicate",
  effect_key = "lua-callback-duplicate-effect",
  payload_digest = "sha256:lua-callback-duplicate",
  kind = "http",
  destination = "https://example.test/callback-duplicate",
}, "callback-duplicate-payload"), nil, "Lua callback duplicate seed append")
assert_ok(callback_seed_txn:commit(), nil, "Lua callback duplicate seed commit")
callback_seed_txn:close()
local callback_duplicate, callback_duplicate_err = callback_outbox:transaction(function(callback_txn)
  -- Deliberately do not return this receipt. The callback wrapper must retain
  -- the duplicate result because it is the only durable outcome of this
  -- no-participant transaction.
  assert_ok(callback_txn:append({
    operation_id = "lua-callback-duplicate-order",
    effect_id = "callback-duplicate",
    effect_key = "lua-callback-duplicate-effect",
    payload_digest = "sha256:lua-callback-duplicate",
    kind = "http",
    destination = "https://example.test/callback-duplicate",
  }, "callback-duplicate-payload"), nil, "Lua callback duplicate append")
end)
if callback_duplicate == nil or callback_duplicate_err ~= nil or
    not callback_duplicate.duplicate then
  error("Lua callback façade did not return its duplicate-only durable result")
end

local callback_rollback, callback_rollback_err = callback_outbox:transaction(function(callback_txn)
  local participant = assert_ok(callback_txn:acquire({
    key = "callback-conflicting-outbox-domain",
    owner = "lua-callback-conflicting-outbox",
    ttl_seconds = 30,
  }), nil, "Lua callback conflicting outbox acquire")
  assert_ok(participant:update_json({ committed = false }), nil,
            "Lua callback conflicting outbox update")
  participant:close()
  local append_result, append_err = callback_txn:append({
    operation_id = "lua-callback-duplicate-order",
    effect_id = "callback-duplicate",
    effect_key = "lua-callback-duplicate-effect",
    payload_digest = "sha256:lua-callback-duplicate-conflict",
    kind = "http",
    destination = "https://example.test/callback-duplicate",
  }, "callback-duplicate-conflict-payload")
  if append_result ~= nil or append_err == nil then
    error("Lua callback conflicting outbox append unexpectedly succeeded")
  end
  -- Returning normally must still roll back the earlier domain participant.
end)
if callback_rollback ~= nil or callback_rollback_err == nil then
  error("Lua callback committed after an ignored staging failure")
end
local callback_rollback_state, callback_rollback_meta =
    client:read_json({
      namespace = "lua-outbox-callback-close",
      key = "callback-conflicting-outbox-domain",
      public_read = true,
    })
if callback_rollback_state ~= nil or callback_rollback_meta == nil or
    not callback_rollback_meta.no_content then
  error("Lua callback staging failure persisted prior domain state")
end

local callback_command_rollback, callback_command_rollback_err =
    callback_outbox:transaction(function(callback_txn)
  assert_ok(callback_txn:accept_command({
    scope = "lua-callback-command-source-failure",
    command_type = "complete",
    idempotency_key = "one",
    request_digest = "sha256:lua-callback-command-source-failure",
  }), nil, "Lua callback command acceptance")
  local participant = assert_ok(callback_txn:acquire({
    key = "callback-command-source-failure-domain",
    owner = "lua-callback-command-source-failure",
    ttl_seconds = 30,
  }), nil, "Lua callback command source failure acquire")
  assert_ok(participant:update_json({ committed = false }), nil,
            "Lua callback command source failure update")
  participant:close()
  local completed, complete_err = callback_txn:complete_command({
    result_code = "ok",
    content_type = "text/plain",
    body = { path = root .. "/missing-command-result" },
  })
  if completed ~= nil or complete_err == nil then
    error("Lua callback command source failure unexpectedly completed")
  end
  -- Returning normally after a source-construction failure is rollback-only.
end)
if callback_command_rollback ~= nil or callback_command_rollback_err == nil then
  error("Lua callback committed after a command result source failure")
end
local callback_command_state, callback_command_meta = client:read_json({
  namespace = "lua-outbox-callback-close",
  key = "callback-command-source-failure-domain",
  public_read = true,
})
if callback_command_state ~= nil or callback_command_meta == nil or
    not callback_command_meta.no_content then
  error("Lua callback command source failure persisted prior domain state")
end
callback_outbox:close()

-- Lua table lookups may execute user code. Closing a wrapper from an options
-- or handler metatable must be a normal structured failure, never a native
-- use-after-free.
local close_dispatcher_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-reentrant-dispatcher-close",
}), nil, "Lua reentrant dispatcher-close outbox creation")
local close_dispatcher = assert_ok(close_dispatcher_outbox:dispatcher(), nil,
                                   "Lua reentrant dispatcher creation")
local close_dispatcher_core = close_dispatcher._core
local close_dispatcher_options = setmetatable({}, {
  __index = function(_, field)
    if field == "handlers" then
      close_dispatcher_core:close()
      return { http = function() end }
    end
  end,
})
local close_dispatcher_result, close_dispatcher_err =
    close_dispatcher_core:pump(close_dispatcher_options)
if close_dispatcher_result ~= nil or close_dispatcher_err == nil then
  error("Lua dispatcher continued after options lookup closed its wrapper")
end
close_dispatcher_outbox:close()

local close_query_client, close_query_open_err = lockdc.open({
  endpoints = { "pouch://" .. root .. "-reentrant-query-client" },
  default_namespace = "lua-outbox-reentrant-query-close",
})
close_query_client = assert_ok(close_query_client, close_query_open_err,
                               "Lua reentrant query-close client open")
local close_query_client_core = close_query_client._core
local close_query_handler = setmetatable({
  chunk = function() end,
}, {
  __index = function(_, field)
    if field == "begin" then
      close_query_client_core:close()
    end
  end,
})
local close_query_result, close_query_err = close_query_client_core:query_keys({
  engine = "scan",
}, close_query_handler)
if close_query_result ~= nil or close_query_err == nil then
  error("Lua query_keys continued after handler setup closed its client")
end

-- Field decoding for a deferred handler outcome is still part of the handler
-- ownership boundary.  A hostile completion-table metatable must not close
-- the native claim below its terminal call; it follows the normal retry path.
local outcome_close_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-reentrant-outcome-close",
}), nil, "Lua reentrant outcome-close outbox creation")
local outcome_close_dispatcher = assert_ok(outcome_close_outbox:dispatcher(), nil,
                                           "Lua reentrant outcome-close dispatcher")
local outcome_close_txn, outcome_close_append_err = outcome_close_outbox:append({
  operation_id = "lua-reentrant-outcome-close-order",
  effect_id = "reentrant-outcome-close",
  effect_key = "lua-reentrant-outcome-close-effect",
  payload_digest = "sha256:lua-reentrant-outcome-close",
  kind = "http",
  destination = "https://example.test/reentrant-outcome-close",
}, "reentrant-outcome-close-payload")
outcome_close_txn = assert_ok(outcome_close_txn, outcome_close_append_err,
                              "Lua reentrant outcome-close outbox append")
assert_ok(outcome_close_txn:commit(), nil,
          "Lua reentrant outcome-close transaction commit")
outcome_close_txn:close()
local outcome_close_pumped, outcome_close_pump_err = outcome_close_dispatcher:pump({
  handlers = {
    http = function(job)
      return job:complete(setmetatable({}, {
        __index = function(_, field)
          if field == "delivery_reference" then
            job:close()
          end
          return nil
        end,
      }))
    end,
  },
  max_jobs = 1,
  timeout_ms = 0,
})
outcome_close_pumped = assert_ok(outcome_close_pumped, outcome_close_pump_err,
                                 "Lua reentrant outcome-close pump")
if outcome_close_pumped ~= 1 then
  error("Lua reentrant outcome-close did not retry its claimed job")
end
assert_ok(outcome_close_dispatcher:stop(-1), nil,
          "Lua reentrant outcome-close dispatcher stop")
outcome_close_dispatcher:close()
outcome_close_outbox:close()

-- Query callback setup must not leave registry references behind when a later
-- metamethod lookup fails.  This keeps a failed request from retaining user
-- callback graphs until Lua state teardown.
local query_leak_payload = { marker = "query-handler-registry-leak" }
local query_leak_weak = setmetatable({ query_leak_payload }, { __mode = "v" })
local query_leak_begin = function()
  return query_leak_payload
end
local query_leak_handler = setmetatable({}, {
  __index = function(_, field)
    if field == "begin" then
      return query_leak_begin
    end
    if field == "chunk" then
      error("intentional query handler setup failure")
    end
  end,
})
local query_leak_ok = pcall(function()
  client._core:query_keys({ engine = "scan" }, query_leak_handler)
end)
if query_leak_ok then
  error("Lua query handler setup failure unexpectedly succeeded")
end
query_leak_handler = nil
query_leak_begin = nil
query_leak_payload = nil
collectgarbage("collect")
collectgarbage("collect")
if query_leak_weak[1] ~= nil then
  error("Lua failed query handler setup retained callback state")
end

-- A reconciliation scan can rediscover a key already handed to the local
-- notification queue. That is a no-op, not an overflow or an eager retry.
local duplicate_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-duplicate-notification",
  recovery_interval_seconds = 0,
}), nil, "Lua duplicate notification outbox creation")
local duplicate_dispatcher = assert_ok(duplicate_outbox:dispatcher(), nil,
                                       "Lua duplicate notification dispatcher")
local duplicate_txn, duplicate_receipt_or_err = duplicate_outbox:append({
  operation_id = "lua-duplicate-notification-order",
  effect_id = "duplicate-notification",
  effect_key = "lua-duplicate-notification-effect",
  payload_digest = "sha256:lua-duplicate-notification",
  kind = "http",
  destination = "https://example.test/duplicate-notification",
}, "duplicate-notification-payload")
duplicate_txn = assert_ok(duplicate_txn, duplicate_receipt_or_err,
                          "Lua duplicate notification outbox append")
assert_ok(duplicate_txn:commit(), nil,
          "Lua duplicate notification transaction commit")
duplicate_txn:close()
assert_ok(duplicate_dispatcher:reconcile(), nil,
          "Lua duplicate notification reconciliation")
local duplicate_stats = assert_ok(duplicate_dispatcher:stats(), nil,
                                  "Lua duplicate notification stats")
if duplicate_stats.notification_overflows ~= 0 then
  error("Lua duplicate notification scheduled a recovery overflow")
end
local duplicate_job = assert_ok(duplicate_dispatcher:next(3000), nil,
                                "Lua duplicate notification job")
assert_ok(duplicate_job:complete(), nil,
          "Lua duplicate notification completion")
assert_ok(duplicate_dispatcher:stop(-1), nil,
          "Lua duplicate notification dispatcher stop")
duplicate_dispatcher:close()
duplicate_outbox:close()

-- Replacing a stopped attachment on one producer must release the old
-- attachment before starting the replacement dispatcher.
local restart_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-dispatcher-restart",
  recovery_interval_seconds = 0,
}), nil, "Lua restart outbox creation")
for restart_index = 1, 3 do
  local restart_dispatcher = assert_ok(restart_outbox:dispatcher(), nil,
                                       "Lua restart dispatcher creation")
  assert_ok(restart_dispatcher:stop(-1), nil, "Lua restart dispatcher stop")
  restart_dispatcher:close()
end
restart_outbox:close()

-- Closing a client is a native dispatcher stop too. Once every Lua wrapper is
-- closed, that shutdown must release the binding's registry-held handler map.
local binding_client = assert_ok(lockdc.open({
  endpoints = { "pouch://" .. root .. "-binding-close" },
}), nil, "Lua binding-close client creation")
local binding_outbox = assert_ok(binding_client:new_outbox({
  namespace = "lua-outbox-binding-close",
  recovery_interval_seconds = 0,
}), nil, "Lua binding-close outbox creation")
local binding_dispatcher = assert_ok(binding_outbox:dispatcher(), nil,
                                     "Lua binding-close dispatcher creation")
local weak_handlers = setmetatable({}, { __mode = "v" })
do
  local binding_handlers = { http = function() end }
  weak_handlers[1] = binding_handlers
  local pumped = assert_ok(binding_dispatcher:pump({
    handlers = binding_handlers,
    max_jobs = 1,
    timeout_ms = 0,
  }), nil, "Lua binding-close handler binding")
  if pumped ~= 0 then
    error("Lua binding-close dispatcher unexpectedly consumed a job")
  end
  binding_handlers = nil
end
binding_dispatcher:close()
binding_outbox:close()
binding_client:close()
collectgarbage("collect")
collectgarbage("collect")
if weak_handlers[1] ~= nil then
  error("Lua client shutdown retained a closed dispatcher handler map")
end

-- Dispatcher bindings must not registry-root their owner client. When an
-- embedding runtime drops a complete outbox graph, client finalization is
-- responsible for stopping its private worker and releasing Pouch resources.
local weak_clients = setmetatable({}, { __mode = "v" })
do
  local collected_client = assert_ok(lockdc.open({
    endpoints = { "pouch://" .. root .. "-binding-gc" },
  }), nil, "Lua binding-gc client creation")
  weak_clients[1] = collected_client
  local collected_outbox = assert_ok(collected_client:new_outbox({
    namespace = "lua-outbox-binding-gc",
    recovery_interval_seconds = 0,
  }), nil, "Lua binding-gc outbox creation")
  local collected_dispatcher = assert_ok(collected_outbox:dispatcher(), nil,
                                         "Lua binding-gc dispatcher creation")
  collected_dispatcher:close()
  collected_outbox:close()
end
collectgarbage("collect")
collectgarbage("collect")
if weak_clients[1] ~= nil then
  error("Lua dispatcher binding retained an unreachable client")
end

-- A handler may naturally close over its client. The C binding must not turn
-- that otherwise collectible Lua graph into a registry root after its final
-- dispatcher wrapper is gone.
local captured_clients = setmetatable({}, { __mode = "v" })
do
  local captured_client = assert_ok(lockdc.open({
    endpoints = { "pouch://" .. root .. "-binding-captured-client" },
  }), nil, "Lua captured-client binding creation")
  captured_clients[1] = captured_client
  local captured_outbox = assert_ok(captured_client:new_outbox({
    namespace = "lua-outbox-binding-captured-client",
    recovery_interval_seconds = 0,
  }), nil, "Lua captured-client outbox creation")
  local captured_dispatcher = assert_ok(captured_outbox:dispatcher(), nil,
                                        "Lua captured-client dispatcher creation")
  assert_ok(captured_dispatcher:pump({
    handlers = { http = function() return captured_client:info() end },
    max_jobs = 1,
    timeout_ms = 0,
  }), nil, "Lua captured-client handler binding")
  captured_dispatcher:close()
  captured_outbox:close()
end
collectgarbage("collect")
collectgarbage("collect")
if captured_clients[1] ~= nil then
  error("Lua dispatcher handler closure retained an unreachable client")
end

-- A handler may also close over its dispatcher. The handler table and its
-- dispatcher userdata form a Lua cycle, which must stay collectible rather
-- than being held alive by a binding registry reference.
local captured_dispatchers = setmetatable({}, { __mode = "v" })
do
  local captured_dispatcher_client = assert_ok(lockdc.open({
    endpoints = { "pouch://" .. root .. "-binding-captured-dispatcher" },
  }), nil, "Lua captured-dispatcher client creation")
  local captured_dispatcher_outbox = assert_ok(captured_dispatcher_client:new_outbox({
    namespace = "lua-outbox-binding-captured-dispatcher",
    recovery_interval_seconds = 0,
  }), nil, "Lua captured-dispatcher outbox creation")
  local captured_dispatcher = assert_ok(captured_dispatcher_outbox:dispatcher(), nil,
                                        "Lua captured-dispatcher creation")
  local captured_dispatcher_ref = captured_dispatcher
  captured_dispatchers[1] = captured_dispatcher
  assert_ok(captured_dispatcher:pump({
    handlers = { http = function() return captured_dispatcher_ref:stats() end },
    max_jobs = 1,
    timeout_ms = 0,
  }), nil, "Lua captured-dispatcher handler binding")
end
collectgarbage("collect")
collectgarbage("collect")
if captured_dispatchers[1] ~= nil then
  error("Lua dispatcher handler closure retained an unreachable dispatcher")
end

-- The coroutine that creates a binding and the main thread that closes the
-- client share one Lua VM. Main-thread close must therefore retire that
-- coroutine-owned binding and its handler map.
local coroutine_handlers = setmetatable({}, { __mode = "v" })
local coroutine_client = assert_ok(lockdc.open({
  endpoints = { "pouch://" .. root .. "-binding-coroutine" },
}), nil, "Lua coroutine binding client creation")
local coroutine_ok, coroutine_dispatcher = coroutine.resume(coroutine.create(function()
  local coroutine_outbox = assert_ok(coroutine_client:new_outbox({
    namespace = "lua-outbox-binding-coroutine",
    recovery_interval_seconds = 0,
  }), nil, "Lua coroutine outbox creation")
  local dispatcher = assert_ok(coroutine_outbox:dispatcher(), nil,
                               "Lua coroutine dispatcher creation")
  local handlers = { http = function() end }
  coroutine_handlers[1] = handlers
  assert_ok(dispatcher:pump({ handlers = handlers, max_jobs = 1, timeout_ms = 0 }),
            nil, "Lua coroutine handler binding")
  coroutine_outbox:close()
  return dispatcher
end))
if not coroutine_ok then
  error("Lua coroutine dispatcher creation failed: " .. tostring(coroutine_dispatcher))
end
coroutine_dispatcher:close()
coroutine_client:close()
coroutine_client = nil
collectgarbage("collect")
collectgarbage("collect")
if coroutine_handlers[1] ~= nil then
  error("Lua main-thread client close retained a coroutine handler map")
end

-- A streaming key callback may close its client. The active native call keeps
-- its own reference until the Pouch scan and Lua callback unwind, so closure
-- cannot invalidate the scan's state mid-visit.
local callback_close_client = assert_ok(lockdc.open({
  endpoints = { "pouch://" .. root .. "-query-callback-close" },
}), nil, "Lua query callback-close client creation")
local callback_close_lease = assert_ok(callback_close_client:acquire({
  namespace = "lua-query-callback-close",
  key = "callback-close-key",
  owner = "lua-query-callback-close-owner",
  ttl_seconds = 30,
}), nil, "Lua query callback-close lease acquisition")
assert_ok(callback_close_lease:update_json({ visible = true }), nil,
          "Lua query callback-close state update")
assert_ok(callback_close_lease:release(), nil,
          "Lua query callback-close lease release")
local callback_close_seen = false
local callback_close_result, callback_close_err = callback_close_client:query_keys({
  namespace = "lua-query-callback-close",
  engine = "scan",
}, function(key)
  if key ~= "callback-close-key" then
    error("Lua query callback-close received an unexpected key")
  end
  callback_close_seen = true
  callback_close_client:close()
end)
if callback_close_result == nil or callback_close_err ~= nil or not callback_close_seen then
  error("Lua query callback-close did not complete after client closure")
end

-- Client close must stop its private worker, but not wait on the durable job
-- handed to the caller. The retained job remains safely closable afterwards.
local shutdown_outbox = assert_ok(client:new_outbox({
  namespace = "lua-outbox-client-close-active-job",
  owner = "lua-outbox-client-close-worker",
  recovery_interval_seconds = 0,
}), nil, "Lua client-close outbox creation")
local shutdown_dispatcher = assert_ok(shutdown_outbox:dispatcher(), nil,
                                      "Lua client-close dispatcher creation")
local shutdown_txn, shutdown_receipt_or_err = shutdown_outbox:append({
  operation_id = "lua-client-close-order",
  effect_id = "client-close",
  effect_key = "lua-client-close-effect",
  payload_digest = "sha256:lua-client-close",
  kind = "http",
  destination = "https://example.test/client-close",
}, "client-close-payload")
shutdown_txn = assert_ok(shutdown_txn, shutdown_receipt_or_err,
                         "Lua client-close outbox append")
assert_ok(shutdown_txn:commit(), nil, "Lua client-close transaction commit")
shutdown_txn:close()
local shutdown_job = assert_ok(shutdown_dispatcher:next(3000), nil,
                               "Lua client-close active job")
shutdown_outbox:close()
client:close()
shutdown_job:close()
shutdown_dispatcher:close()

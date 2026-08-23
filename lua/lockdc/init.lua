local core = require("lockdc.core")
local lonejson = require("lonejson")

local M = { core = core }

local Client = {}
Client.__index = Client

local Lease = {}
Lease.__index = Lease

local Message = {}
Message.__index = Message

local Workflow = {}
Workflow.__index = Workflow

local WorkflowTransaction = {}
WorkflowTransaction.__index = WorkflowTransaction

local WorkflowParticipant = {}
WorkflowParticipant.__index = WorkflowParticipant

local OutboxJob = {}
OutboxJob.__index = OutboxJob

local Service = {}
Service.__index = Service

local JSON_NULL = lonejson.json_null

local function wrap_client(core_client)
  return setmetatable({ _core = core_client }, Client)
end

local function wrap_lease(core_lease)
  return setmetatable({ _core = core_lease, _closed = false }, Lease)
end

local function wrap_message(core_message)
  return setmetatable({ _core = core_message, _closed = false }, Message)
end

local function wrap_workflow(core_workflow)
  return setmetatable({ _core = core_workflow, _closed = false }, Workflow)
end

local function wrap_workflow_transaction(core_transaction)
  return setmetatable({ _core = core_transaction, _closed = false }, WorkflowTransaction)
end

local function wrap_workflow_participant(core_participant)
  return setmetatable({ _core = core_participant, _closed = false }, WorkflowParticipant)
end

local function wrap_outbox_job(core_job)
  return setmetatable({ _core = core_job, _closed = false }, OutboxJob)
end

local function normalize_result(a, b)
  if a == nil then
    return nil, b
  end
  return a, b
end

local function encode_json(value)
  return lonejson.encode_json(value == nil and JSON_NULL or value)
end

local function decode_json(payload)
  return lonejson.decode_json(payload)
end

local function with_json_content_type(req)
  local next_req = {}
  local k, v

  if req ~= nil then
    for k, v in pairs(req) do
      next_req[k] = v
    end
  end
  if next_req.content_type == nil then
    next_req.content_type = "application/json"
  end
  return next_req
end

local function normalize_outbox_entry(entry)
  local next_entry = {}
  local k, v

  entry = entry or {}
  for k, v in pairs(entry) do
    next_entry[k] = v
  end
  if next_entry.headers ~= nil then
    if next_entry.headers_json ~= nil then
      error("outbox entry accepts either headers or headers_json, not both")
    end
    next_entry.headers_json = encode_json(next_entry.headers)
    next_entry.headers = nil
  end
  return next_entry
end

local function unwrap_lease_ref(value)
  if type(value) == "table" and getmetatable(value) == Lease then
    return value._core
  end
  return value
end

local function unwrap_message_ref(value)
  if type(value) == "table" and getmetatable(value) == Message then
    return value._core
  end
  return value
end

local function flatten_lease_request(req)
  local next_req = {}
  local k, v
  local lease_ref

  if type(req) == "table" and getmetatable(req) == Lease then
    return req:info()
  end
  req = req or {}
  for k, v in pairs(req) do
    next_req[k] = v
  end
  lease_ref = req.lease
  if lease_ref ~= nil and next_req.namespace_name == nil then
    if type(lease_ref) == "table" and getmetatable(lease_ref) == Lease then
      lease_ref = lease_ref:info()
    end
    if type(lease_ref) == "table" then
      for k, v in pairs(lease_ref) do
        if next_req[k] == nil then
          next_req[k] = v
        end
      end
    end
  end
  next_req.lease = nil
  return next_req
end

local function flatten_message_request(req)
  local next_req = {}
  local k, v
  local message_ref

  if type(req) == "table" and getmetatable(req) == Message then
    return req:info()
  end
  req = req or {}
  for k, v in pairs(req) do
    next_req[k] = v
  end
  message_ref = req.message
  if message_ref ~= nil and next_req.namespace_name == nil then
    if type(message_ref) == "table" and getmetatable(message_ref) == Message then
      message_ref = message_ref:info()
    end
    if type(message_ref) == "table" then
      for k, v in pairs(message_ref) do
        if next_req[k] == nil then
          next_req[k] = v
        end
      end
    end
  end
  next_req.message = nil
  return next_req
end

function M.encode_json(value)
  return encode_json(value)
end

function M.decode_json(payload)
  return decode_json(payload)
end

function M.version_string()
  return core.version_string()
end

M.json_null = JSON_NULL

function M.open(config)
  local client, err = core.open(config)

  if client == nil then
    return nil, err
  end
  return wrap_client(client)
end

function Client:info()
  return self._core:info()
end

function Client:close()
  if self._core ~= nil then
    self._core:close()
    self._core = nil
  end
end

function Client:new_workflow(config)
  local workflow, err = self._core:new_workflow(config)

  if workflow == nil then
    return nil, err
  end
  return wrap_workflow(workflow)
end

function Client:acquire(req)
  local lease, err = self._core:acquire(req)

  if lease == nil then
    return nil, err
  end
  return wrap_lease(lease)
end

function Client:acquire_for_update(req, handler)
  return normalize_result(self._core:acquire_for_update(req, function(ctx)
    local lease = wrap_lease(ctx.lease)
    local af = {
      lease = lease,
      state = ctx.state,
      state_meta = ctx.state_meta,
    }

    function af:load_json()
      if self.state == nil then
        return nil, self.state_meta
      end
      return decode_json(self.state), self.state_meta
    end

    function af:update_raw(body, opts)
      return lease:update_raw(body, opts)
    end

    function af:update_json(value, opts)
      return lease:update_json(value, opts)
    end

    function af:mutate(update_req)
      return lease:mutate(update_req)
    end

    function af:mutate_local(update_req)
      return lease:mutate_local(update_req)
    end

    function af:metadata(update_req)
      return lease:metadata(update_req)
    end

    function af:remove(update_req)
      return lease:remove(update_req)
    end

    function af:keepalive(update_req)
      return lease:keepalive(update_req)
    end

    local ok, result, handler_err = pcall(handler, af)
    lease:close()
    if not ok then
      error(result)
    end
    if handler_err ~= nil then
      return nil, handler_err
    end
    if result == false then
      return nil, "acquire_for_update handler returned false"
    end
    if type(result) == "string" then
      return nil, result
    end
    return nil
  end))
end

function Client:describe(req)
  return self._core:describe(req)
end

function Client:get_raw(req, dest)
  return self._core:get(req, dest)
end

function Client:get_json(req)
  local payload, meta_or_err = self._core:get(req)

  if payload == nil then
    return nil, meta_or_err
  end
  if meta_or_err ~= nil and meta_or_err.no_content then
    return nil, meta_or_err
  end
  return decode_json(payload), meta_or_err
end

function Client:update_raw(req, body)
  return self._core:update(flatten_lease_request(req), body)
end

function Client:update_json(req, value)
  return self:update_raw(with_json_content_type(req), encode_json(value))
end

function Client:mutate(req)
  return self._core:mutate(flatten_lease_request(req))
end

function Client:metadata(req)
  return self._core:metadata(flatten_lease_request(req))
end

function Client:remove(req)
  return self._core:remove(flatten_lease_request(req))
end

function Client:keepalive(req)
  return self._core:keepalive(flatten_lease_request(req))
end

function Client:release(req)
  return self._core:release(flatten_lease_request(req))
end

function Client:attach(req, body)
  return self._core:attach(flatten_lease_request(req), body)
end

function Client:list_attachments(req)
  return self._core:list_attachments(flatten_lease_request(req))
end

function Client:get_attachment(req, dest)
  return self._core:get_attachment(flatten_lease_request(req), dest)
end

function Client:delete_attachment(req)
  return self._core:delete_attachment(flatten_lease_request(req))
end

function Client:delete_all_attachments(req)
  return self._core:delete_all_attachments(flatten_lease_request(req))
end

function Client:queue_stats(req)
  return self._core:queue_stats(req)
end

function Client:queue_ack(message_or_req)
  return self._core:queue_ack(unwrap_message_ref(message_or_req))
end

function Client:queue_nack(req)
  return self._core:queue_nack(flatten_message_request(req))
end

function Client:queue_extend(req)
  return self._core:queue_extend(flatten_message_request(req))
end

function Client:query_raw(req, dest)
  return self._core:query(req, dest)
end

function Client:get_namespace_config(req)
  return self._core:get_namespace_config(req)
end

function Client:update_namespace_config(req)
  return self._core:update_namespace_config(req)
end

function Client:flush_index(req)
  return self._core:flush_index(req)
end

function Client:enqueue(req, body)
  return self._core:enqueue(req, body)
end

function Client:dequeue(req)
  local message, err = self._core:dequeue(req)

  if message == nil then
    return nil, err
  end
  return wrap_message(message)
end

function Client:dequeue_batch(req)
  local batch, err = self._core:dequeue_batch(req)
  local i

  if batch == nil then
    return nil, err
  end
  for i = 1, #batch do
    batch[i] = wrap_message(batch[i])
  end
  return batch
end

function Client:dequeue_with_state(req)
  local message, err = self._core:dequeue_with_state(req)

  if message == nil then
    return nil, err
  end
  return wrap_message(message)
end

function Lease:info()
  return self._core:info()
end

function Lease:close()
  if self._core ~= nil and not self._closed then
    self._core:close()
    self._closed = true
  end
end

function Lease:describe()
  return self._core:describe()
end

function Lease:get_raw(req, dest)
  return self._core:get(req, dest)
end

function Lease:get_json(req)
  local payload, meta_or_err = self._core:get(req)

  if payload == nil then
    return nil, meta_or_err
  end
  if meta_or_err ~= nil and meta_or_err.no_content then
    return nil, meta_or_err
  end
  return decode_json(payload), meta_or_err
end

function Lease:update_raw(body, req)
  return self._core:update(body, req)
end

function Lease:update_json(value, req)
  return self:update_raw(encode_json(value), with_json_content_type(req))
end

function Lease:mutate(req)
  return self._core:mutate(req)
end

function Lease:mutate_local(req)
  return self._core:mutate_local(req)
end

function Lease:metadata(req)
  return self._core:metadata(req)
end

function Lease:remove(req)
  return self._core:remove(req)
end

function Lease:keepalive(req)
  return self._core:keepalive(req)
end

function Lease:release(req)
  local ok, err = normalize_result(self._core:release(req))

  if ok ~= nil then
    self._closed = true
  end
  return ok, err
end

function Lease:attach(req, body)
  return self._core:attach(req, body)
end

function Lease:list_attachments()
  return self._core:list_attachments()
end

function Lease:get_attachment(req, dest)
  return self._core:get_attachment(req, dest)
end

function Lease:delete_attachment(selector)
  return self._core:delete_attachment(selector)
end

function Lease:delete_all_attachments()
  return self._core:delete_all_attachments()
end

function Message:info()
  return self._core:info()
end

function Message:is_open()
  return not self._closed
end

function Message:close()
  if self._core ~= nil and not self._closed then
    self._core:close()
    self._closed = true
  end
end

function Message:ack()
  local ok, err = normalize_result(self._core:ack())

  if ok ~= nil then
    self._closed = true
  end
  return ok, err
end

function Message:nack(req)
  local ok, err = normalize_result(self._core:nack(req))

  if ok ~= nil then
    self._closed = true
  end
  return ok, err
end

function Message:extend(req)
  return self._core:extend(req)
end

function Message:state()
  local lease = self._core:state()

  if lease == nil then
    return nil
  end
  return wrap_lease(lease)
end

function Message:rewind_payload()
  return self._core:rewind_payload()
end

function Message:payload(dest)
  return self._core:payload(dest)
end

function Message:payload_json()
  local payload, written_or_err = self._core:payload()

  if payload == nil then
    return nil, written_or_err
  end
  return decode_json(payload), written_or_err
end

function Workflow:close()
  if self._core ~= nil and not self._closed then
    self._core:close()
    self._closed = true
  end
end

function Workflow:append_outbox(entry, payload)
  local transaction, receipt_or_err = self._core:append_outbox(
    normalize_outbox_entry(entry), payload)

  if transaction == nil and receipt_or_err == nil then
    return nil
  end
  if transaction == nil and type(receipt_or_err) ~= "table" then
    return nil, receipt_or_err
  end
  if transaction == nil then
    return nil, receipt_or_err
  end
  return wrap_workflow_transaction(transaction), receipt_or_err
end

function Workflow:accept_inbox(message)
  local transaction, result_or_err = self._core:accept_inbox(message)

  if transaction == nil and result_or_err == nil then
    return nil
  end
  if transaction == nil and type(result_or_err) ~= "table" then
    return nil, result_or_err
  end
  if transaction == nil then
    return nil, result_or_err
  end
  return wrap_workflow_transaction(transaction), result_or_err
end

function Workflow:next(timeout_ms)
  local job, err = self._core:next(timeout_ms)

  if job == nil then
    return nil, err
  end
  return wrap_outbox_job(job)
end

function WorkflowTransaction:close()
  if self._core ~= nil and not self._closed then
    self._core:close()
    self._closed = true
  end
end

function WorkflowTransaction:acquire(req)
  local participant, err = self._core:acquire(req)

  if participant == nil then
    return nil, err
  end
  return wrap_workflow_participant(participant)
end

function WorkflowTransaction:append_outbox(entry, payload)
  return self._core:append_outbox(normalize_outbox_entry(entry), payload)
end

function WorkflowTransaction:commit()
  local ok, err = normalize_result(self._core:commit())

  if ok ~= nil then
    self._closed = true
  end
  return ok, err
end

function WorkflowTransaction:rollback()
  local ok, err = normalize_result(self._core:rollback())

  if ok ~= nil then
    self._closed = true
  end
  return ok, err
end

function WorkflowParticipant:info()
  return self._core:info()
end

function WorkflowParticipant:close()
  if self._core ~= nil and not self._closed then
    self._core:close()
    self._closed = true
  end
end

function WorkflowParticipant:describe()
  return self._core:describe()
end

function WorkflowParticipant:get_raw(opts, dest)
  return self._core:get(opts, dest)
end

function WorkflowParticipant:get_json(opts)
  local payload, meta_or_err = self._core:get(opts)

  if payload == nil then
    return nil, meta_or_err
  end
  if meta_or_err ~= nil and meta_or_err.no_content then
    return nil, meta_or_err
  end
  return decode_json(payload), meta_or_err
end

function WorkflowParticipant:update_raw(body, opts)
  return self._core:update(body, opts)
end

function WorkflowParticipant:update_json(value, opts)
  return self:update_raw(encode_json(value), with_json_content_type(opts))
end

function WorkflowParticipant:mutate(req)
  return self._core:mutate(req)
end

function WorkflowParticipant:mutate_local(req)
  return self._core:mutate_local(req)
end

function WorkflowParticipant:metadata(req)
  return self._core:metadata(req)
end

function WorkflowParticipant:remove(req)
  return self._core:remove(req)
end

function WorkflowParticipant:keepalive(req)
  return self._core:keepalive(req)
end

function WorkflowParticipant:attach(req, body)
  return self._core:attach(req, body)
end

function WorkflowParticipant:list_attachments()
  return self._core:list_attachments()
end

function WorkflowParticipant:get_attachment(req, dest)
  return self._core:get_attachment(req, dest)
end

function WorkflowParticipant:delete_attachment(selector)
  return self._core:delete_attachment(selector)
end

function WorkflowParticipant:delete_all_attachments()
  return self._core:delete_all_attachments()
end

function OutboxJob:info()
  return self._core:info()
end

function OutboxJob:close()
  if self._core ~= nil and not self._closed then
    self._core:close()
    self._closed = true
  end
end

function OutboxJob:write_payload(dest)
  return self._core:write_payload(dest)
end

function OutboxJob:payload(dest)
  return self:write_payload(dest)
end

function OutboxJob:payload_json()
  local payload, written_or_err = self:write_payload()

  if payload == nil then
    return nil, written_or_err
  end
  return decode_json(payload), written_or_err
end

function OutboxJob:renew(ttl_seconds)
  return self._core:renew(ttl_seconds)
end

function OutboxJob:complete()
  local ok, err = normalize_result(self._core:complete())

  if ok ~= nil then
    self._closed = true
  end
  return ok, err
end

function OutboxJob:retry(req)
  local ok, err = normalize_result(self._core:retry(req))

  if ok ~= nil then
    self._closed = true
  end
  return ok, err
end

function OutboxJob:dead_letter(diagnostic)
  local ok, err = normalize_result(self._core:dead_letter(diagnostic))

  if ok ~= nil then
    self._closed = true
  end
  return ok, err
end

local function should_continue(err_handler, err)
  if err_handler == nil then
    return false, err
  end
  return err_handler(err) == nil, err
end

local function sleep_seconds(seconds)
  os.execute(string.format("sleep %.3f", seconds))
end

local function run_subscribe(client, req, with_state, handler, should_stop)
  local dequeue_fn

  if with_state then
    dequeue_fn = client.dequeue_with_state
  else
    dequeue_fn = client.dequeue
  end

  while true do
    if should_stop ~= nil and should_stop() then
      return true
    end

    local message, err = dequeue_fn(client, req)
    local ok, handler_err
    local state

    if should_stop ~= nil and should_stop() then
      return true
    end
    if message == nil then
      return nil, err
    end
    state = with_state and message:state() or nil
    ok, handler_err = pcall(handler, message, state)
    if state ~= nil then
      state:close()
      state = nil
    end
    if ok and handler_err == nil and message:is_open() then
      local ack_ok, ack_err = message:ack()

      if ack_ok == nil then
        return nil, ack_err
      end
    elseif not ok then
      if message:is_open() then
        message:nack({ intent = "failure" })
      end
      return nil, handler_err
    elseif message:is_open() then
      message:nack({ intent = "failure" })
      return nil, handler_err
    end
  end
end

function Client:subscribe(req, handler)
  return run_subscribe(self, req, false, handler)
end

function Client:subscribe_with_state(req, handler)
  return run_subscribe(self, req, true, handler)
end

function Client:watch_queue(req, handler)
  local last_signature
  local interval

  interval = tonumber((req or {}).poll_interval_seconds or 1) or 1
  while true do
    local stats, err = self:queue_stats(req)
    local signature

    if stats == nil then
      return nil, err
    end
    signature = table.concat({
      tostring(stats.available),
      stats.head_message_id or "",
    }, "|")
    if signature ~= last_signature then
      local ok, callback_err = pcall(handler, {
        namespace_name = req.namespace_name,
        queue = req.queue,
        available = stats.available,
        head_message_id = stats.head_message_id,
        changed_at_unix = os.time(),
        correlation_id = stats.correlation_id,
      })

      if not ok then
        return nil, callback_err
      end
      last_signature = signature
    end
    sleep_seconds(interval)
  end
end

function Client:new_consumer_service(...)
  return setmetatable({
    _client = self,
    _configs = { ... },
    _stop_requested = false,
  }, Service)
end

function Client:start_consumer(...)
  return self:new_consumer_service(...):run()
end

function Service:stop()
  self._stop_requested = true
  return true
end

function Service:start()
  return self:run()
end

function Service:wait()
  if self._completed or self._stop_requested then
    return true
  end
  return nil, {
    code = core.ERR_INVALID,
    message = "lockdc Lua consumer service wait() only becomes meaningful after start()/run() completes",
  }
end

function Service:run()
  local configs = self._configs
  local i

  if #configs == 0 then
    return nil, {
      code = core.ERR_INVALID,
      message = "lockdc Lua consumer service requires exactly one consumer config",
    }
  end
  if #configs ~= 1 then
    return nil, {
      code = core.ERR_INVALID,
      message = "lockdc Lua consumer service supports exactly one consumer config per blocking service; start separate consumers for separate queues",
    }
  end

  for i = 1, #configs do
    local config = configs[i]
    local req = {}
    local k, v

    for k, v in pairs(config.Options or {}) do
      req[k] = v
    end
    if config.Namespace ~= nil and req.namespace_name == nil then
      req.namespace_name = config.Namespace
    end
    req.queue = config.Queue or req.queue
    req.owner = req.owner or config.Name or config.Queue
    if config.WithState then
      local ok, err = run_subscribe(self._client, req, true, function(message, state)
        return config.MessageHandler(message, state)
      end, function()
        return self._stop_requested
      end)

      if ok == nil then
        return nil, err
      end
    else
      local ok, err = run_subscribe(self._client, req, false, function(message)
        return config.MessageHandler(message)
      end, function()
        return self._stop_requested
      end)

      if ok == nil then
        return nil, err
      end
    end
  end
  self._completed = true
  return true
end

return M

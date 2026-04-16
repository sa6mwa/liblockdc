local core = require("lockdc.core")
local lonejson = require("lonejson")

local M = { core = core }

local JsonEnvelope = lonejson.schema("LockdcJsonEnvelope", {
  lonejson.field("value", lonejson.json_value { required = true }),
})

local Client = {}
Client.__index = Client

local Lease = {}
Lease.__index = Lease

local Message = {}
Message.__index = Message

local Service = {}
Service.__index = Service

local function wrap_client(core_client)
  return setmetatable({ _core = core_client }, Client)
end

local function wrap_lease(core_lease)
  return setmetatable({ _core = core_lease, _closed = false }, Lease)
end

local function wrap_message(core_message)
  return setmetatable({ _core = core_message, _closed = false }, Message)
end

local function normalize_result(a, b)
  if a == nil then
    return nil, b
  end
  return a, b
end

local function encode_json(value)
  local encoded = JsonEnvelope:encode({ value = value })
  local payload = encoded:match('^%{"value":(.*)%}$')

  if payload == nil then
    error("lockdc: failed to encode JSON payload")
  end
  return payload
end

local function decode_json(payload)
  local doc = JsonEnvelope:decode('{"value":' .. payload .. "}")
  return doc.value
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

function Client:acquire(req)
  local lease, err = self._core:acquire(req)

  if lease == nil then
    return nil, err
  end
  return wrap_lease(lease)
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
      stats.correlation_id or "",
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

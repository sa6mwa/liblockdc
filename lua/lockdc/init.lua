local core = require("lockdc.core")
local lonejson = require("lonejson")

local M = {}

local Client = {}
Client.__index = Client

local Lease = {}
Lease.__index = Lease

local Message = {}
Message.__index = Message

local Outbox = {}
Outbox.__index = Outbox

local OutboxDispatcher = {}
OutboxDispatcher.__index = OutboxDispatcher

local OutboxTransaction = {}
OutboxTransaction.__index = OutboxTransaction

local OutboxParticipant = {}
OutboxParticipant.__index = OutboxParticipant

local OutboxJob = {}
OutboxJob.__index = OutboxJob

local HistoryConsumer = {}
HistoryConsumer.__index = HistoryConsumer

local Service = {}
Service.__index = Service

local JSON_NULL = lonejson.json_null
-- Native dispatcher bindings are shared by aliases. Keep an adapter map keyed
-- by the application handler table only after native code has bound it, so
-- aliases pass the same native table without rejected options preserving a
-- stale snapshot of an application handler.
local dispatcher_handler_maps = setmetatable({}, { __mode = "kv" })
local dispatcher_handler_sources = setmetatable({}, { __mode = "kv" })

local function release_dispatcher_handler_map(self)
  local handlers = self._handler_source
  local cached_handlers = self._handler_cache

  self._handler_source = nil
  self._handler_core_map = nil
  self._handler_cache = nil
  if cached_handlers == nil then
    return
  end
  cached_handlers.wrapper_count = cached_handlers.wrapper_count - 1
  if cached_handlers.wrapper_count == 0 then
    if handlers ~= nil and dispatcher_handler_maps[handlers] == cached_handlers then
      dispatcher_handler_maps[handlers] = nil
    end
    dispatcher_handler_sources[cached_handlers] = nil
  end
end

local function wrap_client(core_client)
  return setmetatable({ _core = core_client }, Client)
end

local function wrap_lease(core_lease)
  return setmetatable({ _core = core_lease, _closed = false }, Lease)
end

local function wrap_message(core_message)
  return setmetatable({ _core = core_message, _closed = false }, Message)
end

local function wrap_outbox(core_outbox)
  return setmetatable({ _core = core_outbox, _closed = false }, Outbox)
end

local function wrap_outbox_dispatcher(core_dispatcher)
  return setmetatable({ _core = core_dispatcher, _closed = false },
    OutboxDispatcher)
end

local function wrap_outbox_transaction(core_transaction)
  return setmetatable({
    _core = core_transaction,
    _closed = false,
    _terminal = false,
  }, OutboxTransaction)
end

local function wrap_outbox_participant(core_participant)
  return setmetatable({ _core = core_participant, _closed = false }, OutboxParticipant)
end

local function wrap_outbox_job(core_job)
  return setmetatable({
    _core = core_job,
    _closed = false,
    _terminal = false,
  }, OutboxJob)
end

local function wrap_history_consumer(core_consumer)
  return setmetatable({ _core = core_consumer, _closed = false }, HistoryConsumer)
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

local function require_sink(sink, method, materializer)
  if sink == nil then
    error(method .. " requires a sink; use " .. materializer .. " to materialize")
  end
  return sink
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

function M.xid_new()
  return core.xid_new()
end

function M.pouch_crypto_generate_key()
  return core.pouch_crypto_generate_key()
end

function M.pouch_crypto_default_key_file()
  return core.pouch_crypto_default_key_file()
end

function M.pouch_crypto_generate_key_file(path, overwrite)
  return core.pouch_crypto_generate_key_file(path, overwrite)
end

M.json_null = JSON_NULL
M.OK = core.OK
M.ERR_INVALID = core.ERR_INVALID
M.ERR_NOMEM = core.ERR_NOMEM
M.ERR_TRANSPORT = core.ERR_TRANSPORT
M.ERR_PROTOCOL = core.ERR_PROTOCOL
M.ERR_SERVER = core.ERR_SERVER
M.ERR_TIMEOUT = core.ERR_TIMEOUT
M.NACK_FAILURE = core.NACK_FAILURE
M.NACK_DEFER = core.NACK_DEFER

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

function Client:new_outbox(config, options)
  local core_options = options

  if options ~= nil and options.dispatcher ~= nil then
    core_options = { dispatcher = options.dispatcher._core }
  end
  local outbox, err = self._core:new_outbox(config, core_options)

  if outbox == nil then
    return nil, err
  end
  return wrap_outbox(outbox)
end

function Client:new_history_consumer(config)
  local consumer, err = self._core:new_history_consumer(config)

  if consumer == nil then
    return nil, err
  end
  return wrap_history_consumer(consumer)
end

function HistoryConsumer:position()
  return self._core:position()
end

function HistoryConsumer:advance(acknowledged_index_seq)
  return self._core:advance(acknowledged_index_seq)
end

function HistoryConsumer:unregister()
  return self._core:unregister()
end

function HistoryConsumer:close()
  if self._core ~= nil then
    self._core:close()
    self._core = nil
    self._closed = true
  end
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

    function af:update(body, opts)
      return lease:update(body, opts)
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

function Client:get(req, sink)
  return self._core:get(req, require_sink(sink, "client:get", "client:read"))
end

function Client:read(req)
  return self._core:get(req)
end

function Client:read_json(req)
  local payload, meta_or_err = self:read(req)

  if payload == nil then
    return nil, meta_or_err
  end
  if meta_or_err ~= nil and meta_or_err.no_content then
    return nil, meta_or_err
  end
  return decode_json(payload), meta_or_err
end

function Client:update(req, body)
  return self._core:update(flatten_lease_request(req), body)
end

function Client:update_json(req, value)
  return self:update(with_json_content_type(req), encode_json(value))
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

function Client:get_attachment(req, sink)
  return self._core:get_attachment(flatten_lease_request(req),
    require_sink(sink, "client:get_attachment", "client:read_attachment"))
end

function Client:read_attachment(req)
  return self._core:get_attachment(flatten_lease_request(req))
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

function Client:query(req, sink)
  return self._core:query(req,
    require_sink(sink, "client:query", "client:read_query"))
end

function Client:read_query(req)
  return self._core:query(req)
end

function Client:query_keys(req, handler)
  return self._core:query_keys(req, handler)
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

function Client:txn_replay(req)
  return self._core:txn_replay(req)
end

function Client:txn_prepare(req)
  return self._core:txn_prepare(req)
end

function Client:txn_commit(req)
  return self._core:txn_commit(req)
end

function Client:txn_rollback(req)
  return self._core:txn_rollback(req)
end

function Client:tc_lease_acquire(req)
  return self._core:tc_lease_acquire(req)
end

function Client:tc_lease_renew(req)
  return self._core:tc_lease_renew(req)
end

function Client:tc_lease_release(req)
  return self._core:tc_lease_release(req)
end

function Client:tc_leader()
  return self._core:tc_leader()
end

function Client:tc_cluster_announce(req)
  return self._core:tc_cluster_announce(req)
end

function Client:tc_cluster_leave()
  return self._core:tc_cluster_leave()
end

function Client:tc_cluster_list()
  return self._core:tc_cluster_list()
end

function Client:tc_rm_register(req)
  return self._core:tc_rm_register(req)
end

function Client:tc_rm_unregister(req)
  return self._core:tc_rm_unregister(req)
end

function Client:tc_rm_list()
  return self._core:tc_rm_list()
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

function Lease:get(opts, sink)
  return self._core:get(opts, require_sink(sink, "lease:get", "lease:read"))
end

function Lease:read(opts)
  return self._core:get(opts)
end

function Lease:read_json(opts)
  local payload, meta_or_err = self:read(opts)

  if payload == nil then
    return nil, meta_or_err
  end
  if meta_or_err ~= nil and meta_or_err.no_content then
    return nil, meta_or_err
  end
  return decode_json(payload), meta_or_err
end

function Lease:update(body, req)
  return self._core:update(body, req)
end

function Lease:update_json(value, req)
  return self:update(encode_json(value), with_json_content_type(req))
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

function Lease:get_attachment(req, sink)
  return self._core:get_attachment(req,
    require_sink(sink, "lease:get_attachment", "lease:read_attachment"))
end

function Lease:read_attachment(req)
  return self._core:get_attachment(req)
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

function Message:write_payload(sink)
  return self._core:payload(
    require_sink(sink, "message:write_payload", "message:read_payload"))
end

function Message:read_payload()
  return self._core:payload()
end

function Message:read_payload_json()
  local payload, written_or_err = self:read_payload()

  if payload == nil then
    return nil, written_or_err
  end
  return decode_json(payload), written_or_err
end

function Outbox:close()
  if self._core ~= nil and not self._closed then
    self._core:close()
    self._closed = true
  end
end

function Outbox:begin()
  local transaction, err = self._core:begin()

  if transaction == nil then
    return nil, err
  end
  return wrap_outbox_transaction(transaction)
end

function Outbox:transaction(fn)
  return self._core:transaction(function(transaction)
    return fn(wrap_outbox_transaction(transaction))
  end)
end

function Outbox:dispatcher()
  local dispatcher, err = self._core:dispatcher()

  if dispatcher == nil then
    return nil, err
  end
  return wrap_outbox_dispatcher(dispatcher)
end

function Outbox:append(entry, payload)
  local transaction, receipt_or_err = self._core:append(entry, payload)

  if transaction == nil and receipt_or_err == nil then
    return nil
  end
  if transaction == nil and type(receipt_or_err) ~= "table" then
    return nil, receipt_or_err
  end
  if transaction == nil then
    return nil, receipt_or_err
  end
  return wrap_outbox_transaction(transaction), receipt_or_err
end

function Outbox:accept_inbox(message)
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
  return wrap_outbox_transaction(transaction), result_or_err
end

function Outbox:accept_command(request)
  local transaction, receipt_or_err = self._core:accept_command(request)

  if transaction == nil and receipt_or_err == nil then
    return nil
  end
  if transaction == nil and type(receipt_or_err) ~= "table" then
    return nil, receipt_or_err
  end
  if transaction == nil then
    return nil, receipt_or_err
  end
  return wrap_outbox_transaction(transaction), receipt_or_err
end

function Outbox:get_command_receipt(identity)
  return self._core:get_command_receipt(identity)
end

function Outbox:write_command_result(identity, sink)
  return self._core:write_command_result(identity, require_sink(sink,
    "outbox:write_command_result", "outbox:read_command_result"))
end

function Outbox:read_command_result(identity)
  return self._core:write_command_result(identity)
end

function Outbox:resume_command(identity)
  local transaction, receipt_or_err = self._core:resume_command(identity)

  if transaction == nil and receipt_or_err == nil then
    return nil
  end
  if transaction == nil and type(receipt_or_err) ~= "table" then
    return nil, receipt_or_err
  end
  if transaction == nil then
    return nil, receipt_or_err
  end
  return wrap_outbox_transaction(transaction), receipt_or_err
end

function OutboxDispatcher:close()
  if self._core ~= nil and not self._closed then
    self._core:close()
    self._closed = true
  end
  -- A Lua handler binding lasts while at least one dispatcher wrapper is
  -- reachable. This wrapper no longer needs to retain its adapter map once it
  -- has surrendered its receiver reference.
  release_dispatcher_handler_map(self)
end

function OutboxDispatcher:next(timeout_ms)
  local job, err = self._core:next(timeout_ms)

  if job == nil then
    return nil, err
  end
  return wrap_outbox_job(job)
end

local function dispatcher_handler_options(self, options)
  local handlers
  local key, handler
  local handler_count
  local core_options
  local core_handlers
  local cached_handlers
  local activate_map
  local retain_map

  if type(options) ~= "table" then
    return options
  end
  handlers = options.handlers
  if type(handlers) ~= "table" then
    return options
  end
  handler_count = 0
  for key, handler in pairs(handlers) do
    if type(key) ~= "string" or key == "" or key:find("\0", 1, true) ~= nil or
        type(handler) ~= "function" then
      -- Let the native boundary report the standard structured argument error.
      return options
    end
    handler_count = handler_count + 1
  end
  if handler_count == 0 then
    return options
  end
  cached_handlers = dispatcher_handler_maps[handlers]
  activate_map = function()
    local source = dispatcher_handler_sources[cached_handlers]
    if source ~= nil then
      dispatcher_handler_maps[source] = cached_handlers
    end
  end
  retain_map = function()
    activate_map()
    if self._closed then
      return
    end
    if self._handler_cache ~= cached_handlers then
      release_dispatcher_handler_map(self)
      cached_handlers.wrapper_count = cached_handlers.wrapper_count + 1
    end
    self._handler_source = handlers
    self._handler_core_map = cached_handlers.core_handlers
    self._handler_cache = cached_handlers
  end
  if cached_handlers == nil then
    core_handlers = {}
    for key, handler in pairs(handlers) do
      local handler_function = handler

      core_handlers[key] = function(core_job)
        -- This wrapper is entered only after native code has accepted this
        -- exact map. Promote before application code can recurse through an
        -- alias while the first pump/run invocation remains active.
        activate_map()
        return handler_function(wrap_outbox_job(core_job))
      end
    end
    cached_handlers = { core_handlers = core_handlers, wrapper_count = 0 }
  end
  dispatcher_handler_sources[cached_handlers] = handlers
  core_options = {}
  for key, handler in pairs(options) do
    core_options[key] = handler
  end
  core_options.handlers = cached_handlers.core_handlers
  -- The native layer changes this only after it has accepted this exact
  -- adapter as the dispatcher's immutable handler map.
  core_options._lockdc_facade_handlers_bound = false
  return core_options, retain_map
end

local function dispatcher_handler_call(self, options, method)
  local core_options
  local retain_map
  local result
  local error_result
  local status

  core_options, retain_map = dispatcher_handler_options(self, options)
  if retain_map == nil then
    return method(self._core, core_options)
  end
  result, error_result, status = method(self._core, core_options)
  if core_options._lockdc_facade_handlers_bound then
    retain_map()
  end
  return result, error_result, status
end

function OutboxDispatcher:pump(options)
  return dispatcher_handler_call(self, options, self._core.pump)
end

function OutboxDispatcher:run(options)
  return dispatcher_handler_call(self, options, self._core.run)
end

function OutboxDispatcher:notify_outbox_key(outbox_key)
  return self._core:notify_outbox_key(outbox_key)
end

function OutboxDispatcher:stats()
  return self._core:stats()
end

function OutboxDispatcher:reconcile()
  return self._core:reconcile()
end

function OutboxDispatcher:replay_dead_letter(outbox_key)
  return self._core:replay_dead_letter(outbox_key)
end

function OutboxDispatcher:delete_dead_letter(outbox_key)
  return self._core:delete_dead_letter(outbox_key)
end

function OutboxDispatcher:export_dead_letters(options, sink)
  return self._core:export_dead_letters(options, require_sink(sink,
    "dispatcher:export_dead_letters", "dispatcher:read_dead_letters"))
end

function OutboxDispatcher:read_dead_letters(options)
  return self._core:export_dead_letters(options)
end

function OutboxDispatcher:stop(deadline_ms)
  return self._core:stop(deadline_ms)
end

function OutboxDispatcher:wait(deadline_ms)
  return self._core:wait(deadline_ms)
end

function OutboxTransaction:close()
  if self._core ~= nil and not self._closed then
    self._core:close()
    self._closed = true
  end
end

function OutboxTransaction:acquire(req)
  local participant, err = self._core:acquire(req)

  if participant == nil then
    return nil, err
  end
  return wrap_outbox_participant(participant)
end

function OutboxTransaction:append(entry, payload)
  return self._core:append(entry, payload)
end

function OutboxTransaction:accept_command(request)
  return self._core:accept_command(request)
end

function OutboxTransaction:accept_inbox(message)
  return self._core:accept_inbox(message)
end

function OutboxTransaction:complete_command(result)
  return self._core:complete_command(result)
end

function OutboxTransaction:fail_command(result)
  return self._core:fail_command(result)
end

function OutboxTransaction:commit()
  local ok, err = normalize_result(self._core:commit())

  if ok ~= nil then
    self._terminal = true
  end
  return ok, err
end

function OutboxTransaction:rollback()
  local ok, err = normalize_result(self._core:rollback())

  if ok ~= nil then
    self._terminal = true
  end
  return ok, err
end

function OutboxParticipant:info()
  return self._core:info()
end

function OutboxParticipant:close()
  if self._core ~= nil and not self._closed then
    self._core:close()
    self._closed = true
  end
end

function OutboxParticipant:describe()
  return self._core:describe()
end

function OutboxParticipant:get(opts, sink)
  return self._core:get(opts, require_sink(sink,
    "outbox participant:get", "outbox participant:read"))
end

function OutboxParticipant:read(opts)
  return self._core:get(opts)
end

function OutboxParticipant:read_json(opts)
  local payload, meta_or_err = self:read(opts)

  if payload == nil then
    return nil, meta_or_err
  end
  if meta_or_err ~= nil and meta_or_err.no_content then
    return nil, meta_or_err
  end
  return decode_json(payload), meta_or_err
end

function OutboxParticipant:update(body, opts)
  return self._core:update(body, opts)
end

function OutboxParticipant:update_json(value, opts)
  return self:update(encode_json(value), with_json_content_type(opts))
end

function OutboxParticipant:mutate(req)
  return self._core:mutate(req)
end

function OutboxParticipant:mutate_local(req)
  return self._core:mutate_local(req)
end

function OutboxParticipant:metadata(req)
  return self._core:metadata(req)
end

function OutboxParticipant:remove(req)
  return self._core:remove(req)
end

function OutboxParticipant:keepalive(req)
  return self._core:keepalive(req)
end

function OutboxParticipant:attach(req, body)
  return self._core:attach(req, body)
end

function OutboxParticipant:list_attachments()
  return self._core:list_attachments()
end

function OutboxParticipant:get_attachment(req, sink)
  return self._core:get_attachment(req, require_sink(sink,
    "outbox participant:get_attachment", "outbox participant:read_attachment"))
end

function OutboxParticipant:read_attachment(req)
  return self._core:get_attachment(req)
end

function OutboxParticipant:delete_attachment(selector)
  return self._core:delete_attachment(selector)
end

function OutboxParticipant:delete_all_attachments()
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

function OutboxJob:write_payload(sink)
  return self._core:write_payload(require_sink(sink,
    "outbox job:write_payload", "outbox job:read_payload"))
end

function OutboxJob:read_payload()
  return self._core:write_payload()
end

function OutboxJob:read_payload_json()
  local payload, written_or_err = self:read_payload()

  if payload == nil then
    return nil, written_or_err
  end
  return decode_json(payload), written_or_err
end

function OutboxJob:renew(ttl_seconds)
  return self._core:renew(ttl_seconds)
end

function OutboxJob:complete(completion)
  local ok, err = normalize_result(self._core:complete(completion))

  if ok ~= nil then
    self._terminal = true
    self._closed = true
  end
  return ok, err
end

function OutboxJob:retry(req)
  local ok, err = normalize_result(self._core:retry(req))

  if ok ~= nil then
    self._terminal = true
    self._closed = true
  end
  return ok, err
end

function OutboxJob:dead_letter(diagnostic)
  local ok, err = normalize_result(self._core:dead_letter(diagnostic))

  if ok ~= nil then
    self._terminal = true
    self._closed = true
  end
  return ok, err
end

local function run_threadless_service(client, req, with_state, handler, should_stop)
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
    local ok, handler_result, handler_err
    local state

    if should_stop ~= nil and should_stop() then
      return true
    end
    if message == nil then
      return nil, err
    end
    state = with_state and message:state() or nil
    ok, handler_result, handler_err = pcall(handler, message, state)
    if state ~= nil then
      state:close()
      state = nil
    end
    if ok and (handler_result == nil and handler_err == nil or
        handler_result == true) then
      if message:is_open() then
        local ack_ok, ack_err = message:ack()

        if ack_ok == nil then
          return nil, ack_err
        end
      end
    else
      local failure = handler_err or handler_result

      if not ok then
        failure = handler_result
      end
      if failure == nil or failure == false then
        failure = {
          code = core.ERR_INVALID,
          message = "Lua consumer handler returned false",
        }
      elseif type(failure) ~= "table" then
        failure = {
          code = core.ERR_INVALID,
          message = tostring(failure),
        }
      end
      if message:is_open() then
        local nack_ok, nack_err = message:nack({ intent = "failure" })

        if nack_ok == nil then
          return nil, nack_err
        end
      end
      return nil, failure
    end
  end
end

function Client:subscribe(req, handler)
  return self._core:subscribe(req, handler)
end

function Client:subscribe_with_state(req, handler)
  return self._core:subscribe_with_state(req, handler)
end

function Client:watch_queue(req, handler)
  return self._core:watch_queue(req, handler)
end

function Client:new_consumer_service(config)
  return setmetatable({
    _client = self,
    _config = config,
    _stop_requested = false,
  }, Service)
end

function Service:stop()
  self._stop_requested = true
  return true
end

function Service:wait()
  if self._completed or self._stop_requested then
    return true
  end
  return nil, {
    code = core.ERR_INVALID,
    message = "lockdc Lua consumer service wait() only becomes meaningful after run() completes",
  }
end

function Service:run()
  local config = self._config
  local req = {}
  local k, v

  if type(config) ~= "table" then
    return nil, {
      code = core.ERR_INVALID,
      message = "lockdc Lua consumer service requires one config table",
    }
  end
  if type(config.request) ~= "table" then
    return nil, {
      code = core.ERR_INVALID,
      message = "lockdc Lua consumer service config requires request",
    }
  end
  if type(config.handle) ~= "function" then
    return nil, {
      code = core.ERR_INVALID,
      message = "lockdc Lua consumer service config requires handle",
    }
  end
  for k, v in pairs(config.request) do
    req[k] = v
  end
  req.owner = req.owner or config.name or req.queue
  local ok, err = run_threadless_service(self._client, req,
    config.with_state == true, config.handle, function()
      return self._stop_requested
    end)
  if ok == nil then
    self._completed = true
    return nil, err
  end
  self._completed = true
  return true
end

function Service:close()
  self:stop()
  self._completed = true
end

return M

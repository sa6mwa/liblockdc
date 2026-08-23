local repo_root = assert(os.getenv('LOCKDC_ROOT'), 'LOCKDC_ROOT is required')

package.path = table.concat({
  repo_root .. '/lua/?.lua',
  repo_root .. '/lua/?/init.lua',
  package.path,
}, ';')

local function make_lonejson_stub()
  local M = {
    json_null = setmetatable({}, {
      __tostring = function()
        return 'lonejson.json_null'
      end,
    }),
  }

  function M.encode_json(value)
    if value == M.json_null then
      return 'null'
    end
    return tostring(value)
  end

  function M.decode_json(text)
    if text == 'null' then
      return M.json_null
    end
    return text
  end

  return M
end

package.preload['lonejson'] = function()
  return make_lonejson_stub()
end

local core_stub = {
  ERR_INVALID = 42,
  version_string = function()
    return 'test-version'
  end,
}

package.preload['lockdc.core'] = function()
  return core_stub
end

local lockdc = require('lockdc')

local function assert_eq(actual, expected, message)
  if actual ~= expected then
    error((message or 'assert_eq failed') .. string.format(' (expected %s, got %s)', tostring(expected), tostring(actual)))
  end
end

local function assert_truthy(value, message)
  if not value then
    error(message or 'expected truthy value')
  end
end

local function test_json_helpers()
  assert_eq(lockdc.version_string(), 'test-version', 'version_string should delegate to core')
  assert_eq(lockdc.encode_json('123'), '123', 'encode_json should strip wrapper envelope')
  assert_eq(lockdc.encode_json(nil), 'null', 'encode_json should preserve legacy top-level nil null')
  assert_eq(lockdc.decode_json('{"k":1}'), '{"k":1}', 'decode_json should unwrap envelope payload')
  assert_eq(lockdc.decode_json('null'), lockdc.json_null, 'decode_json should preserve top-level JSON null')
end

local function test_request_flattening_and_default_content_type()
  local lease_info = {
    namespace_name = 'default',
    key = 'lease-key',
    owner = 'lease-owner',
  }
  local captured = {}
  local message_core
  local lease_core = {
    info = function()
      return lease_info
    end,
    close = function() end,
  }
  local client_core = {
    acquire = function(_, req)
      captured.acquire_req = req
      return lease_core
    end,
    update = function(_, req, body)
      captured.update_req = req
      captured.update_body = body
      return true
    end,
    queue_nack = function(_, req)
      captured.queue_nack_req = req
      return true
    end,
    queue_ack = function(_, arg)
      captured.queue_ack_arg = arg
      return true
    end,
    dequeue = function()
      return message_core
    end,
    close = function() end,
  }

  core_stub.open = function(config)
    captured.open_config = config
    return client_core
  end

  local client = assert(lockdc.open({ endpoints = { 'https://example.test' } }))
  local lease = assert(client:acquire({ key = 'lease-key', owner = 'lease-owner' }))

  assert_eq(captured.acquire_req.key, 'lease-key', 'acquire should pass request through')

  local ok = client:update_json({ lease = lease, if_match = 'etag-1' }, '17')
  assert_truthy(ok, 'update_json should return underlying success')
  assert_eq(captured.update_body, '17', 'update_json should encode JSON body')
  assert_eq(captured.update_req.namespace_name, 'default', 'update_json should flatten lease namespace')
  assert_eq(captured.update_req.key, 'lease-key', 'update_json should flatten lease key')
  assert_eq(captured.update_req.owner, 'lease-owner', 'update_json should flatten lease owner')
  assert_eq(captured.update_req.if_match, 'etag-1', 'update_json should preserve explicit request fields')
  assert_eq(captured.update_req.content_type, 'application/json', 'update_json should default content_type')
  assert_eq(captured.update_req.lease, nil, 'update_json should remove nested lease object')

  message_core = {
    info = function()
      return {
        namespace_name = 'default',
        queue = 'jobs',
        message_id = 'msg-1',
      }
    end,
    close = function() end,
  }
  local wrapped_message = assert(client:dequeue({ queue = 'jobs' }))

  client:queue_nack({ message = wrapped_message, intent = 'failure' })
  assert_eq(captured.queue_nack_req.namespace_name, 'default', 'queue_nack should flatten message namespace')
  assert_eq(captured.queue_nack_req.queue, 'jobs', 'queue_nack should flatten message queue')
  assert_eq(captured.queue_nack_req.message_id, 'msg-1', 'queue_nack should flatten message id')
  assert_eq(captured.queue_nack_req.intent, 'failure', 'queue_nack should preserve explicit intent')
  assert_eq(captured.queue_nack_req.message, nil, 'queue_nack should remove nested message object')

  client:queue_ack(wrapped_message)
  assert_eq(captured.queue_ack_arg, message_core, 'queue_ack should unwrap wrapped message')

  client:close()
end

local function test_pouch_open_config_passthrough()
  local captured = {}
  local client_core = {
    close = function() end,
  }

  core_stub.open = function(config)
    captured.config = config
    return client_core
  end

  local client = assert(lockdc.open({
    endpoints = { 'pouch:///var/lib/lockdc-lua-unit' },
    pouch_crypto_key = 'lc-pouch-key-v1:test-key',
    pouch_crypto_key_file = '/var/lib/lockdc-lua-unit/root.key',
    pouch_crypto_generate_key_file = true,
    pouch_compression = 'zlib',
  }))

  assert_eq(captured.config.endpoints[1], 'pouch:///var/lib/lockdc-lua-unit', 'pouch endpoint should pass through')
  assert_eq(captured.config.pouch_crypto_key, 'lc-pouch-key-v1:test-key', 'pouch_crypto_key should pass through')
  assert_eq(captured.config.pouch_crypto_key_file, '/var/lib/lockdc-lua-unit/root.key', 'pouch_crypto_key_file should pass through')
  assert_eq(captured.config.pouch_crypto_generate_key_file, true, 'pouch key-file generation should pass through')
  assert_eq(captured.config.pouch_compression, 'zlib', 'pouch compression should pass through')
  client:close()
end

local function test_subscribe_ack_and_error_paths()
  local function new_message()
    local msg = {
      closed = false,
      ack_count = 0,
      nack_count = 0,
    }

    function msg:ack()
      self.ack_count = self.ack_count + 1
      self.closed = true
      return true
    end

    function msg:nack(req)
      self.nack_count = self.nack_count + 1
      self.last_nack_req = req
      self.closed = true
      return true
    end

    function msg:close()
      self.closed = true
    end

    function msg:state()
      return nil
    end

    return msg
  end

  local function open_client_for_messages(messages)
    local client_core = {
      close = function() end,
    }

    client_core.dequeue = function()
      local next_value = table.remove(messages, 1)
      if next_value == nil then
        return nil, { message = 'queue drained' }
      end
      return next_value
    end

    core_stub.open = function()
      return client_core
    end

    return assert(lockdc.open({}))
  end

  local success_message = new_message()
  local client = open_client_for_messages({ success_message })
  local ok, err = client:subscribe({ queue = 'jobs' }, function(message)
    assert_truthy(message:is_open(), 'message should be open before implicit ack')
    return nil
  end)
  assert_eq(ok, nil, 'subscribe should stop on empty queue after success')
  assert_eq(err.message, 'queue drained', 'subscribe should surface empty queue after draining in this unit stub')
  assert_eq(success_message.ack_count, 1, 'successful handler should trigger implicit ack')
  assert_eq(success_message.nack_count, 0, 'successful handler should not nack')

  local explicit_ack_message = new_message()
  client = open_client_for_messages({ explicit_ack_message })
  ok, err = client:subscribe({ queue = 'jobs' }, function(message)
    local ack_ok = message:ack()
    assert_truthy(ack_ok, 'explicit ack inside handler should succeed')
    return nil
  end)
  assert_eq(ok, nil, 'subscribe should stop on empty queue after explicit ack success')
  assert_eq(err.message, 'queue drained', 'explicit ack path should drain queue in this unit stub')
  assert_eq(explicit_ack_message.ack_count, 1, 'explicit ack should not be repeated implicitly')
  assert_eq(explicit_ack_message.nack_count, 0, 'explicit ack success should not nack')

  local failure_message = new_message()
  client = open_client_for_messages({ failure_message })
  ok, err = client:subscribe({ queue = 'jobs' }, function(_message)
    error('handler exploded')
  end)
  assert_eq(ok, nil, 'handler exception should fail subscribe')
  assert_truthy(type(err) == 'string' and err:match('handler exploded'), 'handler exception should surface pcall error string')
  assert_eq(failure_message.ack_count, 0, 'failed handler should not ack')
  assert_eq(failure_message.nack_count, 1, 'failed handler should nack once')
  assert_eq(failure_message.last_nack_req.intent, 'failure', 'failed handler should nack with failure intent')
end

local function test_acquire_for_update_propagates_sdk_failure_shape()
  local expected_err = { message = 'update failed' }
  local lease_core = {
    closed = false,
    update = function()
      return nil, expected_err
    end,
    close = function(self)
      self.closed = true
    end,
  }
  local client_core = {}

  client_core.acquire_for_update = function(_, _req, handler)
    local result, handler_err = handler({
      lease = lease_core,
      state = '{"value":1}',
      state_meta = {
        has_state = true,
        no_content = false,
      },
    })
    if handler_err ~= nil then
      return nil, handler_err
    end
    if result == false or type(result) == 'string' then
      return nil, result
    end
    return true
  end

  client_core.close = function() end
  core_stub.open = function()
    return client_core
  end

  local client = assert(lockdc.open({}))
  local ok, err = client:acquire_for_update({
    key = 'state-key',
    owner = 'worker-1',
  }, function(af)
    return af:update_json({ value = 2 })
  end)

  assert_eq(ok, nil, 'acquire_for_update should fail when handler returns nil, err')
  assert_eq(err, expected_err, 'acquire_for_update should preserve the SDK error table')
  assert_truthy(lease_core.closed, 'acquire_for_update should close the facade lease after handler return')
end

local function test_subscribe_with_state_and_service_lifecycle()
  local state_lease = {
    closed = false,
    info = function()
      return { namespace_name = 'default', key = 'state-key' }
    end,
    close = function(self)
      self.closed = true
    end,
  }
  local message = {
    closed = false,
    ack_count = 0,
  }
  function message:ack()
    self.ack_count = self.ack_count + 1
    self.closed = true
    return true
  end
  function message:nack(req)
    self.closed = true
    self.last_nack_req = req
    return true
  end
  function message:close()
    self.closed = true
  end
  function message:state()
    return state_lease
  end

  local dequeue_calls = 0
  local client_core = {
    dequeue_with_state = function()
      dequeue_calls = dequeue_calls + 1
      if dequeue_calls == 1 then
        return message
      end
      return nil, { message = 'unexpected extra dequeue' }
    end,
    close = function() end,
  }
  core_stub.open = function()
    return client_core
  end

  local client = assert(lockdc.open({}))
  local seen_state
  local ok, err = client:subscribe_with_state({ queue = 'jobs' }, function(_message, state)
    seen_state = state
    return nil
  end)
  assert_eq(ok, nil, 'subscribe_with_state should stop on empty queue in this unit stub')
  assert_eq(err.message, 'unexpected extra dequeue', 'subscribe_with_state should surface empty queue after draining in this unit stub')
  assert_truthy(seen_state ~= nil, 'subscribe_with_state should pass wrapped state lease')
  assert_eq(seen_state:info().key, 'state-key', 'wrapped state lease should expose info')
  assert_eq(message.ack_count, 1, 'subscribe_with_state success should ack message')

  local service_message = {
    closed = false,
    ack_count = 0,
  }
  function service_message:ack()
    self.ack_count = self.ack_count + 1
    self.closed = true
    return true
  end
  function service_message:nack(req)
    self.closed = true
    self.last_nack_req = req
    return true
  end
  function service_message:close()
    self.closed = true
  end
  function service_message:state()
    return nil
  end

  local service_dequeues = 0
  client_core.dequeue = function()
    service_dequeues = service_dequeues + 1
    if service_dequeues == 1 then
      return service_message
    end
    return nil, { message = 'service should stop before another dequeue' }
  end

  local service
  service = client:new_consumer_service({
    Name = 'worker-1',
    Queue = 'jobs',
    Options = {
      namespace_name = 'default',
    },
    MessageHandler = function(msg)
      assert_truthy(msg:is_open(), 'service handler should receive open message')
      service:stop()
      return nil
    end,
  })

  local wait_ok, wait_err = service:wait()
  assert_eq(wait_ok, nil, 'wait before run should fail')
  assert_eq(wait_err.code, core_stub.ERR_INVALID, 'wait before run should return ERR_INVALID')

  ok, err = service:start()
  assert_truthy(ok, 'service:start should alias run and succeed')
  assert_eq(err, nil, 'service:start success should not return error')
  assert_eq(service_message.ack_count, 1, 'service handler success should ack message')
  assert_truthy(service:wait(), 'wait after completed run should succeed')

  local multi_service = client:new_consumer_service({
    Name = 'worker-1',
    Queue = 'jobs-a',
    MessageHandler = function()
      error('first multi-config handler should not run')
    end,
  }, {
    Name = 'worker-2',
    Queue = 'jobs-b',
    MessageHandler = function()
      error('second multi-config handler should not run')
    end,
  })

  ok, err = multi_service:start()
  assert_eq(ok, nil, 'service:start should reject multiple blocking consumer configs')
  assert_eq(err.code, core_stub.ERR_INVALID, 'multiple consumer configs should return ERR_INVALID')
  assert_truthy(
    err.message:match('exactly one consumer config'),
    'multiple consumer configs should return actionable error'
  )
end

local function test_watch_queue_change_detection()
  local queue_stats_plan = {
    { available = 0, head_message_id = 'a', correlation_id = 'one' },
    { available = 0, head_message_id = 'a', correlation_id = 'two' },
    { available = 1, head_message_id = 'b', correlation_id = 'two' },
  }
  local captured_events = {}
  local sleep_calls = 0
  local original_execute = os.execute
  local client_core = {
    queue_stats = function()
      local next_stats = table.remove(queue_stats_plan, 1)
      if next_stats == nil then
        return nil, { message = 'done' }
      end
      return next_stats
    end,
    close = function() end,
  }
  core_stub.open = function()
    return client_core
  end

  os.execute = function(cmd)
    sleep_calls = sleep_calls + 1
    assert_truthy(cmd:match('^sleep '), 'watch_queue should sleep between polls')
    return true
  end

  local client = assert(lockdc.open({}))
  local ok, err = client:watch_queue({
    namespace_name = 'default',
    queue = 'jobs',
    poll_interval_seconds = 0.01,
  }, function(event)
    captured_events[#captured_events + 1] = event
    if #captured_events == 2 then
      error('stop after second event')
    end
  end)

  os.execute = original_execute

  assert_eq(ok, nil, 'watch_queue callback error should fail watch')
  assert_truthy(type(err) == 'string' and err:match('stop after second event'), 'watch_queue should surface callback failure')
  assert_eq(#captured_events, 2, 'watch_queue should emit only when queue signature changes')
  assert_eq(captured_events[1].available, 0, 'watch_queue should emit initial state')
  assert_eq(captured_events[2].available, 1, 'watch_queue should emit changed state')
  assert_truthy(sleep_calls >= 1, 'watch_queue should sleep between polls')
end

local function test_json_null_roundtrip_helpers()
  local client_core = {
    get = function()
      return 'null', { etag = 'etag-1' }
    end,
    dequeue = function()
      return {
        info = function()
          return {
            namespace_name = 'default',
            queue = 'jobs',
            message_id = 'msg-1',
          }
        end,
        payload = function()
          return 'null', 4
        end,
        close = function() end,
      }
    end,
    close = function() end,
  }

  core_stub.open = function()
    return client_core
  end

  local client = assert(lockdc.open({}))
  local value, meta = client:get_json({ key = 'state-key' })
  assert_eq(value, lockdc.json_null, 'client:get_json should preserve top-level JSON null')
  assert_eq(meta.etag, 'etag-1', 'client:get_json should still return metadata')

  local message = assert(client:dequeue({ queue = 'jobs' }))
  local payload, written = message:payload_json()
  assert_eq(payload, lockdc.json_null, 'message:payload_json should preserve top-level JSON null')
  assert_eq(written, 4, 'message:payload_json should preserve byte count')
end

local function test_workflow_facade_lifecycle()
  local captured = {}
  local participant_core = {
    info = function()
      return { key = 'order-1', txn_id = 'endpoint-minted-xid' }
    end,
    update = function(_, body, opts)
      captured.update_body = body
      captured.update_opts = opts
      return { version = 2 }
    end,
    get = function()
      return 'null', { no_content = false }
    end,
    metadata = function(_, req)
      captured.metadata_req = req
      return { version = 2 }
    end,
    mutate = function(_, req)
      captured.mutate_req = req
      return { version = 3 }
    end,
    mutate_local = function(_, req)
      captured.mutate_local_req = req
      return { version = 4 }
    end,
    remove = function() return true end,
    keepalive = function() return true end,
    attach = function() return { attachment = { name = 'body' } } end,
    list_attachments = function()
      return { items = { { name = 'body' } } }
    end,
    get_attachment = function() return 'attachment', {} end,
    delete_attachment = function(_, selector)
      captured.delete_attachment_selector = selector
      return true
    end,
    delete_all_attachments = function()
      return 1
    end,
    describe = function() return { version = 2 } end,
    close = function(self) self.closed = true end,
  }
  local transaction_core = {
    acquire = function(_, req)
      captured.acquire_req = req
      return participant_core
    end,
    append_outbox = function(_, entry, payload)
      captured.later_entry = entry
      captured.later_payload = payload
      return { outbox_key = 'later-key', duplicate = false }
    end,
    commit = function(self)
      self.committed = true
      return true
    end,
    rollback = function(self)
      self.rolled_back = true
      return true
    end,
    close = function(self) self.closed = true end,
  }
  local job_core = {
    info = function()
      return { effect_key = 'charge:order-1', attempt = 1 }
    end,
    write_payload = function(_, dest)
      captured.payload_dest = dest
      return 'null', 4
    end,
    renew = function(_, ttl)
      captured.renew_ttl = ttl
      return { lease_expires_at_unix = 99 }
    end,
    complete = function(self)
      self.completed = true
      return true
    end,
    retry = function(self, req)
      self.retry_req = req
      return true
    end,
    dead_letter = function(self, diagnostic)
      self.diagnostic = diagnostic
      return true
    end,
    close = function(self) self.closed = true end,
  }
  local workflow_core = {
    append_outbox = function(_, entry, payload)
      captured.first_entry = entry
      captured.first_payload = payload
      return transaction_core, { outbox_key = 'first-key', duplicate = false }
    end,
    accept_inbox = function(_, message)
      captured.inbox_message = message
      return nil, { accepted = false, duplicate = true }
    end,
    next = function(_, timeout)
      captured.next_timeout = timeout
      return job_core
    end,
    close = function(self) self.closed = true end,
  }
  local client_core = {
    new_workflow = function(_, config)
      captured.workflow_config = config
      return workflow_core
    end,
    close = function() end,
  }

  core_stub.open = function()
    return client_core
  end

  local client = assert(lockdc.open({}))
  local workflow = assert(client:new_workflow({
    namespace = 'workflow-ns',
    owner = 'lua-worker',
    recovery_interval_seconds = 7,
  }))
  local txn, receipt = assert(workflow:append_outbox({
    operation_id = 'op-1',
    effect_id = 'charge',
    effect_key = 'charge:order-1',
    kind = 'http',
    destination = 'https://billing.test/charge',
    headers = 'header-json',
  }, 'payload'))

  assert_eq(captured.workflow_config.namespace, 'workflow-ns', 'new_workflow should pass config through')
  assert_eq(captured.first_entry.headers_json, 'header-json', 'workflow should encode headers into headers_json')
  assert_eq(captured.first_entry.headers, nil, 'workflow should not pass façade-only headers')
  assert_eq(captured.first_payload, 'payload', 'workflow should preserve arbitrary payload source')
  assert_eq(receipt.outbox_key, 'first-key', 'workflow should return the durable receipt')

  local participant = assert(txn:acquire({ namespace_name = 'orders', key = 'order-1' }))
  participant:update_json(nil, { if_version = 1 })
  assert_eq(captured.acquire_req.key, 'order-1', 'transaction acquire should pass request through')
  assert_eq(captured.update_body, 'null', 'participant update_json should encode nil as JSON null')
  assert_eq(captured.update_opts.content_type, 'application/json', 'participant update_json should default content type')
  assert_eq(participant:get_json(), lockdc.json_null, 'participant get_json should decode JSON null')
  participant:metadata({ query_hidden = true })
  assert_eq(captured.metadata_req.query_hidden, true, 'participant metadata should delegate')
  participant:mutate({ mutations = { '/revision++' } })
  assert_eq(captured.mutate_req.mutations[1], '/revision++', 'participant mutate should delegate')
  participant:mutate_local({ mutations = { '/label="local"' } })
  assert_eq(captured.mutate_local_req.mutations[1], '/label="local"', 'participant mutate_local should delegate')
  assert_eq(participant:list_attachments().items[1].name, 'body', 'participant attachment list should delegate')
  assert_truthy(participant:delete_attachment({ name = 'body' }), 'participant attachment delete should delegate')
  assert_eq(captured.delete_attachment_selector.name, 'body', 'participant attachment selector should pass through')
  assert_eq(participant:delete_all_attachments(), 1, 'participant attachment delete-all should delegate')
  participant:close()
  assert_truthy(txn:commit(), 'transaction commit should delegate')
  assert_truthy(transaction_core.committed, 'transaction core should observe commit')

  local duplicate_txn, duplicate = workflow:accept_inbox({
    consumer_id = 'billing',
    source_kind = 'http',
    source_id = 'orders',
    message_id = 'message-1',
  })
  assert_eq(duplicate_txn, nil, 'duplicate inbox should not expose a transaction')
  assert_eq(duplicate.duplicate, true, 'duplicate inbox should retain the result')
  assert_eq(captured.inbox_message.message_id, 'message-1', 'inbox identity should pass through')

  local job = assert(workflow:next(123))
  assert_eq(captured.next_timeout, 123, 'workflow next should pass its timeout')
  assert_eq(job:payload_json(), lockdc.json_null, 'job payload_json should decode JSON null')
  assert_truthy(job:renew(90), 'job renewal should delegate')
  assert_eq(captured.renew_ttl, 90, 'job renewal should preserve TTL')
  assert_truthy(job:complete(), 'job completion should delegate')
  assert_truthy(job_core.completed, 'job core should observe completion')

  workflow:close()
  assert_truthy(workflow_core.closed, 'workflow close should close the core receiver')
  client:close()
end

test_json_helpers()
test_request_flattening_and_default_content_type()
test_pouch_open_config_passthrough()
test_subscribe_ack_and_error_paths()
test_acquire_for_update_propagates_sdk_failure_shape()
test_subscribe_with_state_and_service_lifecycle()
test_watch_queue_change_detection()
test_json_null_roundtrip_helpers()
test_workflow_facade_lifecycle()

local repo_root = assert(os.getenv('LOCKDC_ROOT'), 'LOCKDC_ROOT is required')

package.path = table.concat({
  repo_root .. '/lua/?.lua',
  repo_root .. '/lua/?/init.lua',
  package.path,
}, ';')

local function make_lonejson_stub()
  local M = {}

  function M.field(name, spec)
    return { name = name, spec = spec }
  end

  function M.json_value(opts)
    return opts or {}
  end

  function M.schema(_name, _fields)
    return {
      encode = function(_, doc)
        return '{"value":' .. tostring(doc.value) .. '}'
      end,
      decode = function(_, text)
        local payload = assert(text:match('^%{"value":(.*)%}$'), 'decode payload missing wrapper')
        return { value = payload }
      end,
    }
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
  assert_eq(lockdc.decode_json('{"k":1}'), '{"k":1}', 'decode_json should unwrap envelope payload')
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
end

local function test_watch_queue_change_detection()
  local queue_stats_plan = {
    { available = 0, head_message_id = 'a', correlation_id = 'one' },
    { available = 0, head_message_id = 'a', correlation_id = 'one' },
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

test_json_helpers()
test_request_flattening_and_default_content_type()
test_subscribe_ack_and_error_paths()
test_subscribe_with_state_and_service_lifecycle()
test_watch_queue_change_detection()

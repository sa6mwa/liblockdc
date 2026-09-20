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
  OK = 0,
  ERR_INVALID = 42,
  ERR_NOMEM = 43,
  ERR_TRANSPORT = 44,
  ERR_PROTOCOL = 45,
  ERR_SERVER = 46,
  ERR_TIMEOUT = 47,
  NACK_FAILURE = 48,
  NACK_DEFER = 49,
  version_string = function()
    return 'test-version'
  end,
  xid_new = function()
    return '0123456789abcdefghijkl'
  end,
  pouch_crypto_generate_key = function()
    return 'lc-pouch-key-v1:test-key'
  end,
  pouch_crypto_default_key_file = function()
    return '/tmp/lockdc/pouch.key'
  end,
  pouch_crypto_generate_key_file = function(path, overwrite)
    return path .. ':' .. tostring(overwrite)
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
  assert_eq(lockdc.xid_new(), '0123456789abcdefghijkl', 'xid_new should delegate to core')
  assert_eq(lockdc.ERR_INVALID, core_stub.ERR_INVALID,
      'public facade should expose C status constants without exposing core')
  assert_eq(lockdc.ERR_TIMEOUT, core_stub.ERR_TIMEOUT,
      'public facade should expose the timeout status constant')
  assert_eq(lockdc.pouch_crypto_generate_key(), 'lc-pouch-key-v1:test-key',
      'Pouch key generation should delegate to the C helper')
  assert_eq(lockdc.pouch_crypto_default_key_file(), '/tmp/lockdc/pouch.key',
      'Pouch default key-file lookup should delegate to the C helper')
  assert_eq(lockdc.pouch_crypto_generate_key_file('/tmp/key', true),
      '/tmp/key:true', 'Pouch key-file generation should delegate to the C helper')
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
    pouch = {
      crypto_key = 'lc-pouch-key-v1:test-key',
      crypto_key_file = '/var/lib/lockdc-lua-unit/root.key',
      crypto_generate_key_file = true,
      compression = 'zlib',
      query_indexing = false,
      query_engine = 'scan',
      indexer_flush_docs = 64,
    },
  }))

  assert_eq(captured.config.endpoints[1], 'pouch:///var/lib/lockdc-lua-unit', 'pouch endpoint should pass through')
  assert_eq(captured.config.pouch.crypto_key, 'lc-pouch-key-v1:test-key', 'typed pouch crypto key should pass through')
  assert_eq(captured.config.pouch.crypto_key_file, '/var/lib/lockdc-lua-unit/root.key', 'typed pouch key file should pass through')
  assert_eq(captured.config.pouch.crypto_generate_key_file, true, 'typed pouch key-file generation should pass through')
  assert_eq(captured.config.pouch.compression, 'zlib', 'typed pouch compression should pass through')
  assert_eq(captured.config.pouch.query_indexing, false, 'typed pouch false should pass through')
  assert_eq(captured.config.pouch.query_engine, 'scan', 'typed pouch engine should pass through')
  assert_eq(captured.config.pouch.indexer_flush_docs, 64, 'typed pouch numeric setting should pass through')
  client:close()
end

local function test_xa_and_transaction_coordinator_forwarding()
  local captured = {}
  local client_core = {
    close = function() end,
  }
  local methods = {
    'query_keys', 'txn_replay', 'txn_prepare', 'txn_commit', 'txn_rollback',
    'tc_lease_acquire', 'tc_lease_renew', 'tc_lease_release',
    'tc_cluster_announce', 'tc_rm_register', 'tc_rm_unregister',
  }

  for _, name in ipairs(methods) do
    client_core[name] = function(_, req)
      captured[name] = req
      return { method = name }
    end
  end
  client_core.tc_leader = function()
    captured.tc_leader = true
    return { method = 'tc_leader' }
  end
  client_core.tc_cluster_leave = function()
    captured.tc_cluster_leave = true
    return { method = 'tc_cluster_leave' }
  end
  client_core.tc_cluster_list = function()
    captured.tc_cluster_list = true
    return { method = 'tc_cluster_list' }
  end
  client_core.tc_rm_list = function()
    captured.tc_rm_list = true
    return { method = 'tc_rm_list' }
  end
  core_stub.open = function()
    return client_core
  end

  local client = assert(lockdc.open({}))
  assert_eq(lockdc.core, nil,
      "the native implementation must not be re-exported as public facade API")
  local decision = {
    txn_id = '00000000000000000001',
    participants = { { namespace_name = 'orders', key = 'order-1' } },
    tc_term = 1,
  }

  assert_eq(client:txn_replay({ txn_id = decision.txn_id }).method, 'txn_replay',
            'txn_replay should delegate to core')
  assert_eq(client:txn_prepare(decision).method, 'txn_prepare',
            'txn_prepare should delegate to core')
  assert_eq(client:txn_commit(decision).method, 'txn_commit',
            'txn_commit should delegate to core')
  assert_eq(client:txn_rollback(decision).method, 'txn_rollback',
            'txn_rollback should delegate to core')
  assert_eq(captured.txn_commit, decision,
            'raw transaction decisions should preserve the request table')
  assert_eq(client:query_keys({ selector_json = '{"kind":"order"}' }, function() end).method,
            'query_keys', 'query_keys should delegate to core')
  assert_eq(client:tc_lease_acquire({ candidate_id = 'node-a' }).method,
            'tc_lease_acquire', 'TC lease acquire should delegate to core')
  assert_eq(client:tc_lease_renew({ leader_id = 'node-a' }).method,
            'tc_lease_renew', 'TC lease renew should delegate to core')
  assert_eq(client:tc_lease_release({ leader_id = 'node-a' }).method,
            'tc_lease_release', 'TC lease release should delegate to core')
  assert_eq(client:tc_leader().method, 'tc_leader',
            'TC leader should delegate to core')
  assert_eq(client:tc_cluster_announce({ self_endpoint = 'pouch://node-a' }).method,
            'tc_cluster_announce', 'TC cluster announce should delegate to core')
  assert_eq(client:tc_cluster_leave().method, 'tc_cluster_leave',
            'TC cluster leave should delegate to core')
  assert_eq(client:tc_cluster_list().method, 'tc_cluster_list',
            'TC cluster list should delegate to core')
  assert_eq(client:tc_rm_register({ backend_hash = 'backend-a' }).method,
            'tc_rm_register', 'TC RM register should delegate to core')
  assert_eq(client:tc_rm_unregister({ backend_hash = 'backend-a' }).method,
            'tc_rm_unregister', 'TC RM unregister should delegate to core')
  assert_eq(client:tc_rm_list().method, 'tc_rm_list',
            'TC RM list should delegate to core')
  client:close()
end

local function test_subscribe_ack_and_error_paths()
  local captured = {}
  local client_core = {
    close = function() end,
    subscribe = function(_, req, handler)
      captured.subscribe_req = req
      captured.subscribe_handler = handler
      return true
    end,
    subscribe_with_state = function(_, req, handler)
      captured.subscribe_with_state_req = req
      captured.subscribe_with_state_handler = handler
      return true
    end,
  }
  local handler = function() end

  core_stub.open = function()
    return client_core
  end

  local client = assert(lockdc.open({}))
  assert_truthy(client:subscribe({ queue = 'jobs' }, handler),
      'subscribe should delegate to the native C streaming operation')
  assert_eq(captured.subscribe_req.queue, 'jobs',
      'subscribe should preserve the C dequeue request')
  assert_eq(captured.subscribe_handler, handler,
      'subscribe should preserve the Lua callback identity')
  assert_truthy(client:subscribe_with_state({ queue = 'state-jobs' }, handler),
      'subscribe_with_state should delegate to the native C streaming operation')
  assert_eq(captured.subscribe_with_state_req.queue, 'state-jobs',
      'subscribe_with_state should preserve the C dequeue request')
  assert_eq(captured.subscribe_with_state_handler, handler,
      'subscribe_with_state should preserve the Lua callback identity')
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
  local client_core = { close = function() end }
  client_core.dequeue = function()
    service_dequeues = service_dequeues + 1
    if service_dequeues == 1 then
      return service_message
    end
    return nil, { message = 'service should stop before another dequeue' }
  end

  core_stub.open = function()
    return client_core
  end
  local client = assert(lockdc.open({}))
  local service
  service = client:new_consumer_service({
    name = 'worker-1',
    request = {
      namespace_name = 'default',
      queue = 'jobs',
    },
    handle = function(msg)
      assert_truthy(msg:is_open(), 'service handler should receive open message')
      service:stop()
      return nil
    end,
  })

  local wait_ok, wait_err = service:wait()
  assert_eq(wait_ok, nil, 'wait before run should fail')
  assert_eq(wait_err.code, core_stub.ERR_INVALID, 'wait before run should return ERR_INVALID')

  local ok, err = service:run()
  assert_truthy(ok, 'service:run should succeed')
  assert_eq(err, nil, 'service:run success should not return error')
  assert_eq(service_message.ack_count, 1, 'service handler success should ack message')
  assert_truthy(service:wait(), 'wait after completed run should succeed')

  local explicitly_acked_message = {
    closed = false,
    ack_count = 0,
  }
  function explicitly_acked_message:ack()
    self.ack_count = self.ack_count + 1
    self.closed = true
    return true
  end
  function explicitly_acked_message:nack(req)
    self.closed = true
    self.last_nack_req = req
    return true
  end
  function explicitly_acked_message:close()
    self.closed = true
  end
  function explicitly_acked_message:state()
    return nil
  end

  client_core.dequeue = function()
    return explicitly_acked_message
  end
  local explicitly_acking_service
  explicitly_acking_service = client:new_consumer_service({
    name = 'explicitly-acking-worker',
    request = { namespace_name = 'default', queue = 'explicitly-acked-jobs' },
    handle = function(message)
      assert_truthy(message:ack(), 'handler should be able to acknowledge directly')
      explicitly_acking_service:stop()
      return nil
    end,
  })
  ok, err = explicitly_acking_service:run()
  assert_truthy(ok,
      'a normally returning handler that terminalized its message must not fail the service')
  assert_eq(err, nil,
      'a normally returning handler that terminalized its message should not report failure')
  assert_eq(explicitly_acked_message.ack_count, 1,
      'a handler-owned acknowledgement must not be repeated by the service')

  local failed_message = {
    closed = false,
    ack_count = 0,
    nack_count = 0,
  }
  function failed_message:ack()
    self.ack_count = self.ack_count + 1
    self.closed = true
    return true
  end
  function failed_message:nack(req)
    self.nack_count = self.nack_count + 1
    self.last_nack_req = req
    self.closed = true
    return true
  end
  function failed_message:close()
    self.closed = true
  end
  function failed_message:state()
    return nil
  end

  client_core.dequeue = function()
    return failed_message
  end
  local failing_service = client:new_consumer_service({
    name = 'failing-worker',
    request = { namespace_name = 'default', queue = 'failed-jobs' },
    handle = function()
      return nil, { message = 'expected handler failure' }
    end,
  })
  ok, err = failing_service:run()
  assert_eq(ok, nil, 'nil, err handler result should stop the service')
  assert_eq(err.message, 'expected handler failure',
      'service should preserve the handler failure')
  assert_eq(failed_message.ack_count, 0,
      'failed service handler must not acknowledge its message')
  assert_eq(failed_message.nack_count, 1,
      'failed service handler should nack exactly once')
  assert_eq(failed_message.last_nack_req.intent, 'failure',
      'failed service handler should use the failure nack intent')
  assert_truthy(failing_service:wait(),
      'wait after a failed synchronous run should observe completion')

  local invalid_service = client:new_consumer_service({ request = {} })
  ok, err = invalid_service:run()
  assert_eq(ok, nil, 'service:run should require a Lua handler')
  assert_eq(err.code, core_stub.ERR_INVALID, 'missing handler should return ERR_INVALID')
  assert_truthy(err.message:match('requires handle'),
      'missing handler should return actionable error')
end

local function test_watch_queue_change_detection()
  local captured = {}
  local client_core = {
    watch_queue = function(_, req, handler)
      captured.request = req
      captured.handler = handler
      return true
    end,
    close = function() end,
  }
  core_stub.open = function()
    return client_core
  end

  local client = assert(lockdc.open({}))
  local handler = function() end
  local ok, err = client:watch_queue({
    namespace_name = 'default',
    queue = 'jobs',
  }, handler)

  assert_truthy(ok, 'watch_queue should delegate to the native C streaming watch')
  assert_eq(err, nil, 'watch_queue success should not return an error')
  assert_eq(captured.request.queue, 'jobs', 'watch_queue should preserve the C request')
  assert_eq(captured.handler, handler, 'watch_queue should preserve the Lua callback identity')
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
  local value, meta = client:read_json({ key = 'state-key' })
  assert_eq(value, lockdc.json_null, 'client:read_json should preserve top-level JSON null')
  assert_eq(meta.etag, 'etag-1', 'client:read_json should still return metadata')

  local message = assert(client:dequeue({ queue = 'jobs' }))
  local payload, written = message:read_payload_json()
  assert_eq(payload, lockdc.json_null, 'message:read_payload_json should preserve top-level JSON null')
  assert_eq(written, 4, 'message:read_payload_json should preserve byte count')
end

local function test_streaming_surface_requires_sink_and_materializers_are_named()
  local calls = {}
  local client_core = {
    get = function(_, req, sink)
      calls.get_sink = sink
      if sink == nil then
        return "state", { etag = "etag-1" }
      end
      return nil, 5
    end,
    query = function(_, req, sink)
      calls.query_sink = sink
      return nil, 2
    end,
    close = function() end,
  }

  core_stub.open = function()
    return client_core
  end

  local client = assert(lockdc.open({}))
  local ok, err = pcall(function()
    client:get({ key = "state" })
  end)
  assert_eq(ok, false, "client:get must require a sink")
  assert_truthy(tostring(err):find("client:read", 1, true),
      "missing sink error should name the materializer")
  assert_eq(client.get_raw, nil, "legacy get_raw alias must not remain public")
  assert_eq(client.get_json, nil, "legacy get_json alias must not remain public")
  assert_eq(client:read({ key = "state" }), "state",
      "client:read should be the explicit materializer")
  local sink = { write = function() end }
  assert_eq(client:get({ key = "state" }, sink), nil,
      "client:get should preserve streaming output semantics")
  assert_eq(calls.get_sink, sink, "client:get should pass the supplied sink through")
  assert_eq(client:query({ engine = "scan" }, sink), nil,
      "client:query should preserve streaming output semantics")
  assert_eq(calls.query_sink, sink,
      "client:query should pass the supplied sink through")
  assert_truthy(type(client.read_query) == "function",
      "client:read_query should be public")
end

local function test_outbox_facade_lifecycle()
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
    append = function(_, entry, payload)
      captured.later_entry = entry
      captured.later_payload = payload
      return { outbox_key = 'later-key', duplicate = false }
    end,
    accept_command = function(_, request)
      captured.txn_command_request = request
      return { command_id = 'cmd-1', state = 1, duplicate = false }
    end,
    complete_command = function(_, result)
      captured.command_result = result
      return true
    end,
    fail_command = function(_, result)
      captured.command_failure = result
      return true
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
    complete = function(self, completion)
      self.completed = true
      captured.delivery_completion = completion
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
  local outbox_core = {
    accept_command = function(_, request)
      captured.command_request = request
      return transaction_core, { command_id = 'cmd-1', state = 1, duplicate = false }
    end,
    get_command_receipt = function(_, identity)
      captured.command_identity = identity
      return { command_id = 'cmd-1', state = 2, result_code = 'created' }
    end,
    write_command_result = function(_, identity, destination)
      captured.command_result_identity = identity
      captured.command_result_destination = destination
      return nil, 12
    end,
    resume_command = function(_, identity)
      captured.resume_identity = identity
      return nil, { command_id = 'cmd-1', state = 2, duplicate = true }
    end,
    append = function(_, entry, payload)
      captured.first_entry = entry
      captured.first_payload = payload
      return transaction_core, { outbox_key = 'first-key', duplicate = false }
    end,
    accept_inbox = function(_, message)
      captured.inbox_message = message
      return nil, { accepted = false, duplicate = true }
    end,
    dispatcher = function(self)
      return self
    end,
    next = function(_, timeout)
      captured.next_timeout = timeout
      return job_core
    end,
    stats = function()
      return { running = true, recovery_queries = 3 }
    end,
    reconcile = function(self)
      self.reconciled = true
      return true
    end,
    replay_dead_letter = function(self, key)
      self.replayed_key = key
      return true
    end,
    delete_dead_letter = function(self, key)
      self.deleted_key = key
      return true
    end,
    export_dead_letters = function(_, options, dest)
      captured.export_options = options
      captured.export_dest = dest
      return nil, { exported = 1 }
    end,
    close = function(self) self.closed = true end,
  }
  local client_core = {
    new_outbox = function(_, config)
      captured.outbox_config = config
      return outbox_core
    end,
    close = function() end,
  }

  core_stub.open = function()
    return client_core
  end

  local client = assert(lockdc.open({}))
  local outbox = assert(client:new_outbox({
    namespace_name = 'outbox-ns',
    owner = 'lua-worker',
    recovery_interval_seconds = 7,
    shutdown_timeout_ms = 1234,
    replay_dead_letters_on_startup = true,
  }))
  local txn, receipt = assert(outbox:append({
    operation_id = 'op-1',
    effect_id = 'charge',
    effect_key = 'charge:order-1',
    payload_digest = 'sha256:payload',
    kind = 'http',
    destination = 'https://billing.test/charge',
    headers_json = 'header-json',
  }, 'payload'))

  assert_eq(captured.outbox_config.namespace_name, 'outbox-ns', 'new_outbox should pass config through')
  assert_eq(captured.outbox_config.shutdown_timeout_ms, 1234,
      'outbox shutdown timeout should pass through')
  assert_eq(captured.outbox_config.replay_dead_letters_on_startup, true,
      'outbox startup replay option should pass through')
  assert_eq(captured.first_entry.headers_json, 'header-json', 'outbox should preserve headers_json')
  assert_eq(captured.first_entry.payload_digest, 'sha256:payload',
      'outbox should preserve the immutable payload digest')
  assert_eq(captured.first_payload, 'payload', 'outbox should preserve arbitrary payload source')
  assert_eq(receipt.outbox_key, 'first-key', 'outbox should return the durable receipt')

  local participant = assert(txn:acquire({ namespace_name = 'orders', key = 'order-1' }))
  participant:update_json(nil, { if_version = 1 })
  assert_eq(captured.acquire_req.key, 'order-1', 'transaction acquire should pass request through')
  assert_eq(captured.update_body, 'null', 'participant update_json should encode nil as JSON null')
  assert_eq(captured.update_opts.content_type, 'application/json', 'participant update_json should default content type')
  assert_eq(participant:read_json(), lockdc.json_null, 'participant read_json should decode JSON null')
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
  txn:close()
  assert_truthy(transaction_core.closed,
      'transaction close must release the native transaction after commit')

  local duplicate_txn, duplicate = outbox:accept_inbox({
    consumer_id = 'billing',
    source_kind = 'http',
    source_id = 'orders',
    message_id = 'message-1',
  })
  assert_eq(duplicate_txn, nil, 'duplicate inbox should not expose a transaction')
  assert_eq(duplicate.duplicate, true, 'duplicate inbox should retain the result')
  assert_eq(captured.inbox_message.message_id, 'message-1', 'inbox identity should pass through')

  local command_txn, command_receipt = assert(outbox:accept_command({
    scope = 'tenant-a', command_type = 'orders.create.v1',
    idempotency_key = 'request-1', request_digest = 'digest-1',
  }))
  assert_eq(captured.command_request.request_digest, 'digest-1',
      'command request should preserve durable digest')
  assert_eq(command_receipt.command_id, 'cmd-1', 'command receipt should pass through')
  assert_truthy(command_txn:complete_command({ result_code = 'created' }),
      'command completion should delegate')
  assert_eq(captured.command_result.result_code, 'created',
      'command result should pass through')
  assert_truthy(command_txn:fail_command({ failure_code = 'declined' }),
      'command failure should delegate')
  local status = assert(outbox:get_command_receipt({
    scope = 'tenant-a', command_type = 'orders.create.v1', idempotency_key = 'request-1',
  }))
  assert_eq(status.result_code, 'created', 'command status should delegate')
  local result_bytes, result_written = outbox:write_command_result({
    scope = 'tenant-a', command_type = 'orders.create.v1', idempotency_key = 'request-1',
  }, { path = '/tmp/command-result' })
  assert_eq(result_bytes, nil, 'command result should not materialize streamed output')
  assert_eq(result_written, 12, 'command result should preserve byte count')
  local resumed_txn, resumed = outbox:resume_command({
    scope = 'tenant-a', command_type = 'orders.create.v1', idempotency_key = 'request-1',
  })
  assert_eq(resumed_txn, nil, 'terminal command resume should not expose a transaction')
  assert_eq(resumed.duplicate, true, 'terminal command resume should return receipt')

  local dispatcher = assert(outbox:dispatcher())
  local job = assert(dispatcher:next(123))
  assert_eq(captured.next_timeout, 123, 'dispatcher next should pass its timeout')
  assert_eq(job:read_payload_json(), lockdc.json_null, 'job read_payload_json should decode JSON null')
  assert_truthy(job:renew(90), 'job renewal should delegate')
  assert_eq(captured.renew_ttl, 90, 'job renewal should preserve TTL')
  assert_truthy(job:complete({ delivery_reference = 'provider-1' }),
      'job completion should delegate')
  assert_truthy(job_core.completed, 'job core should observe completion')
  assert_eq(captured.delivery_completion.delivery_reference, 'provider-1',
      'job completion evidence should pass through')
  job:close()
  assert_eq(job_core.closed, nil,
      'terminal completion must consume the native job before close')

  local stats = assert(dispatcher:stats())
  assert_eq(stats.recovery_queries, 3, 'dispatcher stats should delegate')
  assert_truthy(dispatcher:reconcile(), 'dispatcher reconciliation should delegate')
  assert_truthy(outbox_core.reconciled, 'outbox core should reconcile')
  assert_truthy(dispatcher:replay_dead_letter('dead-key'),
      'dead-letter replay should delegate')
  assert_eq(outbox_core.replayed_key, 'dead-key',
      'dead-letter replay key should pass through')
  assert_truthy(dispatcher:delete_dead_letter('dead-key'),
      'dead-letter delete should delegate')
  assert_eq(outbox_core.deleted_key, 'dead-key',
      'dead-letter delete key should pass through')
  local exported, export_result = dispatcher:export_dead_letters(
      { format = 'jsonl', limit = 10 }, { path = '/tmp/dead-letter.jsonl' })
  assert_eq(export_result.exported, 1, 'dead-letter export result should delegate')
  assert_eq(captured.export_options.format, 'jsonl',
      'dead-letter export options should pass through')
  assert_eq(captured.export_dest.path, '/tmp/dead-letter.jsonl',
      'dead-letter export destination should pass through')
  assert_eq(exported, nil,
      'dead-letter export should not materialize streamed output')
  local missing_sink, missing_sink_err = pcall(function()
    dispatcher:export_dead_letters({ format = 'jsonl' })
  end)
  assert_eq(missing_sink, false,
      'dead-letter export must require an explicit sink')
  assert_truthy(tostring(missing_sink_err):find('read_dead_letters', 1, true),
      'dead-letter export should point callers to its materializer')

  dispatcher:close()
  assert_truthy(outbox_core.closed, 'dispatcher close should close the core receiver')
  outbox:close()
  assert_truthy(outbox_core.closed, 'outbox close should close the core receiver')
  client:close()
end

local function test_dispatcher_handler_cache_follows_native_activation()
  local calls = {}
  local dispatcher_core = {
    pump = function(_, options)
      if options.max_jobs == 0 then
        error('dispatcher pump limits are invalid')
      end
      if calls.bound_handlers == nil then
        calls.bound_handlers = options.handlers
      else
        assert_eq(options.handlers, calls.bound_handlers,
            'a handler reentry must reuse the native immutable map')
      end
      options._lockdc_facade_handlers_bound = true
      if options.reentrant then
        calls.reentrant = (calls.reentrant or 0) + 1
        return 0
      end
      options.handlers.http({})
      return 1
    end,
    close = function() end,
  }
  local outbox_core = {
    dispatcher = function()
      return dispatcher_core
    end,
    close = function() end,
  }
  local client_core = {
    new_outbox = function()
      return outbox_core
    end,
    close = function() end,
  }

  core_stub.open = function()
    return client_core
  end

  local client = assert(lockdc.open({}))
  local outbox = assert(client:new_outbox({}))
  local dispatcher = assert(outbox:dispatcher())
  local alias = assert(outbox:dispatcher())
  local handlers = {
    http = function()
      calls.old = (calls.old or 0) + 1
    end,
  }
  local ok, err = pcall(function()
    dispatcher:pump({ handlers = handlers, max_jobs = 0 })
  end)
  assert_eq(ok, false,
      'a rejected native pump option must not activate Lua handlers')
  assert_truthy(tostring(err):find('pump limits are invalid', 1, true),
      'the native pump validation error should be preserved')
  handlers.http = function()
    calls.new = (calls.new or 0) + 1
    assert_eq(alias:pump({ handlers = handlers, max_jobs = 1, reentrant = true }),
        0, 'a handler should reuse its map during the first activation')
  end
  assert_eq(dispatcher:pump({ handlers = handlers, max_jobs = 1 }), 1,
      'a corrected handler map should activate successfully')
  assert_eq(calls.old, nil,
      'a rejected activation must not retain stale handler functions')
  assert_eq(calls.new, 1,
      'the successful retry must invoke the corrected handler function')
  assert_eq(calls.reentrant, 1,
      'the first handler invocation must expose its map to dispatcher aliases')
  dispatcher:close()
  assert_eq(alias:pump({ handlers = handlers, max_jobs = 1, reentrant = true }),
      0, 'a surviving alias must retain the activated handler map')
  alias:close()
  outbox:close()
  client:close()
end

test_json_helpers()
test_request_flattening_and_default_content_type()
test_pouch_open_config_passthrough()
test_xa_and_transaction_coordinator_forwarding()
test_subscribe_ack_and_error_paths()
test_acquire_for_update_propagates_sdk_failure_shape()
test_subscribe_with_state_and_service_lifecycle()
test_watch_queue_change_detection()
test_json_null_roundtrip_helpers()
test_streaming_surface_requires_sink_and_materializers_are_named()
test_outbox_facade_lifecycle()
test_dispatcher_handler_cache_follows_native_activation()

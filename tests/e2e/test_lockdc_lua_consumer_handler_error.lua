local lockdc = require('lockdc')

local endpoint = os.getenv('LOCKDC_URL') or 'https://localhost:19441'
local client_pem = os.getenv('LOCKDC_CLIENT_PEM')
  or './devenv/volumes/lockd-disk-a-config/client.pem'
local namespace_name = os.getenv('LOCKDC_NAMESPACE') or 'default'
local queue = os.getenv('LOCKDC_QUEUE') or 'tests-lua-consumer-handler-error'
local owner = os.getenv('LOCKDC_OWNER') or 'tests-lua-consumer-handler-error'

local client, err = lockdc.open({
  endpoints = { endpoint },
  client_bundle_source = { path = client_pem },
  default_namespace = namespace_name,
})

if client == nil then
  error(('lockdc.open failed: %s'):format(err.message))
end

local enqueued, enqueue_err = client:enqueue({
  queue = queue,
  content_type = 'application/json',
  visibility_timeout_seconds = 30,
  ttl_seconds = 300,
}, lockdc.encode_json({
  kind = 'lua-consumer-handler-error',
  attempt = 1,
}))

if enqueued == nil then
  client:close()
  error(('client:enqueue failed: %s'):format(enqueue_err.message))
end

local service = client:new_consumer_service({
  Name = owner,
  Queue = queue,
  Options = {
    namespace_name = namespace_name,
    owner = owner,
    visibility_timeout_seconds = 30,
    wait_seconds = 5,
  },
  MessageHandler = function(_message)
    return { message = 'expected consumer failure' }
  end,
})

local ok, service_err = service:start()
if ok ~= nil then
  client:close()
  error('service:start unexpectedly succeeded for failing handler')
end
if type(service_err) ~= 'table' or service_err.message ~= 'expected consumer failure' then
  client:close()
  error(('unexpected service error payload: %s'):format(tostring(service_err and service_err.message or service_err)))
end

local message, dequeue_err = client:dequeue({
  namespace_name = namespace_name,
  queue = queue,
  owner = owner .. '-probe',
  visibility_timeout_seconds = 30,
  wait_seconds = 5,
})

if message == nil then
  client:close()
  error(('expected redelivery after handler failure, dequeue failed: %s'):format(dequeue_err and dequeue_err.message or tostring(dequeue_err)))
end

local payload, payload_err = message:payload_json()
if payload == nil then
  message:close()
  client:close()
  error(('message:payload_json failed: %s'):format(payload_err and payload_err.message or tostring(payload_err)))
end
if type(payload) ~= 'table' or payload.kind ~= 'lua-consumer-handler-error' or payload.attempt ~= 1 then
  message:close()
  client:close()
  error(('unexpected redelivered payload kind=%s attempt=%s'):format(
    tostring(type(payload) == 'table' and payload.kind or payload),
    tostring(type(payload) == 'table' and payload.attempt or nil)))
end

local ack_ok, ack_err = message:ack()
if ack_ok == nil then
  client:close()
  error(('message:ack failed: %s'):format(ack_err and ack_err.message or tostring(ack_err)))
end

client:close()

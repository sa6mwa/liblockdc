local lockdc = require("lockdc")

local endpoint = assert(os.getenv("LOCKDC_URL"), "LOCKDC_URL is required")
local client_pem = assert(os.getenv("LOCKDC_CLIENT_PEM"), "LOCKDC_CLIENT_PEM is required")
local namespace_name = os.getenv("LOCKDC_NAMESPACE") or "default"
local queue = os.getenv("LOCKDC_QUEUE") or "tests-lua-consumer"
local owner = os.getenv("LOCKDC_OWNER") or "tests-lua-consumer"

local client, err = lockdc.open({
  endpoints = { endpoint },
  client_bundle_path = client_pem,
  default_namespace = namespace_name,
})

if client == nil then
  error(("lockdc.open failed: %s"):format(err.message))
end

local enqueued, enqueue_err = client:enqueue({
  queue = queue,
  content_type = "application/json",
  visibility_timeout_seconds = 30,
  ttl_seconds = 300,
}, lockdc.encode_json({
  kind = "lua-consumer-e2e",
  count = 1,
}))

if enqueued == nil then
  client:close()
  error(("client:enqueue failed: %s"):format(enqueue_err.message))
end

local handled = 0
local service

service = client:new_consumer_service({
  Name = owner,
  Queue = queue,
  WithState = true,
  Options = {
    namespace_name = namespace_name,
    owner = owner,
    visibility_timeout_seconds = 30,
    wait_seconds = 5,
  },
  MessageHandler = function(message, state)
    local payload, payload_err = message:payload_json()
    local document, meta

    if payload == nil then
      return payload_err
    end
    if state == nil then
      return { message = "expected consumer state lease" }
    end

    document, meta = state:get_json()
    if meta ~= nil and meta.no_content then
      document = {}
    elseif document == nil then
      return { message = "state:get_json returned nil without no_content metadata" }
    end

    handled = handled + 1
    document.last_kind = payload.kind
    document.handled = handled

    local updated, update_err = state:update_json(document, {
      content_type = "application/json",
    })

    if updated == nil then
      return update_err
    end

    service:stop()
    return nil
  end,
})

local ok, service_err = service:start()

if ok == nil then
  client:close()
  error(("service:start failed: %s"):format(service_err.message or tostring(service_err)))
end

if handled ~= 1 then
  client:close()
  error(("expected exactly one handled message, got %d"):format(handled))
end

local stats, stats_err = client:queue_stats({
  namespace_name = namespace_name,
  queue = queue,
})

if stats == nil then
  client:close()
  error(("client:queue_stats failed: %s"):format(stats_err.message))
end

if (stats.available or 0) ~= 0 then
  client:close()
  error(("expected empty queue after consumer ack, got available=%s"):format(tostring(stats.available)))
end

client:close()

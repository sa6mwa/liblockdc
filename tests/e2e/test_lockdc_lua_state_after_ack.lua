local lockdc = require("lockdc")

local endpoint = assert(os.getenv("LOCKDC_URL"), "LOCKDC_URL is required")
local client_pem = assert(os.getenv("LOCKDC_CLIENT_PEM"), "LOCKDC_CLIENT_PEM is required")
local namespace_name = os.getenv("LOCKDC_NAMESPACE") or "default"
local queue = os.getenv("LOCKDC_QUEUE") or "tests-lua-state-after-ack"
local owner = os.getenv("LOCKDC_OWNER") or "tests-lua-state-after-ack"

local client, err = lockdc.open({
  endpoints = { endpoint },
  client_bundle_source = { path = client_pem },
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
  kind = "lua-state-after-ack",
  ok = true,
}))

if enqueued == nil then
  client:close()
  error(("client:enqueue failed: %s"):format(enqueue_err.message))
end

local message, dequeue_err = client:dequeue_with_state({
  namespace_name = namespace_name,
  queue = queue,
  owner = owner,
  visibility_timeout_seconds = 30,
  wait_seconds = 5,
})

if message == nil then
  client:close()
  error(("client:dequeue_with_state failed: %s"):format(dequeue_err and dequeue_err.message or tostring(dequeue_err)))
end

local state = message:state()
if state == nil then
  message:close()
  client:close()
  error("message:state returned nil")
end

local info = state:info()
if type(info) ~= "table" or info.lease_id == nil then
  message:close()
  state:close()
  client:close()
  error("state:info did not expose a bound lease")
end

local ack_ok, ack_err = message:ack()
if ack_ok == nil then
  state:close()
  client:close()
  error(("message:ack failed: %s"):format(ack_err and ack_err.message or tostring(ack_err)))
end

local post_ack_info = state:info()
if type(post_ack_info) ~= "table" or post_ack_info.lease_id ~= info.lease_id then
  state:close()
  client:close()
  error("cloned state lease became invalid after ack")
end

state:close()
client:close()

local lockdc = require("lockdc")

local endpoint = os.getenv("LOCKDC_URL") or "https://localhost:19441"
local client_pem = os.getenv("LOCKDC_CLIENT_PEM")
  or "./devenv/volumes/lockd-disk-a-config/client.pem"
local namespace_name = os.getenv("LOCKDC_NAMESPACE") or "default"
local queue = os.getenv("LOCKDC_QUEUE") or "examples-lua-roundtrip"
local owner = os.getenv("LOCKDC_OWNER") or "lua-example-queue"

local client, err = lockdc.open({
  endpoints = { endpoint },
  client_bundle_source = { path = client_pem },
  default_namespace = namespace_name,
})

if client == nil then
  error(("lockdc.open failed: %s"):format(err.message))
end

local enqueue_res, enqueue_err = client:enqueue({
  queue = queue,
  content_type = "application/json",
  visibility_timeout_seconds = 30,
  ttl_seconds = 300,
}, lockdc.encode_json({
  op = "ship",
  example = "queue_roundtrip.lua",
}))

if enqueue_res == nil then
  client:close()
  error(("client:enqueue failed: %s"):format(enqueue_err.message))
end

print(("enqueued message %s on queue %s"):format(
  enqueue_res.message_id,
  enqueue_res.queue
))

local message, dequeue_err = client:dequeue({
  queue = queue,
  owner = owner,
  visibility_timeout_seconds = 30,
  wait_seconds = 5,
})

if message == nil then
  client:close()
  error(("client:dequeue failed: %s"):format(dequeue_err.message))
end

local payload, written_or_err = message:payload_json()
if payload == nil then
  message:close()
  client:close()
  error(("message:payload_json failed: %s"):format(written_or_err.message))
end

print(("dequeued message %s attempts=%d op=%s"):format(
  message:info().message_id,
  message:info().attempts,
  tostring(payload.op)
))

local acked, ack_err = message:ack()
if acked == nil then
  message:close()
  client:close()
  error(("message:ack failed: %s"):format(ack_err.message))
end

client:close()

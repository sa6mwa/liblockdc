local lockdc = require("lockdc")

local endpoint = os.getenv("LOCKDC_URL") or "https://localhost:19441"
local client_pem = os.getenv("LOCKDC_CLIENT_PEM")
  or "./devenv/volumes/lockd-disk-a-config/client.pem"
local namespace_name = os.getenv("LOCKDC_NAMESPACE") or "default"
local queue = os.getenv("LOCKDC_QUEUE") or "examples-lua-consumer"
local owner = os.getenv("LOCKDC_OWNER") or "lua-example-consumer"

-- This consumer path is intentionally blocking and single-threaded in Lua.
-- One message is delivered, handled, and terminalized before the next dequeue.

local client, err = lockdc.open({
  endpoints = { endpoint },
  client_bundle_path = client_pem,
  default_namespace = namespace_name,
})

if client == nil then
  error(("lockdc.open failed: %s"):format(err.message))
end

local ok, consumer_err = client:start_consumer({
  Name = "lua-example-consumer",
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

    print(("received message %s op=%s"):format(
      message:info().message_id,
      tostring(payload.op)
    ))

    if state ~= nil then
      document, meta = state:get_json()
      if meta ~= nil and meta.no_content then
        document = {}
      elseif document == nil then
        return {
          message = "state:get_json returned nil without no_content metadata",
        }
      end

      document.last_message_id = message:info().message_id
      document.last_op = payload.op

      local updated, update_err = state:update_json(document, {
        content_type = "application/json",
      })

      if updated == nil then
        return update_err
      end

      print(("updated state version=%d"):format(updated.version))
    end

    return nil
  end,
})

if ok == nil then
  client:close()
  error(("client:start_consumer failed: %s"):format(consumer_err.message or tostring(consumer_err)))
end

client:close()

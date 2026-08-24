local lockdc = require("lockdc")

local root = assert(os.getenv("LOCKDC_POUCH_ROOT"), "LOCKDC_POUCH_ROOT is required")
local endpoint = "pouch://" .. root
local key_file = root .. "/pouch.key"
local explicit_false_key_file = root .. "/explicit-false/pouch.key"
local namespace_name = "lua-pouch"

local invalid_client, invalid_err = lockdc.open({
  endpoints = { endpoint },
  pouch_compression = "invalid",
})
if invalid_client ~= nil then
  invalid_client:close()
  error("invalid Pouch compression unexpectedly opened")
end
if type(invalid_err) ~= "table" or not (invalid_err.message or ""):match("compression") then
  error("invalid Pouch compression did not return a structured error")
end

local disabled_generation_client, disabled_generation_err = lockdc.open({
  endpoints = {
    endpoint .. "?crypto_key_file=" .. explicit_false_key_file
      .. "&crypto_generate_key_file=true",
  },
  pouch_crypto_generate_key_file = false,
})
if disabled_generation_client ~= nil or disabled_generation_err == nil then
  error("explicit Lua false did not override endpoint key-file generation")
end
local explicit_false_key = io.open(explicit_false_key_file, "rb")
if explicit_false_key ~= nil then
  explicit_false_key:close()
  error("endpoint key-file generation ignored explicit Lua false")
end

local client, err = lockdc.open({
  endpoints = { endpoint },
  default_namespace = namespace_name,
  pouch_crypto_key_file = key_file,
  pouch_crypto_generate_key_file = true,
  pouch_compression = "zlib",
})
if client == nil then
  error(("encrypted Pouch open failed: %s"):format(err and err.message or tostring(err)))
end

local second_client, second_err = lockdc.open({
  endpoints = { endpoint },
  pouch_crypto_key_file = key_file,
  pouch_compression = "zlib",
})
if second_client ~= nil then
  second_client:close()
  client:close()
  error("second exclusive Pouch writer unexpectedly opened")
end
if type(second_err) ~= "table" or not (second_err.message or ""):match("writer") then
  client:close()
  error("second Pouch writer did not return a structured writer error")
end

local lease, acquire_err = client:acquire({
  key = "state",
  owner = "lua-pouch-e2e",
  ttl_seconds = 30,
})
if lease == nil then
  client:close()
  error(("Pouch acquire failed: %s"):format(acquire_err and acquire_err.message or tostring(acquire_err)))
end

local updated, update_err = lease:update_json({ source = "lua-pouch", value = 1 })
if updated == nil then
  lease:close()
  client:close()
  error(("Pouch state update failed: %s"):format(update_err and update_err.message or tostring(update_err)))
end
assert(lease:release())

local state, state_meta_or_err = client:get_json({ key = "state" })
if state == nil or state.source ~= "lua-pouch" or state.value ~= 1 then
  client:close()
  error(("Pouch state read failed: %s"):format(state_meta_or_err and state_meta_or_err.message or tostring(state_meta_or_err)))
end

local enqueued, enqueue_err = client:enqueue({
  queue = "work",
  content_type = "application/json",
  visibility_timeout_seconds = 30,
  ttl_seconds = 30,
}, lockdc.encode_json({ source = "lua-pouch" }))
if enqueued == nil then
  client:close()
  error(("Pouch enqueue failed: %s"):format(enqueue_err and enqueue_err.message or tostring(enqueue_err)))
end

local message, dequeue_err = client:dequeue({
  queue = "work",
  owner = "lua-pouch-e2e",
  visibility_timeout_seconds = 30,
  wait_seconds = 0,
})
if message == nil then
  client:close()
  error(("Pouch dequeue failed: %s"):format(dequeue_err and dequeue_err.message or tostring(dequeue_err)))
end
local payload, written_or_err = message:payload_json()
if payload == nil or payload.source ~= "lua-pouch" or type(written_or_err) ~= "number" then
  message:close()
  client:close()
  error("Pouch queue payload did not round-trip through the Lua facade")
end
assert(message:ack())

local flush, flush_err = client:flush_index({ namespace_name = namespace_name, mode = "wait" })
if flush == nil then
  client:close()
  error(("Pouch index flush failed: %s"):format(flush_err and flush_err.message or tostring(flush_err)))
end
client:close()

local missing_key_client, missing_key_err = lockdc.open({
  endpoints = { endpoint },
  pouch_crypto_key_file = root .. "/missing.key",
  pouch_compression = "zlib",
})
if missing_key_client ~= nil then
  missing_key_client:close()
  error("Pouch open with a missing configured key file unexpectedly succeeded")
end
if type(missing_key_err) ~= "table" or not (missing_key_err.message or ""):match("key file") then
  error("missing Pouch key file did not return a structured error")
end

local reopened, reopen_err = lockdc.open({
  endpoints = { endpoint },
  default_namespace = namespace_name,
  pouch_crypto_key_file = key_file,
  pouch_compression = "zlib",
})
if reopened == nil then
  error(("encrypted Pouch reopen failed: %s"):format(reopen_err and reopen_err.message or tostring(reopen_err)))
end
local reopened_state, reopened_meta_or_err = reopened:get_json({ key = "state" })
if reopened_state == nil or reopened_state.value ~= 1 then
  reopened:close()
  error(("encrypted Pouch persistence read failed: %s"):format(reopened_meta_or_err and reopened_meta_or_err.message or tostring(reopened_meta_or_err)))
end
reopened:close()

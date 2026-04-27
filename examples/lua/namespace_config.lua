local lockdc = require("lockdc")

local endpoint = os.getenv("LOCKDC_URL") or "https://localhost:19441"
local client_pem = os.getenv("LOCKDC_CLIENT_PEM")
  or "./devenv/volumes/lockd-disk-a-config/client.pem"
local namespace_name = os.getenv("LOCKDC_NAMESPACE") or "default"
local flush_mode = os.getenv("LOCKDC_FLUSH_MODE") or "wait"

local client, err = lockdc.open({
  endpoints = { endpoint },
  client_bundle_source = { path = client_pem },
  default_namespace = namespace_name,
})

if client == nil then
  error(("lockdc.open failed: %s"):format(err.message))
end

local config, config_err = client:get_namespace_config({
  namespace_name = namespace_name,
})

if config == nil then
  client:close()
  error(("client:get_namespace_config failed: %s"):format(config_err.message))
end

print(("namespace=%s preferred_engine=%s fallback_engine=%s"):format(
  config.namespace_name,
  tostring(config.preferred_engine),
  tostring(config.fallback_engine)
))

local flush, flush_err = client:flush_index({
  namespace_name = namespace_name,
  mode = flush_mode,
})

if flush == nil then
  client:close()
  error(("client:flush_index failed: %s"):format(flush_err.message))
end

print(("flush accepted=%s flushed=%s pending=%s flush_id=%s"):format(
  tostring(flush.accepted),
  tostring(flush.flushed),
  tostring(flush.pending),
  tostring(flush.flush_id)
))

client:close()

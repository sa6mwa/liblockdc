local lockdc = require("lockdc")

local endpoint = os.getenv("LOCKDC_URL") or "https://localhost:19441"
local client_pem = os.getenv("LOCKDC_CLIENT_PEM")
  or "./devenv/volumes/lockd-disk-a-config/client.pem"
local namespace_name = os.getenv("LOCKDC_NAMESPACE") or "default"
local key = os.getenv("LOCKDC_KEY") or "examples/lua/acquire-update-json"
local owner = os.getenv("LOCKDC_OWNER") or "lua-example-acquire"

local client, err = lockdc.open({
  endpoints = { endpoint },
  client_bundle_source = { path = client_pem },
  default_namespace = namespace_name,
})

if client == nil then
  error(("lockdc.open failed: %s"):format(err.message))
end

local lease, acquire_err = client:acquire({
  key = key,
  owner = owner,
  ttl_seconds = 60,
})

if lease == nil then
  client:close()
  error(("client:acquire failed: %s"):format(acquire_err.message))
end

local state, meta = lease:get_json()
if meta ~= nil and meta.no_content then
  state = {
    kind = "lua-example",
    created_by = owner,
    updates = 0,
  }
elseif state == nil then
  lease:close()
  client:close()
  error("lease:get_json returned nil without no_content metadata")
end

state.status = "updated-via-lua"
state.updates = (state.updates or 0) + 1

local updated, update_err = lease:update_json(state, {
  content_type = "application/json",
})

if updated == nil then
  lease:close()
  client:close()
  error(("lease:update_json failed: %s"):format(update_err.message))
end

print(("updated key %s version=%d etag=%s"):format(
  lease:info().key,
  updated.version,
  updated.state_etag or ""
))

local released, release_err = lease:release()
if released == nil then
  lease:close()
  client:close()
  error(("lease:release failed: %s"):format(release_err.message))
end

client:close()

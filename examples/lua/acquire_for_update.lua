local lockdc = require("lockdc")

local endpoint = os.getenv("LOCKDC_URL") or "https://localhost:19441"
local client_pem = os.getenv("LOCKDC_CLIENT_PEM")
  or "./devenv/volumes/lockd-disk-a-config/client.pem"
local namespace_name = os.getenv("LOCKDC_NAMESPACE") or "default"
local key = os.getenv("LOCKDC_KEY") or "examples/lua/acquire-for-update"
local owner = os.getenv("LOCKDC_OWNER") or "lua-example-acquire-for-update"

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

local seeded, seed_err = lease:update_json({
  kind = "lua-example-acquire-for-update",
  status = "queued",
  updates = 1,
})

if seeded == nil then
  lease:close()
  client:close()
  error(("lease:update_json failed: %s"):format(seed_err.message))
end

local released, release_err = lease:release()
if released == nil then
  lease:close()
  client:close()
  error(("lease:release failed: %s"):format(release_err.message))
end

local ok, update_err = client:acquire_for_update({
  key = key,
  owner = owner,
  ttl_seconds = 60,
}, function(af)
  local state, meta = af:load_json()
  if state == nil and (meta == nil or not meta.no_content) then
    return "expected a snapshot state document"
  end

  state.status = "processing"
  state.updates = (state.updates or 0) + 1

  return af:update_json(state)
end)

if not ok then
  client:close()
  error(("client:acquire_for_update failed: %s"):format(update_err.message))
end

print(("acquire_for_update updated key %s"):format(key))

client:close()

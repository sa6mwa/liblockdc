local lockdc = require("lockdc")

local endpoint = os.getenv("LOCKDC_URL") or "https://localhost:19441"
local client_pem = os.getenv("LOCKDC_CLIENT_PEM")
  or "./devenv/volumes/lockd-disk-a-config/client.pem"
local namespace_name = os.getenv("LOCKDC_NAMESPACE") or "default"
local key = os.getenv("LOCKDC_KEY") or "tests/lua/e2e-acquire-for-update"
local owner = os.getenv("LOCKDC_OWNER") or "lua-e2e-acquire-for-update"

local client, err = lockdc.open({
  endpoints = { endpoint },
  client_bundle_source = { path = client_pem },
  default_namespace = namespace_name,
})

if client == nil then
  error(("lockdc.open failed: %s"):format(err.message))
end

local seed_lease, seed_err = client:acquire({
  key = key,
  owner = owner .. "-seed",
  ttl_seconds = 60,
  block_seconds = 30,
})

if seed_lease == nil then
  client:close()
  error(("seed acquire failed: %s"):format(seed_err.message))
end

local seeded, seed_update_err = seed_lease:update_json({
  value = 1,
  source = "lua-acquire-for-update-seed",
})

if seeded == nil then
  seed_lease:close()
  client:close()
  error(("seed update failed: %s"):format(seed_update_err.message))
end

local released, release_err = seed_lease:release()
if released == nil then
  seed_lease:close()
  client:close()
  error(("seed release failed: %s"):format(release_err.message))
end

local saw_snapshot = false
local ok, af_err = client:acquire_for_update({
  key = key,
  owner = owner,
  ttl_seconds = 60,
  block_seconds = 30,
}, function(af)
  local state, meta = af:load_json()

  if state == nil or meta == nil or meta.no_content then
    return "expected acquire_for_update snapshot state"
  end
  if state.value ~= 1 then
    return ("unexpected snapshot value: %s"):format(tostring(state.value))
  end
  saw_snapshot = true
  return af:update_json({
    value = 2,
    source = "lua-acquire-for-update",
  })
end)

if ok == nil then
  client:close()
  error(("acquire_for_update failed: %s"):format(af_err.message))
end
if not saw_snapshot then
  client:close()
  error("acquire_for_update handler did not observe snapshot")
end

local final, final_meta_or_err = client:get_json({
  key = key,
  public_read = true,
})

if final == nil then
  client:close()
  error(("final get failed: %s"):format(final_meta_or_err.message))
end
if final.value ~= 2 or final.source ~= "lua-acquire-for-update" then
  client:close()
  error(("unexpected final state: %s"):format(lockdc.encode_json(final)))
end

local rollback_ok, rollback_err = client:acquire_for_update({
  key = key,
  owner = owner .. "-rollback",
  ttl_seconds = 60,
  block_seconds = 30,
}, function(af)
  local updated, update_err = af:update_json({
    value = 3,
    source = "lua-acquire-for-update-rolled-back",
  })
  if updated == nil then
    return nil, update_err
  end
  return "intentional acquire_for_update rollback"
end)

if rollback_ok ~= nil then
  client:close()
  error("acquire_for_update rollback case unexpectedly succeeded")
end
if rollback_err == nil or rollback_err.message ~= "intentional acquire_for_update rollback" then
  client:close()
  error(("unexpected rollback error: %s"):format(rollback_err and rollback_err.message or tostring(rollback_err)))
end

local after_rollback, after_rollback_meta_or_err = client:get_json({
  key = key,
  public_read = true,
})

if after_rollback == nil then
  client:close()
  error(("post-rollback get failed: %s"):format(after_rollback_meta_or_err.message))
end
if after_rollback.value ~= 2 or after_rollback.source ~= "lua-acquire-for-update" then
  client:close()
  error(("rollback published partial state: %s"):format(lockdc.encode_json(after_rollback)))
end

local cas_ok, cas_err = client:acquire_for_update({
  key = key,
  owner = owner .. "-cas",
  ttl_seconds = 60,
  block_seconds = 30,
}, function(af)
  return af:update_json({
    value = 4,
    source = "lua-acquire-for-update-stale-cas",
  }, {
    if_version = 999999,
  })
end)

if cas_ok ~= nil then
  client:close()
  error("acquire_for_update stale CAS case unexpectedly succeeded")
end
if cas_err == nil then
  client:close()
  error("acquire_for_update stale CAS did not return an error table")
end
if cas_err.http_status ~= 409 then
  client:close()
  error(("expected preserved 409 status from stale CAS, got %s (%s)"):format(
    tostring(cas_err.http_status),
    cas_err.message or ""
  ))
end
if cas_err.code == lockdc.core.ERR_INVALID then
  client:close()
  error("stale CAS handler error was collapsed to ERR_INVALID")
end

local after_cas, after_cas_meta_or_err = client:get_json({
  key = key,
  public_read = true,
})

if after_cas == nil then
  client:close()
  error(("post-CAS get failed: %s"):format(after_cas_meta_or_err.message))
end
if after_cas.value ~= 2 or after_cas.source ~= "lua-acquire-for-update" then
  client:close()
  error(("stale CAS published partial state: %s"):format(lockdc.encode_json(after_cas)))
end

client:close()

local lockdc = require("lockdc")

local root = assert(os.getenv("LOCKDC_POUCH_ROOT"), "LOCKDC_POUCH_ROOT is required")
local once = os.getenv("LOCKDC_OUTBOX_ONCE") == "1"

local client = assert(lockdc.open({
  endpoints = { "pouch://" .. root .. "?single_writer=false" },
  default_namespace = "outbox-example",
}))
local outbox = assert(client:new_outbox({
  namespace = "outbox-example",
  owner = "outbox-example-producer",
}))
local dispatcher = assert(outbox:dispatcher())

local handlers = {
  ["outbox-example"] = function(job)
    local payload = assert(job:payload_json())
    assert(payload.order_id == 1)
    print(("delivering %s"):format(job:info().effect_key))
    return job:complete()
  end,
}

if once then
  assert(dispatcher:pump({ handlers = handlers, max_jobs = 1, timeout_ms = 5000 }))
else
  assert(dispatcher:run({ handlers = handlers }))
end

dispatcher:stop(5000)
dispatcher:close()
outbox:close()
client:close()

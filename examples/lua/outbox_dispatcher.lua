local lockdc = require("lockdc")

local root = assert(os.getenv("LOCKDC_POUCH_ROOT"), "LOCKDC_POUCH_ROOT is required")
local once = os.getenv("LOCKDC_OUTBOX_ONCE") == "1"

local client = assert(lockdc.open({
  endpoints = { "pouch://" .. root },
  default_namespace = "outbox-example",
  pouch = { single_writer = false },
}))
local outbox = assert(client:new_outbox({
  namespace = "outbox-example",
  owner = "outbox-example-producer",
}))
local dispatcher = assert(outbox:dispatcher())

local handlers = {
  ["outbox-example"] = function(job)
    -- The callback sink receives bounded chunks and never materializes the
    -- payload. A real handler would write each chunk to its foreign transport.
    local bytes = 0
    local streamed, written = job:write_payload({
      write = function(chunk)
        bytes = bytes + #chunk
        return true
      end,
    })
    assert(streamed == nil)
    assert(bytes == written)
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

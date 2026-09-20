local core_dir = assert(os.getenv("LOCKDC_LUA_CORE_DIR"), "LOCKDC_LUA_CORE_DIR is required")

package.cpath = table.concat({
  core_dir .. "/?.so",
  core_dir .. "/?/core.so",
  package.cpath,
}, ";")

local core = require("lockdc.core")

assert(type(core.version_string()) == "string")
local xid = assert(core.xid_new())
assert(type(xid) == "string" and #xid == 20)
assert(xid:match("^[0-9a-v]+$") ~= nil)
local pouch_key = assert(core.pouch_crypto_generate_key())
assert(pouch_key:match("^lc%-pouch%-key%-v1:[A-Za-z0-9_-]+$") ~= nil)
assert(type(assert(core.pouch_crypto_default_key_file())) == "string")

local client, err = core.open({})
assert(client == nil)
assert(type(err) == "table")

local reads = 0
client, err = core.open({
  endpoints = { "https://127.0.0.1:1" },
  client_bundle_source = {
    read = function(_max_bytes)
      reads = reads + 1
      return nil, "lua bundle read failure"
    end,
  },
})
assert(client == nil)
assert(reads > 0)
assert(type(err) == "table")
assert(type(err.message) == "string")
assert(err.message:match("lua bundle read failure") ~= nil)

-- XA participant fields can be supplied by a metatable. They are normalized
-- exactly once before native storage is allocated: a second lookup used to
-- raise after allocation and leak across Lua's longjmp boundary.
local request_client, request_err = core.open({
  endpoints = { "https://127.0.0.1:1" },
  disable_mtls = true,
  timeout_ms = 1,
})
assert(request_client ~= nil, request_err and request_err.message)
local key_lookups = 0
local participant = setmetatable({}, {
  __index = function(_, field)
    if field == "namespace_name" then
      return "lua-xa-parser"
    end
    if field == "key" then
      key_lookups = key_lookups + 1
      assert(key_lookups == 1, "participant key was looked up more than once")
      return "one"
    end
  end,
})
local protected, prepared, prepare_err = pcall(function()
  return request_client:txn_prepare({
    txn_id = assert(core.xid_new()),
    participants = { participant },
  })
end)
assert(protected, prepared)
assert(key_lookups == 1)
assert(prepared == nil)
assert(type(prepare_err) == "table")
request_client:close()

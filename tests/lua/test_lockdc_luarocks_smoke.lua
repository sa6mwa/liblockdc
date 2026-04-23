local lockdc = require("lockdc")

assert(type(lockdc.version_string()) == "string")

local payload = lockdc.encode_json({
  kind = "luarocks-smoke",
  answer = 42,
  ok = true,
})

local decoded = lockdc.decode_json(payload)

assert(decoded.kind == "luarocks-smoke")
assert(decoded.answer == 42)
assert(decoded.ok == true)

local client, err = lockdc.open({})

assert(client == nil)
assert(type(err) == "table")
assert(type(err.message) == "string")

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

assert(lockdc.encode_json(lockdc.json_null) == "null")
assert(lockdc.decode_json("null") == lockdc.json_null)

local with_null = lockdc.decode_json('{"value":null,"items":[null]}')
assert(with_null.value == lockdc.json_null)
assert(with_null.items[1] == lockdc.json_null)

local client, err = lockdc.open({})

assert(client == nil)
assert(type(err) == "table")
assert(type(err.message) == "string")

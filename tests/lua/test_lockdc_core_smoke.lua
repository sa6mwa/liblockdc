local core_dir = assert(os.getenv("LOCKDC_LUA_CORE_DIR"), "LOCKDC_LUA_CORE_DIR is required")

package.cpath = table.concat({
  core_dir .. "/?.so",
  core_dir .. "/?/core.so",
  package.cpath,
}, ";")

local core = require("lockdc.core")

assert(type(core.version_string()) == "string")

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

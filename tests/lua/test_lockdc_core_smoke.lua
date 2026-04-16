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

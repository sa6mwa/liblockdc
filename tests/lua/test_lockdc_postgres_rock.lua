local lockdc = require("lockdc")
local postgres = require("lockdc.postgres")
assert(postgres.register("installed-postgres"))
assert(postgres.OUTCOME_COMMITTED == lockdc.XA_OUTCOME_COMMITTED)
local client = assert(lockdc.open({ endpoints = { "pouch://" .. assert(os.getenv("LOCKDC_TEST_POUCH_ROOT")) },
  pouch = { single_writer = true } }))
local store = assert(client:xa_store())
local id = assert(lockdc.xid_new())
-- Opening copies configuration but performs no native I/O. An installed rock
-- must make the driver available to both journal and coordinator receivers.
local resource = assert(store:resource("installed-postgres", {
  resource_id = id, connection_info = "host=/does-not-exist dbname=xa user=xa" }))
assert(resource:identity().resource_id == id)
assert(resource:close_blocking())
assert(store:close())
client:close()
print("Installed PostgreSQL Lua rock registered the public XA driver")

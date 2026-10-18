local lockdc = require("lockdc")
assert(lockdc.allocator_tracking_configure(false) == nil,
  "the E2E runner must configure tracking before the script")
-- A successful child that emits a report must still fail package validation.
-- This is a diagnostic fixture, not an intentionally leaked dependency object.
local output = os.getenv("LOCKDC_TEST_REPORT_STREAM") == "stdout" and io.stdout or io.stderr
output:write("liblockdc allocator: captured-report-regression\n")

#include "lc_log.h"

/* Keep the owner in an unloadable module so a reachable global cannot hide
 * a missing release from process-exit leak checks. */
int lc_log_unload_probe(const char *mode) {
  pslog_logger *logger = lc_log_noop_logger();
  (void)mode;
  return logger != NULL ? 0 : 1;
}

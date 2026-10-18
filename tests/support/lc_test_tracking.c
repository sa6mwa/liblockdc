#include "lc/lc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Test executables select tracking before any SDK use, including readiness
 * probes and the shipped C examples exercised by E2E. Production stays off. */
static void lc_test_tracking_start(void) __attribute__((constructor));
static void lc_test_tracking_start(void) {
  const char *enabled = getenv("LOCKDC_TEST_ALLOCATOR_TRACKING");
  if (enabled != NULL && strcmp(enabled, "1") == 0 &&
      lc_allocator_tracking_configure(1, NULL) != LC_OK) {
    fprintf(stderr, "cannot enable liblockdc test allocator tracking\n");
    exit(1);
  }
}

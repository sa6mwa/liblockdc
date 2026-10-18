#include "lc/lc.h"
#include <string.h>

int main(int argc, char **argv) {
  char *endpoint = NULL;
  lc_error error;
  lc_error_init(&error);
  /* A second configuration must fail: the E2E startup hook already selected
   * tracking, before this main function was entered. */
  if (lc_allocator_tracking_configure(0, &error) != LC_ERR_INVALID)
    return 2;
  lc_error_cleanup(&error);
  if (lc_pouch_endpoint_build("/test", NULL, 0U, &endpoint, &error) != LC_OK)
    return 1;
  if (argc == 2 && strcmp(argv[1], "clean") == 0)
    lc_pouch_endpoint_free(endpoint);
  lc_error_cleanup(&error);
  return 0;
}

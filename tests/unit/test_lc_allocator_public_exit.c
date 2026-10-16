#include "lc/lc.h"

#include <string.h>

int main(int argc, char **argv) {
  lc_error error;
  char *endpoint;

  if (argc != 3 ||
      (strcmp(argv[1], "clean") != 0 && strcmp(argv[1], "leak") != 0))
    return 2;
  lc_error_init(&error);
  if ((strcmp(argv[2], "1") != 0 && strcmp(argv[2], "0") != 0) ||
      lc_allocator_tracking_configure(strcmp(argv[2], "1") == 0, &error) !=
          LC_OK)
    return 1;
  if (lc_pouch_endpoint_build("/test", NULL, 0U, &endpoint, &error) != LC_OK)
    return 1;
  if (strcmp(endpoint, "pouch:///test") != 0)
    return 1;
  if (strcmp(argv[1], "clean") == 0)
    lc_pouch_endpoint_free(endpoint);
  lc_error_cleanup(&error);
  return 0;
}

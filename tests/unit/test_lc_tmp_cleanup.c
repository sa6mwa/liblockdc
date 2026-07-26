#include "../support/lc_test_tmp.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TMP_CLEANUP_PREFIX "/tmp/liblockdc-unit-tmp-cleanup-"

int main(void) {
  char template_path[] = TMP_CLEANUP_PREFIX "XXXXXX";
  char root[512];
  const char *path_file;
  FILE *fp;

  path_file = getenv("LOCKDC_TMP_CLEANUP_PATH_FILE");
  if (path_file == NULL || path_file[0] == '\0') {
    return 2;
  }
  if (!lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                          TMP_CLEANUP_PREFIX)) {
    return 3;
  }
  fp = fopen(path_file, "w");
  if (fp == NULL) {
    return 4;
  }
  if (fprintf(fp, "%s\n", root) < 0) {
    (void)fclose(fp);
    return 5;
  }
  if (fclose(fp) != 0) {
    return 6;
  }

#ifdef SIGTERM
  (void)raise(SIGTERM);
#endif
  return 7;
}

#include "../support/lc_test_tmp.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#define TMP_CLEANUP_PREFIX "/tmp/liblockdc-unit-tmp-cleanup-"
#define TMP_AUTO_CLEANUP_PREFIX "/tmp/liblockdc-unit-tmp-autocleanup-"

static int path_exists(const char *path) {
  struct stat st;

  return stat(path, &st) == 0;
}

static int run_auto_stale_cleanup_probe(void) {
  char stale_root[] = TMP_AUTO_CLEANUP_PREFIX "abandoned";
  char stale_child[] = TMP_AUTO_CLEANUP_PREFIX "abandoned/child";
  char template_path[] = TMP_AUTO_CLEANUP_PREFIX "fresh-XXXXXX";
  char root[512];

  lc_test_tmp_cleanup_path(stale_root, TMP_AUTO_CLEANUP_PREFIX);
  if (mkdir(stale_root, 0700) != 0) {
    return 10;
  }
  if (mkdir(stale_child, 0700) != 0) {
    lc_test_tmp_cleanup_path(stale_root, TMP_AUTO_CLEANUP_PREFIX);
    return 11;
  }
  if (!lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                          TMP_AUTO_CLEANUP_PREFIX)) {
    lc_test_tmp_cleanup_path(stale_root, TMP_AUTO_CLEANUP_PREFIX);
    return 12;
  }
  if (path_exists(stale_root)) {
    lc_test_tmp_cleanup_path(root, TMP_AUTO_CLEANUP_PREFIX);
    lc_test_tmp_cleanup_path(stale_root, TMP_AUTO_CLEANUP_PREFIX);
    return 13;
  }
  lc_test_tmp_cleanup_path(root, TMP_AUTO_CLEANUP_PREFIX);
  return 0;
}

int main(void) {
  char template_path[] = TMP_CLEANUP_PREFIX "XXXXXX";
  char root[512];
  const char *path_file;
  const char *mode;
  FILE *fp;

  mode = getenv("LOCKDC_TMP_CLEANUP_MODE");
  if (mode != NULL && strcmp(mode, "auto-stale") == 0) {
    return run_auto_stale_cleanup_probe();
  }

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

#include "../support/lc_test_tmp.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#define TMP_CLEANUP_PREFIX "/tmp/liblockdc-unit-tmp-cleanup-"
#define TMP_AUTO_CLEANUP_PREFIX "/tmp/liblockdc-unit-tmp-autocleanup-"
#define TMP_GLOBAL_STALE_PREFIX "/tmp/liblockdc-unit-tmp-global-stale-"
#define TMP_GLOBAL_LIVE_PREFIX "/tmp/liblockdc-unit-tmp-global-live-"

static int path_exists(const char *path) {
  struct stat st;

  return stat(path, &st) == 0;
}

static int run_auto_stale_cleanup_probe(void) {
  char stale_root[] = TMP_AUTO_CLEANUP_PREFIX "abandoned";
  char stale_child[] = TMP_AUTO_CLEANUP_PREFIX "abandoned/child";
  char global_stale_root[] = TMP_GLOBAL_STALE_PREFIX "abandoned";
  char global_stale_child[] = TMP_GLOBAL_STALE_PREFIX "abandoned/child";
  char template_path[] = TMP_AUTO_CLEANUP_PREFIX "fresh-XXXXXX";
  char live_template[] = TMP_AUTO_CLEANUP_PREFIX "live-XXXXXX";
  char root[512];
  char live_root[512];

  lc_test_tmp_cleanup_path(stale_root, TMP_AUTO_CLEANUP_PREFIX);
  lc_test_tmp_cleanup_path(global_stale_root, TMP_GLOBAL_STALE_PREFIX);
  if (mkdir(stale_root, 0700) != 0) {
    return 10;
  }
  if (mkdir(stale_child, 0700) != 0) {
    lc_test_tmp_cleanup_path(stale_root, TMP_AUTO_CLEANUP_PREFIX);
    return 11;
  }
  if (mkdir(global_stale_root, 0700) != 0) {
    lc_test_tmp_cleanup_path(stale_root, TMP_AUTO_CLEANUP_PREFIX);
    return 12;
  }
  if (mkdir(global_stale_child, 0700) != 0) {
    lc_test_tmp_cleanup_path(stale_root, TMP_AUTO_CLEANUP_PREFIX);
    lc_test_tmp_cleanup_path(global_stale_root, TMP_GLOBAL_STALE_PREFIX);
    return 13;
  }
  if (!lc_test_tmp_mkdtemp(template_path, root, sizeof(root),
                           TMP_AUTO_CLEANUP_PREFIX)) {
    lc_test_tmp_cleanup_path(stale_root, TMP_AUTO_CLEANUP_PREFIX);
    lc_test_tmp_cleanup_path(global_stale_root, TMP_GLOBAL_STALE_PREFIX);
    return 14;
  }
  if (path_exists(stale_root)) {
    lc_test_tmp_cleanup_path(root, TMP_AUTO_CLEANUP_PREFIX);
    lc_test_tmp_cleanup_path(stale_root, TMP_AUTO_CLEANUP_PREFIX);
    lc_test_tmp_cleanup_path(global_stale_root, TMP_GLOBAL_STALE_PREFIX);
    return 15;
  }
  if (path_exists(global_stale_root)) {
    lc_test_tmp_cleanup_path(root, TMP_AUTO_CLEANUP_PREFIX);
    lc_test_tmp_cleanup_path(global_stale_root, TMP_GLOBAL_STALE_PREFIX);
    return 16;
  }
  if (!lc_test_tmp_mkdtemp(live_template, live_root, sizeof(live_root),
                           TMP_AUTO_CLEANUP_PREFIX)) {
    lc_test_tmp_cleanup_path(root, TMP_AUTO_CLEANUP_PREFIX);
    return 17;
  }
  lc_test_tmp_cleanup_stale("/tmp", "liblockdc-unit-tmp-autocleanup-",
                            TMP_AUTO_CLEANUP_PREFIX);
  if (!path_exists(live_root)) {
    lc_test_tmp_cleanup_path(root, TMP_AUTO_CLEANUP_PREFIX);
    return 18;
  }
  lc_test_tmp_cleanup_path(live_root, TMP_AUTO_CLEANUP_PREFIX);
  lc_test_tmp_cleanup_path(root, TMP_AUTO_CLEANUP_PREFIX);
  return 0;
}

static int run_explicit_global_stale_cleanup_probe(void) {
  char stale_root[] = TMP_GLOBAL_STALE_PREFIX "explicit-abandoned";
  char stale_child[] = TMP_GLOBAL_STALE_PREFIX "explicit-abandoned/child";
  char live_root[] = TMP_GLOBAL_LIVE_PREFIX "explicit-live";
  char live_child[] = TMP_GLOBAL_LIVE_PREFIX "explicit-live/child";

  lc_test_tmp_cleanup_path(stale_root, TMP_GLOBAL_STALE_PREFIX);
  lc_test_tmp_cleanup_path(live_root, TMP_GLOBAL_LIVE_PREFIX);
  if (mkdir(stale_root, 0700) != 0) {
    return 30;
  }
  if (mkdir(stale_child, 0700) != 0) {
    lc_test_tmp_cleanup_path(stale_root, TMP_GLOBAL_STALE_PREFIX);
    return 31;
  }
  if (mkdir(live_root, 0700) != 0) {
    lc_test_tmp_cleanup_path(stale_root, TMP_GLOBAL_STALE_PREFIX);
    return 32;
  }
  if (mkdir(live_child, 0700) != 0) {
    lc_test_tmp_cleanup_path(stale_root, TMP_GLOBAL_STALE_PREFIX);
    lc_test_tmp_cleanup_path(live_root, TMP_GLOBAL_LIVE_PREFIX);
    return 33;
  }
  if (!lc_test_tmp_track_path(live_root, TMP_GLOBAL_LIVE_PREFIX)) {
    lc_test_tmp_cleanup_path(stale_root, TMP_GLOBAL_STALE_PREFIX);
    lc_test_tmp_cleanup_path(live_root, TMP_GLOBAL_LIVE_PREFIX);
    return 34;
  }

  lc_test_tmp_cleanup_stale("/tmp", "liblockdc-unit-tmp-autocleanup-",
                            TMP_AUTO_CLEANUP_PREFIX);
  if (path_exists(stale_root)) {
    lc_test_tmp_cleanup_path(stale_root, TMP_GLOBAL_STALE_PREFIX);
    lc_test_tmp_cleanup_path(live_root, TMP_GLOBAL_LIVE_PREFIX);
    return 35;
  }
  if (!path_exists(live_root)) {
    return 36;
  }
  lc_test_tmp_cleanup_path(live_root, TMP_GLOBAL_LIVE_PREFIX);
  return 0;
}

static int run_orphan_owner_marker_cleanup_probe(void) {
  char missing_root[] = TMP_AUTO_CLEANUP_PREFIX "orphan-owner";
  char marker[512];
  FILE *fp;
  int written;

  written = snprintf(marker, sizeof(marker), "%s.liblockdc-test-tmp-owner",
                     missing_root);
  if (written < 0 || (size_t)written >= sizeof(marker)) {
    return 50;
  }
  lc_test_tmp_cleanup_path(missing_root, TMP_AUTO_CLEANUP_PREFIX);
  if (path_exists(marker)) {
    return 51;
  }
  if (lc_test_tmp_track_path(missing_root, TMP_AUTO_CLEANUP_PREFIX)) {
    lc_test_tmp_cleanup_path(missing_root, TMP_AUTO_CLEANUP_PREFIX);
    return 52;
  }
  if (path_exists(marker)) {
    lc_test_tmp_cleanup_path(missing_root, TMP_AUTO_CLEANUP_PREFIX);
    return 53;
  }
  fp = fopen(marker, "w");
  if (fp == NULL) {
    return 54;
  }
  if (fprintf(fp, "1\n") < 0) {
    (void)fclose(fp);
    lc_test_tmp_cleanup_path(missing_root, TMP_AUTO_CLEANUP_PREFIX);
    return 55;
  }
  if (fclose(fp) != 0) {
    lc_test_tmp_cleanup_path(missing_root, TMP_AUTO_CLEANUP_PREFIX);
    return 56;
  }
  lc_test_tmp_cleanup_stale("/tmp", "liblockdc-unit-tmp-autocleanup-",
                            TMP_AUTO_CLEANUP_PREFIX);
  if (path_exists(marker)) {
    lc_test_tmp_cleanup_path(missing_root, TMP_AUTO_CLEANUP_PREFIX);
    return 57;
  }
  return 0;
}

int main(void) {
  char template_path[] = TMP_CLEANUP_PREFIX "XXXXXX";
  char root[512];
  const char *path_file;
  const char *mode;
  const char *signal_name;
  FILE *fp;

  mode = getenv("LOCKDC_TMP_CLEANUP_MODE");
  if (mode != NULL && strcmp(mode, "auto-stale") == 0) {
    return run_auto_stale_cleanup_probe();
  }
  if (mode != NULL && strcmp(mode, "explicit-global-stale") == 0) {
    return run_explicit_global_stale_cleanup_probe();
  }
  if (mode != NULL && strcmp(mode, "orphan-owner-marker") == 0) {
    return run_orphan_owner_marker_cleanup_probe();
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

  signal_name = getenv("LOCKDC_TMP_CLEANUP_SIGNAL");
  if (signal_name != NULL && strcmp(signal_name, "ABRT") == 0) {
#ifdef SIGABRT
    (void)raise(SIGABRT);
#endif
  } else {
#ifdef SIGTERM
    (void)raise(SIGTERM);
#endif
  }
  return 7;
}

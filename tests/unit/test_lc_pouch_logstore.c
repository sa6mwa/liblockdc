#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include <cmocka.h>

#include "lc_pouch_logstore.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

typedef struct fsync_capture {
  unsigned long calls;
  int fail;
} fsync_capture;

static int capture_fsync(void *context, int fd) {
  fsync_capture *capture;

  (void)fd;
  capture = (fsync_capture *)context;
  capture->calls++;
  if (capture->fail) {
    errno = EIO;
    return -1;
  }
  return 0;
}

static void test_root_path(char *buffer, size_t buffer_size,
                           const char *name) {
  (void)snprintf(buffer, buffer_size, "/tmp/liblockdc-pouch-logstore-%ld-%s",
                 (long)getpid(), name);
}

static int path_exists(const char *path) {
  struct stat st;

  return stat(path, &st) == 0;
}

static void cleanup_default_logstore(const char *root) {
  char path[512];

  (void)snprintf(path, sizeof(path),
                 "%s/default/logstore/segments/seg-0000000000000002.log",
                 root);
  (void)unlink(path);
  (void)snprintf(path, sizeof(path),
                 "%s/default/logstore/segments/seg-0000000000000001.log",
                 root);
  (void)unlink(path);
  (void)snprintf(path, sizeof(path), "%s/default/logstore/manifest/manifest.log",
                 root);
  (void)unlink(path);
  (void)snprintf(path, sizeof(path), "%s/default/logstore/queue-notify", root);
  (void)rmdir(path);
  (void)snprintf(path, sizeof(path), "%s/default/logstore/markers", root);
  (void)rmdir(path);
  (void)snprintf(path, sizeof(path), "%s/default/logstore/snapshots", root);
  (void)rmdir(path);
  (void)snprintf(path, sizeof(path), "%s/default/logstore/segments", root);
  (void)rmdir(path);
  (void)snprintf(path, sizeof(path), "%s/default/logstore/manifest", root);
  (void)rmdir(path);
  (void)snprintf(path, sizeof(path), "%s/default/logstore", root);
  (void)rmdir(path);
  (void)snprintf(path, sizeof(path), "%s/default", root);
  (void)rmdir(path);
  (void)rmdir(root);
}

static int write_full(int fd, const unsigned char *bytes, size_t count) {
  ssize_t written;

  while (count > 0U) {
    written = write(fd, bytes, count);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      return 0;
    }
    if (written == 0) {
      errno = EIO;
      return 0;
    }
    bytes += written;
    count -= (size_t)written;
  }
  return 1;
}

static void fill_segment_to_seal(const char *path) {
  unsigned char bytes[1024];
  int fd;
  size_t index;

  memset(bytes, 'x', sizeof(bytes));
  fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0666);
  assert_true(fd >= 0);
  for (index = 0U; index < 64U; ++index) {
    assert_true(write_full(fd, bytes, sizeof(bytes)));
  }
  assert_int_equal(close(fd), 0);
}

static void test_logstore_parses_segment_and_snapshot_names(void **state) {
  unsigned long number;

  (void)state;
  number = 0UL;
  assert_true(lc_pouch_logstore_segment_name_parse(
      "seg-0000000000000042.log", &number));
  assert_int_equal(number, 42UL);
  assert_false(lc_pouch_logstore_segment_name_parse(
      "seg-0000000000000000.log", NULL));
  assert_false(lc_pouch_logstore_segment_name_parse(
      "snap-0000000000000042.log", NULL));

  number = 0UL;
  assert_true(lc_pouch_logstore_snapshot_name_parse(
      "snap-0000000000000007.log", &number));
  assert_int_equal(number, 7UL);
  assert_false(lc_pouch_logstore_snapshot_name_parse(
      "snap-0000000000000000.log", NULL));
  assert_false(lc_pouch_logstore_snapshot_name_parse(
      "seg-0000000000000007.log", NULL));
}

static void test_logstore_creates_namespace_manifest_and_segment(void **state) {
  char root[256];
  char path[512];
  lc_pouch_logstore logstore;
  fsync_capture fsyncs;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "create");
  cleanup_default_logstore(root);
  assert_int_equal(mkdir(root, 0777), 0);
  memset(&fsyncs, 0, sizeof(fsyncs));
  memset(&error, 0, sizeof(error));
  lc_pouch_logstore_init(&logstore, NULL, root, capture_fsync, &fsyncs);

  rc = lc_pouch_logstore_ensure_namespace(&logstore, "default", &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(fsyncs.calls, 1UL);

  (void)snprintf(path, sizeof(path),
                 "%s/default/logstore/manifest/manifest.log", root);
  assert_true(path_exists(path));
  (void)snprintf(path, sizeof(path),
                 "%s/default/logstore/segments/seg-0000000000000001.log",
                 root);
  assert_true(path_exists(path));
  lc_error_cleanup(&error);
  cleanup_default_logstore(root);
}

static void test_logstore_rolls_sealed_active_segment(void **state) {
  char root[256];
  char first_path[512];
  lc_pouch_logstore logstore;
  fsync_capture fsyncs;
  lc_error error;
  char *active_path;
  int active_fd;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "rollover");
  cleanup_default_logstore(root);
  assert_int_equal(mkdir(root, 0777), 0);
  memset(&fsyncs, 0, sizeof(fsyncs));
  memset(&error, 0, sizeof(error));
  lc_pouch_logstore_init(&logstore, NULL, root, capture_fsync, &fsyncs);

  rc = lc_pouch_logstore_ensure_namespace(&logstore, "default", &error);
  assert_int_equal(rc, LC_OK);
  (void)snprintf(first_path, sizeof(first_path),
                 "%s/default/logstore/segments/seg-0000000000000001.log",
                 root);
  fill_segment_to_seal(first_path);

  active_path = NULL;
  active_fd = -1;
  rc = lc_pouch_logstore_open_active_segment_for_append(
      &logstore, "default", &active_path, &active_fd, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(active_fd >= 0);
  assert_non_null(active_path);
  assert_non_null(strstr(active_path, "seg-0000000000000002.log"));
  assert_int_equal(close(active_fd), 0);
  lc_pouch_free(NULL, active_path);
  assert_true(fsyncs.calls >= 3UL);

  lc_error_cleanup(&error);
  cleanup_default_logstore(root);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_logstore_parses_segment_and_snapshot_names),
      cmocka_unit_test(test_logstore_creates_namespace_manifest_and_segment),
      cmocka_unit_test(test_logstore_rolls_sealed_active_segment),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}

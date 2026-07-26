#include "lc_test_tmp.h"

#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define LC_TEST_TMP_MAX_TRACKED 512U
#define LC_TEST_TMP_PATH_MAX 1024U

static char lc_test_tmp_tracked[LC_TEST_TMP_MAX_TRACKED][LC_TEST_TMP_PATH_MAX];
static size_t lc_test_tmp_tracked_count;
static int lc_test_tmp_atexit_installed;

static int lc_test_tmp_has_prefix(const char *value, const char *prefix) {
  return value != NULL && prefix != NULL &&
         strncmp(value, prefix, strlen(prefix)) == 0;
}

static void lc_test_tmp_remove_tree(const char *path) {
  DIR *dir;
  struct dirent *entry;
  struct stat st;

  if (path == NULL || path[0] == '\0') {
    return;
  }
  if (lstat(path, &st) != 0) {
    (void)unlink(path);
    return;
  }
  if (!S_ISDIR(st.st_mode)) {
    (void)unlink(path);
    return;
  }
  dir = opendir(path);
  if (dir == NULL) {
    (void)unlink(path);
    return;
  }
  while ((entry = readdir(dir)) != NULL) {
    char child[LC_TEST_TMP_PATH_MAX];
    int written;

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    written = snprintf(child, sizeof(child), "%s/%s", path, entry->d_name);
    if (written < 0 || (size_t)written >= sizeof(child)) {
      continue;
    }
    lc_test_tmp_remove_tree(child);
  }
  (void)closedir(dir);
  (void)rmdir(path);
}

static void lc_test_tmp_cleanup_tracked(void) {
  size_t index;

  index = lc_test_tmp_tracked_count;
  while (index > 0U) {
    --index;
    lc_test_tmp_remove_tree(lc_test_tmp_tracked[index]);
    lc_test_tmp_tracked[index][0] = '\0';
  }
  lc_test_tmp_tracked_count = 0U;
}

static int lc_test_tmp_install_atexit(void) {
  if (lc_test_tmp_atexit_installed) {
    return 1;
  }
  if (atexit(lc_test_tmp_cleanup_tracked) != 0) {
    return 0;
  }
  lc_test_tmp_atexit_installed = 1;
  return 1;
}

int lc_test_tmp_track_path(const char *path, const char *allowed_prefix) {
  size_t index;
  int written;

  if (!lc_test_tmp_has_prefix(path, allowed_prefix)) {
    return 0;
  }
  if (!lc_test_tmp_install_atexit()) {
    return 0;
  }
  for (index = 0U; index < lc_test_tmp_tracked_count; ++index) {
    if (strcmp(lc_test_tmp_tracked[index], path) == 0) {
      return 1;
    }
  }
  if (lc_test_tmp_tracked_count >= LC_TEST_TMP_MAX_TRACKED) {
    return 0;
  }
  written = snprintf(lc_test_tmp_tracked[lc_test_tmp_tracked_count],
                     sizeof(lc_test_tmp_tracked[lc_test_tmp_tracked_count]),
                     "%s", path);
  if (written < 0 ||
      (size_t)written >=
          sizeof(lc_test_tmp_tracked[lc_test_tmp_tracked_count])) {
    return 0;
  }
  ++lc_test_tmp_tracked_count;
  return 1;
}

void lc_test_tmp_untrack_path(const char *path) {
  size_t index;

  if (path == NULL) {
    return;
  }
  for (index = 0U; index < lc_test_tmp_tracked_count; ++index) {
    if (strcmp(lc_test_tmp_tracked[index], path) != 0) {
      continue;
    }
    while (index + 1U < lc_test_tmp_tracked_count) {
      memcpy(lc_test_tmp_tracked[index], lc_test_tmp_tracked[index + 1U],
             sizeof(lc_test_tmp_tracked[index]));
      ++index;
    }
    --lc_test_tmp_tracked_count;
    lc_test_tmp_tracked[lc_test_tmp_tracked_count][0] = '\0';
    return;
  }
}

int lc_test_tmp_mkdtemp(char *template_path, char *out, size_t out_size,
                        const char *allowed_prefix) {
  char *created;
  int written;

  if (!lc_test_tmp_has_prefix(template_path, allowed_prefix)) {
    return 0;
  }
  created = mkdtemp(template_path);
  if (created == NULL) {
    return 0;
  }
  if (!lc_test_tmp_track_path(created, allowed_prefix)) {
    lc_test_tmp_remove_tree(created);
    return 0;
  }
  written = snprintf(out, out_size, "%s", created);
  if (written < 0 || (size_t)written >= out_size) {
    lc_test_tmp_cleanup_path(created, allowed_prefix);
    return 0;
  }
  return 1;
}

int lc_test_tmp_mkstemp(char *template_path, const char *allowed_prefix) {
  int fd;

  if (!lc_test_tmp_has_prefix(template_path, allowed_prefix)) {
    return -1;
  }
  fd = mkstemp(template_path);
  if (fd < 0) {
    return -1;
  }
  if (!lc_test_tmp_track_path(template_path, allowed_prefix)) {
    (void)close(fd);
    lc_test_tmp_remove_tree(template_path);
    return -1;
  }
  return fd;
}

void lc_test_tmp_cleanup_path(const char *path, const char *allowed_prefix) {
  if (!lc_test_tmp_has_prefix(path, allowed_prefix)) {
    return;
  }
  lc_test_tmp_remove_tree(path);
  lc_test_tmp_untrack_path(path);
}

void lc_test_tmp_cleanup_stale(const char *parent_dir,
                               const char *name_prefix,
                               const char *allowed_prefix) {
  DIR *dir;
  struct dirent *entry;

  dir = opendir(parent_dir);
  if (dir == NULL) {
    return;
  }
  while ((entry = readdir(dir)) != NULL) {
    char path[LC_TEST_TMP_PATH_MAX];
    int written;

    if (!lc_test_tmp_has_prefix(entry->d_name, name_prefix)) {
      continue;
    }
    written = snprintf(path, sizeof(path), "%s/%s", parent_dir, entry->d_name);
    if (written < 0 || (size_t)written >= sizeof(path)) {
      continue;
    }
    lc_test_tmp_cleanup_path(path, allowed_prefix);
  }
  (void)closedir(dir);
}

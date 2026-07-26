#include "lc_test_tmp.h"

#include <dirent.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define LC_TEST_TMP_MAX_TRACKED 512U
#define LC_TEST_TMP_MAX_SWEEP_PREFIXES 64U
#define LC_TEST_TMP_PATH_MAX 1024U
#define LC_TEST_TMP_DEFAULT_AUTO_STALE_SECONDS 3600L

static char lc_test_tmp_tracked[LC_TEST_TMP_MAX_TRACKED][LC_TEST_TMP_PATH_MAX];
static char lc_test_tmp_swept[LC_TEST_TMP_MAX_SWEEP_PREFIXES]
                             [LC_TEST_TMP_PATH_MAX];
static size_t lc_test_tmp_tracked_count;
static size_t lc_test_tmp_swept_count;
static int lc_test_tmp_atexit_installed;
static int lc_test_tmp_signal_handlers_installed;

static int lc_test_tmp_has_prefix(const char *value, const char *prefix) {
  return value != NULL && prefix != NULL &&
         strncmp(value, prefix, strlen(prefix)) == 0;
}

static long lc_test_tmp_auto_stale_seconds(void) {
  const char *value;
  char *end;
  long parsed;

  value = getenv("LOCKDC_TMP_AUTO_CLEANUP_STALE_SECONDS");
  if (value == NULL || value[0] == '\0') {
    return LC_TEST_TMP_DEFAULT_AUTO_STALE_SECONDS;
  }
  end = NULL;
  parsed = strtol(value, &end, 10);
  if (end == value || (end != NULL && *end != '\0')) {
    return LC_TEST_TMP_DEFAULT_AUTO_STALE_SECONDS;
  }
  return parsed;
}

static int lc_test_tmp_is_old_enough(const char *path, long min_age_seconds) {
  struct stat st;
  time_t now;

  if (min_age_seconds < 0L) {
    return 0;
  }
  if (min_age_seconds == 0L) {
    return 1;
  }
  if (lstat(path, &st) != 0) {
    return 1;
  }
  now = time(NULL);
  if (now == (time_t)-1) {
    return 0;
  }
  return st.st_mtime <= now - (time_t)min_age_seconds;
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

static void lc_test_tmp_signal_cleanup(int signo) {
  lc_test_tmp_cleanup_tracked();
  (void)signal(signo, SIG_DFL);
  (void)raise(signo);
}

static void lc_test_tmp_install_one_signal_handler(int signo) {
  (void)signal(signo, lc_test_tmp_signal_cleanup);
}

static void lc_test_tmp_install_signal_handlers(void) {
  if (lc_test_tmp_signal_handlers_installed) {
    return;
  }
#ifdef SIGHUP
  lc_test_tmp_install_one_signal_handler(SIGHUP);
#endif
#ifdef SIGINT
  lc_test_tmp_install_one_signal_handler(SIGINT);
#endif
#ifdef SIGQUIT
  lc_test_tmp_install_one_signal_handler(SIGQUIT);
#endif
#ifdef SIGTERM
  lc_test_tmp_install_one_signal_handler(SIGTERM);
#endif
  lc_test_tmp_signal_handlers_installed = 1;
}

static int lc_test_tmp_install_atexit(void) {
  if (lc_test_tmp_atexit_installed) {
    return 1;
  }
  if (atexit(lc_test_tmp_cleanup_tracked) != 0) {
    return 0;
  }
  lc_test_tmp_atexit_installed = 1;
  lc_test_tmp_install_signal_handlers();
  return 1;
}

static int lc_test_tmp_template_parts(const char *template_path, char *parent,
                                      size_t parent_size, char *name_prefix,
                                      size_t name_prefix_size) {
  const char *slash;
  const char *name;
  size_t parent_len;
  size_t name_len;
  int written;

  if (template_path == NULL || parent_size == 0U ||
      name_prefix_size == 0U) {
    return 0;
  }
  slash = strrchr(template_path, '/');
  if (slash == NULL) {
    written = snprintf(parent, parent_size, ".");
    name = template_path;
  } else {
    parent_len = (size_t)(slash - template_path);
    if (parent_len == 0U) {
      written = snprintf(parent, parent_size, "/");
    } else {
      if (parent_len >= parent_size) {
        return 0;
      }
      memcpy(parent, template_path, parent_len);
      parent[parent_len] = '\0';
      written = 0;
    }
    name = slash + 1;
  }
  if (written < 0 || (size_t)written >= parent_size) {
    return 0;
  }
  name_len = strlen(name);
  while (name_len > 0U && name[name_len - 1U] == 'X') {
    --name_len;
  }
  if (name_len == 0U || name_len >= name_prefix_size) {
    return 0;
  }
  memcpy(name_prefix, name, name_len);
  name_prefix[name_len] = '\0';
  return 1;
}

static int lc_test_tmp_prefix_parts(const char *allowed_prefix, char *parent,
                                    size_t parent_size, char *name_prefix,
                                    size_t name_prefix_size) {
  const char *slash;
  const char *name;
  size_t parent_len;
  size_t name_len;
  int written;

  if (allowed_prefix == NULL || allowed_prefix[0] == '\0' ||
      parent_size == 0U || name_prefix_size == 0U) {
    return 0;
  }
  slash = strrchr(allowed_prefix, '/');
  if (slash == NULL) {
    written = snprintf(parent, parent_size, ".");
    name = allowed_prefix;
  } else {
    parent_len = (size_t)(slash - allowed_prefix);
    if (parent_len == 0U) {
      written = snprintf(parent, parent_size, "/");
    } else {
      if (parent_len >= parent_size) {
        return 0;
      }
      memcpy(parent, allowed_prefix, parent_len);
      parent[parent_len] = '\0';
      written = 0;
    }
    name = slash + 1;
  }
  if (written < 0 || (size_t)written >= parent_size) {
    return 0;
  }
  name_len = strlen(name);
  if (name_len == 0U || name_len >= name_prefix_size) {
    return 0;
  }
  memcpy(name_prefix, name, name_len + 1U);
  return 1;
}

static int lc_test_tmp_mark_swept(const char *parent_dir,
                                  const char *name_prefix,
                                  const char *allowed_prefix) {
  char key[LC_TEST_TMP_PATH_MAX];
  size_t index;
  int written;

  written = snprintf(key, sizeof(key), "%s\n%s\n%s", parent_dir, name_prefix,
                     allowed_prefix);
  if (written < 0 || (size_t)written >= sizeof(key)) {
    return 0;
  }
  for (index = 0U; index < lc_test_tmp_swept_count; ++index) {
    if (strcmp(lc_test_tmp_swept[index], key) == 0) {
      return 0;
    }
  }
  if (lc_test_tmp_swept_count >= LC_TEST_TMP_MAX_SWEEP_PREFIXES) {
    return 0;
  }
  memcpy(lc_test_tmp_swept[lc_test_tmp_swept_count], key,
         (size_t)written + 1U);
  ++lc_test_tmp_swept_count;
  return 1;
}

static void lc_test_tmp_cleanup_stale_for_template(const char *template_path,
                                                   const char *allowed_prefix) {
  char parent_dir[LC_TEST_TMP_PATH_MAX];
  char name_prefix[LC_TEST_TMP_PATH_MAX];

  if (!lc_test_tmp_prefix_parts(allowed_prefix, parent_dir, sizeof(parent_dir),
                                name_prefix, sizeof(name_prefix)) &&
      !lc_test_tmp_template_parts(template_path, parent_dir, sizeof(parent_dir),
                                  name_prefix, sizeof(name_prefix))) {
    return;
  }
  if (!lc_test_tmp_mark_swept(parent_dir, name_prefix, allowed_prefix)) {
    return;
  }
  lc_test_tmp_cleanup_stale_older_than(
      parent_dir, name_prefix, allowed_prefix,
      lc_test_tmp_auto_stale_seconds());
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
  lc_test_tmp_cleanup_stale_for_template(template_path, allowed_prefix);
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
  lc_test_tmp_cleanup_stale_for_template(template_path, allowed_prefix);
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
  lc_test_tmp_cleanup_stale_older_than(parent_dir, name_prefix, allowed_prefix,
                                       0L);
}

void lc_test_tmp_cleanup_stale_older_than(const char *parent_dir,
                                          const char *name_prefix,
                                          const char *allowed_prefix,
                                          long min_age_seconds) {
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
    if (!lc_test_tmp_is_old_enough(path, min_age_seconds)) {
      continue;
    }
    lc_test_tmp_cleanup_path(path, allowed_prefix);
  }
  (void)closedir(dir);
}

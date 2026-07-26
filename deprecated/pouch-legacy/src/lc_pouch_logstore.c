#include "lc_pouch_logstore.h"

#include "lc_api_internal.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define LC_POUCH_LOGSTORE_SEGMENT_SEAL_BYTES (64UL * 1024UL)

static int lc_pouch_logstore_set_errno(lc_error *error,
                                       const char *message) {
  return lc_error_set(error, LC_ERR_TRANSPORT, 0L, message, strerror(errno),
                      NULL, NULL);
}

static int lc_pouch_logstore_set_invalid(lc_error *error,
                                         const char *message) {
  return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL, NULL);
}

static int lc_pouch_logstore_set_nomem(lc_error *error, const char *message) {
  return lc_error_set(error, LC_ERR_NOMEM, 0L, message, NULL, NULL, NULL);
}

static int lc_pouch_logstore_write_all(int fd, const void *bytes,
                                       size_t count) {
  const unsigned char *cursor;
  ssize_t written;

  cursor = (const unsigned char *)bytes;
  while (count > 0U) {
    written = write(fd, cursor, count);
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
    cursor += written;
    count -= (size_t)written;
  }
  return 1;
}

static int lc_pouch_logstore_ensure_directory(const char *path,
                                              const char *message,
                                              lc_error *error) {
  struct stat st;

  if (mkdir(path, 0777) == 0) {
    return LC_OK;
  }
  if (errno != EEXIST) {
    return lc_pouch_logstore_set_errno(error, message);
  }
  if (stat(path, &st) != 0) {
    return lc_pouch_logstore_set_errno(error, message);
  }
  if (!S_ISDIR(st.st_mode)) {
    return lc_pouch_logstore_set_invalid(error, message);
  }
  return LC_OK;
}

static int lc_pouch_logstore_path_char_safe(unsigned char value) {
  return (value >= (unsigned char)'a' && value <= (unsigned char)'z') ||
         (value >= (unsigned char)'A' && value <= (unsigned char)'Z') ||
         (value >= (unsigned char)'0' && value <= (unsigned char)'9') ||
         value == (unsigned char)'_' || value == (unsigned char)'-';
}

static size_t lc_pouch_logstore_path_escaped_length(const char *value) {
  const unsigned char *cursor;
  size_t length;

  length = 0U;
  cursor = (const unsigned char *)value;
  while (*cursor != '\0') {
    length += lc_pouch_logstore_path_char_safe(*cursor) ? 1U : 3U;
    cursor++;
  }
  return length;
}

static void lc_pouch_logstore_path_escape(char *dst, const char *value) {
  static const char hex[] = "0123456789abcdef";
  const unsigned char *cursor;

  cursor = (const unsigned char *)value;
  while (*cursor != '\0') {
    if (lc_pouch_logstore_path_char_safe(*cursor)) {
      *dst++ = (char)*cursor;
    } else {
      *dst++ = '%';
      *dst++ = hex[(*cursor >> 4) & 0x0fU];
      *dst++ = hex[*cursor & 0x0fU];
    }
    cursor++;
  }
  *dst = '\0';
}

static char *lc_pouch_logstore_join_path(
    const lc_pouch_allocator *allocator, const char *root, const char *leaf) {
  size_t root_len;
  size_t leaf_len;
  char *path;

  if (root == NULL || leaf == NULL) {
    return NULL;
  }
  root_len = strlen(root);
  leaf_len = strlen(leaf);
  path = (char *)lc_pouch_alloc(allocator, root_len + 1U + leaf_len + 1U);
  if (path == NULL) {
    return NULL;
  }
  memcpy(path, root, root_len);
  path[root_len] = '/';
  memcpy(path + root_len + 1U, leaf, leaf_len);
  path[root_len + 1U + leaf_len] = '\0';
  return path;
}

static char *lc_pouch_logstore_dup_bytes(
    const lc_pouch_allocator *allocator, const char *bytes, size_t length) {
  char *copy;

  copy = (char *)lc_pouch_alloc(allocator, length + 1U);
  if (copy == NULL) {
    return NULL;
  }
  if (length > 0U) {
    memcpy(copy, bytes, length);
  }
  copy[length] = '\0';
  return copy;
}

static char *lc_pouch_logstore_make_namespace_path(
    const lc_pouch_logstore *logstore, const char *namespace_name) {
  size_t root_len;
  size_t ns_len;
  size_t total_len;
  char *path;
  char *cursor;

  root_len = strlen(logstore->root_path);
  ns_len = lc_pouch_logstore_path_escaped_length(namespace_name);
  total_len = root_len + 1U + ns_len;
  path = (char *)lc_pouch_alloc(logstore->allocator, total_len + 1U);
  if (path == NULL) {
    return NULL;
  }
  cursor = path;
  memcpy(cursor, logstore->root_path, root_len);
  cursor += root_len;
  *cursor++ = '/';
  lc_pouch_logstore_path_escape(cursor, namespace_name);
  return path;
}

static int lc_pouch_logstore_append_manifest_record(
    const lc_pouch_logstore *logstore, const char *manifest_log_path,
    const char *event, const char *file_name, lc_error *error) {
  size_t event_len;
  size_t file_len;
  size_t line_len;
  char *line;
  struct stat st;
  char tail;
  int fd;
  int rc;

  event_len = strlen(event);
  file_len = strlen(file_name);
  line_len = event_len + 1U + file_len + 1U;
  line = (char *)lc_pouch_alloc(logstore->allocator, line_len + 1U);
  if (line == NULL) {
    return lc_pouch_logstore_set_nomem(error,
                                       "failed to allocate pouch manifest line");
  }
  memcpy(line, event, event_len);
  line[event_len] = ' ';
  memcpy(line + event_len + 1U, file_name, file_len);
  line[line_len - 1U] = '\n';
  line[line_len] = '\0';
  fd = open(manifest_log_path, O_RDWR | O_CREAT | O_APPEND, 0666);
  if (fd < 0) {
    lc_pouch_free(logstore->allocator, line);
    return lc_pouch_logstore_set_errno(error, "failed to open pouch manifest");
  }
  rc = LC_OK;
  if (fstat(fd, &st) != 0) {
    rc = lc_pouch_logstore_set_errno(error, "failed to stat pouch manifest");
  } else if (st.st_size > 0 && lseek(fd, st.st_size - 1, SEEK_SET) < 0) {
    rc = lc_pouch_logstore_set_errno(error,
                                     "failed to seek pouch manifest tail");
  } else if (st.st_size > 0 && read(fd, &tail, 1U) != 1) {
    rc = lc_pouch_logstore_set_errno(error,
                                     "failed to read pouch manifest tail");
  } else if (st.st_size > 0 && tail != '\n' &&
             !lc_pouch_logstore_write_all(fd, "\n", 1U)) {
    rc = lc_pouch_logstore_set_errno(error,
                                     "failed to terminate pouch manifest tail");
  } else if (!lc_pouch_logstore_write_all(fd, line, line_len)) {
    rc = lc_pouch_logstore_set_errno(error, "failed to append pouch manifest");
  } else if (logstore->fsync_fn != NULL &&
             logstore->fsync_fn(logstore->fsync_context, fd) != 0) {
    rc = lc_pouch_logstore_set_errno(error, "failed to fsync pouch manifest");
  }
  if (close(fd) != 0 && rc == LC_OK) {
    rc = lc_pouch_logstore_set_errno(error, "failed to close pouch manifest");
  }
  lc_pouch_free(logstore->allocator, line);
  return rc;
}

static int lc_pouch_logstore_manifest_active_segment_number(
    const char *manifest_log_path, int *found_active,
    unsigned long *active_number, unsigned long *max_seen, lc_error *error) {
  char line[512];
  FILE *manifest;

  if (found_active != NULL) {
    *found_active = 0;
  }
  if (active_number != NULL) {
    *active_number = 0UL;
  }
  if (max_seen != NULL) {
    *max_seen = 0UL;
  }
  manifest = fopen(manifest_log_path, "r");
  if (manifest == NULL) {
    if (errno == ENOENT || errno == ENOTDIR) {
      return LC_OK;
    }
    return lc_pouch_logstore_set_errno(error, "failed to open pouch manifest");
  }
  while (fgets(line, sizeof(line), manifest) != NULL) {
    char *event;
    char *file_name;
    char *newline;
    unsigned long number;

    event = line;
    file_name = strchr(line, ' ');
    if (file_name == NULL) {
      continue;
    }
    *file_name++ = '\0';
    newline = strchr(file_name, '\n');
    if (newline != NULL) {
      *newline = '\0';
    }
    if (strcmp(event, "snapshot") == 0) {
      continue;
    }
    if (!lc_pouch_logstore_segment_name_parse(file_name, &number)) {
      continue;
    }
    if (max_seen != NULL && number > *max_seen) {
      *max_seen = number;
    }
    if (strcmp(event, "open") == 0 || strcmp(event, "compact") == 0) {
      if (found_active != NULL) {
        *found_active = 1;
      }
      if (active_number != NULL) {
        *active_number = number;
      }
    } else if (strcmp(event, "obsolete") == 0 && found_active != NULL &&
               active_number != NULL && *found_active &&
               *active_number == number) {
      *found_active = 0;
      *active_number = 0UL;
    }
  }
  if (ferror(manifest)) {
    (void)fclose(manifest);
    return lc_pouch_logstore_set_errno(error, "failed to read pouch manifest");
  }
  if (fclose(manifest) != 0) {
    return lc_pouch_logstore_set_errno(error, "failed to close pouch manifest");
  }
  return LC_OK;
}

static char *lc_pouch_logstore_make_namespace_active_segment_path(
    const lc_pouch_logstore *logstore, const char *namespace_name,
    int *needs_open_manifest, lc_error *error) {
  char *namespace_path;
  char *logstore_path;
  char *manifest_path;
  char *manifest_log_path;
  char *segments_path;
  DIR *segments_dir;
  struct dirent *entry;
  unsigned long manifest_number;
  unsigned long manifest_max;
  unsigned long max_number;
  int found;
  int manifest_found;
  int rc;

  if (needs_open_manifest != NULL) {
    *needs_open_manifest = 0;
  }
  namespace_path =
      lc_pouch_logstore_make_namespace_path(logstore, namespace_name);
  if (namespace_path == NULL) {
    (void)lc_pouch_logstore_set_nomem(
        error, "failed to allocate pouch namespace path");
    return NULL;
  }
  logstore_path =
      lc_pouch_logstore_join_path(logstore->allocator, namespace_path,
                                  "logstore");
  manifest_path =
      logstore_path != NULL
          ? lc_pouch_logstore_join_path(logstore->allocator, logstore_path,
                                        "manifest")
          : NULL;
  manifest_log_path =
      manifest_path != NULL
          ? lc_pouch_logstore_join_path(logstore->allocator, manifest_path,
                                        "manifest.log")
          : NULL;
  segments_path =
      logstore_path != NULL
          ? lc_pouch_logstore_join_path(logstore->allocator, logstore_path,
                                        "segments")
          : NULL;
  if (logstore_path == NULL || manifest_path == NULL ||
      manifest_log_path == NULL || segments_path == NULL) {
    lc_pouch_free(logstore->allocator, namespace_path);
    lc_pouch_free(logstore->allocator, logstore_path);
    lc_pouch_free(logstore->allocator, manifest_path);
    lc_pouch_free(logstore->allocator, manifest_log_path);
    lc_pouch_free(logstore->allocator, segments_path);
    (void)lc_pouch_logstore_set_nomem(error,
                                      "failed to allocate pouch segment path");
    return NULL;
  }
  rc = lc_pouch_logstore_manifest_active_segment_number(
      manifest_log_path, &manifest_found, &manifest_number, &manifest_max,
      error);
  if (rc != LC_OK) {
    lc_pouch_free(logstore->allocator, namespace_path);
    lc_pouch_free(logstore->allocator, logstore_path);
    lc_pouch_free(logstore->allocator, manifest_path);
    lc_pouch_free(logstore->allocator, manifest_log_path);
    lc_pouch_free(logstore->allocator, segments_path);
    return NULL;
  }
  if (manifest_found) {
    lc_pouch_free(logstore->allocator, segments_path);
    lc_pouch_free(logstore->allocator, manifest_log_path);
    lc_pouch_free(logstore->allocator, manifest_path);
    lc_pouch_free(logstore->allocator, logstore_path);
    lc_pouch_free(logstore->allocator, namespace_path);
    return lc_pouch_logstore_make_namespace_segment_path(
        logstore, namespace_name, manifest_number);
  }
  if (manifest_max > 0UL) {
    if (manifest_max == (unsigned long)-1) {
      lc_pouch_free(logstore->allocator, namespace_path);
      lc_pouch_free(logstore->allocator, logstore_path);
      lc_pouch_free(logstore->allocator, manifest_path);
      lc_pouch_free(logstore->allocator, manifest_log_path);
      lc_pouch_free(logstore->allocator, segments_path);
      (void)lc_pouch_logstore_set_invalid(error,
                                          "pouch segment number overflow");
      return NULL;
    }
    if (needs_open_manifest != NULL) {
      *needs_open_manifest = 1;
    }
    lc_pouch_free(logstore->allocator, segments_path);
    lc_pouch_free(logstore->allocator, manifest_log_path);
    lc_pouch_free(logstore->allocator, manifest_path);
    lc_pouch_free(logstore->allocator, logstore_path);
    lc_pouch_free(logstore->allocator, namespace_path);
    return lc_pouch_logstore_make_namespace_segment_path(
        logstore, namespace_name, manifest_max + 1UL);
  }
  segments_dir = opendir(segments_path);
  if (segments_dir == NULL) {
    lc_pouch_free(logstore->allocator, namespace_path);
    lc_pouch_free(logstore->allocator, logstore_path);
    lc_pouch_free(logstore->allocator, manifest_path);
    lc_pouch_free(logstore->allocator, manifest_log_path);
    lc_pouch_free(logstore->allocator, segments_path);
    (void)lc_pouch_logstore_set_errno(error,
                                      "failed to open pouch segments directory");
    return NULL;
  }
  max_number = 1UL;
  found = 0;
  while ((entry = readdir(segments_dir)) != NULL) {
    unsigned long number;

    if (lc_pouch_logstore_segment_name_parse(entry->d_name, &number)) {
      if (!found || number > max_number) {
        max_number = number;
      }
      found = 1;
    }
  }
  if (closedir(segments_dir) != 0) {
    lc_pouch_free(logstore->allocator, namespace_path);
    lc_pouch_free(logstore->allocator, logstore_path);
    lc_pouch_free(logstore->allocator, manifest_path);
    lc_pouch_free(logstore->allocator, manifest_log_path);
    lc_pouch_free(logstore->allocator, segments_path);
    (void)lc_pouch_logstore_set_errno(
        error, "failed to close pouch segments directory");
    return NULL;
  }
  lc_pouch_free(logstore->allocator, segments_path);
  lc_pouch_free(logstore->allocator, manifest_log_path);
  lc_pouch_free(logstore->allocator, manifest_path);
  lc_pouch_free(logstore->allocator, logstore_path);
  lc_pouch_free(logstore->allocator, namespace_path);
  return lc_pouch_logstore_make_namespace_segment_path(
      logstore, namespace_name, found ? max_number : 1UL);
}

static int lc_pouch_logstore_ensure_namespace_segment_manifest(
    const lc_pouch_logstore *logstore, const char *manifest_log_path,
    const char *segment_path, lc_error *error) {
  struct stat st;
  int fd;
  int should_append;

  should_append = 1;
  if (stat(manifest_log_path, &st) == 0 && st.st_size > 0) {
    should_append = 0;
  } else if (errno != ENOENT && errno != 0) {
    return lc_pouch_logstore_set_errno(error, "failed to stat pouch manifest");
  }
  fd = open(segment_path, O_RDWR | O_CREAT, 0666);
  if (fd < 0) {
    return lc_pouch_logstore_set_errno(error, "failed to create pouch segment");
  }
  if (close(fd) != 0) {
    return lc_pouch_logstore_set_errno(error, "failed to close pouch segment");
  }
  if (!should_append) {
    return LC_OK;
  }
  return lc_pouch_logstore_append_manifest_record(
      logstore, manifest_log_path, "open", "seg-0000000000000001.log", error);
}

void lc_pouch_logstore_init(lc_pouch_logstore *logstore,
                            const lc_pouch_allocator *allocator,
                            const char *root_path,
                            lc_pouch_logstore_fsync_fn fsync_fn,
                            void *fsync_context) {
  if (logstore == NULL) {
    return;
  }
  logstore->allocator = allocator;
  logstore->root_path = root_path;
  logstore->fsync_fn = fsync_fn;
  logstore->fsync_context = fsync_context;
  logstore->segment_seal_bytes = LC_POUCH_LOGSTORE_SEGMENT_SEAL_BYTES;
  logstore->obsolete_delete_grace_seconds = 0UL;
}

int lc_pouch_logstore_segment_name_parse(const char *name,
                                         unsigned long *number_out) {
  static const char prefix[] = "seg-";
  static const char suffix[] = ".log";
  unsigned long number;
  size_t index;
  size_t suffix_at;

  if (strncmp(name, prefix, sizeof(prefix) - 1U) != 0) {
    return 0;
  }
  suffix_at = sizeof(prefix) - 1U + 16U;
  if (strlen(name) != suffix_at + sizeof(suffix) - 1U ||
      strcmp(name + suffix_at, suffix) != 0) {
    return 0;
  }
  number = 0UL;
  for (index = sizeof(prefix) - 1U; index < suffix_at; ++index) {
    if (name[index] < '0' || name[index] > '9') {
      return 0;
    }
    number = number * 10UL + (unsigned long)(name[index] - '0');
  }
  if (number == 0UL) {
    return 0;
  }
  if (number_out != NULL) {
    *number_out = number;
  }
  return 1;
}

int lc_pouch_logstore_snapshot_name_parse(const char *name,
                                          unsigned long *number_out) {
  static const char prefix[] = "snap-";
  static const char suffix[] = ".log";
  unsigned long number;
  size_t index;
  size_t suffix_at;

  if (strncmp(name, prefix, sizeof(prefix) - 1U) != 0) {
    return 0;
  }
  suffix_at = sizeof(prefix) - 1U + 16U;
  if (strlen(name) != suffix_at + sizeof(suffix) - 1U ||
      strcmp(name + suffix_at, suffix) != 0) {
    return 0;
  }
  number = 0UL;
  for (index = sizeof(prefix) - 1U; index < suffix_at; ++index) {
    if (name[index] < '0' || name[index] > '9') {
      return 0;
    }
    number = number * 10UL + (unsigned long)(name[index] - '0');
  }
  if (number == 0UL) {
    return 0;
  }
  if (number_out != NULL) {
    *number_out = number;
  }
  return 1;
}

void lc_pouch_logstore_segment_name(char *buffer, size_t buffer_size,
                                    unsigned long number) {
  (void)snprintf(buffer, buffer_size, "seg-%016lu.log", number);
}

char *lc_pouch_logstore_make_namespace_segment_path(
    const lc_pouch_logstore *logstore, const char *namespace_name,
    unsigned long segment_number) {
  char *namespace_path;
  char *logstore_path;
  char *segments_path;
  char *segment_path;
  char segment_name[32];

  namespace_path =
      lc_pouch_logstore_make_namespace_path(logstore, namespace_name);
  if (namespace_path == NULL) {
    return NULL;
  }
  lc_pouch_logstore_segment_name(segment_name, sizeof(segment_name),
                                 segment_number);
  logstore_path =
      lc_pouch_logstore_join_path(logstore->allocator, namespace_path,
                                  "logstore");
  segments_path =
      logstore_path != NULL
          ? lc_pouch_logstore_join_path(logstore->allocator, logstore_path,
                                        "segments")
          : NULL;
  segment_path =
      segments_path != NULL
          ? lc_pouch_logstore_join_path(logstore->allocator, segments_path,
                                        segment_name)
          : NULL;
  lc_pouch_free(logstore->allocator, segments_path);
  lc_pouch_free(logstore->allocator, logstore_path);
  lc_pouch_free(logstore->allocator, namespace_path);
  return segment_path;
}

int lc_pouch_logstore_open_active_segment_for_append(
    const lc_pouch_logstore *logstore, const char *namespace_name,
    char **path_out, int *fd_out, lc_error *error) {
  unsigned long segment_number;
  char *segment_path;
  char *next_segment_path;
  char *base_name;
  struct stat st;
  int needs_open_manifest;
  int fd;
  int rc;

  if (path_out != NULL) {
    *path_out = NULL;
  }
  if (fd_out != NULL) {
    *fd_out = -1;
  }
  if (path_out == NULL || fd_out == NULL) {
    return lc_pouch_logstore_set_invalid(
        error, "active segment open requires outputs");
  }
  segment_path = lc_pouch_logstore_make_namespace_active_segment_path(
      logstore, namespace_name, &needs_open_manifest, error);
  if (segment_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  if (needs_open_manifest) {
    rc = lc_pouch_logstore_append_manifest_event_for_record_path(
        logstore, segment_path, "open", error);
    if (rc != LC_OK) {
      lc_pouch_free(logstore->allocator, segment_path);
      return rc;
    }
  }
  errno = 0;
  if (stat(segment_path, &st) == 0 &&
      (unsigned long)st.st_size >= logstore->segment_seal_bytes) {
    base_name = strrchr(segment_path, '/');
    base_name = base_name != NULL ? base_name + 1 : segment_path;
    if (!lc_pouch_logstore_segment_name_parse(base_name, &segment_number)) {
      lc_pouch_free(logstore->allocator, segment_path);
      return lc_pouch_logstore_set_invalid(error, "invalid pouch segment name");
    }
    rc = lc_pouch_logstore_append_manifest_event_for_record_path(
        logstore, segment_path, "seal", error);
    if (rc != LC_OK) {
      lc_pouch_free(logstore->allocator, segment_path);
      return rc;
    }
    next_segment_path = lc_pouch_logstore_make_namespace_segment_path(
        logstore, namespace_name, segment_number + 1UL);
    if (next_segment_path == NULL) {
      lc_pouch_free(logstore->allocator, segment_path);
      return lc_pouch_logstore_set_nomem(
          error, "failed to allocate pouch segment path");
    }
    fd = open(next_segment_path, O_RDWR | O_CREAT, 0666);
    if (fd < 0) {
      lc_pouch_free(logstore->allocator, next_segment_path);
      lc_pouch_free(logstore->allocator, segment_path);
      return lc_pouch_logstore_set_errno(error,
                                         "failed to create pouch segment");
    }
    if (close(fd) != 0) {
      lc_pouch_free(logstore->allocator, next_segment_path);
      lc_pouch_free(logstore->allocator, segment_path);
      return lc_pouch_logstore_set_errno(error,
                                         "failed to close pouch segment");
    }
    rc = lc_pouch_logstore_append_manifest_event_for_record_path(
        logstore, next_segment_path, "open", error);
    lc_pouch_free(logstore->allocator, segment_path);
    if (rc != LC_OK) {
      lc_pouch_free(logstore->allocator, next_segment_path);
      return rc;
    }
    segment_path = next_segment_path;
  } else if (errno != ENOENT && errno != 0) {
    lc_pouch_free(logstore->allocator, segment_path);
    return lc_pouch_logstore_set_errno(error, "failed to stat pouch segment");
  }
  fd = open(segment_path, O_RDWR | O_CREAT | O_APPEND, 0666);
  if (fd < 0) {
    lc_pouch_free(logstore->allocator, segment_path);
    return lc_pouch_logstore_set_errno(error, "failed to open pouch segment");
  }
  *path_out = segment_path;
  *fd_out = fd;
  return LC_OK;
}

int lc_pouch_logstore_ensure_namespace(const lc_pouch_logstore *logstore,
                                       const char *namespace_name,
                                       lc_error *error) {
  char *namespace_path;
  char *logstore_path;
  char *manifest_path;
  char *segments_path;
  char *snapshots_path;
  char *markers_path;
  char *queue_notify_path;
  char *manifest_log_path;
  char *segment_path;
  int rc;

  namespace_path = NULL;
  logstore_path = NULL;
  manifest_path = NULL;
  segments_path = NULL;
  snapshots_path = NULL;
  markers_path = NULL;
  queue_notify_path = NULL;
  manifest_log_path = NULL;
  segment_path = NULL;
  rc = LC_OK;
  if (logstore == NULL || namespace_name == NULL ||
      namespace_name[0] == '\0') {
    return lc_pouch_logstore_set_invalid(
        error, "pouch namespace logstore requires namespace");
  }
  namespace_path =
      lc_pouch_logstore_make_namespace_path(logstore, namespace_name);
  if (namespace_path == NULL) {
    rc = lc_pouch_logstore_set_nomem(
        error, "failed to allocate pouch namespace path");
    goto cleanup;
  }
  logstore_path =
      lc_pouch_logstore_join_path(logstore->allocator, namespace_path,
                                  "logstore");
  manifest_path =
      lc_pouch_logstore_join_path(logstore->allocator, logstore_path,
                                  "manifest");
  segments_path =
      lc_pouch_logstore_join_path(logstore->allocator, logstore_path,
                                  "segments");
  snapshots_path =
      lc_pouch_logstore_join_path(logstore->allocator, logstore_path,
                                  "snapshots");
  markers_path =
      lc_pouch_logstore_join_path(logstore->allocator, logstore_path,
                                  "markers");
  queue_notify_path =
      lc_pouch_logstore_join_path(logstore->allocator, logstore_path,
                                  "queue-notify");
  manifest_log_path =
      lc_pouch_logstore_join_path(logstore->allocator, manifest_path,
                                  "manifest.log");
  segment_path = lc_pouch_logstore_join_path(
      logstore->allocator, segments_path, "seg-0000000000000001.log");
  if (logstore_path == NULL || manifest_path == NULL || segments_path == NULL ||
      snapshots_path == NULL || markers_path == NULL ||
      queue_notify_path == NULL || manifest_log_path == NULL ||
      segment_path == NULL) {
    rc = lc_pouch_logstore_set_nomem(
        error, "failed to allocate pouch namespace logstore path");
    goto cleanup;
  }
  rc = lc_pouch_logstore_ensure_directory(
      namespace_path, "failed to create pouch namespace directory", error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  rc = lc_pouch_logstore_ensure_directory(
      logstore_path, "failed to create pouch namespace logstore directory",
      error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  rc = lc_pouch_logstore_ensure_directory(
      manifest_path, "failed to create pouch manifest directory", error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  rc = lc_pouch_logstore_ensure_directory(
      segments_path, "failed to create pouch segments directory", error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  rc = lc_pouch_logstore_ensure_directory(
      snapshots_path, "failed to create pouch snapshots directory", error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  rc = lc_pouch_logstore_ensure_directory(
      markers_path, "failed to create pouch markers directory", error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  rc = lc_pouch_logstore_ensure_directory(
      queue_notify_path, "failed to create pouch queue notification directory",
      error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  rc = lc_pouch_logstore_ensure_namespace_segment_manifest(
      logstore, manifest_log_path, segment_path, error);

cleanup:
  lc_pouch_free(logstore->allocator, segment_path);
  lc_pouch_free(logstore->allocator, manifest_log_path);
  lc_pouch_free(logstore->allocator, queue_notify_path);
  lc_pouch_free(logstore->allocator, markers_path);
  lc_pouch_free(logstore->allocator, snapshots_path);
  lc_pouch_free(logstore->allocator, segments_path);
  lc_pouch_free(logstore->allocator, manifest_path);
  lc_pouch_free(logstore->allocator, logstore_path);
  lc_pouch_free(logstore->allocator, namespace_path);
  return rc;
}

int lc_pouch_logstore_append_manifest_event_for_record_path(
    const lc_pouch_logstore *logstore, const char *record_path,
    const char *event, lc_error *error) {
  const char segments_marker[] = "/segments/";
  const char snapshots_marker[] = "/snapshots/";
  const char *marker_at;
  const char *file_name;
  size_t marker_len;
  size_t logstore_len;
  char *logstore_path;
  char *manifest_path;
  char *manifest_log_path;
  int rc;

  marker_at = strstr(record_path, segments_marker);
  marker_len = sizeof(segments_marker) - 1U;
  if (marker_at == NULL) {
    marker_at = strstr(record_path, snapshots_marker);
    marker_len = sizeof(snapshots_marker) - 1U;
  }
  if (marker_at == NULL) {
    return lc_pouch_logstore_set_invalid(
        error, "pouch logstore path is not manifestable");
  }
  file_name = marker_at + marker_len;
  logstore_len = (size_t)(marker_at - record_path);
  logstore_path = lc_pouch_logstore_dup_bytes(logstore->allocator,
                                              record_path, logstore_len);
  manifest_path =
      logstore_path != NULL
          ? lc_pouch_logstore_join_path(logstore->allocator, logstore_path,
                                        "manifest")
          : NULL;
  manifest_log_path =
      manifest_path != NULL
          ? lc_pouch_logstore_join_path(logstore->allocator, manifest_path,
                                        "manifest.log")
          : NULL;
  if (logstore_path == NULL || manifest_path == NULL ||
      manifest_log_path == NULL) {
    lc_pouch_free(logstore->allocator, logstore_path);
    lc_pouch_free(logstore->allocator, manifest_path);
    lc_pouch_free(logstore->allocator, manifest_log_path);
    return lc_pouch_logstore_set_nomem(
        error, "failed to allocate pouch manifest path");
  }
  rc = lc_pouch_logstore_ensure_directory(
      manifest_path, "failed to create pouch manifest directory", error);
  if (rc == LC_OK) {
    rc = lc_pouch_logstore_append_manifest_record(
        logstore, manifest_log_path, event, file_name, error);
  }
  lc_pouch_free(logstore->allocator, logstore_path);
  lc_pouch_free(logstore->allocator, manifest_path);
  lc_pouch_free(logstore->allocator, manifest_log_path);
  return rc;
}

void lc_pouch_logstore_paths_cleanup(const lc_pouch_logstore *logstore,
                                     lc_pouch_logstore_paths *paths) {
  size_t index;

  if (paths == NULL) {
    return;
  }
  for (index = 0U; index < paths->count; ++index) {
    lc_pouch_free(logstore != NULL ? logstore->allocator : NULL,
                  paths->items[index]);
  }
  lc_pouch_free(logstore != NULL ? logstore->allocator : NULL, paths->items);
  memset(paths, 0, sizeof(*paths));
}

static int lc_pouch_logstore_path_compare(const void *left,
                                          const void *right) {
  const char *const *left_path;
  const char *const *right_path;
  const char *left_snapshot;
  const char *right_snapshot;
  const char *left_segment;
  const char *right_segment;
  size_t left_prefix_len;
  size_t right_prefix_len;
  int prefix_cmp;

  left_path = (const char *const *)left;
  right_path = (const char *const *)right;
  left_snapshot = strstr(*left_path, "/logstore/snapshots/");
  right_snapshot = strstr(*right_path, "/logstore/snapshots/");
  left_segment = strstr(*left_path, "/logstore/segments/");
  right_segment = strstr(*right_path, "/logstore/segments/");
  if ((left_snapshot != NULL || left_segment != NULL) &&
      (right_snapshot != NULL || right_segment != NULL)) {
    const char *left_marker;
    const char *right_marker;

    left_marker = left_snapshot != NULL ? left_snapshot : left_segment;
    right_marker = right_snapshot != NULL ? right_snapshot : right_segment;
    left_prefix_len = (size_t)(left_marker - *left_path);
    right_prefix_len = (size_t)(right_marker - *right_path);
    prefix_cmp = strncmp(*left_path, *right_path,
                         left_prefix_len < right_prefix_len ? left_prefix_len
                                                            : right_prefix_len);
    if (prefix_cmp != 0) {
      return prefix_cmp;
    }
    if (left_prefix_len != right_prefix_len) {
      return left_prefix_len < right_prefix_len ? -1 : 1;
    }
    if ((left_snapshot != NULL) != (right_snapshot != NULL)) {
      return left_snapshot != NULL ? -1 : 1;
    }
  }
  return strcmp(*left_path, *right_path);
}

int lc_pouch_logstore_paths_add_take(const lc_pouch_logstore *logstore,
                                     lc_pouch_logstore_paths *paths,
                                     char *path) {
  char **grown;
  size_t new_capacity;

  if (paths->count >= paths->capacity) {
    new_capacity = paths->capacity == 0U ? 8U : paths->capacity * 2U;
    grown = (char **)lc_pouch_realloc(
        logstore->allocator, paths->items,
        new_capacity * sizeof(paths->items[0]));
    if (grown == NULL) {
      return 0;
    }
    paths->items = grown;
    paths->capacity = new_capacity;
  }
  paths->items[paths->count++] = path;
  return 1;
}

static int lc_pouch_logstore_paths_contains(
    const lc_pouch_logstore_paths *paths, const char *path) {
  size_t index;

  for (index = 0U; index < paths->count; ++index) {
    if (strcmp(paths->items[index], path) == 0) {
      return 1;
    }
  }
  return 0;
}

static int lc_pouch_logstore_paths_add_copy(
    const lc_pouch_logstore *logstore, lc_pouch_logstore_paths *paths,
    const char *path, lc_error *error) {
  char *copy;

  if (lc_pouch_logstore_paths_contains(paths, path)) {
    return LC_OK;
  }
  copy = lc_pouch_strdup(logstore->allocator, path);
  if (copy == NULL) {
    return lc_pouch_logstore_set_nomem(
        error, "failed to allocate pouch logstore replay path");
  }
  if (!lc_pouch_logstore_paths_add_take(logstore, paths, copy)) {
    lc_pouch_free(logstore->allocator, copy);
    return lc_pouch_logstore_set_nomem(
        error, "failed to collect pouch logstore replay path");
  }
  return LC_OK;
}

static void lc_pouch_logstore_paths_remove(
    const lc_pouch_logstore *logstore, lc_pouch_logstore_paths *paths,
    const char *path) {
  size_t index;

  for (index = 0U; index < paths->count; ++index) {
    if (strcmp(paths->items[index], path) == 0) {
      lc_pouch_free(logstore->allocator, paths->items[index]);
      if (index + 1U < paths->count) {
        memmove(paths->items + index, paths->items + index + 1U,
                (paths->count - index - 1U) * sizeof(paths->items[0]));
      }
      paths->count--;
      return;
    }
  }
}

static int lc_pouch_logstore_read_namespace_manifest_paths(
    const lc_pouch_logstore *logstore, const char *manifest_log_path,
    const char *segments_path, const char *snapshots_path,
    lc_pouch_logstore_paths *paths, lc_pouch_logstore_paths *mentioned_paths,
    lc_pouch_logstore_paths *obsolete_paths, lc_error *error) {
  char line[512];
  FILE *manifest;
  int rc;

  manifest = fopen(manifest_log_path, "r");
  if (manifest == NULL) {
    if (errno == ENOENT || errno == ENOTDIR) {
      return LC_OK;
    }
    return lc_pouch_logstore_set_errno(error, "failed to open pouch manifest");
  }
  rc = LC_OK;
  while (fgets(line, sizeof(line), manifest) != NULL) {
    char *event;
    char *file_name;
    char *newline;
    char *record_path;
    unsigned long ignored_number;

    event = line;
    file_name = strchr(line, ' ');
    if (file_name == NULL) {
      continue;
    }
    *file_name++ = '\0';
    newline = strchr(file_name, '\n');
    if (newline != NULL) {
      *newline = '\0';
    }
    record_path = NULL;
    if (lc_pouch_logstore_segment_name_parse(file_name, &ignored_number)) {
      record_path = lc_pouch_logstore_join_path(logstore->allocator,
                                                segments_path, file_name);
    } else if (lc_pouch_logstore_snapshot_name_parse(file_name,
                                                     &ignored_number)) {
      record_path = lc_pouch_logstore_join_path(logstore->allocator,
                                                snapshots_path, file_name);
    } else {
      continue;
    }
    if (record_path == NULL) {
      rc = lc_pouch_logstore_set_nomem(
          error, "failed to allocate pouch manifest record path");
      break;
    }
    if (lc_pouch_logstore_segment_name_parse(file_name, &ignored_number)) {
      rc = lc_pouch_logstore_paths_add_copy(logstore, mentioned_paths,
                                            record_path, error);
    }
    if (rc == LC_OK && strcmp(event, "obsolete") == 0) {
      rc = lc_pouch_logstore_paths_add_copy(logstore, obsolete_paths,
                                            record_path, error);
    }
    if (rc == LC_OK && strcmp(event, "snapshot") == 0 &&
        lc_pouch_logstore_snapshot_name_parse(file_name, &ignored_number)) {
      rc = lc_pouch_logstore_paths_add_copy(logstore, paths, record_path,
                                            error);
    } else if (rc == LC_OK &&
               lc_pouch_logstore_segment_name_parse(file_name,
                                                    &ignored_number) &&
               (strcmp(event, "open") == 0 || strcmp(event, "compact") == 0)) {
      rc = lc_pouch_logstore_paths_add_copy(logstore, paths, record_path,
                                            error);
    } else if (rc == LC_OK && strcmp(event, "obsolete") == 0) {
      lc_pouch_logstore_paths_remove(logstore, paths, record_path);
    }
    lc_pouch_free(logstore->allocator, record_path);
    if (rc != LC_OK) {
      break;
    }
  }
  if (ferror(manifest) && rc == LC_OK) {
    rc = lc_pouch_logstore_set_errno(error, "failed to read pouch manifest");
  }
  if (fclose(manifest) != 0 && rc == LC_OK) {
    rc = lc_pouch_logstore_set_errno(error, "failed to close pouch manifest");
  }
  return rc;
}

static int lc_pouch_logstore_repair_manifest_for_segment_path(
    const lc_pouch_logstore *logstore, const char *segment_path,
    lc_error *error) {
  const char marker[] = "/segments/";
  const char *marker_at;
  size_t logstore_len;
  char *logstore_path;
  char *manifest_path;
  char *manifest_log_path;
  int rc;

  marker_at = strstr(segment_path, marker);
  if (marker_at == NULL) {
    return lc_pouch_logstore_set_invalid(
        error, "pouch segment path is not manifestable");
  }
  logstore_len = (size_t)(marker_at - segment_path);
  logstore_path = lc_pouch_logstore_dup_bytes(logstore->allocator,
                                              segment_path, logstore_len);
  manifest_path =
      logstore_path != NULL
          ? lc_pouch_logstore_join_path(logstore->allocator, logstore_path,
                                        "manifest")
          : NULL;
  manifest_log_path =
      manifest_path != NULL
          ? lc_pouch_logstore_join_path(logstore->allocator, manifest_path,
                                        "manifest.log")
          : NULL;
  if (logstore_path == NULL || manifest_path == NULL ||
      manifest_log_path == NULL) {
    lc_pouch_free(logstore->allocator, logstore_path);
    lc_pouch_free(logstore->allocator, manifest_path);
    lc_pouch_free(logstore->allocator, manifest_log_path);
    return lc_pouch_logstore_set_nomem(
        error, "failed to allocate pouch manifest path");
  }
  rc = lc_pouch_logstore_ensure_directory(
      manifest_path, "failed to create pouch manifest directory", error);
  if (rc == LC_OK) {
    rc = lc_pouch_logstore_append_manifest_event_for_record_path(
        logstore, segment_path, "open", error);
  }
  lc_pouch_free(logstore->allocator, logstore_path);
  lc_pouch_free(logstore->allocator, manifest_path);
  lc_pouch_free(logstore->allocator, manifest_log_path);
  return rc;
}

static int lc_pouch_logstore_cleanup_obsolete_manifest_paths(
    const lc_pouch_logstore *logstore,
    const lc_pouch_logstore_paths *active_paths,
    const lc_pouch_logstore_paths *obsolete_paths, lc_error *error) {
  size_t index;

  for (index = 0U; index < obsolete_paths->count; ++index) {
    if (lc_pouch_logstore_paths_contains(active_paths,
                                         obsolete_paths->items[index])) {
      continue;
    }
    if (logstore->obsolete_delete_grace_seconds > 0UL) {
      struct stat st;
      time_t now;

      if (stat(obsolete_paths->items[index], &st) != 0) {
        if (errno == ENOENT || errno == ENOTDIR) {
          continue;
        }
        return lc_pouch_logstore_set_errno(
            error, "failed to stat obsolete pouch logstore");
      }
      now = time(NULL);
      if (now != (time_t)-1 &&
          (now <= st.st_mtime ||
           (unsigned long)(now - st.st_mtime) <
               logstore->obsolete_delete_grace_seconds)) {
        continue;
      }
    }
    if (unlink(obsolete_paths->items[index]) != 0 && errno != ENOENT &&
        errno != ENOTDIR) {
      return lc_pouch_logstore_set_errno(
          error, "failed to cleanup obsolete pouch logstore");
    }
  }
  return LC_OK;
}

int lc_pouch_logstore_collect_active_paths(
    const lc_pouch_logstore *logstore, lc_pouch_logstore_paths *paths,
    lc_error *error) {
  DIR *root_dir;
  struct dirent *entry;
  int rc;

  memset(paths, 0, sizeof(*paths));
  root_dir = opendir(logstore->root_path);
  if (root_dir == NULL) {
    return lc_pouch_logstore_set_errno(error,
                                       "failed to open pouch root directory");
  }
  rc = LC_OK;
  while ((entry = readdir(root_dir)) != NULL) {
    DIR *segments_dir;
    struct dirent *segment_entry;
    char *namespace_path;
    char *logstore_path;
    char *segments_path;
    char *snapshots_path;
    char *manifest_path;
    char *manifest_log_path;
    lc_pouch_logstore_paths manifest_active_paths;
    lc_pouch_logstore_paths mentioned_paths;
    lc_pouch_logstore_paths obsolete_paths;

    memset(&manifest_active_paths, 0, sizeof(manifest_active_paths));
    memset(&mentioned_paths, 0, sizeof(mentioned_paths));
    memset(&obsolete_paths, 0, sizeof(obsolete_paths));
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    namespace_path = NULL;
    logstore_path = NULL;
    segments_path = NULL;
    snapshots_path = NULL;
    manifest_path = NULL;
    manifest_log_path = NULL;
    namespace_path = lc_pouch_logstore_join_path(logstore->allocator,
                                                 logstore->root_path,
                                                 entry->d_name);
    logstore_path =
        namespace_path != NULL
            ? lc_pouch_logstore_join_path(logstore->allocator, namespace_path,
                                          "logstore")
            : NULL;
    segments_path =
        logstore_path != NULL
            ? lc_pouch_logstore_join_path(logstore->allocator, logstore_path,
                                          "segments")
            : NULL;
    snapshots_path =
        logstore_path != NULL
            ? lc_pouch_logstore_join_path(logstore->allocator, logstore_path,
                                          "snapshots")
            : NULL;
    manifest_path =
        logstore_path != NULL
            ? lc_pouch_logstore_join_path(logstore->allocator, logstore_path,
                                          "manifest")
            : NULL;
    manifest_log_path =
        manifest_path != NULL
            ? lc_pouch_logstore_join_path(logstore->allocator, manifest_path,
                                          "manifest.log")
            : NULL;
    if (namespace_path == NULL || logstore_path == NULL ||
        segments_path == NULL || snapshots_path == NULL ||
        manifest_path == NULL || manifest_log_path == NULL) {
      lc_pouch_free(logstore->allocator, namespace_path);
      lc_pouch_free(logstore->allocator, logstore_path);
      lc_pouch_free(logstore->allocator, segments_path);
      lc_pouch_free(logstore->allocator, snapshots_path);
      lc_pouch_free(logstore->allocator, manifest_path);
      lc_pouch_free(logstore->allocator, manifest_log_path);
      rc = lc_pouch_logstore_set_nomem(
          error, "failed to allocate pouch segment path");
      break;
    }
    rc = lc_pouch_logstore_read_namespace_manifest_paths(
        logstore, manifest_log_path, segments_path, snapshots_path,
        &manifest_active_paths, &mentioned_paths, &obsolete_paths, error);
    if (rc == LC_OK) {
      size_t path_index;

      for (path_index = 0U; path_index < manifest_active_paths.count;
           ++path_index) {
        struct stat st;

        if (strstr(manifest_active_paths.items[path_index],
                   "/logstore/snapshots/") == NULL) {
          continue;
        }
        errno = 0;
        if (stat(manifest_active_paths.items[path_index], &st) == 0 &&
            S_ISREG(st.st_mode) && st.st_size > 0) {
          rc = lc_pouch_logstore_paths_add_copy(
              logstore, paths, manifest_active_paths.items[path_index], error);
          if (rc != LC_OK) {
            break;
          }
        } else if (errno != ENOENT && errno != ENOTDIR && errno != 0) {
          rc = lc_pouch_logstore_set_errno(error,
                                           "failed to stat pouch snapshot");
          break;
        }
      }
    }
    if (rc != LC_OK) {
      lc_pouch_logstore_paths_cleanup(logstore, &manifest_active_paths);
      lc_pouch_logstore_paths_cleanup(logstore, &mentioned_paths);
      lc_pouch_logstore_paths_cleanup(logstore, &obsolete_paths);
      lc_pouch_free(logstore->allocator, namespace_path);
      lc_pouch_free(logstore->allocator, logstore_path);
      lc_pouch_free(logstore->allocator, segments_path);
      lc_pouch_free(logstore->allocator, snapshots_path);
      lc_pouch_free(logstore->allocator, manifest_path);
      lc_pouch_free(logstore->allocator, manifest_log_path);
      break;
    }
    segments_dir = opendir(segments_path);
    if (segments_dir != NULL) {
      while ((segment_entry = readdir(segments_dir)) != NULL) {
        struct stat st;
        char *segment_path;
        unsigned long ignored_number;

        if (!lc_pouch_logstore_segment_name_parse(segment_entry->d_name,
                                                  &ignored_number)) {
          continue;
        }
        segment_path = lc_pouch_logstore_join_path(logstore->allocator,
                                                   segments_path,
                                                   segment_entry->d_name);
        if (segment_path == NULL) {
          rc = lc_pouch_logstore_set_nomem(
              error, "failed to allocate pouch segment path");
          break;
        }
        errno = 0;
        if (stat(segment_path, &st) == 0 && S_ISREG(st.st_mode) &&
            st.st_size > 0) {
          if (lc_pouch_logstore_paths_contains(&manifest_active_paths,
                                               segment_path)) {
            rc = lc_pouch_logstore_paths_add_copy(logstore, paths,
                                                  segment_path, error);
            if (rc != LC_OK) {
              lc_pouch_free(logstore->allocator, segment_path);
              break;
            }
          } else if (!lc_pouch_logstore_paths_contains(&mentioned_paths,
                                                       segment_path)) {
            rc = lc_pouch_logstore_repair_manifest_for_segment_path(
                logstore, segment_path, error);
            if (rc != LC_OK) {
              lc_pouch_free(logstore->allocator, segment_path);
              break;
            }
            rc = lc_pouch_logstore_paths_add_copy(logstore, paths,
                                                  segment_path, error);
            if (rc != LC_OK) {
              lc_pouch_free(logstore->allocator, segment_path);
              break;
            }
          }
        } else if (errno != ENOENT && errno != ENOTDIR && errno != 0) {
          lc_pouch_free(logstore->allocator, segment_path);
          rc = lc_pouch_logstore_set_errno(error,
                                           "failed to stat pouch segment");
          break;
        }
        lc_pouch_free(logstore->allocator, segment_path);
      }
      if (closedir(segments_dir) != 0 && rc == LC_OK) {
        rc = lc_pouch_logstore_set_errno(
            error, "failed to close pouch segments directory");
      }
    } else if (errno != ENOENT && errno != ENOTDIR) {
      rc = lc_pouch_logstore_set_errno(
          error, "failed to open pouch segments directory");
    }
    if (rc == LC_OK) {
      rc = lc_pouch_logstore_cleanup_obsolete_manifest_paths(
          logstore, &manifest_active_paths, &obsolete_paths, error);
    }
    lc_pouch_logstore_paths_cleanup(logstore, &manifest_active_paths);
    lc_pouch_logstore_paths_cleanup(logstore, &mentioned_paths);
    lc_pouch_logstore_paths_cleanup(logstore, &obsolete_paths);
    lc_pouch_free(logstore->allocator, namespace_path);
    lc_pouch_free(logstore->allocator, logstore_path);
    lc_pouch_free(logstore->allocator, segments_path);
    lc_pouch_free(logstore->allocator, snapshots_path);
    lc_pouch_free(logstore->allocator, manifest_path);
    lc_pouch_free(logstore->allocator, manifest_log_path);
    if (rc != LC_OK) {
      break;
    }
  }
  if (closedir(root_dir) != 0 && rc == LC_OK) {
    rc = lc_pouch_logstore_set_errno(error,
                                     "failed to close pouch root directory");
  }
  if (rc != LC_OK) {
    lc_pouch_logstore_paths_cleanup(logstore, paths);
    return rc;
  }
  if (paths->count > 1U) {
    qsort(paths->items, paths->count, sizeof(paths->items[0]),
          lc_pouch_logstore_path_compare);
  }
  return LC_OK;
}

static int lc_pouch_logstore_path_segment_number(
    const char *path, unsigned long *number_out) {
  const char *marker;
  const char *file_name;

  marker = strstr(path, "/logstore/segments/");
  if (marker == NULL) {
    return 0;
  }
  file_name = strrchr(path, '/');
  file_name = file_name != NULL ? file_name + 1 : path;
  return lc_pouch_logstore_segment_name_parse(file_name, number_out);
}

static int lc_pouch_logstore_paths_same_logstore_prefix(const char *left,
                                                       const char *right) {
  const char *left_marker;
  const char *right_marker;
  size_t left_len;
  size_t right_len;

  left_marker = strstr(left, "/logstore/segments/");
  right_marker = strstr(right, "/logstore/segments/");
  if (left_marker == NULL || right_marker == NULL) {
    return 0;
  }
  left_len = (size_t)(left_marker - left);
  right_len = (size_t)(right_marker - right);
  return left_len == right_len && strncmp(left, right, left_len) == 0;
}

size_t lc_pouch_logstore_compaction_candidate_file_count(
    const lc_pouch_logstore_paths *paths) {
  size_t candidates;
  size_t index;

  if (paths == NULL) {
    return 0U;
  }
  candidates = 0U;
  for (index = 0U; index < paths->count; ++index) {
    unsigned long number;
    size_t other_index;
    int has_later_segment;

    if (strstr(paths->items[index], "/logstore/snapshots/") != NULL) {
      candidates++;
      continue;
    }
    if (!lc_pouch_logstore_path_segment_number(paths->items[index], &number)) {
      continue;
    }
    has_later_segment = 0;
    for (other_index = 0U; other_index < paths->count; ++other_index) {
      unsigned long other_number;

      if (other_index == index ||
          !lc_pouch_logstore_paths_same_logstore_prefix(
              paths->items[index], paths->items[other_index]) ||
          !lc_pouch_logstore_path_segment_number(paths->items[other_index],
                                                 &other_number)) {
        continue;
      }
      if (other_number > number) {
        has_later_segment = 1;
        break;
      }
    }
    if (has_later_segment) {
      candidates++;
    }
  }
  return candidates;
}

unsigned long
lc_pouch_logstore_mix_stat_generation(unsigned long current,
                                      const struct stat *st) {
  unsigned long value;

  value = (unsigned long)st->st_size;
  value ^= ((unsigned long)st->st_dev << 3);
  value ^= ((unsigned long)st->st_ino << 1);
  value ^= ((unsigned long)st->st_mtime << 7);
  current ^= value + 0x9e3779b9UL + (current << 6) + (current >> 2);
  return current;
}

int lc_pouch_logstore_active_generation(const lc_pouch_logstore *logstore,
                                        int *found,
                                        unsigned long *generation,
                                        lc_error *error) {
  lc_pouch_logstore_paths paths;
  unsigned long total;
  size_t path_count;
  size_t index;
  int rc;

  if (found != NULL) {
    *found = 0;
  }
  if (generation != NULL) {
    *generation = 0UL;
  }
  memset(&paths, 0, sizeof(paths));
  total = 0UL;
  rc = lc_pouch_logstore_collect_active_paths(logstore, &paths, error);
  path_count = paths.count;
  for (index = 0U; rc == LC_OK && index < paths.count; ++index) {
    struct stat st;

    if (stat(paths.items[index], &st) != 0) {
      rc = lc_pouch_logstore_set_errno(error,
                                       "failed to stat pouch logstore record");
      break;
    }
    if (!S_ISREG(st.st_mode) || st.st_size <= 0) {
      continue;
    }
    total = lc_pouch_logstore_mix_stat_generation(total, &st);
  }
  lc_pouch_logstore_paths_cleanup(logstore, &paths);
  if (rc == LC_OK) {
    if (path_count > 0U && found != NULL) {
      *found = 1;
    }
    if (generation != NULL) {
      *generation = total;
    }
  }
  return rc;
}

char *lc_pouch_logstore_make_compact_backup_path(
    const lc_pouch_logstore *logstore, const char *path) {
  static const char suffix[] = ".compact.bak";
  size_t path_len;
  size_t suffix_len;
  char *backup_path;

  path_len = strlen(path);
  suffix_len = sizeof(suffix) - 1U;
  backup_path =
      (char *)lc_pouch_alloc(logstore->allocator,
                             path_len + suffix_len + 1U);
  if (backup_path == NULL) {
    return NULL;
  }
  memcpy(backup_path, path, path_len);
  memcpy(backup_path + path_len, suffix, suffix_len + 1U);
  return backup_path;
}

void lc_pouch_logstore_compact_backups_cleanup(
    const lc_pouch_logstore *logstore, lc_pouch_logstore_paths *backups,
    int restore) {
  static const char suffix[] = ".compact.bak";
  size_t index;

  for (index = 0U; index < backups->count; ++index) {
    if (backups->items[index] != NULL) {
      if (restore) {
        size_t backup_len;
        char *active_path;

        backup_len = strlen(backups->items[index]);
        active_path = backup_len >= sizeof(suffix) - 1U
                          ? lc_pouch_logstore_dup_bytes(
                                logstore->allocator, backups->items[index],
                                backup_len - (sizeof(suffix) - 1U))
                          : NULL;
        if (active_path != NULL) {
          (void)unlink(active_path);
          (void)rename(backups->items[index], active_path);
          lc_pouch_free(logstore->allocator, active_path);
        }
      } else {
        (void)unlink(backups->items[index]);
      }
    }
  }
  lc_pouch_logstore_paths_cleanup(logstore, backups);
}

int lc_pouch_logstore_prepare_compact_backups(
    const lc_pouch_logstore *logstore,
    const lc_pouch_logstore_paths *active_paths,
    lc_pouch_logstore_paths *backups, lc_error *error) {
  size_t index;

  memset(backups, 0, sizeof(*backups));
  for (index = 0U; index < active_paths->count; ++index) {
    char *backup_path;

    backup_path = lc_pouch_logstore_make_compact_backup_path(
        logstore, active_paths->items[index]);
    if (backup_path == NULL) {
      lc_pouch_logstore_compact_backups_cleanup(logstore, backups, 1);
      return lc_pouch_logstore_set_nomem(
          error, "failed to allocate pouch segment backup path");
    }
    (void)unlink(backup_path);
    if (rename(active_paths->items[index], backup_path) != 0) {
      lc_pouch_free(logstore->allocator, backup_path);
      lc_pouch_logstore_compact_backups_cleanup(logstore, backups, 1);
      return lc_pouch_logstore_set_errno(error,
                                         "failed to backup pouch segment");
    }
    if (!lc_pouch_logstore_paths_add_take(logstore, backups, backup_path)) {
      (void)rename(backup_path, active_paths->items[index]);
      lc_pouch_free(logstore->allocator, backup_path);
      lc_pouch_logstore_compact_backups_cleanup(logstore, backups, 1);
      return lc_pouch_logstore_set_nomem(
          error, "failed to track pouch segment backup");
    }
  }
  return LC_OK;
}

int lc_pouch_logstore_open_compact_body_fd(
    const lc_pouch_logstore *logstore, const char *path, lc_error *error) {
  char *backup_path;
  int fd;

  backup_path = lc_pouch_logstore_make_compact_backup_path(logstore, path);
  if (backup_path == NULL) {
    (void)lc_pouch_logstore_set_nomem(
        error, "failed to allocate pouch compact body path");
    return -1;
  }
  fd = open(backup_path, O_RDONLY);
  lc_pouch_free(logstore->allocator, backup_path);
  if (fd >= 0) {
    return fd;
  }
  if (errno != ENOENT) {
    (void)lc_pouch_logstore_set_errno(error,
                                      "failed to open pouch compact body");
    return -1;
  }
  fd = open(path, O_RDONLY);
  if (fd < 0) {
    (void)lc_pouch_logstore_set_errno(error,
                                      "failed to open pouch compact body");
  }
  return fd;
}

char *lc_pouch_logstore_make_compact_snapshot_path(
    const lc_pouch_logstore *logstore, const char *segment_path,
    lc_error *error) {
  const char marker[] = "/segments/";
  const char *marker_at;
  size_t logstore_len;
  char *logstore_path;
  char *snapshots_path;
  char *snapshot_path;
  DIR *dir;
  struct dirent *entry;
  unsigned long max_number;
  unsigned long next_number;
  char snapshot_name[32];

  marker_at = strstr(segment_path, marker);
  if (marker_at == NULL) {
    (void)lc_pouch_logstore_set_invalid(
        error, "pouch segment path is not snapshotable");
    return NULL;
  }
  logstore_len = (size_t)(marker_at - segment_path);
  logstore_path = lc_pouch_logstore_dup_bytes(logstore->allocator,
                                              segment_path, logstore_len);
  snapshots_path =
      logstore_path != NULL
          ? lc_pouch_logstore_join_path(logstore->allocator, logstore_path,
                                        "snapshots")
          : NULL;
  if (logstore_path == NULL || snapshots_path == NULL) {
    lc_pouch_free(logstore->allocator, logstore_path);
    lc_pouch_free(logstore->allocator, snapshots_path);
    (void)lc_pouch_logstore_set_nomem(error,
                                      "failed to allocate pouch snapshot path");
    return NULL;
  }
  if (lc_pouch_logstore_ensure_directory(
          snapshots_path, "failed to create pouch snapshots directory",
          error) != LC_OK) {
    lc_pouch_free(logstore->allocator, logstore_path);
    lc_pouch_free(logstore->allocator, snapshots_path);
    return NULL;
  }
  max_number = 0UL;
  dir = opendir(snapshots_path);
  if (dir == NULL) {
    lc_pouch_free(logstore->allocator, logstore_path);
    lc_pouch_free(logstore->allocator, snapshots_path);
    (void)lc_pouch_logstore_set_errno(error,
                                      "failed to open pouch snapshots directory");
    return NULL;
  }
  while ((entry = readdir(dir)) != NULL) {
    unsigned long number;

    if (!lc_pouch_logstore_snapshot_name_parse(entry->d_name, &number)) {
      static const char compact_suffix[] = ".compact.bak";
      size_t name_len;
      size_t suffix_len;
      char base_name[64];

      name_len = strlen(entry->d_name);
      suffix_len = sizeof(compact_suffix) - 1U;
      if (name_len <= suffix_len ||
          strcmp(entry->d_name + name_len - suffix_len, compact_suffix) != 0 ||
          name_len - suffix_len >= sizeof(base_name)) {
        continue;
      }
      memcpy(base_name, entry->d_name, name_len - suffix_len);
      base_name[name_len - suffix_len] = '\0';
      if (!lc_pouch_logstore_snapshot_name_parse(base_name, &number)) {
        continue;
      }
    }
    if (number > max_number) {
      max_number = number;
    }
  }
  if (closedir(dir) != 0) {
    lc_pouch_free(logstore->allocator, logstore_path);
    lc_pouch_free(logstore->allocator, snapshots_path);
    (void)lc_pouch_logstore_set_errno(
        error, "failed to close pouch snapshots directory");
    return NULL;
  }
  if (max_number == (unsigned long)-1) {
    lc_pouch_free(logstore->allocator, logstore_path);
    lc_pouch_free(logstore->allocator, snapshots_path);
    (void)lc_pouch_logstore_set_invalid(error,
                                        "pouch snapshot number overflow");
    return NULL;
  }
  next_number = max_number + 1UL;
  (void)snprintf(snapshot_name, sizeof(snapshot_name), "snap-%016lu.log",
                 next_number);
  snapshot_path =
      lc_pouch_logstore_join_path(logstore->allocator, snapshots_path,
                                  snapshot_name);
  if (snapshot_path == NULL) {
    (void)lc_pouch_logstore_set_nomem(error,
                                      "failed to allocate pouch snapshot path");
  }
  lc_pouch_free(logstore->allocator, logstore_path);
  lc_pouch_free(logstore->allocator, snapshots_path);
  return snapshot_path;
}

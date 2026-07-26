#include "lc_pouch_logstore.h"

#include "lc_api_internal.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
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

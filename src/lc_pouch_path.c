#include "lc_pouch_path.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static int lc_pouch_path_char_safe(unsigned char value) {
  return (value >= 'A' && value <= 'Z') || (value >= 'a' && value <= 'z') ||
         (value >= '0' && value <= '9') || value == '-' || value == '_' ||
         value == '.';
}

char *lc_pouch_path_join(const lc_allocator *allocator, const char *root,
                         const char *leaf) {
  char *path;
  size_t root_len;
  size_t leaf_len;
  int has_slash;

  if (root == NULL || leaf == NULL) {
    return NULL;
  }
  root_len = strlen(root);
  leaf_len = strlen(leaf);
  has_slash = root_len > 0U && root[root_len - 1U] == '/';
  path = (char *)lc_alloc_with_allocator(
      allocator, root_len + (has_slash ? 0U : 1U) + leaf_len + 1U);
  if (path == NULL) {
    return NULL;
  }
  memcpy(path, root, root_len);
  if (!has_slash) {
    path[root_len++] = '/';
  }
  memcpy(path + root_len, leaf, leaf_len + 1U);
  return path;
}

char *lc_pouch_path_escape_name(const lc_allocator *allocator,
                                const char *name) {
  static const char hex[] = "0123456789abcdef";
  const unsigned char *cursor;
  char *escaped;
  char *dst;
  size_t length;

  if (name == NULL || name[0] == '\0') {
    return NULL;
  }
  length = 0U;
  cursor = (const unsigned char *)name;
  while (*cursor != '\0') {
    length += lc_pouch_path_char_safe(*cursor) ? 1U : 3U;
    ++cursor;
  }
  escaped = (char *)lc_alloc_with_allocator(allocator, length + 1U);
  if (escaped == NULL) {
    return NULL;
  }
  dst = escaped;
  cursor = (const unsigned char *)name;
  while (*cursor != '\0') {
    if (lc_pouch_path_char_safe(*cursor)) {
      *dst++ = (char)*cursor;
    } else {
      *dst++ = '%';
      *dst++ = hex[*cursor >> 4];
      *dst++ = hex[*cursor & 0x0fU];
    }
    ++cursor;
  }
  *dst = '\0';
  return escaped;
}

int lc_pouch_path_ensure_directory(const char *path, const char *message,
                                   lc_error *error) {
  struct stat st;

  if (path == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL, NULL);
  }
  if (mkdir(path, 0777) != 0 && errno != EEXIST) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L, message, strerror(errno),
                        NULL, NULL);
  }
  if (stat(path, &st) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L, message, strerror(errno),
                        NULL, NULL);
  }
  if (!S_ISDIR(st.st_mode)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, message,
                        "path exists but is not a directory", NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_path_write_all(int fd, const char *text, size_t length) {
  size_t offset;

  offset = 0U;
  while (offset < length) {
    ssize_t written;

    written = write(fd, text + offset, length - offset);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      return 0;
    }
    if (written == 0) {
      return 0;
    }
    offset += (size_t)written;
  }
  return 1;
}

int lc_pouch_path_write_text_file(const char *path, const char *text,
                                  lc_error *error) {
  int fd;
  int rc;

  if (path == NULL || text == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch text write requires path and text", NULL, NULL,
                        NULL);
  }
  fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create pouch file", strerror(errno), NULL,
                        NULL);
  }
  rc = LC_OK;
  if (!lc_pouch_path_write_all(fd, text, strlen(text))) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L, "failed to write pouch file",
                      strerror(errno), NULL, NULL);
  } else if (fsync(fd) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L, "failed to fsync pouch file",
                      strerror(errno), NULL, NULL);
  }
  if (close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L, "failed to close pouch file",
                      strerror(errno), NULL, NULL);
  }
  return rc;
}

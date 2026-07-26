#include "lc_pouch_namespace.h"

#include "lc_pouch_format.h"
#include "lc_pouch_path.h"

#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define LC_POUCH_SEGMENT_PREFIX "seg-"
#define LC_POUCH_SEGMENT_SUFFIX ".log"

char *lc_pouch_namespace_path(const lc_allocator *allocator,
                              const char *root_path,
                              const char *namespace_name) {
  char *namespaces_path;
  char *escaped;
  char *path;

  namespaces_path = lc_pouch_path_join(allocator, root_path, "namespaces");
  escaped = lc_pouch_path_escape_name(allocator, namespace_name);
  path = namespaces_path != NULL && escaped != NULL
             ? lc_pouch_path_join(allocator, namespaces_path, escaped)
             : NULL;
  lc_free_with_allocator(allocator, namespaces_path);
  lc_free_with_allocator(allocator, escaped);
  return path;
}

char *lc_pouch_namespace_segment_leaf(const lc_allocator *allocator,
                                      unsigned long segment_id) {
  char stack[96];

  snprintf(stack, sizeof(stack), "%s%020lu%s", LC_POUCH_SEGMENT_PREFIX,
           segment_id, LC_POUCH_SEGMENT_SUFFIX);
  return lc_strdup_with_allocator(allocator, stack);
}

static int lc_pouch_namespace_ensure_child(const lc_allocator *allocator,
                                           const char *namespace_path,
                                           const char *child,
                                           const char *message,
                                           lc_error *error) {
  char *path;
  int rc;

  path = lc_pouch_path_join(allocator, namespace_path, child);
  if (path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace path", NULL, NULL,
                        NULL);
  }
  rc = lc_pouch_path_ensure_directory(path, message, error);
  lc_free_with_allocator(allocator, path);
  return rc;
}

static int lc_pouch_namespace_parse_segment_id(const char *leaf,
                                               unsigned long *segment_id) {
  const char *digits;
  char *end;
  unsigned long value;
  size_t prefix_len;
  size_t suffix_len;
  size_t leaf_len;
  size_t digits_len;
  size_t i;

  if (leaf == NULL || segment_id == NULL) {
    return 0;
  }
  prefix_len = strlen(LC_POUCH_SEGMENT_PREFIX);
  suffix_len = strlen(LC_POUCH_SEGMENT_SUFFIX);
  leaf_len = strlen(leaf);
  if (leaf_len <= prefix_len + suffix_len ||
      strncmp(leaf, LC_POUCH_SEGMENT_PREFIX, prefix_len) != 0 ||
      strcmp(leaf + leaf_len - suffix_len, LC_POUCH_SEGMENT_SUFFIX) != 0) {
    return 0;
  }
  digits = leaf + prefix_len;
  digits_len = leaf_len - prefix_len - suffix_len;
  for (i = 0U; i < digits_len; ++i) {
    if (digits[i] < '0' || digits[i] > '9') {
      return 0;
    }
  }
  value = strtoul(digits, &end, 10);
  if (end != digits + digits_len || value == 0UL) {
    return 0;
  }
  *segment_id = value;
  return 1;
}

static int lc_pouch_namespace_scan_segments(const lc_allocator *allocator,
                                            const char *namespace_path,
                                            unsigned long *max_segment_id,
                                            lc_error *error) {
  char *segments_path;
  DIR *dir;
  struct dirent *entry;

  *max_segment_id = 0UL;
  segments_path = lc_pouch_path_join(allocator, namespace_path, "segments");
  if (segments_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch segments path", NULL, NULL,
                        NULL);
  }
  dir = opendir(segments_path);
  lc_free_with_allocator(allocator, segments_path);
  if (dir == NULL) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch namespace segments", NULL, NULL,
                        NULL);
  }
  while ((entry = readdir(dir)) != NULL) {
    unsigned long segment_id;

    if (lc_pouch_namespace_parse_segment_id(entry->d_name, &segment_id) &&
        segment_id > *max_segment_id) {
      *max_segment_id = segment_id;
    }
  }
  closedir(dir);
  return LC_OK;
}

static int lc_pouch_namespace_manifest_read_active(
    const char *manifest_path, unsigned long *active_segment_id) {
  char line[256];
  FILE *fp;

  *active_segment_id = 0UL;
  fp = fopen(manifest_path, "rb");
  if (fp == NULL) {
    return 0;
  }
  while (fgets(line, sizeof(line), fp) != NULL) {
    char *value;
    size_t len;

    len = strlen(line);
    if (len > 0U && line[len - 1U] == '\n') {
      line[len - 1U] = '\0';
    }
    value = strchr(line, '=');
    if (value == NULL) {
      continue;
    }
    *value++ = '\0';
    if (strcmp(line, "active_segment") == 0) {
      unsigned long parsed;

      if (lc_pouch_namespace_parse_segment_id(value, &parsed)) {
        *active_segment_id = parsed;
      }
    }
  }
  fclose(fp);
  return *active_segment_id != 0UL;
}

static int lc_pouch_namespace_manifest_write(
    const lc_allocator *allocator, const char *namespace_name,
    lc_pouch_namespace_manifest *manifest, lc_error *error) {
  char text[512];
  char *manifest_path;
  int rc;

  manifest_path = lc_pouch_path_join(allocator, manifest->namespace_path,
                                     "manifest");
  if (manifest_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace manifest path",
                        NULL, NULL, NULL);
  }
  snprintf(text, sizeof(text),
           "layout=%s\nversion=%lu\nnamespace=%s\nactive_segment=%s\n"
           "max_segment_id=%lu\n",
           LC_POUCH_LAYOUT_NAME, LC_POUCH_LAYOUT_VERSION, namespace_name,
           manifest->active_segment, manifest->max_segment_id);
  rc = lc_pouch_path_write_text_file(manifest_path, text, error);
  lc_free_with_allocator(allocator, manifest_path);
  return rc;
}

int lc_pouch_namespace_manifest_open(const lc_allocator *allocator,
                                     const char *root_path,
                                     const char *namespace_name,
                                     lc_pouch_namespace_manifest *out,
                                     lc_error *error) {
  char *manifest_path;
  unsigned long active_segment_id;
  unsigned long max_segment_id;
  int manifest_valid;
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace manifest open requires out", NULL,
                        NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  out->namespace_path =
      lc_pouch_namespace_path(allocator, root_path, namespace_name);
  if (out->namespace_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace path", NULL, NULL,
                        NULL);
  }
  rc = lc_pouch_namespace_scan_segments(allocator, out->namespace_path,
                                        &max_segment_id, error);
  if (rc != LC_OK) {
    lc_pouch_namespace_manifest_cleanup(allocator, out);
    return rc;
  }
  manifest_path = lc_pouch_path_join(allocator, out->namespace_path,
                                     "manifest");
  if (manifest_path == NULL) {
    lc_pouch_namespace_manifest_cleanup(allocator, out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace manifest path",
                        NULL, NULL, NULL);
  }
  manifest_valid =
      lc_pouch_namespace_manifest_read_active(manifest_path,
                                             &active_segment_id);
  lc_free_with_allocator(allocator, manifest_path);
  if (!manifest_valid || active_segment_id < max_segment_id) {
    active_segment_id = max_segment_id != 0UL ? max_segment_id : 1UL;
    out->repaired = 1;
  }
  if (active_segment_id == 0UL) {
    active_segment_id = 1UL;
    out->repaired = 1;
  }
  out->active_segment_id = active_segment_id;
  out->max_segment_id =
      max_segment_id > active_segment_id ? max_segment_id : active_segment_id;
  out->active_segment =
      lc_pouch_namespace_segment_leaf(allocator, out->active_segment_id);
  if (out->active_segment == NULL) {
    lc_pouch_namespace_manifest_cleanup(allocator, out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch active segment name", NULL,
                        NULL, NULL);
  }
  if (out->repaired) {
    rc = lc_pouch_namespace_manifest_write(allocator, namespace_name, out,
                                           error);
    if (rc != LC_OK) {
      lc_pouch_namespace_manifest_cleanup(allocator, out);
      return rc;
    }
  }
  return LC_OK;
}

int lc_pouch_namespace_manifest_rotate(const lc_allocator *allocator,
                                       const char *namespace_name,
                                       lc_pouch_namespace_manifest *manifest,
                                       unsigned long segment_id,
                                       lc_error *error) {
  char *active_segment;
  int rc;

  if (manifest == NULL || segment_id == 0UL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace manifest rotate requires manifest "
                        "and segment id",
                        NULL, NULL, NULL);
  }
  active_segment = lc_pouch_namespace_segment_leaf(allocator, segment_id);
  if (active_segment == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch active segment name", NULL,
                        NULL, NULL);
  }
  lc_free_with_allocator(allocator, manifest->active_segment);
  manifest->active_segment = active_segment;
  manifest->active_segment_id = segment_id;
  if (segment_id > manifest->max_segment_id) {
    manifest->max_segment_id = segment_id;
  }
  rc = lc_pouch_namespace_manifest_write(allocator, namespace_name, manifest,
                                         error);
  if (rc == LC_OK) {
    manifest->repaired = 0;
  }
  return rc;
}

void lc_pouch_namespace_manifest_cleanup(
    const lc_allocator *allocator, lc_pouch_namespace_manifest *manifest) {
  if (manifest == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, manifest->namespace_path);
  lc_free_with_allocator(allocator, manifest->active_segment);
  memset(manifest, 0, sizeof(*manifest));
}

int lc_pouch_namespace_ensure(const lc_allocator *allocator,
                              const char *root_path,
                              const char *namespace_name, lc_error *error) {
  char *namespace_path;
  lc_pouch_namespace_manifest manifest;
  int rc;

  if (namespace_name == NULL || namespace_name[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace name is required", NULL, NULL, NULL);
  }
  namespace_path =
      lc_pouch_namespace_path(allocator, root_path, namespace_name);
  if (namespace_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace path", NULL, NULL,
                        NULL);
  }
  rc = lc_pouch_path_ensure_directory(
      namespace_path, "failed to create pouch namespace directory", error);
  if (rc == LC_OK) {
    rc = lc_pouch_namespace_ensure_child(
        allocator, namespace_path, "segments",
        "failed to create pouch namespace segments directory", error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_namespace_ensure_child(
        allocator, namespace_path, "payloads",
        "failed to create pouch namespace payloads directory", error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_namespace_ensure_child(
        allocator, namespace_path, "snapshots",
        "failed to create pouch namespace snapshots directory", error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_namespace_ensure_child(
        allocator, namespace_path, "markers",
        "failed to create pouch namespace markers directory", error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_namespace_ensure_child(
        allocator, namespace_path, "index",
        "failed to create pouch namespace index directory", error);
  }
  if (rc == LC_OK) {
    memset(&manifest, 0, sizeof(manifest));
    rc = lc_pouch_namespace_manifest_open(allocator, root_path, namespace_name,
                                          &manifest, error);
    lc_pouch_namespace_manifest_cleanup(allocator, &manifest);
  }
  lc_free_with_allocator(allocator, namespace_path);
  return rc;
}

#include "lc_pouch_namespace.h"

#include "lc_api_internal.h"
#include "lc_pouch_format.h"
#include "lc_pouch_path.h"

#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define LC_POUCH_SEGMENT_PREFIX "seg-"
#define LC_POUCH_SEGMENT_SUFFIX ".log"

typedef struct lc_pouch_marker_entry {
  char *name;
  long size;
  long mtime;
} lc_pouch_marker_entry;

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

static int lc_pouch_marker_entry_compare(const void *left,
                                         const void *right) {
  const lc_pouch_marker_entry *a;
  const lc_pouch_marker_entry *b;

  a = (const lc_pouch_marker_entry *)left;
  b = (const lc_pouch_marker_entry *)right;
  return strcmp(a->name, b->name);
}

static void lc_pouch_marker_entries_cleanup(
    const lc_allocator *allocator, lc_pouch_marker_entry *entries,
    size_t count) {
  size_t i;

  if (entries == NULL) {
    return;
  }
  for (i = 0U; i < count; ++i) {
    lc_free_with_allocator(allocator, entries[i].name);
  }
  lc_free_with_allocator(allocator, entries);
}

static int lc_pouch_marker_entries_append(
    const lc_allocator *allocator, lc_pouch_marker_entry **entries,
    size_t *count, size_t *capacity, const char *name, long size, long mtime,
    lc_error *error) {
  lc_pouch_marker_entry *grown;
  char *name_copy;
  size_t next_capacity;

  if (*count == *capacity) {
    next_capacity = *capacity == 0U ? 8U : *capacity * 2U;
    if (next_capacity < *capacity ||
        next_capacity > ((size_t)-1) / sizeof(**entries)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch marker snapshot", NULL,
                          NULL, NULL);
    }
    grown = (lc_pouch_marker_entry *)lc_realloc_with_allocator(
        allocator, *entries, next_capacity * sizeof(**entries));
    if (grown == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch marker snapshot", NULL,
                          NULL, NULL);
    }
    memset(grown + *capacity, 0,
           (next_capacity - *capacity) * sizeof(**entries));
    *entries = grown;
    *capacity = next_capacity;
  }
  name_copy = lc_strdup_with_allocator(allocator, name);
  if (name_copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch marker name", NULL, NULL,
                        NULL);
  }
  (*entries)[*count].name = name_copy;
  (*entries)[*count].size = size;
  (*entries)[*count].mtime = mtime;
  ++*count;
  return LC_OK;
}

static int lc_pouch_marker_fingerprint_append(
    const lc_allocator *allocator, char **fingerprint, size_t *length,
    size_t *capacity, const char *text, lc_error *error) {
  char *grown;
  size_t text_len;
  size_t required;
  size_t next_capacity;

  text_len = strlen(text);
  required = *length + text_len + 1U;
  if (required < *length) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch marker fingerprint", NULL,
                        NULL, NULL);
  }
  if (required > *capacity) {
    next_capacity = *capacity == 0U ? 128U : *capacity;
    while (next_capacity < required) {
      if (next_capacity > ((size_t)-1) / 2U) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch marker fingerprint", NULL,
                            NULL, NULL);
      }
      next_capacity *= 2U;
    }
    grown = (char *)lc_realloc_with_allocator(allocator, *fingerprint,
                                              next_capacity);
    if (grown == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch marker fingerprint", NULL,
                          NULL, NULL);
    }
    *fingerprint = grown;
    *capacity = next_capacity;
  }
  memcpy(*fingerprint + *length, text, text_len);
  *length += text_len;
  (*fingerprint)[*length] = '\0';
  return LC_OK;
}

static int lc_pouch_marker_snapshot_build_fingerprint(
    const lc_allocator *allocator, lc_pouch_marker_entry *entries,
    size_t count, char **out, lc_error *error) {
  char *fingerprint;
  size_t length;
  size_t capacity;
  size_t i;
  int rc;

  fingerprint = NULL;
  length = 0U;
  capacity = 0U;
  rc = lc_pouch_marker_fingerprint_append(allocator, &fingerprint, &length,
                                          &capacity, "", error);
  if (rc != LC_OK) {
    return rc;
  }
  for (i = 0U; i < count; ++i) {
    char line[512];
    int written;

    written = snprintf(line, sizeof(line), "%s size=%ld mtime=%ld\n",
                       entries[i].name, entries[i].size, entries[i].mtime);
    if (written < 0 || (size_t)written >= sizeof(line)) {
      lc_free_with_allocator(allocator, fingerprint);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to format pouch marker fingerprint", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_marker_fingerprint_append(allocator, &fingerprint, &length,
                                            &capacity, line, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(allocator, fingerprint);
      return rc;
    }
  }
  *out = fingerprint;
  return LC_OK;
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

int lc_pouch_namespace_touch_marker(const lc_allocator *allocator,
                                    const char *namespace_path,
                                    unsigned long sequence, lc_error *error) {
  char leaf[96];
  char text[192];
  char *markers_path;
  char *marker_path;
  int rc;

  if (namespace_path == NULL || sequence == 0UL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch marker touch requires namespace path and "
                        "non-zero sequence",
                        NULL, NULL, NULL);
  }
  snprintf(leaf, sizeof(leaf), "writer-%ld.marker", (long)getpid());
  snprintf(text, sizeof(text), "writer_pid=%ld\nsequence=%020lu\n%s",
           (long)getpid(), sequence,
           (sequence % 2UL) == 0UL ? "pad=x\n" : "");
  markers_path = lc_pouch_path_join(allocator, namespace_path, "markers");
  marker_path = markers_path != NULL ? lc_pouch_path_join(allocator,
                                                          markers_path, leaf)
                                     : NULL;
  lc_free_with_allocator(allocator, markers_path);
  if (marker_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch marker path", NULL, NULL,
                        NULL);
  }
  rc = lc_pouch_path_write_text_file(marker_path, text, error);
  lc_free_with_allocator(allocator, marker_path);
  return rc;
}

int lc_pouch_namespace_marker_snapshot_read(
    const lc_allocator *allocator, const char *namespace_path,
    lc_pouch_namespace_marker_snapshot *out, lc_error *error) {
  lc_pouch_marker_entry *entries;
  char *markers_path;
  char self_leaf[96];
  DIR *dir;
  struct dirent *entry;
  size_t count;
  size_t capacity;
  int rc;

  if (namespace_path == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch marker snapshot requires namespace path and "
                        "out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  entries = NULL;
  markers_path = lc_pouch_path_join(allocator, namespace_path, "markers");
  if (markers_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch markers path", NULL, NULL,
                        NULL);
  }
  dir = opendir(markers_path);
  if (dir == NULL) {
    lc_free_with_allocator(allocator, markers_path);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch marker directory", NULL, NULL,
                        NULL);
  }
  snprintf(self_leaf, sizeof(self_leaf), "writer-%ld.marker", (long)getpid());
  count = 0U;
  capacity = 0U;
  rc = LC_OK;
  while ((entry = readdir(dir)) != NULL) {
    char *marker_path;
    struct stat st;

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0 ||
        strcmp(entry->d_name, self_leaf) == 0) {
      continue;
    }
    marker_path = lc_pouch_path_join(allocator, markers_path, entry->d_name);
    if (marker_path == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch marker path", NULL, NULL,
                        NULL);
      break;
    }
    if (stat(marker_path, &st) == 0 && S_ISREG(st.st_mode)) {
      rc = lc_pouch_marker_entries_append(allocator, &entries, &count,
                                          &capacity, entry->d_name,
                                          (long)st.st_size, (long)st.st_mtime,
                                          error);
      if (rc != LC_OK) {
        lc_free_with_allocator(allocator, marker_path);
        break;
      }
    }
    lc_free_with_allocator(allocator, marker_path);
  }
  closedir(dir);
  lc_free_with_allocator(allocator, markers_path);
  if (rc != LC_OK) {
    lc_pouch_marker_entries_cleanup(allocator, entries, count);
    return rc;
  }
  if (count > 1U) {
    qsort(entries, count, sizeof(*entries), lc_pouch_marker_entry_compare);
  }
  rc = lc_pouch_marker_snapshot_build_fingerprint(allocator, entries, count,
                                                  &out->fingerprint, error);
  if (rc != LC_OK) {
    lc_pouch_marker_entries_cleanup(allocator, entries, count);
    return rc;
  }
  out->marker_count = (unsigned long)count;
  lc_pouch_marker_entries_cleanup(allocator, entries, count);
  return LC_OK;
}

int lc_pouch_namespace_marker_snapshot_changed(
    const lc_pouch_namespace_marker_snapshot *before,
    const lc_pouch_namespace_marker_snapshot *after) {
  const char *before_fingerprint;
  const char *after_fingerprint;

  if (before == NULL || after == NULL) {
    return 1;
  }
  if (before->marker_count != after->marker_count) {
    return 1;
  }
  before_fingerprint =
      before->fingerprint != NULL ? before->fingerprint : "";
  after_fingerprint = after->fingerprint != NULL ? after->fingerprint : "";
  return strcmp(before_fingerprint, after_fingerprint) != 0;
}

void lc_pouch_namespace_marker_snapshot_cleanup(
    const lc_allocator *allocator,
    lc_pouch_namespace_marker_snapshot *snapshot) {
  if (snapshot == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, snapshot->fingerprint);
  memset(snapshot, 0, sizeof(*snapshot));
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

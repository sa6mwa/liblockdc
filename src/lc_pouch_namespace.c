#include "lc_pouch_namespace.h"

#include "lc_api_internal.h"
#include "lc_pouch_format.h"
#include "lc_pouch_path.h"

#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define LC_POUCH_SEGMENT_PREFIX "seg-"
#define LC_POUCH_SEGMENT_SUFFIX ".log"
#define LC_POUCH_SNAPSHOT_PREFIX "snapshot-"
#define LC_POUCH_SNAPSHOT_SUFFIX ".log"

typedef lc_pouch_namespace_marker_peer_stat lc_pouch_marker_entry;

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

char *lc_pouch_namespace_snapshot_leaf(const lc_allocator *allocator,
                                       unsigned long segment_id) {
  char stack[96];

  snprintf(stack, sizeof(stack), "%s%020lu%s", LC_POUCH_SNAPSHOT_PREFIX,
           segment_id, LC_POUCH_SNAPSHOT_SUFFIX);
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

static int lc_pouch_namespace_parse_snapshot_id(const char *leaf,
                                                unsigned long *snapshot_id) {
  const char *digits;
  char *end;
  unsigned long value;
  size_t prefix_len;
  size_t suffix_len;
  size_t leaf_len;
  size_t digits_len;
  size_t i;

  if (leaf == NULL || snapshot_id == NULL) {
    return 0;
  }
  prefix_len = strlen(LC_POUCH_SNAPSHOT_PREFIX);
  suffix_len = strlen(LC_POUCH_SNAPSHOT_SUFFIX);
  leaf_len = strlen(leaf);
  if (leaf_len <= prefix_len + suffix_len ||
      strncmp(leaf, LC_POUCH_SNAPSHOT_PREFIX, prefix_len) != 0 ||
      strcmp(leaf + leaf_len - suffix_len, LC_POUCH_SNAPSHOT_SUFFIX) != 0) {
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
  *snapshot_id = value;
  return 1;
}

static int lc_pouch_namespace_manifest_list_append(
    const lc_allocator *allocator, char ***items, unsigned long *count,
    const char *leaf, lc_error *error) {
  char **grown;
  char *copy;
  unsigned long i;

  if (items == NULL || count == NULL || leaf == NULL || leaf[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch manifest obsolete entry requires a leaf", NULL,
                        NULL, NULL);
  }
  for (i = 0UL; i < *count; ++i) {
    if (strcmp((*items)[i], leaf) == 0) {
      return LC_OK;
    }
  }
  if (*count == (unsigned long)(((size_t)-1) / sizeof(**items))) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch manifest obsolete entries",
                        NULL, NULL, NULL);
  }
  grown = (char **)lc_realloc_with_allocator(
      allocator, *items, ((size_t)(*count) + 1U) * sizeof(**items));
  if (grown == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch manifest obsolete entries",
                        NULL, NULL, NULL);
  }
  *items = grown;
  copy = lc_strdup_with_allocator(allocator, leaf);
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch manifest obsolete entry",
                        NULL, NULL, NULL);
  }
  grown[*count] = copy;
  ++*count;
  return LC_OK;
}

static int lc_pouch_namespace_manifest_list_contains(char **items,
                                                     unsigned long count,
                                                     const char *leaf) {
  unsigned long i;

  if (leaf == NULL) {
    return 0;
  }
  for (i = 0UL; i < count; ++i) {
    if (strcmp(items[i], leaf) == 0) {
      return 1;
    }
  }
  return 0;
}

static void lc_pouch_namespace_manifest_list_cleanup(
    const lc_allocator *allocator, char **items, unsigned long count) {
  unsigned long i;

  if (items == NULL) {
    return;
  }
  for (i = 0UL; i < count; ++i) {
    lc_free_with_allocator(allocator, items[i]);
  }
  lc_free_with_allocator(allocator, items);
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

static int lc_pouch_namespace_scan_snapshots(const lc_allocator *allocator,
                                             const char *namespace_path,
                                             unsigned long *max_snapshot_id,
                                             char **latest_snapshot,
                                             lc_error *error) {
  char *snapshots_path;
  DIR *dir;
  struct dirent *entry;

  *max_snapshot_id = 0UL;
  *latest_snapshot = NULL;
  snapshots_path = lc_pouch_path_join(allocator, namespace_path, "snapshots");
  if (snapshots_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch snapshots path", NULL, NULL,
                        NULL);
  }
  dir = opendir(snapshots_path);
  lc_free_with_allocator(allocator, snapshots_path);
  if (dir == NULL) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch namespace snapshots", NULL, NULL,
                        NULL);
  }
  while ((entry = readdir(dir)) != NULL) {
    unsigned long snapshot_id;

    if (lc_pouch_namespace_parse_snapshot_id(entry->d_name, &snapshot_id) &&
        snapshot_id > *max_snapshot_id) {
      char *copy;

      copy = lc_strdup_with_allocator(allocator, entry->d_name);
      if (copy == NULL) {
        closedir(dir);
        lc_free_with_allocator(allocator, *latest_snapshot);
        *latest_snapshot = NULL;
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch snapshot name", NULL,
                            NULL, NULL);
      }
      lc_free_with_allocator(allocator, *latest_snapshot);
      *latest_snapshot = copy;
      *max_snapshot_id = snapshot_id;
    }
  }
  closedir(dir);
  return LC_OK;
}

static int lc_pouch_namespace_manifest_read(
    const lc_allocator *allocator, const char *manifest_path,
    unsigned long *active_segment_id, unsigned long *snapshot_segment_id,
    char **latest_snapshot, char ***obsolete_segments,
    unsigned long *obsolete_segment_count, char ***obsolete_snapshots,
    unsigned long *obsolete_snapshot_count) {
  char line[256];
  FILE *fp;
  int ok;

  *active_segment_id = 0UL;
  *snapshot_segment_id = 0UL;
  *latest_snapshot = NULL;
  *obsolete_segments = NULL;
  *obsolete_segment_count = 0UL;
  *obsolete_snapshots = NULL;
  *obsolete_snapshot_count = 0UL;
  fp = fopen(manifest_path, "rb");
  if (fp == NULL) {
    return 0;
  }
  ok = 1;
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
    } else if (strcmp(line, "snapshot") == 0) {
      unsigned long parsed;

      if (lc_pouch_namespace_parse_snapshot_id(value, &parsed)) {
        char *copy;

        copy = lc_strdup_with_allocator(allocator, value);
        if (copy == NULL) {
          fclose(fp);
          lc_free_with_allocator(allocator, *latest_snapshot);
          *latest_snapshot = NULL;
          *snapshot_segment_id = 0UL;
          return 0;
        }
        lc_free_with_allocator(allocator, *latest_snapshot);
        *latest_snapshot = copy;
        *snapshot_segment_id = parsed;
      }
    } else if (strcmp(line, "obsolete_segment") == 0) {
      unsigned long parsed;

      if (lc_pouch_namespace_parse_segment_id(value, &parsed) &&
          lc_pouch_namespace_manifest_list_append(
              allocator, obsolete_segments, obsolete_segment_count, value,
              NULL) != LC_OK) {
        ok = 0;
        break;
      }
    } else if (strcmp(line, "obsolete_snapshot") == 0) {
      unsigned long parsed;

      if (lc_pouch_namespace_parse_snapshot_id(value, &parsed) &&
          lc_pouch_namespace_manifest_list_append(
              allocator, obsolete_snapshots, obsolete_snapshot_count, value,
              NULL) != LC_OK) {
        ok = 0;
        break;
      }
    }
  }
  fclose(fp);
  if (!ok) {
    lc_free_with_allocator(allocator, *latest_snapshot);
    *latest_snapshot = NULL;
    lc_pouch_namespace_manifest_list_cleanup(
        allocator, *obsolete_segments, *obsolete_segment_count);
    *obsolete_segments = NULL;
    *obsolete_segment_count = 0UL;
    lc_pouch_namespace_manifest_list_cleanup(
        allocator, *obsolete_snapshots, *obsolete_snapshot_count);
    *obsolete_snapshots = NULL;
    *obsolete_snapshot_count = 0UL;
    *snapshot_segment_id = 0UL;
    return 0;
  }
  return *active_segment_id != 0UL;
}

static int lc_pouch_namespace_manifest_text_append(
    const lc_allocator *allocator, char **text, size_t *length,
    size_t *capacity, const char *line, lc_error *error) {
  char *grown;
  size_t line_len;
  size_t required;
  size_t next_capacity;

  line_len = strlen(line);
  required = *length + line_len + 1U;
  if (required < *length) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace manifest", NULL,
                        NULL, NULL);
  }
  if (required > *capacity) {
    next_capacity = *capacity == 0U ? 1024U : *capacity;
    while (next_capacity < required) {
      if (next_capacity > ((size_t)-1) / 2U) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch namespace manifest",
                            NULL, NULL, NULL);
      }
      next_capacity *= 2U;
    }
    grown = (char *)lc_realloc_with_allocator(allocator, *text,
                                              next_capacity);
    if (grown == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch namespace manifest", NULL,
                          NULL, NULL);
    }
    *text = grown;
    *capacity = next_capacity;
  }
  memcpy(*text + *length, line, line_len);
  *length += line_len;
  (*text)[*length] = '\0';
  return LC_OK;
}

static int lc_pouch_namespace_manifest_write(
    const lc_allocator *allocator, const char *namespace_name,
    lc_pouch_namespace_manifest *manifest, lc_error *error) {
  char line[512];
  char *text;
  char *manifest_path;
  size_t length;
  size_t capacity;
  unsigned long i;
  int written;
  int rc;

  manifest_path = lc_pouch_path_join(allocator, manifest->namespace_path,
                                     "manifest");
  if (manifest_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace manifest path",
                        NULL, NULL, NULL);
  }
  text = NULL;
  length = 0U;
  capacity = 0U;
  written = snprintf(line, sizeof(line),
                     "layout=%s\nversion=%lu\nnamespace=%s\n"
                     "active_segment=%s\nmax_segment_id=%lu\n",
                     LC_POUCH_LAYOUT_NAME, LC_POUCH_LAYOUT_VERSION,
                     namespace_name, manifest->active_segment,
                     manifest->max_segment_id);
  if (written < 0 || (size_t)written >= sizeof(line)) {
    lc_free_with_allocator(allocator, manifest_path);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to format pouch namespace manifest", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_namespace_manifest_text_append(
      allocator, &text, &length, &capacity, line, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(allocator, manifest_path);
    return rc;
  }
  if (manifest->latest_snapshot != NULL) {
    written = snprintf(line, sizeof(line), "snapshot=%s\n",
                       manifest->latest_snapshot);
    if (written < 0 || (size_t)written >= sizeof(line)) {
      lc_free_with_allocator(allocator, manifest_path);
      lc_free_with_allocator(allocator, text);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to format pouch namespace manifest", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_namespace_manifest_text_append(
        allocator, &text, &length, &capacity, line, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(allocator, manifest_path);
      lc_free_with_allocator(allocator, text);
      return rc;
    }
  }
  for (i = 0UL; i < manifest->obsolete_segment_count; ++i) {
    written = snprintf(line, sizeof(line), "obsolete_segment=%s\n",
                       manifest->obsolete_segments[i]);
    if (written < 0 || (size_t)written >= sizeof(line)) {
      lc_free_with_allocator(allocator, manifest_path);
      lc_free_with_allocator(allocator, text);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to format pouch namespace manifest", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_namespace_manifest_text_append(
        allocator, &text, &length, &capacity, line, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(allocator, manifest_path);
      lc_free_with_allocator(allocator, text);
      return rc;
    }
  }
  for (i = 0UL; i < manifest->obsolete_snapshot_count; ++i) {
    written = snprintf(line, sizeof(line), "obsolete_snapshot=%s\n",
                       manifest->obsolete_snapshots[i]);
    if (written < 0 || (size_t)written >= sizeof(line)) {
      lc_free_with_allocator(allocator, manifest_path);
      lc_free_with_allocator(allocator, text);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to format pouch namespace manifest", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_namespace_manifest_text_append(
        allocator, &text, &length, &capacity, line, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(allocator, manifest_path);
      lc_free_with_allocator(allocator, text);
      return rc;
    }
  }
  rc = lc_pouch_path_write_text_file(manifest_path, text, error);
  lc_free_with_allocator(allocator, text);
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

static void lc_pouch_marker_refresh_peer_stats_cleanup(
    const lc_allocator *allocator, lc_pouch_namespace_marker_refresh_state *state) {
  if (state == NULL) {
    return;
  }
  lc_pouch_marker_entries_cleanup(allocator, state->peer_stats,
                                  (size_t)state->peer_stat_count);
  state->peer_stats = NULL;
  state->peer_stat_count = 0UL;
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

static int lc_pouch_marker_entries_read(
    const lc_allocator *allocator, const char *namespace_path,
    const char *self_marker_leaf, lc_pouch_marker_entry **entries_out,
    size_t *count_out, lc_error *error) {
  lc_pouch_marker_entry *entries;
  char *markers_path;
  DIR *dir;
  struct dirent *entry;
  size_t count;
  size_t capacity;
  int rc;

  *entries_out = NULL;
  *count_out = 0U;
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
  count = 0U;
  capacity = 0U;
  rc = LC_OK;
  while ((entry = readdir(dir)) != NULL) {
    char *marker_path;
    struct stat st;

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0 ||
        (self_marker_leaf != NULL &&
         strcmp(entry->d_name, self_marker_leaf) == 0)) {
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
  *entries_out = entries;
  *count_out = count;
  return LC_OK;
}

int lc_pouch_namespace_manifest_open(const lc_allocator *allocator,
                                     const char *root_path,
                                     const char *namespace_name,
                                     lc_pouch_namespace_manifest *out,
                                     lc_error *error) {
  char *manifest_path;
  char *manifest_snapshot;
  char *scanned_snapshot;
  unsigned long active_segment_id;
  unsigned long max_segment_id;
  unsigned long manifest_snapshot_id;
  unsigned long max_snapshot_id;
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
  manifest_snapshot = NULL;
  scanned_snapshot = NULL;
  rc = lc_pouch_namespace_scan_segments(allocator, out->namespace_path,
                                        &max_segment_id, error);
  if (rc != LC_OK) {
    lc_pouch_namespace_manifest_cleanup(allocator, out);
    return rc;
  }
  rc = lc_pouch_namespace_scan_snapshots(allocator, out->namespace_path,
                                         &max_snapshot_id, &scanned_snapshot,
                                         error);
  if (rc != LC_OK) {
    lc_pouch_namespace_manifest_cleanup(allocator, out);
    return rc;
  }
  manifest_path = lc_pouch_path_join(allocator, out->namespace_path,
                                     "manifest");
  if (manifest_path == NULL) {
    lc_free_with_allocator(allocator, scanned_snapshot);
    lc_pouch_namespace_manifest_cleanup(allocator, out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace manifest path",
                        NULL, NULL, NULL);
  }
  manifest_valid =
      lc_pouch_namespace_manifest_read(allocator, manifest_path,
                                       &active_segment_id,
                                       &manifest_snapshot_id,
                                       &manifest_snapshot,
                                       &out->obsolete_segments,
                                       &out->obsolete_segment_count,
                                       &out->obsolete_snapshots,
                                       &out->obsolete_snapshot_count);
  lc_free_with_allocator(allocator, manifest_path);
  if (max_snapshot_id > manifest_snapshot_id &&
      !lc_pouch_namespace_manifest_list_contains(
          out->obsolete_snapshots, out->obsolete_snapshot_count,
          scanned_snapshot)) {
    lc_free_with_allocator(allocator, manifest_snapshot);
    manifest_snapshot = scanned_snapshot;
    scanned_snapshot = NULL;
    manifest_snapshot_id = max_snapshot_id;
    out->repaired = 1;
  }
  if (!manifest_valid || active_segment_id < max_segment_id ||
      active_segment_id <= manifest_snapshot_id) {
    active_segment_id = max_segment_id != 0UL ? max_segment_id : 1UL;
    if (active_segment_id <= manifest_snapshot_id) {
      active_segment_id = manifest_snapshot_id + 1UL;
    }
    out->repaired = 1;
  }
  if (active_segment_id == 0UL) {
    active_segment_id = 1UL;
    out->repaired = 1;
  }
  out->active_segment_id = active_segment_id;
  out->latest_snapshot = manifest_snapshot;
  manifest_snapshot = NULL;
  out->latest_snapshot_segment_id = manifest_snapshot_id;
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
      lc_free_with_allocator(allocator, scanned_snapshot);
      lc_pouch_namespace_manifest_cleanup(allocator, out);
      return rc;
    }
  }
  rc = lc_pouch_namespace_manifest_cleanup_obsolete(allocator, namespace_name,
                                                   out, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(allocator, manifest_snapshot);
    lc_free_with_allocator(allocator, scanned_snapshot);
    lc_pouch_namespace_manifest_cleanup(allocator, out);
    return rc;
  }
  lc_free_with_allocator(allocator, manifest_snapshot);
  lc_free_with_allocator(allocator, scanned_snapshot);
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

int lc_pouch_namespace_manifest_install_snapshot(
    const lc_allocator *allocator, const char *namespace_name,
    lc_pouch_namespace_manifest *manifest, const char *snapshot_leaf,
    unsigned long snapshot_segment_id, unsigned long next_segment_id,
    lc_error *error) {
  char *snapshot_copy;
  char *active_segment;
  int rc;

  if (manifest == NULL || snapshot_leaf == NULL || snapshot_leaf[0] == '\0' ||
      snapshot_segment_id == 0UL || next_segment_id <= snapshot_segment_id) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch snapshot install requires manifest, snapshot "
                        "and later active segment",
                        NULL, NULL, NULL);
  }
  snapshot_copy = lc_strdup_with_allocator(allocator, snapshot_leaf);
  if (snapshot_copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch snapshot name", NULL, NULL,
                        NULL);
  }
  active_segment = lc_pouch_namespace_segment_leaf(allocator, next_segment_id);
  if (active_segment == NULL) {
    lc_free_with_allocator(allocator, snapshot_copy);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch active segment name", NULL,
                        NULL, NULL);
  }
  lc_free_with_allocator(allocator, manifest->latest_snapshot);
  lc_free_with_allocator(allocator, manifest->active_segment);
  manifest->latest_snapshot = snapshot_copy;
  manifest->latest_snapshot_segment_id = snapshot_segment_id;
  manifest->active_segment = active_segment;
  manifest->active_segment_id = next_segment_id;
  if (next_segment_id > manifest->max_segment_id) {
    manifest->max_segment_id = next_segment_id;
  }
  rc = lc_pouch_namespace_manifest_write(allocator, namespace_name, manifest,
                                         error);
  if (rc == LC_OK) {
    manifest->repaired = 0;
  }
  return rc;
}

int lc_pouch_namespace_manifest_mark_obsolete_segment(
    const lc_allocator *allocator, lc_pouch_namespace_manifest *manifest,
    const char *segment_leaf, lc_error *error) {
  unsigned long parsed;

  if (manifest == NULL || !lc_pouch_namespace_parse_segment_id(segment_leaf,
                                                               &parsed)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch obsolete segment requires a segment leaf", NULL,
                        NULL, NULL);
  }
  if (manifest->active_segment != NULL &&
      strcmp(manifest->active_segment, segment_leaf) == 0) {
    return LC_OK;
  }
  return lc_pouch_namespace_manifest_list_append(
      allocator, &manifest->obsolete_segments,
      &manifest->obsolete_segment_count, segment_leaf, error);
}

int lc_pouch_namespace_manifest_mark_obsolete_snapshot(
    const lc_allocator *allocator, lc_pouch_namespace_manifest *manifest,
    const char *snapshot_leaf, lc_error *error) {
  unsigned long parsed;

  if (manifest == NULL || !lc_pouch_namespace_parse_snapshot_id(snapshot_leaf,
                                                                &parsed)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch obsolete snapshot requires a snapshot leaf",
                        NULL, NULL, NULL);
  }
  if (manifest->latest_snapshot != NULL &&
      strcmp(manifest->latest_snapshot, snapshot_leaf) == 0) {
    return LC_OK;
  }
  return lc_pouch_namespace_manifest_list_append(
      allocator, &manifest->obsolete_snapshots,
      &manifest->obsolete_snapshot_count, snapshot_leaf, error);
}

int lc_pouch_namespace_manifest_save(const lc_allocator *allocator,
                                     const char *namespace_name,
                                     lc_pouch_namespace_manifest *manifest,
                                     lc_error *error) {
  if (manifest == NULL || namespace_name == NULL || namespace_name[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace manifest save requires namespace",
                        NULL, NULL, NULL);
  }
  return lc_pouch_namespace_manifest_write(allocator, namespace_name, manifest,
                                           error);
}

static int lc_pouch_namespace_unlink_obsolete(
    const lc_allocator *allocator, const char *namespace_path,
    const char *directory, const char *leaf, int *gone) {
  char *directory_path;
  char *path;

  *gone = 0;
  directory_path = lc_pouch_path_join(allocator, namespace_path, directory);
  path = directory_path != NULL ? lc_pouch_path_join(allocator, directory_path,
                                                     leaf)
                                : NULL;
  lc_free_with_allocator(allocator, directory_path);
  if (path == NULL) {
    return 0;
  }
  if (unlink(path) == 0 || errno == ENOENT) {
    *gone = 1;
  }
  lc_free_with_allocator(allocator, path);
  return 1;
}

static int lc_pouch_namespace_manifest_prune_obsolete_list(
    const lc_allocator *allocator, char ***items, unsigned long *count,
    const char *namespace_path, const char *directory, int *changed) {
  unsigned long read_index;
  unsigned long write_index;

  write_index = 0UL;
  for (read_index = 0UL; read_index < *count; ++read_index) {
    int gone;

    gone = 0;
    if (lc_pouch_namespace_unlink_obsolete(allocator, namespace_path,
                                           directory, (*items)[read_index],
                                           &gone) &&
        gone) {
      lc_free_with_allocator(allocator, (*items)[read_index]);
      *changed = 1;
      continue;
    }
    (*items)[write_index++] = (*items)[read_index];
  }
  *count = write_index;
  return LC_OK;
}

int lc_pouch_namespace_manifest_cleanup_obsolete(
    const lc_allocator *allocator, const char *namespace_name,
    lc_pouch_namespace_manifest *manifest, lc_error *error) {
  int changed;
  int rc;

  if (manifest == NULL || manifest->namespace_path == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch obsolete cleanup requires manifest", NULL,
                        NULL, NULL);
  }
  changed = 0;
  rc = lc_pouch_namespace_manifest_prune_obsolete_list(
      allocator, &manifest->obsolete_segments,
      &manifest->obsolete_segment_count, manifest->namespace_path, "segments",
      &changed);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_namespace_manifest_prune_obsolete_list(
      allocator, &manifest->obsolete_snapshots,
      &manifest->obsolete_snapshot_count, manifest->namespace_path, "snapshots",
      &changed);
  if (rc != LC_OK) {
    return rc;
  }
  if (changed) {
    return lc_pouch_namespace_manifest_write(allocator, namespace_name,
                                             manifest, error);
  }
  return LC_OK;
}

int lc_pouch_namespace_touch_marker(const lc_allocator *allocator,
                                    const char *namespace_path,
                                    const char *writer_marker_leaf,
                                    unsigned long sequence, lc_error *error) {
  char text[256];
  char *markers_path;
  char *marker_path;
  int rc;

  if (namespace_path == NULL || writer_marker_leaf == NULL ||
      writer_marker_leaf[0] == '\0' || sequence == 0UL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch marker touch requires namespace path and "
                        "writer marker leaf and non-zero sequence",
                        NULL, NULL, NULL);
  }
  snprintf(text, sizeof(text), "writer_pid=%ld\nwriter_marker=%s\n"
                               "sequence=%020lu\n%s",
           (long)getpid(), writer_marker_leaf, sequence,
           (sequence % 2UL) == 0UL ? "pad=x\n" : "");
  markers_path = lc_pouch_path_join(allocator, namespace_path, "markers");
  marker_path = markers_path != NULL ? lc_pouch_path_join(allocator,
                                                          markers_path,
                                                          writer_marker_leaf)
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
    const char *self_marker_leaf, lc_pouch_namespace_marker_snapshot *out,
    lc_error *error) {
  lc_pouch_marker_entry *entries;
  size_t count;
  int rc;

  if (namespace_path == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch marker snapshot requires namespace path and "
                        "out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  entries = NULL;
  rc = lc_pouch_marker_entries_read(allocator, namespace_path,
                                    self_marker_leaf, &entries, &count, error);
  if (rc != LC_OK) {
    return rc;
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

int lc_pouch_namespace_marker_directory_snapshot_read(
    const lc_allocator *allocator, const char *namespace_path,
    lc_pouch_namespace_marker_directory_snapshot *out, lc_error *error) {
  char *markers_path;
  struct stat st;

  if (namespace_path == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch marker directory snapshot requires namespace "
                        "path and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  markers_path = lc_pouch_path_join(allocator, namespace_path, "markers");
  if (markers_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch markers path", NULL, NULL,
                        NULL);
  }
  if (stat(markers_path, &st) != 0) {
    lc_free_with_allocator(allocator, markers_path);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to stat pouch marker directory",
                        strerror(errno), NULL, NULL);
  }
  lc_free_with_allocator(allocator, markers_path);
  if (!S_ISDIR(st.st_mode)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch marker path is not a directory", NULL, NULL,
                        NULL);
  }
  out->size = (long)st.st_size;
  out->mtime = (long)st.st_mtime;
  return LC_OK;
}

int lc_pouch_namespace_marker_directory_snapshot_changed(
    const lc_pouch_namespace_marker_directory_snapshot *before,
    const lc_pouch_namespace_marker_directory_snapshot *after) {
  if (before == NULL || after == NULL) {
    return 1;
  }
  return before->size != after->size || before->mtime != after->mtime;
}

static int lc_pouch_marker_refresh_cached_peer_stats_changed(
    const lc_allocator *allocator, const char *namespace_path,
    const lc_pouch_namespace_marker_refresh_state *state, int *changed,
    lc_error *error) {
  char *markers_path;
  unsigned long i;
  int rc;

  *changed = 0;
  markers_path = lc_pouch_path_join(allocator, namespace_path, "markers");
  if (markers_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch markers path", NULL, NULL,
                        NULL);
  }
  rc = LC_OK;
  for (i = 0UL; i < state->peer_stat_count; ++i) {
    char *marker_path;
    struct stat st;

    marker_path =
        lc_pouch_path_join(allocator, markers_path, state->peer_stats[i].name);
    if (marker_path == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch marker path", NULL, NULL,
                        NULL);
      break;
    }
    if (stat(marker_path, &st) != 0 || !S_ISREG(st.st_mode) ||
        state->peer_stats[i].size != (long)st.st_size ||
        state->peer_stats[i].mtime != (long)st.st_mtime) {
      *changed = 1;
      lc_free_with_allocator(allocator, marker_path);
      break;
    }
    lc_free_with_allocator(allocator, marker_path);
  }
  lc_free_with_allocator(allocator, markers_path);
  return rc;
}

static int lc_pouch_marker_refresh_full(
    const lc_allocator *allocator, const char *namespace_path,
    const char *self_marker_leaf,
    lc_pouch_namespace_marker_refresh_state *state,
    const lc_pouch_namespace_marker_directory_snapshot *directory,
    int forced, int *should_scan, lc_error *error) {
  lc_pouch_marker_entry *entries;
  lc_pouch_namespace_marker_snapshot peers;
  size_t count;
  int peers_changed;
  int rc;

  memset(&peers, 0, sizeof(peers));
  entries = NULL;
  rc = lc_pouch_marker_entries_read(allocator, namespace_path,
                                    self_marker_leaf, &entries, &count, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_marker_snapshot_build_fingerprint(allocator, entries, count,
                                                  &peers.fingerprint, error);
  if (rc != LC_OK) {
    lc_pouch_marker_entries_cleanup(allocator, entries, count);
    return rc;
  }
  peers.marker_count = (unsigned long)count;
  peers_changed =
      !state->initialized ||
      lc_pouch_namespace_marker_snapshot_changed(&state->peers, &peers);
  lc_pouch_namespace_marker_snapshot_cleanup(allocator, &state->peers);
  lc_pouch_marker_refresh_peer_stats_cleanup(allocator, state);
  state->peers = peers;
  state->peer_stats = entries;
  state->peer_stat_count = (unsigned long)count;
  memset(&peers, 0, sizeof(peers));
  entries = NULL;
  state->directory = *directory;
  state->initialized = 1;
  state->skipped_refreshes = 0UL;
  *should_scan = forced || peers_changed;
  return LC_OK;
}

int lc_pouch_namespace_marker_refresh_should_scan(
    const lc_allocator *allocator, const char *namespace_path,
    const char *self_marker_leaf,
    lc_pouch_namespace_marker_refresh_state *state,
    unsigned long force_after_skips, int *should_scan, lc_error *error) {
  lc_pouch_namespace_marker_directory_snapshot directory;
  int cached_peers_changed;
  int directory_changed;
  int forced;
  int rc;

  if (state == NULL || should_scan == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch marker refresh requires state and scan output",
                        NULL, NULL, NULL);
  }
  memset(&directory, 0, sizeof(directory));
  rc = lc_pouch_namespace_marker_directory_snapshot_read(
      allocator, namespace_path, &directory, error);
  if (rc != LC_OK) {
    return rc;
  }
  directory_changed =
      !state->initialized ||
      lc_pouch_namespace_marker_directory_snapshot_changed(&state->directory,
                                                           &directory);
  forced = state->initialized && force_after_skips > 0UL &&
           state->skipped_refreshes >= force_after_skips;
  if (!directory_changed && !forced) {
    rc = lc_pouch_marker_refresh_cached_peer_stats_changed(
        allocator, namespace_path, state, &cached_peers_changed, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (cached_peers_changed) {
      return lc_pouch_marker_refresh_full(
          allocator, namespace_path, self_marker_leaf, state, &directory, 0,
          should_scan, error);
    }
    ++state->skipped_refreshes;
    *should_scan = 0;
    return LC_OK;
  }
  return lc_pouch_marker_refresh_full(allocator, namespace_path,
                                      self_marker_leaf, state, &directory,
                                      forced, should_scan, error);
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

void lc_pouch_namespace_marker_refresh_state_cleanup(
    const lc_allocator *allocator,
    lc_pouch_namespace_marker_refresh_state *state) {
  if (state == NULL) {
    return;
  }
  lc_pouch_namespace_marker_snapshot_cleanup(allocator, &state->peers);
  lc_pouch_marker_refresh_peer_stats_cleanup(allocator, state);
  memset(state, 0, sizeof(*state));
}

void lc_pouch_namespace_manifest_cleanup(
    const lc_allocator *allocator, lc_pouch_namespace_manifest *manifest) {
  if (manifest == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, manifest->namespace_path);
  lc_free_with_allocator(allocator, manifest->active_segment);
  lc_free_with_allocator(allocator, manifest->latest_snapshot);
  lc_pouch_namespace_manifest_list_cleanup(
      allocator, manifest->obsolete_segments,
      manifest->obsolete_segment_count);
  lc_pouch_namespace_manifest_list_cleanup(
      allocator, manifest->obsolete_snapshots,
      manifest->obsolete_snapshot_count);
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
    rc = lc_pouch_namespace_ensure_child(
        allocator, namespace_path, "queue-notify",
        "failed to create pouch namespace queue notification directory",
        error);
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

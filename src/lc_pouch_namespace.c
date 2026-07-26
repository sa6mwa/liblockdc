#include "lc_pouch_namespace.h"

#include "lc_pouch_format.h"
#include "lc_pouch_path.h"

#include <stdio.h>
#include <string.h>

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

int lc_pouch_namespace_ensure(const lc_allocator *allocator,
                              const char *root_path,
                              const char *namespace_name, lc_error *error) {
  char manifest[160];
  char *namespace_path;
  char *manifest_path;
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
  manifest_path =
      rc == LC_OK ? lc_pouch_path_join(allocator, namespace_path, "manifest")
                  : NULL;
  if (rc == LC_OK && manifest_path == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch namespace manifest path", NULL,
                      NULL, NULL);
  }
  if (rc == LC_OK) {
    snprintf(manifest, sizeof(manifest),
             "layout=%s\nversion=%lu\nnamespace=%s\n",
             LC_POUCH_LAYOUT_NAME, LC_POUCH_LAYOUT_VERSION, namespace_name);
    rc = lc_pouch_path_write_text_file(manifest_path, manifest, error);
  }
  lc_free_with_allocator(allocator, manifest_path);
  lc_free_with_allocator(allocator, namespace_path);
  return rc;
}

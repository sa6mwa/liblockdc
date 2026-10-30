#include "lc_collection_internal.h"
#include "lc_pouch_path.h"

#include <dirent.h>
#include <errno.h>
#include <string.h>

#ifdef LOCKDC_TEST_BUILD
lc_collection_migrate_hook_fn lc_collection_migrate_test_hook;
void *lc_collection_migrate_test_context;
#endif

static int lc_cm_phase(const char *path, int published, lc_error *error) {
#ifdef LOCKDC_TEST_BUILD
  if (lc_collection_migrate_test_hook != NULL)
    return lc_collection_migrate_test_hook(lc_collection_migrate_test_context,
                                           path, published, error);
#else
  (void)path;
  (void)published;
  (void)error;
#endif
  return LC_OK;
}

static int lc_cm_catalog(lc_pouch *pouch, const char *base, lc_error *error) {
  char id[LC_XID_STRING_SIZE], name[256];
  char *names, *data, *catalog, *partition;
  DIR *dir = NULL;
  struct dirent *entry;
  int rc;
  names = lc_pouch_path_join(&pouch->allocator, base, "names");
  data = lc_pouch_path_join(&pouch->allocator, base, "data");
  rc = names != NULL && data != NULL ? LC_OK : LC_ERR_NOMEM;
  if (rc == LC_OK) {
    dir = opendir(names);
    if (dir == NULL && errno != ENOENT)
      rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to open collection upgrade catalog");
  }
  while (rc == LC_OK && dir != NULL) {
    errno = 0;
    entry = readdir(dir);
    if (entry == NULL) {
      if (errno != 0)
        rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                   "failed to read collection upgrade catalog");
      break;
    }
    if (strlen(entry->d_name) != 64U ||
        strspn(entry->d_name, "0123456789abcdef") != 64U)
      continue;
    catalog = lc_pouch_path_join(&pouch->allocator, names, entry->d_name);
    rc = catalog != NULL ? lc_collection_catalog_read(catalog, id, name, error)
                         : LC_ERR_NOMEM;
    partition =
        rc == LC_OK ? lc_pouch_path_join(&pouch->allocator, data, id) : NULL;
    if (rc == LC_OK && partition == NULL)
      rc = LC_ERR_NOMEM;
    if (rc == LC_OK)
      rc = lc_cm_phase(partition, 0, error);
    if (rc == LC_OK)
      rc = lc_collection_migrate_head(pouch, partition, error);
    if (rc == LC_OK)
      rc = lc_cm_phase(partition, 1, error);
    lc_free_with_allocator(&pouch->allocator, partition);
    lc_free_with_allocator(&pouch->allocator, catalog);
  }
  if (dir != NULL && closedir(dir) != 0 && rc == LC_OK)
    rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                               "failed to close collection upgrade catalog");
  lc_free_with_allocator(&pouch->allocator, names);
  lc_free_with_allocator(&pouch->allocator, data);
  return rc;
}

int lc_collection_migrate_heads(lc_pouch *pouch, lc_error *error) {
  char *path, *ns, *base;
  DIR *dir;
  struct dirent *entry;
  int rc;
  path = lc_pouch_path_join(&pouch->allocator, pouch->root_path, "namespaces");
  if (path == NULL)
    return lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                                 "failed to allocate collection upgrade root");
  dir = opendir(path);
  rc = dir != NULL || errno == ENOENT
           ? LC_OK
           : lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                   "failed to open collection upgrade root");
  while (rc == LC_OK && dir != NULL) {
    errno = 0;
    entry = readdir(dir);
    if (entry == NULL) {
      if (errno != 0)
        rc = lc_collection_failure(
            error, LC_ERR_TRANSPORT, NULL,
            "failed to enumerate collection upgrade root");
      break;
    }
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
      continue;
    ns = lc_pouch_path_join(&pouch->allocator, path, entry->d_name);
    base = ns != NULL ? lc_pouch_path_join(&pouch->allocator, ns, "collections")
                      : NULL;
    rc = base != NULL ? lc_cm_catalog(pouch, base, error) : LC_ERR_NOMEM;
    lc_free_with_allocator(&pouch->allocator, base);
    lc_free_with_allocator(&pouch->allocator, ns);
  }
  if (dir != NULL && closedir(dir) != 0 && rc == LC_OK)
    rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                               "failed to close collection upgrade root");
  lc_free_with_allocator(&pouch->allocator, path);
  if (rc == LC_ERR_NOMEM)
    return lc_collection_failure(error, rc, NULL,
                                 "failed to allocate collection upgrade paths");
  return rc;
}

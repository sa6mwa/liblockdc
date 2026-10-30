#ifndef LC_COLLECTION_MIGRATION_H
#define LC_COLLECTION_MIGRATION_H

#include "lc_pouch.h"

#define LC_COLLECTION_HEAD_BYTES 384U

/* Root-open only, under exclusive root ownership and the manifest lock.
 * Walk catalog bindings, never entry bodies or derived projections. */
int lc_collection_migrate_heads(lc_pouch *pouch, lc_error *error);
int lc_collection_migrate_head(lc_pouch *pouch, const char *directory,
                               lc_error *error);

#ifdef LOCKDC_TEST_BUILD
typedef int (*lc_collection_migrate_hook_fn)(void *context, const char *path,
                                             int published, lc_error *error);
extern lc_collection_migrate_hook_fn lc_collection_migrate_test_hook;
extern void *lc_collection_migrate_test_context;
#endif

#endif

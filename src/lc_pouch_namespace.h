#ifndef LC_POUCH_NAMESPACE_H
#define LC_POUCH_NAMESPACE_H

#include "lc_pouch.h"

int lc_pouch_namespace_ensure(const lc_allocator *allocator,
                              const char *root_path,
                              const char *namespace_name, lc_error *error);
char *lc_pouch_namespace_path(const lc_allocator *allocator,
                              const char *root_path,
                              const char *namespace_name);

typedef struct lc_pouch_namespace_manifest {
  char *namespace_path;
  char *active_segment;
  unsigned long active_segment_id;
  unsigned long max_segment_id;
  int repaired;
} lc_pouch_namespace_manifest;

char *lc_pouch_namespace_segment_leaf(const lc_allocator *allocator,
                                      unsigned long segment_id);
int lc_pouch_namespace_manifest_open(const lc_allocator *allocator,
                                     const char *root_path,
                                     const char *namespace_name,
                                     lc_pouch_namespace_manifest *out,
                                     lc_error *error);
int lc_pouch_namespace_manifest_rotate(const lc_allocator *allocator,
                                       const char *namespace_name,
                                       lc_pouch_namespace_manifest *manifest,
                                       unsigned long segment_id,
                                       lc_error *error);
int lc_pouch_namespace_touch_marker(const lc_allocator *allocator,
                                    const char *namespace_path,
                                    unsigned long sequence, lc_error *error);
void lc_pouch_namespace_manifest_cleanup(
    const lc_allocator *allocator, lc_pouch_namespace_manifest *manifest);

#endif

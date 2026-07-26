#ifndef LC_POUCH_NAMESPACE_H
#define LC_POUCH_NAMESPACE_H

#include "lc_pouch.h"

int lc_pouch_namespace_ensure(const lc_allocator *allocator,
                              const char *root_path,
                              const char *namespace_name, lc_error *error);
char *lc_pouch_namespace_path(const lc_allocator *allocator,
                              const char *root_path,
                              const char *namespace_name);

#endif

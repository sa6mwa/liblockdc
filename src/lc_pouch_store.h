#ifndef LC_POUCH_STORE_H
#define LC_POUCH_STORE_H

#include "lc/lc.h"

#include <stddef.h>

typedef struct lc_pouch_store lc_pouch_store;

typedef struct lc_pouch_allocator {
  void *(*malloc_fn)(void *context, size_t size);
  void *(*calloc_fn)(void *context, size_t count, size_t size);
  void *(*realloc_fn)(void *context, void *ptr, size_t size);
  void (*free_fn)(void *context, void *ptr);
  void *context;
} lc_pouch_allocator;

typedef struct lc_pouch_state_info {
  int no_content;
  char *content_type;
  char *etag;
  long version;
  long bytes;
} lc_pouch_state_info;

typedef struct lc_pouch_put_state_opts {
  const char *content_type;
  const char *if_state_etag;
  long if_version;
  int has_if_version;
} lc_pouch_put_state_opts;

typedef struct lc_pouch_put_state_res {
  long new_version;
  char *new_state_etag;
  long bytes;
} lc_pouch_put_state_res;

struct lc_pouch_store {
  void *impl;
  int (*read_state)(lc_pouch_store *self, const char *namespace_name,
                    const char *key, lc_source **body, lc_pouch_state_info *out,
                    lc_error *error);
  int (*write_state)(lc_pouch_store *self, const char *namespace_name,
                     const char *key, lc_source *body,
                     const lc_pouch_put_state_opts *opts,
                     lc_pouch_put_state_res *out, lc_error *error);
  int (*remove_state)(lc_pouch_store *self, const char *namespace_name,
                      const char *key, const char *expected_etag,
                      lc_error *error);
  int (*close)(lc_pouch_store *self, lc_error *error);
  int (*abort)(lc_pouch_store *self, lc_error *error);
};

void lc_pouch_allocator_from_lc(const lc_allocator *src,
                                lc_pouch_allocator *dst);
void *lc_pouch_alloc(const lc_pouch_allocator *allocator, size_t size);
void *lc_pouch_calloc(const lc_pouch_allocator *allocator, size_t count,
                      size_t size);
void *lc_pouch_realloc(const lc_pouch_allocator *allocator, void *ptr,
                       size_t size);
void lc_pouch_free(const lc_pouch_allocator *allocator, void *ptr);
char *lc_pouch_strdup(const lc_pouch_allocator *allocator, const char *value);
char *lc_pouch_dup_bytes(const lc_pouch_allocator *allocator, const void *bytes,
                         size_t length);

void lc_pouch_state_info_cleanup(const lc_pouch_allocator *allocator,
                                 lc_pouch_state_info *info);
void lc_pouch_put_state_res_cleanup(const lc_pouch_allocator *allocator,
                                    lc_pouch_put_state_res *res);

int lc_pouch_disk_open(const char *root_path,
                       const lc_pouch_allocator *allocator,
                       lc_pouch_store **out, lc_error *error);

#endif

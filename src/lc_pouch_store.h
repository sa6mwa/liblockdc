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

typedef struct lc_pouch_meta {
  char *owner;
  char *lease_id;
  char *txn_id;
  char *state_etag;
  long version;
  long lease_expires_at_unix;
  long fencing_token;
  int has_query_hidden;
  int query_hidden;
} lc_pouch_meta;

typedef struct lc_pouch_meta_record {
  int found;
  char *namespace_name;
  char *key;
  char *etag;
  lc_pouch_meta meta;
} lc_pouch_meta_record;

typedef struct lc_pouch_store_meta_res {
  char *etag;
  long version;
} lc_pouch_store_meta_res;

typedef struct lc_pouch_object_info {
  char *id;
  char *name;
  long size;
  char *plaintext_sha256;
  char *content_type;
  long created_at_unix;
  long updated_at_unix;
} lc_pouch_object_info;

typedef struct lc_pouch_object_list {
  lc_pouch_object_info *items;
  size_t count;
} lc_pouch_object_list;

typedef struct lc_pouch_put_object_opts {
  const char *name;
  const char *content_type;
  long max_bytes;
  int has_max_bytes;
  int prevent_overwrite;
} lc_pouch_put_object_opts;

typedef struct lc_pouch_object_selector {
  const char *id;
  const char *name;
} lc_pouch_object_selector;

struct lc_pouch_store {
  void *impl;
  int (*load_meta)(lc_pouch_store *self, const char *namespace_name,
                   const char *key, lc_pouch_meta_record *out, lc_error *error);
  int (*store_meta)(lc_pouch_store *self, const char *namespace_name,
                    const char *key, const lc_pouch_meta *meta,
                    const char *expected_etag, lc_pouch_store_meta_res *out,
                    lc_error *error);
  int (*delete_meta)(lc_pouch_store *self, const char *namespace_name,
                     const char *key, const char *expected_etag,
                     lc_error *error);
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
  int (*put_object)(lc_pouch_store *self, const char *namespace_name,
                    const char *key, lc_source *body,
                    const lc_pouch_put_object_opts *opts,
                    lc_pouch_object_info *out, lc_error *error);
  int (*list_objects)(lc_pouch_store *self, const char *namespace_name,
                      const char *key, lc_pouch_object_list *out,
                      lc_error *error);
  int (*get_object)(lc_pouch_store *self, const char *namespace_name,
                    const char *key, const lc_pouch_object_selector *selector,
                    lc_source **body, lc_pouch_object_info *out,
                    lc_error *error);
  int (*delete_object)(lc_pouch_store *self, const char *namespace_name,
                       const char *key,
                       const lc_pouch_object_selector *selector, int *deleted,
                       lc_error *error);
  int (*delete_all_objects)(lc_pouch_store *self, const char *namespace_name,
                            const char *key, int *deleted_count,
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
void lc_pouch_meta_cleanup(const lc_pouch_allocator *allocator,
                           lc_pouch_meta *meta);
void lc_pouch_meta_record_cleanup(const lc_pouch_allocator *allocator,
                                  lc_pouch_meta_record *record);
void lc_pouch_store_meta_res_cleanup(const lc_pouch_allocator *allocator,
                                     lc_pouch_store_meta_res *res);
void lc_pouch_object_info_cleanup(const lc_pouch_allocator *allocator,
                                  lc_pouch_object_info *info);
void lc_pouch_object_list_cleanup(const lc_pouch_allocator *allocator,
                                  lc_pouch_object_list *list);

int lc_pouch_disk_open(const char *root_path,
                       const lc_pouch_allocator *allocator,
                       lc_pouch_store **out, lc_error *error);

#endif

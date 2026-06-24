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

typedef struct lc_pouch_promote_staged_opts {
  const char *expected_head_etag;
} lc_pouch_promote_staged_opts;

typedef struct lc_pouch_discard_staged_opts {
  const char *expected_etag;
  int ignore_not_found;
} lc_pouch_discard_staged_opts;

typedef struct lc_pouch_list_staged_req {
  const char *namespace_name;
  const char *key;
  const char *start_after;
  size_t limit;
} lc_pouch_list_staged_req;

typedef struct lc_pouch_staged_state_info {
  char *key;
  char *txn_id;
  char *content_type;
  char *etag;
  long version;
  long bytes;
} lc_pouch_staged_state_info;

typedef struct lc_pouch_staged_state_list {
  lc_pouch_staged_state_info *items;
  size_t count;
  int truncated;
  char *next_start_after;
} lc_pouch_staged_state_list;

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

typedef struct lc_pouch_scan_meta_req {
  const char *namespace_name;
  const char *start_after;
  size_t limit;
} lc_pouch_scan_meta_req;

typedef struct lc_pouch_scan_meta_row {
  const char *key;
  const char *etag;
  const lc_pouch_meta *meta;
} lc_pouch_scan_meta_row;

typedef struct lc_pouch_scan_meta_res {
  size_t visited;
  int truncated;
  char *next_start_after;
} lc_pouch_scan_meta_res;

typedef int (*lc_pouch_scan_meta_visit_fn)(
    void *context, const lc_pouch_scan_meta_row *row, lc_error *error);

typedef struct lc_pouch_query_index_scan_req {
  const char *namespace_name;
  const char *start_after;
  size_t limit;
} lc_pouch_query_index_scan_req;

typedef struct lc_pouch_query_index_scan_res {
  size_t visited;
  int truncated;
  char *next_start_after;
  unsigned long index_seq;
} lc_pouch_query_index_scan_res;

typedef struct lc_pouch_index_flush_res {
  char *namespace_name;
  char *mode;
  char *flush_id;
  int accepted;
  int flushed;
  int pending;
  unsigned long index_seq;
} lc_pouch_index_flush_res;

typedef struct lc_pouch_query_config {
  char *preferred_engine;
  char *fallback_engine;
} lc_pouch_query_config;

typedef struct lc_pouch_disk_open_opts {
  const char *query_engine;
  const char *query_fallback_engine;
} lc_pouch_disk_open_opts;

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

typedef struct lc_pouch_copy_object_opts {
  lc_pouch_object_selector source;
  const char *name;
  int prevent_overwrite;
} lc_pouch_copy_object_opts;

typedef struct lc_pouch_queue_message_info {
  char *namespace_name;
  char *queue;
  char *message_id;
  char *payload_content_type;
  char *lease_id;
  char *txn_id;
  char *meta_etag;
  int attempts;
  int max_attempts;
  int failure_attempts;
  long enqueued_at_unix;
  long not_visible_until_unix;
  long visibility_timeout_seconds;
  long expires_at_unix;
  long lease_expires_at_unix;
  long fencing_token;
  long payload_bytes;
} lc_pouch_queue_message_info;

typedef struct lc_pouch_enqueue_opts {
  const char *content_type;
  long delay_seconds;
  long visibility_timeout_seconds;
  long ttl_seconds;
  int max_attempts;
} lc_pouch_enqueue_opts;

typedef struct lc_pouch_dequeue_opts {
  const char *owner;
  const char *txn_id;
  long visibility_timeout_seconds;
} lc_pouch_dequeue_opts;

typedef struct lc_pouch_queue_ref {
  const char *namespace_name;
  const char *queue;
  const char *message_id;
  const char *lease_id;
  const char *txn_id;
  long fencing_token;
  const char *meta_etag;
} lc_pouch_queue_ref;

typedef struct lc_pouch_queue_stats {
  int available;
  int pending_candidates;
  char *head_message_id;
  long head_enqueued_at_unix;
  long head_not_visible_until_unix;
} lc_pouch_queue_stats;

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
  int (*scan_meta)(lc_pouch_store *self, const lc_pouch_scan_meta_req *req,
                   lc_pouch_scan_meta_visit_fn visit, void *visit_context,
                   lc_pouch_scan_meta_res *out, lc_error *error);
  int (*query_index_scan)(lc_pouch_store *self,
                          const lc_pouch_query_index_scan_req *req,
                          lc_pouch_scan_meta_visit_fn visit,
                          void *visit_context,
                          lc_pouch_query_index_scan_res *out,
                          lc_error *error);
  int (*flush_index)(lc_pouch_store *self, const char *namespace_name,
                     const char *mode, lc_pouch_index_flush_res *out,
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
                      int *removed, lc_error *error);
  int (*stage_state)(lc_pouch_store *self, const char *namespace_name,
                     const char *key, const char *txn_id, lc_source *body,
                     const lc_pouch_put_state_opts *opts,
                     lc_pouch_put_state_res *out, lc_error *error);
  int (*load_staged_state)(lc_pouch_store *self, const char *namespace_name,
                           const char *key, const char *txn_id,
                           lc_source **body, lc_pouch_state_info *out,
                           lc_error *error);
  int (*promote_staged_state)(lc_pouch_store *self, const char *namespace_name,
                              const char *key, const char *txn_id,
                              const lc_pouch_promote_staged_opts *opts,
                              lc_pouch_put_state_res *out, lc_error *error);
  int (*discard_staged_state)(lc_pouch_store *self, const char *namespace_name,
                              const char *key, const char *txn_id,
                              const lc_pouch_discard_staged_opts *opts,
                              lc_error *error);
  int (*list_staged_state)(lc_pouch_store *self,
                           const lc_pouch_list_staged_req *req,
                           lc_pouch_staged_state_list *out, lc_error *error);
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
  int (*copy_object)(lc_pouch_store *self, const char *namespace_name,
                     const char *src_key, const char *dst_key,
                     const lc_pouch_copy_object_opts *opts,
                     lc_pouch_object_info *out, lc_error *error);
  int (*delete_object)(lc_pouch_store *self, const char *namespace_name,
                       const char *key,
                       const lc_pouch_object_selector *selector, int *deleted,
                       lc_error *error);
  int (*delete_all_objects)(lc_pouch_store *self, const char *namespace_name,
                            const char *key, int *deleted_count,
                            lc_error *error);
  int (*enqueue_message)(lc_pouch_store *self, const char *namespace_name,
                         const char *queue, lc_source *body,
                         const lc_pouch_enqueue_opts *opts,
                         lc_pouch_queue_message_info *out, lc_error *error);
  int (*dequeue_message)(lc_pouch_store *self, const char *namespace_name,
                         const char *queue, const lc_pouch_dequeue_opts *opts,
                         lc_source **body, lc_pouch_queue_message_info *out,
                         lc_error *error);
  int (*ack_message)(lc_pouch_store *self, const lc_pouch_queue_ref *ref,
                     int *acked, lc_error *error);
  int (*nack_message)(lc_pouch_store *self, const lc_pouch_queue_ref *ref,
                      long delay_seconds, int count_failure,
                      lc_pouch_queue_message_info *out, lc_error *error);
  int (*extend_message)(lc_pouch_store *self, const lc_pouch_queue_ref *ref,
                        long extend_by_seconds,
                        lc_pouch_queue_message_info *out, lc_error *error);
  int (*queue_stats)(lc_pouch_store *self, const char *namespace_name,
                     const char *queue, lc_pouch_queue_stats *out,
                     lc_error *error);
  int (*query_config)(lc_pouch_store *self, const char *namespace_name,
                      lc_pouch_query_config *out, lc_error *error);
  int (*backend_hash)(lc_pouch_store *self, char **out, lc_error *error);
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
void lc_pouch_scan_meta_res_cleanup(const lc_pouch_allocator *allocator,
                                    lc_pouch_scan_meta_res *res);
void lc_pouch_query_index_scan_res_cleanup(
    const lc_pouch_allocator *allocator, lc_pouch_query_index_scan_res *res);
void lc_pouch_index_flush_res_cleanup(const lc_pouch_allocator *allocator,
                                      lc_pouch_index_flush_res *res);
void lc_pouch_query_config_cleanup(const lc_pouch_allocator *allocator,
                                   lc_pouch_query_config *config);
void lc_pouch_staged_state_info_cleanup(const lc_pouch_allocator *allocator,
                                        lc_pouch_staged_state_info *info);
void lc_pouch_staged_state_list_cleanup(const lc_pouch_allocator *allocator,
                                        lc_pouch_staged_state_list *list);
void lc_pouch_object_info_cleanup(const lc_pouch_allocator *allocator,
                                  lc_pouch_object_info *info);
void lc_pouch_object_list_cleanup(const lc_pouch_allocator *allocator,
                                  lc_pouch_object_list *list);
void lc_pouch_queue_message_info_cleanup(const lc_pouch_allocator *allocator,
                                         lc_pouch_queue_message_info *info);
void lc_pouch_queue_stats_cleanup(const lc_pouch_allocator *allocator,
                                  lc_pouch_queue_stats *stats);

int lc_pouch_disk_open(const char *root_path,
                       const lc_pouch_allocator *allocator,
                       lc_pouch_store **out, lc_error *error);
int lc_pouch_disk_open_with_options(const char *root_path,
                                    const lc_pouch_allocator *allocator,
                                    const lc_pouch_disk_open_opts *opts,
                                    lc_pouch_store **out, lc_error *error);

#endif

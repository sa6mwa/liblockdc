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
  long updated_at_unix;
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
  const char *key;
  const char *owner;
  const char *start_after;
  size_t limit;
  int include_hidden;
  int exclude_deleted_state;
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

typedef struct lc_pouch_namespace_list {
  char **names;
  size_t count;
} lc_pouch_namespace_list;

typedef int (*lc_pouch_scan_meta_visit_fn)(void *context,
                                           const lc_pouch_scan_meta_row *row,
                                           lc_error *error);

typedef int (*lc_pouch_query_index_key_visit_fn)(void *context, const char *key,
                                                 lc_error *error);

typedef struct lc_pouch_document_eq_term {
  const char *field;
  const char *value;
} lc_pouch_document_eq_term;

typedef struct lc_pouch_document_range_term {
  const char *field;
  const char *gt;
  const char *gte;
  const char *lt;
  const char *lte;
} lc_pouch_document_range_term;

typedef struct lc_pouch_document_in_term {
  const char *field;
  const char *const *values;
  size_t value_count;
} lc_pouch_document_in_term;

typedef struct lc_pouch_document_prefix_term {
  const char *field;
  const char *value;
  int ignore_case;
} lc_pouch_document_prefix_term;

typedef struct lc_pouch_document_contains_term {
  const char *field;
  const char *value;
  int ignore_case;
} lc_pouch_document_contains_term;

typedef struct lc_pouch_document_exists_term {
  const char *field;
} lc_pouch_document_exists_term;

typedef struct lc_pouch_query_index_scan_req {
  const char *namespace_name;
  const char *key;
  const char *owner;
  const lc_pouch_document_eq_term *document_eq_terms;
  size_t document_eq_term_count;
  const lc_pouch_document_eq_term *document_not_eq_terms;
  size_t document_not_eq_term_count;
  const lc_pouch_document_eq_term *document_or_eq_terms;
  size_t document_or_eq_term_count;
  const lc_pouch_document_range_term *document_range_terms;
  size_t document_range_term_count;
  const lc_pouch_document_range_term *document_not_range_terms;
  size_t document_not_range_term_count;
  const lc_pouch_document_range_term *document_or_range_terms;
  size_t document_or_range_term_count;
  const lc_pouch_document_in_term *document_in_terms;
  size_t document_in_term_count;
  const lc_pouch_document_in_term *document_not_in_terms;
  size_t document_not_in_term_count;
  const lc_pouch_document_in_term *document_or_in_terms;
  size_t document_or_in_term_count;
  const lc_pouch_document_prefix_term *document_prefix_terms;
  size_t document_prefix_term_count;
  const lc_pouch_document_prefix_term *document_not_prefix_terms;
  size_t document_not_prefix_term_count;
  const lc_pouch_document_prefix_term *document_or_prefix_terms;
  size_t document_or_prefix_term_count;
  const lc_pouch_document_contains_term *document_contains_terms;
  size_t document_contains_term_count;
  const lc_pouch_document_contains_term *document_not_contains_terms;
  size_t document_not_contains_term_count;
  const lc_pouch_document_contains_term *document_or_contains_terms;
  size_t document_or_contains_term_count;
  const lc_pouch_document_exists_term *document_exists_terms;
  size_t document_exists_term_count;
  const lc_pouch_document_exists_term *document_not_exists_terms;
  size_t document_not_exists_term_count;
  const lc_pouch_document_exists_term *document_or_exists_terms;
  size_t document_or_exists_term_count;
  const lc_pouch_document_exists_term *document_exists_path_patterns;
  size_t document_exists_path_pattern_count;
  const lc_pouch_document_exists_term *document_or_exists_path_patterns;
  size_t document_or_exists_path_pattern_count;
  const char *start_after;
  size_t limit;
} lc_pouch_query_index_scan_req;

typedef struct lc_pouch_query_owner_scan_req {
  const char *namespace_name;
  const char *owner;
  const char *start_after;
  size_t limit;
} lc_pouch_query_owner_scan_req;

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

typedef struct lc_pouch_compaction_res {
  char *mode;
  char *skip_reason;
  int accepted;
  int compacted;
  int skipped;
  unsigned long before_log_bytes;
  unsigned long after_log_bytes;
  unsigned long before_query_index_bytes;
  unsigned long after_query_index_bytes;
  unsigned long before_record_count;
  unsigned long after_record_count;
  unsigned long live_record_count;
} lc_pouch_compaction_res;

typedef struct lc_pouch_maintenance_res {
  char *mode;
  char *reason;
  int accepted;
  int compaction_enabled;
  lc_pouch_compaction_res compaction;
} lc_pouch_maintenance_res;

typedef struct lc_pouch_retention_sweep_req {
  long updated_before_unix;
} lc_pouch_retention_sweep_req;

typedef struct lc_pouch_retention_sweep_res {
  unsigned long scanned_metadata;
  unsigned long expired_metadata;
  unsigned long deleted_metadata;
  unsigned long deleted_state;
  unsigned long failed_keys;
} lc_pouch_retention_sweep_res;

typedef struct lc_pouch_query_config {
  char *preferred_engine;
  char *fallback_engine;
} lc_pouch_query_config;

typedef struct lc_pouch_disk_open_opts {
  const char *query_engine;
  const char *query_fallback_engine;
  int single_writer;
  int background_compaction;
  unsigned long background_compaction_min_log_bytes;
  unsigned long background_compaction_obsolete_multiplier;
  unsigned long background_compaction_interval_seconds;
  unsigned long background_compaction_min_candidate_files;
  unsigned long background_compaction_delete_grace_seconds;
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

typedef struct lc_pouch_scan_object_keys_req {
  const char *namespace_name;
  const char *name;
  const char *start_after;
  size_t limit;
} lc_pouch_scan_object_keys_req;

typedef struct lc_pouch_scan_object_keys_res {
  char *next_start_after;
  size_t visited;
  int truncated;
} lc_pouch_scan_object_keys_res;

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
  const char *expected_etag;
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
  const char *start_after;
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
  char *correlation_id;
} lc_pouch_queue_stats;

typedef struct lc_pouch_queue_wake_status {
  char *mode;
  char *reason;
  int uses_marker_hints;
  int uses_filesystem_notifications;
} lc_pouch_queue_wake_status;

typedef struct lc_pouch_key_lock lc_pouch_key_lock;

typedef struct lc_pouch_fsync_stats {
  unsigned long attempted_fsyncs;
  unsigned long failed_fsyncs;
  unsigned long log_fsyncs;
  unsigned long query_index_fsyncs;
  unsigned long root_fsyncs;
  unsigned long writer_marker_fsyncs;
  unsigned long queue_wake_fsyncs;
} lc_pouch_fsync_stats;

typedef struct lc_pouch_writer_status {
  char *mode;
  char *marker_prefix;
  int own_marker_present;
  size_t active_marker_count;
  size_t other_marker_count;
  size_t stale_marker_count;
  unsigned long heartbeat_sequence;
} lc_pouch_writer_status;

typedef struct lc_pouch_lock_status {
  char *mode;
  char *path;
  int uses_fcntl_byte_range_lock;
  int uses_global_writer_lock;
  int uses_per_key_lock_cache;
  size_t key_lock_stripe_count;
  size_t process_active_key_locks;
  unsigned long lock_acquisitions;
  unsigned long lock_releases;
  unsigned long process_key_lock_contentions;
  unsigned long replay_refreshes;
  unsigned long log_reopens;
} lc_pouch_lock_status;

typedef struct lc_pouch_lock_fd_cache_status {
  size_t capacity;
  size_t entries;
  unsigned long hits;
  unsigned long misses;
  unsigned long evictions;
  unsigned long closes;
} lc_pouch_lock_fd_cache_status;

typedef struct lc_pouch_read_fd_cache_status {
  size_t capacity;
  size_t entries;
  unsigned long hits;
  unsigned long misses;
  unsigned long stale;
  unsigned long evictions;
  unsigned long closes;
} lc_pouch_read_fd_cache_status;

typedef struct lc_pouch_backend_capabilities {
  char *backend_kind;
  char *write_coordination;
  int serializes_same_root_writers;
  int general_concurrent_writer_backend;
  int supports_crash_abort_marker;
  int supports_backend_hash;
} lc_pouch_backend_capabilities;

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
  int (*list_namespaces)(lc_pouch_store *self, lc_pouch_namespace_list *out,
                         lc_error *error);
  int (*scan_meta)(lc_pouch_store *self, const lc_pouch_scan_meta_req *req,
                   lc_pouch_scan_meta_visit_fn visit, void *visit_context,
                   lc_pouch_scan_meta_res *out, lc_error *error);
  int (*scan_meta_keys)(lc_pouch_store *self, const lc_pouch_scan_meta_req *req,
                        lc_pouch_query_index_key_visit_fn visit,
                        void *visit_context, lc_pouch_scan_meta_res *out,
                        lc_error *error);
  int (*query_index_scan)(lc_pouch_store *self,
                          const lc_pouch_query_index_scan_req *req,
                          lc_pouch_scan_meta_visit_fn visit,
                          void *visit_context,
                          lc_pouch_query_index_scan_res *out, lc_error *error);
  int (*query_index_keys_scan)(lc_pouch_store *self,
                               const lc_pouch_query_index_scan_req *req,
                               lc_pouch_query_index_key_visit_fn visit,
                               void *visit_context,
                               lc_pouch_query_index_scan_res *out,
                               lc_error *error);
  int (*query_owner_scan)(lc_pouch_store *self,
                          const lc_pouch_query_owner_scan_req *req,
                          lc_pouch_scan_meta_visit_fn visit,
                          void *visit_context,
                          lc_pouch_query_index_scan_res *out, lc_error *error);
  int (*query_owner_keys_scan)(lc_pouch_store *self,
                               const lc_pouch_query_owner_scan_req *req,
                               lc_pouch_query_index_key_visit_fn visit,
                               void *visit_context,
                               lc_pouch_query_index_scan_res *out,
                               lc_error *error);
  int (*flush_index)(lc_pouch_store *self, const char *namespace_name,
                     const char *mode, lc_pouch_index_flush_res *out,
                     lc_error *error);
  int (*compact)(lc_pouch_store *self, const char *mode,
                 lc_pouch_compaction_res *out, lc_error *error);
  int (*maintenance)(lc_pouch_store *self, const char *mode,
                     lc_pouch_maintenance_res *out, lc_error *error);
  int (*retention_sweep)(lc_pouch_store *self,
                         const lc_pouch_retention_sweep_req *req,
                         lc_pouch_retention_sweep_res *out, lc_error *error);
  int (*read_state)(lc_pouch_store *self, const char *namespace_name,
                    const char *key, lc_source **body, lc_pouch_state_info *out,
                    lc_error *error);
  int (*write_state)(lc_pouch_store *self, const char *namespace_name,
                     const char *key, lc_source *body,
                     const lc_pouch_put_state_opts *opts,
                     lc_pouch_put_state_res *out, lc_error *error);
  int (*remove_state)(lc_pouch_store *self, const char *namespace_name,
                      const char *key, const char *expected_etag, int *removed,
                      lc_error *error);
  int (*stage_state)(lc_pouch_store *self, const char *namespace_name,
                     const char *key, const char *txn_id, lc_source *body,
                     const lc_pouch_put_state_opts *opts,
                     lc_pouch_put_state_res *out, lc_error *error);
  int (*stage_state_remove)(lc_pouch_store *self, const char *namespace_name,
                            const char *key, const char *txn_id,
                            const char *expected_etag,
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
  int (*scan_object_keys)(lc_pouch_store *self,
                          const lc_pouch_scan_object_keys_req *req,
                          lc_pouch_query_index_key_visit_fn visit,
                          void *visit_context,
                          lc_pouch_scan_object_keys_res *out, lc_error *error);
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
  int (*apply_queue_txn)(lc_pouch_store *self, const char *txn_id, int commit,
                         lc_error *error);
  int (*queue_stats)(lc_pouch_store *self, const char *namespace_name,
                     const char *queue, lc_pouch_queue_stats *out,
                     lc_error *error);
  int (*queue_wake_status)(lc_pouch_store *self, const char *namespace_name,
                           const char *queue, lc_pouch_queue_wake_status *out,
                           lc_error *error);
  int (*fsync_stats)(lc_pouch_store *self, lc_pouch_fsync_stats *out,
                     lc_error *error);
  int (*writer_status)(lc_pouch_store *self, lc_pouch_writer_status *out,
                       lc_error *error);
  int (*lock_status)(lc_pouch_store *self, lc_pouch_lock_status *out,
                     lc_error *error);
  int (*lock_key_path)(lc_pouch_store *self, const char *namespace_name,
                       const char *key, char **out, lc_error *error);
  int (*try_lock_key)(lc_pouch_store *self, const char *namespace_name,
                      const char *key, lc_pouch_key_lock **lock, int *acquired,
                      lc_error *error);
  int (*unlock_key)(lc_pouch_store *self, lc_pouch_key_lock *lock,
                    lc_error *error);
  int (*lock_fd_cache_status)(lc_pouch_store *self,
                              lc_pouch_lock_fd_cache_status *out,
                              lc_error *error);
  int (*read_fd_cache_status)(lc_pouch_store *self,
                              lc_pouch_read_fd_cache_status *out,
                              lc_error *error);
  int (*query_config)(lc_pouch_store *self, const char *namespace_name,
                      lc_pouch_query_config *out, lc_error *error);
  int (*backend_capabilities)(lc_pouch_store *self,
                              lc_pouch_backend_capabilities *out,
                              lc_error *error);
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
void lc_pouch_namespace_list_cleanup(const lc_pouch_allocator *allocator,
                                     lc_pouch_namespace_list *list);
void lc_pouch_query_index_scan_res_cleanup(const lc_pouch_allocator *allocator,
                                           lc_pouch_query_index_scan_res *res);
void lc_pouch_index_flush_res_cleanup(const lc_pouch_allocator *allocator,
                                      lc_pouch_index_flush_res *res);
void lc_pouch_compaction_res_cleanup(const lc_pouch_allocator *allocator,
                                     lc_pouch_compaction_res *res);
void lc_pouch_maintenance_res_cleanup(const lc_pouch_allocator *allocator,
                                      lc_pouch_maintenance_res *res);
void lc_pouch_query_config_cleanup(const lc_pouch_allocator *allocator,
                                   lc_pouch_query_config *config);
void lc_pouch_queue_wake_status_cleanup(const lc_pouch_allocator *allocator,
                                        lc_pouch_queue_wake_status *status);
void lc_pouch_writer_status_cleanup(const lc_pouch_allocator *allocator,
                                    lc_pouch_writer_status *status);
void lc_pouch_lock_status_cleanup(const lc_pouch_allocator *allocator,
                                  lc_pouch_lock_status *status);
void lc_pouch_backend_capabilities_cleanup(const lc_pouch_allocator *allocator,
                                           lc_pouch_backend_capabilities *caps);
void lc_pouch_staged_state_info_cleanup(const lc_pouch_allocator *allocator,
                                        lc_pouch_staged_state_info *info);
void lc_pouch_staged_state_list_cleanup(const lc_pouch_allocator *allocator,
                                        lc_pouch_staged_state_list *list);
void lc_pouch_object_info_cleanup(const lc_pouch_allocator *allocator,
                                  lc_pouch_object_info *info);
void lc_pouch_object_list_cleanup(const lc_pouch_allocator *allocator,
                                  lc_pouch_object_list *list);
void lc_pouch_scan_object_keys_res_cleanup(const lc_pouch_allocator *allocator,
                                           lc_pouch_scan_object_keys_res *res);
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

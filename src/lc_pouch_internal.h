#ifndef LC_POUCH_INTERNAL_H
#define LC_POUCH_INTERNAL_H

#include "lc_pouch.h"
#include "lc_pouch_crypto.h"

#include <pthread.h>

typedef struct lc_pouch_state_cache_namespace lc_pouch_state_cache_namespace;
typedef struct lc_pouch_source_cache_entry lc_pouch_source_cache_entry;
typedef struct lc_pouch_query_index_generation_cache_entry
    lc_pouch_query_index_generation_cache_entry;
typedef struct lc_pouch_query_index_doc_table_cache_entry
    lc_pouch_query_index_doc_table_cache_entry;
typedef struct lc_pouch_query_index_artifact_cache_entry
    lc_pouch_query_index_artifact_cache_entry;
typedef struct lc_pouch_query_index_packed_cache_entry
    lc_pouch_query_index_packed_cache_entry;
typedef struct lc_pouch_query_index_pending_entry
    lc_pouch_query_index_pending_entry;
typedef struct lc_pouch_query_index_pending_segment
    lc_pouch_query_index_pending_segment;
typedef struct lc_pouch_query_index_manifest_trust_entry
    lc_pouch_query_index_manifest_trust_entry;
typedef struct lc_pouch_fsync_request lc_pouch_fsync_request;
typedef struct lc_pouch_fsync_batcher lc_pouch_fsync_batcher;

typedef struct lc_pouch_state_change_visit_entry {
  const char *key;
  const char *content_type;
  const char *etag;
  lc_pouch_generation version;
  uint64_t bytes;
  uint64_t cipher_bytes;
  const char *descriptor;
  lc_pouch_unix_seconds updated_at_unix;
  int has_query_hidden;
  int query_hidden;
  int found;
} lc_pouch_state_change_visit_entry;

typedef int (*lc_pouch_state_change_visit_fn)(
    const lc_pouch_state_change_visit_entry *entry, void *context,
    lc_error *error);
typedef struct lc_pouch_state_scan_summary_entry {
  const char *key;
  const char *content_type;
  const char *etag;
  const char *descriptor;
  lc_pouch_generation version;
  uint64_t bytes;
  uint64_t cipher_bytes;
  lc_pouch_unix_seconds updated_at_unix;
  int has_query_hidden;
  int query_hidden;
  const void *opaque;
} lc_pouch_state_scan_summary_entry;

typedef struct lc_pouch_state_scan_summaries_result {
  int truncated;
  char *next_start_after;
} lc_pouch_state_scan_summaries_result;

typedef int (*lc_pouch_state_scan_summary_visit_fn)(
    const lc_pouch_state_scan_summary_entry *entry, void *context,
    lc_error *error);

struct lc_pouch {
  lc_allocator allocator;
  char *root_path;
  uint64_t segment_target_bytes;
  uint64_t fsync_batch_max_ops;
  unsigned long compaction_min_segment_count;
  uint64_t compaction_min_reclaimable_bytes;
  uint64_t compaction_interval_seconds;
  uint64_t compaction_delete_grace_seconds;
  uint64_t compaction_max_io_bytes_per_sec;
  uint64_t retention_seconds;
  uint64_t janitor_interval_seconds;
  uint64_t marker_sequence;
  int durable_sync;
  int background_compaction_enabled;
  int compaction_throttling_disabled;
  int single_writer;
  int aborted;
  int queue_watch_enabled;
  int filesystem_capabilities_known;
  int filesystem_is_nfs;
  const char *queue_watch_mode;
  const char *queue_watch_reason;
  uint64_t single_writer_epoch;
  pthread_mutex_t single_writer_mutex;
  int single_writer_mutex_initialized;
  char *query_engine;
  char *query_fallback_engine;
  char *compression;
  lc_pouch_crypto *crypto;
  char *crypto_key_file;
  pslog_logger *base_logger;
  pslog_logger *logger;
  int owns_logger;
  char *writer_marker_leaf;
  char *writer_presence_dir;
  char *writer_presence_leaf;
  char *writer_presence_path;
  pthread_mutex_t writer_presence_mutex;
  pthread_cond_t writer_presence_cond;
  pthread_t writer_presence_thread;
  int writer_presence_mutex_initialized;
  int writer_presence_cond_initialized;
  int writer_presence_thread_started;
  int writer_presence_stop;
  pthread_mutex_t state_mutation_mutex;
  int state_mutation_mutex_initialized;
  lc_pouch_fsync_batcher *fsync_batcher;
  pthread_mutex_t compaction_mutex;
  pthread_cond_t compaction_cond;
  pthread_t compaction_thread;
  int compaction_mutex_initialized;
  int compaction_cond_initialized;
  int compaction_thread_started;
  int compaction_stop;
  int compaction_pending;
  char **compaction_namespaces;
  size_t compaction_namespace_count;
  size_t compaction_namespace_capacity;
  pthread_mutex_t janitor_mutex;
  pthread_cond_t janitor_cond;
  pthread_t janitor_thread;
  int janitor_mutex_initialized;
  int janitor_cond_initialized;
  int janitor_thread_started;
  int janitor_stop;
  int janitor_pending;
  lc_pouch_state_cache_namespace *state_cache_namespaces;
  lc_pouch_source_cache_entry *source_cache_entries;
  size_t source_cache_count;
  unsigned long source_cache_tick;
  lc_pouch_query_index_generation_cache_entry *query_generation_cache;
  size_t query_generation_cache_count;
  lc_pouch_query_index_doc_table_cache_entry *query_doc_table_cache;
  size_t query_doc_table_cache_count;
  lc_pouch_query_index_artifact_cache_entry *query_artifact_cache;
  size_t query_artifact_cache_count;
  lc_pouch_query_index_packed_cache_entry *query_packed_cache;
  size_t query_packed_cache_count;
  lc_pouch_query_index_pending_entry *query_pending_index;
  size_t query_pending_index_count;
  int query_pending_index_incomplete;
  lc_pouch_query_index_pending_segment *query_pending_segments;
  lc_pouch_query_index_manifest_trust_entry *query_manifest_trust;
};

#ifdef LOCKDC_TEST_BUILD
typedef int (*lc_pouch_test_hook)(void *context, lc_error *error);
extern lc_pouch_test_hook lc_pouch_test_after_snapshot_write_hook;
extern void *lc_pouch_test_after_snapshot_write_context;
#endif

void lc_pouch_state_cache_cleanup(lc_pouch *pouch);
int lc_pouch_single_writer_snapshot(lc_pouch *pouch, uint64_t *epoch_out);
int lc_pouch_single_writer_enabled(lc_pouch *pouch);
/**
 * Starts a fresh idle-compaction delay after a successful mutation. The worker
 * does not compact at open or while successful mutations keep arriving.
 */
void lc_pouch_compaction_note_mutation(lc_pouch *pouch);
void lc_pouch_state_source_cache_cleanup(lc_pouch *pouch);
void lc_pouch_query_index_cache_cleanup(lc_pouch *pouch);
int lc_pouch_state_with_namespace_lock(lc_pouch *pouch,
                                       const char *namespace_name,
                                       lc_pouch_state_precondition_fn callback,
                                       void *context, lc_error *error);
int lc_pouch_state_with_key_lock(lc_pouch *pouch, const char *namespace_name,
                                 const char *key,
                                 lc_pouch_state_precondition_fn callback,
                                 void *context, lc_error *error);
int lc_pouch_state_read_metadata_locked(lc_pouch *pouch,
                                        const char *namespace_name,
                                        const char *key,
                                        lc_pouch_state_read_result *out,
                                        lc_error *error);
int lc_pouch_state_update_metadata_locked(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    const lc_pouch_state_write_options *options,
    lc_pouch_state_write_result *out, lc_error *error);
int lc_pouch_state_visit_since(lc_pouch *pouch, const char *namespace_name,
                               lc_pouch_generation after_version,
                               lc_pouch_state_change_visit_fn visitor,
                               void *context, lc_error *error);
int lc_pouch_state_visible_count(lc_pouch *pouch, const char *namespace_name,
                                 size_t *count, lc_error *error);
int lc_pouch_state_warm_namespace(lc_pouch *pouch, const char *namespace_name,
                                  lc_error *error);
int lc_pouch_state_compaction_track_cached_namespaces(lc_pouch *pouch,
                                                      lc_error *error);
int lc_pouch_state_scan_summaries(lc_pouch *pouch, const char *namespace_name,
                                  const char *start_after, size_t limit,
                                  lc_pouch_state_scan_summary_visit_fn visitor,
                                  void *context,
                                  lc_pouch_state_scan_summaries_result *out,
                                  lc_error *error);
int lc_pouch_state_scan_summary_read_body(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_state_scan_summary_entry *entry,
    lc_pouch_state_read_result *out, lc_error *error);
void lc_pouch_state_scan_summaries_result_cleanup(
    const lc_allocator *allocator,
    lc_pouch_state_scan_summaries_result *result);
int lc_pouch_fsync_commit(lc_pouch *pouch, int fd, lc_error *error);
int lc_pouch_queue_watch_wait(lc_pouch *pouch, const char *namespace_name,
                              const char *queue, uint64_t timeout_ms);
/** Converts a durable Pouch byte count for generic C API response fields. */
int lc_pouch_size_to_public_long(uint64_t size, long *out, lc_error *error);
void lc_pouch_janitor_note_mutation(lc_pouch *pouch);
int lc_pouch_compaction_track_namespace(lc_pouch *pouch,
                                        const char *namespace_name,
                                        lc_error *error);

#endif

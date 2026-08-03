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
typedef struct lc_pouch_state_metadata_append_batcher
    lc_pouch_state_metadata_append_batcher;
typedef struct lc_pouch_writer_root_lock_entry lc_pouch_writer_root_lock_entry;
typedef struct lc_pouch_exclusive_append_gate lc_pouch_exclusive_append_gate;

/* Exclusive roots have one in-process writer. These bounded stripes retain
 * conflicting-key serialization through durable completion without shared
 * mode's registry allocation or cross-process lock work. */
#define LC_POUCH_EXCLUSIVE_KEY_STRIPE_COUNT 97U

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

/* A transient state projection view used while a key mutation lock is held. */
typedef struct lc_pouch_state_metadata_view {
  int found;
  lc_pouch_generation version;
  const unsigned char *metadata;
  size_t metadata_length;
  int has_query_hidden;
  int query_hidden;
  int has_body;
} lc_pouch_state_metadata_view;

/* Builds a metadata mutation from the current key projection while the key
 * mutation lock is held. Set `apply` to zero for a successful read-only
 * decision such as a lease already being held. */
typedef int (*lc_pouch_state_metadata_prepare_fn)(
    const lc_pouch_state_metadata_view *current, void *context,
    lc_pouch_state_write_options *options, int *apply, lc_error *error);

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
  /* Readers cover append-capable work. A mode transition takes this writer
   * lock so no operation can append using the mode it observed before the
   * root ownership handoff. */
  pthread_rwlock_t writer_mode_guard;
  int writer_mode_guard_initialized;
  lc_pouch_writer_root_lock_entry *writer_root_lock;
  int writer_root_lock_mode;
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
  uint64_t root_device;
  uint64_t root_inode;
  int root_identity_initialized;
  pthread_mutex_t state_mutation_mutex;
  int state_mutation_mutex_initialized;
  pthread_mutex_t exclusive_key_mutexes[LC_POUCH_EXCLUSIVE_KEY_STRIPE_COUNT];
  size_t exclusive_key_mutex_count;
  pthread_mutex_t exclusive_append_gate_mutex;
  int exclusive_append_gate_mutex_initialized;
  lc_pouch_exclusive_append_gate *exclusive_append_gates;
  lc_pouch_fsync_batcher *fsync_batcher;
  /* Exclusive metadata mutations submit complete inline records here. The
   * state implementation owns the worker and keeps streamed bodies direct. */
  lc_pouch_state_metadata_append_batcher *state_metadata_append_batcher;
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
  pthread_mutex_t source_cache_mutex;
  int source_cache_mutex_initialized;
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
typedef void (*lc_pouch_test_after_acquire_claim_hook_fn)(void *context);
extern lc_pouch_test_after_acquire_claim_hook_fn
    lc_pouch_test_after_acquire_claim_hook;
extern void *lc_pouch_test_after_acquire_claim_context;
#endif

void lc_pouch_state_cache_cleanup(lc_pouch *pouch);
int lc_pouch_single_writer_snapshot(lc_pouch *pouch, uint64_t *epoch_out);
int lc_pouch_single_writer_enabled(lc_pouch *pouch);
/**
 * Pins writer mode for an append-capable operation until its durable result is
 * resolved. Internal callers must pair each successful begin with end.
 */
int lc_pouch_writer_mode_operation_begin(lc_pouch *pouch, lc_error *error);
void lc_pouch_writer_mode_operation_end(lc_pouch *pouch);
/**
 * Starts a fresh idle-compaction delay after a successful mutation. The worker
 * does not compact at open or while successful mutations keep arriving.
 */
void lc_pouch_compaction_note_mutation(lc_pouch *pouch);
void lc_pouch_state_source_cache_cleanup(lc_pouch *pouch);
int lc_pouch_state_metadata_append_worker_init(lc_pouch *pouch,
                                               lc_error *error);
void lc_pouch_state_metadata_append_worker_close(lc_pouch *pouch);
void lc_pouch_state_exclusive_append_gates_cleanup(lc_pouch *pouch);
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
/**
 * Reads a full state record while the caller holds namespace mutation
 * authority. This avoids attempting to recursively acquire the namespace
 * process mutex while transaction application validates and updates paired
 * queue records.
 */
int lc_pouch_state_read_locked(lc_pouch *pouch, const char *namespace_name,
                               const char *key, lc_pouch_state_read_result *out,
                               lc_error *error);
/** Writes a full state record while the caller holds the key mutation lock. */
int lc_pouch_state_write_locked(lc_pouch *pouch, const char *namespace_name,
                                const char *key, lc_source *body,
                                const lc_pouch_state_write_options *options,
                                lc_pouch_state_write_result *out,
                                lc_error *error);
/** Commits staged state under target mutation authority. */
int lc_pouch_state_commit_staged_locked(lc_pouch *pouch,
                                        const char *namespace_name,
                                        const char *key, const char *txn_id,
                                        lc_pouch_state_write_result *out,
                                        lc_error *error);
/** Discards staged state under target mutation authority. */
int lc_pouch_state_discard_staged_locked(lc_pouch *pouch,
                                         const char *namespace_name,
                                         const char *key, const char *txn_id,
                                         int *discarded, lc_error *error);
/**
 * Reads metadata while the caller holds the key mutation lock. On a current
 * exclusive-writer projection, `out` borrows its fields directly from that
 * projection. Those fields must be consumed before the next state mutation or
 * release of the key mutation lock. Shared and cold paths populate
 * `owned_fallback`, whose lifetime is managed by the caller.
 */
int lc_pouch_state_read_metadata_view_locked(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    lc_pouch_state_metadata_view *out,
    lc_pouch_state_read_result *owned_fallback, lc_error *error);
int lc_pouch_state_update_metadata_locked(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    const lc_pouch_state_write_options *options,
    lc_pouch_state_write_result *out, lc_error *error);
/**
 * Prepares and applies one metadata mutation from one locked resident view.
 * The caller must already hold the key mutation lock.
 */
int lc_pouch_state_update_metadata_prepared_locked(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    lc_pouch_state_metadata_prepare_fn prepare, void *prepare_context,
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

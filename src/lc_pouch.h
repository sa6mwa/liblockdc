#ifndef LC_POUCH_H
#define LC_POUCH_H

#include "lc/lc.h"
#include "lc_intcompat.h"

#include <stddef.h>

typedef struct lc_pouch lc_pouch;

/* Durable state generations and Unix timestamps have fixed-width storage. */
typedef lc_u64 lc_pouch_generation;
typedef lc_i64 lc_pouch_unix_seconds;

typedef struct lc_pouch_open_options {
  uint64_t segment_target_bytes;
  unsigned long compaction_min_segment_count;
  uint64_t compaction_min_reclaimable_bytes;
  /**
   * Idle delay for background compaction. The worker starts its delay only
   * after a successful mutation and restarts it after every later mutation;
   * it never sweeps at open. Continuous mutation deliberately defers automatic
   * compaction, so callers requiring a maintenance deadline use
   * `lc_pouch_maintenance_run` explicitly.
   */
  uint64_t compaction_interval_seconds;
  uint64_t compaction_delete_grace_seconds;
  uint64_t compaction_max_io_bytes_per_sec;
  int background_compaction_enabled;
  /** Distinguishes an explicit compaction setting from the Go-compatible
   * default. */
  int background_compaction_enabled_set;
  /** Leaves compaction IO unlimited instead of using the default 8 MiB/s
   * throttle. */
  int compaction_throttling_disabled;
  /** Zero disables retention. Positive values delete state older than this
   * duration. */
  uint64_t retention_seconds;
  /** Zero uses the default one-hour retention sweep interval. */
  uint64_t janitor_interval_seconds;
  /**
   * Selects the writer mode explicitly. When unset, Pouch uses its default
   * exclusive-root writer mode. Set this field before using `single_writer`
   * to opt into either exclusive or shared-root operation deliberately.
   */
  int single_writer_set;
  /**
   * Writer mode selected when `single_writer_set` is nonzero. A nonzero value
   * reserves this root for one writer; zero enables the slower shared-root
   * coordination path. Shared writers retain a local projection and, under
   * append authority, replay only the peer tail after its verified cursor.
   * An incomplete unseen suffix is repaired there before append. Historical
   * bytes are revalidated on recovery or projection invalidation, not before
   * every healthy shared mutation.
   */
  int single_writer;
  const char *query_engine;
  const char *query_fallback_engine;
  const char *crypto_key;
  const char *crypto_key_file;
  int crypto_generate_key_file;
  const char *compression;
  pslog_logger *logger;
  /**
   * Wait for a durable root-scoped fsync group commit at each public mutation.
   * Defaults to zero, matching Go disk failover's NoSync policy.
   */
  int durable_sync;
  /** Maximum fsync requests per durable group commit. The worker commits when
   * this limit is reached or after its two-millisecond coalescing window.
   * Zero leaves the batch unbounded.
   */
  uint64_t fsync_batch_max_ops;
  /** Enables filesystem queue notifications where the root supports them. */
  int queue_watch;
} lc_pouch_open_options;

#define LC_POUCH_FSYNC_BATCH_BOUND_COUNT 13U
#define LC_POUCH_FSYNC_BATCH_BUCKET_COUNT                                      \
  (LC_POUCH_FSYNC_BATCH_BOUND_COUNT + 1U)
#define LC_POUCH_BACKEND_HASH_HEX_BYTES 64U

/** Aggregate group-commit diagnostics, using fixed-width counters on all
 * targets. */
typedef struct lc_pouch_fsync_stats {
  uint64_t total_batches;
  uint64_t total_requests;
  uint64_t max_batch_size;
  uint64_t total_sync_ns;
  uint64_t max_sync_ns;
  uint64_t bounds[LC_POUCH_FSYNC_BATCH_BOUND_COUNT];
  uint64_t counts[LC_POUCH_FSYNC_BATCH_BUCKET_COUNT];
} lc_pouch_fsync_stats;

typedef struct lc_pouch_status {
  char *root_path;
  char *layout_name;
  unsigned long layout_version;
  uint64_t segment_target_bytes;
  unsigned long compaction_min_segment_count;
  uint64_t compaction_min_reclaimable_bytes;
  uint64_t compaction_interval_seconds;
  uint64_t compaction_delete_grace_seconds;
  uint64_t compaction_max_io_bytes_per_sec;
  int background_compaction_enabled;
  int compaction_throttling_disabled;
  uint64_t retention_seconds;
  uint64_t janitor_interval_seconds;
  int janitor_running;
  int single_writer;
  int supports_concurrent_writes;
  int aborted;
  int durable_sync;
  uint64_t fsync_batch_max_ops;
  int queue_watch_enabled;
  int filesystem_capabilities_known;
  int filesystem_is_nfs;
  char *queue_watch_mode;
  char *queue_watch_reason;
  char *query_engine;
  char *query_fallback_engine;
  int crypto_enabled;
  char *crypto_key_file;
  char *compression;
} lc_pouch_status;

/** Root-scoped exclusive-writer presence used to fence HA auto mode. */
typedef struct lc_pouch_exclusive_writer_presence {
  int present;
  lc_pouch_unix_seconds expires_at_unix;
} lc_pouch_exclusive_writer_presence;

typedef struct lc_pouch_maintenance_options {
  const char *namespace_name;
  int force;
  int cleanup_only;
  lc_pouch_unix_seconds retention_updated_before_unix;
} lc_pouch_maintenance_options;

typedef struct lc_pouch_maintenance_result {
  char *namespace_name;
  char *diagnostic;
  unsigned long candidate_segment_count;
  uint64_t candidate_bytes;
  uint64_t compacted_segment_id;
  unsigned long cleanup_deleted_count;
  unsigned long cleanup_pending_count;
  unsigned long retention_scanned_count;
  unsigned long retention_expired_count;
  unsigned long retention_deleted_state_count;
  unsigned long retention_failed_count;
  int compacted;
  int skipped;
  int aborted;
} lc_pouch_maintenance_result;

typedef int (*lc_pouch_state_precondition_fn)(void *context, lc_error *error);

/* A target-key projection borrowed while a state mutation holds its authority.
 * Fields remain valid only for the duration of the precondition callback. */
typedef struct lc_pouch_state_precondition_view {
  int found;
  lc_pouch_generation version;
  const unsigned char *metadata;
  size_t metadata_length;
  int has_query_hidden;
  int query_hidden;
  int has_body;
} lc_pouch_state_precondition_view;

typedef int (*lc_pouch_state_view_precondition_fn)(
    const lc_pouch_state_precondition_view *current, void *context,
    lc_error *error);

typedef struct lc_pouch_state_write_options {
  const char *content_type;
  const char *expected_etag;
  const unsigned char *metadata;
  size_t metadata_length;
  int has_metadata;
  /* Evaluates against the selected target-key projection without a reread. */
  lc_pouch_state_view_precondition_fn view_precondition;
  void *view_precondition_context;
  lc_pouch_state_precondition_fn precondition;
  void *precondition_context;
  lc_pouch_generation expected_version;
  int has_expected_version;
  int create_if_absent;
  int has_query_hidden;
  int query_hidden;
  int disable_compression;
  int object_record;
} lc_pouch_state_write_options;

typedef struct lc_pouch_state_write_result {
  char *etag;
  lc_pouch_generation index_seq;
  lc_pouch_generation version;
  uint64_t bytes;
  uint64_t cipher_bytes;
  char *descriptor;
  unsigned char *metadata;
  size_t metadata_length;
  lc_pouch_unix_seconds updated_at_unix;
  int has_query_hidden;
  int query_hidden;
} lc_pouch_state_write_result;

typedef struct lc_pouch_state_read_result {
  int found;
  char *content_type;
  char *etag;
  lc_pouch_generation index_seq;
  lc_pouch_generation version;
  uint64_t bytes;
  uint64_t cipher_bytes;
  char *descriptor;
  unsigned char *metadata;
  size_t metadata_length;
  lc_pouch_unix_seconds updated_at_unix;
  int has_query_hidden;
  int query_hidden;
  int has_body;
  lc_source *body;
} lc_pouch_state_read_result;

typedef struct lc_pouch_state_visit_entry {
  const char *key;
  const char *content_type;
  const char *etag;
  lc_pouch_generation version;
  uint64_t bytes;
  uint64_t cipher_bytes;
  const char *descriptor;
  const unsigned char *metadata;
  size_t metadata_length;
  lc_pouch_unix_seconds updated_at_unix;
  int has_query_hidden;
  int query_hidden;
} lc_pouch_state_visit_entry;

typedef int (*lc_pouch_state_visit_fn)(const lc_pouch_state_visit_entry *entry,
                                       void *context, lc_error *error);
#define LC_POUCH_STATE_READ_MANY_STOP (-1000)
typedef int (*lc_pouch_state_read_many_fn)(
    const char *key, const lc_pouch_state_read_result *result, void *context,
    lc_error *error);

/**
 * Opens a Pouch root and starts its owned worker threads. Its indexer batches
 * derived-index replay from durable state after 2,000 namespace mutations or
 * 10 seconds, so mutation completion neither parses nor retains document
 * bodies for index construction.
 *
 * A process must fork before opening Pouch, or exec before using Pouch in the
 * child. Re-entering an inherited Pouch handle after fork is unsupported:
 * worker-owned pthread state is not available in the child.
 */
int lc_pouch_open(const char *root_path, const lc_allocator *allocator,
                  const lc_pouch_open_options *options, lc_pouch **out,
                  lc_error *error);
void lc_pouch_close(lc_pouch *pouch);
/**
 * Stops Pouch-owned worker loops, closes resident descriptor and derived-cache
 * state, and releases its process-bound writer lock. The durable
 * exclusive-writer heartbeat remains as crash fencing until its takeover
 * expiry, so another exclusive opener cannot bypass recovery.
 */
int lc_pouch_abort(lc_pouch *pouch, lc_error *error);
/** Returns whether this handle uses shared-root mutation coordination. */
int lc_pouch_supports_concurrent_writes(const lc_pouch *pouch);
/** Copies aggregate group-commit diagnostics into `out`. */
int lc_pouch_fsync_stats_read(lc_pouch *pouch, lc_pouch_fsync_stats *out,
                              lc_error *error);
/** Writes the stable SHA-256 root identity as 64 lowercase hex characters. */
int lc_pouch_backend_hash(lc_pouch *pouch,
                          char out[LC_POUCH_BACKEND_HASH_HEX_BYTES + 1U],
                          lc_error *error);
int lc_pouch_status_read(lc_pouch *pouch, lc_pouch_status *out,
                         lc_error *error);
void lc_pouch_status_cleanup(const lc_allocator *allocator,
                             lc_pouch_status *status);
/**
 * Changes this open handle between exclusive and shared-root operation.
 * The transition waits for this handle's active append-capable operations to
 * resolve their durable result, invalidates their append descriptors through
 * the writer-mode epoch, then changes root ownership. It fails while another
 * incompatible root writer is active.
 */
int lc_pouch_set_single_writer(lc_pouch *pouch, int enabled, lc_error *error);
int lc_pouch_probe_exclusive_writer(lc_pouch *pouch,
                                    lc_pouch_exclusive_writer_presence *out,
                                    lc_error *error);
int lc_pouch_maintenance_run(lc_pouch *pouch,
                             const lc_pouch_maintenance_options *options,
                             lc_pouch_maintenance_result *out, lc_error *error);
void lc_pouch_maintenance_result_cleanup(const lc_allocator *allocator,
                                         lc_pouch_maintenance_result *result);
int lc_pouch_ensure_namespace(lc_pouch *pouch, const char *namespace_name,
                              lc_error *error);
int lc_pouch_state_write(lc_pouch *pouch, const char *namespace_name,
                         const char *key, lc_source *body,
                         const lc_pouch_state_write_options *options,
                         lc_pouch_state_write_result *out, lc_error *error);
int lc_pouch_state_delete(lc_pouch *pouch, const char *namespace_name,
                          const char *key,
                          const lc_pouch_state_write_options *options,
                          lc_pouch_state_write_result *out, lc_error *error);
int lc_pouch_state_update_metadata(lc_pouch *pouch, const char *namespace_name,
                                   const char *key,
                                   const lc_pouch_state_write_options *options,
                                   lc_pouch_state_write_result *out,
                                   lc_error *error);
int lc_pouch_state_stage_write(lc_pouch *pouch, const char *namespace_name,
                               const char *key, const char *txn_id,
                               lc_source *body,
                               const lc_pouch_state_write_options *options,
                               lc_pouch_state_write_result *out,
                               lc_error *error);
int lc_pouch_state_promote_staged(lc_pouch *pouch, const char *namespace_name,
                                  const char *key, const char *txn_id,
                                  const char *expected_committed_etag,
                                  lc_pouch_state_write_result *out,
                                  lc_error *error);
int lc_pouch_state_commit_staged(lc_pouch *pouch, const char *namespace_name,
                                 const char *key, const char *txn_id,
                                 lc_pouch_state_write_result *out,
                                 lc_error *error);
int lc_pouch_state_discard_staged(lc_pouch *pouch, const char *namespace_name,
                                  const char *key, const char *txn_id,
                                  int *discarded, lc_error *error);
int lc_pouch_state_recover_staged_decisions(lc_pouch *pouch,
                                            const char *namespace_name,
                                            lc_error *error);
void lc_pouch_state_write_result_cleanup(const lc_allocator *allocator,
                                         lc_pouch_state_write_result *result);
int lc_pouch_state_read(lc_pouch *pouch, const char *namespace_name,
                        const char *key, lc_pouch_state_read_result *out,
                        lc_error *error);
int lc_pouch_state_copy(lc_pouch *pouch, const char *namespace_name,
                        const char *key, lc_sink *dst,
                        lc_pouch_state_read_result *out, lc_error *error);
int lc_pouch_state_read_metadata(lc_pouch *pouch, const char *namespace_name,
                                 const char *key,
                                 lc_pouch_state_read_result *out,
                                 lc_error *error);
int lc_pouch_state_read_many(lc_pouch *pouch, const char *namespace_name,
                             const char *const *keys, size_t key_count,
                             lc_pouch_state_read_many_fn visitor, void *context,
                             lc_error *error);
int lc_pouch_state_read_many_cached(lc_pouch *pouch, const char *namespace_name,
                                    const char *const *keys, size_t key_count,
                                    lc_pouch_state_read_many_fn visitor,
                                    void *context, lc_error *error);
int lc_pouch_state_read_many_metadata(lc_pouch *pouch,
                                      const char *namespace_name,
                                      const char *const *keys, size_t key_count,
                                      lc_pouch_state_read_many_fn visitor,
                                      void *context, lc_error *error);
int lc_pouch_state_visit(lc_pouch *pouch, const char *namespace_name,
                         lc_pouch_state_visit_fn visitor, void *context,
                         lc_error *error);
int lc_pouch_state_index_seq(lc_pouch *pouch, const char *namespace_name,
                             lc_pouch_generation *out, lc_error *error);
void lc_pouch_state_read_result_cleanup(const lc_allocator *allocator,
                                        lc_pouch_state_read_result *result);

#endif

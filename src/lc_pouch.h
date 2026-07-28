#ifndef LC_POUCH_H
#define LC_POUCH_H

#include "lc/lc.h"

#include <stddef.h>

typedef struct lc_pouch lc_pouch;

typedef struct lc_pouch_open_options {
  unsigned long segment_target_bytes;
  unsigned long compaction_min_segment_count;
  unsigned long compaction_min_reclaimable_bytes;
  unsigned long compaction_interval_seconds;
  int background_compaction_enabled;
  int single_writer;
  const char *query_engine;
  const char *query_fallback_engine;
  const char *crypto_key;
  const char *crypto_key_file;
  int crypto_generate_key_file;
} lc_pouch_open_options;

typedef struct lc_pouch_status {
  char *root_path;
  char *layout_name;
  unsigned long layout_version;
  unsigned long segment_target_bytes;
  unsigned long compaction_min_segment_count;
  unsigned long compaction_min_reclaimable_bytes;
  unsigned long compaction_interval_seconds;
  int background_compaction_enabled;
  int single_writer;
  char *query_engine;
  char *query_fallback_engine;
  int crypto_enabled;
  char *crypto_key_file;
} lc_pouch_status;

typedef struct lc_pouch_maintenance_options {
  const char *namespace_name;
  int force;
  int cleanup_only;
  long retention_updated_before_unix;
} lc_pouch_maintenance_options;

typedef struct lc_pouch_maintenance_result {
  char *namespace_name;
  char *diagnostic;
  unsigned long candidate_segment_count;
  unsigned long candidate_bytes;
  unsigned long compacted_segment_id;
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

typedef struct lc_pouch_state_write_options {
  const char *content_type;
  const char *expected_etag;
  unsigned long expected_version;
  int has_expected_version;
  int has_query_hidden;
  int query_hidden;
} lc_pouch_state_write_options;

typedef struct lc_pouch_state_write_result {
  char *etag;
  unsigned long version;
  unsigned long bytes;
  unsigned long cipher_bytes;
  char *descriptor;
  long updated_at_unix;
  int has_query_hidden;
  int query_hidden;
} lc_pouch_state_write_result;

typedef struct lc_pouch_state_read_result {
  int found;
  char *content_type;
  char *etag;
  unsigned long version;
  unsigned long bytes;
  unsigned long cipher_bytes;
  char *descriptor;
  long updated_at_unix;
  int has_query_hidden;
  int query_hidden;
  lc_source *body;
} lc_pouch_state_read_result;

typedef struct lc_pouch_state_visit_entry {
  const char *key;
  const char *content_type;
  const char *etag;
  unsigned long version;
  unsigned long bytes;
  unsigned long cipher_bytes;
  const char *descriptor;
  long updated_at_unix;
  int has_query_hidden;
  int query_hidden;
} lc_pouch_state_visit_entry;

typedef int (*lc_pouch_state_visit_fn)(const lc_pouch_state_visit_entry *entry,
                                       void *context, lc_error *error);
#define LC_POUCH_STATE_READ_MANY_STOP (-1000)
typedef int (*lc_pouch_state_read_many_fn)(
    const char *key, const lc_pouch_state_read_result *result, void *context,
    lc_error *error);

int lc_pouch_open(const char *root_path, const lc_allocator *allocator,
                  const lc_pouch_open_options *options, lc_pouch **out,
                  lc_error *error);
void lc_pouch_close(lc_pouch *pouch);
int lc_pouch_status_read(lc_pouch *pouch, lc_pouch_status *out,
                         lc_error *error);
void lc_pouch_status_cleanup(const lc_allocator *allocator,
                             lc_pouch_status *status);
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
int lc_pouch_state_read_many(lc_pouch *pouch, const char *namespace_name,
                             const char *const *keys, size_t key_count,
                             lc_pouch_state_read_many_fn visitor, void *context,
                             lc_error *error);
int lc_pouch_state_read_many_metadata(lc_pouch *pouch,
                                      const char *namespace_name,
                                      const char *const *keys, size_t key_count,
                                      lc_pouch_state_read_many_fn visitor,
                                      void *context, lc_error *error);
int lc_pouch_state_visit(lc_pouch *pouch, const char *namespace_name,
                         lc_pouch_state_visit_fn visitor, void *context,
                         lc_error *error);
int lc_pouch_state_index_seq(lc_pouch *pouch, const char *namespace_name,
                             unsigned long *out, lc_error *error);
void lc_pouch_state_read_result_cleanup(const lc_allocator *allocator,
                                        lc_pouch_state_read_result *result);

#endif

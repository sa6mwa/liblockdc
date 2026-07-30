#ifndef LC_POUCH_INTERNAL_H
#define LC_POUCH_INTERNAL_H

#include "lc_pouch.h"
#include "lc_pouch_crypto.h"

typedef struct lc_pouch_state_cache_namespace lc_pouch_state_cache_namespace;
typedef struct lc_pouch_query_index_generation_cache_entry
    lc_pouch_query_index_generation_cache_entry;
typedef struct lc_pouch_query_index_doc_table_cache_entry
    lc_pouch_query_index_doc_table_cache_entry;
typedef struct lc_pouch_query_index_artifact_cache_entry
    lc_pouch_query_index_artifact_cache_entry;
typedef struct lc_pouch_query_index_pending_entry
    lc_pouch_query_index_pending_entry;
typedef struct lc_pouch_query_index_pending_segment
    lc_pouch_query_index_pending_segment;
typedef struct lc_pouch_query_index_manifest_trust_entry
    lc_pouch_query_index_manifest_trust_entry;

typedef struct lc_pouch_state_change_visit_entry {
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
  unsigned long version;
  unsigned long bytes;
  unsigned long cipher_bytes;
  long updated_at_unix;
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
  unsigned long segment_target_bytes;
  unsigned long compaction_min_segment_count;
  unsigned long compaction_min_reclaimable_bytes;
  unsigned long compaction_interval_seconds;
  unsigned long last_compaction_check_seconds;
  unsigned long marker_sequence;
  int background_compaction_enabled;
  int single_writer;
  char *query_engine;
  char *query_fallback_engine;
  char *compression;
  lc_pouch_crypto *crypto;
  char *crypto_key_file;
  char *writer_marker_leaf;
  lc_pouch_state_cache_namespace *state_cache_namespaces;
  lc_pouch_query_index_generation_cache_entry *query_generation_cache;
  size_t query_generation_cache_count;
  lc_pouch_query_index_doc_table_cache_entry *query_doc_table_cache;
  size_t query_doc_table_cache_count;
  lc_pouch_query_index_artifact_cache_entry *query_artifact_cache;
  size_t query_artifact_cache_count;
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
void lc_pouch_query_index_cache_cleanup(lc_pouch *pouch);
int lc_pouch_state_with_namespace_lock(lc_pouch *pouch,
                                       const char *namespace_name,
                                       lc_pouch_state_precondition_fn callback,
                                       void *context, lc_error *error);
int lc_pouch_state_read_metadata_locked(lc_pouch *pouch,
                                        const char *namespace_name,
                                        const char *key,
                                        lc_pouch_state_read_result *out,
                                        lc_error *error);
int lc_pouch_state_visit_since(lc_pouch *pouch, const char *namespace_name,
                               unsigned long after_version,
                               lc_pouch_state_change_visit_fn visitor,
                               void *context, lc_error *error);
int lc_pouch_state_visible_count(lc_pouch *pouch, const char *namespace_name,
                                 size_t *count, lc_error *error);
int lc_pouch_state_scan_summaries(
    lc_pouch *pouch, const char *namespace_name, const char *start_after,
    size_t limit, lc_pouch_state_scan_summary_visit_fn visitor, void *context,
    lc_pouch_state_scan_summaries_result *out, lc_error *error);
int lc_pouch_state_scan_summary_read_body(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_state_scan_summary_entry *entry,
    lc_pouch_state_read_result *out, lc_error *error);
void lc_pouch_state_scan_summaries_result_cleanup(
    const lc_allocator *allocator, lc_pouch_state_scan_summaries_result *result);

#endif

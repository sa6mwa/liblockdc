#ifndef LC_POUCH_INTERNAL_H
#define LC_POUCH_INTERNAL_H

#include "lc_pouch.h"
#include "lc_pouch_crypto.h"

typedef struct lc_pouch_state_cache_namespace lc_pouch_state_cache_namespace;
typedef struct lc_pouch_query_index_prepared_exact
    lc_pouch_query_index_prepared_exact;
typedef struct lc_pouch_query_index_prepared_presence
    lc_pouch_query_index_prepared_presence;
typedef struct lc_pouch_query_index_prepared_range
    lc_pouch_query_index_prepared_range;
typedef struct lc_pouch_query_index_prepared_text
    lc_pouch_query_index_prepared_text;
typedef struct lc_pouch_query_index_prepared_temporal
    lc_pouch_query_index_prepared_temporal;
typedef struct lc_pouch_query_index_flush_cache
    lc_pouch_query_index_flush_cache;

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
  lc_pouch_crypto *crypto;
  char *crypto_key_file;
  char *writer_marker_leaf;
  lc_pouch_state_cache_namespace *state_cache_namespaces;
  lc_pouch_query_index_prepared_exact *prepared_exact_readers;
  lc_pouch_query_index_prepared_presence *prepared_presence_readers;
  lc_pouch_query_index_prepared_range *prepared_range_readers;
  lc_pouch_query_index_prepared_text *prepared_text_readers;
  lc_pouch_query_index_prepared_temporal *prepared_temporal_readers;
  lc_pouch_query_index_flush_cache *query_index_flush_cache;
};

#ifdef LOCKDC_TEST_BUILD
typedef int (*lc_pouch_test_hook)(void *context, lc_error *error);
extern lc_pouch_test_hook lc_pouch_test_after_snapshot_write_hook;
extern void *lc_pouch_test_after_snapshot_write_context;
#endif

void lc_pouch_state_cache_cleanup(lc_pouch *pouch);
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

#endif

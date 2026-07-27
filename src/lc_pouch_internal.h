#ifndef LC_POUCH_INTERNAL_H
#define LC_POUCH_INTERNAL_H

#include "lc_pouch.h"

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
  char *writer_marker_leaf;
  lc_pouch_state_cache_namespace *state_cache_namespaces;
  lc_pouch_query_index_prepared_exact *prepared_exact_readers;
  lc_pouch_query_index_prepared_presence *prepared_presence_readers;
  lc_pouch_query_index_prepared_range *prepared_range_readers;
  lc_pouch_query_index_prepared_text *prepared_text_readers;
  lc_pouch_query_index_prepared_temporal *prepared_temporal_readers;
};

#ifdef LOCKDC_TEST_BUILD
typedef int (*lc_pouch_test_hook)(void *context, lc_error *error);
extern lc_pouch_test_hook lc_pouch_test_after_snapshot_write_hook;
extern void *lc_pouch_test_after_snapshot_write_context;
#endif

void lc_pouch_state_cache_cleanup(lc_pouch *pouch);

#endif

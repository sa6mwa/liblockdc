#ifndef LC_POUCH_INTERNAL_H
#define LC_POUCH_INTERNAL_H

#include "lc_pouch.h"

typedef struct lc_pouch_state_cache_namespace lc_pouch_state_cache_namespace;

struct lc_pouch {
  lc_allocator allocator;
  char *root_path;
  unsigned long segment_target_bytes;
  unsigned long compaction_min_segment_count;
  unsigned long compaction_min_reclaimable_bytes;
  unsigned long compaction_interval_seconds;
  unsigned long marker_sequence;
  int background_compaction_enabled;
  int single_writer;
  lc_pouch_state_cache_namespace *state_cache_namespaces;
};

void lc_pouch_state_cache_cleanup(lc_pouch *pouch);

#endif

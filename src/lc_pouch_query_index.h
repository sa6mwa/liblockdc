#ifndef LC_POUCH_QUERY_INDEX_H
#define LC_POUCH_QUERY_INDEX_H

#include "lc_pouch.h"

typedef struct lc_pouch_query_index_flush_result {
  unsigned long index_seq;
  int repaired;
} lc_pouch_query_index_flush_result;

int lc_pouch_query_index_flush(lc_pouch *pouch, const char *namespace_name,
                               unsigned long state_index_seq,
                               lc_pouch_query_index_flush_result *out,
                               lc_error *error);

#endif

#ifndef LC_POUCH_INDEX_H
#define LC_POUCH_INDEX_H

#include "lc_internal.h"

typedef struct lc_pouch_index_instant {
  double seconds;
} lc_pouch_index_instant;

typedef struct lc_pouch_index_date_bounds {
  int has_gt;
  int has_gte;
  int has_lt;
  int has_lte;
  const char *gt;
  const char *gte;
  const char *lt;
  const char *lte;
} lc_pouch_index_date_bounds;

typedef struct lc_pouch_index_parsed_date_bounds {
  int has_gt;
  int has_gte;
  int has_lt;
  int has_lte;
  lc_pouch_index_instant gt;
  lc_pouch_index_instant gte;
  lc_pouch_index_instant lt;
  lc_pouch_index_instant lte;
} lc_pouch_index_parsed_date_bounds;

int lc_pouch_index_parse_rfc3339(const char *text,
                                 lc_pouch_index_instant *out);
int lc_pouch_index_parse_date_bounds(
    const lc_pouch_index_date_bounds *bounds,
    lc_pouch_index_parsed_date_bounds *out, lc_error *error);
int lc_pouch_index_date_contains_value(
    const lc_pouch_index_parsed_date_bounds *bounds,
    const lc_pouch_index_instant *value);

#endif

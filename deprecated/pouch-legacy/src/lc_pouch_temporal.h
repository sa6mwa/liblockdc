#ifndef LC_POUCH_TEMPORAL_H
#define LC_POUCH_TEMPORAL_H

#include <stdint.h>

typedef struct lc_pouch_temporal {
  int64_t unix_seconds;
  int32_t nanosecond;
} lc_pouch_temporal;

int lc_pouch_temporal_parse(const char *text, lc_pouch_temporal *out);
int lc_pouch_temporal_may_match_liblql(const char *text);
int lc_pouch_temporal_compare(const lc_pouch_temporal *left,
                              const lc_pouch_temporal *right);

#endif

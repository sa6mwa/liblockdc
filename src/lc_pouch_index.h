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

typedef struct lc_pouch_index_docid_set {
  unsigned long *items;
  size_t count;
  size_t capacity;
} lc_pouch_index_docid_set;

typedef struct lc_pouch_index_posting {
  unsigned char *bytes;
  size_t length;
  size_t capacity;
  size_t count;
  unsigned long last_doc_id;
  int has_last_doc_id;
} lc_pouch_index_posting;

void lc_pouch_index_docid_set_cleanup(const lc_allocator *allocator,
                                      lc_pouch_index_docid_set *set);
int lc_pouch_index_docid_set_append_sorted_unique(
    lc_pouch_index_docid_set *set, unsigned long doc_id, int *added,
    const lc_allocator *allocator, lc_error *error);
int lc_pouch_index_docid_set_append_unique(
    lc_pouch_index_docid_set *set, unsigned long doc_id, int *added,
    const lc_allocator *allocator, lc_error *error);
void lc_pouch_index_posting_cleanup(const lc_allocator *allocator,
                                    lc_pouch_index_posting *posting);
int lc_pouch_index_posting_append_sorted_unique(
    lc_pouch_index_posting *posting, unsigned long doc_id, int *added,
    const lc_allocator *allocator, lc_error *error);
int lc_pouch_index_posting_append_to_set(
    const lc_pouch_index_posting *posting, lc_pouch_index_docid_set *set,
    const lc_allocator *allocator, lc_error *error);
int lc_pouch_index_parse_lql_datetime(const char *text,
                                      lc_pouch_index_instant *out);
int lc_pouch_index_parse_date_bounds(
    const lc_pouch_index_date_bounds *bounds,
    lc_pouch_index_parsed_date_bounds *out, lc_error *error);
int lc_pouch_index_date_contains_value(
    const lc_pouch_index_parsed_date_bounds *bounds,
    const lc_pouch_index_instant *value);

#endif

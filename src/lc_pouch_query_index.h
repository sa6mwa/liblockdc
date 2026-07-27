#ifndef LC_POUCH_QUERY_INDEX_H
#define LC_POUCH_QUERY_INDEX_H

#include "lc_pouch.h"
#include "lc_pouch_index.h"

typedef struct lc_pouch_query_index_flush_result {
  unsigned long index_seq;
  int repaired;
} lc_pouch_query_index_flush_result;

typedef struct lc_pouch_query_index_row_view {
  const char *key;
  unsigned long version;
  unsigned long bytes;
  int has_query_hidden;
  int query_hidden;
} lc_pouch_query_index_row_view;

typedef int (*lc_pouch_query_index_row_visit_fn)(
    const lc_pouch_query_index_row_view *row, void *context, lc_error *error);

typedef struct lc_pouch_query_index_key_view {
  const char *key;
  unsigned long version;
  unsigned long bytes;
  int has_query_hidden;
  int query_hidden;
  size_t value_index;
} lc_pouch_query_index_key_view;

typedef struct lc_pouch_query_index_scalar_term {
  const char *field;
  const char *value;
} lc_pouch_query_index_scalar_term;

typedef struct lc_pouch_query_index_range_bounds {
  int has_gt;
  int has_gte;
  int has_lt;
  int has_lte;
  double gt;
  double gte;
  double lt;
  double lte;
} lc_pouch_query_index_range_bounds;

typedef lc_pouch_index_date_bounds lc_pouch_query_index_date_bounds;

typedef int (*lc_pouch_query_index_key_visit_fn)(
    const lc_pouch_query_index_key_view *key, void *context, lc_error *error);

int lc_pouch_query_index_flush(lc_pouch *pouch, const char *namespace_name,
                               unsigned long state_index_seq,
                               lc_pouch_query_index_flush_result *out,
                               lc_error *error);
int lc_pouch_query_index_ensure_current(
    lc_pouch *pouch, const char *namespace_name, unsigned long state_index_seq,
    lc_pouch_query_index_flush_result *out, lc_error *error);
int lc_pouch_query_index_visit(lc_pouch *pouch, const char *namespace_name,
                               lc_pouch_query_index_row_visit_fn visit,
                               void *context, unsigned long *index_seq,
                               lc_error *error);
int lc_pouch_query_index_visit_scalar(lc_pouch *pouch,
                                      const char *namespace_name,
                                      const char *field,
                                      const char *value,
                                      lc_pouch_query_index_key_visit_fn visit,
                                      void *context,
                                      unsigned long *index_seq,
                                      lc_error *error);
int lc_pouch_query_index_visit_scalar_any(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *const *values, size_t value_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error);
int lc_pouch_query_index_visit_scalar_terms(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_scalar_term *terms, size_t term_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error);
int lc_pouch_query_index_visit_scalar_any_merged(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *const *values, size_t value_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error);
int lc_pouch_query_index_visit_prefix(lc_pouch *pouch,
                                      const char *namespace_name,
                                      const char *field,
                                      const char *prefix,
                                      int ignore_case,
                                      lc_pouch_query_index_key_visit_fn visit,
                                      void *context,
                                      unsigned long *index_seq,
                                      lc_error *error);
int lc_pouch_query_index_visit_contains(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *needle, int ignore_case,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error);
int lc_pouch_query_index_visit_range(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const lc_pouch_query_index_range_bounds *bounds,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error);
int lc_pouch_query_index_visit_date(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const lc_pouch_query_index_date_bounds *bounds,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    unsigned long *index_seq, lc_error *error);
int lc_pouch_query_index_visit_exists(lc_pouch *pouch,
                                      const char *namespace_name,
                                      const char *field,
                                      lc_pouch_query_index_key_visit_fn visit,
                                      void *context,
                                      unsigned long *index_seq,
                                      lc_error *error);

#endif

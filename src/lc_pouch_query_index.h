#ifndef LC_POUCH_QUERY_INDEX_H
#define LC_POUCH_QUERY_INDEX_H

#include "lc_pouch.h"
#include "lc_pouch_index.h"

typedef struct lc_pouch_query_index_flush_result {
  lc_pouch_generation index_seq;
  int repaired;
} lc_pouch_query_index_flush_result;

typedef struct lc_pouch_query_index_row_view {
  const char *key;
  const char *key_hex;
  unsigned long doc_id;
  lc_pouch_generation version;
  uint64_t bytes;
  int has_query_hidden;
  int query_hidden;
} lc_pouch_query_index_row_view;

typedef int (*lc_pouch_query_index_row_visit_fn)(
    const lc_pouch_query_index_row_view *row, void *context, lc_error *error);

typedef struct lc_pouch_query_index_key_view {
  const char *key;
  const char *key_hex;
  unsigned long doc_id;
  lc_pouch_generation version;
  uint64_t bytes;
  int has_query_hidden;
  int query_hidden;
  size_t value_index;
  int candidate_exact;
} lc_pouch_query_index_key_view;

typedef lc_pouch_index_plain_term lc_pouch_query_index_scalar_term;

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
                               lc_pouch_generation state_index_seq,
                               lc_pouch_query_index_flush_result *out,
                               lc_error *error);
int lc_pouch_query_index_flush_validated(lc_pouch *pouch,
                                         const char *namespace_name,
                                         lc_pouch_generation state_index_seq,
                                         lc_pouch_query_index_flush_result *out,
                                         lc_error *error);
int lc_pouch_query_index_has_pending(lc_pouch *pouch,
                                     const char *namespace_name);
int lc_pouch_query_index_manifest_seq(lc_pouch *pouch,
                                      const char *namespace_name,
                                      lc_pouch_generation *index_seq,
                                      lc_error *error);
int lc_pouch_query_index_ensure_current(lc_pouch *pouch,
                                        const char *namespace_name,
                                        lc_pouch_generation state_index_seq,
                                        int validate_current,
                                        lc_pouch_query_index_flush_result *out,
                                        lc_error *error);
int lc_pouch_query_index_warm_namespace(lc_pouch *pouch,
                                        const char *namespace_name,
                                        lc_error *error);
void lc_pouch_query_index_note_state_write(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    const char *content_type, lc_source *body,
    const lc_pouch_state_write_result *result);
void lc_pouch_query_index_note_state_delete(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    const lc_pouch_state_write_result *result);
int lc_pouch_query_index_visit(lc_pouch *pouch, const char *namespace_name,
                               lc_pouch_query_index_row_visit_fn visit,
                               void *context, lc_pouch_generation *index_seq,
                               lc_error *error);
int lc_pouch_query_index_visit_scalar(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *value, char value_type, lc_pouch_query_index_key_visit_fn visit,
    void *context, lc_pouch_generation *index_seq, lc_error *error);
int lc_pouch_query_index_scalar_candidates_exact(const char *value,
                                                 char value_type);
int lc_pouch_query_index_visit_scalar_any(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *const *values, const char *value_types, size_t value_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error);
int lc_pouch_query_index_visit_scalar_terms(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_scalar_term *terms, size_t term_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error);
int lc_pouch_query_index_visit_scalar_terms_merged(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_scalar_term *terms, size_t term_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error);
int lc_pouch_query_index_visit_scalar_terms_docids(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_scalar_term *terms, size_t term_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error);
int lc_pouch_query_index_visit_scalar_any_docids(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *const *values, const char *value_types, size_t value_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error);
int lc_pouch_query_index_visit_scalar_any_merged(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *const *values, const char *value_types, size_t value_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error);
int lc_pouch_query_index_visit_prefix(lc_pouch *pouch,
                                      const char *namespace_name,
                                      const char *field, const char *prefix,
                                      int ignore_case,
                                      lc_pouch_query_index_key_visit_fn visit,
                                      void *context, lc_pouch_generation *index_seq,
                                      lc_error *error);
int lc_pouch_query_index_prefix_candidates_exact(const char *prefix);
int lc_pouch_query_index_visit_prefix_candidates(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *prefix, int ignore_case,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error);
int lc_pouch_query_index_visit_contains(lc_pouch *pouch,
                                        const char *namespace_name,
                                        const char *field, const char *needle,
                                        int ignore_case,
                                        lc_pouch_query_index_key_visit_fn visit,
                                        void *context, lc_pouch_generation *index_seq,
                                        lc_error *error);
int lc_pouch_query_index_visit_contains_complete(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *needle, int ignore_case,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, int *complete, lc_error *error);
int lc_pouch_query_index_contains_text_complete(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    int *complete, lc_pouch_generation *index_seq, lc_error *error);
int lc_pouch_query_index_visit_any_text_token(
    lc_pouch *pouch, const char *namespace_name, const char *needle,
    int ignore_case, lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error);
int lc_pouch_query_index_visit_contains_candidates(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *needle, int ignore_case,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error);
int lc_pouch_query_index_visit_range(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const lc_pouch_query_index_range_bounds *bounds,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error);
int lc_pouch_query_index_visit_date(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const lc_pouch_query_index_date_bounds *bounds,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error);
int lc_pouch_query_index_visit_exists(lc_pouch *pouch,
                                      const char *namespace_name,
                                      const char *field,
                                      lc_pouch_query_index_key_visit_fn visit,
                                      void *context, lc_pouch_generation *index_seq,
                                      lc_error *error);

#endif

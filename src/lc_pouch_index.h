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

typedef struct lc_pouch_index_doc {
  const char *key_hex;
  unsigned long version;
  unsigned long bytes;
  int has_query_hidden;
  int query_hidden;
} lc_pouch_index_doc;

typedef struct lc_pouch_index_doc_table {
  lc_pouch_index_doc *items;
  size_t count;
  size_t capacity;
} lc_pouch_index_doc_table;

typedef struct lc_pouch_index_result_key {
  char *key_hex;
  unsigned long doc_id;
  unsigned long version;
  unsigned long bytes;
  int has_query_hidden;
  int query_hidden;
  size_t value_index;
} lc_pouch_index_result_key;

typedef struct lc_pouch_index_result_key_list {
  lc_pouch_index_result_key *items;
  size_t count;
  size_t capacity;
} lc_pouch_index_result_key_list;

typedef struct lc_pouch_index_result_row {
  char *key;
  char *key_hex;
  unsigned long doc_id;
  unsigned long version;
  unsigned long bytes;
  int has_query_hidden;
  int query_hidden;
  size_t value_index;
} lc_pouch_index_result_row;

typedef struct lc_pouch_index_result_row_list {
  lc_pouch_index_result_row *items;
  size_t count;
  size_t capacity;
} lc_pouch_index_result_row_list;

typedef struct lc_pouch_index_term_field {
  char *field_hex;
  unsigned long first_line;
  unsigned long line_count;
} lc_pouch_index_term_field;

typedef struct lc_pouch_index_term_value {
  char *field_hex;
  char *value_hex;
  unsigned long first_line;
  unsigned long line_count;
} lc_pouch_index_term_value;

typedef struct lc_pouch_index_term_key {
  const char *field_hex;
  const char *value_hex;
} lc_pouch_index_term_key;

typedef struct lc_pouch_index_term_range {
  unsigned long first_line;
  unsigned long line_count;
} lc_pouch_index_term_range;

typedef struct lc_pouch_index_posting {
  unsigned char *bytes;
  size_t length;
  size_t capacity;
  size_t count;
  unsigned long last_doc_id;
  int has_last_doc_id;
} lc_pouch_index_posting;

typedef struct lc_pouch_index_dense_posting {
  unsigned char *bits;
  size_t length;
  size_t capacity;
  size_t count;
  unsigned long max_doc_id;
  unsigned long last_doc_id;
  int has_last_doc_id;
} lc_pouch_index_dense_posting;

typedef enum lc_pouch_index_adaptive_posting_kind {
  LC_POUCH_INDEX_ADAPTIVE_POSTING_SPARSE = 1,
  LC_POUCH_INDEX_ADAPTIVE_POSTING_DENSE = 2
} lc_pouch_index_adaptive_posting_kind;

typedef struct lc_pouch_index_adaptive_posting {
  lc_pouch_index_posting sparse;
  lc_pouch_index_dense_posting dense;
  int dense_disabled;
} lc_pouch_index_adaptive_posting;

void lc_pouch_index_docid_set_cleanup(const lc_allocator *allocator,
                                      lc_pouch_index_docid_set *set);
int lc_pouch_index_docid_set_append_sorted_unique(
    lc_pouch_index_docid_set *set, unsigned long doc_id, int *added,
    const lc_allocator *allocator, lc_error *error);
int lc_pouch_index_docid_set_append_unique(
    lc_pouch_index_docid_set *set, unsigned long doc_id, int *added,
    const lc_allocator *allocator, lc_error *error);
int lc_pouch_index_docid_set_union_sorted(
    const lc_pouch_index_docid_set *left,
    const lc_pouch_index_docid_set *right, lc_pouch_index_docid_set *out,
    const lc_allocator *allocator, lc_error *error);
int lc_pouch_index_docid_set_intersect_sorted(
    const lc_pouch_index_docid_set *left,
    const lc_pouch_index_docid_set *right, lc_pouch_index_docid_set *out,
    const lc_allocator *allocator, lc_error *error);
int lc_pouch_index_docid_set_subtract_sorted(
    const lc_pouch_index_docid_set *left,
    const lc_pouch_index_docid_set *right, lc_pouch_index_docid_set *out,
    const lc_allocator *allocator, lc_error *error);
void lc_pouch_index_doc_table_cleanup(const lc_allocator *allocator,
                                      lc_pouch_index_doc_table *table);
int lc_pouch_index_doc_table_append_sorted_unique(
    lc_pouch_index_doc_table *table, const char *key_hex,
    unsigned long version, unsigned long bytes, int has_query_hidden,
    int query_hidden, unsigned long *doc_id, const lc_allocator *allocator,
    lc_error *error);
int lc_pouch_index_doc_table_find_key_hex(
    const lc_pouch_index_doc_table *table, const char *key_hex,
    unsigned long *doc_id, int *found, lc_error *error);
int lc_pouch_index_doc_table_get(const lc_pouch_index_doc_table *table,
                                 unsigned long doc_id,
                                 const lc_pouch_index_doc **doc,
                                 lc_error *error);
void lc_pouch_index_result_key_list_cleanup(
    const lc_allocator *allocator, lc_pouch_index_result_key_list *list);
int lc_pouch_index_result_key_list_add(
    const lc_allocator *allocator, lc_pouch_index_result_key_list *list,
    const char *key_hex, unsigned long doc_id, unsigned long version,
    unsigned long bytes, int has_query_hidden, int query_hidden,
    size_t value_index, lc_error *error);
int lc_pouch_index_result_key_list_sort_compact_docids(
    const lc_allocator *allocator, lc_pouch_index_result_key_list *list,
    lc_error *error);
void lc_pouch_index_result_row_list_cleanup(
    const lc_allocator *allocator, lc_pouch_index_result_row_list *list);
int lc_pouch_index_result_row_list_add(
    const lc_allocator *allocator, lc_pouch_index_result_row_list *list,
    const char *key, const char *key_hex, unsigned long doc_id,
    unsigned long version, unsigned long bytes, int has_query_hidden,
    int query_hidden, size_t value_index, lc_error *error);
void lc_pouch_index_term_fields_cleanup(
    const lc_allocator *allocator, lc_pouch_index_term_field *fields,
    size_t count);
void lc_pouch_index_term_values_cleanup(
    const lc_allocator *allocator, lc_pouch_index_term_value *values,
    size_t count);
int lc_pouch_index_term_fields_find(const lc_pouch_index_term_field *fields,
                                    size_t count, const char *field_hex,
                                    unsigned long *first_line,
                                    unsigned long *line_count);
int lc_pouch_index_term_values_find(const lc_pouch_index_term_value *values,
                                    size_t count, const char *field_hex,
                                    const char *value_hex,
                                    unsigned long *first_line,
                                    unsigned long *line_count);
void lc_pouch_index_term_keys_cleanup(
    const lc_allocator *allocator, lc_pouch_index_term_key *terms,
    size_t count);
int lc_pouch_index_term_key_compare_items(
    const lc_pouch_index_term_key *left,
    const lc_pouch_index_term_key *right);
int lc_pouch_index_term_key_compare(const void *left, const void *right);
int lc_pouch_index_term_key_compare_pair(
    const char *field_hex, const char *value_hex,
    const lc_pouch_index_term_key *term);
int lc_pouch_index_term_keys_find(const lc_pouch_index_term_key *terms,
                                  size_t count, const char *field_hex,
                                  const char *value_hex,
                                  size_t *index_out);
int lc_pouch_index_term_field_parse_line(
    char *line, lc_pouch_index_term_field *field,
    const lc_allocator *allocator, lc_error *error);
int lc_pouch_index_term_value_parse_line(
    char *line, lc_pouch_index_term_value *value,
    const lc_allocator *allocator, lc_error *error);
void lc_pouch_index_term_ranges_cleanup(
    const lc_allocator *allocator, lc_pouch_index_term_range *ranges);
int lc_pouch_index_term_values_collect_ranges(
    const lc_pouch_index_term_value *values, size_t value_count,
    const lc_pouch_index_term_key *terms, size_t term_count,
    lc_pouch_index_term_range **out_ranges, size_t *out_count,
    const lc_allocator *allocator, lc_error *error);
int lc_pouch_index_term_fields_select_range(
    const lc_pouch_index_term_field *fields, size_t field_count,
    const char *field_hex, const lc_pouch_index_term_key *terms,
    size_t term_count, unsigned long term_line_count,
    unsigned long *first_line, unsigned long *line_count);
void lc_pouch_index_posting_cleanup(const lc_allocator *allocator,
                                    lc_pouch_index_posting *posting);
int lc_pouch_index_posting_append_sorted_unique(
    lc_pouch_index_posting *posting, unsigned long doc_id, int *added,
    const lc_allocator *allocator, lc_error *error);
int lc_pouch_index_posting_append_to_set(
    const lc_pouch_index_posting *posting, lc_pouch_index_docid_set *set,
    const lc_allocator *allocator, lc_error *error);
void lc_pouch_index_dense_posting_cleanup(
    const lc_allocator *allocator, lc_pouch_index_dense_posting *posting);
int lc_pouch_index_dense_posting_append_sorted_unique(
    lc_pouch_index_dense_posting *posting, unsigned long doc_id, int *added,
    const lc_allocator *allocator, lc_error *error);
int lc_pouch_index_dense_posting_append_to_set(
    const lc_pouch_index_dense_posting *posting, lc_pouch_index_docid_set *set,
    const lc_allocator *allocator, lc_error *error);
void lc_pouch_index_adaptive_posting_cleanup(
    const lc_allocator *allocator, lc_pouch_index_adaptive_posting *posting);
int lc_pouch_index_adaptive_posting_append_sorted_unique(
    lc_pouch_index_adaptive_posting *posting, unsigned long doc_id, int *added,
    const lc_allocator *allocator, lc_error *error);
lc_pouch_index_adaptive_posting_kind
lc_pouch_index_adaptive_posting_selected_kind(
    const lc_pouch_index_adaptive_posting *posting);
int lc_pouch_index_adaptive_posting_append_to_set(
    const lc_pouch_index_adaptive_posting *posting,
    lc_pouch_index_docid_set *set, const lc_allocator *allocator,
    lc_error *error);
int lc_pouch_index_parse_lql_datetime(const char *text,
                                      lc_pouch_index_instant *out);
int lc_pouch_index_parse_date_bounds(
    const lc_pouch_index_date_bounds *bounds,
    lc_pouch_index_parsed_date_bounds *out, lc_error *error);
int lc_pouch_index_date_contains_value(
    const lc_pouch_index_parsed_date_bounds *bounds,
    const lc_pouch_index_instant *value);

#endif

#ifndef LC_POUCH_INDEX_H
#define LC_POUCH_INDEX_H

#include "lc_pouch_store.h"

#include <stddef.h>
#include <stdint.h>

typedef uint32_t lc_pouch_index_doc_id;
typedef uint32_t lc_pouch_index_term_id;

typedef struct lc_pouch_index_doc_id_set {
  lc_pouch_index_doc_id *items;
  size_t count;
  size_t capacity;
} lc_pouch_index_doc_id_set;

typedef struct lc_pouch_index_doc_id_scratch {
  lc_pouch_index_doc_id_set term;
  lc_pouch_index_doc_id_set merge;
} lc_pouch_index_doc_id_scratch;

typedef struct lc_pouch_index_doc_entry {
  char *namespace_name;
  char *key;
  lc_pouch_index_doc_id id;
} lc_pouch_index_doc_entry;

typedef struct lc_pouch_index_doc_table {
  lc_pouch_index_doc_entry *entries;
  size_t count;
  size_t capacity;
} lc_pouch_index_doc_table;

typedef enum lc_pouch_index_posting_encoding {
  LC_POUCH_INDEX_POSTING_EMPTY = 0,
  LC_POUCH_INDEX_POSTING_SPARSE = 1,
  LC_POUCH_INDEX_POSTING_DENSE = 2
} lc_pouch_index_posting_encoding;

typedef enum lc_pouch_index_result_plan_kind {
  LC_POUCH_INDEX_RESULT_PLAN_EQ = 1,
  LC_POUCH_INDEX_RESULT_PLAN_EXISTS = 2,
  LC_POUCH_INDEX_RESULT_PLAN_IN = 3,
  LC_POUCH_INDEX_RESULT_PLAN_RANGE = 4,
  LC_POUCH_INDEX_RESULT_PLAN_PREFIX = 5,
  LC_POUCH_INDEX_RESULT_PLAN_CONTAINS = 6,
  LC_POUCH_INDEX_RESULT_PLAN_DATE_AFTER = 7
} lc_pouch_index_result_plan_kind;

typedef struct lc_pouch_index_identity {
  uint64_t sequence;
  uint64_t manifest_generation;
} lc_pouch_index_identity;

typedef struct lc_pouch_index_posting {
  lc_pouch_index_posting_encoding encoding;
  size_t count;
  lc_pouch_index_doc_id max_doc_id;
  unsigned char *sparse;
  size_t sparse_len;
  uint64_t *dense;
  size_t dense_word_count;
} lc_pouch_index_posting;

typedef struct lc_pouch_index_term_entry {
  char *field;
  char *value;
  lc_pouch_index_term_id id;
} lc_pouch_index_term_entry;

typedef struct lc_pouch_index_term_table {
  lc_pouch_index_term_entry *entries;
  size_t count;
  size_t capacity;
  lc_pouch_index_term_id next_id;
} lc_pouch_index_term_table;

typedef struct lc_pouch_index_term_posting_entry {
  lc_pouch_index_term_id term_id;
  lc_pouch_index_posting posting;
} lc_pouch_index_term_posting_entry;

typedef struct lc_pouch_index_term_posting_table {
  lc_pouch_index_term_posting_entry *entries;
  size_t count;
  size_t capacity;
} lc_pouch_index_term_posting_table;

typedef struct lc_pouch_index_temporal_doc_entry {
  int64_t unix_seconds;
  int32_t nanosecond;
  lc_pouch_index_doc_id doc_id;
} lc_pouch_index_temporal_doc_entry;

typedef struct lc_pouch_index_temporal_field_entry {
  char *field;
  lc_pouch_index_temporal_doc_entry *values;
  size_t value_count;
  size_t value_capacity;
  int values_sorted;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_posting posting;
  int posting_ready;
} lc_pouch_index_temporal_field_entry;

typedef struct lc_pouch_index_temporal_posting_table {
  lc_pouch_index_temporal_field_entry *fields;
  size_t field_count;
  size_t field_capacity;
} lc_pouch_index_temporal_posting_table;

typedef struct lc_pouch_index_temporal_generation {
  lc_pouch_index_identity identity;
  char *namespace_name;
  lc_pouch_index_temporal_posting_table postings;
} lc_pouch_index_temporal_generation;

typedef struct lc_pouch_index_number_doc_entry {
  char *number;
  lc_pouch_index_doc_id doc_id;
} lc_pouch_index_number_doc_entry;

typedef struct lc_pouch_index_number_field_entry {
  char *field;
  lc_pouch_index_number_doc_entry *values;
  size_t value_count;
  size_t value_capacity;
  int values_sorted;
  lc_pouch_index_doc_id_set doc_ids;
  lc_pouch_index_posting posting;
  int posting_ready;
} lc_pouch_index_number_field_entry;

typedef struct lc_pouch_index_number_posting_table {
  lc_pouch_index_number_field_entry *fields;
  size_t field_count;
  size_t field_capacity;
} lc_pouch_index_number_posting_table;

typedef struct lc_pouch_index_number_generation {
  lc_pouch_index_identity identity;
  char *namespace_name;
  lc_pouch_index_number_posting_table postings;
} lc_pouch_index_number_generation;

typedef struct lc_pouch_index_prepared_term_cache {
  uint64_t generation;
  lc_pouch_index_identity identity;
  lc_pouch_index_term_table terms;
  lc_pouch_index_term_posting_table postings;
} lc_pouch_index_prepared_term_cache;

typedef struct lc_pouch_index_term_generation {
  lc_pouch_index_identity identity;
  char *namespace_name;
  lc_pouch_index_term_table terms;
  lc_pouch_index_term_posting_table postings;
} lc_pouch_index_term_generation;

typedef struct lc_pouch_index_prepared_temporal_cache {
  uint64_t generation;
  lc_pouch_index_identity identity;
  lc_pouch_index_temporal_posting_table postings;
} lc_pouch_index_prepared_temporal_cache;

typedef struct lc_pouch_index_result_cache_entry {
  uint64_t generation;
  lc_pouch_index_identity identity;
  char *plan_key;
  lc_pouch_index_doc_id_set doc_ids;
} lc_pouch_index_result_cache_entry;

typedef struct lc_pouch_index_result_cache {
  lc_pouch_index_result_cache_entry *entries;
  size_t count;
  size_t capacity;
  unsigned long hits;
  unsigned long misses;
  unsigned long puts;
  unsigned long replacements;
} lc_pouch_index_result_cache;

typedef struct lc_pouch_index_result_page {
  lc_pouch_index_doc_id_set doc_ids;
  char *next_start_after;
  int truncated;
} lc_pouch_index_result_page;

typedef int (*lc_pouch_index_exact_term_doc_ids_fn)(
    void *context, const char *field, const char *value,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
typedef int (*lc_pouch_index_exists_term_doc_ids_fn)(
    void *context, const char *field, lc_pouch_index_doc_id_set *doc_ids,
    lc_error *error);
typedef int (*lc_pouch_index_range_term_doc_ids_fn)(
    void *context, const lc_pouch_document_range_term *term,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
typedef int (*lc_pouch_index_prefix_term_doc_ids_fn)(
    void *context, const lc_pouch_document_prefix_term *term,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
typedef int (*lc_pouch_index_contains_term_doc_ids_fn)(
    void *context, const lc_pouch_document_contains_term *term,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
typedef int (*lc_pouch_index_date_after_doc_ids_fn)(
    void *context, const char *field, int64_t unix_seconds, int32_t nanosecond,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
typedef int (*lc_pouch_index_result_collect_doc_ids_fn)(
    void *context, int cacheable, lc_pouch_index_doc_id_set *doc_ids,
    lc_error *error);

void lc_pouch_index_doc_id_set_cleanup(const lc_pouch_allocator *allocator,
                                       lc_pouch_index_doc_id_set *set);
void lc_pouch_index_doc_id_set_reset(lc_pouch_index_doc_id_set *set);
void lc_pouch_index_doc_id_scratch_cleanup(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_doc_id_scratch *scratch);
void lc_pouch_index_doc_table_cleanup(const lc_pouch_allocator *allocator,
                                      lc_pouch_index_doc_table *table);
int lc_pouch_index_doc_table_find(const lc_pouch_index_doc_table *table,
                                  const char *namespace_name, const char *key,
                                  lc_pouch_index_doc_id *id_out);
int lc_pouch_index_doc_table_find_or_add(const lc_pouch_allocator *allocator,
                                         lc_pouch_index_doc_table *table,
                                         const char *namespace_name,
                                         const char *key,
                                         lc_pouch_index_doc_id *id_out);
int lc_pouch_index_doc_table_lookup(const lc_pouch_index_doc_table *table,
                                    lc_pouch_index_doc_id id,
                                    const char **namespace_name_out,
                                    const char **key_out);
int lc_pouch_index_doc_id_set_append(const lc_pouch_allocator *allocator,
                                     lc_pouch_index_doc_id_set *set,
                                     lc_pouch_index_doc_id id);
int lc_pouch_index_doc_id_set_sort_unique(lc_pouch_index_doc_id_set *set);
int lc_pouch_index_doc_id_set_clone(const lc_pouch_allocator *allocator,
                                    lc_pouch_index_doc_id_set *dst,
                                    const lc_pouch_index_doc_id_set *src);
int lc_pouch_index_doc_id_set_union(const lc_pouch_allocator *allocator,
                                    lc_pouch_index_doc_id_set *dst,
                                    const lc_pouch_index_doc_id_set *left,
                                    const lc_pouch_index_doc_id_set *right);
int lc_pouch_index_doc_id_set_intersect(const lc_pouch_allocator *allocator,
                                        lc_pouch_index_doc_id_set *dst,
                                        const lc_pouch_index_doc_id_set *left,
                                        const lc_pouch_index_doc_id_set *right);
int lc_pouch_index_doc_id_set_subtract(const lc_pouch_allocator *allocator,
                                       lc_pouch_index_doc_id_set *dst,
                                       const lc_pouch_index_doc_id_set *left,
                                       const lc_pouch_index_doc_id_set *right);

void lc_pouch_index_posting_cleanup(const lc_pouch_allocator *allocator,
                                    lc_pouch_index_posting *posting);
int lc_pouch_index_posting_build(const lc_pouch_allocator *allocator,
                                 lc_pouch_index_posting *posting,
                                 const lc_pouch_index_doc_id *ids,
                                 size_t count);
int lc_pouch_index_posting_decode(const lc_pouch_allocator *allocator,
                                  const lc_pouch_index_posting *posting,
                                  lc_pouch_index_doc_id_set *dst);
int lc_pouch_index_posting_append(const lc_pouch_allocator *allocator,
                                  const lc_pouch_index_posting *posting,
                                  lc_pouch_index_doc_id_set *dst);
int lc_pouch_index_posting_intersect(const lc_pouch_allocator *allocator,
                                     const lc_pouch_index_posting *posting,
                                     const lc_pouch_index_doc_id_set *filter,
                                     lc_pouch_index_doc_id_set *dst);
void lc_pouch_index_term_table_cleanup(const lc_pouch_allocator *allocator,
                                       lc_pouch_index_term_table *table);
int lc_pouch_index_term_table_find(const lc_pouch_index_term_table *table,
                                   const char *field, const char *value,
                                   lc_pouch_index_term_id *id_out);
int lc_pouch_index_term_table_find_or_add(const lc_pouch_allocator *allocator,
                                          lc_pouch_index_term_table *table,
                                          const char *field, const char *value,
                                          lc_pouch_index_term_id *id_out);
void lc_pouch_index_term_posting_table_cleanup(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_term_posting_table *table);
int lc_pouch_index_term_posting_table_put(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_term_posting_table *table, lc_pouch_index_term_id term_id,
    const lc_pouch_index_doc_id *ids, size_t count);
int lc_pouch_index_term_posting_table_contains(
    const lc_pouch_index_term_posting_table *table,
    lc_pouch_index_term_id term_id);
int lc_pouch_index_term_posting_table_decode(
    const lc_pouch_allocator *allocator,
    const lc_pouch_index_term_posting_table *table,
    lc_pouch_index_term_id term_id, lc_pouch_index_doc_id_set *dst);
int lc_pouch_index_term_posting_table_append(
    const lc_pouch_allocator *allocator,
    const lc_pouch_index_term_posting_table *table,
    lc_pouch_index_term_id term_id, lc_pouch_index_doc_id_set *dst);
void lc_pouch_index_term_generation_cleanup(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_term_generation *generation);
int lc_pouch_index_term_generation_encoded_size(
    const lc_pouch_index_term_generation *generation, size_t *size_out);
int lc_pouch_index_term_generation_encode(
    const lc_pouch_index_term_generation *generation, unsigned char *dst,
    size_t dst_size, size_t *written_out);
int lc_pouch_index_term_generation_decode(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_term_generation *generation, const unsigned char *src,
    size_t src_size);
void lc_pouch_index_temporal_posting_table_cleanup(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_temporal_posting_table *table);
int lc_pouch_index_temporal_posting_table_mark_field(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_temporal_posting_table *table, const char *field);
int lc_pouch_index_temporal_posting_table_has_field(
    const lc_pouch_index_temporal_posting_table *table, const char *field);
int lc_pouch_index_temporal_posting_table_add_value_doc_id(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_temporal_posting_table *table, const char *field,
    int64_t unix_seconds, int32_t nanosecond, lc_pouch_index_doc_id doc_id);
int lc_pouch_index_temporal_posting_table_add_residual_doc_id(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_temporal_posting_table *table, const char *field,
    lc_pouch_index_doc_id doc_id);
int lc_pouch_index_temporal_posting_table_build_postings(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_temporal_posting_table *table);
int lc_pouch_index_temporal_posting_table_append_after(
    const lc_pouch_allocator *allocator,
    const lc_pouch_index_temporal_posting_table *table, const char *field,
    int64_t unix_seconds, int32_t nanosecond, lc_pouch_index_doc_id_set *dst);
int lc_pouch_index_temporal_posting_table_encoded_size(
    const lc_pouch_index_temporal_posting_table *table, size_t *size_out);
int lc_pouch_index_temporal_posting_table_encode(
    const lc_pouch_index_temporal_posting_table *table, unsigned char *dst,
    size_t dst_size, size_t *written_out);
int lc_pouch_index_temporal_posting_table_decode(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_temporal_posting_table *table, const unsigned char *src,
    size_t src_size);
void lc_pouch_index_temporal_generation_cleanup(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_temporal_generation *generation);
int lc_pouch_index_temporal_generation_encoded_size(
    const lc_pouch_index_temporal_generation *generation, size_t *size_out);
int lc_pouch_index_temporal_generation_encode(
    const lc_pouch_index_temporal_generation *generation, unsigned char *dst,
    size_t dst_size, size_t *written_out);
int lc_pouch_index_temporal_generation_decode(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_temporal_generation *generation, const unsigned char *src,
    size_t src_size);
void lc_pouch_index_number_posting_table_cleanup(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_number_posting_table *table);
int lc_pouch_index_number_posting_table_mark_field(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_number_posting_table *table, const char *field);
int lc_pouch_index_number_posting_table_has_field(
    const lc_pouch_index_number_posting_table *table, const char *field);
int lc_pouch_index_number_posting_table_add_value_doc_id(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_number_posting_table *table, const char *field,
    const char *number, lc_pouch_index_doc_id doc_id);
int lc_pouch_index_number_posting_table_add_residual_doc_id(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_number_posting_table *table, const char *field,
    lc_pouch_index_doc_id doc_id);
int lc_pouch_index_number_posting_table_build_postings(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_number_posting_table *table);
int lc_pouch_index_number_posting_table_append_range(
    const lc_pouch_allocator *allocator,
    const lc_pouch_index_number_posting_table *table, const char *field,
    const char *gt, const char *gte, const char *lt, const char *lte,
    lc_pouch_index_doc_id_set *dst);
int lc_pouch_index_number_posting_table_encoded_size(
    const lc_pouch_index_number_posting_table *table, size_t *size_out);
int lc_pouch_index_number_posting_table_encode(
    const lc_pouch_index_number_posting_table *table, unsigned char *dst,
    size_t dst_size, size_t *written_out);
int lc_pouch_index_number_posting_table_decode(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_number_posting_table *table, const unsigned char *src,
    size_t src_size);
void lc_pouch_index_number_generation_cleanup(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_number_generation *generation);
int lc_pouch_index_number_generation_encoded_size(
    const lc_pouch_index_number_generation *generation, size_t *size_out);
int lc_pouch_index_number_generation_encode(
    const lc_pouch_index_number_generation *generation, unsigned char *dst,
    size_t dst_size, size_t *written_out);
int lc_pouch_index_number_generation_decode(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_number_generation *generation, const unsigned char *src,
    size_t src_size);
void lc_pouch_index_prepared_term_cache_cleanup(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_prepared_term_cache *cache);
void lc_pouch_index_prepared_term_cache_refresh(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_prepared_term_cache *cache, uint64_t generation);
void lc_pouch_index_prepared_term_cache_refresh_identity(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_prepared_term_cache *cache,
    lc_pouch_index_identity identity);
void lc_pouch_index_prepared_temporal_cache_cleanup(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_prepared_temporal_cache *cache);
void lc_pouch_index_prepared_temporal_cache_refresh_identity(
    const lc_pouch_allocator *allocator,
    lc_pouch_index_prepared_temporal_cache *cache,
    lc_pouch_index_identity identity);
void lc_pouch_index_result_cache_cleanup(const lc_pouch_allocator *allocator,
                                         lc_pouch_index_result_cache *cache);
void lc_pouch_index_result_page_cleanup(const lc_pouch_allocator *allocator,
                                        lc_pouch_index_result_page *page);
int lc_pouch_index_result_page_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_index_doc_table *doc_table,
    const lc_pouch_index_doc_id_set *doc_ids, const char *namespace_name,
    const char *start_after, size_t limit, lc_pouch_index_result_page *page,
    int *invalid_doc_id_out);
char *lc_pouch_index_result_plan_key(const lc_pouch_allocator *allocator,
                                     const lc_pouch_query_index_scan_req *req,
                                     lc_pouch_index_result_plan_kind kind);
int lc_pouch_index_result_cache_find(const lc_pouch_allocator *allocator,
                                     lc_pouch_index_result_cache *cache,
                                     uint64_t generation, const char *plan_key,
                                     lc_pouch_index_doc_id_set *dst);
int lc_pouch_index_result_cache_find_identity(
    const lc_pouch_allocator *allocator, lc_pouch_index_result_cache *cache,
    lc_pouch_index_identity identity, const char *plan_key,
    lc_pouch_index_doc_id_set *dst);
int lc_pouch_index_result_cache_put(const lc_pouch_allocator *allocator,
                                    lc_pouch_index_result_cache *cache,
                                    uint64_t generation, const char *plan_key,
                                    const lc_pouch_index_doc_id_set *doc_ids);
int lc_pouch_index_result_cache_put_identity(
    const lc_pouch_allocator *allocator, lc_pouch_index_result_cache *cache,
    lc_pouch_index_identity identity, const char *plan_key,
    const lc_pouch_index_doc_id_set *doc_ids);
int lc_pouch_index_cached_result_page(
    const lc_pouch_allocator *allocator,
    const lc_pouch_index_doc_table *doc_table,
    lc_pouch_index_result_cache *cache, uint64_t generation,
    const lc_pouch_query_index_scan_req *req,
    lc_pouch_index_result_plan_kind kind,
    lc_pouch_index_result_collect_doc_ids_fn collect, void *collect_context,
    lc_pouch_index_result_page *page, int *invalid_doc_id_out, lc_error *error);
int lc_pouch_index_cached_result_page_identity(
    const lc_pouch_allocator *allocator,
    const lc_pouch_index_doc_table *doc_table,
    lc_pouch_index_result_cache *cache, lc_pouch_index_identity identity,
    const lc_pouch_query_index_scan_req *req,
    lc_pouch_index_result_plan_kind kind,
    lc_pouch_index_result_collect_doc_ids_fn collect, void *collect_context,
    lc_pouch_index_result_page *page, int *invalid_doc_id_out, lc_error *error);
int lc_pouch_index_collect_eq_term_doc_ids(
    const lc_pouch_allocator *allocator, const lc_pouch_document_eq_term *term,
    lc_pouch_index_exact_term_doc_ids_fn read_exact, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
int lc_pouch_index_collect_eq_term_with_not_eq_doc_ids(
    const lc_pouch_allocator *allocator, const lc_pouch_document_eq_term *term,
    const lc_pouch_document_eq_term *not_eq_terms, size_t not_eq_term_count,
    lc_pouch_index_exact_term_doc_ids_fn read_exact, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
int lc_pouch_index_collect_in_term_doc_ids(
    const lc_pouch_allocator *allocator, const lc_pouch_document_in_term *term,
    lc_pouch_index_exact_term_doc_ids_fn read_exact, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
int lc_pouch_index_collect_in_term_with_eq_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_in_term *in_term,
    const lc_pouch_document_eq_term *eq_terms, size_t eq_term_count,
    lc_pouch_index_exact_term_doc_ids_fn read_exact, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
int lc_pouch_index_collect_in_term_with_eq_and_not_eq_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_in_term *in_term,
    const lc_pouch_document_eq_term *eq_terms, size_t eq_term_count,
    const lc_pouch_document_eq_term *not_eq_terms, size_t not_eq_term_count,
    lc_pouch_index_exact_term_doc_ids_fn read_exact, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
int lc_pouch_index_collect_exists_term_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_exists_term *term,
    lc_pouch_index_exists_term_doc_ids_fn read_exists, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
int lc_pouch_index_collect_exists_term_with_eq_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_exists_term *exists_term,
    const lc_pouch_document_eq_term *eq_terms, size_t eq_term_count,
    lc_pouch_index_exists_term_doc_ids_fn read_exists,
    lc_pouch_index_exact_term_doc_ids_fn read_exact, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
int lc_pouch_index_collect_exists_term_with_eq_and_not_eq_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_exists_term *exists_term,
    const lc_pouch_document_eq_term *eq_terms, size_t eq_term_count,
    const lc_pouch_document_eq_term *not_eq_terms, size_t not_eq_term_count,
    lc_pouch_index_exists_term_doc_ids_fn read_exists,
    lc_pouch_index_exact_term_doc_ids_fn read_exact, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
int lc_pouch_index_collect_range_term_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_range_term *term,
    lc_pouch_index_range_term_doc_ids_fn read_range, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
int lc_pouch_index_collect_range_term_with_eq_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_range_term *range_term,
    const lc_pouch_document_eq_term *eq_terms, size_t eq_term_count,
    lc_pouch_index_range_term_doc_ids_fn read_range,
    lc_pouch_index_exact_term_doc_ids_fn read_exact, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
int lc_pouch_index_collect_range_term_with_eq_and_not_eq_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_range_term *range_term,
    const lc_pouch_document_eq_term *eq_terms, size_t eq_term_count,
    const lc_pouch_document_eq_term *not_eq_terms, size_t not_eq_term_count,
    lc_pouch_index_range_term_doc_ids_fn read_range,
    lc_pouch_index_exact_term_doc_ids_fn read_exact, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
int lc_pouch_index_collect_prefix_term_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_prefix_term *term,
    lc_pouch_index_prefix_term_doc_ids_fn read_prefix, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
int lc_pouch_index_collect_prefix_term_with_eq_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_prefix_term *prefix_term,
    const lc_pouch_document_eq_term *eq_terms, size_t eq_term_count,
    lc_pouch_index_prefix_term_doc_ids_fn read_prefix,
    lc_pouch_index_exact_term_doc_ids_fn read_exact, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
int lc_pouch_index_collect_prefix_term_with_eq_and_not_eq_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_prefix_term *prefix_term,
    const lc_pouch_document_eq_term *eq_terms, size_t eq_term_count,
    const lc_pouch_document_eq_term *not_eq_terms, size_t not_eq_term_count,
    lc_pouch_index_prefix_term_doc_ids_fn read_prefix,
    lc_pouch_index_exact_term_doc_ids_fn read_exact, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
int lc_pouch_index_collect_contains_term_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_contains_term *term,
    lc_pouch_index_contains_term_doc_ids_fn read_contains, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
int lc_pouch_index_collect_contains_term_with_eq_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_contains_term *contains_term,
    const lc_pouch_document_eq_term *eq_terms, size_t eq_term_count,
    lc_pouch_index_contains_term_doc_ids_fn read_contains,
    lc_pouch_index_exact_term_doc_ids_fn read_exact, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
int lc_pouch_index_collect_contains_term_with_eq_and_not_eq_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_contains_term *contains_term,
    const lc_pouch_document_eq_term *eq_terms, size_t eq_term_count,
    const lc_pouch_document_eq_term *not_eq_terms, size_t not_eq_term_count,
    lc_pouch_index_contains_term_doc_ids_fn read_contains,
    lc_pouch_index_exact_term_doc_ids_fn read_exact, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
int lc_pouch_index_collect_date_after_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_date_after_term *term,
    lc_pouch_index_date_after_doc_ids_fn read_date_after, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);

#endif

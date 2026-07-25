#ifndef LC_POUCH_INDEX_H
#define LC_POUCH_INDEX_H

#include "lc_pouch_store.h"

#include <stddef.h>
#include <stdint.h>

typedef uint32_t lc_pouch_index_doc_id;

typedef struct lc_pouch_index_doc_id_set {
  lc_pouch_index_doc_id *items;
  size_t count;
  size_t capacity;
} lc_pouch_index_doc_id_set;

typedef enum lc_pouch_index_posting_encoding {
  LC_POUCH_INDEX_POSTING_EMPTY = 0,
  LC_POUCH_INDEX_POSTING_SPARSE = 1,
  LC_POUCH_INDEX_POSTING_DENSE = 2
} lc_pouch_index_posting_encoding;

typedef struct lc_pouch_index_posting {
  lc_pouch_index_posting_encoding encoding;
  size_t count;
  lc_pouch_index_doc_id max_doc_id;
  unsigned char *sparse;
  size_t sparse_len;
  uint64_t *dense;
  size_t dense_word_count;
} lc_pouch_index_posting;

typedef int (*lc_pouch_index_exact_term_doc_ids_fn)(
    void *context, const char *field, const char *value,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
typedef int (*lc_pouch_index_exists_term_doc_ids_fn)(
    void *context, const char *field, lc_pouch_index_doc_id_set *doc_ids,
    lc_error *error);
typedef int (*lc_pouch_index_range_term_doc_ids_fn)(
    void *context, const lc_pouch_document_range_term *term,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);

void lc_pouch_index_doc_id_set_cleanup(const lc_pouch_allocator *allocator,
                                       lc_pouch_index_doc_id_set *set);
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
int lc_pouch_index_posting_intersect(const lc_pouch_allocator *allocator,
                                     const lc_pouch_index_posting *posting,
                                     const lc_pouch_index_doc_id_set *filter,
                                     lc_pouch_index_doc_id_set *dst);
int lc_pouch_index_collect_eq_term_doc_ids(
    const lc_pouch_allocator *allocator, const lc_pouch_document_eq_term *term,
    lc_pouch_index_exact_term_doc_ids_fn read_exact, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
int lc_pouch_index_collect_in_term_doc_ids(
    const lc_pouch_allocator *allocator, const lc_pouch_document_in_term *term,
    lc_pouch_index_exact_term_doc_ids_fn read_exact, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
int lc_pouch_index_collect_exists_term_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_exists_term *term,
    lc_pouch_index_exists_term_doc_ids_fn read_exists, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);
int lc_pouch_index_collect_range_term_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_range_term *term,
    lc_pouch_index_range_term_doc_ids_fn read_range, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error);

#endif

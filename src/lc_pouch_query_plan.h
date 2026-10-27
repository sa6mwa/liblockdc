#ifndef LC_POUCH_QUERY_PLAN_H
#define LC_POUCH_QUERY_PLAN_H

#include "lc_pouch_index.h"
#include <lql/lql.h>

/* Shared normalization and typed-selector inspection, without a client or
 * document query/result collection. Only selector metadata is inspected. */
int lc_pouch_query_index_build_exact_terms(
    const lc_pouch_index_plain_term *terms, size_t term_count,
    lc_pouch_index_term_key **out_terms, size_t *out_count,
    const lc_allocator *allocator, lc_error *error);
int lc_pouch_query_selector_exact_key(lql *runtime,
                                      const lql_selector *selector,
                                      const char *field,
                                      const lc_allocator *allocator,
                                      lc_pouch_index_term_key **out,
                                      size_t *count, lc_error *error);

#endif

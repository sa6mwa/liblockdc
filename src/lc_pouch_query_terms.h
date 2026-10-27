#ifndef LC_POUCH_QUERY_TERMS_H
#define LC_POUCH_QUERY_TERMS_H

#include "lc_pouch_index.h"

#define LC_POUCH_QUERY_NUMBER_UNINDEXED "query_number_unindexed"

/* A borrowed normalized term, in the existing immutable Pouch index spelling.
 * Types s/b/z are exact scalars; n uses Pouch's double-normalized numeric
 * candidate spelling and h is a candidate-only long-string hash. Numeric/hash
 * hits require residual LQL verification. Presence terms use type s in a
 * separate presence projection, including
 * existing array-wildcard and recursive paths.
 * Fields/values are length-safe hex strings; empty strings use the existing
 * '-' representation. No application payload or whole-document list is kept.
 * The callback consumes each term synchronously and may reject it with an SDK
 * error. It must copy any borrowed data it wants to retain. */
typedef int (*lc_pouch_query_term_fn)(void *context,
                                      const lc_pouch_index_term_key *term,
                                      int presence, lc_error *error);

/* Consumes the source from its current position without resetting or owning
 * it. Reuses normal-state field extraction and numeric normalization, with
 * bounded parser/path/scalar scratch and incremental long-string hashing.
 * A valid number outside the normal index representation reports
 * LC_ERR_UNSUPPORTED / LC_POUCH_QUERY_NUMBER_UNINDEXED only after the complete
 * JSON and source have been validated. A builder can then discard provisional
 * terms and keep the entry uncovered, without retrying the same failed build.
 * Callback/source errors and malformed/truncated JSON are reported explicitly.
 * Terms delivered before an error are provisional: only LC_OK proves complete
 * extraction for that entry, so partial terms cannot establish a nonmatch.
 * There is no result-set allocation, body buffer, or temporary spool. */
int lc_pouch_query_terms_visit(lc_source *source, const lc_allocator *allocator,
                               lc_pouch_query_term_fn visit, void *context,
                               lc_error *error);

#endif

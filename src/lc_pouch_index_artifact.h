#ifndef LC_POUCH_INDEX_ARTIFACT_H
#define LC_POUCH_INDEX_ARTIFACT_H

#include "lc/lc.h"

/* Shared self-contained crypto footer for derived Pouch index artifacts.
 * max_descriptor_bytes == 0 retains the namespace artifact's existing bound;
 * bounded page readers supply an explicit small ceiling. */
int lc_pouch_query_index_append_crypto_footer_fd(int fd, const char *descriptor,
                                                 lc_error *error);
int lc_pouch_query_index_read_crypto_footer(const lc_allocator *allocator,
                                            const char *path,
                                            size_t max_descriptor_bytes,
                                            char **descriptor,
                                            uint64_t *cipher_length,
                                            lc_error *error);

#endif

#ifndef LC_COLLECTION_JSON_H
#define LC_COLLECTION_JSON_H

#include "lc/lc.h"
#include "lc_intcompat.h"

#define LC_COLLECTION_JSON_CHUNK_BYTES 16384U
#define LC_COLLECTION_JSON_MAX_KEY_BYTES 65536U
#define LC_COLLECTION_JSON_MAX_NUMBER_BYTES 4096U
#define LC_COLLECTION_JSON_MAX_DEPTH 128U

/* Validates and compacts one object using lonejson's public push writer. Bytes
 * flow directly into dst; caller must keep the destination unpublished until
 * success. Borrows both streams, never resets/closes them, and never spools. */
int lc_collection_json_copy(lc_source *src, lc_sink *dst,
                            const lc_allocator *allocator, lc_u64 *written,
                            lc_error *error);

#endif

#ifndef LC_COLLECTION_JSON_H
#define LC_COLLECTION_JSON_H

#include "lc/lc.h"
#include "lc_intcompat.h"

#define LC_COLLECTION_JSON_CHUNK_BYTES 16384U
#define LC_COLLECTION_JSON_MAX_KEY_BYTES 65536U
#define LC_COLLECTION_JSON_MAX_NUMBER_BYTES 4096U
#define LC_COLLECTION_JSON_MAX_DEPTH 128U

typedef struct lc_collection_json_writer lc_collection_json_writer;
/* Owned incremental validator. dst and allocator remain borrowed until close.
 * No whole-value buffer is retained; finish supplies EOF exactly once. */
int lc_collection_json_writer_open(lc_sink *dst, const lc_allocator *allocator,
                                   lc_collection_json_writer **out,
                                   lc_error *error);
int lc_collection_json_writer_write(lc_collection_json_writer *writer,
                                    const void *bytes, size_t count,
                                    lc_error *error);
int lc_collection_json_writer_finish(lc_collection_json_writer *writer,
                                     lc_u64 *written, lc_error *error);
void lc_collection_json_writer_close(lc_collection_json_writer *writer);

/* Validates and compacts one object using lonejson's public push writer. Bytes
 * flow directly into dst; caller must keep the destination unpublished until
 * success. Borrows both streams, never resets/closes them, and never spools. */
int lc_collection_json_copy(lc_source *src, lc_sink *dst,
                            const lc_allocator *allocator, lc_u64 *written,
                            lc_error *error);

#endif

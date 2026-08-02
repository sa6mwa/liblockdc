#include "lc_pouch_query_index.h"

#include "lc_api_internal.h"
#include "lc_intcompat.h"
#include "lc_log.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define LC_POUCH_QUERY_INDEX_FORMAT "pouch-query-index"
#define LC_POUCH_QUERY_INDEX_VERSION 11UL
#define LC_POUCH_QUERY_INDEX_LEAF "query.index"
#define LC_POUCH_QUERY_INDEX_DOC_TABLE_LEAF "query.index.lcpdtg"
#define LC_POUCH_QUERY_INDEX_EXACT_TERM_LEAF "query.index.lcpttg"
#define LC_POUCH_QUERY_INDEX_PRESENCE_TERM_LEAF "query.index.lcppg"
#define LC_POUCH_QUERY_INDEX_RANGE_TERM_LEAF "query.index.lcprg"
#define LC_POUCH_QUERY_INDEX_TEXT_TERM_LEAF "query.index.lcptxg"
#define LC_POUCH_QUERY_INDEX_TRIGRAM_TERM_LEAF "query.index.lcpt3g"
#define LC_POUCH_QUERY_INDEX_TEMPORAL_TERM_LEAF "query.index.lcptdg"
#define LC_POUCH_QUERY_INDEX_DELETE_LEAF "query.index.lcpdel"
#define LC_POUCH_QUERY_INDEX_PACKED_LEAF "query.index.lcpseg"
#define LC_POUCH_QUERY_INDEX_PACKED_MAGIC "LPQISEG1"
#define LC_POUCH_QUERY_INDEX_PACKED_MAGIC_LEN 8U
#define LC_POUCH_QUERY_INDEX_PACKED_VERSION ((uint64_t)1U)
#define LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT 8U
#define LC_POUCH_QUERY_INDEX_PACKED_DOC_TABLE 0U
#define LC_POUCH_QUERY_INDEX_PACKED_EXACT 1U
#define LC_POUCH_QUERY_INDEX_PACKED_PRESENCE 2U
#define LC_POUCH_QUERY_INDEX_PACKED_RANGE 3U
#define LC_POUCH_QUERY_INDEX_PACKED_TEXT 4U
#define LC_POUCH_QUERY_INDEX_PACKED_TRIGRAM 5U
#define LC_POUCH_QUERY_INDEX_PACKED_TEMPORAL 6U
#define LC_POUCH_QUERY_INDEX_PACKED_DELETE 7U
#define LC_POUCH_QUERY_INDEX_MANIFEST_FORMAT "pouch-query-index-manifest"
#define LC_POUCH_QUERY_INDEX_MANIFEST_VERSION 2UL
#define LC_POUCH_QUERY_INDEX_MANIFEST_LEAF "query.manifest"
#define LC_POUCH_QUERY_INDEX_CRYPTO_DESC_SUFFIX ".lcpcrypto"
#define LC_POUCH_QUERY_INDEX_CRYPTO_FOOTER_MAGIC "LPQICRF1"
#define LC_POUCH_QUERY_INDEX_CRYPTO_FOOTER_BYTES 16U
#define LC_POUCH_QUERY_INDEX_ANY_TEXT_FIELD "/..."
#define LC_POUCH_QUERY_INDEX_ANY_TEXT_FIELD_HEX "2f2e2e2e"
#define LC_POUCH_QUERY_INDEX_MAX_CONTAINS_TRIGRAMS 8U
#define LC_POUCH_QUERY_INDEX_MAX_SEGMENTS 8U
#define LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_COUNT 9U
#define LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_HEADER 0U
#define LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_DOC_TABLE 1U
#define LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_EXACT 2U
#define LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_PRESENCE 3U
#define LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_RANGE 4U
#define LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_TEXT 5U
#define LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_TRIGRAM 6U
#define LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_TEMPORAL 7U
#define LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_DELETE 8U
#define LC_POUCH_QUERY_INDEX_HASH_OFFSET 2166136261UL
#define LC_POUCH_QUERY_INDEX_HASH_PRIME 16777619UL
#define LC_POUCH_QUERY_INDEX_HASH_MASK 0xffffffffUL
#define LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES 256U
#define LC_POUCH_QUERY_INDEX_EXACT_HASH_TYPE 'h'
#define LC_POUCH_QUERY_INDEX_TEXT_PREFIX_TYPE 'p'
#define LC_POUCH_QUERY_INDEX_TEXT_TOKEN_TYPE 'w'
#define LC_POUCH_QUERY_INDEX_MIN_EXACT_TOKEN_BYTES 4U
#define LC_POUCH_QUERY_INDEX_MAX_EXACT_TOKEN_BYTES 8U
#define LC_POUCH_QUERY_INDEX_TRIGRAM_SEEN_THRESHOLD 4096U
#define LC_POUCH_QUERY_INDEX_TRIGRAM_SEEN_BYTES 2097152U
#define LC_POUCH_QUERY_INDEX_INLINE_DOCIDS 4U
#define LC_POUCH_INDEX_TERM_GENERATION_MAGIC "LPITGEN1"
#define LC_POUCH_INDEX_TERM_GENERATION_MAGIC_LEN 8U
#define LC_POUCH_INDEX_TERM_GENERATION_VERSION ((uint64_t)1U)

typedef struct lc_pouch_query_index_row {
  char *key;
  char *key_hex;
  char *content_type_hex;
  char *etag_hex;
  lc_pouch_generation version;
  uint64_t bytes;
  int has_query_hidden;
  int query_hidden;
} lc_pouch_query_index_row;

typedef struct lc_pouch_query_index_term {
  char *field_hex;
  char *value_hex;
  char *canonical_value_hex;
  char *long_value;
  char *text_prefix_hex;
  char *temporal_value_hex;
  unsigned long *trigram_keys;
  char *token_values;
  size_t long_value_len;
  size_t trigram_key_count;
  size_t token_value_count;
  char *key_hex;
  char value_type;
  int derived_terms_ready;
  unsigned long doc_id;
  lc_pouch_generation version;
  uint64_t bytes;
  int has_query_hidden;
  int query_hidden;
} lc_pouch_query_index_term;

typedef struct lc_pouch_query_index_presence {
  char *field_hex;
  char *key_hex;
} lc_pouch_query_index_presence;

typedef struct lc_pouch_query_index_summary {
  const lc_allocator *allocator;
  lc_pouch *pouch;
  const char *namespace_name;
  lc_pouch_query_index_row *rows;
  size_t count;
  size_t capacity;
  lc_pouch_query_index_term *terms;
  size_t term_count;
  size_t term_capacity;
  int term_index_complete;
  lc_pouch_query_index_presence *presences;
  size_t presence_count;
  size_t presence_capacity;
  int presence_index_complete;
} lc_pouch_query_index_summary;

typedef struct lc_pouch_query_index_extract_batch {
  lc_pouch_query_index_summary *summary;
  const size_t *row_indices;
  size_t index;
} lc_pouch_query_index_extract_batch;

typedef struct lc_pouch_query_index_incremental_change {
  char *key;
  char *key_hex;
  char *content_type;
  char *etag;
  char *descriptor;
  lc_pouch_generation version;
  uint64_t bytes;
  uint64_t cipher_bytes;
  lc_pouch_unix_seconds updated_at_unix;
  int has_query_hidden;
  int query_hidden;
  int found;
} lc_pouch_query_index_incremental_change;

typedef struct lc_pouch_query_index_incremental_context {
  lc_pouch_query_index_summary *summary;
  lc_pouch_query_index_incremental_change *changes;
  size_t change_count;
  size_t change_capacity;
  const char **read_keys;
  size_t read_key_count;
  size_t read_key_capacity;
  size_t *read_row_indices;
  size_t read_row_index_capacity;
} lc_pouch_query_index_incremental_context;

struct lc_pouch_query_index_pending_entry {
  char *namespace_name;
  char *key;
  char *key_hex;
  char *content_type_hex;
  char *etag_hex;
  lc_pouch_generation version;
  uint64_t bytes;
  unsigned long doc_id;
  int has_query_hidden;
  int query_hidden;
  int has_doc_id;
  int deleted;
  int term_index_complete;
  int presence_index_complete;
  lc_pouch_query_index_term *terms;
  size_t term_count;
  lc_pouch_query_index_presence *presences;
  size_t presence_count;
  struct lc_pouch_query_index_pending_entry *next;
};

typedef struct lc_pouch_query_index_text {
  const lc_allocator *allocator;
  char *bytes;
  size_t length;
  size_t capacity;
} lc_pouch_query_index_text;

static int
lc_pouch_query_index_text_append_cstr(lc_pouch_query_index_text *text,
                                      const char *bytes, unsigned long *hash,
                                      lc_error *error);
static int lc_pouch_query_index_exact_generation_number_text_hex(
    const lc_allocator *allocator, const char *value, size_t value_len,
    char **out, lc_error *error);

typedef struct lc_pouch_query_index_read_result {
  lc_pouch_generation index_seq;
  unsigned long row_count;
  unsigned long row_hash;
  unsigned long term_count;
  unsigned long term_hash;
  unsigned long term_field_count;
  unsigned long term_value_count;
  unsigned long presence_count;
  unsigned long presence_hash;
  int term_index_complete;
  int presence_index_complete;
  int present;
  int valid;
} lc_pouch_query_index_read_result;

typedef struct lc_pouch_query_index_manifest_artifact_signature {
  uint64_t size;
  unsigned long mtime;
  unsigned long mtime_nsec;
  unsigned long ctime;
  unsigned long ctime_nsec;
  unsigned long inode;
  int present;
} lc_pouch_query_index_manifest_artifact_signature;

typedef struct lc_pouch_query_index_manifest_segment {
  char *id;
  lc_pouch_generation base_index_seq;
  lc_pouch_generation index_seq;
  unsigned long row_count;
  unsigned long row_hash;
  unsigned long delete_count;
  unsigned long delete_hash;
  lc_pouch_query_index_manifest_artifact_signature
      artifact_signatures[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_COUNT];
} lc_pouch_query_index_manifest_segment;

typedef struct lc_pouch_query_index_manifest {
  lc_pouch_generation index_seq;
  lc_pouch_query_index_manifest_segment *segments;
  size_t segment_count;
  size_t segment_capacity;
  int present;
  int valid;
} lc_pouch_query_index_manifest;

typedef struct lc_pouch_query_index_generation_cache_term {
  char *field_hex;
  char *value_hex;
  char value_type;
  unsigned long term_id;
  unsigned long ordinal;
} lc_pouch_query_index_generation_cache_term;

typedef struct lc_pouch_query_index_generation_cache_posting {
  unsigned long term_id;
  char kind;
  unsigned long count;
  unsigned long max_doc_id;
  unsigned long payload_length;
  const unsigned char *payload;
} lc_pouch_query_index_generation_cache_posting;

struct lc_pouch_query_index_generation_cache_entry {
  char *path;
  char *bytes;
  size_t length;
  lc_pouch_generation index_seq;
  unsigned long row_count;
  unsigned long row_hash;
  lc_pouch_query_index_generation_cache_term *terms;
  size_t term_count;
  lc_pouch_query_index_generation_cache_posting *postings;
  size_t posting_count;
  struct lc_pouch_query_index_generation_cache_entry *next;
};

struct lc_pouch_query_index_doc_table_cache_entry {
  char *path;
  lc_pouch_generation index_seq;
  unsigned long row_count;
  unsigned long row_hash;
  lc_pouch_index_doc_table table;
  char **decoded_keys;
  size_t decoded_key_count;
  struct lc_pouch_query_index_doc_table_cache_entry *next;
};

typedef struct lc_pouch_query_index_key_hex_set {
  char **items;
  size_t count;
  size_t capacity;
} lc_pouch_query_index_key_hex_set;

typedef enum lc_pouch_query_index_segmented_collect_kind {
  LC_POUCH_QUERY_INDEX_SEGMENTED_ALL = 0,
  LC_POUCH_QUERY_INDEX_SEGMENTED_EXACT = 1,
  LC_POUCH_QUERY_INDEX_SEGMENTED_PRESENCE = 2,
  LC_POUCH_QUERY_INDEX_SEGMENTED_RANGE = 3,
  LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT = 4,
  LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT_TOKEN = 5,
  LC_POUCH_QUERY_INDEX_SEGMENTED_TRIGRAM = 6,
  LC_POUCH_QUERY_INDEX_SEGMENTED_TEMPORAL = 7
} lc_pouch_query_index_segmented_collect_kind;

typedef struct lc_pouch_query_index_row_reader {
  const lc_allocator *allocator;
  unsigned long row_index;
  lc_pouch_query_index_row_visit_fn visit;
  void *context;
} lc_pouch_query_index_row_reader;

typedef struct lc_pouch_query_index_term_reader {
  const lc_allocator *allocator;
  const char *field_hex;
  const char *value_hex;
  const char *value_text;
  char value_type;
  int value_type_match;
  int prefix_match;
  int contains_match;
  int ignore_case;
  int string_values_only;
  int range_match;
  int date_match;
  int stop;
  lc_pouch_query_index_range_bounds range_bounds;
  lc_pouch_query_index_date_bounds date_bounds;
  lc_pouch_index_parsed_date_bounds parsed_date_bounds;
  char *value_scratch;
  size_t value_scratch_capacity;
  lc_pouch_query_index_key_visit_fn visit;
  void *context;
} lc_pouch_query_index_term_reader;

typedef struct lc_pouch_query_index_presence_reader {
  const lc_allocator *allocator;
  const char *field_hex;
  lc_pouch_query_index_key_visit_fn visit;
  void *context;
} lc_pouch_query_index_presence_reader;

typedef struct lc_pouch_query_index_any_merge_context {
  const lc_allocator *allocator;
  lc_pouch_index_result_row_list *lists;
  size_t list_count;
} lc_pouch_query_index_any_merge_context;

typedef struct lc_pouch_query_index_exact_generation_docids {
  unsigned long term_id;
  lc_pouch_index_docid_set docids;
  unsigned long inline_docids[LC_POUCH_QUERY_INDEX_INLINE_DOCIDS];
  int using_inline_docids;
  int docids_need_sort;
} lc_pouch_query_index_exact_generation_docids;

typedef struct lc_pouch_query_index_exact_generation_accumulator {
  lc_pouch_query_index_exact_generation_docids *items;
  size_t *term_positions;
  size_t count;
  size_t capacity;
  size_t term_position_capacity;
} lc_pouch_query_index_exact_generation_accumulator;

typedef struct lc_pouch_query_index_trigram_value_set {
  unsigned long *items;
  size_t count;
  size_t capacity;
} lc_pouch_query_index_trigram_value_set;

typedef struct lc_pouch_query_index_token_value_set {
  char *items;
  size_t count;
  size_t capacity;
} lc_pouch_query_index_token_value_set;

static int lc_pouch_query_index_term_precompute_derived(
    lc_pouch_query_index_summary *summary, lc_pouch_query_index_term *term,
    const char *value, size_t value_len, lc_error *error);

typedef struct lc_pouch_query_index_trigram_term_cache_entry {
  const char *field_hex;
  unsigned long field_id;
  unsigned long field_hash;
  unsigned long trigram_key;
  unsigned long term_id;
  int used;
} lc_pouch_query_index_trigram_term_cache_entry;

typedef struct lc_pouch_query_index_trigram_term_cache {
  lc_pouch_query_index_trigram_term_cache_entry *items;
  size_t count;
  size_t capacity;
} lc_pouch_query_index_trigram_term_cache;

typedef struct lc_pouch_query_index_field_cache_entry {
  char *field_hex;
  unsigned long field_hash;
  unsigned long field_id;
  int used;
} lc_pouch_query_index_field_cache_entry;

typedef struct lc_pouch_query_index_field_cache {
  lc_pouch_query_index_field_cache_entry *items;
  size_t count;
  size_t capacity;
  unsigned long next_field_id;
} lc_pouch_query_index_field_cache;

typedef struct lc_pouch_query_index_term_cache_entry {
  const char *field_hex;
  const char *value_hex;
  unsigned long hash;
  unsigned long term_id;
  char value_type;
  int used;
} lc_pouch_query_index_term_cache_entry;

typedef struct lc_pouch_query_index_term_cache {
  lc_pouch_query_index_term_cache_entry *items;
  size_t count;
  size_t capacity;
} lc_pouch_query_index_term_cache;

static int lc_pouch_query_index_text_generation_add_term(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_query_index_term_cache *term_cache, const char *field_hex,
    const char *value_hex, char value_type, unsigned long doc_id,
    lc_error *error);
static int lc_pouch_query_index_exact_generation_accumulator_flush(
    const lc_allocator *allocator,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_index_term_generation *generation, lc_error *error);

struct lc_pouch_query_index_pending_segment {
  char *namespace_name;
  char *last_key_hex;
  lc_pouch_index_doc_table doc_table;
  lc_pouch_index_term_generation exact_generation;
  lc_pouch_index_term_generation presence_generation;
  lc_pouch_index_term_generation range_generation;
  lc_pouch_index_term_generation text_generation;
  lc_pouch_index_term_generation trigram_generation;
  lc_pouch_index_term_generation temporal_generation;
  lc_pouch_query_index_exact_generation_accumulator exact_accumulator;
  lc_pouch_query_index_exact_generation_accumulator presence_accumulator;
  lc_pouch_query_index_exact_generation_accumulator range_accumulator;
  lc_pouch_query_index_exact_generation_accumulator text_accumulator;
  lc_pouch_query_index_exact_generation_accumulator trigram_accumulator;
  lc_pouch_query_index_exact_generation_accumulator temporal_accumulator;
  lc_pouch_query_index_term_cache exact_term_cache;
  lc_pouch_query_index_term_cache presence_term_cache;
  lc_pouch_query_index_term_cache range_term_cache;
  lc_pouch_query_index_term_cache text_term_cache;
  lc_pouch_query_index_term_cache temporal_term_cache;
  lc_pouch_query_index_field_cache trigram_field_cache;
  lc_pouch_query_index_trigram_term_cache trigram_term_cache;
  lc_pouch_query_index_text deletes;
  unsigned long row_hash;
  unsigned long term_hash;
  unsigned long presence_hash;
  unsigned long term_count;
  unsigned long presence_count;
  unsigned long delete_count;
  unsigned long delete_hash;
  int term_index_complete;
  int presence_index_complete;
  struct lc_pouch_query_index_pending_segment *next;
};

static void lc_pouch_query_index_pending_segment_cleanup(
    const lc_allocator *allocator,
    lc_pouch_query_index_pending_segment *segment);
static void lc_pouch_query_index_pending_segment_remove_namespace(
    lc_pouch *pouch, const char *namespace_name);
static lc_pouch_query_index_pending_segment *
lc_pouch_query_index_pending_segment_detach_namespace(
    lc_pouch *pouch, const char *namespace_name);
static lc_pouch_query_index_pending_segment *
lc_pouch_query_index_pending_segment_build_entries(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_query_index_pending_entry *entries, size_t count, lc_error *error);
static lc_pouch_query_index_pending_entry *
lc_pouch_query_index_pending_detach_key(lc_pouch *pouch,
                                        const char *namespace_name,
                                        const char *key, size_t *removed_out);
static void lc_pouch_query_index_pending_entries_cleanup(
    const lc_allocator *allocator, lc_pouch_query_index_pending_entry *entries);
static int lc_pouch_query_index_term_compare(const void *left,
                                             const void *right);
static int lc_pouch_query_index_presence_compare(const void *left,
                                                 const void *right);
static int lc_pouch_query_index_pending_segment_note_write(
    lc_pouch *pouch, lc_pouch_query_index_pending_entry *entry,
    lc_pouch_query_index_pending_entry *replaced_entries, lc_error *error);
static int lc_pouch_query_index_pending_segment_note_delete(
    lc_pouch *pouch, lc_pouch_query_index_pending_entry *entry,
    lc_pouch_query_index_pending_entry *replaced_entries, lc_error *error);
static int lc_pouch_query_index_pending_segment_encode(
    lc_pouch_query_index_pending_segment *segment,
    lc_pouch_generation index_seq, lc_pouch_query_index_text *header_artifact,
    unsigned long *row_hash_out, unsigned long *delete_count_out,
    unsigned long *delete_hash_out, char **doc_table_generation_out,
    size_t *doc_table_generation_length_out, char **exact_term_generation_out,
    size_t *exact_term_generation_length_out,
    char **presence_term_generation_out,
    size_t *presence_term_generation_length_out,
    char **range_term_generation_out, size_t *range_term_generation_length_out,
    char **text_term_generation_out, size_t *text_term_generation_length_out,
    char **trigram_term_generation_out,
    size_t *trigram_term_generation_length_out,
    char **temporal_term_generation_out,
    size_t *temporal_term_generation_length_out, lc_error *error);

typedef struct lc_pouch_query_index_file_signature {
  uint64_t size;
  unsigned long mtime;
  unsigned long mtime_nsec;
  unsigned long ctime;
  unsigned long ctime_nsec;
  unsigned long inode;
  int present;
} lc_pouch_query_index_file_signature;

struct lc_pouch_query_index_artifact_cache_entry {
  char *path;
  int kind;
  lc_pouch_generation expected_a;
  unsigned long expected_b;
  unsigned long expected_c;
  lc_pouch_query_index_file_signature signature;
  struct lc_pouch_query_index_artifact_cache_entry *next;
};

struct lc_pouch_query_index_packed_cache_entry {
  char *path;
  char *bytes;
  size_t length;
  lc_pouch_query_index_file_signature signature;
  struct lc_pouch_query_index_packed_cache_entry *next;
};

struct lc_pouch_query_index_manifest_trust_entry {
  char *namespace_name;
  lc_pouch_generation index_seq;
  uint64_t writer_mode_epoch;
  lc_pouch_query_index_manifest manifest;
  int manifest_cached;
  struct lc_pouch_query_index_manifest_trust_entry *next;
};

#define LC_POUCH_QUERY_INDEX_ARTIFACT_HEADER 1
#define LC_POUCH_QUERY_INDEX_ARTIFACT_DOC_TABLE 2
#define LC_POUCH_QUERY_INDEX_ARTIFACT_TERM_GENERATION 3
#define LC_POUCH_QUERY_INDEX_ARTIFACT_DELETE 4
#define LC_POUCH_QUERY_INDEX_ARTIFACT_CACHE_MAX 768U
#define LC_POUCH_QUERY_INDEX_PACKED_CACHE_MAX 128U

static int lc_pouch_query_index_field_hex_is_any_text(const char *field_hex);
static char *
lc_pouch_query_index_exact_hash_value(const lc_allocator *allocator,
                                      const char *value, size_t value_len,
                                      lc_error *error);
static int lc_pouch_query_index_visit_exact_generation(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_index_term_key *exact_terms, size_t exact_term_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error);
static int lc_pouch_query_index_segmented_collect(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_query_index_segmented_collect_kind kind,
    const lc_pouch_index_term_key *exact_terms, size_t exact_term_count,
    const char *field_hex, const char *needle_hex, const char *needle_text,
    int prefix_match, int contains_match, int ignore_case,
    const lc_pouch_query_index_range_bounds *range_bounds,
    const lc_pouch_index_parsed_date_bounds *temporal_bounds,
    lc_pouch_index_result_row_list *rows, lc_pouch_generation *index_seq,
    int *text_complete_out, lc_error *error);
static int
lc_pouch_query_index_result_rows_reserve(const lc_allocator *allocator,
                                         lc_pouch_index_result_row_list *rows,
                                         size_t needed, lc_error *error);
static int lc_pouch_query_index_parse_header_line(const char *line,
                                                  const char *name,
                                                  unsigned long *out);
static int lc_pouch_query_index_parse_header_u64(const char *line,
                                                 const char *name,
                                                 uint64_t *out);
static int lc_pouch_query_index_artifact_signature(
    const char *path, lc_pouch_query_index_file_signature *out);
static int lc_pouch_query_index_artifact_signature_equal(
    const lc_pouch_query_index_file_signature *left,
    const lc_pouch_query_index_file_signature *right);
static int lc_pouch_query_index_artifact_cache_valid(
    lc_pouch *pouch, const char *path, int kind, lc_pouch_generation expected_a,
    unsigned long expected_b, unsigned long expected_c);
static void lc_pouch_query_index_artifact_cache_remember(
    lc_pouch *pouch, const char *path, int kind, lc_pouch_generation expected_a,
    unsigned long expected_b, unsigned long expected_c);
static void lc_pouch_query_index_artifact_cache_remember_signature(
    lc_pouch *pouch, const char *path, int kind, lc_pouch_generation expected_a,
    unsigned long expected_b, unsigned long expected_c,
    const lc_pouch_query_index_file_signature *signature);
static int
lc_pouch_query_index_manifest_trust_valid(const lc_pouch *pouch,
                                          const char *namespace_name,
                                          lc_pouch_generation index_seq);
static void lc_pouch_query_index_manifest_trust_remember(
    lc_pouch *pouch, const char *namespace_name, lc_pouch_generation index_seq,
    const lc_pouch_query_index_manifest *manifest);
static const lc_pouch_query_index_manifest *
lc_pouch_query_index_manifest_trust_snapshot(const lc_pouch *pouch,
                                             const char *namespace_name,
                                             lc_pouch_generation index_seq);
static int lc_pouch_query_index_append_generation_posting(
    const lc_allocator *allocator, char kind, unsigned long count,
    unsigned long max_doc_id, unsigned long payload_length,
    const unsigned char *payload, size_t value_index,
    const lc_pouch_index_result_docid_list *docid_filter_sorted,
    lc_pouch_index_result_docid_list *docids, lc_error *error);
static int lc_pouch_query_index_segmented_paths(
    lc_pouch *pouch, const char *namespace_name, const char *segment_id,
    char **header_path, char **doc_table_path, char **exact_term_path,
    char **presence_term_path, char **range_term_path, char **text_term_path,
    char **trigram_term_path, char **temporal_term_path, char **delete_path,
    lc_error *error);
static void lc_pouch_query_index_manifest_segment_capture_artifacts(
    lc_pouch_query_index_manifest_segment *segment, const char *header_path,
    const char *doc_table_path, const char *exact_term_path,
    const char *presence_term_path, const char *range_term_path,
    const char *text_term_path, const char *trigram_term_path,
    const char *temporal_term_path, const char *delete_path,
    const char *packed_path);
static int lc_pouch_query_index_manifest_segment_artifacts_match(
    const lc_pouch_query_index_manifest_segment *segment,
    const char *header_path, const char *doc_table_path,
    const char *exact_term_path, const char *presence_term_path,
    const char *range_term_path, const char *text_term_path,
    const char *trigram_term_path, const char *temporal_term_path,
    const char *delete_path, const char *packed_path,
    lc_pouch_query_index_file_signature *actual_signatures);
static void lc_pouch_query_index_segmented_paths_cleanup(
    lc_pouch *pouch, char **header_path, char **doc_table_path,
    char **exact_term_path, char **presence_term_path, char **range_term_path,
    char **text_term_path, char **trigram_term_path, char **temporal_term_path,
    char **delete_path);

typedef struct lc_pouch_query_index_source_reader {
  lc_source *source;
  lc_error error;
} lc_pouch_query_index_source_reader;

static int lc_pouch_query_index_skip_lines(FILE *fp, unsigned long count,
                                           const lc_allocator *allocator,
                                           int *valid, lc_error *error);

typedef struct lc_pouch_query_index_extract_context {
  lc_pouch_query_index_summary *summary;
  const char *key_hex;
  lc_pouch_generation version;
  uint64_t bytes;
  int has_query_hidden;
  int query_hidden;
  char *field;
  size_t field_len;
  size_t field_capacity;
  char *value;
  size_t value_len;
  size_t value_capacity;
} lc_pouch_query_index_extract_context;

static unsigned long lc_pouch_query_index_hash_init(void) {
  return LC_POUCH_QUERY_INDEX_HASH_OFFSET;
}

static int lc_pouch_query_index_hex_value(unsigned char value);

static void lc_pouch_query_index_hash_byte(unsigned long *hash,
                                           unsigned char byte) {
  *hash ^= (unsigned long)byte;
  *hash *= LC_POUCH_QUERY_INDEX_HASH_PRIME;
  *hash &= LC_POUCH_QUERY_INDEX_HASH_MASK;
}

static void lc_pouch_query_index_hash_bytes(unsigned long *hash,
                                            const char *bytes, size_t length) {
  size_t index;

  for (index = 0U; index < length; ++index) {
    lc_pouch_query_index_hash_byte(hash, (unsigned char)bytes[index]);
  }
}

static char *
lc_pouch_query_index_hex_encode_bytes(const lc_allocator *allocator,
                                      const char *value, size_t length) {
  static const char hex[] = "0123456789abcdef";
  const unsigned char *src;
  char *encoded;
  char *dst;

  if (value == NULL || length == 0U) {
    encoded = (char *)lc_alloc_with_allocator(allocator, 2U);
    if (encoded != NULL) {
      encoded[0] = '-';
      encoded[1] = '\0';
    }
    return encoded;
  }
  if (length > ((size_t)-1 - 1U) / 2U) {
    return NULL;
  }
  encoded = (char *)lc_alloc_with_allocator(allocator, (length * 2U) + 1U);
  if (encoded == NULL) {
    return NULL;
  }
  src = (const unsigned char *)value;
  dst = encoded;
  while (length-- > 0U) {
    *dst++ = hex[*src >> 4];
    *dst++ = hex[*src & 0x0fU];
    ++src;
  }
  *dst = '\0';
  return encoded;
}

static void lc_pouch_query_index_hex_encode_byte(char *out,
                                                 unsigned char value) {
  static const char hex[] = "0123456789abcdef";

  out[0] = hex[value >> 4];
  out[1] = hex[value & 0x0fU];
}

static char *lc_pouch_query_index_hex_encode_folded_ascii_bytes(
    const lc_allocator *allocator, const char *value, size_t length) {
  char *encoded;
  size_t index;

  if (value == NULL || length == 0U) {
    return lc_pouch_query_index_hex_encode_bytes(allocator, value, length);
  }
  if (length > ((size_t)-1 - 1U) / 2U) {
    return NULL;
  }
  encoded = (char *)lc_alloc_with_allocator(allocator, (length * 2U) + 1U);
  if (encoded == NULL) {
    return NULL;
  }
  for (index = 0U; index < length; ++index) {
    unsigned char byte;

    byte = (unsigned char)value[index];
    if (byte >= 'A' && byte <= 'Z') {
      byte = (unsigned char)(byte - 'A' + 'a');
    }
    lc_pouch_query_index_hex_encode_byte(encoded + (index * 2U), byte);
  }
  encoded[length * 2U] = '\0';
  return encoded;
}

static char *lc_pouch_query_index_hex_encode(const lc_allocator *allocator,
                                             const char *value) {
  return lc_pouch_query_index_hex_encode_bytes(
      allocator, value, value != NULL ? strlen(value) : 0U);
}

static void
lc_pouch_query_index_summary_cleanup(lc_pouch_query_index_summary *summary) {
  size_t index;

  if (summary == NULL) {
    return;
  }
  for (index = 0U; index < summary->count; ++index) {
    lc_free_with_allocator(summary->allocator, summary->rows[index].key);
    lc_free_with_allocator(summary->allocator, summary->rows[index].key_hex);
    lc_free_with_allocator(summary->allocator,
                           summary->rows[index].content_type_hex);
    lc_free_with_allocator(summary->allocator, summary->rows[index].etag_hex);
  }
  for (index = 0U; index < summary->term_count; ++index) {
    lc_free_with_allocator(summary->allocator, summary->terms[index].field_hex);
    lc_free_with_allocator(summary->allocator, summary->terms[index].value_hex);
    lc_free_with_allocator(summary->allocator,
                           summary->terms[index].canonical_value_hex);
    lc_free_with_allocator(summary->allocator,
                           summary->terms[index].long_value);
    lc_free_with_allocator(summary->allocator,
                           summary->terms[index].text_prefix_hex);
    lc_free_with_allocator(summary->allocator,
                           summary->terms[index].temporal_value_hex);
    lc_free_with_allocator(summary->allocator,
                           summary->terms[index].trigram_keys);
    lc_free_with_allocator(summary->allocator,
                           summary->terms[index].token_values);
    lc_free_with_allocator(summary->allocator, summary->terms[index].key_hex);
  }
  for (index = 0U; index < summary->presence_count; ++index) {
    lc_free_with_allocator(summary->allocator,
                           summary->presences[index].field_hex);
    lc_free_with_allocator(summary->allocator,
                           summary->presences[index].key_hex);
  }
  lc_free_with_allocator(summary->allocator, summary->rows);
  lc_free_with_allocator(summary->allocator, summary->terms);
  lc_free_with_allocator(summary->allocator, summary->presences);
  memset(summary, 0, sizeof(*summary));
}

static int
lc_pouch_query_index_summary_reserve(lc_pouch_query_index_summary *summary,
                                     size_t needed, lc_error *error) {
  lc_pouch_query_index_row *next_rows;
  size_t next_capacity;

  if (needed <= summary->capacity) {
    return LC_OK;
  }
  next_capacity = summary->capacity == 0U ? 16U : summary->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch query-index summary exceeds local limit", NULL,
                          NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_rows = (lc_pouch_query_index_row *)lc_alloc_with_allocator(
      summary->allocator, next_capacity * sizeof(*next_rows));
  if (next_rows == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index summary rows",
                        NULL, NULL, NULL);
  }
  if (summary->rows != NULL) {
    memcpy(next_rows, summary->rows, summary->count * sizeof(*next_rows));
    lc_free_with_allocator(summary->allocator, summary->rows);
  }
  memset(next_rows + summary->count, 0,
         (next_capacity - summary->count) * sizeof(*next_rows));
  summary->rows = next_rows;
  summary->capacity = next_capacity;
  return LC_OK;
}

static int
lc_pouch_query_index_term_reserve(lc_pouch_query_index_summary *summary,
                                  size_t needed, lc_error *error) {
  lc_pouch_query_index_term *next_terms;
  size_t next_capacity;

  if (needed <= summary->term_capacity) {
    return LC_OK;
  }
  next_capacity = summary->term_capacity == 0U ? 32U : summary->term_capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch query-index terms exceed local limit", NULL,
                          NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_terms = (lc_pouch_query_index_term *)lc_alloc_with_allocator(
      summary->allocator, next_capacity * sizeof(*next_terms));
  if (next_terms == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index terms", NULL,
                        NULL, NULL);
  }
  if (summary->terms != NULL) {
    memcpy(next_terms, summary->terms,
           summary->term_count * sizeof(*next_terms));
    lc_free_with_allocator(summary->allocator, summary->terms);
  }
  memset(next_terms + summary->term_count, 0,
         (next_capacity - summary->term_count) * sizeof(*next_terms));
  summary->terms = next_terms;
  summary->term_capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_query_index_term_add(lc_pouch_query_index_summary *summary,
                                         const char *field, size_t field_len,
                                         const char *value, size_t value_len,
                                         const char *key_hex, char value_type,
                                         lc_pouch_generation version,
                                         uint64_t bytes, int has_query_hidden,
                                         int query_hidden, lc_error *error) {
  lc_pouch_query_index_term *term;
  int long_string;
  int rc;

  if (field == NULL || field_len == 0U || value == NULL || key_hex == NULL) {
    return LC_OK;
  }
  long_string = value_type == 's' &&
                value_len > LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES;
  rc = lc_pouch_query_index_term_reserve(summary, summary->term_count + 1U,
                                         error);
  if (rc != LC_OK) {
    return rc;
  }
  term = &summary->terms[summary->term_count];
  memset(term, 0, sizeof(*term));
  term->field_hex = lc_pouch_query_index_hex_encode_bytes(summary->allocator,
                                                          field, field_len);
  term->value_hex =
      long_string ? lc_pouch_query_index_exact_hash_value(
                        summary->allocator, value, value_len, error)
                  : lc_pouch_query_index_hex_encode_bytes(summary->allocator,
                                                          value, value_len);
  if (value_type == 'n') {
    rc = lc_pouch_query_index_exact_generation_number_text_hex(
        summary->allocator, value, value_len, &term->canonical_value_hex,
        error);
    if (rc != LC_OK) {
      lc_free_with_allocator(summary->allocator, term->field_hex);
      lc_free_with_allocator(summary->allocator, term->value_hex);
      memset(term, 0, sizeof(*term));
      return rc;
    }
  }
  term->key_hex = lc_strdup_with_allocator(summary->allocator, key_hex);
  if (long_string) {
    term->long_value =
        (char *)lc_alloc_with_allocator(summary->allocator, value_len + 1U);
    if (term->long_value != NULL) {
      memcpy(term->long_value, value, value_len);
      term->long_value[value_len] = '\0';
      term->long_value_len = value_len;
    }
  }
  if (term->field_hex == NULL || term->value_hex == NULL ||
      term->key_hex == NULL || (long_string && term->long_value == NULL)) {
    lc_free_with_allocator(summary->allocator, term->field_hex);
    lc_free_with_allocator(summary->allocator, term->value_hex);
    lc_free_with_allocator(summary->allocator, term->canonical_value_hex);
    lc_free_with_allocator(summary->allocator, term->long_value);
    lc_free_with_allocator(summary->allocator, term->key_hex);
    memset(term, 0, sizeof(*term));
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index term", NULL, NULL,
                        NULL);
  }
  if (value_type == 's') {
    rc = lc_pouch_query_index_term_precompute_derived(summary, term, value,
                                                      value_len, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(summary->allocator, term->field_hex);
      lc_free_with_allocator(summary->allocator, term->value_hex);
      lc_free_with_allocator(summary->allocator, term->canonical_value_hex);
      lc_free_with_allocator(summary->allocator, term->long_value);
      lc_free_with_allocator(summary->allocator, term->text_prefix_hex);
      lc_free_with_allocator(summary->allocator, term->temporal_value_hex);
      lc_free_with_allocator(summary->allocator, term->trigram_keys);
      lc_free_with_allocator(summary->allocator, term->token_values);
      lc_free_with_allocator(summary->allocator, term->key_hex);
      memset(term, 0, sizeof(*term));
      return rc;
    }
  }
  term->value_type = value_type;
  term->version = version;
  term->bytes = bytes;
  term->has_query_hidden = has_query_hidden;
  term->query_hidden = query_hidden;
  ++summary->term_count;
  return LC_OK;
}

static int
lc_pouch_query_index_presence_reserve(lc_pouch_query_index_summary *summary,
                                      size_t needed, lc_error *error) {
  lc_pouch_query_index_presence *next_presences;
  size_t next_capacity;

  if (needed <= summary->presence_capacity) {
    return LC_OK;
  }
  next_capacity =
      summary->presence_capacity == 0U ? 32U : summary->presence_capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch query-index presences exceed local limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_presences = (lc_pouch_query_index_presence *)lc_alloc_with_allocator(
      summary->allocator, next_capacity * sizeof(*next_presences));
  if (next_presences == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index presences", NULL,
                        NULL, NULL);
  }
  if (summary->presences != NULL) {
    memcpy(next_presences, summary->presences,
           summary->presence_count * sizeof(*next_presences));
    lc_free_with_allocator(summary->allocator, summary->presences);
  }
  memset(next_presences + summary->presence_count, 0,
         (next_capacity - summary->presence_count) * sizeof(*next_presences));
  summary->presences = next_presences;
  summary->presence_capacity = next_capacity;
  return LC_OK;
}

static int
lc_pouch_query_index_presence_add(lc_pouch_query_index_summary *summary,
                                  const char *field, size_t field_len,
                                  const char *key_hex, lc_error *error) {
  lc_pouch_query_index_presence *presence;
  int rc;

  if (field == NULL || field_len == 0U || key_hex == NULL) {
    return LC_OK;
  }
  rc = lc_pouch_query_index_presence_reserve(
      summary, summary->presence_count + 1U, error);
  if (rc != LC_OK) {
    return rc;
  }
  presence = &summary->presences[summary->presence_count];
  memset(presence, 0, sizeof(*presence));
  presence->field_hex = lc_pouch_query_index_hex_encode_bytes(
      summary->allocator, field, field_len);
  presence->key_hex = lc_strdup_with_allocator(summary->allocator, key_hex);
  if (presence->field_hex == NULL || presence->key_hex == NULL) {
    lc_free_with_allocator(summary->allocator, presence->field_hex);
    lc_free_with_allocator(summary->allocator, presence->key_hex);
    memset(presence, 0, sizeof(*presence));
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index presence", NULL,
                        NULL, NULL);
  }
  ++summary->presence_count;
  return LC_OK;
}

static lonejson_read_result
lc_pouch_query_index_lonejson_read(void *user, unsigned char *buffer,
                                   size_t capacity) {
  lc_pouch_query_index_source_reader *reader;
  lonejson_read_result result;

  memset(&result, 0, sizeof(result));
  reader = (lc_pouch_query_index_source_reader *)user;
  if (reader == NULL || reader->source == NULL) {
    result.error_code = EINVAL;
    return result;
  }
  result.bytes_read =
      reader->source->read(reader->source, buffer, capacity, &reader->error);
  if (result.bytes_read == 0U) {
    if (reader->error.code != LC_OK) {
      result.error_code = EIO;
    } else {
      result.eof = 1;
    }
  }
  return result;
}

static int lc_pouch_query_index_extract_reserve(
    lc_pouch_query_index_extract_context *context, int field, size_t extra,
    lc_error *error) {
  char **bytes;
  size_t *length;
  size_t *capacity;
  char *next;
  size_t needed;
  size_t next_capacity;

  bytes = field ? &context->field : &context->value;
  length = field ? &context->field_len : &context->value_len;
  capacity = field ? &context->field_capacity : &context->value_capacity;
  if (extra > (size_t)-1 - *length - 1U) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch query-index extracted term exceeds local limit",
                        NULL, NULL, NULL);
  }
  needed = *length + extra + 1U;
  if (needed <= *capacity) {
    return LC_OK;
  }
  next_capacity = *capacity == 0U ? 64U : *capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch query-index extracted term exceeds local "
                          "limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next = (char *)lc_alloc_with_allocator(context->summary->allocator,
                                         next_capacity);
  if (next == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index extracted term",
                        NULL, NULL, NULL);
  }
  if (*bytes != NULL) {
    memcpy(next, *bytes, *length);
    lc_free_with_allocator(context->summary->allocator, *bytes);
  }
  *bytes = next;
  *capacity = next_capacity;
  (*bytes)[*length] = '\0';
  return LC_OK;
}

static int lc_pouch_query_index_extract_append(
    lc_pouch_query_index_extract_context *context, int field, const char *bytes,
    size_t length, lc_error *error) {
  char **target;
  size_t *target_len;
  int rc;

  rc = lc_pouch_query_index_extract_reserve(context, field, length, error);
  if (rc != LC_OK) {
    return rc;
  }
  target = field ? &context->field : &context->value;
  target_len = field ? &context->field_len : &context->value_len;
  memcpy(*target + *target_len, bytes, length);
  *target_len += length;
  (*target)[*target_len] = '\0';
  return LC_OK;
}

static int lc_pouch_query_index_path_segment_is_array_index(
    const lonejson_path_segment *segment) {
  size_t index;

  if (segment == NULL || segment->len == 0U) {
    return 0;
  }
  for (index = 0U; index < segment->len; ++index) {
    if (segment->data[index] < '0' || segment->data[index] > '9') {
      return 0;
    }
  }
  return 1;
}

static int lc_pouch_query_index_extract_set_field(
    lc_pouch_query_index_extract_context *context,
    const lonejson_value_path *path, lc_error *error) {
  size_t segment_index;

  context->field_len = 0U;
  if (context->field != NULL) {
    context->field[0] = '\0';
  }
  if (path == NULL || path->segment_count == 0U) {
    return LC_OK;
  }
  for (segment_index = 0U; segment_index < path->segment_count;
       ++segment_index) {
    const lonejson_path_segment *segment;
    size_t byte_index;
    int rc;

    segment = &path->segments[segment_index];
    rc = lc_pouch_query_index_extract_append(context, 1, "/", 1U, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (lc_pouch_query_index_path_segment_is_array_index(segment)) {
      rc = lc_pouch_query_index_extract_append(context, 1, "[]", 2U, error);
      if (rc != LC_OK) {
        return rc;
      }
      continue;
    }
    for (byte_index = 0U; byte_index < segment->len; ++byte_index) {
      const char *replacement;
      char byte;

      byte = segment->data[byte_index];
      replacement = NULL;
      if (byte == '~') {
        replacement = "~0";
      } else if (byte == '/') {
        replacement = "~1";
      }
      if (replacement != NULL) {
        rc = lc_pouch_query_index_extract_append(context, 1, replacement, 2U,
                                                 error);
      } else {
        rc = lc_pouch_query_index_extract_append(context, 1, &byte, 1U, error);
      }
      if (rc != LC_OK) {
        return rc;
      }
    }
  }
  return LC_OK;
}

static lonejson_status
lc_pouch_query_index_lonejson_error(lonejson_error *lj_error,
                                    const lc_error *error) {
  if (lj_error != NULL) {
    lonejson_error_init(lj_error);
    lj_error->code = error != NULL && error->code == LC_ERR_NOMEM
                         ? LONEJSON_STATUS_ALLOCATION_FAILED
                         : LONEJSON_STATUS_CALLBACK_FAILED;
    snprintf(lj_error->message, sizeof(lj_error->message), "%s",
             error != NULL && error->message != NULL ? error->message
                                                     : "pouch query-index "
                                                       "callback failed");
  }
  return lj_error != NULL ? lj_error->code : LONEJSON_STATUS_CALLBACK_FAILED;
}

static int lc_pouch_query_index_add_presence_for_current_field(
    lc_pouch_query_index_extract_context *context, lc_error *error) {
  size_t index;
  int rc;

  if (context == NULL || context->field == NULL || context->field_len == 0U) {
    return LC_OK;
  }
  rc = lc_pouch_query_index_presence_add(context->summary, context->field,
                                         context->field_len, context->key_hex,
                                         error);
  if (rc == LC_OK) {
    char *recursive_field;
    size_t recursive_len;

    recursive_len = context->field_len + 3U;
    recursive_field = (char *)lc_alloc_with_allocator(
        context->summary->allocator, recursive_len + 1U);
    if (recursive_field == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch recursive presence field",
                          NULL, NULL, NULL);
    }
    memcpy(recursive_field, context->field, context->field_len);
    memcpy(recursive_field + context->field_len, "/**", 4U);
    rc = lc_pouch_query_index_presence_add(context->summary, recursive_field,
                                           recursive_len, context->key_hex,
                                           error);
    lc_free_with_allocator(context->summary->allocator, recursive_field);
  }
  for (index = 1U; rc == LC_OK && index < context->field_len; ++index) {
    char *recursive_field;
    size_t recursive_len;

    if (context->field[index] != '/') {
      continue;
    }
    recursive_len = index + 3U;
    recursive_field = (char *)lc_alloc_with_allocator(
        context->summary->allocator, recursive_len + 1U);
    if (recursive_field == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch recursive presence field",
                          NULL, NULL, NULL);
    }
    memcpy(recursive_field, context->field, index);
    memcpy(recursive_field + index, "/**", 4U);
    rc = lc_pouch_query_index_presence_add(context->summary, recursive_field,
                                           recursive_len, context->key_hex,
                                           error);
    lc_free_with_allocator(context->summary->allocator, recursive_field);
  }
  return rc;
}

static lonejson_status
lc_pouch_query_index_presence_value(void *user, const lonejson_value_path *path,
                                    lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  int rc;

  context = (lc_pouch_query_index_extract_context *)user;
  lc_error_init(&error);
  rc = lc_pouch_query_index_extract_set_field(context, path, &error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_add_presence_for_current_field(context, &error);
  }
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_query_index_string_begin(void *user, const lonejson_value_path *path,
                                  lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  int rc;

  context = (lc_pouch_query_index_extract_context *)user;
  lc_error_init(&error);
  context->value_len = 0U;
  if (context->value != NULL) {
    context->value[0] = '\0';
  }
  rc = lc_pouch_query_index_extract_set_field(context, path, &error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_add_presence_for_current_field(context, &error);
  }
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_query_index_string_chunk(void *user, const lonejson_value_path *path,
                                  const char *data, size_t len,
                                  lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  int rc;

  (void)path;
  context = (lc_pouch_query_index_extract_context *)user;
  lc_error_init(&error);
  rc = lc_pouch_query_index_extract_append(context, 0, data, len, &error);
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_query_index_string_end(void *user, const lonejson_value_path *path,
                                lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  int rc;

  (void)path;
  context = (lc_pouch_query_index_extract_context *)user;
  lc_error_init(&error);
  rc = lc_pouch_query_index_term_add(
      context->summary, context->field, context->field_len, context->value,
      context->value_len, context->key_hex, 's', context->version,
      context->bytes, context->has_query_hidden, context->query_hidden, &error);
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_query_index_number_begin(void *user, const lonejson_value_path *path,
                                  lonejson_error *lj_error) {
  return lc_pouch_query_index_string_begin(user, path, lj_error);
}

static lonejson_status
lc_pouch_query_index_number_chunk(void *user, const lonejson_value_path *path,
                                  const char *data, size_t len,
                                  lonejson_error *lj_error) {
  return lc_pouch_query_index_string_chunk(user, path, data, len, lj_error);
}

static lonejson_status
lc_pouch_query_index_number_end(void *user, const lonejson_value_path *path,
                                lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  int rc;

  (void)path;
  context = (lc_pouch_query_index_extract_context *)user;
  lc_error_init(&error);
  rc = lc_pouch_query_index_term_add(
      context->summary, context->field, context->field_len, context->value,
      context->value_len, context->key_hex, 'n', context->version,
      context->bytes, context->has_query_hidden, context->query_hidden, &error);
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_query_index_boolean_value(void *user, const lonejson_value_path *path,
                                   int value, lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  const char *text;
  int rc;

  context = (lc_pouch_query_index_extract_context *)user;
  text = value ? "true" : "false";
  lc_error_init(&error);
  rc = lc_pouch_query_index_extract_set_field(context, path, &error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_add_presence_for_current_field(context, &error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_term_add(
        context->summary, context->field, context->field_len, text,
        strlen(text), context->key_hex, 'b', context->version, context->bytes,
        context->has_query_hidden, context->query_hidden, &error);
  }
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static lonejson_status
lc_pouch_query_index_null_value(void *user, const lonejson_value_path *path,
                                lonejson_error *lj_error) {
  lc_pouch_query_index_extract_context *context;
  lc_error error;
  int rc;

  context = (lc_pouch_query_index_extract_context *)user;
  lc_error_init(&error);
  rc = lc_pouch_query_index_extract_set_field(context, path, &error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_add_presence_for_current_field(context, &error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_term_add(
        context->summary, context->field, context->field_len, "null", 4U,
        context->key_hex, 'z', context->version, context->bytes,
        context->has_query_hidden, context->query_hidden, &error);
  }
  if (rc != LC_OK) {
    return lc_pouch_query_index_lonejson_error(lj_error, &error);
  }
  return LONEJSON_STATUS_OK;
}

static void lc_pouch_query_index_extract_context_cleanup(
    lc_pouch_query_index_extract_context *context) {
  if (context == NULL || context->summary == NULL) {
    return;
  }
  lc_free_with_allocator(context->summary->allocator, context->field);
  lc_free_with_allocator(context->summary->allocator, context->value);
  memset(context, 0, sizeof(*context));
}

static int lc_pouch_query_index_extract_terms_from_body(
    lc_pouch_query_index_summary *summary, const char *key,
    const lc_pouch_query_index_row *row, lc_source *body, lc_error *error) {
  lc_pouch_query_index_source_reader reader;
  lc_pouch_query_index_extract_context context;
  lonejson_path_value_visitor visitor;
  lonejson_error lj_error;
  lonejson *runtime;
  lonejson_status status;
  int rc;

  memset(&reader, 0, sizeof(reader));
  memset(&context, 0, sizeof(context));
  if (summary == NULL || key == NULL || row == NULL || body == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index extraction requires summary, key, "
                        "row, and body",
                        NULL, NULL, NULL);
  }
  if (body->reset != NULL) {
    rc = body->reset(body, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  runtime = lc_thread_lonejson_runtime();
  if (runtime == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize pouch query-index JSON runtime",
                        NULL, NULL, NULL);
  }
  reader.source = body;
  lc_error_init(&reader.error);
  context.summary = summary;
  context.key_hex = row->key_hex;
  context.version = row->version;
  context.bytes = row->bytes;
  context.has_query_hidden = row->has_query_hidden;
  context.query_hidden = row->query_hidden;
  visitor = lonejson_default_path_value_visitor();
  visitor.object_begin = lc_pouch_query_index_presence_value;
  visitor.array_begin = lc_pouch_query_index_presence_value;
  visitor.string_begin = lc_pouch_query_index_string_begin;
  visitor.string_chunk = lc_pouch_query_index_string_chunk;
  visitor.string_end = lc_pouch_query_index_string_end;
  visitor.number_begin = lc_pouch_query_index_number_begin;
  visitor.number_chunk = lc_pouch_query_index_number_chunk;
  visitor.number_end = lc_pouch_query_index_number_end;
  visitor.boolean_value = lc_pouch_query_index_boolean_value;
  visitor.null_value = lc_pouch_query_index_null_value;
  lonejson_error_init(&lj_error);
  status = runtime->visit_path_value_reader(
      runtime, lc_pouch_query_index_lonejson_read, &reader, &visitor, &context,
      &lj_error);
  if (reader.error.code != LC_OK) {
    rc = reader.error.code;
    if (error != NULL) {
      *error = reader.error;
      memset(&reader.error, 0, sizeof(reader.error));
    }
  } else if (status == LONEJSON_STATUS_ALLOCATION_FAILED ||
             status == LONEJSON_STATUS_CALLBACK_FAILED) {
    rc = lc_lonejson_error_from_status(
        error, status, &lj_error, "failed to build pouch query-index terms");
  } else {
    if (status != LONEJSON_STATUS_OK) {
      summary->term_index_complete = 0;
      summary->presence_index_complete = 0;
    }
    rc = LC_OK;
  }
  lc_error_cleanup(&reader.error);
  lc_pouch_query_index_extract_context_cleanup(&context);
  return rc;
}

static int lc_pouch_query_index_extract_batch_visit(
    const char *key, const lc_pouch_state_read_result *read_result,
    void *context, lc_error *error) {
  lc_pouch_query_index_extract_batch *batch;
  lc_pouch_query_index_row *row;
  size_t row_index;

  batch = (lc_pouch_query_index_extract_batch *)context;
  if (batch == NULL || batch->summary == NULL || batch->row_indices == NULL ||
      batch->index >= batch->summary->count) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index batch extraction exceeded rows",
                        NULL, NULL, NULL);
  }
  row_index = batch->row_indices[batch->index++];
  if (row_index >= batch->summary->count) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index batch extraction row is invalid",
                        NULL, NULL, NULL);
  }
  row = &batch->summary->rows[row_index];
  if (read_result == NULL || !read_result->found || read_result->body == NULL) {
    return LC_OK;
  }
  return lc_pouch_query_index_extract_terms_from_body(
      batch->summary, key != NULL ? key : row->key, row, read_result->body,
      error);
}

static int
lc_pouch_query_index_summary_visit(const lc_pouch_state_visit_entry *entry,
                                   void *context, lc_error *error) {
  lc_pouch_query_index_summary *summary;
  lc_pouch_query_index_row *row;
  int rc;

  summary = (lc_pouch_query_index_summary *)context;
  if (entry == NULL || entry->key == NULL) {
    return LC_OK;
  }
  if (strncmp(entry->key, ".staging/", sizeof(".staging/") - 1U) == 0 ||
      strstr(entry->key, "/.staging/") != NULL) {
    return LC_OK;
  }
  rc =
      lc_pouch_query_index_summary_reserve(summary, summary->count + 1U, error);
  if (rc != LC_OK) {
    return rc;
  }
  row = &summary->rows[summary->count];
  memset(row, 0, sizeof(*row));
  row->key = lc_strdup_with_allocator(summary->allocator, entry->key);
  row->key_hex =
      lc_pouch_query_index_hex_encode(summary->allocator, entry->key);
  row->content_type_hex =
      lc_pouch_query_index_hex_encode(summary->allocator, entry->content_type);
  row->etag_hex =
      lc_pouch_query_index_hex_encode(summary->allocator, entry->etag);
  if (row->key == NULL || row->key_hex == NULL ||
      row->content_type_hex == NULL || row->etag_hex == NULL) {
    lc_free_with_allocator(summary->allocator, row->key);
    lc_free_with_allocator(summary->allocator, row->key_hex);
    lc_free_with_allocator(summary->allocator, row->content_type_hex);
    lc_free_with_allocator(summary->allocator, row->etag_hex);
    memset(row, 0, sizeof(*row));
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index summary row",
                        NULL, NULL, NULL);
  }
  row->version = entry->version;
  row->bytes = entry->bytes;
  row->has_query_hidden = entry->has_query_hidden;
  row->query_hidden = entry->query_hidden;
  ++summary->count;
  return LC_OK;
}

static void
lc_pouch_query_index_summary_row_cleanup(lc_pouch_query_index_summary *summary,
                                         lc_pouch_query_index_row *row) {
  if (summary == NULL || row == NULL) {
    return;
  }
  lc_free_with_allocator(summary->allocator, row->key);
  lc_free_with_allocator(summary->allocator, row->key_hex);
  lc_free_with_allocator(summary->allocator, row->content_type_hex);
  lc_free_with_allocator(summary->allocator, row->etag_hex);
  memset(row, 0, sizeof(*row));
}

static void
lc_pouch_query_index_summary_term_cleanup(lc_pouch_query_index_summary *summary,
                                          lc_pouch_query_index_term *term) {
  if (summary == NULL || term == NULL) {
    return;
  }
  lc_free_with_allocator(summary->allocator, term->field_hex);
  lc_free_with_allocator(summary->allocator, term->value_hex);
  lc_free_with_allocator(summary->allocator, term->canonical_value_hex);
  lc_free_with_allocator(summary->allocator, term->long_value);
  lc_free_with_allocator(summary->allocator, term->text_prefix_hex);
  lc_free_with_allocator(summary->allocator, term->temporal_value_hex);
  lc_free_with_allocator(summary->allocator, term->trigram_keys);
  lc_free_with_allocator(summary->allocator, term->token_values);
  lc_free_with_allocator(summary->allocator, term->key_hex);
  memset(term, 0, sizeof(*term));
}

static void lc_pouch_query_index_summary_presence_cleanup(
    lc_pouch_query_index_summary *summary,
    lc_pouch_query_index_presence *presence) {
  if (summary == NULL || presence == NULL) {
    return;
  }
  lc_free_with_allocator(summary->allocator, presence->field_hex);
  lc_free_with_allocator(summary->allocator, presence->key_hex);
  memset(presence, 0, sizeof(*presence));
}

static void lc_pouch_query_index_incremental_change_cleanup(
    const lc_allocator *allocator,
    lc_pouch_query_index_incremental_change *change) {
  if (change == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, change->key);
  lc_free_with_allocator(allocator, change->key_hex);
  lc_free_with_allocator(allocator, change->content_type);
  lc_free_with_allocator(allocator, change->etag);
  lc_free_with_allocator(allocator, change->descriptor);
  memset(change, 0, sizeof(*change));
}

static void lc_pouch_query_index_incremental_context_cleanup(
    lc_pouch_query_index_incremental_context *context) {
  const lc_allocator *allocator;
  size_t index;

  if (context == NULL || context->summary == NULL) {
    return;
  }
  allocator = context->summary->allocator;
  for (index = 0U; index < context->change_count; ++index) {
    lc_pouch_query_index_incremental_change_cleanup(allocator,
                                                    &context->changes[index]);
  }
  lc_free_with_allocator(allocator, context->changes);
  lc_free_with_allocator(allocator, context->read_keys);
  lc_free_with_allocator(allocator, context->read_row_indices);
  memset(context, 0, sizeof(*context));
}

static void lc_pouch_query_index_pending_entry_cleanup(
    const lc_allocator *allocator, lc_pouch_query_index_pending_entry *entry) {
  lc_pouch_query_index_summary summary;
  size_t index;

  if (entry == NULL) {
    return;
  }
  memset(&summary, 0, sizeof(summary));
  summary.allocator = allocator;
  lc_free_with_allocator(allocator, entry->namespace_name);
  lc_free_with_allocator(allocator, entry->key);
  lc_free_with_allocator(allocator, entry->key_hex);
  lc_free_with_allocator(allocator, entry->content_type_hex);
  lc_free_with_allocator(allocator, entry->etag_hex);
  for (index = 0U; index < entry->term_count; ++index) {
    lc_pouch_query_index_summary_term_cleanup(&summary, &entry->terms[index]);
  }
  for (index = 0U; index < entry->presence_count; ++index) {
    lc_pouch_query_index_summary_presence_cleanup(&summary,
                                                  &entry->presences[index]);
  }
  lc_free_with_allocator(allocator, entry->terms);
  lc_free_with_allocator(allocator, entry->presences);
  memset(entry, 0, sizeof(*entry));
}

static void lc_pouch_query_index_pending_remove_entry(
    lc_pouch *pouch, lc_pouch_query_index_pending_entry *previous,
    lc_pouch_query_index_pending_entry *entry) {
  if (pouch == NULL || entry == NULL) {
    return;
  }
  if (previous != NULL) {
    previous->next = entry->next;
  } else {
    pouch->query_pending_index = entry->next;
  }
  if (pouch->query_pending_index_count > 0U) {
    --pouch->query_pending_index_count;
  }
  lc_pouch_query_index_pending_entry_cleanup(&pouch->allocator, entry);
  lc_free_with_allocator(&pouch->allocator, entry);
}

static lc_pouch_query_index_pending_entry *
lc_pouch_query_index_pending_detach_key(lc_pouch *pouch,
                                        const char *namespace_name,
                                        const char *key, size_t *removed_out) {
  lc_pouch_query_index_pending_entry *entry;
  lc_pouch_query_index_pending_entry *previous;
  lc_pouch_query_index_pending_entry *detached;
  size_t removed;

  if (removed_out != NULL) {
    *removed_out = 0U;
  }
  if (pouch == NULL || namespace_name == NULL || key == NULL) {
    return NULL;
  }
  previous = NULL;
  entry = pouch->query_pending_index;
  detached = NULL;
  removed = 0U;
  while (entry != NULL) {
    lc_pouch_query_index_pending_entry *next;

    next = entry->next;
    if (entry->namespace_name != NULL && entry->key != NULL &&
        strcmp(entry->namespace_name, namespace_name) == 0 &&
        strcmp(entry->key, key) == 0) {
      if (previous != NULL) {
        previous->next = next;
      } else {
        pouch->query_pending_index = next;
      }
      if (pouch->query_pending_index_count > 0U) {
        --pouch->query_pending_index_count;
      }
      entry->next = detached;
      detached = entry;
      ++removed;
    } else {
      previous = entry;
    }
    entry = next;
  }
  if (removed_out != NULL) {
    *removed_out = removed;
  }
  return detached;
}

static int lc_pouch_query_index_pending_apply(
    lc_pouch *pouch, lc_pouch_query_index_summary *summary,
    const lc_pouch_query_index_row *row, lc_error *error) {
  lc_pouch_query_index_pending_entry *entry;
  lc_pouch_query_index_pending_entry *previous;
  size_t index;
  int rc;

  if (pouch == NULL || summary == NULL || row == NULL || row->key == NULL) {
    return 0;
  }
  previous = NULL;
  entry = pouch->query_pending_index;
  while (entry != NULL) {
    if (entry->namespace_name != NULL && entry->key != NULL &&
        strcmp(entry->namespace_name, summary->namespace_name) == 0 &&
        strcmp(entry->key, row->key) == 0 && entry->version == row->version) {
      rc = lc_pouch_query_index_term_reserve(
          summary, summary->term_count + entry->term_count, error);
      if (rc == LC_OK) {
        rc = lc_pouch_query_index_presence_reserve(
            summary, summary->presence_count + entry->presence_count, error);
      }
      if (rc != LC_OK) {
        if (error != NULL) {
          lc_error_cleanup(error);
          lc_error_init(error);
        }
        return 0;
      }
      if (!entry->term_index_complete) {
        summary->term_index_complete = 0;
      }
      if (!entry->presence_index_complete) {
        summary->presence_index_complete = 0;
      }
      for (index = 0U; index < entry->term_count; ++index) {
        summary->terms[summary->term_count++] = entry->terms[index];
        memset(&entry->terms[index], 0, sizeof(entry->terms[index]));
      }
      for (index = 0U; index < entry->presence_count; ++index) {
        summary->presences[summary->presence_count++] = entry->presences[index];
        memset(&entry->presences[index], 0, sizeof(entry->presences[index]));
      }
      lc_pouch_query_index_pending_segment_remove_namespace(
          pouch, summary->namespace_name);
      lc_pouch_query_index_pending_remove_entry(pouch, previous, entry);
      return 1;
    }
    previous = entry;
    entry = entry->next;
  }
  return 0;
}

static void lc_pouch_query_index_drain_source(lc_source *source) {
  char buffer[4096];
  lc_error error;

  if (source == NULL || source->read == NULL) {
    return;
  }
  lc_error_init(&error);
  while (source->read(source, buffer, sizeof(buffer), &error) > 0U) {
  }
  lc_error_cleanup(&error);
}

static void
lc_pouch_query_index_pending_mark_incomplete(lc_pouch *pouch,
                                             const char *namespace_name) {
  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '.') {
    return;
  }
  pouch->query_pending_index_incomplete = 1;
  lc_pouch_query_index_pending_segment_remove_namespace(pouch, namespace_name);
}

void lc_pouch_query_index_note_state_write(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    const char *content_type, lc_source *body,
    const lc_pouch_state_write_result *result) {
  lc_pouch_query_index_pending_entry *entry;
  lc_pouch_query_index_pending_entry *replaced_entries;
  lc_pouch_query_index_summary summary;
  lc_pouch_query_index_row row;
  lc_error error;
  int query_hidden;
  int rc;

  if (pouch == NULL || namespace_name == NULL) {
    return;
  }
  if (namespace_name[0] == '.') {
    return;
  }
  query_hidden =
      result != NULL && result->has_query_hidden && result->query_hidden;
  if (key == NULL || result == NULL ||
      (!query_hidden && (body == NULL || body->reset == NULL))) {
    lc_pouch_query_index_pending_mark_incomplete(pouch, namespace_name);
    return;
  }
  lc_error_init(&error);
  if (!query_hidden && body->reset(body, &error) != LC_OK) {
    lc_pouch_query_index_pending_mark_incomplete(pouch, namespace_name);
    lc_error_cleanup(&error);
    return;
  }
  memset(&summary, 0, sizeof(summary));
  memset(&row, 0, sizeof(row));
  replaced_entries = NULL;
  summary.allocator = &pouch->allocator;
  summary.pouch = pouch;
  summary.namespace_name = namespace_name;
  summary.term_index_complete = 1;
  summary.presence_index_complete = 1;
  row.key_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, key);
  if (row.key_hex == NULL) {
    lc_pouch_query_index_pending_mark_incomplete(pouch, namespace_name);
    lc_error_cleanup(&error);
    return;
  }
  row.version = result->version;
  row.bytes = result->bytes;
  row.has_query_hidden = result->has_query_hidden;
  row.query_hidden = result->query_hidden;
  if (query_hidden) {
    rc = LC_OK;
  } else {
    rc = lc_pouch_query_index_extract_terms_from_body(&summary, key, &row, body,
                                                      &error);
    lc_pouch_query_index_drain_source(body);
  }
  lc_free_with_allocator(&pouch->allocator, row.key_hex);
  if (rc != LC_OK) {
    lc_pouch_query_index_pending_mark_incomplete(pouch, namespace_name);
    lc_pouch_query_index_summary_cleanup(&summary);
    lc_error_cleanup(&error);
    return;
  }
  entry = (lc_pouch_query_index_pending_entry *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*entry));
  if (entry == NULL) {
    lc_pouch_query_index_pending_mark_incomplete(pouch, namespace_name);
    lc_pouch_query_index_summary_cleanup(&summary);
    lc_error_cleanup(&error);
    return;
  }
  entry->namespace_name =
      lc_strdup_with_allocator(&pouch->allocator, namespace_name);
  entry->key = lc_strdup_with_allocator(&pouch->allocator, key);
  entry->key_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, key);
  entry->content_type_hex = lc_pouch_query_index_hex_encode(
      &pouch->allocator,
      content_type != NULL ? content_type : "application/octet-stream");
  entry->etag_hex =
      lc_pouch_query_index_hex_encode(&pouch->allocator, result->etag);
  if (entry->namespace_name == NULL || entry->key == NULL ||
      entry->key_hex == NULL || entry->content_type_hex == NULL ||
      entry->etag_hex == NULL) {
    lc_pouch_query_index_pending_mark_incomplete(pouch, namespace_name);
    lc_pouch_query_index_pending_entry_cleanup(&pouch->allocator, entry);
    lc_free_with_allocator(&pouch->allocator, entry);
    lc_pouch_query_index_summary_cleanup(&summary);
    lc_error_cleanup(&error);
    return;
  }
  entry->version = result->version;
  entry->bytes = result->bytes;
  entry->has_query_hidden = result->has_query_hidden;
  entry->query_hidden = result->query_hidden;
  entry->term_index_complete = summary.term_index_complete;
  entry->presence_index_complete = summary.presence_index_complete;
  if (summary.term_count > 1U) {
    qsort(summary.terms, summary.term_count, sizeof(summary.terms[0]),
          lc_pouch_query_index_term_compare);
  }
  if (summary.presence_count > 1U) {
    qsort(summary.presences, summary.presence_count,
          sizeof(summary.presences[0]), lc_pouch_query_index_presence_compare);
  }
  entry->terms = summary.terms;
  entry->term_count = summary.term_count;
  entry->presences = summary.presences;
  entry->presence_count = summary.presence_count;
  summary.terms = NULL;
  summary.term_count = 0U;
  summary.term_capacity = 0U;
  summary.presences = NULL;
  summary.presence_count = 0U;
  summary.presence_capacity = 0U;
  replaced_entries =
      lc_pouch_query_index_pending_detach_key(pouch, namespace_name, key, NULL);
  entry->next = pouch->query_pending_index;
  pouch->query_pending_index = entry;
  ++pouch->query_pending_index_count;
  rc = lc_pouch_query_index_pending_segment_note_write(
      pouch, entry, replaced_entries, &error);
  if (rc != LC_OK) {
    lc_pouch_query_index_pending_mark_incomplete(pouch, namespace_name);
  }
  lc_pouch_query_index_pending_entries_cleanup(&pouch->allocator,
                                               replaced_entries);
  lc_pouch_query_index_summary_cleanup(&summary);
  lc_error_cleanup(&error);
}

void lc_pouch_query_index_note_state_delete(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    const lc_pouch_state_write_result *result) {
  lc_pouch_query_index_pending_entry *entry;
  lc_pouch_query_index_pending_entry *replaced_entries;
  lc_error error;
  int rc;

  if (pouch == NULL || namespace_name == NULL) {
    return;
  }
  if (namespace_name[0] == '.') {
    return;
  }
  if (key == NULL || result == NULL) {
    lc_pouch_query_index_pending_mark_incomplete(pouch, namespace_name);
    return;
  }
  lc_error_init(&error);
  replaced_entries = NULL;
  entry = (lc_pouch_query_index_pending_entry *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*entry));
  if (entry == NULL) {
    lc_pouch_query_index_pending_mark_incomplete(pouch, namespace_name);
    lc_error_cleanup(&error);
    return;
  }
  entry->namespace_name =
      lc_strdup_with_allocator(&pouch->allocator, namespace_name);
  entry->key = lc_strdup_with_allocator(&pouch->allocator, key);
  entry->key_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, key);
  if (entry->namespace_name == NULL || entry->key == NULL ||
      entry->key_hex == NULL) {
    lc_pouch_query_index_pending_mark_incomplete(pouch, namespace_name);
    lc_pouch_query_index_pending_entry_cleanup(&pouch->allocator, entry);
    lc_free_with_allocator(&pouch->allocator, entry);
    lc_error_cleanup(&error);
    return;
  }
  entry->version = result->version;
  entry->deleted = 1;
  entry->term_index_complete = 1;
  entry->presence_index_complete = 1;
  replaced_entries =
      lc_pouch_query_index_pending_detach_key(pouch, namespace_name, key, NULL);
  entry->next = pouch->query_pending_index;
  pouch->query_pending_index = entry;
  ++pouch->query_pending_index_count;
  rc = lc_pouch_query_index_pending_segment_note_delete(
      pouch, entry, replaced_entries, &error);
  if (rc != LC_OK) {
    lc_pouch_query_index_pending_mark_incomplete(pouch, namespace_name);
  }
  lc_pouch_query_index_pending_entries_cleanup(&pouch->allocator,
                                               replaced_entries);
  lc_error_cleanup(&error);
}

static lc_pouch_query_index_pending_entry *
lc_pouch_query_index_pending_detach_namespace(lc_pouch *pouch,
                                              const char *namespace_name,
                                              size_t *count_out) {
  lc_pouch_query_index_pending_entry *entry;
  lc_pouch_query_index_pending_entry *previous;
  lc_pouch_query_index_pending_entry *detached;
  size_t count;

  if (count_out != NULL) {
    *count_out = 0U;
  }
  if (pouch == NULL || namespace_name == NULL) {
    return NULL;
  }
  previous = NULL;
  entry = pouch->query_pending_index;
  detached = NULL;
  count = 0U;
  while (entry != NULL) {
    lc_pouch_query_index_pending_entry *next;

    next = entry->next;
    if (entry->namespace_name != NULL &&
        strcmp(entry->namespace_name, namespace_name) == 0) {
      if (previous != NULL) {
        previous->next = next;
      } else {
        pouch->query_pending_index = next;
      }
      if (pouch->query_pending_index_count > 0U) {
        --pouch->query_pending_index_count;
      }
      entry->next = detached;
      detached = entry;
      ++count;
    } else {
      previous = entry;
    }
    entry = next;
  }
  if (count_out != NULL) {
    *count_out = count;
  }
  return detached;
}

int lc_pouch_query_index_has_pending(lc_pouch *pouch,
                                     const char *namespace_name) {
  lc_pouch_query_index_pending_entry *entry;
  lc_pouch_query_index_pending_segment *segment;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0') {
    return 0;
  }
  if (pouch->query_pending_index_incomplete) {
    return 1;
  }
  for (entry = pouch->query_pending_index; entry != NULL; entry = entry->next) {
    if (entry->namespace_name != NULL &&
        strcmp(entry->namespace_name, namespace_name) == 0) {
      return 1;
    }
  }
  for (segment = pouch->query_pending_segments; segment != NULL;
       segment = segment->next) {
    if (segment->namespace_name != NULL &&
        strcmp(segment->namespace_name, namespace_name) == 0) {
      return 1;
    }
  }
  return 0;
}

static void lc_pouch_query_index_pending_entries_cleanup(
    const lc_allocator *allocator,
    lc_pouch_query_index_pending_entry *entries) {
  while (entries != NULL) {
    lc_pouch_query_index_pending_entry *next;

    next = entries->next;
    lc_pouch_query_index_pending_entry_cleanup(allocator, entries);
    lc_free_with_allocator(allocator, entries);
    entries = next;
  }
}

static size_t lc_pouch_query_index_pending_entries_visible_count(
    const lc_pouch_query_index_pending_entry *entries) {
  const lc_pouch_query_index_pending_entry *entry;
  size_t count;

  count = 0U;
  for (entry = entries; entry != NULL; entry = entry->next) {
    if (!entry->deleted) {
      ++count;
    }
  }
  return count;
}

static int lc_pouch_query_index_pending_entries_build_summary(
    const lc_allocator *allocator, lc_pouch_query_index_pending_entry *entries,
    lc_pouch_query_index_summary *summary, lc_pouch_query_index_text *deletes,
    unsigned long *delete_count, unsigned long *delete_hash, lc_error *error) {
  lc_pouch_query_index_pending_entry *entry;
  int rc;

  if (summary == NULL || deletes == NULL || delete_count == NULL ||
      delete_hash == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch pending index build requires outputs", NULL,
                        NULL, NULL);
  }
  *delete_count = 0UL;
  *delete_hash = lc_pouch_query_index_hash_init();
  rc = LC_OK;
  for (entry = entries; rc == LC_OK && entry != NULL; entry = entry->next) {
    if (entry->deleted) {
      rc = lc_pouch_query_index_text_append_cstr(deletes, entry->key_hex, NULL,
                                                 error);
      if (rc == LC_OK) {
        rc = lc_pouch_query_index_text_append_cstr(deletes, "\n", NULL, error);
      }
      if (rc == LC_OK) {
        ++*delete_count;
      }
      continue;
    }
    rc = lc_pouch_query_index_summary_reserve(summary, summary->count + 1U,
                                              error);
    if (rc != LC_OK) {
      break;
    }
    if (entry->term_count > 0U) {
      rc = lc_pouch_query_index_term_reserve(
          summary, summary->term_count + entry->term_count, error);
      if (rc != LC_OK) {
        break;
      }
    }
    if (entry->presence_count > 0U) {
      rc = lc_pouch_query_index_presence_reserve(
          summary, summary->presence_count + entry->presence_count, error);
      if (rc != LC_OK) {
        break;
      }
    }
    summary->rows[summary->count].key = entry->key;
    summary->rows[summary->count].key_hex = entry->key_hex;
    summary->rows[summary->count].content_type_hex = entry->content_type_hex;
    summary->rows[summary->count].etag_hex = entry->etag_hex;
    summary->rows[summary->count].version = entry->version;
    summary->rows[summary->count].bytes = entry->bytes;
    summary->rows[summary->count].has_query_hidden = entry->has_query_hidden;
    summary->rows[summary->count].query_hidden = entry->query_hidden;
    entry->key = NULL;
    entry->key_hex = NULL;
    entry->content_type_hex = NULL;
    entry->etag_hex = NULL;
    ++summary->count;
    if (!entry->term_index_complete) {
      summary->term_index_complete = 0;
    }
    if (!entry->presence_index_complete) {
      summary->presence_index_complete = 0;
    }
    if (entry->term_count > 0U) {
      memcpy(summary->terms + summary->term_count, entry->terms,
             entry->term_count * sizeof(summary->terms[0]));
      summary->term_count += entry->term_count;
      lc_free_with_allocator(allocator, entry->terms);
      entry->terms = NULL;
      entry->term_count = 0U;
    }
    if (entry->presence_count > 0U) {
      memcpy(summary->presences + summary->presence_count, entry->presences,
             entry->presence_count * sizeof(summary->presences[0]));
      summary->presence_count += entry->presence_count;
      lc_free_with_allocator(allocator, entry->presences);
      entry->presences = NULL;
      entry->presence_count = 0U;
    }
  }
  if (rc == LC_OK && deletes->bytes != NULL) {
    lc_pouch_query_index_hash_bytes(delete_hash, deletes->bytes,
                                    deletes->length);
  }
  return rc;
}

static int
lc_pouch_query_index_incremental_change_compare_key(const void *left,
                                                    const void *right) {
  const lc_pouch_query_index_incremental_change *a;
  const lc_pouch_query_index_incremental_change *b;

  a = (const lc_pouch_query_index_incremental_change *)left;
  b = (const lc_pouch_query_index_incremental_change *)right;
  return strcmp(a->key != NULL ? a->key : "", b->key != NULL ? b->key : "");
}

static int
lc_pouch_query_index_incremental_change_compare_key_hex(const void *left,
                                                        const void *right) {
  const lc_pouch_query_index_incremental_change *a;
  const lc_pouch_query_index_incremental_change *b;

  a = (const lc_pouch_query_index_incremental_change *)left;
  b = (const lc_pouch_query_index_incremental_change *)right;
  return strcmp(a->key_hex != NULL ? a->key_hex : "",
                b->key_hex != NULL ? b->key_hex : "");
}

static int lc_pouch_query_index_incremental_change_find_key(
    const lc_pouch_query_index_incremental_change *changes, size_t change_count,
    const char *key) {
  size_t low;
  size_t high;

  if (changes == NULL || key == NULL) {
    return 0;
  }
  low = 0U;
  high = change_count;
  while (low < high) {
    size_t mid;
    int cmp;

    mid = low + ((high - low) / 2U);
    cmp = strcmp(key, changes[mid].key != NULL ? changes[mid].key : "");
    if (cmp == 0) {
      return 1;
    }
    if (cmp < 0) {
      high = mid;
    } else {
      low = mid + 1U;
    }
  }
  return 0;
}

static int lc_pouch_query_index_incremental_change_find_key_hex(
    const lc_pouch_query_index_incremental_change *changes, size_t change_count,
    const char *key_hex) {
  size_t low;
  size_t high;

  if (changes == NULL || key_hex == NULL) {
    return 0;
  }
  low = 0U;
  high = change_count;
  while (low < high) {
    size_t mid;
    int cmp;

    mid = low + ((high - low) / 2U);
    cmp = strcmp(key_hex,
                 changes[mid].key_hex != NULL ? changes[mid].key_hex : "");
    if (cmp == 0) {
      return 1;
    }
    if (cmp < 0) {
      high = mid;
    } else {
      low = mid + 1U;
    }
  }
  return 0;
}

static int lc_pouch_query_index_summary_remove_changes(
    lc_pouch_query_index_summary *summary,
    lc_pouch_query_index_incremental_change *changes, size_t change_count,
    lc_error *error) {
  size_t read_index;
  size_t write_index;
  (void)error;

  if (summary == NULL || changes == NULL || change_count == 0U) {
    return LC_OK;
  }
  qsort(changes, change_count, sizeof(changes[0]),
        lc_pouch_query_index_incremental_change_compare_key);
  write_index = 0U;
  for (read_index = 0U; read_index < summary->count; ++read_index) {
    if (lc_pouch_query_index_incremental_change_find_key(
            changes, change_count, summary->rows[read_index].key)) {
      lc_pouch_query_index_summary_row_cleanup(summary,
                                               &summary->rows[read_index]);
      continue;
    }
    if (write_index != read_index) {
      summary->rows[write_index] = summary->rows[read_index];
      memset(&summary->rows[read_index], 0, sizeof(summary->rows[read_index]));
    }
    ++write_index;
  }
  summary->count = write_index;

  qsort(changes, change_count, sizeof(changes[0]),
        lc_pouch_query_index_incremental_change_compare_key_hex);
  write_index = 0U;
  for (read_index = 0U; read_index < summary->term_count; ++read_index) {
    if (lc_pouch_query_index_incremental_change_find_key_hex(
            changes, change_count, summary->terms[read_index].key_hex)) {
      lc_pouch_query_index_summary_term_cleanup(summary,
                                                &summary->terms[read_index]);
      continue;
    }
    if (write_index != read_index) {
      summary->terms[write_index] = summary->terms[read_index];
      memset(&summary->terms[read_index], 0,
             sizeof(summary->terms[read_index]));
    }
    ++write_index;
  }
  summary->term_count = write_index;

  write_index = 0U;
  for (read_index = 0U; read_index < summary->presence_count; ++read_index) {
    if (lc_pouch_query_index_incremental_change_find_key_hex(
            changes, change_count, summary->presences[read_index].key_hex)) {
      lc_pouch_query_index_summary_presence_cleanup(
          summary, &summary->presences[read_index]);
      continue;
    }
    if (write_index != read_index) {
      summary->presences[write_index] = summary->presences[read_index];
      memset(&summary->presences[read_index], 0,
             sizeof(summary->presences[read_index]));
    }
    ++write_index;
  }
  summary->presence_count = write_index;
  return LC_OK;
}

static int lc_pouch_query_index_incremental_context_add_key(
    lc_pouch_query_index_incremental_context *context, const char *key,
    size_t row_index, lc_error *error) {
  const char **next;
  size_t *next_indices;
  size_t next_capacity;

  if (context == NULL || key == NULL) {
    return LC_OK;
  }
  if (context->read_key_count == context->read_key_capacity) {
    next_capacity = context->read_key_capacity == 0U
                        ? 16U
                        : context->read_key_capacity * 2U;
    next = (const char **)lc_alloc_with_allocator(
        context->summary->allocator, next_capacity * sizeof(*next));
    next_indices = (size_t *)lc_alloc_with_allocator(
        context->summary->allocator, next_capacity * sizeof(*next_indices));
    if (next == NULL || next_indices == NULL) {
      lc_free_with_allocator(context->summary->allocator, next);
      lc_free_with_allocator(context->summary->allocator, next_indices);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch incremental index keys",
                          NULL, NULL, NULL);
    }
    if (context->read_keys != NULL) {
      memcpy(next, context->read_keys, context->read_key_count * sizeof(*next));
      lc_free_with_allocator(context->summary->allocator, context->read_keys);
    }
    if (context->read_row_indices != NULL) {
      memcpy(next_indices, context->read_row_indices,
             context->read_key_count * sizeof(*next_indices));
      lc_free_with_allocator(context->summary->allocator,
                             context->read_row_indices);
    }
    context->read_keys = next;
    context->read_row_indices = next_indices;
    context->read_key_capacity = next_capacity;
    context->read_row_index_capacity = next_capacity;
  }
  context->read_keys[context->read_key_count] = key;
  context->read_row_indices[context->read_key_count] = row_index;
  ++context->read_key_count;
  return LC_OK;
}

static int lc_pouch_query_index_incremental_context_reserve_changes(
    lc_pouch_query_index_incremental_context *context, size_t needed,
    lc_error *error) {
  lc_pouch_query_index_incremental_change *next;
  size_t next_capacity;

  if (context == NULL || context->summary == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch incremental index changes require context", NULL,
                        NULL, NULL);
  }
  if (needed <= context->change_capacity) {
    return LC_OK;
  }
  next_capacity =
      context->change_capacity == 0U ? 16U : context->change_capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch incremental index changes exceed local "
                          "limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next = (lc_pouch_query_index_incremental_change *)lc_alloc_with_allocator(
      context->summary->allocator, next_capacity * sizeof(*next));
  if (next == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch incremental index changes",
                        NULL, NULL, NULL);
  }
  if (context->changes != NULL) {
    memcpy(next, context->changes, context->change_count * sizeof(next[0]));
    lc_free_with_allocator(context->summary->allocator, context->changes);
  }
  memset(next + context->change_count, 0,
         (next_capacity - context->change_count) * sizeof(next[0]));
  context->changes = next;
  context->change_capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_query_index_incremental_context_add_change(
    lc_pouch_query_index_incremental_context *context,
    const lc_pouch_state_change_visit_entry *entry, lc_error *error) {
  lc_pouch_query_index_incremental_change *change;
  int rc;

  if (context == NULL || context->summary == NULL || entry == NULL ||
      entry->key == NULL) {
    return LC_OK;
  }
  rc = lc_pouch_query_index_incremental_context_reserve_changes(
      context, context->change_count + 1U, error);
  if (rc != LC_OK) {
    return rc;
  }
  change = &context->changes[context->change_count];
  memset(change, 0, sizeof(*change));
  change->key =
      lc_strdup_with_allocator(context->summary->allocator, entry->key);
  change->key_hex =
      lc_pouch_query_index_hex_encode(context->summary->allocator, entry->key);
  change->content_type = lc_strdup_with_allocator(context->summary->allocator,
                                                  entry->content_type);
  change->etag =
      lc_strdup_with_allocator(context->summary->allocator, entry->etag);
  change->descriptor =
      lc_strdup_with_allocator(context->summary->allocator, entry->descriptor);
  if (change->key == NULL || change->key_hex == NULL ||
      (entry->content_type != NULL && change->content_type == NULL) ||
      (entry->etag != NULL && change->etag == NULL) ||
      (entry->descriptor != NULL && change->descriptor == NULL)) {
    lc_pouch_query_index_incremental_change_cleanup(context->summary->allocator,
                                                    change);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch incremental index change",
                        NULL, NULL, NULL);
  }
  change->version = entry->version;
  change->bytes = entry->bytes;
  change->cipher_bytes = entry->cipher_bytes;
  change->updated_at_unix = entry->updated_at_unix;
  change->has_query_hidden = entry->has_query_hidden;
  change->query_hidden = entry->query_hidden;
  change->found = entry->found;
  ++context->change_count;
  return LC_OK;
}

static int lc_pouch_query_index_incremental_visit(
    const lc_pouch_state_change_visit_entry *entry, void *context,
    lc_error *error) {
  lc_pouch_query_index_incremental_context *incremental;

  incremental = (lc_pouch_query_index_incremental_context *)context;
  return lc_pouch_query_index_incremental_context_add_change(incremental, entry,
                                                             error);
}

static int lc_pouch_query_index_incremental_apply_changes(
    lc_pouch_query_index_incremental_context *incremental, lc_error *error) {
  lc_pouch_state_visit_entry current;
  size_t change_index;
  int rc;

  if (incremental == NULL || incremental->summary == NULL ||
      incremental->change_count == 0U) {
    return LC_OK;
  }
  rc = lc_pouch_query_index_summary_remove_changes(
      incremental->summary, incremental->changes, incremental->change_count,
      error);
  if (rc != LC_OK) {
    return rc;
  }
  qsort(incremental->changes, incremental->change_count,
        sizeof(incremental->changes[0]),
        lc_pouch_query_index_incremental_change_compare_key);
  for (change_index = 0U; change_index < incremental->change_count;
       ++change_index) {
    const lc_pouch_query_index_incremental_change *change;
    size_t prior_count;

    change = &incremental->changes[change_index];
    if (!change->found) {
      continue;
    }
    memset(&current, 0, sizeof(current));
    current.key = change->key;
    current.content_type = change->content_type;
    current.etag = change->etag;
    current.version = change->version;
    current.bytes = change->bytes;
    current.cipher_bytes = change->cipher_bytes;
    current.descriptor = change->descriptor;
    current.updated_at_unix = change->updated_at_unix;
    current.has_query_hidden = change->has_query_hidden;
    current.query_hidden = change->query_hidden;
    prior_count = incremental->summary->count;
    rc = lc_pouch_query_index_summary_visit(&current, incremental->summary,
                                            error);
    if (rc != LC_OK) {
      return rc;
    }
    if (incremental->summary->count > prior_count) {
      rc = lc_pouch_query_index_incremental_context_add_key(
          incremental, incremental->summary->rows[prior_count].key, prior_count,
          error);
      if (rc != LC_OK) {
        return rc;
      }
    }
  }
  return LC_OK;
}

static int lc_pouch_query_index_row_compare(const void *left,
                                            const void *right) {
  const lc_pouch_query_index_row *a;
  const lc_pouch_query_index_row *b;
  int cmp;

  a = (const lc_pouch_query_index_row *)left;
  b = (const lc_pouch_query_index_row *)right;
  cmp = strcmp(a->key_hex, b->key_hex);
  if (cmp != 0) {
    return cmp;
  }
  if (a->version < b->version) {
    return -1;
  }
  if (a->version > b->version) {
    return 1;
  }
  return 0;
}

static int lc_pouch_query_index_term_compare(const void *left,
                                             const void *right) {
  const lc_pouch_query_index_term *a;
  const lc_pouch_query_index_term *b;
  int cmp;

  a = (const lc_pouch_query_index_term *)left;
  b = (const lc_pouch_query_index_term *)right;
  cmp = strcmp(a->field_hex, b->field_hex);
  if (cmp != 0) {
    return cmp;
  }
  cmp = strcmp(a->value_hex, b->value_hex);
  if (cmp != 0) {
    return cmp;
  }
  if ((a->long_value != NULL) != (b->long_value != NULL)) {
    return a->long_value == NULL ? -1 : 1;
  }
  if (a->long_value != NULL && b->long_value != NULL) {
    size_t min_len;

    min_len = a->long_value_len < b->long_value_len ? a->long_value_len
                                                    : b->long_value_len;
    cmp = memcmp(a->long_value, b->long_value, min_len);
    if (cmp != 0) {
      return cmp;
    }
    if (a->long_value_len != b->long_value_len) {
      return a->long_value_len < b->long_value_len ? -1 : 1;
    }
  }
  if (a->value_type != b->value_type) {
    return a->value_type < b->value_type ? -1 : 1;
  }
  return strcmp(a->key_hex, b->key_hex);
}

static int
lc_pouch_query_index_term_equal(const lc_pouch_query_index_term *left,
                                const lc_pouch_query_index_term *right) {
  return left != NULL && right != NULL &&
         strcmp(left->field_hex, right->field_hex) == 0 &&
         strcmp(left->value_hex, right->value_hex) == 0 &&
         left->value_type == right->value_type &&
         ((left->long_value == NULL && right->long_value == NULL) ||
          (left->long_value != NULL && right->long_value != NULL &&
           left->long_value_len == right->long_value_len &&
           memcmp(left->long_value, right->long_value, left->long_value_len) ==
               0)) &&
         strcmp(left->key_hex, right->key_hex) == 0;
}

static int lc_pouch_query_index_presence_compare(const void *left,
                                                 const void *right) {
  const lc_pouch_query_index_presence *a;
  const lc_pouch_query_index_presence *b;
  int cmp;

  a = (const lc_pouch_query_index_presence *)left;
  b = (const lc_pouch_query_index_presence *)right;
  cmp = strcmp(a->field_hex, b->field_hex);
  if (cmp != 0) {
    return cmp;
  }
  return strcmp(a->key_hex, b->key_hex);
}

static int lc_pouch_query_index_presence_equal(
    const lc_pouch_query_index_presence *left,
    const lc_pouch_query_index_presence *right) {
  return left != NULL && right != NULL &&
         strcmp(left->field_hex, right->field_hex) == 0 &&
         strcmp(left->key_hex, right->key_hex) == 0;
}

static int lc_pouch_query_index_text_reserve(lc_pouch_query_index_text *text,
                                             size_t extra, lc_error *error) {
  char *next_bytes;
  size_t needed;
  size_t next_capacity;

  if (extra > (size_t)-1 - text->length - 1U) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch query-index text exceeds local limit", NULL,
                        NULL, NULL);
  }
  needed = text->length + extra + 1U;
  if (needed <= text->capacity) {
    return LC_OK;
  }
  next_capacity = text->capacity == 0U ? 256U : text->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch query-index text exceeds local limit", NULL,
                          NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_bytes = (char *)lc_alloc_with_allocator(text->allocator, next_capacity);
  if (next_bytes == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index text", NULL, NULL,
                        NULL);
  }
  if (text->bytes != NULL) {
    memcpy(next_bytes, text->bytes, text->length);
    lc_free_with_allocator(text->allocator, text->bytes);
  }
  text->bytes = next_bytes;
  text->capacity = next_capacity;
  text->bytes[text->length] = '\0';
  return LC_OK;
}

static int lc_pouch_query_index_text_append(lc_pouch_query_index_text *text,
                                            const char *bytes, size_t length,
                                            unsigned long *hash,
                                            lc_error *error) {
  int rc;

  if (text != NULL) {
    rc = lc_pouch_query_index_text_reserve(text, length, error);
    if (rc != LC_OK) {
      return rc;
    }
    memcpy(text->bytes + text->length, bytes, length);
    text->length += length;
    text->bytes[text->length] = '\0';
  }
  if (hash != NULL) {
    lc_pouch_query_index_hash_bytes(hash, bytes, length);
  }
  return LC_OK;
}

static int
lc_pouch_query_index_text_append_cstr(lc_pouch_query_index_text *text,
                                      const char *bytes, unsigned long *hash,
                                      lc_error *error) {
  return lc_pouch_query_index_text_append(text, bytes, strlen(bytes), hash,
                                          error);
}

static int lc_pouch_query_index_text_append_ulong_line(
    lc_pouch_query_index_text *text, const char *name, unsigned long value,
    lc_error *error) {
  char line[96];
  int written;

  if (text == NULL || name == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index header line requires text and name",
                        NULL, NULL, NULL);
  }
  written = snprintf(line, sizeof(line), "%s=%lu\n", name, value);
  if (written < 0 || (size_t)written >= sizeof(line)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index header line exceeds local limit",
                        NULL, NULL, NULL);
  }
  return lc_pouch_query_index_text_append(text, line, (size_t)written, NULL,
                                          error);
}

static int
lc_pouch_query_index_text_append_u64_line(lc_pouch_query_index_text *text,
                                          const char *name, uint64_t value,
                                          lc_error *error) {
  char line[96];
  char value_text[32];
  int written;

  if (text == NULL || name == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index header line requires text and name",
                        NULL, NULL, NULL);
  }
  if (lc_u64_format_base10((lc_u64)value, value_text, sizeof(value_text)) < 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index header line exceeds local limit",
                        NULL, NULL, NULL);
  }
  written = snprintf(line, sizeof(line), "%s=%s\n", name, value_text);
  if (written < 0 || (size_t)written >= sizeof(line)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index header line exceeds local limit",
                        NULL, NULL, NULL);
  }
  return lc_pouch_query_index_text_append(text, line, (size_t)written, NULL,
                                          error);
}

static int lc_pouch_query_index_read_line(FILE *fp,
                                          lc_pouch_query_index_text *line,
                                          int *got_line, lc_error *error) {
  int ch;

  if (line == NULL || got_line == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index line read requires outputs", NULL,
                        NULL, NULL);
  }
  line->length = 0U;
  if (line->bytes != NULL) {
    line->bytes[0] = '\0';
  }
  *got_line = 0;
  while ((ch = fgetc(fp)) != EOF) {
    int rc;
    char byte;

    byte = (char)ch;
    rc = lc_pouch_query_index_text_append(line, &byte, 1U, NULL, error);
    if (rc != LC_OK) {
      return rc;
    }
    *got_line = 1;
    if (ch == '\n') {
      return LC_OK;
    }
  }
  if (ferror(fp)) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch query-index row", strerror(errno),
                        NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_query_index_hex_value(unsigned char value) {
  if (value >= '0' && value <= '9') {
    return (int)(value - '0');
  }
  if (value >= 'a' && value <= 'f') {
    return (int)(value - 'a') + 10;
  }
  if (value >= 'A' && value <= 'F') {
    return (int)(value - 'A') + 10;
  }
  return -1;
}

static int lc_pouch_query_index_hex_token_valid(const char *token) {
  size_t index;
  size_t length;

  if (token == NULL || token[0] == '\0') {
    return 0;
  }
  if (strcmp(token, "-") == 0) {
    return 1;
  }
  length = strlen(token);
  if ((length % 2U) != 0U) {
    return 0;
  }
  for (index = 0U; index < length; ++index) {
    if (lc_pouch_query_index_hex_value((unsigned char)token[index]) < 0) {
      return 0;
    }
  }
  return 1;
}

static char *lc_pouch_query_index_hex_decode(const lc_allocator *allocator,
                                             const char *token,
                                             lc_error *error) {
  char *decoded;
  size_t index;
  size_t length;

  if (token == NULL || token[0] == '\0' || strcmp(token, "-") == 0) {
    return lc_strdup_with_allocator(allocator, "");
  }
  length = strlen(token);
  if ((length % 2U) != 0U) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query-index row has invalid hex token", NULL, NULL,
                 NULL);
    return NULL;
  }
  decoded = (char *)lc_alloc_with_allocator(allocator, (length / 2U) + 1U);
  if (decoded == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index decoded key", NULL, NULL,
                 NULL);
    return NULL;
  }
  for (index = 0U; index < length; index += 2U) {
    int high;
    int low;

    high = lc_pouch_query_index_hex_value((unsigned char)token[index]);
    low = lc_pouch_query_index_hex_value((unsigned char)token[index + 1U]);
    if (high < 0 || low < 0) {
      lc_free_with_allocator(allocator, decoded);
      lc_error_set(error, LC_ERR_INVALID, 0L,
                   "pouch query-index row has invalid hex token", NULL, NULL,
                   NULL);
      return NULL;
    }
    decoded[index / 2U] = (char)(((unsigned int)high << 4) | (unsigned int)low);
  }
  decoded[length / 2U] = '\0';
  return decoded;
}

static char *lc_pouch_query_index_hex_decode_scratch(
    lc_pouch_query_index_term_reader *reader, const char *token,
    lc_error *error) {
  char *next;
  size_t index;
  size_t length;
  size_t decoded_length;

  if (reader == NULL || token == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query-index scratch decode requires reader and token",
                 NULL, NULL, NULL);
    return NULL;
  }
  if (token[0] == '\0' || strcmp(token, "-") == 0) {
    decoded_length = 0U;
  } else {
    length = strlen(token);
    if ((length % 2U) != 0U) {
      lc_error_set(error, LC_ERR_INVALID, 0L,
                   "pouch query-index row has invalid hex token", NULL, NULL,
                   NULL);
      return NULL;
    }
    decoded_length = length / 2U;
  }
  if (decoded_length + 1U > reader->value_scratch_capacity) {
    next =
        (char *)lc_alloc_with_allocator(reader->allocator, decoded_length + 1U);
    if (next == NULL) {
      lc_error_set(error, LC_ERR_NOMEM, 0L,
                   "failed to allocate pouch query-index decoded value", NULL,
                   NULL, NULL);
      return NULL;
    }
    lc_free_with_allocator(reader->allocator, reader->value_scratch);
    reader->value_scratch = next;
    reader->value_scratch_capacity = decoded_length + 1U;
  }
  if (decoded_length == 0U) {
    reader->value_scratch[0] = '\0';
    return reader->value_scratch;
  }
  length = decoded_length * 2U;
  for (index = 0U; index < length; index += 2U) {
    int high;
    int low;

    high = lc_pouch_query_index_hex_value((unsigned char)token[index]);
    low = lc_pouch_query_index_hex_value((unsigned char)token[index + 1U]);
    if (high < 0 || low < 0) {
      lc_error_set(error, LC_ERR_INVALID, 0L,
                   "pouch query-index row has invalid hex token", NULL, NULL,
                   NULL);
      return NULL;
    }
    reader->value_scratch[index / 2U] =
        (char)(((unsigned int)high << 4) | (unsigned int)low);
  }
  reader->value_scratch[decoded_length] = '\0';
  return reader->value_scratch;
}

static int lc_pouch_query_index_parse_ulong_token(char **cursor,
                                                  unsigned long *out) {
  char *begin;
  char *end;

  if (cursor == NULL || *cursor == NULL || out == NULL) {
    return 0;
  }
  begin = *cursor;
  if (*begin == '\0' || *begin == '\n' || *begin == ' ') {
    return 0;
  }
  errno = 0;
  *out = strtoul(begin, &end, 10);
  if (errno != 0 || end == begin || *end != ' ') {
    return 0;
  }
  *cursor = end + 1;
  return 1;
}

static int lc_pouch_query_index_parse_u64_token(char **cursor, uint64_t *out) {
  char *begin;
  char *end;
  lc_u64 value;

  if (cursor == NULL || *cursor == NULL || out == NULL) {
    return 0;
  }
  begin = *cursor;
  if (*begin == '\0' || *begin == '\n' || *begin == ' ') {
    return 0;
  }
  end = begin;
  while (*end >= '0' && *end <= '9') {
    ++end;
  }
  if (end == begin || *end != ' ' ||
      !lc_parse_u64_base10_range_checked(begin, (size_t)(end - begin),
                                         &value)) {
    return 0;
  }
  *out = (uint64_t)value;
  *cursor = end + 1;
  return 1;
}

static int lc_pouch_query_index_parse_int_token(char **cursor, int *out) {
  unsigned long parsed;

  if (!lc_pouch_query_index_parse_ulong_token(cursor, &parsed) ||
      parsed > 1UL) {
    return 0;
  }
  *out = parsed != 0UL;
  return 1;
}

static char *lc_pouch_query_index_next_token(char **cursor, int final_token) {
  char *begin;
  char *end;

  if (cursor == NULL || *cursor == NULL) {
    return NULL;
  }
  begin = *cursor;
  if (*begin == '\0' || *begin == '\n' || *begin == ' ') {
    return NULL;
  }
  end = begin;
  while (*end != '\0' && *end != '\n' && *end != ' ') {
    ++end;
  }
  if (final_token) {
    if (*end == ' ') {
      return NULL;
    }
  } else if (*end != ' ') {
    return NULL;
  }
  if (*end != '\0') {
    *end = '\0';
    *cursor = end + 1;
  } else {
    *cursor = end;
  }
  return begin;
}

static int lc_pouch_query_index_parse_and_visit_row(
    const char *line_bytes, lc_pouch_query_index_row_reader *reader,
    lc_error *error) {
  lc_pouch_query_index_row_view row;
  char *line;
  char *cursor;
  char *key_hex;
  char *content_type_hex;
  char *etag_hex;
  char *key;
  size_t line_len;
  int rc;

  if (line_bytes == NULL || reader == NULL || reader->visit == NULL) {
    return LC_OK;
  }
  line_len = strlen(line_bytes);
  line = (char *)lc_alloc_with_allocator(reader->allocator, line_len + 1U);
  if (line == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index row parser", NULL,
                        NULL, NULL);
  }
  memcpy(line, line_bytes, line_len + 1U);
  if (strncmp(line, "row ", sizeof("row ") - 1U) != 0) {
    lc_free_with_allocator(reader->allocator, line);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index row has invalid prefix", NULL, NULL,
                        NULL);
  }
  cursor = line + sizeof("row ") - 1U;
  memset(&row, 0, sizeof(row));
  key = NULL;
  key_hex = NULL;
  content_type_hex = NULL;
  etag_hex = NULL;
  rc = LC_OK;
  if (!lc_pouch_query_index_parse_u64_token(&cursor, &row.version) ||
      !lc_pouch_query_index_parse_u64_token(&cursor, &row.bytes) ||
      !lc_pouch_query_index_parse_int_token(&cursor, &row.has_query_hidden) ||
      !lc_pouch_query_index_parse_int_token(&cursor, &row.query_hidden)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index row has invalid numeric fields", NULL,
                      NULL, NULL);
  }
  if (rc == LC_OK) {
    key_hex = lc_pouch_query_index_next_token(&cursor, 0);
    content_type_hex = lc_pouch_query_index_next_token(&cursor, 0);
    etag_hex = lc_pouch_query_index_next_token(&cursor, 1);
    if (key_hex == NULL || content_type_hex == NULL || etag_hex == NULL ||
        strcmp(key_hex, "-") == 0 ||
        !lc_pouch_query_index_hex_token_valid(key_hex) ||
        !lc_pouch_query_index_hex_token_valid(content_type_hex) ||
        !lc_pouch_query_index_hex_token_valid(etag_hex)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index row has invalid hex fields", NULL,
                        NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    key = lc_pouch_query_index_hex_decode(reader->allocator, key_hex, error);
    if (key == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
  }
  if (rc == LC_OK) {
    row.doc_id = reader->row_index;
    row.key_hex = key_hex;
    row.key = key;
    rc = reader->visit(&row, reader->context, error);
  }
  lc_free_with_allocator(reader->allocator, key);
  lc_free_with_allocator(reader->allocator, line);
  return rc;
}

static int lc_pouch_query_index_parse_number_value(const char *text,
                                                   double *out) {
  char *endptr;
  double value;

  if (text == NULL || text[0] == '\0' || out == NULL) {
    return 0;
  }
  errno = 0;
  value = strtod(text, &endptr);
  if (errno != 0 || endptr == text || *endptr != '\0' || !isfinite(value)) {
    return 0;
  }
  *out = value;
  return 1;
}

static int lc_pouch_query_index_exact_generation_value_hex(
    const lc_allocator *allocator, const char *value_hex, char value_type,
    char **out, lc_error *error) {
  char canonical[64];
  char *value_text;
  double number;
  int written;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index exact generation value requires "
                        "output",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  if (value_hex == NULL || (value_type != 's' && value_type != 'n' &&
                            value_type != 'b' && value_type != 'z')) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index exact generation value requires a "
                        "typed scalar",
                        NULL, NULL, "pouch");
  }
  if (value_type != 'n') {
    *out = lc_strdup_with_allocator(allocator, value_hex);
    if (*out == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch exact generation value",
                          NULL, NULL, NULL);
    }
    return LC_OK;
  }
  value_text = lc_pouch_query_index_hex_decode(allocator, value_hex, error);
  if (value_text == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  if (!lc_pouch_query_index_parse_number_value(value_text, &number)) {
    lc_free_with_allocator(allocator, value_text);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index exact generation number term is "
                        "invalid",
                        NULL, NULL, "pouch");
  }
  lc_free_with_allocator(allocator, value_text);
  if (number == 0.0) {
    written = snprintf(canonical, sizeof(canonical), "0");
  } else {
    written = snprintf(canonical, sizeof(canonical), "%.17g", number);
  }
  if (written < 0 || (size_t)written >= sizeof(canonical)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index exact generation number exceeds "
                        "local limit",
                        NULL, NULL, "pouch");
  }
  *out = lc_pouch_query_index_hex_encode(allocator, canonical);
  if (*out == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch exact numeric generation "
                        "value",
                        NULL, NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_query_index_exact_generation_number_text_hex(
    const lc_allocator *allocator, const char *value, size_t value_len,
    char **out, lc_error *error) {
  char canonical[64];
  char *value_text;
  double number;
  int written;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index numeric generation value requires "
                        "output",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  if (value == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index exact generation number term is "
                        "invalid",
                        NULL, NULL, "pouch");
  }
  value_text = (char *)lc_alloc_with_allocator(allocator, value_len + 1U);
  if (value_text == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch exact numeric generation "
                        "text",
                        NULL, NULL, NULL);
  }
  memcpy(value_text, value, value_len);
  value_text[value_len] = '\0';
  if (!lc_pouch_query_index_parse_number_value(value_text, &number)) {
    lc_free_with_allocator(allocator, value_text);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index exact generation number term is "
                        "invalid",
                        NULL, NULL, "pouch");
  }
  lc_free_with_allocator(allocator, value_text);
  if (number == 0.0) {
    written = snprintf(canonical, sizeof(canonical), "0");
  } else {
    written = snprintf(canonical, sizeof(canonical), "%.17g", number);
  }
  if (written < 0 || (size_t)written >= sizeof(canonical)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index exact generation number exceeds "
                        "local limit",
                        NULL, NULL, "pouch");
  }
  *out = lc_pouch_query_index_hex_encode(allocator, canonical);
  if (*out == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch exact numeric generation "
                        "value",
                        NULL, NULL, NULL);
  }
  return LC_OK;
}

static unsigned long
lc_pouch_query_index_exact_hash_update(unsigned long hash, unsigned char byte) {
  hash ^= (unsigned long)byte;
  return (hash * LC_POUCH_QUERY_INDEX_HASH_PRIME) &
         LC_POUCH_QUERY_INDEX_HASH_MASK;
}

static char *lc_pouch_query_index_exact_hash_format(
    const lc_allocator *allocator, unsigned long length, unsigned long hash,
    unsigned long folded_hash, lc_error *error) {
  char value[25];
  int written;

  written =
      snprintf(value, sizeof(value), "%08lx%08lx%08lx",
               length & LC_POUCH_QUERY_INDEX_HASH_MASK, hash, folded_hash);
  if (written < 0 || (size_t)written >= sizeof(value)) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query-index exact hash exceeds local limit", NULL, NULL,
                 "pouch");
    return NULL;
  }
  return lc_strdup_with_allocator(allocator, value);
}

static char *
lc_pouch_query_index_exact_hash_value(const lc_allocator *allocator,
                                      const char *value, size_t value_len,
                                      lc_error *error) {
  unsigned long hash;
  unsigned long folded_hash;
  size_t index;

  hash = LC_POUCH_QUERY_INDEX_HASH_OFFSET;
  folded_hash = LC_POUCH_QUERY_INDEX_HASH_OFFSET;
  for (index = 0U; index < value_len; ++index) {
    unsigned char byte;
    unsigned char folded;

    byte = (unsigned char)value[index];
    folded =
        byte >= 'A' && byte <= 'Z' ? (unsigned char)(byte - 'A' + 'a') : byte;
    hash = lc_pouch_query_index_exact_hash_update(hash, byte);
    folded_hash = lc_pouch_query_index_exact_hash_update(folded_hash, folded);
  }
  return lc_pouch_query_index_exact_hash_format(
      allocator, (unsigned long)value_len, hash, folded_hash, error);
}

static char *lc_pouch_query_index_exact_hash_value_hex(
    const lc_allocator *allocator, const char *value_hex, lc_error *error) {
  unsigned long hash;
  unsigned long folded_hash;
  size_t value_hex_len;
  size_t index;

  if (value_hex == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query-index exact hash requires value", NULL, NULL,
                 NULL);
    return NULL;
  }
  value_hex_len = strlen(value_hex);
  if ((value_hex_len % 2U) != 0U) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query-index exact hash requires byte-aligned hex", NULL,
                 NULL, "pouch");
    return NULL;
  }
  hash = LC_POUCH_QUERY_INDEX_HASH_OFFSET;
  folded_hash = LC_POUCH_QUERY_INDEX_HASH_OFFSET;
  for (index = 0U; index < value_hex_len; index += 2U) {
    int high;
    int low;
    unsigned char value;
    unsigned char folded;

    high = lc_pouch_query_index_hex_value((unsigned char)value_hex[index]);
    low = lc_pouch_query_index_hex_value((unsigned char)value_hex[index + 1U]);
    if (high < 0 || low < 0) {
      lc_error_set(error, LC_ERR_INVALID, 0L,
                   "pouch query-index exact hash has invalid hex", NULL, NULL,
                   "pouch");
      return NULL;
    }
    value = (unsigned char)(((unsigned int)high << 4U) | (unsigned int)low);
    folded = value >= 'A' && value <= 'Z' ? (unsigned char)(value - 'A' + 'a')
                                          : value;
    hash = lc_pouch_query_index_exact_hash_update(hash, value);
    folded_hash = lc_pouch_query_index_exact_hash_update(folded_hash, folded);
  }
  return lc_pouch_query_index_exact_hash_format(
      allocator, (unsigned long)(value_hex_len / 2U), hash, folded_hash, error);
}

static int
lc_pouch_query_index_exact_string_is_long_hex(const char *value_hex) {
  return value_hex != NULL &&
         strlen(value_hex) >
             (LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES * 2U);
}

int lc_pouch_query_index_scalar_candidates_exact(const char *value,
                                                 char value_type) {
  return value_type != 's' || value == NULL ||
         strlen(value) <= LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES;
}

static int lc_pouch_query_index_rewrite_long_exact_terms(
    const lc_allocator *allocator, lc_pouch_index_term_key *terms,
    size_t *term_count, lc_error *error) {
  size_t index;
  size_t write_index;
  int changed;
  int rc;

  if (terms == NULL || term_count == NULL) {
    return LC_OK;
  }
  changed = 0;
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < *term_count; ++index) {
    if (terms[index].value_type == 's' &&
        lc_pouch_query_index_exact_string_is_long_hex(terms[index].value_hex)) {
      char *hash_value_hex;

      hash_value_hex = lc_pouch_query_index_exact_hash_value_hex(
          allocator, terms[index].value_hex, error);
      if (hash_value_hex == NULL) {
        rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
        break;
      }
      lc_free_with_allocator(allocator, (char *)terms[index].value_hex);
      terms[index].value_hex = hash_value_hex;
      terms[index].value_type = LC_POUCH_QUERY_INDEX_EXACT_HASH_TYPE;
      changed = 1;
    }
  }
  if (rc != LC_OK || !changed || *term_count <= 1U) {
    return rc;
  }
  qsort(terms, *term_count, sizeof(terms[0]), lc_pouch_index_term_key_compare);
  write_index = 0U;
  for (index = 0U; index < *term_count; ++index) {
    if (write_index > 0U && lc_pouch_index_term_key_compare_items(
                                &terms[write_index - 1U], &terms[index]) == 0) {
      lc_free_with_allocator(allocator, (char *)terms[index].field_hex);
      lc_free_with_allocator(allocator, (char *)terms[index].value_hex);
      memset(&terms[index], 0, sizeof(terms[index]));
      continue;
    }
    if (write_index != index) {
      terms[write_index] = terms[index];
      memset(&terms[index], 0, sizeof(terms[index]));
    }
    ++write_index;
  }
  *term_count = write_index;
  return LC_OK;
}

static int lc_pouch_query_index_build_exact_terms(
    const lc_pouch_index_plain_term *terms, size_t term_count,
    lc_pouch_index_term_key **out_terms, size_t *out_count,
    const lc_allocator *allocator, lc_error *error) {
  int rc;

  rc = lc_pouch_index_term_keys_build_exact(terms, term_count, out_terms,
                                            out_count, allocator, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_rewrite_long_exact_terms(allocator, *out_terms,
                                                       out_count, error);
  }
  return rc;
}

static int lc_pouch_query_index_build_exact_terms_for_field(
    const char *field, const char *const *values, const char *value_types,
    size_t value_count, lc_pouch_index_term_key **out_terms, size_t *out_count,
    const lc_allocator *allocator, lc_error *error) {
  int rc;

  rc = lc_pouch_index_term_keys_build_exact_for_field(
      field, values, value_types, value_count, out_terms, out_count, allocator,
      error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_rewrite_long_exact_terms(allocator, *out_terms,
                                                       out_count, error);
  }
  return rc;
}

static int
lc_pouch_query_index_hex_string_may_be_datetime(const char *value_hex) {
  size_t index;

  if (value_hex == NULL) {
    return 0;
  }
  for (index = 0U; index < 20U; ++index) {
    if (value_hex[index] == '\0') {
      return 0;
    }
  }
  return value_hex[0] == '3' && value_hex[2] == '3' && value_hex[4] == '3' &&
         value_hex[6] == '3' && value_hex[1] >= '0' && value_hex[1] <= '9' &&
         value_hex[3] >= '0' && value_hex[3] <= '9' && value_hex[5] >= '0' &&
         value_hex[5] <= '9' && value_hex[7] >= '0' && value_hex[7] <= '9' &&
         value_hex[8] == '2' && (value_hex[9] == 'd' || value_hex[9] == 'D') &&
         value_hex[10] == '3' && value_hex[12] == '3' && value_hex[11] >= '0' &&
         value_hex[11] <= '9' && value_hex[13] >= '0' && value_hex[13] <= '9' &&
         value_hex[14] == '2' &&
         (value_hex[15] == 'd' || value_hex[15] == 'D') &&
         value_hex[16] == '3' && value_hex[18] == '3' && value_hex[17] >= '0' &&
         value_hex[17] <= '9' && value_hex[19] >= '0' && value_hex[19] <= '9';
}

static int lc_pouch_query_index_parse_integer_hex_value(const char *token,
                                                        double *out) {
  double value;
  size_t index;
  size_t length;
  int negative;
  int saw_digit;

  if (token == NULL || token[0] == '\0' || out == NULL) {
    return 0;
  }
  length = strlen(token);
  if ((length % 2U) != 0U) {
    return 0;
  }
  value = 0.0;
  negative = 0;
  saw_digit = 0;
  for (index = 0U; index < length; index += 2U) {
    int high;
    int low;
    unsigned char ch;

    high = lc_pouch_query_index_hex_value((unsigned char)token[index]);
    low = lc_pouch_query_index_hex_value((unsigned char)token[index + 1U]);
    if (high < 0 || low < 0) {
      return 0;
    }
    ch = (unsigned char)(((unsigned int)high << 4) | (unsigned int)low);
    if (index == 0U && ch == '-') {
      negative = 1;
      continue;
    }
    if (ch < '0' || ch > '9') {
      return 0;
    }
    saw_digit = 1;
    value = (value * 10.0) + (double)(ch - '0');
    if (!isfinite(value)) {
      return 0;
    }
  }
  if (!saw_digit) {
    return 0;
  }
  *out = negative ? -value : value;
  return 1;
}

static int lc_pouch_query_index_range_contains_value(
    const lc_pouch_query_index_range_bounds *bounds, double value) {
  if (bounds == NULL || (!bounds->has_gt && !bounds->has_gte &&
                         !bounds->has_lt && !bounds->has_lte)) {
    return 0;
  }
  if (bounds->has_gt && !(value > bounds->gt)) {
    return 0;
  }
  if (bounds->has_gte && !(value >= bounds->gte)) {
    return 0;
  }
  if (bounds->has_lt && !(value < bounds->lt)) {
    return 0;
  }
  if (bounds->has_lte && !(value <= bounds->lte)) {
    return 0;
  }
  return 1;
}

static int lc_pouch_query_index_hex_value(unsigned char value);

static unsigned char lc_pouch_query_index_ascii_lower(unsigned char ch) {
  if (ch >= 'A' && ch <= 'Z') {
    return (unsigned char)(ch - 'A' + 'a');
  }
  return ch;
}

static int lc_pouch_query_index_hex_text_has_prefix(const char *value_hex,
                                                    const char *prefix,
                                                    int ignore_case) {
  size_t index;

  if (value_hex == NULL || prefix == NULL) {
    return 0;
  }
  for (index = 0U; prefix[index] != '\0'; ++index) {
    int high;
    int low;
    unsigned char actual;
    unsigned char expected;

    if (value_hex[index * 2U] == '\0' || value_hex[(index * 2U) + 1U] == '\0') {
      return 0;
    }
    high = lc_pouch_query_index_hex_value((unsigned char)value_hex[index * 2U]);
    low = lc_pouch_query_index_hex_value(
        (unsigned char)value_hex[(index * 2U) + 1U]);
    if (high < 0 || low < 0) {
      return 0;
    }
    actual = (unsigned char)((high << 4) | low);
    if (actual == '\0') {
      return 0;
    }
    expected = (unsigned char)prefix[index];
    if (ignore_case) {
      actual = lc_pouch_query_index_ascii_lower(actual);
      expected = lc_pouch_query_index_ascii_lower(expected);
    }
    if (actual != expected) {
      return 0;
    }
  }
  return 1;
}

static int lc_pouch_query_index_hex_text_contains(const char *value_hex,
                                                  const char *needle,
                                                  int ignore_case) {
  size_t value_index;
  size_t needle_len;
  size_t value_hex_len;
  size_t value_len;

  if (value_hex == NULL || needle == NULL) {
    return 0;
  }
  needle_len = strlen(needle);
  if (needle_len == 0U) {
    return 1;
  }
  value_hex_len = strlen(value_hex);
  value_len = value_hex_len / 2U;
  if (value_hex_len % 2U != 0U || needle_len > value_len) {
    return 0;
  }
  for (value_index = 0U; value_index + needle_len <= value_len; ++value_index) {
    if (lc_pouch_query_index_hex_text_has_prefix(value_hex + (value_index * 2U),
                                                 needle, ignore_case)) {
      return 1;
    }
  }
  return 0;
}

static int lc_pouch_query_index_hex_contains_aligned(const char *value_hex,
                                                     const char *needle_hex) {
  size_t value_len;
  size_t needle_len;
  size_t index;

  if (value_hex == NULL || needle_hex == NULL) {
    return 0;
  }
  needle_len = strlen(needle_hex);
  if (needle_len == 0U) {
    return 1;
  }
  value_len = strlen(value_hex);
  if (value_len % 2U != 0U || needle_len % 2U != 0U || needle_len > value_len) {
    return 0;
  }
  for (index = 0U; index + needle_len <= value_len; index += 2U) {
    if (strncmp(value_hex + index, needle_hex, needle_len) == 0) {
      return 1;
    }
  }
  return 0;
}

static int lc_pouch_query_index_term_reader_matches_value(
    lc_pouch_query_index_term_reader *reader, const char *value_hex,
    int *matched, lc_error *error) {
  char *value_text;
  double number;
  lc_pouch_index_instant instant;

  if (matched == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term match requires output", NULL,
                        NULL, NULL);
  }
  *matched = 0;
  if (reader == NULL || value_hex == NULL) {
    return LC_OK;
  }
  if (reader->prefix_match) {
    if (reader->ignore_case) {
      *matched = lc_pouch_query_index_hex_text_has_prefix(
          value_hex, reader->value_text, 1);
    } else {
      *matched =
          strncmp(value_hex, reader->value_hex, strlen(reader->value_hex)) == 0;
    }
    return LC_OK;
  }
  if (reader->contains_match) {
    if (reader->ignore_case) {
      *matched = lc_pouch_query_index_hex_text_contains(value_hex,
                                                        reader->value_text, 1);
    } else {
      *matched = lc_pouch_query_index_hex_contains_aligned(value_hex,
                                                           reader->value_hex);
    }
    return LC_OK;
  }
  if (reader->range_match) {
    if (lc_pouch_query_index_parse_integer_hex_value(value_hex, &number)) {
      *matched = lc_pouch_query_index_range_contains_value(
          &reader->range_bounds, number);
    } else {
      value_text =
          lc_pouch_query_index_hex_decode_scratch(reader, value_hex, error);
      if (value_text == NULL) {
        return error != NULL && error->code != LC_OK ? error->code
                                                     : LC_ERR_NOMEM;
      }
      if (lc_pouch_query_index_parse_number_value(value_text, &number)) {
        *matched = lc_pouch_query_index_range_contains_value(
            &reader->range_bounds, number);
      }
    }
    return LC_OK;
  }
  if (reader->date_match) {
    value_text =
        lc_pouch_query_index_hex_decode_scratch(reader, value_hex, error);
    if (value_text == NULL) {
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    if (lc_pouch_index_parse_lql_datetime(value_text, &instant)) {
      *matched = lc_pouch_index_date_contains_value(&reader->parsed_date_bounds,
                                                    &instant);
    }
    return LC_OK;
  }
  *matched = strcmp(value_hex, reader->value_hex) == 0;
  return LC_OK;
}

static int lc_pouch_query_index_parse_and_visit_term(
    char *line, lc_pouch_query_index_term_reader *reader, lc_error *error) {
  lc_pouch_query_index_key_view key_view;
  char *cursor;
  char *field_hex;
  char *value_hex;
  char *key_hex;
  char *type_token;
  char *key;
  char value_type;
  int matched;
  int field_cmp;
  int value_cmp;
  int rc;

  if (line == NULL || reader == NULL || reader->visit == NULL) {
    return LC_OK;
  }
  if (strncmp(line, "term ", sizeof("term ") - 1U) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term has invalid prefix", NULL, NULL,
                        NULL);
  }
  cursor = line + sizeof("term ") - 1U;
  key = NULL;
  rc = LC_OK;
  memset(&key_view, 0, sizeof(key_view));
  if (!lc_pouch_query_index_parse_u64_token(&cursor, &key_view.version) ||
      !lc_pouch_query_index_parse_u64_token(&cursor, &key_view.bytes) ||
      !lc_pouch_query_index_parse_int_token(&cursor,
                                            &key_view.has_query_hidden) ||
      !lc_pouch_query_index_parse_int_token(&cursor, &key_view.query_hidden) ||
      !lc_pouch_query_index_parse_ulong_token(&cursor, &key_view.doc_id)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index term has invalid numeric fields", NULL,
                      NULL, NULL);
  }
  field_hex = NULL;
  value_hex = NULL;
  key_hex = NULL;
  type_token = NULL;
  value_type = '\0';
  if (rc == LC_OK) {
    field_hex = lc_pouch_query_index_next_token(&cursor, 0);
    value_hex = lc_pouch_query_index_next_token(&cursor, 0);
    key_hex = lc_pouch_query_index_next_token(&cursor, 0);
    type_token = lc_pouch_query_index_next_token(&cursor, 1);
    if (field_hex == NULL || value_hex == NULL || key_hex == NULL ||
        type_token == NULL || type_token[0] == '\0' || type_token[1] != '\0' ||
        (type_token[0] != 's' && type_token[0] != 'n' && type_token[0] != 'b' &&
         type_token[0] != 'z') ||
        strcmp(field_hex, "-") == 0 || strcmp(key_hex, "-") == 0 ||
        !lc_pouch_query_index_hex_token_valid(field_hex) ||
        !lc_pouch_query_index_hex_token_valid(value_hex) ||
        !lc_pouch_query_index_hex_token_valid(key_hex)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term has invalid hex fields", NULL,
                        NULL, NULL);
    } else {
      value_type = type_token[0];
    }
  }
  matched = 0;
  field_cmp = 0;
  if (rc == LC_OK &&
      lc_pouch_query_index_field_hex_is_any_text(reader->field_hex)) {
    field_cmp = 0;
  } else if (rc == LC_OK &&
             (field_cmp = strcmp(field_hex, reader->field_hex)) > 0) {
    reader->stop = 1;
  } else if (rc == LC_OK && field_cmp == 0) {
    if (reader->value_type_match && value_type != reader->value_type) {
      matched = 0;
    } else if (reader->string_values_only && value_type != 's') {
      matched = 0;
    } else if (!reader->prefix_match && !reader->contains_match &&
               !reader->range_match && !reader->date_match &&
               !reader->ignore_case) {
      value_cmp = strcmp(value_hex, reader->value_hex);
      if (value_cmp > 0) {
        reader->stop = 1;
      } else if (value_cmp == 0) {
        rc = lc_pouch_query_index_term_reader_matches_value(reader, value_hex,
                                                            &matched, error);
      }
    } else {
      rc = lc_pouch_query_index_term_reader_matches_value(reader, value_hex,
                                                          &matched, error);
    }
  }
  if (rc == LC_OK && matched) {
    key_view.key_hex = key_hex;
    key = lc_pouch_query_index_hex_decode(reader->allocator, key_hex, error);
    if (key == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    } else {
      key_view.key = key;
      rc = reader->visit(&key_view, reader->context, error);
    }
  }
  lc_free_with_allocator(reader->allocator, key);
  return rc;
}

static int lc_pouch_query_index_parse_and_visit_presence(
    const char *line_bytes, lc_pouch_query_index_presence_reader *reader,
    lc_error *error) {
  lc_pouch_query_index_key_view key_view;
  char *line;
  char *cursor;
  char *field_hex;
  char *key_hex;
  char *key;
  size_t line_len;
  int rc;

  if (line_bytes == NULL || reader == NULL || reader->visit == NULL) {
    return LC_OK;
  }
  line_len = strlen(line_bytes);
  line = (char *)lc_alloc_with_allocator(reader->allocator, line_len + 1U);
  if (line == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index presence parser",
                        NULL, NULL, NULL);
  }
  memcpy(line, line_bytes, line_len + 1U);
  if (strncmp(line, "present ", sizeof("present ") - 1U) != 0) {
    lc_free_with_allocator(reader->allocator, line);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index presence has invalid prefix", NULL,
                        NULL, NULL);
  }
  cursor = line + sizeof("present ") - 1U;
  key = NULL;
  field_hex = lc_pouch_query_index_next_token(&cursor, 0);
  key_hex = lc_pouch_query_index_next_token(&cursor, 1);
  if (field_hex == NULL || key_hex == NULL || strcmp(field_hex, "-") == 0 ||
      strcmp(key_hex, "-") == 0 ||
      !lc_pouch_query_index_hex_token_valid(field_hex) ||
      !lc_pouch_query_index_hex_token_valid(key_hex)) {
    lc_free_with_allocator(reader->allocator, line);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index presence has invalid hex fields",
                        NULL, NULL, NULL);
  }
  rc = LC_OK;
  if (strcmp(field_hex, reader->field_hex) == 0) {
    key = lc_pouch_query_index_hex_decode(reader->allocator, key_hex, error);
    if (key == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    } else {
      key_view.key = key;
      rc = reader->visit(&key_view, reader->context, error);
    }
  }
  lc_free_with_allocator(reader->allocator, key);
  lc_free_with_allocator(reader->allocator, line);
  return rc;
}

static char *lc_pouch_query_index_path(lc_pouch *pouch,
                                       const char *namespace_name,
                                       lc_error *error) {
  char *namespace_path;
  char *index_path;
  char *header_path;

  namespace_path = lc_pouch_namespace_path(&pouch->allocator, pouch->root_path,
                                           namespace_name);
  if (namespace_path == NULL) {
    return NULL;
  }
  index_path = lc_pouch_path_join(&pouch->allocator, namespace_path, "index");
  lc_free_with_allocator(&pouch->allocator, namespace_path);
  if (index_path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index directory path", NULL,
                 NULL, NULL);
    return NULL;
  }
  header_path = lc_pouch_path_join(&pouch->allocator, index_path,
                                   LC_POUCH_QUERY_INDEX_LEAF);
  lc_free_with_allocator(&pouch->allocator, index_path);
  if (header_path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index header artifact path",
                 NULL, NULL, NULL);
  }
  return header_path;
}

static char *lc_pouch_query_index_manifest_path(lc_pouch *pouch,
                                                const char *namespace_name,
                                                lc_error *error) {
  char *namespace_path;
  char *index_path;
  char *manifest_path;

  namespace_path = lc_pouch_namespace_path(&pouch->allocator, pouch->root_path,
                                           namespace_name);
  if (namespace_path == NULL) {
    return NULL;
  }
  index_path = lc_pouch_path_join(&pouch->allocator, namespace_path, "index");
  lc_free_with_allocator(&pouch->allocator, namespace_path);
  if (index_path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index directory path", NULL,
                 NULL, NULL);
    return NULL;
  }
  manifest_path = lc_pouch_path_join(&pouch->allocator, index_path,
                                     LC_POUCH_QUERY_INDEX_MANIFEST_LEAF);
  lc_free_with_allocator(&pouch->allocator, index_path);
  if (manifest_path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index manifest path", NULL,
                 NULL, NULL);
  }
  return manifest_path;
}

static char *lc_pouch_query_index_segment_artifact_path(
    lc_pouch *pouch, const char *namespace_name, const char *segment_id,
    const char *artifact_leaf, lc_error *error) {
  char *namespace_path;
  char *index_path;
  char *leaf;
  char *path;
  size_t segment_len;
  size_t artifact_len;

  if (segment_id == NULL || segment_id[0] == '\0' || artifact_leaf == NULL ||
      artifact_leaf[0] == '\0') {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query-index segment path requires segment and "
                 "artifact",
                 NULL, NULL, "pouch");
    return NULL;
  }
  segment_len = strlen(segment_id);
  artifact_len = strlen(artifact_leaf);
  if (segment_len > (size_t)-1 - artifact_len - sizeof("query..")) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "pouch query-index segment artifact path exceeds local limit",
                 NULL, NULL, NULL);
    return NULL;
  }
  leaf = (char *)lc_alloc_with_allocator(&pouch->allocator,
                                         sizeof("query.") - 1U + segment_len +
                                             1U + artifact_len + 1U);
  if (leaf == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index segment artifact leaf",
                 NULL, NULL, NULL);
    return NULL;
  }
  memcpy(leaf, "query.", sizeof("query.") - 1U);
  memcpy(leaf + sizeof("query.") - 1U, segment_id, segment_len);
  leaf[sizeof("query.") - 1U + segment_len] = '.';
  memcpy(leaf + sizeof("query.") + segment_len, artifact_leaf,
         artifact_len + 1U);
  namespace_path = lc_pouch_namespace_path(&pouch->allocator, pouch->root_path,
                                           namespace_name);
  if (namespace_path == NULL) {
    lc_free_with_allocator(&pouch->allocator, leaf);
    return NULL;
  }
  index_path = lc_pouch_path_join(&pouch->allocator, namespace_path, "index");
  lc_free_with_allocator(&pouch->allocator, namespace_path);
  if (index_path == NULL) {
    lc_free_with_allocator(&pouch->allocator, leaf);
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index directory path", NULL,
                 NULL, NULL);
    return NULL;
  }
  path = lc_pouch_path_join(&pouch->allocator, index_path, leaf);
  lc_free_with_allocator(&pouch->allocator, index_path);
  lc_free_with_allocator(&pouch->allocator, leaf);
  if (path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index segment artifact path",
                 NULL, NULL, NULL);
  }
  return path;
}

static int lc_pouch_query_index_file_present(const char *path, int *present,
                                             lc_error *error) {
  struct stat st;

  if (path == NULL || present == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index file probe requires path and "
                        "present output",
                        NULL, NULL, NULL);
  }
  *present = 0;
  if (stat(path, &st) == 0) {
    *present = S_ISREG(st.st_mode) ? 1 : 0;
    return LC_OK;
  }
  if (errno == ENOENT) {
    return LC_OK;
  }
  return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to probe pouch query-index artifact",
                      strerror(errno), NULL, "pouch");
}

static char *
lc_pouch_query_index_crypto_descriptor_path(const lc_allocator *allocator,
                                            const char *path, lc_error *error) {
  char *descriptor_path;
  size_t path_len;
  size_t suffix_len;

  if (path == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query-index crypto descriptor requires path", NULL,
                 NULL, "pouch");
    return NULL;
  }
  path_len = strlen(path);
  suffix_len = strlen(LC_POUCH_QUERY_INDEX_CRYPTO_DESC_SUFFIX);
  if (path_len > (size_t)-1 - suffix_len - 1U) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "pouch query-index crypto descriptor path exceeds local "
                 "limit",
                 NULL, NULL, NULL);
    return NULL;
  }
  descriptor_path =
      (char *)lc_alloc_with_allocator(allocator, path_len + suffix_len + 1U);
  if (descriptor_path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index crypto descriptor path",
                 NULL, NULL, NULL);
    return NULL;
  }
  memcpy(descriptor_path, path, path_len);
  memcpy(descriptor_path + path_len, LC_POUCH_QUERY_INDEX_CRYPTO_DESC_SUFFIX,
         suffix_len + 1U);
  return descriptor_path;
}

static char *
lc_pouch_query_index_artifact_context(const lc_allocator *allocator,
                                      const char *namespace_name,
                                      const char *path, lc_error *error) {
  const char prefix[] = "query-index/";
  const char *leaf;
  char *context;
  size_t prefix_len;
  size_t namespace_len;
  size_t leaf_len;
  size_t length;

  if (namespace_name == NULL || path == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch query-index crypto context requires namespace and "
                 "path",
                 NULL, NULL, "pouch");
    return NULL;
  }
  leaf = strrchr(path, '/');
  leaf = leaf != NULL ? leaf + 1 : path;
  prefix_len = sizeof(prefix) - 1U;
  namespace_len = strlen(namespace_name);
  leaf_len = strlen(leaf);
  if (namespace_len > (size_t)-1 - prefix_len - leaf_len - 2U) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "pouch query-index crypto context exceeds local limit", NULL,
                 NULL, NULL);
    return NULL;
  }
  length = prefix_len + namespace_len + 1U + leaf_len;
  context = (char *)lc_alloc_with_allocator(allocator, length + 1U);
  if (context == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch query-index crypto context", NULL,
                 NULL, NULL);
    return NULL;
  }
  memcpy(context, prefix, prefix_len);
  memcpy(context + prefix_len, namespace_name, namespace_len);
  context[prefix_len + namespace_len] = '/';
  memcpy(context + prefix_len + namespace_len + 1U, leaf, leaf_len + 1U);
  return context;
}

static int lc_pouch_query_index_read_file_bytes(const lc_allocator *allocator,
                                                const char *path,
                                                char **out_bytes,
                                                size_t *out_length,
                                                int *present, lc_error *error) {
  FILE *fp;
  char *bytes;
  size_t length;
  size_t capacity;
  size_t got;

  if (path == NULL || out_bytes == NULL || out_length == NULL ||
      present == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index file read requires path and "
                        "outputs",
                        NULL, NULL, NULL);
  }
  *out_bytes = NULL;
  *out_length = 0U;
  *present = 0;
  fp = fopen(path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch query-index file",
                        strerror(errno), NULL, "pouch");
  }
  *present = 1;
  bytes = NULL;
  length = 0U;
  capacity = 0U;
  for (;;) {
    if (capacity - length <= 1U) {
      char *next_bytes;
      size_t next_capacity;

      next_capacity = capacity == 0U ? 4096U : capacity * 2U;
      if (next_capacity <= capacity || next_capacity == (size_t)-1) {
        fclose(fp);
        lc_free_with_allocator(allocator, bytes);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "pouch query-index file exceeds local limit", NULL,
                            NULL, NULL);
      }
      next_bytes = (char *)lc_alloc_with_allocator(allocator, next_capacity);
      if (next_bytes == NULL) {
        fclose(fp);
        lc_free_with_allocator(allocator, bytes);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch query-index file", NULL,
                            NULL, NULL);
      }
      if (bytes != NULL) {
        memcpy(next_bytes, bytes, length);
        lc_free_with_allocator(allocator, bytes);
      }
      bytes = next_bytes;
      capacity = next_capacity;
    }
    got = fread(bytes + length, 1U, capacity - length - 1U, fp);
    length += got;
    if (got == 0U) {
      if (ferror(fp)) {
        fclose(fp);
        lc_free_with_allocator(allocator, bytes);
        return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                            "failed to read pouch query-index file",
                            strerror(errno), NULL, "pouch");
      }
      break;
    }
  }
  if (fclose(fp) != 0) {
    lc_free_with_allocator(allocator, bytes);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch query-index file",
                        strerror(errno), NULL, "pouch");
  }
  bytes[length] = '\0';
  *out_bytes = bytes;
  *out_length = length;
  return LC_OK;
}

static int lc_pouch_query_index_write_bytes_direct_relaxed(const char *path,
                                                           const char *bytes,
                                                           size_t length,
                                                           lc_error *error) {
  const char *cursor;
  size_t remaining;
  int fd;
  int rc;

  if (path == NULL || (bytes == NULL && length > 0U)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index artifact write requires path and "
                        "bytes",
                        NULL, NULL, NULL);
  }
  fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0666);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create pouch query-index artifact",
                        strerror(errno), NULL, "pouch");
  }
  cursor = bytes != NULL ? bytes : "";
  remaining = length;
  rc = LC_OK;
  while (remaining > 0U) {
    ssize_t written;

    written = write(fd, cursor, remaining);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to write pouch query-index artifact",
                        strerror(errno), NULL, "pouch");
      break;
    }
    if (written == 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to make progress writing pouch query-index "
                        "artifact",
                        NULL, NULL, "pouch");
      break;
    }
    cursor += (size_t)written;
    remaining -= (size_t)written;
  }
  if (close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch query-index artifact",
                      strerror(errno), NULL, "pouch");
  }
  if (rc != LC_OK) {
    (void)unlink(path);
  }
  return rc;
}

static void lc_pouch_query_index_put_u64_le(unsigned char *out,
                                            uint64_t value) {
  size_t index;

  if (out == NULL) {
    return;
  }
  for (index = 0U; index < 8U; ++index) {
    out[index] = (unsigned char)((value >> (index * 8U)) & 0xffU);
  }
}

static uint64_t lc_pouch_query_index_get_u64_le(const unsigned char *in) {
  uint64_t value;
  size_t index;

  value = (uint64_t)0U;
  if (in == NULL) {
    return value;
  }
  for (index = 0U; index < 8U; ++index) {
    value |= ((uint64_t)in[index]) << (index * 8U);
  }
  return value;
}

static int lc_pouch_query_index_append_crypto_footer_fd(int fd,
                                                        const char *descriptor,
                                                        lc_error *error) {
  unsigned char footer[LC_POUCH_QUERY_INDEX_CRYPTO_FOOTER_BYTES];
  const char *cursor;
  size_t descriptor_length;
  size_t remaining;
  int rc;

  if (fd < 0 || descriptor == NULL || descriptor[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted query-index artifact requires fd "
                        "and descriptor",
                        NULL, NULL, NULL);
  }
  descriptor_length = strlen(descriptor);
  rc = LC_OK;
  cursor = descriptor;
  remaining = descriptor_length;
  while (remaining > 0U) {
    ssize_t written;

    written = write(fd, cursor, remaining);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to write pouch query-index descriptor footer",
                        strerror(errno), NULL, "pouch");
      break;
    }
    if (written == 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to make progress writing pouch query-index "
                        "descriptor footer",
                        NULL, NULL, "pouch");
      break;
    }
    cursor += (size_t)written;
    remaining -= (size_t)written;
  }
  if (rc == LC_OK) {
    memcpy(footer, LC_POUCH_QUERY_INDEX_CRYPTO_FOOTER_MAGIC, 8U);
    lc_pouch_query_index_put_u64_le(footer + 8U, (uint64_t)descriptor_length);
    cursor = (const char *)footer;
    remaining = sizeof(footer);
    while (remaining > 0U) {
      ssize_t written;

      written = write(fd, cursor, remaining);
      if (written < 0) {
        if (errno == EINTR) {
          continue;
        }
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to write pouch query-index crypto footer",
                          strerror(errno), NULL, "pouch");
        break;
      }
      if (written == 0) {
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to make progress writing pouch query-index "
                          "crypto footer",
                          NULL, NULL, "pouch");
        break;
      }
      cursor += (size_t)written;
      remaining -= (size_t)written;
    }
  }
  return rc;
}

static int lc_pouch_query_index_read_crypto_footer(
    const lc_allocator *allocator, const char *path, char **descriptor_out,
    uint64_t *cipher_length_out, lc_error *error) {
  unsigned char footer[LC_POUCH_QUERY_INDEX_CRYPTO_FOOTER_BYTES];
  char *descriptor;
  struct stat st;
  uint64_t descriptor_length64;
  size_t descriptor_length;
  off_t descriptor_offset;
  ssize_t got;
  int fd;

  if (allocator == NULL || path == NULL || descriptor_out == NULL ||
      cipher_length_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted query-index footer read requires "
                        "allocator, path, and outputs",
                        NULL, NULL, NULL);
  }
  *descriptor_out = NULL;
  *cipher_length_out = 0U;
  if (stat(path, &st) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to stat pouch encrypted query-index artifact",
                        strerror(errno), NULL, "pouch");
  }
  if (st.st_size < 0 ||
      st.st_size < (off_t)LC_POUCH_QUERY_INDEX_CRYPTO_FOOTER_BYTES) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted query-index artifact is missing "
                        "crypto footer",
                        NULL, NULL, "pouch");
  }
  fd = open(path, O_RDONLY);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch encrypted query-index artifact",
                        strerror(errno), NULL, "pouch");
  }
  got = pread(fd, footer, sizeof(footer), st.st_size - (off_t)sizeof(footer));
  if (got < 0) {
    int saved_errno;

    saved_errno = errno;
    close(fd);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch query-index crypto footer",
                        strerror(saved_errno), NULL, "pouch");
  }
  if ((size_t)got != sizeof(footer) ||
      memcmp(footer, LC_POUCH_QUERY_INDEX_CRYPTO_FOOTER_MAGIC, 8U) != 0) {
    close(fd);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted query-index artifact has invalid "
                        "crypto footer",
                        NULL, NULL, "pouch");
  }
  descriptor_length64 = lc_pouch_query_index_get_u64_le(footer + 8U);
  if (descriptor_length64 == 0U ||
      descriptor_length64 >
          (uint64_t)(st.st_size -
                     (off_t)LC_POUCH_QUERY_INDEX_CRYPTO_FOOTER_BYTES) ||
      descriptor_length64 > (uint64_t)((size_t)-1) - 1U) {
    close(fd);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted query-index artifact has invalid "
                        "descriptor length",
                        NULL, NULL, "pouch");
  }
  descriptor_length = (size_t)descriptor_length64;
  descriptor_offset = st.st_size -
                      (off_t)LC_POUCH_QUERY_INDEX_CRYPTO_FOOTER_BYTES -
                      (off_t)descriptor_length;
  if (descriptor_offset < 0) {
    close(fd);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted query-index artifact has invalid "
                        "ciphertext span",
                        NULL, NULL, "pouch");
  }
  descriptor =
      (char *)lc_alloc_with_allocator(allocator, descriptor_length + 1U);
  if (descriptor == NULL) {
    close(fd);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index descriptor", NULL,
                        NULL, NULL);
  }
  got = pread(fd, descriptor, descriptor_length, descriptor_offset);
  if (got < 0) {
    int saved_errno;

    saved_errno = errno;
    close(fd);
    lc_free_with_allocator(allocator, descriptor);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch query-index descriptor",
                        strerror(saved_errno), NULL, "pouch");
  }
  close(fd);
  if ((size_t)got != descriptor_length) {
    lc_free_with_allocator(allocator, descriptor);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted query-index descriptor is truncated",
                        NULL, NULL, "pouch");
  }
  descriptor[descriptor_length] = '\0';
  *descriptor_out = descriptor;
  *cipher_length_out = (uint64_t)descriptor_offset;
  return LC_OK;
}

static int lc_pouch_query_index_source_to_bytes(const lc_allocator *allocator,
                                                lc_source *source,
                                                char **out_bytes,
                                                size_t *out_length,
                                                lc_error *error) {
  lc_sink *sink;
  const void *bytes;
  char *copy;
  size_t length;
  int rc;

  if (source == NULL || out_bytes == NULL || out_length == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index source read requires source and "
                        "outputs",
                        NULL, NULL, NULL);
  }
  *out_bytes = NULL;
  *out_length = 0U;
  sink = NULL;
  rc = lc_sink_to_memory(&sink, error);
  if (rc == LC_OK) {
    rc = lc_copy(source, sink, NULL, error);
  }
  bytes = NULL;
  length = 0U;
  if (rc == LC_OK) {
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  }
  copy = NULL;
  if (rc == LC_OK) {
    if (length == (size_t)-1) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch query-index source exceeds local limit", NULL,
                        NULL, NULL);
    } else {
      copy = (char *)lc_alloc_with_allocator(allocator, length + 1U);
      if (copy == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query-index source bytes",
                          NULL, NULL, NULL);
      } else {
        if (length > 0U) {
          memcpy(copy, bytes, length);
        }
        copy[length] = '\0';
      }
    }
  }
  if (sink != NULL) {
    lc_sink_close(sink);
  }
  if (rc != LC_OK) {
    lc_free_with_allocator(allocator, copy);
    return rc;
  }
  *out_bytes = copy;
  *out_length = length;
  return LC_OK;
}

static int lc_pouch_query_index_read_encrypted_artifact_bytes(
    lc_pouch *pouch, const char *namespace_name, const char *path, int strict,
    char **out_bytes, size_t *out_length, int *present, int *valid,
    lc_error *error) {
  lc_source *source;
  char *context;
  char *descriptor;
  uint64_t cipher_length;
  int rc;

  if (out_bytes == NULL || out_length == NULL || present == NULL ||
      valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted query-index artifact read requires "
                        "outputs",
                        NULL, NULL, NULL);
  }
  *out_bytes = NULL;
  *out_length = 0U;
  *present = 0;
  *valid = 0;
  rc = lc_pouch_query_index_file_present(path, present, error);
  if (rc != LC_OK || !*present) {
    return rc;
  }
  descriptor = NULL;
  cipher_length = 0U;
  rc = lc_pouch_query_index_read_crypto_footer(
      &pouch->allocator, path, &descriptor, &cipher_length, error);
  if (rc != LC_OK) {
    if (!strict && rc != LC_ERR_NOMEM) {
      if (error != NULL) {
        lc_error_cleanup(error);
      }
      return LC_OK;
    }
    return rc;
  }
  context = lc_pouch_query_index_artifact_context(&pouch->allocator,
                                                  namespace_name, path, error);
  if (context == NULL) {
    lc_free_with_allocator(&pouch->allocator, descriptor);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  source = NULL;
  rc = lc_pouch_crypto_source_from_file_span(pouch->crypto, context, path, 0UL,
                                             cipher_length, descriptor, &source,
                                             error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_source_to_bytes(&pouch->allocator, source,
                                              out_bytes, out_length, error);
  }
  if (source != NULL) {
    source->close(source);
  }
  lc_free_with_allocator(&pouch->allocator, context);
  lc_free_with_allocator(&pouch->allocator, descriptor);
  if (rc != LC_OK) {
    if (!strict && rc != LC_ERR_NOMEM) {
      if (error != NULL) {
        lc_error_cleanup(error);
      }
      return LC_OK;
    }
    return rc;
  }
  *valid = 1;
  return LC_OK;
}

static const char *
lc_pouch_query_index_packed_leaf_for_component(size_t component) {
  switch (component) {
  case LC_POUCH_QUERY_INDEX_PACKED_DOC_TABLE:
    return LC_POUCH_QUERY_INDEX_DOC_TABLE_LEAF;
  case LC_POUCH_QUERY_INDEX_PACKED_EXACT:
    return LC_POUCH_QUERY_INDEX_EXACT_TERM_LEAF;
  case LC_POUCH_QUERY_INDEX_PACKED_PRESENCE:
    return LC_POUCH_QUERY_INDEX_PRESENCE_TERM_LEAF;
  case LC_POUCH_QUERY_INDEX_PACKED_RANGE:
    return LC_POUCH_QUERY_INDEX_RANGE_TERM_LEAF;
  case LC_POUCH_QUERY_INDEX_PACKED_TEXT:
    return LC_POUCH_QUERY_INDEX_TEXT_TERM_LEAF;
  case LC_POUCH_QUERY_INDEX_PACKED_TRIGRAM:
    return LC_POUCH_QUERY_INDEX_TRIGRAM_TERM_LEAF;
  case LC_POUCH_QUERY_INDEX_PACKED_TEMPORAL:
    return LC_POUCH_QUERY_INDEX_TEMPORAL_TERM_LEAF;
  case LC_POUCH_QUERY_INDEX_PACKED_DELETE:
    return LC_POUCH_QUERY_INDEX_DELETE_LEAF;
  default:
    return NULL;
  }
}

static size_t lc_pouch_query_index_packed_component_for_path(const char *path) {
  size_t path_len;
  size_t component;

  if (path == NULL) {
    return LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT;
  }
  path_len = strlen(path);
  for (component = 0U; component < LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT;
       ++component) {
    const char *leaf;
    size_t leaf_len;

    leaf = lc_pouch_query_index_packed_leaf_for_component(component);
    if (leaf == NULL) {
      continue;
    }
    leaf_len = strlen(leaf);
    if (path_len >= leaf_len && strcmp(path + path_len - leaf_len, leaf) == 0) {
      return component;
    }
  }
  return LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT;
}

static char *lc_pouch_query_index_packed_path_from_component(
    const lc_allocator *allocator, const char *component_path, size_t component,
    lc_error *error) {
  const char *leaf;
  char *packed_path;
  size_t path_len;
  size_t leaf_len;
  size_t packed_leaf_len;
  size_t prefix_len;

  leaf = lc_pouch_query_index_packed_leaf_for_component(component);
  if (component_path == NULL || leaf == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch packed query-index path requires component path", NULL,
                 NULL, "pouch");
    return NULL;
  }
  path_len = strlen(component_path);
  leaf_len = strlen(leaf);
  packed_leaf_len = strlen(LC_POUCH_QUERY_INDEX_PACKED_LEAF);
  if (path_len < leaf_len ||
      strcmp(component_path + path_len - leaf_len, leaf) != 0) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch packed query-index path has unexpected component", NULL,
                 NULL, "pouch");
    return NULL;
  }
  prefix_len = path_len - leaf_len;
  if (prefix_len > (size_t)-1 - packed_leaf_len - 1U) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "pouch packed query-index path exceeds local limit", NULL,
                 NULL, NULL);
    return NULL;
  }
  packed_path = (char *)lc_alloc_with_allocator(
      allocator, prefix_len + packed_leaf_len + 1U);
  if (packed_path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch packed query-index path", NULL, NULL,
                 NULL);
    return NULL;
  }
  memcpy(packed_path, component_path, prefix_len);
  memcpy(packed_path + prefix_len, LC_POUCH_QUERY_INDEX_PACKED_LEAF,
         packed_leaf_len + 1U);
  return packed_path;
}

static int lc_pouch_query_index_pack_components(
    const lc_allocator *allocator, const char *doc_table, size_t doc_table_len,
    const char *exact, size_t exact_len, const char *presence,
    size_t presence_len, const char *range, size_t range_len, const char *text,
    size_t text_len, const char *trigram, size_t trigram_len,
    const char *temporal, size_t temporal_len, const char *deletes,
    size_t deletes_len, char **out, size_t *out_len, lc_error *error) {
  const char *components[LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT];
  size_t lengths[LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT];
  unsigned char *bytes;
  size_t header_len;
  size_t total_len;
  size_t offset;
  size_t index;

  if (out == NULL || out_len == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch packed query-index encode requires outputs",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  *out_len = 0U;
  components[LC_POUCH_QUERY_INDEX_PACKED_DOC_TABLE] = doc_table;
  components[LC_POUCH_QUERY_INDEX_PACKED_EXACT] = exact;
  components[LC_POUCH_QUERY_INDEX_PACKED_PRESENCE] = presence;
  components[LC_POUCH_QUERY_INDEX_PACKED_RANGE] = range;
  components[LC_POUCH_QUERY_INDEX_PACKED_TEXT] = text;
  components[LC_POUCH_QUERY_INDEX_PACKED_TRIGRAM] = trigram;
  components[LC_POUCH_QUERY_INDEX_PACKED_TEMPORAL] = temporal;
  components[LC_POUCH_QUERY_INDEX_PACKED_DELETE] = deletes;
  lengths[LC_POUCH_QUERY_INDEX_PACKED_DOC_TABLE] = doc_table_len;
  lengths[LC_POUCH_QUERY_INDEX_PACKED_EXACT] = exact_len;
  lengths[LC_POUCH_QUERY_INDEX_PACKED_PRESENCE] = presence_len;
  lengths[LC_POUCH_QUERY_INDEX_PACKED_RANGE] = range_len;
  lengths[LC_POUCH_QUERY_INDEX_PACKED_TEXT] = text_len;
  lengths[LC_POUCH_QUERY_INDEX_PACKED_TRIGRAM] = trigram_len;
  lengths[LC_POUCH_QUERY_INDEX_PACKED_TEMPORAL] = temporal_len;
  lengths[LC_POUCH_QUERY_INDEX_PACKED_DELETE] = deletes_len;

  header_len = LC_POUCH_QUERY_INDEX_PACKED_MAGIC_LEN + 16U +
               (LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT * 8U);
  total_len = header_len;
  for (index = 0U; index < LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT;
       ++index) {
    if (lengths[index] > 0U && components[index] == NULL) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch packed query-index component missing bytes",
                          NULL, NULL, "pouch");
    }
    if (lengths[index] > (size_t)-1 - total_len) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch packed query-index segment exceeds local "
                          "limit",
                          NULL, NULL, NULL);
    }
    total_len += lengths[index];
  }
  bytes = (unsigned char *)lc_alloc_with_allocator(allocator, total_len);
  if (bytes == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch packed query-index segment",
                        NULL, NULL, NULL);
  }
  memcpy(bytes, LC_POUCH_QUERY_INDEX_PACKED_MAGIC,
         LC_POUCH_QUERY_INDEX_PACKED_MAGIC_LEN);
  lc_pouch_query_index_put_u64_le(bytes + LC_POUCH_QUERY_INDEX_PACKED_MAGIC_LEN,
                                  LC_POUCH_QUERY_INDEX_PACKED_VERSION);
  lc_pouch_query_index_put_u64_le(
      bytes + LC_POUCH_QUERY_INDEX_PACKED_MAGIC_LEN + 8U,
      (uint64_t)LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT);
  offset = LC_POUCH_QUERY_INDEX_PACKED_MAGIC_LEN + 16U;
  for (index = 0U; index < LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT;
       ++index) {
    lc_pouch_query_index_put_u64_le(bytes + offset, (uint64_t)lengths[index]);
    offset += 8U;
  }
  for (index = 0U; index < LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT;
       ++index) {
    if (lengths[index] > 0U) {
      memcpy(bytes + offset, components[index], lengths[index]);
      offset += lengths[index];
    }
  }
  *out = (char *)bytes;
  *out_len = total_len;
  return LC_OK;
}

static void lc_pouch_query_index_packed_cache_entry_cleanup(
    const lc_allocator *allocator,
    lc_pouch_query_index_packed_cache_entry *entry) {
  if (entry == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, entry->path);
  lc_free_with_allocator(allocator, entry->bytes);
  memset(entry, 0, sizeof(*entry));
}

static int lc_pouch_query_index_packed_cache_get(
    lc_pouch *pouch, const char *namespace_name, const char *packed_path,
    lc_pouch_query_index_packed_cache_entry **out, int *present, int *valid,
    lc_error *error) {
  lc_pouch_query_index_packed_cache_entry *entry;
  lc_pouch_query_index_packed_cache_entry *previous;
  lc_pouch_query_index_file_signature signature;
  char *bytes;
  size_t length;
  int artifact_valid;
  int rc;

  if (pouch == NULL || packed_path == NULL || out == NULL || present == NULL ||
      valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch packed query-index cache requires inputs", NULL,
                        NULL, NULL);
  }
  *out = NULL;
  *present = 0;
  *valid = 0;
  if (!lc_pouch_query_index_artifact_signature(packed_path, &signature)) {
    *valid = 1;
    return LC_OK;
  }
  previous = NULL;
  entry = pouch->query_packed_cache;
  while (entry != NULL) {
    if (strcmp(entry->path, packed_path) == 0 &&
        lc_pouch_query_index_artifact_signature_equal(&entry->signature,
                                                      &signature)) {
      if (previous != NULL) {
        previous->next = entry->next;
        entry->next = pouch->query_packed_cache;
        pouch->query_packed_cache = entry;
      }
      *out = entry;
      *present = 1;
      *valid = 1;
      return LC_OK;
    }
    previous = entry;
    entry = entry->next;
  }

  bytes = NULL;
  length = 0U;
  artifact_valid = 1;
  if (lc_pouch_crypto_enabled(pouch->crypto)) {
    artifact_valid = 0;
    rc = lc_pouch_query_index_read_encrypted_artifact_bytes(
        pouch, namespace_name, packed_path, 0, &bytes, &length, present,
        &artifact_valid, error);
  } else {
    rc = lc_pouch_query_index_read_file_bytes(&pouch->allocator, packed_path,
                                              &bytes, &length, present, error);
  }
  if (rc != LC_OK || !*present || !artifact_valid) {
    lc_free_with_allocator(&pouch->allocator, bytes);
    *valid = artifact_valid;
    return rc;
  }
  entry = (lc_pouch_query_index_packed_cache_entry *)lc_alloc_with_allocator(
      &pouch->allocator, sizeof(*entry));
  if (entry == NULL) {
    lc_free_with_allocator(&pouch->allocator, bytes);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch packed query-index cache",
                        NULL, NULL, NULL);
  }
  memset(entry, 0, sizeof(*entry));
  entry->path = lc_strdup_with_allocator(&pouch->allocator, packed_path);
  entry->bytes = bytes;
  entry->length = length;
  entry->signature = signature;
  if (entry->path == NULL) {
    lc_pouch_query_index_packed_cache_entry_cleanup(&pouch->allocator, entry);
    lc_free_with_allocator(&pouch->allocator, entry);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch packed query-index path",
                        NULL, NULL, NULL);
  }
  entry->next = pouch->query_packed_cache;
  pouch->query_packed_cache = entry;
  ++pouch->query_packed_cache_count;
  while (pouch->query_packed_cache_count >
         LC_POUCH_QUERY_INDEX_PACKED_CACHE_MAX) {
    lc_pouch_query_index_packed_cache_entry *victim_prev;
    lc_pouch_query_index_packed_cache_entry *victim;

    victim_prev = NULL;
    victim = pouch->query_packed_cache;
    while (victim != NULL && victim->next != NULL) {
      victim_prev = victim;
      victim = victim->next;
    }
    if (victim == NULL) {
      break;
    }
    if (victim_prev != NULL) {
      victim_prev->next = NULL;
    } else {
      pouch->query_packed_cache = NULL;
    }
    lc_pouch_query_index_packed_cache_entry_cleanup(&pouch->allocator, victim);
    lc_free_with_allocator(&pouch->allocator, victim);
    --pouch->query_packed_cache_count;
  }
  *out = entry;
  *valid = 1;
  return LC_OK;
}

static void lc_pouch_query_index_packed_cache_forget(lc_pouch *pouch,
                                                     const char *packed_path) {
  lc_pouch_query_index_packed_cache_entry *entry;
  lc_pouch_query_index_packed_cache_entry *previous;

  if (pouch == NULL || packed_path == NULL) {
    return;
  }
  previous = NULL;
  entry = pouch->query_packed_cache;
  while (entry != NULL) {
    lc_pouch_query_index_packed_cache_entry *next;

    next = entry->next;
    if (entry->path != NULL && strcmp(entry->path, packed_path) == 0) {
      if (previous != NULL) {
        previous->next = next;
      } else {
        pouch->query_packed_cache = next;
      }
      if (pouch->query_packed_cache_count > 0U) {
        --pouch->query_packed_cache_count;
      }
      lc_pouch_query_index_packed_cache_entry_cleanup(&pouch->allocator, entry);
      lc_free_with_allocator(&pouch->allocator, entry);
      entry = next;
      continue;
    }
    previous = entry;
    entry = next;
  }
}

static int lc_pouch_query_index_read_packed_component_bytes(
    lc_pouch *pouch, const char *namespace_name, const char *component_path,
    size_t component, char **out_bytes, size_t *out_length, int *present,
    int *valid, lc_error *error) {
  char *packed_path;
  lc_pouch_query_index_packed_cache_entry *packed;
  char *component_bytes;
  size_t header_len;
  size_t offset;
  size_t component_offset;
  size_t index;
  size_t component_length;
  int rc;

  if (pouch == NULL || out_bytes == NULL || out_length == NULL ||
      present == NULL || valid == NULL ||
      component >= LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch packed query-index component read requires "
                        "inputs",
                        NULL, NULL, NULL);
  }
  *out_bytes = NULL;
  *out_length = 0U;
  *present = 0;
  *valid = 0;
  packed_path = lc_pouch_query_index_packed_path_from_component(
      &pouch->allocator, component_path, component, error);
  if (packed_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  packed = NULL;
  rc = lc_pouch_query_index_packed_cache_get(pouch, namespace_name, packed_path,
                                             &packed, present, valid, error);
  if (rc != LC_OK || !*present || !*valid) {
    lc_free_with_allocator(&pouch->allocator, packed_path);
    return rc;
  }
  header_len = LC_POUCH_QUERY_INDEX_PACKED_MAGIC_LEN + 16U +
               (LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT * 8U);
  if (packed == NULL || packed->length < header_len ||
      memcmp(packed->bytes, LC_POUCH_QUERY_INDEX_PACKED_MAGIC,
             LC_POUCH_QUERY_INDEX_PACKED_MAGIC_LEN) != 0 ||
      lc_pouch_query_index_get_u64_le((const unsigned char *)packed->bytes +
                                      LC_POUCH_QUERY_INDEX_PACKED_MAGIC_LEN) !=
          LC_POUCH_QUERY_INDEX_PACKED_VERSION ||
      lc_pouch_query_index_get_u64_le((const unsigned char *)packed->bytes +
                                      LC_POUCH_QUERY_INDEX_PACKED_MAGIC_LEN +
                                      8U) !=
          LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT) {
    *valid = 0;
    lc_free_with_allocator(&pouch->allocator, packed_path);
    return LC_OK;
  }
  offset = header_len;
  component_offset = 0U;
  component_length = 0U;
  for (index = 0U; index < LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT;
       ++index) {
    uint64_t length64;
    size_t length;

    length64 = lc_pouch_query_index_get_u64_le(
        (const unsigned char *)packed->bytes +
        LC_POUCH_QUERY_INDEX_PACKED_MAGIC_LEN + 16U + (index * 8U));
    if (length64 > (uint64_t)((size_t)-1)) {
      *valid = 0;
      lc_free_with_allocator(&pouch->allocator, packed_path);
      return LC_OK;
    }
    length = (size_t)length64;
    if (offset > packed->length || length > packed->length - offset) {
      *valid = 0;
      lc_free_with_allocator(&pouch->allocator, packed_path);
      return LC_OK;
    }
    if (index == component) {
      component_offset = offset;
      component_length = length;
    }
    offset += length;
  }
  if (offset != packed->length) {
    *valid = 0;
    lc_free_with_allocator(&pouch->allocator, packed_path);
    return LC_OK;
  }
  lc_free_with_allocator(&pouch->allocator, packed_path);
  if (component_offset > packed->length ||
      component_length > packed->length - component_offset) {
    *valid = 0;
    return LC_OK;
  }
  component_bytes =
      (char *)lc_alloc_with_allocator(&pouch->allocator, component_length + 1U);
  if (component_bytes == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch packed query-index "
                        "component",
                        NULL, NULL, NULL);
  }
  if (component_length > 0U) {
    memcpy(component_bytes, packed->bytes + component_offset, component_length);
  }
  component_bytes[component_length] = '\0';
  *out_bytes = component_bytes;
  *out_length = component_length;
  *valid = 1;
  return LC_OK;
}

static int lc_pouch_query_index_write_artifact_bytes(
    lc_pouch *pouch, const char *namespace_name, const char *path,
    const char *bytes, size_t length, lc_error *error) {
  lc_source *source;
  char *context;
  char *descriptor;
  uint64_t plain_bytes;
  uint64_t cipher_bytes;
  int fd;
  int rc;

  if (pouch == NULL || path == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index artifact write requires pouch and "
                        "path",
                        NULL, NULL, NULL);
  }
  lc_pouch_query_index_packed_cache_forget(pouch, path);
  if (!lc_pouch_crypto_enabled(pouch->crypto)) {
    return lc_pouch_query_index_write_bytes_direct_relaxed(
        path, bytes != NULL ? bytes : "", length, error);
  }
  context = lc_pouch_query_index_artifact_context(&pouch->allocator,
                                                  namespace_name, path, error);
  if (context == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  source = NULL;
  descriptor = NULL;
  fd = -1;
  rc =
      lc_source_from_memory(bytes != NULL ? bytes : "", length, &source, error);
  if (rc == LC_OK) {
    fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0666);
    if (fd < 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create pouch query-index artifact",
                        strerror(errno), NULL, "pouch");
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_crypto_stream_to_fd_crc_with_compression(
        pouch->crypto, context, fd, source, &plain_bytes, &cipher_bytes, NULL,
        &descriptor, 0, error);
  }
  if (source != NULL) {
    source->close(source);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_append_crypto_footer_fd(fd, descriptor, error);
  }
  if (fd >= 0 && close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch encrypted query-index artifact",
                      strerror(errno), NULL, "pouch");
  }
  if (rc != LC_OK) {
    unlink(path);
  }
  lc_free_with_allocator(&pouch->allocator, descriptor);
  lc_free_with_allocator(&pouch->allocator, context);
  return rc;
}

static void
lc_pouch_query_index_manifest_cleanup(const lc_allocator *allocator,
                                      lc_pouch_query_index_manifest *manifest) {
  size_t index;

  if (manifest == NULL) {
    return;
  }
  for (index = 0U; index < manifest->segment_count; ++index) {
    lc_free_with_allocator(allocator, manifest->segments[index].id);
  }
  lc_free_with_allocator(allocator, manifest->segments);
  memset(manifest, 0, sizeof(*manifest));
}

static int lc_pouch_query_index_manifest_clone(
    const lc_allocator *allocator, lc_pouch_query_index_manifest *out,
    const lc_pouch_query_index_manifest *source) {
  size_t index;

  if (out == NULL || source == NULL) {
    return LC_ERR_INVALID;
  }
  memset(out, 0, sizeof(*out));
  out->index_seq = source->index_seq;
  out->present = source->present;
  out->valid = source->valid;
  if (source->segment_count == 0U) {
    return LC_OK;
  }
  out->segments =
      (lc_pouch_query_index_manifest_segment *)lc_calloc_with_allocator(
          allocator, source->segment_count, sizeof(out->segments[0]));
  if (out->segments == NULL) {
    return LC_ERR_NOMEM;
  }
  out->segment_capacity = source->segment_count;
  for (index = 0U; index < source->segment_count; ++index) {
    out->segments[index] = source->segments[index];
    out->segments[index].id =
        lc_strdup_with_allocator(allocator, source->segments[index].id);
    if (out->segments[index].id == NULL) {
      lc_pouch_query_index_manifest_cleanup(allocator, out);
      return LC_ERR_NOMEM;
    }
  }
  out->segment_count = source->segment_count;
  return LC_OK;
}

static int
lc_pouch_query_index_manifest_reserve(const lc_allocator *allocator,
                                      lc_pouch_query_index_manifest *manifest,
                                      size_t needed, lc_error *error) {
  lc_pouch_query_index_manifest_segment *next;
  size_t next_capacity;

  if (manifest == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index manifest reserve requires manifest",
                        NULL, NULL, NULL);
  }
  if (needed <= manifest->segment_capacity) {
    return LC_OK;
  }
  next_capacity =
      manifest->segment_capacity == 0U ? 8U : manifest->segment_capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch query-index manifest exceeds local limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next = (lc_pouch_query_index_manifest_segment *)lc_alloc_with_allocator(
      allocator, next_capacity * sizeof(next[0]));
  if (next == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index manifest", NULL,
                        NULL, NULL);
  }
  if (manifest->segments != NULL) {
    memcpy(next, manifest->segments, manifest->segment_count * sizeof(next[0]));
    lc_free_with_allocator(allocator, manifest->segments);
  }
  memset(next + manifest->segment_count, 0,
         (next_capacity - manifest->segment_count) * sizeof(next[0]));
  manifest->segments = next;
  manifest->segment_capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_query_index_segment_id(lc_pouch_generation index_seq,
                                           char *out, size_t out_size,
                                           lc_error *error) {
  if (out == NULL || out_size == 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index segment id requires output", NULL,
                        NULL, NULL);
  }
  if (lc_u64_format_base10_padded((lc_u64)index_seq, 20U, out, out_size) < 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index segment id exceeds local limit",
                        NULL, NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_query_index_manifest_add_front(
    const lc_allocator *allocator, lc_pouch_query_index_manifest *manifest,
    const char *segment_id, lc_pouch_generation base_index_seq,
    lc_pouch_generation index_seq, unsigned long row_count,
    unsigned long row_hash, unsigned long delete_count,
    unsigned long delete_hash, lc_error *error) {
  lc_pouch_query_index_manifest_segment *segment;
  char *segment_id_copy;
  int rc;

  if (manifest == NULL || segment_id == NULL || segment_id[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index manifest append requires segment",
                        NULL, NULL, NULL);
  }
  segment_id_copy = lc_strdup_with_allocator(allocator, segment_id);
  if (segment_id_copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index manifest "
                        "segment",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_query_index_manifest_reserve(
      allocator, manifest, manifest->segment_count + 1U, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(allocator, segment_id_copy);
    return rc;
  }
  if (manifest->segment_count > 0U) {
    memmove(&manifest->segments[1], &manifest->segments[0],
            manifest->segment_count * sizeof(manifest->segments[0]));
    memset(&manifest->segments[0], 0, sizeof(manifest->segments[0]));
  }
  segment = &manifest->segments[0];
  segment->id = segment_id_copy;
  segment->base_index_seq = base_index_seq;
  segment->index_seq = index_seq;
  segment->row_count = row_count;
  segment->row_hash = row_hash;
  segment->delete_count = delete_count;
  segment->delete_hash = delete_hash;
  ++manifest->segment_count;
  manifest->index_seq = index_seq;
  manifest->present = 1;
  manifest->valid = 1;
  return LC_OK;
}

static int lc_pouch_query_index_manifest_coherent(
    const lc_pouch_query_index_manifest *manifest) {
  size_t index;

  if (manifest == NULL || manifest->segment_count == 0U) {
    return 0;
  }
  for (index = 0U; index < manifest->segment_count; ++index) {
    char expected_id[32];

    if (manifest->segments[index].id == NULL ||
        manifest->segments[index].base_index_seq >
            manifest->segments[index].index_seq ||
        (manifest->segments[index].index_seq > 0UL &&
         manifest->segments[index].base_index_seq ==
             manifest->segments[index].index_seq) ||
        lc_pouch_query_index_segment_id(manifest->segments[index].index_seq,
                                        expected_id, sizeof(expected_id),
                                        NULL) != LC_OK ||
        strcmp(manifest->segments[index].id, expected_id) != 0) {
      return 0;
    }
    if (index == 0U) {
      if (manifest->segments[index].index_seq != manifest->index_seq) {
        return 0;
      }
    } else {
      if (manifest->segments[index - 1U].base_index_seq !=
              manifest->segments[index].index_seq ||
          manifest->segments[index - 1U].index_seq <=
              manifest->segments[index].index_seq) {
        return 0;
      }
    }
  }
  return manifest->segments[manifest->segment_count - 1U].base_index_seq == 0UL;
}

static char *lc_pouch_query_index_manifest_next_line(char **cursor) {
  char *line;
  char *newline;

  if (cursor == NULL || *cursor == NULL || **cursor == '\0') {
    return NULL;
  }
  line = *cursor;
  newline = strchr(line, '\n');
  if (newline == NULL) {
    *cursor = line + strlen(line);
  } else {
    *newline = '\0';
    *cursor = newline + 1;
  }
  return line;
}

static char *lc_pouch_query_index_manifest_next_token(char **cursor) {
  char *token;
  char *space;

  if (cursor == NULL || *cursor == NULL || **cursor == '\0') {
    return NULL;
  }
  token = *cursor;
  space = strchr(token, ' ');
  if (space == NULL) {
    *cursor = token + strlen(token);
  } else {
    *space = '\0';
    *cursor = space + 1U;
  }
  return token;
}

static int lc_pouch_query_index_manifest_parse_u64(char **cursor,
                                                   uint64_t *out) {
  char *token;
  lc_u64 value;

  token = lc_pouch_query_index_manifest_next_token(cursor);
  if (token == NULL || !lc_u64_parse_base10(token, &value)) {
    return 0;
  }
  *out = (uint64_t)value;
  return 1;
}

static int lc_pouch_query_index_manifest_parse_ulong(char **cursor,
                                                     unsigned long *out) {
  char *token;

  token = lc_pouch_query_index_manifest_next_token(cursor);
  return token != NULL && lc_parse_ulong_base10_checked(token, out);
}

static lc_pouch_query_index_manifest_segment *
lc_pouch_query_index_manifest_find_segment(
    lc_pouch_query_index_manifest *manifest, const char *segment_id) {
  size_t index;

  if (manifest == NULL || segment_id == NULL || segment_id[0] == '\0') {
    return NULL;
  }
  for (index = 0U; index < manifest->segment_count; ++index) {
    if (manifest->segments[index].id != NULL &&
        strcmp(manifest->segments[index].id, segment_id) == 0) {
      return &manifest->segments[index];
    }
  }
  return NULL;
}

static int
lc_pouch_query_index_manifest_read(lc_pouch *pouch, const char *path,
                                   lc_pouch_query_index_manifest *manifest,
                                   lc_error *error) {
  char *bytes;
  char *cursor;
  char *line;
  size_t length;
  unsigned long version;
  int present;
  int rc;

  if (pouch == NULL || path == NULL || manifest == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index manifest read requires pouch, "
                        "path, and manifest",
                        NULL, NULL, NULL);
  }
  memset(manifest, 0, sizeof(*manifest));
  bytes = NULL;
  length = 0U;
  present = 0;
  rc = lc_pouch_query_index_read_file_bytes(&pouch->allocator, path, &bytes,
                                            &length, &present, error);
  if (rc != LC_OK || !present) {
    lc_free_with_allocator(&pouch->allocator, bytes);
    manifest->present = present;
    return rc;
  }
  manifest->present = 1;
  cursor = bytes;
  line = lc_pouch_query_index_manifest_next_line(&cursor);
  if (line == NULL ||
      strcmp(line, "format=" LC_POUCH_QUERY_INDEX_MANIFEST_FORMAT) != 0) {
    goto done;
  }
  line = lc_pouch_query_index_manifest_next_line(&cursor);
  if (line == NULL ||
      !lc_pouch_query_index_parse_header_line(line, "version", &version) ||
      version != LC_POUCH_QUERY_INDEX_MANIFEST_VERSION) {
    goto done;
  }
  line = lc_pouch_query_index_manifest_next_line(&cursor);
  if (line == NULL || !lc_pouch_query_index_parse_header_u64(
                          line, "state_index_seq", &manifest->index_seq)) {
    goto done;
  }
  while ((line = lc_pouch_query_index_manifest_next_line(&cursor)) != NULL) {
    char *id;
    lc_pouch_generation base_index_seq;
    lc_pouch_generation index_seq;
    unsigned long row_count;
    unsigned long row_hash;
    unsigned long delete_count;
    unsigned long delete_hash;

    if (line[0] == '\0') {
      continue;
    }
    if (strncmp(line, "artifact ", 9U) == 0) {
      lc_pouch_query_index_manifest_segment *segment;
      unsigned long artifact_index;
      uint64_t size;
      unsigned long mtime;
      unsigned long mtime_nsec;
      unsigned long ctime;
      unsigned long ctime_nsec;
      unsigned long inode;

      char *artifact_cursor;

      artifact_cursor = line + 9U;
      id = lc_pouch_query_index_manifest_next_token(&artifact_cursor);
      if (id == NULL || strlen(id) >= 64U ||
          !lc_pouch_query_index_manifest_parse_ulong(&artifact_cursor,
                                                     &artifact_index) ||
          !lc_pouch_query_index_manifest_parse_u64(&artifact_cursor, &size) ||
          !lc_pouch_query_index_manifest_parse_ulong(&artifact_cursor,
                                                     &mtime) ||
          !lc_pouch_query_index_manifest_parse_ulong(&artifact_cursor,
                                                     &mtime_nsec) ||
          !lc_pouch_query_index_manifest_parse_ulong(&artifact_cursor,
                                                     &ctime) ||
          !lc_pouch_query_index_manifest_parse_ulong(&artifact_cursor,
                                                     &ctime_nsec) ||
          !lc_pouch_query_index_manifest_parse_ulong(&artifact_cursor,
                                                     &inode) ||
          lc_pouch_query_index_manifest_next_token(&artifact_cursor) != NULL ||
          artifact_index >= LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_COUNT) {
        goto done;
      }
      segment = lc_pouch_query_index_manifest_find_segment(manifest, id);
      if (segment == NULL) {
        goto done;
      }
      segment->artifact_signatures[artifact_index].size = size;
      segment->artifact_signatures[artifact_index].mtime = mtime;
      segment->artifact_signatures[artifact_index].mtime_nsec = mtime_nsec;
      segment->artifact_signatures[artifact_index].ctime = ctime;
      segment->artifact_signatures[artifact_index].ctime_nsec = ctime_nsec;
      segment->artifact_signatures[artifact_index].inode = inode;
      segment->artifact_signatures[artifact_index].present = 1;
      continue;
    }
    if (strncmp(line, "segment ", 8U) != 0) {
      goto done;
    }
    {
      char *segment_cursor;

      segment_cursor = line + 8U;
      id = lc_pouch_query_index_manifest_next_token(&segment_cursor);
      if (id == NULL || strlen(id) >= 64U ||
          !lc_pouch_query_index_manifest_parse_u64(&segment_cursor,
                                                   &base_index_seq) ||
          !lc_pouch_query_index_manifest_parse_u64(&segment_cursor,
                                                   &index_seq) ||
          !lc_pouch_query_index_manifest_parse_ulong(&segment_cursor,
                                                     &row_count) ||
          !lc_pouch_query_index_manifest_parse_ulong(&segment_cursor,
                                                     &row_hash) ||
          !lc_pouch_query_index_manifest_parse_ulong(&segment_cursor,
                                                     &delete_count) ||
          !lc_pouch_query_index_manifest_parse_ulong(&segment_cursor,
                                                     &delete_hash) ||
          lc_pouch_query_index_manifest_next_token(&segment_cursor) != NULL) {
        goto done;
      }
    }
    rc = lc_pouch_query_index_manifest_reserve(
        &pouch->allocator, manifest, manifest->segment_count + 1U, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(&pouch->allocator, bytes);
      return rc;
    }
    manifest->segments[manifest->segment_count].id =
        lc_strdup_with_allocator(&pouch->allocator, id);
    if (manifest->segments[manifest->segment_count].id == NULL) {
      lc_free_with_allocator(&pouch->allocator, bytes);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query-index manifest "
                          "segment",
                          NULL, NULL, NULL);
    }
    manifest->segments[manifest->segment_count].base_index_seq = base_index_seq;
    manifest->segments[manifest->segment_count].index_seq = index_seq;
    manifest->segments[manifest->segment_count].row_count = row_count;
    manifest->segments[manifest->segment_count].row_hash = row_hash;
    manifest->segments[manifest->segment_count].delete_count = delete_count;
    manifest->segments[manifest->segment_count].delete_hash = delete_hash;
    ++manifest->segment_count;
  }
  manifest->valid = lc_pouch_query_index_manifest_coherent(manifest);

done:
  lc_free_with_allocator(&pouch->allocator, bytes);
  return LC_OK;
}

static void lc_pouch_query_index_unlink_artifact(lc_pouch *pouch,
                                                 const char *path) {
  char *descriptor_path;

  if (path == NULL) {
    return;
  }
  lc_pouch_query_index_packed_cache_forget(pouch, path);
  (void)unlink(path);
  descriptor_path = lc_pouch_query_index_crypto_descriptor_path(
      &pouch->allocator, path, NULL);
  if (descriptor_path != NULL) {
    (void)unlink(descriptor_path);
    lc_free_with_allocator(&pouch->allocator, descriptor_path);
  }
}

static int lc_pouch_query_index_manifest_has_segment(
    const lc_pouch_query_index_manifest *manifest, const char *segment_id) {
  size_t index;

  if (manifest == NULL || segment_id == NULL || segment_id[0] == '\0') {
    return 0;
  }
  for (index = 0U; index < manifest->segment_count; ++index) {
    if (manifest->segments[index].id != NULL &&
        strcmp(manifest->segments[index].id, segment_id) == 0) {
      return 1;
    }
  }
  return 0;
}

static int lc_pouch_query_index_artifact_segment_id(const char *leaf,
                                                    char *segment_id,
                                                    size_t segment_id_size) {
  size_t index;
  size_t leaf_length;

  if (leaf == NULL || segment_id == NULL || segment_id_size < 21U ||
      strncmp(leaf, "query.", 6U) != 0) {
    return 0;
  }
  leaf_length = strlen(leaf);
  if (leaf_length <= 26U) {
    return 0;
  }
  for (index = 0U; index < 20U; ++index) {
    char ch;

    ch = leaf[6U + index];
    if (ch < '0' || ch > '9') {
      return 0;
    }
    segment_id[index] = ch;
  }
  if (leaf[26U] != '.') {
    return 0;
  }
  segment_id[20U] = '\0';
  return 1;
}

static int lc_pouch_query_index_unlink_unreferenced_artifacts(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_manifest *keep_manifest, lc_error *error) {
  char *namespace_path;
  char *index_path;
  DIR *dir;
  struct dirent *entry;
  int rc;

  if (pouch == NULL || namespace_name == NULL || keep_manifest == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index orphan cleanup requires pouch, "
                        "namespace, and manifest",
                        NULL, NULL, NULL);
  }
  namespace_path = lc_pouch_namespace_path(&pouch->allocator, pouch->root_path,
                                           namespace_name);
  if (namespace_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace path", NULL, NULL,
                        NULL);
  }
  index_path = lc_pouch_path_join(&pouch->allocator, namespace_path, "index");
  lc_free_with_allocator(&pouch->allocator, namespace_path);
  if (index_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index path", NULL, NULL,
                        NULL);
  }
  dir = opendir(index_path);
  if (dir == NULL) {
    rc = errno == ENOENT ? LC_OK
                         : lc_error_set(error, LC_ERR_TRANSPORT, errno,
                                        "failed to open pouch query-index "
                                        "directory for cleanup",
                                        index_path, NULL, "pouch");
    lc_free_with_allocator(&pouch->allocator, index_path);
    return rc;
  }
  rc = LC_OK;
  while ((entry = readdir(dir)) != NULL) {
    char segment_id[21];
    char *path;
    int unlink_entry;

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0 ||
        strcmp(entry->d_name, LC_POUCH_QUERY_INDEX_MANIFEST_LEAF) == 0) {
      continue;
    }
    unlink_entry = 0;
    if (lc_pouch_query_index_artifact_segment_id(entry->d_name, segment_id,
                                                 sizeof(segment_id))) {
      unlink_entry =
          !lc_pouch_query_index_manifest_has_segment(keep_manifest, segment_id);
    } else if (strcmp(entry->d_name, LC_POUCH_QUERY_INDEX_LEAF) == 0 ||
               strncmp(entry->d_name, LC_POUCH_QUERY_INDEX_LEAF ".",
                       strlen(LC_POUCH_QUERY_INDEX_LEAF) + 1U) == 0) {
      unlink_entry = 1;
    }
    if (!unlink_entry) {
      continue;
    }
    path = lc_pouch_path_join(&pouch->allocator, index_path, entry->d_name);
    if (path == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index cleanup path",
                        NULL, NULL, NULL);
      break;
    }
    lc_pouch_query_index_unlink_artifact(pouch, path);
    lc_free_with_allocator(&pouch->allocator, path);
  }
  if (closedir(dir) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, errno,
                      "failed to close pouch query-index cleanup directory",
                      index_path, NULL, "pouch");
  }
  lc_free_with_allocator(&pouch->allocator, index_path);
  return rc;
}

static void lc_pouch_query_index_manifest_unlink_segments(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_manifest *manifest,
    const lc_pouch_query_index_manifest *keep_manifest) {
  size_t segment_index;

  if (pouch == NULL || namespace_name == NULL || manifest == NULL) {
    return;
  }
  for (segment_index = 0U; segment_index < manifest->segment_count;
       ++segment_index) {
    char *header_path;
    char *doc_table_path;
    char *exact_term_path;
    char *presence_term_path;
    char *range_term_path;
    char *text_term_path;
    char *trigram_term_path;
    char *temporal_term_path;
    char *delete_path;
    char *packed_path;
    lc_error ignored;
    int keep;

    keep = lc_pouch_query_index_manifest_has_segment(
        keep_manifest, manifest->segments[segment_index].id);
    if (keep) {
      continue;
    }
    header_path = NULL;
    doc_table_path = NULL;
    exact_term_path = NULL;
    presence_term_path = NULL;
    range_term_path = NULL;
    text_term_path = NULL;
    trigram_term_path = NULL;
    temporal_term_path = NULL;
    delete_path = NULL;
    packed_path = NULL;
    lc_error_init(&ignored);
    if (lc_pouch_query_index_segmented_paths(
            pouch, namespace_name, manifest->segments[segment_index].id,
            &header_path, &doc_table_path, &exact_term_path,
            &presence_term_path, &range_term_path, &text_term_path,
            &trigram_term_path, &temporal_term_path, &delete_path,
            &ignored) == LC_OK) {
      lc_pouch_query_index_unlink_artifact(pouch, header_path);
      lc_pouch_query_index_unlink_artifact(pouch, doc_table_path);
      lc_pouch_query_index_unlink_artifact(pouch, exact_term_path);
      lc_pouch_query_index_unlink_artifact(pouch, presence_term_path);
      lc_pouch_query_index_unlink_artifact(pouch, range_term_path);
      lc_pouch_query_index_unlink_artifact(pouch, text_term_path);
      lc_pouch_query_index_unlink_artifact(pouch, trigram_term_path);
      lc_pouch_query_index_unlink_artifact(pouch, temporal_term_path);
      lc_pouch_query_index_unlink_artifact(pouch, delete_path);
      packed_path = lc_pouch_query_index_packed_path_from_component(
          &pouch->allocator, doc_table_path,
          LC_POUCH_QUERY_INDEX_PACKED_DOC_TABLE, &ignored);
      lc_pouch_query_index_unlink_artifact(pouch, packed_path);
    }
    lc_error_cleanup(&ignored);
    lc_free_with_allocator(&pouch->allocator, packed_path);
    lc_pouch_query_index_segmented_paths_cleanup(
        pouch, &header_path, &doc_table_path, &exact_term_path,
        &presence_term_path, &range_term_path, &text_term_path,
        &trigram_term_path, &temporal_term_path, &delete_path);
  }
}

static int lc_pouch_query_index_manifest_write(
    lc_pouch *pouch, const char *path,
    const lc_pouch_query_index_manifest *manifest, lc_error *error) {
  lc_pouch_query_index_text text;
  size_t index;
  int rc;

  if (pouch == NULL || path == NULL || manifest == NULL || !manifest->valid) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index manifest write requires valid "
                        "manifest",
                        NULL, NULL, NULL);
  }
  memset(&text, 0, sizeof(text));
  text.allocator = &pouch->allocator;
  rc = lc_pouch_query_index_text_append_cstr(
      &text, "format=" LC_POUCH_QUERY_INDEX_MANIFEST_FORMAT "\n", NULL, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        &text, "version", LC_POUCH_QUERY_INDEX_MANIFEST_VERSION, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_u64_line(&text, "state_index_seq",
                                                   manifest->index_seq, error);
  }
  for (index = 0U; rc == LC_OK && index < manifest->segment_count; ++index) {
    char line[192];
    char base_index_seq_text[32];
    char index_seq_text[32];
    size_t artifact_index;
    int written;

    if (lc_u64_format_base10((lc_u64)manifest->segments[index].base_index_seq,
                             base_index_seq_text,
                             sizeof(base_index_seq_text)) < 0 ||
        lc_u64_format_base10((lc_u64)manifest->segments[index].index_seq,
                             index_seq_text, sizeof(index_seq_text)) < 0) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index manifest segment exceeds local "
                        "limit",
                        NULL, NULL, NULL);
      break;
    }
    written = snprintf(line, sizeof(line), "segment %s %s %s %lu %lu %lu %lu\n",
                       manifest->segments[index].id, base_index_seq_text,
                       index_seq_text, manifest->segments[index].row_count,
                       manifest->segments[index].row_hash,
                       manifest->segments[index].delete_count,
                       manifest->segments[index].delete_hash);
    if (written < 0 || (size_t)written >= sizeof(line)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index manifest segment exceeds local "
                        "limit",
                        NULL, NULL, NULL);
    } else {
      rc = lc_pouch_query_index_text_append(&text, line, (size_t)written, NULL,
                                            error);
    }
    for (artifact_index = 0U;
         rc == LC_OK &&
         artifact_index < LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_COUNT;
         ++artifact_index) {
      const lc_pouch_query_index_manifest_artifact_signature *signature;
      char artifact_line[256];
      char size_text[32];

      signature =
          &manifest->segments[index].artifact_signatures[artifact_index];
      if (!signature->present) {
        continue;
      }
      if (lc_u64_format_base10((lc_u64)signature->size, size_text,
                               sizeof(size_text)) < 0) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query-index manifest artifact exceeds local "
                          "limit",
                          NULL, NULL, NULL);
        break;
      }
      written =
          snprintf(artifact_line, sizeof(artifact_line),
                   "artifact %s %lu %s %lu %lu %lu %lu %lu\n",
                   manifest->segments[index].id, (unsigned long)artifact_index,
                   size_text, signature->mtime, signature->mtime_nsec,
                   signature->ctime, signature->ctime_nsec, signature->inode);
      if (written < 0 || (size_t)written >= sizeof(artifact_line)) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query-index manifest artifact exceeds local "
                          "limit",
                          NULL, NULL, NULL);
      } else {
        rc = lc_pouch_query_index_text_append(&text, artifact_line,
                                              (size_t)written, NULL, error);
      }
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_path_write_text_file_relaxed(
        path, text.bytes != NULL ? text.bytes : "", error);
  }
  lc_free_with_allocator(&pouch->allocator, text.bytes);
  return rc;
}

static void lc_pouch_query_index_key_hex_set_cleanup(
    const lc_allocator *allocator, lc_pouch_query_index_key_hex_set *set) {
  size_t index;

  if (set == NULL) {
    return;
  }
  for (index = 0U; index < set->count; ++index) {
    lc_free_with_allocator(allocator, set->items[index]);
  }
  lc_free_with_allocator(allocator, set->items);
  memset(set, 0, sizeof(*set));
}

static int lc_pouch_query_index_key_hex_set_find(
    const lc_pouch_query_index_key_hex_set *set, const char *key_hex,
    size_t *position_out) {
  size_t low;
  size_t high;

  if (position_out != NULL) {
    *position_out = 0U;
  }
  if (set == NULL || key_hex == NULL) {
    return 0;
  }
  low = 0U;
  high = set->count;
  while (low < high) {
    size_t mid;
    int cmp;

    mid = low + ((high - low) / 2U);
    cmp = strcmp(key_hex, set->items[mid]);
    if (cmp == 0) {
      if (position_out != NULL) {
        *position_out = mid;
      }
      return 1;
    }
    if (cmp < 0) {
      high = mid;
    } else {
      low = mid + 1U;
    }
  }
  if (position_out != NULL) {
    *position_out = low;
  }
  return 0;
}

static int
lc_pouch_query_index_key_hex_set_add(const lc_allocator *allocator,
                                     lc_pouch_query_index_key_hex_set *set,
                                     const char *key_hex, lc_error *error) {
  char **next;
  size_t position;
  size_t next_capacity;

  if (set == NULL || key_hex == NULL || key_hex[0] == '\0') {
    return LC_OK;
  }
  if (lc_pouch_query_index_key_hex_set_find(set, key_hex, &position)) {
    return LC_OK;
  }
  if (set->count == set->capacity) {
    next_capacity = set->capacity == 0U ? 32U : set->capacity * 2U;
    next = (char **)lc_alloc_with_allocator(allocator,
                                            next_capacity * sizeof(next[0]));
    if (next == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query-index key set", NULL,
                          NULL, NULL);
    }
    if (set->items != NULL) {
      memcpy(next, set->items, set->count * sizeof(next[0]));
      lc_free_with_allocator(allocator, set->items);
    }
    set->items = next;
    set->capacity = next_capacity;
  }
  if (position < set->count) {
    memmove(&set->items[position + 1U], &set->items[position],
            (set->count - position) * sizeof(set->items[0]));
  }
  set->items[position] = lc_strdup_with_allocator(allocator, key_hex);
  if (set->items[position] == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index key", NULL, NULL,
                        NULL);
  }
  ++set->count;
  return LC_OK;
}

static int lc_pouch_query_index_encode_delete_keys(
    lc_pouch_query_index_incremental_change *changes, size_t change_count,
    lc_pouch_query_index_text *out, unsigned long *delete_count,
    unsigned long *delete_hash, lc_error *error) {
  size_t index;
  int rc;

  if (out == NULL || delete_count == NULL || delete_hash == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index delete encode requires outputs",
                        NULL, NULL, NULL);
  }
  *delete_count = 0UL;
  *delete_hash = lc_pouch_query_index_hash_init();
  memset(out, 0, sizeof(*out));
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < change_count; ++index) {
    if (changes[index].found || changes[index].key_hex == NULL) {
      continue;
    }
    rc = lc_pouch_query_index_text_append_cstr(out, changes[index].key_hex,
                                               NULL, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(out, "\n", NULL, error);
    }
    if (rc == LC_OK) {
      ++*delete_count;
    }
  }
  if (rc == LC_OK && out->bytes != NULL) {
    lc_pouch_query_index_hash_bytes(delete_hash, out->bytes, out->length);
  }
  return rc;
}

static int lc_pouch_query_index_load_delete_keys(
    lc_pouch *pouch, const char *namespace_name, const char *path,
    lc_pouch_query_index_key_hex_set *set, unsigned long *delete_count,
    unsigned long *delete_hash, lc_error *error) {
  char *bytes;
  char *cursor;
  char *line;
  size_t length;
  int present;
  int valid;
  int rc;

  if (pouch == NULL || path == NULL || set == NULL || delete_count == NULL ||
      delete_hash == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index delete load requires pouch, path, "
                        "set, count, and hash",
                        NULL, NULL, NULL);
  }
  *delete_count = 0UL;
  *delete_hash = lc_pouch_query_index_hash_init();
  bytes = NULL;
  length = 0U;
  present = 0;
  valid = 1;
  rc = lc_pouch_query_index_read_packed_component_bytes(
      pouch, namespace_name, path, LC_POUCH_QUERY_INDEX_PACKED_DELETE, &bytes,
      &length, &present, &valid, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, bytes);
    return rc;
  }
  if (!present) {
    lc_free_with_allocator(&pouch->allocator, bytes);
    return LC_OK;
  }
  if (!valid) {
    lc_free_with_allocator(&pouch->allocator, bytes);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index delete artifact is not readable",
                        NULL, NULL, "pouch");
  }
  lc_pouch_query_index_hash_bytes(delete_hash, bytes, length);
  cursor = bytes;
  while ((line = lc_pouch_query_index_manifest_next_line(&cursor)) != NULL) {
    if (line[0] == '\0') {
      continue;
    }
    if (!lc_pouch_query_index_hex_token_valid(line)) {
      lc_free_with_allocator(&pouch->allocator, bytes);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query-index delete key is invalid", NULL, NULL,
                          "pouch");
    }
    rc = lc_pouch_query_index_key_hex_set_add(&pouch->allocator, set, line,
                                              error);
    if (rc != LC_OK) {
      lc_free_with_allocator(&pouch->allocator, bytes);
      return rc;
    }
    ++*delete_count;
  }
  lc_free_with_allocator(&pouch->allocator, bytes);
  return LC_OK;
}

static int lc_pouch_query_index_doc_table_generation_load_artifact(
    lc_pouch *pouch, const char *namespace_name, const char *path,
    lc_pouch_generation expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, lc_pouch_index_doc_table *table,
    int *present, int *valid, lc_error *error) {
  char *bytes;
  size_t length;
  int artifact_valid;
  int rc;

  if (pouch == NULL || table == NULL || present == NULL || valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch doc-table generation load requires inputs", NULL,
                        NULL, NULL);
  }
  memset(table, 0, sizeof(*table));
  *present = 0;
  *valid = 0;
  bytes = NULL;
  length = 0U;
  artifact_valid = 0;
  rc = lc_pouch_query_index_read_packed_component_bytes(
      pouch, namespace_name, path, LC_POUCH_QUERY_INDEX_PACKED_DOC_TABLE,
      &bytes, &length, present, &artifact_valid, error);
  if (rc == LC_OK && *present && artifact_valid) {
    rc = lc_pouch_index_doc_table_generation_load_bytes(
        &pouch->allocator, &bytes, length, expected_index_seq,
        expected_row_count, expected_row_hash, table, valid, error);
  } else {
    *valid = artifact_valid;
  }
  lc_free_with_allocator(&pouch->allocator, bytes);
  return rc;
}

static int lc_pouch_query_index_doc_table_generation_validate_artifact(
    lc_pouch *pouch, const char *namespace_name, const char *path,
    lc_pouch_generation expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, int *present, int *valid,
    lc_error *error) {
  lc_pouch_index_doc_table table;
  int rc;

  memset(&table, 0, sizeof(table));
  rc = lc_pouch_query_index_doc_table_generation_load_artifact(
      pouch, namespace_name, path, expected_index_seq, expected_row_count,
      expected_row_hash, &table, present, valid, error);
  lc_pouch_index_doc_table_cleanup(&pouch->allocator, &table);
  return rc;
}

static int lc_pouch_query_index_term_generation_load_artifact(
    lc_pouch *pouch, const char *namespace_name, const char *path,
    lc_pouch_generation expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, lc_pouch_index_term_generation *generation,
    int *present, int *valid, lc_error *error) {
  char *bytes;
  size_t length;
  size_t component;
  int artifact_valid;
  int rc;

  if (pouch == NULL || generation == NULL || present == NULL || valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch term generation load requires inputs", NULL,
                        NULL, NULL);
  }
  memset(generation, 0, sizeof(*generation));
  *present = 0;
  *valid = 0;
  component = lc_pouch_query_index_packed_component_for_path(path);
  if (component == LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT ||
      component == LC_POUCH_QUERY_INDEX_PACKED_DOC_TABLE ||
      component == LC_POUCH_QUERY_INDEX_PACKED_DELETE) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch term generation path has unexpected component",
                        NULL, NULL, "pouch");
  }
  bytes = NULL;
  length = 0U;
  artifact_valid = 0;
  rc = lc_pouch_query_index_read_packed_component_bytes(
      pouch, namespace_name, path, component, &bytes, &length, present,
      &artifact_valid, error);
  if (rc == LC_OK && *present && artifact_valid) {
    rc = lc_pouch_index_term_generation_load_bytes(
        &pouch->allocator, bytes, length, expected_index_seq,
        expected_row_count, expected_row_hash, generation, valid, error);
  } else {
    *valid = artifact_valid;
  }
  lc_free_with_allocator(&pouch->allocator, bytes);
  return rc;
}

static int lc_pouch_query_index_term_generation_validate_artifact(
    lc_pouch *pouch, const char *namespace_name, const char *path,
    lc_pouch_generation expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, int *present, int *valid,
    lc_error *error) {
  lc_pouch_index_term_generation generation;
  int rc;

  memset(&generation, 0, sizeof(generation));
  rc = lc_pouch_query_index_term_generation_load_artifact(
      pouch, namespace_name, path, expected_index_seq, expected_row_count,
      expected_row_hash, &generation, present, valid, error);
  lc_pouch_index_term_generation_cleanup(&pouch->allocator, &generation);
  return rc;
}

static int lc_pouch_query_index_parse_header_line(const char *line,
                                                  const char *name,
                                                  unsigned long *out) {
  const char *value;
  char *end;
  unsigned long parsed;
  size_t name_len;

  if (line == NULL || name == NULL || out == NULL) {
    return 0;
  }
  name_len = strlen(name);
  if (strncmp(line, name, name_len) != 0 || line[name_len] != '=') {
    return 0;
  }
  value = line + name_len + 1U;
  if (*value == '\0' || *value == '\n') {
    return 0;
  }
  errno = 0;
  parsed = strtoul(value, &end, 10);
  if (errno != 0 || end == value || (*end != '\n' && *end != '\0')) {
    return 0;
  }
  *out = parsed;
  return 1;
}

static int lc_pouch_query_index_parse_header_u64(const char *line,
                                                 const char *name,
                                                 uint64_t *out) {
  const char *value;
  size_t name_len;
  size_t value_len;
  lc_u64 parsed;

  if (line == NULL || name == NULL || out == NULL) {
    return 0;
  }
  name_len = strlen(name);
  if (strncmp(line, name, name_len) != 0 || line[name_len] != '=') {
    return 0;
  }
  value = line + name_len + 1U;
  if (*value == '\0' || *value == '\n') {
    return 0;
  }
  value_len = strcspn(value, "\n");
  if (value_len == 0U ||
      (value[value_len] != '\0' && value[value_len] != '\n') ||
      !lc_parse_u64_base10_range_checked(value, value_len, &parsed)) {
    return 0;
  }
  *out = (uint64_t)parsed;
  return 1;
}

static int lc_pouch_query_index_read_rows(
    FILE *fp, unsigned long row_count, unsigned long row_hash,
    lc_pouch_query_index_row_reader *reader, int *valid, lc_error *error) {
  static const char prefix[] = "row ";
  lc_pouch_query_index_text line;
  unsigned long computed_hash;
  unsigned long actual_rows;
  int got_line;
  int rc;

  memset(&line, 0, sizeof(line));
  line.allocator = reader != NULL ? reader->allocator : NULL;
  computed_hash = lc_pouch_query_index_hash_init();
  actual_rows = 0UL;
  rc = LC_OK;
  while (actual_rows < row_count) {
    rc = lc_pouch_query_index_read_line(fp, &line, &got_line, error);
    if (rc != LC_OK || !got_line) {
      if (rc == LC_OK) {
        *valid = 0;
      }
      break;
    }
    lc_pouch_query_index_hash_bytes(&computed_hash, line.bytes, line.length);
    if (line.length <= sizeof(prefix) - 1U ||
        line.bytes[line.length - 1U] != '\n' ||
        strncmp(line.bytes, prefix, sizeof(prefix) - 1U) != 0) {
      *valid = 0;
      break;
    }
    if (reader != NULL && reader->visit != NULL) {
      reader->row_index = actual_rows;
      rc = lc_pouch_query_index_parse_and_visit_row(line.bytes, reader, error);
      if (rc != LC_OK) {
        *valid = 0;
        break;
      }
    }
    ++actual_rows;
  }
  if (rc == LC_OK && (actual_rows != row_count || computed_hash != row_hash)) {
    *valid = 0;
  } else if (rc == LC_OK) {
    *valid = 1;
  }
  lc_free_with_allocator(line.allocator, line.bytes);
  return rc;
}

static int lc_pouch_query_index_read_terms(
    FILE *fp, unsigned long term_count, unsigned long term_hash,
    lc_pouch_query_index_term_reader *reader, int *valid, lc_error *error) {
  static const char prefix[] = "term ";
  lc_pouch_query_index_text line;
  unsigned long computed_hash;
  unsigned long actual_terms;
  int got_line;
  int allow_sorted_stop;
  int rc;

  memset(&line, 0, sizeof(line));
  line.allocator = reader != NULL ? reader->allocator : NULL;
  computed_hash = lc_pouch_query_index_hash_init();
  actual_terms = 0UL;
  allow_sorted_stop = reader != NULL && reader->visit != NULL;
  if (allow_sorted_stop) {
    reader->stop = 0;
  }
  rc = LC_OK;
  while (actual_terms < term_count) {
    rc = lc_pouch_query_index_read_line(fp, &line, &got_line, error);
    if (rc != LC_OK || !got_line) {
      if (rc == LC_OK) {
        *valid = 0;
      }
      break;
    }
    lc_pouch_query_index_hash_bytes(&computed_hash, line.bytes, line.length);
    if (line.length <= sizeof(prefix) - 1U ||
        line.bytes[line.length - 1U] != '\n' ||
        strncmp(line.bytes, prefix, sizeof(prefix) - 1U) != 0) {
      *valid = 0;
      break;
    }
    if (reader != NULL && reader->visit != NULL) {
      rc = lc_pouch_query_index_parse_and_visit_term(line.bytes, reader, error);
      if (rc == LC_POUCH_STATE_READ_MANY_STOP) {
        rc = LC_OK;
        reader->stop = 1;
      }
      if (rc != LC_OK) {
        *valid = 0;
        break;
      }
    }
    ++actual_terms;
    if (allow_sorted_stop && reader->stop) {
      break;
    }
  }
  if (rc == LC_OK && allow_sorted_stop && reader->stop) {
    *valid = 1;
  } else if (rc == LC_OK &&
             (actual_terms != term_count || computed_hash != term_hash)) {
    *valid = 0;
  } else if (rc == LC_OK) {
    *valid = 1;
  }
  lc_free_with_allocator(line.allocator, line.bytes);
  return rc;
}

static int
lc_pouch_query_index_read_terms_slice(FILE *fp, unsigned long term_count,
                                      lc_pouch_query_index_term_reader *reader,
                                      int *valid, lc_error *error) {
  static const char prefix[] = "term ";
  lc_pouch_query_index_text line;
  unsigned long actual_terms;
  int got_line;
  int rc;

  if (valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term slice read requires valid "
                        "output",
                        NULL, NULL, NULL);
  }
  memset(&line, 0, sizeof(line));
  line.allocator = reader != NULL ? reader->allocator : NULL;
  actual_terms = 0UL;
  rc = LC_OK;
  *valid = 0;
  if (reader != NULL) {
    reader->stop = 0;
  }
  while (actual_terms < term_count) {
    rc = lc_pouch_query_index_read_line(fp, &line, &got_line, error);
    if (rc != LC_OK || !got_line) {
      break;
    }
    if (line.length <= sizeof(prefix) - 1U ||
        line.bytes[line.length - 1U] != '\n' ||
        strncmp(line.bytes, prefix, sizeof(prefix) - 1U) != 0) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term has invalid prefix", NULL, NULL,
                        NULL);
      break;
    }
    if (reader != NULL && reader->visit != NULL) {
      rc = lc_pouch_query_index_parse_and_visit_term(line.bytes, reader, error);
      if (rc == LC_POUCH_STATE_READ_MANY_STOP) {
        rc = LC_OK;
        reader->stop = 1;
      }
      if (rc != LC_OK) {
        break;
      }
    }
    ++actual_terms;
    if (reader != NULL && reader->stop) {
      break;
    }
  }
  if (rc == LC_OK &&
      (actual_terms == term_count || (reader != NULL && reader->stop))) {
    *valid = 1;
  }
  lc_free_with_allocator(line.allocator, line.bytes);
  return rc;
}

static int lc_pouch_query_index_seek_term_slice(FILE *fp,
                                                uint64_t term_section_start,
                                                uint64_t first_byte,
                                                const lc_allocator *allocator,
                                                int *valid, lc_error *error) {
  uint64_t offset;
  off_t target;

  if (valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term seek requires valid output",
                        NULL, NULL, NULL);
  }
  *valid = 0;
  if (fp == NULL || first_byte > LC_U64_MAX - term_section_start) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term byte range is invalid", NULL,
                        NULL, NULL);
  }
  offset = term_section_start + first_byte;
  target = (off_t)offset;
  if (target < 0 || (uint64_t)target != offset) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term byte range exceeds platform "
                        "seek limit",
                        NULL, NULL, NULL);
  }
  if (fseeko(fp, target, SEEK_SET) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to seek pouch query-index term slice",
                        strerror(errno), NULL, NULL);
  }
  (void)allocator;
  *valid = 1;
  return LC_OK;
}

static int lc_pouch_query_index_read_term_fields(
    FILE *fp, unsigned long term_field_count, unsigned long term_count,
    const lc_allocator *allocator, lc_pouch_index_term_field **out_fields,
    size_t *out_count, int *valid, lc_error *error) {
  lc_pouch_query_index_text line;
  lc_pouch_index_term_field *fields;
  unsigned long actual_fields;
  unsigned long previous_end;
  uint64_t previous_byte_end;
  int got_line;
  int rc;

  if (valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term field read requires valid "
                        "output",
                        NULL, NULL, NULL);
  }
  if (out_fields != NULL) {
    *out_fields = NULL;
  }
  if (out_count != NULL) {
    *out_count = 0U;
  }
  *valid = 0;
  fields = NULL;
  if (term_field_count > 0UL && out_fields != NULL) {
    fields = (lc_pouch_index_term_field *)lc_alloc_with_allocator(
        allocator, (size_t)term_field_count * sizeof(*fields));
    if (fields == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query-index term fields",
                          NULL, NULL, NULL);
    }
    memset(fields, 0, (size_t)term_field_count * sizeof(*fields));
  }
  memset(&line, 0, sizeof(line));
  line.allocator = allocator;
  actual_fields = 0UL;
  previous_end = 0UL;
  previous_byte_end = 0U;
  rc = LC_OK;
  while (actual_fields < term_field_count) {
    lc_pouch_index_term_field parsed;

    memset(&parsed, 0, sizeof(parsed));
    rc = lc_pouch_query_index_read_line(fp, &line, &got_line, error);
    if (rc != LC_OK || !got_line) {
      break;
    }
    rc = lc_pouch_index_term_field_parse_line(line.bytes, &parsed, allocator,
                                              error);
    if (rc != LC_OK) {
      break;
    }
    if (parsed.line_count == 0UL || parsed.byte_count == 0U ||
        parsed.first_line < previous_end ||
        parsed.first_byte < previous_byte_end ||
        parsed.byte_count > LC_U64_MAX - parsed.first_byte ||
        parsed.first_line > term_count ||
        parsed.line_count > term_count - parsed.first_line ||
        (actual_fields > 0UL && fields != NULL &&
         strcmp(fields[actual_fields - 1UL].field_hex, parsed.field_hex) >=
             0)) {
      lc_free_with_allocator(allocator, parsed.field_hex);
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term field range is invalid", NULL,
                        NULL, NULL);
      break;
    }
    if (fields != NULL) {
      fields[actual_fields] = parsed;
    } else {
      lc_free_with_allocator(allocator, parsed.field_hex);
    }
    previous_end = parsed.first_line + parsed.line_count;
    previous_byte_end = parsed.first_byte + parsed.byte_count;
    ++actual_fields;
  }
  if (rc == LC_OK && actual_fields == term_field_count) {
    *valid = 1;
    if (out_fields != NULL) {
      *out_fields = fields;
      fields = NULL;
    }
    if (out_count != NULL) {
      *out_count = (size_t)actual_fields;
    }
  }
  lc_pouch_index_term_fields_cleanup(allocator, fields,
                                     (size_t)term_field_count);
  lc_free_with_allocator(line.allocator, line.bytes);
  return rc;
}

static int lc_pouch_query_index_read_term_values(
    FILE *fp, unsigned long term_value_count, unsigned long term_count,
    const lc_allocator *allocator, lc_pouch_index_term_value **out_values,
    size_t *out_count, int *valid, lc_error *error) {
  lc_pouch_query_index_text line;
  lc_pouch_index_term_value *values;
  unsigned long actual_values;
  unsigned long previous_end;
  uint64_t previous_byte_end;
  int got_line;
  int rc;

  if (valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term value read requires valid "
                        "output",
                        NULL, NULL, NULL);
  }
  if (out_values != NULL) {
    *out_values = NULL;
  }
  if (out_count != NULL) {
    *out_count = 0U;
  }
  *valid = 0;
  values = NULL;
  if (term_value_count > 0UL && out_values != NULL) {
    values = (lc_pouch_index_term_value *)lc_alloc_with_allocator(
        allocator, (size_t)term_value_count * sizeof(*values));
    if (values == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query-index term values",
                          NULL, NULL, NULL);
    }
    memset(values, 0, (size_t)term_value_count * sizeof(*values));
  }
  memset(&line, 0, sizeof(line));
  line.allocator = allocator;
  actual_values = 0UL;
  previous_end = 0UL;
  previous_byte_end = 0U;
  rc = LC_OK;
  while (actual_values < term_value_count) {
    lc_pouch_index_term_value parsed;

    memset(&parsed, 0, sizeof(parsed));
    rc = lc_pouch_query_index_read_line(fp, &line, &got_line, error);
    if (rc != LC_OK || !got_line) {
      break;
    }
    rc = lc_pouch_index_term_value_parse_line(line.bytes, &parsed, allocator,
                                              error);
    if (rc != LC_OK) {
      break;
    }
    if (parsed.line_count == 0UL || parsed.byte_count == 0U ||
        parsed.first_line < previous_end ||
        parsed.first_byte < previous_byte_end ||
        parsed.byte_count > LC_U64_MAX - parsed.first_byte ||
        parsed.first_line > term_count ||
        parsed.line_count > term_count - parsed.first_line ||
        (actual_values > 0UL && values != NULL &&
         (strcmp(values[actual_values - 1UL].field_hex, parsed.field_hex) > 0 ||
          (strcmp(values[actual_values - 1UL].field_hex, parsed.field_hex) ==
               0 &&
           strcmp(values[actual_values - 1UL].value_hex, parsed.value_hex) >=
               0)))) {
      lc_free_with_allocator(allocator, parsed.field_hex);
      lc_free_with_allocator(allocator, parsed.value_hex);
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term value range is invalid", NULL,
                        NULL, NULL);
      break;
    }
    if (values != NULL) {
      values[actual_values] = parsed;
    } else {
      lc_free_with_allocator(allocator, parsed.field_hex);
      lc_free_with_allocator(allocator, parsed.value_hex);
    }
    previous_end = parsed.first_line + parsed.line_count;
    previous_byte_end = parsed.first_byte + parsed.byte_count;
    ++actual_values;
  }
  if (rc == LC_OK && actual_values == term_value_count) {
    *valid = 1;
    if (out_values != NULL) {
      *out_values = values;
      values = NULL;
    }
    if (out_count != NULL) {
      *out_count = (size_t)actual_values;
    }
  }
  lc_pouch_index_term_values_cleanup(allocator, values,
                                     (size_t)term_value_count);
  lc_free_with_allocator(line.allocator, line.bytes);
  return rc;
}

static int lc_pouch_query_index_term_reader_range(
    const lc_pouch_index_term_field *fields, size_t field_count,
    const lc_pouch_query_index_term_reader *reader, unsigned long term_count,
    uint64_t term_byte_count, unsigned long *first_line,
    unsigned long *line_count, uint64_t *first_byte, uint64_t *byte_count) {
  if (reader == NULL || first_line == NULL || line_count == NULL) {
    return 0;
  }
  if (lc_pouch_query_index_field_hex_is_any_text(reader->field_hex)) {
    *first_line = 0UL;
    *line_count = term_count;
    if (first_byte != NULL) {
      *first_byte = 0U;
    }
    if (byte_count != NULL) {
      *byte_count = term_byte_count;
    }
    return 1;
  }
  return lc_pouch_index_term_fields_select_range(
      fields, field_count, reader->field_hex, NULL, 0U, term_count,
      term_byte_count, first_line, line_count, first_byte, byte_count);
}

static int lc_pouch_query_index_read_presences(
    FILE *fp, unsigned long presence_count, unsigned long presence_hash,
    lc_pouch_query_index_presence_reader *reader, int *valid, lc_error *error) {
  static const char prefix[] = "present ";
  lc_pouch_query_index_text line;
  unsigned long computed_hash;
  unsigned long actual_presences;
  int got_line;
  int rc;

  memset(&line, 0, sizeof(line));
  line.allocator = reader != NULL ? reader->allocator : NULL;
  computed_hash = lc_pouch_query_index_hash_init();
  actual_presences = 0UL;
  rc = LC_OK;
  while (actual_presences < presence_count) {
    rc = lc_pouch_query_index_read_line(fp, &line, &got_line, error);
    if (rc != LC_OK || !got_line) {
      if (rc == LC_OK) {
        *valid = 0;
      }
      break;
    }
    lc_pouch_query_index_hash_bytes(&computed_hash, line.bytes, line.length);
    if (line.length <= sizeof(prefix) - 1U ||
        line.bytes[line.length - 1U] != '\n' ||
        strncmp(line.bytes, prefix, sizeof(prefix) - 1U) != 0) {
      *valid = 0;
      break;
    }
    if (reader != NULL && reader->visit != NULL) {
      rc = lc_pouch_query_index_parse_and_visit_presence(line.bytes, reader,
                                                         error);
      if (rc != LC_OK) {
        *valid = 0;
        break;
      }
    }
    ++actual_presences;
  }
  if (rc == LC_OK &&
      (actual_presences != presence_count || computed_hash != presence_hash)) {
    *valid = 0;
  } else if (rc == LC_OK) {
    *valid = 1;
  }
  lc_free_with_allocator(line.allocator, line.bytes);
  return rc;
}

static const lc_allocator *lc_pouch_query_index_reader_allocator(
    lc_pouch_query_index_row_reader *reader,
    lc_pouch_query_index_term_reader *term_reader,
    lc_pouch_query_index_presence_reader *presence_reader) {
  if (reader != NULL) {
    return reader->allocator;
  }
  if (term_reader != NULL) {
    return term_reader->allocator;
  }
  if (presence_reader != NULL) {
    return presence_reader->allocator;
  }
  return NULL;
}

static int lc_pouch_query_index_skip_lines(FILE *fp, unsigned long count,
                                           const lc_allocator *allocator,
                                           int *valid, lc_error *error) {
  lc_pouch_query_index_text line;
  unsigned long actual;
  int got_line;
  int rc;

  if (valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index skip requires valid output", NULL,
                        NULL, NULL);
  }
  memset(&line, 0, sizeof(line));
  line.allocator = allocator;
  actual = 0UL;
  rc = LC_OK;
  *valid = 0;
  while (actual < count) {
    rc = lc_pouch_query_index_read_line(fp, &line, &got_line, error);
    if (rc != LC_OK || !got_line) {
      break;
    }
    ++actual;
  }
  if (rc == LC_OK && actual == count) {
    *valid = 1;
  }
  lc_free_with_allocator(allocator, line.bytes);
  return rc;
}

static int lc_pouch_query_index_read_with_reader_fp(
    FILE *fp, lc_pouch_query_index_read_result *out,
    lc_pouch_query_index_row_reader *reader,
    lc_pouch_query_index_term_reader *term_reader,
    lc_pouch_query_index_presence_reader *presence_reader, lc_error *error) {
  char format[64];
  char line[256];
  unsigned long version;
  lc_pouch_generation parsed_seq;
  unsigned long row_count;
  unsigned long row_hash;
  unsigned long term_count;
  unsigned long term_hash;
  unsigned long term_index_complete;
  unsigned long term_field_count;
  unsigned long term_value_count;
  unsigned long presence_count;
  unsigned long presence_hash;
  unsigned long presence_index_complete;
  uint64_t first_byte;
  uint64_t byte_count;
  uint64_t term_byte_count;
  lc_pouch_index_term_field *term_fields;
  lc_pouch_index_term_value *term_values;
  size_t term_field_table_count;
  size_t term_value_table_count;
  uint64_t term_section_start;
  int matched;
  int row_valid;
  int term_valid;
  int term_field_valid;
  int term_value_valid;
  int presence_valid;
  int full_read;
  int want_rows;
  int want_terms;
  int want_presences;
  int rc;

  if (fp == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index read requires a stream and output",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  memset(format, 0, sizeof(format));
  version = 0UL;
  parsed_seq = 0UL;
  row_count = 0UL;
  row_hash = 0UL;
  term_count = 0UL;
  term_hash = 0UL;
  term_index_complete = 0UL;
  term_field_count = 0UL;
  term_value_count = 0UL;
  presence_count = 0UL;
  presence_hash = 0UL;
  presence_index_complete = 0UL;
  first_byte = 0U;
  byte_count = 0U;
  term_byte_count = 0U;
  term_fields = NULL;
  term_values = NULL;
  term_field_table_count = 0U;
  term_value_table_count = 0U;
  term_section_start = 0U;
  matched = 0;
  if (fgets(line, sizeof(line), fp) != NULL &&
      sscanf(line, "format=%63s\n", format) == 1) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "version", &version)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_u64(line, "state_index_seq",
                                            &parsed_seq)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "row_count", &row_count)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "summary_hash", &row_hash)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_count", &term_count)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_hash", &term_hash)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_index_complete",
                                             &term_index_complete) &&
      term_index_complete <= 1UL) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "presence_count",
                                             &presence_count)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "presence_hash",
                                             &presence_hash)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "presence_index_complete",
                                             &presence_index_complete) &&
      presence_index_complete <= 1UL) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_field_count",
                                             &term_field_count)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_value_count",
                                             &term_value_count)) {
    ++matched;
  }
  out->present = 1;
  if (matched == 13 && strcmp(format, LC_POUCH_QUERY_INDEX_FORMAT) == 0 &&
      version == LC_POUCH_QUERY_INDEX_VERSION) {
    out->index_seq = parsed_seq;
    out->row_count = row_count;
    out->row_hash = row_hash;
    out->term_count = term_count;
    out->term_hash = term_hash;
    out->term_field_count = term_field_count;
    out->term_value_count = term_value_count;
    out->presence_count = presence_count;
    out->presence_hash = presence_hash;
    out->term_index_complete = term_index_complete != 0UL;
    out->presence_index_complete = presence_index_complete != 0UL;
    full_read =
        reader == NULL && term_reader == NULL && presence_reader == NULL;
    want_rows = full_read || reader != NULL;
    want_terms = full_read || term_reader != NULL;
    want_presences = full_read || presence_reader != NULL;
    row_valid = !want_rows;
    term_valid = !want_terms;
    term_field_valid = 0;
    term_value_valid = 0;
    presence_valid = !want_presences;
    rc = lc_pouch_query_index_read_term_fields(
        fp, term_field_count, term_count,
        lc_pouch_query_index_reader_allocator(reader, term_reader,
                                              presence_reader),
        want_terms ? &term_fields : NULL,
        want_terms ? &term_field_table_count : NULL, &term_field_valid, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_read_term_values(
          fp, term_value_count, term_count,
          lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                presence_reader),
          want_terms ? &term_values : NULL,
          want_terms ? &term_value_table_count : NULL, &term_value_valid,
          error);
    }
    if (rc == LC_OK) {
      off_t term_position;

      term_position = ftello(fp);
      if (term_position < 0) {
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to locate pouch query-index term section",
                          strerror(errno), NULL, NULL);
      } else {
        term_section_start = (uint64_t)term_position;
      }
    }
    if (rc == LC_OK && term_field_table_count > 0U) {
      lc_pouch_index_term_field *last_field;

      last_field = &term_fields[term_field_table_count - 1U];
      if (last_field->byte_count > LC_U64_MAX - last_field->first_byte) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query-index term byte range is invalid", NULL,
                          NULL, NULL);
      } else {
        term_byte_count = last_field->first_byte + last_field->byte_count;
      }
    }
    if (rc == LC_OK && want_terms) {
      unsigned long first_line;
      unsigned long line_count;

      first_line = 0UL;
      line_count = 0UL;
      first_byte = 0U;
      byte_count = 0U;
      if (rc == LC_OK && term_reader != NULL &&
          lc_pouch_query_index_term_reader_range(
              term_fields, term_field_table_count, term_reader, term_count,
              term_byte_count, &first_line, &line_count, &first_byte,
              &byte_count)) {
        rc = lc_pouch_query_index_seek_term_slice(
            fp, term_section_start, first_byte,
            lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                  presence_reader),
            &term_valid, error);
        if (rc == LC_OK && term_valid) {
          rc = lc_pouch_query_index_read_terms_slice(
              fp, line_count, term_reader, &term_valid, error);
        }
        if (rc == LC_OK && (want_rows || want_presences) && term_valid) {
          rc = lc_pouch_query_index_seek_term_slice(
              fp, term_section_start, term_byte_count,
              lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                    presence_reader),
              &term_valid, error);
        }
      } else if (rc == LC_OK && term_reader != NULL && want_rows) {
        rc = lc_pouch_query_index_seek_term_slice(
            fp, term_section_start, 0U,
            lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                  presence_reader),
            &term_valid, error);
        if (rc == LC_OK && term_valid) {
          rc = lc_pouch_query_index_skip_lines(
              fp, term_count,
              lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                    presence_reader),
              &term_valid, error);
        }
      } else if (term_reader != NULL) {
        term_valid = 1;
        if (want_rows || want_presences) {
          rc = lc_pouch_query_index_skip_lines(
              fp, term_count,
              lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                    presence_reader),
              &term_valid, error);
        }
      } else {
        rc = lc_pouch_query_index_read_terms(fp, term_count, term_hash,
                                             term_reader, &term_valid, error);
      }
    } else if (rc == LC_OK && (want_rows || want_presences)) {
      rc = lc_pouch_query_index_seek_term_slice(
          fp, term_section_start, 0U,
          lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                presence_reader),
          &term_valid, error);
    }
    if (rc == LC_OK && !want_terms && (want_rows || want_presences)) {
      rc = lc_pouch_query_index_skip_lines(
          fp, term_count,
          lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                presence_reader),
          &term_valid, error);
    }
    if (want_rows) {
      if (rc == LC_OK) {
        rc = lc_pouch_query_index_read_rows(fp, row_count, row_hash, reader,
                                            &row_valid, error);
      }
    } else if (rc == LC_OK && want_presences) {
      rc = lc_pouch_query_index_skip_lines(
          fp, row_count,
          lc_pouch_query_index_reader_allocator(reader, term_reader,
                                                presence_reader),
          &row_valid, error);
    }
    if (rc == LC_OK && want_presences) {
      rc = lc_pouch_query_index_read_presences(fp, presence_count,
                                               presence_hash, presence_reader,
                                               &presence_valid, error);
    }
    if (rc == LC_OK && full_read) {
      int got_extra;
      lc_pouch_query_index_text extra;

      memset(&extra, 0, sizeof(extra));
      extra.allocator = lc_pouch_query_index_reader_allocator(
          reader, term_reader, presence_reader);
      rc = lc_pouch_query_index_read_line(fp, &extra, &got_extra, error);
      if (rc == LC_OK && got_extra) {
        presence_valid = 0;
      }
      lc_free_with_allocator(extra.allocator, extra.bytes);
    }
    if (rc == LC_OK) {
      out->valid = row_valid && term_valid && term_field_valid &&
                   term_value_valid && presence_valid;
    }
  } else {
    rc = LC_OK;
  }
  lc_pouch_index_term_fields_cleanup(lc_pouch_query_index_reader_allocator(
                                         reader, term_reader, presence_reader),
                                     term_fields, term_field_table_count);
  lc_pouch_index_term_values_cleanup(lc_pouch_query_index_reader_allocator(
                                         reader, term_reader, presence_reader),
                                     term_values, term_value_table_count);
  return rc;
}

static int lc_pouch_query_index_read_with_reader_file(
    const char *path, lc_pouch_query_index_read_result *out,
    lc_pouch_query_index_row_reader *reader,
    lc_pouch_query_index_term_reader *term_reader,
    lc_pouch_query_index_presence_reader *presence_reader, lc_error *error) {
  FILE *fp;
  int rc;

  fp = fopen(path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      memset(out, 0, sizeof(*out));
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch query-index header artifact",
                        strerror(errno), NULL, NULL);
  }
  rc = lc_pouch_query_index_read_with_reader_fp(fp, out, reader, term_reader,
                                                presence_reader, error);
  if (fclose(fp) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch query-index header artifact",
                        strerror(errno), NULL, NULL);
  }
  return rc;
}

static int lc_pouch_query_index_read_with_reader(
    lc_pouch *pouch, const char *namespace_name, const char *path,
    lc_pouch_query_index_read_result *out,
    lc_pouch_query_index_row_reader *reader,
    lc_pouch_query_index_term_reader *term_reader,
    lc_pouch_query_index_presence_reader *presence_reader, lc_error *error) {
  char *bytes;
  size_t length;
  FILE *fp;
  int present;
  int valid;
  int rc;

  if (!lc_pouch_crypto_enabled(pouch != NULL ? pouch->crypto : NULL)) {
    return lc_pouch_query_index_read_with_reader_file(
        path, out, reader, term_reader, presence_reader, error);
  }
  bytes = NULL;
  length = 0U;
  present = 0;
  valid = 0;
  rc = lc_pouch_query_index_read_encrypted_artifact_bytes(
      pouch, namespace_name, path, 1, &bytes, &length, &present, &valid, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(out, 0, sizeof(*out));
  out->present = present;
  if (!present || !valid) {
    lc_free_with_allocator(&pouch->allocator, bytes);
    return LC_OK;
  }
  fp = fmemopen(bytes, length, "rb");
  if (fp == NULL) {
    lc_free_with_allocator(&pouch->allocator, bytes);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to open encrypted pouch query-index memory",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_query_index_read_with_reader_fp(fp, out, reader, term_reader,
                                                presence_reader, error);
  if (fclose(fp) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close encrypted pouch query-index memory",
                      strerror(errno), NULL, NULL);
  }
  lc_free_with_allocator(&pouch->allocator, bytes);
  return rc;
}

static int lc_pouch_query_index_read_header_fp(
    FILE *fp, lc_pouch_query_index_read_result *out, lc_error *error) {
  char format[64];
  char line[256];
  unsigned long version;
  lc_pouch_generation parsed_seq;
  unsigned long row_count;
  unsigned long row_hash;
  unsigned long term_count;
  unsigned long term_hash;
  unsigned long term_index_complete;
  unsigned long term_field_count;
  unsigned long term_value_count;
  unsigned long presence_count;
  unsigned long presence_hash;
  unsigned long presence_index_complete;
  int matched;
  int rc;

  if (fp == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index header read requires a stream and "
                        "output",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  memset(format, 0, sizeof(format));
  version = 0UL;
  parsed_seq = 0UL;
  row_count = 0UL;
  row_hash = 0UL;
  term_count = 0UL;
  term_hash = 0UL;
  term_index_complete = 0UL;
  term_field_count = 0UL;
  term_value_count = 0UL;
  presence_count = 0UL;
  presence_hash = 0UL;
  presence_index_complete = 0UL;
  matched = 0;
  if (fgets(line, sizeof(line), fp) != NULL &&
      sscanf(line, "format=%63s\n", format) == 1) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "version", &version)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_u64(line, "state_index_seq",
                                            &parsed_seq)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "row_count", &row_count)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "summary_hash", &row_hash)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_count", &term_count)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_hash", &term_hash)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_index_complete",
                                             &term_index_complete) &&
      term_index_complete <= 1UL) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "presence_count",
                                             &presence_count)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "presence_hash",
                                             &presence_hash)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "presence_index_complete",
                                             &presence_index_complete) &&
      presence_index_complete <= 1UL) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_field_count",
                                             &term_field_count)) {
    ++matched;
  }
  if (fgets(line, sizeof(line), fp) != NULL &&
      lc_pouch_query_index_parse_header_line(line, "term_value_count",
                                             &term_value_count)) {
    ++matched;
  }
  out->present = 1;
  rc = LC_OK;
  if (matched == 13 && strcmp(format, LC_POUCH_QUERY_INDEX_FORMAT) == 0 &&
      version == LC_POUCH_QUERY_INDEX_VERSION) {
    out->index_seq = parsed_seq;
    out->row_count = row_count;
    out->row_hash = row_hash;
    out->term_count = term_count;
    out->term_hash = term_hash;
    out->term_field_count = term_field_count;
    out->term_value_count = term_value_count;
    out->presence_count = presence_count;
    out->presence_hash = presence_hash;
    out->term_index_complete = term_index_complete != 0UL;
    out->presence_index_complete = presence_index_complete != 0UL;
    out->valid = 1;
  }
  return rc;
}

static int lc_pouch_query_index_read_header_file(
    const char *path, lc_pouch_query_index_read_result *out, lc_error *error) {
  FILE *fp;
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index header read requires an output",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  fp = fopen(path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch query-index header artifact",
                        strerror(errno), NULL, NULL);
  }
  rc = lc_pouch_query_index_read_header_fp(fp, out, error);
  if (fclose(fp) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch query-index header artifact",
                        strerror(errno), NULL, NULL);
  }
  return rc;
}

static int lc_pouch_query_index_read_header(
    lc_pouch *pouch, const char *namespace_name, const char *path,
    lc_pouch_query_index_read_result *out, lc_error *error) {
  (void)pouch;
  (void)namespace_name;
  return lc_pouch_query_index_read_header_file(path, out, error);
}

static int lc_pouch_query_index_exact_generation_flush_posting(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    unsigned long term_id, const lc_pouch_index_docid_set *docids,
    lc_error *error) {
  if (term_id == 0UL || docids == NULL || docids->count == 0U) {
    return LC_OK;
  }
  return lc_pouch_index_term_posting_table_append_sorted_unique_trusted(
      allocator, &generation->postings, term_id, docids->items, docids->count,
      error);
}

static int lc_pouch_query_index_docid_compare(const void *left,
                                              const void *right) {
  const unsigned long *a;
  const unsigned long *b;

  a = (const unsigned long *)left;
  b = (const unsigned long *)right;
  if (*a < *b) {
    return -1;
  }
  if (*a > *b) {
    return 1;
  }
  return 0;
}

static void lc_pouch_query_index_exact_generation_docids_normalize(
    lc_pouch_query_index_exact_generation_docids *item) {
  size_t read_index;
  size_t write_index;

  if (item == NULL || item->docids.count <= 1U) {
    return;
  }
  if (item->docids_need_sort) {
    qsort(item->docids.items, item->docids.count, sizeof(item->docids.items[0]),
          lc_pouch_query_index_docid_compare);
  }
  write_index = 1U;
  for (read_index = 1U; read_index < item->docids.count; ++read_index) {
    if (item->docids.items[read_index] ==
        item->docids.items[write_index - 1U]) {
      continue;
    }
    if (write_index != read_index) {
      item->docids.items[write_index] = item->docids.items[read_index];
    }
    ++write_index;
  }
  item->docids.count = write_index;
  item->docids_need_sort = 0;
}

static void lc_pouch_query_index_exact_generation_accumulator_cleanup(
    const lc_allocator *allocator,
    lc_pouch_query_index_exact_generation_accumulator *accumulator) {
  size_t index;

  if (accumulator == NULL) {
    return;
  }
  for (index = 0U; index < accumulator->count; ++index) {
    if (!accumulator->items[index].using_inline_docids) {
      lc_pouch_index_docid_set_cleanup(allocator,
                                       &accumulator->items[index].docids);
    } else {
      memset(&accumulator->items[index].docids, 0,
             sizeof(accumulator->items[index].docids));
      accumulator->items[index].using_inline_docids = 0;
    }
  }
  lc_free_with_allocator(allocator, accumulator->items);
  lc_free_with_allocator(allocator, accumulator->term_positions);
  memset(accumulator, 0, sizeof(*accumulator));
}

static void lc_pouch_query_index_exact_generation_accumulator_rebind_inline(
    lc_pouch_query_index_exact_generation_accumulator *accumulator) {
  size_t index;

  if (accumulator == NULL) {
    return;
  }
  for (index = 0U; index < accumulator->count; ++index) {
    if (accumulator->items[index].using_inline_docids) {
      accumulator->items[index].docids.items =
          accumulator->items[index].inline_docids;
      accumulator->items[index].docids.capacity =
          LC_POUCH_QUERY_INDEX_INLINE_DOCIDS;
    }
  }
}

static int lc_pouch_query_index_exact_generation_docids_reserve(
    const lc_allocator *allocator,
    lc_pouch_query_index_exact_generation_docids *item, size_t needed,
    lc_error *error) {
  unsigned long *next_items;
  size_t next_capacity;

  if (item == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch exact generation docID reserve requires item",
                        NULL, NULL, NULL);
  }
  if (item->docids.items == NULL) {
    item->docids.items = item->inline_docids;
    item->docids.capacity = LC_POUCH_QUERY_INDEX_INLINE_DOCIDS;
    item->using_inline_docids = 1;
  }
  if (needed <= item->docids.capacity) {
    return LC_OK;
  }
  next_capacity = item->docids.capacity == 0U ? 8U : item->docids.capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch exact generation docID set exceeds local "
                          "limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_items = (unsigned long *)lc_alloc_with_allocator(
      allocator, next_capacity * sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch exact generation docID set",
                        NULL, NULL, NULL);
  }
  if (item->docids.count > 0U) {
    memcpy(next_items, item->docids.items,
           item->docids.count * sizeof(*next_items));
  }
  if (!item->using_inline_docids) {
    lc_free_with_allocator(allocator, item->docids.items);
  }
  item->docids.items = next_items;
  item->docids.capacity = next_capacity;
  item->using_inline_docids = 0;
  return LC_OK;
}

static int lc_pouch_query_index_exact_generation_docids_append_unique(
    const lc_allocator *allocator,
    lc_pouch_query_index_exact_generation_docids *item, unsigned long doc_id,
    lc_error *error) {
  int rc;

  if (item == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch exact generation docID append requires item",
                        NULL, NULL, NULL);
  }
  if (item->docids.count > 0U) {
    if (doc_id == item->docids.items[item->docids.count - 1U]) {
      return LC_OK;
    }
    if (doc_id < item->docids.items[item->docids.count - 1U]) {
      item->docids_need_sort = 1;
    }
  }
  rc = lc_pouch_query_index_exact_generation_docids_reserve(
      allocator, item, item->docids.count + 1U, error);
  if (rc != LC_OK) {
    return rc;
  }
  item->docids.items[item->docids.count] = doc_id;
  ++item->docids.count;
  return LC_OK;
}

static void lc_pouch_query_index_exact_generation_docids_remove(
    lc_pouch_query_index_exact_generation_docids *item, unsigned long doc_id) {
  size_t index;
  size_t write_index;

  if (item == NULL || item->docids.count == 0U) {
    return;
  }
  write_index = 0U;
  for (index = 0U; index < item->docids.count; ++index) {
    if (item->docids.items[index] == doc_id) {
      continue;
    }
    if (write_index != index) {
      item->docids.items[write_index] = item->docids.items[index];
    }
    ++write_index;
  }
  item->docids.count = write_index;
}

static int lc_pouch_query_index_exact_generation_accumulator_position_reserve(
    const lc_allocator *allocator,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    unsigned long term_id, lc_error *error) {
  size_t *next_positions;
  size_t next_capacity;

  if (accumulator == NULL || term_id == 0UL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch exact generation accumulator position cache "
                        "requires accumulator and term id",
                        NULL, NULL, NULL);
  }
  if (term_id < (unsigned long)accumulator->term_position_capacity) {
    return LC_OK;
  }
  next_capacity = accumulator->term_position_capacity == 0U
                      ? 256U
                      : accumulator->term_position_capacity;
  while (term_id >= (unsigned long)next_capacity) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch exact generation accumulator position cache "
                          "exceeds local limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_positions = (size_t *)lc_calloc_with_allocator(allocator, next_capacity,
                                                      sizeof(*next_positions));
  if (next_positions == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch exact generation "
                        "accumulator position cache",
                        NULL, NULL, NULL);
  }
  if (accumulator->term_positions != NULL) {
    memcpy(next_positions, accumulator->term_positions,
           accumulator->term_position_capacity * sizeof(*next_positions));
    lc_free_with_allocator(allocator, accumulator->term_positions);
  }
  accumulator->term_positions = next_positions;
  accumulator->term_position_capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_query_index_exact_generation_accumulator_reserve(
    const lc_allocator *allocator,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    size_t needed, lc_error *error) {
  lc_pouch_query_index_exact_generation_docids *next_items;
  size_t next_capacity;

  if (accumulator == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch exact generation accumulator requires "
                        "accumulator",
                        NULL, NULL, NULL);
  }
  if (needed <= accumulator->capacity) {
    return LC_OK;
  }
  next_capacity = accumulator->capacity == 0U ? 16U : accumulator->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch exact generation accumulator exceeds local "
                          "limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_items =
      (lc_pouch_query_index_exact_generation_docids *)lc_alloc_with_allocator(
          allocator, next_capacity * sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch exact generation "
                        "accumulator",
                        NULL, NULL, NULL);
  }
  if (accumulator->items != NULL) {
    memcpy(next_items, accumulator->items,
           accumulator->count * sizeof(next_items[0]));
    lc_free_with_allocator(allocator, accumulator->items);
  }
  memset(next_items + accumulator->count, 0,
         (next_capacity - accumulator->count) * sizeof(next_items[0]));
  accumulator->items = next_items;
  accumulator->capacity = next_capacity;
  lc_pouch_query_index_exact_generation_accumulator_rebind_inline(accumulator);
  return LC_OK;
}

static int lc_pouch_query_index_exact_generation_accumulator_find(
    const lc_pouch_query_index_exact_generation_accumulator *accumulator,
    unsigned long term_id, size_t *position_out) {
  size_t low;
  size_t high;
  size_t mid;

  if (position_out != NULL) {
    *position_out = 0U;
  }
  if (accumulator == NULL || term_id == 0UL) {
    return 0;
  }
  low = 0U;
  high = accumulator->count;
  while (low < high) {
    mid = low + ((high - low) / 2U);
    if (accumulator->items[mid].term_id < term_id) {
      low = mid + 1U;
    } else {
      high = mid;
    }
  }
  if (position_out != NULL) {
    *position_out = low;
  }
  return low < accumulator->count && accumulator->items[low].term_id == term_id;
}

static int lc_pouch_query_index_exact_generation_accumulator_append(
    const lc_allocator *allocator,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    unsigned long term_id, unsigned long doc_id, lc_error *error) {
  size_t position;
  int rc;

  if (term_id == 0UL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch exact generation accumulator requires term id",
                        NULL, NULL, "pouch");
  }
  position = 0U;
  if (term_id < (unsigned long)accumulator->term_position_capacity &&
      accumulator->term_positions[term_id] > 0U &&
      accumulator->term_positions[term_id] - 1U < accumulator->count &&
      accumulator->items[accumulator->term_positions[term_id] - 1U].term_id ==
          term_id) {
    position = accumulator->term_positions[term_id] - 1U;
  } else if (!lc_pouch_query_index_exact_generation_accumulator_find(
                 accumulator, term_id, &position)) {
    rc = lc_pouch_query_index_exact_generation_accumulator_reserve(
        allocator, accumulator, accumulator->count + 1U, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (position < accumulator->count) {
      memmove(&accumulator->items[position + 1U], &accumulator->items[position],
              (accumulator->count - position) * sizeof(accumulator->items[0]));
      lc_pouch_query_index_exact_generation_accumulator_rebind_inline(
          accumulator);
      memset(&accumulator->items[position], 0,
             sizeof(accumulator->items[position]));
    }
    accumulator->items[position].term_id = term_id;
    ++accumulator->count;
  }
  rc = lc_pouch_query_index_exact_generation_accumulator_position_reserve(
      allocator, accumulator, term_id, error);
  if (rc != LC_OK) {
    return rc;
  }
  accumulator->term_positions[term_id] = position + 1U;
  return lc_pouch_query_index_exact_generation_docids_append_unique(
      allocator, &accumulator->items[position], doc_id, error);
}

static void lc_pouch_query_index_exact_generation_accumulator_remove(
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    unsigned long term_id, unsigned long doc_id) {
  size_t position;

  if (accumulator == NULL || term_id == 0UL) {
    return;
  }
  position = 0U;
  if (term_id < (unsigned long)accumulator->term_position_capacity &&
      accumulator->term_positions[term_id] > 0U &&
      accumulator->term_positions[term_id] - 1U < accumulator->count &&
      accumulator->items[accumulator->term_positions[term_id] - 1U].term_id ==
          term_id) {
    position = accumulator->term_positions[term_id] - 1U;
  } else if (!lc_pouch_query_index_exact_generation_accumulator_find(
                 accumulator, term_id, &position)) {
    return;
  }
  lc_pouch_query_index_exact_generation_docids_remove(
      &accumulator->items[position], doc_id);
}

static void lc_pouch_query_index_trigram_value_set_cleanup(
    const lc_allocator *allocator,
    lc_pouch_query_index_trigram_value_set *set) {
  if (set == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, set->items);
  memset(set, 0, sizeof(*set));
}

static int lc_pouch_query_index_trigram_value_set_reserve(
    const lc_allocator *allocator, lc_pouch_query_index_trigram_value_set *set,
    size_t needed, lc_error *error) {
  unsigned long *next_items;
  size_t next_capacity;

  if (set == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch trigram set reserve requires set", NULL, NULL,
                        NULL);
  }
  if (needed <= set->capacity) {
    return LC_OK;
  }
  next_capacity = set->capacity == 0U ? 64U : set->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch trigram set exceeds local limit", NULL, NULL,
                          NULL);
    }
    next_capacity *= 2U;
  }
  if (next_capacity > ((size_t)-1 / sizeof(*next_items))) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch trigram set exceeds local limit", NULL, NULL,
                        NULL);
  }
  next_items = (unsigned long *)lc_alloc_with_allocator(
      allocator, next_capacity * sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch trigram set", NULL, NULL,
                        NULL);
  }
  if (set->items != NULL) {
    memcpy(next_items, set->items, set->count * sizeof(*next_items));
    lc_free_with_allocator(allocator, set->items);
  }
  memset(next_items + set->count, 0,
         (next_capacity - set->count) * sizeof(*next_items));
  set->items = next_items;
  set->capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_query_index_trigram_value_set_add(
    const lc_allocator *allocator, lc_pouch_query_index_trigram_value_set *set,
    unsigned long key, lc_error *error) {
  size_t position;
  int rc;

  if (key > 0xffffffUL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch trigram set requires a 24-bit trigram key", NULL,
                        NULL, "pouch");
  }
  position = 0U;
  if (set != NULL) {
    size_t low;
    size_t high;

    low = 0U;
    high = set->count;
    while (low < high) {
      size_t mid;

      mid = low + ((high - low) / 2U);
      if (set->items[mid] < key) {
        low = mid + 1U;
      } else if (set->items[mid] > key) {
        high = mid;
      } else {
        return LC_OK;
      }
    }
    position = low;
  }
  rc = lc_pouch_query_index_trigram_value_set_reserve(allocator, set,
                                                      set->count + 1U, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (position < set->count) {
    memmove(&set->items[position + 1U], &set->items[position],
            (set->count - position) * sizeof(set->items[0]));
  }
  set->items[position] = key;
  ++set->count;
  return LC_OK;
}

static void lc_pouch_query_index_trigram_key_hex(unsigned long key,
                                                 char out[7]) {
  lc_pouch_query_index_hex_encode_byte(out,
                                       (unsigned char)((key >> 16U) & 0xffU));
  lc_pouch_query_index_hex_encode_byte(out + 2U,
                                       (unsigned char)((key >> 8U) & 0xffU));
  lc_pouch_query_index_hex_encode_byte(out + 4U, (unsigned char)(key & 0xffU));
  out[6] = '\0';
}

static unsigned long
lc_pouch_query_index_trigram_field_hash(const char *field_hex) {
  unsigned long hash;

  hash = lc_pouch_query_index_hash_init();
  if (field_hex != NULL) {
    lc_pouch_query_index_hash_bytes(&hash, field_hex, strlen(field_hex));
  }
  return hash;
}

static void lc_pouch_query_index_field_cache_cleanup(
    const lc_allocator *allocator, lc_pouch_query_index_field_cache *cache) {
  size_t index;

  if (cache == NULL) {
    return;
  }
  for (index = 0U; index < cache->capacity; ++index) {
    if (cache->items[index].used) {
      lc_free_with_allocator(allocator, cache->items[index].field_hex);
    }
  }
  lc_free_with_allocator(allocator, cache->items);
  memset(cache, 0, sizeof(*cache));
}

static int lc_pouch_query_index_field_cache_reserve(
    const lc_allocator *allocator, lc_pouch_query_index_field_cache *cache,
    size_t needed, lc_error *error) {
  lc_pouch_query_index_field_cache_entry *next_items;
  size_t next_capacity;
  size_t index;

  if (cache == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch field cache reserve requires cache", NULL, NULL,
                        NULL);
  }
  if (needed * 2U <= cache->capacity) {
    return LC_OK;
  }
  next_capacity = cache->capacity == 0U ? 128U : cache->capacity;
  while (needed * 2U > next_capacity) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch field cache exceeds local limit", NULL, NULL,
                          NULL);
    }
    next_capacity *= 2U;
  }
  next_items =
      (lc_pouch_query_index_field_cache_entry *)lc_calloc_with_allocator(
          allocator, next_capacity, sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch field cache", NULL, NULL,
                        NULL);
  }
  if (cache->items != NULL) {
    for (index = 0U; index < cache->capacity; ++index) {
      size_t slot;

      if (!cache->items[index].used) {
        continue;
      }
      slot = (size_t)(cache->items[index].field_hash %
                      (unsigned long)next_capacity);
      while (next_items[slot].used) {
        slot = (slot + 1U) % next_capacity;
      }
      next_items[slot] = cache->items[index];
    }
    lc_free_with_allocator(allocator, cache->items);
  }
  cache->items = next_items;
  cache->capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_query_index_field_cache_find_or_add(
    const lc_allocator *allocator, lc_pouch_query_index_field_cache *cache,
    const char *field_hex, unsigned long *field_id_out,
    unsigned long *field_hash_out, lc_error *error) {
  char *field_hex_copy;
  unsigned long field_hash;
  size_t slot;
  int rc;

  if (field_id_out != NULL) {
    *field_id_out = 0UL;
  }
  if (field_hash_out != NULL) {
    *field_hash_out = 0UL;
  }
  if (cache == NULL || field_hex == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch field cache requires cache and field", NULL,
                        NULL, NULL);
  }
  field_hash = lc_pouch_query_index_trigram_field_hash(field_hex);
  rc = lc_pouch_query_index_field_cache_reserve(allocator, cache,
                                                cache->count + 1U, error);
  if (rc != LC_OK) {
    return rc;
  }
  slot = (size_t)(field_hash % (unsigned long)cache->capacity);
  while (cache->items[slot].used) {
    if (cache->items[slot].field_hash == field_hash &&
        strcmp(cache->items[slot].field_hex, field_hex) == 0) {
      if (field_id_out != NULL) {
        *field_id_out = cache->items[slot].field_id;
      }
      if (field_hash_out != NULL) {
        *field_hash_out = field_hash;
      }
      return LC_OK;
    }
    slot = (slot + 1U) % cache->capacity;
  }
  if (cache->next_field_id == 0UL) {
    cache->next_field_id = 1UL;
  }
  field_hex_copy = lc_strdup_with_allocator(allocator, field_hex);
  if (field_hex_copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch field cache key", NULL, NULL,
                        NULL);
  }
  cache->items[slot].field_hex = field_hex_copy;
  cache->items[slot].field_hash = field_hash;
  cache->items[slot].field_id = cache->next_field_id++;
  cache->items[slot].used = 1;
  ++cache->count;
  if (field_id_out != NULL) {
    *field_id_out = cache->items[slot].field_id;
  }
  if (field_hash_out != NULL) {
    *field_hash_out = field_hash;
  }
  return LC_OK;
}

static unsigned long lc_pouch_query_index_term_cache_hash(const char *field_hex,
                                                          const char *value_hex,
                                                          char value_type) {
  unsigned long hash;

  hash = lc_pouch_query_index_hash_init();
  if (field_hex != NULL) {
    lc_pouch_query_index_hash_bytes(&hash, field_hex, strlen(field_hex));
  }
  lc_pouch_query_index_hash_bytes(&hash, &value_type, 1U);
  if (value_hex != NULL) {
    lc_pouch_query_index_hash_bytes(&hash, value_hex, strlen(value_hex));
  }
  return hash != 0UL ? hash : 1UL;
}

static void lc_pouch_query_index_term_cache_cleanup(
    const lc_allocator *allocator, lc_pouch_query_index_term_cache *cache) {
  if (cache == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, cache->items);
  memset(cache, 0, sizeof(*cache));
}

static int
lc_pouch_query_index_term_cache_reserve(const lc_allocator *allocator,
                                        lc_pouch_query_index_term_cache *cache,
                                        size_t needed, lc_error *error) {
  lc_pouch_query_index_term_cache_entry *next_items;
  size_t next_capacity;
  size_t index;

  if (cache == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch term cache reserve requires cache", NULL, NULL,
                        NULL);
  }
  if (needed * 2U <= cache->capacity) {
    return LC_OK;
  }
  next_capacity = cache->capacity == 0U ? 256U : cache->capacity;
  while (needed * 2U > next_capacity) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch term cache exceeds local limit", NULL, NULL,
                          NULL);
    }
    next_capacity *= 2U;
  }
  next_items =
      (lc_pouch_query_index_term_cache_entry *)lc_calloc_with_allocator(
          allocator, next_capacity, sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch term cache", NULL, NULL,
                        NULL);
  }
  if (cache->items != NULL) {
    for (index = 0U; index < cache->capacity; ++index) {
      size_t slot;

      if (!cache->items[index].used) {
        continue;
      }
      slot = (size_t)(cache->items[index].hash % (unsigned long)next_capacity);
      while (next_items[slot].used) {
        slot = (slot + 1U) % next_capacity;
      }
      next_items[slot] = cache->items[index];
    }
    lc_free_with_allocator(allocator, cache->items);
  }
  cache->items = next_items;
  cache->capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_query_index_term_cache_find(
    const lc_pouch_query_index_term_cache *cache, const char *field_hex,
    const char *value_hex, char value_type, unsigned long hash,
    unsigned long *term_id_out) {
  size_t slot;
  size_t visited;

  if (term_id_out != NULL) {
    *term_id_out = 0UL;
  }
  if (cache == NULL || cache->items == NULL || cache->capacity == 0U ||
      field_hex == NULL || value_hex == NULL) {
    return 0;
  }
  slot = (size_t)(hash % (unsigned long)cache->capacity);
  visited = 0U;
  while (visited < cache->capacity && cache->items[slot].used) {
    if (cache->items[slot].hash == hash &&
        cache->items[slot].value_type == value_type &&
        strcmp(cache->items[slot].field_hex, field_hex) == 0 &&
        strcmp(cache->items[slot].value_hex, value_hex) == 0) {
      if (term_id_out != NULL) {
        *term_id_out = cache->items[slot].term_id;
      }
      return 1;
    }
    slot = (slot + 1U) % cache->capacity;
    ++visited;
  }
  return 0;
}

static int lc_pouch_query_index_term_cache_put(
    const lc_allocator *allocator, lc_pouch_query_index_term_cache *cache,
    const char *field_hex, const char *value_hex, char value_type,
    unsigned long hash, unsigned long term_id, lc_error *error) {
  size_t slot;
  int rc;

  rc = lc_pouch_query_index_term_cache_reserve(allocator, cache,
                                               cache->count + 1U, error);
  if (rc != LC_OK) {
    return rc;
  }
  slot = (size_t)(hash % (unsigned long)cache->capacity);
  while (cache->items[slot].used) {
    if (cache->items[slot].hash == hash &&
        cache->items[slot].value_type == value_type &&
        strcmp(cache->items[slot].field_hex, field_hex) == 0 &&
        strcmp(cache->items[slot].value_hex, value_hex) == 0) {
      cache->items[slot].term_id = term_id;
      return LC_OK;
    }
    slot = (slot + 1U) % cache->capacity;
  }
  cache->items[slot].field_hex = field_hex;
  cache->items[slot].value_hex = value_hex;
  cache->items[slot].value_type = value_type;
  cache->items[slot].hash = hash;
  cache->items[slot].term_id = term_id;
  cache->items[slot].used = 1;
  ++cache->count;
  return LC_OK;
}

static int lc_pouch_query_index_term_find_or_add_cached(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_term_cache *cache, const char *field_hex,
    const char *value_hex, char value_type, unsigned long *term_id_out,
    lc_error *error) {
  unsigned long hash;
  unsigned long term_id;
  int rc;

  if (term_id_out != NULL) {
    *term_id_out = 0UL;
  }
  hash = lc_pouch_query_index_term_cache_hash(field_hex, value_hex, value_type);
  if (lc_pouch_query_index_term_cache_find(cache, field_hex, value_hex,
                                           value_type, hash, &term_id)) {
    if (term_id_out != NULL) {
      *term_id_out = term_id;
    }
    return LC_OK;
  }
  term_id = 0UL;
  rc = lc_pouch_index_term_table_append_trusted(allocator, &generation->terms,
                                                field_hex, value_hex,
                                                value_type, &term_id, error);
  if (rc == LC_OK) {
    const lc_pouch_index_term_entry *entry;

    entry = NULL;
    if (term_id > 0UL && term_id <= (unsigned long)generation->terms.count) {
      entry = &generation->terms.items[term_id - 1UL];
    }
    if (entry == NULL) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch term cache append returned invalid term id",
                          NULL, NULL, "pouch");
    }
    rc = lc_pouch_query_index_term_cache_put(allocator, cache, entry->field_hex,
                                             entry->value_hex, value_type, hash,
                                             term_id, error);
  }
  if (rc == LC_OK && term_id_out != NULL) {
    *term_id_out = term_id;
  }
  return rc;
}

static void lc_pouch_query_index_trigram_term_cache_cleanup(
    const lc_allocator *allocator,
    lc_pouch_query_index_trigram_term_cache *cache) {
  if (cache == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, cache->items);
  memset(cache, 0, sizeof(*cache));
}

static int lc_pouch_query_index_trigram_term_cache_reserve(
    const lc_allocator *allocator,
    lc_pouch_query_index_trigram_term_cache *cache, size_t needed,
    lc_error *error) {
  lc_pouch_query_index_trigram_term_cache_entry *next_items;
  size_t next_capacity;
  size_t index;

  if (cache == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch trigram term cache reserve requires cache", NULL,
                        NULL, NULL);
  }
  if (needed * 2U <= cache->capacity) {
    return LC_OK;
  }
  next_capacity = cache->capacity == 0U ? 4096U : cache->capacity;
  while (needed * 2U > next_capacity) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch trigram term cache exceeds local limit", NULL,
                          NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_items =
      (lc_pouch_query_index_trigram_term_cache_entry *)lc_calloc_with_allocator(
          allocator, next_capacity, sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch trigram term cache", NULL,
                        NULL, NULL);
  }
  if (cache->items != NULL) {
    for (index = 0U; index < cache->capacity; ++index) {
      size_t slot;

      if (!cache->items[index].used) {
        continue;
      }
      slot = (size_t)((cache->items[index].field_hash ^
                       cache->items[index].trigram_key) %
                      (unsigned long)next_capacity);
      while (next_items[slot].used) {
        slot = (slot + 1U) % next_capacity;
      }
      next_items[slot] = cache->items[index];
    }
    lc_free_with_allocator(allocator, cache->items);
  }
  cache->items = next_items;
  cache->capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_query_index_trigram_term_cache_find(
    const lc_pouch_query_index_trigram_term_cache *cache, const char *field_hex,
    unsigned long field_id, unsigned long field_hash, unsigned long trigram_key,
    unsigned long *term_id_out) {
  size_t slot;
  size_t visited;

  if (term_id_out != NULL) {
    *term_id_out = 0UL;
  }
  if (cache == NULL || cache->items == NULL || cache->capacity == 0U ||
      field_hex == NULL) {
    return 0;
  }
  slot = (size_t)((field_hash ^ trigram_key) % (unsigned long)cache->capacity);
  visited = 0U;
  while (visited < cache->capacity && cache->items[slot].used) {
    if ((field_id != 0UL
             ? cache->items[slot].field_id == field_id
             : cache->items[slot].field_hash == field_hash &&
                   strcmp(cache->items[slot].field_hex, field_hex) == 0) &&
        cache->items[slot].trigram_key == trigram_key &&
        cache->items[slot].field_hex != NULL) {
      if (term_id_out != NULL) {
        *term_id_out = cache->items[slot].term_id;
      }
      return 1;
    }
    slot = (slot + 1U) % cache->capacity;
    ++visited;
  }
  return 0;
}

static int lc_pouch_query_index_trigram_term_cache_put(
    const lc_allocator *allocator,
    lc_pouch_query_index_trigram_term_cache *cache, const char *field_hex,
    unsigned long field_id, unsigned long field_hash, unsigned long trigram_key,
    unsigned long term_id, lc_error *error) {
  size_t slot;
  int rc;

  rc = lc_pouch_query_index_trigram_term_cache_reserve(
      allocator, cache, cache->count + 1U, error);
  if (rc != LC_OK) {
    return rc;
  }
  slot = (size_t)((field_hash ^ trigram_key) % (unsigned long)cache->capacity);
  while (cache->items[slot].used) {
    if ((field_id != 0UL
             ? cache->items[slot].field_id == field_id
             : cache->items[slot].field_hash == field_hash &&
                   strcmp(cache->items[slot].field_hex, field_hex) == 0) &&
        cache->items[slot].trigram_key == trigram_key &&
        cache->items[slot].field_hex != NULL) {
      cache->items[slot].term_id = term_id;
      return LC_OK;
    }
    slot = (slot + 1U) % cache->capacity;
  }
  cache->items[slot].field_hex = field_hex;
  cache->items[slot].field_id = field_id;
  cache->items[slot].field_hash = field_hash;
  cache->items[slot].trigram_key = trigram_key;
  cache->items[slot].term_id = term_id;
  cache->items[slot].used = 1;
  ++cache->count;
  return LC_OK;
}

static int lc_pouch_query_index_trigram_term_find_or_add(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_trigram_term_cache *cache, const char *field_hex,
    unsigned long field_id, unsigned long field_hash, unsigned long trigram_key,
    unsigned long *term_id_out, lc_error *error) {
  unsigned long term_id;
  char trigram_hex[7];
  int rc;

  if (term_id_out != NULL) {
    *term_id_out = 0UL;
  }
  if (lc_pouch_query_index_trigram_term_cache_find(
          cache, field_hex, field_id, field_hash, trigram_key, &term_id)) {
    if (term_id_out != NULL) {
      *term_id_out = term_id;
    }
    return LC_OK;
  }
  lc_pouch_query_index_trigram_key_hex(trigram_key, trigram_hex);
  rc = lc_pouch_index_term_table_append_trusted(allocator, &generation->terms,
                                                field_hex, trigram_hex, 's',
                                                &term_id, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_trigram_term_cache_put(
        allocator, cache, field_hex, field_id, field_hash, trigram_key, term_id,
        error);
  }
  if (rc == LC_OK && term_id_out != NULL) {
    *term_id_out = term_id;
  }
  return rc;
}

static int lc_pouch_query_index_trigram_generation_append_docid(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_query_index_field_cache *field_cache,
    lc_pouch_query_index_trigram_term_cache *term_cache, const char *field_hex,
    unsigned long trigram_key, unsigned long doc_id, lc_error *error) {
  unsigned long field_hash;
  unsigned long field_id;
  unsigned long term_id;
  int rc;

  if (field_hex == NULL) {
    return LC_OK;
  }
  field_hash = 0UL;
  field_id = 0UL;
  rc = lc_pouch_query_index_field_cache_find_or_add(
      allocator, field_cache, field_hex, &field_id, &field_hash, error);
  if (rc != LC_OK) {
    return rc;
  }
  term_id = 0UL;
  rc = lc_pouch_query_index_trigram_term_find_or_add(
      allocator, generation, term_cache, field_hex, field_id, field_hash,
      trigram_key, &term_id, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_append(
        allocator, accumulator, term_id, doc_id, error);
  }
  return rc;
}

static int lc_pouch_query_index_trigram_generation_remove_docid(
    const lc_allocator *allocator,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_query_index_field_cache *field_cache,
    lc_pouch_query_index_trigram_term_cache *term_cache, const char *field_hex,
    unsigned long trigram_key, unsigned long doc_id, lc_error *error) {
  unsigned long field_hash;
  unsigned long field_id;
  unsigned long term_id;
  int rc;

  if (field_hex == NULL) {
    return LC_OK;
  }
  field_hash = 0UL;
  field_id = 0UL;
  rc = lc_pouch_query_index_field_cache_find_or_add(
      allocator, field_cache, field_hex, &field_id, &field_hash, error);
  if (rc != LC_OK) {
    return rc;
  }
  term_id = 0UL;
  if (lc_pouch_query_index_trigram_term_cache_find(
          term_cache, field_hex, field_id, field_hash, trigram_key, &term_id)) {
    lc_pouch_query_index_exact_generation_accumulator_remove(accumulator,
                                                             term_id, doc_id);
  }
  return LC_OK;
}

static size_t lc_pouch_query_index_token_hex_stride(void) {
  return (LC_POUCH_QUERY_INDEX_MAX_EXACT_TOKEN_BYTES * 2U) + 1U;
}

static void lc_pouch_query_index_token_value_set_cleanup(
    const lc_allocator *allocator, lc_pouch_query_index_token_value_set *set) {
  if (set == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, set->items);
  memset(set, 0, sizeof(*set));
}

static int lc_pouch_query_index_token_value_set_reserve(
    const lc_allocator *allocator, lc_pouch_query_index_token_value_set *set,
    size_t needed, lc_error *error) {
  char *next_items;
  size_t next_capacity;
  size_t stride;

  if (set == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch token set reserve requires set", NULL, NULL,
                        NULL);
  }
  if (needed <= set->capacity) {
    return LC_OK;
  }
  next_capacity = set->capacity == 0U ? 16U : set->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch token set exceeds local limit", NULL, NULL,
                          NULL);
    }
    next_capacity *= 2U;
  }
  stride = lc_pouch_query_index_token_hex_stride();
  if (next_capacity > ((size_t)-1 / stride)) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch token set exceeds local limit", NULL, NULL,
                        NULL);
  }
  next_items =
      (char *)lc_alloc_with_allocator(allocator, next_capacity * stride);
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch token set", NULL, NULL, NULL);
  }
  if (set->items != NULL) {
    memcpy(next_items, set->items, set->count * stride);
    lc_free_with_allocator(allocator, set->items);
  }
  set->items = next_items;
  set->capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_query_index_token_value_set_find(
    const lc_pouch_query_index_token_value_set *set, const char *term,
    size_t *position_out) {
  size_t low;
  size_t high;
  size_t stride;

  if (position_out != NULL) {
    *position_out = 0U;
  }
  if (set == NULL || term == NULL) {
    return 0;
  }
  stride = lc_pouch_query_index_token_hex_stride();
  low = 0U;
  high = set->count;
  while (low < high) {
    size_t mid;
    int cmp;

    mid = low + ((high - low) / 2U);
    cmp = strcmp(set->items + (mid * stride), term);
    if (cmp < 0) {
      low = mid + 1U;
    } else if (cmp > 0) {
      high = mid;
    } else {
      if (position_out != NULL) {
        *position_out = mid;
      }
      return 1;
    }
  }
  if (position_out != NULL) {
    *position_out = low;
  }
  return 0;
}

static int lc_pouch_query_index_token_value_set_add(
    const lc_allocator *allocator, lc_pouch_query_index_token_value_set *set,
    const char *term, lc_error *error) {
  size_t position;
  size_t stride;
  int rc;

  if (term == NULL || term[0] == '\0' ||
      strlen(term) > LC_POUCH_QUERY_INDEX_MAX_EXACT_TOKEN_BYTES * 2U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch token set requires a bounded hex token", NULL,
                        NULL, "pouch");
  }
  if (lc_pouch_query_index_token_value_set_find(set, term, &position)) {
    return LC_OK;
  }
  rc = lc_pouch_query_index_token_value_set_reserve(allocator, set,
                                                    set->count + 1U, error);
  if (rc != LC_OK) {
    return rc;
  }
  stride = lc_pouch_query_index_token_hex_stride();
  if (position < set->count) {
    memmove(set->items + ((position + 1U) * stride),
            set->items + (position * stride), (set->count - position) * stride);
  }
  memset(set->items + (position * stride), 0, stride);
  memcpy(set->items + (position * stride), term, strlen(term) + 1U);
  ++set->count;
  return LC_OK;
}

static int lc_pouch_query_index_trigram_seen_mark(unsigned char *seen,
                                                  unsigned long key) {
  unsigned long byte_index;
  unsigned char mask;

  if (seen == NULL) {
    return 1;
  }
  byte_index = key >> 3U;
  mask = (unsigned char)(1U << (key & 7U));
  if ((seen[byte_index] & mask) != 0U) {
    return 0;
  }
  seen[byte_index] = (unsigned char)(seen[byte_index] | mask);
  return 1;
}

static int lc_pouch_query_index_trigram_repeated_space(unsigned char a,
                                                       unsigned char b,
                                                       unsigned char c) {
  return a == b && b == c && (a == ' ' || a == '\t' || a == '\n' || a == '\r');
}

static int lc_pouch_query_index_trigram_generation_add_value(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_query_index_field_cache *field_cache,
    lc_pouch_query_index_trigram_term_cache *term_cache, const char *field_hex,
    const char *value_hex, unsigned long doc_id, lc_error *error) {
  lc_pouch_query_index_trigram_value_set unique_terms;
  unsigned char *seen;
  size_t value_hex_len;
  size_t index;
  int rc;

  if (allocator == NULL || generation == NULL || accumulator == NULL ||
      field_cache == NULL || field_hex == NULL || value_hex == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch trigram generation requires allocator, "
                        "generation, accumulator, field, and value",
                        NULL, NULL, NULL);
  }
  if (strcmp(value_hex, "-") == 0) {
    return LC_OK;
  }
  value_hex_len = strlen(value_hex);
  if ((value_hex_len % 2U) != 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch trigram generation requires byte-aligned hex",
                        NULL, NULL, "pouch");
  }
  if (value_hex_len < 6U) {
    return LC_OK;
  }
  memset(&unique_terms, 0, sizeof(unique_terms));
  seen = NULL;
  if ((value_hex_len / 2U) >= LC_POUCH_QUERY_INDEX_TRIGRAM_SEEN_THRESHOLD) {
    seen = (unsigned char *)lc_calloc_with_allocator(
        allocator, LC_POUCH_QUERY_INDEX_TRIGRAM_SEEN_BYTES, 1U);
    if (seen == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch trigram seen set", NULL,
                          NULL, NULL);
    }
  }
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index + 6U <= value_hex_len; index += 2U) {
    unsigned char folded[3] = {0U, 0U, 0U};
    size_t byte_index;
    unsigned long folded_key;

    for (byte_index = 0U; rc == LC_OK && byte_index < 3U; ++byte_index) {
      int high;
      int low;
      unsigned char value;

      high = lc_pouch_query_index_hex_value(
          (unsigned char)value_hex[index + (byte_index * 2U)]);
      low = lc_pouch_query_index_hex_value(
          (unsigned char)value_hex[index + (byte_index * 2U) + 1U]);
      if (high < 0 || low < 0) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch trigram generation has invalid hex", NULL,
                          NULL, "pouch");
        break;
      }
      value = (unsigned char)(((unsigned int)high << 4) | (unsigned int)low);
      folded[byte_index] = lc_pouch_query_index_ascii_lower(value);
    }
    if (rc != LC_OK) {
      break;
    }
    folded_key = ((unsigned long)folded[0] << 16U) |
                 ((unsigned long)folded[1] << 8U) | (unsigned long)folded[2];
    if (lc_pouch_query_index_trigram_seen_mark(seen, folded_key)) {
      rc = lc_pouch_query_index_trigram_value_set_add(allocator, &unique_terms,
                                                      folded_key, error);
    }
  }
  for (index = 0U; rc == LC_OK && index < unique_terms.count; ++index) {
    rc = lc_pouch_query_index_trigram_generation_append_docid(
        allocator, generation, accumulator, field_cache, term_cache, field_hex,
        unique_terms.items[index], doc_id, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_trigram_generation_append_docid(
          allocator, generation, accumulator, field_cache, term_cache,
          LC_POUCH_QUERY_INDEX_ANY_TEXT_FIELD_HEX, unique_terms.items[index],
          doc_id, error);
    }
  }
  lc_pouch_query_index_trigram_value_set_cleanup(allocator, &unique_terms);
  lc_free_with_allocator(allocator, seen);
  return rc;
}

static int lc_pouch_query_index_trigram_generation_add_raw_value(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_query_index_field_cache *field_cache,
    lc_pouch_query_index_trigram_term_cache *term_cache, const char *field_hex,
    const char *value, size_t value_len, unsigned long doc_id,
    lc_error *error) {
  lc_pouch_query_index_trigram_value_set unique_terms;
  unsigned char *seen;
  size_t index;
  int rc;

  if (allocator == NULL || generation == NULL || accumulator == NULL ||
      field_cache == NULL || field_hex == NULL || value == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch raw trigram generation requires allocator, "
                        "generation, accumulator, field, and value",
                        NULL, NULL, NULL);
  }
  if (value_len < 3U) {
    return LC_OK;
  }
  memset(&unique_terms, 0, sizeof(unique_terms));
  seen = NULL;
  if (value_len >= LC_POUCH_QUERY_INDEX_TRIGRAM_SEEN_THRESHOLD) {
    seen = (unsigned char *)lc_calloc_with_allocator(
        allocator, LC_POUCH_QUERY_INDEX_TRIGRAM_SEEN_BYTES, 1U);
    if (seen == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch raw trigram seen set", NULL,
                          NULL, NULL);
    }
  }
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index + 2U < value_len; ++index) {
    unsigned char bytes[3];
    unsigned char folded_bytes[3];
    size_t byte_index;
    unsigned long folded_key;

    for (byte_index = 0U; byte_index < 3U; ++byte_index) {
      unsigned char byte;
      unsigned char folded;

      byte = (unsigned char)value[index + byte_index];
      folded =
          byte >= 'A' && byte <= 'Z' ? (unsigned char)(byte - 'A' + 'a') : byte;
      bytes[byte_index] = byte;
      folded_bytes[byte_index] = folded;
    }
    folded_key = ((unsigned long)folded_bytes[0] << 16U) |
                 ((unsigned long)folded_bytes[1] << 8U) |
                 (unsigned long)folded_bytes[2];
    if (lc_pouch_query_index_trigram_seen_mark(seen, folded_key)) {
      rc = lc_pouch_query_index_trigram_value_set_add(allocator, &unique_terms,
                                                      folded_key, error);
    }
    if (lc_pouch_query_index_trigram_repeated_space(bytes[0], bytes[1],
                                                    bytes[2])) {
      size_t run_end;

      run_end = index + 3U;
      while (run_end < value_len && (unsigned char)value[run_end] == bytes[0]) {
        ++run_end;
      }
      if (run_end > index + 3U) {
        index = run_end >= 3U ? run_end - 3U : run_end;
      }
    }
  }
  for (index = 0U; rc == LC_OK && index < unique_terms.count; ++index) {
    rc = lc_pouch_query_index_trigram_generation_append_docid(
        allocator, generation, accumulator, field_cache, term_cache, field_hex,
        unique_terms.items[index], doc_id, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_trigram_generation_append_docid(
          allocator, generation, accumulator, field_cache, term_cache,
          LC_POUCH_QUERY_INDEX_ANY_TEXT_FIELD_HEX, unique_terms.items[index],
          doc_id, error);
    }
  }
  lc_pouch_query_index_trigram_value_set_cleanup(allocator, &unique_terms);
  lc_free_with_allocator(allocator, seen);
  return rc;
}

static int lc_pouch_query_index_token_generation_add_aggregate_term(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_query_index_term_cache *term_cache, const char *value_hex,
    unsigned long doc_id, lc_error *error) {
  unsigned long term_id;
  int rc;

  term_id = 0UL;
  rc = lc_pouch_query_index_term_find_or_add_cached(
      allocator, generation, term_cache,
      LC_POUCH_QUERY_INDEX_ANY_TEXT_FIELD_HEX, value_hex,
      LC_POUCH_QUERY_INDEX_TEXT_TOKEN_TYPE, &term_id, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_append(
        allocator, accumulator, term_id, doc_id, error);
  }
  return rc;
}

static int lc_pouch_query_index_token_generation_add_value(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_query_index_term_cache *term_cache, const char *field_hex,
    const char *value_hex, unsigned long doc_id, lc_error *error) {
  lc_pouch_query_index_token_value_set unique_tokens;
  size_t value_hex_len;
  size_t index;
  size_t token_len;
  int token_overflow;
  int rc;
  char raw_hex[(LC_POUCH_QUERY_INDEX_MAX_EXACT_TOKEN_BYTES * 2U) + 1U];
  char folded_hex[(LC_POUCH_QUERY_INDEX_MAX_EXACT_TOKEN_BYTES * 2U) + 1U];

  if (field_hex == NULL ||
      lc_pouch_query_index_field_hex_is_any_text(field_hex) ||
      value_hex == NULL || strcmp(value_hex, "-") == 0) {
    return LC_OK;
  }
  memset(&unique_tokens, 0, sizeof(unique_tokens));
  value_hex_len = strlen(value_hex);
  if ((value_hex_len % 2U) != 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch token generation requires byte-aligned hex",
                        NULL, NULL, "pouch");
  }
  token_len = 0U;
  token_overflow = 0;
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index <= value_hex_len; index += 2U) {
    unsigned char value;
    int token_char;

    value = 0U;
    token_char = 0;
    if (index < value_hex_len) {
      int high;
      int low;

      high = lc_pouch_query_index_hex_value((unsigned char)value_hex[index]);
      low =
          lc_pouch_query_index_hex_value((unsigned char)value_hex[index + 1U]);
      if (high < 0 || low < 0) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch token generation has invalid hex", NULL, NULL,
                          "pouch");
        break;
      }
      value = (unsigned char)(((unsigned int)high << 4) | (unsigned int)low);
      token_char = (value >= 'A' && value <= 'Z') ||
                   (value >= 'a' && value <= 'z') ||
                   (value >= '0' && value <= '9') || value == '_';
    }
    if (token_char) {
      if (token_len < LC_POUCH_QUERY_INDEX_MAX_EXACT_TOKEN_BYTES) {
        unsigned char folded;

        folded = value >= 'A' && value <= 'Z'
                     ? (unsigned char)(value - 'A' + 'a')
                     : value;
        lc_pouch_query_index_hex_encode_byte(raw_hex + (token_len * 2U), value);
        lc_pouch_query_index_hex_encode_byte(folded_hex + (token_len * 2U),
                                             folded);
      } else {
        token_overflow = 1;
      }
      ++token_len;
    } else {
      if (!token_overflow &&
          token_len >= LC_POUCH_QUERY_INDEX_MIN_EXACT_TOKEN_BYTES &&
          token_len <= LC_POUCH_QUERY_INDEX_MAX_EXACT_TOKEN_BYTES) {
        raw_hex[token_len * 2U] = '\0';
        folded_hex[token_len * 2U] = '\0';
        rc = lc_pouch_query_index_token_value_set_add(allocator, &unique_tokens,
                                                      raw_hex, error);
        if (rc == LC_OK && strcmp(folded_hex, raw_hex) != 0) {
          rc = lc_pouch_query_index_token_value_set_add(
              allocator, &unique_tokens, folded_hex, error);
        }
      }
      if (index < value_hex_len &&
          (value == ' ' || value == '\t' || value == '\n' || value == '\r')) {
        size_t run_end;

        run_end = index + 2U;
        while (run_end + 1U < value_hex_len) {
          int high;
          int low;
          unsigned char next_value;

          high =
              lc_pouch_query_index_hex_value((unsigned char)value_hex[run_end]);
          low = lc_pouch_query_index_hex_value(
              (unsigned char)value_hex[run_end + 1U]);
          if (high < 0 || low < 0) {
            rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                              "pouch token generation has invalid hex", NULL,
                              NULL, "pouch");
            break;
          }
          next_value =
              (unsigned char)(((unsigned int)high << 4) | (unsigned int)low);
          if (next_value != value) {
            break;
          }
          run_end += 2U;
        }
        if (rc == LC_OK && run_end > index + 2U) {
          index = run_end - 2U;
        }
      }
      token_len = 0U;
      token_overflow = 0;
    }
  }
  for (index = 0U; rc == LC_OK && index < unique_tokens.count; ++index) {
    rc = lc_pouch_query_index_token_generation_add_aggregate_term(
        allocator, generation, accumulator, term_cache,
        unique_tokens.items + (index * lc_pouch_query_index_token_hex_stride()),
        doc_id, error);
  }
  lc_pouch_query_index_token_value_set_cleanup(allocator, &unique_tokens);
  return rc;
}

static int lc_pouch_query_index_token_generation_add_raw_value(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_query_index_term_cache *term_cache, const char *field_hex,
    const char *value, size_t value_len, unsigned long doc_id,
    lc_error *error) {
  lc_pouch_query_index_token_value_set unique_tokens;
  size_t index;
  size_t token_len;
  int token_overflow;
  int rc;
  char raw_hex[(LC_POUCH_QUERY_INDEX_MAX_EXACT_TOKEN_BYTES * 2U) + 1U];
  char folded_hex[(LC_POUCH_QUERY_INDEX_MAX_EXACT_TOKEN_BYTES * 2U) + 1U];

  if (field_hex == NULL ||
      lc_pouch_query_index_field_hex_is_any_text(field_hex) || value == NULL) {
    return LC_OK;
  }
  memset(&unique_tokens, 0, sizeof(unique_tokens));
  token_len = 0U;
  token_overflow = 0;
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index <= value_len; ++index) {
    unsigned char raw;
    int token_char;

    raw = index < value_len ? (unsigned char)value[index] : 0U;
    token_char = index < value_len &&
                 ((raw >= 'A' && raw <= 'Z') || (raw >= 'a' && raw <= 'z') ||
                  (raw >= '0' && raw <= '9') || raw == '_');
    if (token_char) {
      if (token_len < LC_POUCH_QUERY_INDEX_MAX_EXACT_TOKEN_BYTES) {
        unsigned char folded;

        folded =
            raw >= 'A' && raw <= 'Z' ? (unsigned char)(raw - 'A' + 'a') : raw;
        lc_pouch_query_index_hex_encode_byte(raw_hex + (token_len * 2U), raw);
        lc_pouch_query_index_hex_encode_byte(folded_hex + (token_len * 2U),
                                             folded);
      } else {
        token_overflow = 1;
      }
      ++token_len;
    } else {
      if (!token_overflow &&
          token_len >= LC_POUCH_QUERY_INDEX_MIN_EXACT_TOKEN_BYTES &&
          token_len <= LC_POUCH_QUERY_INDEX_MAX_EXACT_TOKEN_BYTES) {
        raw_hex[token_len * 2U] = '\0';
        folded_hex[token_len * 2U] = '\0';
        rc = lc_pouch_query_index_token_value_set_add(allocator, &unique_tokens,
                                                      raw_hex, error);
        if (rc == LC_OK && strcmp(folded_hex, raw_hex) != 0) {
          rc = lc_pouch_query_index_token_value_set_add(
              allocator, &unique_tokens, folded_hex, error);
        }
      }
      if (index < value_len &&
          (raw == ' ' || raw == '\t' || raw == '\n' || raw == '\r')) {
        size_t run_end;

        run_end = index + 1U;
        while (run_end < value_len && (unsigned char)value[run_end] == raw) {
          ++run_end;
        }
        if (run_end > index + 1U) {
          index = run_end - 1U;
        }
      }
      token_len = 0U;
      token_overflow = 0;
    }
  }
  for (index = 0U; rc == LC_OK && index < unique_tokens.count; ++index) {
    rc = lc_pouch_query_index_token_generation_add_aggregate_term(
        allocator, generation, accumulator, term_cache,
        unique_tokens.items + (index * lc_pouch_query_index_token_hex_stride()),
        doc_id, error);
  }
  lc_pouch_query_index_token_value_set_cleanup(allocator, &unique_tokens);
  return rc;
}

static int lc_pouch_query_index_text_generation_add_term(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_query_index_term_cache *term_cache, const char *field_hex,
    const char *value_hex, char value_type, unsigned long doc_id,
    lc_error *error) {
  unsigned long term_id;
  int rc;

  term_id = 0UL;
  rc = lc_pouch_query_index_term_find_or_add_cached(
      allocator, generation, term_cache, field_hex, value_hex, value_type,
      &term_id, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_append(
        allocator, accumulator, term_id, doc_id, error);
  }
  return rc;
}

static int lc_pouch_query_index_text_generation_add_value(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_query_index_term_cache *term_cache, const char *field_hex,
    const char *value_hex, unsigned long doc_id, lc_error *error) {
  char *prefix_hex;
  size_t prefix_hex_len;
  int rc;

  if (value_hex == NULL) {
    return LC_OK;
  }
  if (!lc_pouch_query_index_exact_string_is_long_hex(value_hex)) {
    rc = lc_pouch_query_index_text_generation_add_term(
        allocator, generation, accumulator, term_cache, field_hex, value_hex,
        's', doc_id, error);
    return rc;
  }
  prefix_hex_len = LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES * 2U;
  prefix_hex = (char *)lc_alloc_with_allocator(allocator, prefix_hex_len + 1U);
  if (prefix_hex == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch text prefix term", NULL, NULL,
                        NULL);
  }
  memcpy(prefix_hex, value_hex, prefix_hex_len);
  prefix_hex[prefix_hex_len] = '\0';
  rc = lc_pouch_query_index_text_generation_add_term(
      allocator, generation, accumulator, term_cache, field_hex, prefix_hex,
      LC_POUCH_QUERY_INDEX_TEXT_PREFIX_TYPE, doc_id, error);
  lc_free_with_allocator(allocator, prefix_hex);
  return rc;
}

static int lc_pouch_query_index_text_generation_add_raw_long_value(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_query_index_term_cache *term_cache, const char *field_hex,
    const char *value, size_t value_len, unsigned long doc_id,
    lc_error *error) {
  char *prefix_hex;
  size_t prefix_len;
  int rc;

  if (value == NULL ||
      value_len <= LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES) {
    return LC_OK;
  }
  prefix_len = LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES;
  prefix_hex =
      lc_pouch_query_index_hex_encode_bytes(allocator, value, prefix_len);
  if (prefix_hex == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch raw text prefix term", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_query_index_text_generation_add_term(
      allocator, generation, accumulator, term_cache, field_hex, prefix_hex,
      LC_POUCH_QUERY_INDEX_TEXT_PREFIX_TYPE, doc_id, error);
  lc_free_with_allocator(allocator, prefix_hex);
  return rc;
}

static char *lc_pouch_query_index_temporal_instant_value_hex(
    const lc_allocator *allocator, const lc_pouch_index_instant *instant,
    lc_error *error) {
  char value[96];
  int written;

  if (instant == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch temporal generation value requires instant", NULL, NULL,
                 NULL);
    return NULL;
  }
  written = snprintf(value, sizeof(value), "%.17g", instant->seconds);
  if (written < 0 || (size_t)written >= sizeof(value)) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch temporal generation value exceeds local limit", NULL,
                 NULL, "pouch");
    return NULL;
  }
  return lc_pouch_query_index_hex_encode(allocator, value);
}

static int lc_pouch_query_index_term_precompute_derived(
    lc_pouch_query_index_summary *summary, lc_pouch_query_index_term *term,
    const char *value, size_t value_len, lc_error *error) {
  lc_pouch_query_index_trigram_value_set trigram_set;
  lc_pouch_query_index_token_value_set token_set;
  lc_pouch_index_instant instant;
  unsigned char *seen;
  size_t index;
  size_t token_len;
  int token_overflow;
  int rc;
  char raw_hex[(LC_POUCH_QUERY_INDEX_MAX_EXACT_TOKEN_BYTES * 2U) + 1U];
  char folded_hex[(LC_POUCH_QUERY_INDEX_MAX_EXACT_TOKEN_BYTES * 2U) + 1U];

  if (summary == NULL || term == NULL || value == NULL) {
    return LC_OK;
  }
  memset(&trigram_set, 0, sizeof(trigram_set));
  memset(&token_set, 0, sizeof(token_set));
  seen = NULL;
  rc = LC_OK;
  if (value_len > LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES) {
    term->text_prefix_hex = lc_pouch_query_index_hex_encode_bytes(
        summary->allocator, value,
        LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES);
    if (term->text_prefix_hex == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to precompute pouch text prefix", NULL, NULL,
                        NULL);
      goto cleanup;
    }
  }
  if (value_len >= LC_POUCH_QUERY_INDEX_TRIGRAM_SEEN_THRESHOLD) {
    seen = (unsigned char *)lc_calloc_with_allocator(
        summary->allocator, LC_POUCH_QUERY_INDEX_TRIGRAM_SEEN_BYTES, 1U);
    if (seen == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch precomputed trigram seen set",
                        NULL, NULL, NULL);
      goto cleanup;
    }
  }
  for (index = 0U; rc == LC_OK && index + 2U < value_len; ++index) {
    unsigned char bytes[3];
    unsigned char folded_bytes[3];
    size_t byte_index;
    unsigned long folded_key;

    for (byte_index = 0U; byte_index < 3U; ++byte_index) {
      unsigned char byte;

      byte = (unsigned char)value[index + byte_index];
      bytes[byte_index] = byte;
      folded_bytes[byte_index] =
          byte >= 'A' && byte <= 'Z' ? (unsigned char)(byte - 'A' + 'a') : byte;
    }
    folded_key = ((unsigned long)folded_bytes[0] << 16U) |
                 ((unsigned long)folded_bytes[1] << 8U) |
                 (unsigned long)folded_bytes[2];
    if (lc_pouch_query_index_trigram_seen_mark(seen, folded_key)) {
      rc = lc_pouch_query_index_trigram_value_set_add(
          summary->allocator, &trigram_set, folded_key, error);
    }
    if (lc_pouch_query_index_trigram_repeated_space(bytes[0], bytes[1],
                                                    bytes[2])) {
      size_t run_end;

      run_end = index + 3U;
      while (run_end < value_len && (unsigned char)value[run_end] == bytes[0]) {
        ++run_end;
      }
      if (run_end > index + 3U) {
        index = run_end >= 3U ? run_end - 3U : run_end;
      }
    }
  }
  token_len = 0U;
  token_overflow = 0;
  for (index = 0U; rc == LC_OK && index <= value_len; ++index) {
    unsigned char raw;
    int token_char;

    raw = index < value_len ? (unsigned char)value[index] : 0U;
    token_char = index < value_len &&
                 ((raw >= 'A' && raw <= 'Z') || (raw >= 'a' && raw <= 'z') ||
                  (raw >= '0' && raw <= '9') || raw == '_');
    if (token_char) {
      if (token_len < LC_POUCH_QUERY_INDEX_MAX_EXACT_TOKEN_BYTES) {
        unsigned char folded;

        folded =
            raw >= 'A' && raw <= 'Z' ? (unsigned char)(raw - 'A' + 'a') : raw;
        lc_pouch_query_index_hex_encode_byte(raw_hex + (token_len * 2U), raw);
        lc_pouch_query_index_hex_encode_byte(folded_hex + (token_len * 2U),
                                             folded);
      } else {
        token_overflow = 1;
      }
      ++token_len;
    } else {
      if (!token_overflow &&
          token_len >= LC_POUCH_QUERY_INDEX_MIN_EXACT_TOKEN_BYTES &&
          token_len <= LC_POUCH_QUERY_INDEX_MAX_EXACT_TOKEN_BYTES) {
        raw_hex[token_len * 2U] = '\0';
        folded_hex[token_len * 2U] = '\0';
        rc = lc_pouch_query_index_token_value_set_add(
            summary->allocator, &token_set, raw_hex, error);
        if (rc == LC_OK && strcmp(folded_hex, raw_hex) != 0) {
          rc = lc_pouch_query_index_token_value_set_add(
              summary->allocator, &token_set, folded_hex, error);
        }
      }
      if (index < value_len &&
          (raw == ' ' || raw == '\t' || raw == '\n' || raw == '\r')) {
        size_t run_end;

        run_end = index + 1U;
        while (run_end < value_len && (unsigned char)value[run_end] == raw) {
          ++run_end;
        }
        if (run_end > index + 1U) {
          index = run_end - 1U;
        }
      }
      token_len = 0U;
      token_overflow = 0;
    }
  }
  if (rc == LC_OK && lc_pouch_index_parse_lql_datetime(value, &instant)) {
    term->temporal_value_hex = lc_pouch_query_index_temporal_instant_value_hex(
        summary->allocator, &instant, error);
    if (term->temporal_value_hex == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
  }
  if (rc == LC_OK && trigram_set.count > 0U) {
    term->trigram_keys = trigram_set.items;
    term->trigram_key_count = trigram_set.count;
    trigram_set.items = NULL;
    trigram_set.count = 0U;
    trigram_set.capacity = 0U;
  }
  if (rc == LC_OK && token_set.count > 0U) {
    term->token_values = token_set.items;
    term->token_value_count = token_set.count;
    token_set.items = NULL;
    token_set.count = 0U;
    token_set.capacity = 0U;
  }
  if (rc == LC_OK) {
    term->derived_terms_ready = 1;
  }

cleanup:
  lc_free_with_allocator(summary->allocator, seen);
  lc_pouch_query_index_trigram_value_set_cleanup(summary->allocator,
                                                 &trigram_set);
  lc_pouch_query_index_token_value_set_cleanup(summary->allocator, &token_set);
  return rc;
}

static int lc_pouch_query_index_generation_append_docid(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_query_index_term_cache *term_cache, const char *field_hex,
    const char *value_hex, char value_type, unsigned long doc_id,
    lc_error *error) {
  unsigned long term_id;
  int rc;

  term_id = 0UL;
  rc = lc_pouch_query_index_term_find_or_add_cached(
      allocator, generation, term_cache, field_hex, value_hex, value_type,
      &term_id, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_append(
        allocator, accumulator, term_id, doc_id, error);
  }
  return rc;
}

static int lc_pouch_query_index_pending_segment_generation_init(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    const char *namespace_name, lc_error *error) {
  generation->namespace_name =
      lc_strdup_with_allocator(allocator, namespace_name);
  if (generation->namespace_name == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch pending generation "
                        "namespace",
                        NULL, NULL, NULL);
  }
  return LC_OK;
}

static lc_pouch_query_index_pending_segment *
lc_pouch_query_index_pending_segment_new(const lc_allocator *allocator,
                                         const char *namespace_name,
                                         lc_error *error) {
  lc_pouch_query_index_pending_segment *segment;
  int rc;

  segment = (lc_pouch_query_index_pending_segment *)lc_calloc_with_allocator(
      allocator, 1U, sizeof(*segment));
  if (segment == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch pending query-index segment", NULL,
                 NULL, NULL);
    return NULL;
  }
  segment->namespace_name = lc_strdup_with_allocator(allocator, namespace_name);
  segment->deletes.allocator = allocator;
  segment->row_hash = lc_pouch_query_index_hash_init();
  segment->term_hash = lc_pouch_query_index_hash_init();
  segment->presence_hash = lc_pouch_query_index_hash_init();
  segment->delete_hash = lc_pouch_query_index_hash_init();
  segment->term_index_complete = 1;
  segment->presence_index_complete = 1;
  rc = segment->namespace_name != NULL ? LC_OK : LC_ERR_NOMEM;
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_pending_segment_generation_init(
        allocator, &segment->exact_generation, namespace_name, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_pending_segment_generation_init(
        allocator, &segment->presence_generation, namespace_name, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_pending_segment_generation_init(
        allocator, &segment->range_generation, namespace_name, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_pending_segment_generation_init(
        allocator, &segment->text_generation, namespace_name, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_pending_segment_generation_init(
        allocator, &segment->trigram_generation, namespace_name, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_pending_segment_generation_init(
        allocator, &segment->temporal_generation, namespace_name, error);
  }
  if (rc != LC_OK) {
    if (error != NULL && error->code == LC_OK) {
      lc_error_set(error, LC_ERR_NOMEM, 0L,
                   "failed to allocate pouch pending generation namespace",
                   NULL, NULL, NULL);
    }
    lc_pouch_query_index_pending_segment_cleanup(allocator, segment);
    lc_free_with_allocator(allocator, segment);
    return NULL;
  }
  return segment;
}

static void lc_pouch_query_index_pending_segment_cleanup(
    const lc_allocator *allocator,
    lc_pouch_query_index_pending_segment *segment) {
  if (segment == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, segment->namespace_name);
  lc_free_with_allocator(allocator, segment->last_key_hex);
  lc_pouch_index_doc_table_cleanup(allocator, &segment->doc_table);
  lc_pouch_index_term_generation_cleanup(allocator, &segment->exact_generation);
  lc_pouch_index_term_generation_cleanup(allocator,
                                         &segment->presence_generation);
  lc_pouch_index_term_generation_cleanup(allocator, &segment->range_generation);
  lc_pouch_index_term_generation_cleanup(allocator, &segment->text_generation);
  lc_pouch_index_term_generation_cleanup(allocator,
                                         &segment->trigram_generation);
  lc_pouch_index_term_generation_cleanup(allocator,
                                         &segment->temporal_generation);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(
      allocator, &segment->exact_accumulator);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(
      allocator, &segment->presence_accumulator);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(
      allocator, &segment->range_accumulator);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(
      allocator, &segment->text_accumulator);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(
      allocator, &segment->trigram_accumulator);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(
      allocator, &segment->temporal_accumulator);
  lc_pouch_query_index_term_cache_cleanup(allocator,
                                          &segment->exact_term_cache);
  lc_pouch_query_index_term_cache_cleanup(allocator,
                                          &segment->presence_term_cache);
  lc_pouch_query_index_term_cache_cleanup(allocator,
                                          &segment->range_term_cache);
  lc_pouch_query_index_term_cache_cleanup(allocator, &segment->text_term_cache);
  lc_pouch_query_index_term_cache_cleanup(allocator,
                                          &segment->temporal_term_cache);
  lc_pouch_query_index_field_cache_cleanup(allocator,
                                           &segment->trigram_field_cache);
  lc_pouch_query_index_trigram_term_cache_cleanup(allocator,
                                                  &segment->trigram_term_cache);
  lc_free_with_allocator(allocator, segment->deletes.bytes);
  memset(segment, 0, sizeof(*segment));
}

static lc_pouch_query_index_pending_segment *
lc_pouch_query_index_pending_segment_find(lc_pouch *pouch,
                                          const char *namespace_name) {
  lc_pouch_query_index_pending_segment *segment;

  if (pouch == NULL || namespace_name == NULL) {
    return NULL;
  }
  for (segment = pouch->query_pending_segments; segment != NULL;
       segment = segment->next) {
    if (segment->namespace_name != NULL &&
        strcmp(segment->namespace_name, namespace_name) == 0) {
      return segment;
    }
  }
  return NULL;
}

static void lc_pouch_query_index_pending_segment_remove_namespace(
    lc_pouch *pouch, const char *namespace_name) {
  lc_pouch_query_index_pending_segment *segment;
  lc_pouch_query_index_pending_segment *previous;

  if (pouch == NULL || namespace_name == NULL) {
    return;
  }
  previous = NULL;
  segment = pouch->query_pending_segments;
  while (segment != NULL) {
    lc_pouch_query_index_pending_segment *next;

    next = segment->next;
    if (segment->namespace_name != NULL &&
        strcmp(segment->namespace_name, namespace_name) == 0) {
      if (previous != NULL) {
        previous->next = next;
      } else {
        pouch->query_pending_segments = next;
      }
      lc_pouch_query_index_pending_segment_cleanup(&pouch->allocator, segment);
      lc_free_with_allocator(&pouch->allocator, segment);
      return;
    }
    previous = segment;
    segment = next;
  }
}

static lc_pouch_query_index_pending_segment *
lc_pouch_query_index_pending_segment_detach_namespace(
    lc_pouch *pouch, const char *namespace_name) {
  lc_pouch_query_index_pending_segment *segment;
  lc_pouch_query_index_pending_segment *previous;

  if (pouch == NULL || namespace_name == NULL) {
    return NULL;
  }
  previous = NULL;
  segment = pouch->query_pending_segments;
  while (segment != NULL) {
    lc_pouch_query_index_pending_segment *next;

    next = segment->next;
    if (segment->namespace_name != NULL &&
        strcmp(segment->namespace_name, namespace_name) == 0) {
      if (previous != NULL) {
        previous->next = next;
      } else {
        pouch->query_pending_segments = next;
      }
      segment->next = NULL;
      return segment;
    }
    previous = segment;
    segment = next;
  }
  return NULL;
}

static int
lc_pouch_query_index_pending_entry_compare_key_hex_ptr(const void *left,
                                                       const void *right) {
  const lc_pouch_query_index_pending_entry *const *a;
  const lc_pouch_query_index_pending_entry *const *b;

  a = (const lc_pouch_query_index_pending_entry *const *)left;
  b = (const lc_pouch_query_index_pending_entry *const *)right;
  return strcmp((*a)->key_hex != NULL ? (*a)->key_hex : "",
                (*b)->key_hex != NULL ? (*b)->key_hex : "");
}

static int lc_pouch_query_index_pending_segment_append_delete(
    const lc_allocator *allocator,
    lc_pouch_query_index_pending_segment *segment,
    const lc_pouch_query_index_pending_entry *entry, lc_error *error) {
  int rc;

  if (segment == NULL || entry == NULL || entry->key_hex == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch pending delete requires segment and key", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_query_index_text_append_cstr(&segment->deletes, entry->key_hex,
                                             &segment->delete_hash, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_cstr(&segment->deletes, "\n",
                                               &segment->delete_hash, error);
  }
  if (rc == LC_OK) {
    ++segment->delete_count;
  }
  (void)allocator;
  return rc;
}

static int lc_pouch_query_index_pending_segment_append_term(
    const lc_allocator *allocator,
    lc_pouch_query_index_pending_segment *segment,
    const lc_pouch_query_index_term *term, unsigned long doc_id,
    lc_error *error) {
  char *allocated_exact_value_hex;
  const char *exact_value_hex;
  const char *indexed_exact_value_hex;
  char indexed_exact_value_type;
  int rc;

  allocated_exact_value_hex = NULL;
  exact_value_hex = NULL;
  indexed_exact_value_hex = NULL;
  indexed_exact_value_type = term->value_type;
  if (term->value_type == 's' && term->long_value != NULL) {
    exact_value_hex = term->value_hex;
    indexed_exact_value_hex = term->value_hex;
    indexed_exact_value_type = LC_POUCH_QUERY_INDEX_EXACT_HASH_TYPE;
  } else if (term->value_type == 'n') {
    if (term->canonical_value_hex != NULL) {
      exact_value_hex = term->canonical_value_hex;
      indexed_exact_value_hex = term->canonical_value_hex;
    } else {
      rc = lc_pouch_query_index_exact_generation_value_hex(
          allocator, term->value_hex, term->value_type,
          &allocated_exact_value_hex, error);
      if (rc != LC_OK) {
        return rc;
      }
      exact_value_hex = allocated_exact_value_hex;
      indexed_exact_value_hex = allocated_exact_value_hex;
    }
  } else {
    exact_value_hex = term->value_hex;
    indexed_exact_value_hex = term->value_hex;
  }
  rc = lc_pouch_query_index_generation_append_docid(
      allocator, &segment->exact_generation, &segment->exact_accumulator,
      &segment->exact_term_cache, term->field_hex, indexed_exact_value_hex,
      indexed_exact_value_type, doc_id, error);
  if (rc == LC_OK && term->value_type == 'n') {
    rc = lc_pouch_query_index_generation_append_docid(
        allocator, &segment->range_generation, &segment->range_accumulator,
        &segment->range_term_cache, term->field_hex, exact_value_hex,
        term->value_type, doc_id, error);
  }
  if (rc == LC_OK && term->value_type == 's') {
    if (term->derived_terms_ready) {
      if (term->long_value != NULL && term->text_prefix_hex != NULL) {
        rc = lc_pouch_query_index_generation_append_docid(
            allocator, &segment->text_generation, &segment->text_accumulator,
            &segment->text_term_cache, term->field_hex, term->text_prefix_hex,
            LC_POUCH_QUERY_INDEX_TEXT_PREFIX_TYPE, doc_id, error);
      } else if (term->long_value == NULL) {
        rc = lc_pouch_query_index_generation_append_docid(
            allocator, &segment->text_generation, &segment->text_accumulator,
            &segment->text_term_cache, term->field_hex, exact_value_hex, 's',
            doc_id, error);
      }
      if (rc == LC_OK) {
        size_t derived_index;

        for (derived_index = 0U;
             rc == LC_OK && derived_index < term->trigram_key_count;
             ++derived_index) {
          rc = lc_pouch_query_index_trigram_generation_append_docid(
              allocator, &segment->trigram_generation,
              &segment->trigram_accumulator, &segment->trigram_field_cache,
              &segment->trigram_term_cache, term->field_hex,
              term->trigram_keys[derived_index], doc_id, error);
        }
      }
      if (rc == LC_OK && term->temporal_value_hex != NULL) {
        rc = lc_pouch_query_index_generation_append_docid(
            allocator, &segment->temporal_generation,
            &segment->temporal_accumulator, &segment->temporal_term_cache,
            term->field_hex, term->temporal_value_hex, 'n', doc_id, error);
      }
    } else {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch pending segment requires precomputed string "
                        "terms",
                        NULL, NULL, "pouch");
    }
  }
  lc_free_with_allocator(allocator, allocated_exact_value_hex);
  return rc;
}

static void lc_pouch_query_index_pending_segment_remove_docid_for_term(
    lc_pouch_index_term_generation *generation,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_query_index_term_cache *cache, const char *field_hex,
    const char *value_hex, char value_type, unsigned long doc_id) {
  unsigned long hash;
  unsigned long term_id;

  if (generation == NULL || accumulator == NULL || cache == NULL ||
      field_hex == NULL || value_hex == NULL) {
    return;
  }
  hash = lc_pouch_query_index_term_cache_hash(field_hex, value_hex, value_type);
  if (!lc_pouch_query_index_term_cache_find(cache, field_hex, value_hex,
                                            value_type, hash, &term_id)) {
    return;
  }
  (void)generation;
  lc_pouch_query_index_exact_generation_accumulator_remove(accumulator, term_id,
                                                           doc_id);
}

static int lc_pouch_query_index_pending_segment_remove_term(
    const lc_allocator *allocator,
    lc_pouch_query_index_pending_segment *segment,
    const lc_pouch_query_index_term *term, unsigned long doc_id,
    lc_error *error) {
  char *allocated_exact_value_hex;
  const char *exact_value_hex;
  const char *indexed_exact_value_hex;
  char indexed_exact_value_type;
  int rc;

  allocated_exact_value_hex = NULL;
  exact_value_hex = NULL;
  indexed_exact_value_hex = NULL;
  indexed_exact_value_type = term->value_type;
  rc = LC_OK;
  if (term->value_type == 's' && term->long_value != NULL) {
    exact_value_hex = term->value_hex;
    indexed_exact_value_hex = term->value_hex;
    indexed_exact_value_type = LC_POUCH_QUERY_INDEX_EXACT_HASH_TYPE;
  } else if (term->value_type == 'n') {
    if (term->canonical_value_hex != NULL) {
      exact_value_hex = term->canonical_value_hex;
      indexed_exact_value_hex = term->canonical_value_hex;
    } else {
      rc = lc_pouch_query_index_exact_generation_value_hex(
          allocator, term->value_hex, term->value_type,
          &allocated_exact_value_hex, error);
      if (rc != LC_OK) {
        return rc;
      }
      exact_value_hex = allocated_exact_value_hex;
      indexed_exact_value_hex = allocated_exact_value_hex;
    }
  } else {
    exact_value_hex = term->value_hex;
    indexed_exact_value_hex = term->value_hex;
  }
  lc_pouch_query_index_pending_segment_remove_docid_for_term(
      &segment->exact_generation, &segment->exact_accumulator,
      &segment->exact_term_cache, term->field_hex, indexed_exact_value_hex,
      indexed_exact_value_type, doc_id);
  if (term->value_type == 'n') {
    lc_pouch_query_index_pending_segment_remove_docid_for_term(
        &segment->range_generation, &segment->range_accumulator,
        &segment->range_term_cache, term->field_hex, exact_value_hex,
        term->value_type, doc_id);
  }
  if (term->value_type == 's' && term->derived_terms_ready) {
    if (term->long_value != NULL && term->text_prefix_hex != NULL) {
      lc_pouch_query_index_pending_segment_remove_docid_for_term(
          &segment->text_generation, &segment->text_accumulator,
          &segment->text_term_cache, term->field_hex, term->text_prefix_hex,
          LC_POUCH_QUERY_INDEX_TEXT_PREFIX_TYPE, doc_id);
    } else if (term->long_value == NULL) {
      lc_pouch_query_index_pending_segment_remove_docid_for_term(
          &segment->text_generation, &segment->text_accumulator,
          &segment->text_term_cache, term->field_hex, exact_value_hex, 's',
          doc_id);
    }
    if (term->trigram_key_count > 0U) {
      size_t derived_index;

      for (derived_index = 0U;
           rc == LC_OK && derived_index < term->trigram_key_count;
           ++derived_index) {
        rc = lc_pouch_query_index_trigram_generation_remove_docid(
            allocator, &segment->trigram_accumulator,
            &segment->trigram_field_cache, &segment->trigram_term_cache,
            term->field_hex, term->trigram_keys[derived_index], doc_id, error);
      }
    }
    if (term->temporal_value_hex != NULL) {
      lc_pouch_query_index_pending_segment_remove_docid_for_term(
          &segment->temporal_generation, &segment->temporal_accumulator,
          &segment->temporal_term_cache, term->field_hex,
          term->temporal_value_hex, 'n', doc_id);
    }
  }
  lc_free_with_allocator(allocator, allocated_exact_value_hex);
  return rc;
}

static int lc_pouch_query_index_pending_entry_collect_aggregate_terms(
    const lc_allocator *allocator,
    const lc_pouch_query_index_pending_entry *entry,
    lc_pouch_query_index_trigram_value_set *trigrams,
    lc_pouch_query_index_token_value_set *tokens, lc_error *error) {
  size_t index;
  size_t token_stride;
  int rc;

  if (entry == NULL || entry->deleted) {
    return LC_OK;
  }
  token_stride = lc_pouch_query_index_token_hex_stride();
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < entry->term_count; ++index) {
    const lc_pouch_query_index_term *term;
    size_t derived_index;

    if (index > 0U && lc_pouch_query_index_term_equal(&entry->terms[index - 1U],
                                                      &entry->terms[index])) {
      continue;
    }
    term = &entry->terms[index];
    if (term->value_type != 's' || !term->derived_terms_ready) {
      continue;
    }
    for (derived_index = 0U;
         rc == LC_OK && derived_index < term->trigram_key_count;
         ++derived_index) {
      rc = lc_pouch_query_index_trigram_value_set_add(
          allocator, trigrams, term->trigram_keys[derived_index], error);
    }
    for (derived_index = 0U;
         rc == LC_OK && derived_index < term->token_value_count;
         ++derived_index) {
      rc = lc_pouch_query_index_token_value_set_add(
          allocator, tokens,
          term->token_values + (derived_index * token_stride), error);
    }
  }
  return rc;
}

static int lc_pouch_query_index_pending_segment_append_entry_aggregate_terms(
    const lc_allocator *allocator,
    lc_pouch_query_index_pending_segment *segment,
    const lc_pouch_query_index_pending_entry *entry, unsigned long doc_id,
    lc_error *error) {
  lc_pouch_query_index_trigram_value_set trigrams;
  lc_pouch_query_index_token_value_set tokens;
  size_t index;
  size_t token_stride;
  int rc;

  memset(&trigrams, 0, sizeof(trigrams));
  memset(&tokens, 0, sizeof(tokens));
  rc = lc_pouch_query_index_pending_entry_collect_aggregate_terms(
      allocator, entry, &trigrams, &tokens, error);
  for (index = 0U; rc == LC_OK && index < trigrams.count; ++index) {
    rc = lc_pouch_query_index_trigram_generation_append_docid(
        allocator, &segment->trigram_generation, &segment->trigram_accumulator,
        &segment->trigram_field_cache, &segment->trigram_term_cache,
        LC_POUCH_QUERY_INDEX_ANY_TEXT_FIELD_HEX, trigrams.items[index], doc_id,
        error);
  }
  token_stride = lc_pouch_query_index_token_hex_stride();
  for (index = 0U; rc == LC_OK && index < tokens.count; ++index) {
    rc = lc_pouch_query_index_generation_append_docid(
        allocator, &segment->text_generation, &segment->text_accumulator,
        &segment->text_term_cache, LC_POUCH_QUERY_INDEX_ANY_TEXT_FIELD_HEX,
        tokens.items + (index * token_stride),
        LC_POUCH_QUERY_INDEX_TEXT_TOKEN_TYPE, doc_id, error);
  }
  lc_pouch_query_index_trigram_value_set_cleanup(allocator, &trigrams);
  lc_pouch_query_index_token_value_set_cleanup(allocator, &tokens);
  return rc;
}

static int lc_pouch_query_index_pending_segment_remove_entry_aggregate_terms(
    const lc_allocator *allocator,
    lc_pouch_query_index_pending_segment *segment,
    const lc_pouch_query_index_pending_entry *entry, unsigned long doc_id,
    lc_error *error) {
  lc_pouch_query_index_trigram_value_set trigrams;
  lc_pouch_query_index_token_value_set tokens;
  size_t index;
  size_t token_stride;
  int rc;

  memset(&trigrams, 0, sizeof(trigrams));
  memset(&tokens, 0, sizeof(tokens));
  rc = lc_pouch_query_index_pending_entry_collect_aggregate_terms(
      allocator, entry, &trigrams, &tokens, error);
  for (index = 0U; rc == LC_OK && index < trigrams.count; ++index) {
    rc = lc_pouch_query_index_trigram_generation_remove_docid(
        allocator, &segment->trigram_accumulator, &segment->trigram_field_cache,
        &segment->trigram_term_cache, LC_POUCH_QUERY_INDEX_ANY_TEXT_FIELD_HEX,
        trigrams.items[index], doc_id, error);
  }
  token_stride = lc_pouch_query_index_token_hex_stride();
  for (index = 0U; rc == LC_OK && index < tokens.count; ++index) {
    lc_pouch_query_index_pending_segment_remove_docid_for_term(
        &segment->text_generation, &segment->text_accumulator,
        &segment->text_term_cache, LC_POUCH_QUERY_INDEX_ANY_TEXT_FIELD_HEX,
        tokens.items + (index * token_stride),
        LC_POUCH_QUERY_INDEX_TEXT_TOKEN_TYPE, doc_id);
  }
  lc_pouch_query_index_trigram_value_set_cleanup(allocator, &trigrams);
  lc_pouch_query_index_token_value_set_cleanup(allocator, &tokens);
  return rc;
}

static int lc_pouch_query_index_pending_segment_append_entry(
    const lc_allocator *allocator,
    lc_pouch_query_index_pending_segment *segment,
    lc_pouch_query_index_pending_entry *entry, lc_error *error) {
  char line[256];
  char version_text[32];
  char bytes_text[32];
  unsigned long doc_id;
  size_t index;
  int written;
  int rc;

  if (segment == NULL || entry == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch pending segment append requires segment and "
                        "entry",
                        NULL, NULL, NULL);
  }
  if (entry->deleted) {
    return lc_pouch_query_index_pending_segment_append_delete(
        allocator, segment, entry, error);
  }
  if (lc_u64_format_base10((lc_u64)entry->version, version_text,
                           sizeof(version_text)) < 0 ||
      lc_u64_format_base10((lc_u64)entry->bytes, bytes_text,
                           sizeof(bytes_text)) < 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch pending segment row exceeds local limit", NULL,
                        NULL, NULL);
  }
  written =
      snprintf(line, sizeof(line), "row %s %s %d %d ", version_text, bytes_text,
               entry->has_query_hidden ? 1 : 0, entry->query_hidden ? 1 : 0);
  if (written < 0 || (size_t)written >= sizeof(line)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch pending segment row exceeds local limit", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_query_index_text_append(NULL, line, (size_t)written,
                                        &segment->row_hash, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_cstr(NULL, entry->key_hex,
                                               &segment->row_hash, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_cstr(NULL, " ", &segment->row_hash,
                                               error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_cstr(NULL, entry->content_type_hex,
                                               &segment->row_hash, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_cstr(NULL, " ", &segment->row_hash,
                                               error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_cstr(NULL, entry->etag_hex,
                                               &segment->row_hash, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_doc_table_append_owned(
        &segment->doc_table, entry->key_hex, entry->version, entry->bytes,
        entry->has_query_hidden, entry->query_hidden, &doc_id, allocator,
        error);
    if (rc == LC_OK) {
      entry->doc_id = doc_id;
      entry->has_doc_id = 1;
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_cstr(NULL, "\n", &segment->row_hash,
                                               error);
  }
  if (rc != LC_OK) {
    return rc;
  }
  if (!entry->term_index_complete) {
    segment->term_index_complete = 0;
  }
  if (!entry->presence_index_complete) {
    segment->presence_index_complete = 0;
  }
  for (index = 0U; rc == LC_OK && index < entry->term_count; ++index) {
    if (index > 0U && lc_pouch_query_index_term_equal(&entry->terms[index - 1U],
                                                      &entry->terms[index])) {
      continue;
    }
    rc = lc_pouch_query_index_pending_segment_append_term(
        allocator, segment, &entry->terms[index], doc_id, error);
    if (rc == LC_OK) {
      ++segment->term_count;
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_pending_segment_append_entry_aggregate_terms(
        allocator, segment, entry, doc_id, error);
  }
  for (index = 0U; rc == LC_OK && index < entry->presence_count; ++index) {
    unsigned long presence_term_id;

    if (index > 0U &&
        lc_pouch_query_index_presence_equal(&entry->presences[index - 1U],
                                            &entry->presences[index])) {
      continue;
    }
    presence_term_id = 0UL;
    rc = lc_pouch_query_index_term_find_or_add_cached(
        allocator, &segment->presence_generation, &segment->presence_term_cache,
        entry->presences[index].field_hex, "-", 'z', &presence_term_id, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_exact_generation_accumulator_append(
          allocator, &segment->presence_accumulator, presence_term_id, doc_id,
          error);
    }
    if (rc == LC_OK) {
      ++segment->presence_count;
    }
  }
  if (rc == LC_OK) {
    char *last_key_hex;

    last_key_hex = lc_strdup_with_allocator(allocator, entry->key_hex);
    if (last_key_hex == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch pending segment key", NULL,
                          NULL, NULL);
    }
    lc_free_with_allocator(allocator, segment->last_key_hex);
    segment->last_key_hex = last_key_hex;
  }
  return rc;
}

static int lc_pouch_query_index_pending_segment_remove_entry_terms(
    const lc_allocator *allocator,
    lc_pouch_query_index_pending_segment *segment,
    const lc_pouch_query_index_pending_entry *entry, unsigned long doc_id,
    lc_error *error) {
  size_t index;
  int rc;

  rc = LC_OK;
  if (entry == NULL || entry->deleted) {
    return LC_OK;
  }
  for (index = 0U; rc == LC_OK && index < entry->term_count; ++index) {
    if (index > 0U && lc_pouch_query_index_term_equal(&entry->terms[index - 1U],
                                                      &entry->terms[index])) {
      continue;
    }
    rc = lc_pouch_query_index_pending_segment_remove_term(
        allocator, segment, &entry->terms[index], doc_id, error);
    if (rc == LC_OK && segment->term_count > 0UL) {
      --segment->term_count;
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_pending_segment_remove_entry_aggregate_terms(
        allocator, segment, entry, doc_id, error);
  }
  for (index = 0U; rc == LC_OK && index < entry->presence_count; ++index) {
    unsigned long presence_term_id;
    unsigned long hash;

    if (index > 0U &&
        lc_pouch_query_index_presence_equal(&entry->presences[index - 1U],
                                            &entry->presences[index])) {
      continue;
    }
    hash = lc_pouch_query_index_term_cache_hash(
        entry->presences[index].field_hex, "-", 'z');
    presence_term_id = 0UL;
    if (lc_pouch_query_index_term_cache_find(
            &segment->presence_term_cache, entry->presences[index].field_hex,
            "-", 'z', hash, &presence_term_id)) {
      lc_pouch_query_index_exact_generation_accumulator_remove(
          &segment->presence_accumulator, presence_term_id, doc_id);
    }
    if (segment->presence_count > 0UL) {
      --segment->presence_count;
    }
  }
  return rc;
}

static int lc_pouch_query_index_pending_segment_replace_entry(
    const lc_allocator *allocator,
    lc_pouch_query_index_pending_segment *segment,
    const lc_pouch_query_index_pending_entry *old_entry,
    lc_pouch_query_index_pending_entry *new_entry, lc_error *error) {
  lc_pouch_index_doc *doc;
  unsigned long doc_id;
  size_t new_index;
  size_t old_index;
  int found;
  int rc;

  if (segment == NULL || old_entry == NULL || new_entry == NULL ||
      old_entry->key_hex == NULL || new_entry->key_hex == NULL) {
    return LC_ERR_INVALID;
  }
  if (old_entry->has_doc_id) {
    doc_id = old_entry->doc_id;
    found = doc_id < (unsigned long)segment->doc_table.count;
    rc = LC_OK;
  } else {
    doc_id = 0UL;
    found = 0;
    rc = lc_pouch_index_doc_table_find_key_hex(
        &segment->doc_table, old_entry->key_hex, &doc_id, &found, error);
  }
  if (rc != LC_OK || !found ||
      doc_id >= (unsigned long)segment->doc_table.count) {
    return rc == LC_OK ? LC_ERR_INVALID : rc;
  }
  new_entry->doc_id = doc_id;
  new_entry->has_doc_id = 1;
  doc = &segment->doc_table.items[doc_id];
  if (!doc->owns_key_hex || strcmp(doc->key_hex, new_entry->key_hex) != 0) {
    char *owned_key_hex;

    owned_key_hex = lc_strdup_with_allocator(allocator, new_entry->key_hex);
    if (owned_key_hex == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch pending doc key", NULL,
                          NULL, NULL);
    }
    if (doc->owns_key_hex) {
      lc_free_with_allocator(allocator, (char *)doc->key_hex);
    }
    doc->key_hex = owned_key_hex;
    doc->owns_key_hex = 1;
  }
  doc->version = new_entry->version;
  doc->bytes = new_entry->bytes;
  doc->has_query_hidden = new_entry->has_query_hidden;
  doc->query_hidden = new_entry->query_hidden;
  if (new_entry->deleted) {
    rc = lc_pouch_query_index_pending_segment_remove_entry_terms(
        allocator, segment, old_entry, doc_id, error);
    if (rc != LC_OK) {
      return rc;
    }
    doc->has_query_hidden = 1;
    doc->query_hidden = 1;
    return lc_pouch_query_index_pending_segment_append_delete(
        allocator, segment, new_entry, error);
  }
  if (!new_entry->term_index_complete) {
    segment->term_index_complete = 0;
  }
  if (!new_entry->presence_index_complete) {
    segment->presence_index_complete = 0;
  }
  old_index = 0U;
  new_index = 0U;
  while (rc == LC_OK && (old_index < old_entry->term_count ||
                         new_index < new_entry->term_count)) {
    int cmp;

    if (old_index > 0U && old_index < old_entry->term_count &&
        lc_pouch_query_index_term_equal(&old_entry->terms[old_index - 1U],
                                        &old_entry->terms[old_index])) {
      ++old_index;
      continue;
    }
    if (new_index > 0U && new_index < new_entry->term_count &&
        lc_pouch_query_index_term_equal(&new_entry->terms[new_index - 1U],
                                        &new_entry->terms[new_index])) {
      ++new_index;
      continue;
    }
    if (old_index >= old_entry->term_count) {
      cmp = 1;
    } else if (new_index >= new_entry->term_count) {
      cmp = -1;
    } else {
      cmp = lc_pouch_query_index_term_compare(&old_entry->terms[old_index],
                                              &new_entry->terms[new_index]);
    }
    if (cmp == 0) {
      ++old_index;
      ++new_index;
    } else if (cmp < 0) {
      rc = lc_pouch_query_index_pending_segment_remove_term(
          allocator, segment, &old_entry->terms[old_index], doc_id, error);
      if (rc == LC_OK && segment->term_count > 0UL) {
        --segment->term_count;
      }
      ++old_index;
    } else {
      rc = lc_pouch_query_index_pending_segment_append_term(
          allocator, segment, &new_entry->terms[new_index], doc_id, error);
      if (rc == LC_OK) {
        ++segment->term_count;
      }
      ++new_index;
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_pending_segment_remove_entry_aggregate_terms(
        allocator, segment, old_entry, doc_id, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_pending_segment_append_entry_aggregate_terms(
        allocator, segment, new_entry, doc_id, error);
  }
  old_index = 0U;
  new_index = 0U;
  while (rc == LC_OK && (old_index < old_entry->presence_count ||
                         new_index < new_entry->presence_count)) {
    int cmp;

    if (old_index > 0U && old_index < old_entry->presence_count &&
        lc_pouch_query_index_presence_equal(
            &old_entry->presences[old_index - 1U],
            &old_entry->presences[old_index])) {
      ++old_index;
      continue;
    }
    if (new_index > 0U && new_index < new_entry->presence_count &&
        lc_pouch_query_index_presence_equal(
            &new_entry->presences[new_index - 1U],
            &new_entry->presences[new_index])) {
      ++new_index;
      continue;
    }
    if (old_index >= old_entry->presence_count) {
      cmp = 1;
    } else if (new_index >= new_entry->presence_count) {
      cmp = -1;
    } else {
      cmp = lc_pouch_query_index_presence_compare(
          &old_entry->presences[old_index], &new_entry->presences[new_index]);
    }
    if (cmp == 0) {
      ++old_index;
      ++new_index;
    } else if (cmp < 0) {
      unsigned long hash;
      unsigned long presence_term_id;

      hash = lc_pouch_query_index_term_cache_hash(
          old_entry->presences[old_index].field_hex, "-", 'z');
      presence_term_id = 0UL;
      if (lc_pouch_query_index_term_cache_find(
              &segment->presence_term_cache,
              old_entry->presences[old_index].field_hex, "-", 'z', hash,
              &presence_term_id)) {
        lc_pouch_query_index_exact_generation_accumulator_remove(
            &segment->presence_accumulator, presence_term_id, doc_id);
      }
      if (segment->presence_count > 0UL) {
        --segment->presence_count;
      }
      ++old_index;
    } else {
      unsigned long presence_term_id;

      presence_term_id = 0UL;
      rc = lc_pouch_query_index_term_find_or_add_cached(
          allocator, &segment->presence_generation,
          &segment->presence_term_cache,
          new_entry->presences[new_index].field_hex, "-", 'z',
          &presence_term_id, error);
      if (rc == LC_OK) {
        rc = lc_pouch_query_index_exact_generation_accumulator_append(
            allocator, &segment->presence_accumulator, presence_term_id, doc_id,
            error);
      }
      if (rc == LC_OK) {
        ++segment->presence_count;
      }
      ++new_index;
    }
  }
  return rc;
}

static lc_pouch_query_index_pending_segment *
lc_pouch_query_index_pending_segment_build_entries(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_query_index_pending_entry *pending_entries, size_t count,
    lc_error *error) {
  lc_pouch_query_index_pending_segment *segment;
  lc_pouch_query_index_pending_entry **entries;
  lc_pouch_query_index_pending_entry *entry;
  size_t index;
  int rc;

  if (pouch == NULL || namespace_name == NULL) {
    lc_error_set(error, LC_ERR_INVALID, 0L,
                 "pouch pending segment build requires pouch and namespace",
                 NULL, NULL, NULL);
    return NULL;
  }
  if (count == 0U) {
    return NULL;
  }
  entries = (lc_pouch_query_index_pending_entry **)lc_alloc_with_allocator(
      &pouch->allocator, count * sizeof(*entries));
  if (entries == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch pending segment entries", NULL, NULL,
                 NULL);
    return NULL;
  }
  index = 0U;
  for (entry = pending_entries; entry != NULL && index < count;
       entry = entry->next) {
    if (entry->namespace_name == NULL ||
        strcmp(entry->namespace_name, namespace_name) != 0) {
      continue;
    }
    entries[index++] = entry;
  }
  count = index;
  if (count == 0U) {
    lc_free_with_allocator(&pouch->allocator, entries);
    return NULL;
  }
  qsort(entries, count, sizeof(entries[0]),
        lc_pouch_query_index_pending_entry_compare_key_hex_ptr);
  segment = lc_pouch_query_index_pending_segment_new(&pouch->allocator,
                                                     namespace_name, error);
  if (segment == NULL) {
    lc_free_with_allocator(&pouch->allocator, entries);
    return NULL;
  }
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < count; ++index) {
    rc = lc_pouch_query_index_pending_segment_append_entry(
        &pouch->allocator, segment, entries[index], error);
  }
  lc_free_with_allocator(&pouch->allocator, entries);
  if (rc != LC_OK) {
    lc_pouch_query_index_pending_segment_cleanup(&pouch->allocator, segment);
    lc_free_with_allocator(&pouch->allocator, segment);
    return NULL;
  }
  return segment;
}

static int lc_pouch_query_index_pending_segment_rebuild_namespace(
    lc_pouch *pouch, const char *namespace_name, lc_error *error) {
  lc_pouch_query_index_pending_segment *segment;
  lc_pouch_query_index_pending_entry *entry;
  size_t count;

  if (pouch == NULL || namespace_name == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch pending segment rebuild requires pouch and "
                        "namespace",
                        NULL, NULL, NULL);
  }
  lc_pouch_query_index_pending_segment_remove_namespace(pouch, namespace_name);
  count = 0U;
  for (entry = pouch->query_pending_index; entry != NULL; entry = entry->next) {
    if (entry->namespace_name != NULL &&
        strcmp(entry->namespace_name, namespace_name) == 0) {
      ++count;
    }
  }
  if (count == 0U) {
    return LC_OK;
  }
  segment = lc_pouch_query_index_pending_segment_build_entries(
      pouch, namespace_name, pouch->query_pending_index, count, error);
  if (segment == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  segment->next = pouch->query_pending_segments;
  pouch->query_pending_segments = segment;
  return LC_OK;
}

static int lc_pouch_query_index_pending_segment_note_entry(
    lc_pouch *pouch, lc_pouch_query_index_pending_entry *entry,
    lc_pouch_query_index_pending_entry *replaced_entries, lc_error *error) {
  lc_pouch_query_index_pending_segment *segment;
  int rc;

  if (pouch == NULL || entry == NULL || entry->namespace_name == NULL) {
    return LC_OK;
  }
  if (replaced_entries != NULL && replaced_entries->next != NULL) {
    return lc_pouch_query_index_pending_segment_rebuild_namespace(
        pouch, entry->namespace_name, error);
  }
  segment =
      lc_pouch_query_index_pending_segment_find(pouch, entry->namespace_name);
  if (segment != NULL && replaced_entries != NULL) {
    rc = lc_pouch_query_index_pending_segment_replace_entry(
        &pouch->allocator, segment, replaced_entries, entry, error);
    if (rc == LC_OK) {
      return LC_OK;
    }
    if (error != NULL) {
      lc_error_cleanup(error);
      lc_error_init(error);
    }
    return lc_pouch_query_index_pending_segment_rebuild_namespace(
        pouch, entry->namespace_name, error);
  }
  if (segment == NULL) {
    segment = lc_pouch_query_index_pending_segment_new(
        &pouch->allocator, entry->namespace_name, error);
    if (segment == NULL) {
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    segment->next = pouch->query_pending_segments;
    pouch->query_pending_segments = segment;
  }
  rc = lc_pouch_query_index_pending_segment_append_entry(&pouch->allocator,
                                                         segment, entry, error);
  if (rc == LC_ERR_INVALID) {
    lc_error_cleanup(error);
    lc_error_init(error);
    rc = lc_pouch_query_index_pending_segment_rebuild_namespace(
        pouch, entry->namespace_name, error);
  }
  return rc;
}

static int lc_pouch_query_index_pending_segment_note_write(
    lc_pouch *pouch, lc_pouch_query_index_pending_entry *entry,
    lc_pouch_query_index_pending_entry *replaced_entries, lc_error *error) {
  return lc_pouch_query_index_pending_segment_note_entry(
      pouch, entry, replaced_entries, error);
}

static int lc_pouch_query_index_pending_segment_note_delete(
    lc_pouch *pouch, lc_pouch_query_index_pending_entry *entry,
    lc_pouch_query_index_pending_entry *replaced_entries, lc_error *error) {
  return lc_pouch_query_index_pending_segment_note_entry(
      pouch, entry, replaced_entries, error);
}

static int lc_pouch_query_index_pending_segment_encode_generation(
    const lc_allocator *allocator, lc_pouch_index_term_generation *generation,
    lc_pouch_generation index_seq, unsigned long row_count,
    unsigned long row_hash, char **out, size_t *out_length, lc_error *error) {
  generation->index_seq = index_seq;
  generation->row_count = row_count;
  generation->row_hash = row_hash;
  return lc_pouch_index_term_generation_encode(generation, allocator, out,
                                               out_length, error);
}

static int lc_pouch_query_index_pending_segment_encode(
    lc_pouch_query_index_pending_segment *segment,
    lc_pouch_generation index_seq, lc_pouch_query_index_text *header_artifact,
    unsigned long *row_hash_out, unsigned long *delete_count_out,
    unsigned long *delete_hash_out, char **doc_table_generation_out,
    size_t *doc_table_generation_length_out, char **exact_term_generation_out,
    size_t *exact_term_generation_length_out,
    char **presence_term_generation_out,
    size_t *presence_term_generation_length_out,
    char **range_term_generation_out, size_t *range_term_generation_length_out,
    char **text_term_generation_out, size_t *text_term_generation_length_out,
    char **trigram_term_generation_out,
    size_t *trigram_term_generation_length_out,
    char **temporal_term_generation_out,
    size_t *temporal_term_generation_length_out, lc_error *error) {
  const lc_allocator *allocator;
  unsigned long row_count;
  int rc;

  if (segment == NULL || header_artifact == NULL || row_hash_out == NULL ||
      delete_count_out == NULL || delete_hash_out == NULL ||
      doc_table_generation_out == NULL ||
      doc_table_generation_length_out == NULL ||
      exact_term_generation_out == NULL ||
      exact_term_generation_length_out == NULL ||
      presence_term_generation_out == NULL ||
      presence_term_generation_length_out == NULL ||
      range_term_generation_out == NULL ||
      range_term_generation_length_out == NULL ||
      text_term_generation_out == NULL ||
      text_term_generation_length_out == NULL ||
      trigram_term_generation_out == NULL ||
      trigram_term_generation_length_out == NULL ||
      temporal_term_generation_out == NULL ||
      temporal_term_generation_length_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch pending segment encode requires outputs", NULL,
                        NULL, NULL);
  }
  allocator = segment->deletes.allocator;
  memset(header_artifact, 0, sizeof(*header_artifact));
  header_artifact->allocator = allocator;
  row_count = (unsigned long)segment->doc_table.count;
  *row_hash_out = segment->row_hash;
  *delete_count_out = segment->delete_count;
  *delete_hash_out = segment->delete_hash;
  rc = lc_pouch_query_index_text_append_cstr(
      header_artifact, "format=" LC_POUCH_QUERY_INDEX_FORMAT "\n", NULL, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        header_artifact, "version", LC_POUCH_QUERY_INDEX_VERSION, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_u64_line(
        header_artifact, "state_index_seq", index_seq, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        header_artifact, "row_count", row_count, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        header_artifact, "summary_hash", segment->row_hash, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        header_artifact, "term_count", segment->term_count, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        header_artifact, "term_hash", segment->term_hash, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        header_artifact, "term_index_complete",
        segment->term_index_complete ? 1UL : 0UL, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        header_artifact, "presence_count", segment->presence_count, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        header_artifact, "presence_hash", segment->presence_hash, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        header_artifact, "presence_index_complete",
        segment->presence_index_complete ? 1UL : 0UL, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        header_artifact, "term_field_count", 0UL, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        header_artifact, "term_value_count", 0UL, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_doc_table_generation_encode(
        &segment->doc_table, index_seq, segment->row_hash, allocator,
        doc_table_generation_out, doc_table_generation_length_out, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        allocator, &segment->exact_accumulator, &segment->exact_generation,
        error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        allocator, &segment->presence_accumulator,
        &segment->presence_generation, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        allocator, &segment->range_accumulator, &segment->range_generation,
        error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        allocator, &segment->text_accumulator, &segment->text_generation,
        error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        allocator, &segment->trigram_accumulator, &segment->trigram_generation,
        error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        allocator, &segment->temporal_accumulator,
        &segment->temporal_generation, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_pending_segment_encode_generation(
        allocator, &segment->exact_generation, index_seq, row_count,
        segment->row_hash, exact_term_generation_out,
        exact_term_generation_length_out, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_pending_segment_encode_generation(
        allocator, &segment->presence_generation, index_seq, row_count,
        segment->row_hash, presence_term_generation_out,
        presence_term_generation_length_out, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_pending_segment_encode_generation(
        allocator, &segment->range_generation, index_seq, row_count,
        segment->row_hash, range_term_generation_out,
        range_term_generation_length_out, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_pending_segment_encode_generation(
        allocator, &segment->text_generation, index_seq, row_count,
        segment->row_hash, text_term_generation_out,
        text_term_generation_length_out, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_pending_segment_encode_generation(
        allocator, &segment->trigram_generation, index_seq, row_count,
        segment->row_hash, trigram_term_generation_out,
        trigram_term_generation_length_out, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_pending_segment_encode_generation(
        allocator, &segment->temporal_generation, index_seq, row_count,
        segment->row_hash, temporal_term_generation_out,
        temporal_term_generation_length_out, error);
  }
  return rc;
}

static int lc_pouch_query_index_exact_generation_accumulator_flush(
    const lc_allocator *allocator,
    lc_pouch_query_index_exact_generation_accumulator *accumulator,
    lc_pouch_index_term_generation *generation, lc_error *error) {
  size_t index;
  int rc;
  (void)allocator;

  if (accumulator == NULL || generation == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch exact generation flush requires accumulator "
                        "and generation",
                        NULL, NULL, NULL);
  }
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < accumulator->count; ++index) {
    lc_pouch_query_index_exact_generation_docids_normalize(
        &accumulator->items[index]);
    rc = lc_pouch_query_index_exact_generation_flush_posting(
        allocator, generation, accumulator->items[index].term_id,
        &accumulator->items[index].docids, error);
  }
  return rc;
}

static int lc_pouch_query_index_build_text(
    lc_pouch_generation index_seq, lc_pouch_query_index_summary *summary,
    lc_pouch_query_index_text *out, unsigned long *row_hash_out,
    unsigned long *term_count_out, unsigned long *term_hash_out,
    unsigned long *term_field_count_out, unsigned long *term_value_count_out,
    unsigned long *presence_count_out, unsigned long *presence_hash_out,
    char **doc_table_generation_out, size_t *doc_table_generation_length_out,
    char **exact_term_generation_out, size_t *exact_term_generation_length_out,
    char **presence_term_generation_out,
    size_t *presence_term_generation_length_out,
    char **range_term_generation_out, size_t *range_term_generation_length_out,
    char **text_term_generation_out, size_t *text_term_generation_length_out,
    char **trigram_term_generation_out,
    size_t *trigram_term_generation_length_out,
    char **temporal_term_generation_out,
    size_t *temporal_term_generation_length_out, lc_error *error) {
  lc_pouch_query_index_text header;
  lc_pouch_index_doc_table doc_table;
  lc_pouch_index_term_generation exact_generation;
  lc_pouch_index_term_generation presence_generation;
  lc_pouch_index_term_generation range_generation;
  lc_pouch_index_term_generation text_generation;
  lc_pouch_index_term_generation trigram_generation;
  lc_pouch_index_term_generation temporal_generation;
  lc_pouch_query_index_exact_generation_accumulator exact_accumulator;
  lc_pouch_query_index_exact_generation_accumulator presence_accumulator;
  lc_pouch_query_index_exact_generation_accumulator range_accumulator;
  lc_pouch_query_index_exact_generation_accumulator text_accumulator;
  lc_pouch_query_index_exact_generation_accumulator trigram_accumulator;
  lc_pouch_query_index_exact_generation_accumulator temporal_accumulator;
  lc_pouch_query_index_field_cache trigram_field_cache;
  lc_pouch_query_index_trigram_term_cache trigram_term_cache;
  lc_pouch_query_index_term_cache exact_term_cache;
  lc_pouch_query_index_term_cache presence_term_cache;
  lc_pouch_query_index_term_cache range_term_cache;
  lc_pouch_query_index_term_cache text_term_cache;
  lc_pouch_query_index_term_cache temporal_term_cache;
  char line[256];
  unsigned long row_hash;
  unsigned long term_hash;
  unsigned long term_line_count;
  unsigned long term_field_line_count;
  unsigned long term_value_line_count;
  unsigned long presence_hash;
  unsigned long presence_line_count;
  unsigned long doc_id;
  const char *last_term_key_hex;
  const char *last_presence_key_hex;
  size_t index;
  int found;
  int written;
  int rc;

  if (summary == NULL || out == NULL || row_hash_out == NULL ||
      term_count_out == NULL || term_hash_out == NULL ||
      term_field_count_out == NULL || term_value_count_out == NULL ||
      presence_count_out == NULL || presence_hash_out == NULL ||
      doc_table_generation_out == NULL ||
      doc_table_generation_length_out == NULL ||
      exact_term_generation_out == NULL ||
      exact_term_generation_length_out == NULL ||
      presence_term_generation_out == NULL ||
      presence_term_generation_length_out == NULL ||
      range_term_generation_out == NULL ||
      range_term_generation_length_out == NULL ||
      text_term_generation_out == NULL ||
      text_term_generation_length_out == NULL ||
      trigram_term_generation_out == NULL ||
      trigram_term_generation_length_out == NULL ||
      temporal_term_generation_out == NULL ||
      temporal_term_generation_length_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index build requires summary outputs",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  out->allocator = summary->allocator;
  *doc_table_generation_out = NULL;
  *doc_table_generation_length_out = 0U;
  *exact_term_generation_out = NULL;
  *exact_term_generation_length_out = 0U;
  *presence_term_generation_out = NULL;
  *presence_term_generation_length_out = 0U;
  *range_term_generation_out = NULL;
  *range_term_generation_length_out = 0U;
  *text_term_generation_out = NULL;
  *text_term_generation_length_out = 0U;
  *trigram_term_generation_out = NULL;
  *trigram_term_generation_length_out = 0U;
  *temporal_term_generation_out = NULL;
  *temporal_term_generation_length_out = 0U;
  *row_hash_out = lc_pouch_query_index_hash_init();
  *term_count_out = 0UL;
  *term_hash_out = lc_pouch_query_index_hash_init();
  *term_field_count_out = 0UL;
  *term_value_count_out = 0UL;
  *presence_count_out = 0UL;
  *presence_hash_out = lc_pouch_query_index_hash_init();
  if (summary->count > 1U) {
    qsort(summary->rows, summary->count, sizeof(summary->rows[0]),
          lc_pouch_query_index_row_compare);
  }
  memset(&header, 0, sizeof(header));
  memset(&doc_table, 0, sizeof(doc_table));
  memset(&exact_generation, 0, sizeof(exact_generation));
  memset(&presence_generation, 0, sizeof(presence_generation));
  memset(&range_generation, 0, sizeof(range_generation));
  memset(&text_generation, 0, sizeof(text_generation));
  memset(&trigram_generation, 0, sizeof(trigram_generation));
  memset(&temporal_generation, 0, sizeof(temporal_generation));
  memset(&exact_accumulator, 0, sizeof(exact_accumulator));
  memset(&presence_accumulator, 0, sizeof(presence_accumulator));
  memset(&range_accumulator, 0, sizeof(range_accumulator));
  memset(&text_accumulator, 0, sizeof(text_accumulator));
  memset(&trigram_accumulator, 0, sizeof(trigram_accumulator));
  memset(&temporal_accumulator, 0, sizeof(temporal_accumulator));
  memset(&trigram_field_cache, 0, sizeof(trigram_field_cache));
  memset(&trigram_term_cache, 0, sizeof(trigram_term_cache));
  memset(&exact_term_cache, 0, sizeof(exact_term_cache));
  memset(&presence_term_cache, 0, sizeof(presence_term_cache));
  memset(&range_term_cache, 0, sizeof(range_term_cache));
  memset(&text_term_cache, 0, sizeof(text_term_cache));
  memset(&temporal_term_cache, 0, sizeof(temporal_term_cache));
  header.allocator = summary->allocator;
  row_hash = lc_pouch_query_index_hash_init();
  term_hash = lc_pouch_query_index_hash_init();
  term_line_count = 0UL;
  term_field_line_count = 0UL;
  term_value_line_count = 0UL;
  last_term_key_hex = NULL;
  last_presence_key_hex = NULL;
  presence_hash = lc_pouch_query_index_hash_init();
  presence_line_count = 0UL;
  rc = LC_OK;
  exact_generation.namespace_name =
      lc_strdup_with_allocator(summary->allocator, summary->namespace_name);
  if (exact_generation.namespace_name == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch exact generation namespace",
                      NULL, NULL, NULL);
  }
  if (rc == LC_OK) {
    presence_generation.namespace_name =
        lc_strdup_with_allocator(summary->allocator, summary->namespace_name);
    if (presence_generation.namespace_name == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch presence generation "
                        "namespace",
                        NULL, NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    range_generation.namespace_name =
        lc_strdup_with_allocator(summary->allocator, summary->namespace_name);
    if (range_generation.namespace_name == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch range generation namespace",
                        NULL, NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    text_generation.namespace_name =
        lc_strdup_with_allocator(summary->allocator, summary->namespace_name);
    if (text_generation.namespace_name == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch text generation namespace",
                        NULL, NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    temporal_generation.namespace_name =
        lc_strdup_with_allocator(summary->allocator, summary->namespace_name);
    if (temporal_generation.namespace_name == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch temporal generation "
                        "namespace",
                        NULL, NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    trigram_generation.namespace_name =
        lc_strdup_with_allocator(summary->allocator, summary->namespace_name);
    if (trigram_generation.namespace_name == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch trigram generation "
                        "namespace",
                        NULL, NULL, NULL);
    }
  }
  for (index = 0U; rc == LC_OK && index < summary->count; ++index) {
    lc_pouch_query_index_row *row;
    char version_text[32];
    char bytes_text[32];

    row = &summary->rows[index];
    if (lc_u64_format_base10((lc_u64)row->version, version_text,
                             sizeof(version_text)) < 0 ||
        lc_u64_format_base10((lc_u64)row->bytes, bytes_text,
                             sizeof(bytes_text)) < 0) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index row exceeds local limit", NULL, NULL,
                        NULL);
      break;
    }
    written = snprintf(line, sizeof(line), "row %s %s %d %d ", version_text,
                       bytes_text, row->has_query_hidden ? 1 : 0,
                       row->query_hidden ? 1 : 0);
    if (written < 0 || (size_t)written >= sizeof(line)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index row exceeds local limit", NULL, NULL,
                        NULL);
      break;
    }
    rc = lc_pouch_query_index_text_append(NULL, line, (size_t)written,
                                          &row_hash, error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(NULL, row->key_hex, &row_hash,
                                                 error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(NULL, " ", &row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(NULL, row->content_type_hex,
                                                 &row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(NULL, " ", &row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(NULL, row->etag_hex, &row_hash,
                                                 error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_index_doc_table_append_sorted_unique(
          &doc_table, row->key_hex, row->version, row->bytes,
          row->has_query_hidden, row->query_hidden, &doc_id, summary->allocator,
          error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_text_append_cstr(NULL, "\n", &row_hash, error);
    }
  }
  for (index = 0U; rc == LC_OK && index < summary->term_count; ++index) {
    lc_pouch_query_index_term *term;

    term = &summary->terms[index];
    if (index > 0U &&
        lc_pouch_query_index_term_equal(&summary->terms[index - 1U], term)) {
      continue;
    }
    if (last_term_key_hex != NULL &&
        strcmp(last_term_key_hex, term->key_hex) == 0) {
      term->doc_id = doc_id;
    } else {
      rc = lc_pouch_index_doc_table_find_key_hex(&doc_table, term->key_hex,
                                                 &term->doc_id, &found, error);
      if (rc != LC_OK) {
        break;
      }
      if (!found) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query-index term references missing doc table "
                          "entry",
                          NULL, NULL, NULL);
        break;
      }
      last_term_key_hex = term->key_hex;
      doc_id = term->doc_id;
    }
    if (rc == LC_OK) {
      char *allocated_exact_value_hex;
      const char *exact_value_hex;
      const char *indexed_exact_value_hex;
      unsigned long exact_term_id;
      char indexed_exact_value_type;

      allocated_exact_value_hex = NULL;
      exact_value_hex = NULL;
      indexed_exact_value_hex = NULL;
      exact_term_id = 0UL;
      indexed_exact_value_type = term->value_type;
      if (term->value_type == 's' && term->long_value != NULL) {
        exact_value_hex = term->value_hex;
        indexed_exact_value_hex = term->value_hex;
        indexed_exact_value_type = LC_POUCH_QUERY_INDEX_EXACT_HASH_TYPE;
      } else if (term->value_type == 'n') {
        if (term->canonical_value_hex != NULL) {
          exact_value_hex = term->canonical_value_hex;
          indexed_exact_value_hex = term->canonical_value_hex;
        } else {
          rc = lc_pouch_query_index_exact_generation_value_hex(
              summary->allocator, term->value_hex, term->value_type,
              &allocated_exact_value_hex, error);
          exact_value_hex = allocated_exact_value_hex;
          indexed_exact_value_hex = allocated_exact_value_hex;
        }
      } else {
        exact_value_hex = term->value_hex;
        indexed_exact_value_hex = term->value_hex;
      }
      if (rc == LC_OK) {
        rc = lc_pouch_query_index_term_find_or_add_cached(
            summary->allocator, &exact_generation, &exact_term_cache,
            term->field_hex, indexed_exact_value_hex, indexed_exact_value_type,
            &exact_term_id, error);
      }
      if (rc == LC_OK) {
        rc = lc_pouch_query_index_exact_generation_accumulator_append(
            summary->allocator, &exact_accumulator, exact_term_id, term->doc_id,
            error);
      }
      if (rc == LC_OK && term->value_type == 'n') {
        unsigned long range_term_id;

        range_term_id = 0UL;
        rc = lc_pouch_query_index_term_find_or_add_cached(
            summary->allocator, &range_generation, &range_term_cache,
            term->field_hex, exact_value_hex, term->value_type, &range_term_id,
            error);
        if (rc == LC_OK) {
          rc = lc_pouch_query_index_exact_generation_accumulator_append(
              summary->allocator, &range_accumulator, range_term_id,
              term->doc_id, error);
        }
      }
      if (rc == LC_OK && term->value_type == 's') {
        char *date_text;
        lc_pouch_index_instant ignored_instant;

        date_text = NULL;
        if (term->derived_terms_ready) {
          if (term->long_value != NULL && term->text_prefix_hex != NULL) {
            rc = lc_pouch_query_index_text_generation_add_term(
                summary->allocator, &text_generation, &text_accumulator,
                &text_term_cache, term->field_hex, term->text_prefix_hex,
                LC_POUCH_QUERY_INDEX_TEXT_PREFIX_TYPE, term->doc_id, error);
          } else if (term->long_value == NULL) {
            rc = lc_pouch_query_index_text_generation_add_value(
                summary->allocator, &text_generation, &text_accumulator,
                &text_term_cache, term->field_hex, exact_value_hex,
                term->doc_id, error);
          }
          if (rc == LC_OK) {
            size_t derived_index;

            for (derived_index = 0U;
                 rc == LC_OK && derived_index < term->trigram_key_count;
                 ++derived_index) {
              rc = lc_pouch_query_index_trigram_generation_append_docid(
                  summary->allocator, &trigram_generation, &trigram_accumulator,
                  &trigram_field_cache, &trigram_term_cache, term->field_hex,
                  term->trigram_keys[derived_index], term->doc_id, error);
              if (rc == LC_OK) {
                rc = lc_pouch_query_index_trigram_generation_append_docid(
                    summary->allocator, &trigram_generation,
                    &trigram_accumulator, &trigram_field_cache,
                    &trigram_term_cache,
                    LC_POUCH_QUERY_INDEX_ANY_TEXT_FIELD_HEX,
                    term->trigram_keys[derived_index], term->doc_id, error);
              }
            }
          }
          if (rc == LC_OK) {
            size_t token_index;
            size_t token_stride;

            token_stride = lc_pouch_query_index_token_hex_stride();
            for (token_index = 0U;
                 rc == LC_OK && token_index < term->token_value_count;
                 ++token_index) {
              rc = lc_pouch_query_index_token_generation_add_aggregate_term(
                  summary->allocator, &text_generation, &text_accumulator,
                  &text_term_cache,
                  term->token_values + (token_index * token_stride),
                  term->doc_id, error);
            }
          }
          if (rc == LC_OK && term->temporal_value_hex != NULL) {
            unsigned long temporal_term_id;

            temporal_term_id = 0UL;
            rc = lc_pouch_query_index_term_find_or_add_cached(
                summary->allocator, &temporal_generation, &temporal_term_cache,
                term->field_hex, term->temporal_value_hex, 'n',
                &temporal_term_id, error);
            if (rc == LC_OK) {
              rc = lc_pouch_query_index_exact_generation_accumulator_append(
                  summary->allocator, &temporal_accumulator, temporal_term_id,
                  term->doc_id, error);
            }
          }
        } else if (term->long_value != NULL) {
          rc = lc_pouch_query_index_text_generation_add_raw_long_value(
              summary->allocator, &text_generation, &text_accumulator,
              &text_term_cache, term->field_hex, term->long_value,
              term->long_value_len, term->doc_id, error);
          if (rc == LC_OK) {
            rc = lc_pouch_query_index_trigram_generation_add_raw_value(
                summary->allocator, &trigram_generation, &trigram_accumulator,
                &trigram_field_cache, &trigram_term_cache, term->field_hex,
                term->long_value, term->long_value_len, term->doc_id, error);
          }
          if (rc == LC_OK) {
            rc = lc_pouch_query_index_token_generation_add_raw_value(
                summary->allocator, &text_generation, &text_accumulator,
                &text_term_cache, term->field_hex, term->long_value,
                term->long_value_len, term->doc_id, error);
          }
        } else {
          rc = lc_pouch_query_index_text_generation_add_value(
              summary->allocator, &text_generation, &text_accumulator,
              &text_term_cache, term->field_hex, exact_value_hex, term->doc_id,
              error);
          if (rc == LC_OK) {
            rc = lc_pouch_query_index_trigram_generation_add_value(
                summary->allocator, &trigram_generation, &trigram_accumulator,
                &trigram_field_cache, &trigram_term_cache, term->field_hex,
                exact_value_hex, term->doc_id, error);
          }
          if (rc == LC_OK) {
            rc = lc_pouch_query_index_token_generation_add_value(
                summary->allocator, &text_generation, &text_accumulator,
                &text_term_cache, term->field_hex, exact_value_hex,
                term->doc_id, error);
          }
        }
        if (rc == LC_OK && !term->derived_terms_ready &&
            exact_value_hex != NULL &&
            lc_pouch_query_index_hex_string_may_be_datetime(exact_value_hex)) {
          date_text = lc_pouch_query_index_hex_decode(summary->allocator,
                                                      exact_value_hex, error);
          if (date_text == NULL) {
            rc = error != NULL && error->code != LC_OK ? error->code
                                                       : LC_ERR_NOMEM;
          }
        }
        if (rc == LC_OK && !term->derived_terms_ready &&
            lc_pouch_index_parse_lql_datetime(date_text, &ignored_instant)) {
          char *temporal_value_hex;
          unsigned long temporal_term_id;

          temporal_value_hex = lc_pouch_query_index_temporal_instant_value_hex(
              summary->allocator, &ignored_instant, error);
          if (temporal_value_hex == NULL) {
            rc = error != NULL && error->code != LC_OK ? error->code
                                                       : LC_ERR_NOMEM;
          }
          temporal_term_id = 0UL;
          if (rc == LC_OK) {
            rc = lc_pouch_query_index_term_find_or_add_cached(
                summary->allocator, &temporal_generation, &temporal_term_cache,
                term->field_hex, temporal_value_hex, 'n', &temporal_term_id,
                error);
          }
          if (rc == LC_OK) {
            rc = lc_pouch_query_index_exact_generation_accumulator_append(
                summary->allocator, &temporal_accumulator, temporal_term_id,
                term->doc_id, error);
          }
          lc_free_with_allocator(summary->allocator, temporal_value_hex);
        }
        lc_free_with_allocator(summary->allocator, date_text);
      }
      lc_free_with_allocator(summary->allocator, allocated_exact_value_hex);
    }
    if (rc != LC_OK) {
      break;
    }
    if (rc == LC_OK) {
      ++term_line_count;
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        summary->allocator, &exact_accumulator, &exact_generation, error);
  }
  for (index = 0U; rc == LC_OK && index < summary->presence_count; ++index) {
    lc_pouch_query_index_presence *presence;

    presence = &summary->presences[index];
    if (index > 0U && lc_pouch_query_index_presence_equal(
                          &summary->presences[index - 1U], presence)) {
      continue;
    }
    if (last_presence_key_hex == NULL ||
        strcmp(last_presence_key_hex, presence->key_hex) != 0) {
      rc = lc_pouch_index_doc_table_find_key_hex(&doc_table, presence->key_hex,
                                                 &doc_id, &found, error);
      if (rc != LC_OK) {
        break;
      }
      if (!found) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query-index presence references missing doc "
                          "table entry",
                          NULL, NULL, NULL);
        break;
      }
      last_presence_key_hex = presence->key_hex;
    }
    if (rc == LC_OK) {
      unsigned long presence_term_id;

      presence_term_id = 0UL;
      rc = lc_pouch_query_index_term_find_or_add_cached(
          summary->allocator, &presence_generation, &presence_term_cache,
          presence->field_hex, "-", 'z', &presence_term_id, error);
      if (rc == LC_OK) {
        rc = lc_pouch_query_index_exact_generation_accumulator_append(
            summary->allocator, &presence_accumulator, presence_term_id, doc_id,
            error);
      }
    }
    if (rc != LC_OK) {
      break;
    }
    if (rc == LC_OK) {
      ++presence_line_count;
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        summary->allocator, &presence_accumulator, &presence_generation, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        summary->allocator, &range_accumulator, &range_generation, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        summary->allocator, &text_accumulator, &text_generation, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        summary->allocator, &trigram_accumulator, &trigram_generation, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_exact_generation_accumulator_flush(
        summary->allocator, &temporal_accumulator, &temporal_generation, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_cstr(
        &header, "format=" LC_POUCH_QUERY_INDEX_FORMAT "\n", NULL, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        &header, "version", LC_POUCH_QUERY_INDEX_VERSION, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_u64_line(&header, "state_index_seq",
                                                   index_seq, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        &header, "row_count", (unsigned long)summary->count, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(&header, "summary_hash",
                                                     row_hash, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(&header, "term_count",
                                                     term_line_count, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(&header, "term_hash",
                                                     term_hash, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        &header, "term_index_complete",
        summary->term_index_complete ? 1UL : 0UL, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        &header, "presence_count", presence_line_count, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(&header, "presence_hash",
                                                     presence_hash, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        &header, "presence_index_complete",
        summary->presence_index_complete ? 1UL : 0UL, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        &header, "term_field_count", term_field_line_count, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_text_append_ulong_line(
        &header, "term_value_count", term_value_line_count, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_index_doc_table_generation_encode(
        &doc_table, index_seq, row_hash, summary->allocator,
        doc_table_generation_out, doc_table_generation_length_out, error);
  }
  if (rc == LC_OK) {
    exact_generation.index_seq = index_seq;
    exact_generation.row_count = (unsigned long)summary->count;
    exact_generation.row_hash = row_hash;
    rc = lc_pouch_index_term_generation_encode(
        &exact_generation, summary->allocator, exact_term_generation_out,
        exact_term_generation_length_out, error);
  }
  if (rc == LC_OK) {
    presence_generation.index_seq = index_seq;
    presence_generation.row_count = (unsigned long)summary->count;
    presence_generation.row_hash = row_hash;
    rc = lc_pouch_index_term_generation_encode(
        &presence_generation, summary->allocator, presence_term_generation_out,
        presence_term_generation_length_out, error);
  }
  if (rc == LC_OK) {
    range_generation.index_seq = index_seq;
    range_generation.row_count = (unsigned long)summary->count;
    range_generation.row_hash = row_hash;
    rc = lc_pouch_index_term_generation_encode(
        &range_generation, summary->allocator, range_term_generation_out,
        range_term_generation_length_out, error);
  }
  if (rc == LC_OK) {
    text_generation.index_seq = index_seq;
    text_generation.row_count = (unsigned long)summary->count;
    text_generation.row_hash = row_hash;
    rc = lc_pouch_index_term_generation_encode(
        &text_generation, summary->allocator, text_term_generation_out,
        text_term_generation_length_out, error);
  }
  if (rc == LC_OK) {
    trigram_generation.index_seq = index_seq;
    trigram_generation.row_count = (unsigned long)summary->count;
    trigram_generation.row_hash = row_hash;
    rc = lc_pouch_index_term_generation_encode(
        &trigram_generation, summary->allocator, trigram_term_generation_out,
        trigram_term_generation_length_out, error);
  }
  if (rc == LC_OK) {
    temporal_generation.index_seq = index_seq;
    temporal_generation.row_count = (unsigned long)summary->count;
    temporal_generation.row_hash = row_hash;
    rc = lc_pouch_index_term_generation_encode(
        &temporal_generation, summary->allocator, temporal_term_generation_out,
        temporal_term_generation_length_out, error);
  }
  if (rc == LC_OK) {
    *out = header;
    memset(&header, 0, sizeof(header));
    *row_hash_out = row_hash;
    *term_count_out = term_line_count;
    *term_hash_out = term_hash;
    *term_field_count_out = term_field_line_count;
    *term_value_count_out = term_value_line_count;
    *presence_count_out = presence_line_count;
    *presence_hash_out = presence_hash;
  }
  lc_free_with_allocator(summary->allocator, header.bytes);
  if (rc != LC_OK) {
    lc_free_with_allocator(summary->allocator, *doc_table_generation_out);
    *doc_table_generation_out = NULL;
    *doc_table_generation_length_out = 0U;
    lc_free_with_allocator(summary->allocator, *exact_term_generation_out);
    *exact_term_generation_out = NULL;
    *exact_term_generation_length_out = 0U;
    lc_free_with_allocator(summary->allocator, *presence_term_generation_out);
    *presence_term_generation_out = NULL;
    *presence_term_generation_length_out = 0U;
    lc_free_with_allocator(summary->allocator, *range_term_generation_out);
    *range_term_generation_out = NULL;
    *range_term_generation_length_out = 0U;
    lc_free_with_allocator(summary->allocator, *text_term_generation_out);
    *text_term_generation_out = NULL;
    *text_term_generation_length_out = 0U;
    lc_free_with_allocator(summary->allocator, *trigram_term_generation_out);
    *trigram_term_generation_out = NULL;
    *trigram_term_generation_length_out = 0U;
    lc_free_with_allocator(summary->allocator, *temporal_term_generation_out);
    *temporal_term_generation_out = NULL;
    *temporal_term_generation_length_out = 0U;
  }
  lc_pouch_query_index_exact_generation_accumulator_cleanup(summary->allocator,
                                                            &exact_accumulator);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(
      summary->allocator, &presence_accumulator);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(summary->allocator,
                                                            &range_accumulator);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(summary->allocator,
                                                            &text_accumulator);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(
      summary->allocator, &trigram_accumulator);
  lc_pouch_query_index_exact_generation_accumulator_cleanup(
      summary->allocator, &temporal_accumulator);
  lc_pouch_query_index_trigram_term_cache_cleanup(summary->allocator,
                                                  &trigram_term_cache);
  lc_pouch_query_index_field_cache_cleanup(summary->allocator,
                                           &trigram_field_cache);
  lc_pouch_query_index_term_cache_cleanup(summary->allocator,
                                          &temporal_term_cache);
  lc_pouch_query_index_term_cache_cleanup(summary->allocator, &text_term_cache);
  lc_pouch_query_index_term_cache_cleanup(summary->allocator,
                                          &range_term_cache);
  lc_pouch_query_index_term_cache_cleanup(summary->allocator,
                                          &presence_term_cache);
  lc_pouch_query_index_term_cache_cleanup(summary->allocator,
                                          &exact_term_cache);
  lc_pouch_index_term_generation_cleanup(summary->allocator, &exact_generation);
  lc_pouch_index_term_generation_cleanup(summary->allocator,
                                         &presence_generation);
  lc_pouch_index_term_generation_cleanup(summary->allocator, &range_generation);
  lc_pouch_index_term_generation_cleanup(summary->allocator, &text_generation);
  lc_pouch_index_term_generation_cleanup(summary->allocator,
                                         &trigram_generation);
  lc_pouch_index_term_generation_cleanup(summary->allocator,
                                         &temporal_generation);
  lc_pouch_index_doc_table_cleanup(summary->allocator, &doc_table);
  return rc;
}

static int
lc_pouch_query_index_write_header_text(lc_pouch *pouch, const char *path,
                                       lc_pouch_query_index_text *text,
                                       lc_error *error) {
  if (pouch == NULL || path == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index header write requires pouch and "
                        "path",
                        NULL, NULL, NULL);
  }
  return lc_pouch_query_index_write_bytes_direct_relaxed(
      path, text != NULL && text->bytes != NULL ? text->bytes : "",
      text != NULL ? text->length : 0U, error);
}

static int lc_pouch_query_index_segmented_paths(
    lc_pouch *pouch, const char *namespace_name, const char *segment_id,
    char **header_path, char **doc_table_path, char **exact_term_path,
    char **presence_term_path, char **range_term_path, char **text_term_path,
    char **trigram_term_path, char **temporal_term_path, char **delete_path,
    lc_error *error) {
  if (header_path == NULL || doc_table_path == NULL ||
      exact_term_path == NULL || presence_term_path == NULL ||
      range_term_path == NULL || text_term_path == NULL ||
      trigram_term_path == NULL || temporal_term_path == NULL ||
      delete_path == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index segment paths require outputs", NULL,
                        NULL, NULL);
  }
  *header_path = lc_pouch_query_index_segment_artifact_path(
      pouch, namespace_name, segment_id, LC_POUCH_QUERY_INDEX_LEAF, error);
  if (*header_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  *doc_table_path = lc_pouch_query_index_segment_artifact_path(
      pouch, namespace_name, segment_id, LC_POUCH_QUERY_INDEX_DOC_TABLE_LEAF,
      error);
  *exact_term_path = *doc_table_path != NULL
                         ? lc_pouch_query_index_segment_artifact_path(
                               pouch, namespace_name, segment_id,
                               LC_POUCH_QUERY_INDEX_EXACT_TERM_LEAF, error)
                         : NULL;
  *presence_term_path =
      *exact_term_path != NULL
          ? lc_pouch_query_index_segment_artifact_path(
                pouch, namespace_name, segment_id,
                LC_POUCH_QUERY_INDEX_PRESENCE_TERM_LEAF, error)
          : NULL;
  *range_term_path = *presence_term_path != NULL
                         ? lc_pouch_query_index_segment_artifact_path(
                               pouch, namespace_name, segment_id,
                               LC_POUCH_QUERY_INDEX_RANGE_TERM_LEAF, error)
                         : NULL;
  *text_term_path = *range_term_path != NULL
                        ? lc_pouch_query_index_segment_artifact_path(
                              pouch, namespace_name, segment_id,
                              LC_POUCH_QUERY_INDEX_TEXT_TERM_LEAF, error)
                        : NULL;
  *trigram_term_path = *text_term_path != NULL
                           ? lc_pouch_query_index_segment_artifact_path(
                                 pouch, namespace_name, segment_id,
                                 LC_POUCH_QUERY_INDEX_TRIGRAM_TERM_LEAF, error)
                           : NULL;
  *temporal_term_path =
      *trigram_term_path != NULL
          ? lc_pouch_query_index_segment_artifact_path(
                pouch, namespace_name, segment_id,
                LC_POUCH_QUERY_INDEX_TEMPORAL_TERM_LEAF, error)
          : NULL;
  *delete_path = *temporal_term_path != NULL
                     ? lc_pouch_query_index_segment_artifact_path(
                           pouch, namespace_name, segment_id,
                           LC_POUCH_QUERY_INDEX_DELETE_LEAF, error)
                     : NULL;
  if (*doc_table_path == NULL || *exact_term_path == NULL ||
      *presence_term_path == NULL || *range_term_path == NULL ||
      *text_term_path == NULL || *trigram_term_path == NULL ||
      *temporal_term_path == NULL || *delete_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  return LC_OK;
}

static void lc_pouch_query_index_segmented_paths_cleanup(
    lc_pouch *pouch, char **header_path, char **doc_table_path,
    char **exact_term_path, char **presence_term_path, char **range_term_path,
    char **text_term_path, char **trigram_term_path, char **temporal_term_path,
    char **delete_path) {
  if (pouch == NULL) {
    return;
  }
  lc_free_with_allocator(&pouch->allocator,
                         delete_path != NULL ? *delete_path : NULL);
  lc_free_with_allocator(&pouch->allocator, temporal_term_path != NULL
                                                ? *temporal_term_path
                                                : NULL);
  lc_free_with_allocator(&pouch->allocator,
                         trigram_term_path != NULL ? *trigram_term_path : NULL);
  lc_free_with_allocator(&pouch->allocator,
                         text_term_path != NULL ? *text_term_path : NULL);
  lc_free_with_allocator(&pouch->allocator,
                         range_term_path != NULL ? *range_term_path : NULL);
  lc_free_with_allocator(&pouch->allocator, presence_term_path != NULL
                                                ? *presence_term_path
                                                : NULL);
  lc_free_with_allocator(&pouch->allocator,
                         exact_term_path != NULL ? *exact_term_path : NULL);
  lc_free_with_allocator(&pouch->allocator,
                         doc_table_path != NULL ? *doc_table_path : NULL);
  lc_free_with_allocator(&pouch->allocator,
                         header_path != NULL ? *header_path : NULL);
}

static int lc_pouch_query_index_manifest_segments_validate(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_manifest *manifest, int *valid_out,
    lc_error *error) {
  size_t segment_index;
  int rc;

  if (pouch == NULL || namespace_name == NULL || manifest == NULL ||
      valid_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index manifest validation requires "
                        "pouch, namespace, manifest, and output",
                        NULL, NULL, NULL);
  }
  *valid_out = 1;
  rc = LC_OK;
  for (segment_index = 0U;
       rc == LC_OK && segment_index < manifest->segment_count;
       ++segment_index) {
    const lc_pouch_query_index_manifest_segment *segment;
    lc_pouch_query_index_read_result header_artifact;
    lc_pouch_query_index_key_hex_set deletes;
    char *header_path;
    char *doc_table_path;
    char *exact_term_path;
    char *presence_term_path;
    char *range_term_path;
    char *text_term_path;
    char *trigram_term_path;
    char *temporal_term_path;
    char *delete_path;
    char *packed_path;
    unsigned long delete_count;
    unsigned long delete_hash;
    lc_pouch_query_index_file_signature
        artifact_signatures[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_COUNT];
    int present;
    int valid;

    segment = &manifest->segments[segment_index];
    memset(&header_artifact, 0, sizeof(header_artifact));
    memset(&deletes, 0, sizeof(deletes));
    header_path = NULL;
    doc_table_path = NULL;
    exact_term_path = NULL;
    presence_term_path = NULL;
    range_term_path = NULL;
    text_term_path = NULL;
    trigram_term_path = NULL;
    temporal_term_path = NULL;
    delete_path = NULL;
    packed_path = NULL;
    delete_count = 0UL;
    delete_hash = lc_pouch_query_index_hash_init();
    rc = lc_pouch_query_index_segmented_paths(
        pouch, namespace_name, segment->id, &header_path, &doc_table_path,
        &exact_term_path, &presence_term_path, &range_term_path,
        &text_term_path, &trigram_term_path, &temporal_term_path, &delete_path,
        error);
    if (rc == LC_OK) {
      packed_path = lc_pouch_query_index_packed_path_from_component(
          &pouch->allocator, doc_table_path,
          LC_POUCH_QUERY_INDEX_PACKED_DOC_TABLE, error);
      if (packed_path == NULL) {
        rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      }
    }
    if (rc == LC_OK &&
        lc_pouch_query_index_manifest_segment_artifacts_match(
            segment, header_path, doc_table_path, exact_term_path,
            presence_term_path, range_term_path, text_term_path,
            trigram_term_path, temporal_term_path, delete_path, packed_path,
            artifact_signatures)) {
      lc_pouch_query_index_artifact_cache_remember_signature(
          pouch, header_path, LC_POUCH_QUERY_INDEX_ARTIFACT_HEADER,
          segment->index_seq, segment->row_count, segment->row_hash,
          &artifact_signatures[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_HEADER]);
      lc_pouch_query_index_artifact_cache_remember_signature(
          pouch, doc_table_path, LC_POUCH_QUERY_INDEX_ARTIFACT_DOC_TABLE,
          segment->index_seq, segment->row_count, segment->row_hash,
          &artifact_signatures
              [LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_DOC_TABLE]);
      lc_pouch_query_index_artifact_cache_remember_signature(
          pouch, exact_term_path, LC_POUCH_QUERY_INDEX_ARTIFACT_TERM_GENERATION,
          segment->index_seq, segment->row_count, segment->row_hash,
          &artifact_signatures[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_EXACT]);
      lc_pouch_query_index_artifact_cache_remember_signature(
          pouch, presence_term_path,
          LC_POUCH_QUERY_INDEX_ARTIFACT_TERM_GENERATION, segment->index_seq,
          segment->row_count, segment->row_hash,
          &artifact_signatures
              [LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_PRESENCE]);
      lc_pouch_query_index_artifact_cache_remember_signature(
          pouch, range_term_path, LC_POUCH_QUERY_INDEX_ARTIFACT_TERM_GENERATION,
          segment->index_seq, segment->row_count, segment->row_hash,
          &artifact_signatures[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_RANGE]);
      lc_pouch_query_index_artifact_cache_remember_signature(
          pouch, text_term_path, LC_POUCH_QUERY_INDEX_ARTIFACT_TERM_GENERATION,
          segment->index_seq, segment->row_count, segment->row_hash,
          &artifact_signatures[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_TEXT]);
      lc_pouch_query_index_artifact_cache_remember_signature(
          pouch, trigram_term_path,
          LC_POUCH_QUERY_INDEX_ARTIFACT_TERM_GENERATION, segment->index_seq,
          segment->row_count, segment->row_hash,
          &artifact_signatures[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_TRIGRAM]);
      lc_pouch_query_index_artifact_cache_remember_signature(
          pouch, temporal_term_path,
          LC_POUCH_QUERY_INDEX_ARTIFACT_TERM_GENERATION, segment->index_seq,
          segment->row_count, segment->row_hash,
          &artifact_signatures
              [LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_TEMPORAL]);
      lc_pouch_query_index_artifact_cache_remember_signature(
          pouch, delete_path, LC_POUCH_QUERY_INDEX_ARTIFACT_DELETE,
          segment->index_seq, segment->delete_count, segment->delete_hash,
          &artifact_signatures[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_DELETE]);
      lc_pouch_query_index_segmented_paths_cleanup(
          pouch, &header_path, &doc_table_path, &exact_term_path,
          &presence_term_path, &range_term_path, &text_term_path,
          &trigram_term_path, &temporal_term_path, &delete_path);
      lc_free_with_allocator(&pouch->allocator, packed_path);
      continue;
    }
    if (rc == LC_OK &&
        !lc_pouch_query_index_artifact_cache_valid(
            pouch, header_path, LC_POUCH_QUERY_INDEX_ARTIFACT_HEADER,
            segment->index_seq, segment->row_count, segment->row_hash)) {
      rc = lc_pouch_query_index_read_header(pouch, namespace_name, header_path,
                                            &header_artifact, error);
      if (rc == LC_OK && (!header_artifact.present || !header_artifact.valid ||
                          header_artifact.index_seq != segment->index_seq ||
                          header_artifact.row_count != segment->row_count ||
                          header_artifact.row_hash != segment->row_hash ||
                          !header_artifact.term_index_complete ||
                          !header_artifact.presence_index_complete)) {
        *valid_out = 0;
      }
      if (rc == LC_OK && *valid_out) {
        lc_pouch_query_index_artifact_cache_remember(
            pouch, header_path, LC_POUCH_QUERY_INDEX_ARTIFACT_HEADER,
            segment->index_seq, segment->row_count, segment->row_hash);
      }
    }
    if (rc == LC_OK && *valid_out) {
      present = 0;
      valid = 0;
      if (!lc_pouch_query_index_artifact_cache_valid(
              pouch, doc_table_path, LC_POUCH_QUERY_INDEX_ARTIFACT_DOC_TABLE,
              segment->index_seq, segment->row_count, segment->row_hash)) {
        rc = lc_pouch_query_index_doc_table_generation_validate_artifact(
            pouch, namespace_name, doc_table_path, segment->index_seq,
            segment->row_count, segment->row_hash, &present, &valid, error);
        if (rc == LC_OK && (!present || !valid)) {
          *valid_out = 0;
        }
        if (rc == LC_OK && *valid_out) {
          lc_pouch_query_index_artifact_cache_remember(
              pouch, doc_table_path, LC_POUCH_QUERY_INDEX_ARTIFACT_DOC_TABLE,
              segment->index_seq, segment->row_count, segment->row_hash);
        }
      }
    }
    if (rc == LC_OK && *valid_out) {
      present = 0;
      valid = 0;
      if (!lc_pouch_query_index_artifact_cache_valid(
              pouch, exact_term_path,
              LC_POUCH_QUERY_INDEX_ARTIFACT_TERM_GENERATION, segment->index_seq,
              segment->row_count, segment->row_hash)) {
        rc = lc_pouch_query_index_term_generation_validate_artifact(
            pouch, namespace_name, exact_term_path, segment->index_seq,
            segment->row_count, segment->row_hash, &present, &valid, error);
        if (rc == LC_OK && (!present || !valid)) {
          *valid_out = 0;
        }
        if (rc == LC_OK && *valid_out) {
          lc_pouch_query_index_artifact_cache_remember(
              pouch, exact_term_path,
              LC_POUCH_QUERY_INDEX_ARTIFACT_TERM_GENERATION, segment->index_seq,
              segment->row_count, segment->row_hash);
        }
      }
    }
    if (rc == LC_OK && *valid_out) {
      present = 0;
      valid = 0;
      if (!lc_pouch_query_index_artifact_cache_valid(
              pouch, presence_term_path,
              LC_POUCH_QUERY_INDEX_ARTIFACT_TERM_GENERATION, segment->index_seq,
              segment->row_count, segment->row_hash)) {
        rc = lc_pouch_query_index_term_generation_validate_artifact(
            pouch, namespace_name, presence_term_path, segment->index_seq,
            segment->row_count, segment->row_hash, &present, &valid, error);
        if (rc == LC_OK && (!present || !valid)) {
          *valid_out = 0;
        }
        if (rc == LC_OK && *valid_out) {
          lc_pouch_query_index_artifact_cache_remember(
              pouch, presence_term_path,
              LC_POUCH_QUERY_INDEX_ARTIFACT_TERM_GENERATION, segment->index_seq,
              segment->row_count, segment->row_hash);
        }
      }
    }
    if (rc == LC_OK && *valid_out) {
      present = 0;
      valid = 0;
      if (!lc_pouch_query_index_artifact_cache_valid(
              pouch, range_term_path,
              LC_POUCH_QUERY_INDEX_ARTIFACT_TERM_GENERATION, segment->index_seq,
              segment->row_count, segment->row_hash)) {
        rc = lc_pouch_query_index_term_generation_validate_artifact(
            pouch, namespace_name, range_term_path, segment->index_seq,
            segment->row_count, segment->row_hash, &present, &valid, error);
        if (rc == LC_OK && (!present || !valid)) {
          *valid_out = 0;
        }
        if (rc == LC_OK && *valid_out) {
          lc_pouch_query_index_artifact_cache_remember(
              pouch, range_term_path,
              LC_POUCH_QUERY_INDEX_ARTIFACT_TERM_GENERATION, segment->index_seq,
              segment->row_count, segment->row_hash);
        }
      }
    }
    if (rc == LC_OK && *valid_out) {
      present = 0;
      valid = 0;
      if (!lc_pouch_query_index_artifact_cache_valid(
              pouch, text_term_path,
              LC_POUCH_QUERY_INDEX_ARTIFACT_TERM_GENERATION, segment->index_seq,
              segment->row_count, segment->row_hash)) {
        rc = lc_pouch_query_index_term_generation_validate_artifact(
            pouch, namespace_name, text_term_path, segment->index_seq,
            segment->row_count, segment->row_hash, &present, &valid, error);
        if (rc == LC_OK && (!present || !valid)) {
          *valid_out = 0;
        }
        if (rc == LC_OK && *valid_out) {
          lc_pouch_query_index_artifact_cache_remember(
              pouch, text_term_path,
              LC_POUCH_QUERY_INDEX_ARTIFACT_TERM_GENERATION, segment->index_seq,
              segment->row_count, segment->row_hash);
        }
      }
    }
    if (rc == LC_OK && *valid_out) {
      present = 0;
      valid = 0;
      if (!lc_pouch_query_index_artifact_cache_valid(
              pouch, trigram_term_path,
              LC_POUCH_QUERY_INDEX_ARTIFACT_TERM_GENERATION, segment->index_seq,
              segment->row_count, segment->row_hash)) {
        rc = lc_pouch_query_index_term_generation_validate_artifact(
            pouch, namespace_name, trigram_term_path, segment->index_seq,
            segment->row_count, segment->row_hash, &present, &valid, error);
        if (rc == LC_OK && (!present || !valid)) {
          *valid_out = 0;
        }
        if (rc == LC_OK && *valid_out) {
          lc_pouch_query_index_artifact_cache_remember(
              pouch, trigram_term_path,
              LC_POUCH_QUERY_INDEX_ARTIFACT_TERM_GENERATION, segment->index_seq,
              segment->row_count, segment->row_hash);
        }
      }
    }
    if (rc == LC_OK && *valid_out) {
      present = 0;
      valid = 0;
      if (!lc_pouch_query_index_artifact_cache_valid(
              pouch, temporal_term_path,
              LC_POUCH_QUERY_INDEX_ARTIFACT_TERM_GENERATION, segment->index_seq,
              segment->row_count, segment->row_hash)) {
        rc = lc_pouch_query_index_term_generation_validate_artifact(
            pouch, namespace_name, temporal_term_path, segment->index_seq,
            segment->row_count, segment->row_hash, &present, &valid, error);
        if (rc == LC_OK && (!present || !valid)) {
          *valid_out = 0;
        }
        if (rc == LC_OK && *valid_out) {
          lc_pouch_query_index_artifact_cache_remember(
              pouch, temporal_term_path,
              LC_POUCH_QUERY_INDEX_ARTIFACT_TERM_GENERATION, segment->index_seq,
              segment->row_count, segment->row_hash);
        }
      }
    }
    if (rc == LC_OK && *valid_out) {
      if (!lc_pouch_query_index_artifact_cache_valid(
              pouch, delete_path, LC_POUCH_QUERY_INDEX_ARTIFACT_DELETE,
              segment->index_seq, segment->delete_count,
              segment->delete_hash)) {
        rc = lc_pouch_query_index_load_delete_keys(
            pouch, namespace_name, delete_path, &deletes, &delete_count,
            &delete_hash, error);
        if (rc == LC_ERR_INVALID) {
          if (error != NULL) {
            lc_error_cleanup(error);
          }
          *valid_out = 0;
          rc = LC_OK;
        }
        if (rc == LC_OK && (delete_count != segment->delete_count ||
                            delete_hash != segment->delete_hash)) {
          *valid_out = 0;
        }
        if (rc == LC_OK && *valid_out) {
          lc_pouch_query_index_artifact_cache_remember(
              pouch, delete_path, LC_POUCH_QUERY_INDEX_ARTIFACT_DELETE,
              segment->index_seq, segment->delete_count, segment->delete_hash);
        }
      }
    }
    lc_pouch_query_index_key_hex_set_cleanup(&pouch->allocator, &deletes);
    lc_pouch_query_index_segmented_paths_cleanup(
        pouch, &header_path, &doc_table_path, &exact_term_path,
        &presence_term_path, &range_term_path, &text_term_path,
        &trigram_term_path, &temporal_term_path, &delete_path);
    lc_free_with_allocator(&pouch->allocator, packed_path);
  }
  return rc;
}

static int lc_pouch_query_index_flush_segmented(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_generation state_index_seq, int validate_existing_segments,
    lc_pouch_query_index_flush_result *out, lc_error *error) {
  lc_pouch_query_index_manifest manifest;
  lc_pouch_query_index_manifest retired_manifest;
  lc_pouch_query_index_summary summary;
  lc_pouch_query_index_extract_batch extract_batch;
  lc_pouch_query_index_incremental_context incremental;
  lc_pouch_query_index_text text;
  lc_pouch_query_index_text deletes;
  const char **extract_keys;
  size_t *extract_row_indices;
  char segment_id[32];
  char *manifest_path;
  char *header_path;
  char *doc_table_path;
  char *exact_term_path;
  char *presence_term_path;
  char *range_term_path;
  char *text_term_path;
  char *trigram_term_path;
  char *temporal_term_path;
  char *delete_path;
  char *doc_table_generation;
  char *exact_term_generation;
  char *presence_term_generation;
  char *range_term_generation;
  char *text_term_generation;
  char *trigram_term_generation;
  char *temporal_term_generation;
  char *packed_path;
  char *packed_generation;
  lc_pouch_query_index_pending_entry *pending_entries;
  lc_pouch_query_index_pending_segment *pending_segment;
  size_t doc_table_generation_length;
  size_t exact_term_generation_length;
  size_t presence_term_generation_length;
  size_t range_term_generation_length;
  size_t text_term_generation_length;
  size_t trigram_term_generation_length;
  size_t temporal_term_generation_length;
  size_t packed_generation_length;
  size_t extract_count;
  size_t extract_index;
  size_t pending_count;
  unsigned long segment_row_count;
  unsigned long row_hash;
  unsigned long term_count;
  unsigned long term_hash;
  unsigned long term_field_count;
  unsigned long term_value_count;
  unsigned long presence_count;
  unsigned long presence_hash;
  unsigned long delete_count;
  unsigned long delete_hash;
  lc_pouch_generation segment_base_index_seq;
  int manifest_segments_valid;
  int manifest_trusted;
  int full_rebuild;
  int cleanup_unreferenced;
  int pending_fast_path_allowed;
  int use_pending_entries;
  int use_pending_segment;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_query_index_flush requires pouch, "
                        "namespace, and out",
                        NULL, NULL, NULL);
  }
  {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("ns", namespace_name);
    fields[1] = lc_log_u64_field("index_seq", state_index_seq);
    fields[2] = lc_log_bool_field("validate", validate_existing_segments);
    lc_log_trace(pouch->logger, "index.flush.start", fields, 3U);
  }
  memset(out, 0, sizeof(*out));
  memset(&manifest, 0, sizeof(manifest));
  memset(&retired_manifest, 0, sizeof(retired_manifest));
  memset(&summary, 0, sizeof(summary));
  memset(&extract_batch, 0, sizeof(extract_batch));
  memset(&incremental, 0, sizeof(incremental));
  memset(&text, 0, sizeof(text));
  memset(&deletes, 0, sizeof(deletes));
  extract_keys = NULL;
  extract_row_indices = NULL;
  manifest_path = NULL;
  header_path = NULL;
  doc_table_path = NULL;
  exact_term_path = NULL;
  presence_term_path = NULL;
  range_term_path = NULL;
  text_term_path = NULL;
  trigram_term_path = NULL;
  temporal_term_path = NULL;
  delete_path = NULL;
  doc_table_generation = NULL;
  exact_term_generation = NULL;
  presence_term_generation = NULL;
  range_term_generation = NULL;
  text_term_generation = NULL;
  trigram_term_generation = NULL;
  temporal_term_generation = NULL;
  packed_path = NULL;
  packed_generation = NULL;
  pending_entries = NULL;
  pending_segment = NULL;
  doc_table_generation_length = 0U;
  exact_term_generation_length = 0U;
  presence_term_generation_length = 0U;
  range_term_generation_length = 0U;
  text_term_generation_length = 0U;
  trigram_term_generation_length = 0U;
  temporal_term_generation_length = 0U;
  packed_generation_length = 0U;
  pending_count = 0U;
  segment_row_count = 0UL;
  delete_hash = lc_pouch_query_index_hash_init();
  segment_base_index_seq = 0UL;
  full_rebuild = 0;
  cleanup_unreferenced = 0;
  use_pending_entries = 0;
  use_pending_segment = 0;
  summary.allocator = &pouch->allocator;
  summary.pouch = pouch;
  summary.namespace_name = namespace_name;
  summary.term_index_complete = 1;
  summary.presence_index_complete = 1;
  text.allocator = &pouch->allocator;
  deletes.allocator = &pouch->allocator;

  manifest_path =
      lc_pouch_query_index_manifest_path(pouch, namespace_name, error);
  if (manifest_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_query_index_manifest_read(pouch, manifest_path, &manifest,
                                          error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  manifest_segments_valid = 0;
  manifest_trusted = 0;
  if (manifest.present && manifest.valid) {
    manifest_trusted = lc_pouch_query_index_manifest_trust_valid(
        pouch, namespace_name, manifest.index_seq);
    if (!validate_existing_segments &&
        (manifest_trusted || manifest.index_seq <= state_index_seq)) {
      manifest_segments_valid = 1;
    } else {
      rc = lc_pouch_query_index_manifest_segments_validate(
          pouch, namespace_name, &manifest, &manifest_segments_valid, error);
      if (rc != LC_OK) {
        goto cleanup;
      }
    }
    if (manifest_segments_valid && manifest.index_seq == state_index_seq &&
        manifest.segment_count < LC_POUCH_QUERY_INDEX_MAX_SEGMENTS) {
      out->index_seq = state_index_seq;
      goto cleanup;
    }
  }
  full_rebuild = !manifest_segments_valid || !manifest.present ||
                 !manifest.valid || manifest.index_seq > state_index_seq ||
                 manifest.segment_count >= LC_POUCH_QUERY_INDEX_MAX_SEGMENTS;
  cleanup_unreferenced = full_rebuild || validate_existing_segments;
  pending_fast_path_allowed = lc_pouch_single_writer_enabled(pouch) &&
                              !pouch->query_pending_index_incomplete;
  if (full_rebuild) {
    retired_manifest = manifest;
    memset(&manifest, 0, sizeof(manifest));
    segment_base_index_seq = 0UL;
    if (pending_fast_path_allowed) {
      pending_entries = lc_pouch_query_index_pending_detach_namespace(
          pouch, namespace_name, &pending_count);
      pending_segment = lc_pouch_query_index_pending_segment_detach_namespace(
          pouch, namespace_name);
    }
    if (pending_count > 0U) {
      size_t pending_visible_count;
      size_t state_visible_count;

      pending_visible_count =
          lc_pouch_query_index_pending_entries_visible_count(pending_entries);
      state_visible_count = 0U;
      rc = lc_pouch_state_visible_count(pouch, namespace_name,
                                        &state_visible_count, error);
      if (rc == LC_OK && pending_visible_count == state_visible_count &&
          pending_segment == NULL) {
        pending_segment = lc_pouch_query_index_pending_segment_build_entries(
            pouch, namespace_name, pending_entries, pending_count, error);
        if (pending_segment == NULL) {
          rc = error != NULL && error->code != LC_OK ? error->code
                                                     : LC_ERR_NOMEM;
        }
      }
      if (rc == LC_OK && pending_segment != NULL) {
        use_pending_segment = 1;
        segment_row_count = (unsigned long)pending_segment->doc_table.count;
        rc = lc_pouch_query_index_pending_segment_encode(
            pending_segment, state_index_seq, &text, &row_hash, &delete_count,
            &delete_hash, &doc_table_generation, &doc_table_generation_length,
            &exact_term_generation, &exact_term_generation_length,
            &presence_term_generation, &presence_term_generation_length,
            &range_term_generation, &range_term_generation_length,
            &text_term_generation, &text_term_generation_length,
            &trigram_term_generation, &trigram_term_generation_length,
            &temporal_term_generation, &temporal_term_generation_length, error);
      } else if (rc == LC_OK && pending_visible_count == state_visible_count) {
        use_pending_entries = 1;
        rc = lc_pouch_query_index_pending_entries_build_summary(
            &pouch->allocator, pending_entries, &summary, &deletes,
            &delete_count, &delete_hash, error);
      } else if (rc == LC_OK) {
        rc = lc_pouch_state_visit(pouch, namespace_name,
                                  lc_pouch_query_index_summary_visit, &summary,
                                  error);
      }
    } else {
      rc = lc_pouch_state_visit(pouch, namespace_name,
                                lc_pouch_query_index_summary_visit, &summary,
                                error);
    }
  } else {
    segment_base_index_seq = manifest.index_seq;
    if (pending_fast_path_allowed) {
      pending_entries = lc_pouch_query_index_pending_detach_namespace(
          pouch, namespace_name, &pending_count);
      pending_segment = lc_pouch_query_index_pending_segment_detach_namespace(
          pouch, namespace_name);
    }
    if (pending_count > 0U && pending_segment == NULL) {
      pending_segment = lc_pouch_query_index_pending_segment_build_entries(
          pouch, namespace_name, pending_entries, pending_count, error);
      if (pending_segment == NULL) {
        rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      }
    }
    if (rc == LC_OK && pending_count > 0U && pending_segment != NULL) {
      use_pending_segment = 1;
      segment_row_count = (unsigned long)pending_segment->doc_table.count;
      rc = lc_pouch_query_index_pending_segment_encode(
          pending_segment, state_index_seq, &text, &row_hash, &delete_count,
          &delete_hash, &doc_table_generation, &doc_table_generation_length,
          &exact_term_generation, &exact_term_generation_length,
          &presence_term_generation, &presence_term_generation_length,
          &range_term_generation, &range_term_generation_length,
          &text_term_generation, &text_term_generation_length,
          &trigram_term_generation, &trigram_term_generation_length,
          &temporal_term_generation, &temporal_term_generation_length, error);
    } else if (pending_count > 0U) {
      use_pending_entries = 1;
      rc = lc_pouch_query_index_pending_entries_build_summary(
          &pouch->allocator, pending_entries, &summary, &deletes, &delete_count,
          &delete_hash, error);
    } else {
      incremental.summary = &summary;
      rc = lc_pouch_state_visit_since(pouch, namespace_name, manifest.index_seq,
                                      lc_pouch_query_index_incremental_visit,
                                      &incremental, error);
      if (rc == LC_OK) {
        rc =
            lc_pouch_query_index_incremental_apply_changes(&incremental, error);
      }
    }
  }
  if (rc != LC_OK) {
    goto cleanup;
  }
  if (summary.count > 0U && !use_pending_entries && !use_pending_segment) {
    extract_keys = (const char **)lc_calloc_with_allocator(
        &pouch->allocator, summary.count, sizeof(*extract_keys));
    extract_row_indices = (size_t *)lc_calloc_with_allocator(
        &pouch->allocator, summary.count, sizeof(*extract_row_indices));
    if (extract_keys == NULL || extract_row_indices == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index extraction keys",
                        NULL, NULL, NULL);
      goto cleanup;
    }
    extract_count = 0U;
    for (extract_index = 0U; extract_index < summary.count; ++extract_index) {
      if (lc_pouch_query_index_pending_apply(
              pouch, &summary, &summary.rows[extract_index], error)) {
        continue;
      }
      extract_keys[extract_count] = summary.rows[extract_index].key;
      extract_row_indices[extract_count] = extract_index;
      ++extract_count;
    }
    extract_batch.summary = &summary;
    extract_batch.row_indices = extract_row_indices;
    extract_batch.index = 0U;
    if (extract_count > 0U) {
      rc = lc_pouch_state_read_many_cached(
          pouch, namespace_name, extract_keys, extract_count,
          lc_pouch_query_index_extract_batch_visit, &extract_batch, error);
    }
  }
  if (rc == LC_OK && !use_pending_entries && !use_pending_segment) {
    rc = lc_pouch_query_index_encode_delete_keys(
        incremental.changes, incremental.change_count, &deletes, &delete_count,
        &delete_hash, error);
  }
  if (rc != LC_OK) {
    goto cleanup;
  }
  if (!use_pending_segment) {
    rc = lc_pouch_query_index_build_text(
        state_index_seq, &summary, &text, &row_hash, &term_count, &term_hash,
        &term_field_count, &term_value_count, &presence_count, &presence_hash,
        &doc_table_generation, &doc_table_generation_length,
        &exact_term_generation, &exact_term_generation_length,
        &presence_term_generation, &presence_term_generation_length,
        &range_term_generation, &range_term_generation_length,
        &text_term_generation, &text_term_generation_length,
        &trigram_term_generation, &trigram_term_generation_length,
        &temporal_term_generation, &temporal_term_generation_length, error);
    segment_row_count = (unsigned long)summary.count;
  }
  (void)term_count;
  (void)term_hash;
  (void)term_field_count;
  (void)term_value_count;
  (void)presence_count;
  (void)presence_hash;
  if (rc != LC_OK) {
    goto cleanup;
  }
  rc = lc_pouch_query_index_segment_id(state_index_seq, segment_id,
                                       sizeof(segment_id), error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_segmented_paths(
        pouch, namespace_name, segment_id, &header_path, &doc_table_path,
        &exact_term_path, &presence_term_path, &range_term_path,
        &text_term_path, &trigram_term_path, &temporal_term_path, &delete_path,
        error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_write_header_text(pouch, header_path, &text,
                                                error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_pack_components(
        &pouch->allocator,
        doc_table_generation != NULL ? doc_table_generation : "",
        doc_table_generation_length,
        exact_term_generation != NULL ? exact_term_generation : "",
        exact_term_generation_length,
        presence_term_generation != NULL ? presence_term_generation : "",
        presence_term_generation_length,
        range_term_generation != NULL ? range_term_generation : "",
        range_term_generation_length,
        text_term_generation != NULL ? text_term_generation : "",
        text_term_generation_length,
        trigram_term_generation != NULL ? trigram_term_generation : "",
        trigram_term_generation_length,
        temporal_term_generation != NULL ? temporal_term_generation : "",
        temporal_term_generation_length,
        use_pending_segment && pending_segment->deletes.bytes != NULL
            ? pending_segment->deletes.bytes
            : (deletes.bytes != NULL ? deletes.bytes : ""),
        use_pending_segment ? pending_segment->deletes.length : deletes.length,
        &packed_generation, &packed_generation_length, error);
  }
  if (rc == LC_OK) {
    packed_path = lc_pouch_query_index_packed_path_from_component(
        &pouch->allocator, doc_table_path,
        LC_POUCH_QUERY_INDEX_PACKED_DOC_TABLE, error);
    if (packed_path == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_write_artifact_bytes(
        pouch, namespace_name, packed_path,
        packed_generation != NULL ? packed_generation : "",
        packed_generation_length, error);
  }
  if (rc == LC_OK) {
    lc_pouch_query_index_artifact_cache_remember(
        pouch, header_path, LC_POUCH_QUERY_INDEX_ARTIFACT_HEADER,
        state_index_seq, segment_row_count, row_hash);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_manifest_add_front(
        &pouch->allocator, &manifest, segment_id, segment_base_index_seq,
        state_index_seq, segment_row_count, row_hash, delete_count, delete_hash,
        error);
  }
  if (rc == LC_OK && manifest.segment_count > 0U) {
    lc_pouch_query_index_manifest_segment_capture_artifacts(
        &manifest.segments[0], header_path, doc_table_path, exact_term_path,
        presence_term_path, range_term_path, text_term_path, trigram_term_path,
        temporal_term_path, delete_path, packed_path);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_manifest_write(pouch, manifest_path, &manifest,
                                             error);
  }
  if (rc == LC_OK && !use_pending_entries) {
    lc_pouch_query_index_pending_entries_cleanup(
        &pouch->allocator, lc_pouch_query_index_pending_detach_namespace(
                               pouch, namespace_name, NULL));
    pouch->query_pending_index_incomplete = 0;
  } else if (rc == LC_OK) {
    pouch->query_pending_index_incomplete = 0;
  }
  if (rc == LC_OK && retired_manifest.segment_count > 0U) {
    lc_pouch_query_index_manifest_unlink_segments(pouch, namespace_name,
                                                  &retired_manifest, &manifest);
    cleanup_unreferenced = 1;
  }
  if (rc == LC_OK && cleanup_unreferenced) {
    rc = lc_pouch_query_index_unlink_unreferenced_artifacts(
        pouch, namespace_name, &manifest, error);
  }
  if (rc == LC_OK) {
    lc_pouch_query_index_manifest_trust_remember(pouch, namespace_name,
                                                 state_index_seq, &manifest);
    out->index_seq = state_index_seq;
    out->repaired = 1;
  }

cleanup:
  if (rc == LC_OK) {
    pslog_field fields[7];

    fields[0] = lc_log_str_field("ns", namespace_name);
    fields[1] = lc_log_u64_field("index_seq", out->index_seq);
    fields[2] = lc_log_bool_field("repaired", out->repaired);
    fields[3] = lc_log_bool_field("full_rebuild", full_rebuild);
    fields[4] = lc_log_bool_field("pending_entries", use_pending_entries);
    fields[5] = lc_log_bool_field("pending_segment", use_pending_segment);
    fields[6] = lc_log_u64_field("records", segment_row_count);
    lc_log_debug(pouch->logger, "index.flush", fields, 7U);
  } else {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("ns", namespace_name);
    fields[1] = lc_log_u64_field("index_seq", state_index_seq);
    fields[2] = lc_log_error_field("error", error);
    fields[3] = lc_log_code_field(error);
    lc_log_error(pouch->logger, "index.flush.error", fields, 4U);
  }
  lc_free_with_allocator(&pouch->allocator, packed_generation);
  lc_free_with_allocator(&pouch->allocator, temporal_term_generation);
  lc_free_with_allocator(&pouch->allocator, trigram_term_generation);
  lc_free_with_allocator(&pouch->allocator, text_term_generation);
  lc_free_with_allocator(&pouch->allocator, range_term_generation);
  lc_free_with_allocator(&pouch->allocator, presence_term_generation);
  lc_free_with_allocator(&pouch->allocator, exact_term_generation);
  lc_free_with_allocator(&pouch->allocator, doc_table_generation);
  lc_free_with_allocator(&pouch->allocator, deletes.bytes);
  lc_pouch_query_index_pending_entries_cleanup(&pouch->allocator,
                                               pending_entries);
  lc_pouch_query_index_pending_segment_cleanup(&pouch->allocator,
                                               pending_segment);
  lc_free_with_allocator(&pouch->allocator, pending_segment);
  lc_free_with_allocator(&pouch->allocator, text.bytes);
  lc_free_with_allocator(&pouch->allocator, extract_row_indices);
  lc_free_with_allocator(&pouch->allocator, extract_keys);
  lc_pouch_query_index_incremental_context_cleanup(&incremental);
  lc_pouch_query_index_summary_cleanup(&summary);
  lc_pouch_query_index_manifest_cleanup(&pouch->allocator, &retired_manifest);
  lc_pouch_query_index_manifest_cleanup(&pouch->allocator, &manifest);
  lc_pouch_query_index_segmented_paths_cleanup(
      pouch, &header_path, &doc_table_path, &exact_term_path,
      &presence_term_path, &range_term_path, &text_term_path,
      &trigram_term_path, &temporal_term_path, &delete_path);
  lc_free_with_allocator(&pouch->allocator, packed_path);
  lc_free_with_allocator(&pouch->allocator, manifest_path);
  return rc;
}

int lc_pouch_query_index_flush(lc_pouch *pouch, const char *namespace_name,
                               lc_pouch_generation state_index_seq,
                               lc_pouch_query_index_flush_result *out,
                               lc_error *error) {
  return lc_pouch_query_index_flush_segmented(pouch, namespace_name,
                                              state_index_seq, 0, out, error);
}

int lc_pouch_query_index_flush_validated(lc_pouch *pouch,
                                         const char *namespace_name,
                                         lc_pouch_generation state_index_seq,
                                         lc_pouch_query_index_flush_result *out,
                                         lc_error *error) {
  return lc_pouch_query_index_flush_segmented(pouch, namespace_name,
                                              state_index_seq, 1, out, error);
}

int lc_pouch_query_index_manifest_seq(lc_pouch *pouch,
                                      const char *namespace_name,
                                      lc_pouch_generation *index_seq,
                                      lc_error *error) {
  lc_pouch_query_index_manifest manifest;
  char *manifest_path;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index manifest sequence requires pouch, "
                        "namespace, and output",
                        NULL, NULL, NULL);
  }
  *index_seq = 0UL;
  memset(&manifest, 0, sizeof(manifest));
  manifest_path =
      lc_pouch_query_index_manifest_path(pouch, namespace_name, error);
  if (manifest_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_query_index_manifest_read(pouch, manifest_path, &manifest,
                                          error);
  lc_free_with_allocator(&pouch->allocator, manifest_path);
  if (rc == LC_OK && manifest.present) {
    if (!manifest.valid) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index manifest is invalid", NULL, NULL,
                        "pouch");
    } else {
      *index_seq = manifest.index_seq;
      lc_pouch_query_index_manifest_trust_remember(
          pouch, namespace_name, manifest.index_seq, &manifest);
    }
  }
  lc_pouch_query_index_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

int lc_pouch_query_index_ensure_current(lc_pouch *pouch,
                                        const char *namespace_name,
                                        lc_pouch_generation state_index_seq,
                                        int validate_current,
                                        lc_pouch_query_index_flush_result *out,
                                        lc_error *error) {
  lc_pouch_query_index_manifest manifest;
  const lc_pouch_query_index_manifest *manifest_snapshot;
  char *manifest_path;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_query_index_ensure_current requires pouch, "
                        "namespace, and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  manifest_snapshot = lc_pouch_query_index_manifest_trust_snapshot(
      pouch, namespace_name, state_index_seq);
  if (!validate_current && manifest_snapshot != NULL) {
    out->index_seq = state_index_seq;
    return LC_OK;
  }
  memset(&manifest, 0, sizeof(manifest));
  manifest_path =
      lc_pouch_query_index_manifest_path(pouch, namespace_name, error);
  if (manifest_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_query_index_manifest_read(pouch, manifest_path, &manifest,
                                          error);
  lc_free_with_allocator(&pouch->allocator, manifest_path);
  if (rc != LC_OK) {
    lc_pouch_query_index_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  if (manifest.present && manifest.valid &&
      manifest.index_seq == state_index_seq) {
    int segments_valid;

    if (!validate_current) {
      out->index_seq = state_index_seq;
      lc_pouch_query_index_manifest_cleanup(&pouch->allocator, &manifest);
      return LC_OK;
    }
    segments_valid = 0;
    rc = lc_pouch_query_index_manifest_segments_validate(
        pouch, namespace_name, &manifest, &segments_valid, error);
    if (rc != LC_OK) {
      lc_pouch_query_index_manifest_cleanup(&pouch->allocator, &manifest);
      return rc;
    }
    if (segments_valid) {
      out->index_seq = state_index_seq;
      lc_pouch_query_index_manifest_cleanup(&pouch->allocator, &manifest);
      return LC_OK;
    }
  }
  lc_pouch_query_index_manifest_cleanup(&pouch->allocator, &manifest);
  if (validate_current) {
    return lc_pouch_query_index_flush_validated(pouch, namespace_name,
                                                state_index_seq, out, error);
  }
  return lc_pouch_query_index_flush(pouch, namespace_name, state_index_seq, out,
                                    error);
}

int lc_pouch_query_index_visit(lc_pouch *pouch, const char *namespace_name,
                               lc_pouch_query_index_row_visit_fn visit,
                               void *context, lc_pouch_generation *index_seq,
                               lc_error *error) {
  lc_pouch_index_result_row_list rows;
  lc_pouch_query_index_row_view row_view;
  size_t row_index;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_query_index_visit requires pouch, "
                        "namespace, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  *index_seq = 0UL;
  memset(&rows, 0, sizeof(rows));
  rc = lc_pouch_query_index_segmented_collect(
      pouch, namespace_name, LC_POUCH_QUERY_INDEX_SEGMENTED_ALL, NULL, 0U, NULL,
      NULL, NULL, 0, 0, 0, NULL, NULL, &rows, index_seq, NULL, error);
  if (rc == LC_OK) {
    for (row_index = 0U; rc == LC_OK && row_index < rows.count; ++row_index) {
      lc_pouch_index_result_row *row;

      row = &rows.items[row_index];
      memset(&row_view, 0, sizeof(row_view));
      row_view.key = row->key;
      row_view.key_hex = row->key_hex;
      row_view.doc_id = row->doc_id;
      row_view.version = row->version;
      row_view.bytes = row->bytes;
      row_view.has_query_hidden = row->has_query_hidden;
      row_view.query_hidden = row->query_hidden;
      rc = visit(&row_view, context, error);
    }
    if (rc == LC_POUCH_STATE_READ_MANY_STOP) {
      rc = LC_OK;
    }
  }
  lc_pouch_index_result_row_list_cleanup(&pouch->allocator, &rows);
  return rc;
}

static int lc_pouch_query_index_visit_term_match(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *value, char value_type, int prefix_match, int contains_match,
    int ignore_case, const lc_pouch_query_index_range_bounds *range_bounds,
    const lc_pouch_query_index_date_bounds *date_bounds,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  lc_pouch_query_index_read_result header_artifact;
  lc_pouch_query_index_term_reader reader;
  lc_pouch_index_term_key *exact_terms;
  char *header_path;
  char *field_hex;
  char *value_hex;
  size_t exact_term_count;
  int exact_scalar;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      field == NULL || field[0] == '\0' ||
      (value == NULL && range_bounds == NULL && date_bounds == NULL) ||
      visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index term lookup requires pouch, "
                        "namespace, field, value, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  *index_seq = 0UL;
  header_path = lc_pouch_query_index_path(pouch, namespace_name, error);
  if (header_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  exact_terms = NULL;
  exact_term_count = 0U;
  field_hex = NULL;
  value_hex = NULL;
  exact_scalar = !prefix_match && !contains_match && !ignore_case &&
                 range_bounds == NULL && date_bounds == NULL;
  if (exact_scalar && (value_type != 's' && value_type != 'n' &&
                       value_type != 'b' && value_type != 'z')) {
    lc_free_with_allocator(&pouch->allocator, header_path);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index scalar lookup requires a JSON "
                        "scalar value type",
                        NULL, NULL, NULL);
  }
  if (exact_scalar) {
    char value_types[1];

    value_types[0] = value_type;
    rc = lc_pouch_query_index_build_exact_terms_for_field(
        field, &value, value_types, 1U, &exact_terms, &exact_term_count,
        &pouch->allocator, error);
  } else {
    field_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, field);
    value_hex = range_bounds == NULL && date_bounds == NULL
                    ? lc_pouch_query_index_hex_encode(&pouch->allocator, value)
                    : lc_strdup_with_allocator(&pouch->allocator, "");
    rc = field_hex != NULL && value_hex != NULL ? LC_OK : LC_ERR_NOMEM;
  }
  if (rc != LC_OK) {
    if (rc == LC_ERR_NOMEM) {
      lc_error_set(error, LC_ERR_NOMEM, 0L,
                   "failed to allocate pouch query-index scalar lookup", NULL,
                   NULL, NULL);
    }
    lc_pouch_index_term_keys_cleanup(&pouch->allocator, exact_terms, 1U);
    lc_free_with_allocator(&pouch->allocator, field_hex);
    lc_free_with_allocator(&pouch->allocator, value_hex);
    lc_free_with_allocator(&pouch->allocator, header_path);
    return rc;
  }
  if (exact_scalar) {
    rc = lc_pouch_query_index_visit_exact_generation(
        pouch, namespace_name, exact_terms, exact_term_count, visit, context,
        index_seq, error);
    lc_pouch_index_term_keys_cleanup(&pouch->allocator, exact_terms, 1U);
    lc_free_with_allocator(&pouch->allocator, header_path);
    return rc;
  }
  memset(&reader, 0, sizeof(reader));
  reader.allocator = &pouch->allocator;
  reader.field_hex = field_hex;
  reader.value_hex = value_hex;
  reader.value_text = value;
  reader.prefix_match = prefix_match;
  reader.contains_match = contains_match;
  reader.ignore_case = ignore_case;
  reader.string_values_only = prefix_match || contains_match;
  if (range_bounds != NULL) {
    reader.range_match = 1;
    reader.range_bounds = *range_bounds;
  }
  if (date_bounds != NULL) {
    reader.date_match = 1;
    reader.date_bounds = *date_bounds;
    rc = lc_pouch_index_parse_date_bounds(date_bounds,
                                          &reader.parsed_date_bounds, error);
    if (rc != LC_OK) {
      lc_free_with_allocator(&pouch->allocator, reader.value_scratch);
      lc_free_with_allocator(&pouch->allocator, field_hex);
      lc_free_with_allocator(&pouch->allocator, value_hex);
      lc_free_with_allocator(&pouch->allocator, header_path);
      return rc;
    }
  }
  reader.visit = visit;
  reader.context = context;
  rc = lc_pouch_query_index_read_with_reader(pouch, namespace_name, header_path,
                                             &header_artifact, NULL, &reader,
                                             NULL, error);
  if (rc == LC_OK && (!header_artifact.present || !header_artifact.valid)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index header artifact is not readable", NULL,
                      NULL, "pouch");
  }
  if (rc == LC_OK && !header_artifact.term_index_complete) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index scalar postings are incomplete", NULL,
                      NULL, "pouch");
  }
  if (rc == LC_OK) {
    *index_seq = header_artifact.index_seq;
  }
  lc_free_with_allocator(&pouch->allocator, reader.value_scratch);
  lc_pouch_index_term_keys_cleanup(&pouch->allocator, exact_terms, 1U);
  lc_free_with_allocator(&pouch->allocator, field_hex);
  lc_free_with_allocator(&pouch->allocator, value_hex);
  lc_free_with_allocator(&pouch->allocator, header_path);
  return rc;
}

int lc_pouch_query_index_visit_scalar(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *value, char value_type, lc_pouch_query_index_key_visit_fn visit,
    void *context, lc_pouch_generation *index_seq, lc_error *error) {
  return lc_pouch_query_index_visit_term_match(
      pouch, namespace_name, field, value, value_type, 0, 0, 0, NULL, NULL,
      visit, context, index_seq, error);
}

int lc_pouch_query_index_visit_scalar_any(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *const *values, const char *value_types, size_t value_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  lc_pouch_index_term_key *exact_terms;
  size_t exact_term_count;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      field == NULL || field[0] == '\0' || values == NULL ||
      value_types == NULL || value_count == 0U || visit == NULL ||
      index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index multi-scalar lookup requires "
                        "pouch, namespace, field, values, visitor, and "
                        "index_seq",
                        NULL, NULL, NULL);
  }
  if (value_count == 1U) {
    return lc_pouch_query_index_visit_scalar(pouch, namespace_name, field,
                                             values[0], value_types[0], visit,
                                             context, index_seq, error);
  }
  *index_seq = 0UL;
  exact_terms = NULL;
  exact_term_count = 0U;
  rc = lc_pouch_query_index_build_exact_terms_for_field(
      field, values, value_types, value_count, &exact_terms, &exact_term_count,
      &pouch->allocator, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_visit_exact_generation(
        pouch, namespace_name, exact_terms, exact_term_count, visit, context,
        index_seq, error);
  }
  lc_pouch_index_term_keys_cleanup(&pouch->allocator, exact_terms,
                                   exact_term_count);
  return rc;
}

int lc_pouch_query_index_visit_scalar_terms(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_scalar_term *terms, size_t term_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  lc_pouch_index_term_key *exact_terms;
  size_t exact_term_count;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      terms == NULL || term_count == 0U || visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index scalar term-set lookup requires "
                        "pouch, namespace, terms, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  if (term_count == 1U) {
    return lc_pouch_query_index_visit_scalar(
        pouch, namespace_name, terms[0].field, terms[0].value,
        terms[0].value_type, visit, context, index_seq, error);
  }
  *index_seq = 0UL;
  exact_terms = NULL;
  exact_term_count = 0U;
  rc = lc_pouch_query_index_build_exact_terms(terms, term_count, &exact_terms,
                                              &exact_term_count,
                                              &pouch->allocator, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_visit_exact_generation(
        pouch, namespace_name, exact_terms, exact_term_count, visit, context,
        index_seq, error);
  }
  lc_pouch_index_term_keys_cleanup(&pouch->allocator, exact_terms,
                                   exact_term_count);
  return rc;
}

static int
lc_pouch_query_index_rows_emit(const lc_pouch_index_result_row_list *rows,
                               lc_pouch_query_index_key_visit_fn visit,
                               void *context, lc_error *error) {
  lc_pouch_query_index_key_view key_view;
  size_t index;
  int rc;

  if (rows == NULL || visit == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index row emit requires rows and visitor",
                        NULL, NULL, NULL);
  }
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < rows->count; ++index) {
    const lc_pouch_index_result_row *row;

    row = &rows->items[index];
    memset(&key_view, 0, sizeof(key_view));
    key_view.key = row->key;
    key_view.key_hex = row->key_hex;
    key_view.doc_id = row->doc_id;
    key_view.version = row->version;
    key_view.bytes = row->bytes;
    key_view.has_query_hidden = row->has_query_hidden;
    key_view.query_hidden = row->query_hidden;
    key_view.value_index = row->value_index;
    rc = visit(&key_view, context, error);
  }
  if (rc == LC_POUCH_STATE_READ_MANY_STOP) {
    rc = LC_OK;
  }
  return rc;
}

static void lc_pouch_query_index_generation_cache_entry_cleanup(
    const lc_allocator *allocator,
    lc_pouch_query_index_generation_cache_entry *entry) {
  if (entry == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, entry->path);
  lc_free_with_allocator(allocator, entry->bytes);
  lc_free_with_allocator(allocator, entry->terms);
  lc_free_with_allocator(allocator, entry->postings);
  memset(entry, 0, sizeof(*entry));
}

static void lc_pouch_query_index_doc_table_cache_entry_cleanup(
    const lc_allocator *allocator,
    lc_pouch_query_index_doc_table_cache_entry *entry) {
  size_t index;

  if (entry == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, entry->path);
  for (index = 0U; index < entry->decoded_key_count; ++index) {
    lc_free_with_allocator(allocator, entry->decoded_keys[index]);
  }
  lc_free_with_allocator(allocator, entry->decoded_keys);
  lc_pouch_index_doc_table_cleanup(allocator, &entry->table);
  memset(entry, 0, sizeof(*entry));
}

static void lc_pouch_query_index_artifact_cache_entry_cleanup(
    const lc_allocator *allocator,
    lc_pouch_query_index_artifact_cache_entry *entry) {
  if (entry == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, entry->path);
  memset(entry, 0, sizeof(*entry));
}

static int lc_pouch_query_index_artifact_signature(
    const char *path, lc_pouch_query_index_file_signature *out) {
  struct stat st;

  if (path == NULL || out == NULL) {
    return 0;
  }
  memset(out, 0, sizeof(*out));
  if (stat(path, &st) != 0) {
    return 0;
  }
  if (st.st_size < 0) {
    return 0;
  }
  out->size = (uint64_t)st.st_size;
#if defined(__APPLE__)
  out->mtime = (unsigned long)st.st_mtimespec.tv_sec;
  out->mtime_nsec = (unsigned long)st.st_mtimespec.tv_nsec;
  out->ctime = (unsigned long)st.st_ctimespec.tv_sec;
  out->ctime_nsec = (unsigned long)st.st_ctimespec.tv_nsec;
#elif defined(__linux__)
  out->mtime = (unsigned long)st.st_mtim.tv_sec;
  out->mtime_nsec = (unsigned long)st.st_mtim.tv_nsec;
  out->ctime = (unsigned long)st.st_ctim.tv_sec;
  out->ctime_nsec = (unsigned long)st.st_ctim.tv_nsec;
#else
  out->mtime = (unsigned long)st.st_mtime;
  out->mtime_nsec = 0UL;
  out->ctime = (unsigned long)st.st_ctime;
  out->ctime_nsec = 0UL;
#endif
  out->inode = (unsigned long)st.st_ino;
  out->present = 1;
  return 1;
}

static int lc_pouch_query_index_artifact_signature_equal(
    const lc_pouch_query_index_file_signature *left,
    const lc_pouch_query_index_file_signature *right) {
  return left != NULL && right != NULL && left->present && right->present &&
         left->size == right->size && left->mtime == right->mtime &&
         left->mtime_nsec == right->mtime_nsec && left->ctime == right->ctime &&
         left->ctime_nsec == right->ctime_nsec && left->inode == right->inode;
}

static void lc_pouch_query_index_manifest_signature_from_file(
    lc_pouch_query_index_manifest_artifact_signature *out,
    const lc_pouch_query_index_file_signature *signature) {
  if (out == NULL) {
    return;
  }
  memset(out, 0, sizeof(*out));
  if (signature == NULL || !signature->present) {
    return;
  }
  out->size = signature->size;
  out->mtime = signature->mtime;
  out->mtime_nsec = signature->mtime_nsec;
  out->ctime = signature->ctime;
  out->ctime_nsec = signature->ctime_nsec;
  out->inode = signature->inode;
  out->present = 1;
}

static void lc_pouch_query_index_manifest_segment_capture_artifacts(
    lc_pouch_query_index_manifest_segment *segment, const char *header_path,
    const char *doc_table_path, const char *exact_term_path,
    const char *presence_term_path, const char *range_term_path,
    const char *text_term_path, const char *trigram_term_path,
    const char *temporal_term_path, const char *delete_path,
    const char *packed_path) {
  const char *paths[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_COUNT];
  lc_pouch_query_index_file_signature packed_signature;
  size_t index;

  if (segment == NULL) {
    return;
  }
  if (packed_path != NULL &&
      lc_pouch_query_index_artifact_signature(packed_path, &packed_signature)) {
    lc_pouch_query_index_file_signature header_signature;

    if (lc_pouch_query_index_artifact_signature(header_path,
                                                &header_signature)) {
      lc_pouch_query_index_manifest_signature_from_file(
          &segment->artifact_signatures
               [LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_HEADER],
          &header_signature);
    } else {
      memset(&segment->artifact_signatures
                  [LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_HEADER],
             0,
             sizeof(segment->artifact_signatures
                        [LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_HEADER]));
    }
    for (index = LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_DOC_TABLE;
         index < LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_COUNT; ++index) {
      lc_pouch_query_index_manifest_signature_from_file(
          &segment->artifact_signatures[index], &packed_signature);
    }
    return;
  }
  paths[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_HEADER] = header_path;
  paths[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_DOC_TABLE] = doc_table_path;
  paths[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_EXACT] = exact_term_path;
  paths[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_PRESENCE] = presence_term_path;
  paths[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_RANGE] = range_term_path;
  paths[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_TEXT] = text_term_path;
  paths[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_TRIGRAM] = trigram_term_path;
  paths[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_TEMPORAL] = temporal_term_path;
  paths[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_DELETE] = delete_path;
  for (index = 0U; index < LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_COUNT;
       ++index) {
    lc_pouch_query_index_file_signature signature;

    memset(&segment->artifact_signatures[index], 0,
           sizeof(segment->artifact_signatures[index]));
    if (lc_pouch_query_index_artifact_signature(paths[index], &signature)) {
      lc_pouch_query_index_manifest_signature_from_file(
          &segment->artifact_signatures[index], &signature);
    }
  }
}

static int lc_pouch_query_index_manifest_segment_artifacts_match(
    const lc_pouch_query_index_manifest_segment *segment,
    const char *header_path, const char *doc_table_path,
    const char *exact_term_path, const char *presence_term_path,
    const char *range_term_path, const char *text_term_path,
    const char *trigram_term_path, const char *temporal_term_path,
    const char *delete_path, const char *packed_path,
    lc_pouch_query_index_file_signature *actual_signatures) {
  const char *paths[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_COUNT];
  lc_pouch_query_index_file_signature packed_signature;
  size_t index;

  if (segment == NULL) {
    return 0;
  }
  paths[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_HEADER] = header_path;
  paths[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_DOC_TABLE] = doc_table_path;
  paths[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_EXACT] = exact_term_path;
  paths[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_PRESENCE] = presence_term_path;
  paths[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_RANGE] = range_term_path;
  paths[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_TEXT] = text_term_path;
  paths[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_TRIGRAM] = trigram_term_path;
  paths[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_TEMPORAL] = temporal_term_path;
  paths[LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_DELETE] = delete_path;
  if (packed_path != NULL &&
      lc_pouch_query_index_artifact_signature(packed_path, &packed_signature)) {
    lc_pouch_query_index_file_signature header_signature;

    if (segment->artifact_signatures
                [LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_HEADER]
                    .present == 0 ||
        !lc_pouch_query_index_artifact_signature(header_path,
                                                 &header_signature) ||
        segment->artifact_signatures
                [LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_HEADER]
                    .size != header_signature.size ||
        segment->artifact_signatures
                [LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_HEADER]
                    .mtime != header_signature.mtime ||
        segment->artifact_signatures
                [LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_HEADER]
                    .mtime_nsec != header_signature.mtime_nsec ||
        segment->artifact_signatures
                [LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_HEADER]
                    .ctime != header_signature.ctime ||
        segment->artifact_signatures
                [LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_HEADER]
                    .ctime_nsec != header_signature.ctime_nsec ||
        segment->artifact_signatures
                [LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_HEADER]
                    .inode != header_signature.inode) {
      return 0;
    }
    for (index = LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_DOC_TABLE;
         index < LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_COUNT; ++index) {
      lc_pouch_query_index_file_signature obsolete_signature;

      if (lc_pouch_query_index_artifact_signature(paths[index],
                                                  &obsolete_signature)) {
        return 0;
      }
    }
    for (index = 0U; index < LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_COUNT;
         ++index) {
      if (actual_signatures != NULL) {
        memset(&actual_signatures[index], 0, sizeof(actual_signatures[index]));
      }
      if (index == LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_HEADER) {
        if (actual_signatures != NULL) {
          actual_signatures[index] = header_signature;
        }
        continue;
      }
      if (segment->artifact_signatures[index].present == 0 ||
          segment->artifact_signatures[index].size != packed_signature.size ||
          segment->artifact_signatures[index].mtime != packed_signature.mtime ||
          segment->artifact_signatures[index].mtime_nsec !=
              packed_signature.mtime_nsec ||
          segment->artifact_signatures[index].ctime != packed_signature.ctime ||
          segment->artifact_signatures[index].ctime_nsec !=
              packed_signature.ctime_nsec ||
          segment->artifact_signatures[index].inode != packed_signature.inode) {
        return 0;
      }
      if (actual_signatures != NULL) {
        actual_signatures[index] = packed_signature;
      }
    }
    return 1;
  }
  for (index = 0U; index < LC_POUCH_QUERY_INDEX_MANIFEST_ARTIFACT_COUNT;
       ++index) {
    lc_pouch_query_index_file_signature actual;

    if (actual_signatures != NULL) {
      memset(&actual_signatures[index], 0, sizeof(actual_signatures[index]));
    }
    if (segment->artifact_signatures[index].present == 0 ||
        !lc_pouch_query_index_artifact_signature(paths[index], &actual) ||
        segment->artifact_signatures[index].size != actual.size ||
        segment->artifact_signatures[index].mtime != actual.mtime ||
        segment->artifact_signatures[index].mtime_nsec != actual.mtime_nsec ||
        segment->artifact_signatures[index].ctime != actual.ctime ||
        segment->artifact_signatures[index].ctime_nsec != actual.ctime_nsec ||
        segment->artifact_signatures[index].inode != actual.inode) {
      return 0;
    }
    if (actual_signatures != NULL) {
      actual_signatures[index] = actual;
    }
  }
  return 1;
}

static int lc_pouch_query_index_artifact_cache_valid(
    lc_pouch *pouch, const char *path, int kind, lc_pouch_generation expected_a,
    unsigned long expected_b, unsigned long expected_c) {
  lc_pouch_query_index_artifact_cache_entry *entry;
  lc_pouch_query_index_artifact_cache_entry *previous;
  lc_pouch_query_index_file_signature signature;

  if (pouch == NULL || path == NULL ||
      !lc_pouch_query_index_artifact_signature(path, &signature)) {
    return 0;
  }
  previous = NULL;
  entry = pouch->query_artifact_cache;
  while (entry != NULL) {
    if (entry->kind == kind && entry->expected_a == expected_a &&
        entry->expected_b == expected_b && entry->expected_c == expected_c &&
        strcmp(entry->path, path) == 0 &&
        lc_pouch_query_index_artifact_signature_equal(&entry->signature,
                                                      &signature)) {
      if (previous != NULL) {
        previous->next = entry->next;
        entry->next = pouch->query_artifact_cache;
        pouch->query_artifact_cache = entry;
      }
      return 1;
    }
    previous = entry;
    entry = entry->next;
  }
  return 0;
}

static void lc_pouch_query_index_artifact_cache_remember(
    lc_pouch *pouch, const char *path, int kind, lc_pouch_generation expected_a,
    unsigned long expected_b, unsigned long expected_c) {
  lc_pouch_query_index_file_signature signature;

  if (pouch == NULL || path == NULL ||
      !lc_pouch_query_index_artifact_signature(path, &signature)) {
    return;
  }
  lc_pouch_query_index_artifact_cache_remember_signature(
      pouch, path, kind, expected_a, expected_b, expected_c, &signature);
}

static void lc_pouch_query_index_artifact_cache_remember_signature(
    lc_pouch *pouch, const char *path, int kind, lc_pouch_generation expected_a,
    unsigned long expected_b, unsigned long expected_c,
    const lc_pouch_query_index_file_signature *signature) {
  lc_pouch_query_index_artifact_cache_entry *entry;
  lc_pouch_query_index_artifact_cache_entry *previous;

  if (pouch == NULL || path == NULL || signature == NULL ||
      !signature->present) {
    return;
  }
  previous = NULL;
  entry = pouch->query_artifact_cache;
  while (entry != NULL) {
    if (entry->kind == kind && entry->expected_a == expected_a &&
        entry->expected_b == expected_b && entry->expected_c == expected_c &&
        strcmp(entry->path, path) == 0) {
      entry->signature = *signature;
      if (previous != NULL) {
        previous->next = entry->next;
        entry->next = pouch->query_artifact_cache;
        pouch->query_artifact_cache = entry;
      }
      return;
    }
    previous = entry;
    entry = entry->next;
  }
  entry = (lc_pouch_query_index_artifact_cache_entry *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*entry));
  if (entry == NULL) {
    return;
  }
  entry->path = lc_strdup_with_allocator(&pouch->allocator, path);
  if (entry->path == NULL) {
    lc_free_with_allocator(&pouch->allocator, entry);
    return;
  }
  entry->kind = kind;
  entry->expected_a = expected_a;
  entry->expected_b = expected_b;
  entry->expected_c = expected_c;
  entry->signature = *signature;
  entry->next = pouch->query_artifact_cache;
  pouch->query_artifact_cache = entry;
  ++pouch->query_artifact_cache_count;
  while (pouch->query_artifact_cache_count >
         LC_POUCH_QUERY_INDEX_ARTIFACT_CACHE_MAX) {
    lc_pouch_query_index_artifact_cache_entry *victim_prev;
    lc_pouch_query_index_artifact_cache_entry *victim;

    victim_prev = NULL;
    victim = pouch->query_artifact_cache;
    while (victim != NULL && victim->next != NULL) {
      victim_prev = victim;
      victim = victim->next;
    }
    if (victim == NULL) {
      break;
    }
    if (victim_prev != NULL) {
      victim_prev->next = NULL;
    } else {
      pouch->query_artifact_cache = NULL;
    }
    lc_pouch_query_index_artifact_cache_entry_cleanup(&pouch->allocator,
                                                      victim);
    lc_free_with_allocator(&pouch->allocator, victim);
    --pouch->query_artifact_cache_count;
  }
}

static void lc_pouch_query_index_manifest_trust_entry_cleanup(
    const lc_allocator *allocator,
    lc_pouch_query_index_manifest_trust_entry *entry) {
  if (entry == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, entry->namespace_name);
  lc_pouch_query_index_manifest_cleanup(allocator, &entry->manifest);
  memset(entry, 0, sizeof(*entry));
}

static int
lc_pouch_query_index_manifest_trust_valid(const lc_pouch *pouch,
                                          const char *namespace_name,
                                          lc_pouch_generation index_seq) {
  lc_pouch_query_index_manifest_trust_entry *entry;
  uint64_t writer_mode_epoch;

  if (pouch == NULL ||
      !lc_pouch_single_writer_snapshot((lc_pouch *)pouch, &writer_mode_epoch) ||
      namespace_name == NULL || namespace_name[0] == '\0') {
    return 0;
  }
  entry = pouch->query_manifest_trust;
  while (entry != NULL) {
    if (entry->writer_mode_epoch == writer_mode_epoch &&
        entry->index_seq == index_seq && entry->namespace_name != NULL &&
        strcmp(entry->namespace_name, namespace_name) == 0) {
      return 1;
    }
    entry = entry->next;
  }
  return 0;
}

static void lc_pouch_query_index_manifest_trust_remember(
    lc_pouch *pouch, const char *namespace_name, lc_pouch_generation index_seq,
    const lc_pouch_query_index_manifest *manifest) {
  lc_pouch_query_index_manifest_trust_entry *entry;
  uint64_t writer_mode_epoch;

  if (pouch == NULL ||
      !lc_pouch_single_writer_snapshot(pouch, &writer_mode_epoch) ||
      namespace_name == NULL || namespace_name[0] == '\0') {
    return;
  }
  entry = pouch->query_manifest_trust;
  while (entry != NULL) {
    if (entry->namespace_name != NULL &&
        strcmp(entry->namespace_name, namespace_name) == 0) {
      entry->index_seq = index_seq;
      entry->writer_mode_epoch = writer_mode_epoch;
      lc_pouch_query_index_manifest_cleanup(&pouch->allocator,
                                            &entry->manifest);
      entry->manifest_cached = 0;
      if (manifest != NULL && manifest->present && manifest->valid &&
          manifest->index_seq == index_seq &&
          lc_pouch_query_index_manifest_clone(
              &pouch->allocator, &entry->manifest, manifest) == LC_OK) {
        entry->manifest_cached = 1;
      }
      return;
    }
    entry = entry->next;
  }
  entry = (lc_pouch_query_index_manifest_trust_entry *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*entry));
  if (entry == NULL) {
    return;
  }
  entry->namespace_name =
      lc_strdup_with_allocator(&pouch->allocator, namespace_name);
  if (entry->namespace_name == NULL) {
    lc_free_with_allocator(&pouch->allocator, entry);
    return;
  }
  entry->index_seq = index_seq;
  entry->writer_mode_epoch = writer_mode_epoch;
  if (manifest != NULL && manifest->present && manifest->valid &&
      manifest->index_seq == index_seq &&
      lc_pouch_query_index_manifest_clone(&pouch->allocator, &entry->manifest,
                                          manifest) == LC_OK) {
    entry->manifest_cached = 1;
  }
  entry->next = pouch->query_manifest_trust;
  pouch->query_manifest_trust = entry;
}

static const lc_pouch_query_index_manifest *
lc_pouch_query_index_manifest_trust_snapshot(const lc_pouch *pouch,
                                             const char *namespace_name,
                                             lc_pouch_generation index_seq) {
  lc_pouch_query_index_manifest_trust_entry *entry;
  uint64_t writer_mode_epoch;

  if (pouch == NULL ||
      !lc_pouch_single_writer_snapshot((lc_pouch *)pouch, &writer_mode_epoch) ||
      namespace_name == NULL || namespace_name[0] == '\0') {
    return NULL;
  }
  entry = pouch->query_manifest_trust;
  while (entry != NULL) {
    if (entry->manifest_cached &&
        entry->writer_mode_epoch == writer_mode_epoch &&
        entry->index_seq == index_seq &&
        entry->manifest.index_seq == index_seq &&
        entry->namespace_name != NULL &&
        strcmp(entry->namespace_name, namespace_name) == 0) {
      return &entry->manifest;
    }
    entry = entry->next;
  }
  return NULL;
}

void lc_pouch_query_index_cache_cleanup(lc_pouch *pouch) {
  lc_pouch_query_index_generation_cache_entry *entry;
  lc_pouch_query_index_doc_table_cache_entry *doc_entry;
  lc_pouch_query_index_artifact_cache_entry *artifact_entry;
  lc_pouch_query_index_packed_cache_entry *packed_entry;
  lc_pouch_query_index_pending_entry *pending_entry;
  lc_pouch_query_index_pending_segment *pending_segment;
  lc_pouch_query_index_manifest_trust_entry *trust_entry;

  if (pouch == NULL) {
    return;
  }
  entry = pouch->query_generation_cache;
  while (entry != NULL) {
    lc_pouch_query_index_generation_cache_entry *next;

    next = entry->next;
    lc_pouch_query_index_generation_cache_entry_cleanup(&pouch->allocator,
                                                        entry);
    lc_free_with_allocator(&pouch->allocator, entry);
    entry = next;
  }
  pouch->query_generation_cache = NULL;
  pouch->query_generation_cache_count = 0U;
  doc_entry = pouch->query_doc_table_cache;
  while (doc_entry != NULL) {
    lc_pouch_query_index_doc_table_cache_entry *next;

    next = doc_entry->next;
    lc_pouch_query_index_doc_table_cache_entry_cleanup(&pouch->allocator,
                                                       doc_entry);
    lc_free_with_allocator(&pouch->allocator, doc_entry);
    doc_entry = next;
  }
  pouch->query_doc_table_cache = NULL;
  pouch->query_doc_table_cache_count = 0U;
  artifact_entry = pouch->query_artifact_cache;
  while (artifact_entry != NULL) {
    lc_pouch_query_index_artifact_cache_entry *next;

    next = artifact_entry->next;
    lc_pouch_query_index_artifact_cache_entry_cleanup(&pouch->allocator,
                                                      artifact_entry);
    lc_free_with_allocator(&pouch->allocator, artifact_entry);
    artifact_entry = next;
  }
  pouch->query_artifact_cache = NULL;
  pouch->query_artifact_cache_count = 0U;
  packed_entry = pouch->query_packed_cache;
  while (packed_entry != NULL) {
    lc_pouch_query_index_packed_cache_entry *next;

    next = packed_entry->next;
    lc_pouch_query_index_packed_cache_entry_cleanup(&pouch->allocator,
                                                    packed_entry);
    lc_free_with_allocator(&pouch->allocator, packed_entry);
    packed_entry = next;
  }
  pouch->query_packed_cache = NULL;
  pouch->query_packed_cache_count = 0U;
  pending_entry = pouch->query_pending_index;
  while (pending_entry != NULL) {
    lc_pouch_query_index_pending_entry *next;

    next = pending_entry->next;
    lc_pouch_query_index_pending_entry_cleanup(&pouch->allocator,
                                               pending_entry);
    lc_free_with_allocator(&pouch->allocator, pending_entry);
    pending_entry = next;
  }
  pouch->query_pending_index = NULL;
  pouch->query_pending_index_count = 0U;
  pouch->query_pending_index_incomplete = 0;
  pending_segment = pouch->query_pending_segments;
  while (pending_segment != NULL) {
    lc_pouch_query_index_pending_segment *next;

    next = pending_segment->next;
    lc_pouch_query_index_pending_segment_cleanup(&pouch->allocator,
                                                 pending_segment);
    lc_free_with_allocator(&pouch->allocator, pending_segment);
    pending_segment = next;
  }
  pouch->query_pending_segments = NULL;
  trust_entry = pouch->query_manifest_trust;
  while (trust_entry != NULL) {
    lc_pouch_query_index_manifest_trust_entry *next;

    next = trust_entry->next;
    lc_pouch_query_index_manifest_trust_entry_cleanup(&pouch->allocator,
                                                      trust_entry);
    lc_free_with_allocator(&pouch->allocator, trust_entry);
    trust_entry = next;
  }
  pouch->query_manifest_trust = NULL;
}

static int lc_pouch_query_index_generation_cache_term_compare_parts(
    const char *left_field_hex, const char *left_value_hex,
    char left_value_type, const char *right_field_hex,
    const char *right_value_hex, char right_value_type) {
  int cmp;

  cmp = strcmp(left_field_hex, right_field_hex);
  if (cmp != 0) {
    return cmp;
  }
  cmp = strcmp(left_value_hex, right_value_hex);
  if (cmp != 0) {
    return cmp;
  }
  if (left_value_type < right_value_type) {
    return -1;
  }
  if (left_value_type > right_value_type) {
    return 1;
  }
  return 0;
}

static size_t lc_pouch_query_index_generation_cache_term_lower_bound(
    const lc_pouch_query_index_generation_cache_entry *entry,
    const char *field_hex, const char *value_hex, char value_type) {
  size_t low;
  size_t high;

  low = 0U;
  high = entry != NULL ? entry->term_count : 0U;
  while (low < high) {
    size_t mid;
    int cmp;

    mid = low + ((high - low) / 2U);
    cmp = lc_pouch_query_index_generation_cache_term_compare_parts(
        entry->terms[mid].field_hex, entry->terms[mid].value_hex,
        entry->terms[mid].value_type, field_hex, value_hex, value_type);
    if (cmp < 0) {
      low = mid + 1U;
    } else {
      high = mid;
    }
  }
  return low;
}

static int lc_pouch_query_index_generation_cache_find_term(
    const lc_pouch_query_index_generation_cache_entry *entry,
    const char *field_hex, const char *value_hex, char value_type,
    const lc_pouch_query_index_generation_cache_term **term) {
  size_t position;

  if (term != NULL) {
    *term = NULL;
  }
  if (entry == NULL || field_hex == NULL || value_hex == NULL) {
    return 0;
  }
  position = lc_pouch_query_index_generation_cache_term_lower_bound(
      entry, field_hex, value_hex, value_type);
  if (position >= entry->term_count ||
      lc_pouch_query_index_generation_cache_term_compare_parts(
          entry->terms[position].field_hex, entry->terms[position].value_hex,
          entry->terms[position].value_type, field_hex, value_hex,
          value_type) != 0) {
    return 0;
  }
  if (term != NULL) {
    *term = &entry->terms[position];
  }
  return 1;
}

static size_t lc_pouch_query_index_generation_cache_field_lower_bound(
    const lc_pouch_query_index_generation_cache_entry *entry,
    const char *field_hex) {
  size_t low;
  size_t high;

  low = 0U;
  high = entry != NULL ? entry->term_count : 0U;
  while (low < high) {
    size_t mid;

    mid = low + ((high - low) / 2U);
    if (strcmp(entry->terms[mid].field_hex, field_hex) < 0) {
      low = mid + 1U;
    } else {
      high = mid;
    }
  }
  return low;
}

static int lc_pouch_query_index_generation_cache_find_posting(
    const lc_pouch_query_index_generation_cache_entry *entry,
    unsigned long term_id,
    const lc_pouch_query_index_generation_cache_posting **posting) {
  size_t low;
  size_t high;

  if (posting != NULL) {
    *posting = NULL;
  }
  if (entry == NULL || term_id == 0UL) {
    return 0;
  }
  low = 0U;
  high = entry->posting_count;
  while (low < high) {
    size_t mid;

    mid = low + ((high - low) / 2U);
    if (entry->postings[mid].term_id < term_id) {
      low = mid + 1U;
    } else {
      high = mid;
    }
  }
  if (low >= entry->posting_count || entry->postings[low].term_id != term_id) {
    return 0;
  }
  if (posting != NULL) {
    *posting = &entry->postings[low];
  }
  return 1;
}

typedef struct lc_pouch_query_index_binary_cursor {
  const unsigned char *bytes;
  size_t length;
  size_t offset;
} lc_pouch_query_index_binary_cursor;

static int
lc_pouch_query_index_binary_read(lc_pouch_query_index_binary_cursor *cursor,
                                 size_t length, const unsigned char **out) {
  if (out != NULL) {
    *out = NULL;
  }
  if (cursor == NULL || (length > 0U && out == NULL)) {
    return 0;
  }
  if (length > cursor->length || cursor->offset > cursor->length - length) {
    return 0;
  }
  if (out != NULL) {
    *out = cursor->bytes + cursor->offset;
  }
  cursor->offset += length;
  return 1;
}

static int
lc_pouch_query_index_binary_u8(lc_pouch_query_index_binary_cursor *cursor,
                               unsigned char *out) {
  const unsigned char *bytes;

  if (out == NULL || !lc_pouch_query_index_binary_read(cursor, 1U, &bytes)) {
    return 0;
  }
  *out = bytes[0];
  return 1;
}

static int
lc_pouch_query_index_binary_u64(lc_pouch_query_index_binary_cursor *cursor,
                                uint64_t *out) {
  const unsigned char *bytes;
  uint64_t value;
  size_t index;

  if (out == NULL || !lc_pouch_query_index_binary_read(cursor, 8U, &bytes)) {
    return 0;
  }
  value = (uint64_t)0U;
  for (index = 0U; index < 8U; ++index) {
    value |= ((uint64_t)bytes[index]) << (index * 8U);
  }
  *out = value;
  return 1;
}

static int
lc_pouch_query_index_binary_ulong(lc_pouch_query_index_binary_cursor *cursor,
                                  unsigned long *out) {
  uint64_t value;

  if (out == NULL || !lc_pouch_query_index_binary_u64(cursor, &value) ||
      value > (uint64_t)ULONG_MAX) {
    return 0;
  }
  *out = (unsigned long)value;
  return 1;
}

static int
lc_pouch_query_index_binary_string(lc_pouch_query_index_binary_cursor *cursor,
                                   char **out) {
  const unsigned char *bytes;
  uint64_t length64;
  size_t length;

  if (out == NULL) {
    return 0;
  }
  *out = NULL;
  if (!lc_pouch_query_index_binary_u64(cursor, &length64) ||
      length64 > (uint64_t)((size_t)-1)) {
    return 0;
  }
  length = (size_t)length64;
  if (length > cursor->length || cursor->offset > cursor->length - length ||
      cursor->offset + length >= cursor->length) {
    return 0;
  }
  bytes = cursor->bytes + cursor->offset;
  if (memchr(bytes, '\0', length) != NULL ||
      cursor->bytes[cursor->offset + length] != (unsigned char)'\0') {
    return 0;
  }
  cursor->offset += length + 1U;
  *out = (char *)bytes;
  return 1;
}

static int lc_pouch_query_index_generation_cache_parse(
    const lc_allocator *allocator,
    lc_pouch_query_index_generation_cache_entry *entry, lc_error *error) {
  lc_pouch_query_index_binary_cursor cursor;
  const unsigned char *magic;
  uint64_t version;
  lc_pouch_generation index_seq;
  unsigned long row_count;
  unsigned long row_hash;
  unsigned long term_count;
  unsigned long posting_count;
  unsigned long index;
  unsigned char *term_id_seen;
  int rc;

  if (entry == NULL || entry->bytes == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch generation cache parse requires bytes", NULL,
                        NULL, NULL);
  }
  memset(&cursor, 0, sizeof(cursor));
  cursor.bytes = (const unsigned char *)entry->bytes;
  cursor.length = entry->length;
  term_id_seen = NULL;
  rc = LC_OK;
  magic = NULL;
  if (!lc_pouch_query_index_binary_read(
          &cursor, LC_POUCH_INDEX_TERM_GENERATION_MAGIC_LEN, &magic) ||
      memcmp(magic, LC_POUCH_INDEX_TERM_GENERATION_MAGIC,
             LC_POUCH_INDEX_TERM_GENERATION_MAGIC_LEN) != 0 ||
      !lc_pouch_query_index_binary_u64(&cursor, &version) ||
      version != LC_POUCH_INDEX_TERM_GENERATION_VERSION ||
      !lc_pouch_query_index_binary_u64(&cursor, &index_seq) ||
      !lc_pouch_query_index_binary_ulong(&cursor, &row_count) ||
      !lc_pouch_query_index_binary_ulong(&cursor, &row_hash)) {
    return LC_ERR_INVALID;
  }
  if (index_seq != entry->index_seq || row_count != entry->row_count ||
      row_hash != entry->row_hash) {
    return LC_ERR_INVALID;
  }
  {
    char *namespace_name;

    namespace_name = NULL;
    if (!lc_pouch_query_index_binary_string(&cursor, &namespace_name) ||
        namespace_name[0] == '\0') {
      return LC_ERR_INVALID;
    }
  }
  if (!lc_pouch_query_index_binary_ulong(&cursor, &term_count) ||
      !lc_pouch_query_index_binary_ulong(&cursor, &posting_count)) {
    return LC_ERR_INVALID;
  }
  if (term_count > (unsigned long)((size_t)-1 / sizeof(entry->terms[0])) ||
      posting_count >
          (unsigned long)((size_t)-1 / sizeof(entry->postings[0]))) {
    return LC_ERR_NOMEM;
  }
  if (term_count > 0UL) {
    entry->terms =
        (lc_pouch_query_index_generation_cache_term *)lc_alloc_with_allocator(
            allocator, (size_t)term_count * sizeof(entry->terms[0]));
    if (entry->terms == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch generation term cache",
                          NULL, NULL, NULL);
    }
    memset(entry->terms, 0, (size_t)term_count * sizeof(entry->terms[0]));
  }
  if (posting_count > 0UL) {
    entry->postings = (lc_pouch_query_index_generation_cache_posting *)
        lc_alloc_with_allocator(allocator, (size_t)posting_count *
                                               sizeof(entry->postings[0]));
    if (entry->postings == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch generation posting cache",
                          NULL, NULL, NULL);
    }
    memset(entry->postings, 0,
           (size_t)posting_count * sizeof(entry->postings[0]));
  }
  entry->term_count = (size_t)term_count;
  entry->posting_count = (size_t)posting_count;
  if (term_count > 0UL) {
    term_id_seen = (unsigned char *)lc_calloc_with_allocator(
        allocator, (size_t)term_count + 1U, sizeof(term_id_seen[0]));
    if (term_id_seen == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch generation term id set",
                          NULL, NULL, NULL);
    }
  }
  for (index = 0UL; index < term_count; ++index) {
    char *field_hex;
    char *value_hex;
    unsigned long term_id;
    unsigned char value_type;

    field_hex = NULL;
    value_hex = NULL;
    if (!lc_pouch_query_index_binary_ulong(&cursor, &term_id) ||
        !lc_pouch_query_index_binary_u8(&cursor, &value_type) ||
        !lc_pouch_query_index_binary_string(&cursor, &field_hex) ||
        !lc_pouch_query_index_binary_string(&cursor, &value_hex) ||
        term_id == 0UL ||
        (value_type != (unsigned char)'s' && value_type != (unsigned char)'n' &&
         value_type != (unsigned char)'b' && value_type != (unsigned char)'z' &&
         value_type != (unsigned char)LC_POUCH_QUERY_INDEX_EXACT_HASH_TYPE &&
         value_type != (unsigned char)LC_POUCH_QUERY_INDEX_TEXT_PREFIX_TYPE &&
         value_type != (unsigned char)LC_POUCH_QUERY_INDEX_TEXT_TOKEN_TYPE) ||
        !lc_pouch_query_index_hex_token_valid(field_hex) ||
        !lc_pouch_query_index_hex_token_valid(value_hex)) {
      rc = LC_ERR_INVALID;
      goto cleanup;
    }
    if (term_id > term_count || term_id_seen[term_id] != 0U) {
      rc = LC_ERR_INVALID;
      goto cleanup;
    }
    term_id_seen[term_id] = 1U;
    if (index > 0UL && lc_pouch_query_index_generation_cache_term_compare_parts(
                           entry->terms[index - 1UL].field_hex,
                           entry->terms[index - 1UL].value_hex,
                           entry->terms[index - 1UL].value_type, field_hex,
                           value_hex, (char)value_type) >= 0) {
      rc = LC_ERR_INVALID;
      goto cleanup;
    }
    entry->terms[index].field_hex = field_hex;
    entry->terms[index].value_hex = value_hex;
    entry->terms[index].value_type = (char)value_type;
    entry->terms[index].term_id = term_id;
    entry->terms[index].ordinal = index;
  }
  for (index = 0UL; index < posting_count; ++index) {
    const unsigned char *payload;
    unsigned long term_id;
    unsigned long count;
    unsigned long max_doc_id;
    unsigned long payload_length;
    unsigned char kind;

    payload = NULL;
    if (!lc_pouch_query_index_binary_ulong(&cursor, &term_id) ||
        !lc_pouch_query_index_binary_u8(&cursor, &kind) ||
        !lc_pouch_query_index_binary_ulong(&cursor, &count) ||
        !lc_pouch_query_index_binary_ulong(&cursor, &max_doc_id) ||
        !lc_pouch_query_index_binary_ulong(&cursor, &payload_length) ||
        !lc_pouch_query_index_binary_read(&cursor, (size_t)payload_length,
                                          &payload) ||
        term_id == 0UL ||
        (kind != (unsigned char)'s' && kind != (unsigned char)'d') ||
        (index > 0UL && entry->postings[index - 1UL].term_id >= term_id) ||
        term_id > term_count || term_id_seen[term_id] == 0U ||
        (count == 0UL && payload_length != 0UL) ||
        (count > 0UL && payload_length == 0UL)) {
      rc = LC_ERR_INVALID;
      goto cleanup;
    }
    entry->postings[index].term_id = term_id;
    entry->postings[index].kind = (char)kind;
    entry->postings[index].count = count;
    entry->postings[index].max_doc_id = max_doc_id;
    entry->postings[index].payload_length = payload_length;
    entry->postings[index].payload = payload;
  }
  if (cursor.offset != cursor.length) {
    rc = LC_ERR_INVALID;
    goto cleanup;
  }
cleanup:
  lc_free_with_allocator(allocator, term_id_seen);
  return rc;
}

static int lc_pouch_query_index_generation_cache_get(
    lc_pouch *pouch, const char *namespace_name, const char *path,
    lc_pouch_generation expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash,
    lc_pouch_query_index_generation_cache_entry **out, int *present, int *valid,
    lc_error *error) {
  lc_pouch_query_index_generation_cache_entry *entry;
  lc_pouch_query_index_generation_cache_entry *previous;
  char *bytes;
  size_t length;
  size_t component;
  int artifact_valid;
  int rc;

  if (pouch == NULL || path == NULL || out == NULL || present == NULL ||
      valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch generation cache lookup requires inputs", NULL,
                        NULL, NULL);
  }
  *out = NULL;
  *present = 0;
  *valid = 0;
  previous = NULL;
  entry = pouch->query_generation_cache;
  while (entry != NULL) {
    if (entry->index_seq == expected_index_seq &&
        entry->row_count == expected_row_count &&
        entry->row_hash == expected_row_hash &&
        strcmp(entry->path, path) == 0) {
      if (previous != NULL) {
        previous->next = entry->next;
        entry->next = pouch->query_generation_cache;
        pouch->query_generation_cache = entry;
      }
      *present = 1;
      *valid = 1;
      *out = entry;
      return LC_OK;
    }
    previous = entry;
    entry = entry->next;
  }

  bytes = NULL;
  length = 0U;
  artifact_valid = 1;
  component = lc_pouch_query_index_packed_component_for_path(path);
  if (component == LC_POUCH_QUERY_INDEX_PACKED_COMPONENT_COUNT ||
      component == LC_POUCH_QUERY_INDEX_PACKED_DOC_TABLE ||
      component == LC_POUCH_QUERY_INDEX_PACKED_DELETE) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index generation cache path has "
                        "unexpected component",
                        NULL, NULL, "pouch");
  }
  artifact_valid = 0;
  rc = lc_pouch_query_index_read_packed_component_bytes(
      pouch, namespace_name, path, component, &bytes, &length, present,
      &artifact_valid, error);
  if (rc != LC_OK || !*present || !artifact_valid) {
    lc_free_with_allocator(&pouch->allocator, bytes);
    *valid = artifact_valid;
    return rc;
  }

  entry =
      (lc_pouch_query_index_generation_cache_entry *)lc_alloc_with_allocator(
          &pouch->allocator, sizeof(*entry));
  if (entry == NULL) {
    lc_free_with_allocator(&pouch->allocator, bytes);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch generation cache entry", NULL,
                        NULL, NULL);
  }
  memset(entry, 0, sizeof(*entry));
  entry->path = lc_strdup_with_allocator(&pouch->allocator, path);
  entry->bytes = bytes;
  entry->length = length;
  entry->index_seq = expected_index_seq;
  entry->row_count = expected_row_count;
  entry->row_hash = expected_row_hash;
  if (entry->path == NULL) {
    lc_pouch_query_index_generation_cache_entry_cleanup(&pouch->allocator,
                                                        entry);
    lc_free_with_allocator(&pouch->allocator, entry);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch generation cache path", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_query_index_generation_cache_parse(&pouch->allocator, entry,
                                                   error);
  if (rc != LC_OK) {
    lc_pouch_query_index_generation_cache_entry_cleanup(&pouch->allocator,
                                                        entry);
    lc_free_with_allocator(&pouch->allocator, entry);
    *valid = 0;
    return rc == LC_ERR_INVALID ? LC_OK : rc;
  }
  entry->next = pouch->query_generation_cache;
  pouch->query_generation_cache = entry;
  ++pouch->query_generation_cache_count;
  while (pouch->query_generation_cache_count > 512U) {
    lc_pouch_query_index_generation_cache_entry *victim_prev;
    lc_pouch_query_index_generation_cache_entry *victim;

    victim_prev = NULL;
    victim = pouch->query_generation_cache;
    while (victim != NULL && victim->next != NULL) {
      victim_prev = victim;
      victim = victim->next;
    }
    if (victim == NULL || victim_prev == NULL) {
      break;
    }
    victim_prev->next = NULL;
    lc_pouch_query_index_generation_cache_entry_cleanup(&pouch->allocator,
                                                        victim);
    lc_free_with_allocator(&pouch->allocator, victim);
    --pouch->query_generation_cache_count;
  }
  *valid = 1;
  *out = entry;
  return LC_OK;
}

static int lc_pouch_query_index_doc_table_cache_get(
    lc_pouch *pouch, const char *namespace_name, const char *path,
    lc_pouch_generation expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash,
    lc_pouch_query_index_doc_table_cache_entry **out, int *present, int *valid,
    lc_error *error) {
  lc_pouch_query_index_doc_table_cache_entry *entry;
  lc_pouch_query_index_doc_table_cache_entry *previous;
  size_t index;
  int rc;

  if (pouch == NULL || path == NULL || out == NULL || present == NULL ||
      valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch doc-table cache lookup requires inputs", NULL,
                        NULL, NULL);
  }
  *out = NULL;
  *present = 0;
  *valid = 0;
  previous = NULL;
  entry = pouch->query_doc_table_cache;
  while (entry != NULL) {
    if (entry->index_seq == expected_index_seq &&
        entry->row_count == expected_row_count &&
        entry->row_hash == expected_row_hash &&
        strcmp(entry->path, path) == 0) {
      if (previous != NULL) {
        previous->next = entry->next;
        entry->next = pouch->query_doc_table_cache;
        pouch->query_doc_table_cache = entry;
      }
      *present = 1;
      *valid = 1;
      *out = entry;
      return LC_OK;
    }
    previous = entry;
    entry = entry->next;
  }

  entry = (lc_pouch_query_index_doc_table_cache_entry *)lc_alloc_with_allocator(
      &pouch->allocator, sizeof(*entry));
  if (entry == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch doc-table cache entry", NULL,
                        NULL, NULL);
  }
  memset(entry, 0, sizeof(*entry));
  entry->path = lc_strdup_with_allocator(&pouch->allocator, path);
  entry->index_seq = expected_index_seq;
  entry->row_count = expected_row_count;
  entry->row_hash = expected_row_hash;
  if (entry->path == NULL) {
    lc_pouch_query_index_doc_table_cache_entry_cleanup(&pouch->allocator,
                                                       entry);
    lc_free_with_allocator(&pouch->allocator, entry);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch doc-table cache path", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_query_index_doc_table_generation_load_artifact(
      pouch, namespace_name, path, expected_index_seq, expected_row_count,
      expected_row_hash, &entry->table, present, valid, error);
  if (rc != LC_OK || !*present || !*valid) {
    lc_pouch_query_index_doc_table_cache_entry_cleanup(&pouch->allocator,
                                                       entry);
    lc_free_with_allocator(&pouch->allocator, entry);
    return rc;
  }
  if (entry->table.count > 0U) {
    entry->decoded_keys = (char **)lc_alloc_with_allocator(
        &pouch->allocator, entry->table.count * sizeof(entry->decoded_keys[0]));
    if (entry->decoded_keys == NULL) {
      lc_pouch_query_index_doc_table_cache_entry_cleanup(&pouch->allocator,
                                                         entry);
      lc_free_with_allocator(&pouch->allocator, entry);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch doc-table decoded keys",
                          NULL, NULL, NULL);
    }
    memset(entry->decoded_keys, 0,
           entry->table.count * sizeof(entry->decoded_keys[0]));
    entry->decoded_key_count = entry->table.count;
  }
  for (index = 0U; index < entry->table.count; ++index) {
    entry->decoded_keys[index] = lc_pouch_query_index_hex_decode(
        &pouch->allocator, entry->table.items[index].key_hex, error);
    if (entry->decoded_keys[index] == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      lc_pouch_query_index_doc_table_cache_entry_cleanup(&pouch->allocator,
                                                         entry);
      lc_free_with_allocator(&pouch->allocator, entry);
      return rc;
    }
  }
  entry->next = pouch->query_doc_table_cache;
  pouch->query_doc_table_cache = entry;
  ++pouch->query_doc_table_cache_count;
  while (pouch->query_doc_table_cache_count > 64U) {
    lc_pouch_query_index_doc_table_cache_entry *victim_prev;
    lc_pouch_query_index_doc_table_cache_entry *victim;

    victim_prev = NULL;
    victim = pouch->query_doc_table_cache;
    while (victim != NULL && victim->next != NULL) {
      victim_prev = victim;
      victim = victim->next;
    }
    if (victim == NULL || victim_prev == NULL) {
      break;
    }
    victim_prev->next = NULL;
    lc_pouch_query_index_doc_table_cache_entry_cleanup(&pouch->allocator,
                                                       victim);
    lc_free_with_allocator(&pouch->allocator, victim);
    --pouch->query_doc_table_cache_count;
  }
  *out = entry;
  return LC_OK;
}

static int lc_pouch_query_index_warm_generation_artifact(
    lc_pouch *pouch, const char *namespace_name, const char *path,
    lc_pouch_generation index_seq, unsigned long row_count,
    unsigned long row_hash, lc_error *error) {
  lc_pouch_query_index_generation_cache_entry *entry;
  int present;
  int valid;
  int rc;

  entry = NULL;
  present = 0;
  valid = 0;
  rc = lc_pouch_query_index_generation_cache_get(
      pouch, namespace_name, path, index_seq, row_count, row_hash, &entry,
      &present, &valid, error);
  if (rc == LC_OK && (!present || !valid)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index generation artifact is not readable",
                      NULL, NULL, "pouch");
  }
  return rc;
}

int lc_pouch_query_index_warm_namespace(lc_pouch *pouch,
                                        const char *namespace_name,
                                        lc_error *error) {
  lc_pouch_query_index_manifest manifest;
  char *manifest_path;
  size_t segment_index;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index warm requires pouch and namespace",
                        NULL, NULL, NULL);
  }
  memset(&manifest, 0, sizeof(manifest));
  manifest_path =
      lc_pouch_query_index_manifest_path(pouch, namespace_name, error);
  if (manifest_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_query_index_manifest_read(pouch, manifest_path, &manifest,
                                          error);
  lc_free_with_allocator(&pouch->allocator, manifest_path);
  if (rc != LC_OK) {
    goto cleanup;
  }
  if (!manifest.present) {
    goto cleanup;
  }
  if (!manifest.valid) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index manifest is not readable", NULL, NULL,
                      "pouch");
    goto cleanup;
  }
  for (segment_index = 0U;
       rc == LC_OK && segment_index < manifest.segment_count; ++segment_index) {
    const lc_pouch_query_index_manifest_segment *segment;
    lc_pouch_query_index_doc_table_cache_entry *doc_entry;
    lc_pouch_query_index_read_result header;
    char *header_path;
    char *doc_table_path;
    char *exact_term_path;
    char *presence_term_path;
    char *range_term_path;
    char *text_term_path;
    char *trigram_term_path;
    char *temporal_term_path;
    char *delete_path;
    lc_pouch_query_index_key_hex_set deletes;
    unsigned long delete_count;
    unsigned long delete_hash;
    int present;
    int valid;

    segment = &manifest.segments[segment_index];
    doc_entry = NULL;
    memset(&header, 0, sizeof(header));
    header_path = NULL;
    doc_table_path = NULL;
    exact_term_path = NULL;
    presence_term_path = NULL;
    range_term_path = NULL;
    text_term_path = NULL;
    trigram_term_path = NULL;
    temporal_term_path = NULL;
    delete_path = NULL;
    memset(&deletes, 0, sizeof(deletes));
    delete_count = 0UL;
    delete_hash = lc_pouch_query_index_hash_init();
    rc = lc_pouch_query_index_segmented_paths(
        pouch, namespace_name, segment->id, &header_path, &doc_table_path,
        &exact_term_path, &presence_term_path, &range_term_path,
        &text_term_path, &trigram_term_path, &temporal_term_path, &delete_path,
        error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_read_header(pouch, namespace_name, header_path,
                                            &header, error);
    }
    if (rc == LC_OK && (!header.present || !header.valid ||
                        header.index_seq != segment->index_seq ||
                        header.row_count != segment->row_count ||
                        header.row_hash != segment->row_hash)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index segment header is not readable",
                        NULL, NULL, "pouch");
    }
    if (rc == LC_OK) {
      lc_pouch_query_index_artifact_cache_remember(
          pouch, header_path, LC_POUCH_QUERY_INDEX_ARTIFACT_HEADER,
          segment->index_seq, segment->row_count, segment->row_hash);
    }
    present = 0;
    valid = 0;
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_doc_table_cache_get(
          pouch, namespace_name, doc_table_path, segment->index_seq,
          segment->row_count, segment->row_hash, &doc_entry, &present, &valid,
          error);
    }
    if (rc == LC_OK && (!present || !valid)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index segment doc table is not readable",
                        NULL, NULL, "pouch");
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_warm_generation_artifact(
          pouch, namespace_name, exact_term_path, segment->index_seq,
          segment->row_count, segment->row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_warm_generation_artifact(
          pouch, namespace_name, presence_term_path, segment->index_seq,
          segment->row_count, segment->row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_warm_generation_artifact(
          pouch, namespace_name, range_term_path, segment->index_seq,
          segment->row_count, segment->row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_warm_generation_artifact(
          pouch, namespace_name, text_term_path, segment->index_seq,
          segment->row_count, segment->row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_warm_generation_artifact(
          pouch, namespace_name, trigram_term_path, segment->index_seq,
          segment->row_count, segment->row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_warm_generation_artifact(
          pouch, namespace_name, temporal_term_path, segment->index_seq,
          segment->row_count, segment->row_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_load_delete_keys(
          pouch, namespace_name, delete_path, &deletes, &delete_count,
          &delete_hash, error);
    }
    if (rc == LC_OK && (delete_count != segment->delete_count ||
                        delete_hash != segment->delete_hash)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index segment deletes do not match the "
                        "manifest",
                        NULL, NULL, "pouch");
    }
    lc_pouch_query_index_key_hex_set_cleanup(&pouch->allocator, &deletes);
    lc_pouch_query_index_segmented_paths_cleanup(
        pouch, &header_path, &doc_table_path, &exact_term_path,
        &presence_term_path, &range_term_path, &text_term_path,
        &trigram_term_path, &temporal_term_path, &delete_path);
  }
  if (rc == LC_OK) {
    lc_pouch_query_index_manifest_trust_remember(pouch, namespace_name,
                                                 manifest.index_seq, &manifest);
  }

cleanup:
  lc_pouch_query_index_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

static int lc_pouch_query_index_generation_cache_append_posting(
    const lc_allocator *allocator,
    const lc_pouch_query_index_generation_cache_entry *entry,
    unsigned long term_id, size_t value_index,
    const lc_pouch_index_result_docid_list *docid_filter_sorted,
    lc_pouch_index_result_docid_list *docids, lc_error *error) {
  const lc_pouch_query_index_generation_cache_posting *posting;

  if (!lc_pouch_query_index_generation_cache_find_posting(entry, term_id,
                                                          &posting)) {
    return LC_OK;
  }
  return lc_pouch_query_index_append_generation_posting(
      allocator, posting->kind, posting->count, posting->max_doc_id,
      posting->payload_length, posting->payload, value_index,
      docid_filter_sorted, docids, error);
}

static int lc_pouch_query_index_read_sparse_posting_varint(
    const unsigned char *bytes, size_t length, size_t *offset,
    unsigned long *out, lc_error *error) {
  unsigned long value;
  unsigned int shift;

  if ((bytes == NULL && length > 0U) || offset == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch posting varint read requires inputs", NULL, NULL,
                        NULL);
  }
  value = 0UL;
  shift = 0U;
  while (*offset < length) {
    unsigned char byte;

    byte = bytes[*offset];
    ++*offset;
    if (shift >= (unsigned int)(sizeof(unsigned long) * CHAR_BIT)) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch posting varint is too large", NULL, NULL,
                          "pouch");
    }
    value |= ((unsigned long)(byte & 0x7FU)) << shift;
    if ((byte & 0x80U) == 0U) {
      *out = value;
      return LC_OK;
    }
    shift += 7U;
  }
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch posting varint is truncated", NULL, NULL, "pouch");
}

static int lc_pouch_query_index_docid_list_contains_sorted(
    const lc_pouch_index_result_docid_list *list, unsigned long doc_id) {
  size_t left;
  size_t right;

  if (list == NULL) {
    return 1;
  }
  left = 0U;
  right = list->count;
  while (left < right) {
    size_t middle;
    unsigned long candidate;

    middle = left + ((right - left) / 2U);
    candidate = list->items[middle].doc_id;
    if (candidate == doc_id) {
      return 1;
    }
    if (candidate < doc_id) {
      left = middle + 1U;
    } else {
      right = middle;
    }
  }
  return 0;
}

static int lc_pouch_query_index_docid_list_intersect_sorted(
    const lc_allocator *allocator, const lc_pouch_index_result_docid_list *left,
    const lc_pouch_index_result_docid_list *right,
    lc_pouch_index_result_docid_list *out, lc_error *error) {
  size_t left_index;
  size_t right_index;
  int rc;

  if (left == NULL || right == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch docID intersection requires inputs", NULL, NULL,
                        NULL);
  }
  left_index = 0U;
  right_index = 0U;
  rc = LC_OK;
  while (rc == LC_OK && left_index < left->count &&
         right_index < right->count) {
    unsigned long left_doc_id;
    unsigned long right_doc_id;

    left_doc_id = left->items[left_index].doc_id;
    right_doc_id = right->items[right_index].doc_id;
    if (left_doc_id == right_doc_id) {
      rc = lc_pouch_index_result_docid_list_add(
          allocator, out, left_doc_id, left->items[left_index].value_index,
          error);
      ++left_index;
      ++right_index;
    } else if (left_doc_id < right_doc_id) {
      ++left_index;
    } else {
      ++right_index;
    }
  }
  return rc;
}

static int lc_pouch_query_index_collect_trigram_docids(
    lc_pouch *pouch, const lc_pouch_query_index_generation_cache_entry *entry,
    const char *field_hex, const char *needle_hex,
    lc_pouch_index_result_docid_list *docids, lc_error *error) {
  size_t index;
  int rc;

  if (pouch == NULL || entry == NULL || field_hex == NULL ||
      needle_hex == NULL || docids == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch trigram collection requires inputs", NULL, NULL,
                        NULL);
  }
  rc = LC_OK;
  if (!lc_pouch_query_index_field_hex_is_any_text(field_hex)) {
    const lc_pouch_query_index_generation_cache_term *term;

    if (lc_pouch_query_index_generation_cache_find_term(
            entry, field_hex, needle_hex, 's', &term)) {
      rc = lc_pouch_query_index_generation_cache_append_posting(
          &pouch->allocator, entry, term->term_id, 0U, NULL, docids, error);
    }
    return rc;
  }

  {
    const lc_pouch_query_index_generation_cache_term *term;

    if (lc_pouch_query_index_generation_cache_find_term(
            entry, LC_POUCH_QUERY_INDEX_ANY_TEXT_FIELD_HEX, needle_hex, 's',
            &term)) {
      return lc_pouch_query_index_generation_cache_append_posting(
          &pouch->allocator, entry, term->term_id, 0U, NULL, docids, error);
    }
  }

  for (index = 0U; rc == LC_OK && index < entry->term_count; ++index) {
    const lc_pouch_query_index_generation_cache_term *term;

    term = &entry->terms[index];
    if (term->value_type == 's' && strcmp(term->value_hex, needle_hex) == 0) {
      rc = lc_pouch_query_index_generation_cache_append_posting(
          &pouch->allocator, entry, term->term_id, 0U, NULL, docids, error);
    }
  }
  return rc;
}

static int lc_pouch_query_index_collect_trigram_intersection_docids(
    lc_pouch *pouch, const char *namespace_name, const char *path,
    lc_pouch_generation expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, const char *field_hex,
    const char trigrams[][7], size_t trigram_count,
    lc_pouch_index_result_docid_list *docids, int *present, int *valid,
    lc_error *error) {
  lc_pouch_query_index_generation_cache_entry *entry;
  lc_pouch_index_result_docid_list intersection;
  size_t trigram_index;
  int has_intersection;
  int rc;

  if (pouch == NULL || field_hex == NULL || trigrams == NULL ||
      trigram_count == 0U || docids == NULL || present == NULL ||
      valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch trigram intersection requires inputs", NULL,
                        NULL, NULL);
  }
  *present = 0;
  *valid = 0;
  entry = NULL;
  rc = lc_pouch_query_index_generation_cache_get(
      pouch, namespace_name, path, expected_index_seq, expected_row_count,
      expected_row_hash, &entry, present, valid, error);
  if (rc != LC_OK || !*present || !*valid) {
    return rc;
  }

  memset(&intersection, 0, sizeof(intersection));
  has_intersection = 0;
  for (trigram_index = 0U; rc == LC_OK && trigram_index < trigram_count;
       ++trigram_index) {
    lc_pouch_index_result_docid_list postings;

    memset(&postings, 0, sizeof(postings));
    rc = lc_pouch_query_index_collect_trigram_docids(
        pouch, entry, field_hex, trigrams[trigram_index], &postings, error);
    if (rc == LC_OK) {
      rc = lc_pouch_index_result_docid_list_sort_compact(&pouch->allocator,
                                                         &postings, error);
    }
    if (rc == LC_OK && !has_intersection) {
      intersection = postings;
      memset(&postings, 0, sizeof(postings));
      has_intersection = 1;
    } else if (rc == LC_OK) {
      lc_pouch_index_result_docid_list next;

      memset(&next, 0, sizeof(next));
      rc = lc_pouch_query_index_docid_list_intersect_sorted(
          &pouch->allocator, &intersection, &postings, &next, error);
      lc_pouch_index_result_docid_list_cleanup(&pouch->allocator,
                                               &intersection);
      intersection = next;
    }
    lc_pouch_index_result_docid_list_cleanup(&pouch->allocator, &postings);
    if (rc == LC_OK && has_intersection && intersection.count == 0U) {
      break;
    }
  }
  if (rc == LC_OK && has_intersection) {
    *docids = intersection;
    memset(&intersection, 0, sizeof(intersection));
  }
  lc_pouch_index_result_docid_list_cleanup(&pouch->allocator, &intersection);
  return rc;
}

static int lc_pouch_query_index_append_generation_posting(
    const lc_allocator *allocator, char kind, unsigned long count,
    unsigned long max_doc_id, unsigned long payload_length,
    const unsigned char *payload_bytes, size_t value_index,
    const lc_pouch_index_result_docid_list *docid_filter_sorted,
    lc_pouch_index_result_docid_list *docids, lc_error *error) {
  size_t index;
  size_t offset;
  size_t seen;
  unsigned long doc_id;
  unsigned long delta;
  int rc;

  (void)allocator;
  if ((payload_bytes == NULL && payload_length > 0UL) ||
      (count == 0UL && payload_length != 0UL) ||
      (count > 0UL && payload_length == 0UL)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch posting count and payload mismatch", NULL, NULL,
                        "pouch");
  }
  if (payload_length > (unsigned long)((size_t)-1)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch posting payload exceeds local limit", NULL, NULL,
                        "pouch");
  }
  rc = LC_OK;
  if (kind == 's') {
    doc_id = 0UL;
    delta = 0UL;
    offset = 0U;
    for (index = 0U; rc == LC_OK && index < (size_t)count; ++index) {
      rc = lc_pouch_query_index_read_sparse_posting_varint(
          payload_bytes, (size_t)payload_length, &offset, &delta, error);
      if (rc != LC_OK) {
        break;
      }
      if (index > 0U && delta > ULONG_MAX - doc_id) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch posting docID delta overflows", NULL, NULL,
                          "pouch");
        break;
      }
      doc_id = index == 0U ? delta : doc_id + delta;
      if (lc_pouch_query_index_docid_list_contains_sorted(docid_filter_sorted,
                                                          doc_id)) {
        rc = lc_pouch_index_result_docid_list_add(allocator, docids, doc_id,
                                                  value_index, error);
      }
    }
    if (rc == LC_OK && offset != (size_t)payload_length) {
      rc =
          lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch posting has trailing bytes", NULL, NULL, "pouch");
    }
    if (rc == LC_OK && count > 0UL && doc_id != max_doc_id) {
      rc =
          lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch posting max docID mismatch", NULL, NULL, "pouch");
    }
  } else if (kind == 'd') {
    seen = 0U;
    for (index = 0U; rc == LC_OK && index < (size_t)payload_length; ++index) {
      unsigned int bit_index;

      for (bit_index = 0U; bit_index < (unsigned int)CHAR_BIT; ++bit_index) {
        if ((payload_bytes[index] & (unsigned char)(1U << bit_index)) == 0U) {
          continue;
        }
        doc_id = ((unsigned long)index * (unsigned long)CHAR_BIT) +
                 (unsigned long)bit_index;
        if (doc_id > max_doc_id) {
          rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch posting dense bit exceeds max docID", NULL,
                            NULL, "pouch");
          break;
        }
        if (lc_pouch_query_index_docid_list_contains_sorted(docid_filter_sorted,
                                                            doc_id)) {
          rc = lc_pouch_index_result_docid_list_add(allocator, docids, doc_id,
                                                    value_index, error);
          if (rc != LC_OK) {
            break;
          }
        }
        ++seen;
      }
    }
    if (rc == LC_OK && seen != (size_t)count) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch posting count mismatch", NULL, NULL, "pouch");
    }
  } else {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch posting has invalid kind", NULL, NULL, "pouch");
  }
  return rc;
}

static int lc_pouch_query_index_generation_term_matches(
    const lc_allocator *allocator,
    lc_pouch_query_index_segmented_collect_kind kind,
    const lc_pouch_index_term_key *exact_terms, size_t exact_term_count,
    const char *field_hex, const char *needle_hex, const char *needle_text,
    int prefix_match, int contains_match, int ignore_case,
    const lc_pouch_query_index_range_bounds *range_bounds,
    const lc_pouch_index_parsed_date_bounds *temporal_bounds,
    unsigned long term_ordinal, const char *term_field_hex,
    const char *term_value_hex, char term_value_type, int *matched,
    size_t *value_index, lc_error *error) {
  size_t index;

  *matched = 0;
  *value_index = (size_t)term_ordinal;
  if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_EXACT) {
    for (index = 0U; index < exact_term_count; ++index) {
      if (strcmp(term_field_hex, exact_terms[index].field_hex) == 0 &&
          strcmp(term_value_hex, exact_terms[index].value_hex) == 0 &&
          term_value_type == exact_terms[index].value_type) {
        *matched = 1;
        *value_index = index;
        break;
      }
    }
  } else if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_PRESENCE) {
    *matched = strcmp(term_field_hex, field_hex) == 0 &&
               strcmp(term_value_hex, "-") == 0 && term_value_type == 'z';
    *value_index = 0U;
  } else if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TRIGRAM) {
    *matched = (lc_pouch_query_index_field_hex_is_any_text(field_hex) ||
                strcmp(term_field_hex, field_hex) == 0) &&
               term_value_type == 's' &&
               strcmp(term_value_hex, needle_hex) == 0;
    *value_index = 0U;
  } else if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_RANGE ||
             kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TEMPORAL) {
    if (strcmp(term_field_hex, field_hex) == 0 && term_value_type == 'n' &&
        strcmp(term_value_hex, "-") != 0) {
      char *value_text;
      double number;

      value_text =
          lc_pouch_query_index_hex_decode(allocator, term_value_hex, error);
      if (value_text == NULL) {
        return error != NULL && error->code != LC_OK ? error->code
                                                     : LC_ERR_NOMEM;
      }
      if (lc_pouch_query_index_parse_number_value(value_text, &number)) {
        if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_RANGE) {
          *matched =
              lc_pouch_query_index_range_contains_value(range_bounds, number);
        } else {
          lc_pouch_index_instant instant;

          instant.seconds = number;
          *matched =
              lc_pouch_index_date_contains_value(temporal_bounds, &instant);
        }
      }
      lc_free_with_allocator(allocator, value_text);
    }
  } else if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT) {
    if ((lc_pouch_query_index_field_hex_is_any_text(field_hex) ||
         strcmp(term_field_hex, field_hex) == 0) &&
        strcmp(term_value_hex, "-") != 0) {
      if (prefix_match) {
        if (term_value_type == 's' ||
            term_value_type == LC_POUCH_QUERY_INDEX_TEXT_PREFIX_TYPE) {
          *matched = ignore_case ? lc_pouch_query_index_hex_text_has_prefix(
                                       term_value_hex, needle_text, 1)
                                 : strncmp(term_value_hex, needle_hex,
                                           strlen(needle_hex)) == 0;
        }
      } else if (contains_match) {
        if (term_value_type == 's') {
          *matched = ignore_case ? lc_pouch_query_index_hex_text_contains(
                                       term_value_hex, needle_text, 1)
                                 : lc_pouch_query_index_hex_contains_aligned(
                                       term_value_hex, needle_hex);
        }
      }
    }
  }
  return LC_OK;
}

static int lc_pouch_query_index_collect_generation_artifact_docids(
    lc_pouch *pouch, const char *namespace_name, const char *path,
    lc_pouch_generation expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash,
    lc_pouch_query_index_segmented_collect_kind kind,
    const lc_pouch_index_term_key *exact_terms, size_t exact_term_count,
    const char *field_hex, const char *needle_hex, const char *needle_text,
    int prefix_match, int contains_match, int ignore_case,
    const lc_pouch_query_index_range_bounds *range_bounds,
    const lc_pouch_index_parsed_date_bounds *temporal_bounds,
    const lc_pouch_index_result_docid_list *docid_filter_sorted,
    lc_pouch_index_result_docid_list *docids, int *present, int *valid,
    int *text_prefix_seen, lc_error *error) {
  lc_pouch_query_index_generation_cache_entry *entry;
  size_t index;
  size_t start;
  int any_text_direct;
  int any_text_lookup;
  int rc;

  *present = 0;
  *valid = 0;
  entry = NULL;
  rc = lc_pouch_query_index_generation_cache_get(
      pouch, namespace_name, path, expected_index_seq, expected_row_count,
      expected_row_hash, &entry, present, valid, error);
  if (rc != LC_OK || !*present || !*valid) {
    return rc;
  }

  if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_EXACT) {
    for (index = 0U; rc == LC_OK && index < exact_term_count; ++index) {
      const lc_pouch_query_index_generation_cache_term *term;

      if (lc_pouch_query_index_generation_cache_find_term(
              entry, exact_terms[index].field_hex, exact_terms[index].value_hex,
              exact_terms[index].value_type, &term)) {
        rc = lc_pouch_query_index_generation_cache_append_posting(
            &pouch->allocator, entry, term->term_id, index, docid_filter_sorted,
            docids, error);
      }
    }
    return rc;
  }

  if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_PRESENCE) {
    const lc_pouch_query_index_generation_cache_term *term;

    if (lc_pouch_query_index_generation_cache_find_term(entry, field_hex, "-",
                                                        'z', &term)) {
      rc = lc_pouch_query_index_generation_cache_append_posting(
          &pouch->allocator, entry, term->term_id, 0U, docid_filter_sorted,
          docids, error);
    }
    return rc;
  }

  if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT_TOKEN &&
      lc_pouch_query_index_field_hex_is_any_text(field_hex)) {
    const lc_pouch_query_index_generation_cache_term *term;

    if (lc_pouch_query_index_generation_cache_find_term(
            entry, LC_POUCH_QUERY_INDEX_ANY_TEXT_FIELD_HEX, needle_hex,
            LC_POUCH_QUERY_INDEX_TEXT_TOKEN_TYPE, &term)) {
      rc = lc_pouch_query_index_generation_cache_append_posting(
          &pouch->allocator, entry, term->term_id, 0U, docid_filter_sorted,
          docids, error);
    }
    return rc;
  }

  any_text_lookup = (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT ||
                     kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TRIGRAM) &&
                    lc_pouch_query_index_field_hex_is_any_text(field_hex);
  any_text_direct = 0;
  if (any_text_lookup && kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT &&
      !contains_match) {
    start = lc_pouch_query_index_generation_cache_field_lower_bound(entry,
                                                                    field_hex);
    any_text_direct = start < entry->term_count &&
                      strcmp(entry->terms[start].field_hex, field_hex) == 0;
    if (!any_text_direct) {
      start = 0U;
    }
  } else if (any_text_lookup) {
    start = 0U;
  } else {
    start = lc_pouch_query_index_generation_cache_field_lower_bound(entry,
                                                                    field_hex);
  }
  for (index = start; rc == LC_OK && index < entry->term_count; ++index) {
    const lc_pouch_query_index_generation_cache_term *term;
    size_t value_index;
    int matched;

    term = &entry->terms[index];
    if ((any_text_direct || !any_text_lookup) &&
        strcmp(term->field_hex, field_hex) != 0) {
      break;
    }
    if (text_prefix_seen != NULL &&
        kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT &&
        term->value_type == LC_POUCH_QUERY_INDEX_TEXT_PREFIX_TYPE &&
        (lc_pouch_query_index_field_hex_is_any_text(field_hex) ||
         strcmp(term->field_hex, field_hex) == 0)) {
      *text_prefix_seen = 1;
    }
    value_index = (size_t)term->ordinal;
    matched = 0;
    rc = lc_pouch_query_index_generation_term_matches(
        &pouch->allocator, kind, exact_terms, exact_term_count, field_hex,
        needle_hex, needle_text, prefix_match, contains_match, ignore_case,
        range_bounds, temporal_bounds, term->ordinal, term->field_hex,
        term->value_hex, term->value_type, &matched, &value_index, error);
    if (rc == LC_OK && matched) {
      rc = lc_pouch_query_index_generation_cache_append_posting(
          &pouch->allocator, entry, term->term_id, value_index,
          docid_filter_sorted, docids, error);
    }
  }
  return rc;
}

static int lc_pouch_query_index_result_row_list_add_view(
    const lc_allocator *allocator, lc_pouch_index_result_row_list *rows,
    char *key, char *key_hex, int owns_key, int owns_key_hex,
    unsigned long doc_id, lc_pouch_generation version, uint64_t bytes,
    int has_query_hidden, int query_hidden, size_t value_index,
    lc_error *error) {
  lc_pouch_index_result_row *row;
  int rc;

  if (rows == NULL || key == NULL || key[0] == '\0') {
    return LC_OK;
  }
  rc = lc_pouch_query_index_result_rows_reserve(allocator, rows,
                                                rows->count + 1U, error);
  if (rc != LC_OK) {
    return rc;
  }
  row = &rows->items[rows->count++];
  memset(row, 0, sizeof(*row));
  row->key = key;
  row->key_hex = key_hex;
  row->doc_id = doc_id;
  row->version = version;
  row->bytes = bytes;
  row->has_query_hidden = has_query_hidden ? 1 : 0;
  row->query_hidden = query_hidden ? 1 : 0;
  row->owns_key = owns_key ? 1 : 0;
  row->owns_key_hex = owns_key_hex ? 1 : 0;
  row->value_index = value_index;
  return LC_OK;
}

static int lc_pouch_query_index_docid_rows_build(
    const lc_allocator *allocator, lc_pouch_index_result_docid_list *docids,
    const lc_pouch_index_doc_table *doc_table, char *const *decoded_keys,
    lc_pouch_index_result_row_list *rows, lc_error *error) {
  const lc_pouch_index_doc *doc;
  char *key;
  size_t index;
  int rc;

  if (docids == NULL || doc_table == NULL || rows == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index docID row build requires docIDs, "
                        "doc table, and rows",
                        NULL, NULL, NULL);
  }
  if (docids->count == 0U) {
    return LC_OK;
  }
  rc = lc_pouch_index_result_docid_list_sort_compact(allocator, docids, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (rows->count == 0U) {
    rows->items = (lc_pouch_index_result_row *)lc_alloc_with_allocator(
        allocator, docids->count * sizeof(rows->items[0]));
    if (rows->items == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch query-index result rows",
                          NULL, NULL, NULL);
    }
    memset(rows->items, 0, docids->count * sizeof(rows->items[0]));
    rows->capacity = docids->count;
  }
  for (index = 0U; rc == LC_OK && index < docids->count; ++index) {
    int key_owned;

    doc = NULL;
    rc = lc_pouch_index_doc_table_get(doc_table, docids->items[index].doc_id,
                                      &doc, error);
    if (rc != LC_OK) {
      break;
    }
    key = NULL;
    if (decoded_keys != NULL &&
        docids->items[index].doc_id < doc_table->count) {
      key = decoded_keys[docids->items[index].doc_id];
    }
    key_owned = 0;
    if (key == NULL) {
      key = lc_pouch_query_index_hex_decode(allocator, doc->key_hex, error);
      if (key == NULL) {
        rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
        break;
      }
      key_owned = 1;
    }
    rc = lc_pouch_query_index_result_row_list_add_view(
        allocator, rows, key, (char *)doc->key_hex, key_owned, 0,
        docids->items[index].doc_id, doc->version, doc->bytes,
        doc->has_query_hidden, doc->query_hidden,
        docids->items[index].value_index, error);
    if (rc != LC_OK && key_owned) {
      lc_free_with_allocator(allocator, key);
    }
  }
  return rc;
}

static int lc_pouch_query_index_field_hex_is_any_text(const char *field_hex) {
  return field_hex != NULL &&
         strcmp(field_hex, LC_POUCH_QUERY_INDEX_ANY_TEXT_FIELD_HEX) == 0;
}

static int lc_pouch_query_index_result_row_compare_key(const void *left,
                                                       const void *right) {
  const lc_pouch_index_result_row *left_row;
  const lc_pouch_index_result_row *right_row;
  int cmp;

  left_row = (const lc_pouch_index_result_row *)left;
  right_row = (const lc_pouch_index_result_row *)right;
  if (left_row->key_hex == NULL && right_row->key_hex == NULL) {
    cmp = 0;
  } else if (left_row->key_hex == NULL) {
    cmp = -1;
  } else if (right_row->key_hex == NULL) {
    cmp = 1;
  } else {
    cmp = strcmp(left_row->key_hex, right_row->key_hex);
  }
  if (cmp != 0) {
    return cmp;
  }
  if (left_row->value_index < right_row->value_index) {
    return -1;
  }
  if (left_row->value_index > right_row->value_index) {
    return 1;
  }
  if (left_row->doc_id < right_row->doc_id) {
    return -1;
  }
  if (left_row->doc_id > right_row->doc_id) {
    return 1;
  }
  return 0;
}

static void
lc_pouch_query_index_result_rows_sort(const lc_allocator *allocator,
                                      lc_pouch_index_result_row_list *rows) {
  size_t index;
  size_t write_index;

  if (rows == NULL || rows->count <= 1U) {
    return;
  }
  qsort(rows->items, rows->count, sizeof(rows->items[0]),
        lc_pouch_query_index_result_row_compare_key);
  write_index = 0U;
  for (index = 0U; index < rows->count; ++index) {
    if (write_index > 0U && rows->items[index].key_hex != NULL &&
        rows->items[write_index - 1U].key_hex != NULL &&
        strcmp(rows->items[index].key_hex,
               rows->items[write_index - 1U].key_hex) == 0) {
      if (rows->items[index].owns_key) {
        lc_free_with_allocator(allocator, rows->items[index].key);
      }
      if (rows->items[index].owns_key_hex) {
        lc_free_with_allocator(allocator, rows->items[index].key_hex);
      }
      memset(&rows->items[index], 0, sizeof(rows->items[index]));
      continue;
    }
    if (write_index != index) {
      rows->items[write_index] = rows->items[index];
      memset(&rows->items[index], 0, sizeof(rows->items[index]));
    }
    ++write_index;
  }
  rows->count = write_index;
}

static int
lc_pouch_query_index_result_rows_reserve(const lc_allocator *allocator,
                                         lc_pouch_index_result_row_list *rows,
                                         size_t needed, lc_error *error) {
  lc_pouch_index_result_row *next_items;
  size_t next_capacity;

  if (rows == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index result row reserve requires rows",
                        NULL, NULL, NULL);
  }
  if (needed <= rows->capacity) {
    return LC_OK;
  }
  next_capacity = rows->capacity == 0U ? 16U : rows->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch query-index result rows exceed local limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_items = (lc_pouch_index_result_row *)lc_alloc_with_allocator(
      allocator, next_capacity * sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index result rows",
                        NULL, NULL, NULL);
  }
  if (rows->items != NULL) {
    memcpy(next_items, rows->items, rows->count * sizeof(*next_items));
    lc_free_with_allocator(allocator, rows->items);
  }
  memset(next_items + rows->count, 0,
         (next_capacity - rows->count) * sizeof(*next_items));
  rows->items = next_items;
  rows->capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_query_index_segmented_rows_move_visible(
    const lc_allocator *allocator, lc_pouch_index_result_row_list *rows,
    lc_pouch_index_result_row_list *segment_rows,
    const lc_pouch_query_index_key_hex_set *hidden, lc_error *error) {
  size_t index;
  int rc;

  if (rows == NULL || segment_rows == NULL || hidden == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch segmented row merge requires output rows, "
                        "segment rows, and hidden set",
                        NULL, NULL, NULL);
  }
  rc = LC_OK;
  if (segment_rows->count > ((size_t)-1 - rows->count)) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch segmented row merge exceeds local limit", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_query_index_result_rows_reserve(
      allocator, rows, rows->count + segment_rows->count, error);
  for (index = 0U; rc == LC_OK && index < segment_rows->count; ++index) {
    lc_pouch_index_result_row *row;

    row = &segment_rows->items[index];
    if (row->key_hex != NULL &&
        lc_pouch_query_index_key_hex_set_find(hidden, row->key_hex, NULL)) {
      continue;
    }
    rows->items[rows->count++] = *row;
    memset(row, 0, sizeof(*row));
  }
  return rc;
}

static int lc_pouch_query_index_segmented_mark_changed(
    const lc_allocator *allocator, const lc_pouch_index_doc_table *doc_table,
    lc_pouch_query_index_key_hex_set *hidden, lc_error *error) {
  size_t index;
  int rc;

  if (doc_table == NULL || hidden == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch segmented changed-key tracking requires doc "
                        "table and hidden set",
                        NULL, NULL, NULL);
  }
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < doc_table->count; ++index) {
    rc = lc_pouch_query_index_key_hex_set_add(
        allocator, hidden, doc_table->items[index].key_hex, error);
  }
  return rc;
}

static int lc_pouch_query_index_segmented_collect(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_query_index_segmented_collect_kind kind,
    const lc_pouch_index_term_key *exact_terms, size_t exact_term_count,
    const char *field_hex, const char *needle_hex, const char *needle_text,
    int prefix_match, int contains_match, int ignore_case,
    const lc_pouch_query_index_range_bounds *range_bounds,
    const lc_pouch_index_parsed_date_bounds *temporal_bounds,
    lc_pouch_index_result_row_list *rows, lc_pouch_generation *index_seq,
    int *text_complete_out, lc_error *error) {
  lc_pouch_query_index_manifest manifest;
  const lc_pouch_query_index_manifest *cached_manifest;
  lc_pouch_query_index_key_hex_set hidden;
  lc_pouch_query_index_segmented_collect_kind collect_kind;
  char *manifest_path;
  const char *collect_needle_hex;
  lc_pouch_generation state_index_seq;
  char trigram_needles[LC_POUCH_QUERY_INDEX_MAX_CONTAINS_TRIGRAMS][7];
  size_t trigram_count;
  size_t segment_index;
  int trigram_prefilter;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      rows == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch segmented query collect requires pouch, "
                        "namespace, rows, and index_seq",
                        NULL, NULL, NULL);
  }
  if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_EXACT &&
      (exact_terms == NULL || exact_term_count == 0U)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch segmented exact query requires terms", NULL,
                        NULL, NULL);
  }
  if ((kind == LC_POUCH_QUERY_INDEX_SEGMENTED_PRESENCE ||
       kind == LC_POUCH_QUERY_INDEX_SEGMENTED_RANGE ||
       kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT ||
       kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT_TOKEN ||
       kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TEMPORAL) &&
      (field_hex == NULL || field_hex[0] == '\0')) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch segmented field query requires a field", NULL,
                        NULL, NULL);
  }
  if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_RANGE && range_bounds == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch segmented range query requires bounds", NULL,
                        NULL, NULL);
  }
  if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TEMPORAL &&
      temporal_bounds == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch segmented date query requires parsed bounds",
                        NULL, NULL, NULL);
  }
  if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT &&
      (needle_hex == NULL || needle_text == NULL)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch segmented text query requires a needle", NULL,
                        NULL, NULL);
  }

  memset(&manifest, 0, sizeof(manifest));
  memset(&hidden, 0, sizeof(hidden));
  cached_manifest = NULL;
  state_index_seq = 0UL;
  collect_kind = kind;
  collect_needle_hex = needle_hex;
  memset(trigram_needles, 0, sizeof(trigram_needles));
  trigram_count = 0U;
  trigram_prefilter = 0;
  if (text_complete_out != NULL) {
    *text_complete_out = 1;
  }
  if ((kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TRIGRAM ||
       (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT && field_hex != NULL &&
        lc_pouch_query_index_field_hex_is_any_text(field_hex))) &&
      contains_match && needle_text != NULL && strlen(needle_text) >= 3U) {
    size_t needle_length;
    size_t window_index;

    needle_length = strlen(needle_text);
    for (window_index = 0U;
         window_index + 2U < needle_length &&
         trigram_count < LC_POUCH_QUERY_INDEX_MAX_CONTAINS_TRIGRAMS;
         ++window_index) {
      char candidate[7];
      size_t byte_index;
      size_t existing_index;
      int duplicate;

      for (byte_index = 0U; byte_index < 3U; ++byte_index) {
        unsigned char byte_value;

        byte_value = (unsigned char)needle_text[window_index + byte_index];
        byte_value = lc_pouch_query_index_ascii_lower(byte_value);
        lc_pouch_query_index_hex_encode_byte(candidate + (byte_index * 2U),
                                             byte_value);
      }
      candidate[6] = '\0';
      duplicate = 0;
      for (existing_index = 0U; existing_index < trigram_count;
           ++existing_index) {
        if (strcmp(trigram_needles[existing_index], candidate) == 0) {
          duplicate = 1;
          break;
        }
      }
      if (!duplicate) {
        memcpy(trigram_needles[trigram_count], candidate, sizeof(candidate));
        ++trigram_count;
      }
    }
    if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TRIGRAM) {
      collect_kind = LC_POUCH_QUERY_INDEX_SEGMENTED_TRIGRAM;
      collect_needle_hex = trigram_needles[0];
    } else {
      trigram_prefilter = trigram_count > 0U;
    }
  }
  *index_seq = 0UL;
  rc = LC_OK;
  if (lc_pouch_single_writer_enabled(pouch)) {
    rc = lc_pouch_state_index_seq(pouch, namespace_name, &state_index_seq,
                                  error);
    if (rc == LC_OK) {
      cached_manifest = lc_pouch_query_index_manifest_trust_snapshot(
          pouch, namespace_name, state_index_seq);
    }
  }
  if (rc == LC_OK && cached_manifest != NULL) {
    manifest = *cached_manifest;
  } else if (rc == LC_OK) {
    manifest_path =
        lc_pouch_query_index_manifest_path(pouch, namespace_name, error);
    if (manifest_path == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_manifest_read(pouch, manifest_path, &manifest,
                                              error);
    }
    lc_free_with_allocator(&pouch->allocator, manifest_path);
    if (rc == LC_OK && (!manifest.present || !manifest.valid)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index manifest is not readable", NULL,
                        NULL, "pouch");
    }
  }
  if (rc != LC_OK) {
    goto cleanup;
  }
  *index_seq = manifest.index_seq;

  for (segment_index = 0U;
       rc == LC_OK && segment_index < manifest.segment_count; ++segment_index) {
    const lc_pouch_query_index_manifest_segment *segment;
    lc_pouch_query_index_read_result header_artifact;
    lc_pouch_query_index_doc_table_cache_entry *doc_cache;
    const lc_pouch_index_doc_table *doc_table;
    lc_pouch_index_result_docid_list docids;
    lc_pouch_index_result_row_list segment_rows;
    char *header_path;
    char *doc_table_path;
    char *exact_term_path;
    char *presence_term_path;
    char *range_term_path;
    char *text_term_path;
    char *trigram_term_path;
    char *temporal_term_path;
    char *delete_path;
    const char *term_path;
    unsigned long delete_count;
    unsigned long delete_hash;
    size_t doc_index;
    int present;
    int valid;
    int segment_fully_hidden;
    int text_prefix_seen;

    segment = &manifest.segments[segment_index];
    memset(&header_artifact, 0, sizeof(header_artifact));
    doc_cache = NULL;
    doc_table = NULL;
    memset(&docids, 0, sizeof(docids));
    memset(&segment_rows, 0, sizeof(segment_rows));
    header_path = NULL;
    doc_table_path = NULL;
    exact_term_path = NULL;
    presence_term_path = NULL;
    range_term_path = NULL;
    text_term_path = NULL;
    trigram_term_path = NULL;
    temporal_term_path = NULL;
    delete_path = NULL;
    delete_count = 0UL;
    delete_hash = lc_pouch_query_index_hash_init();
    segment_fully_hidden = 0;
    text_prefix_seen = 0;
    rc = lc_pouch_query_index_segmented_paths(
        pouch, namespace_name, segment->id, &header_path, &doc_table_path,
        &exact_term_path, &presence_term_path, &range_term_path,
        &text_term_path, &trigram_term_path, &temporal_term_path, &delete_path,
        error);
    if (rc == LC_OK &&
        lc_pouch_query_index_artifact_cache_valid(
            pouch, header_path, LC_POUCH_QUERY_INDEX_ARTIFACT_HEADER,
            segment->index_seq, segment->row_count, segment->row_hash)) {
      header_artifact.present = 1;
      header_artifact.valid = 1;
      header_artifact.index_seq = segment->index_seq;
      header_artifact.row_count = segment->row_count;
      header_artifact.row_hash = segment->row_hash;
      header_artifact.term_index_complete = 1;
      header_artifact.presence_index_complete = 1;
    } else if (rc == LC_OK) {
      rc = lc_pouch_query_index_read_header(pouch, namespace_name, header_path,
                                            &header_artifact, error);
      if (rc == LC_OK && header_artifact.present && header_artifact.valid &&
          header_artifact.index_seq == segment->index_seq &&
          header_artifact.row_count == segment->row_count &&
          header_artifact.row_hash == segment->row_hash) {
        lc_pouch_query_index_artifact_cache_remember(
            pouch, header_path, LC_POUCH_QUERY_INDEX_ARTIFACT_HEADER,
            segment->index_seq, segment->row_count, segment->row_hash);
      }
    }
    if (rc == LC_OK && (!header_artifact.present || !header_artifact.valid ||
                        header_artifact.index_seq != segment->index_seq ||
                        header_artifact.row_count != segment->row_count ||
                        header_artifact.row_hash != segment->row_hash)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index segment header is not readable",
                        NULL, NULL, "pouch");
    }
    if (rc == LC_OK) {
      present = 0;
      valid = 0;
      rc = lc_pouch_query_index_doc_table_cache_get(
          pouch, namespace_name, doc_table_path, segment->index_seq,
          segment->row_count, segment->row_hash, &doc_cache, &present, &valid,
          error);
      if (rc == LC_OK && (!present || !valid)) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch query-index segment doc table is not "
                          "readable",
                          NULL, NULL, "pouch");
      }
      if (rc == LC_OK) {
        doc_table = &doc_cache->table;
      }
    }
    if (rc == LC_OK) {
      segment_fully_hidden = doc_table->count > 0U;
      for (doc_index = 0U; segment_fully_hidden && doc_index < doc_table->count;
           ++doc_index) {
        if (!lc_pouch_query_index_key_hex_set_find(
                &hidden, doc_table->items[doc_index].key_hex, NULL)) {
          segment_fully_hidden = 0;
        }
      }
    }
    if (rc == LC_OK && !segment_fully_hidden) {
      switch (kind) {
      case LC_POUCH_QUERY_INDEX_SEGMENTED_ALL:
        for (doc_index = 0U; rc == LC_OK && doc_index < doc_table->count;
             ++doc_index) {
          rc = lc_pouch_index_result_docid_list_add(
              &pouch->allocator, &docids, (unsigned long)doc_index, 0U, error);
        }
        break;
      case LC_POUCH_QUERY_INDEX_SEGMENTED_EXACT:
      case LC_POUCH_QUERY_INDEX_SEGMENTED_PRESENCE:
      case LC_POUCH_QUERY_INDEX_SEGMENTED_RANGE:
      case LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT:
      case LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT_TOKEN:
      case LC_POUCH_QUERY_INDEX_SEGMENTED_TRIGRAM:
      case LC_POUCH_QUERY_INDEX_SEGMENTED_TEMPORAL:
        if (kind == LC_POUCH_QUERY_INDEX_SEGMENTED_PRESENCE &&
            !header_artifact.presence_index_complete) {
          rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch query-index segment presence postings are "
                            "incomplete",
                            NULL, NULL, "pouch");
          break;
        }
        if (kind != LC_POUCH_QUERY_INDEX_SEGMENTED_PRESENCE &&
            !header_artifact.term_index_complete) {
          rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch query-index segment scalar postings are "
                            "incomplete",
                            NULL, NULL, "pouch");
          break;
        }
        term_path = exact_term_path;
        if (collect_kind == LC_POUCH_QUERY_INDEX_SEGMENTED_PRESENCE) {
          term_path = presence_term_path;
        } else if (collect_kind == LC_POUCH_QUERY_INDEX_SEGMENTED_RANGE) {
          term_path = range_term_path;
        } else if (collect_kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT ||
                   collect_kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT_TOKEN) {
          term_path = text_term_path;
        } else if (collect_kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TRIGRAM) {
          term_path = trigram_term_path;
        } else if (collect_kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TEMPORAL) {
          term_path = temporal_term_path;
        }
        present = 0;
        valid = 0;
        if (trigram_prefilter) {
          lc_pouch_index_result_docid_list trigram_docids;
          lc_pouch_index_result_docid_list exact_docids;

          memset(&trigram_docids, 0, sizeof(trigram_docids));
          memset(&exact_docids, 0, sizeof(exact_docids));
          text_prefix_seen = 0;
          rc = lc_pouch_query_index_collect_trigram_intersection_docids(
              pouch, namespace_name, trigram_term_path, segment->index_seq,
              segment->row_count, segment->row_hash, field_hex,
              (const char (*)[7])trigram_needles, trigram_count,
              &trigram_docids, &present, &valid, error);
          if (rc == LC_OK && (!present || !valid)) {
            rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                              "pouch query-index segment generation is not "
                              "readable",
                              NULL, NULL, "pouch");
          }
          if (rc == LC_OK && trigram_docids.count > 0U) {
            rc = lc_pouch_index_result_docid_list_sort_compact(
                &pouch->allocator, &trigram_docids, error);
          }
          if (rc == LC_OK && trigram_docids.count > 0U) {
            rc = lc_pouch_query_index_collect_generation_artifact_docids(
                pouch, namespace_name, text_term_path, segment->index_seq,
                segment->row_count, segment->row_hash,
                LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT, exact_terms,
                exact_term_count, field_hex, needle_hex, needle_text,
                prefix_match, contains_match, ignore_case, range_bounds,
                temporal_bounds, &trigram_docids, &exact_docids, &present,
                &valid, &text_prefix_seen, error);
            if (text_prefix_seen && text_complete_out != NULL) {
              *text_complete_out = 0;
            }
          }
          if (rc == LC_OK && trigram_docids.count > 0U &&
              (!present || !valid)) {
            rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                              "pouch query-index segment generation is not "
                              "readable",
                              NULL, NULL, "pouch");
          }
          if (rc == LC_OK && trigram_docids.count > 0U &&
              exact_docids.count > 0U) {
            rc = lc_pouch_index_result_docid_list_sort_compact(
                &pouch->allocator, &exact_docids, error);
          }
          if (rc == LC_OK && exact_docids.count > 0U) {
            docids = exact_docids;
            memset(&exact_docids, 0, sizeof(exact_docids));
          }
          lc_pouch_index_result_docid_list_cleanup(&pouch->allocator,
                                                   &exact_docids);
          lc_pouch_index_result_docid_list_cleanup(&pouch->allocator,
                                                   &trigram_docids);
        } else if (collect_kind == LC_POUCH_QUERY_INDEX_SEGMENTED_TRIGRAM &&
                   trigram_count > 0U) {
          rc = lc_pouch_query_index_collect_trigram_intersection_docids(
              pouch, namespace_name, term_path, segment->index_seq,
              segment->row_count, segment->row_hash, field_hex,
              (const char (*)[7])trigram_needles, trigram_count, &docids,
              &present, &valid, error);
        } else {
          rc = lc_pouch_query_index_collect_generation_artifact_docids(
              pouch, namespace_name, term_path, segment->index_seq,
              segment->row_count, segment->row_hash, collect_kind, exact_terms,
              exact_term_count, field_hex, collect_needle_hex, needle_text,
              prefix_match, contains_match, ignore_case, range_bounds,
              temporal_bounds, NULL, &docids, &present, &valid,
              text_complete_out != NULL ? &text_prefix_seen : NULL, error);
          if (text_prefix_seen && text_complete_out != NULL) {
            *text_complete_out = 0;
          }
        }
        if (rc == LC_OK && (!present || !valid)) {
          rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch query-index segment generation is not "
                            "readable",
                            NULL, NULL, "pouch");
        }
        break;
      }
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_docid_rows_build(
          &pouch->allocator, &docids, doc_table,
          doc_cache != NULL ? doc_cache->decoded_keys : NULL, &segment_rows,
          error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_segmented_rows_move_visible(
          &pouch->allocator, rows, &segment_rows, &hidden, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_segmented_mark_changed(
          &pouch->allocator, doc_table, &hidden, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_load_delete_keys(
          pouch, namespace_name, delete_path, &hidden, &delete_count,
          &delete_hash, error);
    }
    if (rc == LC_OK && (delete_count != segment->delete_count ||
                        delete_hash != segment->delete_hash)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index segment deletes do not match the "
                        "manifest",
                        NULL, NULL, "pouch");
    }
    lc_pouch_index_result_row_list_cleanup(&pouch->allocator, &segment_rows);
    lc_pouch_index_result_docid_list_cleanup(&pouch->allocator, &docids);
    lc_pouch_query_index_segmented_paths_cleanup(
        pouch, &header_path, &doc_table_path, &exact_term_path,
        &presence_term_path, &range_term_path, &text_term_path,
        &trigram_term_path, &temporal_term_path, &delete_path);
  }
  if (rc == LC_OK) {
    lc_pouch_query_index_result_rows_sort(&pouch->allocator, rows);
  }

cleanup:
  lc_pouch_query_index_key_hex_set_cleanup(&pouch->allocator, &hidden);
  if (cached_manifest == NULL) {
    lc_pouch_query_index_manifest_cleanup(&pouch->allocator, &manifest);
  }
  return rc;
}

static int lc_pouch_query_index_visit_text_generation(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *needle, int prefix_match, int contains_match, int ignore_case,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, int *text_complete_out, lc_error *error) {
  lc_pouch_index_result_row_list rows;
  char *field_hex;
  char *needle_hex;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      field == NULL || field[0] == '\0' || needle == NULL ||
      needle[0] == '\0' || visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index text lookup requires pouch, "
                        "namespace, field, needle, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  *index_seq = 0UL;
  field_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, field);
  needle_hex = ignore_case
                   ? lc_pouch_query_index_hex_encode_folded_ascii_bytes(
                         &pouch->allocator, needle, strlen(needle))
                   : lc_pouch_query_index_hex_encode(&pouch->allocator, needle);
  if (field_hex == NULL || needle_hex == NULL) {
    lc_free_with_allocator(&pouch->allocator, needle_hex);
    lc_free_with_allocator(&pouch->allocator, field_hex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index text lookup",
                        NULL, NULL, NULL);
  }
  memset(&rows, 0, sizeof(rows));
  rc = lc_pouch_query_index_segmented_collect(
      pouch, namespace_name, LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT, NULL, 0U,
      field_hex, needle_hex, needle, prefix_match, contains_match, ignore_case,
      NULL, NULL, &rows, index_seq, text_complete_out, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_rows_emit(&rows, visit, context, error);
  }
  lc_pouch_index_result_row_list_cleanup(&pouch->allocator, &rows);
  lc_free_with_allocator(&pouch->allocator, needle_hex);
  lc_free_with_allocator(&pouch->allocator, field_hex);
  return rc;
}

static int lc_pouch_query_index_visit_exact_generation(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_index_term_key *exact_terms, size_t exact_term_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  lc_pouch_index_result_row_list rows;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      exact_terms == NULL || exact_term_count == 0U || visit == NULL ||
      index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index exact generation visit requires "
                        "pouch, namespace, terms, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  memset(&rows, 0, sizeof(rows));
  *index_seq = 0UL;
  rc = lc_pouch_query_index_segmented_collect(
      pouch, namespace_name, LC_POUCH_QUERY_INDEX_SEGMENTED_EXACT, exact_terms,
      exact_term_count, NULL, NULL, NULL, 0, 0, 0, NULL, NULL, &rows, index_seq,
      NULL, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_rows_emit(&rows, visit, context, error);
  }
  lc_pouch_index_result_row_list_cleanup(&pouch->allocator, &rows);
  return rc;
}

static int
lc_pouch_query_index_any_merge_collect(const lc_pouch_query_index_key_view *key,
                                       void *context, lc_error *error) {
  lc_pouch_query_index_any_merge_context *merge_context;

  merge_context = (lc_pouch_query_index_any_merge_context *)context;
  if (merge_context == NULL || key == NULL ||
      key->value_index >= merge_context->list_count) {
    return LC_OK;
  }
  return lc_pouch_index_result_row_list_add(
      merge_context->allocator, &merge_context->lists[key->value_index],
      key->key, key->key_hex, key->doc_id, key->version, key->bytes,
      key->has_query_hidden, key->query_hidden, key->value_index, error);
}

static int lc_pouch_query_index_any_merge_emit(
    lc_pouch_query_index_any_merge_context *merge_context,
    lc_pouch_query_index_key_visit_fn visit, void *context, lc_error *error) {
  size_t *positions;
  size_t index;
  size_t min_index;
  const char *min_key;
  int rc;

  if (merge_context == NULL || visit == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index merge emit requires context and "
                        "visitor",
                        NULL, NULL, NULL);
  }
  positions = (size_t *)lc_alloc_with_allocator(
      merge_context->allocator, merge_context->list_count * sizeof(*positions));
  if (positions == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index merge cursors",
                        NULL, NULL, NULL);
  }
  memset(positions, 0, merge_context->list_count * sizeof(*positions));
  rc = LC_OK;
  while (rc == LC_OK) {
    min_index = merge_context->list_count;
    min_key = NULL;
    for (index = 0U; index < merge_context->list_count; ++index) {
      lc_pouch_index_result_row_list *list;

      list = &merge_context->lists[index];
      if (positions[index] >= list->count) {
        continue;
      }
      if (min_key == NULL ||
          strcmp(list->items[positions[index]].key, min_key) < 0) {
        min_key = list->items[positions[index]].key;
        min_index = index;
      }
    }
    if (min_key == NULL || min_index >= merge_context->list_count) {
      break;
    }
    {
      lc_pouch_index_result_row *row;
      lc_pouch_query_index_key_view key_view;

      row = &merge_context->lists[min_index].items[positions[min_index]];
      memset(&key_view, 0, sizeof(key_view));
      key_view.key = row->key;
      key_view.key_hex = row->key_hex;
      key_view.doc_id = row->doc_id;
      key_view.version = row->version;
      key_view.bytes = row->bytes;
      key_view.has_query_hidden = row->has_query_hidden;
      key_view.query_hidden = row->query_hidden;
      key_view.value_index = row->value_index;
      rc = visit(&key_view, context, error);
    }
    for (index = 0U; index < merge_context->list_count; ++index) {
      lc_pouch_index_result_row_list *list;

      list = &merge_context->lists[index];
      while (positions[index] < list->count &&
             strcmp(list->items[positions[index]].key, min_key) == 0) {
        ++positions[index];
      }
    }
  }
  if (rc == LC_POUCH_STATE_READ_MANY_STOP) {
    rc = LC_OK;
  }
  lc_free_with_allocator(merge_context->allocator, positions);
  return rc;
}

int lc_pouch_query_index_visit_scalar_any_merged(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *const *values, const char *value_types, size_t value_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  lc_pouch_query_index_any_merge_context merge_context;
  size_t index;
  int rc;

  memset(&merge_context, 0, sizeof(merge_context));
  if (pouch == NULL || value_count == 0U) {
    return lc_pouch_query_index_visit_scalar_any(
        pouch, namespace_name, field, values, value_types, value_count, visit,
        context, index_seq, error);
  }
  if (value_count == 1U) {
    return lc_pouch_query_index_visit_scalar(pouch, namespace_name, field,
                                             values[0], value_types[0], visit,
                                             context, index_seq, error);
  }
  merge_context.allocator = &pouch->allocator;
  merge_context.list_count = value_count;
  merge_context.lists =
      (lc_pouch_index_result_row_list *)lc_alloc_with_allocator(
          &pouch->allocator, value_count * sizeof(*merge_context.lists));
  if (merge_context.lists == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index merge lists",
                        NULL, NULL, NULL);
  }
  memset(merge_context.lists, 0, value_count * sizeof(*merge_context.lists));
  rc = lc_pouch_query_index_visit_scalar_any(
      pouch, namespace_name, field, values, value_types, value_count,
      lc_pouch_query_index_any_merge_collect, &merge_context, index_seq, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_any_merge_emit(&merge_context, visit, context,
                                             error);
  }
  for (index = 0U; index < value_count; ++index) {
    lc_pouch_index_result_row_list_cleanup(&pouch->allocator,
                                           &merge_context.lists[index]);
  }
  lc_free_with_allocator(&pouch->allocator, merge_context.lists);
  return rc;
}

int lc_pouch_query_index_visit_scalar_terms_merged(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_scalar_term *terms, size_t term_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  lc_pouch_query_index_any_merge_context merge_context;
  size_t index;
  int rc;

  memset(&merge_context, 0, sizeof(merge_context));
  if (pouch == NULL || term_count == 0U) {
    return lc_pouch_query_index_visit_scalar_terms(pouch, namespace_name, terms,
                                                   term_count, visit, context,
                                                   index_seq, error);
  }
  if (term_count == 1U) {
    return lc_pouch_query_index_visit_scalar(
        pouch, namespace_name, terms[0].field, terms[0].value,
        terms[0].value_type, visit, context, index_seq, error);
  }
  merge_context.allocator = &pouch->allocator;
  merge_context.list_count = term_count;
  merge_context.lists =
      (lc_pouch_index_result_row_list *)lc_alloc_with_allocator(
          &pouch->allocator, term_count * sizeof(*merge_context.lists));
  if (merge_context.lists == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index term merge "
                        "lists",
                        NULL, NULL, NULL);
  }
  memset(merge_context.lists, 0, term_count * sizeof(*merge_context.lists));
  rc = lc_pouch_query_index_visit_scalar_terms(
      pouch, namespace_name, terms, term_count,
      lc_pouch_query_index_any_merge_collect, &merge_context, index_seq, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_any_merge_emit(&merge_context, visit, context,
                                             error);
  }
  for (index = 0U; index < term_count; ++index) {
    lc_pouch_index_result_row_list_cleanup(&pouch->allocator,
                                           &merge_context.lists[index]);
  }
  lc_free_with_allocator(&pouch->allocator, merge_context.lists);
  return rc;
}

int lc_pouch_query_index_visit_scalar_terms_docids(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_query_index_scalar_term *terms, size_t term_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  lc_pouch_index_term_key *exact_terms;
  size_t exact_term_count;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      terms == NULL || term_count == 0U || visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index scalar term docID lookup requires "
                        "pouch, namespace, terms, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  *index_seq = 0UL;
  exact_terms = NULL;
  exact_term_count = 0U;
  rc = lc_pouch_query_index_build_exact_terms(terms, term_count, &exact_terms,
                                              &exact_term_count,
                                              &pouch->allocator, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_visit_exact_generation(
        pouch, namespace_name, exact_terms, exact_term_count, visit, context,
        index_seq, error);
  }
  lc_pouch_index_term_keys_cleanup(&pouch->allocator, exact_terms,
                                   exact_term_count);
  return rc;
}

int lc_pouch_query_index_visit_scalar_any_docids(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *const *values, const char *value_types, size_t value_count,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  lc_pouch_query_index_scalar_term *terms;
  size_t index;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      field == NULL || field[0] == '\0' || values == NULL ||
      value_types == NULL || value_count == 0U || visit == NULL ||
      index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index multi-scalar docID lookup requires "
                        "pouch, namespace, field, values, visitor, and "
                        "index_seq",
                        NULL, NULL, NULL);
  }
  terms = (lc_pouch_query_index_scalar_term *)lc_alloc_with_allocator(
      &pouch->allocator, value_count * sizeof(*terms));
  if (terms == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index multi-scalar "
                        "docID lookup",
                        NULL, NULL, NULL);
  }
  memset(terms, 0, value_count * sizeof(*terms));
  rc = LC_OK;
  for (index = 0U; index < value_count; ++index) {
    if (values[index] == NULL ||
        (value_types[index] != 's' && value_types[index] != 'n' &&
         value_types[index] != 'b' && value_types[index] != 'z')) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index multi-scalar docID lookup requires "
                        "non-null values and scalar value types",
                        NULL, NULL, NULL);
      break;
    }
    terms[index].field = field;
    terms[index].value = values[index];
    terms[index].value_type = value_types[index];
  }
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_visit_scalar_terms_docids(
        pouch, namespace_name, terms, value_count, visit, context, index_seq,
        error);
  }
  lc_free_with_allocator(&pouch->allocator, terms);
  return rc;
}

int lc_pouch_query_index_visit_prefix(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *prefix, int ignore_case,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  if (prefix == NULL || prefix[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index prefix lookup requires non-empty "
                        "prefix",
                        NULL, NULL, NULL);
  }
  return lc_pouch_query_index_visit_text_generation(
      pouch, namespace_name, field, prefix, 1, 0, ignore_case, visit, context,
      index_seq, NULL, error);
}

int lc_pouch_query_index_prefix_candidates_exact(const char *prefix) {
  return prefix == NULL ||
         strlen(prefix) <= LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES;
}

int lc_pouch_query_index_visit_prefix_candidates(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *prefix, int ignore_case,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  char *candidate_prefix;
  size_t candidate_len;
  int rc;

  if (prefix == NULL || prefix[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index prefix candidate lookup requires "
                        "non-empty prefix",
                        NULL, NULL, NULL);
  }
  if (pouch == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index prefix candidate lookup requires "
                        "pouch",
                        NULL, NULL, NULL);
  }
  if (lc_pouch_query_index_prefix_candidates_exact(prefix)) {
    return lc_pouch_query_index_visit_prefix(pouch, namespace_name, field,
                                             prefix, ignore_case, visit,
                                             context, index_seq, error);
  }
  candidate_len = LC_POUCH_QUERY_INDEX_EXACT_LONG_STRING_BYTES;
  candidate_prefix =
      (char *)lc_alloc_with_allocator(&pouch->allocator, candidate_len + 1U);
  if (candidate_prefix == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch prefix candidate", NULL, NULL,
                        NULL);
  }
  memcpy(candidate_prefix, prefix, candidate_len);
  candidate_prefix[candidate_len] = '\0';
  rc = lc_pouch_query_index_visit_text_generation(
      pouch, namespace_name, field, candidate_prefix, 1, 0, ignore_case, visit,
      context, index_seq, NULL, error);
  lc_free_with_allocator(&pouch->allocator, candidate_prefix);
  return rc;
}

int lc_pouch_query_index_visit_contains(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *needle, int ignore_case,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  if (needle == NULL || needle[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index contains lookup requires non-empty "
                        "needle",
                        NULL, NULL, NULL);
  }
  return lc_pouch_query_index_visit_text_generation(
      pouch, namespace_name, field, needle, 0, 1, ignore_case, visit, context,
      index_seq, NULL, error);
}

int lc_pouch_query_index_visit_contains_complete(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *needle, int ignore_case,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, int *complete, lc_error *error) {
  if (complete == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index contains complete lookup requires "
                        "complete output",
                        NULL, NULL, NULL);
  }
  *complete = 0;
  if (needle == NULL || needle[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index contains complete lookup requires "
                        "non-empty needle",
                        NULL, NULL, NULL);
  }
  return lc_pouch_query_index_visit_text_generation(
      pouch, namespace_name, field, needle, 0, 1, ignore_case, visit, context,
      index_seq, complete, error);
}

int lc_pouch_query_index_contains_text_complete(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    int *complete, lc_pouch_generation *index_seq, lc_error *error) {
  lc_pouch_query_index_manifest manifest;
  char *field_hex;
  char *manifest_path;
  size_t segment_index;
  int rc;

  if (complete == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index contains completeness requires "
                        "outputs",
                        NULL, NULL, NULL);
  }
  *complete = 0;
  *index_seq = 0UL;
  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      field == NULL || field[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index contains completeness requires "
                        "pouch, namespace, and field",
                        NULL, NULL, NULL);
  }
  field_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, field);
  if (field_hex == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch contains field", NULL, NULL,
                        NULL);
  }
  memset(&manifest, 0, sizeof(manifest));
  manifest_path =
      lc_pouch_query_index_manifest_path(pouch, namespace_name, error);
  if (manifest_path == NULL) {
    lc_free_with_allocator(&pouch->allocator, field_hex);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_query_index_manifest_read(pouch, manifest_path, &manifest,
                                          error);
  lc_free_with_allocator(&pouch->allocator, manifest_path);
  if (rc != LC_OK) {
    goto cleanup;
  }
  if (!manifest.present || !manifest.valid) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch query-index manifest is not readable", NULL, NULL,
                      "pouch");
    goto cleanup;
  }
  *index_seq = manifest.index_seq;
  *complete = 1;
  for (segment_index = 0U;
       rc == LC_OK && *complete && segment_index < manifest.segment_count;
       ++segment_index) {
    const lc_pouch_query_index_manifest_segment *segment;
    lc_pouch_query_index_generation_cache_entry *entry;
    char *header_path;
    char *doc_table_path;
    char *exact_term_path;
    char *presence_term_path;
    char *range_term_path;
    char *text_term_path;
    char *trigram_term_path;
    char *temporal_term_path;
    char *delete_path;
    size_t start;
    size_t term_index;
    int any_text_direct;
    int any_text_lookup;
    int present;
    int valid;

    segment = &manifest.segments[segment_index];
    entry = NULL;
    header_path = NULL;
    doc_table_path = NULL;
    exact_term_path = NULL;
    presence_term_path = NULL;
    range_term_path = NULL;
    text_term_path = NULL;
    trigram_term_path = NULL;
    temporal_term_path = NULL;
    delete_path = NULL;
    rc = lc_pouch_query_index_segmented_paths(
        pouch, namespace_name, segment->id, &header_path, &doc_table_path,
        &exact_term_path, &presence_term_path, &range_term_path,
        &text_term_path, &trigram_term_path, &temporal_term_path, &delete_path,
        error);
    present = 0;
    valid = 0;
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_generation_cache_get(
          pouch, namespace_name, text_term_path, segment->index_seq,
          segment->row_count, segment->row_hash, &entry, &present, &valid,
          error);
    }
    if (rc == LC_OK && (!present || !valid)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index segment text generation is not "
                        "readable",
                        NULL, NULL, "pouch");
    }
    any_text_lookup = lc_pouch_query_index_field_hex_is_any_text(field_hex);
    any_text_direct = 0;
    if (any_text_lookup) {
      start = 0U;
    } else {
      start = lc_pouch_query_index_generation_cache_field_lower_bound(
          entry, field_hex);
    }
    for (term_index = start; rc == LC_OK && term_index < entry->term_count;
         ++term_index) {
      const lc_pouch_query_index_generation_cache_term *term;

      term = &entry->terms[term_index];
      if ((any_text_direct || !any_text_lookup) &&
          strcmp(term->field_hex, field_hex) != 0) {
        break;
      }
      if (term->value_type == LC_POUCH_QUERY_INDEX_TEXT_PREFIX_TYPE &&
          (lc_pouch_query_index_field_hex_is_any_text(field_hex) ||
           strcmp(term->field_hex, field_hex) == 0)) {
        *complete = 0;
        break;
      }
    }
    lc_pouch_query_index_segmented_paths_cleanup(
        pouch, &header_path, &doc_table_path, &exact_term_path,
        &presence_term_path, &range_term_path, &text_term_path,
        &trigram_term_path, &temporal_term_path, &delete_path);
  }

cleanup:
  lc_pouch_query_index_manifest_cleanup(&pouch->allocator, &manifest);
  lc_free_with_allocator(&pouch->allocator, field_hex);
  return rc;
}

int lc_pouch_query_index_visit_any_text_token(
    lc_pouch *pouch, const char *namespace_name, const char *needle,
    int ignore_case, lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  lc_pouch_index_result_row_list rows;
  char *needle_hex;
  size_t needle_len;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      needle == NULL || visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index token lookup requires pouch, "
                        "namespace, needle, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  needle_len = strlen(needle);
  if (needle_len < LC_POUCH_QUERY_INDEX_MIN_EXACT_TOKEN_BYTES ||
      needle_len > LC_POUCH_QUERY_INDEX_MAX_EXACT_TOKEN_BYTES) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index token lookup requires a 4-8 byte "
                        "needle",
                        NULL, NULL, NULL);
  }
  *index_seq = 0UL;
  needle_hex = ignore_case ? lc_pouch_query_index_hex_encode_folded_ascii_bytes(
                                 &pouch->allocator, needle, needle_len)
                           : lc_pouch_query_index_hex_encode_bytes(
                                 &pouch->allocator, needle, needle_len);
  if (needle_hex == NULL) {
    lc_free_with_allocator(&pouch->allocator, needle_hex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index token needle",
                        NULL, NULL, NULL);
  }
  memset(&rows, 0, sizeof(rows));
  rc = lc_pouch_query_index_segmented_collect(
      pouch, namespace_name, LC_POUCH_QUERY_INDEX_SEGMENTED_TEXT_TOKEN, NULL,
      0U, LC_POUCH_QUERY_INDEX_ANY_TEXT_FIELD_HEX, needle_hex, NULL, 0, 0, 0,
      NULL, NULL, &rows, index_seq, NULL, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_rows_emit(&rows, visit, context, error);
  }
  lc_pouch_index_result_row_list_cleanup(&pouch->allocator, &rows);
  lc_free_with_allocator(&pouch->allocator, needle_hex);
  return rc;
}

int lc_pouch_query_index_visit_contains_candidates(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const char *needle, int ignore_case,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  lc_pouch_index_result_row_list rows;
  char *field_hex;
  char *needle_hex;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      field == NULL || field[0] == '\0' || needle == NULL ||
      strlen(needle) < 3U || visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index contains candidate lookup requires "
                        "pouch, namespace, field, 3-byte needle, visitor, and "
                        "index_seq",
                        NULL, NULL, NULL);
  }
  *index_seq = 0UL;
  field_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, field);
  needle_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, needle);
  if (field_hex == NULL || needle_hex == NULL) {
    lc_free_with_allocator(&pouch->allocator, needle_hex);
    lc_free_with_allocator(&pouch->allocator, field_hex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index contains "
                        "candidate lookup",
                        NULL, NULL, NULL);
  }
  memset(&rows, 0, sizeof(rows));
  rc = lc_pouch_query_index_segmented_collect(
      pouch, namespace_name, LC_POUCH_QUERY_INDEX_SEGMENTED_TRIGRAM, NULL, 0U,
      field_hex, needle_hex, needle, 0, 1, ignore_case, NULL, NULL, &rows,
      index_seq, NULL, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_rows_emit(&rows, visit, context, error);
  }
  lc_pouch_index_result_row_list_cleanup(&pouch->allocator, &rows);
  lc_free_with_allocator(&pouch->allocator, needle_hex);
  lc_free_with_allocator(&pouch->allocator, field_hex);
  return rc;
}

int lc_pouch_query_index_visit_range(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const lc_pouch_query_index_range_bounds *bounds,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  lc_pouch_index_result_row_list rows;
  char *field_hex;
  int rc;

  if (bounds == NULL || (!bounds->has_gt && !bounds->has_gte &&
                         !bounds->has_lt && !bounds->has_lte)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index range lookup requires numeric "
                        "bounds",
                        NULL, NULL, NULL);
  }
  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      field == NULL || field[0] == '\0' || visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index range lookup requires pouch, "
                        "namespace, field, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  *index_seq = 0UL;
  field_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, field);
  if (field_hex == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index range lookup",
                        NULL, NULL, NULL);
  }
  memset(&rows, 0, sizeof(rows));
  rc = lc_pouch_query_index_segmented_collect(
      pouch, namespace_name, LC_POUCH_QUERY_INDEX_SEGMENTED_RANGE, NULL, 0U,
      field_hex, NULL, NULL, 0, 0, 0, bounds, NULL, &rows, index_seq, NULL,
      error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_rows_emit(&rows, visit, context, error);
  }
  lc_pouch_index_result_row_list_cleanup(&pouch->allocator, &rows);
  lc_free_with_allocator(&pouch->allocator, field_hex);
  return rc;
}

int lc_pouch_query_index_visit_date(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    const lc_pouch_query_index_date_bounds *bounds,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  lc_pouch_index_parsed_date_bounds parsed_bounds;
  lc_pouch_index_result_row_list rows;
  char *field_hex;
  int rc;

  if (bounds == NULL || (!bounds->has_gt && !bounds->has_gte &&
                         !bounds->has_lt && !bounds->has_lte)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index date lookup requires temporal "
                        "bounds",
                        NULL, NULL, NULL);
  }
  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      field == NULL || field[0] == '\0' || visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query-index date lookup requires pouch, "
                        "namespace, field, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_index_parse_date_bounds(bounds, &parsed_bounds, error);
  if (rc != LC_OK) {
    return rc;
  }
  *index_seq = 0UL;
  field_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, field);
  if (field_hex == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index date lookup",
                        NULL, NULL, NULL);
  }
  memset(&rows, 0, sizeof(rows));
  rc = lc_pouch_query_index_segmented_collect(
      pouch, namespace_name, LC_POUCH_QUERY_INDEX_SEGMENTED_TEMPORAL, NULL, 0U,
      field_hex, NULL, NULL, 0, 0, 0, NULL, &parsed_bounds, &rows, index_seq,
      NULL, error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_rows_emit(&rows, visit, context, error);
  }
  lc_pouch_index_result_row_list_cleanup(&pouch->allocator, &rows);
  lc_free_with_allocator(&pouch->allocator, field_hex);
  return rc;
}

int lc_pouch_query_index_visit_exists(
    lc_pouch *pouch, const char *namespace_name, const char *field,
    lc_pouch_query_index_key_visit_fn visit, void *context,
    lc_pouch_generation *index_seq, lc_error *error) {
  lc_pouch_index_result_row_list rows;
  char *field_hex;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      field == NULL || field[0] == '\0' || visit == NULL || index_seq == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_query_index_visit_exists requires pouch, "
                        "namespace, field, visitor, and index_seq",
                        NULL, NULL, NULL);
  }
  *index_seq = 0UL;
  field_hex = lc_pouch_query_index_hex_encode(&pouch->allocator, field);
  if (field_hex == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch query-index exists lookup",
                        NULL, NULL, NULL);
  }
  memset(&rows, 0, sizeof(rows));
  rc = lc_pouch_query_index_segmented_collect(
      pouch, namespace_name, LC_POUCH_QUERY_INDEX_SEGMENTED_PRESENCE, NULL, 0U,
      field_hex, NULL, NULL, 0, 0, 0, NULL, NULL, &rows, index_seq, NULL,
      error);
  if (rc == LC_OK) {
    rc = lc_pouch_query_index_rows_emit(&rows, visit, context, error);
  }
  lc_pouch_index_result_row_list_cleanup(&pouch->allocator, &rows);
  lc_free_with_allocator(&pouch->allocator, field_hex);
  return rc;
}

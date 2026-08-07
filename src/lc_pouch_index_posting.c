#include "lc_pouch_index.h"

#include "lc_api_internal.h"

#include <limits.h>
#include <string.h>

#define LC_POUCH_INDEX_DENSE_TRACK_MIN_BYTES 4096U
#define LC_POUCH_INDEX_DENSE_TRACK_MIN_COUNT 4U
#define LC_POUCH_INDEX_DENSE_TRACK_SPARSE_MULTIPLIER 2U

static void lc_pouch_index_posting_rebind_inline_storage(
    lc_pouch_index_posting *posting) {
  if (posting != NULL && posting->using_inline_bytes) {
    posting->bytes = posting->inline_bytes;
    posting->capacity = sizeof(posting->inline_bytes);
  }
}

void lc_pouch_index_posting_cleanup(const lc_allocator *allocator,
                                    lc_pouch_index_posting *posting) {
  if (posting == NULL) {
    return;
  }
  if (!posting->using_inline_bytes) {
    lc_free_with_allocator(allocator, posting->bytes);
  }
  memset(posting, 0, sizeof(*posting));
}

static int lc_pouch_index_posting_reserve(lc_pouch_index_posting *posting,
                                          size_t needed,
                                          const lc_allocator *allocator,
                                          lc_error *error) {
  unsigned char *next_bytes;
  size_t next_capacity;

  if (posting == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index posting reserve requires posting", NULL,
                        NULL, NULL);
  }
  if (posting->bytes == NULL) {
    posting->bytes = posting->inline_bytes;
    posting->capacity = sizeof(posting->inline_bytes);
    posting->using_inline_bytes = 1;
  }
  if (needed <= posting->capacity) {
    return LC_OK;
  }
  next_capacity = posting->capacity == 0U ? 16U : posting->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch index posting exceeds local limit", NULL, NULL,
                          NULL);
    }
    next_capacity *= 2U;
  }
  next_bytes =
      (unsigned char *)lc_alloc_with_allocator(allocator, next_capacity);
  if (next_bytes == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index posting", NULL, NULL,
                        NULL);
  }
  if (posting->bytes != NULL) {
    memcpy(next_bytes, posting->bytes, posting->length);
    if (!posting->using_inline_bytes) {
      lc_free_with_allocator(allocator, posting->bytes);
    }
  }
  posting->bytes = next_bytes;
  posting->capacity = next_capacity;
  posting->using_inline_bytes = 0;
  return LC_OK;
}

static int lc_pouch_index_posting_append_varint(lc_pouch_index_posting *posting,
                                                unsigned long value,
                                                const lc_allocator *allocator,
                                                lc_error *error) {
  unsigned char encoded[sizeof(unsigned long) * 2U];
  size_t encoded_length;
  int rc;

  if (posting == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index posting append requires posting", NULL,
                        NULL, NULL);
  }
  encoded_length = 0U;
  do {
    unsigned char byte;

    byte = (unsigned char)(value & 0x7FUL);
    value >>= 7U;
    if (value != 0UL) {
      byte = (unsigned char)(byte | 0x80U);
    }
    encoded[encoded_length++] = byte;
  } while (value != 0UL && encoded_length < sizeof(encoded));
  if (value != 0UL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index posting varint exceeds local limit", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_index_posting_reserve(posting, posting->length + encoded_length,
                                      allocator, error);
  if (rc != LC_OK) {
    return rc;
  }
  memcpy(posting->bytes + posting->length, encoded, encoded_length);
  posting->length += encoded_length;
  return LC_OK;
}

int lc_pouch_index_posting_append_sorted_unique(lc_pouch_index_posting *posting,
                                                unsigned long doc_id,
                                                int *added,
                                                const lc_allocator *allocator,
                                                lc_error *error) {
  unsigned long delta;
  int rc;

  if (posting == NULL || added == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index posting append requires posting and "
                        "added",
                        NULL, NULL, NULL);
  }
  *added = 0;
  if (posting->has_last_doc_id) {
    if (doc_id < posting->last_doc_id) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index posting append requires sorted input",
                          NULL, NULL, "pouch");
    }
    if (doc_id == posting->last_doc_id) {
      return LC_OK;
    }
    delta = doc_id - posting->last_doc_id;
  } else {
    delta = doc_id;
  }
  rc = lc_pouch_index_posting_append_varint(posting, delta, allocator, error);
  if (rc != LC_OK) {
    return rc;
  }
  posting->last_doc_id = doc_id;
  posting->has_last_doc_id = 1;
  ++posting->count;
  *added = 1;
  return LC_OK;
}

static int
lc_pouch_index_posting_read_varint(const lc_pouch_index_posting *posting,
                                   size_t *offset, unsigned long *out,
                                   lc_error *error) {
  unsigned long value;
  unsigned int shift;

  if (posting == NULL || offset == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index posting varint read requires inputs", NULL,
                        NULL, NULL);
  }
  value = 0UL;
  shift = 0U;
  while (*offset < posting->length) {
    unsigned char byte;

    byte = posting->bytes[*offset];
    ++*offset;
    if (shift >= (unsigned int)(sizeof(unsigned long) * CHAR_BIT)) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index posting varint is too large", NULL, NULL,
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
                      "pouch index posting varint is truncated", NULL, NULL,
                      "pouch");
}

int lc_pouch_index_posting_append_to_set(const lc_pouch_index_posting *posting,
                                         lc_pouch_index_docid_set *set,
                                         const lc_allocator *allocator,
                                         lc_error *error) {
  unsigned long doc_id;
  unsigned long delta;
  size_t offset;
  size_t index;
  int added;
  int rc;

  if (posting == NULL || set == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index posting decode requires posting and set",
                        NULL, NULL, NULL);
  }
  doc_id = 0UL;
  delta = 0UL;
  offset = 0U;
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < posting->count; ++index) {
    rc = lc_pouch_index_posting_read_varint(posting, &offset, &delta, error);
    if (rc != LC_OK) {
      break;
    }
    if (index > 0U && delta > ULONG_MAX - doc_id) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index posting docID delta overflows", NULL, NULL,
                        "pouch");
      break;
    }
    doc_id = index == 0U ? delta : doc_id + delta;
    rc = lc_pouch_index_docid_set_append_sorted_unique(set, doc_id, &added,
                                                       allocator, error);
  }
  if (rc == LC_OK && offset != posting->length) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index posting has trailing bytes", NULL, NULL,
                      "pouch");
  }
  return rc;
}

void lc_pouch_index_dense_posting_cleanup(
    const lc_allocator *allocator, lc_pouch_index_dense_posting *posting) {
  if (posting == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, posting->bits);
  memset(posting, 0, sizeof(*posting));
}

static int lc_pouch_index_dense_posting_reserve(
    lc_pouch_index_dense_posting *posting, size_t needed,
    const lc_allocator *allocator, lc_error *error) {
  unsigned char *next_bits;
  size_t next_capacity;

  if (posting == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index dense posting reserve requires posting",
                        NULL, NULL, NULL);
  }
  if (needed <= posting->capacity) {
    return LC_OK;
  }
  next_capacity = posting->capacity == 0U ? 16U : posting->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch index dense posting exceeds local limit", NULL,
                          NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_bits =
      (unsigned char *)lc_alloc_with_allocator(allocator, next_capacity);
  if (next_bits == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index dense posting", NULL,
                        NULL, NULL);
  }
  memset(next_bits, 0, next_capacity);
  if (posting->bits != NULL) {
    memcpy(next_bits, posting->bits, posting->length);
    lc_free_with_allocator(allocator, posting->bits);
  }
  posting->bits = next_bits;
  posting->capacity = next_capacity;
  return LC_OK;
}

int lc_pouch_index_dense_posting_append_sorted_unique(
    lc_pouch_index_dense_posting *posting, unsigned long doc_id, int *added,
    const lc_allocator *allocator, lc_error *error) {
  unsigned long byte_index_ul;
  size_t byte_index;
  unsigned char mask;
  int rc;

  if (posting == NULL || added == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index dense posting append requires posting "
                        "and added",
                        NULL, NULL, NULL);
  }
  *added = 0;
  if (posting->has_last_doc_id) {
    if (doc_id < posting->last_doc_id) {
      return lc_error_set(
          error, LC_ERR_INVALID, 0L,
          "pouch index dense posting append requires sorted input", NULL, NULL,
          "pouch");
    }
    if (doc_id == posting->last_doc_id) {
      return LC_OK;
    }
  }
  byte_index_ul = doc_id / CHAR_BIT;
  if (byte_index_ul > (unsigned long)((size_t)-1)) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch index dense posting exceeds local limit", NULL,
                        NULL, NULL);
  }
  byte_index = (size_t)byte_index_ul;
  rc = lc_pouch_index_dense_posting_reserve(posting, byte_index + 1U, allocator,
                                            error);
  if (rc != LC_OK) {
    return rc;
  }
  if (posting->length < byte_index + 1U) {
    posting->length = byte_index + 1U;
  }
  mask = (unsigned char)(1U << (unsigned int)(doc_id % CHAR_BIT));
  if ((posting->bits[byte_index] & mask) == 0U) {
    posting->bits[byte_index] =
        (unsigned char)(posting->bits[byte_index] | mask);
    ++posting->count;
    *added = 1;
  }
  posting->max_doc_id = doc_id;
  posting->last_doc_id = doc_id;
  posting->has_last_doc_id = 1;
  return LC_OK;
}

int lc_pouch_index_dense_posting_append_to_set(
    const lc_pouch_index_dense_posting *posting, lc_pouch_index_docid_set *set,
    const lc_allocator *allocator, lc_error *error) {
  size_t byte_index;
  unsigned int bit_index;
  size_t seen;
  unsigned long doc_id;
  int added;
  int rc;

  if (posting == NULL || set == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch index dense posting decode requires posting and set", NULL, NULL,
        NULL);
  }
  if (posting->length > 0U && posting->bits == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index dense posting is missing bit storage",
                        NULL, NULL, "pouch");
  }
  seen = 0U;
  rc = LC_OK;
  for (byte_index = 0U; rc == LC_OK && byte_index < posting->length;
       ++byte_index) {
    for (bit_index = 0U; bit_index < (unsigned int)CHAR_BIT; ++bit_index) {
      if ((posting->bits[byte_index] & (unsigned char)(1U << bit_index)) ==
          0U) {
        continue;
      }
      doc_id = ((unsigned long)byte_index * (unsigned long)CHAR_BIT) +
               (unsigned long)bit_index;
      if (!posting->has_last_doc_id || doc_id > posting->max_doc_id) {
        return lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch index dense posting bit exceeds max docID",
                            NULL, NULL, "pouch");
      }
      rc = lc_pouch_index_docid_set_append_sorted_unique(set, doc_id, &added,
                                                         allocator, error);
      if (rc != LC_OK) {
        break;
      }
      ++seen;
    }
  }
  if (rc == LC_OK && seen != posting->count) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index dense posting count mismatch", NULL, NULL,
                      "pouch");
  }
  return rc;
}

void lc_pouch_index_adaptive_posting_cleanup(
    const lc_allocator *allocator, lc_pouch_index_adaptive_posting *posting) {
  if (posting == NULL) {
    return;
  }
  lc_pouch_index_posting_cleanup(allocator, &posting->sparse);
  lc_pouch_index_dense_posting_cleanup(allocator, &posting->dense);
  memset(posting, 0, sizeof(*posting));
}

void lc_pouch_index_adaptive_posting_rebind_inline_storage(
    lc_pouch_index_adaptive_posting *posting) {
  if (posting == NULL) {
    return;
  }
  lc_pouch_index_posting_rebind_inline_storage(&posting->sparse);
}

static int lc_pouch_index_adaptive_should_track_dense(
    const lc_pouch_index_adaptive_posting *posting, unsigned long doc_id) {
  unsigned long dense_length_ul;
  size_t dense_length;
  size_t sparse_limit;

  if (posting == NULL || posting->dense_disabled) {
    return 0;
  }
  dense_length_ul = (doc_id / CHAR_BIT) + 1UL;
  if (dense_length_ul > (unsigned long)((size_t)-1)) {
    return 0;
  }
  dense_length = (size_t)dense_length_ul;
  sparse_limit =
      posting->sparse.length * LC_POUCH_INDEX_DENSE_TRACK_SPARSE_MULTIPLIER;
  if (sparse_limit < LC_POUCH_INDEX_DENSE_TRACK_MIN_BYTES) {
    sparse_limit = LC_POUCH_INDEX_DENSE_TRACK_MIN_BYTES;
  }
  return dense_length <= sparse_limit;
}

static int lc_pouch_index_adaptive_posting_rebuild_dense(
    lc_pouch_index_adaptive_posting *posting, const lc_allocator *allocator,
    lc_error *error) {
  lc_pouch_index_docid_set decoded;
  size_t index;
  int added;
  int rc;

  if (posting == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index adaptive posting rebuild requires "
                        "posting",
                        NULL, NULL, NULL);
  }
  memset(&decoded, 0, sizeof(decoded));
  lc_pouch_index_dense_posting_cleanup(allocator, &posting->dense);
  rc = lc_pouch_index_posting_append_to_set(&posting->sparse, &decoded,
                                            allocator, error);
  for (index = 0U; rc == LC_OK && index < decoded.count; ++index) {
    added = 0;
    rc = lc_pouch_index_dense_posting_append_sorted_unique(
        &posting->dense, decoded.items[index], &added, allocator, error);
    if (rc == LC_OK && !added) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index adaptive posting dense rebuild lost "
                        "docID",
                        NULL, NULL, "pouch");
    }
  }
  lc_pouch_index_docid_set_cleanup(allocator, &decoded);
  return rc;
}

int lc_pouch_index_adaptive_posting_append_sorted_unique(
    lc_pouch_index_adaptive_posting *posting, unsigned long doc_id, int *added,
    const lc_allocator *allocator, lc_error *error) {
  int dense_added;
  int rc;

  if (posting == NULL || added == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index adaptive posting append requires "
                        "posting and added",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_index_posting_append_sorted_unique(&posting->sparse, doc_id,
                                                   added, allocator, error);
  if (rc != LC_OK || !*added) {
    return rc;
  }
  if (posting->sparse.count < LC_POUCH_INDEX_DENSE_TRACK_MIN_COUNT) {
    return LC_OK;
  }
  if (!lc_pouch_index_adaptive_should_track_dense(posting, doc_id)) {
    posting->dense_disabled = 1;
    lc_pouch_index_dense_posting_cleanup(allocator, &posting->dense);
    return LC_OK;
  }
  if (posting->dense.count == 0U && !posting->dense.has_last_doc_id) {
    return lc_pouch_index_adaptive_posting_rebuild_dense(posting, allocator,
                                                         error);
  }
  dense_added = 0;
  rc = lc_pouch_index_dense_posting_append_sorted_unique(
      &posting->dense, doc_id, &dense_added, allocator, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!dense_added) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index adaptive posting dense append lost docID",
                        NULL, NULL, "pouch");
  }
  return LC_OK;
}

int lc_pouch_index_adaptive_posting_build_sorted_unique_trusted(
    lc_pouch_index_adaptive_posting *posting, const unsigned long *doc_ids,
    size_t doc_id_count, const lc_allocator *allocator, lc_error *error) {
  unsigned long last_doc_id;
  unsigned long slots;
  unsigned long density_threshold;
  size_t index;
  int rc;

  if (posting == NULL || (doc_ids == NULL && doc_id_count > 0U)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index adaptive posting build requires posting "
                        "and docIDs",
                        NULL, NULL, NULL);
  }
  memset(posting, 0, sizeof(*posting));
  if (doc_id_count == 0U) {
    return LC_OK;
  }
  last_doc_id = 0UL;
  for (index = 0U; index < doc_id_count; ++index) {
    unsigned long doc_id;
    unsigned long delta;

    doc_id = doc_ids[index];
    if (index > 0U && doc_id <= last_doc_id) {
      lc_pouch_index_adaptive_posting_cleanup(allocator, posting);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index adaptive posting build requires sorted "
                          "unique docIDs",
                          NULL, NULL, "pouch");
    }
    delta = index == 0U ? doc_id : doc_id - last_doc_id;
    rc = lc_pouch_index_posting_append_varint(&posting->sparse, delta,
                                              allocator, error);
    if (rc != LC_OK) {
      lc_pouch_index_adaptive_posting_cleanup(allocator, posting);
      return rc;
    }
    last_doc_id = doc_id;
  }
  posting->sparse.count = doc_id_count;
  posting->sparse.last_doc_id = last_doc_id;
  posting->sparse.has_last_doc_id = 1;
  if (doc_id_count < LC_POUCH_INDEX_DENSE_TRACK_MIN_COUNT ||
      !lc_pouch_index_adaptive_should_track_dense(posting, last_doc_id) ||
      last_doc_id == (unsigned long)-1) {
    return LC_OK;
  }
  slots = last_doc_id + 1UL;
  density_threshold = slots / 4UL;
  if (slots % 4UL != 0UL) {
    ++density_threshold;
  }
  if (posting->sparse.count > (size_t)ULONG_MAX ||
      (unsigned long)posting->sparse.count < density_threshold) {
    return LC_OK;
  }
  rc = lc_pouch_index_dense_posting_reserve(
      &posting->dense, (size_t)((last_doc_id / CHAR_BIT) + 1UL), allocator,
      error);
  if (rc != LC_OK) {
    lc_pouch_index_adaptive_posting_cleanup(allocator, posting);
    return rc;
  }
  posting->dense.length = (size_t)((last_doc_id / CHAR_BIT) + 1UL);
  posting->dense.count = doc_id_count;
  posting->dense.max_doc_id = last_doc_id;
  posting->dense.last_doc_id = last_doc_id;
  posting->dense.has_last_doc_id = 1;
  for (index = 0U; index < doc_id_count; ++index) {
    unsigned long doc_id;
    size_t byte_index;
    unsigned char mask;

    doc_id = doc_ids[index];
    byte_index = (size_t)(doc_id / CHAR_BIT);
    mask = (unsigned char)(1U << (unsigned int)(doc_id % CHAR_BIT));
    posting->dense.bits[byte_index] =
        (unsigned char)(posting->dense.bits[byte_index] | mask);
  }
  return LC_OK;
}

lc_pouch_index_adaptive_posting_kind
lc_pouch_index_adaptive_posting_selected_kind(
    const lc_pouch_index_adaptive_posting *posting) {
  unsigned long slots;
  unsigned long dense_count;
  unsigned long density_threshold;

  if (posting == NULL || posting->dense_disabled ||
      posting->dense.count != posting->sparse.count ||
      posting->sparse.count == 0U) {
    return LC_POUCH_INDEX_ADAPTIVE_POSTING_SPARSE;
  }
  slots = posting->dense.max_doc_id + 1UL;
  if (slots < posting->dense.max_doc_id) {
    return LC_POUCH_INDEX_ADAPTIVE_POSTING_SPARSE;
  }
  if (posting->dense.count > (size_t)ULONG_MAX) {
    return LC_POUCH_INDEX_ADAPTIVE_POSTING_SPARSE;
  }
  dense_count = (unsigned long)posting->dense.count;
  density_threshold = slots / 4UL;
  if (slots % 4UL != 0UL) {
    ++density_threshold;
  }
  if (posting->dense.length <= posting->sparse.length &&
      dense_count >= density_threshold) {
    return LC_POUCH_INDEX_ADAPTIVE_POSTING_DENSE;
  }
  return LC_POUCH_INDEX_ADAPTIVE_POSTING_SPARSE;
}

int lc_pouch_index_adaptive_posting_append_to_set(
    const lc_pouch_index_adaptive_posting *posting,
    lc_pouch_index_docid_set *set, const lc_allocator *allocator,
    lc_error *error) {
  if (posting == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index adaptive posting decode requires posting",
                        NULL, NULL, NULL);
  }
  if (lc_pouch_index_adaptive_posting_selected_kind(posting) ==
      LC_POUCH_INDEX_ADAPTIVE_POSTING_DENSE) {
    return lc_pouch_index_dense_posting_append_to_set(&posting->dense, set,
                                                      allocator, error);
  }
  return lc_pouch_index_posting_append_to_set(&posting->sparse, set, allocator,
                                              error);
}

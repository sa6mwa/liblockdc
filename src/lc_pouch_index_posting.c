#include "lc_pouch_index.h"

#include "lc_api_internal.h"

#include <limits.h>
#include <string.h>

void lc_pouch_index_posting_cleanup(const lc_allocator *allocator,
                                    lc_pouch_index_posting *posting) {
  if (posting == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, posting->bytes);
  memset(posting, 0, sizeof(*posting));
}

static int lc_pouch_index_posting_reserve(
    lc_pouch_index_posting *posting, size_t needed,
    const lc_allocator *allocator, lc_error *error) {
  unsigned char *next_bytes;
  size_t next_capacity;

  if (posting == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index posting reserve requires posting", NULL,
                        NULL, NULL);
  }
  if (needed <= posting->capacity) {
    return LC_OK;
  }
  next_capacity = posting->capacity == 0U ? 16U : posting->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch index posting exceeds local limit", NULL,
                          NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_bytes = (unsigned char *)lc_alloc_with_allocator(allocator,
                                                        next_capacity);
  if (next_bytes == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index posting", NULL, NULL,
                        NULL);
  }
  if (posting->bytes != NULL) {
    memcpy(next_bytes, posting->bytes, posting->length);
    lc_free_with_allocator(allocator, posting->bytes);
  }
  posting->bytes = next_bytes;
  posting->capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_index_posting_append_varint(
    lc_pouch_index_posting *posting, unsigned long value,
    const lc_allocator *allocator, lc_error *error) {
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
  rc = lc_pouch_index_posting_reserve(
      posting, posting->length + encoded_length, allocator, error);
  if (rc != LC_OK) {
    return rc;
  }
  memcpy(posting->bytes + posting->length, encoded, encoded_length);
  posting->length += encoded_length;
  return LC_OK;
}

int lc_pouch_index_posting_append_sorted_unique(
    lc_pouch_index_posting *posting, unsigned long doc_id, int *added,
    const lc_allocator *allocator, lc_error *error) {
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
                          NULL, NULL, "pouch-redesign");
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

static int lc_pouch_index_posting_read_varint(
    const lc_pouch_index_posting *posting, size_t *offset,
    unsigned long *out, lc_error *error) {
  unsigned long value;
  unsigned int shift;

  if (posting == NULL || offset == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index posting varint read requires inputs",
                        NULL, NULL, NULL);
  }
  value = 0UL;
  shift = 0U;
  while (*offset < posting->length) {
    unsigned char byte;

    byte = posting->bytes[*offset];
    ++*offset;
    if (shift >= (unsigned int)(sizeof(unsigned long) * CHAR_BIT)) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index posting varint is too large", NULL,
                          NULL, "pouch-redesign");
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
                      "pouch-redesign");
}

int lc_pouch_index_posting_append_to_set(
    const lc_pouch_index_posting *posting, lc_pouch_index_docid_set *set,
    const lc_allocator *allocator, lc_error *error) {
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
                        "pouch index posting docID delta overflows", NULL,
                        NULL, "pouch-redesign");
      break;
    }
    doc_id = index == 0U ? delta : doc_id + delta;
    rc = lc_pouch_index_docid_set_append_sorted_unique(
        set, doc_id, &added, allocator, error);
  }
  if (rc == LC_OK && offset != posting->length) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch index posting has trailing bytes", NULL, NULL,
                      "pouch-redesign");
  }
  return rc;
}

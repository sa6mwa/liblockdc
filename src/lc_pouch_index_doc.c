#include "lc_pouch_index.h"

#include "lc_api_internal.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define LC_POUCH_INDEX_DOC_TABLE_GENERATION_FORMAT "pouch-doc-table-generation"
#define LC_POUCH_INDEX_DOC_TABLE_GENERATION_VERSION 1UL

void lc_pouch_index_docid_set_cleanup(const lc_allocator *allocator,
                                      lc_pouch_index_docid_set *set) {
  if (set == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, set->items);
  memset(set, 0, sizeof(*set));
}

static int lc_pouch_index_docid_set_reserve(lc_pouch_index_docid_set *set,
                                            size_t needed,
                                            const lc_allocator *allocator,
                                            lc_error *error) {
  unsigned long *next_items;
  size_t next_capacity;

  if (set == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index docID set reserve requires set", NULL,
                        NULL, NULL);
  }
  if (needed <= set->capacity) {
    return LC_OK;
  }
  next_capacity = set->capacity == 0U ? 16U : set->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch index docID set exceeds local limit", NULL,
                          NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_items = (unsigned long *)lc_alloc_with_allocator(
      allocator, next_capacity * sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index docID set", NULL, NULL,
                        NULL);
  }
  if (set->items != NULL) {
    memcpy(next_items, set->items, set->count * sizeof(*next_items));
    lc_free_with_allocator(allocator, set->items);
  }
  set->items = next_items;
  set->capacity = next_capacity;
  return LC_OK;
}

int lc_pouch_index_docid_set_append_sorted_unique(lc_pouch_index_docid_set *set,
                                                  unsigned long doc_id,
                                                  int *added,
                                                  const lc_allocator *allocator,
                                                  lc_error *error) {
  int rc;

  if (set == NULL || added == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index docID set append requires set and added",
                        NULL, NULL, NULL);
  }
  *added = 0;
  if (set->count > 0U) {
    if (doc_id < set->items[set->count - 1U]) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index docID set append requires sorted input",
                          NULL, NULL, "pouch");
    }
    if (doc_id == set->items[set->count - 1U]) {
      return LC_OK;
    }
  }
  rc = lc_pouch_index_docid_set_reserve(set, set->count + 1U, allocator, error);
  if (rc != LC_OK) {
    return rc;
  }
  set->items[set->count++] = doc_id;
  *added = 1;
  return LC_OK;
}

int lc_pouch_index_docid_set_append_unique(lc_pouch_index_docid_set *set,
                                           unsigned long doc_id, int *added,
                                           const lc_allocator *allocator,
                                           lc_error *error) {
  size_t index;
  int rc;

  if (set == NULL || added == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index docID set append requires set and added",
                        NULL, NULL, NULL);
  }
  *added = 0;
  for (index = 0U; index < set->count; ++index) {
    if (set->items[index] == doc_id) {
      return LC_OK;
    }
  }
  rc = lc_pouch_index_docid_set_reserve(set, set->count + 1U, allocator, error);
  if (rc != LC_OK) {
    return rc;
  }
  set->items[set->count++] = doc_id;
  *added = 1;
  return LC_OK;
}

static int lc_pouch_index_docid_set_validate_sorted_unique(
    const lc_pouch_index_docid_set *set, const char *role, lc_error *error) {
  size_t index;

  if (set == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index docID set merge requires inputs", NULL,
                        NULL, role);
  }
  for (index = 1U; index < set->count; ++index) {
    if (set->items[index] <= set->items[index - 1U]) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch index docID set merge requires sorted unique "
                          "inputs",
                          NULL, NULL, role);
    }
  }
  return LC_OK;
}

static int lc_pouch_index_docid_set_prepare_merge(
    const lc_pouch_index_docid_set *left, const lc_pouch_index_docid_set *right,
    lc_pouch_index_docid_set *out, lc_error *error) {
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index docID set merge requires output", NULL,
                        NULL, NULL);
  }
  if (out == left || out == right) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index docID set merge output must not alias "
                        "inputs",
                        NULL, NULL, "pouch");
  }
  if (out->count != 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index docID set merge requires empty output",
                        NULL, NULL, "pouch");
  }
  rc = lc_pouch_index_docid_set_validate_sorted_unique(left, "left", error);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_pouch_index_docid_set_validate_sorted_unique(right, "right", error);
}

int lc_pouch_index_docid_set_union_sorted(const lc_pouch_index_docid_set *left,
                                          const lc_pouch_index_docid_set *right,
                                          lc_pouch_index_docid_set *out,
                                          const lc_allocator *allocator,
                                          lc_error *error) {
  size_t left_index;
  size_t right_index;
  unsigned long doc_id;
  int added;
  int rc;

  rc = lc_pouch_index_docid_set_prepare_merge(left, right, out, error);
  if (rc != LC_OK) {
    return rc;
  }
  left_index = 0U;
  right_index = 0U;
  while (left_index < left->count || right_index < right->count) {
    if (right_index >= right->count ||
        (left_index < left->count &&
         left->items[left_index] < right->items[right_index])) {
      doc_id = left->items[left_index++];
    } else if (left_index >= left->count ||
               right->items[right_index] < left->items[left_index]) {
      doc_id = right->items[right_index++];
    } else {
      doc_id = left->items[left_index];
      ++left_index;
      ++right_index;
    }
    rc = lc_pouch_index_docid_set_append_sorted_unique(out, doc_id, &added,
                                                       allocator, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  return LC_OK;
}

int lc_pouch_index_docid_set_intersect_sorted(
    const lc_pouch_index_docid_set *left, const lc_pouch_index_docid_set *right,
    lc_pouch_index_docid_set *out, const lc_allocator *allocator,
    lc_error *error) {
  size_t left_index;
  size_t right_index;
  int added;
  int rc;

  rc = lc_pouch_index_docid_set_prepare_merge(left, right, out, error);
  if (rc != LC_OK) {
    return rc;
  }
  left_index = 0U;
  right_index = 0U;
  while (left_index < left->count && right_index < right->count) {
    if (left->items[left_index] < right->items[right_index]) {
      ++left_index;
      continue;
    }
    if (right->items[right_index] < left->items[left_index]) {
      ++right_index;
      continue;
    }
    rc = lc_pouch_index_docid_set_append_sorted_unique(
        out, left->items[left_index], &added, allocator, error);
    if (rc != LC_OK) {
      return rc;
    }
    ++left_index;
    ++right_index;
  }
  return LC_OK;
}

int lc_pouch_index_docid_set_subtract_sorted(
    const lc_pouch_index_docid_set *left, const lc_pouch_index_docid_set *right,
    lc_pouch_index_docid_set *out, const lc_allocator *allocator,
    lc_error *error) {
  size_t left_index;
  size_t right_index;
  int added;
  int rc;

  rc = lc_pouch_index_docid_set_prepare_merge(left, right, out, error);
  if (rc != LC_OK) {
    return rc;
  }
  left_index = 0U;
  right_index = 0U;
  while (left_index < left->count) {
    while (right_index < right->count &&
           right->items[right_index] < left->items[left_index]) {
      ++right_index;
    }
    if (right_index < right->count &&
        right->items[right_index] == left->items[left_index]) {
      ++left_index;
      continue;
    }
    rc = lc_pouch_index_docid_set_append_sorted_unique(
        out, left->items[left_index], &added, allocator, error);
    if (rc != LC_OK) {
      return rc;
    }
    ++left_index;
  }
  return LC_OK;
}

void lc_pouch_index_doc_table_cleanup(const lc_allocator *allocator,
                                      lc_pouch_index_doc_table *table) {
  if (table == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, table->items);
  lc_free_with_allocator(allocator, table->owned_generation_bytes);
  memset(table, 0, sizeof(*table));
}

static int lc_pouch_index_doc_table_reserve(lc_pouch_index_doc_table *table,
                                            size_t needed,
                                            const lc_allocator *allocator,
                                            lc_error *error) {
  lc_pouch_index_doc *next_items;
  size_t next_capacity;

  if (table == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index doc table reserve requires table", NULL,
                        NULL, NULL);
  }
  if (needed <= table->capacity) {
    return LC_OK;
  }
  next_capacity = table->capacity == 0U ? 64U : table->capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch index doc table exceeds local limit", NULL,
                          NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_items = (lc_pouch_index_doc *)lc_alloc_with_allocator(
      allocator, next_capacity * sizeof(*next_items));
  if (next_items == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch index doc table", NULL, NULL,
                        NULL);
  }
  if (table->items != NULL) {
    memcpy(next_items, table->items, table->count * sizeof(*next_items));
    lc_free_with_allocator(allocator, table->items);
  }
  table->items = next_items;
  table->capacity = next_capacity;
  return LC_OK;
}

int lc_pouch_index_doc_table_append_sorted_unique(
    lc_pouch_index_doc_table *table, const char *key_hex, unsigned long version,
    unsigned long bytes, int has_query_hidden, int query_hidden,
    unsigned long *doc_id, const lc_allocator *allocator, lc_error *error) {
  lc_pouch_index_doc *doc;
  int cmp;
  int rc;

  if (table == NULL || key_hex == NULL || key_hex[0] == '\0' ||
      doc_id == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index doc table append requires table, key, and "
                        "docID output",
                        NULL, NULL, NULL);
  }
  if (table->count > 0U) {
    cmp = strcmp(key_hex, table->items[table->count - 1U].key_hex);
    if (cmp <= 0) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          cmp == 0 ? "pouch index doc table rejects duplicate "
                                     "keys"
                                   : "pouch index doc table append requires "
                                     "sorted keys",
                          NULL, NULL, "pouch");
    }
  }
  rc = lc_pouch_index_doc_table_reserve(table, table->count + 1U, allocator,
                                        error);
  if (rc != LC_OK) {
    return rc;
  }
  *doc_id = (unsigned long)table->count;
  doc = &table->items[table->count++];
  doc->key_hex = key_hex;
  doc->version = version;
  doc->bytes = bytes;
  doc->has_query_hidden = has_query_hidden ? 1 : 0;
  doc->query_hidden = query_hidden ? 1 : 0;
  return LC_OK;
}

int lc_pouch_index_doc_table_find_key_hex(const lc_pouch_index_doc_table *table,
                                          const char *key_hex,
                                          unsigned long *doc_id, int *found,
                                          lc_error *error) {
  size_t low;
  size_t high;
  size_t mid;
  int cmp;

  if (table == NULL || key_hex == NULL || key_hex[0] == '\0' ||
      doc_id == NULL || found == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index doc table lookup requires table, key, "
                        "docID output, and found output",
                        NULL, NULL, NULL);
  }
  *found = 0;
  low = 0U;
  high = table->count;
  while (low < high) {
    mid = low + ((high - low) / 2U);
    cmp = strcmp(key_hex, table->items[mid].key_hex);
    if (cmp == 0) {
      *doc_id = (unsigned long)mid;
      *found = 1;
      return LC_OK;
    }
    if (cmp < 0) {
      high = mid;
    } else {
      low = mid + 1U;
    }
  }
  return LC_OK;
}

int lc_pouch_index_doc_table_get(const lc_pouch_index_doc_table *table,
                                 unsigned long doc_id,
                                 const lc_pouch_index_doc **doc,
                                 lc_error *error) {
  if (table == NULL || doc == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index doc table get requires table and output",
                        NULL, NULL, NULL);
  }
  *doc = NULL;
  if (doc_id >= (unsigned long)table->count) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index doc table docID is out of range", NULL,
                        NULL, "pouch");
  }
  *doc = &table->items[doc_id];
  return LC_OK;
}

static int lc_pouch_index_doc_generation_reserve(const lc_allocator *allocator,
                                                 char **bytes, size_t *capacity,
                                                 size_t length, size_t needed,
                                                 lc_error *error) {
  char *next_bytes;
  size_t next_capacity;

  if (bytes == NULL || capacity == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch doc table generation reserve requires output",
                        NULL, NULL, NULL);
  }
  if (needed <= *capacity) {
    return LC_OK;
  }
  next_capacity = *capacity == 0U ? 256U : *capacity;
  while (next_capacity < needed) {
    if (next_capacity > ((size_t)-1 / 2U)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch doc table generation exceeds local limit",
                          NULL, NULL, NULL);
    }
    next_capacity *= 2U;
  }
  next_bytes = (char *)lc_alloc_with_allocator(allocator, next_capacity);
  if (next_bytes == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch doc table generation", NULL,
                        NULL, NULL);
  }
  if (*bytes != NULL) {
    memcpy(next_bytes, *bytes, length);
    lc_free_with_allocator(allocator, *bytes);
  }
  *bytes = next_bytes;
  *capacity = next_capacity;
  return LC_OK;
}

static int lc_pouch_index_doc_generation_append(
    const lc_allocator *allocator, char **bytes, size_t *length,
    size_t *capacity, const char *data, size_t data_length, lc_error *error) {
  int rc;

  if (length == NULL || data == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch doc table generation append requires data", NULL,
                        NULL, NULL);
  }
  if (data_length > (size_t)-1 - *length - 1U) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch doc table generation exceeds local limit", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_index_doc_generation_reserve(
      allocator, bytes, capacity, *length, *length + data_length + 1U, error);
  if (rc != LC_OK) {
    return rc;
  }
  memcpy(*bytes + *length, data, data_length);
  *length += data_length;
  (*bytes)[*length] = '\0';
  return LC_OK;
}

static int lc_pouch_index_doc_generation_append_cstr(
    const lc_allocator *allocator, char **bytes, size_t *length,
    size_t *capacity, const char *text, lc_error *error) {
  return lc_pouch_index_doc_generation_append(
      allocator, bytes, length, capacity, text, strlen(text), error);
}

int lc_pouch_index_doc_table_generation_encode(
    const lc_pouch_index_doc_table *table, unsigned long index_seq,
    unsigned long row_hash, const lc_allocator *allocator, char **out_bytes,
    size_t *out_length, lc_error *error) {
  char *bytes;
  size_t length;
  size_t capacity;
  char line[256];
  size_t index;
  int written;
  int rc;

  if (table == NULL || out_bytes == NULL || out_length == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch doc table generation encode requires table and "
                        "outputs",
                        NULL, NULL, NULL);
  }
  *out_bytes = NULL;
  *out_length = 0U;
  bytes = NULL;
  length = 0U;
  capacity = 0U;
  rc = lc_pouch_index_doc_generation_append_cstr(
      allocator, &bytes, &length, &capacity,
      "format=" LC_POUCH_INDEX_DOC_TABLE_GENERATION_FORMAT "\n", error);
  if (rc == LC_OK) {
    written = snprintf(line, sizeof(line), "version=%lu\n",
                       LC_POUCH_INDEX_DOC_TABLE_GENERATION_VERSION);
    rc = written >= 0 && (size_t)written < sizeof(line)
             ? lc_pouch_index_doc_generation_append(allocator, &bytes, &length,
                                                    &capacity, line,
                                                    (size_t)written, error)
             : lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch doc table generation version exceeds local "
                            "limit",
                            NULL, NULL, NULL);
  }
  if (rc == LC_OK) {
    written = snprintf(line, sizeof(line), "index_seq=%lu\n", index_seq);
    rc = written >= 0 && (size_t)written < sizeof(line)
             ? lc_pouch_index_doc_generation_append(allocator, &bytes, &length,
                                                    &capacity, line,
                                                    (size_t)written, error)
             : lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch doc table generation index exceeds local "
                            "limit",
                            NULL, NULL, NULL);
  }
  if (rc == LC_OK) {
    written = snprintf(line, sizeof(line), "row_count=%lu\n",
                       (unsigned long)table->count);
    rc = written >= 0 && (size_t)written < sizeof(line)
             ? lc_pouch_index_doc_generation_append(allocator, &bytes, &length,
                                                    &capacity, line,
                                                    (size_t)written, error)
             : lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch doc table generation count exceeds local "
                            "limit",
                            NULL, NULL, NULL);
  }
  if (rc == LC_OK) {
    written = snprintf(line, sizeof(line), "row_hash=%lu\n", row_hash);
    rc = written >= 0 && (size_t)written < sizeof(line)
             ? lc_pouch_index_doc_generation_append(allocator, &bytes, &length,
                                                    &capacity, line,
                                                    (size_t)written, error)
             : lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch doc table generation hash exceeds local "
                            "limit",
                            NULL, NULL, NULL);
  }
  for (index = 0U; rc == LC_OK && index < table->count; ++index) {
    const lc_pouch_index_doc *doc;

    doc = &table->items[index];
    written = snprintf(line, sizeof(line), "doc ");
    rc = written >= 0 && (size_t)written < sizeof(line)
             ? lc_pouch_index_doc_generation_append(allocator, &bytes, &length,
                                                    &capacity, line,
                                                    (size_t)written, error)
             : lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch doc table generation row exceeds local "
                            "limit",
                            NULL, NULL, NULL);
    if (rc == LC_OK) {
      rc = lc_pouch_index_doc_generation_append_cstr(
          allocator, &bytes, &length, &capacity, doc->key_hex, error);
    }
    if (rc == LC_OK) {
      written = snprintf(line, sizeof(line), " %lu %lu %d %d\n", doc->version,
                         doc->bytes, doc->has_query_hidden ? 1 : 0,
                         doc->query_hidden ? 1 : 0);
      rc = written >= 0 && (size_t)written < sizeof(line)
               ? lc_pouch_index_doc_generation_append(allocator, &bytes,
                                                      &length, &capacity, line,
                                                      (size_t)written, error)
               : lc_error_set(error, LC_ERR_INVALID, 0L,
                              "pouch doc table generation row exceeds local "
                              "limit",
                              NULL, NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    *out_bytes = bytes;
    *out_length = length;
    bytes = NULL;
  }
  lc_free_with_allocator(allocator, bytes);
  return rc;
}

static int lc_pouch_index_doc_generation_parse_ulong(const char *text,
                                                     unsigned long *out) {
  char *end;
  unsigned long value;

  if (text == NULL || text[0] == '\0' || out == NULL) {
    return 0;
  }
  errno = 0;
  value = strtoul(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0') {
    return 0;
  }
  *out = value;
  return 1;
}

static int lc_pouch_index_doc_generation_header_ulong(const char *line,
                                                      const char *prefix,
                                                      unsigned long *out) {
  size_t prefix_len;

  prefix_len = strlen(prefix);
  if (strncmp(line, prefix, prefix_len) != 0) {
    return 0;
  }
  return lc_pouch_index_doc_generation_parse_ulong(line + prefix_len, out);
}

static char *lc_pouch_index_doc_generation_next_line(char **cursor) {
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

static int lc_pouch_index_doc_generation_hex_token_valid(const char *token) {
  size_t index;
  unsigned char ch;

  if (token == NULL || token[0] == '\0') {
    return 0;
  }
  for (index = 0U; token[index] != '\0'; ++index) {
    ch = (unsigned char)token[index];
    if (!((ch >= (unsigned char)'0' && ch <= (unsigned char)'9') ||
          (ch >= (unsigned char)'a' && ch <= (unsigned char)'f') ||
          (ch >= (unsigned char)'A' && ch <= (unsigned char)'F'))) {
      return 0;
    }
  }
  return index % 2U == 0U;
}

static int lc_pouch_index_doc_generation_parse_bytes(
    const lc_allocator *allocator, char *bytes,
    unsigned long expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, lc_pouch_index_doc_table *table,
    int *valid, lc_error *error) {
  char *cursor;
  char *line;
  char *previous_key;
  unsigned long version;
  unsigned long row_count;
  unsigned long row_hash;
  unsigned long actual_count;
  unsigned long doc_bytes;
  int has_hidden;
  int hidden;
  int consumed;
  int rc;
  unsigned long doc_id;

  if (valid == NULL || bytes == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch doc table generation parse requires bytes and "
                        "valid output",
                        NULL, NULL, NULL);
  }
  *valid = 0;
  cursor = bytes;
  line = lc_pouch_index_doc_generation_next_line(&cursor);
  if (line == NULL ||
      strcmp(line, "format=" LC_POUCH_INDEX_DOC_TABLE_GENERATION_FORMAT) != 0) {
    goto done;
  }
  line = lc_pouch_index_doc_generation_next_line(&cursor);
  if (line == NULL ||
      !lc_pouch_index_doc_generation_header_ulong(line, "version=", &version) ||
      version != LC_POUCH_INDEX_DOC_TABLE_GENERATION_VERSION) {
    goto done;
  }
  line = lc_pouch_index_doc_generation_next_line(&cursor);
  if (line == NULL ||
      !lc_pouch_index_doc_generation_header_ulong(line,
                                                  "index_seq=", &version) ||
      version != expected_index_seq) {
    goto done;
  }
  line = lc_pouch_index_doc_generation_next_line(&cursor);
  if (line == NULL ||
      !lc_pouch_index_doc_generation_header_ulong(line,
                                                  "row_count=", &row_count) ||
      row_count != expected_row_count) {
    goto done;
  }
  line = lc_pouch_index_doc_generation_next_line(&cursor);
  if (line == NULL ||
      !lc_pouch_index_doc_generation_header_ulong(line,
                                                  "row_hash=", &row_hash) ||
      row_hash != expected_row_hash) {
    goto done;
  }
  previous_key = NULL;
  actual_count = 0UL;
  while ((line = lc_pouch_index_doc_generation_next_line(&cursor)) != NULL) {
    char *key;
    char *rest;

    if (line[0] == '\0') {
      continue;
    }
    if (strncmp(line, "doc ", 4U) != 0) {
      goto done;
    }
    key = line + 4U;
    rest = strchr(key, ' ');
    if (rest == NULL || rest == key) {
      goto done;
    }
    *rest++ = '\0';
    if (!lc_pouch_index_doc_generation_hex_token_valid(key) ||
        (previous_key != NULL && strcmp(previous_key, key) >= 0)) {
      goto done;
    }
    consumed = 0;
    if (sscanf(rest, "%lu %lu %d %d %n", &version, &doc_bytes, &has_hidden,
               &hidden, &consumed) != 4 ||
        consumed <= 0 || rest[consumed] != '\0' ||
        (has_hidden != 0 && has_hidden != 1) || (hidden != 0 && hidden != 1)) {
      goto done;
    }
    if (table != NULL) {
      rc = lc_pouch_index_doc_table_append_sorted_unique(
          table, key, version, doc_bytes, has_hidden, hidden, &doc_id,
          allocator, error);
      if (rc != LC_OK) {
        return rc;
      }
    }
    previous_key = key;
    ++actual_count;
  }
  *valid = actual_count == row_count;

done:
  (void)allocator;
  return LC_OK;
}

static int lc_pouch_index_doc_generation_read_file(
    const lc_allocator *allocator, const char *path, char **out_bytes,
    size_t *out_length, int *present, lc_error *error) {
  FILE *fp;
  char *bytes;
  size_t length;
  size_t capacity;
  size_t got;

  if (path == NULL || out_bytes == NULL || out_length == NULL ||
      present == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch doc table generation read requires path "
                        "and outputs",
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
                        "failed to open pouch doc table generation",
                        strerror(errno), NULL, NULL);
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
                            "pouch doc table generation exceeds local limit",
                            NULL, NULL, NULL);
      }
      next_bytes = (char *)lc_alloc_with_allocator(allocator, next_capacity);
      if (next_bytes == NULL) {
        fclose(fp);
        lc_free_with_allocator(allocator, bytes);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch doc table generation",
                            NULL, NULL, NULL);
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
                            "failed to read pouch doc table generation",
                            strerror(errno), NULL, NULL);
      }
      break;
    }
  }
  if (fclose(fp) != 0) {
    lc_free_with_allocator(allocator, bytes);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch doc table generation",
                        strerror(errno), NULL, NULL);
  }
  bytes[length] = '\0';
  *out_bytes = bytes;
  *out_length = length;
  return LC_OK;
}

int lc_pouch_index_doc_table_generation_validate_file(
    const lc_allocator *allocator, const char *path,
    unsigned long expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, int *present, int *valid,
    lc_error *error) {
  FILE *fp;
  char line[256];
  unsigned long value;

  (void)allocator;
  if (valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch doc table generation validation requires "
                        "valid output",
                        NULL, NULL, NULL);
  }
  *valid = 0;
  if (present != NULL) {
    *present = 0;
  }
  fp = fopen(path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, (long)errno,
                        "failed to open pouch doc table generation",
                        strerror(errno), NULL, NULL);
  }
  if (present != NULL) {
    *present = 1;
  }
  if (fgets(line, sizeof(line), fp) == NULL) {
    fclose(fp);
    return LC_OK;
  }
  line[strcspn(line, "\n")] = '\0';
  if (strcmp(line, "format=" LC_POUCH_INDEX_DOC_TABLE_GENERATION_FORMAT) != 0) {
    fclose(fp);
    return LC_OK;
  }
  if (fgets(line, sizeof(line), fp) == NULL) {
    fclose(fp);
    return LC_OK;
  }
  line[strcspn(line, "\n")] = '\0';
  if (!lc_pouch_index_doc_generation_header_ulong(line, "version=", &value) ||
      value != LC_POUCH_INDEX_DOC_TABLE_GENERATION_VERSION) {
    fclose(fp);
    return LC_OK;
  }
  if (fgets(line, sizeof(line), fp) == NULL) {
    fclose(fp);
    return LC_OK;
  }
  line[strcspn(line, "\n")] = '\0';
  if (!lc_pouch_index_doc_generation_header_ulong(line, "index_seq=", &value) ||
      value != expected_index_seq) {
    fclose(fp);
    return LC_OK;
  }
  if (fgets(line, sizeof(line), fp) == NULL) {
    fclose(fp);
    return LC_OK;
  }
  line[strcspn(line, "\n")] = '\0';
  if (!lc_pouch_index_doc_generation_header_ulong(line, "row_count=", &value) ||
      value != expected_row_count) {
    fclose(fp);
    return LC_OK;
  }
  if (fgets(line, sizeof(line), fp) == NULL) {
    fclose(fp);
    return LC_OK;
  }
  line[strcspn(line, "\n")] = '\0';
  if (!lc_pouch_index_doc_generation_header_ulong(line, "row_hash=", &value) ||
      value != expected_row_hash) {
    fclose(fp);
    return LC_OK;
  }
  if (fclose(fp) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch doc table generation",
                        strerror(errno), NULL, NULL);
  }
  *valid = 1;
  return LC_OK;
}

int lc_pouch_index_doc_table_generation_load_file(
    const lc_allocator *allocator, const char *path,
    unsigned long expected_index_seq, unsigned long expected_row_count,
    unsigned long expected_row_hash, lc_pouch_index_doc_table *table,
    int *present, int *valid, lc_error *error) {
  char *bytes;
  size_t length;
  int rc;

  if (table == NULL || valid == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch doc table generation load requires table and "
                        "valid output",
                        NULL, NULL, NULL);
  }
  memset(table, 0, sizeof(*table));
  *valid = 0;
  bytes = NULL;
  length = 0U;
  rc = lc_pouch_index_doc_generation_read_file(allocator, path, &bytes, &length,
                                               present, error);
  if (rc == LC_OK && present != NULL && *present) {
    rc = lc_pouch_index_doc_generation_parse_bytes(
        allocator, bytes, expected_index_seq, expected_row_count,
        expected_row_hash, table, valid, error);
    if (rc == LC_OK && *valid) {
      table->owned_generation_bytes = bytes;
      bytes = NULL;
    } else {
      lc_pouch_index_doc_table_cleanup(allocator, table);
    }
  }
  lc_free_with_allocator(allocator, bytes);
  return rc;
}

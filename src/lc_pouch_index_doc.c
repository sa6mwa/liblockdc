#include "lc_pouch_index.h"

#include "lc_api_internal.h"

#include <string.h>

void lc_pouch_index_docid_set_cleanup(const lc_allocator *allocator,
                                      lc_pouch_index_docid_set *set) {
  if (set == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, set->items);
  memset(set, 0, sizeof(*set));
}

static int lc_pouch_index_docid_set_reserve(
    lc_pouch_index_docid_set *set, size_t needed,
    const lc_allocator *allocator, lc_error *error) {
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
                        "failed to allocate pouch index docID set", NULL,
                        NULL, NULL);
  }
  if (set->items != NULL) {
    memcpy(next_items, set->items, set->count * sizeof(*next_items));
    lc_free_with_allocator(allocator, set->items);
  }
  set->items = next_items;
  set->capacity = next_capacity;
  return LC_OK;
}

int lc_pouch_index_docid_set_append_sorted_unique(
    lc_pouch_index_docid_set *set, unsigned long doc_id, int *added,
    const lc_allocator *allocator, lc_error *error) {
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
                          NULL, NULL, "pouch-redesign");
    }
    if (doc_id == set->items[set->count - 1U]) {
      return LC_OK;
    }
  }
  rc = lc_pouch_index_docid_set_reserve(set, set->count + 1U, allocator,
                                        error);
  if (rc != LC_OK) {
    return rc;
  }
  set->items[set->count++] = doc_id;
  *added = 1;
  return LC_OK;
}

int lc_pouch_index_docid_set_append_unique(
    lc_pouch_index_docid_set *set, unsigned long doc_id, int *added,
    const lc_allocator *allocator, lc_error *error) {
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
  rc = lc_pouch_index_docid_set_reserve(set, set->count + 1U, allocator,
                                        error);
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
    const lc_pouch_index_docid_set *left,
    const lc_pouch_index_docid_set *right, lc_pouch_index_docid_set *out,
    lc_error *error) {
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
                        NULL, NULL, "pouch-redesign");
  }
  if (out->count != 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch index docID set merge requires empty output",
                        NULL, NULL, "pouch-redesign");
  }
  rc = lc_pouch_index_docid_set_validate_sorted_unique(left, "left", error);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_pouch_index_docid_set_validate_sorted_unique(right, "right", error);
}

int lc_pouch_index_docid_set_union_sorted(
    const lc_pouch_index_docid_set *left,
    const lc_pouch_index_docid_set *right, lc_pouch_index_docid_set *out,
    const lc_allocator *allocator, lc_error *error) {
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
    rc = lc_pouch_index_docid_set_append_sorted_unique(
        out, doc_id, &added, allocator, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  return LC_OK;
}

int lc_pouch_index_docid_set_intersect_sorted(
    const lc_pouch_index_docid_set *left,
    const lc_pouch_index_docid_set *right, lc_pouch_index_docid_set *out,
    const lc_allocator *allocator, lc_error *error) {
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
    const lc_pouch_index_docid_set *left,
    const lc_pouch_index_docid_set *right, lc_pouch_index_docid_set *out,
    const lc_allocator *allocator, lc_error *error) {
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
  memset(table, 0, sizeof(*table));
}

static int lc_pouch_index_doc_table_reserve(
    lc_pouch_index_doc_table *table, size_t needed,
    const lc_allocator *allocator, lc_error *error) {
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
                        "failed to allocate pouch index doc table", NULL,
                        NULL, NULL);
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
    lc_pouch_index_doc_table *table, const char *key_hex,
    unsigned long version, unsigned long bytes, int has_query_hidden,
    int query_hidden, unsigned long *doc_id, const lc_allocator *allocator,
    lc_error *error) {
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
                          NULL, NULL, "pouch-redesign");
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

int lc_pouch_index_doc_table_find_key_hex(
    const lc_pouch_index_doc_table *table, const char *key_hex,
    unsigned long *doc_id, int *found, lc_error *error) {
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
                        NULL, "pouch-redesign");
  }
  *doc = &table->items[doc_id];
  return LC_OK;
}

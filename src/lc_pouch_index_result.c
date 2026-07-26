#include "lc_pouch_index.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int lc_pouch_index_result_plan_has_no_eq_filters(
    const lc_pouch_query_index_scan_req *req) {
  return req->document_eq_term_count == 0U &&
         req->document_not_eq_term_count == 0U &&
         req->document_or_eq_term_count == 0U;
}

static int lc_pouch_index_result_plan_has_no_range_filters(
    const lc_pouch_query_index_scan_req *req) {
  return req->document_range_term_count == 0U &&
         req->document_not_range_term_count == 0U &&
         req->document_or_range_term_count == 0U;
}

static int lc_pouch_index_result_plan_has_no_in_filters(
    const lc_pouch_query_index_scan_req *req) {
  return req->document_in_term_count == 0U &&
         req->document_not_in_term_count == 0U &&
         req->document_or_in_term_count == 0U;
}

static int lc_pouch_index_result_plan_has_no_prefix_filters(
    const lc_pouch_query_index_scan_req *req) {
  return req->document_prefix_term_count == 0U &&
         req->document_not_prefix_term_count == 0U &&
         req->document_or_prefix_term_count == 0U;
}

static int lc_pouch_index_result_plan_has_no_contains_filters(
    const lc_pouch_query_index_scan_req *req) {
  return req->document_contains_term_count == 0U &&
         req->document_not_contains_term_count == 0U &&
         req->document_or_contains_term_count == 0U;
}

static int lc_pouch_index_result_plan_has_no_exists_filters(
    const lc_pouch_query_index_scan_req *req) {
  return req->document_exists_term_count == 0U &&
         req->document_not_exists_term_count == 0U &&
         req->document_or_exists_term_count == 0U &&
         req->document_exists_path_pattern_count == 0U &&
         req->document_or_exists_path_pattern_count == 0U;
}

static int lc_pouch_index_result_plan_cacheable_common(
    const lc_pouch_query_index_scan_req *req) {
  return req != NULL && req->namespace_name != NULL && req->key == NULL &&
         req->owner == NULL;
}

static int lc_pouch_index_result_plan_eq_cacheable(
    const lc_pouch_query_index_scan_req *req) {
  return lc_pouch_index_result_plan_cacheable_common(req) &&
         req->document_eq_terms != NULL && req->document_eq_term_count == 1U &&
         req->document_eq_terms[0].field != NULL &&
         req->document_eq_terms[0].value != NULL &&
         req->document_not_eq_term_count == 0U &&
         req->document_or_eq_term_count == 0U &&
         lc_pouch_index_result_plan_has_no_range_filters(req) &&
         lc_pouch_index_result_plan_has_no_in_filters(req) &&
         lc_pouch_index_result_plan_has_no_prefix_filters(req) &&
         lc_pouch_index_result_plan_has_no_contains_filters(req) &&
         lc_pouch_index_result_plan_has_no_exists_filters(req);
}

static int lc_pouch_index_result_plan_exists_cacheable(
    const lc_pouch_query_index_scan_req *req) {
  return lc_pouch_index_result_plan_cacheable_common(req) &&
         req->document_exists_terms != NULL &&
         req->document_exists_term_count == 1U &&
         req->document_exists_terms[0].field != NULL &&
         lc_pouch_index_result_plan_has_no_eq_filters(req) &&
         lc_pouch_index_result_plan_has_no_range_filters(req) &&
         lc_pouch_index_result_plan_has_no_in_filters(req) &&
         lc_pouch_index_result_plan_has_no_prefix_filters(req) &&
         lc_pouch_index_result_plan_has_no_contains_filters(req) &&
         req->document_not_exists_term_count == 0U &&
         req->document_or_exists_term_count == 0U &&
         req->document_exists_path_pattern_count == 0U &&
         req->document_or_exists_path_pattern_count == 0U;
}

static int lc_pouch_index_result_plan_in_cacheable(
    const lc_pouch_query_index_scan_req *req) {
  return lc_pouch_index_result_plan_cacheable_common(req) &&
         req->document_in_terms != NULL && req->document_in_term_count == 1U &&
         req->document_in_terms[0].field != NULL &&
         req->document_in_terms[0].values != NULL &&
         req->document_in_terms[0].value_count > 0U &&
         strchr(req->document_in_terms[0].field, '*') == NULL &&
         lc_pouch_index_result_plan_has_no_eq_filters(req) &&
         lc_pouch_index_result_plan_has_no_range_filters(req) &&
         req->document_not_in_term_count == 0U &&
         req->document_or_in_term_count == 0U &&
         lc_pouch_index_result_plan_has_no_prefix_filters(req) &&
         lc_pouch_index_result_plan_has_no_contains_filters(req) &&
         lc_pouch_index_result_plan_has_no_exists_filters(req);
}

static int lc_pouch_index_result_plan_range_cacheable(
    const lc_pouch_query_index_scan_req *req) {
  const lc_pouch_document_range_term *term;

  if (!lc_pouch_index_result_plan_cacheable_common(req) ||
      req->document_range_terms == NULL ||
      req->document_range_term_count != 1U ||
      req->document_range_terms[0].field == NULL) {
    return 0;
  }
  term = &req->document_range_terms[0];
  return (term->gt != NULL || term->gte != NULL || term->lt != NULL ||
          term->lte != NULL) &&
         lc_pouch_index_result_plan_has_no_eq_filters(req) &&
         req->document_not_range_term_count == 0U &&
         req->document_or_range_term_count == 0U &&
         lc_pouch_index_result_plan_has_no_in_filters(req) &&
         lc_pouch_index_result_plan_has_no_prefix_filters(req) &&
         lc_pouch_index_result_plan_has_no_contains_filters(req) &&
         lc_pouch_index_result_plan_has_no_exists_filters(req);
}

static int lc_pouch_index_result_plan_prefix_cacheable(
    const lc_pouch_query_index_scan_req *req) {
  return lc_pouch_index_result_plan_cacheable_common(req) &&
         req->document_prefix_terms != NULL &&
         req->document_prefix_term_count == 1U &&
         req->document_prefix_terms[0].field != NULL &&
         req->document_prefix_terms[0].value != NULL &&
         lc_pouch_index_result_plan_has_no_eq_filters(req) &&
         lc_pouch_index_result_plan_has_no_range_filters(req) &&
         lc_pouch_index_result_plan_has_no_in_filters(req) &&
         req->document_not_prefix_term_count == 0U &&
         req->document_or_prefix_term_count == 0U &&
         lc_pouch_index_result_plan_has_no_contains_filters(req) &&
         lc_pouch_index_result_plan_has_no_exists_filters(req);
}

static int lc_pouch_index_result_plan_contains_cacheable(
    const lc_pouch_query_index_scan_req *req) {
  return lc_pouch_index_result_plan_cacheable_common(req) &&
         req->document_contains_terms != NULL &&
         req->document_contains_term_count == 1U &&
         req->document_contains_terms[0].field != NULL &&
         req->document_contains_terms[0].value != NULL &&
         lc_pouch_index_result_plan_has_no_eq_filters(req) &&
         lc_pouch_index_result_plan_has_no_range_filters(req) &&
         lc_pouch_index_result_plan_has_no_in_filters(req) &&
         lc_pouch_index_result_plan_has_no_prefix_filters(req) &&
         req->document_not_contains_term_count == 0U &&
         req->document_or_contains_term_count == 0U &&
         lc_pouch_index_result_plan_has_no_exists_filters(req);
}

static char *
lc_pouch_index_eq_result_plan_key(const lc_pouch_allocator *allocator,
                                  const lc_pouch_query_index_scan_req *req) {
  const lc_pouch_document_eq_term *term;
  size_t namespace_len;
  size_t field_len;
  size_t value_len;
  int written;
  size_t needed;
  char *key;

  if (!lc_pouch_index_result_plan_eq_cacheable(req)) {
    return NULL;
  }
  term = &req->document_eq_terms[0];
  namespace_len = strlen(req->namespace_name);
  field_len = strlen(term->field);
  value_len = strlen(term->value);
  written =
      snprintf(NULL, 0, "eq:%lu:%s:%lu:%s:%lu:%s", (unsigned long)namespace_len,
               req->namespace_name, (unsigned long)field_len, term->field,
               (unsigned long)value_len, term->value);
  if (written < 0) {
    return NULL;
  }
  needed = (size_t)written + 1U;
  key = (char *)lc_pouch_alloc(allocator, needed);
  if (key == NULL) {
    return NULL;
  }
  (void)snprintf(key, needed, "eq:%lu:%s:%lu:%s:%lu:%s",
                 (unsigned long)namespace_len, req->namespace_name,
                 (unsigned long)field_len, term->field,
                 (unsigned long)value_len, term->value);
  return key;
}

static char *lc_pouch_index_exists_result_plan_key(
    const lc_pouch_allocator *allocator,
    const lc_pouch_query_index_scan_req *req) {
  const lc_pouch_document_exists_term *term;
  size_t namespace_len;
  size_t field_len;
  int written;
  size_t needed;
  char *key;

  if (!lc_pouch_index_result_plan_exists_cacheable(req)) {
    return NULL;
  }
  term = &req->document_exists_terms[0];
  namespace_len = strlen(req->namespace_name);
  field_len = strlen(term->field);
  written =
      snprintf(NULL, 0, "exists:%lu:%s:%lu:%s", (unsigned long)namespace_len,
               req->namespace_name, (unsigned long)field_len, term->field);
  if (written < 0) {
    return NULL;
  }
  needed = (size_t)written + 1U;
  key = (char *)lc_pouch_alloc(allocator, needed);
  if (key == NULL) {
    return NULL;
  }
  (void)snprintf(key, needed, "exists:%lu:%s:%lu:%s",
                 (unsigned long)namespace_len, req->namespace_name,
                 (unsigned long)field_len, term->field);
  return key;
}

static int lc_pouch_index_in_value_ptr_compare(const void *left,
                                               const void *right) {
  const char *left_value;
  const char *right_value;

  left_value = *(const char *const *)left;
  right_value = *(const char *const *)right;
  return strcmp(left_value, right_value);
}

static char *
lc_pouch_index_in_result_plan_key(const lc_pouch_allocator *allocator,
                                  const lc_pouch_query_index_scan_req *req) {
  const lc_pouch_document_in_term *term;
  const char **values;
  size_t namespace_len;
  size_t field_len;
  size_t value_count;
  size_t value_index;
  size_t unique_count;
  size_t total_len;
  int written;
  char *key;
  char *cursor;
  size_t offset;

  if (!lc_pouch_index_result_plan_in_cacheable(req)) {
    return NULL;
  }
  term = &req->document_in_terms[0];
  value_count = 0U;
  for (value_index = 0U; value_index < term->value_count; ++value_index) {
    if (term->values[value_index] != NULL) {
      value_count++;
    }
  }
  if (value_count == 0U || value_count > ((size_t)-1) / sizeof(values[0])) {
    return NULL;
  }
  values =
      (const char **)lc_pouch_alloc(allocator, value_count * sizeof(values[0]));
  if (values == NULL) {
    return NULL;
  }
  value_count = 0U;
  for (value_index = 0U; value_index < term->value_count; ++value_index) {
    if (term->values[value_index] != NULL) {
      values[value_count++] = term->values[value_index];
    }
  }
  qsort(values, value_count, sizeof(values[0]),
        lc_pouch_index_in_value_ptr_compare);
  unique_count = 0U;
  for (value_index = 0U; value_index < value_count; ++value_index) {
    if (unique_count > 0U &&
        strcmp(values[value_index], values[unique_count - 1U]) == 0) {
      continue;
    }
    values[unique_count++] = values[value_index];
  }
  namespace_len = strlen(req->namespace_name);
  field_len = strlen(term->field);
  written =
      snprintf(NULL, 0, "in:%lu:%s:%lu:%s:%lu", (unsigned long)namespace_len,
               req->namespace_name, (unsigned long)field_len, term->field,
               (unsigned long)unique_count);
  if (written < 0) {
    lc_pouch_free(allocator, values);
    return NULL;
  }
  total_len = (size_t)written;
  for (value_index = 0U; value_index < unique_count; ++value_index) {
    size_t value_len;

    value_len = strlen(values[value_index]);
    written = snprintf(NULL, 0, ":%lu:%s", (unsigned long)value_len,
                       values[value_index]);
    if (written < 0 || total_len > ((size_t)-1) - (size_t)written) {
      lc_pouch_free(allocator, values);
      return NULL;
    }
    total_len += (size_t)written;
  }
  if (total_len == (size_t)-1) {
    lc_pouch_free(allocator, values);
    return NULL;
  }
  key = (char *)lc_pouch_alloc(allocator, total_len + 1U);
  if (key == NULL) {
    lc_pouch_free(allocator, values);
    return NULL;
  }
  cursor = key;
  offset = 0U;
  written = snprintf(cursor, (total_len + 1U) - offset, "in:%lu:%s:%lu:%s:%lu",
                     (unsigned long)namespace_len, req->namespace_name,
                     (unsigned long)field_len, term->field,
                     (unsigned long)unique_count);
  offset += (size_t)written;
  cursor = key + offset;
  for (value_index = 0U; value_index < unique_count; ++value_index) {
    size_t value_len;

    value_len = strlen(values[value_index]);
    written = snprintf(cursor, (total_len + 1U) - offset, ":%lu:%s",
                       (unsigned long)value_len, values[value_index]);
    offset += (size_t)written;
    cursor = key + offset;
  }
  lc_pouch_free(allocator, values);
  return key;
}

static char *
lc_pouch_index_range_result_plan_key(const lc_pouch_allocator *allocator,
                                     const lc_pouch_query_index_scan_req *req) {
  const lc_pouch_document_range_term *term;
  const char *gt;
  const char *gte;
  const char *lt;
  const char *lte;
  size_t namespace_len;
  size_t field_len;
  size_t gt_len;
  size_t gte_len;
  size_t lt_len;
  size_t lte_len;
  int written;
  size_t needed;
  char *key;

  if (!lc_pouch_index_result_plan_range_cacheable(req)) {
    return NULL;
  }
  term = &req->document_range_terms[0];
  gt = term->gt != NULL ? term->gt : "";
  gte = term->gte != NULL ? term->gte : "";
  lt = term->lt != NULL ? term->lt : "";
  lte = term->lte != NULL ? term->lte : "";
  namespace_len = strlen(req->namespace_name);
  field_len = strlen(term->field);
  gt_len = strlen(gt);
  gte_len = strlen(gte);
  lt_len = strlen(lt);
  lte_len = strlen(lte);
  written = snprintf(NULL, 0,
                     "range:%lu:%s:%lu:%s:%d:%lu:%s:%d:%lu:%s:%d:%lu:%s:%d:%"
                     "lu:%s",
                     (unsigned long)namespace_len, req->namespace_name,
                     (unsigned long)field_len, term->field,
                     term->gt != NULL ? 1 : 0, (unsigned long)gt_len, gt,
                     term->gte != NULL ? 1 : 0, (unsigned long)gte_len, gte,
                     term->lt != NULL ? 1 : 0, (unsigned long)lt_len, lt,
                     term->lte != NULL ? 1 : 0, (unsigned long)lte_len, lte);
  if (written < 0) {
    return NULL;
  }
  needed = (size_t)written + 1U;
  key = (char *)lc_pouch_alloc(allocator, needed);
  if (key == NULL) {
    return NULL;
  }
  (void)snprintf(key, needed,
                 "range:%lu:%s:%lu:%s:%d:%lu:%s:%d:%lu:%s:%d:%lu:%s:%d:%lu:%s",
                 (unsigned long)namespace_len, req->namespace_name,
                 (unsigned long)field_len, term->field,
                 term->gt != NULL ? 1 : 0, (unsigned long)gt_len, gt,
                 term->gte != NULL ? 1 : 0, (unsigned long)gte_len, gte,
                 term->lt != NULL ? 1 : 0, (unsigned long)lt_len, lt,
                 term->lte != NULL ? 1 : 0, (unsigned long)lte_len, lte);
  return key;
}

static char *
lc_pouch_index_text_result_plan_key(const lc_pouch_allocator *allocator,
                                    const lc_pouch_query_index_scan_req *req,
                                    const char *kind, const char *field,
                                    const char *value, int ignore_case) {
  size_t namespace_len;
  size_t field_len;
  size_t value_len;
  int written;
  size_t needed;
  char *key;

  if (req == NULL || kind == NULL || field == NULL || value == NULL) {
    return NULL;
  }
  namespace_len = strlen(req->namespace_name);
  field_len = strlen(field);
  value_len = strlen(value);
  written = snprintf(NULL, 0, "%s:%lu:%s:%lu:%s:%d:%lu:%s", kind,
                     (unsigned long)namespace_len, req->namespace_name,
                     (unsigned long)field_len, field, ignore_case ? 1 : 0,
                     (unsigned long)value_len, value);
  if (written < 0) {
    return NULL;
  }
  needed = (size_t)written + 1U;
  key = (char *)lc_pouch_alloc(allocator, needed);
  if (key == NULL) {
    return NULL;
  }
  (void)snprintf(key, needed, "%s:%lu:%s:%lu:%s:%d:%lu:%s", kind,
                 (unsigned long)namespace_len, req->namespace_name,
                 (unsigned long)field_len, field, ignore_case ? 1 : 0,
                 (unsigned long)value_len, value);
  return key;
}

static char *lc_pouch_index_prefix_result_plan_key(
    const lc_pouch_allocator *allocator,
    const lc_pouch_query_index_scan_req *req) {
  const lc_pouch_document_prefix_term *term;

  if (!lc_pouch_index_result_plan_prefix_cacheable(req)) {
    return NULL;
  }
  term = &req->document_prefix_terms[0];
  return lc_pouch_index_text_result_plan_key(
      allocator, req, "prefix", term->field, term->value, term->ignore_case);
}

static char *lc_pouch_index_contains_result_plan_key(
    const lc_pouch_allocator *allocator,
    const lc_pouch_query_index_scan_req *req) {
  const lc_pouch_document_contains_term *term;

  if (!lc_pouch_index_result_plan_contains_cacheable(req)) {
    return NULL;
  }
  term = &req->document_contains_terms[0];
  return lc_pouch_index_text_result_plan_key(
      allocator, req, "contains", term->field, term->value, term->ignore_case);
}

char *lc_pouch_index_result_plan_key(const lc_pouch_allocator *allocator,
                                     const lc_pouch_query_index_scan_req *req,
                                     lc_pouch_index_result_plan_kind kind) {
  switch (kind) {
  case LC_POUCH_INDEX_RESULT_PLAN_EQ:
    return lc_pouch_index_eq_result_plan_key(allocator, req);
  case LC_POUCH_INDEX_RESULT_PLAN_EXISTS:
    return lc_pouch_index_exists_result_plan_key(allocator, req);
  case LC_POUCH_INDEX_RESULT_PLAN_IN:
    return lc_pouch_index_in_result_plan_key(allocator, req);
  case LC_POUCH_INDEX_RESULT_PLAN_RANGE:
    return lc_pouch_index_range_result_plan_key(allocator, req);
  case LC_POUCH_INDEX_RESULT_PLAN_PREFIX:
    return lc_pouch_index_prefix_result_plan_key(allocator, req);
  case LC_POUCH_INDEX_RESULT_PLAN_CONTAINS:
    return lc_pouch_index_contains_result_plan_key(allocator, req);
  }
  return NULL;
}

static int lc_pouch_index_result_cache_compare(uint64_t left_generation,
                                               const char *left_plan_key,
                                               uint64_t right_generation,
                                               const char *right_plan_key) {
  int cmp;

  if (left_generation < right_generation) {
    return -1;
  }
  if (left_generation > right_generation) {
    return 1;
  }
  cmp = strcmp(left_plan_key, right_plan_key);
  if (cmp < 0) {
    return -1;
  }
  if (cmp > 0) {
    return 1;
  }
  return 0;
}

static int lc_pouch_index_result_cache_find_position(
    const lc_pouch_index_result_cache *cache, uint64_t generation,
    const char *plan_key, size_t *position_out) {
  size_t low;
  size_t high;

  if (position_out != NULL) {
    *position_out = 0U;
  }
  if (cache == NULL || plan_key == NULL) {
    return 0;
  }
  low = 0U;
  high = cache->count;
  while (low < high) {
    size_t mid;
    int cmp;

    mid = low + ((high - low) / 2U);
    cmp = lc_pouch_index_result_cache_compare(cache->entries[mid].generation,
                                              cache->entries[mid].plan_key,
                                              generation, plan_key);
    if (cmp < 0) {
      low = mid + 1U;
    } else {
      high = mid;
    }
  }
  if (position_out != NULL) {
    *position_out = low;
  }
  return low < cache->count &&
         lc_pouch_index_result_cache_compare(cache->entries[low].generation,
                                             cache->entries[low].plan_key,
                                             generation, plan_key) == 0;
}

static int
lc_pouch_index_result_cache_reserve(const lc_pouch_allocator *allocator,
                                    lc_pouch_index_result_cache *cache,
                                    size_t needed) {
  size_t new_capacity;
  lc_pouch_index_result_cache_entry *grown;

  if (cache == NULL) {
    return 0;
  }
  if (needed <= cache->capacity) {
    return 1;
  }
  new_capacity = cache->capacity == 0U ? 16U : cache->capacity;
  while (new_capacity < needed) {
    if (new_capacity > ((size_t)-1) / 2U) {
      return 0;
    }
    new_capacity *= 2U;
  }
  if (new_capacity > ((size_t)-1) / sizeof(cache->entries[0])) {
    return 0;
  }
  grown = (lc_pouch_index_result_cache_entry *)lc_pouch_realloc(
      allocator, cache->entries, new_capacity * sizeof(cache->entries[0]));
  if (grown == NULL) {
    return 0;
  }
  cache->entries = grown;
  cache->capacity = new_capacity;
  return 1;
}

void lc_pouch_index_result_cache_cleanup(const lc_pouch_allocator *allocator,
                                         lc_pouch_index_result_cache *cache) {
  size_t index;

  if (cache == NULL) {
    return;
  }
  for (index = 0U; index < cache->count; ++index) {
    lc_pouch_free(allocator, cache->entries[index].plan_key);
    lc_pouch_index_doc_id_set_cleanup(allocator,
                                      &cache->entries[index].doc_ids);
  }
  lc_pouch_free(allocator, cache->entries);
  memset(cache, 0, sizeof(*cache));
}

void lc_pouch_index_result_page_cleanup(const lc_pouch_allocator *allocator,
                                        lc_pouch_index_result_page *page) {
  if (page == NULL) {
    return;
  }
  lc_pouch_index_doc_id_set_cleanup(allocator, &page->doc_ids);
  lc_pouch_free(allocator, page->next_start_after);
  memset(page, 0, sizeof(*page));
}

int lc_pouch_index_result_page_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_index_doc_table *doc_table,
    const lc_pouch_index_doc_id_set *doc_ids, const char *namespace_name,
    const char *start_after, size_t limit, lc_pouch_index_result_page *page,
    int *invalid_doc_id_out) {
  size_t index;

  if (page == NULL) {
    return 0;
  }
  memset(page, 0, sizeof(*page));
  if (invalid_doc_id_out != NULL) {
    *invalid_doc_id_out = 0;
  }
  if (doc_table == NULL || doc_ids == NULL || namespace_name == NULL) {
    return 1;
  }
  for (index = 0U; index < doc_ids->count; ++index) {
    const char *doc_namespace;
    const char *key;

    if (!lc_pouch_index_doc_table_lookup(doc_table, doc_ids->items[index],
                                         &doc_namespace, &key)) {
      if (invalid_doc_id_out != NULL) {
        *invalid_doc_id_out = 1;
      }
      lc_pouch_index_result_page_cleanup(allocator, page);
      return 0;
    }
    if (strcmp(doc_namespace, namespace_name) != 0) {
      continue;
    }
    if (start_after != NULL && strcmp(key, start_after) <= 0) {
      continue;
    }
    if (limit > 0U && page->doc_ids.count == limit) {
      page->next_start_after = lc_pouch_strdup(
          allocator,
          doc_table->entries[page->doc_ids.items[page->doc_ids.count - 1U]]
              .key);
      if (page->next_start_after == NULL) {
        lc_pouch_index_result_page_cleanup(allocator, page);
        return 0;
      }
      page->truncated = 1;
      return 1;
    }
    if (!lc_pouch_index_doc_id_set_append(allocator, &page->doc_ids,
                                          doc_ids->items[index])) {
      lc_pouch_index_result_page_cleanup(allocator, page);
      return 0;
    }
  }
  return 1;
}

int lc_pouch_index_result_cache_find(const lc_pouch_allocator *allocator,
                                     lc_pouch_index_result_cache *cache,
                                     uint64_t generation, const char *plan_key,
                                     lc_pouch_index_doc_id_set *dst) {
  size_t position;

  if (dst == NULL) {
    return 0;
  }
  dst->count = 0U;
  if (!lc_pouch_index_result_cache_find_position(cache, generation, plan_key,
                                                 &position)) {
    if (cache != NULL && plan_key != NULL) {
      cache->misses++;
    }
    return 0;
  }
  if (!lc_pouch_index_doc_id_set_clone(allocator, dst,
                                       &cache->entries[position].doc_ids)) {
    return 0;
  }
  cache->hits++;
  return 1;
}

int lc_pouch_index_result_cache_put(const lc_pouch_allocator *allocator,
                                    lc_pouch_index_result_cache *cache,
                                    uint64_t generation, const char *plan_key,
                                    const lc_pouch_index_doc_id_set *doc_ids) {
  lc_pouch_index_doc_id_set copy;
  char *plan_key_copy;
  size_t position;

  if (cache == NULL || plan_key == NULL || doc_ids == NULL) {
    return 0;
  }
  memset(&copy, 0, sizeof(copy));
  if (!lc_pouch_index_doc_id_set_clone(allocator, &copy, doc_ids) ||
      !lc_pouch_index_doc_id_set_sort_unique(&copy)) {
    lc_pouch_index_doc_id_set_cleanup(allocator, &copy);
    return 0;
  }
  if (lc_pouch_index_result_cache_find_position(cache, generation, plan_key,
                                                &position)) {
    lc_pouch_index_doc_id_set_cleanup(allocator,
                                      &cache->entries[position].doc_ids);
    cache->entries[position].doc_ids = copy;
    cache->puts++;
    cache->replacements++;
    return 1;
  }
  plan_key_copy = lc_pouch_strdup(allocator, plan_key);
  if (plan_key_copy == NULL) {
    lc_pouch_index_doc_id_set_cleanup(allocator, &copy);
    return 0;
  }
  if (!lc_pouch_index_result_cache_reserve(allocator, cache,
                                           cache->count + 1U)) {
    lc_pouch_free(allocator, plan_key_copy);
    lc_pouch_index_doc_id_set_cleanup(allocator, &copy);
    return 0;
  }
  if (position < cache->count) {
    memmove(&cache->entries[position + 1U], &cache->entries[position],
            (cache->count - position) * sizeof(cache->entries[0]));
  }
  cache->entries[position].generation = generation;
  cache->entries[position].plan_key = plan_key_copy;
  cache->entries[position].doc_ids = copy;
  cache->count++;
  cache->puts++;
  return 1;
}

int lc_pouch_index_cached_result_page(
    const lc_pouch_allocator *allocator,
    const lc_pouch_index_doc_table *doc_table,
    lc_pouch_index_result_cache *cache, uint64_t generation,
    const lc_pouch_query_index_scan_req *req,
    lc_pouch_index_result_plan_kind kind,
    lc_pouch_index_result_collect_doc_ids_fn collect, void *collect_context,
    lc_pouch_index_result_page *page, int *invalid_doc_id_out,
    lc_error *error) {
  lc_pouch_index_doc_id_set doc_ids;
  char *plan_key;
  int cacheable;
  int cache_hit;
  int invalid_doc_id;
  int rc;

  if (page == NULL || collect == NULL) {
    return LC_ERR_INVALID;
  }
  memset(&doc_ids, 0, sizeof(doc_ids));
  invalid_doc_id = 0;
  plan_key = lc_pouch_index_result_plan_key(allocator, req, kind);
  cacheable = plan_key != NULL;
  cache_hit = cacheable &&
              lc_pouch_index_result_cache_find(allocator, cache, generation,
                                               plan_key, &doc_ids);
  if (!cache_hit) {
    rc = collect(collect_context, cacheable, &doc_ids, error);
    if (rc != LC_OK) {
      lc_pouch_free(allocator, plan_key);
      lc_pouch_index_doc_id_set_cleanup(allocator, &doc_ids);
      return rc;
    }
    if (cacheable && !lc_pouch_index_result_cache_put(
                         allocator, cache, generation, plan_key, &doc_ids)) {
      lc_pouch_free(allocator, plan_key);
      lc_pouch_index_doc_id_set_cleanup(allocator, &doc_ids);
      return LC_ERR_NOMEM;
    }
  }
  if (!lc_pouch_index_result_page_doc_ids(
          allocator, doc_table, &doc_ids,
          req != NULL ? req->namespace_name : NULL,
          req != NULL ? req->start_after : NULL, req != NULL ? req->limit : 0U,
          page, &invalid_doc_id)) {
    if (invalid_doc_id_out != NULL) {
      *invalid_doc_id_out = invalid_doc_id;
    }
    if (invalid_doc_id) {
      lc_pouch_free(allocator, plan_key);
      lc_pouch_index_doc_id_set_cleanup(allocator, &doc_ids);
      return LC_ERR_INVALID;
    }
    lc_pouch_free(allocator, plan_key);
    lc_pouch_index_doc_id_set_cleanup(allocator, &doc_ids);
    return LC_ERR_NOMEM;
  }
  if (invalid_doc_id_out != NULL) {
    *invalid_doc_id_out = 0;
  }
  lc_pouch_free(allocator, plan_key);
  lc_pouch_index_doc_id_set_cleanup(allocator, &doc_ids);
  return LC_OK;
}

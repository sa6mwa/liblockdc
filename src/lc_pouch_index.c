#include "lc_pouch_index.h"

#include <string.h>

int lc_pouch_index_collect_eq_term_doc_ids(
    const lc_pouch_allocator *allocator, const lc_pouch_document_eq_term *term,
    lc_pouch_index_exact_term_doc_ids_fn read_exact, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error) {
  int rc;

  if (term == NULL || term->field == NULL || term->value == NULL ||
      read_exact == NULL || doc_ids == NULL) {
    return LC_OK;
  }
  rc = read_exact(read_context, term->field, term->value, doc_ids, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!lc_pouch_index_doc_id_set_sort_unique(doc_ids)) {
    return LC_ERR_NOMEM;
  }
  (void)allocator;
  (void)error;
  return LC_OK;
}

int lc_pouch_index_collect_in_term_doc_ids(
    const lc_pouch_allocator *allocator, const lc_pouch_document_in_term *term,
    lc_pouch_index_exact_term_doc_ids_fn read_exact, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error) {
  size_t value_index;
  int rc;

  if (term == NULL || term->field == NULL || term->values == NULL ||
      term->value_count == 0U || read_exact == NULL || doc_ids == NULL) {
    return LC_OK;
  }
  for (value_index = 0U; value_index < term->value_count; ++value_index) {
    if (term->values[value_index] == NULL) {
      continue;
    }
    rc = read_exact(read_context, term->field, term->values[value_index],
                    doc_ids, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  if (!lc_pouch_index_doc_id_set_sort_unique(doc_ids)) {
    return LC_ERR_NOMEM;
  }
  (void)allocator;
  (void)error;
  return LC_OK;
}

int lc_pouch_index_collect_exists_term_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_exists_term *term,
    lc_pouch_index_exists_term_doc_ids_fn read_exists, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error) {
  int rc;

  if (term == NULL || term->field == NULL || read_exists == NULL ||
      doc_ids == NULL) {
    return LC_OK;
  }
  rc = read_exists(read_context, term->field, doc_ids, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!lc_pouch_index_doc_id_set_sort_unique(doc_ids)) {
    return LC_ERR_NOMEM;
  }
  (void)allocator;
  (void)error;
  return LC_OK;
}

int lc_pouch_index_collect_range_term_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_range_term *term,
    lc_pouch_index_range_term_doc_ids_fn read_range, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error) {
  int rc;

  if (term == NULL || term->field == NULL || read_range == NULL ||
      doc_ids == NULL) {
    return LC_OK;
  }
  rc = read_range(read_context, term, doc_ids, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!lc_pouch_index_doc_id_set_sort_unique(doc_ids)) {
    return LC_ERR_NOMEM;
  }
  (void)allocator;
  (void)error;
  return LC_OK;
}

int lc_pouch_index_collect_range_term_with_eq_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_range_term *range_term,
    const lc_pouch_document_eq_term *eq_terms, size_t eq_term_count,
    lc_pouch_index_range_term_doc_ids_fn read_range,
    lc_pouch_index_exact_term_doc_ids_fn read_exact, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error) {
  size_t index;
  int rc;

  rc = lc_pouch_index_collect_range_term_doc_ids(
      allocator, range_term, read_range, read_context, doc_ids, error);
  if (rc != LC_OK || doc_ids == NULL || eq_term_count == 0U) {
    return rc;
  }
  if (eq_terms == NULL || read_exact == NULL) {
    doc_ids->count = 0U;
    return LC_OK;
  }
  for (index = 0U; index < eq_term_count; ++index) {
    lc_pouch_index_doc_id_set eq_doc_ids;
    lc_pouch_index_doc_id_set intersected;
    const lc_pouch_document_eq_term *term;

    term = &eq_terms[index];
    if (term->field == NULL || term->value == NULL) {
      doc_ids->count = 0U;
      return LC_OK;
    }
    memset(&eq_doc_ids, 0, sizeof(eq_doc_ids));
    memset(&intersected, 0, sizeof(intersected));
    rc = read_exact(read_context, term->field, term->value, &eq_doc_ids, error);
    if (rc != LC_OK) {
      lc_pouch_index_doc_id_set_cleanup(allocator, &eq_doc_ids);
      return rc;
    }
    if (!lc_pouch_index_doc_id_set_sort_unique(&eq_doc_ids) ||
        !lc_pouch_index_doc_id_set_intersect(allocator, &intersected, doc_ids,
                                             &eq_doc_ids)) {
      lc_pouch_index_doc_id_set_cleanup(allocator, &intersected);
      lc_pouch_index_doc_id_set_cleanup(allocator, &eq_doc_ids);
      return LC_ERR_NOMEM;
    }
    lc_pouch_index_doc_id_set_cleanup(allocator, doc_ids);
    *doc_ids = intersected;
    lc_pouch_index_doc_id_set_cleanup(allocator, &eq_doc_ids);
    if (doc_ids->count == 0U) {
      return LC_OK;
    }
  }
  (void)error;
  return LC_OK;
}

int lc_pouch_index_collect_prefix_term_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_prefix_term *term,
    lc_pouch_index_prefix_term_doc_ids_fn read_prefix, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error) {
  int rc;

  if (term == NULL || term->field == NULL || term->value == NULL ||
      read_prefix == NULL || doc_ids == NULL) {
    return LC_OK;
  }
  rc = read_prefix(read_context, term, doc_ids, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!lc_pouch_index_doc_id_set_sort_unique(doc_ids)) {
    return LC_ERR_NOMEM;
  }
  (void)allocator;
  (void)error;
  return LC_OK;
}

int lc_pouch_index_collect_contains_term_doc_ids(
    const lc_pouch_allocator *allocator,
    const lc_pouch_document_contains_term *term,
    lc_pouch_index_contains_term_doc_ids_fn read_contains, void *read_context,
    lc_pouch_index_doc_id_set *doc_ids, lc_error *error) {
  int rc;

  if (term == NULL || term->field == NULL || term->value == NULL ||
      read_contains == NULL || doc_ids == NULL) {
    return LC_OK;
  }
  rc = read_contains(read_context, term, doc_ids, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!lc_pouch_index_doc_id_set_sort_unique(doc_ids)) {
    return LC_ERR_NOMEM;
  }
  (void)allocator;
  (void)error;
  return LC_OK;
}

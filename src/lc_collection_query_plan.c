#include "lc_collection_query.h"
#include "lc_pouch_query_plan.h"

#include <string.h>

static int lc_cqp_path(lql_string_view *field, int *prefix) {
  size_t i, start, end;
  if (field->data == NULL || field->len == 0U || field->data[0] != '/' ||
      field->len > 65536U || memchr(field->data, '\0', field->len) != NULL)
    return 0;
  for (start = 1U; start < field->len; start = end + 1U) {
    end = start;
    while (end < field->len && field->data[end] != '/')
      ++end;
    if (end - start == 3U && memcmp(field->data + start, "...", 3U) == 0)
      return 0;
    for (i = start; i < end; ++i)
      if (field->data[i] == '[' || field->data[i] == ']' ||
          field->data[i] == '*') {
        field->len = start;
        *prefix = 1;
        return 1;
      }
    if (end - start > 1U && field->data[start] == '0' &&
        field->data[start + 1U] >= '0' && field->data[start + 1U] <= '9')
      return 0;
  }
  return 1;
}

void lc_collection_query_plan_cleanup(lc_collection_query_plan *plan) {
  lc_pouch_index_term_keys_cleanup(&plan->allocator, plan->keys, plan->count);
  memset(plan, 0, sizeof(*plan));
}

int lc_collection_query_plan_init(lc_collection_query_plan *plan,
                                  const lc_allocator *allocator, lql *runtime,
                                  const lql_selector *selector,
                                  lc_error *error) {
  lql_selector_node root;
  lql_selector_string_term scalar;
  lql_string_view field;
  lql_error query_error;
  lql_status status;
  lc_pouch_index_plain_term term;
  lc_error attempt;
  char *path;
  int rc;
  memset(plan, 0, sizeof(*plan));
  if (allocator != NULL)
    plan->allocator = *allocator;
  if (selector == NULL)
    return LC_OK;
  lql_error_init(&query_error);
  status = runtime->selector_root(runtime, selector, &root, &query_error);
  if (status != LQL_STATUS_OK)
    return LC_OK;
  memset(&field, 0, sizeof(field));
  if (root.kind == LQL_SELECTOR_NODE_EQ) {
    status = runtime->selector_node_string_term(runtime, root, &scalar,
                                                &query_error);
    if (status != LQL_STATUS_OK || !scalar.value_present ||
        scalar.any_count != 0U || scalar.value.len > 65536U ||
        (scalar.value.len != 0U &&
         (scalar.value.data == NULL ||
          memchr(scalar.value.data, '\0', scalar.value.len) != NULL)))
      return LC_OK;
    field = scalar.field;
  } else if (root.kind == LQL_SELECTOR_NODE_EXISTS) {
    status =
        runtime->selector_node_exists_path(runtime, root, &field, &query_error);
    if (status != LQL_STATUS_OK)
      return LC_OK;
    plan->presence = 1;
  } else
    return LC_OK;
  if (!lc_cqp_path(&field, &plan->prefix))
    return LC_OK;
  path = (char *)lc_alloc_with_allocator(&plan->allocator, field.len + 1U);
  if (path == NULL)
    return lc_collection_failure(
        error, LC_ERR_NOMEM, NULL,
        "failed to allocate collection indexed selector path");
  memcpy(path, field.data, field.len);
  path[field.len] = '\0';
  lc_error_init(&attempt);
  if (plan->presence) {
    term.field = path;
    term.value = "";
    term.value_type = 's';
    rc = lc_pouch_index_term_keys_build_exact(
        &term, 1U, &plan->keys, &plan->count, &plan->allocator, &attempt);
  } else
    rc = lc_pouch_query_selector_exact_key(runtime, selector, path,
                                           &plan->allocator, &plan->keys,
                                           &plan->count, &attempt);
  lc_free_with_allocator(&plan->allocator, path);
  if (rc != LC_OK)
    lc_collection_query_plan_cleanup(plan);
  if (rc == LC_ERR_NOMEM && error != NULL) {
    lc_error_cleanup(error);
    *error = attempt;
    memset(&attempt, 0, sizeof(attempt));
  }
  lc_error_cleanup(&attempt);
  return rc == LC_ERR_NOMEM ? rc : LC_OK;
}

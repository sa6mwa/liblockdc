#include "lc_pouch_store.h"

#include <stdlib.h>
#include <string.h>

static void *lc_pouch_default_malloc(void *context, size_t size) {
  (void)context;
  return malloc(size);
}

static void *lc_pouch_default_calloc(void *context, size_t count, size_t size) {
  (void)context;
  return calloc(count, size);
}

static void *lc_pouch_default_realloc(void *context, void *ptr, size_t size) {
  (void)context;
  return realloc(ptr, size);
}

static void lc_pouch_default_free(void *context, void *ptr) {
  (void)context;
  free(ptr);
}

void lc_pouch_allocator_from_lc(const lc_allocator *src,
                                lc_pouch_allocator *dst) {
  if (dst == NULL) {
    return;
  }
  memset(dst, 0, sizeof(*dst));
  if (src == NULL || src->malloc_fn == NULL || src->free_fn == NULL) {
    return;
  }
  dst->malloc_fn = src->malloc_fn;
  dst->realloc_fn = src->realloc_fn;
  dst->free_fn = src->free_fn;
  dst->context = src->context;
}

void *lc_pouch_alloc(const lc_pouch_allocator *allocator, size_t size) {
  if (allocator != NULL && allocator->malloc_fn != NULL) {
    return allocator->malloc_fn(allocator->context, size);
  }
  return lc_pouch_default_malloc(NULL, size);
}

void *lc_pouch_calloc(const lc_pouch_allocator *allocator, size_t count,
                      size_t size) {
  void *ptr;
  size_t bytes;

  if (allocator != NULL && allocator->calloc_fn != NULL) {
    return allocator->calloc_fn(allocator->context, count, size);
  }
  if (allocator == NULL || allocator->malloc_fn == NULL) {
    return lc_pouch_default_calloc(NULL, count, size);
  }
  if (count != 0U && size > ((size_t)-1) / count) {
    return NULL;
  }
  bytes = count * size;
  ptr = lc_pouch_alloc(allocator, bytes);
  if (ptr != NULL) {
    memset(ptr, 0, bytes);
  }
  return ptr;
}

void *lc_pouch_realloc(const lc_pouch_allocator *allocator, void *ptr,
                       size_t size) {
  if (allocator != NULL && allocator->realloc_fn != NULL) {
    return allocator->realloc_fn(allocator->context, ptr, size);
  }
  return lc_pouch_default_realloc(NULL, ptr, size);
}

void lc_pouch_free(const lc_pouch_allocator *allocator, void *ptr) {
  if (ptr == NULL) {
    return;
  }
  if (allocator != NULL && allocator->free_fn != NULL) {
    allocator->free_fn(allocator->context, ptr);
    return;
  }
  lc_pouch_default_free(NULL, ptr);
}

char *lc_pouch_strdup(const lc_pouch_allocator *allocator, const char *value) {
  size_t length;
  char *copy;

  if (value == NULL) {
    return NULL;
  }
  length = strlen(value);
  copy = (char *)lc_pouch_alloc(allocator, length + 1U);
  if (copy == NULL) {
    return NULL;
  }
  memcpy(copy, value, length + 1U);
  return copy;
}

char *lc_pouch_dup_bytes(const lc_pouch_allocator *allocator, const void *bytes,
                         size_t length) {
  char *copy;

  copy = (char *)lc_pouch_alloc(allocator, length + 1U);
  if (copy == NULL) {
    return NULL;
  }
  if (length > 0U) {
    memcpy(copy, bytes, length);
  }
  copy[length] = '\0';
  return copy;
}

void lc_pouch_state_info_cleanup(const lc_pouch_allocator *allocator,
                                 lc_pouch_state_info *info) {
  if (info == NULL) {
    return;
  }
  lc_pouch_free(allocator, info->content_type);
  lc_pouch_free(allocator, info->etag);
  memset(info, 0, sizeof(*info));
}

void lc_pouch_put_state_res_cleanup(const lc_pouch_allocator *allocator,
                                    lc_pouch_put_state_res *res) {
  if (res == NULL) {
    return;
  }
  lc_pouch_free(allocator, res->new_state_etag);
  memset(res, 0, sizeof(*res));
}

void lc_pouch_meta_cleanup(const lc_pouch_allocator *allocator,
                           lc_pouch_meta *meta) {
  if (meta == NULL) {
    return;
  }
  lc_pouch_free(allocator, meta->owner);
  lc_pouch_free(allocator, meta->lease_id);
  lc_pouch_free(allocator, meta->txn_id);
  lc_pouch_free(allocator, meta->state_etag);
  memset(meta, 0, sizeof(*meta));
}

void lc_pouch_meta_record_cleanup(const lc_pouch_allocator *allocator,
                                  lc_pouch_meta_record *record) {
  if (record == NULL) {
    return;
  }
  lc_pouch_free(allocator, record->namespace_name);
  lc_pouch_free(allocator, record->key);
  lc_pouch_free(allocator, record->etag);
  lc_pouch_meta_cleanup(allocator, &record->meta);
  memset(record, 0, sizeof(*record));
}

void lc_pouch_store_meta_res_cleanup(const lc_pouch_allocator *allocator,
                                     lc_pouch_store_meta_res *res) {
  if (res == NULL) {
    return;
  }
  lc_pouch_free(allocator, res->etag);
  memset(res, 0, sizeof(*res));
}

void lc_pouch_object_info_cleanup(const lc_pouch_allocator *allocator,
                                  lc_pouch_object_info *info) {
  if (info == NULL) {
    return;
  }
  lc_pouch_free(allocator, info->id);
  lc_pouch_free(allocator, info->name);
  lc_pouch_free(allocator, info->plaintext_sha256);
  lc_pouch_free(allocator, info->content_type);
  memset(info, 0, sizeof(*info));
}

void lc_pouch_object_list_cleanup(const lc_pouch_allocator *allocator,
                                  lc_pouch_object_list *list) {
  size_t index;

  if (list == NULL) {
    return;
  }
  for (index = 0U; index < list->count; ++index) {
    lc_pouch_object_info_cleanup(allocator, &list->items[index]);
  }
  lc_pouch_free(allocator, list->items);
  memset(list, 0, sizeof(*list));
}

#include "lc_internal.h"

#include <stdlib.h>
#include <string.h>

static void *lc_engine_default_malloc(void *context, size_t size) {
  (void)context;
  return malloc(size);
}

static void *lc_engine_default_realloc(void *context, void *ptr, size_t size) {
  (void)context;
  return realloc(ptr, size);
}

static void lc_engine_default_free(void *context, void *ptr) {
  (void)context;
  free(ptr);
}

void lc_engine_allocator_init(lc_engine_allocator *allocator) {
  if (allocator == NULL) {
    return;
  }
  allocator->malloc_fn = NULL;
  allocator->realloc_fn = NULL;
  allocator->free_fn = NULL;
  allocator->context = NULL;
}

int lc_engine_client_set_allocator(lc_engine_client *client,
                                   const lc_engine_allocator *allocator,
                                   lc_engine_error *error) {
  if (client == NULL || allocator == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "set_allocator requires client, allocator, and error");
  }
  if (allocator->malloc_fn == NULL || allocator->free_fn == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "allocator requires malloc_fn and free_fn");
  }
  client->allocator = *allocator;
  if (client->allocator.realloc_fn == NULL) {
    client->allocator.realloc_fn = lc_engine_default_realloc;
  }
  return LC_ENGINE_OK;
}

int lc_engine_client_get_allocator(lc_engine_client *client,
                                   lc_engine_allocator *allocator,
                                   lc_engine_error *error) {
  if (client == NULL || allocator == NULL || error == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "get_allocator requires client, allocator, and error");
  }
  *allocator = client->allocator;
  if (allocator->malloc_fn == NULL) {
    allocator->malloc_fn = lc_engine_default_malloc;
    allocator->realloc_fn = lc_engine_default_realloc;
    allocator->free_fn = lc_engine_default_free;
    allocator->context = NULL;
  }
  return LC_ENGINE_OK;
}

void *lc_engine_client_alloc(lc_engine_client *client, size_t size) {
  if (client != NULL && client->allocator.malloc_fn != NULL) {
    return client->allocator.malloc_fn(client->allocator.context, size);
  }
  return lc_engine_default_malloc(NULL, size);
}

void lc_engine_client_free_alloc(lc_engine_client *client, void *ptr) {
  if (ptr == NULL) {
    return;
  }
  if (client != NULL && client->allocator.free_fn != NULL) {
    client->allocator.free_fn(client->allocator.context, ptr);
    return;
  }
  lc_engine_default_free(NULL, ptr);
}

char *lc_engine_client_strdup(lc_engine_client *client, const char *value) {
  char *copy;
  size_t length;

  if (value == NULL) {
    return NULL;
  }
  length = strlen(value);
  copy = (char *)lc_engine_client_alloc(client, length + 1U);
  if (copy == NULL) {
    return NULL;
  }
  memcpy(copy, value, length + 1U);
  return copy;
}

char *lc_engine_client_strdup_range(lc_engine_client *client, const char *begin,
                                    const char *end) {
  char *copy;
  size_t length;

  if (begin == NULL || end == NULL || end < begin) {
    return NULL;
  }
  length = (size_t)(end - begin);
  copy = (char *)lc_engine_client_alloc(client, length + 1U);
  if (copy == NULL) {
    return NULL;
  }
  if (length > 0U) {
    memcpy(copy, begin, length);
  }
  copy[length] = '\0';
  return copy;
}

void lc_engine_query_stream_response_cleanup(
    lc_engine_client *client, lc_engine_query_stream_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_client_free_alloc(client, response->cursor);
  response->cursor = NULL;
  lc_engine_client_free_alloc(client, response->correlation_id);
  response->correlation_id = NULL;
  lc_engine_client_free_alloc(client, response->metadata_json);
  response->metadata_json = NULL;
  lc_engine_client_free_alloc(client, response->return_mode);
  response->return_mode = NULL;
  response->index_seq = 0UL;
  response->http_status = 0L;
}

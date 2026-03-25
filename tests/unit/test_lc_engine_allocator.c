#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include <cmocka.h>

#include "lc_engine_api.h"

void lc_engine_client_free_alloc(lc_engine_client *client, void *ptr);
char *lc_engine_client_strdup(lc_engine_client *client, const char *value);
char *lc_engine_client_strdup_range(lc_engine_client *client, const char *begin,
                                    const char *end);

typedef struct tracked_engine_allocator_state {
  size_t malloc_calls;
  size_t realloc_calls;
  size_t free_calls;
} tracked_engine_allocator_state;

static void *tracked_engine_malloc(void *context, size_t size) {
  tracked_engine_allocator_state *state;

  state = (tracked_engine_allocator_state *)context;
  state->malloc_calls++;
  return malloc(size);
}

static void *tracked_engine_realloc(void *context, void *ptr, size_t size) {
  tracked_engine_allocator_state *state;

  state = (tracked_engine_allocator_state *)context;
  state->realloc_calls++;
  return realloc(ptr, size);
}

static void tracked_engine_free(void *context, void *ptr) {
  tracked_engine_allocator_state *state;

  state = (tracked_engine_allocator_state *)context;
  state->free_calls++;
  free(ptr);
}

static void init_engine_client_config(lc_engine_client_config *config) {
  static const char *endpoints[] = {"http://127.0.0.1:1"};

  memset(config, 0, sizeof(*config));
  config->endpoints = endpoints;
  config->endpoint_count = sizeof(endpoints) / sizeof(endpoints[0]);
  config->disable_mtls = 1;
  config->timeout_ms = 100L;
  config->prefer_http_2 = 0;
}

static void test_engine_get_allocator_returns_default_hooks(void **state) {
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_allocator allocator;
  lc_engine_error error;
  int rc;

  (void)state;
  client = NULL;
  memset(&allocator, 0, sizeof(allocator));
  memset(&error, 0, sizeof(error));
  init_engine_client_config(&config);

  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_non_null(client);

  rc = lc_engine_client_get_allocator(client, &allocator, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_non_null(allocator.malloc_fn);
  assert_non_null(allocator.realloc_fn);
  assert_non_null(allocator.free_fn);
  assert_null(allocator.context);

  lc_engine_client_close(client);
  lc_engine_error_cleanup(&error);
}

static void
test_engine_set_allocator_validates_and_uses_custom_hooks(void **state) {
  lc_engine_client_config config;
  lc_engine_client *client;
  lc_engine_allocator allocator;
  lc_engine_allocator observed;
  lc_engine_query_stream_response response;
  lc_engine_error error;
  tracked_engine_allocator_state alloc_state;
  char *copy;
  const char *text;
  int rc;

  (void)state;
  client = NULL;
  memset(&allocator, 0, sizeof(allocator));
  memset(&observed, 0, sizeof(observed));
  memset(&response, 0, sizeof(response));
  memset(&error, 0, sizeof(error));
  memset(&alloc_state, 0, sizeof(alloc_state));
  init_engine_client_config(&config);

  rc = lc_engine_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_non_null(client);

  rc = lc_engine_client_set_allocator(NULL, &allocator, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_INVALID_ARGUMENT);
  lc_engine_error_cleanup(&error);

  rc = lc_engine_client_get_allocator(NULL, &observed, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_INVALID_ARGUMENT);
  lc_engine_error_cleanup(&error);

  allocator.malloc_fn = tracked_engine_malloc;
  allocator.context = &alloc_state;
  rc = lc_engine_client_set_allocator(client, &allocator, &error);
  assert_int_equal(rc, LC_ENGINE_ERROR_INVALID_ARGUMENT);
  lc_engine_error_cleanup(&error);

  allocator.free_fn = tracked_engine_free;
  rc = lc_engine_client_set_allocator(client, &allocator, &error);
  assert_int_equal(rc, LC_ENGINE_OK);

  rc = lc_engine_client_get_allocator(client, &observed, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_ptr_equal(observed.malloc_fn, tracked_engine_malloc);
  assert_ptr_equal(observed.free_fn, tracked_engine_free);
  assert_non_null(observed.realloc_fn);
  assert_ptr_equal(observed.context, &alloc_state);

  allocator.realloc_fn = tracked_engine_realloc;
  rc = lc_engine_client_set_allocator(client, &allocator, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  rc = lc_engine_client_get_allocator(client, &observed, &error);
  assert_int_equal(rc, LC_ENGINE_OK);
  assert_ptr_equal(observed.realloc_fn, tracked_engine_realloc);

  text = "hello";
  copy = lc_engine_client_strdup_range(client, text, text + 5);
  assert_non_null(copy);
  assert_string_equal(copy, "hello");
  assert_int_equal(alloc_state.malloc_calls, 1U);
  lc_engine_client_free_alloc(client, copy);
  assert_int_equal(alloc_state.free_calls, 1U);

  copy = (char *)observed.realloc_fn(observed.context, NULL, 32U);
  assert_non_null(copy);
  assert_int_equal(alloc_state.realloc_calls, 1U);
  lc_engine_client_free_alloc(client, copy);
  assert_int_equal(alloc_state.free_calls, 2U);

  response.cursor = lc_engine_client_strdup(client, "cursor-1");
  response.correlation_id = lc_engine_client_strdup(client, "corr-1");
  response.metadata_json =
      lc_engine_client_strdup(client, "{\"partial\":false}");
  response.return_mode = lc_engine_client_strdup(client, "compact");
  response.index_seq = 7UL;
  response.http_status = 200L;
  assert_non_null(response.cursor);
  assert_non_null(response.correlation_id);
  assert_non_null(response.metadata_json);
  assert_non_null(response.return_mode);

  lc_engine_query_stream_response_cleanup(client, &response);
  assert_null(response.cursor);
  assert_null(response.correlation_id);
  assert_null(response.metadata_json);
  assert_null(response.return_mode);
  assert_int_equal(response.index_seq, 0UL);
  assert_int_equal(response.http_status, 0L);
  assert_true(alloc_state.free_calls >= 5U);

  lc_engine_client_close(client);
  lc_engine_error_cleanup(&error);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_engine_get_allocator_returns_default_hooks),
      cmocka_unit_test(
          test_engine_set_allocator_validates_and_uses_custom_hooks),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}

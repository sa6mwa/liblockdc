#include <errno.h>
#include <pthread.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <cmocka.h>

#include "../support/lc_test_tmp.h"
#include "lc/lc.h"
#include "lc_pouch.h"

#define POUCH_E2E_TMP_PREFIX "/tmp/liblockdc-e2e-pouch-"
#define LOCAL_MUTATE_TMP_PREFIX "/tmp/liblockdc-local-mutate-"
#define S3_LOCAL_MUTATE_TMP_PREFIX "/tmp/liblockdc-s3-local-mutate-"

typedef struct e2e_status_doc {
  char *status;
} e2e_status_doc;

static const lonejson_field e2e_status_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(e2e_status_doc, status, "status")};

LONEJSON_MAP_DEFINE(e2e_status_map, e2e_status_doc, e2e_status_fields);

static void assert_lc_ok(int rc, lc_error *error);
static void assert_lc_server_error(int rc, lc_error *error, long http_status);
static int buffer_contains(const void *bytes, size_t length,
                           const char *needle);
static lonejson *e2e_lonejson_runtime(void);
static void e2e_lonejson_cleanup(const lonejson_map *map, void *value);

static lonejson *e2e_runtime_instance;
static pthread_once_t e2e_runtime_once = PTHREAD_ONCE_INIT;

static void e2e_lonejson_runtime_init(void) {
  lonejson_error error;

  lonejson_error_init(&error);
  e2e_runtime_instance = lonejson_new(NULL, &error);
}

static lonejson *e2e_lonejson_runtime(void) {
  (void)pthread_once(&e2e_runtime_once, e2e_lonejson_runtime_init);
  return e2e_runtime_instance;
}

static void e2e_lonejson_cleanup(const lonejson_map *map, void *value) {
  lonejson *runtime;

  runtime = e2e_lonejson_runtime();
  assert_non_null(runtime);
  runtime->cleanup(runtime, map, value);
}

static void save_json_text_or_die(lc_lease *lease, const char *json_text,
                                  lc_error *error) {
  lc_source *src;
  int rc;

  src = NULL;
  rc = lc_source_from_memory(json_text, strlen(json_text), &src, error);
  assert_lc_ok(rc, error);
  rc = lease->update(lease, src, NULL, error);
  lc_source_close(src);
  assert_lc_ok(rc, error);
}

typedef struct acquire_for_update_e2e_state {
  const char *expected;
  int saw_snapshot;
} acquire_for_update_e2e_state;

typedef struct queue_watch_e2e_context {
  const char *endpoint;
  const char *bundle_path;
  const char *queue_name;
  char queue_name_storage[128];
  int event_count;
  int unavailable_count;
  int available_count;
  int saw_available;
  int saw_unavailable;
  int stop_after_initial_unavailable;
  int stop_after_available;
  int stop_after_unavailable_after_available;
  char event_queue[128];
  char head_message_id[128];
  char unavailable_head_message_id[128];
  char correlation_id[128];
  int enqueue_rc;
  int dequeue_rc;
  lc_error enqueue_error;
  lc_error dequeue_error;
} queue_watch_e2e_context;

typedef struct query_keys_e2e_capture {
  const char *key_prefix;
  size_t key_prefix_len;
  size_t expected_count;
  size_t begin_calls;
  size_t chunk_calls;
  size_t end_calls;
  size_t total_bytes;
  char current[160];
  size_t current_length;
  unsigned char seen[512];
} query_keys_e2e_capture;

typedef struct pouch_e2e_key_count {
  size_t rows;
} pouch_e2e_key_count;

static int query_keys_e2e_begin(void *context, lc_error *error) {
  query_keys_e2e_capture *capture;

  (void)error;
  capture = (query_keys_e2e_capture *)context;
  capture->begin_calls += 1U;
  capture->current_length = 0U;
  capture->current[0] = '\0';
  return 1;
}

static int query_keys_e2e_chunk(void *context, const char *bytes, size_t len,
                                lc_error *error) {
  query_keys_e2e_capture *capture;
  size_t available;

  (void)error;
  capture = (query_keys_e2e_capture *)context;
  capture->chunk_calls += 1U;
  capture->total_bytes += len;
  available = sizeof(capture->current) - capture->current_length - 1U;
  assert_true(len <= available);
  memcpy(capture->current + capture->current_length, bytes, len);
  capture->current_length += len;
  capture->current[capture->current_length] = '\0';
  return 1;
}

static int query_keys_e2e_end(void *context, lc_error *error) {
  query_keys_e2e_capture *capture;
  const char *suffix;
  char *end;
  unsigned long index;

  (void)error;
  capture = (query_keys_e2e_capture *)context;
  assert_true(capture->current_length > capture->key_prefix_len);
  assert_int_equal(
      strncmp(capture->current, capture->key_prefix, capture->key_prefix_len),
      0);
  suffix = capture->current + capture->key_prefix_len;
  index = strtoul(suffix, &end, 10);
  assert_non_null(end);
  assert_int_equal(*end, '\0');
  assert_true(index < capture->expected_count);
  assert_int_equal(capture->seen[index], 0U);
  capture->seen[index] = 1U;
  capture->end_calls += 1U;
  capture->current_length = 0U;
  capture->current[0] = '\0';
  return 1;
}

static int pouch_e2e_key_begin(void *context, lc_error *error) {
  (void)context;
  (void)error;
  return 1;
}

static int pouch_e2e_key_chunk(void *context, const char *bytes, size_t len,
                               lc_error *error) {
  (void)context;
  (void)bytes;
  (void)len;
  (void)error;
  return 1;
}

static int pouch_e2e_key_end(void *context, lc_error *error) {
  pouch_e2e_key_count *count;

  (void)error;
  count = (pouch_e2e_key_count *)context;
  count->rows++;
  return 1;
}

static int acquire_for_update_e2e_handler(void *context,
                                          lc_acquire_for_update_context *update,
                                          lc_error *error) {
  acquire_for_update_e2e_state *state;
  lc_sink *sink;
  lc_source *src;
  const void *bytes;
  size_t length;
  size_t written;
  int rc;

  state = (acquire_for_update_e2e_state *)context;
  sink = NULL;
  src = NULL;
  assert_non_null(update);
  assert_non_null(update->lease);
  assert_true(update->state.has_state);
  assert_non_null(update->state.reader);
  assert_true(update->state.version >= 1L);
  rc = lc_sink_to_memory(&sink, error);
  assert_lc_ok(rc, error);
  rc = lc_copy(update->state.reader, sink, &written, error);
  assert_lc_ok(rc, error);
  assert_true(written > 0U);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  assert_lc_ok(rc, error);
  assert_true(buffer_contains(bytes, length, state->expected));
  state->saw_snapshot = 1;
  lc_sink_close(sink);

  rc = lc_source_from_memory(
      "{\"value\":2,\"via\":\"acquire-for-update\"}",
      strlen("{\"value\":2,\"via\":\"acquire-for-update\"}"), &src, error);
  assert_lc_ok(rc, error);
  rc = update->lease->update(update->lease, src, NULL, error);
  lc_source_close(src);
  return rc;
}

static int acquire_for_update_failing_handler(
    void *context, lc_acquire_for_update_context *update, lc_error *error) {
  lc_source *src;
  int rc;

  (void)context;
  src = NULL;
  assert_non_null(update);
  assert_non_null(update->lease);
  rc = lc_source_from_memory(
      "{\"value\":3,\"via\":\"failed-acquire-for-update\"}",
      strlen("{\"value\":3,\"via\":\"failed-acquire-for-update\"}"), &src,
      error);
  assert_lc_ok(rc, error);
  rc = update->lease->update(update->lease, src, NULL, error);
  lc_source_close(src);
  assert_lc_ok(rc, error);
  if (error != NULL) {
    error->code = LC_ERR_INVALID;
    error->message = strdup("intentional acquire_for_update handler failure");
    assert_non_null(error->message);
  }
  return LC_ERR_INVALID;
}

static void write_temp_file_or_die(char *template_path,
                                   const char *allowed_prefix,
                                   const unsigned char *bytes, size_t length) {
  int fd;
  FILE *fp;

  fd = lc_test_tmp_mkstemp(template_path, allowed_prefix);
  assert_true(fd >= 0);
  fp = fdopen(fd, "wb");
  assert_non_null(fp);
  assert_int_equal(fwrite(bytes, 1U, length, fp), (int)length);
  assert_int_equal(fclose(fp), 0);
}

static const char *env_or_default(const char *name, const char *fallback) {
  const char *value;

  value = getenv(name);
  if (value == NULL || value[0] == '\0') {
    return fallback;
  }
  return value;
}

static int file_exists(const char *path) {
  struct stat st;

  return path != NULL && stat(path, &st) == 0 && S_ISREG(st.st_mode);
}

static int socket_exists(const char *path) {
  struct stat st;

  return path != NULL && stat(path, &st) == 0 && S_ISSOCK(st.st_mode);
}

static void require_file_or_skip(const char *path) {
  if (!file_exists(path)) {
    skip();
  }
}

static void require_socket_or_skip(const char *path) {
  if (!socket_exists(path)) {
    skip();
  }
}

static void assert_lc_ok(int rc, lc_error *error) {
  if (rc != LC_OK) {
    print_message(
        "lc error: code=%d http=%ld message=%s detail=%s server_code=%s "
        "correlation=%s\n",
        error != NULL ? error->code : -1,
        error != NULL ? error->http_status : 0L,
        error != NULL && error->message != NULL ? error->message : "(null)",
        error != NULL && error->detail != NULL ? error->detail : "(null)",
        error != NULL && error->server_code != NULL ? error->server_code
                                                    : "(null)",
        error != NULL && error->correlation_id != NULL ? error->correlation_id
                                                       : "(null)");
  }
  assert_int_equal(rc, LC_OK);
}

static void assert_lc_server_error(int rc, lc_error *error, long http_status) {
  if (rc == LC_OK) {
    print_message("expected server error, got success\n");
  } else {
    print_message(
        "expected server error: code=%d http=%ld message=%s detail=%s "
        "server_code=%s correlation=%s\n",
        error != NULL ? error->code : -1,
        error != NULL ? error->http_status : 0L,
        error != NULL && error->message != NULL ? error->message : "(null)",
        error != NULL && error->detail != NULL ? error->detail : "(null)",
        error != NULL && error->server_code != NULL ? error->server_code
                                                    : "(null)",
        error != NULL && error->correlation_id != NULL ? error->correlation_id
                                                       : "(null)");
  }
  assert_int_not_equal(rc, LC_OK);
  assert_non_null(error);
  assert_int_equal(error->code, LC_ERR_SERVER);
  if (http_status > 0L) {
    assert_int_equal(error->http_status, http_status);
  } else {
    assert_true(error->http_status >= 400L);
  }
}

static int buffer_contains(const void *bytes, size_t length,
                           const char *needle) {
  const unsigned char *cursor;
  size_t needle_length;
  size_t i;

  if (bytes == NULL || needle == NULL) {
    return 0;
  }
  needle_length = strlen(needle);
  if (needle_length == 0U || length < needle_length) {
    return 0;
  }
  cursor = (const unsigned char *)bytes;
  for (i = 0U; i + needle_length <= length; ++i) {
    if (memcmp(cursor + i, needle, needle_length) == 0) {
      return 1;
    }
  }
  return 0;
}

static void assert_local_mutate_file_variants(lc_lease *lease,
                                              const char *text_path,
                                              const char *binary_path,
                                              lc_error *error) {
  const char *mutations[9];
  const char *failure_mutations[1];
  lc_mutate_local_req mutate_req;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char text_mutation[512];
  char base64_mutation[512];
  char auto_text_mutation[512];
  char auto_bin_mutation[512];
  char missing_file_mutation[512];
  char missing_path[] = "/tmp/liblockdc-local-mutate-missing-XXXXXX";
  int missing_fd;
  int rc;

  lc_mutate_local_req_init(&mutate_req);
  lc_get_opts_init(&get_opts);
  memset(&get_res, 0, sizeof(get_res));
  mutate_req.file_value_base_dir = NULL;

  mutations[0] = "/filename=\"blob.txt\"";
  mutations[1] = "/counter=2";
  mutations[2] = "rm:/remove_me";
  mutations[3] = "time:/ts=NOW";
  snprintf(text_mutation, sizeof(text_mutation), "textfile:/text_blob=%s",
           text_path);
  mutations[4] = text_mutation;
  snprintf(base64_mutation, sizeof(base64_mutation), "base64file:/bin_blob=%s",
           binary_path);
  mutations[5] = base64_mutation;
  snprintf(auto_text_mutation, sizeof(auto_text_mutation), "file:/auto_text=%s",
           text_path);
  mutations[6] = auto_text_mutation;
  snprintf(auto_bin_mutation, sizeof(auto_bin_mutation), "file:/auto_bin=%s",
           binary_path);
  mutations[7] = auto_bin_mutation;
  mutations[8] = "rm:/optional/missing";
  mutate_req.mutations = mutations;
  mutate_req.mutation_count = 9U;
  rc = lease->mutate_local(lease, &mutate_req, error);
  assert_lc_ok(rc, error);
  assert_non_null(lease->state_etag);

  sink = NULL;
  bytes = NULL;
  length = 0U;
  rc = lc_sink_to_memory(&sink, error);
  assert_lc_ok(rc, error);
  rc = lease->get(lease, sink, &get_opts, &get_res, error);
  assert_lc_ok(rc, error);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  assert_lc_ok(rc, error);
  assert_true(buffer_contains(bytes, length, "\"filename\":\"blob.txt\""));
  assert_true(buffer_contains(bytes, length, "\"counter\":3"));
  assert_true(buffer_contains(bytes, length,
                              "\"text_blob\":\"hello\\n\\\"quoted\\\"\""));
  assert_true(buffer_contains(bytes, length, "\"bin_blob\":\"AAECYQ==\""));
  assert_true(buffer_contains(bytes, length,
                              "\"auto_text\":\"hello\\n\\\"quoted\\\"\""));
  assert_true(buffer_contains(bytes, length, "\"auto_bin\":\"AAECYQ==\""));
  assert_true(buffer_contains(bytes, length, "\"ts\":\""));
  assert_false(buffer_contains(bytes, length, "\"remove_me\""));
  assert_false(buffer_contains(bytes, length, "\"optional\""));
  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);

  missing_fd = lc_test_tmp_mkstemp(missing_path, LOCAL_MUTATE_TMP_PREFIX);
  assert_true(missing_fd >= 0);
  assert_int_equal(close(missing_fd), 0);
  assert_int_equal(unlink(missing_path), 0);
  lc_test_tmp_untrack_path(missing_path);
  snprintf(missing_file_mutation, sizeof(missing_file_mutation),
           "base64file:/missing=%s", missing_path);
  failure_mutations[0] = missing_file_mutation;
  lc_mutate_local_req_init(&mutate_req);
  mutate_req.mutations = failure_mutations;
  mutate_req.mutation_count = 1U;
  lc_error_cleanup(error);
  lc_error_init(error);
  rc = lease->mutate_local(lease, &mutate_req, error);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_int_equal(error->code, LC_ERR_TRANSPORT);
  assert_non_null(error->message);

  sink = NULL;
  bytes = NULL;
  length = 0U;
  memset(&get_res, 0, sizeof(get_res));
  lc_error_cleanup(error);
  lc_error_init(error);
  rc = lc_sink_to_memory(&sink, error);
  assert_lc_ok(rc, error);
  rc = lease->get(lease, sink, &get_opts, &get_res, error);
  assert_lc_ok(rc, error);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  assert_lc_ok(rc, error);
  assert_true(buffer_contains(bytes, length, "\"filename\":\"blob.txt\""));
  assert_true(buffer_contains(bytes, length, "\"counter\":3"));
  assert_false(buffer_contains(bytes, length, "\"missing\""));
  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
}

static void make_unique_name(const char *prefix, char *buffer,
                             size_t capacity) {
  long now;
  int pid;

  now = (long)time(NULL);
  pid = (int)getpid();
  snprintf(buffer, capacity, "%s-%ld-%d", prefix, now, pid);
}

static void make_pouch_root(const char *suffix, char *root,
                            size_t root_capacity, char *endpoint,
                            size_t endpoint_capacity) {
  char template_path[512];
  int written;

  written = snprintf(template_path, sizeof(template_path),
                     POUCH_E2E_TMP_PREFIX "%s-XXXXXX", suffix);
  assert_true(written > 0 && (size_t)written < sizeof(template_path));
  assert_true(lc_test_tmp_mkdtemp(template_path, root, root_capacity,
                                  POUCH_E2E_TMP_PREFIX));
  written = snprintf(endpoint, endpoint_capacity, "pouch://%s", root);
  assert_true(written > 0 && (size_t)written < endpoint_capacity);
}

static void cleanup_pouch_root(const char *root) {
  lc_test_tmp_cleanup_path(root, POUCH_E2E_TMP_PREFIX);
}

static void cleanup_all_pouch_roots(void) {
  lc_test_tmp_cleanup_stale("/tmp", "liblockdc-e2e-pouch-",
                            POUCH_E2E_TMP_PREFIX);
}

static void pouch_e2e_write_text_file(const char *path, const char *text) {
  FILE *fp;

  fp = fopen(path, "wb");
  assert_non_null(fp);
  assert_int_equal(fputs(text, fp) < 0 ? -1 : 0, 0);
  assert_int_equal(fclose(fp), 0);
}

static int setup_pouch_e2e_group(void **state) {
  (void)state;
  cleanup_all_pouch_roots();
  return 0;
}

static int teardown_pouch_e2e_group(void **state) {
  (void)state;
  cleanup_all_pouch_roots();
  return 0;
}

static void open_tcp_client_with_json_response_limit(const char *endpoint,
                                                     const char *bundle_path,
                                                     size_t json_response_limit,
                                                     lc_client **out,
                                                     lc_error *error) {
  lc_client_config config;
  lc_source *bundle_source;
  const char *endpoints[1];
  int rc;

  lc_client_config_init(&config);
  bundle_source = NULL;
  rc = lc_source_from_file(bundle_path, &bundle_source, error);
  assert_lc_ok(rc, error);
  endpoints[0] = endpoint;
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.client_bundle_source = bundle_source;
  config.default_namespace = "default";
  config.timeout_ms = 5000L;
  config.insecure_skip_verify = 1;
  config.http_json_response_limit_bytes = json_response_limit;
  rc = lc_client_open(&config, out, error);
  lc_source_close(bundle_source);
  assert_lc_ok(rc, error);
}

static void open_tcp_client(const char *endpoint, const char *bundle_path,
                            lc_client **out, lc_error *error) {
  open_tcp_client_with_json_response_limit(endpoint, bundle_path, 0U, out,
                                           error);
}

static void open_tcp_client_allow_error(const char *endpoint,
                                        const char *bundle_path,
                                        lc_client **out, lc_error *error) {
  lc_client_config config;
  lc_source *bundle_source;
  const char *endpoints[1];
  int rc;

  lc_client_config_init(&config);
  bundle_source = NULL;
  rc = lc_source_from_file(bundle_path, &bundle_source, error);
  assert_lc_ok(rc, error);
  endpoints[0] = endpoint;
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.client_bundle_source = bundle_source;
  config.default_namespace = "default";
  config.timeout_ms = 5000L;
  config.insecure_skip_verify = 1;
  *out = NULL;
  lc_client_open(&config, out, error);
  lc_source_close(bundle_source);
}

static void open_pouch_client(const char *endpoint, lc_client **out,
                              lc_error *error) {
  lc_client_config config;
  const char *endpoints[1];
  int rc;

  lc_client_config_init(&config);
  endpoints[0] = endpoint;
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "default";
  config.timeout_ms = 5000L;
  rc = lc_client_open(&config, out, error);
  assert_lc_ok(rc, error);
}

static void open_uds_client(const char *socket_path, lc_client **out,
                            lc_error *error) {
  lc_client_config config;
  const char *endpoints[1];
  int rc;

  lc_client_config_init(&config);
  endpoints[0] = "http://localhost";
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.unix_socket_path = socket_path;
  config.default_namespace = "default";
  config.disable_mtls = 1;
  config.timeout_ms = 5000L;
  rc = lc_client_open(&config, out, error);
  assert_lc_ok(rc, error);
}

static void test_disk_lease_state_roundtrip(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_get_res get_res;
  lc_get_opts get_opts;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char key[96];
  static const char json_text[] =
      "{\"kind\":\"e2e\",\"value\":1,\"tags\":[\"disk\"]}";
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./devenv/volumes/lockd-disk-a-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&get_res, 0, sizeof(get_res));
  memset(&get_opts, 0, sizeof(get_opts));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("disk-state", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  save_json_text_or_die(lease, json_text, &error);
  assert_true(lease->version >= 1L);
  assert_non_null(lease->state_etag);

  rc = lease->describe(lease, &error);
  assert_lc_ok(rc, &error);
  assert_string_equal(lease->key, key);
  assert_string_equal(lease->namespace_name, "default");
  assert_non_null(lease->owner);
  assert_true(lease->owner[0] != '\0');
  assert_non_null(lease->lease_id);

  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  get_opts.public_read = 1;
  rc = client->get(client, key, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(get_res.no_content, 0);
  assert_true(get_res.version >= 1L);

  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_true(buffer_contains(bytes, length, "\"kind\":\"e2e\""));
  assert_true(buffer_contains(bytes, length, "\"tags\":[\"disk\"]"));

  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_server_minted_multikey_xa_transaction(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *commit_a;
  lc_lease *commit_b;
  lc_lease *rollback_a;
  lc_lease *rollback_b;
  lc_acquire_req acquire_req;
  lc_release_req release_req;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char commit_key_a[96];
  char commit_key_b[96];
  char rollback_key_a[96];
  char rollback_key_b[96];
  int rc;
  lc_error error;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./devenv/volumes/lockd-disk-a-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  commit_a = NULL;
  commit_b = NULL;
  rollback_a = NULL;
  rollback_b = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_release_req_init(&release_req);
  lc_get_opts_init(&get_opts);
  memset(&get_res, 0, sizeof(get_res));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("disk-xa-server-commit-a", commit_key_a,
                   sizeof(commit_key_a));
  make_unique_name("disk-xa-server-commit-b", commit_key_b,
                   sizeof(commit_key_b));

  acquire_req.key = commit_key_a;
  acquire_req.owner = "lc-e2e-xa";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &commit_a, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(commit_a);
  assert_non_null(commit_a->txn_id);
  assert_true(commit_a->txn_id[0] != '\0');

  acquire_req.key = commit_key_b;
  acquire_req.txn_id = commit_a->txn_id;
  rc = client->acquire(client, &acquire_req, &commit_b, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(commit_b);
  assert_string_equal(commit_b->txn_id, commit_a->txn_id);

  save_json_text_or_die(commit_a, "{\"transaction\":\"commit-a\"}", &error);
  save_json_text_or_die(commit_b, "{\"transaction\":\"commit-b\"}", &error);
  rc = commit_a->release(commit_a, NULL, &error);
  assert_lc_ok(rc, &error);
  commit_a = NULL;
  rc = commit_b->release(commit_b, NULL, &error);
  assert_lc_ok(rc, &error);
  commit_b = NULL;

  get_opts.public_read = 1;
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = client->get(client, commit_key_a, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_false(get_res.no_content);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_true(buffer_contains(bytes, length, "\"transaction\":\"commit-a\""));
  lc_sink_close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = client->get(client, commit_key_b, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_false(get_res.no_content);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_true(buffer_contains(bytes, length, "\"transaction\":\"commit-b\""));
  lc_sink_close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);

  lc_acquire_req_init(&acquire_req);
  make_unique_name("disk-xa-server-rollback-a", rollback_key_a,
                   sizeof(rollback_key_a));
  make_unique_name("disk-xa-server-rollback-b", rollback_key_b,
                   sizeof(rollback_key_b));
  acquire_req.key = rollback_key_a;
  acquire_req.owner = "lc-e2e-xa";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &rollback_a, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(rollback_a);
  assert_non_null(rollback_a->txn_id);

  acquire_req.key = rollback_key_b;
  acquire_req.txn_id = rollback_a->txn_id;
  rc = client->acquire(client, &acquire_req, &rollback_b, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(rollback_b);
  assert_string_equal(rollback_b->txn_id, rollback_a->txn_id);

  save_json_text_or_die(rollback_a, "{\"transaction\":\"rollback-a\"}", &error);
  save_json_text_or_die(rollback_b, "{\"transaction\":\"rollback-b\"}", &error);
  release_req.rollback = 1;
  rc = rollback_a->release(rollback_a, &release_req, &error);
  assert_lc_ok(rc, &error);
  rollback_a = NULL;
  rc = rollback_b->release(rollback_b, &release_req, &error);
  assert_lc_ok(rc, &error);
  rollback_b = NULL;

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = client->get(client, rollback_key_a, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_true(get_res.no_content);
  lc_sink_close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = client->get(client, rollback_key_b, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_true(get_res.no_content);
  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_server_explicit_xa_enlists_on_acquire(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *first;
  lc_lease *second;
  lc_acquire_req acquire_req;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lc_sink *sink;
  lc_error error;
  char txn_id[LC_XID_STRING_SIZE];
  char first_key[96];
  char second_key[96];
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./devenv/volumes/lockd-disk-a-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  first = NULL;
  second = NULL;
  sink = NULL;
  txn_id[0] = '\0';
  lc_acquire_req_init(&acquire_req);
  lc_get_opts_init(&get_opts);
  memset(&get_res, 0, sizeof(get_res));
  lc_error_init(&error);

  open_tcp_client(endpoint, bundle_path, &client, &error);
  rc = lc_xid_new(txn_id, &error);
  assert_lc_ok(rc, &error);
  make_unique_name("disk-xa-terminal-enlist-a", first_key, sizeof(first_key));
  make_unique_name("disk-xa-terminal-enlist-b", second_key, sizeof(second_key));

  acquire_req.key = first_key;
  acquire_req.owner = "lc-e2e-xa-terminal-enlist";
  acquire_req.ttl_seconds = 30L;
  acquire_req.txn_id = txn_id;
  rc = client->acquire(client, &acquire_req, &first, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(first);
  acquire_req.key = second_key;
  rc = client->acquire(client, &acquire_req, &second, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(second);

  save_json_text_or_die(first, "{\"transaction\":\"first\"}", &error);
  save_json_text_or_die(second, "{\"transaction\":\"second\"}", &error);
  rc = first->release(first, NULL, &error);
  assert_lc_ok(rc, &error);
  first = NULL;

  get_opts.public_read = 1;
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = client->get(client, first_key, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_false(get_res.no_content);
  lc_sink_close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = client->get(client, second_key, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_false(get_res.no_content);
  lc_sink_close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);

  rc = second->release(second, NULL, &error);
  assert_lc_ok(rc, &error);
  second = NULL;
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = client->get(client, second_key, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_false(get_res.no_content);
  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void
test_disk_server_metadata_finalization_preserves_staged_version(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_acquire_req acquire_req;
  lc_metadata_req metadata_req;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lc_sink *sink;
  lc_error error;
  char key[96];
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./devenv/volumes/lockd-disk-a-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  sink = NULL;
  lc_acquire_req_init(&acquire_req);
  lc_metadata_req_init(&metadata_req);
  lc_get_opts_init(&get_opts);
  memset(&get_res, 0, sizeof(get_res));
  lc_error_init(&error);

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("disk-metadata-staged-version", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e-metadata-staged-version";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);
  save_json_text_or_die(lease, "{\"value\":31}", &error);
  assert_int_equal(lease->version, 1L);

  metadata_req.has_query_hidden = 1;
  metadata_req.query_hidden = 1;
  rc = lease->metadata(lease, &metadata_req, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(lease->version, 1L);
  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  get_opts.public_read = 1;
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = client->get(client, key, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_false(get_res.no_content);
  assert_int_equal(get_res.version, 1L);
  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_acquire_for_update_roundtrip(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char key[96];
  acquire_for_update_e2e_state handler_state;
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./devenv/volumes/lockd-disk-a-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_get_opts_init(&get_opts);
  memset(&get_res, 0, sizeof(get_res));
  memset(&handler_state, 0, sizeof(handler_state));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("disk-acquire-for-update", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e-seed";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  save_json_text_or_die(lease, "{\"value\":1}", &error);
  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  acquire_req.owner = "lc-e2e-acquire-for-update";
  handler_state.expected = "\"value\":1";
  rc = lc_acquire_for_update(client, &acquire_req,
                             acquire_for_update_e2e_handler, &handler_state,
                             &error);
  assert_lc_ok(rc, &error);
  assert_true(handler_state.saw_snapshot);

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  get_opts.public_read = 1;
  rc = client->get(client, key, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(get_res.no_content, 0);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_true(buffer_contains(bytes, length, "\"value\":2"));
  assert_true(buffer_contains(bytes, length, "\"via\":\"acquire-for-update\""));

  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void
test_disk_acquire_for_update_handler_error_rolls_back(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char key[96];
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./devenv/volumes/lockd-disk-a-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_get_opts_init(&get_opts);
  memset(&get_res, 0, sizeof(get_res));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("disk-acquire-for-update-error", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e-seed";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  save_json_text_or_die(lease, "{\"value\":1}", &error);
  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  acquire_req.owner = "lc-e2e-acquire-for-update-error";
  rc = lc_acquire_for_update(client, &acquire_req,
                             acquire_for_update_failing_handler, NULL, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(error.code, LC_ERR_INVALID);
  assert_non_null(error.message);
  assert_string_equal(error.message,
                      "intentional acquire_for_update handler failure");
  lc_error_cleanup(&error);

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  get_opts.public_read = 1;
  rc = client->get(client, key, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(get_res.no_content, 0);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_true(buffer_contains(bytes, length, "\"value\":1"));
  assert_false(
      buffer_contains(bytes, length, "\"via\":\"failed-acquire-for-update\""));

  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_s3_lease_state_roundtrip(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_get_res get_res;
  lc_get_opts get_opts;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char key[96];
  static const char json_text[] =
      "{\"kind\":\"e2e\",\"value\":1,\"tags\":[\"s3\"]}";
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_S3_ENDPOINT", "https://localhost:19443");
  bundle_path = env_or_default("LOCKDC_E2E_S3_BUNDLE",
                               "./devenv/volumes/lockd-s3-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&get_res, 0, sizeof(get_res));
  memset(&get_opts, 0, sizeof(get_opts));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("s3-state", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  save_json_text_or_die(lease, json_text, &error);
  assert_true(lease->version >= 1L);
  assert_non_null(lease->state_etag);

  rc = lease->describe(lease, &error);
  assert_lc_ok(rc, &error);
  assert_string_equal(lease->key, key);
  assert_string_equal(lease->namespace_name, "default");
  assert_non_null(lease->owner);
  assert_true(lease->owner[0] != '\0');
  assert_non_null(lease->lease_id);

  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  get_opts.public_read = 1;
  rc = client->get(client, key, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(get_res.no_content, 0);
  assert_true(get_res.version >= 1L);

  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_true(buffer_contains(bytes, length, "\"kind\":\"e2e\""));
  assert_true(buffer_contains(bytes, length, "\"tags\":[\"s3\"]"));

  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_s3_attachment_roundtrip(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_source *src;
  lc_sink *sink;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_attach_req attach_req;
  lc_attach_res attach_res;
  lc_attachment_list attachment_list;
  lc_attachment_get_req get_req;
  lc_attachment_get_res get_res;
  const void *bytes;
  size_t length;
  char key[96];
  static const unsigned char payload[] = {'h', 'e', 'l', 'l',
                                          'o', '-', 's', '3'};
  int deleted;
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_S3_ENDPOINT", "https://localhost:19443");
  bundle_path = env_or_default("LOCKDC_E2E_S3_BUNDLE",
                               "./devenv/volumes/lockd-s3-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  src = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  deleted = 0;
  lc_error_init(&error);
  memset(&acquire_req, 0, sizeof(acquire_req));
  memset(&attach_req, 0, sizeof(attach_req));
  memset(&attach_res, 0, sizeof(attach_res));
  memset(&attachment_list, 0, sizeof(attachment_list));
  memset(&get_req, 0, sizeof(get_req));
  memset(&get_res, 0, sizeof(get_res));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("s3-attachment", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  save_json_text_or_die(lease, "{\"kind\":\"attachment\"}", &error);

  rc = lc_source_from_memory(payload, sizeof(payload), &src, &error);
  assert_lc_ok(rc, &error);
  attach_req.name = "blob.bin";
  attach_req.content_type = "application/octet-stream";
  rc = lease->attach(lease, &attach_req, src, &attach_res, &error);
  assert_lc_ok(rc, &error);
  assert_string_equal(attach_res.attachment.name, "blob.bin");
  assert_true(attach_res.attachment.size >= (long)sizeof(payload));

  rc = lease->list_attachments(lease, &attachment_list, &error);
  assert_lc_ok(rc, &error);
  assert_true(attachment_list.count >= 1U);

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  memset(&get_req, 0, sizeof(get_req));
  get_req.selector.name = "blob.bin";
  rc = lease->get_attachment(lease, &get_req, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(length, sizeof(payload));
  assert_memory_equal(bytes, payload, sizeof(payload));

  rc = lease->delete_attachment(lease, &get_req.selector, &deleted, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(deleted, 1);

  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  lc_sink_close(sink);
  lc_source_close(src);
  lc_attachment_get_res_cleanup(&get_res);
  lc_attachment_list_cleanup(&attachment_list);
  lc_attach_res_cleanup(&attach_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_mem_uds_queue_roundtrip(void **state) {
  const char *socket_path;
  lc_client *client;
  lc_source *src;
  lc_sink *sink;
  lc_message *message;
  lc_error error;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  const void *bytes;
  size_t length;
  size_t written;
  char queue_name[96];
  static const unsigned char payload[] = {'h', 'e', 'l', 'l', 'o',
                                          '-', 'u', 'd', 's'};
  int rc;

  (void)state;
  socket_path = env_or_default("LOCKDC_E2E_MEM_SOCKET",
                               "./devenv/volumes/lockd-mem-run/lockd.sock");
  require_socket_or_skip(socket_path);

  client = NULL;
  src = NULL;
  sink = NULL;
  message = NULL;
  bytes = NULL;
  length = 0U;
  written = 0U;
  lc_error_init(&error);
  memset(&enqueue_req, 0, sizeof(enqueue_req));
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  memset(&dequeue_req, 0, sizeof(dequeue_req));

  open_uds_client(socket_path, &client, &error);
  make_unique_name("mem-queue", queue_name, sizeof(queue_name));
  rc = lc_source_from_memory(payload, sizeof(payload), &src, &error);
  assert_lc_ok(rc, &error);

  enqueue_req.queue = queue_name;
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 300L;
  enqueue_req.max_attempts = 5;
  rc = client->enqueue(client, &enqueue_req, src, &enqueue_res, &error);
  assert_lc_ok(rc, &error);
  assert_string_equal(enqueue_res.queue, queue_name);
  assert_true(enqueue_res.payload_bytes >= (long)sizeof(payload));

  dequeue_req.queue = queue_name;
  dequeue_req.owner = "lc-e2e-worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  dequeue_req.wait_seconds = 2L;
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(message);
  assert_string_equal(message->queue, queue_name);
  assert_string_equal(message->payload_content_type, "text/plain");

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = message->write_payload(message, sink, &written, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(written, sizeof(payload));
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(length, sizeof(payload));
  assert_memory_equal(bytes, payload, sizeof(payload));

  rc = message->rewind_payload(message, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(error.code, LC_ERR_INVALID);
  lc_error_cleanup(&error);
  rc = message->ack(message, &error);
  assert_lc_ok(rc, &error);
  message = NULL;

  lc_enqueue_res_cleanup(&enqueue_res);
  lc_sink_close(sink);
  lc_source_close(src);
  if (message != NULL) {
    lc_message_close(message);
  }
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_mem_uds_dequeue_batch_roundtrip(void **state) {
  const char *socket_path;
  lc_client *client;
  lc_source *src;
  lc_error error;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_dequeue_batch_res batch;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char queue_name[96];
  static const unsigned char payload_a[] = {'b', 'a', 't', 'c', 'h', '-', '1'};
  static const unsigned char payload_b[] = {'b', 'a', 't', 'c', 'h', '-', '2'};
  int rc;
  size_t i;

  (void)state;
  socket_path = env_or_default("LOCKDC_E2E_MEM_SOCKET",
                               "./devenv/volumes/lockd-mem-run/lockd.sock");
  require_socket_or_skip(socket_path);

  client = NULL;
  src = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_dequeue_req_init(&dequeue_req);
  memset(&batch, 0, sizeof(batch));

  open_uds_client(socket_path, &client, &error);
  make_unique_name("mem-queue-batch", queue_name, sizeof(queue_name));
  enqueue_req.queue = queue_name;
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 300L;
  enqueue_req.max_attempts = 5;

  rc = lc_source_from_memory(payload_a, sizeof(payload_a), &src, &error);
  assert_lc_ok(rc, &error);
  rc = client->enqueue(client, &enqueue_req, src, &enqueue_res, &error);
  assert_lc_ok(rc, &error);
  lc_source_close(src);
  src = NULL;
  lc_enqueue_res_cleanup(&enqueue_res);

  rc = lc_source_from_memory(payload_b, sizeof(payload_b), &src, &error);
  assert_lc_ok(rc, &error);
  rc = client->enqueue(client, &enqueue_req, src, &enqueue_res, &error);
  assert_lc_ok(rc, &error);
  lc_source_close(src);
  src = NULL;
  lc_enqueue_res_cleanup(&enqueue_res);

  dequeue_req.queue = queue_name;
  dequeue_req.owner = "lc-e2e-batch-worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  dequeue_req.wait_seconds = 2L;
  dequeue_req.page_size = 2;
  rc = client->dequeue_batch(client, &dequeue_req, &batch, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(batch.count, 2U);

  for (i = 0U; i < batch.count; ++i) {
    rc = lc_sink_to_memory(&sink, &error);
    assert_lc_ok(rc, &error);
    rc =
        batch.messages[i]->write_payload(batch.messages[i], sink, NULL, &error);
    assert_lc_ok(rc, &error);
    rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
    assert_lc_ok(rc, &error);
    assert_true(length == sizeof(payload_a) || length == sizeof(payload_b));
    rc = batch.messages[i]->ack(batch.messages[i], &error);
    assert_lc_ok(rc, &error);
    lc_sink_close(sink);
    sink = NULL;
  }

  lc_dequeue_batch_cleanup(&batch);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static int queue_watch_e2e_handler(void *context, const lc_watch_event *event,
                                   lc_error *error) {
  queue_watch_e2e_context *watch;

  watch = (queue_watch_e2e_context *)context;
  watch->event_count += 1;
  if (event->queue != NULL) {
    snprintf(watch->event_queue, sizeof(watch->event_queue), "%s",
             event->queue);
  }
  if (event->correlation_id != NULL) {
    snprintf(watch->correlation_id, sizeof(watch->correlation_id), "%s",
             event->correlation_id);
  }
  if (event->available) {
    watch->available_count += 1;
    watch->saw_available = 1;
    if (event->head_message_id != NULL) {
      snprintf(watch->head_message_id, sizeof(watch->head_message_id), "%s",
               event->head_message_id);
    }
    if (!watch->stop_after_available) {
      return 1;
    }
    error->code = LC_ERR_TRANSPORT;
    error->message = strdup("queue watch e2e observed available event");
    assert_non_null(error->message);
    return 0;
  } else {
    watch->unavailable_count += 1;
    watch->saw_unavailable = 1;
    if (event->head_message_id != NULL) {
      snprintf(watch->unavailable_head_message_id,
               sizeof(watch->unavailable_head_message_id), "%s",
               event->head_message_id);
    }
    if (!watch->stop_after_initial_unavailable &&
        (!watch->stop_after_unavailable_after_available ||
         !watch->saw_available)) {
      return 1;
    }
    error->code = LC_ERR_TRANSPORT;
    error->message = strdup("queue watch e2e observed unavailable event");
    assert_non_null(error->message);
    return 0;
  }
}

static void *queue_watch_e2e_enqueue_thread(void *arg) {
  queue_watch_e2e_context *watch;
  lc_client *client;
  lc_source *src;
  lc_enqueue_req req;
  lc_enqueue_res res;
  static const unsigned char payload[] = {'w', 'a', 't', 'c', 'h'};

  watch = (queue_watch_e2e_context *)arg;
  client = NULL;
  src = NULL;
  lc_error_init(&watch->enqueue_error);
  lc_enqueue_req_init(&req);
  memset(&res, 0, sizeof(res));
  usleep(200000U);

  open_tcp_client(watch->endpoint, watch->bundle_path, &client,
                  &watch->enqueue_error);
  watch->enqueue_rc = lc_source_from_memory(payload, sizeof(payload), &src,
                                            &watch->enqueue_error);
  if (watch->enqueue_rc == LC_OK) {
    req.queue = watch->queue_name;
    req.content_type = "text/plain";
    req.visibility_timeout_seconds = 30L;
    req.ttl_seconds = 300L;
    watch->enqueue_rc =
        client->enqueue(client, &req, src, &res, &watch->enqueue_error);
  }
  if (src != NULL) {
    lc_source_close(src);
  }
  lc_enqueue_res_cleanup(&res);
  lc_client_close(client);
  return NULL;
}

static void *queue_watch_e2e_enqueue_dequeue_thread(void *arg) {
  queue_watch_e2e_context *watch;
  lc_client *client;
  lc_source *src;
  lc_message *message;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  static const unsigned char payload[] = {'w', 'a', 't', 'c', 'h',
                                          '-', 'a', 'c', 'k'};

  watch = (queue_watch_e2e_context *)arg;
  client = NULL;
  src = NULL;
  message = NULL;
  lc_error_init(&watch->enqueue_error);
  lc_error_init(&watch->dequeue_error);
  lc_enqueue_req_init(&enqueue_req);
  lc_dequeue_req_init(&dequeue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  usleep(200000U);

  open_tcp_client(watch->endpoint, watch->bundle_path, &client,
                  &watch->enqueue_error);
  watch->enqueue_rc = lc_source_from_memory(payload, sizeof(payload), &src,
                                            &watch->enqueue_error);
  if (watch->enqueue_rc == LC_OK) {
    enqueue_req.queue = watch->queue_name;
    enqueue_req.content_type = "text/plain";
    enqueue_req.visibility_timeout_seconds = 30L;
    enqueue_req.ttl_seconds = 300L;
    watch->enqueue_rc = client->enqueue(client, &enqueue_req, src, &enqueue_res,
                                        &watch->enqueue_error);
  }
  if (src != NULL) {
    lc_source_close(src);
  }
  lc_enqueue_res_cleanup(&enqueue_res);

  if (watch->enqueue_rc == LC_OK) {
    usleep(200000U);
    dequeue_req.queue = watch->queue_name;
    dequeue_req.owner = "lc-e2e-watch-worker";
    dequeue_req.visibility_timeout_seconds = 30L;
    dequeue_req.wait_seconds = 2L;
    watch->dequeue_rc =
        client->dequeue(client, &dequeue_req, &message, &watch->dequeue_error);
    if (watch->dequeue_rc == LC_OK) {
      watch->dequeue_rc = message->ack(message, &watch->dequeue_error);
      message = NULL;
    }
  } else {
    watch->dequeue_rc = watch->enqueue_rc;
  }

  if (message != NULL) {
    lc_message_close(message);
  }
  lc_client_close(client);
  return NULL;
}

static void
test_disk_queue_watch_stream_observes_initial_unavailable(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_watch_queue_req req;
  lc_watch_handler handler;
  queue_watch_e2e_context watch;
  lc_error error;
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./devenv/volumes/lockd-disk-a-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lc_error_init(&error);
  lc_watch_queue_req_init(&req);
  lc_watch_handler_init(&handler);
  memset(&watch, 0, sizeof(watch));
  make_unique_name("disk-watch-empty", watch.queue_name_storage,
                   sizeof(watch.queue_name_storage));
  watch.queue_name = watch.queue_name_storage;
  watch.stop_after_initial_unavailable = 1;

  open_tcp_client(endpoint, bundle_path, &client, &error);
  req.queue = watch.queue_name;
  handler.handle = queue_watch_e2e_handler;
  handler.context = &watch;
  rc = client->watch_queue(client, &req, &handler, &error);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_string_equal(error.message,
                      "queue watch e2e observed unavailable event");
  assert_true(watch.event_count >= 1);
  assert_true(watch.unavailable_count >= 1);
  assert_int_equal(watch.available_count, 0);
  assert_int_equal(watch.saw_unavailable, 1);
  assert_int_equal(watch.saw_available, 0);
  assert_string_equal(watch.event_queue, watch.queue_name);
  assert_true(watch.head_message_id[0] == '\0');
  assert_true(watch.correlation_id[0] != '\0');

  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_queue_watch_stream_observes_enqueue(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_watch_queue_req req;
  lc_watch_handler handler;
  queue_watch_e2e_context watch;
  pthread_t enqueue_thread;
  lc_error error;
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./devenv/volumes/lockd-disk-a-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lc_error_init(&error);
  lc_watch_queue_req_init(&req);
  lc_watch_handler_init(&handler);
  memset(&watch, 0, sizeof(watch));
  watch.endpoint = endpoint;
  watch.bundle_path = bundle_path;
  make_unique_name("disk-watch-queue", watch.queue_name_storage,
                   sizeof(watch.queue_name_storage));
  watch.queue_name = watch.queue_name_storage;
  watch.stop_after_available = 1;

  open_tcp_client(endpoint, bundle_path, &client, &error);
  assert_int_equal(pthread_create(&enqueue_thread, NULL,
                                  queue_watch_e2e_enqueue_thread, &watch),
                   0);
  req.queue = watch.queue_name;
  handler.handle = queue_watch_e2e_handler;
  handler.context = &watch;
  rc = client->watch_queue(client, &req, &handler, &error);
  assert_int_equal(pthread_join(enqueue_thread, NULL), 0);
  assert_lc_ok(watch.enqueue_rc, &watch.enqueue_error);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_string_equal(error.message,
                      "queue watch e2e observed available event");
  assert_true(watch.event_count >= 1);
  assert_true(watch.unavailable_count >= 1);
  assert_true(watch.available_count >= 1);
  assert_int_equal(watch.saw_available, 1);
  assert_int_equal(watch.saw_unavailable, 1);
  assert_string_equal(watch.event_queue, watch.queue_name);
  assert_true(watch.head_message_id[0] != '\0');
  assert_true(watch.correlation_id[0] != '\0');

  lc_client_close(client);
  lc_error_cleanup(&watch.enqueue_error);
  lc_error_cleanup(&error);
}

static void
test_disk_queue_watch_stream_observes_available_then_unavailable(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_watch_queue_req req;
  lc_watch_handler handler;
  queue_watch_e2e_context watch;
  pthread_t worker_thread;
  lc_error error;
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./devenv/volumes/lockd-disk-a-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lc_error_init(&error);
  lc_watch_queue_req_init(&req);
  lc_watch_handler_init(&handler);
  memset(&watch, 0, sizeof(watch));
  watch.endpoint = endpoint;
  watch.bundle_path = bundle_path;
  make_unique_name("disk-watch-transition", watch.queue_name_storage,
                   sizeof(watch.queue_name_storage));
  watch.queue_name = watch.queue_name_storage;
  watch.stop_after_unavailable_after_available = 1;

  open_tcp_client(endpoint, bundle_path, &client, &error);
  assert_int_equal(pthread_create(&worker_thread, NULL,
                                  queue_watch_e2e_enqueue_dequeue_thread,
                                  &watch),
                   0);
  req.queue = watch.queue_name;
  handler.handle = queue_watch_e2e_handler;
  handler.context = &watch;
  rc = client->watch_queue(client, &req, &handler, &error);
  assert_int_equal(pthread_join(worker_thread, NULL), 0);
  assert_lc_ok(watch.enqueue_rc, &watch.enqueue_error);
  assert_lc_ok(watch.dequeue_rc, &watch.dequeue_error);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_string_equal(error.message,
                      "queue watch e2e observed unavailable event");
  assert_true(watch.event_count >= 3);
  assert_true(watch.unavailable_count >= 2);
  assert_true(watch.available_count >= 1);
  assert_int_equal(watch.saw_available, 1);
  assert_int_equal(watch.saw_unavailable, 1);
  assert_string_equal(watch.event_queue, watch.queue_name);
  assert_true(watch.head_message_id[0] != '\0');
  assert_true(watch.correlation_id[0] != '\0');

  lc_client_close(client);
  lc_error_cleanup(&watch.enqueue_error);
  lc_error_cleanup(&watch.dequeue_error);
  lc_error_cleanup(&error);
}

static void test_disk_local_mutate_stream_roundtrip(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_error error;
  lc_acquire_req acquire_req;
  char key[96];
  char text_template[] = "/tmp/liblockdc-local-mutate-text-XXXXXX";
  char binary_template[] = "/tmp/liblockdc-local-mutate-bin-XXXXXX";
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./devenv/volumes/lockd-disk-a-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_test_tmp_cleanup_stale("/tmp", "liblockdc-local-mutate-",
                            LOCAL_MUTATE_TMP_PREFIX);
  write_temp_file_or_die(text_template, LOCAL_MUTATE_TMP_PREFIX,
                         (const unsigned char *)"hello\n\"quoted\"",
                         sizeof("hello\n\"quoted\"") - 1U);
  write_temp_file_or_die(binary_template, LOCAL_MUTATE_TMP_PREFIX,
                         (const unsigned char[]){0x00, 0x01, 0x02, 'a'}, 4U);

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("disk-local-mutate", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  save_json_text_or_die(
      lease, "{\"kind\":\"e2e-local\",\"counter\":1,\"remove_me\":true}",
      &error);
  assert_local_mutate_file_variants(lease, text_template, binary_template,
                                    &error);

  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  lc_test_tmp_cleanup_path(text_template, LOCAL_MUTATE_TMP_PREFIX);
  lc_test_tmp_cleanup_path(binary_template, LOCAL_MUTATE_TMP_PREFIX);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_s3_local_mutate_stream_roundtrip(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_error error;
  lc_acquire_req acquire_req;
  char key[96];
  char text_template[] = "/tmp/liblockdc-s3-local-mutate-text-XXXXXX";
  char binary_template[] = "/tmp/liblockdc-s3-local-mutate-bin-XXXXXX";
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_S3_ENDPOINT", "https://localhost:19443");
  bundle_path = env_or_default("LOCKDC_E2E_S3_BUNDLE",
                               "./devenv/volumes/lockd-s3-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_test_tmp_cleanup_stale("/tmp", "liblockdc-s3-local-mutate-",
                            S3_LOCAL_MUTATE_TMP_PREFIX);
  write_temp_file_or_die(text_template, S3_LOCAL_MUTATE_TMP_PREFIX,
                         (const unsigned char *)"hello\n\"quoted\"",
                         sizeof("hello\n\"quoted\"") - 1U);
  write_temp_file_or_die(binary_template, S3_LOCAL_MUTATE_TMP_PREFIX,
                         (const unsigned char[]){0x00, 0x01, 0x02, 'a'}, 4U);

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("s3-local-mutate", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  save_json_text_or_die(
      lease, "{\"kind\":\"e2e-local\",\"counter\":1,\"remove_me\":true}",
      &error);
  assert_local_mutate_file_variants(lease, text_template, binary_template,
                                    &error);

  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  lc_test_tmp_cleanup_path(text_template, S3_LOCAL_MUTATE_TMP_PREFIX);
  lc_test_tmp_cleanup_path(binary_template, S3_LOCAL_MUTATE_TMP_PREFIX);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_acquire_if_not_exists_conflict(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_lease *conflicting_lease;
  lc_error error;
  lc_acquire_req acquire_req;
  char key[96];
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./devenv/volumes/lockd-disk-a-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  conflicting_lease = NULL;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("disk-conflict", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e";
  acquire_req.ttl_seconds = 30L;
  acquire_req.if_not_exists = 1;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  rc = client->acquire(client, &acquire_req, &conflicting_lease, &error);
  assert_lc_server_error(rc, &error, 409L);
  assert_null(conflicting_lease);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);

  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_state_cas_failure_modes(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_update_opts update_opts;
  lc_source *src;
  lc_mutate_req mutate_req;
  lc_metadata_req metadata_req;
  lc_remove_req remove_req;
  const char *mutations[1];
  char key[96];
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./devenv/volumes/lockd-disk-a-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  src = NULL;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_update_opts_init(&update_opts);
  lc_mutate_req_init(&mutate_req);
  lc_metadata_req_init(&metadata_req);
  lc_remove_req_init(&remove_req);

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("disk-cas", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  save_json_text_or_die(lease, "{\"kind\":\"cas\"}", &error);

  rc = lc_source_from_memory("{\"kind\":\"cas-update\"}",
                             strlen("{\"kind\":\"cas-update\"}"), &src, &error);
  assert_lc_ok(rc, &error);
  update_opts.if_version = lease->version + 100L;
  update_opts.has_if_version = 1;
  rc = lease->update(lease, src, &update_opts, &error);
  assert_lc_server_error(rc, &error, 409L);
  lc_source_close(src);
  src = NULL;
  lc_error_cleanup(&error);
  lc_error_init(&error);

  mutations[0] = "/kind=\"cas-mutate\"";
  mutate_req.mutations = mutations;
  mutate_req.mutation_count = 1U;
  mutate_req.if_version = lease->version + 100L;
  mutate_req.has_if_version = 1;
  rc = lease->mutate(lease, &mutate_req, &error);
  assert_lc_server_error(rc, &error, 409L);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  metadata_req.has_query_hidden = 1;
  metadata_req.query_hidden = 1;
  metadata_req.if_version = lease->version + 100L;
  metadata_req.has_if_version = 1;
  rc = lease->metadata(lease, &metadata_req, &error);
  assert_lc_server_error(rc, &error, 409L);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  remove_req.if_state_etag = "stale-etag";
  rc = lease->remove(lease, &remove_req, &error);
  assert_lc_server_error(rc, &error, 409L);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);

  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_query_rejects_invalid_inputs(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_error error;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_sink *sink;
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./devenv/volumes/lockd-disk-a-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  sink = NULL;
  lc_error_init(&error);
  lc_query_req_init(&query_req);
  memset(&query_res, 0, sizeof(query_res));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  query_req.selector_json = NULL;
  query_req.limit = 1L;
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = client->query(client, &query_req, sink, &query_res, &error);
  assert_int_not_equal(rc, LC_OK);
  assert_int_equal(error.code, LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_query_res_cleanup(&query_res);
  memset(&query_res, 0, sizeof(query_res));

  query_req.selector_json = "";
  rc = client->query(client, &query_req, sink, &query_res, &error);
  assert_int_not_equal(rc, LC_OK);
  assert_int_equal(error.code, LC_ERR_INVALID);

  lc_sink_close(sink);
  lc_query_res_cleanup(&query_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_query_documents_captures_lockd_trailers(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_release_req release_req;
  lc_index_flush_req flush_req;
  lc_index_flush_res flush_res;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  char key_a[96];
  char key_b[96];
  char kind[96];
  char json_text[256];
  char selector_json[192];
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./devenv/volumes/lockd-disk-a-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_release_req_init(&release_req);
  lc_index_flush_req_init(&flush_req);
  memset(&flush_res, 0, sizeof(flush_res));
  lc_query_req_init(&query_req);
  memset(&query_res, 0, sizeof(query_res));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("disk-query-trailer-kind", kind, sizeof(kind));
  make_unique_name("disk-query-trailer-a", key_a, sizeof(key_a));
  make_unique_name("disk-query-trailer-b", key_b, sizeof(key_b));

  acquire_req.owner = "lc-e2e-query-trailers";
  acquire_req.ttl_seconds = 30L;

  acquire_req.key = key_a;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  snprintf(json_text, sizeof(json_text),
           "{\"kind\":\"%s\",\"ordinal\":1,\"status\":\"ready\"}", kind);
  save_json_text_or_die(lease, json_text, &error);
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  acquire_req.key = key_b;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  snprintf(json_text, sizeof(json_text),
           "{\"kind\":\"%s\",\"ordinal\":2,\"status\":\"ready\"}", kind);
  save_json_text_or_die(lease, json_text, &error);
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  flush_req.mode = "wait";
  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_lc_ok(rc, &error);
  assert_true(flush_res.flushed);

  snprintf(selector_json, sizeof(selector_json),
           "{\"eq\":{\"field\":\"/kind\",\"value\":\"%s\"}}", kind);
  query_req.selector_json = selector_json;
  query_req.limit = 1L;
  query_req.return_mode = "documents";
  query_req.engine = "index";
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = client->query(client, &query_req, sink, &query_res, &error);
  assert_lc_ok(rc, &error);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_true(length > 0U);
  assert_true(buffer_contains(bytes, length, kind));
  assert_true(buffer_contains(bytes, length, "\"status\":\"ready\""));
  assert_non_null(query_res.metadata_json);
  assert_true(buffer_contains(query_res.metadata_json,
                              strlen(query_res.metadata_json),
                              "query_candidates"));
  assert_string_equal(query_res.return_mode, "documents");

  lc_sink_close(sink);
  lc_query_res_cleanup(&query_res);
  lc_index_flush_res_cleanup(&flush_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_query_keys_streams_many_indexed_keys(void **state) {
  enum { key_count = 512 };
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_release_req release_req;
  lc_index_flush_req flush_req;
  lc_index_flush_res flush_res;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_query_key_handler handler;
  query_keys_e2e_capture capture;
  char kind[96];
  char key_prefix[128];
  char key[160];
  char json_text[256];
  char selector_json[192];
  size_t index;
  size_t seen_count;
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./devenv/volumes/lockd-disk-a-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_release_req_init(&release_req);
  lc_index_flush_req_init(&flush_req);
  memset(&flush_res, 0, sizeof(flush_res));
  lc_query_req_init(&query_req);
  memset(&query_res, 0, sizeof(query_res));
  memset(&handler, 0, sizeof(handler));
  memset(&capture, 0, sizeof(capture));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("disk-query-keys-kind", kind, sizeof(kind));
  snprintf(key_prefix, sizeof(key_prefix), "e2e/query-keys/%s/", kind);
  acquire_req.owner = "lc-e2e-query-keys";
  acquire_req.ttl_seconds = 120L;

  for (index = 0U; index < (size_t)key_count; ++index) {
    snprintf(key, sizeof(key), "%s%04zu", key_prefix, index);
    acquire_req.key = key;
    rc = client->acquire(client, &acquire_req, &lease, &error);
    assert_lc_ok(rc, &error);
    snprintf(json_text, sizeof(json_text),
             "{\"kind\":\"%s\",\"ordinal\":%zu,\"status\":\"ready\"}", kind,
             index);
    save_json_text_or_die(lease, json_text, &error);
    rc = lease->release(lease, &release_req, &error);
    assert_lc_ok(rc, &error);
    lease = NULL;
  }

  flush_req.mode = "wait";
  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_lc_ok(rc, &error);
  assert_true(flush_res.flushed);

  snprintf(selector_json, sizeof(selector_json),
           "{\"eq\":{\"field\":\"/kind\",\"value\":\"%s\"}}", kind);
  query_req.selector_json = selector_json;
  query_req.limit = key_count;
  query_req.engine = "index";
  handler.begin = query_keys_e2e_begin;
  handler.chunk = query_keys_e2e_chunk;
  handler.end = query_keys_e2e_end;
  capture.key_prefix = key_prefix;
  capture.key_prefix_len = strlen(key_prefix);
  capture.expected_count = key_count;
  rc =
      lc_query_keys(client, &query_req, &handler, &capture, &query_res, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(capture.begin_calls, (size_t)key_count);
  assert_int_equal(capture.end_calls, (size_t)key_count);
  assert_true(capture.chunk_calls >= (size_t)key_count);
  assert_true(capture.total_bytes >=
              (strlen(key_prefix) + 4U) * (size_t)key_count);
  seen_count = 0U;
  for (index = 0U; index < (size_t)key_count; ++index) {
    seen_count += capture.seen[index] != 0U ? 1U : 0U;
  }
  assert_int_equal(seen_count, (size_t)key_count);

  lc_query_res_cleanup(&query_res);
  lc_index_flush_res_cleanup(&flush_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_s3_attachment_failure_modes(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_lease *lease;
  lc_source *src;
  lc_sink *sink;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_attach_req attach_req;
  lc_attach_res attach_res;
  lc_attachment_get_req get_req;
  lc_attachment_get_res get_res;
  char key[96];
  static const unsigned char payload[] = {'a', 't', 't', 'a', 'c', 'h'};
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_S3_ENDPOINT", "https://localhost:19443");
  bundle_path = env_or_default("LOCKDC_E2E_S3_BUNDLE",
                               "./devenv/volumes/lockd-s3-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lease = NULL;
  src = NULL;
  sink = NULL;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_attach_req_init(&attach_req);
  memset(&attach_res, 0, sizeof(attach_res));
  lc_attachment_get_req_init(&get_req);
  memset(&get_res, 0, sizeof(get_res));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("s3-attach-fail", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  save_json_text_or_die(lease, "{\"kind\":\"attachment-failure\"}", &error);

  rc = lc_source_from_memory(payload, sizeof(payload), &src, &error);
  assert_lc_ok(rc, &error);
  attach_req.name = "blob.txt";
  attach_req.content_type = "text/plain";
  attach_req.has_max_bytes = 1;
  attach_req.max_bytes = 4L;
  rc = lease->attach(lease, &attach_req, src, &attach_res, &error);
  assert_lc_server_error(rc, &error, 413L);
  lc_source_close(src);
  src = NULL;
  lc_error_cleanup(&error);
  lc_error_init(&error);

  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  get_req.selector.name = "missing.txt";
  rc = lease->get_attachment(lease, &get_req, sink, &get_res, &error);
  assert_lc_server_error(rc, &error, 404L);

  lc_sink_close(sink);
  lc_attachment_get_res_cleanup(&get_res);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);

  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_mem_uds_queue_failure_modes(void **state) {
  const char *socket_path;
  lc_client *client;
  lc_source *src;
  lc_message *message;
  lc_message *invalid_message;
  lc_error error;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_dequeue_req invalid_dequeue_req;
  lc_nack_req nack_req;
  char queue_name[96];
  static const unsigned char payload[] = {'q', 'u', 'e', 'u', 'e',
                                          '-', 'n', 'e', 'g'};
  int rc;

  (void)state;
  socket_path = env_or_default("LOCKDC_E2E_MEM_SOCKET",
                               "./devenv/volumes/lockd-mem-run/lockd.sock");
  require_socket_or_skip(socket_path);

  client = NULL;
  src = NULL;
  message = NULL;
  invalid_message = NULL;
  lc_error_init(&error);
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_dequeue_req_init(&dequeue_req);
  lc_dequeue_req_init(&invalid_dequeue_req);
  lc_nack_req_init(&nack_req);

  open_uds_client(socket_path, &client, &error);
  make_unique_name("mem-queue-fail", queue_name, sizeof(queue_name));

  rc = lc_source_from_memory(payload, sizeof(payload), &src, &error);
  assert_lc_ok(rc, &error);
  enqueue_req.queue = queue_name;
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 300L;
  enqueue_req.max_attempts = 5;
  rc = client->enqueue(client, &enqueue_req, src, &enqueue_res, &error);
  assert_lc_ok(rc, &error);
  lc_source_close(src);
  src = NULL;
  lc_enqueue_res_cleanup(&enqueue_res);

  dequeue_req.owner = "lc-e2e-worker";
  dequeue_req.queue = queue_name;
  dequeue_req.wait_seconds = 2L;
  dequeue_req.visibility_timeout_seconds = 30L;
  rc = client->dequeue(client, &dequeue_req, &message, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(message);

  nack_req.intent = (lc_nack_intent)99;
  rc = message->nack(message, &nack_req, &error);
  assert_int_not_equal(rc, LC_OK);
  assert_int_equal(error.code, LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  invalid_dequeue_req.queue = queue_name;
  invalid_dequeue_req.wait_seconds = 1L;
  invalid_dequeue_req.visibility_timeout_seconds = 30L;
  rc = client->dequeue(client, &invalid_dequeue_req, &invalid_message, &error);
  assert_int_not_equal(rc, LC_OK);
  assert_int_equal(error.code, LC_ERR_INVALID);
  assert_null(invalid_message);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  rc = message->ack(message, &error);
  assert_lc_ok(rc, &error);
  message = NULL;

  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_management_failure_modes(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_error error;
  lc_tc_lease_acquire_req acquire_req;
  lc_tc_lease_acquire_res acquire_res;
  lc_tc_cluster_res cluster_res;
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./devenv/volumes/lockd-disk-a-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  lc_error_init(&error);
  lc_tc_lease_acquire_req_init(&acquire_req);
  memset(&acquire_res, 0, sizeof(acquire_res));
  memset(&cluster_res, 0, sizeof(cluster_res));

  open_tcp_client(endpoint, bundle_path, &client, &error);

  acquire_req.candidate_id = "rw-client";
  acquire_req.candidate_endpoint = "https://example.invalid:9443";
  acquire_req.term = 1UL;
  acquire_req.ttl_ms = 1000L;
  rc = client->tc_lease_acquire(client, &acquire_req, &acquire_res, &error);
  assert_lc_server_error(rc, &error, 403L);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  rc = client->tc_cluster_list(client, &cluster_res, &error);
  assert_lc_server_error(rc, &error, 403L);

  lc_tc_lease_acquire_res_cleanup(&acquire_res);
  lc_tc_cluster_res_cleanup(&cluster_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_auth_permission_failure_modes(void **state) {
  const char *endpoint;
  const char *rw_bundle_path;
  const char *tc_bundle_path;
  lc_client *rw_client;
  lc_client *tc_client;
  lc_error error;
  lc_tc_lease_acquire_req tc_req;
  lc_tc_lease_acquire_res tc_res;
  lc_acquire_req acquire_req;
  lc_lease *lease;
  char key[96];
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  rw_bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./devenv/volumes/lockd-disk-a-config/client.pem");
  tc_bundle_path = "./devenv/volumes/lockd-disk-a-config/tc-client.pem";
  require_file_or_skip(rw_bundle_path);
  require_file_or_skip(tc_bundle_path);

  rw_client = NULL;
  tc_client = NULL;
  lease = NULL;
  lc_error_init(&error);
  lc_tc_lease_acquire_req_init(&tc_req);
  memset(&tc_res, 0, sizeof(tc_res));
  lc_acquire_req_init(&acquire_req);

  open_tcp_client(endpoint, rw_bundle_path, &rw_client, &error);
  tc_req.candidate_id = "lc-e2e-tc";
  tc_req.candidate_endpoint = "https://example.invalid:9443";
  tc_req.term = 1UL;
  tc_req.ttl_ms = 1000L;
  rc = rw_client->tc_lease_acquire(rw_client, &tc_req, &tc_res, &error);
  assert_lc_server_error(rc, &error, 403L);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  open_tcp_client(endpoint, tc_bundle_path, &tc_client, &error);
  make_unique_name("tc-no-data", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e";
  acquire_req.ttl_seconds = 30L;
  rc = tc_client->acquire(tc_client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);
  rc = lease->release(lease, NULL, &error);
  assert_lc_ok(rc, &error);

  lc_tc_lease_acquire_res_cleanup(&tc_res);
  lc_client_close(tc_client);
  lc_client_close(rw_client);
  lc_error_cleanup(&error);
}

typedef struct e2e_consumer_backend {
  const char *label;
  const char *endpoint_env;
  const char *endpoint_default;
  const char *bundle_env;
  const char *bundle_default;
  const char *socket_env;
  const char *socket_default;
  int use_uds;
} e2e_consumer_backend;

typedef struct e2e_consumer_context {
  pthread_mutex_t mutex;
  int handled;
  int error_events;
  int start_events;
  int stop_events;
  int first_delivery_mode;
  int expect_state;
  int read_payload;
  int saw_state;
  long last_visibility_timeout_seconds;
  int last_error_code;
  long last_error_http_status;
  char last_error_server_code[96];
  char last_error_message[192];
  const char *backend_label;
  const char *queue_name;
  int enqueue_count;
  size_t worker_count;
  int max_failures;
  int expected_minimum_count;
  int queue_stats_available;
  long queue_stats_waiting_consumers;
  long queue_stats_pending_candidates;
  long queue_stats_total_consumers;
  int queue_stats_has_active_watcher;
  long queue_stats_head_enqueued_at_unix;
  long queue_stats_head_not_visible_until_unix;
  long queue_stats_head_age_seconds;
  char queue_stats_head_message_id[160];
  char queue_stats_correlation_id[96];
  char payload[64];
  char state_json[128];
  char state_key[160];
} e2e_consumer_context;

enum {
  E2E_CONSUMER_FIRST_DELIVERY_NONE = 0,
  E2E_CONSUMER_FIRST_DELIVERY_FAIL = 1,
  E2E_CONSUMER_FIRST_DELIVERY_DEFER = 2,
  E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE = 3,
  E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE_ERROR = 4,
  E2E_CONSUMER_FIRST_DELIVERY_FAIL_TWICE = 5,
  E2E_CONSUMER_FIRST_DELIVERY_ACK_ERROR = 6,
  E2E_CONSUMER_FIRST_DELIVERY_ACK = 7
};

static const e2e_consumer_backend e2e_backend_disk = {
    "disk",
    "LOCKDC_E2E_DISK_A_ENDPOINT",
    "https://127.0.0.1:19441",
    "LOCKDC_E2E_DISK_A_BUNDLE",
    "./devenv/volumes/lockd-disk-a-config/client.pem",
    NULL,
    NULL,
    0};

static const e2e_consumer_backend e2e_backend_s3 = {
    "s3",
    "LOCKDC_E2E_S3_ENDPOINT",
    "https://127.0.0.1:19443",
    "LOCKDC_E2E_S3_BUNDLE",
    "./devenv/volumes/lockd-s3-config/client.pem",
    NULL,
    NULL,
    0};

static const e2e_consumer_backend e2e_backend_mem = {
    "mem",
    NULL,
    NULL,
    NULL,
    NULL,
    "LOCKDC_E2E_MEM_SOCKET",
    "./devenv/volumes/lockd-mem-run/lockd.sock",
    1};

static const char *e2e_consumer_delivery_mode_name(int mode) {
  switch (mode) {
  case E2E_CONSUMER_FIRST_DELIVERY_NONE:
    return "none";
  case E2E_CONSUMER_FIRST_DELIVERY_FAIL:
    return "fail";
  case E2E_CONSUMER_FIRST_DELIVERY_DEFER:
    return "defer";
  case E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE:
    return "nack_failure";
  case E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE_ERROR:
    return "nack_failure_error";
  case E2E_CONSUMER_FIRST_DELIVERY_FAIL_TWICE:
    return "fail_twice";
  case E2E_CONSUMER_FIRST_DELIVERY_ACK_ERROR:
    return "ack_error";
  case E2E_CONSUMER_FIRST_DELIVERY_ACK:
    return "ack";
  default:
    return "unknown";
  }
}

static void e2e_consumer_capture_queue_stats(lc_client *client,
                                             const char *queue_name,
                                             e2e_consumer_context *context) {
  lc_queue_stats_req req;
  lc_queue_stats_res res;
  lc_error error;
  int rc;

  if (client == NULL || queue_name == NULL || context == NULL) {
    return;
  }

  lc_queue_stats_req_init(&req);
  memset(&res, 0, sizeof(res));
  lc_error_init(&error);
  req.queue = queue_name;
  rc = client->queue_stats(client, &req, &res, &error);

  pthread_mutex_lock(&context->mutex);
  if (rc == LC_OK) {
    context->queue_stats_available = res.available;
    context->queue_stats_waiting_consumers = res.waiting_consumers;
    context->queue_stats_pending_candidates = res.pending_candidates;
    context->queue_stats_total_consumers = res.total_consumers;
    context->queue_stats_has_active_watcher = res.has_active_watcher;
    context->queue_stats_head_enqueued_at_unix = res.head_enqueued_at_unix;
    context->queue_stats_head_not_visible_until_unix =
        res.head_not_visible_until_unix;
    context->queue_stats_head_age_seconds = res.head_age_seconds;
    memset(context->queue_stats_head_message_id, 0,
           sizeof(context->queue_stats_head_message_id));
    if (res.head_message_id != NULL) {
      strncpy(context->queue_stats_head_message_id, res.head_message_id,
              sizeof(context->queue_stats_head_message_id) - 1U);
    }
    memset(context->queue_stats_correlation_id, 0,
           sizeof(context->queue_stats_correlation_id));
    if (res.correlation_id != NULL) {
      strncpy(context->queue_stats_correlation_id, res.correlation_id,
              sizeof(context->queue_stats_correlation_id) - 1U);
    }
  } else {
    context->queue_stats_available = -1;
    context->queue_stats_waiting_consumers = -1L;
    context->queue_stats_pending_candidates = -1L;
    context->queue_stats_total_consumers = -1L;
    context->queue_stats_has_active_watcher = 0;
    context->queue_stats_head_enqueued_at_unix = -1L;
    context->queue_stats_head_not_visible_until_unix = -1L;
    context->queue_stats_head_age_seconds = -1L;
    memset(context->queue_stats_head_message_id, 0,
           sizeof(context->queue_stats_head_message_id));
    memset(context->queue_stats_correlation_id, 0,
           sizeof(context->queue_stats_correlation_id));
    if (error.correlation_id != NULL) {
      strncpy(context->queue_stats_correlation_id, error.correlation_id,
              sizeof(context->queue_stats_correlation_id) - 1U);
    }
  }
  pthread_mutex_unlock(&context->mutex);

  lc_queue_stats_res_cleanup(&res);
  lc_error_cleanup(&error);
}

static void open_consumer_backend_client(const e2e_consumer_backend *backend,
                                         lc_client **out, lc_error *error) {
  const char *endpoint;
  const char *bundle_path;
  const char *socket_path;

  if (backend->use_uds) {
    socket_path = env_or_default(backend->socket_env, backend->socket_default);
    require_socket_or_skip(socket_path);
    open_uds_client(socket_path, out, error);
    return;
  }

  endpoint = env_or_default(backend->endpoint_env, backend->endpoint_default);
  bundle_path = env_or_default(backend->bundle_env, backend->bundle_default);
  require_file_or_skip(bundle_path);
  open_tcp_client(endpoint, bundle_path, out, error);
}

static int e2e_consumer_record_payload(e2e_consumer_context *consumer_context,
                                       const void *bytes, size_t length) {
  pthread_mutex_lock(&consumer_context->mutex);
  memset(consumer_context->payload, 0, sizeof(consumer_context->payload));
  if (length >= sizeof(consumer_context->payload)) {
    length = sizeof(consumer_context->payload) - 1U;
  }
  memcpy(consumer_context->payload, bytes, length);
  consumer_context->handled += 1;
  pthread_mutex_unlock(&consumer_context->mutex);
  return LC_OK;
}

static void e2e_sleep_ms(long delay_ms) {
  struct timespec ts;

  if (delay_ms <= 0L) {
    return;
  }
  ts.tv_sec = delay_ms / 1000L;
  ts.tv_nsec = (delay_ms % 1000L) * 1000000L;
  while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {
  }
}

static int e2e_consumer_persist_state(e2e_consumer_context *consumer_context,
                                      lc_lease *state, lc_error *error) {
  e2e_status_doc doc;
  lc_get_res get_res;
  int rc;

  if (state == NULL) {
    return LC_OK;
  }

  memset(&doc, 0, sizeof(doc));
  memset(&get_res, 0, sizeof(get_res));
  doc.status = "from-consumer-service";
  rc = state->save(state, &e2e_status_map, &doc, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&doc, 0, sizeof(doc));
  rc = state->load(state, &e2e_status_map, &doc, NULL, &get_res, error);
  if (rc != LC_OK) {
    lc_get_res_cleanup(&get_res);
    return rc;
  }

  pthread_mutex_lock(&consumer_context->mutex);
  consumer_context->saw_state = 1;
  memset(consumer_context->state_json, 0, sizeof(consumer_context->state_json));
  if (doc.status != NULL) {
    strncpy(consumer_context->state_json, doc.status,
            sizeof(consumer_context->state_json) - 1U);
  }
  memset(consumer_context->state_key, 0, sizeof(consumer_context->state_key));
  if (state->key != NULL) {
    strncpy(consumer_context->state_key, state->key,
            sizeof(consumer_context->state_key) - 1U);
  }
  pthread_mutex_unlock(&consumer_context->mutex);

  e2e_lonejson_cleanup(&e2e_status_map, &doc);
  lc_get_res_cleanup(&get_res);
  return LC_OK;
}

static int e2e_consumer_handle(void *context, lc_consumer_message *delivery,
                               lc_error *error) {
  e2e_consumer_context *consumer_context;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  int handled_count;
  lc_lease *state_lease;
  int rc;
  lc_nack_req nack_req;

  consumer_context = (e2e_consumer_context *)context;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  handled_count = 0;
  if (consumer_context->read_payload) {
    rc = lc_sink_to_memory(&sink, error);
    if (rc != LC_OK) {
      return rc;
    }
    rc = delivery->message->write_payload(delivery->message, sink, NULL, error);
    if (rc != LC_OK) {
      lc_sink_close(sink);
      return rc;
    }
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
    if (rc == LC_OK) {
      rc = e2e_consumer_record_payload(consumer_context, bytes, length);
    }
    lc_sink_close(sink);
    if (rc != LC_OK) {
      return rc;
    }
  } else {
    pthread_mutex_lock(&consumer_context->mutex);
    consumer_context->handled += 1;
    pthread_mutex_unlock(&consumer_context->mutex);
  }

  pthread_mutex_lock(&consumer_context->mutex);
  consumer_context->last_visibility_timeout_seconds =
      delivery != NULL && delivery->message != NULL
          ? delivery->message->visibility_timeout_seconds
          : 0L;
  pthread_mutex_unlock(&consumer_context->mutex);

  state_lease = delivery->state;
  if (state_lease == NULL && delivery->message != NULL &&
      delivery->message->state != NULL) {
    state_lease = delivery->message->state(delivery->message);
  }
  if (consumer_context->expect_state && state_lease == NULL) {
    return LC_ERR_PROTOCOL;
  }
  rc = e2e_consumer_persist_state(consumer_context, state_lease, error);
  if (rc != LC_OK) {
    return rc;
  }

  pthread_mutex_lock(&consumer_context->mutex);
  handled_count = consumer_context->handled;
  pthread_mutex_unlock(&consumer_context->mutex);
  if (consumer_context->first_delivery_mode ==
          E2E_CONSUMER_FIRST_DELIVERY_FAIL &&
      handled_count == 1) {
    e2e_sleep_ms(1500L);
    return LC_ERR_TRANSPORT;
  }
  if (consumer_context->first_delivery_mode ==
          E2E_CONSUMER_FIRST_DELIVERY_FAIL_TWICE &&
      handled_count <= 2) {
    e2e_sleep_ms(1500L);
    return LC_ERR_TRANSPORT;
  }
  if (consumer_context->first_delivery_mode ==
          E2E_CONSUMER_FIRST_DELIVERY_DEFER &&
      handled_count == 1) {
    lc_nack_req_init(&nack_req);
    nack_req.intent = LC_NACK_INTENT_DEFER;
    nack_req.delay_seconds = 0L;
    return delivery->message->nack(delivery->message, &nack_req, error);
  }
  if (consumer_context->first_delivery_mode ==
          E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE &&
      handled_count == 1) {
    lc_nack_req_init(&nack_req);
    nack_req.intent = LC_NACK_INTENT_FAILURE;
    nack_req.delay_seconds = 0L;
    return delivery->message->nack(delivery->message, &nack_req, error);
  }
  if (consumer_context->first_delivery_mode ==
          E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE_ERROR &&
      handled_count == 1) {
    lc_nack_req_init(&nack_req);
    nack_req.intent = LC_NACK_INTENT_FAILURE;
    nack_req.delay_seconds = 0L;
    rc = delivery->message->nack(delivery->message, &nack_req, error);
    if (rc != LC_OK) {
      return rc;
    }
    return LC_ERR_TRANSPORT;
  }
  if (consumer_context->first_delivery_mode ==
          E2E_CONSUMER_FIRST_DELIVERY_ACK_ERROR &&
      handled_count == 1) {
    rc = delivery->message->ack(delivery->message, error);
    if (rc != LC_OK) {
      return rc;
    }
    return LC_ERR_TRANSPORT;
  }
  if (consumer_context->first_delivery_mode ==
          E2E_CONSUMER_FIRST_DELIVERY_ACK &&
      handled_count == 1) {
    return delivery->message->ack(delivery->message, error);
  }

  return LC_OK;
}

static int e2e_consumer_on_error(void *context, const lc_consumer_error *event,
                                 lc_error *error) {
  e2e_consumer_context *consumer_context;

  consumer_context = (e2e_consumer_context *)context;
  pthread_mutex_lock(&consumer_context->mutex);
  consumer_context->error_events += 1;
  consumer_context->last_error_code =
      event != NULL && event->cause != NULL ? event->cause->code : LC_OK;
  consumer_context->last_error_http_status =
      event != NULL && event->cause != NULL ? event->cause->http_status : 0L;
  memset(consumer_context->last_error_server_code, 0,
         sizeof(consumer_context->last_error_server_code));
  if (event != NULL && event->cause != NULL &&
      event->cause->server_code != NULL) {
    strncpy(consumer_context->last_error_server_code, event->cause->server_code,
            sizeof(consumer_context->last_error_server_code) - 1U);
  }
  memset(consumer_context->last_error_message, 0,
         sizeof(consumer_context->last_error_message));
  if (event != NULL && event->cause != NULL && event->cause->message != NULL) {
    strncpy(consumer_context->last_error_message, event->cause->message,
            sizeof(consumer_context->last_error_message) - 1U);
  }
  pthread_mutex_unlock(&consumer_context->mutex);
  (void)error;
  return LC_OK;
}

static void e2e_consumer_on_start(void *context,
                                  const lc_consumer_lifecycle_event *event) {
  e2e_consumer_context *consumer_context;

  (void)event;
  consumer_context = (e2e_consumer_context *)context;
  pthread_mutex_lock(&consumer_context->mutex);
  consumer_context->start_events += 1;
  pthread_mutex_unlock(&consumer_context->mutex);
}

static void e2e_consumer_on_stop(void *context,
                                 const lc_consumer_lifecycle_event *event) {
  e2e_consumer_context *consumer_context;

  (void)event;
  consumer_context = (e2e_consumer_context *)context;
  pthread_mutex_lock(&consumer_context->mutex);
  consumer_context->stop_events += 1;
  pthread_mutex_unlock(&consumer_context->mutex);
}

static void wait_for_consumer_handled(e2e_consumer_context *consumer_context,
                                      int minimum_count) {
  int maximum_retries;
  int retries;

  maximum_retries = 120;
  if (minimum_count > 1) {
    maximum_retries = 600;
  }
  retries = 0;
  for (;;) {
    pthread_mutex_lock(&consumer_context->mutex);
    if (consumer_context->handled >= minimum_count &&
        (!consumer_context->expect_state || consumer_context->saw_state)) {
      pthread_mutex_unlock(&consumer_context->mutex);
      return;
    }
    pthread_mutex_unlock(&consumer_context->mutex);
    if (retries++ > maximum_retries) {
      fail_msg(
          "consumer retry timed out: backend=%s queue=%s mode=%s "
          "enqueue_count=%d worker_count=%lu max_failures=%d "
          "expected_minimum_count=%d handled=%d start_events=%d "
          "stop_events=%d error_events=%d last_error_code=%d "
          "last_error_http_status=%ld last_error_server_code=%s "
          "last_error_message=%s last_visibility_timeout_seconds=%ld "
          "payload=%s state_json=%s state_key=%s "
          "queue_available=%d waiting_consumers=%ld "
          "pending_candidates=%ld total_consumers=%ld "
          "has_active_watcher=%d head_message_id=%s "
          "head_enqueued_at_unix=%ld head_not_visible_until_unix=%ld "
          "head_age_seconds=%ld stats_cid=%s",
          consumer_context->backend_label, consumer_context->queue_name,
          e2e_consumer_delivery_mode_name(
              consumer_context->first_delivery_mode),
          consumer_context->enqueue_count,
          (unsigned long)consumer_context->worker_count,
          consumer_context->max_failures,
          consumer_context->expected_minimum_count, consumer_context->handled,
          consumer_context->start_events, consumer_context->stop_events,
          consumer_context->error_events, consumer_context->last_error_code,
          consumer_context->last_error_http_status,
          consumer_context->last_error_server_code,
          consumer_context->last_error_message,
          consumer_context->last_visibility_timeout_seconds,
          consumer_context->payload, consumer_context->state_json,
          consumer_context->state_key, consumer_context->queue_stats_available,
          consumer_context->queue_stats_waiting_consumers,
          consumer_context->queue_stats_pending_candidates,
          consumer_context->queue_stats_total_consumers,
          consumer_context->queue_stats_has_active_watcher,
          consumer_context->queue_stats_head_message_id,
          consumer_context->queue_stats_head_enqueued_at_unix,
          consumer_context->queue_stats_head_not_visible_until_unix,
          consumer_context->queue_stats_head_age_seconds,
          consumer_context->queue_stats_correlation_id);
    }
    usleep(100000);
  }
}

static void run_consumer_service_variant_with_max_failures(
    const e2e_consumer_backend *backend, const char *name_prefix,
    int enqueue_count, int first_delivery_mode, int expect_state,
    size_t worker_count, int max_failures, int read_payload) {
  lc_client *client;
  lc_consumer_service *service;
  lc_error error;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_consumer_config consumer;
  lc_consumer_service_config service_config;
  e2e_consumer_context consumer_context;
  char queue_name[96];
  static const unsigned char payload[] = {'c', 'o', 'n', 's', 'u', 'm',
                                          'e', 'r', '-', 'o', 'k'};
  int rc;
  int i;

  client = NULL;
  service = NULL;
  lc_error_init(&error);
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&service_config);
  memset(&consumer_context, 0, sizeof(consumer_context));
  consumer_context.backend_label = backend->label;
  consumer_context.first_delivery_mode = first_delivery_mode;
  consumer_context.expect_state = expect_state;
  consumer_context.read_payload = read_payload;
  consumer_context.enqueue_count = enqueue_count;
  consumer_context.worker_count = worker_count;
  consumer_context.max_failures = max_failures;
  consumer_context.queue_stats_available = -1;
  consumer_context.queue_stats_waiting_consumers = -1L;
  consumer_context.queue_stats_pending_candidates = -1L;
  consumer_context.queue_stats_total_consumers = -1L;
  consumer_context.queue_stats_head_enqueued_at_unix = -1L;
  consumer_context.queue_stats_head_not_visible_until_unix = -1L;
  consumer_context.queue_stats_head_age_seconds = -1L;
  assert_int_equal(pthread_mutex_init(&consumer_context.mutex, NULL), 0);

  open_consumer_backend_client(backend, &client, &error);
  make_unique_name(name_prefix, queue_name, sizeof(queue_name));
  consumer_context.queue_name = queue_name;

  consumer.name = backend->label;
  consumer.request.queue = queue_name;
  consumer.request.visibility_timeout_seconds =
      first_delivery_mode != E2E_CONSUMER_FIRST_DELIVERY_NONE ? 2L : 30L;
  consumer.request.wait_seconds = 1L;
  consumer.worker_count = worker_count;
  consumer.with_state = expect_state;
  consumer.handle = e2e_consumer_handle;
  consumer.on_error = e2e_consumer_on_error;
  consumer.on_start = e2e_consumer_on_start;
  consumer.on_stop = e2e_consumer_on_stop;
  consumer.context = &consumer_context;
  lc_consumer_restart_policy_init(&consumer.restart_policy);
  consumer.restart_policy.base_delay_ms = 100L;
  consumer.restart_policy.max_delay_ms = 250L;
  consumer.restart_policy.max_failures = max_failures;

  service_config.consumers = &consumer;
  service_config.consumer_count = 1U;
  rc = client->new_consumer_service(client, &service_config, &service, &error);
  if (rc != LC_OK) {
    fail_msg("consumer service create failed: backend=%s queue=%s mode=%s "
             "enqueue_count=%d worker_count=%lu max_failures=%d rc=%d "
             "error_code=%d http_status=%ld message=%s detail=%s",
             backend->label, queue_name,
             e2e_consumer_delivery_mode_name(first_delivery_mode),
             enqueue_count, (unsigned long)worker_count, max_failures, rc,
             error.code, error.http_status,
             error.message != NULL ? error.message : "",
             error.detail != NULL ? error.detail : "");
  }
  assert_lc_ok(rc, &error);
  assert_non_null(service);

  rc = service->start(service, &error);
  if (rc != LC_OK) {
    fail_msg("consumer service start failed: backend=%s queue=%s mode=%s "
             "enqueue_count=%d worker_count=%lu max_failures=%d rc=%d "
             "error_code=%d http_status=%ld message=%s detail=%s",
             backend->label, queue_name,
             e2e_consumer_delivery_mode_name(first_delivery_mode),
             enqueue_count, (unsigned long)worker_count, max_failures, rc,
             error.code, error.http_status,
             error.message != NULL ? error.message : "",
             error.detail != NULL ? error.detail : "");
  }
  assert_lc_ok(rc, &error);

  enqueue_req.queue = queue_name;
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 300L;
  enqueue_req.max_attempts = 5;
  for (i = 0; i < enqueue_count; ++i) {
    lc_source *src;

    src = NULL;
    rc = lc_source_from_memory(payload, sizeof(payload), &src, &error);
    assert_lc_ok(rc, &error);
    rc = client->enqueue(client, &enqueue_req, src, &enqueue_res, &error);
    assert_lc_ok(rc, &error);
    lc_source_close(src);
    lc_enqueue_res_cleanup(&enqueue_res);
    memset(&enqueue_res, 0, sizeof(enqueue_res));
  }

  {
    int minimum_count;

    minimum_count = enqueue_count;
    if (first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_FAIL ||
        first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_DEFER ||
        first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE ||
        first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE_ERROR) {
      minimum_count += 1;
    } else if (first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_FAIL_TWICE) {
      minimum_count += 2;
    }
    consumer_context.expected_minimum_count = minimum_count;
    wait_for_consumer_handled(&consumer_context, minimum_count);
  }

  e2e_consumer_capture_queue_stats(client, queue_name, &consumer_context);
  rc = (service->stop)(service);
  if (rc != LC_OK) {
    fail_msg("consumer service stop failed: backend=%s queue=%s mode=%s "
             "handled=%d error_events=%d start_events=%d stop_events=%d rc=%d",
             backend->label, queue_name,
             e2e_consumer_delivery_mode_name(first_delivery_mode),
             consumer_context.handled, consumer_context.error_events,
             consumer_context.start_events, consumer_context.stop_events, rc);
  }
  assert_int_equal(rc, LC_OK);
  rc = service->wait(service, &error);
  e2e_consumer_capture_queue_stats(client, queue_name, &consumer_context);
  if (rc != LC_OK) {
    fail_msg(
        "consumer service wait failed: backend=%s queue=%s mode=%s "
        "enqueue_count=%d worker_count=%lu max_failures=%d handled=%d "
        "start_events=%d stop_events=%d error_events=%d "
        "last_error_code=%d last_error_http_status=%ld "
        "last_error_server_code=%s last_error_message=%s "
        "queue_available=%d waiting_consumers=%ld pending_candidates=%ld "
        "total_consumers=%ld has_active_watcher=%d head_message_id=%s "
        "head_enqueued_at_unix=%ld head_not_visible_until_unix=%ld "
        "head_age_seconds=%ld "
        "stats_cid=%s rc=%d error_code=%d http_status=%ld message=%s detail=%s",
        backend->label, queue_name,
        e2e_consumer_delivery_mode_name(first_delivery_mode), enqueue_count,
        (unsigned long)worker_count, max_failures, consumer_context.handled,
        consumer_context.start_events, consumer_context.stop_events,
        consumer_context.error_events, consumer_context.last_error_code,
        consumer_context.last_error_http_status,
        consumer_context.last_error_server_code,
        consumer_context.last_error_message,
        consumer_context.queue_stats_available,
        consumer_context.queue_stats_waiting_consumers,
        consumer_context.queue_stats_pending_candidates,
        consumer_context.queue_stats_total_consumers,
        consumer_context.queue_stats_has_active_watcher,
        consumer_context.queue_stats_head_message_id,
        consumer_context.queue_stats_head_enqueued_at_unix,
        consumer_context.queue_stats_head_not_visible_until_unix,
        consumer_context.queue_stats_head_age_seconds,
        consumer_context.queue_stats_correlation_id, rc, error.code,
        error.http_status, error.message != NULL ? error.message : "",
        error.detail != NULL ? error.detail : "");
  }
  assert_lc_ok(rc, &error);

  pthread_mutex_lock(&consumer_context.mutex);
  if (!(consumer_context.start_events >= (int)worker_count)) {
    fail_msg("consumer start_events too low: backend=%s queue=%s mode=%s "
             "start_events=%d worker_count=%lu handled=%d error_events=%d",
             backend->label, queue_name,
             e2e_consumer_delivery_mode_name(first_delivery_mode),
             consumer_context.start_events, (unsigned long)worker_count,
             consumer_context.handled, consumer_context.error_events);
  }
  if (!(consumer_context.stop_events >= (int)worker_count)) {
    fail_msg("consumer stop_events too low: backend=%s queue=%s mode=%s "
             "stop_events=%d worker_count=%lu handled=%d error_events=%d",
             backend->label, queue_name,
             e2e_consumer_delivery_mode_name(first_delivery_mode),
             consumer_context.stop_events, (unsigned long)worker_count,
             consumer_context.handled, consumer_context.error_events);
  }
  assert_true(consumer_context.start_events >= (int)worker_count);
  assert_true(consumer_context.stop_events >= (int)worker_count);
  if (first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_FAIL ||
      first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_FAIL_TWICE ||
      first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE_ERROR) {
    if (consumer_context.handled !=
        enqueue_count +
            (first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_FAIL_TWICE
                 ? 2
                 : 1)) {
      fail_msg("unexpected handled count after retrying failure path: "
               "backend=%s queue=%s mode=%s handled=%d enqueue_count=%d "
               "error_events=%d start_events=%d stop_events=%d "
               "last_error_code=%d last_error_http_status=%ld "
               "last_error_server_code=%s last_error_message=%s",
               backend->label, queue_name,
               e2e_consumer_delivery_mode_name(first_delivery_mode),
               consumer_context.handled, enqueue_count,
               consumer_context.error_events, consumer_context.start_events,
               consumer_context.stop_events, consumer_context.last_error_code,
               consumer_context.last_error_http_status,
               consumer_context.last_error_server_code,
               consumer_context.last_error_message);
    }
    assert_int_equal(
        consumer_context.handled,
        enqueue_count +
            (first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_FAIL_TWICE
                 ? 2
                 : 1));
    assert_true(consumer_context.error_events >= 1);
    assert_true(consumer_context.start_events >= (int)worker_count + 1);
  } else if (first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_ACK_ERROR) {
    if (consumer_context.handled != enqueue_count ||
        consumer_context.error_events < 1 ||
        consumer_context.start_events < (int)worker_count + 1) {
      fail_msg(
          "unexpected ack-error consumer state: backend=%s queue=%s "
          "mode=%s handled=%d enqueue_count=%d error_events=%d "
          "start_events=%d stop_events=%d last_error_code=%d "
          "last_error_http_status=%ld last_error_server_code=%s "
          "last_error_message=%s payload=%s "
          "queue_available=%d waiting_consumers=%ld pending_candidates=%ld "
          "total_consumers=%ld head_message_id=%s "
          "head_enqueued_at_unix=%ld head_not_visible_until_unix=%ld "
          "head_age_seconds=%ld stats_cid=%s",
          backend->label, queue_name,
          e2e_consumer_delivery_mode_name(first_delivery_mode),
          consumer_context.handled, enqueue_count,
          consumer_context.error_events, consumer_context.start_events,
          consumer_context.stop_events, consumer_context.last_error_code,
          consumer_context.last_error_http_status,
          consumer_context.last_error_server_code,
          consumer_context.last_error_message, consumer_context.payload,
          consumer_context.queue_stats_available,
          consumer_context.queue_stats_waiting_consumers,
          consumer_context.queue_stats_pending_candidates,
          consumer_context.queue_stats_total_consumers,
          consumer_context.queue_stats_head_message_id,
          consumer_context.queue_stats_head_enqueued_at_unix,
          consumer_context.queue_stats_head_not_visible_until_unix,
          consumer_context.queue_stats_head_age_seconds,
          consumer_context.queue_stats_correlation_id);
    }
    assert_int_equal(consumer_context.handled, enqueue_count);
    assert_true(consumer_context.error_events >= 1);
    assert_true(consumer_context.start_events >= (int)worker_count + 1);
  } else if (first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_ACK) {
    assert_int_equal(consumer_context.handled, enqueue_count);
    assert_int_equal(consumer_context.error_events, 0);
    assert_int_equal(consumer_context.start_events, (int)worker_count);
  } else if (first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_DEFER ||
             first_delivery_mode == E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE) {
    assert_int_equal(consumer_context.handled, enqueue_count + 1);
    if (consumer_context.error_events != 0) {
      fail_msg(
          "unexpected consumer error event: error_events=%d "
          "last_error_code=%d last_error_http_status=%ld "
          "last_error_server_code=%s last_error_message=%s "
          "start_events=%d stop_events=%d last_visibility_timeout_seconds=%ld",
          consumer_context.error_events, consumer_context.last_error_code,
          consumer_context.last_error_http_status,
          consumer_context.last_error_server_code,
          consumer_context.last_error_message, consumer_context.start_events,
          consumer_context.stop_events,
          consumer_context.last_visibility_timeout_seconds);
    }
    assert_int_equal(consumer_context.start_events, (int)worker_count);
  } else {
    assert_int_equal(consumer_context.handled, enqueue_count);
    assert_int_equal(consumer_context.error_events, 0);
    assert_int_equal(consumer_context.start_events, (int)worker_count);
  }
  if (read_payload) {
    if (strcmp(consumer_context.payload, "consumer-ok") != 0) {
      fail_msg("unexpected consumer payload: backend=%s queue=%s mode=%s "
               "payload=%s handled=%d error_events=%d",
               backend->label, queue_name,
               e2e_consumer_delivery_mode_name(first_delivery_mode),
               consumer_context.payload, consumer_context.handled,
               consumer_context.error_events);
    }
    assert_string_equal(consumer_context.payload, "consumer-ok");
  } else {
    assert_string_equal(consumer_context.payload, "");
  }
  if (expect_state) {
    if (consumer_context.saw_state != 1 ||
        strstr(consumer_context.state_json, "from-consumer-service") == NULL ||
        strstr(consumer_context.state_key, "/state/") == NULL ||
        strstr(consumer_context.state_key, queue_name) == NULL) {
      fail_msg("unexpected consumer state persistence: backend=%s queue=%s "
               "mode=%s saw_state=%d state_json=%s state_key=%s",
               backend->label, queue_name,
               e2e_consumer_delivery_mode_name(first_delivery_mode),
               consumer_context.saw_state, consumer_context.state_json,
               consumer_context.state_key);
    }
    assert_int_equal(consumer_context.saw_state, 1);
    assert_non_null(
        strstr(consumer_context.state_json, "from-consumer-service"));
    assert_non_null(strstr(consumer_context.state_key, "/state/"));
    assert_non_null(strstr(consumer_context.state_key, queue_name));
  } else {
    assert_int_equal(consumer_context.saw_state, 0);
  }
  pthread_mutex_unlock(&consumer_context.mutex);

  pthread_mutex_destroy(&consumer_context.mutex);
  lc_enqueue_res_cleanup(&enqueue_res);
  lc_consumer_service_close(service);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void run_consumer_service_variant(const e2e_consumer_backend *backend,
                                         const char *name_prefix,
                                         int enqueue_count,
                                         int first_delivery_mode,
                                         int expect_state,
                                         size_t worker_count) {
  run_consumer_service_variant_with_max_failures(
      backend, name_prefix, enqueue_count, first_delivery_mode, expect_state,
      worker_count, 5, 1);
}

static void run_consumer_service_variant_without_payload_read(
    const e2e_consumer_backend *backend, const char *name_prefix,
    int enqueue_count, int first_delivery_mode, int expect_state,
    size_t worker_count) {
  run_consumer_service_variant_with_max_failures(
      backend, name_prefix, enqueue_count, first_delivery_mode, expect_state,
      worker_count, 5, 0);
}

static void test_disk_consumer_service_happy(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_disk, "disk-consumer-happy", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 0, 1U);
}

static void test_disk_consumer_service_explicit_ack(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_disk, "disk-consumer-ack", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_ACK, 0, 1U);
}

static void test_disk_consumer_service_multi_delivery(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_disk, "disk-consumer-multi", 2,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 0, 1U);
}

static void test_disk_consumer_service_with_state(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_disk, "disk-consumer-state", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 1, 1U);
}

static void
test_disk_consumer_service_restart_after_handler_failure(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_disk, "disk-consumer-retry", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_FAIL, 0, 1U);
}

static void test_disk_consumer_service_delivery_failures_ignore_failure_budget(
    void **state) {
  (void)state;
  run_consumer_service_variant_with_max_failures(
      &e2e_backend_disk, "disk-consumer-failure-budget", 1,
      E2E_CONSUMER_FIRST_DELIVERY_FAIL_TWICE, 0, 1U, 1, 1);
}

static void test_disk_consumer_service_defer_then_redeliver(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_disk, "disk-consumer-defer", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_DEFER, 0, 1U);
}

static void test_disk_consumer_service_explicit_failure_nack(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_disk, "disk-consumer-fail-nack", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE, 0, 1U);
}

static void test_disk_consumer_service_explicit_failure_nack_then_handler_error(
    void **state) {
  (void)state;
  run_consumer_service_variant(
      &e2e_backend_disk, "disk-consumer-fail-nack-error", 1,
      E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE_ERROR, 0, 1U);
}

static void
test_disk_consumer_service_acks_without_reading_payload(void **state) {
  (void)state;
  run_consumer_service_variant_without_payload_read(
      &e2e_backend_disk, "disk-consumer-unread-ack", 1,
      E2E_CONSUMER_FIRST_DELIVERY_NONE, 0, 1U);
}

static void
test_disk_consumer_service_defers_without_reading_payload(void **state) {
  (void)state;
  run_consumer_service_variant_without_payload_read(
      &e2e_backend_disk, "disk-consumer-unread-defer", 1,
      E2E_CONSUMER_FIRST_DELIVERY_DEFER, 0, 1U);
}

static void
test_disk_consumer_service_failure_nacks_without_reading_payload(void **state) {
  (void)state;
  run_consumer_service_variant_without_payload_read(
      &e2e_backend_disk, "disk-consumer-unread-fail-nack", 1,
      E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE, 0, 1U);
}

static void
test_disk_consumer_service_explicit_ack_then_handler_error(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_disk, "disk-consumer-ack-error", 2,
                               E2E_CONSUMER_FIRST_DELIVERY_ACK_ERROR, 0, 1U);
}

static void test_s3_consumer_service_happy(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_s3, "s3-consumer-happy", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 0, 1U);
}

static void test_s3_consumer_service_explicit_ack(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_s3, "s3-consumer-ack", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_ACK, 0, 1U);
}

static void test_s3_consumer_service_multi_delivery(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_s3, "s3-consumer-multi", 2,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 0, 1U);
}

static void test_s3_consumer_service_with_state(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_s3, "s3-consumer-state", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 1, 1U);
}

static void
test_s3_consumer_service_restart_after_handler_failure(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_s3, "s3-consumer-retry", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_FAIL, 0, 1U);
}

static void
test_s3_consumer_service_delivery_failures_ignore_failure_budget(void **state) {
  (void)state;
  run_consumer_service_variant_with_max_failures(
      &e2e_backend_s3, "s3-consumer-failure-budget", 1,
      E2E_CONSUMER_FIRST_DELIVERY_FAIL_TWICE, 0, 1U, 1, 1);
}

static void test_s3_consumer_service_defer_then_redeliver(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_s3, "s3-consumer-defer", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_DEFER, 0, 1U);
}

static void test_s3_consumer_service_explicit_failure_nack(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_s3, "s3-consumer-fail-nack", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE, 0, 1U);
}

static void test_s3_consumer_service_explicit_failure_nack_then_handler_error(
    void **state) {
  (void)state;
  run_consumer_service_variant(
      &e2e_backend_s3, "s3-consumer-fail-nack-error", 1,
      E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE_ERROR, 0, 1U);
}

static void
test_s3_consumer_service_acks_without_reading_payload(void **state) {
  (void)state;
  run_consumer_service_variant_without_payload_read(
      &e2e_backend_s3, "s3-consumer-unread-ack", 1,
      E2E_CONSUMER_FIRST_DELIVERY_NONE, 0, 1U);
}

static void
test_s3_consumer_service_defers_without_reading_payload(void **state) {
  (void)state;
  run_consumer_service_variant_without_payload_read(
      &e2e_backend_s3, "s3-consumer-unread-defer", 1,
      E2E_CONSUMER_FIRST_DELIVERY_DEFER, 0, 1U);
}

static void
test_s3_consumer_service_failure_nacks_without_reading_payload(void **state) {
  (void)state;
  run_consumer_service_variant_without_payload_read(
      &e2e_backend_s3, "s3-consumer-unread-fail-nack", 1,
      E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE, 0, 1U);
}

static void
test_s3_consumer_service_explicit_ack_then_handler_error(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_s3, "s3-consumer-ack-error", 2,
                               E2E_CONSUMER_FIRST_DELIVERY_ACK_ERROR, 0, 1U);
}

static void test_mem_uds_consumer_service_happy(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_mem, "mem-consumer-happy", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 0, 1U);
}

static void test_mem_uds_consumer_service_explicit_ack(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_mem, "mem-consumer-ack", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_ACK, 0, 1U);
}

static void test_mem_uds_consumer_service_multi_delivery(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_mem, "mem-consumer-multi", 2,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 0, 1U);
}

static void test_mem_uds_consumer_service_with_state(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_mem, "mem-consumer-state", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_NONE, 1, 1U);
}

static void
test_mem_uds_consumer_service_restart_after_handler_failure(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_mem, "mem-consumer-retry", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_FAIL, 0, 1U);
}

static void
test_mem_uds_consumer_service_delivery_failures_ignore_failure_budget(
    void **state) {
  (void)state;
  run_consumer_service_variant_with_max_failures(
      &e2e_backend_mem, "mem-consumer-failure-budget", 1,
      E2E_CONSUMER_FIRST_DELIVERY_FAIL_TWICE, 0, 1U, 1, 1);
}

static void test_mem_uds_consumer_service_defer_then_redeliver(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_mem, "mem-consumer-defer", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_DEFER, 0, 1U);
}

static void test_mem_uds_consumer_service_explicit_failure_nack(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_mem, "mem-consumer-fail-nack", 1,
                               E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE, 0, 1U);
}

static void
test_mem_uds_consumer_service_explicit_failure_nack_then_handler_error(
    void **state) {
  (void)state;
  run_consumer_service_variant(
      &e2e_backend_mem, "mem-consumer-fail-nack-error", 1,
      E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE_ERROR, 0, 1U);
}

static void
test_mem_uds_consumer_service_acks_without_reading_payload(void **state) {
  (void)state;
  run_consumer_service_variant_without_payload_read(
      &e2e_backend_mem, "mem-consumer-unread-ack", 1,
      E2E_CONSUMER_FIRST_DELIVERY_NONE, 0, 1U);
}

static void
test_mem_uds_consumer_service_defers_without_reading_payload(void **state) {
  (void)state;
  run_consumer_service_variant_without_payload_read(
      &e2e_backend_mem, "mem-consumer-unread-defer", 1,
      E2E_CONSUMER_FIRST_DELIVERY_DEFER, 0, 1U);
}

static void test_mem_uds_consumer_service_failure_nacks_without_reading_payload(
    void **state) {
  (void)state;
  run_consumer_service_variant_without_payload_read(
      &e2e_backend_mem, "mem-consumer-unread-fail-nack", 1,
      E2E_CONSUMER_FIRST_DELIVERY_NACK_FAILURE, 0, 1U);
}

static void
test_mem_uds_consumer_service_explicit_ack_then_handler_error(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_mem, "mem-consumer-ack-error", 2,
                               E2E_CONSUMER_FIRST_DELIVERY_ACK_ERROR, 0, 1U);
}

static void test_mem_uds_consumer_service_multi_worker(void **state) {
  (void)state;
  run_consumer_service_variant(&e2e_backend_mem, "mem-consumer-workers", 2, 0,
                               0, 2U);
}

static void test_mem_uds_dequeue_with_state_roundtrip(void **state) {
  const char *socket_path;
  lc_client *client;
  lc_source *src;
  lc_message *message;
  lc_lease *state_lease;
  lc_error error;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_get_res get_res;
  char queue_name[96];
  char expected_state_key[160];
  static const unsigned char payload[] = {'s', 't', 'a', 't', 'e',
                                          '-', 'u', 'd', 's'};
  int rc;

  (void)state;
  socket_path = env_or_default("LOCKDC_E2E_MEM_SOCKET",
                               "./devenv/volumes/lockd-mem-run/lockd.sock");
  require_socket_or_skip(socket_path);

  client = NULL;
  src = NULL;
  message = NULL;
  state_lease = NULL;
  lc_error_init(&error);
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_dequeue_req_init(&dequeue_req);
  memset(&get_res, 0, sizeof(get_res));

  open_uds_client(socket_path, &client, &error);
  make_unique_name("mem-state-queue", queue_name, sizeof(queue_name));
  rc = lc_source_from_memory(payload, sizeof(payload), &src, &error);
  assert_lc_ok(rc, &error);

  enqueue_req.queue = queue_name;
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 300L;
  enqueue_req.max_attempts = 5;
  rc = client->enqueue(client, &enqueue_req, src, &enqueue_res, &error);
  assert_lc_ok(rc, &error);

  dequeue_req.queue = queue_name;
  dequeue_req.owner = "lc-e2e-state-worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  dequeue_req.wait_seconds = 2L;
  rc = client->dequeue_with_state(client, &dequeue_req, &message, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(message);

  state_lease = message->state(message);
  assert_non_null(state_lease);
  snprintf(expected_state_key, sizeof(expected_state_key), "q/%s/state/%s",
           queue_name, message->message_id);
  assert_string_equal(state_lease->key, expected_state_key);

  {
    e2e_status_doc doc;

    doc.status = "from-direct-state";
    rc = state_lease->save(state_lease, &e2e_status_map, &doc, &error);
  }
  assert_lc_ok(rc, &error);
  {
    e2e_status_doc doc;

    memset(&doc, 0, sizeof(doc));
    rc = state_lease->load(state_lease, &e2e_status_map, &doc, NULL, &get_res,
                           &error);
    assert_lc_ok(rc, &error);
    assert_non_null(doc.status);
    assert_string_equal(doc.status, "from-direct-state");
    e2e_lonejson_cleanup(&e2e_status_map, &doc);
  }
  assert_lc_ok(rc, &error);

  rc = message->ack(message, &error);
  assert_lc_ok(rc, &error);
  message = NULL;

  lc_get_res_cleanup(&get_res);
  lc_enqueue_res_cleanup(&enqueue_res);
  lc_source_close(src);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_s3_dequeue_with_state_roundtrip(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_source *src;
  lc_message *message;
  lc_lease *state_lease;
  lc_error error;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  lc_get_res get_res;
  char queue_name[96];
  char expected_state_key[160];
  static const unsigned char payload[] = {'s', 't', 'a', 't',
                                          'e', '-', 's', '3'};
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_S3_ENDPOINT", "https://localhost:19443");
  bundle_path = env_or_default("LOCKDC_E2E_S3_BUNDLE",
                               "./devenv/volumes/lockd-s3-config/client.pem");
  require_file_or_skip(bundle_path);

  client = NULL;
  src = NULL;
  message = NULL;
  state_lease = NULL;
  lc_error_init(&error);
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_dequeue_req_init(&dequeue_req);
  memset(&get_res, 0, sizeof(get_res));

  open_tcp_client(endpoint, bundle_path, &client, &error);
  make_unique_name("s3-state-queue", queue_name, sizeof(queue_name));
  rc = lc_source_from_memory(payload, sizeof(payload), &src, &error);
  assert_lc_ok(rc, &error);

  enqueue_req.queue = queue_name;
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 300L;
  enqueue_req.max_attempts = 5;
  rc = client->enqueue(client, &enqueue_req, src, &enqueue_res, &error);
  assert_lc_ok(rc, &error);

  dequeue_req.queue = queue_name;
  dequeue_req.owner = "lc-e2e-state-worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  dequeue_req.wait_seconds = 2L;
  rc = client->dequeue_with_state(client, &dequeue_req, &message, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(message);

  state_lease = message->state(message);
  assert_non_null(state_lease);
  snprintf(expected_state_key, sizeof(expected_state_key), "q/%s/state/%s",
           queue_name, message->message_id);
  assert_string_equal(state_lease->key, expected_state_key);

  {
    e2e_status_doc doc;

    doc.status = "from-s3-direct-state";
    rc = state_lease->save(state_lease, &e2e_status_map, &doc, &error);
  }
  assert_lc_ok(rc, &error);
  {
    e2e_status_doc doc;

    memset(&doc, 0, sizeof(doc));
    rc = state_lease->load(state_lease, &e2e_status_map, &doc, NULL, &get_res,
                           &error);
    assert_lc_ok(rc, &error);
    assert_non_null(doc.status);
    assert_string_equal(doc.status, "from-s3-direct-state");
    e2e_lonejson_cleanup(&e2e_status_map, &doc);
  }
  assert_lc_ok(rc, &error);

  rc = message->ack(message, &error);
  assert_lc_ok(rc, &error);
  message = NULL;

  lc_get_res_cleanup(&get_res);
  lc_enqueue_res_cleanup(&enqueue_res);
  lc_source_close(src);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_pouch_direct_state_attachment_reopen_roundtrip(void **state) {
  lc_client *client;
  lc_client *reader;
  lc_lease *lease;
  lc_source *src;
  lc_sink *sink;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_release_req release_req;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lc_attach_req attach_req;
  lc_attach_res attach_res;
  lc_attachment_list_req list_req;
  lc_attachment_list attachment_list;
  lc_attachment_get_op get_attachment_req;
  lc_attachment_get_res get_attachment_res;
  const void *bytes;
  size_t length;
  char root[256];
  char endpoint[320];
  char key[96];
  static const unsigned char attachment_payload[] = {'p', 'o', 'u', 'c', 'h'};
  int rc;

  (void)state;
  make_pouch_root("state-attachment", root, sizeof(root), endpoint,
                  sizeof(endpoint));
  assert_true(snprintf(endpoint, sizeof(endpoint),
                       "pouch://%s?single_writer=false", root) > 0);

  client = NULL;
  reader = NULL;
  lease = NULL;
  src = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_release_req_init(&release_req);
  lc_get_opts_init(&get_opts);
  memset(&get_res, 0, sizeof(get_res));
  lc_attach_req_init(&attach_req);
  memset(&attach_res, 0, sizeof(attach_res));
  lc_attachment_list_req_init(&list_req);
  memset(&attachment_list, 0, sizeof(attachment_list));
  lc_attachment_get_op_init(&get_attachment_req);
  memset(&get_attachment_res, 0, sizeof(get_attachment_res));

  open_pouch_client(endpoint, &client, &error);
  make_unique_name("pouch-direct-state", key, sizeof(key));
  acquire_req.key = key;
  acquire_req.owner = "lc-e2e-pouch";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(lease);

  save_json_text_or_die(
      lease, "{\"kind\":\"pouch-e2e\",\"value\":1,\"tags\":[\"pouch\"]}",
      &error);
  assert_true(lease->version >= 1L);
  assert_non_null(lease->state_etag);

  rc = lc_source_from_memory(attachment_payload, sizeof(attachment_payload),
                             &src, &error);
  assert_lc_ok(rc, &error);
  attach_req.name = "blob.bin";
  attach_req.content_type = "application/octet-stream";
  rc = lease->attach(lease, &attach_req, src, &attach_res, &error);
  lc_source_close(src);
  src = NULL;
  assert_lc_ok(rc, &error);
  assert_string_equal(attach_res.attachment.name, "blob.bin");

  reader = NULL;
  open_pouch_client(endpoint, &reader, &error);
  list_req.lease.namespace_name = lease->namespace_name;
  list_req.lease.key = lease->key;
  list_req.lease.lease_id = lease->lease_id;
  list_req.lease.txn_id = lease->txn_id;
  list_req.lease.fencing_token = lease->fencing_token;
  rc = reader->list_attachments(reader, &list_req, &attachment_list, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(attachment_list.count, 1U);
  assert_string_equal(attachment_list.items[0].name, "blob.bin");

  get_attachment_req.lease = list_req.lease;
  get_attachment_req.selector.name = "blob.bin";
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  rc = reader->get_attachment(reader, &get_attachment_req, sink,
                              &get_attachment_res, &error);
  assert_lc_ok(rc, &error);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(length, sizeof(attachment_payload));
  assert_memory_equal(bytes, attachment_payload, sizeof(attachment_payload));
  lc_sink_close(sink);
  sink = NULL;

  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;
  lc_client_close(client);
  client = NULL;
  lc_client_close(reader);
  reader = NULL;

  open_pouch_client(endpoint, &reader, &error);
  rc = lc_sink_to_memory(&sink, &error);
  assert_lc_ok(rc, &error);
  get_opts.public_read = 1;
  rc = reader->get(reader, key, &get_opts, sink, &get_res, &error);
  assert_lc_ok(rc, &error);
  assert_false(get_res.no_content);
  assert_true(get_res.version >= 1L);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_lc_ok(rc, &error);
  assert_true(buffer_contains(bytes, length, "\"kind\":\"pouch-e2e\""));
  assert_true(buffer_contains(bytes, length, "\"tags\":[\"pouch\"]"));

  lc_sink_close(sink);
  lc_get_res_cleanup(&get_res);
  lc_attachment_get_res_cleanup(&get_attachment_res);
  lc_attachment_list_cleanup(&attachment_list);
  lc_attach_res_cleanup(&attach_res);
  lc_client_close(reader);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void pouch_e2e_run_maintenance(const char *root,
                                      const char *namespace_name, int force,
                                      int cleanup_only, long retention_cutoff,
                                      lc_error *error) {
  lc_pouch *pouch;
  lc_pouch_open_options open_options;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  int rc;

  pouch = NULL;
  memset(&open_options, 0, sizeof(open_options));
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, error);
  assert_lc_ok(rc, error);
  maintenance_options.namespace_name = namespace_name;
  maintenance_options.force = force;
  maintenance_options.cleanup_only = cleanup_only;
  maintenance_options.retention_updated_before_unix = retention_cutoff;
  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, error);
  assert_lc_ok(rc, error);
  if (force) {
    assert_true(maintenance_result.compacted || maintenance_result.skipped);
  }
  if (retention_cutoff > 0L) {
    assert_string_equal(maintenance_result.diagnostic, "retention-complete");
  }
  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  lc_pouch_close(pouch);
}

static size_t pouch_e2e_query_key_count(lc_client *client,
                                        const char *namespace_name,
                                        const char *selector_json,
                                        lc_error *error) {
  lc_query_key_handler handler;
  lc_query_req query_req;
  lc_query_res query_res;
  pouch_e2e_key_count count;
  int rc;

  memset(&handler, 0, sizeof(handler));
  lc_query_req_init(&query_req);
  memset(&query_res, 0, sizeof(query_res));
  memset(&count, 0, sizeof(count));
  handler.begin = pouch_e2e_key_begin;
  handler.chunk = pouch_e2e_key_chunk;
  handler.end = pouch_e2e_key_end;
  query_req.namespace_name = namespace_name;
  query_req.selector_json = selector_json;
  query_req.engine = "index";
  query_req.limit = 512L;
  rc = lc_query_keys(client, &query_req, &handler, &count, &query_res, error);
  assert_lc_ok(rc, error);
  lc_query_res_cleanup(&query_res);
  return count.rows;
}

static void pouch_e2e_write_segmented_docs_direct(const char *root,
                                                  const char *namespace_name,
                                                  const char *kind,
                                                  size_t doc_count,
                                                  lc_error *error) {
  lc_pouch *pouch;
  lc_pouch_open_options open_options;
  lc_pouch_state_write_options write_options;
  lc_pouch_state_write_result write_result;
  lc_source *source;
  char body[1024];
  char key[192];
  char pad[640];
  size_t index;
  int written;
  int rc;

  pouch = NULL;
  source = NULL;
  memset(&open_options, 0, sizeof(open_options));
  memset(&write_options, 0, sizeof(write_options));
  memset(&write_result, 0, sizeof(write_result));
  memset(pad, 'x', sizeof(pad) - 1U);
  pad[sizeof(pad) - 1U] = '\0';
  open_options.segment_target_bytes = 4096UL;
  open_options.compaction_min_segment_count = 2UL;
  open_options.compaction_min_reclaimable_bytes = 1UL;
  open_options.background_compaction_enabled = 0;
  open_options.background_compaction_enabled_set = 1;
  open_options.query_engine = "index";
  write_options.content_type = "application/json";

  rc = lc_pouch_open(root, NULL, &open_options, &pouch, error);
  assert_lc_ok(rc, error);
  for (index = 0U; index < doc_count; ++index) {
    written = snprintf(key, sizeof(key), "pouch/large/%s/%04zu", kind, index);
    assert_true(written > 0 && (size_t)written < sizeof(key));
    written = snprintf(body, sizeof(body),
                       "{\"kind\":\"%s\",\"ordinal\":%zu,\"group\":\"%s\","
                       "\"tags\":[\"pouch\",\"large\",\"%s\"],\"pad\":\"%s\"}",
                       kind, index, index % 2U == 0U ? "even" : "odd",
                       index % 3U == 0U ? "planning" : "finance", pad);
    assert_true(written > 0 && (size_t)written < sizeof(body));
    rc = lc_source_from_memory(body, strlen(body), &source, error);
    assert_lc_ok(rc, error);
    rc = lc_pouch_state_write(pouch, namespace_name, key, source,
                              &write_options, &write_result, error);
    lc_source_close(source);
    source = NULL;
    assert_lc_ok(rc, error);
    lc_pouch_state_write_result_cleanup(NULL, &write_result);
  }
  lc_pouch_close(pouch);
}

static void pouch_e2e_force_maintenance_expect_segments(
    const char *root, const char *namespace_name, lc_error *error) {
  lc_pouch *pouch;
  lc_pouch_open_options open_options;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  int rc;

  pouch = NULL;
  memset(&open_options, 0, sizeof(open_options));
  memset(&maintenance_options, 0, sizeof(maintenance_options));
  memset(&maintenance_result, 0, sizeof(maintenance_result));
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, error);
  assert_lc_ok(rc, error);
  maintenance_options.namespace_name = namespace_name;
  maintenance_options.force = 1;
  rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                &maintenance_result, error);
  assert_lc_ok(rc, error);
  assert_true(maintenance_result.compacted);
  assert_true(maintenance_result.candidate_segment_count > 1UL);
  assert_true(maintenance_result.candidate_bytes > 4096UL);
  lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  lc_pouch_close(pouch);
}

static void
test_pouch_direct_lifecycle_maintenance_reopen_roundtrip(void **state) {
  lc_client *client;
  lc_client *reader;
  lc_lease *lease;
  lc_message *message;
  lc_source *src;
  lc_error error;
  lc_acquire_req acquire_req;
  lc_release_req release_req;
  lc_attach_req attach_req;
  lc_attach_res attach_res;
  lc_attachment_list attachment_list;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_dequeue_req dequeue_req;
  char root[256];
  char endpoint[320];
  char kind[96];
  char keep_key[128];
  char expired_kind[96];
  char expired_key[128];
  char selector_json[192];
  char expired_selector_json[192];
  char queue_name[160];
  size_t rows;
  int rc;

  (void)state;
  make_pouch_root("lifecycle-maintenance", root, sizeof(root), endpoint,
                  sizeof(endpoint));

  client = NULL;
  reader = NULL;
  lease = NULL;
  message = NULL;
  src = NULL;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_release_req_init(&release_req);
  lc_attach_req_init(&attach_req);
  memset(&attach_res, 0, sizeof(attach_res));
  memset(&attachment_list, 0, sizeof(attachment_list));
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_dequeue_req_init(&dequeue_req);

  make_unique_name("pouch-life", kind, sizeof(kind));
  make_unique_name("pouch-expired", expired_kind, sizeof(expired_kind));
  snprintf(keep_key, sizeof(keep_key), "pouch/lifecycle/%s/keep", kind);
  snprintf(expired_key, sizeof(expired_key), "pouch/lifecycle/%s/dead",
           expired_kind);
  snprintf(selector_json, sizeof(selector_json),
           "{\"eq\":{\"field\":\"/kind\",\"value\":\"%s\"}}", kind);
  snprintf(expired_selector_json, sizeof(expired_selector_json),
           "{\"eq\":{\"field\":\"/kind\",\"value\":\"%s\"}}", expired_kind);
  snprintf(queue_name, sizeof(queue_name), "pouch-life-%s", kind);

  open_pouch_client(endpoint, &client, &error);
  acquire_req.namespace_name = "life";
  acquire_req.key = keep_key;
  acquire_req.owner = "lc-e2e-pouch-life";
  acquire_req.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  {
    char body[256];

    snprintf(body, sizeof(body),
             "{\"kind\":\"%s\",\"status\":\"keep\",\"value\":1}", kind);
    save_json_text_or_die(lease, body, &error);
  }
  attach_req.name = "life.txt";
  attach_req.content_type = "text/plain";
  rc =
      lc_source_from_memory("life-object", strlen("life-object"), &src, &error);
  assert_lc_ok(rc, &error);
  rc = lease->attach(lease, &attach_req, src, &attach_res, &error);
  lc_source_close(src);
  src = NULL;
  assert_lc_ok(rc, &error);
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  acquire_req.namespace_name = "life-retention";
  acquire_req.key = expired_key;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  {
    char body[256];

    snprintf(body, sizeof(body),
             "{\"kind\":\"%s\",\"status\":\"expired\",\"value\":2}",
             expired_kind);
    save_json_text_or_die(lease, body, &error);
  }
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  enqueue_req.namespace_name = "life";
  enqueue_req.queue = queue_name;
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 300L;
  enqueue_req.max_attempts = 3;
  rc = lc_source_from_memory("life-queue", strlen("life-queue"), &src, &error);
  assert_lc_ok(rc, &error);
  rc = client->enqueue(client, &enqueue_req, src, &enqueue_res, &error);
  lc_source_close(src);
  src = NULL;
  assert_lc_ok(rc, &error);

  rows = pouch_e2e_query_key_count(client, "life", selector_json, &error);
  assert_int_equal(rows, 1U);
  rows = pouch_e2e_query_key_count(client, "life-retention",
                                   expired_selector_json, &error);
  assert_int_equal(rows, 1U);
  lc_client_close(client);
  client = NULL;

  pouch_e2e_run_maintenance(root, "life", 1, 0, 0L, &error);
  pouch_e2e_run_maintenance(root, "life", 0, 1, 0L, &error);
  pouch_e2e_run_maintenance(root, "life-retention", 0, 0, 2147483647L, &error);

  open_pouch_client(endpoint, &reader, &error);
  rows = pouch_e2e_query_key_count(reader, "life", selector_json, &error);
  assert_int_equal(rows, 1U);
  rows = pouch_e2e_query_key_count(reader, "life-retention",
                                   expired_selector_json, &error);
  assert_int_equal(rows, 0U);

  acquire_req.namespace_name = "life";
  acquire_req.key = keep_key;
  acquire_req.owner = "lc-e2e-pouch-life-reader";
  rc = reader->acquire(reader, &acquire_req, &lease, &error);
  assert_lc_ok(rc, &error);
  rc = lease->list_attachments(lease, &attachment_list, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(attachment_list.count, 1U);
  assert_string_equal(attachment_list.items[0].name, "life.txt");
  rc = lease->release(lease, &release_req, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;

  dequeue_req.namespace_name = "life";
  dequeue_req.queue = queue_name;
  dequeue_req.owner = "lc-e2e-pouch-life-worker";
  dequeue_req.visibility_timeout_seconds = 30L;
  rc = reader->dequeue(reader, &dequeue_req, &message, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(message);
  rc = message->ack(message, &error);
  assert_lc_ok(rc, &error);
  message = NULL;

  lc_attachment_list_cleanup(&attachment_list);
  lc_enqueue_res_cleanup(&enqueue_res);
  lc_attach_res_cleanup(&attach_res);
  lc_client_close(reader);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void
test_pouch_direct_large_namespace_segmented_index_reopen(void **state) {
  lc_client *client;
  lc_index_flush_req flush_req;
  lc_index_flush_res flush_res;
  lc_error error;
  char root[256];
  char endpoint[320];
  char kind[96];
  char selector_json[192];
  size_t rows;
  const size_t doc_count = 72U;
  int rc;

  (void)state;
  make_pouch_root("large-namespace", root, sizeof(root), endpoint,
                  sizeof(endpoint));

  client = NULL;
  memset(&flush_res, 0, sizeof(flush_res));
  lc_index_flush_req_init(&flush_req);
  lc_error_init(&error);

  make_unique_name("pouch-large", kind, sizeof(kind));
  snprintf(selector_json, sizeof(selector_json),
           "{\"eq\":{\"field\":\"/kind\",\"value\":\"%s\"}}", kind);

  pouch_e2e_write_segmented_docs_direct(root, "large", kind, doc_count, &error);

  open_pouch_client(endpoint, &client, &error);
  flush_req.namespace_name = "large";
  flush_req.mode = "wait";
  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_lc_ok(rc, &error);
  assert_true(flush_res.flushed);
  rows = pouch_e2e_query_key_count(client, "large", selector_json, &error);
  assert_int_equal(rows, doc_count);
  lc_index_flush_res_cleanup(&flush_res);
  lc_client_close(client);
  client = NULL;

  pouch_e2e_force_maintenance_expect_segments(root, "large", &error);
  pouch_e2e_run_maintenance(root, "large", 0, 1, 0L, &error);

  open_pouch_client(endpoint, &client, &error);
  memset(&flush_res, 0, sizeof(flush_res));
  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_lc_ok(rc, &error);
  assert_true(flush_res.flushed);
  rows = pouch_e2e_query_key_count(client, "large", selector_json, &error);
  assert_int_equal(rows, doc_count);

  lc_index_flush_res_cleanup(&flush_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void
test_pouch_direct_marker_damage_and_index_rebuild_after_snapshot(void **state) {
  lc_client *client;
  lc_index_flush_req flush_req;
  lc_index_flush_res flush_res;
  lc_error error;
  char root[256];
  char endpoint[320];
  char kind[96];
  char selector_json[192];
  char namespace_path[512];
  char manifest_path[640];
  char marker_path[640];
  size_t rows;
  const size_t doc_count = 36U;
  int written;
  int rc;

  (void)state;
  make_pouch_root("marker-index-repair", root, sizeof(root), endpoint,
                  sizeof(endpoint));

  client = NULL;
  lc_index_flush_req_init(&flush_req);
  memset(&flush_res, 0, sizeof(flush_res));
  lc_error_init(&error);

  make_unique_name("pouch-repair", kind, sizeof(kind));
  snprintf(selector_json, sizeof(selector_json),
           "{\"eq\":{\"field\":\"/kind\",\"value\":\"%s\"}}", kind);
  written = snprintf(namespace_path, sizeof(namespace_path),
                     "%s/namespaces/repair", root);
  assert_true(written > 0 && (size_t)written < sizeof(namespace_path));
  written = snprintf(manifest_path, sizeof(manifest_path),
                     "%s/index/query.manifest", namespace_path);
  assert_true(written > 0 && (size_t)written < sizeof(manifest_path));
  written = snprintf(marker_path, sizeof(marker_path),
                     "%s/markers/writer-damaged.marker", namespace_path);
  assert_true(written > 0 && (size_t)written < sizeof(marker_path));

  pouch_e2e_write_segmented_docs_direct(root, "repair", kind, doc_count,
                                        &error);

  open_pouch_client(endpoint, &client, &error);
  flush_req.namespace_name = "repair";
  flush_req.mode = "wait";
  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_lc_ok(rc, &error);
  assert_true(flush_res.flushed);
  rows = pouch_e2e_query_key_count(client, "repair", selector_json, &error);
  assert_int_equal(rows, doc_count);
  lc_index_flush_res_cleanup(&flush_res);
  lc_client_close(client);
  client = NULL;

  pouch_e2e_force_maintenance_expect_segments(root, "repair", &error);
  pouch_e2e_run_maintenance(root, "repair", 0, 1, 0L, &error);

  /* Recreate the post-snapshot derived index, then remove its manifest. */
  open_pouch_client(endpoint, &client, &error);
  memset(&flush_res, 0, sizeof(flush_res));
  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_lc_ok(rc, &error);
  assert_true(flush_res.flushed);
  lc_index_flush_res_cleanup(&flush_res);
  lc_client_close(client);
  client = NULL;

  assert_int_equal(unlink(manifest_path), 0);
  pouch_e2e_write_text_file(marker_path, "damaged-marker\n");

  open_pouch_client(endpoint, &client, &error);
  memset(&flush_res, 0, sizeof(flush_res));
  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_lc_ok(rc, &error);
  assert_true(flush_res.flushed);
  rows = pouch_e2e_query_key_count(client, "repair", selector_json, &error);
  assert_int_equal(rows, doc_count);

  lc_index_flush_res_cleanup(&flush_res);
  lc_client_close(client);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

static void test_pouch_direct_consumer_service_with_state(void **state) {
  lc_client *client;
  lc_consumer_service *service;
  lc_error error;
  lc_enqueue_req enqueue_req;
  lc_enqueue_res enqueue_res;
  lc_consumer_config consumer;
  lc_consumer_service_config service_config;
  e2e_consumer_context consumer_context;
  lc_source *src;
  char root[256];
  char endpoint[320];
  char queue_name[96];
  static const unsigned char payload[] = {'p', 'o', 'u', 'c', 'h',
                                          '-', 'w', 'o', 'r', 'k'};
  int rc;

  (void)state;
  make_pouch_root("consumer", root, sizeof(root), endpoint, sizeof(endpoint));

  client = NULL;
  service = NULL;
  src = NULL;
  lc_error_init(&error);
  lc_enqueue_req_init(&enqueue_req);
  memset(&enqueue_res, 0, sizeof(enqueue_res));
  lc_consumer_config_init(&consumer);
  lc_consumer_service_config_init(&service_config);
  memset(&consumer_context, 0, sizeof(consumer_context));
  assert_int_equal(pthread_mutex_init(&consumer_context.mutex, NULL), 0);
  consumer_context.backend_label = "pouch";
  consumer_context.first_delivery_mode = E2E_CONSUMER_FIRST_DELIVERY_NONE;
  consumer_context.expect_state = 1;
  consumer_context.read_payload = 1;
  consumer_context.enqueue_count = 1;
  consumer_context.worker_count = 1U;
  consumer_context.max_failures = 5;

  open_pouch_client(endpoint, &client, &error);
  make_unique_name("pouch-consumer", queue_name, sizeof(queue_name));
  consumer_context.queue_name = queue_name;

  consumer.name = "pouch";
  consumer.request.queue = queue_name;
  consumer.request.owner = "lc-e2e-pouch-consumer";
  consumer.request.visibility_timeout_seconds = 30L;
  consumer.request.wait_seconds = 1L;
  consumer.with_state = 1;
  consumer.handle = e2e_consumer_handle;
  consumer.on_error = e2e_consumer_on_error;
  consumer.on_start = e2e_consumer_on_start;
  consumer.on_stop = e2e_consumer_on_stop;
  consumer.context = &consumer_context;
  lc_consumer_restart_policy_init(&consumer.restart_policy);
  consumer.restart_policy.base_delay_ms = 100L;
  consumer.restart_policy.max_delay_ms = 250L;
  consumer.restart_policy.max_failures = 5;

  service_config.consumers = &consumer;
  service_config.consumer_count = 1U;
  rc = client->new_consumer_service(client, &service_config, &service, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(service);

  enqueue_req.queue = queue_name;
  enqueue_req.content_type = "text/plain";
  enqueue_req.visibility_timeout_seconds = 30L;
  enqueue_req.ttl_seconds = 300L;
  enqueue_req.max_attempts = 5;
  rc = lc_source_from_memory(payload, sizeof(payload), &src, &error);
  assert_lc_ok(rc, &error);
  rc = client->enqueue(client, &enqueue_req, src, &enqueue_res, &error);
  lc_source_close(src);
  src = NULL;
  assert_lc_ok(rc, &error);

  /* The service retains Pouch's exclusive local session after its caller
   * closes the source client. */
  lc_client_close(client);
  client = NULL;

  rc = service->start(service, &error);
  assert_lc_ok(rc, &error);

  consumer_context.expected_minimum_count = 1;
  wait_for_consumer_handled(&consumer_context, 1);

  rc = (service->stop)(service);
  assert_int_equal(rc, LC_OK);
  rc = service->wait(service, &error);
  assert_lc_ok(rc, &error);

  pthread_mutex_lock(&consumer_context.mutex);
  assert_int_equal(consumer_context.handled, 1);
  assert_int_equal(consumer_context.error_events, 0);
  assert_true(consumer_context.start_events >= 1);
  assert_true(consumer_context.stop_events >= 1);
  assert_string_equal(consumer_context.payload, "pouch-work");
  assert_int_equal(consumer_context.saw_state, 1);
  assert_non_null(strstr(consumer_context.state_json, "from-consumer-service"));
  assert_non_null(strstr(consumer_context.state_key, "/state/"));
  assert_non_null(strstr(consumer_context.state_key, queue_name));
  pthread_mutex_unlock(&consumer_context.mutex);

  pthread_mutex_destroy(&consumer_context.mutex);
  lc_enqueue_res_cleanup(&enqueue_res);
  lc_consumer_service_close(service);
  lc_error_cleanup(&error);
  cleanup_pouch_root(root);
}

#if defined(LC_E2E_GROUP_DISK_DIRECT)
static void test_disk_workflow_implicit_xa_roundtrip(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_workflow *workflow;
  lc_workflow_config config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_outbox_receipt duplicate_receipt;
  lc_workflow_transaction *transaction;
  lc_workflow_transaction *duplicate_transaction;
  lc_workflow_participant_request participant_request;
  lc_workflow_participant *participant;
  lc_outbox_job *job;
  lc_source *payload;
  lc_source *duplicate_payload;
  lc_source *domain_state;
  lc_sink *payload_sink;
  lc_get_opts get_options;
  lc_get_res get_result;
  const void *payload_bytes;
  size_t payload_length;
  size_t payload_written;
  lc_error error;
  char domain_key[128];
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./devenv/volumes/lockd-disk-a-config/client.pem");
  require_file_or_skip(bundle_path);
  make_unique_name("workflow-domain", domain_key, sizeof(domain_key));
  client = NULL;
  workflow = NULL;
  transaction = NULL;
  participant = NULL;
  job = NULL;
  payload = NULL;
  duplicate_payload = NULL;
  domain_state = NULL;
  duplicate_transaction = NULL;
  payload_sink = NULL;
  payload_bytes = NULL;
  payload_length = 0U;
  payload_written = 0U;
  lc_error_init(&error);
  lc_get_opts_init(&get_options);
  memset(&get_result, 0, sizeof(get_result));
  open_tcp_client(endpoint, bundle_path, &client, &error);
  lc_workflow_config_init(&config);
  config.namespace_name = "default";
  config.owner = "workflow-e2e";
  rc = lc_client_new_workflow(client, &config, &workflow, &error);
  assert_lc_ok(rc, &error);
  lc_outbox_entry_init(&entry);
  entry.operation_id = domain_key;
  entry.effect_id = "notify";
  entry.effect_key = domain_key;
  entry.kind = "test";
  entry.destination = "https://example.invalid/workflow";
  entry.content_type = "text/plain";
  rc = lc_source_from_memory("workflow-payload", 16U, &payload, &error);
  assert_lc_ok(rc, &error);
  lc_outbox_receipt_init(&receipt);
  rc = lc_workflow_append_outbox(workflow, &entry, payload, &transaction,
                                 &receipt, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(transaction);
  lc_workflow_participant_request_init(&participant_request);
  participant_request.acquire.namespace_name = "default";
  participant_request.acquire.key = domain_key;
  participant_request.acquire.owner = "workflow-e2e";
  participant_request.acquire.ttl_seconds = 30L;
  rc = lc_workflow_transaction_acquire(transaction, &participant_request,
                                       &participant, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(participant->txn_id);
  rc = lc_source_from_memory("{\"workflow\":true}", 17U, &domain_state, &error);
  assert_lc_ok(rc, &error);
  rc = participant->update(participant, domain_state, NULL, &error);
  assert_lc_ok(rc, &error);
  lc_source_close(domain_state);
  domain_state = NULL;
  lc_workflow_participant_close(participant);
  participant = NULL;
  rc = lc_workflow_transaction_commit(transaction, &error);
  assert_lc_ok(rc, &error);
  lc_workflow_transaction_close(transaction);
  get_options.public_read = 1;
  rc = lc_sink_to_memory(&payload_sink, &error);
  assert_lc_ok(rc, &error);
  rc = client->get(client, domain_key, &get_options, payload_sink, &get_result,
                   &error);
  assert_lc_ok(rc, &error);
  assert_false(get_result.no_content);
  rc = lc_sink_memory_bytes(payload_sink, &payload_bytes, &payload_length,
                            &error);
  assert_lc_ok(rc, &error);
  assert_memory_equal(payload_bytes, "{\"workflow\":true}", 17U);
  lc_get_res_cleanup(&get_result);
  lc_sink_close(payload_sink);
  payload_sink = NULL;
  rc = lc_workflow_next(workflow, 2000L, &job, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(job);
  assert_string_equal(job->effect_key, domain_key);
  rc = lc_source_from_memory("workflow-payload", 16U, &duplicate_payload,
                             &error);
  assert_lc_ok(rc, &error);
  lc_outbox_receipt_init(&duplicate_receipt);
  duplicate_transaction = (lc_workflow_transaction *)1;
  rc = lc_workflow_append_outbox(workflow, &entry, duplicate_payload,
                                 &duplicate_transaction, &duplicate_receipt,
                                 &error);
  assert_lc_ok(rc, &error);
  assert_null(duplicate_transaction);
  assert_true(duplicate_receipt.duplicate);
  assert_string_equal(duplicate_receipt.outbox_key, receipt.outbox_key);
  lc_outbox_receipt_cleanup(&duplicate_receipt);
  lc_source_close(duplicate_payload);
  duplicate_payload = NULL;
  rc = lc_sink_to_memory(&payload_sink, &error);
  assert_lc_ok(rc, &error);
  rc = lc_outbox_job_write_payload(job, payload_sink, &payload_written, &error);
  assert_lc_ok(rc, &error);
  rc = lc_sink_memory_bytes(payload_sink, &payload_bytes, &payload_length,
                            &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(payload_written, 16U);
  assert_int_equal(payload_length, 16U);
  assert_memory_equal(payload_bytes, "workflow-payload", 16U);
  lc_sink_close(payload_sink);
  payload_sink = NULL;
  rc = lc_outbox_job_complete(job, NULL, &error);
  assert_lc_ok(rc, &error);
  lc_outbox_job_close(job);
  job = NULL;
  lc_source_close(payload);
  lc_outbox_receipt_cleanup(&receipt);
  if (duplicate_payload != NULL)
    lc_source_close(duplicate_payload);
  if (payload_sink != NULL)
    lc_sink_close(payload_sink);
  if (job != NULL)
    lc_outbox_job_close(job);
  lc_workflow_close(workflow);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void
test_disk_workflow_dispatcher_uses_internal_json_limit(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_workflow *workflow;
  lc_workflow_config config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_workflow_transaction *transaction;
  lc_outbox_job *job;
  lc_source *payload;
  lc_error error;
  char effect_key[128];
  char header_value[1537];
  char headers_json[1600];
  int header_length;
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./devenv/volumes/lockd-disk-a-config/client.pem");
  require_file_or_skip(bundle_path);
  make_unique_name("workflow-response-limit", effect_key, sizeof(effect_key));
  memset(header_value, 'a', sizeof(header_value) - 1U);
  header_value[sizeof(header_value) - 1U] = '\0';
  header_length = snprintf(headers_json, sizeof(headers_json),
                           "{\"x-workflow-proof\":\"%s\"}", header_value);
  assert_true(header_length > 1024);
  assert_true((size_t)header_length < sizeof(headers_json));
  client = NULL;
  workflow = NULL;
  transaction = NULL;
  job = NULL;
  payload = NULL;
  lc_error_init(&error);
  open_tcp_client_with_json_response_limit(endpoint, bundle_path, 1024U,
                                           &client, &error);
  lc_workflow_config_init(&config);
  config.namespace_name = "default";
  config.owner = "workflow-response-limit-e2e";
  rc = lc_client_new_workflow(client, &config, &workflow, &error);
  assert_lc_ok(rc, &error);
  lc_outbox_entry_init(&entry);
  entry.operation_id = effect_key;
  entry.effect_id = "notify";
  entry.effect_key = effect_key;
  entry.kind = "test";
  entry.destination = "https://example.invalid/workflow-response-limit";
  entry.content_type = "text/plain";
  entry.headers_json = headers_json;
  rc = lc_source_from_memory("workflow-payload", 16U, &payload, &error);
  assert_lc_ok(rc, &error);
  lc_outbox_receipt_init(&receipt);
  rc = lc_workflow_append_outbox(workflow, &entry, payload, &transaction,
                                 &receipt, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(transaction);
  rc = lc_workflow_transaction_commit(transaction, &error);
  assert_lc_ok(rc, &error);
  lc_workflow_transaction_close(transaction);
  transaction = NULL;
  rc = lc_workflow_next(workflow, 5000L, &job, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(job);
  assert_string_equal(job->effect_key, effect_key);
  rc = lc_outbox_job_complete(job, NULL, &error);
  assert_lc_ok(rc, &error);
  lc_outbox_job_close(job);
  lc_outbox_receipt_cleanup(&receipt);
  lc_source_close(payload);
  lc_workflow_close(workflow);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_workflow_retry_redelivery(void **state) {
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_workflow *workflow;
  lc_workflow_config config;
  lc_outbox_entry entry;
  lc_outbox_receipt receipt;
  lc_workflow_transaction *transaction;
  lc_outbox_job *job;
  lc_outbox_retry retry;
  lc_source *payload;
  lc_sink *export_sink;
  lc_dead_letter_export_opts export_options;
  lc_dead_letter_export_res export_result;
  lc_workflow_stats workflow_stats;
  const void *exported_bytes;
  size_t exported_length;
  lc_error error;
  char effect_key[128];
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./devenv/volumes/lockd-disk-a-config/client.pem");
  require_file_or_skip(bundle_path);
  make_unique_name("workflow-retry", effect_key, sizeof(effect_key));
  client = NULL;
  workflow = NULL;
  transaction = NULL;
  job = NULL;
  payload = NULL;
  export_sink = NULL;
  lc_error_init(&error);
  open_tcp_client(endpoint, bundle_path, &client, &error);
  lc_workflow_config_init(&config);
  config.namespace_name = "default";
  config.owner = "workflow-retry-e2e";
  rc = lc_client_new_workflow(client, &config, &workflow, &error);
  assert_lc_ok(rc, &error);
  lc_outbox_entry_init(&entry);
  entry.operation_id = effect_key;
  entry.effect_id = "retry";
  entry.effect_key = effect_key;
  entry.kind = "test";
  entry.destination = "retry://target";
  rc = lc_source_from_memory("retry-payload", 13U, &payload, &error);
  assert_lc_ok(rc, &error);
  lc_outbox_receipt_init(&receipt);
  rc = lc_workflow_append_outbox(workflow, &entry, payload, &transaction,
                                 &receipt, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(transaction);
  rc = lc_workflow_transaction_commit(transaction, &error);
  assert_lc_ok(rc, &error);
  lc_workflow_transaction_close(transaction);
  transaction = NULL;
  rc = lc_workflow_next(workflow, 3000L, &job, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(job);
  lc_outbox_retry_init(&retry);
  retry.delay_seconds = 1L;
  retry.diagnostic = "temporary";
  rc = lc_outbox_job_retry(job, &retry, &error);
  assert_lc_ok(rc, &error);
  lc_outbox_job_close(job);
  job = NULL;
  rc = lc_workflow_next(workflow, 5000L, &job, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(job);
  assert_string_equal(job->effect_key, effect_key);
  assert_int_equal(job->attempt, 2);
  rc = lc_outbox_job_dead_letter(job, "permanent", &error);
  assert_lc_ok(rc, &error);
  lc_outbox_job_close(job);
  job = NULL;
  lc_workflow_stats_init(&workflow_stats);
  rc = lc_workflow_get_stats(workflow, &workflow_stats, &error);
  assert_lc_ok(rc, &error);
  assert_true(workflow_stats.running);
  lc_workflow_stats_cleanup(&workflow_stats);
  lc_dead_letter_export_opts_init(&export_options);
  export_options.format = LC_DEAD_LETTER_EXPORT_JSONL;
  lc_dead_letter_export_res_init(&export_result);
  rc = lc_sink_to_memory(&export_sink, &error);
  assert_lc_ok(rc, &error);
  rc = lc_workflow_export_dead_letters(workflow, &export_options, export_sink,
                                       &export_result, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(export_result.exported, 1U);
  exported_bytes = NULL;
  exported_length = 0U;
  rc = lc_sink_memory_bytes(export_sink, &exported_bytes, &exported_length,
                            &error);
  assert_lc_ok(rc, &error);
  assert_true(exported_length > 0U);
  lc_sink_close(export_sink);
  export_sink = NULL;
  rc = lc_workflow_replay_dead_letter(workflow, receipt.outbox_key, &error);
  assert_lc_ok(rc, &error);
  rc = lc_workflow_next(workflow, 5000L, &job, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(job);
  assert_string_equal(job->effect_key, effect_key);
  assert_int_equal(job->attempt, 1);
  rc = lc_outbox_job_complete(job, NULL, &error);
  assert_lc_ok(rc, &error);
  lc_outbox_job_close(job);
  lc_dead_letter_export_res_init(&export_result);
  rc = lc_sink_to_memory(&export_sink, &error);
  assert_lc_ok(rc, &error);
  rc = lc_workflow_export_dead_letters(workflow, &export_options, export_sink,
                                       &export_result, &error);
  assert_lc_ok(rc, &error);
  assert_int_equal(export_result.exported, 0U);
  lc_sink_close(export_sink);
  export_sink = NULL;
  lc_outbox_receipt_cleanup(&receipt);
  lc_source_close(payload);
  lc_workflow_close(workflow);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

static void test_disk_workflow_startup_recovery(void **state) {
  static const char state_json[] =
      "{\"record_type\":\"lockdc.outbox.v1\",\"operation_id\":\"recovery-op\","
      "\"effect_id\":\"recovery-effect\",\"effect_key\":\"recovery-key\","
      "\"message_id\":\"msg_recovery\","
      "\"kind\":\"test\",\"destination\":\"recovery://target\","
      "\"content_type\":\"text/plain\",\"dispatch_state\":\"pending\","
      "\"attempt_count\":0,\"not_before_unix\":0}";
  const char *endpoint;
  const char *bundle_path;
  lc_client *client;
  lc_workflow *workflow;
  lc_workflow_config config;
  lc_acquire_req acquire;
  lc_lease *lease;
  lc_source *state_source;
  lc_source *payload_source;
  lc_attach_req attach;
  lc_attach_res attach_result;
  lc_outbox_job *job;
  lc_error error;
  char suffix[64];
  char key[128];
  int rc;

  (void)state;
  endpoint =
      env_or_default("LOCKDC_E2E_DISK_ENDPOINT", "https://localhost:19441");
  bundle_path =
      env_or_default("LOCKDC_E2E_DISK_BUNDLE",
                     "./devenv/volumes/lockd-disk-a-config/client.pem");
  require_file_or_skip(bundle_path);
  make_unique_name("workflow-recovery", suffix, sizeof(suffix));
  /* This fixture sorts before digest-shaped production keys so a bounded
   * recovery page proves discovery without claiming unrelated durable work in
   * the shared compose-test namespace. */
  assert_true(snprintf(key, sizeof(key), "__lockdc_io/v1/outbox/-%s", suffix) >
              0);
  client = NULL;
  workflow = NULL;
  lease = NULL;
  state_source = NULL;
  payload_source = NULL;
  job = NULL;
  lc_error_init(&error);
  open_tcp_client(endpoint, bundle_path, &client, &error);
  lc_acquire_req_init(&acquire);
  acquire.namespace_name = "default";
  acquire.key = key;
  acquire.owner = "workflow-recovery-e2e";
  acquire.ttl_seconds = 30L;
  rc = lc_acquire(client, &acquire, &lease, &error);
  assert_lc_ok(rc, &error);
  rc = lc_source_from_memory(state_json, sizeof(state_json) - 1U, &state_source,
                             &error);
  assert_lc_ok(rc, &error);
  rc = lc_lease_update(lease, state_source, NULL, &error);
  assert_lc_ok(rc, &error);
  lc_source_close(state_source);
  state_source = NULL;
  rc = lc_source_from_memory("recovery-payload", 16U, &payload_source, &error);
  assert_lc_ok(rc, &error);
  lc_attach_req_init(&attach);
  attach.name = "payload";
  attach.content_type = "text/plain";
  attach.prevent_overwrite = 1;
  memset(&attach_result, 0, sizeof(attach_result));
  rc = lc_lease_attach(lease, &attach, payload_source, &attach_result, &error);
  assert_lc_ok(rc, &error);
  lc_attach_res_cleanup(&attach_result);
  lc_source_close(payload_source);
  payload_source = NULL;
  rc = lc_lease_release(lease, NULL, &error);
  assert_lc_ok(rc, &error);
  lease = NULL;
  lc_workflow_config_init(&config);
  config.namespace_name = "default";
  config.owner = "workflow-recovery-e2e";
  config.notification_capacity = 1U;
  rc = lc_client_new_workflow(client, &config, &workflow, &error);
  assert_lc_ok(rc, &error);
  rc = lc_workflow_next(workflow, 5000L, &job, &error);
  assert_lc_ok(rc, &error);
  assert_non_null(job);
  assert_string_equal(job->effect_key, "recovery-key");
  rc = lc_outbox_job_complete(job, NULL, &error);
  assert_lc_ok(rc, &error);
  lc_outbox_job_close(job);
  lc_workflow_close(workflow);
  lc_client_close(client);
  lc_error_cleanup(&error);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_disk_lease_state_roundtrip),
      cmocka_unit_test(test_disk_server_minted_multikey_xa_transaction),
      cmocka_unit_test(test_disk_workflow_implicit_xa_roundtrip),
      cmocka_unit_test(test_disk_workflow_dispatcher_uses_internal_json_limit),
      cmocka_unit_test(test_disk_workflow_retry_redelivery),
      cmocka_unit_test(test_disk_workflow_startup_recovery),
      cmocka_unit_test(test_disk_server_explicit_xa_enlists_on_acquire),
      cmocka_unit_test(
          test_disk_server_metadata_finalization_preserves_staged_version),
      cmocka_unit_test(test_disk_acquire_for_update_roundtrip),
      cmocka_unit_test(test_disk_acquire_for_update_handler_error_rolls_back),
      cmocka_unit_test(test_disk_acquire_if_not_exists_conflict),
      cmocka_unit_test(test_disk_state_cas_failure_modes),
      cmocka_unit_test(test_disk_query_rejects_invalid_inputs),
      cmocka_unit_test(test_disk_query_documents_captures_lockd_trailers),
      cmocka_unit_test(test_disk_query_keys_streams_many_indexed_keys),
      cmocka_unit_test(test_disk_management_failure_modes),
      cmocka_unit_test(test_disk_auth_permission_failure_modes),
      cmocka_unit_test(
          test_disk_queue_watch_stream_observes_initial_unavailable),
      cmocka_unit_test(test_disk_queue_watch_stream_observes_enqueue),
      cmocka_unit_test(
          test_disk_queue_watch_stream_observes_available_then_unavailable),
      cmocka_unit_test(test_disk_local_mutate_stream_roundtrip)};
  return cmocka_run_group_tests(tests, NULL, NULL);
}
#elif defined(LC_E2E_GROUP_POUCH_DIRECT)
int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_pouch_direct_state_attachment_reopen_roundtrip),
      cmocka_unit_test(
          test_pouch_direct_lifecycle_maintenance_reopen_roundtrip),
      cmocka_unit_test(
          test_pouch_direct_large_namespace_segmented_index_reopen),
      cmocka_unit_test(
          test_pouch_direct_marker_damage_and_index_rebuild_after_snapshot),
      cmocka_unit_test(test_pouch_direct_consumer_service_with_state)};
  return cmocka_run_group_tests(tests, setup_pouch_e2e_group,
                                teardown_pouch_e2e_group);
}
#elif defined(LC_E2E_GROUP_S3_DIRECT)
int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_s3_lease_state_roundtrip),
      cmocka_unit_test(test_s3_attachment_failure_modes),
      cmocka_unit_test(test_s3_local_mutate_stream_roundtrip),
      cmocka_unit_test(test_s3_attachment_roundtrip),
      cmocka_unit_test(test_s3_dequeue_with_state_roundtrip)};
  return cmocka_run_group_tests(tests, NULL, NULL);
}
#elif defined(LC_E2E_GROUP_MEM_DIRECT)
int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_mem_uds_dequeue_with_state_roundtrip),
      cmocka_unit_test(test_mem_uds_dequeue_batch_roundtrip),
      cmocka_unit_test(test_mem_uds_queue_roundtrip),
      cmocka_unit_test(test_mem_uds_queue_failure_modes)};
  return cmocka_run_group_tests(tests, NULL, NULL);
}
#elif defined(LC_E2E_GROUP_DISK_CONSUMER)
int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_disk_consumer_service_happy),
      cmocka_unit_test(test_disk_consumer_service_explicit_ack),
      cmocka_unit_test(test_disk_consumer_service_multi_delivery),
      cmocka_unit_test(test_disk_consumer_service_with_state),
      cmocka_unit_test(
          test_disk_consumer_service_restart_after_handler_failure),
      cmocka_unit_test(
          test_disk_consumer_service_delivery_failures_ignore_failure_budget),
      cmocka_unit_test(test_disk_consumer_service_defer_then_redeliver),
      cmocka_unit_test(test_disk_consumer_service_explicit_failure_nack),
      cmocka_unit_test(
          test_disk_consumer_service_explicit_failure_nack_then_handler_error),
      cmocka_unit_test(test_disk_consumer_service_acks_without_reading_payload),
      cmocka_unit_test(
          test_disk_consumer_service_defers_without_reading_payload),
      cmocka_unit_test(
          test_disk_consumer_service_failure_nacks_without_reading_payload),
      cmocka_unit_test(
          test_disk_consumer_service_explicit_ack_then_handler_error)};
  return cmocka_run_group_tests(tests, NULL, NULL);
}
#elif defined(LC_E2E_GROUP_S3_CONSUMER)
int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_s3_consumer_service_happy),
      cmocka_unit_test(test_s3_consumer_service_explicit_ack),
      cmocka_unit_test(test_s3_consumer_service_multi_delivery),
      cmocka_unit_test(test_s3_consumer_service_with_state),
      cmocka_unit_test(test_s3_consumer_service_restart_after_handler_failure),
      cmocka_unit_test(
          test_s3_consumer_service_delivery_failures_ignore_failure_budget),
      cmocka_unit_test(test_s3_consumer_service_defer_then_redeliver),
      cmocka_unit_test(test_s3_consumer_service_explicit_failure_nack),
      cmocka_unit_test(
          test_s3_consumer_service_explicit_failure_nack_then_handler_error),
      cmocka_unit_test(test_s3_consumer_service_acks_without_reading_payload),
      cmocka_unit_test(test_s3_consumer_service_defers_without_reading_payload),
      cmocka_unit_test(
          test_s3_consumer_service_failure_nacks_without_reading_payload),
      cmocka_unit_test(
          test_s3_consumer_service_explicit_ack_then_handler_error)};
  return cmocka_run_group_tests(tests, NULL, NULL);
}
#elif defined(LC_E2E_GROUP_MEM_CONSUMER)
int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_mem_uds_consumer_service_happy),
      cmocka_unit_test(test_mem_uds_consumer_service_explicit_ack),
      cmocka_unit_test(test_mem_uds_consumer_service_multi_delivery),
      cmocka_unit_test(test_mem_uds_consumer_service_multi_worker),
      cmocka_unit_test(test_mem_uds_consumer_service_with_state),
      cmocka_unit_test(
          test_mem_uds_consumer_service_restart_after_handler_failure),
      cmocka_unit_test(
          test_mem_uds_consumer_service_delivery_failures_ignore_failure_budget),
      cmocka_unit_test(test_mem_uds_consumer_service_defer_then_redeliver),
      cmocka_unit_test(test_mem_uds_consumer_service_explicit_failure_nack),
      cmocka_unit_test(
          test_mem_uds_consumer_service_explicit_failure_nack_then_handler_error),
      cmocka_unit_test(
          test_mem_uds_consumer_service_acks_without_reading_payload),
      cmocka_unit_test(
          test_mem_uds_consumer_service_defers_without_reading_payload),
      cmocka_unit_test(
          test_mem_uds_consumer_service_failure_nacks_without_reading_payload),
      cmocka_unit_test(
          test_mem_uds_consumer_service_explicit_ack_then_handler_error)};
  return cmocka_run_group_tests(tests, NULL, NULL);
}
#else
#error "An LC_E2E_GROUP_* compile definition is required"
#endif

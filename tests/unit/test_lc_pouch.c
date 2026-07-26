#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cmocka.h>

#include "lc/lc.h"
#include "lc_pouch.h"
#include "lc_pouch_namespace.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

typedef struct pouch_value_doc {
  lonejson_int64 value;
} pouch_value_doc;

static const lonejson_field pouch_value_fields[] = {
    LONEJSON_FIELD_I64(pouch_value_doc, value, "value")};

LONEJSON_MAP_DEFINE(pouch_value_map, pouch_value_doc, pouch_value_fields);

static void make_root(const char *suffix, char *root, size_t root_size) {
  snprintf(root, root_size, "/tmp/liblockdc-unit-pouch-redesign-%ld-%s",
           (long)getpid(), suffix);
}

static void make_endpoint(const char *root, char *endpoint,
                          size_t endpoint_size) {
  snprintf(endpoint, endpoint_size, "pouch://%s", root);
}

static int has_prefix(const char *value, const char *prefix) {
  return value != NULL && strncmp(value, prefix, strlen(prefix)) == 0;
}

static void cleanup_tree(const char *path) {
  DIR *dir;
  struct dirent *entry;
  struct stat st;

  if (lstat(path, &st) != 0) {
    unlink(path);
    return;
  }
  if (!S_ISDIR(st.st_mode)) {
    unlink(path);
    return;
  }
  dir = opendir(path);
  if (dir == NULL) {
    unlink(path);
    return;
  }
  while ((entry = readdir(dir)) != NULL) {
    char child[1024];

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    snprintf(child, sizeof(child), "%s/%s", path, entry->d_name);
    cleanup_tree(child);
  }
  closedir(dir);
  rmdir(path);
}

static void cleanup_root(const char *root) {
  static const char prefix[] = "/tmp/liblockdc-unit-pouch-redesign-";

  if (has_prefix(root, prefix)) {
    cleanup_tree(root);
  }
}

static void cleanup_all_roots(void) {
  static const char prefix[] = "liblockdc-unit-pouch-redesign-";
  DIR *dir;
  struct dirent *entry;

  dir = opendir("/tmp");
  if (dir == NULL) {
    return;
  }
  while ((entry = readdir(dir)) != NULL) {
    char path[1024];

    if (!has_prefix(entry->d_name, prefix)) {
      continue;
    }
    snprintf(path, sizeof(path), "/tmp/%s", entry->d_name);
    cleanup_root(path);
  }
  closedir(dir);
}

static int setup_pouch_unit_group(void **state) {
  (void)state;
  cleanup_all_roots();
  return 0;
}

static int teardown_pouch_unit_group(void **state) {
  (void)state;
  cleanup_all_roots();
  return 0;
}

static int path_is_dir(const char *path) {
  struct stat st;

  return stat(path, &st) == 0 && S_ISDIR(st.st_mode);
}

static int path_is_file(const char *path) {
  struct stat st;

  return stat(path, &st) == 0 && S_ISREG(st.st_mode);
}

static void assert_path_dir(const char *root, const char *leaf) {
  char path[1024];

  snprintf(path, sizeof(path), "%s/%s", root, leaf);
  assert_true(path_is_dir(path));
}

static void assert_path_file(const char *root, const char *leaf) {
  char path[1024];

  snprintf(path, sizeof(path), "%s/%s", root, leaf);
  assert_true(path_is_file(path));
}

static void write_text_file(const char *path, const char *text) {
  FILE *fp;

  fp = fopen(path, "wb");
  assert_non_null(fp);
  assert_int_equal(fputs(text, fp) < 0 ? -1 : 0, 0);
  assert_int_equal(fclose(fp), 0);
}

static void assert_file_contains(const char *path, const char *needle) {
  FILE *fp;
  char bytes[1024];
  size_t nread;

  fp = fopen(path, "rb");
  assert_non_null(fp);
  nread = fread(bytes, 1U, sizeof(bytes) - 1U, fp);
  assert_int_equal(fclose(fp), 0);
  bytes[nread] = '\0';
  assert_non_null(strstr(bytes, needle));
}

static void assert_path_file_contains(const char *root, const char *leaf,
                                      const char *needle) {
  char path[1024];

  snprintf(path, sizeof(path), "%s/%s", root, leaf);
  assert_file_contains(path, needle);
}

static void read_source_to_string(lc_source *source, char *buffer,
                                  size_t buffer_size) {
  lc_error error;
  size_t offset;

  lc_error_init(&error);
  offset = 0U;
  for (;;) {
    size_t nread;

    assert_true(offset < buffer_size);
    nread = source->read(source, buffer + offset, buffer_size - offset - 1U,
                         &error);
    if (nread == 0U) {
      assert_int_equal(error.code, LC_OK);
      break;
    }
    offset += nread;
  }
  buffer[offset] = '\0';
  lc_error_cleanup(&error);
}

static void open_pouch_client(const char *root, lc_client **out,
                              lc_error *error) {
  lc_client_config config;
  const char *endpoints[1];
  char endpoint[540];
  int rc;

  make_endpoint(root, endpoint, sizeof(endpoint));
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  rc = lc_client_open(&config, out, error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(*out);
}

static void write_client_state(lc_client *client, const char *key,
                               const char *json,
                               const char *expected_etag,
                               long expected_version,
                               int has_expected_version,
                               lc_update_res *out, lc_error *error) {
  lc_update_req update_req;
  lc_source *source;
  int rc;

  lc_update_req_init(&update_req);
  update_req.lease.key = key;
  update_req.if_state_etag = expected_etag;
  update_req.if_version = expected_version;
  update_req.has_if_version = has_expected_version;
  rc = lc_source_from_memory(json, strlen(json), &source, error);
  assert_int_equal(rc, LC_OK);
  rc = client->update(client, &update_req, source, out, error);
  source->close(source);
  assert_int_equal(rc, LC_OK);
}

static void test_open_creates_segmented_root_layout(void **state) {
  lc_pouch *pouch;
  lc_pouch_status status;
  lc_pouch_open_options options;
  lc_error error;
  char root[512];
  int rc;

  (void)state;
  memset(&options, 0, sizeof(options));
  memset(&status, 0, sizeof(status));
  lc_error_init(&error);
  make_root("root-layout", root, sizeof(root));
  cleanup_root(root);

  options.segment_target_bytes = 4096UL;
  options.background_compaction_enabled = 1;
  options.compaction_interval_seconds = 30UL;
  rc = lc_pouch_open(root, NULL, &options, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(pouch);
  assert_path_dir(root, "namespaces");
  assert_path_file(root, "manifest");

  rc = lc_pouch_status_read(pouch, &status, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(status.root_path, root);
  assert_string_equal(status.layout_name, "pouch-segmented");
  assert_int_equal(status.layout_version, 1UL);
  assert_int_equal(status.segment_target_bytes, 4096UL);
  assert_int_equal(status.background_compaction_enabled, 1);
  assert_int_equal(status.compaction_interval_seconds, 30UL);

  lc_pouch_status_cleanup(NULL, &status);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_ensure_namespace_creates_per_namespace_layout(void **state) {
  lc_pouch *pouch;
  lc_error error;
  char root[512];
  char *namespace_path;
  int rc;

  (void)state;
  lc_error_init(&error);
  make_root("namespace-layout", root, sizeof(root));
  cleanup_root(root);

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_ensure_namespace(pouch, "team/alpha", &error);
  assert_int_equal(rc, LC_OK);

  namespace_path = lc_pouch_namespace_path(NULL, root, "team/alpha");
  assert_non_null(namespace_path);
  assert_true(strstr(namespace_path, "team%2falpha") != NULL);
  assert_path_dir(namespace_path, "segments");
  assert_path_dir(namespace_path, "payloads");
  assert_path_dir(namespace_path, "snapshots");
  assert_path_dir(namespace_path, "markers");
  assert_path_dir(namespace_path, "index");
  assert_path_file(namespace_path, "manifest");
  assert_path_file_contains(namespace_path, "manifest",
                            "active_segment=seg-00000000000000000001.log");

  free(namespace_path);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_pouch_endpoint_opens_new_backend_without_http_engine(
    void **state) {
  lc_client_config config;
  lc_client *client;
  lc_error error;
  const char *endpoints[1];
  char root[512];
  char endpoint[540];
  int rc;

  (void)state;
  lc_error_init(&error);
  make_root("client-open", root, sizeof(root));
  cleanup_root(root);
  snprintf(endpoint, sizeof(endpoint), "pouch://%s", root);
  endpoints[0] = endpoint;

  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;

  rc = lc_client_open(&config, &client, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(client);
  assert_path_file(root, "manifest");

  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_state_write_read_replays_segment_after_reopen(void **state) {
  lc_pouch *pouch;
  lc_source *body;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result write_result;
  lc_pouch_state_read_result read_result;
  lc_error error;
  char root[512];
  char bytes[64];
  int rc;

  (void)state;
  lc_error_init(&error);
  memset(&options, 0, sizeof(options));
  memset(&write_result, 0, sizeof(write_result));
  memset(&read_result, 0, sizeof(read_result));
  make_root("state-replay", root, sizeof(root));
  cleanup_root(root);

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("alpha-state", strlen("alpha-state"), &body,
                             &error);
  assert_int_equal(rc, LC_OK);
  options.content_type = "text/plain";
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/current", body,
                            &options, &write_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(write_result.etag, "pouch-state-1");
  assert_int_equal(write_result.version, 1UL);
  assert_int_equal(write_result.bytes, strlen("alpha-state"));
  body->close(body);
  lc_pouch_close(pouch);

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, "team/alpha", "state/current", &read_result,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_result.found);
  assert_string_equal(read_result.content_type, "text/plain");
  assert_string_equal(read_result.etag, "pouch-state-1");
  assert_int_equal(read_result.version, 1UL);
  assert_int_equal(read_result.bytes, strlen("alpha-state"));
  read_source_to_string(read_result.body, bytes, sizeof(bytes));
  assert_string_equal(bytes, "alpha-state");

  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_state_write_enforces_expected_etag(void **state) {
  lc_pouch *pouch;
  lc_source *body;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result first;
  lc_pouch_state_write_result second;
  lc_error error;
  char root[512];
  int rc;

  (void)state;
  lc_error_init(&error);
  memset(&options, 0, sizeof(options));
  memset(&first, 0, sizeof(first));
  memset(&second, 0, sizeof(second));
  make_root("state-etag", root, sizeof(root));
  cleanup_root(root);

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("one", strlen("one"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/current", body, NULL,
                            &first, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);

  lc_error_cleanup(&error);
  lc_error_init(&error);
  rc = lc_source_from_memory("two", strlen("two"), &body, &error);
  assert_int_equal(rc, LC_OK);
  options.expected_etag = "wrong-etag";
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/current", body,
                            &options, &second, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  body->close(body);

  lc_error_cleanup(&error);
  lc_error_init(&error);
  options.expected_etag = first.etag;
  rc = lc_source_from_memory("two", strlen("two"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/current", body,
                            &options, &second, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(second.etag, "pouch-state-2");
  assert_int_equal(second.version, 2UL);
  body->close(body);

  lc_pouch_state_write_result_cleanup(NULL, &first);
  lc_pouch_state_write_result_cleanup(NULL, &second);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_state_writes_roll_active_manifest_segment(void **state) {
  lc_pouch *pouch;
  lc_source *body;
  lc_pouch_open_options open_options;
  lc_pouch_state_write_result first;
  lc_pouch_state_write_result second;
  lc_pouch_state_read_result read_result;
  lc_error error;
  char root[512];
  char bytes[64];
  char *namespace_path;
  int rc;

  (void)state;
  lc_error_init(&error);
  memset(&open_options, 0, sizeof(open_options));
  memset(&first, 0, sizeof(first));
  memset(&second, 0, sizeof(second));
  memset(&read_result, 0, sizeof(read_result));
  make_root("state-rollover", root, sizeof(root));
  cleanup_root(root);

  open_options.segment_target_bytes = 128UL;
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("one", strlen("one"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/current", body, NULL,
                            &first, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);

  rc = lc_source_from_memory("two", strlen("two"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/current", body, NULL,
                            &second, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);

  namespace_path = lc_pouch_namespace_path(NULL, root, "team/alpha");
  assert_non_null(namespace_path);
  assert_path_file(namespace_path, "segments/seg-00000000000000000001.log");
  assert_path_file(namespace_path, "segments/seg-00000000000000000002.log");
  assert_path_file_contains(namespace_path, "manifest",
                            "active_segment=seg-00000000000000000002.log");

  lc_pouch_close(pouch);
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, "team/alpha", "state/current", &read_result,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_result.found);
  assert_string_equal(read_result.etag, "pouch-state-2");
  read_source_to_string(read_result.body, bytes, sizeof(bytes));
  assert_string_equal(bytes, "two");

  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_state_write_result_cleanup(NULL, &first);
  lc_pouch_state_write_result_cleanup(NULL, &second);
  free(namespace_path);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_namespace_manifest_repairs_from_existing_segments(
    void **state) {
  lc_pouch *pouch;
  lc_source *body;
  lc_pouch_state_write_result write_result;
  lc_pouch_state_read_result read_result;
  lc_error error;
  char root[512];
  char manifest_path[1024];
  char bytes[64];
  char *namespace_path;
  int rc;

  (void)state;
  lc_error_init(&error);
  memset(&write_result, 0, sizeof(write_result));
  memset(&read_result, 0, sizeof(read_result));
  make_root("manifest-repair", root, sizeof(root));
  cleanup_root(root);

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("repair-me", strlen("repair-me"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/current", body, NULL,
                            &write_result, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);
  lc_pouch_close(pouch);

  namespace_path = lc_pouch_namespace_path(NULL, root, "team/alpha");
  assert_non_null(namespace_path);
  snprintf(manifest_path, sizeof(manifest_path), "%s/manifest",
           namespace_path);
  write_text_file(manifest_path, "broken=true\nactive_segment=bad\n");

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, "team/alpha", "state/current", &read_result,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_result.found);
  read_source_to_string(read_result.body, bytes, sizeof(bytes));
  assert_string_equal(bytes, "repair-me");
  assert_path_file_contains(namespace_path, "manifest",
                            "active_segment=seg-00000000000000000001.log");

  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  free(namespace_path);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_client_update_get_load_roundtrips_state(void **state) {
  lc_client *client;
  lc_client *reader;
  lc_sink *sink;
  lc_update_res update_res;
  lc_get_res get_res;
  pouch_value_doc doc;
  lc_error error;
  const void *bytes;
  size_t length;
  char root[512];
  char key[96];
  int rc;

  (void)state;
  lc_error_init(&error);
  memset(&update_res, 0, sizeof(update_res));
  memset(&get_res, 0, sizeof(get_res));
  memset(&doc, 0, sizeof(doc));
  make_root("client-state", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/client/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  write_client_state(client, key, "{\"value\":42}", NULL, 0L, 0, &update_res,
                     &error);
  assert_string_equal(update_res.new_state_etag, "pouch-state-1");
  assert_int_equal(update_res.new_version, 1L);
  assert_int_equal(update_res.bytes, strlen("{\"value\":42}"));
  lc_client_close(client);

  open_pouch_client(root, &reader, &error);
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = reader->get(reader, key, NULL, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  assert_string_equal(get_res.content_type, "application/json");
  assert_string_equal(get_res.etag, "pouch-state-1");
  assert_int_equal(get_res.version, 1L);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, strlen("{\"value\":42}"));
  assert_memory_equal(bytes, "{\"value\":42}", strlen("{\"value\":42}"));
  sink->close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);
  memset(&get_res, 0, sizeof(get_res));

  rc = reader->load(reader, key, &pouch_value_map, &doc, NULL, &get_res,
                    &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  assert_int_equal(doc.value, 42);
  assert_string_equal(get_res.etag, "pouch-state-1");

  lc_get_res_cleanup(&get_res);
  lc_update_res_cleanup(&update_res);
  lc_client_close(reader);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_client_update_enforces_state_preconditions(void **state) {
  lc_client *client;
  lc_update_res first;
  lc_update_res second;
  lc_source *source;
  lc_update_req update_req;
  lc_error error;
  char root[512];
  int rc;

  (void)state;
  lc_error_init(&error);
  memset(&first, 0, sizeof(first));
  memset(&second, 0, sizeof(second));
  make_root("client-preconditions", root, sizeof(root));
  cleanup_root(root);

  open_pouch_client(root, &client, &error);
  write_client_state(client, "state/current", "{\"value\":1}", NULL, 0L, 0,
                     &first, &error);

  lc_update_req_init(&update_req);
  update_req.lease.key = "state/current";
  update_req.if_state_etag = "wrong";
  rc = lc_source_from_memory("{\"value\":2}", strlen("{\"value\":2}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->update(client, &update_req, source, &second, &error);
  source->close(source);
  assert_int_equal(rc, LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  lc_update_req_init(&update_req);
  update_req.lease.key = "state/current";
  update_req.if_state_etag = first.new_state_etag;
  update_req.if_version = 99L;
  update_req.has_if_version = 1;
  rc = lc_source_from_memory("{\"value\":2}", strlen("{\"value\":2}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->update(client, &update_req, source, &second, &error);
  source->close(source);
  assert_int_equal(rc, LC_ERR_INVALID);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  write_client_state(client, "state/current", "{\"value\":2}",
                     first.new_state_etag, 1L, 1, &second, &error);
  assert_string_equal(second.new_state_etag, "pouch-state-2");
  assert_int_equal(second.new_version, 2L);

  lc_update_res_cleanup(&first);
  lc_update_res_cleanup(&second);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_client_get_missing_and_public_state_behavior(void **state) {
  lc_client *client;
  lc_sink *sink;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lc_error error;
  char root[512];
  int rc;

  (void)state;
  lc_error_init(&error);
  memset(&get_res, 0, sizeof(get_res));
  lc_get_opts_init(&get_opts);
  make_root("client-missing", root, sizeof(root));
  cleanup_root(root);

  open_pouch_client(root, &client, &error);
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get(client, "missing", NULL, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(get_res.no_content);
  lc_get_res_cleanup(&get_res);
  sink->close(sink);

  lc_error_cleanup(&error);
  lc_error_init(&error);
  memset(&get_res, 0, sizeof(get_res));
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  get_opts.public_read = 1;
  rc = client->get(client, "missing", &get_opts, sink, &get_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  sink->close(sink);

  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_lease_bound_state_update_get_and_release(void **state) {
  lc_client *client;
  lc_client *reader;
  lc_lease *lease;
  lc_source *source;
  lc_sink *sink;
  lc_acquire_req acquire_req;
  lc_release_req release_req;
  lc_get_res get_res;
  lc_error error;
  const void *bytes;
  size_t length;
  char root[512];
  char key[96];
  int rc;

  (void)state;
  client = NULL;
  reader = NULL;
  lease = NULL;
  source = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  lc_release_req_init(&release_req);
  memset(&get_res, 0, sizeof(get_res));
  make_root("lease-state", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/lease/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(lease);
  assert_string_equal(lease->key, key);
  assert_string_equal(lease->owner, "lc-unit-pouch");
  assert_int_equal(lease->version, 0L);

  rc = lc_source_from_memory("{\"value\":7}", strlen("{\"value\":7}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lease->update(lease, source, NULL, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 1L);
  assert_string_equal(lease->state_etag, "pouch-state-1");

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = lease->get(lease, sink, NULL, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  assert_string_equal(get_res.content_type, "application/json");
  assert_string_equal(get_res.etag, "pouch-state-1");
  assert_int_equal(get_res.version, 1L);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, strlen("{\"value\":7}"));
  assert_memory_equal(bytes, "{\"value\":7}", strlen("{\"value\":7}"));
  sink->close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);

  rc = lease->release(lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  lease = NULL;

  lc_error_cleanup(&error);
  lc_error_init(&error);
  lc_acquire_req_init(&acquire_req);
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 30L;
  acquire_req.if_not_exists = 1;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(lease);

  lc_client_close(client);
  client = NULL;

  lc_error_cleanup(&error);
  lc_error_init(&error);
  memset(&get_res, 0, sizeof(get_res));
  open_pouch_client(root, &reader, &error);
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = reader->get(reader, key, NULL, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  assert_int_equal(get_res.version, 1L);
  sink->close(sink);

  lc_get_res_cleanup(&get_res);
  lc_client_close(reader);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_open_creates_segmented_root_layout),
      cmocka_unit_test(test_ensure_namespace_creates_per_namespace_layout),
      cmocka_unit_test(
          test_pouch_endpoint_opens_new_backend_without_http_engine),
      cmocka_unit_test(test_state_write_read_replays_segment_after_reopen),
      cmocka_unit_test(test_state_write_enforces_expected_etag),
      cmocka_unit_test(test_state_writes_roll_active_manifest_segment),
      cmocka_unit_test(
          test_namespace_manifest_repairs_from_existing_segments),
      cmocka_unit_test(test_client_update_get_load_roundtrips_state),
      cmocka_unit_test(test_client_update_enforces_state_preconditions),
      cmocka_unit_test(test_client_get_missing_and_public_state_behavior),
      cmocka_unit_test(test_lease_bound_state_update_get_and_release),
  };

  return cmocka_run_group_tests(tests, setup_pouch_unit_group,
                                teardown_pouch_unit_group);
}

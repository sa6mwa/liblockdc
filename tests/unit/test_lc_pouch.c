#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cmocka.h>

#include "lc/lc.h"
#include "lc_pouch.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "../support/lc_test_tmp.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define POUCH_UNIT_TMP_PREFIX "/tmp/liblockdc-unit-pouch-redesign-"

typedef struct pouch_value_doc {
  lonejson_int64 value;
} pouch_value_doc;

typedef struct pouch_acquire_for_update_state {
  const char *expected_snapshot;
  const char *expected_visible_during_update;
  const char *replacement;
  lc_client *observer;
  const char *key;
  int saw_snapshot;
  int saw_staged_invisible;
  int saw_staging_key_rejected;
  int fail;
} pouch_acquire_for_update_state;

static const lonejson_field pouch_value_fields[] = {
    LONEJSON_FIELD_I64(pouch_value_doc, value, "value")};

LONEJSON_MAP_DEFINE(pouch_value_map, pouch_value_doc, pouch_value_fields);

static void make_root(const char *suffix, char *root, size_t root_size) {
  char template_path[512];
  int written;

  written = snprintf(template_path, sizeof(template_path),
                     POUCH_UNIT_TMP_PREFIX "%s-XXXXXX", suffix);
  assert_true(written > 0 && (size_t)written < sizeof(template_path));
  assert_true(
      lc_test_tmp_mkdtemp(template_path, root, root_size,
                          POUCH_UNIT_TMP_PREFIX));
}

static void make_endpoint(const char *root, char *endpoint,
                          size_t endpoint_size) {
  snprintf(endpoint, endpoint_size, "pouch://%s", root);
}

static void cleanup_root(const char *root) {
  lc_test_tmp_cleanup_path(root, POUCH_UNIT_TMP_PREFIX);
}

static void cleanup_all_roots(void) {
  lc_test_tmp_cleanup_stale("/tmp", "liblockdc-unit-pouch-redesign-",
                            POUCH_UNIT_TMP_PREFIX);
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

static void find_single_marker_path(const char *root,
                                    const char *namespace_name, char *path,
                                    size_t path_size) {
  char *namespace_path;
  char markers_path[1024];
  DIR *dir;
  struct dirent *entry;
  int found;
  int written;

  namespace_path = lc_pouch_namespace_path(NULL, root, namespace_name);
  assert_non_null(namespace_path);
  written = snprintf(markers_path, sizeof(markers_path), "%s/markers",
                     namespace_path);
  assert_true(written > 0 && (size_t)written < sizeof(markers_path));
  dir = opendir(markers_path);
  assert_non_null(dir);
  found = 0;
  while ((entry = readdir(dir)) != NULL) {
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    assert_int_equal(found, 0);
    written = snprintf(path, path_size, "%s/%s", markers_path, entry->d_name);
    assert_true(written > 0 && (size_t)written < path_size);
    found = 1;
  }
  assert_int_equal(closedir(dir), 0);
  assert_int_equal(found, 1);
  free(namespace_path);
}

static void make_peer_marker_path(const char *root, const char *namespace_name,
                                  const char *leaf, char *path,
                                  size_t path_size) {
  char *namespace_path;
  int written;

  namespace_path = lc_pouch_namespace_path(NULL, root, namespace_name);
  assert_non_null(namespace_path);
  written = snprintf(path, path_size, "%s/markers/%s", namespace_path, leaf);
  assert_true(written > 0 && (size_t)written < path_size);
  free(namespace_path);
}

static unsigned long read_marker_sequence(const char *path, size_t *size) {
  FILE *fp;
  struct stat st;
  char line[256];
  unsigned long sequence;
  int found;

  assert_int_equal(stat(path, &st), 0);
  assert_true(S_ISREG(st.st_mode));
  if (size != NULL) {
    *size = (size_t)st.st_size;
  }
  fp = fopen(path, "rb");
  assert_non_null(fp);
  sequence = 0UL;
  found = 0;
  while (fgets(line, sizeof(line), fp) != NULL) {
    unsigned long parsed;

    if (sscanf(line, "sequence=%lu", &parsed) == 1) {
      sequence = parsed;
      found = 1;
      break;
    }
  }
  assert_int_equal(fclose(fp), 0);
  assert_true(found);
  return sequence;
}

static void test_marker_snapshots_detect_peer_changes(void **state) {
  lc_pouch *pouch;
  lc_pouch_namespace_marker_snapshot empty_snapshot;
  lc_pouch_namespace_marker_snapshot self_snapshot;
  lc_pouch_namespace_marker_snapshot peer_snapshot;
  lc_pouch_namespace_marker_snapshot unchanged_snapshot;
  lc_pouch_namespace_marker_snapshot changed_snapshot;
  lc_error error;
  char root[512];
  char *namespace_path;
  char peer_a_path[1024];
  char peer_b_path[1024];
  const char *peer_a;
  const char *peer_b;
  int rc;

  (void)state;
  pouch = NULL;
  namespace_path = NULL;
  memset(&empty_snapshot, 0, sizeof(empty_snapshot));
  memset(&self_snapshot, 0, sizeof(self_snapshot));
  memset(&peer_snapshot, 0, sizeof(peer_snapshot));
  memset(&unchanged_snapshot, 0, sizeof(unchanged_snapshot));
  memset(&changed_snapshot, 0, sizeof(changed_snapshot));
  lc_error_init(&error);
  make_root("marker-snapshot", root, sizeof(root));
  cleanup_root(root);

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_ensure_namespace(pouch, "default", &error);
  assert_int_equal(rc, LC_OK);
  namespace_path = lc_pouch_namespace_path(NULL, root, "default");
  assert_non_null(namespace_path);

  rc = lc_pouch_namespace_marker_snapshot_read(NULL, namespace_path, NULL,
                                               &empty_snapshot, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(empty_snapshot.marker_count, 0UL);
  assert_string_equal(empty_snapshot.fingerprint, "");

  rc = lc_pouch_namespace_touch_marker(NULL, namespace_path,
                                       "writer-self.marker", 1UL, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_namespace_marker_snapshot_read(NULL, namespace_path,
                                               "writer-self.marker",
                                               &self_snapshot, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(self_snapshot.marker_count, 0UL);
  assert_int_equal(lc_pouch_namespace_marker_snapshot_changed(
                       &empty_snapshot, &self_snapshot),
                   0);

  make_peer_marker_path(root, "default", "writer-000000000002.marker",
                        peer_b_path, sizeof(peer_b_path));
  make_peer_marker_path(root, "default", "writer-000000000001.marker",
                        peer_a_path, sizeof(peer_a_path));
  write_text_file(peer_b_path, "writer_pid=2\nsequence=1\n");
  write_text_file(peer_a_path, "writer_pid=1\nsequence=1\npad=x\n");
  rc = lc_pouch_namespace_marker_snapshot_read(NULL, namespace_path,
                                               "writer-self.marker",
                                               &peer_snapshot, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(peer_snapshot.marker_count, 2UL);
  peer_a = strstr(peer_snapshot.fingerprint, "writer-000000000001.marker");
  peer_b = strstr(peer_snapshot.fingerprint, "writer-000000000002.marker");
  assert_non_null(peer_a);
  assert_non_null(peer_b);
  assert_true(peer_a < peer_b);
  assert_int_equal(lc_pouch_namespace_marker_snapshot_changed(
                       &self_snapshot, &peer_snapshot),
                   1);

  rc = lc_pouch_namespace_marker_snapshot_read(NULL, namespace_path,
                                               "writer-self.marker",
                                               &unchanged_snapshot, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lc_pouch_namespace_marker_snapshot_changed(
                       &peer_snapshot, &unchanged_snapshot),
                   0);

  write_text_file(peer_a_path, "writer_pid=1\nsequence=2\npad=longer\n");
  rc = lc_pouch_namespace_marker_snapshot_read(NULL, namespace_path,
                                               "writer-self.marker",
                                               &changed_snapshot, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(changed_snapshot.marker_count, 2UL);
  assert_int_equal(lc_pouch_namespace_marker_snapshot_changed(
                       &unchanged_snapshot, &changed_snapshot),
                   1);

  lc_pouch_namespace_marker_snapshot_cleanup(NULL, &changed_snapshot);
  lc_pouch_namespace_marker_snapshot_cleanup(NULL, &unchanged_snapshot);
  lc_pouch_namespace_marker_snapshot_cleanup(NULL, &peer_snapshot);
  lc_pouch_namespace_marker_snapshot_cleanup(NULL, &self_snapshot);
  lc_pouch_namespace_marker_snapshot_cleanup(NULL, &empty_snapshot);
  free(namespace_path);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_marker_snapshots_treat_same_process_handles_as_peers(
    void **state) {
  lc_pouch *first;
  lc_pouch *second;
  lc_pouch_namespace_marker_snapshot first_view;
  lc_pouch_namespace_marker_snapshot second_view;
  lc_error error;
  char root[512];
  char *namespace_path;
  int rc;

  (void)state;
  first = NULL;
  second = NULL;
  namespace_path = NULL;
  memset(&first_view, 0, sizeof(first_view));
  memset(&second_view, 0, sizeof(second_view));
  lc_error_init(&error);
  make_root("marker-same-process", root, sizeof(root));
  cleanup_root(root);

  rc = lc_pouch_open(root, NULL, NULL, &first, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_open(root, NULL, NULL, &second, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(first->writer_marker_leaf);
  assert_non_null(second->writer_marker_leaf);
  assert_true(strcmp(first->writer_marker_leaf, second->writer_marker_leaf) !=
              0);
  rc = lc_pouch_ensure_namespace(first, "default", &error);
  assert_int_equal(rc, LC_OK);
  namespace_path = lc_pouch_namespace_path(NULL, root, "default");
  assert_non_null(namespace_path);

  rc = lc_pouch_namespace_touch_marker(NULL, namespace_path,
                                       first->writer_marker_leaf, 1UL, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_namespace_touch_marker(NULL, namespace_path,
                                       second->writer_marker_leaf, 1UL, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_pouch_namespace_marker_snapshot_read(NULL, namespace_path,
                                               first->writer_marker_leaf,
                                               &first_view, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(first_view.marker_count, 1UL);
  assert_non_null(strstr(first_view.fingerprint, second->writer_marker_leaf));
  assert_null(strstr(first_view.fingerprint, first->writer_marker_leaf));

  rc = lc_pouch_namespace_marker_snapshot_read(NULL, namespace_path,
                                               second->writer_marker_leaf,
                                               &second_view, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(second_view.marker_count, 1UL);
  assert_non_null(strstr(second_view.fingerprint, first->writer_marker_leaf));
  assert_null(strstr(second_view.fingerprint, second->writer_marker_leaf));

  lc_pouch_namespace_marker_snapshot_cleanup(NULL, &second_view);
  lc_pouch_namespace_marker_snapshot_cleanup(NULL, &first_view);
  free(namespace_path);
  lc_pouch_close(second);
  lc_pouch_close(first);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_marker_refresh_uses_directory_fast_path_and_force(
    void **state) {
  lc_pouch *pouch;
  lc_pouch_namespace_marker_refresh_state refresh;
  lc_pouch_namespace_marker_directory_snapshot before_dir;
  lc_pouch_namespace_marker_directory_snapshot after_dir;
  lc_error error;
  char root[512];
  char *namespace_path;
  char peer_path[1024];
  int should_scan;
  int rc;

  (void)state;
  pouch = NULL;
  namespace_path = NULL;
  memset(&refresh, 0, sizeof(refresh));
  memset(&before_dir, 0, sizeof(before_dir));
  memset(&after_dir, 0, sizeof(after_dir));
  should_scan = 0;
  lc_error_init(&error);
  make_root("marker-refresh", root, sizeof(root));
  cleanup_root(root);

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_ensure_namespace(pouch, "default", &error);
  assert_int_equal(rc, LC_OK);
  namespace_path = lc_pouch_namespace_path(NULL, root, "default");
  assert_non_null(namespace_path);

  rc = lc_pouch_namespace_marker_refresh_should_scan(
      NULL, namespace_path, "writer-self.marker", &refresh, 2UL, &should_scan,
      &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(should_scan, 1);
  assert_int_equal(refresh.initialized, 1);

  rc = lc_pouch_namespace_marker_refresh_should_scan(
      NULL, namespace_path, "writer-self.marker", &refresh, 2UL, &should_scan,
      &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(should_scan, 0);

  rc = lc_pouch_namespace_marker_directory_snapshot_read(
      NULL, namespace_path, &before_dir, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_namespace_touch_marker(NULL, namespace_path,
                                       "writer-self.marker", 1UL, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_namespace_marker_refresh_should_scan(
      NULL, namespace_path, "writer-self.marker", &refresh, 2UL, &should_scan,
      &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(should_scan, 0);

  make_peer_marker_path(root, "default", "writer-000000000009.marker",
                        peer_path, sizeof(peer_path));
  write_text_file(peer_path, "writer_pid=9\nsequence=1\n");
  rc = lc_pouch_namespace_marker_directory_snapshot_read(
      NULL, namespace_path, &after_dir, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lc_pouch_namespace_marker_directory_snapshot_changed(
                       &before_dir, &after_dir),
                   1);
  rc = lc_pouch_namespace_marker_refresh_should_scan(
      NULL, namespace_path, "writer-self.marker", &refresh, 2UL, &should_scan,
      &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(should_scan, 1);

  rc = lc_pouch_namespace_marker_refresh_should_scan(
      NULL, namespace_path, "writer-self.marker", &refresh, 2UL, &should_scan,
      &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(should_scan, 0);
  rc = lc_pouch_namespace_marker_refresh_should_scan(
      NULL, namespace_path, "writer-self.marker", &refresh, 2UL, &should_scan,
      &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(should_scan, 0);
  rc = lc_pouch_namespace_marker_refresh_should_scan(
      NULL, namespace_path, "writer-self.marker", &refresh, 2UL, &should_scan,
      &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(should_scan, 1);

  lc_pouch_namespace_marker_refresh_state_cleanup(NULL, &refresh);
  free(namespace_path);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
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

static void test_single_writer_state_read_uses_projection_cache(void **state) {
  lc_pouch *pouch;
  lc_source *source;
  lc_pouch_open_options options;
  lc_pouch_state_write_result write_res;
  lc_pouch_state_read_result read_res;
  lc_error error;
  char root[512];
  char *namespace_path;
  char *segment_leaf;
  char segment_path[1024];
  char buffer[64];
  int written;
  int rc;

  (void)state;
  pouch = NULL;
  source = NULL;
  namespace_path = NULL;
  segment_leaf = NULL;
  memset(&options, 0, sizeof(options));
  memset(&write_res, 0, sizeof(write_res));
  memset(&read_res, 0, sizeof(read_res));
  lc_error_init(&error);
  make_root("single-writer-cache", root, sizeof(root));
  cleanup_root(root);

  options.single_writer = 1;
  rc = lc_pouch_open(root, NULL, &options, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("{\"value\":1}", strlen("{\"value\":1}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "default", "cache/key", source, NULL,
                            &write_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_source_close(source);
  source = NULL;

  rc = lc_pouch_state_read(pouch, "default", "cache/key", &read_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_res.found);
  read_source_to_string(read_res.body, buffer, sizeof(buffer));
  assert_string_equal(buffer, "{\"value\":1}");
  lc_pouch_state_read_result_cleanup(NULL, &read_res);

  namespace_path = lc_pouch_namespace_path(NULL, root, "default");
  segment_leaf = lc_pouch_namespace_segment_leaf(NULL, 1UL);
  assert_non_null(namespace_path);
  assert_non_null(segment_leaf);
  written = snprintf(segment_path, sizeof(segment_path), "%s/segments/%s",
                     namespace_path, segment_leaf);
  assert_true(written > 0 && (size_t)written < sizeof(segment_path));
  assert_int_equal(unlink(segment_path), 0);

  rc = lc_pouch_state_read(pouch, "default", "cache/key", &read_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_res.found);
  assert_int_equal(read_res.version, write_res.version);
  read_source_to_string(read_res.body, buffer, sizeof(buffer));
  assert_string_equal(buffer, "{\"value\":1}");

  lc_pouch_state_read_result_cleanup(NULL, &read_res);
  lc_pouch_state_write_result_cleanup(NULL, &write_res);
  free(segment_leaf);
  free(namespace_path);
  lc_pouch_close(pouch);
  cleanup_root(root);
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

static int pouch_acquire_for_update_handler(
    void *context, lc_acquire_for_update_context *update, lc_error *error) {
  pouch_acquire_for_update_state *state;
  lc_source *source;
  char snapshot[256];
  int rc;

  state = (pouch_acquire_for_update_state *)context;
  source = NULL;
  assert_non_null(update);
  assert_non_null(update->lease);
  if (state->expected_snapshot != NULL) {
    assert_true(update->state.has_state);
    assert_non_null(update->state.reader);
    read_source_to_string(update->state.reader, snapshot, sizeof(snapshot));
    assert_non_null(strstr(snapshot, state->expected_snapshot));
    state->saw_snapshot = 1;
  } else {
    assert_false(update->state.has_state);
    assert_null(update->state.reader);
  }

  rc = lc_source_from_memory(state->replacement, strlen(state->replacement),
                             &source, error);
  assert_int_equal(rc, LC_OK);
  rc = update->lease->update(update->lease, source, NULL, error);
  source->close(source);
  assert_int_equal(rc, LC_OK);
  if (state->observer != NULL) {
    lc_sink *sink;
    lc_get_res get_res;
    lc_error observer_error;
    const void *bytes;
    size_t length;

    sink = NULL;
    bytes = NULL;
    length = 0U;
    memset(&get_res, 0, sizeof(get_res));
    lc_error_init(&observer_error);
    rc = lc_sink_to_memory(&sink, &observer_error);
    assert_int_equal(rc, LC_OK);
    rc = state->observer->get(state->observer, state->key, NULL, sink,
                              &get_res, &observer_error);
    assert_int_equal(rc, LC_OK);
    if (state->expected_visible_during_update != NULL) {
      assert_false(get_res.no_content);
      rc = lc_sink_memory_bytes(sink, &bytes, &length, &observer_error);
      assert_int_equal(rc, LC_OK);
      assert_int_equal(length, strlen(state->expected_visible_during_update));
      assert_memory_equal(bytes, state->expected_visible_during_update,
                          strlen(state->expected_visible_during_update));
    } else {
      assert_true(get_res.no_content);
    }
    sink->close(sink);
    lc_get_res_cleanup(&get_res);
    {
      char staging_key[256];
      const char *stage_id;

      stage_id = update->lease->txn_id != NULL &&
                         update->lease->txn_id[0] != '\0'
                     ? update->lease->txn_id
                     : update->lease->lease_id;
      snprintf(staging_key, sizeof(staging_key), "%s/.staging/%s",
               state->key, stage_id);
      sink = NULL;
      rc = lc_sink_to_memory(&sink, &observer_error);
      assert_int_equal(rc, LC_OK);
      rc = state->observer->get(state->observer, staging_key, NULL, sink,
                                &get_res, &observer_error);
      assert_int_equal(rc, LC_ERR_INVALID);
      assert_string_equal(observer_error.message,
                          "pouch staging keys are reserved for internal state");
      sink->close(sink);
      lc_get_res_cleanup(&get_res);
      lc_error_cleanup(&observer_error);
      lc_error_init(&observer_error);
    }
    state->saw_staged_invisible = 1;
    state->saw_staging_key_rejected = 1;
    lc_error_cleanup(&observer_error);
  }
  if (state->fail) {
    if (error != NULL) {
      error->code = LC_ERR_INVALID;
      error->message = strdup("intentional pouch acquire_for_update failure");
      assert_non_null(error->message);
    }
    return LC_ERR_INVALID;
  }
  return LC_OK;
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

static void test_client_remove_tombstones_state_and_enforces_preconditions(
    void **state) {
  lc_client *client;
  lc_sink *sink;
  lc_update_res update_res;
  lc_remove_op remove_op;
  lc_remove_res remove_res;
  lc_get_res get_res;
  lc_error error;
  char root[512];
  char key[96];
  int rc;

  (void)state;
  client = NULL;
  sink = NULL;
  memset(&update_res, 0, sizeof(update_res));
  memset(&remove_res, 0, sizeof(remove_res));
  memset(&get_res, 0, sizeof(get_res));
  lc_remove_op_init(&remove_op);
  lc_error_init(&error);
  make_root("client-remove", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/remove/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  write_client_state(client, key, "{\"value\":11}", NULL, 0L, 0, &update_res,
                     &error);

  remove_op.lease.key = key;
  remove_op.if_state_etag = "wrong";
  rc = client->remove(client, &remove_op, &remove_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(remove_res.removed, 0);
  lc_error_cleanup(&error);
  lc_error_init(&error);

  lc_remove_op_init(&remove_op);
  memset(&remove_res, 0, sizeof(remove_res));
  remove_op.lease.key = key;
  remove_op.if_state_etag = update_res.new_state_etag;
  remove_op.if_version = update_res.new_version;
  remove_op.has_if_version = 1;
  rc = client->remove(client, &remove_op, &remove_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(remove_res.removed, 1);
  assert_int_equal(remove_res.new_version, 2L);

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get(client, key, NULL, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(get_res.no_content);
  sink->close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);

  lc_remove_op_init(&remove_op);
  memset(&remove_res, 0, sizeof(remove_res));
  remove_op.lease.key = "missing";
  rc = client->remove(client, &remove_op, &remove_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(remove_res.removed, 0);
  assert_int_equal(remove_res.new_version, 0L);

  lc_remove_res_cleanup(&remove_res);
  lc_update_res_cleanup(&update_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_state_mutations_touch_writer_marker(void **state) {
  lc_client *client;
  lc_sink *sink;
  lc_update_res update_res;
  lc_remove_op remove_op;
  lc_remove_res remove_res;
  lc_get_res get_res;
  lc_acquire_req acquire_req;
  lc_error error;
  pouch_acquire_for_update_state handler_state;
  const void *bytes;
  size_t length;
  size_t size1;
  size_t size2;
  size_t size3;
  char root[512];
  char key[96];
  char marker_path[1024];
  int rc;

  (void)state;
  client = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  size1 = 0U;
  size2 = 0U;
  size3 = 0U;
  memset(&update_res, 0, sizeof(update_res));
  memset(&remove_res, 0, sizeof(remove_res));
  memset(&get_res, 0, sizeof(get_res));
  lc_remove_op_init(&remove_op);
  lc_acquire_req_init(&acquire_req);
  lc_error_init(&error);
  make_root("state-marker", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/marker/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  write_client_state(client, key, "{\"value\":1}", NULL, 0L, 0, &update_res,
                     &error);
  find_single_marker_path(root, "default", marker_path, sizeof(marker_path));
  assert_int_equal(read_marker_sequence(marker_path, &size1), 1UL);
  lc_update_res_cleanup(&update_res);

  memset(&update_res, 0, sizeof(update_res));
  write_client_state(client, key, "{\"value\":2}", NULL, 0L, 0, &update_res,
                     &error);
  assert_int_equal(read_marker_sequence(marker_path, &size2), 2UL);
  assert_true(size1 != size2);
  lc_update_res_cleanup(&update_res);

  remove_op.lease.key = key;
  rc = client->remove(client, &remove_op, &remove_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(remove_res.removed, 1);
  assert_int_equal(read_marker_sequence(marker_path, &size3), 3UL);
  assert_true(size2 != size3);
  lc_remove_res_cleanup(&remove_res);

  memset(&handler_state, 0, sizeof(handler_state));
  handler_state.replacement = "{\"value\":4}";
  handler_state.observer = client;
  handler_state.key = key;
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire_for_update(client, &acquire_req,
                                  pouch_acquire_for_update_handler,
                                  &handler_state, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(handler_state.saw_staged_invisible, 1);
  assert_int_equal(read_marker_sequence(marker_path, NULL), 5UL);

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get(client, key, NULL, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, strlen("{\"value\":4}"));
  assert_memory_equal(bytes, "{\"value\":4}", strlen("{\"value\":4}"));
  sink->close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);

  lc_error_cleanup(&error);
  lc_error_init(&error);
  memset(&handler_state, 0, sizeof(handler_state));
  handler_state.expected_snapshot = "\"value\":4";
  handler_state.expected_visible_during_update = "{\"value\":4}";
  handler_state.replacement = "{\"value\":5}";
  handler_state.observer = client;
  handler_state.key = key;
  handler_state.fail = 1;
  rc = client->acquire_for_update(client, &acquire_req,
                                  pouch_acquire_for_update_handler,
                                  &handler_state, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(handler_state.saw_staged_invisible, 1);
  assert_int_equal(read_marker_sequence(marker_path, NULL), 7UL);

  lc_error_cleanup(&error);
  lc_error_init(&error);
  memset(&get_res, 0, sizeof(get_res));
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get(client, key, NULL, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, strlen("{\"value\":4}"));
  assert_memory_equal(bytes, "{\"value\":4}", strlen("{\"value\":4}"));
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

static void test_lease_remove_tombstones_state_and_refreshes_view(
    void **state) {
  lc_client *client;
  lc_lease *lease;
  lc_source *source;
  lc_sink *sink;
  lc_acquire_req acquire_req;
  lc_get_res get_res;
  lc_error error;
  char root[512];
  char key[96];
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  source = NULL;
  sink = NULL;
  memset(&get_res, 0, sizeof(get_res));
  lc_acquire_req_init(&acquire_req);
  lc_error_init(&error);
  make_root("lease-remove", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/lease-remove/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("{\"value\":12}", strlen("{\"value\":12}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lease->update(lease, source, NULL, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 1L);
  assert_non_null(lease->state_etag);

  rc = lease->remove(lease, NULL, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 0L);
  assert_null(lease->state_etag);

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get(client, key, NULL, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(get_res.no_content);
  sink->close(sink);

  lc_get_res_cleanup(&get_res);
  lease->close(lease);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_acquire_for_update_success_and_rollback(void **state) {
  lc_client *client;
  lc_sink *sink;
  lc_update_res update_res;
  lc_get_res get_res;
  lc_acquire_req acquire_req;
  lc_error error;
  pouch_acquire_for_update_state handler_state;
  const void *bytes;
  size_t length;
  char root[512];
  char key[96];
  int rc;

  (void)state;
  client = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  memset(&update_res, 0, sizeof(update_res));
  memset(&get_res, 0, sizeof(get_res));
  lc_acquire_req_init(&acquire_req);
  lc_error_init(&error);
  make_root("acquire-for-update", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/afu/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  write_client_state(client, key, "{\"value\":1}", NULL, 0L, 0, &update_res,
                     &error);
  lc_update_res_cleanup(&update_res);

  memset(&handler_state, 0, sizeof(handler_state));
  handler_state.expected_snapshot = "\"value\":1";
  handler_state.expected_visible_during_update = "{\"value\":1}";
  handler_state.replacement = "{\"value\":2}";
  handler_state.observer = client;
  handler_state.key = key;
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire_for_update(client, &acquire_req,
                                  pouch_acquire_for_update_handler,
                                  &handler_state, &error);
  if (rc != LC_OK) {
    fail_msg("acquire_for_update failed: %s",
             error.message != NULL ? error.message : "(no message)");
  }
  assert_int_equal(rc, LC_OK);
  assert_int_equal(handler_state.saw_snapshot, 1);
  assert_int_equal(handler_state.saw_staged_invisible, 1);
  assert_int_equal(handler_state.saw_staging_key_rejected, 1);

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get(client, key, NULL, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, strlen("{\"value\":2}"));
  assert_memory_equal(bytes, "{\"value\":2}", strlen("{\"value\":2}"));
  sink->close(sink);
  sink = NULL;
  lc_get_res_cleanup(&get_res);

  lc_error_cleanup(&error);
  lc_error_init(&error);
  memset(&handler_state, 0, sizeof(handler_state));
  handler_state.expected_snapshot = "\"value\":2";
  handler_state.expected_visible_during_update = "{\"value\":2}";
  handler_state.replacement = "{\"value\":3}";
  handler_state.observer = client;
  handler_state.key = key;
  handler_state.fail = 1;
  rc = client->acquire_for_update(client, &acquire_req,
                                  pouch_acquire_for_update_handler,
                                  &handler_state, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "intentional pouch acquire_for_update failure");
  assert_int_equal(handler_state.saw_staged_invisible, 1);
  assert_int_equal(handler_state.saw_staging_key_rejected, 1);

  lc_error_cleanup(&error);
  lc_error_init(&error);
  memset(&get_res, 0, sizeof(get_res));
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get(client, key, NULL, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, strlen("{\"value\":2}"));
  assert_memory_equal(bytes, "{\"value\":2}", strlen("{\"value\":2}"));
  sink->close(sink);

  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_acquire_for_update_rollback_removes_new_state(void **state) {
  lc_client *client;
  lc_sink *sink;
  lc_get_res get_res;
  lc_acquire_req acquire_req;
  lc_error error;
  pouch_acquire_for_update_state handler_state;
  char root[512];
  char key[96];
  int rc;

  (void)state;
  client = NULL;
  sink = NULL;
  memset(&get_res, 0, sizeof(get_res));
  lc_acquire_req_init(&acquire_req);
  lc_error_init(&error);
  make_root("acquire-for-update-new-rollback", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/afu-new/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  memset(&handler_state, 0, sizeof(handler_state));
  handler_state.replacement = "{\"value\":9}";
  handler_state.observer = client;
  handler_state.key = key;
  handler_state.fail = 1;
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire_for_update(client, &acquire_req,
                                  pouch_acquire_for_update_handler,
                                  &handler_state, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "intentional pouch acquire_for_update failure");
  assert_int_equal(handler_state.saw_staged_invisible, 1);
  assert_int_equal(handler_state.saw_staging_key_rejected, 1);

  lc_error_cleanup(&error);
  lc_error_init(&error);
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->get(client, key, NULL, sink, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(get_res.no_content);
  sink->close(sink);

  lc_get_res_cleanup(&get_res);
  lc_client_close(client);
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
      cmocka_unit_test(
          test_client_remove_tombstones_state_and_enforces_preconditions),
      cmocka_unit_test(test_state_mutations_touch_writer_marker),
      cmocka_unit_test(test_marker_snapshots_detect_peer_changes),
      cmocka_unit_test(
          test_marker_snapshots_treat_same_process_handles_as_peers),
      cmocka_unit_test(
          test_marker_refresh_uses_directory_fast_path_and_force),
      cmocka_unit_test(test_single_writer_state_read_uses_projection_cache),
      cmocka_unit_test(test_lease_bound_state_update_get_and_release),
      cmocka_unit_test(test_lease_remove_tombstones_state_and_refreshes_view),
      cmocka_unit_test(test_acquire_for_update_success_and_rollback),
      cmocka_unit_test(test_acquire_for_update_rollback_removes_new_state),
  };

  return cmocka_run_group_tests(tests, setup_pouch_unit_group,
                                teardown_pouch_unit_group);
}

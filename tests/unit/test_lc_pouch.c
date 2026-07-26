#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

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

typedef struct pouch_query_key_capture {
  char keys[8][128];
  char current[128];
  size_t current_len;
  size_t count;
} pouch_query_key_capture;

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

static int pouch_query_key_begin(void *context, lc_error *error) {
  pouch_query_key_capture *capture;

  (void)error;
  capture = (pouch_query_key_capture *)context;
  capture->current_len = 0U;
  capture->current[0] = '\0';
  return 1;
}

static int pouch_query_key_chunk(void *context, const char *bytes, size_t len,
                                 lc_error *error) {
  pouch_query_key_capture *capture;

  (void)error;
  capture = (pouch_query_key_capture *)context;
  if (capture->current_len + len >= sizeof(capture->current)) {
    return 0;
  }
  memcpy(capture->current + capture->current_len, bytes, len);
  capture->current_len += len;
  capture->current[capture->current_len] = '\0';
  return 1;
}

static int pouch_query_key_end(void *context, lc_error *error) {
  pouch_query_key_capture *capture;

  (void)error;
  capture = (pouch_query_key_capture *)context;
  if (capture->count >= sizeof(capture->keys) / sizeof(capture->keys[0])) {
    return 0;
  }
  snprintf(capture->keys[capture->count], sizeof(capture->keys[capture->count]),
           "%s", capture->current);
  ++capture->count;
  return 1;
}

static int pouch_query_capture_has(const pouch_query_key_capture *capture,
                                   const char *key) {
  size_t i;

  for (i = 0U; i < capture->count; ++i) {
    if (strcmp(capture->keys[i], key) == 0) {
      return 1;
    }
  }
  return 0;
}

static int bytes_contain_text(const void *bytes, size_t length,
                              const char *needle) {
  const unsigned char *haystack;
  size_t needle_len;
  size_t offset;

  if (bytes == NULL || needle == NULL) {
    return 0;
  }
  needle_len = strlen(needle);
  if (needle_len == 0U || needle_len > length) {
    return 0;
  }
  haystack = (const unsigned char *)bytes;
  for (offset = 0U; offset + needle_len <= length; ++offset) {
    if (memcmp(haystack + offset, needle, needle_len) == 0) {
      return 1;
    }
  }
  return 0;
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

static void append_text_file(const char *path, const char *text) {
  FILE *fp;

  fp = fopen(path, "ab");
  assert_non_null(fp);
  assert_int_equal(fputs(text, fp) < 0 ? -1 : 0, 0);
  assert_int_equal(fclose(fp), 0);
}

static void hex_encode_string(const char *value, char *out, size_t out_size) {
  static const char hex[] = "0123456789abcdef";
  const unsigned char *src;
  size_t offset;

  src = (const unsigned char *)value;
  offset = 0U;
  while (*src != '\0') {
    assert_true(offset + 2U < out_size);
    out[offset++] = hex[*src >> 4];
    out[offset++] = hex[*src & 0x0fU];
    ++src;
  }
  assert_true(offset < out_size);
  out[offset] = '\0';
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

  rc = lc_pouch_namespace_marker_directory_snapshot_read(
      NULL, namespace_path, &before_dir, &error);
  assert_int_equal(rc, LC_OK);
  write_text_file(peer_path, "writer_pid=9\nsequence=2\npad=longer\n");
  rc = lc_pouch_namespace_marker_directory_snapshot_read(
      NULL, namespace_path, &after_dir, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lc_pouch_namespace_marker_directory_snapshot_changed(
                       &before_dir, &after_dir),
                   0);
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

static void
test_shared_state_projection_cache_refreshes_peer_markers(void **state) {
  lc_pouch *writer;
  lc_pouch *reader;
  lc_source *source;
  lc_pouch_state_write_result write_res;
  lc_pouch_state_read_result read_res;
  lc_error error;
  char root[512];
  char buffer[64];
  int rc;

  (void)state;
  writer = NULL;
  reader = NULL;
  source = NULL;
  memset(&write_res, 0, sizeof(write_res));
  memset(&read_res, 0, sizeof(read_res));
  lc_error_init(&error);
  make_root("shared-cache-markers", root, sizeof(root));
  cleanup_root(root);

  rc = lc_pouch_open(root, NULL, NULL, &writer, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_open(root, NULL, NULL, &reader, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory("{\"value\":1}", strlen("{\"value\":1}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(writer, "default", "cache/key", source, NULL,
                            &write_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_source_close(source);
  source = NULL;
  lc_pouch_state_write_result_cleanup(NULL, &write_res);

  rc = lc_pouch_state_read(reader, "default", "cache/key", &read_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_res.found);
  read_source_to_string(read_res.body, buffer, sizeof(buffer));
  assert_string_equal(buffer, "{\"value\":1}");
  lc_pouch_state_read_result_cleanup(NULL, &read_res);

  rc = lc_source_from_memory("{\"value\":2}", strlen("{\"value\":2}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(writer, "default", "cache/key", source, NULL,
                            &write_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_source_close(source);
  source = NULL;
  lc_pouch_state_write_result_cleanup(NULL, &write_res);

  rc = lc_pouch_state_read(reader, "default", "cache/key", &read_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_res.found);
  read_source_to_string(read_res.body, buffer, sizeof(buffer));
  assert_string_equal(buffer, "{\"value\":2}");
  lc_pouch_state_read_result_cleanup(NULL, &read_res);

  rc = lc_source_from_memory("{\"value\":3}", strlen("{\"value\":3}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(reader, "default", "cache/key", source, NULL,
                            &write_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_source_close(source);
  source = NULL;
  lc_pouch_state_write_result_cleanup(NULL, &write_res);

  rc = lc_pouch_state_read(reader, "default", "cache/key", &read_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_res.found);
  read_source_to_string(read_res.body, buffer, sizeof(buffer));
  assert_string_equal(buffer, "{\"value\":3}");

  lc_pouch_state_read_result_cleanup(NULL, &read_res);
  if (source != NULL) {
    lc_source_close(source);
  }
  lc_pouch_close(reader);
  lc_pouch_close(writer);
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

static void test_state_scheduled_compaction_installs_snapshot(void **state) {
  lc_pouch *pouch;
  lc_source *body;
  lc_pouch_open_options open_options;
  lc_pouch_state_write_result write_a;
  lc_pouch_state_write_result delete_a;
  lc_pouch_state_write_result write_b;
  lc_pouch_state_read_result read_result;
  lc_error error;
  char root[512];
  char bytes[64];
  char *namespace_path;
  char path[1024];
  int written;
  int rc;

  (void)state;
  lc_error_init(&error);
  memset(&open_options, 0, sizeof(open_options));
  memset(&write_a, 0, sizeof(write_a));
  memset(&delete_a, 0, sizeof(delete_a));
  memset(&write_b, 0, sizeof(write_b));
  memset(&read_result, 0, sizeof(read_result));
  make_root("state-compact", root, sizeof(root));
  cleanup_root(root);

  open_options.segment_target_bytes = 1UL;
  open_options.compaction_min_segment_count = 2UL;
  open_options.compaction_min_reclaimable_bytes = 1UL;
  open_options.background_compaction_enabled = 1;
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory("one", strlen("one"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/a", body, NULL,
                            &write_a, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);

  rc = lc_pouch_state_delete(pouch, "team/alpha", "state/a", NULL, &delete_a,
                             &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(delete_a.etag, "pouch-state-2");

  namespace_path = lc_pouch_namespace_path(NULL, root, "team/alpha");
  assert_non_null(namespace_path);
  assert_path_file(namespace_path,
                   "snapshots/snapshot-00000000000000000002.log");
  assert_path_file_contains(namespace_path, "manifest",
                            "snapshot=snapshot-00000000000000000002.log");
  assert_path_file_contains(namespace_path, "manifest",
                            "active_segment=seg-00000000000000000003.log");
  written = snprintf(path, sizeof(path), "%s/segments/%s", namespace_path,
                     "seg-00000000000000000001.log");
  assert_true(written > 0 && (size_t)written < sizeof(path));
  assert_false(path_is_file(path));
  written = snprintf(path, sizeof(path), "%s/segments/%s", namespace_path,
                     "seg-00000000000000000002.log");
  assert_true(written > 0 && (size_t)written < sizeof(path));
  assert_false(path_is_file(path));

  rc = lc_source_from_memory("two", strlen("two"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/b", body, NULL,
                            &write_b, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);

  lc_pouch_close(pouch);
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, "team/alpha", "state/a", &read_result,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_false(read_result.found);
  lc_pouch_state_read_result_cleanup(NULL, &read_result);

  rc = lc_pouch_state_read(pouch, "team/alpha", "state/b", &read_result,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_result.found);
  assert_string_equal(read_result.etag, "pouch-state-3");
  read_source_to_string(read_result.body, bytes, sizeof(bytes));
  assert_string_equal(bytes, "two");

  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_state_write_result_cleanup(NULL, &write_a);
  lc_pouch_state_write_result_cleanup(NULL, &delete_a);
  lc_pouch_state_write_result_cleanup(NULL, &write_b);
  free(namespace_path);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_state_metadata_survives_snapshot_compaction(void **state) {
  lc_pouch *pouch;
  lc_source *body;
  lc_pouch_open_options open_options;
  lc_pouch_state_write_result write_result;
  lc_pouch_state_write_result metadata_result;
  lc_pouch_state_read_result read_result;
  lc_pouch_state_write_options metadata_options;
  lc_error error;
  char root[512];
  char bytes[64];
  char *namespace_path;
  int rc;

  (void)state;
  lc_error_init(&error);
  memset(&open_options, 0, sizeof(open_options));
  memset(&write_result, 0, sizeof(write_result));
  memset(&metadata_result, 0, sizeof(metadata_result));
  memset(&read_result, 0, sizeof(read_result));
  memset(&metadata_options, 0, sizeof(metadata_options));
  make_root("state-metadata-compact", root, sizeof(root));
  cleanup_root(root);

  open_options.segment_target_bytes = 1UL;
  open_options.compaction_min_segment_count = 2UL;
  open_options.compaction_min_reclaimable_bytes = 1UL;
  open_options.background_compaction_enabled = 1;
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory("visible", strlen("visible"), &body, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/meta", body, NULL,
                            &write_result, &error);
  assert_int_equal(rc, LC_OK);
  body->close(body);

  metadata_options.has_query_hidden = 1;
  metadata_options.query_hidden = 1;
  rc = lc_pouch_state_update_metadata(pouch, "team/alpha", "state/meta",
                                      &metadata_options, &metadata_result,
                                      &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(metadata_result.version, 2UL);
  assert_string_equal(metadata_result.etag, "pouch-state-1");
  assert_true(metadata_result.has_query_hidden);
  assert_true(metadata_result.query_hidden);

  namespace_path = lc_pouch_namespace_path(NULL, root, "team/alpha");
  assert_non_null(namespace_path);
  assert_path_file(namespace_path,
                   "snapshots/snapshot-00000000000000000002.log");
  assert_path_file_contains(namespace_path,
                            "snapshots/snapshot-00000000000000000002.log",
                            " 1 1");

  lc_pouch_close(pouch);
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, "team/alpha", "state/meta", &read_result,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_result.found);
  assert_int_equal(read_result.version, 2UL);
  assert_string_equal(read_result.etag, "pouch-state-1");
  assert_true(read_result.has_query_hidden);
  assert_true(read_result.query_hidden);
  read_source_to_string(read_result.body, bytes, sizeof(bytes));
  assert_string_equal(bytes, "visible");

  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  lc_pouch_state_write_result_cleanup(NULL, &metadata_result);
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

static void test_staged_state_writes_durable_decision_records(void **state) {
  lc_pouch *pouch;
  lc_source *source;
  lc_pouch_state_write_result committed;
  lc_pouch_state_write_result staged_commit;
  lc_pouch_state_write_result promoted;
  lc_pouch_state_write_result staged_discard;
  lc_error error;
  char root[512];
  char *namespace_path;
  char *segment_leaf;
  char segment_path[1024];
  char committed_hex[64];
  char discarded_hex[64];
  int discarded;
  int written;
  int rc;

  (void)state;
  pouch = NULL;
  source = NULL;
  namespace_path = NULL;
  segment_leaf = NULL;
  memset(&committed, 0, sizeof(committed));
  memset(&staged_commit, 0, sizeof(staged_commit));
  memset(&promoted, 0, sizeof(promoted));
  memset(&staged_discard, 0, sizeof(staged_discard));
  lc_error_init(&error);
  make_root("state-decision-records", root, sizeof(root));
  cleanup_root(root);

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("{\"value\":1}", strlen("{\"value\":1}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "team/alpha", "state/decision", source,
                            NULL, &committed, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory("{\"value\":2}", strlen("{\"value\":2}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_stage_write(pouch, "team/alpha", "state/decision",
                                  "commit-txn", source, NULL, &staged_commit,
                                  &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_promote_staged(pouch, "team/alpha", "state/decision",
                                     "commit-txn", committed.etag, &promoted,
                                     &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory("{\"value\":3}", strlen("{\"value\":3}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_stage_write(pouch, "team/alpha", "state/discard",
                                  "discard-txn", source, NULL,
                                  &staged_discard, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  discarded = 0;
  rc = lc_pouch_state_discard_staged(pouch, "team/alpha", "state/discard",
                                     "discard-txn", &discarded, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(discarded, 1);

  namespace_path = lc_pouch_namespace_path(NULL, root, "team/alpha");
  segment_leaf = lc_pouch_namespace_segment_leaf(NULL, 1UL);
  assert_non_null(namespace_path);
  assert_non_null(segment_leaf);
  written = snprintf(segment_path, sizeof(segment_path), "%s/segments/%s",
                     namespace_path, segment_leaf);
  assert_true(written > 0 && (size_t)written < sizeof(segment_path));
  hex_encode_string("committed", committed_hex, sizeof(committed_hex));
  hex_encode_string("discarded", discarded_hex, sizeof(discarded_hex));
  assert_file_contains(segment_path, "T ");
  assert_file_contains(segment_path, committed_hex);
  assert_file_contains(segment_path, discarded_hex);

  free(segment_leaf);
  free(namespace_path);
  lc_pouch_state_write_result_cleanup(NULL, &staged_discard);
  lc_pouch_state_write_result_cleanup(NULL, &promoted);
  lc_pouch_state_write_result_cleanup(NULL, &staged_commit);
  lc_pouch_state_write_result_cleanup(NULL, &committed);
  lc_pouch_close(pouch);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_staged_decision_recovery_tombstones_interrupted_discard(
    void **state) {
  lc_pouch *pouch;
  lc_source *source;
  lc_pouch_state_write_result staged;
  lc_pouch_state_read_result read_result;
  lc_error error;
  char root[512];
  char staged_key[256];
  char staged_key_hex[512];
  char etag_hex[128];
  char decision_hex[64];
  char decision_record[1024];
  char *namespace_path;
  char *segment_leaf;
  char segment_path[1024];
  int written;
  int rc;

  (void)state;
  pouch = NULL;
  source = NULL;
  namespace_path = NULL;
  segment_leaf = NULL;
  memset(&staged, 0, sizeof(staged));
  memset(&read_result, 0, sizeof(read_result));
  lc_error_init(&error);
  make_root("state-decision-recovery", root, sizeof(root));
  cleanup_root(root);

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("{\"value\":9}", strlen("{\"value\":9}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_stage_write(pouch, "team/alpha", "state/recover",
                                  "txn-recover", source, NULL, &staged,
                                  &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_close(pouch);
  pouch = NULL;

  namespace_path = lc_pouch_namespace_path(NULL, root, "team/alpha");
  segment_leaf = lc_pouch_namespace_segment_leaf(NULL, 1UL);
  assert_non_null(namespace_path);
  assert_non_null(segment_leaf);
  written = snprintf(segment_path, sizeof(segment_path), "%s/segments/%s",
                     namespace_path, segment_leaf);
  assert_true(written > 0 && (size_t)written < sizeof(segment_path));
  written = snprintf(staged_key, sizeof(staged_key),
                     "state/recover/.staging/txn-recover");
  assert_true(written > 0 && (size_t)written < sizeof(staged_key));
  hex_encode_string(staged_key, staged_key_hex, sizeof(staged_key_hex));
  hex_encode_string(staged.etag, etag_hex, sizeof(etag_hex));
  hex_encode_string("discarded", decision_hex, sizeof(decision_hex));
  written = snprintf(decision_record, sizeof(decision_record), "T 2 %s %s %s\n",
                     staged_key_hex, etag_hex, decision_hex);
  assert_true(written > 0 && (size_t)written < sizeof(decision_record));
  append_text_file(segment_path, decision_record);

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, "team/alpha", staged_key, &read_result,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(read_result.found, 0);
  assert_file_contains(segment_path, "D 3 ");
  assert_file_contains(segment_path, staged_key_hex);

  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  free(segment_leaf);
  free(namespace_path);
  lc_pouch_state_write_result_cleanup(NULL, &staged);
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

static void test_lease_save_streams_mapped_json_and_replays_after_reopen(
    void **state) {
  lc_client *client;
  lc_client *reader;
  lc_lease *lease;
  lc_acquire_req acquire_req;
  lc_release_req release_req;
  lc_get_res get_res;
  lc_error error;
  pouch_value_doc saved;
  pouch_value_doc loaded;
  char root[512];
  char key[96];
  int rc;

  (void)state;
  client = NULL;
  reader = NULL;
  lease = NULL;
  memset(&get_res, 0, sizeof(get_res));
  memset(&saved, 0, sizeof(saved));
  memset(&loaded, 0, sizeof(loaded));
  lc_acquire_req_init(&acquire_req);
  lc_release_req_init(&release_req);
  lc_error_init(&error);
  make_root("lease-save", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/lease-save/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);

  saved.value = 42;
  rc = lease->save(lease, &pouch_value_map, &saved, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 1L);
  assert_string_equal(lease->state_etag, "pouch-state-1");

  rc = lease->load(lease, &pouch_value_map, &loaded, NULL, &get_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  assert_int_equal(loaded.value, 42);
  assert_string_equal(get_res.content_type, "application/json");
  assert_string_equal(get_res.etag, "pouch-state-1");
  lc_get_res_cleanup(&get_res);

  rc = lease->release(lease, &release_req, &error);
  assert_int_equal(rc, LC_OK);
  lease = NULL;
  lc_client_close(client);
  client = NULL;

  memset(&loaded, 0, sizeof(loaded));
  memset(&get_res, 0, sizeof(get_res));
  open_pouch_client(root, &reader, &error);
  rc = reader->load(reader, key, &pouch_value_map, &loaded, NULL, &get_res,
                    &error);
  assert_int_equal(rc, LC_OK);
  assert_false(get_res.no_content);
  assert_int_equal(loaded.value, 42);
  assert_string_equal(get_res.etag, "pouch-state-1");

  lc_get_res_cleanup(&get_res);
  lc_client_close(reader);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_lease_keepalive_and_release_use_local_lifecycle(
    void **state) {
  lc_client *client;
  lc_lease *lease;
  lc_source *source;
  lc_acquire_req acquire_req;
  lc_keepalive_req keepalive_req;
  lc_keepalive_op keepalive_op;
  lc_keepalive_res keepalive_res;
  lc_release_op release_op;
  lc_release_res release_res;
  lc_error error;
  char root[512];
  char key[96];
  long before;
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  source = NULL;
  memset(&keepalive_res, 0, sizeof(keepalive_res));
  memset(&release_res, 0, sizeof(release_res));
  lc_acquire_req_init(&acquire_req);
  lc_keepalive_req_init(&keepalive_req);
  lc_keepalive_op_init(&keepalive_op);
  lc_release_op_init(&release_op);
  lc_error_init(&error);
  make_root("lease-keepalive", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/lease-keepalive/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 30L;
  before = (long)time(NULL);
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(lease->lease_expires_at_unix >= before + 30L);

  rc = lc_source_from_memory("{\"value\":9}", strlen("{\"value\":9}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lease->update(lease, source, NULL, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 1L);
  assert_string_equal(lease->state_etag, "pouch-state-1");

  keepalive_req.ttl_seconds = 45L;
  before = (long)time(NULL);
  rc = lease->keepalive(lease, &keepalive_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(lease->lease_expires_at_unix >= before + 45L);
  assert_int_equal(lease->version, 1L);
  assert_string_equal(lease->state_etag, "pouch-state-1");

  keepalive_op.lease.namespace_name = lease->namespace_name;
  keepalive_op.lease.key = lease->key;
  keepalive_op.lease.lease_id = lease->lease_id;
  keepalive_op.lease.txn_id = lease->txn_id;
  keepalive_op.lease.fencing_token = lease->fencing_token;
  keepalive_op.ttl_seconds = 60L;
  before = (long)time(NULL);
  rc = client->keepalive(client, &keepalive_op, &keepalive_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(keepalive_res.lease_expires_at_unix >= before + 60L);
  assert_int_equal(keepalive_res.version, 1L);
  assert_string_equal(keepalive_res.state_etag, "pouch-state-1");
  lc_keepalive_res_cleanup(&keepalive_res);

  release_op.lease.namespace_name = lease->namespace_name;
  release_op.lease.key = lease->key;
  release_op.lease.lease_id = lease->lease_id;
  release_op.lease.txn_id = lease->txn_id;
  release_op.lease.fencing_token = lease->fencing_token;
  rc = client->release(client, &release_op, &release_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(release_res.released, 1);
  lc_release_res_cleanup(&release_res);

  rc = lease->release(lease, NULL, &error);
  assert_int_equal(rc, LC_OK);
  lease = NULL;

  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_acquire_rejects_non_positive_ttl(void **state) {
  lc_client *client;
  lc_lease *lease;
  lc_acquire_req acquire_req;
  lc_error error;
  char root[512];
  char key[96];
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  lc_acquire_req_init(&acquire_req);
  lc_error_init(&error);
  make_root("lease-invalid-ttl", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/lease-invalid-ttl/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 0L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(lease);

  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_lease_metadata_persists_query_hidden(void **state) {
  lc_client *client;
  lc_client *reader;
  lc_lease *lease;
  lc_source *source;
  lc_acquire_req acquire_req;
  lc_metadata_req metadata_req;
  lc_describe_req describe_req;
  lc_describe_res describe_res;
  lc_error error;
  char root[512];
  char key[96];
  int rc;

  (void)state;
  client = NULL;
  reader = NULL;
  lease = NULL;
  source = NULL;
  memset(&describe_res, 0, sizeof(describe_res));
  lc_acquire_req_init(&acquire_req);
  lc_metadata_req_init(&metadata_req);
  lc_describe_req_init(&describe_req);
  lc_error_init(&error);
  make_root("lease-metadata", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/lease-metadata/%ld", (long)getpid());

  open_pouch_client(root, &client, &error);
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("{\"value\":15}", strlen("{\"value\":15}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lease->update(lease, source, NULL, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 1L);
  assert_false(lease->has_query_hidden);

  metadata_req.has_query_hidden = 1;
  metadata_req.query_hidden = 1;
  rc = lease->metadata(lease, &metadata_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 2L);
  assert_string_equal(lease->state_etag, "pouch-state-1");
  assert_true(lease->has_query_hidden);
  assert_true(lease->query_hidden);

  rc = lc_source_from_memory("{\"value\":16}", strlen("{\"value\":16}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lease->update(lease, source, NULL, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 3L);
  assert_string_equal(lease->state_etag, "pouch-state-3");
  assert_true(lease->has_query_hidden);
  assert_true(lease->query_hidden);

  rc = lease->release(lease, NULL, &error);
  assert_int_equal(rc, LC_OK);
  lease = NULL;
  lc_client_close(client);
  client = NULL;

  open_pouch_client(root, &reader, &error);
  describe_req.key = key;
  rc = reader->describe(reader, &describe_req, &describe_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(describe_res.version, 3L);
  assert_string_equal(describe_res.state_etag, "pouch-state-3");
  assert_true(describe_res.has_query_hidden);
  assert_true(describe_res.query_hidden);
  lc_describe_res_cleanup(&describe_res);

  lc_acquire_req_init(&acquire_req);
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 30L;
  rc = reader->acquire(reader, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 3L);
  assert_true(lease->has_query_hidden);
  assert_true(lease->query_hidden);

  lc_metadata_req_init(&metadata_req);
  metadata_req.has_query_hidden = 1;
  metadata_req.query_hidden = 0;
  rc = lease->metadata(lease, &metadata_req, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(lease->version, 4L);
  assert_true(lease->has_query_hidden);
  assert_false(lease->query_hidden);

  rc = lease->release(lease, NULL, &error);
  assert_int_equal(rc, LC_OK);
  lease = NULL;
  lc_client_close(reader);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_client_metadata_enforces_version_precondition(void **state) {
  lc_client *client;
  lc_lease *lease;
  lc_source *source;
  lc_acquire_req acquire_req;
  lc_metadata_op metadata_op;
  lc_metadata_res metadata_res;
  lc_error error;
  char root[512];
  char key[96];
  int rc;

  (void)state;
  client = NULL;
  lease = NULL;
  source = NULL;
  memset(&metadata_res, 0, sizeof(metadata_res));
  lc_acquire_req_init(&acquire_req);
  lc_metadata_op_init(&metadata_op);
  lc_error_init(&error);
  make_root("metadata-precondition", root, sizeof(root));
  cleanup_root(root);
  snprintf(key, sizeof(key), "state/metadata-precondition/%ld",
           (long)getpid());

  open_pouch_client(root, &client, &error);
  acquire_req.key = key;
  acquire_req.owner = "lc-unit-pouch";
  acquire_req.ttl_seconds = 30L;
  rc = client->acquire(client, &acquire_req, &lease, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("{\"value\":21}", strlen("{\"value\":21}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lease->update(lease, source, NULL, &error);
  source->close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  metadata_op.lease.namespace_name = lease->namespace_name;
  metadata_op.lease.key = lease->key;
  metadata_op.lease.lease_id = lease->lease_id;
  metadata_op.lease.txn_id = lease->txn_id;
  metadata_op.lease.fencing_token = lease->fencing_token;
  metadata_op.has_query_hidden = 1;
  metadata_op.query_hidden = 1;
  metadata_op.has_if_version = 1;
  metadata_op.if_version = 99L;
  rc = client->metadata(client, &metadata_op, &metadata_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_false(metadata_res.has_query_hidden);
  lc_metadata_res_cleanup(&metadata_res);

  rc = lease->release(lease, NULL, &error);
  assert_int_equal(rc, LC_OK);
  lease = NULL;
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_query_keys_scan_uses_liblql_and_query_hidden(void **state) {
  static const char selector[] =
      "{\"eq\":{\"field\":\"/category\",\"value\":\"planning\"}}";
  lc_client *client;
  lc_pouch *pouch;
  lc_source *source;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_query_key_handler handler;
  pouch_query_key_capture first_page;
  pouch_query_key_capture second_page;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result write_result;
  lc_error error;
  char root[512];
  char cursor[64];
  int rc;

  (void)state;
  client = NULL;
  pouch = NULL;
  source = NULL;
  memset(&query_res, 0, sizeof(query_res));
  memset(&handler, 0, sizeof(handler));
  memset(&first_page, 0, sizeof(first_page));
  memset(&second_page, 0, sizeof(second_page));
  memset(&options, 0, sizeof(options));
  memset(&write_result, 0, sizeof(write_result));
  lc_query_req_init(&query_req);
  lc_error_init(&error);
  make_root("query-keys-scan", root, sizeof(root));
  cleanup_root(root);
  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory("{\"category\":\"planning\",\"n\":1}",
                             strlen("{\"category\":\"planning\",\"n\":1}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query", "doc/a", source, NULL,
                            &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"category\":\"planning\",\"n\":2}",
                             strlen("{\"category\":\"planning\",\"n\":2}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query", "doc/b", source, NULL,
                            &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"category\":\"finance\",\"n\":3}",
                             strlen("{\"category\":\"finance\",\"n\":3}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query", "doc/c", source, NULL,
                            &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  memset(&options, 0, sizeof(options));
  options.has_query_hidden = 1;
  options.query_hidden = 1;
  rc = lc_source_from_memory("{\"category\":\"planning\",\"n\":4}",
                             strlen("{\"category\":\"planning\",\"n\":4}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query", "doc/hidden", source,
                            &options, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"category\":\"planning\",\"n\":5}",
                             strlen("{\"category\":\"planning\",\"n\":5}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_stage_write(pouch, "docs/query", "doc/staged",
                                  "txn-query-hidden", source, NULL,
                                  &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  open_pouch_client(root, &client, &error);

  handler.begin = pouch_query_key_begin;
  handler.chunk = pouch_query_key_chunk;
  handler.end = pouch_query_key_end;
  query_req.namespace_name = "docs/query";
  query_req.selector_json = selector;
  query_req.limit = 1L;
  rc = client->query_keys(client, &query_req, &handler, &first_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(first_page.count, 1);
  assert_non_null(query_res.cursor);
  assert_string_equal(query_res.return_mode, "keys");
  assert_string_equal(query_res.metadata_json, "{\"engine\":\"scan\"}");
  assert_true(query_res.index_seq > 0UL);

  snprintf(cursor, sizeof(cursor), "%s", query_res.cursor);
  query_req.cursor = cursor;
  lc_query_res_cleanup(&query_res);
  rc = client->query_keys(client, &query_req, &handler, &second_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(second_page.count, 1);
  assert_null(query_res.cursor);
  assert_true(pouch_query_capture_has(&first_page, "doc/a") ||
              pouch_query_capture_has(&second_page, "doc/a"));
  assert_true(pouch_query_capture_has(&first_page, "doc/b") ||
              pouch_query_capture_has(&second_page, "doc/b"));
  assert_false(pouch_query_capture_has(&first_page, "doc/hidden"));
  assert_false(pouch_query_capture_has(&second_page, "doc/hidden"));
  assert_false(pouch_query_capture_has(&first_page, "doc/staged"));
  assert_false(pouch_query_capture_has(&second_page, "doc/staged"));
  assert_false(pouch_query_capture_has(&first_page, "doc/c"));
  assert_false(pouch_query_capture_has(&second_page, "doc/c"));

  lc_query_res_cleanup(&query_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_query_keys_index_summary_uses_sidecar_rows(void **state) {
  static const char selector[] =
      "{\"eq\":{\"field\":\"/category\",\"value\":\"planning\"}}";
  lc_client *client;
  lc_pouch *pouch;
  lc_source *source;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_query_key_handler handler;
  pouch_query_key_capture first_page;
  pouch_query_key_capture second_page;
  pouch_query_key_capture indexed_page;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result write_result;
  lc_pouch_state_write_result delete_result;
  lc_error error;
  char *namespace_path;
  char root[512];
  char cursor[64];
  int rc;

  (void)state;
  client = NULL;
  pouch = NULL;
  source = NULL;
  namespace_path = NULL;
  memset(&query_res, 0, sizeof(query_res));
  memset(&handler, 0, sizeof(handler));
  memset(&first_page, 0, sizeof(first_page));
  memset(&second_page, 0, sizeof(second_page));
  memset(&indexed_page, 0, sizeof(indexed_page));
  memset(&options, 0, sizeof(options));
  memset(&write_result, 0, sizeof(write_result));
  memset(&delete_result, 0, sizeof(delete_result));
  lc_query_req_init(&query_req);
  lc_error_init(&error);
  make_root("query-keys-index-summary", root, sizeof(root));
  cleanup_root(root);
  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory("{\"category\":\"planning\",\"n\":1}",
                             strlen("{\"category\":\"planning\",\"n\":1}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index", "doc/a", source, NULL,
                            &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"category\":\"finance\",\"n\":2}",
                             strlen("{\"category\":\"finance\",\"n\":2}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index", "doc/b", source, NULL,
                            &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  memset(&options, 0, sizeof(options));
  options.has_query_hidden = 1;
  options.query_hidden = 1;
  rc = lc_source_from_memory("{\"category\":\"planning\",\"n\":3}",
                             strlen("{\"category\":\"planning\",\"n\":3}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index", "doc/hidden", source,
                            &options, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"category\":\"planning\",\"n\":4}",
                             strlen("{\"category\":\"planning\",\"n\":4}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index", "doc/deleted", source,
                            NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  rc = lc_pouch_state_delete(pouch, "docs/query-index", "doc/deleted", NULL,
                             &delete_result, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &delete_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  open_pouch_client(root, &client, &error);
  handler.begin = pouch_query_key_begin;
  handler.chunk = pouch_query_key_chunk;
  handler.end = pouch_query_key_end;
  query_req.namespace_name = "docs/query-index";
  query_req.refresh = "wait_for";
  query_req.limit = 1L;
  rc = client->query_keys(client, &query_req, &handler, &first_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(first_page.count, 1);
  assert_non_null(query_res.cursor);
  assert_string_equal(query_res.return_mode, "keys");
  assert_non_null(query_res.metadata_json);
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index-summary\""));
  assert_true(query_res.index_seq > 0UL);
  namespace_path = lc_pouch_namespace_path(NULL, root, "docs/query-index");
  assert_non_null(namespace_path);
  assert_path_file_contains(namespace_path, "index/query.index",
                            "row_count=3");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "term_index_complete=1");

  snprintf(cursor, sizeof(cursor), "%s", query_res.cursor);
  query_req.cursor = cursor;
  query_req.engine = "index";
  lc_query_res_cleanup(&query_res);
  rc = client->query_keys(client, &query_req, &handler, &second_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(second_page.count, 1);
  assert_null(query_res.cursor);
  assert_true(pouch_query_capture_has(&first_page, "doc/a") ||
              pouch_query_capture_has(&second_page, "doc/a"));
  assert_true(pouch_query_capture_has(&first_page, "doc/b") ||
              pouch_query_capture_has(&second_page, "doc/b"));
  assert_false(pouch_query_capture_has(&first_page, "doc/hidden"));
  assert_false(pouch_query_capture_has(&second_page, "doc/hidden"));
  assert_false(pouch_query_capture_has(&first_page, "doc/deleted"));
  assert_false(pouch_query_capture_has(&second_page, "doc/deleted"));
  lc_query_res_cleanup(&query_res);

  memset(&query_res, 0, sizeof(query_res));
  query_req.cursor = NULL;
  query_req.selector_json = selector;
  rc = client->query_keys(client, &query_req, &handler, &indexed_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(indexed_page.count, 1);
  assert_true(pouch_query_capture_has(&indexed_page, "doc/a"));
  assert_false(pouch_query_capture_has(&indexed_page, "doc/b"));
  assert_false(pouch_query_capture_has(&indexed_page, "doc/hidden"));
  assert_false(pouch_query_capture_has(&indexed_page, "doc/deleted"));
  assert_null(query_res.cursor);
  assert_non_null(query_res.metadata_json);
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));

  free(namespace_path);
  lc_query_res_cleanup(&query_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_query_keys_index_scalar_in_uses_array_postings(void **state) {
  static const char selector[] =
      "{\"in\":{\"field\":\"/tags[]\",\"any\":[\"planning\",\"finance\"]}}";
  lc_client *client;
  lc_pouch *pouch;
  lc_source *source;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_query_key_handler handler;
  pouch_query_key_capture first_page;
  pouch_query_key_capture second_page;
  pouch_query_key_capture unsupported_page;
  lc_pouch_state_write_options hidden_options;
  lc_pouch_state_write_result write_result;
  lc_pouch_state_write_result delete_result;
  lc_error error;
  char *namespace_path;
  char root[512];
  char cursor[64];
  int rc;

  (void)state;
  client = NULL;
  pouch = NULL;
  source = NULL;
  namespace_path = NULL;
  memset(&query_res, 0, sizeof(query_res));
  memset(&handler, 0, sizeof(handler));
  memset(&first_page, 0, sizeof(first_page));
  memset(&second_page, 0, sizeof(second_page));
  memset(&unsupported_page, 0, sizeof(unsupported_page));
  memset(&hidden_options, 0, sizeof(hidden_options));
  memset(&write_result, 0, sizeof(write_result));
  memset(&delete_result, 0, sizeof(delete_result));
  lc_query_req_init(&query_req);
  lc_error_init(&error);
  make_root("query-keys-index-in", root, sizeof(root));
  cleanup_root(root);
  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory(
      "{\"tags\":[\"planning\",\"finance\"],\"n\":1}",
      strlen("{\"tags\":[\"planning\",\"finance\"],\"n\":1}"), &source,
      &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-in", "doc/a", source,
                            NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"tags\":[\"ops\"],\"n\":2}",
                             strlen("{\"tags\":[\"ops\"],\"n\":2}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-in", "doc/b", source,
                            NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"tags\":[\"finance\"],\"n\":3}",
                             strlen("{\"tags\":[\"finance\"],\"n\":3}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-in", "doc/c", source,
                            NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  hidden_options.has_query_hidden = 1;
  hidden_options.query_hidden = 1;
  rc = lc_source_from_memory("{\"tags\":[\"planning\"],\"n\":4}",
                             strlen("{\"tags\":[\"planning\"],\"n\":4}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-in", "doc/hidden",
                            source, &hidden_options, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"tags\":[\"finance\"],\"n\":5}",
                             strlen("{\"tags\":[\"finance\"],\"n\":5}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-index-in", "doc/deleted",
                            source, NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  rc = lc_pouch_state_delete(pouch, "docs/query-index-in", "doc/deleted",
                             NULL, &delete_result, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &delete_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  open_pouch_client(root, &client, &error);
  handler.begin = pouch_query_key_begin;
  handler.chunk = pouch_query_key_chunk;
  handler.end = pouch_query_key_end;
  query_req.namespace_name = "docs/query-index-in";
  query_req.selector_json = selector;
  query_req.engine = "index";
  query_req.refresh = "wait_for";
  query_req.limit = 1L;
  rc = client->query_keys(client, &query_req, &handler, &first_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(first_page.count, 1);
  assert_non_null(query_res.cursor);
  assert_non_null(query_res.metadata_json);
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));
  namespace_path =
      lc_pouch_namespace_path(NULL, root, "docs/query-index-in");
  assert_non_null(namespace_path);
  assert_path_file_contains(namespace_path, "index/query.index",
                            "term_index_complete=1");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "2f746167732f5b5d");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "706c616e6e696e67");

  snprintf(cursor, sizeof(cursor), "%s", query_res.cursor);
  query_req.cursor = cursor;
  lc_query_res_cleanup(&query_res);
  rc = client->query_keys(client, &query_req, &handler, &second_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(second_page.count, 1);
  assert_null(query_res.cursor);
  assert_true(pouch_query_capture_has(&first_page, "doc/a") ||
              pouch_query_capture_has(&second_page, "doc/a"));
  assert_true(pouch_query_capture_has(&first_page, "doc/c") ||
              pouch_query_capture_has(&second_page, "doc/c"));
  assert_false(pouch_query_capture_has(&first_page, "doc/b"));
  assert_false(pouch_query_capture_has(&second_page, "doc/b"));
  assert_false(pouch_query_capture_has(&first_page, "doc/hidden"));
  assert_false(pouch_query_capture_has(&second_page, "doc/hidden"));
  assert_false(pouch_query_capture_has(&first_page, "doc/deleted"));
  assert_false(pouch_query_capture_has(&second_page, "doc/deleted"));
  lc_query_res_cleanup(&query_res);

  memset(&query_res, 0, sizeof(query_res));
  query_req.cursor = NULL;
  query_req.selector_json =
      "{\"and\":[{\"eq\":{\"field\":\"/n\",\"value\":\"1\"}}]}";
  rc = client->query_keys(client, &query_req, &handler, &unsupported_page,
                          &query_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_true(bytes_contain_text(error.message, strlen(error.message),
                                 "supports exact scalar equality and in"));

  free(namespace_path);
  lc_query_res_cleanup(&query_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_query_documents_scan_streams_rows(void **state) {
  static const char selector[] =
      "{\"eq\":{\"field\":\"/category\",\"value\":\"planning\"}}";
  lc_client *client;
  lc_pouch *pouch;
  lc_source *source;
  lc_sink *first_sink;
  lc_sink *second_sink;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result write_result;
  lc_error error;
  const void *first_bytes;
  const void *second_bytes;
  size_t first_length;
  size_t second_length;
  char root[512];
  char cursor[64];
  int rc;

  (void)state;
  client = NULL;
  pouch = NULL;
  source = NULL;
  first_sink = NULL;
  second_sink = NULL;
  first_bytes = NULL;
  second_bytes = NULL;
  first_length = 0U;
  second_length = 0U;
  memset(&query_res, 0, sizeof(query_res));
  memset(&options, 0, sizeof(options));
  memset(&write_result, 0, sizeof(write_result));
  lc_query_req_init(&query_req);
  lc_error_init(&error);
  make_root("query-documents-scan", root, sizeof(root));
  cleanup_root(root);
  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory("{\"category\":\"planning\",\"n\":1}",
                             strlen("{\"category\":\"planning\",\"n\":1}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-docs", "doc/a", source, NULL,
                            &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"category\":\"planning\",\"n\":2}",
                             strlen("{\"category\":\"planning\",\"n\":2}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-docs", "doc/b", source, NULL,
                            &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"category\":\"finance\",\"n\":3}",
                             strlen("{\"category\":\"finance\",\"n\":3}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-docs", "doc/c", source, NULL,
                            &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  memset(&options, 0, sizeof(options));
  options.has_query_hidden = 1;
  options.query_hidden = 1;
  rc = lc_source_from_memory("{\"category\":\"planning\",\"n\":4}",
                             strlen("{\"category\":\"planning\",\"n\":4}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-docs", "doc/hidden", source,
                            &options, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"category\":\"planning\",\"n\":5}",
                             strlen("{\"category\":\"planning\",\"n\":5}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_stage_write(pouch, "docs/query-docs", "doc/staged",
                                  "txn-query-docs-hidden", source, NULL,
                                  &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  open_pouch_client(root, &client, &error);
  rc = lc_sink_to_memory(&first_sink, &error);
  assert_int_equal(rc, LC_OK);

  query_req.namespace_name = "docs/query-docs";
  query_req.selector_json = selector;
  query_req.limit = 1L;
  query_req.return_mode = "documents";
  query_req.engine = "scan";
  rc = client->query(client, &query_req, first_sink, &query_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(first_sink, &first_bytes, &first_length, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(first_length > 0U);
  assert_non_null(query_res.cursor);
  assert_string_equal(query_res.return_mode, "documents");
  assert_non_null(query_res.metadata_json);
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"scan\""));
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "query_candidates"));
  assert_true(query_res.index_seq > 0UL);

  snprintf(cursor, sizeof(cursor), "%s", query_res.cursor);
  query_req.cursor = cursor;
  lc_query_res_cleanup(&query_res);
  rc = lc_sink_to_memory(&second_sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->query(client, &query_req, second_sink, &query_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(second_sink, &second_bytes, &second_length, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(second_length > 0U);
  assert_null(query_res.cursor);
  assert_true(bytes_contain_text(first_bytes, first_length, "\"n\":1") ||
              bytes_contain_text(second_bytes, second_length, "\"n\":1"));
  assert_true(bytes_contain_text(first_bytes, first_length, "\"n\":2") ||
              bytes_contain_text(second_bytes, second_length, "\"n\":2"));
  assert_false(bytes_contain_text(first_bytes, first_length, "\"n\":3"));
  assert_false(bytes_contain_text(second_bytes, second_length, "\"n\":3"));
  assert_false(bytes_contain_text(first_bytes, first_length, "\"n\":4"));
  assert_false(bytes_contain_text(second_bytes, second_length, "\"n\":4"));
  assert_false(bytes_contain_text(first_bytes, first_length, "\"n\":5"));
  assert_false(bytes_contain_text(second_bytes, second_length, "\"n\":5"));

  lc_sink_close(first_sink);
  lc_sink_close(second_sink);
  lc_query_res_cleanup(&query_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_query_documents_index_uses_scalar_postings(void **state) {
  static const char selector[] =
      "{\"in\":{\"field\":\"/tags[]\",\"any\":[\"planning\",\"finance\"]}}";
  lc_client *client;
  lc_pouch *pouch;
  lc_source *source;
  lc_sink *first_sink;
  lc_sink *second_sink;
  lc_sink *unsupported_sink;
  lc_query_req query_req;
  lc_query_res query_res;
  lc_pouch_state_write_options hidden_options;
  lc_pouch_state_write_result write_result;
  lc_pouch_state_write_result delete_result;
  lc_error error;
  const void *first_bytes;
  const void *second_bytes;
  size_t first_length;
  size_t second_length;
  char root[512];
  char cursor[64];
  int rc;

  (void)state;
  client = NULL;
  pouch = NULL;
  source = NULL;
  first_sink = NULL;
  second_sink = NULL;
  unsupported_sink = NULL;
  first_bytes = NULL;
  second_bytes = NULL;
  first_length = 0U;
  second_length = 0U;
  memset(&query_res, 0, sizeof(query_res));
  memset(&hidden_options, 0, sizeof(hidden_options));
  memset(&write_result, 0, sizeof(write_result));
  memset(&delete_result, 0, sizeof(delete_result));
  lc_query_req_init(&query_req);
  lc_error_init(&error);
  make_root("query-documents-index", root, sizeof(root));
  cleanup_root(root);
  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_source_from_memory(
      "{\"tags\":[\"planning\",\"finance\"],\"n\":1}",
      strlen("{\"tags\":[\"planning\",\"finance\"],\"n\":1}"), &source,
      &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-docs-index", "doc/a", source,
                            NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"tags\":[\"ops\"],\"n\":2}",
                             strlen("{\"tags\":[\"ops\"],\"n\":2}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-docs-index", "doc/b", source,
                            NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"tags\":[\"finance\"],\"n\":3}",
                             strlen("{\"tags\":[\"finance\"],\"n\":3}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-docs-index", "doc/c", source,
                            NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  hidden_options.has_query_hidden = 1;
  hidden_options.query_hidden = 1;
  rc = lc_source_from_memory("{\"tags\":[\"planning\"],\"n\":4}",
                             strlen("{\"tags\":[\"planning\"],\"n\":4}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-docs-index", "doc/hidden",
                            source, &hidden_options, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"tags\":[\"finance\"],\"n\":5}",
                             strlen("{\"tags\":[\"finance\"],\"n\":5}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, "docs/query-docs-index", "doc/deleted",
                            source, NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  rc = lc_pouch_state_delete(pouch, "docs/query-docs-index", "doc/deleted",
                             NULL, &delete_result, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &delete_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  open_pouch_client(root, &client, &error);
  rc = lc_sink_to_memory(&first_sink, &error);
  assert_int_equal(rc, LC_OK);
  query_req.namespace_name = "docs/query-docs-index";
  query_req.selector_json = selector;
  query_req.limit = 1L;
  query_req.return_mode = "documents";
  query_req.engine = "index";
  query_req.refresh = "wait_for";
  rc = client->query(client, &query_req, first_sink, &query_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(first_sink, &first_bytes, &first_length, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(first_length > 0U);
  assert_non_null(query_res.cursor);
  assert_string_equal(query_res.return_mode, "documents");
  assert_non_null(query_res.metadata_json);
  assert_true(bytes_contain_text(query_res.metadata_json,
                                 strlen(query_res.metadata_json),
                                 "\"engine\":\"index\""));

  snprintf(cursor, sizeof(cursor), "%s", query_res.cursor);
  query_req.cursor = cursor;
  lc_query_res_cleanup(&query_res);
  rc = lc_sink_to_memory(&second_sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->query(client, &query_req, second_sink, &query_res, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_memory_bytes(second_sink, &second_bytes, &second_length,
                            &error);
  assert_int_equal(rc, LC_OK);
  assert_true(second_length > 0U);
  assert_null(query_res.cursor);
  assert_true(bytes_contain_text(first_bytes, first_length, "\"n\":1") ||
              bytes_contain_text(second_bytes, second_length, "\"n\":1"));
  assert_true(bytes_contain_text(first_bytes, first_length, "\"n\":3") ||
              bytes_contain_text(second_bytes, second_length, "\"n\":3"));
  assert_false(bytes_contain_text(first_bytes, first_length, "\"n\":2"));
  assert_false(bytes_contain_text(second_bytes, second_length, "\"n\":2"));
  assert_false(bytes_contain_text(first_bytes, first_length, "\"n\":4"));
  assert_false(bytes_contain_text(second_bytes, second_length, "\"n\":4"));
  assert_false(bytes_contain_text(first_bytes, first_length, "\"n\":5"));
  assert_false(bytes_contain_text(second_bytes, second_length, "\"n\":5"));
  lc_query_res_cleanup(&query_res);

  memset(&query_res, 0, sizeof(query_res));
  query_req.cursor = NULL;
  query_req.selector_json =
      "{\"and\":[{\"eq\":{\"field\":\"/n\",\"value\":\"1\"}}]}";
  rc = lc_sink_to_memory(&unsupported_sink, &error);
  assert_int_equal(rc, LC_OK);
  rc = client->query(client, &query_req, unsupported_sink, &query_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_true(bytes_contain_text(error.message, strlen(error.message),
                                 "supports exact scalar equality and in"));

  lc_sink_close(first_sink);
  lc_sink_close(second_sink);
  lc_sink_close(unsupported_sink);
  lc_query_res_cleanup(&query_res);
  lc_client_close(client);
  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_flush_index_reports_projection_high_water(void **state) {
  lc_client *client;
  lc_pouch *writer;
  lc_source *source;
  lc_pouch_state_write_options hidden_options;
  lc_pouch_state_write_result write_result;
  lc_pouch_state_write_result live_result;
  lc_pouch_state_write_result hidden_result;
  lc_pouch_state_write_result delete_result;
  lc_index_flush_req flush_req;
  lc_index_flush_res flush_res;
  lc_error error;
  char *namespace_path;
  char sidecar_path[1024];
  char root[512];
  unsigned long delete_version;
  int rc;

  (void)state;
  client = NULL;
  writer = NULL;
  source = NULL;
  namespace_path = NULL;
  memset(&hidden_options, 0, sizeof(hidden_options));
  memset(&write_result, 0, sizeof(write_result));
  memset(&live_result, 0, sizeof(live_result));
  memset(&hidden_result, 0, sizeof(hidden_result));
  memset(&delete_result, 0, sizeof(delete_result));
  lc_index_flush_req_init(&flush_req);
  memset(&flush_res, 0, sizeof(flush_res));
  lc_error_init(&error);
  make_root("flush-index", root, sizeof(root));
  cleanup_root(root);

  open_pouch_client(root, &client, &error);
  rc = lc_pouch_open(root, NULL, NULL, &writer, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("{\"kind\":\"flush\",\"n\":1}",
                             strlen("{\"kind\":\"flush\",\"n\":1}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(writer, "docs/flush", "doc/a", source, NULL,
                            &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("{\"kind\":\"flush\",\"n\":2}",
                             strlen("{\"kind\":\"flush\",\"n\":2}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(writer, "docs/flush", "doc/live", source, NULL,
                            &live_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &live_result);

  hidden_options.has_query_hidden = 1;
  hidden_options.query_hidden = 1;
  rc = lc_source_from_memory("{\"kind\":\"flush\",\"n\":3}",
                             strlen("{\"kind\":\"flush\",\"n\":3}"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(writer, "docs/flush", "doc/hidden", source,
                            &hidden_options, &hidden_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &hidden_result);

  rc = lc_pouch_state_delete(writer, "docs/flush", "doc/a", NULL,
                             &delete_result, &error);
  assert_int_equal(rc, LC_OK);
  delete_version = delete_result.version;
  lc_pouch_state_write_result_cleanup(NULL, &delete_result);
  lc_pouch_close(writer);
  writer = NULL;

  flush_req.namespace_name = "docs/flush";
  flush_req.mode = "wait";
  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(flush_res.namespace_name, "docs/flush");
  assert_string_equal(flush_res.mode, "wait");
  assert_string_equal(flush_res.flush_id, "pouch-query-index-repair");
  assert_true(flush_res.accepted);
  assert_true(flush_res.flushed);
  assert_false(flush_res.pending);
  assert_true(flush_res.index_seq >= delete_version);
  assert_string_equal(flush_res.correlation_id, "pouch-index-flush");
  namespace_path = lc_pouch_namespace_path(NULL, root, "docs/flush");
  assert_non_null(namespace_path);
  assert_path_file_contains(namespace_path, "index/query.index",
                            "format=pouch-query-index");
  assert_path_file_contains(namespace_path, "index/query.index", "version=3");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "state_index_seq=4");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "row_count=2");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "term_count=");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "term_index_complete=1");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "summary_hash=");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "646f632f6c697665");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "row 3 ");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "646f632f68696464656e");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "row 2 ");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "2f6b696e64");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "666c757368");
  lc_index_flush_res_cleanup(&flush_res);

  snprintf(sidecar_path, sizeof(sidecar_path), "%s/index/query.index",
           namespace_path);
  write_text_file(sidecar_path,
                  "format=pouch-query-index\nversion=2\nstate_index_seq=4\n"
                  "row_count=1\nsummary_hash=1\nrow 3 22 0 0 "
                  "646f632f6c697665 - -\n");
  flush_req.mode = "sync";
  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(flush_res.mode, "sync");
  assert_string_equal(flush_res.flush_id, "pouch-query-index-repair");
  assert_true(flush_res.index_seq >= delete_version);
  assert_path_file_contains(namespace_path, "index/query.index",
                            "format=pouch-query-index");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "state_index_seq=4");
  assert_path_file_contains(namespace_path, "index/query.index",
                            "row_count=2");
  lc_index_flush_res_cleanup(&flush_res);

  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(flush_res.flush_id, "pouch-query-index-flush");
  lc_index_flush_res_cleanup(&flush_res);

  flush_req.mode = "eventually";
  rc = client->flush_index(client, &flush_req, &flush_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message, "pouch flush_index mode must be wait or sync");

  free(namespace_path);
  lc_error_cleanup(&error);
  lc_client_close(client);
  cleanup_root(root);
}

static void test_txn_decisions_persist_participant_records(void **state) {
  lc_client *client;
  lc_client *reader;
  lc_pouch *pouch;
  lc_source *source;
  lc_txn_participant participants[2];
  lc_txn_decision_req decision_req;
  lc_txn_decision_res decision_res;
  lc_txn_replay_req replay_req;
  lc_txn_replay_res replay_res;
  lc_pouch_state_write_result write_result;
  lc_pouch_state_read_result read_result;
  lc_error error;
  char root[512];
  char txn_record[1024];
  char state_bytes[128];
  char namespace_hex[128];
  char key_hex[128];
  char backend_hex[128];
  int rc;

  (void)state;
  client = NULL;
  reader = NULL;
  pouch = NULL;
  source = NULL;
  memset(participants, 0, sizeof(participants));
  memset(&decision_res, 0, sizeof(decision_res));
  memset(&replay_res, 0, sizeof(replay_res));
  memset(&write_result, 0, sizeof(write_result));
  memset(&read_result, 0, sizeof(read_result));
  lc_txn_decision_req_init(&decision_req);
  lc_txn_replay_req_init(&replay_req);
  lc_error_init(&error);
  make_root("txn-records", root, sizeof(root));
  cleanup_root(root);

  participants[0].namespace_name = "orders/eu";
  participants[0].key = "state/order-1";
  participants[0].backend_hash = "backend-a";
  participants[1].namespace_name = "orders/us";
  participants[1].key = "state/order-2";
  participants[1].backend_hash = "backend-b";
  decision_req.txn_id = "txn-pouch-records";
  decision_req.participants = participants;
  decision_req.participant_count = 2U;
  decision_req.expires_at_unix = 2147483647L;
  decision_req.tc_term = 7UL;
  decision_req.target_backend_hash = "target-backend";

  open_pouch_client(root, &client, &error);
  rc = client->txn_prepare(client, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(decision_res.txn_id, "txn-pouch-records");
  assert_string_equal(decision_res.state, "prepare");
  assert_string_equal(decision_res.correlation_id,
                      "pouch-txn-00000000000000000001");
  lc_txn_decision_res_cleanup(&decision_res);
  lc_client_close(client);
  client = NULL;

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_source_from_memory("committed-order-1",
                             strlen("committed-order-1"), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_stage_write(pouch, "orders/eu", "state/order-1",
                                  "txn-pouch-records", source, NULL,
                                  &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("committed-order-2",
                             strlen("committed-order-2"), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_stage_write(pouch, "orders/us", "state/order-2",
                                  "txn-pouch-records", source, NULL,
                                  &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  rc = lc_source_from_memory("rolled-back-order",
                             strlen("rolled-back-order"), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_stage_write(pouch, "orders/eu", "state/order-1",
                                  "txn-pouch-rollback", source, NULL,
                                  &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  open_pouch_client(root, &reader, &error);
  replay_req.txn_id = "txn-pouch-records";
  rc = reader->txn_replay(reader, &replay_req, &replay_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(replay_res.txn_id, "txn-pouch-records");
  assert_string_equal(replay_res.state, "prepare");
  assert_string_equal(replay_res.correlation_id,
                      "pouch-txn-00000000000000000001");
  lc_txn_replay_res_cleanup(&replay_res);

  rc = reader->txn_commit(reader, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(decision_res.state, "commit");
  assert_string_equal(decision_res.correlation_id,
                      "pouch-txn-00000000000000000002");
  lc_txn_decision_res_cleanup(&decision_res);

  rc = reader->txn_replay(reader, &replay_req, &replay_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(replay_res.state, "commit");
  assert_string_equal(replay_res.correlation_id,
                      "pouch-txn-00000000000000000002");
  lc_txn_replay_res_cleanup(&replay_res);
  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, "orders/eu", "state/order-1",
                           &read_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_result.found);
  read_source_to_string(read_result.body, state_bytes, sizeof(state_bytes));
  assert_string_equal(state_bytes, "committed-order-1");
  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  rc = lc_pouch_state_read(pouch, "orders/eu",
                           "state/order-1/.staging/txn-pouch-records",
                           &read_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(read_result.found);
  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  rc = lc_pouch_state_read(pouch, "orders/eu", "state/order-1",
                           &read_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_result.found);
  read_source_to_string(read_result.body, state_bytes, sizeof(state_bytes));
  assert_string_equal(state_bytes, "committed-order-1");
  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  decision_req.txn_id = "txn-pouch-rollback";
  rc = reader->txn_rollback(reader, &decision_req, &decision_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(decision_res.txn_id, "txn-pouch-rollback");
  assert_string_equal(decision_res.state, "rollback");
  assert_string_equal(decision_res.correlation_id,
                      "pouch-txn-00000000000000000003");
  lc_txn_decision_res_cleanup(&decision_res);

  replay_req.txn_id = "txn-pouch-rollback";
  rc = reader->txn_replay(reader, &replay_req, &replay_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(replay_res.txn_id, "txn-pouch-rollback");
  assert_string_equal(replay_res.state, "rollback");
  assert_string_equal(replay_res.correlation_id,
                      "pouch-txn-00000000000000000003");
  lc_txn_replay_res_cleanup(&replay_res);
  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, "orders/eu",
                           "state/order-1/.staging/txn-pouch-rollback",
                           &read_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(read_result.found);
  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_close(pouch);
  pouch = NULL;
  lc_client_close(reader);
  reader = NULL;

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, ".lockd/txn", "txn/txn-pouch-records",
                           &read_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_result.found);
  assert_string_equal(read_result.content_type,
                      "application/x-lockdc-pouch-txn");
  read_source_to_string(read_result.body, txn_record, sizeof(txn_record));
  hex_encode_string("orders/eu", namespace_hex, sizeof(namespace_hex));
  hex_encode_string("state/order-1", key_hex, sizeof(key_hex));
  hex_encode_string("backend-a", backend_hex, sizeof(backend_hex));
  assert_non_null(strstr(txn_record, "state 636f6d6d6974\n"));
  assert_non_null(strstr(txn_record, namespace_hex));
  assert_non_null(strstr(txn_record, key_hex));
  assert_non_null(strstr(txn_record, backend_hex));

  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  open_pouch_client(root, &reader, &error);
  lc_client_close(reader);
  reader = NULL;

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, ".lockd/txn", "txn/txn-pouch-records",
                           &read_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(read_result.found);
  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  cleanup_root(root);
  lc_error_cleanup(&error);
}

static void test_txn_recovery_applies_decisions_on_client_open(void **state) {
  lc_client *client;
  lc_pouch *pouch;
  lc_source *source;
  lc_pouch_state_write_result write_result;
  lc_pouch_state_read_result read_result;
  lc_error error;
  char root[512];
  char record[1024];
  char namespace_hex[128];
  char key_hex[128];
  char backend_hex[128];
  char bytes[128];
  int written;
  int rc;

  (void)state;
  client = NULL;
  pouch = NULL;
  source = NULL;
  memset(&write_result, 0, sizeof(write_result));
  memset(&read_result, 0, sizeof(read_result));
  lc_error_init(&error);
  make_root("txn-recovery", root, sizeof(root));
  cleanup_root(root);

  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  hex_encode_string("orders/recover", namespace_hex, sizeof(namespace_hex));
  hex_encode_string("state/recover-commit", key_hex, sizeof(key_hex));
  hex_encode_string("backend-recover", backend_hex, sizeof(backend_hex));
  written = snprintf(record, sizeof(record),
                     "format pouch-txn-v1\nstate 636f6d6d6974\n"
                     "expires_at_unix 0\ntc_term 1\n"
                     "target_backend_hash \nparticipant_count 1\n"
                     "participant %s %s %s\n",
                     namespace_hex, key_hex, backend_hex);
  assert_true(written > 0 && (size_t)written < sizeof(record));
  rc = lc_source_from_memory("recovered-commit", strlen("recovered-commit"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_stage_write(pouch, "orders/recover",
                                  "state/recover-commit",
                                  "txn-recover-commit", source, NULL,
                                  &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  rc = lc_source_from_memory(record, strlen(record), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, ".lockd/txn", "txn/txn-recover-commit",
                            source, NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);

  hex_encode_string("state/recover-expired", key_hex, sizeof(key_hex));
  written = snprintf(record, sizeof(record),
                     "format pouch-txn-v1\nstate 70726570617265\n"
                     "expires_at_unix 1\ntc_term 1\n"
                     "target_backend_hash \nparticipant_count 1\n"
                     "participant %s %s %s\n",
                     namespace_hex, key_hex, backend_hex);
  assert_true(written > 0 && (size_t)written < sizeof(record));
  rc = lc_source_from_memory("expired-stage", strlen("expired-stage"),
                             &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_stage_write(pouch, "orders/recover",
                                  "state/recover-expired",
                                  "txn-recover-expired", source, NULL,
                                  &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  rc = lc_source_from_memory(record, strlen(record), &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_write(pouch, ".lockd/txn", "txn/txn-recover-expired",
                            source, NULL, &write_result, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_write_result_cleanup(NULL, &write_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  open_pouch_client(root, &client, &error);
  rc = lc_pouch_open(root, NULL, NULL, &pouch, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_state_read(pouch, "orders/recover", "state/recover-commit",
                           &read_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(read_result.found);
  read_source_to_string(read_result.body, bytes, sizeof(bytes));
  assert_string_equal(bytes, "recovered-commit");
  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  rc = lc_pouch_state_read(
      pouch, "orders/recover",
      "state/recover-expired/.staging/txn-recover-expired", &read_result,
      &error);
  assert_int_equal(rc, LC_OK);
  assert_false(read_result.found);
  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  rc = lc_pouch_state_read(pouch, "orders/recover", "state/recover-expired",
                           &read_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(read_result.found);
  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  rc = lc_pouch_state_read(pouch, ".lockd/txn", "txn/txn-recover-commit",
                           &read_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(read_result.found);
  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  rc = lc_pouch_state_read(pouch, ".lockd/txn", "txn/txn-recover-expired",
                           &read_result, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(read_result.found);
  lc_pouch_state_read_result_cleanup(NULL, &read_result);
  lc_pouch_close(pouch);
  pouch = NULL;

  lc_client_close(client);
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
      cmocka_unit_test(test_state_scheduled_compaction_installs_snapshot),
      cmocka_unit_test(test_state_metadata_survives_snapshot_compaction),
      cmocka_unit_test(
          test_namespace_manifest_repairs_from_existing_segments),
      cmocka_unit_test(test_staged_state_writes_durable_decision_records),
      cmocka_unit_test(
          test_staged_decision_recovery_tombstones_interrupted_discard),
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
      cmocka_unit_test(
          test_shared_state_projection_cache_refreshes_peer_markers),
      cmocka_unit_test(test_lease_bound_state_update_get_and_release),
      cmocka_unit_test(
          test_lease_save_streams_mapped_json_and_replays_after_reopen),
      cmocka_unit_test(test_lease_keepalive_and_release_use_local_lifecycle),
      cmocka_unit_test(test_acquire_rejects_non_positive_ttl),
      cmocka_unit_test(test_lease_metadata_persists_query_hidden),
      cmocka_unit_test(test_client_metadata_enforces_version_precondition),
      cmocka_unit_test(test_query_keys_scan_uses_liblql_and_query_hidden),
      cmocka_unit_test(test_query_keys_index_summary_uses_sidecar_rows),
      cmocka_unit_test(test_query_keys_index_scalar_in_uses_array_postings),
      cmocka_unit_test(test_query_documents_scan_streams_rows),
      cmocka_unit_test(test_query_documents_index_uses_scalar_postings),
      cmocka_unit_test(test_flush_index_reports_projection_high_water),
      cmocka_unit_test(test_txn_decisions_persist_participant_records),
      cmocka_unit_test(test_txn_recovery_applies_decisions_on_client_open),
      cmocka_unit_test(test_lease_remove_tombstones_state_and_refreshes_view),
      cmocka_unit_test(test_acquire_for_update_success_and_rollback),
      cmocka_unit_test(test_acquire_for_update_rollback_removes_new_state),
  };

  return cmocka_run_group_tests(tests, setup_pouch_unit_group,
                                teardown_pouch_unit_group);
}

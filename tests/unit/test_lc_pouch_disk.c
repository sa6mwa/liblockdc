#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cmocka.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "lc_pouch_store.h"

typedef struct tracked_allocator {
  size_t malloc_calls;
  size_t realloc_calls;
  size_t free_calls;
} tracked_allocator;

static void *tracked_malloc(void *context, size_t size) {
  tracked_allocator *tracked;

  tracked = (tracked_allocator *)context;
  tracked->malloc_calls++;
  return malloc(size);
}

static void *tracked_realloc(void *context, void *ptr, size_t size) {
  tracked_allocator *tracked;

  tracked = (tracked_allocator *)context;
  tracked->realloc_calls++;
  return realloc(ptr, size);
}

static void tracked_free(void *context, void *ptr) {
  tracked_allocator *tracked;

  tracked = (tracked_allocator *)context;
  tracked->free_calls++;
  free(ptr);
}

static void test_allocator_init(lc_pouch_allocator *allocator,
                                tracked_allocator *tracked) {
  memset(tracked, 0, sizeof(*tracked));
  memset(allocator, 0, sizeof(*allocator));
  allocator->malloc_fn = tracked_malloc;
  allocator->realloc_fn = tracked_realloc;
  allocator->free_fn = tracked_free;
  allocator->context = tracked;
}

static void test_root_path(char *buffer, size_t buffer_size,
                           const char *suffix) {
  snprintf(buffer, buffer_size, "/tmp/liblockdc-pouch-%ld-%s", (long)getpid(),
           suffix);
}

static void test_cleanup_root(const char *root) {
  char path[512];

  snprintf(path, sizeof(path), "%s/store.log", root);
  unlink(path);
  snprintf(path, sizeof(path), "%s/writer.lock", root);
  unlink(path);
  rmdir(root);
}

static lc_source *source_from_text(const char *text) {
  lc_source *source;
  lc_error error;
  int rc;

  source = NULL;
  memset(&error, 0, sizeof(error));
  rc = lc_source_from_memory(text, strlen(text), &source, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(source);
  lc_error_cleanup(&error);
  return source;
}

static char *read_source_text(lc_source *source) {
  char buffer[256];
  size_t got;
  char *copy;
  lc_error error;

  memset(&error, 0, sizeof(error));
  got = source->read(source, buffer, sizeof(buffer) - 1U, &error);
  assert_int_equal(error.code, LC_OK);
  assert_true(got < sizeof(buffer));
  buffer[got] = '\0';
  copy = strdup(buffer);
  assert_non_null(copy);
  lc_error_cleanup(&error);
  return copy;
}

static void test_write_read_reopen_and_allocator_hooks(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res put_res;
  lc_pouch_state_info info;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "durable");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&info, 0, sizeof(info));
  store = NULL;
  read_body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(store);

  source = source_from_text("{\"value\":1}");
  opts.content_type = "application/json";
  rc = store->write_state(store, "default", "alpha", source, &opts, &put_res,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(put_res.new_version, 1L);
  assert_non_null(put_res.new_state_etag);
  assert_int_equal(put_res.bytes, 11L);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->read_state(store, "default", "alpha", &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  assert_string_equal(info.content_type, "application/json");
  assert_string_equal(info.etag, put_res.new_state_etag);
  assert_int_equal(info.version, 1L);
  text = read_source_text(read_body);
  assert_string_equal(text, "{\"value\":1}");
  free(text);
  lc_source_close(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  assert_true(tracked.malloc_calls > 0U);
  assert_true(tracked.free_calls > 0U);
  test_cleanup_root(root);
}

static void test_cas_and_remove_semantics(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res first;
  lc_pouch_put_state_res second;
  lc_pouch_state_info info;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "cas");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  memset(&first, 0, sizeof(first));
  memset(&second, 0, sizeof(second));
  memset(&info, 0, sizeof(info));
  store = NULL;
  read_body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("one");
  rc = store->write_state(store, "default", "beta", source, NULL, &first,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  opts.if_state_etag = "wrong";
  source = source_from_text("two");
  rc = store->write_state(store, "default", "beta", source, &opts, &second,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 412L);
  lc_error_cleanup(&error);

  opts.if_state_etag = first.new_state_etag;
  source = source_from_text("two");
  rc = store->write_state(store, "default", "beta", source, &opts, &second,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(second.new_version, 2L);

  rc = store->remove_state(store, "default", "beta", "wrong", &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  lc_error_cleanup(&error);

  rc = store->remove_state(store, "default", "beta", second.new_state_etag,
                           &error);
  assert_int_equal(rc, LC_OK);

  rc = store->read_state(store, "default", "beta", &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(info.no_content);
  assert_null(read_body);

  lc_pouch_put_state_res_cleanup(&allocator, &first);
  lc_pouch_put_state_res_cleanup(&allocator, &second);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_replay_truncates_trailing_partial_record(void **state) {
  char root[256];
  char log_path[512];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_state_res put_res;
  lc_pouch_state_info info;
  lc_error error;
  char *text;
  int fd;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "partial");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&put_res, 0, sizeof(put_res));
  memset(&info, 0, sizeof(info));
  store = NULL;
  read_body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("stable");
  rc = store->write_state(store, "default", "gamma", source, NULL, &put_res,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);

  snprintf(log_path, sizeof(log_path), "%s/store.log", root);
  fd = open(log_path, O_WRONLY | O_APPEND);
  assert_true(fd >= 0);
  assert_int_equal(write(fd, "bad", 3U), 3);
  close(fd);

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->read_state(store, "default", "gamma", &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  text = read_source_text(read_body);
  assert_string_equal(text, "stable");
  free(text);
  lc_source_close(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_metadata_roundtrip_cas_delete_and_reopen(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_store_meta_res updated;
  lc_pouch_meta_record loaded;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "meta");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  memset(&updated, 0, sizeof(updated));
  memset(&loaded, 0, sizeof(loaded));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  meta.owner = "owner-a";
  meta.lease_id = "lease-a";
  meta.txn_id = "txn-a";
  meta.state_etag = "state-a";
  meta.version = 7L;
  meta.lease_expires_at_unix = 1234L;
  meta.fencing_token = 77L;
  meta.has_query_hidden = 1;
  meta.query_hidden = 1;
  rc = store->store_meta(store, "default", "lease-key", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(stored.etag);
  assert_int_equal(stored.version, 7L);

  rc = store->load_meta(store, "default", "lease-key", &loaded, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(loaded.found);
  assert_string_equal(loaded.namespace_name, "default");
  assert_string_equal(loaded.key, "lease-key");
  assert_string_equal(loaded.etag, stored.etag);
  assert_string_equal(loaded.meta.owner, "owner-a");
  assert_string_equal(loaded.meta.lease_id, "lease-a");
  assert_string_equal(loaded.meta.txn_id, "txn-a");
  assert_string_equal(loaded.meta.state_etag, "state-a");
  assert_int_equal(loaded.meta.version, 7L);
  assert_int_equal(loaded.meta.lease_expires_at_unix, 1234L);
  assert_int_equal(loaded.meta.fencing_token, 77L);
  assert_true(loaded.meta.has_query_hidden);
  assert_true(loaded.meta.query_hidden);
  lc_pouch_meta_record_cleanup(&allocator, &loaded);

  meta.owner = "owner-b";
  meta.lease_id = "lease-b";
  meta.txn_id = NULL;
  meta.state_etag = "state-b";
  meta.version = 8L;
  meta.lease_expires_at_unix = 2234L;
  meta.fencing_token = 78L;
  meta.has_query_hidden = 1;
  meta.query_hidden = 0;
  rc = store->store_meta(store, "default", "lease-key", &meta, "wrong",
                         &updated, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 412L);
  lc_error_cleanup(&error);

  rc = store->store_meta(store, "default", "lease-key", &meta, stored.etag,
                         &updated, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(updated.etag);
  assert_string_not_equal(updated.etag, stored.etag);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->load_meta(store, "default", "lease-key", &loaded, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(loaded.found);
  assert_string_equal(loaded.etag, updated.etag);
  assert_string_equal(loaded.meta.owner, "owner-b");
  assert_null(loaded.meta.txn_id);
  assert_false(loaded.meta.query_hidden);
  lc_pouch_meta_record_cleanup(&allocator, &loaded);

  rc = store->delete_meta(store, "default", "lease-key", "wrong", &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  lc_error_cleanup(&error);
  rc = store->delete_meta(store, "default", "lease-key", updated.etag, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->load_meta(store, "default", "lease-key", &loaded, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(loaded.found);

  lc_pouch_store_meta_res_cleanup(&allocator, &stored);
  lc_pouch_store_meta_res_cleanup(&allocator, &updated);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_write_read_reopen_and_allocator_hooks),
      cmocka_unit_test(test_cas_and_remove_semantics),
      cmocka_unit_test(test_replay_truncates_trailing_partial_record),
      cmocka_unit_test(test_metadata_roundtrip_cas_delete_and_reopen),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}

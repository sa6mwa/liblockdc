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

#define TEST_POUCH_HEADER_SIZE 64U
#define TEST_POUCH_HEADER_RECORD_VERSION_OFFSET 56U

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

static unsigned long test_get_u32(const unsigned char *src) {
  return ((unsigned long)src[0]) | (((unsigned long)src[1]) << 8) |
         (((unsigned long)src[2]) << 16) | (((unsigned long)src[3]) << 24);
}

static unsigned long test_get_u64(const unsigned char *src) {
  return test_get_u32(src) | (test_get_u32(src + 4) << 32);
}

static void test_put_u32(unsigned char *dst, unsigned long value) {
  dst[0] = (unsigned char)(value & 255UL);
  dst[1] = (unsigned char)((value >> 8) & 255UL);
  dst[2] = (unsigned char)((value >> 16) & 255UL);
  dst[3] = (unsigned char)((value >> 24) & 255UL);
}

static void corrupt_first_log_match(const char *root, const char *needle) {
  char log_path[512];
  unsigned char *bytes;
  size_t needle_len;
  size_t index;
  struct stat st;
  int fd;
  int found;

  snprintf(log_path, sizeof(log_path), "%s/store.log", root);
  fd = open(log_path, O_RDWR);
  assert_true(fd >= 0);
  assert_int_equal(fstat(fd, &st), 0);
  assert_true(st.st_size > 0);
  bytes = (unsigned char *)malloc((size_t)st.st_size);
  assert_non_null(bytes);
  assert_int_equal(lseek(fd, 0, SEEK_SET), 0);
  assert_int_equal(read(fd, bytes, (size_t)st.st_size), st.st_size);
  needle_len = strlen(needle);
  found = 0;
  for (index = 0U; index + needle_len <= (size_t)st.st_size; ++index) {
    if (memcmp(bytes + index, needle, needle_len) == 0) {
      bytes[index] ^= 1U;
      assert_int_equal(lseek(fd, (off_t)index, SEEK_SET), (off_t)index);
      assert_int_equal(write(fd, bytes + index, 1U), 1);
      found = 1;
      break;
    }
  }
  free(bytes);
  close(fd);
  assert_true(found);
}

static void set_first_log_match_record_version(const char *root,
                                               const char *needle,
                                               unsigned long version) {
  char log_path[512];
  unsigned char header[TEST_POUCH_HEADER_SIZE];
  unsigned char *payload;
  size_t needle_len;
  unsigned long payload_len;
  off_t record_offset;
  int fd;
  int found;

  snprintf(log_path, sizeof(log_path), "%s/store.log", root);
  fd = open(log_path, O_RDWR);
  assert_true(fd >= 0);
  assert_int_equal(lseek(fd, 0, SEEK_SET), 0);
  needle_len = strlen(needle);
  found = 0;
  while (!found) {
    record_offset = lseek(fd, 0, SEEK_CUR);
    assert_true(record_offset >= 0);
    assert_int_equal(read(fd, header, sizeof(header)), sizeof(header));
    assert_memory_equal(header, "LCP1", 4U);
    payload_len = test_get_u64(header + 44);
    payload = (unsigned char *)malloc((size_t)payload_len);
    assert_non_null(payload);
    assert_int_equal(read(fd, payload, (size_t)payload_len), payload_len);
    if (payload_len >= needle_len) {
      size_t index;

      for (index = 0U; index + needle_len <= payload_len; ++index) {
        if (memcmp(payload + index, needle, needle_len) == 0) {
          test_put_u32(header + TEST_POUCH_HEADER_RECORD_VERSION_OFFSET,
                       version);
          assert_int_equal(
              lseek(fd,
                    record_offset + TEST_POUCH_HEADER_RECORD_VERSION_OFFSET,
                    SEEK_SET),
              record_offset + TEST_POUCH_HEADER_RECORD_VERSION_OFFSET);
          assert_int_equal(
              write(fd, header + TEST_POUCH_HEADER_RECORD_VERSION_OFFSET, 4U),
              4);
          found = 1;
          break;
        }
      }
    }
    free(payload);
    if (!found) {
      assert_int_equal(lseek(fd, record_offset + TEST_POUCH_HEADER_SIZE +
                                     (off_t)payload_len,
                              SEEK_SET),
                       record_offset + TEST_POUCH_HEADER_SIZE +
                           (off_t)payload_len);
    }
  }
  close(fd);
}

typedef struct scan_capture {
  char keys[8][64];
  long versions[8];
  int query_hidden[8];
  size_t count;
} scan_capture;

static int capture_scan_row(void *context, const lc_pouch_scan_meta_row *row,
                            lc_error *error) {
  scan_capture *capture;

  (void)error;
  capture = (scan_capture *)context;
  assert_non_null(row);
  assert_non_null(row->key);
  assert_non_null(row->etag);
  assert_non_null(row->meta);
  assert_true(capture->count < sizeof(capture->keys) / sizeof(capture->keys[0]));
  snprintf(capture->keys[capture->count],
           sizeof(capture->keys[capture->count]), "%s", row->key);
  capture->versions[capture->count] = row->meta->version;
  capture->query_hidden[capture->count] = row->meta->query_hidden;
  capture->count++;
  return LC_OK;
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

static void test_staged_state_promote_discard_and_reopen(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_state_opts state_opts;
  lc_pouch_put_state_res staged;
  lc_pouch_put_state_res promoted;
  lc_pouch_put_state_res second_staged;
  lc_pouch_put_state_res second_promoted;
  lc_pouch_put_state_res discarded;
  lc_pouch_promote_staged_opts promote_opts;
  lc_pouch_discard_staged_opts discard_opts;
  lc_pouch_state_info info;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "staged");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&state_opts, 0, sizeof(state_opts));
  memset(&staged, 0, sizeof(staged));
  memset(&promoted, 0, sizeof(promoted));
  memset(&second_staged, 0, sizeof(second_staged));
  memset(&second_promoted, 0, sizeof(second_promoted));
  memset(&discarded, 0, sizeof(discarded));
  memset(&promote_opts, 0, sizeof(promote_opts));
  memset(&discard_opts, 0, sizeof(discard_opts));
  memset(&info, 0, sizeof(info));
  store = NULL;
  read_body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(store->stage_state);
  assert_non_null(store->load_staged_state);
  assert_non_null(store->promote_staged_state);
  assert_non_null(store->discard_staged_state);
  assert_non_null(store->list_staged_state);

  state_opts.content_type = "text/plain";
  source = source_from_text("draft-one");
  rc = store->stage_state(store, "default", "lease-key", "txn-1", source,
                          &state_opts, &staged, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_non_null(staged.new_state_etag);

  rc = store->load_staged_state(store, "default", "lease-key", "txn-1",
                                &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  assert_string_equal(info.content_type, "text/plain");
  assert_string_equal(info.etag, staged.new_state_etag);
  text = read_source_text(read_body);
  assert_string_equal(text, "draft-one");
  free(text);
  lc_source_close(read_body);
  read_body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &info);

  rc = store->read_state(store, "default", "lease-key", &read_body, &info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_true(info.no_content);
  assert_null(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);

  rc = store->promote_staged_state(store, "default", "lease-key", "txn-1",
                                   NULL, &promoted, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(promoted.new_state_etag);
  assert_int_equal(promoted.bytes, 9L);

  rc = store->load_staged_state(store, "default", "lease-key", "txn-1",
                                &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(info.no_content);
  assert_null(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);

  rc = store->read_state(store, "default", "lease-key", &read_body, &info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  assert_string_equal(info.etag, promoted.new_state_etag);
  text = read_source_text(read_body);
  assert_string_equal(text, "draft-one");
  free(text);
  lc_source_close(read_body);
  read_body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &info);

  source = source_from_text("draft-two");
  rc = store->stage_state(store, "default", "lease-key", "txn-2", source,
                          &state_opts, &second_staged, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  rc = store->promote_staged_state(store, "default", "lease-key", "txn-2",
                                   NULL, &second_promoted, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 412L);
  lc_error_cleanup(&error);

  promote_opts.expected_head_etag = "wrong";
  rc = store->promote_staged_state(store, "default", "lease-key", "txn-2",
                                   &promote_opts, &second_promoted, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 412L);
  lc_error_cleanup(&error);

  promote_opts.expected_head_etag = promoted.new_state_etag;
  rc = store->promote_staged_state(store, "default", "lease-key", "txn-2",
                                   &promote_opts, &second_promoted, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(second_promoted.new_state_etag);
  assert_string_not_equal(second_promoted.new_state_etag,
                          promoted.new_state_etag);

  source = source_from_text("discard-me");
  rc = store->stage_state(store, "default", "lease-key", "txn-3", source,
                          &state_opts, &discarded, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  discard_opts.expected_etag = "wrong";
  rc = store->discard_staged_state(store, "default", "lease-key", "txn-3",
                                   &discard_opts, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 412L);
  lc_error_cleanup(&error);

  discard_opts.expected_etag = discarded.new_state_etag;
  rc = store->discard_staged_state(store, "default", "lease-key", "txn-3",
                                   &discard_opts, &error);
  assert_int_equal(rc, LC_OK);

  discard_opts.expected_etag = NULL;
  discard_opts.ignore_not_found = 1;
  rc = store->discard_staged_state(store, "default", "lease-key", "txn-3",
                                   &discard_opts, &error);
  assert_int_equal(rc, LC_OK);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  rc = store->read_state(store, "default", "lease-key", &read_body, &info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  assert_string_equal(info.etag, second_promoted.new_state_etag);
  text = read_source_text(read_body);
  assert_string_equal(text, "draft-two");
  free(text);
  lc_source_close(read_body);
  read_body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &info);

  rc = store->load_staged_state(store, "default", "lease-key", "txn-3",
                                &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(info.no_content);
  assert_null(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);

  lc_pouch_put_state_res_cleanup(&allocator, &staged);
  lc_pouch_put_state_res_cleanup(&allocator, &promoted);
  lc_pouch_put_state_res_cleanup(&allocator, &second_staged);
  lc_pouch_put_state_res_cleanup(&allocator, &second_promoted);
  lc_pouch_put_state_res_cleanup(&allocator, &discarded);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_staged_state_listing_orders_paginates_and_replays(
    void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts state_opts;
  lc_pouch_put_state_res staged_alpha;
  lc_pouch_put_state_res staged_bravo;
  lc_pouch_put_state_res staged_charlie;
  lc_pouch_put_state_res staged_other;
  lc_pouch_put_state_res nested;
  lc_pouch_put_state_res promoted;
  lc_pouch_discard_staged_opts discard_opts;
  lc_pouch_list_staged_req req;
  lc_pouch_staged_state_list list;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "staged-list");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&state_opts, 0, sizeof(state_opts));
  memset(&staged_alpha, 0, sizeof(staged_alpha));
  memset(&staged_bravo, 0, sizeof(staged_bravo));
  memset(&staged_charlie, 0, sizeof(staged_charlie));
  memset(&staged_other, 0, sizeof(staged_other));
  memset(&nested, 0, sizeof(nested));
  memset(&promoted, 0, sizeof(promoted));
  memset(&discard_opts, 0, sizeof(discard_opts));
  memset(&req, 0, sizeof(req));
  memset(&list, 0, sizeof(list));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  state_opts.content_type = "text/plain";
  source = source_from_text("bravo");
  rc = store->stage_state(store, "default", "lease-key", "txn-bravo", source,
                          &state_opts, &staged_bravo, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("alpha");
  rc = store->stage_state(store, "default", "lease-key", "txn-alpha", source,
                          &state_opts, &staged_alpha, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("charlie");
  rc = store->stage_state(store, "default", "lease-key", "txn-charlie",
                          source, &state_opts, &staged_charlie, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("other");
  rc = store->stage_state(store, "default", "other-key", "txn-other", source,
                          &state_opts, &staged_other, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("nested");
  rc = store->write_state(store, "default",
                          "lease-key/.staging/txn-nested/attachments/blob",
                          source, &state_opts, &nested, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  discard_opts.expected_etag = staged_bravo.new_state_etag;
  rc = store->discard_staged_state(store, "default", "lease-key", "txn-bravo",
                                   &discard_opts, &error);
  assert_int_equal(rc, LC_OK);

  req.namespace_name = "default";
  req.key = "lease-key";
  req.limit = 1U;
  rc = store->list_staged_state(store, &req, &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 1U);
  assert_true(list.truncated);
  assert_string_equal(list.items[0].key, "lease-key");
  assert_string_equal(list.items[0].txn_id, "txn-alpha");
  assert_string_equal(list.items[0].etag, staged_alpha.new_state_etag);
  assert_string_equal(list.items[0].content_type, "text/plain");
  assert_int_equal(list.items[0].bytes, 5L);
  assert_string_equal(list.next_start_after, "txn-alpha");
  lc_pouch_staged_state_list_cleanup(&allocator, &list);

  req.start_after = "txn-alpha";
  req.limit = 0U;
  rc = store->list_staged_state(store, &req, &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 1U);
  assert_false(list.truncated);
  assert_null(list.next_start_after);
  assert_string_equal(list.items[0].txn_id, "txn-charlie");
  assert_string_equal(list.items[0].etag, staged_charlie.new_state_etag);
  lc_pouch_staged_state_list_cleanup(&allocator, &list);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  req.start_after = NULL;
  req.limit = 0U;
  rc = store->list_staged_state(store, &req, &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 2U);
  assert_string_equal(list.items[0].txn_id, "txn-alpha");
  assert_string_equal(list.items[1].txn_id, "txn-charlie");
  lc_pouch_staged_state_list_cleanup(&allocator, &list);

  rc = store->promote_staged_state(store, "default", "lease-key", "txn-alpha",
                                   NULL, &promoted, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->list_staged_state(store, &req, &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 1U);
  assert_string_equal(list.items[0].txn_id, "txn-charlie");
  lc_pouch_staged_state_list_cleanup(&allocator, &list);

  lc_pouch_put_state_res_cleanup(&allocator, &staged_alpha);
  lc_pouch_put_state_res_cleanup(&allocator, &staged_bravo);
  lc_pouch_put_state_res_cleanup(&allocator, &staged_charlie);
  lc_pouch_put_state_res_cleanup(&allocator, &staged_other);
  lc_pouch_put_state_res_cleanup(&allocator, &nested);
  lc_pouch_put_state_res_cleanup(&allocator, &promoted);
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

static void test_replay_stops_at_corrupt_record_and_discards_later_records(
    void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_state_res first;
  lc_pouch_put_state_res second;
  lc_pouch_put_state_res third;
  lc_pouch_state_info info;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "corrupt");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&first, 0, sizeof(first));
  memset(&second, 0, sizeof(second));
  memset(&third, 0, sizeof(third));
  memset(&info, 0, sizeof(info));
  store = NULL;
  read_body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("stable-prefix");
  rc = store->write_state(store, "default", "good", source, NULL, &first,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("corrupt-middle");
  rc = store->write_state(store, "default", "corrupt", source, NULL, &second,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("later-record");
  rc = store->write_state(store, "default", "later", source, NULL, &third,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  corrupt_first_log_match(root, "corrupt-middle");

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->read_state(store, "default", "good", &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  text = read_source_text(read_body);
  assert_string_equal(text, "stable-prefix");
  free(text);
  lc_source_close(read_body);
  read_body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &info);

  rc = store->read_state(store, "default", "corrupt", &read_body, &info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_true(info.no_content);
  assert_null(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);

  rc = store->read_state(store, "default", "later", &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(info.no_content);
  assert_null(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);

  lc_pouch_put_state_res_cleanup(&allocator, &first);
  lc_pouch_put_state_res_cleanup(&allocator, &second);
  lc_pouch_put_state_res_cleanup(&allocator, &third);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_replay_stops_at_unsupported_record_version(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_state_res first;
  lc_pouch_put_state_res second;
  lc_pouch_put_state_res third;
  lc_pouch_state_info info;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "record-version");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&first, 0, sizeof(first));
  memset(&second, 0, sizeof(second));
  memset(&third, 0, sizeof(third));
  memset(&info, 0, sizeof(info));
  store = NULL;
  read_body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("version-prefix");
  rc = store->write_state(store, "default", "good", source, NULL, &first,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("unsupported-version");
  rc = store->write_state(store, "default", "unsupported", source, NULL,
                          &second, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("version-later");
  rc = store->write_state(store, "default", "later", source, NULL, &third,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  set_first_log_match_record_version(root, "unsupported-version", 99UL);

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->read_state(store, "default", "good", &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  text = read_source_text(read_body);
  assert_string_equal(text, "version-prefix");
  free(text);
  lc_source_close(read_body);
  read_body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &info);

  rc = store->read_state(store, "default", "unsupported", &read_body, &info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_true(info.no_content);
  assert_null(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);

  rc = store->read_state(store, "default", "later", &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(info.no_content);
  assert_null(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);

  lc_pouch_put_state_res_cleanup(&allocator, &first);
  lc_pouch_put_state_res_cleanup(&allocator, &second);
  lc_pouch_put_state_res_cleanup(&allocator, &third);
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

static void test_metadata_scan_orders_paginates_and_replays(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_scan_meta_req req;
  lc_pouch_scan_meta_res scan;
  scan_capture capture;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "meta-scan");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  memset(&req, 0, sizeof(req));
  memset(&scan, 0, sizeof(scan));
  memset(&capture, 0, sizeof(capture));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  meta.owner = "owner";
  meta.lease_id = "lease-b";
  meta.state_etag = "state-b";
  meta.version = 20L;
  meta.has_query_hidden = 1;
  meta.query_hidden = 0;
  rc = store->store_meta(store, "default", "bravo", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-a";
  meta.state_etag = "state-a";
  meta.version = 10L;
  meta.query_hidden = 1;
  rc = store->store_meta(store, "default", "alpha", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-c";
  meta.state_etag = "state-c";
  meta.version = 30L;
  meta.query_hidden = 0;
  rc = store->store_meta(store, "other", "aardvark", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-d";
  meta.state_etag = "state-d";
  meta.version = 40L;
  rc = store->store_meta(store, "default", "charlie", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  rc = store->delete_meta(store, "default", "charlie", stored.etag, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  req.namespace_name = "default";
  req.limit = 1U;
  rc = store->scan_meta(store, &req, capture_scan_row, &capture, &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1U);
  assert_string_equal(capture.keys[0], "alpha");
  assert_int_equal(capture.versions[0], 10L);
  assert_true(capture.query_hidden[0]);
  assert_true(scan.truncated);
  assert_string_equal(scan.next_start_after, "alpha");
  lc_pouch_scan_meta_res_cleanup(&allocator, &scan);

  memset(&capture, 0, sizeof(capture));
  req.start_after = "alpha";
  req.limit = 8U;
  rc = store->scan_meta(store, &req, capture_scan_row, &capture, &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1U);
  assert_string_equal(capture.keys[0], "bravo");
  assert_int_equal(capture.versions[0], 20L);
  assert_false(capture.query_hidden[0]);
  assert_false(scan.truncated);
  assert_null(scan.next_start_after);
  lc_pouch_scan_meta_res_cleanup(&allocator, &scan);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  memset(&capture, 0, sizeof(capture));
  req.start_after = NULL;
  req.limit = 0U;
  rc = store->scan_meta(store, &req, capture_scan_row, &capture, &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 2U);
  assert_string_equal(capture.keys[0], "alpha");
  assert_string_equal(capture.keys[1], "bravo");
  assert_false(scan.truncated);
  lc_pouch_scan_meta_res_cleanup(&allocator, &scan);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_object_roundtrip_overwrite_delete_and_reopen(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_object_opts opts;
  lc_pouch_object_info first;
  lc_pouch_object_info copied;
  lc_pouch_object_info fetched;
  lc_pouch_object_list list;
  lc_pouch_object_selector selector;
  lc_pouch_copy_object_opts copy_opts;
  lc_error error;
  char *text;
  int deleted;
  int deleted_count;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "objects");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  memset(&first, 0, sizeof(first));
  memset(&copied, 0, sizeof(copied));
  memset(&fetched, 0, sizeof(fetched));
  memset(&list, 0, sizeof(list));
  memset(&selector, 0, sizeof(selector));
  memset(&copy_opts, 0, sizeof(copy_opts));
  store = NULL;
  read_body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  opts.name = "result.txt";
  opts.content_type = "text/plain";
  opts.prevent_overwrite = 1;
  opts.has_max_bytes = 1;
  opts.max_bytes = 32L;
  source = source_from_text("payload-one");
  rc = store->put_object(store, "default", "lease-key", source, &opts, &first,
                         &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_non_null(first.id);
  assert_string_equal(first.name, "result.txt");
  assert_string_equal(first.content_type, "text/plain");
  assert_int_equal(first.size, 11L);

  source = source_from_text("payload-two");
  rc = store->put_object(store, "default", "lease-key", source, &opts, &fetched,
                         &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 409L);
  lc_error_cleanup(&error);
  lc_pouch_object_info_cleanup(&allocator, &fetched);

  rc = store->list_objects(store, "default", "lease-key", &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 1U);
  assert_string_equal(list.items[0].id, first.id);
  assert_string_equal(list.items[0].name, "result.txt");
  lc_pouch_object_list_cleanup(&allocator, &list);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  selector.name = "result.txt";
  rc = store->get_object(store, "default", "lease-key", &selector, &read_body,
                         &fetched, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(fetched.id, first.id);
  text = read_source_text(read_body);
  assert_string_equal(text, "payload-one");
  free(text);
  lc_source_close(read_body);
  lc_pouch_object_info_cleanup(&allocator, &fetched);

  copy_opts.source.name = "result.txt";
  copy_opts.prevent_overwrite = 1;
  rc = store->copy_object(store, "default", "lease-key", "copy-key",
                          &copy_opts, &copied, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(copied.name, "result.txt");
  assert_string_equal(copied.id, first.id);
  assert_string_equal(copied.content_type, "text/plain");
  assert_int_equal(copied.size, 11L);

  rc = store->copy_object(store, "default", "lease-key", "copy-key",
                          &copy_opts, &fetched, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 409L);
  lc_error_cleanup(&error);
  lc_pouch_object_info_cleanup(&allocator, &fetched);

  selector.name = "result.txt";
  rc = store->get_object(store, "default", "copy-key", &selector, &read_body,
                         &fetched, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(fetched.id, first.id);
  assert_string_equal(fetched.content_type, "text/plain");
  text = read_source_text(read_body);
  assert_string_equal(text, "payload-one");
  free(text);
  lc_source_close(read_body);
  lc_pouch_object_info_cleanup(&allocator, &fetched);

  rc = store->delete_object(store, "default", "lease-key", &selector, &deleted,
                            &error);
  assert_int_equal(rc, LC_OK);
  assert_true(deleted);
  rc = store->delete_object(store, "default", "lease-key", &selector, &deleted,
                            &error);
  assert_int_equal(rc, LC_OK);
  assert_false(deleted);

  rc = store->list_objects(store, "default", "lease-key", &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 0U);

  opts.prevent_overwrite = 0;
  source = source_from_text("payload-two");
  rc = store->put_object(store, "default", "lease-key", source, &opts, &fetched,
                         &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_object_info_cleanup(&allocator, &fetched);
  rc = store->delete_all_objects(store, "default", "lease-key", &deleted_count,
                                 &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(deleted_count, 1);

  lc_pouch_object_info_cleanup(&allocator, &first);
  lc_pouch_object_info_cleanup(&allocator, &copied);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_queue_enqueue_dequeue_nack_ack_and_reopen(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *payload;
  lc_pouch_enqueue_opts enqueue_opts;
  lc_pouch_dequeue_opts dequeue_opts;
  lc_pouch_queue_message_info enqueued;
  lc_pouch_queue_message_info dequeued;
  lc_pouch_queue_message_info updated;
  lc_pouch_queue_ref ref;
  lc_pouch_queue_stats stats;
  lc_error error;
  char *text;
  int acked;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&dequeue_opts, 0, sizeof(dequeue_opts));
  memset(&enqueued, 0, sizeof(enqueued));
  memset(&dequeued, 0, sizeof(dequeued));
  memset(&updated, 0, sizeof(updated));
  memset(&ref, 0, sizeof(ref));
  memset(&stats, 0, sizeof(stats));
  store = NULL;
  payload = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  enqueue_opts.content_type = "text/plain";
  enqueue_opts.visibility_timeout_seconds = 30L;
  enqueue_opts.ttl_seconds = 3600L;
  enqueue_opts.max_attempts = 3;
  source = source_from_text("queued-payload");
  rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                              &enqueued, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(enqueued.queue, "jobs");
  assert_non_null(enqueued.message_id);
  assert_int_equal(enqueued.payload_bytes, 14L);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  rc = store->queue_stats(store, "default", "jobs", &stats, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats.available, 1);
  assert_string_equal(stats.head_message_id, enqueued.message_id);
  lc_pouch_queue_stats_cleanup(&allocator, &stats);

  dequeue_opts.owner = "worker-a";
  dequeue_opts.visibility_timeout_seconds = 45L;
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts, &payload,
                              &dequeued, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(payload);
  assert_int_equal(dequeued.attempts, 1);
  assert_non_null(dequeued.lease_id);
  text = read_source_text(payload);
  assert_string_equal(text, "queued-payload");
  free(text);
  lc_source_close(payload);

  ref.namespace_name = dequeued.namespace_name;
  ref.queue = dequeued.queue;
  ref.message_id = dequeued.message_id;
  ref.lease_id = dequeued.lease_id;
  ref.txn_id = dequeued.txn_id;
  ref.fencing_token = dequeued.fencing_token;
  ref.meta_etag = dequeued.meta_etag;
  rc = store->nack_message(store, &ref, 0L, 1, &updated, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(updated.failure_attempts, 1);
  lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);

  dequeue_opts.owner = "worker-b";
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts, &payload,
                              &dequeued, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(payload);
  assert_int_equal(dequeued.attempts, 2);
  lc_source_close(payload);

  ref.namespace_name = dequeued.namespace_name;
  ref.queue = dequeued.queue;
  ref.message_id = dequeued.message_id;
  ref.lease_id = dequeued.lease_id;
  ref.txn_id = dequeued.txn_id;
  ref.fencing_token = dequeued.fencing_token;
  ref.meta_etag = dequeued.meta_etag;
  rc = store->ack_message(store, &ref, &acked, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(acked);

  rc = store->queue_stats(store, "default", "jobs", &stats, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats.available, 0);

  lc_pouch_queue_stats_cleanup(&allocator, &stats);
  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);
  lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);
  lc_pouch_queue_message_info_cleanup(&allocator, &updated);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_backend_hash_persists_across_handles(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *first;
  lc_pouch_store *second;
  lc_pouch_object_selector selector;
  lc_pouch_object_info info;
  lc_source *body;
  lc_error error;
  char *first_hash;
  char *second_hash;
  char *stored_hash;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "backend-hash");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&selector, 0, sizeof(selector));
  memset(&info, 0, sizeof(info));
  first = NULL;
  second = NULL;
  body = NULL;
  first_hash = NULL;
  second_hash = NULL;
  stored_hash = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &first, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_disk_open(root, &allocator, &second, &error);
  assert_int_equal(rc, LC_OK);

  rc = first->backend_hash(first, &first_hash, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(first_hash);
  assert_true(strncmp(first_hash, "pouch-", 6U) == 0);

  rc = second->backend_hash(second, &second_hash, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(second_hash, first_hash);

  rc = first->close(first, &error);
  assert_int_equal(rc, LC_OK);
  first = NULL;
  rc = lc_pouch_disk_open(root, &allocator, &first, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_free(&allocator, second_hash);
  second_hash = NULL;
  rc = first->backend_hash(first, &second_hash, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(second_hash, first_hash);

  selector.name = "backend-id";
  rc = first->get_object(first, ".lockd", "backend-id", &selector, &body,
                         &info, &error);
  assert_int_equal(rc, LC_OK);
  stored_hash = read_source_text(body);
  assert_string_equal(stored_hash, first_hash);
  free(stored_hash);
  stored_hash = NULL;
  lc_source_close(body);
  body = NULL;
  lc_pouch_object_info_cleanup(&allocator, &info);

  lc_pouch_free(&allocator, first_hash);
  lc_pouch_free(&allocator, second_hash);
  rc = second->close(second, &error);
  assert_int_equal(rc, LC_OK);
  rc = first->close(first, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_write_read_reopen_and_allocator_hooks),
      cmocka_unit_test(test_cas_and_remove_semantics),
      cmocka_unit_test(test_staged_state_promote_discard_and_reopen),
      cmocka_unit_test(test_staged_state_listing_orders_paginates_and_replays),
      cmocka_unit_test(test_replay_truncates_trailing_partial_record),
      cmocka_unit_test(
          test_replay_stops_at_corrupt_record_and_discards_later_records),
      cmocka_unit_test(test_replay_stops_at_unsupported_record_version),
      cmocka_unit_test(test_metadata_roundtrip_cas_delete_and_reopen),
      cmocka_unit_test(test_metadata_scan_orders_paginates_and_replays),
      cmocka_unit_test(test_object_roundtrip_overwrite_delete_and_reopen),
      cmocka_unit_test(test_queue_enqueue_dequeue_nack_ack_and_reopen),
      cmocka_unit_test(test_backend_hash_persists_across_handles),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}

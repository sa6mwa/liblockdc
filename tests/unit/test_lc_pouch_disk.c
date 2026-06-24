#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cmocka.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "lc_pouch_store.h"

#define TEST_POUCH_HEADER_SIZE 64U
#define TEST_POUCH_HEADER_BODY_LENGTH_OFFSET 28U
#define TEST_POUCH_HEADER_PAYLOAD_LENGTH_OFFSET 44U
#define TEST_POUCH_HEADER_RECORD_VERSION_OFFSET 56U
#define TEST_POUCH_QUERY_INDEX_HEADER_SIZE 64U
#define TEST_POUCH_QUERY_INDEX_RECORD_META 1U
#define TEST_POUCH_RECORD_STATE_PUT 1U
#define TEST_POUCH_RECORD_STATE_REMOVE 2U
#define TEST_POUCH_RECORD_META_PUT 3U
#define TEST_POUCH_RECORD_OBJECT_PUT 5U
#define TEST_POUCH_RECORD_QUEUE_PUT 7U
#define TEST_POUCH_RECORD_STATE_LINK 10U
#define TEST_POUCH_MAX_INLINE_BODY_BYTES (64UL * 1024UL * 1024UL)
#define TEST_POUCH_WRITER_MARKER_PREFIX "writer-presence-"
#define TEST_POUCH_QUEUE_WAKE_PREFIX "queue-wake-"

typedef struct tracked_allocator {
  size_t malloc_calls;
  size_t realloc_calls;
  size_t free_calls;
  size_t max_malloc_size;
  size_t max_realloc_size;
  size_t fail_malloc_size;
  size_t fail_realloc_size;
} tracked_allocator;

typedef struct counting_source {
  lc_source pub;
  size_t length;
  size_t position;
} counting_source;

static void *tracked_malloc(void *context, size_t size) {
  tracked_allocator *tracked;

  tracked = (tracked_allocator *)context;
  tracked->malloc_calls++;
  if (size > tracked->max_malloc_size) {
    tracked->max_malloc_size = size;
  }
  if (tracked->fail_malloc_size != 0U &&
      size == tracked->fail_malloc_size) {
    return NULL;
  }
  return malloc(size);
}

static void *tracked_realloc(void *context, void *ptr, size_t size) {
  tracked_allocator *tracked;

  tracked = (tracked_allocator *)context;
  tracked->realloc_calls++;
  if (size > tracked->max_realloc_size) {
    tracked->max_realloc_size = size;
  }
  if (tracked->fail_realloc_size != 0U &&
      size == tracked->fail_realloc_size) {
    return NULL;
  }
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

static int test_is_writer_marker(const char *name) {
  size_t prefix_len;
  size_t name_len;
  size_t marker_len;

  prefix_len = strlen(TEST_POUCH_WRITER_MARKER_PREFIX);
  marker_len = strlen(".marker");
  name_len = strlen(name);
  return name_len > prefix_len + marker_len &&
         strncmp(name, TEST_POUCH_WRITER_MARKER_PREFIX, prefix_len) == 0 &&
         strcmp(name + name_len - marker_len, ".marker") == 0;
}

static int test_is_queue_wake_marker(const char *name) {
  size_t prefix_len;
  size_t name_len;
  size_t marker_len;

  prefix_len = strlen(TEST_POUCH_QUEUE_WAKE_PREFIX);
  marker_len = strlen(".marker");
  name_len = strlen(name);
  return name_len > prefix_len + marker_len &&
         strncmp(name, TEST_POUCH_QUEUE_WAKE_PREFIX, prefix_len) == 0 &&
         strcmp(name + name_len - marker_len, ".marker") == 0;
}

static unsigned long test_queue_wake_hash(const char *namespace_name,
                                          const char *queue) {
  const unsigned char *cursor;
  unsigned long hash;

  hash = 2166136261UL;
  cursor = (const unsigned char *)namespace_name;
  while (*cursor != '\0') {
    hash ^= (unsigned long)*cursor++;
    hash *= 16777619UL;
  }
  hash ^= 0xffUL;
  hash *= 16777619UL;
  cursor = (const unsigned char *)queue;
  while (*cursor != '\0') {
    hash ^= (unsigned long)*cursor++;
    hash *= 16777619UL;
  }
  return hash;
}

static void test_queue_wake_marker_path(const char *root,
                                        const char *namespace_name,
                                        const char *queue, char *path,
                                        size_t path_size) {
  snprintf(path, path_size, "%s/%s%08lx.marker", root,
           TEST_POUCH_QUEUE_WAKE_PREFIX,
           test_queue_wake_hash(namespace_name, queue));
}

static size_t test_count_writer_markers(const char *root) {
  DIR *dir;
  struct dirent *entry;
  size_t count;

  dir = opendir(root);
  if (dir == NULL) {
    return 0U;
  }
  count = 0U;
  while ((entry = readdir(dir)) != NULL) {
    if (test_is_writer_marker(entry->d_name)) {
      count++;
    }
  }
  closedir(dir);
  return count;
}

static size_t test_count_queue_wake_markers(const char *root) {
  DIR *dir;
  struct dirent *entry;
  size_t count;

  dir = opendir(root);
  if (dir == NULL) {
    return 0U;
  }
  count = 0U;
  while ((entry = readdir(dir)) != NULL) {
    if (test_is_queue_wake_marker(entry->d_name)) {
      count++;
    }
  }
  closedir(dir);
  return count;
}

static int test_first_writer_marker_path(const char *root, char *path,
                                         size_t path_size) {
  DIR *dir;
  struct dirent *entry;
  int found;

  dir = opendir(root);
  if (dir == NULL) {
    return 0;
  }
  found = 0;
  while ((entry = readdir(dir)) != NULL) {
    if (test_is_writer_marker(entry->d_name)) {
      snprintf(path, path_size, "%s/%s", root, entry->d_name);
      found = 1;
      break;
    }
  }
  closedir(dir);
  return found;
}

static void test_read_file_text(const char *path, char *buffer,
                                size_t buffer_size) {
  int fd;
  ssize_t got;

  assert_true(buffer_size > 0U);
  fd = open(path, O_RDONLY);
  assert_true(fd >= 0);
  got = read(fd, buffer, buffer_size - 1U);
  assert_true(got >= 0);
  buffer[(size_t)got] = '\0';
  assert_int_equal(close(fd), 0);
}

static void test_remove_writer_markers(const char *root) {
  DIR *dir;
  struct dirent *entry;
  char path[512];

  dir = opendir(root);
  if (dir == NULL) {
    return;
  }
  while ((entry = readdir(dir)) != NULL) {
    if (test_is_writer_marker(entry->d_name)) {
      snprintf(path, sizeof(path), "%s/%s", root, entry->d_name);
      if (unlink(path) != 0) {
        rmdir(path);
      }
    }
  }
  closedir(dir);
}

static void test_remove_queue_wake_markers(const char *root) {
  DIR *dir;
  struct dirent *entry;
  char path[512];

  dir = opendir(root);
  if (dir == NULL) {
    return;
  }
  while ((entry = readdir(dir)) != NULL) {
    if (test_is_queue_wake_marker(entry->d_name)) {
      snprintf(path, sizeof(path), "%s/%s", root, entry->d_name);
      if (unlink(path) != 0) {
        rmdir(path);
      }
    }
  }
  closedir(dir);
}

static void test_cleanup_root(const char *root) {
  char path[512];

  test_remove_writer_markers(root);
  test_remove_queue_wake_markers(root);
  snprintf(path, sizeof(path), "%s/store.compact.tmp", root);
  unlink(path);
  snprintf(path, sizeof(path), "%s/query.index.compact.tmp", root);
  unlink(path);
  snprintf(path, sizeof(path), "%s/store.log", root);
  unlink(path);
  snprintf(path, sizeof(path), "%s/writer.lock", root);
  unlink(path);
  snprintf(path, sizeof(path), "%s/query.index", root);
  unlink(path);
  rmdir(root);
}

static void test_write_marker_file(const char *path) {
  static const char marker[] = "stale";
  int fd;

  fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0666);
  assert_true(fd >= 0);
  assert_int_equal(write(fd, marker, sizeof(marker) - 1U),
                   (ssize_t)(sizeof(marker) - 1U));
  assert_int_equal(close(fd), 0);
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

static size_t counting_source_read(lc_source *self, void *buffer, size_t count,
                                   lc_error *error) {
  counting_source *source;
  size_t remaining;
  size_t produced;

  (void)error;
  source = (counting_source *)self;
  if (source->position >= source->length) {
    return 0U;
  }
  remaining = source->length - source->position;
  produced = remaining < count ? remaining : count;
  memset(buffer, 'x', produced);
  source->position += produced;
  return produced;
}

static int counting_source_reset(lc_source *self, lc_error *error) {
  counting_source *source;

  (void)error;
  source = (counting_source *)self;
  source->position = 0U;
  return LC_OK;
}

static void counting_source_close(lc_source *self) { (void)self; }

static void counting_source_init(counting_source *source, size_t length) {
  memset(source, 0, sizeof(*source));
  source->pub.read = counting_source_read;
  source->pub.reset = counting_source_reset;
  source->pub.close = counting_source_close;
  source->length = length;
}

static size_t read_source_count_x(lc_source *source) {
  char buffer[4096];
  size_t total;
  size_t got;
  size_t index;
  lc_error error;

  memset(&error, 0, sizeof(error));
  total = 0U;
  for (;;) {
    got = source->read(source, buffer, sizeof(buffer), &error);
    assert_int_equal(error.code, LC_OK);
    if (got == 0U) {
      break;
    }
    for (index = 0U; index < got; ++index) {
      assert_int_equal(buffer[index], 'x');
    }
    total += got;
  }
  lc_error_cleanup(&error);
  return total;
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

static size_t count_log_records_of_type(const char *root, unsigned long type) {
  char log_path[512];
  unsigned char header[TEST_POUCH_HEADER_SIZE];
  unsigned long payload_len;
  unsigned long record_type;
  size_t count;
  ssize_t got;
  int fd;

  snprintf(log_path, sizeof(log_path), "%s/store.log", root);
  fd = open(log_path, O_RDONLY);
  assert_true(fd >= 0);
  count = 0U;
  for (;;) {
    got = read(fd, header, sizeof(header));
    if (got == 0) {
      break;
    }
    if (got != (ssize_t)sizeof(header)) {
      break;
    }
    if (memcmp(header, "LCP1", 4U) != 0) {
      break;
    }
    record_type = test_get_u32(header + 8);
    payload_len = test_get_u64(header + 44);
    if (record_type == type) {
      count++;
    }
    if (lseek(fd, (off_t)payload_len, SEEK_CUR) < 0) {
      break;
    }
  }
  close(fd);
  return count;
}

static size_t count_query_index_records_of_type(const char *root,
                                                unsigned long type) {
  char index_path[512];
  unsigned char header[TEST_POUCH_QUERY_INDEX_HEADER_SIZE];
  unsigned long payload_len;
  unsigned long record_type;
  size_t count;
  ssize_t got;
  int fd;

  snprintf(index_path, sizeof(index_path), "%s/query.index", root);
  fd = open(index_path, O_RDONLY);
  assert_true(fd >= 0);
  count = 0U;
  for (;;) {
    got = read(fd, header, sizeof(header));
    if (got == 0) {
      break;
    }
    if (got != (ssize_t)sizeof(header)) {
      break;
    }
    if (memcmp(header, "LCQI", 4U) != 0) {
      break;
    }
    record_type = test_get_u32(header + 8);
    payload_len = test_get_u64(header + 44);
    if (record_type == type) {
      count++;
    }
    if (lseek(fd, (off_t)payload_len, SEEK_CUR) < 0) {
      break;
    }
  }
  close(fd);
  return count;
}

static off_t test_log_size(const char *root) {
  char log_path[512];
  struct stat st;

  snprintf(log_path, sizeof(log_path), "%s/store.log", root);
  assert_int_equal(stat(log_path, &st), 0);
  return st.st_size;
}

static off_t test_query_index_size(const char *root) {
  char index_path[512];
  struct stat st;

  snprintf(index_path, sizeof(index_path), "%s/query.index", root);
  assert_int_equal(stat(index_path, &st), 0);
  return st.st_size;
}

static void truncate_query_index_tail(const char *root, off_t remove_bytes) {
  char index_path[512];
  off_t size;
  int fd;

  snprintf(index_path, sizeof(index_path), "%s/query.index", root);
  size = test_query_index_size(root);
  assert_true(remove_bytes > 0);
  assert_true(size > remove_bytes);
  fd = open(index_path, O_WRONLY);
  assert_true(fd >= 0);
  assert_int_equal(ftruncate(fd, size - remove_bytes), 0);
  close(fd);
}

static void truncate_store_log(const char *root) {
  char log_path[512];
  int fd;

  snprintf(log_path, sizeof(log_path), "%s/store.log", root);
  fd = open(log_path, O_WRONLY | O_TRUNC);
  assert_true(fd >= 0);
  close(fd);
}

static void chmod_store_log(const char *root, mode_t mode) {
  char log_path[512];

  snprintf(log_path, sizeof(log_path), "%s/store.log", root);
  assert_int_equal(chmod(log_path, mode), 0);
}

static void test_put_u32(unsigned char *dst, unsigned long value) {
  dst[0] = (unsigned char)(value & 255UL);
  dst[1] = (unsigned char)((value >> 8) & 255UL);
  dst[2] = (unsigned char)((value >> 16) & 255UL);
  dst[3] = (unsigned char)((value >> 24) & 255UL);
}

static void test_put_u64(unsigned char *dst, unsigned long value) {
  test_put_u32(dst, value & 0xffffffffUL);
  test_put_u32(dst + 4, (value >> 32) & 0xffffffffUL);
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

static void corrupt_first_query_index_match(const char *root,
                                            const char *needle) {
  char index_path[512];
  unsigned char *bytes;
  size_t needle_len;
  size_t index;
  struct stat st;
  int fd;
  int found;

  snprintf(index_path, sizeof(index_path), "%s/query.index", root);
  fd = open(index_path, O_RDWR);
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

static void set_first_log_match_body_length(const char *root,
                                            const char *needle,
                                            unsigned long body_length) {
  char log_path[512];
  unsigned char header[TEST_POUCH_HEADER_SIZE];
  unsigned char *payload;
  size_t needle_len;
  unsigned long ns_len;
  unsigned long key_len;
  unsigned long ct_len;
  unsigned long etag_len;
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
    payload_len = test_get_u64(header + TEST_POUCH_HEADER_PAYLOAD_LENGTH_OFFSET);
    payload = (unsigned char *)malloc((size_t)payload_len);
    assert_non_null(payload);
    assert_int_equal(read(fd, payload, (size_t)payload_len), payload_len);
    if (payload_len >= needle_len) {
      size_t index;

      for (index = 0U; index + needle_len <= payload_len; ++index) {
        if (memcmp(payload + index, needle, needle_len) == 0) {
          ns_len = test_get_u32(header + 12);
          key_len = test_get_u32(header + 16);
          ct_len = test_get_u32(header + 20);
          etag_len = test_get_u32(header + 24);
          payload_len = ns_len + key_len + ct_len + etag_len + body_length;
          test_put_u64(header + TEST_POUCH_HEADER_BODY_LENGTH_OFFSET,
                       body_length);
          test_put_u64(header + TEST_POUCH_HEADER_PAYLOAD_LENGTH_OFFSET,
                       payload_len);
          assert_int_equal(
              lseek(fd,
                    record_offset + TEST_POUCH_HEADER_BODY_LENGTH_OFFSET,
                    SEEK_SET),
              record_offset + TEST_POUCH_HEADER_BODY_LENGTH_OFFSET);
          assert_int_equal(
              write(fd, header + TEST_POUCH_HEADER_BODY_LENGTH_OFFSET, 8U),
              8);
          assert_int_equal(
              lseek(fd,
                    record_offset + TEST_POUCH_HEADER_PAYLOAD_LENGTH_OFFSET,
                    SEEK_SET),
              record_offset + TEST_POUCH_HEADER_PAYLOAD_LENGTH_OFFSET);
          assert_int_equal(
              write(fd, header + TEST_POUCH_HEADER_PAYLOAD_LENGTH_OFFSET, 8U),
              8);
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

static void truncate_log_after_first_record(const char *root) {
  char log_path[512];
  unsigned char header[TEST_POUCH_HEADER_SIZE];
  unsigned long payload_len;
  off_t truncate_at;
  int fd;

  snprintf(log_path, sizeof(log_path), "%s/store.log", root);
  fd = open(log_path, O_RDWR);
  assert_true(fd >= 0);
  assert_int_equal(read(fd, header, sizeof(header)), sizeof(header));
  assert_memory_equal(header, "LCP1", 4U);
  payload_len = test_get_u64(header + 44);
  truncate_at = (off_t)(TEST_POUCH_HEADER_SIZE + payload_len);
  assert_int_equal(ftruncate(fd, truncate_at), 0);
  close(fd);
}

typedef struct scan_capture {
  char keys[8][64];
  long versions[8];
  int query_hidden[8];
  size_t count;
} scan_capture;

typedef struct key_capture {
  char keys[8][64];
  size_t count;
} key_capture;

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

static int capture_query_key(void *context, const char *key, lc_error *error) {
  key_capture *capture;

  (void)error;
  capture = (key_capture *)context;
  assert_non_null(key);
  assert_true(capture->count < sizeof(capture->keys) / sizeof(capture->keys[0]));
  snprintf(capture->keys[capture->count],
           sizeof(capture->keys[capture->count]), "%s", key);
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

static void test_state_read_skips_replay_after_same_handle_write(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  counting_source source;
  lc_source *read_body;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res put_res;
  lc_pouch_state_info info;
  lc_error error;
  size_t payload_length;
  size_t read_length;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "state-no-replay");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&info, 0, sizeof(info));
  store = NULL;
  read_body = NULL;
  payload_length = 128U * 1024U;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  tracked.max_malloc_size = 0U;
  tracked.max_realloc_size = 0U;
  counting_source_init(&source, payload_length);
  opts.content_type = "application/octet-stream";
  rc = store->write_state(store, "default", "large", &source.pub, &opts,
                          &put_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(put_res.bytes, (long)payload_length);
  assert_true(tracked.max_malloc_size < payload_length);
  assert_true(tracked.max_realloc_size < payload_length);

  tracked.max_malloc_size = 0U;
  tracked.max_realloc_size = 0U;
  rc = store->read_state(store, "default", "large", &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(read_body);
  assert_false(info.no_content);
  assert_int_equal(info.version, put_res.new_version);
  assert_true(tracked.max_malloc_size < payload_length);
  assert_true(tracked.max_realloc_size < payload_length);
  read_length = read_source_count_x(read_body);
  assert_int_equal(read_length, payload_length);

  lc_source_close(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
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
  lc_pouch_put_state_res third;
  lc_pouch_put_state_res stale;
  lc_pouch_state_info info;
  lc_error error;
  char *text;
  int removed;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "cas");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  memset(&first, 0, sizeof(first));
  memset(&second, 0, sizeof(second));
  memset(&third, 0, sizeof(third));
  memset(&stale, 0, sizeof(stale));
  memset(&info, 0, sizeof(info));
  store = NULL;
  read_body = NULL;
  text = NULL;
  removed = 0;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  rc = store->remove_state(store, "default", "beta", NULL, &removed, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(removed);

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

  rc = store->remove_state(store, "default", "beta", "wrong", &removed,
                           &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  lc_error_cleanup(&error);

  rc = store->remove_state(store, "default", "beta", second.new_state_etag,
                           &removed, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(removed);

  rc = store->remove_state(store, "default", "beta", NULL, &removed, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(removed);

  rc = store->remove_state(store, "default", "beta", second.new_state_etag,
                           &removed, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 412L);
  lc_error_cleanup(&error);

  rc = store->read_state(store, "default", "beta", &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(info.no_content);
  assert_null(read_body);

  memset(&opts, 0, sizeof(opts));
  opts.has_if_version = 1;
  opts.if_version = second.new_version;
  source = source_from_text("stale");
  rc = store->write_state(store, "default", "beta", source, &opts, &third,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 412L);
  lc_error_cleanup(&error);

  memset(&opts, 0, sizeof(opts));
  source = source_from_text("three");
  rc = store->write_state(store, "default", "beta", source, &opts, &third,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(third.new_version, 4L);
  assert_string_not_equal(third.new_state_etag, second.new_state_etag);

  memset(&opts, 0, sizeof(opts));
  opts.has_if_version = 1;
  opts.if_version = second.new_version;
  source = source_from_text("stale-after-recreate");
  rc = store->write_state(store, "default", "beta", source, &opts, &stale,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 412L);
  lc_error_cleanup(&error);

  lc_pouch_state_info_cleanup(&allocator, &info);
  memset(&info, 0, sizeof(info));
  read_body = NULL;
  rc = store->read_state(store, "default", "beta", &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  assert_string_equal(info.etag, third.new_state_etag);
  assert_int_equal(info.version, third.new_version);
  assert_non_null(read_body);
  text = read_source_text(read_body);
  assert_string_equal(text, "three");
  free(text);
  lc_source_close(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  read_body = NULL;
  memset(&info, 0, sizeof(info));
  rc = store->read_state(store, "default", "beta", &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  assert_string_equal(info.etag, third.new_state_etag);
  assert_int_equal(info.version, third.new_version);
  text = read_source_text(read_body);
  assert_string_equal(text, "three");
  free(text);
  lc_source_close(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);

  lc_pouch_put_state_res_cleanup(&allocator, &first);
  lc_pouch_put_state_res_cleanup(&allocator, &second);
  lc_pouch_put_state_res_cleanup(&allocator, &third);
  lc_pouch_put_state_res_cleanup(&allocator, &stale);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_state_write_index_allocation_failure_replays_cleanly(
    void **state) {
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
  char *text;
  const char *replacement_type;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "state-write-nomem");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  memset(&first, 0, sizeof(first));
  memset(&second, 0, sizeof(second));
  memset(&info, 0, sizeof(info));
  store = NULL;
  read_body = NULL;
  replacement_type = "application/x-pouch-state-index-replay";

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  opts.content_type = "text/plain";
  source = source_from_text("payload-one");
  rc = store->write_state(store, "default", "state-key", source, &opts,
                          &first, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  opts.content_type = replacement_type;
  tracked.fail_malloc_size = strlen(replacement_type) + 1U;
  source = source_from_text("payload-two");
  rc = store->write_state(store, "default", "state-key", source, &opts,
                          &second, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_NOMEM);
  tracked.fail_malloc_size = 0U;
  lc_error_cleanup(&error);
  memset(&error, 0, sizeof(error));

  rc = store->read_state(store, "default", "state-key", &read_body, &info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  assert_int_equal(info.version, 2L);
  assert_string_equal(info.content_type, replacement_type);
  assert_string_not_equal(info.etag, first.new_state_etag);
  text = read_source_text(read_body);
  assert_string_equal(text, "payload-two");
  free(text);
  lc_source_close(read_body);

  lc_pouch_state_info_cleanup(&allocator, &info);
  lc_pouch_put_state_res_cleanup(&allocator, &second);
  lc_pouch_put_state_res_cleanup(&allocator, &first);
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
  assert_string_equal(promoted.new_state_etag, staged.new_state_etag);
  assert_int_equal(promoted.bytes, 9L);
  assert_int_equal(count_log_records_of_type(root, TEST_POUCH_RECORD_STATE_LINK),
                   1U);

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
  assert_string_equal(second_promoted.new_state_etag,
                      second_staged.new_state_etag);
  assert_string_not_equal(second_promoted.new_state_etag,
                          promoted.new_state_etag);
  assert_int_equal(count_log_records_of_type(root, TEST_POUCH_RECORD_STATE_LINK),
                   2U);

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

static void test_replay_rebuilds_indexes_after_external_truncation(
    void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_state_res first;
  lc_pouch_put_state_res second;
  lc_pouch_state_info info;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "replay-reset");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&first, 0, sizeof(first));
  memset(&second, 0, sizeof(second));
  memset(&info, 0, sizeof(info));
  store = NULL;
  read_body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("durable-head");
  rc = store->write_state(store, "default", "reset-key", source, NULL, &first,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("truncated-head");
  rc = store->write_state(store, "default", "reset-key", source, NULL, &second,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(second.new_version, first.new_version + 1L);

  truncate_log_after_first_record(root);

  rc = store->read_state(store, "default", "reset-key", &read_body, &info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  assert_int_equal(info.version, first.new_version);
  assert_string_equal(info.etag, first.new_state_etag);
  text = read_source_text(read_body);
  assert_string_equal(text, "durable-head");
  free(text);
  lc_source_close(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);
  lc_pouch_put_state_res_cleanup(&allocator, &first);
  lc_pouch_put_state_res_cleanup(&allocator, &second);
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

static void test_replay_stops_at_oversized_record_without_allocating_payload(
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
  test_root_path(root, sizeof(root), "oversized-replay");
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
  source = source_from_text("oversized-prefix");
  rc = store->write_state(store, "default", "good", source, NULL, &first,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("oversized-middle");
  rc = store->write_state(store, "default", "oversized", source, NULL,
                          &second, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("oversized-later");
  rc = store->write_state(store, "default", "later", source, NULL, &third,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  set_first_log_match_body_length(root, "oversized-middle",
                                  TEST_POUCH_MAX_INLINE_BODY_BYTES + 1UL);

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->read_state(store, "default", "good", &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  text = read_source_text(read_body);
  assert_string_equal(text, "oversized-prefix");
  free(text);
  lc_source_close(read_body);
  read_body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &info);

  rc = store->read_state(store, "default", "oversized", &read_body, &info,
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

static void test_replay_stops_at_truncated_object_metadata(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_state_res first;
  lc_pouch_put_state_res later;
  lc_pouch_put_object_opts object_opts;
  lc_pouch_object_info object_info;
  lc_pouch_object_list list;
  lc_pouch_state_info state_info;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "object-truncated-replay");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&first, 0, sizeof(first));
  memset(&later, 0, sizeof(later));
  memset(&object_opts, 0, sizeof(object_opts));
  memset(&object_info, 0, sizeof(object_info));
  memset(&list, 0, sizeof(list));
  memset(&state_info, 0, sizeof(state_info));
  store = NULL;
  read_body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("object-prefix");
  rc = store->write_state(store, "default", "good", source, NULL, &first,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  object_opts.name = "artifact.txt";
  object_opts.content_type = "text/plain";
  source = source_from_text("object-truncated");
  rc = store->put_object(store, "default", "object-key", source, &object_opts,
                         &object_info, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("object-later");
  rc = store->write_state(store, "default", "later", source, NULL, &later,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  set_first_log_match_body_length(root, "object-truncated", 8UL);

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->read_state(store, "default", "good", &read_body, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_false(state_info.no_content);
  text = read_source_text(read_body);
  assert_string_equal(text, "object-prefix");
  free(text);
  lc_source_close(read_body);
  read_body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  rc = store->list_objects(store, "default", "object-key", &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 0U);
  lc_pouch_object_list_cleanup(&allocator, &list);

  rc = store->read_state(store, "default", "later", &read_body, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_true(state_info.no_content);
  assert_null(read_body);
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  lc_pouch_object_info_cleanup(&allocator, &object_info);
  lc_pouch_put_state_res_cleanup(&allocator, &first);
  lc_pouch_put_state_res_cleanup(&allocator, &later);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_replay_stops_at_truncated_queue_metadata(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_state_res first;
  lc_pouch_put_state_res later;
  lc_pouch_enqueue_opts enqueue_opts;
  lc_pouch_queue_message_info enqueued;
  lc_pouch_queue_stats stats;
  lc_pouch_state_info state_info;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-truncated-replay");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&first, 0, sizeof(first));
  memset(&later, 0, sizeof(later));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&enqueued, 0, sizeof(enqueued));
  memset(&stats, 0, sizeof(stats));
  memset(&state_info, 0, sizeof(state_info));
  store = NULL;
  read_body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("queue-prefix");
  rc = store->write_state(store, "default", "good", source, NULL, &first,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  enqueue_opts.content_type = "text/plain";
  enqueue_opts.visibility_timeout_seconds = 30L;
  enqueue_opts.ttl_seconds = 3600L;
  enqueue_opts.max_attempts = 3;
  source = source_from_text("queue-truncated");
  rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                              &enqueued, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("queue-later");
  rc = store->write_state(store, "default", "later", source, NULL, &later,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  set_first_log_match_body_length(root, "queue-truncated", 8UL);

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->read_state(store, "default", "good", &read_body, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_false(state_info.no_content);
  text = read_source_text(read_body);
  assert_string_equal(text, "queue-prefix");
  free(text);
  lc_source_close(read_body);
  read_body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  rc = store->queue_stats(store, "default", "jobs", &stats, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(stats.available);
  assert_int_equal(stats.pending_candidates, 0);
  assert_null(stats.head_message_id);
  lc_pouch_queue_stats_cleanup(&allocator, &stats);

  rc = store->read_state(store, "default", "later", &read_body, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_true(state_info.no_content);
  assert_null(read_body);
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);
  lc_pouch_put_state_res_cleanup(&allocator, &first);
  lc_pouch_put_state_res_cleanup(&allocator, &later);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_replay_stops_at_corrupt_object_record(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_state_res first;
  lc_pouch_put_state_res later;
  lc_pouch_put_object_opts object_opts;
  lc_pouch_object_info object_info;
  lc_pouch_object_list list;
  lc_pouch_state_info state_info;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "object-corrupt-replay");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&first, 0, sizeof(first));
  memset(&later, 0, sizeof(later));
  memset(&object_opts, 0, sizeof(object_opts));
  memset(&object_info, 0, sizeof(object_info));
  memset(&list, 0, sizeof(list));
  memset(&state_info, 0, sizeof(state_info));
  store = NULL;
  read_body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("object-crc-prefix");
  rc = store->write_state(store, "default", "good", source, NULL, &first,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  object_opts.name = "artifact.txt";
  object_opts.content_type = "text/plain";
  source = source_from_text("object-crc-corrupt");
  rc = store->put_object(store, "default", "object-key", source, &object_opts,
                         &object_info, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("object-crc-later");
  rc = store->write_state(store, "default", "later", source, NULL, &later,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  corrupt_first_log_match(root, "object-crc-corrupt");

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->read_state(store, "default", "good", &read_body, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_false(state_info.no_content);
  text = read_source_text(read_body);
  assert_string_equal(text, "object-crc-prefix");
  free(text);
  lc_source_close(read_body);
  read_body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  rc = store->list_objects(store, "default", "object-key", &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 0U);
  lc_pouch_object_list_cleanup(&allocator, &list);

  rc = store->read_state(store, "default", "later", &read_body, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_true(state_info.no_content);
  assert_null(read_body);
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  lc_pouch_object_info_cleanup(&allocator, &object_info);
  lc_pouch_put_state_res_cleanup(&allocator, &first);
  lc_pouch_put_state_res_cleanup(&allocator, &later);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_replay_stops_at_corrupt_queue_record(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_state_res first;
  lc_pouch_put_state_res later;
  lc_pouch_enqueue_opts enqueue_opts;
  lc_pouch_queue_message_info enqueued;
  lc_pouch_queue_stats stats;
  lc_pouch_state_info state_info;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-corrupt-replay");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&first, 0, sizeof(first));
  memset(&later, 0, sizeof(later));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&enqueued, 0, sizeof(enqueued));
  memset(&stats, 0, sizeof(stats));
  memset(&state_info, 0, sizeof(state_info));
  store = NULL;
  read_body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("queue-crc-prefix");
  rc = store->write_state(store, "default", "good", source, NULL, &first,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  enqueue_opts.content_type = "text/plain";
  enqueue_opts.visibility_timeout_seconds = 30L;
  enqueue_opts.ttl_seconds = 3600L;
  enqueue_opts.max_attempts = 3;
  source = source_from_text("queue-crc-corrupt");
  rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                              &enqueued, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("queue-crc-later");
  rc = store->write_state(store, "default", "later", source, NULL, &later,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  corrupt_first_log_match(root, "queue-crc-corrupt");

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->read_state(store, "default", "good", &read_body, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_false(state_info.no_content);
  text = read_source_text(read_body);
  assert_string_equal(text, "queue-crc-prefix");
  free(text);
  lc_source_close(read_body);
  read_body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  rc = store->queue_stats(store, "default", "jobs", &stats, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(stats.available);
  assert_int_equal(stats.pending_candidates, 0);
  assert_null(stats.head_message_id);
  lc_pouch_queue_stats_cleanup(&allocator, &stats);

  rc = store->read_state(store, "default", "later", &read_body, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_true(state_info.no_content);
  assert_null(read_body);
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);
  lc_pouch_put_state_res_cleanup(&allocator, &first);
  lc_pouch_put_state_res_cleanup(&allocator, &later);
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

  meta.lease_id = "lease-d";
  meta.state_etag = "state-d";
  meta.version = 50L;
  meta.query_hidden = 0;
  rc = store->store_meta(store, "default", "delta", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  req.namespace_name = "default";
  req.limit = 1U;
  rc = store->scan_meta(store, &req, capture_scan_row, &capture, &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1U);
  assert_string_equal(capture.keys[0], "bravo");
  assert_int_equal(capture.versions[0], 20L);
  assert_false(capture.query_hidden[0]);
  assert_true(scan.truncated);
  assert_string_equal(scan.next_start_after, "bravo");
  lc_pouch_scan_meta_res_cleanup(&allocator, &scan);

  memset(&capture, 0, sizeof(capture));
  req.start_after = "bravo";
  req.limit = 8U;
  rc = store->scan_meta(store, &req, capture_scan_row, &capture, &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1U);
  assert_string_equal(capture.keys[0], "delta");
  assert_int_equal(capture.versions[0], 50L);
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
  assert_string_equal(capture.keys[0], "bravo");
  assert_string_equal(capture.keys[1], "delta");
  assert_false(scan.truncated);
  lc_pouch_scan_meta_res_cleanup(&allocator, &scan);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_metadata_scan_forces_full_log_replay(void **state) {
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
  test_root_path(root, sizeof(root), "meta-scan-full-replay");
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
  meta.lease_id = "lease";
  meta.state_etag = "state";
  meta.version = 1L;
  rc = store->store_meta(store, "default", "alpha", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  req.namespace_name = "default";
  tracked.fail_malloc_size = strlen("default") + 1U;
  rc = store->scan_meta(store, &req, capture_scan_row, &capture, &scan, &error);
  assert_int_equal(rc, LC_ERR_NOMEM);
  assert_string_equal(error.message, "failed to decode pouch replay key");
  tracked.fail_malloc_size = 0U;
  lc_error_cleanup(&error);

  memset(&capture, 0, sizeof(capture));
  rc = store->scan_meta(store, &req, capture_scan_row, &capture, &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1U);
  assert_string_equal(capture.keys[0], "alpha");
  assert_int_equal(capture.versions[0], 1L);
  lc_pouch_scan_meta_res_cleanup(&allocator, &scan);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_staged_state_rejects_pathlike_transaction_ids(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_state_opts state_opts;
  lc_pouch_put_state_res put_res;
  lc_pouch_state_info info;
  lc_pouch_promote_staged_opts promote_opts;
  lc_pouch_discard_staged_opts discard_opts;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "staged-txn-path");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&state_opts, 0, sizeof(state_opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&info, 0, sizeof(info));
  memset(&promote_opts, 0, sizeof(promote_opts));
  memset(&discard_opts, 0, sizeof(discard_opts));
  store = NULL;
  read_body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  state_opts.content_type = "text/plain";
  source = source_from_text("draft");
  rc = store->stage_state(store, "default", "lease-key", "txn/bad", source,
                          &state_opts, &put_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(count_log_records_of_type(root, TEST_POUCH_RECORD_STATE_PUT),
                   0U);
  lc_error_cleanup(&error);

  rc = store->load_staged_state(store, "default", "lease-key", "txn/bad",
                                &read_body, &info, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(read_body);
  lc_error_cleanup(&error);

  rc = store->promote_staged_state(store, "default", "lease-key", "txn/bad",
                                   &promote_opts, &put_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  lc_error_cleanup(&error);

  rc = store->discard_staged_state(store, "default", "lease-key", "txn/bad",
                                   &discard_opts, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(count_log_records_of_type(root, TEST_POUCH_RECORD_STATE_PUT),
                   0U);
  assert_int_equal(
      count_log_records_of_type(root, TEST_POUCH_RECORD_STATE_REMOVE), 0U);
  lc_error_cleanup(&error);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_state_info_cleanup(&allocator, &info);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_root_staged_state_lists_and_discards(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_state_opts state_opts;
  lc_pouch_put_state_res staged;
  lc_pouch_put_state_res nested;
  lc_pouch_discard_staged_opts discard_opts;
  lc_pouch_list_staged_req req;
  lc_pouch_staged_state_list list;
  lc_pouch_state_info info;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "root-staged");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&state_opts, 0, sizeof(state_opts));
  memset(&staged, 0, sizeof(staged));
  memset(&nested, 0, sizeof(nested));
  memset(&discard_opts, 0, sizeof(discard_opts));
  memset(&req, 0, sizeof(req));
  memset(&list, 0, sizeof(list));
  memset(&info, 0, sizeof(info));
  store = NULL;
  read_body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  state_opts.content_type = "application/json";
  source = source_from_text("{\"draft\":true}");
  rc = store->stage_state(store, "default", "", "txn-root", source,
                          &state_opts, &staged, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_non_null(staged.new_state_etag);

  rc = store->load_staged_state(store, "default", "", "txn-root", &read_body,
                                &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  assert_string_equal(info.content_type, "application/json");
  assert_string_equal(info.etag, staged.new_state_etag);
  text = read_source_text(read_body);
  assert_string_equal(text, "{\"draft\":true}");
  free(text);
  lc_source_close(read_body);
  read_body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &info);

  source = source_from_text("nested");
  rc = store->write_state(store, "default",
                          ".staging/txn-root-nested/attachments/blob", source,
                          &state_opts, &nested, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  req.namespace_name = "default";
  req.key = "";
  rc = store->list_staged_state(store, &req, &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 1U);
  assert_string_equal(list.items[0].key, "");
  assert_string_equal(list.items[0].txn_id, "txn-root");
  assert_string_equal(list.items[0].etag, staged.new_state_etag);
  assert_string_equal(list.items[0].content_type, "application/json");
  assert_int_equal(list.items[0].bytes, 14L);
  assert_false(list.truncated);
  assert_null(list.next_start_after);
  lc_pouch_staged_state_list_cleanup(&allocator, &list);

  discard_opts.expected_etag = staged.new_state_etag;
  rc = store->discard_staged_state(store, "default", "", "txn-root",
                                   &discard_opts, &error);
  assert_int_equal(rc, LC_OK);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  rc = store->list_staged_state(store, &req, &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 0U);
  lc_pouch_staged_state_list_cleanup(&allocator, &list);

  rc = store->load_staged_state(store, "default", "", "txn-root", &read_body,
                                &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(info.no_content);
  assert_null(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);

  lc_pouch_put_state_res_cleanup(&allocator, &staged);
  lc_pouch_put_state_res_cleanup(&allocator, &nested);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_query_index_scan_orders_paginates_and_reports_seq(
    void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_query_index_scan_req req;
  lc_pouch_query_index_scan_res scan;
  scan_capture capture;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-index-scan");
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
  assert_non_null(store->query_index_scan);

  meta.owner = "owner";
  meta.lease_id = "lease-b";
  meta.state_etag = "state-b";
  meta.version = 2L;
  rc = store->store_meta(store, "default", "bravo", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-a";
  meta.state_etag = "state-a";
  meta.version = 1L;
  rc = store->store_meta(store, "default", "alpha", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-h";
  meta.state_etag = "state-h";
  meta.version = 3L;
  meta.has_query_hidden = 1;
  meta.query_hidden = 1;
  rc = store->store_meta(store, "default", "hidden", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-before";
  meta.state_etag = "state-before";
  meta.version = 4L;
  rc = store->store_meta(store, "aaa", "before-default", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-c";
  meta.state_etag = "state-c";
  meta.version = 5L;
  meta.has_query_hidden = 0;
  meta.query_hidden = 0;
  rc = store->store_meta(store, "default", "charlie", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-after";
  meta.state_etag = "state-after";
  meta.version = 6L;
  rc = store->store_meta(store, "zzz", "after-default", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  req.namespace_name = "default";
  req.limit = 1U;
  rc = store->query_index_scan(store, &req, capture_scan_row, &capture, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1U);
  assert_string_equal(capture.keys[0], "alpha");
  assert_true(scan.truncated);
  assert_string_equal(scan.next_start_after, "alpha");
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&capture, 0, sizeof(capture));
  req.start_after = "alpha";
  req.limit = 8U;
  rc = store->query_index_scan(store, &req, capture_scan_row, &capture, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 2U);
  assert_string_equal(capture.keys[0], "bravo");
  assert_string_equal(capture.keys[1], "charlie");
  assert_false(scan.truncated);
  assert_null(scan.next_start_after);
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&capture, 0, sizeof(capture));
  req.start_after = "bravo-z";
  req.limit = 8U;
  rc = store->query_index_scan(store, &req, capture_scan_row, &capture, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1U);
  assert_string_equal(capture.keys[0], "charlie");
  assert_false(scan.truncated);
  assert_null(scan.next_start_after);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_query_index_keys_scan_avoids_metadata_row_copies(
    void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_query_index_scan_req req;
  lc_pouch_query_index_scan_res scan;
  key_capture capture;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-index-key-only");
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
  assert_non_null(store->query_index_keys_scan);

  meta.owner = "owner-key-only-allocation-sentinel";
  meta.lease_id = "lease-alpha";
  meta.state_etag = "state-alpha";
  meta.version = 1L;
  rc = store->store_meta(store, "default", "alpha", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-bravo";
  meta.state_etag = "state-bravo";
  meta.version = 2L;
  rc = store->store_meta(store, "default", "bravo", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-charlie";
  meta.state_etag = "state-charlie";
  meta.version = 3L;
  meta.has_query_hidden = 1;
  meta.query_hidden = 1;
  rc = store->store_meta(store, "default", "charlie", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-delta";
  meta.state_etag = "state-delta";
  meta.version = 4L;
  meta.has_query_hidden = 0;
  meta.query_hidden = 0;
  rc = store->store_meta(store, "default", "delta", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  rc = store->delete_meta(store, "default", "delta", stored.etag, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  tracked.fail_malloc_size = strlen(meta.owner) + 1U;
  req.namespace_name = "default";
  req.limit = 1U;
  rc = store->query_index_keys_scan(store, &req, capture_query_key, &capture,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1U);
  assert_string_equal(capture.keys[0], "alpha");
  assert_true(scan.truncated);
  assert_string_equal(scan.next_start_after, "alpha");
  assert_true(scan.index_seq > 0UL);
  tracked.fail_malloc_size = 0U;
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&capture, 0, sizeof(capture));
  req.start_after = "alpha";
  req.limit = 8U;
  rc = store->query_index_keys_scan(store, &req, capture_query_key, &capture,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1U);
  assert_string_equal(capture.keys[0], "bravo");
  assert_false(scan.truncated);
  assert_null(scan.next_start_after);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_query_index_projection_replays_updates_and_deletes(
    void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_store_meta_res updated;
  lc_pouch_query_index_scan_req req;
  lc_pouch_query_index_scan_res scan;
  scan_capture capture;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-index-projection-replay");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  memset(&updated, 0, sizeof(updated));
  memset(&req, 0, sizeof(req));
  memset(&scan, 0, sizeof(scan));
  memset(&capture, 0, sizeof(capture));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  meta.owner = "owner";
  meta.lease_id = "lease-c";
  meta.state_etag = "state-c";
  meta.version = 3L;
  rc = store->store_meta(store, "default", "charlie", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-a";
  meta.state_etag = "state-a";
  meta.version = 1L;
  rc = store->store_meta(store, "default", "alpha", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);

  meta.lease_id = "lease-b";
  meta.state_etag = "state-b";
  meta.version = 2L;
  rc = store->store_meta(store, "default", "bravo", &meta, NULL, &updated,
                         &error);
  assert_int_equal(rc, LC_OK);
  rc = store->delete_meta(store, "default", "bravo", updated.etag, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &updated);

  meta.lease_id = "lease-a2";
  meta.state_etag = "state-a2";
  meta.version = 4L;
  meta.has_query_hidden = 1;
  meta.query_hidden = 1;
  rc = store->store_meta(store, "default", "alpha", &meta, stored.etag,
                         &updated, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);
  lc_pouch_store_meta_res_cleanup(&allocator, &updated);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  req.namespace_name = "default";
  rc = store->query_index_scan(store, &req, capture_scan_row, &capture, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1U);
  assert_string_equal(capture.keys[0], "charlie");
  assert_int_equal(capture.versions[0], 3L);
  assert_false(scan.truncated);
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_metadata_update_allocation_failure_preserves_indexes(
    void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_meta meta;
  lc_pouch_meta_record loaded;
  lc_pouch_store_meta_res stored;
  lc_pouch_store_meta_res updated;
  lc_pouch_query_index_scan_req req;
  lc_pouch_query_index_scan_res scan;
  scan_capture capture;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "meta-update-alloc-failure");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&meta, 0, sizeof(meta));
  memset(&loaded, 0, sizeof(loaded));
  memset(&stored, 0, sizeof(stored));
  memset(&updated, 0, sizeof(updated));
  memset(&req, 0, sizeof(req));
  memset(&scan, 0, sizeof(scan));
  memset(&capture, 0, sizeof(capture));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  meta.owner = "owner-old";
  meta.lease_id = "lease-old";
  meta.state_etag = "state-old";
  meta.version = 1L;
  rc = store->store_meta(store, "default", "lease-key", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);

  meta.owner = "owner-allocation-failure-sentinel";
  meta.lease_id = "lease-new";
  meta.state_etag = "state-new";
  meta.version = 2L;
  tracked.fail_malloc_size = strlen(meta.owner) + 1U;
  rc = store->store_meta(store, "default", "lease-key", &meta, stored.etag,
                         &updated, &error);
  assert_int_equal(rc, LC_ERR_NOMEM);
  assert_string_equal(error.message, "failed to update pouch metadata index");
  tracked.fail_malloc_size = 0U;
  lc_error_cleanup(&error);

  rc = store->load_meta(store, "default", "lease-key", &loaded, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(loaded.found);
  assert_string_equal(loaded.meta.owner, "owner-old");
  assert_string_equal(loaded.meta.lease_id, "lease-old");
  assert_int_equal(loaded.meta.version, 1L);
  lc_pouch_meta_record_cleanup(&allocator, &loaded);

  req.namespace_name = "default";
  rc = store->query_index_scan(store, &req, capture_scan_row, &capture, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1U);
  assert_string_equal(capture.keys[0], "lease-key");
  assert_int_equal(capture.versions[0], 1L);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  lc_pouch_store_meta_res_cleanup(&allocator, &stored);
  lc_pouch_store_meta_res_cleanup(&allocator, &updated);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_index_flush_reports_current_projection(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_index_flush_res flushed;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "index-flush");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  memset(&flushed, 0, sizeof(flushed));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(store->flush_index);

  meta.owner = "owner";
  meta.lease_id = "lease";
  meta.state_etag = "state";
  meta.version = 1L;
  rc = store->store_meta(store, "default", "alpha", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  rc = store->flush_index(store, "default", NULL, &flushed, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(flushed.namespace_name, "default");
  assert_string_equal(flushed.mode, "wait");
  assert_string_equal(flushed.flush_id, "local");
  assert_true(flushed.accepted);
  assert_true(flushed.flushed);
  assert_false(flushed.pending);
  assert_true(flushed.index_seq > 0UL);
  lc_pouch_index_flush_res_cleanup(&allocator, &flushed);

  rc = store->flush_index(store, "default", "now", &flushed, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(flushed.mode, "now");
  assert_true(flushed.index_seq > 0UL);
  lc_pouch_index_flush_res_cleanup(&allocator, &flushed);

  rc = store->flush_index(store, "default", "later", &flushed, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "pouch index flush mode must be wait or now");
  lc_error_cleanup(&error);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_index_flush_recovers_from_corrupt_sidecar_tail(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_index_flush_res flushed;
  lc_pouch_query_index_scan_req req;
  lc_pouch_query_index_scan_res scan;
  key_capture capture;
  lc_error error;
  off_t original_query_index_size;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "index-flush-corrupt-sidecar-tail");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  memset(&flushed, 0, sizeof(flushed));
  memset(&req, 0, sizeof(req));
  memset(&scan, 0, sizeof(scan));
  memset(&capture, 0, sizeof(capture));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  meta.owner = "owner";
  meta.lease_id = "lease-alpha";
  meta.state_etag = "state-alpha";
  meta.version = 1L;
  rc = store->store_meta(store, "default", "alpha", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-corrupt";
  meta.state_etag = "state-corrupt";
  meta.version = 2L;
  rc = store->store_meta(store, "default", "corrupt", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-later";
  meta.state_etag = "state-later";
  meta.version = 3L;
  rc = store->store_meta(store, "default", "later", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  original_query_index_size = test_query_index_size(root);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  corrupt_first_query_index_match(root, "corrupt");

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->flush_index(store, "default", "wait", &flushed, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(flushed.namespace_name, "default");
  assert_string_equal(flushed.mode, "wait");
  assert_string_equal(flushed.flush_id, "local");
  assert_true(flushed.accepted);
  assert_true(flushed.flushed);
  assert_false(flushed.pending);
  assert_true(flushed.index_seq >= 3UL);
  assert_true(test_query_index_size(root) < original_query_index_size);
  lc_pouch_index_flush_res_cleanup(&allocator, &flushed);

  req.namespace_name = "default";
  req.limit = 8U;
  rc = store->query_index_keys_scan(store, &req, capture_query_key, &capture,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 3U);
  assert_string_equal(capture.keys[0], "alpha");
  assert_string_equal(capture.keys[1], "corrupt");
  assert_string_equal(capture.keys[2], "later");
  assert_false(scan.truncated);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_query_index_sidecar_appends_metadata_records(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-index-sidecar");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(count_query_index_records_of_type(
                       root, TEST_POUCH_QUERY_INDEX_RECORD_META),
                   0U);

  meta.owner = "owner";
  meta.lease_id = "lease";
  meta.state_etag = "state";
  meta.version = 1L;
  meta.has_query_hidden = 1;
  meta.query_hidden = 0;
  rc = store->store_meta(store, "default", "alpha", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(count_query_index_records_of_type(
                       root, TEST_POUCH_QUERY_INDEX_RECORD_META),
                   1U);

  rc = store->delete_meta(store, "default", "alpha", stored.etag, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(count_query_index_records_of_type(
                       root, TEST_POUCH_QUERY_INDEX_RECORD_META),
                   2U);

  lc_pouch_store_meta_res_cleanup(&allocator, &stored);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_query_index_keys_ignores_sidecar_without_metadata_log(
    void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_query_index_scan_req req;
  lc_pouch_query_index_scan_res scan;
  key_capture capture;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-index-sidecar-replay");
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
  meta.lease_id = "lease-alpha";
  meta.state_etag = "state-alpha";
  meta.version = 1L;
  rc = store->store_meta(store, "default", "alpha", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-bravo";
  meta.state_etag = "state-bravo";
  meta.version = 2L;
  rc = store->store_meta(store, "default", "bravo", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-charlie";
  meta.state_etag = "state-charlie";
  meta.version = 3L;
  meta.has_query_hidden = 1;
  meta.query_hidden = 1;
  rc = store->store_meta(store, "default", "charlie", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-delta";
  meta.state_etag = "state-delta";
  meta.version = 4L;
  meta.has_query_hidden = 0;
  meta.query_hidden = 0;
  rc = store->store_meta(store, "default", "delta", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  rc = store->delete_meta(store, "default", "delta", stored.etag, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  truncate_store_log(root);

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  req.namespace_name = "default";
  req.limit = 4U;
  rc = store->query_index_keys_scan(store, &req, capture_query_key, &capture,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 0U);
  assert_false(scan.truncated);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_query_index_keys_recovers_from_corrupt_sidecar_tail(
    void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_query_index_scan_req req;
  lc_pouch_query_index_scan_res scan;
  scan_capture row_capture;
  key_capture capture;
  lc_error error;
  off_t original_query_index_size;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-index-sidecar-corrupt-tail");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  memset(&req, 0, sizeof(req));
  memset(&scan, 0, sizeof(scan));
  memset(&row_capture, 0, sizeof(row_capture));
  memset(&capture, 0, sizeof(capture));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  meta.owner = "owner";
  meta.lease_id = "lease-alpha";
  meta.state_etag = "state-alpha";
  meta.version = 1L;
  rc = store->store_meta(store, "default", "alpha", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-corrupt";
  meta.state_etag = "state-corrupt";
  meta.version = 2L;
  rc = store->store_meta(store, "default", "corrupt", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-later";
  meta.state_etag = "state-later";
  meta.version = 3L;
  rc = store->store_meta(store, "default", "later", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  original_query_index_size = test_query_index_size(root);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  corrupt_first_query_index_match(root, "corrupt");

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  req.namespace_name = "default";
  req.limit = 8U;
  rc = store->query_index_scan(store, &req, capture_scan_row, &row_capture,
                               &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(row_capture.count, 3U);
  assert_string_equal(row_capture.keys[0], "alpha");
  assert_int_equal(row_capture.versions[0], 1L);
  assert_string_equal(row_capture.keys[1], "corrupt");
  assert_int_equal(row_capture.versions[1], 2L);
  assert_string_equal(row_capture.keys[2], "later");
  assert_int_equal(row_capture.versions[2], 3L);
  assert_false(scan.truncated);
  assert_true(test_query_index_size(root) < original_query_index_size);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->query_index_keys_scan(store, &req, capture_query_key, &capture,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 3U);
  assert_string_equal(capture.keys[0], "alpha");
  assert_string_equal(capture.keys[1], "corrupt");
  assert_string_equal(capture.keys[2], "later");
  assert_false(scan.truncated);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  meta.lease_id = "lease-bravo";
  meta.state_etag = "state-bravo";
  meta.version = 4L;
  rc = store->store_meta(store, "default", "bravo", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  memset(&capture, 0, sizeof(capture));
  rc = store->query_index_keys_scan(store, &req, capture_query_key, &capture,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 4U);
  assert_string_equal(capture.keys[0], "alpha");
  assert_string_equal(capture.keys[1], "bravo");
  assert_string_equal(capture.keys[2], "corrupt");
  assert_string_equal(capture.keys[3], "later");
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  memset(&capture, 0, sizeof(capture));
  rc = store->query_index_keys_scan(store, &req, capture_query_key, &capture,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 4U);
  assert_string_equal(capture.keys[0], "alpha");
  assert_string_equal(capture.keys[1], "bravo");
  assert_string_equal(capture.keys[2], "corrupt");
  assert_string_equal(capture.keys[3], "later");
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_query_index_keys_truncates_partial_sidecar_field(
    void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_query_index_scan_req req;
  lc_pouch_query_index_scan_res scan;
  key_capture capture;
  lc_error error;
  off_t truncated_query_index_size;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-index-sidecar-partial-field");
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
  meta.lease_id = "lease-alpha";
  meta.state_etag = "state-alpha";
  meta.version = 1L;
  rc = store->store_meta(store, "default", "alpha", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-bravo";
  meta.state_etag = "state-bravo";
  meta.version = 2L;
  rc = store->store_meta(store, "default", "bravo", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  truncated_query_index_size = test_query_index_size(root) - 2;
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  truncate_query_index_tail(root, 2);
  assert_int_equal(test_query_index_size(root), truncated_query_index_size);

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  req.namespace_name = "default";
  rc = store->query_index_keys_scan(store, &req, capture_query_key, &capture,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 2U);
  assert_string_equal(capture.keys[0], "alpha");
  assert_string_equal(capture.keys[1], "bravo");
  assert_true(test_query_index_size(root) < truncated_query_index_size);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_scan_meta_ignores_corrupt_query_sidecar(void **state) {
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
  off_t original_query_index_size;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "scan-meta-corrupt-query-sidecar");
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
  meta.lease_id = "lease-alpha";
  meta.state_etag = "state-alpha";
  meta.version = 1L;
  rc = store->store_meta(store, "default", "alpha", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-corrupt";
  meta.state_etag = "state-corrupt";
  meta.version = 2L;
  rc = store->store_meta(store, "default", "corrupt", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-later";
  meta.state_etag = "state-later";
  meta.version = 3L;
  rc = store->store_meta(store, "default", "later", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  original_query_index_size = test_query_index_size(root);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  corrupt_first_query_index_match(root, "corrupt");

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  req.namespace_name = "default";
  req.limit = 8U;
  rc = store->scan_meta(store, &req, capture_scan_row, &capture, &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 3U);
  assert_string_equal(capture.keys[0], "alpha");
  assert_string_equal(capture.keys[1], "corrupt");
  assert_string_equal(capture.keys[2], "later");
  assert_false(scan.truncated);
  assert_int_equal(test_query_index_size(root), original_query_index_size);
  lc_pouch_scan_meta_res_cleanup(&allocator, &scan);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_query_index_sidecar_compacts_with_store_log(void **state) {
  char root[256];
  char owner[2048];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_store_meta_res updated;
  lc_pouch_query_index_scan_req req;
  lc_pouch_query_index_scan_res scan;
  key_capture capture;
  lc_error error;
  size_t index;
  size_t sidecar_records;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-index-sidecar-compact");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  memset(&updated, 0, sizeof(updated));
  memset(&req, 0, sizeof(req));
  memset(&scan, 0, sizeof(scan));
  memset(&capture, 0, sizeof(capture));
  memset(owner, 'o', sizeof(owner));
  owner[sizeof(owner) - 1U] = '\0';
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  meta.owner = owner;
  meta.lease_id = "lease-compact";
  meta.state_etag = "state-compact";
  for (index = 0U; index < 160U; ++index) {
    meta.version = (long)index + 1L;
    rc = store->store_meta(store, "default", "compact-key", &meta,
                           stored.etag, &updated, &error);
    assert_int_equal(rc, LC_OK);
    lc_pouch_store_meta_res_cleanup(&allocator, &stored);
    stored = updated;
    memset(&updated, 0, sizeof(updated));
  }

  sidecar_records = count_query_index_records_of_type(
      root, TEST_POUCH_QUERY_INDEX_RECORD_META);
  assert_true(sidecar_records < 80U);

  req.namespace_name = "default";
  rc = store->query_index_keys_scan(store, &req, capture_query_key, &capture,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1U);
  assert_string_equal(capture.keys[0], "compact-key");
  assert_true(scan.index_seq >= 160UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  lc_pouch_store_meta_res_cleanup(&allocator, &stored);
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

static void test_object_overwrite_allocation_failure_replays_cleanly(
    void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_object_opts opts;
  lc_pouch_object_selector selector;
  lc_pouch_object_info first;
  lc_pouch_object_info overwritten;
  lc_pouch_object_info fetched;
  lc_error error;
  char *text;
  const char *replacement_type;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "object-overwrite-nomem");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  memset(&selector, 0, sizeof(selector));
  memset(&first, 0, sizeof(first));
  memset(&overwritten, 0, sizeof(overwritten));
  memset(&fetched, 0, sizeof(fetched));
  store = NULL;
  read_body = NULL;
  replacement_type = "application/x-pouch-overwrite-unique";

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  opts.name = "same.txt";
  opts.content_type = "text/plain";
  source = source_from_text("payload-one");
  rc = store->put_object(store, "default", "lease-key", source, &opts, &first,
                         &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  opts.content_type = replacement_type;
  tracked.fail_malloc_size = strlen(replacement_type) + 1U;
  source = source_from_text("payload-two");
  rc = store->put_object(store, "default", "lease-key", source, &opts,
                         &overwritten, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_NOMEM);
  tracked.fail_malloc_size = 0U;
  lc_error_cleanup(&error);
  memset(&error, 0, sizeof(error));

  selector.name = "same.txt";
  rc = store->get_object(store, "default", "lease-key", &selector, &read_body,
                         &fetched, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_not_equal(fetched.id, first.id);
  assert_string_equal(fetched.name, "same.txt");
  assert_string_equal(fetched.content_type, replacement_type);
  text = read_source_text(read_body);
  assert_string_equal(text, "payload-two");
  free(text);
  lc_source_close(read_body);

  lc_pouch_object_info_cleanup(&allocator, &fetched);
  lc_pouch_object_info_cleanup(&allocator, &overwritten);
  lc_pouch_object_info_cleanup(&allocator, &first);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_object_listing_orders_by_name_after_replay(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_object_opts opts;
  lc_pouch_object_info info;
  lc_pouch_object_list list;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "objects-order");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  memset(&info, 0, sizeof(info));
  memset(&list, 0, sizeof(list));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  opts.content_type = "text/plain";
  opts.name = "zeta.txt";
  source = source_from_text("zeta");
  rc = store->put_object(store, "default", "lease-key", source, &opts, &info,
                         &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_object_info_cleanup(&allocator, &info);

  opts.name = "alpha.txt";
  source = source_from_text("alpha");
  rc = store->put_object(store, "default", "lease-key", source, &opts, &info,
                         &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_object_info_cleanup(&allocator, &info);

  opts.name = "middle.txt";
  source = source_from_text("middle");
  rc = store->put_object(store, "default", "lease-key", source, &opts, &info,
                         &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_object_info_cleanup(&allocator, &info);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  rc = store->list_objects(store, "default", "lease-key", &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 3U);
  assert_string_equal(list.items[0].name, "alpha.txt");
  assert_string_equal(list.items[1].name, "middle.txt");
  assert_string_equal(list.items[2].name, "zeta.txt");

  lc_pouch_object_list_cleanup(&allocator, &list);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_object_max_bytes_reads_only_limit_plus_one(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  counting_source source;
  lc_pouch_put_object_opts opts;
  lc_pouch_object_info info;
  lc_pouch_object_list list;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "object-max-bytes");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  memset(&info, 0, sizeof(info));
  memset(&list, 0, sizeof(list));
  store = NULL;
  counting_source_init(&source, 100U);

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  opts.name = "large.bin";
  opts.content_type = "application/octet-stream";
  opts.has_max_bytes = 1;
  opts.max_bytes = 5L;
  rc = store->put_object(store, "default", "lease-key", &source.pub, &opts,
                         &info, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 413L);
  assert_int_equal(source.position, 6U);
  assert_int_equal(count_log_records_of_type(root, TEST_POUCH_RECORD_OBJECT_PUT),
                   0U);
  lc_error_cleanup(&error);

  rc = store->list_objects(store, "default", "lease-key", &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 0U);
  lc_pouch_object_list_cleanup(&allocator, &list);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_object_put_streams_payload_without_large_alloc(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  counting_source source;
  lc_source *read_body;
  lc_pouch_put_object_opts opts;
  lc_pouch_object_selector selector;
  lc_pouch_object_info info;
  lc_pouch_object_info fetched;
  lc_error error;
  size_t payload_length;
  size_t read_length;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "object-put-stream");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  memset(&selector, 0, sizeof(selector));
  memset(&info, 0, sizeof(info));
  memset(&fetched, 0, sizeof(fetched));
  store = NULL;
  read_body = NULL;
  payload_length = 128U * 1024U;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  tracked.max_malloc_size = 0U;
  tracked.max_realloc_size = 0U;
  counting_source_init(&source, payload_length);
  opts.name = "large.bin";
  opts.content_type = "application/octet-stream";
  opts.prevent_overwrite = 1;
  rc = store->put_object(store, "default", "lease-key", &source.pub, &opts,
                         &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(info.size, (long)payload_length);
  assert_true(tracked.max_malloc_size < payload_length);
  assert_true(tracked.max_realloc_size < payload_length);

  selector.name = "large.bin";
  rc = store->get_object(store, "default", "lease-key", &selector, &read_body,
                         &fetched, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(fetched.id, info.id);
  read_length = read_source_count_x(read_body);
  assert_int_equal(read_length, payload_length);

  lc_source_close(read_body);
  lc_pouch_object_info_cleanup(&allocator, &fetched);
  lc_pouch_object_info_cleanup(&allocator, &info);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_object_copy_streams_existing_payload_without_large_alloc(
    void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  counting_source source;
  lc_source *read_body;
  lc_pouch_put_object_opts opts;
  lc_pouch_copy_object_opts copy_opts;
  lc_pouch_object_selector selector;
  lc_pouch_object_info original;
  lc_pouch_object_info copied;
  lc_pouch_object_info fetched;
  lc_error error;
  size_t payload_length;
  size_t copied_length;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "object-copy-stream");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  memset(&copy_opts, 0, sizeof(copy_opts));
  memset(&selector, 0, sizeof(selector));
  memset(&original, 0, sizeof(original));
  memset(&copied, 0, sizeof(copied));
  memset(&fetched, 0, sizeof(fetched));
  store = NULL;
  read_body = NULL;
  payload_length = 128U * 1024U;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  counting_source_init(&source, payload_length);
  opts.name = "large.bin";
  opts.content_type = "application/octet-stream";
  opts.prevent_overwrite = 1;
  rc = store->put_object(store, "default", "source-key", &source.pub, &opts,
                         &original, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(original.size, (long)payload_length);

  tracked.max_malloc_size = 0U;
  tracked.max_realloc_size = 0U;
  copy_opts.source.name = "large.bin";
  copy_opts.prevent_overwrite = 1;
  rc = store->copy_object(store, "default", "source-key", "dest-key",
                          &copy_opts, &copied, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(copied.id, original.id);
  assert_int_equal(copied.size, (long)payload_length);
  assert_true(tracked.max_malloc_size < payload_length);
  assert_true(tracked.max_realloc_size < payload_length);

  selector.name = "large.bin";
  rc = store->get_object(store, "default", "dest-key", &selector, &read_body,
                         &fetched, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(fetched.id, original.id);
  copied_length = read_source_count_x(read_body);
  assert_int_equal(copied_length, payload_length);
  lc_source_close(read_body);
  lc_pouch_object_info_cleanup(&allocator, &fetched);
  lc_pouch_object_info_cleanup(&allocator, &copied);
  lc_pouch_object_info_cleanup(&allocator, &original);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_object_copy_source_open_failure_leaves_destination_unchanged(
    void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  counting_source source;
  lc_pouch_put_object_opts put_opts;
  lc_pouch_copy_object_opts copy_opts;
  lc_pouch_object_info original;
  lc_pouch_object_info copied;
  lc_pouch_object_list list;
  lc_error error;
  size_t payload_length;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "object-copy-source-open-failure");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&put_opts, 0, sizeof(put_opts));
  memset(&copy_opts, 0, sizeof(copy_opts));
  memset(&original, 0, sizeof(original));
  memset(&copied, 0, sizeof(copied));
  memset(&list, 0, sizeof(list));
  store = NULL;
  payload_length = 128U * 1024U;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  counting_source_init(&source, payload_length);
  put_opts.name = "large.bin";
  put_opts.content_type = "application/octet-stream";
  put_opts.prevent_overwrite = 1;
  rc = store->put_object(store, "default", "source-key", &source.pub,
                         &put_opts, &original, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(original.size, (long)payload_length);
  assert_int_equal(count_log_records_of_type(root, TEST_POUCH_RECORD_OBJECT_PUT),
                   1U);

  chmod_store_log(root, 0);

  copy_opts.source.name = "large.bin";
  copy_opts.prevent_overwrite = 1;
  rc = store->copy_object(store, "default", "source-key", "dest-key",
                          &copy_opts, &copied, &error);
  chmod_store_log(root, 0600);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_string_equal(error.message, "failed to open pouch log for object copy");
  assert_null(copied.id);
  assert_int_equal(count_log_records_of_type(root, TEST_POUCH_RECORD_OBJECT_PUT),
                   1U);
  lc_error_cleanup(&error);

  rc = store->list_objects(store, "default", "dest-key", &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 0U);
  lc_pouch_object_list_cleanup(&allocator, &list);

  lc_pouch_object_info_cleanup(&allocator, &original);
  lc_pouch_object_info_cleanup(&allocator, &copied);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_queue_dequeue_skips_replay_after_same_handle_enqueue(
    void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  counting_source source;
  lc_source *payload;
  lc_pouch_enqueue_opts enqueue_opts;
  lc_pouch_dequeue_opts dequeue_opts;
  lc_pouch_queue_message_info enqueued;
  lc_pouch_queue_message_info dequeued;
  lc_error error;
  size_t payload_length;
  size_t read_length;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-no-replay");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&dequeue_opts, 0, sizeof(dequeue_opts));
  memset(&enqueued, 0, sizeof(enqueued));
  memset(&dequeued, 0, sizeof(dequeued));
  store = NULL;
  payload = NULL;
  payload_length = 128U * 1024U;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  tracked.max_malloc_size = 0U;
  tracked.max_realloc_size = 0U;
  counting_source_init(&source, payload_length);
  enqueue_opts.content_type = "application/octet-stream";
  enqueue_opts.visibility_timeout_seconds = 30L;
  enqueue_opts.ttl_seconds = 3600L;
  enqueue_opts.max_attempts = 3;
  rc = store->enqueue_message(store, "default", "jobs", &source.pub,
                              &enqueue_opts, &enqueued, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(enqueued.payload_bytes, (long)payload_length);
  assert_true(tracked.max_malloc_size < payload_length);
  assert_true(tracked.max_realloc_size < payload_length);

  tracked.max_malloc_size = 0U;
  tracked.max_realloc_size = 0U;
  dequeue_opts.owner = "worker-a";
  dequeue_opts.visibility_timeout_seconds = 30L;
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts,
                              &payload, &dequeued, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(payload);
  assert_int_equal(dequeued.payload_bytes, (long)payload_length);
  assert_true(tracked.max_malloc_size < payload_length);
  assert_true(tracked.max_realloc_size < payload_length);
  read_length = read_source_count_x(payload);
  assert_int_equal(read_length, payload_length);

  lc_source_close(payload);
  lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);
  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_queue_rejects_negative_timing_options(void **state) {
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
  lc_error error;
  int acked;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-negative-options");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&dequeue_opts, 0, sizeof(dequeue_opts));
  memset(&enqueued, 0, sizeof(enqueued));
  memset(&dequeued, 0, sizeof(dequeued));
  memset(&updated, 0, sizeof(updated));
  memset(&ref, 0, sizeof(ref));
  store = NULL;
  payload = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  enqueue_opts.content_type = "text/plain";
  enqueue_opts.delay_seconds = -1L;
  source = source_from_text("bad-delay");
  rc = store->enqueue_message(store, "default", "jobs", source,
                              &enqueue_opts, &enqueued, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "enqueue_message delay_seconds must be non-negative");
  lc_error_cleanup(&error);

  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  enqueue_opts.content_type = "text/plain";
  enqueue_opts.visibility_timeout_seconds = -1L;
  source = source_from_text("bad-visibility");
  rc = store->enqueue_message(store, "default", "jobs", source,
                              &enqueue_opts, &enqueued, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(
      error.message,
      "enqueue_message visibility_timeout_seconds must be non-negative");
  lc_error_cleanup(&error);

  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  enqueue_opts.content_type = "text/plain";
  enqueue_opts.ttl_seconds = -1L;
  source = source_from_text("bad-ttl");
  rc = store->enqueue_message(store, "default", "jobs", source,
                              &enqueue_opts, &enqueued, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "enqueue_message ttl_seconds must be non-negative");
  lc_error_cleanup(&error);

  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  enqueue_opts.content_type = "text/plain";
  enqueue_opts.max_attempts = -1;
  source = source_from_text("bad-attempts");
  rc = store->enqueue_message(store, "default", "jobs", source,
                              &enqueue_opts, &enqueued, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "enqueue_message max_attempts must be non-negative");
  lc_error_cleanup(&error);
  assert_int_equal(count_log_records_of_type(root, TEST_POUCH_RECORD_QUEUE_PUT),
                   0U);

  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  enqueue_opts.content_type = "text/plain";
  enqueue_opts.visibility_timeout_seconds = 30L;
  enqueue_opts.ttl_seconds = 3600L;
  enqueue_opts.max_attempts = 3;
  source = source_from_text("valid");
  rc = store->enqueue_message(store, "default", "jobs", source,
                              &enqueue_opts, &enqueued, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  dequeue_opts.owner = "worker-a";
  dequeue_opts.visibility_timeout_seconds = -1L;
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts,
                              &payload, &dequeued, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(payload);
  assert_string_equal(
      error.message,
      "dequeue_message visibility_timeout_seconds must be non-negative");
  lc_error_cleanup(&error);

  dequeue_opts.visibility_timeout_seconds = 30L;
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts,
                              &payload, &dequeued, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(payload);

  ref.namespace_name = dequeued.namespace_name;
  ref.queue = dequeued.queue;
  ref.message_id = dequeued.message_id;
  ref.lease_id = dequeued.lease_id;
  ref.txn_id = dequeued.txn_id;
  ref.fencing_token = dequeued.fencing_token;
  ref.meta_etag = dequeued.meta_etag;

  rc = store->nack_message(store, &ref, -1L, 1, &updated, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "nack_message delay_seconds must be non-negative");
  lc_error_cleanup(&error);

  rc = store->extend_message(store, &ref, -1L, &updated, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "extend_message extend_by_seconds must be non-negative");
  lc_error_cleanup(&error);

  acked = 0;
  rc = store->ack_message(store, &ref, &acked, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(acked);

  lc_source_close(payload);
  lc_pouch_queue_message_info_cleanup(&allocator, &updated);
  lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);
  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_replay_streams_large_bodies_without_large_alloc(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  counting_source source;
  lc_source *body;
  lc_pouch_put_state_opts state_opts;
  lc_pouch_put_state_res state_res;
  lc_pouch_state_info state_info;
  lc_pouch_put_object_opts object_opts;
  lc_pouch_object_selector selector;
  lc_pouch_object_info object_info;
  lc_pouch_enqueue_opts enqueue_opts;
  lc_pouch_dequeue_opts dequeue_opts;
  lc_pouch_queue_message_info queue_info;
  lc_error error;
  size_t payload_length;
  size_t read_length;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "large-replay");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&state_opts, 0, sizeof(state_opts));
  memset(&state_res, 0, sizeof(state_res));
  memset(&state_info, 0, sizeof(state_info));
  memset(&object_opts, 0, sizeof(object_opts));
  memset(&selector, 0, sizeof(selector));
  memset(&object_info, 0, sizeof(object_info));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&dequeue_opts, 0, sizeof(dequeue_opts));
  memset(&queue_info, 0, sizeof(queue_info));
  store = NULL;
  body = NULL;
  payload_length = 128U * 1024U;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  counting_source_init(&source, payload_length);
  state_opts.content_type = "application/octet-stream";
  rc = store->write_state(store, "default", "large-state", &source.pub,
                          &state_opts, &state_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(state_res.bytes, (long)payload_length);

  counting_source_init(&source, payload_length);
  object_opts.name = "large.bin";
  object_opts.content_type = "application/octet-stream";
  rc = store->put_object(store, "default", "large-objects", &source.pub,
                         &object_opts, &object_info, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(object_info.size, (long)payload_length);

  counting_source_init(&source, payload_length);
  enqueue_opts.content_type = "application/octet-stream";
  enqueue_opts.visibility_timeout_seconds = 30L;
  enqueue_opts.ttl_seconds = 3600L;
  enqueue_opts.max_attempts = 3;
  rc = store->enqueue_message(store, "default", "jobs", &source.pub,
                              &enqueue_opts, &queue_info, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(queue_info.payload_bytes, (long)payload_length);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;
  lc_pouch_object_info_cleanup(&allocator, &object_info);
  lc_pouch_queue_message_info_cleanup(&allocator, &queue_info);

  tracked.max_malloc_size = 0U;
  tracked.max_realloc_size = 0U;
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(tracked.max_malloc_size < payload_length);
  assert_true(tracked.max_realloc_size < payload_length);

  rc = store->read_state(store, "default", "large-state", &body, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(body);
  read_length = read_source_count_x(body);
  assert_int_equal(read_length, payload_length);
  lc_source_close(body);
  body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  selector.name = "large.bin";
  rc = store->get_object(store, "default", "large-objects", &selector, &body,
                         &object_info, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(body);
  assert_int_equal(object_info.size, (long)payload_length);
  read_length = read_source_count_x(body);
  assert_int_equal(read_length, payload_length);
  lc_source_close(body);
  body = NULL;

  dequeue_opts.owner = "worker-a";
  dequeue_opts.visibility_timeout_seconds = 30L;
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts, &body,
                              &queue_info, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(body);
  assert_int_equal(queue_info.payload_bytes, (long)payload_length);
  read_length = read_source_count_x(body);
  assert_int_equal(read_length, payload_length);
  lc_source_close(body);

  lc_pouch_queue_message_info_cleanup(&allocator, &queue_info);
  lc_pouch_object_info_cleanup(&allocator, &object_info);
  lc_pouch_put_state_res_cleanup(&allocator, &state_res);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_auto_compaction_preserves_live_heads_and_tokens(void **state) {
  char root[256];
  char payload[4096];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *body;
  lc_pouch_put_state_opts state_opts;
  lc_pouch_put_state_res put_res;
  lc_pouch_put_state_res after_res;
  lc_pouch_state_info state_info;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res meta_res;
  lc_pouch_meta_record loaded_meta;
  lc_pouch_query_index_scan_req query_req;
  lc_pouch_query_index_scan_res before_query;
  lc_pouch_query_index_scan_res after_query;
  lc_pouch_query_index_scan_res reopened_query;
  scan_capture before_capture;
  scan_capture after_capture;
  scan_capture reopened_capture;
  lc_pouch_put_object_opts object_opts;
  lc_pouch_object_selector selector;
  lc_pouch_object_info object_info;
  lc_pouch_enqueue_opts enqueue_opts;
  lc_pouch_dequeue_opts dequeue_opts;
  lc_pouch_queue_message_info enqueued;
  lc_pouch_queue_message_info dequeued;
  lc_error error;
  char *text;
  long last_version;
  size_t index;
  size_t read_length;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "auto-compact");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&state_opts, 0, sizeof(state_opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&after_res, 0, sizeof(after_res));
  memset(&state_info, 0, sizeof(state_info));
  memset(&meta, 0, sizeof(meta));
  memset(&meta_res, 0, sizeof(meta_res));
  memset(&loaded_meta, 0, sizeof(loaded_meta));
  memset(&query_req, 0, sizeof(query_req));
  memset(&before_query, 0, sizeof(before_query));
  memset(&after_query, 0, sizeof(after_query));
  memset(&reopened_query, 0, sizeof(reopened_query));
  memset(&before_capture, 0, sizeof(before_capture));
  memset(&after_capture, 0, sizeof(after_capture));
  memset(&reopened_capture, 0, sizeof(reopened_capture));
  memset(&object_opts, 0, sizeof(object_opts));
  memset(&selector, 0, sizeof(selector));
  memset(&object_info, 0, sizeof(object_info));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&dequeue_opts, 0, sizeof(dequeue_opts));
  memset(&enqueued, 0, sizeof(enqueued));
  memset(&dequeued, 0, sizeof(dequeued));
  memset(payload, 'x', sizeof(payload));
  store = NULL;
  body = NULL;
  last_version = 0L;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  meta.owner = "owner-a";
  meta.lease_id = "lease-a";
  meta.txn_id = "txn-a";
  meta.state_etag = "state-a";
  meta.version = 10L;
  meta.fencing_token = 11L;
  rc = store->store_meta(store, "default", "lease-key", &meta, NULL,
                         &meta_res, &error);
  assert_int_equal(rc, LC_OK);

  query_req.namespace_name = "default";
  rc = store->query_index_scan(store, &query_req, capture_scan_row,
                               &before_capture, &before_query, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(before_capture.count, 1U);
  assert_string_equal(before_capture.keys[0], "lease-key");
  assert_true(before_query.index_seq >= 10UL);

  object_opts.name = "live.txt";
  object_opts.content_type = "text/plain";
  source = source_from_text("object-live");
  rc = store->put_object(store, "default", "object-key", source, &object_opts,
                         &object_info, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_object_info_cleanup(&allocator, &object_info);

  enqueue_opts.content_type = "text/plain";
  enqueue_opts.visibility_timeout_seconds = 30L;
  enqueue_opts.ttl_seconds = 3600L;
  enqueue_opts.max_attempts = 3;
  source = source_from_text("queue-live");
  rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                              &enqueued, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  state_opts.content_type = "application/octet-stream";
  for (index = 0U; index < 80U; ++index) {
    source = NULL;
    rc = lc_source_from_memory(payload, sizeof(payload), &source, &error);
    assert_int_equal(rc, LC_OK);
    rc = store->write_state(store, "default", "hot-key", source, &state_opts,
                            &put_res, &error);
    lc_source_close(source);
    assert_int_equal(rc, LC_OK);
    last_version = put_res.new_version;
    lc_pouch_put_state_res_cleanup(&allocator, &put_res);
    memset(&put_res, 0, sizeof(put_res));
  }

  assert_true(test_log_size(root) < (off_t)(80U * (sizeof(payload) + 128U)));
  assert_true(count_log_records_of_type(root, TEST_POUCH_RECORD_STATE_PUT) <
              25U);

  rc = store->query_index_scan(store, &query_req, capture_scan_row,
                               &after_capture, &after_query, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(after_capture.count, 1U);
  assert_string_equal(after_capture.keys[0], "lease-key");
  assert_true(after_query.index_seq >= before_query.index_seq);

  rc = store->read_state(store, "default", "hot-key", &body, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(body);
  assert_int_equal(state_info.version, last_version);
  read_length = read_source_count_x(body);
  assert_int_equal(read_length, sizeof(payload));
  lc_source_close(body);
  body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  rc = store->query_index_scan(store, &query_req, capture_scan_row,
                               &reopened_capture, &reopened_query, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(reopened_capture.count, 1U);
  assert_string_equal(reopened_capture.keys[0], "lease-key");
  assert_true(reopened_query.index_seq >= after_query.index_seq);

  rc = store->read_state(store, "default", "hot-key", &body, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(body);
  assert_int_equal(state_info.version, last_version);
  read_length = read_source_count_x(body);
  assert_int_equal(read_length, sizeof(payload));
  lc_source_close(body);
  body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  rc = store->load_meta(store, "default", "lease-key", &loaded_meta, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(loaded_meta.found);
  assert_string_equal(loaded_meta.etag, meta_res.etag);
  assert_string_equal(loaded_meta.meta.owner, "owner-a");
  lc_pouch_meta_record_cleanup(&allocator, &loaded_meta);

  selector.name = "live.txt";
  rc = store->get_object(store, "default", "object-key", &selector, &body,
                         &object_info, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(body);
  text = read_source_text(body);
  assert_string_equal(text, "object-live");
  free(text);
  lc_source_close(body);
  body = NULL;
  lc_pouch_object_info_cleanup(&allocator, &object_info);

  dequeue_opts.owner = "worker-a";
  dequeue_opts.visibility_timeout_seconds = 30L;
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts, &body,
                              &dequeued, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(body);
  assert_string_equal(dequeued.message_id, enqueued.message_id);
  text = read_source_text(body);
  assert_string_equal(text, "queue-live");
  free(text);
  lc_source_close(body);
  body = NULL;
  lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);

  source = source_from_text("after-compact");
  rc = store->write_state(store, "default", "after-key", source, NULL,
                          &after_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_true(after_res.new_version > last_version);

  lc_pouch_put_state_res_cleanup(&allocator, &after_res);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &before_query);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &after_query);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &reopened_query);
  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);
  lc_pouch_store_meta_res_cleanup(&allocator, &meta_res);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_manual_compaction_reports_stats_and_preserves_state(
    void **state) {
  char root[256];
  char payload[4096];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *body;
  lc_pouch_put_state_opts state_opts;
  lc_pouch_put_state_res put_res;
  lc_pouch_state_info state_info;
  lc_pouch_compaction_res compacted;
  lc_error error;
  off_t before_log_size;
  off_t before_query_size;
  long last_version;
  size_t index;
  size_t read_length;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "manual-compact");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&state_opts, 0, sizeof(state_opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&state_info, 0, sizeof(state_info));
  memset(&compacted, 0, sizeof(compacted));
  memset(payload, 'x', sizeof(payload));
  store = NULL;
  body = NULL;
  last_version = 0L;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(store->compact);

  state_opts.content_type = "application/octet-stream";
  for (index = 0U; index < 8U; ++index) {
    source = NULL;
    rc = lc_source_from_memory(payload, sizeof(payload), &source, &error);
    assert_int_equal(rc, LC_OK);
    rc = store->write_state(store, "default", "hot-key", source, &state_opts,
                            &put_res, &error);
    lc_source_close(source);
    assert_int_equal(rc, LC_OK);
    last_version = put_res.new_version;
    lc_pouch_put_state_res_cleanup(&allocator, &put_res);
    memset(&put_res, 0, sizeof(put_res));
  }

  before_log_size = test_log_size(root);
  before_query_size = test_query_index_size(root);
  assert_true(before_log_size > 0);
  rc = store->compact(store, "force", &compacted, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(compacted.mode, "force");
  assert_null(compacted.skip_reason);
  assert_int_equal(compacted.accepted, 1);
  assert_int_equal(compacted.compacted, 1);
  assert_int_equal(compacted.skipped, 0);
  assert_int_equal(compacted.before_log_bytes, (unsigned long)before_log_size);
  assert_int_equal(compacted.before_query_index_bytes,
                   (unsigned long)before_query_size);
  assert_true(compacted.before_record_count > compacted.after_record_count);
  assert_true(compacted.after_record_count <= compacted.live_record_count);
  assert_true(compacted.after_log_bytes < compacted.before_log_bytes);
  lc_pouch_compaction_res_cleanup(&allocator, &compacted);

  rc = store->read_state(store, "default", "hot-key", &body, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(body);
  assert_int_equal(state_info.version, last_version);
  read_length = read_source_count_x(body);
  assert_int_equal(read_length, sizeof(payload));
  lc_source_close(body);
  body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->read_state(store, "default", "hot-key", &body, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(body);
  assert_int_equal(state_info.version, last_version);
  read_length = read_source_count_x(body);
  assert_int_equal(read_length, sizeof(payload));
  lc_source_close(body);
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_compaction_if_needed_skip_and_allocator_failure(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_compaction_res skipped;
  lc_pouch_compaction_res failed;
  lc_error error;
  off_t before_log_size;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "compact-if-needed");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&skipped, 0, sizeof(skipped));
  memset(&failed, 0, sizeof(failed));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  before_log_size = test_log_size(root);

  rc = store->compact(store, "if_needed", &skipped, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(skipped.mode, "if_needed");
  assert_string_equal(skipped.skip_reason, "below-min-log-size");
  assert_int_equal(skipped.accepted, 1);
  assert_int_equal(skipped.compacted, 0);
  assert_int_equal(skipped.skipped, 1);
  assert_int_equal(skipped.before_log_bytes, (unsigned long)before_log_size);
  assert_int_equal(skipped.after_log_bytes, skipped.before_log_bytes);
  assert_int_equal(test_log_size(root), before_log_size);
  lc_pouch_compaction_res_cleanup(&allocator, &skipped);

  rc = store->compact(store, "later", &failed, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  lc_error_cleanup(&error);
  memset(&error, 0, sizeof(error));

  tracked.fail_malloc_size = strlen("if_needed") + 1U;
  rc = store->compact(store, "if_needed", &failed, &error);
  tracked.fail_malloc_size = 0U;
  assert_int_equal(rc, LC_ERR_NOMEM);
  assert_null(failed.mode);
  assert_null(failed.skip_reason);
  assert_int_equal(test_log_size(root), before_log_size);
  lc_error_cleanup(&error);
  memset(&error, 0, sizeof(error));

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_compaction_preserves_promoted_staged_state_link(void **state) {
  char root[256];
  char payload[4096];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  counting_source staged_source;
  lc_source *source;
  lc_source *body;
  lc_pouch_put_state_opts state_opts;
  lc_pouch_put_state_res staged;
  lc_pouch_put_state_res promoted;
  lc_pouch_put_state_res churned;
  lc_pouch_state_info info;
  lc_error error;
  size_t index;
  size_t read_length;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "staged-link-compact");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&state_opts, 0, sizeof(state_opts));
  memset(&staged, 0, sizeof(staged));
  memset(&promoted, 0, sizeof(promoted));
  memset(&churned, 0, sizeof(churned));
  memset(&info, 0, sizeof(info));
  memset(payload, 'x', sizeof(payload));
  store = NULL;
  body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  state_opts.content_type = "application/octet-stream";
  counting_source_init(&staged_source, 128U * 1024U);
  rc = store->stage_state(store, "default", "linked-key", "txn-compact",
                          &staged_source.pub, &state_opts, &staged, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->promote_staged_state(store, "default", "linked-key",
                                   "txn-compact", NULL, &promoted, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(promoted.new_state_etag, staged.new_state_etag);
  assert_int_equal(promoted.bytes, 128L * 1024L);
  assert_int_equal(count_log_records_of_type(root, TEST_POUCH_RECORD_STATE_LINK),
                   1U);

  for (index = 0U; index < 80U; ++index) {
    source = NULL;
    rc = lc_source_from_memory(payload, sizeof(payload), &source, &error);
    assert_int_equal(rc, LC_OK);
    rc = store->write_state(store, "default", "hot-key", source, &state_opts,
                            &churned, &error);
    lc_source_close(source);
    assert_int_equal(rc, LC_OK);
    lc_pouch_put_state_res_cleanup(&allocator, &churned);
    memset(&churned, 0, sizeof(churned));
  }
  assert_int_equal(count_log_records_of_type(root, TEST_POUCH_RECORD_STATE_LINK),
                   0U);

  rc = store->read_state(store, "default", "linked-key", &body, &info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(body);
  assert_false(info.no_content);
  assert_string_equal(info.etag, promoted.new_state_etag);
  assert_int_equal(info.version, promoted.new_version);
  read_length = read_source_count_x(body);
  assert_int_equal(read_length, 128U * 1024U);
  lc_source_close(body);
  body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &info);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->read_state(store, "default", "linked-key", &body, &info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(body);
  assert_false(info.no_content);
  assert_string_equal(info.etag, promoted.new_state_etag);
  assert_int_equal(info.version, promoted.new_version);
  read_length = read_source_count_x(body);
  assert_int_equal(read_length, 128U * 1024U);
  lc_source_close(body);
  body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &info);

  lc_pouch_put_state_res_cleanup(&allocator, &staged);
  lc_pouch_put_state_res_cleanup(&allocator, &promoted);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_independent_handle_refreshes_after_log_replacement(
    void **state) {
  char root[256];
  char payload[4096];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *first;
  lc_pouch_store *second;
  lc_source *source;
  lc_source *body;
  lc_pouch_put_state_opts state_opts;
  lc_pouch_put_state_res put_res;
  lc_pouch_state_info state_info;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res meta_res;
  lc_pouch_meta_record loaded_meta;
  lc_error error;
  size_t index;
  size_t read_length;
  long last_version;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "replace-refresh");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&state_opts, 0, sizeof(state_opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&state_info, 0, sizeof(state_info));
  memset(&meta, 0, sizeof(meta));
  memset(&meta_res, 0, sizeof(meta_res));
  memset(&loaded_meta, 0, sizeof(loaded_meta));
  memset(payload, 'x', sizeof(payload));
  first = NULL;
  second = NULL;
  body = NULL;
  last_version = 0L;

  rc = lc_pouch_disk_open(root, &allocator, &first, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_disk_open(root, &allocator, &second, &error);
  assert_int_equal(rc, LC_OK);

  rc = second->load_meta(second, "default", "lease-key", &loaded_meta,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_false(loaded_meta.found);
  lc_pouch_meta_record_cleanup(&allocator, &loaded_meta);

  meta.owner = "owner-a";
  meta.lease_id = "lease-a";
  meta.state_etag = "state-a";
  meta.version = 10L;
  rc = first->store_meta(first, "default", "lease-key", &meta, NULL,
                         &meta_res, &error);
  assert_int_equal(rc, LC_OK);

  state_opts.content_type = "application/octet-stream";
  for (index = 0U; index < 80U; ++index) {
    source = NULL;
    rc = lc_source_from_memory(payload, sizeof(payload), &source, &error);
    assert_int_equal(rc, LC_OK);
    rc = first->write_state(first, "default", "hot-key", source, &state_opts,
                            &put_res, &error);
    lc_source_close(source);
    assert_int_equal(rc, LC_OK);
    last_version = put_res.new_version;
    lc_pouch_put_state_res_cleanup(&allocator, &put_res);
    memset(&put_res, 0, sizeof(put_res));
  }

  assert_true(count_log_records_of_type(root, TEST_POUCH_RECORD_STATE_PUT) <
              25U);

  rc = second->load_meta(second, "default", "lease-key", &loaded_meta,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_true(loaded_meta.found);
  assert_string_equal(loaded_meta.etag, meta_res.etag);
  assert_string_equal(loaded_meta.meta.owner, "owner-a");
  lc_pouch_meta_record_cleanup(&allocator, &loaded_meta);

  rc = second->read_state(second, "default", "hot-key", &body, &state_info,
                          &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(body);
  assert_int_equal(state_info.version, last_version);
  read_length = read_source_count_x(body);
  assert_int_equal(read_length, sizeof(payload));
  lc_source_close(body);
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  lc_pouch_store_meta_res_cleanup(&allocator, &meta_res);
  rc = second->close(second, &error);
  assert_int_equal(rc, LC_OK);
  rc = first->close(first, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_queue_dequeue_survives_compaction_refresh(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *body;
  lc_pouch_enqueue_opts enqueue_opts;
  lc_pouch_dequeue_opts dequeue_opts;
  lc_pouch_queue_message_info enqueued;
  lc_pouch_queue_message_info dequeued;
  lc_pouch_queue_ref ref;
  lc_error error;
  char *text;
  size_t index;
  int acked;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-dequeue-compact");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&dequeue_opts, 0, sizeof(dequeue_opts));
  store = NULL;
  body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  enqueue_opts.content_type = "text/plain";
  enqueue_opts.visibility_timeout_seconds = 30L;
  enqueue_opts.ttl_seconds = 3600L;
  enqueue_opts.max_attempts = 3;
  dequeue_opts.owner = "worker-a";
  dequeue_opts.visibility_timeout_seconds = 30L;

  for (index = 0U; index < 120U; ++index) {
    memset(&enqueued, 0, sizeof(enqueued));
    memset(&dequeued, 0, sizeof(dequeued));
    memset(&ref, 0, sizeof(ref));
    source = source_from_text("queue-payload");
    rc = store->enqueue_message(store, "default", "jobs", source,
                                &enqueue_opts, &enqueued, &error);
    lc_source_close(source);
    assert_int_equal(rc, LC_OK);

    rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts,
                                &body, &dequeued, &error);
    assert_int_equal(rc, LC_OK);
    assert_non_null(body);
    assert_string_equal(dequeued.message_id, enqueued.message_id);
    text = read_source_text(body);
    assert_string_equal(text, "queue-payload");
    free(text);
    lc_source_close(body);
    body = NULL;

    ref.namespace_name = dequeued.namespace_name;
    ref.queue = dequeued.queue;
    ref.message_id = dequeued.message_id;
    ref.lease_id = dequeued.lease_id;
    ref.txn_id = dequeued.txn_id;
    ref.fencing_token = dequeued.fencing_token;
    ref.meta_etag = dequeued.meta_etag;
    acked = 0;
    rc = store->ack_message(store, &ref, &acked, &error);
    assert_int_equal(rc, LC_OK);
    assert_true(acked);

    lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);
    lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);
  }

  assert_true(test_log_size(root) < (off_t)(120U * 256U));

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_empty_identifiers_are_rejected_before_append(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts state_opts;
  lc_pouch_put_state_res state_res;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res meta_res;
  lc_pouch_put_object_opts object_opts;
  lc_pouch_object_info object_info;
  lc_pouch_enqueue_opts enqueue_opts;
  lc_pouch_queue_message_info queue_info;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "empty-identifiers");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&state_opts, 0, sizeof(state_opts));
  memset(&state_res, 0, sizeof(state_res));
  memset(&meta, 0, sizeof(meta));
  memset(&meta_res, 0, sizeof(meta_res));
  memset(&object_opts, 0, sizeof(object_opts));
  memset(&object_info, 0, sizeof(object_info));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&queue_info, 0, sizeof(queue_info));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("state");
  rc = store->write_state(store, "", "key", source, &state_opts, &state_res,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(count_log_records_of_type(root, TEST_POUCH_RECORD_STATE_PUT),
                   0U);
  lc_error_cleanup(&error);

  meta.version = 1L;
  rc = store->store_meta(store, "default", "", &meta, NULL, &meta_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(count_log_records_of_type(root, TEST_POUCH_RECORD_META_PUT),
                   0U);
  lc_error_cleanup(&error);

  object_opts.name = "";
  source = source_from_text("object");
  rc = store->put_object(store, "default", "key", source, &object_opts,
                         &object_info, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(count_log_records_of_type(root, TEST_POUCH_RECORD_OBJECT_PUT),
                   0U);
  lc_error_cleanup(&error);

  source = source_from_text("queue");
  rc = store->enqueue_message(store, "default", "", source, &enqueue_opts,
                              &queue_info, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(count_log_records_of_type(root, TEST_POUCH_RECORD_QUEUE_PUT),
                   0U);
  lc_error_cleanup(&error);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_pathlike_identifiers_are_rejected_before_append(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts state_opts;
  lc_pouch_put_state_res state_res;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res meta_res;
  lc_pouch_enqueue_opts enqueue_opts;
  lc_pouch_queue_message_info queue_info;
  lc_pouch_scan_meta_req scan_req;
  lc_pouch_scan_meta_res scan_res;
  scan_capture capture;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "pathlike-identifiers");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&state_opts, 0, sizeof(state_opts));
  memset(&state_res, 0, sizeof(state_res));
  memset(&meta, 0, sizeof(meta));
  memset(&meta_res, 0, sizeof(meta_res));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&queue_info, 0, sizeof(queue_info));
  memset(&scan_req, 0, sizeof(scan_req));
  memset(&scan_res, 0, sizeof(scan_res));
  memset(&capture, 0, sizeof(capture));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("state");
  rc = store->write_state(store, "bad/ns", "key", source, &state_opts,
                          &state_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message, "write_state namespace must not contain '/'");
  assert_int_equal(count_log_records_of_type(root, TEST_POUCH_RECORD_STATE_PUT),
                   0U);
  lc_error_cleanup(&error);

  source = source_from_text("state");
  rc = store->write_state(store, "default", "alpha//bravo", source,
                          &state_opts, &state_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(
      error.message,
      "write_state key must not contain empty path components");
  assert_int_equal(count_log_records_of_type(root, TEST_POUCH_RECORD_STATE_PUT),
                   0U);
  lc_error_cleanup(&error);

  meta.version = 1L;
  rc = store->store_meta(store, "default", "../bad", &meta, NULL, &meta_res,
                         &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "store_meta key must not contain dot path components");
  assert_int_equal(count_log_records_of_type(root, TEST_POUCH_RECORD_META_PUT),
                   0U);
  lc_error_cleanup(&error);

  source = source_from_text("queue");
  rc = store->enqueue_message(store, "default", "jobs/./bad", source,
                              &enqueue_opts, &queue_info, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(
      error.message,
      "enqueue_message queue must not contain dot path components");
  assert_int_equal(count_log_records_of_type(root, TEST_POUCH_RECORD_QUEUE_PUT),
                   0U);
  lc_error_cleanup(&error);

  scan_req.namespace_name = "bad/ns";
  rc = store->scan_meta(store, &scan_req, capture_scan_row, &capture,
                        &scan_res, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message, "scan_meta namespace must not contain '/'");
  lc_error_cleanup(&error);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_queue_mutations_touch_wake_marker(void **state) {
  char root[256];
  char marker_path[512];
  char marker_text[512];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *payload;
  lc_pouch_enqueue_opts enqueue_opts;
  lc_pouch_dequeue_opts dequeue_opts;
  lc_pouch_queue_message_info enqueued;
  lc_pouch_queue_message_info dequeued;
  lc_pouch_queue_message_info nacked;
  lc_pouch_queue_ref ref;
  lc_error error;
  int acked;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-wake-marker");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&dequeue_opts, 0, sizeof(dequeue_opts));
  memset(&enqueued, 0, sizeof(enqueued));
  memset(&dequeued, 0, sizeof(dequeued));
  memset(&nacked, 0, sizeof(nacked));
  memset(&ref, 0, sizeof(ref));
  store = NULL;
  payload = NULL;

  test_queue_wake_marker_path(root, "default", "jobs/high", marker_path,
                              sizeof(marker_path));
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(test_count_queue_wake_markers(root), 0U);

  enqueue_opts.content_type = "text/plain";
  enqueue_opts.visibility_timeout_seconds = 30L;
  enqueue_opts.ttl_seconds = 3600L;
  enqueue_opts.max_attempts = 3;
  source = source_from_text("wake-payload");
  rc = store->enqueue_message(store, "default", "jobs/high", source,
                              &enqueue_opts, &enqueued, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(test_count_queue_wake_markers(root), 1U);
  test_read_file_text(marker_path, marker_text, sizeof(marker_text));
  assert_non_null(strstr(marker_text, "namespace=default\n"));
  assert_non_null(strstr(marker_text, "queue=jobs/high\n"));
  assert_non_null(strstr(marker_text, "sequence=1\n"));

  dequeue_opts.owner = "worker";
  dequeue_opts.visibility_timeout_seconds = 30L;
  rc = store->dequeue_message(store, "default", "jobs/high", &dequeue_opts,
                              &payload, &dequeued, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(payload);
  lc_source_close(payload);
  payload = NULL;
  test_read_file_text(marker_path, marker_text, sizeof(marker_text));
  assert_non_null(strstr(marker_text, "sequence=2\n"));

  ref.namespace_name = dequeued.namespace_name;
  ref.queue = dequeued.queue;
  ref.message_id = dequeued.message_id;
  ref.lease_id = dequeued.lease_id;
  ref.txn_id = dequeued.txn_id;
  ref.fencing_token = dequeued.fencing_token;
  ref.meta_etag = dequeued.meta_etag;
  rc = store->nack_message(store, &ref, 0L, 0, &nacked, &error);
  assert_int_equal(rc, LC_OK);
  test_read_file_text(marker_path, marker_text, sizeof(marker_text));
  assert_non_null(strstr(marker_text, "sequence=3\n"));

  lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);
  memset(&dequeued, 0, sizeof(dequeued));
  rc = store->dequeue_message(store, "default", "jobs/high", &dequeue_opts,
                              &payload, &dequeued, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(payload);
  lc_source_close(payload);
  payload = NULL;
  test_read_file_text(marker_path, marker_text, sizeof(marker_text));
  assert_non_null(strstr(marker_text, "sequence=4\n"));

  ref.namespace_name = dequeued.namespace_name;
  ref.queue = dequeued.queue;
  ref.message_id = dequeued.message_id;
  ref.lease_id = dequeued.lease_id;
  ref.txn_id = dequeued.txn_id;
  ref.fencing_token = dequeued.fencing_token;
  ref.meta_etag = dequeued.meta_etag;
  acked = 0;
  rc = store->ack_message(store, &ref, &acked, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(acked);
  test_read_file_text(marker_path, marker_text, sizeof(marker_text));
  assert_non_null(strstr(marker_text, "sequence=5\n"));

  lc_pouch_queue_message_info_cleanup(&allocator, &nacked);
  lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);
  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_queue_wake_marker_failure_does_not_rollback_enqueue(
    void **state) {
  char root[256];
  char marker_path[512];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *payload;
  lc_pouch_enqueue_opts enqueue_opts;
  lc_pouch_dequeue_opts dequeue_opts;
  lc_pouch_queue_message_info enqueued;
  lc_pouch_queue_message_info dequeued;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-wake-marker-failure");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&dequeue_opts, 0, sizeof(dequeue_opts));
  memset(&enqueued, 0, sizeof(enqueued));
  memset(&dequeued, 0, sizeof(dequeued));
  store = NULL;
  payload = NULL;

  test_queue_wake_marker_path(root, "default", "jobs", marker_path,
                              sizeof(marker_path));
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(mkdir(marker_path, 0777), 0);

  enqueue_opts.content_type = "text/plain";
  enqueue_opts.visibility_timeout_seconds = 30L;
  enqueue_opts.ttl_seconds = 3600L;
  enqueue_opts.max_attempts = 3;
  source = source_from_text("queued despite wake failure");
  rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                              &enqueued, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  assert_int_equal(rmdir(marker_path), 0);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  dequeue_opts.owner = "worker";
  dequeue_opts.visibility_timeout_seconds = 30L;
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts,
                              &payload, &dequeued, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(payload);
  assert_string_equal(dequeued.message_id, enqueued.message_id);
  text = read_source_text(payload);
  assert_string_equal(text, "queued despite wake failure");
  free(text);
  lc_source_close(payload);

  lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);
  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_queue_wake_status_reports_polling_marker_mode(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_queue_wake_status status;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-wake-status");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&status, 0, sizeof(status));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(store->queue_wake_status);

  rc = store->queue_wake_status(store, "default", "jobs/high", &status,
                                &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(status.mode, "polling");
  assert_string_equal(status.reason,
                      "disk pouch uses polling with best-effort queue marker "
                      "hints");
  assert_true(status.uses_marker_hints);
  assert_false(status.uses_filesystem_notifications);
  lc_pouch_queue_wake_status_cleanup(&allocator, &status);

  rc = store->queue_wake_status(store, "bad/ns", "jobs", &status, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "queue_wake_status namespace must not contain '/'");
  lc_error_cleanup(&error);

  tracked.fail_malloc_size = strlen("polling") + 1U;
  rc = store->queue_wake_status(store, "default", "jobs", &status, &error);
  assert_int_equal(rc, LC_ERR_NOMEM);
  assert_string_equal(error.message, "failed to copy pouch queue wake status");
  assert_null(status.mode);
  assert_null(status.reason);
  assert_false(status.uses_marker_hints);
  assert_false(status.uses_filesystem_notifications);
  tracked.fail_malloc_size = 0U;
  lc_error_cleanup(&error);

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

static void test_queue_enqueue_index_allocation_failure_replays_cleanly(
    void **state) {
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
  lc_pouch_queue_stats stats;
  lc_error error;
  char *text;
  const char *content_type;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-enqueue-index-nomem");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&dequeue_opts, 0, sizeof(dequeue_opts));
  memset(&enqueued, 0, sizeof(enqueued));
  memset(&dequeued, 0, sizeof(dequeued));
  memset(&stats, 0, sizeof(stats));
  store = NULL;
  payload = NULL;
  content_type = "application/x-pouch-queue-index-replay";

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  enqueue_opts.content_type = content_type;
  enqueue_opts.visibility_timeout_seconds = 30L;
  enqueue_opts.ttl_seconds = 3600L;
  enqueue_opts.max_attempts = 3;
  tracked.fail_malloc_size = strlen(content_type) + 1U;
  source = source_from_text("queued-after-index-failure");
  rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                              &enqueued, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_NOMEM);
  tracked.fail_malloc_size = 0U;
  lc_error_cleanup(&error);
  memset(&error, 0, sizeof(error));
  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);

  rc = store->queue_stats(store, "default", "jobs", &stats, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats.available, 1);
  assert_int_equal(stats.pending_candidates, 1);
  assert_non_null(stats.head_message_id);
  lc_pouch_queue_stats_cleanup(&allocator, &stats);

  dequeue_opts.owner = "worker-a";
  dequeue_opts.visibility_timeout_seconds = 30L;
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts,
                              &payload, &dequeued, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(payload);
  assert_string_equal(dequeued.payload_content_type, content_type);
  text = read_source_text(payload);
  assert_string_equal(text, "queued-after-index-failure");
  free(text);
  lc_source_close(payload);

  lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_queue_ref_requires_current_meta_etag(void **state) {
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
  lc_pouch_queue_ref ref;
  lc_error error;
  int acked;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-ref-etag");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&dequeue_opts, 0, sizeof(dequeue_opts));
  memset(&enqueued, 0, sizeof(enqueued));
  memset(&dequeued, 0, sizeof(dequeued));
  memset(&ref, 0, sizeof(ref));
  store = NULL;
  payload = NULL;
  acked = 0;

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

  dequeue_opts.owner = "worker-a";
  dequeue_opts.visibility_timeout_seconds = 30L;
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts,
                              &payload, &dequeued, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(payload);
  lc_source_close(payload);

  ref.namespace_name = dequeued.namespace_name;
  ref.queue = dequeued.queue;
  ref.message_id = dequeued.message_id;
  ref.lease_id = dequeued.lease_id;
  ref.txn_id = dequeued.txn_id;
  ref.fencing_token = dequeued.fencing_token;
  ref.meta_etag = NULL;
  rc = store->ack_message(store, &ref, &acked, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(error.code, LC_ERR_INVALID);
  assert_false(acked);
  lc_error_cleanup(&error);
  memset(&error, 0, sizeof(error));

  ref.meta_etag = "stale-meta-etag";
  rc = store->ack_message(store, &ref, &acked, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 409L);
  assert_string_equal(error.server_code, "queue_lease_not_active");
  assert_false(acked);
  lc_error_cleanup(&error);
  memset(&error, 0, sizeof(error));

  ref.meta_etag = dequeued.meta_etag;
  rc = store->ack_message(store, &ref, &acked, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(acked);

  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);
  lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_queue_delay_hides_until_visible(void **state) {
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
  lc_pouch_queue_stats stats;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-delay");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&dequeue_opts, 0, sizeof(dequeue_opts));
  memset(&enqueued, 0, sizeof(enqueued));
  memset(&dequeued, 0, sizeof(dequeued));
  memset(&stats, 0, sizeof(stats));
  store = NULL;
  payload = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  enqueue_opts.content_type = "text/plain";
  enqueue_opts.delay_seconds = 1L;
  enqueue_opts.visibility_timeout_seconds = 30L;
  enqueue_opts.ttl_seconds = 3600L;
  enqueue_opts.max_attempts = 3;
  source = source_from_text("delayed-payload");
  rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                              &enqueued, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_true(enqueued.not_visible_until_unix > enqueued.enqueued_at_unix);

  rc = store->queue_stats(store, "default", "jobs", &stats, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats.available, 0);
  assert_int_equal(stats.pending_candidates, 1);
  assert_null(stats.head_message_id);
  lc_pouch_queue_stats_cleanup(&allocator, &stats);

  dequeue_opts.owner = "worker-a";
  dequeue_opts.visibility_timeout_seconds = 30L;
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts,
                              &payload, &dequeued, &error);
  assert_int_equal(rc, LC_OK);
  assert_null(payload);
  assert_null(dequeued.message_id);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  rc = store->queue_stats(store, "default", "jobs", &stats, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats.available, 0);
  assert_int_equal(stats.pending_candidates, 1);
  assert_null(stats.head_message_id);
  lc_pouch_queue_stats_cleanup(&allocator, &stats);

  sleep(2U);

  rc = store->queue_stats(store, "default", "jobs", &stats, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats.available, 1);
  assert_int_equal(stats.pending_candidates, 1);
  assert_string_equal(stats.head_message_id, enqueued.message_id);
  lc_pouch_queue_stats_cleanup(&allocator, &stats);

  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts,
                              &payload, &dequeued, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(payload);
  assert_string_equal(dequeued.message_id, enqueued.message_id);
  assert_int_equal(dequeued.attempts, 1);
  text = read_source_text(payload);
  assert_string_equal(text, "delayed-payload");
  free(text);
  lc_source_close(payload);

  lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);
  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_queue_ttl_expiry_removes_pending_candidate_after_replay(
    void **state) {
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
  lc_pouch_queue_stats stats;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-ttl-expired");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&dequeue_opts, 0, sizeof(dequeue_opts));
  memset(&enqueued, 0, sizeof(enqueued));
  memset(&dequeued, 0, sizeof(dequeued));
  memset(&stats, 0, sizeof(stats));
  store = NULL;
  payload = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  enqueue_opts.content_type = "text/plain";
  enqueue_opts.visibility_timeout_seconds = 30L;
  enqueue_opts.ttl_seconds = 1L;
  enqueue_opts.max_attempts = 3;
  source = source_from_text("expires");
  rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                              &enqueued, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_true(enqueued.expires_at_unix > enqueued.enqueued_at_unix);

  rc = store->queue_stats(store, "default", "jobs", &stats, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats.available, 1);
  assert_int_equal(stats.pending_candidates, 1);
  assert_string_equal(stats.head_message_id, enqueued.message_id);
  lc_pouch_queue_stats_cleanup(&allocator, &stats);

  sleep(2U);

  rc = store->queue_stats(store, "default", "jobs", &stats, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats.available, 0);
  assert_int_equal(stats.pending_candidates, 0);
  assert_null(stats.head_message_id);
  lc_pouch_queue_stats_cleanup(&allocator, &stats);

  dequeue_opts.owner = "worker-a";
  dequeue_opts.visibility_timeout_seconds = 30L;
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts,
                              &payload, &dequeued, &error);
  assert_int_equal(rc, LC_OK);
  assert_null(payload);
  assert_null(dequeued.message_id);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  rc = store->queue_stats(store, "default", "jobs", &stats, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats.available, 0);
  assert_int_equal(stats.pending_candidates, 0);
  assert_null(stats.head_message_id);

  lc_pouch_queue_stats_cleanup(&allocator, &stats);
  lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);
  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_queue_nack_allocation_failure_preserves_active_lease(
    void **state) {
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
  int acked;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-nack-nomem");
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
  acked = 0;

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

  dequeue_opts.owner = "worker-a";
  dequeue_opts.visibility_timeout_seconds = 45L;
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts,
                              &payload, &dequeued, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(payload);
  lc_source_close(payload);

  ref.namespace_name = dequeued.namespace_name;
  ref.queue = dequeued.queue;
  ref.message_id = dequeued.message_id;
  ref.lease_id = dequeued.lease_id;
  ref.txn_id = dequeued.txn_id;
  ref.fencing_token = dequeued.fencing_token;
  ref.meta_etag = dequeued.meta_etag;

  tracked.fail_malloc_size = 17U;
  rc = store->nack_message(store, &ref, 0L, 1, &updated, &error);
  assert_int_equal(rc, LC_ERR_NOMEM);
  tracked.fail_malloc_size = 0U;
  lc_error_cleanup(&error);

  rc = store->ack_message(store, &ref, &acked, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(acked);

  rc = store->queue_stats(store, "default", "jobs", &stats, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats.available, 0);

  lc_pouch_queue_stats_cleanup(&allocator, &stats);
  lc_pouch_queue_message_info_cleanup(&allocator, &updated);
  lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);
  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_queue_extend_allocation_failure_preserves_active_lease(
    void **state) {
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
  int acked;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-extend-nomem");
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
  acked = 0;

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

  dequeue_opts.owner = "worker-a";
  dequeue_opts.visibility_timeout_seconds = 45L;
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts,
                              &payload, &dequeued, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(payload);
  lc_source_close(payload);

  ref.namespace_name = dequeued.namespace_name;
  ref.queue = dequeued.queue;
  ref.message_id = dequeued.message_id;
  ref.lease_id = dequeued.lease_id;
  ref.txn_id = dequeued.txn_id;
  ref.fencing_token = dequeued.fencing_token;
  ref.meta_etag = dequeued.meta_etag;

  tracked.fail_malloc_size = 17U;
  rc = store->extend_message(store, &ref, 90L, &updated, &error);
  assert_int_equal(rc, LC_ERR_NOMEM);
  tracked.fail_malloc_size = 0U;
  lc_error_cleanup(&error);

  rc = store->ack_message(store, &ref, &acked, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(acked);

  rc = store->queue_stats(store, "default", "jobs", &stats, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats.available, 0);

  lc_pouch_queue_stats_cleanup(&allocator, &stats);
  lc_pouch_queue_message_info_cleanup(&allocator, &updated);
  lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);
  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_queue_retry_exhaustion_is_not_pending_after_replay(
    void **state) {
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
  lc_pouch_queue_message_info nacked;
  lc_pouch_queue_ref ref;
  lc_pouch_queue_stats stats;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-exhausted");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&dequeue_opts, 0, sizeof(dequeue_opts));
  memset(&enqueued, 0, sizeof(enqueued));
  memset(&dequeued, 0, sizeof(dequeued));
  memset(&nacked, 0, sizeof(nacked));
  memset(&ref, 0, sizeof(ref));
  memset(&stats, 0, sizeof(stats));
  store = NULL;
  payload = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  enqueue_opts.content_type = "text/plain";
  enqueue_opts.visibility_timeout_seconds = 30L;
  enqueue_opts.ttl_seconds = 3600L;
  enqueue_opts.max_attempts = 1;
  source = source_from_text("one-shot");
  rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                              &enqueued, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  dequeue_opts.owner = "worker-a";
  dequeue_opts.visibility_timeout_seconds = 30L;
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts,
                              &payload, &dequeued, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(payload);
  lc_source_close(payload);
  payload = NULL;

  ref.namespace_name = dequeued.namespace_name;
  ref.queue = dequeued.queue;
  ref.message_id = dequeued.message_id;
  ref.lease_id = dequeued.lease_id;
  ref.txn_id = dequeued.txn_id;
  ref.fencing_token = dequeued.fencing_token;
  ref.meta_etag = dequeued.meta_etag;
  rc = store->nack_message(store, &ref, 0L, 1, &nacked, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(nacked.failure_attempts, 1);
  assert_int_equal(nacked.max_attempts, 1);

  rc = store->queue_stats(store, "default", "jobs", &stats, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats.available, 0);
  assert_int_equal(stats.pending_candidates, 0);
  lc_pouch_queue_stats_cleanup(&allocator, &stats);

  lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts,
                              &payload, &dequeued, &error);
  assert_int_equal(rc, LC_OK);
  assert_null(payload);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->queue_stats(store, "default", "jobs", &stats, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats.available, 0);
  assert_int_equal(stats.pending_candidates, 0);

  lc_pouch_queue_stats_cleanup(&allocator, &stats);
  lc_pouch_queue_message_info_cleanup(&allocator, &nacked);
  lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);
  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);
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

static void test_list_namespaces_reports_live_projection_names(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *first;
  lc_pouch_store *second;
  lc_source *source;
  lc_pouch_put_state_opts state_opts;
  lc_pouch_put_state_res state_res;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res meta_res;
  lc_pouch_put_object_opts object_opts;
  lc_pouch_object_info object_info;
  lc_pouch_enqueue_opts enqueue_opts;
  lc_pouch_queue_message_info enqueued;
  lc_pouch_namespace_list namespaces;
  lc_error error;
  int removed;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "namespaces");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&state_opts, 0, sizeof(state_opts));
  memset(&state_res, 0, sizeof(state_res));
  memset(&meta, 0, sizeof(meta));
  memset(&meta_res, 0, sizeof(meta_res));
  memset(&object_opts, 0, sizeof(object_opts));
  memset(&object_info, 0, sizeof(object_info));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&enqueued, 0, sizeof(enqueued));
  memset(&namespaces, 0, sizeof(namespaces));
  first = NULL;
  second = NULL;
  removed = 0;

  rc = lc_pouch_disk_open(root, &allocator, &first, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(first->list_namespaces);

  state_opts.content_type = "application/json";
  source = source_from_text("{\"ns\":\"zeta\"}");
  rc = first->write_state(first, "zeta", "state-key", source, &state_opts,
                          &state_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&allocator, &state_res);

  source = source_from_text("{\"ns\":\"alpha\"}");
  rc = first->write_state(first, "alpha", "duplicate-state", source,
                          &state_opts, &state_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&allocator, &state_res);

  source = source_from_text("{\"removed\":true}");
  rc = first->write_state(first, "removed", "removed-state", source,
                          &state_opts, &state_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  rc = first->remove_state(first, "removed", "removed-state",
                           state_res.new_state_etag, &removed, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(removed);
  lc_pouch_put_state_res_cleanup(&allocator, &state_res);

  meta.owner = "owner";
  meta.lease_id = "lease";
  meta.version = 100L;
  meta.fencing_token = 100L;
  rc = first->store_meta(first, "alpha", "meta-key", &meta, NULL, &meta_res,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &meta_res);

  object_opts.name = "blob";
  object_opts.content_type = "text/plain";
  source = source_from_text("object-body");
  rc = first->put_object(first, "gamma", "object-key", source, &object_opts,
                         &object_info, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_object_info_cleanup(&allocator, &object_info);

  enqueue_opts.content_type = "text/plain";
  enqueue_opts.ttl_seconds = 60L;
  enqueue_opts.max_attempts = 3;
  source = source_from_text("queue-body");
  rc = first->enqueue_message(first, "beta", "jobs", source, &enqueue_opts,
                              &enqueued, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);

  rc = lc_pouch_disk_open(root, &allocator, &second, &error);
  assert_int_equal(rc, LC_OK);
  rc = second->list_namespaces(second, &namespaces, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(namespaces.count, 4U);
  assert_string_equal(namespaces.names[0], "alpha");
  assert_string_equal(namespaces.names[1], "beta");
  assert_string_equal(namespaces.names[2], "gamma");
  assert_string_equal(namespaces.names[3], "zeta");
  lc_pouch_namespace_list_cleanup(&allocator, &namespaces);

  rc = second->close(second, &error);
  assert_int_equal(rc, LC_OK);
  second = NULL;

  tracked.fail_realloc_size = sizeof(char *);
  rc = first->list_namespaces(first, &namespaces, &error);
  assert_int_equal(rc, LC_ERR_NOMEM);
  assert_string_equal(error.message, "failed to copy pouch namespace list");
  assert_null(namespaces.names);
  assert_int_equal(namespaces.count, 0U);
  tracked.fail_realloc_size = 0U;
  lc_error_cleanup(&error);

  rc = first->close(first, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_backend_capabilities_report_disk_writer_model(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_backend_capabilities caps;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "backend-capabilities");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&caps, 0, sizeof(caps));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(store->backend_capabilities);

  rc = store->backend_capabilities(store, &caps, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(caps.backend_kind, "disk-log");
  assert_string_equal(caps.write_coordination, "advisory-file-lock");
  assert_true(caps.serializes_same_root_writers);
  assert_false(caps.general_concurrent_writer_backend);
  assert_true(caps.supports_crash_abort_marker);
  assert_true(caps.supports_backend_hash);
  lc_pouch_backend_capabilities_cleanup(&allocator, &caps);

  tracked.fail_malloc_size = strlen("disk-log") + 1U;
  rc = store->backend_capabilities(store, &caps, &error);
  assert_int_equal(rc, LC_ERR_NOMEM);
  assert_string_equal(error.message,
                      "failed to copy pouch backend capabilities");
  assert_null(caps.backend_kind);
  assert_null(caps.write_coordination);
  assert_false(caps.serializes_same_root_writers);
  assert_false(caps.general_concurrent_writer_backend);
  assert_false(caps.supports_crash_abort_marker);
  assert_false(caps.supports_backend_hash);
  tracked.fail_malloc_size = 0U;
  lc_error_cleanup(&error);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static int child_exit_code(pid_t pid);
static void child_process_backend_hash(const char *root, int start_fd);

static void test_backend_hash_create_race_publishes_single_identity(
    void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_error error;
  char *hash;
  int start_pipe[2];
  int first_code;
  int second_code;
  int rc;
  pid_t first_pid;
  pid_t second_pid;

  (void)state;
  test_root_path(root, sizeof(root), "backend-hash-race");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  store = NULL;
  hash = NULL;
  start_pipe[0] = -1;
  start_pipe[1] = -1;

  assert_int_equal(mkdir(root, 0777), 0);
  rc = pipe(start_pipe);
  assert_int_equal(rc, 0);
  first_pid = fork();
  assert_true(first_pid >= 0);
  if (first_pid == 0) {
    close(start_pipe[1]);
    child_process_backend_hash(root, start_pipe[0]);
  }
  second_pid = fork();
  assert_true(second_pid >= 0);
  if (second_pid == 0) {
    close(start_pipe[1]);
    child_process_backend_hash(root, start_pipe[0]);
  }
  close(start_pipe[0]);
  start_pipe[0] = -1;
  assert_int_equal(write(start_pipe[1], "xx", 2U), 2);
  close(start_pipe[1]);
  start_pipe[1] = -1;

  first_code = child_exit_code(first_pid);
  second_code = child_exit_code(second_pid);
  assert_int_equal(first_code, 0);
  assert_int_equal(second_code, 0);
  assert_int_equal(count_log_records_of_type(root, TEST_POUCH_RECORD_OBJECT_PUT),
                   1U);

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->backend_hash(store, &hash, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(hash);
  assert_true(strncmp(hash, "pouch-", 6U) == 0);

  lc_pouch_free(&allocator, hash);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_independent_handles_refresh_before_operations(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *first;
  lc_pouch_store *second;
  lc_source *source;
  lc_source *body;
  lc_pouch_put_state_opts state_opts;
  lc_pouch_put_state_res first_put;
  lc_pouch_put_state_res second_put;
  lc_pouch_state_info state_info;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res first_meta;
  lc_pouch_store_meta_res second_meta;
  lc_pouch_meta_record loaded_meta;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "shared-refresh");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&state_opts, 0, sizeof(state_opts));
  memset(&first_put, 0, sizeof(first_put));
  memset(&second_put, 0, sizeof(second_put));
  memset(&state_info, 0, sizeof(state_info));
  memset(&meta, 0, sizeof(meta));
  memset(&first_meta, 0, sizeof(first_meta));
  memset(&second_meta, 0, sizeof(second_meta));
  memset(&loaded_meta, 0, sizeof(loaded_meta));
  first = NULL;
  second = NULL;
  body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &first, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_disk_open(root, &allocator, &second, &error);
  assert_int_equal(rc, LC_OK);

  state_opts.content_type = "application/json";
  source = source_from_text("{\"owner\":\"first\"}");
  rc = first->write_state(first, "default", "shared-key", source, &state_opts,
                          &first_put, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  rc = second->read_state(second, "default", "shared-key", &body, &state_info,
                          &error);
  assert_int_equal(rc, LC_OK);
  assert_false(state_info.no_content);
  assert_string_equal(state_info.etag, first_put.new_state_etag);
  text = read_source_text(body);
  assert_string_equal(text, "{\"owner\":\"first\"}");
  free(text);
  lc_source_close(body);
  body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  state_opts.if_state_etag = first_put.new_state_etag;
  source = source_from_text("{\"owner\":\"second\"}");
  rc = second->write_state(second, "default", "shared-key", source,
                           &state_opts, &second_put, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_true(second_put.new_version > first_put.new_version);

  rc = first->read_state(first, "default", "shared-key", &body, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_false(state_info.no_content);
  assert_string_equal(state_info.etag, second_put.new_state_etag);
  text = read_source_text(body);
  assert_string_equal(text, "{\"owner\":\"second\"}");
  free(text);
  lc_source_close(body);
  body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  meta.owner = "first-owner";
  meta.lease_id = "lease-a";
  meta.version = 100L;
  meta.fencing_token = 1L;
  rc = first->store_meta(first, "default", "shared-key", &meta, NULL,
                         &first_meta, &error);
  assert_int_equal(rc, LC_OK);

  rc = second->load_meta(second, "default", "shared-key", &loaded_meta,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_true(loaded_meta.found);
  assert_string_equal(loaded_meta.etag, first_meta.etag);
  assert_string_equal(loaded_meta.meta.owner, "first-owner");
  lc_pouch_meta_record_cleanup(&allocator, &loaded_meta);

  meta.owner = "second-owner";
  meta.lease_id = "lease-b";
  meta.version = 101L;
  meta.fencing_token = 2L;
  rc = second->store_meta(second, "default", "shared-key", &meta,
                          first_meta.etag, &second_meta, &error);
  assert_int_equal(rc, LC_OK);

  rc = first->load_meta(first, "default", "shared-key", &loaded_meta, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(loaded_meta.found);
  assert_string_equal(loaded_meta.etag, second_meta.etag);
  assert_string_equal(loaded_meta.meta.owner, "second-owner");

  lc_pouch_meta_record_cleanup(&allocator, &loaded_meta);
  lc_pouch_store_meta_res_cleanup(&allocator, &first_meta);
  lc_pouch_store_meta_res_cleanup(&allocator, &second_meta);
  lc_pouch_put_state_res_cleanup(&allocator, &first_put);
  lc_pouch_put_state_res_cleanup(&allocator, &second_put);
  rc = second->close(second, &error);
  assert_int_equal(rc, LC_OK);
  rc = first->close(first, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_independent_handles_refresh_index_projection(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *first;
  lc_pouch_store *second;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res first_meta;
  lc_pouch_store_meta_res second_meta;
  lc_pouch_query_index_scan_req scan_req;
  lc_pouch_query_index_scan_res scan_res;
  scan_capture row_capture;
  key_capture key_rows;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "shared-index-refresh");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&meta, 0, sizeof(meta));
  memset(&first_meta, 0, sizeof(first_meta));
  memset(&second_meta, 0, sizeof(second_meta));
  memset(&scan_req, 0, sizeof(scan_req));
  memset(&scan_res, 0, sizeof(scan_res));
  memset(&row_capture, 0, sizeof(row_capture));
  memset(&key_rows, 0, sizeof(key_rows));
  first = NULL;
  second = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &first, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_disk_open(root, &allocator, &second, &error);
  assert_int_equal(rc, LC_OK);

  meta.owner = "writer-one";
  meta.lease_id = "lease-alpha";
  meta.state_etag = "state-alpha";
  meta.version = 10L;
  rc = first->store_meta(first, "default", "alpha", &meta, NULL, &first_meta,
                         &error);
  assert_int_equal(rc, LC_OK);

  scan_req.namespace_name = "default";
  rc = second->query_index_scan(second, &scan_req, capture_scan_row,
                                &row_capture, &scan_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(row_capture.count, 1U);
  assert_string_equal(row_capture.keys[0], "alpha");
  assert_int_equal(row_capture.versions[0], 10L);
  assert_true(scan_res.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan_res);

  meta.owner = "writer-two";
  meta.lease_id = "lease-bravo";
  meta.state_etag = "state-bravo";
  meta.version = 20L;
  rc = second->store_meta(second, "default", "bravo", &meta, NULL,
                          &second_meta, &error);
  assert_int_equal(rc, LC_OK);

  rc = first->query_index_keys_scan(first, &scan_req, capture_query_key,
                                    &key_rows, &scan_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(key_rows.count, 2U);
  assert_string_equal(key_rows.keys[0], "alpha");
  assert_string_equal(key_rows.keys[1], "bravo");
  assert_true(scan_res.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan_res);

  lc_pouch_store_meta_res_cleanup(&allocator, &first_meta);
  lc_pouch_store_meta_res_cleanup(&allocator, &second_meta);
  rc = second->close(second, &error);
  assert_int_equal(rc, LC_OK);
  rc = first->close(first, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_independent_handle_reopens_compacted_query_index(
    void **state) {
  char root[256];
  char owner[2048];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *first;
  lc_pouch_store *second;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_store_meta_res updated;
  lc_pouch_query_index_scan_req scan_req;
  lc_pouch_query_index_scan_res scan_res;
  key_capture key_rows;
  lc_error error;
  size_t index;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "shared-query-index-reopen");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  memset(&updated, 0, sizeof(updated));
  memset(&scan_req, 0, sizeof(scan_req));
  memset(&scan_res, 0, sizeof(scan_res));
  memset(&key_rows, 0, sizeof(key_rows));
  memset(owner, 'o', sizeof(owner));
  owner[sizeof(owner) - 1U] = '\0';
  first = NULL;
  second = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &first, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_disk_open(root, &allocator, &second, &error);
  assert_int_equal(rc, LC_OK);

  meta.owner = "initial-owner";
  meta.lease_id = "lease-initial";
  meta.state_etag = "state-initial";
  meta.version = 1L;
  rc = second->store_meta(second, "default", "shared-key", &meta, NULL,
                          &stored, &error);
  assert_int_equal(rc, LC_OK);

  scan_req.namespace_name = "default";
  rc = first->query_index_keys_scan(first, &scan_req, capture_query_key,
                                    &key_rows, &scan_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(key_rows.count, 1U);
  assert_string_equal(key_rows.keys[0], "shared-key");
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan_res);

  meta.owner = owner;
  meta.lease_id = "lease-compact";
  meta.state_etag = "state-compact";
  meta.has_query_hidden = 0;
  meta.query_hidden = 0;
  for (index = 0U; index < 160U; ++index) {
    meta.version = (long)index + 2L;
    rc = second->store_meta(second, "default", "shared-key", &meta,
                            stored.etag, &updated, &error);
    assert_int_equal(rc, LC_OK);
    lc_pouch_store_meta_res_cleanup(&allocator, &stored);
    stored = updated;
    memset(&updated, 0, sizeof(updated));
  }

  meta.owner = "hidden-owner";
  meta.lease_id = "lease-hidden";
  meta.state_etag = "state-hidden";
  meta.version = 1000L;
  meta.has_query_hidden = 1;
  meta.query_hidden = 1;
  rc = second->store_meta(second, "default", "shared-key", &meta, stored.etag,
                          &updated, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);
  stored = updated;
  memset(&updated, 0, sizeof(updated));

  memset(&key_rows, 0, sizeof(key_rows));
  rc = first->query_index_keys_scan(first, &scan_req, capture_query_key,
                                    &key_rows, &scan_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(key_rows.count, 0U);
  assert_true(scan_res.index_seq >= 1000UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan_res);

  lc_pouch_store_meta_res_cleanup(&allocator, &stored);
  rc = second->close(second, &error);
  assert_int_equal(rc, LC_OK);
  rc = first->close(first, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void child_process_cas_update(const char *root, const char *expected_etag,
                                     int start_fd, const char *payload) {
  lc_pouch_store *store;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res put_res;
  lc_source *source;
  lc_error error;
  char ready;
  int rc;

  store = NULL;
  source = NULL;
  memset(&opts, 0, sizeof(opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&error, 0, sizeof(error));
  if (read(start_fd, &ready, 1U) != 1) {
    _exit(20);
  }
  close(start_fd);
  rc = lc_pouch_disk_open(root, NULL, &store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    _exit(21);
  }
  opts.content_type = "application/json";
  opts.if_state_etag = expected_etag;
  rc = lc_source_from_memory(payload, strlen(payload), &source, &error);
  if (rc != LC_OK) {
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(22);
  }
  rc = store->write_state(store, "default", "process-key", source, &opts,
                          &put_res, &error);
  lc_source_close(source);
  lc_pouch_put_state_res_cleanup(NULL, &put_res);
  store->close(store, NULL);
  if (rc == LC_OK) {
    lc_error_cleanup(&error);
    _exit(0);
  }
  if (rc == LC_ERR_SERVER && error.http_status == 412L) {
    lc_error_cleanup(&error);
    _exit(2);
  }
  lc_error_cleanup(&error);
  _exit(23);
}

static int child_exit_code(pid_t pid) {
  int status;
  int rc;

  status = 0;
  rc = waitpid(pid, &status, 0);
  assert_int_equal(rc, pid);
  assert_true(WIFEXITED(status));
  return WEXITSTATUS(status);
}

static void child_process_backend_hash(const char *root, int start_fd) {
  lc_pouch_store *store;
  lc_error error;
  char *hash;
  char ready;
  int rc;

  store = NULL;
  hash = NULL;
  memset(&error, 0, sizeof(error));
  rc = lc_pouch_disk_open(root, NULL, &store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    _exit(40);
  }
  if (read(start_fd, &ready, 1U) != 1) {
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(41);
  }
  close(start_fd);
  rc = store->backend_hash(store, &hash, &error);
  if (rc != LC_OK) {
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(42);
  }
  if (hash == NULL || strncmp(hash, "pouch-", 6U) != 0) {
    lc_pouch_free(NULL, hash);
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(43);
  }
  lc_pouch_free(NULL, hash);
  store->close(store, NULL);
  lc_error_cleanup(&error);
  _exit(0);
}

static void child_process_dequeue_one(const char *root, int start_fd) {
  lc_pouch_store *store;
  lc_pouch_dequeue_opts opts;
  lc_pouch_queue_message_info message;
  lc_source *body;
  lc_error error;
  char ready;
  int rc;

  store = NULL;
  body = NULL;
  memset(&opts, 0, sizeof(opts));
  memset(&message, 0, sizeof(message));
  memset(&error, 0, sizeof(error));
  if (read(start_fd, &ready, 1U) != 1) {
    _exit(30);
  }
  close(start_fd);
  rc = lc_pouch_disk_open(root, NULL, &store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    _exit(31);
  }
  opts.owner = "worker";
  opts.visibility_timeout_seconds = 60L;
  rc = store->dequeue_message(store, "default", "jobs", &opts, &body,
                              &message, &error);
  if (rc != LC_OK) {
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(32);
  }
  if (body == NULL) {
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(2);
  }
  lc_source_close(body);
  lc_pouch_queue_message_info_cleanup(NULL, &message);
  store->close(store, NULL);
  lc_error_cleanup(&error);
  _exit(0);
}

static void test_independent_processes_contend_with_cas(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *body;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res initial_put;
  lc_pouch_state_info state_info;
  lc_error error;
  char *text;
  int start_pipe[2];
  int first_code;
  int second_code;
  int rc;
  pid_t first_pid;
  pid_t second_pid;

  (void)state;
  test_root_path(root, sizeof(root), "process-cas");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  memset(&initial_put, 0, sizeof(initial_put));
  memset(&state_info, 0, sizeof(state_info));
  store = NULL;
  body = NULL;
  start_pipe[0] = -1;
  start_pipe[1] = -1;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  opts.content_type = "application/json";
  source = source_from_text("{\"owner\":\"initial\"}");
  rc = store->write_state(store, "default", "process-key", source, &opts,
                          &initial_put, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  rc = pipe(start_pipe);
  assert_int_equal(rc, 0);
  first_pid = fork();
  assert_true(first_pid >= 0);
  if (first_pid == 0) {
    close(start_pipe[1]);
    child_process_cas_update(root, initial_put.new_state_etag, start_pipe[0],
                             "{\"owner\":\"first-process\"}");
  }
  second_pid = fork();
  assert_true(second_pid >= 0);
  if (second_pid == 0) {
    close(start_pipe[1]);
    child_process_cas_update(root, initial_put.new_state_etag, start_pipe[0],
                             "{\"owner\":\"second-process\"}");
  }
  close(start_pipe[0]);
  start_pipe[0] = -1;
  assert_int_equal(write(start_pipe[1], "xx", 2U), 2);
  close(start_pipe[1]);
  start_pipe[1] = -1;

  first_code = child_exit_code(first_pid);
  second_code = child_exit_code(second_pid);
  assert_true((first_code == 0 && second_code == 2) ||
              (first_code == 2 && second_code == 0));

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->read_state(store, "default", "process-key", &body, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_false(state_info.no_content);
  assert_int_equal(state_info.version, initial_put.new_version + 1L);
  text = read_source_text(body);
  assert_true(strcmp(text, "{\"owner\":\"first-process\"}") == 0 ||
              strcmp(text, "{\"owner\":\"second-process\"}") == 0);
  free(text);
  lc_source_close(body);
  body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &state_info);
  lc_pouch_put_state_res_cleanup(&allocator, &initial_put);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_independent_processes_dequeue_single_message_once(
    void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_enqueue_opts enqueue_opts;
  lc_pouch_queue_message_info enqueued;
  lc_pouch_queue_stats stats;
  lc_error error;
  int start_pipe[2];
  int first_code;
  int second_code;
  int rc;
  pid_t first_pid;
  pid_t second_pid;

  (void)state;
  test_root_path(root, sizeof(root), "process-dequeue");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&enqueued, 0, sizeof(enqueued));
  memset(&stats, 0, sizeof(stats));
  store = NULL;
  start_pipe[0] = -1;
  start_pipe[1] = -1;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  enqueue_opts.content_type = "text/plain";
  enqueue_opts.visibility_timeout_seconds = 60L;
  enqueue_opts.ttl_seconds = 3600L;
  enqueue_opts.max_attempts = 3;
  source = source_from_text("queue-process-body");
  rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                              &enqueued, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  rc = pipe(start_pipe);
  assert_int_equal(rc, 0);
  first_pid = fork();
  assert_true(first_pid >= 0);
  if (first_pid == 0) {
    close(start_pipe[1]);
    child_process_dequeue_one(root, start_pipe[0]);
  }
  second_pid = fork();
  assert_true(second_pid >= 0);
  if (second_pid == 0) {
    close(start_pipe[1]);
    child_process_dequeue_one(root, start_pipe[0]);
  }
  close(start_pipe[0]);
  start_pipe[0] = -1;
  assert_int_equal(write(start_pipe[1], "xx", 2U), 2);
  close(start_pipe[1]);
  start_pipe[1] = -1;

  first_code = child_exit_code(first_pid);
  second_code = child_exit_code(second_pid);
  assert_true((first_code == 0 && second_code == 2) ||
              (first_code == 2 && second_code == 0));

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->queue_stats(store, "default", "jobs", &stats, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats.available, 0);
  assert_int_equal(stats.pending_candidates, 1);
  lc_pouch_queue_stats_cleanup(&allocator, &stats);
  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_query_config_defaults_and_configured_options(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_disk_open_opts opts;
  lc_pouch_query_config config;
  lc_pouch_store *store;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-config");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&config, 0, sizeof(config));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(store->query_config);
  rc = store->query_config(store, "default", &config, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(config.preferred_engine, "index");
  assert_string_equal(config.fallback_engine, "none");
  lc_pouch_query_config_cleanup(&allocator, &config);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  memset(&opts, 0, sizeof(opts));
  opts.query_engine = "scan";
  opts.query_fallback_engine = "index";
  rc = lc_pouch_disk_open_with_options(root, &allocator, &opts, &store,
                                       &error);
  assert_int_equal(rc, LC_OK);
  rc = store->query_config(store, "default", &config, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(config.preferred_engine, "scan");
  assert_string_equal(config.fallback_engine, "index");
  lc_pouch_query_config_cleanup(&allocator, &config);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_query_config_rejects_invalid_options(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_disk_open_opts opts;
  lc_pouch_store *store;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-config-invalid");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  store = NULL;

  opts.query_engine = "linear";
  rc = lc_pouch_disk_open_with_options(root, &allocator, &opts, &store,
                                       &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(store);
  assert_string_equal(error.message,
                      "pouch disk query_engine must be index or scan");
  lc_error_cleanup(&error);

  memset(&error, 0, sizeof(error));
  opts.query_engine = "index";
  opts.query_fallback_engine = "linear";
  rc = lc_pouch_disk_open_with_options(root, &allocator, &opts, &store,
                                       &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(store);
  assert_string_equal(
      error.message,
      "pouch disk query_fallback_engine must be none, index, or scan");
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_open_removes_stale_compaction_temps(void **state) {
  char root[256];
  char temp_log[512];
  char temp_query[512];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_put_state_res put_res;
  lc_source *source;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "open-stale-compaction");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&put_res, 0, sizeof(put_res));
  store = NULL;
  source = NULL;

  assert_int_equal(mkdir(root, 0777), 0);
  snprintf(temp_log, sizeof(temp_log), "%s/store.compact.tmp", root);
  snprintf(temp_query, sizeof(temp_query), "%s/query.index.compact.tmp", root);
  test_write_marker_file(temp_log);
  test_write_marker_file(temp_query);

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(store);
  assert_int_equal(access(temp_log, F_OK), -1);
  assert_int_equal(errno, ENOENT);
  errno = 0;
  assert_int_equal(access(temp_query, F_OK), -1);
  assert_int_equal(errno, ENOENT);

  source = source_from_text("after stale cleanup");
  rc = store->write_state(store, "default", "key", source, NULL, &put_res,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_close_removes_writer_marker(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "close-writer-marker");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(test_count_writer_markers(root), 1U);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(test_count_writer_markers(root), 0U);

  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_abort_preserves_writer_marker(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "abort-writer-marker");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(test_count_writer_markers(root), 1U);

  rc = store->abort(store, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(test_count_writer_markers(root), 1U);

  test_remove_writer_markers(root);
  assert_int_equal(test_count_writer_markers(root), 0U);

  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_writer_status_reports_marker_presence(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *first;
  lc_pouch_store *second;
  lc_pouch_writer_status status;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "writer-status");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&status, 0, sizeof(status));
  first = NULL;
  second = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &first, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_disk_open(root, &allocator, &second, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(first->writer_status);

  rc = first->writer_status(first, &status, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(status.mode, "advisory-file-lock-marker");
  assert_string_equal(status.marker_prefix, TEST_POUCH_WRITER_MARKER_PREFIX);
  assert_true(status.own_marker_present);
  assert_int_equal(status.active_marker_count, 2U);
  assert_int_equal(status.other_marker_count, 1U);
  assert_int_equal(status.heartbeat_sequence, 1UL);
  lc_pouch_writer_status_cleanup(&allocator, &status);

  rc = second->close(second, &error);
  assert_int_equal(rc, LC_OK);
  second = NULL;
  rc = first->writer_status(first, &status, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(status.own_marker_present);
  assert_int_equal(status.active_marker_count, 1U);
  assert_int_equal(status.other_marker_count, 0U);
  lc_pouch_writer_status_cleanup(&allocator, &status);

  tracked.fail_malloc_size = strlen("advisory-file-lock-marker") + 1U;
  rc = first->writer_status(first, &status, &error);
  assert_int_equal(rc, LC_ERR_NOMEM);
  assert_string_equal(error.message, "failed to copy pouch writer status");
  assert_null(status.mode);
  assert_null(status.marker_prefix);
  assert_false(status.own_marker_present);
  assert_int_equal(status.active_marker_count, 0U);
  tracked.fail_malloc_size = 0U;
  lc_error_cleanup(&error);

  rc = first->close(first, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_writer_marker_heartbeat_updates_after_commit(void **state) {
  char root[256];
  char marker_path[512];
  char before[256];
  char after_state[256];
  char after_meta[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts state_opts;
  lc_pouch_put_state_res state_res;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res meta_res;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "writer-marker-heartbeat");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&state_opts, 0, sizeof(state_opts));
  memset(&state_res, 0, sizeof(state_res));
  memset(&meta, 0, sizeof(meta));
  memset(&meta_res, 0, sizeof(meta_res));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(test_first_writer_marker_path(root, marker_path,
                                            sizeof(marker_path)));
  test_read_file_text(marker_path, before, sizeof(before));
  assert_non_null(strstr(before, "sequence=1\n"));

  state_opts.content_type = "text/plain";
  source = source_from_text("marker heartbeat");
  rc = store->write_state(store, "default", "heartbeat-key", source,
                          &state_opts, &state_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  test_read_file_text(marker_path, after_state, sizeof(after_state));
  assert_non_null(strstr(after_state, "sequence=2\n"));
  assert_string_not_equal(before, after_state);

  meta.owner = "owner";
  meta.lease_id = "lease";
  meta.state_etag = state_res.new_state_etag;
  meta.version = state_res.new_version;
  rc = store->store_meta(store, "default", "heartbeat-key", &meta, NULL,
                         &meta_res, &error);
  assert_int_equal(rc, LC_OK);
  test_read_file_text(marker_path, after_meta, sizeof(after_meta));
  assert_non_null(strstr(after_meta, "sequence=3\n"));
  assert_string_not_equal(after_state, after_meta);

  lc_pouch_store_meta_res_cleanup(&allocator, &meta_res);
  lc_pouch_put_state_res_cleanup(&allocator, &state_res);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_writer_marker_touch_failure_does_not_rollback_commit(
    void **state) {
  char root[256];
  char marker_path[512];
  char body_text[64];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *body;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res put_res;
  lc_pouch_state_info state_info;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "writer-marker-touch-failure");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&state_info, 0, sizeof(state_info));
  store = NULL;
  body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(test_first_writer_marker_path(root, marker_path,
                                            sizeof(marker_path)));
  assert_int_equal(unlink(marker_path), 0);
  assert_int_equal(mkdir(marker_path, 0777), 0);

  opts.content_type = "text/plain";
  source = source_from_text("durable despite marker failure");
  rc = store->write_state(store, "default", "durable-key", source, &opts,
                          &put_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  assert_int_equal(rmdir(marker_path), 0);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->read_state(store, "default", "durable-key", &body, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_false(state_info.no_content);
  assert_non_null(body);
  memset(body_text, 0, sizeof(body_text));
  assert_int_equal(body->read(body, body_text, sizeof(body_text) - 1U, &error),
                   strlen("durable despite marker failure"));
  assert_string_equal(body_text, "durable despite marker failure");
  body->close(body);
  body = NULL;

  lc_pouch_state_info_cleanup(&allocator, &state_info);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_write_read_reopen_and_allocator_hooks),
      cmocka_unit_test(test_state_read_skips_replay_after_same_handle_write),
      cmocka_unit_test(test_cas_and_remove_semantics),
      cmocka_unit_test(
          test_state_write_index_allocation_failure_replays_cleanly),
      cmocka_unit_test(test_staged_state_promote_discard_and_reopen),
      cmocka_unit_test(test_staged_state_listing_orders_paginates_and_replays),
      cmocka_unit_test(test_staged_state_rejects_pathlike_transaction_ids),
      cmocka_unit_test(test_root_staged_state_lists_and_discards),
      cmocka_unit_test(test_replay_truncates_trailing_partial_record),
      cmocka_unit_test(test_replay_rebuilds_indexes_after_external_truncation),
      cmocka_unit_test(
          test_replay_stops_at_corrupt_record_and_discards_later_records),
      cmocka_unit_test(test_replay_stops_at_unsupported_record_version),
      cmocka_unit_test(
          test_replay_stops_at_oversized_record_without_allocating_payload),
      cmocka_unit_test(test_replay_stops_at_truncated_object_metadata),
      cmocka_unit_test(test_replay_stops_at_truncated_queue_metadata),
      cmocka_unit_test(test_replay_stops_at_corrupt_object_record),
      cmocka_unit_test(test_replay_stops_at_corrupt_queue_record),
      cmocka_unit_test(test_metadata_roundtrip_cas_delete_and_reopen),
      cmocka_unit_test(test_metadata_scan_orders_paginates_and_replays),
      cmocka_unit_test(test_metadata_scan_forces_full_log_replay),
      cmocka_unit_test(
          test_query_index_scan_orders_paginates_and_reports_seq),
      cmocka_unit_test(
          test_query_index_keys_scan_avoids_metadata_row_copies),
      cmocka_unit_test(
          test_query_index_projection_replays_updates_and_deletes),
      cmocka_unit_test(
          test_metadata_update_allocation_failure_preserves_indexes),
      cmocka_unit_test(test_index_flush_reports_current_projection),
      cmocka_unit_test(test_index_flush_recovers_from_corrupt_sidecar_tail),
      cmocka_unit_test(test_query_index_sidecar_appends_metadata_records),
      cmocka_unit_test(
          test_query_index_keys_ignores_sidecar_without_metadata_log),
      cmocka_unit_test(
          test_query_index_keys_recovers_from_corrupt_sidecar_tail),
      cmocka_unit_test(
          test_query_index_keys_truncates_partial_sidecar_field),
      cmocka_unit_test(test_scan_meta_ignores_corrupt_query_sidecar),
      cmocka_unit_test(test_query_index_sidecar_compacts_with_store_log),
      cmocka_unit_test(test_object_roundtrip_overwrite_delete_and_reopen),
      cmocka_unit_test(
          test_object_overwrite_allocation_failure_replays_cleanly),
      cmocka_unit_test(test_object_listing_orders_by_name_after_replay),
      cmocka_unit_test(test_object_max_bytes_reads_only_limit_plus_one),
      cmocka_unit_test(test_object_put_streams_payload_without_large_alloc),
      cmocka_unit_test(
          test_object_copy_streams_existing_payload_without_large_alloc),
      cmocka_unit_test(
          test_object_copy_source_open_failure_leaves_destination_unchanged),
      cmocka_unit_test(
          test_queue_dequeue_skips_replay_after_same_handle_enqueue),
      cmocka_unit_test(test_queue_rejects_negative_timing_options),
      cmocka_unit_test(test_replay_streams_large_bodies_without_large_alloc),
      cmocka_unit_test(
          test_auto_compaction_preserves_live_heads_and_tokens),
      cmocka_unit_test(
          test_manual_compaction_reports_stats_and_preserves_state),
      cmocka_unit_test(
          test_compaction_if_needed_skip_and_allocator_failure),
      cmocka_unit_test(
          test_compaction_preserves_promoted_staged_state_link),
      cmocka_unit_test(
          test_independent_handle_refreshes_after_log_replacement),
      cmocka_unit_test(test_queue_dequeue_survives_compaction_refresh),
      cmocka_unit_test(test_queue_mutations_touch_wake_marker),
      cmocka_unit_test(
          test_queue_wake_marker_failure_does_not_rollback_enqueue),
      cmocka_unit_test(test_queue_wake_status_reports_polling_marker_mode),
      cmocka_unit_test(test_empty_identifiers_are_rejected_before_append),
      cmocka_unit_test(
          test_pathlike_identifiers_are_rejected_before_append),
      cmocka_unit_test(test_queue_enqueue_dequeue_nack_ack_and_reopen),
      cmocka_unit_test(
          test_queue_enqueue_index_allocation_failure_replays_cleanly),
      cmocka_unit_test(test_queue_ref_requires_current_meta_etag),
      cmocka_unit_test(test_queue_delay_hides_until_visible),
      cmocka_unit_test(
          test_queue_ttl_expiry_removes_pending_candidate_after_replay),
      cmocka_unit_test(
          test_queue_nack_allocation_failure_preserves_active_lease),
      cmocka_unit_test(
          test_queue_extend_allocation_failure_preserves_active_lease),
      cmocka_unit_test(
          test_queue_retry_exhaustion_is_not_pending_after_replay),
      cmocka_unit_test(test_independent_handles_refresh_before_operations),
      cmocka_unit_test(test_independent_handles_refresh_index_projection),
      cmocka_unit_test(
          test_independent_handle_reopens_compacted_query_index),
      cmocka_unit_test(test_independent_processes_contend_with_cas),
      cmocka_unit_test(test_independent_processes_dequeue_single_message_once),
      cmocka_unit_test(test_query_config_defaults_and_configured_options),
      cmocka_unit_test(test_query_config_rejects_invalid_options),
      cmocka_unit_test(test_open_removes_stale_compaction_temps),
      cmocka_unit_test(test_close_removes_writer_marker),
      cmocka_unit_test(test_abort_preserves_writer_marker),
      cmocka_unit_test(test_writer_status_reports_marker_presence),
      cmocka_unit_test(test_writer_marker_heartbeat_updates_after_commit),
      cmocka_unit_test(
          test_writer_marker_touch_failure_does_not_rollback_commit),
      cmocka_unit_test(test_list_namespaces_reports_live_projection_names),
      cmocka_unit_test(test_backend_capabilities_report_disk_writer_model),
      cmocka_unit_test(test_backend_hash_persists_across_handles),
      cmocka_unit_test(
          test_backend_hash_create_race_publishes_single_identity),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}

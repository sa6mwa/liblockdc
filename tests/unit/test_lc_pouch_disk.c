#include <errno.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cmocka.h>
#include <dirent.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <utime.h>

#include "lc_pouch_store.h"

#define TEST_POUCH_HEADER_SIZE 64U
#define TEST_POUCH_HEADER_BODY_LENGTH_OFFSET 28U
#define TEST_POUCH_HEADER_PAYLOAD_LENGTH_OFFSET 44U
#define TEST_POUCH_HEADER_RECORD_VERSION_OFFSET 56U
#define TEST_POUCH_QUERY_INDEX_HEADER_SIZE 64U
#define TEST_POUCH_QUERY_INDEX_FORMAT_VERSION_OFFSET 12U
#define TEST_POUCH_QUERY_INDEX_RECORD_VERSION_OFFSET 56U
#define TEST_POUCH_QUERY_INDEX_RECORD_META 1U
#define TEST_POUCH_QUERY_INDEX_RECORD_FIELD_VALUE 3U
#define TEST_POUCH_QUERY_INDEX_RECORD_FORMAT 4U
#define TEST_POUCH_RECORD_STATE_PUT 1U
#define TEST_POUCH_RECORD_STATE_REMOVE 2U
#define TEST_POUCH_RECORD_META_PUT 3U
#define TEST_POUCH_RECORD_META_REMOVE 4U
#define TEST_POUCH_RECORD_OBJECT_PUT 5U
#define TEST_POUCH_RECORD_QUEUE_PUT 7U
#define TEST_POUCH_RECORD_STATE_LINK 10U
#define TEST_POUCH_RECORD_HIGH_WATER 11U
#define TEST_POUCH_MAX_INLINE_BODY_BYTES (64UL * 1024UL * 1024UL)
#define TEST_POUCH_WRITER_MARKER_PREFIX "writer-presence-"
#define TEST_POUCH_LOGSTORE_WRITER_MARKER_PREFIX "writer-"
#define TEST_POUCH_QUEUE_WAKE_PREFIX "queue-wake-"

typedef struct test_same_process_key_lock_thread {
  const char *root;
  int start_fd;
  int ready_fd;
  int done_fd;
  int result;
} test_same_process_key_lock_thread;

typedef struct tracked_allocator {
  size_t malloc_calls;
  size_t realloc_calls;
  size_t free_calls;
  size_t max_malloc_size;
  size_t max_realloc_size;
  size_t fail_malloc_size;
  size_t fail_malloc_after_calls;
  size_t fail_realloc_size;
} tracked_allocator;

typedef struct counting_source {
  lc_source pub;
  size_t length;
  size_t position;
} counting_source;

typedef struct failing_callback_source {
  const char *prefix;
  size_t offset;
  unsigned int reads_before_failure;
} failing_callback_source;

static void *tracked_malloc(void *context, size_t size) {
  tracked_allocator *tracked;

  tracked = (tracked_allocator *)context;
  tracked->malloc_calls++;
  if (size > tracked->max_malloc_size) {
    tracked->max_malloc_size = size;
  }
  if (tracked->fail_malloc_size != 0U && size == tracked->fail_malloc_size &&
      (tracked->fail_malloc_after_calls == 0U ||
       tracked->malloc_calls > tracked->fail_malloc_after_calls)) {
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
  if (tracked->fail_realloc_size != 0U && size == tracked->fail_realloc_size) {
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

static int test_is_logstore_writer_marker(const char *name) {
  size_t prefix_len;
  size_t name_len;
  size_t marker_len;

  prefix_len = strlen(TEST_POUCH_LOGSTORE_WRITER_MARKER_PREFIX);
  marker_len = strlen(".marker");
  name_len = strlen(name);
  return name_len > prefix_len + marker_len &&
         strncmp(name, TEST_POUCH_LOGSTORE_WRITER_MARKER_PREFIX, prefix_len) ==
             0 &&
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

static size_t test_count_logstore_writer_markers(const char *root,
                                                 const char *namespace_name) {
  char markers_path[512];
  DIR *dir;
  struct dirent *entry;
  size_t count;

  snprintf(markers_path, sizeof(markers_path), "%s/%s/logstore/markers", root,
           namespace_name);
  dir = opendir(markers_path);
  if (dir == NULL) {
    return 0U;
  }
  count = 0U;
  while ((entry = readdir(dir)) != NULL) {
    if (test_is_logstore_writer_marker(entry->d_name)) {
      count++;
    }
  }
  closedir(dir);
  return count;
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

static int test_first_logstore_writer_marker_path(const char *root,
                                                  const char *namespace_name,
                                                  char *path,
                                                  size_t path_size) {
  char markers_path[512];
  DIR *dir;
  struct dirent *entry;
  int found;

  snprintf(markers_path, sizeof(markers_path), "%s/%s/logstore/markers", root,
           namespace_name);
  dir = opendir(markers_path);
  if (dir == NULL) {
    return 0;
  }
  found = 0;
  while ((entry = readdir(dir)) != NULL) {
    if (test_is_logstore_writer_marker(entry->d_name)) {
      size_t dir_len;
      size_t name_len;

      dir_len = strlen(markers_path);
      name_len = strlen(entry->d_name);
      assert_true(dir_len + 1U + name_len + 1U <= path_size);
      memcpy(path, markers_path, dir_len);
      path[dir_len] = '/';
      memcpy(path + dir_len + 1U, entry->d_name, name_len + 1U);
      found = 1;
      break;
    }
  }
  closedir(dir);
  return found;
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

static int test_join_path_buf(char *dst, size_t dst_size, const char *root,
                              const char *leaf) {
  size_t root_len;
  size_t leaf_len;

  root_len = strlen(root);
  leaf_len = strlen(leaf);
  if (root_len + 1U + leaf_len + 1U > dst_size) {
    return 0;
  }
  memcpy(dst, root, root_len);
  dst[root_len] = '/';
  memcpy(dst + root_len + 1U, leaf, leaf_len + 1U);
  return 1;
}

static void test_sleep_for_lock_wait(void) {
  struct timespec delay;

  delay.tv_sec = 0;
  delay.tv_nsec = 10000000L;
  while (nanosleep(&delay, &delay) != 0 && errno == EINTR) {
  }
}

static void test_remove_lock_tree(const char *root) {
  DIR *locks_dir;
  DIR *namespace_dir;
  struct dirent *namespace_entry;
  struct dirent *lock_entry;
  char locks_path[2048];
  char namespace_path[2048];
  char lock_path[2048];

  if (!test_join_path_buf(locks_path, sizeof(locks_path), root, "locks")) {
    return;
  }
  locks_dir = opendir(locks_path);
  if (locks_dir == NULL) {
    return;
  }
  while ((namespace_entry = readdir(locks_dir)) != NULL) {
    if (strcmp(namespace_entry->d_name, ".") == 0 ||
        strcmp(namespace_entry->d_name, "..") == 0) {
      continue;
    }
    if (!test_join_path_buf(namespace_path, sizeof(namespace_path), locks_path,
                            namespace_entry->d_name)) {
      continue;
    }
    namespace_dir = opendir(namespace_path);
    if (namespace_dir != NULL) {
      while ((lock_entry = readdir(namespace_dir)) != NULL) {
        if (strcmp(lock_entry->d_name, ".") == 0 ||
            strcmp(lock_entry->d_name, "..") == 0) {
          continue;
        }
        if (test_join_path_buf(lock_path, sizeof(lock_path), namespace_path,
                               lock_entry->d_name)) {
          unlink(lock_path);
        }
      }
      closedir(namespace_dir);
    }
    rmdir(namespace_path);
  }
  closedir(locks_dir);
  rmdir(locks_path);
}

static void test_cleanup_root(const char *root) {
  DIR *dir;
  struct dirent *entry;
  char path[512];
  struct stat st;

  dir = opendir(root);
  if (dir == NULL) {
    rmdir(root);
    return;
  }
  while ((entry = readdir(dir)) != NULL) {
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    if (!test_join_path_buf(path, sizeof(path), root, entry->d_name)) {
      continue;
    }
    if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
      test_cleanup_root(path);
    } else {
      unlink(path);
    }
  }
  closedir(dir);
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

static void test_write_text_file(const char *path, const char *text) {
  int fd;
  size_t length;

  length = strlen(text);
  fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0666);
  assert_true(fd >= 0);
  assert_int_equal(write(fd, text, length), (ssize_t)length);
  assert_int_equal(close(fd), 0);
}

static void test_set_file_mtime(const char *path, time_t mtime) {
  struct utimbuf times;

  times.actime = mtime;
  times.modtime = mtime;
  assert_int_equal(utime(path, &times), 0);
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

static size_t failing_callback_source_read(void *context, void *buffer,
                                           size_t count, lc_error *error) {
  failing_callback_source *source;
  size_t remaining;
  size_t produced;
  const char message[] = "intentional pouch disk source failure";

  source = (failing_callback_source *)context;
  if (source->reads_before_failure == 0U) {
    if (error != NULL) {
      lc_error_cleanup(error);
      error->code = LC_ERR_PROTOCOL;
      error->message = (char *)malloc(sizeof(message));
      assert_non_null(error->message);
      memcpy(error->message, message, sizeof(message));
    }
    return 0U;
  }
  source->reads_before_failure -= 1U;
  remaining = strlen(source->prefix) - source->offset;
  produced = remaining < count ? remaining : count;
  memcpy(buffer, source->prefix + source->offset, produced);
  source->offset += produced;
  return produced;
}

static lc_source *source_that_fails_after_prefix(const char *prefix) {
  failing_callback_source *context;
  lc_source *source;
  lc_error error;
  int rc;

  context = (failing_callback_source *)malloc(sizeof(*context));
  assert_non_null(context);
  context->prefix = prefix;
  context->offset = 0U;
  context->reads_before_failure = 1U;
  source = NULL;
  memset(&error, 0, sizeof(error));
  rc = lc_source_from_callbacks(failing_callback_source_read, NULL, free,
                                context, &source, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(source);
  lc_error_cleanup(&error);
  return source;
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

static void test_allocator_from_lc_requires_matching_realloc(void **state) {
  tracked_allocator tracked;
  lc_allocator public_allocator;
  lc_pouch_allocator pouch_allocator;
  void *ptr;
  void *grown;

  (void)state;
  memset(&tracked, 0, sizeof(tracked));
  lc_allocator_init(&public_allocator);
  public_allocator.malloc_fn = tracked_malloc;
  public_allocator.free_fn = tracked_free;
  public_allocator.context = &tracked;

  lc_pouch_allocator_from_lc(&public_allocator, &pouch_allocator);
  assert_null(pouch_allocator.malloc_fn);
  assert_null(pouch_allocator.realloc_fn);
  assert_null(pouch_allocator.free_fn);

  memset(&pouch_allocator, 0, sizeof(pouch_allocator));
  pouch_allocator.malloc_fn = tracked_malloc;
  pouch_allocator.free_fn = tracked_free;
  pouch_allocator.context = &tracked;
  ptr = lc_pouch_realloc(&pouch_allocator, NULL, 16U);
  assert_non_null(ptr);
  assert_int_equal(tracked.malloc_calls, 1U);
  grown = lc_pouch_realloc(&pouch_allocator, ptr, 32U);
  assert_null(grown);
  assert_int_equal(tracked.realloc_calls, 0U);
  lc_pouch_free(&pouch_allocator, ptr);
  assert_int_equal(tracked.free_calls, 1U);
}

static unsigned long test_get_u32(const unsigned char *src) {
  return ((unsigned long)src[0]) | (((unsigned long)src[1]) << 8) |
         (((unsigned long)src[2]) << 16) | (((unsigned long)src[3]) << 24);
}

static unsigned long test_get_u64(const unsigned char *src) {
#if ULONG_MAX > 0xffffffffUL
  return test_get_u32(src) | (test_get_u32(src + 4) << 32);
#else
  return test_get_u32(src);
#endif
}

static size_t count_log_records_at_path_of_type(const char *log_path,
                                                unsigned long type) {
  unsigned char header[TEST_POUCH_HEADER_SIZE];
  unsigned long payload_len;
  unsigned long record_type;
  size_t count;
  ssize_t got;
  int fd;

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

static size_t count_log_records_of_type(const char *root, unsigned long type) {
  char log_path[512];

  snprintf(log_path, sizeof(log_path), "%s/store.log", root);
  return count_log_records_at_path_of_type(log_path, type);
}

static size_t count_namespace_segment_records_of_type(
    const char *root, const char *escaped_namespace, unsigned long type) {
  char segments_path[2048];
  char segment_path[2048];
  unsigned char header[TEST_POUCH_HEADER_SIZE];
  unsigned long payload_len;
  unsigned long record_type;
  DIR *dir;
  struct dirent *entry;
  size_t count;
  ssize_t got;
  int fd;

  snprintf(segments_path, sizeof(segments_path), "%s/%s/logstore/segments",
           root, escaped_namespace);
  dir = opendir(segments_path);
  if (dir == NULL) {
    assert_true(errno == ENOENT || errno == ENOTDIR);
    return 0U;
  }
  count = 0U;
  while ((entry = readdir(dir)) != NULL) {
    size_t name_index;
    int valid_segment;

    valid_segment = strncmp(entry->d_name, "seg-", 4U) == 0 &&
                    strlen(entry->d_name) == 24U &&
                    strcmp(entry->d_name + 20U, ".log") == 0;
    for (name_index = 4U; valid_segment && name_index < 20U; ++name_index) {
      valid_segment =
          entry->d_name[name_index] >= '0' && entry->d_name[name_index] <= '9';
    }
    if (!valid_segment) {
      continue;
    }
    assert_true(test_join_path_buf(segment_path, sizeof(segment_path),
                                   segments_path, entry->d_name));
    fd = open(segment_path, O_RDONLY);
    assert_true(fd >= 0);
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
  }
  assert_int_equal(closedir(dir), 0);
  return count;
}

static void test_query_index_path(const char *root, char *path,
                                  size_t path_size);

static size_t count_query_index_records_of_type(const char *root,
                                                unsigned long type) {
  char index_path[512];
  unsigned char header[TEST_POUCH_QUERY_INDEX_HEADER_SIZE];
  unsigned long payload_len;
  unsigned long record_type;
  size_t count;
  ssize_t got;
  int fd;

  test_query_index_path(root, index_path, sizeof(index_path));
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

static size_t count_query_index_field_values_with_prefix(const char *root,
                                                         const char *prefix) {
  char index_path[512];
  unsigned char header[TEST_POUCH_QUERY_INDEX_HEADER_SIZE];
  unsigned long payload_len;
  unsigned long record_type;
  unsigned long ns_len;
  unsigned long key_len;
  unsigned long field_len;
  unsigned long value_len;
  unsigned long state_etag_len;
  size_t prefix_len;
  size_t count;
  ssize_t got;
  int fd;

  test_query_index_path(root, index_path, sizeof(index_path));
  fd = open(index_path, O_RDONLY);
  assert_true(fd >= 0);
  prefix_len = strlen(prefix);
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
    ns_len = test_get_u32(header + 12);
    key_len = test_get_u32(header + 16);
    field_len = test_get_u32(header + 20);
    value_len = test_get_u32(header + 24);
    state_etag_len = test_get_u64(header + 28);
    payload_len = test_get_u64(header + 44);
    if (record_type == TEST_POUCH_QUERY_INDEX_RECORD_FIELD_VALUE &&
        value_len >= prefix_len) {
      char value_prefix[16];

      assert_true(prefix_len <= sizeof(value_prefix));
      assert_true(lseek(fd, (off_t)(ns_len + key_len + field_len), SEEK_CUR) >=
                  0);
      assert_int_equal(read(fd, value_prefix, prefix_len),
                       (ssize_t)prefix_len);
      if (memcmp(value_prefix, prefix, prefix_len) == 0) {
        count++;
      }
      assert_true(lseek(fd,
                        (off_t)(value_len - prefix_len + state_etag_len),
                        SEEK_CUR) >= 0);
    } else {
      assert_true(lseek(fd, (off_t)payload_len, SEEK_CUR) >= 0);
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

static void test_segment_path(const char *root, const char *escaped_namespace,
                              unsigned long segment_number, char *path,
                              size_t path_size) {
  snprintf(path, path_size, "%s/%s/logstore/segments/seg-%016lu.log", root,
           escaped_namespace, segment_number);
}

static void test_snapshot_path(const char *root, const char *escaped_namespace,
                               unsigned long snapshot_number, char *path,
                               size_t path_size) {
  snprintf(path, path_size, "%s/%s/logstore/snapshots/snap-%016lu.log", root,
           escaped_namespace, snapshot_number);
}

static void test_active_segment_path(const char *root,
                                     const char *escaped_namespace, char *path,
                                     size_t path_size) {
  test_segment_path(root, escaped_namespace, 1UL, path, path_size);
}

static off_t test_segment_size(const char *root, const char *escaped_namespace,
                               unsigned long segment_number) {
  char segment_path[512];
  struct stat st;

  test_segment_path(root, escaped_namespace, segment_number, segment_path,
                    sizeof(segment_path));
  assert_int_equal(stat(segment_path, &st), 0);
  return st.st_size;
}

static off_t test_namespace_segments_size(const char *root,
                                          const char *escaped_namespace) {
  char segments_path[2048];
  char segment_path[2048];
  DIR *dir;
  struct dirent *entry;
  struct stat st;
  off_t total;

  snprintf(segments_path, sizeof(segments_path), "%s/%s/logstore/segments",
           root, escaped_namespace);
  dir = opendir(segments_path);
  if (dir == NULL && errno == ENOENT) {
    return 0;
  }
  assert_non_null(dir);
  total = 0;
  while ((entry = readdir(dir)) != NULL) {
    if (strncmp(entry->d_name, "seg-", 4U) != 0 ||
        strstr(entry->d_name, ".compact.bak") != NULL) {
      continue;
    }
    assert_true(test_join_path_buf(segment_path, sizeof(segment_path),
                                   segments_path, entry->d_name));
    assert_int_equal(stat(segment_path, &st), 0);
    if (S_ISREG(st.st_mode)) {
      total += st.st_size;
    }
  }
  assert_int_equal(closedir(dir), 0);
  return total;
}

static off_t test_namespace_snapshots_size(const char *root,
                                           const char *escaped_namespace) {
  char snapshots_path[2048];
  char snapshot_path[2048];
  DIR *dir;
  struct dirent *entry;
  struct stat st;
  off_t total;

  snprintf(snapshots_path, sizeof(snapshots_path), "%s/%s/logstore/snapshots",
           root, escaped_namespace);
  dir = opendir(snapshots_path);
  if (dir == NULL && errno == ENOENT) {
    return 0;
  }
  assert_non_null(dir);
  total = 0;
  while ((entry = readdir(dir)) != NULL) {
    if (strncmp(entry->d_name, "snap-", 5U) != 0 ||
        strstr(entry->d_name, ".compact.bak") != NULL) {
      continue;
    }
    assert_true(test_join_path_buf(snapshot_path, sizeof(snapshot_path),
                                   snapshots_path, entry->d_name));
    assert_int_equal(stat(snapshot_path, &st), 0);
    if (S_ISREG(st.st_mode)) {
      total += st.st_size;
    }
  }
  assert_int_equal(closedir(dir), 0);
  return total;
}

static off_t test_active_segment_size(const char *root,
                                      const char *escaped_namespace) {
  return test_segment_size(root, escaped_namespace, 1UL);
}

static void test_manifest_path(const char *root, const char *escaped_namespace,
                               char *path, size_t path_size) {
  snprintf(path, path_size, "%s/%s/logstore/manifest/manifest.log", root,
           escaped_namespace);
}

static void test_query_index_path(const char *root, char *path,
                                  size_t path_size) {
  snprintf(path, path_size, "%s/%%2elockd/logstore/query.index", root);
}

static off_t test_query_index_size(const char *root) {
  char index_path[512];
  struct stat st;

  test_query_index_path(root, index_path, sizeof(index_path));
  assert_int_equal(stat(index_path, &st), 0);
  return st.st_size;
}

static void truncate_query_index_tail(const char *root, off_t remove_bytes) {
  char index_path[512];
  off_t size;
  int fd;

  test_query_index_path(root, index_path, sizeof(index_path));
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
#if ULONG_MAX > 0xffffffffUL
  test_put_u32(dst + 4, (value >> 32) & 0xffffffffUL);
#else
  test_put_u32(dst + 4, 0UL);
#endif
}

static unsigned long
test_crc32_update(unsigned long crc, const unsigned char *bytes, size_t count) {
  size_t index;

  for (index = 0U; index < count; ++index) {
    unsigned long byte;
    int bit;

    byte = bytes != NULL ? bytes[index] : 0UL;
    crc ^= byte;
    for (bit = 0; bit < 8; ++bit) {
      if ((crc & 1UL) != 0UL) {
        crc = (crc >> 1) ^ 0xedb88320UL;
      } else {
        crc >>= 1;
      }
    }
  }
  return crc;
}

static void write_root_store_log_state_put(const char *root, const char *ns,
                                           const char *key,
                                           const char *content_type,
                                           const char *etag, const char *body,
                                           unsigned long version) {
  char log_path[512];
  unsigned char header[TEST_POUCH_HEADER_SIZE];
  unsigned char *payload;
  size_t ns_len;
  size_t key_len;
  size_t ct_len;
  size_t etag_len;
  size_t body_len;
  size_t payload_len;
  unsigned long crc;
  int fd;

  snprintf(log_path, sizeof(log_path), "%s/store.log", root);
  ns_len = strlen(ns);
  key_len = strlen(key);
  ct_len = strlen(content_type);
  etag_len = strlen(etag);
  body_len = strlen(body);
  payload_len = ns_len + key_len + ct_len + etag_len + body_len;
  payload = (unsigned char *)malloc(payload_len);
  assert_non_null(payload);
  memcpy(payload, ns, ns_len);
  memcpy(payload + ns_len, key, key_len);
  memcpy(payload + ns_len + key_len, content_type, ct_len);
  memcpy(payload + ns_len + key_len + ct_len, etag, etag_len);
  memcpy(payload + ns_len + key_len + ct_len + etag_len, body, body_len);

  crc = test_crc32_update(0xffffffffUL, payload, payload_len) ^ 0xffffffffUL;
  memset(header, 0, sizeof(header));
  memcpy(header, "LCP1", 4U);
  test_put_u32(header + 4, TEST_POUCH_HEADER_SIZE);
  test_put_u32(header + 8, TEST_POUCH_RECORD_STATE_PUT);
  test_put_u32(header + 12, (unsigned long)ns_len);
  test_put_u32(header + 16, (unsigned long)key_len);
  test_put_u32(header + 20, (unsigned long)ct_len);
  test_put_u32(header + 24, (unsigned long)etag_len);
  test_put_u64(header + TEST_POUCH_HEADER_BODY_LENGTH_OFFSET,
               (unsigned long)body_len);
  test_put_u64(header + 36, version);
  test_put_u64(header + TEST_POUCH_HEADER_PAYLOAD_LENGTH_OFFSET,
               (unsigned long)payload_len);
  test_put_u32(header + 52, crc);
  test_put_u32(header + TEST_POUCH_HEADER_RECORD_VERSION_OFFSET, 1UL);

  fd = open(log_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  assert_true(fd >= 0);
  assert_int_equal(write(fd, header, sizeof(header)), sizeof(header));
  assert_int_equal(write(fd, payload, payload_len), (ssize_t)payload_len);
  assert_int_equal(fsync(fd), 0);
  assert_int_equal(close(fd), 0);
  free(payload);
}

static void rewrite_query_index_as_v1_without_owner(const char *root) {
  char index_path[512];
  char temp_path[512];
  unsigned char header[TEST_POUCH_QUERY_INDEX_HEADER_SIZE];
  unsigned char *payload;
  unsigned long ns_len;
  unsigned long key_len;
  unsigned long owner_len;
  unsigned long etag_len;
  unsigned long payload_len;
  unsigned long new_payload_len;
  unsigned long crc;
  unsigned long type;
  ssize_t got;
  int in_fd;
  int out_fd;

  test_query_index_path(root, index_path, sizeof(index_path));
  snprintf(temp_path, sizeof(temp_path), "%s/query.index.v1tmp", root);
  in_fd = open(index_path, O_RDONLY);
  assert_true(in_fd >= 0);
  out_fd = open(temp_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  assert_true(out_fd >= 0);

  for (;;) {
    got = read(in_fd, header, sizeof(header));
    if (got == 0) {
      break;
    }
    assert_int_equal(got, sizeof(header));
    assert_memory_equal(header, "LCQI", 4U);
    type = test_get_u32(header + 8);
    ns_len = test_get_u32(header + 12);
    key_len = test_get_u32(header + 16);
    owner_len = test_get_u32(header + 20);
    etag_len = test_get_u32(header + 24);
    payload_len = test_get_u64(header + 44);
    payload = (unsigned char *)malloc((size_t)payload_len);
    assert_non_null(payload);
    assert_int_equal(read(in_fd, payload, (size_t)payload_len), payload_len);
    if (type != TEST_POUCH_QUERY_INDEX_RECORD_META) {
      free(payload);
      continue;
    }
    assert_int_equal(payload_len, ns_len + key_len + owner_len + etag_len);

    new_payload_len = ns_len + key_len + etag_len;
    crc = 0xffffffffUL;
    crc = test_crc32_update(crc, payload, (size_t)(ns_len + key_len));
    if (etag_len > 0UL) {
      crc = test_crc32_update(crc, payload + ns_len + key_len + owner_len,
                              (size_t)etag_len);
    }
    test_put_u32(header + 20, 0UL);
    test_put_u64(header + 44, new_payload_len);
    test_put_u32(header + 52, crc ^ 0xffffffffUL);
    test_put_u32(header + TEST_POUCH_QUERY_INDEX_RECORD_VERSION_OFFSET, 1UL);
    assert_int_equal(write(out_fd, header, sizeof(header)), sizeof(header));
    assert_int_equal(write(out_fd, payload, (size_t)(ns_len + key_len)),
                     ns_len + key_len);
    if (etag_len > 0UL) {
      assert_int_equal(write(out_fd, payload + ns_len + key_len + owner_len,
                             (size_t)etag_len),
                       etag_len);
    }
    free(payload);
  }

  close(in_fd);
  assert_int_equal(fsync(out_fd), 0);
  close(out_fd);
  assert_int_equal(rename(temp_path, index_path), 0);
}

static int corrupt_first_log_match_at_path(const char *log_path,
                                           const char *needle,
                                           int require_match) {
  unsigned char *bytes;
  size_t needle_len;
  size_t index;
  struct stat st;
  int fd;
  int found;

  fd = open(log_path, O_RDWR);
  if (fd < 0) {
    assert_false(require_match);
    return 0;
  }
  assert_int_equal(fstat(fd, &st), 0);
  if (st.st_size == 0) {
    assert_false(require_match);
    close(fd);
    return 0;
  }
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
  if (require_match) {
    assert_true(found);
  }
  return found;
}

static void corrupt_first_log_match(const char *root, const char *needle) {
  char log_path[512];
  char segment_path[512];
  int found;

  snprintf(log_path, sizeof(log_path), "%s/store.log", root);
  found = corrupt_first_log_match_at_path(log_path, needle, 0);
  test_active_segment_path(root, "default", segment_path, sizeof(segment_path));
  found |= corrupt_first_log_match_at_path(segment_path, needle, 0);
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

  test_query_index_path(root, index_path, sizeof(index_path));
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

static void set_first_query_index_match_record_version(const char *root,
                                                       const char *needle,
                                                       unsigned long version) {
  char index_path[512];
  unsigned char header[TEST_POUCH_QUERY_INDEX_HEADER_SIZE];
  unsigned char *payload;
  size_t needle_len;
  unsigned long payload_len;
  off_t record_offset;
  int fd;
  int found;

  test_query_index_path(root, index_path, sizeof(index_path));
  fd = open(index_path, O_RDWR);
  assert_true(fd >= 0);
  assert_int_equal(lseek(fd, 0, SEEK_SET), 0);
  needle_len = strlen(needle);
  found = 0;
  while (!found) {
    record_offset = lseek(fd, 0, SEEK_CUR);
    assert_true(record_offset >= 0);
    assert_int_equal(read(fd, header, sizeof(header)), sizeof(header));
    assert_memory_equal(header, "LCQI", 4U);
    payload_len = test_get_u64(header + 44);
    payload = (unsigned char *)malloc((size_t)payload_len);
    assert_non_null(payload);
    assert_int_equal(read(fd, payload, (size_t)payload_len), payload_len);
    if (payload_len >= needle_len) {
      size_t index;

      for (index = 0U; index + needle_len <= payload_len; ++index) {
        if (memcmp(payload + index, needle, needle_len) == 0) {
          test_put_u32(header + TEST_POUCH_QUERY_INDEX_RECORD_VERSION_OFFSET,
                       version);
          assert_int_equal(
              lseek(fd,
                    record_offset +
                        TEST_POUCH_QUERY_INDEX_RECORD_VERSION_OFFSET,
                    SEEK_SET),
              record_offset + TEST_POUCH_QUERY_INDEX_RECORD_VERSION_OFFSET);
          assert_int_equal(
              write(fd, header + TEST_POUCH_QUERY_INDEX_RECORD_VERSION_OFFSET,
                    4U),
              4);
          found = 1;
          break;
        }
      }
    }
    free(payload);
    if (!found) {
      assert_int_equal(lseek(fd,
                             record_offset +
                                 TEST_POUCH_QUERY_INDEX_HEADER_SIZE +
                                 (off_t)payload_len,
                             SEEK_SET),
                       record_offset + TEST_POUCH_QUERY_INDEX_HEADER_SIZE +
                           (off_t)payload_len);
    }
  }
  close(fd);
}

static void set_query_index_format_version(const char *root,
                                           unsigned long version) {
  char index_path[512];
  unsigned char header[TEST_POUCH_QUERY_INDEX_HEADER_SIZE];
  int fd;

  test_query_index_path(root, index_path, sizeof(index_path));
  fd = open(index_path, O_RDWR);
  assert_true(fd >= 0);
  assert_int_equal(read(fd, header, sizeof(header)), sizeof(header));
  assert_memory_equal(header, "LCQI", 4U);
  assert_int_equal(test_get_u32(header + 8),
                   TEST_POUCH_QUERY_INDEX_RECORD_FORMAT);
  test_put_u32(header + TEST_POUCH_QUERY_INDEX_FORMAT_VERSION_OFFSET, version);
  assert_int_equal(lseek(fd, TEST_POUCH_QUERY_INDEX_FORMAT_VERSION_OFFSET,
                         SEEK_SET),
                   TEST_POUCH_QUERY_INDEX_FORMAT_VERSION_OFFSET);
  assert_int_equal(write(fd,
                         header + TEST_POUCH_QUERY_INDEX_FORMAT_VERSION_OFFSET,
                         4U),
                   4);
  assert_int_equal(fsync(fd), 0);
  assert_int_equal(close(fd), 0);
}

static void strip_query_index_format_record(const char *root) {
  char index_path[512];
  char temp_path[512];
  unsigned char buffer[4096];
  ssize_t got;
  int in_fd;
  int out_fd;

  test_query_index_path(root, index_path, sizeof(index_path));
  snprintf(temp_path, sizeof(temp_path), "%s/query.index.no-format.tmp", root);
  in_fd = open(index_path, O_RDONLY);
  assert_true(in_fd >= 0);
  assert_int_equal(lseek(in_fd, TEST_POUCH_QUERY_INDEX_HEADER_SIZE, SEEK_SET),
                   TEST_POUCH_QUERY_INDEX_HEADER_SIZE);
  out_fd = open(temp_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  assert_true(out_fd >= 0);
  for (;;) {
    got = read(in_fd, buffer, sizeof(buffer));
    assert_true(got >= 0);
    if (got == 0) {
      break;
    }
    assert_int_equal(write(out_fd, buffer, (size_t)got), got);
  }
  assert_int_equal(close(in_fd), 0);
  assert_int_equal(fsync(out_fd), 0);
  assert_int_equal(close(out_fd), 0);
  assert_int_equal(rename(temp_path, index_path), 0);
}

static int set_first_log_match_record_version_at_path(const char *log_path,
                                                      const char *needle,
                                                      unsigned long version,
                                                      int require_match) {
  unsigned char header[TEST_POUCH_HEADER_SIZE];
  unsigned char *payload;
  size_t needle_len;
  unsigned long payload_len;
  off_t record_offset;
  struct stat st;
  ssize_t nread;
  int fd;
  int found;

  fd = open(log_path, O_RDWR);
  if (fd < 0) {
    assert_false(require_match);
    return 0;
  }
  assert_int_equal(fstat(fd, &st), 0);
  if (st.st_size == 0) {
    assert_false(require_match);
    close(fd);
    return 0;
  }
  assert_int_equal(lseek(fd, 0, SEEK_SET), 0);
  needle_len = strlen(needle);
  found = 0;
  while (!found) {
    record_offset = lseek(fd, 0, SEEK_CUR);
    assert_true(record_offset >= 0);
    nread = read(fd, header, sizeof(header));
    if (nread == 0 && !require_match) {
      close(fd);
      return 0;
    }
    assert_int_equal(nread, sizeof(header));
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
              lseek(fd, record_offset + TEST_POUCH_HEADER_RECORD_VERSION_OFFSET,
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
      assert_int_equal(
          lseek(fd, record_offset + TEST_POUCH_HEADER_SIZE + (off_t)payload_len,
                SEEK_SET),
          record_offset + TEST_POUCH_HEADER_SIZE + (off_t)payload_len);
    }
  }
  close(fd);
  if (require_match) {
    assert_true(found);
  }
  return found;
}

static void set_first_log_match_record_version(const char *root,
                                               const char *needle,
                                               unsigned long version) {
  char log_path[512];
  char segment_path[512];
  int found;

  snprintf(log_path, sizeof(log_path), "%s/store.log", root);
  found =
      set_first_log_match_record_version_at_path(log_path, needle, version, 0);
  test_active_segment_path(root, "default", segment_path, sizeof(segment_path));
  found |= set_first_log_match_record_version_at_path(segment_path, needle,
                                                      version, 0);
  assert_true(found);
}

static int set_first_log_match_body_length_at_path(const char *log_path,
                                                   const char *needle,
                                                   unsigned long body_length,
                                                   int require_match) {
  unsigned char header[TEST_POUCH_HEADER_SIZE];
  unsigned char *payload;
  size_t needle_len;
  unsigned long ns_len;
  unsigned long key_len;
  unsigned long ct_len;
  unsigned long etag_len;
  unsigned long payload_len;
  off_t record_offset;
  struct stat st;
  ssize_t nread;
  int fd;
  int found;

  fd = open(log_path, O_RDWR);
  if (fd < 0) {
    assert_false(require_match);
    return 0;
  }
  assert_int_equal(fstat(fd, &st), 0);
  if (st.st_size == 0) {
    assert_false(require_match);
    close(fd);
    return 0;
  }
  assert_int_equal(lseek(fd, 0, SEEK_SET), 0);
  needle_len = strlen(needle);
  found = 0;
  while (!found) {
    record_offset = lseek(fd, 0, SEEK_CUR);
    assert_true(record_offset >= 0);
    nread = read(fd, header, sizeof(header));
    if (nread == 0 && !require_match) {
      close(fd);
      return 0;
    }
    assert_int_equal(nread, sizeof(header));
    assert_memory_equal(header, "LCP1", 4U);
    payload_len =
        test_get_u64(header + TEST_POUCH_HEADER_PAYLOAD_LENGTH_OFFSET);
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
              lseek(fd, record_offset + TEST_POUCH_HEADER_BODY_LENGTH_OFFSET,
                    SEEK_SET),
              record_offset + TEST_POUCH_HEADER_BODY_LENGTH_OFFSET);
          assert_int_equal(
              write(fd, header + TEST_POUCH_HEADER_BODY_LENGTH_OFFSET, 8U), 8);
          assert_int_equal(
              lseek(fd, record_offset + TEST_POUCH_HEADER_PAYLOAD_LENGTH_OFFSET,
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
      assert_int_equal(
          lseek(fd, record_offset + TEST_POUCH_HEADER_SIZE + (off_t)payload_len,
                SEEK_SET),
          record_offset + TEST_POUCH_HEADER_SIZE + (off_t)payload_len);
    }
  }
  close(fd);
  if (require_match) {
    assert_true(found);
  }
  return found;
}

static void set_first_log_match_body_length(const char *root,
                                            const char *needle,
                                            unsigned long body_length) {
  char log_path[512];
  char segment_path[512];
  int found;

  snprintf(log_path, sizeof(log_path), "%s/store.log", root);
  found =
      set_first_log_match_body_length_at_path(log_path, needle, body_length, 0);
  test_active_segment_path(root, "default", segment_path, sizeof(segment_path));
  found |= set_first_log_match_body_length_at_path(segment_path, needle,
                                                   body_length, 0);
  assert_true(found);
}

static int truncate_log_after_first_record_at_path(const char *log_path,
                                                   int require_file) {
  unsigned char header[TEST_POUCH_HEADER_SIZE];
  unsigned long payload_len;
  off_t truncate_at;
  struct stat st;
  int fd;

  fd = open(log_path, O_RDWR);
  if (fd < 0) {
    assert_false(require_file);
    return 0;
  }
  if (fstat(fd, &st) != 0 || st.st_size == 0) {
    assert_false(require_file);
    close(fd);
    return 0;
  }
  assert_int_equal(read(fd, header, sizeof(header)), sizeof(header));
  assert_memory_equal(header, "LCP1", 4U);
  payload_len = test_get_u64(header + 44);
  truncate_at = (off_t)(TEST_POUCH_HEADER_SIZE + payload_len);
  assert_int_equal(ftruncate(fd, truncate_at), 0);
  close(fd);
  return 1;
}

static void truncate_log_after_first_record(const char *root) {
  char log_path[512];
  char segment_path[512];
  int found;

  snprintf(log_path, sizeof(log_path), "%s/store.log", root);
  found = truncate_log_after_first_record_at_path(log_path, 0);
  test_active_segment_path(root, "default", segment_path, sizeof(segment_path));
  found |= truncate_log_after_first_record_at_path(segment_path, 0);
  assert_true(found);
}

typedef struct scan_capture {
  char keys[8][64];
  char owners[8][64];
  long versions[8];
  long updated_at_unix[8];
  int query_hidden[8];
  size_t body_count;
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
  assert_true(capture->count <
              sizeof(capture->keys) / sizeof(capture->keys[0]));
  snprintf(capture->keys[capture->count], sizeof(capture->keys[capture->count]),
           "%s", row->key);
  if (row->meta->owner != NULL) {
    snprintf(capture->owners[capture->count],
             sizeof(capture->owners[capture->count]), "%s", row->meta->owner);
  }
  if (row->body != NULL && row->state != NULL) {
    capture->body_count++;
  }
  capture->versions[capture->count] = row->meta->version;
  capture->updated_at_unix[capture->count] = row->meta->updated_at_unix;
  capture->query_hidden[capture->count] = row->meta->query_hidden;
  capture->count++;
  return LC_OK;
}

static int capture_query_key(void *context, const char *key, size_t key_len,
                             lc_error *error) {
  key_capture *capture;

  (void)error;
  capture = (key_capture *)context;
  assert_non_null(key);
  assert_int_equal(key_len, strlen(key));
  assert_true(capture->count <
              sizeof(capture->keys) / sizeof(capture->keys[0]));
  snprintf(capture->keys[capture->count], sizeof(capture->keys[capture->count]),
           "%s", key);
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
  const char *state_sha256;
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
  state_sha256 =
      "48208f9428d64634bd8e28ff345bf0eab60d53c18fa2fbdb0b9bc1e84df2b5f6";

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
  assert_string_equal(put_res.new_state_etag, state_sha256);
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

static void
test_state_write_creates_segmented_namespace_logstore(void **state) {
  char root[256];
  char path[512];
  char manifest_text[128];
  struct stat st;
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res put_res;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "segmented-namespace-layout");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&opts, 0, sizeof(opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&error, 0, sizeof(error));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  test_query_index_path(root, path, sizeof(path));
  assert_int_equal(stat(path, &st), 0);
  assert_true(S_ISREG(st.st_mode));
  snprintf(path, sizeof(path), "%s/query.index", root);
  assert_int_equal(stat(path, &st), -1);
  assert_int_equal(errno, ENOENT);

  source = source_from_text("{\"value\":1}");
  opts.content_type = "application/json";
  rc = store->write_state(store, "team.alpha", "alpha", source, &opts, &put_res,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  snprintf(path, sizeof(path), "%s/team%%2ealpha/logstore/manifest", root);
  assert_int_equal(stat(path, &st), 0);
  assert_true(S_ISDIR(st.st_mode));
  snprintf(path, sizeof(path), "%s/team%%2ealpha/logstore/segments", root);
  assert_int_equal(stat(path, &st), 0);
  assert_true(S_ISDIR(st.st_mode));
  snprintf(path, sizeof(path), "%s/team%%2ealpha/logstore/snapshots", root);
  assert_int_equal(stat(path, &st), 0);
  assert_true(S_ISDIR(st.st_mode));
  snprintf(path, sizeof(path), "%s/team%%2ealpha/logstore/markers", root);
  assert_int_equal(stat(path, &st), 0);
  assert_true(S_ISDIR(st.st_mode));
  snprintf(path, sizeof(path), "%s/team%%2ealpha/logstore/queue-notify", root);
  assert_int_equal(stat(path, &st), 0);
  assert_true(S_ISDIR(st.st_mode));
  snprintf(path, sizeof(path),
           "%s/team%%2ealpha/logstore/segments/seg-0000000000000001.log", root);
  assert_int_equal(stat(path, &st), 0);
  assert_true(S_ISREG(st.st_mode));
  assert_true(st.st_size > (off_t)TEST_POUCH_HEADER_SIZE);
  snprintf(path, sizeof(path),
           "%s/team%%2ealpha/logstore/manifest/manifest.log", root);
  test_read_file_text(path, manifest_text, sizeof(manifest_text));
  assert_string_equal(manifest_text, "open seg-0000000000000001.log\n");

  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_segment_payload_refs_survive_root_log_truncation(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_state_res put_res;
  lc_pouch_state_info state_info;
  lc_pouch_put_object_opts object_opts;
  lc_pouch_object_selector object_selector;
  lc_pouch_object_info object_info;
  lc_pouch_enqueue_opts enqueue_opts;
  lc_pouch_dequeue_opts dequeue_opts;
  lc_pouch_queue_message_info enqueued;
  lc_pouch_queue_message_info dequeued;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "segment-payload-refs");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&put_res, 0, sizeof(put_res));
  memset(&state_info, 0, sizeof(state_info));
  memset(&object_opts, 0, sizeof(object_opts));
  memset(&object_selector, 0, sizeof(object_selector));
  memset(&object_info, 0, sizeof(object_info));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&dequeue_opts, 0, sizeof(dequeue_opts));
  memset(&enqueued, 0, sizeof(enqueued));
  memset(&dequeued, 0, sizeof(dequeued));
  memset(&error, 0, sizeof(error));
  store = NULL;
  read_body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("state-segment-body");
  rc = store->write_state(store, "default", "alpha", source, NULL, &put_res,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  object_opts.name = "payload.txt";
  object_opts.content_type = "text/plain";
  source = source_from_text("object-segment-body");
  rc = store->put_object(store, "default", "alpha", source, &object_opts,
                         &object_info, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_object_info_cleanup(&allocator, &object_info);

  enqueue_opts.content_type = "text/plain";
  enqueue_opts.visibility_timeout_seconds = 30L;
  enqueue_opts.ttl_seconds = 3600L;
  enqueue_opts.max_attempts = 3;
  source = source_from_text("queue-segment-body");
  rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                              &enqueued, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  truncate_store_log(root);

  rc = store->read_state(store, "default", "alpha", &read_body, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_false(state_info.no_content);
  text = read_source_text(read_body);
  assert_string_equal(text, "state-segment-body");
  free(text);
  lc_source_close(read_body);
  read_body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  object_selector.name = "payload.txt";
  rc = store->get_object(store, "default", "alpha", &object_selector,
                         &read_body, &object_info, &error);
  assert_int_equal(rc, LC_OK);
  text = read_source_text(read_body);
  assert_string_equal(text, "object-segment-body");
  free(text);
  lc_source_close(read_body);
  read_body = NULL;
  lc_pouch_object_info_cleanup(&allocator, &object_info);

  dequeue_opts.owner = "worker-a";
  dequeue_opts.visibility_timeout_seconds = 30L;
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts,
                              &read_body, &dequeued, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(read_body);
  text = read_source_text(read_body);
  assert_string_equal(text, "queue-segment-body");
  free(text);
  lc_source_close(read_body);

  lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);
  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_segment_generation_refreshes_independent_handle(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *writer;
  lc_pouch_store *reader;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_state_res put_res;
  lc_pouch_state_info info;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "segment-generation-refresh");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&put_res, 0, sizeof(put_res));
  memset(&info, 0, sizeof(info));
  memset(&error, 0, sizeof(error));
  writer = NULL;
  reader = NULL;
  read_body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &writer, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_disk_open(root, &allocator, &reader, &error);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("{\"fresh\":true}");
  rc = writer->write_state(writer, "default", "shared", source, NULL, &put_res,
                           &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  truncate_store_log(root);

  rc = reader->read_state(reader, "default", "shared", &read_body, &info,
                          &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  text = read_source_text(read_body);
  assert_string_equal(text, "{\"fresh\":true}");
  free(text);
  lc_source_close(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);

  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  rc = reader->close(reader, &error);
  assert_int_equal(rc, LC_OK);
  rc = writer->close(writer, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_memory_records_append_only_to_segments(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res meta_res;
  lc_pouch_put_state_res put_res;
  lc_pouch_put_state_res removed_res;
  lc_pouch_put_state_res staged_res;
  lc_pouch_put_state_res promoted_res;
  lc_error error;
  off_t root_bytes;
  off_t after_state_put_bytes;
  off_t after_stage_bytes;
  int removed;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "segment-memory-records");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&meta, 0, sizeof(meta));
  memset(&meta_res, 0, sizeof(meta_res));
  memset(&put_res, 0, sizeof(put_res));
  memset(&removed_res, 0, sizeof(removed_res));
  memset(&staged_res, 0, sizeof(staged_res));
  memset(&promoted_res, 0, sizeof(promoted_res));
  memset(&error, 0, sizeof(error));
  store = NULL;
  removed = 0;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  root_bytes = test_log_size(root);

  meta.owner = "owner";
  meta.lease_id = "lease";
  meta.version = 1L;
  rc = store->store_meta(store, "default", "meta-key", &meta, NULL, &meta_res,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(test_log_size(root), root_bytes);
  assert_int_equal(count_namespace_segment_records_of_type(
                       root, "default", TEST_POUCH_RECORD_META_PUT),
                   1U);

  rc = store->delete_meta(store, "default", "meta-key", meta_res.etag, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(test_log_size(root), root_bytes);
  assert_int_equal(count_namespace_segment_records_of_type(
                       root, "default", TEST_POUCH_RECORD_META_REMOVE),
                   1U);

  source = source_from_text("remove-me");
  rc = store->write_state(store, "default", "remove-key", source, NULL,
                          &put_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  after_state_put_bytes = test_log_size(root);
  assert_int_equal(after_state_put_bytes, root_bytes);
  assert_int_equal(count_namespace_segment_records_of_type(
                       root, "default", TEST_POUCH_RECORD_STATE_PUT),
                   1U);
  rc = store->remove_state(store, "default", "remove-key",
                           put_res.new_state_etag, &removed, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(removed);
  assert_int_equal(test_log_size(root), after_state_put_bytes);
  assert_int_equal(count_namespace_segment_records_of_type(
                       root, "default", TEST_POUCH_RECORD_STATE_REMOVE),
                   1U);

  source = source_from_text("linked-body");
  rc = store->stage_state(store, "default", "linked-key", "txn-link", source,
                          NULL, &staged_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  after_stage_bytes = test_log_size(root);
  assert_int_equal(after_stage_bytes, after_state_put_bytes);
  rc = store->promote_staged_state(store, "default", "linked-key", "txn-link",
                                   NULL, &promoted_res, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(test_log_size(root), after_stage_bytes);
  assert_int_equal(count_namespace_segment_records_of_type(
                       root, "default", TEST_POUCH_RECORD_STATE_LINK),
                   1U);

  lc_pouch_put_state_res_cleanup(&allocator, &promoted_res);
  lc_pouch_put_state_res_cleanup(&allocator, &staged_res);
  lc_pouch_put_state_res_cleanup(&allocator, &removed_res);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  lc_pouch_store_meta_res_cleanup(&allocator, &meta_res);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_replay_recovers_state_from_namespace_segment(void **state) {
  char root[256];
  char log_path[512];
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
  test_root_path(root, sizeof(root), "segment-replay-state");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&opts, 0, sizeof(opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&info, 0, sizeof(info));
  memset(&error, 0, sizeof(error));
  store = NULL;
  read_body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("{\"value\":2}");
  opts.content_type = "application/json";
  rc = store->write_state(store, "team.alpha", "alpha", source, &opts, &put_res,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_true(test_active_segment_size(root, "team%2ealpha") >
              (off_t)TEST_POUCH_HEADER_SIZE);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  snprintf(log_path, sizeof(log_path), "%s/store.log", root);
  assert_int_equal(unlink(log_path), 0);

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->read_state(store, "team.alpha", "alpha", &read_body, &info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  assert_string_equal(info.content_type, "application/json");
  assert_string_equal(info.etag, put_res.new_state_etag);
  assert_int_equal(info.version, put_res.new_version);
  text = read_source_text(read_body);
  assert_string_equal(text, "{\"value\":2}");
  free(text);
  lc_source_close(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);

  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_replay_repairs_missing_namespace_manifest(void **state) {
  char root[256];
  char manifest_path[512];
  char manifest_text[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_state_res put_res;
  lc_pouch_state_info info;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "manifest-repair");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&put_res, 0, sizeof(put_res));
  memset(&info, 0, sizeof(info));
  memset(&error, 0, sizeof(error));
  store = NULL;
  read_body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("manifest-repair-body");
  rc = store->write_state(store, "team.alpha", "alpha", source, NULL, &put_res,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  test_manifest_path(root, "team%2ealpha", manifest_path,
                     sizeof(manifest_path));
  assert_int_equal(unlink(manifest_path), 0);

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  test_read_file_text(manifest_path, manifest_text, sizeof(manifest_text));
  assert_string_equal(manifest_text, "open seg-0000000000000001.log\n");
  rc = store->read_state(store, "team.alpha", "alpha", &read_body, &info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  text = read_source_text(read_body);
  assert_string_equal(text, "manifest-repair-body");
  free(text);
  lc_source_close(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);

  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_replay_repairs_crash_incomplete_namespace_manifest(void **state) {
  char root[256];
  char manifest_path[512];
  char manifest_text[1024];
  char repaired_manifest_text[1024];
  const char partial_open[] = "open seg-000000000";
  char *open2;
  size_t prefix_len;
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  counting_source large_source;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_state_res put_res;
  lc_pouch_state_info info;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "manifest-crash-incomplete-repair");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&put_res, 0, sizeof(put_res));
  memset(&info, 0, sizeof(info));
  memset(&error, 0, sizeof(error));
  store = NULL;
  read_body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  counting_source_init(&large_source, 40000U);
  rc = store->write_state(store, "default", "large-a", &large_source.pub, NULL,
                          &put_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  memset(&put_res, 0, sizeof(put_res));

  counting_source_init(&large_source, 40000U);
  rc = store->write_state(store, "default", "large-b", &large_source.pub, NULL,
                          &put_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  memset(&put_res, 0, sizeof(put_res));

  source = source_from_text("manifest-repaired-tail");
  rc = store->write_state(store, "default", "tail", source, NULL, &put_res,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  test_manifest_path(root, "default", manifest_path, sizeof(manifest_path));
  test_read_file_text(manifest_path, manifest_text, sizeof(manifest_text));
  open2 = strstr(manifest_text, "open seg-0000000000000002.log\n");
  assert_non_null(open2);
  prefix_len = (size_t)(open2 - manifest_text);
  assert_true(prefix_len + sizeof(partial_open) <
              sizeof(repaired_manifest_text));
  memcpy(repaired_manifest_text, manifest_text, prefix_len);
  memcpy(repaired_manifest_text + prefix_len, partial_open,
         sizeof(partial_open));
  test_write_text_file(manifest_path, repaired_manifest_text);

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  test_read_file_text(manifest_path, manifest_text, sizeof(manifest_text));
  assert_non_null(strstr(manifest_text, "open seg-000000000\n"
                                        "open seg-0000000000000002.log\n"));
  rc = store->read_state(store, "default", "tail", &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  text = read_source_text(read_body);
  assert_string_equal(text, "manifest-repaired-tail");
  free(text);
  lc_source_close(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);

  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_segment_rotation_replays_multiple_segments(void **state) {
  char root[256];
  char log_path[512];
  char manifest_path[512];
  char manifest_text[512];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  counting_source large_source;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_state_res put_res;
  lc_pouch_state_info info;
  lc_error error;
  char *text;
  size_t read_length;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "segment-rotation");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&put_res, 0, sizeof(put_res));
  memset(&info, 0, sizeof(info));
  memset(&error, 0, sizeof(error));
  store = NULL;
  read_body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  counting_source_init(&large_source, 40000U);
  rc = store->write_state(store, "default", "large-a", &large_source.pub, NULL,
                          &put_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  memset(&put_res, 0, sizeof(put_res));

  counting_source_init(&large_source, 40000U);
  rc = store->write_state(store, "default", "large-b", &large_source.pub, NULL,
                          &put_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  memset(&put_res, 0, sizeof(put_res));
  assert_true(test_segment_size(root, "default", 1UL) > (off_t)(64U * 1024U));

  source = source_from_text("rotated-tail");
  rc = store->write_state(store, "default", "tail", source, NULL, &put_res,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_true(test_segment_size(root, "default", 2UL) >
              (off_t)TEST_POUCH_HEADER_SIZE);

  test_manifest_path(root, "default", manifest_path, sizeof(manifest_path));
  test_read_file_text(manifest_path, manifest_text, sizeof(manifest_text));
  assert_non_null(strstr(manifest_text, "open seg-0000000000000001.log\n"));
  assert_non_null(strstr(manifest_text, "seal seg-0000000000000001.log\n"));
  assert_non_null(strstr(manifest_text, "open seg-0000000000000002.log\n"));

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;
  snprintf(log_path, sizeof(log_path), "%s/store.log", root);
  assert_int_equal(unlink(log_path), 0);

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc =
      store->read_state(store, "default", "large-a", &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  read_length = read_source_count_x(read_body);
  assert_int_equal(read_length, 40000U);
  lc_source_close(read_body);
  read_body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &info);

  rc = store->read_state(store, "default", "tail", &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  text = read_source_text(read_body);
  assert_string_equal(text, "rotated-tail");
  free(text);
  lc_source_close(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);

  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_segment_replay_truncates_rotated_tail(void **state) {
  char root[256];
  char log_path[512];
  char segment_path[512];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  counting_source large_source;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_state_res put_res;
  lc_pouch_state_info info;
  lc_error error;
  char *text;
  off_t clean_size;
  off_t dirty_size;
  int fd;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "segment-tail-truncate");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&put_res, 0, sizeof(put_res));
  memset(&info, 0, sizeof(info));
  memset(&error, 0, sizeof(error));
  store = NULL;
  read_body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  counting_source_init(&large_source, 40000U);
  rc = store->write_state(store, "default", "large-a", &large_source.pub, NULL,
                          &put_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  memset(&put_res, 0, sizeof(put_res));

  counting_source_init(&large_source, 40000U);
  rc = store->write_state(store, "default", "large-b", &large_source.pub, NULL,
                          &put_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  memset(&put_res, 0, sizeof(put_res));

  source = source_from_text("rotated-tail");
  rc = store->write_state(store, "default", "tail", source, NULL, &put_res,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  test_segment_path(root, "default", 2UL, segment_path, sizeof(segment_path));
  clean_size = test_segment_size(root, "default", 2UL);
  fd = open(segment_path, O_WRONLY | O_APPEND);
  assert_true(fd >= 0);
  assert_int_equal(write(fd, "bad", 3U), 3);
  assert_int_equal(close(fd), 0);
  dirty_size = test_segment_size(root, "default", 2UL);
  assert_int_equal(dirty_size, clean_size + 3);

  snprintf(log_path, sizeof(log_path), "%s/store.log", root);
  assert_int_equal(unlink(log_path), 0);

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(test_segment_size(root, "default", 2UL), clean_size);
  rc = store->read_state(store, "default", "tail", &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  text = read_source_text(read_body);
  assert_string_equal(text, "rotated-tail");
  free(text);
  lc_source_close(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);

  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_segment_replay_honors_manifest_obsolete(void **state) {
  char root[256];
  char log_path[512];
  char manifest_path[512];
  const char obsolete_line[] = "obsolete seg-0000000000000001.log\n";
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  counting_source large_source;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_state_res put_res;
  lc_pouch_state_info info;
  lc_error error;
  char *text;
  int fd;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "segment-manifest-obsolete");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&put_res, 0, sizeof(put_res));
  memset(&info, 0, sizeof(info));
  memset(&error, 0, sizeof(error));
  store = NULL;
  read_body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  counting_source_init(&large_source, 40000U);
  rc = store->write_state(store, "default", "large-a", &large_source.pub, NULL,
                          &put_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  memset(&put_res, 0, sizeof(put_res));

  counting_source_init(&large_source, 40000U);
  rc = store->write_state(store, "default", "large-b", &large_source.pub, NULL,
                          &put_res, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  memset(&put_res, 0, sizeof(put_res));

  source = source_from_text("rotated-tail");
  rc = store->write_state(store, "default", "tail", source, NULL, &put_res,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  test_manifest_path(root, "default", manifest_path, sizeof(manifest_path));
  fd = open(manifest_path, O_WRONLY | O_APPEND);
  assert_true(fd >= 0);
  assert_int_equal(write(fd, obsolete_line, sizeof(obsolete_line) - 1U),
                   (ssize_t)(sizeof(obsolete_line) - 1U));
  assert_int_equal(close(fd), 0);

  snprintf(log_path, sizeof(log_path), "%s/store.log", root);
  assert_int_equal(unlink(log_path), 0);

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc =
      store->read_state(store, "default", "large-a", &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(info.no_content);
  assert_null(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);

  rc = store->read_state(store, "default", "tail", &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  text = read_source_text(read_body);
  assert_string_equal(text, "rotated-tail");
  free(text);
  lc_source_close(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);

  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_replay_installed_snapshot_without_segment_tail(void **state) {
  char root[256];
  char segment_path[512];
  char snapshot_path[512];
  char manifest_path[512];
  const char snapshot_line[] = "snapshot snap-0000000000000001.log\n";
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *body;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res put_res;
  lc_pouch_put_state_res tail_res;
  lc_pouch_state_info info;
  lc_error error;
  char *text;
  int fd;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "snapshot-replay");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&tail_res, 0, sizeof(tail_res));
  memset(&info, 0, sizeof(info));
  store = NULL;
  body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  opts.content_type = "text/plain";
  source = source_from_text("snapshot-body");
  rc = store->write_state(store, "default", "snap-key", source, &opts, &put_res,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  test_segment_path(root, "default", 1UL, segment_path, sizeof(segment_path));
  test_snapshot_path(root, "default", 1UL, snapshot_path,
                     sizeof(snapshot_path));
  assert_int_equal(rename(segment_path, snapshot_path), 0);
  test_manifest_path(root, "default", manifest_path, sizeof(manifest_path));
  fd = open(manifest_path, O_WRONLY | O_TRUNC);
  assert_true(fd >= 0);
  assert_int_equal(write(fd, snapshot_line, sizeof(snapshot_line) - 1U),
                   (ssize_t)(sizeof(snapshot_line) - 1U));
  assert_int_equal(close(fd), 0);

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->read_state(store, "default", "snap-key", &body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  assert_string_equal(info.etag, put_res.new_state_etag);
  assert_int_equal(info.version, put_res.new_version);
  text = read_source_text(body);
  assert_string_equal(text, "snapshot-body");
  free(text);
  lc_source_close(body);
  body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &info);

  source = source_from_text("tail-body");
  opts.if_state_etag = put_res.new_state_etag;
  rc = store->write_state(store, "default", "snap-key", source, &opts,
                          &tail_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_true(tail_res.new_version > put_res.new_version);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->read_state(store, "default", "snap-key", &body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  assert_string_equal(info.etag, tail_res.new_state_etag);
  assert_int_equal(info.version, tail_res.new_version);
  text = read_source_text(body);
  assert_string_equal(text, "tail-body");
  free(text);
  lc_source_close(body);
  body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &info);

  lc_pouch_put_state_res_cleanup(&allocator, &tail_res);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_replay_cleans_obsolete_snapshot_files(void **state) {
  char root[256];
  char segment_path[512];
  char active_snapshot_path[512];
  char obsolete_snapshot_path[512];
  char manifest_path[512];
  const char manifest_text[] = "snapshot snap-0000000000000001.log\n"
                               "snapshot snap-0000000000000002.log\n"
                               "obsolete snap-0000000000000001.log\n";
  const char obsolete_text[] = "obsolete snapshot bytes";
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *body;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res put_res;
  lc_pouch_state_info info;
  lc_error error;
  char *text;
  int fd;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "snapshot-obsolete-cleanup");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&info, 0, sizeof(info));
  store = NULL;
  body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  opts.content_type = "text/plain";
  source = source_from_text("active-snapshot-body");
  rc = store->write_state(store, "default", "snap-key", source, &opts, &put_res,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  test_segment_path(root, "default", 1UL, segment_path, sizeof(segment_path));
  test_snapshot_path(root, "default", 1UL, obsolete_snapshot_path,
                     sizeof(obsolete_snapshot_path));
  test_snapshot_path(root, "default", 2UL, active_snapshot_path,
                     sizeof(active_snapshot_path));
  assert_int_equal(rename(segment_path, active_snapshot_path), 0);
  fd = open(obsolete_snapshot_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  assert_true(fd >= 0);
  assert_int_equal(write(fd, obsolete_text, sizeof(obsolete_text) - 1U),
                   (ssize_t)(sizeof(obsolete_text) - 1U));
  assert_int_equal(close(fd), 0);

  test_manifest_path(root, "default", manifest_path, sizeof(manifest_path));
  fd = open(manifest_path, O_WRONLY | O_TRUNC);
  assert_true(fd >= 0);
  assert_int_equal(write(fd, manifest_text, sizeof(manifest_text) - 1U),
                   (ssize_t)(sizeof(manifest_text) - 1U));
  assert_int_equal(close(fd), 0);

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(access(obsolete_snapshot_path, F_OK), -1);
  assert_int_equal(errno, ENOENT);
  assert_int_equal(access(active_snapshot_path, R_OK), 0);
  rc = store->read_state(store, "default", "snap-key", &body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  assert_string_equal(info.etag, put_res.new_state_etag);
  assert_int_equal(info.version, put_res.new_version);
  text = read_source_text(body);
  assert_string_equal(text, "active-snapshot-body");
  free(text);
  lc_source_close(body);
  body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &info);

  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_maintenance_cleanup_cleans_obsolete_snapshot_files(
    void **state) {
  char root[256];
  char segment_path[512];
  char active_snapshot_path[512];
  char obsolete_snapshot_path[512];
  char manifest_path[512];
  const char manifest_text[] = "snapshot snap-0000000000000001.log\n"
                               "snapshot snap-0000000000000002.log\n"
                               "obsolete snap-0000000000000001.log\n";
  const char obsolete_text[] = "obsolete snapshot bytes";
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *body;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res put_res;
  lc_pouch_state_info info;
  lc_pouch_maintenance_res maintenance;
  lc_error error;
  char *text;
  int fd;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "maintenance-snapshot-cleanup");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&info, 0, sizeof(info));
  memset(&maintenance, 0, sizeof(maintenance));
  store = NULL;
  body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  opts.content_type = "text/plain";
  source = source_from_text("active-snapshot-body");
  rc = store->write_state(store, "default", "snap-key", source, &opts,
                          &put_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  test_segment_path(root, "default", 1UL, segment_path, sizeof(segment_path));
  test_snapshot_path(root, "default", 1UL, obsolete_snapshot_path,
                     sizeof(obsolete_snapshot_path));
  test_snapshot_path(root, "default", 2UL, active_snapshot_path,
                     sizeof(active_snapshot_path));
  assert_int_equal(rename(segment_path, active_snapshot_path), 0);
  fd = open(obsolete_snapshot_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  assert_true(fd >= 0);
  assert_int_equal(write(fd, obsolete_text, sizeof(obsolete_text) - 1U),
                   (ssize_t)(sizeof(obsolete_text) - 1U));
  assert_int_equal(close(fd), 0);

  test_manifest_path(root, "default", manifest_path, sizeof(manifest_path));
  fd = open(manifest_path, O_WRONLY | O_TRUNC);
  assert_true(fd >= 0);
  assert_int_equal(write(fd, manifest_text, sizeof(manifest_text) - 1U),
                   (ssize_t)(sizeof(manifest_text) - 1U));
  assert_int_equal(close(fd), 0);

  rc = store->maintenance(store, "cleanup", &maintenance, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance.mode, "cleanup");
  assert_string_equal(maintenance.reason, "cleanup-completed");
  assert_int_equal(maintenance.accepted, 1);
  assert_int_equal(maintenance.compaction_enabled, 0);
  assert_int_equal(maintenance.compaction.accepted, 0);
  lc_pouch_maintenance_res_cleanup(&allocator, &maintenance);
  assert_int_equal(access(obsolete_snapshot_path, F_OK), -1);
  assert_int_equal(errno, ENOENT);
  assert_int_equal(access(active_snapshot_path, R_OK), 0);

  rc = store->read_state(store, "default", "snap-key", &body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  assert_string_equal(info.etag, put_res.new_state_etag);
  assert_int_equal(info.version, put_res.new_version);
  text = read_source_text(body);
  assert_string_equal(text, "active-snapshot-body");
  free(text);
  lc_source_close(body);
  body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &info);

  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_maintenance_cleanup_honors_obsolete_delete_grace(
    void **state) {
  char root[256];
  char segment_path[512];
  char active_snapshot_path[512];
  char obsolete_snapshot_path[512];
  char manifest_path[512];
  const char manifest_text[] = "snapshot snap-0000000000000001.log\n"
                               "snapshot snap-0000000000000002.log\n"
                               "obsolete snap-0000000000000001.log\n";
  const char obsolete_text[] = "obsolete snapshot bytes";
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_disk_open_opts open_opts;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res put_res;
  lc_pouch_maintenance_res maintenance;
  lc_error error;
  time_t now;
  int fd;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "maintenance-delete-grace");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&open_opts, 0, sizeof(open_opts));
  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&maintenance, 0, sizeof(maintenance));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  opts.content_type = "text/plain";
  source = source_from_text("active-snapshot-body");
  rc = store->write_state(store, "default", "snap-key", source, &opts,
                          &put_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  test_segment_path(root, "default", 1UL, segment_path, sizeof(segment_path));
  test_snapshot_path(root, "default", 1UL, obsolete_snapshot_path,
                     sizeof(obsolete_snapshot_path));
  test_snapshot_path(root, "default", 2UL, active_snapshot_path,
                     sizeof(active_snapshot_path));
  assert_int_equal(rename(segment_path, active_snapshot_path), 0);
  fd = open(obsolete_snapshot_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  assert_true(fd >= 0);
  assert_int_equal(write(fd, obsolete_text, sizeof(obsolete_text) - 1U),
                   (ssize_t)(sizeof(obsolete_text) - 1U));
  assert_int_equal(close(fd), 0);

  test_manifest_path(root, "default", manifest_path, sizeof(manifest_path));
  fd = open(manifest_path, O_WRONLY | O_TRUNC);
  assert_true(fd >= 0);
  assert_int_equal(write(fd, manifest_text, sizeof(manifest_text) - 1U),
                   (ssize_t)(sizeof(manifest_text) - 1U));
  assert_int_equal(close(fd), 0);

  open_opts.background_compaction_delete_grace_seconds = 3600UL;
  rc = lc_pouch_disk_open_with_options(root, &allocator, &open_opts, &store,
                                       &error);
  assert_int_equal(rc, LC_OK);
  rc = store->maintenance(store, "cleanup", &maintenance, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance.reason, "cleanup-completed");
  lc_pouch_maintenance_res_cleanup(&allocator, &maintenance);
  assert_int_equal(access(obsolete_snapshot_path, R_OK), 0);
  assert_int_equal(access(active_snapshot_path, R_OK), 0);

  now = time(NULL);
  assert_true(now != (time_t)-1);
  test_set_file_mtime(obsolete_snapshot_path, now - 7200);
  memset(&maintenance, 0, sizeof(maintenance));
  rc = store->maintenance(store, "cleanup", &maintenance, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance.reason, "cleanup-completed");
  lc_pouch_maintenance_res_cleanup(&allocator, &maintenance);
  assert_int_equal(access(obsolete_snapshot_path, F_OK), -1);
  assert_int_equal(errno, ENOENT);
  assert_int_equal(access(active_snapshot_path, R_OK), 0);

  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_replay_ignores_root_store_log_without_segments(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *body;
  lc_pouch_state_info info;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "root-log-not-authoritative");
  test_cleanup_root(root);
  assert_int_equal(mkdir(root, 0777), 0);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&info, 0, sizeof(info));
  store = NULL;
  body = NULL;

  write_root_store_log_state_put(root, "default", "legacy-key", "text/plain",
                                 "root-only-etag", "root-only-body", 7UL);

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->read_state(store, "default", "legacy-key", &body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(info.no_content);
  assert_null(body);

  lc_pouch_state_info_cleanup(&allocator, &info);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_state_put_propagates_source_failure_before_append(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res res;
  lc_source *source;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "state-source-failure");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&opts, 0, sizeof(opts));
  memset(&res, 0, sizeof(res));
  memset(&error, 0, sizeof(error));
  store = NULL;
  source = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  opts.content_type = "application/json";
  source = source_that_fails_after_prefix("{\"partial\":");
  rc = store->write_state(store, "default", "source-failure", source, &opts,
                          &res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_PROTOCOL);
  assert_string_equal(error.message, "intentional pouch disk source failure");
  assert_int_equal(count_log_records_of_type(root, TEST_POUCH_RECORD_STATE_PUT),
                   0U);
  lc_error_cleanup(&error);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
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

static void test_query_index_scan_skips_replay_after_same_handle_write(
    void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res put_res;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res meta_res;
  lc_pouch_document_eq_term term;
  lc_pouch_query_index_scan_req req;
  lc_pouch_query_index_scan_res scan;
  lc_pouch_lock_status before_status;
  lc_pouch_lock_status after_status;
  scan_capture rows;
  lc_error error;
  unsigned long replay_refreshes;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-index-no-replay");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&meta, 0, sizeof(meta));
  memset(&meta_res, 0, sizeof(meta_res));
  memset(&term, 0, sizeof(term));
  memset(&req, 0, sizeof(req));
  memset(&scan, 0, sizeof(scan));
  memset(&before_status, 0, sizeof(before_status));
  memset(&after_status, 0, sizeof(after_status));
  memset(&rows, 0, sizeof(rows));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(store->query_index_scan);
  assert_non_null(store->lock_status);

  opts.content_type = "application/json";
  source = source_from_text("{\"bucket\":\"needle\",\"value\":1}");
  rc = store->write_state(store, "default", "hot", source, &opts, &put_res,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  meta.owner = "owner";
  meta.state_etag = put_res.new_state_etag;
  meta.version = put_res.new_version;
  rc = store->store_meta(store, "default", "hot", &meta, NULL, &meta_res,
                         &error);
  assert_int_equal(rc, LC_OK);

  rc = store->lock_status(store, &before_status, &error);
  assert_int_equal(rc, LC_OK);
  replay_refreshes = before_status.replay_refreshes;
  lc_pouch_lock_status_cleanup(&allocator, &before_status);

  term.field = "/bucket";
  term.value = "s:needle";
  req.namespace_name = "default";
  req.document_eq_terms = &term;
  req.document_eq_term_count = 1U;
  rc = store->query_index_scan(store, &req, capture_scan_row, &rows, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(rows.count, 1U);
  assert_string_equal(rows.keys[0], "hot");
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->lock_status(store, &after_status, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(after_status.replay_refreshes, replay_refreshes);
  lc_pouch_lock_status_cleanup(&allocator, &after_status);

  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  lc_pouch_store_meta_res_cleanup(&allocator, &meta_res);
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

  rc = store->remove_state(store, "default", "beta", "wrong", &removed, &error);
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
  rewrite_query_index_as_v1_without_owner(root);
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

static void test_state_lookup_index_orders_updates_and_replays(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_state_res alpha;
  lc_pouch_put_state_res beta;
  lc_pouch_put_state_res gamma;
  lc_pouch_put_state_res beta_update;
  lc_pouch_state_info info;
  lc_error error;
  char *text;
  int removed;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "state-lookup-index-order");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&alpha, 0, sizeof(alpha));
  memset(&beta, 0, sizeof(beta));
  memset(&gamma, 0, sizeof(gamma));
  memset(&beta_update, 0, sizeof(beta_update));
  memset(&info, 0, sizeof(info));
  store = NULL;
  read_body = NULL;
  text = NULL;
  removed = 0;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("gamma");
  rc = store->write_state(store, "default", "gamma", source, NULL, &gamma,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("alpha");
  rc = store->write_state(store, "default", "alpha", source, NULL, &alpha,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("beta");
  rc = store->write_state(store, "default", "beta", source, NULL, &beta,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("beta-updated");
  rc = store->write_state(store, "default", "beta", source, NULL,
                          &beta_update, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  rc = store->remove_state(store, "default", "gamma", gamma.new_state_etag,
                           &removed, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(removed);

  rc = store->read_state(store, "default", "alpha", &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  text = read_source_text(read_body);
  assert_string_equal(text, "alpha");
  free(text);
  lc_source_close(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);
  read_body = NULL;
  memset(&info, 0, sizeof(info));

  rc = store->read_state(store, "default", "beta", &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  assert_string_equal(info.etag, beta_update.new_state_etag);
  text = read_source_text(read_body);
  assert_string_equal(text, "beta-updated");
  free(text);
  lc_source_close(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);
  read_body = NULL;
  memset(&info, 0, sizeof(info));

  rc = store->read_state(store, "default", "gamma", &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(info.no_content);
  assert_null(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  rc = store->read_state(store, "default", "beta", &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  assert_string_equal(info.etag, beta_update.new_state_etag);
  text = read_source_text(read_body);
  assert_string_equal(text, "beta-updated");
  free(text);
  lc_source_close(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);
  read_body = NULL;
  memset(&info, 0, sizeof(info));

  rc = store->read_state(store, "default", "gamma", &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(info.no_content);
  assert_null(read_body);
  lc_pouch_state_info_cleanup(&allocator, &info);

  lc_pouch_put_state_res_cleanup(&allocator, &alpha);
  lc_pouch_put_state_res_cleanup(&allocator, &beta);
  lc_pouch_put_state_res_cleanup(&allocator, &gamma);
  lc_pouch_put_state_res_cleanup(&allocator, &beta_update);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_state_write_index_allocation_failure_replays_cleanly(void **state) {
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
  rc = store->write_state(store, "default", "state-key", source, &opts, &first,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  opts.content_type = replacement_type;
  tracked.fail_malloc_size = strlen(replacement_type) + 1U;
  source = source_from_text("payload-two");
  rc = store->write_state(store, "default", "state-key", source, &opts, &second,
                          &error);
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

  rc = store->promote_staged_state(store, "default", "lease-key", "txn-1", NULL,
                                   &promoted, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(promoted.new_state_etag);
  assert_string_equal(promoted.new_state_etag, staged.new_state_etag);
  assert_int_equal(promoted.bytes, 9L);
  assert_int_equal(count_namespace_segment_records_of_type(
                       root, "default", TEST_POUCH_RECORD_STATE_LINK),
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

  rc = store->promote_staged_state(store, "default", "lease-key", "txn-2", NULL,
                                   &second_promoted, &error);
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
  assert_int_equal(count_namespace_segment_records_of_type(
                       root, "default", TEST_POUCH_RECORD_STATE_LINK),
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

static void test_staged_state_remove_promote_discard_and_reopen(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_state_res base;
  lc_pouch_put_state_res staged_remove;
  lc_pouch_put_state_res promoted_remove;
  lc_pouch_put_state_res discarded_remove;
  lc_pouch_promote_staged_opts promote_opts;
  lc_pouch_discard_staged_opts discard_opts;
  lc_pouch_state_info info;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "staged-remove");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&base, 0, sizeof(base));
  memset(&staged_remove, 0, sizeof(staged_remove));
  memset(&promoted_remove, 0, sizeof(promoted_remove));
  memset(&discarded_remove, 0, sizeof(discarded_remove));
  memset(&promote_opts, 0, sizeof(promote_opts));
  memset(&discard_opts, 0, sizeof(discard_opts));
  memset(&info, 0, sizeof(info));
  store = NULL;
  read_body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(store->stage_state_remove);

  source = source_from_text("base");
  rc = store->write_state(store, "default", "lease-key", source, NULL, &base,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  rc = store->stage_state_remove(store, "default", "lease-key", "txn-remove",
                                 NULL, &staged_remove, &error);
  assert_int_equal(rc, LC_OK);
  assert_null(staged_remove.new_state_etag);

  rc = store->load_staged_state(store, "default", "lease-key", "txn-remove",
                                &read_body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(info.no_content);
  assert_null(read_body);
  assert_int_equal(info.version, staged_remove.new_version);
  lc_pouch_state_info_cleanup(&allocator, &info);

  rc = store->read_state(store, "default", "lease-key", &read_body, &info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  text = read_source_text(read_body);
  assert_string_equal(text, "base");
  free(text);
  lc_source_close(read_body);
  read_body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &info);

  discard_opts.ignore_not_found = 0;
  rc = store->discard_staged_state(store, "default", "lease-key", "txn-remove",
                                   &discard_opts, &error);
  assert_int_equal(rc, LC_OK);

  rc = store->read_state(store, "default", "lease-key", &read_body, &info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  text = read_source_text(read_body);
  assert_string_equal(text, "base");
  free(text);
  lc_source_close(read_body);
  read_body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &info);

  rc = store->stage_state_remove(store, "default", "lease-key", "txn-remove-2",
                                 NULL, &discarded_remove, &error);
  assert_int_equal(rc, LC_OK);
  promote_opts.expected_head_etag = base.new_state_etag;
  rc =
      store->promote_staged_state(store, "default", "lease-key", "txn-remove-2",
                                  &promote_opts, &promoted_remove, &error);
  assert_int_equal(rc, LC_OK);
  assert_null(promoted_remove.new_state_etag);
  assert_true(promoted_remove.new_version > base.new_version);

  rc = store->read_state(store, "default", "lease-key", &read_body, &info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_true(info.no_content);
  assert_null(read_body);
  assert_int_equal(info.version, promoted_remove.new_version);
  lc_pouch_state_info_cleanup(&allocator, &info);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  rc = store->read_state(store, "default", "lease-key", &read_body, &info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_true(info.no_content);
  assert_null(read_body);
  assert_int_equal(info.version, promoted_remove.new_version);
  lc_pouch_state_info_cleanup(&allocator, &info);

  lc_pouch_put_state_res_cleanup(&allocator, &base);
  lc_pouch_put_state_res_cleanup(&allocator, &staged_remove);
  lc_pouch_put_state_res_cleanup(&allocator, &promoted_remove);
  lc_pouch_put_state_res_cleanup(&allocator, &discarded_remove);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_staged_state_listing_orders_paginates_and_replays(void **state) {
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
  rc = store->stage_state(store, "default", "lease-key", "txn-charlie", source,
                          &state_opts, &staged_charlie, &error);
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

static void
test_replay_rebuilds_indexes_after_external_truncation(void **state) {
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

static void
test_replay_stops_at_corrupt_record_and_discards_later_records(void **state) {
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

  rc =
      store->read_state(store, "default", "corrupt", &read_body, &info, &error);
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

static void
test_replay_stops_at_oversized_record_without_allocating_payload(void **state) {
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
  rc = store->write_state(store, "default", "oversized", source, NULL, &second,
                          &error);
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
  meta.updated_at_unix = 1111L;
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
  assert_int_equal(loaded.meta.updated_at_unix, 1111L);
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
  meta.updated_at_unix = 2222L;
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
  assert_int_equal(loaded.meta.updated_at_unix, 2222L);
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
  meta.updated_at_unix = 2020L;
  meta.has_query_hidden = 1;
  meta.query_hidden = 0;
  rc = store->store_meta(store, "default", "bravo", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-a";
  meta.state_etag = "state-a";
  meta.version = 10L;
  meta.updated_at_unix = 1010L;
  meta.query_hidden = 1;
  rc = store->store_meta(store, "default", "alpha", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-c";
  meta.state_etag = "state-c";
  meta.version = 30L;
  meta.updated_at_unix = 3030L;
  meta.query_hidden = 0;
  rc = store->store_meta(store, "other", "aardvark", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-d";
  meta.state_etag = "state-d";
  meta.version = 40L;
  meta.updated_at_unix = 4040L;
  rc = store->store_meta(store, "default", "charlie", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  rc = store->delete_meta(store, "default", "charlie", stored.etag, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-d";
  meta.state_etag = "state-d";
  meta.version = 50L;
  meta.updated_at_unix = 5050L;
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
  assert_int_equal(capture.updated_at_unix[0], 2020L);
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
  assert_int_equal(capture.updated_at_unix[0], 5050L);
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
  assert_int_equal(capture.updated_at_unix[0], 2020L);
  assert_int_equal(capture.updated_at_unix[1], 5050L);
  assert_false(scan.truncated);
  lc_pouch_scan_meta_res_cleanup(&allocator, &scan);

  memset(&capture, 0, sizeof(capture));
  req.key = "delta";
  req.start_after = NULL;
  req.limit = 8U;
  rc = store->scan_meta(store, &req, capture_scan_row, &capture, &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1U);
  assert_string_equal(capture.keys[0], "delta");
  assert_int_equal(capture.versions[0], 50L);
  assert_false(scan.truncated);
  assert_null(scan.next_start_after);
  lc_pouch_scan_meta_res_cleanup(&allocator, &scan);

  memset(&capture, 0, sizeof(capture));
  req.start_after = "delta";
  rc = store->scan_meta(store, &req, capture_scan_row, &capture, &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 0U);
  assert_false(scan.truncated);
  assert_null(scan.next_start_after);
  lc_pouch_scan_meta_res_cleanup(&allocator, &scan);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_metadata_scan_skips_replay_after_same_handle_write(
    void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_scan_meta_req req;
  lc_pouch_scan_meta_res scan;
  lc_pouch_lock_status before_status;
  lc_pouch_lock_status after_status;
  scan_capture capture;
  lc_error error;
  unsigned long replay_refreshes;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "meta-scan-no-replay");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  memset(&req, 0, sizeof(req));
  memset(&scan, 0, sizeof(scan));
  memset(&before_status, 0, sizeof(before_status));
  memset(&after_status, 0, sizeof(after_status));
  memset(&capture, 0, sizeof(capture));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(store->lock_status);

  meta.owner = "owner";
  meta.lease_id = "lease";
  meta.state_etag = "state";
  meta.version = 1L;
  rc = store->store_meta(store, "default", "alpha", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  rc = store->lock_status(store, &before_status, &error);
  assert_int_equal(rc, LC_OK);
  replay_refreshes = before_status.replay_refreshes;
  lc_pouch_lock_status_cleanup(&allocator, &before_status);

  req.namespace_name = "default";
  rc = store->scan_meta(store, &req, capture_scan_row, &capture, &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1U);
  assert_string_equal(capture.keys[0], "alpha");
  assert_int_equal(capture.versions[0], 1L);
  lc_pouch_scan_meta_res_cleanup(&allocator, &scan);

  rc = store->lock_status(store, &after_status, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(after_status.replay_refreshes, replay_refreshes);
  lc_pouch_lock_status_cleanup(&allocator, &after_status);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_metadata_key_scan_orders_paginates_and_replays(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_scan_meta_req req;
  lc_pouch_scan_meta_res scan;
  key_capture capture;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "meta-key-scan");
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
  assert_non_null(store->scan_meta_keys);

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

  meta.lease_id = "lease-hidden";
  meta.state_etag = "state-hidden";
  meta.version = 3L;
  meta.has_query_hidden = 1;
  meta.query_hidden = 1;
  rc = store->store_meta(store, "default", "hidden", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-deleted";
  meta.state_etag = "state-deleted";
  meta.version = 4L;
  meta.has_query_hidden = 0;
  meta.query_hidden = 0;
  rc = store->store_meta(store, "default", "deleted", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  rc = store->delete_meta(store, "default", "deleted", stored.etag, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  req.namespace_name = "default";
  req.limit = 1U;
  rc = store->scan_meta_keys(store, &req, capture_query_key, &capture, &scan,
                             &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1U);
  assert_string_equal(capture.keys[0], "alpha");
  assert_true(scan.truncated);
  assert_string_equal(scan.next_start_after, "alpha");
  lc_pouch_scan_meta_res_cleanup(&allocator, &scan);

  memset(&capture, 0, sizeof(capture));
  req.start_after = "alpha";
  req.limit = 8U;
  rc = store->scan_meta_keys(store, &req, capture_query_key, &capture, &scan,
                             &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1U);
  assert_string_equal(capture.keys[0], "bravo");
  assert_false(scan.truncated);
  assert_null(scan.next_start_after);
  lc_pouch_scan_meta_res_cleanup(&allocator, &scan);

  memset(&capture, 0, sizeof(capture));
  req.start_after = NULL;
  req.limit = 8U;
  req.include_hidden = 1;
  rc = store->scan_meta_keys(store, &req, capture_query_key, &capture, &scan,
                             &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 3U);
  assert_string_equal(capture.keys[0], "alpha");
  assert_string_equal(capture.keys[1], "bravo");
  assert_string_equal(capture.keys[2], "hidden");
  assert_false(scan.truncated);
  assert_null(scan.next_start_after);
  lc_pouch_scan_meta_res_cleanup(&allocator, &scan);

  memset(&capture, 0, sizeof(capture));
  req.key = "bravo";
  req.start_after = NULL;
  req.limit = 8U;
  req.include_hidden = 0;
  rc = store->scan_meta_keys(store, &req, capture_query_key, &capture, &scan,
                             &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1U);
  assert_string_equal(capture.keys[0], "bravo");
  assert_false(scan.truncated);
  assert_null(scan.next_start_after);
  lc_pouch_scan_meta_res_cleanup(&allocator, &scan);

  memset(&capture, 0, sizeof(capture));
  req.start_after = "bravo";
  rc = store->scan_meta_keys(store, &req, capture_query_key, &capture, &scan,
                             &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 0U);
  assert_false(scan.truncated);
  assert_null(scan.next_start_after);
  lc_pouch_scan_meta_res_cleanup(&allocator, &scan);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_metadata_scan_can_exclude_removed_state(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts put_opts;
  lc_pouch_put_state_res alpha_state;
  lc_pouch_put_state_res removed_state;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_scan_meta_req req;
  lc_pouch_scan_meta_res scan;
  scan_capture scan_rows;
  key_capture scan_keys;
  int removed;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "meta-scan-exclude-removed");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&put_opts, 0, sizeof(put_opts));
  memset(&alpha_state, 0, sizeof(alpha_state));
  memset(&removed_state, 0, sizeof(removed_state));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  memset(&req, 0, sizeof(req));
  memset(&scan, 0, sizeof(scan));
  memset(&scan_rows, 0, sizeof(scan_rows));
  memset(&scan_keys, 0, sizeof(scan_keys));
  store = NULL;
  source = NULL;
  removed = 0;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  put_opts.content_type = "application/json";
  source = source_from_text("{\"live\":\"alpha\"}");
  rc = store->write_state(store, "default", "alpha", source, &put_opts,
                          &alpha_state, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  source = source_from_text("{\"live\":\"removed\"}");
  rc = store->write_state(store, "default", "removed", source, &put_opts,
                          &removed_state, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  meta.owner = "owner";
  meta.lease_id = "lease-alpha";
  meta.state_etag = alpha_state.new_state_etag;
  meta.version = alpha_state.new_version;
  meta.fencing_token = alpha_state.new_version;
  rc = store->store_meta(store, "default", "alpha", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-removed";
  meta.state_etag = removed_state.new_state_etag;
  meta.version = removed_state.new_version;
  meta.fencing_token = removed_state.new_version;
  rc = store->store_meta(store, "default", "removed", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  rc = store->remove_state(store, "default", "removed",
                           removed_state.new_state_etag, &removed, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(removed);

  req.namespace_name = "default";
  rc = store->scan_meta(store, &req, capture_scan_row, &scan_rows, &scan,
                        &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(scan_rows.count, 2U);
  assert_string_equal(scan_rows.keys[0], "alpha");
  assert_string_equal(scan_rows.keys[1], "removed");
  assert_false(scan.truncated);
  lc_pouch_scan_meta_res_cleanup(&allocator, &scan);

  memset(&scan_rows, 0, sizeof(scan_rows));
  req.exclude_deleted_state = 1;
  rc = store->scan_meta(store, &req, capture_scan_row, &scan_rows, &scan,
                        &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(scan_rows.count, 1U);
  assert_string_equal(scan_rows.keys[0], "alpha");
  assert_false(scan.truncated);
  lc_pouch_scan_meta_res_cleanup(&allocator, &scan);

  rc = store->scan_meta_keys(store, &req, capture_query_key, &scan_keys, &scan,
                             &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(scan_keys.count, 1U);
  assert_string_equal(scan_keys.keys[0], "alpha");
  assert_false(scan.truncated);
  lc_pouch_scan_meta_res_cleanup(&allocator, &scan);

  lc_pouch_put_state_res_cleanup(&allocator, &removed_state);
  lc_pouch_put_state_res_cleanup(&allocator, &alpha_state);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_metadata_scan_paginates_across_removed_state(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts put_opts;
  lc_pouch_put_state_res alpha_state;
  lc_pouch_put_state_res bravo_state;
  lc_pouch_put_state_res charlie_state;
  lc_pouch_put_state_res delta_state;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_scan_meta_req req;
  lc_pouch_scan_meta_res scan;
  scan_capture rows;
  key_capture keys;
  int removed;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "meta-key-page-removed");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&put_opts, 0, sizeof(put_opts));
  memset(&alpha_state, 0, sizeof(alpha_state));
  memset(&bravo_state, 0, sizeof(bravo_state));
  memset(&charlie_state, 0, sizeof(charlie_state));
  memset(&delta_state, 0, sizeof(delta_state));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  memset(&req, 0, sizeof(req));
  memset(&scan, 0, sizeof(scan));
  memset(&rows, 0, sizeof(rows));
  memset(&keys, 0, sizeof(keys));
  store = NULL;
  source = NULL;
  removed = 0;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(store->scan_meta_keys);

  put_opts.content_type = "application/json";
  source = source_from_text("{\"live\":\"alpha\"}");
  rc = store->write_state(store, "default", "alpha", source, &put_opts,
                          &alpha_state, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  source = source_from_text("{\"live\":\"bravo\"}");
  rc = store->write_state(store, "default", "bravo", source, &put_opts,
                          &bravo_state, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  source = source_from_text("{\"live\":\"charlie\"}");
  rc = store->write_state(store, "default", "charlie", source, &put_opts,
                          &charlie_state, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  source = source_from_text("{\"live\":\"delta\"}");
  rc = store->write_state(store, "default", "delta", source, &put_opts,
                          &delta_state, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  meta.owner = "owner";
  meta.lease_id = "lease-alpha";
  meta.state_etag = alpha_state.new_state_etag;
  meta.version = alpha_state.new_version;
  meta.fencing_token = alpha_state.new_version;
  rc = store->store_meta(store, "default", "alpha", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-bravo";
  meta.state_etag = bravo_state.new_state_etag;
  meta.version = bravo_state.new_version;
  meta.fencing_token = bravo_state.new_version;
  rc = store->store_meta(store, "default", "bravo", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-charlie";
  meta.state_etag = charlie_state.new_state_etag;
  meta.version = charlie_state.new_version;
  meta.fencing_token = charlie_state.new_version;
  rc = store->store_meta(store, "default", "charlie", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-delta";
  meta.state_etag = delta_state.new_state_etag;
  meta.version = delta_state.new_version;
  meta.fencing_token = delta_state.new_version;
  rc = store->store_meta(store, "default", "delta", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  rc = store->remove_state(store, "default", "bravo",
                           bravo_state.new_state_etag, &removed, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(removed);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  req.namespace_name = "default";
  req.exclude_deleted_state = 1;
  req.limit = 1U;
  rc = store->scan_meta(store, &req, capture_scan_row, &rows, &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(rows.count, 1U);
  assert_string_equal(rows.keys[0], "alpha");
  assert_true(scan.truncated);
  assert_string_equal(scan.next_start_after, "alpha");
  lc_pouch_scan_meta_res_cleanup(&allocator, &scan);

  memset(&rows, 0, sizeof(rows));
  req.start_after = "alpha";
  rc = store->scan_meta(store, &req, capture_scan_row, &rows, &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(rows.count, 1U);
  assert_string_equal(rows.keys[0], "charlie");
  assert_true(scan.truncated);
  assert_string_equal(scan.next_start_after, "charlie");
  lc_pouch_scan_meta_res_cleanup(&allocator, &scan);

  memset(&rows, 0, sizeof(rows));
  req.start_after = "charlie";
  rc = store->scan_meta(store, &req, capture_scan_row, &rows, &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(rows.count, 1U);
  assert_string_equal(rows.keys[0], "delta");
  assert_false(scan.truncated);
  assert_null(scan.next_start_after);
  lc_pouch_scan_meta_res_cleanup(&allocator, &scan);

  req.start_after = NULL;
  rc = store->scan_meta_keys(store, &req, capture_query_key, &keys, &scan,
                             &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(keys.count, 1U);
  assert_string_equal(keys.keys[0], "alpha");
  assert_true(scan.truncated);
  assert_string_equal(scan.next_start_after, "alpha");
  lc_pouch_scan_meta_res_cleanup(&allocator, &scan);

  memset(&keys, 0, sizeof(keys));
  req.start_after = "alpha";
  rc = store->scan_meta_keys(store, &req, capture_query_key, &keys, &scan,
                             &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(keys.count, 1U);
  assert_string_equal(keys.keys[0], "charlie");
  assert_true(scan.truncated);
  assert_string_equal(scan.next_start_after, "charlie");
  lc_pouch_scan_meta_res_cleanup(&allocator, &scan);

  memset(&keys, 0, sizeof(keys));
  req.start_after = "charlie";
  rc = store->scan_meta_keys(store, &req, capture_query_key, &keys, &scan,
                             &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(keys.count, 1U);
  assert_string_equal(keys.keys[0], "delta");
  assert_false(scan.truncated);
  assert_null(scan.next_start_after);
  lc_pouch_scan_meta_res_cleanup(&allocator, &scan);

  lc_pouch_put_state_res_cleanup(&allocator, &delta_state);
  lc_pouch_put_state_res_cleanup(&allocator, &charlie_state);
  lc_pouch_put_state_res_cleanup(&allocator, &bravo_state);
  lc_pouch_put_state_res_cleanup(&allocator, &alpha_state);
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
  assert_int_equal(count_namespace_segment_records_of_type(
                       root, "default", TEST_POUCH_RECORD_STATE_REMOVE),
                   0U);
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
  rc = store->stage_state(store, "default", "", "txn-root", source, &state_opts,
                          &staged, &error);
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

static void
test_query_index_scan_orders_paginates_and_reports_seq(void **state) {
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
  assert_int_equal(capture.versions[0], 1L);
  assert_false(capture.query_hidden[0]);
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
  assert_int_equal(capture.versions[0], 2L);
  assert_int_equal(capture.versions[1], 5L);
  assert_false(capture.query_hidden[0]);
  assert_false(capture.query_hidden[1]);
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

  memset(&capture, 0, sizeof(capture));
  req.key = "bravo";
  req.owner = "owner";
  req.start_after = NULL;
  req.limit = 8U;
  rc = store->query_index_scan(store, &req, capture_scan_row, &capture, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1U);
  assert_string_equal(capture.keys[0], "bravo");
  assert_int_equal(capture.versions[0], 2L);
  assert_false(scan.truncated);
  assert_null(scan.next_start_after);
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&capture, 0, sizeof(capture));
  req.owner = "other-owner";
  rc = store->query_index_scan(store, &req, capture_scan_row, &capture, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 0U);
  assert_false(scan.truncated);
  assert_null(scan.next_start_after);
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&capture, 0, sizeof(capture));
  req.owner = NULL;
  req.start_after = "bravo";
  rc = store->query_index_scan(store, &req, capture_scan_row, &capture, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 0U);
  assert_false(scan.truncated);
  assert_null(scan.next_start_after);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_query_index_range_scans_field_posting_candidates(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts put_opts;
  lc_pouch_put_state_res state_low;
  lc_pouch_put_state_res state_mid;
  lc_pouch_put_state_res state_more;
  lc_pouch_put_state_res state_high;
  lc_pouch_put_state_res state_removed;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_document_range_term range;
  lc_pouch_document_eq_term eq;
  lc_pouch_query_index_scan_req req;
  lc_pouch_query_index_scan_res scan;
  scan_capture rows;
  key_capture keys;
  lc_error error;
  int removed;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-index-range-candidates");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&put_opts, 0, sizeof(put_opts));
  memset(&state_low, 0, sizeof(state_low));
  memset(&state_mid, 0, sizeof(state_mid));
  memset(&state_more, 0, sizeof(state_more));
  memset(&state_high, 0, sizeof(state_high));
  memset(&state_removed, 0, sizeof(state_removed));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  memset(&range, 0, sizeof(range));
  memset(&eq, 0, sizeof(eq));
  memset(&req, 0, sizeof(req));
  memset(&scan, 0, sizeof(scan));
  memset(&rows, 0, sizeof(rows));
  memset(&keys, 0, sizeof(keys));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  put_opts.content_type = "application/json";
  source = source_from_text("{\"score\":1,\"kind\":\"include\"}");
  rc = store->write_state(store, "default", "low", source, &put_opts,
                          &state_low, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("{\"score\":3,\"kind\":\"include\"}");
  rc = store->write_state(store, "default", "mid", source, &put_opts,
                          &state_mid, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("{\"score\":4,\"kind\":\"include\"}");
  rc = store->write_state(store, "default", "omega", source, &put_opts,
                          &state_more, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("{\"score\":5,\"kind\":\"include\"}");
  rc = store->write_state(store, "default", "high", source, &put_opts,
                          &state_high, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("{\"score\":3,\"kind\":\"include\"}");
  rc = store->write_state(store, "default", "removed", source, &put_opts,
                          &state_removed, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  rc = store->remove_state(store, "default", "removed",
                           state_removed.new_state_etag, &removed, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(removed);

  meta.owner = "owner";
  meta.lease_id = "lease-low";
  meta.state_etag = state_low.new_state_etag;
  meta.version = state_low.new_version;
  meta.fencing_token = state_low.new_version;
  rc = store->store_meta(store, "default", "low", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-mid";
  meta.state_etag = state_mid.new_state_etag;
  meta.version = state_mid.new_version;
  meta.fencing_token = state_mid.new_version;
  rc = store->store_meta(store, "default", "mid", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-omega";
  meta.state_etag = state_more.new_state_etag;
  meta.version = state_more.new_version;
  meta.fencing_token = state_more.new_version;
  rc = store->store_meta(store, "default", "omega", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-high";
  meta.state_etag = state_high.new_state_etag;
  meta.version = state_high.new_version;
  meta.fencing_token = state_high.new_version;
  rc = store->store_meta(store, "default", "high", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-removed";
  meta.state_etag = state_removed.new_state_etag;
  meta.version = state_removed.new_version;
  meta.fencing_token = state_removed.new_version;
  rc = store->store_meta(store, "default", "removed", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  range.field = "/score";
  range.gte = "n:+:2:0";
  range.lte = "n:+:4:0";
  req.namespace_name = "default";
  req.limit = 1U;
  req.document_range_terms = &range;
  req.document_range_term_count = 1U;
  rc = store->query_index_scan(store, &req, capture_scan_row, &rows, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(rows.count, 1U);
  assert_int_equal(rows.body_count, 1U);
  assert_string_equal(rows.keys[0], "mid");
  assert_true(scan.truncated);
  assert_string_equal(scan.next_start_after, "mid");
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&rows, 0, sizeof(rows));
  req.start_after = "mid";
  req.limit = 8U;
  rc = store->query_index_scan(store, &req, capture_scan_row, &rows, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(rows.count, 1U);
  assert_int_equal(rows.body_count, 1U);
  assert_string_equal(rows.keys[0], "omega");
  assert_false(scan.truncated);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  req.start_after = NULL;
  req.limit = 8U;
  rc = store->query_index_keys_scan(store, &req, capture_query_key, &keys,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(keys.count, 2U);
  assert_string_equal(keys.keys[0], "mid");
  assert_string_equal(keys.keys[1], "omega");
  assert_false(scan.truncated);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&keys, 0, sizeof(keys));
  memset(&rows, 0, sizeof(rows));
  eq.field = "/kind";
  eq.value = "s:include";
  req.document_eq_terms = &eq;
  req.document_eq_term_count = 1U;

  rc = store->query_index_scan(store, &req, capture_scan_row, &rows, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(rows.count, 2U);
  assert_int_equal(rows.body_count, 2U);
  assert_string_equal(rows.keys[0], "mid");
  assert_string_equal(rows.keys[1], "omega");
  assert_false(scan.truncated);
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->query_index_keys_scan(store, &req, capture_query_key, &keys,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(keys.count, 2U);
  assert_string_equal(keys.keys[0], "mid");
  assert_string_equal(keys.keys[1], "omega");
  assert_false(scan.truncated);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&keys, 0, sizeof(keys));
  eq.field = "/kind";
  eq.value = "s:missing";
  req.document_eq_terms = &eq;
  req.document_eq_term_count = 1U;
  rc = store->query_index_keys_scan(store, &req, capture_query_key, &keys,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(keys.count, 0U);
  assert_false(scan.truncated);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  lc_pouch_put_state_res_cleanup(&allocator, &state_removed);
  lc_pouch_put_state_res_cleanup(&allocator, &state_high);
  lc_pouch_put_state_res_cleanup(&allocator, &state_more);
  lc_pouch_put_state_res_cleanup(&allocator, &state_mid);
  lc_pouch_put_state_res_cleanup(&allocator, &state_low);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void write_query_range_number_state(lc_pouch_allocator *allocator,
                                           lc_pouch_store *store,
                                           const char *key, const char *json,
                                           lc_error *error) {
  lc_source *source;
  lc_pouch_put_state_opts put_opts;
  lc_pouch_put_state_res state;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  int rc;

  memset(&put_opts, 0, sizeof(put_opts));
  memset(&state, 0, sizeof(state));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  put_opts.content_type = "application/json";
  source = source_from_text(json);
  rc = store->write_state(store, "default", key, source, &put_opts, &state,
                          error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  meta.owner = "range-owner";
  meta.lease_id = "range-lease";
  meta.state_etag = state.new_state_etag;
  meta.version = state.new_version;
  meta.fencing_token = state.new_version;
  rc = store->store_meta(store, "default", key, &meta, NULL, &stored, error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(allocator, &stored);
  lc_pouch_put_state_res_cleanup(allocator, &state);
}

static void
test_query_index_range_uses_numeric_order_for_multidigit_values(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_document_range_term range;
  lc_pouch_query_index_scan_req req;
  lc_pouch_query_index_scan_res scan;
  scan_capture rows;
  key_capture keys;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-index-range-numeric-order");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&range, 0, sizeof(range));
  memset(&req, 0, sizeof(req));
  memset(&scan, 0, sizeof(scan));
  memset(&rows, 0, sizeof(rows));
  memset(&keys, 0, sizeof(keys));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  write_query_range_number_state(&allocator, store, "k07", "{\"score\":7}",
                                 &error);
  write_query_range_number_state(&allocator, store, "k08", "{\"score\":8}",
                                 &error);
  write_query_range_number_state(&allocator, store, "k09", "{\"score\":9}",
                                 &error);
  write_query_range_number_state(&allocator, store, "k10", "{\"score\":10}",
                                 &error);
  write_query_range_number_state(&allocator, store, "k11", "{\"score\":11}",
                                 &error);
  write_query_range_number_state(&allocator, store, "k12", "{\"score\":12}",
                                 &error);

  range.field = "/score";
  range.gte = "n:+:8:0";
  range.lte = "n:+:11:0";
  req.namespace_name = "default";
  req.limit = 8U;
  req.document_range_terms = &range;
  req.document_range_term_count = 1U;
  rc = store->query_index_scan(store, &req, capture_scan_row, &rows, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(rows.count, 4U);
  assert_string_equal(rows.keys[0], "k08");
  assert_string_equal(rows.keys[1], "k09");
  assert_string_equal(rows.keys[2], "k10");
  assert_string_equal(rows.keys[3], "k11");
  assert_false(scan.truncated);
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&keys, 0, sizeof(keys));
  rc = store->query_index_keys_scan(store, &req, capture_query_key, &keys,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(keys.count, 4U);
  assert_string_equal(keys.keys[0], "k08");
  assert_string_equal(keys.keys[1], "k09");
  assert_string_equal(keys.keys[2], "k10");
  assert_string_equal(keys.keys[3], "k11");
  assert_false(scan.truncated);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&keys, 0, sizeof(keys));
  range.gte = NULL;
  range.lte = "n:+:9:0";
  rc = store->query_index_keys_scan(store, &req, capture_query_key, &keys,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(keys.count, 3U);
  assert_string_equal(keys.keys[0], "k07");
  assert_string_equal(keys.keys[1], "k08");
  assert_string_equal(keys.keys[2], "k09");
  assert_false(scan.truncated);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_query_index_in_deduplicates_duplicate_values(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  const char *values[3];
  lc_pouch_document_in_term in_term;
  lc_pouch_query_index_scan_req req;
  lc_pouch_query_index_scan_res scan;
  scan_capture rows;
  key_capture keys;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-index-in-duplicate-values");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&in_term, 0, sizeof(in_term));
  memset(&req, 0, sizeof(req));
  memset(&scan, 0, sizeof(scan));
  memset(&rows, 0, sizeof(rows));
  memset(&keys, 0, sizeof(keys));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  write_query_range_number_state(&allocator, store, "both",
                                 "{\"tags\":[\"planning\",\"finance\"]}",
                                 &error);
  write_query_range_number_state(&allocator, store, "finance",
                                 "{\"tags\":[\"finance\"]}", &error);
  write_query_range_number_state(&allocator, store, "ops",
                                 "{\"tags\":[\"ops\"]}", &error);
  write_query_range_number_state(&allocator, store, "planning",
                                 "{\"tags\":[\"planning\"]}", &error);

  values[0] = "s:planning";
  values[1] = "s:finance";
  values[2] = "s:planning";
  in_term.field = "/tags/0";
  in_term.values = values;
  in_term.value_count = 3U;
  req.namespace_name = "default";
  req.limit = 8U;
  req.document_in_terms = &in_term;
  req.document_in_term_count = 1U;

  rc = store->query_index_keys_scan(store, &req, capture_query_key, &keys,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(keys.count, 3U);
  assert_string_equal(keys.keys[0], "both");
  assert_string_equal(keys.keys[1], "finance");
  assert_string_equal(keys.keys[2], "planning");
  assert_false(scan.truncated);
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->query_index_scan(store, &req, capture_scan_row, &rows, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(rows.count, 3U);
  assert_string_equal(rows.keys[0], "both");
  assert_string_equal(rows.keys[1], "finance");
  assert_string_equal(rows.keys[2], "planning");
  assert_false(scan.truncated);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&keys, 0, sizeof(keys));
  memset(&rows, 0, sizeof(rows));
  in_term.field = "/tags/*";

  rc = store->query_index_keys_scan(store, &req, capture_query_key, &keys,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(keys.count, 3U);
  assert_string_equal(keys.keys[0], "both");
  assert_string_equal(keys.keys[1], "finance");
  assert_string_equal(keys.keys[2], "planning");
  assert_false(scan.truncated);
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->query_index_scan(store, &req, capture_scan_row, &rows, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(rows.count, 3U);
  assert_string_equal(rows.keys[0], "both");
  assert_string_equal(rows.keys[1], "finance");
  assert_string_equal(rows.keys[2], "planning");
  assert_false(scan.truncated);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);

  store = NULL;
  memset(&scan, 0, sizeof(scan));
  memset(&keys, 0, sizeof(keys));
  memset(&rows, 0, sizeof(rows));
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  rc = store->query_index_keys_scan(store, &req, capture_query_key, &keys,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(keys.count, 3U);
  assert_string_equal(keys.keys[0], "both");
  assert_string_equal(keys.keys[1], "finance");
  assert_string_equal(keys.keys[2], "planning");
  assert_false(scan.truncated);
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->query_index_scan(store, &req, capture_scan_row, &rows, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(rows.count, 3U);
  assert_string_equal(rows.keys[0], "both");
  assert_string_equal(rows.keys[1], "finance");
  assert_string_equal(rows.keys[2], "planning");
  assert_false(scan.truncated);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_query_index_contains_uses_trigram_posting_candidates(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts put_opts;
  lc_pouch_put_state_res state_alpha;
  lc_pouch_put_state_res state_bravo;
  lc_pouch_put_state_res state_charlie;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_document_contains_term contains;
  lc_pouch_query_index_scan_req req;
  lc_pouch_query_index_scan_res scan;
  scan_capture rows;
  key_capture keys;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-index-contains-trigram");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&put_opts, 0, sizeof(put_opts));
  memset(&state_alpha, 0, sizeof(state_alpha));
  memset(&state_bravo, 0, sizeof(state_bravo));
  memset(&state_charlie, 0, sizeof(state_charlie));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  memset(&contains, 0, sizeof(contains));
  memset(&req, 0, sizeof(req));
  memset(&scan, 0, sizeof(scan));
  memset(&rows, 0, sizeof(rows));
  memset(&keys, 0, sizeof(keys));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  put_opts.content_type = "application/json";
  source = source_from_text("{\"body\":\"xxABCxx\"}");
  rc = store->write_state(store, "default", "alpha", source, &put_opts,
                          &state_alpha, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("{\"body\":\"xxabcxx\"}");
  rc = store->write_state(store, "default", "bravo", source, &put_opts,
                          &state_bravo, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("{\"body\":\"nomatch\"}");
  rc = store->write_state(store, "default", "charlie", source, &put_opts,
                          &state_charlie, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  assert_true(count_query_index_field_values_with_prefix(root, "g:") >= 12U);

  meta.owner = "owner";
  meta.lease_id = "lease-alpha";
  meta.state_etag = state_alpha.new_state_etag;
  meta.version = state_alpha.new_version;
  meta.fencing_token = state_alpha.new_version;
  rc = store->store_meta(store, "default", "alpha", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-bravo";
  meta.state_etag = state_bravo.new_state_etag;
  meta.version = state_bravo.new_version;
  meta.fencing_token = state_bravo.new_version;
  rc = store->store_meta(store, "default", "bravo", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-charlie";
  meta.state_etag = state_charlie.new_state_etag;
  meta.version = state_charlie.new_version;
  meta.fencing_token = state_charlie.new_version;
  rc = store->store_meta(store, "default", "charlie", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  contains.field = "/body";
  contains.value = "ABC";
  contains.ignore_case = 0;
  req.namespace_name = "default";
  req.document_contains_terms = &contains;
  req.document_contains_term_count = 1U;
  rc = store->query_index_scan(store, &req, capture_scan_row, &rows, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(rows.count, 1U);
  assert_string_equal(rows.keys[0], "alpha");
  assert_false(scan.truncated);
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&rows, 0, sizeof(rows));
  contains.ignore_case = 0;
  req.document_contains_terms = NULL;
  req.document_contains_term_count = 0U;
  req.document_or_contains_terms = &contains;
  req.document_or_contains_term_count = 1U;
  rc = store->query_index_scan(store, &req, capture_scan_row, &rows, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(rows.count, 1U);
  assert_string_equal(rows.keys[0], "alpha");
  assert_false(scan.truncated);
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&keys, 0, sizeof(keys));
  contains.ignore_case = 1;
  rc = store->query_index_keys_scan(store, &req, capture_query_key, &keys,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(keys.count, 2U);
  assert_string_equal(keys.keys[0], "alpha");
  assert_string_equal(keys.keys[1], "bravo");
  assert_false(scan.truncated);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  lc_pouch_put_state_res_cleanup(&allocator, &state_charlie);
  lc_pouch_put_state_res_cleanup(&allocator, &state_bravo);
  lc_pouch_put_state_res_cleanup(&allocator, &state_alpha);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_query_index_summary_scan_applies_negative_terms(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts put_opts;
  lc_pouch_put_state_res state_alpha;
  lc_pouch_put_state_res state_bravo;
  lc_pouch_put_state_res state_charlie;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_document_eq_term not_eq;
  lc_pouch_document_exists_term not_exists;
  lc_pouch_query_index_scan_req req;
  lc_pouch_query_index_scan_res scan;
  scan_capture rows;
  key_capture keys;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-index-negative-summary");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&put_opts, 0, sizeof(put_opts));
  memset(&state_alpha, 0, sizeof(state_alpha));
  memset(&state_bravo, 0, sizeof(state_bravo));
  memset(&state_charlie, 0, sizeof(state_charlie));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  memset(&not_eq, 0, sizeof(not_eq));
  memset(&not_exists, 0, sizeof(not_exists));
  memset(&req, 0, sizeof(req));
  memset(&scan, 0, sizeof(scan));
  memset(&rows, 0, sizeof(rows));
  memset(&keys, 0, sizeof(keys));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  put_opts.content_type = "application/json";
  source = source_from_text("{\"kind\":\"allow\"}");
  rc = store->write_state(store, "default", "alpha", source, &put_opts,
                          &state_alpha, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("{\"kind\":\"block\",\"blocked\":true}");
  rc = store->write_state(store, "default", "bravo", source, &put_opts,
                          &state_bravo, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("{\"kind\":\"allow\"}");
  rc = store->write_state(store, "default", "charlie", source, &put_opts,
                          &state_charlie, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  meta.owner = "owner";
  meta.lease_id = "lease-alpha";
  meta.state_etag = state_alpha.new_state_etag;
  meta.version = state_alpha.new_version;
  meta.fencing_token = state_alpha.new_version;
  rc = store->store_meta(store, "default", "alpha", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-bravo";
  meta.state_etag = state_bravo.new_state_etag;
  meta.version = state_bravo.new_version;
  meta.fencing_token = state_bravo.new_version;
  rc = store->store_meta(store, "default", "bravo", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-charlie";
  meta.state_etag = state_charlie.new_state_etag;
  meta.version = state_charlie.new_version;
  meta.fencing_token = state_charlie.new_version;
  rc = store->store_meta(store, "default", "charlie", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  not_eq.field = "/kind";
  not_eq.value = "s:block";
  not_exists.field = "/blocked";
  req.namespace_name = "default";
  req.document_not_eq_terms = &not_eq;
  req.document_not_eq_term_count = 1U;
  req.document_not_exists_terms = &not_exists;
  req.document_not_exists_term_count = 1U;
  req.limit = 1U;
  rc = store->query_index_scan(store, &req, capture_scan_row, &rows, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(rows.count, 1U);
  assert_string_equal(rows.keys[0], "alpha");
  assert_true(scan.truncated);
  assert_string_equal(scan.next_start_after, "alpha");
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&keys, 0, sizeof(keys));
  req.start_after = "alpha";
  req.limit = 8U;
  rc = store->query_index_keys_scan(store, &req, capture_query_key, &keys,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(keys.count, 1U);
  assert_string_equal(keys.keys[0], "charlie");
  assert_false(scan.truncated);
  assert_null(scan.next_start_after);
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  lc_pouch_put_state_res_cleanup(&allocator, &state_charlie);
  lc_pouch_put_state_res_cleanup(&allocator, &state_bravo);
  lc_pouch_put_state_res_cleanup(&allocator, &state_alpha);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_query_index_path_pattern_scan_intersects_positive_terms(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts put_opts;
  lc_pouch_put_state_res state_alpha;
  lc_pouch_put_state_res state_bravo;
  lc_pouch_put_state_res state_charlie;
  lc_pouch_put_state_res state_delta;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_document_exists_term patterns[2];
  lc_pouch_query_index_scan_req req;
  lc_pouch_query_index_scan_res scan;
  scan_capture rows;
  key_capture keys;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-index-path-pattern-and");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&put_opts, 0, sizeof(put_opts));
  memset(&state_alpha, 0, sizeof(state_alpha));
  memset(&state_bravo, 0, sizeof(state_bravo));
  memset(&state_charlie, 0, sizeof(state_charlie));
  memset(&state_delta, 0, sizeof(state_delta));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  memset(&patterns, 0, sizeof(patterns));
  memset(&req, 0, sizeof(req));
  memset(&scan, 0, sizeof(scan));
  memset(&rows, 0, sizeof(rows));
  memset(&keys, 0, sizeof(keys));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  put_opts.content_type = "application/json";
  source = source_from_text("{\"box\":{\"a\":1}}");
  rc = store->write_state(store, "default", "alpha", source, &put_opts,
                          &state_alpha, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("{\"rack\":{\"b\":2}}");
  rc = store->write_state(store, "default", "bravo", source, &put_opts,
                          &state_bravo, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("{\"box\":{\"a\":1},\"rack\":{\"b\":2}}");
  rc = store->write_state(store, "default", "charlie", source, &put_opts,
                          &state_charlie, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("{\"box\":{\"c\":3},\"rack\":{\"d\":4}}");
  rc = store->write_state(store, "default", "delta", source, &put_opts,
                          &state_delta, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  meta.owner = "owner";
  meta.lease_id = "lease-alpha";
  meta.state_etag = state_alpha.new_state_etag;
  meta.version = state_alpha.new_version;
  meta.fencing_token = state_alpha.new_version;
  rc = store->store_meta(store, "default", "alpha", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-bravo";
  meta.state_etag = state_bravo.new_state_etag;
  meta.version = state_bravo.new_version;
  meta.fencing_token = state_bravo.new_version;
  rc = store->store_meta(store, "default", "bravo", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-charlie";
  meta.state_etag = state_charlie.new_state_etag;
  meta.version = state_charlie.new_version;
  meta.fencing_token = state_charlie.new_version;
  rc = store->store_meta(store, "default", "charlie", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-delta";
  meta.state_etag = state_delta.new_state_etag;
  meta.version = state_delta.new_version;
  meta.fencing_token = state_delta.new_version;
  rc = store->store_meta(store, "default", "delta", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  patterns[0].field = "/box/*";
  patterns[1].field = "/rack/*";
  req.namespace_name = "default";
  req.document_exists_path_patterns = patterns;
  req.document_exists_path_pattern_count = 2U;
  req.limit = 1U;
  rc = store->query_index_scan(store, &req, capture_scan_row, &rows, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(rows.count, 1U);
  assert_string_equal(rows.keys[0], "charlie");
  assert_true(scan.truncated);
  assert_string_equal(scan.next_start_after, "charlie");
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&keys, 0, sizeof(keys));
  req.start_after = "charlie";
  req.limit = 8U;
  rc = store->query_index_keys_scan(store, &req, capture_query_key, &keys,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(keys.count, 1U);
  assert_string_equal(keys.keys[0], "delta");
  assert_false(scan.truncated);
  assert_null(scan.next_start_after);
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&keys, 0, sizeof(keys));
  memset(&req, 0, sizeof(req));
  patterns[0].field = "/box/**";
  patterns[1].field = "/rack/*";
  req.namespace_name = "default";
  req.document_exists_path_patterns = patterns;
  req.document_exists_path_pattern_count = 2U;
  req.limit = 8U;
  rc = store->query_index_keys_scan(store, &req, capture_query_key, &keys,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(keys.count, 2U);
  assert_string_equal(keys.keys[0], "charlie");
  assert_string_equal(keys.keys[1], "delta");
  assert_false(scan.truncated);
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  lc_pouch_put_state_res_cleanup(&allocator, &state_delta);
  lc_pouch_put_state_res_cleanup(&allocator, &state_charlie);
  lc_pouch_put_state_res_cleanup(&allocator, &state_bravo);
  lc_pouch_put_state_res_cleanup(&allocator, &state_alpha);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_query_index_keys_scan_avoids_metadata_row_copies(void **state) {
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

  meta.owner = "owner";
  meta.lease_id = "lease-key-only-allocation-sentinel";
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

  tracked.fail_malloc_size = strlen("lease-key-only-allocation-sentinel") + 1U;
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

  memset(&capture, 0, sizeof(capture));
  req.key = "bravo";
  req.owner = "owner";
  req.start_after = NULL;
  req.limit = 8U;
  rc = store->query_index_keys_scan(store, &req, capture_query_key, &capture,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1U);
  assert_string_equal(capture.keys[0], "bravo");
  assert_false(scan.truncated);
  assert_null(scan.next_start_after);
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&capture, 0, sizeof(capture));
  req.owner = "other-owner";
  rc = store->query_index_keys_scan(store, &req, capture_query_key, &capture,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 0U);
  assert_false(scan.truncated);
  assert_null(scan.next_start_after);
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&capture, 0, sizeof(capture));
  req.owner = NULL;
  req.start_after = "bravo";
  rc = store->query_index_keys_scan(store, &req, capture_query_key, &capture,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 0U);
  assert_false(scan.truncated);
  assert_null(scan.next_start_after);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_query_owner_index_scans_candidates_and_replays(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_query_owner_scan_req req;
  lc_pouch_query_index_scan_res scan;
  scan_capture rows;
  key_capture keys;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-owner-index");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  memset(&req, 0, sizeof(req));
  memset(&scan, 0, sizeof(scan));
  memset(&rows, 0, sizeof(rows));
  memset(&keys, 0, sizeof(keys));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(store->query_owner_scan);
  assert_non_null(store->query_owner_keys_scan);

  meta.owner = "owner-a";
  meta.lease_id = "lease-alpha";
  meta.state_etag = "state-alpha";
  meta.version = 1L;
  rc = store->store_meta(store, "default", "alpha", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.owner = "owner-b";
  meta.lease_id = "lease-bravo";
  meta.state_etag = "state-bravo";
  meta.version = 2L;
  rc = store->store_meta(store, "default", "bravo", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.owner = "owner-a";
  meta.lease_id = "lease-charlie";
  meta.state_etag = "state-charlie";
  meta.version = 3L;
  rc = store->store_meta(store, "default", "charlie", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.owner = "owner-a";
  meta.lease_id = "lease-hidden";
  meta.state_etag = "state-hidden";
  meta.version = 4L;
  meta.has_query_hidden = 1;
  meta.query_hidden = 1;
  rc = store->store_meta(store, "default", "hidden", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.owner = "owner-a";
  meta.lease_id = "lease-deleted";
  meta.state_etag = "state-deleted";
  meta.version = 5L;
  meta.has_query_hidden = 0;
  meta.query_hidden = 0;
  rc = store->store_meta(store, "default", "deleted", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  rc = store->delete_meta(store, "default", "deleted", stored.etag, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.owner = "owner-a";
  meta.lease_id = "lease-other";
  meta.state_etag = "state-other";
  meta.version = 6L;
  rc = store->store_meta(store, "other", "aardvark", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  req.namespace_name = "default";
  req.owner = "owner-a";
  req.limit = 1U;
  rc = store->query_owner_scan(store, &req, capture_scan_row, &rows, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(rows.count, 1U);
  assert_string_equal(rows.keys[0], "alpha");
  assert_string_equal(rows.owners[0], "owner-a");
  assert_true(scan.truncated);
  assert_string_equal(scan.next_start_after, "alpha");
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&keys, 0, sizeof(keys));
  req.start_after = "alpha";
  req.limit = 8U;
  rc = store->query_owner_keys_scan(store, &req, capture_query_key, &keys,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(keys.count, 1U);
  assert_string_equal(keys.keys[0], "charlie");
  assert_false(scan.truncated);
  assert_null(scan.next_start_after);
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  meta.owner = "owner-b";
  meta.lease_id = "lease-charlie-moved";
  meta.state_etag = "state-charlie-moved";
  meta.version = 7L;
  rc = store->store_meta(store, "default", "charlie", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  memset(&keys, 0, sizeof(keys));
  req.start_after = NULL;
  req.limit = 8U;
  rc = store->query_owner_keys_scan(store, &req, capture_query_key, &keys,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(keys.count, 1U);
  assert_string_equal(keys.keys[0], "alpha");
  assert_false(scan.truncated);
  assert_null(scan.next_start_after);
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&keys, 0, sizeof(keys));
  req.owner = "owner-b";
  rc = store->query_owner_keys_scan(store, &req, capture_query_key, &keys,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(keys.count, 2U);
  assert_string_equal(keys.keys[0], "bravo");
  assert_string_equal(keys.keys[1], "charlie");
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_query_index_scans_exclude_removed_state(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts put_opts;
  lc_pouch_put_state_res alpha_state;
  lc_pouch_put_state_res bravo_state;
  lc_pouch_put_state_res removed_state;
  lc_pouch_put_state_res noise_state;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_query_index_scan_req index_req;
  lc_pouch_query_owner_scan_req owner_req;
  lc_pouch_query_index_scan_res scan;
  scan_capture rows;
  key_capture keys;
  int removed;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-index-exclude-removed-state");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&put_opts, 0, sizeof(put_opts));
  memset(&alpha_state, 0, sizeof(alpha_state));
  memset(&bravo_state, 0, sizeof(bravo_state));
  memset(&removed_state, 0, sizeof(removed_state));
  memset(&noise_state, 0, sizeof(noise_state));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  memset(&index_req, 0, sizeof(index_req));
  memset(&owner_req, 0, sizeof(owner_req));
  memset(&scan, 0, sizeof(scan));
  memset(&rows, 0, sizeof(rows));
  memset(&keys, 0, sizeof(keys));
  store = NULL;
  source = NULL;
  removed = 0;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  put_opts.content_type = "application/json";
  source = source_from_text("{\"live\":\"alpha\"}");
  rc = store->write_state(store, "default", "alpha", source, &put_opts,
                          &alpha_state, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  source = source_from_text("{\"live\":\"bravo\"}");
  rc = store->write_state(store, "default", "bravo", source, &put_opts,
                          &bravo_state, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  source = source_from_text("{\"live\":\"removed\"}");
  rc = store->write_state(store, "default", "removed", source, &put_opts,
                          &removed_state, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  source = source_from_text("{\"live\":\"noise\"}");
  rc = store->write_state(store, "default", "noise", source, &put_opts,
                          &noise_state, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  meta.owner = "target-owner";
  meta.lease_id = "lease-alpha";
  meta.state_etag = alpha_state.new_state_etag;
  meta.version = alpha_state.new_version;
  meta.fencing_token = alpha_state.new_version;
  rc = store->store_meta(store, "default", "alpha", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-bravo";
  meta.state_etag = bravo_state.new_state_etag;
  meta.version = bravo_state.new_version;
  meta.fencing_token = bravo_state.new_version;
  rc = store->store_meta(store, "default", "bravo", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-removed";
  meta.state_etag = removed_state.new_state_etag;
  meta.version = removed_state.new_version;
  meta.fencing_token = removed_state.new_version;
  rc = store->store_meta(store, "default", "removed", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.owner = "noise-owner";
  meta.lease_id = "lease-noise";
  meta.state_etag = noise_state.new_state_etag;
  meta.version = noise_state.new_version;
  meta.fencing_token = noise_state.new_version;
  rc = store->store_meta(store, "default", "noise", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  rc = store->remove_state(store, "default", "removed",
                           removed_state.new_state_etag, &removed, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(removed);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  index_req.namespace_name = "default";
  rc = store->query_index_scan(store, &index_req, capture_scan_row, &rows,
                               &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(rows.count, 3U);
  assert_string_equal(rows.keys[0], "alpha");
  assert_string_equal(rows.keys[1], "bravo");
  assert_string_equal(rows.keys[2], "noise");
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&keys, 0, sizeof(keys));
  index_req.owner = "target-owner";
  rc = store->query_index_keys_scan(store, &index_req, capture_query_key, &keys,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(keys.count, 2U);
  assert_string_equal(keys.keys[0], "alpha");
  assert_string_equal(keys.keys[1], "bravo");
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&rows, 0, sizeof(rows));
  owner_req.namespace_name = "default";
  owner_req.owner = "target-owner";
  rc = store->query_owner_scan(store, &owner_req, capture_scan_row, &rows,
                               &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(rows.count, 2U);
  assert_string_equal(rows.keys[0], "alpha");
  assert_string_equal(rows.keys[1], "bravo");
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&keys, 0, sizeof(keys));
  rc = store->query_owner_keys_scan(store, &owner_req, capture_query_key, &keys,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(keys.count, 2U);
  assert_string_equal(keys.keys[0], "alpha");
  assert_string_equal(keys.keys[1], "bravo");
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  lc_pouch_put_state_res_cleanup(&allocator, &noise_state);
  lc_pouch_put_state_res_cleanup(&allocator, &removed_state);
  lc_pouch_put_state_res_cleanup(&allocator, &bravo_state);
  lc_pouch_put_state_res_cleanup(&allocator, &alpha_state);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_query_owner_index_paginates_across_removed_state(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts put_opts;
  lc_pouch_put_state_res alpha_state;
  lc_pouch_put_state_res bravo_state;
  lc_pouch_put_state_res charlie_state;
  lc_pouch_put_state_res delta_state;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_query_owner_scan_req req;
  lc_pouch_query_index_scan_res scan;
  scan_capture rows;
  key_capture keys;
  int removed;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-owner-page-removed-state");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&put_opts, 0, sizeof(put_opts));
  memset(&alpha_state, 0, sizeof(alpha_state));
  memset(&bravo_state, 0, sizeof(bravo_state));
  memset(&charlie_state, 0, sizeof(charlie_state));
  memset(&delta_state, 0, sizeof(delta_state));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  memset(&req, 0, sizeof(req));
  memset(&scan, 0, sizeof(scan));
  memset(&rows, 0, sizeof(rows));
  memset(&keys, 0, sizeof(keys));
  store = NULL;
  source = NULL;
  removed = 0;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  put_opts.content_type = "application/json";
  source = source_from_text("{\"live\":\"alpha\"}");
  rc = store->write_state(store, "default", "alpha", source, &put_opts,
                          &alpha_state, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  source = source_from_text("{\"live\":\"bravo\"}");
  rc = store->write_state(store, "default", "bravo", source, &put_opts,
                          &bravo_state, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  source = source_from_text("{\"live\":\"charlie\"}");
  rc = store->write_state(store, "default", "charlie", source, &put_opts,
                          &charlie_state, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  source = source_from_text("{\"live\":\"delta\"}");
  rc = store->write_state(store, "default", "delta", source, &put_opts,
                          &delta_state, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);

  meta.owner = "page-owner";
  meta.lease_id = "lease-alpha";
  meta.state_etag = alpha_state.new_state_etag;
  meta.version = alpha_state.new_version;
  meta.fencing_token = alpha_state.new_version;
  rc = store->store_meta(store, "default", "alpha", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-bravo";
  meta.state_etag = bravo_state.new_state_etag;
  meta.version = bravo_state.new_version;
  meta.fencing_token = bravo_state.new_version;
  rc = store->store_meta(store, "default", "bravo", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-charlie";
  meta.state_etag = charlie_state.new_state_etag;
  meta.version = charlie_state.new_version;
  meta.fencing_token = charlie_state.new_version;
  rc = store->store_meta(store, "default", "charlie", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-delta";
  meta.state_etag = delta_state.new_state_etag;
  meta.version = delta_state.new_version;
  meta.fencing_token = delta_state.new_version;
  rc = store->store_meta(store, "default", "delta", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  rc = store->remove_state(store, "default", "bravo",
                           bravo_state.new_state_etag, &removed, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(removed);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  req.namespace_name = "default";
  req.owner = "page-owner";
  req.limit = 1U;
  rc = store->query_owner_scan(store, &req, capture_scan_row, &rows, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(rows.count, 1U);
  assert_string_equal(rows.keys[0], "alpha");
  assert_true(scan.truncated);
  assert_string_equal(scan.next_start_after, "alpha");
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&rows, 0, sizeof(rows));
  req.start_after = "alpha";
  rc = store->query_owner_scan(store, &req, capture_scan_row, &rows, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(rows.count, 1U);
  assert_string_equal(rows.keys[0], "charlie");
  assert_true(scan.truncated);
  assert_string_equal(scan.next_start_after, "charlie");
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&rows, 0, sizeof(rows));
  req.start_after = "charlie";
  rc = store->query_owner_scan(store, &req, capture_scan_row, &rows, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(rows.count, 1U);
  assert_string_equal(rows.keys[0], "delta");
  assert_false(scan.truncated);
  assert_null(scan.next_start_after);
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  req.start_after = NULL;
  rc = store->query_owner_keys_scan(store, &req, capture_query_key, &keys,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(keys.count, 1U);
  assert_string_equal(keys.keys[0], "alpha");
  assert_true(scan.truncated);
  assert_string_equal(scan.next_start_after, "alpha");
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&keys, 0, sizeof(keys));
  req.start_after = "alpha";
  rc = store->query_owner_keys_scan(store, &req, capture_query_key, &keys,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(keys.count, 1U);
  assert_string_equal(keys.keys[0], "charlie");
  assert_true(scan.truncated);
  assert_string_equal(scan.next_start_after, "charlie");
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&keys, 0, sizeof(keys));
  req.start_after = "charlie";
  rc = store->query_owner_keys_scan(store, &req, capture_query_key, &keys,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(keys.count, 1U);
  assert_string_equal(keys.keys[0], "delta");
  assert_false(scan.truncated);
  assert_null(scan.next_start_after);
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  lc_pouch_put_state_res_cleanup(&allocator, &delta_state);
  lc_pouch_put_state_res_cleanup(&allocator, &charlie_state);
  lc_pouch_put_state_res_cleanup(&allocator, &bravo_state);
  lc_pouch_put_state_res_cleanup(&allocator, &alpha_state);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_query_index_scans_refresh_stale_reader_before_sidecar(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *reader_docs;
  lc_pouch_store *reader_keys;
  lc_pouch_store *writer;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_query_index_scan_req req;
  lc_pouch_query_index_scan_res scan;
  scan_capture doc_capture;
  key_capture key_capture_rows;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-index-stale-reader-refresh");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  memset(&req, 0, sizeof(req));
  memset(&scan, 0, sizeof(scan));
  memset(&doc_capture, 0, sizeof(doc_capture));
  memset(&key_capture_rows, 0, sizeof(key_capture_rows));
  reader_docs = NULL;
  reader_keys = NULL;
  writer = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &reader_docs, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_disk_open(root, &allocator, &reader_keys, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_disk_open(root, &allocator, &writer, &error);
  assert_int_equal(rc, LC_OK);

  req.namespace_name = "default";
  rc = reader_docs->query_index_scan(reader_docs, &req, capture_scan_row,
                                     &doc_capture, &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(doc_capture.count, 0U);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = reader_keys->query_index_keys_scan(reader_keys, &req, capture_query_key,
                                          &key_capture_rows, &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(key_capture_rows.count, 0U);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  meta.owner = "writer";
  meta.lease_id = "lease-alpha";
  meta.state_etag = "state-alpha";
  meta.version = 1L;
  rc = writer->store_meta(writer, "default", "alpha", &meta, NULL, &stored,
                          &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-bravo";
  meta.state_etag = "state-bravo";
  meta.version = 2L;
  rc = writer->store_meta(writer, "default", "bravo", &meta, NULL, &stored,
                          &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  memset(&doc_capture, 0, sizeof(doc_capture));
  rc = reader_docs->query_index_scan(reader_docs, &req, capture_scan_row,
                                     &doc_capture, &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(doc_capture.count, 2U);
  assert_string_equal(doc_capture.keys[0], "alpha");
  assert_string_equal(doc_capture.keys[1], "bravo");
  assert_int_equal(doc_capture.versions[0], 1L);
  assert_int_equal(doc_capture.versions[1], 2L);
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&key_capture_rows, 0, sizeof(key_capture_rows));
  rc = reader_keys->query_index_keys_scan(reader_keys, &req, capture_query_key,
                                          &key_capture_rows, &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(key_capture_rows.count, 2U);
  assert_string_equal(key_capture_rows.keys[0], "alpha");
  assert_string_equal(key_capture_rows.keys[1], "bravo");
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = writer->close(writer, &error);
  assert_int_equal(rc, LC_OK);
  rc = reader_keys->close(reader_keys, &error);
  assert_int_equal(rc, LC_OK);
  rc = reader_docs->close(reader_docs, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_query_index_projection_replays_updates_and_deletes(void **state) {
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

static void
test_metadata_update_allocation_failure_preserves_indexes(void **state) {
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
  meta.updated_at_unix = 101L;
  rc = store->store_meta(store, "default", "lease-key", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);

  meta.owner = "owner-allocation-failure-sentinel";
  meta.lease_id = "lease-new";
  meta.state_etag = "state-new";
  meta.version = 2L;
  meta.updated_at_unix = 202L;
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
  assert_int_equal(loaded.meta.updated_at_unix, 101L);
  lc_pouch_meta_record_cleanup(&allocator, &loaded);

  req.namespace_name = "default";
  rc = store->query_index_scan(store, &req, capture_scan_row, &capture, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1U);
  assert_string_equal(capture.keys[0], "lease-key");
  assert_int_equal(capture.versions[0], 1L);
  assert_int_equal(capture.updated_at_unix[0], 101L);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  lc_pouch_store_meta_res_cleanup(&allocator, &stored);
  lc_pouch_store_meta_res_cleanup(&allocator, &updated);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_retention_sweep_deletes_expired_metadata_and_state(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_put_state_res state_res;
  lc_pouch_state_info state_info;
  lc_pouch_meta_record loaded;
  lc_pouch_retention_sweep_req req;
  lc_pouch_retention_sweep_res sweep;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "retention-sweep");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  memset(&state_res, 0, sizeof(state_res));
  memset(&state_info, 0, sizeof(state_info));
  memset(&loaded, 0, sizeof(loaded));
  memset(&req, 0, sizeof(req));
  memset(&sweep, 0, sizeof(sweep));
  store = NULL;
  read_body = NULL;
  text = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(store->retention_sweep);

  meta.owner = "retention-owner";
  meta.lease_id = "lease-expired";
  meta.state_etag = "state-expired";
  meta.version = 10L;
  meta.updated_at_unix = 100L;
  rc = store->store_meta(store, "default", "expired", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);
  source = source_from_text("expired-state");
  rc = store->write_state(store, "default", "expired", source, NULL, &state_res,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&allocator, &state_res);

  meta.lease_id = "lease-current";
  meta.state_etag = "state-current";
  meta.version = 20L;
  meta.updated_at_unix = 5000L;
  rc = store->store_meta(store, "default", "current", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);
  source = source_from_text("current-state");
  rc = store->write_state(store, "default", "current", source, NULL, &state_res,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&allocator, &state_res);

  meta.lease_id = "lease-orphan";
  meta.state_etag = "state-orphan";
  meta.version = 30L;
  meta.updated_at_unix = 150L;
  rc = store->store_meta(store, "default", "orphan", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  req.updated_before_unix = 1000L;
  rc = store->retention_sweep(store, &req, &sweep, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(sweep.scanned_metadata, 3UL);
  assert_int_equal(sweep.expired_metadata, 2UL);
  assert_int_equal(sweep.deleted_metadata, 2UL);
  assert_int_equal(sweep.deleted_state, 1UL);
  assert_int_equal(sweep.failed_keys, 0UL);
  assert_int_equal(count_namespace_segment_records_of_type(
                       root, "default", TEST_POUCH_RECORD_META_REMOVE),
                   2U);
  assert_int_equal(count_namespace_segment_records_of_type(
                       root, "default", TEST_POUCH_RECORD_STATE_REMOVE),
                   1U);

  rc = store->load_meta(store, "default", "expired", &loaded, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(loaded.found);
  rc = store->read_state(store, "default", "expired", &read_body, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_true(state_info.no_content);
  assert_null(read_body);
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  rc = store->load_meta(store, "default", "orphan", &loaded, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(loaded.found);

  rc = store->load_meta(store, "default", "current", &loaded, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(loaded.found);
  assert_int_equal(loaded.meta.updated_at_unix, 5000L);
  lc_pouch_meta_record_cleanup(&allocator, &loaded);
  rc = store->read_state(store, "default", "current", &read_body, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_false(state_info.no_content);
  assert_non_null(read_body);
  text = read_source_text(read_body);
  assert_string_equal(text, "current-state");
  free(text);
  text = NULL;
  lc_source_close(read_body);
  read_body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  memset(&sweep, 0, sizeof(sweep));
  rc = store->retention_sweep(store, &req, &sweep, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(sweep.scanned_metadata, 1UL);
  assert_int_equal(sweep.expired_metadata, 0UL);
  assert_int_equal(sweep.deleted_metadata, 0UL);
  assert_int_equal(sweep.deleted_state, 0UL);
  assert_int_equal(sweep.failed_keys, 0UL);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  memset(&sweep, 0, sizeof(sweep));
  rc = store->retention_sweep(store, &req, &sweep, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(sweep.scanned_metadata, 1UL);
  assert_int_equal(sweep.expired_metadata, 0UL);
  assert_int_equal(sweep.deleted_metadata, 0UL);
  assert_int_equal(sweep.deleted_state, 0UL);
  assert_int_equal(sweep.failed_keys, 0UL);
  assert_int_equal(count_namespace_segment_records_of_type(
                       root, "default", TEST_POUCH_RECORD_META_REMOVE),
                   2U);
  assert_int_equal(count_namespace_segment_records_of_type(
                       root, "default", TEST_POUCH_RECORD_STATE_REMOVE),
                   1U);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_retention_sweep_keeps_metadata_when_state_delete_fails(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_put_state_res state_res;
  lc_pouch_state_info state_info;
  lc_pouch_meta_record loaded;
  lc_pouch_retention_sweep_req req;
  lc_pouch_retention_sweep_res sweep;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "retention-sweep-state-failure");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  memset(&state_res, 0, sizeof(state_res));
  memset(&state_info, 0, sizeof(state_info));
  memset(&loaded, 0, sizeof(loaded));
  memset(&req, 0, sizeof(req));
  memset(&sweep, 0, sizeof(sweep));
  store = NULL;
  read_body = NULL;
  text = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  meta.owner = "retention-owner";
  meta.lease_id = "lease-expired";
  meta.state_etag = "state-expired";
  meta.version = 10L;
  meta.updated_at_unix = 100L;
  rc = store->store_meta(store, "default", "expired", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  source = source_from_text("expired-state");
  rc = store->write_state(store, "default", "expired", source, NULL, &state_res,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&allocator, &state_res);

  req.updated_before_unix = 50L;
  rc = store->retention_sweep(store, &req, &sweep, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(sweep.scanned_metadata, 1UL);
  assert_int_equal(sweep.expired_metadata, 0UL);
  assert_int_equal(sweep.deleted_metadata, 0UL);
  assert_int_equal(sweep.deleted_state, 0UL);
  assert_int_equal(sweep.failed_keys, 0UL);
  memset(&sweep, 0, sizeof(sweep));

  req.updated_before_unix = 1000L;
  tracked.fail_malloc_size = strlen("e3b0c44298fc1c149afbf4c8996fb92427ae41e"
                                    "4649b934ca495991b7852b855") +
                             1U;
  tracked.fail_malloc_after_calls = tracked.malloc_calls + 107U;
  rc = store->retention_sweep(store, &req, &sweep, &error);
  if (rc == LC_OK) {
    assert_int_equal(sweep.scanned_metadata, 1UL);
    assert_int_equal(sweep.expired_metadata, 1UL);
    assert_int_equal(sweep.deleted_metadata, 0UL);
    assert_int_equal(sweep.deleted_state, 0UL);
    assert_int_equal(sweep.failed_keys, 1UL);
  } else {
    assert_int_equal(rc, LC_ERR_NOMEM);
    lc_error_cleanup(&error);
  }
  assert_int_equal(count_namespace_segment_records_of_type(
                       root, "default", TEST_POUCH_RECORD_META_REMOVE),
                   0U);
  assert_int_equal(count_namespace_segment_records_of_type(
                       root, "default", TEST_POUCH_RECORD_STATE_REMOVE),
                   0U);
  tracked.fail_malloc_size = 0U;
  tracked.fail_malloc_after_calls = 0U;
  rc = store->load_meta(store, "default", "expired", &loaded, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(loaded.found);
  lc_pouch_meta_record_cleanup(&allocator, &loaded);
  rc = store->read_state(store, "default", "expired", &read_body, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_false(state_info.no_content);
  assert_non_null(read_body);
  text = read_source_text(read_body);
  assert_string_equal(text, "expired-state");
  free(text);
  text = NULL;
  lc_source_close(read_body);
  read_body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  memset(&sweep, 0, sizeof(sweep));
  rc = store->retention_sweep(store, &req, &sweep, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(sweep.scanned_metadata, 1UL);
  assert_int_equal(sweep.expired_metadata, 1UL);
  assert_int_equal(sweep.deleted_metadata, 1UL);
  assert_int_equal(sweep.deleted_state, 1UL);
  assert_int_equal(sweep.failed_keys, 0UL);
  assert_int_equal(count_namespace_segment_records_of_type(
                       root, "default", TEST_POUCH_RECORD_META_REMOVE),
                   1U);
  assert_int_equal(count_namespace_segment_records_of_type(
                       root, "default", TEST_POUCH_RECORD_STATE_REMOVE),
                   1U);

  rc = store->load_meta(store, "default", "expired", &loaded, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(loaded.found);
  rc = store->read_state(store, "default", "expired", &read_body, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_true(state_info.no_content);
  assert_null(read_body);
  lc_pouch_state_info_cleanup(&allocator, &state_info);

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
  assert_int_equal(test_query_index_size(root), original_query_index_size);
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

static void
test_query_index_keys_recovers_from_missing_legacy_store_log(void **state) {
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
  assert_int_equal(capture.count, 2U);
  assert_string_equal(capture.keys[0], "alpha");
  assert_string_equal(capture.keys[1], "bravo");
  assert_false(scan.truncated);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_query_index_rebuilds_field_postings_from_segments(void **state) {
  char root[256];
  char index_path[512];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts put_opts;
  lc_pouch_put_state_res alpha_state;
  lc_pouch_put_state_res beta_state;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_document_eq_term term;
  lc_pouch_query_index_scan_req req;
  lc_pouch_query_index_scan_res scan;
  scan_capture rows;
  key_capture keys;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-index-field-rebuild-segments");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&put_opts, 0, sizeof(put_opts));
  memset(&alpha_state, 0, sizeof(alpha_state));
  memset(&beta_state, 0, sizeof(beta_state));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  memset(&term, 0, sizeof(term));
  memset(&req, 0, sizeof(req));
  memset(&scan, 0, sizeof(scan));
  memset(&rows, 0, sizeof(rows));
  memset(&keys, 0, sizeof(keys));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  put_opts.content_type = "application/json";
  source = source_from_text("{\"value\":\"alpha\"}");
  rc = store->write_state(store, "default", "alpha", source, &put_opts,
                          &alpha_state, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("{\"value\":\"beta\"}");
  rc = store->write_state(store, "default", "beta", source, &put_opts,
                          &beta_state, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  meta.owner = "owner";
  meta.lease_id = "lease-alpha";
  meta.state_etag = alpha_state.new_state_etag;
  meta.version = alpha_state.new_version;
  meta.fencing_token = alpha_state.new_version;
  rc = store->store_meta(store, "default", "alpha", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-beta";
  meta.state_etag = beta_state.new_state_etag;
  meta.version = beta_state.new_version;
  meta.fencing_token = beta_state.new_version;
  rc =
      store->store_meta(store, "default", "beta", &meta, NULL, &stored, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  truncate_store_log(root);
  test_query_index_path(root, index_path, sizeof(index_path));
  assert_int_equal(unlink(index_path), 0);

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  term.field = "/value";
  term.value = "s:alpha";
  req.namespace_name = "default";
  req.document_eq_terms = &term;
  req.document_eq_term_count = 1U;

  rc = store->query_index_scan(store, &req, capture_scan_row, &rows, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(rows.count, 1U);
  assert_string_equal(rows.keys[0], "alpha");
  assert_int_equal(rows.versions[0], alpha_state.new_version);
  assert_false(scan.truncated);
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->query_index_keys_scan(store, &req, capture_query_key, &keys,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(keys.count, 1U);
  assert_string_equal(keys.keys[0], "alpha");
  assert_false(scan.truncated);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  lc_pouch_put_state_res_cleanup(&allocator, &beta_state);
  lc_pouch_put_state_res_cleanup(&allocator, &alpha_state);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_query_index_keys_recovers_from_corrupt_sidecar_tail(void **state) {
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
  assert_int_equal(test_query_index_size(root), original_query_index_size);
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

static void test_query_index_keys_recreates_missing_sidecar(void **state) {
  char root[256];
  char index_path[512];
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
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-index-sidecar-missing");
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
  assert_true(test_query_index_size(root) > 0);

  test_query_index_path(root, index_path, sizeof(index_path));
  assert_int_equal(unlink(index_path), 0);

  req.namespace_name = "default";
  rc = store->query_index_scan(store, &req, capture_scan_row, &row_capture,
                               &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(row_capture.count, 1U);
  assert_string_equal(row_capture.keys[0], "alpha");
  assert_int_equal(row_capture.versions[0], 1L);
  assert_false(scan.truncated);
  assert_true(test_query_index_size(root) > 0);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->query_index_keys_scan(store, &req, capture_query_key, &capture,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1U);
  assert_string_equal(capture.keys[0], "alpha");
  assert_false(scan.truncated);
  assert_true(test_query_index_size(root) > 0);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_query_index_keys_rebuilds_future_sidecar_version(void **state) {
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
  test_root_path(root, sizeof(root), "query-index-sidecar-future-version");
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

  meta.lease_id = "lease-bravo";
  meta.state_etag = "state-bravo";
  meta.version = 2L;
  rc = store->store_meta(store, "default", "bravo", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);
  original_query_index_size = test_query_index_size(root);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  set_first_query_index_match_record_version(root, "bravo", 99UL);

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  req.namespace_name = "default";
  rc = store->query_index_scan(store, &req, capture_scan_row, &row_capture,
                               &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(row_capture.count, 2U);
  assert_string_equal(row_capture.keys[0], "alpha");
  assert_int_equal(row_capture.versions[0], 1L);
  assert_string_equal(row_capture.keys[1], "bravo");
  assert_int_equal(row_capture.versions[1], 2L);
  assert_false(scan.truncated);
  assert_int_equal(test_query_index_size(root), original_query_index_size);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->query_index_keys_scan(store, &req, capture_query_key, &capture,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 2U);
  assert_string_equal(capture.keys[0], "alpha");
  assert_string_equal(capture.keys[1], "bravo");
  assert_false(scan.truncated);
  assert_int_equal(test_query_index_size(root), original_query_index_size);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_query_index_rebuilds_future_format_version(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts put_opts;
  lc_pouch_put_state_res alpha_state;
  lc_pouch_put_state_res bravo_state;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_document_eq_term term;
  lc_pouch_query_index_scan_req req;
  lc_pouch_query_index_scan_res scan;
  scan_capture row_capture;
  key_capture key_rows;
  lc_error error;
  off_t original_query_index_size;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-index-format-future-version");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&put_opts, 0, sizeof(put_opts));
  memset(&alpha_state, 0, sizeof(alpha_state));
  memset(&bravo_state, 0, sizeof(bravo_state));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  memset(&term, 0, sizeof(term));
  memset(&req, 0, sizeof(req));
  memset(&scan, 0, sizeof(scan));
  memset(&row_capture, 0, sizeof(row_capture));
  memset(&key_rows, 0, sizeof(key_rows));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  put_opts.content_type = "application/json";
  source = source_from_text("{\"value\":\"alpha\"}");
  rc = store->write_state(store, "default", "alpha", source, &put_opts,
                          &alpha_state, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("{\"value\":\"bravo\"}");
  rc = store->write_state(store, "default", "bravo", source, &put_opts,
                          &bravo_state, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  meta.owner = "owner";
  meta.lease_id = "lease-alpha";
  meta.state_etag = alpha_state.new_state_etag;
  meta.version = alpha_state.new_version;
  meta.fencing_token = alpha_state.new_version;
  rc = store->store_meta(store, "default", "alpha", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  meta.lease_id = "lease-bravo";
  meta.state_etag = bravo_state.new_state_etag;
  meta.version = bravo_state.new_version;
  meta.fencing_token = bravo_state.new_version;
  rc = store->store_meta(store, "default", "bravo", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);
  assert_int_equal(count_query_index_records_of_type(
                       root, TEST_POUCH_QUERY_INDEX_RECORD_FORMAT),
                   1U);
  original_query_index_size = test_query_index_size(root);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  set_query_index_format_version(root, 99UL);

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  req.namespace_name = "default";
  rc = store->query_index_scan(store, &req, capture_scan_row, &row_capture,
                               &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(row_capture.count, 2U);
  assert_string_equal(row_capture.keys[0], "alpha");
  assert_string_equal(row_capture.keys[1], "bravo");
  assert_false(scan.truncated);
  assert_int_equal(test_query_index_size(root), original_query_index_size);
  assert_int_equal(count_query_index_records_of_type(
                       root, TEST_POUCH_QUERY_INDEX_RECORD_FORMAT),
                   1U);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  memset(&row_capture, 0, sizeof(row_capture));
  term.field = "/value";
  term.value = "s:alpha";
  req.document_eq_terms = &term;
  req.document_eq_term_count = 1U;
  rc = store->query_index_scan(store, &req, capture_scan_row, &row_capture,
                               &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(row_capture.count, 1U);
  assert_string_equal(row_capture.keys[0], "alpha");
  assert_false(scan.truncated);
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->query_index_keys_scan(store, &req, capture_query_key, &key_rows,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(key_rows.count, 1U);
  assert_string_equal(key_rows.keys[0], "alpha");
  assert_false(scan.truncated);
  assert_true(scan.index_seq > 0UL);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  lc_pouch_put_state_res_cleanup(&allocator, &bravo_state);
  lc_pouch_put_state_res_cleanup(&allocator, &alpha_state);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_query_index_rebuilds_legacy_sidecar_without_format(
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
  lc_error error;
  off_t original_query_index_size;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "query-index-format-legacy-missing");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&meta, 0, sizeof(meta));
  memset(&stored, 0, sizeof(stored));
  memset(&req, 0, sizeof(req));
  memset(&scan, 0, sizeof(scan));
  memset(&row_capture, 0, sizeof(row_capture));
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
  original_query_index_size = test_query_index_size(root);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  strip_query_index_format_record(root);
  assert_int_equal(test_query_index_size(root),
                   original_query_index_size -
                       (off_t)TEST_POUCH_QUERY_INDEX_HEADER_SIZE);

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  req.namespace_name = "default";
  rc = store->query_index_scan(store, &req, capture_scan_row, &row_capture,
                               &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(row_capture.count, 2U);
  assert_string_equal(row_capture.keys[0], "alpha");
  assert_string_equal(row_capture.keys[1], "bravo");
  assert_false(scan.truncated);
  assert_int_equal(test_query_index_size(root), original_query_index_size);
  assert_int_equal(count_query_index_records_of_type(
                       root, TEST_POUCH_QUERY_INDEX_RECORD_FORMAT),
                   1U);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_query_index_keys_truncates_partial_sidecar_field(void **state) {
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

  meta.lease_id = "lease-bravo";
  meta.state_etag = "state-bravo";
  meta.version = 2L;
  rc = store->store_meta(store, "default", "bravo", &meta, NULL, &stored,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &stored);

  original_query_index_size = test_query_index_size(root);
  truncated_query_index_size = original_query_index_size - 2;
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  truncate_query_index_tail(root, 2);
  assert_int_equal(test_query_index_size(root), truncated_query_index_size);

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  req.namespace_name = "default";
  rc = store->query_index_scan(store, &req, capture_scan_row, &row_capture,
                               &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(row_capture.count, 2U);
  assert_string_equal(row_capture.keys[0], "alpha");
  assert_int_equal(row_capture.versions[0], 1L);
  assert_string_equal(row_capture.keys[1], "bravo");
  assert_int_equal(row_capture.versions[1], 2L);
  assert_int_equal(test_query_index_size(root), original_query_index_size);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &scan);

  rc = store->query_index_keys_scan(store, &req, capture_query_key, &capture,
                                    &scan, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 2U);
  assert_string_equal(capture.keys[0], "alpha");
  assert_string_equal(capture.keys[1], "bravo");
  assert_int_equal(test_query_index_size(root), original_query_index_size);
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

static void test_query_index_sidecar_compacts_with_segments(void **state) {
  char root[256];
  char owner[2048];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res stored;
  lc_pouch_store_meta_res updated;
  lc_pouch_compaction_res compacted;
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
  memset(&compacted, 0, sizeof(compacted));
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
    rc = store->store_meta(store, "default", "compact-key", &meta, stored.etag,
                           &updated, &error);
    assert_int_equal(rc, LC_OK);
    lc_pouch_store_meta_res_cleanup(&allocator, &stored);
    stored = updated;
    memset(&updated, 0, sizeof(updated));
  }
  rc = store->compact(store, "force", &compacted, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(compacted.compacted);
  lc_pouch_compaction_res_cleanup(&allocator, &compacted);

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
  const char *payload_one_sha256;
  const char *payload_two_sha256;
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
  payload_one_sha256 =
      "47317b01099959fa40efebee37254415511f6f9fde3c93a74223e3730e515398";
  payload_two_sha256 =
      "1dcb0ea8c6e918ead74f08603000abd242de543722b49ff7b0e843a8edc5a2a1";

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
  assert_non_null(strstr(first.id, payload_one_sha256));
  assert_string_equal(first.name, "result.txt");
  assert_string_equal(first.content_type, "text/plain");
  assert_string_equal(first.plaintext_sha256, payload_one_sha256);
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
  assert_string_equal(list.items[0].plaintext_sha256, payload_one_sha256);
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
  assert_string_equal(fetched.plaintext_sha256, payload_one_sha256);
  text = read_source_text(read_body);
  assert_string_equal(text, "payload-one");
  free(text);
  lc_source_close(read_body);
  lc_pouch_object_info_cleanup(&allocator, &fetched);

  copy_opts.source.name = "result.txt";
  copy_opts.prevent_overwrite = 1;
  rc = store->copy_object(store, "default", "lease-key", "copy-key", &copy_opts,
                          &copied, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(copied.name, "result.txt");
  assert_string_equal(copied.id, first.id);
  assert_non_null(strstr(copied.id, payload_one_sha256));
  assert_string_equal(copied.content_type, "text/plain");
  assert_string_equal(copied.plaintext_sha256, payload_one_sha256);
  assert_int_equal(copied.size, 11L);

  rc = store->copy_object(store, "default", "lease-key", "copy-key", &copy_opts,
                          &fetched, &error);
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
  assert_string_equal(fetched.plaintext_sha256, payload_one_sha256);
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
  assert_non_null(strstr(fetched.id, payload_two_sha256));
  assert_string_equal(fetched.plaintext_sha256, payload_two_sha256);
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

static void
test_object_overwrite_allocation_failure_replays_cleanly(void **state) {
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

static void test_object_key_scan_orders_pages_and_filters_name(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_object_opts opts;
  lc_pouch_object_info info;
  lc_pouch_scan_object_keys_req req;
  lc_pouch_scan_object_keys_res scan;
  key_capture capture;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "object-key-scan");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  memset(&info, 0, sizeof(info));
  memset(&req, 0, sizeof(req));
  memset(&scan, 0, sizeof(scan));
  memset(&capture, 0, sizeof(capture));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(store->scan_object_keys);

  opts.content_type = "text/plain";
  opts.name = "decision";
  source = source_from_text("bravo-decision");
  rc = store->put_object(store, "default", "bravo", source, &opts, &info,
                         &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_object_info_cleanup(&allocator, &info);

  source = source_from_text("alpha-decision");
  rc = store->put_object(store, "default", "alpha", source, &opts, &info,
                         &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_object_info_cleanup(&allocator, &info);

  opts.name = "other";
  source = source_from_text("alpha-other");
  rc = store->put_object(store, "default", "alpha", source, &opts, &info,
                         &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_object_info_cleanup(&allocator, &info);

  opts.name = "decision";
  source = source_from_text("other-namespace");
  rc = store->put_object(store, "other", "aardvark", source, &opts, &info,
                         &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_object_info_cleanup(&allocator, &info);

  req.namespace_name = "default";
  req.name = "decision";
  req.limit = 1U;
  rc = store->scan_object_keys(store, &req, capture_query_key, &capture, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1U);
  assert_string_equal(capture.keys[0], "alpha");
  assert_true(scan.truncated);
  assert_string_equal(scan.next_start_after, "alpha");
  lc_pouch_scan_object_keys_res_cleanup(&allocator, &scan);

  memset(&capture, 0, sizeof(capture));
  req.start_after = "alpha";
  rc = store->scan_object_keys(store, &req, capture_query_key, &capture, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1U);
  assert_string_equal(capture.keys[0], "bravo");
  assert_false(scan.truncated);
  assert_null(scan.next_start_after);
  lc_pouch_scan_object_keys_res_cleanup(&allocator, &scan);

  memset(&capture, 0, sizeof(capture));
  req.name = "other";
  req.start_after = NULL;
  req.limit = 0U;
  rc = store->scan_object_keys(store, &req, capture_query_key, &capture, &scan,
                               &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(capture.count, 1U);
  assert_string_equal(capture.keys[0], "alpha");
  assert_false(scan.truncated);
  lc_pouch_scan_object_keys_res_cleanup(&allocator, &scan);

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
  assert_int_equal(
      count_log_records_of_type(root, TEST_POUCH_RECORD_OBJECT_PUT), 0U);
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

static void
test_object_copy_streams_existing_payload_without_large_alloc(void **state) {
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
  off_t segment_before_copy;
  off_t segment_after_copy;
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
  segment_before_copy = test_namespace_segments_size(root, "default");

  tracked.max_malloc_size = 0U;
  tracked.max_realloc_size = 0U;
  copy_opts.source.name = "large.bin";
  copy_opts.prevent_overwrite = 1;
  rc = store->copy_object(store, "default", "source-key", "dest-key",
                          &copy_opts, &copied, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(copied.id, original.id);
  assert_int_equal(copied.size, (long)payload_length);
  segment_after_copy = test_namespace_segments_size(root, "default");
  assert_true(segment_after_copy > segment_before_copy);
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

static void test_object_copy_rename_preserves_payload_metadata(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_object_opts put_opts;
  lc_pouch_copy_object_opts copy_opts;
  lc_pouch_object_selector selector;
  lc_pouch_object_info original;
  lc_pouch_object_info copied;
  lc_pouch_object_info fetched;
  lc_error error;
  char *text;
  const char *payload_sha256;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "object-copy-rename");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&put_opts, 0, sizeof(put_opts));
  memset(&copy_opts, 0, sizeof(copy_opts));
  memset(&selector, 0, sizeof(selector));
  memset(&original, 0, sizeof(original));
  memset(&copied, 0, sizeof(copied));
  memset(&fetched, 0, sizeof(fetched));
  store = NULL;
  read_body = NULL;
  payload_sha256 =
      "3b13ff1f1f217911e505d690692c4c533d6bb10349c9b08f405764657cbc6793";

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  put_opts.name = "source.bin";
  put_opts.content_type = "application/x-pouch-test";
  put_opts.prevent_overwrite = 1;
  source = source_from_text("copy-rename-payload");
  rc = store->put_object(store, "default", "source-key", source, &put_opts,
                         &original, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(original.name, "source.bin");
  assert_string_equal(original.content_type, "application/x-pouch-test");
  assert_string_equal(original.plaintext_sha256, payload_sha256);

  copy_opts.source.name = "source.bin";
  copy_opts.name = "renamed.bin";
  copy_opts.prevent_overwrite = 1;
  rc = store->copy_object(store, "default", "source-key", "dest-key",
                          &copy_opts, &copied, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(copied.name, "renamed.bin");
  assert_string_equal(copied.content_type, "application/x-pouch-test");
  assert_string_equal(copied.plaintext_sha256, payload_sha256);
  assert_int_equal(copied.size, original.size);
  assert_non_null(strstr(copied.id, payload_sha256));
  assert_non_null(strstr(copied.id, "-renamed.bin"));
  assert_null(strstr(copied.id, "-source.bin"));

  selector.name = "renamed.bin";
  rc = store->get_object(store, "default", "dest-key", &selector, &read_body,
                         &fetched, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(fetched.id, copied.id);
  assert_string_equal(fetched.name, "renamed.bin");
  assert_string_equal(fetched.content_type, "application/x-pouch-test");
  assert_string_equal(fetched.plaintext_sha256, payload_sha256);
  text = read_source_text(read_body);
  assert_string_equal(text, "copy-rename-payload");
  free(text);
  lc_source_close(read_body);

  lc_pouch_object_info_cleanup(&allocator, &fetched);
  lc_pouch_object_info_cleanup(&allocator, &copied);
  lc_pouch_object_info_cleanup(&allocator, &original);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_object_copy_enforces_expected_etag(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_object_opts put_opts;
  lc_pouch_copy_object_opts copy_opts;
  lc_pouch_object_info original;
  lc_pouch_object_info copied;
  lc_pouch_object_list list;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "object-copy-etag");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&put_opts, 0, sizeof(put_opts));
  memset(&copy_opts, 0, sizeof(copy_opts));
  memset(&original, 0, sizeof(original));
  memset(&copied, 0, sizeof(copied));
  memset(&list, 0, sizeof(list));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  put_opts.name = "source.bin";
  put_opts.content_type = "application/octet-stream";
  put_opts.prevent_overwrite = 1;
  source = source_from_text("copy-etag-payload");
  rc = store->put_object(store, "default", "source-key", source, &put_opts,
                         &original, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  copy_opts.source.name = "source.bin";
  copy_opts.name = "dest.bin";
  copy_opts.expected_etag = "wrong-etag";
  copy_opts.prevent_overwrite = 1;
  rc = store->copy_object(store, "default", "source-key", "dest-key",
                          &copy_opts, &copied, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 412L);
  assert_string_equal(error.server_code, "precondition_failed");
  assert_null(copied.id);
  lc_error_cleanup(&error);

  rc = store->list_objects(store, "default", "dest-key", &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 0U);
  lc_pouch_object_list_cleanup(&allocator, &list);

  copy_opts.expected_etag = original.id;
  rc = store->copy_object(store, "default", "source-key", "dest-key",
                          &copy_opts, &copied, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(copied.name, "dest.bin");
  assert_non_null(strstr(copied.id, "-dest.bin"));
  assert_null(strstr(copied.id, "-source.bin"));
  assert_string_equal(copied.plaintext_sha256, original.plaintext_sha256);
  assert_int_equal(copied.size, original.size);

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
  char segment_path[512];
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
  rc = store->put_object(store, "default", "source-key", &source.pub, &put_opts,
                         &original, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(original.size, (long)payload_length);
  assert_int_equal(count_namespace_segment_records_of_type(
                       root, "default", TEST_POUCH_RECORD_OBJECT_PUT),
                   1U);

  test_active_segment_path(root, "default", segment_path, sizeof(segment_path));
  assert_int_equal(chmod(segment_path, 0), 0);

  copy_opts.source.name = "large.bin";
  copy_opts.prevent_overwrite = 1;
  rc = store->copy_object(store, "default", "source-key", "dest-key",
                          &copy_opts, &copied, &error);
  assert_int_equal(chmod(segment_path, 0600), 0);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_string_equal(error.message,
                      "failed to open pouch log for object copy");
  assert_null(copied.id);
  assert_int_equal(count_namespace_segment_records_of_type(
                       root, "default", TEST_POUCH_RECORD_OBJECT_PUT),
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

static void test_object_copy_refreshes_after_segment_compaction(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *writer;
  lc_pouch_store *reader;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_object_opts put_opts;
  lc_pouch_copy_object_opts copy_opts;
  lc_pouch_object_selector selector;
  lc_pouch_object_info original;
  lc_pouch_object_info stale_view;
  lc_pouch_object_info copied;
  lc_pouch_object_info fetched;
  lc_pouch_compaction_res compacted;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "object-copy-log-replacement");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&put_opts, 0, sizeof(put_opts));
  memset(&copy_opts, 0, sizeof(copy_opts));
  memset(&selector, 0, sizeof(selector));
  memset(&original, 0, sizeof(original));
  memset(&stale_view, 0, sizeof(stale_view));
  memset(&copied, 0, sizeof(copied));
  memset(&fetched, 0, sizeof(fetched));
  memset(&compacted, 0, sizeof(compacted));
  writer = NULL;
  reader = NULL;
  read_body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &writer, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_disk_open(root, &allocator, &reader, &error);
  assert_int_equal(rc, LC_OK);

  put_opts.name = "source.bin";
  put_opts.content_type = "application/octet-stream";
  put_opts.prevent_overwrite = 1;
  source = source_from_text("copy-after-compaction");
  rc = writer->put_object(writer, "default", "source-key", source, &put_opts,
                          &original, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  selector.name = "source.bin";
  rc = reader->get_object(reader, "default", "source-key", &selector,
                          &read_body, &stale_view, &error);
  assert_int_equal(rc, LC_OK);
  text = read_source_text(read_body);
  assert_string_equal(text, "copy-after-compaction");
  free(text);
  lc_source_close(read_body);
  read_body = NULL;
  lc_pouch_object_info_cleanup(&allocator, &stale_view);

  rc = writer->compact(writer, "force", &compacted, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(compacted.compacted);
  lc_pouch_compaction_res_cleanup(&allocator, &compacted);

  copy_opts.source.name = "source.bin";
  copy_opts.name = "copied.bin";
  copy_opts.prevent_overwrite = 1;
  rc = reader->copy_object(reader, "default", "source-key", "dest-key",
                           &copy_opts, &copied, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(copied.name, "copied.bin");
  assert_string_equal(copied.plaintext_sha256, original.plaintext_sha256);
  assert_int_equal(copied.size, original.size);

  selector.name = "copied.bin";
  rc = reader->get_object(reader, "default", "dest-key", &selector, &read_body,
                          &fetched, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(fetched.id, copied.id);
  text = read_source_text(read_body);
  assert_string_equal(text, "copy-after-compaction");
  free(text);
  lc_source_close(read_body);

  lc_pouch_object_info_cleanup(&allocator, &fetched);
  lc_pouch_object_info_cleanup(&allocator, &copied);
  lc_pouch_object_info_cleanup(&allocator, &original);
  rc = reader->close(reader, &error);
  assert_int_equal(rc, LC_OK);
  rc = writer->close(writer, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_queue_dequeue_skips_replay_after_same_handle_enqueue(void **state) {
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
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts, &payload,
                              &dequeued, &error);
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

static void test_queue_dequeue_honors_start_after_cursor(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_enqueue_opts enqueue_opts;
  lc_pouch_dequeue_opts dequeue_opts;
  lc_pouch_queue_message_info enqueued[3];
  lc_pouch_queue_message_info dequeued;
  lc_source *source;
  lc_source *payload;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-start-after");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&dequeue_opts, 0, sizeof(dequeue_opts));
  memset(enqueued, 0, sizeof(enqueued));
  memset(&dequeued, 0, sizeof(dequeued));
  store = NULL;
  source = NULL;
  payload = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  enqueue_opts.content_type = "text/plain";
  enqueue_opts.visibility_timeout_seconds = 30L;
  enqueue_opts.ttl_seconds = 3600L;
  enqueue_opts.max_attempts = 3;

  source = source_from_text("first");
  rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                              &enqueued[0], &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("second");
  rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                              &enqueued[1], &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  source = source_from_text("third");
  rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                              &enqueued[2], &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  dequeue_opts.owner = "worker-a";
  dequeue_opts.visibility_timeout_seconds = 30L;
  dequeue_opts.start_after = enqueued[0].message_id;
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts, &payload,
                              &dequeued, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(payload);
  assert_string_equal(dequeued.message_id, enqueued[1].message_id);

  lc_source_close(payload);
  lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);
  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued[0]);
  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued[1]);
  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued[2]);
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
  rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                              &enqueued, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "enqueue_message delay_seconds must be non-negative");
  lc_error_cleanup(&error);

  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  enqueue_opts.content_type = "text/plain";
  enqueue_opts.visibility_timeout_seconds = -1L;
  source = source_from_text("bad-visibility");
  rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                              &enqueued, &error);
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
  rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                              &enqueued, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "enqueue_message ttl_seconds must be non-negative");
  lc_error_cleanup(&error);

  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  enqueue_opts.content_type = "text/plain";
  enqueue_opts.max_attempts = -1;
  source = source_from_text("bad-attempts");
  rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                              &enqueued, &error);
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
  rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                              &enqueued, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  dequeue_opts.owner = "worker-a";
  dequeue_opts.visibility_timeout_seconds = -1L;
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts, &payload,
                              &dequeued, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(payload);
  assert_string_equal(
      error.message,
      "dequeue_message visibility_timeout_seconds must be non-negative");
  lc_error_cleanup(&error);

  dequeue_opts.visibility_timeout_seconds = 30L;
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts, &payload,
                              &dequeued, &error);
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
  rc = store->store_meta(store, "default", "lease-key", &meta, NULL, &meta_res,
                         &error);
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

static void
test_manual_compaction_reports_stats_and_preserves_state(void **state) {
  char root[256];
  char manifest_path[512];
  char manifest_text[1024];
  char snapshot_path[512];
  char snapshot2_path[512];
  char backend_snapshot_path[512];
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
  char *text;
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

  before_log_size =
      test_log_size(root) + test_namespace_segments_size(root, "default");
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
  assert_int_equal(test_log_size(root), 0);
  test_snapshot_path(root, "%2elockd", 1UL, backend_snapshot_path,
                     sizeof(backend_snapshot_path));
  assert_int_equal(count_log_records_at_path_of_type(
                       backend_snapshot_path, TEST_POUCH_RECORD_HIGH_WATER),
                   1U);
  test_manifest_path(root, "default", manifest_path, sizeof(manifest_path));
  test_read_file_text(manifest_path, manifest_text, sizeof(manifest_text));
  assert_non_null(strstr(manifest_text, "open seg-0000000000000001.log\n"));
  assert_non_null(
      strstr(manifest_text, "snapshot snap-0000000000000001.log\n"));
  assert_non_null(strstr(manifest_text, "obsolete seg-0000000000000001.log\n"));
  test_snapshot_path(root, "default", 1UL, snapshot_path,
                     sizeof(snapshot_path));
  assert_true(access(snapshot_path, R_OK) == 0);
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
  body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  state_opts.content_type = "text/plain";
  source = source_from_text("second-generation");
  rc = store->write_state(store, "default", "hot-key", source, &state_opts,
                          &put_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  last_version = put_res.new_version;
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  memset(&put_res, 0, sizeof(put_res));

  memset(&compacted, 0, sizeof(compacted));
  rc = store->compact(store, "force", &compacted, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(compacted.compacted, 1);
  lc_pouch_compaction_res_cleanup(&allocator, &compacted);
  test_read_file_text(manifest_path, manifest_text, sizeof(manifest_text));
  assert_non_null(
      strstr(manifest_text, "snapshot snap-0000000000000002.log\n"));
  assert_non_null(
      strstr(manifest_text, "obsolete snap-0000000000000001.log\n"));
  test_snapshot_path(root, "default", 2UL, snapshot2_path,
                     sizeof(snapshot2_path));
  assert_int_equal(access(snapshot_path, F_OK), -1);
  assert_int_equal(errno, ENOENT);
  assert_true(access(snapshot2_path, R_OK) == 0);

  rc = store->read_state(store, "default", "hot-key", &body, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(body);
  assert_int_equal(state_info.version, last_version);
  text = read_source_text(body);
  assert_string_equal(text, "second-generation");
  free(text);
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
  text = read_source_text(body);
  assert_string_equal(text, "second-generation");
  free(text);
  lc_source_close(body);
  body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_compaction_if_needed_skip_and_allocator_failure(void **state) {
  char root[256];
  char key[64];
  char payload[4096];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts state_opts;
  lc_pouch_put_state_res put_res;
  lc_pouch_compaction_res skipped;
  lc_pouch_compaction_res failed;
  lc_error error;
  off_t before_log_size;
  off_t live_log_size;
  size_t index;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "compact-if-needed");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&state_opts, 0, sizeof(state_opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&skipped, 0, sizeof(skipped));
  memset(&failed, 0, sizeof(failed));
  memset(payload, 'l', sizeof(payload));
  store = NULL;
  source = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  before_log_size =
      test_log_size(root) + test_namespace_segments_size(root, "default");

  rc = store->compact(store, "if_needed", &skipped, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(skipped.mode, "if_needed");
  assert_string_equal(skipped.skip_reason, "below-min-log-size");
  assert_int_equal(skipped.accepted, 1);
  assert_int_equal(skipped.compacted, 0);
  assert_int_equal(skipped.skipped, 1);
  assert_int_equal(skipped.before_log_bytes, (unsigned long)before_log_size);
  assert_int_equal(skipped.after_log_bytes, skipped.before_log_bytes);
  assert_int_equal(test_log_size(root) +
                       test_namespace_segments_size(root, "default"),
                   before_log_size);
  lc_pouch_compaction_res_cleanup(&allocator, &skipped);

  state_opts.content_type = "application/octet-stream";
  for (index = 0U; index < 20U; ++index) {
    snprintf(key, sizeof(key), "live-key-%02lu", (unsigned long)index);
    rc = lc_source_from_memory(payload, sizeof(payload), &source, &error);
    assert_int_equal(rc, LC_OK);
    rc = store->write_state(store, "default", key, source, &state_opts,
                            &put_res, &error);
    lc_source_close(source);
    source = NULL;
    assert_int_equal(rc, LC_OK);
    lc_pouch_put_state_res_cleanup(&allocator, &put_res);
    memset(&put_res, 0, sizeof(put_res));
  }
  live_log_size =
      test_log_size(root) + test_namespace_segments_size(root, "default");
  assert_true(live_log_size > before_log_size);

  memset(&skipped, 0, sizeof(skipped));
  rc = store->compact(store, "if_needed", &skipped, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(skipped.mode, "if_needed");
  assert_string_equal(skipped.skip_reason, "below-obsolete-threshold");
  assert_int_equal(skipped.accepted, 1);
  assert_int_equal(skipped.compacted, 0);
  assert_int_equal(skipped.skipped, 1);
  assert_int_equal(skipped.before_log_bytes, (unsigned long)live_log_size);
  assert_int_equal(skipped.after_log_bytes, skipped.before_log_bytes);
  assert_int_equal(test_log_size(root) +
                       test_namespace_segments_size(root, "default"),
                   live_log_size);
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
  assert_int_equal(test_log_size(root) +
                       test_namespace_segments_size(root, "default"),
                   live_log_size);
  lc_error_cleanup(&error);
  memset(&error, 0, sizeof(error));

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_scheduled_maintenance_reports_compaction_diagnostics(
    void **state) {
  char root[256];
  char payload[4096];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_disk_open_opts opts;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts state_opts;
  lc_pouch_put_state_res put_res;
  lc_pouch_maintenance_res maintenance;
  lc_error error;
  off_t churned_log_size;
  size_t index;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "scheduled-maintenance-compact");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&opts, 0, sizeof(opts));
  memset(&error, 0, sizeof(error));
  memset(&state_opts, 0, sizeof(state_opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&maintenance, 0, sizeof(maintenance));
  memset(payload, 'm', sizeof(payload));
  payload[sizeof(payload) - 1U] = '\0';
  store = NULL;
  source = NULL;

  opts.background_compaction_min_log_bytes = (unsigned long)-1;
  rc = lc_pouch_disk_open_with_options(root, &allocator, &opts, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(store->maintenance);
  state_opts.content_type = "application/octet-stream";
  for (index = 0U; index < 24U; ++index) {
    source = source_from_text(payload);
    rc = store->write_state(store, "default", "scheduled-key", source,
                            &state_opts, &put_res, &error);
    lc_source_close(source);
    source = NULL;
    assert_int_equal(rc, LC_OK);
    lc_pouch_put_state_res_cleanup(&allocator, &put_res);
    memset(&put_res, 0, sizeof(put_res));
  }
  churned_log_size =
      test_log_size(root) + test_namespace_segments_size(root, "default");

  rc = store->maintenance(store, "scheduled", &maintenance, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance.mode, "scheduled");
  assert_string_equal(maintenance.reason, "background-disabled");
  assert_int_equal(maintenance.accepted, 1);
  assert_int_equal(maintenance.compaction_enabled, 0);
  assert_int_equal(maintenance.compaction.accepted, 0);
  assert_int_equal(test_log_size(root) +
                       test_namespace_segments_size(root, "default"),
                   churned_log_size);
  lc_pouch_maintenance_res_cleanup(&allocator, &maintenance);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  memset(&opts, 0, sizeof(opts));
  opts.background_compaction = 1;
  opts.background_compaction_min_log_bytes = 1UL;
  opts.background_compaction_obsolete_multiplier = 2UL;
  opts.background_compaction_interval_seconds = 3600UL;
  rc = lc_pouch_disk_open_with_options(root, &allocator, &opts, &store, &error);
  assert_int_equal(rc, LC_OK);

  memset(&maintenance, 0, sizeof(maintenance));
  rc = store->maintenance(store, NULL, &maintenance, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance.mode, "scheduled");
  assert_string_equal(maintenance.reason, "compacted");
  assert_int_equal(maintenance.accepted, 1);
  assert_int_equal(maintenance.compaction_enabled, 1);
  assert_string_equal(maintenance.compaction.mode, "if_needed");
  assert_int_equal(maintenance.compaction.accepted, 1);
  assert_int_equal(maintenance.compaction.compacted, 1);
  assert_int_equal(maintenance.compaction.skipped, 0);
  assert_true(maintenance.compaction.before_log_bytes >=
              (unsigned long)churned_log_size);
  assert_true(maintenance.compaction.after_log_bytes <
              maintenance.compaction.before_log_bytes);
  lc_pouch_maintenance_res_cleanup(&allocator, &maintenance);

  memset(&maintenance, 0, sizeof(maintenance));
  rc = store->maintenance(store, "scheduled", &maintenance, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance.reason, "interval-not-elapsed");
  assert_int_equal(maintenance.compaction_enabled, 1);
  assert_int_equal(maintenance.compaction.accepted, 0);
  assert_int_equal(maintenance.compaction.compacted, 0);
  assert_int_equal(maintenance.compaction.skipped, 0);
  lc_pouch_maintenance_res_cleanup(&allocator, &maintenance);

  rc = store->maintenance(store, "manual", &maintenance, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  lc_error_cleanup(&error);
  memset(&error, 0, sizeof(error));

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_scheduled_maintenance_honors_not_before_deadline(
    void **state) {
  char root[256];
  char payload[4096];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_disk_open_opts generate_opts;
  lc_pouch_disk_open_opts opts;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts state_opts;
  lc_pouch_put_state_res put_res;
  lc_pouch_maintenance_res maintenance;
  lc_error error;
  off_t before_log_size;
  time_t now;
  size_t index;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "scheduled-maintenance-deadline");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&generate_opts, 0, sizeof(generate_opts));
  memset(&opts, 0, sizeof(opts));
  memset(&error, 0, sizeof(error));
  memset(&state_opts, 0, sizeof(state_opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&maintenance, 0, sizeof(maintenance));
  memset(payload, 'd', sizeof(payload));
  payload[sizeof(payload) - 1U] = '\0';
  store = NULL;
  source = NULL;

  now = time(NULL);
  assert_true(now > (time_t)0);

  generate_opts.background_compaction_min_log_bytes = (unsigned long)-1;
  rc = lc_pouch_disk_open_with_options(root, &allocator, &generate_opts, &store,
                                       &error);
  assert_int_equal(rc, LC_OK);

  state_opts.content_type = "application/octet-stream";
  for (index = 0U; index < 24U; ++index) {
    source = source_from_text(payload);
    rc = store->write_state(store, "default", "deadline-key", source,
                            &state_opts, &put_res, &error);
    lc_source_close(source);
    source = NULL;
    assert_int_equal(rc, LC_OK);
    lc_pouch_put_state_res_cleanup(&allocator, &put_res);
    memset(&put_res, 0, sizeof(put_res));
  }
  before_log_size =
      test_log_size(root) + test_namespace_segments_size(root, "default");

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  opts.background_compaction = 1;
  opts.background_compaction_min_log_bytes = 1UL;
  opts.background_compaction_obsolete_multiplier = 2UL;
  opts.background_compaction_not_before_unix = (long)now + 3600L;
  rc = lc_pouch_disk_open_with_options(root, &allocator, &opts, &store, &error);
  assert_int_equal(rc, LC_OK);

  rc = store->maintenance(store, "scheduled", &maintenance, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance.mode, "scheduled");
  assert_string_equal(maintenance.reason, "deadline-not-reached");
  assert_int_equal(maintenance.accepted, 1);
  assert_int_equal(maintenance.compaction_enabled, 1);
  assert_int_equal(maintenance.compaction.accepted, 0);
  assert_int_equal(maintenance.compaction.compacted, 0);
  assert_int_equal(test_log_size(root) +
                       test_namespace_segments_size(root, "default"),
                   before_log_size);
  lc_pouch_maintenance_res_cleanup(&allocator, &maintenance);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  opts.background_compaction_not_before_unix = (long)now - 1L;
  rc = lc_pouch_disk_open_with_options(root, &allocator, &opts, &store, &error);
  assert_int_equal(rc, LC_OK);

  memset(&maintenance, 0, sizeof(maintenance));
  rc = store->maintenance(store, "scheduled", &maintenance, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance.reason, "compacted");
  assert_int_equal(maintenance.compaction_enabled, 1);
  assert_int_equal(maintenance.compaction.accepted, 1);
  assert_int_equal(maintenance.compaction.compacted, 1);
  assert_int_equal(maintenance.compaction.skipped, 0);
  assert_true(maintenance.compaction.before_log_bytes ==
              (unsigned long)before_log_size);
  assert_true(maintenance.compaction.after_log_bytes <
              maintenance.compaction.before_log_bytes);
  lc_pouch_maintenance_res_cleanup(&allocator, &maintenance);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  memset(&opts, 0, sizeof(opts));
  opts.background_compaction_not_before_unix = -1L;
  rc = lc_pouch_disk_open_with_options(root, &allocator, &opts, &store, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(store);
  assert_string_equal(
      error.message,
      "pouch disk background_compaction_not_before_unix must be non-negative");

  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_scheduled_maintenance_honors_min_candidate_files(
    void **state) {
  char root[256];
  char payload[4096];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_disk_open_opts generate_opts;
  lc_pouch_disk_open_opts opts;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts state_opts;
  lc_pouch_put_state_res put_res;
  lc_pouch_maintenance_res maintenance;
  lc_error error;
  off_t one_candidate_log_size;
  size_t index;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "scheduled-maintenance-min-candidates");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&generate_opts, 0, sizeof(generate_opts));
  memset(&opts, 0, sizeof(opts));
  memset(&error, 0, sizeof(error));
  memset(&state_opts, 0, sizeof(state_opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&maintenance, 0, sizeof(maintenance));
  memset(payload, 'c', sizeof(payload));
  payload[sizeof(payload) - 1U] = '\0';
  store = NULL;
  source = NULL;

  generate_opts.background_compaction_min_log_bytes = (unsigned long)-1;
  rc = lc_pouch_disk_open_with_options(root, &allocator, &generate_opts, &store,
                                       &error);
  assert_int_equal(rc, LC_OK);

  state_opts.content_type = "application/octet-stream";
  for (index = 0U; index < 18U; ++index) {
    source = source_from_text(payload);
    rc = store->write_state(store, "default", "candidate-key", source,
                            &state_opts, &put_res, &error);
    lc_source_close(source);
    source = NULL;
    assert_int_equal(rc, LC_OK);
    lc_pouch_put_state_res_cleanup(&allocator, &put_res);
    memset(&put_res, 0, sizeof(put_res));
  }
  one_candidate_log_size =
      test_log_size(root) + test_namespace_segments_size(root, "default");

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  opts.background_compaction = 1;
  opts.background_compaction_min_log_bytes = 1UL;
  opts.background_compaction_obsolete_multiplier = 2UL;
  opts.background_compaction_min_candidate_files = 2UL;
  rc = lc_pouch_disk_open_with_options(root, &allocator, &opts, &store, &error);
  assert_int_equal(rc, LC_OK);

  rc = store->maintenance(store, "scheduled", &maintenance, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance.mode, "scheduled");
  assert_string_equal(maintenance.reason, "below-candidate-threshold");
  assert_int_equal(maintenance.accepted, 1);
  assert_int_equal(maintenance.compaction_enabled, 1);
  assert_int_equal(maintenance.compaction.accepted, 0);
  assert_int_equal(maintenance.compaction.compacted, 0);
  assert_int_equal(test_log_size(root) +
                       test_namespace_segments_size(root, "default"),
                   one_candidate_log_size);
  lc_pouch_maintenance_res_cleanup(&allocator, &maintenance);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  rc = lc_pouch_disk_open_with_options(root, &allocator, &generate_opts, &store,
                                       &error);
  assert_int_equal(rc, LC_OK);
  for (index = 0U; index < 20U; ++index) {
    source = source_from_text(payload);
    rc = store->write_state(store, "default", "candidate-key", source,
                            &state_opts, &put_res, &error);
    lc_source_close(source);
    source = NULL;
    assert_int_equal(rc, LC_OK);
    lc_pouch_put_state_res_cleanup(&allocator, &put_res);
    memset(&put_res, 0, sizeof(put_res));
  }
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  rc = lc_pouch_disk_open_with_options(root, &allocator, &opts, &store, &error);
  assert_int_equal(rc, LC_OK);

  memset(&maintenance, 0, sizeof(maintenance));
  rc = store->maintenance(store, "scheduled", &maintenance, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance.reason, "compacted");
  assert_int_equal(maintenance.compaction_enabled, 1);
  assert_int_equal(maintenance.compaction.accepted, 1);
  assert_int_equal(maintenance.compaction.compacted, 1);
  assert_int_equal(maintenance.compaction.skipped, 0);
  assert_true(maintenance.compaction.before_log_bytes >
              (unsigned long)one_candidate_log_size);
  assert_true(maintenance.compaction.after_log_bytes <
              maintenance.compaction.before_log_bytes);
  lc_pouch_maintenance_res_cleanup(&allocator, &maintenance);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_scheduled_maintenance_honors_min_reclaimable_bytes(
    void **state) {
  char root[256];
  char payload[4096];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_disk_open_opts generate_opts;
  lc_pouch_disk_open_opts opts;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts state_opts;
  lc_pouch_put_state_res put_res;
  lc_pouch_maintenance_res maintenance;
  lc_error error;
  off_t before_log_size;
  size_t index;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "scheduled-maintenance-min-reclaimable");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&generate_opts, 0, sizeof(generate_opts));
  memset(&opts, 0, sizeof(opts));
  memset(&error, 0, sizeof(error));
  memset(&state_opts, 0, sizeof(state_opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&maintenance, 0, sizeof(maintenance));
  memset(payload, 'r', sizeof(payload));
  payload[sizeof(payload) - 1U] = '\0';
  store = NULL;
  source = NULL;

  generate_opts.background_compaction_min_log_bytes = (unsigned long)-1;
  rc = lc_pouch_disk_open_with_options(root, &allocator, &generate_opts, &store,
                                       &error);
  assert_int_equal(rc, LC_OK);

  state_opts.content_type = "application/octet-stream";
  for (index = 0U; index < 24U; ++index) {
    source = source_from_text(payload);
    rc = store->write_state(store, "default", "reclaimable-key", source,
                            &state_opts, &put_res, &error);
    lc_source_close(source);
    source = NULL;
    assert_int_equal(rc, LC_OK);
    lc_pouch_put_state_res_cleanup(&allocator, &put_res);
    memset(&put_res, 0, sizeof(put_res));
  }
  before_log_size =
      test_log_size(root) + test_namespace_segments_size(root, "default");

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  opts.background_compaction = 1;
  opts.background_compaction_min_log_bytes = 1UL;
  opts.background_compaction_obsolete_multiplier = 2UL;
  opts.background_compaction_min_reclaimable_bytes =
      (unsigned long)before_log_size + 1UL;
  rc = lc_pouch_disk_open_with_options(root, &allocator, &opts, &store, &error);
  assert_int_equal(rc, LC_OK);

  rc = store->maintenance(store, "scheduled", &maintenance, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance.mode, "scheduled");
  assert_string_equal(maintenance.reason, "below-reclaimable-threshold");
  assert_int_equal(maintenance.accepted, 1);
  assert_int_equal(maintenance.compaction_enabled, 1);
  assert_int_equal(maintenance.compaction.accepted, 0);
  assert_int_equal(maintenance.compaction.compacted, 0);
  assert_int_equal(test_log_size(root) +
                       test_namespace_segments_size(root, "default"),
                   before_log_size);
  lc_pouch_maintenance_res_cleanup(&allocator, &maintenance);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  opts.background_compaction_min_reclaimable_bytes = 1UL;
  rc = lc_pouch_disk_open_with_options(root, &allocator, &opts, &store, &error);
  assert_int_equal(rc, LC_OK);

  memset(&maintenance, 0, sizeof(maintenance));
  rc = store->maintenance(store, "scheduled", &maintenance, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance.reason, "compacted");
  assert_int_equal(maintenance.compaction_enabled, 1);
  assert_int_equal(maintenance.compaction.accepted, 1);
  assert_int_equal(maintenance.compaction.compacted, 1);
  assert_int_equal(maintenance.compaction.skipped, 0);
  assert_true(maintenance.compaction.before_log_bytes ==
              (unsigned long)before_log_size);
  assert_true(maintenance.compaction.after_log_bytes <
              maintenance.compaction.before_log_bytes);
  lc_pouch_maintenance_res_cleanup(&allocator, &maintenance);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_scheduled_maintenance_honors_io_throttle(void **state) {
  char root[256];
  char payload[4096];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_disk_open_opts generate_opts;
  lc_pouch_disk_open_opts opts;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts state_opts;
  lc_pouch_put_state_res put_res;
  lc_pouch_maintenance_res maintenance;
  lc_pouch_compaction_res compacted;
  lc_error error;
  off_t before_log_size;
  size_t index;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "scheduled-maintenance-io-throttle");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&generate_opts, 0, sizeof(generate_opts));
  memset(&opts, 0, sizeof(opts));
  memset(&error, 0, sizeof(error));
  memset(&state_opts, 0, sizeof(state_opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&maintenance, 0, sizeof(maintenance));
  memset(&compacted, 0, sizeof(compacted));
  memset(payload, 't', sizeof(payload));
  payload[sizeof(payload) - 1U] = '\0';
  store = NULL;
  source = NULL;

  generate_opts.background_compaction_min_log_bytes = (unsigned long)-1;
  rc = lc_pouch_disk_open_with_options(root, &allocator, &generate_opts, &store,
                                       &error);
  assert_int_equal(rc, LC_OK);

  state_opts.content_type = "application/octet-stream";
  for (index = 0U; index < 24U; ++index) {
    source = source_from_text(payload);
    rc = store->write_state(store, "default", "throttled-key", source,
                            &state_opts, &put_res, &error);
    lc_source_close(source);
    source = NULL;
    assert_int_equal(rc, LC_OK);
    lc_pouch_put_state_res_cleanup(&allocator, &put_res);
    memset(&put_res, 0, sizeof(put_res));
  }
  before_log_size =
      test_log_size(root) + test_namespace_segments_size(root, "default");
  assert_true(before_log_size > 1);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  store = NULL;

  opts.background_compaction = 1;
  opts.background_compaction_min_log_bytes = 1UL;
  opts.background_compaction_obsolete_multiplier = 2UL;
  opts.background_compaction_max_io_bytes =
      (unsigned long)before_log_size - 1UL;
  rc = lc_pouch_disk_open_with_options(root, &allocator, &opts, &store, &error);
  assert_int_equal(rc, LC_OK);

  rc = store->maintenance(store, "scheduled", &maintenance, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(maintenance.mode, "scheduled");
  assert_string_equal(maintenance.reason, "io-throttled");
  assert_int_equal(maintenance.accepted, 1);
  assert_int_equal(maintenance.compaction_enabled, 1);
  assert_int_equal(maintenance.compaction.accepted, 0);
  assert_int_equal(maintenance.compaction.compacted, 0);
  assert_int_equal(test_log_size(root) +
                       test_namespace_segments_size(root, "default"),
                   before_log_size);
  lc_pouch_maintenance_res_cleanup(&allocator, &maintenance);

  rc = store->compact(store, "force", &compacted, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(compacted.mode, "force");
  assert_int_equal(compacted.accepted, 1);
  assert_int_equal(compacted.compacted, 1);
  assert_true(compacted.before_log_bytes == (unsigned long)before_log_size);
  assert_true(compacted.after_log_bytes < compacted.before_log_bytes);
  lc_pouch_compaction_res_cleanup(&allocator, &compacted);

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
  assert_int_equal(count_namespace_segment_records_of_type(
                       root, "default", TEST_POUCH_RECORD_STATE_LINK),
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
  assert_int_equal(count_namespace_segment_records_of_type(
                       root, "default", TEST_POUCH_RECORD_STATE_LINK),
                   0U);

  rc = store->read_state(store, "default", "linked-key", &body, &info, &error);
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
  rc = store->read_state(store, "default", "linked-key", &body, &info, &error);
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

static void
test_independent_handle_refreshes_after_segment_compaction(void **state) {
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

  rc = second->load_meta(second, "default", "lease-key", &loaded_meta, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(loaded_meta.found);
  lc_pouch_meta_record_cleanup(&allocator, &loaded_meta);

  meta.owner = "owner-a";
  meta.lease_id = "lease-a";
  meta.state_etag = "state-a";
  meta.version = 10L;
  rc = first->store_meta(first, "default", "lease-key", &meta, NULL, &meta_res,
                         &error);
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

  rc = second->load_meta(second, "default", "lease-key", &loaded_meta, &error);
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
  lc_pouch_compaction_res compacted;
  lc_error error;
  char *text;
  size_t index;
  off_t snapshot_after_compaction;
  int acked;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-dequeue-compact");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&dequeue_opts, 0, sizeof(dequeue_opts));
  memset(&compacted, 0, sizeof(compacted));
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
    rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                                &enqueued, &error);
    lc_source_close(source);
    assert_int_equal(rc, LC_OK);

    rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts, &body,
                                &dequeued, &error);
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

  memset(&enqueued, 0, sizeof(enqueued));
  source = source_from_text("queue-payload");
  rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                              &enqueued, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  assert_true(test_log_size(root) < (off_t)(120U * 256U));
  rc = store->compact(store, "force", &compacted, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(compacted.compacted);
  snapshot_after_compaction = test_namespace_snapshots_size(root, "default");
  assert_true(snapshot_after_compaction > (off_t)TEST_POUCH_HEADER_SIZE);
  lc_pouch_compaction_res_cleanup(&allocator, &compacted);

  memset(&dequeued, 0, sizeof(dequeued));
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts, &body,
                              &dequeued, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(body);
  assert_string_equal(dequeued.message_id, enqueued.message_id);
  text = read_source_text(body);
  assert_string_equal(text, "queue-payload");
  free(text);
  lc_source_close(body);
  body = NULL;
  lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);
  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);

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
  assert_int_equal(
      count_log_records_of_type(root, TEST_POUCH_RECORD_OBJECT_PUT), 0U);
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
  assert_string_equal(error.message,
                      "write_state namespace must not contain '/'");
  assert_int_equal(count_log_records_of_type(root, TEST_POUCH_RECORD_STATE_PUT),
                   0U);
  lc_error_cleanup(&error);

  source = source_from_text("state");
  rc = store->write_state(store, "default", "alpha//bravo", source, &state_opts,
                          &state_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
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
  rc = store->scan_meta(store, &scan_req, capture_scan_row, &capture, &scan_res,
                        &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "scan_meta namespace must not contain '/'");
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

static void test_queue_transaction_apply_touches_wake_marker(void **state) {
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
  lc_pouch_queue_message_info redelivered;
  lc_pouch_queue_ref ref;
  lc_error error;
  int acked;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-txn-wake-marker");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&dequeue_opts, 0, sizeof(dequeue_opts));
  memset(&enqueued, 0, sizeof(enqueued));
  memset(&dequeued, 0, sizeof(dequeued));
  memset(&redelivered, 0, sizeof(redelivered));
  memset(&ref, 0, sizeof(ref));
  store = NULL;
  payload = NULL;

  test_queue_wake_marker_path(root, "default", "jobs", marker_path,
                              sizeof(marker_path));
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(store->apply_queue_txn);
  assert_int_equal(test_count_queue_wake_markers(root), 0U);

  enqueue_opts.content_type = "text/plain";
  enqueue_opts.visibility_timeout_seconds = 30L;
  enqueue_opts.ttl_seconds = 3600L;
  enqueue_opts.max_attempts = 3;
  source = source_from_text("transactional-wake-payload");
  rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                              &enqueued, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  test_read_file_text(marker_path, marker_text, sizeof(marker_text));
  assert_non_null(strstr(marker_text, "sequence=1\n"));

  dequeue_opts.owner = "worker";
  dequeue_opts.txn_id = "txn-queue-wake";
  dequeue_opts.visibility_timeout_seconds = 30L;
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts, &payload,
                              &dequeued, &error);
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
  acked = 0;
  rc = store->ack_message(store, &ref, &acked, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(acked);
  test_read_file_text(marker_path, marker_text, sizeof(marker_text));
  assert_non_null(strstr(marker_text, "sequence=3\n"));

  rc = store->apply_queue_txn(store, "txn-queue-wake", 0, &error);
  assert_int_equal(rc, LC_OK);
  test_read_file_text(marker_path, marker_text, sizeof(marker_text));
  assert_non_null(strstr(marker_text, "sequence=4\n"));

  memset(&dequeue_opts, 0, sizeof(dequeue_opts));
  dequeue_opts.owner = "worker-2";
  dequeue_opts.visibility_timeout_seconds = 30L;
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts, &payload,
                              &redelivered, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(payload);
  lc_source_close(payload);
  payload = NULL;
  assert_string_equal(redelivered.message_id, enqueued.message_id);

  lc_pouch_queue_message_info_cleanup(&allocator, &redelivered);
  lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);
  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_queue_wake_marker_failure_does_not_rollback_enqueue(void **state) {
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
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts, &payload,
                              &dequeued, &error);
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

  rc = store->queue_wake_status(store, "default", "jobs/high", &status, &error);
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
  assert_string_equal(stats.correlation_id, "pouch-queue-stats");
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

static void
test_queue_enqueue_index_allocation_failure_replays_cleanly(void **state) {
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
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts, &payload,
                              &dequeued, &error);
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
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts, &payload,
                              &dequeued, &error);
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
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts, &payload,
                              &dequeued, &error);
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

  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts, &payload,
                              &dequeued, &error);
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

static void
test_queue_ttl_expiry_removes_pending_candidate_after_replay(void **state) {
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
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts, &payload,
                              &dequeued, &error);
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

static void test_queue_inflight_ttl_expiry_rejects_ack(void **state) {
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
  lc_pouch_queue_message_info extended;
  lc_pouch_queue_ref ref;
  lc_pouch_queue_stats stats;
  lc_error error;
  int acked;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-inflight-ttl-expired");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&dequeue_opts, 0, sizeof(dequeue_opts));
  memset(&enqueued, 0, sizeof(enqueued));
  memset(&dequeued, 0, sizeof(dequeued));
  memset(&nacked, 0, sizeof(nacked));
  memset(&extended, 0, sizeof(extended));
  memset(&ref, 0, sizeof(ref));
  memset(&stats, 0, sizeof(stats));
  store = NULL;
  payload = NULL;
  acked = 0;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);

  enqueue_opts.content_type = "text/plain";
  enqueue_opts.visibility_timeout_seconds = 30L;
  enqueue_opts.ttl_seconds = 1L;
  enqueue_opts.max_attempts = 3;
  source = source_from_text("expires-in-flight");
  rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                              &enqueued, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  dequeue_opts.owner = "worker-a";
  dequeue_opts.visibility_timeout_seconds = 30L;
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts, &payload,
                              &dequeued, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(payload);
  lc_source_close(payload);
  payload = NULL;
  assert_string_equal(dequeued.message_id, enqueued.message_id);

  ref.namespace_name = dequeued.namespace_name;
  ref.queue = dequeued.queue;
  ref.message_id = dequeued.message_id;
  ref.lease_id = dequeued.lease_id;
  ref.txn_id = dequeued.txn_id;
  ref.fencing_token = dequeued.fencing_token;
  ref.meta_etag = dequeued.meta_etag;

  sleep(2U);

  rc = store->queue_stats(store, "default", "jobs", &stats, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats.available, 0);
  assert_int_equal(stats.pending_candidates, 0);
  assert_null(stats.head_message_id);
  lc_pouch_queue_stats_cleanup(&allocator, &stats);

  rc = store->ack_message(store, &ref, &acked, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 409L);
  assert_string_equal(error.server_code, "queue_message_expired");
  assert_false(acked);
  lc_error_cleanup(&error);

  rc = store->nack_message(store, &ref, 0L, 1, &nacked, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 409L);
  assert_string_equal(error.server_code, "queue_message_expired");
  assert_null(nacked.message_id);
  lc_error_cleanup(&error);

  rc = store->extend_message(store, &ref, 30L, &extended, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 409L);
  assert_string_equal(error.server_code, "queue_message_expired");
  assert_null(extended.message_id);
  lc_error_cleanup(&error);

  lc_pouch_queue_message_info_cleanup(&allocator, &extended);
  lc_pouch_queue_message_info_cleanup(&allocator, &nacked);
  lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);
  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_queue_nack_allocation_failure_preserves_active_lease(void **state) {
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
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts, &payload,
                              &dequeued, &error);
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

static void
test_queue_extend_allocation_failure_preserves_active_lease(void **state) {
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
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts, &payload,
                              &dequeued, &error);
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

static void
test_queue_retry_exhaustion_is_not_pending_after_replay(void **state) {
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
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts, &payload,
                              &dequeued, &error);
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
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts, &payload,
                              &dequeued, &error);
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
  rc = first->get_object(first, ".lockd", "backend-id", &selector, &body, &info,
                         &error);
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
  char *backend_hash;
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
  backend_hash = NULL;
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

  rc = first->backend_hash(first, &backend_hash, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(backend_hash);
  lc_pouch_free(&allocator, backend_hash);
  backend_hash = NULL;

  object_opts.name = "decision";
  object_opts.content_type = "application/octet-stream";
  source = source_from_text("transaction-decision");
  rc = first->put_object(first, ".lockd-txn", "txn-key", source, &object_opts,
                         &object_info, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_object_info_cleanup(&allocator, &object_info);

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

static void test_fsync_stats_report_disk_sync_targets(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_fsync_stats before;
  lc_pouch_fsync_stats after;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res meta_res;
  lc_pouch_enqueue_opts enqueue_opts;
  lc_pouch_queue_message_info enqueued;
  lc_pouch_compaction_res compacted;
  lc_source *source;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "fsync-stats");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&before, 0, sizeof(before));
  memset(&after, 0, sizeof(after));
  memset(&meta, 0, sizeof(meta));
  memset(&meta_res, 0, sizeof(meta_res));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&enqueued, 0, sizeof(enqueued));
  memset(&compacted, 0, sizeof(compacted));
  store = NULL;
  source = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(store->fsync_stats);

  rc = store->fsync_stats(store, &before, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(before.attempted_fsyncs >= 1UL);
  assert_int_equal(before.failed_fsyncs, 0UL);
  assert_true(before.writer_marker_fsyncs >= 1UL);

  meta.owner = "owner-a";
  meta.lease_id = "lease-a";
  meta.txn_id = "txn-a";
  meta.state_etag = "state-a";
  meta.version = 1L;
  meta.fencing_token = 2L;
  rc = store->store_meta(store, "default", "fsync-key", &meta, NULL, &meta_res,
                         &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_store_meta_res_cleanup(&allocator, &meta_res);

  rc = store->fsync_stats(store, &after, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(after.log_fsyncs > before.log_fsyncs);
  assert_true(after.query_index_fsyncs > before.query_index_fsyncs);
  assert_true(after.writer_marker_fsyncs > before.writer_marker_fsyncs);
  assert_int_equal(after.failed_fsyncs, 0UL);
  before = after;

  enqueue_opts.content_type = "text/plain";
  enqueue_opts.ttl_seconds = 60L;
  enqueue_opts.max_attempts = 3;
  source = source_from_text("queue-body");
  rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                              &enqueued, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);

  rc = store->fsync_stats(store, &after, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(after.log_fsyncs > before.log_fsyncs);
  assert_true(after.queue_wake_fsyncs > before.queue_wake_fsyncs);
  assert_true(after.writer_marker_fsyncs > before.writer_marker_fsyncs);
  assert_int_equal(after.failed_fsyncs, 0UL);
  before = after;

  rc = store->compact(store, "force", &compacted, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_compaction_res_cleanup(&allocator, &compacted);

  rc = store->fsync_stats(store, &after, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(after.log_fsyncs > before.log_fsyncs);
  assert_true(after.query_index_fsyncs > before.query_index_fsyncs);
  assert_true(after.root_fsyncs > before.root_fsyncs);
  assert_true(after.writer_marker_fsyncs > before.writer_marker_fsyncs);
  assert_int_equal(after.failed_fsyncs, 0UL);
  assert_true(after.attempted_fsyncs >=
              after.log_fsyncs + after.query_index_fsyncs + after.root_fsyncs +
                  after.writer_marker_fsyncs + after.queue_wake_fsyncs);

  rc = store->fsync_stats(NULL, &after, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message, "fsync_stats requires store and out");
  lc_error_cleanup(&error);
  memset(&error, 0, sizeof(error));

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static int child_exit_code(pid_t pid);
static void child_process_backend_hash(const char *root, int start_fd);

static void
test_backend_hash_create_race_publishes_single_identity(void **state) {
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
  assert_int_equal(count_namespace_segment_records_of_type(
                       root, "%2elockd", TEST_POUCH_RECORD_OBJECT_PUT),
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
  lc_pouch_put_state_res stale_put;
  lc_pouch_state_info state_info;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res first_meta;
  lc_pouch_store_meta_res second_meta;
  lc_pouch_meta_record loaded_meta;
  lc_error error;
  char *text;
  int removed;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "shared-refresh");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&state_opts, 0, sizeof(state_opts));
  memset(&first_put, 0, sizeof(first_put));
  memset(&second_put, 0, sizeof(second_put));
  memset(&stale_put, 0, sizeof(stale_put));
  memset(&state_info, 0, sizeof(state_info));
  memset(&meta, 0, sizeof(meta));
  memset(&first_meta, 0, sizeof(first_meta));
  memset(&second_meta, 0, sizeof(second_meta));
  memset(&loaded_meta, 0, sizeof(loaded_meta));
  first = NULL;
  second = NULL;
  body = NULL;
  removed = 0;

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
  rc = second->write_state(second, "default", "shared-key", source, &state_opts,
                           &second_put, &error);
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

  rc = second->load_meta(second, "default", "shared-key", &loaded_meta, &error);
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

  state_opts.if_state_etag = first_put.new_state_etag;
  source = source_from_text("{\"owner\":\"stale\"}");
  rc = first->write_state(first, "default", "shared-key", source, &state_opts,
                          &stale_put, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 412L);
  lc_error_cleanup(&error);
  memset(&error, 0, sizeof(error));

  rc = second->remove_state(second, "default", "shared-key",
                            second_put.new_state_etag, &removed, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(removed);

  rc = first->read_state(first, "default", "shared-key", &body, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_true(state_info.no_content);
  assert_null(body);
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  rc = first->remove_state(first, "default", "shared-key",
                           second_put.new_state_etag, &removed, &error);
  assert_int_equal(rc, LC_ERR_SERVER);
  assert_int_equal(error.http_status, 412L);
  lc_error_cleanup(&error);
  memset(&error, 0, sizeof(error));

  lc_pouch_meta_record_cleanup(&allocator, &loaded_meta);
  lc_pouch_store_meta_res_cleanup(&allocator, &first_meta);
  lc_pouch_store_meta_res_cleanup(&allocator, &second_meta);
  lc_pouch_put_state_res_cleanup(&allocator, &first_put);
  lc_pouch_put_state_res_cleanup(&allocator, &second_put);
  lc_pouch_put_state_res_cleanup(&allocator, &stale_put);
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
  rc = second->store_meta(second, "default", "bravo", &meta, NULL, &second_meta,
                          &error);
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

static void
test_independent_handle_reopens_compacted_query_index(void **state) {
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
  rc = second->store_meta(second, "default", "shared-key", &meta, NULL, &stored,
                          &error);
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
    rc = second->store_meta(second, "default", "shared-key", &meta, stored.etag,
                            &updated, &error);
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

static void child_process_cas_update(const char *root,
                                     const char *expected_etag, int start_fd,
                                     const char *payload) {
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
  rc = store->dequeue_message(store, "default", "jobs", &opts, &body, &message,
                              &error);
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

static void
test_independent_processes_dequeue_single_message_once(void **state) {
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
  rc = lc_pouch_disk_open_with_options(root, &allocator, &opts, &store, &error);
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
  rc = lc_pouch_disk_open_with_options(root, &allocator, &opts, &store, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(store);
  assert_string_equal(error.message,
                      "pouch disk query_engine must be index or scan");
  lc_error_cleanup(&error);

  memset(&error, 0, sizeof(error));
  opts.query_engine = "index";
  opts.query_fallback_engine = "linear";
  rc = lc_pouch_disk_open_with_options(root, &allocator, &opts, &store, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(store);
  assert_string_equal(
      error.message,
      "pouch disk query_fallback_engine must be none, index, or scan");
  lc_error_cleanup(&error);

  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  opts.single_writer = 2;
  rc = lc_pouch_disk_open_with_options(root, &allocator, &opts, &store, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(store);
  assert_string_equal(error.message,
                      "pouch disk single_writer must be 0 or 1");
  lc_error_cleanup(&error);

  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  opts.background_compaction = 2;
  rc = lc_pouch_disk_open_with_options(root, &allocator, &opts, &store, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(store);
  assert_string_equal(error.message,
                      "pouch disk background_compaction must be 0 or 1");
  lc_error_cleanup(&error);

  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  opts.background_compaction_obsolete_multiplier = 1UL;
  rc = lc_pouch_disk_open_with_options(root, &allocator, &opts, &store, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_null(store);
  assert_string_equal(
      error.message,
      "pouch disk background_compaction_obsolete_multiplier must be 0 or at "
      "least 2");
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_open_removes_stale_compaction_temps(void **state) {
  char root[256];
  char temp_log[512];
  char temp_query[512];
  char temp_query_internal[512];
  char path[512];
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
  snprintf(path, sizeof(path), "%s/%%2elockd", root);
  assert_int_equal(mkdir(path, 0777), 0);
  snprintf(path, sizeof(path), "%s/%%2elockd/logstore", root);
  assert_int_equal(mkdir(path, 0777), 0);
  test_query_index_path(root, temp_query_internal, sizeof(temp_query_internal));
  strncat(temp_query_internal, ".compact.tmp",
          sizeof(temp_query_internal) - strlen(temp_query_internal) - 1U);
  test_write_marker_file(temp_log);
  test_write_marker_file(temp_query);
  test_write_marker_file(temp_query_internal);

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(store);
  assert_int_equal(access(temp_log, F_OK), -1);
  assert_int_equal(errno, ENOENT);
  errno = 0;
  assert_int_equal(access(temp_query, F_OK), -1);
  assert_int_equal(errno, ENOENT);
  errno = 0;
  assert_int_equal(access(temp_query_internal, F_OK), -1);
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
  assert_int_equal(status.stale_marker_count, 0U);
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
  assert_int_equal(status.stale_marker_count, 0U);
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

static void test_writer_status_classifies_stale_markers(void **state) {
  char root[256];
  char stale_path[512];
  char legacy_path[512];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_writer_status status;
  lc_error error;
  time_t now;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "writer-status-stale");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&status, 0, sizeof(status));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  now = time(NULL);

  snprintf(stale_path, sizeof(stale_path), "%s/%sstale-heartbeat.marker", root,
           TEST_POUCH_WRITER_MARKER_PREFIX);
  test_write_text_file(stale_path, "pid=999\nsequence=1\nupdated_at_unix=1\n");
  test_set_file_mtime(stale_path, now + 3600);

  snprintf(legacy_path, sizeof(legacy_path), "%s/%slegacy.marker", root,
           TEST_POUCH_WRITER_MARKER_PREFIX);
  test_write_text_file(legacy_path, "legacy marker without heartbeat\n");
  test_set_file_mtime(legacy_path, now);

  rc = store->writer_status(store, &status, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(status.own_marker_present);
  assert_int_equal(status.active_marker_count, 2U);
  assert_int_equal(status.other_marker_count, 1U);
  assert_int_equal(status.stale_marker_count, 1U);
  lc_pouch_writer_status_cleanup(&allocator, &status);

  test_set_file_mtime(legacy_path, now - 3600);
  rc = store->writer_status(store, &status, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(status.own_marker_present);
  assert_int_equal(status.active_marker_count, 1U);
  assert_int_equal(status.other_marker_count, 0U);
  assert_int_equal(status.stale_marker_count, 2U);
  lc_pouch_writer_status_cleanup(&allocator, &status);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_lock_status_reports_global_writer_lock_counters(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *first;
  lc_pouch_store *second;
  lc_source *source;
  lc_source *body;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res put_res;
  lc_pouch_state_info state_info;
  lc_pouch_lock_status status;
  lc_pouch_compaction_res compact;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "lock-status");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&state_info, 0, sizeof(state_info));
  memset(&status, 0, sizeof(status));
  memset(&compact, 0, sizeof(compact));
  first = NULL;
  second = NULL;
  body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &first, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_disk_open(root, &allocator, &second, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(first->lock_status);

  rc = first->lock_status(first, &status, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(status.mode, "key-striped-fcntl");
  assert_non_null(strstr(status.path, "writer.lock"));
  assert_true(status.uses_fcntl_byte_range_lock);
  assert_true(status.uses_global_writer_lock);
  assert_true(status.uses_per_key_lock_cache);
  assert_true(status.lock_acquisitions >= 1UL);
  assert_true(status.lock_releases <= status.lock_acquisitions);
  lc_pouch_lock_status_cleanup(&allocator, &status);

  opts.content_type = "text/plain";
  source = source_from_text("lock-status");
  rc = first->write_state(first, "default", "lock-key", source, &opts, &put_res,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);

  rc = first->lock_status(first, &status, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(status.lock_acquisitions >= 2UL);
  assert_true(status.lock_releases >= 2UL);
  assert_true(status.replay_refreshes >= 1UL);
  assert_int_equal(status.log_reopens, 0UL);
  lc_pouch_lock_status_cleanup(&allocator, &status);

  rc = first->compact(first, "force", &compact, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_compaction_res_cleanup(&allocator, &compact);

  rc = second->read_state(second, "default", "lock-key", &body, &state_info,
                          &error);
  assert_int_equal(rc, LC_OK);
  assert_false(state_info.no_content);
  assert_non_null(body);
  lc_source_close(body);
  body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  rc = second->lock_status(second, &status, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(status.log_reopens, 0UL);
  assert_true(status.replay_refreshes >= 1UL);
  lc_pouch_lock_status_cleanup(&allocator, &status);

  tracked.fail_malloc_size = strlen("key-striped-fcntl") + 1U;
  rc = first->lock_status(first, &status, &error);
  assert_int_equal(rc, LC_ERR_NOMEM);
  assert_string_equal(error.message, "failed to copy pouch lock status");
  assert_null(status.mode);
  assert_null(status.path);
  tracked.fail_malloc_size = 0U;
  lc_error_cleanup(&error);

  rc = second->close(second, &error);
  assert_int_equal(rc, LC_OK);
  rc = first->close(first, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_lock_key_path_escapes_namespace_and_key(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_error error;
  char *path;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "lock-key-path");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  store = NULL;
  path = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(store->lock_key_path);

  rc =
      store->lock_key_path(store, "default", "alpha/beta.gamma", &path, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(path);
  assert_non_null(strstr(path, "/locks/default/alpha%2fbeta%2egamma"));
  lc_pouch_free(&allocator, path);
  path = NULL;

  rc = store->lock_key_path(store, "name-space_1", "key:with spaces/%", &path,
                            &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(
      strstr(path, "/locks/name-space_1/key%3awith%20spaces%2f%25"));
  lc_pouch_free(&allocator, path);
  path = NULL;

  rc = store->lock_key_path(store, "default", "bad//key", &path, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "lock_key_path key must not contain empty path "
                      "components");
  assert_null(path);
  lc_error_cleanup(&error);

  rc = store->lock_key_path(store, "bad/ns", "key", &path, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_string_equal(error.message,
                      "lock_key_path namespace must not contain '/'");
  assert_null(path);
  lc_error_cleanup(&error);

  tracked.fail_malloc_size = strlen(root) + strlen("/locks/default/key") + 1U;
  rc = store->lock_key_path(store, "default", "key", &path, &error);
  assert_int_equal(rc, LC_ERR_NOMEM);
  assert_string_equal(error.message, "failed to allocate pouch lock key path");
  assert_null(path);
  tracked.fail_malloc_size = 0U;
  lc_error_cleanup(&error);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void child_process_hold_key_lock(const char *root, int ready_fd,
                                        int release_fd) {
  lc_pouch_store *store;
  lc_pouch_key_lock *lock;
  lc_error error;
  char release;
  int acquired;
  int rc;

  store = NULL;
  lock = NULL;
  memset(&error, 0, sizeof(error));
  rc = lc_pouch_disk_open(root, NULL, &store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    _exit(60);
  }
  rc = store->try_lock_key(store, "default", "alpha/beta", &lock, &acquired,
                           &error);
  if (rc != LC_OK || !acquired || lock == NULL) {
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(61);
  }
  if (write(ready_fd, "r", 1U) != 1) {
    store->unlock_key(store, lock, NULL);
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(62);
  }
  close(ready_fd);
  if (read(release_fd, &release, 1U) != 1) {
    store->unlock_key(store, lock, NULL);
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(63);
  }
  close(release_fd);
  rc = store->unlock_key(store, lock, &error);
  if (rc != LC_OK) {
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(64);
  }
  rc = store->close(store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    _exit(65);
  }
  lc_error_cleanup(&error);
  _exit(0);
}

static void child_process_probe_key_locks(const char *root) {
  lc_pouch_store *store;
  lc_pouch_key_lock *lock;
  lc_error error;
  int acquired;
  int rc;

  store = NULL;
  lock = NULL;
  memset(&error, 0, sizeof(error));
  rc = lc_pouch_disk_open(root, NULL, &store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    _exit(70);
  }
  acquired = 1;
  rc = store->try_lock_key(store, "default", "alpha/beta", &lock, &acquired,
                           &error);
  if (rc != LC_OK || acquired || lock != NULL) {
    if (lock != NULL) {
      store->unlock_key(store, lock, NULL);
    }
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(71);
  }
  rc = store->try_lock_key(store, "default", "alpha/gamma", &lock, &acquired,
                           &error);
  if (rc != LC_OK || !acquired || lock == NULL) {
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(72);
  }
  rc = store->unlock_key(store, lock, &error);
  if (rc != LC_OK) {
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(73);
  }
  rc = store->close(store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    _exit(74);
  }
  lc_error_cleanup(&error);
  _exit(0);
}

static void child_process_write_state_after_ready(const char *root,
                                                  int start_fd, int ready_fd) {
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res put_res;
  lc_error error;
  char start;
  int rc;

  store = NULL;
  source = NULL;
  memset(&opts, 0, sizeof(opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&error, 0, sizeof(error));
  rc = lc_pouch_disk_open(root, NULL, &store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    _exit(80);
  }
  opts.content_type = "application/json";
  rc = lc_source_from_memory("{\"owner\":\"blocked-writer\"}",
                             strlen("{\"owner\":\"blocked-writer\"}"), &source,
                             &error);
  if (rc != LC_OK) {
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(81);
  }
  if (read(start_fd, &start, 1U) != 1) {
    lc_source_close(source);
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(82);
  }
  close(start_fd);
  if (write(ready_fd, "r", 1U) != 1) {
    lc_source_close(source);
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(83);
  }
  close(ready_fd);
  rc = store->write_state(store, "default", "process-key", source, &opts,
                          &put_res, &error);
  lc_source_close(source);
  lc_pouch_put_state_res_cleanup(NULL, &put_res);
  store->close(store, NULL);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    _exit(84);
  }
  lc_error_cleanup(&error);
  _exit(0);
}

static void child_process_store_meta_after_ready(const char *root, int start_fd,
                                                 int ready_fd) {
  lc_pouch_store *store;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res meta_res;
  lc_error error;
  char start;
  int rc;

  store = NULL;
  memset(&meta, 0, sizeof(meta));
  memset(&meta_res, 0, sizeof(meta_res));
  memset(&error, 0, sizeof(error));
  rc = lc_pouch_disk_open(root, NULL, &store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    _exit(90);
  }
  meta.owner = "blocked-meta-writer";
  meta.lease_id = "blocked-meta-lease";
  meta.version = 1L;
  if (read(start_fd, &start, 1U) != 1) {
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(91);
  }
  close(start_fd);
  if (write(ready_fd, "r", 1U) != 1) {
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(92);
  }
  close(ready_fd);
  rc = store->store_meta(store, "default", "meta-key", &meta, NULL, &meta_res,
                         &error);
  lc_pouch_store_meta_res_cleanup(NULL, &meta_res);
  store->close(store, NULL);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    _exit(93);
  }
  lc_error_cleanup(&error);
  _exit(0);
}

static void child_process_put_object_after_ready(const char *root, int start_fd,
                                                 int ready_fd) {
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_object_opts opts;
  lc_pouch_object_info info;
  lc_error error;
  char start;
  int rc;

  store = NULL;
  source = NULL;
  memset(&opts, 0, sizeof(opts));
  memset(&info, 0, sizeof(info));
  memset(&error, 0, sizeof(error));
  rc = lc_pouch_disk_open(root, NULL, &store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    _exit(100);
  }
  opts.name = "artifact.txt";
  opts.content_type = "text/plain";
  rc = lc_source_from_memory("blocked-object", strlen("blocked-object"),
                             &source, &error);
  if (rc != LC_OK) {
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(101);
  }
  if (read(start_fd, &start, 1U) != 1) {
    lc_source_close(source);
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(102);
  }
  close(start_fd);
  if (write(ready_fd, "r", 1U) != 1) {
    lc_source_close(source);
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(103);
  }
  close(ready_fd);
  rc = store->put_object(store, "default", "object-key", source, &opts, &info,
                         &error);
  lc_source_close(source);
  lc_pouch_object_info_cleanup(NULL, &info);
  store->close(store, NULL);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    _exit(104);
  }
  lc_error_cleanup(&error);
  _exit(0);
}

static void child_process_copy_object_after_ready(const char *root,
                                                  int start_fd, int ready_fd) {
  lc_pouch_store *store;
  lc_pouch_copy_object_opts opts;
  lc_pouch_object_info info;
  lc_error error;
  char start;
  int rc;

  store = NULL;
  memset(&opts, 0, sizeof(opts));
  memset(&info, 0, sizeof(info));
  memset(&error, 0, sizeof(error));
  rc = lc_pouch_disk_open(root, NULL, &store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    _exit(110);
  }
  opts.source.name = "artifact.txt";
  opts.name = "copy.txt";
  opts.prevent_overwrite = 1;
  if (read(start_fd, &start, 1U) != 1) {
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(111);
  }
  close(start_fd);
  if (write(ready_fd, "r", 1U) != 1) {
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(112);
  }
  close(ready_fd);
  rc = store->copy_object(store, "default", "source-key", "copy-key", &opts,
                          &info, &error);
  lc_pouch_object_info_cleanup(NULL, &info);
  store->close(store, NULL);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    _exit(113);
  }
  lc_error_cleanup(&error);
  _exit(0);
}

static void child_process_enqueue_after_ready(const char *root, int start_fd,
                                              int ready_fd) {
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_enqueue_opts opts;
  lc_pouch_queue_message_info info;
  lc_error error;
  char start;
  int rc;

  store = NULL;
  source = NULL;
  memset(&opts, 0, sizeof(opts));
  memset(&info, 0, sizeof(info));
  memset(&error, 0, sizeof(error));
  rc = lc_pouch_disk_open(root, NULL, &store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    _exit(120);
  }
  opts.content_type = "text/plain";
  opts.visibility_timeout_seconds = 30L;
  opts.ttl_seconds = 3600L;
  opts.max_attempts = 3;
  rc = lc_source_from_memory("queued-blocked", strlen("queued-blocked"),
                             &source, &error);
  if (rc != LC_OK) {
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(121);
  }
  if (read(start_fd, &start, 1U) != 1) {
    lc_source_close(source);
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(122);
  }
  close(start_fd);
  if (write(ready_fd, "r", 1U) != 1) {
    lc_source_close(source);
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(123);
  }
  close(ready_fd);
  rc = store->enqueue_message(store, "default", "jobs", source, &opts, &info,
                              &error);
  lc_source_close(source);
  lc_pouch_queue_message_info_cleanup(NULL, &info);
  store->close(store, NULL);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    _exit(124);
  }
  lc_error_cleanup(&error);
  _exit(0);
}

static void child_process_dequeue_after_ready(const char *root, int start_fd,
                                              int ready_fd) {
  lc_pouch_store *store;
  lc_source *body;
  lc_pouch_dequeue_opts opts;
  lc_pouch_queue_message_info info;
  lc_error error;
  char start;
  int rc;

  store = NULL;
  body = NULL;
  memset(&opts, 0, sizeof(opts));
  memset(&info, 0, sizeof(info));
  memset(&error, 0, sizeof(error));
  rc = lc_pouch_disk_open(root, NULL, &store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    _exit(130);
  }
  opts.owner = "blocked-worker";
  opts.visibility_timeout_seconds = 30L;
  if (read(start_fd, &start, 1U) != 1) {
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(131);
  }
  close(start_fd);
  if (write(ready_fd, "r", 1U) != 1) {
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(132);
  }
  close(ready_fd);
  rc = store->dequeue_message(store, "default", "jobs", &opts, &body, &info,
                              &error);
  if (body != NULL) {
    lc_source_close(body);
  }
  lc_pouch_queue_message_info_cleanup(NULL, &info);
  store->close(store, NULL);
  if (rc != LC_OK || body == NULL) {
    lc_error_cleanup(&error);
    _exit(133);
  }
  lc_error_cleanup(&error);
  _exit(0);
}

static void child_process_promote_staged_after_ready(const char *root,
                                                     int start_fd,
                                                     int ready_fd) {
  lc_pouch_store *store;
  lc_pouch_put_state_res promoted;
  lc_error error;
  char start;
  int rc;

  store = NULL;
  memset(&promoted, 0, sizeof(promoted));
  memset(&error, 0, sizeof(error));
  rc = lc_pouch_disk_open(root, NULL, &store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    _exit(140);
  }
  if (read(start_fd, &start, 1U) != 1) {
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(141);
  }
  close(start_fd);
  if (write(ready_fd, "r", 1U) != 1) {
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(142);
  }
  close(ready_fd);
  rc = store->promote_staged_state(store, "default", "lease-key", "txn-block",
                                   NULL, &promoted, &error);
  lc_pouch_put_state_res_cleanup(NULL, &promoted);
  store->close(store, NULL);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    _exit(143);
  }
  lc_error_cleanup(&error);
  _exit(0);
}

static void child_process_discard_staged_after_ready(const char *root,
                                                     int start_fd,
                                                     int ready_fd) {
  lc_pouch_store *store;
  lc_error error;
  char start;
  int rc;

  store = NULL;
  memset(&error, 0, sizeof(error));
  rc = lc_pouch_disk_open(root, NULL, &store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    _exit(150);
  }
  if (read(start_fd, &start, 1U) != 1) {
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(151);
  }
  close(start_fd);
  if (write(ready_fd, "r", 1U) != 1) {
    store->close(store, NULL);
    lc_error_cleanup(&error);
    _exit(152);
  }
  close(ready_fd);
  rc = store->discard_staged_state(store, "default", "lease-key", "txn-block",
                                   NULL, &error);
  store->close(store, NULL);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    _exit(153);
  }
  lc_error_cleanup(&error);
  _exit(0);
}

static void test_thread_write_signal(int fd, const char *value) {
  ssize_t written;

  do {
    written = write(fd, value, 1U);
  } while (written < 0 && errno == EINTR);
  (void)written;
}

static void *thread_write_state_after_ready(void *arg) {
  test_same_process_key_lock_thread *ctx;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res put_res;
  lc_error error;
  char start;
  int rc;

  ctx = (test_same_process_key_lock_thread *)arg;
  store = NULL;
  source = NULL;
  memset(&opts, 0, sizeof(opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&error, 0, sizeof(error));
  ctx->result = 200;
  rc = lc_pouch_disk_open(ctx->root, NULL, &store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    ctx->result = rc;
    test_thread_write_signal(ctx->done_fd, "e");
    return NULL;
  }
  opts.content_type = "text/plain";
  rc = lc_source_from_memory("same-process-thread",
                             strlen("same-process-thread"), &source, &error);
  if (rc != LC_OK) {
    store->close(store, NULL);
    lc_error_cleanup(&error);
    ctx->result = rc;
    test_thread_write_signal(ctx->done_fd, "e");
    return NULL;
  }
  if (read(ctx->start_fd, &start, 1U) != 1) {
    lc_source_close(source);
    store->close(store, NULL);
    lc_error_cleanup(&error);
    ctx->result = 201;
    test_thread_write_signal(ctx->done_fd, "e");
    return NULL;
  }
  close(ctx->start_fd);
  ctx->start_fd = -1;
  if (write(ctx->ready_fd, "r", 1U) != 1) {
    lc_source_close(source);
    store->close(store, NULL);
    lc_error_cleanup(&error);
    ctx->result = 202;
    test_thread_write_signal(ctx->done_fd, "e");
    return NULL;
  }
  close(ctx->ready_fd);
  ctx->ready_fd = -1;
  rc = store->write_state(store, "default", "thread-key", source, &opts,
                          &put_res, &error);
  lc_source_close(source);
  lc_pouch_put_state_res_cleanup(NULL, &put_res);
  store->close(store, NULL);
  lc_error_cleanup(&error);
  ctx->result = rc;
  test_thread_write_signal(ctx->done_fd, "d");
  return NULL;
}

static void test_try_lock_key_serializes_same_process_handles(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *first;
  lc_pouch_store *second;
  lc_pouch_key_lock *first_lock;
  lc_pouch_key_lock *second_lock;
  lc_pouch_lock_status status;
  lc_error error;
  int acquired;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "key-lock-contention");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&status, 0, sizeof(status));
  first = NULL;
  second = NULL;
  first_lock = NULL;
  second_lock = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &first, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_disk_open(root, &allocator, &second, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(first->try_lock_key);
  assert_non_null(first->unlock_key);
  assert_non_null(second->try_lock_key);
  assert_non_null(second->unlock_key);

  rc = first->try_lock_key(first, "default", "alpha/beta", &first_lock,
                           &acquired, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(acquired);
  assert_non_null(first_lock);

  acquired = 1;
  rc = second->try_lock_key(second, "default", "alpha/beta", &second_lock,
                            &acquired, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(acquired);
  assert_null(second_lock);

  rc = second->lock_status(second, &status, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(status.mode, "key-striped-fcntl");
  assert_true(status.key_lock_stripe_count > 0U);
  assert_true(status.process_active_key_locks >= 1U);
  assert_true(status.process_key_lock_contentions >= 1UL);
  lc_pouch_lock_status_cleanup(&allocator, &status);

  rc = second->try_lock_key(second, "default", "alpha/gamma", &second_lock,
                            &acquired, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(acquired);
  assert_non_null(second_lock);

  rc = second->unlock_key(second, second_lock, &error);
  assert_int_equal(rc, LC_OK);
  second_lock = NULL;
  rc = first->unlock_key(first, first_lock, &error);
  assert_int_equal(rc, LC_OK);
  first_lock = NULL;

  rc = second->try_lock_key(second, "default", "alpha/beta", &second_lock,
                            &acquired, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(acquired);
  assert_non_null(second_lock);

  rc = second->lock_status(second, &status, &error);
  assert_int_equal(rc, LC_OK);
  assert_string_equal(status.mode, "key-striped-fcntl");
  assert_true(status.key_lock_stripe_count > 0U);
  assert_true(status.process_active_key_locks >= 1U);
  assert_true(status.lock_acquisitions >= 2UL);
  assert_true(status.process_key_lock_contentions >= 1UL);
  lc_pouch_lock_status_cleanup(&allocator, &status);

  rc = second->unlock_key(second, second_lock, &error);
  assert_int_equal(rc, LC_OK);
  second_lock = NULL;
  rc = second->close(second, &error);
  assert_int_equal(rc, LC_OK);
  rc = first->close(first, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_lock_fd_cache_reuses_released_key_descriptors(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_store *second;
  lc_pouch_key_lock *lock;
  lc_pouch_lock_fd_cache_status status;
  lc_pouch_lock_fd_cache_status baseline;
  lc_error error;
  char key[32];
  size_t index;
  int acquired;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "lock-fd-cache");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&status, 0, sizeof(status));
  memset(&baseline, 0, sizeof(baseline));
  store = NULL;
  second = NULL;
  lock = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_disk_open(root, &allocator, &second, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(store->lock_fd_cache_status);

  rc = store->lock_fd_cache_status(store, &status, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(status.capacity, 32U);
  assert_int_equal(status.entries, 0U);
  baseline = status;

  rc = store->try_lock_key(store, "default", "cached-key", &lock, &acquired,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_true(acquired);
  assert_non_null(lock);
  rc = store->lock_fd_cache_status(store, &status, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(status.entries, 0U);
  assert_int_equal(status.hits, baseline.hits);
  assert_int_equal(status.misses, baseline.misses + 1UL);

  rc = store->unlock_key(store, lock, &error);
  assert_int_equal(rc, LC_OK);
  lock = NULL;
  rc = store->lock_fd_cache_status(store, &status, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(status.entries, 1U);

  rc = store->try_lock_key(store, "default", "cached-key", &lock, &acquired,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_true(acquired);
  assert_non_null(lock);
  rc = store->lock_fd_cache_status(store, &status, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(status.entries, 0U);
  assert_int_equal(status.hits, baseline.hits + 1UL);
  assert_int_equal(status.misses, baseline.misses + 1UL);
  rc = store->unlock_key(store, lock, &error);
  assert_int_equal(rc, LC_OK);
  lock = NULL;

  rc = second->try_lock_key(second, "default", "cached-key", &lock, &acquired,
                            &error);
  assert_int_equal(rc, LC_OK);
  assert_true(acquired);
  assert_non_null(lock);
  rc = second->lock_fd_cache_status(second, &status, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(status.entries, 0U);
  assert_int_equal(status.hits, baseline.hits + 2UL);
  assert_int_equal(status.misses, baseline.misses + 1UL);
  rc = second->unlock_key(second, lock, &error);
  assert_int_equal(rc, LC_OK);
  lock = NULL;

  for (index = 0U; index < 40U; ++index) {
    snprintf(key, sizeof(key), "key-%02lu", (unsigned long)index);
    rc = store->try_lock_key(store, "default", key, &lock, &acquired, &error);
    assert_int_equal(rc, LC_OK);
    assert_true(acquired);
    assert_non_null(lock);
    rc = store->unlock_key(store, lock, &error);
    assert_int_equal(rc, LC_OK);
    lock = NULL;
  }

  rc = store->lock_fd_cache_status(store, &status, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(status.capacity, 32U);
  assert_int_equal(status.entries, 32U);
  assert_true(status.evictions >= baseline.evictions + 9UL);
  assert_true(status.closes >= status.evictions);

  rc = second->close(second, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_read_fd_cache_reuses_descriptors_without_closing_active_readers(
    void **state) {
  char root[256];
  char key[32];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *active;
  lc_source *sources[40];
  lc_pouch_put_state_res put_res;
  lc_pouch_state_info state_info;
  lc_pouch_compaction_res compact;
  lc_pouch_read_fd_cache_status status;
  lc_pouch_read_fd_cache_status baseline;
  lc_pouch_read_fd_cache_status before_compact;
  lc_error error;
  char *text;
  size_t index;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "read-fd-cache");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&put_res, 0, sizeof(put_res));
  memset(&state_info, 0, sizeof(state_info));
  memset(&compact, 0, sizeof(compact));
  memset(&status, 0, sizeof(status));
  memset(&baseline, 0, sizeof(baseline));
  memset(&before_compact, 0, sizeof(before_compact));
  memset(sources, 0, sizeof(sources));
  store = NULL;
  active = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(store->read_fd_cache_status);

  source = source_from_text("active-payload");
  rc = store->write_state(store, "default", "active", source, NULL, &put_res,
                          &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  for (index = 0U; index < 40U; ++index) {
    snprintf(key, sizeof(key), "read-%02lu", (unsigned long)index);
    source = source_from_text("cached-payload");
    rc = store->write_state(store, "default", key, source, NULL, &put_res,
                            &error);
    lc_source_close(source);
    assert_int_equal(rc, LC_OK);
    lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  }

  rc = store->read_fd_cache_status(store, &status, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(status.capacity, 32U);
  assert_int_equal(status.entries, 0U);
  baseline = status;

  rc = store->read_state(store, "default", "active", &active, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(active);
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  for (index = 0U; index < 40U; ++index) {
    snprintf(key, sizeof(key), "read-%02lu", (unsigned long)index);
    rc = store->read_state(store, "default", key, &sources[index], &state_info,
                           &error);
    assert_int_equal(rc, LC_OK);
    assert_non_null(sources[index]);
    lc_pouch_state_info_cleanup(&allocator, &state_info);
  }
  rc = store->read_fd_cache_status(store, &status, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(status.entries, 0U);
  assert_int_equal(status.misses, baseline.misses + 41UL);

  for (index = 0U; index < 40U; ++index) {
    lc_source_close(sources[index]);
    sources[index] = NULL;
  }
  rc = store->read_fd_cache_status(store, &status, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(status.entries, 32U);
  assert_true(status.evictions >= baseline.evictions + 8UL);
  assert_true(status.closes >= status.evictions);

  text = read_source_text(active);
  assert_string_equal(text, "active-payload");
  free(text);
  lc_source_close(active);
  active = NULL;

  rc = store->read_state(store, "default", "read-00", &source, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(source);
  rc = store->read_fd_cache_status(store, &status, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(status.hits >= baseline.hits + 1UL);
  lc_source_close(source);
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  rc = store->read_fd_cache_status(store, &before_compact, &error);
  assert_int_equal(rc, LC_OK);
  rc = store->compact(store, "force", &compact, &error);
  assert_int_equal(rc, LC_OK);
  lc_pouch_compaction_res_cleanup(&allocator, &compact);

  rc = store->read_state(store, "default", "active", &source, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(source);
  text = read_source_text(source);
  assert_string_equal(text, "active-payload");
  free(text);
  lc_source_close(source);
  lc_pouch_state_info_cleanup(&allocator, &state_info);
  rc = store->read_fd_cache_status(store, &status, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(status.hits >= before_compact.hits);
  assert_true(status.misses >= before_compact.misses);
  assert_true(status.stale >= before_compact.stale);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_read_source_survives_store_close_without_cache_owner(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *body;
  lc_pouch_put_state_res put_res;
  lc_pouch_state_info state_info;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "read-source-after-close");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&put_res, 0, sizeof(put_res));
  memset(&state_info, 0, sizeof(state_info));
  store = NULL;
  body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("survives-close");
  rc = store->write_state(store, "default", "kept-source", source, NULL,
                          &put_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);

  rc = store->read_state(store, "default", "kept-source", &body, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(body);
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  text = read_source_text(body);
  assert_string_equal(text, "survives-close");
  free(text);
  lc_source_close(body);

  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_object_source_survives_store_close_without_cache_owner(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *body;
  lc_pouch_put_object_opts put_opts;
  lc_pouch_object_selector selector;
  lc_pouch_object_info put_info;
  lc_pouch_object_info get_info;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "object-source-after-close");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&put_opts, 0, sizeof(put_opts));
  memset(&selector, 0, sizeof(selector));
  memset(&put_info, 0, sizeof(put_info));
  memset(&get_info, 0, sizeof(get_info));
  store = NULL;
  body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  put_opts.name = "blob";
  put_opts.content_type = "text/plain";
  source = source_from_text("object-survives-close");
  rc = store->put_object(store, "default", "kept-object", source, &put_opts,
                         &put_info, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  selector.name = "blob";
  rc = store->get_object(store, "default", "kept-object", &selector, &body,
                         &get_info, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(body);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  text = read_source_text(body);
  assert_string_equal(text, "object-survives-close");
  free(text);
  lc_source_close(body);

  lc_pouch_object_info_cleanup(&allocator, &get_info);
  lc_pouch_object_info_cleanup(&allocator, &put_info);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_queue_source_survives_store_close_without_cache_owner(void **state) {
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
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "queue-source-after-close");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&dequeue_opts, 0, sizeof(dequeue_opts));
  memset(&enqueued, 0, sizeof(enqueued));
  memset(&dequeued, 0, sizeof(dequeued));
  store = NULL;
  body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  enqueue_opts.content_type = "text/plain";
  enqueue_opts.visibility_timeout_seconds = 30L;
  enqueue_opts.ttl_seconds = 60L;
  enqueue_opts.max_attempts = 3;
  source = source_from_text("queue-survives-close");
  rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                              &enqueued, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);

  dequeue_opts.owner = "worker";
  dequeue_opts.visibility_timeout_seconds = 30L;
  rc = store->dequeue_message(store, "default", "jobs", &dequeue_opts, &body,
                              &dequeued, &error);
  assert_int_equal(rc, LC_OK);
  assert_non_null(body);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  text = read_source_text(body);
  assert_string_equal(text, "queue-survives-close");
  free(text);
  lc_source_close(body);

  lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);
  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_key_lock_wait_serializes_same_process_threads(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_key_lock *lock;
  lc_source *body;
  lc_pouch_state_info state_info;
  lc_error error;
  test_same_process_key_lock_thread ctx;
  pthread_t thread;
  int start_pipe[2];
  int ready_pipe[2];
  int done_pipe[2];
  char byte;
  int acquired;
  int flags;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "key-lock-thread-contention");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&state_info, 0, sizeof(state_info));
  memset(&ctx, 0, sizeof(ctx));
  store = NULL;
  lock = NULL;
  body = NULL;
  start_pipe[0] = -1;
  start_pipe[1] = -1;
  ready_pipe[0] = -1;
  ready_pipe[1] = -1;
  done_pipe[0] = -1;
  done_pipe[1] = -1;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = pipe(start_pipe);
  assert_int_equal(rc, 0);
  rc = pipe(ready_pipe);
  assert_int_equal(rc, 0);
  rc = pipe(done_pipe);
  assert_int_equal(rc, 0);
  flags = fcntl(done_pipe[0], F_GETFL, 0);
  assert_true(flags >= 0);
  rc = fcntl(done_pipe[0], F_SETFL, flags | O_NONBLOCK);
  assert_int_equal(rc, 0);

  rc = store->try_lock_key(store, "default", "thread-key", &lock, &acquired,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_true(acquired);
  assert_non_null(lock);

  ctx.root = root;
  ctx.start_fd = start_pipe[0];
  ctx.ready_fd = ready_pipe[1];
  ctx.done_fd = done_pipe[1];
  ctx.result = 199;
  rc = pthread_create(&thread, NULL, thread_write_state_after_ready, &ctx);
  assert_int_equal(rc, 0);
  assert_int_equal(write(start_pipe[1], "x", 1U), 1);
  close(start_pipe[1]);
  start_pipe[1] = -1;
  assert_int_equal(read(ready_pipe[0], &byte, 1U), 1);
  close(ready_pipe[0]);
  ready_pipe[0] = -1;

  test_sleep_for_lock_wait();
  rc = read(done_pipe[0], &byte, 1U);
  assert_int_equal(rc, -1);
  assert_int_equal(errno, EAGAIN);

  rc = store->unlock_key(store, lock, &error);
  assert_int_equal(rc, LC_OK);
  lock = NULL;
  rc = pthread_join(thread, NULL);
  assert_int_equal(rc, 0);
  assert_int_equal(ctx.result, LC_OK);
  assert_int_equal(read(done_pipe[0], &byte, 1U), 1);
  close(done_pipe[0]);
  done_pipe[0] = -1;
  close(done_pipe[1]);
  done_pipe[1] = -1;

  rc = store->read_state(store, "default", "thread-key", &body, &state_info,
                         &error);
  assert_int_equal(rc, LC_OK);
  assert_false(state_info.no_content);
  assert_non_null(body);
  lc_source_close(body);
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_try_lock_key_serializes_cross_process_handles(void **state) {
  char root[256];
  int ready_pipe[2];
  int release_pipe[2];
  char ready;
  int holder_code;
  int probe_code;
  int rc;
  pid_t holder_pid;
  pid_t probe_pid;

  (void)state;
  test_root_path(root, sizeof(root), "key-lock-process-contention");
  test_cleanup_root(root);
  ready_pipe[0] = -1;
  ready_pipe[1] = -1;
  release_pipe[0] = -1;
  release_pipe[1] = -1;

  rc = pipe(ready_pipe);
  assert_int_equal(rc, 0);
  rc = pipe(release_pipe);
  assert_int_equal(rc, 0);

  holder_pid = fork();
  assert_true(holder_pid >= 0);
  if (holder_pid == 0) {
    close(ready_pipe[0]);
    close(release_pipe[1]);
    child_process_hold_key_lock(root, ready_pipe[1], release_pipe[0]);
  }
  close(ready_pipe[1]);
  ready_pipe[1] = -1;
  close(release_pipe[0]);
  release_pipe[0] = -1;
  assert_int_equal(read(ready_pipe[0], &ready, 1U), 1);
  close(ready_pipe[0]);
  ready_pipe[0] = -1;

  probe_pid = fork();
  assert_true(probe_pid >= 0);
  if (probe_pid == 0) {
    close(release_pipe[1]);
    child_process_probe_key_locks(root);
  }
  probe_code = child_exit_code(probe_pid);
  assert_int_equal(probe_code, 0);

  assert_int_equal(write(release_pipe[1], "x", 1U), 1);
  close(release_pipe[1]);
  release_pipe[1] = -1;
  holder_code = child_exit_code(holder_pid);
  assert_int_equal(holder_code, 0);

  test_cleanup_root(root);
}

static void test_write_state_waits_for_cross_process_key_lock(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_key_lock *lock;
  lc_error error;
  int start_pipe[2];
  int ready_pipe[2];
  char ready;
  int acquired;
  int status;
  int child_code;
  int rc;
  pid_t pid;

  (void)state;
  test_root_path(root, sizeof(root), "write-state-key-lock-wait");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  store = NULL;
  lock = NULL;
  start_pipe[0] = -1;
  start_pipe[1] = -1;
  ready_pipe[0] = -1;
  ready_pipe[1] = -1;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = pipe(start_pipe);
  assert_int_equal(rc, 0);
  rc = pipe(ready_pipe);
  assert_int_equal(rc, 0);
  pid = fork();
  assert_true(pid >= 0);
  if (pid == 0) {
    close(start_pipe[1]);
    close(ready_pipe[0]);
    child_process_write_state_after_ready(root, start_pipe[0], ready_pipe[1]);
  }
  close(start_pipe[0]);
  start_pipe[0] = -1;
  close(ready_pipe[1]);
  ready_pipe[1] = -1;

  rc = store->try_lock_key(store, "default", "process-key", &lock, &acquired,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_true(acquired);
  assert_non_null(lock);
  assert_int_equal(write(start_pipe[1], "x", 1U), 1);
  close(start_pipe[1]);
  start_pipe[1] = -1;
  assert_int_equal(read(ready_pipe[0], &ready, 1U), 1);
  close(ready_pipe[0]);
  ready_pipe[0] = -1;

  test_sleep_for_lock_wait();
  status = 0;
  rc = waitpid(pid, &status, WNOHANG);
  assert_int_equal(rc, 0);

  rc = store->unlock_key(store, lock, &error);
  assert_int_equal(rc, LC_OK);
  lock = NULL;
  child_code = child_exit_code(pid);
  assert_int_equal(child_code, 0);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_store_meta_waits_for_cross_process_key_lock(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_key_lock *lock;
  lc_pouch_meta_record loaded;
  lc_error error;
  int start_pipe[2];
  int ready_pipe[2];
  char ready;
  int acquired;
  int status;
  int child_code;
  int rc;
  pid_t pid;

  (void)state;
  test_root_path(root, sizeof(root), "store-meta-key-lock-wait");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&loaded, 0, sizeof(loaded));
  memset(&error, 0, sizeof(error));
  store = NULL;
  lock = NULL;
  start_pipe[0] = -1;
  start_pipe[1] = -1;
  ready_pipe[0] = -1;
  ready_pipe[1] = -1;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = pipe(start_pipe);
  assert_int_equal(rc, 0);
  rc = pipe(ready_pipe);
  assert_int_equal(rc, 0);
  pid = fork();
  assert_true(pid >= 0);
  if (pid == 0) {
    close(start_pipe[1]);
    close(ready_pipe[0]);
    child_process_store_meta_after_ready(root, start_pipe[0], ready_pipe[1]);
  }
  close(start_pipe[0]);
  start_pipe[0] = -1;
  close(ready_pipe[1]);
  ready_pipe[1] = -1;

  rc = store->try_lock_key(store, "default", "meta-key", &lock, &acquired,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_true(acquired);
  assert_non_null(lock);
  assert_int_equal(write(start_pipe[1], "x", 1U), 1);
  close(start_pipe[1]);
  start_pipe[1] = -1;
  assert_int_equal(read(ready_pipe[0], &ready, 1U), 1);
  close(ready_pipe[0]);
  ready_pipe[0] = -1;

  test_sleep_for_lock_wait();
  status = 0;
  rc = waitpid(pid, &status, WNOHANG);
  assert_int_equal(rc, 0);

  rc = store->unlock_key(store, lock, &error);
  assert_int_equal(rc, LC_OK);
  lock = NULL;
  child_code = child_exit_code(pid);
  assert_int_equal(child_code, 0);

  rc = store->load_meta(store, "default", "meta-key", &loaded, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(loaded.found);
  assert_string_equal(loaded.meta.owner, "blocked-meta-writer");
  lc_pouch_meta_record_cleanup(&allocator, &loaded);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_put_object_waits_for_cross_process_key_lock(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_key_lock *lock;
  lc_pouch_object_list list;
  lc_error error;
  int start_pipe[2];
  int ready_pipe[2];
  char ready;
  int acquired;
  int status;
  int child_code;
  int rc;
  pid_t pid;

  (void)state;
  test_root_path(root, sizeof(root), "put-object-key-lock-wait");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&list, 0, sizeof(list));
  memset(&error, 0, sizeof(error));
  store = NULL;
  lock = NULL;
  start_pipe[0] = -1;
  start_pipe[1] = -1;
  ready_pipe[0] = -1;
  ready_pipe[1] = -1;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = pipe(start_pipe);
  assert_int_equal(rc, 0);
  rc = pipe(ready_pipe);
  assert_int_equal(rc, 0);
  pid = fork();
  assert_true(pid >= 0);
  if (pid == 0) {
    close(start_pipe[1]);
    close(ready_pipe[0]);
    child_process_put_object_after_ready(root, start_pipe[0], ready_pipe[1]);
  }
  close(start_pipe[0]);
  start_pipe[0] = -1;
  close(ready_pipe[1]);
  ready_pipe[1] = -1;

  rc = store->try_lock_key(store, "default", "object-key", &lock, &acquired,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_true(acquired);
  assert_non_null(lock);
  assert_int_equal(write(start_pipe[1], "x", 1U), 1);
  close(start_pipe[1]);
  start_pipe[1] = -1;
  assert_int_equal(read(ready_pipe[0], &ready, 1U), 1);
  close(ready_pipe[0]);
  ready_pipe[0] = -1;

  test_sleep_for_lock_wait();
  status = 0;
  rc = waitpid(pid, &status, WNOHANG);
  assert_int_equal(rc, 0);

  rc = store->unlock_key(store, lock, &error);
  assert_int_equal(rc, LC_OK);
  lock = NULL;
  child_code = child_exit_code(pid);
  assert_int_equal(child_code, 0);

  rc = store->list_objects(store, "default", "object-key", &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 1);
  assert_string_equal(list.items[0].name, "artifact.txt");
  lc_pouch_object_list_cleanup(&allocator, &list);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void test_copy_object_waits_for_cross_process_key_lock(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_key_lock *lock;
  lc_pouch_put_object_opts put_opts;
  lc_pouch_object_info put_info;
  lc_pouch_object_list list;
  lc_error error;
  int start_pipe[2];
  int ready_pipe[2];
  char ready;
  int acquired;
  int status;
  int child_code;
  int rc;
  pid_t pid;

  (void)state;
  test_root_path(root, sizeof(root), "copy-object-key-lock-wait");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&put_opts, 0, sizeof(put_opts));
  memset(&put_info, 0, sizeof(put_info));
  memset(&list, 0, sizeof(list));
  memset(&error, 0, sizeof(error));
  store = NULL;
  source = NULL;
  lock = NULL;
  start_pipe[0] = -1;
  start_pipe[1] = -1;
  ready_pipe[0] = -1;
  ready_pipe[1] = -1;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  put_opts.name = "artifact.txt";
  put_opts.content_type = "text/plain";
  source = source_from_text("copy-source");
  rc = store->put_object(store, "default", "source-key", source, &put_opts,
                         &put_info, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_object_info_cleanup(&allocator, &put_info);

  rc = pipe(start_pipe);
  assert_int_equal(rc, 0);
  rc = pipe(ready_pipe);
  assert_int_equal(rc, 0);
  pid = fork();
  assert_true(pid >= 0);
  if (pid == 0) {
    close(start_pipe[1]);
    close(ready_pipe[0]);
    child_process_copy_object_after_ready(root, start_pipe[0], ready_pipe[1]);
  }
  close(start_pipe[0]);
  start_pipe[0] = -1;
  close(ready_pipe[1]);
  ready_pipe[1] = -1;

  rc = store->try_lock_key(store, "default", "copy-key", &lock, &acquired,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_true(acquired);
  assert_non_null(lock);
  assert_int_equal(write(start_pipe[1], "x", 1U), 1);
  close(start_pipe[1]);
  start_pipe[1] = -1;
  assert_int_equal(read(ready_pipe[0], &ready, 1U), 1);
  close(ready_pipe[0]);
  ready_pipe[0] = -1;

  test_sleep_for_lock_wait();
  status = 0;
  rc = waitpid(pid, &status, WNOHANG);
  assert_int_equal(rc, 0);

  rc = store->unlock_key(store, lock, &error);
  assert_int_equal(rc, LC_OK);
  lock = NULL;
  child_code = child_exit_code(pid);
  assert_int_equal(child_code, 0);

  rc = store->list_objects(store, "default", "copy-key", &list, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(list.count, 1);
  assert_string_equal(list.items[0].name, "copy.txt");
  assert_string_equal(list.items[0].content_type, "text/plain");
  assert_int_equal(list.items[0].size, 11L);
  lc_pouch_object_list_cleanup(&allocator, &list);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_enqueue_message_waits_for_cross_process_queue_lock(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_pouch_key_lock *lock;
  lc_pouch_queue_stats stats;
  lc_error error;
  int start_pipe[2];
  int ready_pipe[2];
  char ready;
  int acquired;
  int status;
  int child_code;
  int rc;
  pid_t pid;

  (void)state;
  test_root_path(root, sizeof(root), "enqueue-queue-lock-wait");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&stats, 0, sizeof(stats));
  memset(&error, 0, sizeof(error));
  store = NULL;
  lock = NULL;
  start_pipe[0] = -1;
  start_pipe[1] = -1;
  ready_pipe[0] = -1;
  ready_pipe[1] = -1;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  rc = pipe(start_pipe);
  assert_int_equal(rc, 0);
  rc = pipe(ready_pipe);
  assert_int_equal(rc, 0);
  pid = fork();
  assert_true(pid >= 0);
  if (pid == 0) {
    close(start_pipe[1]);
    close(ready_pipe[0]);
    child_process_enqueue_after_ready(root, start_pipe[0], ready_pipe[1]);
  }
  close(start_pipe[0]);
  start_pipe[0] = -1;
  close(ready_pipe[1]);
  ready_pipe[1] = -1;

  rc = store->try_lock_key(store, "default", "jobs", &lock, &acquired, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(acquired);
  assert_non_null(lock);
  assert_int_equal(write(start_pipe[1], "x", 1U), 1);
  close(start_pipe[1]);
  start_pipe[1] = -1;
  assert_int_equal(read(ready_pipe[0], &ready, 1U), 1);
  close(ready_pipe[0]);
  ready_pipe[0] = -1;

  test_sleep_for_lock_wait();
  status = 0;
  rc = waitpid(pid, &status, WNOHANG);
  assert_int_equal(rc, 0);

  rc = store->unlock_key(store, lock, &error);
  assert_int_equal(rc, LC_OK);
  lock = NULL;
  child_code = child_exit_code(pid);
  assert_int_equal(child_code, 0);

  rc = store->queue_stats(store, "default", "jobs", &stats, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats.available, 1);
  assert_int_equal(stats.pending_candidates, 1);
  assert_non_null(stats.head_message_id);
  lc_pouch_queue_stats_cleanup(&allocator, &stats);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_dequeue_message_waits_for_cross_process_queue_lock(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_key_lock *lock;
  lc_pouch_enqueue_opts enqueue_opts;
  lc_pouch_queue_message_info enqueued;
  lc_pouch_queue_stats stats;
  lc_error error;
  int start_pipe[2];
  int ready_pipe[2];
  char ready;
  int acquired;
  int status;
  int child_code;
  int rc;
  pid_t pid;

  (void)state;
  test_root_path(root, sizeof(root), "dequeue-queue-lock-wait");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&enqueued, 0, sizeof(enqueued));
  memset(&stats, 0, sizeof(stats));
  memset(&error, 0, sizeof(error));
  store = NULL;
  source = NULL;
  lock = NULL;
  start_pipe[0] = -1;
  start_pipe[1] = -1;
  ready_pipe[0] = -1;
  ready_pipe[1] = -1;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  enqueue_opts.content_type = "text/plain";
  enqueue_opts.visibility_timeout_seconds = 30L;
  enqueue_opts.ttl_seconds = 3600L;
  enqueue_opts.max_attempts = 3;
  source = source_from_text("queue-lock-payload");
  rc = store->enqueue_message(store, "default", "jobs", source, &enqueue_opts,
                              &enqueued, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);

  rc = pipe(start_pipe);
  assert_int_equal(rc, 0);
  rc = pipe(ready_pipe);
  assert_int_equal(rc, 0);
  pid = fork();
  assert_true(pid >= 0);
  if (pid == 0) {
    close(start_pipe[1]);
    close(ready_pipe[0]);
    child_process_dequeue_after_ready(root, start_pipe[0], ready_pipe[1]);
  }
  close(start_pipe[0]);
  start_pipe[0] = -1;
  close(ready_pipe[1]);
  ready_pipe[1] = -1;

  rc = store->try_lock_key(store, "default", "jobs", &lock, &acquired, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(acquired);
  assert_non_null(lock);
  assert_int_equal(write(start_pipe[1], "x", 1U), 1);
  close(start_pipe[1]);
  start_pipe[1] = -1;
  assert_int_equal(read(ready_pipe[0], &ready, 1U), 1);
  close(ready_pipe[0]);
  ready_pipe[0] = -1;

  test_sleep_for_lock_wait();
  status = 0;
  rc = waitpid(pid, &status, WNOHANG);
  assert_int_equal(rc, 0);

  rc = store->unlock_key(store, lock, &error);
  assert_int_equal(rc, LC_OK);
  lock = NULL;
  child_code = child_exit_code(pid);
  assert_int_equal(child_code, 0);

  rc = store->queue_stats(store, "default", "jobs", &stats, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(stats.available, 0);
  assert_int_equal(stats.pending_candidates, 1);
  lc_pouch_queue_stats_cleanup(&allocator, &stats);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_promote_staged_state_waits_for_cross_process_key_lock(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *body;
  lc_pouch_key_lock *lock;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res staged;
  lc_pouch_state_info info;
  lc_error error;
  int start_pipe[2];
  int ready_pipe[2];
  char ready;
  int acquired;
  int status;
  int child_code;
  int rc;
  pid_t pid;

  (void)state;
  test_root_path(root, sizeof(root), "promote-staged-key-lock-wait");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&opts, 0, sizeof(opts));
  memset(&staged, 0, sizeof(staged));
  memset(&info, 0, sizeof(info));
  memset(&error, 0, sizeof(error));
  store = NULL;
  source = NULL;
  body = NULL;
  lock = NULL;
  start_pipe[0] = -1;
  start_pipe[1] = -1;
  ready_pipe[0] = -1;
  ready_pipe[1] = -1;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  opts.content_type = "text/plain";
  source = source_from_text("promote-blocked");
  rc = store->stage_state(store, "default", "lease-key", "txn-block", source,
                          &opts, &staged, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&allocator, &staged);

  rc = pipe(start_pipe);
  assert_int_equal(rc, 0);
  rc = pipe(ready_pipe);
  assert_int_equal(rc, 0);
  pid = fork();
  assert_true(pid >= 0);
  if (pid == 0) {
    close(start_pipe[1]);
    close(ready_pipe[0]);
    child_process_promote_staged_after_ready(root, start_pipe[0],
                                             ready_pipe[1]);
  }
  close(start_pipe[0]);
  start_pipe[0] = -1;
  close(ready_pipe[1]);
  ready_pipe[1] = -1;

  rc = store->try_lock_key(store, "default", "lease-key", &lock, &acquired,
                           &error);
  assert_int_equal(rc, LC_OK);
  assert_true(acquired);
  assert_non_null(lock);
  assert_int_equal(write(start_pipe[1], "x", 1U), 1);
  close(start_pipe[1]);
  start_pipe[1] = -1;
  assert_int_equal(read(ready_pipe[0], &ready, 1U), 1);
  close(ready_pipe[0]);
  ready_pipe[0] = -1;

  test_sleep_for_lock_wait();
  status = 0;
  rc = waitpid(pid, &status, WNOHANG);
  assert_int_equal(rc, 0);

  rc = store->unlock_key(store, lock, &error);
  assert_int_equal(rc, LC_OK);
  lock = NULL;
  child_code = child_exit_code(pid);
  assert_int_equal(child_code, 0);

  rc = store->read_state(store, "default", "lease-key", &body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(info.no_content);
  assert_non_null(body);
  lc_source_close(body);
  lc_pouch_state_info_cleanup(&allocator, &info);

  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_discard_staged_state_waits_for_cross_process_key_lock(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *body;
  lc_pouch_key_lock *lock;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res staged;
  lc_pouch_state_info info;
  lc_error error;
  int start_pipe[2];
  int ready_pipe[2];
  char ready;
  int acquired;
  int status;
  int child_code;
  int rc;
  pid_t pid;

  (void)state;
  test_root_path(root, sizeof(root), "discard-staged-key-lock-wait");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&opts, 0, sizeof(opts));
  memset(&staged, 0, sizeof(staged));
  memset(&info, 0, sizeof(info));
  memset(&error, 0, sizeof(error));
  store = NULL;
  source = NULL;
  body = NULL;
  lock = NULL;
  start_pipe[0] = -1;
  start_pipe[1] = -1;
  ready_pipe[0] = -1;
  ready_pipe[1] = -1;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  opts.content_type = "text/plain";
  source = source_from_text("discard-blocked");
  rc = store->stage_state(store, "default", "lease-key", "txn-block", source,
                          &opts, &staged, &error);
  lc_source_close(source);
  source = NULL;
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&allocator, &staged);

  rc = pipe(start_pipe);
  assert_int_equal(rc, 0);
  rc = pipe(ready_pipe);
  assert_int_equal(rc, 0);
  pid = fork();
  assert_true(pid >= 0);
  if (pid == 0) {
    close(start_pipe[1]);
    close(ready_pipe[0]);
    child_process_discard_staged_after_ready(root, start_pipe[0],
                                             ready_pipe[1]);
  }
  close(start_pipe[0]);
  start_pipe[0] = -1;
  close(ready_pipe[1]);
  ready_pipe[1] = -1;

  rc = store->try_lock_key(store, "default", "lease-key/.staging/txn-block",
                           &lock, &acquired, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(acquired);
  assert_non_null(lock);
  assert_int_equal(write(start_pipe[1], "x", 1U), 1);
  close(start_pipe[1]);
  start_pipe[1] = -1;
  assert_int_equal(read(ready_pipe[0], &ready, 1U), 1);
  close(ready_pipe[0]);
  ready_pipe[0] = -1;

  test_sleep_for_lock_wait();
  status = 0;
  rc = waitpid(pid, &status, WNOHANG);
  assert_int_equal(rc, 0);

  rc = store->unlock_key(store, lock, &error);
  assert_int_equal(rc, LC_OK);
  lock = NULL;
  child_code = child_exit_code(pid);
  assert_int_equal(child_code, 0);

  rc = store->load_staged_state(store, "default", "lease-key", "txn-block",
                                &body, &info, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(info.no_content);
  assert_null(body);
  lc_pouch_state_info_cleanup(&allocator, &info);

  rc = store->close(store, &error);
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
  assert_true(
      test_first_writer_marker_path(root, marker_path, sizeof(marker_path)));
  test_read_file_text(marker_path, before, sizeof(before));
  assert_non_null(strstr(before, "sequence=1\n"));
  assert_non_null(strstr(before, "updated_at_unix="));

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

static void
test_logstore_writer_marker_updates_after_namespace_commit(void **state) {
  char root[256];
  char marker_path[512];
  char before[256];
  char after[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res put_res;
  lc_error error;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "logstore-writer-marker");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  memset(&put_res, 0, sizeof(put_res));
  store = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(test_count_logstore_writer_markers(root, "default"), 0U);

  opts.content_type = "text/plain";
  source = source_from_text("first marker write");
  rc = store->write_state(store, "default", "marker-key", source, &opts,
                          &put_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(test_count_logstore_writer_markers(root, "default"), 1U);
  assert_true(test_first_logstore_writer_marker_path(
      root, "default", marker_path, sizeof(marker_path)));
  test_read_file_text(marker_path, before, sizeof(before));
  assert_non_null(strstr(before, "sequence=2\n"));
  assert_non_null(strstr(before, "updated_at_unix="));
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  memset(&put_res, 0, sizeof(put_res));

  source = source_from_text("second marker write");
  rc = store->write_state(store, "default", "marker-key", source, &opts,
                          &put_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  test_read_file_text(marker_path, after, sizeof(after));
  assert_non_null(strstr(after, "sequence=3\n"));
  assert_string_not_equal(before, after);

  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  rc = store->close(store, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_marker_snapshot_skips_unchanged_independent_refresh(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *writer;
  lc_pouch_store *reader;
  lc_source *source;
  lc_source *body;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res put_res;
  lc_pouch_state_info state_info;
  lc_pouch_lock_status before_status;
  lc_pouch_lock_status after_status;
  lc_pouch_lock_status changed_status;
  lc_error error;
  unsigned long first_refreshes;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "marker-snapshot-refresh");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&state_info, 0, sizeof(state_info));
  memset(&before_status, 0, sizeof(before_status));
  memset(&after_status, 0, sizeof(after_status));
  memset(&changed_status, 0, sizeof(changed_status));
  writer = NULL;
  reader = NULL;
  body = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &writer, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_disk_open(root, &allocator, &reader, &error);
  assert_int_equal(rc, LC_OK);

  opts.content_type = "text/plain";
  source = source_from_text("visible through marker");
  rc = writer->write_state(writer, "default", "marker-cache-key", source, &opts,
                           &put_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  memset(&put_res, 0, sizeof(put_res));

  rc = reader->read_state(reader, "default", "marker-cache-key", &body,
                          &state_info, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(state_info.no_content);
  lc_source_close(body);
  body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &state_info);
  memset(&state_info, 0, sizeof(state_info));

  rc = reader->lock_status(reader, &before_status, &error);
  assert_int_equal(rc, LC_OK);
  first_refreshes = before_status.replay_refreshes;

  rc = reader->read_state(reader, "default", "marker-cache-key", &body,
                          &state_info, &error);
  assert_int_equal(rc, LC_OK);
  lc_source_close(body);
  body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &state_info);
  memset(&state_info, 0, sizeof(state_info));

  rc = reader->lock_status(reader, &after_status, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(after_status.replay_refreshes, first_refreshes);

  source = source_from_text("changed through marker");
  rc = writer->write_state(writer, "default", "marker-cache-key", source, &opts,
                           &put_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);

  rc = reader->read_state(reader, "default", "marker-cache-key", &body,
                          &state_info, &error);
  assert_int_equal(rc, LC_OK);
  lc_source_close(body);
  body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  rc = reader->lock_status(reader, &changed_status, &error);
  assert_int_equal(rc, LC_OK);
  assert_true(changed_status.replay_refreshes > first_refreshes);

  lc_pouch_lock_status_cleanup(&allocator, &before_status);
  lc_pouch_lock_status_cleanup(&allocator, &after_status);
  lc_pouch_lock_status_cleanup(&allocator, &changed_status);
  rc = reader->close(reader, &error);
  assert_int_equal(rc, LC_OK);
  rc = writer->close(writer, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_marker_dir_mtime_fast_path_stats_known_markers(void **state) {
  char root[256];
  char markers_path[512];
  struct stat marker_dir_stat;
  struct utimbuf marker_dir_times;
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_store *writer;
  lc_pouch_store *reader;
  lc_source *source;
  lc_source *body;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res put_res;
  lc_pouch_state_info state_info;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "marker-dir-fast-path");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&error, 0, sizeof(error));
  memset(&opts, 0, sizeof(opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&state_info, 0, sizeof(state_info));
  writer = NULL;
  reader = NULL;
  body = NULL;
  text = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &writer, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_pouch_disk_open(root, &allocator, &reader, &error);
  assert_int_equal(rc, LC_OK);

  opts.content_type = "text/plain";
  source = source_from_text("directory fast path");
  rc = writer->write_state(writer, "default", "dir-fast-path-key", source,
                           &opts, &put_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);

  rc = reader->read_state(reader, "default", "dir-fast-path-key", &body,
                          &state_info, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(state_info.no_content);
  text = read_source_text(body);
  assert_string_equal(text, "directory fast path");
  free(text);
  text = NULL;
  lc_source_close(body);
  body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &state_info);
  memset(&state_info, 0, sizeof(state_info));

  snprintf(markers_path, sizeof(markers_path), "%s/default/logstore/markers",
           root);
  assert_int_equal(stat(markers_path, &marker_dir_stat), 0);
  source = source_from_text("directory fast path changed");
  rc = writer->write_state(writer, "default", "dir-fast-path-key", source,
                           &opts, &put_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  marker_dir_times.actime = marker_dir_stat.st_atime;
  marker_dir_times.modtime = marker_dir_stat.st_mtime;
  assert_int_equal(utime(markers_path, &marker_dir_times), 0);

  rc = reader->read_state(reader, "default", "dir-fast-path-key", &body,
                          &state_info, &error);
  assert_int_equal(rc, LC_OK);
  assert_false(state_info.no_content);
  text = read_source_text(body);
  assert_string_equal(text, "directory fast path changed");
  free(text);
  lc_source_close(body);
  body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  rc = reader->close(reader, &error);
  assert_int_equal(rc, LC_OK);
  rc = writer->close(writer, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_single_writer_mode_skips_peer_refresh_after_sync(void **state) {
  char root[256];
  lc_pouch_allocator allocator;
  tracked_allocator tracked;
  lc_pouch_disk_open_opts opts;
  lc_pouch_store *writer;
  lc_pouch_store *reader;
  lc_source *source;
  lc_source *body;
  lc_pouch_put_state_res put_res;
  lc_pouch_state_info state_info;
  lc_error error;
  char *text;
  int rc;

  (void)state;
  test_root_path(root, sizeof(root), "single-writer-refresh");
  test_cleanup_root(root);
  test_allocator_init(&allocator, &tracked);
  memset(&opts, 0, sizeof(opts));
  memset(&error, 0, sizeof(error));
  memset(&put_res, 0, sizeof(put_res));
  memset(&state_info, 0, sizeof(state_info));
  writer = NULL;
  reader = NULL;
  body = NULL;
  text = NULL;

  rc = lc_pouch_disk_open(root, &allocator, &writer, &error);
  assert_int_equal(rc, LC_OK);
  source = source_from_text("single-writer-v1");
  rc = writer->write_state(writer, "default", "single-writer-key", source,
                           NULL, &put_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);
  memset(&put_res, 0, sizeof(put_res));

  opts.single_writer = 1;
  rc = lc_pouch_disk_open_with_options(root, &allocator, &opts, &reader,
                                       &error);
  assert_int_equal(rc, LC_OK);

  rc = reader->read_state(reader, "default", "single-writer-key", &body,
                          &state_info, &error);
  assert_int_equal(rc, LC_OK);
  text = read_source_text(body);
  assert_string_equal(text, "single-writer-v1");
  free(text);
  text = NULL;
  lc_source_close(body);
  body = NULL;
  lc_pouch_state_info_cleanup(&allocator, &state_info);
  memset(&state_info, 0, sizeof(state_info));

  source = source_from_text("single-writer-v2");
  rc = writer->write_state(writer, "default", "single-writer-key", source,
                           NULL, &put_res, &error);
  lc_source_close(source);
  assert_int_equal(rc, LC_OK);
  lc_pouch_put_state_res_cleanup(&allocator, &put_res);

  rc = reader->read_state(reader, "default", "single-writer-key", &body,
                          &state_info, &error);
  assert_int_equal(rc, LC_OK);
  text = read_source_text(body);
  assert_string_equal(text, "single-writer-v1");
  free(text);
  lc_source_close(body);
  lc_pouch_state_info_cleanup(&allocator, &state_info);

  rc = reader->close(reader, &error);
  assert_int_equal(rc, LC_OK);
  rc = writer->close(writer, &error);
  assert_int_equal(rc, LC_OK);
  lc_error_cleanup(&error);
  test_cleanup_root(root);
}

static void
test_writer_marker_touch_failure_does_not_rollback_commit(void **state) {
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
  assert_true(
      test_first_writer_marker_path(root, marker_path, sizeof(marker_path)));
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
      cmocka_unit_test(test_allocator_from_lc_requires_matching_realloc),
      cmocka_unit_test(test_write_read_reopen_and_allocator_hooks),
      cmocka_unit_test(test_state_write_creates_segmented_namespace_logstore),
      cmocka_unit_test(test_segment_payload_refs_survive_root_log_truncation),
      cmocka_unit_test(test_segment_generation_refreshes_independent_handle),
      cmocka_unit_test(test_memory_records_append_only_to_segments),
      cmocka_unit_test(test_replay_recovers_state_from_namespace_segment),
      cmocka_unit_test(test_replay_repairs_missing_namespace_manifest),
      cmocka_unit_test(test_replay_repairs_crash_incomplete_namespace_manifest),
      cmocka_unit_test(test_segment_rotation_replays_multiple_segments),
      cmocka_unit_test(test_segment_replay_truncates_rotated_tail),
      cmocka_unit_test(test_segment_replay_honors_manifest_obsolete),
      cmocka_unit_test(test_replay_installed_snapshot_without_segment_tail),
      cmocka_unit_test(test_replay_cleans_obsolete_snapshot_files),
      cmocka_unit_test(test_maintenance_cleanup_cleans_obsolete_snapshot_files),
      cmocka_unit_test(
          test_maintenance_cleanup_honors_obsolete_delete_grace),
      cmocka_unit_test(test_replay_ignores_root_store_log_without_segments),
      cmocka_unit_test(test_state_put_propagates_source_failure_before_append),
      cmocka_unit_test(test_state_read_skips_replay_after_same_handle_write),
      cmocka_unit_test(
          test_query_index_scan_skips_replay_after_same_handle_write),
      cmocka_unit_test(test_cas_and_remove_semantics),
      cmocka_unit_test(test_state_lookup_index_orders_updates_and_replays),
      cmocka_unit_test(
          test_state_write_index_allocation_failure_replays_cleanly),
      cmocka_unit_test(test_staged_state_promote_discard_and_reopen),
      cmocka_unit_test(test_staged_state_remove_promote_discard_and_reopen),
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
      cmocka_unit_test(test_metadata_scan_skips_replay_after_same_handle_write),
      cmocka_unit_test(test_metadata_key_scan_orders_paginates_and_replays),
      cmocka_unit_test(test_metadata_scan_can_exclude_removed_state),
      cmocka_unit_test(test_metadata_scan_paginates_across_removed_state),
      cmocka_unit_test(test_query_index_scan_orders_paginates_and_reports_seq),
      cmocka_unit_test(test_query_index_range_scans_field_posting_candidates),
      cmocka_unit_test(
          test_query_index_range_uses_numeric_order_for_multidigit_values),
      cmocka_unit_test(test_query_index_in_deduplicates_duplicate_values),
      cmocka_unit_test(
          test_query_index_contains_uses_trigram_posting_candidates),
      cmocka_unit_test(
          test_query_index_summary_scan_applies_negative_terms),
      cmocka_unit_test(
          test_query_index_path_pattern_scan_intersects_positive_terms),
      cmocka_unit_test(test_query_index_keys_scan_avoids_metadata_row_copies),
      cmocka_unit_test(test_query_owner_index_scans_candidates_and_replays),
      cmocka_unit_test(test_query_index_scans_exclude_removed_state),
      cmocka_unit_test(test_query_owner_index_paginates_across_removed_state),
      cmocka_unit_test(
          test_query_index_scans_refresh_stale_reader_before_sidecar),
      cmocka_unit_test(test_query_index_projection_replays_updates_and_deletes),
      cmocka_unit_test(
          test_metadata_update_allocation_failure_preserves_indexes),
      cmocka_unit_test(test_retention_sweep_deletes_expired_metadata_and_state),
      cmocka_unit_test(
          test_retention_sweep_keeps_metadata_when_state_delete_fails),
      cmocka_unit_test(test_index_flush_reports_current_projection),
      cmocka_unit_test(test_index_flush_recovers_from_corrupt_sidecar_tail),
      cmocka_unit_test(test_query_index_sidecar_appends_metadata_records),
      cmocka_unit_test(
          test_query_index_keys_recovers_from_missing_legacy_store_log),
      cmocka_unit_test(test_query_index_rebuilds_field_postings_from_segments),
      cmocka_unit_test(
          test_query_index_keys_recovers_from_corrupt_sidecar_tail),
      cmocka_unit_test(test_query_index_keys_recreates_missing_sidecar),
      cmocka_unit_test(test_query_index_keys_rebuilds_future_sidecar_version),
      cmocka_unit_test(test_query_index_rebuilds_future_format_version),
      cmocka_unit_test(
          test_query_index_rebuilds_legacy_sidecar_without_format),
      cmocka_unit_test(test_query_index_keys_truncates_partial_sidecar_field),
      cmocka_unit_test(test_scan_meta_ignores_corrupt_query_sidecar),
      cmocka_unit_test(test_query_index_sidecar_compacts_with_segments),
      cmocka_unit_test(test_object_roundtrip_overwrite_delete_and_reopen),
      cmocka_unit_test(
          test_object_overwrite_allocation_failure_replays_cleanly),
      cmocka_unit_test(test_object_listing_orders_by_name_after_replay),
      cmocka_unit_test(test_object_key_scan_orders_pages_and_filters_name),
      cmocka_unit_test(test_object_max_bytes_reads_only_limit_plus_one),
      cmocka_unit_test(test_object_put_streams_payload_without_large_alloc),
      cmocka_unit_test(
          test_object_copy_streams_existing_payload_without_large_alloc),
      cmocka_unit_test(test_object_copy_rename_preserves_payload_metadata),
      cmocka_unit_test(
          test_object_copy_source_open_failure_leaves_destination_unchanged),
      cmocka_unit_test(test_object_copy_refreshes_after_segment_compaction),
      cmocka_unit_test(
          test_queue_dequeue_skips_replay_after_same_handle_enqueue),
      cmocka_unit_test(test_queue_dequeue_honors_start_after_cursor),
      cmocka_unit_test(test_queue_rejects_negative_timing_options),
      cmocka_unit_test(test_replay_streams_large_bodies_without_large_alloc),
      cmocka_unit_test(test_auto_compaction_preserves_live_heads_and_tokens),
      cmocka_unit_test(
          test_manual_compaction_reports_stats_and_preserves_state),
      cmocka_unit_test(test_compaction_if_needed_skip_and_allocator_failure),
      cmocka_unit_test(
          test_scheduled_maintenance_reports_compaction_diagnostics),
      cmocka_unit_test(
          test_scheduled_maintenance_honors_not_before_deadline),
      cmocka_unit_test(
          test_scheduled_maintenance_honors_min_candidate_files),
      cmocka_unit_test(
          test_scheduled_maintenance_honors_min_reclaimable_bytes),
      cmocka_unit_test(test_scheduled_maintenance_honors_io_throttle),
      cmocka_unit_test(test_compaction_preserves_promoted_staged_state_link),
      cmocka_unit_test(
          test_independent_handle_refreshes_after_segment_compaction),
      cmocka_unit_test(test_queue_dequeue_survives_compaction_refresh),
      cmocka_unit_test(test_queue_mutations_touch_wake_marker),
      cmocka_unit_test(test_queue_transaction_apply_touches_wake_marker),
      cmocka_unit_test(
          test_queue_wake_marker_failure_does_not_rollback_enqueue),
      cmocka_unit_test(test_queue_wake_status_reports_polling_marker_mode),
      cmocka_unit_test(test_empty_identifiers_are_rejected_before_append),
      cmocka_unit_test(test_pathlike_identifiers_are_rejected_before_append),
      cmocka_unit_test(test_queue_enqueue_dequeue_nack_ack_and_reopen),
      cmocka_unit_test(
          test_queue_enqueue_index_allocation_failure_replays_cleanly),
      cmocka_unit_test(test_queue_ref_requires_current_meta_etag),
      cmocka_unit_test(test_queue_delay_hides_until_visible),
      cmocka_unit_test(
          test_queue_ttl_expiry_removes_pending_candidate_after_replay),
      cmocka_unit_test(test_queue_inflight_ttl_expiry_rejects_ack),
      cmocka_unit_test(
          test_queue_nack_allocation_failure_preserves_active_lease),
      cmocka_unit_test(
          test_queue_extend_allocation_failure_preserves_active_lease),
      cmocka_unit_test(test_queue_retry_exhaustion_is_not_pending_after_replay),
      cmocka_unit_test(test_independent_handles_refresh_before_operations),
      cmocka_unit_test(test_independent_handles_refresh_index_projection),
      cmocka_unit_test(test_independent_handle_reopens_compacted_query_index),
      cmocka_unit_test(test_independent_processes_contend_with_cas),
      cmocka_unit_test(test_independent_processes_dequeue_single_message_once),
      cmocka_unit_test(test_query_config_defaults_and_configured_options),
      cmocka_unit_test(test_query_config_rejects_invalid_options),
      cmocka_unit_test(test_open_removes_stale_compaction_temps),
      cmocka_unit_test(test_close_removes_writer_marker),
      cmocka_unit_test(test_abort_preserves_writer_marker),
      cmocka_unit_test(test_writer_status_reports_marker_presence),
      cmocka_unit_test(test_writer_status_classifies_stale_markers),
      cmocka_unit_test(test_lock_status_reports_global_writer_lock_counters),
      cmocka_unit_test(test_lock_key_path_escapes_namespace_and_key),
      cmocka_unit_test(test_try_lock_key_serializes_same_process_handles),
      cmocka_unit_test(test_lock_fd_cache_reuses_released_key_descriptors),
      cmocka_unit_test(
          test_read_fd_cache_reuses_descriptors_without_closing_active_readers),
      cmocka_unit_test(
          test_read_source_survives_store_close_without_cache_owner),
      cmocka_unit_test(
          test_object_source_survives_store_close_without_cache_owner),
      cmocka_unit_test(
          test_queue_source_survives_store_close_without_cache_owner),
      cmocka_unit_test(test_object_copy_enforces_expected_etag),
      cmocka_unit_test(test_key_lock_wait_serializes_same_process_threads),
      cmocka_unit_test(test_try_lock_key_serializes_cross_process_handles),
      cmocka_unit_test(test_write_state_waits_for_cross_process_key_lock),
      cmocka_unit_test(test_store_meta_waits_for_cross_process_key_lock),
      cmocka_unit_test(test_put_object_waits_for_cross_process_key_lock),
      cmocka_unit_test(test_copy_object_waits_for_cross_process_key_lock),
      cmocka_unit_test(test_enqueue_message_waits_for_cross_process_queue_lock),
      cmocka_unit_test(test_dequeue_message_waits_for_cross_process_queue_lock),
      cmocka_unit_test(
          test_promote_staged_state_waits_for_cross_process_key_lock),
      cmocka_unit_test(
          test_discard_staged_state_waits_for_cross_process_key_lock),
      cmocka_unit_test(test_writer_marker_heartbeat_updates_after_commit),
      cmocka_unit_test(
          test_logstore_writer_marker_updates_after_namespace_commit),
      cmocka_unit_test(
          test_marker_snapshot_skips_unchanged_independent_refresh),
      cmocka_unit_test(
          test_marker_dir_mtime_fast_path_stats_known_markers),
      cmocka_unit_test(test_single_writer_mode_skips_peer_refresh_after_sync),
      cmocka_unit_test(
          test_writer_marker_touch_failure_does_not_rollback_commit),
      cmocka_unit_test(test_list_namespaces_reports_live_projection_names),
      cmocka_unit_test(test_backend_capabilities_report_disk_writer_model),
      cmocka_unit_test(test_fsync_stats_report_disk_sync_targets),
      cmocka_unit_test(test_backend_hash_persists_across_handles),
      cmocka_unit_test(test_backend_hash_create_race_publishes_single_identity),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}

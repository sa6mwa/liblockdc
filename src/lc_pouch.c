#include "lc_pouch.h"

#include "lc_api_internal.h"
#include "lc_intcompat.h"
#include "lc_log.h"
#include "lc_pouch_format.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"
#include "lc_pouch_query_index.h"

#include <openssl/crypto.h>
#include <openssl/evp.h>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#if defined(__linux__)
#include <poll.h>
#include <sys/inotify.h>
#include <sys/vfs.h>
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) ||     \
    defined(__OpenBSD__) || defined(__DragonFly__)
#include <strings.h>
#include <sys/mount.h>
#endif

#include <openssl/rand.h>

static uint64_t lc_pouch_next_writer_marker_id;
static pthread_mutex_t lc_pouch_writer_marker_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t lc_pouch_root_manifest_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t lc_pouch_writer_root_lock_mutex =
    PTHREAD_MUTEX_INITIALIZER;

/*
 * POSIX record locks belong to a process, rather than an individual file
 * descriptor. One process therefore needs one retained descriptor and a
 * local holder registry for each Pouch root; otherwise a second local open
 * can accidentally upgrade a shared lock or release every lock on close.
 */
struct lc_pouch_writer_root_lock_entry {
  lc_allocator allocator;
  dev_t device;
  ino_t inode;
  int fd;
  unsigned long shared_holders;
  unsigned long exclusive_holders;
  struct lc_pouch_writer_root_lock_entry *next;
};

static lc_pouch_writer_root_lock_entry *lc_pouch_writer_root_locks;

/* Match Go disk's bounded durable group-commit window. */
#define LC_POUCH_FSYNC_BATCH_DELAY_NS 2000000L
#define LC_POUCH_EXCLUSIVE_WRITER_TOUCH_NS 1000000000L
#define LC_POUCH_EXCLUSIVE_WRITER_TTL_NS ((int64_t)3 * (int64_t)1000000000)
#define LC_POUCH_WRITER_PRESENCE_MISSING (-1001)
#define LC_POUCH_WRITER_ROOT_LOCK_NONE 0
#define LC_POUCH_WRITER_ROOT_LOCK_SHARED 1
#define LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE 2
/* Match Go disk's disk-store index writer batching policy. */
#define LC_POUCH_DEFAULT_INDEXER_FLUSH_DOCS 2000U
#define LC_POUCH_DEFAULT_INDEXER_FLUSH_INTERVAL_SECONDS 10U

struct lc_pouch_indexer_pending_namespace {
  char *namespace_name;
  uint64_t write_count;
  struct lc_pouch_indexer_pending_namespace *next;
};

static const uint64_t
    lc_pouch_fsync_batch_bounds[LC_POUCH_FSYNC_BATCH_BOUND_COUNT] = {
        1U, 2U, 4U, 8U, 16U, 32U, 64U, 128U, 256U, 512U, 1024U, 2048U, 4096U,
};

struct lc_pouch_fsync_request {
  int fd;
  int done;
  int errnum;
  pthread_cond_t cond;
  struct lc_pouch_fsync_request *next;
};

typedef struct lc_pouch_fsync_batch_file {
  dev_t dev;
  ino_t ino;
  int fd;
} lc_pouch_fsync_batch_file;

struct lc_pouch_fsync_batcher {
  dev_t device;
  ino_t inode;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  pthread_t thread;
  lc_pouch_fsync_request *head;
  lc_pouch_fsync_request *tail;
  size_t queue_count;
  uint64_t batch_max_ops;
  lc_pouch_fsync_stats stats;
  unsigned long refcount;
  int stop;
  struct lc_pouch_fsync_batcher *next;
};

static pthread_mutex_t lc_pouch_fsync_batcher_registry_mutex =
    PTHREAD_MUTEX_INITIALIZER;
static lc_pouch_fsync_batcher *lc_pouch_fsync_batchers;

#ifdef LOCKDC_TEST_BUILD
long lc_pouch_test_fsync_batch_delay_ns;
#endif

static int lc_pouch_mutex_init_recursive(pthread_mutex_t *mutex) {
  pthread_mutexattr_t attr;
  int rc;

  rc = pthread_mutexattr_init(&attr);
  if (rc != 0) {
    return rc;
  }
  rc = pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
  if (rc == 0) {
    rc = pthread_mutex_init(mutex, &attr);
  }
  (void)pthread_mutexattr_destroy(&attr);
  return rc;
}

static int lc_pouch_sync_fd(int fd) {
#ifdef __linux__
  return fdatasync(fd);
#else
  return fsync(fd);
#endif
}

static void lc_pouch_fsync_deadline(struct timespec *deadline, long delay_ns) {
  if (deadline == NULL) {
    return;
  }
  clock_gettime(CLOCK_REALTIME, deadline);
  deadline->tv_nsec += delay_ns;
  if (deadline->tv_nsec >= 1000000000L) {
    deadline->tv_sec += deadline->tv_nsec / 1000000000L;
    deadline->tv_nsec %= 1000000000L;
  }
}

static uint64_t lc_pouch_elapsed_ns(const struct timespec *started,
                                    const struct timespec *finished) {
  uint64_t seconds;
  uint64_t nanos;

  if (started == NULL || finished == NULL ||
      finished->tv_sec < started->tv_sec ||
      (finished->tv_sec == started->tv_sec &&
       finished->tv_nsec < started->tv_nsec)) {
    return 0U;
  }
  seconds = (uint64_t)(finished->tv_sec - started->tv_sec);
  if (finished->tv_nsec < started->tv_nsec) {
    --seconds;
    nanos = (uint64_t)(1000000000L + finished->tv_nsec - started->tv_nsec);
  } else {
    nanos = (uint64_t)(finished->tv_nsec - started->tv_nsec);
  }
  if (seconds > LC_U64_MAX / 1000000000U) {
    return LC_U64_MAX;
  }
  seconds *= 1000000000U;
  if (nanos > LC_U64_MAX - seconds) {
    return LC_U64_MAX;
  }
  return seconds + nanos;
}

static void lc_pouch_fsync_record_batch_locked(lc_pouch_fsync_batcher *batcher,
                                               size_t count, uint64_t sync_ns) {
  size_t bucket;
  size_t index;

  if (batcher == NULL || count == 0U) {
    return;
  }
  ++batcher->stats.total_batches;
  batcher->stats.total_requests += (uint64_t)count;
  if ((uint64_t)count > batcher->stats.max_batch_size) {
    batcher->stats.max_batch_size = (uint64_t)count;
  }
  batcher->stats.total_sync_ns += sync_ns;
  if (sync_ns > batcher->stats.max_sync_ns) {
    batcher->stats.max_sync_ns = sync_ns;
  }
  bucket = LC_POUCH_FSYNC_BATCH_BOUND_COUNT;
  for (index = 0U; index < LC_POUCH_FSYNC_BATCH_BOUND_COUNT; ++index) {
    if ((uint64_t)count <= lc_pouch_fsync_batch_bounds[index]) {
      bucket = index;
      break;
    }
  }
  ++batcher->stats.counts[bucket];
}

static int
lc_pouch_fsync_batch_limit_reached(const lc_pouch_fsync_batcher *batcher) {
  if (batcher == NULL || batcher->batch_max_ops == 0U) {
    return 0;
  }
  return (uint64_t)batcher->queue_count >= batcher->batch_max_ops;
}

static int lc_pouch_fsync_batch_seen(lc_pouch_fsync_batch_file *files,
                                     size_t count, const struct stat *st) {
  size_t index;

  if (files == NULL || st == NULL) {
    return 0;
  }
  for (index = 0U; index < count; ++index) {
    if (files[index].dev == st->st_dev && files[index].ino == st->st_ino) {
      return 1;
    }
  }
  return 0;
}

static void lc_pouch_fsync_process_batch(lc_pouch_fsync_request *batch,
                                         size_t count, uint64_t *sync_ns_out) {
  lc_pouch_fsync_batch_file inline_files[64];
  lc_pouch_fsync_batch_file *files;
  lc_pouch_fsync_request *request;
  struct stat st;
  size_t file_count;
  int errnum;
  struct timespec started;
  struct timespec finished;
  int clock_started;

  if (sync_ns_out != NULL) {
    *sync_ns_out = 0U;
  }
  clock_started = clock_gettime(CLOCK_MONOTONIC, &started) == 0;

  files = inline_files;
  if (count > sizeof(inline_files) / sizeof(inline_files[0])) {
    files = (lc_pouch_fsync_batch_file *)lc_calloc_with_allocator(
        NULL, count, sizeof(lc_pouch_fsync_batch_file));
    if (files == NULL) {
      errnum = ENOMEM;
      goto finish;
    }
  }
  file_count = 0U;
  errnum = 0;
  for (request = batch; request != NULL; request = request->next) {
    if (request->fd < 0) {
      continue;
    }
    if (fstat(request->fd, &st) != 0) {
      if (errnum == 0) {
        errnum = errno != 0 ? errno : EIO;
      }
      continue;
    }
    if (lc_pouch_fsync_batch_seen(files, file_count, &st)) {
      continue;
    }
    files[file_count].dev = st.st_dev;
    files[file_count].ino = st.st_ino;
    files[file_count].fd = request->fd;
    ++file_count;
    if (lc_pouch_sync_fd(request->fd) != 0 && errnum == 0) {
      errnum = errno != 0 ? errno : EIO;
    }
  }
  if (files != inline_files) {
    lc_free_with_allocator(NULL, files);
  }

finish:
  if (clock_started && clock_gettime(CLOCK_MONOTONIC, &finished) == 0 &&
      sync_ns_out != NULL) {
    *sync_ns_out = lc_pouch_elapsed_ns(&started, &finished);
  }
  for (request = batch; request != NULL; request = request->next) {
    request->errnum = errnum;
  }
}

static lc_pouch_fsync_request *
lc_pouch_fsync_take_batch(lc_pouch_fsync_batcher *batcher, size_t *out_count) {
  lc_pouch_fsync_request *batch;
  lc_pouch_fsync_request *tail;
  size_t count;

  batch = batcher->head;
  tail = NULL;
  count = 0U;
  while (batcher->head != NULL && (batcher->batch_max_ops == 0U ||
                                   (uint64_t)count < batcher->batch_max_ops)) {
    tail = batcher->head;
    batcher->head = batcher->head->next;
    ++count;
  }
  if (tail != NULL) {
    tail->next = NULL;
  }
  if (batcher->head == NULL) {
    batcher->tail = NULL;
  }
  if (batcher->queue_count >= count) {
    batcher->queue_count -= count;
  } else {
    batcher->queue_count = 0U;
  }
  if (out_count != NULL) {
    *out_count = count;
  }
  return batch;
}

static void *lc_pouch_fsync_worker(void *arg) {
  lc_pouch_fsync_batcher *batcher;
  lc_pouch_fsync_request *batch;
  lc_pouch_fsync_request *request;
  struct timespec deadline;
  size_t batch_count;
  uint64_t sync_ns;
  long batch_delay_ns;

  batcher = (lc_pouch_fsync_batcher *)arg;
  pthread_mutex_lock(&batcher->mutex);
  for (;;) {
    while (batcher->head == NULL && !batcher->stop) {
      pthread_cond_wait(&batcher->cond, &batcher->mutex);
    }
    if (batcher->head == NULL && batcher->stop) {
      pthread_mutex_unlock(&batcher->mutex);
      return NULL;
    }
    batch_delay_ns = LC_POUCH_FSYNC_BATCH_DELAY_NS;
#ifdef LOCKDC_TEST_BUILD
    if (lc_pouch_test_fsync_batch_delay_ns > 0L) {
      batch_delay_ns = lc_pouch_test_fsync_batch_delay_ns;
    }
#endif
    if (!batcher->stop && batch_delay_ns > 0L &&
        !lc_pouch_fsync_batch_limit_reached(batcher)) {
      lc_pouch_fsync_deadline(&deadline, batch_delay_ns);
      while (!batcher->stop && !lc_pouch_fsync_batch_limit_reached(batcher)) {
        if (pthread_cond_timedwait(&batcher->cond, &batcher->mutex,
                                   &deadline) == ETIMEDOUT) {
          break;
        }
      }
    }
    batch = lc_pouch_fsync_take_batch(batcher, &batch_count);
    pthread_mutex_unlock(&batcher->mutex);
    sync_ns = 0U;
    lc_pouch_fsync_process_batch(batch, batch_count, &sync_ns);
    pthread_mutex_lock(&batcher->mutex);
    lc_pouch_fsync_record_batch_locked(batcher, batch_count, sync_ns);
    for (request = batch; request != NULL; request = request->next) {
      request->done = 1;
      pthread_cond_signal(&request->cond);
    }
  }
}

static int lc_pouch_fsync_batcher_init(lc_pouch *pouch, lc_error *error) {
  struct stat st;
  lc_pouch_fsync_batcher *batcher;
  lc_pouch_fsync_batcher *created;
  int pthread_rc;

  if (pouch == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch fsync batcher requires pouch", NULL, NULL,
                        "pouch");
  }
  if (stat(pouch->root_path, &st) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to identify pouch fsync root", strerror(errno),
                        NULL, "pouch");
  }
  pthread_mutex_lock(&lc_pouch_fsync_batcher_registry_mutex);
  for (batcher = lc_pouch_fsync_batchers; batcher != NULL;
       batcher = batcher->next) {
    if (batcher->device == st.st_dev && batcher->inode == st.st_ino) {
      break;
    }
  }
  if (batcher != NULL) {
    pthread_mutex_lock(&batcher->mutex);
    if (batcher->batch_max_ops == 0U ||
        (pouch->fsync_batch_max_ops != 0U &&
         pouch->fsync_batch_max_ops < batcher->batch_max_ops)) {
      batcher->batch_max_ops = pouch->fsync_batch_max_ops;
    }
    ++batcher->refcount;
    pthread_mutex_unlock(&batcher->mutex);
    pthread_mutex_unlock(&lc_pouch_fsync_batcher_registry_mutex);
    pouch->fsync_batcher = batcher;
    return LC_OK;
  }
  created = (lc_pouch_fsync_batcher *)lc_calloc_with_allocator(
      NULL, 1U, sizeof(*created));
  if (created == NULL) {
    pthread_mutex_unlock(&lc_pouch_fsync_batcher_registry_mutex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch fsync batcher", NULL, NULL,
                        "pouch");
  }
  created->device = st.st_dev;
  created->inode = st.st_ino;
  pthread_rc = pthread_mutex_init(&created->mutex, NULL);
  if (pthread_rc != 0) {
    pthread_mutex_unlock(&lc_pouch_fsync_batcher_registry_mutex);
    lc_free_with_allocator(NULL, created);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch fsync mutex",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pthread_rc = pthread_cond_init(&created->cond, NULL);
  if (pthread_rc != 0) {
    pthread_mutex_unlock(&lc_pouch_fsync_batcher_registry_mutex);
    pthread_mutex_destroy(&created->mutex);
    lc_free_with_allocator(NULL, created);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch fsync condition",
                        strerror(pthread_rc), NULL, "pouch");
  }
  created->batch_max_ops = pouch->fsync_batch_max_ops;
  created->refcount = 1UL;
  pthread_rc =
      pthread_create(&created->thread, NULL, lc_pouch_fsync_worker, created);
  if (pthread_rc != 0) {
    pthread_mutex_unlock(&lc_pouch_fsync_batcher_registry_mutex);
    pthread_cond_destroy(&created->cond);
    pthread_mutex_destroy(&created->mutex);
    lc_free_with_allocator(NULL, created);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to start pouch fsync worker",
                        strerror(pthread_rc), NULL, "pouch");
  }
  created->next = lc_pouch_fsync_batchers;
  lc_pouch_fsync_batchers = created;
  pthread_mutex_unlock(&lc_pouch_fsync_batcher_registry_mutex);
  pouch->fsync_batcher = created;
  return LC_OK;
}

static void lc_pouch_fsync_batcher_close(lc_pouch *pouch) {
  lc_pouch_fsync_batcher *batcher;
  lc_pouch_fsync_batcher **cursor;

  if (pouch == NULL) {
    return;
  }
  pthread_mutex_lock(&lc_pouch_fsync_batcher_registry_mutex);
  batcher = pouch->fsync_batcher;
  pouch->fsync_batcher = NULL;
  if (batcher == NULL) {
    pthread_mutex_unlock(&lc_pouch_fsync_batcher_registry_mutex);
    return;
  }
  pthread_mutex_lock(&batcher->mutex);
  if (batcher->refcount > 0UL) {
    --batcher->refcount;
  }
  if (batcher->refcount != 0UL) {
    pthread_mutex_unlock(&batcher->mutex);
    pthread_mutex_unlock(&lc_pouch_fsync_batcher_registry_mutex);
    return;
  }
  for (cursor = &lc_pouch_fsync_batchers; *cursor != NULL;
       cursor = &(*cursor)->next) {
    if (*cursor == batcher) {
      *cursor = batcher->next;
      break;
    }
  }
  batcher->stop = 1;
  pthread_cond_broadcast(&batcher->cond);
  pthread_mutex_unlock(&batcher->mutex);
  pthread_mutex_unlock(&lc_pouch_fsync_batcher_registry_mutex);
  pthread_join(batcher->thread, NULL);
  pthread_cond_destroy(&batcher->cond);
  pthread_mutex_destroy(&batcher->mutex);
  lc_free_with_allocator(NULL, batcher);
}

static void lc_pouch_compaction_deadline(lc_pouch *pouch,
                                         struct timespec *deadline) {
  uint64_t seconds;

  if (deadline == NULL) {
    return;
  }
  clock_gettime(CLOCK_REALTIME, deadline);
  seconds = pouch->compaction_interval_seconds;
  if (seconds > (uint64_t)(LONG_MAX - deadline->tv_sec)) {
    deadline->tv_sec = LONG_MAX;
  } else {
    deadline->tv_sec += (time_t)seconds;
  }
}

static void lc_pouch_janitor_deadline(lc_pouch *pouch,
                                      struct timespec *deadline) {
  uint64_t seconds;

  if (deadline == NULL) {
    return;
  }
  clock_gettime(CLOCK_REALTIME, deadline);
  seconds = pouch->janitor_interval_seconds;
  if (seconds > (uint64_t)(LONG_MAX - deadline->tv_sec)) {
    deadline->tv_sec = LONG_MAX;
  } else {
    deadline->tv_sec += (time_t)seconds;
  }
}

int lc_pouch_compaction_track_namespace(lc_pouch *pouch,
                                        const char *namespace_name,
                                        lc_error *error) {
  char **next_namespaces;
  char *name_copy;
  size_t index;
  size_t next_capacity;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch compaction namespace requires inputs", NULL,
                        NULL, "pouch");
  }
  if (!pouch->compaction_mutex_initialized) {
    return LC_OK;
  }
  pthread_mutex_lock(&pouch->compaction_mutex);
  for (index = 0U; index < pouch->compaction_namespace_count; ++index) {
    if (strcmp(pouch->compaction_namespaces[index], namespace_name) == 0) {
      pthread_mutex_unlock(&pouch->compaction_mutex);
      return LC_OK;
    }
  }
  if (pouch->compaction_namespace_count >=
      pouch->compaction_namespace_capacity) {
    next_capacity = pouch->compaction_namespace_capacity == 0U
                        ? 8U
                        : pouch->compaction_namespace_capacity * 2U;
    if (next_capacity <= pouch->compaction_namespace_capacity ||
        next_capacity > (size_t)-1 / sizeof(*next_namespaces)) {
      pthread_mutex_unlock(&pouch->compaction_mutex);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch compaction namespace count exceeds limit",
                          NULL, NULL, NULL);
    }
    next_namespaces = (char **)lc_realloc_with_allocator(
        &pouch->allocator, pouch->compaction_namespaces,
        next_capacity * sizeof(*next_namespaces));
    if (next_namespaces == NULL) {
      pthread_mutex_unlock(&pouch->compaction_mutex);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to grow pouch compaction namespaces", NULL,
                          NULL, NULL);
    }
    pouch->compaction_namespaces = next_namespaces;
    pouch->compaction_namespace_capacity = next_capacity;
  }
  name_copy = lc_strdup_with_allocator(&pouch->allocator, namespace_name);
  if (name_copy == NULL) {
    pthread_mutex_unlock(&pouch->compaction_mutex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch compaction namespace", NULL, NULL,
                        NULL);
  }
  pouch->compaction_namespaces[pouch->compaction_namespace_count++] = name_copy;
  pthread_mutex_unlock(&pouch->compaction_mutex);
  return LC_OK;
}

static int lc_pouch_compaction_copy_namespaces(lc_pouch *pouch,
                                               char ***out_namespaces,
                                               size_t *out_count,
                                               lc_error *error) {
  char **namespaces;
  size_t count;
  size_t index;

  *out_namespaces = NULL;
  *out_count = 0U;
  pthread_mutex_lock(&pouch->compaction_mutex);
  count = pouch->compaction_namespace_count;
  namespaces = count > 0U ? (char **)lc_calloc_with_allocator(
                                &pouch->allocator, count, sizeof(*namespaces))
                          : NULL;
  if (count > 0U && namespaces == NULL) {
    pthread_mutex_unlock(&pouch->compaction_mutex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch compaction namespaces", NULL,
                        NULL, NULL);
  }
  for (index = 0U; index < count; ++index) {
    namespaces[index] = lc_strdup_with_allocator(
        &pouch->allocator, pouch->compaction_namespaces[index]);
    if (namespaces[index] == NULL) {
      while (index > 0U) {
        lc_free_with_allocator(&pouch->allocator, namespaces[--index]);
      }
      lc_free_with_allocator(&pouch->allocator, namespaces);
      pthread_mutex_unlock(&pouch->compaction_mutex);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to copy pouch compaction namespace", NULL,
                          NULL, NULL);
    }
  }
  pthread_mutex_unlock(&pouch->compaction_mutex);
  *out_namespaces = namespaces;
  *out_count = count;
  return LC_OK;
}

static void lc_pouch_compaction_namespaces_cleanup(lc_pouch *pouch,
                                                   char **namespaces,
                                                   size_t count) {
  size_t index;

  for (index = 0U; index < count; ++index) {
    lc_free_with_allocator(&pouch->allocator, namespaces[index]);
  }
  lc_free_with_allocator(&pouch->allocator, namespaces);
}

static int lc_pouch_track_root_namespaces(lc_pouch *pouch, lc_error *error) {
  char *namespaces_path;
  DIR *dir;
  struct dirent *entry;
  int rc;

  if (pouch == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace tracking requires pouch", NULL, NULL,
                        "pouch");
  }
  namespaces_path =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, "namespaces");
  if (namespaces_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespaces path", NULL, NULL,
                        "pouch");
  }
  dir = opendir(namespaces_path);
  lc_free_with_allocator(&pouch->allocator, namespaces_path);
  if (dir == NULL) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to scan pouch namespaces", strerror(errno),
                        NULL, "pouch");
  }
  rc = LC_OK;
  while (rc == LC_OK) {
    char *namespace_name;

    errno = 0;
    entry = readdir(dir);
    if (entry == NULL) {
      if (errno != 0) {
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to scan pouch namespaces", strerror(errno),
                          NULL, "pouch");
      }
      break;
    }
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    namespace_name =
        lc_pouch_path_unescape_name(&pouch->allocator, entry->d_name);
    if (namespace_name == NULL) {
      continue;
    }
    rc = lc_pouch_compaction_track_namespace(pouch, namespace_name, error);
    lc_free_with_allocator(&pouch->allocator, namespace_name);
  }
  if (closedir(dir) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch namespaces", strerror(errno), NULL,
                      "pouch");
  }
  return rc;
}

static void lc_pouch_compaction_run_pass(lc_pouch *pouch) {
  char **namespaces;
  size_t namespace_count;
  size_t index;
  lc_error error;
  int rc;

  if (pouch == NULL || !pouch->background_compaction_enabled ||
      pouch->compaction_interval_seconds == 0U) {
    return;
  }
  lc_error_init(&error);
  rc = lc_pouch_compaction_copy_namespaces(pouch, &namespaces, &namespace_count,
                                           &error);
  if (rc != LC_OK) {
    pslog_field fields[2];

    fields[0] = lc_log_error_field("error", &error);
    fields[1] = lc_log_code_field(&error);
    lc_log_warn(pouch->logger, "compaction.background.namespaces.error", fields,
                2U);
    lc_error_cleanup(&error);
    return;
  }
  lc_error_cleanup(&error);
  for (index = 0U; index < namespace_count; ++index) {
    lc_pouch_maintenance_options options;
    lc_error maintenance_error;

    memset(&options, 0, sizeof(options));
    options.namespace_name = namespaces[index];
    lc_error_init(&maintenance_error);
    rc = lc_pouch_maintenance_run(pouch, &options, NULL, &maintenance_error);
    if (rc != LC_OK) {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("ns", namespaces[index]);
      fields[1] = lc_log_error_field("error", &maintenance_error);
      fields[2] = lc_log_code_field(&maintenance_error);
      lc_log_warn(pouch->logger, "compaction.background.error", fields, 3U);
    }
    lc_error_cleanup(&maintenance_error);
  }
  lc_pouch_compaction_namespaces_cleanup(pouch, namespaces, namespace_count);
}

static void *lc_pouch_compaction_worker(void *arg) {
  lc_pouch *pouch;
  int armed;

  pouch = (lc_pouch *)arg;
  armed = 0;
  pthread_mutex_lock(&pouch->compaction_mutex);
  while (!pouch->compaction_stop) {
    struct timespec deadline;
    int wait_rc;

    /* Opening a root alone must not schedule background maintenance. */
    if (!armed) {
      while (!pouch->compaction_stop && !pouch->compaction_pending) {
        (void)pthread_cond_wait(&pouch->compaction_cond,
                                &pouch->compaction_mutex);
      }
      if (pouch->compaction_stop) {
        break;
      }
      pouch->compaction_pending = 0;
      armed = 1;
    }
    lc_pouch_compaction_deadline(pouch, &deadline);
    wait_rc = 0;
    while (!pouch->compaction_stop && !pouch->compaction_pending &&
           wait_rc != ETIMEDOUT) {
      wait_rc = pthread_cond_timedwait(&pouch->compaction_cond,
                                       &pouch->compaction_mutex, &deadline);
    }
    if (pouch->compaction_stop) {
      break;
    }
    if (pouch->compaction_pending) {
      /* A later mutation restarts the full idle delay before maintenance. */
      pouch->compaction_pending = 0;
      continue;
    }
    pthread_mutex_unlock(&pouch->compaction_mutex);
    lc_pouch_compaction_run_pass(pouch);
    pthread_mutex_lock(&pouch->compaction_mutex);
  }
  pthread_mutex_unlock(&pouch->compaction_mutex);
  return NULL;
}

static int lc_pouch_compaction_worker_init(lc_pouch *pouch, lc_error *error) {
  int pthread_rc;
  int rc;

  if (pouch == NULL) {
    return LC_OK;
  }
  pthread_rc = pthread_mutex_init(&pouch->compaction_mutex, NULL);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch compaction mutex",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->compaction_mutex_initialized = 1;
  pthread_rc = pthread_cond_init(&pouch->compaction_cond, NULL);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch compaction condition",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->compaction_cond_initialized = 1;
  rc = lc_pouch_state_compaction_track_cached_namespaces(pouch, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_track_root_namespaces(pouch, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!pouch->background_compaction_enabled ||
      pouch->compaction_interval_seconds == 0U) {
    return LC_OK;
  }
  pthread_rc = pthread_create(&pouch->compaction_thread, NULL,
                              lc_pouch_compaction_worker, pouch);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to start pouch compaction worker",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->compaction_thread_started = 1;
  return LC_OK;
}

static void lc_pouch_compaction_worker_close(lc_pouch *pouch) {
  if (pouch == NULL) {
    return;
  }
  if (pouch->compaction_thread_started) {
    pthread_mutex_lock(&pouch->compaction_mutex);
    pouch->compaction_stop = 1;
    pthread_cond_broadcast(&pouch->compaction_cond);
    pthread_mutex_unlock(&pouch->compaction_mutex);
    pthread_join(pouch->compaction_thread, NULL);
    pouch->compaction_thread_started = 0;
  }
  if (pouch->compaction_cond_initialized) {
    pthread_cond_destroy(&pouch->compaction_cond);
    pouch->compaction_cond_initialized = 0;
  }
  if (pouch->compaction_mutex_initialized) {
    pthread_mutex_destroy(&pouch->compaction_mutex);
    pouch->compaction_mutex_initialized = 0;
  }
  lc_pouch_compaction_namespaces_cleanup(pouch, pouch->compaction_namespaces,
                                         pouch->compaction_namespace_count);
  pouch->compaction_namespaces = NULL;
  pouch->compaction_namespace_count = 0U;
  pouch->compaction_namespace_capacity = 0U;
}

void lc_pouch_compaction_note_mutation(lc_pouch *pouch) {
  if (pouch == NULL || !pouch->background_compaction_enabled ||
      pouch->compaction_interval_seconds == 0U || pouch->aborted ||
      !pouch->compaction_thread_started) {
    return;
  }
  pthread_mutex_lock(&pouch->compaction_mutex);
  pouch->compaction_pending = 1;
  pthread_cond_signal(&pouch->compaction_cond);
  pthread_mutex_unlock(&pouch->compaction_mutex);
}

static void lc_pouch_indexer_deadline(const lc_pouch *pouch,
                                      struct timespec *deadline) {
  uint64_t seconds;

  if (pouch == NULL || deadline == NULL) {
    return;
  }
  clock_gettime(CLOCK_REALTIME, deadline);
  seconds = pouch->indexer_flush_interval_seconds;
  if (seconds > (uint64_t)(LONG_MAX - deadline->tv_sec)) {
    deadline->tv_sec = LONG_MAX;
  } else {
    deadline->tv_sec += (time_t)seconds;
  }
}

#ifdef LOCKDC_TEST_BUILD
void lc_pouch_test_indexer_deadline(lc_pouch *pouch,
                                    struct timespec *deadline) {
  lc_pouch_indexer_deadline(pouch, deadline);
}
#endif

static void lc_pouch_indexer_pending_namespaces_cleanup(
    lc_pouch *pouch, lc_pouch_indexer_pending_namespace *namespaces) {
  while (namespaces != NULL) {
    lc_pouch_indexer_pending_namespace *next;

    next = namespaces->next;
    lc_free_with_allocator(&pouch->allocator, namespaces->namespace_name);
    lc_free_with_allocator(&pouch->allocator, namespaces);
    namespaces = next;
  }
}

static int lc_pouch_indexer_queue_namespace_locked(lc_pouch *pouch,
                                                   const char *namespace_name) {
  lc_pouch_indexer_pending_namespace *entry;

  for (entry = pouch->indexer_pending_namespaces; entry != NULL;
       entry = entry->next) {
    if (strcmp(entry->namespace_name, namespace_name) == 0) {
      if (entry->write_count != LC_U64_MAX) {
        ++entry->write_count;
      }
      return LC_OK;
    }
  }
  entry = (lc_pouch_indexer_pending_namespace *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*entry));
  if (entry == NULL) {
    return LC_ERR_NOMEM;
  }
  entry->namespace_name =
      lc_strdup_with_allocator(&pouch->allocator, namespace_name);
  if (entry->namespace_name == NULL) {
    lc_free_with_allocator(&pouch->allocator, entry);
    return LC_ERR_NOMEM;
  }
  entry->write_count = 1U;
  entry->next = pouch->indexer_pending_namespaces;
  pouch->indexer_pending_namespaces = entry;
  return LC_OK;
}

static int lc_pouch_indexer_flush_limit_reached_locked(lc_pouch *pouch) {
  lc_pouch_indexer_pending_namespace *entry;

  for (entry = pouch->indexer_pending_namespaces; entry != NULL;
       entry = entry->next) {
    if (entry->write_count >= pouch->indexer_flush_docs &&
        (!lc_pouch_single_writer_enabled(pouch) ||
         lc_pouch_query_index_pending_document_limit_reached_locked(
             pouch, entry->namespace_name, pouch->indexer_flush_docs))) {
      return 1;
    }
  }
  return 0;
}

static void lc_pouch_indexer_requeue(lc_pouch *pouch,
                                     lc_pouch_indexer_pending_namespace *list) {
  while (list != NULL) {
    lc_pouch_indexer_pending_namespace *next;
    lc_pouch_indexer_pending_namespace *entry;

    next = list->next;
    for (entry = pouch->indexer_pending_namespaces; entry != NULL;
         entry = entry->next) {
      if (strcmp(entry->namespace_name, list->namespace_name) == 0) {
        if (entry->write_count > LC_U64_MAX - list->write_count) {
          entry->write_count = LC_U64_MAX;
        } else {
          entry->write_count += list->write_count;
        }
        lc_free_with_allocator(&pouch->allocator, list->namespace_name);
        lc_free_with_allocator(&pouch->allocator, list);
        list = next;
        break;
      }
    }
    if (entry == NULL) {
      list->next = pouch->indexer_pending_namespaces;
      pouch->indexer_pending_namespaces = list;
      list = next;
    }
  }
}

static void
lc_pouch_indexer_run_batch(lc_pouch *pouch,
                           lc_pouch_indexer_pending_namespace *batch) {
  lc_pouch_indexer_pending_namespace *failed;
  lc_pouch_indexer_pending_namespace *entry;

  failed = NULL;
  entry = batch;
  while (entry != NULL) {
    lc_pouch_indexer_pending_namespace *next;
    lc_pouch_query_index_flush_result flush_result;
    lc_pouch_generation state_index_seq;
    lc_error error;
    int rc;

    next = entry->next;
    entry->next = NULL;
    memset(&flush_result, 0, sizeof(flush_result));
    state_index_seq = 0UL;
    lc_error_init(&error);
    rc = lc_pouch_state_index_seq(pouch, entry->namespace_name,
                                  &state_index_seq, &error);
    if (rc == LC_OK) {
      rc = lc_pouch_query_index_flush(pouch, entry->namespace_name,
                                      state_index_seq, &flush_result, &error);
    }
    if (rc != LC_OK) {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("ns", entry->namespace_name);
      fields[1] = lc_log_error_field("error", &error);
      fields[2] = lc_log_code_field(&error);
      lc_log_warn(pouch->logger, "index.background.error", fields, 3U);
      entry->next = failed;
      failed = entry;
    } else {
      lc_free_with_allocator(&pouch->allocator, entry->namespace_name);
      lc_free_with_allocator(&pouch->allocator, entry);
    }
    lc_error_cleanup(&error);
    entry = next;
  }
  if (failed != NULL) {
    pthread_mutex_lock(&pouch->indexer_mutex);
    lc_pouch_indexer_requeue(pouch, failed);
    pthread_cond_signal(&pouch->indexer_cond);
    pthread_mutex_unlock(&pouch->indexer_mutex);
  }
}

static void *lc_pouch_indexer_worker(void *arg) {
  lc_pouch *pouch;

  pouch = (lc_pouch *)arg;
  pthread_mutex_lock(&pouch->indexer_mutex);
  while (!pouch->indexer_stop) {
    lc_pouch_indexer_pending_namespace *batch;
    struct timespec deadline;
    int wait_rc;

    while (!pouch->indexer_stop && pouch->indexer_pending_namespaces == NULL) {
      (void)pthread_cond_wait(&pouch->indexer_cond, &pouch->indexer_mutex);
    }
    if (pouch->indexer_stop) {
      break;
    }
    lc_pouch_indexer_deadline(pouch, &deadline);
    wait_rc = 0;
    while (!pouch->indexer_stop &&
           !lc_pouch_indexer_flush_limit_reached_locked(pouch) &&
           wait_rc != ETIMEDOUT) {
      wait_rc = pthread_cond_timedwait(&pouch->indexer_cond,
                                       &pouch->indexer_mutex, &deadline);
    }
    if (pouch->indexer_stop) {
      break;
    }
    batch = pouch->indexer_pending_namespaces;
    pouch->indexer_pending_namespaces = NULL;
    pthread_mutex_unlock(&pouch->indexer_mutex);
    lc_pouch_indexer_run_batch(pouch, batch);
    pthread_mutex_lock(&pouch->indexer_mutex);
  }
  pthread_mutex_unlock(&pouch->indexer_mutex);
  return NULL;
}

static int lc_pouch_indexer_worker_init(lc_pouch *pouch, lc_error *error) {
  int pthread_rc;

  if (pouch == NULL) {
    return LC_OK;
  }
  pthread_rc = pthread_mutex_init(&pouch->indexer_mutex, NULL);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch indexer mutex",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->indexer_mutex_initialized = 1;
  pthread_rc = pthread_cond_init(&pouch->indexer_cond, NULL);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch indexer condition",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->indexer_cond_initialized = 1;
  pthread_rc = pthread_create(&pouch->indexer_thread, NULL,
                              lc_pouch_indexer_worker, pouch);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to start pouch indexer worker",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->indexer_thread_started = 1;
  return LC_OK;
}

static void lc_pouch_indexer_worker_close(lc_pouch *pouch) {
  if (pouch == NULL) {
    return;
  }
  if (pouch->indexer_thread_started) {
    pthread_mutex_lock(&pouch->indexer_mutex);
    pouch->indexer_stop = 1;
    pthread_cond_broadcast(&pouch->indexer_cond);
    pthread_mutex_unlock(&pouch->indexer_mutex);
    pthread_join(pouch->indexer_thread, NULL);
    pouch->indexer_thread_started = 0;
  }
  if (pouch->indexer_cond_initialized) {
    pthread_cond_destroy(&pouch->indexer_cond);
    pouch->indexer_cond_initialized = 0;
  }
  if (pouch->indexer_mutex_initialized) {
    lc_pouch_indexer_pending_namespaces_cleanup(
        pouch, pouch->indexer_pending_namespaces);
    pouch->indexer_pending_namespaces = NULL;
    pthread_mutex_destroy(&pouch->indexer_mutex);
    pouch->indexer_mutex_initialized = 0;
  }
}

void lc_pouch_indexer_note_mutation(lc_pouch *pouch,
                                    const char *namespace_name) {
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      pouch->aborted || !pouch->indexer_thread_started) {
    return;
  }
  pthread_mutex_lock(&pouch->indexer_mutex);
  rc = pouch->indexer_stop
           ? LC_ERR_INVALID
           : lc_pouch_indexer_queue_namespace_locked(pouch, namespace_name);
  if (rc == LC_OK) {
    pthread_cond_signal(&pouch->indexer_cond);
  }
  pthread_mutex_unlock(&pouch->indexer_mutex);
  if (rc != LC_OK && rc != LC_ERR_INVALID) {
    pslog_field fields[1];

    fields[0] = lc_log_str_field("ns", namespace_name);
    lc_log_warn(pouch->logger, "index.background.queue.error", fields, 1U);
  }
}

static void lc_pouch_janitor_run_pass(lc_pouch *pouch) {
  char **namespaces;
  size_t namespace_count;
  size_t index;
  time_t wall_now;
  lc_pouch_unix_seconds now;
  lc_pouch_unix_seconds cutoff;
  lc_error error;
  int rc;

  if (pouch == NULL || pouch->retention_seconds == 0U) {
    return;
  }
  wall_now = time(NULL);
  now = wall_now > 0 ? (lc_pouch_unix_seconds)wall_now : 0L;
  if (now <= 0L || (uint64_t)now <= pouch->retention_seconds) {
    return;
  }
  cutoff = (lc_pouch_unix_seconds)((uint64_t)now - pouch->retention_seconds);
  lc_error_init(&error);
  rc = lc_pouch_compaction_copy_namespaces(pouch, &namespaces, &namespace_count,
                                           &error);
  if (rc != LC_OK) {
    pslog_field fields[2];

    fields[0] = lc_log_error_field("error", &error);
    fields[1] = lc_log_code_field(&error);
    lc_log_warn(pouch->logger, "janitor.namespaces.error", fields, 2U);
    lc_error_cleanup(&error);
    return;
  }
  lc_error_cleanup(&error);
  for (index = 0U; index < namespace_count; ++index) {
    lc_pouch_maintenance_options options;
    lc_error maintenance_error;

    memset(&options, 0, sizeof(options));
    options.namespace_name = namespaces[index];
    options.retention_updated_before_unix = cutoff;
    lc_error_init(&maintenance_error);
    rc = lc_pouch_maintenance_run(pouch, &options, NULL, &maintenance_error);
    if (rc != LC_OK) {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("ns", namespaces[index]);
      fields[1] = lc_log_error_field("error", &maintenance_error);
      fields[2] = lc_log_code_field(&maintenance_error);
      lc_log_warn(pouch->logger, "janitor.retention.error", fields, 3U);
    }
    lc_error_cleanup(&maintenance_error);
  }
  lc_pouch_compaction_namespaces_cleanup(pouch, namespaces, namespace_count);
}

static void *lc_pouch_janitor_worker(void *arg) {
  lc_pouch *pouch;

  pouch = (lc_pouch *)arg;
  pthread_mutex_lock(&pouch->janitor_mutex);
  while (!pouch->janitor_stop) {
    struct timespec deadline;
    int wait_rc;

    while (!pouch->janitor_stop && !pouch->janitor_pending) {
      pthread_cond_wait(&pouch->janitor_cond, &pouch->janitor_mutex);
    }
    if (pouch->janitor_stop) {
      break;
    }
    lc_pouch_janitor_deadline(pouch, &deadline);
    wait_rc = 0;
    while (!pouch->janitor_stop && wait_rc != ETIMEDOUT) {
      wait_rc = pthread_cond_timedwait(&pouch->janitor_cond,
                                       &pouch->janitor_mutex, &deadline);
    }
    if (pouch->janitor_stop) {
      break;
    }
    pouch->janitor_pending = 0;
    pthread_mutex_unlock(&pouch->janitor_mutex);
    lc_pouch_janitor_run_pass(pouch);
    pthread_mutex_lock(&pouch->janitor_mutex);
  }
  pthread_mutex_unlock(&pouch->janitor_mutex);
  return NULL;
}

static int lc_pouch_janitor_worker_init(lc_pouch *pouch, lc_error *error) {
  int pthread_rc;

  if (pouch == NULL || pouch->retention_seconds == 0U) {
    return LC_OK;
  }
  pthread_rc = pthread_mutex_init(&pouch->janitor_mutex, NULL);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch janitor mutex",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->janitor_mutex_initialized = 1;
  pthread_rc = pthread_cond_init(&pouch->janitor_cond, NULL);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch janitor condition",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->janitor_cond_initialized = 1;
  pthread_rc = pthread_create(&pouch->janitor_thread, NULL,
                              lc_pouch_janitor_worker, pouch);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to start pouch janitor worker",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->janitor_thread_started = 1;
  return LC_OK;
}

static void lc_pouch_janitor_worker_close(lc_pouch *pouch) {
  if (pouch == NULL) {
    return;
  }
  if (pouch->janitor_thread_started) {
    pthread_mutex_lock(&pouch->janitor_mutex);
    pouch->janitor_stop = 1;
    pthread_cond_broadcast(&pouch->janitor_cond);
    pthread_mutex_unlock(&pouch->janitor_mutex);
    pthread_join(pouch->janitor_thread, NULL);
    pouch->janitor_thread_started = 0;
  }
  if (pouch->janitor_cond_initialized) {
    pthread_cond_destroy(&pouch->janitor_cond);
    pouch->janitor_cond_initialized = 0;
  }
  if (pouch->janitor_mutex_initialized) {
    pthread_mutex_destroy(&pouch->janitor_mutex);
    pouch->janitor_mutex_initialized = 0;
  }
}

void lc_pouch_janitor_note_mutation(lc_pouch *pouch) {
  if (pouch == NULL) {
    return;
  }
  lc_pouch_compaction_note_mutation(pouch);
  if (pouch->retention_seconds == 0U || pouch->aborted ||
      !pouch->janitor_mutex_initialized) {
    return;
  }
  pthread_mutex_lock(&pouch->janitor_mutex);
  if (!pouch->janitor_stop) {
    pouch->janitor_pending = 1;
    pthread_cond_signal(&pouch->janitor_cond);
  }
  pthread_mutex_unlock(&pouch->janitor_mutex);
}

int lc_pouch_fsync_commit(lc_pouch *pouch, int fd, lc_error *error) {
  lc_pouch_fsync_request request;
  lc_pouch_fsync_batcher *batcher;
  int pthread_rc;

  if (pouch == NULL || fd < 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch fsync commit requires pouch and fd", NULL, NULL,
                        "pouch");
  }
  if (!pouch->durable_sync) {
    return LC_OK;
  }
  if (pouch->aborted) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch fsync batcher is closed after abort", NULL, NULL,
                        "pouch");
  }
  batcher = pouch->fsync_batcher;
  if (batcher == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch fsync batcher is closed", NULL, NULL, "pouch");
  }
  memset(&request, 0, sizeof(request));
  request.fd = fd;
  pthread_rc = pthread_cond_init(&request.cond, NULL);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch fsync request",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pthread_mutex_lock(&batcher->mutex);
  if (batcher->stop) {
    pthread_mutex_unlock(&batcher->mutex);
    pthread_cond_destroy(&request.cond);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch fsync batcher is closed", NULL, NULL, "pouch");
  }
  if (batcher->tail != NULL) {
    batcher->tail->next = &request;
  } else {
    batcher->head = &request;
  }
  batcher->tail = &request;
  ++batcher->queue_count;
  pthread_cond_signal(&batcher->cond);
  while (!request.done) {
    pthread_cond_wait(&request.cond, &batcher->mutex);
  }
  pthread_mutex_unlock(&batcher->mutex);
  pthread_cond_destroy(&request.cond);
  if (request.errnum != 0) {
    pslog_field fields[2];

    fields[0] = lc_log_i64_field("fd", fd);
    fields[1] = lc_log_str_field("error", strerror(request.errnum));
    lc_log_error(pouch->logger, "logstore.fsync.error", fields, 2U);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to sync pouch state segment",
                        strerror(request.errnum), NULL, "pouch");
  }
  {
    pslog_field fields[2];

    fields[0] = lc_log_i64_field("fd", fd);
    fields[1] = lc_log_bool_field("batched", 1);
    lc_log_trace(pouch->logger, "logstore.fsync", fields, 2U);
  }
  return LC_OK;
}

int lc_pouch_queue_watch_wait(lc_pouch *pouch, const char *namespace_name,
                              const char *queue, uint64_t timeout_ms) {
#if defined(__linux__)
  char *namespace_path;
  char *notify_dir;
  char *escaped_queue;
  char *notify_leaf;
  struct pollfd pollfd;
  union {
    unsigned char bytes[4096];
    struct inotify_event alignment;
  } events;
  size_t leaf_len;
  size_t offset;
  ssize_t bytes_read;
  int fd;
  int watch;
  int timeout;
  int ready;
  int result;
  uint64_t timeout_ns;
  uint64_t elapsed_ns;
  uint64_t remaining_ns;
  struct timespec started;
  struct timespec now;
  int clock_started;

  if (pouch == NULL || !pouch->queue_watch_enabled || pouch->aborted ||
      namespace_name == NULL || namespace_name[0] == '\0' || queue == NULL ||
      queue[0] == '\0') {
    return -1;
  }
  namespace_path = lc_pouch_namespace_path(&pouch->allocator, pouch->root_path,
                                           namespace_name);
  notify_dir = namespace_path != NULL
                   ? lc_pouch_path_join(&pouch->allocator, namespace_path,
                                        "queue-notify")
                   : NULL;
  escaped_queue = lc_pouch_path_escape_name(&pouch->allocator, queue);
  notify_leaf = NULL;
  result = -1;
  if (notify_dir == NULL || escaped_queue == NULL ||
      strlen(escaped_queue) > SIZE_MAX - strlen(".notify") - 1U) {
    goto cleanup;
  }
  leaf_len = strlen(escaped_queue) + strlen(".notify") + 1U;
  notify_leaf = (char *)lc_alloc_with_allocator(&pouch->allocator, leaf_len);
  if (notify_leaf == NULL) {
    goto cleanup;
  }
  snprintf(notify_leaf, leaf_len, "%s.notify", escaped_queue);
  fd = inotify_init();
  if (fd < 0) {
    goto cleanup;
  }
  watch = inotify_add_watch(fd, notify_dir,
                            IN_MOVED_TO | IN_CREATE | IN_CLOSE_WRITE |
                                IN_ATTRIB | IN_DELETE_SELF | IN_MOVE_SELF);
  if (watch < 0) {
    (void)close(fd);
    goto cleanup;
  }
  result = 0;
  timeout_ns = timeout_ms > LC_U64_MAX / (uint64_t)1000000UL
                   ? LC_U64_MAX
                   : timeout_ms * (uint64_t)1000000UL;
  clock_started = clock_gettime(CLOCK_MONOTONIC, &started) == 0;
  memset(&pollfd, 0, sizeof(pollfd));
  pollfd.fd = fd;
  pollfd.events = POLLIN;
  for (;;) {
    if (clock_started && clock_gettime(CLOCK_MONOTONIC, &now) == 0) {
      elapsed_ns = lc_pouch_elapsed_ns(&started, &now);
      if (elapsed_ns >= timeout_ns) {
        break;
      }
      remaining_ns = timeout_ns - elapsed_ns;
      timeout = remaining_ns / (uint64_t)1000000UL >= (uint64_t)INT_MAX
                    ? INT_MAX
                    : (int)((remaining_ns + (uint64_t)999999UL) /
                            (uint64_t)1000000UL);
    } else {
      timeout = timeout_ms > (uint64_t)INT_MAX ? INT_MAX : (int)timeout_ms;
    }
    pollfd.revents = 0;
    ready = poll(&pollfd, 1U, timeout);
    if (ready == 0) {
      break;
    }
    if (ready < 0) {
      if (errno == EINTR) {
        continue;
      }
      break;
    }
    if ((pollfd.revents & POLLIN) == 0) {
      break;
    }
    bytes_read = read(fd, events.bytes, sizeof(events.bytes));
    if (bytes_read < 0) {
      if (errno == EINTR) {
        continue;
      }
      break;
    }
    offset = 0U;
    while (offset + sizeof(struct inotify_event) <= (size_t)bytes_read) {
      const struct inotify_event *event;
      size_t event_size;

      event = (const struct inotify_event *)(events.bytes + offset);
      event_size = sizeof(*event) + event->len;
      if (event_size > (size_t)bytes_read - offset) {
        break;
      }
      if ((event->mask & (IN_Q_OVERFLOW | IN_DELETE_SELF | IN_MOVE_SELF)) !=
              0U ||
          (event->len > 0U && strcmp(event->name, notify_leaf) == 0)) {
        result = 1;
        break;
      }
      offset += event_size;
    }
    if (result != 0) {
      break;
    }
  }
  (void)inotify_rm_watch(fd, watch);
  (void)close(fd);

cleanup:
  lc_free_with_allocator(&pouch->allocator, notify_leaf);
  lc_free_with_allocator(&pouch->allocator, escaped_queue);
  lc_free_with_allocator(&pouch->allocator, notify_dir);
  lc_free_with_allocator(&pouch->allocator, namespace_path);
  return result;
#else
  (void)pouch;
  (void)namespace_name;
  (void)queue;
  (void)timeout_ms;
  return -1;
#endif
}

static const char *lc_pouch_option_string(const char *value,
                                          const char *fallback) {
  if (value != NULL && value[0] != '\0') {
    return value;
  }
  return fallback;
}

static const char *
lc_pouch_compression_option(const lc_pouch_open_options *options) {
  const char *value;

  value = options != NULL ? options->compression : NULL;
  if (value == NULL || value[0] == '\0') {
    return "none";
  }
  return value;
}

static void lc_pouch_init_options(lc_pouch *pouch,
                                  const lc_pouch_open_options *options) {
  pouch->segment_target_bytes =
      options != NULL && options->segment_target_bytes != 0U
          ? options->segment_target_bytes
          : LC_POUCH_DEFAULT_SEGMENT_TARGET_BYTES;
  pouch->indexer_flush_docs =
      options != NULL && options->indexer_flush_docs != 0U
          ? options->indexer_flush_docs
          : (uint64_t)LC_POUCH_DEFAULT_INDEXER_FLUSH_DOCS;
  pouch->indexer_flush_interval_seconds =
      options != NULL && options->indexer_flush_interval_seconds != 0U
          ? options->indexer_flush_interval_seconds
          : (uint64_t)LC_POUCH_DEFAULT_INDEXER_FLUSH_INTERVAL_SECONDS;
  pouch->fsync_batch_max_ops =
      options != NULL ? options->fsync_batch_max_ops : 0U;
  pouch->durable_sync = options != NULL && options->durable_sync ? 1 : 0;
  pouch->compaction_min_segment_count =
      options != NULL && options->compaction_min_segment_count != 0UL
          ? options->compaction_min_segment_count
          : LC_POUCH_DEFAULT_COMPACTION_MIN_SEGMENT_COUNT;
  pouch->compaction_min_reclaimable_bytes =
      options != NULL && options->compaction_min_reclaimable_bytes != 0U
          ? options->compaction_min_reclaimable_bytes
          : LC_POUCH_DEFAULT_COMPACTION_MIN_RECLAIMABLE_BYTES;
  pouch->compaction_interval_seconds =
      options != NULL && options->compaction_interval_seconds != 0U
          ? options->compaction_interval_seconds
          : LC_POUCH_DEFAULT_COMPACTION_INTERVAL_SECONDS;
  pouch->compaction_delete_grace_seconds =
      options != NULL && options->compaction_delete_grace_seconds != 0U
          ? options->compaction_delete_grace_seconds
          : LC_POUCH_DEFAULT_COMPACTION_DELETE_GRACE_SECONDS;
  pouch->compaction_throttling_disabled =
      options != NULL && options->compaction_throttling_disabled ? 1 : 0;
  pouch->compaction_max_io_bytes_per_sec =
      pouch->compaction_throttling_disabled
          ? 0U
          : (options != NULL && options->compaction_max_io_bytes_per_sec != 0U
                 ? options->compaction_max_io_bytes_per_sec
                 : LC_POUCH_DEFAULT_COMPACTION_MAX_IO_BYTES_PER_SEC);
  pouch->background_compaction_enabled =
      options != NULL && options->background_compaction_enabled_set
          ? (options->background_compaction_enabled ? 1 : 0)
          : 1;
  pouch->retention_seconds = options != NULL ? options->retention_seconds : 0U;
  pouch->janitor_interval_seconds =
      options != NULL && options->janitor_interval_seconds != 0U
          ? options->janitor_interval_seconds
          : LC_POUCH_DEFAULT_JANITOR_INTERVAL_SECONDS;
  pouch->single_writer = options != NULL && options->single_writer_set
                             ? (options->single_writer != 0 ? 1 : 0)
                             : 1;
  pouch->queue_watch_enabled = options != NULL ? options->queue_watch : 0;
  pouch->queue_watch_mode = "polling";
  pouch->queue_watch_reason = "config_disabled";
  pouch->query_engine = lc_strdup_with_allocator(
      &pouch->allocator,
      lc_pouch_option_string(options != NULL ? options->query_engine : NULL,
                             "index"));
  pouch->query_fallback_engine = lc_strdup_with_allocator(
      &pouch->allocator,
      lc_pouch_option_string(
          options != NULL ? options->query_fallback_engine : NULL, ""));
  pouch->compression = lc_strdup_with_allocator(
      &pouch->allocator, lc_pouch_compression_option(options));
}

static void lc_pouch_detect_filesystem_capabilities(lc_pouch *pouch) {
#if defined(__linux__)
  struct statfs st;

  if (pouch != NULL && pouch->root_path != NULL &&
      statfs(pouch->root_path, &st) == 0) {
    pouch->filesystem_capabilities_known = 1;
    pouch->filesystem_is_nfs = st.f_type == 0x6969;
  }
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) ||     \
    defined(__OpenBSD__) || defined(__DragonFly__)
  struct statfs st;

  if (pouch != NULL && pouch->root_path != NULL &&
      statfs(pouch->root_path, &st) == 0) {
    pouch->filesystem_capabilities_known = 1;
    pouch->filesystem_is_nfs = strcasecmp(st.f_fstypename, "nfs") == 0 ||
                               strcasecmp(st.f_fstypename, "nfs4") == 0;
  }
#else
  (void)pouch;
#endif
}

static int lc_pouch_queue_watch_supported(const lc_pouch *pouch) {
#if defined(__linux__)
  return pouch != NULL && pouch->filesystem_capabilities_known &&
         !pouch->filesystem_is_nfs;
#else
  (void)pouch;
  return 0;
#endif
}

static void lc_pouch_configure_filesystem_capabilities(lc_pouch *pouch) {
  int requested;

  if (pouch == NULL) {
    return;
  }
  pouch->filesystem_capabilities_known = 0;
  pouch->filesystem_is_nfs = 0;
  lc_pouch_detect_filesystem_capabilities(pouch);
  requested = pouch->queue_watch_enabled;
  pouch->queue_watch_enabled = 0;
  pouch->queue_watch_mode = "polling";
  pouch->queue_watch_reason = "config_disabled";
  if (requested) {
    if (lc_pouch_queue_watch_supported(pouch)) {
      pouch->queue_watch_enabled = 1;
      pouch->queue_watch_mode = "fsnotify";
      pouch->queue_watch_reason = "filesystem_watch_enabled";
    } else {
      pouch->queue_watch_reason = "filesystem_not_supported";
    }
  }
}

static int lc_pouch_write_root_manifest(lc_pouch *pouch, lc_error *error) {
  char manifest[512];
  char *crypto_key_id;
  char *manifest_path;
  int rc;

  crypto_key_id = NULL;
  if (lc_pouch_crypto_enabled(pouch->crypto)) {
    rc = lc_pouch_crypto_key_id(pouch->crypto, &crypto_key_id, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  manifest_path =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, "manifest");
  if (manifest_path == NULL) {
    lc_free_with_allocator(NULL, crypto_key_id);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch root manifest path", NULL,
                        NULL, NULL);
  }
  snprintf(manifest, sizeof(manifest),
           "layout=%s\nversion=%lu\ncompression=%s\ncrypto=%s\n"
           "crypto_key_id=%s\n",
           LC_POUCH_LAYOUT_NAME, LC_POUCH_LAYOUT_VERSION, pouch->compression,
           lc_pouch_crypto_enabled(pouch->crypto) ? "encrypted" : "plaintext",
           crypto_key_id != NULL ? crypto_key_id : "");
  rc = lc_pouch_path_write_text_file(manifest_path, manifest, error);
  lc_free_with_allocator(&pouch->allocator, manifest_path);
  lc_free_with_allocator(NULL, crypto_key_id);
  return rc;
}

static int lc_pouch_root_manifest_read(
    lc_pouch *pouch, char *layout, size_t layout_size, unsigned long *version,
    char *mode, size_t mode_size, char *key_id, size_t key_id_size,
    int *found_manifest, int *found_crypto_mode, char *compression,
    size_t compression_size, int *found_compression, lc_error *error) {
  char line[512];
  char *manifest_path;
  FILE *fp;
  int found_key_id;
  int found_layout;
  int found_mode;
  int found_compression_mode;
  int found_version;
  int rc;

  if (layout == NULL || layout_size == 0U || version == NULL || mode == NULL ||
      mode_size == 0U || key_id == NULL || key_id_size == 0U ||
      found_manifest == NULL || found_crypto_mode == NULL ||
      compression == NULL || compression_size == 0U ||
      found_compression == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch root manifest read requires outputs", NULL, NULL,
                        NULL);
  }
  layout[0] = '\0';
  *version = 0UL;
  mode[0] = '\0';
  key_id[0] = '\0';
  compression[0] = '\0';
  *found_manifest = 0;
  *found_crypto_mode = 0;
  *found_compression = 0;
  manifest_path =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, "manifest");
  if (manifest_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch root manifest path", NULL,
                        NULL, NULL);
  }
  fp = fopen(manifest_path, "rb");
  if (fp == NULL) {
    rc = errno == ENOENT ? LC_OK
                         : lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                                        "failed to open pouch root manifest",
                                        strerror(errno), NULL, "pouch");
    lc_free_with_allocator(&pouch->allocator, manifest_path);
    return rc;
  }
  *found_manifest = 1;
  found_layout = 0;
  found_version = 0;
  found_mode = 0;
  found_compression_mode = 0;
  found_key_id = 0;
  rc = LC_OK;
  while (fgets(line, sizeof(line), fp) != NULL) {
    char *value;
    size_t len;

    if (strncmp(line, "layout=", 7U) == 0) {
      if (found_layout) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest has duplicate layout", NULL,
                          NULL, "pouch");
        break;
      }
      value = line + 7U;
      len = strcspn(value, "\r\n");
      value[len] = '\0';
      if (len == 0U || len + 1U > layout_size) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest layout is invalid", NULL, NULL,
                          "pouch");
        break;
      }
      memcpy(layout, value, len + 1U);
      found_layout = 1;
      continue;
    }
    if (strncmp(line, "version=", 8U) == 0) {
      char *end;
      unsigned long parsed;

      if (found_version) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest has duplicate version", NULL,
                          NULL, "pouch");
        break;
      }
      value = line + 8U;
      len = strcspn(value, "\r\n");
      value[len] = '\0';
      end = NULL;
      parsed = strtoul(value, &end, 10);
      if (end == value || (end != NULL && *end != '\0') || parsed == 0UL) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest version is invalid", NULL, NULL,
                          "pouch");
        break;
      }
      *version = parsed;
      found_version = 1;
      continue;
    }
    if (strncmp(line, "crypto=", 7U) == 0) {
      if (found_mode) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest has duplicate crypto mode", NULL,
                          NULL, "pouch");
        break;
      }
      value = line + 7U;
      len = strcspn(value, "\r\n");
      value[len] = '\0';
      if (strcmp(value, "plaintext") != 0 && strcmp(value, "encrypted") != 0) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest has invalid crypto mode", NULL,
                          NULL, "pouch");
        break;
      }
      if (len + 1U > mode_size) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest crypto mode is too long", NULL,
                          NULL, "pouch");
        break;
      }
      memcpy(mode, value, len + 1U);
      found_mode = 1;
      continue;
    }
    if (strncmp(line, "compression=", 12U) == 0) {
      if (found_compression_mode) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest has duplicate compression mode",
                          NULL, NULL, "pouch");
        break;
      }
      value = line + 12U;
      len = strcspn(value, "\r\n");
      value[len] = '\0';
      if (strcmp(value, "none") != 0 && strcmp(value, "zlib") != 0) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest has invalid compression mode",
                          NULL, NULL, "pouch");
        break;
      }
      if (len + 1U > compression_size) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest compression mode is too long",
                          NULL, NULL, "pouch");
        break;
      }
      memcpy(compression, value, len + 1U);
      found_compression_mode = 1;
      continue;
    }
    if (strncmp(line, "crypto_key_id=", 14U) == 0) {
      if (found_key_id) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest has duplicate crypto key id",
                          NULL, NULL, "pouch");
        break;
      }
      value = line + 14U;
      len = strcspn(value, "\r\n");
      value[len] = '\0';
      if (len + 1U > key_id_size) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest crypto key id is too long", NULL,
                          NULL, "pouch");
        break;
      }
      memcpy(key_id, value, len + 1U);
      found_key_id = 1;
    }
  }
  if (ferror(fp) && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to read pouch root manifest", strerror(errno),
                      NULL, "pouch");
  }
  if (fclose(fp) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch root manifest", strerror(errno),
                      NULL, "pouch");
  }
  lc_free_with_allocator(&pouch->allocator, manifest_path);
  if (rc == LC_OK && !found_layout) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch root manifest requires layout", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK && !found_version) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch root manifest requires version", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK && !found_mode) {
    (void)snprintf(mode, mode_size, "plaintext");
  }
  if (rc == LC_OK && !found_compression_mode) {
    (void)snprintf(compression, compression_size, "none");
  }
  if (rc == LC_OK) {
    *found_crypto_mode = found_mode;
    *found_compression = found_compression_mode;
  }
  return rc;
}

static int lc_pouch_root_has_namespace_entries(lc_pouch *pouch, int *out,
                                               lc_error *error) {
  char *namespaces_path;
  DIR *dir;
  struct dirent *entry;
  int saved_errno;

  if (pouch == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch root namespace scan requires pouch and output",
                        NULL, NULL, NULL);
  }
  *out = 0;
  namespaces_path =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, "namespaces");
  if (namespaces_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespaces path", NULL, NULL,
                        NULL);
  }
  dir = opendir(namespaces_path);
  if (dir == NULL) {
    saved_errno = errno;
    lc_free_with_allocator(&pouch->allocator, namespaces_path);
    if (saved_errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to scan pouch namespaces",
                        strerror(saved_errno), NULL, "pouch");
  }
  errno = 0;
  while ((entry = readdir(dir)) != NULL) {
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    *out = 1;
    break;
  }
  saved_errno = errno;
  if (closedir(dir) != 0 && saved_errno == 0) {
    saved_errno = errno != 0 ? errno : EIO;
  }
  lc_free_with_allocator(&pouch->allocator, namespaces_path);
  if (saved_errno != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to scan pouch namespaces",
                        strerror(saved_errno), NULL, "pouch");
  }
  return LC_OK;
}

/*
 * Exclusive Pouch pays this bounded, read-only cache construction at open so
 * its first query does not decode immutable segment readers. Plain shared
 * roots remain lazy to keep their optional multi-writer resident footprint
 * bounded. Transformed roots retain their existing eager warming in either
 * writer mode.
 */
static int lc_pouch_warm_open_namespaces(lc_pouch *pouch, lc_error *error) {
  char *namespaces_path;
  DIR *dir;
  struct dirent *entry;
  unsigned long namespace_count;
  int saved_errno;
  int rc;

  if (pouch == NULL || (!lc_pouch_single_writer_enabled(pouch) &&
                        !lc_pouch_crypto_enabled(pouch->crypto) &&
                        !lc_pouch_crypto_compression_enabled(pouch->crypto))) {
    return LC_OK;
  }
  namespaces_path =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, "namespaces");
  if (namespaces_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespaces path", NULL, NULL,
                        NULL);
  }
  dir = opendir(namespaces_path);
  if (dir == NULL) {
    saved_errno = errno;
    lc_free_with_allocator(&pouch->allocator, namespaces_path);
    if (saved_errno == ENOENT) {
      pslog_field fields[1];

      fields[0] = lc_log_str_field("reason", "namespaces-missing");
      lc_log_debug(pouch->logger, "cache.load.skip", fields, 1U);
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to scan pouch namespaces",
                        strerror(saved_errno), NULL, "pouch");
  }
  rc = LC_OK;
  saved_errno = 0;
  namespace_count = 0UL;
  {
    pslog_field fields[2];

    fields[0] =
        lc_log_bool_field("crypto", lc_pouch_crypto_enabled(pouch->crypto));
    fields[1] = lc_log_bool_field(
        "compression", lc_pouch_crypto_compression_enabled(pouch->crypto));
    lc_log_debug(pouch->logger, "cache.load.start", fields, 2U);
  }
  while (rc == LC_OK) {
    char *namespace_name;
    lc_error warm_error;
    int index_rc;
    int state_rc;

    errno = 0;
    entry = readdir(dir);
    if (entry == NULL) {
      saved_errno = errno;
      break;
    }
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    namespace_name =
        lc_pouch_path_unescape_name(&pouch->allocator, entry->d_name);
    if (namespace_name == NULL) {
      continue;
    }
    lc_error_init(&warm_error);
    state_rc =
        lc_pouch_state_warm_namespace(pouch, namespace_name, &warm_error);
    if (state_rc != LC_OK) {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("ns", namespace_name);
      fields[1] = lc_log_error_field("error", &warm_error);
      fields[2] = lc_log_code_field(&warm_error);
      lc_log_warn(pouch->logger, "cache.load.state.error", fields, 3U);
    }
    lc_error_cleanup(&warm_error);
    lc_error_init(&warm_error);
    index_rc =
        lc_pouch_query_index_warm_namespace(pouch, namespace_name, &warm_error);
    if (index_rc != LC_OK) {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("ns", namespace_name);
      fields[1] = lc_log_error_field("error", &warm_error);
      fields[2] = lc_log_code_field(&warm_error);
      lc_log_warn(pouch->logger, "cache.load.index.error", fields, 3U);
    }
    lc_error_cleanup(&warm_error);
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("ns", namespace_name);
      fields[1] = lc_log_bool_field("state_ok", state_rc == LC_OK);
      fields[2] = lc_log_bool_field("index_ok", index_rc == LC_OK);
      lc_log_debug(pouch->logger, "cache.load.namespace", fields, 3U);
    }
    namespace_count++;
    lc_free_with_allocator(&pouch->allocator, namespace_name);
  }
  if (closedir(dir) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to scan pouch namespaces",
                      strerror(errno != 0 ? errno : EIO), NULL, "pouch");
  } else if (rc == LC_OK && saved_errno != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to scan pouch namespaces", strerror(saved_errno),
                      NULL, "pouch");
  }
  lc_free_with_allocator(&pouch->allocator, namespaces_path);
  if (rc == LC_OK) {
    pslog_field fields[1];

    fields[0] = lc_log_u64_field("count", namespace_count);
    lc_log_debug(pouch->logger, "cache.load.complete", fields, 1U);
  }
  return rc;
}

static int lc_pouch_validate_root_crypto_mode(lc_pouch *pouch,
                                              lc_error *error) {
  char stored_key_id[128];
  char stored_layout[64];
  char stored_mode[sizeof("encrypted")];
  char stored_compression[sizeof("zlib")];
  char *requested_key_id;
  const char *requested_mode;
  const char *requested_compression;
  unsigned long stored_version;
  int found_crypto_mode;
  int found_compression;
  int found_manifest;
  int has_namespace_entries;
  int rc;

  requested_key_id = NULL;
  requested_mode =
      lc_pouch_crypto_enabled(pouch->crypto) ? "encrypted" : "plaintext";
  requested_compression =
      pouch->compression != NULL && strcmp(pouch->compression, "zlib") == 0
          ? "zlib"
          : "none";
  stored_version = 0UL;
  found_crypto_mode = 0;
  found_compression = 0;
  rc = lc_pouch_root_manifest_read(
      pouch, stored_layout, sizeof(stored_layout), &stored_version, stored_mode,
      sizeof(stored_mode), stored_key_id, sizeof(stored_key_id),
      &found_manifest, &found_crypto_mode, stored_compression,
      sizeof(stored_compression), &found_compression, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!found_manifest) {
    has_namespace_entries = 0;
    rc = lc_pouch_root_has_namespace_entries(pouch, &has_namespace_entries,
                                             error);
    if (rc != LC_OK) {
      return rc;
    }
    if (has_namespace_entries) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root manifest is required for populated root",
                          NULL, NULL, "pouch");
    }
    return LC_OK;
  }
  if (strcmp(stored_layout, LC_POUCH_LAYOUT_NAME) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch root manifest layout is unsupported", NULL, NULL,
                        "pouch");
  }
  if (stored_version != LC_POUCH_LAYOUT_VERSION) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch root manifest version is unsupported", NULL,
                        NULL, "pouch");
  }
  if (!found_crypto_mode) {
    has_namespace_entries = 0;
    rc = lc_pouch_root_has_namespace_entries(pouch, &has_namespace_entries,
                                             error);
    if (rc != LC_OK) {
      return rc;
    }
    if (has_namespace_entries) {
      return lc_error_set(
          error, LC_ERR_INVALID, 0L,
          "pouch root manifest requires crypto mode for populated root", NULL,
          NULL, "pouch");
    }
    if (stored_key_id[0] != '\0') {
      return lc_error_set(
          error, LC_ERR_INVALID, 0L,
          "pouch root manifest crypto key id requires crypto mode", NULL, NULL,
          "pouch");
    }
    return LC_OK;
  }
  if (!found_compression) {
    has_namespace_entries = 0;
    rc = lc_pouch_root_has_namespace_entries(pouch, &has_namespace_entries,
                                             error);
    if (rc != LC_OK) {
      return rc;
    }
    if (has_namespace_entries) {
      return lc_error_set(
          error, LC_ERR_INVALID, 0L,
          "pouch root manifest requires compression mode for populated root",
          NULL, NULL, "pouch");
    }
    return LC_OK;
  }
  if (strcmp(stored_mode, requested_mode) != 0) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch root crypto mode cannot be changed after initialization", NULL,
        NULL, "pouch");
  }
  if (strcmp(stored_compression, requested_compression) != 0) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch root compression mode cannot be changed after initialization",
        NULL, NULL, "pouch");
  }
  if (strcmp(stored_mode, "plaintext") == 0 && stored_key_id[0] != '\0') {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch plaintext root manifest cannot contain crypto key id", NULL,
        NULL, "pouch");
  }
  if (strcmp(stored_mode, "encrypted") != 0) {
    return LC_OK;
  }
  if (stored_key_id[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted root manifest requires crypto key id",
                        NULL, NULL, "pouch");
  }
  rc = lc_pouch_crypto_key_id(pouch->crypto, &requested_key_id, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (strcmp(stored_key_id, requested_key_id) != 0) {
    lc_free_with_allocator(NULL, requested_key_id);
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch root crypto key does not match initialized encrypted root", NULL,
        NULL, "pouch");
  }
  lc_free_with_allocator(NULL, requested_key_id);
  return LC_OK;
}

static int lc_pouch_root_manifest_lock(lc_pouch *pouch, int *out,
                                       lc_error *error) {
  char *lock_path;
  struct flock fl;
  int fd;

  if (pouch == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch root manifest lock requires pouch and output",
                        NULL, NULL, NULL);
  }
  *out = -1;
  lock_path =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, "manifest.lock");
  if (lock_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch root manifest lock path",
                        NULL, NULL, NULL);
  }
  fd = open(lock_path, O_CREAT | O_RDWR, 0666);
  lc_free_with_allocator(&pouch->allocator, lock_path);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch root manifest lock",
                        strerror(errno), NULL, "pouch");
  }
  memset(&fl, 0, sizeof(fl));
  fl.l_type = F_WRLCK;
  fl.l_whence = SEEK_SET;
  while (fcntl(fd, F_SETLKW, &fl) != 0) {
    if (errno == EINTR) {
      continue;
    }
    close(fd);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch root manifest", strerror(errno),
                        NULL, "pouch");
  }
  *out = fd;
  return LC_OK;
}

static void lc_pouch_root_manifest_unlock(int fd) {
  struct flock fl;

  if (fd < 0) {
    return;
  }
  memset(&fl, 0, sizeof(fl));
  fl.l_type = F_UNLCK;
  fl.l_whence = SEEK_SET;
  (void)fcntl(fd, F_SETLK, &fl);
  close(fd);
}

static int lc_pouch_ensure_root(lc_pouch *pouch, lc_error *error) {
  char *namespaces_path;
  int manifest_mutex_locked;
  int lock_fd;
  int pthread_rc;
  int rc;

  rc = lc_pouch_path_ensure_directory(
      pouch->root_path, "failed to create pouch root directory", error);
  if (rc != LC_OK) {
    return rc;
  }
  namespaces_path =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, "namespaces");
  if (namespaces_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespaces path", NULL, NULL,
                        NULL);
  }
  rc = lc_pouch_path_ensure_directory(
      namespaces_path, "failed to create pouch namespaces directory", error);
  lc_free_with_allocator(&pouch->allocator, namespaces_path);
  if (rc != LC_OK) {
    return rc;
  }
  manifest_mutex_locked = 0;
  lock_fd = -1;
  pthread_rc = pthread_mutex_lock(&lc_pouch_root_manifest_mutex);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch root manifest mutex",
                        strerror(pthread_rc), NULL, "pouch");
  }
  manifest_mutex_locked = 1;
  rc = lc_pouch_root_manifest_lock(pouch, &lock_fd, error);
  if (rc == LC_OK) {
    rc = lc_pouch_validate_root_crypto_mode(pouch, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_write_root_manifest(pouch, error);
  }
  lc_pouch_root_manifest_unlock(lock_fd);
  if (manifest_mutex_locked) {
    pthread_mutex_unlock(&lc_pouch_root_manifest_mutex);
  }
  return rc;
}

static int lc_pouch_init_writer_marker(lc_pouch *pouch, lc_error *error) {
  unsigned char writer_random[16];
  char marker_leaf[128];
  char presence_leaf[128];
  char writer_hex[33];
  char writer_marker_text[32];
  uint64_t writer_marker_id;
  size_t index;

  if (RAND_bytes(writer_random, (int)sizeof(writer_random)) != 1) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to generate pouch writer identity", NULL, NULL,
                        "pouch");
  }
  for (index = 0U; index < sizeof(writer_random); ++index) {
    (void)snprintf(writer_hex + (index * 2U), 3U, "%02x", writer_random[index]);
  }
  writer_hex[sizeof(writer_hex) - 1U] = '\0';
  pthread_mutex_lock(&lc_pouch_writer_marker_mutex);
  writer_marker_id = ++lc_pouch_next_writer_marker_id;
  pthread_mutex_unlock(&lc_pouch_writer_marker_mutex);
  if (lc_u64_format_base10_padded((lc_u64)writer_marker_id, 20U,
                                  writer_marker_text,
                                  sizeof(writer_marker_text)) < 0 ||
      snprintf(marker_leaf, sizeof(marker_leaf), "writer-%s-%s.marker",
               writer_hex, writer_marker_text) < 0 ||
      snprintf(presence_leaf, sizeof(presence_leaf), "writer-%s-%s.presence",
               writer_hex, writer_marker_text) < 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "failed to format pouch writer marker", NULL, NULL,
                        "pouch");
  }
  pouch->writer_marker_leaf =
      lc_strdup_with_allocator(&pouch->allocator, marker_leaf);
  pouch->writer_presence_leaf =
      lc_strdup_with_allocator(&pouch->allocator, presence_leaf);
  pouch->writer_presence_dir = lc_pouch_path_join(
      &pouch->allocator, pouch->root_path, "exclusive-writers");
  pouch->writer_presence_path =
      pouch->writer_presence_dir != NULL
          ? lc_pouch_path_join(&pouch->allocator, pouch->writer_presence_dir,
                               presence_leaf)
          : NULL;
  if (pouch->writer_marker_leaf == NULL ||
      pouch->writer_presence_leaf == NULL ||
      pouch->writer_presence_dir == NULL ||
      pouch->writer_presence_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch writer marker paths", NULL,
                        NULL, "pouch");
  }
  return LC_OK;
}

static lc_pouch_writer_root_lock_entry *
lc_pouch_writer_root_lock_find(dev_t device, ino_t inode) {
  lc_pouch_writer_root_lock_entry *entry;

  entry = lc_pouch_writer_root_locks;
  while (entry != NULL) {
    if (entry->device == device && entry->inode == inode) {
      return entry;
    }
    entry = entry->next;
  }
  return NULL;
}

static void lc_pouch_writer_root_lock_release(lc_pouch *pouch) {
  lc_pouch_writer_root_lock_entry *entry;
  lc_pouch_writer_root_lock_entry **link;
  struct flock fl;

  if (pouch == NULL || pouch->writer_root_lock == NULL) {
    return;
  }
  pthread_mutex_lock(&lc_pouch_writer_root_lock_mutex);
  entry = pouch->writer_root_lock;
  if (pouch->writer_root_lock_mode == LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE) {
    if (entry->exclusive_holders > 0UL) {
      --entry->exclusive_holders;
    }
  } else if (pouch->writer_root_lock_mode == LC_POUCH_WRITER_ROOT_LOCK_SHARED &&
             entry->shared_holders > 0UL) {
    --entry->shared_holders;
  }
  pouch->writer_root_lock = NULL;
  pouch->writer_root_lock_mode = LC_POUCH_WRITER_ROOT_LOCK_NONE;
  if (entry->exclusive_holders == 0UL && entry->shared_holders == 0UL) {
    link = &lc_pouch_writer_root_locks;
    while (*link != NULL && *link != entry) {
      link = &(*link)->next;
    }
    if (*link == entry) {
      *link = entry->next;
    }
    memset(&fl, 0, sizeof(fl));
    fl.l_type = F_UNLCK;
    fl.l_whence = SEEK_SET;
    (void)fcntl(entry->fd, F_SETLK, &fl);
    (void)close(entry->fd);
    lc_free_with_allocator(&entry->allocator, entry);
  }
  pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
}

static int lc_pouch_writer_root_lock_acquire(lc_pouch *pouch, int mode,
                                             lc_error *error) {
  char *path;
  lc_pouch_writer_root_lock_entry *entry;
  struct flock fl;
  struct stat st;
  int fd;
  int saved_errno;

  if (pouch == NULL || (mode != LC_POUCH_WRITER_ROOT_LOCK_SHARED &&
                        mode != LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch writer root lock requires a valid mode", NULL,
                        NULL, "pouch");
  }
  if (pouch->writer_root_lock != NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch writer root lock is already held", NULL, NULL,
                        "pouch");
  }
  path = lc_pouch_path_join(&pouch->allocator, pouch->root_path,
                            "writer-mode.lock");
  if (path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch writer root lock path", NULL,
                        NULL, "pouch");
  }
  pthread_mutex_lock(&lc_pouch_writer_root_lock_mutex);
  if (stat(path, &st) == 0) {
    entry = lc_pouch_writer_root_lock_find(st.st_dev, st.st_ino);
    if (entry != NULL) {
      if ((mode == LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE &&
           (entry->exclusive_holders != 0UL || entry->shared_holders != 0UL)) ||
          (mode == LC_POUCH_WRITER_ROOT_LOCK_SHARED &&
           entry->exclusive_holders != 0UL)) {
        pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
        lc_free_with_allocator(&pouch->allocator, path);
        return lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch root writer mode is already owned", NULL,
                            NULL, "pouch");
      }
      if (mode == LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE) {
        ++entry->exclusive_holders;
      } else {
        ++entry->shared_holders;
      }
      pouch->writer_root_lock = entry;
      pouch->writer_root_lock_mode = mode;
      pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
      lc_free_with_allocator(&pouch->allocator, path);
      return LC_OK;
    }
  } else if (errno != ENOENT) {
    saved_errno = errno;
    pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
    lc_free_with_allocator(&pouch->allocator, path);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to stat pouch writer root lock",
                        strerror(saved_errno), NULL, "pouch");
  }
  fd = open(path, O_CREAT | O_RDWR, 0666);
  lc_free_with_allocator(&pouch->allocator, path);
  if (fd < 0) {
    saved_errno = errno;
    pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch writer root lock",
                        strerror(saved_errno), NULL, "pouch");
  }
  if (fstat(fd, &st) != 0) {
    saved_errno = errno;
    (void)close(fd);
    pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to stat pouch writer root lock",
                        strerror(saved_errno), NULL, "pouch");
  }
  memset(&fl, 0, sizeof(fl));
  fl.l_type = mode == LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE ? F_WRLCK : F_RDLCK;
  fl.l_whence = SEEK_SET;
  while (fcntl(fd, F_SETLK, &fl) != 0) {
    if (errno == EINTR) {
      continue;
    }
    saved_errno = errno;
    (void)close(fd);
    pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
    if (saved_errno == EACCES || saved_errno == EAGAIN) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root writer mode is already owned", NULL, NULL,
                          "pouch");
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch writer root",
                        strerror(saved_errno), NULL, "pouch");
  }
  entry = (lc_pouch_writer_root_lock_entry *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*entry));
  if (entry == NULL) {
    fl.l_type = F_UNLCK;
    (void)fcntl(fd, F_SETLK, &fl);
    (void)close(fd);
    pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch writer root lock", NULL, NULL,
                        "pouch");
  }
  entry->allocator = pouch->allocator;
  entry->device = st.st_dev;
  entry->inode = st.st_ino;
  entry->fd = fd;
  if (mode == LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE) {
    entry->exclusive_holders = 1UL;
  } else {
    entry->shared_holders = 1UL;
  }
  entry->next = lc_pouch_writer_root_locks;
  lc_pouch_writer_root_locks = entry;
  pouch->writer_root_lock = entry;
  pouch->writer_root_lock_mode = mode;
  pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
  return LC_OK;
}

static int lc_pouch_writer_root_lock_change(lc_pouch *pouch, int mode,
                                            lc_error *error) {
  lc_pouch_writer_root_lock_entry *entry;
  struct flock fl;
  int saved_errno;

  if (pouch == NULL || pouch->writer_root_lock == NULL ||
      (mode != LC_POUCH_WRITER_ROOT_LOCK_SHARED &&
       mode != LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch writer root lock cannot change mode", NULL, NULL,
                        "pouch");
  }
  pthread_mutex_lock(&lc_pouch_writer_root_lock_mutex);
  entry = pouch->writer_root_lock;
  if (pouch->writer_root_lock_mode == mode) {
    pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
    return LC_OK;
  }
  if (mode == LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE &&
      (entry->exclusive_holders != 0UL || entry->shared_holders != 1UL)) {
    pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch root has active shared writers", NULL, NULL,
                        "pouch");
  }
  if (mode == LC_POUCH_WRITER_ROOT_LOCK_SHARED &&
      (entry->exclusive_holders != 1UL || entry->shared_holders != 0UL)) {
    pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch root writer lock state is inconsistent", NULL,
                        NULL, "pouch");
  }
  memset(&fl, 0, sizeof(fl));
  fl.l_type = mode == LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE ? F_WRLCK : F_RDLCK;
  fl.l_whence = SEEK_SET;
  while (fcntl(entry->fd, F_SETLK, &fl) != 0) {
    if (errno == EINTR) {
      continue;
    }
    saved_errno = errno;
    pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
    if (saved_errno == EACCES || saved_errno == EAGAIN) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch root writer mode is already owned", NULL, NULL,
                          "pouch");
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to change pouch writer root lock",
                        strerror(saved_errno), NULL, "pouch");
  }
  if (mode == LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE) {
    entry->shared_holders = 0UL;
    entry->exclusive_holders = 1UL;
  } else {
    entry->exclusive_holders = 0UL;
    entry->shared_holders = 1UL;
  }
  pouch->writer_root_lock_mode = mode;
  pthread_mutex_unlock(&lc_pouch_writer_root_lock_mutex);
  return LC_OK;
}

static int lc_pouch_writer_root_lock_check_presence(lc_pouch *pouch,
                                                    lc_error *error) {
  lc_pouch_exclusive_writer_presence presence;
  int rc;

  memset(&presence, 0, sizeof(presence));
  rc = lc_pouch_probe_exclusive_writer(pouch, &presence, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (presence.present) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch root has an unexpired exclusive writer", NULL,
                        NULL, "pouch");
  }
  return LC_OK;
}

int lc_pouch_single_writer_snapshot(lc_pouch *pouch, uint64_t *epoch_out) {
  int enabled;

  if (pouch == NULL) {
    if (epoch_out != NULL) {
      *epoch_out = 0U;
    }
    return 0;
  }
  if (!pouch->single_writer_mutex_initialized) {
    if (epoch_out != NULL) {
      *epoch_out = pouch->single_writer_epoch;
    }
    return pouch->single_writer;
  }
  pthread_mutex_lock(&pouch->single_writer_mutex);
  enabled = pouch->single_writer;
  if (epoch_out != NULL) {
    *epoch_out = pouch->single_writer_epoch;
  }
  pthread_mutex_unlock(&pouch->single_writer_mutex);
  return enabled;
}

int lc_pouch_single_writer_enabled(lc_pouch *pouch) {
  return lc_pouch_single_writer_snapshot(pouch, NULL);
}

int lc_pouch_writer_mode_operation_begin(lc_pouch *pouch, lc_error *error) {
  int pthread_rc;

  if (pouch == NULL || !pouch->writer_mode_guard_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch writer mode operation requires an open pouch",
                        NULL, NULL, "pouch");
  }
  pthread_rc = pthread_rwlock_rdlock(&pouch->writer_mode_guard);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch writer mode operation",
                        strerror(pthread_rc), NULL, "pouch");
  }
  return LC_OK;
}

void lc_pouch_writer_mode_operation_end(lc_pouch *pouch) {
  if (pouch != NULL && pouch->writer_mode_guard_initialized) {
    (void)pthread_rwlock_unlock(&pouch->writer_mode_guard);
  }
}

static int lc_pouch_writer_presence_now_ns(int64_t *out, lc_error *error) {
  struct timespec now;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch writer presence requires output storage", NULL,
                        NULL, "pouch");
  }
  if (clock_gettime(CLOCK_REALTIME, &now) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch writer presence clock",
                        strerror(errno), NULL, "pouch");
  }
  if (now.tv_sec < 0 ||
      (uintmax_t)now.tv_sec >
          ((uintmax_t)LC_I64_MAX - (uintmax_t)now.tv_nsec) / 1000000000U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch writer presence clock is out of range", NULL,
                        NULL, "pouch");
  }
  *out = (int64_t)now.tv_sec * (int64_t)1000000000 + (int64_t)now.tv_nsec;
  return LC_OK;
}

static int lc_pouch_writer_presence_touch(lc_pouch *pouch, lc_error *error) {
  char payload[64];
  int64_t now_ns = 0;
  int rc;

  if (pouch == NULL || pouch->writer_presence_dir == NULL ||
      pouch->writer_presence_path == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch writer presence is not initialized", NULL, NULL,
                        "pouch");
  }
  rc = lc_pouch_writer_presence_now_ns(&now_ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (lc_i64_format_base10((lc_i64)now_ns, payload, sizeof(payload)) < 0 ||
      strlen(payload) + 2U > sizeof(payload) ||
      snprintf(payload + strlen(payload), sizeof(payload) - strlen(payload),
               "\n") < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to format pouch writer heartbeat", NULL, NULL,
                        "pouch");
  }
  rc = lc_pouch_path_ensure_directory(
      pouch->writer_presence_dir,
      "failed to create pouch exclusive-writer directory", error);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_pouch_path_write_text_file_relaxed(pouch->writer_presence_path,
                                               payload, error);
}

static void *lc_pouch_writer_presence_main(void *context) {
  lc_pouch *pouch;

  pouch = (lc_pouch *)context;
  pthread_mutex_lock(&pouch->writer_presence_mutex);
  while (!pouch->writer_presence_stop) {
    struct timespec deadline;
    int wait_rc;

    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_nsec += LC_POUCH_EXCLUSIVE_WRITER_TOUCH_NS;
    if (deadline.tv_nsec >= 1000000000L) {
      deadline.tv_sec += deadline.tv_nsec / 1000000000L;
      deadline.tv_nsec %= 1000000000L;
    }
    wait_rc = pthread_cond_timedwait(&pouch->writer_presence_cond,
                                     &pouch->writer_presence_mutex, &deadline);
    if (pouch->writer_presence_stop) {
      break;
    }
    if (wait_rc == 0 || wait_rc == ETIMEDOUT) {
      lc_error error;

      pthread_mutex_unlock(&pouch->writer_presence_mutex);
      lc_error_init(&error);
      (void)lc_pouch_writer_presence_touch(pouch, &error);
      lc_error_cleanup(&error);
      pthread_mutex_lock(&pouch->writer_presence_mutex);
    }
  }
  pthread_mutex_unlock(&pouch->writer_presence_mutex);
  return NULL;
}

static int lc_pouch_writer_presence_start(lc_pouch *pouch, lc_error *error) {
  int pthread_rc;
  int rc;

  if (pouch == NULL || !pouch->writer_presence_mutex_initialized ||
      !pouch->writer_presence_cond_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch writer presence is not initialized", NULL, NULL,
                        "pouch");
  }
  pthread_mutex_lock(&pouch->writer_presence_mutex);
  if (pouch->writer_presence_thread_started) {
    pthread_mutex_unlock(&pouch->writer_presence_mutex);
    return LC_OK;
  }
  pthread_mutex_unlock(&pouch->writer_presence_mutex);
  rc = lc_pouch_writer_presence_touch(pouch, error);
  if (rc != LC_OK) {
    return rc;
  }
  pthread_mutex_lock(&pouch->writer_presence_mutex);
  pouch->writer_presence_stop = 0;
  pthread_rc = pthread_create(&pouch->writer_presence_thread, NULL,
                              lc_pouch_writer_presence_main, pouch);
  if (pthread_rc == 0) {
    pouch->writer_presence_thread_started = 1;
  }
  pthread_mutex_unlock(&pouch->writer_presence_mutex);
  if (pthread_rc != 0) {
    (void)unlink(pouch->writer_presence_path);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to start pouch writer presence worker",
                        strerror(pthread_rc), NULL, "pouch");
  }
  return LC_OK;
}

static void lc_pouch_writer_presence_stop_internal(lc_pouch *pouch,
                                                   int remove_marker) {
  int join_thread;

  if (pouch == NULL || !pouch->writer_presence_mutex_initialized) {
    return;
  }
  pthread_mutex_lock(&pouch->writer_presence_mutex);
  join_thread = pouch->writer_presence_thread_started;
  if (join_thread) {
    pouch->writer_presence_stop = 1;
    pthread_cond_signal(&pouch->writer_presence_cond);
  }
  pthread_mutex_unlock(&pouch->writer_presence_mutex);
  if (join_thread) {
    (void)pthread_join(pouch->writer_presence_thread, NULL);
    pthread_mutex_lock(&pouch->writer_presence_mutex);
    pouch->writer_presence_thread_started = 0;
    pouch->writer_presence_stop = 0;
    pthread_mutex_unlock(&pouch->writer_presence_mutex);
  }
  if (remove_marker && pouch->writer_presence_path != NULL) {
    (void)unlink(pouch->writer_presence_path);
  }
}

static void lc_pouch_writer_presence_stop(lc_pouch *pouch) {
  lc_pouch_writer_presence_stop_internal(pouch, 1);
}

static void lc_pouch_writer_presence_stop_abrupt(lc_pouch *pouch) {
  lc_pouch_writer_presence_stop_internal(pouch, 0);
}

int lc_pouch_open(const char *root_path, const lc_allocator *allocator,
                  const lc_pouch_open_options *options, lc_pouch **out,
                  lc_error *error) {
  lc_pouch *pouch;
  lc_pouch_crypto_open_options crypto_options;
  int requested_single_writer;
  int pthread_rc;
  int rc;
  size_t key_mutex_index;
  struct stat root_stat;

  if (root_path == NULL || root_path[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_open requires root_path and out", NULL, NULL,
                        NULL);
  }
  *out = NULL;
  pouch = (lc_pouch *)lc_calloc_with_allocator(allocator, 1U, sizeof(*pouch));
  if (pouch == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch handle", NULL, NULL, NULL);
  }
  if (allocator != NULL) {
    pouch->allocator = *allocator;
  } else {
    lc_allocator_init(&pouch->allocator);
  }
  pthread_rc = pthread_mutex_init(&pouch->single_writer_mutex, NULL);
  if (pthread_rc != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch single-writer mutex",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->single_writer_mutex_initialized = 1;
  pthread_rc = pthread_rwlock_init(&pouch->writer_mode_guard, NULL);
  if (pthread_rc != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch writer mode guard",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->writer_mode_guard_initialized = 1;
  pthread_rc = pthread_mutex_init(&pouch->writer_presence_mutex, NULL);
  if (pthread_rc != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch writer presence mutex",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->writer_presence_mutex_initialized = 1;
  pthread_rc = pthread_mutex_init(&pouch->state_mutation_mutex, NULL);
  if (pthread_rc != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch state mutation mutex",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->state_mutation_mutex_initialized = 1;
  pthread_rc = pthread_mutex_init(&pouch->state_cache_mutex, NULL);
  if (pthread_rc != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch state cache mutex",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->state_cache_mutex_initialized = 1;
  pthread_rc = pthread_mutex_init(&pouch->query_flush_mutex, NULL);
  if (pthread_rc != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch query flush mutex",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->query_flush_mutex_initialized = 1;
  pthread_rc = pthread_mutex_init(&pouch->source_cache_mutex, NULL);
  if (pthread_rc != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch source cache mutex",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->source_cache_mutex_initialized = 1;
  for (key_mutex_index = 0U;
       key_mutex_index < LC_POUCH_EXCLUSIVE_KEY_STRIPE_COUNT;
       ++key_mutex_index) {
    pthread_rc = lc_pouch_mutex_init_recursive(
        &pouch->exclusive_key_mutexes[key_mutex_index]);
    if (pthread_rc != 0) {
      lc_pouch_close(pouch);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to initialize pouch exclusive key mutex",
                          strerror(pthread_rc), NULL, "pouch");
    }
    pouch->exclusive_key_mutex_count += 1U;
  }
  pthread_rc = pthread_cond_init(&pouch->writer_presence_cond, NULL);
  if (pthread_rc != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch writer presence condition",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->writer_presence_cond_initialized = 1;
  pouch->root_path = lc_strdup_with_allocator(&pouch->allocator, root_path);
  if (pouch->root_path == NULL) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch root path", NULL, NULL, NULL);
  }
  lc_pouch_init_options(pouch, options);
  requested_single_writer = pouch->single_writer;
  pouch->single_writer = 0;
  pouch->single_writer_epoch = 1U;
  pouch->base_logger = options != NULL && options->logger != NULL
                           ? options->logger
                           : lc_log_noop_logger();
  pouch->logger = lc_log_pouch_logger(pouch->base_logger);
  if (pouch->logger == NULL) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize pouch logger", NULL, NULL,
                        "pouch");
  }
  pouch->owns_logger = (pouch->logger != pouch->base_logger &&
                        pouch->logger != lc_log_noop_logger())
                           ? 1
                           : 0;
  if (pouch->query_engine == NULL || pouch->query_fallback_engine == NULL ||
      pouch->compression == NULL) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch open options", NULL, NULL,
                        NULL);
  }
  if (strcmp(pouch->query_engine, "index") != 0 &&
      strcmp(pouch->query_engine, "scan") != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query_engine must be index or scan", NULL, NULL,
                        "pouch");
  }
  if (pouch->query_fallback_engine[0] != '\0' &&
      strcmp(pouch->query_fallback_engine, "index") != 0 &&
      strcmp(pouch->query_fallback_engine, "scan") != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch query_fallback_engine must be index or scan",
                        NULL, NULL, "pouch");
  }
  if (strcmp(pouch->compression, "none") != 0 &&
      strcmp(pouch->compression, "zlib") != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch compression must be none or zlib", NULL, NULL,
                        "pouch");
  }
  memset(&crypto_options, 0, sizeof(crypto_options));
  if (options != NULL) {
    crypto_options.key_string = options->crypto_key;
    crypto_options.key_file = options->crypto_key_file;
    crypto_options.generate_key_file = options->crypto_generate_key_file;
  }
  crypto_options.compression_enabled =
      strcmp(pouch->compression, "zlib") == 0 ? 1 : 0;
  rc = lc_pouch_crypto_open(&pouch->allocator, &crypto_options, &pouch->crypto,
                            &pouch->crypto_key_file, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  rc = lc_pouch_init_writer_marker(pouch, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  rc = lc_pouch_path_ensure_directory(
      pouch->root_path, "failed to create pouch root directory", error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  rc = lc_pouch_set_single_writer(pouch, requested_single_writer, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  rc = lc_pouch_ensure_root(pouch, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  if (stat(pouch->root_path, &root_stat) != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to identify pouch root", strerror(errno), NULL,
                        "pouch");
  }
  pouch->root_device = (uint64_t)root_stat.st_dev;
  pouch->root_inode = (uint64_t)root_stat.st_ino;
  pouch->root_identity_initialized = 1;
  lc_pouch_configure_filesystem_capabilities(pouch);
  if (pouch->durable_sync) {
    rc = lc_pouch_fsync_batcher_init(pouch, error);
    if (rc != LC_OK) {
      lc_pouch_close(pouch);
      return rc;
    }
  }
  rc = lc_pouch_state_metadata_append_worker_init(pouch, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  rc = lc_pouch_warm_open_namespaces(pouch, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  rc = lc_pouch_compaction_worker_init(pouch, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  rc = lc_pouch_indexer_worker_init(pouch, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  rc = lc_pouch_janitor_worker_init(pouch, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  *out = pouch;
  {
    pslog_field fields[11];

    fields[0] = lc_log_str_field("path", pouch->root_path);
    fields[1] = lc_log_str_field("query_engine", pouch->query_engine);
    fields[2] =
        lc_log_str_field("query_fallback_engine", pouch->query_fallback_engine);
    fields[3] = lc_log_str_field("compression", pouch->compression);
    fields[4] =
        lc_log_bool_field("crypto", lc_pouch_crypto_enabled(pouch->crypto));
    fields[5] =
        lc_log_u64_field("segment_target_bytes", pouch->segment_target_bytes);
    fields[6] = lc_log_bool_field("single_writer",
                                  lc_pouch_single_writer_enabled(pouch));
    fields[7] = lc_log_bool_field("background_compaction",
                                  pouch->background_compaction_enabled);
    fields[8] = lc_log_bool_field("durable_sync", pouch->durable_sync);
    fields[9] =
        lc_log_u64_field("indexer_flush_docs", pouch->indexer_flush_docs);
    fields[10] = lc_log_u64_field("indexer_flush_interval_seconds",
                                  pouch->indexer_flush_interval_seconds);
    lc_log_info(pouch->logger, "open", fields, 11U);
  }
  return LC_OK;
}

void lc_pouch_close(lc_pouch *pouch) {
  lc_allocator allocator;

  if (pouch == NULL) {
    return;
  }
  allocator = pouch->allocator;
  if (pouch->logger != NULL) {
    pslog_field fields[1];

    fields[0] = lc_log_str_field("path", pouch->root_path);
    lc_log_debug(pouch->logger, "close", fields, 1U);
  }
  if (pouch->aborted) {
    lc_pouch_writer_presence_stop_abrupt(pouch);
  } else {
    lc_pouch_writer_presence_stop(pouch);
  }
  lc_pouch_janitor_worker_close(pouch);
  lc_pouch_indexer_worker_close(pouch);
  lc_pouch_compaction_worker_close(pouch);
  lc_pouch_state_metadata_append_worker_close(pouch);
  lc_pouch_fsync_batcher_close(pouch);
  if (!pouch->aborted) {
    lc_pouch_state_checkpoint_clean_close(pouch);
  }
  lc_pouch_state_cache_cleanup(pouch);
  lc_pouch_state_source_cache_cleanup(pouch);
  lc_pouch_query_index_cache_cleanup(pouch);
  lc_pouch_writer_root_lock_release(pouch);
  lc_pouch_crypto_close(pouch->crypto);
  lc_free_with_allocator(&allocator, pouch->crypto_key_file);
  lc_free_with_allocator(&allocator, pouch->writer_presence_path);
  lc_free_with_allocator(&allocator, pouch->writer_presence_leaf);
  lc_free_with_allocator(&allocator, pouch->writer_presence_dir);
  lc_free_with_allocator(&allocator, pouch->writer_marker_leaf);
  lc_free_with_allocator(&allocator, pouch->compression);
  lc_free_with_allocator(&allocator, pouch->query_fallback_engine);
  lc_free_with_allocator(&allocator, pouch->query_engine);
  lc_free_with_allocator(&allocator, pouch->root_path);
  if (pouch->owns_logger && pouch->logger != NULL &&
      pouch->logger != lc_log_noop_logger()) {
    pouch->logger->destroy(pouch->logger);
  }
  if (pouch->writer_presence_cond_initialized) {
    pthread_cond_destroy(&pouch->writer_presence_cond);
  }
  if (pouch->writer_presence_mutex_initialized) {
    pthread_mutex_destroy(&pouch->writer_presence_mutex);
  }
  if (pouch->state_mutation_mutex_initialized) {
    pthread_mutex_destroy(&pouch->state_mutation_mutex);
  }
  if (pouch->state_cache_mutex_initialized) {
    pthread_mutex_destroy(&pouch->state_cache_mutex);
  }
  if (pouch->query_flush_mutex_initialized) {
    pthread_mutex_destroy(&pouch->query_flush_mutex);
  }
  if (pouch->source_cache_mutex_initialized) {
    pthread_mutex_destroy(&pouch->source_cache_mutex);
  }
  while (pouch->exclusive_key_mutex_count > 0U) {
    pouch->exclusive_key_mutex_count -= 1U;
    (void)pthread_mutex_destroy(
        &pouch->exclusive_key_mutexes[pouch->exclusive_key_mutex_count]);
  }
  if (pouch->writer_mode_guard_initialized) {
    (void)pthread_rwlock_destroy(&pouch->writer_mode_guard);
  }
  if (pouch->single_writer_mutex_initialized) {
    pthread_mutex_destroy(&pouch->single_writer_mutex);
  }
  lc_free_with_allocator(&allocator, pouch);
}

int lc_pouch_abort(lc_pouch *pouch, lc_error *error) {
  if (pouch == NULL || !pouch->single_writer_mutex_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch abort requires an open pouch", NULL, NULL,
                        "pouch");
  }
  pthread_mutex_lock(&pouch->single_writer_mutex);
  if (pouch->aborted) {
    pthread_mutex_unlock(&pouch->single_writer_mutex);
    return LC_OK;
  }
  pouch->aborted = 1;
  pthread_mutex_unlock(&pouch->single_writer_mutex);
  lc_pouch_writer_presence_stop_abrupt(pouch);
  lc_pouch_janitor_worker_close(pouch);
  lc_pouch_indexer_worker_close(pouch);
  lc_pouch_compaction_worker_close(pouch);
  lc_pouch_state_metadata_append_worker_close(pouch);
  lc_pouch_fsync_batcher_close(pouch);
  /* Abort mirrors Go disk logstore close before ownership is released: no
   * resident append/read descriptor or derived cache may outlive this writer.
   */
  lc_pouch_state_cache_cleanup(pouch);
  lc_pouch_state_source_cache_cleanup(pouch);
  lc_pouch_query_index_cache_cleanup(pouch);
  lc_pouch_writer_root_lock_release(pouch);
  {
    pslog_field fields[1];

    fields[0] = lc_log_str_field("path", pouch->root_path);
    lc_log_info(pouch->logger, "abort", fields, 1U);
  }
  return LC_OK;
}

int lc_pouch_supports_concurrent_writes(const lc_pouch *pouch) {
  return pouch != NULL &&
                 !lc_pouch_single_writer_snapshot((lc_pouch *)pouch, NULL)
             ? 1
             : 0;
}

int lc_pouch_fsync_stats_read(lc_pouch *pouch, lc_pouch_fsync_stats *out,
                              lc_error *error) {
  lc_pouch_fsync_batcher *batcher;
  size_t index;

  if (pouch == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch fsync stats requires pouch and output", NULL,
                        NULL, "pouch");
  }
  memset(out, 0, sizeof(*out));
  batcher = pouch->fsync_batcher;
  if (batcher != NULL) {
    pthread_mutex_lock(&batcher->mutex);
    *out = batcher->stats;
    pthread_mutex_unlock(&batcher->mutex);
  }
  for (index = 0U; index < LC_POUCH_FSYNC_BATCH_BOUND_COUNT; ++index) {
    out->bounds[index] = lc_pouch_fsync_batch_bounds[index];
  }
  return LC_OK;
}

static int
lc_pouch_backend_hash_derived(lc_pouch *pouch,
                              char out[LC_POUCH_BACKEND_HASH_HEX_BYTES + 1U],
                              lc_error *error) {
  static const char hex[] = "0123456789abcdef";
  const char *root;
  char current_directory[PATH_MAX];
  char *descriptor;
  EVP_MD_CTX *ctx;
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int digest_len;
  size_t descriptor_len;
  size_t index;
  size_t root_len;
  size_t current_directory_len;
  int rc;

  if (pouch == NULL || out == NULL || pouch->root_path == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch backend hash requires pouch and output", NULL,
                        NULL, "pouch");
  }
  out[0] = '\0';
  root = pouch->root_path;
  root_len = strlen(root);
  if (root[0] != '/' &&
      getcwd(current_directory, sizeof(current_directory)) != NULL) {
    current_directory_len = strlen(current_directory);
    if (current_directory_len > SIZE_MAX - sizeof("pouch|/") ||
        root_len > SIZE_MAX - current_directory_len - sizeof("pouch|/")) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch backend hash descriptor exceeds limit", NULL,
                          NULL, "pouch");
    }
    descriptor_len = current_directory_len + root_len + sizeof("pouch|/");
    descriptor =
        (char *)lc_alloc_with_allocator(&pouch->allocator, descriptor_len);
    if (descriptor == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch backend hash descriptor",
                          NULL, NULL, "pouch");
    }
    snprintf(descriptor, descriptor_len, "pouch|%s/%s", current_directory,
             root);
  } else {
    if (root_len > SIZE_MAX - sizeof("pouch|")) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch backend hash descriptor exceeds limit", NULL,
                          NULL, "pouch");
    }
    descriptor_len = root_len + sizeof("pouch|");
    descriptor =
        (char *)lc_alloc_with_allocator(&pouch->allocator, descriptor_len);
    if (descriptor == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch backend hash descriptor",
                          NULL, NULL, "pouch");
    }
    snprintf(descriptor, descriptor_len, "pouch|%s", root);
  }
  ctx = EVP_MD_CTX_new();
  digest_len = 0U;
  rc = LC_OK;
  if (ctx == NULL || EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) != 1 ||
      EVP_DigestUpdate(ctx, descriptor, strlen(descriptor)) != 1 ||
      EVP_DigestFinal_ex(ctx, digest, &digest_len) != 1 || digest_len != 32U) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to calculate pouch backend hash", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK) {
    for (index = 0U; index < digest_len; ++index) {
      out[index * 2U] = hex[(digest[index] >> 4U) & 0x0fU];
      out[index * 2U + 1U] = hex[digest[index] & 0x0fU];
    }
    out[digest_len * 2U] = '\0';
  }
  OPENSSL_cleanse(digest, sizeof(digest));
  EVP_MD_CTX_free(ctx);
  lc_free_with_allocator(&pouch->allocator, descriptor);
  return rc;
}

static int lc_pouch_backend_hash_marker_read(
    lc_pouch *pouch, char out[LC_POUCH_BACKEND_HASH_HEX_BYTES + 1U], int *found,
    lc_error *error) {
  lc_pouch_state_read_result read_result;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  size_t index;
  int rc;

  if (pouch == NULL || out == NULL || found == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch backend hash marker requires inputs", NULL, NULL,
                        "pouch");
  }
  out[0] = '\0';
  *found = 0;
  memset(&read_result, 0, sizeof(read_result));
  sink = NULL;
  rc = lc_pouch_state_read(pouch, ".lockd", "backend-id", &read_result, error);
  if (rc == LC_OK && read_result.found) {
    rc = lc_sink_to_memory(&sink, error);
  }
  if (rc == LC_OK && read_result.found) {
    rc = lc_copy(read_result.body, sink, NULL, error);
  }
  if (rc == LC_OK && read_result.found) {
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  }
  if (rc == LC_OK && read_result.found) {
    if (length != LC_POUCH_BACKEND_HASH_HEX_BYTES) {
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch backend hash marker has invalid length", NULL,
                        NULL, "pouch");
    }
    for (index = 0U; rc == LC_OK && index < length; ++index) {
      unsigned char value;

      value = ((const unsigned char *)bytes)[index];
      if (!((value >= '0' && value <= '9') || (value >= 'a' && value <= 'f'))) {
        rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch backend hash marker is not lowercase hex",
                          NULL, NULL, "pouch");
      }
    }
    if (rc == LC_OK) {
      memcpy(out, bytes, length);
      out[length] = '\0';
      *found = 1;
    }
  }
  if (sink != NULL) {
    lc_sink_close(sink);
  }
  lc_pouch_state_read_result_cleanup(&pouch->allocator, &read_result);
  return rc;
}

static int lc_pouch_backend_hash_marker_write(
    lc_pouch *pouch, const char value[LC_POUCH_BACKEND_HASH_HEX_BYTES + 1U],
    lc_error *error) {
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  lc_source *source;
  int rc;

  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  source = NULL;
  rc = lc_source_from_memory(value, LC_POUCH_BACKEND_HASH_HEX_BYTES, &source,
                             error);
  if (rc == LC_OK) {
    options.content_type = "text/plain";
    options.create_if_absent = 1;
    options.object_record = 1;
    rc = lc_pouch_state_write(pouch, ".lockd", "backend-id", source, &options,
                              &result, error);
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  lc_pouch_state_write_result_cleanup(&pouch->allocator, &result);
  return rc;
}

static int lc_pouch_backend_hash_create_collision(const lc_error *error) {
  return error != NULL && error->code == LC_ERR_INVALID &&
         error->message != NULL &&
         strstr(error->message, "create-if-absent precondition failed") != NULL;
}

int lc_pouch_backend_hash(lc_pouch *pouch,
                          char out[LC_POUCH_BACKEND_HASH_HEX_BYTES + 1U],
                          lc_error *error) {
  char derived[LC_POUCH_BACKEND_HASH_HEX_BYTES + 1U];
  int found;
  int rc;

  if (pouch == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch backend hash requires pouch and output", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_backend_hash_derived(pouch, derived, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_backend_hash_marker_read(pouch, out, &found, error);
  if (rc != LC_OK || found) {
    return rc;
  }
  rc = lc_pouch_backend_hash_marker_write(pouch, derived, error);
  if (rc == LC_OK) {
    memcpy(out, derived, sizeof(derived));
    return LC_OK;
  }
  if (!lc_pouch_backend_hash_create_collision(error)) {
    return rc;
  }
  lc_error_cleanup(error);
  lc_error_init(error);
  rc = lc_pouch_backend_hash_marker_read(pouch, out, &found, error);
  if (rc != LC_OK || found) {
    return rc;
  }
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch backend hash marker disappeared after collision",
                      NULL, NULL, "pouch");
}

int lc_pouch_set_single_writer(lc_pouch *pouch, int enabled, lc_error *error) {
  lc_error restart_error;
  int had_root_lock;
  int mode_changed;
  int normalized;
  int rc;

  if (pouch == NULL || !pouch->single_writer_mutex_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch single-writer control requires an open pouch",
                        NULL, NULL, "pouch");
  }
  normalized = enabled != 0 ? 1 : 0;
  if (!pouch->writer_mode_guard_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch single-writer control requires a mode guard",
                        NULL, NULL, "pouch");
  }
  rc = pthread_rwlock_wrlock(&pouch->writer_mode_guard);
  if (rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch writer mode transition",
                        strerror(rc), NULL, "pouch");
  }
  mode_changed = 0;
  pthread_mutex_lock(&pouch->single_writer_mutex);
  if (pouch->aborted) {
    pthread_mutex_unlock(&pouch->single_writer_mutex);
    (void)pthread_rwlock_unlock(&pouch->writer_mode_guard);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch single-writer control is closed after abort",
                        NULL, NULL, "pouch");
  }
  had_root_lock = pouch->writer_root_lock != NULL;
  if (pouch->single_writer == normalized && had_root_lock) {
    pthread_mutex_unlock(&pouch->single_writer_mutex);
    (void)pthread_rwlock_unlock(&pouch->writer_mode_guard);
    return LC_OK;
  }
  if (!had_root_lock) {
    rc = lc_pouch_writer_root_lock_acquire(
        pouch,
        normalized ? LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE
                   : LC_POUCH_WRITER_ROOT_LOCK_SHARED,
        error);
    if (rc != LC_OK) {
      pthread_mutex_unlock(&pouch->single_writer_mutex);
      (void)pthread_rwlock_unlock(&pouch->writer_mode_guard);
      return rc;
    }
    rc = lc_pouch_writer_root_lock_check_presence(pouch, error);
    if (rc != LC_OK) {
      lc_pouch_writer_root_lock_release(pouch);
      pthread_mutex_unlock(&pouch->single_writer_mutex);
      (void)pthread_rwlock_unlock(&pouch->writer_mode_guard);
      return rc;
    }
    if (normalized) {
      rc = lc_pouch_writer_presence_start(pouch, error);
      if (rc != LC_OK) {
        lc_pouch_writer_root_lock_release(pouch);
        pthread_mutex_unlock(&pouch->single_writer_mutex);
        (void)pthread_rwlock_unlock(&pouch->writer_mode_guard);
        return rc;
      }
    }
    pouch->single_writer = normalized;
    mode_changed = 1;
  } else if (normalized) {
    rc = lc_pouch_writer_root_lock_change(
        pouch, LC_POUCH_WRITER_ROOT_LOCK_EXCLUSIVE, error);
    if (rc != LC_OK) {
      pthread_mutex_unlock(&pouch->single_writer_mutex);
      (void)pthread_rwlock_unlock(&pouch->writer_mode_guard);
      return rc;
    }
    rc = lc_pouch_writer_root_lock_check_presence(pouch, error);
    if (rc != LC_OK) {
      lc_error downgrade_error;

      lc_error_init(&downgrade_error);
      (void)lc_pouch_writer_root_lock_change(
          pouch, LC_POUCH_WRITER_ROOT_LOCK_SHARED, &downgrade_error);
      lc_error_cleanup(&downgrade_error);
      pthread_mutex_unlock(&pouch->single_writer_mutex);
      (void)pthread_rwlock_unlock(&pouch->writer_mode_guard);
      return rc;
    }
    rc = lc_pouch_writer_presence_start(pouch, error);
    if (rc != LC_OK) {
      lc_error downgrade_error;

      lc_error_init(&downgrade_error);
      (void)lc_pouch_writer_root_lock_change(
          pouch, LC_POUCH_WRITER_ROOT_LOCK_SHARED, &downgrade_error);
      lc_error_cleanup(&downgrade_error);
      pthread_mutex_unlock(&pouch->single_writer_mutex);
      (void)pthread_rwlock_unlock(&pouch->writer_mode_guard);
      return rc;
    }
    pouch->single_writer = 1;
    mode_changed = 1;
  } else {
    lc_pouch_writer_presence_stop(pouch);
    rc = lc_pouch_writer_root_lock_change(
        pouch, LC_POUCH_WRITER_ROOT_LOCK_SHARED, error);
    if (rc != LC_OK) {
      lc_error_init(&restart_error);
      (void)lc_pouch_writer_presence_start(pouch, &restart_error);
      lc_error_cleanup(&restart_error);
      pthread_mutex_unlock(&pouch->single_writer_mutex);
      (void)pthread_rwlock_unlock(&pouch->writer_mode_guard);
      return rc;
    }
    pouch->single_writer = 0;
    mode_changed = 1;
  }
  if (mode_changed && pouch->single_writer_epoch != LC_U64_MAX) {
    ++pouch->single_writer_epoch;
  }
  pthread_mutex_unlock(&pouch->single_writer_mutex);
  (void)pthread_rwlock_unlock(&pouch->writer_mode_guard);
  {
    pslog_field fields[1];

    fields[0] = lc_log_bool_field("single_writer", normalized);
    lc_log_info(pouch->logger, "single_writer.set", fields, 1U);
  }
  return LC_OK;
}

static int lc_pouch_writer_presence_read(const char *path,
                                         int64_t *heartbeat_ns,
                                         int *has_heartbeat, lc_error *error) {
  char buffer[128];
  size_t length;
  int fd;

  if (path == NULL || heartbeat_ns == NULL || has_heartbeat == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch writer heartbeat requires path and outputs",
                        NULL, NULL, "pouch");
  }
  *heartbeat_ns = 0;
  *has_heartbeat = 0;
  fd = open(path, O_RDONLY);
  if (fd < 0) {
    if (errno == ENOENT) {
      return LC_POUCH_WRITER_PRESENCE_MISSING;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch writer heartbeat",
                        strerror(errno), NULL, "pouch");
  }
  length = 0U;
  for (;;) {
    ssize_t got;

    if (length + 1U >= sizeof(buffer)) {
      break;
    }
    got = read(fd, buffer + length, sizeof(buffer) - length - 1U);
    if (got < 0) {
      if (errno == EINTR) {
        continue;
      }
      close(fd);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to read pouch writer heartbeat",
                          strerror(errno), NULL, "pouch");
    }
    if (got == 0) {
      break;
    }
    length += (size_t)got;
  }
  if (close(fd) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch writer heartbeat",
                        strerror(errno), NULL, "pouch");
  }
  buffer[length] = '\0';
  if (length > 0U) {
    char *end;
    lc_i64 value;

    while (*buffer == ' ' || *buffer == '\t' || *buffer == '\r' ||
           *buffer == '\n') {
      memmove(buffer, buffer + 1U, strlen(buffer));
    }
    end = buffer + strlen(buffer);
    while (end > buffer && (end[-1] == ' ' || end[-1] == '\t' ||
                            end[-1] == '\r' || end[-1] == '\n')) {
      *--end = '\0';
    }
    if (lc_i64_parse_base10(buffer, &value) && value > 0) {
      *heartbeat_ns = (int64_t)value;
      *has_heartbeat = 1;
    }
  }
  return LC_OK;
}

static int lc_pouch_writer_presence_mtime_ns(const struct stat *st,
                                             int64_t *out) {
  int64_t seconds;
  long nanos;

  if (st == NULL || out == NULL) {
    return 0;
  }
#if defined(__APPLE__)
  seconds = (int64_t)st->st_mtimespec.tv_sec;
  nanos = st->st_mtimespec.tv_nsec;
#else
  seconds = (int64_t)st->st_mtim.tv_sec;
  nanos = st->st_mtim.tv_nsec;
#endif
  if (seconds < 0 || nanos < 0 ||
      (uintmax_t)seconds >
          ((uintmax_t)LC_I64_MAX - (uintmax_t)nanos) / 1000000000U) {
    return 0;
  }
  *out = seconds * (int64_t)1000000000 + (int64_t)nanos;
  return 1;
}

int lc_pouch_probe_exclusive_writer(lc_pouch *pouch,
                                    lc_pouch_exclusive_writer_presence *out,
                                    lc_error *error) {
  DIR *dir;
  struct dirent *entry;
  int64_t now_ns = 0;
  int rc;

  if (pouch == NULL || out == NULL || pouch->writer_presence_dir == NULL ||
      pouch->writer_presence_leaf == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch writer probe requires an open pouch and output",
                        NULL, NULL, "pouch");
  }
  memset(out, 0, sizeof(*out));
  rc = lc_pouch_writer_presence_now_ns(&now_ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  dir = opendir(pouch->writer_presence_dir);
  if (dir == NULL) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch exclusive-writer directory",
                        strerror(errno), NULL, "pouch");
  }
  rc = LC_OK;
  while (rc == LC_OK && (entry = readdir(dir)) != NULL) {
    char *path;
    struct stat st;
    int64_t heartbeat_ns;
    int64_t expires_ns;
    int has_heartbeat;

    if (entry->d_name[0] == '\0' ||
        strcmp(entry->d_name, pouch->writer_presence_leaf) == 0) {
      continue;
    }
    path = lc_pouch_path_join(&pouch->allocator, pouch->writer_presence_dir,
                              entry->d_name);
    if (path == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch writer presence path", NULL,
                        NULL, "pouch");
      break;
    }
    if (stat(path, &st) != 0) {
      int saved_errno;

      saved_errno = errno;
      lc_free_with_allocator(&pouch->allocator, path);
      if (saved_errno == ENOENT) {
        continue;
      }
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to stat pouch writer heartbeat",
                        strerror(saved_errno), NULL, "pouch");
      break;
    }
    if (!S_ISREG(st.st_mode)) {
      lc_free_with_allocator(&pouch->allocator, path);
      continue;
    }
    rc = lc_pouch_writer_presence_read(path, &heartbeat_ns, &has_heartbeat,
                                       error);
    lc_free_with_allocator(&pouch->allocator, path);
    if (rc == LC_POUCH_WRITER_PRESENCE_MISSING) {
      rc = LC_OK;
      continue;
    }
    if (rc != LC_OK) {
      break;
    }
    if (!has_heartbeat &&
        !lc_pouch_writer_presence_mtime_ns(&st, &heartbeat_ns)) {
      continue;
    }
    expires_ns = heartbeat_ns > LC_I64_MAX - LC_POUCH_EXCLUSIVE_WRITER_TTL_NS
                     ? LC_I64_MAX
                     : heartbeat_ns + LC_POUCH_EXCLUSIVE_WRITER_TTL_NS;
    if (expires_ns > now_ns) {
      out->present = 1;
      out->expires_at_unix = expires_ns / (int64_t)1000000000;
      break;
    }
  }
  if (closedir(dir) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch exclusive-writer directory",
                      strerror(errno), NULL, "pouch");
  }
  return rc;
}

int lc_pouch_status_read(lc_pouch *pouch, lc_pouch_status *out,
                         lc_error *error) {
  if (pouch == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_status_read requires pouch and out", NULL,
                        NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  out->root_path =
      lc_strdup_with_allocator(&pouch->allocator, pouch->root_path);
  out->layout_name =
      lc_strdup_with_allocator(&pouch->allocator, LC_POUCH_LAYOUT_NAME);
  if (out->root_path == NULL || out->layout_name == NULL) {
    lc_pouch_status_cleanup(&pouch->allocator, out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to copy pouch status",
                        NULL, NULL, NULL);
  }
  out->layout_version = LC_POUCH_LAYOUT_VERSION;
  out->segment_target_bytes = pouch->segment_target_bytes;
  out->indexer_flush_docs = pouch->indexer_flush_docs;
  out->indexer_flush_interval_seconds = pouch->indexer_flush_interval_seconds;
  out->compaction_min_segment_count = pouch->compaction_min_segment_count;
  out->compaction_min_reclaimable_bytes =
      pouch->compaction_min_reclaimable_bytes;
  out->compaction_interval_seconds = pouch->compaction_interval_seconds;
  out->compaction_delete_grace_seconds = pouch->compaction_delete_grace_seconds;
  out->compaction_max_io_bytes_per_sec = pouch->compaction_max_io_bytes_per_sec;
  out->background_compaction_enabled = pouch->background_compaction_enabled;
  out->compaction_throttling_disabled = pouch->compaction_throttling_disabled;
  out->retention_seconds = pouch->retention_seconds;
  out->janitor_interval_seconds = pouch->janitor_interval_seconds;
  out->janitor_running = pouch->janitor_thread_started;
  out->single_writer = lc_pouch_single_writer_enabled(pouch);
  out->supports_concurrent_writes = lc_pouch_supports_concurrent_writes(pouch);
  out->aborted = pouch->aborted;
  out->durable_sync = pouch->durable_sync;
  out->fsync_batch_max_ops = pouch->fsync_batch_max_ops;
  out->queue_watch_enabled = pouch->queue_watch_enabled;
  out->filesystem_capabilities_known = pouch->filesystem_capabilities_known;
  out->filesystem_is_nfs = pouch->filesystem_is_nfs;
  out->queue_watch_mode =
      lc_strdup_with_allocator(&pouch->allocator, pouch->queue_watch_mode);
  out->queue_watch_reason =
      lc_strdup_with_allocator(&pouch->allocator, pouch->queue_watch_reason);
  out->query_engine =
      lc_strdup_with_allocator(&pouch->allocator, pouch->query_engine);
  out->query_fallback_engine =
      lc_strdup_with_allocator(&pouch->allocator, pouch->query_fallback_engine);
  out->compression =
      lc_strdup_with_allocator(&pouch->allocator, pouch->compression);
  out->crypto_enabled = lc_pouch_crypto_enabled(pouch->crypto);
  out->crypto_key_file =
      pouch->crypto_key_file != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, pouch->crypto_key_file)
          : NULL;
  if (out->queue_watch_mode == NULL || out->queue_watch_reason == NULL ||
      out->query_engine == NULL || out->query_fallback_engine == NULL ||
      out->compression == NULL) {
    lc_pouch_status_cleanup(&pouch->allocator, out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch status options", NULL, NULL,
                        NULL);
  }
  if (pouch->crypto_key_file != NULL && out->crypto_key_file == NULL) {
    lc_pouch_status_cleanup(&pouch->allocator, out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch crypto key file status", NULL,
                        NULL, NULL);
  }
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("path", out->root_path);
    fields[1] = lc_log_str_field("layout", out->layout_name);
    fields[2] = lc_log_u64_field("version", out->layout_version);
    fields[3] = lc_log_bool_field("crypto", out->crypto_enabled);
    fields[4] = lc_log_str_field("compression", out->compression);
    lc_log_trace(pouch->logger, "status.read", fields, 5U);
  }
  return LC_OK;
}

void lc_pouch_status_cleanup(const lc_allocator *allocator,
                             lc_pouch_status *status) {
  if (status == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, status->root_path);
  lc_free_with_allocator(allocator, status->layout_name);
  lc_free_with_allocator(allocator, status->queue_watch_mode);
  lc_free_with_allocator(allocator, status->queue_watch_reason);
  lc_free_with_allocator(allocator, status->query_engine);
  lc_free_with_allocator(allocator, status->query_fallback_engine);
  lc_free_with_allocator(allocator, status->compression);
  lc_free_with_allocator(allocator, status->crypto_key_file);
  memset(status, 0, sizeof(*status));
}

int lc_pouch_ensure_namespace(lc_pouch *pouch, const char *namespace_name,
                              lc_error *error) {
  int rc;

  if (pouch == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_ensure_namespace requires pouch", NULL, NULL,
                        NULL);
  }
  rc = lc_pouch_namespace_ensure(&pouch->allocator, pouch->root_path,
                                 namespace_name, error);
  if (rc != LC_OK) {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("ns", namespace_name);
    fields[1] = lc_log_error_field("error", error);
    fields[2] = lc_log_code_field(error);
    lc_log_error(pouch->logger, "manifest.ensure_namespace.error", fields, 3U);
    return rc;
  }
  rc = lc_pouch_state_recover_staged_decisions(pouch, namespace_name, error);
  if (rc == LC_OK) {
    pslog_field fields[1];

    fields[0] = lc_log_str_field("ns", namespace_name);
    lc_log_trace(pouch->logger, "manifest.ensure_namespace", fields, 1U);
  } else {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("ns", namespace_name);
    fields[1] = lc_log_error_field("error", error);
    fields[2] = lc_log_code_field(error);
    lc_log_error(pouch->logger, "manifest.recover_staged.error", fields, 3U);
  }
  return rc;
}

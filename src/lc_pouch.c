#include "lc_pouch.h"

#include "lc_api_internal.h"
#include "lc_log.h"
#include "lc_pouch_format.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"
#include "lc_pouch_query_index.h"

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

static unsigned long lc_pouch_next_writer_marker_id;
static pthread_mutex_t lc_pouch_writer_marker_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t lc_pouch_root_manifest_mutex = PTHREAD_MUTEX_INITIALIZER;

#define LC_POUCH_FSYNC_BATCH_MAX_OPS 4096U
#define LC_POUCH_FSYNC_BATCH_DELAY_NS (2L * 1000L * 1000L)
#define LC_POUCH_EXCLUSIVE_WRITER_TOUCH_NS 1000000000L
#define LC_POUCH_EXCLUSIVE_WRITER_TTL_NS \
  ((int64_t)3 * (int64_t)1000000000)
#define LC_POUCH_WRITER_PRESENCE_MISSING (-1001)

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

static int lc_pouch_sync_fd(int fd) {
#ifdef __linux__
  return fdatasync(fd);
#else
  return fsync(fd);
#endif
}

static void lc_pouch_fsync_deadline(struct timespec *deadline) {
  if (deadline == NULL) {
    return;
  }
  clock_gettime(CLOCK_REALTIME, deadline);
  deadline->tv_nsec += LC_POUCH_FSYNC_BATCH_DELAY_NS;
  if (deadline->tv_nsec >= 1000000000L) {
    deadline->tv_sec += deadline->tv_nsec / 1000000000L;
    deadline->tv_nsec %= 1000000000L;
  }
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

static void lc_pouch_fsync_process_batch(lc_pouch *pouch,
                                         lc_pouch_fsync_request *batch,
                                         size_t count) {
  lc_pouch_fsync_batch_file inline_files[64];
  lc_pouch_fsync_batch_file *files;
  lc_pouch_fsync_request *request;
  struct stat st;
  size_t file_count;
  int errnum;

  files = inline_files;
  if (count > sizeof(inline_files) / sizeof(inline_files[0])) {
    files = (lc_pouch_fsync_batch_file *)lc_calloc_with_allocator(
        &pouch->allocator, count, sizeof(lc_pouch_fsync_batch_file));
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
    lc_free_with_allocator(&pouch->allocator, files);
  }

finish:
  for (request = batch; request != NULL; request = request->next) {
    request->errnum = errnum;
  }
}

static lc_pouch_fsync_request *lc_pouch_fsync_take_batch(lc_pouch *pouch,
                                                         size_t *out_count) {
  lc_pouch_fsync_request *batch;
  lc_pouch_fsync_request *tail;
  size_t count;

  batch = pouch->fsync_head;
  tail = NULL;
  count = 0U;
  while (pouch->fsync_head != NULL && count < LC_POUCH_FSYNC_BATCH_MAX_OPS) {
    tail = pouch->fsync_head;
    pouch->fsync_head = pouch->fsync_head->next;
    ++count;
  }
  if (tail != NULL) {
    tail->next = NULL;
  }
  if (pouch->fsync_head == NULL) {
    pouch->fsync_tail = NULL;
  }
  if (pouch->fsync_queue_count >= count) {
    pouch->fsync_queue_count -= count;
  } else {
    pouch->fsync_queue_count = 0U;
  }
  if (out_count != NULL) {
    *out_count = count;
  }
  return batch;
}

static void *lc_pouch_fsync_worker(void *arg) {
  lc_pouch *pouch;
  lc_pouch_fsync_request *batch;
  lc_pouch_fsync_request *request;
  struct timespec deadline;
  size_t batch_count;

  pouch = (lc_pouch *)arg;
  pthread_mutex_lock(&pouch->fsync_mutex);
  for (;;) {
    while (pouch->fsync_head == NULL && !pouch->fsync_stop) {
      pthread_cond_wait(&pouch->fsync_cond, &pouch->fsync_mutex);
    }
    if (pouch->fsync_head == NULL && pouch->fsync_stop) {
      pthread_mutex_unlock(&pouch->fsync_mutex);
      return NULL;
    }
    if (!pouch->fsync_stop && LC_POUCH_FSYNC_BATCH_DELAY_NS > 0L &&
        pouch->fsync_queue_count < LC_POUCH_FSYNC_BATCH_MAX_OPS) {
      lc_pouch_fsync_deadline(&deadline);
      while (!pouch->fsync_stop &&
             pouch->fsync_queue_count < LC_POUCH_FSYNC_BATCH_MAX_OPS) {
        if (pthread_cond_timedwait(&pouch->fsync_cond, &pouch->fsync_mutex,
                                   &deadline) == ETIMEDOUT) {
          break;
        }
      }
    }
    batch = lc_pouch_fsync_take_batch(pouch, &batch_count);
    pthread_mutex_unlock(&pouch->fsync_mutex);
    lc_pouch_fsync_process_batch(pouch, batch, batch_count);
    pthread_mutex_lock(&pouch->fsync_mutex);
    for (request = batch; request != NULL; request = request->next) {
      request->done = 1;
      pthread_cond_signal(&request->cond);
    }
  }
}

static int lc_pouch_fsync_batcher_init(lc_pouch *pouch, lc_error *error) {
  int pthread_rc;

  if (pouch == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch fsync batcher requires pouch", NULL, NULL,
                        "pouch");
  }
  pthread_rc = pthread_mutex_init(&pouch->fsync_mutex, NULL);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch fsync mutex",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->fsync_mutex_initialized = 1;
  pthread_rc = pthread_cond_init(&pouch->fsync_cond, NULL);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch fsync condition",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->fsync_cond_initialized = 1;
  pthread_rc =
      pthread_create(&pouch->fsync_thread, NULL, lc_pouch_fsync_worker, pouch);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to start pouch fsync worker",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->fsync_thread_started = 1;
  return LC_OK;
}

static void lc_pouch_fsync_batcher_close(lc_pouch *pouch) {
  if (pouch == NULL) {
    return;
  }
  if (pouch->fsync_thread_started) {
    pthread_mutex_lock(&pouch->fsync_mutex);
    pouch->fsync_stop = 1;
    pthread_cond_broadcast(&pouch->fsync_cond);
    pthread_mutex_unlock(&pouch->fsync_mutex);
    pthread_join(pouch->fsync_thread, NULL);
    pouch->fsync_thread_started = 0;
  }
  if (pouch->fsync_cond_initialized) {
    pthread_cond_destroy(&pouch->fsync_cond);
    pouch->fsync_cond_initialized = 0;
  }
  if (pouch->fsync_mutex_initialized) {
    pthread_mutex_destroy(&pouch->fsync_mutex);
    pouch->fsync_mutex_initialized = 0;
  }
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
  if (pouch->compaction_namespace_count >= pouch->compaction_namespace_capacity) {
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
                        "failed to copy pouch compaction namespace", NULL,
                        NULL, NULL);
  }
  pouch->compaction_namespaces[pouch->compaction_namespace_count++] =
      name_copy;
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
  rc = lc_pouch_compaction_copy_namespaces(pouch, &namespaces,
                                           &namespace_count, &error);
  if (rc != LC_OK) {
    pslog_field fields[2];

    fields[0] = lc_log_error_field("error", &error);
    fields[1] = lc_log_code_field(&error);
    lc_log_warn(pouch->logger, "compaction.background.namespaces.error",
                fields, 2U);
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

  pouch = (lc_pouch *)arg;
  lc_pouch_compaction_run_pass(pouch);
  pthread_mutex_lock(&pouch->compaction_mutex);
  while (!pouch->compaction_stop) {
    struct timespec deadline;
    int wait_rc;

    lc_pouch_compaction_deadline(pouch, &deadline);
    wait_rc = 0;
    while (!pouch->compaction_stop && wait_rc != ETIMEDOUT) {
      wait_rc = pthread_cond_timedwait(&pouch->compaction_cond,
                                       &pouch->compaction_mutex, &deadline);
    }
    if (pouch->compaction_stop) {
      break;
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

  if (pouch == NULL || !pouch->background_compaction_enabled ||
      pouch->compaction_interval_seconds == 0U) {
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
  lc_pouch_compaction_namespaces_cleanup(
      pouch, pouch->compaction_namespaces, pouch->compaction_namespace_count);
  pouch->compaction_namespaces = NULL;
  pouch->compaction_namespace_count = 0U;
  pouch->compaction_namespace_capacity = 0U;
}

int lc_pouch_fsync_commit(lc_pouch *pouch, int fd, lc_error *error) {
  lc_pouch_fsync_request request;
  int pthread_rc;

  if (pouch == NULL || fd < 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch fsync commit requires pouch and fd", NULL, NULL,
                        "pouch");
  }
  if (!pouch->fsync_thread_started) {
    if (lc_pouch_sync_fd(fd) != 0) {
      pslog_field fields[2];

      fields[0] = lc_log_i64_field("fd", fd);
      fields[1] = lc_log_str_field("error", strerror(errno));
      lc_log_error(pouch->logger, "logstore.fsync.error", fields, 2U);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to sync pouch state segment", strerror(errno),
                          NULL, "pouch");
    }
    {
      pslog_field fields[2];

      fields[0] = lc_log_i64_field("fd", fd);
      fields[1] = lc_log_bool_field("batched", 0);
      lc_log_trace(pouch->logger, "logstore.fsync", fields, 2U);
    }
    return LC_OK;
  }
  memset(&request, 0, sizeof(request));
  request.fd = fd;
  pthread_rc = pthread_cond_init(&request.cond, NULL);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch fsync request",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pthread_mutex_lock(&pouch->fsync_mutex);
  if (pouch->fsync_stop) {
    pthread_mutex_unlock(&pouch->fsync_mutex);
    pthread_cond_destroy(&request.cond);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch fsync batcher is closed", NULL, NULL, "pouch");
  }
  if (pouch->fsync_tail != NULL) {
    pouch->fsync_tail->next = &request;
  } else {
    pouch->fsync_head = &request;
  }
  pouch->fsync_tail = &request;
  ++pouch->fsync_queue_count;
  pthread_cond_signal(&pouch->fsync_cond);
  while (!request.done) {
    pthread_cond_wait(&request.cond, &pouch->fsync_mutex);
  }
  pthread_mutex_unlock(&pouch->fsync_mutex);
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
  pouch->compaction_min_segment_count =
      options != NULL && options->compaction_min_segment_count != 0UL
          ? options->compaction_min_segment_count
          : LC_POUCH_DEFAULT_COMPACTION_MIN_SEGMENT_COUNT;
  pouch->compaction_min_reclaimable_bytes =
      options != NULL && options->compaction_min_reclaimable_bytes != 0U
          ? options->compaction_min_reclaimable_bytes
          : LC_POUCH_DEFAULT_COMPACTION_MIN_RECLAIMABLE_BYTES;
  pouch->compaction_interval_seconds =
      options != NULL ? options->compaction_interval_seconds : 0U;
  pouch->compaction_delete_grace_seconds =
      options != NULL && options->compaction_delete_grace_seconds != 0U
          ? options->compaction_delete_grace_seconds
          : LC_POUCH_DEFAULT_COMPACTION_DELETE_GRACE_SECONDS;
  pouch->compaction_max_io_bytes_per_sec =
      options != NULL ? options->compaction_max_io_bytes_per_sec : 0U;
  pouch->background_compaction_enabled =
      options != NULL ? options->background_compaction_enabled : 0;
  pouch->single_writer = options != NULL ? options->single_writer : 0;
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

static int lc_pouch_warm_transformed_namespaces(lc_pouch *pouch,
                                                lc_error *error) {
  char *namespaces_path;
  DIR *dir;
  struct dirent *entry;
  unsigned long namespace_count;
  int saved_errno;
  int rc;

  if (pouch == NULL || (!lc_pouch_crypto_enabled(pouch->crypto) &&
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
  char marker_leaf[128];
  char presence_leaf[128];
  unsigned long writer_id;

  pthread_mutex_lock(&lc_pouch_writer_marker_mutex);
  writer_id = ++lc_pouch_next_writer_marker_id;
  pthread_mutex_unlock(&lc_pouch_writer_marker_mutex);
  snprintf(marker_leaf, sizeof(marker_leaf), "writer-%ld-%020lu.marker",
           (long)getpid(),
           writer_id);
  snprintf(presence_leaf, sizeof(presence_leaf), "writer-%ld-%020lu.presence",
           (long)getpid(), writer_id);
  pouch->writer_marker_leaf =
      lc_strdup_with_allocator(&pouch->allocator, marker_leaf);
  pouch->writer_presence_leaf =
      lc_strdup_with_allocator(&pouch->allocator, presence_leaf);
  pouch->writer_presence_dir = lc_pouch_path_join(
      &pouch->allocator, pouch->root_path, "exclusive-writers");
  pouch->writer_presence_path = pouch->writer_presence_dir != NULL
                                    ? lc_pouch_path_join(&pouch->allocator,
                                                         pouch->writer_presence_dir,
                                                         presence_leaf)
                                    : NULL;
  if (pouch->writer_marker_leaf == NULL || pouch->writer_presence_leaf == NULL ||
      pouch->writer_presence_dir == NULL || pouch->writer_presence_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch writer marker paths", NULL,
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
          ((uintmax_t)INT64_MAX - (uintmax_t)now.tv_nsec) / 1000000000U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch writer presence clock is out of range", NULL,
                        NULL, "pouch");
  }
  *out = (int64_t)now.tv_sec * (int64_t)1000000000 +
         (int64_t)now.tv_nsec;
  return LC_OK;
}

static int lc_pouch_writer_presence_touch(lc_pouch *pouch, lc_error *error) {
  char payload[64];
  int64_t now_ns;
  int rc;

  if (pouch == NULL || pouch->writer_presence_dir == NULL ||
      pouch->writer_presence_path == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch writer presence is not initialized", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_writer_presence_now_ns(&now_ns, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (snprintf(payload, sizeof(payload), "%" PRId64 "\n", now_ns) < 0) {
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
                        "pouch writer presence is not initialized", NULL,
                        NULL, "pouch");
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

static void lc_pouch_writer_presence_stop(lc_pouch *pouch) {
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
  if (pouch->writer_presence_path != NULL) {
    (void)unlink(pouch->writer_presence_path);
  }
}

int lc_pouch_open(const char *root_path, const lc_allocator *allocator,
                  const lc_pouch_open_options *options, lc_pouch **out,
                  lc_error *error) {
  lc_pouch *pouch;
  lc_pouch_crypto_open_options crypto_options;
  int requested_single_writer;
  int pthread_rc;
  int rc;

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
  pthread_rc = pthread_mutex_init(&pouch->writer_presence_mutex, NULL);
  if (pthread_rc != 0) {
    lc_pouch_close(pouch);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch writer presence mutex",
                        strerror(pthread_rc), NULL, "pouch");
  }
  pouch->writer_presence_mutex_initialized = 1;
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
  rc = lc_pouch_ensure_root(pouch, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  rc = lc_pouch_fsync_batcher_init(pouch, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  rc = lc_pouch_warm_transformed_namespaces(pouch, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  rc = lc_pouch_compaction_worker_init(pouch, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  rc = lc_pouch_set_single_writer(pouch, requested_single_writer, error);
  if (rc != LC_OK) {
    lc_pouch_close(pouch);
    return rc;
  }
  *out = pouch;
  {
    pslog_field fields[8];

    fields[0] = lc_log_str_field("path", pouch->root_path);
    fields[1] = lc_log_str_field("query_engine", pouch->query_engine);
    fields[2] =
        lc_log_str_field("query_fallback_engine", pouch->query_fallback_engine);
    fields[3] = lc_log_str_field("compression", pouch->compression);
    fields[4] =
        lc_log_bool_field("crypto", lc_pouch_crypto_enabled(pouch->crypto));
    fields[5] =
        lc_log_u64_field("segment_target_bytes", pouch->segment_target_bytes);
    fields[6] =
        lc_log_bool_field("single_writer", lc_pouch_single_writer_enabled(pouch));
    fields[7] = lc_log_bool_field("background_compaction",
                                  pouch->background_compaction_enabled);
    lc_log_info(pouch->logger, "open", fields, 8U);
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
  lc_pouch_writer_presence_stop(pouch);
  lc_pouch_compaction_worker_close(pouch);
  lc_pouch_fsync_batcher_close(pouch);
  lc_pouch_state_cache_cleanup(pouch);
  lc_pouch_state_source_cache_cleanup(pouch);
  lc_pouch_query_index_cache_cleanup(pouch);
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
  if (pouch->single_writer_mutex_initialized) {
    pthread_mutex_destroy(&pouch->single_writer_mutex);
  }
  lc_free_with_allocator(&allocator, pouch);
}

int lc_pouch_set_single_writer(lc_pouch *pouch, int enabled, lc_error *error) {
  int normalized;
  int rc;

  if (pouch == NULL || !pouch->single_writer_mutex_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch single-writer control requires an open pouch",
                        NULL, NULL, "pouch");
  }
  normalized = enabled != 0 ? 1 : 0;
  pthread_mutex_lock(&pouch->single_writer_mutex);
  if (pouch->single_writer == normalized) {
    pthread_mutex_unlock(&pouch->single_writer_mutex);
    return LC_OK;
  }
  if (normalized) {
    rc = lc_pouch_writer_presence_start(pouch, error);
    if (rc != LC_OK) {
      pthread_mutex_unlock(&pouch->single_writer_mutex);
      return rc;
    }
    pouch->single_writer = 1;
  } else {
    pouch->single_writer = 0;
    lc_pouch_writer_presence_stop(pouch);
  }
  if (pouch->single_writer_epoch != UINT64_MAX) {
    ++pouch->single_writer_epoch;
  }
  pthread_mutex_unlock(&pouch->single_writer_mutex);
  {
    pslog_field fields[1];

    fields[0] = lc_log_bool_field("single_writer", normalized);
    lc_log_info(pouch->logger, "single_writer.set", fields, 1U);
  }
  return LC_OK;
}

static int lc_pouch_writer_presence_read(
    const char *path, int64_t *heartbeat_ns, int *has_heartbeat,
    lc_error *error) {
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
    intmax_t value;

    errno = 0;
    value = strtoimax(buffer, &end, 10);
    while (end != NULL && (*end == ' ' || *end == '\t' || *end == '\r' ||
                           *end == '\n')) {
      ++end;
    }
    if (errno == 0 && end != buffer && end != NULL && *end == '\0' &&
        value > 0 && (uintmax_t)value <= (uintmax_t)INT64_MAX) {
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
          ((uintmax_t)INT64_MAX - (uintmax_t)nanos) / 1000000000U) {
    return 0;
  }
  *out = seconds * (int64_t)1000000000 + (int64_t)nanos;
  return 1;
}

int lc_pouch_probe_exclusive_writer(
    lc_pouch *pouch, lc_pouch_exclusive_writer_presence *out,
    lc_error *error) {
  DIR *dir;
  struct dirent *entry;
  int64_t now_ns;
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
    expires_ns = heartbeat_ns > INT64_MAX - LC_POUCH_EXCLUSIVE_WRITER_TTL_NS
                     ? INT64_MAX
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
  out->compaction_min_segment_count = pouch->compaction_min_segment_count;
  out->compaction_min_reclaimable_bytes =
      pouch->compaction_min_reclaimable_bytes;
  out->compaction_interval_seconds = pouch->compaction_interval_seconds;
  out->compaction_delete_grace_seconds =
      pouch->compaction_delete_grace_seconds;
  out->compaction_max_io_bytes_per_sec =
      pouch->compaction_max_io_bytes_per_sec;
  out->background_compaction_enabled = pouch->background_compaction_enabled;
  out->single_writer = lc_pouch_single_writer_enabled(pouch);
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
  if (out->query_engine == NULL || out->query_fallback_engine == NULL ||
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

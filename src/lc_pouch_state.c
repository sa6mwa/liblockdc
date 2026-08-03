#include "lc_pouch.h"

#include "lc_api_internal.h"
#include "lc_log.h"
#include "lc_pouch_crypto.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"
#include "lc_pouch_query_index.h"
#include "lc_pouch_record.h"

#include <openssl/crypto.h>
#include <openssl/evp.h>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>
#include <zlib.h>

#define LC_POUCH_STATE_RECORD_HEADER_BYTES LC_POUCH_RECORD_HEADER_BYTES
#define LC_POUCH_STATE_PAYLOAD_META_FIXED_BYTES 74U
#define LC_POUCH_STATE_RECORD_META_MAX_BYTES (1024U * 1024U)
#define LC_POUCH_STATE_RECORD_KEY_MAX_BYTES (1024U * 1024U)
#define LC_POUCH_STATE_RECORD_DESCRIPTOR_RESERVE 256U
#define LC_POUCH_STATE_SHARED_FORCE_AFTER_SKIPS 64UL
#define LC_POUCH_STATE_BODY_CACHE_MAX_BYTES (128U * 1024U * 1024U)
#define LC_POUCH_STATE_BODY_CACHE_RECORD_MAX_BYTES (1024U * 1024U)
#define LC_POUCH_STATE_SOURCE_CACHE_MAX_FILES 64U
#define LC_POUCH_STATE_WRITEV_MAX_PARTS 15
#define LC_POUCH_STATE_WRITEV_BATCH_RECORDS 5U
#define LC_POUCH_STATE_INLINE_APPEND_BATCH_MAX_BYTES (256U * 1024U)
#define LC_POUCH_STATE_METADATA_APPEND_BATCH_MAX 128U
#define LC_POUCH_STATE_DECISION_COMMITTED "committed"
#define LC_POUCH_STATE_DECISION_DISCARDED "discarded"
#define LC_POUCH_STATE_HIGH_WATER_KEY ".lockd/high-water"
#define LC_POUCH_STATE_INDEX_TRAILER_BYTES 12U
#define LC_POUCH_STATE_INDEX_TRAILER_MAGIC 0x4c435349UL

#ifdef LOCKDC_TEST_BUILD
lc_pouch_test_hook lc_pouch_test_after_snapshot_write_hook = NULL;
void *lc_pouch_test_after_snapshot_write_context = NULL;
lc_pouch_test_metadata_append_hook_fn lc_pouch_test_metadata_append_hook = NULL;
void *lc_pouch_test_metadata_append_context = NULL;
lc_pouch_test_tail_repair_hook_fn lc_pouch_test_tail_repair_hook = NULL;
void *lc_pouch_test_tail_repair_context = NULL;
#endif

typedef struct lc_pouch_state_namespace_lock {
  int fd;
  struct lc_pouch_state_process_namespace_mutex *process_mutex;
  struct lc_pouch_state_process_namespace_guard *maintenance_guard;
  pthread_mutex_t *exclusive_append_gate;
  lc_pouch *pouch;
  const char *namespace_name;
  struct lc_pouch_state_namespace_lock *previous_namespace_lock;
} lc_pouch_state_namespace_lock;

typedef struct lc_pouch_state_key_lock {
  int fd;
  int maintenance_fd;
  struct lc_pouch_state_process_namespace_mutex *process_mutex;
  struct lc_pouch_state_process_namespace_guard *maintenance_guard;
  pthread_mutex_t *exclusive_key_mutex;
  lc_pouch *pouch;
  const char *namespace_name;
  int projection_uses_namespace_mutex;
  struct lc_pouch_state_key_lock *previous_projection_lock;
} lc_pouch_state_key_lock;

typedef struct lc_pouch_state_append_lock {
  int fd;
  struct lc_pouch_state_process_namespace_mutex *process_mutex;
  pthread_mutex_t *exclusive_gate;
} lc_pouch_state_append_lock;

typedef struct lc_pouch_state_deferred_fsync {
  int fd;
  dev_t device;
  ino_t inode;
  struct lc_pouch_state_deferred_fsync *next;
} lc_pouch_state_deferred_fsync;

typedef struct lc_pouch_state_commit_group {
  lc_pouch *pouch;
  struct lc_pouch_state_commit_group *parent;
  lc_pouch_state_deferred_fsync *head;
  lc_pouch_state_deferred_fsync *tail;
} lc_pouch_state_commit_group;

typedef struct lc_pouch_state_process_namespace_mutex {
  char *identity;
  pthread_mutex_t mutex;
  unsigned long refcount;
  struct lc_pouch_state_process_namespace_mutex *next;
} lc_pouch_state_process_namespace_mutex;

typedef struct lc_pouch_state_process_namespace_guard {
  char *identity;
  pthread_rwlock_t lock;
  pthread_mutex_t writer_mutex;
  pthread_t writer;
  unsigned long writer_depth;
  int writer_active;
  unsigned long refcount;
  struct lc_pouch_state_process_namespace_guard *next;
} lc_pouch_state_process_namespace_guard;

static pthread_mutex_t lc_pouch_state_process_mutex_registry =
    PTHREAD_MUTEX_INITIALIZER;
static lc_pouch_state_process_namespace_mutex *lc_pouch_state_process_mutexes;
static pthread_mutex_t lc_pouch_state_process_guard_registry =
    PTHREAD_MUTEX_INITIALIZER;
static lc_pouch_state_process_namespace_guard *lc_pouch_state_process_guards;
static pthread_once_t lc_pouch_state_commit_group_key_once = PTHREAD_ONCE_INIT;
static pthread_key_t lc_pouch_state_commit_group_key;
static pthread_once_t lc_pouch_state_namespace_lock_key_once =
    PTHREAD_ONCE_INIT;
static pthread_key_t lc_pouch_state_namespace_lock_key;
static int lc_pouch_state_namespace_lock_key_status;
static pthread_once_t lc_pouch_state_projection_lock_key_once =
    PTHREAD_ONCE_INIT;
static pthread_key_t lc_pouch_state_projection_lock_key;
static int lc_pouch_state_projection_lock_key_status;

typedef struct lc_pouch_state_entry lc_pouch_state_entry;

static int lc_pouch_state_manifest_leaf_obsolete(
    const lc_pouch_namespace_manifest *manifest, const char *leaf,
    int snapshot);

static int lc_pouch_state_manifest_segment_visible(
    const lc_pouch_namespace_manifest *manifest, const char *leaf) {
  if (manifest == NULL || leaf == NULL ||
      lc_pouch_state_manifest_leaf_obsolete(manifest, leaf, 0)) {
    return 0;
  }
  return 1;
}

static int
lc_pouch_state_manifest_has_segment(const lc_pouch_namespace_manifest *manifest,
                                    const char *leaf) {
  unsigned long index;

  if (manifest == NULL || leaf == NULL || manifest->segment_leaves == NULL) {
    return 0;
  }
  for (index = 0UL; index < manifest->segment_count; ++index) {
    if (manifest->segment_leaves[index] != NULL &&
        strcmp(manifest->segment_leaves[index], leaf) == 0) {
      return 1;
    }
  }
  return 0;
}

static int lc_pouch_state_manifest_segment_allows_tail_repair(
    const lc_pouch_namespace_manifest *manifest, const char *leaf) {
  if (manifest == NULL || leaf == NULL) {
    return 0;
  }
  return manifest->active_segment != NULL &&
         strcmp(leaf, manifest->active_segment) == 0;
}

static int lc_pouch_state_meta_set_index_seq(unsigned char *meta,
                                             size_t meta_len,
                                             lc_pouch_generation index_seq,
                                             lc_error *error);
static int lc_pouch_state_meta_index_seq(const unsigned char *meta,
                                         size_t meta_len,
                                         lc_pouch_generation *out,
                                         lc_error *error);
static void
lc_pouch_state_namespace_lock_release(lc_pouch_state_namespace_lock *lock);
static int lc_pouch_state_manifest_max_version(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    lc_pouch_generation *max_version, lc_error *error);
static int lc_pouch_state_repair_active_tail_locked(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_namespace_manifest *manifest, const char *segment_path,
    const char *segment_leaf, lc_error *error);
static int lc_pouch_state_fd_seek(int fd, uint64_t offset, const char *message,
                                  lc_error *error);
static int lc_pouch_state_cache_set_active_segment(
    lc_pouch *pouch, lc_pouch_namespace_logstore *cache,
    const lc_pouch_namespace_manifest *manifest, uint64_t offset,
    lc_error *error);
static int lc_pouch_state_cache_active_append_fd(
    lc_pouch *pouch, lc_pouch_namespace_logstore *cache,
    const lc_pouch_namespace_manifest *manifest, int *out, lc_error *error);
static int
lc_pouch_state_cache_lookup(lc_pouch *pouch, const char *namespace_name,
                            const lc_pouch_namespace_manifest *manifest,
                            const char *key, lc_pouch_state_entry *out,
                            lc_pouch_generation *max_version_out,
                            lc_error *error);
static int lc_pouch_state_cache_refresh_for_mode(
    lc_pouch *pouch, lc_pouch_namespace_logstore *cache,
    const lc_pouch_namespace_manifest *manifest, lc_error *error);
static int lc_pouch_state_cache_matches_manifest(
    const lc_pouch_namespace_logstore *cache,
    const lc_pouch_namespace_manifest *manifest);
static void
lc_pouch_state_cache_invalidate_namespace(lc_pouch *pouch,
                                          const char *namespace_name);
static int
lc_pouch_state_manifest_materialize(lc_pouch *pouch, const char *namespace_name,
                                    lc_pouch_namespace_manifest *manifest,
                                    int *from_cache, lc_error *error);
static int lc_pouch_state_process_namespace_mutex_lock(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_state_process_namespace_mutex **out, lc_error *error);
static void lc_pouch_state_process_namespace_mutex_unlock(
    lc_pouch_state_process_namespace_mutex **mutex);
static int lc_pouch_state_exclusive_append_gate_lock(lc_pouch *pouch,
                                                     const char *namespace_name,
                                                     pthread_mutex_t **out,
                                                     lc_error *error);
static void lc_pouch_state_exclusive_append_gate_unlock(pthread_mutex_t **gate);

static void lc_pouch_state_commit_group_key_init(void) {
  (void)pthread_key_create(&lc_pouch_state_commit_group_key, NULL);
}

static void lc_pouch_state_namespace_lock_key_init(void) {
  lc_pouch_state_namespace_lock_key_status =
      pthread_key_create(&lc_pouch_state_namespace_lock_key, NULL);
}

static void lc_pouch_state_projection_lock_key_init(void) {
  lc_pouch_state_projection_lock_key_status =
      pthread_key_create(&lc_pouch_state_projection_lock_key, NULL);
}

static int
lc_pouch_state_namespace_lock_track(lc_pouch_state_namespace_lock *lock,
                                    lc_pouch *pouch, const char *namespace_name,
                                    lc_error *error) {
  int pthread_rc;

  pthread_once(&lc_pouch_state_namespace_lock_key_once,
               lc_pouch_state_namespace_lock_key_init);
  if (lc_pouch_state_namespace_lock_key_status != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch namespace lock tracking",
                        strerror(lc_pouch_state_namespace_lock_key_status),
                        NULL, "pouch");
  }
  lock->pouch = pouch;
  lock->namespace_name = namespace_name;
  lock->previous_namespace_lock =
      (lc_pouch_state_namespace_lock *)pthread_getspecific(
          lc_pouch_state_namespace_lock_key);
  pthread_rc = pthread_setspecific(lc_pouch_state_namespace_lock_key, lock);
  if (pthread_rc != 0) {
    lock->pouch = NULL;
    lock->namespace_name = NULL;
    lock->previous_namespace_lock = NULL;
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to track pouch namespace lock",
                        strerror(pthread_rc), NULL, "pouch");
  }
  return LC_OK;
}

static int lc_pouch_state_projection_lock_track(lc_pouch_state_key_lock *lock,
                                                lc_error *error) {
  int pthread_rc;

  if (lock == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch projection lock tracking requires lock", NULL,
                        NULL, "pouch");
  }
  pthread_once(&lc_pouch_state_projection_lock_key_once,
               lc_pouch_state_projection_lock_key_init);
  if (lc_pouch_state_projection_lock_key_status != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch projection lock tracking",
                        strerror(lc_pouch_state_projection_lock_key_status),
                        NULL, "pouch");
  }
  lock->previous_projection_lock =
      (lc_pouch_state_key_lock *)pthread_getspecific(
          lc_pouch_state_projection_lock_key);
  pthread_rc = pthread_setspecific(lc_pouch_state_projection_lock_key, lock);
  if (pthread_rc != 0) {
    lock->previous_projection_lock = NULL;
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to track pouch projection lock",
                        strerror(pthread_rc), NULL, "pouch");
  }
  return LC_OK;
}

static void
lc_pouch_state_projection_lock_untrack(lc_pouch_state_key_lock *lock) {
  if (lock == NULL) {
    return;
  }
  pthread_once(&lc_pouch_state_projection_lock_key_once,
               lc_pouch_state_projection_lock_key_init);
  if (lc_pouch_state_projection_lock_key_status == 0) {
    (void)pthread_setspecific(lc_pouch_state_projection_lock_key,
                              lock->previous_projection_lock);
  }
  lock->previous_projection_lock = NULL;
}

static lc_pouch_state_key_lock *
lc_pouch_state_projection_lock_current(lc_pouch *pouch,
                                       const char *namespace_name) {
  lc_pouch_state_key_lock *lock;

  pthread_once(&lc_pouch_state_projection_lock_key_once,
               lc_pouch_state_projection_lock_key_init);
  if (lc_pouch_state_projection_lock_key_status != 0) {
    return NULL;
  }
  lock = (lc_pouch_state_key_lock *)pthread_getspecific(
      lc_pouch_state_projection_lock_key);
  if (lock == NULL || lock->pouch != pouch || lock->namespace_name == NULL ||
      lock->namespace_name[0] == '\0' || pouch == NULL ||
      namespace_name == NULL ||
      strcmp(lock->namespace_name, namespace_name) != 0) {
    return NULL;
  }
  return lock;
}

static int lc_pouch_state_namespace_lock_is_held(lc_pouch *pouch,
                                                 const char *namespace_name) {
  lc_pouch_state_namespace_lock *lock;

  pthread_once(&lc_pouch_state_namespace_lock_key_once,
               lc_pouch_state_namespace_lock_key_init);
  if (lc_pouch_state_namespace_lock_key_status != 0) {
    return 0;
  }
  for (lock = (lc_pouch_state_namespace_lock *)pthread_getspecific(
           lc_pouch_state_namespace_lock_key);
       lock != NULL; lock = lock->previous_namespace_lock) {
    if (lock->pouch == pouch && lock->namespace_name != NULL &&
        strcmp(lock->namespace_name, namespace_name) == 0) {
      return 1;
    }
  }
  return 0;
}

static void
lc_pouch_state_namespace_lock_untrack(lc_pouch_state_namespace_lock *lock) {
  if (lock == NULL || lock->pouch == NULL) {
    return;
  }
  if (pthread_getspecific(lc_pouch_state_namespace_lock_key) == lock) {
    (void)pthread_setspecific(lc_pouch_state_namespace_lock_key,
                              lock->previous_namespace_lock);
  }
  lock->pouch = NULL;
  lock->namespace_name = NULL;
  lock->previous_namespace_lock = NULL;
}

static lc_pouch_state_commit_group *lc_pouch_state_commit_group_current(void) {
  pthread_once(&lc_pouch_state_commit_group_key_once,
               lc_pouch_state_commit_group_key_init);
  return (lc_pouch_state_commit_group *)pthread_getspecific(
      lc_pouch_state_commit_group_key);
}

static void
lc_pouch_state_commit_group_set(lc_pouch_state_commit_group *group) {
  pthread_once(&lc_pouch_state_commit_group_key_once,
               lc_pouch_state_commit_group_key_init);
  (void)pthread_setspecific(lc_pouch_state_commit_group_key, group);
}

static int lc_pouch_state_commit_group_begin(lc_pouch *pouch,
                                             lc_pouch_state_commit_group **out,
                                             int *owned, lc_error *error) {
  lc_pouch_state_commit_group *current;
  lc_pouch_state_commit_group *group;

  if (pouch == NULL || out == NULL || owned == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch commit group requires pouch and outputs", NULL,
                        NULL, "pouch");
  }
  if (pouch->aborted) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch mutations are closed after abort", NULL, NULL,
                        "pouch");
  }
  current = lc_pouch_state_commit_group_current();
  if (current != NULL && current->pouch == pouch) {
    *out = current;
    *owned = 0;
    return LC_OK;
  }
  group = (lc_pouch_state_commit_group *)lc_calloc_with_allocator(
      NULL, 1U, sizeof(*group));
  if (group == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch commit group", NULL, NULL,
                        "pouch");
  }
  group->pouch = pouch;
  group->parent = current;
  lc_pouch_state_commit_group_set(group);
  *out = group;
  *owned = 1;
  return LC_OK;
}

static void
lc_pouch_state_commit_group_free(lc_pouch_state_commit_group *group) {
  lc_pouch_state_deferred_fsync *item;
  lc_pouch_state_deferred_fsync *next;

  if (group == NULL) {
    return;
  }
  item = group->head;
  while (item != NULL) {
    next = item->next;
    if (item->fd >= 0) {
      close(item->fd);
    }
    lc_free_with_allocator(NULL, item);
    item = next;
  }
  lc_free_with_allocator(NULL, group);
}

static int lc_pouch_state_commit_group_end(lc_pouch_state_commit_group *group,
                                           int owned, lc_error *error) {
  lc_pouch_state_deferred_fsync *item;
  int rc;

  if (!owned) {
    return LC_OK;
  }
  if (group == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch commit group end requires group", NULL, NULL,
                        "pouch");
  }
  lc_pouch_state_commit_group_set(group->parent);
  rc = LC_OK;
  for (item = group->head; item != NULL; item = item->next) {
    if (item->fd >= 0) {
      rc = lc_pouch_fsync_commit(group->pouch, item->fd, error);
      if (rc != LC_OK) {
        break;
      }
    }
  }
  lc_pouch_state_commit_group_free(group);
  return rc;
}

static int lc_pouch_state_defer_fsync(lc_pouch *pouch, int fd,
                                      lc_error *error) {
  lc_pouch_state_commit_group *group;
  lc_pouch_state_deferred_fsync *item;
  lc_pouch_state_deferred_fsync *existing;
  struct stat st;
  int dup_fd;

  if (pouch == NULL || !pouch->durable_sync) {
    return LC_OK;
  }
  group = lc_pouch_state_commit_group_current();
  if (group == NULL || group->pouch != pouch) {
    return lc_pouch_fsync_commit(pouch, fd, error);
  }
  if (fstat(fd, &st) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to stat pouch deferred fsync file",
                        strerror(errno), NULL, "pouch");
  }
  for (existing = group->head; existing != NULL; existing = existing->next) {
    if (existing->device == st.st_dev && existing->inode == st.st_ino) {
      return LC_OK;
    }
  }
  dup_fd = dup(fd);
  if (dup_fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to duplicate pouch state segment fd",
                        strerror(errno), NULL, "pouch");
  }
  item = (lc_pouch_state_deferred_fsync *)lc_calloc_with_allocator(
      NULL, 1U, sizeof(*item));
  if (item == NULL) {
    close(dup_fd);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch deferred fsync", NULL, NULL,
                        "pouch");
  }
  item->fd = dup_fd;
  item->device = st.st_dev;
  item->inode = st.st_ino;
  if (group->tail != NULL) {
    group->tail->next = item;
  } else {
    group->head = item;
  }
  group->tail = item;
  return LC_OK;
}

static int
lc_pouch_state_finish_commit_group_common(lc_pouch_state_commit_group *group,
                                          int owned, int rc, lc_error *error) {
  int commit_rc;

  commit_rc = LC_OK;
  if (group != NULL) {
    commit_rc = lc_pouch_state_commit_group_end(group, owned,
                                                rc == LC_OK ? error : NULL);
  }
  if (commit_rc != LC_OK && rc == LC_OK) {
    rc = commit_rc;
  }
  return rc;
}

static int
lc_pouch_state_finish_commit_group(lc_pouch_state_commit_group *group,
                                   int owned, int rc, lc_error *error) {
  return lc_pouch_state_finish_commit_group_common(group, owned, rc, error);
}

static int lc_pouch_state_mutex_init_recursive(pthread_mutex_t *mutex,
                                               lc_error *error) {
  pthread_mutexattr_t attr;
  int pthread_rc;

  pthread_rc = pthread_mutexattr_init(&attr);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch mutex attributes",
                        strerror(pthread_rc), NULL, NULL);
  }
  pthread_rc = pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
  if (pthread_rc == 0) {
    pthread_rc = pthread_mutex_init(mutex, &attr);
  }
  (void)pthread_mutexattr_destroy(&attr);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch namespace mutex",
                        strerror(pthread_rc), NULL, NULL);
  }
  return LC_OK;
}

static int lc_pouch_state_process_mutex_identity(lc_pouch *pouch,
                                                 const char *namespace_name,
                                                 char **out, lc_error *error) {
  char root_identity[34];
  char device_hex[17];
  char inode_hex[17];
  size_t namespace_len;
  char *identity;

  if (pouch == NULL || namespace_name == NULL || out == NULL ||
      !pouch->root_identity_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace mutex identity requires pouch, "
                        "namespace, and output",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  if (lc_u64_format_base16_padded((lc_u64)pouch->root_device, 16U, device_hex,
                                  sizeof(device_hex)) < 0 ||
      lc_u64_format_base16_padded((lc_u64)pouch->root_inode, 16U, inode_hex,
                                  sizeof(inode_hex)) < 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace mutex identity is too large", NULL,
                        NULL, "pouch");
  }
  memcpy(root_identity, device_hex, 16U);
  root_identity[16] = ':';
  memcpy(root_identity + 17U, inode_hex, 16U);
  root_identity[33] = '\0';
  namespace_len = strlen(namespace_name);
  identity = (char *)lc_alloc_with_allocator(NULL, sizeof(root_identity) +
                                                       namespace_len + 1U);
  if (identity == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace mutex identity",
                        NULL, NULL, NULL);
  }
  memcpy(identity, root_identity, sizeof(root_identity) - 1U);
  identity[sizeof(root_identity) - 1U] = '\n';
  memcpy(identity + sizeof(root_identity), namespace_name, namespace_len + 1U);
  *out = identity;
  return LC_OK;
}

static int lc_pouch_state_process_namespace_mutex_lock(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_state_process_namespace_mutex **out, lc_error *error) {
  lc_pouch_state_process_namespace_mutex *entry;
  lc_pouch_state_process_namespace_mutex *created;
  char *identity;
  int pthread_rc;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace mutex requires pouch, namespace, and "
                        "output",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  identity = NULL;
  rc = lc_pouch_state_process_mutex_identity(pouch, namespace_name, &identity,
                                             error);
  if (rc != LC_OK) {
    return rc;
  }
  pthread_rc = pthread_mutex_lock(&lc_pouch_state_process_mutex_registry);
  if (pthread_rc != 0) {
    lc_free_with_allocator(NULL, identity);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch namespace mutex registry",
                        strerror(pthread_rc), NULL, NULL);
  }
  for (entry = lc_pouch_state_process_mutexes; entry != NULL;
       entry = entry->next) {
    if (strcmp(entry->identity, identity) == 0) {
      break;
    }
  }
  if (entry == NULL) {
    created =
        (lc_pouch_state_process_namespace_mutex *)lc_calloc_with_allocator(
            NULL, 1U, sizeof(*created));
    if (created == NULL) {
      pthread_mutex_unlock(&lc_pouch_state_process_mutex_registry);
      lc_free_with_allocator(NULL, identity);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch namespace mutex", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_state_mutex_init_recursive(&created->mutex, error);
    if (rc != LC_OK) {
      pthread_mutex_unlock(&lc_pouch_state_process_mutex_registry);
      lc_free_with_allocator(NULL, identity);
      lc_free_with_allocator(NULL, created);
      return rc;
    }
    created->identity = identity;
    created->next = lc_pouch_state_process_mutexes;
    lc_pouch_state_process_mutexes = created;
    entry = created;
    identity = NULL;
  }
  entry->refcount += 1UL;
  pthread_mutex_unlock(&lc_pouch_state_process_mutex_registry);
  lc_free_with_allocator(NULL, identity);
  pthread_rc = pthread_mutex_lock(&entry->mutex);
  if (pthread_rc != 0) {
    pthread_mutex_lock(&lc_pouch_state_process_mutex_registry);
    entry->refcount -= 1UL;
    pthread_mutex_unlock(&lc_pouch_state_process_mutex_registry);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch namespace mutex",
                        strerror(pthread_rc), NULL, NULL);
  }
  *out = entry;
  return LC_OK;
}

static void lc_pouch_state_process_namespace_mutex_unlock(
    lc_pouch_state_process_namespace_mutex **mutex) {
  lc_pouch_state_process_namespace_mutex *entry;

  if (mutex == NULL || *mutex == NULL) {
    return;
  }
  entry = *mutex;
  *mutex = NULL;
  pthread_mutex_unlock(&entry->mutex);
  pthread_mutex_lock(&lc_pouch_state_process_mutex_registry);
  if (entry->refcount > 0UL) {
    entry->refcount -= 1UL;
  }
  pthread_mutex_unlock(&lc_pouch_state_process_mutex_registry);
}

/* Default exclusive mode protects one namespace projection with the same
 * in-process namespace mutex used by readers and maintenance. Shared mode
 * retains the global projection mutex until its cross-process path has its own
 * resident namespace owner. */
static void lc_pouch_state_projection_mutex_unlock(lc_pouch *pouch,
                                                   const char *namespace_name) {
  lc_pouch_state_key_lock *lock;

  if (lc_pouch_single_writer_enabled(pouch)) {
    lock = lc_pouch_state_projection_lock_current(pouch, namespace_name);
    if (lock != NULL && lock->process_mutex != NULL) {
      lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
    }
    return;
  }
  (void)pthread_mutex_unlock(&pouch->state_mutation_mutex);
}

static int lc_pouch_state_projection_mutex_lock(lc_pouch *pouch,
                                                const char *namespace_name,
                                                lc_error *error) {
  lc_pouch_state_key_lock *lock;
  int pthread_rc;

  if (lc_pouch_single_writer_enabled(pouch)) {
    lock = lc_pouch_state_projection_lock_current(pouch, namespace_name);
    if (lock == NULL || lock->process_mutex != NULL) {
      return LC_OK;
    }
    return lc_pouch_state_process_namespace_mutex_lock(
        pouch, namespace_name, &lock->process_mutex, error);
  }
  pthread_rc = pthread_mutex_lock(&pouch->state_mutation_mutex);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to restore pouch mutation state",
                        strerror(pthread_rc), NULL, "pouch");
  }
  return LC_OK;
}

static int lc_pouch_state_process_namespace_guard_lock(
    lc_pouch *pouch, const char *namespace_name, int exclusive,
    lc_pouch_state_process_namespace_guard **out, lc_error *error) {
  lc_pouch_state_process_namespace_guard *entry;
  lc_pouch_state_process_namespace_guard *created;
  char *identity;
  int pthread_rc;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch maintenance guard requires namespace and output",
                        NULL, NULL, "pouch");
  }
  *out = NULL;
  identity = NULL;
  rc = lc_pouch_state_process_mutex_identity(pouch, namespace_name, &identity,
                                             error);
  if (rc != LC_OK) {
    return rc;
  }
  pthread_rc = pthread_mutex_lock(&lc_pouch_state_process_guard_registry);
  if (pthread_rc != 0) {
    lc_free_with_allocator(NULL, identity);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch maintenance guard registry",
                        strerror(pthread_rc), NULL, "pouch");
  }
  for (entry = lc_pouch_state_process_guards; entry != NULL;
       entry = entry->next) {
    if (strcmp(entry->identity, identity) == 0) {
      break;
    }
  }
  if (entry == NULL) {
    created =
        (lc_pouch_state_process_namespace_guard *)lc_calloc_with_allocator(
            NULL, 1U, sizeof(*created));
    if (created == NULL) {
      pthread_mutex_unlock(&lc_pouch_state_process_guard_registry);
      lc_free_with_allocator(NULL, identity);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch maintenance guard", NULL,
                          NULL, "pouch");
    }
    pthread_rc = pthread_rwlock_init(&created->lock, NULL);
    if (pthread_rc != 0) {
      pthread_mutex_unlock(&lc_pouch_state_process_guard_registry);
      lc_free_with_allocator(NULL, identity);
      lc_free_with_allocator(NULL, created);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to initialize pouch maintenance guard",
                          strerror(pthread_rc), NULL, "pouch");
    }
    pthread_rc = pthread_mutex_init(&created->writer_mutex, NULL);
    if (pthread_rc != 0) {
      (void)pthread_rwlock_destroy(&created->lock);
      pthread_mutex_unlock(&lc_pouch_state_process_guard_registry);
      lc_free_with_allocator(NULL, identity);
      lc_free_with_allocator(NULL, created);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to initialize pouch maintenance writer state",
                          strerror(pthread_rc), NULL, "pouch");
    }
    created->identity = identity;
    created->next = lc_pouch_state_process_guards;
    lc_pouch_state_process_guards = created;
    entry = created;
    identity = NULL;
  }
  entry->refcount += 1UL;
  pthread_mutex_unlock(&lc_pouch_state_process_guard_registry);
  lc_free_with_allocator(NULL, identity);
  if (exclusive) {
    pthread_rc = pthread_mutex_lock(&entry->writer_mutex);
    if (pthread_rc != 0) {
      pthread_mutex_lock(&lc_pouch_state_process_guard_registry);
      entry->refcount -= 1UL;
      pthread_mutex_unlock(&lc_pouch_state_process_guard_registry);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to lock pouch maintenance writer state",
                          strerror(pthread_rc), NULL, "pouch");
    }
    if (entry->writer_active && pthread_equal(entry->writer, pthread_self())) {
      entry->writer_depth += 1UL;
      (void)pthread_mutex_unlock(&entry->writer_mutex);
      *out = entry;
      return LC_OK;
    }
    (void)pthread_mutex_unlock(&entry->writer_mutex);
  }
  pthread_rc = exclusive ? pthread_rwlock_wrlock(&entry->lock)
                         : pthread_rwlock_rdlock(&entry->lock);
  if (pthread_rc != 0) {
    pthread_mutex_lock(&lc_pouch_state_process_guard_registry);
    entry->refcount -= 1UL;
    pthread_mutex_unlock(&lc_pouch_state_process_guard_registry);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch maintenance guard",
                        strerror(pthread_rc), NULL, "pouch");
  }
  if (exclusive) {
    pthread_rc = pthread_mutex_lock(&entry->writer_mutex);
    if (pthread_rc != 0) {
      (void)pthread_rwlock_unlock(&entry->lock);
      pthread_mutex_lock(&lc_pouch_state_process_guard_registry);
      entry->refcount -= 1UL;
      pthread_mutex_unlock(&lc_pouch_state_process_guard_registry);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to record pouch maintenance writer",
                          strerror(pthread_rc), NULL, "pouch");
    }
    entry->writer = pthread_self();
    entry->writer_active = 1;
    entry->writer_depth = 1UL;
    (void)pthread_mutex_unlock(&entry->writer_mutex);
  }
  *out = entry;
  return LC_OK;
}

static void lc_pouch_state_process_namespace_guard_unlock(
    lc_pouch_state_process_namespace_guard **guard) {
  lc_pouch_state_process_namespace_guard *entry;
  int unlock_rwlock;

  if (guard == NULL || *guard == NULL) {
    return;
  }
  entry = *guard;
  *guard = NULL;
  unlock_rwlock = 1;
  if (pthread_mutex_lock(&entry->writer_mutex) == 0) {
    if (entry->writer_active && pthread_equal(entry->writer, pthread_self())) {
      if (entry->writer_depth > 0UL) {
        entry->writer_depth -= 1UL;
      }
      if (entry->writer_depth == 0UL) {
        entry->writer_active = 0;
      } else {
        unlock_rwlock = 0;
      }
    }
    (void)pthread_mutex_unlock(&entry->writer_mutex);
  }
  if (unlock_rwlock) {
    (void)pthread_rwlock_unlock(&entry->lock);
  }
  pthread_mutex_lock(&lc_pouch_state_process_guard_registry);
  if (entry->refcount > 0UL) {
    entry->refcount -= 1UL;
  }
  pthread_mutex_unlock(&lc_pouch_state_process_guard_registry);
}

typedef struct lc_pouch_state_payload_span {
  char *container_leaf;
  uint64_t record_offset;
  uint64_t payload_offset;
  uint64_t payload_length;
  unsigned long payload_crc;
  int present;
} lc_pouch_state_payload_span;

struct lc_pouch_state_entry {
  char *key;
  char *content_type;
  char *etag;
  lc_pouch_state_payload_span payload_span;
  char *record_container_leaf;
  uint64_t record_offset;
  char *payload_context;
  char *descriptor;
  unsigned char *metadata;
  size_t metadata_length;
  char *decision;
  lc_pouch_generation index_seq;
  lc_pouch_generation version;
  uint64_t bytes;
  uint64_t cipher_bytes;
  lc_pouch_unix_seconds updated_at_unix;
  int has_query_hidden;
  int query_hidden;
  int seen;
  int found;
  int control;
  int has_record_ref;
  unsigned char record_type;
};

static void lc_pouch_state_precondition_view_from_entry(
    const lc_pouch_state_entry *entry, lc_pouch_state_precondition_view *out) {
  memset(out, 0, sizeof(*out));
  if (entry == NULL || !entry->found) {
    return;
  }
  out->found = 1;
  out->version = entry->version;
  out->metadata = entry->metadata;
  out->metadata_length = entry->metadata_length;
  out->has_query_hidden = entry->has_query_hidden;
  out->query_hidden = entry->query_hidden;
  out->has_body = entry->payload_span.present;
}

typedef enum lc_pouch_state_record_type {
  LC_POUCH_STATE_RECORD_STATE_PUT = 1,
  LC_POUCH_STATE_RECORD_STATE_DELETE = 2,
  LC_POUCH_STATE_RECORD_STATE_LINK = 3,
  LC_POUCH_STATE_RECORD_STATE_META = 4,
  LC_POUCH_STATE_RECORD_DECISION = 5,
  LC_POUCH_STATE_RECORD_HIGH_WATER = 6,
  LC_POUCH_STATE_RECORD_OBJECT_PUT = 7,
  LC_POUCH_STATE_RECORD_OBJECT_DELETE = 8
} lc_pouch_state_record_type;

typedef struct lc_pouch_state_binary_append_item {
  unsigned char record_type;
  const void *key;
  size_t key_len;
  unsigned char *meta;
  size_t meta_len;
} lc_pouch_state_binary_append_item;

typedef struct lc_pouch_state_metadata_append_request {
  const char *namespace_name;
  const char *key;
  lc_pouch_state_entry *current;
  lc_pouch_state_write_options options;
  lc_pouch_generation version;
  lc_pouch_unix_seconds updated_at_unix;
  int has_query_hidden;
  int query_hidden;
  lc_pouch_state_write_result *out;
  lc_error *error;
  int rc;
  int done;
  pthread_cond_t cond;
  struct lc_pouch_state_metadata_append_request *next;
} lc_pouch_state_metadata_append_request;

struct lc_pouch_state_metadata_append_batcher {
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  pthread_t thread;
  lc_pouch *pouch;
  char *namespace_name;
  lc_pouch_state_metadata_append_request *head;
  lc_pouch_state_metadata_append_request *tail;
  int stop;
  struct lc_pouch_state_metadata_append_batcher *next;
};

typedef struct lc_pouch_state_body_cache_entry {
  unsigned char *bytes;
  size_t length;
  lc_pouch_generation version;
  unsigned long refcount;
  int retired;
} lc_pouch_state_body_cache_entry;

struct lc_pouch_source_cache_entry {
  char *path;
  int fd;
  unsigned long last_used;
  struct lc_pouch_source_cache_entry *next;
};

typedef struct lc_pouch_state_decision {
  char *staged_key;
  char *etag;
  char *decision;
  lc_pouch_generation index_seq;
  lc_pouch_generation version;
  struct lc_pouch_state_decision *next;
} lc_pouch_state_decision;

typedef struct lc_pouch_state_cache_record {
  char *key;
  char *content_type;
  char *etag;
  lc_pouch_state_payload_span payload_span;
  char *record_container_leaf;
  uint64_t record_offset;
  char *payload_context;
  char *descriptor;
  unsigned char *metadata;
  size_t metadata_length;
  lc_pouch_generation index_seq;
  lc_pouch_generation version;
  uint64_t bytes;
  uint64_t cipher_bytes;
  lc_pouch_unix_seconds updated_at_unix;
  lc_pouch_state_body_cache_entry *body_cache;
  int has_query_hidden;
  int query_hidden;
  int found;
  int has_record_ref;
  unsigned char record_type;
  struct lc_pouch_state_cache_record *next;
  struct lc_pouch_state_cache_record *bucket_next;
} lc_pouch_state_cache_record;

static lc_pouch_namespace_logstore *
lc_pouch_namespace_logstore_find(lc_pouch *pouch, const char *namespace_name,
                                 int create, lc_error *error);
static lc_pouch_state_cache_record *
lc_pouch_state_cache_record_find(lc_pouch_namespace_logstore *ns,
                                 const char *key);
static int lc_pouch_state_compaction_record_ref_matches(
    const lc_pouch_state_cache_record *record, const char *container,
    uint64_t offset);
static int lc_pouch_state_read_many_snapshot_body_from_cache(
    lc_pouch *pouch, lc_pouch_namespace_logstore *cache,
    lc_pouch_state_cache_record *record, const char *crypto_context,
    const char *payload_span_path, uint64_t payload_offset,
    uint64_t payload_length, const lc_pouch_state_entry *current,
    int copy_cached_body, lc_source **out, lc_error *error);

static int lc_pouch_state_payload_container_is_valid(const char *leaf);
static void
lc_pouch_state_payload_span_cleanup(const lc_allocator *allocator,
                                    lc_pouch_state_payload_span *span);
static int lc_pouch_state_payload_span_copy(
    const lc_allocator *allocator, const lc_pouch_state_payload_span *src,
    lc_pouch_state_payload_span *dst, lc_error *error);
static int lc_pouch_state_payload_span_set(
    const lc_allocator *allocator, lc_pouch_state_payload_span *span,
    const char *container_leaf, uint64_t record_offset, uint64_t payload_offset,
    uint64_t payload_length, unsigned long payload_crc, lc_error *error);
static char *lc_pouch_state_payload_span_path(
    const lc_allocator *allocator, const char *namespace_path,
    const lc_pouch_state_payload_span *span, lc_error *error);
static int
lc_pouch_state_source_from_span(lc_pouch *pouch, const char *crypto_context,
                                const char *path, uint64_t payload_offset,
                                uint64_t payload_length, const char *descriptor,
                                lc_source **out, lc_error *error);

typedef struct lc_pouch_state_visit_snapshot {
  char *key;
  char *content_type;
  char *etag;
  char *descriptor;
  unsigned char *metadata;
  size_t metadata_length;
  lc_pouch_generation index_seq;
  lc_pouch_generation version;
  uint64_t bytes;
  uint64_t cipher_bytes;
  lc_pouch_unix_seconds updated_at_unix;
  int has_query_hidden;
  int query_hidden;
  int found;
  unsigned char record_type;
} lc_pouch_state_visit_snapshot;

typedef struct lc_pouch_state_read_many_snapshot {
  char *key;
  char *content_type;
  char *etag;
  char *payload_span_path;
  lc_pouch_state_payload_span payload_span;
  char *descriptor;
  unsigned char *metadata;
  size_t metadata_length;
  char *crypto_context;
  lc_source *body;
  lc_pouch_generation index_seq;
  lc_pouch_generation version;
  uint64_t bytes;
  uint64_t cipher_bytes;
  lc_pouch_unix_seconds updated_at_unix;
  int has_query_hidden;
  int query_hidden;
  int found;
  unsigned char record_type;
} lc_pouch_state_read_many_snapshot;

typedef struct lc_pouch_state_scan_body_snapshot {
  char *key;
  char *content_type;
  char *etag;
  char *payload_span_path;
  lc_pouch_state_payload_span payload_span;
  char *payload_context;
  char *descriptor;
  unsigned char *metadata;
  size_t metadata_length;
  lc_pouch_state_body_cache_entry *body_cache;
  lc_pouch_generation version;
  uint64_t bytes;
  uint64_t cipher_bytes;
  lc_pouch_unix_seconds updated_at_unix;
  int has_query_hidden;
  int query_hidden;
  int found;
  unsigned char record_type;
} lc_pouch_state_scan_body_snapshot;

typedef struct lc_pouch_state_hash_source {
  lc_source pub;
  lc_source *inner;
  EVP_MD_CTX *ctx;
  int failed;
} lc_pouch_state_hash_source;

typedef struct lc_pouch_state_body_cache_source {
  lc_source pub;
  lc_allocator allocator;
  lc_pouch_state_body_cache_entry *entry;
  size_t offset;
} lc_pouch_state_body_cache_source;

static int lc_pouch_state_read_text_file(lc_pouch *pouch, const char *path,
                                         char **out, size_t *out_length,
                                         lc_error *error);
static char *lc_pouch_state_child_path(const lc_allocator *allocator,
                                       const char *namespace_path,
                                       const char *child_dir, const char *leaf);
static void
lc_pouch_state_body_cache_entry_release(const lc_allocator *allocator,
                                        lc_pouch_state_body_cache_entry *entry);
static int lc_pouch_state_recover_staged_decisions_locked(
    lc_pouch *pouch, const char *namespace_name, lc_error *error);
static int
lc_pouch_state_delete_locked(lc_pouch *pouch, const char *namespace_name,
                             const char *key,
                             const lc_pouch_state_write_options *options,
                             lc_pouch_state_write_result *out, lc_error *error);
static int lc_pouch_state_lock_namespace_for_mutation(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_state_namespace_lock *lock, lc_error *error);
static int lc_pouch_state_visit_internal(lc_pouch *pouch,
                                         const char *namespace_name,
                                         lc_pouch_state_visit_fn visitor,
                                         void *context, int locked,
                                         lc_error *error);

static size_t lc_pouch_state_hash_source_read(lc_source *self, void *buffer,
                                              size_t count, lc_error *error) {
  lc_pouch_state_hash_source *source;
  size_t got;

  source = (lc_pouch_state_hash_source *)self->impl;
  got = source->inner->read(source->inner, buffer, count, error);
  if (got > 0U &&
      EVP_DigestUpdate(source->ctx, (const unsigned char *)buffer, got) != 1) {
    source->failed = 1;
    (void)lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                       "failed to hash pouch state payload", NULL, NULL,
                       "pouch");
    return 0U;
  }
  return got;
}

static int lc_pouch_state_hash_source_reset(lc_source *self, lc_error *error) {
  lc_pouch_state_hash_source *source;
  int rc;

  source = (lc_pouch_state_hash_source *)self->impl;
  if (source->inner->reset == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state hash source cannot reset inner source",
                        NULL, NULL, "pouch");
  }
  rc = source->inner->reset(source->inner, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (EVP_DigestInit_ex(source->ctx, EVP_sha256(), NULL) != 1) {
    source->failed = 1;
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to reset pouch state payload hash", NULL, NULL,
                        "pouch");
  }
  source->failed = 0;
  return LC_OK;
}

static void lc_pouch_state_hash_source_close(lc_source *self) { (void)self; }

static size_t lc_pouch_state_body_cache_source_read(lc_source *self,
                                                    void *buffer, size_t count,
                                                    lc_error *error) {
  lc_pouch_state_body_cache_source *source;
  size_t available;
  size_t take;

  (void)error;
  if (self == NULL || buffer == NULL || count == 0U) {
    return 0U;
  }
  source = (lc_pouch_state_body_cache_source *)self->impl;
  if (source == NULL || source->entry == NULL ||
      source->offset >= source->entry->length) {
    return 0U;
  }
  available = source->entry->length - source->offset;
  take = count < available ? count : available;
  memcpy(buffer, source->entry->bytes + source->offset, take);
  source->offset += take;
  return take;
}

static int lc_pouch_state_body_cache_source_reset(lc_source *self,
                                                  lc_error *error) {
  lc_pouch_state_body_cache_source *source;

  (void)error;
  if (self == NULL) {
    return LC_ERR_INVALID;
  }
  source = (lc_pouch_state_body_cache_source *)self->impl;
  if (source == NULL) {
    return LC_ERR_INVALID;
  }
  source->offset = 0U;
  return LC_OK;
}

static void lc_pouch_state_body_cache_source_close(lc_source *self) {
  lc_pouch_state_body_cache_source *source;

  if (self == NULL) {
    return;
  }
  source = (lc_pouch_state_body_cache_source *)self->impl;
  if (source == NULL) {
    return;
  }
  if (source->entry != NULL && source->entry->refcount > 0UL) {
    source->entry->refcount -= 1UL;
    lc_pouch_state_body_cache_entry_release(&source->allocator, source->entry);
  }
  lc_free_with_allocator(&source->allocator, source);
}

static int
lc_pouch_state_body_cache_source_open(const lc_allocator *allocator,
                                      lc_pouch_state_body_cache_entry *entry,
                                      lc_source **out, lc_error *error) {
  lc_pouch_state_body_cache_source *source;

  if (entry == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch body cache source requires entry and output",
                        NULL, NULL, "pouch");
  }
  *out = NULL;
  source = (lc_pouch_state_body_cache_source *)lc_calloc_with_allocator(
      allocator, 1U, sizeof(*source));
  if (source == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch body cache source", NULL,
                        NULL, "pouch");
  }
  if (allocator != NULL) {
    source->allocator = *allocator;
  } else {
    lc_allocator_init(&source->allocator);
  }
  entry->refcount += 1UL;
  source->entry = entry;
  source->pub.read = lc_pouch_state_body_cache_source_read;
  source->pub.reset = lc_pouch_state_body_cache_source_reset;
  source->pub.close = lc_pouch_state_body_cache_source_close;
  source->pub.impl = source;
  *out = &source->pub;
  return LC_OK;
}

static int lc_pouch_state_hash_source_init(lc_pouch_state_hash_source *source,
                                           lc_source *inner, EVP_MD_CTX *ctx,
                                           lc_error *error) {
  if (source == NULL || inner == NULL || ctx == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch state hash source requires source, inner and ctx", NULL, NULL,
        "pouch");
  }
  memset(source, 0, sizeof(*source));
  source->pub.read = lc_pouch_state_hash_source_read;
  source->pub.reset = lc_pouch_state_hash_source_reset;
  source->pub.close = lc_pouch_state_hash_source_close;
  source->pub.impl = source;
  source->inner = inner;
  source->ctx = ctx;
  if (EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) != 1) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch state payload hash", NULL,
                        NULL, "pouch");
  }
  return LC_OK;
}

static char *lc_pouch_state_hash_final(const lc_allocator *allocator,
                                       EVP_MD_CTX *ctx, lc_error *error) {
  static const char hex[] = "0123456789abcdef";
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int digest_len;
  char *etag;
  unsigned int i;

  digest_len = 0U;
  etag = NULL;
  if (EVP_DigestFinal_ex(ctx, digest, &digest_len) != 1) {
    (void)lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                       "failed to finalize pouch state payload hash", NULL,
                       NULL, "pouch");
    return NULL;
  }
  if (digest_len != 32U) {
    OPENSSL_cleanse(digest, sizeof(digest));
    (void)lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                       "pouch state payload hash length is invalid", NULL, NULL,
                       "pouch");
    return NULL;
  }
  etag = (char *)lc_alloc_with_allocator(allocator,
                                         ((size_t)digest_len * 2U) + 1U);
  if (etag == NULL) {
    OPENSSL_cleanse(digest, sizeof(digest));
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch state payload etag", NULL,
                       NULL, "pouch");
    return NULL;
  }
  for (i = 0U; i < digest_len; ++i) {
    etag[i * 2U] = hex[(digest[i] >> 4) & 0x0FU];
    etag[(i * 2U) + 1U] = hex[digest[i] & 0x0FU];
  }
  etag[digest_len * 2U] = '\0';
  OPENSSL_cleanse(digest, sizeof(digest));
  return etag;
}

static char *lc_pouch_state_hash_bytes(const lc_allocator *allocator,
                                       const unsigned char *bytes,
                                       size_t length, lc_error *error) {
  EVP_MD_CTX *ctx;
  char *etag;

  if (length > 0U && bytes == NULL) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch inline payload hash requires bytes", NULL, NULL,
                       "pouch");
    return NULL;
  }
  ctx = EVP_MD_CTX_new();
  if (ctx == NULL) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch state payload hash context",
                       NULL, NULL, "pouch");
    return NULL;
  }
  etag = NULL;
  if (EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) != 1 ||
      (length > 0U && EVP_DigestUpdate(ctx, bytes, length) != 1)) {
    (void)lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                       "failed to hash pouch state payload", NULL, NULL,
                       "pouch");
  } else {
    etag = lc_pouch_state_hash_final(allocator, ctx, error);
  }
  EVP_MD_CTX_free(ctx);
  return etag;
}

static char *lc_pouch_state_empty_etag(const lc_allocator *allocator,
                                       lc_error *error) {
  EVP_MD_CTX *ctx;
  char *etag;

  ctx = EVP_MD_CTX_new();
  if (ctx == NULL) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch state payload hash context",
                       NULL, NULL, "pouch");
    return NULL;
  }
  if (EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) != 1) {
    EVP_MD_CTX_free(ctx);
    (void)lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                       "failed to initialize pouch state empty hash", NULL,
                       NULL, "pouch");
    return NULL;
  }
  etag = lc_pouch_state_hash_final(allocator, ctx, error);
  EVP_MD_CTX_free(ctx);
  return etag;
}

static int lc_pouch_state_namespace_lock_acquire(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_state_namespace_lock *lock, lc_error *error) {
  char *namespace_path;
  char *lock_path;
  struct flock fl;
  int fd;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      lock == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch mutation lock requires pouch and namespace",
                        NULL, NULL, NULL);
  }
  lock->fd = -1;
  lock->process_mutex = NULL;
  lock->maintenance_guard = NULL;
  lock->exclusive_append_gate = NULL;
  lock->pouch = NULL;
  lock->namespace_name = NULL;
  lock->previous_namespace_lock = NULL;
  if (lc_pouch_state_process_namespace_guard_lock(
          pouch, namespace_name, 1, &lock->maintenance_guard, error) != LC_OK) {
    return error != NULL && error->code != LC_OK ? error->code
                                                 : LC_ERR_TRANSPORT;
  }
  /* Namespace callbacks retain projection authority across multiple writes.
   * Claim the physical gate before that projection so a metadata worker cannot
   * hold the gate while waiting for this callback's namespace mutex. */
  if (lc_pouch_single_writer_enabled(pouch)) {
    if (lc_pouch_state_exclusive_append_gate_lock(pouch, namespace_name,
                                                  &lock->exclusive_append_gate,
                                                  error) != LC_OK) {
      lc_pouch_state_process_namespace_guard_unlock(&lock->maintenance_guard);
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_TRANSPORT;
    }
  }
  if (lc_pouch_state_process_namespace_mutex_lock(
          pouch, namespace_name, &lock->process_mutex, error) != LC_OK) {
    lc_pouch_state_exclusive_append_gate_unlock(&lock->exclusive_append_gate);
    lc_pouch_state_process_namespace_guard_unlock(&lock->maintenance_guard);
    return error != NULL && error->code != LC_OK ? error->code
                                                 : LC_ERR_TRANSPORT;
  }
  if (lc_pouch_single_writer_enabled(pouch)) {
    if (lc_pouch_state_namespace_lock_track(lock, pouch, namespace_name,
                                            error) != LC_OK) {
      lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
      lc_pouch_state_exclusive_append_gate_unlock(&lock->exclusive_append_gate);
      lc_pouch_state_process_namespace_guard_unlock(&lock->maintenance_guard);
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_TRANSPORT;
    }
    return LC_OK;
  }

  namespace_path = lc_pouch_namespace_path(&pouch->allocator, pouch->root_path,
                                           namespace_name);
  lock_path =
      namespace_path != NULL
          ? lc_pouch_path_join(&pouch->allocator, namespace_path, "write.lock")
          : NULL;
  lc_free_with_allocator(&pouch->allocator, namespace_path);
  if (lock_path == NULL) {
    lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
    lc_pouch_state_exclusive_append_gate_unlock(&lock->exclusive_append_gate);
    lc_pouch_state_process_namespace_guard_unlock(&lock->maintenance_guard);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch mutation lock path", NULL,
                        NULL, NULL);
  }
  fd = open(lock_path, O_CREAT | O_RDWR, 0666);
  lc_free_with_allocator(&pouch->allocator, lock_path);
  if (fd < 0) {
    lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
    lc_pouch_state_exclusive_append_gate_unlock(&lock->exclusive_append_gate);
    lc_pouch_state_process_namespace_guard_unlock(&lock->maintenance_guard);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch mutation lock", strerror(errno),
                        NULL, NULL);
  }

  memset(&fl, 0, sizeof(fl));
  fl.l_type = F_WRLCK;
  fl.l_whence = SEEK_SET;
  while (fcntl(fd, F_SETLKW, &fl) != 0) {
    if (errno == EINTR) {
      continue;
    }
    close(fd);
    lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
    lc_pouch_state_exclusive_append_gate_unlock(&lock->exclusive_append_gate);
    lc_pouch_state_process_namespace_guard_unlock(&lock->maintenance_guard);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch mutation file", strerror(errno),
                        NULL, NULL);
  }
  lock->fd = fd;
  if (lc_pouch_state_namespace_lock_track(lock, pouch, namespace_name, error) !=
      LC_OK) {
    lc_pouch_state_namespace_lock_release(lock);
    return error != NULL && error->code != LC_OK ? error->code
                                                 : LC_ERR_TRANSPORT;
  }
  return LC_OK;
}

static void
lc_pouch_state_namespace_lock_release(lc_pouch_state_namespace_lock *lock) {
  if (lock == NULL) {
    return;
  }
  lc_pouch_state_namespace_lock_untrack(lock);
  if (lock->fd >= 0) {
    struct flock fl;

    memset(&fl, 0, sizeof(fl));
    fl.l_type = F_UNLCK;
    fl.l_whence = SEEK_SET;
    (void)fcntl(lock->fd, F_SETLK, &fl);
    close(lock->fd);
    lock->fd = -1;
  }
  lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
  lc_pouch_state_exclusive_append_gate_unlock(&lock->exclusive_append_gate);
  lc_pouch_state_process_namespace_guard_unlock(&lock->maintenance_guard);
}

/* Every exclusive Pouch owns stable per-namespace physical append gates for
 * its full lifetime. Shared roots add the process-local and cross-process
 * gates below it. This gives the exclusive path the same split as Go disk:
 * callers may prepare a value concurrently, but only one finalized byte range
 * is appended at a time, without filesystem discovery on a healthy mutation.
 */
static int lc_pouch_state_append_lock_acquire(lc_pouch *pouch,
                                              const char *namespace_name,
                                              lc_pouch_state_append_lock *lock,
                                              lc_error *error) {
  char *namespace_path;
  char *lock_path;
  struct flock fl;
  int fd;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      lock == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch append lock requires namespace", NULL, NULL,
                        "pouch");
  }
  lock->fd = -1;
  lock->process_mutex = NULL;
  lock->exclusive_gate = NULL;
  if (lc_pouch_single_writer_enabled(pouch)) {
    return lc_pouch_state_exclusive_append_gate_lock(
        pouch, namespace_name, &lock->exclusive_gate, error);
  }
  rc = lc_pouch_state_process_namespace_mutex_lock(pouch, namespace_name,
                                                   &lock->process_mutex, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (lc_pouch_state_namespace_lock_is_held(pouch, namespace_name)) {
    return LC_OK;
  }
  namespace_path = lc_pouch_namespace_path(&pouch->allocator, pouch->root_path,
                                           namespace_name);
  lock_path =
      namespace_path != NULL
          ? lc_pouch_path_join(&pouch->allocator, namespace_path, "append.lock")
          : NULL;
  lc_free_with_allocator(&pouch->allocator, namespace_path);
  if (lock_path == NULL) {
    lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch append lock path", NULL, NULL,
                        "pouch");
  }
  fd = open(lock_path, O_CREAT | O_RDWR, 0666);
  lc_free_with_allocator(&pouch->allocator, lock_path);
  if (fd < 0) {
    lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch append lock", strerror(errno),
                        NULL, "pouch");
  }
  memset(&fl, 0, sizeof(fl));
  fl.l_type = F_WRLCK;
  fl.l_whence = SEEK_SET;
  while (fcntl(fd, F_SETLKW, &fl) != 0) {
    if (errno == EINTR) {
      continue;
    }
    close(fd);
    lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch append file", strerror(errno),
                        NULL, "pouch");
  }
  lock->fd = fd;
  return LC_OK;
}

static void
lc_pouch_state_append_lock_release(lc_pouch_state_append_lock *lock) {
  if (lock == NULL) {
    return;
  }
  if (lock->fd >= 0) {
    struct flock fl;

    memset(&fl, 0, sizeof(fl));
    fl.l_type = F_UNLCK;
    fl.l_whence = SEEK_SET;
    (void)fcntl(lock->fd, F_SETLK, &fl);
    (void)close(lock->fd);
    lock->fd = -1;
  }
  lc_pouch_state_exclusive_append_gate_unlock(&lock->exclusive_gate);
  lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
}

/* Normal exact-key mutations arrive with the projection mutex held. Take the
 * physical appender first, then restore the projection mutex before reading or
 * publishing cache state. This ordering lets a streaming owner drop only the
 * projection mutex while retaining its exact-key and append ownership; other
 * writers prepare or wait at the append gate without deadlocking publication.
 * Namespace callbacks already own stronger namespace authority and never own
 * the projection mutex through this helper. */
static int lc_pouch_state_append_lock_enter_after_mutation(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_state_append_lock *lock, lc_error *error) {
  int namespace_locked;
  int rc;
  int restore_rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      lock == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch append transition requires namespace", NULL,
                        NULL, "pouch");
  }
  namespace_locked =
      lc_pouch_state_namespace_lock_is_held(pouch, namespace_name);
  if (!namespace_locked) {
    lc_pouch_state_projection_mutex_unlock(pouch, namespace_name);
  }
  rc = lc_pouch_state_append_lock_acquire(pouch, namespace_name, lock, error);
  if (!namespace_locked) {
    restore_rc =
        lc_pouch_state_projection_mutex_lock(pouch, namespace_name, error);
    if (restore_rc != LC_OK && rc == LC_OK) {
      lc_pouch_state_append_lock_release(lock);
      return restore_rc;
    }
  }
  return rc;
}

static int lc_pouch_state_key_lock_acquire(lc_pouch *pouch,
                                           const char *namespace_name,
                                           const char *key,
                                           lc_pouch_state_key_lock *lock,
                                           lc_error *error) {
  char *identity;
  char *namespace_path;
  char *locks_path;
  char *lock_path;
  char *maintenance_lock_path;
  uint64_t hash;
  const unsigned char *cursor;
  const char *canonical_key;
  struct flock fl;
  size_t namespace_len;
  size_t key_len;
  int fd;
  int maintenance_fd;
  int rc;
  int pthread_rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || lock == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch key lock requires namespace and key", NULL, NULL,
                        "pouch");
  }
  lock->fd = -1;
  lock->maintenance_fd = -1;
  lock->process_mutex = NULL;
  lock->maintenance_guard = NULL;
  lock->exclusive_key_mutex = NULL;
  lock->pouch = pouch;
  lock->namespace_name = namespace_name;
  lock->projection_uses_namespace_mutex = 0;
  lock->previous_projection_lock = NULL;
  if (lc_pouch_state_namespace_lock_is_held(pouch, namespace_name)) {
    lock->projection_uses_namespace_mutex =
        lc_pouch_single_writer_enabled(pouch);
    return LC_OK;
  }
  canonical_key = strstr(key, "/.staging/");
  key_len = canonical_key != NULL ? (size_t)(canonical_key - key) : strlen(key);
  if (key_len == 0U) {
    canonical_key = key;
    key_len = strlen(key);
  } else {
    canonical_key = key;
  }
  if (lc_pouch_single_writer_enabled(pouch)) {
    hash = ((uint64_t)0xcbf29ce4UL << 32U) | (uint64_t)0x84222325UL;
    for (cursor = (const unsigned char *)namespace_name; *cursor != '\0';
         ++cursor) {
      hash ^= (uint64_t)*cursor;
      hash *= ((uint64_t)0x00000100UL << 32U) | (uint64_t)0x000001b3UL;
    }
    hash ^= (uint64_t)'\n';
    hash *= ((uint64_t)0x00000100UL << 32U) | (uint64_t)0x000001b3UL;
    for (cursor = (const unsigned char *)canonical_key;
         (size_t)(cursor - (const unsigned char *)canonical_key) < key_len;
         ++cursor) {
      hash ^= (uint64_t)*cursor;
      hash *= ((uint64_t)0x00000100UL << 32U) | (uint64_t)0x000001b3UL;
    }
    if (pouch->exclusive_key_mutex_count == 0U) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch exclusive key mutexes are unavailable", NULL,
                          NULL, "pouch");
    }
    lock->exclusive_key_mutex = &pouch->exclusive_key_mutexes[(
        size_t)(hash % pouch->exclusive_key_mutex_count)];
    pthread_rc = pthread_mutex_lock(lock->exclusive_key_mutex);
    if (pthread_rc != 0) {
      lock->exclusive_key_mutex = NULL;
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to lock pouch exclusive key mutex",
                          strerror(pthread_rc), NULL, "pouch");
    }
    /*
     * Maintenance takes its write guard before namespace projection.  Keep
     * regular mutations in that order as well, otherwise recovery can hold
     * the write guard while waiting for a projection mutex held by a writer
     * waiting for the read guard.
     */
    rc = lc_pouch_state_process_namespace_guard_lock(
        pouch, namespace_name, 0, &lock->maintenance_guard, error);
    if (rc != LC_OK) {
      (void)pthread_mutex_unlock(lock->exclusive_key_mutex);
      lock->exclusive_key_mutex = NULL;
      return rc;
    }
    rc = lc_pouch_state_process_namespace_mutex_lock(
        pouch, namespace_name, &lock->process_mutex, error);
    if (rc != LC_OK) {
      lc_pouch_state_process_namespace_guard_unlock(&lock->maintenance_guard);
      (void)pthread_mutex_unlock(lock->exclusive_key_mutex);
      lock->exclusive_key_mutex = NULL;
    }
    if (rc == LC_OK) {
      lock->projection_uses_namespace_mutex = 1;
    }
    return rc;
  }
  namespace_len = strlen(namespace_name);
  if (namespace_len > (size_t)-1 - key_len - 2U) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "pouch key lock identity exceeds local limit", NULL,
                        NULL, "pouch");
  }
  identity =
      (char *)lc_alloc_with_allocator(NULL, namespace_len + key_len + 2U);
  if (identity == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch key lock identity", NULL,
                        NULL, "pouch");
  }
  memcpy(identity, namespace_name, namespace_len);
  identity[namespace_len] = '\n';
  memcpy(identity + namespace_len + 1U, canonical_key, key_len);
  identity[namespace_len + key_len + 1U] = '\0';
  rc = lc_pouch_state_process_namespace_mutex_lock(pouch, identity,
                                                   &lock->process_mutex, error);
  lc_free_with_allocator(NULL, identity);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_process_namespace_guard_lock(
      pouch, namespace_name, 0, &lock->maintenance_guard, error);
  if (rc != LC_OK) {
    lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
    return rc;
  }
  namespace_path = lc_pouch_namespace_path(&pouch->allocator, pouch->root_path,
                                           namespace_name);
  locks_path =
      namespace_path != NULL
          ? lc_pouch_path_join(&pouch->allocator, namespace_path, "locks")
          : NULL;
  maintenance_lock_path =
      namespace_path != NULL
          ? lc_pouch_path_join(&pouch->allocator, namespace_path, "write.lock")
          : NULL;
  if (namespace_path == NULL || locks_path == NULL ||
      maintenance_lock_path == NULL) {
    lc_free_with_allocator(&pouch->allocator, namespace_path);
    lc_free_with_allocator(&pouch->allocator, locks_path);
    lc_free_with_allocator(&pouch->allocator, maintenance_lock_path);
    lc_pouch_state_process_namespace_guard_unlock(&lock->maintenance_guard);
    lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch key lock path", NULL, NULL,
                        "pouch");
  }
  rc = lc_pouch_path_ensure_directory(
      locks_path, "failed to create pouch key lock directory", error);
  maintenance_fd = -1;
  if (rc == LC_OK) {
    maintenance_fd = open(maintenance_lock_path, O_CREAT | O_RDWR, 0666);
    if (maintenance_fd < 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch maintenance lock",
                        strerror(errno), NULL, "pouch");
    }
  }
  if (rc == LC_OK) {
    memset(&fl, 0, sizeof(fl));
    fl.l_type = F_RDLCK;
    fl.l_whence = SEEK_SET;
    while (fcntl(maintenance_fd, F_SETLKW, &fl) != 0) {
      if (errno == EINTR) {
        continue;
      }
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch maintenance file",
                        strerror(errno), NULL, "pouch");
      break;
    }
  }
  hash = ((uint64_t)0xcbf29ce4UL << 32U) | (uint64_t)0x84222325UL;
  for (cursor = (const unsigned char *)canonical_key;
       (size_t)(cursor - (const unsigned char *)canonical_key) < key_len;
       ++cursor) {
    hash ^= (uint64_t)*cursor;
    hash *= ((uint64_t)0x00000100UL << 32U) | (uint64_t)0x000001b3UL;
  }
  lock_path = NULL;
  if (rc == LC_OK) {
    char leaf[32];

    if (lc_u64_format_base16_padded((lc_u64)hash, 16U, leaf, sizeof(leaf)) <
        0) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "failed to format pouch key lock path", NULL, NULL,
                        "pouch");
    } else {
      memcpy(leaf + 16U, ".lock", sizeof(".lock"));
      lock_path = lc_pouch_path_join(&pouch->allocator, locks_path, leaf);
    }
    if (rc == LC_OK && lock_path == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch key lock path", NULL, NULL,
                        "pouch");
    }
  }
  lc_free_with_allocator(&pouch->allocator, namespace_path);
  lc_free_with_allocator(&pouch->allocator, locks_path);
  lc_free_with_allocator(&pouch->allocator, maintenance_lock_path);
  if (rc != LC_OK) {
    if (maintenance_fd >= 0) {
      close(maintenance_fd);
    }
    lc_free_with_allocator(&pouch->allocator, lock_path);
    lc_pouch_state_process_namespace_guard_unlock(&lock->maintenance_guard);
    lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
    return rc;
  }
  fd = open(lock_path, O_CREAT | O_RDWR, 0666);
  lc_free_with_allocator(&pouch->allocator, lock_path);
  if (fd < 0) {
    close(maintenance_fd);
    lc_pouch_state_process_namespace_guard_unlock(&lock->maintenance_guard);
    lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch key lock", strerror(errno), NULL,
                        "pouch");
  }
  memset(&fl, 0, sizeof(fl));
  fl.l_type = F_WRLCK;
  fl.l_whence = SEEK_SET;
  while (fcntl(fd, F_SETLKW, &fl) != 0) {
    if (errno == EINTR) {
      continue;
    }
    close(fd);
    close(maintenance_fd);
    lc_pouch_state_process_namespace_guard_unlock(&lock->maintenance_guard);
    lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L, "failed to lock pouch key",
                        strerror(errno), NULL, "pouch");
  }
  lock->fd = fd;
  lock->maintenance_fd = maintenance_fd;
  return LC_OK;
}

static void lc_pouch_state_key_lock_release(lc_pouch_state_key_lock *lock) {
  if (lock == NULL) {
    return;
  }
  if (lock->fd >= 0) {
    struct flock fl;

    memset(&fl, 0, sizeof(fl));
    fl.l_type = F_UNLCK;
    fl.l_whence = SEEK_SET;
    (void)fcntl(lock->fd, F_SETLK, &fl);
    (void)close(lock->fd);
    lock->fd = -1;
  }
  if (lock->maintenance_fd >= 0) {
    struct flock fl;

    memset(&fl, 0, sizeof(fl));
    fl.l_type = F_UNLCK;
    fl.l_whence = SEEK_SET;
    (void)fcntl(lock->maintenance_fd, F_SETLK, &fl);
    (void)close(lock->maintenance_fd);
    lock->maintenance_fd = -1;
  }
  lc_pouch_state_process_namespace_guard_unlock(&lock->maintenance_guard);
  lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
  if (lock->exclusive_key_mutex != NULL) {
    (void)pthread_mutex_unlock(lock->exclusive_key_mutex);
    lock->exclusive_key_mutex = NULL;
  }
  lock->pouch = NULL;
  lock->namespace_name = NULL;
  lock->projection_uses_namespace_mutex = 0;
}

static int lc_pouch_state_key_mutation_begin(lc_pouch *pouch,
                                             const char *namespace_name,
                                             const char *key,
                                             lc_pouch_state_key_lock *lock,
                                             lc_error *error) {
  int pthread_rc;
  int rc;

  if (pouch == NULL || !pouch->state_mutation_mutex_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch mutation requires an open pouch", NULL, NULL,
                        "pouch");
  }
  /* Recovery has its own mutation authority. Complete it first so this
   * operation can pin one writer mode through append and durable completion. */
  rc = lc_pouch_state_namespace_lock_is_held(pouch, namespace_name)
           ? LC_OK
           : lc_pouch_state_recover_staged_decisions(pouch, namespace_name,
                                                     error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_writer_mode_operation_begin(pouch, error);
  if (rc != LC_OK) {
    return rc;
  }
  /* Exact-key ownership is independent of the resident projection. Take it
   * first so a conflicting key/file lock never occupies the short projection
   * mutex; this also gives maintenance a clear key/namespace barrier. */
  rc = lc_pouch_state_key_lock_acquire(pouch, namespace_name, key, lock, error);
  if (rc != LC_OK) {
    lc_pouch_writer_mode_operation_end(pouch);
    return rc;
  }
  rc = lc_pouch_state_projection_lock_track(lock, error);
  if (rc != LC_OK) {
    lc_pouch_state_key_lock_release(lock);
    lc_pouch_writer_mode_operation_end(pouch);
    return rc;
  }
  if (lock->projection_uses_namespace_mutex) {
    return LC_OK;
  }
  pthread_rc = pthread_mutex_lock(&pouch->state_mutation_mutex);
  if (pthread_rc != 0) {
    lc_pouch_state_projection_lock_untrack(lock);
    lc_pouch_state_key_lock_release(lock);
    lc_pouch_writer_mode_operation_end(pouch);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch mutation state",
                        strerror(pthread_rc), NULL, "pouch");
  }
  return LC_OK;
}

static void lc_pouch_state_key_mutation_end(lc_pouch *pouch,
                                            lc_pouch_state_key_lock *lock) {
  if (pouch != NULL && lock != NULL && !lock->projection_uses_namespace_mutex &&
      pouch->state_mutation_mutex_initialized) {
    pthread_mutex_unlock(&pouch->state_mutation_mutex);
  }
  lc_pouch_state_projection_lock_untrack(lock);
  lc_pouch_state_key_lock_release(lock);
  lc_pouch_writer_mode_operation_end(pouch);
}

/* Keep the per-key lock through durable completion, but let independent
 * namespace projections proceed while this request waits for its root-scoped
 * fsync group. Shared mode retains its global projection mutex. */
static int lc_pouch_state_finish_commit_group_after_mutation(
    lc_pouch *pouch, lc_pouch_state_key_lock *lock,
    lc_pouch_state_commit_group *group, int owned, int rc, lc_error *error) {
  int projection_released;

  projection_released = 0;
  if (pouch != NULL && lock != NULL && lock->projection_uses_namespace_mutex &&
      lock->process_mutex != NULL) {
    lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
    projection_released = 1;
  } else if (pouch != NULL && lock != NULL &&
             !lock->projection_uses_namespace_mutex &&
             pouch->state_mutation_mutex_initialized) {
    pthread_mutex_unlock(&pouch->state_mutation_mutex);
  }
  rc = lc_pouch_state_finish_commit_group_common(group, owned, rc, error);
  if (rc != LC_OK && pouch != NULL && lock != NULL) {
    if (lock->projection_uses_namespace_mutex) {
      if (projection_released) {
        if (lc_pouch_state_process_namespace_mutex_lock(
                pouch, lock->namespace_name, &lock->process_mutex, NULL) !=
            LC_OK) {
          lc_pouch_state_projection_lock_untrack(lock);
          lc_pouch_state_key_lock_release(lock);
          lc_pouch_writer_mode_operation_end(pouch);
          return rc;
        }
      }
      lc_pouch_state_cache_invalidate_namespace(pouch, lock->namespace_name);
    } else if (pouch->state_mutation_mutex_initialized &&
               pthread_mutex_lock(&pouch->state_mutation_mutex) == 0) {
      lc_pouch_state_cache_invalidate_namespace(pouch, lock->namespace_name);
      pthread_mutex_unlock(&pouch->state_mutation_mutex);
    }
  }
  lc_pouch_state_projection_lock_untrack(lock);
  lc_pouch_state_key_lock_release(lock);
  lc_pouch_writer_mode_operation_end(pouch);
  return rc;
}

int lc_pouch_state_with_namespace_lock(lc_pouch *pouch,
                                       const char *namespace_name,
                                       lc_pouch_state_precondition_fn callback,
                                       void *context, lc_error *error) {
  lc_pouch_state_commit_group *commit_group;
  lc_pouch_state_namespace_lock lock;
  int owns_commit_group;
  int rc;

  commit_group = NULL;
  owns_commit_group = 0;
  if (callback == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace lock requires callback", NULL, NULL,
                        NULL);
  }
  rc = lc_pouch_writer_mode_operation_begin(pouch, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_lock_namespace_for_mutation(pouch, namespace_name, &lock,
                                                  error);
  if (rc != LC_OK) {
    lc_pouch_writer_mode_operation_end(pouch);
    return rc;
  }
  rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                         &owns_commit_group, error);
  if (rc != LC_OK) {
    lc_pouch_state_namespace_lock_release(&lock);
    lc_pouch_writer_mode_operation_end(pouch);
    return rc;
  }
  rc = callback(context, error);
  rc = lc_pouch_state_finish_commit_group(commit_group, owns_commit_group, rc,
                                          error);
  if (rc != LC_OK) {
    lc_pouch_state_cache_invalidate_namespace(pouch, namespace_name);
  }
  lc_pouch_state_namespace_lock_release(&lock);
  lc_pouch_writer_mode_operation_end(pouch);
  return rc;
}

int lc_pouch_state_with_key_lock(lc_pouch *pouch, const char *namespace_name,
                                 const char *key,
                                 lc_pouch_state_precondition_fn callback,
                                 void *context, lc_error *error) {
  lc_pouch_state_commit_group *commit_group;
  lc_pouch_state_key_lock lock;
  int owns_commit_group;
  int rc;

  if (callback == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch key lock requires callback", NULL, NULL, NULL);
  }
  commit_group = NULL;
  owns_commit_group = 0;
  rc = lc_pouch_state_key_mutation_begin(pouch, namespace_name, key, &lock,
                                         error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                         &owns_commit_group, error);
  if (rc == LC_OK) {
    rc = callback(context, error);
    rc = lc_pouch_state_finish_commit_group_after_mutation(
        pouch, &lock, commit_group, owns_commit_group, rc, error);
  } else {
    lc_pouch_state_key_mutation_end(pouch, &lock);
  }
  if (rc == LC_OK) {
    lc_pouch_janitor_note_mutation(pouch);
  }
  return rc;
}

static int lc_pouch_state_lock_namespace_for_mutation(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_state_namespace_lock *lock, lc_error *error) {
  int rc;

  rc = lc_pouch_namespace_ensure_layout(&pouch->allocator, pouch->root_path,
                                        namespace_name, error);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_pouch_state_namespace_lock_acquire(pouch, namespace_name, lock,
                                               error);
}

static int lc_pouch_state_ensure_namespace_locked(lc_pouch *pouch,
                                                  const char *namespace_name,
                                                  lc_error *error) {
  int rc;

  rc = lc_pouch_namespace_ensure(&pouch->allocator, pouch->root_path,
                                 namespace_name, error);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_pouch_state_recover_staged_decisions_locked(pouch, namespace_name,
                                                        error);
}

static char *lc_pouch_state_crypto_context(const lc_allocator *allocator,
                                           const char *namespace_name,
                                           const char *key) {
  size_t ns_len;
  size_t key_len;
  char *context;

  if (namespace_name == NULL || key == NULL) {
    return NULL;
  }
  key_len = strlen(key);
  ns_len = strlen(namespace_name);
  context =
      (char *)lc_alloc_with_allocator(allocator, ns_len + 1U + key_len + 1U);
  if (context == NULL) {
    return NULL;
  }
  memcpy(context, namespace_name, ns_len);
  context[ns_len] = '/';
  memcpy(context + ns_len + 1U, key, key_len);
  context[ns_len + 1U + key_len] = '\0';
  return context;
}

static char *lc_pouch_state_payload_context_for_read(
    const lc_allocator *allocator, const char *namespace_name,
    const char *logical_key, const char *payload_context) {
  if (payload_context != NULL && payload_context[0] != '\0') {
    return lc_strdup_with_allocator(allocator, payload_context);
  }
  return lc_pouch_state_crypto_context(allocator, namespace_name, logical_key);
}

static void
lc_pouch_state_payload_span_cleanup(const lc_allocator *allocator,
                                    lc_pouch_state_payload_span *span) {
  if (span == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, span->container_leaf);
  memset(span, 0, sizeof(*span));
}

static int lc_pouch_state_payload_span_set(
    const lc_allocator *allocator, lc_pouch_state_payload_span *span,
    const char *container_leaf, uint64_t record_offset, uint64_t payload_offset,
    uint64_t payload_length, unsigned long payload_crc, lc_error *error) {
  char *container_copy;

  if (span == NULL ||
      !lc_pouch_state_payload_container_is_valid(container_leaf)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch payload span requires a valid container", NULL,
                        NULL, "pouch");
  }
  container_copy = lc_strdup_with_allocator(allocator, container_leaf);
  if (container_copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch payload span container", NULL,
                        NULL, NULL);
  }
  lc_pouch_state_payload_span_cleanup(allocator, span);
  span->container_leaf = container_copy;
  span->record_offset = record_offset;
  span->payload_offset = payload_offset;
  span->payload_length = payload_length;
  span->payload_crc = payload_crc;
  span->present = 1;
  return LC_OK;
}

static int lc_pouch_state_payload_span_copy(
    const lc_allocator *allocator, const lc_pouch_state_payload_span *src,
    lc_pouch_state_payload_span *dst, lc_error *error) {
  if (dst == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch payload span copy requires output", NULL, NULL,
                        "pouch");
  }
  memset(dst, 0, sizeof(*dst));
  if (src == NULL || !src->present) {
    return LC_OK;
  }
  return lc_pouch_state_payload_span_set(
      allocator, dst, src->container_leaf, src->record_offset,
      src->payload_offset, src->payload_length, src->payload_crc, error);
}

static void lc_pouch_state_record_ref_cleanup(const lc_allocator *allocator,
                                              char **container_leaf,
                                              uint64_t *record_offset,
                                              int *present) {
  if (container_leaf != NULL) {
    lc_free_with_allocator(allocator, *container_leaf);
    *container_leaf = NULL;
  }
  if (record_offset != NULL) {
    *record_offset = 0UL;
  }
  if (present != NULL) {
    *present = 0;
  }
}

static int lc_pouch_state_record_ref_set(const lc_allocator *allocator,
                                         char **container_leaf,
                                         uint64_t *record_offset, int *present,
                                         const char *leaf, uint64_t offset,
                                         lc_error *error) {
  char *copy;

  if (container_leaf == NULL || record_offset == NULL || present == NULL ||
      !lc_pouch_state_payload_container_is_valid(leaf)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch record ref requires a valid container", NULL,
                        NULL, "pouch");
  }
  copy = lc_strdup_with_allocator(allocator, leaf);
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch record ref container", NULL, NULL,
                        NULL);
  }
  lc_pouch_state_record_ref_cleanup(allocator, container_leaf, record_offset,
                                    present);
  *container_leaf = copy;
  *record_offset = offset;
  *present = 1;
  return LC_OK;
}

static int lc_pouch_state_record_ref_copy(
    const lc_allocator *allocator, const char *src_container_leaf,
    uint64_t src_record_offset, int src_present, char **dst_container_leaf,
    uint64_t *dst_record_offset, int *dst_present, lc_error *error) {
  if (dst_container_leaf == NULL || dst_record_offset == NULL ||
      dst_present == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch record ref copy requires outputs", NULL, NULL,
                        "pouch");
  }
  *dst_container_leaf = NULL;
  *dst_record_offset = 0UL;
  *dst_present = 0;
  if (!src_present) {
    return LC_OK;
  }
  return lc_pouch_state_record_ref_set(
      allocator, dst_container_leaf, dst_record_offset, dst_present,
      src_container_leaf, src_record_offset, error);
}

typedef struct lc_pouch_state_compaction_capture {
  char *manifest_text;
  char **candidate_leaves;
  int *candidate_snapshots;
  uint64_t *candidate_sizes;
  unsigned long *candidate_checksums;
  size_t candidate_count;
  size_t candidate_capacity;
  char **captured_keys;
  char **captured_record_containers;
  uint64_t *captured_record_offsets;
  size_t captured_count;
  size_t captured_capacity;
  uint64_t candidate_bytes;
} lc_pouch_state_compaction_capture;

struct lc_pouch_namespace_logstore {
  char *namespace_name;
  unsigned long namespace_hash;
  pthread_mutex_t exclusive_append_gate;
  int exclusive_append_gate_initialized;
  lc_pouch_state_metadata_append_batcher *metadata_append_batcher;
  char *namespace_path;
  char *active_segment_leaf;
  char *latest_snapshot_leaf;
  int active_append_fd;
  int active_append_fd_owned;
  uint64_t active_segment_offset;
  uint64_t active_segment_id;
  uint64_t max_segment_id;
  unsigned long segment_count;
  lc_pouch_generation max_version;
  uint64_t writer_mode_epoch;
  int initialized;
  int decision_recovery_checked;
  lc_pouch_state_cache_record *records;
  lc_pouch_state_cache_record **record_buckets;
  size_t record_bucket_count;
  size_t record_count;
  size_t body_cache_bytes;
  struct lc_pouch_namespace_logstore *hash_next;
  struct lc_pouch_namespace_logstore *next;
};

static unsigned long lc_pouch_state_namespace_hash(const char *namespace_name) {
  const unsigned char *cursor;
  unsigned long hash;

  hash = 2166136261UL;
  for (cursor = (const unsigned char *)namespace_name;
       cursor != NULL && *cursor != '\0'; ++cursor) {
    hash ^= (unsigned long)*cursor;
    hash *= 16777619UL;
  }
  return hash;
}

static void lc_pouch_state_entry_cleanup(const lc_allocator *allocator,
                                         lc_pouch_state_entry *entry) {
  if (entry == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, entry->key);
  lc_free_with_allocator(allocator, entry->content_type);
  lc_free_with_allocator(allocator, entry->etag);
  lc_pouch_state_payload_span_cleanup(allocator, &entry->payload_span);
  lc_pouch_state_record_ref_cleanup(allocator, &entry->record_container_leaf,
                                    &entry->record_offset,
                                    &entry->has_record_ref);
  lc_free_with_allocator(allocator, entry->payload_context);
  lc_free_with_allocator(allocator, entry->descriptor);
  lc_free_with_allocator(allocator, entry->metadata);
  lc_free_with_allocator(allocator, entry->decision);
  memset(entry, 0, sizeof(*entry));
}

/* The metadata append worker only needs fields that describe the existing
 * durable value. Copy these while the caller still holds mutation authority;
 * the worker then owns no borrowed projection pointers. */
static int lc_pouch_state_metadata_append_entry_copy(
    lc_pouch *pouch, const lc_pouch_state_entry *source,
    lc_pouch_state_entry *destination, lc_error *error) {
  int rc;

  if (pouch == NULL || source == NULL || destination == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata append entry copy requires inputs",
                        NULL, NULL, "pouch");
  }
  memset(destination, 0, sizeof(*destination));
  destination->found = source->found;
  destination->version = source->version;
  destination->bytes = source->bytes;
  destination->cipher_bytes = source->cipher_bytes;
  destination->has_query_hidden = source->has_query_hidden;
  destination->query_hidden = source->query_hidden;
  if (source->content_type != NULL) {
    destination->content_type =
        lc_strdup_with_allocator(&pouch->allocator, source->content_type);
  }
  if (source->etag != NULL) {
    destination->etag =
        lc_strdup_with_allocator(&pouch->allocator, source->etag);
  }
  if (source->payload_context != NULL) {
    destination->payload_context =
        lc_strdup_with_allocator(&pouch->allocator, source->payload_context);
  }
  if (source->descriptor != NULL) {
    destination->descriptor =
        lc_strdup_with_allocator(&pouch->allocator, source->descriptor);
  }
  if ((source->content_type != NULL && destination->content_type == NULL) ||
      (source->etag != NULL && destination->etag == NULL) ||
      (source->payload_context != NULL &&
       destination->payload_context == NULL) ||
      (source->descriptor != NULL && destination->descriptor == NULL)) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to copy pouch metadata append entry", NULL, NULL,
                      "pouch");
    goto cleanup;
  }
  rc =
      lc_pouch_state_payload_span_copy(&pouch->allocator, &source->payload_span,
                                       &destination->payload_span, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  if (source->metadata_length > 0U && source->metadata == NULL) {
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "pouch metadata append source is malformed", NULL, NULL,
                      "pouch");
    goto cleanup;
  }
  if (source->metadata_length > 0U) {
    destination->metadata = (unsigned char *)lc_alloc_with_allocator(
        &pouch->allocator, source->metadata_length);
    if (destination->metadata == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch metadata append blob", NULL, NULL,
                        "pouch");
      goto cleanup;
    }
    memcpy(destination->metadata, source->metadata, source->metadata_length);
    destination->metadata_length = source->metadata_length;
  }
  return LC_OK;

cleanup:
  lc_pouch_state_entry_cleanup(&pouch->allocator, destination);
  return rc;
}

static void lc_pouch_state_decision_cleanup(const lc_allocator *allocator,
                                            lc_pouch_state_decision *decision) {
  if (decision == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, decision->staged_key);
  lc_free_with_allocator(allocator, decision->etag);
  lc_free_with_allocator(allocator, decision->decision);
  lc_free_with_allocator(allocator, decision);
}

static void
lc_pouch_state_decisions_cleanup(const lc_allocator *allocator,
                                 lc_pouch_state_decision *decision) {
  while (decision != NULL) {
    lc_pouch_state_decision *next;

    next = decision->next;
    lc_pouch_state_decision_cleanup(allocator, decision);
    decision = next;
  }
}

static int lc_pouch_state_string_equal(const char *left, const char *right) {
  if (left == NULL || right == NULL) {
    return left == right;
  }
  return strcmp(left, right) == 0;
}

static void lc_pouch_state_body_cache_entry_release(
    const lc_allocator *allocator, lc_pouch_state_body_cache_entry *entry) {
  if (entry == NULL || entry->refcount > 0UL || !entry->retired) {
    return;
  }
  if (entry->bytes != NULL) {
    OPENSSL_cleanse(entry->bytes, entry->length);
    lc_free_with_allocator(allocator, entry->bytes);
  }
  lc_free_with_allocator(allocator, entry);
}

static void
lc_pouch_state_cache_record_body_clear(const lc_allocator *allocator,
                                       lc_pouch_namespace_logstore *ns,
                                       lc_pouch_state_cache_record *record) {
  lc_pouch_state_body_cache_entry *entry;

  if (record == NULL || record->body_cache == NULL) {
    return;
  }
  entry = record->body_cache;
  record->body_cache = NULL;
  if (ns != NULL) {
    if (ns->body_cache_bytes >= entry->length) {
      ns->body_cache_bytes -= entry->length;
    } else {
      ns->body_cache_bytes = 0U;
    }
  }
  entry->retired = 1;
  lc_pouch_state_body_cache_entry_release(allocator, entry);
}

static void
lc_pouch_namespace_logstore_clear_body_cache(const lc_allocator *allocator,
                                             lc_pouch_namespace_logstore *ns) {
  lc_pouch_state_cache_record *record;

  if (ns == NULL) {
    return;
  }
  for (record = ns->records; record != NULL; record = record->next) {
    lc_pouch_state_cache_record_body_clear(allocator, NULL, record);
  }
  ns->body_cache_bytes = 0U;
}

static void
lc_pouch_state_cache_record_cleanup(const lc_allocator *allocator,
                                    lc_pouch_state_cache_record *record) {
  if (record == NULL) {
    return;
  }
  lc_pouch_state_cache_record_body_clear(allocator, NULL, record);
  lc_free_with_allocator(allocator, record->key);
  lc_free_with_allocator(allocator, record->content_type);
  lc_free_with_allocator(allocator, record->etag);
  lc_pouch_state_payload_span_cleanup(allocator, &record->payload_span);
  lc_pouch_state_record_ref_cleanup(allocator, &record->record_container_leaf,
                                    &record->record_offset,
                                    &record->has_record_ref);
  lc_free_with_allocator(allocator, record->payload_context);
  lc_free_with_allocator(allocator, record->descriptor);
  lc_free_with_allocator(allocator, record->metadata);
  lc_free_with_allocator(allocator, record);
}

static void
lc_pouch_state_cache_records_cleanup(const lc_allocator *allocator,
                                     lc_pouch_state_cache_record *record) {
  while (record != NULL) {
    lc_pouch_state_cache_record *next;

    next = record->next;
    lc_pouch_state_cache_record_cleanup(allocator, record);
    record = next;
  }
}

static void
lc_pouch_namespace_logstore_clear_records(const lc_allocator *allocator,
                                          lc_pouch_namespace_logstore *ns) {
  if (ns == NULL) {
    return;
  }
  if (ns->active_append_fd_owned && ns->active_append_fd >= 0) {
    (void)close(ns->active_append_fd);
  }
  ns->active_append_fd = -1;
  ns->active_append_fd_owned = 0;
  lc_pouch_state_cache_records_cleanup(allocator, ns->records);
  lc_free_with_allocator(allocator, ns->record_buckets);
  ns->records = NULL;
  ns->record_buckets = NULL;
  ns->record_bucket_count = 0U;
  ns->record_count = 0U;
  ns->body_cache_bytes = 0U;
  lc_free_with_allocator(allocator, ns->active_segment_leaf);
  lc_free_with_allocator(allocator, ns->latest_snapshot_leaf);
  lc_free_with_allocator(allocator, ns->namespace_path);
  ns->active_segment_leaf = NULL;
  ns->latest_snapshot_leaf = NULL;
  ns->namespace_path = NULL;
  ns->active_segment_offset = 0U;
  ns->active_segment_id = 0U;
}

/* The caller already owns the namespace projection or shared-mode mutation
 * authority. Keep the namespace node stable so unrelated exclusive namespace
 * operations cannot observe a freed cache-list entry after a failed commit. */
static void
lc_pouch_state_cache_invalidate_namespace(lc_pouch *pouch,
                                          const char *namespace_name) {
  lc_pouch_namespace_logstore *cache;

  if (pouch == NULL || namespace_name == NULL) {
    return;
  }
  cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 0, NULL);
  if (cache == NULL) {
    return;
  }
  lc_pouch_namespace_logstore_clear_records(&pouch->allocator, cache);
  cache->max_segment_id = 0U;
  cache->max_version = 0UL;
  cache->writer_mode_epoch = 0U;
  cache->initialized = 0;
  cache->decision_recovery_checked = 0;
}

static void
lc_pouch_state_visit_snapshot_cleanup(const lc_allocator *allocator,
                                      lc_pouch_state_visit_snapshot *snapshot) {
  if (snapshot == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, snapshot->key);
  lc_free_with_allocator(allocator, snapshot->content_type);
  lc_free_with_allocator(allocator, snapshot->etag);
  lc_free_with_allocator(allocator, snapshot->descriptor);
  lc_free_with_allocator(allocator, snapshot->metadata);
  memset(snapshot, 0, sizeof(*snapshot));
}

static void
lc_pouch_state_visit_snapshots_cleanup(const lc_allocator *allocator,
                                       lc_pouch_state_visit_snapshot *snapshots,
                                       size_t count) {
  size_t i;

  if (snapshots == NULL) {
    return;
  }
  for (i = 0U; i < count; ++i) {
    lc_pouch_state_visit_snapshot_cleanup(allocator, &snapshots[i]);
  }
  lc_free_with_allocator(allocator, snapshots);
}

static int lc_pouch_state_visit_snapshot_append(
    const lc_allocator *allocator, lc_pouch_state_visit_snapshot **snapshots,
    size_t *count, size_t *capacity, const lc_pouch_state_cache_record *record,
    lc_error *error) {
  lc_pouch_state_visit_snapshot *next;
  lc_pouch_state_visit_snapshot *snapshot;
  size_t next_capacity;

  if (*count == *capacity) {
    next_capacity = *capacity == 0U ? 16U : *capacity * 2U;
    next = (lc_pouch_state_visit_snapshot *)lc_calloc_with_allocator(
        allocator, next_capacity, sizeof(**snapshots));
    if (next == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state visit snapshot", NULL,
                          NULL, NULL);
    }
    if (*snapshots != NULL) {
      memcpy(next, *snapshots, *count * sizeof(**snapshots));
      lc_free_with_allocator(allocator, *snapshots);
    }
    *snapshots = next;
    *capacity = next_capacity;
  }
  snapshot = &(*snapshots)[*count];
  snapshot->key = lc_strdup_with_allocator(allocator, record->key);
  snapshot->content_type =
      record->content_type != NULL
          ? lc_strdup_with_allocator(allocator, record->content_type)
          : NULL;
  snapshot->etag = record->etag != NULL
                       ? lc_strdup_with_allocator(allocator, record->etag)
                       : NULL;
  snapshot->descriptor =
      record->descriptor != NULL
          ? lc_strdup_with_allocator(allocator, record->descriptor)
          : NULL;
  if (record->metadata_length > 0U) {
    snapshot->metadata = (unsigned char *)lc_alloc_with_allocator(
        allocator, record->metadata_length);
    if (snapshot->metadata != NULL) {
      memcpy(snapshot->metadata, record->metadata, record->metadata_length);
      snapshot->metadata_length = record->metadata_length;
    }
  }
  if (snapshot->key == NULL ||
      (record->content_type != NULL && snapshot->content_type == NULL) ||
      (record->etag != NULL && snapshot->etag == NULL) ||
      (record->descriptor != NULL && snapshot->descriptor == NULL) ||
      (record->metadata_length > 0U && snapshot->metadata == NULL)) {
    lc_pouch_state_visit_snapshot_cleanup(allocator, snapshot);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state visit entry", NULL,
                        NULL, NULL);
  }
  snapshot->index_seq = record->index_seq;
  snapshot->version = record->version;
  snapshot->bytes = record->bytes;
  snapshot->cipher_bytes = record->cipher_bytes;
  snapshot->updated_at_unix = record->updated_at_unix;
  snapshot->has_query_hidden = record->has_query_hidden;
  snapshot->query_hidden = record->query_hidden;
  snapshot->found = record->found;
  snapshot->record_type = record->record_type;
  ++(*count);
  return LC_OK;
}

static void lc_pouch_state_read_many_snapshot_cleanup(
    const lc_allocator *allocator,
    lc_pouch_state_read_many_snapshot *snapshot) {
  if (snapshot == NULL) {
    return;
  }
  if (snapshot->body != NULL) {
    snapshot->body->close(snapshot->body);
  }
  lc_free_with_allocator(allocator, snapshot->key);
  lc_free_with_allocator(allocator, snapshot->content_type);
  lc_free_with_allocator(allocator, snapshot->etag);
  lc_free_with_allocator(allocator, snapshot->payload_span_path);
  lc_pouch_state_payload_span_cleanup(allocator, &snapshot->payload_span);
  lc_free_with_allocator(allocator, snapshot->descriptor);
  lc_free_with_allocator(allocator, snapshot->metadata);
  lc_free_with_allocator(allocator, snapshot->crypto_context);
  memset(snapshot, 0, sizeof(*snapshot));
}

static void lc_pouch_state_read_many_snapshots_cleanup(
    const lc_allocator *allocator, lc_pouch_state_read_many_snapshot *snapshots,
    size_t count) {
  size_t i;

  if (snapshots == NULL) {
    return;
  }
  for (i = 0U; i < count; ++i) {
    lc_pouch_state_read_many_snapshot_cleanup(allocator, &snapshots[i]);
  }
  lc_free_with_allocator(allocator, snapshots);
}

static void lc_pouch_state_scan_body_snapshot_cleanup(
    const lc_allocator *allocator,
    lc_pouch_state_scan_body_snapshot *snapshot) {
  if (snapshot == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, snapshot->key);
  lc_free_with_allocator(allocator, snapshot->content_type);
  lc_free_with_allocator(allocator, snapshot->etag);
  lc_free_with_allocator(allocator, snapshot->payload_span_path);
  lc_pouch_state_payload_span_cleanup(allocator, &snapshot->payload_span);
  lc_free_with_allocator(allocator, snapshot->payload_context);
  lc_free_with_allocator(allocator, snapshot->descriptor);
  lc_free_with_allocator(allocator, snapshot->metadata);
  if (snapshot->body_cache != NULL && snapshot->body_cache->refcount > 0UL) {
    snapshot->body_cache->refcount -= 1UL;
    lc_pouch_state_body_cache_entry_release(allocator, snapshot->body_cache);
  }
  memset(snapshot, 0, sizeof(*snapshot));
}

static void lc_pouch_state_scan_body_snapshots_cleanup(
    const lc_allocator *allocator, lc_pouch_state_scan_body_snapshot *snapshots,
    size_t count) {
  size_t i;

  if (snapshots == NULL) {
    return;
  }
  for (i = 0U; i < count; ++i) {
    lc_pouch_state_scan_body_snapshot_cleanup(allocator, &snapshots[i]);
  }
  lc_free_with_allocator(allocator, snapshots);
}

static int lc_pouch_state_scan_body_snapshot_append(
    lc_pouch *pouch, lc_pouch_state_scan_body_snapshot **snapshots,
    size_t *count, size_t *capacity,
    const lc_pouch_namespace_manifest *manifest,
    const lc_pouch_state_cache_record *record, lc_error *error) {
  lc_pouch_state_scan_body_snapshot *next;
  lc_pouch_state_scan_body_snapshot *snapshot;
  size_t next_capacity;

  if (pouch == NULL || snapshots == NULL || count == NULL || capacity == NULL ||
      manifest == NULL || record == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch scan snapshot requires pouch, snapshots, "
                        "count, capacity, and record",
                        NULL, NULL, "pouch");
  }
  if (!record->found) {
    return LC_OK;
  }
  if (!record->payload_span.present) {
    return LC_OK;
  }
  if (*count == *capacity) {
    next_capacity = *capacity == 0U ? 64U : *capacity * 2U;
    next = (lc_pouch_state_scan_body_snapshot *)lc_calloc_with_allocator(
        &pouch->allocator, next_capacity, sizeof(**snapshots));
    if (next == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch scan snapshots", NULL, NULL,
                          NULL);
    }
    if (*snapshots != NULL) {
      memcpy(next, *snapshots, *count * sizeof(**snapshots));
      lc_free_with_allocator(&pouch->allocator, *snapshots);
    }
    *snapshots = next;
    *capacity = next_capacity;
  }
  snapshot = &(*snapshots)[*count];
  snapshot->key = lc_strdup_with_allocator(&pouch->allocator, record->key);
  snapshot->content_type =
      record->content_type != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, record->content_type)
          : NULL;
  snapshot->etag =
      record->etag != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, record->etag)
          : NULL;
  if (lc_pouch_state_payload_span_copy(&pouch->allocator, &record->payload_span,
                                       &snapshot->payload_span,
                                       error) != LC_OK) {
    lc_pouch_state_scan_body_snapshot_cleanup(&pouch->allocator, snapshot);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  snapshot->payload_context =
      record->payload_context != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, record->payload_context)
          : NULL;
  snapshot->payload_span_path = lc_pouch_state_payload_span_path(
      &pouch->allocator, manifest->namespace_path, &record->payload_span,
      error);
  snapshot->descriptor =
      record->descriptor != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, record->descriptor)
          : NULL;
  if (record->metadata_length > 0U) {
    snapshot->metadata = (unsigned char *)lc_alloc_with_allocator(
        &pouch->allocator, record->metadata_length);
    if (snapshot->metadata != NULL) {
      memcpy(snapshot->metadata, record->metadata, record->metadata_length);
      snapshot->metadata_length = record->metadata_length;
    }
  }
  if (snapshot->key == NULL ||
      (record->content_type != NULL && snapshot->content_type == NULL) ||
      (record->etag != NULL && snapshot->etag == NULL) ||
      snapshot->payload_span_path == NULL || !snapshot->payload_span.present ||
      (record->payload_context != NULL && snapshot->payload_context == NULL) ||
      (record->descriptor != NULL && snapshot->descriptor == NULL) ||
      (record->metadata_length > 0U && snapshot->metadata == NULL)) {
    lc_pouch_state_scan_body_snapshot_cleanup(&pouch->allocator, snapshot);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch scan metadata", NULL, NULL,
                        NULL);
  }
  snapshot->version = record->version;
  snapshot->bytes = record->bytes;
  snapshot->cipher_bytes = record->cipher_bytes;
  if (record->body_cache != NULL &&
      record->body_cache->version == record->version &&
      record->body_cache->length == (size_t)record->bytes) {
    record->body_cache->refcount += 1UL;
    snapshot->body_cache = record->body_cache;
  }
  snapshot->updated_at_unix = record->updated_at_unix;
  snapshot->has_query_hidden = record->has_query_hidden;
  snapshot->query_hidden = record->query_hidden;
  snapshot->found = 1;
  snapshot->record_type = record->record_type;
  ++(*count);
  return LC_OK;
}

static int lc_pouch_state_scan_body_snapshot_open_result(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_state_scan_body_snapshot *snapshot,
    lc_pouch_state_read_result *out, lc_error *error) {
  lc_pouch_state_process_namespace_mutex *process_mutex;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_cache_record *record;
  lc_pouch_state_entry current;
  char *crypto_context;
  int rc;

  if (pouch == NULL || namespace_name == NULL || snapshot == NULL ||
      out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch scan body requires pouch, namespace, snapshot, "
                        "and output",
                        NULL, NULL, "pouch");
  }
  memset(out, 0, sizeof(*out));
  if (!snapshot->found) {
    return LC_OK;
  }
  if (!snapshot->payload_span.present) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch scan payload span is invalid", NULL, NULL,
                        "pouch");
  }
  if (snapshot->body_cache != NULL &&
      snapshot->body_cache->version == snapshot->version &&
      snapshot->body_cache->length == (size_t)snapshot->bytes) {
    rc = lc_pouch_state_body_cache_source_open(
        &pouch->allocator, snapshot->body_cache, &out->body, error);
  } else {
    rc = LC_OK;
  }
  crypto_context = NULL;
  if (rc == LC_OK && out->body == NULL && snapshot->descriptor != NULL) {
    crypto_context = lc_pouch_state_payload_context_for_read(
        &pouch->allocator, namespace_name, snapshot->key,
        snapshot->payload_context);
    if (crypto_context == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch scan crypto context", NULL,
                          NULL, NULL);
    }
  }
  process_mutex = NULL;
  if (rc == LC_OK && out->body == NULL &&
      snapshot->bytes <= LC_POUCH_STATE_BODY_CACHE_RECORD_MAX_BYTES) {
    rc = lc_pouch_state_process_namespace_mutex_lock(pouch, namespace_name,
                                                     &process_mutex, error);
    if (rc == LC_OK) {
      cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 0, error);
      record = cache != NULL
                   ? lc_pouch_state_cache_record_find(cache, snapshot->key)
                   : NULL;
      if (record != NULL && record->found &&
          record->version == snapshot->version &&
          record->bytes == snapshot->bytes) {
        memset(&current, 0, sizeof(current));
        current.key = snapshot->key;
        current.content_type = snapshot->content_type;
        current.etag = snapshot->etag;
        current.payload_span = snapshot->payload_span;
        current.payload_context = snapshot->payload_context;
        current.descriptor = snapshot->descriptor;
        current.metadata = snapshot->metadata;
        current.metadata_length = snapshot->metadata_length;
        current.version = snapshot->version;
        current.bytes = snapshot->bytes;
        current.cipher_bytes = snapshot->cipher_bytes;
        current.updated_at_unix = snapshot->updated_at_unix;
        current.has_query_hidden = snapshot->has_query_hidden;
        current.query_hidden = snapshot->query_hidden;
        current.seen = 1;
        current.found = 1;
        rc = lc_pouch_state_read_many_snapshot_body_from_cache(
            pouch, cache, record, crypto_context, snapshot->payload_span_path,
            snapshot->payload_span.payload_offset,
            snapshot->payload_span.payload_length, &current, 0, &out->body,
            error);
      }
      lc_pouch_state_process_namespace_mutex_unlock(&process_mutex);
    }
  }
  if (rc == LC_OK && out->body == NULL) {
    rc = lc_pouch_state_source_from_span(
        pouch, crypto_context, snapshot->payload_span_path,
        snapshot->payload_span.payload_offset,
        snapshot->payload_span.payload_length, snapshot->descriptor, &out->body,
        error);
  }
  lc_free_with_allocator(&pouch->allocator, crypto_context);
  if (rc != LC_OK) {
    return rc;
  }
  out->content_type = snapshot->content_type;
  out->etag = snapshot->etag;
  out->descriptor = snapshot->descriptor;
  out->metadata = snapshot->metadata;
  out->metadata_length = snapshot->metadata_length;
  out->version = snapshot->version;
  out->bytes = snapshot->bytes;
  out->cipher_bytes = snapshot->cipher_bytes;
  out->updated_at_unix = snapshot->updated_at_unix;
  out->has_query_hidden = snapshot->has_query_hidden;
  out->query_hidden = snapshot->query_hidden;
  out->has_body = snapshot->payload_span.present;
  out->found = 1;
  snapshot->content_type = NULL;
  snapshot->etag = NULL;
  snapshot->descriptor = NULL;
  snapshot->metadata = NULL;
  snapshot->metadata_length = 0U;
  return LC_OK;
}

static int lc_pouch_state_read_many_snapshot_body_from_cache(
    lc_pouch *pouch, lc_pouch_namespace_logstore *cache,
    lc_pouch_state_cache_record *record, const char *crypto_context,
    const char *payload_span_path, uint64_t payload_offset,
    uint64_t payload_length, const lc_pouch_state_entry *current,
    int copy_cached_body, lc_source **out, lc_error *error) {
  lc_source *source;
  lc_sink *sink;
  lc_pouch_state_body_cache_entry *entry;
  const void *bytes;
  size_t length;
  size_t written;
  int rc;

  entry = NULL;
  if (pouch == NULL || cache == NULL || record == NULL ||
      payload_span_path == NULL || current == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch read-many body cache requires pouch, cache, "
                        "record, path, entry, and output",
                        NULL, NULL, "pouch");
  }
  *out = NULL;
  if (record->body_cache != NULL &&
      record->body_cache->version == record->version &&
      record->body_cache->length == (size_t)record->bytes) {
    if (copy_cached_body) {
      return lc_source_from_memory(record->body_cache->bytes,
                                   record->body_cache->length, out, error);
    }
    return lc_pouch_state_body_cache_source_open(
        &pouch->allocator, record->body_cache, out, error);
  }
  if (record->body_cache != NULL) {
    lc_pouch_state_cache_record_body_clear(&pouch->allocator, cache, record);
  }
  if (current->bytes > LC_POUCH_STATE_BODY_CACHE_RECORD_MAX_BYTES) {
    return lc_pouch_state_source_from_span(
        pouch, crypto_context, payload_span_path, payload_offset,
        payload_length, current->descriptor, out, error);
  }
  source = NULL;
  sink = NULL;
  rc = lc_pouch_state_source_from_span(pouch, crypto_context, payload_span_path,
                                       payload_offset, payload_length,
                                       current->descriptor, &source, error);
  if (rc == LC_OK) {
    rc = lc_sink_to_memory(&sink, error);
  }
  written = 0U;
  if (rc == LC_OK) {
    rc = lc_copy(source, sink, &written, error);
  }
  if (rc == LC_OK && written != (size_t)current->bytes) {
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "pouch payload plaintext byte count changed", NULL, NULL,
                      "pouch");
  }
  bytes = NULL;
  length = 0U;
  if (rc == LC_OK) {
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  }
  if (rc == LC_OK && length != (size_t)current->bytes) {
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "pouch payload cache byte count changed", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK) {
    if (cache->body_cache_bytes + length >
        LC_POUCH_STATE_BODY_CACHE_MAX_BYTES) {
      lc_pouch_namespace_logstore_clear_body_cache(&pouch->allocator, cache);
    }
    if (length <= LC_POUCH_STATE_BODY_CACHE_MAX_BYTES) {
      entry = (lc_pouch_state_body_cache_entry *)lc_calloc_with_allocator(
          &pouch->allocator, 1U, sizeof(*entry));
      if (entry == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch body cache", NULL, NULL,
                          "pouch");
      } else {
        if (length > 0U) {
          entry->bytes = (unsigned char *)lc_alloc_with_allocator(
              &pouch->allocator, length);
          if (entry->bytes == NULL) {
            lc_free_with_allocator(&pouch->allocator, entry);
            entry = NULL;
            rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                              "failed to allocate pouch body cache", NULL, NULL,
                              "pouch");
          } else {
            memcpy(entry->bytes, bytes, length);
          }
        }
        if (rc == LC_OK) {
          entry->length = length;
          entry->version = record->version;
          record->body_cache = entry;
          cache->body_cache_bytes += length;
          entry = NULL;
        }
      }
    }
  }
  if (entry != NULL) {
    entry->retired = 1;
    lc_pouch_state_body_cache_entry_release(&pouch->allocator, entry);
  }
  if (rc == LC_OK) {
    if (!copy_cached_body && record->body_cache != NULL &&
        record->body_cache->version == record->version &&
        record->body_cache->length == length) {
      rc = lc_pouch_state_body_cache_source_open(
          &pouch->allocator, record->body_cache, out, error);
    } else {
      const void *cached_bytes;

      cached_bytes =
          record->body_cache != NULL && record->body_cache->bytes != NULL
              ? (const void *)record->body_cache->bytes
              : (const void *)"";
      rc = lc_source_from_memory(cached_bytes, length, out, error);
    }
  }
  if (source != NULL) {
    source->close(source);
  }
  if (sink != NULL) {
    sink->close(sink);
  }
  return rc;
}

static void lc_pouch_state_cache_record_store_body_source(
    lc_pouch *pouch, lc_pouch_namespace_logstore *cache,
    lc_pouch_state_cache_record *record, lc_source *body,
    lc_pouch_generation version, uint64_t bytes) {
  lc_pouch_state_body_cache_entry *entry;
  lc_sink *sink;
  const void *data;
  size_t length;
  size_t written;
  lc_error error;
  int rc;

  if (pouch == NULL || cache == NULL || record == NULL || body == NULL ||
      body->reset == NULL ||
      bytes > LC_POUCH_STATE_BODY_CACHE_RECORD_MAX_BYTES) {
    return;
  }
  lc_error_init(&error);
  entry = NULL;
  sink = NULL;
  rc = body->reset(body, &error);
  if (rc == LC_OK) {
    rc = lc_sink_to_memory(&sink, &error);
  }
  written = 0U;
  if (rc == LC_OK) {
    rc = lc_copy(body, sink, &written, &error);
  }
  data = NULL;
  length = 0U;
  if (rc == LC_OK) {
    rc = lc_sink_memory_bytes(sink, &data, &length, &error);
  }
  if (rc == LC_OK && (written != (size_t)bytes || length != (size_t)bytes)) {
    rc = LC_ERR_PROTOCOL;
  }
  if (rc == LC_OK) {
    if (record->body_cache != NULL) {
      lc_pouch_state_cache_record_body_clear(&pouch->allocator, cache, record);
    }
    if (cache->body_cache_bytes + length >
        LC_POUCH_STATE_BODY_CACHE_MAX_BYTES) {
      lc_pouch_namespace_logstore_clear_body_cache(&pouch->allocator, cache);
    }
    entry = (lc_pouch_state_body_cache_entry *)lc_calloc_with_allocator(
        &pouch->allocator, 1U, sizeof(*entry));
    if (entry != NULL) {
      if (length > 0U) {
        entry->bytes =
            (unsigned char *)lc_alloc_with_allocator(&pouch->allocator, length);
        if (entry->bytes == NULL) {
          lc_free_with_allocator(&pouch->allocator, entry);
          entry = NULL;
        } else {
          memcpy(entry->bytes, data, length);
        }
      }
      if (entry != NULL) {
        entry->length = length;
        entry->version = version;
        record->body_cache = entry;
        cache->body_cache_bytes += length;
      }
    }
  }
  if (sink != NULL) {
    sink->close(sink);
  }
  lc_error_cleanup(&error);
}

static int lc_pouch_state_read_many_snapshot_from_entry(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_namespace_manifest *manifest, const char *requested_key,
    lc_pouch_namespace_logstore *cache, lc_pouch_state_cache_record *record,
    const lc_pouch_state_entry *current, int include_body, int copy_cached_body,
    lc_pouch_state_read_many_snapshot *snapshot, lc_error *error) {
  int rc;

  if (pouch == NULL || namespace_name == NULL || manifest == NULL ||
      requested_key == NULL || current == NULL || snapshot == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch read-many snapshot requires pouch, namespace, "
                        "manifest, key, entry, and snapshot",
                        NULL, NULL, NULL);
  }
  memset(snapshot, 0, sizeof(*snapshot));
  snapshot->key = lc_strdup_with_allocator(&pouch->allocator, requested_key);
  if (snapshot->key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch read-many key", NULL, NULL,
                        NULL);
  }
  if (!current->found) {
    return LC_OK;
  }
  if (!current->payload_span.present && include_body) {
    return LC_OK;
  }
  if (!current->payload_span.present && !include_body) {
    snapshot->content_type =
        current->content_type != NULL
            ? lc_strdup_with_allocator(&pouch->allocator, current->content_type)
            : NULL;
    snapshot->etag =
        current->etag != NULL
            ? lc_strdup_with_allocator(&pouch->allocator, current->etag)
            : NULL;
    if (current->metadata_length > 0U) {
      snapshot->metadata = (unsigned char *)lc_alloc_with_allocator(
          &pouch->allocator, current->metadata_length);
      if (snapshot->metadata != NULL) {
        memcpy(snapshot->metadata, current->metadata, current->metadata_length);
        snapshot->metadata_length = current->metadata_length;
      }
    }
    if ((current->content_type != NULL && snapshot->content_type == NULL) ||
        (current->etag != NULL && snapshot->etag == NULL) ||
        (current->metadata_length > 0U && snapshot->metadata == NULL)) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch read-many metadata", NULL,
                          NULL, NULL);
    }
    snapshot->index_seq = current->index_seq;
    snapshot->version = current->version;
    snapshot->bytes = current->bytes;
    snapshot->cipher_bytes = current->cipher_bytes;
    snapshot->updated_at_unix = current->updated_at_unix;
    snapshot->has_query_hidden = current->has_query_hidden;
    snapshot->query_hidden = current->query_hidden;
    snapshot->found = 1;
    snapshot->record_type = current->record_type;
    return LC_OK;
  }
  if (!current->payload_span.present) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state payload span is invalid", NULL, NULL,
                        "pouch");
  }
  snapshot->content_type =
      current->content_type != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, current->content_type)
          : NULL;
  snapshot->etag =
      current->etag != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, current->etag)
          : NULL;
  snapshot->descriptor =
      current->descriptor != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, current->descriptor)
          : NULL;
  if (current->metadata_length > 0U) {
    snapshot->metadata = (unsigned char *)lc_alloc_with_allocator(
        &pouch->allocator, current->metadata_length);
    if (snapshot->metadata != NULL) {
      memcpy(snapshot->metadata, current->metadata, current->metadata_length);
      snapshot->metadata_length = current->metadata_length;
    }
  }
  snapshot->crypto_context =
      current->payload_context != NULL
          ? lc_strdup_with_allocator(&pouch->allocator,
                                     current->payload_context)
          : NULL;
  if (lc_pouch_state_payload_span_copy(
          &pouch->allocator, &current->payload_span, &snapshot->payload_span,
          error) != LC_OK) {
    lc_pouch_state_read_many_snapshot_cleanup(&pouch->allocator, snapshot);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  if ((current->content_type != NULL && snapshot->content_type == NULL) ||
      (current->etag != NULL && snapshot->etag == NULL) ||
      (current->payload_context != NULL && snapshot->crypto_context == NULL) ||
      (current->descriptor != NULL && snapshot->descriptor == NULL) ||
      (current->metadata_length > 0U && snapshot->metadata == NULL)) {
    lc_pouch_state_read_many_snapshot_cleanup(&pouch->allocator, snapshot);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch read-many metadata", NULL,
                        NULL, NULL);
  }
  if (include_body) {
    snapshot->payload_span_path = lc_pouch_state_payload_span_path(
        &pouch->allocator, manifest->namespace_path, &current->payload_span,
        error);
    if (snapshot->payload_span_path == NULL) {
      lc_pouch_state_read_many_snapshot_cleanup(&pouch->allocator, snapshot);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch read-many payload span",
                          NULL, NULL, NULL);
    }
    if (current->descriptor != NULL && snapshot->crypto_context == NULL) {
      snapshot->crypto_context = lc_pouch_state_crypto_context(
          &pouch->allocator, namespace_name, requested_key);
      if (snapshot->crypto_context == NULL) {
        lc_pouch_state_read_many_snapshot_cleanup(&pouch->allocator, snapshot);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch read-many crypto context",
                            NULL, NULL, NULL);
      }
    }
    if (cache != NULL && record != NULL) {
      rc = lc_pouch_state_read_many_snapshot_body_from_cache(
          pouch, cache, record, snapshot->crypto_context,
          snapshot->payload_span_path, current->payload_span.payload_offset,
          current->payload_span.payload_length, current, copy_cached_body,
          &snapshot->body, error);
    } else {
      rc = lc_pouch_state_source_from_span(
          pouch, snapshot->crypto_context, snapshot->payload_span_path,
          current->payload_span.payload_offset,
          current->payload_span.payload_length, snapshot->descriptor,
          &snapshot->body, error);
    }
    if (rc != LC_OK) {
      lc_pouch_state_read_many_snapshot_cleanup(&pouch->allocator, snapshot);
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_TRANSPORT;
    }
  }
  snapshot->index_seq = current->index_seq;
  snapshot->version = current->version;
  snapshot->bytes = current->bytes;
  snapshot->cipher_bytes = current->cipher_bytes;
  snapshot->updated_at_unix = current->updated_at_unix;
  snapshot->has_query_hidden = current->has_query_hidden;
  snapshot->query_hidden = current->query_hidden;
  snapshot->found = 1;
  snapshot->record_type = current->record_type;
  return LC_OK;
}

void lc_pouch_state_cache_cleanup(lc_pouch *pouch) {
  lc_pouch_namespace_logstore *ns;

  if (pouch == NULL) {
    return;
  }
  ns = pouch->namespace_logstores;
  while (ns != NULL) {
    lc_pouch_namespace_logstore *next;

    next = ns->next;
    if (ns->exclusive_append_gate_initialized) {
      (void)pthread_mutex_destroy(&ns->exclusive_append_gate);
    }
    lc_free_with_allocator(&pouch->allocator, ns->namespace_name);
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator, ns);
    lc_free_with_allocator(&pouch->allocator, ns);
    ns = next;
  }
  pouch->namespace_logstores = NULL;
  memset(pouch->namespace_logstore_buckets, 0,
         sizeof(pouch->namespace_logstore_buckets));
}

void lc_pouch_state_source_cache_cleanup(lc_pouch *pouch) {
  lc_pouch_source_cache_entry *entry;

  if (pouch == NULL) {
    return;
  }
  if (pouch->source_cache_mutex_initialized) {
    (void)pthread_mutex_lock(&pouch->source_cache_mutex);
  }
  entry = pouch->source_cache_entries;
  while (entry != NULL) {
    lc_pouch_source_cache_entry *next;

    next = entry->next;
    if (entry->fd >= 0) {
      close(entry->fd);
    }
    lc_free_with_allocator(&pouch->allocator, entry->path);
    lc_free_with_allocator(&pouch->allocator, entry);
    entry = next;
  }
  pouch->source_cache_entries = NULL;
  pouch->source_cache_count = 0U;
  pouch->source_cache_tick = 0UL;
  if (pouch->source_cache_mutex_initialized) {
    (void)pthread_mutex_unlock(&pouch->source_cache_mutex);
  }
}

static void lc_pouch_state_source_cache_evict_one(lc_pouch *pouch) {
  lc_pouch_source_cache_entry **cursor;
  lc_pouch_source_cache_entry **oldest_cursor;
  unsigned long oldest_tick;

  if (pouch == NULL || pouch->source_cache_entries == NULL) {
    return;
  }
  cursor = &pouch->source_cache_entries;
  oldest_cursor = cursor;
  oldest_tick = (*cursor)->last_used;
  while (*cursor != NULL) {
    if ((*cursor)->last_used <= oldest_tick) {
      oldest_tick = (*cursor)->last_used;
      oldest_cursor = cursor;
    }
    cursor = &(*cursor)->next;
  }
  if (*oldest_cursor != NULL) {
    lc_pouch_source_cache_entry *evicted;

    evicted = *oldest_cursor;
    *oldest_cursor = evicted->next;
    if (evicted->fd >= 0) {
      close(evicted->fd);
    }
    lc_free_with_allocator(&pouch->allocator, evicted->path);
    lc_free_with_allocator(&pouch->allocator, evicted);
    if (pouch->source_cache_count > 0U) {
      --pouch->source_cache_count;
    }
  }
}

static int lc_pouch_state_source_cache_dup_fd(lc_pouch *pouch, const char *path,
                                              int *out_fd, lc_error *error) {
  lc_pouch_source_cache_entry *entry;
  int fd;
  int pthread_rc;
  int rc;

  if (pouch == NULL || path == NULL || out_fd == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch source cache requires pouch, path, and output",
                        NULL, NULL, "pouch");
  }
  *out_fd = -1;
  if (!pouch->source_cache_mutex_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch source cache mutex is unavailable", NULL, NULL,
                        "pouch");
  }
  pthread_rc = pthread_mutex_lock(&pouch->source_cache_mutex);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch source cache",
                        strerror(pthread_rc), NULL, "pouch");
  }
  rc = LC_OK;
  for (entry = pouch->source_cache_entries; entry != NULL;
       entry = entry->next) {
    if (strcmp(entry->path, path) == 0) {
      entry->last_used = ++pouch->source_cache_tick;
      fd = dup(entry->fd);
      if (fd < 0) {
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to duplicate pouch source descriptor",
                          strerror(errno), NULL, "pouch");
      } else {
        *out_fd = fd;
      }
      goto cleanup;
    }
  }
  while (pouch->source_cache_count >= LC_POUCH_STATE_SOURCE_CACHE_MAX_FILES) {
    lc_pouch_state_source_cache_evict_one(pouch);
  }
  entry = (lc_pouch_source_cache_entry *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*entry));
  if (entry == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch source cache entry", NULL, NULL,
                      "pouch");
    goto cleanup;
  }
  entry->path = lc_strdup_with_allocator(&pouch->allocator, path);
  if (entry->path == NULL) {
    lc_free_with_allocator(&pouch->allocator, entry);
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to copy pouch source cache path", NULL, NULL,
                      "pouch");
    goto cleanup;
  }
  entry->fd = open(path, O_RDONLY);
  if (entry->fd < 0) {
    lc_free_with_allocator(&pouch->allocator, entry->path);
    lc_free_with_allocator(&pouch->allocator, entry);
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to open pouch source cache file", strerror(errno),
                      NULL, "pouch");
    goto cleanup;
  }
  entry->last_used = ++pouch->source_cache_tick;
  entry->next = pouch->source_cache_entries;
  pouch->source_cache_entries = entry;
  ++pouch->source_cache_count;
  fd = dup(entry->fd);
  if (fd < 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to duplicate pouch source descriptor",
                      strerror(errno), NULL, "pouch");
    goto cleanup;
  }
  *out_fd = fd;
cleanup:
  (void)pthread_mutex_unlock(&pouch->source_cache_mutex);
  return rc;
}

static int
lc_pouch_state_source_from_span(lc_pouch *pouch, const char *crypto_context,
                                const char *path, uint64_t payload_offset,
                                uint64_t payload_length, const char *descriptor,
                                lc_source **out, lc_error *error) {
  int fd;
  int rc;

  if (pouch == NULL || path == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch span source requires pouch, path, and output",
                        NULL, NULL, "pouch");
  }
  fd = -1;
  rc = lc_pouch_state_source_cache_dup_fd(pouch, path, &fd, error);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_pouch_crypto_source_from_fd_span(pouch->crypto, crypto_context, fd,
                                             payload_offset, payload_length,
                                             descriptor, out, error);
}

static lc_pouch_namespace_logstore *
lc_pouch_namespace_logstore_find(lc_pouch *pouch, const char *namespace_name,
                                 int create, lc_error *error) {
  lc_pouch_namespace_logstore *ns;
  unsigned long namespace_hash;
  size_t bucket_index;
  int rc;
  int pthread_rc;

  if (pouch == NULL || namespace_name == NULL) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch state cache namespace requires context", NULL,
                       NULL, "pouch");
    return NULL;
  }
  namespace_hash = lc_pouch_state_namespace_hash(namespace_name);
  bucket_index =
      (size_t)(namespace_hash %
               (unsigned long)LC_POUCH_NAMESPACE_REGISTRY_BUCKET_COUNT);
  pthread_rc = pthread_mutex_lock(&pouch->state_cache_mutex);
  if (pthread_rc != 0) {
    (void)lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                       "failed to lock pouch state cache registry",
                       strerror(pthread_rc), NULL, "pouch");
    return NULL;
  }
  for (ns = pouch->namespace_logstore_buckets[bucket_index]; ns != NULL;
       ns = ns->hash_next) {
    if (ns->namespace_hash == namespace_hash &&
        strcmp(ns->namespace_name, namespace_name) == 0) {
      (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
      return ns;
    }
  }
  if (!create) {
    (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
    return NULL;
  }
  ns = (lc_pouch_namespace_logstore *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*ns));
  if (ns == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch state cache namespace", NULL, NULL,
                 NULL);
    (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
    return NULL;
  }
  ns->namespace_name =
      lc_strdup_with_allocator(&pouch->allocator, namespace_name);
  if (ns->namespace_name == NULL) {
    lc_free_with_allocator(&pouch->allocator, ns);
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch state cache namespace name", NULL,
                 NULL, NULL);
    (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
    return NULL;
  }
  ns->active_append_fd = -1;
  ns->namespace_hash = namespace_hash;
  ns->hash_next = pouch->namespace_logstore_buckets[bucket_index];
  pouch->namespace_logstore_buckets[bucket_index] = ns;
  ns->next = pouch->namespace_logstores;
  pouch->namespace_logstores = ns;
  rc = lc_pouch_compaction_track_namespace(pouch, ns->namespace_name, error);
  if (rc != LC_OK) {
    pouch->namespace_logstores = ns->next;
    pouch->namespace_logstore_buckets[bucket_index] = ns->hash_next;
    lc_free_with_allocator(&pouch->allocator, ns->namespace_name);
    lc_free_with_allocator(&pouch->allocator, ns);
    (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
    return NULL;
  }
  (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
  return ns;
}

static int lc_pouch_state_exclusive_append_gate_lock(lc_pouch *pouch,
                                                     const char *namespace_name,
                                                     pthread_mutex_t **out,
                                                     lc_error *error) {
  lc_pouch_namespace_logstore *cache;
  pthread_mutex_t *gate;
  int pthread_rc;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      out == NULL || !pouch->state_cache_mutex_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch exclusive append gate requires namespace", NULL,
                        NULL, "pouch");
  }
  *out = NULL;
  cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 1, error);
  if (cache == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  pthread_rc = pthread_mutex_lock(&pouch->state_cache_mutex);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch namespace owner registry",
                        strerror(pthread_rc), NULL, "pouch");
  }
  rc = LC_OK;
  if (!cache->exclusive_append_gate_initialized) {
    rc = lc_pouch_state_mutex_init_recursive(&cache->exclusive_append_gate,
                                             error);
    if (rc == LC_OK) {
      cache->exclusive_append_gate_initialized = 1;
    }
  }
  gate = rc == LC_OK ? &cache->exclusive_append_gate : NULL;
  (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
  if (rc != LC_OK) {
    return rc;
  }
  pthread_rc = pthread_mutex_lock(gate);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch exclusive append gate",
                        strerror(pthread_rc), NULL, "pouch");
  }
  *out = gate;
  return LC_OK;
}

static void
lc_pouch_state_exclusive_append_gate_unlock(pthread_mutex_t **gate) {
  if (gate == NULL || *gate == NULL) {
    return;
  }
  (void)pthread_mutex_unlock(*gate);
  *gate = NULL;
}

int lc_pouch_state_compaction_track_cached_namespaces(lc_pouch *pouch,
                                                      lc_error *error) {
  lc_pouch_namespace_logstore *ns;
  int pthread_rc;
  int rc;

  if (pouch == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch compaction cache tracking requires pouch", NULL,
                        NULL, "pouch");
  }
  pthread_rc = pthread_mutex_lock(&pouch->state_cache_mutex);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch state cache registry",
                        strerror(pthread_rc), NULL, "pouch");
  }
  for (ns = pouch->namespace_logstores; ns != NULL; ns = ns->next) {
    rc = lc_pouch_compaction_track_namespace(pouch, ns->namespace_name, error);
    if (rc != LC_OK) {
      (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
      return rc;
    }
  }
  (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
  return LC_OK;
}

static unsigned long lc_pouch_state_cache_key_hash(const char *key) {
  unsigned long hash;
  const unsigned char *cursor;

  hash = 2166136261UL;
  cursor = (const unsigned char *)key;
  while (cursor != NULL && *cursor != '\0') {
    hash ^= (unsigned long)*cursor++;
    hash *= 16777619UL;
  }
  return hash;
}

static size_t lc_pouch_state_cache_bucket_count_for(size_t record_count) {
  size_t bucket_count;

  bucket_count = 1024U;
  while (bucket_count < record_count * 2U) {
    bucket_count *= 2U;
  }
  return bucket_count;
}

static int lc_pouch_state_cache_record_index_rebuild(
    lc_pouch *pouch, lc_pouch_namespace_logstore *ns, size_t next_record_count,
    lc_error *error) {
  lc_pouch_state_cache_record **buckets;
  lc_pouch_state_cache_record *record;
  size_t bucket_count;

  if (pouch == NULL || ns == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state cache index requires pouch and namespace",
                        NULL, NULL, NULL);
  }
  bucket_count = lc_pouch_state_cache_bucket_count_for(next_record_count);
  buckets = (lc_pouch_state_cache_record **)lc_calloc_with_allocator(
      &pouch->allocator, bucket_count, sizeof(*buckets));
  if (buckets == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state cache lookup index",
                        NULL, NULL, NULL);
  }
  for (record = ns->records; record != NULL; record = record->next) {
    size_t bucket;

    bucket =
        (size_t)(lc_pouch_state_cache_key_hash(record->key) % bucket_count);
    record->bucket_next = buckets[bucket];
    buckets[bucket] = record;
  }
  lc_free_with_allocator(&pouch->allocator, ns->record_buckets);
  ns->record_buckets = buckets;
  ns->record_bucket_count = bucket_count;
  return LC_OK;
}

static int lc_pouch_state_cache_record_index_ensure(
    lc_pouch *pouch, lc_pouch_namespace_logstore *ns, size_t next_record_count,
    lc_error *error) {
  if (ns->record_buckets != NULL &&
      next_record_count * 2U <= ns->record_bucket_count) {
    return LC_OK;
  }
  return lc_pouch_state_cache_record_index_rebuild(pouch, ns, next_record_count,
                                                   error);
}

static lc_pouch_state_cache_record *
lc_pouch_state_cache_record_find(lc_pouch_namespace_logstore *ns,
                                 const char *key) {
  lc_pouch_state_cache_record *record;

  if (ns == NULL || key == NULL) {
    return NULL;
  }
  if (ns->record_buckets != NULL && ns->record_bucket_count > 0U) {
    size_t bucket;

    bucket =
        (size_t)(lc_pouch_state_cache_key_hash(key) % ns->record_bucket_count);
    for (record = ns->record_buckets[bucket]; record != NULL;
         record = record->bucket_next) {
      if (strcmp(record->key, key) == 0) {
        return record;
      }
    }
    return NULL;
  }
  for (record = ns->records; record != NULL; record = record->next) {
    if (strcmp(record->key, key) == 0) {
      return record;
    }
  }
  return NULL;
}

static int lc_pouch_state_cache_record_ptr_compare(const void *left,
                                                   const void *right) {
  const lc_pouch_state_cache_record *a;
  const lc_pouch_state_cache_record *b;

  a = *(const lc_pouch_state_cache_record *const *)left;
  b = *(const lc_pouch_state_cache_record *const *)right;
  if (a == NULL && b == NULL) {
    return 0;
  }
  if (a == NULL) {
    return -1;
  }
  if (b == NULL) {
    return 1;
  }
  return strcmp(a->key, b->key);
}

static int lc_pouch_state_cache_record_key_compare(const void *left,
                                                   const void *right) {
  const char *key;
  const lc_pouch_state_cache_record *record;

  key = (const char *)left;
  record = *(const lc_pouch_state_cache_record *const *)right;
  if (record == NULL) {
    return key == NULL ? 0 : 1;
  }
  if (key == NULL) {
    return -1;
  }
  return strcmp(key, record->key);
}

static int lc_pouch_state_cache_record_index_build(
    lc_pouch *pouch, lc_pouch_namespace_logstore *ns,
    lc_pouch_state_cache_record ***records_out, size_t *count_out,
    lc_error *error) {
  lc_pouch_state_cache_record **records;
  lc_pouch_state_cache_record *record;
  size_t count;
  size_t index;

  if (records_out == NULL || count_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state cache record index requires outputs", NULL,
                        NULL, NULL);
  }
  *records_out = NULL;
  *count_out = 0U;
  if (pouch == NULL || ns == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state cache record index requires pouch and "
                        "namespace cache",
                        NULL, NULL, NULL);
  }
  count = 0U;
  for (record = ns->records; record != NULL; record = record->next) {
    ++count;
  }
  if (count == 0U) {
    return LC_OK;
  }
  records = (lc_pouch_state_cache_record **)lc_alloc_with_allocator(
      &pouch->allocator, count * sizeof(*records));
  if (records == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state cache record index",
                        NULL, NULL, NULL);
  }
  index = 0U;
  for (record = ns->records; record != NULL; record = record->next) {
    records[index++] = record;
  }
  qsort(records, count, sizeof(*records),
        lc_pouch_state_cache_record_ptr_compare);
  *records_out = records;
  *count_out = count;
  return LC_OK;
}

static lc_pouch_state_cache_record *
lc_pouch_state_cache_record_index_find(lc_pouch_state_cache_record **records,
                                       size_t count, const char *key) {
  lc_pouch_state_cache_record **found;

  if (records == NULL || count == 0U || key == NULL) {
    return NULL;
  }
  found = (lc_pouch_state_cache_record **)bsearch(
      key, records, count, sizeof(*records),
      lc_pouch_state_cache_record_key_compare);
  return found != NULL ? *found : NULL;
}

static size_t lc_pouch_state_cache_record_index_start_after(
    lc_pouch_state_cache_record **records, size_t count,
    const char *start_after) {
  size_t low;
  size_t high;

  if (records == NULL || count == 0U || start_after == NULL ||
      start_after[0] == '\0') {
    return 0U;
  }
  low = 0U;
  high = count;
  while (low < high) {
    size_t mid;
    int cmp;

    mid = low + (high - low) / 2U;
    cmp = records[mid] != NULL && records[mid]->key != NULL
              ? strcmp(records[mid]->key, start_after)
              : -1;
    if (cmp <= 0) {
      low = mid + 1U;
    } else {
      high = mid;
    }
  }
  return low;
}

static int
lc_pouch_state_entry_supersedes(const lc_pouch_state_entry *candidate,
                                const lc_pouch_state_entry *current) {
  if (candidate == NULL) {
    return 0;
  }
  if (current == NULL || !current->seen) {
    return 1;
  }
  if (candidate->record_type == LC_POUCH_STATE_RECORD_STATE_META) {
    /* Metadata is ordered independently from payload state. */
    return candidate->index_seq > current->index_seq;
  }
  return candidate->version > current->version ||
         (candidate->version == current->version &&
          candidate->index_seq > current->index_seq);
}

static int lc_pouch_state_cache_apply_entry(lc_pouch *pouch,
                                            lc_pouch_namespace_logstore *ns,
                                            const lc_pouch_state_entry *entry,
                                            lc_error *error) {
  lc_pouch_state_cache_record *record;
  char *key;
  char *content_type;
  char *etag;
  lc_pouch_state_payload_span payload_span;
  char *record_container_leaf;
  uint64_t record_offset;
  int has_record_ref;
  char *payload_context;
  char *descriptor;
  unsigned char *metadata;
  int rc;

  memset(&payload_span, 0, sizeof(payload_span));
  record_container_leaf = NULL;
  record_offset = 0U;
  has_record_ref = 0;
  metadata = NULL;
  if (entry == NULL || !entry->seen || entry->key == NULL) {
    return LC_OK;
  }
  if (entry->metadata_length > 0U && entry->metadata == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state metadata blob requires bytes", NULL, NULL,
                        "pouch");
  }
  if (entry->control) {
    if (entry->index_seq > ns->max_version) {
      ns->max_version = entry->index_seq;
    }
    return LC_OK;
  }
  record = lc_pouch_state_cache_record_find(ns, entry->key);
  if (record != NULL) {
    lc_pouch_state_entry current;

    memset(&current, 0, sizeof(current));
    current.seen = 1;
    current.version = record->version;
    current.index_seq = record->index_seq;
    current.record_type = record->record_type;
    if (!lc_pouch_state_entry_supersedes(entry, &current)) {
      return LC_OK;
    }
  }
  if (record == NULL) {
    rc = lc_pouch_state_cache_record_index_ensure(pouch, ns,
                                                  ns->record_count + 1U, error);
    if (rc != LC_OK) {
      return rc;
    }
    record = (lc_pouch_state_cache_record *)lc_calloc_with_allocator(
        &pouch->allocator, 1U, sizeof(*record));
    if (record == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state cache record", NULL,
                          NULL, NULL);
    }
    record->key = lc_strdup_with_allocator(&pouch->allocator, entry->key);
    if (record->key == NULL) {
      lc_pouch_state_cache_record_cleanup(&pouch->allocator, record);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state cache key", NULL,
                          NULL, NULL);
    }
    record->next = ns->records;
    ns->records = record;
    ++ns->record_count;
    if (ns->record_buckets != NULL && ns->record_bucket_count > 0U) {
      size_t bucket;

      bucket = (size_t)(lc_pouch_state_cache_key_hash(record->key) %
                        ns->record_bucket_count);
      record->bucket_next = ns->record_buckets[bucket];
      ns->record_buckets[bucket] = record;
    }
  }
  key = record->key;
  content_type =
      entry->content_type != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, entry->content_type)
          : NULL;
  etag = entry->etag != NULL
             ? lc_strdup_with_allocator(&pouch->allocator, entry->etag)
             : NULL;
  rc = lc_pouch_state_payload_span_copy(&pouch->allocator, &entry->payload_span,
                                        &payload_span, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, content_type);
    lc_free_with_allocator(&pouch->allocator, etag);
    return rc;
  }
  rc = lc_pouch_state_record_ref_copy(
      &pouch->allocator, entry->record_container_leaf, entry->record_offset,
      entry->has_record_ref, &record_container_leaf, &record_offset,
      &has_record_ref, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, content_type);
    lc_free_with_allocator(&pouch->allocator, etag);
    lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
    return rc;
  }
  payload_context =
      entry->payload_context != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, entry->payload_context)
          : NULL;
  descriptor =
      entry->descriptor != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, entry->descriptor)
          : NULL;
  if (entry->metadata_length > 0U) {
    metadata = (unsigned char *)lc_alloc_with_allocator(&pouch->allocator,
                                                        entry->metadata_length);
    if (metadata != NULL) {
      memcpy(metadata, entry->metadata, entry->metadata_length);
    }
  }
  if ((entry->content_type != NULL && content_type == NULL) ||
      (entry->etag != NULL && etag == NULL) ||
      (entry->payload_span.present && !payload_span.present) ||
      (entry->has_record_ref && !has_record_ref) ||
      (entry->payload_context != NULL && payload_context == NULL) ||
      (entry->descriptor != NULL && descriptor == NULL) ||
      (entry->metadata_length > 0U && metadata == NULL)) {
    lc_free_with_allocator(&pouch->allocator, content_type);
    lc_free_with_allocator(&pouch->allocator, etag);
    lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
    lc_pouch_state_record_ref_cleanup(&pouch->allocator, &record_container_leaf,
                                      &record_offset, &has_record_ref);
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_free_with_allocator(&pouch->allocator, descriptor);
    lc_free_with_allocator(&pouch->allocator, metadata);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch state cache record", NULL, NULL,
                        NULL);
  }
  if (record->body_cache != NULL &&
      (!entry->found || !record->found || record->version != entry->version ||
       record->bytes != entry->bytes ||
       record->cipher_bytes != entry->cipher_bytes ||
       !lc_pouch_state_string_equal(record->etag, entry->etag) ||
       record->payload_span.present != entry->payload_span.present ||
       (record->payload_span.present &&
        (strcmp(record->payload_span.container_leaf,
                entry->payload_span.container_leaf) != 0 ||
         record->payload_span.record_offset !=
             entry->payload_span.record_offset ||
         record->payload_span.payload_offset !=
             entry->payload_span.payload_offset ||
         record->payload_span.payload_length !=
             entry->payload_span.payload_length ||
         record->payload_span.payload_crc !=
             entry->payload_span.payload_crc)) ||
       record->has_record_ref != entry->has_record_ref ||
       (record->has_record_ref &&
        (strcmp(record->record_container_leaf, entry->record_container_leaf) !=
             0 ||
         record->record_offset != entry->record_offset)) ||
       !lc_pouch_state_string_equal(record->payload_context,
                                    entry->payload_context) ||
       !lc_pouch_state_string_equal(record->descriptor, entry->descriptor) ||
       record->metadata_length != entry->metadata_length ||
       (record->metadata_length > 0U &&
        memcmp(record->metadata, entry->metadata, record->metadata_length) !=
            0))) {
    lc_pouch_state_cache_record_body_clear(&pouch->allocator, ns, record);
  }
  lc_free_with_allocator(&pouch->allocator, record->content_type);
  lc_free_with_allocator(&pouch->allocator, record->etag);
  lc_pouch_state_payload_span_cleanup(&pouch->allocator, &record->payload_span);
  lc_pouch_state_record_ref_cleanup(
      &pouch->allocator, &record->record_container_leaf, &record->record_offset,
      &record->has_record_ref);
  lc_free_with_allocator(&pouch->allocator, record->payload_context);
  lc_free_with_allocator(&pouch->allocator, record->descriptor);
  lc_free_with_allocator(&pouch->allocator, record->metadata);
  record->key = key;
  record->content_type = content_type;
  record->etag = etag;
  record->payload_span = payload_span;
  memset(&payload_span, 0, sizeof(payload_span));
  record->record_container_leaf = record_container_leaf;
  record->record_offset = record_offset;
  record->has_record_ref = has_record_ref;
  record_container_leaf = NULL;
  record_offset = 0UL;
  has_record_ref = 0;
  record->payload_context = payload_context;
  record->descriptor = descriptor;
  record->metadata = metadata;
  record->metadata_length = entry->metadata_length;
  record->index_seq = entry->index_seq;
  record->version = entry->version;
  record->bytes = entry->bytes;
  record->cipher_bytes = entry->cipher_bytes;
  record->updated_at_unix = entry->updated_at_unix;
  record->has_query_hidden = entry->has_query_hidden;
  record->query_hidden = entry->query_hidden;
  record->found = entry->found;
  if (entry->record_type == LC_POUCH_STATE_RECORD_OBJECT_PUT ||
      entry->record_type == LC_POUCH_STATE_RECORD_OBJECT_DELETE) {
    record->record_type = entry->found ? LC_POUCH_STATE_RECORD_OBJECT_PUT
                                       : LC_POUCH_STATE_RECORD_OBJECT_DELETE;
  } else if (entry->record_type == LC_POUCH_STATE_RECORD_STATE_META) {
    record->record_type = LC_POUCH_STATE_RECORD_STATE_META;
  } else {
    record->record_type = entry->found ? LC_POUCH_STATE_RECORD_STATE_PUT
                                       : LC_POUCH_STATE_RECORD_STATE_DELETE;
  }
  if (entry->index_seq > ns->max_version) {
    ns->max_version = entry->index_seq;
  }
  return LC_OK;
}

static int lc_pouch_state_entry_from_cache_record(
    lc_pouch *pouch, const lc_pouch_state_cache_record *record,
    lc_pouch_state_entry *out, lc_error *error) {
  memset(out, 0, sizeof(*out));
  if (record == NULL) {
    return LC_OK;
  }
  out->key = lc_strdup_with_allocator(&pouch->allocator, record->key);
  out->content_type =
      record->content_type != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, record->content_type)
          : NULL;
  out->etag = record->etag != NULL
                  ? lc_strdup_with_allocator(&pouch->allocator, record->etag)
                  : NULL;
  if (lc_pouch_state_payload_span_copy(&pouch->allocator, &record->payload_span,
                                       &out->payload_span, error) != LC_OK) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, out);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  if (lc_pouch_state_record_ref_copy(
          &pouch->allocator, record->record_container_leaf,
          record->record_offset, record->has_record_ref,
          &out->record_container_leaf, &out->record_offset,
          &out->has_record_ref, error) != LC_OK) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, out);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  out->payload_context =
      record->payload_context != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, record->payload_context)
          : NULL;
  out->descriptor =
      record->descriptor != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, record->descriptor)
          : NULL;
  if (record->metadata_length > 0U) {
    out->metadata = (unsigned char *)lc_alloc_with_allocator(
        &pouch->allocator, record->metadata_length);
    if (out->metadata != NULL) {
      memcpy(out->metadata, record->metadata, record->metadata_length);
      out->metadata_length = record->metadata_length;
    }
  }
  if (out->key == NULL ||
      (record->content_type != NULL && out->content_type == NULL) ||
      (record->etag != NULL && out->etag == NULL) ||
      (record->payload_span.present && !out->payload_span.present) ||
      (record->has_record_ref && !out->has_record_ref) ||
      (record->payload_context != NULL && out->payload_context == NULL) ||
      (record->descriptor != NULL && out->descriptor == NULL) ||
      (record->metadata_length > 0U && out->metadata == NULL)) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch state cache entry", NULL, NULL,
                        NULL);
  }
  out->index_seq = record->index_seq;
  out->version = record->version;
  out->bytes = record->bytes;
  out->cipher_bytes = record->cipher_bytes;
  out->updated_at_unix = record->updated_at_unix;
  out->has_query_hidden = record->has_query_hidden;
  out->query_hidden = record->query_hidden;
  out->seen = 1;
  out->found = record->found;
  out->record_type = record->record_type;
  return LC_OK;
}

/* The caller owns the cache lifetime and must not clean up this view. */
static void lc_pouch_state_entry_borrow_cache_record(
    const lc_pouch_state_cache_record *record, lc_pouch_state_entry *out) {
  memset(out, 0, sizeof(*out));
  if (record == NULL) {
    return;
  }
  out->key = record->key;
  out->content_type = record->content_type;
  out->etag = record->etag;
  out->payload_span = record->payload_span;
  out->record_container_leaf = record->record_container_leaf;
  out->record_offset = record->record_offset;
  out->has_record_ref = record->has_record_ref;
  out->payload_context = record->payload_context;
  out->descriptor = record->descriptor;
  out->metadata = record->metadata;
  out->metadata_length = record->metadata_length;
  out->index_seq = record->index_seq;
  out->version = record->version;
  out->bytes = record->bytes;
  out->cipher_bytes = record->cipher_bytes;
  out->updated_at_unix = record->updated_at_unix;
  out->has_query_hidden = record->has_query_hidden;
  out->query_hidden = record->query_hidden;
  out->seen = 1;
  out->found = record->found;
  out->record_type = record->record_type;
}

static char *lc_pouch_state_child_path(const lc_allocator *allocator,
                                       const char *namespace_path,
                                       const char *child, const char *leaf) {
  char *dir;
  char *path;

  dir = lc_pouch_path_join(allocator, namespace_path, child);
  path = dir != NULL ? lc_pouch_path_join(allocator, dir, leaf) : NULL;
  lc_free_with_allocator(allocator, dir);
  return path;
}

static char *lc_pouch_state_payload_span_path(
    const lc_allocator *allocator, const char *namespace_path,
    const lc_pouch_state_payload_span *span, lc_error *error) {
  const char *child;

  if (namespace_path == NULL || span == NULL || !span->present ||
      !lc_pouch_state_payload_container_is_valid(span->container_leaf)) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch payload span path requires a valid span", NULL,
                       NULL, "pouch");
    return NULL;
  }
  child = strncmp(span->container_leaf, "snapshot-", 9U) == 0 ? "snapshots"
                                                              : "segments";
  return lc_pouch_state_child_path(allocator, namespace_path, child,
                                   span->container_leaf);
}

static int lc_pouch_state_write_all(int fd, const void *bytes, size_t count,
                                    lc_error *error) {
  const unsigned char *cursor;

  cursor = (const unsigned char *)bytes;
  while (count > 0U) {
    ssize_t written;

    written = write(fd, cursor, count);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to write pouch state bytes", strerror(errno),
                          NULL, NULL);
    }
    if (written == 0) {
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "short write while writing pouch state bytes", NULL,
                          NULL, NULL);
    }
    cursor += written;
    count -= (size_t)written;
  }
  return LC_OK;
}

static int lc_pouch_state_compaction_throttle(lc_pouch *pouch, size_t bytes,
                                              lc_error *error) {
  struct timespec delay;
  struct timespec remaining;
  uint64_t rate;
  uint64_t seconds;
  uint64_t remainder;
  uint64_t nanoseconds;

  if (pouch == NULL || bytes == 0U ||
      pouch->compaction_max_io_bytes_per_sec == 0U) {
    return LC_OK;
  }
  rate = pouch->compaction_max_io_bytes_per_sec;
  seconds = (uint64_t)bytes / rate;
  remainder = (uint64_t)bytes % rate;
  if (rate <= LC_U64_MAX / 1000000000U) {
    nanoseconds = remainder * 1000000000U / rate;
  } else {
    nanoseconds = remainder / (rate / 1000000000U);
  }
  delay.tv_sec = (time_t)seconds;
  delay.tv_nsec = (long)nanoseconds;
  if (delay.tv_sec == 0 && delay.tv_nsec == 0L) {
    return LC_OK;
  }
  while (nanosleep(&delay, &remaining) != 0) {
    if (errno != EINTR) {
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to throttle pouch compaction",
                          strerror(errno), NULL, "pouch");
    }
    delay = remaining;
  }
  return LC_OK;
}

static int lc_pouch_state_copy_file_span_to_fd_crc(
    lc_pouch *pouch, const char *path, uint64_t offset, uint64_t length, int fd,
    unsigned long *stored_crc, lc_error *error) {
  unsigned char buffer[128U * 1024U];
  uint64_t remaining;
  int in_fd;
  int rc;

  if (path == NULL || stored_crc == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch span copy requires path and crc output", NULL,
                        NULL, "pouch");
  }
  if ((off_t)offset < 0 || (uint64_t)(off_t)offset != offset) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch span copy offset exceeds local limit", NULL,
                        NULL, "pouch");
  }
  in_fd = open(path, O_RDONLY);
  if (in_fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch payload span", strerror(errno),
                        path, "pouch");
  }
  if (lseek(in_fd, (off_t)offset, SEEK_SET) < 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to seek pouch payload span", strerror(errno),
                      path, "pouch");
    close(in_fd);
    return rc;
  }
  remaining = length;
  *stored_crc = (unsigned long)crc32(0L, Z_NULL, 0);
  rc = LC_OK;
  while (remaining > 0UL) {
    size_t want;
    ssize_t got;

    want = remaining < (uint64_t)sizeof(buffer) ? (size_t)remaining
                                                : sizeof(buffer);
    got = read(in_fd, buffer, want);
    if (got < 0) {
      if (errno == EINTR) {
        continue;
      }
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch payload span", strerror(errno),
                        path, "pouch");
      break;
    }
    if (got == 0) {
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch payload span is truncated", NULL, path, "pouch");
      break;
    }
    *stored_crc = (unsigned long)crc32((uLong)*stored_crc, buffer, (uInt)got);
    rc = lc_pouch_state_write_all(fd, buffer, (size_t)got, error);
    if (rc == LC_OK) {
      rc = lc_pouch_state_compaction_throttle(pouch, (size_t)got, error);
    }
    if (rc != LC_OK) {
      break;
    }
    remaining -= (uint64_t)got;
  }
  if (close(in_fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch payload span", strerror(errno),
                      path, "pouch");
  }
  return rc;
}

static void lc_pouch_state_put16(unsigned char *out, unsigned long value) {
  out[0] = (unsigned char)(value & 0xFFUL);
  out[1] = (unsigned char)((value >> 8U) & 0xFFUL);
}

static void lc_pouch_state_put32(unsigned char *out, unsigned long value) {
  out[0] = (unsigned char)(value & 0xFFUL);
  out[1] = (unsigned char)((value >> 8U) & 0xFFUL);
  out[2] = (unsigned char)((value >> 16U) & 0xFFUL);
  out[3] = (unsigned char)((value >> 24U) & 0xFFUL);
}

static void lc_pouch_state_put64(unsigned char *out, uint64_t value) {
  size_t i;

  for (i = 0U; i < 8U; ++i) {
    out[i] = (unsigned char)((value >> (i * 8U)) & 0xFFU);
  }
}

static unsigned long lc_pouch_state_get16(const unsigned char *in) {
  return (unsigned long)in[0] | ((unsigned long)in[1] << 8U);
}

static unsigned long lc_pouch_state_get32(const unsigned char *in) {
  return (unsigned long)in[0] | ((unsigned long)in[1] << 8U) |
         ((unsigned long)in[2] << 16U) | ((unsigned long)in[3] << 24U);
}

static uint64_t lc_pouch_state_get64(const unsigned char *in) {
  uint64_t value;
  size_t i;

  value = 0U;
  for (i = 0U; i < 8U; ++i) {
    value |= ((uint64_t)in[i]) << (i * 8U);
  }
  return value;
}

static int lc_pouch_state_decode_generation(uint64_t value,
                                            lc_pouch_generation *out,
                                            lc_error *error) {
  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch generation decode requires output", NULL, NULL,
                        "pouch");
  }
  *out = value;
  return LC_OK;
}

static int lc_pouch_state_fd_seek(int fd, uint64_t offset, const char *message,
                                  lc_error *error);

static lc_pouch_unix_seconds
lc_pouch_state_decode_unix_seconds(uint64_t value) {
  if (value <= (uint64_t)LC_I64_MAX) {
    return (lc_pouch_unix_seconds)value;
  }
  return -1 - (lc_pouch_unix_seconds)(LC_U64_MAX - value);
}

static int lc_pouch_state_record_header_encode(
    unsigned char type, const void *key, size_t key_len,
    const unsigned char *meta, size_t meta_len, uint64_t payload_len,
    unsigned long payload_crc, unsigned long flags,
    unsigned char encoded[LC_POUCH_STATE_RECORD_HEADER_BYTES],
    lc_error *error) {
  lc_pouch_record_header header;

  if ((key_len > 0U && key == NULL) || (meta_len > 0U && meta == NULL) ||
      key_len > 0xFFFFFFFFUL || meta_len > 0xFFFFFFFFUL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state record prefix is invalid", NULL, NULL,
                        "pouch");
  }
  memset(&header, 0, sizeof(header));
  header.type = type;
  header.flags = flags;
  header.key_len = (unsigned long)key_len;
  header.meta_len = (unsigned long)meta_len;
  header.payload_len = payload_len;
  header.payload_crc = payload_crc;
  lc_pouch_record_header_encode(&header, encoded);
  return LC_OK;
}

static int lc_pouch_state_record_write_header(
    int fd, unsigned char type, const void *key, size_t key_len,
    const unsigned char *meta, size_t meta_len, uint64_t payload_len,
    unsigned long payload_crc, unsigned long flags, lc_error *error) {
  unsigned char encoded[LC_POUCH_STATE_RECORD_HEADER_BYTES];
  int rc;

  rc = lc_pouch_state_record_header_encode(type, key, key_len, meta, meta_len,
                                           payload_len, payload_crc, flags,
                                           encoded, error);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_pouch_state_write_all(fd, encoded, sizeof(encoded), error);
}

static int lc_pouch_state_writev_all(int fd, const struct iovec *parts,
                                     int part_count, lc_error *error) {
  struct iovec pending[LC_POUCH_STATE_WRITEV_MAX_PARTS];
  int pending_count;
  int index;

  if (parts == NULL || part_count <= 0 ||
      part_count > LC_POUCH_STATE_WRITEV_MAX_PARTS) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch vectored state write requires valid parts", NULL,
                        NULL, "pouch");
  }
  for (index = 0; index < part_count; ++index) {
    pending[index] = parts[index];
  }
  index = 0;
  pending_count = part_count;
  while (pending_count > 0) {
    ssize_t written;
    size_t consumed;

    written = writev(fd, pending + index, pending_count);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to write pouch state record", strerror(errno),
                          NULL, "pouch");
    }
    if (written == 0) {
      errno = EIO;
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to write pouch state record", strerror(errno),
                          NULL, "pouch");
    }
    consumed = (size_t)written;
    while (pending_count > 0 && consumed >= pending[index].iov_len) {
      consumed -= pending[index].iov_len;
      ++index;
      --pending_count;
    }
    if (pending_count > 0 && consumed > 0U) {
      pending[index].iov_base = (char *)pending[index].iov_base + consumed;
      pending[index].iov_len -= consumed;
    }
  }
  return LC_OK;
}

static int lc_pouch_state_record_write_prefix(
    int fd, unsigned char type, const void *key, size_t key_len,
    const unsigned char *meta, size_t meta_len, uint64_t payload_len,
    unsigned long payload_crc, unsigned long flags, lc_error *error) {
  unsigned char encoded[LC_POUCH_STATE_RECORD_HEADER_BYTES];
  struct iovec parts[3];
  int part_count;
  int rc;

  rc = lc_pouch_state_record_header_encode(type, key, key_len, meta, meta_len,
                                           payload_len, payload_crc, flags,
                                           encoded, error);
  if (rc != LC_OK) {
    return rc;
  }
  part_count = 0;
  parts[part_count].iov_base = encoded;
  parts[part_count].iov_len = sizeof(encoded);
  ++part_count;
  if (key_len > 0U) {
    parts[part_count].iov_base = (void *)key;
    parts[part_count].iov_len = key_len;
    ++part_count;
  }
  if (meta_len > 0U) {
    parts[part_count].iov_base = (void *)meta;
    parts[part_count].iov_len = meta_len;
    ++part_count;
  }
  return lc_pouch_state_writev_all(fd, parts, part_count, error);
}

static int lc_pouch_state_record_write_complete(
    int fd, unsigned char type, const void *key, size_t key_len,
    const unsigned char *meta, size_t meta_len, const unsigned char *payload,
    size_t payload_len, unsigned long payload_crc, lc_error *error) {
  unsigned char encoded[LC_POUCH_STATE_RECORD_HEADER_BYTES];
  struct iovec parts[4];
  int part_count;
  int rc;

  if ((key_len > 0U && key == NULL) || (meta_len > 0U && meta == NULL) ||
      (payload_len > 0U && payload == NULL)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch inline state record is invalid", NULL, NULL,
                        "pouch");
  }
  rc = lc_pouch_state_record_header_encode(type, key, key_len, meta, meta_len,
                                           (uint64_t)payload_len, payload_crc,
                                           0UL, encoded, error);
  if (rc != LC_OK) {
    return rc;
  }
  part_count = 0;
  parts[part_count].iov_base = encoded;
  parts[part_count].iov_len = sizeof(encoded);
  ++part_count;
  if (key_len > 0U) {
    parts[part_count].iov_base = (void *)key;
    parts[part_count].iov_len = key_len;
    ++part_count;
  }
  if (meta_len > 0U) {
    parts[part_count].iov_base = (void *)meta;
    parts[part_count].iov_len = meta_len;
    ++part_count;
  }
  if (payload_len > 0U) {
    parts[part_count].iov_base = (void *)payload;
    parts[part_count].iov_len = payload_len;
    ++part_count;
  }
  return lc_pouch_state_writev_all(fd, parts, part_count, error);
}

/* Batch only already-encoded inline metadata records. A bounded aggregate
 * uses one contiguous append to avoid a syscall per five records; larger
 * batches retain the vectored fallback. Streaming payload records retain their
 * pending-header lifecycle and never enter either path. Fifteen iovecs stays
 * below the POSIX minimum IOV_MAX. */
static int lc_pouch_state_record_write_prefix_batch(
    int fd, const lc_pouch_state_binary_append_item *items, size_t item_count,
    lc_error *error) {
  unsigned char headers[LC_POUCH_STATE_WRITEV_BATCH_RECORDS]
                       [LC_POUCH_STATE_RECORD_HEADER_BYTES];
  struct iovec parts[LC_POUCH_STATE_WRITEV_MAX_PARTS];
  unsigned char *inline_bytes;
  size_t inline_length;
  size_t inline_offset;
  size_t item_size;
  size_t batch_count;
  size_t index;
  size_t item_index;
  int part_count;
  int rc;

  if (items == NULL || item_count == 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch vectored record batch requires items", NULL,
                        NULL, "pouch");
  }
  inline_bytes = NULL;
  inline_length = 0U;
  if (item_count > 1U) {
    for (index = 0U; index < item_count; ++index) {
      if (items[index].key_len >
              (size_t)-1 - LC_POUCH_STATE_RECORD_HEADER_BYTES ||
          items[index].meta_len > (size_t)-1 -
                                      LC_POUCH_STATE_RECORD_HEADER_BYTES -
                                      items[index].key_len) {
        inline_length = LC_POUCH_STATE_INLINE_APPEND_BATCH_MAX_BYTES + 1U;
        break;
      }
      item_size = LC_POUCH_STATE_RECORD_HEADER_BYTES + items[index].key_len +
                  items[index].meta_len;
      if (item_size >
          LC_POUCH_STATE_INLINE_APPEND_BATCH_MAX_BYTES - inline_length) {
        inline_length = LC_POUCH_STATE_INLINE_APPEND_BATCH_MAX_BYTES + 1U;
        break;
      }
      inline_length += item_size;
    }
  }
  if (inline_length > 0U &&
      inline_length <= LC_POUCH_STATE_INLINE_APPEND_BATCH_MAX_BYTES) {
    inline_bytes =
        (unsigned char *)lc_alloc_with_allocator(NULL, inline_length);
    if (inline_bytes != NULL) {
      inline_offset = 0U;
      for (index = 0U; index < item_count; ++index) {
        const lc_pouch_state_binary_append_item *item;

        item = &items[index];
        rc = lc_pouch_state_record_header_encode(
            item->record_type, item->key, item->key_len, item->meta,
            item->meta_len, 0U, 0UL, 0UL, inline_bytes + inline_offset, error);
        if (rc != LC_OK) {
          lc_free_with_allocator(NULL, inline_bytes);
          return rc;
        }
        inline_offset += LC_POUCH_STATE_RECORD_HEADER_BYTES;
        if (item->key_len > 0U) {
          memcpy(inline_bytes + inline_offset, item->key, item->key_len);
          inline_offset += item->key_len;
        }
        if (item->meta_len > 0U) {
          memcpy(inline_bytes + inline_offset, item->meta, item->meta_len);
          inline_offset += item->meta_len;
        }
      }
      rc = lc_pouch_state_write_all(fd, inline_bytes, inline_length, error);
      lc_free_with_allocator(NULL, inline_bytes);
      return rc;
    }
  }
  index = 0U;
  while (index < item_count) {
    batch_count = item_count - index;
    if (batch_count > LC_POUCH_STATE_WRITEV_BATCH_RECORDS) {
      batch_count = LC_POUCH_STATE_WRITEV_BATCH_RECORDS;
    }
    part_count = 0;
    for (item_index = 0U; item_index < batch_count; ++item_index) {
      const lc_pouch_state_binary_append_item *item;

      item = &items[index + item_index];
      rc = lc_pouch_state_record_header_encode(
          item->record_type, item->key, item->key_len, item->meta,
          item->meta_len, 0U, 0UL, 0UL, headers[item_index], error);
      if (rc != LC_OK) {
        return rc;
      }
      parts[part_count].iov_base = headers[item_index];
      parts[part_count].iov_len = LC_POUCH_STATE_RECORD_HEADER_BYTES;
      ++part_count;
      if (item->key_len > 0U) {
        parts[part_count].iov_base = (void *)item->key;
        parts[part_count].iov_len = item->key_len;
        ++part_count;
      }
      if (item->meta_len > 0U) {
        parts[part_count].iov_base = item->meta;
        parts[part_count].iov_len = item->meta_len;
        ++part_count;
      }
    }
    rc = lc_pouch_state_writev_all(fd, parts, part_count, error);
    if (rc != LC_OK) {
      return rc;
    }
    index += batch_count;
  }
  return LC_OK;
}

/* Publish the final header last so active-log readers only see complete data.
 */
static int lc_pouch_state_record_finalize(
    int fd, uint64_t record_offset, unsigned char type, const void *key,
    size_t key_len, const unsigned char *meta, size_t meta_len,
    uint64_t payload_len, unsigned long payload_crc, lc_error *error) {
  struct iovec parts[2];
  int part_count;
  int rc;

  if ((key_len > 0U && key == NULL) || (meta_len > 0U && meta == NULL) ||
      key_len > 0xFFFFFFFFUL || meta_len > 0xFFFFFFFFUL ||
      record_offset > LC_U64_MAX - LC_POUCH_STATE_RECORD_HEADER_BYTES) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state record finalization is invalid", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_state_fd_seek(
      fd, record_offset + LC_POUCH_STATE_RECORD_HEADER_BYTES,
      "failed to rewrite pouch state record metadata", error);
  part_count = 0;
  if (key_len > 0U) {
    parts[part_count].iov_base = (void *)key;
    parts[part_count].iov_len = key_len;
    ++part_count;
  }
  if (meta_len > 0U) {
    parts[part_count].iov_base = (void *)meta;
    parts[part_count].iov_len = meta_len;
    ++part_count;
  }
  if (rc == LC_OK && part_count > 0) {
    rc = lc_pouch_state_writev_all(fd, parts, part_count, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_fd_seek(fd, record_offset,
                                "failed to publish pouch state record header",
                                error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_record_write_header(fd, type, key, key_len, meta,
                                            meta_len, payload_len, payload_crc,
                                            0UL, error);
  }
  return rc;
}

static int lc_pouch_state_file_size(const char *path, uint64_t *size,
                                    lc_error *error) {
  struct stat st;

  if (stat(path, &st) != 0) {
    if (errno == ENOENT) {
      *size = 0U;
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to stat pouch state segment", strerror(errno),
                        NULL, NULL);
  }
  if (!S_ISREG(st.st_mode)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state segment path is not a file", NULL, NULL,
                        NULL);
  }
  if (st.st_size < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "pouch state segment size is invalid", NULL, path,
                        "pouch");
  }
  *size = (uint64_t)st.st_size;
  return LC_OK;
}

static int lc_pouch_state_file_checksum(const char *path,
                                        uint64_t expected_size,
                                        unsigned long *checksum,
                                        lc_error *error) {
  unsigned char buffer[65536];
  struct stat st;
  uint64_t read_total;
  unsigned long value;
  ssize_t got;
  int fd;

  if (path == NULL || checksum == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state checksum requires path and output", NULL,
                        NULL, NULL);
  }
  fd = open(path, O_RDONLY);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch compaction candidate",
                        strerror(errno), NULL, "pouch");
  }
  if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size < 0 ||
      (uint64_t)st.st_size != expected_size) {
    (void)close(fd);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch compaction candidate changed", NULL, NULL,
                        "pouch");
  }
  value = (unsigned long)crc32(0L, Z_NULL, 0);
  read_total = 0U;
  for (;;) {
    got = read(fd, buffer, sizeof(buffer));
    if (got < 0) {
      int saved_errno;

      saved_errno = errno;
      (void)close(fd);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to read pouch compaction candidate",
                          strerror(saved_errno), NULL, "pouch");
    }
    if (got == 0) {
      break;
    }
    if ((uint64_t)got > expected_size - read_total) {
      (void)close(fd);
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch compaction candidate changed", NULL, NULL,
                          "pouch");
    }
    value = (unsigned long)crc32((uLong)value, buffer, (uInt)got);
    read_total += (uint64_t)got;
  }
  if (fstat(fd, &st) != 0 || st.st_size < 0 ||
      (uint64_t)st.st_size != expected_size || read_total != expected_size) {
    (void)close(fd);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch compaction candidate changed", NULL, NULL,
                        "pouch");
  }
  if (close(fd) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch compaction candidate",
                        strerror(errno), NULL, "pouch");
  }
  *checksum = value;
  return LC_OK;
}

static int lc_pouch_state_fd_seek(int fd, uint64_t offset, const char *message,
                                  lc_error *error) {
  off_t target;

  target = (off_t)offset;
  if (target < 0 || (uint64_t)target != offset) {
    errno = EINVAL;
    return lc_error_set(error, LC_ERR_INVALID, 0L, message, strerror(errno),
                        NULL, "pouch");
  }
  if (lseek(fd, target, SEEK_SET) < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L, message, strerror(errno),
                        NULL, "pouch");
  }
  return LC_OK;
}

static int lc_pouch_state_fd_end(int fd, uint64_t *offset, const char *message,
                                 lc_error *error) {
  off_t end;

  if (offset == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch file end requires an output", NULL, NULL,
                        "pouch");
  }
  end = lseek(fd, 0, SEEK_END);
  if (end < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L, message, strerror(errno),
                        NULL, "pouch");
  }
  *offset = (uint64_t)end;
  return LC_OK;
}

static void lc_pouch_state_truncate_fd_best_effort(int fd, uint64_t size) {
  int ignored;

  if ((off_t)size < 0 || (uint64_t)(off_t)size != size) {
    return;
  }
  ignored = ftruncate(fd, (off_t)size);
  (void)ignored;
}

static void lc_pouch_state_append_fd_rollback(int fd, uint64_t size,
                                              int retain_fd) {
  lc_pouch_state_truncate_fd_best_effort(fd, size);
  if (retain_fd) {
    if ((off_t)size >= 0 && (uint64_t)(off_t)size == size) {
      (void)lseek(fd, (off_t)size, SEEK_SET);
    }
  } else {
    (void)close(fd);
  }
}

static void lc_pouch_state_truncate_path_best_effort(const char *path,
                                                     uint64_t size) {
  int ignored;

  if (path == NULL) {
    return;
  }
  if ((off_t)size < 0 || (uint64_t)(off_t)size != size) {
    return;
  }
  ignored = truncate(path, (off_t)size);
  (void)ignored;
}

static int lc_pouch_state_truncate_path(const char *path, uint64_t size,
                                        lc_error *error) {
  off_t offset;

  if (path == NULL || path[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch active-tail repair requires a path", NULL, NULL,
                        "pouch");
  }
  offset = (off_t)size;
  if (offset < 0 || (uint64_t)offset != size) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch active-tail repair exceeds local offset range",
                        NULL, NULL, "pouch");
  }
  if (truncate(path, offset) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to repair pouch active segment tail",
                        strerror(errno), NULL, "pouch");
  }
  return LC_OK;
}

static int lc_pouch_state_reserve_index_records(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_namespace_manifest *manifest, unsigned long count,
    lc_pouch_generation *first_index_out, lc_error *error) {
  char *lock_path;
  char *sequence_path;
  struct flock fl;
  char line[96];
  char end_index_text[32];
  FILE *fp;
  lc_u64 parsed;
  lc_pouch_generation base;
  lc_pouch_generation end_index;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_process_namespace_mutex *sequence_mutex;
  int fd;
  int scan_required;
  int rc;

  if (first_index_out != NULL) {
    *first_index_out = 0UL;
  }
  if (pouch == NULL || namespace_name == NULL || manifest == NULL ||
      manifest->namespace_path == NULL || count == 0UL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state index reservation requires context", NULL,
                        NULL, "pouch");
  }
  if (lc_pouch_single_writer_enabled(pouch)) {
    cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 0, NULL);
    if (cache != NULL && cache->initialized) {
      base = manifest->state_max_version;
      if (cache->max_version > base) {
        base = cache->max_version;
      }
      if (base > LC_U64_MAX - (uint64_t)count) {
        return lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch state index sequence overflow", NULL, NULL,
                            "pouch");
      }
      end_index = base + (uint64_t)count;
      cache->max_version = end_index;
      manifest->state_max_version = end_index;
      if (first_index_out != NULL) {
        *first_index_out = base + 1UL;
      }
      /* The exclusive owner has one resident allocator, so its projection is
       * authoritative while live. After close, abort, or recovery, replay
       * derives the high-water mark from finalized records. The sequence file
       * remains an advisory cross-process allocator for shared-root mode. */
      return LC_OK;
    }
  }
  sequence_mutex = NULL;
  rc = lc_pouch_state_process_namespace_mutex_lock(pouch, namespace_name,
                                                   &sequence_mutex, error);
  if (rc != LC_OK) {
    return rc;
  }
  lock_path = lc_pouch_path_join(&pouch->allocator, manifest->namespace_path,
                                 "sequence.lock");
  sequence_path = lc_pouch_path_join(&pouch->allocator,
                                     manifest->namespace_path, "sequence");
  if (lock_path == NULL || sequence_path == NULL) {
    lc_free_with_allocator(&pouch->allocator, lock_path);
    lc_free_with_allocator(&pouch->allocator, sequence_path);
    lc_pouch_state_process_namespace_mutex_unlock(&sequence_mutex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state sequence paths", NULL,
                        NULL, "pouch");
  }
  fd = open(lock_path, O_CREAT | O_RDWR, 0666);
  lc_free_with_allocator(&pouch->allocator, lock_path);
  if (fd < 0) {
    lc_free_with_allocator(&pouch->allocator, sequence_path);
    lc_pouch_state_process_namespace_mutex_unlock(&sequence_mutex);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch state sequence lock",
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
    lc_free_with_allocator(&pouch->allocator, sequence_path);
    lc_pouch_state_process_namespace_mutex_unlock(&sequence_mutex);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch state sequence", strerror(errno),
                        NULL, "pouch");
  }
  base = manifest->state_max_version;
  cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 0, NULL);
  scan_required = cache == NULL || !cache->initialized;
  if (cache != NULL && cache->initialized && cache->max_version > base) {
    base = cache->max_version;
  }
  fp = fopen(sequence_path, "rb");
  if (fp != NULL) {
    int sequence_valid;

    sequence_valid =
        fgets(line, sizeof(line), fp) != NULL && strncmp(line, "max=", 4U) == 0;
    if (sequence_valid) {
      sequence_valid = lc_parse_u64_base10_range_checked(
          line + 4U, strlen(line + 4U), &parsed);
    }
    if (fclose(fp) != 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch state sequence", strerror(errno),
                        NULL, "pouch");
      goto cleanup;
    }
    if (!sequence_valid) {
      /* The sequence file is advisory and may be torn after a crash. */
      scan_required = 1;
    } else if ((lc_pouch_generation)parsed > base) {
      base = (lc_pouch_generation)parsed;
    }
  } else if (errno != ENOENT) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to read pouch state sequence", strerror(errno),
                      NULL, "pouch");
    goto cleanup;
  } else {
    scan_required = 1;
  }
  if (scan_required) {
    lc_pouch_generation durable_max;

    rc = lc_pouch_state_manifest_max_version(pouch, manifest, &durable_max,
                                             error);
    if (rc != LC_OK) {
      goto cleanup;
    }
    if (durable_max > base) {
      base = durable_max;
    }
  }
  if (base > LC_U64_MAX - (uint64_t)count) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch state index sequence overflow", NULL, NULL,
                      "pouch");
    goto cleanup;
  }
  end_index = base + (uint64_t)count;
  if (lc_u64_format_base10((lc_u64)end_index, end_index_text,
                           sizeof(end_index_text)) < 0 ||
      snprintf(line, sizeof(line), "max=%s\n", end_index_text) < 0) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "failed to format pouch state sequence", NULL, NULL,
                      "pouch");
    goto cleanup;
  }
  {
    int sequence_fd;

    sequence_fd = open(sequence_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (sequence_fd < 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to write pouch state sequence", strerror(errno),
                        NULL, "pouch");
      goto cleanup;
    }
    rc = lc_pouch_state_write_all(sequence_fd, line, strlen(line), error);
    if (close(sequence_fd) != 0 && rc == LC_OK) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch state sequence", strerror(errno),
                        NULL, "pouch");
    }
    if (rc != LC_OK) {
      goto cleanup;
    }
  }
  manifest->state_max_version = end_index;
  if (first_index_out != NULL) {
    *first_index_out = base + 1UL;
  }
  rc = LC_OK;
cleanup:
  memset(&fl, 0, sizeof(fl));
  fl.l_type = F_UNLCK;
  fl.l_whence = SEEK_SET;
  (void)fcntl(fd, F_SETLK, &fl);
  (void)close(fd);
  lc_free_with_allocator(&pouch->allocator, sequence_path);
  lc_pouch_state_process_namespace_mutex_unlock(&sequence_mutex);
  return rc;
}

static int lc_pouch_state_append_binary_records_locked(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_namespace_manifest *manifest,
    lc_pouch_state_binary_append_item *items, size_t item_count,
    lc_error *error) {
  char *segment_path;
  lc_pouch_namespace_logstore *cache;
  int fd;
  int rc;
  int manifest_from_cache;
  int retain_active_append_fd;
  int single_writer;
  lc_pouch_generation first_index;
  uint64_t record_size;
  uint64_t segment_size;
  size_t index;

  if (pouch == NULL || namespace_name == NULL || manifest == NULL ||
      items == NULL || item_count == 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch binary record batch append requires inputs",
                        NULL, NULL, "pouch");
  }
  segment_path = NULL;
  cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 0, NULL);
  single_writer = lc_pouch_single_writer_enabled(pouch);
  manifest_from_cache =
      single_writer && cache != NULL && cache->initialized &&
      cache->active_segment_leaf != NULL && manifest->active_segment != NULL &&
      manifest->segment_leaves == NULL &&
      strcmp(cache->active_segment_leaf, manifest->active_segment) == 0;
  retain_active_append_fd = 0;
  first_index = 0UL;
  record_size = 0U;
  rc = LC_OK;
  for (index = 0U; index < item_count; ++index) {
    if ((items[index].key_len > 0U && items[index].key == NULL) ||
        (items[index].meta_len > 0U && items[index].meta == NULL) ||
        items[index].key_len > 0xFFFFFFFFUL ||
        items[index].meta_len > 0xFFFFFFFFUL) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch binary record batch item is invalid", NULL,
                          NULL, "pouch");
    }
    if (record_size > LC_U64_MAX - LC_POUCH_STATE_RECORD_HEADER_BYTES -
                          (uint64_t)items[index].key_len -
                          (uint64_t)items[index].meta_len) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch binary record batch exceeds local limit", NULL,
                          NULL, "pouch");
    }
    record_size += LC_POUCH_STATE_RECORD_HEADER_BYTES +
                   (uint64_t)items[index].key_len +
                   (uint64_t)items[index].meta_len;
  }
  if (rc == LC_OK && !manifest_from_cache) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, manifest);
    memset(manifest, 0, sizeof(*manifest));
    rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                          namespace_name, manifest, NULL, NULL,
                                          error);
  }
  if (rc == LC_OK && !manifest_from_cache) {
    segment_path =
        lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                  "segments", manifest->active_segment);
    if (segment_path == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state segment path", NULL,
                        NULL, "pouch");
    }
  }
  if (rc == LC_OK && !single_writer) {
    cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 1, error);
    if (cache == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    } else {
      /* Another shared writer may have committed after our precondition read.
       * Under append authority, advance only this projection's unseen tail so
       * the cursor remains authoritative for the next local mutation. */
      rc = lc_pouch_state_cache_refresh_for_mode(pouch, cache, manifest, error);
    }
  }
  if (rc == LC_OK &&
      !(single_writer && cache != NULL && cache->initialized &&
        cache->active_segment_leaf != NULL &&
        strcmp(cache->active_segment_leaf, manifest->active_segment) == 0)) {
    rc = lc_pouch_state_repair_active_tail_locked(
        pouch, namespace_name, manifest, segment_path, manifest->active_segment,
        error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_reserve_index_records(pouch, namespace_name, manifest,
                                              (unsigned long)item_count,
                                              &first_index, error);
  }
  for (index = 0U; index < item_count; ++index) {
    if (rc == LC_OK) {
      rc = lc_pouch_state_meta_set_index_seq(
          items[index].meta, items[index].meta_len,
          first_index + (lc_pouch_generation)index, error);
    }
  }
  if (rc == LC_OK) {
    if (cache != NULL && cache->initialized &&
        cache->active_segment_leaf != NULL &&
        strcmp(cache->active_segment_leaf, manifest->active_segment) == 0) {
      segment_size = cache->active_segment_offset;
    } else {
      rc = lc_pouch_state_file_size(segment_path, &segment_size, error);
    }
  }
  if (rc == LC_OK && segment_size > 0U &&
      (record_size > LC_U64_MAX - segment_size ||
       segment_size + record_size > pouch->segment_target_bytes)) {
    lc_free_with_allocator(&pouch->allocator, segment_path);
    segment_path = NULL;
    rc = lc_pouch_state_manifest_materialize(pouch, namespace_name, manifest,
                                             &manifest_from_cache, error);
    if (rc == LC_OK) {
      rc = lc_pouch_namespace_manifest_rotate(
          &pouch->allocator, namespace_name, manifest,
          manifest->active_segment_id + 1U, error);
    }
    if (rc == LC_OK) {
      segment_path =
          lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                    "segments", manifest->active_segment);
      if (segment_path == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state segment path", NULL,
                          NULL, "pouch");
      }
    }
    if (rc == LC_OK && cache != NULL && cache->initialized) {
      rc = lc_pouch_state_cache_set_active_segment(pouch, cache, manifest, 0U,
                                                   error);
    }
  }
  fd = -1;
  if (rc == LC_OK) {
    if (single_writer && cache != NULL && cache->initialized &&
        cache->active_segment_leaf != NULL &&
        strcmp(cache->active_segment_leaf, manifest->active_segment) == 0) {
      rc = lc_pouch_state_cache_active_append_fd(pouch, cache, manifest, &fd,
                                                 error);
      retain_active_append_fd = rc == LC_OK ? 1 : 0;
    } else {
      fd = open(segment_path, O_WRONLY | O_CREAT | O_APPEND, 0666);
    }
    if (rc == LC_OK && fd < 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch state segment", strerror(errno),
                        NULL, "pouch");
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_record_write_prefix_batch(fd, items, item_count, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_defer_fsync(pouch, fd, error);
  }
  if (!retain_active_append_fd && fd >= 0 && close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state segment", strerror(errno),
                      NULL, NULL);
  }
  if (rc == LC_OK && cache != NULL && cache->initialized &&
      cache->active_segment_leaf != NULL &&
      strcmp(cache->active_segment_leaf, manifest->active_segment) == 0) {
    cache->active_segment_offset = segment_size + record_size;
  }
  if (rc != LC_OK && retain_active_append_fd) {
    lc_pouch_state_append_fd_rollback(fd, segment_size, 1);
  }
  lc_free_with_allocator(&pouch->allocator, segment_path);
  return rc;
}

/* The physical append gate must precede the projection mutex. Every ordinary
 * caller enters under an exact-key mutation and therefore returns with that
 * mutex restored; namespace callbacks retain their stronger authority and use
 * the same in-process append gate recursively. */
static int
lc_pouch_state_append_binary_records(lc_pouch *pouch,
                                     const char *namespace_name,
                                     lc_pouch_namespace_manifest *manifest,
                                     lc_pouch_state_binary_append_item *items,
                                     size_t item_count, lc_error *error) {
  lc_pouch_state_append_lock append_lock;
  int rc;

  append_lock.fd = -1;
  append_lock.process_mutex = NULL;
  append_lock.exclusive_gate = NULL;
  rc = lc_pouch_state_append_lock_enter_after_mutation(pouch, namespace_name,
                                                       &append_lock, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_append_binary_records_locked(
        pouch, namespace_name, manifest, items, item_count, error);
  }
  lc_pouch_state_append_lock_release(&append_lock);
  return rc;
}

static int
lc_pouch_state_append_binary_record(lc_pouch *pouch, const char *namespace_name,
                                    lc_pouch_namespace_manifest *manifest,
                                    unsigned char record_type, const void *key,
                                    size_t key_len, unsigned char *meta,
                                    size_t meta_len, lc_error *error) {
  lc_pouch_state_binary_append_item item;

  memset(&item, 0, sizeof(item));
  item.record_type = record_type;
  item.key = key;
  item.key_len = key_len;
  item.meta = meta;
  item.meta_len = meta_len;
  return lc_pouch_state_append_binary_records(pouch, namespace_name, manifest,
                                              &item, 1U, error);
}

static char *lc_pouch_state_bytes_to_string(const lc_allocator *allocator,
                                            const unsigned char *bytes,
                                            size_t length, lc_error *error) {
  char *out;

  if (length > 0U && bytes == NULL) {
    (void)lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch state string bytes are invalid", NULL, NULL,
                       "pouch");
    return NULL;
  }
  if (length > 0U && memchr(bytes, '\0', length) != NULL) {
    (void)lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                       "pouch state metadata string contains NUL", NULL, NULL,
                       "pouch");
    return NULL;
  }
  out = (char *)lc_alloc_with_allocator(allocator, length + 1U);
  if (out == NULL) {
    (void)lc_error_set(error, LC_ERR_NOMEM, 0L,
                       "failed to allocate pouch state metadata string", NULL,
                       NULL, NULL);
    return NULL;
  }
  if (length > 0U) {
    memcpy(out, bytes, length);
  }
  out[length] = '\0';
  return out;
}

static int lc_pouch_state_meta_new(const lc_allocator *allocator, size_t length,
                                   unsigned char **out, lc_error *error) {
  *out = NULL;
  if (length > LC_POUCH_STATE_RECORD_META_MAX_BYTES) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state metadata exceeds local limit", NULL, NULL,
                        "pouch");
  }
  *out = (unsigned char *)lc_calloc_with_allocator(allocator, length, 1U);
  if (*out == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state metadata", NULL, NULL,
                        NULL);
  }
  return LC_OK;
}

static int lc_pouch_state_meta_set_index_seq(unsigned char *meta,
                                             size_t meta_len,
                                             lc_pouch_generation index_seq,
                                             lc_error *error) {
  size_t trailer_offset;

  if (meta == NULL || meta_len < LC_POUCH_STATE_INDEX_TRAILER_BYTES) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state metadata has no index trailer", NULL, NULL,
                        "pouch");
  }
  trailer_offset = meta_len - LC_POUCH_STATE_INDEX_TRAILER_BYTES;
  if (lc_pouch_state_get32(meta + trailer_offset) !=
      LC_POUCH_STATE_INDEX_TRAILER_MAGIC) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state metadata index trailer is invalid", NULL,
                        NULL, "pouch");
  }
  lc_pouch_state_put64(meta + trailer_offset + 4U, (uint64_t)index_seq);
  return LC_OK;
}

static int lc_pouch_state_meta_index_seq(const unsigned char *meta,
                                         size_t meta_len,
                                         lc_pouch_generation *out,
                                         lc_error *error) {
  size_t trailer_offset;
  int rc;

  if (meta == NULL || out == NULL ||
      meta_len < LC_POUCH_STATE_INDEX_TRAILER_BYTES) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state metadata index trailer is missing", NULL,
                        NULL, "pouch");
  }
  trailer_offset = meta_len - LC_POUCH_STATE_INDEX_TRAILER_BYTES;
  if (lc_pouch_state_get32(meta + trailer_offset) !=
      LC_POUCH_STATE_INDEX_TRAILER_MAGIC) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state metadata index trailer is invalid", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_state_decode_generation(
      lc_pouch_state_get64(meta + trailer_offset + 4U), out, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (*out == 0UL) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state metadata index sequence is invalid", NULL,
                        NULL, "pouch");
  }
  return LC_OK;
}

static int lc_pouch_state_encode_payload_meta(
    const lc_allocator *allocator, lc_pouch_generation version,
    lc_pouch_unix_seconds updated_at_unix, uint64_t plain_bytes,
    uint64_t stored_bytes, const char *content_type, const char *etag,
    const char *descriptor, const lc_pouch_state_payload_span *payload_span,
    const char *payload_context, const unsigned char *metadata,
    size_t metadata_length, size_t descriptor_reserve, int has_query_hidden,
    int query_hidden, unsigned char **out, size_t *out_length,
    lc_error *error) {
  char ref_container[64];
  uint64_t ref_record_offset;
  uint64_t ref_offset;
  uint64_t ref_length;
  unsigned long ref_crc;
  size_t content_type_len;
  size_t etag_len;
  size_t descriptor_len;
  size_t descriptor_capacity;
  size_t ref_container_len;
  size_t payload_context_len;
  size_t metadata_trailer_len;
  size_t length;
  unsigned char *meta;
  unsigned char *cursor;

  if (content_type == NULL || etag == NULL || out == NULL ||
      out_length == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state payload metadata requires inputs", NULL,
                        NULL, "pouch");
  }
  ref_container[0] = '\0';
  ref_record_offset = 0U;
  ref_offset = 0U;
  ref_length = 0U;
  ref_crc = 0UL;
  if (payload_span != NULL && payload_span->present) {
    if (!lc_pouch_state_payload_container_is_valid(
            payload_span->container_leaf)) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch state payload span is invalid", NULL, NULL,
                          "pouch");
    }
    if (strlen(payload_span->container_leaf) >= sizeof(ref_container)) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch state payload container exceeds local limit",
                          NULL, NULL, "pouch");
    }
    strcpy(ref_container, payload_span->container_leaf);
    ref_record_offset = payload_span->record_offset;
    ref_offset = payload_span->payload_offset;
    ref_length = payload_span->payload_length;
    ref_crc = payload_span->payload_crc;
  }
  content_type_len = strlen(content_type);
  etag_len = strlen(etag);
  descriptor_len = descriptor != NULL ? strlen(descriptor) : 0U;
  descriptor_capacity =
      descriptor_reserve > descriptor_len ? descriptor_reserve : descriptor_len;
  ref_container_len = ref_container[0] != '\0' ? strlen(ref_container) : 0U;
  payload_context_len = payload_context != NULL && payload_context[0] != '\0'
                            ? strlen(payload_context)
                            : 0U;
  if (metadata_length > 0U && metadata == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state side metadata requires bytes", NULL, NULL,
                        "pouch");
  }
  if (content_type_len > 0xFFFFU || etag_len > 0xFFFFU ||
      descriptor_capacity > 0xFFFFFFFFUL || ref_container_len > 0xFFFFU ||
      payload_context_len > 0xFFFFU || metadata_length > 0xFFFFFFFFUL ||
      ref_crc > 0xFFFFFFFFUL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state metadata field exceeds local limit", NULL,
                        NULL, "pouch");
  }
  metadata_trailer_len = 4U + metadata_length;
  length = LC_POUCH_STATE_PAYLOAD_META_FIXED_BYTES + content_type_len +
           etag_len + descriptor_capacity + ref_container_len +
           payload_context_len + metadata_trailer_len +
           LC_POUCH_STATE_INDEX_TRAILER_BYTES;
  if (lc_pouch_state_meta_new(allocator, length, &meta, error) != LC_OK) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  lc_pouch_state_put64(meta, (uint64_t)version);
  lc_pouch_state_put64(meta + 8, (uint64_t)updated_at_unix);
  lc_pouch_state_put64(meta + 16, plain_bytes);
  lc_pouch_state_put64(meta + 24, stored_bytes);
  lc_pouch_state_put64(meta + 32, ref_record_offset);
  lc_pouch_state_put64(meta + 40, ref_offset);
  lc_pouch_state_put64(meta + 48, ref_length);
  lc_pouch_state_put32(meta + 56, ref_crc);
  meta[60] = has_query_hidden ? 1U : 0U;
  if (query_hidden) {
    meta[60] |= 2U;
  }
  lc_pouch_state_put16(meta + 62, (unsigned long)content_type_len);
  lc_pouch_state_put16(meta + 64, (unsigned long)etag_len);
  lc_pouch_state_put32(meta + 66, (unsigned long)descriptor_len);
  lc_pouch_state_put16(meta + 70, (unsigned long)ref_container_len);
  lc_pouch_state_put16(meta + 72, (unsigned long)payload_context_len);
  cursor = meta + LC_POUCH_STATE_PAYLOAD_META_FIXED_BYTES;
  memcpy(cursor, content_type, content_type_len);
  cursor += content_type_len;
  memcpy(cursor, etag, etag_len);
  cursor += etag_len;
  if (descriptor_len > 0U) {
    memcpy(cursor, descriptor, descriptor_len);
  }
  cursor += descriptor_len;
  if (ref_container_len > 0U) {
    memcpy(cursor, ref_container, ref_container_len);
  }
  cursor += ref_container_len;
  if (payload_context_len > 0U) {
    memcpy(cursor, payload_context, payload_context_len);
  }
  cursor += payload_context_len;
  lc_pouch_state_put32(cursor, (unsigned long)metadata_length);
  cursor += 4U;
  if (metadata_length > 0U) {
    memcpy(cursor, metadata, metadata_length);
    cursor += metadata_length;
  }
  lc_pouch_state_put32(meta + length - LC_POUCH_STATE_INDEX_TRAILER_BYTES,
                       LC_POUCH_STATE_INDEX_TRAILER_MAGIC);
  lc_pouch_state_put64(meta + length - 8U, 0U);
  *out = meta;
  *out_length = length;
  return LC_OK;
}

static int lc_pouch_state_encode_delete_meta(
    const lc_allocator *allocator, lc_pouch_generation version,
    lc_pouch_unix_seconds updated_at_unix, const char *etag,
    unsigned char **out, size_t *out_length, lc_error *error) {
  size_t etag_len;
  unsigned char *meta;

  if (etag == NULL || out == NULL || out_length == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch delete metadata requires etag and outputs", NULL,
                        NULL, "pouch");
  }
  etag_len = strlen(etag);
  if (etag_len > 0xFFFFU) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch delete metadata etag exceeds local limit", NULL,
                        NULL, "pouch");
  }
  if (lc_pouch_state_meta_new(
          allocator, 18U + etag_len + LC_POUCH_STATE_INDEX_TRAILER_BYTES, &meta,
          error) != LC_OK) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  lc_pouch_state_put64(meta, (uint64_t)version);
  lc_pouch_state_put64(meta + 8, (uint64_t)updated_at_unix);
  lc_pouch_state_put16(meta + 16, (unsigned long)etag_len);
  memcpy(meta + 18, etag, etag_len);
  lc_pouch_state_put32(meta + 18U + etag_len,
                       LC_POUCH_STATE_INDEX_TRAILER_MAGIC);
  lc_pouch_state_put64(meta + 22U + etag_len, 0U);
  *out = meta;
  *out_length = 18U + etag_len + LC_POUCH_STATE_INDEX_TRAILER_BYTES;
  return LC_OK;
}

static int lc_pouch_state_encode_decision_meta(
    const lc_allocator *allocator, lc_pouch_generation version,
    const char *etag, const char *decision, unsigned char **out,
    size_t *out_length, lc_error *error) {
  size_t etag_len;
  unsigned char decision_code;
  unsigned char *meta;

  if (etag == NULL || decision == NULL || out == NULL || out_length == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch decision metadata requires inputs", NULL, NULL,
                        "pouch");
  }
  if (strcmp(decision, LC_POUCH_STATE_DECISION_COMMITTED) == 0) {
    decision_code = 1U;
  } else if (strcmp(decision, LC_POUCH_STATE_DECISION_DISCARDED) == 0) {
    decision_code = 2U;
  } else {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch decision metadata decision is invalid", NULL,
                        NULL, "pouch");
  }
  etag_len = strlen(etag);
  if (etag_len > 0xFFFFU) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch decision metadata etag exceeds local limit",
                        NULL, NULL, "pouch");
  }
  if (lc_pouch_state_meta_new(
          allocator, 19U + etag_len + LC_POUCH_STATE_INDEX_TRAILER_BYTES, &meta,
          error) != LC_OK) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  lc_pouch_state_put64(meta, (uint64_t)version);
  lc_pouch_state_put64(meta + 8, 0U);
  lc_pouch_state_put16(meta + 16, (unsigned long)etag_len);
  meta[18] = decision_code;
  memcpy(meta + 19, etag, etag_len);
  lc_pouch_state_put32(meta + 19U + etag_len,
                       LC_POUCH_STATE_INDEX_TRAILER_MAGIC);
  lc_pouch_state_put64(meta + 23U + etag_len, 0U);
  *out = meta;
  *out_length = 19U + etag_len + LC_POUCH_STATE_INDEX_TRAILER_BYTES;
  return LC_OK;
}

static int lc_pouch_state_encode_high_water_meta(const lc_allocator *allocator,
                                                 lc_pouch_generation version,
                                                 unsigned char **out,
                                                 size_t *out_length,
                                                 lc_error *error) {
  unsigned char *meta;

  if (out == NULL || out_length == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch high-water metadata requires outputs", NULL,
                        NULL, "pouch");
  }
  if (lc_pouch_state_meta_new(allocator, 8U, &meta, error) != LC_OK) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  lc_pouch_state_put64(meta, (uint64_t)version);
  *out = meta;
  *out_length = 8U;
  return LC_OK;
}

static int lc_pouch_state_decode_payload_meta(
    const lc_allocator *allocator, const unsigned char *key, size_t key_len,
    const unsigned char *meta, size_t meta_len, unsigned char record_type,
    const char *container_leaf, uint64_t record_offset, uint64_t payload_offset,
    uint64_t payload_length, unsigned long payload_crc,
    lc_pouch_state_entry *entry, lc_error *error) {
  lc_pouch_generation version;
  uint64_t plain_bytes;
  uint64_t stored_bytes;
  uint64_t ref_record_offset;
  uint64_t ref_offset;
  uint64_t ref_length;
  unsigned long ref_crc;
  unsigned long content_type_len;
  unsigned long etag_len;
  unsigned long descriptor_len;
  unsigned long ref_container_len;
  unsigned long payload_context_len;
  unsigned long side_metadata_len;
  unsigned long consumed_len;
  const unsigned char *cursor;
  char *ref_container;
  uint64_t value;
  int rc;

  version = 0UL;
  plain_bytes = 0U;
  stored_bytes = 0U;
  ref_record_offset = 0U;
  ref_offset = 0U;
  ref_length = 0U;
  ref_crc = 0UL;
  content_type_len = 0UL;
  etag_len = 0UL;
  descriptor_len = 0UL;
  ref_container_len = 0UL;
  payload_context_len = 0UL;
  side_metadata_len = 0UL;
  consumed_len = 0UL;
  ref_container = NULL;
  if (meta_len < LC_POUCH_STATE_PAYLOAD_META_FIXED_BYTES) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state metadata is truncated", NULL, NULL,
                        "pouch");
  }
  value = lc_pouch_state_get64(meta);
  rc = lc_pouch_state_decode_generation(value, &version, error);
  if (rc != LC_OK) {
    return rc;
  }
  plain_bytes = lc_pouch_state_get64(meta + 16);
  stored_bytes = lc_pouch_state_get64(meta + 24);
  ref_record_offset = lc_pouch_state_get64(meta + 32);
  ref_offset = lc_pouch_state_get64(meta + 40);
  ref_length = lc_pouch_state_get64(meta + 48);
  ref_crc = lc_pouch_state_get32(meta + 56);
  content_type_len = lc_pouch_state_get16(meta + 62);
  etag_len = lc_pouch_state_get16(meta + 64);
  descriptor_len = lc_pouch_state_get32(meta + 66);
  ref_container_len = lc_pouch_state_get16(meta + 70);
  payload_context_len = lc_pouch_state_get16(meta + 72);
  if ((uint64_t)LC_POUCH_STATE_PAYLOAD_META_FIXED_BYTES + content_type_len +
          etag_len + descriptor_len + ref_container_len + payload_context_len >
      (uint64_t)meta_len) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state metadata variable fields are truncated",
                        NULL, NULL, "pouch");
  }
  lc_pouch_state_entry_cleanup(allocator, entry);
  entry->key = lc_pouch_state_bytes_to_string(allocator, key, key_len, error);
  cursor = meta + LC_POUCH_STATE_PAYLOAD_META_FIXED_BYTES;
  entry->content_type = lc_pouch_state_bytes_to_string(
      allocator, cursor, (size_t)content_type_len, error);
  cursor += content_type_len;
  entry->etag = lc_pouch_state_bytes_to_string(allocator, cursor,
                                               (size_t)etag_len, error);
  cursor += etag_len;
  entry->descriptor =
      descriptor_len > 0UL
          ? lc_pouch_state_bytes_to_string(allocator, cursor,
                                           (size_t)descriptor_len, error)
          : NULL;
  cursor += descriptor_len;
  ref_container = NULL;
  ref_container = lc_pouch_state_bytes_to_string(
      allocator, cursor, (size_t)ref_container_len, error);
  if (ref_container == NULL) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state payload ref", NULL,
                        NULL, "pouch");
  }
  if (ref_container[0] == '\0' &&
      record_type != LC_POUCH_STATE_RECORD_STATE_PUT &&
      record_type != LC_POUCH_STATE_RECORD_OBJECT_PUT &&
      ref_record_offset == 0UL && ref_offset == 0UL && ref_length == 0UL &&
      ref_crc == 0UL && stored_bytes == 0UL) {
    cursor += ref_container_len;
    lc_free_with_allocator(allocator, ref_container);
    ref_container = NULL;
    goto payload_ref_decoded;
  }
  if (ref_container[0] == '\0' ||
      !lc_pouch_state_payload_container_is_valid(ref_container) ||
      ref_length != stored_bytes) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    lc_free_with_allocator(allocator, ref_container);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state payload ref metadata is invalid", NULL,
                        NULL, "pouch");
  }
  if (record_type == LC_POUCH_STATE_RECORD_STATE_PUT ||
      record_type == LC_POUCH_STATE_RECORD_OBJECT_PUT) {
    if (container_leaf == NULL || strcmp(container_leaf, ref_container) != 0 ||
        ref_record_offset != record_offset || payload_offset != ref_offset ||
        payload_length != ref_length || payload_length != stored_bytes ||
        payload_crc != ref_crc) {
      lc_pouch_state_entry_cleanup(allocator, entry);
      lc_free_with_allocator(allocator, ref_container);
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch state put payload metadata is invalid", NULL,
                          NULL, "pouch");
    }
    rc = lc_pouch_state_payload_span_set(
        allocator, &entry->payload_span, ref_container, ref_record_offset,
        ref_offset, ref_length, ref_crc, error);
    if (rc != LC_OK) {
      lc_pouch_state_entry_cleanup(allocator, entry);
      lc_free_with_allocator(allocator, ref_container);
      return rc;
    }
  } else {
    rc = lc_pouch_state_payload_span_set(
        allocator, &entry->payload_span, ref_container, ref_record_offset,
        ref_offset, ref_length, ref_crc, error);
    if (rc != LC_OK) {
      lc_pouch_state_entry_cleanup(allocator, entry);
      lc_free_with_allocator(allocator, ref_container);
      return rc;
    }
  }
  lc_free_with_allocator(allocator, ref_container);
  cursor += ref_container_len;
payload_ref_decoded:
  entry->payload_context =
      payload_context_len > 0UL
          ? lc_pouch_state_bytes_to_string(allocator, cursor,
                                           (size_t)payload_context_len, error)
          : NULL;
  cursor += payload_context_len;
  consumed_len = (unsigned long)(cursor - meta);
  if (meta_len > consumed_len) {
    if (meta_len - consumed_len < 4U) {
      lc_pouch_state_entry_cleanup(allocator, entry);
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch state side metadata is truncated", NULL, NULL,
                          "pouch");
    }
    side_metadata_len = lc_pouch_state_get32(cursor);
    cursor += 4U;
    consumed_len += 4U;
    if (side_metadata_len > meta_len - consumed_len) {
      lc_pouch_state_entry_cleanup(allocator, entry);
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch state side metadata payload is truncated",
                          NULL, NULL, "pouch");
    }
    if (side_metadata_len > 0UL) {
      entry->metadata = (unsigned char *)lc_alloc_with_allocator(
          allocator, (size_t)side_metadata_len);
      if (entry->metadata == NULL) {
        lc_pouch_state_entry_cleanup(allocator, entry);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch state side metadata",
                            NULL, NULL, NULL);
      }
      memcpy(entry->metadata, cursor, (size_t)side_metadata_len);
      entry->metadata_length = (size_t)side_metadata_len;
    }
    cursor += side_metadata_len;
    consumed_len += side_metadata_len;
  }
  if (entry->key == NULL || entry->content_type == NULL ||
      entry->etag == NULL ||
      (record_type != LC_POUCH_STATE_RECORD_STATE_META &&
       !entry->payload_span.present) ||
      (descriptor_len > 0UL && entry->descriptor == NULL) ||
      (payload_context_len > 0UL && entry->payload_context == NULL)) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_state_meta_index_seq(meta, meta_len, &entry->index_seq, error);
  if (rc != LC_OK) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    return rc;
  }
  entry->version = version;
  entry->bytes = plain_bytes;
  entry->cipher_bytes = stored_bytes;
  entry->updated_at_unix =
      lc_pouch_state_decode_unix_seconds(lc_pouch_state_get64(meta + 8));
  entry->has_query_hidden = (meta[60] & 1U) != 0;
  entry->query_hidden = (meta[60] & 2U) != 0;
  entry->seen = 1;
  entry->found = 1;
  entry->record_type = record_type;
  rc = lc_pouch_state_record_ref_set(
      allocator, &entry->record_container_leaf, &entry->record_offset,
      &entry->has_record_ref, container_leaf, record_offset, error);
  if (rc != LC_OK) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    return rc;
  }
  return LC_OK;
}

static int lc_pouch_state_decode_delete_meta(
    const lc_allocator *allocator, const unsigned char *key, size_t key_len,
    const unsigned char *meta, size_t meta_len, unsigned char record_type,
    const char *container_leaf, uint64_t record_offset,
    lc_pouch_state_entry *entry, lc_error *error) {
  lc_pouch_generation version;
  unsigned long etag_len;
  int rc;

  version = 0UL;
  etag_len = 0UL;
  if (meta_len < 18U) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state delete metadata is truncated", NULL, NULL,
                        "pouch");
  }
  rc = lc_pouch_state_decode_generation(lc_pouch_state_get64(meta), &version,
                                        error);
  if (rc != LC_OK) {
    return rc;
  }
  etag_len = lc_pouch_state_get16(meta + 16);
  if (18U + etag_len > meta_len) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state delete etag is truncated", NULL, NULL,
                        "pouch");
  }
  lc_pouch_state_entry_cleanup(allocator, entry);
  entry->key = lc_pouch_state_bytes_to_string(allocator, key, key_len, error);
  entry->etag =
      lc_pouch_state_bytes_to_string(allocator, meta + 18, etag_len, error);
  if (entry->key == NULL || entry->etag == NULL) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_state_meta_index_seq(meta, meta_len, &entry->index_seq, error);
  if (rc != LC_OK) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    return rc;
  }
  entry->version = version;
  entry->updated_at_unix =
      lc_pouch_state_decode_unix_seconds(lc_pouch_state_get64(meta + 8));
  entry->seen = 1;
  entry->found = 0;
  entry->record_type = record_type;
  rc = lc_pouch_state_record_ref_set(
      allocator, &entry->record_container_leaf, &entry->record_offset,
      &entry->has_record_ref, container_leaf, record_offset, error);
  if (rc != LC_OK) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    return rc;
  }
  return LC_OK;
}

static int lc_pouch_state_decode_decision_meta(
    const lc_allocator *allocator, const unsigned char *key, size_t key_len,
    const unsigned char *meta, size_t meta_len, lc_pouch_state_entry *entry,
    lc_error *error) {
  lc_pouch_generation version;
  unsigned long etag_len;
  const char *decision;
  int rc;

  version = 0UL;
  etag_len = 0UL;
  decision = NULL;
  if (meta_len < 19U) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state decision metadata is truncated", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_state_decode_generation(lc_pouch_state_get64(meta), &version,
                                        error);
  if (rc != LC_OK) {
    return rc;
  }
  etag_len = lc_pouch_state_get16(meta + 16);
  if (19U + etag_len > meta_len) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state decision etag is truncated", NULL, NULL,
                        "pouch");
  }
  if (meta[18] == 1U) {
    decision = LC_POUCH_STATE_DECISION_COMMITTED;
  } else if (meta[18] == 2U) {
    decision = LC_POUCH_STATE_DECISION_DISCARDED;
  } else {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state decision value is invalid", NULL, NULL,
                        "pouch");
  }
  lc_pouch_state_entry_cleanup(allocator, entry);
  entry->key = lc_pouch_state_bytes_to_string(allocator, key, key_len, error);
  entry->etag =
      lc_pouch_state_bytes_to_string(allocator, meta + 19, etag_len, error);
  entry->decision = lc_strdup_with_allocator(allocator, decision);
  if (entry->key == NULL || entry->etag == NULL || entry->decision == NULL) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_state_meta_index_seq(meta, meta_len, &entry->index_seq, error);
  if (rc != LC_OK) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    return rc;
  }
  entry->version = version;
  entry->seen = 1;
  entry->found = 0;
  entry->control = 1;
  entry->record_type = LC_POUCH_STATE_RECORD_DECISION;
  return LC_OK;
}

static int lc_pouch_state_decode_high_water_meta(const lc_allocator *allocator,
                                                 const unsigned char *meta,
                                                 size_t meta_len,
                                                 lc_pouch_state_entry *entry,
                                                 lc_error *error) {
  lc_pouch_generation version;
  int rc;

  version = 0UL;
  if (meta_len < 8U) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state high-water metadata is truncated", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_state_decode_generation(lc_pouch_state_get64(meta), &version,
                                        error);
  if (rc != LC_OK) {
    return rc;
  }
  lc_pouch_state_entry_cleanup(allocator, entry);
  entry->key =
      lc_strdup_with_allocator(allocator, LC_POUCH_STATE_HIGH_WATER_KEY);
  if (entry->key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to decode pouch high-water record", NULL, NULL,
                        NULL);
  }
  entry->version = version;
  entry->index_seq = version;
  entry->seen = 1;
  entry->found = 0;
  entry->control = 1;
  entry->record_type = LC_POUCH_STATE_RECORD_HIGH_WATER;
  return LC_OK;
}

static int lc_pouch_state_decode_record(
    const lc_allocator *allocator, const lc_pouch_record_header *header,
    const unsigned char *key, const unsigned char *meta,
    const char *container_leaf, uint64_t record_offset, uint64_t payload_offset,
    uint64_t payload_length, unsigned long payload_crc,
    lc_pouch_state_entry *entry, lc_error *error) {
  switch (header->type) {
  case LC_POUCH_STATE_RECORD_STATE_PUT:
  case LC_POUCH_STATE_RECORD_OBJECT_PUT:
  case LC_POUCH_STATE_RECORD_STATE_LINK:
  case LC_POUCH_STATE_RECORD_STATE_META:
    return lc_pouch_state_decode_payload_meta(
        allocator, key, header->key_len, meta, header->meta_len, header->type,
        container_leaf, record_offset, payload_offset, payload_length,
        payload_crc, entry, error);
  case LC_POUCH_STATE_RECORD_STATE_DELETE:
  case LC_POUCH_STATE_RECORD_OBJECT_DELETE:
    return lc_pouch_state_decode_delete_meta(
        allocator, key, header->key_len, meta, header->meta_len, header->type,
        container_leaf, record_offset, entry, error);
  case LC_POUCH_STATE_RECORD_DECISION:
    return lc_pouch_state_decode_decision_meta(
        allocator, key, header->key_len, meta, header->meta_len, entry, error);
  case LC_POUCH_STATE_RECORD_HIGH_WATER:
    return lc_pouch_state_decode_high_water_meta(
        allocator, meta, header->meta_len, entry, error);
  default:
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state segment record type is invalid", NULL,
                        NULL, "pouch");
  }
}

static int lc_pouch_state_read_next_record(
    FILE *fp, const lc_allocator *allocator, const char *container_leaf,
    int allow_truncated_tail, int verify_payload, uint64_t file_size,
    lc_pouch_state_entry *entry, int *found, int *truncated_tail,
    lc_error *error) {
  lc_pouch_record_header header;
  unsigned char encoded[LC_POUCH_STATE_RECORD_HEADER_BYTES];
  unsigned char *key;
  unsigned char *meta;
  unsigned char payload_buffer[16384];
  uint64_t remaining;
  uint64_t payload_offset;
  uint64_t payload_length;
  uint64_t record_start;
  unsigned long crc;
  size_t got;
  off_t position;
  int rc;

  key = NULL;
  meta = NULL;
  payload_offset = 0U;
  payload_length = 0U;
  record_start = 0U;
  crc = 0UL;
  if (fp == NULL || allocator == NULL || entry == NULL || found == NULL ||
      truncated_tail == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state record read requires inputs", NULL, NULL,
                        "pouch");
  }
  *found = 0;
  *truncated_tail = 0;
  position = ftello(fp);
  if (position < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch state record offset",
                        strerror(errno), NULL, "pouch");
  }
  record_start = (uint64_t)position;
  got = fread(encoded, 1U, sizeof(encoded), fp);
  if (got == 0U) {
    if (ferror(fp)) {
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to read pouch state segment", strerror(errno),
                          NULL, "pouch");
    }
    return LC_OK;
  }
  if (got != sizeof(encoded)) {
    if (allow_truncated_tail && !ferror(fp)) {
      *truncated_tail = 1;
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state segment record header is truncated", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_record_header_decode(encoded, &header, error);
  if (rc != LC_OK) {
    return rc;
  }
  if ((header.flags & ~LC_POUCH_RECORD_FLAG_PENDING) != 0UL) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state segment record flags are invalid", NULL,
                        NULL, "pouch");
  }
  if ((header.flags & LC_POUCH_RECORD_FLAG_PENDING) != 0UL) {
    if (allow_truncated_tail) {
      *truncated_tail = 1;
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state segment contains pending record", NULL,
                        NULL, "pouch");
  }
  if (header.key_len > LC_POUCH_STATE_RECORD_KEY_MAX_BYTES ||
      header.meta_len > LC_POUCH_STATE_RECORD_META_MAX_BYTES) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state segment record exceeds local limits", NULL,
                        NULL, "pouch");
  }
  if (header.key_len > 0UL) {
    key = (unsigned char *)lc_alloc_with_allocator(allocator, header.key_len);
    if (key == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state record key", NULL,
                          NULL, NULL);
    }
    got = fread(key, 1U, header.key_len, fp);
    if (got != header.key_len) {
      lc_free_with_allocator(allocator, key);
      if (allow_truncated_tail && !ferror(fp)) {
        *truncated_tail = 1;
        return LC_OK;
      }
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch state segment record key is truncated", NULL,
                          NULL, "pouch");
    }
  }
  if (header.meta_len > 0UL) {
    meta = (unsigned char *)lc_alloc_with_allocator(allocator, header.meta_len);
    if (meta == NULL) {
      lc_free_with_allocator(allocator, key);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state record metadata",
                          NULL, NULL, NULL);
    }
    got = fread(meta, 1U, header.meta_len, fp);
    if (got != header.meta_len) {
      lc_free_with_allocator(allocator, key);
      lc_free_with_allocator(allocator, meta);
      if (allow_truncated_tail && !ferror(fp)) {
        *truncated_tail = 1;
        return LC_OK;
      }
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch state segment record metadata is truncated",
                          NULL, NULL, "pouch");
    }
  }
  position = ftello(fp);
  if (position < 0) {
    lc_free_with_allocator(allocator, key);
    lc_free_with_allocator(allocator, meta);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch state payload offset",
                        strerror(errno), NULL, "pouch");
  }
  payload_offset = (uint64_t)position;
  payload_length = header.payload_len;
  if (payload_length > file_size ||
      payload_offset > file_size - payload_length) {
    lc_free_with_allocator(allocator, key);
    lc_free_with_allocator(allocator, meta);
    if (allow_truncated_tail) {
      /* The active file can grow after fstat but before this header read. */
      *truncated_tail = 1;
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state segment payload is truncated", NULL, NULL,
                        "pouch");
  }
  if (verify_payload) {
    crc = (unsigned long)crc32(0L, Z_NULL, 0);
    remaining = header.payload_len;
    while (remaining > 0U) {
      size_t want;

      want = remaining > sizeof(payload_buffer) ? sizeof(payload_buffer)
                                                : (size_t)remaining;
      got = fread(payload_buffer, 1U, want, fp);
      if (got != want) {
        lc_free_with_allocator(allocator, key);
        lc_free_with_allocator(allocator, meta);
        OPENSSL_cleanse(payload_buffer, sizeof(payload_buffer));
        return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                            "pouch state segment payload is truncated", NULL,
                            NULL, "pouch");
      }
      crc = (unsigned long)crc32((uLong)crc, payload_buffer, (uInt)got);
      remaining -= (uint64_t)got;
    }
    OPENSSL_cleanse(payload_buffer, sizeof(payload_buffer));
    if (crc != header.payload_crc) {
      lc_free_with_allocator(allocator, key);
      lc_free_with_allocator(allocator, meta);
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch state segment payload CRC mismatch", NULL,
                          NULL, "pouch");
    }
  } else if (payload_length > LC_U64_MAX - payload_offset ||
             (off_t)(payload_offset + payload_length) < 0 ||
             (uint64_t)(off_t)(payload_offset + payload_length) !=
                 payload_offset + payload_length ||
             fseeko(fp, (off_t)(payload_offset + payload_length), SEEK_SET) !=
                 0) {
    lc_free_with_allocator(allocator, key);
    lc_free_with_allocator(allocator, meta);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to skip pouch state payload", strerror(errno),
                        NULL, "pouch");
  }
  rc = lc_pouch_state_decode_record(
      allocator, &header, key, meta, container_leaf, record_start,
      payload_offset, payload_length, header.payload_crc, entry, error);
  lc_free_with_allocator(allocator, key);
  lc_free_with_allocator(allocator, meta);
  if (rc == LC_OK) {
    *found = 1;
  }
  return rc;
}

static void lc_pouch_state_log_tail_repair(lc_pouch *pouch, const char *reason,
                                           const char *segment,
                                           uint64_t offset) {
  pslog_field fields[3];

#ifdef LOCKDC_TEST_BUILD
  if (lc_pouch_test_tail_repair_hook != NULL) {
    lc_pouch_test_tail_repair_hook(lc_pouch_test_tail_repair_context, reason,
                                   segment);
  }
#endif

  fields[0] = lc_log_str_field("reason", reason);
  fields[1] = lc_log_str_field("segment", segment);
  fields[2] = lc_log_u64_field("record_offset", offset);
  lc_log_warn(pouch->logger, "logstore.tail.repair", fields, 3U);
}

static int
lc_pouch_state_scan_file(lc_pouch *pouch, const char *segment_path,
                         const char *container_leaf, int allow_truncated_tail,
                         int repair_truncated_tail, uint64_t start_offset,
                         const char *key, lc_pouch_state_entry *current,
                         lc_pouch_generation *max_version, lc_error *error) {
  lc_pouch_state_entry entry;
  FILE *fp;
  int found;
  int truncated_tail;
  int repair_tail;
  uint64_t repair_offset;
  uint64_t file_size;
  struct stat st;
  int rc;

  fp = fopen(segment_path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch state segment", strerror(errno),
                        NULL, NULL);
  }
  if (fstat(fileno(fp), &st) != 0 || st.st_size < 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to stat pouch state segment", strerror(errno),
                      NULL, "pouch");
    (void)fclose(fp);
    return rc;
  }
  file_size = (uint64_t)st.st_size;
  if (start_offset > file_size) {
    start_offset = 0U;
  }
  if (start_offset > 0U) {
    off_t offset;

    offset = (off_t)start_offset;
    if (offset < 0 || (uint64_t)offset != start_offset ||
        fseeko(fp, offset, SEEK_SET) != 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to seek pouch state active-tail repair offset",
                        strerror(errno), NULL, "pouch");
      (void)fclose(fp);
      return rc;
    }
  }

  memset(&entry, 0, sizeof(entry));
  rc = LC_OK;
  repair_tail = 0;
  repair_offset = 0U;
  for (;;) {
    off_t good_position;

    good_position = ftello(fp);
    if (good_position < 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch state scan offset",
                        strerror(errno), NULL, "pouch");
      break;
    }
    found = 0;
    truncated_tail = 0;
    rc = lc_pouch_state_read_next_record(
        fp, &pouch->allocator, container_leaf, allow_truncated_tail, 0,
        file_size, &entry, &found, &truncated_tail, error);
    if (rc != LC_OK) {
      break;
    }
    if (truncated_tail) {
      repair_tail = allow_truncated_tail && repair_truncated_tail;
      repair_offset = (uint64_t)good_position;
      break;
    }
    if (!found) {
      break;
    }
    if (!entry.seen) {
      continue;
    }
    if (max_version != NULL && entry.version > *max_version) {
      *max_version = entry.version;
    }
    if (!entry.control && key != NULL && strcmp(entry.key, key) == 0) {
      if (lc_pouch_state_entry_supersedes(&entry, current)) {
        lc_pouch_state_entry_cleanup(&pouch->allocator, current);
        *current = entry;
        memset(&entry, 0, sizeof(entry));
      } else {
        lc_pouch_state_entry_cleanup(&pouch->allocator, &entry);
      }
    } else {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &entry);
    }
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &entry);
  if (fclose(fp) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state segment", strerror(errno),
                      NULL, NULL);
  }
  if (rc == LC_OK && repair_tail) {
    rc = lc_pouch_state_truncate_path(segment_path, repair_offset, error);
    if (rc == LC_OK) {
      lc_pouch_state_log_tail_repair(pouch, "append", container_leaf,
                                     repair_offset);
    }
  }
  return rc;
}

static int lc_pouch_state_repair_active_tail_locked(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_namespace_manifest *manifest, const char *segment_path,
    const char *segment_leaf, lc_error *error) {
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_entry current;
  lc_pouch_generation max_version;
  uint64_t start_offset;
  int rc;

  if (pouch == NULL || namespace_name == NULL || manifest == NULL ||
      segment_path == NULL || segment_leaf == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch active-tail repair requires inputs", NULL, NULL,
                        "pouch");
  }
  start_offset = 0U;
  cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 0, NULL);
  if (cache != NULL && cache->initialized &&
      cache->active_segment_leaf != NULL &&
      strcmp(cache->active_segment_leaf, segment_leaf) == 0 &&
      lc_pouch_state_cache_matches_manifest(cache, manifest)) {
    start_offset = cache->active_segment_offset;
  }
  memset(&current, 0, sizeof(current));
  max_version = 0UL;
  rc = lc_pouch_state_scan_file(pouch, segment_path, segment_leaf, 1, 1,
                                start_offset, NULL, &current, &max_version,
                                error);
  lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  return rc;
}

static int lc_pouch_state_skip_file_bytes(FILE *fp, uint64_t length,
                                          lc_error *error) {
  while (length > 0U) {
    const uint64_t max_seek_chunk = 2147483647U;
    off_t chunk;

    chunk = (off_t)(length > max_seek_chunk ? max_seek_chunk : length);
    if (fseeko(fp, chunk, SEEK_CUR) != 0) {
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to skip pouch state record bytes",
                          strerror(errno), NULL, "pouch");
    }
    length -= (uint64_t)chunk;
  }
  return LC_OK;
}

static int lc_pouch_state_scan_file_max_version(
    lc_pouch *pouch, const char *segment_path, int allow_truncated_tail,
    lc_pouch_generation *max_version, lc_error *error) {
  FILE *fp;
  int rc;

  if (pouch == NULL || segment_path == NULL || max_version == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state sequence scan requires inputs", NULL, NULL,
                        "pouch");
  }
  fp = fopen(segment_path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch state sequence file",
                        strerror(errno), NULL, NULL);
  }
  rc = LC_OK;
  for (;;) {
    lc_pouch_record_header header;
    unsigned char encoded[LC_POUCH_STATE_RECORD_HEADER_BYTES];
    unsigned char version_meta[8];
    lc_pouch_generation version;
    size_t got;

    version = 0UL;
    got = fread(encoded, 1U, sizeof(encoded), fp);
    if (got == 0U) {
      if (ferror(fp)) {
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to read pouch state sequence file",
                          strerror(errno), NULL, "pouch");
      }
      break;
    }
    if (got != sizeof(encoded)) {
      if (allow_truncated_tail && !ferror(fp)) {
        break;
      }
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state sequence header is truncated", NULL, NULL,
                        "pouch");
      break;
    }
    rc = lc_pouch_record_header_decode(encoded, &header, error);
    if (rc != LC_OK) {
      break;
    }
    if ((header.flags & ~LC_POUCH_RECORD_FLAG_PENDING) != 0UL) {
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state sequence record flags are invalid", NULL,
                        NULL, "pouch");
      break;
    }
    if ((header.flags & LC_POUCH_RECORD_FLAG_PENDING) != 0UL) {
      if (allow_truncated_tail) {
        break;
      }
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state sequence contains pending record", NULL,
                        NULL, "pouch");
      break;
    }
    if (header.key_len > LC_POUCH_STATE_RECORD_KEY_MAX_BYTES ||
        header.meta_len > LC_POUCH_STATE_RECORD_META_MAX_BYTES) {
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state sequence record exceeds local limits",
                        NULL, NULL, "pouch");
      break;
    }
    switch (header.type) {
    case LC_POUCH_STATE_RECORD_STATE_PUT:
    case LC_POUCH_STATE_RECORD_OBJECT_PUT:
    case LC_POUCH_STATE_RECORD_STATE_DELETE:
    case LC_POUCH_STATE_RECORD_OBJECT_DELETE:
    case LC_POUCH_STATE_RECORD_STATE_LINK:
    case LC_POUCH_STATE_RECORD_STATE_META:
    case LC_POUCH_STATE_RECORD_DECISION:
    case LC_POUCH_STATE_RECORD_HIGH_WATER:
      break;
    default:
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state sequence record type is invalid", NULL,
                        NULL, "pouch");
      break;
    }
    if (rc != LC_OK) {
      break;
    }
    if (header.meta_len < sizeof(version_meta)) {
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state sequence metadata is truncated", NULL,
                        NULL, "pouch");
      break;
    }
    rc = lc_pouch_state_skip_file_bytes(fp, (uint64_t)header.key_len, error);
    if (rc != LC_OK) {
      break;
    }
    got = fread(version_meta, 1U, sizeof(version_meta), fp);
    if (got != sizeof(version_meta)) {
      if (allow_truncated_tail && !ferror(fp)) {
        break;
      }
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state sequence metadata is truncated", NULL,
                        NULL, "pouch");
      break;
    }
    rc = lc_pouch_state_decode_generation(lc_pouch_state_get64(version_meta),
                                          &version, error);
    if (rc != LC_OK) {
      break;
    }
    if (version > *max_version) {
      *max_version = version;
    }
    rc = lc_pouch_state_skip_file_bytes(
        fp, (uint64_t)(header.meta_len - sizeof(version_meta)), error);
    if (rc != LC_OK) {
      break;
    }
    rc = lc_pouch_state_skip_file_bytes(fp, header.payload_len, error);
    if (rc != LC_OK) {
      break;
    }
  }
  if (fclose(fp) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state sequence file",
                      strerror(errno), NULL, NULL);
  }
  return rc;
}

static int lc_pouch_state_manifest_max_version(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    lc_pouch_generation *max_version, lc_error *error) {
  unsigned long segment_index;
  int rc;

  if (pouch == NULL || manifest == NULL || max_version == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state sequence requires pouch, manifest, and "
                        "output",
                        NULL, NULL, "pouch");
  }
  *max_version = 0UL;
  rc = LC_OK;
  if (manifest->latest_snapshot != NULL) {
    char *snapshot_path;

    snapshot_path =
        lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                  "snapshots", manifest->latest_snapshot);
    if (snapshot_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state snapshot path", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_state_scan_file_max_version(pouch, snapshot_path, 0,
                                              max_version, error);
    lc_free_with_allocator(&pouch->allocator, snapshot_path);
    if (rc != LC_OK) {
      return rc;
    }
  }
  for (segment_index = 0UL; segment_index < manifest->segment_count;
       ++segment_index) {
    const char *segment_leaf;
    char *segment_path;
    int allow_truncated_tail;

    segment_leaf = manifest->segment_leaves[segment_index];
    if (!lc_pouch_state_manifest_segment_visible(manifest, segment_leaf)) {
      continue;
    }
    segment_path = lc_pouch_state_child_path(
        &pouch->allocator, manifest->namespace_path, "segments", segment_leaf);
    if (segment_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state segment path", NULL,
                          NULL, NULL);
    }
    allow_truncated_tail = lc_pouch_state_manifest_segment_allows_tail_repair(
        manifest, segment_leaf);
    rc = lc_pouch_state_scan_file_max_version(
        pouch, segment_path, allow_truncated_tail, max_version, error);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    if (rc != LC_OK) {
      break;
    }
  }
  return rc;
}

static int lc_pouch_state_scan(lc_pouch *pouch,
                               const lc_pouch_namespace_manifest *manifest,
                               const char *key, lc_pouch_state_entry *current,
                               lc_pouch_generation *max_version,
                               lc_error *error) {
  unsigned long segment_index;
  int rc;

  memset(current, 0, sizeof(*current));
  if (max_version != NULL) {
    *max_version = 0UL;
  }
  rc = LC_OK;
  if (manifest->latest_snapshot != NULL) {
    char *snapshot_path;

    snapshot_path =
        lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                  "snapshots", manifest->latest_snapshot);
    if (snapshot_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state snapshot path", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_state_scan_file(pouch, snapshot_path,
                                  manifest->latest_snapshot, 0, 0, 0U, key,
                                  current, max_version, error);
    lc_free_with_allocator(&pouch->allocator, snapshot_path);
    if (rc != LC_OK) {
      return rc;
    }
  }
  for (segment_index = 0UL; segment_index < manifest->segment_count;
       ++segment_index) {
    const char *segment_leaf;
    char *segment_path;

    segment_leaf = manifest->segment_leaves[segment_index];
    if (!lc_pouch_state_manifest_segment_visible(manifest, segment_leaf)) {
      continue;
    }
    segment_path = lc_pouch_state_child_path(
        &pouch->allocator, manifest->namespace_path, "segments", segment_leaf);
    if (segment_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state segment path", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_state_scan_file(
        pouch, segment_path, segment_leaf,
        lc_pouch_state_manifest_segment_allows_tail_repair(manifest,
                                                           segment_leaf),
        0, 0U, key, current, max_version, error);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    if (rc != LC_OK) {
      break;
    }
  }
  return rc;
}

static int lc_pouch_state_decision_apply(lc_pouch *pouch,
                                         lc_pouch_state_decision **decisions,
                                         const lc_pouch_state_entry *entry,
                                         lc_error *error) {
  lc_pouch_state_decision *decision;
  char *staged_key;
  char *etag;
  char *decision_value;

  if (entry == NULL || !entry->control || entry->key == NULL ||
      entry->etag == NULL || entry->decision == NULL) {
    return LC_OK;
  }
  for (decision = *decisions; decision != NULL; decision = decision->next) {
    if (strcmp(decision->staged_key, entry->key) != 0) {
      continue;
    }
    if (entry->version < decision->version ||
        (entry->version == decision->version &&
         entry->index_seq <= decision->index_seq)) {
      return LC_OK;
    }
    staged_key = lc_strdup_with_allocator(&pouch->allocator, entry->key);
    etag = lc_strdup_with_allocator(&pouch->allocator, entry->etag);
    decision_value =
        lc_strdup_with_allocator(&pouch->allocator, entry->decision);
    if (staged_key == NULL || etag == NULL || decision_value == NULL) {
      lc_free_with_allocator(&pouch->allocator, staged_key);
      lc_free_with_allocator(&pouch->allocator, etag);
      lc_free_with_allocator(&pouch->allocator, decision_value);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to copy pouch state decision", NULL, NULL,
                          NULL);
    }
    lc_free_with_allocator(&pouch->allocator, decision->staged_key);
    lc_free_with_allocator(&pouch->allocator, decision->etag);
    lc_free_with_allocator(&pouch->allocator, decision->decision);
    decision->staged_key = staged_key;
    decision->etag = etag;
    decision->decision = decision_value;
    decision->index_seq = entry->index_seq;
    decision->version = entry->version;
    return LC_OK;
  }

  decision = (lc_pouch_state_decision *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*decision));
  if (decision == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state decision", NULL, NULL,
                        NULL);
  }
  decision->staged_key =
      lc_strdup_with_allocator(&pouch->allocator, entry->key);
  decision->etag = lc_strdup_with_allocator(&pouch->allocator, entry->etag);
  decision->decision =
      lc_strdup_with_allocator(&pouch->allocator, entry->decision);
  if (decision->staged_key == NULL || decision->etag == NULL ||
      decision->decision == NULL) {
    lc_pouch_state_decision_cleanup(&pouch->allocator, decision);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch state decision", NULL, NULL,
                        NULL);
  }
  decision->index_seq = entry->index_seq;
  decision->version = entry->version;
  decision->next = *decisions;
  *decisions = decision;
  return LC_OK;
}

static int lc_pouch_state_collect_decisions_file(
    lc_pouch *pouch, const char *segment_path, const char *container_leaf,
    int allow_truncated_tail, lc_pouch_state_decision **decisions,
    lc_error *error) {
  lc_pouch_state_entry entry;
  FILE *fp;
  int found;
  int truncated_tail;
  uint64_t file_size;
  struct stat st;
  int rc;

  fp = fopen(segment_path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch state segment", strerror(errno),
                        NULL, NULL);
  }
  if (fstat(fileno(fp), &st) != 0 || st.st_size < 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to stat pouch state segment", strerror(errno),
                      NULL, "pouch");
    (void)fclose(fp);
    return rc;
  }
  file_size = (uint64_t)st.st_size;
  memset(&entry, 0, sizeof(entry));
  rc = LC_OK;
  for (;;) {
    found = 0;
    truncated_tail = 0;
    rc = lc_pouch_state_read_next_record(
        fp, &pouch->allocator, container_leaf, allow_truncated_tail, 0,
        file_size, &entry, &found, &truncated_tail, error);
    if (rc != LC_OK) {
      break;
    }
    if (truncated_tail) {
      break;
    }
    if (!found) {
      break;
    }
    rc = lc_pouch_state_decision_apply(pouch, decisions, &entry, error);
    if (rc != LC_OK) {
      break;
    }
    lc_pouch_state_entry_cleanup(&pouch->allocator, &entry);
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &entry);
  if (fclose(fp) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state segment", strerror(errno),
                      NULL, NULL);
  }
  return rc;
}

static int lc_pouch_state_collect_decisions(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    lc_pouch_state_decision **decisions, lc_error *error) {
  unsigned long segment_index;
  int rc;

  *decisions = NULL;
  rc = LC_OK;
  for (segment_index = 0UL; segment_index < manifest->segment_count;
       ++segment_index) {
    const char *segment_leaf;
    char *segment_path;

    segment_leaf = manifest->segment_leaves[segment_index];
    if (!lc_pouch_state_manifest_segment_visible(manifest, segment_leaf)) {
      continue;
    }
    segment_path = lc_pouch_state_child_path(
        &pouch->allocator, manifest->namespace_path, "segments", segment_leaf);
    if (segment_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state segment path", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_state_collect_decisions_file(
        pouch, segment_path, segment_leaf,
        lc_pouch_state_manifest_segment_allows_tail_repair(manifest,
                                                           segment_leaf),
        decisions, error);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    if (rc != LC_OK) {
      break;
    }
  }
  return rc;
}

static int lc_pouch_state_cache_replay_file(
    lc_pouch *pouch, lc_pouch_namespace_logstore *cache,
    const char *segment_path, const char *container_leaf,
    int allow_truncated_tail, uint64_t start_offset, uint64_t *end_offset_out,
    lc_error *error) {
  lc_pouch_state_entry entry;
  FILE *fp;
  int found;
  int truncated_tail;
  int truncated_tail_seen;
  int repair_tail;
  uint64_t repair_offset;
  uint64_t file_size;
  struct stat st;
  int rc;

  fp = fopen(segment_path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch state segment", strerror(errno),
                        NULL, NULL);
  }
  if (fstat(fileno(fp), &st) != 0 || st.st_size < 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to stat pouch state segment", strerror(errno),
                      NULL, "pouch");
    (void)fclose(fp);
    return rc;
  }
  file_size = (uint64_t)st.st_size;
  if (start_offset > file_size ||
      (start_offset > 0U && ((off_t)start_offset < 0 ||
                             (uint64_t)(off_t)start_offset != start_offset ||
                             fseeko(fp, (off_t)start_offset, SEEK_SET) != 0))) {
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                      "pouch active segment offset is invalid", NULL, NULL,
                      "pouch");
    (void)fclose(fp);
    return rc;
  }
  memset(&entry, 0, sizeof(entry));
  rc = LC_OK;
  repair_tail = 0;
  truncated_tail_seen = 0;
  repair_offset = 0U;
  for (;;) {
    off_t good_position;

    good_position = ftello(fp);
    if (good_position < 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch state replay offset",
                        strerror(errno), NULL, "pouch");
      break;
    }
    found = 0;
    truncated_tail = 0;
    rc = lc_pouch_state_read_next_record(
        fp, &pouch->allocator, container_leaf, allow_truncated_tail, 0,
        file_size, &entry, &found, &truncated_tail, error);
    if (rc != LC_OK) {
      break;
    }
    if (truncated_tail) {
      /* A reader must never repair the tail of an active writer's log. */
      repair_tail = 0;
      truncated_tail_seen = 1;
      repair_offset = (uint64_t)good_position;
      break;
    }
    if (!found) {
      break;
    }
    if (entry.seen && entry.index_seq > cache->max_version) {
      cache->max_version = entry.index_seq;
    }
    rc = lc_pouch_state_cache_apply_entry(pouch, cache, &entry, error);
    if (rc != LC_OK) {
      break;
    }
    lc_pouch_state_entry_cleanup(&pouch->allocator, &entry);
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &entry);
  if (fclose(fp) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state segment", strerror(errno),
                      NULL, NULL);
  }
  if (rc == LC_OK && repair_tail) {
    lc_pouch_state_truncate_path_best_effort(segment_path, repair_offset);
    lc_pouch_state_log_tail_repair(pouch, "replay", container_leaf,
                                   repair_offset);
  }
  if (rc == LC_OK && end_offset_out != NULL) {
    *end_offset_out = repair_offset;
    if (!truncated_tail_seen) {
      off_t end_position;

      end_position = (off_t)file_size;
      if (end_position < 0 || (uint64_t)end_position != file_size) {
        return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                            "pouch state segment exceeds local offset range",
                            NULL, NULL, "pouch");
      }
      *end_offset_out = (uint64_t)end_position;
    }
  }
  return rc;
}

static int lc_pouch_state_cache_warm_transformed_bodies(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_namespace_logstore *cache,
    const lc_pouch_namespace_manifest *manifest, lc_error *error) {
  lc_pouch_state_cache_record *record;
  int transformed;
  int rc;

  if (pouch == NULL || namespace_name == NULL || cache == NULL ||
      manifest == NULL || manifest->namespace_path == NULL) {
    return LC_OK;
  }
  transformed = lc_pouch_crypto_enabled(pouch->crypto) ||
                lc_pouch_crypto_compression_enabled(pouch->crypto);
  if (!transformed) {
    return LC_OK;
  }
  rc = LC_OK;
  for (record = cache->records; rc == LC_OK && record != NULL;
       record = record->next) {
    lc_pouch_state_entry current;
    lc_source *source;
    char *payload_span_path;
    char *crypto_context;

    if (!record->found || !record->payload_span.present ||
        (record->has_query_hidden && record->query_hidden) ||
        record->bytes > LC_POUCH_STATE_BODY_CACHE_RECORD_MAX_BYTES ||
        cache->body_cache_bytes >= LC_POUCH_STATE_BODY_CACHE_MAX_BYTES ||
        (record->body_cache != NULL &&
         record->body_cache->version == record->version &&
         record->body_cache->length == (size_t)record->bytes)) {
      continue;
    }
    payload_span_path = lc_pouch_state_payload_span_path(
        &pouch->allocator, manifest->namespace_path, &record->payload_span,
        error);
    crypto_context = payload_span_path != NULL
                         ? lc_pouch_state_payload_context_for_read(
                               &pouch->allocator, namespace_name, record->key,
                               record->payload_context)
                         : NULL;
    if (payload_span_path == NULL ||
        (record->descriptor != NULL && crypto_context == NULL)) {
      lc_free_with_allocator(&pouch->allocator, payload_span_path);
      lc_free_with_allocator(&pouch->allocator, crypto_context);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch transformed cache warm "
                          "context",
                          NULL, NULL, "pouch");
    }
    memset(&current, 0, sizeof(current));
    current.key = record->key;
    current.content_type = record->content_type;
    current.etag = record->etag;
    current.payload_span = record->payload_span;
    current.payload_context = record->payload_context;
    current.descriptor = record->descriptor;
    current.version = record->version;
    current.bytes = record->bytes;
    current.cipher_bytes = record->cipher_bytes;
    current.updated_at_unix = record->updated_at_unix;
    current.has_query_hidden = record->has_query_hidden;
    current.query_hidden = record->query_hidden;
    current.seen = 1;
    current.found = 1;
    source = NULL;
    rc = lc_pouch_state_read_many_snapshot_body_from_cache(
        pouch, cache, record, crypto_context, payload_span_path,
        record->payload_span.payload_offset,
        record->payload_span.payload_length, &current, 0, &source, error);
    if (source != NULL) {
      source->close(source);
    }
    lc_free_with_allocator(&pouch->allocator, crypto_context);
    lc_free_with_allocator(&pouch->allocator, payload_span_path);
  }
  return rc;
}

static int lc_pouch_state_cache_matches_manifest(
    const lc_pouch_namespace_logstore *cache,
    const lc_pouch_namespace_manifest *manifest) {
  if (cache == NULL || manifest == NULL || !cache->initialized ||
      cache->max_segment_id != manifest->max_segment_id ||
      cache->active_segment_leaf == NULL || manifest->active_segment == NULL ||
      strcmp(cache->active_segment_leaf, manifest->active_segment) != 0) {
    return 0;
  }
  if (cache->latest_snapshot_leaf == NULL ||
      manifest->latest_snapshot == NULL) {
    if (cache->latest_snapshot_leaf != NULL ||
        manifest->latest_snapshot != NULL) {
      return 0;
    }
  } else if (strcmp(cache->latest_snapshot_leaf, manifest->latest_snapshot) !=
             0) {
    return 0;
  }
  if (cache->segment_count == manifest->segment_count) {
    return 1;
  }
  /* A namespace starts with no physical active segment. Its first append makes
   * only the already-known active leaf visible to manifest discovery. The
   * cache can safely tail that leaf from its verified cursor; any other
   * topology change remains a full projection rebuild. */
  return cache->segment_count < ULONG_MAX &&
         manifest->segment_count == cache->segment_count + 1UL &&
         lc_pouch_state_manifest_has_segment(manifest,
                                             manifest->active_segment);
}

static int
lc_pouch_state_cache_tail_active(lc_pouch *pouch,
                                 lc_pouch_namespace_logstore *cache,
                                 const lc_pouch_namespace_manifest *manifest,
                                 int *rebuild, lc_error *error) {
  char *segment_path;
  uint64_t file_size;
  uint64_t end_offset;
  int rc;

  *rebuild = 0;
  segment_path =
      lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                "segments", manifest->active_segment);
  if (segment_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch active segment path", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_state_file_size(segment_path, &file_size, error);
  if (rc == LC_OK && file_size < cache->active_segment_offset) {
    *rebuild = 1;
  }
  if (rc == LC_OK && !*rebuild && file_size > cache->active_segment_offset) {
    end_offset = cache->active_segment_offset;
    rc = lc_pouch_state_cache_replay_file(
        pouch, cache, segment_path, manifest->active_segment, 1,
        cache->active_segment_offset, &end_offset, error);
    if (rc == LC_OK) {
      cache->active_segment_offset = end_offset;
    }
  }
  lc_free_with_allocator(&pouch->allocator, segment_path);
  if (rc != LC_OK || *rebuild) {
    return rc;
  }
  rc = lc_pouch_state_cache_warm_transformed_bodies(
      pouch, cache->namespace_name, cache, manifest, error);
  if (rc == LC_OK && manifest->state_max_version > cache->max_version) {
    cache->max_version = manifest->state_max_version;
  }
  return rc;
}

static int lc_pouch_state_cache_refresh(
    lc_pouch *pouch, lc_pouch_namespace_logstore *cache,
    const lc_pouch_namespace_manifest *manifest, int force, lc_error *error) {
  unsigned long segment_index;
  int rebuild;
  int rc;

  /* A writer-mode epoch change cannot reuse an exclusive append descriptor.
   * The projection remains valid until the normal manifest/tail validation
   * below says otherwise, so this does not turn a mode transition into a full
   * namespace replay. */
  if (force && cache->active_append_fd_owned && cache->active_append_fd >= 0) {
    (void)close(cache->active_append_fd);
    cache->active_append_fd = -1;
    cache->active_append_fd_owned = 0;
  }

  if (lc_pouch_state_cache_matches_manifest(cache, manifest)) {
    rc = lc_pouch_state_cache_tail_active(pouch, cache, manifest, &rebuild,
                                          error);
    if (rc != LC_OK || !rebuild) {
      return rc;
    }
  }
  lc_pouch_namespace_logstore_clear_records(&pouch->allocator, cache);
  cache->max_version = 0UL;
  cache->max_segment_id = 0UL;
  cache->segment_count = 0UL;
  rc = LC_OK;
  if (manifest->latest_snapshot != NULL) {
    char *snapshot_path;

    snapshot_path =
        lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                  "snapshots", manifest->latest_snapshot);
    if (snapshot_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state snapshot path", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_state_cache_replay_file(pouch, cache, snapshot_path,
                                          manifest->latest_snapshot, 0, 0U,
                                          NULL, error);
    lc_free_with_allocator(&pouch->allocator, snapshot_path);
    if (rc != LC_OK) {
      return rc;
    }
  }
  for (segment_index = 0UL; segment_index < manifest->segment_count;
       ++segment_index) {
    const char *segment_leaf;
    char *segment_path;
    uint64_t *end_offset;

    segment_leaf = manifest->segment_leaves[segment_index];
    if (!lc_pouch_state_manifest_segment_visible(manifest, segment_leaf)) {
      continue;
    }
    segment_path = lc_pouch_state_child_path(
        &pouch->allocator, manifest->namespace_path, "segments", segment_leaf);
    if (segment_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state segment path", NULL,
                          NULL, NULL);
    }
    end_offset = strcmp(segment_leaf, manifest->active_segment) == 0
                     ? &cache->active_segment_offset
                     : NULL;
    rc = lc_pouch_state_cache_replay_file(
        pouch, cache, segment_path, segment_leaf,
        lc_pouch_state_manifest_segment_allows_tail_repair(manifest,
                                                           segment_leaf),
        0U, end_offset, error);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    if (rc != LC_OK) {
      break;
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_cache_warm_transformed_bodies(
        pouch, cache->namespace_name, cache, manifest, error);
  }
  if (rc == LC_OK) {
    cache->namespace_path =
        lc_strdup_with_allocator(&pouch->allocator, manifest->namespace_path);
    cache->active_segment_leaf =
        lc_strdup_with_allocator(&pouch->allocator, manifest->active_segment);
    cache->latest_snapshot_leaf =
        manifest->latest_snapshot != NULL
            ? lc_strdup_with_allocator(&pouch->allocator,
                                       manifest->latest_snapshot)
            : NULL;
    if (cache->namespace_path == NULL || cache->active_segment_leaf == NULL ||
        (manifest->latest_snapshot != NULL &&
         cache->latest_snapshot_leaf == NULL)) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to retain pouch cache manifest state", NULL,
                        NULL, "pouch");
    }
  }
  if (rc == LC_OK) {
    cache->active_segment_id = manifest->active_segment_id;
    cache->max_segment_id = manifest->max_segment_id;
    cache->segment_count = manifest->segment_count;
    if (manifest->state_max_version > cache->max_version) {
      cache->max_version = manifest->state_max_version;
    }
    cache->initialized = 1;
  }
  return rc;
}

static int lc_pouch_state_cache_refresh_for_mode(
    lc_pouch *pouch, lc_pouch_namespace_logstore *cache,
    const lc_pouch_namespace_manifest *manifest, lc_error *error) {
  uint64_t mode_epoch;
  int force_refresh;
  int single_writer;
  int rc;

  if (pouch == NULL || cache == NULL || manifest == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch cache mode refresh requires inputs", NULL, NULL,
                        "pouch");
  }
  single_writer = lc_pouch_single_writer_snapshot(pouch, &mode_epoch);
  force_refresh = cache->writer_mode_epoch != mode_epoch;
  if (single_writer && cache->initialized && !force_refresh) {
    return LC_OK;
  }
  rc = lc_pouch_state_cache_refresh(pouch, cache, manifest, force_refresh,
                                    error);
  if (rc == LC_OK) {
    cache->writer_mode_epoch = mode_epoch;
  }
  return rc;
}

static int lc_pouch_state_cache_set_active_segment(
    lc_pouch *pouch, lc_pouch_namespace_logstore *cache,
    const lc_pouch_namespace_manifest *manifest, uint64_t offset,
    lc_error *error) {
  char *active_segment_leaf;

  if (pouch == NULL || cache == NULL || manifest == NULL ||
      manifest->active_segment == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch cache active segment requires context", NULL,
                        NULL, "pouch");
  }
  active_segment_leaf =
      lc_strdup_with_allocator(&pouch->allocator, manifest->active_segment);
  if (active_segment_leaf == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to retain pouch active segment", NULL, NULL,
                        "pouch");
  }
  if (cache->active_append_fd_owned && cache->active_append_fd >= 0) {
    (void)close(cache->active_append_fd);
  }
  cache->active_append_fd = -1;
  cache->active_append_fd_owned = 0;
  lc_free_with_allocator(&pouch->allocator, cache->active_segment_leaf);
  cache->active_segment_leaf = active_segment_leaf;
  cache->active_segment_offset = offset;
  cache->active_segment_id = manifest->active_segment_id;
  cache->max_segment_id = manifest->max_segment_id;
  cache->segment_count = manifest->segment_count;
  return LC_OK;
}

static int lc_pouch_state_cache_active_append_fd(
    lc_pouch *pouch, lc_pouch_namespace_logstore *cache,
    const lc_pouch_namespace_manifest *manifest, int *out, lc_error *error) {
  char *segment_path;
  int fd;
  int rc;

  if (pouch == NULL || cache == NULL || manifest == NULL || out == NULL ||
      !cache->initialized || cache->active_segment_leaf == NULL ||
      manifest->active_segment == NULL ||
      strcmp(cache->active_segment_leaf, manifest->active_segment) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch active append cache is not current", NULL, NULL,
                        "pouch");
  }
  if (cache->active_append_fd_owned) {
    rc = lc_pouch_state_fd_seek(
        cache->active_append_fd, cache->active_segment_offset,
        "failed to seek pouch cached state segment append offset", error);
    if (rc != LC_OK) {
      (void)close(cache->active_append_fd);
      cache->active_append_fd = -1;
      cache->active_append_fd_owned = 0;
      return rc;
    }
    *out = cache->active_append_fd;
    return LC_OK;
  }
  segment_path =
      lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                "segments", manifest->active_segment);
  if (segment_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch active segment path", NULL,
                        NULL, "pouch");
  }
  fd = open(segment_path, O_RDWR | O_CREAT, 0666);
  lc_free_with_allocator(&pouch->allocator, segment_path);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch cached state segment",
                        strerror(errno), NULL, "pouch");
  }
  rc = lc_pouch_state_fd_seek(fd, cache->active_segment_offset,
                              "failed to seek pouch cached state segment "
                              "append offset",
                              error);
  if (rc != LC_OK) {
    (void)close(fd);
    return rc;
  }
  cache->active_append_fd = fd;
  cache->active_append_fd_owned = 1;
  *out = fd;
  return LC_OK;
}

/* The caller holds the namespace projection and append gate. The resident
 * fields therefore remain stable for the lifetime of this read-only view.
 * Durable manifest changes must first call manifest_materialize(), which
 * replaces the view with an owned manifest before any mutation. */
static int lc_pouch_state_cache_manifest_borrow(
    lc_pouch *pouch, const lc_pouch_namespace_logstore *cache,
    lc_pouch_namespace_manifest *manifest, lc_error *error) {
  if (pouch == NULL || cache == NULL || manifest == NULL ||
      !cache->initialized || cache->namespace_path == NULL ||
      cache->active_segment_leaf == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch cache manifest is not current", NULL, NULL,
                        "pouch");
  }
  (void)pouch;
  memset(manifest, 0, sizeof(*manifest));
  manifest->namespace_path = cache->namespace_path;
  manifest->active_segment = cache->active_segment_leaf;
  manifest->active_segment_id = cache->active_segment_id;
  manifest->max_segment_id = cache->max_segment_id;
  manifest->segment_count = cache->segment_count;
  manifest->state_max_version = cache->max_version;
  manifest->borrowed = 1;
  return LC_OK;
}

/* Public read paths can release projection coordination before consuming their
 * result. Keep their manifest independent of the resident owner. */
static int lc_pouch_state_cache_manifest_copy(
    lc_pouch *pouch, const lc_pouch_namespace_logstore *cache,
    lc_pouch_namespace_manifest *manifest, lc_error *error) {
  if (pouch == NULL || cache == NULL || manifest == NULL ||
      !cache->initialized || cache->namespace_path == NULL ||
      cache->active_segment_leaf == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch cache manifest is not current", NULL, NULL,
                        "pouch");
  }
  memset(manifest, 0, sizeof(*manifest));
  manifest->namespace_path =
      lc_strdup_with_allocator(&pouch->allocator, cache->namespace_path);
  manifest->active_segment =
      lc_strdup_with_allocator(&pouch->allocator, cache->active_segment_leaf);
  if (manifest->namespace_path == NULL || manifest->active_segment == NULL) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, manifest);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch cache manifest", NULL, NULL,
                        "pouch");
  }
  manifest->active_segment_id = cache->active_segment_id;
  manifest->max_segment_id = cache->max_segment_id;
  manifest->segment_count = cache->segment_count;
  manifest->state_max_version = cache->max_version;
  return LC_OK;
}

/* Metadata batches materialize their own append manifest. While exclusive
 * mutation authority is held, the resident record can therefore be borrowed
 * directly instead of allocating an otherwise-unused manifest copy. */
static int lc_pouch_state_cache_borrow_exclusive_record(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    lc_pouch_state_entry *current) {
  lc_pouch_namespace_logstore *cache;
  uint64_t writer_mode_epoch;

  if (pouch == NULL || namespace_name == NULL || key == NULL ||
      current == NULL ||
      !lc_pouch_single_writer_snapshot(pouch, &writer_mode_epoch)) {
    return 0;
  }
  cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 0, NULL);
  if (cache == NULL || !cache->initialized ||
      cache->writer_mode_epoch != writer_mode_epoch ||
      cache->namespace_path == NULL || cache->active_segment_leaf == NULL) {
    return 0;
  }
  lc_pouch_state_entry_borrow_cache_record(
      lc_pouch_state_cache_record_find(cache, key), current);
  return 1;
}

static int
lc_pouch_state_manifest_materialize(lc_pouch *pouch, const char *namespace_name,
                                    lc_pouch_namespace_manifest *manifest,
                                    int *from_cache, lc_error *error) {
  lc_pouch_namespace_manifest loaded;
  int rc;

  if (pouch == NULL || namespace_name == NULL || manifest == NULL ||
      from_cache == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch manifest materialization requires context", NULL,
                        NULL, "pouch");
  }
  if (!*from_cache) {
    return LC_OK;
  }
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, manifest);
  memset(&loaded, 0, sizeof(loaded));
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                        namespace_name, &loaded, NULL, NULL,
                                        error);
  if (rc != LC_OK) {
    return rc;
  }
  *manifest = loaded;
  *from_cache = 0;
  return LC_OK;
}

static int lc_pouch_state_manifest_lookup_cached(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    lc_pouch_namespace_manifest *manifest, lc_pouch_state_entry *current,
    lc_pouch_generation *max_version_out, lc_error *error) {
  lc_pouch_namespace_logstore *cache;
  uint64_t writer_mode_epoch;
  int single_writer;
  int rc;

  if (pouch == NULL || namespace_name == NULL || key == NULL ||
      manifest == NULL || current == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch cached manifest lookup requires context", NULL,
                        NULL, "pouch");
  }
  memset(manifest, 0, sizeof(*manifest));
  memset(current, 0, sizeof(*current));
  cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 0, NULL);
  single_writer = lc_pouch_single_writer_snapshot(pouch, &writer_mode_epoch);
  if (single_writer && cache != NULL && cache->initialized &&
      cache->writer_mode_epoch == writer_mode_epoch &&
      cache->namespace_path != NULL && cache->active_segment_leaf != NULL) {
    rc = lc_pouch_state_cache_manifest_copy(pouch, cache, manifest, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (max_version_out != NULL) {
      *max_version_out = cache->max_version;
    }
    return lc_pouch_state_entry_from_cache_record(
        pouch, lc_pouch_state_cache_record_find(cache, key), current, error);
  }
  rc = lc_pouch_namespace_ensure(&pouch->allocator, pouch->root_path,
                                 namespace_name, error);
  if (rc == LC_OK) {
    rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                          namespace_name, manifest, NULL, NULL,
                                          error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_cache_lookup(pouch, namespace_name, manifest, key,
                                     current, max_version_out, error);
  }
  if (rc != LC_OK) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, manifest);
  }
  return rc;
}

/* Returns a borrowed record only while exclusive mutation serialization holds.
 */
static int lc_pouch_state_manifest_lookup_cached_borrowed(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    lc_pouch_namespace_manifest *manifest, lc_pouch_state_entry *current,
    lc_pouch_generation *max_version_out, int *current_is_borrowed,
    lc_error *error) {
  lc_pouch_namespace_logstore *cache;
  uint64_t writer_mode_epoch;
  int single_writer;
  int rc;

  if (pouch == NULL || namespace_name == NULL || key == NULL ||
      manifest == NULL || current == NULL || current_is_borrowed == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch borrowed manifest lookup requires context", NULL,
                        NULL, "pouch");
  }
  *current_is_borrowed = 0;
  cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 0, NULL);
  single_writer = lc_pouch_single_writer_snapshot(pouch, &writer_mode_epoch);
  if (single_writer && cache != NULL && cache->initialized &&
      cache->writer_mode_epoch == writer_mode_epoch &&
      cache->namespace_path != NULL && cache->active_segment_leaf != NULL) {
    rc = lc_pouch_state_cache_manifest_borrow(pouch, cache, manifest, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (max_version_out != NULL) {
      *max_version_out = cache->max_version;
    }
    lc_pouch_state_entry_borrow_cache_record(
        lc_pouch_state_cache_record_find(cache, key), current);
    *current_is_borrowed = 1;
    return LC_OK;
  }
  return lc_pouch_state_manifest_lookup_cached(
      pouch, namespace_name, key, manifest, current, max_version_out, error);
}

/* Locked mutation callers retain their existing owned record cleanup, but do
 * not need to allocate a manifest copy while the resident owner is stable. */
static int lc_pouch_state_manifest_lookup_cached_for_mutation(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    lc_pouch_namespace_manifest *manifest, lc_pouch_state_entry *current,
    lc_pouch_generation *max_version_out, lc_error *error) {
  lc_pouch_namespace_logstore *cache;
  uint64_t writer_mode_epoch;
  int single_writer;
  int rc;

  if (pouch == NULL || namespace_name == NULL || key == NULL ||
      manifest == NULL || current == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch mutation manifest lookup requires context", NULL,
                        NULL, "pouch");
  }
  memset(manifest, 0, sizeof(*manifest));
  memset(current, 0, sizeof(*current));
  cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 0, NULL);
  single_writer = lc_pouch_single_writer_snapshot(pouch, &writer_mode_epoch);
  if (single_writer && cache != NULL && cache->initialized &&
      cache->writer_mode_epoch == writer_mode_epoch &&
      cache->namespace_path != NULL && cache->active_segment_leaf != NULL) {
    rc = lc_pouch_state_cache_manifest_borrow(pouch, cache, manifest, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (max_version_out != NULL) {
      *max_version_out = cache->max_version;
    }
    return lc_pouch_state_entry_from_cache_record(
        pouch, lc_pouch_state_cache_record_find(cache, key), current, error);
  }
  return lc_pouch_state_manifest_lookup_cached(
      pouch, namespace_name, key, manifest, current, max_version_out, error);
}

/* Return a projection-backed manifest in exclusive mode; otherwise refresh. */
static int lc_pouch_state_manifest_view(lc_pouch *pouch,
                                        const char *namespace_name,
                                        lc_pouch_namespace_manifest *manifest,
                                        lc_pouch_namespace_logstore **cache_out,
                                        lc_error *error) {
  lc_pouch_namespace_logstore *cache;
  uint64_t writer_mode_epoch;
  int single_writer;
  int rc;

  if (pouch == NULL || namespace_name == NULL || manifest == NULL ||
      cache_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch manifest view requires context", NULL, NULL,
                        "pouch");
  }
  *cache_out = NULL;
  memset(manifest, 0, sizeof(*manifest));
  cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 0, NULL);
  single_writer = lc_pouch_single_writer_snapshot(pouch, &writer_mode_epoch);
  if (single_writer && cache != NULL && cache->initialized &&
      cache->writer_mode_epoch == writer_mode_epoch &&
      cache->namespace_path != NULL && cache->active_segment_leaf != NULL) {
    rc = lc_pouch_state_cache_manifest_borrow(pouch, cache, manifest, error);
    if (rc == LC_OK) {
      *cache_out = cache;
    }
    return rc;
  }
  rc = lc_pouch_namespace_ensure_layout(&pouch->allocator, pouch->root_path,
                                        namespace_name, error);
  if (rc == LC_OK) {
    rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                          namespace_name, manifest, NULL, NULL,
                                          error);
  }
  if (rc == LC_OK) {
    cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 1, error);
    if (cache == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    } else {
      rc = lc_pouch_state_cache_refresh_for_mode(pouch, cache, manifest, error);
    }
  }
  if (rc != LC_OK) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, manifest);
  } else {
    *cache_out = cache;
  }
  return rc;
}

int lc_pouch_state_warm_namespace(lc_pouch *pouch, const char *namespace_name,
                                  lc_error *error) {
  lc_pouch_namespace_logstore *cache;
  lc_pouch_namespace_manifest manifest;
  lc_pouch_state_process_namespace_mutex *process_mutex;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace warm requires pouch and namespace",
                        NULL, NULL, "pouch");
  }
  process_mutex = NULL;
  rc = lc_pouch_state_process_namespace_mutex_lock(pouch, namespace_name,
                                                   &process_mutex, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_namespace_ensure_layout(&pouch->allocator, pouch->root_path,
                                        namespace_name, error);
  if (rc != LC_OK) {
    goto cleanup_unlocked;
  }
  memset(&manifest, 0, sizeof(manifest));
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                        namespace_name, &manifest, NULL, NULL,
                                        error);
  if (rc != LC_OK) {
    goto cleanup_unlocked;
  }
  cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 1, error);
  if (cache == NULL) {
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  } else {
    rc = lc_pouch_state_cache_refresh_for_mode(pouch, cache, &manifest, error);
  }
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
cleanup_unlocked:
  lc_pouch_state_process_namespace_mutex_unlock(&process_mutex);
  return rc;
}

static int
lc_pouch_state_cache_lookup(lc_pouch *pouch, const char *namespace_name,
                            const lc_pouch_namespace_manifest *manifest,
                            const char *key, lc_pouch_state_entry *out,
                            lc_pouch_generation *max_version_out,
                            lc_error *error) {
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_cache_record *record;
  int rc;

  cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 1, error);
  if (cache == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  rc = lc_pouch_state_cache_refresh_for_mode(pouch, cache, manifest, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (max_version_out != NULL) {
    *max_version_out = cache->max_version;
  }
  record = lc_pouch_state_cache_record_find(cache, key);
  return lc_pouch_state_entry_from_cache_record(pouch, record, out, error);
}

static int lc_pouch_state_read_result_from_entry(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_namespace_manifest *manifest, lc_pouch_state_entry *current,
    int include_body, lc_pouch_state_read_result *out, lc_error *error) {
  char *payload_span_path;
  char *crypto_context;
  int rc;

  if (pouch == NULL || manifest == NULL || current == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state read result requires pouch, manifest, "
                        "entry, and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  if (!current->found) {
    return LC_OK;
  }
  if (include_body && !current->payload_span.present) {
    return LC_OK;
  }
  if (!include_body && !current->payload_span.present) {
    rc = LC_OK;
  } else if (!current->payload_span.present) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state payload span is invalid", NULL, NULL,
                        "pouch");
  } else {
    rc = LC_OK;
  }
  if (include_body) {
    payload_span_path = lc_pouch_state_payload_span_path(
        &pouch->allocator, manifest->namespace_path, &current->payload_span,
        error);
    if (payload_span_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state payload span", NULL,
                          NULL, NULL);
    }
    crypto_context = lc_pouch_state_payload_context_for_read(
        &pouch->allocator, namespace_name, current->key,
        current->payload_context);
    if (crypto_context == NULL) {
      lc_free_with_allocator(&pouch->allocator, payload_span_path);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state crypto context", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_state_source_from_span(
        pouch, crypto_context, payload_span_path,
        current->payload_span.payload_offset,
        current->payload_span.payload_length, current->descriptor, &out->body,
        error);
    lc_free_with_allocator(&pouch->allocator, crypto_context);
    lc_free_with_allocator(&pouch->allocator, payload_span_path);
  }
  if (rc == LC_OK) {
    out->content_type = current->content_type;
    out->etag = current->etag;
    out->descriptor = current->descriptor;
    out->metadata = current->metadata;
    out->metadata_length = current->metadata_length;
    out->index_seq = current->index_seq;
    out->version = current->version;
    out->bytes = current->bytes;
    out->cipher_bytes = current->cipher_bytes;
    out->updated_at_unix = current->updated_at_unix;
    out->has_query_hidden = current->has_query_hidden;
    out->query_hidden = current->query_hidden;
    out->has_body = current->payload_span.present;
    out->found = 1;
    current->content_type = NULL;
    current->etag = NULL;
    current->descriptor = NULL;
    current->metadata = NULL;
    current->metadata_length = 0U;
  }
  return rc;
}

static int lc_pouch_state_read_result_from_cache_record(
    lc_pouch *pouch, const lc_pouch_state_cache_record *record,
    lc_pouch_state_read_result *out, lc_error *error) {
  if (pouch == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch cached read result requires pouch and out", NULL,
                        NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  if (record == NULL || !record->found) {
    return LC_OK;
  }
  if (!record->payload_span.present) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch cached state payload span is invalid", NULL,
                        NULL, "pouch");
  }
  out->content_type =
      record->content_type != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, record->content_type)
          : NULL;
  out->etag = record->etag != NULL
                  ? lc_strdup_with_allocator(&pouch->allocator, record->etag)
                  : NULL;
  out->descriptor =
      record->descriptor != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, record->descriptor)
          : NULL;
  if (record->metadata_length > 0U) {
    out->metadata = (unsigned char *)lc_alloc_with_allocator(
        &pouch->allocator, record->metadata_length);
    if (out->metadata != NULL) {
      memcpy(out->metadata, record->metadata, record->metadata_length);
      out->metadata_length = record->metadata_length;
    }
  }
  if ((record->content_type != NULL && out->content_type == NULL) ||
      (record->etag != NULL && out->etag == NULL) ||
      (record->descriptor != NULL && out->descriptor == NULL) ||
      (record->metadata_length > 0U && out->metadata == NULL)) {
    lc_pouch_state_read_result_cleanup(&pouch->allocator, out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch cached read metadata", NULL,
                        NULL, NULL);
  }
  out->version = record->version;
  out->bytes = record->bytes;
  out->cipher_bytes = record->cipher_bytes;
  out->updated_at_unix = record->updated_at_unix;
  out->has_query_hidden = record->has_query_hidden;
  out->query_hidden = record->query_hidden;
  out->index_seq = record->index_seq;
  out->has_body = record->payload_span.present;
  out->found = 1;
  return LC_OK;
}

static int lc_pouch_state_snapshot_write_record(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_namespace_manifest *manifest, const char *snapshot_leaf,
    int fd, const lc_pouch_state_cache_record *record, lc_error *error) {
  char *old_path;
  lc_pouch_state_payload_span snapshot_span;
  unsigned char *meta;
  unsigned char *final_meta;
  uint64_t cipher_bytes;
  unsigned long stored_crc;
  uint64_t record_start;
  uint64_t payload_offset;
  size_t meta_len;
  size_t final_meta_len;
  int rc;
  unsigned char put_record_type;
  unsigned char delete_record_type;

  (void)namespace_name;
  memset(&snapshot_span, 0, sizeof(snapshot_span));
  old_path = NULL;
  meta = NULL;
  final_meta = NULL;
  record_start = 0U;
  put_record_type = record->record_type == LC_POUCH_STATE_RECORD_OBJECT_PUT
                        ? LC_POUCH_STATE_RECORD_OBJECT_PUT
                        : LC_POUCH_STATE_RECORD_STATE_PUT;
  delete_record_type =
      record->record_type == LC_POUCH_STATE_RECORD_OBJECT_DELETE
          ? LC_POUCH_STATE_RECORD_OBJECT_DELETE
          : LC_POUCH_STATE_RECORD_STATE_DELETE;
  if (record->found) {
    rc = lc_pouch_state_fd_end(fd, &record_start,
                               "failed to seek pouch snapshot append offset",
                               error);
    if (rc != LC_OK) {
      return rc;
    }
    if (!record->payload_span.present) {
      put_record_type = LC_POUCH_STATE_RECORD_STATE_META;
      rc = lc_pouch_state_encode_payload_meta(
          &pouch->allocator, record->version, record->updated_at_unix,
          record->bytes, record->cipher_bytes, record->content_type,
          record->etag, record->descriptor, NULL, record->payload_context,
          record->metadata, record->metadata_length, 0U,
          record->has_query_hidden, record->query_hidden, &meta, &meta_len,
          error);
      if (rc == LC_OK) {
        rc = lc_pouch_state_meta_set_index_seq(meta, meta_len,
                                               record->index_seq, error);
      }
      if (rc == LC_OK) {
        rc = lc_pouch_state_record_write_prefix(
            fd, put_record_type, record->key, strlen(record->key), meta,
            meta_len, 0U, 0UL, 0UL, error);
      }
      lc_free_with_allocator(&pouch->allocator, meta);
      return rc;
    }
    rc = lc_pouch_state_payload_span_set(&pouch->allocator, &snapshot_span,
                                         snapshot_leaf, record_start, 0UL, 0UL,
                                         0UL, error);
    if (rc != LC_OK) {
      return rc;
    }
    rc = lc_pouch_state_encode_payload_meta(
        &pouch->allocator, record->version, record->updated_at_unix, 0UL, 0UL,
        record->content_type, record->etag, NULL, &snapshot_span,
        record->payload_context, record->metadata, record->metadata_length,
        LC_POUCH_STATE_RECORD_DESCRIPTOR_RESERVE, record->has_query_hidden,
        record->query_hidden, &meta, &meta_len, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (record_start > LC_U64_MAX - LC_POUCH_STATE_RECORD_HEADER_BYTES ||
        (uint64_t)strlen(record->key) >
            LC_U64_MAX - LC_POUCH_STATE_RECORD_HEADER_BYTES - record_start ||
        (uint64_t)meta_len > LC_U64_MAX - LC_POUCH_STATE_RECORD_HEADER_BYTES -
                                 record_start - (uint64_t)strlen(record->key)) {
      lc_free_with_allocator(&pouch->allocator, meta);
      lc_pouch_state_payload_span_cleanup(&pouch->allocator, &snapshot_span);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch snapshot record offset exceeds u64", NULL,
                          NULL, "pouch");
    }
    payload_offset = record_start + LC_POUCH_STATE_RECORD_HEADER_BYTES +
                     (uint64_t)strlen(record->key) + (uint64_t)meta_len;
    old_path = lc_pouch_state_payload_span_path(&pouch->allocator,
                                                manifest->namespace_path,
                                                &record->payload_span, error);
    if (old_path == NULL) {
      lc_free_with_allocator(&pouch->allocator, meta);
      lc_free_with_allocator(&pouch->allocator, old_path);
      return error != NULL && error->code != LC_OK
                 ? error->code
                 : lc_error_set(error, LC_ERR_NOMEM, 0L,
                                "failed to prepare pouch snapshot payload ref",
                                NULL, NULL, "pouch");
    }
    rc = lc_pouch_state_record_write_prefix(
        fd, put_record_type, record->key, strlen(record->key), meta, meta_len,
        0U, 0UL, LC_POUCH_RECORD_FLAG_PENDING, error);
    if (rc == LC_OK) {
      rc = lc_pouch_state_copy_file_span_to_fd_crc(
          pouch, old_path, record->payload_span.payload_offset,
          record->payload_span.payload_length, fd, &stored_crc, error);
    }
    cipher_bytes = record->payload_span.payload_length;
    if (rc == LC_OK) {
      lc_pouch_state_payload_span_cleanup(&pouch->allocator, &snapshot_span);
      rc = lc_pouch_state_payload_span_set(
          &pouch->allocator, &snapshot_span, snapshot_leaf, record_start,
          payload_offset, cipher_bytes, stored_crc, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_encode_payload_meta(
          &pouch->allocator, record->version, record->updated_at_unix,
          record->bytes, cipher_bytes, record->content_type, record->etag,
          record->descriptor, &snapshot_span, record->payload_context,
          record->metadata, record->metadata_length,
          LC_POUCH_STATE_RECORD_DESCRIPTOR_RESERVE, record->has_query_hidden,
          record->query_hidden, &final_meta, &final_meta_len, error);
      if (rc == LC_OK && final_meta_len != meta_len) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch snapshot metadata reservation changed size",
                          NULL, NULL, "pouch");
      }
      if (rc == LC_OK) {
        rc = lc_pouch_state_meta_set_index_seq(final_meta, final_meta_len,
                                               record->index_seq, error);
      }
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_record_finalize(
          fd, record_start, put_record_type, record->key, strlen(record->key),
          final_meta, final_meta_len, cipher_bytes, stored_crc, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_fd_end(
          fd, &payload_offset, "failed to restore pouch snapshot append offset",
          error);
    }
    if (rc != LC_OK) {
      lc_pouch_state_truncate_fd_best_effort(fd, record_start);
      lc_free_with_allocator(&pouch->allocator, meta);
      lc_free_with_allocator(&pouch->allocator, final_meta);
      lc_free_with_allocator(&pouch->allocator, old_path);
      lc_pouch_state_payload_span_cleanup(&pouch->allocator, &snapshot_span);
      return rc;
    }
  } else {
    old_path = NULL;
    cipher_bytes = 0UL;
    rc = lc_pouch_state_encode_delete_meta(
        &pouch->allocator, record->version, record->updated_at_unix,
        record->etag, &meta, &meta_len, error);
    if (rc == LC_OK) {
      rc = lc_pouch_state_meta_set_index_seq(meta, meta_len, record->index_seq,
                                             error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_record_write_prefix(
          fd, delete_record_type, record->key, strlen(record->key), meta,
          meta_len, 0U, 0UL, 0UL, error);
    }
  }
  lc_free_with_allocator(&pouch->allocator, meta);
  lc_free_with_allocator(&pouch->allocator, final_meta);
  lc_free_with_allocator(&pouch->allocator, old_path);
  lc_pouch_state_payload_span_cleanup(&pouch->allocator, &snapshot_span);
  return rc;
}

static int lc_pouch_state_snapshot_write_high_water(
    int fd, lc_pouch_generation high_water_version, lc_error *error) {
  unsigned char *meta;
  size_t meta_len;
  int rc;

  meta = NULL;
  meta_len = 0U;
  rc = lc_pouch_state_encode_high_water_meta(NULL, high_water_version, &meta,
                                             &meta_len, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_record_write_prefix(
        fd, LC_POUCH_STATE_RECORD_HIGH_WATER, NULL, 0U, meta, meta_len, 0U, 0UL,
        0UL, error);
  }
  lc_free_with_allocator(NULL, meta);
  return rc;
}

static int lc_pouch_state_write_snapshot(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    const char *snapshot_leaf, lc_pouch_namespace_logstore *cache,
    const lc_pouch_state_compaction_capture *capture, lc_error *error) {
  lc_pouch_state_cache_record **records;
  char tmp_leaf[128];
  char *snapshot_path;
  char *tmp_path;
  size_t record_count;
  size_t index;
  int fd;
  int rc;
  int len;

  len = snprintf(tmp_leaf, sizeof(tmp_leaf), "%s.tmp", snapshot_leaf);
  if (len < 0 || (size_t)len >= sizeof(tmp_leaf)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state snapshot temp path exceeds limit", NULL,
                        NULL, NULL);
  }
  snapshot_path = lc_pouch_state_child_path(
      &pouch->allocator, manifest->namespace_path, "snapshots", snapshot_leaf);
  tmp_path = snapshot_path != NULL
                 ? lc_pouch_state_child_path(&pouch->allocator,
                                             manifest->namespace_path,
                                             "snapshots", tmp_leaf)
                 : NULL;
  if (snapshot_path == NULL || tmp_path == NULL) {
    lc_free_with_allocator(&pouch->allocator, snapshot_path);
    lc_free_with_allocator(&pouch->allocator, tmp_path);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state snapshot path", NULL,
                        NULL, NULL);
  }
  fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if (fd < 0) {
    lc_free_with_allocator(&pouch->allocator, snapshot_path);
    lc_free_with_allocator(&pouch->allocator, tmp_path);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create pouch state snapshot",
                        strerror(errno), NULL, NULL);
  }
  rc = LC_OK;
  records = NULL;
  record_count = 0U;
  rc = lc_pouch_state_cache_record_index_build(pouch, cache, &records,
                                               &record_count, error);
  if (rc != LC_OK) {
    (void)close(fd);
    unlink(tmp_path);
    lc_free_with_allocator(&pouch->allocator, snapshot_path);
    lc_free_with_allocator(&pouch->allocator, tmp_path);
    return rc;
  }
  rc = lc_pouch_state_snapshot_write_high_water(fd, cache->max_version, error);
  for (index = 0U;
       rc == LC_OK && capture != NULL && index < capture->captured_count;
       ++index) {
    lc_pouch_state_cache_record *record;

    record = lc_pouch_state_cache_record_index_find(
        records, record_count, capture->captured_keys[index]);
    if (!lc_pouch_state_compaction_record_ref_matches(
            record, capture->captured_record_containers[index],
            capture->captured_record_offsets[index])) {
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch compaction captured ref drifted", NULL, NULL,
                        "pouch");
      break;
    }
    if (rc != LC_OK) {
      break;
    }
    rc = lc_pouch_state_snapshot_write_record(pouch, cache->namespace_name,
                                              manifest, snapshot_leaf, fd,
                                              record, error);
  }
  lc_free_with_allocator(&pouch->allocator, records);
  if (rc == LC_OK && fsync(fd) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to fsync pouch state snapshot", strerror(errno),
                      NULL, NULL);
  }
  if (close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state snapshot", strerror(errno),
                      NULL, NULL);
  }
  if (rc == LC_OK && rename(tmp_path, snapshot_path) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to install pouch state snapshot", strerror(errno),
                      NULL, NULL);
  }
  if (rc != LC_OK) {
    unlink(tmp_path);
  }
  lc_free_with_allocator(&pouch->allocator, snapshot_path);
  lc_free_with_allocator(&pouch->allocator, tmp_path);
  return rc;
}

static int lc_pouch_state_read_text_file(lc_pouch *pouch, const char *path,
                                         char **out, size_t *out_length,
                                         lc_error *error) {
  char buffer[4096];
  char *bytes;
  FILE *fp;
  size_t capacity;
  size_t length;
  int rc;

  *out = NULL;
  fp = fopen(path, "rb");
  if (fp == NULL) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch text file", strerror(errno), path,
                        NULL);
  }
  bytes = NULL;
  capacity = 0U;
  length = 0U;
  rc = LC_OK;
  while (!feof(fp)) {
    size_t got;

    got = fread(buffer, 1U, sizeof(buffer), fp);
    if (got != 0U) {
      if (length + got + 1U < length) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch text file", NULL, path,
                          NULL);
        break;
      }
      if (length + got + 1U > capacity) {
        char *grown;
        size_t next_capacity;

        next_capacity = capacity == 0U ? 4096U : capacity;
        while (next_capacity < length + got + 1U) {
          if (next_capacity > ((size_t)-1) / 2U) {
            rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                              "failed to allocate pouch text file", NULL, path,
                              NULL);
            break;
          }
          next_capacity *= 2U;
        }
        if (rc != LC_OK) {
          break;
        }
        grown = (char *)lc_realloc_with_allocator(&pouch->allocator, bytes,
                                                  next_capacity);
        if (grown == NULL) {
          rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch text file", NULL, path,
                            NULL);
          break;
        }
        bytes = grown;
        capacity = next_capacity;
      }
      memcpy(bytes + length, buffer, got);
      length += got;
    }
    if (ferror(fp)) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch text file", strerror(errno), path,
                        NULL);
      break;
    }
  }
  if (fclose(fp) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch text file", strerror(errno), path,
                      NULL);
  }
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, bytes);
    return rc;
  }
  if (bytes == NULL) {
    bytes = lc_strdup_with_allocator(&pouch->allocator, "");
    if (bytes == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch text file", NULL, path,
                          NULL);
    }
  } else {
    bytes[length] = '\0';
  }
  *out = bytes;
  if (out_length != NULL) {
    *out_length = length;
  }
  return LC_OK;
}

static void lc_pouch_state_compaction_capture_cleanup(
    lc_pouch *pouch, lc_pouch_state_compaction_capture *capture) {
  size_t index;

  if (capture == NULL) {
    return;
  }
  lc_free_with_allocator(&pouch->allocator, capture->manifest_text);
  for (index = 0U; index < capture->candidate_count; ++index) {
    lc_free_with_allocator(&pouch->allocator, capture->candidate_leaves[index]);
  }
  lc_free_with_allocator(&pouch->allocator, capture->candidate_leaves);
  lc_free_with_allocator(&pouch->allocator, capture->candidate_snapshots);
  lc_free_with_allocator(&pouch->allocator, capture->candidate_sizes);
  lc_free_with_allocator(&pouch->allocator, capture->candidate_checksums);
  for (index = 0U; index < capture->captured_count; ++index) {
    lc_free_with_allocator(&pouch->allocator, capture->captured_keys[index]);
    lc_free_with_allocator(&pouch->allocator,
                           capture->captured_record_containers[index]);
  }
  lc_free_with_allocator(&pouch->allocator, capture->captured_keys);
  lc_free_with_allocator(&pouch->allocator,
                         capture->captured_record_containers);
  lc_free_with_allocator(&pouch->allocator, capture->captured_record_offsets);
  memset(capture, 0, sizeof(*capture));
}

static int lc_pouch_state_manifest_leaf_obsolete(
    const lc_pouch_namespace_manifest *manifest, const char *leaf,
    int snapshot) {
  char **items;
  unsigned long count;
  unsigned long index;

  if (manifest == NULL || leaf == NULL) {
    return 0;
  }
  items = snapshot ? manifest->obsolete_snapshots : manifest->obsolete_segments;
  count = snapshot ? manifest->obsolete_snapshot_count
                   : manifest->obsolete_segment_count;
  for (index = 0UL; index < count; ++index) {
    if (items[index] != NULL && strcmp(items[index], leaf) == 0) {
      return 1;
    }
  }
  return 0;
}

static int lc_pouch_state_compaction_candidate_index(
    const lc_pouch_state_compaction_capture *capture, const char *leaf) {
  size_t index;

  if (capture == NULL || leaf == NULL) {
    return -1;
  }
  for (index = 0U; index < capture->candidate_count; ++index) {
    if (capture->candidate_leaves[index] != NULL &&
        strcmp(capture->candidate_leaves[index], leaf) == 0) {
      return (int)index;
    }
  }
  return -1;
}

static int lc_pouch_state_compaction_candidate_contains(
    const lc_pouch_state_compaction_capture *capture, const char *leaf) {
  return lc_pouch_state_compaction_candidate_index(capture, leaf) >= 0;
}

static int lc_pouch_state_compaction_candidate_add(
    lc_pouch *pouch, lc_pouch_state_compaction_capture *capture,
    const lc_pouch_namespace_manifest *manifest, const char *leaf, int snapshot,
    lc_error *error) {
  char *path;
  char *leaf_copy;
  uint64_t size = 0U;
  int rc;

  if (capture == NULL || leaf == NULL || leaf[0] == '\0') {
    return LC_OK;
  }
  if (lc_pouch_state_compaction_candidate_contains(capture, leaf)) {
    return LC_OK;
  }
  if (capture->candidate_count >= capture->candidate_capacity) {
    char **next_leaves;
    int *next_snapshots;
    uint64_t *next_sizes;
    unsigned long *next_checksums;
    size_t next_capacity;

    next_capacity = capture->candidate_capacity == 0U
                        ? 8U
                        : capture->candidate_capacity * 2U;
    if (next_capacity <= capture->candidate_capacity) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch compaction candidate list exceeds local limit",
                          NULL, NULL, NULL);
    }
    next_leaves = (char **)lc_realloc_with_allocator(
        &pouch->allocator, capture->candidate_leaves,
        next_capacity * sizeof(capture->candidate_leaves[0]));
    if (next_leaves == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch compaction candidates",
                          NULL, NULL, NULL);
    }
    capture->candidate_leaves = next_leaves;
    next_snapshots = (int *)lc_realloc_with_allocator(
        &pouch->allocator, capture->candidate_snapshots,
        next_capacity * sizeof(capture->candidate_snapshots[0]));
    if (next_snapshots == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch compaction candidate "
                          "types",
                          NULL, NULL, NULL);
    }
    capture->candidate_snapshots = next_snapshots;
    next_sizes = (uint64_t *)lc_realloc_with_allocator(
        &pouch->allocator, capture->candidate_sizes,
        next_capacity * sizeof(capture->candidate_sizes[0]));
    if (next_sizes == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch compaction candidate "
                          "sizes",
                          NULL, NULL, NULL);
    }
    capture->candidate_sizes = next_sizes;
    next_checksums = (unsigned long *)lc_realloc_with_allocator(
        &pouch->allocator, capture->candidate_checksums,
        next_capacity * sizeof(capture->candidate_checksums[0]));
    if (next_checksums == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch compaction candidate "
                          "checksums",
                          NULL, NULL, NULL);
    }
    capture->candidate_checksums = next_checksums;
    capture->candidate_capacity = next_capacity;
  }
  leaf_copy = lc_strdup_with_allocator(&pouch->allocator, leaf);
  if (leaf_copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch compaction candidate", NULL, NULL,
                        NULL);
  }
  path = lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                   snapshot ? "snapshots" : "segments", leaf);
  if (path == NULL) {
    lc_free_with_allocator(&pouch->allocator, leaf_copy);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch compaction candidate path",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_state_file_size(path, &size, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_file_checksum(
        path, size, &capture->candidate_checksums[capture->candidate_count],
        error);
  }
  lc_free_with_allocator(&pouch->allocator, path);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, leaf_copy);
    return rc;
  }
  if (size > LC_U64_MAX - capture->candidate_bytes) {
    lc_free_with_allocator(&pouch->allocator, leaf_copy);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch compaction candidate bytes overflow", NULL, NULL,
                        "pouch");
  }
  capture->candidate_leaves[capture->candidate_count] = leaf_copy;
  capture->candidate_snapshots[capture->candidate_count] = snapshot ? 1 : 0;
  capture->candidate_sizes[capture->candidate_count] = size;
  ++capture->candidate_count;
  capture->candidate_bytes += size;
  return LC_OK;
}

static int lc_pouch_state_compaction_build_candidates(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    lc_pouch_state_compaction_capture *capture, lc_error *error) {
  unsigned long segment_index;
  int rc;

  if (manifest->latest_snapshot != NULL &&
      !lc_pouch_state_manifest_leaf_obsolete(manifest,
                                             manifest->latest_snapshot, 1)) {
    rc = lc_pouch_state_compaction_candidate_add(
        pouch, capture, manifest, manifest->latest_snapshot, 1, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  for (segment_index = 0UL; segment_index < manifest->segment_count;
       ++segment_index) {
    const char *segment_leaf = manifest->segment_leaves[segment_index];

    if (!lc_pouch_state_manifest_segment_visible(manifest, segment_leaf) ||
        (manifest->active_segment != NULL &&
         strcmp(segment_leaf, manifest->active_segment) == 0)) {
      continue;
    }
    rc = lc_pouch_state_compaction_candidate_add(pouch, capture, manifest,
                                                 segment_leaf, 0, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  return LC_OK;
}

static void lc_pouch_state_compaction_candidate_remove_at(
    lc_pouch *pouch, lc_pouch_state_compaction_capture *capture, size_t index) {
  if (capture == NULL || index >= capture->candidate_count) {
    return;
  }
  lc_free_with_allocator(&pouch->allocator, capture->candidate_leaves[index]);
  if (capture->candidate_bytes >= capture->candidate_sizes[index]) {
    capture->candidate_bytes -= capture->candidate_sizes[index];
  } else {
    capture->candidate_bytes = 0UL;
  }
  if (index + 1U < capture->candidate_count) {
    memmove(capture->candidate_leaves + index,
            capture->candidate_leaves + index + 1U,
            (capture->candidate_count - index - 1U) *
                sizeof(capture->candidate_leaves[0]));
    memmove(capture->candidate_snapshots + index,
            capture->candidate_snapshots + index + 1U,
            (capture->candidate_count - index - 1U) *
                sizeof(capture->candidate_snapshots[0]));
    memmove(capture->candidate_sizes + index,
            capture->candidate_sizes + index + 1U,
            (capture->candidate_count - index - 1U) *
                sizeof(capture->candidate_sizes[0]));
    memmove(capture->candidate_checksums + index,
            capture->candidate_checksums + index + 1U,
            (capture->candidate_count - index - 1U) *
                sizeof(capture->candidate_checksums[0]));
  }
  --capture->candidate_count;
}

static int lc_pouch_state_compaction_protect_live_links(
    lc_pouch *pouch, lc_pouch_state_compaction_capture *capture,
    lc_pouch_namespace_logstore *cache, int *blocked, lc_error *error) {
  lc_pouch_state_cache_record *record;

  (void)error;
  if (blocked != NULL) {
    *blocked = 0;
  }
  for (record = cache->records; record != NULL; record = record->next) {
    int payload_candidate;
    int record_candidate;

    if (!record->payload_span.present ||
        !lc_pouch_state_compaction_candidate_contains(
            capture, record->payload_span.container_leaf)) {
      continue;
    }
    record_candidate =
        record->has_record_ref && lc_pouch_state_compaction_candidate_contains(
                                      capture, record->record_container_leaf);
    if (record_candidate) {
      continue;
    }
    payload_candidate = lc_pouch_state_compaction_candidate_index(
        capture, record->payload_span.container_leaf);
    if (payload_candidate < 0) {
      continue;
    }
    if (capture->candidate_snapshots[payload_candidate]) {
      if (blocked != NULL) {
        *blocked = 1;
      }
      return LC_OK;
    }
    lc_pouch_state_compaction_candidate_remove_at(pouch, capture,
                                                  (size_t)payload_candidate);
  }
  return LC_OK;
}

static int lc_pouch_state_compaction_capture_record_add(
    lc_pouch *pouch, lc_pouch_state_compaction_capture *capture,
    const lc_pouch_state_cache_record *record, lc_error *error) {
  char *key_copy;
  char *container_copy;

  if (capture->captured_count >= capture->captured_capacity) {
    char **next_keys;
    char **next_containers;
    uint64_t *next_offsets;
    size_t next_capacity;

    next_capacity = capture->captured_capacity == 0U
                        ? 32U
                        : capture->captured_capacity * 2U;
    if (next_capacity <= capture->captured_capacity) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch compaction captured refs exceed local limit",
                          NULL, NULL, NULL);
    }
    next_keys = (char **)lc_realloc_with_allocator(
        &pouch->allocator, capture->captured_keys,
        next_capacity * sizeof(capture->captured_keys[0]));
    if (next_keys == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch compaction captured keys",
                          NULL, NULL, NULL);
    }
    capture->captured_keys = next_keys;
    next_containers = (char **)lc_realloc_with_allocator(
        &pouch->allocator, capture->captured_record_containers,
        next_capacity * sizeof(capture->captured_record_containers[0]));
    if (next_containers == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch compaction captured "
                          "containers",
                          NULL, NULL, NULL);
    }
    capture->captured_record_containers = next_containers;
    next_offsets = (uint64_t *)lc_realloc_with_allocator(
        &pouch->allocator, capture->captured_record_offsets,
        next_capacity * sizeof(capture->captured_record_offsets[0]));
    if (next_offsets == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch compaction captured "
                          "offsets",
                          NULL, NULL, NULL);
    }
    capture->captured_record_offsets = next_offsets;
    capture->captured_capacity = next_capacity;
  }
  key_copy = lc_strdup_with_allocator(&pouch->allocator, record->key);
  container_copy = lc_strdup_with_allocator(&pouch->allocator,
                                            record->record_container_leaf);
  if (key_copy == NULL || container_copy == NULL) {
    lc_free_with_allocator(&pouch->allocator, key_copy);
    lc_free_with_allocator(&pouch->allocator, container_copy);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch compaction captured ref", NULL,
                        NULL, NULL);
  }
  capture->captured_keys[capture->captured_count] = key_copy;
  capture->captured_record_containers[capture->captured_count] = container_copy;
  capture->captured_record_offsets[capture->captured_count] =
      record->record_offset;
  ++capture->captured_count;
  return LC_OK;
}

static int lc_pouch_state_compaction_capture_records(
    lc_pouch *pouch, lc_pouch_state_compaction_capture *capture,
    lc_pouch_namespace_logstore *cache, lc_error *error) {
  lc_pouch_state_cache_record **records;
  size_t record_count;
  size_t index;
  int rc;

  records = NULL;
  record_count = 0U;
  rc = lc_pouch_state_cache_record_index_build(pouch, cache, &records,
                                               &record_count, error);
  for (index = 0U; rc == LC_OK && index < record_count; ++index) {
    lc_pouch_state_cache_record *record;

    record = records[index];
    if (!record->found || !record->has_record_ref) {
      continue;
    }
    if (!lc_pouch_state_compaction_candidate_contains(
            capture, record->record_container_leaf)) {
      continue;
    }
    rc = lc_pouch_state_compaction_capture_record_add(pouch, capture, record,
                                                      error);
  }
  lc_free_with_allocator(&pouch->allocator, records);
  return rc;
}

static int lc_pouch_state_compaction_record_ref_matches(
    const lc_pouch_state_cache_record *record, const char *container,
    uint64_t offset) {
  return record != NULL && record->has_record_ref &&
         record->record_container_leaf != NULL &&
         strcmp(record->record_container_leaf, container) == 0 &&
         record->record_offset == offset;
}

static int lc_pouch_state_compaction_capture_now(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    lc_pouch_state_compaction_capture *capture, lc_error *error) {
  char *manifest_path;
  int rc;

  memset(capture, 0, sizeof(*capture));
  manifest_path = lc_pouch_path_join(&pouch->allocator,
                                     manifest->namespace_path, "manifest");
  if (manifest_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace manifest path",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_state_read_text_file(pouch, manifest_path,
                                     &capture->manifest_text, NULL, error);
  lc_free_with_allocator(&pouch->allocator, manifest_path);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_compaction_build_candidates(pouch, manifest, capture,
                                                  error);
  if (rc != LC_OK) {
    lc_pouch_state_compaction_capture_cleanup(pouch, capture);
  }
  return rc;
}

static int lc_pouch_state_compaction_validate_candidates(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    const lc_pouch_state_compaction_capture *capture, lc_error *error) {
  size_t index;

  if (pouch == NULL || manifest == NULL || capture == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch compaction candidate validation requires "
                        "inputs",
                        NULL, NULL, NULL);
  }
  for (index = 0U; index < capture->candidate_count; ++index) {
    char *path;
    unsigned long checksum;
    int rc;

    path = lc_pouch_state_child_path(
        &pouch->allocator, manifest->namespace_path,
        capture->candidate_snapshots[index] ? "snapshots" : "segments",
        capture->candidate_leaves[index]);
    if (path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch compaction candidate path",
                          NULL, NULL, NULL);
    }
    checksum = 0UL;
    rc = lc_pouch_state_file_checksum(path, capture->candidate_sizes[index],
                                      &checksum, error);
    lc_free_with_allocator(&pouch->allocator, path);
    if (rc != LC_OK) {
      return rc;
    }
    if (checksum != capture->candidate_checksums[index]) {
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch compaction validation drift", NULL, NULL,
                          "pouch");
    }
  }
  return LC_OK;
}

static int lc_pouch_state_compaction_validate_capture(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    const lc_pouch_state_compaction_capture *before,
    lc_pouch_namespace_logstore *cache, lc_error *error) {
  lc_pouch_state_cache_record **records;
  char *manifest_path;
  char *manifest_text;
  size_t record_count;
  size_t index;
  int rc;

  records = NULL;
  record_count = 0U;
  manifest_text = NULL;
  manifest_path = lc_pouch_path_join(&pouch->allocator,
                                     manifest->namespace_path, "manifest");
  if (manifest_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace manifest path",
                        NULL, NULL, NULL);
  }
  rc = lc_pouch_state_read_text_file(pouch, manifest_path, &manifest_text, NULL,
                                     error);
  lc_free_with_allocator(&pouch->allocator, manifest_path);
  if (rc != LC_OK) {
    return rc;
  }
  if (before->manifest_text == NULL || manifest_text == NULL ||
      strcmp(before->manifest_text, manifest_text) != 0) {
    lc_free_with_allocator(&pouch->allocator, manifest_text);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch compaction validation drift", NULL, NULL,
                        "pouch");
  }
  lc_free_with_allocator(&pouch->allocator, manifest_text);
  rc = lc_pouch_state_compaction_validate_candidates(pouch, manifest, before,
                                                     error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_cache_record_index_build(pouch, cache, &records,
                                               &record_count, error);
  if (rc != LC_OK) {
    return rc;
  }
  for (index = 0U; index < before->captured_count; ++index) {
    lc_pouch_state_cache_record *record;

    record = lc_pouch_state_cache_record_index_find(
        records, record_count, before->captured_keys[index]);
    if (!lc_pouch_state_compaction_record_ref_matches(
            record, before->captured_record_containers[index],
            before->captured_record_offsets[index])) {
      lc_free_with_allocator(&pouch->allocator, records);
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch compaction validation drift", NULL, NULL,
                          "pouch");
    }
  }
  lc_free_with_allocator(&pouch->allocator, records);
  return LC_OK;
}

static int lc_pouch_maintenance_set_string(lc_pouch *pouch, char **target,
                                           const char *value, lc_error *error) {
  char *copy;

  if (target == NULL) {
    return LC_OK;
  }
  copy =
      lc_strdup_with_allocator(&pouch->allocator, value != NULL ? value : "");
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch maintenance diagnostic", NULL,
                        NULL, NULL);
  }
  lc_free_with_allocator(&pouch->allocator, *target);
  *target = copy;
  return LC_OK;
}

static int lc_pouch_maintenance_set_diagnostic(lc_pouch *pouch,
                                               lc_pouch_maintenance_result *out,
                                               const char *diagnostic,
                                               lc_error *error) {
  if (out == NULL) {
    return LC_OK;
  }
  return lc_pouch_maintenance_set_string(pouch, &out->diagnostic, diagnostic,
                                         error);
}

static void lc_pouch_maintenance_set_diagnostic_best_effort(
    lc_pouch *pouch, lc_pouch_maintenance_result *out, const char *diagnostic) {
  char *copy;

  if (out == NULL) {
    return;
  }
  copy = lc_strdup_with_allocator(&pouch->allocator,
                                  diagnostic != NULL ? diagnostic : "");
  if (copy == NULL) {
    return;
  }
  lc_free_with_allocator(&pouch->allocator, out->diagnostic);
  out->diagnostic = copy;
}

static void lc_pouch_maintenance_mark_aborted(lc_pouch *pouch,
                                              lc_pouch_maintenance_result *out,
                                              const char *diagnostic) {
  if (out == NULL) {
    return;
  }
  out->aborted = 1;
  out->compacted = 0;
  out->skipped = 0;
  lc_pouch_maintenance_set_diagnostic_best_effort(
      pouch, out, diagnostic != NULL ? diagnostic : "compaction-aborted");
}

static lc_pouch_unix_seconds lc_pouch_maintenance_now_seconds(void) {
  time_t now;

  now = time(NULL);
  if (now <= 0) {
    return 0;
  }
  if ((uintmax_t)now > (uintmax_t)LC_I64_MAX) {
    return LC_I64_MAX;
  }
  return (lc_pouch_unix_seconds)now;
}

typedef struct lc_pouch_retention_key {
  char *key;
  lc_pouch_generation version;
} lc_pouch_retention_key;

typedef struct lc_pouch_retention_scan {
  lc_pouch *pouch;
  lc_pouch_unix_seconds cutoff_unix;
  lc_pouch_retention_key *keys;
  size_t count;
  size_t capacity;
  unsigned long scanned_count;
  unsigned long expired_count;
} lc_pouch_retention_scan;

static void lc_pouch_retention_scan_cleanup(lc_pouch_retention_scan *scan) {
  size_t index;

  if (scan == NULL || scan->pouch == NULL) {
    return;
  }
  for (index = 0U; index < scan->count; ++index) {
    lc_free_with_allocator(&scan->pouch->allocator, scan->keys[index].key);
  }
  lc_free_with_allocator(&scan->pouch->allocator, scan->keys);
  memset(scan, 0, sizeof(*scan));
}

static int lc_pouch_retention_scan_add(lc_pouch_retention_scan *scan,
                                       const char *key,
                                       lc_pouch_generation version,
                                       lc_error *error) {
  lc_pouch_retention_key *next_keys;
  size_t next_capacity;
  char *copy;

  if (scan->count >= scan->capacity) {
    next_capacity = scan->capacity == 0U ? 16U : scan->capacity * 2U;
    if (next_capacity <= scan->capacity) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "pouch retention key list exceeds local limit", NULL,
                          NULL, NULL);
    }
    next_keys = (lc_pouch_retention_key *)lc_realloc_with_allocator(
        &scan->pouch->allocator, scan->keys,
        next_capacity * sizeof(*next_keys));
    if (next_keys == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch retention key list", NULL,
                          NULL, NULL);
    }
    scan->keys = next_keys;
    scan->capacity = next_capacity;
  }
  copy = lc_strdup_with_allocator(&scan->pouch->allocator, key);
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch retention key", NULL, NULL, NULL);
  }
  scan->keys[scan->count].key = copy;
  scan->keys[scan->count].version = version;
  ++scan->count;
  ++scan->expired_count;
  return LC_OK;
}

static int
lc_pouch_retention_scan_visit(const lc_pouch_state_visit_entry *entry,
                              void *context, lc_error *error) {
  lc_pouch_retention_scan *scan;

  scan = (lc_pouch_retention_scan *)context;
  if (scan == NULL || entry == NULL || entry->key == NULL) {
    return LC_OK;
  }
  ++scan->scanned_count;
  if (entry->updated_at_unix > 0L &&
      entry->updated_at_unix < scan->cutoff_unix) {
    return lc_pouch_retention_scan_add(scan, entry->key, entry->version, error);
  }
  return LC_OK;
}

static int lc_pouch_state_retention_sweep(lc_pouch *pouch,
                                          const char *namespace_name,
                                          lc_pouch_unix_seconds cutoff_unix,
                                          lc_pouch_maintenance_result *out,
                                          lc_error *error) {
  lc_pouch_retention_scan scan;
  unsigned long deleted_count;
  unsigned long failed_count;
  size_t index;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      cutoff_unix <= 0L) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch retention sweep requires namespace and positive "
                        "cutoff",
                        NULL, NULL, NULL);
  }
  if (out != NULL) {
    rc = lc_pouch_maintenance_set_string(pouch, &out->namespace_name,
                                         namespace_name, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  memset(&scan, 0, sizeof(scan));
  scan.pouch = pouch;
  scan.cutoff_unix = cutoff_unix;
  rc = lc_pouch_state_visit_internal(
      pouch, namespace_name, lc_pouch_retention_scan_visit, &scan, 1, error);
  if (rc != LC_OK) {
    lc_pouch_retention_scan_cleanup(&scan);
    return rc;
  }
  deleted_count = 0UL;
  failed_count = 0UL;
  for (index = 0U; index < scan.count; ++index) {
    lc_pouch_state_write_options delete_options;
    lc_pouch_state_write_result delete_result;
    lc_error delete_error;

    memset(&delete_options, 0, sizeof(delete_options));
    memset(&delete_result, 0, sizeof(delete_result));
    lc_error_init(&delete_error);
    delete_options.has_expected_version = 1;
    delete_options.expected_version = scan.keys[index].version;
    rc = lc_pouch_state_delete_locked(pouch, namespace_name,
                                      scan.keys[index].key, &delete_options,
                                      &delete_result, &delete_error);
    if (rc == LC_OK) {
      ++deleted_count;
    } else {
      ++failed_count;
    }
    lc_error_cleanup(&delete_error);
    lc_pouch_state_write_result_cleanup(&pouch->allocator, &delete_result);
  }
  if (out != NULL) {
    out->retention_scanned_count = scan.scanned_count;
    out->retention_expired_count = scan.expired_count;
    out->retention_deleted_state_count = deleted_count;
    out->retention_failed_count = failed_count;
  }
  lc_pouch_retention_scan_cleanup(&scan);
  return lc_pouch_maintenance_set_diagnostic(
      pouch, out,
      failed_count != 0UL ? "retention-failures" : "retention-complete", error);
}

static int lc_pouch_state_compact_namespace(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_namespace_manifest *manifest, unsigned long *cleanup_deleted_count,
    unsigned long *cleanup_pending_count, const char **abort_diagnostic,
    int *compacted_out, lc_error *error);

static int lc_pouch_state_queue_compacted_files(
    lc_pouch *pouch, lc_pouch_namespace_manifest *manifest,
    const char *namespace_name,
    const lc_pouch_state_compaction_capture *capture,
    unsigned long *cleanup_deleted_count, unsigned long *cleanup_pending_count,
    lc_error *error);

static int lc_pouch_state_payload_container_is_valid(const char *leaf) {
  size_t len;
  size_t i;
  uint64_t segment_id;

  if (leaf == NULL) {
    return 0;
  }
  len = strlen(leaf);
  if (lc_pouch_namespace_parse_segment_leaf(leaf, &segment_id)) {
    return 1;
  }
  if (len == 33U && strncmp(leaf, "snapshot-", 9U) == 0 &&
      strcmp(leaf + 29U, ".log") == 0) {
    for (i = 9U; i < 29U; ++i) {
      if (leaf[i] < '0' || leaf[i] > '9') {
        return 0;
      }
    }
    return 1;
  }
  return 0;
}

static int lc_pouch_state_queue_compacted_files(
    lc_pouch *pouch, lc_pouch_namespace_manifest *manifest,
    const char *namespace_name,
    const lc_pouch_state_compaction_capture *capture,
    unsigned long *cleanup_deleted_count, unsigned long *cleanup_pending_count,
    lc_error *error) {
  size_t index;
  uint64_t now_unix;
  int rc;

  if (capture == NULL) {
    return LC_OK;
  }
  now_unix = (uint64_t)lc_pouch_maintenance_now_seconds();
  if (now_unix == 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch obsolete file timestamp is invalid", NULL, NULL,
                        "pouch");
  }
  for (index = 0U; index < capture->candidate_count; ++index) {
    if (capture->candidate_snapshots[index]) {
      rc = lc_pouch_namespace_manifest_mark_obsolete_snapshot(
          &pouch->allocator, manifest, capture->candidate_leaves[index],
          now_unix, error);
      if (rc != LC_OK) {
        return rc;
      }
    } else {
      rc = lc_pouch_namespace_manifest_mark_obsolete_segment(
          &pouch->allocator, manifest, capture->candidate_leaves[index],
          now_unix, error);
      if (rc != LC_OK) {
        return rc;
      }
    }
  }
  rc = lc_pouch_namespace_manifest_save(&pouch->allocator, namespace_name,
                                        manifest, error);
  if (rc != LC_OK) {
    return rc;
  }
  lc_pouch_state_source_cache_cleanup(pouch);
  return lc_pouch_namespace_manifest_cleanup_obsolete(
      &pouch->allocator, namespace_name, manifest, now_unix,
      pouch->compaction_delete_grace_seconds, cleanup_deleted_count,
      cleanup_pending_count, error);
}

static int lc_pouch_state_compact_namespace_if_needed(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_namespace_manifest *manifest, int force,
    lc_pouch_maintenance_result *out, lc_error *error) {
  unsigned long candidate_count;
  uint64_t candidate_bytes;
  unsigned long cleanup_deleted_count;
  unsigned long cleanup_pending_count;
  uint64_t compacted_segment_id;
  uint64_t now_seconds;
  lc_pouch_state_compaction_capture capture;
  lc_pouch_namespace_logstore candidate_cache;
  int protected_snapshot_blocked;
  int rc;

  if (out != NULL) {
    memset(out, 0, sizeof(*out));
    rc = lc_pouch_maintenance_set_string(pouch, &out->namespace_name,
                                         namespace_name, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  cleanup_deleted_count = 0UL;
  cleanup_pending_count = 0UL;
  now_seconds = (uint64_t)lc_pouch_maintenance_now_seconds();
  rc = lc_pouch_namespace_manifest_cleanup_obsolete(
      &pouch->allocator, namespace_name, manifest, now_seconds,
      pouch->compaction_delete_grace_seconds, &cleanup_deleted_count,
      &cleanup_pending_count, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (out != NULL) {
    out->cleanup_deleted_count = cleanup_deleted_count;
    out->cleanup_pending_count = cleanup_pending_count;
  }
  if (!force && !pouch->background_compaction_enabled) {
    if (out != NULL) {
      out->skipped = 1;
    }
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("ns", namespace_name);
      fields[1] = lc_log_str_field("reason", "disabled");
      lc_log_trace(pouch->logger, "compaction.skip", fields, 2U);
    }
    return lc_pouch_maintenance_set_diagnostic(pouch, out, "disabled", error);
  }
  candidate_count = 0UL;
  {
    unsigned long segment_index;

    for (segment_index = 0UL; segment_index < manifest->segment_count;
         ++segment_index) {
      const char *segment_leaf = manifest->segment_leaves[segment_index];

      if (lc_pouch_state_manifest_segment_visible(manifest, segment_leaf) &&
          (manifest->active_segment == NULL ||
           strcmp(segment_leaf, manifest->active_segment) != 0)) {
        ++candidate_count;
      }
    }
  }
  if (out != NULL) {
    out->candidate_segment_count = candidate_count;
  }
  if (candidate_count == 0UL) {
    if (out != NULL) {
      out->skipped = 1;
    }
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("ns", namespace_name);
      fields[1] = lc_log_str_field("reason", "no-candidates");
      lc_log_trace(pouch->logger, "compaction.skip", fields, 2U);
    }
    return lc_pouch_maintenance_set_diagnostic(pouch, out, "no-candidates",
                                               error);
  }
  memset(&capture, 0, sizeof(capture));
  memset(&candidate_cache, 0, sizeof(candidate_cache));
  protected_snapshot_blocked = 0;
  rc = lc_pouch_state_compaction_capture_now(pouch, manifest, &capture, error);
  if (rc != LC_OK) {
    lc_pouch_maintenance_mark_aborted(pouch, out, "candidate-read-aborted");
    return rc;
  }
  candidate_cache.namespace_name =
      lc_strdup_with_allocator(&pouch->allocator, namespace_name);
  if (candidate_cache.namespace_name == NULL) {
    lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch compaction namespace name",
                        NULL, NULL, NULL);
  }
  rc =
      lc_pouch_state_cache_refresh(pouch, &candidate_cache, manifest, 1, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_compaction_protect_live_links(
        pouch, &capture, &candidate_cache, &protected_snapshot_blocked, error);
  }
  lc_free_with_allocator(&pouch->allocator, candidate_cache.namespace_name);
  candidate_cache.namespace_name = NULL;
  lc_pouch_namespace_logstore_clear_records(&pouch->allocator,
                                            &candidate_cache);
  if (rc != LC_OK) {
    lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
    lc_pouch_maintenance_mark_aborted(pouch, out, "candidate-read-aborted");
    return rc;
  }
  candidate_bytes = capture.candidate_bytes;
  candidate_count = (unsigned long)capture.candidate_count;
  lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
  if (out != NULL) {
    out->candidate_segment_count = candidate_count;
    out->candidate_bytes = candidate_bytes;
  }
  if (protected_snapshot_blocked) {
    if (out != NULL) {
      out->skipped = 1;
    }
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("ns", namespace_name);
      fields[1] = lc_log_str_field("reason", "protected-live-links");
      lc_log_trace(pouch->logger, "compaction.skip", fields, 2U);
    }
    return lc_pouch_maintenance_set_diagnostic(pouch, out,
                                               "protected-live-links", error);
  }
  if (candidate_count == 0UL) {
    if (out != NULL) {
      out->skipped = 1;
    }
    {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("ns", namespace_name);
      fields[1] = lc_log_str_field("reason", "no-candidates");
      lc_log_trace(pouch->logger, "compaction.skip", fields, 2U);
    }
    return lc_pouch_maintenance_set_diagnostic(pouch, out, "no-candidates",
                                               error);
  }
  if (!force && candidate_count < pouch->compaction_min_segment_count) {
    if (out != NULL) {
      out->skipped = 1;
    }
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("ns", namespace_name);
      fields[1] = lc_log_str_field("reason", "below-segment-threshold");
      fields[2] = lc_log_u64_field("count", candidate_count);
      lc_log_trace(pouch->logger, "compaction.skip", fields, 3U);
    }
    return lc_pouch_maintenance_set_diagnostic(
        pouch, out, "below-segment-threshold", error);
  }
  if (!force && candidate_bytes < pouch->compaction_min_reclaimable_bytes) {
    if (out != NULL) {
      out->skipped = 1;
    }
    {
      pslog_field fields[3];

      fields[0] = lc_log_str_field("ns", namespace_name);
      fields[1] = lc_log_str_field("reason", "below-reclaimable-threshold");
      fields[2] = lc_log_u64_field("reclaim_bytes", candidate_bytes);
      lc_log_trace(pouch->logger, "compaction.skip", fields, 3U);
    }
    return lc_pouch_maintenance_set_diagnostic(
        pouch, out, "below-reclaimable-threshold", error);
  }
  compacted_segment_id = manifest->max_segment_id;
  if (manifest->latest_snapshot_segment_id > compacted_segment_id) {
    compacted_segment_id = manifest->latest_snapshot_segment_id;
  }
  if (compacted_segment_id == LC_U64_MAX) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch snapshot sequence overflow", NULL, NULL,
                        "pouch");
  }
  ++compacted_segment_id;
  {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("ns", namespace_name);
    fields[1] = lc_log_u64_field("count", candidate_count);
    fields[2] = lc_log_u64_field("reclaim_bytes", candidate_bytes);
    fields[3] = lc_log_bool_field("force", force);
    lc_log_debug(pouch->logger, "compaction.start", fields, 4U);
  }
  {
    const char *abort_diagnostic;

    abort_diagnostic = NULL;
    rc = lc_pouch_state_compact_namespace(
        pouch, namespace_name, manifest, &cleanup_deleted_count,
        &cleanup_pending_count, &abort_diagnostic,
        out != NULL ? &out->compacted : NULL, error);
    if (rc != LC_OK) {
      lc_pouch_maintenance_mark_aborted(pouch, out, abort_diagnostic);
      {
        pslog_field fields[4];

        fields[0] = lc_log_str_field("ns", namespace_name);
        fields[1] = lc_log_str_field("reason", abort_diagnostic);
        fields[2] = lc_log_error_field("error", error);
        fields[3] = lc_log_code_field(error);
        lc_log_error(pouch->logger, "compaction.error", fields, 4U);
      }
      return rc;
    }
    if (out != NULL && !out->compacted) {
      out->skipped = 1;
      return lc_pouch_maintenance_set_diagnostic(
          pouch, out,
          abort_diagnostic != NULL ? abort_diagnostic : "no-candidates", error);
    }
  }
  if (out != NULL) {
    out->compacted = 1;
    out->compacted_segment_id = compacted_segment_id;
    out->cleanup_deleted_count = cleanup_deleted_count;
    out->cleanup_pending_count = cleanup_pending_count;
  }
  {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("ns", namespace_name);
    fields[1] = lc_log_u64_field("segment", compacted_segment_id);
    fields[2] = lc_log_u64_field("reclaim_bytes", candidate_bytes);
    fields[3] = lc_log_u64_field("records", cleanup_deleted_count);
    fields[4] = lc_log_u64_field("pending", cleanup_pending_count);
    lc_log_debug(pouch->logger, "compaction.complete", fields, 5U);
  }
  if (cleanup_pending_count != 0UL) {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("ns", namespace_name);
    fields[1] = lc_log_u64_field("pending", cleanup_pending_count);
    fields[2] = lc_log_str_field("reason", "cleanup-pending");
    lc_log_warn(pouch->logger, "compaction.cleanup.pending", fields, 3U);
  }
  return lc_pouch_maintenance_set_diagnostic(pouch, out, "compacted", error);
}

static int lc_pouch_state_compact_namespace(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_namespace_manifest *manifest, unsigned long *cleanup_deleted_count,
    unsigned long *cleanup_pending_count, const char **abort_diagnostic,
    int *compacted_out, lc_error *error) {
  lc_pouch_namespace_logstore snapshot_cache;
  lc_pouch_state_compaction_capture capture;
  char *snapshot_leaf;
  uint64_t compacted_segment_id;
  int snapshot_installed;
  int protected_snapshot_blocked;
  int rc;

  snapshot_installed = 0;
  protected_snapshot_blocked = 0;
  if (compacted_out != NULL) {
    *compacted_out = 0;
  }
  if (abort_diagnostic != NULL) {
    *abort_diagnostic = NULL;
  }
  if (manifest->segment_count == 0UL) {
    return LC_OK;
  }
  memset(&snapshot_cache, 0, sizeof(snapshot_cache));
  snapshot_cache.namespace_name =
      lc_strdup_with_allocator(&pouch->allocator, namespace_name);
  if (snapshot_cache.namespace_name == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch snapshot namespace name",
                        NULL, NULL, NULL);
  }
  memset(&capture, 0, sizeof(capture));
  rc = lc_pouch_state_compaction_capture_now(pouch, manifest, &capture, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, snapshot_cache.namespace_name);
    if (abort_diagnostic != NULL) {
      *abort_diagnostic = "validation-read-aborted";
    }
    return rc;
  }
  rc = lc_pouch_state_cache_refresh(pouch, &snapshot_cache, manifest, 1, error);
  if (rc != LC_OK) {
    if (abort_diagnostic != NULL) {
      *abort_diagnostic = "snapshot-refresh-aborted";
    }
    lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
    lc_free_with_allocator(&pouch->allocator, snapshot_cache.namespace_name);
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator,
                                              &snapshot_cache);
    return rc;
  }
  rc = lc_pouch_state_compaction_protect_live_links(
      pouch, &capture, &snapshot_cache, &protected_snapshot_blocked, error);
  if (rc != LC_OK) {
    if (abort_diagnostic != NULL) {
      *abort_diagnostic = "candidate-protection-aborted";
    }
    lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
    lc_free_with_allocator(&pouch->allocator, snapshot_cache.namespace_name);
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator,
                                              &snapshot_cache);
    return rc;
  }
  if (protected_snapshot_blocked || capture.candidate_count == 0U) {
    if (abort_diagnostic != NULL) {
      *abort_diagnostic =
          protected_snapshot_blocked ? "protected-live-links" : "no-candidates";
    }
    lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
    lc_free_with_allocator(&pouch->allocator, snapshot_cache.namespace_name);
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator,
                                              &snapshot_cache);
    return LC_OK;
  }
  rc = lc_pouch_state_compaction_capture_records(pouch, &capture,
                                                 &snapshot_cache, error);
  if (rc != LC_OK) {
    if (abort_diagnostic != NULL) {
      *abort_diagnostic = "capture-records-aborted";
    }
    lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
    lc_free_with_allocator(&pouch->allocator, snapshot_cache.namespace_name);
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator,
                                              &snapshot_cache);
    return rc;
  }
  compacted_segment_id = manifest->max_segment_id;
  if (manifest->latest_snapshot_segment_id > compacted_segment_id) {
    compacted_segment_id = manifest->latest_snapshot_segment_id;
  }
  if (compacted_segment_id == LC_U64_MAX) {
    lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
    lc_free_with_allocator(&pouch->allocator, snapshot_cache.namespace_name);
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator,
                                              &snapshot_cache);
    if (abort_diagnostic != NULL) {
      *abort_diagnostic = "snapshot-sequence-overflow";
    }
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch snapshot sequence overflow", NULL, NULL,
                        "pouch");
  }
  ++compacted_segment_id;
  snapshot_leaf =
      lc_pouch_namespace_snapshot_leaf(&pouch->allocator, compacted_segment_id);
  if (snapshot_leaf == NULL) {
    lc_free_with_allocator(&pouch->allocator, snapshot_leaf);
    lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
    lc_free_with_allocator(&pouch->allocator, snapshot_cache.namespace_name);
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator,
                                              &snapshot_cache);
    if (abort_diagnostic != NULL) {
      *abort_diagnostic = "snapshot-prepare-aborted";
    }
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state snapshot name", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_state_write_snapshot(pouch, manifest, snapshot_leaf,
                                     &snapshot_cache, &capture, error);
  if (rc == LC_OK) {
#ifdef LOCKDC_TEST_BUILD
    if (lc_pouch_test_after_snapshot_write_hook != NULL) {
      rc = lc_pouch_test_after_snapshot_write_hook(
          lc_pouch_test_after_snapshot_write_context, error);
    }
#endif
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_cache_refresh(pouch, &snapshot_cache, manifest, 1,
                                      error);
    if (rc == LC_OK) {
      rc = lc_pouch_state_compaction_validate_capture(pouch, manifest, &capture,
                                                      &snapshot_cache, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_namespace_manifest_install_snapshot(
          &pouch->allocator, namespace_name, manifest, snapshot_leaf,
          compacted_segment_id, error);
      if (rc == LC_OK) {
        snapshot_installed = 1;
      } else if (abort_diagnostic != NULL) {
        *abort_diagnostic = "snapshot-install-aborted";
      }
    } else if (abort_diagnostic != NULL) {
      *abort_diagnostic = "validation-drift-aborted";
    }
  } else if (abort_diagnostic != NULL) {
    *abort_diagnostic = "snapshot-write-aborted";
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_queue_compacted_files(pouch, manifest, namespace_name,
                                              &capture, cleanup_deleted_count,
                                              cleanup_pending_count, error);
    if (rc != LC_OK && abort_diagnostic != NULL) {
      *abort_diagnostic = "obsolete-cleanup-aborted";
    }
  }
  if (rc == LC_OK) {
    lc_pouch_namespace_logstore *live_cache;

    live_cache =
        lc_pouch_namespace_logstore_find(pouch, namespace_name, 0, NULL);
    if (live_cache != NULL) {
      lc_pouch_namespace_logstore_clear_records(&pouch->allocator, live_cache);
      live_cache->initialized = 0;
      live_cache->max_segment_id = 0UL;
      live_cache->segment_count = 0UL;
      live_cache->max_version = 0UL;
    }
  }
  if (rc == LC_OK) {
    if (compacted_out != NULL) {
      *compacted_out = 1;
    }
  } else if (!snapshot_installed) {
    char *snapshot_path;

    snapshot_path =
        lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                  "snapshots", snapshot_leaf);
    if (snapshot_path != NULL) {
      (void)unlink(snapshot_path);
      lc_free_with_allocator(&pouch->allocator, snapshot_path);
    }
  }
  lc_free_with_allocator(&pouch->allocator, snapshot_leaf);
  lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
  lc_free_with_allocator(&pouch->allocator, snapshot_cache.namespace_name);
  snapshot_cache.namespace_name = NULL;
  lc_pouch_namespace_logstore_clear_records(&pouch->allocator, &snapshot_cache);
  return rc;
}

static int lc_pouch_maintenance_run_locked(
    lc_pouch *pouch, const lc_pouch_maintenance_options *options,
    lc_pouch_maintenance_result *out, lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  const char *namespace_name;
  unsigned long cleanup_deleted_count;
  unsigned long cleanup_pending_count;
  uint64_t now_seconds;
  int force;
  int rc;

  if (out != NULL) {
    memset(out, 0, sizeof(*out));
  }
  if (pouch == NULL || options == NULL || options->namespace_name == NULL ||
      options->namespace_name[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch maintenance requires a namespace", NULL, NULL,
                        NULL);
  }
  namespace_name = options->namespace_name;
  force = options->force ? 1 : 0;
  cleanup_deleted_count = 0UL;
  cleanup_pending_count = 0UL;
  {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("ns", namespace_name);
    fields[1] = lc_log_bool_field("force", force);
    fields[2] = lc_log_bool_field("cleanup_only", options->cleanup_only);
    lc_log_debug(pouch->logger, "maintenance.start", fields, 3U);
  }
  rc = lc_pouch_namespace_ensure_layout(&pouch->allocator, pouch->root_path,
                                        namespace_name, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_namespace_manifest_open(
      &pouch->allocator, pouch->root_path, namespace_name, &manifest,
      &cleanup_deleted_count, &cleanup_pending_count, error);
  if (rc != LC_OK) {
    return rc;
  }
  now_seconds = (uint64_t)lc_pouch_maintenance_now_seconds();
  rc = lc_pouch_namespace_manifest_cleanup_obsolete(
      &pouch->allocator, namespace_name, &manifest, now_seconds,
      pouch->compaction_delete_grace_seconds, &cleanup_deleted_count,
      &cleanup_pending_count, error);
  if (rc != LC_OK) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  if (out != NULL) {
    out->cleanup_deleted_count = cleanup_deleted_count;
    out->cleanup_pending_count = cleanup_pending_count;
  }
  if (options->retention_updated_before_unix > 0L) {
    rc = lc_pouch_state_retention_sweep(pouch, namespace_name,
                                        options->retention_updated_before_unix,
                                        out, error);
  } else if (options->cleanup_only) {
    if (out != NULL) {
      out->skipped = 1;
      rc = lc_pouch_maintenance_set_string(pouch, &out->namespace_name,
                                           namespace_name, error);
      if (rc != LC_OK) {
        lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
        return rc;
      }
    }
    rc = lc_pouch_maintenance_set_diagnostic(
        pouch, out,
        cleanup_pending_count != 0UL ? "cleanup-pending" : "cleanup-complete",
        error);
  } else {
    rc = lc_pouch_state_compact_namespace_if_needed(
        pouch, namespace_name, &manifest, force, out, error);
    if (out != NULL) {
      out->cleanup_deleted_count += cleanup_deleted_count;
      out->cleanup_pending_count += cleanup_pending_count;
    }
  }
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  if (rc == LC_OK) {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("ns", namespace_name);
    fields[1] =
        lc_log_bool_field("compacted", out != NULL ? out->compacted : 0);
    fields[2] = lc_log_bool_field("skipped", out != NULL ? out->skipped : 0);
    fields[3] =
        lc_log_str_field("reason", out != NULL ? out->diagnostic : NULL);
    fields[4] = lc_log_u64_field("reclaim_bytes",
                                 out != NULL ? out->candidate_bytes : 0UL);
    lc_log_debug(pouch->logger, "maintenance.complete", fields, 5U);
    if (out != NULL && out->cleanup_pending_count != 0UL) {
      pslog_field warn_fields[3];

      warn_fields[0] = lc_log_str_field("ns", namespace_name);
      warn_fields[1] = lc_log_u64_field("pending", out->cleanup_pending_count);
      warn_fields[2] = lc_log_str_field("reason", out->diagnostic);
      lc_log_warn(pouch->logger, "maintenance.cleanup.pending", warn_fields,
                  3U);
    }
  } else {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("ns", namespace_name);
    fields[1] = lc_log_error_field("error", error);
    fields[2] = lc_log_code_field(error);
    lc_log_error(pouch->logger, "maintenance.error", fields, 3U);
  }
  return rc;
}

int lc_pouch_maintenance_run(lc_pouch *pouch,
                             const lc_pouch_maintenance_options *options,
                             lc_pouch_maintenance_result *out,
                             lc_error *error) {
  lc_pouch_state_namespace_lock lock;
  const char *namespace_name;
  lc_pouch_state_commit_group *commit_group;
  int owns_commit_group;
  int rc;

  namespace_name = options != NULL ? options->namespace_name : NULL;
  commit_group = NULL;
  owns_commit_group = 0;
  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0') {
    return lc_pouch_maintenance_run_locked(pouch, options, out, error);
  }
  rc = lc_pouch_writer_mode_operation_begin(pouch, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_lock_namespace_for_mutation(pouch, namespace_name, &lock,
                                                  error);
  if (rc != LC_OK) {
    lc_pouch_writer_mode_operation_end(pouch);
    return rc;
  }
  rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                         &owns_commit_group, error);
  if (rc != LC_OK) {
    lc_pouch_state_namespace_lock_release(&lock);
    lc_pouch_writer_mode_operation_end(pouch);
    return rc;
  }
  rc = lc_pouch_maintenance_run_locked(pouch, options, out, error);
  rc = lc_pouch_state_finish_commit_group(commit_group, owns_commit_group, rc,
                                          error);
  if (rc != LC_OK) {
    lc_pouch_state_cache_invalidate_namespace(pouch, namespace_name);
  }
  lc_pouch_state_namespace_lock_release(&lock);
  lc_pouch_writer_mode_operation_end(pouch);
  return rc;
}

void lc_pouch_maintenance_result_cleanup(const lc_allocator *allocator,
                                         lc_pouch_maintenance_result *result) {
  if (result == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, result->namespace_name);
  lc_free_with_allocator(allocator, result->diagnostic);
  memset(result, 0, sizeof(*result));
}

static int lc_pouch_state_cache_apply_write(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_namespace_manifest *manifest, const char *key,
    const char *content_type, const char *etag,
    const lc_pouch_state_payload_span *payload_span,
    const char *payload_context, const unsigned char *metadata,
    size_t metadata_length, lc_pouch_generation version, uint64_t bytes,
    uint64_t cipher_bytes, const char *descriptor,
    lc_pouch_unix_seconds updated_at_unix, int has_query_hidden,
    int query_hidden, int found, unsigned char record_type) {
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_entry entry;
  lc_error ignored;
  int rc;

  cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 0, NULL);
  if (cache == NULL || !cache->initialized) {
    return LC_OK;
  }
  memset(&entry, 0, sizeof(entry));
  entry.key = (char *)key;
  entry.content_type = (char *)content_type;
  entry.etag = (char *)etag;
  if (payload_span != NULL) {
    entry.payload_span = *payload_span;
  }
  entry.payload_context = (char *)payload_context;
  entry.descriptor = (char *)descriptor;
  entry.metadata = (unsigned char *)metadata;
  entry.metadata_length = metadata_length;
  entry.index_seq = manifest->state_max_version;
  entry.version = version;
  entry.bytes = bytes;
  entry.cipher_bytes = cipher_bytes;
  entry.updated_at_unix = updated_at_unix;
  entry.has_query_hidden = has_query_hidden;
  entry.query_hidden = query_hidden;
  entry.seen = 1;
  entry.found = found;
  entry.record_type = record_type;
  lc_error_init(&ignored);
  rc = lc_pouch_state_cache_apply_entry(pouch, cache, &entry, &ignored);
  lc_error_cleanup(&ignored);
  if (rc != LC_OK) {
    lc_pouch_namespace_logstore_clear_records(&pouch->allocator, cache);
    cache->initialized = 0;
    cache->max_segment_id = 0UL;
    cache->segment_count = 0UL;
    cache->max_version = 0UL;
    return LC_OK;
  }
  if (rc == LC_OK && manifest->max_segment_id > cache->max_segment_id) {
    cache->max_segment_id = manifest->max_segment_id;
  }
  if (rc == LC_OK) {
    cache->segment_count = manifest->segment_count;
  }
  return rc;
}

static int lc_pouch_state_append_tombstone(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_namespace_manifest *manifest, const char *key, const char *etag,
    lc_pouch_generation version, lc_pouch_unix_seconds updated_at_unix,
    unsigned char record_type, lc_error *error) {
  unsigned char *meta;
  size_t meta_len;
  int rc;
  unsigned char delete_record_type;

  meta = NULL;
  meta_len = 0U;
  delete_record_type = record_type == LC_POUCH_STATE_RECORD_OBJECT_DELETE ||
                               record_type == LC_POUCH_STATE_RECORD_OBJECT_PUT
                           ? LC_POUCH_STATE_RECORD_OBJECT_DELETE
                           : LC_POUCH_STATE_RECORD_STATE_DELETE;
  rc = lc_pouch_state_encode_delete_meta(&pouch->allocator, version,
                                         updated_at_unix, etag, &meta,
                                         &meta_len, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_append_binary_record(pouch, namespace_name, manifest,
                                           delete_record_type, key, strlen(key),
                                           meta, meta_len, error);
  lc_free_with_allocator(&pouch->allocator, meta);
  return rc;
}

static int lc_pouch_state_append_record(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_namespace_manifest *manifest, unsigned char record_type,
    const char *key, const char *content_type, const char *etag,
    const lc_pouch_state_payload_span *payload_span,
    const char *payload_context, const unsigned char *metadata,
    size_t metadata_length, lc_pouch_generation version, uint64_t bytes,
    uint64_t cipher_bytes, const char *descriptor,
    lc_pouch_unix_seconds updated_at_unix, int has_query_hidden,
    int query_hidden, lc_error *error) {
  unsigned char *meta;
  size_t meta_len;
  int rc;

  if (record_type != LC_POUCH_STATE_RECORD_STATE_PUT &&
      record_type != LC_POUCH_STATE_RECORD_OBJECT_PUT &&
      record_type != LC_POUCH_STATE_RECORD_STATE_LINK &&
      record_type != LC_POUCH_STATE_RECORD_STATE_META) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state record type is invalid", NULL, NULL,
                        "pouch");
  }
  meta = NULL;
  meta_len = 0U;
  rc = lc_pouch_state_encode_payload_meta(
      &pouch->allocator, version, updated_at_unix, bytes, cipher_bytes,
      content_type, etag, descriptor, payload_span, payload_context, metadata,
      metadata_length, 0U, has_query_hidden, query_hidden, &meta, &meta_len,
      error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_append_binary_record(pouch, namespace_name, manifest,
                                             record_type, key, strlen(key),
                                             meta, meta_len, error);
  }
  lc_free_with_allocator(&pouch->allocator, meta);
  return rc;
}

static int lc_pouch_state_append_staged_commit_batch(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_namespace_manifest *manifest, const char *key,
    const char *staged_key, const lc_pouch_state_entry *staged,
    const unsigned char *metadata, size_t metadata_length,
    lc_pouch_generation version, lc_pouch_generation decision_version,
    lc_pouch_generation discard_version, lc_pouch_unix_seconds updated_at_unix,
    lc_error *error) {
  lc_pouch_state_binary_append_item items[3];
  unsigned char *link_meta;
  unsigned char *decision_meta;
  unsigned char *delete_meta;
  size_t link_meta_len;
  size_t decision_meta_len;
  size_t delete_meta_len;
  int rc;

  if (staged == NULL || !staged->found) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged commit batch requires staged state", NULL,
                        NULL, "pouch");
  }
  link_meta = NULL;
  decision_meta = NULL;
  delete_meta = NULL;
  link_meta_len = 0U;
  decision_meta_len = 0U;
  delete_meta_len = 0U;
  rc = lc_pouch_state_encode_payload_meta(
      &pouch->allocator, version, updated_at_unix, staged->bytes,
      staged->cipher_bytes, staged->content_type, staged->etag,
      staged->descriptor, &staged->payload_span, staged->payload_context,
      metadata, metadata_length, 0U, staged->has_query_hidden,
      staged->query_hidden, &link_meta, &link_meta_len, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_encode_decision_meta(
        &pouch->allocator, decision_version, staged->etag,
        LC_POUCH_STATE_DECISION_COMMITTED, &decision_meta, &decision_meta_len,
        error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_encode_delete_meta(
        &pouch->allocator, discard_version, updated_at_unix, staged->etag,
        &delete_meta, &delete_meta_len, error);
  }
  if (rc == LC_OK) {
    memset(items, 0, sizeof(items));
    items[0].record_type = LC_POUCH_STATE_RECORD_STATE_LINK;
    items[0].key = key;
    items[0].key_len = strlen(key);
    items[0].meta = link_meta;
    items[0].meta_len = link_meta_len;
    items[1].record_type = LC_POUCH_STATE_RECORD_DECISION;
    items[1].key = staged_key;
    items[1].key_len = strlen(staged_key);
    items[1].meta = decision_meta;
    items[1].meta_len = decision_meta_len;
    items[2].record_type = LC_POUCH_STATE_RECORD_STATE_DELETE;
    items[2].key = staged_key;
    items[2].key_len = strlen(staged_key);
    items[2].meta = delete_meta;
    items[2].meta_len = delete_meta_len;
    rc = lc_pouch_state_append_binary_records(pouch, namespace_name, manifest,
                                              items, 3U, error);
  }
  lc_free_with_allocator(&pouch->allocator, link_meta);
  lc_free_with_allocator(&pouch->allocator, decision_meta);
  lc_free_with_allocator(&pouch->allocator, delete_meta);
  return rc;
}

static int lc_pouch_state_append_staged_discard_batch(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_namespace_manifest *manifest, const char *staged_key,
    const char *etag, lc_pouch_generation decision_version,
    lc_pouch_generation tombstone_version,
    lc_pouch_unix_seconds updated_at_unix, lc_error *error) {
  lc_pouch_state_binary_append_item items[2];
  unsigned char *decision_meta;
  unsigned char *delete_meta;
  size_t decision_meta_len;
  size_t delete_meta_len;
  int rc;

  decision_meta = NULL;
  delete_meta = NULL;
  decision_meta_len = 0U;
  delete_meta_len = 0U;
  rc = lc_pouch_state_encode_decision_meta(
      &pouch->allocator, decision_version, etag,
      LC_POUCH_STATE_DECISION_DISCARDED, &decision_meta, &decision_meta_len,
      error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_encode_delete_meta(&pouch->allocator, tombstone_version,
                                           updated_at_unix, etag, &delete_meta,
                                           &delete_meta_len, error);
  }
  if (rc == LC_OK) {
    memset(items, 0, sizeof(items));
    items[0].record_type = LC_POUCH_STATE_RECORD_DECISION;
    items[0].key = staged_key;
    items[0].key_len = strlen(staged_key);
    items[0].meta = decision_meta;
    items[0].meta_len = decision_meta_len;
    items[1].record_type = LC_POUCH_STATE_RECORD_STATE_DELETE;
    items[1].key = staged_key;
    items[1].key_len = strlen(staged_key);
    items[1].meta = delete_meta;
    items[1].meta_len = delete_meta_len;
    rc = lc_pouch_state_append_binary_records(pouch, namespace_name, manifest,
                                              items, 2U, error);
  }
  lc_free_with_allocator(&pouch->allocator, decision_meta);
  lc_free_with_allocator(&pouch->allocator, delete_meta);
  return rc;
}

static char *lc_pouch_state_staged_key(const lc_allocator *allocator,
                                       const char *key, const char *txn_id) {
  size_t key_len;
  size_t txn_len;
  size_t suffix_len;
  char *staged;

  if (key == NULL || key[0] == '\0' || txn_id == NULL || txn_id[0] == '\0') {
    return NULL;
  }
  key_len = strlen(key);
  txn_len = strlen(txn_id);
  suffix_len = sizeof("/.staging/") - 1U;
  staged = (char *)lc_alloc_with_allocator(allocator,
                                           key_len + suffix_len + txn_len + 1U);
  if (staged == NULL) {
    return NULL;
  }
  memcpy(staged, key, key_len);
  memcpy(staged + key_len, "/.staging/", suffix_len);
  memcpy(staged + key_len + suffix_len, txn_id, txn_len);
  staged[key_len + suffix_len + txn_len] = '\0';
  return staged;
}

static int lc_pouch_state_recover_staged_decisions_locked(
    lc_pouch *pouch, const char *namespace_name, lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_decision *decisions;
  lc_pouch_state_decision *decision;
  unsigned long recovered_count;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_recover_staged_decisions requires "
                        "pouch and namespace",
                        NULL, NULL, NULL);
  }
  cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 1, error);
  if (cache == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  if (cache->decision_recovery_checked) {
    return LC_OK;
  }
  memset(&manifest, 0, sizeof(manifest));
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                        namespace_name, &manifest, NULL, NULL,
                                        error);
  if (rc != LC_OK) {
    return rc;
  }
  decisions = NULL;
  recovered_count = 0UL;
  rc = lc_pouch_state_collect_decisions(pouch, &manifest, &decisions, error);
  for (decision = decisions; rc == LC_OK && decision != NULL;
       decision = decision->next) {
    lc_pouch_state_entry staged;
    lc_pouch_generation max_version;
    lc_pouch_generation tombstone_version;
    lc_pouch_unix_seconds updated_at_unix;

    memset(&staged, 0, sizeof(staged));
    rc = lc_pouch_state_scan(pouch, &manifest, decision->staged_key, &staged,
                             &max_version, error);
    if (rc != LC_OK) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &staged);
      break;
    }
    if (!staged.found) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &staged);
      continue;
    }
    (void)max_version;
    tombstone_version = staged.version + 1UL;
    updated_at_unix = lc_pouch_maintenance_now_seconds();
    rc = lc_pouch_state_append_tombstone(
        pouch, namespace_name, &manifest, decision->staged_key, decision->etag,
        tombstone_version, updated_at_unix, LC_POUCH_STATE_RECORD_STATE_DELETE,
        error);
    if (rc == LC_OK) {
      (void)lc_pouch_state_cache_apply_write(
          pouch, namespace_name, &manifest, decision->staged_key, NULL,
          decision->etag, NULL, NULL, NULL, 0U, tombstone_version, 0UL, 0UL,
          NULL, updated_at_unix, 0, 0, 0, LC_POUCH_STATE_RECORD_STATE_DELETE);
      recovered_count++;
    }
    lc_pouch_state_entry_cleanup(&pouch->allocator, &staged);
  }
  if (rc == LC_OK) {
    cache->decision_recovery_checked = 1;
    if (recovered_count != 0UL) {
      pslog_field fields[2];

      fields[0] = lc_log_str_field("ns", namespace_name);
      fields[1] = lc_log_u64_field("count", recovered_count);
      lc_log_debug(pouch->logger, "recovery.staged.complete", fields, 2U);
    }
  }
  lc_pouch_state_decisions_cleanup(&pouch->allocator, decisions);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

int lc_pouch_state_recover_staged_decisions(lc_pouch *pouch,
                                            const char *namespace_name,
                                            lc_error *error) {
  lc_pouch_state_namespace_lock lock;
  lc_pouch_namespace_logstore *cache;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0') {
    return lc_pouch_state_recover_staged_decisions_locked(pouch, namespace_name,
                                                          error);
  }
  cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 0, NULL);
  if (cache != NULL && cache->decision_recovery_checked) {
    return LC_OK;
  }
  rc = lc_pouch_state_lock_namespace_for_mutation(pouch, namespace_name, &lock,
                                                  error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_recover_staged_decisions_locked(pouch, namespace_name,
                                                      error);
  lc_pouch_state_namespace_lock_release(&lock);
  return rc;
}

int lc_pouch_state_write_locked(lc_pouch *pouch, const char *namespace_name,
                                const char *key, lc_source *body,
                                const lc_pouch_state_write_options *options,
                                lc_pouch_state_write_result *out,
                                lc_error *error) {
  lc_pouch_state_entry current;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_namespace_manifest manifest;
  lc_pouch_state_hash_source hash_source;
  const char *content_type;
  EVP_MD_CTX *hash_ctx;
  char *segment_path;
  lc_pouch_state_append_lock append_lock;
  char *descriptor;
  char *crypto_context;
  char *payload_context;
  lc_pouch_state_payload_span payload_span;
  char *etag;
  unsigned char *meta;
  unsigned char *final_meta;
  const unsigned char *materialized_plain;
  unsigned char *materialized_stored;
  const char placeholder_etag[] =
      "0000000000000000000000000000000000000000000000000000000000000000";
  int fd;
  lc_pouch_generation max_version;
  lc_pouch_generation version;
  uint64_t bytes;
  uint64_t cipher_bytes;
  unsigned long stored_crc;
  uint64_t segment_size;
  uint64_t payload_offset;
  uint64_t record_end;
  size_t meta_len;
  size_t final_meta_len;
  size_t materialized_plain_len;
  size_t materialized_stored_len;
  lc_pouch_unix_seconds updated_at_unix;
  int has_query_hidden;
  int query_hidden;
  const unsigned char *metadata;
  size_t metadata_length;
  lc_pouch_generation index_seq;
  int rc;
  int manifest_from_cache;
  int retain_active_append_fd;
  int single_writer;
  int use_materialized_record;
  int materialized_transform_ready;
  int namespace_locked;
  int streaming_projection_mutex_released;
  int projection_rc;
  uint64_t writer_mode_epoch;
  unsigned char put_record_type;
  lc_pouch_state_precondition_view precondition_view;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || body == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_write requires pouch, namespace, key, "
                        "body and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  memset(&manifest, 0, sizeof(manifest));
  memset(&current, 0, sizeof(current));
  cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 0, NULL);
  single_writer = lc_pouch_single_writer_snapshot(pouch, &writer_mode_epoch);
  namespace_locked =
      lc_pouch_state_namespace_lock_is_held(pouch, namespace_name);
  manifest_from_cache = single_writer && cache != NULL && cache->initialized &&
                        cache->writer_mode_epoch == writer_mode_epoch &&
                        cache->namespace_path != NULL &&
                        cache->active_segment_leaf != NULL;
  if (manifest_from_cache) {
    rc = lc_pouch_state_cache_manifest_borrow(pouch, cache, &manifest, error);
    if (rc == LC_OK) {
      max_version = cache->max_version;
      rc = lc_pouch_state_entry_from_cache_record(
          pouch, lc_pouch_state_cache_record_find(cache, key), &current, error);
    }
  } else {
    rc = lc_pouch_namespace_ensure(&pouch->allocator, pouch->root_path,
                                   namespace_name, error);
    if (rc == LC_OK) {
      rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                            namespace_name, &manifest, NULL,
                                            NULL, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_cache_lookup(pouch, namespace_name, &manifest, key,
                                       &current, &max_version, error);
    }
    cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 0, NULL);
    single_writer = lc_pouch_single_writer_snapshot(pouch, &writer_mode_epoch);
  }
  if (rc != LC_OK) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  retain_active_append_fd = 0;
  if (options != NULL && options->view_precondition != NULL) {
    lc_pouch_state_precondition_view_from_entry(&current, &precondition_view);
    rc = options->view_precondition(&precondition_view,
                                    options->view_precondition_context, error);
    if (rc != LC_OK) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return rc;
    }
  }
  if (options != NULL && options->expected_etag != NULL) {
    if (!current.found || strcmp(current.etag, options->expected_etag) != 0) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch state etag precondition failed", NULL, NULL,
                          NULL);
    }
  }
  if (options != NULL && options->has_expected_version) {
    if (options->expected_version == 0UL) {
      if (current.found && current.payload_span.present) {
        lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
        lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
        return lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch state version precondition failed", NULL,
                            NULL, NULL);
      }
    } else if (!current.found || current.version != options->expected_version) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch state version precondition failed", NULL, NULL,
                          NULL);
    }
  }
  if (options != NULL && options->create_if_absent && current.found &&
      current.payload_span.present) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state create-if-absent precondition failed",
                        NULL, NULL, NULL);
  }
  if (options != NULL && options->precondition != NULL) {
    rc = options->precondition(options->precondition_context, error);
    if (rc != LC_OK) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return rc;
    }
  }
  (void)max_version;
  version = current.found && current.payload_span.present
                ? current.version + 1UL
                : 1UL;
  updated_at_unix = lc_pouch_maintenance_now_seconds();
  etag = NULL;
  descriptor = NULL;
  segment_path = NULL;
  append_lock.fd = -1;
  append_lock.process_mutex = NULL;
  append_lock.exclusive_gate = NULL;
  payload_context = NULL;
  memset(&payload_span, 0, sizeof(payload_span));
  meta = NULL;
  final_meta = NULL;
  materialized_plain = NULL;
  materialized_stored = NULL;
  materialized_plain_len = 0U;
  materialized_stored_len = 0U;
  record_end = 0UL;
  use_materialized_record =
      lc_source_memory_view(body, &materialized_plain,
                            &materialized_plain_len) &&
      materialized_plain_len <= LC_POUCH_CRYPTO_MEMORY_TRANSFORM_MAX_BYTES;
  materialized_transform_ready = 0;
  streaming_projection_mutex_released = 0;
  content_type = options != NULL && options->content_type != NULL
                     ? options->content_type
                     : "application/octet-stream";
  put_record_type = options != NULL && options->object_record
                        ? LC_POUCH_STATE_RECORD_OBJECT_PUT
                        : LC_POUCH_STATE_RECORD_STATE_PUT;
  index_seq = 0UL;
  has_query_hidden = current.has_query_hidden;
  query_hidden = current.query_hidden;
  if (options != NULL && options->has_query_hidden) {
    has_query_hidden = 1;
    query_hidden = options->query_hidden;
  } else if (current.record_type == LC_POUCH_STATE_RECORD_STATE_META &&
             !current.payload_span.present) {
    /* A first lease claim is metadata-only and must not hide its first body. */
    has_query_hidden = 0;
    query_hidden = 0;
  }
  metadata = current.metadata;
  metadata_length = current.metadata_length;
  if (options != NULL && options->has_metadata) {
    metadata = options->metadata;
    metadata_length = options->metadata_length;
  }
  payload_context =
      lc_pouch_state_crypto_context(&pouch->allocator, namespace_name, key);
  if (payload_context == NULL) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state payload context", NULL,
                        NULL, NULL);
  }
  /* A bounded SDK memory source has already been materialized by its caller.
   * Its compression/encryption and etag work neither observes nor mutates the
   * resident projection, so exclusive exact-key mutations perform it before
   * claiming the physical appender. Streaming sources deliberately stay out of
   * this path and retain direct source-to-segment flow below. */
  if (use_materialized_record && single_writer && !namespace_locked) {
    bytes = (uint64_t)materialized_plain_len;
    cipher_bytes = 0UL;
    stored_crc = (unsigned long)crc32(0L, Z_NULL, 0);
    lc_pouch_state_projection_mutex_unlock(pouch, namespace_name);
    rc = lc_pouch_crypto_transform_memory(
        pouch->crypto, payload_context, materialized_plain,
        materialized_plain_len,
        options == NULL || !options->disable_compression, &materialized_stored,
        &materialized_stored_len, &stored_crc, &descriptor, error);
    if (rc == LC_OK) {
      cipher_bytes = (uint64_t)materialized_stored_len;
      etag = lc_pouch_state_hash_bytes(&pouch->allocator, materialized_plain,
                                       materialized_plain_len, error);
      if (etag == NULL) {
        rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      }
    }
    projection_rc =
        lc_pouch_state_projection_mutex_lock(pouch, namespace_name, error);
    if (projection_rc != LC_OK && rc == LC_OK) {
      rc = projection_rc;
    }
    if (rc != LC_OK) {
      lc_free_with_allocator(&pouch->allocator, etag);
      lc_free_with_allocator(&pouch->allocator, descriptor);
      lc_free_with_allocator(&pouch->allocator, materialized_stored);
      lc_free_with_allocator(&pouch->allocator, payload_context);
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return rc;
    }
    materialized_transform_ready = 1;
  }
  rc = lc_pouch_state_append_lock_enter_after_mutation(pouch, namespace_name,
                                                       &append_lock, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, etag);
    lc_free_with_allocator(&pouch->allocator, descriptor);
    lc_free_with_allocator(&pouch->allocator, materialized_stored);
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  /* Waiting behind another exclusive append may have advanced or rotated the
   * resident writer. Refresh the borrowed manifest after the append gate is
   * held, while the exact-key precondition remains protected by its key lock.
   */
  if (single_writer) {
    cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 0, NULL);
    if (cache != NULL && cache->initialized &&
        cache->writer_mode_epoch == writer_mode_epoch &&
        cache->namespace_path != NULL && cache->active_segment_leaf != NULL) {
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      memset(&manifest, 0, sizeof(manifest));
      rc = lc_pouch_state_cache_manifest_borrow(pouch, cache, &manifest, error);
      manifest_from_cache = rc == LC_OK ? 1 : 0;
    } else {
      manifest_from_cache = 0;
    }
  }
  if (rc == LC_OK && !manifest_from_cache) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    memset(&manifest, 0, sizeof(manifest));
    rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                          namespace_name, &manifest, NULL, NULL,
                                          error);
  }
  if (rc == LC_OK && !manifest_from_cache) {
    segment_path =
        lc_pouch_state_child_path(&pouch->allocator, manifest.namespace_path,
                                  "segments", manifest.active_segment);
    if (segment_path == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state segment path", NULL,
                        NULL, "pouch");
    }
  }
  if (rc == LC_OK && !single_writer) {
    cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 1, error);
    if (cache == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    } else {
      /* Append authority makes the observed tail stable while this shared
       * projection incorporates peer records before reserving its own slot. */
      rc =
          lc_pouch_state_cache_refresh_for_mode(pouch, cache, &manifest, error);
    }
  }
  if (rc == LC_OK &&
      !(single_writer && cache != NULL && cache->initialized &&
        cache->active_segment_leaf != NULL &&
        strcmp(cache->active_segment_leaf, manifest.active_segment) == 0)) {
    rc = lc_pouch_state_repair_active_tail_locked(
        pouch, namespace_name, &manifest, segment_path, manifest.active_segment,
        error);
  }
  if (rc == LC_OK) {
    if (cache != NULL && cache->initialized &&
        cache->active_segment_leaf != NULL &&
        strcmp(cache->active_segment_leaf, manifest.active_segment) == 0) {
      segment_size = cache->active_segment_offset;
    } else {
      rc = lc_pouch_state_file_size(segment_path, &segment_size, error);
    }
  }
  if (rc == LC_OK &&
      (segment_size > LC_U64_MAX - LC_POUCH_STATE_RECORD_HEADER_BYTES ||
       (uint64_t)strlen(key) >
           LC_U64_MAX - LC_POUCH_STATE_RECORD_HEADER_BYTES - segment_size)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch state record offset exceeds u64", NULL, NULL,
                      "pouch");
  }
  if (rc == LC_OK && segment_size > 0U &&
      segment_size + LC_POUCH_STATE_RECORD_HEADER_BYTES +
              (uint64_t)strlen(key) >
          pouch->segment_target_bytes) {
    lc_free_with_allocator(&pouch->allocator, segment_path);
    segment_path = NULL;
    rc = lc_pouch_state_manifest_materialize(pouch, namespace_name, &manifest,
                                             &manifest_from_cache, error);
    if (rc == LC_OK) {
      rc = lc_pouch_namespace_manifest_rotate(
          &pouch->allocator, namespace_name, &manifest,
          manifest.active_segment_id + 1U, error);
    }
    if (rc == LC_OK) {
      segment_path =
          lc_pouch_state_child_path(&pouch->allocator, manifest.namespace_path,
                                    "segments", manifest.active_segment);
      if (segment_path == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state segment path", NULL,
                          NULL, "pouch");
      }
    }
    if (rc == LC_OK && cache != NULL && cache->initialized) {
      rc = lc_pouch_state_cache_set_active_segment(pouch, cache, &manifest, 0U,
                                                   error);
    }
    segment_size = 0U;
  }
  if (rc != LC_OK) {
    lc_pouch_state_append_lock_release(&append_lock);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  fd = -1;
  if (single_writer && cache != NULL && cache->initialized &&
      cache->active_segment_leaf != NULL &&
      strcmp(cache->active_segment_leaf, manifest.active_segment) == 0) {
    rc = lc_pouch_state_cache_active_append_fd(pouch, cache, &manifest, &fd,
                                               error);
    retain_active_append_fd = rc == LC_OK ? 1 : 0;
  } else {
    fd = open(segment_path, O_RDWR | O_CREAT, 0666);
  }
  if (rc == LC_OK && fd < 0) {
    lc_free_with_allocator(&pouch->allocator, segment_path);
    lc_pouch_state_append_lock_release(&append_lock);
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch state segment", strerror(errno),
                        NULL, NULL);
  }
  if (rc == LC_OK && retain_active_append_fd) {
    rc = lc_pouch_state_fd_seek(
        fd, segment_size, "failed to seek pouch state segment append offset",
        error);
  } else if (rc == LC_OK) {
    rc = lc_pouch_state_fd_end(
        fd, &segment_size, "failed to seek pouch state segment append offset",
        error);
  }
  if (rc != LC_OK) {
    if (!retain_active_append_fd) {
      (void)close(fd);
    }
    lc_free_with_allocator(&pouch->allocator, segment_path);
    lc_pouch_state_append_lock_release(&append_lock);
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  rc = lc_pouch_state_payload_span_set(&pouch->allocator, &payload_span,
                                       manifest.active_segment, segment_size,
                                       0UL, 0UL, 0UL, error);
  if (rc != LC_OK) {
    lc_pouch_state_append_fd_rollback(fd, segment_size,
                                      retain_active_append_fd);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    lc_pouch_state_append_lock_release(&append_lock);
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  rc = lc_pouch_state_encode_payload_meta(
      &pouch->allocator, version, updated_at_unix, 0UL, 0UL, content_type,
      placeholder_etag, NULL, &payload_span, payload_context, metadata,
      metadata_length, LC_POUCH_STATE_RECORD_DESCRIPTOR_RESERVE,
      has_query_hidden, query_hidden, &meta, &meta_len, error);
  if (rc != LC_OK) {
    lc_pouch_state_append_fd_rollback(fd, segment_size,
                                      retain_active_append_fd);
    lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    lc_pouch_state_append_lock_release(&append_lock);
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  if (segment_size > LC_U64_MAX - LC_POUCH_STATE_RECORD_HEADER_BYTES ||
      (uint64_t)strlen(key) >
          LC_U64_MAX - LC_POUCH_STATE_RECORD_HEADER_BYTES - segment_size ||
      (uint64_t)meta_len > LC_U64_MAX - LC_POUCH_STATE_RECORD_HEADER_BYTES -
                               segment_size - (uint64_t)strlen(key)) {
    lc_pouch_state_append_fd_rollback(fd, segment_size,
                                      retain_active_append_fd);
    lc_free_with_allocator(&pouch->allocator, meta);
    lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    lc_pouch_state_append_lock_release(&append_lock);
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state record offset exceeds u64", NULL, NULL,
                        "pouch");
  }
  payload_offset = segment_size + LC_POUCH_STATE_RECORD_HEADER_BYTES +
                   (uint64_t)strlen(key) + (uint64_t)meta_len;
  /* A bounded in-memory source is already materialized by its caller. Both
   * writer modes hold append authority here, so they can publish one complete
   * record without the pending streaming prefix/finalization protocol. Other
   * sources retain the real streaming path below. */
  if (use_materialized_record) {
    if (!materialized_transform_ready) {
      bytes = (uint64_t)materialized_plain_len;
      cipher_bytes = 0UL;
      stored_crc = (unsigned long)crc32(0L, Z_NULL, 0);
      rc = lc_pouch_crypto_transform_memory(
          pouch->crypto, payload_context, materialized_plain,
          materialized_plain_len,
          options == NULL || !options->disable_compression,
          &materialized_stored, &materialized_stored_len, &stored_crc,
          &descriptor, error);
      if (rc == LC_OK) {
        cipher_bytes = (uint64_t)materialized_stored_len;
        etag = lc_pouch_state_hash_bytes(&pouch->allocator, materialized_plain,
                                         materialized_plain_len, error);
        if (etag == NULL) {
          rc = error != NULL && error->code != LC_OK ? error->code
                                                     : LC_ERR_NOMEM;
        }
      }
    }
    updated_at_unix = lc_pouch_maintenance_now_seconds();
    if (rc == LC_OK) {
      lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
      rc = lc_pouch_state_payload_span_set(
          &pouch->allocator, &payload_span, manifest.active_segment,
          segment_size, payload_offset, cipher_bytes, stored_crc, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_encode_payload_meta(
          &pouch->allocator, version, updated_at_unix, bytes, cipher_bytes,
          content_type, etag, descriptor, &payload_span, payload_context,
          metadata, metadata_length, LC_POUCH_STATE_RECORD_DESCRIPTOR_RESERVE,
          has_query_hidden, query_hidden, &final_meta, &final_meta_len, error);
      if (rc == LC_OK && final_meta_len != meta_len) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch state metadata reservation changed size", NULL,
                          NULL, "pouch");
      }
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_reserve_index_records(
          pouch, namespace_name, &manifest, 1UL, &index_seq, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_meta_set_index_seq(final_meta, final_meta_len,
                                             index_seq, error);
    }
    if (rc == LC_OK && cipher_bytes > LC_U64_MAX - payload_offset) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state record offset exceeds u64", NULL, NULL,
                        "pouch");
    }
    if (rc == LC_OK) {
      record_end = payload_offset + cipher_bytes;
      rc = lc_pouch_state_record_write_complete(
          fd, put_record_type, key, strlen(key), final_meta, final_meta_len,
          materialized_stored, materialized_stored_len, stored_crc, error);
    }
    if (rc == LC_OK) {
      lc_source_memory_consume(body);
      payload_offset = record_end;
      rc = lc_pouch_state_defer_fsync(pouch, fd, error);
    }
  } else {
    rc = lc_pouch_state_record_write_prefix(
        fd, put_record_type, key, strlen(key), meta, meta_len, 0U, 0UL,
        LC_POUCH_RECORD_FLAG_PENDING, error);
    if (rc != LC_OK) {
      lc_pouch_state_append_fd_rollback(fd, segment_size,
                                        retain_active_append_fd);
      lc_free_with_allocator(&pouch->allocator, meta);
      lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
      lc_free_with_allocator(&pouch->allocator, payload_context);
      lc_free_with_allocator(&pouch->allocator, segment_path);
      lc_pouch_state_append_lock_release(&append_lock);
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return rc;
    }

    bytes = 0UL;
    cipher_bytes = 0UL;
    stored_crc = (unsigned long)crc32(0L, Z_NULL, 0);
    descriptor = NULL;
    hash_ctx = EVP_MD_CTX_new();
    if (hash_ctx == NULL) {
      lc_pouch_state_append_fd_rollback(fd, segment_size,
                                        retain_active_append_fd);
      lc_free_with_allocator(&pouch->allocator, meta);
      lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
      lc_free_with_allocator(&pouch->allocator, payload_context);
      lc_free_with_allocator(&pouch->allocator, segment_path);
      lc_pouch_state_append_lock_release(&append_lock);
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state payload hash context",
                          NULL, NULL, "pouch");
    }
    rc = lc_pouch_state_hash_source_init(&hash_source, body, hash_ctx, error);
    if (rc != LC_OK) {
      EVP_MD_CTX_free(hash_ctx);
      lc_pouch_state_append_fd_rollback(fd, segment_size,
                                        retain_active_append_fd);
      lc_free_with_allocator(&pouch->allocator, meta);
      lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
      lc_free_with_allocator(&pouch->allocator, payload_context);
      lc_free_with_allocator(&pouch->allocator, segment_path);
      lc_pouch_state_append_lock_release(&append_lock);
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return rc;
    }
    crypto_context =
        lc_strdup_with_allocator(&pouch->allocator, payload_context);
    if (crypto_context == NULL) {
      EVP_MD_CTX_free(hash_ctx);
      lc_pouch_state_append_fd_rollback(fd, segment_size,
                                        retain_active_append_fd);
      lc_free_with_allocator(&pouch->allocator, meta);
      lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
      lc_free_with_allocator(&pouch->allocator, payload_context);
      lc_free_with_allocator(&pouch->allocator, segment_path);
      lc_pouch_state_append_lock_release(&append_lock);
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state crypto context", NULL,
                          NULL, NULL);
    }
    /* The append gate owns this byte range. Let independent keys capture
     * metadata and prepare values while this source flows directly into the
     * segment; publication reacquires the projection mutex below. */
    if (!namespace_locked) {
      lc_pouch_state_projection_mutex_unlock(pouch, namespace_name);
      streaming_projection_mutex_released = 1;
    }
    rc = lc_pouch_crypto_stream_to_fd_crc_with_compression(
        pouch->crypto, crypto_context, fd, &hash_source.pub, &bytes,
        &cipher_bytes, &stored_crc, &descriptor,
        options == NULL || !options->disable_compression, error);
    lc_free_with_allocator(&pouch->allocator, crypto_context);
    if (rc == LC_OK && hash_source.failed) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to hash pouch state payload", NULL, NULL,
                        "pouch");
    }
    if (rc == LC_OK) {
      etag = lc_pouch_state_hash_final(&pouch->allocator, hash_ctx, error);
      if (etag == NULL) {
        rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
      }
    }
    EVP_MD_CTX_free(hash_ctx);
    if (streaming_projection_mutex_released) {
      projection_rc =
          lc_pouch_state_projection_mutex_lock(pouch, namespace_name, error);
      streaming_projection_mutex_released = 0;
      if (projection_rc != LC_OK && rc == LC_OK) {
        rc = projection_rc;
      }
    }
    updated_at_unix = lc_pouch_maintenance_now_seconds();
    if (rc == LC_OK) {
      lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
      rc = lc_pouch_state_payload_span_set(
          &pouch->allocator, &payload_span, manifest.active_segment,
          segment_size, payload_offset, cipher_bytes, stored_crc, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_encode_payload_meta(
          &pouch->allocator, version, updated_at_unix, bytes, cipher_bytes,
          content_type, etag, descriptor, &payload_span, payload_context,
          metadata, metadata_length, LC_POUCH_STATE_RECORD_DESCRIPTOR_RESERVE,
          has_query_hidden, query_hidden, &final_meta, &final_meta_len, error);
      if (rc == LC_OK && final_meta_len != meta_len) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch state metadata reservation changed size", NULL,
                          NULL, "pouch");
      }
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_reserve_index_records(
          pouch, namespace_name, &manifest, 1UL, &index_seq, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_meta_set_index_seq(final_meta, final_meta_len,
                                             index_seq, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_record_finalize(
          fd, segment_size, put_record_type, key, strlen(key), final_meta,
          final_meta_len, cipher_bytes, stored_crc, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_fd_end(
          fd, &payload_offset,
          "failed to restore pouch state segment append offset", error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_defer_fsync(pouch, fd, error);
    }
  }
  if (!retain_active_append_fd && close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state segment", strerror(errno),
                      NULL, NULL);
  }
  if (!retain_active_append_fd) {
    fd = -1;
  }
  if (rc != LC_OK) {
    if (retain_active_append_fd) {
      lc_pouch_state_append_fd_rollback(fd, segment_size, 1);
    } else {
      lc_pouch_state_truncate_path_best_effort(segment_path, segment_size);
    }
  }
  lc_pouch_state_append_lock_release(&append_lock);
  if (rc == LC_OK) {
    if (cache != NULL && cache->initialized &&
        cache->active_segment_leaf != NULL &&
        strcmp(cache->active_segment_leaf, manifest.active_segment) == 0) {
      cache->active_segment_offset = payload_offset;
    }
    (void)lc_pouch_state_cache_apply_write(
        pouch, namespace_name, &manifest, key, content_type, etag,
        &payload_span, payload_context, metadata, metadata_length, version,
        bytes, cipher_bytes, descriptor, updated_at_unix, has_query_hidden,
        query_hidden, 1, put_record_type);
    if (body->reset != NULL &&
        bytes <= LC_POUCH_STATE_BODY_CACHE_RECORD_MAX_BYTES) {
      lc_pouch_namespace_logstore *cache;
      lc_pouch_state_cache_record *record;
      lc_error cache_error;

      lc_error_init(&cache_error);
      cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 0,
                                               &cache_error);
      record =
          cache != NULL ? lc_pouch_state_cache_record_find(cache, key) : NULL;
      if (record != NULL && record->found && record->version == version &&
          record->bytes == bytes) {
        lc_pouch_state_cache_record_store_body_source(pouch, cache, record,
                                                      body, version, bytes);
      }
      lc_error_cleanup(&cache_error);
    }
  }
  if (rc == LC_OK) {
    out->etag = etag;
    out->index_seq = index_seq;
    out->version = version;
    out->bytes = bytes;
    out->cipher_bytes = cipher_bytes;
    out->descriptor = descriptor;
    if (metadata_length > 0U) {
      out->metadata = (unsigned char *)lc_alloc_with_allocator(
          &pouch->allocator, metadata_length);
      if (out->metadata == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state result metadata",
                          NULL, NULL, NULL);
      } else {
        memcpy(out->metadata, metadata, metadata_length);
        out->metadata_length = metadata_length;
      }
    }
    out->updated_at_unix = updated_at_unix;
    out->has_query_hidden = has_query_hidden;
    out->query_hidden = query_hidden;
    etag = NULL;
    descriptor = NULL;
  }
  lc_free_with_allocator(&pouch->allocator, etag);
  lc_free_with_allocator(&pouch->allocator, descriptor);
  lc_free_with_allocator(&pouch->allocator, materialized_stored);
  lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
  lc_free_with_allocator(&pouch->allocator, payload_context);
  lc_free_with_allocator(&pouch->allocator, meta);
  lc_free_with_allocator(&pouch->allocator, final_meta);
  lc_free_with_allocator(&pouch->allocator, segment_path);
  lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

int lc_pouch_state_write(lc_pouch *pouch, const char *namespace_name,
                         const char *key, lc_source *body,
                         const lc_pouch_state_write_options *options,
                         lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_state_commit_group *commit_group;
  lc_pouch_state_key_lock lock;
  int owns_commit_group;
  int rc;

  commit_group = NULL;
  owns_commit_group = 0;
  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0') {
    return lc_pouch_state_write_locked(pouch, namespace_name, key, body,
                                       options, out, error);
  }
  rc = lc_pouch_state_key_mutation_begin(pouch, namespace_name, key, &lock,
                                         error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                         &owns_commit_group, error);
  if (rc != LC_OK) {
    lc_pouch_state_key_mutation_end(pouch, &lock);
    return rc;
  }
  rc = lc_pouch_state_write_locked(pouch, namespace_name, key, body, options,
                                   out, error);
  rc = lc_pouch_state_finish_commit_group_after_mutation(
      pouch, &lock, commit_group, owns_commit_group, rc, error);
  if (rc == LC_OK) {
    lc_pouch_janitor_note_mutation(pouch);
  }
  if (rc == LC_OK) {
    pslog_field fields[7];

    fields[0] = lc_log_str_field("ns", namespace_name);
    fields[1] = lc_log_str_field("key", key);
    fields[2] =
        lc_log_u64_field("generation", out != NULL ? out->version : 0UL);
    fields[3] =
        lc_log_u64_field("payload_bytes", out != NULL ? out->bytes : 0UL);
    fields[4] =
        lc_log_u64_field("stored_bytes", out != NULL ? out->cipher_bytes : 0UL);
    fields[5] = lc_log_str_field("content_type",
                                 options != NULL ? options->content_type
                                                 : "application/octet-stream");
    fields[6] = lc_log_bool_field("query_hidden",
                                  options != NULL && options->has_query_hidden
                                      ? options->query_hidden
                                      : 0);
    lc_log_trace(pouch->logger, "logstore.write", fields, 7U);
    lc_pouch_query_index_note_state_write(pouch, namespace_name, key,
                                          options != NULL &&
                                                  options->content_type != NULL
                                              ? options->content_type
                                              : "application/octet-stream",
                                          body, out);
  } else {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("ns", namespace_name);
    fields[1] = lc_log_str_field("key", key);
    fields[2] = lc_log_error_field("error", error);
    fields[3] = lc_log_code_field(error);
    lc_log_error(pouch->logger, "logstore.write.error", fields, 4U);
  }
  return rc;
}

void lc_pouch_state_write_result_cleanup(const lc_allocator *allocator,
                                         lc_pouch_state_write_result *result) {
  if (result == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, result->etag);
  lc_free_with_allocator(allocator, result->descriptor);
  lc_free_with_allocator(allocator, result->metadata);
  memset(result, 0, sizeof(*result));
}

static int lc_pouch_state_metadata_write_result_build(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    const lc_pouch_state_entry *current,
    const lc_pouch_state_write_options *options, lc_pouch_generation version,
    lc_pouch_unix_seconds updated_at_unix, int has_query_hidden,
    int query_hidden, lc_pouch_state_write_result *out, lc_error *error) {
  const unsigned char *metadata;
  size_t metadata_length;

  out->etag = lc_strdup_with_allocator(
      &pouch->allocator, current->etag != NULL ? current->etag : "");
  if (out->etag == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch metadata state etag", NULL,
                        NULL, NULL);
  }
  out->index_seq = manifest->state_max_version;
  out->version = version;
  out->bytes = current->bytes;
  out->cipher_bytes = current->cipher_bytes;
  if (current->descriptor != NULL) {
    out->descriptor =
        lc_strdup_with_allocator(&pouch->allocator, current->descriptor);
    if (out->descriptor == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch metadata descriptor", NULL,
                          NULL, NULL);
    }
  }
  metadata = options->has_metadata ? options->metadata : current->metadata;
  metadata_length = options->has_metadata ? options->metadata_length
                                          : current->metadata_length;
  if (metadata_length > 0U) {
    out->metadata = (unsigned char *)lc_alloc_with_allocator(&pouch->allocator,
                                                             metadata_length);
    if (out->metadata == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch metadata result blob", NULL,
                          NULL, NULL);
    }
    memcpy(out->metadata, metadata, metadata_length);
    out->metadata_length = metadata_length;
  }
  out->updated_at_unix = updated_at_unix;
  out->has_query_hidden = has_query_hidden;
  out->query_hidden = query_hidden;
  return LC_OK;
}

static void lc_pouch_state_metadata_append_complete(
    lc_pouch_state_metadata_append_batcher *batcher,
    lc_pouch_state_metadata_append_request **requests, size_t count) {
  size_t index;

  if (batcher == NULL || requests == NULL) {
    return;
  }
  pthread_mutex_lock(&batcher->mutex);
  for (index = 0U; index < count; ++index) {
    if (requests[index] != NULL) {
      requests[index]->done = 1;
      pthread_cond_signal(&requests[index]->cond);
    }
  }
  pthread_mutex_unlock(&batcher->mutex);
}

static void lc_pouch_state_metadata_append_fail(
    lc_pouch_state_metadata_append_request *request, int rc,
    const lc_error *cause) {
  const char *message;

  if (request == NULL) {
    return;
  }
  message = cause != NULL && cause->message != NULL
                ? cause->message
                : "pouch metadata append batch failed";
  request->rc =
      lc_error_set(request->error, rc, 0L, message, NULL, NULL, "pouch");
}

static void lc_pouch_state_metadata_append_process(
    lc_pouch *pouch, lc_pouch_state_metadata_append_request **requests,
    size_t count, int projection_held) {
  lc_pouch_state_binary_append_item
      items[LC_POUCH_STATE_METADATA_APPEND_BATCH_MAX];
  lc_pouch_namespace_manifest manifest;
  lc_pouch_state_append_lock append_lock;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_process_namespace_mutex *projection_mutex;
  size_t index;
  int cache_cleared;
  int projection_locked;
  int rc;
  int single_writer;
  lc_error batch_error;

  if (pouch == NULL || requests == NULL || count == 0U) {
    return;
  }
  memset(items, 0, sizeof(items));
  memset(&manifest, 0, sizeof(manifest));
  append_lock.fd = -1;
  append_lock.process_mutex = NULL;
  append_lock.exclusive_gate = NULL;
  lc_error_init(&batch_error);
  cache_cleared = 0;
  cache = NULL;
  projection_mutex = NULL;
  projection_locked = 0;
  single_writer = lc_pouch_single_writer_enabled(pouch);
  rc = LC_OK;
  if (!projection_held && single_writer) {
    rc = lc_pouch_state_append_lock_acquire(pouch, requests[0]->namespace_name,
                                            &append_lock, &batch_error);
  }
  if (!projection_held && rc == LC_OK && single_writer) {
    rc = lc_pouch_state_process_namespace_mutex_lock(
        pouch, requests[0]->namespace_name, &projection_mutex, &batch_error);
    if (rc == LC_OK) {
      projection_locked = 1;
    }
  } else if (!projection_held && !single_writer) {
    rc = pthread_mutex_lock(&pouch->state_mutation_mutex);
    if (rc != 0) {
      rc = LC_ERR_TRANSPORT;
    }
  }
  if (rc != LC_OK) {
    for (index = 0U; index < count; ++index) {
      lc_pouch_state_metadata_append_fail(requests[index], LC_ERR_TRANSPORT,
                                          &batch_error);
    }
    lc_pouch_state_append_lock_release(&append_lock);
    goto cleanup;
  }
  rc = lc_pouch_state_manifest_view(pouch, requests[0]->namespace_name,
                                    &manifest, &cache, &batch_error);
  if (rc == LC_OK) {
    for (index = 0U; index < count; ++index) {
      lc_pouch_state_metadata_append_request *request;
      const unsigned char *metadata;
      size_t metadata_length;

      request = requests[index];
      request->updated_at_unix = lc_pouch_maintenance_now_seconds();
      metadata = request->options.has_metadata ? request->options.metadata
                                               : request->current->metadata;
      metadata_length = request->options.has_metadata
                            ? request->options.metadata_length
                            : request->current->metadata_length;
      rc = lc_pouch_state_encode_payload_meta(
          &pouch->allocator, request->version, request->updated_at_unix,
          request->current->bytes, request->current->cipher_bytes,
          request->current->content_type != NULL
              ? request->current->content_type
              : "application/octet-stream",
          request->current->etag != NULL ? request->current->etag : "",
          request->current->descriptor, &request->current->payload_span,
          request->current->payload_context, metadata, metadata_length, 0U,
          request->has_query_hidden, request->query_hidden, &items[index].meta,
          &items[index].meta_len, &batch_error);
      if (rc != LC_OK) {
        break;
      }
      items[index].record_type = LC_POUCH_STATE_RECORD_STATE_META;
      items[index].key = request->key;
      items[index].key_len = strlen(request->key);
    }
  }
  if (rc == LC_OK) {
    if (single_writer) {
      rc = lc_pouch_state_append_binary_records_locked(
          pouch, requests[0]->namespace_name, &manifest, items, count,
          &batch_error);
    } else {
      rc = lc_pouch_state_append_binary_records(
          pouch, requests[0]->namespace_name, &manifest, items, count,
          &batch_error);
    }
  }
  if (rc == LC_OK) {
    for (index = 0U; index < count; ++index) {
      lc_pouch_state_metadata_append_request *request;
      lc_pouch_namespace_manifest result_manifest;
      lc_pouch_generation index_seq;
      int result_rc;

      request = requests[index];
      index_seq = 0UL;
      result_rc = lc_pouch_state_meta_index_seq(
          items[index].meta, items[index].meta_len, &index_seq, request->error);
      if (result_rc == LC_OK) {
        result_manifest = manifest;
        result_manifest.state_max_version = index_seq;
        result_rc = lc_pouch_state_metadata_write_result_build(
            pouch, &result_manifest, request->current, &request->options,
            request->version, request->updated_at_unix,
            request->has_query_hidden, request->query_hidden, request->out,
            request->error);
      }
      if (result_rc == LC_OK) {
        const unsigned char *metadata;
        size_t metadata_length;

        metadata = request->options.has_metadata ? request->options.metadata
                                                 : request->current->metadata;
        metadata_length = request->options.has_metadata
                              ? request->options.metadata_length
                              : request->current->metadata_length;
        result_manifest = manifest;
        result_manifest.state_max_version = index_seq;
        (void)lc_pouch_state_cache_apply_write(
            pouch, request->namespace_name, &result_manifest, request->key,
            request->current->content_type != NULL
                ? request->current->content_type
                : "application/octet-stream",
            request->current->etag != NULL ? request->current->etag : "",
            &request->current->payload_span, request->current->payload_context,
            metadata, metadata_length, request->version,
            request->current->bytes, request->current->cipher_bytes,
            request->current->descriptor, request->out->updated_at_unix,
            request->has_query_hidden, request->query_hidden, 1,
            LC_POUCH_STATE_RECORD_STATE_META);
        request->rc = LC_OK;
      } else {
        lc_pouch_state_write_result_cleanup(&pouch->allocator, request->out);
        if (!cache_cleared) {
          lc_pouch_state_cache_invalidate_namespace(pouch,
                                                    request->namespace_name);
          cache_cleared = 1;
        }
        request->rc = result_rc;
      }
    }
  } else {
    for (index = 0U; index < count; ++index) {
      lc_pouch_state_metadata_append_fail(requests[index], rc, &batch_error);
    }
  }
  if (projection_locked) {
    lc_pouch_state_process_namespace_mutex_unlock(&projection_mutex);
  } else if (!single_writer && !projection_held) {
    (void)pthread_mutex_unlock(&pouch->state_mutation_mutex);
  }
  lc_pouch_state_append_lock_release(&append_lock);

cleanup:
  for (index = 0U; index < count; ++index) {
    lc_free_with_allocator(&pouch->allocator, items[index].meta);
  }
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  lc_error_cleanup(&batch_error);
}

static void *lc_pouch_state_metadata_append_worker(void *context) {
  lc_pouch_state_metadata_append_batcher *batcher;
  lc_pouch_state_metadata_append_request
      *requests[LC_POUCH_STATE_METADATA_APPEND_BATCH_MAX];
  size_t count;

  batcher = (lc_pouch_state_metadata_append_batcher *)context;
  pthread_mutex_lock(&batcher->mutex);
  for (;;) {
    while (batcher->head == NULL && !batcher->stop) {
      pthread_cond_wait(&batcher->cond, &batcher->mutex);
    }
    if (batcher->head == NULL && batcher->stop) {
      pthread_mutex_unlock(&batcher->mutex);
      return NULL;
    }
    count = 0U;
    while (batcher->head != NULL &&
           count < LC_POUCH_STATE_METADATA_APPEND_BATCH_MAX) {
      requests[count] = batcher->head;
      batcher->head = batcher->head->next;
      requests[count]->next = NULL;
      ++count;
    }
    if (batcher->head == NULL) {
      batcher->tail = NULL;
    }
    pthread_mutex_unlock(&batcher->mutex);
#ifdef LOCKDC_TEST_BUILD
    if (lc_pouch_test_metadata_append_hook != NULL) {
      lc_pouch_test_metadata_append_hook(lc_pouch_test_metadata_append_context,
                                         batcher->namespace_name);
    }
#endif
    lc_pouch_state_metadata_append_process(batcher->pouch, requests, count, 0);
    lc_pouch_state_metadata_append_complete(batcher, requests, count);
    pthread_mutex_lock(&batcher->mutex);
  }
}

static void lc_pouch_state_metadata_append_batcher_stop(
    lc_pouch_state_metadata_append_batcher *batcher) {
  if (batcher == NULL) {
    return;
  }
  pthread_mutex_lock(&batcher->mutex);
  batcher->stop = 1;
  pthread_cond_broadcast(&batcher->cond);
  pthread_mutex_unlock(&batcher->mutex);
}

static void lc_pouch_state_metadata_append_batcher_destroy(
    lc_pouch_state_metadata_append_batcher *batcher) {
  if (batcher == NULL) {
    return;
  }
  lc_pouch_state_metadata_append_batcher_stop(batcher);
  (void)pthread_join(batcher->thread, NULL);
  (void)pthread_cond_destroy(&batcher->cond);
  (void)pthread_mutex_destroy(&batcher->mutex);
  lc_free_with_allocator(&batcher->pouch->allocator, batcher->namespace_name);
  lc_free_with_allocator(&batcher->pouch->allocator, batcher);
}

static int lc_pouch_state_metadata_append_batcher_get(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_state_metadata_append_batcher **out, lc_error *error) {
  lc_pouch_state_metadata_append_batcher *batcher;
  lc_pouch_namespace_logstore *cache;
  int cond_initialized;
  int mutex_initialized;
  int pthread_rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      out == NULL || !pouch->state_cache_mutex_initialized) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata append queue requires namespace", NULL,
                        NULL, "pouch");
  }
  *out = NULL;
  cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 1, error);
  if (cache == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  pthread_rc = pthread_mutex_lock(&pouch->state_cache_mutex);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch namespace owner registry",
                        strerror(pthread_rc), NULL, "pouch");
  }
  batcher = cache->metadata_append_batcher;
  if (batcher != NULL) {
    (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
    *out = batcher;
    return LC_OK;
  }
  batcher = (lc_pouch_state_metadata_append_batcher *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*batcher));
  if (batcher == NULL) {
    (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch metadata append queue", NULL,
                        NULL, "pouch");
  }
  batcher->pouch = pouch;
  batcher->namespace_name =
      lc_strdup_with_allocator(&pouch->allocator, namespace_name);
  if (batcher->namespace_name == NULL) {
    lc_free_with_allocator(&pouch->allocator, batcher);
    (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch metadata append namespace",
                        NULL, NULL, "pouch");
  }
  mutex_initialized = 0;
  cond_initialized = 0;
  pthread_rc = pthread_mutex_init(&batcher->mutex, NULL);
  if (pthread_rc == 0) {
    mutex_initialized = 1;
    pthread_rc = pthread_cond_init(&batcher->cond, NULL);
    if (pthread_rc == 0) {
      cond_initialized = 1;
      pthread_rc =
          pthread_create(&batcher->thread, NULL,
                         lc_pouch_state_metadata_append_worker, batcher);
    }
  }
  if (pthread_rc != 0) {
    if (cond_initialized) {
      (void)pthread_cond_destroy(&batcher->cond);
    }
    if (mutex_initialized) {
      (void)pthread_mutex_destroy(&batcher->mutex);
    }
    lc_free_with_allocator(&pouch->allocator, batcher->namespace_name);
    lc_free_with_allocator(&pouch->allocator, batcher);
    (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to start pouch metadata append queue",
                        strerror(pthread_rc), NULL, "pouch");
  }
  cache->metadata_append_batcher = batcher;
  (void)pthread_mutex_unlock(&pouch->state_cache_mutex);
  *out = batcher;
  return LC_OK;
}

int lc_pouch_state_metadata_append_worker_init(lc_pouch *pouch,
                                               lc_error *error) {
  if (pouch == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata append worker requires pouch", NULL,
                        NULL, "pouch");
  }
  return LC_OK;
}

void lc_pouch_state_metadata_append_worker_close(lc_pouch *pouch) {
  lc_pouch_state_metadata_append_batcher *batcher;
  lc_pouch_state_metadata_append_batcher *next;
  lc_pouch_namespace_logstore *cache;

  if (pouch == NULL || !pouch->state_cache_mutex_initialized) {
    return;
  }
  batcher = NULL;
  pthread_mutex_lock(&pouch->state_cache_mutex);
  for (cache = pouch->namespace_logstores; cache != NULL; cache = cache->next) {
    if (cache->metadata_append_batcher != NULL) {
      cache->metadata_append_batcher->next = batcher;
      batcher = cache->metadata_append_batcher;
      cache->metadata_append_batcher = NULL;
    }
  }
  pthread_mutex_unlock(&pouch->state_cache_mutex);
  for (next = batcher; next != NULL; next = next->next) {
    lc_pouch_state_metadata_append_batcher_stop(next);
  }
  while (batcher != NULL) {
    next = batcher->next;
    lc_pouch_state_metadata_append_batcher_destroy(batcher);
    batcher = next;
  }
}

static int lc_pouch_state_metadata_append_submit_locked(
    lc_pouch *pouch, lc_pouch_state_metadata_append_request *request,
    lc_error *error) {
  lc_pouch_state_metadata_append_batcher *batcher;
  lc_pouch_state_metadata_append_request *inline_request[1];
  int pthread_rc;
  int rc;

  if (pouch == NULL || request == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata append worker is unavailable", NULL,
                        NULL, "pouch");
  }
  pthread_rc = pthread_cond_init(&request->cond, NULL);
  if (pthread_rc != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to initialize pouch metadata append request",
                        strerror(pthread_rc), NULL, "pouch");
  }
  request->next = NULL;
  request->done = 0;
  request->rc = LC_OK;
  /* Namespace callbacks retain a stronger atomic authority. Their metadata
   * records must stay on this thread so a worker cannot wait on that authority
   * while the callback waits for its completion. */
  if (lc_pouch_state_namespace_lock_is_held(pouch, request->namespace_name)) {
    inline_request[0] = request;
    lc_pouch_state_metadata_append_process(pouch, inline_request, 1U, 1);
    pthread_cond_destroy(&request->cond);
    return request->rc;
  }
  rc = lc_pouch_state_metadata_append_batcher_get(
      pouch, request->namespace_name, &batcher, error);
  if (rc != LC_OK) {
    pthread_cond_destroy(&request->cond);
    return rc;
  }
  /* The caller still owns its exact-key lock. Release projection ownership
   * while the bounded inline batch is appended and published by the worker. */
  lc_pouch_state_projection_mutex_unlock(pouch, request->namespace_name);
  pthread_mutex_lock(&batcher->mutex);
  if (batcher->tail != NULL) {
    batcher->tail->next = request;
  } else {
    batcher->head = request;
  }
  batcher->tail = request;
  pthread_cond_signal(&batcher->cond);
  while (!request->done) {
    pthread_cond_wait(&request->cond, &batcher->mutex);
  }
  pthread_mutex_unlock(&batcher->mutex);
  rc = lc_pouch_state_projection_mutex_lock(pouch, request->namespace_name,
                                            error);
  pthread_cond_destroy(&request->cond);
  if (rc != LC_OK) {
    return rc;
  }
  return request->rc;
}

static int lc_pouch_state_update_metadata_from_current_locked(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    lc_pouch_namespace_manifest *manifest, lc_pouch_state_entry *current,
    const lc_pouch_state_write_options *options,
    lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_generation logical_version;
  lc_pouch_generation version;
  lc_pouch_unix_seconds updated_at_unix;
  int has_query_hidden;
  int query_hidden;
  int rc;
  lc_pouch_state_precondition_view precondition_view;

  if (pouch == NULL || namespace_name == NULL || key == NULL ||
      manifest == NULL || current == NULL || options == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata update current state is invalid", NULL,
                        NULL, "pouch");
  }
  memset(out, 0, sizeof(*out));
  if (options->view_precondition != NULL) {
    lc_pouch_state_precondition_view_from_entry(current, &precondition_view);
    rc = options->view_precondition(&precondition_view,
                                    options->view_precondition_context, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  if (!current->found && !options->has_metadata) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata update requires existing state", NULL,
                        NULL, NULL);
  }
  logical_version =
      current->found && current->payload_span.present ? current->version : 0UL;
  if (options->has_expected_version &&
      logical_version != options->expected_version) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata version precondition failed", NULL,
                        NULL, NULL);
  }
  if (options->precondition != NULL) {
    rc = options->precondition(options->precondition_context, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  has_query_hidden = current->has_query_hidden;
  query_hidden = current->query_hidden;
  if (options->has_query_hidden) {
    has_query_hidden = 1;
    query_hidden = options->query_hidden;
  } else if (!current->found) {
    has_query_hidden = 1;
    query_hidden = 1;
  }
  version = logical_version;
  updated_at_unix = lc_pouch_maintenance_now_seconds();
  rc = lc_pouch_state_append_record(
      pouch, namespace_name, manifest, LC_POUCH_STATE_RECORD_STATE_META, key,
      current->content_type != NULL ? current->content_type
                                    : "application/octet-stream",
      current->etag != NULL ? current->etag : "", &current->payload_span,
      current->payload_context,
      options->has_metadata ? options->metadata : current->metadata,
      options->has_metadata ? options->metadata_length
                            : current->metadata_length,
      version, current->bytes, current->cipher_bytes, current->descriptor,
      updated_at_unix, has_query_hidden, query_hidden, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_metadata_write_result_build(
        pouch, manifest, current, options, version, updated_at_unix,
        has_query_hidden, query_hidden, out, error);
    (void)lc_pouch_state_cache_apply_write(
        pouch, namespace_name, manifest, key,
        current->content_type != NULL ? current->content_type
                                      : "application/octet-stream",
        current->etag != NULL ? current->etag : "", &current->payload_span,
        current->payload_context,
        options->has_metadata ? options->metadata : current->metadata,
        options->has_metadata ? options->metadata_length
                              : current->metadata_length,
        version, current->bytes, current->cipher_bytes, current->descriptor,
        updated_at_unix, has_query_hidden, query_hidden, 1,
        LC_POUCH_STATE_RECORD_STATE_META);
  }
  if (rc != LC_OK) {
    lc_pouch_state_write_result_cleanup(&pouch->allocator, out);
  }
  return rc;
}

/* Queue only complete metadata records. The caller keeps its exact-key lock
 * while waiting, so the copied current entry remains the CAS decision for this
 * request even though independent keys may advance the append projection. */
static int lc_pouch_state_metadata_append_schedule_from_current_locked(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    lc_pouch_state_entry *current, int current_is_borrowed,
    const lc_pouch_state_write_options *options,
    lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_state_metadata_append_request request;
  lc_pouch_state_entry scheduled_current;
  lc_pouch_generation logical_version;
  int has_query_hidden;
  int query_hidden;
  int rc;
  lc_pouch_state_precondition_view precondition_view;

  if (pouch == NULL || namespace_name == NULL || key == NULL ||
      current == NULL || options == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata append scheduling requires inputs",
                        NULL, NULL, "pouch");
  }
  memset(out, 0, sizeof(*out));
  if (options->view_precondition != NULL) {
    lc_pouch_state_precondition_view_from_entry(current, &precondition_view);
    rc = options->view_precondition(&precondition_view,
                                    options->view_precondition_context, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  if (!current->found && !options->has_metadata) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata update requires existing state", NULL,
                        NULL, NULL);
  }
  logical_version =
      current->found && current->payload_span.present ? current->version : 0UL;
  if (options->has_expected_version &&
      logical_version != options->expected_version) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata version precondition failed", NULL,
                        NULL, NULL);
  }
  if (options->precondition != NULL) {
    rc = options->precondition(options->precondition_context, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  has_query_hidden = current->has_query_hidden;
  query_hidden = current->query_hidden;
  if (options->has_query_hidden) {
    has_query_hidden = 1;
    query_hidden = options->query_hidden;
  } else if (!current->found) {
    has_query_hidden = 1;
    query_hidden = 1;
  }
  memset(&request, 0, sizeof(request));
  memset(&scheduled_current, 0, sizeof(scheduled_current));
  if (current_is_borrowed) {
    rc = lc_pouch_state_metadata_append_entry_copy(pouch, current,
                                                   &scheduled_current, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  request.namespace_name = namespace_name;
  request.key = key;
  request.current = current_is_borrowed ? &scheduled_current : current;
  request.options = *options;
  request.version = logical_version;
  request.has_query_hidden = has_query_hidden;
  request.query_hidden = query_hidden;
  request.out = out;
  request.error = error;
  rc = lc_pouch_state_metadata_append_submit_locked(pouch, &request, error);
  lc_pouch_state_entry_cleanup(&pouch->allocator, &scheduled_current);
  return rc;
}

int lc_pouch_state_update_metadata_locked(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    const lc_pouch_state_write_options *options,
    lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_state_entry current;
  lc_pouch_namespace_manifest manifest;
  lc_pouch_generation max_version;
  int use_metadata_batcher;
  int current_is_borrowed;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || options == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_update_metadata requires pouch, "
                        "namespace, key, options, and out",
                        NULL, NULL, NULL);
  }
  memset(&current, 0, sizeof(current));
  memset(&manifest, 0, sizeof(manifest));
  current_is_borrowed = 0;
  /* Submission releases the exact-key operation's projection ownership while
   * its namespace worker owns append authority. Namespace-scoped callbacks
   * already hold stronger namespace authority, so they append inline and
   * never unlock a mutex they do not own. Shared-root key mutations retain
   * their per-key file lock through the worker and are safe to coalesce with
   * this handle's other independent keys. */
  use_metadata_batcher =
      !lc_pouch_state_namespace_lock_is_held(pouch, namespace_name);
  if (use_metadata_batcher && lc_pouch_state_cache_borrow_exclusive_record(
                                  pouch, namespace_name, key, &current)) {
    current_is_borrowed = 1;
    rc = LC_OK;
  } else {
    rc = lc_pouch_state_manifest_lookup_cached_borrowed(
        pouch, namespace_name, key, &manifest, &current, &max_version,
        &current_is_borrowed, error);
  }
  if (rc == LC_OK) {
    if (use_metadata_batcher) {
      rc = lc_pouch_state_metadata_append_schedule_from_current_locked(
          pouch, namespace_name, key, &current, current_is_borrowed, options,
          out, error);
    } else {
      rc = lc_pouch_state_update_metadata_from_current_locked(
          pouch, namespace_name, key, &manifest, &current, options, out, error);
    }
  }
  if (!current_is_borrowed) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  }
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

int lc_pouch_state_update_metadata_prepared_locked(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    lc_pouch_state_metadata_prepare_fn prepare, void *prepare_context,
    lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_state_entry current;
  lc_pouch_state_metadata_view view;
  lc_pouch_namespace_manifest manifest;
  lc_pouch_state_write_options options;
  lc_pouch_generation max_version;
  int apply;
  int use_metadata_batcher;
  int current_is_borrowed;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || prepare == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch prepared metadata update requires valid inputs",
                        NULL, NULL, "pouch");
  }
  memset(out, 0, sizeof(*out));
  memset(&current, 0, sizeof(current));
  memset(&manifest, 0, sizeof(manifest));
  memset(&view, 0, sizeof(view));
  memset(&options, 0, sizeof(options));
  current_is_borrowed = 0;
  /* See lc_pouch_state_update_metadata_locked: only exact-key operations
   * transfer projection ownership to the metadata append worker. */
  use_metadata_batcher =
      !lc_pouch_state_namespace_lock_is_held(pouch, namespace_name);
  if (use_metadata_batcher && lc_pouch_state_cache_borrow_exclusive_record(
                                  pouch, namespace_name, key, &current)) {
    current_is_borrowed = 1;
    rc = LC_OK;
  } else {
    rc = lc_pouch_state_manifest_lookup_cached_borrowed(
        pouch, namespace_name, key, &manifest, &current, &max_version,
        &current_is_borrowed, error);
  }
  if (rc == LC_OK) {
    view.found = current.found;
    view.version = current.version;
    view.metadata = current.metadata;
    view.metadata_length = current.metadata_length;
    view.has_query_hidden = current.has_query_hidden;
    view.query_hidden = current.query_hidden;
    view.has_body = current.payload_span.present;
    apply = 1;
    rc = prepare(&view, prepare_context, &options, &apply, error);
    if (rc == LC_OK && apply) {
      if (use_metadata_batcher) {
        rc = lc_pouch_state_metadata_append_schedule_from_current_locked(
            pouch, namespace_name, key, &current, current_is_borrowed, &options,
            out, error);
      } else {
        rc = lc_pouch_state_update_metadata_from_current_locked(
            pouch, namespace_name, key, &manifest, &current, &options, out,
            error);
      }
    }
  }
  if (!current_is_borrowed) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  }
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

int lc_pouch_state_update_metadata(lc_pouch *pouch, const char *namespace_name,
                                   const char *key,
                                   const lc_pouch_state_write_options *options,
                                   lc_pouch_state_write_result *out,
                                   lc_error *error) {
  lc_pouch_state_commit_group *commit_group;
  lc_pouch_state_key_lock lock;
  int owns_commit_group;
  int rc;

  commit_group = NULL;
  owns_commit_group = 0;
  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0') {
    return lc_pouch_state_update_metadata_locked(pouch, namespace_name, key,
                                                 options, out, error);
  }
  rc = lc_pouch_state_key_mutation_begin(pouch, namespace_name, key, &lock,
                                         error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                         &owns_commit_group, error);
  if (rc != LC_OK) {
    lc_pouch_state_key_mutation_end(pouch, &lock);
    return rc;
  }
  rc = lc_pouch_state_update_metadata_locked(pouch, namespace_name, key,
                                             options, out, error);
  rc = lc_pouch_state_finish_commit_group_after_mutation(
      pouch, &lock, commit_group, owns_commit_group, rc, error);
  if (rc == LC_OK) {
    lc_pouch_janitor_note_mutation(pouch);
  }
  return rc;
}

static int lc_pouch_state_delete_locked(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    const lc_pouch_state_write_options *options,
    lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_state_entry current;
  lc_pouch_namespace_manifest manifest;
  char *etag;
  lc_pouch_generation max_version;
  lc_pouch_generation version;
  lc_pouch_unix_seconds updated_at_unix;
  int rc;
  lc_pouch_state_precondition_view precondition_view;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_delete requires pouch, namespace, key "
                        "and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  rc = lc_pouch_state_manifest_lookup_cached_for_mutation(
      pouch, namespace_name, key, &manifest, &current, &max_version, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (options != NULL && options->view_precondition != NULL) {
    lc_pouch_state_precondition_view_from_entry(&current, &precondition_view);
    rc = options->view_precondition(&precondition_view,
                                    options->view_precondition_context, error);
    if (rc != LC_OK) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return rc;
    }
  }
  if (options != NULL && options->expected_etag != NULL) {
    if (!current.found || strcmp(current.etag, options->expected_etag) != 0) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch state delete etag precondition failed", NULL,
                          NULL, NULL);
    }
  }
  if (options != NULL && options->has_expected_version) {
    if (!current.found || current.version != options->expected_version) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch state delete version precondition failed",
                          NULL, NULL, NULL);
    }
  }
  if (options != NULL && options->precondition != NULL) {
    rc = options->precondition(options->precondition_context, error);
    if (rc != LC_OK) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return rc;
    }
  }
  if (!current.found) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return LC_OK;
  }
  (void)max_version;
  version = current.found && current.payload_span.present
                ? current.version + 1UL
                : 1UL;
  etag = lc_pouch_state_empty_etag(&pouch->allocator, error);
  if (etag == NULL) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  updated_at_unix = lc_pouch_maintenance_now_seconds();
  rc = lc_pouch_state_append_tombstone(pouch, namespace_name, &manifest, key,
                                       etag, version, updated_at_unix,
                                       options != NULL && options->object_record
                                           ? LC_POUCH_STATE_RECORD_OBJECT_DELETE
                                           : current.record_type,
                                       error);
  if (rc == LC_OK) {
    (void)lc_pouch_state_cache_apply_write(
        pouch, namespace_name, &manifest, key, NULL, etag, NULL, NULL, NULL, 0U,
        version, 0UL, 0UL, NULL, updated_at_unix, 0, 0, 0,
        options != NULL && options->object_record
            ? LC_POUCH_STATE_RECORD_OBJECT_DELETE
            : current.record_type);
  }
  if (rc == LC_OK) {
    out->etag = etag;
    out->index_seq = manifest.state_max_version;
    out->version = version;
    out->bytes = 0UL;
    out->updated_at_unix = updated_at_unix;
    etag = NULL;
  }
  lc_free_with_allocator(&pouch->allocator, etag);
  lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

int lc_pouch_state_delete(lc_pouch *pouch, const char *namespace_name,
                          const char *key,
                          const lc_pouch_state_write_options *options,
                          lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_state_commit_group *commit_group;
  lc_pouch_state_key_lock lock;
  int owns_commit_group;
  int rc;

  commit_group = NULL;
  owns_commit_group = 0;
  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0') {
    return lc_pouch_state_delete_locked(pouch, namespace_name, key, options,
                                        out, error);
  }
  rc = lc_pouch_state_key_mutation_begin(pouch, namespace_name, key, &lock,
                                         error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                         &owns_commit_group, error);
  if (rc != LC_OK) {
    lc_pouch_state_key_mutation_end(pouch, &lock);
    return rc;
  }
  rc = lc_pouch_state_delete_locked(pouch, namespace_name, key, options, out,
                                    error);
  rc = lc_pouch_state_finish_commit_group_after_mutation(
      pouch, &lock, commit_group, owns_commit_group, rc, error);
  if (rc == LC_OK) {
    lc_pouch_janitor_note_mutation(pouch);
  }
  if (rc == LC_OK) {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("ns", namespace_name);
    fields[1] = lc_log_str_field("key", key);
    fields[2] =
        lc_log_u64_field("generation", out != NULL ? out->version : 0UL);
    lc_log_trace(pouch->logger, "logstore.delete", fields, 3U);
  } else {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("ns", namespace_name);
    fields[1] = lc_log_str_field("key", key);
    fields[2] = lc_log_error_field("error", error);
    fields[3] = lc_log_code_field(error);
    lc_log_error(pouch->logger, "logstore.delete.error", fields, 4U);
  }
  if (rc == LC_OK && out != NULL && out->version > 0UL) {
    lc_pouch_query_index_note_state_delete(pouch, namespace_name, key, out);
  }
  return rc;
}

int lc_pouch_state_stage_write(lc_pouch *pouch, const char *namespace_name,
                               const char *key, const char *txn_id,
                               lc_source *body,
                               const lc_pouch_state_write_options *options,
                               lc_pouch_state_write_result *out,
                               lc_error *error) {
  char *staged_key;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || txn_id == NULL || txn_id[0] == '\0' ||
      body == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_stage_write requires pouch, "
                        "namespace, key, txn_id, body and out",
                        NULL, NULL, NULL);
  }
  staged_key = lc_pouch_state_staged_key(&pouch->allocator, key, txn_id);
  if (staged_key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch staged state key", NULL, NULL,
                        NULL);
  }
  rc = lc_pouch_state_write(pouch, namespace_name, staged_key, body, options,
                            out, error);
  lc_free_with_allocator(&pouch->allocator, staged_key);
  return rc;
}

static int lc_pouch_state_promote_staged_locked(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    const char *txn_id, const char *expected_committed_etag,
    lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_state_entry committed;
  lc_pouch_state_entry staged;
  lc_pouch_namespace_manifest manifest;
  char *staged_key;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_generation version;
  lc_pouch_generation decision_version;
  lc_pouch_generation discard_version;
  const unsigned char *promoted_metadata;
  size_t promoted_metadata_length;
  lc_pouch_unix_seconds updated_at_unix;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || txn_id == NULL || txn_id[0] == '\0' ||
      out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_promote_staged requires pouch, "
                        "namespace, key, txn_id and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  staged_key = lc_pouch_state_staged_key(&pouch->allocator, key, txn_id);
  if (staged_key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch staged state key", NULL, NULL,
                        NULL);
  }
  memset(&manifest, 0, sizeof(manifest));
  memset(&committed, 0, sizeof(committed));
  memset(&staged, 0, sizeof(staged));
  rc = lc_pouch_state_manifest_lookup_cached_for_mutation(
      pouch, namespace_name, key, &manifest, &committed, NULL, error);
  if (rc == LC_OK) {
    cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 0, NULL);
    if (cache != NULL && cache->initialized) {
      rc = lc_pouch_state_entry_from_cache_record(
          pouch, lc_pouch_state_cache_record_find(cache, staged_key), &staged,
          error);
    } else {
      rc = lc_pouch_state_scan(pouch, &manifest, staged_key, &staged, NULL,
                               error);
    }
  }
  if (rc != LC_OK) {
    goto cleanup;
  }
  if (expected_committed_etag != NULL) {
    if (!committed.found ||
        strcmp(committed.etag, expected_committed_etag) != 0) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged promotion committed etag precondition "
                        "failed",
                        NULL, NULL, NULL);
      goto cleanup;
    }
  } else if (committed.found && committed.payload_span.present) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch staged promotion committed state already exists",
                      NULL, NULL, NULL);
    goto cleanup;
  }
  if (!staged.found) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch staged promotion source is missing", NULL, NULL,
                      NULL);
    goto cleanup;
  }
  promoted_metadata = staged.metadata;
  promoted_metadata_length = staged.metadata_length;
  if (promoted_metadata_length == 0U && committed.metadata_length > 0U) {
    promoted_metadata = committed.metadata;
    promoted_metadata_length = committed.metadata_length;
  }
  version = committed.found && committed.payload_span.present
                ? committed.version + 1UL
                : 1UL;
  updated_at_unix = lc_pouch_maintenance_now_seconds();
  decision_version = staged.version + 1UL;
  discard_version = decision_version + 1UL;
  rc = lc_pouch_state_append_staged_commit_batch(
      pouch, namespace_name, &manifest, key, staged_key, &staged,
      promoted_metadata, promoted_metadata_length, version, decision_version,
      discard_version, updated_at_unix, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  (void)lc_pouch_state_cache_apply_write(
      pouch, namespace_name, &manifest, key, staged.content_type, staged.etag,
      &staged.payload_span, staged.payload_context, promoted_metadata,
      promoted_metadata_length, version, staged.bytes, staged.cipher_bytes,
      staged.descriptor, updated_at_unix, staged.has_query_hidden,
      staged.query_hidden, 1, LC_POUCH_STATE_RECORD_STATE_PUT);
  (void)lc_pouch_state_cache_apply_write(
      pouch, namespace_name, &manifest, staged_key, NULL, staged.etag, NULL,
      NULL, NULL, 0U, discard_version, 0UL, 0UL, NULL, updated_at_unix, 0, 0, 0,
      LC_POUCH_STATE_RECORD_STATE_DELETE);
  out->etag = lc_strdup_with_allocator(&pouch->allocator, staged.etag);
  if (out->etag == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch staged promotion etag", NULL,
                      NULL, NULL);
    goto cleanup;
  }
  out->index_seq = manifest.state_max_version;
  out->version = version;
  out->bytes = staged.bytes;
  out->cipher_bytes = staged.cipher_bytes;
  if (staged.descriptor != NULL) {
    out->descriptor =
        lc_strdup_with_allocator(&pouch->allocator, staged.descriptor);
    if (out->descriptor == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch staged promotion descriptor",
                        NULL, NULL, NULL);
      goto cleanup;
    }
  }
  out->updated_at_unix = updated_at_unix;
  lc_pouch_query_index_note_state_write(
      pouch, namespace_name, key,
      staged.content_type != NULL ? staged.content_type : "application/json",
      NULL, out);
cleanup:
  lc_pouch_state_entry_cleanup(&pouch->allocator, &staged);
  lc_pouch_state_entry_cleanup(&pouch->allocator, &committed);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  lc_free_with_allocator(&pouch->allocator, staged_key);
  if (rc != LC_OK) {
    lc_pouch_state_write_result_cleanup(&pouch->allocator, out);
  }
  return rc;
}

int lc_pouch_state_promote_staged(lc_pouch *pouch, const char *namespace_name,
                                  const char *key, const char *txn_id,
                                  const char *expected_committed_etag,
                                  lc_pouch_state_write_result *out,
                                  lc_error *error) {
  lc_pouch_state_commit_group *commit_group;
  lc_pouch_state_key_lock lock;
  int owns_commit_group;
  int rc;

  commit_group = NULL;
  owns_commit_group = 0;
  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0') {
    return lc_pouch_state_promote_staged_locked(pouch, namespace_name, key,
                                                txn_id, expected_committed_etag,
                                                out, error);
  }
  rc = lc_pouch_state_key_mutation_begin(pouch, namespace_name, key, &lock,
                                         error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                         &owns_commit_group, error);
  if (rc != LC_OK) {
    lc_pouch_state_key_mutation_end(pouch, &lock);
    return rc;
  }
  rc = lc_pouch_state_promote_staged_locked(
      pouch, namespace_name, key, txn_id, expected_committed_etag, out, error);
  rc = lc_pouch_state_finish_commit_group_after_mutation(
      pouch, &lock, commit_group, owns_commit_group, rc, error);
  if (rc == LC_OK) {
    lc_pouch_janitor_note_mutation(pouch);
  }
  return rc;
}

int lc_pouch_state_commit_staged_locked(lc_pouch *pouch,
                                        const char *namespace_name,
                                        const char *key, const char *txn_id,
                                        lc_pouch_state_write_result *out,
                                        lc_error *error) {
  lc_pouch_state_entry committed;
  lc_pouch_state_entry staged;
  lc_pouch_namespace_manifest manifest;
  char *staged_key;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_generation version;
  lc_pouch_generation decision_version;
  lc_pouch_generation discard_version;
  const unsigned char *promoted_metadata;
  size_t promoted_metadata_length;
  lc_pouch_unix_seconds updated_at_unix;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || txn_id == NULL || txn_id[0] == '\0' ||
      out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_commit_staged requires pouch, "
                        "namespace, key, txn_id and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  staged_key = lc_pouch_state_staged_key(&pouch->allocator, key, txn_id);
  if (staged_key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch staged state key", NULL, NULL,
                        NULL);
  }
  memset(&manifest, 0, sizeof(manifest));
  memset(&committed, 0, sizeof(committed));
  memset(&staged, 0, sizeof(staged));
  rc = lc_pouch_state_manifest_lookup_cached_for_mutation(
      pouch, namespace_name, key, &manifest, &committed, NULL, error);
  if (rc == LC_OK) {
    cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 0, NULL);
    if (cache != NULL && cache->initialized) {
      rc = lc_pouch_state_entry_from_cache_record(
          pouch, lc_pouch_state_cache_record_find(cache, staged_key), &staged,
          error);
    } else {
      rc = lc_pouch_state_scan(pouch, &manifest, staged_key, &staged, NULL,
                               error);
    }
  }
  if (rc != LC_OK) {
    goto cleanup;
  }
  if (!staged.found) {
    goto cleanup;
  }
  promoted_metadata = staged.metadata;
  promoted_metadata_length = staged.metadata_length;
  if (promoted_metadata_length == 0U && committed.metadata_length > 0U) {
    promoted_metadata = committed.metadata;
    promoted_metadata_length = committed.metadata_length;
  }

  version = committed.found && committed.payload_span.present
                ? committed.version + 1UL
                : 1UL;
  updated_at_unix = lc_pouch_maintenance_now_seconds();
  decision_version = staged.version + 1UL;
  discard_version = decision_version + 1UL;
  rc = lc_pouch_state_append_staged_commit_batch(
      pouch, namespace_name, &manifest, key, staged_key, &staged,
      promoted_metadata, promoted_metadata_length, version, decision_version,
      discard_version, updated_at_unix, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  (void)lc_pouch_state_cache_apply_write(
      pouch, namespace_name, &manifest, key, staged.content_type, staged.etag,
      &staged.payload_span, staged.payload_context, promoted_metadata,
      promoted_metadata_length, version, staged.bytes, staged.cipher_bytes,
      staged.descriptor, updated_at_unix, staged.has_query_hidden,
      staged.query_hidden, 1, LC_POUCH_STATE_RECORD_STATE_PUT);
  (void)lc_pouch_state_cache_apply_write(
      pouch, namespace_name, &manifest, staged_key, NULL, staged.etag, NULL,
      NULL, NULL, 0U, discard_version, 0UL, 0UL, NULL, updated_at_unix, 0, 0, 0,
      LC_POUCH_STATE_RECORD_STATE_DELETE);
  out->etag = lc_strdup_with_allocator(&pouch->allocator, staged.etag);
  if (out->etag == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch staged commit etag", NULL, NULL,
                      NULL);
    goto cleanup;
  }
  out->index_seq = manifest.state_max_version;
  out->version = version;
  out->bytes = staged.bytes;
  out->cipher_bytes = staged.cipher_bytes;
  if (staged.descriptor != NULL) {
    out->descriptor =
        lc_strdup_with_allocator(&pouch->allocator, staged.descriptor);
    if (out->descriptor == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch staged commit descriptor",
                        NULL, NULL, NULL);
      goto cleanup;
    }
  }
  out->updated_at_unix = updated_at_unix;
  out->has_query_hidden = staged.has_query_hidden;
  out->query_hidden = staged.query_hidden;
  lc_pouch_query_index_note_state_write(
      pouch, namespace_name, key,
      staged.content_type != NULL ? staged.content_type : "application/json",
      NULL, out);
cleanup:
  lc_pouch_state_entry_cleanup(&pouch->allocator, &staged);
  lc_pouch_state_entry_cleanup(&pouch->allocator, &committed);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  lc_free_with_allocator(&pouch->allocator, staged_key);
  if (rc != LC_OK) {
    lc_pouch_state_write_result_cleanup(&pouch->allocator, out);
  }
  return rc;
}

int lc_pouch_state_commit_staged(lc_pouch *pouch, const char *namespace_name,
                                 const char *key, const char *txn_id,
                                 lc_pouch_state_write_result *out,
                                 lc_error *error) {
  lc_pouch_state_commit_group *commit_group;
  lc_pouch_state_key_lock lock;
  int owns_commit_group;
  int rc;

  commit_group = NULL;
  owns_commit_group = 0;
  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0') {
    return lc_pouch_state_commit_staged_locked(pouch, namespace_name, key,
                                               txn_id, out, error);
  }
  rc = lc_pouch_state_key_mutation_begin(pouch, namespace_name, key, &lock,
                                         error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                         &owns_commit_group, error);
  if (rc != LC_OK) {
    lc_pouch_state_key_mutation_end(pouch, &lock);
    return rc;
  }
  rc = lc_pouch_state_commit_staged_locked(pouch, namespace_name, key, txn_id,
                                           out, error);
  rc = lc_pouch_state_finish_commit_group_after_mutation(
      pouch, &lock, commit_group, owns_commit_group, rc, error);
  if (rc == LC_OK) {
    lc_pouch_janitor_note_mutation(pouch);
  }
  return rc;
}

int lc_pouch_state_discard_staged_locked(lc_pouch *pouch,
                                         const char *namespace_name,
                                         const char *key, const char *txn_id,
                                         int *discarded, lc_error *error) {
  lc_pouch_state_entry staged;
  lc_pouch_namespace_manifest manifest;
  char *staged_key;
  char *etag;
  lc_pouch_generation decision_version;
  lc_pouch_generation tombstone_version;
  lc_pouch_unix_seconds updated_at_unix;
  int rc;

  if (discarded != NULL) {
    *discarded = 0;
  }
  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || txn_id == NULL || txn_id[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_discard_staged requires pouch, "
                        "namespace, key and txn_id",
                        NULL, NULL, NULL);
  }
  staged_key = lc_pouch_state_staged_key(&pouch->allocator, key, txn_id);
  if (staged_key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch staged state key", NULL, NULL,
                        NULL);
  }
  memset(&manifest, 0, sizeof(manifest));
  memset(&staged, 0, sizeof(staged));
  etag = NULL;
  rc = lc_pouch_state_manifest_lookup_cached_for_mutation(
      pouch, namespace_name, staged_key, &manifest, &staged, NULL, error);
  if (rc == LC_OK && staged.found) {
    decision_version = staged.version + 1UL;
    tombstone_version = decision_version + 1UL;
    updated_at_unix = lc_pouch_maintenance_now_seconds();
    etag = lc_pouch_state_empty_etag(&pouch->allocator, error);
    if (etag == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    } else {
      rc = lc_pouch_state_append_staged_discard_batch(
          pouch, namespace_name, &manifest, staged_key, etag, decision_version,
          tombstone_version, updated_at_unix, error);
      if (rc == LC_OK) {
        (void)lc_pouch_state_cache_apply_write(
            pouch, namespace_name, &manifest, staged_key, NULL, etag, NULL,
            NULL, NULL, 0U, tombstone_version, 0UL, 0UL, NULL, updated_at_unix,
            0, 0, 0, LC_POUCH_STATE_RECORD_STATE_DELETE);
      }
      if (rc == LC_OK && discarded != NULL) {
        *discarded = 1;
      }
    }
  }
  lc_free_with_allocator(&pouch->allocator, etag);
  lc_pouch_state_entry_cleanup(&pouch->allocator, &staged);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  lc_free_with_allocator(&pouch->allocator, staged_key);
  return rc;
}

int lc_pouch_state_discard_staged(lc_pouch *pouch, const char *namespace_name,
                                  const char *key, const char *txn_id,
                                  int *discarded, lc_error *error) {
  lc_pouch_state_commit_group *commit_group;
  lc_pouch_state_key_lock lock;
  int owns_commit_group;
  int rc;

  commit_group = NULL;
  owns_commit_group = 0;
  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0') {
    return lc_pouch_state_discard_staged_locked(pouch, namespace_name, key,
                                                txn_id, discarded, error);
  }
  rc = lc_pouch_state_key_mutation_begin(pouch, namespace_name, key, &lock,
                                         error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                         &owns_commit_group, error);
  if (rc != LC_OK) {
    lc_pouch_state_key_mutation_end(pouch, &lock);
    return rc;
  }
  rc = lc_pouch_state_discard_staged_locked(pouch, namespace_name, key, txn_id,
                                            discarded, error);
  rc = lc_pouch_state_finish_commit_group_after_mutation(
      pouch, &lock, commit_group, owns_commit_group, rc, error);
  if (rc == LC_OK) {
    lc_pouch_janitor_note_mutation(pouch);
  }
  return rc;
}

static int lc_pouch_state_read_internal(lc_pouch *pouch,
                                        const char *namespace_name,
                                        const char *key, int include_body,
                                        lc_pouch_state_read_result *out,
                                        lc_error *error) {
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_cache_record *record;
  lc_pouch_state_entry current;
  lc_pouch_namespace_manifest manifest;
  lc_pouch_state_process_namespace_mutex *process_mutex;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_read requires pouch, namespace, key "
                        "and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  process_mutex = NULL;
  rc = lc_pouch_state_process_namespace_mutex_lock(pouch, namespace_name,
                                                   &process_mutex, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_manifest_lookup_cached(pouch, namespace_name, key,
                                             &manifest, &current, NULL, error);
  if (rc != LC_OK || !current.found) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    goto cleanup_unlocked;
  }
  if (include_body && !current.payload_span.present) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    goto cleanup_unlocked;
  }
  cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 0, NULL);
  record = cache != NULL ? lc_pouch_state_cache_record_find(cache, key) : NULL;
  if (include_body && lc_pouch_single_writer_enabled(pouch) && cache != NULL &&
      record != NULL) {
    char *payload_span_path;
    char *crypto_context;

    payload_span_path = lc_pouch_state_payload_span_path(
        &pouch->allocator, manifest.namespace_path, &current.payload_span,
        error);
    crypto_context = payload_span_path != NULL
                         ? lc_pouch_state_payload_context_for_read(
                               &pouch->allocator, namespace_name, current.key,
                               current.payload_context)
                         : NULL;
    if (payload_span_path == NULL || crypto_context == NULL) {
      lc_free_with_allocator(&pouch->allocator, payload_span_path);
      lc_free_with_allocator(&pouch->allocator, crypto_context);
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state cached read context",
                        NULL, NULL, NULL);
    } else {
      memset(out, 0, sizeof(*out));
      rc = lc_pouch_state_read_many_snapshot_body_from_cache(
          pouch, cache, record, crypto_context, payload_span_path,
          current.payload_span.payload_offset,
          current.payload_span.payload_length, &current, 0, &out->body, error);
      if (rc == LC_OK) {
        out->content_type = current.content_type;
        out->etag = current.etag;
        out->descriptor = current.descriptor;
        out->metadata = current.metadata;
        out->metadata_length = current.metadata_length;
        out->index_seq = current.index_seq;
        out->version = current.version;
        out->bytes = current.bytes;
        out->cipher_bytes = current.cipher_bytes;
        out->updated_at_unix = current.updated_at_unix;
        out->has_query_hidden = current.has_query_hidden;
        out->query_hidden = current.query_hidden;
        out->has_body = current.payload_span.present;
        out->found = 1;
        current.content_type = NULL;
        current.etag = NULL;
        current.descriptor = NULL;
        current.metadata = NULL;
        current.metadata_length = 0U;
      }
      lc_free_with_allocator(&pouch->allocator, payload_span_path);
      lc_free_with_allocator(&pouch->allocator, crypto_context);
    }
  } else {
    rc = lc_pouch_state_read_result_from_entry(
        pouch, namespace_name, &manifest, &current, include_body, out, error);
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
cleanup_unlocked:
  lc_pouch_state_process_namespace_mutex_unlock(&process_mutex);
  if (rc == LC_OK) {
    pslog_field fields[6];

    fields[0] = lc_log_str_field("ns", namespace_name);
    fields[1] = lc_log_str_field("key", key);
    fields[2] = lc_log_bool_field("with_state", include_body);
    fields[3] = lc_log_bool_field("found", out != NULL ? out->found : 0);
    fields[4] =
        lc_log_u64_field("payload_bytes", out != NULL ? out->bytes : 0UL);
    fields[5] =
        lc_log_u64_field("stored_bytes", out != NULL ? out->cipher_bytes : 0UL);
    lc_log_trace(pouch->logger,
                 include_body ? "logstore.read" : "logstore.read.meta", fields,
                 6U);
  } else {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("ns", namespace_name);
    fields[1] = lc_log_str_field("key", key);
    fields[2] = lc_log_bool_field("with_state", include_body);
    fields[3] = lc_log_error_field("error", error);
    fields[4] = lc_log_code_field(error);
    lc_log_error(pouch->logger, "logstore.read.error", fields, 5U);
  }
  return rc;
}

int lc_pouch_state_read_metadata_locked(lc_pouch *pouch,
                                        const char *namespace_name,
                                        const char *key,
                                        lc_pouch_state_read_result *out,
                                        lc_error *error) {
  lc_pouch_state_entry current;
  lc_pouch_namespace_manifest manifest;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_read_metadata_locked requires pouch, "
                        "namespace, key and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  rc = lc_pouch_state_manifest_lookup_cached(pouch, namespace_name, key,
                                             &manifest, &current, NULL, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (rc == LC_OK && current.found) {
    rc = lc_pouch_state_read_result_from_entry(pouch, namespace_name, &manifest,
                                               &current, 0, out, error);
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

int lc_pouch_state_read_locked(lc_pouch *pouch, const char *namespace_name,
                               const char *key, lc_pouch_state_read_result *out,
                               lc_error *error) {
  lc_pouch_state_entry current;
  lc_pouch_namespace_manifest manifest;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_read_locked requires pouch, namespace, "
                        "key and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  memset(&current, 0, sizeof(current));
  memset(&manifest, 0, sizeof(manifest));
  rc = lc_pouch_state_manifest_lookup_cached(pouch, namespace_name, key,
                                             &manifest, &current, NULL, error);
  if (rc == LC_OK && current.found && current.payload_span.present) {
    rc = lc_pouch_state_read_result_from_entry(pouch, namespace_name, &manifest,
                                               &current, 1, out, error);
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

int lc_pouch_state_read_metadata_view_locked(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    lc_pouch_state_metadata_view *out,
    lc_pouch_state_read_result *owned_fallback, lc_error *error) {
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_cache_record *record;
  uint64_t writer_mode_epoch;
  int single_writer;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || out == NULL || owned_fallback == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state metadata view requires pouch, namespace, "
                        "key, view and fallback",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  memset(owned_fallback, 0, sizeof(*owned_fallback));
  cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 0, NULL);
  single_writer = lc_pouch_single_writer_snapshot(pouch, &writer_mode_epoch);
  if (single_writer && cache != NULL && cache->initialized &&
      cache->writer_mode_epoch == writer_mode_epoch &&
      cache->namespace_path != NULL && cache->active_segment_leaf != NULL) {
    record = lc_pouch_state_cache_record_find(cache, key);
    if (record != NULL && record->found) {
      out->found = 1;
      out->version = record->version;
      out->metadata = record->metadata;
      out->metadata_length = record->metadata_length;
      out->has_query_hidden = record->has_query_hidden;
      out->query_hidden = record->query_hidden;
      out->has_body = record->payload_span.present;
    }
    return LC_OK;
  }
  rc = lc_pouch_state_read_metadata_locked(pouch, namespace_name, key,
                                           owned_fallback, error);
  if (rc == LC_OK && owned_fallback->found) {
    out->found = 1;
    out->version = owned_fallback->version;
    out->metadata = owned_fallback->metadata;
    out->metadata_length = owned_fallback->metadata_length;
    out->has_query_hidden = owned_fallback->has_query_hidden;
    out->query_hidden = owned_fallback->query_hidden;
    out->has_body = owned_fallback->has_body;
  }
  return rc;
}

int lc_pouch_state_read(lc_pouch *pouch, const char *namespace_name,
                        const char *key, lc_pouch_state_read_result *out,
                        lc_error *error) {
  return lc_pouch_state_read_internal(pouch, namespace_name, key, 1, out,
                                      error);
}

int lc_pouch_state_copy(lc_pouch *pouch, const char *namespace_name,
                        const char *key, lc_sink *dst,
                        lc_pouch_state_read_result *out, lc_error *error) {
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_cache_record *record;
  lc_pouch_state_entry current;
  lc_pouch_namespace_manifest manifest;
  lc_pouch_state_process_namespace_mutex *process_mutex;
  lc_source *source;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_copy requires pouch, namespace, key, "
                        "sink, and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  memset(&current, 0, sizeof(current));
  memset(&manifest, 0, sizeof(manifest));
  process_mutex = NULL;
  source = NULL;
  rc = lc_pouch_state_process_namespace_mutex_lock(pouch, namespace_name,
                                                   &process_mutex, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (lc_pouch_single_writer_enabled(pouch)) {
    cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 0, NULL);
    record =
        cache != NULL ? lc_pouch_state_cache_record_find(cache, key) : NULL;
    if (record != NULL && record->found && record->body_cache != NULL &&
        record->body_cache->version == record->version &&
        record->body_cache->length == (size_t)record->bytes) {
      rc = lc_pouch_state_read_result_from_cache_record(pouch, record, out,
                                                        error);
      if (rc == LC_OK) {
        rc = lc_pouch_state_body_cache_source_open(
            &pouch->allocator, record->body_cache, &source, error);
      }
      goto cleanup_unlocked;
    }
  }
  rc = lc_pouch_state_manifest_lookup_cached(pouch, namespace_name, key,
                                             &manifest, &current, NULL, error);
  if (rc != LC_OK || !current.found) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    goto cleanup_unlocked;
  }
  if (!current.payload_span.present) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    goto cleanup_unlocked;
  }
  cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 0, NULL);
  record = cache != NULL ? lc_pouch_state_cache_record_find(cache, key) : NULL;
  if (cache != NULL && record != NULL && record->body_cache != NULL &&
      record->body_cache->version == record->version &&
      record->body_cache->length == (size_t)record->bytes) {
    if (rc == LC_OK) {
      rc = lc_pouch_state_read_result_from_cache_record(pouch, record, out,
                                                        error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_body_cache_source_open(
          &pouch->allocator, record->body_cache, &source, error);
    }
  } else {
    rc = lc_pouch_state_read_result_from_entry(pouch, namespace_name, &manifest,
                                               &current, 1, out, error);
    if (rc == LC_OK && out->found) {
      source = out->body;
      out->body = NULL;
    }
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
cleanup_unlocked:
  lc_pouch_state_process_namespace_mutex_unlock(&process_mutex);
  /* A returned source owns either a duplicated segment descriptor or a body
   * cache reference. Stream only after releasing namespace coordination so a
   * slow caller cannot serialize unrelated reads or projection snapshots. */
  if (rc == LC_OK && source != NULL && out->found && out->bytes > 0UL &&
      out->bytes <= (uint64_t)(size_t)-1) {
    rc = lc_sink_memory_reserve(dst, (size_t)out->bytes, error);
  }
  if (rc == LC_OK && source != NULL) {
    rc = lc_copy(source, dst, NULL, error);
  }
  if (source != NULL) {
    source->close(source);
  }
  return rc;
}

int lc_pouch_state_read_metadata(lc_pouch *pouch, const char *namespace_name,
                                 const char *key,
                                 lc_pouch_state_read_result *out,
                                 lc_error *error) {
  return lc_pouch_state_read_internal(pouch, namespace_name, key, 0, out,
                                      error);
}

static int lc_pouch_state_read_many_internal(
    lc_pouch *pouch, const char *namespace_name, const char *const *keys,
    size_t key_count, lc_pouch_state_read_many_fn visitor, void *context,
    int include_body, int copy_cached_body, lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_cache_record **record_index;
  lc_pouch_state_read_many_snapshot *snapshots;
  size_t record_index_count;
  size_t index;
  size_t snapshot_count;
  lc_pouch_state_process_namespace_mutex *process_mutex;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      (keys == NULL && key_count != 0U) || visitor == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_read_many requires pouch, namespace, "
                        "keys, and visitor",
                        NULL, NULL, NULL);
  }
  snapshots = NULL;
  snapshot_count = 0U;
  record_index = NULL;
  record_index_count = 0U;
  if (key_count > 0U) {
    snapshots = (lc_pouch_state_read_many_snapshot *)lc_calloc_with_allocator(
        &pouch->allocator, key_count, sizeof(*snapshots));
    if (snapshots == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch read-many snapshots", NULL,
                          NULL, NULL);
    }
  }
  process_mutex = NULL;
  rc = lc_pouch_ensure_namespace(pouch, namespace_name, error);
  if (rc != LC_OK) {
    lc_pouch_state_read_many_snapshots_cleanup(&pouch->allocator, snapshots,
                                               snapshot_count);
    return rc;
  }
  rc = lc_pouch_state_process_namespace_mutex_lock(pouch, namespace_name,
                                                   &process_mutex, error);
  if (rc != LC_OK) {
    lc_pouch_state_read_many_snapshots_cleanup(&pouch->allocator, snapshots,
                                               snapshot_count);
    return rc;
  }
  rc = lc_pouch_state_manifest_view(pouch, namespace_name, &manifest, &cache,
                                    error);
  if (rc == LC_OK && key_count > 8U) {
    rc = lc_pouch_state_cache_record_index_build(pouch, cache, &record_index,
                                                 &record_index_count, error);
  }
  for (index = 0U; rc == LC_OK && index < key_count; ++index) {
    lc_pouch_state_entry current;
    lc_pouch_state_cache_record *record;

    if (keys[index] == NULL || keys[index][0] == '\0') {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch read-many requires non-empty keys", NULL, NULL,
                        NULL);
      break;
    }
    memset(&current, 0, sizeof(current));
    record = record_index != NULL
                 ? lc_pouch_state_cache_record_index_find(
                       record_index, record_index_count, keys[index])
                 : lc_pouch_state_cache_record_find(cache, keys[index]);
    rc = lc_pouch_state_entry_from_cache_record(pouch, record, &current, error);
    if (rc == LC_OK) {
      rc = lc_pouch_state_read_many_snapshot_from_entry(
          pouch, namespace_name, &manifest, keys[index], cache, record,
          &current, include_body, copy_cached_body, &snapshots[index], error);
      if (rc == LC_OK) {
        snapshot_count = index + 1U;
      }
    }
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  }
  lc_free_with_allocator(&pouch->allocator, record_index);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  lc_pouch_state_process_namespace_mutex_unlock(&process_mutex);
  if (rc == LC_OK) {
    for (index = 0U; rc == LC_OK && index < snapshot_count; ++index) {
      lc_pouch_state_read_result read_result;

      memset(&read_result, 0, sizeof(read_result));
      if (snapshots[index].found) {
        if (include_body) {
          read_result.body = snapshots[index].body;
          snapshots[index].body = NULL;
        }
        read_result.content_type = snapshots[index].content_type;
        read_result.etag = snapshots[index].etag;
        read_result.descriptor = snapshots[index].descriptor;
        read_result.metadata = snapshots[index].metadata;
        read_result.metadata_length = snapshots[index].metadata_length;
        read_result.index_seq = snapshots[index].index_seq;
        read_result.version = snapshots[index].version;
        read_result.bytes = snapshots[index].bytes;
        read_result.cipher_bytes = snapshots[index].cipher_bytes;
        read_result.updated_at_unix = snapshots[index].updated_at_unix;
        read_result.has_query_hidden = snapshots[index].has_query_hidden;
        read_result.query_hidden = snapshots[index].query_hidden;
        read_result.found = 1;
        snapshots[index].content_type = NULL;
        snapshots[index].etag = NULL;
        snapshots[index].descriptor = NULL;
        snapshots[index].metadata = NULL;
        snapshots[index].metadata_length = 0U;
      }
      rc = visitor(snapshots[index].key, &read_result, context, error);
      lc_pouch_state_read_result_cleanup(&pouch->allocator, &read_result);
      if (rc == LC_POUCH_STATE_READ_MANY_STOP) {
        rc = LC_OK;
        break;
      }
    }
  }
  lc_pouch_state_read_many_snapshots_cleanup(&pouch->allocator, snapshots,
                                             snapshot_count);
  if (rc == LC_OK) {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("ns", namespace_name);
    fields[1] = lc_log_u64_field("count", key_count);
    fields[2] = lc_log_bool_field("with_state", include_body);
    fields[3] = lc_log_bool_field("copy_cached_body", copy_cached_body);
    lc_log_trace(pouch->logger, "logstore.read.many", fields, 4U);
  } else {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("ns", namespace_name);
    fields[1] = lc_log_u64_field("count", key_count);
    fields[2] = lc_log_error_field("error", error);
    fields[3] = lc_log_code_field(error);
    lc_log_error(pouch->logger, "logstore.read.many.error", fields, 4U);
  }
  return rc;
}

int lc_pouch_state_read_many(lc_pouch *pouch, const char *namespace_name,
                             const char *const *keys, size_t key_count,
                             lc_pouch_state_read_many_fn visitor, void *context,
                             lc_error *error) {
  return lc_pouch_state_read_many_internal(
      pouch, namespace_name, keys, key_count, visitor, context, 1, 1, error);
}

int lc_pouch_state_read_many_cached(lc_pouch *pouch, const char *namespace_name,
                                    const char *const *keys, size_t key_count,
                                    lc_pouch_state_read_many_fn visitor,
                                    void *context, lc_error *error) {
  return lc_pouch_state_read_many_internal(
      pouch, namespace_name, keys, key_count, visitor, context, 1, 0, error);
}

int lc_pouch_state_read_many_metadata(lc_pouch *pouch,
                                      const char *namespace_name,
                                      const char *const *keys, size_t key_count,
                                      lc_pouch_state_read_many_fn visitor,
                                      void *context, lc_error *error) {
  return lc_pouch_state_read_many_internal(
      pouch, namespace_name, keys, key_count, visitor, context, 0, 1, error);
}

int lc_pouch_state_scan_summary_read_body(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_state_scan_summary_entry *entry,
    lc_pouch_state_read_result *out, lc_error *error) {
  lc_pouch_state_scan_body_snapshot *snapshot;

  if (entry == NULL || entry->opaque == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch scan summary body read requires a summary row",
                        NULL, NULL, "pouch");
  }
  snapshot = (lc_pouch_state_scan_body_snapshot *)entry->opaque;
  return lc_pouch_state_scan_body_snapshot_open_result(pouch, namespace_name,
                                                       snapshot, out, error);
}

void lc_pouch_state_scan_summaries_result_cleanup(
    const lc_allocator *allocator,
    lc_pouch_state_scan_summaries_result *result) {
  if (result == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, result->next_start_after);
  memset(result, 0, sizeof(*result));
}

int lc_pouch_state_scan_summaries(lc_pouch *pouch, const char *namespace_name,
                                  const char *start_after, size_t limit,
                                  lc_pouch_state_scan_summary_visit_fn visitor,
                                  void *context,
                                  lc_pouch_state_scan_summaries_result *out,
                                  lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_cache_record **record_index;
  lc_pouch_state_scan_body_snapshot *snapshots;
  size_t record_index_count;
  size_t snapshot_count;
  size_t snapshot_capacity;
  size_t index;
  size_t next_index;
  lc_pouch_state_process_namespace_mutex *process_mutex;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      visitor == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_scan_summaries requires pouch, "
                        "namespace, visitor, and output",
                        NULL, NULL, "pouch");
  }
  memset(out, 0, sizeof(*out));
  if (limit == 0U) {
    limit = 2048U;
  }
  record_index = NULL;
  record_index_count = 0U;
  snapshots = NULL;
  snapshot_count = 0U;
  snapshot_capacity = 0U;
  process_mutex = NULL;
  rc = lc_pouch_state_process_namespace_mutex_lock(pouch, namespace_name,
                                                   &process_mutex, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_manifest_view(pouch, namespace_name, &manifest, &cache,
                                    error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_cache_record_index_build(pouch, cache, &record_index,
                                                 &record_index_count, error);
  }
  index = rc == LC_OK ? lc_pouch_state_cache_record_index_start_after(
                            record_index, record_index_count, start_after)
                      : 0U;
  for (; rc == LC_OK && index < record_index_count && snapshot_count < limit;
       ++index) {
    lc_pouch_state_cache_record *record;

    record = record_index[index];
    if (record == NULL || !record->found) {
      continue;
    }
    rc = lc_pouch_state_scan_body_snapshot_append(
        pouch, &snapshots, &snapshot_count, &snapshot_capacity, &manifest,
        record, error);
  }
  if (rc == LC_OK && snapshot_count > 0U) {
    next_index = index;
    while (next_index < record_index_count &&
           (record_index[next_index] == NULL ||
            !record_index[next_index]->found)) {
      ++next_index;
    }
    if (next_index < record_index_count) {
      out->truncated = 1;
      out->next_start_after = lc_strdup_with_allocator(
          &pouch->allocator, snapshots[snapshot_count - 1U].key);
      if (out->next_start_after == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch scan cursor", NULL, NULL,
                          NULL);
      }
    }
  }
  lc_free_with_allocator(&pouch->allocator, record_index);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  lc_pouch_state_process_namespace_mutex_unlock(&process_mutex);
  for (index = 0U; rc == LC_OK && index < snapshot_count; ++index) {
    lc_pouch_state_scan_summary_entry entry;

    memset(&entry, 0, sizeof(entry));
    entry.key = snapshots[index].key;
    entry.content_type = snapshots[index].content_type;
    entry.etag = snapshots[index].etag;
    entry.descriptor = snapshots[index].descriptor;
    entry.version = snapshots[index].version;
    entry.bytes = snapshots[index].bytes;
    entry.cipher_bytes = snapshots[index].cipher_bytes;
    entry.updated_at_unix = snapshots[index].updated_at_unix;
    entry.has_query_hidden = snapshots[index].has_query_hidden;
    entry.query_hidden = snapshots[index].query_hidden;
    entry.opaque = &snapshots[index];
    rc = visitor(&entry, context, error);
    if (rc != LC_OK) {
      break;
    }
  }
  lc_pouch_state_scan_body_snapshots_cleanup(&pouch->allocator, snapshots,
                                             snapshot_count);
  if (rc != LC_OK) {
    lc_pouch_state_scan_summaries_result_cleanup(&pouch->allocator, out);
  }
  if (rc == LC_OK) {
    pslog_field fields[5];

    fields[0] = lc_log_str_field("ns", namespace_name);
    fields[1] = lc_log_str_field("cursor", start_after);
    fields[2] = lc_log_u64_field("limit", limit);
    fields[3] = lc_log_u64_field("count", snapshot_count);
    fields[4] =
        lc_log_bool_field("truncated", out != NULL ? out->truncated : 0);
    lc_log_trace(pouch->logger, "scan.summaries", fields, 5U);
  } else {
    pslog_field fields[4];

    fields[0] = lc_log_str_field("ns", namespace_name);
    fields[1] = lc_log_u64_field("limit", limit);
    fields[2] = lc_log_error_field("error", error);
    fields[3] = lc_log_code_field(error);
    lc_log_error(pouch->logger, "scan.summaries.error", fields, 4U);
  }
  return rc;
}

static int lc_pouch_state_visit_internal(lc_pouch *pouch,
                                         const char *namespace_name,
                                         lc_pouch_state_visit_fn visitor,
                                         void *context, int locked,
                                         lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_cache_record *record;
  lc_pouch_state_visit_snapshot *snapshots;
  size_t snapshot_count;
  size_t snapshot_capacity;
  size_t i;
  lc_pouch_state_process_namespace_mutex *process_mutex;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      visitor == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_visit requires pouch, namespace, and "
                        "visitor",
                        NULL, NULL, NULL);
  }
  process_mutex = NULL;
  if (locked) {
    rc = lc_pouch_state_ensure_namespace_locked(pouch, namespace_name, error);
    if (rc != LC_OK) {
      return rc;
    }
  } else {
    rc = lc_pouch_ensure_namespace(pouch, namespace_name, error);
    if (rc != LC_OK) {
      return rc;
    }
    rc = lc_pouch_state_process_namespace_mutex_lock(pouch, namespace_name,
                                                     &process_mutex, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  snapshots = NULL;
  snapshot_count = 0U;
  snapshot_capacity = 0U;
  rc = lc_pouch_state_manifest_view(pouch, namespace_name, &manifest, &cache,
                                    error);
  for (record = rc == LC_OK ? cache->records : NULL; record != NULL;
       record = record->next) {
    if (!record->found) {
      continue;
    }
    rc = lc_pouch_state_visit_snapshot_append(
        &pouch->allocator, &snapshots, &snapshot_count, &snapshot_capacity,
        record, error);
    if (rc != LC_OK) {
      break;
    }
  }
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  if (!locked) {
    lc_pouch_state_process_namespace_mutex_unlock(&process_mutex);
  }
  for (i = 0U; rc == LC_OK && i < snapshot_count; ++i) {
    lc_pouch_state_visit_entry entry;

    memset(&entry, 0, sizeof(entry));
    entry.key = snapshots[i].key;
    entry.content_type = snapshots[i].content_type;
    entry.etag = snapshots[i].etag;
    entry.version = snapshots[i].version;
    entry.bytes = snapshots[i].bytes;
    entry.cipher_bytes = snapshots[i].cipher_bytes;
    entry.descriptor = snapshots[i].descriptor;
    entry.metadata = snapshots[i].metadata;
    entry.metadata_length = snapshots[i].metadata_length;
    entry.updated_at_unix = snapshots[i].updated_at_unix;
    entry.has_query_hidden = snapshots[i].has_query_hidden;
    entry.query_hidden = snapshots[i].query_hidden;
    rc = visitor(&entry, context, error);
    if (rc != LC_OK) {
      break;
    }
  }
  lc_pouch_state_visit_snapshots_cleanup(&pouch->allocator, snapshots,
                                         snapshot_count);
  lc_pouch_state_process_namespace_mutex_unlock(&process_mutex);
  if (rc == LC_OK) {
    pslog_field fields[2];

    fields[0] = lc_log_str_field("ns", namespace_name);
    fields[1] = lc_log_u64_field("count", snapshot_count);
    lc_log_trace(pouch->logger, "scan.visit", fields, 2U);
  } else {
    pslog_field fields[3];

    fields[0] = lc_log_str_field("ns", namespace_name);
    fields[1] = lc_log_error_field("error", error);
    fields[2] = lc_log_code_field(error);
    lc_log_error(pouch->logger, "scan.visit.error", fields, 3U);
  }
  return rc;
}

int lc_pouch_state_visit(lc_pouch *pouch, const char *namespace_name,
                         lc_pouch_state_visit_fn visitor, void *context,
                         lc_error *error) {
  return lc_pouch_state_visit_internal(pouch, namespace_name, visitor, context,
                                       0, error);
}

int lc_pouch_state_visible_count(lc_pouch *pouch, const char *namespace_name,
                                 size_t *count, lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_cache_record *record;
  lc_pouch_state_process_namespace_mutex *process_mutex;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      count == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_visible_count requires pouch, "
                        "namespace, and count",
                        NULL, NULL, NULL);
  }
  *count = 0U;
  process_mutex = NULL;
  rc = lc_pouch_state_process_namespace_mutex_lock(pouch, namespace_name,
                                                   &process_mutex, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_manifest_view(pouch, namespace_name, &manifest, &cache,
                                    error);
  if (rc == LC_OK) {
    for (record = cache->records; record != NULL; record = record->next) {
      if (!record->found ||
          (record->has_query_hidden && record->query_hidden)) {
        continue;
      }
      if (strncmp(record->key, ".staging/", sizeof(".staging/") - 1U) == 0 ||
          strstr(record->key, "/.staging/") != NULL) {
        continue;
      }
      ++*count;
    }
  }
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);

  lc_pouch_state_process_namespace_mutex_unlock(&process_mutex);
  return rc;
}

int lc_pouch_state_visit_since(lc_pouch *pouch, const char *namespace_name,
                               lc_pouch_generation after_version,
                               lc_pouch_state_change_visit_fn visitor,
                               void *context, lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_cache_record *record;
  lc_pouch_state_visit_snapshot *snapshots;
  lc_pouch_state_process_namespace_mutex *process_mutex;
  size_t snapshot_count;
  size_t snapshot_capacity;
  size_t i;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      visitor == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_visit_since requires pouch, "
                        "namespace, and visitor",
                        NULL, NULL, NULL);
  }
  process_mutex = NULL;
  snapshots = NULL;
  snapshot_count = 0U;
  snapshot_capacity = 0U;
  rc = lc_pouch_ensure_namespace(pouch, namespace_name, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  rc = lc_pouch_state_process_namespace_mutex_lock(pouch, namespace_name,
                                                   &process_mutex, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  rc = lc_pouch_state_manifest_view(pouch, namespace_name, &manifest, &cache,
                                    error);
  for (record = rc == LC_OK ? cache->records : NULL; record != NULL;
       record = record->next) {
    if (record->index_seq <= after_version) {
      continue;
    }
    rc = lc_pouch_state_visit_snapshot_append(
        &pouch->allocator, &snapshots, &snapshot_count, &snapshot_capacity,
        record, error);
    if (rc != LC_OK) {
      break;
    }
  }
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  lc_pouch_state_process_namespace_mutex_unlock(&process_mutex);
  for (i = 0U; rc == LC_OK && i < snapshot_count; ++i) {
    lc_pouch_state_change_visit_entry entry;

    memset(&entry, 0, sizeof(entry));
    entry.key = snapshots[i].key;
    entry.content_type = snapshots[i].content_type;
    entry.etag = snapshots[i].etag;
    entry.version = snapshots[i].version;
    entry.bytes = snapshots[i].bytes;
    entry.cipher_bytes = snapshots[i].cipher_bytes;
    entry.descriptor = snapshots[i].descriptor;
    entry.updated_at_unix = snapshots[i].updated_at_unix;
    entry.has_query_hidden = snapshots[i].has_query_hidden;
    entry.query_hidden = snapshots[i].query_hidden;
    entry.found = snapshots[i].found;
    rc = visitor(&entry, context, error);
  }
  lc_pouch_state_visit_snapshots_cleanup(&pouch->allocator, snapshots,
                                         snapshot_count);
  return rc;

cleanup:
  lc_pouch_state_process_namespace_mutex_unlock(&process_mutex);
  lc_pouch_state_visit_snapshots_cleanup(&pouch->allocator, snapshots,
                                         snapshot_count);
  return rc;
}

int lc_pouch_state_index_seq(lc_pouch *pouch, const char *namespace_name,
                             lc_pouch_generation *out, lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  lc_pouch_namespace_logstore *cache;
  lc_pouch_state_process_namespace_mutex *process_mutex;
  uint64_t writer_mode_epoch;
  int single_writer;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_index_seq requires pouch, namespace, "
                        "and out",
                        NULL, NULL, NULL);
  }
  *out = 0UL;
  process_mutex = NULL;
  single_writer = lc_pouch_single_writer_snapshot(pouch, &writer_mode_epoch);
  cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 0, NULL);
  if (single_writer && cache != NULL && cache->initialized &&
      cache->writer_mode_epoch == writer_mode_epoch) {
    *out = cache->max_version;
    return LC_OK;
  }
  if (!single_writer) {
    rc = lc_pouch_state_process_namespace_mutex_lock(pouch, namespace_name,
                                                     &process_mutex, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  rc = lc_pouch_namespace_ensure_layout(&pouch->allocator, pouch->root_path,
                                        namespace_name, error);
  if (rc != LC_OK) {
    goto cleanup_unlocked;
  }
  memset(&manifest, 0, sizeof(manifest));
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                        namespace_name, &manifest, NULL, NULL,
                                        error);
  if (rc != LC_OK) {
    goto cleanup_unlocked;
  }
  cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 0, NULL);
  if (single_writer) {
    if (cache != NULL && cache->initialized) {
      *out = cache->max_version > manifest.state_max_version
                 ? cache->max_version
                 : manifest.state_max_version;
    } else if (manifest.state_max_version > 0UL) {
      *out = manifest.state_max_version;
      rc = LC_OK;
    } else {
      rc = lc_pouch_state_manifest_max_version(pouch, &manifest, out, error);
    }
  } else {
    cache = lc_pouch_namespace_logstore_find(pouch, namespace_name, 1, error);
    if (cache == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    } else {
      rc =
          lc_pouch_state_cache_refresh_for_mode(pouch, cache, &manifest, error);
      if (rc == LC_OK) {
        *out = cache->max_version;
      }
    }
  }
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
cleanup_unlocked:
  lc_pouch_state_process_namespace_mutex_unlock(&process_mutex);
  return rc;
}

void lc_pouch_state_read_result_cleanup(const lc_allocator *allocator,
                                        lc_pouch_state_read_result *result) {
  if (result == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, result->content_type);
  lc_free_with_allocator(allocator, result->etag);
  lc_free_with_allocator(allocator, result->descriptor);
  lc_free_with_allocator(allocator, result->metadata);
  if (result->body != NULL) {
    result->body->close(result->body);
  }
  memset(result, 0, sizeof(*result));
}

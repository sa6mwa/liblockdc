#include "lc_pouch.h"

#include "lc_api_internal.h"
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
#define LC_POUCH_STATE_DECISION_COMMITTED "committed"
#define LC_POUCH_STATE_DECISION_DISCARDED "discarded"
#define LC_POUCH_STATE_HIGH_WATER_KEY ".lockd/high-water"

#ifdef LOCKDC_TEST_BUILD
lc_pouch_test_hook lc_pouch_test_after_snapshot_write_hook = NULL;
void *lc_pouch_test_after_snapshot_write_context = NULL;
#endif

typedef struct lc_pouch_state_namespace_lock {
  int fd;
  struct lc_pouch_state_process_namespace_mutex *process_mutex;
} lc_pouch_state_namespace_lock;

typedef struct lc_pouch_state_deferred_fsync {
  int fd;
  struct lc_pouch_state_deferred_fsync *next;
} lc_pouch_state_deferred_fsync;

typedef struct lc_pouch_state_deferred_marker {
  char *namespace_path;
  struct lc_pouch_state_deferred_marker *next;
} lc_pouch_state_deferred_marker;

typedef struct lc_pouch_state_commit_group {
  lc_pouch *pouch;
  struct lc_pouch_state_commit_group *parent;
  lc_pouch_state_deferred_fsync *head;
  lc_pouch_state_deferred_fsync *tail;
  lc_pouch_state_deferred_marker *marker_head;
  lc_pouch_state_deferred_marker *marker_tail;
} lc_pouch_state_commit_group;

typedef struct lc_pouch_state_process_namespace_mutex {
  char *identity;
  pthread_mutex_t mutex;
  unsigned long refcount;
  struct lc_pouch_state_process_namespace_mutex *next;
} lc_pouch_state_process_namespace_mutex;

static pthread_mutex_t lc_pouch_state_process_mutex_registry =
    PTHREAD_MUTEX_INITIALIZER;
static lc_pouch_state_process_namespace_mutex *lc_pouch_state_process_mutexes;
static pthread_once_t lc_pouch_state_commit_group_key_once = PTHREAD_ONCE_INIT;
static pthread_key_t lc_pouch_state_commit_group_key;

static void lc_pouch_state_commit_group_key_init(void) {
  (void)pthread_key_create(&lc_pouch_state_commit_group_key, NULL);
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
  lc_pouch_state_deferred_marker *marker;
  lc_pouch_state_deferred_marker *marker_next;

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
  marker = group->marker_head;
  while (marker != NULL) {
    marker_next = marker->next;
    lc_free_with_allocator(NULL, marker->namespace_path);
    lc_free_with_allocator(NULL, marker);
    marker = marker_next;
  }
  lc_free_with_allocator(NULL, group);
}

static int lc_pouch_state_touch_marker_now(lc_pouch *pouch,
                                           const char *namespace_path,
                                           lc_error *error) {
  unsigned long sequence;

  if (pouch == NULL || namespace_path == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch marker touch requires pouch and namespace path",
                        NULL, NULL, "pouch");
  }
  if (pouch->single_writer) {
    return LC_OK;
  }
  sequence = ++pouch->marker_sequence;
  return lc_pouch_namespace_touch_marker(&pouch->allocator, namespace_path,
                                         pouch->writer_marker_leaf, sequence,
                                         error);
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
  if (rc == LC_OK) {
    lc_pouch_state_deferred_marker *marker;

    for (marker = group->marker_head; marker != NULL; marker = marker->next) {
      rc = lc_pouch_state_touch_marker_now(group->pouch, marker->namespace_path,
                                           error);
      if (rc != LC_OK) {
        break;
      }
    }
  }
  if (rc != LC_OK) {
    lc_pouch_state_cache_cleanup(group->pouch);
  }
  lc_pouch_state_commit_group_free(group);
  return rc;
}

static int lc_pouch_state_defer_fsync(lc_pouch *pouch, int fd,
                                      lc_error *error) {
  lc_pouch_state_commit_group *group;
  lc_pouch_state_deferred_fsync *item;
  int dup_fd;

  group = lc_pouch_state_commit_group_current();
  if (group == NULL || group->pouch != pouch) {
    return lc_pouch_fsync_commit(pouch, fd, error);
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
  if (group->tail != NULL) {
    group->tail->next = item;
  } else {
    group->head = item;
  }
  group->tail = item;
  return LC_OK;
}

static int
lc_pouch_state_defer_marker(lc_pouch *pouch,
                            const lc_pouch_namespace_manifest *manifest,
                            lc_error *error) {
  lc_pouch_state_commit_group *group;
  lc_pouch_state_deferred_marker *marker;

  if (pouch == NULL || manifest == NULL || manifest->namespace_path == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch marker deferral requires pouch and manifest",
                        NULL, NULL, "pouch");
  }
  group = lc_pouch_state_commit_group_current();
  if (group == NULL || group->pouch != pouch) {
    return lc_pouch_state_touch_marker_now(pouch, manifest->namespace_path,
                                           error);
  }
  marker = (lc_pouch_state_deferred_marker *)lc_calloc_with_allocator(
      NULL, 1U, sizeof(*marker));
  if (marker == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch deferred marker", NULL, NULL,
                        "pouch");
  }
  marker->namespace_path =
      lc_strdup_with_allocator(NULL, manifest->namespace_path);
  if (marker->namespace_path == NULL) {
    lc_free_with_allocator(NULL, marker);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch deferred marker path", NULL, NULL,
                        "pouch");
  }
  if (group->marker_tail != NULL) {
    group->marker_tail->next = marker;
  } else {
    group->marker_head = marker;
  }
  group->marker_tail = marker;
  return LC_OK;
}

static int
lc_pouch_state_finish_commit_group(lc_pouch_state_commit_group *group,
                                   int owned, int rc, lc_error *error) {
  lc_pouch *pouch;
  int commit_rc;

  pouch = group != NULL ? group->pouch : NULL;
  commit_rc =
      lc_pouch_state_commit_group_end(group, owned, rc == LC_OK ? error : NULL);
  if (commit_rc != LC_OK && rc == LC_OK) {
    return commit_rc;
  }
  if (rc != LC_OK && pouch != NULL) {
    lc_pouch_state_cache_cleanup(pouch);
  }
  return rc;
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

static int lc_pouch_state_process_mutex_identity(const char *root_path,
                                                 const char *namespace_name,
                                                 char **out, lc_error *error) {
  struct stat st;
  char root_identity[128];
  int root_identity_len;
  size_t namespace_len;
  char *identity;

  if (root_path == NULL || namespace_name == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace mutex identity requires root, "
                        "namespace, and output",
                        NULL, NULL, NULL);
  }
  *out = NULL;
  if (stat(root_path, &st) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to stat pouch root for namespace mutex",
                        strerror(errno), NULL, "pouch");
  }
  root_identity_len =
      snprintf(root_identity, sizeof(root_identity), "%lu:%lu",
               (unsigned long)st.st_dev, (unsigned long)st.st_ino);
  if (root_identity_len < 0 ||
      (size_t)root_identity_len >= sizeof(root_identity)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch namespace mutex identity is too large", NULL,
                        NULL, "pouch");
  }
  namespace_len = strlen(namespace_name);
  identity = (char *)lc_alloc_with_allocator(NULL, (size_t)root_identity_len +
                                                       namespace_len + 2U);
  if (identity == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch namespace mutex identity",
                        NULL, NULL, NULL);
  }
  memcpy(identity, root_identity, (size_t)root_identity_len);
  identity[root_identity_len] = '\n';
  memcpy(identity + root_identity_len + 1U, namespace_name, namespace_len + 1U);
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
  rc = lc_pouch_state_process_mutex_identity(pouch->root_path, namespace_name,
                                             &identity, error);
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

typedef struct lc_pouch_state_payload_span {
  char *container_leaf;
  unsigned long record_offset;
  unsigned long payload_offset;
  unsigned long payload_length;
  unsigned long payload_crc;
  int present;
} lc_pouch_state_payload_span;

typedef struct lc_pouch_state_entry {
  char *key;
  char *content_type;
  char *etag;
  lc_pouch_state_payload_span payload_span;
  char *payload_context;
  char *descriptor;
  char *decision;
  unsigned long version;
  unsigned long bytes;
  unsigned long cipher_bytes;
  long updated_at_unix;
  int has_query_hidden;
  int query_hidden;
  int seen;
  int found;
  int control;
} lc_pouch_state_entry;

typedef enum lc_pouch_state_record_type {
  LC_POUCH_STATE_RECORD_STATE_PUT = 1,
  LC_POUCH_STATE_RECORD_STATE_DELETE = 2,
  LC_POUCH_STATE_RECORD_STATE_LINK = 3,
  LC_POUCH_STATE_RECORD_STATE_META = 4,
  LC_POUCH_STATE_RECORD_DECISION = 5,
  LC_POUCH_STATE_RECORD_HIGH_WATER = 6
} lc_pouch_state_record_type;

typedef struct lc_pouch_state_binary_append_item {
  unsigned char record_type;
  const void *key;
  size_t key_len;
  const unsigned char *meta;
  size_t meta_len;
} lc_pouch_state_binary_append_item;

typedef struct lc_pouch_state_body_cache_entry {
  unsigned char *bytes;
  size_t length;
  unsigned long version;
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
  unsigned long version;
  struct lc_pouch_state_decision *next;
} lc_pouch_state_decision;

typedef struct lc_pouch_state_cache_record {
  char *key;
  char *content_type;
  char *etag;
  lc_pouch_state_payload_span payload_span;
  char *payload_context;
  char *descriptor;
  unsigned long version;
  unsigned long bytes;
  unsigned long cipher_bytes;
  long updated_at_unix;
  lc_pouch_state_body_cache_entry *body_cache;
  int has_query_hidden;
  int query_hidden;
  int found;
  struct lc_pouch_state_cache_record *next;
  struct lc_pouch_state_cache_record *bucket_next;
} lc_pouch_state_cache_record;

static lc_pouch_state_cache_namespace *
lc_pouch_state_cache_namespace_find(lc_pouch *pouch, const char *namespace_name,
                                    int create, lc_error *error);
static lc_pouch_state_cache_record *
lc_pouch_state_cache_record_find(lc_pouch_state_cache_namespace *ns,
                                 const char *key);
static int lc_pouch_state_read_many_snapshot_body_from_cache(
    lc_pouch *pouch, lc_pouch_state_cache_namespace *cache,
    lc_pouch_state_cache_record *record, const char *crypto_context,
    const char *payload_span_path, unsigned long payload_offset,
    unsigned long payload_length, const lc_pouch_state_entry *current,
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
    const char *container_leaf, unsigned long record_offset,
    unsigned long payload_offset, unsigned long payload_length,
    unsigned long payload_crc, lc_error *error);
static char *lc_pouch_state_payload_span_path(
    const lc_allocator *allocator, const char *namespace_path,
    const lc_pouch_state_payload_span *span, lc_error *error);
static int lc_pouch_state_source_from_span(
    lc_pouch *pouch, const char *crypto_context, const char *path,
    unsigned long payload_offset, unsigned long payload_length,
    const char *descriptor, lc_source **out, lc_error *error);

typedef struct lc_pouch_state_visit_snapshot {
  char *key;
  char *content_type;
  char *etag;
  char *descriptor;
  unsigned long version;
  unsigned long bytes;
  unsigned long cipher_bytes;
  long updated_at_unix;
  int has_query_hidden;
  int query_hidden;
  int found;
} lc_pouch_state_visit_snapshot;

typedef struct lc_pouch_state_read_many_snapshot {
  char *key;
  char *content_type;
  char *etag;
  char *payload_span_path;
  lc_pouch_state_payload_span payload_span;
  char *descriptor;
  char *crypto_context;
  lc_source *body;
  unsigned long version;
  unsigned long bytes;
  unsigned long cipher_bytes;
  long updated_at_unix;
  int has_query_hidden;
  int query_hidden;
  int found;
} lc_pouch_state_read_many_snapshot;

typedef struct lc_pouch_state_scan_body_snapshot {
  char *key;
  char *content_type;
  char *etag;
  char *payload_span_path;
  lc_pouch_state_payload_span payload_span;
  char *payload_context;
  char *descriptor;
  lc_pouch_state_body_cache_entry *body_cache;
  unsigned long version;
  unsigned long bytes;
  unsigned long cipher_bytes;
  long updated_at_unix;
  int has_query_hidden;
  int query_hidden;
  int found;
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
  if (lc_pouch_state_process_namespace_mutex_lock(
          pouch, namespace_name, &lock->process_mutex, error) != LC_OK) {
    return error != NULL && error->code != LC_OK ? error->code
                                                 : LC_ERR_TRANSPORT;
  }
  if (pouch->single_writer) {
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
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch mutation lock path", NULL,
                        NULL, NULL);
  }
  fd = open(lock_path, O_CREAT | O_RDWR, 0666);
  lc_free_with_allocator(&pouch->allocator, lock_path);
  if (fd < 0) {
    lc_pouch_state_process_namespace_mutex_unlock(&lock->process_mutex);
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
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to lock pouch mutation file", strerror(errno),
                        NULL, NULL);
  }
  lock->fd = fd;
  return LC_OK;
}

static void
lc_pouch_state_namespace_lock_release(lc_pouch_state_namespace_lock *lock) {
  if (lock == NULL) {
    return;
  }
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
  rc = lc_pouch_state_lock_namespace_for_mutation(pouch, namespace_name, &lock,
                                                  error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                         &owns_commit_group, error);
  if (rc != LC_OK) {
    lc_pouch_state_namespace_lock_release(&lock);
    return rc;
  }
  rc = callback(context, error);
  rc = lc_pouch_state_finish_commit_group(commit_group, owns_commit_group, rc,
                                          error);
  lc_pouch_state_namespace_lock_release(&lock);
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
    const char *container_leaf, unsigned long record_offset,
    unsigned long payload_offset, unsigned long payload_length,
    unsigned long payload_crc, lc_error *error) {
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

typedef struct lc_pouch_state_compaction_capture {
  char *manifest_text;
  unsigned long candidate_bytes;
  unsigned long candidate_hash_a;
  unsigned long candidate_hash_b;
} lc_pouch_state_compaction_capture;

struct lc_pouch_state_cache_namespace {
  char *namespace_name;
  unsigned long max_segment_id;
  unsigned long max_version;
  int initialized;
  int decision_recovery_checked;
  lc_pouch_namespace_marker_refresh_state marker_refresh;
  lc_pouch_state_cache_record *records;
  lc_pouch_state_cache_record **record_buckets;
  size_t record_bucket_count;
  size_t record_count;
  size_t body_cache_bytes;
  struct lc_pouch_state_cache_namespace *next;
};

static void lc_pouch_state_entry_cleanup(const lc_allocator *allocator,
                                         lc_pouch_state_entry *entry) {
  if (entry == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, entry->key);
  lc_free_with_allocator(allocator, entry->content_type);
  lc_free_with_allocator(allocator, entry->etag);
  lc_pouch_state_payload_span_cleanup(allocator, &entry->payload_span);
  lc_free_with_allocator(allocator, entry->payload_context);
  lc_free_with_allocator(allocator, entry->descriptor);
  lc_free_with_allocator(allocator, entry->decision);
  memset(entry, 0, sizeof(*entry));
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
                                       lc_pouch_state_cache_namespace *ns,
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

static void lc_pouch_state_cache_namespace_clear_body_cache(
    const lc_allocator *allocator, lc_pouch_state_cache_namespace *ns) {
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
  lc_free_with_allocator(allocator, record->payload_context);
  lc_free_with_allocator(allocator, record->descriptor);
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

static void lc_pouch_state_cache_namespace_clear_records(
    const lc_allocator *allocator, lc_pouch_state_cache_namespace *ns) {
  if (ns == NULL) {
    return;
  }
  lc_pouch_state_cache_records_cleanup(allocator, ns->records);
  lc_free_with_allocator(allocator, ns->record_buckets);
  ns->records = NULL;
  ns->record_buckets = NULL;
  ns->record_bucket_count = 0U;
  ns->record_count = 0U;
  ns->body_cache_bytes = 0U;
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
  if (snapshot->key == NULL ||
      (record->content_type != NULL && snapshot->content_type == NULL) ||
      (record->etag != NULL && snapshot->etag == NULL) ||
      (record->descriptor != NULL && snapshot->descriptor == NULL)) {
    lc_pouch_state_visit_snapshot_cleanup(allocator, snapshot);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state visit entry", NULL,
                        NULL, NULL);
  }
  snapshot->version = record->version;
  snapshot->bytes = record->bytes;
  snapshot->cipher_bytes = record->cipher_bytes;
  snapshot->updated_at_unix = record->updated_at_unix;
  snapshot->has_query_hidden = record->has_query_hidden;
  snapshot->query_hidden = record->query_hidden;
  snapshot->found = record->found;
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
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state payload span is invalid", NULL, NULL,
                        "pouch");
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
  if (snapshot->key == NULL ||
      (record->content_type != NULL && snapshot->content_type == NULL) ||
      (record->etag != NULL && snapshot->etag == NULL) ||
      snapshot->payload_span_path == NULL || !snapshot->payload_span.present ||
      (record->payload_context != NULL && snapshot->payload_context == NULL) ||
      (record->descriptor != NULL && snapshot->descriptor == NULL)) {
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
  ++(*count);
  return LC_OK;
}

static int lc_pouch_state_scan_body_snapshot_open_result(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_state_scan_body_snapshot *snapshot,
    lc_pouch_state_read_result *out, lc_error *error) {
  lc_pouch_state_process_namespace_mutex *process_mutex;
  lc_pouch_state_cache_namespace *cache;
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
      cache =
          lc_pouch_state_cache_namespace_find(pouch, namespace_name, 0, error);
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
  out->version = snapshot->version;
  out->bytes = snapshot->bytes;
  out->cipher_bytes = snapshot->cipher_bytes;
  out->updated_at_unix = snapshot->updated_at_unix;
  out->has_query_hidden = snapshot->has_query_hidden;
  out->query_hidden = snapshot->query_hidden;
  out->found = 1;
  snapshot->content_type = NULL;
  snapshot->etag = NULL;
  snapshot->descriptor = NULL;
  return LC_OK;
}

static int lc_pouch_state_read_many_snapshot_body_from_cache(
    lc_pouch *pouch, lc_pouch_state_cache_namespace *cache,
    lc_pouch_state_cache_record *record, const char *crypto_context,
    const char *payload_span_path, unsigned long payload_offset,
    unsigned long payload_length, const lc_pouch_state_entry *current,
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
      lc_pouch_state_cache_namespace_clear_body_cache(&pouch->allocator, cache);
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
    lc_pouch *pouch, lc_pouch_state_cache_namespace *cache,
    lc_pouch_state_cache_record *record, lc_source *body, unsigned long version,
    unsigned long bytes) {
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
      lc_pouch_state_cache_namespace_clear_body_cache(&pouch->allocator, cache);
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
    lc_pouch_state_cache_namespace *cache, lc_pouch_state_cache_record *record,
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
      (current->descriptor != NULL && snapshot->descriptor == NULL)) {
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
  snapshot->version = current->version;
  snapshot->bytes = current->bytes;
  snapshot->cipher_bytes = current->cipher_bytes;
  snapshot->updated_at_unix = current->updated_at_unix;
  snapshot->has_query_hidden = current->has_query_hidden;
  snapshot->query_hidden = current->query_hidden;
  snapshot->found = 1;
  return LC_OK;
}

void lc_pouch_state_cache_cleanup(lc_pouch *pouch) {
  lc_pouch_state_cache_namespace *ns;

  if (pouch == NULL) {
    return;
  }
  ns = pouch->state_cache_namespaces;
  while (ns != NULL) {
    lc_pouch_state_cache_namespace *next;

    next = ns->next;
    lc_free_with_allocator(&pouch->allocator, ns->namespace_name);
    lc_pouch_state_cache_namespace_clear_records(&pouch->allocator, ns);
    lc_pouch_namespace_marker_refresh_state_cleanup(&pouch->allocator,
                                                    &ns->marker_refresh);
    lc_free_with_allocator(&pouch->allocator, ns);
    ns = next;
  }
  pouch->state_cache_namespaces = NULL;
}

void lc_pouch_state_source_cache_cleanup(lc_pouch *pouch) {
  lc_pouch_source_cache_entry *entry;

  if (pouch == NULL) {
    return;
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

  if (pouch == NULL || path == NULL || out_fd == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch source cache requires pouch, path, and output",
                        NULL, NULL, "pouch");
  }
  *out_fd = -1;
  for (entry = pouch->source_cache_entries; entry != NULL;
       entry = entry->next) {
    if (strcmp(entry->path, path) == 0) {
      entry->last_used = ++pouch->source_cache_tick;
      fd = dup(entry->fd);
      if (fd < 0) {
        return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                            "failed to duplicate pouch source descriptor",
                            strerror(errno), NULL, "pouch");
      }
      *out_fd = fd;
      return LC_OK;
    }
  }
  while (pouch->source_cache_count >= LC_POUCH_STATE_SOURCE_CACHE_MAX_FILES) {
    lc_pouch_state_source_cache_evict_one(pouch);
  }
  entry = (lc_pouch_source_cache_entry *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*entry));
  if (entry == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch source cache entry", NULL,
                        NULL, "pouch");
  }
  entry->path = lc_strdup_with_allocator(&pouch->allocator, path);
  if (entry->path == NULL) {
    lc_free_with_allocator(&pouch->allocator, entry);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch source cache path", NULL, NULL,
                        "pouch");
  }
  entry->fd = open(path, O_RDONLY);
  if (entry->fd < 0) {
    lc_free_with_allocator(&pouch->allocator, entry->path);
    lc_free_with_allocator(&pouch->allocator, entry);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch source cache file",
                        strerror(errno), NULL, "pouch");
  }
  entry->last_used = ++pouch->source_cache_tick;
  entry->next = pouch->source_cache_entries;
  pouch->source_cache_entries = entry;
  ++pouch->source_cache_count;
  fd = dup(entry->fd);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to duplicate pouch source descriptor",
                        strerror(errno), NULL, "pouch");
  }
  *out_fd = fd;
  return LC_OK;
}

static int lc_pouch_state_source_from_span(
    lc_pouch *pouch, const char *crypto_context, const char *path,
    unsigned long payload_offset, unsigned long payload_length,
    const char *descriptor, lc_source **out, lc_error *error) {
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

static lc_pouch_state_cache_namespace *
lc_pouch_state_cache_namespace_find(lc_pouch *pouch, const char *namespace_name,
                                    int create, lc_error *error) {
  lc_pouch_state_cache_namespace *ns;

  for (ns = pouch->state_cache_namespaces; ns != NULL; ns = ns->next) {
    if (strcmp(ns->namespace_name, namespace_name) == 0) {
      return ns;
    }
  }
  if (!create) {
    return NULL;
  }
  ns = (lc_pouch_state_cache_namespace *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*ns));
  if (ns == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch state cache namespace", NULL, NULL,
                 NULL);
    return NULL;
  }
  ns->namespace_name =
      lc_strdup_with_allocator(&pouch->allocator, namespace_name);
  if (ns->namespace_name == NULL) {
    lc_free_with_allocator(&pouch->allocator, ns);
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch state cache namespace name", NULL,
                 NULL, NULL);
    return NULL;
  }
  ns->next = pouch->state_cache_namespaces;
  pouch->state_cache_namespaces = ns;
  return ns;
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
    lc_pouch *pouch, lc_pouch_state_cache_namespace *ns,
    size_t next_record_count, lc_error *error) {
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
    lc_pouch *pouch, lc_pouch_state_cache_namespace *ns,
    size_t next_record_count, lc_error *error) {
  if (ns->record_buckets != NULL &&
      next_record_count * 2U <= ns->record_bucket_count) {
    return LC_OK;
  }
  return lc_pouch_state_cache_record_index_rebuild(pouch, ns, next_record_count,
                                                   error);
}

static lc_pouch_state_cache_record *
lc_pouch_state_cache_record_find(lc_pouch_state_cache_namespace *ns,
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
    lc_pouch *pouch, lc_pouch_state_cache_namespace *ns,
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

static int lc_pouch_state_cache_apply_entry(lc_pouch *pouch,
                                            lc_pouch_state_cache_namespace *ns,
                                            const lc_pouch_state_entry *entry,
                                            lc_error *error) {
  lc_pouch_state_cache_record *record;
  char *key;
  char *content_type;
  char *etag;
  lc_pouch_state_payload_span payload_span;
  char *payload_context;
  char *descriptor;
  int rc;

  memset(&payload_span, 0, sizeof(payload_span));
  if (entry == NULL || !entry->seen || entry->key == NULL) {
    return LC_OK;
  }
  if (entry->control) {
    if (entry->version > ns->max_version) {
      ns->max_version = entry->version;
    }
    return LC_OK;
  }
  record = lc_pouch_state_cache_record_find(ns, entry->key);
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
  payload_context =
      entry->payload_context != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, entry->payload_context)
          : NULL;
  descriptor =
      entry->descriptor != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, entry->descriptor)
          : NULL;
  if ((entry->content_type != NULL && content_type == NULL) ||
      (entry->etag != NULL && etag == NULL) ||
      (entry->payload_span.present && !payload_span.present) ||
      (entry->payload_context != NULL && payload_context == NULL) ||
      (entry->descriptor != NULL && descriptor == NULL)) {
    lc_free_with_allocator(&pouch->allocator, content_type);
    lc_free_with_allocator(&pouch->allocator, etag);
    lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_free_with_allocator(&pouch->allocator, descriptor);
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
       !lc_pouch_state_string_equal(record->payload_context,
                                    entry->payload_context) ||
       !lc_pouch_state_string_equal(record->descriptor, entry->descriptor))) {
    lc_pouch_state_cache_record_body_clear(&pouch->allocator, ns, record);
  }
  lc_free_with_allocator(&pouch->allocator, record->content_type);
  lc_free_with_allocator(&pouch->allocator, record->etag);
  lc_pouch_state_payload_span_cleanup(&pouch->allocator, &record->payload_span);
  lc_free_with_allocator(&pouch->allocator, record->payload_context);
  lc_free_with_allocator(&pouch->allocator, record->descriptor);
  record->key = key;
  record->content_type = content_type;
  record->etag = etag;
  record->payload_span = payload_span;
  memset(&payload_span, 0, sizeof(payload_span));
  record->payload_context = payload_context;
  record->descriptor = descriptor;
  record->version = entry->version;
  record->bytes = entry->bytes;
  record->cipher_bytes = entry->cipher_bytes;
  record->updated_at_unix = entry->updated_at_unix;
  record->has_query_hidden = entry->has_query_hidden;
  record->query_hidden = entry->query_hidden;
  record->found = entry->found;
  if (entry->version > ns->max_version) {
    ns->max_version = entry->version;
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
  out->payload_context =
      record->payload_context != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, record->payload_context)
          : NULL;
  out->descriptor =
      record->descriptor != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, record->descriptor)
          : NULL;
  if (out->key == NULL ||
      (record->content_type != NULL && out->content_type == NULL) ||
      (record->etag != NULL && out->etag == NULL) ||
      (record->payload_span.present && !out->payload_span.present) ||
      (record->payload_context != NULL && out->payload_context == NULL) ||
      (record->descriptor != NULL && out->descriptor == NULL)) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch state cache entry", NULL, NULL,
                        NULL);
  }
  out->version = record->version;
  out->bytes = record->bytes;
  out->cipher_bytes = record->cipher_bytes;
  out->updated_at_unix = record->updated_at_unix;
  out->has_query_hidden = record->has_query_hidden;
  out->query_hidden = record->query_hidden;
  out->seen = 1;
  out->found = record->found;
  return LC_OK;
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

static int
lc_pouch_state_touch_marker(lc_pouch *pouch,
                            const lc_pouch_namespace_manifest *manifest,
                            lc_error *error) {
  return lc_pouch_state_defer_marker(pouch, manifest, error);
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

static int lc_pouch_state_copy_file_span_to_fd_crc(const char *path,
                                                   unsigned long offset,
                                                   unsigned long length, int fd,
                                                   unsigned long *stored_crc,
                                                   lc_error *error) {
  unsigned char buffer[128U * 1024U];
  unsigned long remaining;
  int in_fd;
  int rc;

  if (path == NULL || stored_crc == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch span copy requires path and crc output", NULL,
                        NULL, "pouch");
  }
  if (offset > (unsigned long)LONG_MAX) {
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

    want = remaining < (unsigned long)sizeof(buffer) ? (size_t)remaining
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
    if (rc != LC_OK) {
      break;
    }
    remaining -= (unsigned long)got;
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

static int lc_pouch_state_u64_to_ulong(uint64_t value, unsigned long *out,
                                       const char *message, lc_error *error) {
  if (value > (uint64_t)ULONG_MAX) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL,
                        "pouch");
  }
  *out = (unsigned long)value;
  return LC_OK;
}

static int lc_pouch_state_record_write_prefix(
    int fd, unsigned char type, const void *key, size_t key_len,
    const unsigned char *meta, size_t meta_len, uint64_t payload_len,
    unsigned long payload_crc, unsigned long flags, lc_error *error) {
  lc_pouch_record_header header;
  unsigned char encoded[LC_POUCH_STATE_RECORD_HEADER_BYTES];

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
  if (lc_pouch_state_write_all(fd, encoded, sizeof(encoded), error) != LC_OK) {
    return error != NULL && error->code != LC_OK ? error->code
                                                 : LC_ERR_TRANSPORT;
  }
  if (key_len > 0U &&
      lc_pouch_state_write_all(fd, key, key_len, error) != LC_OK) {
    return error != NULL && error->code != LC_OK ? error->code
                                                 : LC_ERR_TRANSPORT;
  }
  if (meta_len > 0U &&
      lc_pouch_state_write_all(fd, meta, meta_len, error) != LC_OK) {
    return error != NULL && error->code != LC_OK ? error->code
                                                 : LC_ERR_TRANSPORT;
  }
  return LC_OK;
}

static int lc_pouch_state_file_size(const char *path, unsigned long *size,
                                    lc_error *error) {
  struct stat st;

  if (stat(path, &st) != 0) {
    if (errno == ENOENT) {
      *size = 0UL;
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
  *size = (unsigned long)st.st_size;
  return LC_OK;
}

static void lc_pouch_state_truncate_fd_best_effort(int fd, unsigned long size) {
  int ignored;

  ignored = ftruncate(fd, (off_t)size);
  (void)ignored;
}

static void lc_pouch_state_truncate_path_best_effort(const char *path,
                                                     unsigned long size) {
  int ignored;

  if (path == NULL) {
    return;
  }
  ignored = truncate(path, (off_t)size);
  (void)ignored;
}

static int lc_pouch_state_append_binary_records(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_namespace_manifest *manifest,
    const lc_pouch_state_binary_append_item *items, size_t item_count,
    lc_error *error) {
  char *segment_path;
  int fd;
  int rc;
  unsigned long segment_size;
  unsigned long record_size;
  size_t index;

  if (pouch == NULL || namespace_name == NULL || manifest == NULL ||
      items == NULL || item_count == 0U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch binary record batch append requires inputs",
                        NULL, NULL, "pouch");
  }
  record_size = 0UL;
  for (index = 0U; index < item_count; ++index) {
    if ((items[index].key_len > 0U && items[index].key == NULL) ||
        (items[index].meta_len > 0U && items[index].meta == NULL) ||
        items[index].key_len > 0xFFFFFFFFUL ||
        items[index].meta_len > 0xFFFFFFFFUL) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch binary record batch item is invalid", NULL,
                          NULL, "pouch");
    }
    if (record_size > ULONG_MAX - LC_POUCH_STATE_RECORD_HEADER_BYTES -
                          (unsigned long)items[index].key_len -
                          (unsigned long)items[index].meta_len) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch binary record batch exceeds local limit", NULL,
                          NULL, "pouch");
    }
    record_size += LC_POUCH_STATE_RECORD_HEADER_BYTES +
                   (unsigned long)items[index].key_len +
                   (unsigned long)items[index].meta_len;
  }
  segment_path =
      lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                "segments", manifest->active_segment);
  if (segment_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state segment path", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_state_file_size(segment_path, &segment_size, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, segment_path);
    return rc;
  }
  if (segment_size > 0UL &&
      segment_size + record_size > pouch->segment_target_bytes) {
    lc_free_with_allocator(&pouch->allocator, segment_path);
    rc = lc_pouch_namespace_manifest_rotate(
        &pouch->allocator, namespace_name, manifest,
        manifest->active_segment_id + 1UL, error);
    if (rc != LC_OK) {
      return rc;
    }
    segment_path =
        lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                  "segments", manifest->active_segment);
    if (segment_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state segment path", NULL,
                          NULL, NULL);
    }
  }
  fd = open(segment_path, O_WRONLY | O_CREAT | O_APPEND, 0666);
  lc_free_with_allocator(&pouch->allocator, segment_path);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch state segment", strerror(errno),
                        NULL, NULL);
  }
  rc = LC_OK;
  for (index = 0U; rc == LC_OK && index < item_count; ++index) {
    rc = lc_pouch_state_record_write_prefix(
        fd, items[index].record_type, items[index].key, items[index].key_len,
        items[index].meta, items[index].meta_len, 0U, 0UL, 0UL, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_defer_fsync(pouch, fd, error);
  }
  if (close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state segment", strerror(errno),
                      NULL, NULL);
  }
  return rc;
}

static int
lc_pouch_state_append_binary_record(lc_pouch *pouch, const char *namespace_name,
                                    lc_pouch_namespace_manifest *manifest,
                                    unsigned char record_type, const void *key,
                                    size_t key_len, const unsigned char *meta,
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

static int lc_pouch_state_encode_payload_meta(
    const lc_allocator *allocator, unsigned long version, long updated_at_unix,
    unsigned long plain_bytes, unsigned long stored_bytes,
    const char *content_type, const char *etag, const char *descriptor,
    const lc_pouch_state_payload_span *payload_span,
    const char *payload_context, size_t descriptor_reserve,
    int has_query_hidden, int query_hidden, unsigned char **out,
    size_t *out_length, lc_error *error) {
  char ref_container[64];
  unsigned long ref_record_offset;
  unsigned long ref_offset;
  unsigned long ref_length;
  unsigned long ref_crc;
  size_t content_type_len;
  size_t etag_len;
  size_t descriptor_len;
  size_t descriptor_capacity;
  size_t ref_container_len;
  size_t payload_context_len;
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
  ref_record_offset = 0UL;
  ref_offset = 0UL;
  ref_length = 0UL;
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
  if (content_type_len > 0xFFFFU || etag_len > 0xFFFFU ||
      descriptor_capacity > 0xFFFFFFFFUL || ref_container_len > 0xFFFFU ||
      payload_context_len > 0xFFFFU || ref_crc > 0xFFFFFFFFUL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state metadata field exceeds local limit", NULL,
                        NULL, "pouch");
  }
  length = LC_POUCH_STATE_PAYLOAD_META_FIXED_BYTES + content_type_len +
           etag_len + descriptor_capacity + ref_container_len +
           payload_context_len;
  if (lc_pouch_state_meta_new(allocator, length, &meta, error) != LC_OK) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  lc_pouch_state_put64(meta, (uint64_t)version);
  lc_pouch_state_put64(meta + 8, (uint64_t)updated_at_unix);
  lc_pouch_state_put64(meta + 16, (uint64_t)plain_bytes);
  lc_pouch_state_put64(meta + 24, (uint64_t)stored_bytes);
  lc_pouch_state_put64(meta + 32, (uint64_t)ref_record_offset);
  lc_pouch_state_put64(meta + 40, (uint64_t)ref_offset);
  lc_pouch_state_put64(meta + 48, (uint64_t)ref_length);
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
  *out = meta;
  *out_length = length;
  return LC_OK;
}

static int
lc_pouch_state_encode_delete_meta(const lc_allocator *allocator,
                                  unsigned long version, long updated_at_unix,
                                  const char *etag, unsigned char **out,
                                  size_t *out_length, lc_error *error) {
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
  if (lc_pouch_state_meta_new(allocator, 18U + etag_len, &meta, error) !=
      LC_OK) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  lc_pouch_state_put64(meta, (uint64_t)version);
  lc_pouch_state_put64(meta + 8, (uint64_t)updated_at_unix);
  lc_pouch_state_put16(meta + 16, (unsigned long)etag_len);
  memcpy(meta + 18, etag, etag_len);
  *out = meta;
  *out_length = 18U + etag_len;
  return LC_OK;
}

static int
lc_pouch_state_encode_decision_meta(const lc_allocator *allocator,
                                    unsigned long version, const char *etag,
                                    const char *decision, unsigned char **out,
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
  if (lc_pouch_state_meta_new(allocator, 19U + etag_len, &meta, error) !=
      LC_OK) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  lc_pouch_state_put64(meta, (uint64_t)version);
  lc_pouch_state_put64(meta + 8, 0U);
  lc_pouch_state_put16(meta + 16, (unsigned long)etag_len);
  meta[18] = decision_code;
  memcpy(meta + 19, etag, etag_len);
  *out = meta;
  *out_length = 19U + etag_len;
  return LC_OK;
}

static int lc_pouch_state_encode_high_water_meta(const lc_allocator *allocator,
                                                 unsigned long version,
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
    const char *container_leaf, unsigned long record_offset,
    unsigned long payload_offset, unsigned long payload_length,
    unsigned long payload_crc, lc_pouch_state_entry *entry, lc_error *error) {
  unsigned long version;
  unsigned long plain_bytes;
  unsigned long stored_bytes;
  unsigned long ref_record_offset;
  unsigned long ref_offset;
  unsigned long ref_length;
  unsigned long ref_crc;
  unsigned long content_type_len;
  unsigned long etag_len;
  unsigned long descriptor_len;
  unsigned long ref_container_len;
  unsigned long payload_context_len;
  const unsigned char *cursor;
  char *ref_container;
  uint64_t value;
  int rc;

  version = 0UL;
  plain_bytes = 0UL;
  stored_bytes = 0UL;
  ref_record_offset = 0UL;
  ref_offset = 0UL;
  ref_length = 0UL;
  ref_crc = 0UL;
  content_type_len = 0UL;
  etag_len = 0UL;
  descriptor_len = 0UL;
  ref_container_len = 0UL;
  payload_context_len = 0UL;
  ref_container = NULL;
  if (meta_len < LC_POUCH_STATE_PAYLOAD_META_FIXED_BYTES) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state metadata is truncated", NULL, NULL,
                        "pouch");
  }
  value = lc_pouch_state_get64(meta);
  rc = lc_pouch_state_u64_to_ulong(
      value, &version, "pouch state version exceeds local limit", error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_u64_to_ulong(
      lc_pouch_state_get64(meta + 16), &plain_bytes,
      "pouch state plaintext size exceeds local limit", error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_u64_to_ulong(
      lc_pouch_state_get64(meta + 24), &stored_bytes,
      "pouch state stored size exceeds local limit", error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_u64_to_ulong(
      lc_pouch_state_get64(meta + 32), &ref_record_offset,
      "pouch state ref record offset exceeds local limit", error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_u64_to_ulong(lc_pouch_state_get64(meta + 40), &ref_offset,
                                   "pouch state ref offset exceeds local limit",
                                   error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_u64_to_ulong(lc_pouch_state_get64(meta + 48), &ref_length,
                                   "pouch state ref length exceeds local limit",
                                   error);
  if (rc != LC_OK) {
    return rc;
  }
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
  if (ref_container == NULL || ref_container[0] == '\0' ||
      !lc_pouch_state_payload_container_is_valid(ref_container) ||
      ref_length != stored_bytes) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    lc_free_with_allocator(allocator, ref_container);
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state payload ref metadata is invalid", NULL,
                        NULL, "pouch");
  }
  if (record_type == LC_POUCH_STATE_RECORD_STATE_PUT) {
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
  entry->payload_context =
      payload_context_len > 0UL
          ? lc_pouch_state_bytes_to_string(allocator, cursor,
                                           (size_t)payload_context_len, error)
          : NULL;
  if (entry->key == NULL || entry->content_type == NULL ||
      entry->etag == NULL || !entry->payload_span.present ||
      (descriptor_len > 0UL && entry->descriptor == NULL) ||
      (payload_context_len > 0UL && entry->payload_context == NULL)) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  entry->version = version;
  entry->bytes = plain_bytes;
  entry->cipher_bytes = stored_bytes;
  entry->updated_at_unix = (long)lc_pouch_state_get64(meta + 8);
  entry->has_query_hidden = (meta[60] & 1U) != 0;
  entry->query_hidden = (meta[60] & 2U) != 0;
  entry->seen = 1;
  entry->found = 1;
  return LC_OK;
}

static int lc_pouch_state_decode_delete_meta(
    const lc_allocator *allocator, const unsigned char *key, size_t key_len,
    const unsigned char *meta, size_t meta_len, lc_pouch_state_entry *entry,
    lc_error *error) {
  unsigned long version;
  unsigned long etag_len;
  int rc;

  version = 0UL;
  etag_len = 0UL;
  if (meta_len < 18U) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state delete metadata is truncated", NULL, NULL,
                        "pouch");
  }
  rc = lc_pouch_state_u64_to_ulong(
      lc_pouch_state_get64(meta), &version,
      "pouch state delete version exceeds local limit", error);
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
  entry->version = version;
  entry->updated_at_unix = (long)lc_pouch_state_get64(meta + 8);
  entry->seen = 1;
  entry->found = 0;
  return LC_OK;
}

static int lc_pouch_state_decode_decision_meta(
    const lc_allocator *allocator, const unsigned char *key, size_t key_len,
    const unsigned char *meta, size_t meta_len, lc_pouch_state_entry *entry,
    lc_error *error) {
  unsigned long version;
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
  rc = lc_pouch_state_u64_to_ulong(
      lc_pouch_state_get64(meta), &version,
      "pouch state decision version exceeds local limit", error);
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
  entry->version = version;
  entry->seen = 1;
  entry->found = 0;
  entry->control = 1;
  return LC_OK;
}

static int lc_pouch_state_decode_high_water_meta(const lc_allocator *allocator,
                                                 const unsigned char *meta,
                                                 size_t meta_len,
                                                 lc_pouch_state_entry *entry,
                                                 lc_error *error) {
  unsigned long version;
  int rc;

  version = 0UL;
  if (meta_len < 8U) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state high-water metadata is truncated", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_state_u64_to_ulong(
      lc_pouch_state_get64(meta), &version,
      "pouch high-water version exceeds local limit", error);
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
  entry->seen = 1;
  entry->found = 0;
  entry->control = 1;
  return LC_OK;
}

static int lc_pouch_state_decode_record(
    const lc_allocator *allocator, const lc_pouch_record_header *header,
    const unsigned char *key, const unsigned char *meta,
    const char *container_leaf, unsigned long record_offset,
    unsigned long payload_offset, unsigned long payload_length,
    unsigned long payload_crc, lc_pouch_state_entry *entry, lc_error *error) {
  switch (header->type) {
  case LC_POUCH_STATE_RECORD_STATE_PUT:
  case LC_POUCH_STATE_RECORD_STATE_LINK:
  case LC_POUCH_STATE_RECORD_STATE_META:
    return lc_pouch_state_decode_payload_meta(
        allocator, key, header->key_len, meta, header->meta_len, header->type,
        container_leaf, record_offset, payload_offset, payload_length,
        payload_crc, entry, error);
  case LC_POUCH_STATE_RECORD_STATE_DELETE:
    return lc_pouch_state_decode_delete_meta(
        allocator, key, header->key_len, meta, header->meta_len, entry, error);
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
    int allow_truncated_tail, int verify_payload, unsigned long file_size,
    lc_pouch_state_entry *entry, int *found, int *truncated_tail,
    lc_error *error) {
  lc_pouch_record_header header;
  unsigned char encoded[LC_POUCH_STATE_RECORD_HEADER_BYTES];
  unsigned char *key;
  unsigned char *meta;
  unsigned char payload_buffer[16384];
  uint64_t remaining;
  unsigned long payload_offset;
  unsigned long payload_length;
  unsigned long record_start;
  unsigned long crc;
  size_t got;
  long position;
  int rc;

  key = NULL;
  meta = NULL;
  payload_offset = 0UL;
  payload_length = 0UL;
  record_start = 0UL;
  crc = 0UL;
  if (fp == NULL || allocator == NULL || entry == NULL || found == NULL ||
      truncated_tail == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state record read requires inputs", NULL, NULL,
                        "pouch");
  }
  *found = 0;
  *truncated_tail = 0;
  position = ftell(fp);
  if (position < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch state record offset",
                        strerror(errno), NULL, "pouch");
  }
  record_start = (unsigned long)position;
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
  position = ftell(fp);
  if (position < 0) {
    lc_free_with_allocator(allocator, key);
    lc_free_with_allocator(allocator, meta);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch state payload offset",
                        strerror(errno), NULL, "pouch");
  }
  payload_offset = (unsigned long)position;
  rc = lc_pouch_state_u64_to_ulong(
      header.payload_len, &payload_length,
      "pouch state payload length exceeds local limit", error);
  if (rc != LC_OK) {
    lc_free_with_allocator(allocator, key);
    lc_free_with_allocator(allocator, meta);
    return rc;
  }
  if (payload_length > file_size ||
      payload_offset > file_size - payload_length) {
    lc_free_with_allocator(allocator, key);
    lc_free_with_allocator(allocator, meta);
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
  } else if (fseek(fp, (long)(payload_offset + payload_length), SEEK_SET) !=
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

static int lc_pouch_state_scan_file(lc_pouch *pouch, const char *segment_path,
                                    const char *container_leaf,
                                    int allow_truncated_tail, const char *key,
                                    lc_pouch_state_entry *current,
                                    unsigned long *max_version,
                                    lc_error *error) {
  lc_pouch_state_entry entry;
  FILE *fp;
  int found;
  int truncated_tail;
  int repair_tail;
  unsigned long repair_offset;
  unsigned long file_size;
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
  if (fstat(fileno(fp), &st) != 0 || st.st_size < 0 ||
      (uint64_t)st.st_size > (uint64_t)ULONG_MAX) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to stat pouch state segment", strerror(errno),
                      NULL, "pouch");
    (void)fclose(fp);
    return rc;
  }
  file_size = (unsigned long)st.st_size;

  memset(&entry, 0, sizeof(entry));
  rc = LC_OK;
  repair_tail = 0;
  repair_offset = 0UL;
  for (;;) {
    long good_position;

    good_position = ftell(fp);
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
      repair_tail = allow_truncated_tail;
      repair_offset = (unsigned long)good_position;
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
      lc_pouch_state_entry_cleanup(&pouch->allocator, current);
      *current = entry;
      memset(&entry, 0, sizeof(entry));
    }
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &entry);
  if (fclose(fp) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state segment", strerror(errno),
                      NULL, NULL);
  }
  if (rc == LC_OK && repair_tail) {
    lc_pouch_state_truncate_path_best_effort(segment_path, repair_offset);
  }
  return rc;
}

static int lc_pouch_state_skip_file_bytes(FILE *fp, uint64_t length,
                                          lc_error *error) {
  while (length > 0U) {
    long chunk;

    chunk = length > (uint64_t)LONG_MAX ? LONG_MAX : (long)length;
    if (fseek(fp, chunk, SEEK_CUR) != 0) {
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to skip pouch state record bytes",
                          strerror(errno), NULL, "pouch");
    }
    length -= (uint64_t)chunk;
  }
  return LC_OK;
}

static int lc_pouch_state_scan_file_max_version(lc_pouch *pouch,
                                                const char *segment_path,
                                                int allow_truncated_tail,
                                                unsigned long *max_version,
                                                lc_error *error) {
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
    unsigned long version;
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
    case LC_POUCH_STATE_RECORD_STATE_DELETE:
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
    rc = lc_pouch_state_u64_to_ulong(
        lc_pouch_state_get64(version_meta), &version,
        "pouch state sequence version exceeds local limit", error);
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
    unsigned long *max_version, lc_error *error) {
  unsigned long segment_id;
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
  for (segment_id = 1UL; segment_id <= manifest->max_segment_id; ++segment_id) {
    char *segment_leaf;
    char *segment_path;
    int allow_truncated_tail;

    if (segment_id <= manifest->latest_snapshot_segment_id) {
      continue;
    }
    segment_leaf =
        lc_pouch_namespace_segment_leaf(&pouch->allocator, segment_id);
    segment_path = segment_leaf != NULL
                       ? lc_pouch_state_child_path(&pouch->allocator,
                                                   manifest->namespace_path,
                                                   "segments", segment_leaf)
                       : NULL;
    if (segment_path == NULL) {
      lc_free_with_allocator(&pouch->allocator, segment_leaf);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state segment path", NULL,
                          NULL, NULL);
    }
    allow_truncated_tail = manifest->active_segment != NULL &&
                           strcmp(segment_leaf, manifest->active_segment) == 0;
    rc = lc_pouch_state_scan_file_max_version(
        pouch, segment_path, allow_truncated_tail, max_version, error);
    lc_free_with_allocator(&pouch->allocator, segment_leaf);
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
                               unsigned long *max_version, lc_error *error) {
  unsigned long segment_id;
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
                                  manifest->latest_snapshot, 0, key, current,
                                  max_version, error);
    lc_free_with_allocator(&pouch->allocator, snapshot_path);
    if (rc != LC_OK) {
      return rc;
    }
  }
  for (segment_id = 1UL; segment_id <= manifest->max_segment_id; ++segment_id) {
    char *segment_leaf;
    char *segment_path;

    if (segment_id <= manifest->latest_snapshot_segment_id) {
      continue;
    }
    segment_leaf =
        lc_pouch_namespace_segment_leaf(&pouch->allocator, segment_id);
    segment_path = segment_leaf != NULL
                       ? lc_pouch_state_child_path(&pouch->allocator,
                                                   manifest->namespace_path,
                                                   "segments", segment_leaf)
                       : NULL;
    if (segment_path == NULL) {
      lc_free_with_allocator(&pouch->allocator, segment_leaf);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state segment path", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_state_scan_file(
        pouch, segment_path, segment_leaf,
        strcmp(segment_leaf, manifest->active_segment) == 0, key, current,
        max_version, error);
    lc_free_with_allocator(&pouch->allocator, segment_leaf);
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
    if (entry->version < decision->version) {
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
  int repair_tail;
  unsigned long repair_offset;
  unsigned long file_size;
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
  if (fstat(fileno(fp), &st) != 0 || st.st_size < 0 ||
      (uint64_t)st.st_size > (uint64_t)ULONG_MAX) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to stat pouch state segment", strerror(errno),
                      NULL, "pouch");
    (void)fclose(fp);
    return rc;
  }
  file_size = (unsigned long)st.st_size;
  memset(&entry, 0, sizeof(entry));
  rc = LC_OK;
  repair_tail = 0;
  repair_offset = 0UL;
  for (;;) {
    long good_position;

    good_position = ftell(fp);
    if (good_position < 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch state decision offset",
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
      repair_tail = allow_truncated_tail;
      repair_offset = (unsigned long)good_position;
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
  if (rc == LC_OK && repair_tail) {
    lc_pouch_state_truncate_path_best_effort(segment_path, repair_offset);
  }
  return rc;
}

static int lc_pouch_state_collect_decisions(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    lc_pouch_state_decision **decisions, lc_error *error) {
  unsigned long segment_id;
  int rc;

  *decisions = NULL;
  rc = LC_OK;
  for (segment_id = manifest->latest_snapshot_segment_id + 1UL;
       segment_id <= manifest->max_segment_id; ++segment_id) {
    char *segment_leaf;
    char *segment_path;

    segment_leaf =
        lc_pouch_namespace_segment_leaf(&pouch->allocator, segment_id);
    segment_path = segment_leaf != NULL
                       ? lc_pouch_state_child_path(&pouch->allocator,
                                                   manifest->namespace_path,
                                                   "segments", segment_leaf)
                       : NULL;
    if (segment_path == NULL) {
      lc_free_with_allocator(&pouch->allocator, segment_leaf);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state segment path", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_state_collect_decisions_file(
        pouch, segment_path, segment_leaf,
        strcmp(segment_leaf, manifest->active_segment) == 0, decisions, error);
    lc_free_with_allocator(&pouch->allocator, segment_leaf);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    if (rc != LC_OK) {
      break;
    }
  }
  return rc;
}

static int lc_pouch_state_cache_replay_file(
    lc_pouch *pouch, lc_pouch_state_cache_namespace *cache,
    const char *segment_path, const char *container_leaf,
    int allow_truncated_tail, lc_error *error) {
  lc_pouch_state_entry entry;
  FILE *fp;
  int found;
  int truncated_tail;
  int repair_tail;
  unsigned long repair_offset;
  unsigned long file_size;
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
  if (fstat(fileno(fp), &st) != 0 || st.st_size < 0 ||
      (uint64_t)st.st_size > (uint64_t)ULONG_MAX) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to stat pouch state segment", strerror(errno),
                      NULL, "pouch");
    (void)fclose(fp);
    return rc;
  }
  file_size = (unsigned long)st.st_size;
  memset(&entry, 0, sizeof(entry));
  rc = LC_OK;
  repair_tail = 0;
  repair_offset = 0UL;
  for (;;) {
    long good_position;

    good_position = ftell(fp);
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
      repair_tail = allow_truncated_tail;
      repair_offset = (unsigned long)good_position;
      break;
    }
    if (!found) {
      break;
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
  }
  return rc;
}

static int lc_pouch_state_cache_warm_transformed_bodies(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_state_cache_namespace *cache,
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

static int lc_pouch_state_cache_refresh(
    lc_pouch *pouch, lc_pouch_state_cache_namespace *cache,
    const lc_pouch_namespace_manifest *manifest, int force, lc_error *error) {
  unsigned long segment_id;
  int rc;

  if (!force && cache->initialized &&
      cache->max_segment_id == manifest->max_segment_id) {
    return LC_OK;
  }
  lc_pouch_state_cache_namespace_clear_records(&pouch->allocator, cache);
  cache->max_version = 0UL;
  cache->max_segment_id = 0UL;
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
                                          manifest->latest_snapshot, 0, error);
    lc_free_with_allocator(&pouch->allocator, snapshot_path);
    if (rc != LC_OK) {
      return rc;
    }
  }
  for (segment_id = 1UL; segment_id <= manifest->max_segment_id; ++segment_id) {
    char *segment_leaf;
    char *segment_path;

    if (segment_id <= manifest->latest_snapshot_segment_id) {
      continue;
    }
    segment_leaf =
        lc_pouch_namespace_segment_leaf(&pouch->allocator, segment_id);
    segment_path = segment_leaf != NULL
                       ? lc_pouch_state_child_path(&pouch->allocator,
                                                   manifest->namespace_path,
                                                   "segments", segment_leaf)
                       : NULL;
    if (segment_path == NULL) {
      lc_free_with_allocator(&pouch->allocator, segment_leaf);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state segment path", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_state_cache_replay_file(
        pouch, cache, segment_path, segment_leaf,
        strcmp(segment_leaf, manifest->active_segment) == 0, error);
    lc_free_with_allocator(&pouch->allocator, segment_leaf);
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
    cache->max_segment_id = manifest->max_segment_id;
    cache->initialized = 1;
  }
  return rc;
}

int lc_pouch_state_warm_namespace(lc_pouch *pouch, const char *namespace_name,
                                  lc_error *error) {
  lc_pouch_state_cache_namespace *cache;
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
  cache = lc_pouch_state_cache_namespace_find(pouch, namespace_name, 1, error);
  if (cache == NULL) {
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  } else {
    rc = lc_pouch_state_cache_refresh(pouch, cache, &manifest, 0, error);
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
                            unsigned long *max_version_out, lc_error *error) {
  lc_pouch_state_cache_namespace *cache;
  lc_pouch_state_cache_record *record;
  int force_refresh;
  int rc;

  cache = lc_pouch_state_cache_namespace_find(pouch, namespace_name, 1, error);
  if (cache == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  if (pouch->single_writer) {
    force_refresh = 0;
  } else {
    force_refresh = 0;
    rc = lc_pouch_namespace_marker_refresh_should_scan(
        &pouch->allocator, manifest->namespace_path, pouch->writer_marker_leaf,
        &cache->marker_refresh, LC_POUCH_STATE_SHARED_FORCE_AFTER_SKIPS,
        &force_refresh, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  rc = lc_pouch_state_cache_refresh(pouch, cache, manifest, force_refresh,
                                    error);
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
  if (!current->payload_span.present) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state payload span is invalid", NULL, NULL,
                        "pouch");
  }
  rc = LC_OK;
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
    out->version = current->version;
    out->bytes = current->bytes;
    out->cipher_bytes = current->cipher_bytes;
    out->updated_at_unix = current->updated_at_unix;
    out->has_query_hidden = current->has_query_hidden;
    out->query_hidden = current->query_hidden;
    out->found = 1;
    current->content_type = NULL;
    current->etag = NULL;
    current->descriptor = NULL;
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
  if ((record->content_type != NULL && out->content_type == NULL) ||
      (record->etag != NULL && out->etag == NULL) ||
      (record->descriptor != NULL && out->descriptor == NULL)) {
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
  unsigned long cipher_bytes;
  unsigned long stored_crc;
  unsigned long record_start;
  unsigned long payload_offset;
  size_t meta_len;
  size_t final_meta_len;
  int rc;

  (void)namespace_name;
  memset(&snapshot_span, 0, sizeof(snapshot_span));
  old_path = NULL;
  meta = NULL;
  final_meta = NULL;
  record_start = 0UL;
  if (record->found) {
    record_start = (unsigned long)lseek(fd, 0, SEEK_END);
    if (!record->payload_span.present) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch snapshot record payload span is invalid", NULL,
                          NULL, "pouch");
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
        record->payload_context, LC_POUCH_STATE_RECORD_DESCRIPTOR_RESERVE,
        record->has_query_hidden, record->query_hidden, &meta, &meta_len,
        error);
    if (rc != LC_OK) {
      return rc;
    }
    payload_offset = record_start + LC_POUCH_STATE_RECORD_HEADER_BYTES +
                     (unsigned long)strlen(record->key) +
                     (unsigned long)meta_len;
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
        fd, LC_POUCH_STATE_RECORD_STATE_PUT, record->key, strlen(record->key),
        meta, meta_len, 0U, 0UL, LC_POUCH_RECORD_FLAG_PENDING, error);
    if (rc == LC_OK) {
      rc = lc_pouch_state_copy_file_span_to_fd_crc(
          old_path, record->payload_span.payload_offset,
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
          LC_POUCH_STATE_RECORD_DESCRIPTOR_RESERVE, record->has_query_hidden,
          record->query_hidden, &final_meta, &final_meta_len, error);
      if (rc == LC_OK && final_meta_len != meta_len) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch snapshot metadata reservation changed size",
                          NULL, NULL, "pouch");
      }
    }
    if (rc == LC_OK && lseek(fd, (off_t)record_start, SEEK_SET) < 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to rewrite pouch snapshot record header",
                        strerror(errno), NULL, "pouch");
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_record_write_prefix(
          fd, LC_POUCH_STATE_RECORD_STATE_PUT, record->key, strlen(record->key),
          final_meta, final_meta_len, (uint64_t)cipher_bytes, stored_crc, 0UL,
          error);
    }
    if (rc == LC_OK && lseek(fd, 0, SEEK_END) < 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to restore pouch snapshot append offset",
                        strerror(errno), NULL, "pouch");
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
      rc = lc_pouch_state_record_write_prefix(
          fd, LC_POUCH_STATE_RECORD_STATE_DELETE, record->key,
          strlen(record->key), meta, meta_len, 0U, 0UL, 0UL, error);
    }
  }
  lc_free_with_allocator(&pouch->allocator, meta);
  lc_free_with_allocator(&pouch->allocator, final_meta);
  lc_free_with_allocator(&pouch->allocator, old_path);
  lc_pouch_state_payload_span_cleanup(&pouch->allocator, &snapshot_span);
  return rc;
}

static int lc_pouch_state_snapshot_write_high_water(
    int fd, unsigned long high_water_version, lc_error *error) {
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
    const char *snapshot_leaf, lc_pouch_state_cache_namespace *cache,
    lc_error *error) {
  lc_pouch_state_cache_record *record;
  char tmp_leaf[128];
  char *snapshot_path;
  char *tmp_path;
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
  rc = lc_pouch_state_snapshot_write_high_water(fd, cache->max_version, error);
  for (record = cache->records; record != NULL; record = record->next) {
    if (rc != LC_OK) {
      break;
    }
    rc = lc_pouch_state_snapshot_write_record(pouch, cache->namespace_name,
                                              manifest, snapshot_leaf, fd,
                                              record, error);
  }
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

static void lc_pouch_state_compaction_hash_byte(unsigned long *hash_a,
                                                unsigned long *hash_b,
                                                unsigned char byte) {
  *hash_a ^= (unsigned long)byte;
  *hash_a *= 16777619UL;
  *hash_b = (*hash_b * 65599UL) + (unsigned long)byte + 1UL;
}

static void lc_pouch_state_compaction_hash_string(unsigned long *hash_a,
                                                  unsigned long *hash_b,
                                                  const char *value) {
  const unsigned char *cursor;

  cursor = (const unsigned char *)value;
  while (*cursor != '\0') {
    lc_pouch_state_compaction_hash_byte(hash_a, hash_b, *cursor);
    ++cursor;
  }
  lc_pouch_state_compaction_hash_byte(hash_a, hash_b, 0U);
}

static int lc_pouch_state_compaction_hash_segment(const char *segment_path,
                                                  unsigned long *bytes,
                                                  unsigned long *hash_a,
                                                  unsigned long *hash_b,
                                                  lc_error *error) {
  unsigned char buffer[4096];
  FILE *fp;
  int rc;

  fp = fopen(segment_path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      lc_pouch_state_compaction_hash_byte(hash_a, hash_b, 0U);
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch state segment", strerror(errno),
                        segment_path, NULL);
  }
  lc_pouch_state_compaction_hash_byte(hash_a, hash_b, 1U);
  rc = LC_OK;
  while (!feof(fp)) {
    size_t got;
    size_t index;

    got = fread(buffer, 1U, sizeof(buffer), fp);
    if (got != 0U) {
      *bytes += (unsigned long)got;
      for (index = 0U; index < got; ++index) {
        lc_pouch_state_compaction_hash_byte(hash_a, hash_b, buffer[index]);
      }
    }
    if (ferror(fp)) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch state segment", strerror(errno),
                        segment_path, NULL);
      break;
    }
  }
  if (fclose(fp) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state segment", strerror(errno),
                      segment_path, NULL);
  }
  return rc;
}

static int lc_pouch_state_compaction_candidate_fingerprint(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    unsigned long *bytes, unsigned long *hash_a, unsigned long *hash_b,
    lc_error *error) {
  unsigned long segment_id;

  *bytes = 0UL;
  *hash_a = 2166136261UL;
  *hash_b = 0UL;
  for (segment_id = manifest->latest_snapshot_segment_id + 1UL;
       segment_id <= manifest->max_segment_id; ++segment_id) {
    char *segment_leaf;
    char *segment_path;
    int rc;

    segment_leaf =
        lc_pouch_namespace_segment_leaf(&pouch->allocator, segment_id);
    segment_path = segment_leaf != NULL
                       ? lc_pouch_state_child_path(&pouch->allocator,
                                                   manifest->namespace_path,
                                                   "segments", segment_leaf)
                       : NULL;
    if (segment_leaf == NULL || segment_path == NULL) {
      lc_free_with_allocator(&pouch->allocator, segment_leaf);
      lc_free_with_allocator(&pouch->allocator, segment_path);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state segment path", NULL,
                          NULL, NULL);
    }
    lc_pouch_state_compaction_hash_string(hash_a, hash_b, segment_leaf);
    lc_free_with_allocator(&pouch->allocator, segment_leaf);
    rc = lc_pouch_state_compaction_hash_segment(segment_path, bytes, hash_a,
                                                hash_b, error);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    if (rc != LC_OK) {
      return rc;
    }
  }
  return LC_OK;
}

static int lc_pouch_state_compaction_candidate_bytes(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    unsigned long *bytes, lc_error *error) {
  unsigned long segment_id;

  *bytes = 0UL;
  for (segment_id = manifest->latest_snapshot_segment_id + 1UL;
       segment_id <= manifest->max_segment_id; ++segment_id) {
    char *segment_leaf;
    char *segment_path;
    unsigned long size;
    int rc;

    segment_leaf =
        lc_pouch_namespace_segment_leaf(&pouch->allocator, segment_id);
    segment_path = segment_leaf != NULL
                       ? lc_pouch_state_child_path(&pouch->allocator,
                                                   manifest->namespace_path,
                                                   "segments", segment_leaf)
                       : NULL;
    lc_free_with_allocator(&pouch->allocator, segment_leaf);
    if (segment_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state segment path", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_state_file_size(segment_path, &size, error);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    if (rc != LC_OK) {
      return rc;
    }
    *bytes += size;
  }
  return LC_OK;
}

static void lc_pouch_state_compaction_capture_cleanup(
    lc_pouch *pouch, lc_pouch_state_compaction_capture *capture) {
  if (capture == NULL) {
    return;
  }
  lc_free_with_allocator(&pouch->allocator, capture->manifest_text);
  memset(capture, 0, sizeof(*capture));
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
  rc = lc_pouch_state_compaction_candidate_fingerprint(
      pouch, manifest, &capture->candidate_bytes, &capture->candidate_hash_a,
      &capture->candidate_hash_b, error);
  if (rc != LC_OK) {
    lc_pouch_state_compaction_capture_cleanup(pouch, capture);
  }
  return rc;
}

static int lc_pouch_state_compaction_validate_capture(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    const lc_pouch_state_compaction_capture *before, lc_error *error) {
  lc_pouch_state_compaction_capture after;
  int matched;
  int rc;

  memset(&after, 0, sizeof(after));
  rc = lc_pouch_state_compaction_capture_now(pouch, manifest, &after, error);
  if (rc != LC_OK) {
    return rc;
  }
  matched = before->candidate_bytes == after.candidate_bytes &&
            before->candidate_hash_a == after.candidate_hash_a &&
            before->candidate_hash_b == after.candidate_hash_b &&
            before->manifest_text != NULL && after.manifest_text != NULL &&
            strcmp(before->manifest_text, after.manifest_text) == 0;
  lc_pouch_state_compaction_capture_cleanup(pouch, &after);
  if (!matched) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch compaction validation drift", NULL, NULL,
                        "pouch");
  }
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

static unsigned long lc_pouch_maintenance_now_seconds(void) {
  time_t now;

  now = time(NULL);
  return now > 0 ? (unsigned long)now : 0UL;
}

typedef struct lc_pouch_retention_key {
  char *key;
  unsigned long version;
} lc_pouch_retention_key;

typedef struct lc_pouch_retention_scan {
  lc_pouch *pouch;
  long cutoff_unix;
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
                                       const char *key, unsigned long version,
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
                                          long cutoff_unix,
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
    lc_error *error);

static int lc_pouch_state_queue_compacted_files(
    lc_pouch *pouch, lc_pouch_namespace_manifest *manifest,
    const char *namespace_name, unsigned long compacted_segment_id,
    const char *old_snapshot, const lc_pouch_state_cache_namespace *live_cache,
    unsigned long *cleanup_deleted_count, unsigned long *cleanup_pending_count,
    lc_error *error);

static int lc_pouch_state_payload_container_is_valid(const char *leaf) {
  size_t len;
  size_t i;

  if (leaf == NULL) {
    return 0;
  }
  len = strlen(leaf);
  if (len == 28U && strncmp(leaf, "seg-", 4U) == 0 &&
      strcmp(leaf + 24U, ".log") == 0) {
    for (i = 4U; i < 24U; ++i) {
      if (leaf[i] < '0' || leaf[i] > '9') {
        return 0;
      }
    }
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
    const char *namespace_name, unsigned long compacted_segment_id,
    const char *old_snapshot, const lc_pouch_state_cache_namespace *live_cache,
    unsigned long *cleanup_deleted_count, unsigned long *cleanup_pending_count,
    lc_error *error) {
  unsigned long segment_id;
  int rc;

  (void)live_cache;
  for (segment_id = 1UL; segment_id <= compacted_segment_id; ++segment_id) {
    char *segment_leaf;

    segment_leaf =
        lc_pouch_namespace_segment_leaf(&pouch->allocator, segment_id);
    if (segment_leaf == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch obsolete segment name",
                          NULL, NULL, NULL);
    }
    rc = lc_pouch_namespace_manifest_mark_obsolete_segment(
        &pouch->allocator, manifest, segment_leaf, error);
    lc_free_with_allocator(&pouch->allocator, segment_leaf);
    if (rc != LC_OK) {
      return rc;
    }
  }
  if (old_snapshot != NULL &&
      (manifest->latest_snapshot == NULL ||
       strcmp(old_snapshot, manifest->latest_snapshot) != 0)) {
    rc = lc_pouch_namespace_manifest_mark_obsolete_snapshot(
        &pouch->allocator, manifest, old_snapshot, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  rc = lc_pouch_namespace_manifest_save(&pouch->allocator, namespace_name,
                                        manifest, error);
  if (rc != LC_OK) {
    return rc;
  }
  lc_pouch_state_source_cache_cleanup(pouch);
  return lc_pouch_namespace_manifest_cleanup_obsolete(
      &pouch->allocator, namespace_name, manifest, cleanup_deleted_count,
      cleanup_pending_count, error);
}

static int lc_pouch_state_compact_namespace_if_needed(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_namespace_manifest *manifest, int force,
    lc_pouch_maintenance_result *out, lc_error *error) {
  unsigned long candidate_count;
  unsigned long candidate_bytes;
  unsigned long cleanup_deleted_count;
  unsigned long cleanup_pending_count;
  unsigned long compacted_segment_id;
  unsigned long now_seconds;
  int rc;

  if (out != NULL) {
    memset(out, 0, sizeof(*out));
    rc = lc_pouch_maintenance_set_string(pouch, &out->namespace_name,
                                         namespace_name, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  if (!force && !pouch->background_compaction_enabled) {
    if (out != NULL) {
      out->skipped = 1;
    }
    return lc_pouch_maintenance_set_diagnostic(pouch, out, "disabled", error);
  }
  candidate_count =
      manifest->max_segment_id > manifest->latest_snapshot_segment_id
          ? manifest->max_segment_id - manifest->latest_snapshot_segment_id
          : 0UL;
  if (out != NULL) {
    out->candidate_segment_count = candidate_count;
  }
  if (candidate_count == 0UL) {
    if (out != NULL) {
      out->skipped = 1;
    }
    return lc_pouch_maintenance_set_diagnostic(pouch, out, "no-candidates",
                                               error);
  }
  rc = lc_pouch_state_compaction_candidate_bytes(pouch, manifest,
                                                 &candidate_bytes, error);
  if (rc != LC_OK) {
    lc_pouch_maintenance_mark_aborted(pouch, out, "candidate-read-aborted");
    return rc;
  }
  if (out != NULL) {
    out->candidate_bytes = candidate_bytes;
  }
  if (!force && candidate_count < pouch->compaction_min_segment_count) {
    if (out != NULL) {
      out->skipped = 1;
    }
    return lc_pouch_maintenance_set_diagnostic(
        pouch, out, "below-segment-threshold", error);
  }
  if (!force && candidate_bytes < pouch->compaction_min_reclaimable_bytes) {
    if (out != NULL) {
      out->skipped = 1;
    }
    return lc_pouch_maintenance_set_diagnostic(
        pouch, out, "below-reclaimable-threshold", error);
  }
  now_seconds = lc_pouch_maintenance_now_seconds();
  if (!force && pouch->compaction_interval_seconds != 0UL &&
      pouch->last_compaction_check_seconds != 0UL &&
      now_seconds >= pouch->last_compaction_check_seconds &&
      now_seconds - pouch->last_compaction_check_seconds <
          pouch->compaction_interval_seconds) {
    if (out != NULL) {
      out->skipped = 1;
    }
    return lc_pouch_maintenance_set_diagnostic(pouch, out,
                                               "interval-not-elapsed", error);
  }
  compacted_segment_id = manifest->max_segment_id;
  cleanup_deleted_count = 0UL;
  cleanup_pending_count = 0UL;
  if (!force && now_seconds != 0UL) {
    pouch->last_compaction_check_seconds = now_seconds;
  }
  {
    const char *abort_diagnostic;

    abort_diagnostic = NULL;
    rc = lc_pouch_state_compact_namespace(
        pouch, namespace_name, manifest, &cleanup_deleted_count,
        &cleanup_pending_count, &abort_diagnostic, error);
    if (rc != LC_OK) {
      lc_pouch_maintenance_mark_aborted(pouch, out, abort_diagnostic);
      return rc;
    }
  }
  if (out != NULL) {
    out->compacted = 1;
    out->compacted_segment_id = compacted_segment_id;
    out->cleanup_deleted_count = cleanup_deleted_count;
    out->cleanup_pending_count = cleanup_pending_count;
  }
  return lc_pouch_maintenance_set_diagnostic(pouch, out, "compacted", error);
}

static int lc_pouch_state_compact_namespace(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_namespace_manifest *manifest, unsigned long *cleanup_deleted_count,
    unsigned long *cleanup_pending_count, const char **abort_diagnostic,
    lc_error *error) {
  lc_pouch_state_cache_namespace snapshot_cache;
  lc_pouch_state_compaction_capture capture;
  char *snapshot_leaf;
  char *old_snapshot;
  unsigned long compacted_segment_id;
  int snapshot_installed;
  int rc;

  snapshot_installed = 0;
  if (abort_diagnostic != NULL) {
    *abort_diagnostic = NULL;
  }
  if (manifest->max_segment_id <= manifest->latest_snapshot_segment_id) {
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
    lc_pouch_state_cache_namespace_clear_records(&pouch->allocator,
                                                 &snapshot_cache);
    return rc;
  }
  compacted_segment_id = manifest->max_segment_id;
  snapshot_leaf =
      lc_pouch_namespace_snapshot_leaf(&pouch->allocator, compacted_segment_id);
  old_snapshot = manifest->latest_snapshot != NULL
                     ? lc_strdup_with_allocator(&pouch->allocator,
                                                manifest->latest_snapshot)
                     : NULL;
  if (snapshot_leaf == NULL ||
      (manifest->latest_snapshot != NULL && old_snapshot == NULL)) {
    lc_free_with_allocator(&pouch->allocator, snapshot_leaf);
    lc_free_with_allocator(&pouch->allocator, old_snapshot);
    lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
    lc_free_with_allocator(&pouch->allocator, snapshot_cache.namespace_name);
    lc_pouch_state_cache_namespace_clear_records(&pouch->allocator,
                                                 &snapshot_cache);
    if (abort_diagnostic != NULL) {
      *abort_diagnostic = "snapshot-prepare-aborted";
    }
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state snapshot name", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_state_write_snapshot(pouch, manifest, snapshot_leaf,
                                     &snapshot_cache, error);
  if (rc == LC_OK) {
#ifdef LOCKDC_TEST_BUILD
    if (lc_pouch_test_after_snapshot_write_hook != NULL) {
      rc = lc_pouch_test_after_snapshot_write_hook(
          lc_pouch_test_after_snapshot_write_context, error);
    }
#endif
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_compaction_validate_capture(pouch, manifest, &capture,
                                                    error);
    if (rc == LC_OK) {
      rc = lc_pouch_namespace_manifest_install_snapshot(
          &pouch->allocator, namespace_name, manifest, snapshot_leaf,
          compacted_segment_id, compacted_segment_id + 1UL, error);
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
    rc = lc_pouch_state_queue_compacted_files(
        pouch, manifest, namespace_name, compacted_segment_id, old_snapshot,
        &snapshot_cache, cleanup_deleted_count, cleanup_pending_count, error);
    if (rc != LC_OK && abort_diagnostic != NULL) {
      *abort_diagnostic = "obsolete-cleanup-aborted";
    }
  }
  if (rc == LC_OK) {
    lc_pouch_state_cache_namespace *live_cache;

    live_cache =
        lc_pouch_state_cache_namespace_find(pouch, namespace_name, 0, NULL);
    if (live_cache != NULL) {
      lc_pouch_state_cache_namespace_clear_records(&pouch->allocator,
                                                   live_cache);
      live_cache->initialized = 0;
      live_cache->max_segment_id = 0UL;
      live_cache->max_version = 0UL;
    }
  }
  if (rc == LC_OK) {
    (void)lc_pouch_state_touch_marker(pouch, manifest, error);
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
  lc_free_with_allocator(&pouch->allocator, old_snapshot);
  lc_pouch_state_compaction_capture_cleanup(pouch, &capture);
  lc_free_with_allocator(&pouch->allocator, snapshot_cache.namespace_name);
  snapshot_cache.namespace_name = NULL;
  lc_pouch_state_cache_namespace_clear_records(&pouch->allocator,
                                               &snapshot_cache);
  return rc;
}

static void
lc_pouch_state_maybe_compact(lc_pouch *pouch, const char *namespace_name,
                             lc_pouch_namespace_manifest *manifest) {
  lc_error ignored;
  int rc;

  lc_error_init(&ignored);
  rc = lc_pouch_state_compact_namespace_if_needed(pouch, namespace_name,
                                                  manifest, 0, NULL, &ignored);
  (void)rc;
  lc_error_cleanup(&ignored);
}

static int lc_pouch_maintenance_run_locked(
    lc_pouch *pouch, const lc_pouch_maintenance_options *options,
    lc_pouch_maintenance_result *out, lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  const char *namespace_name;
  unsigned long cleanup_deleted_count;
  unsigned long cleanup_pending_count;
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
  rc = lc_pouch_state_lock_namespace_for_mutation(pouch, namespace_name, &lock,
                                                  error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                         &owns_commit_group, error);
  if (rc != LC_OK) {
    lc_pouch_state_namespace_lock_release(&lock);
    return rc;
  }
  rc = lc_pouch_maintenance_run_locked(pouch, options, out, error);
  rc = lc_pouch_state_finish_commit_group(commit_group, owns_commit_group, rc,
                                          error);
  lc_pouch_state_namespace_lock_release(&lock);
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
    const char *payload_context, unsigned long version, unsigned long bytes,
    unsigned long cipher_bytes, const char *descriptor, long updated_at_unix,
    int has_query_hidden, int query_hidden, int found) {
  lc_pouch_state_cache_namespace *cache;
  lc_pouch_state_entry entry;
  lc_error ignored;
  int rc;

  cache = lc_pouch_state_cache_namespace_find(pouch, namespace_name, 0, NULL);
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
  entry.version = version;
  entry.bytes = bytes;
  entry.cipher_bytes = cipher_bytes;
  entry.updated_at_unix = updated_at_unix;
  entry.has_query_hidden = has_query_hidden;
  entry.query_hidden = query_hidden;
  entry.seen = 1;
  entry.found = found;
  lc_error_init(&ignored);
  rc = lc_pouch_state_cache_apply_entry(pouch, cache, &entry, &ignored);
  lc_error_cleanup(&ignored);
  if (rc != LC_OK) {
    lc_pouch_state_cache_namespace_clear_records(&pouch->allocator, cache);
    cache->initialized = 0;
    cache->max_segment_id = 0UL;
    cache->max_version = 0UL;
    return LC_OK;
  }
  if (rc == LC_OK && manifest->max_segment_id > cache->max_segment_id) {
    cache->max_segment_id = manifest->max_segment_id;
  }
  return rc;
}

static int lc_pouch_state_append_tombstone(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_namespace_manifest *manifest, const char *key, const char *etag,
    unsigned long version, long updated_at_unix, lc_error *error) {
  unsigned char *meta;
  size_t meta_len;
  int rc;

  meta = NULL;
  meta_len = 0U;
  rc = lc_pouch_state_encode_delete_meta(&pouch->allocator, version,
                                         updated_at_unix, etag, &meta,
                                         &meta_len, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_append_binary_record(
      pouch, namespace_name, manifest, LC_POUCH_STATE_RECORD_STATE_DELETE, key,
      strlen(key), meta, meta_len, error);
  lc_free_with_allocator(&pouch->allocator, meta);
  return rc;
}

static int lc_pouch_state_append_record(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_namespace_manifest *manifest, unsigned char record_type,
    const char *key, const char *content_type, const char *etag,
    const lc_pouch_state_payload_span *payload_span,
    const char *payload_context, unsigned long version, unsigned long bytes,
    unsigned long cipher_bytes, const char *descriptor, long updated_at_unix,
    int has_query_hidden, int query_hidden, lc_error *error) {
  unsigned char *meta;
  size_t meta_len;
  int rc;

  if (record_type != LC_POUCH_STATE_RECORD_STATE_PUT &&
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
      content_type, etag, descriptor, payload_span, payload_context, 0U,
      has_query_hidden, query_hidden, &meta, &meta_len, error);
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
    unsigned long version, unsigned long decision_version,
    unsigned long discard_version, long updated_at_unix, lc_error *error) {
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
      staged->descriptor, &staged->payload_span, staged->payload_context, 0U,
      staged->has_query_hidden, staged->query_hidden, &link_meta,
      &link_meta_len, error);
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
    const char *etag, unsigned long decision_version,
    unsigned long tombstone_version, long updated_at_unix, lc_error *error) {
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
  lc_pouch_state_cache_namespace *cache;
  lc_pouch_state_decision *decisions;
  lc_pouch_state_decision *decision;
  int recovered;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_recover_staged_decisions requires "
                        "pouch and namespace",
                        NULL, NULL, NULL);
  }
  cache = lc_pouch_state_cache_namespace_find(pouch, namespace_name, 1, error);
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
  recovered = 0;
  rc = lc_pouch_state_collect_decisions(pouch, &manifest, &decisions, error);
  for (decision = decisions; rc == LC_OK && decision != NULL;
       decision = decision->next) {
    lc_pouch_state_entry staged;
    unsigned long max_version;
    unsigned long tombstone_version;
    long updated_at_unix;

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
    tombstone_version = max_version + 1UL;
    updated_at_unix = (long)lc_pouch_maintenance_now_seconds();
    rc = lc_pouch_state_append_tombstone(
        pouch, namespace_name, &manifest, decision->staged_key, decision->etag,
        tombstone_version, updated_at_unix, error);
    if (rc == LC_OK) {
      (void)lc_pouch_state_cache_apply_write(
          pouch, namespace_name, &manifest, decision->staged_key, NULL,
          decision->etag, NULL, NULL, tombstone_version, 0UL, 0UL, NULL,
          updated_at_unix, 0, 0, 0);
      recovered = 1;
    }
    lc_pouch_state_entry_cleanup(&pouch->allocator, &staged);
  }
  if (rc == LC_OK && recovered) {
    rc = lc_pouch_state_touch_marker(pouch, &manifest, error);
  }
  if (rc == LC_OK && recovered) {
    lc_pouch_state_maybe_compact(pouch, namespace_name, &manifest);
  }
  if (rc == LC_OK) {
    cache->decision_recovery_checked = 1;
  }
  lc_pouch_state_decisions_cleanup(&pouch->allocator, decisions);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

int lc_pouch_state_recover_staged_decisions(lc_pouch *pouch,
                                            const char *namespace_name,
                                            lc_error *error) {
  lc_pouch_state_namespace_lock lock;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0') {
    return lc_pouch_state_recover_staged_decisions_locked(pouch, namespace_name,
                                                          error);
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

static int
lc_pouch_state_write_locked(lc_pouch *pouch, const char *namespace_name,
                            const char *key, lc_source *body,
                            const lc_pouch_state_write_options *options,
                            lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_state_entry current;
  lc_pouch_namespace_manifest manifest;
  lc_pouch_state_hash_source hash_source;
  const char *content_type;
  EVP_MD_CTX *hash_ctx;
  char *segment_path;
  char *descriptor;
  char *crypto_context;
  char *payload_context;
  lc_pouch_state_payload_span payload_span;
  char *etag;
  unsigned char *meta;
  unsigned char *final_meta;
  const char placeholder_etag[] =
      "0000000000000000000000000000000000000000000000000000000000000000";
  int fd;
  unsigned long max_version;
  unsigned long version;
  unsigned long bytes;
  unsigned long cipher_bytes;
  unsigned long stored_crc;
  unsigned long segment_size;
  unsigned long payload_offset;
  size_t meta_len;
  size_t final_meta_len;
  long updated_at_unix;
  int has_query_hidden;
  int query_hidden;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || body == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_write requires pouch, namespace, key, "
                        "body and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  rc = lc_pouch_state_ensure_namespace_locked(pouch, namespace_name, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&manifest, 0, sizeof(manifest));
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                        namespace_name, &manifest, NULL, NULL,
                                        error);
  if (rc != LC_OK) {
    return rc;
  }

  memset(&current, 0, sizeof(current));
  rc = lc_pouch_state_cache_lookup(pouch, namespace_name, &manifest, key,
                                   &current, &max_version, error);
  if (rc != LC_OK) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
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
      if (current.found) {
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
  if (options != NULL && options->create_if_absent && current.found) {
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
  version = max_version + 1UL;
  updated_at_unix = (long)lc_pouch_maintenance_now_seconds();
  etag = NULL;
  payload_context = NULL;
  memset(&payload_span, 0, sizeof(payload_span));
  meta = NULL;
  final_meta = NULL;
  content_type = options != NULL && options->content_type != NULL
                     ? options->content_type
                     : "application/octet-stream";
  has_query_hidden = current.has_query_hidden;
  query_hidden = current.query_hidden;
  if (options != NULL && options->has_query_hidden) {
    has_query_hidden = 1;
    query_hidden = options->query_hidden;
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
  segment_path =
      lc_pouch_state_child_path(&pouch->allocator, manifest.namespace_path,
                                "segments", manifest.active_segment);
  if (segment_path == NULL) {
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state segment path", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_state_file_size(segment_path, &segment_size, error);
  if (rc == LC_OK && segment_size > 0UL &&
      segment_size + LC_POUCH_STATE_RECORD_HEADER_BYTES + strlen(key) >
          pouch->segment_target_bytes) {
    lc_free_with_allocator(&pouch->allocator, segment_path);
    rc = lc_pouch_namespace_manifest_rotate(
        &pouch->allocator, namespace_name, &manifest,
        manifest.active_segment_id + 1UL, error);
    if (rc == LC_OK) {
      segment_path =
          lc_pouch_state_child_path(&pouch->allocator, manifest.namespace_path,
                                    "segments", manifest.active_segment);
      if (segment_path == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state segment path", NULL,
                          NULL, NULL);
      }
    }
  }
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  fd = open(segment_path, O_RDWR | O_CREAT, 0666);
  if (fd < 0) {
    lc_free_with_allocator(&pouch->allocator, segment_path);
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch state segment", strerror(errno),
                        NULL, NULL);
  }
  segment_size = (unsigned long)lseek(fd, 0, SEEK_END);
  rc = lc_pouch_state_payload_span_set(&pouch->allocator, &payload_span,
                                       manifest.active_segment, segment_size,
                                       0UL, 0UL, 0UL, error);
  if (rc != LC_OK) {
    close(fd);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  rc = lc_pouch_state_encode_payload_meta(
      &pouch->allocator, version, updated_at_unix, 0UL, 0UL, content_type,
      placeholder_etag, NULL, &payload_span, payload_context,
      LC_POUCH_STATE_RECORD_DESCRIPTOR_RESERVE, has_query_hidden, query_hidden,
      &meta, &meta_len, error);
  if (rc != LC_OK) {
    close(fd);
    lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  payload_offset = segment_size + LC_POUCH_STATE_RECORD_HEADER_BYTES +
                   (unsigned long)strlen(key) + (unsigned long)meta_len;
  rc = lc_pouch_state_record_write_prefix(
      fd, LC_POUCH_STATE_RECORD_STATE_PUT, key, strlen(key), meta, meta_len, 0U,
      0UL, LC_POUCH_RECORD_FLAG_PENDING, error);
  if (rc != LC_OK) {
    close(fd);
    lc_free_with_allocator(&pouch->allocator, meta);
    lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_free_with_allocator(&pouch->allocator, segment_path);
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
    close(fd);
    lc_free_with_allocator(&pouch->allocator, meta);
    lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state payload hash context",
                        NULL, NULL, "pouch");
  }
  rc = lc_pouch_state_hash_source_init(&hash_source, body, hash_ctx, error);
  if (rc != LC_OK) {
    EVP_MD_CTX_free(hash_ctx);
    close(fd);
    lc_free_with_allocator(&pouch->allocator, meta);
    lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  crypto_context = lc_strdup_with_allocator(&pouch->allocator, payload_context);
  if (crypto_context == NULL) {
    EVP_MD_CTX_free(hash_ctx);
    close(fd);
    lc_free_with_allocator(&pouch->allocator, meta);
    lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
    lc_free_with_allocator(&pouch->allocator, payload_context);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state crypto context", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_crypto_stream_to_fd_crc(pouch->crypto, crypto_context, fd,
                                        &hash_source.pub, &bytes, &cipher_bytes,
                                        &stored_crc, &descriptor, error);
  lc_free_with_allocator(&pouch->allocator, crypto_context);
  if (rc == LC_OK && hash_source.failed) {
    rc =
        lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                     "failed to hash pouch state payload", NULL, NULL, "pouch");
  }
  if (rc == LC_OK) {
    etag = lc_pouch_state_hash_final(&pouch->allocator, hash_ctx, error);
    if (etag == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
  }
  EVP_MD_CTX_free(hash_ctx);
  updated_at_unix = (long)lc_pouch_maintenance_now_seconds();
  if (rc == LC_OK) {
    lc_pouch_state_payload_span_cleanup(&pouch->allocator, &payload_span);
    rc = lc_pouch_state_payload_span_set(
        &pouch->allocator, &payload_span, manifest.active_segment, segment_size,
        payload_offset, cipher_bytes, stored_crc, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_encode_payload_meta(
        &pouch->allocator, version, updated_at_unix, bytes, cipher_bytes,
        content_type, etag, descriptor, &payload_span, payload_context,
        LC_POUCH_STATE_RECORD_DESCRIPTOR_RESERVE, has_query_hidden,
        query_hidden, &final_meta, &final_meta_len, error);
    if (rc == LC_OK && final_meta_len != meta_len) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state metadata reservation changed size", NULL,
                        NULL, "pouch");
    }
  }
  if (rc == LC_OK) {
    if (lseek(fd, (off_t)segment_size, SEEK_SET) < 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to rewrite pouch state segment header",
                        strerror(errno), NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_record_write_prefix(
        fd, LC_POUCH_STATE_RECORD_STATE_PUT, key, strlen(key), final_meta,
        final_meta_len, (uint64_t)cipher_bytes, stored_crc, 0UL, error);
  }
  if (rc == LC_OK && lseek(fd, 0, SEEK_END) < 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to restore pouch state segment append offset",
                      strerror(errno), NULL, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_defer_fsync(pouch, fd, error);
  }
  if (close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state segment", strerror(errno),
                      NULL, NULL);
  }
  fd = -1;
  if (rc != LC_OK) {
    lc_pouch_state_truncate_path_best_effort(segment_path, segment_size);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_touch_marker(pouch, &manifest, error);
  }
  if (rc == LC_OK) {
    (void)lc_pouch_state_cache_apply_write(
        pouch, namespace_name, &manifest, key, content_type, etag,
        &payload_span, payload_context, version, bytes, cipher_bytes,
        descriptor, updated_at_unix, has_query_hidden, query_hidden, 1);
    if (body->reset != NULL &&
        bytes <= LC_POUCH_STATE_BODY_CACHE_RECORD_MAX_BYTES) {
      lc_pouch_state_cache_namespace *cache;
      lc_pouch_state_cache_record *record;
      lc_error cache_error;

      lc_error_init(&cache_error);
      cache = lc_pouch_state_cache_namespace_find(pouch, namespace_name, 0,
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
    lc_pouch_state_maybe_compact(pouch, namespace_name, &manifest);
  }
  if (rc == LC_OK) {
    out->etag = etag;
    out->version = version;
    out->bytes = bytes;
    out->cipher_bytes = cipher_bytes;
    out->descriptor = descriptor;
    out->updated_at_unix = updated_at_unix;
    out->has_query_hidden = has_query_hidden;
    out->query_hidden = query_hidden;
    etag = NULL;
    descriptor = NULL;
  }
  lc_free_with_allocator(&pouch->allocator, etag);
  lc_free_with_allocator(&pouch->allocator, descriptor);
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
  lc_pouch_state_namespace_lock lock;
  lc_pouch_state_commit_group *current_group;
  int owns_commit_group;
  int rc;

  commit_group = NULL;
  owns_commit_group = 0;
  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0') {
    return lc_pouch_state_write_locked(pouch, namespace_name, key, body,
                                       options, out, error);
  }
  rc = lc_pouch_state_lock_namespace_for_mutation(pouch, namespace_name, &lock,
                                                  error);
  if (rc != LC_OK) {
    return rc;
  }
  current_group = lc_pouch_state_commit_group_current();
  if (current_group != NULL && current_group->pouch == pouch) {
    rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                           &owns_commit_group, error);
    if (rc != LC_OK) {
      lc_pouch_state_namespace_lock_release(&lock);
      return rc;
    }
  }
  rc = lc_pouch_state_write_locked(pouch, namespace_name, key, body, options,
                                   out, error);
  if (commit_group != NULL) {
    rc = lc_pouch_state_finish_commit_group(commit_group, owns_commit_group, rc,
                                            error);
  } else if (rc != LC_OK) {
    lc_pouch_state_cache_cleanup(pouch);
  }
  lc_pouch_state_namespace_lock_release(&lock);
  if (rc == LC_OK) {
    lc_pouch_query_index_note_state_write(pouch, namespace_name, key,
                                          options != NULL &&
                                                  options->content_type != NULL
                                              ? options->content_type
                                              : "application/octet-stream",
                                          body, out);
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
  memset(result, 0, sizeof(*result));
}

static int lc_pouch_state_update_metadata_locked(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    const lc_pouch_state_write_options *options,
    lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_state_entry current;
  lc_pouch_namespace_manifest manifest;
  unsigned long max_version;
  unsigned long version;
  long updated_at_unix;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || options == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_update_metadata requires pouch, "
                        "namespace, key, options, and out",
                        NULL, NULL, NULL);
  }
  if (!options->has_query_hidden) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata update requires query_hidden", NULL,
                        NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  rc = lc_pouch_state_ensure_namespace_locked(pouch, namespace_name, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&manifest, 0, sizeof(manifest));
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                        namespace_name, &manifest, NULL, NULL,
                                        error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&current, 0, sizeof(current));
  rc = lc_pouch_state_cache_lookup(pouch, namespace_name, &manifest, key,
                                   &current, &max_version, error);
  if (rc != LC_OK) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  if (!current.found) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata update requires existing state", NULL,
                        NULL, NULL);
  }
  if (options->has_expected_version &&
      current.version != options->expected_version) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata version precondition failed", NULL,
                        NULL, NULL);
  }
  if (options->precondition != NULL) {
    rc = options->precondition(options->precondition_context, error);
    if (rc != LC_OK) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return rc;
    }
  }
  version = max_version + 1UL;
  updated_at_unix = (long)lc_pouch_maintenance_now_seconds();
  rc = lc_pouch_state_append_record(
      pouch, namespace_name, &manifest, LC_POUCH_STATE_RECORD_STATE_META, key,
      current.content_type, current.etag, &current.payload_span,
      current.payload_context, version, current.bytes, current.cipher_bytes,
      current.descriptor, updated_at_unix, 1, options->query_hidden, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_touch_marker(pouch, &manifest, error);
  }
  if (rc == LC_OK) {
    (void)lc_pouch_state_cache_apply_write(
        pouch, namespace_name, &manifest, key, current.content_type,
        current.etag, &current.payload_span, current.payload_context, version,
        current.bytes, current.cipher_bytes, current.descriptor,
        updated_at_unix, 1, options->query_hidden, 1);
    lc_pouch_state_maybe_compact(pouch, namespace_name, &manifest);
  }
  if (rc == LC_OK) {
    out->etag = lc_strdup_with_allocator(&pouch->allocator, current.etag);
    if (out->etag == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch metadata state etag", NULL,
                        NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    out->version = version;
    out->bytes = current.bytes;
    out->cipher_bytes = current.cipher_bytes;
    if (current.descriptor != NULL) {
      out->descriptor =
          lc_strdup_with_allocator(&pouch->allocator, current.descriptor);
      if (out->descriptor == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch metadata descriptor", NULL,
                          NULL, NULL);
      }
    }
  }
  if (rc == LC_OK) {
    out->updated_at_unix = updated_at_unix;
    out->has_query_hidden = 1;
    out->query_hidden = options->query_hidden;
  } else {
    lc_pouch_state_write_result_cleanup(&pouch->allocator, out);
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

int lc_pouch_state_update_metadata(lc_pouch *pouch, const char *namespace_name,
                                   const char *key,
                                   const lc_pouch_state_write_options *options,
                                   lc_pouch_state_write_result *out,
                                   lc_error *error) {
  lc_pouch_state_commit_group *commit_group;
  lc_pouch_state_namespace_lock lock;
  lc_pouch_state_commit_group *current_group;
  int owns_commit_group;
  int rc;

  commit_group = NULL;
  owns_commit_group = 0;
  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0') {
    return lc_pouch_state_update_metadata_locked(pouch, namespace_name, key,
                                                 options, out, error);
  }
  rc = lc_pouch_state_lock_namespace_for_mutation(pouch, namespace_name, &lock,
                                                  error);
  if (rc != LC_OK) {
    return rc;
  }
  current_group = lc_pouch_state_commit_group_current();
  if (current_group != NULL && current_group->pouch == pouch) {
    rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                           &owns_commit_group, error);
    if (rc != LC_OK) {
      lc_pouch_state_namespace_lock_release(&lock);
      return rc;
    }
  }
  rc = lc_pouch_state_update_metadata_locked(pouch, namespace_name, key,
                                             options, out, error);
  if (commit_group != NULL) {
    rc = lc_pouch_state_finish_commit_group(commit_group, owns_commit_group, rc,
                                            error);
  } else if (rc != LC_OK) {
    lc_pouch_state_cache_cleanup(pouch);
  }
  lc_pouch_state_namespace_lock_release(&lock);
  return rc;
}

static int lc_pouch_state_delete_locked(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    const lc_pouch_state_write_options *options,
    lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_state_entry current;
  lc_pouch_namespace_manifest manifest;
  char *etag;
  unsigned long max_version;
  unsigned long version;
  long updated_at_unix;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_delete requires pouch, namespace, key "
                        "and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  rc = lc_pouch_state_ensure_namespace_locked(pouch, namespace_name, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&manifest, 0, sizeof(manifest));
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                        namespace_name, &manifest, NULL, NULL,
                                        error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&current, 0, sizeof(current));
  rc = lc_pouch_state_cache_lookup(pouch, namespace_name, &manifest, key,
                                   &current, &max_version, error);
  if (rc != LC_OK) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
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
  version = max_version + 1UL;
  etag = lc_pouch_state_empty_etag(&pouch->allocator, error);
  if (etag == NULL) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  updated_at_unix = (long)lc_pouch_maintenance_now_seconds();
  rc = lc_pouch_state_append_tombstone(pouch, namespace_name, &manifest, key,
                                       etag, version, updated_at_unix, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_touch_marker(pouch, &manifest, error);
  }
  if (rc == LC_OK) {
    (void)lc_pouch_state_cache_apply_write(
        pouch, namespace_name, &manifest, key, NULL, etag, NULL, NULL, version,
        0UL, 0UL, NULL, updated_at_unix, 0, 0, 0);
    lc_pouch_state_maybe_compact(pouch, namespace_name, &manifest);
  }
  if (rc == LC_OK) {
    out->etag = etag;
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
  lc_pouch_state_namespace_lock lock;
  lc_pouch_state_commit_group *current_group;
  int owns_commit_group;
  int rc;

  commit_group = NULL;
  owns_commit_group = 0;
  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0') {
    return lc_pouch_state_delete_locked(pouch, namespace_name, key, options,
                                        out, error);
  }
  rc = lc_pouch_state_lock_namespace_for_mutation(pouch, namespace_name, &lock,
                                                  error);
  if (rc != LC_OK) {
    return rc;
  }
  current_group = lc_pouch_state_commit_group_current();
  if (current_group != NULL && current_group->pouch == pouch) {
    rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                           &owns_commit_group, error);
    if (rc != LC_OK) {
      lc_pouch_state_namespace_lock_release(&lock);
      return rc;
    }
  }
  rc = lc_pouch_state_delete_locked(pouch, namespace_name, key, options, out,
                                    error);
  if (commit_group != NULL) {
    rc = lc_pouch_state_finish_commit_group(commit_group, owns_commit_group, rc,
                                            error);
  } else if (rc != LC_OK) {
    lc_pouch_state_cache_cleanup(pouch);
  }
  lc_pouch_state_namespace_lock_release(&lock);
  if (rc == LC_OK && out->version > 0UL) {
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
  unsigned long committed_max_version;
  unsigned long staged_max_version;
  unsigned long version;
  unsigned long decision_version;
  unsigned long discard_version;
  long updated_at_unix;
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
  rc = lc_pouch_state_ensure_namespace_locked(pouch, namespace_name, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, staged_key);
    return rc;
  }
  memset(&manifest, 0, sizeof(manifest));
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                        namespace_name, &manifest, NULL, NULL,
                                        error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, staged_key);
    return rc;
  }
  memset(&committed, 0, sizeof(committed));
  memset(&staged, 0, sizeof(staged));
  rc = lc_pouch_state_scan(pouch, &manifest, key, &committed,
                           &committed_max_version, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_scan(pouch, &manifest, staged_key, &staged,
                             &staged_max_version, error);
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
  } else if (committed.found) {
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
  version = committed_max_version > staged_max_version ? committed_max_version
                                                       : staged_max_version;
  version++;
  updated_at_unix = (long)lc_pouch_maintenance_now_seconds();
  decision_version = version + 1UL;
  discard_version = decision_version + 1UL;
  rc = lc_pouch_state_append_staged_commit_batch(
      pouch, namespace_name, &manifest, key, staged_key, &staged, version,
      decision_version, discard_version, updated_at_unix, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  rc = lc_pouch_state_touch_marker(pouch, &manifest, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  (void)lc_pouch_state_cache_apply_write(
      pouch, namespace_name, &manifest, key, staged.content_type, staged.etag,
      &staged.payload_span, staged.payload_context, version, staged.bytes,
      staged.cipher_bytes, staged.descriptor, updated_at_unix,
      staged.has_query_hidden, staged.query_hidden, 1);
  (void)lc_pouch_state_cache_apply_write(
      pouch, namespace_name, &manifest, staged_key, NULL, staged.etag, NULL,
      NULL, discard_version, 0UL, 0UL, NULL, updated_at_unix, 0, 0, 0);
  out->etag = lc_strdup_with_allocator(&pouch->allocator, staged.etag);
  if (out->etag == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch staged promotion etag", NULL,
                      NULL, NULL);
    goto cleanup;
  }
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
  lc_pouch_state_maybe_compact(pouch, namespace_name, &manifest);

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
  lc_pouch_state_namespace_lock lock;
  int owns_commit_group;
  int rc;

  commit_group = NULL;
  owns_commit_group = 0;
  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0') {
    return lc_pouch_state_promote_staged_locked(pouch, namespace_name, key,
                                                txn_id, expected_committed_etag,
                                                out, error);
  }
  rc = lc_pouch_state_lock_namespace_for_mutation(pouch, namespace_name, &lock,
                                                  error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                         &owns_commit_group, error);
  if (rc != LC_OK) {
    lc_pouch_state_namespace_lock_release(&lock);
    return rc;
  }
  rc = lc_pouch_state_promote_staged_locked(
      pouch, namespace_name, key, txn_id, expected_committed_etag, out, error);
  rc = lc_pouch_state_finish_commit_group(commit_group, owns_commit_group, rc,
                                          error);
  lc_pouch_state_namespace_lock_release(&lock);
  return rc;
}

static int lc_pouch_state_commit_staged_locked(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    const char *txn_id, lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_state_entry committed;
  lc_pouch_state_entry staged;
  lc_pouch_namespace_manifest manifest;
  char *staged_key;
  unsigned long committed_max_version;
  unsigned long staged_max_version;
  unsigned long version;
  unsigned long decision_version;
  unsigned long discard_version;
  long updated_at_unix;
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
  rc = lc_pouch_state_ensure_namespace_locked(pouch, namespace_name, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, staged_key);
    return rc;
  }
  memset(&manifest, 0, sizeof(manifest));
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                        namespace_name, &manifest, NULL, NULL,
                                        error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, staged_key);
    return rc;
  }
  memset(&committed, 0, sizeof(committed));
  memset(&staged, 0, sizeof(staged));
  rc = lc_pouch_state_scan(pouch, &manifest, key, &committed,
                           &committed_max_version, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_scan(pouch, &manifest, staged_key, &staged,
                             &staged_max_version, error);
  }
  if (rc != LC_OK) {
    goto cleanup;
  }
  if (!staged.found) {
    goto cleanup;
  }

  version = committed_max_version > staged_max_version ? committed_max_version
                                                       : staged_max_version;
  version++;
  updated_at_unix = (long)lc_pouch_maintenance_now_seconds();
  decision_version = version + 1UL;
  discard_version = decision_version + 1UL;
  rc = lc_pouch_state_append_staged_commit_batch(
      pouch, namespace_name, &manifest, key, staged_key, &staged, version,
      decision_version, discard_version, updated_at_unix, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  rc = lc_pouch_state_touch_marker(pouch, &manifest, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  (void)lc_pouch_state_cache_apply_write(
      pouch, namespace_name, &manifest, key, staged.content_type, staged.etag,
      &staged.payload_span, staged.payload_context, version, staged.bytes,
      staged.cipher_bytes, staged.descriptor, updated_at_unix,
      staged.has_query_hidden, staged.query_hidden, 1);
  (void)lc_pouch_state_cache_apply_write(
      pouch, namespace_name, &manifest, staged_key, NULL, staged.etag, NULL,
      NULL, discard_version, 0UL, 0UL, NULL, updated_at_unix, 0, 0, 0);
  out->etag = lc_strdup_with_allocator(&pouch->allocator, staged.etag);
  if (out->etag == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch staged commit etag", NULL, NULL,
                      NULL);
    goto cleanup;
  }
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
  lc_pouch_state_maybe_compact(pouch, namespace_name, &manifest);

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
  lc_pouch_state_namespace_lock lock;
  int owns_commit_group;
  int rc;

  commit_group = NULL;
  owns_commit_group = 0;
  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0') {
    return lc_pouch_state_commit_staged_locked(pouch, namespace_name, key,
                                               txn_id, out, error);
  }
  rc = lc_pouch_state_lock_namespace_for_mutation(pouch, namespace_name, &lock,
                                                  error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                         &owns_commit_group, error);
  if (rc != LC_OK) {
    lc_pouch_state_namespace_lock_release(&lock);
    return rc;
  }
  rc = lc_pouch_state_commit_staged_locked(pouch, namespace_name, key, txn_id,
                                           out, error);
  rc = lc_pouch_state_finish_commit_group(commit_group, owns_commit_group, rc,
                                          error);
  lc_pouch_state_namespace_lock_release(&lock);
  return rc;
}

static int lc_pouch_state_discard_staged_locked(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    const char *txn_id, int *discarded, lc_error *error) {
  lc_pouch_state_entry staged;
  lc_pouch_namespace_manifest manifest;
  char *staged_key;
  char *etag;
  unsigned long max_version;
  unsigned long decision_version;
  unsigned long tombstone_version;
  long updated_at_unix;
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
  rc = lc_pouch_state_ensure_namespace_locked(pouch, namespace_name, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, staged_key);
    return rc;
  }
  memset(&manifest, 0, sizeof(manifest));
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                        namespace_name, &manifest, NULL, NULL,
                                        error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, staged_key);
    return rc;
  }
  memset(&staged, 0, sizeof(staged));
  etag = NULL;
  rc = lc_pouch_state_scan(pouch, &manifest, staged_key, &staged, &max_version,
                           error);
  if (rc == LC_OK && staged.found) {
    decision_version = max_version + 1UL;
    tombstone_version = decision_version + 1UL;
    updated_at_unix = (long)lc_pouch_maintenance_now_seconds();
    etag = lc_pouch_state_empty_etag(&pouch->allocator, error);
    if (etag == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    } else {
      rc = lc_pouch_state_append_staged_discard_batch(
          pouch, namespace_name, &manifest, staged_key, etag, decision_version,
          tombstone_version, updated_at_unix, error);
      if (rc == LC_OK) {
        rc = lc_pouch_state_touch_marker(pouch, &manifest, error);
      }
      if (rc == LC_OK) {
        (void)lc_pouch_state_cache_apply_write(
            pouch, namespace_name, &manifest, staged_key, NULL, etag, NULL,
            NULL, tombstone_version, 0UL, 0UL, NULL, updated_at_unix, 0, 0, 0);
        lc_pouch_state_maybe_compact(pouch, namespace_name, &manifest);
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
  lc_pouch_state_namespace_lock lock;
  int owns_commit_group;
  int rc;

  commit_group = NULL;
  owns_commit_group = 0;
  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0') {
    return lc_pouch_state_discard_staged_locked(pouch, namespace_name, key,
                                                txn_id, discarded, error);
  }
  rc = lc_pouch_state_lock_namespace_for_mutation(pouch, namespace_name, &lock,
                                                  error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_state_commit_group_begin(pouch, &commit_group,
                                         &owns_commit_group, error);
  if (rc != LC_OK) {
    lc_pouch_state_namespace_lock_release(&lock);
    return rc;
  }
  rc = lc_pouch_state_discard_staged_locked(pouch, namespace_name, key, txn_id,
                                            discarded, error);
  rc = lc_pouch_state_finish_commit_group(commit_group, owns_commit_group, rc,
                                          error);
  lc_pouch_state_namespace_lock_release(&lock);
  return rc;
}

static int lc_pouch_state_read_internal(lc_pouch *pouch,
                                        const char *namespace_name,
                                        const char *key, int include_body,
                                        lc_pouch_state_read_result *out,
                                        lc_error *error) {
  lc_pouch_state_cache_namespace *cache;
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
  rc = lc_pouch_ensure_namespace(pouch, namespace_name, error);
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
  memset(&current, 0, sizeof(current));
  rc = lc_pouch_state_cache_lookup(pouch, namespace_name, &manifest, key,
                                   &current, NULL, error);
  if (rc != LC_OK || !current.found) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    goto cleanup_unlocked;
  }
  cache = lc_pouch_state_cache_namespace_find(pouch, namespace_name, 0, NULL);
  record = cache != NULL ? lc_pouch_state_cache_record_find(cache, key) : NULL;
  if (include_body && pouch->single_writer && cache != NULL && record != NULL) {
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
        out->version = current.version;
        out->bytes = current.bytes;
        out->cipher_bytes = current.cipher_bytes;
        out->updated_at_unix = current.updated_at_unix;
        out->has_query_hidden = current.has_query_hidden;
        out->query_hidden = current.query_hidden;
        out->found = 1;
        current.content_type = NULL;
        current.etag = NULL;
        current.descriptor = NULL;
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
  memset(&manifest, 0, sizeof(manifest));
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                        namespace_name, &manifest, NULL, NULL,
                                        error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&current, 0, sizeof(current));
  rc = lc_pouch_state_cache_lookup(pouch, namespace_name, &manifest, key,
                                   &current, NULL, error);
  if (rc == LC_OK && current.found) {
    rc = lc_pouch_state_read_result_from_entry(pouch, namespace_name, &manifest,
                                               &current, 0, out, error);
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
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
  lc_pouch_state_cache_namespace *cache;
  lc_pouch_state_cache_record *record;
  lc_pouch_state_entry current;
  lc_pouch_namespace_manifest manifest;
  lc_pouch_state_process_namespace_mutex *process_mutex;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || dst == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_copy requires pouch, namespace, key, "
                        "sink, and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  process_mutex = NULL;
  rc = lc_pouch_state_process_namespace_mutex_lock(pouch, namespace_name,
                                                   &process_mutex, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (pouch->single_writer) {
    cache = lc_pouch_state_cache_namespace_find(pouch, namespace_name, 0, NULL);
    record =
        cache != NULL ? lc_pouch_state_cache_record_find(cache, key) : NULL;
    if (record != NULL && record->found && record->body_cache != NULL &&
        record->body_cache->version == record->version &&
        record->body_cache->length == (size_t)record->bytes) {
      rc = lc_pouch_state_read_result_from_cache_record(pouch, record, out,
                                                        error);
      if (rc == LC_OK) {
        rc = lc_sink_memory_reserve(dst, record->body_cache->length, error);
      }
      if (rc == LC_OK && record->body_cache->length > 0U &&
          !dst->write(dst, record->body_cache->bytes,
                      record->body_cache->length, error)) {
        rc = error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_TRANSPORT;
      }
      goto cleanup_unlocked;
    }
  }
  rc = lc_pouch_ensure_namespace(pouch, namespace_name, error);
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
  memset(&current, 0, sizeof(current));
  rc = lc_pouch_state_cache_lookup(pouch, namespace_name, &manifest, key,
                                   &current, NULL, error);
  if (rc != LC_OK || !current.found) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    goto cleanup_unlocked;
  }
  cache = lc_pouch_state_cache_namespace_find(pouch, namespace_name, 0, NULL);
  record = cache != NULL ? lc_pouch_state_cache_record_find(cache, key) : NULL;
  if (cache != NULL && record != NULL && record->body_cache != NULL &&
      record->body_cache->version == record->version &&
      record->body_cache->length == (size_t)record->bytes) {
    rc = lc_sink_memory_reserve(dst, record->body_cache->length, error);
    if (rc == LC_OK && record->body_cache->length > 0U &&
        !dst->write(dst, record->body_cache->bytes, record->body_cache->length,
                    error)) {
      rc = error != NULL && error->code != LC_OK ? error->code
                                                 : LC_ERR_TRANSPORT;
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_read_result_from_entry(
          pouch, namespace_name, &manifest, &current, 0, out, error);
    }
  } else {
    rc = lc_pouch_state_read_result_from_entry(pouch, namespace_name, &manifest,
                                               &current, 1, out, error);
    if (rc == LC_OK && out->found) {
      rc = out->bytes > 0UL
               ? lc_sink_memory_reserve(dst, (size_t)out->bytes, error)
               : LC_OK;
    }
    if (rc == LC_OK && out->found && out->body != NULL) {
      rc = lc_copy(out->body, dst, NULL, error);
      out->body->close(out->body);
      out->body = NULL;
    }
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
cleanup_unlocked:
  lc_pouch_state_process_namespace_mutex_unlock(&process_mutex);
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
  lc_pouch_state_cache_namespace *cache;
  lc_pouch_state_cache_record **record_index;
  lc_pouch_state_read_many_snapshot *snapshots;
  size_t record_index_count;
  size_t index;
  size_t snapshot_count;
  lc_pouch_state_process_namespace_mutex *process_mutex;
  int force_refresh;
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
  rc = lc_pouch_state_process_namespace_mutex_lock(pouch, namespace_name,
                                                   &process_mutex, error);
  if (rc != LC_OK) {
    lc_pouch_state_read_many_snapshots_cleanup(&pouch->allocator, snapshots,
                                               snapshot_count);
    return rc;
  }
  rc = lc_pouch_ensure_namespace(pouch, namespace_name, error);
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
  cache = lc_pouch_state_cache_namespace_find(pouch, namespace_name, 1, error);
  if (cache == NULL) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    goto cleanup_unlocked;
  }
  if (pouch->single_writer) {
    force_refresh = 0;
    rc = LC_OK;
  } else {
    force_refresh = 0;
    rc = lc_pouch_namespace_marker_refresh_should_scan(
        &pouch->allocator, manifest.namespace_path, pouch->writer_marker_leaf,
        &cache->marker_refresh, LC_POUCH_STATE_SHARED_FORCE_AFTER_SKIPS,
        &force_refresh, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_cache_refresh(pouch, cache, &manifest, force_refresh,
                                      error);
  }
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
cleanup_unlocked:
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
  lc_pouch_state_cache_namespace *cache;
  lc_pouch_state_cache_record **record_index;
  lc_pouch_state_scan_body_snapshot *snapshots;
  size_t record_index_count;
  size_t snapshot_count;
  size_t snapshot_capacity;
  size_t index;
  size_t next_index;
  lc_pouch_state_process_namespace_mutex *process_mutex;
  int force_refresh;
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
  cache = lc_pouch_state_cache_namespace_find(pouch, namespace_name, 1, error);
  if (cache == NULL) {
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    goto cleanup_unlocked;
  }
  if (pouch->single_writer) {
    force_refresh = 0;
    rc = LC_OK;
  } else {
    force_refresh = 0;
    rc = lc_pouch_namespace_marker_refresh_should_scan(
        &pouch->allocator, manifest.namespace_path, pouch->writer_marker_leaf,
        &cache->marker_refresh, LC_POUCH_STATE_SHARED_FORCE_AFTER_SKIPS,
        &force_refresh, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_cache_refresh(pouch, cache, &manifest, force_refresh,
                                      error);
  }
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
cleanup_unlocked:
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
  return rc;
}

static int lc_pouch_state_visit_internal(lc_pouch *pouch,
                                         const char *namespace_name,
                                         lc_pouch_state_visit_fn visitor,
                                         void *context, int locked,
                                         lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  lc_pouch_state_cache_namespace *cache;
  lc_pouch_state_cache_record *record;
  lc_pouch_state_visit_snapshot *snapshots;
  size_t snapshot_count;
  size_t snapshot_capacity;
  size_t i;
  lc_pouch_state_process_namespace_mutex *process_mutex;
  int force_refresh;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      visitor == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_visit requires pouch, namespace, and "
                        "visitor",
                        NULL, NULL, NULL);
  }
  process_mutex = NULL;
  if (!locked) {
    rc = lc_pouch_state_process_namespace_mutex_lock(pouch, namespace_name,
                                                     &process_mutex, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  rc = locked ? lc_pouch_state_ensure_namespace_locked(pouch, namespace_name,
                                                       error)
              : lc_pouch_ensure_namespace(pouch, namespace_name, error);
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
  snapshots = NULL;
  snapshot_count = 0U;
  snapshot_capacity = 0U;
  cache = lc_pouch_state_cache_namespace_find(pouch, namespace_name, 1, error);
  if (cache == NULL) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    goto cleanup_unlocked;
  }
  if (pouch->single_writer) {
    force_refresh = 0;
    rc = LC_OK;
  } else {
    force_refresh = 0;
    rc = lc_pouch_namespace_marker_refresh_should_scan(
        &pouch->allocator, manifest.namespace_path, pouch->writer_marker_leaf,
        &cache->marker_refresh, LC_POUCH_STATE_SHARED_FORCE_AFTER_SKIPS,
        &force_refresh, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_cache_refresh(pouch, cache, &manifest, force_refresh,
                                      error);
  }
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
cleanup_unlocked:
  lc_pouch_state_process_namespace_mutex_unlock(&process_mutex);
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
  lc_pouch_state_cache_namespace *cache;
  lc_pouch_state_cache_record *record;
  lc_pouch_state_process_namespace_mutex *process_mutex;
  int force_refresh;
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
  cache = lc_pouch_state_cache_namespace_find(pouch, namespace_name, 0, error);
  if (pouch->single_writer && cache != NULL && cache->initialized) {
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
    goto cleanup_unlocked;
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
  cache = lc_pouch_state_cache_namespace_find(pouch, namespace_name, 1, error);
  if (cache == NULL) {
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    goto cleanup_unlocked;
  }
  if (pouch->single_writer) {
    force_refresh = 0;
    rc = LC_OK;
  } else {
    force_refresh = 0;
    rc = lc_pouch_namespace_marker_refresh_should_scan(
        &pouch->allocator, manifest.namespace_path, pouch->writer_marker_leaf,
        &cache->marker_refresh, LC_POUCH_STATE_SHARED_FORCE_AFTER_SKIPS,
        &force_refresh, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_cache_refresh(pouch, cache, &manifest, force_refresh,
                                      error);
  }
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

cleanup_unlocked:
  lc_pouch_state_process_namespace_mutex_unlock(&process_mutex);
  return rc;
}

int lc_pouch_state_visit_since(lc_pouch *pouch, const char *namespace_name,
                               unsigned long after_version,
                               lc_pouch_state_change_visit_fn visitor,
                               void *context, lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  lc_pouch_state_cache_namespace *cache;
  lc_pouch_state_cache_record *record;
  lc_pouch_state_visit_snapshot *snapshots;
  lc_pouch_state_process_namespace_mutex *process_mutex;
  size_t snapshot_count;
  size_t snapshot_capacity;
  size_t i;
  int force_refresh;
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
  rc = lc_pouch_state_process_namespace_mutex_lock(pouch, namespace_name,
                                                   &process_mutex, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_ensure_namespace(pouch, namespace_name, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  memset(&manifest, 0, sizeof(manifest));
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                        namespace_name, &manifest, NULL, NULL,
                                        error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  cache = lc_pouch_state_cache_namespace_find(pouch, namespace_name, 1, error);
  if (cache == NULL) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    goto cleanup;
  }
  if (pouch->single_writer) {
    force_refresh = 0;
    rc = LC_OK;
  } else {
    force_refresh = 0;
    rc = lc_pouch_namespace_marker_refresh_should_scan(
        &pouch->allocator, manifest.namespace_path, pouch->writer_marker_leaf,
        &cache->marker_refresh, LC_POUCH_STATE_SHARED_FORCE_AFTER_SKIPS,
        &force_refresh, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_cache_refresh(pouch, cache, &manifest, force_refresh,
                                      error);
  }
  for (record = rc == LC_OK ? cache->records : NULL; record != NULL;
       record = record->next) {
    if (record->version <= after_version) {
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
                             unsigned long *out, lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  lc_pouch_state_cache_namespace *cache;
  lc_pouch_state_process_namespace_mutex *process_mutex;
  int force_refresh;
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
  if (!pouch->single_writer) {
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
  cache = lc_pouch_state_cache_namespace_find(pouch, namespace_name, 0, NULL);
  if (pouch->single_writer) {
    if (cache != NULL && cache->initialized) {
      *out = cache->max_version > manifest.state_max_version
                 ? cache->max_version
                 : manifest.state_max_version;
      if (*out > manifest.state_max_version) {
        manifest.state_max_version = *out;
        rc = lc_pouch_namespace_manifest_save(&pouch->allocator, namespace_name,
                                              &manifest, error);
      }
    } else if (manifest.state_max_version > 0UL) {
      *out = manifest.state_max_version;
      rc = LC_OK;
    } else {
      rc = lc_pouch_state_manifest_max_version(pouch, &manifest, out, error);
      if (rc == LC_OK && *out > manifest.state_max_version) {
        manifest.state_max_version = *out;
        rc = lc_pouch_namespace_manifest_save(&pouch->allocator, namespace_name,
                                              &manifest, error);
      }
    }
  } else {
    cache =
        lc_pouch_state_cache_namespace_find(pouch, namespace_name, 1, error);
    if (cache == NULL) {
      rc = error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    } else {
      force_refresh = 0;
      rc = lc_pouch_namespace_marker_refresh_should_scan(
          &pouch->allocator, manifest.namespace_path, pouch->writer_marker_leaf,
          &cache->marker_refresh, LC_POUCH_STATE_SHARED_FORCE_AFTER_SKIPS,
          &force_refresh, error);
      if (rc == LC_OK) {
        rc = lc_pouch_state_cache_refresh(pouch, cache, &manifest,
                                          force_refresh, error);
      }
      if (rc == LC_OK) {
        *out = cache->max_version;
        if (*out > manifest.state_max_version) {
          manifest.state_max_version = *out;
          rc = lc_pouch_namespace_manifest_save(
              &pouch->allocator, namespace_name, &manifest, error);
        }
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
  if (result->body != NULL) {
    result->body->close(result->body);
  }
  memset(result, 0, sizeof(*result));
}

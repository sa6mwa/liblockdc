#include "lc_pouch_store.h"

#include "lc_api_internal.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define LC_POUCH_LOG_MAGIC "LCP1"
#define LC_POUCH_QUERY_INDEX_MAGIC "LCQI"
#define LC_POUCH_HEADER_SIZE 64U
#define LC_POUCH_QUERY_INDEX_HEADER_SIZE 64U
#define LC_POUCH_RECORD_VERSION 1U
#define LC_POUCH_QUERY_INDEX_RECORD_VERSION 1U
#define LC_POUCH_RECORD_STATE_PUT 1U
#define LC_POUCH_RECORD_STATE_REMOVE 2U
#define LC_POUCH_RECORD_META_PUT 3U
#define LC_POUCH_RECORD_META_REMOVE 4U
#define LC_POUCH_RECORD_OBJECT_PUT 5U
#define LC_POUCH_RECORD_OBJECT_REMOVE 6U
#define LC_POUCH_RECORD_QUEUE_PUT 7U
#define LC_POUCH_RECORD_QUEUE_UPDATE 8U
#define LC_POUCH_RECORD_QUEUE_REMOVE 9U
#define LC_POUCH_RECORD_STATE_LINK 10U
#define LC_POUCH_RECORD_HIGH_WATER 11U
#define LC_POUCH_BACKEND_NAMESPACE ".lockd"
#define LC_POUCH_BACKEND_KEY "backend-id"
#define LC_POUCH_BACKEND_CONTENT_TYPE "text/plain"
#define LC_POUCH_META_FLAG_QUERY_HIDDEN_SET 1UL
#define LC_POUCH_META_FLAG_QUERY_HIDDEN 2UL
#define LC_POUCH_QUERY_INDEX_RECORD_META 1UL
#define LC_POUCH_QUERY_INDEX_FLAG_DELETED 1UL
#define LC_POUCH_QUERY_INDEX_FLAG_QUERY_HIDDEN_SET 2UL
#define LC_POUCH_QUERY_INDEX_FLAG_QUERY_HIDDEN 4UL
#define LC_POUCH_OBJECT_META_SIZE 28U
#define LC_POUCH_QUEUE_META_SIZE 88U
#define LC_POUCH_MAX_NAME_BYTES 4096UL
#define LC_POUCH_MAX_CONTENT_TYPE_BYTES 4096UL
#define LC_POUCH_MAX_ETAG_BYTES 4096UL
#define LC_POUCH_MAX_INLINE_BODY_BYTES (64UL * 1024UL * 1024UL)
#define LC_POUCH_COMPACT_MIN_LOG_BYTES (64UL * 1024UL)
#define LC_POUCH_COMPACT_OBSOLETE_MULTIPLIER 2UL

typedef struct lc_pouch_disk_state_entry {
  char *namespace_name;
  char *key;
  char *content_type;
  char *etag;
  long version;
  unsigned long body_offset;
  unsigned long body_length;
  int deleted;
} lc_pouch_disk_state_entry;

typedef struct lc_pouch_disk_meta_entry {
  char *namespace_name;
  char *key;
  char *etag;
  lc_pouch_meta meta;
  int deleted;
} lc_pouch_disk_meta_entry;

typedef struct lc_pouch_disk_meta_upsert {
  lc_pouch_disk_meta_entry entry;
  int existing;
  int is_new;
} lc_pouch_disk_meta_upsert;

typedef struct lc_pouch_disk_object_entry {
  char *namespace_name;
  char *key;
  char *id;
  char *name;
  char *content_type;
  long size;
  long created_at_unix;
  long updated_at_unix;
  unsigned long body_offset;
  unsigned long body_length;
  int deleted;
} lc_pouch_disk_object_entry;

typedef struct lc_pouch_disk_queue_entry {
  char *namespace_name;
  char *queue;
  char *message_id;
  char *payload_content_type;
  char *lease_id;
  char *txn_id;
  char *meta_etag;
  int attempts;
  int max_attempts;
  int failure_attempts;
  long enqueued_at_unix;
  long not_visible_until_unix;
  long visibility_timeout_seconds;
  long expires_at_unix;
  long lease_expires_at_unix;
  long fencing_token;
  unsigned long body_offset;
  unsigned long body_length;
  int deleted;
} lc_pouch_disk_queue_entry;

typedef struct lc_pouch_disk_store {
  lc_pouch_store pub;
  lc_pouch_allocator allocator;
  char *root_path;
  char *log_path;
  char *lock_path;
  char *query_index_path;
  char *query_engine;
  char *query_fallback_engine;
  int log_fd;
  int lock_fd;
  int query_index_fd;
  lc_pouch_disk_state_entry *state_entries;
  size_t state_entry_count;
  size_t state_entry_capacity;
  lc_pouch_disk_meta_entry *meta_entries;
  size_t meta_entry_count;
  size_t meta_entry_capacity;
  size_t *query_meta_indices;
  size_t query_meta_index_count;
  size_t query_meta_index_capacity;
  lc_pouch_disk_object_entry *object_entries;
  size_t object_entry_count;
  size_t object_entry_capacity;
  lc_pouch_disk_queue_entry *queue_entries;
  size_t queue_entry_count;
  size_t queue_entry_capacity;
  long next_version;
  unsigned long replayed_log_size;
  unsigned long replayed_record_count;
  int defer_record_fsync;
} lc_pouch_disk_store;

typedef struct lc_pouch_file_source {
  lc_pouch_allocator allocator;
  int fd;
  unsigned long remaining;
} lc_pouch_file_source;

typedef struct lc_pouch_disk_scan_meta_copy {
  char *key;
  char *etag;
  lc_pouch_meta meta;
} lc_pouch_disk_scan_meta_copy;

typedef struct lc_pouch_disk_staged_match {
  lc_pouch_disk_state_entry *entry;
  const char *txn_id;
} lc_pouch_disk_staged_match;

static unsigned long lc_pouch_crc32(const unsigned char *bytes, size_t count);
static unsigned long lc_pouch_crc32_update(unsigned long crc,
                                           const unsigned char *bytes,
                                           size_t count);
static int lc_pouch_disk_load_meta(lc_pouch_store *self,
                                   const char *namespace_name, const char *key,
                                   lc_pouch_meta_record *out, lc_error *error);
static int lc_pouch_disk_store_meta(lc_pouch_store *self,
                                    const char *namespace_name, const char *key,
                                    const lc_pouch_meta *meta,
                                    const char *expected_etag,
                                    lc_pouch_store_meta_res *out,
                                    lc_error *error);
static int lc_pouch_disk_delete_meta(lc_pouch_store *self,
                                     const char *namespace_name,
                                     const char *key, const char *expected_etag,
                                     lc_error *error);
static int lc_pouch_disk_scan_meta(lc_pouch_store *self,
                                   const lc_pouch_scan_meta_req *req,
                                   lc_pouch_scan_meta_visit_fn visit,
                                   void *visit_context,
                                   lc_pouch_scan_meta_res *out,
                                   lc_error *error);
static int lc_pouch_disk_query_index_scan(
    lc_pouch_store *self, const lc_pouch_query_index_scan_req *req,
    lc_pouch_scan_meta_visit_fn visit, void *visit_context,
    lc_pouch_query_index_scan_res *out, lc_error *error);
static int lc_pouch_disk_query_index_keys_scan(
    lc_pouch_store *self, const lc_pouch_query_index_scan_req *req,
    lc_pouch_query_index_key_visit_fn visit, void *visit_context,
    lc_pouch_query_index_scan_res *out, lc_error *error);
static int lc_pouch_disk_flush_index(lc_pouch_store *self,
                                     const char *namespace_name,
                                     const char *mode,
                                     lc_pouch_index_flush_res *out,
                                     lc_error *error);
static int lc_pouch_disk_append_record(
    lc_pouch_disk_store *store, unsigned long type, const char *namespace_name,
    const char *key, const char *content_type, const char *etag, long version,
    const unsigned char *body, size_t body_length,
    unsigned long *body_offset_out, lc_error *error);
static int lc_pouch_disk_append_fd_record(
    lc_pouch_disk_store *store, unsigned long type, const char *namespace_name,
    const char *key, const char *content_type, const char *etag, long version,
    int body_fd, unsigned long body_offset, unsigned long body_length,
    unsigned long *body_offset_out, lc_error *error);
static int lc_pouch_disk_append_object_copy_record(
    lc_pouch_disk_store *store, int read_fd, const char *namespace_name,
    const char *key, const char *name, const char *id,
    const char *payload_content_type, unsigned long src_body_offset,
    unsigned long src_body_length, long created_at_unix, long updated_at_unix,
    unsigned long *payload_offset_out, lc_error *error);
static int lc_pouch_disk_append_queue_put_fd_entry(
    lc_pouch_disk_store *store, lc_pouch_disk_queue_entry *entry,
    int payload_fd, unsigned long payload_offset, unsigned long payload_length,
    lc_error *error);
static int lc_pouch_disk_append_high_water_record(lc_pouch_disk_store *store,
                                                  long version,
                                                  lc_error *error);
static int lc_pouch_disk_append_query_index_record(
    lc_pouch_disk_store *store, const char *namespace_name, const char *key,
    const char *etag, long version, const lc_pouch_meta *meta, int deleted,
    lc_error *error);
static int lc_pouch_disk_query_index_reserve(lc_pouch_disk_store *store);
static int lc_pouch_disk_query_index_insert(lc_pouch_disk_store *store,
                                            size_t meta_index);
static unsigned long lc_pouch_disk_index_sequence(
    const lc_pouch_disk_store *store);
static int lc_pouch_disk_maybe_compact_locked(lc_pouch_disk_store *store,
                                              unsigned long log_size,
                                              lc_error *error);
static int lc_pouch_disk_replay(lc_pouch_disk_store *store, lc_error *error);
static int lc_pouch_disk_read_state(lc_pouch_store *self,
                                    const char *namespace_name, const char *key,
                                    lc_source **body, lc_pouch_state_info *out,
                                    lc_error *error);
static int lc_pouch_disk_write_state(lc_pouch_store *self,
                                     const char *namespace_name,
                                     const char *key, lc_source *body,
                                     const lc_pouch_put_state_opts *opts,
                                     lc_pouch_put_state_res *out,
                                     lc_error *error);
static int lc_pouch_disk_remove_state(lc_pouch_store *self,
                                      const char *namespace_name,
                                      const char *key,
                                      const char *expected_etag,
                                      int *removed, lc_error *error);
static int lc_pouch_disk_stage_state(lc_pouch_store *self,
                                     const char *namespace_name,
                                     const char *key, const char *txn_id,
                                     lc_source *body,
                                     const lc_pouch_put_state_opts *opts,
                                     lc_pouch_put_state_res *out,
                                     lc_error *error);
static int lc_pouch_disk_load_staged_state(lc_pouch_store *self,
                                           const char *namespace_name,
                                           const char *key,
                                           const char *txn_id,
                                           lc_source **body,
                                           lc_pouch_state_info *out,
                                           lc_error *error);
static int lc_pouch_disk_promote_staged_state(
    lc_pouch_store *self, const char *namespace_name, const char *key,
    const char *txn_id, const lc_pouch_promote_staged_opts *opts,
    lc_pouch_put_state_res *out, lc_error *error);
static int lc_pouch_disk_discard_staged_state(
    lc_pouch_store *self, const char *namespace_name, const char *key,
    const char *txn_id, const lc_pouch_discard_staged_opts *opts,
    lc_error *error);
static int lc_pouch_disk_list_staged_state(
    lc_pouch_store *self, const lc_pouch_list_staged_req *req,
    lc_pouch_staged_state_list *out, lc_error *error);
static int lc_pouch_disk_put_object(lc_pouch_store *self,
                                    const char *namespace_name, const char *key,
                                    lc_source *body,
                                    const lc_pouch_put_object_opts *opts,
                                    lc_pouch_object_info *out, lc_error *error);
static int lc_pouch_disk_list_objects(lc_pouch_store *self,
                                      const char *namespace_name,
                                      const char *key,
                                      lc_pouch_object_list *out,
                                      lc_error *error);
static int lc_pouch_disk_get_object(lc_pouch_store *self,
                                    const char *namespace_name, const char *key,
                                    const lc_pouch_object_selector *selector,
                                    lc_source **body, lc_pouch_object_info *out,
                                    lc_error *error);
static char *lc_pouch_make_object_id_from_crc(lc_pouch_disk_store *store,
                                              const char *name,
                                              unsigned long crc);
static int lc_pouch_disk_copy_object(lc_pouch_store *self,
                                     const char *namespace_name,
                                     const char *src_key, const char *dst_key,
                                     const lc_pouch_copy_object_opts *opts,
                                     lc_pouch_object_info *out,
                                     lc_error *error);
static int lc_pouch_disk_delete_object(lc_pouch_store *self,
                                       const char *namespace_name,
                                       const char *key,
                                       const lc_pouch_object_selector *selector,
                                       int *deleted, lc_error *error);
static int lc_pouch_disk_delete_all_objects(lc_pouch_store *self,
                                            const char *namespace_name,
                                            const char *key, int *deleted_count,
                                            lc_error *error);
static int lc_pouch_disk_enqueue_message(lc_pouch_store *self,
                                         const char *namespace_name,
                                         const char *queue, lc_source *body,
                                         const lc_pouch_enqueue_opts *opts,
                                         lc_pouch_queue_message_info *out,
                                         lc_error *error);
static int lc_pouch_disk_dequeue_message(
    lc_pouch_store *self, const char *namespace_name, const char *queue,
    const lc_pouch_dequeue_opts *opts, lc_source **body,
    lc_pouch_queue_message_info *out, lc_error *error);
static int lc_pouch_disk_ack_message(lc_pouch_store *self,
                                     const lc_pouch_queue_ref *ref, int *acked,
                                     lc_error *error);
static int lc_pouch_disk_nack_message(lc_pouch_store *self,
                                      const lc_pouch_queue_ref *ref,
                                      long delay_seconds, int count_failure,
                                      lc_pouch_queue_message_info *out,
                                      lc_error *error);
static int lc_pouch_disk_extend_message(lc_pouch_store *self,
                                        const lc_pouch_queue_ref *ref,
                                        long extend_by_seconds,
                                        lc_pouch_queue_message_info *out,
                                        lc_error *error);
static int lc_pouch_disk_queue_stats(lc_pouch_store *self,
                                     const char *namespace_name,
                                     const char *queue,
                                     lc_pouch_queue_stats *out,
                                     lc_error *error);
static int lc_pouch_disk_query_config(lc_pouch_store *self,
                                      const char *namespace_name,
                                      lc_pouch_query_config *out,
                                      lc_error *error);
static int lc_pouch_disk_backend_hash(lc_pouch_store *self, char **out,
                                      lc_error *error);
static int lc_pouch_disk_close(lc_pouch_store *self, lc_error *error);
static int lc_pouch_disk_abort(lc_pouch_store *self, lc_error *error);

static int lc_pouch_set_errno(lc_error *error, const char *message) {
  return lc_error_set(error, LC_ERR_TRANSPORT, 0L, message, strerror(errno),
                      NULL, NULL);
}

static int lc_pouch_set_invalid(lc_error *error, const char *message) {
  return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL, NULL);
}

static int lc_pouch_set_nomem(lc_error *error, const char *message) {
  return lc_error_set(error, LC_ERR_NOMEM, 0L, message, NULL, NULL, NULL);
}

static int lc_pouch_disk_validate_name(lc_error *error, const char *operation,
                                       const char *kind, const char *value) {
  char message[160];

  if (value != NULL && value[0] != '\0') {
    return LC_OK;
  }
  snprintf(message, sizeof(message), "%s requires a non-empty %s", operation,
           kind);
  return lc_pouch_set_invalid(error, message);
}

static int lc_pouch_disk_validate_namespace_key(lc_error *error,
                                                const char *operation,
                                                const char *namespace_name,
                                                const char *key) {
  int rc;

  rc = lc_pouch_disk_validate_name(error, operation, "namespace",
                                   namespace_name);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_pouch_disk_validate_name(error, operation, "key", key);
}

static int lc_pouch_disk_validate_namespace_queue(lc_error *error,
                                                  const char *operation,
                                                  const char *namespace_name,
                                                  const char *queue) {
  int rc;

  rc = lc_pouch_disk_validate_name(error, operation, "namespace",
                                   namespace_name);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_pouch_disk_validate_name(error, operation, "queue", queue);
}

static int lc_pouch_disk_query_engine_supported(const char *value,
                                                int allow_none) {
  if (value == NULL || value[0] == '\0') {
    return 1;
  }
  if (strcmp(value, "index") == 0 || strcmp(value, "scan") == 0) {
    return 1;
  }
  return allow_none && strcmp(value, "none") == 0;
}

static const char *lc_pouch_disk_query_engine_default(const char *value) {
  return value != NULL && value[0] != '\0' ? value : "index";
}

static const char *lc_pouch_disk_query_fallback_default(const char *value) {
  return value != NULL && value[0] != '\0' ? value : "none";
}

static int lc_pouch_disk_validate_open_opts(
    const lc_pouch_disk_open_opts *opts, lc_error *error) {
  if (opts == NULL) {
    return LC_OK;
  }
  if (!lc_pouch_disk_query_engine_supported(opts->query_engine, 0)) {
    return lc_pouch_set_invalid(error,
                                "pouch disk query_engine must be index or scan");
  }
  if (!lc_pouch_disk_query_engine_supported(opts->query_fallback_engine, 1)) {
    return lc_pouch_set_invalid(
        error, "pouch disk query_fallback_engine must be none, index, or scan");
  }
  return LC_OK;
}

static int lc_pouch_disk_add_overflows(unsigned long left,
                                       unsigned long right,
                                       unsigned long *out) {
  if (left > ((unsigned long)-1) - right) {
    return 1;
  }
  *out = left + right;
  return 0;
}

static int lc_pouch_disk_validate_record_lengths(
    unsigned long ns_len, unsigned long key_len, unsigned long ct_len,
    unsigned long etag_len, unsigned long body_len, unsigned long payload_len) {
  unsigned long total;

  if (ns_len == 0UL || key_len == 0UL || ns_len > LC_POUCH_MAX_NAME_BYTES ||
      key_len > LC_POUCH_MAX_NAME_BYTES ||
      ct_len > LC_POUCH_MAX_CONTENT_TYPE_BYTES ||
      etag_len > LC_POUCH_MAX_ETAG_BYTES ||
      body_len > LC_POUCH_MAX_INLINE_BODY_BYTES ||
      payload_len > (unsigned long)(((size_t)-1) - 1U)) {
    return 0;
  }
  total = ns_len;
  if (lc_pouch_disk_add_overflows(total, key_len, &total) ||
      lc_pouch_disk_add_overflows(total, ct_len, &total) ||
      lc_pouch_disk_add_overflows(total, etag_len, &total) ||
      lc_pouch_disk_add_overflows(total, body_len, &total)) {
    return 0;
  }
  return total == payload_len;
}

static void lc_pouch_put_u32(unsigned char *dst, unsigned long value) {
  dst[0] = (unsigned char)(value & 255UL);
  dst[1] = (unsigned char)((value >> 8) & 255UL);
  dst[2] = (unsigned char)((value >> 16) & 255UL);
  dst[3] = (unsigned char)((value >> 24) & 255UL);
}

static void lc_pouch_put_u64(unsigned char *dst, unsigned long value) {
  lc_pouch_put_u32(dst, value & 0xffffffffUL);
  lc_pouch_put_u32(dst + 4, (value >> 32) & 0xffffffffUL);
}

static unsigned long lc_pouch_get_u32(const unsigned char *src) {
  return ((unsigned long)src[0]) | (((unsigned long)src[1]) << 8) |
         (((unsigned long)src[2]) << 16) | (((unsigned long)src[3]) << 24);
}

static unsigned long lc_pouch_get_u64(const unsigned char *src) {
  return lc_pouch_get_u32(src) | (lc_pouch_get_u32(src + 4) << 32);
}

static int lc_pouch_write_all(int fd, const void *bytes, size_t count) {
  const unsigned char *cursor;
  ssize_t written;

  cursor = (const unsigned char *)bytes;
  while (count > 0U) {
    written = write(fd, cursor, count);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      return 0;
    }
    if (written == 0) {
      errno = EIO;
      return 0;
    }
    cursor += written;
    count -= (size_t)written;
  }
  return 1;
}

static int lc_pouch_read_all(int fd, void *bytes, size_t count,
                             int *short_read) {
  unsigned char *cursor;
  ssize_t got;

  cursor = (unsigned char *)bytes;
  *short_read = 0;
  while (count > 0U) {
    got = read(fd, cursor, count);
    if (got < 0) {
      if (errno == EINTR) {
        continue;
      }
      return 0;
    }
    if (got == 0) {
      *short_read = 1;
      return 1;
    }
    cursor += got;
    count -= (size_t)got;
  }
  return 1;
}

static int lc_pouch_disk_fsync_record(lc_pouch_disk_store *store,
                                      const char *message, lc_error *error) {
  if (store->defer_record_fsync) {
    return LC_OK;
  }
  if (fsync(store->log_fd) != 0) {
    return lc_pouch_set_errno(error, message);
  }
  return LC_OK;
}

static char *lc_pouch_join_path(const lc_pouch_allocator *allocator,
                                const char *root, const char *leaf) {
  size_t root_len;
  size_t leaf_len;
  int need_slash;
  char *path;

  root_len = strlen(root);
  leaf_len = strlen(leaf);
  need_slash = root_len == 0U || root[root_len - 1U] != '/';
  path = (char *)lc_pouch_alloc(allocator, root_len + (need_slash ? 1U : 0U) +
                                               leaf_len + 1U);
  if (path == NULL) {
    return NULL;
  }
  memcpy(path, root, root_len);
  if (need_slash) {
    path[root_len] = '/';
    memcpy(path + root_len + 1U, leaf, leaf_len + 1U);
  } else {
    memcpy(path + root_len, leaf, leaf_len + 1U);
  }
  return path;
}

static int lc_pouch_disk_lock(lc_pouch_disk_store *store, lc_error *error) {
  struct flock lock;
  struct stat st;
  struct stat path_st;
  int new_fd;
  int rc;

  memset(&lock, 0, sizeof(lock));
  lock.l_type = F_WRLCK;
  lock.l_whence = SEEK_SET;
  if (fcntl(store->lock_fd, F_SETLKW, &lock) != 0) {
    return lc_pouch_set_errno(error, "failed to lock pouch writer lock");
  }
  if (fstat(store->log_fd, &st) != 0) {
    lock.l_type = F_UNLCK;
    (void)fcntl(store->lock_fd, F_SETLK, &lock);
    return lc_pouch_set_errno(error, "failed to stat pouch log");
  }
  if (stat(store->log_path, &path_st) != 0) {
    lock.l_type = F_UNLCK;
    (void)fcntl(store->lock_fd, F_SETLK, &lock);
    return lc_pouch_set_errno(error, "failed to stat pouch log path");
  }
  if (st.st_dev != path_st.st_dev || st.st_ino != path_st.st_ino) {
    new_fd = open(store->log_path, O_RDWR);
    if (new_fd < 0) {
      lock.l_type = F_UNLCK;
      (void)fcntl(store->lock_fd, F_SETLK, &lock);
      return lc_pouch_set_errno(error, "failed to reopen replaced pouch log");
    }
    close(store->log_fd);
    store->log_fd = new_fd;
    store->replayed_log_size = (unsigned long)-1;
    if (fstat(store->log_fd, &st) != 0) {
      lock.l_type = F_UNLCK;
      (void)fcntl(store->lock_fd, F_SETLK, &lock);
      return lc_pouch_set_errno(error, "failed to stat reopened pouch log");
    }
  }
  if ((unsigned long)st.st_size == store->replayed_log_size) {
    return LC_OK;
  }
  rc = lc_pouch_disk_replay(store, error);
  if (rc != LC_OK) {
    lock.l_type = F_UNLCK;
    (void)fcntl(store->lock_fd, F_SETLK, &lock);
  }
  return rc;
}

static int lc_pouch_disk_mark_replayed_to_current_size(
    lc_pouch_disk_store *store, lc_error *error) {
  struct stat st;

  if (fstat(store->log_fd, &st) != 0) {
    return lc_pouch_set_errno(error, "failed to stat pouch log");
  }
  store->replayed_log_size = (unsigned long)st.st_size;
  return lc_pouch_disk_maybe_compact_locked(store, (unsigned long)st.st_size,
                                            error);
}

static int lc_pouch_disk_force_replay_locked(lc_pouch_disk_store *store,
                                             lc_error *error) {
  store->replayed_log_size = (unsigned long)-1;
  return lc_pouch_disk_replay(store, error);
}

static int lc_pouch_disk_unlock(lc_pouch_disk_store *store, lc_error *error) {
  struct flock lock;

  memset(&lock, 0, sizeof(lock));
  lock.l_type = F_UNLCK;
  lock.l_whence = SEEK_SET;
  if (fcntl(store->lock_fd, F_SETLK, &lock) != 0) {
    return lc_pouch_set_errno(error, "failed to unlock pouch writer lock");
  }
  return LC_OK;
}

static int lc_pouch_disk_find_entry(lc_pouch_disk_store *store,
                                    const char *namespace_name,
                                    const char *key) {
  size_t index;

  for (index = 0U; index < store->state_entry_count; ++index) {
    if (strcmp(store->state_entries[index].namespace_name, namespace_name) ==
            0 &&
        strcmp(store->state_entries[index].key, key) == 0) {
      return (int)index;
    }
  }
  return -1;
}

static void lc_pouch_disk_entry_cleanup(lc_pouch_disk_store *store,
                                        lc_pouch_disk_state_entry *entry) {
  lc_pouch_free(&store->allocator, entry->namespace_name);
  lc_pouch_free(&store->allocator, entry->key);
  lc_pouch_free(&store->allocator, entry->content_type);
  lc_pouch_free(&store->allocator, entry->etag);
  memset(entry, 0, sizeof(*entry));
}

static int lc_pouch_disk_upsert_entry(lc_pouch_disk_store *store,
                                      const char *namespace_name,
                                      const char *key, const char *content_type,
                                      const char *etag, long version,
                                      unsigned long body_offset,
                                      unsigned long body_length, int deleted) {
  lc_pouch_disk_state_entry *entry;
  lc_pouch_disk_state_entry *grown;
  int existing;

  existing = lc_pouch_disk_find_entry(store, namespace_name, key);
  if (existing >= 0) {
    entry = &store->state_entries[existing];
    lc_pouch_free(&store->allocator, entry->content_type);
    lc_pouch_free(&store->allocator, entry->etag);
    entry->content_type = NULL;
    entry->etag = NULL;
  } else {
    if (store->state_entry_count == store->state_entry_capacity) {
      size_t new_capacity;

      new_capacity = store->state_entry_capacity == 0U
                         ? 16U
                         : store->state_entry_capacity * 2U;
      grown = (lc_pouch_disk_state_entry *)lc_pouch_realloc(
          &store->allocator, store->state_entries,
          new_capacity * sizeof(store->state_entries[0]));
      if (grown == NULL) {
        return 0;
      }
      memset(grown + store->state_entry_capacity, 0,
             (new_capacity - store->state_entry_capacity) * sizeof(grown[0]));
      store->state_entries = grown;
      store->state_entry_capacity = new_capacity;
    }
    entry = &store->state_entries[store->state_entry_count++];
    entry->namespace_name = lc_pouch_strdup(&store->allocator, namespace_name);
    entry->key = lc_pouch_strdup(&store->allocator, key);
    if (entry->namespace_name == NULL || entry->key == NULL) {
      return 0;
    }
  }
  entry->content_type = lc_pouch_strdup(&store->allocator, content_type);
  entry->etag = lc_pouch_strdup(&store->allocator, etag);
  if ((content_type != NULL && entry->content_type == NULL) ||
      (etag != NULL && entry->etag == NULL)) {
    return 0;
  }
  entry->version = version;
  entry->body_offset = body_offset;
  entry->body_length = body_length;
  entry->deleted = deleted;
  return 1;
}

static int lc_pouch_disk_find_meta_entry(lc_pouch_disk_store *store,
                                         const char *namespace_name,
                                         const char *key) {
  size_t index;

  for (index = 0U; index < store->meta_entry_count; ++index) {
    if (strcmp(store->meta_entries[index].namespace_name, namespace_name) ==
            0 &&
        strcmp(store->meta_entries[index].key, key) == 0) {
      return (int)index;
    }
  }
  return -1;
}

static int lc_pouch_meta_copy(const lc_pouch_allocator *allocator,
                              lc_pouch_meta *dst, const lc_pouch_meta *src) {
  memset(dst, 0, sizeof(*dst));
  if (src == NULL) {
    return 1;
  }
  dst->owner = lc_pouch_strdup(allocator, src->owner);
  dst->lease_id = lc_pouch_strdup(allocator, src->lease_id);
  dst->txn_id = lc_pouch_strdup(allocator, src->txn_id);
  dst->state_etag = lc_pouch_strdup(allocator, src->state_etag);
  if ((src->owner != NULL && dst->owner == NULL) ||
      (src->lease_id != NULL && dst->lease_id == NULL) ||
      (src->txn_id != NULL && dst->txn_id == NULL) ||
      (src->state_etag != NULL && dst->state_etag == NULL)) {
    lc_pouch_meta_cleanup(allocator, dst);
    return 0;
  }
  dst->version = src->version;
  dst->lease_expires_at_unix = src->lease_expires_at_unix;
  dst->fencing_token = src->fencing_token;
  dst->has_query_hidden = src->has_query_hidden;
  dst->query_hidden = src->query_hidden;
  return 1;
}

static void lc_pouch_disk_meta_entry_cleanup(lc_pouch_disk_store *store,
                                             lc_pouch_disk_meta_entry *entry) {
  lc_pouch_free(&store->allocator, entry->namespace_name);
  lc_pouch_free(&store->allocator, entry->key);
  lc_pouch_free(&store->allocator, entry->etag);
  lc_pouch_meta_cleanup(&store->allocator, &entry->meta);
  memset(entry, 0, sizeof(*entry));
}

static int lc_pouch_disk_upsert_meta_entry(lc_pouch_disk_store *store,
                                           const char *namespace_name,
                                           const char *key, const char *etag,
                                           const lc_pouch_meta *meta,
                                           int deleted) {
  lc_pouch_disk_meta_entry *entry;
  lc_pouch_disk_meta_entry *grown;
  lc_pouch_disk_meta_entry staged;
  int existing;
  int is_new;

  existing = lc_pouch_disk_find_meta_entry(store, namespace_name, key);
  is_new = existing < 0;
  memset(&staged, 0, sizeof(staged));
  staged.namespace_name = is_new ? lc_pouch_strdup(&store->allocator,
                                                   namespace_name)
                                 : NULL;
  staged.key = is_new ? lc_pouch_strdup(&store->allocator, key) : NULL;
  staged.etag = lc_pouch_strdup(&store->allocator, etag);
  if ((is_new && (staged.namespace_name == NULL || staged.key == NULL)) ||
      (etag != NULL && staged.etag == NULL) ||
      !lc_pouch_meta_copy(&store->allocator, &staged.meta, meta)) {
    lc_pouch_disk_meta_entry_cleanup(store, &staged);
    return 0;
  }
  staged.deleted = deleted;

  if (existing >= 0) {
    entry = &store->meta_entries[existing];
    lc_pouch_free(&store->allocator, entry->etag);
    entry->etag = NULL;
    lc_pouch_meta_cleanup(&store->allocator, &entry->meta);
    entry->etag = staged.etag;
    entry->meta = staged.meta;
    entry->deleted = staged.deleted;
    staged.etag = NULL;
    memset(&staged.meta, 0, sizeof(staged.meta));
    return 1;
  } else {
    if (!lc_pouch_disk_query_index_reserve(store)) {
      lc_pouch_disk_meta_entry_cleanup(store, &staged);
      return 0;
    }
    if (store->meta_entry_count == store->meta_entry_capacity) {
      size_t new_capacity;

      new_capacity = store->meta_entry_capacity == 0U
                         ? 16U
                         : store->meta_entry_capacity * 2U;
      grown = (lc_pouch_disk_meta_entry *)lc_pouch_realloc(
          &store->allocator, store->meta_entries,
          new_capacity * sizeof(store->meta_entries[0]));
      if (grown == NULL) {
        lc_pouch_disk_meta_entry_cleanup(store, &staged);
        return 0;
      }
      memset(grown + store->meta_entry_capacity, 0,
             (new_capacity - store->meta_entry_capacity) * sizeof(grown[0]));
      store->meta_entries = grown;
      store->meta_entry_capacity = new_capacity;
    }
    entry = &store->meta_entries[store->meta_entry_count++];
    *entry = staged;
    memset(&staged, 0, sizeof(staged));
  }
  if (is_new && !lc_pouch_disk_query_index_insert(
                    store, store->meta_entry_count - 1U)) {
    store->meta_entry_count--;
    lc_pouch_disk_meta_entry_cleanup(store, entry);
    return 0;
  }
  return 1;
}

static void lc_pouch_disk_meta_upsert_cleanup(
    lc_pouch_disk_store *store, lc_pouch_disk_meta_upsert *upsert) {
  lc_pouch_disk_meta_entry_cleanup(store, &upsert->entry);
  upsert->existing = -1;
  upsert->is_new = 0;
}

static int lc_pouch_disk_prepare_meta_upsert(
    lc_pouch_disk_store *store, const char *namespace_name, const char *key,
    const char *etag, const lc_pouch_meta *meta, int deleted,
    lc_pouch_disk_meta_upsert *upsert) {
  lc_pouch_disk_meta_entry *grown;

  memset(upsert, 0, sizeof(*upsert));
  upsert->existing = lc_pouch_disk_find_meta_entry(store, namespace_name, key);
  upsert->is_new = upsert->existing < 0;
  upsert->entry.namespace_name =
      upsert->is_new ? lc_pouch_strdup(&store->allocator, namespace_name) : NULL;
  upsert->entry.key =
      upsert->is_new ? lc_pouch_strdup(&store->allocator, key) : NULL;
  upsert->entry.etag = lc_pouch_strdup(&store->allocator, etag);
  if ((upsert->is_new &&
       (upsert->entry.namespace_name == NULL || upsert->entry.key == NULL)) ||
      (etag != NULL && upsert->entry.etag == NULL) ||
      !lc_pouch_meta_copy(&store->allocator, &upsert->entry.meta, meta)) {
    lc_pouch_disk_meta_upsert_cleanup(store, upsert);
    return 0;
  }
  upsert->entry.deleted = deleted;

  if (upsert->is_new) {
    if (!lc_pouch_disk_query_index_reserve(store)) {
      lc_pouch_disk_meta_upsert_cleanup(store, upsert);
      return 0;
    }
    if (store->meta_entry_count == store->meta_entry_capacity) {
      size_t new_capacity;

      new_capacity = store->meta_entry_capacity == 0U
                         ? 16U
                         : store->meta_entry_capacity * 2U;
      grown = (lc_pouch_disk_meta_entry *)lc_pouch_realloc(
          &store->allocator, store->meta_entries,
          new_capacity * sizeof(store->meta_entries[0]));
      if (grown == NULL) {
        lc_pouch_disk_meta_upsert_cleanup(store, upsert);
        return 0;
      }
      memset(grown + store->meta_entry_capacity, 0,
             (new_capacity - store->meta_entry_capacity) * sizeof(grown[0]));
      store->meta_entries = grown;
      store->meta_entry_capacity = new_capacity;
    }
  }
  return 1;
}

static int lc_pouch_disk_commit_meta_upsert(
    lc_pouch_disk_store *store, lc_pouch_disk_meta_upsert *upsert) {
  lc_pouch_disk_meta_entry *entry;

  if (!upsert->is_new) {
    entry = &store->meta_entries[upsert->existing];
    lc_pouch_free(&store->allocator, entry->etag);
    entry->etag = NULL;
    lc_pouch_meta_cleanup(&store->allocator, &entry->meta);
    entry->etag = upsert->entry.etag;
    entry->meta = upsert->entry.meta;
    entry->deleted = upsert->entry.deleted;
    upsert->entry.etag = NULL;
    memset(&upsert->entry.meta, 0, sizeof(upsert->entry.meta));
    return 1;
  }

  entry = &store->meta_entries[store->meta_entry_count++];
  *entry = upsert->entry;
  memset(&upsert->entry, 0, sizeof(upsert->entry));
  if (!lc_pouch_disk_query_index_insert(store, store->meta_entry_count - 1U)) {
    store->meta_entry_count--;
    lc_pouch_disk_meta_entry_cleanup(store, entry);
    return 0;
  }
  return 1;
}

static int lc_pouch_disk_meta_entry_ptr_compare(const void *left,
                                                const void *right) {
  const lc_pouch_disk_meta_entry *const *left_entry;
  const lc_pouch_disk_meta_entry *const *right_entry;

  left_entry = (const lc_pouch_disk_meta_entry *const *)left;
  right_entry = (const lc_pouch_disk_meta_entry *const *)right;
  return strcmp((*left_entry)->key, (*right_entry)->key);
}

static int lc_pouch_disk_meta_entry_compare_namespace_key(
    const lc_pouch_disk_meta_entry *entry, const char *namespace_name,
    const char *key) {
  int cmp;

  cmp = strcmp(entry->namespace_name, namespace_name);
  if (cmp != 0) {
    return cmp;
  }
  return strcmp(entry->key, key);
}

static int lc_pouch_disk_query_index_find(lc_pouch_disk_store *store,
                                          const char *namespace_name,
                                          const char *key, size_t *position) {
  size_t low;
  size_t high;

  low = 0U;
  high = store->query_meta_index_count;
  while (low < high) {
    size_t mid;
    size_t meta_index;
    int cmp;

    mid = low + ((high - low) / 2U);
    meta_index = store->query_meta_indices[mid];
    cmp = lc_pouch_disk_meta_entry_compare_namespace_key(
        &store->meta_entries[meta_index], namespace_name, key);
    if (cmp < 0) {
      low = mid + 1U;
    } else {
      high = mid;
    }
  }
  if (position != NULL) {
    *position = low;
  }
  if (low < store->query_meta_index_count) {
    size_t meta_index;

    meta_index = store->query_meta_indices[low];
    return lc_pouch_disk_meta_entry_compare_namespace_key(
               &store->meta_entries[meta_index], namespace_name, key) == 0;
  }
  return 0;
}

static int lc_pouch_disk_query_index_reserve(lc_pouch_disk_store *store) {
  size_t *grown;
  size_t new_capacity;

  if (store->query_meta_index_count < store->query_meta_index_capacity) {
    return 1;
  }
  new_capacity = store->query_meta_index_capacity == 0U
                     ? 16U
                     : store->query_meta_index_capacity * 2U;
  grown = (size_t *)lc_pouch_realloc(
      &store->allocator, store->query_meta_indices,
      new_capacity * sizeof(store->query_meta_indices[0]));
  if (grown == NULL) {
    return 0;
  }
  store->query_meta_indices = grown;
  store->query_meta_index_capacity = new_capacity;
  return 1;
}

static int lc_pouch_disk_query_index_insert(lc_pouch_disk_store *store,
                                            size_t meta_index) {
  size_t position;

  if (lc_pouch_disk_query_index_find(
          store, store->meta_entries[meta_index].namespace_name,
          store->meta_entries[meta_index].key, &position)) {
    return 1;
  }
  if (!lc_pouch_disk_query_index_reserve(store)) {
    return 0;
  }
  if (position < store->query_meta_index_count) {
    memmove(store->query_meta_indices + position + 1U,
            store->query_meta_indices + position,
            (store->query_meta_index_count - position) *
                sizeof(store->query_meta_indices[0]));
  }
  store->query_meta_indices[position] = meta_index;
  store->query_meta_index_count++;
  return 1;
}

static unsigned long lc_pouch_disk_index_sequence(
    const lc_pouch_disk_store *store) {
  unsigned long version_seq;

  if (store == NULL) {
    return 0UL;
  }
  version_seq = store->next_version > 1L
                    ? (unsigned long)(store->next_version - 1L)
                    : 0UL;
  return version_seq > store->replayed_record_count
             ? version_seq
             : store->replayed_record_count;
}

static int lc_pouch_disk_append_query_index_record(
    lc_pouch_disk_store *store, const char *namespace_name, const char *key,
    const char *etag, long version, const lc_pouch_meta *meta, int deleted,
    lc_error *error) {
  unsigned char header[LC_POUCH_QUERY_INDEX_HEADER_SIZE];
  unsigned long ns_len;
  unsigned long key_len;
  unsigned long etag_len;
  unsigned long payload_len;
  unsigned long flags;
  unsigned long crc;

  ns_len = (unsigned long)strlen(namespace_name);
  key_len = (unsigned long)strlen(key);
  etag_len = etag != NULL ? (unsigned long)strlen(etag) : 0UL;
  if (lc_pouch_disk_add_overflows(ns_len, key_len, &payload_len) ||
      lc_pouch_disk_add_overflows(payload_len, etag_len, &payload_len) ||
      !lc_pouch_disk_validate_record_lengths(ns_len, key_len, 0UL, etag_len,
                                             0UL, payload_len)) {
    return lc_pouch_set_invalid(error,
                                "pouch query index record exceeds limits");
  }

  flags = deleted ? LC_POUCH_QUERY_INDEX_FLAG_DELETED : 0UL;
  if (meta != NULL && meta->has_query_hidden) {
    flags |= LC_POUCH_QUERY_INDEX_FLAG_QUERY_HIDDEN_SET;
  }
  if (meta != NULL && meta->query_hidden) {
    flags |= LC_POUCH_QUERY_INDEX_FLAG_QUERY_HIDDEN;
  }

  memset(header, 0, sizeof(header));
  memcpy(header, LC_POUCH_QUERY_INDEX_MAGIC, 4U);
  lc_pouch_put_u32(header + 4, LC_POUCH_QUERY_INDEX_HEADER_SIZE);
  lc_pouch_put_u32(header + 8, LC_POUCH_QUERY_INDEX_RECORD_META);
  lc_pouch_put_u32(header + 12, ns_len);
  lc_pouch_put_u32(header + 16, key_len);
  lc_pouch_put_u32(header + 24, etag_len);
  lc_pouch_put_u64(header + 36, (unsigned long)version);
  lc_pouch_put_u64(header + 44, payload_len);
  lc_pouch_put_u32(header + 56, LC_POUCH_QUERY_INDEX_RECORD_VERSION);
  lc_pouch_put_u32(header + 60, flags);

  crc = 0xffffffffUL;
  crc = lc_pouch_crc32_update(crc, (const unsigned char *)namespace_name,
                              (size_t)ns_len);
  crc = lc_pouch_crc32_update(crc, (const unsigned char *)key, (size_t)key_len);
  if (etag_len > 0UL) {
    crc = lc_pouch_crc32_update(crc, (const unsigned char *)etag,
                                (size_t)etag_len);
  }
  lc_pouch_put_u32(header + 52, crc ^ 0xffffffffUL);

  if (lseek(store->query_index_fd, 0, SEEK_END) < 0) {
    return lc_pouch_set_errno(error, "failed to seek pouch query index");
  }
  if (!lc_pouch_write_all(store->query_index_fd, header, sizeof(header)) ||
      !lc_pouch_write_all(store->query_index_fd, namespace_name, ns_len) ||
      !lc_pouch_write_all(store->query_index_fd, key, key_len) ||
      (etag_len > 0UL &&
       !lc_pouch_write_all(store->query_index_fd, etag, etag_len))) {
    return lc_pouch_set_errno(error,
                              "failed to append pouch query index record");
  }
  if (fsync(store->query_index_fd) != 0) {
    return lc_pouch_set_errno(error, "failed to fsync pouch query index");
  }
  return LC_OK;
}

static int lc_pouch_disk_object_entry_ptr_compare(const void *left,
                                                  const void *right) {
  const lc_pouch_disk_object_entry *const *left_entry;
  const lc_pouch_disk_object_entry *const *right_entry;

  left_entry = (const lc_pouch_disk_object_entry *const *)left;
  right_entry = (const lc_pouch_disk_object_entry *const *)right;
  return strcmp((*left_entry)->name, (*right_entry)->name);
}

static int lc_pouch_disk_queue_entry_before(
    const lc_pouch_disk_queue_entry *candidate,
    const lc_pouch_disk_queue_entry *current) {
  if (current == NULL) {
    return 1;
  }
  if (candidate->enqueued_at_unix != current->enqueued_at_unix) {
    return candidate->enqueued_at_unix < current->enqueued_at_unix;
  }
  if (candidate->body_offset != current->body_offset) {
    return candidate->body_offset < current->body_offset;
  }
  return strcmp(candidate->message_id, current->message_id) < 0;
}

static void lc_pouch_disk_scan_meta_copy_cleanup(
    const lc_pouch_allocator *allocator, lc_pouch_disk_scan_meta_copy *copy) {
  if (copy == NULL) {
    return;
  }
  lc_pouch_free(allocator, copy->key);
  lc_pouch_free(allocator, copy->etag);
  lc_pouch_meta_cleanup(allocator, &copy->meta);
  memset(copy, 0, sizeof(*copy));
}

static int lc_pouch_disk_copy_meta_for_scan(
    lc_pouch_disk_store *store, lc_pouch_disk_scan_meta_copy *dst,
    const lc_pouch_disk_meta_entry *src) {
  memset(dst, 0, sizeof(*dst));
  dst->key = lc_pouch_strdup(&store->allocator, src->key);
  dst->etag = lc_pouch_strdup(&store->allocator, src->etag);
  if (dst->key == NULL || (src->etag != NULL && dst->etag == NULL) ||
      !lc_pouch_meta_copy(&store->allocator, &dst->meta, &src->meta)) {
    lc_pouch_disk_scan_meta_copy_cleanup(&store->allocator, dst);
    return 0;
  }
  return 1;
}

static int lc_pouch_encode_meta(lc_pouch_disk_store *store,
                                const lc_pouch_meta *meta, unsigned char **out,
                                size_t *out_length) {
  unsigned long owner_len;
  unsigned long lease_len;
  unsigned long txn_len;
  unsigned long state_etag_len;
  unsigned long flags;
  unsigned char *bytes;
  unsigned char *cursor;
  size_t length;

  owner_len = meta != NULL && meta->owner != NULL
                  ? (unsigned long)strlen(meta->owner)
                  : 0UL;
  lease_len = meta != NULL && meta->lease_id != NULL
                  ? (unsigned long)strlen(meta->lease_id)
                  : 0UL;
  txn_len = meta != NULL && meta->txn_id != NULL
                ? (unsigned long)strlen(meta->txn_id)
                : 0UL;
  state_etag_len = meta != NULL && meta->state_etag != NULL
                       ? (unsigned long)strlen(meta->state_etag)
                       : 0UL;
  length = 48U + (size_t)owner_len + (size_t)lease_len + (size_t)txn_len +
           (size_t)state_etag_len;
  bytes = (unsigned char *)lc_pouch_alloc(&store->allocator, length);
  if (bytes == NULL) {
    return 0;
  }
  flags = 0UL;
  if (meta != NULL && meta->has_query_hidden) {
    flags |= LC_POUCH_META_FLAG_QUERY_HIDDEN_SET;
  }
  if (meta != NULL && meta->query_hidden) {
    flags |= LC_POUCH_META_FLAG_QUERY_HIDDEN;
  }
  lc_pouch_put_u64(bytes, meta != NULL ? (unsigned long)meta->version : 0UL);
  lc_pouch_put_u64(bytes + 8, meta != NULL
                                  ? (unsigned long)meta->lease_expires_at_unix
                                  : 0UL);
  lc_pouch_put_u64(bytes + 16,
                   meta != NULL ? (unsigned long)meta->fencing_token : 0UL);
  lc_pouch_put_u64(bytes + 24, flags);
  lc_pouch_put_u32(bytes + 32, owner_len);
  lc_pouch_put_u32(bytes + 36, lease_len);
  lc_pouch_put_u32(bytes + 40, txn_len);
  lc_pouch_put_u32(bytes + 44, state_etag_len);
  cursor = bytes + 48;
  if (owner_len > 0UL) {
    memcpy(cursor, meta->owner, (size_t)owner_len);
    cursor += owner_len;
  }
  if (lease_len > 0UL) {
    memcpy(cursor, meta->lease_id, (size_t)lease_len);
    cursor += lease_len;
  }
  if (txn_len > 0UL) {
    memcpy(cursor, meta->txn_id, (size_t)txn_len);
    cursor += txn_len;
  }
  if (state_etag_len > 0UL) {
    memcpy(cursor, meta->state_etag, (size_t)state_etag_len);
  }
  *out = bytes;
  *out_length = length;
  return 1;
}

static int lc_pouch_decode_meta(lc_pouch_disk_store *store,
                                const unsigned char *bytes, size_t length,
                                lc_pouch_meta *out) {
  unsigned long owner_len;
  unsigned long lease_len;
  unsigned long txn_len;
  unsigned long state_etag_len;
  unsigned long flags;
  const unsigned char *cursor;

  if (length < 48U) {
    return 0;
  }
  owner_len = lc_pouch_get_u32(bytes + 32);
  lease_len = lc_pouch_get_u32(bytes + 36);
  txn_len = lc_pouch_get_u32(bytes + 40);
  state_etag_len = lc_pouch_get_u32(bytes + 44);
  if (48U + (size_t)owner_len + (size_t)lease_len + (size_t)txn_len +
          (size_t)state_etag_len !=
      length) {
    return 0;
  }
  memset(out, 0, sizeof(*out));
  out->version = (long)lc_pouch_get_u64(bytes);
  out->lease_expires_at_unix = (long)lc_pouch_get_u64(bytes + 8);
  out->fencing_token = (long)lc_pouch_get_u64(bytes + 16);
  flags = lc_pouch_get_u64(bytes + 24);
  out->has_query_hidden = (flags & LC_POUCH_META_FLAG_QUERY_HIDDEN_SET) != 0UL;
  out->query_hidden = (flags & LC_POUCH_META_FLAG_QUERY_HIDDEN) != 0UL;
  cursor = bytes + 48;
  out->owner = owner_len > 0UL ? lc_pouch_dup_bytes(&store->allocator, cursor,
                                                    (size_t)owner_len)
                               : NULL;
  cursor += owner_len;
  out->lease_id =
      lease_len > 0UL
          ? lc_pouch_dup_bytes(&store->allocator, cursor, (size_t)lease_len)
          : NULL;
  cursor += lease_len;
  out->txn_id = txn_len > 0UL ? lc_pouch_dup_bytes(&store->allocator, cursor,
                                                   (size_t)txn_len)
                              : NULL;
  cursor += txn_len;
  out->state_etag = state_etag_len > 0UL
                        ? lc_pouch_dup_bytes(&store->allocator, cursor,
                                             (size_t)state_etag_len)
                        : NULL;
  if ((owner_len > 0UL && out->owner == NULL) ||
      (lease_len > 0UL && out->lease_id == NULL) ||
      (txn_len > 0UL && out->txn_id == NULL) ||
      (state_etag_len > 0UL && out->state_etag == NULL)) {
    lc_pouch_meta_cleanup(&store->allocator, out);
    return 0;
  }
  return 1;
}

static int lc_pouch_disk_find_object_by_name(lc_pouch_disk_store *store,
                                             const char *namespace_name,
                                             const char *key,
                                             const char *name) {
  size_t index;

  for (index = 0U; index < store->object_entry_count; ++index) {
    if (!store->object_entries[index].deleted &&
        strcmp(store->object_entries[index].namespace_name, namespace_name) ==
            0 &&
        strcmp(store->object_entries[index].key, key) == 0 &&
        strcmp(store->object_entries[index].name, name) == 0) {
      return (int)index;
    }
  }
  return -1;
}

static int lc_pouch_disk_find_object(lc_pouch_disk_store *store,
                                     const char *namespace_name,
                                     const char *key,
                                     const lc_pouch_object_selector *selector) {
  size_t index;

  if (selector == NULL) {
    return -1;
  }
  for (index = 0U; index < store->object_entry_count; ++index) {
    if (store->object_entries[index].deleted ||
        strcmp(store->object_entries[index].namespace_name, namespace_name) !=
            0 ||
        strcmp(store->object_entries[index].key, key) != 0) {
      continue;
    }
    if (selector->id != NULL &&
        strcmp(store->object_entries[index].id, selector->id) == 0) {
      return (int)index;
    }
    if (selector->name != NULL &&
        strcmp(store->object_entries[index].name, selector->name) == 0) {
      return (int)index;
    }
  }
  return -1;
}

static void
lc_pouch_disk_object_entry_cleanup(lc_pouch_disk_store *store,
                                   lc_pouch_disk_object_entry *entry) {
  lc_pouch_free(&store->allocator, entry->namespace_name);
  lc_pouch_free(&store->allocator, entry->key);
  lc_pouch_free(&store->allocator, entry->id);
  lc_pouch_free(&store->allocator, entry->name);
  lc_pouch_free(&store->allocator, entry->content_type);
  memset(entry, 0, sizeof(*entry));
}

static int
lc_pouch_object_info_from_entry(const lc_pouch_allocator *allocator,
                                lc_pouch_object_info *dst,
                                const lc_pouch_disk_object_entry *entry) {
  memset(dst, 0, sizeof(*dst));
  dst->id = lc_pouch_strdup(allocator, entry->id);
  dst->name = lc_pouch_strdup(allocator, entry->name);
  dst->content_type = lc_pouch_strdup(allocator, entry->content_type);
  dst->plaintext_sha256 = lc_pouch_strdup(allocator, entry->id);
  if ((entry->id != NULL && dst->id == NULL) ||
      (entry->name != NULL && dst->name == NULL) ||
      (entry->content_type != NULL && dst->content_type == NULL) ||
      (entry->id != NULL && dst->plaintext_sha256 == NULL)) {
    lc_pouch_object_info_cleanup(allocator, dst);
    return 0;
  }
  dst->size = entry->size;
  dst->created_at_unix = entry->created_at_unix;
  dst->updated_at_unix = entry->updated_at_unix;
  return 1;
}

static int lc_pouch_disk_upsert_object_entry(
    lc_pouch_disk_store *store, const char *namespace_name, const char *key,
    const char *id, const char *name, const char *content_type, long size,
    long created_at_unix, long updated_at_unix, unsigned long body_offset,
    unsigned long body_length, int deleted) {
  lc_pouch_disk_object_entry *entry;
  lc_pouch_disk_object_entry *grown;
  int existing;

  existing =
      lc_pouch_disk_find_object_by_name(store, namespace_name, key, name);
  if (existing >= 0) {
    entry = &store->object_entries[existing];
    lc_pouch_free(&store->allocator, entry->id);
    lc_pouch_free(&store->allocator, entry->content_type);
    entry->id = NULL;
    entry->content_type = NULL;
  } else {
    if (store->object_entry_count == store->object_entry_capacity) {
      size_t new_capacity;

      new_capacity = store->object_entry_capacity == 0U
                         ? 16U
                         : store->object_entry_capacity * 2U;
      grown = (lc_pouch_disk_object_entry *)lc_pouch_realloc(
          &store->allocator, store->object_entries,
          new_capacity * sizeof(store->object_entries[0]));
      if (grown == NULL) {
        return 0;
      }
      memset(grown + store->object_entry_capacity, 0,
             (new_capacity - store->object_entry_capacity) * sizeof(grown[0]));
      store->object_entries = grown;
      store->object_entry_capacity = new_capacity;
    }
    entry = &store->object_entries[store->object_entry_count++];
    entry->namespace_name = lc_pouch_strdup(&store->allocator, namespace_name);
    entry->key = lc_pouch_strdup(&store->allocator, key);
    entry->name = lc_pouch_strdup(&store->allocator, name);
    if (entry->namespace_name == NULL || entry->key == NULL ||
        entry->name == NULL) {
      return 0;
    }
  }
  entry->id = lc_pouch_strdup(&store->allocator, id);
  entry->content_type = lc_pouch_strdup(&store->allocator, content_type);
  if ((id != NULL && entry->id == NULL) ||
      (content_type != NULL && entry->content_type == NULL)) {
    return 0;
  }
  entry->size = size;
  entry->created_at_unix = created_at_unix;
  entry->updated_at_unix = updated_at_unix;
  entry->body_offset = body_offset;
  entry->body_length = body_length;
  entry->deleted = deleted;
  return 1;
}

static char *lc_pouch_make_object_id_from_crc(lc_pouch_disk_store *store,
                                              const char *name,
                                              unsigned long crc) {
  char stack[128];

  snprintf(stack, sizeof(stack), "pouch-obj-%08lx-%s", crc,
           name != NULL ? name : "attachment");
  return lc_pouch_strdup(&store->allocator, stack);
}

static int lc_pouch_disk_find_queue_entry(lc_pouch_disk_store *store,
                                          const char *namespace_name,
                                          const char *queue,
                                          const char *message_id) {
  size_t index;

  for (index = 0U; index < store->queue_entry_count; ++index) {
    if (strcmp(store->queue_entries[index].namespace_name, namespace_name) ==
            0 &&
        strcmp(store->queue_entries[index].queue, queue) == 0 &&
        strcmp(store->queue_entries[index].message_id, message_id) == 0) {
      return (int)index;
    }
  }
  return -1;
}

static void
lc_pouch_disk_queue_entry_cleanup(lc_pouch_disk_store *store,
                                  lc_pouch_disk_queue_entry *entry) {
  lc_pouch_free(&store->allocator, entry->namespace_name);
  lc_pouch_free(&store->allocator, entry->queue);
  lc_pouch_free(&store->allocator, entry->message_id);
  lc_pouch_free(&store->allocator, entry->payload_content_type);
  lc_pouch_free(&store->allocator, entry->lease_id);
  lc_pouch_free(&store->allocator, entry->txn_id);
  lc_pouch_free(&store->allocator, entry->meta_etag);
  memset(entry, 0, sizeof(*entry));
}

static void lc_pouch_disk_reset_indexes(lc_pouch_disk_store *store) {
  size_t index;

  if (store == NULL) {
    return;
  }
  for (index = 0U; index < store->state_entry_count; ++index) {
    lc_pouch_disk_entry_cleanup(store, &store->state_entries[index]);
  }
  store->state_entry_count = 0U;
  for (index = 0U; index < store->meta_entry_count; ++index) {
    lc_pouch_disk_meta_entry_cleanup(store, &store->meta_entries[index]);
  }
  store->meta_entry_count = 0U;
  store->query_meta_index_count = 0U;
  for (index = 0U; index < store->object_entry_count; ++index) {
    lc_pouch_disk_object_entry_cleanup(store, &store->object_entries[index]);
  }
  store->object_entry_count = 0U;
  for (index = 0U; index < store->queue_entry_count; ++index) {
    lc_pouch_disk_queue_entry_cleanup(store, &store->queue_entries[index]);
  }
  store->queue_entry_count = 0U;
  store->next_version = 1L;
}

static int
lc_pouch_queue_info_from_entry(const lc_pouch_allocator *allocator,
                               lc_pouch_queue_message_info *dst,
                               const lc_pouch_disk_queue_entry *entry) {
  memset(dst, 0, sizeof(*dst));
  dst->namespace_name = lc_pouch_strdup(allocator, entry->namespace_name);
  dst->queue = lc_pouch_strdup(allocator, entry->queue);
  dst->message_id = lc_pouch_strdup(allocator, entry->message_id);
  dst->payload_content_type =
      lc_pouch_strdup(allocator, entry->payload_content_type);
  dst->lease_id = lc_pouch_strdup(allocator, entry->lease_id);
  dst->txn_id = lc_pouch_strdup(allocator, entry->txn_id);
  dst->meta_etag = lc_pouch_strdup(allocator, entry->meta_etag);
  if (dst->namespace_name == NULL || dst->queue == NULL ||
      dst->message_id == NULL ||
      (entry->payload_content_type != NULL &&
       dst->payload_content_type == NULL) ||
      (entry->lease_id != NULL && dst->lease_id == NULL) ||
      (entry->txn_id != NULL && dst->txn_id == NULL) ||
      (entry->meta_etag != NULL && dst->meta_etag == NULL)) {
    lc_pouch_queue_message_info_cleanup(allocator, dst);
    return 0;
  }
  dst->attempts = entry->attempts;
  dst->max_attempts = entry->max_attempts;
  dst->failure_attempts = entry->failure_attempts;
  dst->enqueued_at_unix = entry->enqueued_at_unix;
  dst->not_visible_until_unix = entry->not_visible_until_unix;
  dst->visibility_timeout_seconds = entry->visibility_timeout_seconds;
  dst->expires_at_unix = entry->expires_at_unix;
  dst->lease_expires_at_unix = entry->lease_expires_at_unix;
  dst->fencing_token = entry->fencing_token;
  dst->payload_bytes = (long)entry->body_length;
  return 1;
}

static int lc_pouch_disk_upsert_queue_entry(
    lc_pouch_disk_store *store, const char *namespace_name, const char *queue,
    const char *message_id, const char *content_type, const char *lease_id,
    const char *txn_id, const char *meta_etag, int attempts, int max_attempts,
    int failure_attempts, long enqueued_at_unix, long not_visible_until_unix,
    long visibility_timeout_seconds, long expires_at_unix,
    long lease_expires_at_unix, long fencing_token, unsigned long body_offset,
    unsigned long body_length, int has_body, int deleted) {
  lc_pouch_disk_queue_entry *entry;
  lc_pouch_disk_queue_entry *grown;
  int existing;

  existing =
      lc_pouch_disk_find_queue_entry(store, namespace_name, queue, message_id);
  if (existing >= 0) {
    char *new_content_type;
    char *new_lease_id;
    char *new_txn_id;
    char *new_meta_etag;

    entry = &store->queue_entries[existing];
    new_content_type = lc_pouch_strdup(&store->allocator, content_type);
    new_lease_id = lc_pouch_strdup(&store->allocator, lease_id);
    new_txn_id = lc_pouch_strdup(&store->allocator, txn_id);
    new_meta_etag = lc_pouch_strdup(&store->allocator, meta_etag);
    if ((content_type != NULL && new_content_type == NULL) ||
        (lease_id != NULL && new_lease_id == NULL) ||
        (txn_id != NULL && new_txn_id == NULL) ||
        (meta_etag != NULL && new_meta_etag == NULL)) {
      lc_pouch_free(&store->allocator, new_content_type);
      lc_pouch_free(&store->allocator, new_lease_id);
      lc_pouch_free(&store->allocator, new_txn_id);
      lc_pouch_free(&store->allocator, new_meta_etag);
      return 0;
    }
    lc_pouch_free(&store->allocator, entry->payload_content_type);
    lc_pouch_free(&store->allocator, entry->lease_id);
    lc_pouch_free(&store->allocator, entry->txn_id);
    lc_pouch_free(&store->allocator, entry->meta_etag);
    entry->payload_content_type = new_content_type;
    entry->lease_id = new_lease_id;
    entry->txn_id = new_txn_id;
    entry->meta_etag = new_meta_etag;
  } else {
    if (store->queue_entry_count == store->queue_entry_capacity) {
      size_t new_capacity;

      new_capacity = store->queue_entry_capacity == 0U
                         ? 16U
                         : store->queue_entry_capacity * 2U;
      grown = (lc_pouch_disk_queue_entry *)lc_pouch_realloc(
          &store->allocator, store->queue_entries,
          new_capacity * sizeof(store->queue_entries[0]));
      if (grown == NULL) {
        return 0;
      }
      memset(grown + store->queue_entry_capacity, 0,
             (new_capacity - store->queue_entry_capacity) * sizeof(grown[0]));
      store->queue_entries = grown;
      store->queue_entry_capacity = new_capacity;
    }
    entry = &store->queue_entries[store->queue_entry_count++];
    entry->namespace_name = lc_pouch_strdup(&store->allocator, namespace_name);
    entry->queue = lc_pouch_strdup(&store->allocator, queue);
    entry->message_id = lc_pouch_strdup(&store->allocator, message_id);
    if (entry->namespace_name == NULL || entry->queue == NULL ||
        entry->message_id == NULL) {
      return 0;
    }
    entry->payload_content_type =
        lc_pouch_strdup(&store->allocator, content_type);
    entry->lease_id = lc_pouch_strdup(&store->allocator, lease_id);
    entry->txn_id = lc_pouch_strdup(&store->allocator, txn_id);
    entry->meta_etag = lc_pouch_strdup(&store->allocator, meta_etag);
    if ((content_type != NULL && entry->payload_content_type == NULL) ||
        (lease_id != NULL && entry->lease_id == NULL) ||
        (txn_id != NULL && entry->txn_id == NULL) ||
        (meta_etag != NULL && entry->meta_etag == NULL)) {
      return 0;
    }
  }
  entry->attempts = attempts;
  entry->max_attempts = max_attempts;
  entry->failure_attempts = failure_attempts;
  entry->enqueued_at_unix = enqueued_at_unix;
  entry->not_visible_until_unix = not_visible_until_unix;
  entry->visibility_timeout_seconds = visibility_timeout_seconds;
  entry->expires_at_unix = expires_at_unix;
  entry->lease_expires_at_unix = lease_expires_at_unix;
  entry->fencing_token = fencing_token;
  if (has_body) {
    entry->body_offset = body_offset;
    entry->body_length = body_length;
  }
  entry->deleted = deleted;
  return 1;
}

static char *lc_pouch_make_queue_message_id(lc_pouch_disk_store *store,
                                            const char *queue, long sequence) {
  char stack[160];

  snprintf(stack, sizeof(stack), "pouch-msg-%ld-%ld-%s", (long)time(NULL),
           sequence, queue != NULL ? queue : "queue");
  return lc_pouch_strdup(&store->allocator, stack);
}

static char *lc_pouch_make_queue_lease_id(lc_pouch_disk_store *store,
                                          const char *message_id,
                                          long fencing_token) {
  char stack[192];

  snprintf(stack, sizeof(stack), "pouch-qlease-%ld-%s", fencing_token,
           message_id != NULL ? message_id : "message");
  return lc_pouch_strdup(&store->allocator, stack);
}

static char *lc_pouch_make_etag_from_crc(lc_pouch_disk_store *store,
                                         long version, unsigned long crc) {
  char stack[64];

  snprintf(stack, sizeof(stack), "pouch-%ld-%08lx", version, crc);
  return lc_pouch_strdup(&store->allocator, stack);
}

static char *lc_pouch_make_etag(lc_pouch_disk_store *store, long version,
                                const void *body, size_t body_length) {
  unsigned long crc;

  crc = lc_pouch_crc32((const unsigned char *)body, body_length);
  return lc_pouch_make_etag_from_crc(store, version, crc);
}

static int lc_pouch_check_cas(lc_pouch_disk_state_entry *entry,
                              const char *expected_etag, long if_version,
                              int has_if_version, lc_error *error) {
  if (expected_etag != NULL &&
      (entry == NULL || entry->deleted || entry->etag == NULL ||
       strcmp(entry->etag, expected_etag) != 0)) {
    return lc_error_set(error, LC_ERR_SERVER, 412L,
                        "pouch state etag precondition failed", NULL,
                        "precondition_failed", NULL);
  }
  if (has_if_version &&
      (entry == NULL || entry->deleted || entry->version != if_version)) {
    return lc_error_set(error, LC_ERR_SERVER, 412L,
                        "pouch state version precondition failed", NULL,
                        "precondition_failed", NULL);
  }
  return LC_OK;
}

static int lc_pouch_check_meta_cas(lc_pouch_disk_meta_entry *entry,
                                   const char *expected_etag, lc_error *error) {
  if (expected_etag != NULL &&
      (entry == NULL || entry->deleted || entry->etag == NULL ||
       strcmp(entry->etag, expected_etag) != 0)) {
    return lc_error_set(error, LC_ERR_SERVER, 412L,
                        "pouch metadata etag precondition failed", NULL,
                        "precondition_failed", NULL);
  }
  return LC_OK;
}

static int lc_pouch_read_source_all(const lc_pouch_allocator *allocator,
                                    lc_source *source, unsigned char **out,
                                    size_t *out_length, lc_error *error) {
  unsigned char *buffer;
  unsigned char temp[8192];
  size_t capacity;
  size_t length;
  size_t got;

  buffer = NULL;
  capacity = 0U;
  length = 0U;
  while (1) {
    got = source->read(source, temp, sizeof(temp), error);
    if (got == 0U) {
      break;
    }
    if (got > ((size_t)-1) - length) {
      lc_pouch_free(allocator, buffer);
      return lc_pouch_set_invalid(error, "pouch state payload is too large");
    }
    if ((unsigned long)(length + got) > LC_POUCH_MAX_INLINE_BODY_BYTES) {
      lc_pouch_free(allocator, buffer);
      return lc_pouch_set_invalid(error,
                                  "pouch inline payload exceeds storage limit");
    }
    if (length + got > capacity) {
      size_t new_capacity;
      unsigned char *grown;

      new_capacity = capacity == 0U ? 8192U : capacity;
      while (new_capacity < length + got) {
        if (new_capacity > ((size_t)-1) / 2U) {
          lc_pouch_free(allocator, buffer);
          return lc_pouch_set_invalid(error,
                                      "pouch state payload is too large");
        }
        new_capacity *= 2U;
      }
      grown =
          (unsigned char *)lc_pouch_realloc(allocator, buffer, new_capacity);
      if (grown == NULL) {
        lc_pouch_free(allocator, buffer);
        return lc_pouch_set_nomem(error, "failed to allocate pouch payload");
      }
      buffer = grown;
      capacity = new_capacity;
    }
    memcpy(buffer + length, temp, got);
    length += got;
  }
  *out = buffer;
  *out_length = length;
  return LC_OK;
}

static int lc_pouch_encode_queue_record(
    lc_pouch_disk_store *store, const lc_pouch_disk_queue_entry *entry,
    const unsigned char *payload, size_t payload_length,
    unsigned char **out_body, size_t *out_body_length, lc_error *error) {
  const char *content_type;
  const char *lease_id;
  const char *txn_id;
  size_t content_type_length;
  size_t lease_id_length;
  size_t txn_id_length;
  size_t body_length;
  unsigned char *body;

  content_type = entry->payload_content_type != NULL
                     ? entry->payload_content_type
                     : "application/octet-stream";
  lease_id = entry->lease_id != NULL ? entry->lease_id : "";
  txn_id = entry->txn_id != NULL ? entry->txn_id : "";
  content_type_length = strlen(content_type);
  lease_id_length = strlen(lease_id);
  txn_id_length = strlen(txn_id);
  body_length = LC_POUCH_QUEUE_META_SIZE + content_type_length +
                lease_id_length + txn_id_length + payload_length;
  body = (unsigned char *)lc_pouch_alloc(&store->allocator, body_length);
  if (body == NULL) {
    return lc_pouch_set_nomem(error, "failed to allocate pouch queue record");
  }
  lc_pouch_put_u32(body, entry->deleted ? 1UL : 0UL);
  lc_pouch_put_u32(body + 4, (unsigned long)entry->attempts);
  lc_pouch_put_u32(body + 8, (unsigned long)entry->max_attempts);
  lc_pouch_put_u32(body + 12, (unsigned long)entry->failure_attempts);
  lc_pouch_put_u64(body + 16, (unsigned long)entry->enqueued_at_unix);
  lc_pouch_put_u64(body + 24, (unsigned long)entry->not_visible_until_unix);
  lc_pouch_put_u64(body + 32, (unsigned long)entry->visibility_timeout_seconds);
  lc_pouch_put_u64(body + 40, (unsigned long)entry->expires_at_unix);
  lc_pouch_put_u64(body + 48, (unsigned long)entry->lease_expires_at_unix);
  lc_pouch_put_u64(body + 56, (unsigned long)payload_length);
  lc_pouch_put_u64(body + 64, (unsigned long)content_type_length);
  lc_pouch_put_u64(body + 72, (unsigned long)lease_id_length);
  lc_pouch_put_u64(body + 80, (unsigned long)txn_id_length);
  memcpy(body + LC_POUCH_QUEUE_META_SIZE, content_type, content_type_length);
  memcpy(body + LC_POUCH_QUEUE_META_SIZE + content_type_length, lease_id,
         lease_id_length);
  memcpy(body + LC_POUCH_QUEUE_META_SIZE + content_type_length +
             lease_id_length,
         txn_id, txn_id_length);
  if (payload_length > 0U) {
    memcpy(body + LC_POUCH_QUEUE_META_SIZE + content_type_length +
               lease_id_length + txn_id_length,
           payload, payload_length);
  }
  *out_body = body;
  *out_body_length = body_length;
  return LC_OK;
}

static int lc_pouch_decode_queue_record(
    lc_pouch_disk_store *store, const unsigned char *body,
    unsigned long body_len, int *deleted, int *attempts, int *max_attempts,
    int *failure_attempts, long *enqueued_at_unix, long *not_visible_until_unix,
    long *visibility_timeout_seconds, long *expires_at_unix,
    long *lease_expires_at_unix, unsigned long *payload_len,
    char **content_type, char **lease_id, char **txn_id,
    unsigned long *payload_relative_offset) {
  unsigned long content_type_len;
  unsigned long lease_id_len;
  unsigned long txn_id_len;
  unsigned long needed;
  const unsigned char *cursor;

  if (body_len < LC_POUCH_QUEUE_META_SIZE) {
    return 0;
  }
  *deleted = lc_pouch_get_u32(body) != 0UL;
  *attempts = (int)lc_pouch_get_u32(body + 4);
  *max_attempts = (int)lc_pouch_get_u32(body + 8);
  *failure_attempts = (int)lc_pouch_get_u32(body + 12);
  *enqueued_at_unix = (long)lc_pouch_get_u64(body + 16);
  *not_visible_until_unix = (long)lc_pouch_get_u64(body + 24);
  *visibility_timeout_seconds = (long)lc_pouch_get_u64(body + 32);
  *expires_at_unix = (long)lc_pouch_get_u64(body + 40);
  *lease_expires_at_unix = (long)lc_pouch_get_u64(body + 48);
  *payload_len = lc_pouch_get_u64(body + 56);
  content_type_len = lc_pouch_get_u64(body + 64);
  lease_id_len = lc_pouch_get_u64(body + 72);
  txn_id_len = lc_pouch_get_u64(body + 80);
  needed = LC_POUCH_QUEUE_META_SIZE + content_type_len + lease_id_len +
           txn_id_len + *payload_len;
  if (needed != body_len) {
    return 0;
  }
  cursor = body + LC_POUCH_QUEUE_META_SIZE;
  *content_type = content_type_len > 0UL
                      ? lc_pouch_dup_bytes(&store->allocator, cursor,
                                           (size_t)content_type_len)
                      : lc_pouch_strdup(&store->allocator, "");
  cursor += content_type_len;
  *lease_id = lease_id_len > 0UL ? lc_pouch_dup_bytes(&store->allocator, cursor,
                                                      (size_t)lease_id_len)
                                 : NULL;
  cursor += lease_id_len;
  *txn_id = txn_id_len > 0UL ? lc_pouch_dup_bytes(&store->allocator, cursor,
                                                  (size_t)txn_id_len)
                             : NULL;
  if (*content_type == NULL || (lease_id_len > 0UL && *lease_id == NULL) ||
      (txn_id_len > 0UL && *txn_id == NULL)) {
    lc_pouch_free(&store->allocator, *content_type);
    lc_pouch_free(&store->allocator, *lease_id);
    lc_pouch_free(&store->allocator, *txn_id);
    *content_type = NULL;
    *lease_id = NULL;
    *txn_id = NULL;
    return 0;
  }
  *payload_relative_offset =
      LC_POUCH_QUEUE_META_SIZE + content_type_len + lease_id_len + txn_id_len;
  return 1;
}

static int lc_pouch_disk_load_meta(lc_pouch_store *self,
                                   const char *namespace_name, const char *key,
                                   lc_pouch_meta_record *out, lc_error *error) {
  lc_pouch_disk_store *store;
  lc_pouch_disk_meta_entry *entry;
  int index;

  if (self == NULL || namespace_name == NULL || key == NULL || out == NULL) {
    return lc_pouch_set_invalid(error,
                                "load_meta requires store, namespace, key, "
                                "and output metadata");
  }
  index = lc_pouch_disk_validate_namespace_key(error, "load_meta",
                                               namespace_name, key);
  if (index != LC_OK) {
    return index;
  }
  store = (lc_pouch_disk_store *)self->impl;
  memset(out, 0, sizeof(*out));
  index = lc_pouch_disk_lock(store, error);
  if (index != LC_OK) {
    return index;
  }
  index = lc_pouch_disk_find_meta_entry(store, namespace_name, key);
  if (index < 0 || store->meta_entries[index].deleted) {
    lc_pouch_disk_unlock(store, error);
    return LC_OK;
  }
  entry = &store->meta_entries[index];
  out->namespace_name = lc_pouch_strdup(&store->allocator, namespace_name);
  out->key = lc_pouch_strdup(&store->allocator, key);
  out->etag = lc_pouch_strdup(&store->allocator, entry->etag);
  if (out->namespace_name == NULL || out->key == NULL ||
      (entry->etag != NULL && out->etag == NULL) ||
      !lc_pouch_meta_copy(&store->allocator, &out->meta, &entry->meta)) {
    lc_pouch_meta_record_cleanup(&store->allocator, out);
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_nomem(error, "failed to copy pouch metadata");
  }
  out->found = 1;
  if (lc_pouch_disk_unlock(store, error) != LC_OK) {
    lc_pouch_meta_record_cleanup(&store->allocator, out);
    return LC_ERR_TRANSPORT;
  }
  return LC_OK;
}

static int lc_pouch_disk_store_meta(lc_pouch_store *self,
                                    const char *namespace_name, const char *key,
                                    const lc_pouch_meta *meta,
                                    const char *expected_etag,
                                    lc_pouch_store_meta_res *out,
                                    lc_error *error) {
  lc_pouch_disk_store *store;
  lc_pouch_disk_meta_entry *entry;
  unsigned char *payload;
  size_t payload_length;
  char *etag;
  unsigned long body_offset;
  lc_pouch_disk_meta_upsert upsert;
  int index;
  int rc;

  if (self == NULL || namespace_name == NULL || key == NULL || meta == NULL ||
      out == NULL) {
    return lc_pouch_set_invalid(error,
                                "store_meta requires store, namespace, key, "
                                "metadata, and output metadata");
  }
  rc = lc_pouch_disk_validate_namespace_key(error, "store_meta",
                                           namespace_name, key);
  if (rc != LC_OK) {
    return rc;
  }
  store = (lc_pouch_disk_store *)self->impl;
  memset(out, 0, sizeof(*out));
  rc = lc_pouch_disk_lock(store, error);
  if (rc != LC_OK) {
    return rc;
  }
  index = lc_pouch_disk_find_meta_entry(store, namespace_name, key);
  entry = index >= 0 ? &store->meta_entries[index] : NULL;
  rc = lc_pouch_check_meta_cas(entry, expected_etag, error);
  if (rc != LC_OK) {
    lc_pouch_disk_unlock(store, error);
    return rc;
  }
  payload = NULL;
  payload_length = 0U;
  memset(&upsert, 0, sizeof(upsert));
  if (!lc_pouch_encode_meta(store, meta, &payload, &payload_length)) {
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_nomem(error, "failed to encode pouch metadata");
  }
  etag = lc_pouch_make_etag(store, meta->version, payload, payload_length);
  if (etag == NULL) {
    lc_pouch_free(&store->allocator, payload);
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_nomem(error, "failed to allocate pouch metadata etag");
  }
  if (!lc_pouch_disk_prepare_meta_upsert(store, namespace_name, key, etag, meta,
                                         0, &upsert)) {
    lc_pouch_free(&store->allocator, etag);
    lc_pouch_free(&store->allocator, payload);
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_nomem(error, "failed to update pouch metadata index");
  }
  rc = lc_pouch_disk_append_query_index_record(
      store, namespace_name, key, etag, meta->version, meta, 0, error);
  if (rc == LC_OK) {
    rc = lc_pouch_disk_append_record(
        store, LC_POUCH_RECORD_META_PUT, namespace_name, key, NULL, etag,
        meta->version, payload, payload_length, &body_offset, error);
  }
  if (rc == LC_OK && !lc_pouch_disk_commit_meta_upsert(store, &upsert)) {
    rc = lc_pouch_set_nomem(error, "failed to update pouch metadata index");
  }
  if (rc == LC_OK && meta->version >= store->next_version) {
    store->next_version = meta->version + 1L;
  }
  if (rc == LC_OK) {
    out->etag = lc_pouch_strdup(&store->allocator, etag);
    out->version = meta->version;
    if (out->etag == NULL) {
      rc = lc_pouch_set_nomem(error, "failed to copy pouch metadata etag");
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_disk_mark_replayed_to_current_size(store, error);
  }
  lc_pouch_free(&store->allocator, etag);
  lc_pouch_free(&store->allocator, payload);
  lc_pouch_disk_meta_upsert_cleanup(store, &upsert);
  if (lc_pouch_disk_unlock(store, error) != LC_OK && rc == LC_OK) {
    rc = LC_ERR_TRANSPORT;
  }
  return rc;
}

static int lc_pouch_disk_delete_meta(lc_pouch_store *self,
                                     const char *namespace_name,
                                     const char *key, const char *expected_etag,
                                     lc_error *error) {
  lc_pouch_disk_store *store;
  lc_pouch_disk_meta_entry *entry;
  char *etag;
  unsigned long body_offset;
  long version;
  int index;
  int rc;

  if (self == NULL || namespace_name == NULL || key == NULL) {
    return lc_pouch_set_invalid(error,
                                "delete_meta requires store, namespace, key");
  }
  rc = lc_pouch_disk_validate_namespace_key(error, "delete_meta",
                                           namespace_name, key);
  if (rc != LC_OK) {
    return rc;
  }
  store = (lc_pouch_disk_store *)self->impl;
  rc = lc_pouch_disk_lock(store, error);
  if (rc != LC_OK) {
    return rc;
  }
  index = lc_pouch_disk_find_meta_entry(store, namespace_name, key);
  entry = index >= 0 ? &store->meta_entries[index] : NULL;
  rc = lc_pouch_check_meta_cas(entry, expected_etag, error);
  if (rc != LC_OK) {
    lc_pouch_disk_unlock(store, error);
    return rc;
  }
  version = store->next_version++;
  etag = lc_pouch_make_etag(store, version, NULL, 0U);
  if (etag == NULL) {
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_nomem(error, "failed to allocate pouch metadata etag");
  }
  rc = lc_pouch_disk_append_query_index_record(store, namespace_name, key, etag,
                                              version, NULL, 1, error);
  if (rc == LC_OK) {
    rc = lc_pouch_disk_append_record(store, LC_POUCH_RECORD_META_REMOVE,
                                     namespace_name, key, NULL, etag, version,
                                     NULL, 0U, &body_offset, error);
  }
  if (rc == LC_OK && !lc_pouch_disk_upsert_meta_entry(store, namespace_name,
                                                      key, etag, NULL, 1)) {
    rc = lc_pouch_set_nomem(error, "failed to update pouch metadata index");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_disk_mark_replayed_to_current_size(store, error);
  }
  lc_pouch_free(&store->allocator, etag);
  if (lc_pouch_disk_unlock(store, error) != LC_OK && rc == LC_OK) {
    rc = LC_ERR_TRANSPORT;
  }
  return rc;
}

static int lc_pouch_disk_scan_meta(lc_pouch_store *self,
                                   const lc_pouch_scan_meta_req *req,
                                   lc_pouch_scan_meta_visit_fn visit,
                                   void *visit_context,
                                   lc_pouch_scan_meta_res *out,
                                   lc_error *error) {
  lc_pouch_disk_store *store;
  lc_pouch_disk_meta_entry **matches;
  lc_pouch_disk_scan_meta_copy *rows;
  size_t match_count;
  size_t visit_count;
  size_t index;
  size_t row_index;
  int rc;

  if (self == NULL || req == NULL || req->namespace_name == NULL ||
      visit == NULL || out == NULL) {
    return lc_pouch_set_invalid(error,
                                "scan_meta requires store, request, "
                                "namespace, visitor, and output");
  }
  rc = lc_pouch_disk_validate_name(error, "scan_meta", "namespace",
                                   req->namespace_name);
  if (rc != LC_OK) {
    return rc;
  }
  store = (lc_pouch_disk_store *)self->impl;
  memset(out, 0, sizeof(*out));
  matches = NULL;
  rows = NULL;
  match_count = 0U;
  visit_count = 0U;

  rc = lc_pouch_disk_lock(store, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_disk_force_replay_locked(store, error);
  if (rc != LC_OK) {
    lc_pouch_disk_unlock(store, error);
    return rc;
  }

  if (store->meta_entry_count > 0U) {
    matches = (lc_pouch_disk_meta_entry **)lc_pouch_alloc(
        &store->allocator,
        store->meta_entry_count * sizeof(lc_pouch_disk_meta_entry *));
    if (matches == NULL) {
      lc_pouch_disk_unlock(store, error);
      return lc_pouch_set_nomem(error,
                                "failed to allocate pouch metadata scan");
    }
  }
  for (index = 0U; index < store->meta_entry_count; ++index) {
    lc_pouch_disk_meta_entry *entry;

    entry = &store->meta_entries[index];
    if (entry->deleted ||
        strcmp(entry->namespace_name, req->namespace_name) != 0 ||
        (entry->meta.has_query_hidden && entry->meta.query_hidden)) {
      continue;
    }
    if (req->start_after != NULL &&
        strcmp(entry->key, req->start_after) <= 0) {
      continue;
    }
    matches[match_count++] = entry;
  }
  if (match_count > 1U) {
    qsort(matches, match_count, sizeof(matches[0]),
          lc_pouch_disk_meta_entry_ptr_compare);
  }

  visit_count = match_count;
  if (req->limit > 0U && visit_count > req->limit) {
    visit_count = req->limit;
    out->truncated = 1;
  }
  if (visit_count > 0U) {
    rows = (lc_pouch_disk_scan_meta_copy *)lc_pouch_calloc(
        &store->allocator, visit_count, sizeof(rows[0]));
    if (rows == NULL) {
      lc_pouch_free(&store->allocator, matches);
      lc_pouch_disk_unlock(store, error);
      return lc_pouch_set_nomem(error,
                                "failed to allocate pouch metadata scan rows");
    }
  }
  for (row_index = 0U; row_index < visit_count; ++row_index) {
    if (!lc_pouch_disk_copy_meta_for_scan(store, &rows[row_index],
                                          matches[row_index])) {
      for (index = 0U; index < row_index; ++index) {
        lc_pouch_disk_scan_meta_copy_cleanup(&store->allocator, &rows[index]);
      }
      lc_pouch_free(&store->allocator, rows);
      lc_pouch_free(&store->allocator, matches);
      lc_pouch_disk_unlock(store, error);
      return lc_pouch_set_nomem(error,
                                "failed to copy pouch metadata scan row");
    }
  }
  if (out->truncated && visit_count > 0U) {
    out->next_start_after =
        lc_pouch_strdup(&store->allocator, rows[visit_count - 1U].key);
    if (out->next_start_after == NULL) {
      for (index = 0U; index < visit_count; ++index) {
        lc_pouch_disk_scan_meta_copy_cleanup(&store->allocator, &rows[index]);
      }
      lc_pouch_free(&store->allocator, rows);
      lc_pouch_free(&store->allocator, matches);
      lc_pouch_disk_unlock(store, error);
      return lc_pouch_set_nomem(error,
                                "failed to allocate pouch scan cursor");
    }
  }
  lc_pouch_free(&store->allocator, matches);
  rc = lc_pouch_disk_unlock(store, error);
  if (rc != LC_OK) {
    for (index = 0U; index < visit_count; ++index) {
      lc_pouch_disk_scan_meta_copy_cleanup(&store->allocator, &rows[index]);
    }
    lc_pouch_free(&store->allocator, rows);
    lc_pouch_scan_meta_res_cleanup(&store->allocator, out);
    return rc;
  }

  for (row_index = 0U; row_index < visit_count; ++row_index) {
    lc_pouch_scan_meta_row row;

    memset(&row, 0, sizeof(row));
    row.key = rows[row_index].key;
    row.etag = rows[row_index].etag;
    row.meta = &rows[row_index].meta;
    rc = visit(visit_context, &row, error);
    if (rc != LC_OK) {
      for (index = 0U; index < visit_count; ++index) {
        lc_pouch_disk_scan_meta_copy_cleanup(&store->allocator, &rows[index]);
      }
      lc_pouch_free(&store->allocator, rows);
      lc_pouch_scan_meta_res_cleanup(&store->allocator, out);
      return rc;
    }
    out->visited++;
  }

  for (index = 0U; index < visit_count; ++index) {
    lc_pouch_disk_scan_meta_copy_cleanup(&store->allocator, &rows[index]);
  }
  lc_pouch_free(&store->allocator, rows);
  return LC_OK;
}

static int lc_pouch_disk_query_index_scan(
    lc_pouch_store *self, const lc_pouch_query_index_scan_req *req,
    lc_pouch_scan_meta_visit_fn visit, void *visit_context,
    lc_pouch_query_index_scan_res *out, lc_error *error) {
  lc_pouch_disk_store *store;
  lc_pouch_disk_scan_meta_copy *rows;
  const char *start_key;
  size_t start_index;
  size_t visit_count;
  size_t index;
  size_t row_index;
  int rc;

  if (self == NULL || req == NULL || req->namespace_name == NULL ||
      visit == NULL || out == NULL) {
    return lc_pouch_set_invalid(error,
                                "query_index_scan requires store, request, "
                                "namespace, visitor, and output");
  }
  rc = lc_pouch_disk_validate_name(error, "query_index_scan", "namespace",
                                   req->namespace_name);
  if (rc != LC_OK) {
    return rc;
  }
  store = (lc_pouch_disk_store *)self->impl;
  memset(out, 0, sizeof(*out));
  rows = NULL;
  start_index = 0U;
  visit_count = 0U;

  rc = lc_pouch_disk_lock(store, error);
  if (rc != LC_OK) {
    return rc;
  }

  start_key = req->start_after != NULL ? req->start_after : "";
  if (lc_pouch_disk_query_index_find(store, req->namespace_name, start_key,
                                     &start_index) &&
      req->start_after != NULL) {
    start_index++;
  }

  for (index = start_index; index < store->query_meta_index_count; ++index) {
    lc_pouch_disk_meta_entry *entry;
    int namespace_cmp;

    entry = &store->meta_entries[store->query_meta_indices[index]];
    namespace_cmp = strcmp(entry->namespace_name, req->namespace_name);
    if (namespace_cmp > 0) {
      break;
    }
    if (namespace_cmp < 0) {
      continue;
    }
    if (entry->deleted ||
        (entry->meta.has_query_hidden && entry->meta.query_hidden)) {
      continue;
    }
    if (req->limit > 0U && visit_count == req->limit) {
      out->truncated = 1;
      break;
    }
    visit_count++;
  }

  if (visit_count > 0U) {
    rows = (lc_pouch_disk_scan_meta_copy *)lc_pouch_calloc(
        &store->allocator, visit_count, sizeof(rows[0]));
    if (rows == NULL) {
      lc_pouch_disk_unlock(store, error);
      return lc_pouch_set_nomem(error,
                                "failed to allocate pouch query index rows");
    }
  }
  row_index = 0U;
  for (index = start_index; index < store->query_meta_index_count &&
                        row_index < visit_count;
       ++index) {
    lc_pouch_disk_meta_entry *entry;
    int namespace_cmp;

    entry = &store->meta_entries[store->query_meta_indices[index]];
    namespace_cmp = strcmp(entry->namespace_name, req->namespace_name);
    if (namespace_cmp > 0) {
      break;
    }
    if (namespace_cmp < 0) {
      continue;
    }
    if (entry->deleted ||
        (entry->meta.has_query_hidden && entry->meta.query_hidden)) {
      continue;
    }
    if (!lc_pouch_disk_copy_meta_for_scan(store, &rows[row_index], entry)) {
      for (index = 0U; index < row_index; ++index) {
        lc_pouch_disk_scan_meta_copy_cleanup(&store->allocator, &rows[index]);
      }
      lc_pouch_free(&store->allocator, rows);
      lc_pouch_disk_unlock(store, error);
      return lc_pouch_set_nomem(error,
                                "failed to copy pouch query index row");
    }
    row_index++;
  }
  if (out->truncated && visit_count > 0U) {
    out->next_start_after =
        lc_pouch_strdup(&store->allocator, rows[visit_count - 1U].key);
    if (out->next_start_after == NULL) {
      for (index = 0U; index < visit_count; ++index) {
        lc_pouch_disk_scan_meta_copy_cleanup(&store->allocator, &rows[index]);
      }
      lc_pouch_free(&store->allocator, rows);
      lc_pouch_disk_unlock(store, error);
      return lc_pouch_set_nomem(error,
                                "failed to allocate pouch query index cursor");
    }
  }
  out->index_seq = lc_pouch_disk_index_sequence(store);

  rc = lc_pouch_disk_unlock(store, error);
  if (rc != LC_OK) {
    for (index = 0U; index < visit_count; ++index) {
      lc_pouch_disk_scan_meta_copy_cleanup(&store->allocator, &rows[index]);
    }
    lc_pouch_free(&store->allocator, rows);
    lc_pouch_query_index_scan_res_cleanup(&store->allocator, out);
    return rc;
  }

  for (row_index = 0U; row_index < visit_count; ++row_index) {
    lc_pouch_scan_meta_row row;

    memset(&row, 0, sizeof(row));
    row.key = rows[row_index].key;
    row.etag = rows[row_index].etag;
    row.meta = &rows[row_index].meta;
    rc = visit(visit_context, &row, error);
    if (rc != LC_OK) {
      for (index = 0U; index < visit_count; ++index) {
        lc_pouch_disk_scan_meta_copy_cleanup(&store->allocator, &rows[index]);
      }
      lc_pouch_free(&store->allocator, rows);
      lc_pouch_query_index_scan_res_cleanup(&store->allocator, out);
      return rc;
    }
    out->visited++;
  }

  for (index = 0U; index < visit_count; ++index) {
    lc_pouch_disk_scan_meta_copy_cleanup(&store->allocator, &rows[index]);
  }
  lc_pouch_free(&store->allocator, rows);
  return LC_OK;
}

static int lc_pouch_disk_query_index_keys_scan(
    lc_pouch_store *self, const lc_pouch_query_index_scan_req *req,
    lc_pouch_query_index_key_visit_fn visit, void *visit_context,
    lc_pouch_query_index_scan_res *out, lc_error *error) {
  lc_pouch_disk_store *store;
  char **keys;
  const char *start_key;
  size_t start_index;
  size_t visit_count;
  size_t index;
  size_t row_index;
  int rc;

  if (self == NULL || req == NULL || req->namespace_name == NULL ||
      visit == NULL || out == NULL) {
    return lc_pouch_set_invalid(
        error,
        "query_index_keys_scan requires store, request, namespace, visitor, "
        "and output");
  }
  rc = lc_pouch_disk_validate_name(error, "query_index_keys_scan",
                                   "namespace", req->namespace_name);
  if (rc != LC_OK) {
    return rc;
  }
  store = (lc_pouch_disk_store *)self->impl;
  memset(out, 0, sizeof(*out));
  keys = NULL;
  start_index = 0U;
  visit_count = 0U;

  rc = lc_pouch_disk_lock(store, error);
  if (rc != LC_OK) {
    return rc;
  }

  start_key = req->start_after != NULL ? req->start_after : "";
  if (lc_pouch_disk_query_index_find(store, req->namespace_name, start_key,
                                     &start_index) &&
      req->start_after != NULL) {
    start_index++;
  }

  for (index = start_index; index < store->query_meta_index_count; ++index) {
    lc_pouch_disk_meta_entry *entry;
    int namespace_cmp;

    entry = &store->meta_entries[store->query_meta_indices[index]];
    namespace_cmp = strcmp(entry->namespace_name, req->namespace_name);
    if (namespace_cmp > 0) {
      break;
    }
    if (namespace_cmp < 0) {
      continue;
    }
    if (entry->deleted ||
        (entry->meta.has_query_hidden && entry->meta.query_hidden)) {
      continue;
    }
    if (req->limit > 0U && visit_count == req->limit) {
      out->truncated = 1;
      break;
    }
    visit_count++;
  }

  if (visit_count > 0U) {
    keys = (char **)lc_pouch_calloc(&store->allocator, visit_count,
                                    sizeof(keys[0]));
    if (keys == NULL) {
      lc_pouch_disk_unlock(store, error);
      return lc_pouch_set_nomem(error,
                                "failed to allocate pouch query index keys");
    }
  }
  row_index = 0U;
  for (index = start_index; index < store->query_meta_index_count &&
                        row_index < visit_count;
       ++index) {
    lc_pouch_disk_meta_entry *entry;
    int namespace_cmp;

    entry = &store->meta_entries[store->query_meta_indices[index]];
    namespace_cmp = strcmp(entry->namespace_name, req->namespace_name);
    if (namespace_cmp > 0) {
      break;
    }
    if (namespace_cmp < 0) {
      continue;
    }
    if (entry->deleted ||
        (entry->meta.has_query_hidden && entry->meta.query_hidden)) {
      continue;
    }
    keys[row_index] = lc_pouch_strdup(&store->allocator, entry->key);
    if (keys[row_index] == NULL) {
      for (index = 0U; index < row_index; ++index) {
        lc_pouch_free(&store->allocator, keys[index]);
      }
      lc_pouch_free(&store->allocator, keys);
      lc_pouch_disk_unlock(store, error);
      return lc_pouch_set_nomem(error,
                                "failed to copy pouch query index key");
    }
    row_index++;
  }
  if (out->truncated && visit_count > 0U) {
    out->next_start_after =
        lc_pouch_strdup(&store->allocator, keys[visit_count - 1U]);
    if (out->next_start_after == NULL) {
      for (index = 0U; index < visit_count; ++index) {
        lc_pouch_free(&store->allocator, keys[index]);
      }
      lc_pouch_free(&store->allocator, keys);
      lc_pouch_disk_unlock(store, error);
      return lc_pouch_set_nomem(error,
                                "failed to allocate pouch query index cursor");
    }
  }
  out->index_seq = lc_pouch_disk_index_sequence(store);

  rc = lc_pouch_disk_unlock(store, error);
  if (rc != LC_OK) {
    for (index = 0U; index < visit_count; ++index) {
      lc_pouch_free(&store->allocator, keys[index]);
    }
    lc_pouch_free(&store->allocator, keys);
    lc_pouch_query_index_scan_res_cleanup(&store->allocator, out);
    return rc;
  }

  for (row_index = 0U; row_index < visit_count; ++row_index) {
    rc = visit(visit_context, keys[row_index], error);
    if (rc != LC_OK) {
      for (index = 0U; index < visit_count; ++index) {
        lc_pouch_free(&store->allocator, keys[index]);
      }
      lc_pouch_free(&store->allocator, keys);
      lc_pouch_query_index_scan_res_cleanup(&store->allocator, out);
      return rc;
    }
    out->visited++;
  }

  for (index = 0U; index < visit_count; ++index) {
    lc_pouch_free(&store->allocator, keys[index]);
  }
  lc_pouch_free(&store->allocator, keys);
  return LC_OK;
}

static int lc_pouch_disk_flush_index(lc_pouch_store *self,
                                     const char *namespace_name,
                                     const char *mode,
                                     lc_pouch_index_flush_res *out,
                                     lc_error *error) {
  lc_pouch_disk_store *store;
  const char *effective_mode;
  int rc;

  if (self == NULL || namespace_name == NULL || out == NULL) {
    return lc_pouch_set_invalid(error,
                                "flush_index requires store, namespace, "
                                "and output");
  }
  rc = lc_pouch_disk_validate_name(error, "flush_index", "namespace",
                                   namespace_name);
  if (rc != LC_OK) {
    return rc;
  }
  effective_mode = mode != NULL && mode[0] != '\0' ? mode : "wait";
  if (strcmp(effective_mode, "wait") != 0 &&
      strcmp(effective_mode, "now") != 0) {
    return lc_pouch_set_invalid(error,
                                "pouch index flush mode must be wait or now");
  }

  store = (lc_pouch_disk_store *)self->impl;
  memset(out, 0, sizeof(*out));
  rc = lc_pouch_disk_lock(store, error);
  if (rc != LC_OK) {
    return rc;
  }
  out->namespace_name = lc_pouch_strdup(&store->allocator, namespace_name);
  out->mode = lc_pouch_strdup(&store->allocator, effective_mode);
  out->flush_id = lc_pouch_strdup(&store->allocator, "local");
  out->accepted = 1;
  out->flushed = 1;
  out->pending = 0;
  out->index_seq = lc_pouch_disk_index_sequence(store);
  if (out->namespace_name == NULL || out->mode == NULL ||
      out->flush_id == NULL) {
    lc_pouch_index_flush_res_cleanup(&store->allocator, out);
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_nomem(error, "failed to allocate pouch index flush");
  }
  if (lc_pouch_disk_unlock(store, error) != LC_OK) {
    lc_pouch_index_flush_res_cleanup(&store->allocator, out);
    return LC_ERR_TRANSPORT;
  }
  return LC_OK;
}

static int lc_pouch_disk_append_record(
    lc_pouch_disk_store *store, unsigned long type, const char *namespace_name,
    const char *key, const char *content_type, const char *etag, long version,
    const unsigned char *body, size_t body_length,
    unsigned long *body_offset_out, lc_error *error) {
  unsigned char header[LC_POUCH_HEADER_SIZE];
  unsigned long ns_len;
  unsigned long key_len;
  unsigned long ct_len;
  unsigned long etag_len;
  unsigned long payload_len;
  unsigned long crc;
  off_t start;
  int rc;

  ns_len = (unsigned long)strlen(namespace_name);
  key_len = (unsigned long)strlen(key);
  ct_len = content_type != NULL ? (unsigned long)strlen(content_type) : 0UL;
  etag_len = etag != NULL ? (unsigned long)strlen(etag) : 0UL;
  if (lc_pouch_disk_add_overflows(ns_len, key_len, &payload_len) ||
      lc_pouch_disk_add_overflows(payload_len, ct_len, &payload_len) ||
      lc_pouch_disk_add_overflows(payload_len, etag_len, &payload_len) ||
      lc_pouch_disk_add_overflows(payload_len, (unsigned long)body_length,
                                  &payload_len) ||
      !lc_pouch_disk_validate_record_lengths(
          ns_len, key_len, ct_len, etag_len, (unsigned long)body_length,
          payload_len)) {
    return lc_pouch_set_invalid(error,
                                "pouch log record exceeds inline limits");
  }
  start = lseek(store->log_fd, 0, SEEK_END);
  if (start < 0) {
    return lc_pouch_set_errno(error, "failed to seek pouch log");
  }

  memset(header, 0, sizeof(header));
  memcpy(header, LC_POUCH_LOG_MAGIC, 4U);
  lc_pouch_put_u32(header + 4, LC_POUCH_HEADER_SIZE);
  lc_pouch_put_u32(header + 8, type);
  lc_pouch_put_u32(header + 12, ns_len);
  lc_pouch_put_u32(header + 16, key_len);
  lc_pouch_put_u32(header + 20, ct_len);
  lc_pouch_put_u32(header + 24, etag_len);
  lc_pouch_put_u64(header + 28, (unsigned long)body_length);
  lc_pouch_put_u64(header + 36, (unsigned long)version);
  lc_pouch_put_u64(header + 44, payload_len);
  lc_pouch_put_u32(header + 56, LC_POUCH_RECORD_VERSION);

  crc = 0xffffffffUL;
  crc = lc_pouch_crc32_update(crc, (const unsigned char *)namespace_name,
                              (size_t)ns_len);
  crc = lc_pouch_crc32_update(crc, (const unsigned char *)key, (size_t)key_len);
  if (ct_len > 0UL) {
    crc = lc_pouch_crc32_update(crc, (const unsigned char *)content_type,
                                (size_t)ct_len);
  }
  if (etag_len > 0UL) {
    crc = lc_pouch_crc32_update(crc, (const unsigned char *)etag,
                                (size_t)etag_len);
  }
  if (body_length > 0U) {
    crc = lc_pouch_crc32_update(crc, body, body_length);
  }
  lc_pouch_put_u32(header + 52, crc ^ 0xffffffffUL);

  if (!lc_pouch_write_all(store->log_fd, header, sizeof(header)) ||
      !lc_pouch_write_all(store->log_fd, namespace_name, ns_len) ||
      !lc_pouch_write_all(store->log_fd, key, key_len) ||
      (ct_len > 0UL &&
       !lc_pouch_write_all(store->log_fd, content_type, ct_len)) ||
      (etag_len > 0UL && !lc_pouch_write_all(store->log_fd, etag, etag_len)) ||
      (body_length > 0U &&
       !lc_pouch_write_all(store->log_fd, body, body_length))) {
    return lc_pouch_set_errno(error, "failed to append pouch log record");
  }
  rc = lc_pouch_disk_fsync_record(store, "failed to fsync pouch log", error);
  if (rc != LC_OK) {
    return rc;
  }
  *body_offset_out = (unsigned long)start + LC_POUCH_HEADER_SIZE + ns_len +
                     key_len + ct_len + etag_len;
  store->replayed_log_size = (unsigned long)-1;
  store->replayed_record_count++;
  return LC_OK;
}

static int lc_pouch_disk_append_high_water_record(lc_pouch_disk_store *store,
                                                  long version,
                                                  lc_error *error) {
  unsigned char header[LC_POUCH_HEADER_SIZE];

  memset(header, 0, sizeof(header));
  memcpy(header, LC_POUCH_LOG_MAGIC, 4U);
  lc_pouch_put_u32(header + 4, LC_POUCH_HEADER_SIZE);
  lc_pouch_put_u32(header + 8, LC_POUCH_RECORD_HIGH_WATER);
  lc_pouch_put_u64(header + 36, (unsigned long)version);
  lc_pouch_put_u32(header + 52, lc_pouch_crc32(NULL, 0U));
  lc_pouch_put_u32(header + 56, LC_POUCH_RECORD_VERSION);
  if (lseek(store->log_fd, 0, SEEK_END) < 0) {
    return lc_pouch_set_errno(error, "failed to seek pouch log");
  }
  if (!lc_pouch_write_all(store->log_fd, header, sizeof(header))) {
    return lc_pouch_set_errno(error, "failed to append pouch log record");
  }
  if (lc_pouch_disk_fsync_record(store, "failed to fsync pouch log", error) !=
      LC_OK) {
    return LC_ERR_TRANSPORT;
  }
  store->replayed_log_size = (unsigned long)-1;
  store->replayed_record_count++;
  return LC_OK;
}

static int lc_pouch_disk_crc_fd_span(int fd, unsigned long offset,
                                     unsigned long length, unsigned long *crc,
                                     lc_error *error) {
  unsigned char buffer[8192];
  unsigned long remaining;
  size_t want;
  ssize_t got;

  if (lseek(fd, (off_t)offset, SEEK_SET) < 0) {
    return lc_pouch_set_errno(error, "failed to seek pouch object payload");
  }
  remaining = length;
  while (remaining > 0UL) {
    want = remaining < (unsigned long)sizeof(buffer) ? (size_t)remaining
                                                     : sizeof(buffer);
    got = read(fd, buffer, want);
    if (got < 0) {
      if (errno == EINTR) {
        continue;
      }
      return lc_pouch_set_errno(error, "failed to read pouch object payload");
    }
    if (got == 0) {
      errno = EIO;
      return lc_pouch_set_errno(error,
                                "failed to read complete pouch object payload");
    }
    *crc = lc_pouch_crc32_update(*crc, buffer, (size_t)got);
    remaining -= (unsigned long)got;
  }
  return LC_OK;
}

static int lc_pouch_disk_write_fd_span(int out_fd, int in_fd,
                                       unsigned long offset,
                                       unsigned long length,
                                       lc_error *error) {
  unsigned char buffer[8192];
  unsigned long remaining;
  size_t want;
  ssize_t got;

  if (lseek(in_fd, (off_t)offset, SEEK_SET) < 0) {
    return lc_pouch_set_errno(error, "failed to seek pouch object payload");
  }
  remaining = length;
  while (remaining > 0UL) {
    want = remaining < (unsigned long)sizeof(buffer) ? (size_t)remaining
                                                     : sizeof(buffer);
    got = read(in_fd, buffer, want);
    if (got < 0) {
      if (errno == EINTR) {
        continue;
      }
      return lc_pouch_set_errno(error, "failed to read pouch object payload");
    }
    if (got == 0) {
      errno = EIO;
      return lc_pouch_set_errno(error,
                                "failed to read complete pouch object payload");
    }
    if (!lc_pouch_write_all(out_fd, buffer, (size_t)got)) {
      return lc_pouch_set_errno(error, "failed to append pouch object payload");
    }
    remaining -= (unsigned long)got;
  }
  return LC_OK;
}

static int lc_pouch_disk_append_fd_record(
    lc_pouch_disk_store *store, unsigned long type, const char *namespace_name,
    const char *key, const char *content_type, const char *etag, long version,
    int body_fd, unsigned long body_offset, unsigned long body_length,
    unsigned long *body_offset_out, lc_error *error) {
  unsigned char header[LC_POUCH_HEADER_SIZE];
  unsigned long ns_len;
  unsigned long key_len;
  unsigned long ct_len;
  unsigned long etag_len;
  unsigned long payload_len;
  unsigned long crc;
  off_t start;
  int rc;

  ns_len = (unsigned long)strlen(namespace_name);
  key_len = (unsigned long)strlen(key);
  ct_len = content_type != NULL ? (unsigned long)strlen(content_type) : 0UL;
  etag_len = etag != NULL ? (unsigned long)strlen(etag) : 0UL;
  if (lc_pouch_disk_add_overflows(ns_len, key_len, &payload_len) ||
      lc_pouch_disk_add_overflows(payload_len, ct_len, &payload_len) ||
      lc_pouch_disk_add_overflows(payload_len, etag_len, &payload_len) ||
      lc_pouch_disk_add_overflows(payload_len, body_length, &payload_len) ||
      !lc_pouch_disk_validate_record_lengths(ns_len, key_len, ct_len, etag_len,
                                             body_length, payload_len)) {
    return lc_pouch_set_invalid(error,
                                "pouch log record exceeds inline limits");
  }

  crc = 0xffffffffUL;
  crc = lc_pouch_crc32_update(crc, (const unsigned char *)namespace_name,
                              (size_t)ns_len);
  crc = lc_pouch_crc32_update(crc, (const unsigned char *)key, (size_t)key_len);
  if (ct_len > 0UL) {
    crc = lc_pouch_crc32_update(crc, (const unsigned char *)content_type,
                                (size_t)ct_len);
  }
  if (etag_len > 0UL) {
    crc = lc_pouch_crc32_update(crc, (const unsigned char *)etag,
                                (size_t)etag_len);
  }
  rc = lc_pouch_disk_crc_fd_span(body_fd, body_offset, body_length, &crc,
                                 error);
  if (rc != LC_OK) {
    return rc;
  }

  start = lseek(store->log_fd, 0, SEEK_END);
  if (start < 0) {
    return lc_pouch_set_errno(error, "failed to seek pouch log");
  }

  memset(header, 0, sizeof(header));
  memcpy(header, LC_POUCH_LOG_MAGIC, 4U);
  lc_pouch_put_u32(header + 4, LC_POUCH_HEADER_SIZE);
  lc_pouch_put_u32(header + 8, type);
  lc_pouch_put_u32(header + 12, ns_len);
  lc_pouch_put_u32(header + 16, key_len);
  lc_pouch_put_u32(header + 20, ct_len);
  lc_pouch_put_u32(header + 24, etag_len);
  lc_pouch_put_u64(header + 28, body_length);
  lc_pouch_put_u64(header + 36, (unsigned long)version);
  lc_pouch_put_u64(header + 44, payload_len);
  lc_pouch_put_u32(header + 52, crc ^ 0xffffffffUL);
  lc_pouch_put_u32(header + 56, LC_POUCH_RECORD_VERSION);

  if (!lc_pouch_write_all(store->log_fd, header, sizeof(header)) ||
      !lc_pouch_write_all(store->log_fd, namespace_name, (size_t)ns_len) ||
      !lc_pouch_write_all(store->log_fd, key, (size_t)key_len) ||
      (ct_len > 0UL &&
       !lc_pouch_write_all(store->log_fd, content_type, (size_t)ct_len)) ||
      (etag_len > 0UL &&
       !lc_pouch_write_all(store->log_fd, etag, (size_t)etag_len))) {
    return lc_pouch_set_errno(error, "failed to append pouch log record");
  }
  rc = lc_pouch_disk_write_fd_span(store->log_fd, body_fd, body_offset,
                                   body_length, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_disk_fsync_record(store, "failed to fsync pouch log", error);
  if (rc != LC_OK) {
    return rc;
  }
  *body_offset_out = (unsigned long)start + LC_POUCH_HEADER_SIZE + ns_len +
                     key_len + ct_len + etag_len;
  store->replayed_log_size = (unsigned long)-1;
  store->replayed_record_count++;
  return LC_OK;
}

static int lc_pouch_disk_spool_source_to_temp(
    lc_pouch_disk_store *store, lc_source *source, int has_max_bytes,
    size_t max_bytes, int *fd_out, unsigned long *length_out,
    unsigned long *crc_out, lc_error *error) {
  const char suffix[] = "/payload-XXXXXX";
  unsigned char buffer[8192];
  char *template_path;
  size_t root_len;
  size_t suffix_len;
  size_t want;
  size_t got;
  size_t remaining;
  unsigned long length;
  unsigned long crc;
  int fd;

  *fd_out = -1;
  *length_out = 0UL;
  *crc_out = 0UL;
  root_len = strlen(store->root_path);
  suffix_len = sizeof(suffix) - 1U;
  template_path =
      (char *)lc_pouch_alloc(&store->allocator, root_len + suffix_len + 1U);
  if (template_path == NULL) {
    return lc_pouch_set_nomem(error, "failed to allocate pouch temp path");
  }
  memcpy(template_path, store->root_path, root_len);
  memcpy(template_path + root_len, suffix, suffix_len + 1U);
  fd = mkstemp(template_path);
  if (fd < 0) {
    lc_pouch_free(&store->allocator, template_path);
    return lc_pouch_set_errno(error, "failed to create pouch temp payload");
  }
  (void)unlink(template_path);
  lc_pouch_free(&store->allocator, template_path);

  length = 0UL;
  crc = 0xffffffffUL;
  for (;;) {
    want = sizeof(buffer);
    if (has_max_bytes) {
      if ((unsigned long)max_bytes < length) {
        want = 1U;
      } else {
        remaining = max_bytes - (size_t)length;
        if (remaining < want) {
          want = remaining == (size_t)-1 ? sizeof(buffer) : remaining + 1U;
        }
      }
    }
    got = source->read(source, buffer, want, error);
    if (got == 0U) {
      break;
    }
    if ((unsigned long)got > ((unsigned long)-1) - length) {
      close(fd);
      return lc_pouch_set_invalid(error, "pouch payload is too large");
    }
    if (length + (unsigned long)got > LC_POUCH_MAX_INLINE_BODY_BYTES) {
      close(fd);
      return lc_pouch_set_invalid(error,
                                  "pouch inline payload exceeds storage limit");
    }
    if (has_max_bytes && length + (unsigned long)got > (unsigned long)max_bytes) {
      close(fd);
      return lc_error_set(error, LC_ERR_SERVER, 413L,
                          "pouch object exceeds max_bytes", NULL,
                          "attachment_too_large", NULL);
    }
    if (!lc_pouch_write_all(fd, buffer, got)) {
      close(fd);
      return lc_pouch_set_errno(error, "failed to write pouch temp payload");
    }
    crc = lc_pouch_crc32_update(crc, buffer, got);
    length += (unsigned long)got;
  }
  if (lseek(fd, 0, SEEK_SET) < 0) {
    close(fd);
    return lc_pouch_set_errno(error, "failed to rewind pouch temp payload");
  }
  *fd_out = fd;
  *length_out = length;
  *crc_out = crc ^ 0xffffffffUL;
  return LC_OK;
}

static unsigned long
lc_pouch_disk_compaction_live_record_count(lc_pouch_disk_store *store) {
  unsigned long count;
  size_t index;

  count = store->next_version > 1L ? 1UL : 0UL;
  for (index = 0U; index < store->state_entry_count; ++index) {
    if (!store->state_entries[index].deleted) {
      count++;
    }
  }
  for (index = 0U; index < store->meta_entry_count; ++index) {
    if (!store->meta_entries[index].deleted) {
      count++;
    }
  }
  for (index = 0U; index < store->object_entry_count; ++index) {
    if (!store->object_entries[index].deleted) {
      count++;
    }
  }
  for (index = 0U; index < store->queue_entry_count; ++index) {
    if (!store->queue_entries[index].deleted) {
      count++;
    }
  }
  return count;
}

static int lc_pouch_disk_fsync_root(lc_pouch_disk_store *store) {
  int fd;
  int rc;

  fd = open(store->root_path, O_RDONLY);
  if (fd < 0) {
    return 0;
  }
  rc = fsync(fd) == 0;
  close(fd);
  return rc;
}

static int lc_pouch_disk_compact_locked(lc_pouch_disk_store *store,
                                        lc_error *error) {
  char *temp_path;
  unsigned char *meta_payload;
  size_t meta_payload_length;
  unsigned long body_offset;
  unsigned long old_record_count;
  unsigned long old_replayed_size;
  int old_fd;
  int temp_fd;
  int rc;
  size_t index;

  temp_path = lc_pouch_join_path(&store->allocator, store->root_path,
                                 "store.compact.tmp");
  if (temp_path == NULL) {
    return lc_pouch_set_nomem(error, "failed to allocate pouch compact path");
  }
  temp_fd = open(temp_path, O_RDWR | O_CREAT | O_TRUNC, 0666);
  if (temp_fd < 0) {
    lc_pouch_free(&store->allocator, temp_path);
    return lc_pouch_set_errno(error, "failed to open pouch compact log");
  }

  old_fd = store->log_fd;
  old_record_count = store->replayed_record_count;
  old_replayed_size = store->replayed_log_size;
  store->log_fd = temp_fd;
  store->defer_record_fsync = 1;
  rc = LC_OK;
  if (store->next_version > 1L) {
    rc = lc_pouch_disk_append_high_water_record(store, store->next_version - 1L,
                                                error);
  }
  for (index = 0U; rc == LC_OK && index < store->state_entry_count; ++index) {
    lc_pouch_disk_state_entry *entry;

    entry = &store->state_entries[index];
    if (entry->deleted) {
      continue;
    }
    rc = lc_pouch_disk_append_fd_record(
        store, LC_POUCH_RECORD_STATE_PUT, entry->namespace_name, entry->key,
        entry->content_type, entry->etag, entry->version, old_fd,
        entry->body_offset, entry->body_length, &body_offset, error);
  }
  for (index = 0U; rc == LC_OK && index < store->meta_entry_count; ++index) {
    lc_pouch_disk_meta_entry *entry;

    entry = &store->meta_entries[index];
    if (entry->deleted) {
      continue;
    }
    meta_payload = NULL;
    meta_payload_length = 0U;
    if (!lc_pouch_encode_meta(store, &entry->meta, &meta_payload,
                              &meta_payload_length)) {
      rc = lc_pouch_set_nomem(error,
                              "failed to encode pouch compact metadata");
    } else {
      rc = lc_pouch_disk_append_record(
          store, LC_POUCH_RECORD_META_PUT, entry->namespace_name, entry->key,
          NULL, entry->etag, entry->meta.version, meta_payload,
          meta_payload_length, &body_offset, error);
    }
    lc_pouch_free(&store->allocator, meta_payload);
  }
  for (index = 0U; rc == LC_OK && index < store->object_entry_count; ++index) {
    lc_pouch_disk_object_entry *entry;
    unsigned long payload_offset;

    entry = &store->object_entries[index];
    if (entry->deleted) {
      continue;
    }
    rc = lc_pouch_disk_append_object_copy_record(
        store, old_fd, entry->namespace_name, entry->key, entry->name,
        entry->id, entry->content_type, entry->body_offset, entry->body_length,
        entry->created_at_unix, entry->updated_at_unix, &payload_offset,
        error);
  }
  for (index = 0U; rc == LC_OK && index < store->queue_entry_count; ++index) {
    lc_pouch_disk_queue_entry *entry;

    entry = &store->queue_entries[index];
    if (entry->deleted) {
      continue;
    }
    rc = lc_pouch_disk_append_queue_put_fd_entry(store, entry, old_fd,
                                                 entry->body_offset,
                                                 entry->body_length, error);
  }
  if (rc == LC_OK && fsync(temp_fd) != 0) {
    rc = lc_pouch_set_errno(error, "failed to fsync pouch compact log");
  }
  store->defer_record_fsync = 0;
  if (rc == LC_OK) {
    rc = lc_pouch_disk_replay(store, error);
  }

  if (rc != LC_OK) {
    store->log_fd = old_fd;
    store->replayed_record_count = old_record_count;
    store->replayed_log_size = old_replayed_size;
    (void)lc_pouch_disk_replay(store, NULL);
    close(temp_fd);
    unlink(temp_path);
    lc_pouch_free(&store->allocator, temp_path);
    return rc;
  }
  if (rename(temp_path, store->log_path) != 0) {
    store->log_fd = old_fd;
    store->replayed_record_count = old_record_count;
    store->replayed_log_size = old_replayed_size;
    (void)lc_pouch_disk_replay(store, NULL);
    close(temp_fd);
    unlink(temp_path);
    lc_pouch_free(&store->allocator, temp_path);
    return lc_pouch_set_errno(error, "failed to install pouch compact log");
  }
  lc_pouch_free(&store->allocator, temp_path);
  close(old_fd);
  if (!lc_pouch_disk_fsync_root(store)) {
    return lc_pouch_set_errno(error, "failed to fsync pouch root directory");
  }
  return LC_OK;
}

static int lc_pouch_disk_maybe_compact_locked(lc_pouch_disk_store *store,
                                              unsigned long log_size,
                                              lc_error *error) {
  unsigned long live_count;

  if (log_size < LC_POUCH_COMPACT_MIN_LOG_BYTES) {
    return LC_OK;
  }
  live_count = lc_pouch_disk_compaction_live_record_count(store);
  if (store->replayed_record_count <=
      live_count * LC_POUCH_COMPACT_OBSOLETE_MULTIPLIER) {
    return LC_OK;
  }
  return lc_pouch_disk_compact_locked(store, error);
}

static int lc_pouch_disk_append_object_copy_record(
    lc_pouch_disk_store *store, int read_fd, const char *namespace_name,
    const char *key, const char *name, const char *id,
    const char *payload_content_type, unsigned long src_body_offset,
    unsigned long src_body_length, long created_at_unix, long updated_at_unix,
    unsigned long *payload_offset_out, lc_error *error) {
  unsigned char header[LC_POUCH_HEADER_SIZE];
  unsigned char object_meta[LC_POUCH_OBJECT_META_SIZE];
  unsigned long ns_len;
  unsigned long key_len;
  unsigned long name_len;
  unsigned long id_len;
  unsigned long payload_ct_len;
  unsigned long object_body_len;
  unsigned long payload_len;
  unsigned long crc;
  off_t start;
  int rc;

  ns_len = (unsigned long)strlen(namespace_name);
  key_len = (unsigned long)strlen(key);
  name_len = (unsigned long)strlen(name);
  id_len = (unsigned long)strlen(id);
  payload_ct_len = (unsigned long)strlen(payload_content_type);
  if (lc_pouch_disk_add_overflows(LC_POUCH_OBJECT_META_SIZE, payload_ct_len,
                                  &object_body_len) ||
      lc_pouch_disk_add_overflows(object_body_len, src_body_length,
                                  &object_body_len) ||
      lc_pouch_disk_add_overflows(ns_len, key_len, &payload_len) ||
      lc_pouch_disk_add_overflows(payload_len, name_len, &payload_len) ||
      lc_pouch_disk_add_overflows(payload_len, id_len, &payload_len) ||
      lc_pouch_disk_add_overflows(payload_len, object_body_len, &payload_len) ||
      !lc_pouch_disk_validate_record_lengths(ns_len, key_len, name_len, id_len,
                                             object_body_len, payload_len)) {
    return lc_pouch_set_invalid(error,
                                "pouch log record exceeds inline limits");
  }

  lc_pouch_put_u64(object_meta, (unsigned long)created_at_unix);
  lc_pouch_put_u64(object_meta + 8, (unsigned long)updated_at_unix);
  lc_pouch_put_u32(object_meta + 16, payload_ct_len);
  lc_pouch_put_u64(object_meta + 20, src_body_length);

  crc = 0xffffffffUL;
  crc = lc_pouch_crc32_update(crc, (const unsigned char *)namespace_name,
                              (size_t)ns_len);
  crc = lc_pouch_crc32_update(crc, (const unsigned char *)key,
                              (size_t)key_len);
  crc = lc_pouch_crc32_update(crc, (const unsigned char *)name,
                              (size_t)name_len);
  crc = lc_pouch_crc32_update(crc, (const unsigned char *)id, (size_t)id_len);
  crc = lc_pouch_crc32_update(crc, object_meta, sizeof(object_meta));
  crc = lc_pouch_crc32_update(crc, (const unsigned char *)payload_content_type,
                              (size_t)payload_ct_len);
  rc = lc_pouch_disk_crc_fd_span(read_fd, src_body_offset, src_body_length,
                                 &crc, error);
  if (rc != LC_OK) {
    return rc;
  }

  start = lseek(store->log_fd, 0, SEEK_END);
  if (start < 0) {
    return lc_pouch_set_errno(error, "failed to seek pouch log");
  }

  memset(header, 0, sizeof(header));
  memcpy(header, LC_POUCH_LOG_MAGIC, 4U);
  lc_pouch_put_u32(header + 4, LC_POUCH_HEADER_SIZE);
  lc_pouch_put_u32(header + 8, LC_POUCH_RECORD_OBJECT_PUT);
  lc_pouch_put_u32(header + 12, ns_len);
  lc_pouch_put_u32(header + 16, key_len);
  lc_pouch_put_u32(header + 20, name_len);
  lc_pouch_put_u32(header + 24, id_len);
  lc_pouch_put_u64(header + 28, object_body_len);
  lc_pouch_put_u64(header + 36, 0UL);
  lc_pouch_put_u64(header + 44, payload_len);
  lc_pouch_put_u32(header + 52, crc ^ 0xffffffffUL);
  lc_pouch_put_u32(header + 56, LC_POUCH_RECORD_VERSION);

  if (!lc_pouch_write_all(store->log_fd, header, sizeof(header)) ||
      !lc_pouch_write_all(store->log_fd, namespace_name, (size_t)ns_len) ||
      !lc_pouch_write_all(store->log_fd, key, (size_t)key_len) ||
      !lc_pouch_write_all(store->log_fd, name, (size_t)name_len) ||
      !lc_pouch_write_all(store->log_fd, id, (size_t)id_len) ||
      !lc_pouch_write_all(store->log_fd, object_meta, sizeof(object_meta)) ||
      !lc_pouch_write_all(store->log_fd, payload_content_type,
                          (size_t)payload_ct_len)) {
    return lc_pouch_set_errno(error, "failed to append pouch object record");
  }
  rc = lc_pouch_disk_write_fd_span(store->log_fd, read_fd, src_body_offset,
                                   src_body_length, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_disk_fsync_record(store, "failed to fsync pouch log", error);
  if (rc != LC_OK) {
    return rc;
  }
  *payload_offset_out = (unsigned long)start + LC_POUCH_HEADER_SIZE + ns_len +
                        key_len + name_len + id_len +
                        LC_POUCH_OBJECT_META_SIZE + payload_ct_len;
  store->replayed_log_size = (unsigned long)-1;
  store->replayed_record_count++;
  return LC_OK;
}

static int lc_pouch_disk_replay_read_crc(int fd, void *bytes, size_t count,
                                         unsigned long *crc, int *short_read,
                                         lc_error *error) {
  if (!lc_pouch_read_all(fd, bytes, count, short_read)) {
    return lc_pouch_set_errno(error, "failed to read pouch log payload");
  }
  if (*short_read) {
    return LC_OK;
  }
  if (count > 0U) {
    *crc = lc_pouch_crc32_update(*crc, (const unsigned char *)bytes, count);
  }
  return LC_OK;
}

static int lc_pouch_disk_replay_skip_crc(int fd, unsigned long length,
                                         unsigned long *crc, int *short_read,
                                         lc_error *error) {
  unsigned char buffer[8192];
  unsigned long remaining;
  size_t want;
  int rc;

  remaining = length;
  while (remaining > 0UL) {
    want = remaining < (unsigned long)sizeof(buffer) ? (size_t)remaining
                                                     : sizeof(buffer);
    rc = lc_pouch_disk_replay_read_crc(fd, buffer, want, crc, short_read,
                                       error);
    if (rc != LC_OK || *short_read) {
      return rc;
    }
    remaining -= (unsigned long)want;
  }
  return LC_OK;
}

static char *lc_pouch_disk_replay_read_string(lc_pouch_disk_store *store,
                                              unsigned long length,
                                              unsigned long *crc,
                                              int *short_read,
                                              lc_error *error) {
  char *copy;
  int rc;

  copy = (char *)lc_pouch_alloc(&store->allocator, (size_t)length + 1U);
  if (copy == NULL) {
    (void)lc_pouch_set_nomem(error, "failed to allocate replay field");
    return NULL;
  }
  rc = lc_pouch_disk_replay_read_crc(store->log_fd, copy, (size_t)length, crc,
                                     short_read, error);
  if (rc != LC_OK || *short_read) {
    lc_pouch_free(&store->allocator, copy);
    return NULL;
  }
  copy[length] = '\0';
  return copy;
}

static void lc_pouch_disk_replay_free_fields(lc_pouch_disk_store *store,
                                             char *ns_copy, char *key_copy,
                                             char *ct_copy, char *etag_copy) {
  lc_pouch_free(&store->allocator, ns_copy);
  lc_pouch_free(&store->allocator, key_copy);
  lc_pouch_free(&store->allocator, ct_copy);
  lc_pouch_free(&store->allocator, etag_copy);
}

static int lc_pouch_disk_replay_stream_index_record(
    lc_pouch_disk_store *store, unsigned long type, unsigned long ns_len,
    unsigned long key_len, unsigned long ct_len, unsigned long etag_len,
    unsigned long body_len, unsigned long version, unsigned long expected_crc,
    unsigned long offset, int *stop, lc_error *error) {
  unsigned char link_body[16];
  unsigned char object_meta[LC_POUCH_OBJECT_META_SIZE];
  unsigned char queue_meta[LC_POUCH_QUEUE_META_SIZE];
  char *ns_copy;
  char *key_copy;
  char *ct_copy;
  char *etag_copy;
  char *object_ct_copy;
  char *queue_content_type;
  char *queue_lease_id;
  char *queue_txn_id;
  unsigned long crc;
  unsigned long indexed_body_offset;
  unsigned long indexed_body_len;
  unsigned long created_at;
  unsigned long updated_at;
  unsigned long object_ct_len;
  unsigned long object_body_len;
  unsigned long queue_payload_len;
  unsigned long queue_content_type_len;
  unsigned long queue_lease_id_len;
  unsigned long queue_txn_id_len;
  unsigned long queue_needed;
  unsigned long queue_payload_offset;
  int queue_deleted;
  int queue_attempts;
  int queue_max_attempts;
  int queue_failure_attempts;
  long queue_enqueued_at;
  long queue_not_visible_until;
  long queue_visibility_timeout;
  long queue_expires_at;
  long queue_lease_expires_at;
  int object_index;
  int short_read;
  int rc;

  *stop = 0;
  ns_copy = NULL;
  key_copy = NULL;
  ct_copy = NULL;
  etag_copy = NULL;
  object_ct_copy = NULL;
  queue_content_type = NULL;
  queue_lease_id = NULL;
  queue_txn_id = NULL;
  crc = 0xffffffffUL;
  short_read = 0;

  ns_copy = lc_pouch_disk_replay_read_string(store, ns_len, &crc, &short_read,
                                             error);
  if (ns_copy == NULL) {
    *stop = short_read;
    return short_read ? LC_OK
                      : (error != NULL && error->code != LC_OK ? error->code
                                                               : LC_ERR_NOMEM);
  }
  key_copy = lc_pouch_disk_replay_read_string(store, key_len, &crc,
                                              &short_read, error);
  if (key_copy == NULL) {
    lc_pouch_disk_replay_free_fields(store, ns_copy, NULL, NULL, NULL);
    *stop = short_read;
    return short_read ? LC_OK
                      : (error != NULL && error->code != LC_OK ? error->code
                                                               : LC_ERR_NOMEM);
  }
  if (ct_len > 0UL) {
    ct_copy = lc_pouch_disk_replay_read_string(store, ct_len, &crc,
                                               &short_read, error);
    if (ct_copy == NULL) {
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, NULL, NULL);
      *stop = short_read;
      return short_read ? LC_OK
                        : (error != NULL && error->code != LC_OK
                               ? error->code
                               : LC_ERR_NOMEM);
    }
  }
  if (etag_len > 0UL) {
    etag_copy = lc_pouch_disk_replay_read_string(store, etag_len, &crc,
                                                 &short_read, error);
    if (etag_copy == NULL) {
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy, NULL);
      *stop = short_read;
      return short_read ? LC_OK
                        : (error != NULL && error->code != LC_OK
                               ? error->code
                               : LC_ERR_NOMEM);
    }
  }

  indexed_body_offset =
      offset + LC_POUCH_HEADER_SIZE + ns_len + key_len + ct_len + etag_len;
  indexed_body_len = body_len;
  if (type == LC_POUCH_RECORD_STATE_PUT ||
      type == LC_POUCH_RECORD_STATE_REMOVE) {
    rc = lc_pouch_disk_replay_skip_crc(store->log_fd, body_len, &crc,
                                       &short_read, error);
    if (rc != LC_OK || short_read) {
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                       etag_copy);
      *stop = short_read;
      return rc;
    }
    if ((crc ^ 0xffffffffUL) != expected_crc) {
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                       etag_copy);
      *stop = 1;
      return LC_OK;
    }
    if (!lc_pouch_disk_upsert_entry(
            store, ns_copy, key_copy, ct_copy, etag_copy, (long)version,
            indexed_body_offset, indexed_body_len,
            type == LC_POUCH_RECORD_STATE_REMOVE)) {
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                       etag_copy);
      return lc_pouch_set_nomem(error, "failed to index pouch replay record");
    }
  } else if (type == LC_POUCH_RECORD_STATE_LINK) {
    if (body_len != 16UL) {
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                       etag_copy);
      *stop = 1;
      return LC_OK;
    }
    rc = lc_pouch_disk_replay_read_crc(store->log_fd, link_body,
                                       sizeof(link_body), &crc, &short_read,
                                       error);
    if (rc != LC_OK || short_read) {
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                       etag_copy);
      *stop = short_read;
      return rc;
    }
    if ((crc ^ 0xffffffffUL) != expected_crc) {
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                       etag_copy);
      *stop = 1;
      return LC_OK;
    }
    indexed_body_offset = lc_pouch_get_u64(link_body);
    indexed_body_len = lc_pouch_get_u64(link_body + 8);
    if (indexed_body_offset > offset ||
        indexed_body_len > offset - indexed_body_offset) {
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                       etag_copy);
      *stop = 1;
      return LC_OK;
    }
    if (!lc_pouch_disk_upsert_entry(store, ns_copy, key_copy, ct_copy,
                                    etag_copy, (long)version,
                                    indexed_body_offset, indexed_body_len, 0)) {
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                       etag_copy);
      return lc_pouch_set_nomem(error, "failed to index pouch replay record");
    }
  } else if (type == LC_POUCH_RECORD_OBJECT_PUT) {
    if (ct_copy == NULL || etag_copy == NULL ||
        body_len < LC_POUCH_OBJECT_META_SIZE) {
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                       etag_copy);
      *stop = 1;
      return LC_OK;
    }
    rc = lc_pouch_disk_replay_read_crc(store->log_fd, object_meta,
                                       sizeof(object_meta), &crc, &short_read,
                                       error);
    if (rc != LC_OK || short_read) {
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                       etag_copy);
      *stop = short_read;
      return rc;
    }
    created_at = lc_pouch_get_u64(object_meta);
    updated_at = lc_pouch_get_u64(object_meta + 8);
    object_ct_len = lc_pouch_get_u32(object_meta + 16);
    object_body_len = lc_pouch_get_u64(object_meta + 20);
    if (object_ct_len > body_len - LC_POUCH_OBJECT_META_SIZE ||
        object_body_len !=
            body_len - LC_POUCH_OBJECT_META_SIZE - object_ct_len) {
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                       etag_copy);
      *stop = 1;
      return LC_OK;
    }
    object_ct_copy = lc_pouch_disk_replay_read_string(
        store, object_ct_len, &crc, &short_read, error);
    if (object_ct_copy == NULL) {
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                       etag_copy);
      *stop = short_read;
      return short_read ? LC_OK
                        : (error != NULL && error->code != LC_OK
                               ? error->code
                               : LC_ERR_NOMEM);
    }
    rc = lc_pouch_disk_replay_skip_crc(store->log_fd, object_body_len, &crc,
                                       &short_read, error);
    if (rc != LC_OK || short_read) {
      lc_pouch_free(&store->allocator, object_ct_copy);
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                       etag_copy);
      *stop = short_read;
      return rc;
    }
    if ((crc ^ 0xffffffffUL) != expected_crc) {
      lc_pouch_free(&store->allocator, object_ct_copy);
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                       etag_copy);
      *stop = 1;
      return LC_OK;
    }
    if (!lc_pouch_disk_upsert_object_entry(
            store, ns_copy, key_copy, etag_copy, ct_copy, object_ct_copy,
            (long)object_body_len, (long)created_at, (long)updated_at,
            offset + LC_POUCH_HEADER_SIZE + ns_len + key_len + ct_len +
                etag_len + LC_POUCH_OBJECT_META_SIZE + object_ct_len,
            object_body_len, 0)) {
      lc_pouch_free(&store->allocator, object_ct_copy);
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                       etag_copy);
      return lc_pouch_set_nomem(error, "failed to index pouch object replay");
    }
    lc_pouch_free(&store->allocator, object_ct_copy);
  } else if (type == LC_POUCH_RECORD_OBJECT_REMOVE) {
    rc = lc_pouch_disk_replay_skip_crc(store->log_fd, body_len, &crc,
                                       &short_read, error);
    if (rc != LC_OK || short_read) {
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                       etag_copy);
      *stop = short_read;
      return rc;
    }
    if ((crc ^ 0xffffffffUL) != expected_crc ||
        (ct_copy == NULL && etag_copy == NULL)) {
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                       etag_copy);
      *stop = 1;
      return LC_OK;
    }
    if (ct_copy != NULL) {
      object_index =
          lc_pouch_disk_find_object_by_name(store, ns_copy, key_copy, ct_copy);
    } else {
      lc_pouch_object_selector selector;

      memset(&selector, 0, sizeof(selector));
      selector.id = etag_copy;
      object_index = lc_pouch_disk_find_object(store, ns_copy, key_copy,
                                               &selector);
    }
    if (object_index >= 0) {
      store->object_entries[object_index].deleted = 1;
    }
  } else if (type == LC_POUCH_RECORD_QUEUE_PUT ||
             type == LC_POUCH_RECORD_QUEUE_UPDATE ||
             type == LC_POUCH_RECORD_QUEUE_REMOVE) {
    if (ct_copy == NULL || etag_copy == NULL ||
        body_len < LC_POUCH_QUEUE_META_SIZE) {
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                       etag_copy);
      *stop = 1;
      return LC_OK;
    }
    rc = lc_pouch_disk_replay_read_crc(store->log_fd, queue_meta,
                                       sizeof(queue_meta), &crc, &short_read,
                                       error);
    if (rc != LC_OK || short_read) {
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                       etag_copy);
      *stop = short_read;
      return rc;
    }
    queue_deleted = lc_pouch_get_u32(queue_meta) != 0UL;
    queue_attempts = (int)lc_pouch_get_u32(queue_meta + 4);
    queue_max_attempts = (int)lc_pouch_get_u32(queue_meta + 8);
    queue_failure_attempts = (int)lc_pouch_get_u32(queue_meta + 12);
    queue_enqueued_at = (long)lc_pouch_get_u64(queue_meta + 16);
    queue_not_visible_until = (long)lc_pouch_get_u64(queue_meta + 24);
    queue_visibility_timeout = (long)lc_pouch_get_u64(queue_meta + 32);
    queue_expires_at = (long)lc_pouch_get_u64(queue_meta + 40);
    queue_lease_expires_at = (long)lc_pouch_get_u64(queue_meta + 48);
    queue_payload_len = lc_pouch_get_u64(queue_meta + 56);
    queue_content_type_len = lc_pouch_get_u64(queue_meta + 64);
    queue_lease_id_len = lc_pouch_get_u64(queue_meta + 72);
    queue_txn_id_len = lc_pouch_get_u64(queue_meta + 80);
    queue_needed = LC_POUCH_QUEUE_META_SIZE;
    if (lc_pouch_disk_add_overflows(queue_needed, queue_content_type_len,
                                    &queue_needed) ||
        lc_pouch_disk_add_overflows(queue_needed, queue_lease_id_len,
                                    &queue_needed) ||
        lc_pouch_disk_add_overflows(queue_needed, queue_txn_id_len,
                                    &queue_needed) ||
        lc_pouch_disk_add_overflows(queue_needed, queue_payload_len,
                                    &queue_needed) ||
        queue_needed != body_len) {
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                       etag_copy);
      *stop = 1;
      return LC_OK;
    }
    queue_content_type = lc_pouch_disk_replay_read_string(
        store, queue_content_type_len, &crc, &short_read, error);
    if (queue_content_type == NULL) {
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                       etag_copy);
      *stop = short_read;
      return short_read ? LC_OK
                        : (error != NULL && error->code != LC_OK
                               ? error->code
                               : LC_ERR_NOMEM);
    }
    if (queue_lease_id_len > 0UL) {
      queue_lease_id = lc_pouch_disk_replay_read_string(
          store, queue_lease_id_len, &crc, &short_read, error);
      if (queue_lease_id == NULL) {
        lc_pouch_free(&store->allocator, queue_content_type);
        lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                         etag_copy);
        *stop = short_read;
        return short_read ? LC_OK
                          : (error != NULL && error->code != LC_OK
                                 ? error->code
                                 : LC_ERR_NOMEM);
      }
    }
    if (queue_txn_id_len > 0UL) {
      queue_txn_id = lc_pouch_disk_replay_read_string(
          store, queue_txn_id_len, &crc, &short_read, error);
      if (queue_txn_id == NULL) {
        lc_pouch_free(&store->allocator, queue_content_type);
        lc_pouch_free(&store->allocator, queue_lease_id);
        lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                         etag_copy);
        *stop = short_read;
        return short_read ? LC_OK
                          : (error != NULL && error->code != LC_OK
                                 ? error->code
                                 : LC_ERR_NOMEM);
      }
    }
    queue_payload_offset =
        LC_POUCH_QUEUE_META_SIZE + queue_content_type_len +
        queue_lease_id_len + queue_txn_id_len;
    rc = lc_pouch_disk_replay_skip_crc(store->log_fd, queue_payload_len, &crc,
                                       &short_read, error);
    if (rc != LC_OK || short_read) {
      lc_pouch_free(&store->allocator, queue_content_type);
      lc_pouch_free(&store->allocator, queue_lease_id);
      lc_pouch_free(&store->allocator, queue_txn_id);
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                       etag_copy);
      *stop = short_read;
      return rc;
    }
    if ((crc ^ 0xffffffffUL) != expected_crc) {
      lc_pouch_free(&store->allocator, queue_content_type);
      lc_pouch_free(&store->allocator, queue_lease_id);
      lc_pouch_free(&store->allocator, queue_txn_id);
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                       etag_copy);
      *stop = 1;
      return LC_OK;
    }
    if (type == LC_POUCH_RECORD_QUEUE_REMOVE) {
      queue_deleted = 1;
    }
    if (!lc_pouch_disk_upsert_queue_entry(
            store, ns_copy, key_copy, ct_copy, queue_content_type,
            queue_lease_id, queue_txn_id, etag_copy, queue_attempts,
            queue_max_attempts, queue_failure_attempts, queue_enqueued_at,
            queue_not_visible_until, queue_visibility_timeout,
            queue_expires_at, queue_lease_expires_at, (long)version,
            offset + LC_POUCH_HEADER_SIZE + ns_len + key_len + ct_len +
                etag_len + queue_payload_offset,
            queue_payload_len, type == LC_POUCH_RECORD_QUEUE_PUT,
            queue_deleted)) {
      lc_pouch_free(&store->allocator, queue_content_type);
      lc_pouch_free(&store->allocator, queue_lease_id);
      lc_pouch_free(&store->allocator, queue_txn_id);
      lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                       etag_copy);
      return lc_pouch_set_nomem(error, "failed to index pouch queue replay");
    }
    lc_pouch_free(&store->allocator, queue_content_type);
    lc_pouch_free(&store->allocator, queue_lease_id);
    lc_pouch_free(&store->allocator, queue_txn_id);
  } else {
    lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                     etag_copy);
    *stop = 1;
    return LC_OK;
  }

  lc_pouch_disk_replay_free_fields(store, ns_copy, key_copy, ct_copy,
                                   etag_copy);
  return LC_OK;
}

static int lc_pouch_disk_replay(lc_pouch_disk_store *store, lc_error *error) {
  unsigned char header[LC_POUCH_HEADER_SIZE];
  unsigned char *payload;
  unsigned long type;
  unsigned long header_size;
  unsigned long ns_len;
  unsigned long key_len;
  unsigned long ct_len;
  unsigned long etag_len;
  unsigned long body_len;
  unsigned long version;
  unsigned long payload_len;
  unsigned long expected_crc;
  unsigned long actual_crc;
  unsigned long record_version;
  unsigned long offset;
  int short_read;

  offset = 0UL;
  lc_pouch_disk_reset_indexes(store);
  store->replayed_record_count = 0UL;
  if (lseek(store->log_fd, 0, SEEK_SET) < 0) {
    return lc_pouch_set_errno(error, "failed to rewind pouch log");
  }
  while (1) {
    if (!lc_pouch_read_all(store->log_fd, header, sizeof(header),
                           &short_read)) {
      return lc_pouch_set_errno(error, "failed to read pouch log header");
    }
    if (short_read) {
      break;
    }
    if (memcmp(header, LC_POUCH_LOG_MAGIC, 4U) != 0) {
      break;
    }
    header_size = lc_pouch_get_u32(header + 4);
    type = lc_pouch_get_u32(header + 8);
    ns_len = lc_pouch_get_u32(header + 12);
    key_len = lc_pouch_get_u32(header + 16);
    ct_len = lc_pouch_get_u32(header + 20);
    etag_len = lc_pouch_get_u32(header + 24);
    body_len = lc_pouch_get_u64(header + 28);
    version = lc_pouch_get_u64(header + 36);
    payload_len = lc_pouch_get_u64(header + 44);
    expected_crc = lc_pouch_get_u32(header + 52);
    record_version = lc_pouch_get_u32(header + 56);
    if (header_size != LC_POUCH_HEADER_SIZE ||
        (record_version != 0UL && record_version != LC_POUCH_RECORD_VERSION)) {
      break;
    }
    if (type == LC_POUCH_RECORD_HIGH_WATER) {
      if (ns_len != 0UL || key_len != 0UL || ct_len != 0UL ||
          etag_len != 0UL || body_len != 0UL || payload_len != 0UL ||
          expected_crc != lc_pouch_crc32(NULL, 0U)) {
        break;
      }
      if ((long)version >= store->next_version) {
        store->next_version = (long)version + 1L;
      }
      offset += LC_POUCH_HEADER_SIZE;
      store->replayed_record_count++;
      continue;
    }
    if (!lc_pouch_disk_validate_record_lengths(
            ns_len, key_len, ct_len, etag_len, body_len, payload_len) ||
        (type != LC_POUCH_RECORD_STATE_PUT &&
         type != LC_POUCH_RECORD_STATE_REMOVE &&
         type != LC_POUCH_RECORD_META_PUT &&
         type != LC_POUCH_RECORD_META_REMOVE &&
         type != LC_POUCH_RECORD_OBJECT_PUT &&
         type != LC_POUCH_RECORD_OBJECT_REMOVE &&
         type != LC_POUCH_RECORD_QUEUE_PUT &&
         type != LC_POUCH_RECORD_QUEUE_UPDATE &&
         type != LC_POUCH_RECORD_QUEUE_REMOVE &&
         type != LC_POUCH_RECORD_STATE_LINK)) {
      break;
    }
    if (type == LC_POUCH_RECORD_STATE_PUT ||
        type == LC_POUCH_RECORD_STATE_REMOVE ||
        type == LC_POUCH_RECORD_OBJECT_PUT ||
        type == LC_POUCH_RECORD_OBJECT_REMOVE ||
        type == LC_POUCH_RECORD_QUEUE_PUT ||
        type == LC_POUCH_RECORD_QUEUE_UPDATE ||
        type == LC_POUCH_RECORD_QUEUE_REMOVE ||
        type == LC_POUCH_RECORD_STATE_LINK) {
      int stop;
      int rc;

      stop = 0;
      rc = lc_pouch_disk_replay_stream_index_record(
          store, type, ns_len, key_len, ct_len, etag_len, body_len, version,
          expected_crc, offset, &stop, error);
      if (rc != LC_OK) {
        return rc;
      }
      if (stop) {
        break;
      }
      if ((long)version >= store->next_version) {
        store->next_version = (long)version + 1L;
      }
      offset += LC_POUCH_HEADER_SIZE + payload_len;
      store->replayed_record_count++;
      continue;
    }
    payload = (unsigned char *)lc_pouch_alloc(&store->allocator,
                                              (size_t)payload_len + 1U);
    if (payload == NULL) {
      return lc_pouch_set_nomem(error, "failed to allocate replay payload");
    }
    if (!lc_pouch_read_all(store->log_fd, payload, (size_t)payload_len,
                           &short_read)) {
      lc_pouch_free(&store->allocator, payload);
      return lc_pouch_set_errno(error, "failed to read pouch log payload");
    }
    if (short_read) {
      lc_pouch_free(&store->allocator, payload);
      break;
    }
    payload[payload_len] = '\0';
    actual_crc = lc_pouch_crc32(payload, (size_t)payload_len);
    if (actual_crc != expected_crc) {
      lc_pouch_free(&store->allocator, payload);
      break;
    }
    {
      char *ns_copy;
      char *key_copy;
      char *ct_copy;
      char *etag_copy;
      const unsigned char *key_begin;
      const unsigned char *ct_begin;
      const unsigned char *etag_begin;
      const unsigned char *body_begin;

      key_begin = payload + ns_len;
      ct_begin = key_begin + key_len;
      etag_begin = ct_begin + ct_len;
      body_begin = etag_begin + etag_len;
      ns_copy = lc_pouch_dup_bytes(&store->allocator, payload, (size_t)ns_len);
      key_copy =
          lc_pouch_dup_bytes(&store->allocator, key_begin, (size_t)key_len);
      ct_copy = ct_len > 0UL ? lc_pouch_dup_bytes(&store->allocator, ct_begin,
                                                  (size_t)ct_len)
                             : NULL;
      etag_copy = etag_len > 0UL
                      ? lc_pouch_dup_bytes(&store->allocator, etag_begin,
                                           (size_t)etag_len)
                      : NULL;
      if (ns_copy == NULL || key_copy == NULL ||
          (ct_len > 0UL && ct_copy == NULL) ||
          (etag_len > 0UL && etag_copy == NULL)) {
        lc_pouch_free(&store->allocator, ns_copy);
        lc_pouch_free(&store->allocator, key_copy);
        lc_pouch_free(&store->allocator, ct_copy);
        lc_pouch_free(&store->allocator, etag_copy);
        lc_pouch_free(&store->allocator, payload);
        return lc_pouch_set_nomem(error, "failed to decode pouch replay key");
      }
      if (type == LC_POUCH_RECORD_STATE_PUT ||
          type == LC_POUCH_RECORD_STATE_REMOVE ||
          type == LC_POUCH_RECORD_STATE_LINK) {
        unsigned long indexed_body_offset;
        unsigned long indexed_body_len;

        indexed_body_offset =
            offset + LC_POUCH_HEADER_SIZE + ns_len + key_len + ct_len +
            etag_len;
        indexed_body_len = body_len;
        if (type == LC_POUCH_RECORD_STATE_LINK) {
          if (body_len != 16UL) {
            lc_pouch_free(&store->allocator, ns_copy);
            lc_pouch_free(&store->allocator, key_copy);
            lc_pouch_free(&store->allocator, ct_copy);
            lc_pouch_free(&store->allocator, etag_copy);
            lc_pouch_free(&store->allocator, payload);
            break;
          }
          indexed_body_offset = lc_pouch_get_u64(body_begin);
          indexed_body_len = lc_pouch_get_u64(body_begin + 8);
          if (indexed_body_offset > offset ||
              indexed_body_len > offset - indexed_body_offset) {
            lc_pouch_free(&store->allocator, ns_copy);
            lc_pouch_free(&store->allocator, key_copy);
            lc_pouch_free(&store->allocator, ct_copy);
            lc_pouch_free(&store->allocator, etag_copy);
            lc_pouch_free(&store->allocator, payload);
            break;
          }
        }
        if (!lc_pouch_disk_upsert_entry(
                store, ns_copy, key_copy, ct_copy, etag_copy, (long)version,
                indexed_body_offset, indexed_body_len,
                type == LC_POUCH_RECORD_STATE_REMOVE)) {
          lc_pouch_free(&store->allocator, ns_copy);
          lc_pouch_free(&store->allocator, key_copy);
          lc_pouch_free(&store->allocator, ct_copy);
          lc_pouch_free(&store->allocator, etag_copy);
          lc_pouch_free(&store->allocator, payload);
          return lc_pouch_set_nomem(error,
                                    "failed to index pouch replay record");
        }
      } else if (type == LC_POUCH_RECORD_META_PUT) {
        lc_pouch_meta meta;

        if (!lc_pouch_decode_meta(store, body_begin, (size_t)body_len, &meta)) {
          lc_pouch_free(&store->allocator, ns_copy);
          lc_pouch_free(&store->allocator, key_copy);
          lc_pouch_free(&store->allocator, ct_copy);
          lc_pouch_free(&store->allocator, etag_copy);
          lc_pouch_free(&store->allocator, payload);
          break;
        }
        if (!lc_pouch_disk_upsert_meta_entry(store, ns_copy, key_copy,
                                             etag_copy, &meta, 0)) {
          lc_pouch_meta_cleanup(&store->allocator, &meta);
          lc_pouch_free(&store->allocator, ns_copy);
          lc_pouch_free(&store->allocator, key_copy);
          lc_pouch_free(&store->allocator, ct_copy);
          lc_pouch_free(&store->allocator, etag_copy);
          lc_pouch_free(&store->allocator, payload);
          return lc_pouch_set_nomem(
              error, "failed to index pouch metadata replay record");
        }
        lc_pouch_meta_cleanup(&store->allocator, &meta);
      } else if (type == LC_POUCH_RECORD_META_REMOVE) {
        if (!lc_pouch_disk_upsert_meta_entry(store, ns_copy, key_copy,
                                             etag_copy, NULL, 1)) {
          lc_pouch_free(&store->allocator, ns_copy);
          lc_pouch_free(&store->allocator, key_copy);
          lc_pouch_free(&store->allocator, ct_copy);
          lc_pouch_free(&store->allocator, etag_copy);
          lc_pouch_free(&store->allocator, payload);
          return lc_pouch_set_nomem(
              error, "failed to index pouch metadata replay record");
        }
      } else if (type == LC_POUCH_RECORD_OBJECT_PUT) {
        unsigned long created_at;
        unsigned long updated_at;
        unsigned long object_ct_len;
        unsigned long object_body_len;
        const unsigned char *object_ct_begin;
        char *object_ct_copy;

        if (ct_copy == NULL || etag_copy == NULL ||
            body_len < LC_POUCH_OBJECT_META_SIZE) {
          lc_pouch_free(&store->allocator, ns_copy);
          lc_pouch_free(&store->allocator, key_copy);
          lc_pouch_free(&store->allocator, ct_copy);
          lc_pouch_free(&store->allocator, etag_copy);
          lc_pouch_free(&store->allocator, payload);
          break;
        }
        created_at = lc_pouch_get_u64(body_begin);
        updated_at = lc_pouch_get_u64(body_begin + 8);
        object_ct_len = lc_pouch_get_u32(body_begin + 16);
        object_body_len = lc_pouch_get_u64(body_begin + 20);
        if (object_ct_len > body_len - LC_POUCH_OBJECT_META_SIZE ||
            object_body_len !=
                body_len - LC_POUCH_OBJECT_META_SIZE - object_ct_len) {
          lc_pouch_free(&store->allocator, ns_copy);
          lc_pouch_free(&store->allocator, key_copy);
          lc_pouch_free(&store->allocator, ct_copy);
          lc_pouch_free(&store->allocator, etag_copy);
          lc_pouch_free(&store->allocator, payload);
          break;
        }
        object_ct_begin = body_begin + LC_POUCH_OBJECT_META_SIZE;
        object_ct_copy =
            object_ct_len > 0UL
                ? lc_pouch_dup_bytes(&store->allocator, object_ct_begin,
                                     (size_t)object_ct_len)
                : lc_pouch_strdup(&store->allocator, "");
        if (object_ct_copy == NULL) {
          lc_pouch_free(&store->allocator, ns_copy);
          lc_pouch_free(&store->allocator, key_copy);
          lc_pouch_free(&store->allocator, ct_copy);
          lc_pouch_free(&store->allocator, etag_copy);
          lc_pouch_free(&store->allocator, payload);
          return lc_pouch_set_nomem(
              error, "failed to decode pouch object replay record");
        }
        if (!lc_pouch_disk_upsert_object_entry(
                store, ns_copy, key_copy, etag_copy, ct_copy, object_ct_copy,
                (long)object_body_len, (long)created_at, (long)updated_at,
                offset + LC_POUCH_HEADER_SIZE + ns_len + key_len + ct_len +
                    etag_len + LC_POUCH_OBJECT_META_SIZE + object_ct_len,
                object_body_len, 0)) {
          lc_pouch_free(&store->allocator, object_ct_copy);
          lc_pouch_free(&store->allocator, ns_copy);
          lc_pouch_free(&store->allocator, key_copy);
          lc_pouch_free(&store->allocator, ct_copy);
          lc_pouch_free(&store->allocator, etag_copy);
          lc_pouch_free(&store->allocator, payload);
          return lc_pouch_set_nomem(error,
                                    "failed to index pouch object replay");
        }
        lc_pouch_free(&store->allocator, object_ct_copy);
      } else if (type == LC_POUCH_RECORD_OBJECT_REMOVE) {
        int object_index;

        if (ct_copy == NULL && etag_copy == NULL) {
          lc_pouch_free(&store->allocator, ns_copy);
          lc_pouch_free(&store->allocator, key_copy);
          lc_pouch_free(&store->allocator, payload);
          break;
        }
        if (ct_copy != NULL) {
          object_index = lc_pouch_disk_find_object_by_name(store, ns_copy,
                                                           key_copy, ct_copy);
        } else {
          lc_pouch_object_selector selector;

          memset(&selector, 0, sizeof(selector));
          selector.id = etag_copy;
          object_index =
              lc_pouch_disk_find_object(store, ns_copy, key_copy, &selector);
        }
        if (object_index >= 0) {
          store->object_entries[object_index].deleted = 1;
        }
      } else if (type == LC_POUCH_RECORD_QUEUE_PUT ||
                 type == LC_POUCH_RECORD_QUEUE_UPDATE ||
                 type == LC_POUCH_RECORD_QUEUE_REMOVE) {
        char *queue_content_type;
        char *queue_lease_id;
        char *queue_txn_id;
        unsigned long queue_payload_len;
        unsigned long queue_payload_offset;
        int queue_deleted;
        int queue_attempts;
        int queue_max_attempts;
        int queue_failure_attempts;
        long queue_enqueued_at;
        long queue_not_visible_until;
        long queue_visibility_timeout;
        long queue_expires_at;
        long queue_lease_expires_at;

        queue_content_type = NULL;
        queue_lease_id = NULL;
        queue_txn_id = NULL;
        if (ct_copy == NULL || etag_copy == NULL ||
            !lc_pouch_decode_queue_record(
                store, body_begin, body_len, &queue_deleted, &queue_attempts,
                &queue_max_attempts, &queue_failure_attempts,
                &queue_enqueued_at, &queue_not_visible_until,
                &queue_visibility_timeout, &queue_expires_at,
                &queue_lease_expires_at, &queue_payload_len,
                &queue_content_type, &queue_lease_id, &queue_txn_id,
                &queue_payload_offset)) {
          lc_pouch_free(&store->allocator, queue_content_type);
          lc_pouch_free(&store->allocator, queue_lease_id);
          lc_pouch_free(&store->allocator, queue_txn_id);
          lc_pouch_free(&store->allocator, ns_copy);
          lc_pouch_free(&store->allocator, key_copy);
          lc_pouch_free(&store->allocator, ct_copy);
          lc_pouch_free(&store->allocator, etag_copy);
          lc_pouch_free(&store->allocator, payload);
          break;
        }
        if (type == LC_POUCH_RECORD_QUEUE_REMOVE) {
          queue_deleted = 1;
        }
        if (!lc_pouch_disk_upsert_queue_entry(
                store, ns_copy, key_copy, ct_copy, queue_content_type,
                queue_lease_id, queue_txn_id, etag_copy, queue_attempts,
                queue_max_attempts, queue_failure_attempts, queue_enqueued_at,
                queue_not_visible_until, queue_visibility_timeout,
                queue_expires_at, queue_lease_expires_at, (long)version,
                offset + LC_POUCH_HEADER_SIZE + ns_len + key_len + ct_len +
                    etag_len + queue_payload_offset,
                queue_payload_len, type == LC_POUCH_RECORD_QUEUE_PUT,
                queue_deleted)) {
          lc_pouch_free(&store->allocator, queue_content_type);
          lc_pouch_free(&store->allocator, queue_lease_id);
          lc_pouch_free(&store->allocator, queue_txn_id);
          lc_pouch_free(&store->allocator, ns_copy);
          lc_pouch_free(&store->allocator, key_copy);
          lc_pouch_free(&store->allocator, ct_copy);
          lc_pouch_free(&store->allocator, etag_copy);
          lc_pouch_free(&store->allocator, payload);
          return lc_pouch_set_nomem(error,
                                    "failed to index pouch queue replay");
        }
        lc_pouch_free(&store->allocator, queue_content_type);
        lc_pouch_free(&store->allocator, queue_lease_id);
        lc_pouch_free(&store->allocator, queue_txn_id);
      } else {
        lc_pouch_free(&store->allocator, ns_copy);
        lc_pouch_free(&store->allocator, key_copy);
        lc_pouch_free(&store->allocator, ct_copy);
        lc_pouch_free(&store->allocator, etag_copy);
        lc_pouch_free(&store->allocator, payload);
        break;
      }
      lc_pouch_free(&store->allocator, ns_copy);
      lc_pouch_free(&store->allocator, key_copy);
      lc_pouch_free(&store->allocator, ct_copy);
      lc_pouch_free(&store->allocator, etag_copy);
    }
    if ((long)version >= store->next_version) {
      store->next_version = (long)version + 1L;
    }
    offset += LC_POUCH_HEADER_SIZE + payload_len;
    store->replayed_record_count++;
    lc_pouch_free(&store->allocator, payload);
  }
  if (lseek(store->log_fd, (off_t)offset, SEEK_SET) < 0 ||
      ftruncate(store->log_fd, (off_t)offset) != 0) {
    return lc_pouch_set_errno(error, "failed to truncate pouch log tail");
  }
  store->replayed_log_size = offset;
  return LC_OK;
}

static size_t lc_pouch_file_source_read(lc_source *self, void *buffer,
                                        size_t count, lc_error *error) {
  lc_pouch_file_source *source;
  ssize_t got;

  source = (lc_pouch_file_source *)self->impl;
  if (source == NULL || source->fd < 0) {
    lc_error_set(error, LC_ERR_INVALID, 0L, "pouch source is closed", NULL,
                 NULL, NULL);
    return 0U;
  }
  if (source->remaining == 0UL || count == 0U) {
    return 0U;
  }
  if ((unsigned long)count > source->remaining) {
    count = (size_t)source->remaining;
  }
  while (1) {
    got = read(source->fd, buffer, count);
    if (got < 0 && errno == EINTR) {
      continue;
    }
    break;
  }
  if (got < 0) {
    lc_pouch_set_errno(error, "failed to read pouch state source");
    return 0U;
  }
  source->remaining -= (unsigned long)got;
  return (size_t)got;
}

static int lc_pouch_file_source_reset(lc_source *self, lc_error *error) {
  (void)self;
  return lc_pouch_set_invalid(error, "pouch file source is not rewindable");
}

static void lc_pouch_file_source_close(lc_source *self) {
  lc_pouch_file_source *source;
  lc_pouch_allocator allocator;

  if (self == NULL) {
    return;
  }
  source = (lc_pouch_file_source *)self->impl;
  if (source != NULL) {
    allocator = source->allocator;
    if (source->fd >= 0) {
      close(source->fd);
      source->fd = -1;
    }
    lc_pouch_free(&allocator, source);
    lc_pouch_free(&allocator, self);
  }
}

static int lc_pouch_disk_read_state(lc_pouch_store *self,
                                    const char *namespace_name, const char *key,
                                    lc_source **body, lc_pouch_state_info *out,
                                    lc_error *error) {
  lc_pouch_disk_store *store;
  lc_pouch_disk_state_entry *entry;
  lc_source *source_pub;
  lc_pouch_file_source *source;
  int index;
  int fd;

  if (self == NULL || namespace_name == NULL || key == NULL || body == NULL ||
      out == NULL) {
    return lc_pouch_set_invalid(error,
                                "read_state requires store, namespace, key, "
                                "body, and output metadata");
  }
  index = lc_pouch_disk_validate_namespace_key(error, "read_state",
                                               namespace_name, key);
  if (index != LC_OK) {
    return index;
  }
  store = (lc_pouch_disk_store *)self->impl;
  memset(out, 0, sizeof(*out));
  *body = NULL;
  index = lc_pouch_disk_lock(store, error);
  if (index != LC_OK) {
    return index;
  }
  index = lc_pouch_disk_find_entry(store, namespace_name, key);
  if (index < 0 || store->state_entries[index].deleted) {
    out->no_content = 1;
    if (lc_pouch_disk_unlock(store, error) != LC_OK) {
      return LC_ERR_TRANSPORT;
    }
    return LC_OK;
  }
  entry = &store->state_entries[index];
  fd = open(store->log_path, O_RDONLY);
  if (fd < 0) {
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_errno(error, "failed to open pouch log for state read");
  }
  if (lseek(fd, (off_t)entry->body_offset, SEEK_SET) < 0) {
    close(fd);
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_errno(error, "failed to seek pouch state body");
  }
  source_pub =
      (lc_source *)lc_pouch_calloc(&store->allocator, 1U, sizeof(*source_pub));
  source = (lc_pouch_file_source *)lc_pouch_calloc(&store->allocator, 1U,
                                                   sizeof(*source));
  if (source_pub == NULL || source == NULL) {
    close(fd);
    lc_pouch_free(&store->allocator, source_pub);
    lc_pouch_free(&store->allocator, source);
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_nomem(error, "failed to allocate pouch state source");
  }
  source->allocator = store->allocator;
  source->fd = fd;
  source->remaining = entry->body_length;
  source_pub->read = lc_pouch_file_source_read;
  source_pub->reset = lc_pouch_file_source_reset;
  source_pub->close = lc_pouch_file_source_close;
  source_pub->impl = source;
  out->content_type = lc_pouch_strdup(&store->allocator, entry->content_type);
  out->etag = lc_pouch_strdup(&store->allocator, entry->etag);
  if ((entry->content_type != NULL && out->content_type == NULL) ||
      (entry->etag != NULL && out->etag == NULL)) {
    source_pub->close(source_pub);
    lc_pouch_state_info_cleanup(&store->allocator, out);
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_nomem(error, "failed to copy pouch state metadata");
  }
  out->version = entry->version;
  out->bytes = (long)entry->body_length;
  *body = source_pub;
  if (lc_pouch_disk_unlock(store, error) != LC_OK) {
    source_pub->close(source_pub);
    *body = NULL;
    lc_pouch_state_info_cleanup(&store->allocator, out);
    return LC_ERR_TRANSPORT;
  }
  return LC_OK;
}

static int lc_pouch_disk_write_state(lc_pouch_store *self,
                                     const char *namespace_name,
                                     const char *key, lc_source *body,
                                     const lc_pouch_put_state_opts *opts,
                                     lc_pouch_put_state_res *out,
                                     lc_error *error) {
  lc_pouch_disk_store *store;
  lc_pouch_disk_state_entry *entry;
  unsigned long payload_length;
  unsigned long payload_crc;
  char *etag;
  const char *content_type;
  unsigned long body_offset;
  long version;
  int index;
  int temp_fd;
  int rc;

  if (self == NULL || namespace_name == NULL || key == NULL || body == NULL ||
      out == NULL) {
    return lc_pouch_set_invalid(error,
                                "write_state requires store, namespace, key, "
                                "body, and output metadata");
  }
  rc = lc_pouch_disk_validate_namespace_key(error, "write_state",
                                           namespace_name, key);
  if (rc != LC_OK) {
    return rc;
  }
  store = (lc_pouch_disk_store *)self->impl;
  memset(out, 0, sizeof(*out));
  body_offset = 0UL;
  rc = lc_pouch_disk_lock(store, error);
  if (rc != LC_OK) {
    return rc;
  }
  index = lc_pouch_disk_find_entry(store, namespace_name, key);
  entry = index >= 0 ? &store->state_entries[index] : NULL;
  rc = lc_pouch_check_cas(entry, opts != NULL ? opts->if_state_etag : NULL,
                          opts != NULL ? opts->if_version : 0L,
                          opts != NULL ? opts->has_if_version : 0, error);
  if (rc != LC_OK) {
    lc_pouch_disk_unlock(store, error);
    return rc;
  }
  temp_fd = -1;
  rc = lc_pouch_disk_spool_source_to_temp(
      store, body, 0, 0U, &temp_fd, &payload_length, &payload_crc, error);
  if (rc != LC_OK) {
    lc_pouch_disk_unlock(store, error);
    return rc;
  }
  version = store->next_version++;
  etag = lc_pouch_make_etag_from_crc(store, version, payload_crc);
  if (etag == NULL) {
    close(temp_fd);
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_nomem(error, "failed to allocate pouch state etag");
  }
  content_type = opts != NULL && opts->content_type != NULL
                     ? opts->content_type
                     : "application/json";
  rc = lc_pouch_disk_append_fd_record(
      store, LC_POUCH_RECORD_STATE_PUT, namespace_name, key, content_type, etag,
      version, temp_fd, 0UL, payload_length, &body_offset, error);
  close(temp_fd);
  if (rc == LC_OK &&
      !lc_pouch_disk_upsert_entry(store, namespace_name, key, content_type,
                                  etag, version, body_offset,
                                  payload_length, 0)) {
    rc = lc_pouch_set_nomem(error, "failed to update pouch state index");
  }
  if (rc == LC_OK) {
    out->new_version = version;
    out->new_state_etag = lc_pouch_strdup(&store->allocator, etag);
    out->bytes = (long)payload_length;
    if (out->new_state_etag == NULL) {
      rc = lc_pouch_set_nomem(error, "failed to copy pouch state etag");
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_disk_mark_replayed_to_current_size(store, error);
  }
  lc_pouch_free(&store->allocator, etag);
  if (lc_pouch_disk_unlock(store, error) != LC_OK && rc == LC_OK) {
    rc = LC_ERR_TRANSPORT;
  }
  return rc;
}

static int lc_pouch_disk_remove_state(lc_pouch_store *self,
                                      const char *namespace_name,
                                      const char *key,
                                      const char *expected_etag,
                                      int *removed, lc_error *error) {
  lc_pouch_disk_store *store;
  lc_pouch_disk_state_entry *entry;
  char *etag;
  unsigned long body_offset;
  long version;
  int index;
  int rc;

  if (self == NULL || namespace_name == NULL || key == NULL ||
      removed == NULL) {
    return lc_pouch_set_invalid(error,
                                "remove_state requires store, namespace, key, "
                                "and removed output");
  }
  *removed = 0;
  rc = lc_pouch_disk_validate_namespace_key(error, "remove_state",
                                           namespace_name, key);
  if (rc != LC_OK) {
    return rc;
  }
  store = (lc_pouch_disk_store *)self->impl;
  rc = lc_pouch_disk_lock(store, error);
  if (rc != LC_OK) {
    return rc;
  }
  index = lc_pouch_disk_find_entry(store, namespace_name, key);
  entry = index >= 0 ? &store->state_entries[index] : NULL;
  rc = lc_pouch_check_cas(entry, expected_etag, 0L, 0, error);
  if (rc != LC_OK) {
    lc_pouch_disk_unlock(store, error);
    return rc;
  }
  if (entry == NULL || entry->deleted) {
    if (lc_pouch_disk_unlock(store, error) != LC_OK) {
      return LC_ERR_TRANSPORT;
    }
    return LC_OK;
  }
  version = store->next_version++;
  etag = lc_pouch_make_etag(store, version, NULL, 0U);
  if (etag == NULL) {
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_nomem(error, "failed to allocate pouch remove etag");
  }
  rc = lc_pouch_disk_append_record(store, LC_POUCH_RECORD_STATE_REMOVE,
                                   namespace_name, key, NULL, etag, version,
                                   NULL, 0U, &body_offset, error);
  if (rc == LC_OK &&
      !lc_pouch_disk_upsert_entry(store, namespace_name, key, NULL, etag,
                                  version, body_offset, 0UL, 1)) {
    rc = lc_pouch_set_nomem(error, "failed to update pouch remove index");
  }
  if (rc == LC_OK) {
    *removed = 1;
    rc = lc_pouch_disk_mark_replayed_to_current_size(store, error);
  }
  lc_pouch_free(&store->allocator, etag);
  if (lc_pouch_disk_unlock(store, error) != LC_OK && rc == LC_OK) {
    rc = LC_ERR_TRANSPORT;
  }
  return rc;
}

static char *lc_pouch_disk_make_staged_key(lc_pouch_disk_store *store,
                                           const char *key,
                                           const char *txn_id) {
  const char suffix[] = "/.staging/";
  const char root_prefix[] = ".staging/";
  size_t key_len;
  size_t txn_len;
  size_t suffix_len;
  size_t prefix_len;
  char *staged_key;

  key_len = strlen(key);
  while (key_len > 0U && key[key_len - 1U] == '/') {
    key_len--;
  }
  txn_len = strlen(txn_id);
  suffix_len =
      key_len > 0U ? (sizeof(suffix) - 1U) : (sizeof(root_prefix) - 1U);
  staged_key = (char *)lc_pouch_alloc(&store->allocator,
                                      key_len + suffix_len + txn_len + 1U);
  if (staged_key == NULL) {
    return NULL;
  }
  if (key_len > 0U) {
    memcpy(staged_key, key, key_len);
    memcpy(staged_key + key_len, suffix, suffix_len);
    prefix_len = key_len + suffix_len;
  } else {
    memcpy(staged_key, root_prefix, suffix_len);
    prefix_len = suffix_len;
  }
  memcpy(staged_key + prefix_len, txn_id, txn_len + 1U);
  return staged_key;
}

static int lc_pouch_disk_validate_staged_args(lc_error *error,
                                             const char *operation,
                                             lc_pouch_store *self,
                                             const char *namespace_name,
                                             const char *key,
                                             const char *txn_id,
                                             int allow_empty_key) {
  size_t key_len;

  if (self == NULL || namespace_name == NULL || key == NULL ||
      txn_id == NULL || txn_id[0] == '\0') {
    return lc_pouch_set_invalid(error, operation);
  }
  if (namespace_name[0] == '\0') {
    return lc_pouch_disk_validate_name(error, operation, "namespace",
                                       namespace_name);
  }
  key_len = strlen(key);
  while (key_len > 0U && key[key_len - 1U] == '/') {
    key_len--;
  }
  if (key_len == 0U && (!allow_empty_key || key[0] != '\0')) {
    return lc_pouch_set_invalid(error, operation);
  }
  if (strchr(txn_id, '/') != NULL) {
    return lc_pouch_set_invalid(error, operation);
  }
  return LC_OK;
}

static char *lc_pouch_disk_dup_trimmed_key(lc_pouch_disk_store *store,
                                           const char *key) {
  size_t key_len;

  key_len = strlen(key);
  while (key_len > 0U && key[key_len - 1U] == '/') {
    key_len--;
  }
  return lc_pouch_dup_bytes(&store->allocator, key, key_len);
}

static int lc_pouch_disk_stage_state(lc_pouch_store *self,
                                     const char *namespace_name,
                                     const char *key, const char *txn_id,
                                     lc_source *body,
                                     const lc_pouch_put_state_opts *opts,
                                     lc_pouch_put_state_res *out,
                                     lc_error *error) {
  lc_pouch_disk_store *store;
  char *staged_key;
  int rc;

  rc = lc_pouch_disk_validate_staged_args(
      error,
      "stage_state requires store, namespace, key, transaction id, body, and "
      "output metadata",
      self, namespace_name, key, txn_id, 1);
  if (rc != LC_OK) {
    return rc;
  }
  if (body == NULL || out == NULL) {
    return lc_pouch_set_invalid(
        error,
        "stage_state requires store, namespace, key, transaction id, body, and "
        "output metadata");
  }
  store = (lc_pouch_disk_store *)self->impl;
  staged_key = lc_pouch_disk_make_staged_key(store, key, txn_id);
  if (staged_key == NULL) {
    return lc_pouch_set_nomem(error, "failed to allocate pouch staged key");
  }
  rc = lc_pouch_disk_write_state(self, namespace_name, staged_key, body, opts,
                                 out, error);
  lc_pouch_free(&store->allocator, staged_key);
  return rc;
}

static int lc_pouch_disk_load_staged_state(lc_pouch_store *self,
                                           const char *namespace_name,
                                           const char *key,
                                           const char *txn_id,
                                           lc_source **body,
                                           lc_pouch_state_info *out,
                                           lc_error *error) {
  lc_pouch_disk_store *store;
  char *staged_key;
  int rc;

  rc = lc_pouch_disk_validate_staged_args(
      error,
      "load_staged_state requires store, namespace, key, transaction id, body, "
      "and output metadata",
      self, namespace_name, key, txn_id, 1);
  if (rc != LC_OK) {
    return rc;
  }
  if (body == NULL || out == NULL) {
    return lc_pouch_set_invalid(
        error,
        "load_staged_state requires store, namespace, key, transaction id, "
        "body, and output metadata");
  }
  store = (lc_pouch_disk_store *)self->impl;
  staged_key = lc_pouch_disk_make_staged_key(store, key, txn_id);
  if (staged_key == NULL) {
    return lc_pouch_set_nomem(error, "failed to allocate pouch staged key");
  }
  rc = lc_pouch_disk_read_state(self, namespace_name, staged_key, body, out,
                                error);
  lc_pouch_free(&store->allocator, staged_key);
  return rc;
}

static int lc_pouch_disk_append_state_remove_locked(
    lc_pouch_disk_store *store, const char *namespace_name, const char *key,
    lc_error *error) {
  char *etag;
  unsigned long body_offset;
  long version;
  int rc;

  version = store->next_version++;
  etag = lc_pouch_make_etag(store, version, NULL, 0U);
  if (etag == NULL) {
    return lc_pouch_set_nomem(error, "failed to allocate pouch remove etag");
  }
  rc = lc_pouch_disk_append_record(store, LC_POUCH_RECORD_STATE_REMOVE,
                                   namespace_name, key, NULL, etag, version,
                                   NULL, 0U, &body_offset, error);
  if (rc == LC_OK &&
      !lc_pouch_disk_upsert_entry(store, namespace_name, key, NULL, etag,
                                  version, body_offset, 0UL, 1)) {
    rc = lc_pouch_set_nomem(error, "failed to update pouch remove index");
  }
  lc_pouch_free(&store->allocator, etag);
  return rc;
}

static int lc_pouch_disk_append_state_link_locked(
    lc_pouch_disk_store *store, const char *namespace_name, const char *key,
    const lc_pouch_disk_state_entry *target, lc_pouch_put_state_res *out,
    lc_error *error) {
  unsigned char link_body[16];
  const char *content_type;
  unsigned long link_body_offset;
  long version;
  int rc;

  if (target == NULL || target->deleted || target->etag == NULL) {
    return lc_pouch_set_invalid(error, "pouch state link requires live target");
  }
  version = store->next_version++;
  content_type =
      target->content_type != NULL ? target->content_type : "application/json";
  lc_pouch_put_u64(link_body, target->body_offset);
  lc_pouch_put_u64(link_body + 8, target->body_length);
  rc = lc_pouch_disk_append_record(
      store, LC_POUCH_RECORD_STATE_LINK, namespace_name, key, content_type,
      target->etag, version, link_body, sizeof(link_body), &link_body_offset,
      error);
  if (rc == LC_OK &&
      !lc_pouch_disk_upsert_entry(store, namespace_name, key, content_type,
                                  target->etag, version, target->body_offset,
                                  target->body_length, 0)) {
    rc = lc_pouch_set_nomem(error, "failed to update pouch state link index");
  }
  if (rc == LC_OK && out != NULL) {
    out->new_version = version;
    out->new_state_etag = lc_pouch_strdup(&store->allocator, target->etag);
    out->bytes = (long)target->body_length;
    if (out->new_state_etag == NULL) {
      rc = lc_pouch_set_nomem(error, "failed to copy pouch state link etag");
    }
  }
  return rc;
}

static int lc_pouch_disk_promote_staged_state(
    lc_pouch_store *self, const char *namespace_name, const char *key,
    const char *txn_id, const lc_pouch_promote_staged_opts *opts,
    lc_pouch_put_state_res *out, lc_error *error) {
  lc_pouch_disk_store *store;
  lc_pouch_disk_state_entry *staged;
  lc_pouch_disk_state_entry *head;
  lc_pouch_put_state_res committed;
  char *staged_key;
  int staged_index;
  int head_index;
  int rc;

  rc = lc_pouch_disk_validate_staged_args(
      error,
      "promote_staged_state requires store, namespace, key, transaction id, "
      "and output metadata",
      self, namespace_name, key, txn_id, 0);
  if (rc != LC_OK) {
    return rc;
  }
  if (out == NULL) {
    return lc_pouch_set_invalid(
        error,
        "promote_staged_state requires store, namespace, key, transaction id, "
        "and output metadata");
  }
  store = (lc_pouch_disk_store *)self->impl;
  memset(out, 0, sizeof(*out));
  memset(&committed, 0, sizeof(committed));
  staged_key = lc_pouch_disk_make_staged_key(store, key, txn_id);
  if (staged_key == NULL) {
    return lc_pouch_set_nomem(error, "failed to allocate pouch staged key");
  }

  rc = lc_pouch_disk_lock(store, error);
  if (rc != LC_OK) {
    lc_pouch_free(&store->allocator, staged_key);
    return rc;
  }
  staged_index = lc_pouch_disk_find_entry(store, namespace_name, staged_key);
  if (staged_index < 0 || store->state_entries[staged_index].deleted) {
    lc_pouch_disk_unlock(store, error);
    lc_pouch_free(&store->allocator, staged_key);
    return lc_error_set(error, LC_ERR_SERVER, 404L,
                        "pouch staged state was not found", NULL, "not_found",
                        NULL);
  }
  staged = &store->state_entries[staged_index];
  head_index = lc_pouch_disk_find_entry(store, namespace_name, key);
  head = head_index >= 0 ? &store->state_entries[head_index] : NULL;
  if (opts != NULL && opts->expected_head_etag != NULL) {
    rc = lc_pouch_check_cas(head, opts->expected_head_etag, 0L, 0, error);
  } else if (head != NULL && !head->deleted) {
    rc = lc_error_set(error, LC_ERR_SERVER, 412L,
                      "pouch state create precondition failed", NULL,
                      "precondition_failed", NULL);
  }
  if (rc != LC_OK) {
    lc_pouch_disk_unlock(store, error);
    lc_pouch_free(&store->allocator, staged_key);
    return rc;
  }
  rc = lc_pouch_disk_append_state_link_locked(store, namespace_name, key,
                                              staged, &committed, error);
  if (rc == LC_OK) {
    rc = lc_pouch_disk_append_state_remove_locked(store, namespace_name,
                                                  staged_key, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_disk_mark_replayed_to_current_size(store, error);
  }
  if (lc_pouch_disk_unlock(store, error) != LC_OK && rc == LC_OK) {
    rc = LC_ERR_TRANSPORT;
  }
  lc_pouch_free(&store->allocator, staged_key);
  if (rc != LC_OK) {
    lc_pouch_put_state_res_cleanup(&store->allocator, &committed);
    return rc;
  }
  *out = committed;
  return LC_OK;
}

static int lc_pouch_disk_discard_staged_state(
    lc_pouch_store *self, const char *namespace_name, const char *key,
    const char *txn_id, const lc_pouch_discard_staged_opts *opts,
    lc_error *error) {
  lc_pouch_disk_store *store;
  lc_pouch_disk_state_entry *staged;
  char *staged_key;
  int staged_index;
  int rc;

  rc = lc_pouch_disk_validate_staged_args(
      error, "discard_staged_state requires store, namespace, key, and "
             "transaction id",
      self, namespace_name, key, txn_id, 1);
  if (rc != LC_OK) {
    return rc;
  }
  store = (lc_pouch_disk_store *)self->impl;
  staged_key = lc_pouch_disk_make_staged_key(store, key, txn_id);
  if (staged_key == NULL) {
    return lc_pouch_set_nomem(error, "failed to allocate pouch staged key");
  }

  rc = lc_pouch_disk_lock(store, error);
  if (rc != LC_OK) {
    lc_pouch_free(&store->allocator, staged_key);
    return rc;
  }
  staged_index = lc_pouch_disk_find_entry(store, namespace_name, staged_key);
  if (staged_index < 0 || store->state_entries[staged_index].deleted) {
    lc_pouch_disk_unlock(store, error);
    lc_pouch_free(&store->allocator, staged_key);
    if (opts != NULL && opts->ignore_not_found) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_SERVER, 404L,
                        "pouch staged state was not found", NULL, "not_found",
                        NULL);
  }
  staged = &store->state_entries[staged_index];
  rc = lc_pouch_check_cas(staged, opts != NULL ? opts->expected_etag : NULL, 0L,
                          0, error);
  if (rc == LC_OK) {
    rc = lc_pouch_disk_append_state_remove_locked(store, namespace_name,
                                                  staged_key, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_disk_mark_replayed_to_current_size(store, error);
  }
  if (lc_pouch_disk_unlock(store, error) != LC_OK && rc == LC_OK) {
    rc = LC_ERR_TRANSPORT;
  }
  lc_pouch_free(&store->allocator, staged_key);
  return rc;
}

static int lc_pouch_disk_staged_match_compare(const void *left,
                                              const void *right) {
  const lc_pouch_disk_staged_match *left_match;
  const lc_pouch_disk_staged_match *right_match;

  left_match = (const lc_pouch_disk_staged_match *)left;
  right_match = (const lc_pouch_disk_staged_match *)right;
  return strcmp(left_match->txn_id, right_match->txn_id);
}

static int lc_pouch_disk_copy_staged_info(
    lc_pouch_disk_store *store, lc_pouch_staged_state_info *dst,
    const char *base_key, const lc_pouch_disk_staged_match *match) {
  memset(dst, 0, sizeof(*dst));
  dst->key = lc_pouch_strdup(&store->allocator, base_key);
  dst->txn_id = lc_pouch_strdup(&store->allocator, match->txn_id);
  dst->content_type =
      lc_pouch_strdup(&store->allocator, match->entry->content_type);
  dst->etag = lc_pouch_strdup(&store->allocator, match->entry->etag);
  if (dst->key == NULL || dst->txn_id == NULL ||
      (match->entry->content_type != NULL && dst->content_type == NULL) ||
      (match->entry->etag != NULL && dst->etag == NULL)) {
    lc_pouch_staged_state_info_cleanup(&store->allocator, dst);
    return 0;
  }
  dst->version = match->entry->version;
  dst->bytes = (long)match->entry->body_length;
  return 1;
}

static int lc_pouch_disk_list_staged_state(
    lc_pouch_store *self, const lc_pouch_list_staged_req *req,
    lc_pouch_staged_state_list *out, lc_error *error) {
  lc_pouch_disk_store *store;
  lc_pouch_disk_staged_match *matches;
  char *staged_prefix;
  char *base_key;
  size_t prefix_len;
  size_t match_count;
  size_t copy_count;
  size_t index;
  int rc;

  if (self == NULL || req == NULL || req->namespace_name == NULL ||
      req->key == NULL || out == NULL) {
    return lc_pouch_set_invalid(error,
                                "list_staged_state requires store, request, "
                                "namespace, key, and output");
  }
  rc = lc_pouch_disk_validate_name(error, "list_staged_state", "namespace",
                                   req->namespace_name);
  if (rc != LC_OK) {
    return rc;
  }
  store = (lc_pouch_disk_store *)self->impl;
  memset(out, 0, sizeof(*out));
  matches = NULL;
  staged_prefix = NULL;
  base_key = lc_pouch_disk_dup_trimmed_key(store, req->key);
  if (base_key == NULL) {
    lc_pouch_free(&store->allocator, base_key);
    return lc_pouch_set_nomem(error, "failed to allocate pouch staged key");
  }
  if (base_key[0] == '\0' && req->key[0] != '\0') {
    lc_pouch_free(&store->allocator, base_key);
    return lc_pouch_set_invalid(error, "list_staged_state requires valid key");
  }
  staged_prefix = lc_pouch_disk_make_staged_key(store, base_key, "");
  if (staged_prefix == NULL) {
    lc_pouch_free(&store->allocator, base_key);
    return lc_pouch_set_nomem(error, "failed to allocate pouch staged prefix");
  }
  prefix_len = strlen(staged_prefix);
  match_count = 0U;
  copy_count = 0U;

  rc = lc_pouch_disk_lock(store, error);
  if (rc != LC_OK) {
    lc_pouch_free(&store->allocator, staged_prefix);
    lc_pouch_free(&store->allocator, base_key);
    return rc;
  }
  if (store->state_entry_count > 0U) {
    matches = (lc_pouch_disk_staged_match *)lc_pouch_alloc(
        &store->allocator,
        store->state_entry_count * sizeof(lc_pouch_disk_staged_match));
    if (matches == NULL) {
      lc_pouch_disk_unlock(store, error);
      lc_pouch_free(&store->allocator, staged_prefix);
      lc_pouch_free(&store->allocator, base_key);
      return lc_pouch_set_nomem(error,
                                "failed to allocate pouch staged listing");
    }
  }
  for (index = 0U; index < store->state_entry_count; ++index) {
    lc_pouch_disk_state_entry *entry;
    const char *txn_id;

    entry = &store->state_entries[index];
    if (entry->deleted ||
        strcmp(entry->namespace_name, req->namespace_name) != 0 ||
        strncmp(entry->key, staged_prefix, prefix_len) != 0) {
      continue;
    }
    txn_id = entry->key + prefix_len;
    if (txn_id[0] == '\0' || strchr(txn_id, '/') != NULL) {
      continue;
    }
    if (req->start_after != NULL && strcmp(txn_id, req->start_after) <= 0) {
      continue;
    }
    matches[match_count].entry = entry;
    matches[match_count].txn_id = txn_id;
    match_count++;
  }
  if (match_count > 1U) {
    qsort(matches, match_count, sizeof(matches[0]),
          lc_pouch_disk_staged_match_compare);
  }
  copy_count = match_count;
  if (req->limit > 0U && copy_count > req->limit) {
    copy_count = req->limit;
    out->truncated = 1;
  }
  if (copy_count > 0U) {
    out->items = (lc_pouch_staged_state_info *)lc_pouch_calloc(
        &store->allocator, copy_count, sizeof(out->items[0]));
    if (out->items == NULL) {
      lc_pouch_free(&store->allocator, matches);
      lc_pouch_disk_unlock(store, error);
      lc_pouch_free(&store->allocator, staged_prefix);
      lc_pouch_free(&store->allocator, base_key);
      return lc_pouch_set_nomem(error,
                                "failed to allocate pouch staged rows");
    }
  }
  for (index = 0U; index < copy_count; ++index) {
    if (!lc_pouch_disk_copy_staged_info(store, &out->items[index], base_key,
                                        &matches[index])) {
      out->count = index;
      lc_pouch_staged_state_list_cleanup(&store->allocator, out);
      lc_pouch_free(&store->allocator, matches);
      lc_pouch_disk_unlock(store, error);
      lc_pouch_free(&store->allocator, staged_prefix);
      lc_pouch_free(&store->allocator, base_key);
      return lc_pouch_set_nomem(error, "failed to copy pouch staged row");
    }
    out->count++;
  }
  if (out->truncated && copy_count > 0U) {
    out->next_start_after =
        lc_pouch_strdup(&store->allocator, matches[copy_count - 1U].txn_id);
    if (out->next_start_after == NULL) {
      lc_pouch_staged_state_list_cleanup(&store->allocator, out);
      lc_pouch_free(&store->allocator, matches);
      lc_pouch_disk_unlock(store, error);
      lc_pouch_free(&store->allocator, staged_prefix);
      lc_pouch_free(&store->allocator, base_key);
      return lc_pouch_set_nomem(error,
                                "failed to allocate pouch staged cursor");
    }
  }
  lc_pouch_free(&store->allocator, matches);
  if (lc_pouch_disk_unlock(store, error) != LC_OK) {
    lc_pouch_staged_state_list_cleanup(&store->allocator, out);
    lc_pouch_free(&store->allocator, staged_prefix);
    lc_pouch_free(&store->allocator, base_key);
    return LC_ERR_TRANSPORT;
  }
  lc_pouch_free(&store->allocator, staged_prefix);
  lc_pouch_free(&store->allocator, base_key);
  return LC_OK;
}

static int lc_pouch_disk_put_object(lc_pouch_store *self,
                                    const char *namespace_name, const char *key,
                                    lc_source *body,
                                    const lc_pouch_put_object_opts *opts,
                                    lc_pouch_object_info *out,
                                    lc_error *error) {
  lc_pouch_disk_store *store;
  char *id;
  const char *name;
  const char *content_type;
  unsigned long payload_length;
  unsigned long payload_crc;
  unsigned long payload_offset;
  long now_unix;
  int existing;
  int temp_fd;
  int rc;

  if (self == NULL || namespace_name == NULL || key == NULL || body == NULL ||
      opts == NULL || opts->name == NULL || out == NULL) {
    return lc_pouch_set_invalid(error,
                                "put_object requires store, namespace, key, "
                                "body, name, and output metadata");
  }
  rc = lc_pouch_disk_validate_namespace_key(error, "put_object",
                                           namespace_name, key);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_disk_validate_name(error, "put_object", "name", opts->name);
  if (rc != LC_OK) {
    return rc;
  }
  store = (lc_pouch_disk_store *)self->impl;
  memset(out, 0, sizeof(*out));
  if (opts->has_max_bytes && opts->max_bytes < 0L) {
    return lc_pouch_set_invalid(error,
                                "put_object requires non-negative max_bytes");
  }
  rc = lc_pouch_disk_lock(store, error);
  if (rc != LC_OK) {
    return rc;
  }
  name = opts->name;
  content_type = opts->content_type != NULL ? opts->content_type
                                            : "application/octet-stream";
  existing =
      lc_pouch_disk_find_object_by_name(store, namespace_name, key, name);
  if (existing >= 0 && opts->prevent_overwrite) {
    lc_pouch_disk_unlock(store, error);
    return lc_error_set(error, LC_ERR_SERVER, 409L,
                        "pouch attachment already exists", NULL,
                        "attachment_exists", NULL);
  }
  temp_fd = -1;
  rc = lc_pouch_disk_spool_source_to_temp(
      store, body, opts->has_max_bytes, (size_t)opts->max_bytes, &temp_fd,
      &payload_length, &payload_crc, error);
  if (rc != LC_OK) {
    lc_pouch_disk_unlock(store, error);
    return rc;
  }
  id = lc_pouch_make_object_id_from_crc(store, name, payload_crc);
  if (id == NULL) {
    close(temp_fd);
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_nomem(error, "failed to allocate pouch object id");
  }
  now_unix = (long)time(NULL);
  rc = lc_pouch_disk_append_object_copy_record(
      store, temp_fd, namespace_name, key, name, id, content_type, 0UL,
      payload_length, now_unix, now_unix, &payload_offset, error);
  close(temp_fd);
  if (rc == LC_OK &&
      !lc_pouch_disk_upsert_object_entry(
          store, namespace_name, key, id, name, content_type,
          (long)payload_length, now_unix, now_unix,
          payload_offset, payload_length, 0)) {
    rc = lc_pouch_set_nomem(error, "failed to update pouch object index");
  }
  if (rc == LC_OK) {
    existing =
        lc_pouch_disk_find_object_by_name(store, namespace_name, key, name);
    if (existing >= 0 &&
        !lc_pouch_object_info_from_entry(&store->allocator, out,
                                         &store->object_entries[existing])) {
      rc = lc_pouch_set_nomem(error, "failed to copy pouch object metadata");
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_disk_mark_replayed_to_current_size(store, error);
  }
  lc_pouch_free(&store->allocator, id);
  if (lc_pouch_disk_unlock(store, error) != LC_OK && rc == LC_OK) {
    rc = LC_ERR_TRANSPORT;
  }
  return rc;
}

static int lc_pouch_disk_list_objects(lc_pouch_store *self,
                                      const char *namespace_name,
                                      const char *key,
                                      lc_pouch_object_list *out,
                                      lc_error *error) {
  lc_pouch_disk_store *store;
  lc_pouch_disk_object_entry **matches;
  size_t index;
  size_t count;
  int rc;

  if (self == NULL || namespace_name == NULL || key == NULL || out == NULL) {
    return lc_pouch_set_invalid(error,
                                "list_objects requires store, namespace, key, "
                                "and output list");
  }
  rc = lc_pouch_disk_validate_namespace_key(error, "list_objects",
                                           namespace_name, key);
  if (rc != LC_OK) {
    return rc;
  }
  store = (lc_pouch_disk_store *)self->impl;
  memset(out, 0, sizeof(*out));
  matches = NULL;
  rc = lc_pouch_disk_lock(store, error);
  if (rc != LC_OK) {
    return rc;
  }
  count = 0U;
  for (index = 0U; index < store->object_entry_count; ++index) {
    if (!store->object_entries[index].deleted &&
        strcmp(store->object_entries[index].namespace_name, namespace_name) ==
            0 &&
        strcmp(store->object_entries[index].key, key) == 0) {
      ++count;
    }
  }
  if (count == 0U) {
    if (lc_pouch_disk_unlock(store, error) != LC_OK) {
      return LC_ERR_TRANSPORT;
    }
    return LC_OK;
  }
  matches = (lc_pouch_disk_object_entry **)lc_pouch_calloc(
      &store->allocator, count, sizeof(matches[0]));
  if (matches == NULL) {
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_nomem(error, "failed to allocate pouch object refs");
  }
  out->items = (lc_pouch_object_info *)lc_pouch_calloc(&store->allocator, count,
                                                       sizeof(out->items[0]));
  if (out->items == NULL) {
    lc_pouch_free(&store->allocator, matches);
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_nomem(error, "failed to allocate pouch object list");
  }
  count = 0U;
  for (index = 0U; index < store->object_entry_count; ++index) {
    if (!store->object_entries[index].deleted &&
        strcmp(store->object_entries[index].namespace_name, namespace_name) ==
            0 &&
        strcmp(store->object_entries[index].key, key) == 0) {
      matches[count++] = &store->object_entries[index];
    }
  }
  qsort(matches, count, sizeof(matches[0]),
        lc_pouch_disk_object_entry_ptr_compare);
  out->count = count;
  for (index = 0U; index < count; ++index) {
    if (!lc_pouch_object_info_from_entry(&store->allocator, &out->items[index],
                                         matches[index])) {
      lc_pouch_object_list_cleanup(&store->allocator, out);
      lc_pouch_free(&store->allocator, matches);
      lc_pouch_disk_unlock(store, error);
      return lc_pouch_set_nomem(error, "failed to copy pouch object list");
    }
  }
  lc_pouch_free(&store->allocator, matches);
  if (lc_pouch_disk_unlock(store, error) != LC_OK) {
    lc_pouch_object_list_cleanup(&store->allocator, out);
    return LC_ERR_TRANSPORT;
  }
  return LC_OK;
}

static int lc_pouch_disk_get_object(lc_pouch_store *self,
                                    const char *namespace_name, const char *key,
                                    const lc_pouch_object_selector *selector,
                                    lc_source **body, lc_pouch_object_info *out,
                                    lc_error *error) {
  lc_pouch_disk_store *store;
  lc_pouch_disk_object_entry *entry;
  lc_source *source_pub;
  lc_pouch_file_source *source;
  int index;
  int fd;

  if (self == NULL || namespace_name == NULL || key == NULL ||
      selector == NULL || body == NULL || out == NULL) {
    return lc_pouch_set_invalid(error,
                                "get_object requires store, namespace, key, "
                                "selector, body, and output metadata");
  }
  index = lc_pouch_disk_validate_namespace_key(error, "get_object",
                                               namespace_name, key);
  if (index != LC_OK) {
    return index;
  }
  store = (lc_pouch_disk_store *)self->impl;
  memset(out, 0, sizeof(*out));
  *body = NULL;
  index = lc_pouch_disk_lock(store, error);
  if (index != LC_OK) {
    return index;
  }
  index = lc_pouch_disk_find_object(store, namespace_name, key, selector);
  if (index < 0) {
    lc_pouch_disk_unlock(store, error);
    return lc_error_set(error, LC_ERR_SERVER, 404L,
                        "pouch attachment was not found", NULL, "not_found",
                        NULL);
  }
  entry = &store->object_entries[index];
  fd = open(store->log_path, O_RDONLY);
  if (fd < 0) {
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_errno(error,
                              "failed to open pouch log for object read");
  }
  if (lseek(fd, (off_t)entry->body_offset, SEEK_SET) < 0) {
    close(fd);
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_errno(error, "failed to seek pouch object body");
  }
  source_pub =
      (lc_source *)lc_pouch_calloc(&store->allocator, 1U, sizeof(*source_pub));
  source = (lc_pouch_file_source *)lc_pouch_calloc(&store->allocator, 1U,
                                                   sizeof(*source));
  if (source_pub == NULL || source == NULL) {
    close(fd);
    lc_pouch_free(&store->allocator, source_pub);
    lc_pouch_free(&store->allocator, source);
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_nomem(error, "failed to allocate pouch object source");
  }
  source->allocator = store->allocator;
  source->fd = fd;
  source->remaining = entry->body_length;
  source_pub->read = lc_pouch_file_source_read;
  source_pub->reset = lc_pouch_file_source_reset;
  source_pub->close = lc_pouch_file_source_close;
  source_pub->impl = source;
  if (!lc_pouch_object_info_from_entry(&store->allocator, out, entry)) {
    source_pub->close(source_pub);
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_nomem(error, "failed to copy pouch object metadata");
  }
  *body = source_pub;
  if (lc_pouch_disk_unlock(store, error) != LC_OK) {
    source_pub->close(source_pub);
    *body = NULL;
    lc_pouch_object_info_cleanup(&store->allocator, out);
    return LC_ERR_TRANSPORT;
  }
  return LC_OK;
}

static int lc_pouch_disk_copy_object(lc_pouch_store *self,
                                     const char *namespace_name,
                                     const char *src_key, const char *dst_key,
                                     const lc_pouch_copy_object_opts *opts,
                                     lc_pouch_object_info *out,
                                     lc_error *error) {
  lc_pouch_disk_store *store;
  lc_pouch_disk_object_entry *src_entry;
  char *id;
  char *src_content_type;
  const char *name;
  unsigned long payload_crc;
  unsigned long payload_offset;
  unsigned long src_body_offset;
  unsigned long src_body_length;
  long src_size;
  long now_unix;
  int existing;
  int read_fd;
  int rc;

  if (self == NULL || namespace_name == NULL || src_key == NULL ||
      dst_key == NULL || opts == NULL || out == NULL) {
    return lc_pouch_set_invalid(error,
                                "copy_object requires store, namespace, "
                                "source key, destination key, options, and "
                                "output metadata");
  }
  rc = lc_pouch_disk_validate_name(error, "copy_object", "namespace",
                                   namespace_name);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_disk_validate_name(error, "copy_object", "source key",
                                   src_key);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_disk_validate_name(error, "copy_object", "destination key",
                                   dst_key);
  if (rc != LC_OK) {
    return rc;
  }
  if (opts->name != NULL) {
    rc = lc_pouch_disk_validate_name(error, "copy_object", "name", opts->name);
    if (rc != LC_OK) {
      return rc;
    }
  }
  memset(out, 0, sizeof(*out));
  store = (lc_pouch_disk_store *)self->impl;
  id = NULL;
  src_content_type = NULL;
  payload_offset = 0UL;
  read_fd = -1;

  rc = lc_pouch_disk_lock(store, error);
  if (rc != LC_OK) {
    return rc;
  }
  existing = lc_pouch_disk_find_object(store, namespace_name, src_key,
                                       &opts->source);
  if (existing < 0) {
    lc_pouch_disk_unlock(store, error);
    return lc_error_set(error, LC_ERR_SERVER, 404L,
                        "pouch attachment was not found", NULL, "not_found",
                        NULL);
  }
  src_entry = &store->object_entries[existing];
  name = opts->name != NULL ? opts->name : src_entry->name;
  src_content_type = lc_pouch_strdup(&store->allocator,
                                     src_entry->content_type);
  if (src_content_type == NULL) {
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_nomem(error,
                              "failed to copy pouch object content type");
  }
  src_body_offset = src_entry->body_offset;
  src_body_length = src_entry->body_length;
  src_size = src_entry->size;
  read_fd = open(store->log_path, O_RDONLY);
  if (read_fd < 0) {
    lc_pouch_free(&store->allocator, src_content_type);
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_errno(error,
                              "failed to open pouch log for object copy");
  }
  payload_crc = 0xffffffffUL;
  rc = lc_pouch_disk_crc_fd_span(read_fd, src_body_offset, src_body_length,
                                 &payload_crc, error);
  if (rc != LC_OK) {
    close(read_fd);
    lc_pouch_free(&store->allocator, src_content_type);
    lc_pouch_disk_unlock(store, error);
    return rc;
  }
  payload_crc ^= 0xffffffffUL;
  existing = lc_pouch_disk_find_object_by_name(store, namespace_name, dst_key,
                                               name);
  if (existing >= 0 && opts->prevent_overwrite) {
    close(read_fd);
    lc_pouch_free(&store->allocator, src_content_type);
    lc_pouch_disk_unlock(store, error);
    return lc_error_set(error, LC_ERR_SERVER, 409L,
                        "pouch attachment already exists", NULL,
                        "attachment_exists", NULL);
  }
  id = lc_pouch_make_object_id_from_crc(store, name, payload_crc);
  if (id == NULL) {
    close(read_fd);
    lc_pouch_free(&store->allocator, src_content_type);
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_nomem(error, "failed to allocate pouch object id");
  }
  now_unix = (long)time(NULL);
  rc = lc_pouch_disk_append_object_copy_record(
      store, read_fd, namespace_name, dst_key, name, id,
      src_content_type, src_body_offset, src_body_length, now_unix, now_unix,
      &payload_offset, error);
  close(read_fd);
  if (rc == LC_OK &&
      !lc_pouch_disk_upsert_object_entry(
          store, namespace_name, dst_key, id, name, src_content_type, src_size,
          now_unix, now_unix, payload_offset, src_body_length, 0)) {
    rc = lc_pouch_set_nomem(error, "failed to update pouch object index");
  }
  if (rc == LC_OK) {
    existing = lc_pouch_disk_find_object_by_name(store, namespace_name, dst_key,
                                                 name);
    if (existing >= 0 &&
        !lc_pouch_object_info_from_entry(&store->allocator, out,
                                         &store->object_entries[existing])) {
      rc = lc_pouch_set_nomem(error, "failed to copy pouch object metadata");
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_disk_mark_replayed_to_current_size(store, error);
  }
  lc_pouch_free(&store->allocator, id);
  lc_pouch_free(&store->allocator, src_content_type);
  if (lc_pouch_disk_unlock(store, error) != LC_OK && rc == LC_OK) {
    rc = LC_ERR_TRANSPORT;
  }
  return rc;
}

static int lc_pouch_disk_delete_object(lc_pouch_store *self,
                                       const char *namespace_name,
                                       const char *key,
                                       const lc_pouch_object_selector *selector,
                                       int *deleted, lc_error *error) {
  lc_pouch_disk_store *store;
  lc_pouch_disk_object_entry *entry;
  unsigned long body_offset;
  int index;
  int rc;

  if (self == NULL || namespace_name == NULL || key == NULL ||
      selector == NULL || deleted == NULL) {
    return lc_pouch_set_invalid(error,
                                "delete_object requires store, namespace, key, "
                                "selector, and deleted output");
  }
  rc = lc_pouch_disk_validate_namespace_key(error, "delete_object",
                                           namespace_name, key);
  if (rc != LC_OK) {
    return rc;
  }
  store = (lc_pouch_disk_store *)self->impl;
  *deleted = 0;
  rc = lc_pouch_disk_lock(store, error);
  if (rc != LC_OK) {
    return rc;
  }
  index = lc_pouch_disk_find_object(store, namespace_name, key, selector);
  if (index >= 0) {
    entry = &store->object_entries[index];
    rc = lc_pouch_disk_append_record(
        store, LC_POUCH_RECORD_OBJECT_REMOVE, namespace_name, key, entry->name,
        entry->id, 0L, NULL, 0U, &body_offset, error);
    if (rc == LC_OK) {
      entry->deleted = 1;
      *deleted = 1;
    }
  }
  if (rc == LC_OK && *deleted) {
    rc = lc_pouch_disk_mark_replayed_to_current_size(store, error);
  }
  if (lc_pouch_disk_unlock(store, error) != LC_OK && rc == LC_OK) {
    rc = LC_ERR_TRANSPORT;
  }
  return rc;
}

static int lc_pouch_disk_delete_all_objects(lc_pouch_store *self,
                                            const char *namespace_name,
                                            const char *key, int *deleted_count,
                                            lc_error *error) {
  lc_pouch_disk_store *store;
  unsigned long body_offset;
  size_t index;
  int rc;

  if (self == NULL || namespace_name == NULL || key == NULL ||
      deleted_count == NULL) {
    return lc_pouch_set_invalid(error,
                                "delete_all_objects requires store, namespace, "
                                "key, and deleted_count");
  }
  rc = lc_pouch_disk_validate_namespace_key(error, "delete_all_objects",
                                           namespace_name, key);
  if (rc != LC_OK) {
    return rc;
  }
  store = (lc_pouch_disk_store *)self->impl;
  *deleted_count = 0;
  rc = lc_pouch_disk_lock(store, error);
  if (rc != LC_OK) {
    return rc;
  }
  for (index = 0U; index < store->object_entry_count; ++index) {
    if (!store->object_entries[index].deleted &&
        strcmp(store->object_entries[index].namespace_name, namespace_name) ==
            0 &&
        strcmp(store->object_entries[index].key, key) == 0) {
      rc = lc_pouch_disk_append_record(
          store, LC_POUCH_RECORD_OBJECT_REMOVE, namespace_name, key,
          store->object_entries[index].name, store->object_entries[index].id,
          0L, NULL, 0U, &body_offset, error);
      if (rc != LC_OK) {
        break;
      }
      store->object_entries[index].deleted = 1;
      ++(*deleted_count);
    }
  }
  if (rc == LC_OK && *deleted_count > 0) {
    rc = lc_pouch_disk_mark_replayed_to_current_size(store, error);
  }
  if (lc_pouch_disk_unlock(store, error) != LC_OK && rc == LC_OK) {
    rc = LC_ERR_TRANSPORT;
  }
  return rc;
}

static int lc_pouch_disk_queue_ref_valid(lc_pouch_disk_queue_entry *entry,
                                         const lc_pouch_queue_ref *ref,
                                         lc_error *error) {
  long now_unix;

  if (entry == NULL || entry->deleted || ref == NULL || ref->lease_id == NULL ||
      entry->lease_id == NULL || strcmp(entry->lease_id, ref->lease_id) != 0 ||
      entry->fencing_token != ref->fencing_token ||
      (ref->meta_etag != NULL && entry->meta_etag != NULL &&
       strcmp(entry->meta_etag, ref->meta_etag) != 0)) {
    return lc_error_set(error, LC_ERR_SERVER, 409L,
                        "pouch queue message lease is not active", NULL,
                        "queue_lease_not_active", NULL);
  }
  now_unix = (long)time(NULL);
  if (entry->lease_expires_at_unix <= now_unix) {
    return lc_error_set(error, LC_ERR_SERVER, 409L,
                        "pouch queue message lease has expired", NULL,
                        "queue_lease_expired", NULL);
  }
  if (entry->txn_id != NULL && ref->txn_id == NULL) {
    return lc_error_set(error, LC_ERR_SERVER, 400L,
                        "pouch queue operation requires transaction id for "
                        "this message lease",
                        NULL, "missing_txn", NULL);
  }
  if (entry->txn_id != NULL && ref->txn_id != NULL &&
      strcmp(entry->txn_id, ref->txn_id) != 0) {
    return lc_error_set(error, LC_ERR_SERVER, 409L,
                        "pouch queue transaction id does not match active "
                        "message lease",
                        NULL, "txn_mismatch", NULL);
  }
  return LC_OK;
}

static int lc_pouch_disk_validate_queue_ref(lc_error *error,
                                            const char *operation,
                                            const lc_pouch_queue_ref *ref) {
  int rc;

  if (ref == NULL || ref->namespace_name == NULL || ref->queue == NULL ||
      ref->message_id == NULL || ref->lease_id == NULL ||
      ref->fencing_token <= 0L || ref->meta_etag == NULL) {
    return lc_pouch_set_invalid(error, operation);
  }
  rc = lc_pouch_disk_validate_namespace_queue(error, operation,
                                              ref->namespace_name, ref->queue);
  if (rc != LC_OK) {
    return rc;
  }
  return lc_pouch_disk_validate_name(error, operation, "message id",
                                     ref->message_id);
}

static int lc_pouch_disk_append_queue_entry(lc_pouch_disk_store *store,
                                            unsigned long record_type,
                                            lc_pouch_disk_queue_entry *entry,
                                            const unsigned char *payload,
                                            size_t payload_length, int has_body,
                                            lc_error *error) {
  unsigned char *record_body;
  size_t record_body_length;
  unsigned long body_offset;
  int rc;

  record_body = NULL;
  record_body_length = 0U;
  rc = lc_pouch_encode_queue_record(store, entry, has_body ? payload : NULL,
                                    has_body ? payload_length : 0U,
                                    &record_body, &record_body_length, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_disk_append_record(
      store, record_type, entry->namespace_name, entry->queue,
      entry->message_id, entry->meta_etag, entry->fencing_token, record_body,
      record_body_length, &body_offset, error);
  if (rc == LC_OK &&
      !lc_pouch_disk_upsert_queue_entry(
          store, entry->namespace_name, entry->queue, entry->message_id,
          entry->payload_content_type, entry->lease_id, entry->txn_id,
          entry->meta_etag, entry->attempts, entry->max_attempts,
          entry->failure_attempts, entry->enqueued_at_unix,
          entry->not_visible_until_unix, entry->visibility_timeout_seconds,
          entry->expires_at_unix, entry->lease_expires_at_unix,
          entry->fencing_token,
          body_offset + LC_POUCH_QUEUE_META_SIZE +
              strlen(entry->payload_content_type != NULL
                         ? entry->payload_content_type
                         : "") +
              strlen(entry->lease_id != NULL ? entry->lease_id : "") +
              strlen(entry->txn_id != NULL ? entry->txn_id : ""),
          (unsigned long)payload_length, has_body, entry->deleted)) {
    rc = lc_pouch_set_nomem(error, "failed to update pouch queue index");
  }
  lc_pouch_free(&store->allocator, record_body);
  return rc;
}

static int lc_pouch_disk_append_queue_put_fd_entry(
    lc_pouch_disk_store *store, lc_pouch_disk_queue_entry *entry,
    int payload_fd, unsigned long payload_offset, unsigned long payload_length,
    lc_error *error) {
  unsigned char header[LC_POUCH_HEADER_SIZE];
  unsigned char queue_meta[LC_POUCH_QUEUE_META_SIZE];
  const char *content_type;
  const char *lease_id;
  const char *txn_id;
  unsigned long ns_len;
  unsigned long queue_len;
  unsigned long message_id_len;
  unsigned long meta_etag_len;
  unsigned long content_type_len;
  unsigned long lease_id_len;
  unsigned long txn_id_len;
  unsigned long body_length;
  unsigned long payload_len;
  unsigned long crc;
  unsigned long body_offset;
  off_t start;
  int rc;

  content_type = entry->payload_content_type != NULL
                     ? entry->payload_content_type
                     : "application/octet-stream";
  lease_id = entry->lease_id != NULL ? entry->lease_id : "";
  txn_id = entry->txn_id != NULL ? entry->txn_id : "";
  ns_len = (unsigned long)strlen(entry->namespace_name);
  queue_len = (unsigned long)strlen(entry->queue);
  message_id_len = (unsigned long)strlen(entry->message_id);
  meta_etag_len =
      entry->meta_etag != NULL ? (unsigned long)strlen(entry->meta_etag) : 0UL;
  content_type_len = (unsigned long)strlen(content_type);
  lease_id_len = (unsigned long)strlen(lease_id);
  txn_id_len = (unsigned long)strlen(txn_id);

  if (lc_pouch_disk_add_overflows(LC_POUCH_QUEUE_META_SIZE, content_type_len,
                                  &body_length) ||
      lc_pouch_disk_add_overflows(body_length, lease_id_len, &body_length) ||
      lc_pouch_disk_add_overflows(body_length, txn_id_len, &body_length) ||
      lc_pouch_disk_add_overflows(body_length, payload_length, &body_length) ||
      lc_pouch_disk_add_overflows(ns_len, queue_len, &payload_len) ||
      lc_pouch_disk_add_overflows(payload_len, message_id_len, &payload_len) ||
      lc_pouch_disk_add_overflows(payload_len, meta_etag_len, &payload_len) ||
      lc_pouch_disk_add_overflows(payload_len, body_length, &payload_len) ||
      !lc_pouch_disk_validate_record_lengths(ns_len, queue_len, message_id_len,
                                             meta_etag_len, body_length,
                                             payload_len)) {
    return lc_pouch_set_invalid(error,
                                "pouch log record exceeds inline limits");
  }

  lc_pouch_put_u32(queue_meta, entry->deleted ? 1UL : 0UL);
  lc_pouch_put_u32(queue_meta + 4, (unsigned long)entry->attempts);
  lc_pouch_put_u32(queue_meta + 8, (unsigned long)entry->max_attempts);
  lc_pouch_put_u32(queue_meta + 12, (unsigned long)entry->failure_attempts);
  lc_pouch_put_u64(queue_meta + 16, (unsigned long)entry->enqueued_at_unix);
  lc_pouch_put_u64(queue_meta + 24,
                   (unsigned long)entry->not_visible_until_unix);
  lc_pouch_put_u64(queue_meta + 32,
                   (unsigned long)entry->visibility_timeout_seconds);
  lc_pouch_put_u64(queue_meta + 40, (unsigned long)entry->expires_at_unix);
  lc_pouch_put_u64(queue_meta + 48,
                   (unsigned long)entry->lease_expires_at_unix);
  lc_pouch_put_u64(queue_meta + 56, payload_length);
  lc_pouch_put_u64(queue_meta + 64, content_type_len);
  lc_pouch_put_u64(queue_meta + 72, lease_id_len);
  lc_pouch_put_u64(queue_meta + 80, txn_id_len);

  crc = 0xffffffffUL;
  crc = lc_pouch_crc32_update(crc, (const unsigned char *)entry->namespace_name,
                              (size_t)ns_len);
  crc = lc_pouch_crc32_update(crc, (const unsigned char *)entry->queue,
                              (size_t)queue_len);
  crc = lc_pouch_crc32_update(crc, (const unsigned char *)entry->message_id,
                              (size_t)message_id_len);
  if (meta_etag_len > 0UL) {
    crc = lc_pouch_crc32_update(crc, (const unsigned char *)entry->meta_etag,
                                (size_t)meta_etag_len);
  }
  crc = lc_pouch_crc32_update(crc, queue_meta, sizeof(queue_meta));
  crc = lc_pouch_crc32_update(crc, (const unsigned char *)content_type,
                              (size_t)content_type_len);
  if (lease_id_len > 0UL) {
    crc = lc_pouch_crc32_update(crc, (const unsigned char *)lease_id,
                                (size_t)lease_id_len);
  }
  if (txn_id_len > 0UL) {
    crc = lc_pouch_crc32_update(crc, (const unsigned char *)txn_id,
                                (size_t)txn_id_len);
  }
  rc = lc_pouch_disk_crc_fd_span(payload_fd, payload_offset, payload_length,
                                 &crc, error);
  if (rc != LC_OK) {
    return rc;
  }

  start = lseek(store->log_fd, 0, SEEK_END);
  if (start < 0) {
    return lc_pouch_set_errno(error, "failed to seek pouch log");
  }
  memset(header, 0, sizeof(header));
  memcpy(header, LC_POUCH_LOG_MAGIC, 4U);
  lc_pouch_put_u32(header + 4, LC_POUCH_HEADER_SIZE);
  lc_pouch_put_u32(header + 8, LC_POUCH_RECORD_QUEUE_PUT);
  lc_pouch_put_u32(header + 12, ns_len);
  lc_pouch_put_u32(header + 16, queue_len);
  lc_pouch_put_u32(header + 20, message_id_len);
  lc_pouch_put_u32(header + 24, meta_etag_len);
  lc_pouch_put_u64(header + 28, body_length);
  lc_pouch_put_u64(header + 36, (unsigned long)entry->fencing_token);
  lc_pouch_put_u64(header + 44, payload_len);
  lc_pouch_put_u32(header + 52, crc ^ 0xffffffffUL);
  lc_pouch_put_u32(header + 56, LC_POUCH_RECORD_VERSION);

  if (!lc_pouch_write_all(store->log_fd, header, sizeof(header)) ||
      !lc_pouch_write_all(store->log_fd, entry->namespace_name,
                          (size_t)ns_len) ||
      !lc_pouch_write_all(store->log_fd, entry->queue, (size_t)queue_len) ||
      !lc_pouch_write_all(store->log_fd, entry->message_id,
                          (size_t)message_id_len) ||
      (meta_etag_len > 0UL &&
       !lc_pouch_write_all(store->log_fd, entry->meta_etag,
                           (size_t)meta_etag_len)) ||
      !lc_pouch_write_all(store->log_fd, queue_meta, sizeof(queue_meta)) ||
      !lc_pouch_write_all(store->log_fd, content_type,
                          (size_t)content_type_len) ||
      (lease_id_len > 0UL &&
       !lc_pouch_write_all(store->log_fd, lease_id, (size_t)lease_id_len)) ||
      (txn_id_len > 0UL &&
       !lc_pouch_write_all(store->log_fd, txn_id, (size_t)txn_id_len))) {
    return lc_pouch_set_errno(error, "failed to append pouch queue record");
  }
  rc = lc_pouch_disk_write_fd_span(store->log_fd, payload_fd, payload_offset,
                                   payload_length, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_disk_fsync_record(store, "failed to fsync pouch log", error);
  if (rc != LC_OK) {
    return rc;
  }
  body_offset = (unsigned long)start + LC_POUCH_HEADER_SIZE + ns_len +
                queue_len + message_id_len + meta_etag_len;
  if (!lc_pouch_disk_upsert_queue_entry(
          store, entry->namespace_name, entry->queue, entry->message_id,
          content_type, entry->lease_id, entry->txn_id,
          entry->meta_etag, entry->attempts, entry->max_attempts,
          entry->failure_attempts, entry->enqueued_at_unix,
          entry->not_visible_until_unix, entry->visibility_timeout_seconds,
          entry->expires_at_unix, entry->lease_expires_at_unix,
          entry->fencing_token,
          body_offset + LC_POUCH_QUEUE_META_SIZE + content_type_len +
              lease_id_len + txn_id_len,
          payload_length, 1, entry->deleted)) {
    return lc_pouch_set_nomem(error, "failed to update pouch queue index");
  }
  store->replayed_log_size = (unsigned long)-1;
  store->replayed_record_count++;
  return LC_OK;
}

static int lc_pouch_disk_enqueue_message(lc_pouch_store *self,
                                         const char *namespace_name,
                                         const char *queue, lc_source *body,
                                         const lc_pouch_enqueue_opts *opts,
                                         lc_pouch_queue_message_info *out,
                                         lc_error *error) {
  lc_pouch_disk_store *store;
  lc_pouch_disk_queue_entry entry;
  unsigned long payload_length;
  unsigned long payload_crc;
  long now_unix;
  int temp_fd;
  int rc;

  if (self == NULL || namespace_name == NULL || queue == NULL || body == NULL ||
      opts == NULL || out == NULL) {
    return lc_pouch_set_invalid(
        error, "enqueue_message requires store, namespace, queue, body, opts, "
               "and out");
  }
  rc = lc_pouch_disk_validate_namespace_queue(error, "enqueue_message",
                                              namespace_name, queue);
  if (rc != LC_OK) {
    return rc;
  }
  store = (lc_pouch_disk_store *)self->impl;
  memset(out, 0, sizeof(*out));
  memset(&entry, 0, sizeof(entry));
  temp_fd = -1;
  payload_length = 0UL;
  payload_crc = 0UL;
  rc = lc_pouch_disk_spool_source_to_temp(store, body, 0, 0U, &temp_fd,
                                          &payload_length, &payload_crc, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_disk_lock(store, error);
  if (rc != LC_OK) {
    close(temp_fd);
    return rc;
  }
  now_unix = (long)time(NULL);
  entry.namespace_name = (char *)namespace_name;
  entry.queue = (char *)queue;
  entry.message_id =
      lc_pouch_make_queue_message_id(store, queue, store->next_version);
  entry.payload_content_type =
      (char *)(opts->content_type != NULL ? opts->content_type
                                          : "application/octet-stream");
  entry.lease_id = NULL;
  entry.txn_id = NULL;
  entry.attempts = 0;
  entry.max_attempts = opts->max_attempts > 0 ? opts->max_attempts : 1;
  entry.failure_attempts = 0;
  entry.enqueued_at_unix = now_unix;
  entry.not_visible_until_unix =
      now_unix + (opts->delay_seconds > 0L ? opts->delay_seconds : 0L);
  entry.visibility_timeout_seconds = opts->visibility_timeout_seconds > 0L
                                         ? opts->visibility_timeout_seconds
                                         : 30L;
  entry.expires_at_unix =
      now_unix + (opts->ttl_seconds > 0L ? opts->ttl_seconds : 86400L);
  entry.lease_expires_at_unix = 0L;
  entry.fencing_token = store->next_version++;
  entry.meta_etag =
      lc_pouch_make_etag_from_crc(store, entry.fencing_token, payload_crc);
  entry.body_length = payload_length;
  if (entry.message_id == NULL || entry.meta_etag == NULL) {
    lc_pouch_free(&store->allocator, entry.message_id);
    lc_pouch_free(&store->allocator, entry.meta_etag);
    close(temp_fd);
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_nomem(error, "failed to allocate pouch queue ids");
  }
  rc = lc_pouch_disk_append_queue_put_fd_entry(store, &entry, temp_fd, 0UL,
                                               payload_length, error);
  if (rc == LC_OK) {
    int index;

    index = lc_pouch_disk_find_queue_entry(store, namespace_name, queue,
                                           entry.message_id);
    if (index >= 0 &&
        !lc_pouch_queue_info_from_entry(&store->allocator, out,
                                        &store->queue_entries[index])) {
      rc = lc_pouch_set_nomem(error, "failed to copy pouch queue message");
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_disk_mark_replayed_to_current_size(store, error);
  }
  lc_pouch_free(&store->allocator, entry.message_id);
  lc_pouch_free(&store->allocator, entry.meta_etag);
  close(temp_fd);
  if (lc_pouch_disk_unlock(store, error) != LC_OK && rc == LC_OK) {
    rc = LC_ERR_TRANSPORT;
  }
  return rc;
}

static int lc_pouch_disk_dequeue_message(
    lc_pouch_store *self, const char *namespace_name, const char *queue,
    const lc_pouch_dequeue_opts *opts, lc_source **body,
    lc_pouch_queue_message_info *out, lc_error *error) {
  lc_pouch_disk_store *store;
  lc_pouch_disk_queue_entry *entry;
  lc_pouch_disk_queue_entry updated;
  lc_source *source_pub;
  lc_pouch_file_source *source;
  char *lease_id;
  char *message_id;
  char *meta_etag;
  char *txn_id;
  long now_unix;
  size_t index;
  int found;
  int fd;
  int rc;

  if (self == NULL || namespace_name == NULL || queue == NULL || opts == NULL ||
      opts->owner == NULL || body == NULL || out == NULL) {
    return lc_pouch_set_invalid(
        error, "dequeue_message requires store, namespace, queue, owner, body, "
               "and out");
  }
  rc = lc_pouch_disk_validate_namespace_queue(error, "dequeue_message",
                                              namespace_name, queue);
  if (rc != LC_OK) {
    return rc;
  }
  store = (lc_pouch_disk_store *)self->impl;
  memset(out, 0, sizeof(*out));
  *body = NULL;
  fd = -1;
  source_pub = NULL;
  source = NULL;
  rc = lc_pouch_disk_lock(store, error);
  if (rc != LC_OK) {
    return rc;
  }
  now_unix = (long)time(NULL);
  found = -1;
  for (index = 0U; index < store->queue_entry_count; ++index) {
    entry = &store->queue_entries[index];
    if (!entry->deleted && strcmp(entry->namespace_name, namespace_name) == 0 &&
        strcmp(entry->queue, queue) == 0 &&
        entry->not_visible_until_unix <= now_unix &&
        entry->expires_at_unix > now_unix &&
        entry->failure_attempts < entry->max_attempts &&
        lc_pouch_disk_queue_entry_before(
            entry, found >= 0 ? &store->queue_entries[found] : NULL)) {
      found = (int)index;
    }
  }
  if (found < 0) {
    lc_pouch_disk_unlock(store, error);
    return LC_OK;
  }
  entry = &store->queue_entries[found];
  message_id = lc_pouch_strdup(&store->allocator, entry->message_id);
  if (message_id == NULL) {
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_nomem(error, "failed to copy pouch queue message id");
  }
  lease_id = lc_pouch_make_queue_lease_id(store, entry->message_id,
                                          entry->fencing_token + 1L);
  meta_etag = lc_pouch_make_etag(store, entry->fencing_token + 1L,
                                 entry->message_id, strlen(entry->message_id));
  if (lease_id == NULL || meta_etag == NULL) {
    lc_pouch_free(&store->allocator, message_id);
    lc_pouch_free(&store->allocator, lease_id);
    lc_pouch_free(&store->allocator, meta_etag);
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_nomem(error, "failed to allocate pouch queue lease");
  }
  txn_id = lc_pouch_strdup(&store->allocator, opts->txn_id);
  if (opts->txn_id != NULL && txn_id == NULL) {
    lc_pouch_free(&store->allocator, message_id);
    lc_pouch_free(&store->allocator, lease_id);
    lc_pouch_free(&store->allocator, meta_etag);
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_nomem(error, "failed to allocate pouch queue txn id");
  }
  updated = *entry;
  updated.lease_id = lease_id;
  updated.txn_id = txn_id;
  updated.meta_etag = meta_etag;
  updated.attempts += 1;
  updated.fencing_token += 1L;
  updated.visibility_timeout_seconds =
      opts->visibility_timeout_seconds > 0L ? opts->visibility_timeout_seconds
                                            : entry->visibility_timeout_seconds;
  updated.not_visible_until_unix = now_unix + updated.visibility_timeout_seconds;
  updated.lease_expires_at_unix = updated.not_visible_until_unix;
  rc = lc_pouch_disk_append_queue_entry(store, LC_POUCH_RECORD_QUEUE_UPDATE,
                                        &updated, NULL, 0U, 0, error);
  lc_pouch_free(&store->allocator, lease_id);
  lc_pouch_free(&store->allocator, txn_id);
  lc_pouch_free(&store->allocator, meta_etag);
  if (rc == LC_OK) {
    rc = lc_pouch_disk_mark_replayed_to_current_size(store, error);
  }
  if (rc == LC_OK) {
    found = lc_pouch_disk_find_queue_entry(store, namespace_name, queue,
                                           message_id);
    if (found < 0) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to refresh pouch queue lease after compaction",
                        NULL, NULL, NULL);
    } else {
      entry = &store->queue_entries[found];
    }
  }
  if (rc == LC_OK) {
    fd = open(store->log_path, O_RDONLY);
    if (fd < 0) {
      rc = lc_pouch_set_errno(error,
                              "failed to open pouch log for queue read");
    }
  }
  if (rc == LC_OK && lseek(fd, (off_t)entry->body_offset, SEEK_SET) < 0) {
    close(fd);
    fd = -1;
    rc = lc_pouch_set_errno(error, "failed to seek pouch queue payload");
  }
  if (rc == LC_OK) {
    source_pub =
        (lc_source *)lc_pouch_calloc(&store->allocator, 1U, sizeof(*source_pub));
    source = (lc_pouch_file_source *)lc_pouch_calloc(&store->allocator, 1U,
                                                     sizeof(*source));
    if (source_pub == NULL || source == NULL) {
      close(fd);
      fd = -1;
      lc_pouch_free(&store->allocator, source_pub);
      lc_pouch_free(&store->allocator, source);
      source_pub = NULL;
      source = NULL;
      rc = lc_pouch_set_nomem(error, "failed to allocate pouch queue source");
    }
  }
  if (rc == LC_OK) {
    source->allocator = store->allocator;
    source->fd = fd;
    source->remaining = entry->body_length;
    source_pub->read = lc_pouch_file_source_read;
    source_pub->reset = lc_pouch_file_source_reset;
    source_pub->close = lc_pouch_file_source_close;
    source_pub->impl = source;
    fd = -1;
    source = NULL;
    if (!lc_pouch_queue_info_from_entry(&store->allocator, out, entry)) {
      source_pub->close(source_pub);
      source_pub = NULL;
      rc = lc_pouch_set_nomem(error, "failed to copy pouch queue message");
    } else {
      *body = source_pub;
      source_pub = NULL;
    }
  }
  lc_pouch_free(&store->allocator, message_id);
  if (lc_pouch_disk_unlock(store, error) != LC_OK && rc == LC_OK) {
    if (*body != NULL) {
      (*body)->close(*body);
      *body = NULL;
    }
    lc_pouch_queue_message_info_cleanup(&store->allocator, out);
    rc = LC_ERR_TRANSPORT;
  }
  if (fd >= 0) {
    close(fd);
  }
  if (source_pub != NULL) {
    source_pub->close(source_pub);
  } else {
    lc_pouch_free(&store->allocator, source_pub);
    lc_pouch_free(&store->allocator, source);
  }
  if (rc != LC_OK) {
    return rc;
  }
  return LC_OK;
}

static int lc_pouch_disk_ack_message(lc_pouch_store *self,
                                     const lc_pouch_queue_ref *ref, int *acked,
                                     lc_error *error) {
  lc_pouch_disk_store *store;
  lc_pouch_disk_queue_entry *entry;
  lc_pouch_disk_queue_entry updated;
  int index;
  int rc;

  if (self == NULL || ref == NULL || acked == NULL) {
    return lc_pouch_set_invalid(error, "ack_message requires message ref");
  }
  rc = lc_pouch_disk_validate_queue_ref(error, "ack_message", ref);
  if (rc != LC_OK) {
    return rc;
  }
  store = (lc_pouch_disk_store *)self->impl;
  *acked = 0;
  rc = lc_pouch_disk_lock(store, error);
  if (rc != LC_OK) {
    return rc;
  }
  index = lc_pouch_disk_find_queue_entry(store, ref->namespace_name, ref->queue,
                                         ref->message_id);
  entry = index >= 0 ? &store->queue_entries[index] : NULL;
  rc = lc_pouch_disk_queue_ref_valid(entry, ref, error);
  if (rc == LC_OK) {
    updated = *entry;
    updated.deleted = 1;
    updated.fencing_token += 1L;
    rc = lc_pouch_disk_append_queue_entry(store, LC_POUCH_RECORD_QUEUE_REMOVE,
                                          &updated, NULL, 0U, 0, error);
    if (rc == LC_OK) {
      *acked = 1;
    }
  }
  if (rc == LC_OK && *acked) {
    rc = lc_pouch_disk_mark_replayed_to_current_size(store, error);
  }
  if (lc_pouch_disk_unlock(store, error) != LC_OK && rc == LC_OK) {
    rc = LC_ERR_TRANSPORT;
  }
  return rc;
}

static int lc_pouch_disk_nack_message(lc_pouch_store *self,
                                      const lc_pouch_queue_ref *ref,
                                      long delay_seconds, int count_failure,
                                      lc_pouch_queue_message_info *out,
                                      lc_error *error) {
  lc_pouch_disk_store *store;
  lc_pouch_disk_queue_entry *entry;
  lc_pouch_disk_queue_entry updated;
  char *meta_etag;
  int index;
  int rc;

  if (self == NULL || ref == NULL || out == NULL) {
    return lc_pouch_set_invalid(error, "nack_message requires message ref");
  }
  rc = lc_pouch_disk_validate_queue_ref(error, "nack_message", ref);
  if (rc != LC_OK) {
    return rc;
  }
  store = (lc_pouch_disk_store *)self->impl;
  memset(out, 0, sizeof(*out));
  rc = lc_pouch_disk_lock(store, error);
  if (rc != LC_OK) {
    return rc;
  }
  index = lc_pouch_disk_find_queue_entry(store, ref->namespace_name, ref->queue,
                                         ref->message_id);
  entry = index >= 0 ? &store->queue_entries[index] : NULL;
  rc = lc_pouch_disk_queue_ref_valid(entry, ref, error);
  if (rc == LC_OK) {
    updated = *entry;
    updated.not_visible_until_unix =
        (long)time(NULL) + (delay_seconds > 0L ? delay_seconds : 0L);
    updated.lease_expires_at_unix = 0L;
    updated.fencing_token += 1L;
    if (count_failure) {
      updated.failure_attempts += 1;
    }
    updated.lease_id = NULL;
    meta_etag = lc_pouch_make_etag(store, updated.fencing_token,
                                   updated.message_id,
                                   strlen(updated.message_id));
    updated.meta_etag = meta_etag;
    if (meta_etag == NULL) {
      rc = lc_pouch_set_nomem(error, "failed to allocate pouch queue etag");
    } else {
      rc = lc_pouch_disk_append_queue_entry(store, LC_POUCH_RECORD_QUEUE_UPDATE,
                                            &updated, NULL, 0U, 0, error);
      lc_pouch_free(&store->allocator, meta_etag);
    }
  }
  if (rc == LC_OK) {
    index = lc_pouch_disk_find_queue_entry(store, ref->namespace_name,
                                           ref->queue, ref->message_id);
    entry = index >= 0 ? &store->queue_entries[index] : NULL;
  }
  if (rc == LC_OK &&
      !lc_pouch_queue_info_from_entry(&store->allocator, out, entry)) {
    rc = lc_pouch_set_nomem(error, "failed to copy pouch queue message");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_disk_mark_replayed_to_current_size(store, error);
  }
  if (lc_pouch_disk_unlock(store, error) != LC_OK && rc == LC_OK) {
    rc = LC_ERR_TRANSPORT;
  }
  return rc;
}

static int lc_pouch_disk_extend_message(lc_pouch_store *self,
                                        const lc_pouch_queue_ref *ref,
                                        long extend_by_seconds,
                                        lc_pouch_queue_message_info *out,
                                        lc_error *error) {
  lc_pouch_disk_store *store;
  lc_pouch_disk_queue_entry *entry;
  lc_pouch_disk_queue_entry updated;
  char *meta_etag;
  int index;
  int rc;

  if (self == NULL || ref == NULL || out == NULL) {
    return lc_pouch_set_invalid(error, "extend_message requires message ref");
  }
  rc = lc_pouch_disk_validate_queue_ref(error, "extend_message", ref);
  if (rc != LC_OK) {
    return rc;
  }
  store = (lc_pouch_disk_store *)self->impl;
  memset(out, 0, sizeof(*out));
  rc = lc_pouch_disk_lock(store, error);
  if (rc != LC_OK) {
    return rc;
  }
  index = lc_pouch_disk_find_queue_entry(store, ref->namespace_name, ref->queue,
                                         ref->message_id);
  entry = index >= 0 ? &store->queue_entries[index] : NULL;
  rc = lc_pouch_disk_queue_ref_valid(entry, ref, error);
  if (rc == LC_OK) {
    updated = *entry;
    updated.visibility_timeout_seconds = extend_by_seconds > 0L
                                             ? extend_by_seconds
                                             : entry->visibility_timeout_seconds;
    updated.not_visible_until_unix =
        (long)time(NULL) + updated.visibility_timeout_seconds;
    updated.lease_expires_at_unix = updated.not_visible_until_unix;
    updated.fencing_token += 1L;
    meta_etag = lc_pouch_make_etag(store, updated.fencing_token,
                                   updated.message_id,
                                   strlen(updated.message_id));
    updated.meta_etag = meta_etag;
    if (meta_etag == NULL) {
      rc = lc_pouch_set_nomem(error, "failed to allocate pouch queue etag");
    } else {
      rc = lc_pouch_disk_append_queue_entry(store, LC_POUCH_RECORD_QUEUE_UPDATE,
                                            &updated, NULL, 0U, 0, error);
      lc_pouch_free(&store->allocator, meta_etag);
    }
  }
  if (rc == LC_OK) {
    index = lc_pouch_disk_find_queue_entry(store, ref->namespace_name,
                                           ref->queue, ref->message_id);
    entry = index >= 0 ? &store->queue_entries[index] : NULL;
  }
  if (rc == LC_OK &&
      !lc_pouch_queue_info_from_entry(&store->allocator, out, entry)) {
    rc = lc_pouch_set_nomem(error, "failed to copy pouch queue message");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_disk_mark_replayed_to_current_size(store, error);
  }
  if (lc_pouch_disk_unlock(store, error) != LC_OK && rc == LC_OK) {
    rc = LC_ERR_TRANSPORT;
  }
  return rc;
}

static int lc_pouch_disk_queue_stats(lc_pouch_store *self,
                                     const char *namespace_name,
                                     const char *queue,
                                     lc_pouch_queue_stats *out,
                                     lc_error *error) {
  lc_pouch_disk_store *store;
  lc_pouch_disk_queue_entry *entry;
  lc_pouch_disk_queue_entry *head_entry;
  long now_unix;
  size_t index;
  int rc;

  if (self == NULL || namespace_name == NULL || queue == NULL || out == NULL) {
    return lc_pouch_set_invalid(error,
                                "queue_stats requires store, namespace, queue, "
                                "and out");
  }
  rc = lc_pouch_disk_validate_namespace_queue(error, "queue_stats",
                                              namespace_name, queue);
  if (rc != LC_OK) {
    return rc;
  }
  store = (lc_pouch_disk_store *)self->impl;
  memset(out, 0, sizeof(*out));
  rc = lc_pouch_disk_lock(store, error);
  if (rc != LC_OK) {
    return rc;
  }
  head_entry = NULL;
  now_unix = (long)time(NULL);
  for (index = 0U; index < store->queue_entry_count; ++index) {
    entry = &store->queue_entries[index];
    if (!entry->deleted && strcmp(entry->namespace_name, namespace_name) == 0 &&
        strcmp(entry->queue, queue) == 0 && entry->expires_at_unix > now_unix &&
        entry->failure_attempts < entry->max_attempts) {
      ++out->pending_candidates;
      if (entry->not_visible_until_unix <= now_unix) {
        out->available += 1;
        if (lc_pouch_disk_queue_entry_before(entry, head_entry)) {
          lc_pouch_free(&store->allocator, out->head_message_id);
          out->head_message_id =
              lc_pouch_strdup(&store->allocator, entry->message_id);
          if (out->head_message_id == NULL) {
            lc_pouch_queue_stats_cleanup(&store->allocator, out);
            lc_pouch_disk_unlock(store, error);
            return lc_pouch_set_nomem(error, "failed to copy pouch queue head");
          }
          head_entry = entry;
          out->head_enqueued_at_unix = entry->enqueued_at_unix;
          out->head_not_visible_until_unix = entry->not_visible_until_unix;
        }
      }
    }
  }
  if (lc_pouch_disk_unlock(store, error) != LC_OK) {
    lc_pouch_queue_stats_cleanup(&store->allocator, out);
    return LC_ERR_TRANSPORT;
  }
  return LC_OK;
}

static int lc_pouch_disk_query_config(lc_pouch_store *self,
                                      const char *namespace_name,
                                      lc_pouch_query_config *out,
                                      lc_error *error) {
  lc_pouch_disk_store *store;
  int rc;

  if (self == NULL || namespace_name == NULL || out == NULL) {
    return lc_pouch_set_invalid(
        error, "query_config requires store, namespace, and out");
  }
  rc = lc_pouch_disk_validate_name(error, "query_config", "namespace",
                                   namespace_name);
  if (rc != LC_OK) {
    return rc;
  }
  store = (lc_pouch_disk_store *)self->impl;
  memset(out, 0, sizeof(*out));
  out->preferred_engine =
      lc_pouch_strdup(&store->allocator, store->query_engine);
  out->fallback_engine =
      lc_pouch_strdup(&store->allocator, store->query_fallback_engine);
  if (out->preferred_engine == NULL || out->fallback_engine == NULL) {
    lc_pouch_query_config_cleanup(&store->allocator, out);
    return lc_pouch_set_nomem(error, "failed to copy pouch query config");
  }
  return LC_OK;
}

static char *lc_pouch_disk_make_backend_hash(lc_pouch_disk_store *store) {
  unsigned long crc;
  char stack[96];

  crc = lc_pouch_crc32((const unsigned char *)store->root_path,
                       strlen(store->root_path));
  snprintf(stack, sizeof(stack), "pouch-%08lx", crc);
  return lc_pouch_strdup(&store->allocator, stack);
}

static int lc_pouch_disk_read_backend_hash_object(lc_pouch_store *self,
                                                  char **out,
                                                  lc_error *error) {
  lc_pouch_disk_store *store;
  lc_pouch_object_selector selector;
  lc_pouch_object_info info;
  lc_source *body;
  unsigned char *payload;
  size_t payload_length;
  int rc;

  store = (lc_pouch_disk_store *)self->impl;
  memset(&selector, 0, sizeof(selector));
  memset(&info, 0, sizeof(info));
  selector.name = LC_POUCH_BACKEND_KEY;
  body = NULL;
  payload = NULL;
  payload_length = 0U;

  rc = lc_pouch_disk_get_object(self, LC_POUCH_BACKEND_NAMESPACE,
                                LC_POUCH_BACKEND_KEY, &selector, &body, &info,
                                error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_read_source_all(&store->allocator, body, &payload,
                                &payload_length, error);
  lc_source_close(body);
  lc_pouch_object_info_cleanup(&store->allocator, &info);
  if (rc != LC_OK) {
    lc_pouch_free(&store->allocator, payload);
    return rc;
  }
  if (payload_length == 0U) {
    lc_pouch_free(&store->allocator, payload);
    return lc_pouch_set_invalid(error, "pouch backend id is empty");
  }
  *out = lc_pouch_dup_bytes(&store->allocator, payload, payload_length);
  lc_pouch_free(&store->allocator, payload);
  if (*out == NULL) {
    return lc_pouch_set_nomem(error, "failed to copy pouch backend id");
  }
  return LC_OK;
}

static int lc_pouch_disk_backend_hash(lc_pouch_store *self, char **out,
                                      lc_error *error) {
  lc_pouch_disk_store *store;
  lc_pouch_put_object_opts opts;
  lc_pouch_object_info info;
  lc_source *source;
  char *candidate;
  int rc;

  if (self == NULL || out == NULL) {
    return lc_pouch_set_invalid(error,
                                "backend_hash requires store and output");
  }
  store = (lc_pouch_disk_store *)self->impl;
  *out = NULL;

  rc = lc_pouch_disk_read_backend_hash_object(self, out, error);
  if (rc == LC_OK) {
    return LC_OK;
  }
  if (rc != LC_ERR_SERVER || error == NULL || error->http_status != 404L) {
    return rc;
  }
  lc_error_cleanup(error);

  candidate = lc_pouch_disk_make_backend_hash(store);
  if (candidate == NULL) {
    return lc_pouch_set_nomem(error, "failed to allocate pouch backend id");
  }
  source = NULL;
  rc = lc_source_from_memory(candidate, strlen(candidate), &source, error);
  if (rc != LC_OK) {
    lc_pouch_free(&store->allocator, candidate);
    return rc;
  }
  memset(&opts, 0, sizeof(opts));
  memset(&info, 0, sizeof(info));
  opts.name = LC_POUCH_BACKEND_KEY;
  opts.content_type = LC_POUCH_BACKEND_CONTENT_TYPE;
  opts.prevent_overwrite = 1;
  rc = lc_pouch_disk_put_object(self, LC_POUCH_BACKEND_NAMESPACE,
                                LC_POUCH_BACKEND_KEY, source, &opts, &info,
                                error);
  lc_source_close(source);
  lc_pouch_object_info_cleanup(&store->allocator, &info);
  if (rc == LC_OK) {
    *out = candidate;
    return LC_OK;
  }
  lc_pouch_free(&store->allocator, candidate);
  if (rc == LC_ERR_SERVER && error != NULL && error->http_status == 409L) {
    lc_error_cleanup(error);
    return lc_pouch_disk_read_backend_hash_object(self, out, error);
  }
  return rc;
}

static int lc_pouch_disk_close(lc_pouch_store *self, lc_error *error) {
  lc_pouch_disk_store *store;
  lc_pouch_allocator allocator;
  size_t index;

  (void)error;
  if (self == NULL) {
    return LC_OK;
  }
  store = (lc_pouch_disk_store *)self->impl;
  if (store == NULL) {
    return LC_OK;
  }
  allocator = store->allocator;
  if (store->log_fd >= 0) {
    close(store->log_fd);
    store->log_fd = -1;
  }
  if (store->lock_fd >= 0) {
    close(store->lock_fd);
    store->lock_fd = -1;
  }
  if (store->query_index_fd >= 0) {
    close(store->query_index_fd);
    store->query_index_fd = -1;
  }
  for (index = 0U; index < store->state_entry_count; ++index) {
    lc_pouch_disk_entry_cleanup(store, &store->state_entries[index]);
  }
  for (index = 0U; index < store->meta_entry_count; ++index) {
    lc_pouch_disk_meta_entry_cleanup(store, &store->meta_entries[index]);
  }
  for (index = 0U; index < store->object_entry_count; ++index) {
    lc_pouch_disk_object_entry_cleanup(store, &store->object_entries[index]);
  }
  for (index = 0U; index < store->queue_entry_count; ++index) {
    lc_pouch_disk_queue_entry_cleanup(store, &store->queue_entries[index]);
  }
  lc_pouch_free(&allocator, store->state_entries);
  lc_pouch_free(&allocator, store->meta_entries);
  lc_pouch_free(&allocator, store->query_meta_indices);
  lc_pouch_free(&allocator, store->object_entries);
  lc_pouch_free(&allocator, store->queue_entries);
  lc_pouch_free(&allocator, store->root_path);
  lc_pouch_free(&allocator, store->log_path);
  lc_pouch_free(&allocator, store->lock_path);
  lc_pouch_free(&allocator, store->query_index_path);
  lc_pouch_free(&allocator, store->query_engine);
  lc_pouch_free(&allocator, store->query_fallback_engine);
  lc_pouch_free(&allocator, store);
  return LC_OK;
}

static int lc_pouch_disk_abort(lc_pouch_store *self, lc_error *error) {
  return lc_pouch_disk_close(self, error);
}

int lc_pouch_disk_open(const char *root_path,
                       const lc_pouch_allocator *allocator,
                       lc_pouch_store **out, lc_error *error) {
  return lc_pouch_disk_open_with_options(root_path, allocator, NULL, out,
                                         error);
}

int lc_pouch_disk_open_with_options(const char *root_path,
                                    const lc_pouch_allocator *allocator,
                                    const lc_pouch_disk_open_opts *opts,
                                    lc_pouch_store **out, lc_error *error) {
  lc_pouch_disk_store *store;
  const char *query_engine;
  const char *query_fallback_engine;
  struct stat st;
  int rc;

  if (root_path == NULL || root_path[0] == '\0' || out == NULL) {
    return lc_pouch_set_invalid(error,
                                "pouch disk open requires root_path and out");
  }
  *out = NULL;
  rc = lc_pouch_disk_validate_open_opts(opts, error);
  if (rc != LC_OK) {
    return rc;
  }
  query_engine = lc_pouch_disk_query_engine_default(
      opts != NULL ? opts->query_engine : NULL);
  query_fallback_engine = lc_pouch_disk_query_fallback_default(
      opts != NULL ? opts->query_fallback_engine : NULL);
  if (stat(root_path, &st) != 0) {
    if (errno != ENOENT || mkdir(root_path, 0777) != 0) {
      return lc_pouch_set_errno(error, "failed to create pouch root directory");
    }
  } else if (!S_ISDIR(st.st_mode)) {
    return lc_pouch_set_invalid(error, "pouch root path is not a directory");
  }
  store = (lc_pouch_disk_store *)lc_pouch_calloc(allocator, 1U, sizeof(*store));
  if (store == NULL) {
    return lc_pouch_set_nomem(error, "failed to allocate pouch disk store");
  }
  if (allocator != NULL) {
    store->allocator = *allocator;
  }
  store->log_fd = -1;
  store->lock_fd = -1;
  store->query_index_fd = -1;
  store->next_version = 1L;
  store->pub.impl = store;
  store->root_path = lc_pouch_strdup(&store->allocator, root_path);
  store->log_path =
      lc_pouch_join_path(&store->allocator, root_path, "store.log");
  store->lock_path =
      lc_pouch_join_path(&store->allocator, root_path, "writer.lock");
  store->query_index_path =
      lc_pouch_join_path(&store->allocator, root_path, "query.index");
  store->query_engine = lc_pouch_strdup(&store->allocator, query_engine);
  store->query_fallback_engine =
      lc_pouch_strdup(&store->allocator, query_fallback_engine);
  if (store->root_path == NULL || store->log_path == NULL ||
      store->lock_path == NULL || store->query_index_path == NULL ||
      store->query_engine == NULL || store->query_fallback_engine == NULL) {
    lc_pouch_disk_close(&store->pub, error);
    return lc_pouch_set_nomem(error, "failed to allocate pouch disk options");
  }
  store->lock_fd = open(store->lock_path, O_RDWR | O_CREAT, 0666);
  if (store->lock_fd < 0) {
    lc_pouch_disk_close(&store->pub, error);
    return lc_pouch_set_errno(error, "failed to open pouch writer lock");
  }
  store->log_fd = open(store->log_path, O_RDWR | O_CREAT, 0666);
  if (store->log_fd < 0) {
    lc_pouch_disk_close(&store->pub, error);
    return lc_pouch_set_errno(error, "failed to open pouch log");
  }
  store->query_index_fd = open(store->query_index_path, O_RDWR | O_CREAT, 0666);
  if (store->query_index_fd < 0) {
    lc_pouch_disk_close(&store->pub, error);
    return lc_pouch_set_errno(error, "failed to open pouch query index");
  }
  rc = lc_pouch_disk_lock(store, error);
  if (rc != LC_OK) {
    lc_pouch_disk_close(&store->pub, error);
    return rc;
  }
  if (lc_pouch_disk_unlock(store, error) != LC_OK) {
    rc = LC_ERR_TRANSPORT;
  }
  if (rc != LC_OK) {
    lc_pouch_disk_close(&store->pub, error);
    return rc;
  }
  store->pub.impl = store;
  store->pub.load_meta = lc_pouch_disk_load_meta;
  store->pub.store_meta = lc_pouch_disk_store_meta;
  store->pub.delete_meta = lc_pouch_disk_delete_meta;
  store->pub.scan_meta = lc_pouch_disk_scan_meta;
  store->pub.query_index_scan = lc_pouch_disk_query_index_scan;
  store->pub.query_index_keys_scan = lc_pouch_disk_query_index_keys_scan;
  store->pub.flush_index = lc_pouch_disk_flush_index;
  store->pub.read_state = lc_pouch_disk_read_state;
  store->pub.write_state = lc_pouch_disk_write_state;
  store->pub.remove_state = lc_pouch_disk_remove_state;
  store->pub.stage_state = lc_pouch_disk_stage_state;
  store->pub.load_staged_state = lc_pouch_disk_load_staged_state;
  store->pub.promote_staged_state = lc_pouch_disk_promote_staged_state;
  store->pub.discard_staged_state = lc_pouch_disk_discard_staged_state;
  store->pub.list_staged_state = lc_pouch_disk_list_staged_state;
  store->pub.put_object = lc_pouch_disk_put_object;
  store->pub.list_objects = lc_pouch_disk_list_objects;
  store->pub.get_object = lc_pouch_disk_get_object;
  store->pub.copy_object = lc_pouch_disk_copy_object;
  store->pub.delete_object = lc_pouch_disk_delete_object;
  store->pub.delete_all_objects = lc_pouch_disk_delete_all_objects;
  store->pub.enqueue_message = lc_pouch_disk_enqueue_message;
  store->pub.dequeue_message = lc_pouch_disk_dequeue_message;
  store->pub.ack_message = lc_pouch_disk_ack_message;
  store->pub.nack_message = lc_pouch_disk_nack_message;
  store->pub.extend_message = lc_pouch_disk_extend_message;
  store->pub.queue_stats = lc_pouch_disk_queue_stats;
  store->pub.query_config = lc_pouch_disk_query_config;
  store->pub.backend_hash = lc_pouch_disk_backend_hash;
  store->pub.close = lc_pouch_disk_close;
  store->pub.abort = lc_pouch_disk_abort;
  *out = &store->pub;
  return LC_OK;
}

static unsigned long lc_pouch_crc32_update(unsigned long crc,
                                           const unsigned char *bytes,
                                           size_t count) {
  size_t index;
  int bit;

  for (index = 0U; index < count; ++index) {
    crc ^= (unsigned long)bytes[index];
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

static unsigned long lc_pouch_crc32(const unsigned char *bytes, size_t count) {
  return lc_pouch_crc32_update(0xffffffffUL, bytes, count) ^ 0xffffffffUL;
}

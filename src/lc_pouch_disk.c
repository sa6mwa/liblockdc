#include "lc_pouch_store.h"

#include "lc_api_internal.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define LC_POUCH_LOG_MAGIC "LCP1"
#define LC_POUCH_HEADER_SIZE 64U
#define LC_POUCH_RECORD_STATE_PUT 1U
#define LC_POUCH_RECORD_STATE_REMOVE 2U
#define LC_POUCH_RECORD_META_PUT 3U
#define LC_POUCH_RECORD_META_REMOVE 4U
#define LC_POUCH_RECORD_OBJECT_PUT 5U
#define LC_POUCH_RECORD_OBJECT_REMOVE 6U
#define LC_POUCH_META_FLAG_QUERY_HIDDEN_SET 1UL
#define LC_POUCH_META_FLAG_QUERY_HIDDEN 2UL
#define LC_POUCH_OBJECT_META_SIZE 28U

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

typedef struct lc_pouch_disk_store {
  lc_pouch_store pub;
  lc_pouch_allocator allocator;
  char *root_path;
  char *log_path;
  char *lock_path;
  int log_fd;
  int lock_fd;
  lc_pouch_disk_state_entry *state_entries;
  size_t state_entry_count;
  size_t state_entry_capacity;
  lc_pouch_disk_meta_entry *meta_entries;
  size_t meta_entry_count;
  size_t meta_entry_capacity;
  lc_pouch_disk_object_entry *object_entries;
  size_t object_entry_count;
  size_t object_entry_capacity;
  long next_version;
} lc_pouch_disk_store;

typedef struct lc_pouch_file_source {
  lc_pouch_allocator allocator;
  int fd;
  unsigned long remaining;
} lc_pouch_file_source;

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
static int lc_pouch_disk_append_record(
    lc_pouch_disk_store *store, unsigned long type, const char *namespace_name,
    const char *key, const char *content_type, const char *etag, long version,
    const unsigned char *body, size_t body_length,
    unsigned long *body_offset_out, lc_error *error);
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
                                      lc_error *error);
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
static int lc_pouch_disk_delete_object(lc_pouch_store *self,
                                       const char *namespace_name,
                                       const char *key,
                                       const lc_pouch_object_selector *selector,
                                       int *deleted, lc_error *error);
static int lc_pouch_disk_delete_all_objects(lc_pouch_store *self,
                                            const char *namespace_name,
                                            const char *key, int *deleted_count,
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

  memset(&lock, 0, sizeof(lock));
  lock.l_type = F_WRLCK;
  lock.l_whence = SEEK_SET;
  if (fcntl(store->lock_fd, F_SETLKW, &lock) != 0) {
    return lc_pouch_set_errno(error, "failed to lock pouch writer lock");
  }
  return LC_OK;
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
  int existing;

  existing = lc_pouch_disk_find_meta_entry(store, namespace_name, key);
  if (existing >= 0) {
    entry = &store->meta_entries[existing];
    lc_pouch_free(&store->allocator, entry->etag);
    entry->etag = NULL;
    lc_pouch_meta_cleanup(&store->allocator, &entry->meta);
  } else {
    if (store->meta_entry_count == store->meta_entry_capacity) {
      size_t new_capacity;

      new_capacity = store->meta_entry_capacity == 0U
                         ? 16U
                         : store->meta_entry_capacity * 2U;
      grown = (lc_pouch_disk_meta_entry *)lc_pouch_realloc(
          &store->allocator, store->meta_entries,
          new_capacity * sizeof(store->meta_entries[0]));
      if (grown == NULL) {
        return 0;
      }
      memset(grown + store->meta_entry_capacity, 0,
             (new_capacity - store->meta_entry_capacity) * sizeof(grown[0]));
      store->meta_entries = grown;
      store->meta_entry_capacity = new_capacity;
    }
    entry = &store->meta_entries[store->meta_entry_count++];
    entry->namespace_name = lc_pouch_strdup(&store->allocator, namespace_name);
    entry->key = lc_pouch_strdup(&store->allocator, key);
    if (entry->namespace_name == NULL || entry->key == NULL) {
      return 0;
    }
  }
  entry->etag = lc_pouch_strdup(&store->allocator, etag);
  if (etag != NULL && entry->etag == NULL) {
    return 0;
  }
  if (!lc_pouch_meta_copy(&store->allocator, &entry->meta, meta)) {
    return 0;
  }
  entry->deleted = deleted;
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

static char *lc_pouch_make_object_id(lc_pouch_disk_store *store,
                                     const char *name, const void *body,
                                     size_t body_length) {
  unsigned long crc;
  char stack[128];

  crc = lc_pouch_crc32((const unsigned char *)body, body_length);
  snprintf(stack, sizeof(stack), "pouch-obj-%08lx-%s", crc,
           name != NULL ? name : "attachment");
  return lc_pouch_strdup(&store->allocator, stack);
}

static char *lc_pouch_make_etag(lc_pouch_disk_store *store, long version,
                                const void *body, size_t body_length) {
  unsigned long crc;
  char stack[64];

  crc = lc_pouch_crc32((const unsigned char *)body, body_length);
  snprintf(stack, sizeof(stack), "pouch-%ld-%08lx", version, crc);
  return lc_pouch_strdup(&store->allocator, stack);
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
  store = (lc_pouch_disk_store *)self->impl;
  memset(out, 0, sizeof(*out));
  index = lc_pouch_disk_find_meta_entry(store, namespace_name, key);
  if (index < 0 || store->meta_entries[index].deleted) {
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
    return lc_pouch_set_nomem(error, "failed to copy pouch metadata");
  }
  out->found = 1;
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
  int index;
  int rc;

  if (self == NULL || namespace_name == NULL || key == NULL || meta == NULL ||
      out == NULL) {
    return lc_pouch_set_invalid(error,
                                "store_meta requires store, namespace, key, "
                                "metadata, and output metadata");
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
  rc = lc_pouch_disk_append_record(
      store, LC_POUCH_RECORD_META_PUT, namespace_name, key, NULL, etag,
      meta->version, payload, payload_length, &body_offset, error);
  if (rc == LC_OK && !lc_pouch_disk_upsert_meta_entry(store, namespace_name,
                                                      key, etag, meta, 0)) {
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
  lc_pouch_free(&store->allocator, etag);
  lc_pouch_free(&store->allocator, payload);
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
  rc = lc_pouch_disk_append_record(store, LC_POUCH_RECORD_META_REMOVE,
                                   namespace_name, key, NULL, etag, version,
                                   NULL, 0U, &body_offset, error);
  if (rc == LC_OK && !lc_pouch_disk_upsert_meta_entry(store, namespace_name,
                                                      key, etag, NULL, 1)) {
    rc = lc_pouch_set_nomem(error, "failed to update pouch metadata index");
  }
  lc_pouch_free(&store->allocator, etag);
  if (lc_pouch_disk_unlock(store, error) != LC_OK && rc == LC_OK) {
    rc = LC_ERR_TRANSPORT;
  }
  return rc;
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

  ns_len = (unsigned long)strlen(namespace_name);
  key_len = (unsigned long)strlen(key);
  ct_len = content_type != NULL ? (unsigned long)strlen(content_type) : 0UL;
  etag_len = etag != NULL ? (unsigned long)strlen(etag) : 0UL;
  payload_len =
      ns_len + key_len + ct_len + etag_len + (unsigned long)body_length;
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
  if (fsync(store->log_fd) != 0) {
    return lc_pouch_set_errno(error, "failed to fsync pouch log");
  }
  *body_offset_out = (unsigned long)start + LC_POUCH_HEADER_SIZE + ns_len +
                     key_len + ct_len + etag_len;
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
  unsigned long offset;
  int short_read;

  offset = 0UL;
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
    if (header_size != LC_POUCH_HEADER_SIZE ||
        payload_len != ns_len + key_len + ct_len + etag_len + body_len ||
        ns_len == 0UL || key_len == 0UL ||
        (type != LC_POUCH_RECORD_STATE_PUT &&
         type != LC_POUCH_RECORD_STATE_REMOVE &&
         type != LC_POUCH_RECORD_META_PUT &&
         type != LC_POUCH_RECORD_META_REMOVE &&
         type != LC_POUCH_RECORD_OBJECT_PUT &&
         type != LC_POUCH_RECORD_OBJECT_REMOVE) ||
        payload_len > (unsigned long)(((size_t)-1) - 1U)) {
      break;
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
          type == LC_POUCH_RECORD_STATE_REMOVE) {
        if (!lc_pouch_disk_upsert_entry(
                store, ns_copy, key_copy, ct_copy, etag_copy, (long)version,
                offset + LC_POUCH_HEADER_SIZE + ns_len + key_len + ct_len +
                    etag_len,
                body_len, type == LC_POUCH_RECORD_STATE_REMOVE)) {
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
    lc_pouch_free(&store->allocator, payload);
  }
  if (lseek(store->log_fd, (off_t)offset, SEEK_SET) < 0 ||
      ftruncate(store->log_fd, (off_t)offset) != 0) {
    return lc_pouch_set_errno(error, "failed to truncate pouch log tail");
  }
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
  store = (lc_pouch_disk_store *)self->impl;
  memset(out, 0, sizeof(*out));
  *body = NULL;
  index = lc_pouch_disk_find_entry(store, namespace_name, key);
  if (index < 0 || store->state_entries[index].deleted) {
    out->no_content = 1;
    return LC_OK;
  }
  entry = &store->state_entries[index];
  fd = open(store->log_path, O_RDONLY);
  if (fd < 0) {
    return lc_pouch_set_errno(error, "failed to open pouch log for state read");
  }
  if (lseek(fd, (off_t)entry->body_offset, SEEK_SET) < 0) {
    close(fd);
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
    return lc_pouch_set_nomem(error, "failed to copy pouch state metadata");
  }
  out->version = entry->version;
  out->bytes = (long)entry->body_length;
  *body = source_pub;
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
  unsigned char *payload;
  size_t payload_length;
  char *etag;
  const char *content_type;
  unsigned long body_offset;
  long version;
  int index;
  int rc;

  if (self == NULL || namespace_name == NULL || key == NULL || body == NULL ||
      out == NULL) {
    return lc_pouch_set_invalid(error,
                                "write_state requires store, namespace, key, "
                                "body, and output metadata");
  }
  store = (lc_pouch_disk_store *)self->impl;
  memset(out, 0, sizeof(*out));
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
  payload = NULL;
  payload_length = 0U;
  rc = lc_pouch_read_source_all(&store->allocator, body, &payload,
                                &payload_length, error);
  if (rc != LC_OK) {
    lc_pouch_disk_unlock(store, error);
    return rc;
  }
  version = store->next_version++;
  etag = lc_pouch_make_etag(store, version, payload, payload_length);
  if (etag == NULL) {
    lc_pouch_free(&store->allocator, payload);
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_nomem(error, "failed to allocate pouch state etag");
  }
  content_type = opts != NULL && opts->content_type != NULL
                     ? opts->content_type
                     : "application/json";
  rc = lc_pouch_disk_append_record(
      store, LC_POUCH_RECORD_STATE_PUT, namespace_name, key, content_type, etag,
      version, payload, payload_length, &body_offset, error);
  if (rc == LC_OK &&
      !lc_pouch_disk_upsert_entry(store, namespace_name, key, content_type,
                                  etag, version, body_offset,
                                  (unsigned long)payload_length, 0)) {
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
  lc_pouch_free(&store->allocator, etag);
  lc_pouch_free(&store->allocator, payload);
  if (lc_pouch_disk_unlock(store, error) != LC_OK && rc == LC_OK) {
    rc = LC_ERR_TRANSPORT;
  }
  return rc;
}

static int lc_pouch_disk_remove_state(lc_pouch_store *self,
                                      const char *namespace_name,
                                      const char *key,
                                      const char *expected_etag,
                                      lc_error *error) {
  lc_pouch_disk_store *store;
  lc_pouch_disk_state_entry *entry;
  char *etag;
  unsigned long body_offset;
  long version;
  int index;
  int rc;

  if (self == NULL || namespace_name == NULL || key == NULL) {
    return lc_pouch_set_invalid(error,
                                "remove_state requires store, namespace, key");
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
  lc_pouch_free(&store->allocator, etag);
  if (lc_pouch_disk_unlock(store, error) != LC_OK && rc == LC_OK) {
    rc = LC_ERR_TRANSPORT;
  }
  return rc;
}

static int lc_pouch_disk_put_object(lc_pouch_store *self,
                                    const char *namespace_name, const char *key,
                                    lc_source *body,
                                    const lc_pouch_put_object_opts *opts,
                                    lc_pouch_object_info *out,
                                    lc_error *error) {
  lc_pouch_disk_store *store;
  unsigned char *payload;
  unsigned char *record_body;
  char *id;
  const char *name;
  const char *content_type;
  size_t payload_length;
  size_t content_type_length;
  size_t record_length;
  unsigned long body_offset;
  long now_unix;
  int existing;
  int rc;

  if (self == NULL || namespace_name == NULL || key == NULL || body == NULL ||
      opts == NULL || opts->name == NULL || out == NULL) {
    return lc_pouch_set_invalid(error,
                                "put_object requires store, namespace, key, "
                                "body, name, and output metadata");
  }
  store = (lc_pouch_disk_store *)self->impl;
  memset(out, 0, sizeof(*out));
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
  payload = NULL;
  payload_length = 0U;
  rc = lc_pouch_read_source_all(&store->allocator, body, &payload,
                                &payload_length, error);
  if (rc != LC_OK) {
    lc_pouch_disk_unlock(store, error);
    return rc;
  }
  if (opts->has_max_bytes && payload_length > (size_t)opts->max_bytes) {
    lc_pouch_free(&store->allocator, payload);
    lc_pouch_disk_unlock(store, error);
    return lc_error_set(error, LC_ERR_SERVER, 413L,
                        "pouch attachment exceeds max_bytes", NULL,
                        "attachment_too_large", NULL);
  }
  id = lc_pouch_make_object_id(store, name, payload, payload_length);
  if (id == NULL) {
    lc_pouch_free(&store->allocator, payload);
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_nomem(error, "failed to allocate pouch object id");
  }
  now_unix = (long)time(NULL);
  content_type_length = strlen(content_type);
  record_length =
      LC_POUCH_OBJECT_META_SIZE + content_type_length + payload_length;
  record_body =
      (unsigned char *)lc_pouch_alloc(&store->allocator, record_length);
  if (record_body == NULL) {
    lc_pouch_free(&store->allocator, id);
    lc_pouch_free(&store->allocator, payload);
    lc_pouch_disk_unlock(store, error);
    return lc_pouch_set_nomem(error, "failed to allocate pouch object record");
  }
  lc_pouch_put_u64(record_body, (unsigned long)now_unix);
  lc_pouch_put_u64(record_body + 8, (unsigned long)now_unix);
  lc_pouch_put_u32(record_body + 16, (unsigned long)content_type_length);
  lc_pouch_put_u64(record_body + 20, (unsigned long)payload_length);
  memcpy(record_body + LC_POUCH_OBJECT_META_SIZE, content_type,
         content_type_length);
  if (payload_length > 0U) {
    memcpy(record_body + LC_POUCH_OBJECT_META_SIZE + content_type_length,
           payload, payload_length);
  }
  rc = lc_pouch_disk_append_record(
      store, LC_POUCH_RECORD_OBJECT_PUT, namespace_name, key, name, id, 0L,
      record_body, record_length, &body_offset, error);
  if (rc == LC_OK &&
      !lc_pouch_disk_upsert_object_entry(
          store, namespace_name, key, id, name, content_type,
          (long)payload_length, now_unix, now_unix,
          body_offset + LC_POUCH_OBJECT_META_SIZE + content_type_length,
          (unsigned long)payload_length, 0)) {
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
  lc_pouch_free(&store->allocator, record_body);
  lc_pouch_free(&store->allocator, id);
  lc_pouch_free(&store->allocator, payload);
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
  size_t index;
  size_t count;

  if (self == NULL || namespace_name == NULL || key == NULL || out == NULL) {
    return lc_pouch_set_invalid(error,
                                "list_objects requires store, namespace, key, "
                                "and output list");
  }
  store = (lc_pouch_disk_store *)self->impl;
  memset(out, 0, sizeof(*out));
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
    return LC_OK;
  }
  out->items = (lc_pouch_object_info *)lc_pouch_calloc(&store->allocator, count,
                                                       sizeof(out->items[0]));
  if (out->items == NULL) {
    return lc_pouch_set_nomem(error, "failed to allocate pouch object list");
  }
  out->count = count;
  count = 0U;
  for (index = 0U; index < store->object_entry_count; ++index) {
    if (!store->object_entries[index].deleted &&
        strcmp(store->object_entries[index].namespace_name, namespace_name) ==
            0 &&
        strcmp(store->object_entries[index].key, key) == 0) {
      if (!lc_pouch_object_info_from_entry(&store->allocator,
                                           &out->items[count],
                                           &store->object_entries[index])) {
        lc_pouch_object_list_cleanup(&store->allocator, out);
        return lc_pouch_set_nomem(error, "failed to copy pouch object list");
      }
      ++count;
    }
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
  store = (lc_pouch_disk_store *)self->impl;
  memset(out, 0, sizeof(*out));
  *body = NULL;
  index = lc_pouch_disk_find_object(store, namespace_name, key, selector);
  if (index < 0) {
    return lc_error_set(error, LC_ERR_SERVER, 404L,
                        "pouch attachment was not found", NULL, "not_found",
                        NULL);
  }
  entry = &store->object_entries[index];
  fd = open(store->log_path, O_RDONLY);
  if (fd < 0) {
    return lc_pouch_set_errno(error,
                              "failed to open pouch log for object read");
  }
  if (lseek(fd, (off_t)entry->body_offset, SEEK_SET) < 0) {
    close(fd);
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
    return lc_pouch_set_nomem(error, "failed to copy pouch object metadata");
  }
  *body = source_pub;
  return LC_OK;
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
  if (lc_pouch_disk_unlock(store, error) != LC_OK && rc == LC_OK) {
    rc = LC_ERR_TRANSPORT;
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
  for (index = 0U; index < store->state_entry_count; ++index) {
    lc_pouch_disk_entry_cleanup(store, &store->state_entries[index]);
  }
  for (index = 0U; index < store->meta_entry_count; ++index) {
    lc_pouch_disk_meta_entry_cleanup(store, &store->meta_entries[index]);
  }
  for (index = 0U; index < store->object_entry_count; ++index) {
    lc_pouch_disk_object_entry_cleanup(store, &store->object_entries[index]);
  }
  lc_pouch_free(&allocator, store->state_entries);
  lc_pouch_free(&allocator, store->meta_entries);
  lc_pouch_free(&allocator, store->object_entries);
  lc_pouch_free(&allocator, store->root_path);
  lc_pouch_free(&allocator, store->log_path);
  lc_pouch_free(&allocator, store->lock_path);
  lc_pouch_free(&allocator, store);
  return LC_OK;
}

static int lc_pouch_disk_abort(lc_pouch_store *self, lc_error *error) {
  return lc_pouch_disk_close(self, error);
}

int lc_pouch_disk_open(const char *root_path,
                       const lc_pouch_allocator *allocator,
                       lc_pouch_store **out, lc_error *error) {
  lc_pouch_disk_store *store;
  struct stat st;
  int rc;

  if (root_path == NULL || root_path[0] == '\0' || out == NULL) {
    return lc_pouch_set_invalid(error,
                                "pouch disk open requires root_path and out");
  }
  *out = NULL;
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
  store->next_version = 1L;
  store->pub.impl = store;
  store->root_path = lc_pouch_strdup(&store->allocator, root_path);
  store->log_path =
      lc_pouch_join_path(&store->allocator, root_path, "store.log");
  store->lock_path =
      lc_pouch_join_path(&store->allocator, root_path, "writer.lock");
  if (store->root_path == NULL || store->log_path == NULL ||
      store->lock_path == NULL) {
    lc_pouch_disk_close(&store->pub, error);
    return lc_pouch_set_nomem(error, "failed to allocate pouch disk paths");
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
  rc = lc_pouch_disk_lock(store, error);
  if (rc != LC_OK) {
    lc_pouch_disk_close(&store->pub, error);
    return rc;
  }
  rc = lc_pouch_disk_replay(store, error);
  if (lc_pouch_disk_unlock(store, error) != LC_OK && rc == LC_OK) {
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
  store->pub.read_state = lc_pouch_disk_read_state;
  store->pub.write_state = lc_pouch_disk_write_state;
  store->pub.remove_state = lc_pouch_disk_remove_state;
  store->pub.put_object = lc_pouch_disk_put_object;
  store->pub.list_objects = lc_pouch_disk_list_objects;
  store->pub.get_object = lc_pouch_disk_get_object;
  store->pub.delete_object = lc_pouch_disk_delete_object;
  store->pub.delete_all_objects = lc_pouch_disk_delete_all_objects;
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

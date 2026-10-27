#include "lc_collection_query.h"
#include "lc_pouch_crypto.h"
#include "lc_pouch_index_artifact.h"
#include "lc_pouch_path.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#define LC_CQS_DESCRIPTOR_BYTES 4096U
#define LC_CQS_STORED_BYTES (LC_COLLECTION_INDEX_MAX_BYTES + 65536U)

#ifdef LOCKDC_TEST_BUILD
lc_collection_query_hook_fn lc_collection_query_test_hook;
void *lc_collection_query_test_context;
#endif

static int lc_cqs_paths(lc_collection_handle *collection, const char *query_id,
                        lc_u64 ordinal, char **directory, char **path,
                        char context[128], lc_error *error) {
  char number[32];
  char leaf[40];
  char *base;
  int n;
  *directory = *path = NULL;
  if (!lc_xid_is_valid(query_id))
    return lc_collection_failure(
        error, LC_ERR_INVALID, NULL,
        "collection query projection identity is invalid");
  if (lc_u64_format_base10(ordinal, number, sizeof(number)) < 0)
    return lc_collection_failure(error, LC_ERR_INVALID, NULL,
                                 "collection query page ordinal is invalid");
  n = snprintf(leaf, sizeof(leaf), "page-%s", number);
  if (n < 0 || (size_t)n >= sizeof(leaf))
    return lc_collection_failure(error, LC_ERR_INVALID, NULL,
                                 "collection query page name is too long");
  n = snprintf(context, 128U, "collection-index:%s:%s:%s", collection->id,
               query_id, number);
  if (n < 0 || n >= 128)
    return lc_collection_failure(error, LC_ERR_INVALID, NULL,
                                 "collection query page binding is too long");
  base = lc_pouch_path_join(&collection->allocator, collection->directory,
                            "query");
  if (base != NULL)
    *directory = lc_pouch_path_join(&collection->allocator, base, query_id);
  lc_free_with_allocator(&collection->allocator, base);
  if (*directory != NULL)
    *path = lc_pouch_path_join(&collection->allocator, *directory, leaf);
  if (*path == NULL) {
    lc_free_with_allocator(&collection->allocator, *directory);
    *directory = NULL;
    return lc_collection_failure(
        error, LC_ERR_NOMEM, NULL,
        "failed to allocate collection query page path");
  }
  return LC_OK;
}

int lc_collection_query_store(lc_collection_handle *collection,
                              lc_collection_index *index, lc_error *error) {
  char *directory, *path, *temporary, *bytes, *descriptor, *base;
  char context[128];
  char xid[LC_XID_STRING_SIZE];
  char leaf[48];
  lc_source *source;
  lc_collection_order_row rows[LC_COLLECTION_ORDER_PAGE_ROWS];
  lc_u64 plain, stored;
  size_t length, count;
  int fd, rc, created, held, skip;
  directory = path = temporary = bytes = descriptor = base = NULL;
  source = NULL;
  fd = -1;
  created = 0;
  held = skip = 0;
  rc = lc_cqs_paths(collection, index->query_id, index->exact.index_seq,
                    &directory, &path, context, error);
  if (rc == LC_OK)
    rc = lc_collection_index_encode(index, &bytes, &length, error);
  if (rc == LC_OK) {
    base = lc_pouch_path_join(&collection->allocator, collection->directory,
                              "query");
    if (base == NULL)
      rc = lc_collection_failure(
          error, LC_ERR_NOMEM, NULL,
          "failed to allocate collection query directory");
  }
  if (rc == LC_OK)
    rc = lc_xid_new(xid, error);
  if (rc == LC_OK) {
    snprintf(leaf, sizeof(leaf), ".page-%s.tmp", xid);
    temporary = lc_pouch_path_join(&collection->allocator, directory, leaf);
    if (temporary == NULL)
      rc = lc_collection_failure(
          error, LC_ERR_NOMEM, NULL,
          "failed to allocate collection query temporary path");
  }
  if (rc == LC_OK) {
    rc = lc_collection_metadata_enter(collection, 0, error);
    held = rc == LC_OK;
  }
  if (rc == LC_OK)
    rc = lc_collection_refresh_view(collection, error);
  if (rc == LC_OK)
    skip = strcmp(collection->order.query_id, index->query_id) != 0;
  if (rc == LC_OK && !skip)
    rc = lc_pouch_path_ensure_directory(
        base, "failed to create collection query directory", error);
  if (rc == LC_OK && !skip)
    rc = lc_pouch_path_ensure_directory(
        directory, "failed to create collection query directory", error);
  if (rc == LC_OK && !skip) {
    fd = open(temporary, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (fd < 0)
      rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to create collection query artifact");
    else
      created = 1;
  }
  if (rc == LC_OK && !skip && flock(fd, LOCK_EX) != 0)
    rc =
        lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                              "failed to protect collection query publication");
  if (held) {
    lc_collection_metadata_leave(collection);
    held = 0;
  }
#ifdef LOCKDC_TEST_BUILD
  if (rc == LC_OK && !skip && lc_collection_query_test_hook != NULL)
    rc = lc_collection_query_test_hook(lc_collection_query_test_context,
                                       collection, temporary, error);
#endif
  if (rc == LC_OK && !skip)
    rc = lc_source_from_memory(bytes, length, &source, error);
  if (rc == LC_OK && !skip)
    rc = lc_pouch_crypto_stream_to_fd_crc_with_compression(
        collection->pouch->crypto, context, fd, source, &plain, &stored, NULL,
        &descriptor, 0, error);
  if (rc == LC_OK && !skip && (plain != length || stored > LC_CQS_STORED_BYTES))
    rc = lc_collection_failure(
        error, LC_ERR_PROTOCOL, NULL,
        "collection query artifact exceeds its bounded metadata span");
  if (rc == LC_OK && !skip && descriptor != NULL && descriptor[0] != '\0')
    rc = lc_pouch_query_index_append_crypto_footer_fd(fd, descriptor, error);
  if (rc == LC_OK && !skip)
    rc = lc_pouch_fsync_commit(collection->pouch, fd, error);
  if (rc == LC_OK && !skip) {
    rc = lc_collection_metadata_enter(collection, 0, error);
    held = rc == LC_OK;
  }
  if (rc == LC_OK && !skip)
    rc = lc_collection_refresh_view(collection, error);
  if (rc == LC_OK && !skip) {
    skip = strcmp(collection->order.query_id, index->query_id) != 0;
    if (!skip) {
      rc = lc_collection_order_rows(&collection->order, index->exact.index_seq,
                                    rows, LC_COLLECTION_ORDER_PAGE_ROWS, &count,
                                    error);
      if (rc == LC_OK)
        skip =
            count != index->rows ||
            lc_collection_query_row_hash(rows, count) != index->exact.row_hash;
    }
  }
  if (rc == LC_OK && !skip) {
    if (rename(temporary, path) != 0)
      rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to publish collection query artifact");
    else
      created = 0;
  }
  if (rc == LC_OK && !skip && collection->pouch->durable_sync)
    rc = lc_pouch_path_fsync_directory(
        directory, "failed to sync collection query artifact name", error);
  if (held)
    lc_collection_metadata_leave(collection);
  if (fd >= 0) {
    if (close(fd) != 0 && rc == LC_OK)
      rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to close collection query artifact");
    fd = -1;
  }
  if (created)
    (void)unlink(temporary);
  lc_source_close(source);
  lc_free_with_allocator(&index->allocator, bytes);
  lc_free_with_allocator(&collection->allocator, directory);
  lc_free_with_allocator(&collection->allocator, path);
  lc_free_with_allocator(&collection->allocator, temporary);
  lc_free_with_allocator(&collection->allocator, descriptor);
  lc_free_with_allocator(&collection->allocator, base);
  return rc;
}

/* This is deliberately a bounded METADATA buffer. State sources never pass
 * through it. The extra byte makes an over-limit decoded artifact detectable
 * without accepting a truncated prefix or trusting compressed byte counts. */
static int lc_cqs_read(lc_collection_handle *collection, lc_source *source,
                       size_t stored_length, char **out, size_t *length,
                       lc_error *error) {
  char *bytes, *next;
  size_t capacity, used, got, grown;
  int rc;
  *out = NULL;
  *length = 0U;
  capacity = stored_length < 16384U ? stored_length + 1U : 16384U;
  used = 0U;
  bytes = (char *)lc_alloc_with_allocator(&collection->allocator, capacity);
  if (bytes == NULL)
    return lc_collection_failure(
        error, LC_ERR_NOMEM, NULL,
        "failed to allocate bounded collection query metadata");
  rc = LC_OK;
  for (;;) {
    if (used == capacity) {
      grown = capacity * 2U;
      if (grown > LC_COLLECTION_INDEX_MAX_BYTES + 1U)
        grown = LC_COLLECTION_INDEX_MAX_BYTES + 1U;
      next = (char *)lc_realloc_with_allocator(&collection->allocator, bytes,
                                               grown);
      if (next == NULL) {
        rc = lc_collection_failure(
            error, LC_ERR_NOMEM, NULL,
            "failed to grow bounded collection query metadata");
        break;
      }
      bytes = next;
      capacity = grown;
    }
    got = source->read(source, bytes + used, capacity - used, error);
    if (error->code != LC_OK) {
      rc = error->code;
      break;
    }
    if (got > capacity - used || got > LC_COLLECTION_INDEX_MAX_BYTES - used) {
      rc = lc_collection_failure(
          error, LC_ERR_PROTOCOL, NULL,
          "collection query metadata exceeds its decoded size limit");
      break;
    }
    used += got;
    if (got == 0U)
      break;
  }
  if (rc == LC_OK) {
    *out = bytes;
    *length = used;
  } else
    lc_free_with_allocator(&collection->allocator, bytes);
  return rc;
}

int lc_collection_query_load(lc_collection_handle *collection,
                             const char *query_id, lc_u64 ordinal,
                             unsigned long row_hash, lc_collection_index *index,
                             int *found, lc_error *error) {
  char *directory, *path, *descriptor, *bytes;
  char context[128];
  lc_source *source;
  lc_error failure;
  lc_u64 cipher_length;
  struct stat st;
  size_t length;
  int rc;
  memset(index, 0, sizeof(*index));
  *found = 0;
  directory = path = descriptor = bytes = NULL;
  source = NULL;
  lc_error_init(&failure);
  rc = lc_cqs_paths(collection, query_id, ordinal, &directory, &path, context,
                    &failure);
  if (rc == LC_OK) {
    if (lstat(path, &st) != 0)
      rc = LC_ERR_TRANSPORT;
    else if (!S_ISREG(st.st_mode) || st.st_size < 0 ||
             (lc_u64)st.st_size > LC_CQS_STORED_BYTES)
      rc = LC_ERR_PROTOCOL;
  }
  if (rc == LC_OK && lc_pouch_crypto_enabled(collection->pouch->crypto)) {
    rc = lc_pouch_query_index_read_crypto_footer(
        &collection->allocator, path, LC_CQS_DESCRIPTOR_BYTES, &descriptor,
        &cipher_length, &failure);
    if (rc == LC_OK)
      rc = lc_pouch_crypto_source_from_file_span(
          collection->pouch->crypto, context, context, path, 0U, cipher_length,
          descriptor, &source, &failure);
  } else if (rc == LC_OK)
    rc = lc_source_from_file(path, &source, &failure);
  if (rc == LC_OK)
    rc = lc_cqs_read(collection, source, (size_t)st.st_size, &bytes, &length,
                     &failure);
  if (rc == LC_OK)
    rc = lc_collection_index_decode(index, &collection->allocator,
                                    collection->id, query_id, ordinal, row_hash,
                                    bytes, length, &failure);
  if (rc == LC_OK)
    *found = 1;
  else
    lc_collection_index_cleanup(index);
  lc_source_close(source);
  lc_free_with_allocator(&collection->allocator, directory);
  lc_free_with_allocator(&collection->allocator, path);
  lc_free_with_allocator(&collection->allocator, bytes);
  lc_free_with_allocator(&collection->allocator, descriptor);
  if (rc == LC_ERR_NOMEM && error != NULL) {
    lc_error_cleanup(error);
    *error = failure;
    memset(&failure, 0, sizeof(failure));
  }
  lc_error_cleanup(&failure);
  return rc == LC_ERR_NOMEM ? rc : LC_OK;
}

#include "lc_collection_internal.h"
#include "lc_collection_json.h"
#include "lc_log.h"
#include "lc_pouch_path.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define LC_CW_MEMORY_LIMIT (64U * 1024U)

enum { LC_CW_OPEN, LC_CW_FAILED, LC_CW_COMMITTED, LC_CW_ABORTED };
typedef struct lc_collection_writer_handle {
  lc_collection_writer pub;
  lc_collection_handle *collection;
  lc_allocator allocator;
  lc_pouch_collection_writer *encoder;
  lc_sink staging;
  unsigned char *memory;
  size_t capacity;
  lc_u64 length;
  lc_u64 offset;
  char *pending_path;
  int fd;
  int created;
  int state;
  int active;
  int closed;
  unsigned int references;
  lc_u64 ownership_generation;
} lc_collection_writer_handle;

static pthread_mutex_t lc_cw_lifecycle = PTHREAD_MUTEX_INITIALIZER;

static int lc_cw_discard(lc_collection_writer_handle *writer, lc_error *error) {
  int rc = LC_OK;
  lc_pouch_collection_writer_close(writer->encoder);
  writer->encoder = NULL;
  if (writer->created) {
    if (unlink(writer->pending_path) != 0 && errno != ENOENT)
      rc = lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                 "failed to remove pending collection append");
    else
      writer->created = 0;
  }
  if (writer->fd >= 0) {
    (void)close(writer->fd);
    writer->fd = -1;
  }
  lc_free_with_allocator(&writer->allocator, writer->memory);
  writer->memory = NULL;
  writer->capacity = 0U;
  return rc;
}

static int lc_cw_store(lc_collection_writer_handle *writer, const void *bytes,
                       size_t count, lc_error *error) {
  const unsigned char *cursor = bytes;
  ssize_t written;
  while (count != 0U) {
    written = write(writer->fd, cursor, count);
    if (written < 0 && errno == EINTR)
      continue;
    if (written <= 0)
      return lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                   "failed to write collection append spool");
    cursor += written;
    count -= (size_t)written;
  }
  return LC_OK;
}

static int lc_cw_stage(lc_sink *sink, const void *bytes, size_t count,
                       lc_error *error) {
  lc_collection_writer_handle *writer = sink->impl;
  unsigned char *grown;
  char id[LC_XID_STRING_SIZE], leaf[64];
  size_t capacity;
  int rc = LC_OK;
  if ((lc_u64)count > (lc_u64)LC_I64_MAX - writer->length) {
    lc_collection_failure(error, LC_ERR_INVALID, NULL,
                          "collection append exceeds file offset range");
    return 0;
  }
  if (writer->fd < 0 && writer->length + count <= LC_CW_MEMORY_LIMIT) {
    capacity = writer->capacity != 0U ? writer->capacity : 2048U;
    while (capacity < writer->length + count)
      capacity *= 2U;
    if (capacity != writer->capacity) {
      grown = lc_realloc_with_allocator(&writer->allocator, writer->memory,
                                        capacity);
      if (grown == NULL) {
        lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                              "failed to grow collection append buffer");
        return 0;
      }
      writer->memory = grown;
      writer->capacity = capacity;
    }
    if (count != 0U)
      memcpy(writer->memory + (size_t)writer->length, bytes, count);
  } else {
    if (writer->fd < 0) {
      rc = lc_xid_new(id, error);
      if (rc == LC_OK) {
        (void)snprintf(leaf, sizeof(leaf), "pending-%s.log", id);
        writer->pending_path = lc_pouch_path_join(
            &writer->allocator, writer->collection->directory, leaf);
        if (writer->pending_path == NULL)
          rc =
              lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                                    "failed to allocate collection spool path");
      }
      if (rc == LC_OK) {
        writer->fd = open(writer->pending_path,
                          O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
        writer->created = writer->fd >= 0;
        if (writer->fd < 0)
          rc =
              lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                    "failed to create collection append spool");
      }
      /* Only this fd owns the spool. Process exit reclaims it automatically;
       * a concurrent dropped collection cannot turn it into canonical data. */
      if (rc == LC_OK) {
        if (unlink(writer->pending_path) != 0 && errno != ENOENT)
          rc =
              lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                    "failed to unlink collection append spool");
        else
          writer->created = 0;
      }
      if (rc == LC_OK)
        rc = lc_cw_store(writer, writer->memory, (size_t)writer->length, error);
      lc_free_with_allocator(&writer->allocator, writer->memory);
      writer->memory = NULL;
      writer->capacity = 0U;
    }
    if (rc == LC_OK)
      rc = lc_cw_store(writer, bytes, count, error);
  }
  if (rc == LC_OK)
    writer->length += count;
  return rc == LC_OK;
}

static size_t lc_cw_read(lc_source *source, void *bytes, size_t count,
                         lc_error *error) {
  lc_collection_writer_handle *writer = source->impl;
  ssize_t got;
  if ((lc_u64)count > writer->length - writer->offset)
    count = (size_t)(writer->length - writer->offset);
  if (count == 0U)
    return 0U;
  if (writer->fd < 0) {
    memcpy(bytes, writer->memory + (size_t)writer->offset, count);
    got = (ssize_t)count;
  } else {
    do {
      got = pread(writer->fd, bytes, count, (off_t)writer->offset);
    } while (got < 0 && errno == EINTR);
    if (got <= 0) {
      lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                            "failed to read collection append spool");
      return 0U;
    }
  }
  writer->offset += (size_t)got;
  return (size_t)got;
}

static void lc_cw_destroy(lc_collection_writer_handle *writer) {
  lc_allocator allocator = writer->allocator;
  lc_collection_handle *collection = writer->collection;
  lc_error error;
  lc_error_init(&error);
  if (lc_cw_discard(writer, &error) != LC_OK) {
    pslog_field field = lc_log_error_field("error", &error);
    lc_log_warn(collection->pouch->logger,
                "collection writer cleanup deferred to maintenance", &field,
                1U);
  }
  lc_error_cleanup(&error);
  lc_free_with_allocator(&allocator, writer->pending_path);
  lc_free_with_allocator(&allocator, writer);
  lc_collection_handle_release(collection);
}

static int lc_cw_enter(lc_collection_writer *self, int require_open,
                       lc_collection_writer_handle **out, lc_error *error) {
  lc_collection_writer_handle *writer;
  *out = NULL;
  if (self == NULL || self->impl == NULL)
    return lc_collection_failure(
        error, LC_ERR_INVALID, NULL,
        "operation requires a collection append writer");
  writer = self->impl;
  pthread_mutex_lock(&lc_cw_lifecycle);
  if (writer->closed || writer->active ||
      (require_open && writer->state != LC_CW_OPEN)) {
    pthread_mutex_unlock(&lc_cw_lifecycle);
    return lc_collection_failure(
        error, LC_ERR_INVALID, NULL,
        "collection writer is closed, busy or terminal");
  }
  writer->active = 1;
  ++writer->references;
  pthread_mutex_unlock(&lc_cw_lifecycle);
  *out = writer;
  return LC_OK;
}

static void lc_cw_leave(lc_collection_writer_handle *writer) {
  int release;
  pthread_mutex_lock(&lc_cw_lifecycle);
  writer->active = 0;
  release = --writer->references == 0U;
  pthread_mutex_unlock(&lc_cw_lifecycle);
  if (release)
    lc_cw_destroy(writer);
}

static void lc_cw_close(lc_collection_writer *self) {
  lc_collection_writer_handle *writer = self->impl;
  int release = 0;
  pthread_mutex_lock(&lc_cw_lifecycle);
  if (!writer->closed) {
    writer->closed = 1;
    release = --writer->references == 0U;
  }
  pthread_mutex_unlock(&lc_cw_lifecycle);
  if (release)
    lc_cw_destroy(writer);
}

static lc_error *lc_cw_error(lc_error *error, lc_error *local) {
  lc_error_init(local);
  if (error == NULL)
    return local;
  lc_error_cleanup(error);
  lc_error_init(error);
  return error;
}

static int lc_cw_write(lc_collection_writer *self, const void *bytes,
                       size_t count, lc_error *error) {
  lc_collection_writer_handle *writer;
  lc_error local;
  int rc;
  error = lc_cw_error(error, &local);
  rc = lc_cw_enter(self, 1, &writer, error);
  if (rc == LC_OK) {
    rc = lc_pouch_collection_writer_write(writer->encoder, bytes, count, error);
    if (rc != LC_OK)
      writer->state = LC_CW_FAILED;
    lc_cw_leave(writer);
  }
  lc_error_cleanup(&local);
  return rc;
}

static int lc_cw_commit(lc_collection_writer *self,
                        lc_collection_append_res *out, lc_error *error) {
  lc_collection_writer_handle *writer;
  lc_collection_call call;
  lc_pouch_collection_record body;
  lc_pouch_collection_writer *encoder;
  lc_source staged;
  lc_error local;
  int rc, cleanup;
  error = lc_cw_error(error, &local);
  if (out == NULL) {
    rc = lc_collection_failure(error, LC_ERR_INVALID, NULL,
                               "collection writer commit requires output");
    lc_error_cleanup(&local);
    return rc;
  }
  memset(out, 0, sizeof(*out));
  rc = lc_cw_enter(self, 1, &writer, error);
  if (rc == LC_OK) {
    rc = lc_pouch_collection_writer_finish(writer->encoder, &body, error);
    if (rc == LC_OK) {
      rc = lc_collection_call_begin_held(writer->collection, 1, &call, error);
      if (rc == LC_OK) {
        memset(&staged, 0, sizeof(staged));
        staged.read = lc_cw_read;
        staged.impl = writer;
        encoder = writer->encoder;
        writer->encoder = NULL;
        rc = lc_collection_publish_staged(&call, encoder, &staged,
                                          writer->ownership_generation, &body,
                                          out, error);
        lc_collection_call_end(&call);
      }
    }
    writer->state = rc == LC_OK ? LC_CW_COMMITTED : LC_CW_FAILED;
    cleanup = lc_cw_discard(writer, rc == LC_OK ? error : NULL);
    if (rc == LC_OK)
      rc = cleanup;
    lc_cw_leave(writer);
  }
  lc_error_cleanup(&local);
  return rc;
}

static int lc_cw_abort(lc_collection_writer *self, lc_error *error) {
  lc_collection_writer_handle *writer;
  lc_error local;
  int rc;
  error = lc_cw_error(error, &local);
  rc = lc_cw_enter(self, 0, &writer, error);
  if (rc == LC_OK) {
    if (writer->state != LC_CW_COMMITTED)
      writer->state = LC_CW_ABORTED;
    rc = lc_cw_discard(writer, error);
    lc_cw_leave(writer);
  }
  lc_error_cleanup(&local);
  return rc;
}

int lc_collection_append_begin_method(lc_collection *self,
                                      const lc_collection_append_req *req,
                                      lc_collection_writer **out,
                                      lc_error *error) {
  lc_collection_call call;
  lc_collection_guard guard;
  lc_collection_writer_handle *writer = NULL;
  lc_pouch_collection_record existing;
  lc_u64 segment;
  int rc, entered = 0, found = 0;
  if (out != NULL)
    *out = NULL;
  if (out == NULL)
    return lc_collection_failure(error, LC_ERR_INVALID, NULL,
                                 "collection append begin requires output");
  rc = lc_collection_call_begin(self, 1, &call, error);
  if (rc != LC_OK)
    return rc;
  error = call.error;
  rc = lc_collection_append_validate(call.collection, req, error);
  if (rc == LC_OK) {
    writer = lc_calloc_with_allocator(&call.collection->allocator, 1U,
                                      sizeof(*writer));
    if (writer == NULL)
      rc = lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                                 "failed to allocate collection append writer");
    else {
      writer->pub.write = lc_cw_write;
      writer->pub.commit = lc_cw_commit;
      writer->pub.abort = lc_cw_abort;
      writer->pub.close = lc_cw_close;
      writer->pub.impl = writer;
      writer->collection = call.collection;
      writer->allocator = call.collection->allocator;
      writer->fd = -1;
      writer->references = 1U;
      writer->staging.write = lc_cw_stage;
      writer->staging.impl = writer;
      lc_collection_handle_hold(writer->collection);
    }
  }
  if (rc == LC_OK) {
    rc = lc_collection_append_enter(&call, &guard, error);
    entered = rc == LC_OK;
  }
  if (rc == LC_OK)
    writer->ownership_generation = call.collection->head.lease.fencing_token;
  if (rc == LC_OK)
    rc = lc_collection_lookup_id(call.collection, req->append_id, &existing,
                                 &segment, &found, error);
  if (rc == LC_OK)
    rc = lc_pouch_collection_writer_open_staged(
        call.collection->pouch, found ? NULL : &writer->staging,
        call.collection->id, req->append_id,
        req->has_retention_seconds
            ? -(lc_pouch_unix_seconds)req->retention_seconds
            : 0,
        req->has_collection_retention_seconds,
        req->has_collection_retention_seconds
            ? -(lc_pouch_unix_seconds)req->collection_retention_seconds
            : 0,
        &writer->encoder, error);
  if (entered) {
    lc_collection_metadata_leave(call.collection);
    lc_collection_write_leave(&guard);
  }
  if (rc == LC_OK)
    *out = &writer->pub;
  else if (writer != NULL)
    lc_cw_destroy(writer);
  lc_collection_call_end(&call);
  return rc;
}

int lc_collection_append_staged_method(lc_collection *self,
                                       const lc_collection_append_req *req,
                                       lc_source *source,
                                       lc_collection_append_res *out,
                                       lc_error *error) {
  lc_collection_call call;
  lc_collection_writer *writer = NULL;
  unsigned char *buffer = NULL;
  size_t got;
  int rc;
  if (out != NULL)
    memset(out, 0, sizeof(*out));
  if (out == NULL || source == NULL || source->read == NULL)
    return lc_collection_failure(error, LC_ERR_INVALID, NULL,
                                 "staged append requires source and output");
  rc = lc_collection_call_begin(self, 1, &call, error);
  if (rc != LC_OK)
    return rc;
  error = call.error;
  rc = lc_collection_append_begin_method(self, req, &writer, error);
  if (rc == LC_OK) {
    buffer = lc_alloc_with_allocator(&call.collection->allocator,
                                     LC_COLLECTION_JSON_CHUNK_BYTES);
    if (buffer == NULL)
      rc =
          lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                                "failed to allocate staged append input chunk");
  }
  while (rc == LC_OK) {
    lc_collection_call_callback(&call, 1, 1);
    got = source->read(source, buffer, LC_COLLECTION_JSON_CHUNK_BYTES, error);
    lc_collection_call_callback(&call, 0, 0);
    if (error->code != LC_OK)
      rc = error->code;
    else if (got > LC_COLLECTION_JSON_CHUNK_BYTES)
      rc = lc_collection_failure(
          error, LC_ERR_INVALID, NULL,
          "collection source exceeded requested chunk size");
    else if (got == 0U) {
      rc = lc_collection_writer_commit(writer, out, error);
      break;
    } else
      rc = lc_collection_writer_write(writer, buffer, got, error);
  }
  lc_collection_writer_close(writer);
  lc_free_with_allocator(&call.collection->allocator, buffer);
  lc_collection_call_end(&call);
  return rc;
}

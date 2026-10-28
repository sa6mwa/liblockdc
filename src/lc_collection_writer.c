#include "lc_collection_internal.h"
#include "lc_log.h"
#include "lc_pouch_path.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/file.h>
#include <unistd.h>

enum { LC_CW_OPEN, LC_CW_FAILED, LC_CW_COMMITTED, LC_CW_ABORTED };
typedef struct lc_collection_writer_handle {
  lc_collection_writer pub;
  lc_collection_handle *collection;
  lc_allocator allocator;
  lc_pouch_collection_writer *encoder;
  char *pending_path;
  int fd;
  int created;
  int state;
  int active;
  int closed;
  unsigned int references;
} lc_collection_writer_handle;

static pthread_mutex_t lc_cw_lifecycle = PTHREAD_MUTEX_INITIALIZER;

static int lc_cw_discard(lc_collection_writer_handle *writer, lc_error *error) {
  int rc = LC_OK;
  lc_pouch_collection_writer_close(writer->encoder);
  writer->encoder = NULL;
  if (writer->created) {
    /* Keep the original private name after canonical rename. Cleanup must
     * never unlink the adopted segment, including after a lost acknowledgement.
     */
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
  return rc;
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
        rc = lc_collection_publish_pending(&call, writer->encoder,
                                           writer->pending_path, writer->fd,
                                           &body, out, error);
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
  char id[LC_XID_STRING_SIZE], leaf[64];
  const char *placeholder = "seg-00000000000000000001.log";
  int rc, entered = 0, found = 0, locked;
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
      lc_collection_handle_hold(writer->collection);
    }
  }
  if (rc == LC_OK) {
    rc = lc_collection_write_enter(&call, &guard, error);
    entered = rc == LC_OK;
  }
  if (rc == LC_OK)
    rc = lc_collection_lookup_id(call.collection, req->append_id, &existing,
                                 &segment, &found, error);
  if (rc == LC_OK && !found) {
    rc = lc_xid_new(id, error);
    if (rc == LC_OK) {
      (void)snprintf(leaf, sizeof(leaf), "pending-%s.log", id);
      writer->pending_path = lc_pouch_path_join(
          &writer->allocator, call.collection->directory, leaf);
      if (writer->pending_path == NULL)
        rc =
            lc_collection_failure(error, LC_ERR_NOMEM, NULL,
                                  "failed to allocate collection pending path");
    }
    if (rc == LC_OK) {
      writer->fd = open(writer->pending_path,
                        O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
      writer->created = writer->fd >= 0;
      if (writer->fd < 0)
        rc =
            lc_collection_failure(error, LC_ERR_TRANSPORT, NULL,
                                  "failed to create pending collection append");
    }
    if (rc == LC_OK) {
      do {
        locked = flock(writer->fd, LOCK_EX | LOCK_NB);
      } while (locked != 0 && errno == EINTR);
      if (locked != 0)
        rc = lc_collection_failure(
            error, LC_ERR_TRANSPORT, NULL,
            "failed to protect pending collection append");
    }
  }
  if (rc == LC_OK)
    rc = lc_pouch_collection_writer_open(
        call.collection->pouch, writer->fd, placeholder, call.collection->id,
        req->append_id, 0, 0,
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

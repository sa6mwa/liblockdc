#include "lc/lc.h"
#include "lc_engine_api.h"
#include "lc_log.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>

#include "lc_api_internal.h"

typedef struct lc_source_impl {
  lc_source pub;
  size_t (*read_impl)(struct lc_source_impl *self, void *buffer, size_t count,
                      lc_error *error);
  int (*reset_impl)(struct lc_source_impl *self, lc_error *error);
  void (*close_impl)(struct lc_source_impl *self);
} lc_source_impl;

typedef struct lc_sink_impl {
  lc_sink pub;
  int (*write_impl)(struct lc_sink_impl *self, const void *bytes, size_t count,
                    lc_error *error);
  void (*close_impl)(struct lc_sink_impl *self);
} lc_sink_impl;

typedef struct lc_memory_source {
  lc_source_impl base;
  unsigned char *bytes;
  size_t length;
  size_t offset;
  int owns_bytes;
} lc_memory_source;

typedef struct lc_file_source {
  lc_source_impl base;
  FILE *fp;
  int close_file;
} lc_file_source;

typedef struct lc_fd_source {
  lc_source_impl base;
  int fd;
} lc_fd_source;

typedef struct lc_callback_source {
  lc_source_impl base;
  lc_source_read_fn read;
  lc_source_reset_fn reset;
  lc_source_close_fn close;
  void *context;
} lc_callback_source;

typedef struct lc_bundle_capture_source {
  lc_source pub;
  lc_source *inner;
  const lc_allocator *allocator;
  unsigned char *bytes;
  size_t length;
  size_t capacity;
} lc_bundle_capture_source;

static int lc_endpoint_is_pouch(const char *endpoint) {
  return endpoint != NULL && strncmp(endpoint, "pouch://", 8U) == 0;
}

static const char *lc_pouch_endpoint_path(const char *endpoint) {
  return endpoint != NULL ? endpoint + 8 : NULL;
}

typedef struct lc_fd_sink {
  lc_sink_impl base;
  int fd;
  int close_fd;
} lc_fd_sink;

typedef struct lc_discard_sink {
  lc_sink_impl base;
} lc_discard_sink;

static int lc_discard_sink_marker;

typedef struct lc_memory_sink {
  lc_sink_impl base;
  unsigned char *bytes;
  size_t length;
  size_t capacity;
} lc_memory_sink;

struct lc_stream_pipe {
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  lc_allocator allocator;
  unsigned char *buffer;
  size_t capacity;
  size_t read_pos;
  size_t write_pos;
  size_t used;
  int writer_closed;
  int reader_closed;
  int reader_ref;
  int writer_ref;
  int error_code;
  char *error_message;
};

typedef struct lc_stream_source {
  lc_source_impl base;
  lc_allocator allocator;
  lc_stream_pipe *pipe;
} lc_stream_source;

static void *lc_default_malloc(size_t size) { return malloc(size); }

static void *lc_default_realloc(void *ptr, size_t size) {
  return realloc(ptr, size);
}

void *lc_alloc_with_allocator(const lc_allocator *allocator, size_t size) {
  if (allocator != NULL && allocator->malloc_fn != NULL) {
    return allocator->malloc_fn(allocator->context, size);
  }
  return lc_default_malloc(size);
}

void *lc_calloc_with_allocator(const lc_allocator *allocator, size_t count,
                               size_t size) {
  void *ptr;
  size_t total;

  if (count != 0U && size > ((size_t)-1) / count) {
    return NULL;
  }
  total = count * size;
  ptr = lc_alloc_with_allocator(allocator, total == 0U ? 1U : total);
  if (ptr != NULL) {
    memset(ptr, 0, total);
  }
  return ptr;
}

void *lc_realloc_with_allocator(const lc_allocator *allocator, void *ptr,
                                size_t size) {
  if (allocator != NULL && allocator->realloc_fn != NULL) {
    return allocator->realloc_fn(allocator->context, ptr, size);
  }
  return lc_default_realloc(ptr, size);
}

void lc_free_with_allocator(const lc_allocator *allocator, void *ptr) {
  if (ptr == NULL) {
    return;
  }
  if (allocator != NULL && allocator->free_fn != NULL) {
    allocator->free_fn(allocator->context, ptr);
    return;
  }
  free(ptr);
}

void lc_secret_wipe(void *ptr, size_t length) {
  volatile unsigned char *cursor;

  if (ptr == NULL) {
    return;
  }
  cursor = (volatile unsigned char *)ptr;
  while (length > 0U) {
    *cursor++ = 0U;
    --length;
  }
}

void lc_secret_free_string_with_allocator(const lc_allocator *allocator,
                                          char *value) {
  if (value == NULL) {
    return;
  }
  lc_secret_wipe(value, strlen(value));
  lc_free_with_allocator(allocator, value);
}

char *lc_strdup_with_allocator(const lc_allocator *allocator,
                               const char *value) {
  size_t length;
  char *copy;

  if (value == NULL) {
    return NULL;
  }
  length = strlen(value);
  copy = (char *)lc_alloc_with_allocator(allocator, length + 1U);
  if (copy == NULL) {
    return NULL;
  }
  memcpy(copy, value, length + 1U);
  return copy;
}

char *lc_dup_bytes_with_allocator(const lc_allocator *allocator,
                                  const void *bytes, size_t length) {
  char *copy;

  copy = (char *)lc_alloc_with_allocator(allocator, length + 1U);
  if (copy == NULL) {
    return NULL;
  }
  if (length > 0U) {
    memcpy(copy, bytes, length);
  }
  copy[length] = '\0';
  return copy;
}

void *lc_client_alloc(lc_client_handle *client, size_t size) {
  return lc_alloc_with_allocator(client != NULL ? &client->allocator : NULL,
                                 size);
}

void *lc_client_calloc(lc_client_handle *client, size_t count, size_t size) {
  return lc_calloc_with_allocator(client != NULL ? &client->allocator : NULL,
                                  count, size);
}

void *lc_client_realloc(lc_client_handle *client, void *ptr, size_t size) {
  return lc_realloc_with_allocator(client != NULL ? &client->allocator : NULL,
                                   ptr, size);
}

void lc_client_free(lc_client_handle *client, void *ptr) {
  lc_free_with_allocator(client != NULL ? &client->allocator : NULL, ptr);
}

char *lc_client_strdup(lc_client_handle *client, const char *value) {
  return lc_strdup_with_allocator(client != NULL ? &client->allocator : NULL,
                                  value);
}

int lc_error_set(lc_error *error, int code, long http_status,
                 const char *message, const char *detail,
                 const char *server_code, const char *correlation_id) {
  char *message_copy;
  char *detail_copy;
  char *server_code_copy;
  char *correlation_id_copy;
  size_t length;

  if (error == NULL) {
    return code;
  }
  message_copy = NULL;
  detail_copy = NULL;
  server_code_copy = NULL;
  correlation_id_copy = NULL;
  if (message != NULL) {
    length = strlen(message);
    message_copy = (char *)malloc(length + 1U);
    if (message_copy != NULL) {
      memcpy(message_copy, message, length + 1U);
    }
  }
  if (detail != NULL) {
    length = strlen(detail);
    detail_copy = (char *)malloc(length + 1U);
    if (detail_copy != NULL) {
      memcpy(detail_copy, detail, length + 1U);
    }
  }
  if (server_code != NULL) {
    length = strlen(server_code);
    server_code_copy = (char *)malloc(length + 1U);
    if (server_code_copy != NULL) {
      memcpy(server_code_copy, server_code, length + 1U);
    }
  }
  if (correlation_id != NULL) {
    length = strlen(correlation_id);
    correlation_id_copy = (char *)malloc(length + 1U);
    if (correlation_id_copy != NULL) {
      memcpy(correlation_id_copy, correlation_id, length + 1U);
    }
  }
  lc_error_cleanup(error);
  error->code = code;
  error->http_status = http_status;
  error->message = message_copy;
  error->detail = detail_copy;
  error->server_code = server_code_copy;
  error->correlation_id = correlation_id_copy;
  return code;
}

int lc_error_from_engine(lc_error *error, lc_engine_error *engine) {
  int code;

  code = LC_ERR_SERVER;
  if (engine->code == LC_ENGINE_OK) {
    code = LC_OK;
  } else if (engine->code == LC_ENGINE_ERROR_INVALID_ARGUMENT) {
    code = LC_ERR_INVALID;
  } else if (engine->code == LC_ENGINE_ERROR_NO_MEMORY) {
    code = LC_ERR_NOMEM;
  } else if (engine->code == LC_ENGINE_ERROR_TRANSPORT) {
    code = LC_ERR_TRANSPORT;
  } else if (engine->code == LC_ENGINE_ERROR_PROTOCOL) {
    code = LC_ERR_PROTOCOL;
  }
  return lc_error_set(error, code, engine->http_status, engine->message,
                      engine->detail, engine->server_error_code,
                      engine->correlation_id);
}

void *lc_calloc_local(size_t count, size_t size) { return calloc(count, size); }

void *lc_realloc_local(void *ptr, size_t size) { return realloc(ptr, size); }

char *lc_strdup_local(const char *value) {
  size_t length;
  char *copy;

  if (value == NULL) {
    return NULL;
  }
  length = strlen(value);
  copy = (char *)malloc(length + 1U);
  if (copy == NULL) {
    return NULL;
  }
  memcpy(copy, value, length + 1U);
  return copy;
}

char *lc_dup_bytes_as_text(const void *bytes, size_t length) {
  char *copy;

  copy = (char *)malloc(length + 1U);
  if (copy == NULL) {
    return NULL;
  }
  if (length > 0U) {
    memcpy(copy, bytes, length);
  }
  copy[length] = '\0';
  return copy;
}

void lc_attachment_info_copy(lc_attachment_info *dst,
                             const lc_engine_attachment_info *src) {
  memset(dst, 0, sizeof(*dst));
  if (src == NULL) {
    return;
  }
  dst->id = lc_strdup_local(src->id);
  dst->name = lc_strdup_local(src->name);
  dst->size = src->size;
  dst->plaintext_sha256 = lc_strdup_local(src->plaintext_sha256);
  dst->content_type = lc_strdup_local(src->content_type);
  dst->created_at_unix = src->created_at_unix;
  dst->updated_at_unix = src->updated_at_unix;
}

static size_t lc_source_pub_read(lc_source *self, void *buffer, size_t count,
                                 lc_error *error) {
  lc_source_impl *impl;

  impl = (lc_source_impl *)self;
  return impl->read_impl(impl, buffer, count, error);
}

static int lc_source_pub_reset(lc_source *self, lc_error *error) {
  lc_source_impl *impl;

  impl = (lc_source_impl *)self;
  if (impl->reset_impl == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, "source is not resettable",
                        NULL, NULL, NULL);
  }
  return impl->reset_impl(impl, error);
}

static void lc_source_pub_close(lc_source *self) {
  lc_source_impl *impl;

  if (self == NULL) {
    return;
  }
  impl = (lc_source_impl *)self;
  if (impl->close_impl != NULL) {
    impl->close_impl(impl);
  }
}

static size_t lc_bundle_capture_source_read(lc_source *self, void *buffer,
                                            size_t count, lc_error *error) {
  lc_bundle_capture_source *capture;
  size_t nread;

  capture = (lc_bundle_capture_source *)self->impl;
  nread = capture->inner->read(capture->inner, buffer, count, error);
  if (nread == 0U) {
    return 0U;
  }
  if (nread > ((size_t)-1) - capture->length) {
    lc_error_set(error, LC_ERR_NOMEM, 0L, "client bundle source is too large",
                 NULL, NULL, NULL);
    return 0U;
  }
  if (capture->length + nread > capture->capacity) {
    size_t next_capacity;
    unsigned char *next_bytes;

    next_capacity = capture->capacity == 0U ? 8192U : capture->capacity;
    while (next_capacity < capture->length + nread) {
      if (next_capacity > ((size_t)-1) / 2U) {
        next_capacity = capture->length + nread;
        break;
      }
      next_capacity *= 2U;
    }
    next_bytes = (unsigned char *)lc_realloc_with_allocator(
        capture->allocator, capture->bytes, next_capacity);
    if (next_bytes == NULL) {
      lc_error_set(error, LC_ERR_NOMEM, 0L,
                   "failed to retain client bundle source", NULL, NULL, NULL);
      return 0U;
    }
    capture->bytes = next_bytes;
    capture->capacity = next_capacity;
  }
  memcpy(capture->bytes + capture->length, buffer, nread);
  capture->length += nread;
  return nread;
}

static int lc_bundle_capture_source_reset(lc_source *self, lc_error *error) {
  (void)self;
  return lc_error_set(error, LC_ERR_INVALID, 0L, "source is not resettable",
                      NULL, NULL, NULL);
}

static void lc_bundle_capture_source_close(lc_source *self) { (void)self; }

static int lc_sink_pub_write(lc_sink *self, const void *bytes, size_t count,
                             lc_error *error) {
  lc_sink_impl *impl;

  impl = (lc_sink_impl *)self;
  return impl->write_impl(impl, bytes, count, error);
}

static void lc_sink_pub_close(lc_sink *self) {
  lc_sink_impl *impl;

  if (self == NULL) {
    return;
  }
  impl = (lc_sink_impl *)self;
  if (impl->close_impl != NULL) {
    impl->close_impl(impl);
  }
}

static void lc_stream_pipe_release_locked(lc_stream_pipe *pipe) {
  int free_pipe;

  free_pipe = pipe->reader_ref == 0 && pipe->writer_ref == 0;
  pthread_mutex_unlock(&pipe->mutex);
  if (free_pipe) {
    pthread_cond_destroy(&pipe->cond);
    pthread_mutex_destroy(&pipe->mutex);
    lc_free_with_allocator(&pipe->allocator, pipe->buffer);
    lc_free_with_allocator(&pipe->allocator, pipe->error_message);
    lc_free_with_allocator(&pipe->allocator, pipe);
  }
}

static size_t lc_stream_source_read(lc_source_impl *self, void *buffer,
                                    size_t count, lc_error *error) {
  lc_stream_source *source;
  lc_stream_pipe *pipe;
  size_t first;
  size_t second;

  source = (lc_stream_source *)self;
  pipe = source->pipe;
  pthread_mutex_lock(&pipe->mutex);
  while (pipe->used == 0U && !pipe->writer_closed && !pipe->reader_closed) {
    pthread_cond_wait(&pipe->cond, &pipe->mutex);
  }
  if (pipe->reader_closed) {
    lc_stream_pipe_release_locked(pipe);
    return 0U;
  }
  if (pipe->used == 0U) {
    if (pipe->error_code != LC_OK) {
      lc_error_set(error, pipe->error_code, 0L,
                   pipe->error_message != NULL ? pipe->error_message
                                               : "stream delivery failed",
                   NULL, NULL, NULL);
    }
    lc_stream_pipe_release_locked(pipe);
    return 0U;
  }
  if (count > pipe->used) {
    count = pipe->used;
  }
  first = count;
  if (pipe->read_pos + first > pipe->capacity) {
    first = pipe->capacity - pipe->read_pos;
  }
  memcpy(buffer, pipe->buffer + pipe->read_pos, first);
  second = count - first;
  if (second > 0U) {
    memcpy((unsigned char *)buffer + first, pipe->buffer, second);
  }
  pipe->read_pos = (pipe->read_pos + count) % pipe->capacity;
  pipe->used -= count;
  pthread_cond_broadcast(&pipe->cond);
  lc_stream_pipe_release_locked(pipe);
  return count;
}

static int lc_stream_source_reset(lc_source_impl *self, lc_error *error) {
  (void)self;
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "streamed payloads are not rewindable", NULL, NULL, NULL);
}

static void lc_stream_source_close(lc_source_impl *self) {
  lc_stream_source *source;
  lc_stream_pipe *pipe;

  source = (lc_stream_source *)self;
  pipe = source->pipe;
  pthread_mutex_lock(&pipe->mutex);
  pipe->reader_closed = 1;
  pipe->reader_ref = 0;
  pthread_cond_broadcast(&pipe->cond);
  lc_stream_pipe_release_locked(pipe);
  lc_free_with_allocator(&source->allocator, source);
}

static size_t lc_memory_source_read(lc_source_impl *base, void *buffer,
                                    size_t count, lc_error *error) {
  lc_memory_source *source;
  size_t available;
  size_t chunk;

  (void)error;
  source = (lc_memory_source *)base;
  if (source->offset >= source->length) {
    return 0U;
  }
  available = source->length - source->offset;
  chunk = count;
  if (chunk > available) {
    chunk = available;
  }
  memcpy(buffer, source->bytes + source->offset, chunk);
  source->offset += chunk;
  return chunk;
}

static int lc_memory_source_reset(lc_source_impl *base, lc_error *error) {
  lc_memory_source *source;

  (void)error;
  source = (lc_memory_source *)base;
  source->offset = 0U;
  return LC_OK;
}

static void lc_memory_source_close(lc_source_impl *base) {
  lc_memory_source *source;

  source = (lc_memory_source *)base;
  if (source->owns_bytes) {
    free(source->bytes);
  }
  free(source);
}

static size_t lc_file_source_read(lc_source_impl *base, void *buffer,
                                  size_t count, lc_error *error) {
  lc_file_source *source;
  size_t nread;

  (void)error;
  source = (lc_file_source *)base;
  nread = fread(buffer, 1U, count, source->fp);
  return nread;
}

static int lc_file_source_reset(lc_source_impl *base, lc_error *error) {
  lc_file_source *source;

  source = (lc_file_source *)base;
  if (fseek(source->fp, 0L, SEEK_SET) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to rewind file source", NULL, NULL, NULL);
  }
  return LC_OK;
}

static void lc_file_source_close(lc_source_impl *base) {
  lc_file_source *source;

  source = (lc_file_source *)base;
  if (source->close_file && source->fp != NULL) {
    fclose(source->fp);
  }
  free(source);
}

static size_t lc_fd_source_read(lc_source_impl *base, void *buffer,
                                size_t count, lc_error *error) {
  lc_fd_source *source;
  ssize_t nread;

  source = (lc_fd_source *)base;
  nread = read(source->fd, buffer, count);
  if (nread < 0) {
    lc_error_set(error, LC_ERR_TRANSPORT, 0L, "failed to read from fd source",
                 NULL, NULL, NULL);
    return 0U;
  }
  return (size_t)nread;
}

static void lc_fd_source_close(lc_source_impl *base) {
  lc_fd_source *source;

  source = (lc_fd_source *)base;
  free(source);
}

static size_t lc_callback_source_read(lc_source_impl *base, void *buffer,
                                      size_t count, lc_error *error) {
  lc_callback_source *source;

  source = (lc_callback_source *)base;
  return source->read(source->context, buffer, count, error);
}

static int lc_callback_source_reset(lc_source_impl *base, lc_error *error) {
  lc_callback_source *source;

  source = (lc_callback_source *)base;
  if (source->reset == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, "source is not resettable",
                        NULL, NULL, NULL);
  }
  return source->reset(source->context, error);
}

static void lc_callback_source_close(lc_source_impl *base) {
  lc_callback_source *source;

  source = (lc_callback_source *)base;
  if (source->close != NULL) {
    source->close(source->context);
  }
  free(source);
}

static int lc_fd_sink_write(lc_sink_impl *base, const void *bytes, size_t count,
                            lc_error *error) {
  lc_fd_sink *sink;
  ssize_t nwritten;

  sink = (lc_fd_sink *)base;
  nwritten = write(sink->fd, bytes, count);
  if (nwritten < 0 || (size_t)nwritten != count) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to write to fd sink", NULL, NULL, NULL);
  }
  return 1;
}

static void lc_fd_sink_close(lc_sink_impl *base) {
  lc_fd_sink *sink;

  sink = (lc_fd_sink *)base;
  if (sink->close_fd) {
    close(sink->fd);
  }
  free(sink);
}

static int lc_discard_sink_write(lc_sink_impl *base, const void *bytes,
                                 size_t count, lc_error *error) {
  (void)base;
  (void)bytes;
  (void)count;
  (void)error;
  return 1;
}

static void lc_discard_sink_close(lc_sink_impl *base) { free(base); }

static int lc_memory_sink_write(lc_sink_impl *base, const void *bytes,
                                size_t count, lc_error *error) {
  lc_memory_sink *sink;
  unsigned char *next;
  size_t needed;
  size_t capacity;

  sink = (lc_memory_sink *)base;
  needed = sink->length + count;
  if (needed > sink->capacity) {
    capacity = sink->capacity == 0U ? 4096U : sink->capacity * 2U;
    while (capacity < needed) {
      capacity *= 2U;
    }
    next = (unsigned char *)realloc(sink->bytes, capacity);
    if (next == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to grow memory sink",
                          NULL, NULL, NULL);
    }
    sink->bytes = next;
    sink->capacity = capacity;
  }
  memcpy(sink->bytes + sink->length, bytes, count);
  sink->length += count;
  return 1;
}

static void lc_memory_sink_close(lc_sink_impl *base) {
  lc_memory_sink *sink;

  sink = (lc_memory_sink *)base;
  free(sink->bytes);
  free(sink);
}

size_t lc_engine_read_bridge(void *context, void *buffer, size_t count,
                             lc_engine_error *error) {
  lc_read_bridge *bridge;
  lc_error public_error;
  size_t nread;

  bridge = (lc_read_bridge *)context;
  lc_error_init(&public_error);
  nread = bridge->source->read(bridge->source, buffer, count, &public_error);
  if (public_error.code != LC_OK) {
    error->code = LC_ENGINE_ERROR_TRANSPORT;
    error->message = lc_strdup_local(public_error.message);
  }
  lc_error_cleanup(&public_error);
  return nread;
}

int lc_engine_reset_bridge(void *context, lc_engine_error *error) {
  lc_read_bridge *bridge;
  lc_error public_error;
  int rc;

  bridge = (lc_read_bridge *)context;
  if (bridge == NULL || bridge->source == NULL) {
    if (error != NULL) {
      if (error->message != NULL) {
        free(error->message);
        error->message = NULL;
      }
      error->code = LC_ENGINE_ERROR_INVALID_ARGUMENT;
      error->http_status = 0L;
      error->message = lc_strdup_local("payload reset requires source");
    }
    return LC_ENGINE_ERROR_INVALID_ARGUMENT;
  }
  if (bridge->source->reset == NULL) {
    if (error != NULL) {
      if (error->message != NULL) {
        free(error->message);
        error->message = NULL;
      }
      error->code = LC_ENGINE_ERROR_INVALID_ARGUMENT;
      error->http_status = 0L;
      error->message = lc_strdup_local("payload source is not rewindable");
    }
    return LC_ENGINE_ERROR_INVALID_ARGUMENT;
  }
  lc_error_init(&public_error);
  rc = bridge->source->reset(bridge->source, &public_error);
  if (rc != LC_OK && public_error.code != LC_OK) {
    error->code = LC_ENGINE_ERROR_TRANSPORT;
    error->message = lc_strdup_local(public_error.message);
  }
  lc_error_cleanup(&public_error);
  return rc;
}

int lc_engine_write_bridge(void *context, const void *bytes, size_t count,
                           lc_engine_error *error) {
  lc_write_bridge *bridge;
  lc_error public_error;
  int rc;

  bridge = (lc_write_bridge *)context;
  lc_error_init(&public_error);
  rc = bridge->sink->write(bridge->sink, bytes, count, &public_error);
  if (!rc && public_error.code != LC_OK) {
    error->code = LC_ENGINE_ERROR_TRANSPORT;
    error->message = lc_strdup_local(public_error.message);
  }
  lc_error_cleanup(&public_error);
  return rc;
}

lc_lease *lc_lease_new(lc_client_handle *client, const char *namespace_name,
                       const char *key, const char *owner, const char *lease_id,
                       const char *txn_id, long fencing_token, lc_version version,
                       const char *state_etag, const char *queue_state_etag) {
  lc_lease_handle *lease;

  lease = (lc_lease_handle *)lc_client_calloc(client, 1U, sizeof(*lease));
  if (lease == NULL) {
    return NULL;
  }
  lease->pub.describe = lc_lease_describe_method;
  lease->pub.get = lc_lease_get_method;
  lease->pub.load = lc_lease_load_method;
  lease->pub.save = lc_lease_save_method;
  lease->pub.update = lc_lease_update_method;
  lease->pub.mutate = lc_lease_mutate_method;
  lease->pub.mutate_local = lc_lease_mutate_local_method;
  lease->pub.metadata = lc_lease_metadata_method;
  lease->pub.remove = lc_lease_remove_method;
  lease->pub.keepalive = lc_lease_keepalive_method;
  lease->pub.release = lc_lease_release_method;
  lease->pub.attach = lc_lease_attach_method;
  lease->pub.list_attachments = lc_lease_list_attachments_method;
  lease->pub.get_attachment = lc_lease_get_attachment_method;
  lease->pub.delete_attachment = lc_lease_delete_attachment_method;
  lease->pub.delete_all_attachments = lc_lease_delete_all_attachments_method;
  lease->pub.close = lc_lease_close_method;
  lease->client = client;
  lease->namespace_name = lc_client_strdup(client, namespace_name);
  lease->key = lc_client_strdup(client, key);
  lease->owner = lc_client_strdup(client, owner);
  lease->lease_id = lc_client_strdup(client, lease_id);
  lease->txn_id = lc_client_strdup(client, txn_id);
  lease->fencing_token = fencing_token;
  lease->version = version;
  lease->lease_expires_at_unix = 0L;
  lease->state_etag = lc_client_strdup(client, state_etag);
  lease->queue_state_etag = lc_client_strdup(client, queue_state_etag);
  lease->has_query_hidden = 0;
  lease->query_hidden = 0;
  lease->pub.namespace_name = lease->namespace_name;
  lease->pub.key = lease->key;
  lease->pub.owner = lease->owner;
  lease->pub.lease_id = lease->lease_id;
  lease->pub.txn_id = lease->txn_id;
  lease->pub.fencing_token = lease->fencing_token;
  lease->pub.version = lease->version;
  lease->pub.lease_expires_at_unix = lease->lease_expires_at_unix;
  lease->pub.state_etag = lease->state_etag;
  lease->pub.has_query_hidden = lease->has_query_hidden;
  lease->pub.query_hidden = lease->query_hidden;
  return &lease->pub;
}

static char *lc_queue_state_key_new(lc_client_handle *client, const char *queue,
                                    const char *message_id) {
  size_t queue_length;
  size_t message_id_length;
  size_t total_length;
  char *key;

  if (queue == NULL || queue[0] == '\0' || message_id == NULL ||
      message_id[0] == '\0') {
    return NULL;
  }
  queue_length = strlen(queue);
  message_id_length = strlen(message_id);
  total_length = sizeof("q/") - 1U + queue_length + sizeof("/state/") - 1U +
                 message_id_length + 1U;
  key = (char *)lc_client_alloc(client, total_length);
  if (key == NULL) {
    return NULL;
  }
  snprintf(key, total_length, "q/%s/state/%s", queue, message_id);
  return key;
}

lc_message *lc_message_new(lc_client_handle *client,
                           const lc_engine_dequeue_response *engine,
                           lc_source *payload, int *terminal_flag) {
  lc_message_handle *message;
  char *state_key;

  message = (lc_message_handle *)lc_client_calloc(client, 1U, sizeof(*message));
  if (message == NULL) {
    return NULL;
  }
  message->pub.ack = lc_message_ack_method;
  message->pub.nack = lc_message_nack_method;
  message->pub.extend = lc_message_extend_method;
  if (client != NULL && client->is_pouch) {
    message->pub.ack = lc_pouch_message_ack_method;
    message->pub.nack = lc_pouch_message_nack_method;
    message->pub.extend = lc_pouch_message_extend_method;
  }
  message->pub.state = lc_message_state_method;
  message->pub.payload_reader = lc_message_payload_reader_method;
  message->pub.rewind_payload = lc_message_rewind_payload_method;
  message->pub.write_payload = lc_message_write_payload_method;
  message->pub.close = lc_message_close_method;
  message->client = client;
  message->namespace_name = lc_client_strdup(client, engine->namespace_name);
  message->queue = lc_client_strdup(client, engine->queue);
  message->message_id = lc_client_strdup(client, engine->message_id);
  message->attempts = engine->attempts;
  message->max_attempts = engine->max_attempts;
  message->failure_attempts = engine->failure_attempts;
  message->not_visible_until_unix = engine->not_visible_until_unix;
  message->visibility_timeout_seconds = engine->visibility_timeout_seconds;
  message->payload_content_type =
      lc_client_strdup(client, engine->payload_content_type);
  message->correlation_id = lc_client_strdup(client, engine->correlation_id);
  message->lease_id = lc_client_strdup(client, engine->lease_id);
  message->lease_expires_at_unix = engine->lease_expires_at_unix;
  message->fencing_token = engine->fencing_token;
  message->txn_id = lc_client_strdup(client, engine->txn_id);
  message->meta_etag = lc_client_strdup(client, engine->meta_etag);
  message->next_cursor = lc_client_strdup(client, engine->next_cursor);
  message->payload = payload;
  message->terminal_flag = terminal_flag;
  message->state_etag = lc_client_strdup(client, engine->state_etag);
  message->state_lease_id = lc_client_strdup(client, engine->state_lease_id);
  message->state_lease_expires_at_unix = engine->state_lease_expires_at_unix;
  message->state_fencing_token = engine->state_fencing_token;
  message->state_txn_id = lc_client_strdup(client, engine->state_txn_id);
  state_key = NULL;
  if (engine->state_lease_id != NULL && engine->state_lease_id[0] != '\0') {
    state_key =
        lc_queue_state_key_new(client, engine->queue, engine->message_id);
    message->state_lease =
        lc_lease_new(client, engine->namespace_name, state_key, NULL,
                     engine->state_lease_id, engine->state_txn_id,
                     engine->state_fencing_token, 0L, NULL, engine->state_etag);
    lc_client_free(client, state_key);
  }
  message->pub.namespace_name = message->namespace_name;
  message->pub.queue = message->queue;
  message->pub.message_id = message->message_id;
  message->pub.attempts = message->attempts;
  message->pub.max_attempts = message->max_attempts;
  message->pub.failure_attempts = message->failure_attempts;
  message->pub.not_visible_until_unix = message->not_visible_until_unix;
  message->pub.visibility_timeout_seconds = message->visibility_timeout_seconds;
  message->pub.payload_content_type = message->payload_content_type;
  message->pub.correlation_id = message->correlation_id;
  message->pub.lease_id = message->lease_id;
  message->pub.lease_expires_at_unix = message->lease_expires_at_unix;
  message->pub.fencing_token = message->fencing_token;
  message->pub.txn_id = message->txn_id;
  message->pub.meta_etag = message->meta_etag;
  message->pub.next_cursor = message->next_cursor;
  message->pub.payload = message->payload;
  return &message->pub;
}

const char *lc_version_string(void) { return lc_engine_version_string(); }

void lc_error_init(lc_error *error) {
  if (error != NULL) {
    memset(error, 0, sizeof(*error));
  }
}

void lc_error_cleanup(lc_error *error) {
  if (error == NULL) {
    return;
  }
  free(error->message);
  free(error->detail);
  free(error->server_code);
  free(error->correlation_id);
  memset(error, 0, sizeof(*error));
}

void lc_allocator_init(lc_allocator *allocator) {
  if (allocator != NULL) {
    memset(allocator, 0, sizeof(*allocator));
  }
}

typedef struct lc_pouch_endpoint_options {
  char *root_path;
  char *query_engine;
  char *query_fallback_engine;
  char *crypto_key;
  char *crypto_key_file;
  char *compression;
  int crypto_generate_key_file;
  int single_writer;
  uint64_t fsync_batch_max_ops;
  int queue_watch;
  int background_compaction_enabled;
  int background_compaction_enabled_set;
  int compaction_throttling_disabled;
  uint64_t retention_seconds;
  uint64_t janitor_interval_seconds;
} lc_pouch_endpoint_options;

static void
lc_pouch_endpoint_options_cleanup(const lc_allocator *allocator,
                                  lc_pouch_endpoint_options *options) {
  if (options == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, options->root_path);
  lc_free_with_allocator(allocator, options->query_engine);
  lc_free_with_allocator(allocator, options->query_fallback_engine);
  lc_secret_free_string_with_allocator(allocator, options->crypto_key);
  lc_free_with_allocator(allocator, options->crypto_key_file);
  lc_free_with_allocator(allocator, options->compression);
  memset(options, 0, sizeof(*options));
}

static int lc_query_part_equal(const char *part, size_t part_len,
                               const char *expected) {
  size_t expected_len;

  expected_len = strlen(expected);
  return part_len == expected_len && strncmp(part, expected, part_len) == 0;
}

static int lc_uri_hex_value(char ch) {
  if (ch >= '0' && ch <= '9') {
    return ch - '0';
  }
  if (ch >= 'a' && ch <= 'f') {
    return ch - 'a' + 10;
  }
  if (ch >= 'A' && ch <= 'F') {
    return ch - 'A' + 10;
  }
  return -1;
}

static char *lc_pouch_endpoint_decode_component(const lc_allocator *allocator,
                                                const char *src, size_t src_len,
                                                const char *component,
                                                lc_error *error) {
  char *decoded;
  size_t src_index;
  size_t dst_index;

  decoded = (char *)lc_alloc_with_allocator(allocator, src_len + 1U);
  if (decoded == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch endpoint component", NULL, NULL,
                 NULL);
    return NULL;
  }
  src_index = 0U;
  dst_index = 0U;
  while (src_index < src_len) {
    if (src[src_index] == '%') {
      int high;
      int low;
      unsigned char value;

      if (src_index + 2U >= src_len) {
        lc_free_with_allocator(allocator, decoded);
        lc_error_set(error, LC_ERR_INVALID, 0L,
                     "invalid percent escape in pouch endpoint", component,
                     NULL, NULL);
        return NULL;
      }
      high = lc_uri_hex_value(src[src_index + 1U]);
      low = lc_uri_hex_value(src[src_index + 2U]);
      if (high < 0 || low < 0) {
        lc_free_with_allocator(allocator, decoded);
        lc_error_set(error, LC_ERR_INVALID, 0L,
                     "invalid percent escape in pouch endpoint", component,
                     NULL, NULL);
        return NULL;
      }
      value = (unsigned char)((high << 4) | low);
      if (value == '\0') {
        lc_free_with_allocator(allocator, decoded);
        lc_error_set(error, LC_ERR_INVALID, 0L,
                     "pouch endpoint component must not contain NUL", component,
                     NULL, NULL);
        return NULL;
      }
      decoded[dst_index++] = (char)value;
      src_index += 3U;
    } else {
      decoded[dst_index++] = src[src_index++];
    }
  }
  decoded[dst_index] = '\0';
  return decoded;
}

static int lc_pouch_endpoint_parse_boolean(const lc_allocator *allocator,
                                           const char *value, size_t value_len,
                                           const char *option, int *out,
                                           lc_error *error) {
  char *copy;
  int result;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch endpoint boolean output is required", NULL,
                        NULL, "pouch");
  }
  copy = lc_pouch_endpoint_decode_component(allocator, value, value_len,
                                            option, error);
  if (copy == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  if (strcmp(copy, "true") == 0 || strcmp(copy, "1") == 0) {
    result = 1;
  } else if (strcmp(copy, "false") == 0 || strcmp(copy, "0") == 0) {
    result = 0;
  } else {
    lc_free_with_allocator(allocator, copy);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch endpoint boolean option must be true or false",
                        option, NULL, "pouch");
  }
  lc_free_with_allocator(allocator, copy);
  *out = result;
  return LC_OK;
}

static int lc_pouch_endpoint_parse_u64(const lc_allocator *allocator,
                                       const char *value, size_t value_len,
                                       const char *option, uint64_t *out,
                                       lc_error *error) {
  char *copy;
  char *end;
  uintmax_t parsed;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch endpoint integer output is required", NULL,
                        NULL, "pouch");
  }
  copy = lc_pouch_endpoint_decode_component(allocator, value, value_len,
                                            option, error);
  if (copy == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  errno = 0;
  parsed = strtoumax(copy, &end, 10);
  if (errno == ERANGE || end == copy || *end != '\0' ||
      parsed > (uintmax_t)UINT64_MAX) {
    lc_free_with_allocator(allocator, copy);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch endpoint option must be a u64", option, NULL,
                        "pouch");
  }
  lc_free_with_allocator(allocator, copy);
  *out = (uint64_t)parsed;
  return LC_OK;
}

static int lc_pouch_endpoint_parse_option(const lc_allocator *allocator,
                                          const char *key, size_t key_len,
                                          const char *value, size_t value_len,
                                          lc_pouch_endpoint_options *options,
                                          lc_error *error) {
  char *decoded_key;
  char *copy;
  int rc;

  if (key_len == 0U) {
    return LC_OK;
  }
  decoded_key = lc_pouch_endpoint_decode_component(allocator, key, key_len,
                                                   "query option", error);
  if (decoded_key == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key), "single_writer") ||
      lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "pouch_single_writer")) {
    copy = lc_pouch_endpoint_decode_component(allocator, value, value_len,
                                              "single_writer", error);
    if (copy == NULL) {
      lc_free_with_allocator(allocator, decoded_key);
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    if (strcmp(copy, "true") == 0 || strcmp(copy, "1") == 0) {
      options->single_writer = 1;
    } else if (strcmp(copy, "false") == 0 || strcmp(copy, "0") == 0) {
      options->single_writer = 0;
    } else {
      lc_free_with_allocator(allocator, copy);
      lc_free_with_allocator(allocator, decoded_key);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch endpoint single_writer must be true or false",
                          NULL, NULL, NULL);
    }
    lc_free_with_allocator(allocator, copy);
    lc_free_with_allocator(allocator, decoded_key);
    return LC_OK;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key), "queue_watch") ||
      lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "pouch_queue_watch")) {
    copy = lc_pouch_endpoint_decode_component(allocator, value, value_len,
                                              "queue_watch", error);
    if (copy == NULL) {
      lc_free_with_allocator(allocator, decoded_key);
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    if (strcmp(copy, "true") == 0 || strcmp(copy, "1") == 0) {
      options->queue_watch = 1;
    } else if (strcmp(copy, "false") == 0 || strcmp(copy, "0") == 0) {
      options->queue_watch = 0;
    } else {
      lc_free_with_allocator(allocator, copy);
      lc_free_with_allocator(allocator, decoded_key);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch endpoint queue_watch must be true or false",
                          NULL, NULL, "pouch");
    }
    lc_free_with_allocator(allocator, copy);
    lc_free_with_allocator(allocator, decoded_key);
    return LC_OK;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "fsync_batch_max_ops") ||
      lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "pouch_fsync_batch_max_ops")) {
    uintmax_t parsed;
    char *end;

    copy = lc_pouch_endpoint_decode_component(allocator, value, value_len,
                                              "fsync_batch_max_ops", error);
    if (copy == NULL) {
      lc_free_with_allocator(allocator, decoded_key);
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    errno = 0;
    parsed = strtoumax(copy, &end, 10);
    if (errno == ERANGE || end == copy || *end != '\0' ||
        parsed > (uintmax_t)UINT64_MAX) {
      lc_free_with_allocator(allocator, copy);
      lc_free_with_allocator(allocator, decoded_key);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch endpoint fsync_batch_max_ops must be a u64",
                          NULL, NULL, "pouch");
    }
    options->fsync_batch_max_ops = (uint64_t)parsed;
    lc_free_with_allocator(allocator, copy);
    lc_free_with_allocator(allocator, decoded_key);
    return LC_OK;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "background_compaction") ||
      lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "pouch_background_compaction")) {
    rc = lc_pouch_endpoint_parse_boolean(
        allocator, value, value_len, "background_compaction",
        &options->background_compaction_enabled, error);
    if (rc == LC_OK) {
      options->background_compaction_enabled_set = 1;
    }
    lc_free_with_allocator(allocator, decoded_key);
    return rc;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "disable_compaction_throttling") ||
      lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "pouch_disable_compaction_throttling")) {
    rc = lc_pouch_endpoint_parse_boolean(
        allocator, value, value_len, "disable_compaction_throttling",
        &options->compaction_throttling_disabled, error);
    lc_free_with_allocator(allocator, decoded_key);
    return rc;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "retention_seconds") ||
      lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "pouch_retention_seconds")) {
    rc = lc_pouch_endpoint_parse_u64(allocator, value, value_len,
                                     "retention_seconds",
                                     &options->retention_seconds, error);
    lc_free_with_allocator(allocator, decoded_key);
    return rc;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "janitor_interval_seconds") ||
      lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "pouch_janitor_interval_seconds")) {
    rc = lc_pouch_endpoint_parse_u64(allocator, value, value_len,
                                     "janitor_interval_seconds",
                                     &options->janitor_interval_seconds,
                                     error);
    lc_free_with_allocator(allocator, decoded_key);
    return rc;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key), "query_engine") ||
      lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "pouch_query_engine") ||
      lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "query_fallback_engine") ||
      lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "pouch_query_fallback_engine")) {
    int fallback;

    fallback = strstr(decoded_key, "fallback") != NULL ? 1 : 0;
    copy = lc_pouch_endpoint_decode_component(
        allocator, value, value_len,
        fallback ? "query_fallback_engine" : "query_engine", error);
    if (copy == NULL) {
      lc_free_with_allocator(allocator, decoded_key);
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    if (strcmp(copy, "index") != 0 && strcmp(copy, "scan") != 0) {
      lc_free_with_allocator(allocator, copy);
      lc_free_with_allocator(allocator, decoded_key);
      return lc_error_set(
          error, LC_ERR_INVALID, 0L,
          fallback
              ? "pouch endpoint query_fallback_engine must be index or scan"
              : "pouch endpoint query_engine must be index or scan",
          NULL, NULL, "pouch");
    }
    if (fallback) {
      lc_free_with_allocator(allocator, options->query_fallback_engine);
      options->query_fallback_engine = copy;
    } else {
      lc_free_with_allocator(allocator, options->query_engine);
      options->query_engine = copy;
    }
    lc_free_with_allocator(allocator, decoded_key);
    return LC_OK;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "pouch_crypto_key")) {
    copy = lc_pouch_endpoint_decode_component(allocator, value, value_len,
                                              "pouch_crypto_key", error);
    if (copy == NULL) {
      lc_free_with_allocator(allocator, decoded_key);
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    lc_secret_free_string_with_allocator(allocator, options->crypto_key);
    options->crypto_key = copy;
    lc_free_with_allocator(allocator, decoded_key);
    return LC_OK;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "pouch_crypto_key_file")) {
    copy = lc_pouch_endpoint_decode_component(allocator, value, value_len,
                                              "pouch_crypto_key_file", error);
    if (copy == NULL) {
      lc_free_with_allocator(allocator, decoded_key);
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    lc_free_with_allocator(allocator, options->crypto_key_file);
    options->crypto_key_file = copy;
    lc_free_with_allocator(allocator, decoded_key);
    return LC_OK;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "pouch_crypto_generate_key_file")) {
    copy = lc_pouch_endpoint_decode_component(
        allocator, value, value_len, "pouch_crypto_generate_key_file", error);
    if (copy == NULL) {
      lc_free_with_allocator(allocator, decoded_key);
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    if (strcmp(copy, "true") == 0 || strcmp(copy, "1") == 0) {
      options->crypto_generate_key_file = 1;
    } else if (strcmp(copy, "false") == 0 || strcmp(copy, "0") == 0) {
      options->crypto_generate_key_file = 0;
    } else {
      lc_free_with_allocator(allocator, copy);
      lc_free_with_allocator(allocator, decoded_key);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch endpoint crypto generate flag must be true "
                          "or false",
                          NULL, NULL, "pouch");
    }
    lc_free_with_allocator(allocator, copy);
    lc_free_with_allocator(allocator, decoded_key);
    return LC_OK;
  }
  if (lc_query_part_equal(decoded_key, strlen(decoded_key), "compression") ||
      lc_query_part_equal(decoded_key, strlen(decoded_key),
                          "pouch_compression")) {
    copy = lc_pouch_endpoint_decode_component(allocator, value, value_len,
                                              "pouch_compression", error);
    if (copy == NULL) {
      lc_free_with_allocator(allocator, decoded_key);
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    if (strcmp(copy, "none") != 0 && strcmp(copy, "zlib") != 0) {
      lc_free_with_allocator(allocator, copy);
      lc_free_with_allocator(allocator, decoded_key);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch endpoint compression must be none or zlib",
                          NULL, NULL, "pouch");
    }
    lc_free_with_allocator(allocator, options->compression);
    options->compression = copy;
    lc_free_with_allocator(allocator, decoded_key);
    return LC_OK;
  }
  lc_free_with_allocator(allocator, decoded_key);
  return lc_error_set(error, LC_ERR_INVALID, 0L,
                      "unsupported pouch endpoint query option", NULL, NULL,
                      NULL);
}

static int lc_pouch_endpoint_options_parse(const lc_allocator *allocator,
                                           const char *endpoint,
                                           lc_pouch_endpoint_options *options,
                                           lc_error *error) {
  const char *path;
  const char *query;
  const char *cursor;
  size_t path_len;
  int rc;

  memset(options, 0, sizeof(*options));
  path = lc_pouch_endpoint_path(endpoint);
  query = path != NULL ? strchr(path, '?') : NULL;
  path_len = query != NULL ? (size_t)(query - path) : strlen(path);
  options->root_path = lc_pouch_endpoint_decode_component(
      allocator, path, path_len, "path", error);
  if (options->root_path == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  if (options->root_path[0] != '/') {
    lc_pouch_endpoint_options_cleanup(allocator, options);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch endpoint path must be absolute", NULL, NULL,
                        NULL);
  }
  if (query == NULL) {
    return LC_OK;
  }

  cursor = query + 1;
  while (*cursor != '\0') {
    const char *part;
    const char *equals;
    const char *next;
    size_t key_len;
    size_t value_len;

    part = cursor;
    next = strchr(part, '&');
    if (next == NULL) {
      next = part + strlen(part);
    }
    equals = part;
    while (equals < next && *equals != '=') {
      ++equals;
    }
    key_len = (size_t)(equals - part);
    value_len = equals < next ? (size_t)(next - equals - 1) : 0U;
    rc = lc_pouch_endpoint_parse_option(allocator, part, key_len,
                                        equals < next ? equals + 1 : next,
                                        value_len, options, error);
    if (rc != LC_OK) {
      lc_pouch_endpoint_options_cleanup(allocator, options);
      return rc;
    }
    cursor = *next == '&' ? next + 1 : next;
  }
  return LC_OK;
}

static int lc_pouch_endpoint_redacted_copy(const lc_allocator *allocator,
                                           const char *endpoint, char **out,
                                           lc_error *error) {
  const char *path;
  const char *query;
  const char *cursor;
  char *copy;
  size_t prefix_len;
  size_t dst;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch endpoint redaction requires out", NULL, NULL,
                        NULL);
  }
  *out = NULL;
  if (!lc_endpoint_is_pouch(endpoint)) {
    *out = lc_strdup_with_allocator(allocator, endpoint);
    if (*out == NULL && endpoint != NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to copy client endpoint", NULL, NULL, NULL);
    }
    return LC_OK;
  }

  path = lc_pouch_endpoint_path(endpoint);
  query = path != NULL ? strchr(path, '?') : NULL;
  if (query == NULL) {
    *out = lc_strdup_with_allocator(allocator, endpoint);
    if (*out == NULL && endpoint != NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to copy client endpoint", NULL, NULL, NULL);
    }
    return LC_OK;
  }

  copy = (char *)lc_alloc_with_allocator(allocator, strlen(endpoint) + 1U);
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy client endpoint", NULL, NULL, NULL);
  }
  prefix_len = (size_t)(query - endpoint);
  memcpy(copy, endpoint, prefix_len);
  dst = prefix_len;
  cursor = query + 1;
  while (*cursor != '\0') {
    const char *part;
    const char *equals;
    const char *next;
    char *decoded_key;
    size_t key_len;
    size_t part_len;
    int is_secret;

    part = cursor;
    next = strchr(part, '&');
    if (next == NULL) {
      next = part + strlen(part);
    }
    equals = part;
    while (equals < next && *equals != '=') {
      ++equals;
    }
    key_len = (size_t)(equals - part);
    part_len = (size_t)(next - part);
    decoded_key = lc_pouch_endpoint_decode_component(allocator, part, key_len,
                                                     "query option", error);
    if (decoded_key == NULL) {
      lc_free_with_allocator(allocator, copy);
      return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
    }
    is_secret = strcmp(decoded_key, "pouch_crypto_key") == 0 ? 1 : 0;
    lc_free_with_allocator(allocator, decoded_key);
    if (!is_secret) {
      copy[dst] = dst == prefix_len ? '?' : '&';
      ++dst;
      if (part_len > 0U) {
        memcpy(copy + dst, part, part_len);
        dst += part_len;
      }
    }
    cursor = *next == '&' ? next + 1 : next;
  }
  copy[dst] = '\0';
  *out = copy;
  return LC_OK;
}

void lc_client_config_init(lc_client_config *config) {
  if (config == NULL) {
    return;
  }
  memset(config, 0, sizeof(*config));
  config->timeout_ms = 30000L;
  config->prefer_http_2 = 1;
  config->http_json_response_limit_bytes = 0U;
}

#define LC_INIT_STRUCT_FUNC(type_name, func_name)                              \
  void func_name(type_name *value) {                                           \
    if (value != NULL) {                                                       \
      memset(value, 0, sizeof(*value));                                        \
    }                                                                          \
  }

LC_INIT_STRUCT_FUNC(lc_lease_ref, lc_lease_ref_init)
LC_INIT_STRUCT_FUNC(lc_acquire_req, lc_acquire_req_init)
LC_INIT_STRUCT_FUNC(lc_describe_req, lc_describe_req_init)
LC_INIT_STRUCT_FUNC(lc_get_opts, lc_get_opts_init)
LC_INIT_STRUCT_FUNC(lc_update_opts, lc_update_opts_init)
LC_INIT_STRUCT_FUNC(lc_update_req, lc_update_req_init)
LC_INIT_STRUCT_FUNC(lc_mutate_req, lc_mutate_req_init)
LC_INIT_STRUCT_FUNC(lc_mutate_local_req, lc_mutate_local_req_init)
LC_INIT_STRUCT_FUNC(lc_mutate_op, lc_mutate_op_init)
LC_INIT_STRUCT_FUNC(lc_metadata_req, lc_metadata_req_init)
LC_INIT_STRUCT_FUNC(lc_metadata_op, lc_metadata_op_init)
LC_INIT_STRUCT_FUNC(lc_remove_req, lc_remove_req_init)
LC_INIT_STRUCT_FUNC(lc_remove_op, lc_remove_op_init)
LC_INIT_STRUCT_FUNC(lc_keepalive_req, lc_keepalive_req_init)
LC_INIT_STRUCT_FUNC(lc_keepalive_op, lc_keepalive_op_init)
LC_INIT_STRUCT_FUNC(lc_release_req, lc_release_req_init)
LC_INIT_STRUCT_FUNC(lc_release_op, lc_release_op_init)
LC_INIT_STRUCT_FUNC(lc_query_req, lc_query_req_init)
LC_INIT_STRUCT_FUNC(lc_namespace_config_req, lc_namespace_config_req_init)
LC_INIT_STRUCT_FUNC(lc_index_flush_req, lc_index_flush_req_init)
LC_INIT_STRUCT_FUNC(lc_txn_replay_req, lc_txn_replay_req_init)
LC_INIT_STRUCT_FUNC(lc_txn_decision_req, lc_txn_decision_req_init)
LC_INIT_STRUCT_FUNC(lc_tc_lease_acquire_req, lc_tc_lease_acquire_req_init)
LC_INIT_STRUCT_FUNC(lc_tc_lease_renew_req, lc_tc_lease_renew_req_init)
LC_INIT_STRUCT_FUNC(lc_tc_lease_release_req, lc_tc_lease_release_req_init)
LC_INIT_STRUCT_FUNC(lc_tc_cluster_announce_req, lc_tc_cluster_announce_req_init)
LC_INIT_STRUCT_FUNC(lc_tc_rm_register_req, lc_tc_rm_register_req_init)
LC_INIT_STRUCT_FUNC(lc_tc_rm_unregister_req, lc_tc_rm_unregister_req_init)
LC_INIT_STRUCT_FUNC(lc_enqueue_req, lc_enqueue_req_init)
LC_INIT_STRUCT_FUNC(lc_dequeue_req, lc_dequeue_req_init)
LC_INIT_STRUCT_FUNC(lc_queue_stats_req, lc_queue_stats_req_init)
LC_INIT_STRUCT_FUNC(lc_message_ref, lc_message_ref_init)
LC_INIT_STRUCT_FUNC(lc_nack_req, lc_nack_req_init)
LC_INIT_STRUCT_FUNC(lc_nack_op, lc_nack_op_init)
LC_INIT_STRUCT_FUNC(lc_extend_req, lc_extend_req_init)
LC_INIT_STRUCT_FUNC(lc_extend_op, lc_extend_op_init)
LC_INIT_STRUCT_FUNC(lc_watch_queue_req, lc_watch_queue_req_init)
LC_INIT_STRUCT_FUNC(lc_watch_handler, lc_watch_handler_init)
LC_INIT_STRUCT_FUNC(lc_consumer, lc_consumer_init)
LC_INIT_STRUCT_FUNC(lc_consumer_service_config, lc_consumer_service_config_init)
LC_INIT_STRUCT_FUNC(lc_attachment_selector, lc_attachment_selector_init)
LC_INIT_STRUCT_FUNC(lc_attach_req, lc_attach_req_init)
LC_INIT_STRUCT_FUNC(lc_attach_op, lc_attach_op_init)
LC_INIT_STRUCT_FUNC(lc_attachment_get_req, lc_attachment_get_req_init)
LC_INIT_STRUCT_FUNC(lc_attachment_list_req, lc_attachment_list_req_init)
LC_INIT_STRUCT_FUNC(lc_attachment_get_op, lc_attachment_get_op_init)
LC_INIT_STRUCT_FUNC(lc_attachment_delete_op, lc_attachment_delete_op_init)
LC_INIT_STRUCT_FUNC(lc_attachment_delete_all_op,
                    lc_attachment_delete_all_op_init)

#undef LC_INIT_STRUCT_FUNC

const char *lc_nack_intent_to_string(lc_nack_intent intent) {
  switch (intent) {
  case LC_NACK_INTENT_UNSPECIFIED:
    return "unspecified";
  case LC_NACK_INTENT_FAILURE:
    return "failure";
  case LC_NACK_INTENT_DEFER:
    return "defer";
  default:
    return "invalid";
  }
}

int lc_nack_intent_to_wire_string(lc_nack_intent intent, const char **out,
                                  lc_error *error) {
  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "nack intent mapping requires output", NULL, NULL,
                        NULL);
  }
  switch (intent) {
  case LC_NACK_INTENT_UNSPECIFIED:
  case LC_NACK_INTENT_FAILURE:
    *out = "failure";
    return LC_OK;
  case LC_NACK_INTENT_DEFER:
    *out = "defer";
    return LC_OK;
  default:
    *out = NULL;
    return lc_error_set(error, LC_ERR_INVALID, 0L, "invalid nack intent", NULL,
                        NULL, NULL);
  }
}

void lc_consumer_restart_policy_init(lc_consumer_restart_policy *policy) {
  if (policy == NULL) {
    return;
  }
  memset(policy, 0, sizeof(*policy));
  policy->immediate_retries = 3;
  policy->base_delay_ms = 250L;
  policy->max_delay_ms = 300000L;
  policy->multiplier = 2.0;
}

void lc_consumer_config_init(lc_consumer_config *config) {
  if (config == NULL) {
    return;
  }
  memset(config, 0, sizeof(*config));
  config->worker_count = 1U;
  lc_consumer_restart_policy_init(&config->restart_policy);
}

int lc_client_open(const lc_client_config *config, lc_client **out,
                   lc_error *error) {
  lc_engine_client_config engine_config;
  lc_engine_error engine_error;
  lc_bundle_capture_source bundle_capture;
  lc_pouch_endpoint_options pouch_endpoint_options;
  lc_pouch_open_options pouch_open_options;
  unsigned char *bundle_bytes;
  size_t bundle_length;
  lc_client_handle *client;
  size_t i;
  size_t pouch_endpoint_count;
  int is_pouch;
  int rc;

  if (config == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_client_open requires config and out", NULL, NULL,
                        NULL);
  }
  lc_engine_error_init(&engine_error);
  memset(&bundle_capture, 0, sizeof(bundle_capture));
  memset(&pouch_endpoint_options, 0, sizeof(pouch_endpoint_options));
  bundle_bytes = NULL;
  bundle_length = 0U;
  pouch_endpoint_count = 0U;
  if (config->endpoints != NULL) {
    for (i = 0U; i < config->endpoint_count; ++i) {
      if (lc_endpoint_is_pouch(config->endpoints[i])) {
        ++pouch_endpoint_count;
      }
    }
  }
  if (pouch_endpoint_count != 0U &&
      (config->endpoint_count != 1U || config->unix_socket_path != NULL)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch endpoints must be configured alone", NULL, NULL,
                        NULL);
  }
  is_pouch = pouch_endpoint_count == 1U;
  if (!config->disable_mtls && config->client_bundle_source != NULL) {
    bundle_capture.inner = config->client_bundle_source;
    bundle_capture.allocator = &config->allocator;
    bundle_capture.pub.read = lc_bundle_capture_source_read;
    bundle_capture.pub.reset = lc_bundle_capture_source_reset;
    bundle_capture.pub.close = lc_bundle_capture_source_close;
    bundle_capture.pub.impl = &bundle_capture;
  }

  memset(&engine_config, 0, sizeof(engine_config));
  engine_config.endpoints = config->endpoints;
  engine_config.endpoint_count = config->endpoint_count;
  engine_config.unix_socket_path = config->unix_socket_path;
  engine_config.client_bundle_source =
      bundle_capture.inner != NULL ? &bundle_capture.pub : NULL;
  engine_config.client_bundle_path =
      bundle_capture.inner == NULL ? config->client_bundle_path : NULL;
  engine_config.default_namespace = config->default_namespace;
  engine_config.timeout_ms = config->timeout_ms;
  engine_config.disable_mtls = is_pouch ? 1 : config->disable_mtls;
  engine_config.insecure_skip_verify = config->insecure_skip_verify;
  engine_config.prefer_http_2 = config->prefer_http_2;
  engine_config.http_json_response_limit_bytes =
      config->http_json_response_limit_bytes;
  engine_config.logger = config->logger;
  engine_config.disable_logger_sys_field = config->disable_logger_sys_field;
  engine_config.allocator.malloc_fn = config->allocator.malloc_fn;
  engine_config.allocator.realloc_fn = config->allocator.realloc_fn;
  engine_config.allocator.free_fn = config->allocator.free_fn;
  engine_config.allocator.context = config->allocator.context;

  client = (lc_client_handle *)lc_calloc_with_allocator(&config->allocator, 1U,
                                                        sizeof(*client));
  if (client == NULL) {
    lc_free_with_allocator(&config->allocator, bundle_capture.bytes);
    lc_engine_error_cleanup(&engine_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to allocate client",
                        NULL, NULL, NULL);
  }
  client->allocator = config->allocator;
  client->is_pouch = is_pouch;
  client->disable_logger_sys_field = config->disable_logger_sys_field;
  client->base_logger =
      config->logger != NULL ? config->logger : lc_log_noop_logger();
  if (is_pouch) {
    client->logger = lc_log_client_logger(client->base_logger,
                                          client->disable_logger_sys_field);
    if (client->logger == NULL) {
      lc_free_with_allocator(&config->allocator, bundle_capture.bytes);
      lc_engine_error_cleanup(&engine_error);
      lc_free_with_allocator(&config->allocator, client);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to initialize client logger", NULL, NULL,
                          NULL);
    }
    client->owns_logger = (client->logger != client->base_logger &&
                           client->logger != lc_log_noop_logger())
                              ? 1
                              : 0;
  }
  if (!is_pouch) {
    rc = lc_engine_client_open(&engine_config, &client->engine, &engine_error);
    if (rc != LC_ENGINE_OK) {
      int public_rc;

      public_rc = lc_error_from_engine(error, &engine_error);
      lc_engine_error_cleanup(&engine_error);
      lc_free_with_allocator(&config->allocator, bundle_capture.bytes);
      lc_free_with_allocator(&config->allocator, client);
      return public_rc;
    }
  }
  if (client->is_pouch) {
    rc = lc_pouch_endpoint_options_parse(&config->allocator,
                                         config->endpoints[0],
                                         &pouch_endpoint_options, error);
    if (rc != LC_OK) {
      lc_client_close_method(&client->pub);
      lc_engine_error_cleanup(&engine_error);
      lc_free_with_allocator(&config->allocator, bundle_capture.bytes);
      return rc;
    }
    memset(&pouch_open_options, 0, sizeof(pouch_open_options));
    pouch_open_options.single_writer = pouch_endpoint_options.single_writer;
    pouch_open_options.fsync_batch_max_ops =
        pouch_endpoint_options.fsync_batch_max_ops;
    pouch_open_options.queue_watch = pouch_endpoint_options.queue_watch;
    pouch_open_options.background_compaction_enabled =
        pouch_endpoint_options.background_compaction_enabled;
    pouch_open_options.background_compaction_enabled_set =
        pouch_endpoint_options.background_compaction_enabled_set;
    pouch_open_options.compaction_throttling_disabled =
        pouch_endpoint_options.compaction_throttling_disabled;
    pouch_open_options.retention_seconds = pouch_endpoint_options.retention_seconds;
    pouch_open_options.janitor_interval_seconds =
        pouch_endpoint_options.janitor_interval_seconds;
    pouch_open_options.query_engine = pouch_endpoint_options.query_engine;
    pouch_open_options.query_fallback_engine =
        pouch_endpoint_options.query_fallback_engine;
    pouch_open_options.crypto_key = config->pouch_crypto_key != NULL
                                        ? config->pouch_crypto_key
                                        : pouch_endpoint_options.crypto_key;
    pouch_open_options.crypto_key_file =
        config->pouch_crypto_key_file != NULL
            ? config->pouch_crypto_key_file
            : pouch_endpoint_options.crypto_key_file;
    pouch_open_options.crypto_generate_key_file =
        config->pouch_crypto_generate_key_file != 0
            ? config->pouch_crypto_generate_key_file
            : pouch_endpoint_options.crypto_generate_key_file;
    pouch_open_options.compression = config->pouch_compression != NULL
                                         ? config->pouch_compression
                                         : pouch_endpoint_options.compression;
    pouch_open_options.logger = client->base_logger;
    rc = lc_pouch_open(pouch_endpoint_options.root_path, &config->allocator,
                       &pouch_open_options, &client->pouch, error);
    if (rc != LC_OK) {
      lc_client_close_method(&client->pub);
      lc_engine_error_cleanup(&engine_error);
      lc_free_with_allocator(&config->allocator, bundle_capture.bytes);
      lc_pouch_endpoint_options_cleanup(&config->allocator,
                                        &pouch_endpoint_options);
      return rc;
    }
    client->pouch_crypto_key = lc_strdup_with_allocator(
        &config->allocator, pouch_open_options.crypto_key);
    if (pouch_open_options.crypto_key != NULL &&
        client->pouch_crypto_key == NULL) {
      lc_client_close_method(&client->pub);
      lc_engine_error_cleanup(&engine_error);
      lc_free_with_allocator(&config->allocator, bundle_capture.bytes);
      lc_pouch_endpoint_options_cleanup(&config->allocator,
                                        &pouch_endpoint_options);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to copy pouch crypto key", NULL, NULL, NULL);
    }
    client->pouch_crypto_key_file = lc_strdup_with_allocator(
        &config->allocator, pouch_open_options.crypto_key_file);
    if (pouch_open_options.crypto_key_file != NULL &&
        client->pouch_crypto_key_file == NULL) {
      lc_client_close_method(&client->pub);
      lc_engine_error_cleanup(&engine_error);
      lc_free_with_allocator(&config->allocator, bundle_capture.bytes);
      lc_pouch_endpoint_options_cleanup(&config->allocator,
                                        &pouch_endpoint_options);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to copy pouch crypto key file", NULL, NULL,
                          NULL);
    }
    client->pouch_crypto_generate_key_file =
        pouch_open_options.crypto_generate_key_file;
    lc_pouch_endpoint_options_cleanup(&config->allocator,
                                      &pouch_endpoint_options);
  }
  bundle_bytes = bundle_capture.bytes;
  bundle_length = bundle_capture.length;
  bundle_capture.bytes = NULL;
  client->client_bundle_bytes = bundle_bytes;
  client->client_bundle_length = bundle_length;
  client->endpoint_count = config->endpoint_count;
  if (config->endpoint_count != 0U) {
    client->endpoints = (char **)lc_calloc_with_allocator(
        &config->allocator, config->endpoint_count, sizeof(char *));
    if (client->endpoints == NULL) {
      lc_client_close_method(&client->pub);
      lc_engine_error_cleanup(&engine_error);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate client endpoint copy", NULL, NULL,
                          NULL);
    }
    for (i = 0U; i < config->endpoint_count; ++i) {
      rc = lc_pouch_endpoint_redacted_copy(&config->allocator,
                                           config->endpoints[i],
                                           &client->endpoints[i], error);
      if (rc != LC_OK) {
        lc_client_close_method(&client->pub);
        lc_engine_error_cleanup(&engine_error);
        return rc;
      }
    }
  }
  client->unix_socket_path =
      lc_strdup_with_allocator(&config->allocator, config->unix_socket_path);
  if (config->unix_socket_path != NULL && client->unix_socket_path == NULL) {
    lc_client_close_method(&client->pub);
    lc_engine_error_cleanup(&engine_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy unix_socket_path", NULL, NULL, NULL);
  }
  client->client_bundle_path =
      lc_strdup_with_allocator(&config->allocator, config->client_bundle_path);
  if (config->client_bundle_path != NULL &&
      client->client_bundle_path == NULL) {
    lc_client_close_method(&client->pub);
    lc_engine_error_cleanup(&engine_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy client bundle path", NULL, NULL, NULL);
  }
  client->default_namespace =
      lc_strdup_with_allocator(&config->allocator, config->default_namespace);
  if (config->default_namespace != NULL && client->default_namespace == NULL) {
    lc_client_close_method(&client->pub);
    lc_engine_error_cleanup(&engine_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy default namespace", NULL, NULL, NULL);
  }
  client->timeout_ms = config->timeout_ms;
  client->disable_mtls = is_pouch ? 1 : config->disable_mtls;
  client->insecure_skip_verify = config->insecure_skip_verify;
  client->prefer_http_2 = config->prefer_http_2;
  if (client->engine != NULL) {
    client->logger = lc_engine_client_logger(client->engine);
  }
  client->http_json_response_limit_bytes =
      config->http_json_response_limit_bytes;
  client->pub.acquire = lc_client_acquire_method;
  client->pub.acquire_for_update = lc_client_acquire_for_update_method;
  client->pub.describe = lc_client_describe_method;
  client->pub.get = lc_client_get_method;
  client->pub.load = lc_client_load_method;
  client->pub.update = lc_client_update_method;
  client->pub.mutate = lc_client_mutate_method;
  client->pub.metadata = lc_client_metadata_method;
  client->pub.remove = lc_client_remove_method;
  client->pub.keepalive = lc_client_keepalive_method;
  client->pub.release = lc_client_release_method;
  client->pub.attach = lc_client_attach_method;
  client->pub.list_attachments = lc_client_list_attachments_method;
  client->pub.get_attachment = lc_client_get_attachment_method;
  client->pub.delete_attachment = lc_client_delete_attachment_method;
  client->pub.delete_all_attachments = lc_client_delete_all_attachments_method;
  client->pub.queue_stats = lc_client_queue_stats_method;
  client->pub.queue_ack = lc_client_queue_ack_method;
  client->pub.queue_nack = lc_client_queue_nack_method;
  client->pub.queue_extend = lc_client_queue_extend_method;
  client->pub.query = lc_client_query_method;
  client->pub.query_keys = lc_client_query_keys_method;
  client->pub.get_namespace_config = lc_client_get_namespace_config_method;
  client->pub.update_namespace_config =
      lc_client_update_namespace_config_method;
  client->pub.flush_index = lc_client_flush_index_method;
  client->pub.txn_replay = lc_client_txn_replay_method;
  client->pub.txn_prepare = lc_client_txn_prepare_method;
  client->pub.txn_commit = lc_client_txn_commit_method;
  client->pub.txn_rollback = lc_client_txn_rollback_method;
  client->pub.tc_lease_acquire = lc_client_tc_lease_acquire_method;
  client->pub.tc_lease_renew = lc_client_tc_lease_renew_method;
  client->pub.tc_lease_release = lc_client_tc_lease_release_method;
  client->pub.tc_leader = lc_client_tc_leader_method;
  client->pub.tc_cluster_announce = lc_client_tc_cluster_announce_method;
  client->pub.tc_cluster_leave = lc_client_tc_cluster_leave_method;
  client->pub.tc_cluster_list = lc_client_tc_cluster_list_method;
  client->pub.tc_rm_register = lc_client_tc_rm_register_method;
  client->pub.tc_rm_unregister = lc_client_tc_rm_unregister_method;
  client->pub.tc_rm_list = lc_client_tc_rm_list_method;
  client->pub.enqueue = lc_client_enqueue_method;
  client->pub.dequeue = lc_client_dequeue_method;
  client->pub.dequeue_batch = lc_client_dequeue_batch_method;
  client->pub.dequeue_with_state = lc_client_dequeue_with_state_method;
  client->pub.subscribe = lc_client_subscribe_method;
  client->pub.subscribe_with_state = lc_client_subscribe_with_state_method;
  client->pub.new_consumer_service = lc_client_new_consumer_service_method;
  client->pub.watch_queue = lc_client_watch_queue_method;
  client->pub.close = lc_client_close_method;
  if (client->is_pouch) {
    client->pub.acquire = lc_pouch_client_acquire_method;
    client->pub.acquire_for_update = lc_pouch_client_acquire_for_update_method;
    client->pub.describe = lc_pouch_client_describe_method;
    client->pub.get = lc_pouch_client_get_method;
    client->pub.load = lc_pouch_client_load_method;
    client->pub.update = lc_pouch_client_update_method;
    client->pub.mutate = lc_pouch_client_mutate_method;
    client->pub.metadata = lc_pouch_client_metadata_method;
    client->pub.remove = lc_pouch_client_remove_method;
    client->pub.keepalive = lc_pouch_client_keepalive_method;
    client->pub.release = lc_pouch_client_release_method;
    client->pub.attach = lc_pouch_client_attach_method;
    client->pub.list_attachments = lc_pouch_client_list_attachments_method;
    client->pub.get_attachment = lc_pouch_client_get_attachment_method;
    client->pub.delete_attachment = lc_pouch_client_delete_attachment_method;
    client->pub.delete_all_attachments =
        lc_pouch_client_delete_all_attachments_method;
    client->pub.queue_stats = lc_pouch_client_queue_stats_method;
    client->pub.queue_ack = lc_pouch_client_queue_ack_method;
    client->pub.queue_nack = lc_pouch_client_queue_nack_method;
    client->pub.queue_extend = lc_pouch_client_queue_extend_method;
    client->pub.enqueue = lc_pouch_client_enqueue_method;
    client->pub.dequeue = lc_pouch_client_dequeue_method;
    client->pub.dequeue_batch = lc_pouch_client_dequeue_batch_method;
    client->pub.dequeue_with_state = lc_pouch_client_dequeue_with_state_method;
    client->pub.subscribe = lc_pouch_client_subscribe_method;
    client->pub.subscribe_with_state =
        lc_pouch_client_subscribe_with_state_method;
    client->pub.watch_queue = lc_pouch_client_watch_queue_method;
    client->pub.query = lc_pouch_client_query_method;
    client->pub.query_keys = lc_pouch_client_query_keys_method;
    client->pub.get_namespace_config =
        lc_pouch_client_get_namespace_config_method;
    client->pub.update_namespace_config =
        lc_pouch_client_update_namespace_config_method;
    client->pub.flush_index = lc_pouch_client_flush_index_method;
    client->pub.txn_replay = lc_pouch_client_txn_replay_method;
    client->pub.txn_prepare = lc_pouch_client_txn_prepare_method;
    client->pub.txn_commit = lc_pouch_client_txn_commit_method;
    client->pub.txn_rollback = lc_pouch_client_txn_rollback_method;
    client->pub.tc_lease_acquire = lc_pouch_client_tc_lease_acquire_method;
    client->pub.tc_lease_renew = lc_pouch_client_tc_lease_renew_method;
    client->pub.tc_lease_release = lc_pouch_client_tc_lease_release_method;
    client->pub.tc_leader = lc_pouch_client_tc_leader_method;
    client->pub.tc_cluster_announce =
        lc_pouch_client_tc_cluster_announce_method;
    client->pub.tc_cluster_leave = lc_pouch_client_tc_cluster_leave_method;
    client->pub.tc_cluster_list = lc_pouch_client_tc_cluster_list_method;
    client->pub.tc_rm_register = lc_pouch_client_tc_rm_register_method;
    client->pub.tc_rm_unregister = lc_pouch_client_tc_rm_unregister_method;
    client->pub.tc_rm_list = lc_pouch_client_tc_rm_list_method;
    rc = lc_pouch_client_recover_transactions(&client->pub, error);
    if (rc != LC_OK) {
      lc_client_close_method(&client->pub);
      lc_engine_error_cleanup(&engine_error);
      return rc;
    }
  }
  client->pub.default_namespace = client->default_namespace;
  *out = &client->pub;
  lc_engine_error_cleanup(&engine_error);
  return LC_OK;
}

int lc_source_from_memory(const void *bytes, size_t length, lc_source **out,
                          lc_error *error) {
  lc_memory_source *source;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "source_from_memory requires out", NULL, NULL, NULL);
  }
  source = (lc_memory_source *)calloc(1U, sizeof(*source));
  if (source == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate memory source", NULL, NULL, NULL);
  }
  source->base.pub.read = lc_source_pub_read;
  source->base.pub.reset = lc_source_pub_reset;
  source->base.pub.close = lc_source_pub_close;
  source->base.read_impl = lc_memory_source_read;
  source->base.reset_impl = lc_memory_source_reset;
  source->base.close_impl = lc_memory_source_close;
  if (length > 0U) {
    source->bytes = (unsigned char *)malloc(length);
    if (source->bytes == NULL) {
      free(source);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate memory source bytes", NULL, NULL,
                          NULL);
    }
    memcpy(source->bytes, bytes, length);
  }
  source->length = length;
  source->owns_bytes = 1;
  *out = &source->base.pub;
  return LC_OK;
}

int lc_source_from_file(const char *path, lc_source **out, lc_error *error) {
  lc_file_source *source;

  if (path == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "source_from_file requires path and out", NULL, NULL,
                        NULL);
  }
  source = (lc_file_source *)calloc(1U, sizeof(*source));
  if (source == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate file source", NULL, NULL, NULL);
  }
  source->fp = fopen(path, "rb");
  if (source->fp == NULL) {
    free(source);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open file source", NULL, NULL, NULL);
  }
  source->close_file = 1;
  source->base.pub.read = lc_source_pub_read;
  source->base.pub.reset = lc_source_pub_reset;
  source->base.pub.close = lc_source_pub_close;
  source->base.read_impl = lc_file_source_read;
  source->base.reset_impl = lc_file_source_reset;
  source->base.close_impl = lc_file_source_close;
  *out = &source->base.pub;
  return LC_OK;
}

int lc_source_from_fd(int fd, lc_source **out, lc_error *error) {
  lc_fd_source *source;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "source_from_fd requires out", NULL, NULL, NULL);
  }
  source = (lc_fd_source *)calloc(1U, sizeof(*source));
  if (source == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to allocate fd source",
                        NULL, NULL, NULL);
  }
  source->fd = fd;
  source->base.pub.read = lc_source_pub_read;
  source->base.pub.reset = lc_source_pub_reset;
  source->base.pub.close = lc_source_pub_close;
  source->base.read_impl = lc_fd_source_read;
  source->base.close_impl = lc_fd_source_close;
  *out = &source->base.pub;
  return LC_OK;
}

int lc_source_from_callbacks(lc_source_read_fn read, lc_source_reset_fn reset,
                             lc_source_close_fn close, void *context,
                             lc_source **out, lc_error *error) {
  lc_callback_source *source;

  if (read == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "source_from_callbacks requires read and out", NULL,
                        NULL, NULL);
  }
  source = (lc_callback_source *)calloc(1U, sizeof(*source));
  if (source == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate callback source", NULL, NULL, NULL);
  }
  source->base.pub.read = lc_source_pub_read;
  source->base.pub.reset = lc_source_pub_reset;
  source->base.pub.close = lc_source_pub_close;
  source->base.read_impl = lc_callback_source_read;
  source->base.reset_impl = lc_callback_source_reset;
  source->base.close_impl = lc_callback_source_close;
  source->read = read;
  source->reset = reset;
  source->close = close;
  source->context = context;
  *out = &source->base.pub;
  return LC_OK;
}

int lc_sink_to_file(const char *path, lc_sink **out, lc_error *error) {
  int fd;
  int rc;
  lc_fd_sink *sink;

  if (path == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, "sink_to_file requires path",
                        NULL, NULL, NULL);
  }
  fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L, "failed to open file sink",
                        NULL, NULL, NULL);
  }
  rc = lc_sink_to_fd(fd, out, error);
  if (rc != LC_OK) {
    close(fd);
    return rc;
  }
  sink = (lc_fd_sink *)*out;
  sink->close_fd = 1;
  return LC_OK;
}

int lc_sink_to_fd(int fd, lc_sink **out, lc_error *error) {
  lc_fd_sink *sink;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, "sink_to_fd requires out",
                        NULL, NULL, NULL);
  }
  sink = (lc_fd_sink *)calloc(1U, sizeof(*sink));
  if (sink == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to allocate fd sink",
                        NULL, NULL, NULL);
  }
  sink->fd = fd;
  sink->base.pub.write = lc_sink_pub_write;
  sink->base.pub.close = lc_sink_pub_close;
  sink->base.write_impl = lc_fd_sink_write;
  sink->base.close_impl = lc_fd_sink_close;
  *out = &sink->base.pub;
  return LC_OK;
}

int lc_sink_to_discard(lc_sink **out, lc_error *error) {
  lc_discard_sink *sink;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "sink_to_discard requires out", NULL, NULL, NULL);
  }
  sink = (lc_discard_sink *)calloc(1U, sizeof(*sink));
  if (sink == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate discard sink", NULL, NULL, NULL);
  }
  sink->base.pub.write = lc_sink_pub_write;
  sink->base.pub.close = lc_sink_pub_close;
  sink->base.pub.impl = &lc_discard_sink_marker;
  sink->base.write_impl = lc_discard_sink_write;
  sink->base.close_impl = lc_discard_sink_close;
  *out = &sink->base.pub;
  return LC_OK;
}

int lc_sink_is_discard(const lc_sink *sink) {
  if (sink == NULL) {
    return 0;
  }
  return sink->write == lc_sink_pub_write && sink->close == lc_sink_pub_close &&
         sink->impl == &lc_discard_sink_marker;
}

int lc_sink_to_memory(lc_sink **out, lc_error *error) {
  lc_memory_sink *sink;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "sink_to_memory requires out", NULL, NULL, NULL);
  }
  sink = (lc_memory_sink *)calloc(1U, sizeof(*sink));
  if (sink == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate memory sink", NULL, NULL, NULL);
  }
  sink->base.pub.write = lc_sink_pub_write;
  sink->base.pub.close = lc_sink_pub_close;
  sink->base.write_impl = lc_memory_sink_write;
  sink->base.close_impl = lc_memory_sink_close;
  *out = &sink->base.pub;
  return LC_OK;
}

int lc_sink_memory_bytes(lc_sink *sink, const void **bytes, size_t *length,
                         lc_error *error) {
  lc_memory_sink *memory_sink;

  (void)error;
  if (sink == NULL || bytes == NULL || length == NULL) {
    return LC_ERR_INVALID;
  }
  memory_sink = (lc_memory_sink *)sink;
  *bytes = memory_sink->bytes;
  *length = memory_sink->length;
  return LC_OK;
}

int lc_sink_memory_reserve(lc_sink *sink, size_t capacity, lc_error *error) {
  lc_sink_impl *impl;
  lc_memory_sink *memory_sink;
  unsigned char *next;

  if (sink == NULL) {
    return LC_OK;
  }
  if (sink->write != lc_sink_pub_write || sink->close != lc_sink_pub_close) {
    return LC_OK;
  }
  impl = (lc_sink_impl *)sink;
  if (impl->write_impl != lc_memory_sink_write) {
    return LC_OK;
  }
  memory_sink = (lc_memory_sink *)sink;
  if (capacity <= memory_sink->capacity) {
    return LC_OK;
  }
  next = (unsigned char *)realloc(memory_sink->bytes, capacity);
  if (next == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to grow memory sink",
                        NULL, NULL, NULL);
  }
  memory_sink->bytes = next;
  memory_sink->capacity = capacity;
  return LC_OK;
}

int lc_copy(lc_source *src, lc_sink *dst, size_t *written, lc_error *error) {
  unsigned char buffer[65536];
  size_t total;
  size_t nread;
  int rc;

  if (src == NULL || dst == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_copy requires src and dst", NULL, NULL, NULL);
  }
  total = 0U;
  for (;;) {
    nread = src->read(src, buffer, sizeof(buffer), error);
    if (nread == 0U) {
      if (written != NULL) {
        *written = total;
      }
      return error != NULL && error->code != LC_OK ? error->code : LC_OK;
    }
    rc = dst->write(dst, buffer, nread, error);
    if (!rc) {
      return error != NULL && error->code != LC_OK ? error->code
                                                   : LC_ERR_TRANSPORT;
    }
    total += nread;
  }
}

void lc_describe_res_cleanup(lc_describe_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->namespace_name);
  free(response->key);
  free(response->owner);
  free(response->lease_id);
  free(response->txn_id);
  free(response->state_etag);
  free(response->public_state_etag);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_get_res_cleanup(lc_get_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->content_type);
  free(response->etag);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_update_res_cleanup(lc_update_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->new_state_etag);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_mutate_res_cleanup(lc_mutate_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->new_state_etag);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_metadata_res_cleanup(lc_metadata_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->namespace_name);
  free(response->key);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_remove_res_cleanup(lc_remove_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_keepalive_res_cleanup(lc_keepalive_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->state_etag);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_release_res_cleanup(lc_release_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_query_res_cleanup(lc_query_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->cursor);
  free(response->return_mode);
  free(response->metadata_json);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_string_list_cleanup(lc_string_list *response) {
  size_t index;

  if (response == NULL) {
    return;
  }
  for (index = 0U; index < response->count; ++index) {
    free(response->items[index]);
  }
  free(response->items);
  memset(response, 0, sizeof(*response));
}

void lc_namespace_config_res_cleanup(lc_namespace_config_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->namespace_name);
  free(response->preferred_engine);
  free(response->fallback_engine);
  free(response->etag);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_index_flush_res_cleanup(lc_index_flush_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->namespace_name);
  free(response->mode);
  free(response->flush_id);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_txn_replay_res_cleanup(lc_txn_replay_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->txn_id);
  free(response->state);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_txn_decision_res_cleanup(lc_txn_decision_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->txn_id);
  free(response->state);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_tc_lease_acquire_res_cleanup(lc_tc_lease_acquire_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->leader_id);
  free(response->leader_endpoint);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_tc_lease_renew_res_cleanup(lc_tc_lease_renew_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->leader_id);
  free(response->leader_endpoint);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_tc_lease_release_res_cleanup(lc_tc_lease_release_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_tc_leader_res_cleanup(lc_tc_leader_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->leader_id);
  free(response->leader_endpoint);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_tc_cluster_res_cleanup(lc_tc_cluster_res *response) {
  if (response == NULL) {
    return;
  }
  lc_string_list_cleanup(&response->endpoints);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_tc_rm_res_cleanup(lc_tc_rm_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->backend_hash);
  lc_string_list_cleanup(&response->endpoints);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_tc_rm_list_res_cleanup(lc_tc_rm_list_res *response) {
  size_t index;

  if (response == NULL) {
    return;
  }
  for (index = 0U; index < response->backend_count; ++index) {
    free(response->backends[index].backend_hash);
    lc_string_list_cleanup(&response->backends[index].endpoints);
  }
  free(response->backends);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_enqueue_res_cleanup(lc_enqueue_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->namespace_name);
  free(response->queue);
  free(response->message_id);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_queue_stats_res_cleanup(lc_queue_stats_res *response) {
  if (response == NULL) {
    return;
  }
  free(response->namespace_name);
  free(response->queue);
  free(response->head_message_id);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_ack_res_cleanup(lc_ack_res *response) {
  if (response != NULL) {
    free(response->correlation_id);
    memset(response, 0, sizeof(*response));
  }
}

void lc_nack_res_cleanup(lc_nack_res *response) {
  if (response != NULL) {
    free(response->meta_etag);
    free(response->correlation_id);
    memset(response, 0, sizeof(*response));
  }
}

void lc_extend_res_cleanup(lc_extend_res *response) {
  if (response != NULL) {
    free(response->meta_etag);
    free(response->correlation_id);
    memset(response, 0, sizeof(*response));
  }
}

void lc_dequeue_batch_cleanup(lc_dequeue_batch_res *response) {
  size_t index;

  if (response == NULL) {
    return;
  }
  for (index = 0U; index < response->count; ++index) {
    if (response->messages[index] != NULL) {
      response->messages[index]->close(response->messages[index]);
    }
  }
  free(response->messages);
  memset(response, 0, sizeof(*response));
}

void lc_watch_event_cleanup(lc_watch_event *event) {
  if (event == NULL) {
    return;
  }
  free(event->namespace_name);
  free(event->queue);
  free(event->head_message_id);
  free(event->correlation_id);
  memset(event, 0, sizeof(*event));
}

void lc_attachment_info_cleanup(lc_attachment_info *info) {
  if (info == NULL) {
    return;
  }
  free(info->id);
  free(info->name);
  free(info->plaintext_sha256);
  free(info->content_type);
  memset(info, 0, sizeof(*info));
}

void lc_attach_res_cleanup(lc_attach_res *response) {
  if (response == NULL) {
    return;
  }
  lc_attachment_info_cleanup(&response->attachment);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_attachment_list_cleanup(lc_attachment_list *response) {
  size_t index;

  if (response == NULL) {
    return;
  }
  for (index = 0U; index < response->count; ++index) {
    lc_attachment_info_cleanup(&response->items[index]);
  }
  free(response->items);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

void lc_attachment_get_res_cleanup(lc_attachment_get_res *response) {
  if (response == NULL) {
    return;
  }
  lc_attachment_info_cleanup(&response->attachment);
  free(response->correlation_id);
  memset(response, 0, sizeof(*response));
}

lc_source *lc_source_from_open_file(FILE *fp, int close_file) {
  lc_file_source *source;

  if (fp == NULL) {
    return NULL;
  }
  source = (lc_file_source *)calloc(1U, sizeof(*source));
  if (source == NULL) {
    if (close_file) {
      fclose(fp);
    }
    return NULL;
  }
  source->fp = fp;
  source->close_file = close_file;
  source->base.pub.read = lc_source_pub_read;
  source->base.pub.reset = lc_source_pub_reset;
  source->base.pub.close = lc_source_pub_close;
  source->base.read_impl = lc_file_source_read;
  source->base.reset_impl = lc_file_source_reset;
  source->base.close_impl = lc_file_source_close;
  return &source->base.pub;
}

int lc_stream_pipe_open(size_t capacity, const lc_allocator *allocator,
                        lc_source **out, lc_stream_pipe **pipe,
                        lc_error *error) {
  lc_stream_pipe *state;
  lc_stream_source *source;

  if (out == NULL || pipe == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "stream pipe requires out and pipe", NULL, NULL, NULL);
  }
  if (capacity == 0U) {
    capacity = 65536U;
  }
  state =
      (lc_stream_pipe *)lc_calloc_with_allocator(allocator, 1U, sizeof(*state));
  source = NULL;
  if (state == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate stream pipe", NULL, NULL, NULL);
  }
  memset(&state->allocator, 0, sizeof(state->allocator));
  if (allocator != NULL) {
    state->allocator = *allocator;
  }
  state->buffer = (unsigned char *)lc_alloc_with_allocator(allocator, capacity);
  if (state->buffer == NULL) {
    lc_free_with_allocator(allocator, state);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate stream buffer", NULL, NULL, NULL);
  }
  state->capacity = capacity;
  state->reader_ref = 1;
  state->writer_ref = 1;
  pthread_mutex_init(&state->mutex, NULL);
  pthread_cond_init(&state->cond, NULL);
  source = (lc_stream_source *)lc_calloc_with_allocator(allocator, 1U,
                                                        sizeof(*source));
  if (source == NULL) {
    pthread_cond_destroy(&state->cond);
    pthread_mutex_destroy(&state->mutex);
    lc_free_with_allocator(allocator, state->buffer);
    lc_free_with_allocator(allocator, state);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate stream source", NULL, NULL, NULL);
  }
  source->allocator = state->allocator;
  source->pipe = state;
  source->base.pub.read = lc_source_pub_read;
  source->base.pub.reset = lc_source_pub_reset;
  source->base.pub.close = lc_source_pub_close;
  source->base.read_impl = lc_stream_source_read;
  source->base.reset_impl = lc_stream_source_reset;
  source->base.close_impl = lc_stream_source_close;
  *out = &source->base.pub;
  *pipe = state;
  return LC_OK;
}

int lc_stream_pipe_write(lc_stream_pipe *pipe, const void *bytes, size_t count,
                         lc_error *error) {
  const unsigned char *src;
  size_t chunk;
  size_t first;
  size_t second;

  if (pipe == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "stream pipe write requires pipe", NULL, NULL, NULL);
  }
  src = (const unsigned char *)bytes;
  pthread_mutex_lock(&pipe->mutex);
  while (count > 0U) {
    while (pipe->used == pipe->capacity && !pipe->reader_closed) {
      pthread_cond_wait(&pipe->cond, &pipe->mutex);
    }
    if (pipe->reader_closed) {
      pthread_mutex_unlock(&pipe->mutex);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "stream consumer closed payload early", NULL, NULL,
                          NULL);
    }
    chunk = pipe->capacity - pipe->used;
    if (chunk > count) {
      chunk = count;
    }
    first = chunk;
    if (pipe->write_pos + first > pipe->capacity) {
      first = pipe->capacity - pipe->write_pos;
    }
    memcpy(pipe->buffer + pipe->write_pos, src, first);
    second = chunk - first;
    if (second > 0U) {
      memcpy(pipe->buffer, src + first, second);
    }
    pipe->write_pos = (pipe->write_pos + chunk) % pipe->capacity;
    pipe->used += chunk;
    src += chunk;
    count -= chunk;
    pthread_cond_broadcast(&pipe->cond);
  }
  pthread_mutex_unlock(&pipe->mutex);
  return LC_OK;
}

void lc_stream_pipe_finish(lc_stream_pipe *pipe) {
  if (pipe == NULL) {
    return;
  }
  pthread_mutex_lock(&pipe->mutex);
  pipe->writer_closed = 1;
  pipe->writer_ref = 0;
  pthread_cond_broadcast(&pipe->cond);
  lc_stream_pipe_release_locked(pipe);
}

void lc_stream_pipe_fail(lc_stream_pipe *pipe, int code, const char *message) {
  if (pipe == NULL) {
    return;
  }
  pthread_mutex_lock(&pipe->mutex);
  pipe->writer_closed = 1;
  pipe->writer_ref = 0;
  pipe->error_code = code != LC_OK ? code : LC_ERR_TRANSPORT;
  lc_free_with_allocator(&pipe->allocator, pipe->error_message);
  pipe->error_message = lc_strdup_with_allocator(&pipe->allocator, message);
  pthread_cond_broadcast(&pipe->cond);
  lc_stream_pipe_release_locked(pipe);
}

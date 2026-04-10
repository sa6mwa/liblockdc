#include "lc/lc.h"
#include "lc_engine_api.h"
#include "lc_log.h"

#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

typedef struct lc_fd_sink {
  lc_sink_impl base;
  int fd;
  int close_fd;
} lc_fd_sink;

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
                       const char *txn_id, long fencing_token, long version,
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
  lc_client_handle *client;
  size_t i;
  int rc;

  if (config == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_client_open requires config and out", NULL, NULL,
                        NULL);
  }
  memset(&engine_config, 0, sizeof(engine_config));
  engine_config.endpoints = config->endpoints;
  engine_config.endpoint_count = config->endpoint_count;
  engine_config.unix_socket_path = config->unix_socket_path;
  engine_config.client_bundle_path = config->client_bundle_path;
  engine_config.default_namespace = config->default_namespace;
  engine_config.timeout_ms = config->timeout_ms;
  engine_config.disable_mtls = config->disable_mtls;
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

  lc_engine_error_init(&engine_error);
  client = (lc_client_handle *)lc_calloc_with_allocator(&config->allocator, 1U,
                                                        sizeof(*client));
  if (client == NULL) {
    lc_engine_error_cleanup(&engine_error);
    return lc_error_set(error, LC_ERR_NOMEM, 0L, "failed to allocate client",
                        NULL, NULL, NULL);
  }
  rc = lc_engine_client_open(&engine_config, &client->engine, &engine_error);
  if (rc != LC_ENGINE_OK) {
    lc_error_from_engine(error, &engine_error);
    lc_engine_error_cleanup(&engine_error);
    lc_free_with_allocator(&config->allocator, client);
    return error->code;
  }
  client->endpoint_count = config->endpoint_count;
  if (config->endpoint_count != 0U) {
    client->endpoints = (char **)lc_calloc_with_allocator(
        &config->allocator, config->endpoint_count, sizeof(char *));
    if (client->endpoints == NULL) {
      lc_engine_client_close(client->engine);
      lc_engine_error_cleanup(&engine_error);
      lc_free_with_allocator(&config->allocator, client);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate client endpoint copy", NULL, NULL,
                          NULL);
    }
    for (i = 0U; i < config->endpoint_count; ++i) {
      client->endpoints[i] =
          lc_strdup_with_allocator(&config->allocator, config->endpoints[i]);
      if (client->endpoints[i] == NULL && config->endpoints[i] != NULL) {
        lc_client_close_method(&client->pub);
        lc_engine_error_cleanup(&engine_error);
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to copy client endpoint", NULL, NULL, NULL);
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
  client->disable_mtls = config->disable_mtls;
  client->insecure_skip_verify = config->insecure_skip_verify;
  client->prefer_http_2 = config->prefer_http_2;
  client->disable_logger_sys_field = config->disable_logger_sys_field;
  client->base_logger =
      config->logger != NULL ? config->logger : lc_log_noop_logger();
  client->logger = lc_engine_client_logger(client->engine);
  client->http_json_response_limit_bytes =
      config->http_json_response_limit_bytes;
  client->allocator = config->allocator;
  client->pub.acquire = lc_client_acquire_method;
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

int lc_copy(lc_source *src, lc_sink *dst, size_t *written, lc_error *error) {
  unsigned char buffer[8192];
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

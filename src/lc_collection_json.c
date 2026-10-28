#include "lc_collection_json.h"
#include "lc_api_internal.h"
#include "lc_utf8.h"

#include <lonejson.h>

struct lc_collection_json_writer {
  const lc_allocator *allocator;
  lonejson_allocator json_allocator;
  lonejson *runtime;
  lonejson_writer writer;
  lonejson_writer_value_stream value;
  lc_sink *sink;
  lc_error *error;
  lc_utf8_state utf8;
  lc_u64 written;
  unsigned char output[4096];
  size_t used;
  int root_seen;
  int failed;
  int finished;
};

static void *lc_cj_alloc(void *context, size_t size) {
  return lc_alloc_with_allocator((const lc_allocator *)context, size);
}
static void *lc_cj_realloc(void *context, void *ptr, size_t size) {
  return lc_realloc_with_allocator((const lc_allocator *)context, ptr, size);
}
static void lc_cj_free(void *context, void *ptr) {
  lc_free_with_allocator((const lc_allocator *)context, ptr);
}

static lonejson_status lc_cj_flush(lc_collection_json_writer *out) {
  if (out->used != 0U &&
      !out->sink->write(out->sink, out->output, out->used, out->error)) {
    out->failed = out->error != NULL && out->error->code != LC_OK
                      ? out->error->code
                      : LC_ERR_TRANSPORT;
    if (out->error != NULL && out->error->code == LC_OK)
      lc_error_set(out->error, out->failed, 0L,
                   "collection JSON destination rejected bytes", NULL, NULL,
                   NULL);
    return LONEJSON_STATUS_CALLBACK_FAILED;
  }
  out->written += (lc_u64)out->used;
  out->used = 0U;
  return LONEJSON_STATUS_OK;
}

static lonejson_status lc_cj_write(void *user, const void *data, size_t length,
                                   lonejson_error *json_error) {
  lc_collection_json_writer *out = user;
  const unsigned char *cursor = data;
  size_t take;
  (void)json_error;
  if ((lc_u64)length > LC_U64_MAX - out->written - (lc_u64)out->used) {
    out->failed =
        lc_error_set(out->error, LC_ERR_INVALID, 0L,
                     "collection JSON byte count overflow", NULL, NULL, NULL);
    return LONEJSON_STATUS_OVERFLOW;
  }
  /* lonejson can emit individual string bytes. Coalesce only a bounded chunk,
   * flushing on capacity and before write returns; never wait for object EOF.
   */
  while (length != 0U) {
    take = sizeof(out->output) - out->used;
    if (take > length)
      take = length;
    memcpy(out->output + out->used, cursor, take);
    out->used += take;
    cursor += take;
    length -= take;
    if (out->used == sizeof(out->output) &&
        lc_cj_flush(out) != LONEJSON_STATUS_OK)
      return LONEJSON_STATUS_CALLBACK_FAILED;
  }
  return LONEJSON_STATUS_OK;
}

static int lc_cj_status(lc_collection_json_writer *writer,
                        lonejson_status status, lonejson_error *json_error,
                        lc_error *error) {
  int rc = writer->failed;
  if (rc == LC_OK && status != LONEJSON_STATUS_OK)
    rc = lc_error_set(error,
                      status == LONEJSON_STATUS_ALLOCATION_FAILED
                          ? LC_ERR_NOMEM
                          : LC_ERR_INVALID,
                      0L, "collection JSON validation failed",
                      json_error->message, NULL, NULL);
  writer->failed = rc;
  writer->error = NULL;
  return rc;
}

void lc_collection_json_writer_close(lc_collection_json_writer *writer) {
  const lc_allocator *allocator;
  if (writer == NULL)
    return;
  allocator = writer->allocator;
  lonejson_writer_value_stream_cleanup(&writer->value);
  lonejson_writer_cleanup(&writer->writer);
  lonejson_free(writer->runtime);
  lc_free_with_allocator(allocator, writer);
}

int lc_collection_json_writer_open(lc_sink *dst, const lc_allocator *allocator,
                                   lc_collection_json_writer **out,
                                   lc_error *error) {
  lc_collection_json_writer *writer;
  lonejson_config config;
  lonejson_error json_error;
  lonejson_status status;
  int rc;
  if (out != NULL)
    *out = NULL;
  if (dst == NULL || dst->write == NULL || out == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection JSON writer requires sink and output", NULL,
                        NULL, NULL);
  writer = lc_calloc_with_allocator(allocator, 1U, sizeof(*writer));
  if (writer == NULL)
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate collection JSON writer", NULL, NULL,
                        NULL);
  writer->allocator = allocator;
  writer->sink = dst;
  writer->json_allocator.malloc_fn = lc_cj_alloc;
  writer->json_allocator.realloc_fn = lc_cj_realloc;
  writer->json_allocator.free_fn = lc_cj_free;
  writer->json_allocator.ctx = (void *)allocator;
  config = lonejson_default_config();
  config.allocator = &writer->json_allocator;
  config.max_alloc_bytes = 1024U * 1024U;
  config.max_depth = LC_COLLECTION_JSON_MAX_DEPTH;
  config.json_value_max_depth = LC_COLLECTION_JSON_MAX_DEPTH;
  config.json_value_max_total_bytes = 0U;
  config.json_value_max_string_bytes = 0U;
  config.json_value_max_key_bytes = LC_COLLECTION_JSON_MAX_KEY_BYTES;
  config.json_value_max_number_bytes = LC_COLLECTION_JSON_MAX_NUMBER_BYTES;
  config.write_pretty = 0;
  lonejson_error_init(&json_error);
  writer->runtime = lonejson_new(&config, &json_error);
  if (writer->runtime == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to initialize collection JSON validator",
                      json_error.message, NULL, NULL);
    lc_collection_json_writer_close(writer);
    return rc;
  }
  lonejson_writer_value_stream_init(&writer->value);
  status = lonejson_writer_init_sink(writer->runtime, &writer->writer,
                                     lc_cj_write, writer, &json_error);
  if (status == LONEJSON_STATUS_OK)
    status = lonejson_writer_value_stream_open(&writer->value, &writer->writer,
                                               &json_error);
  rc = lc_cj_status(writer, status, &json_error, error);
  if (rc != LC_OK)
    lc_collection_json_writer_close(writer);
  else
    *out = writer;
  return rc;
}

int lc_collection_json_writer_write(lc_collection_json_writer *writer,
                                    const void *bytes, size_t count,
                                    lc_error *error) {
  const unsigned char *cursor = bytes;
  lonejson_error json_error;
  lonejson_status status;
  size_t take, i;
  int rc = LC_OK;
  if (writer == NULL || writer->failed || writer->finished)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection JSON writer is not writable", NULL, NULL,
                        NULL);
  if (count != 0U && bytes == NULL) {
    writer->failed = LC_ERR_INVALID;
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection JSON write requires bytes", NULL, NULL,
                        NULL);
  }
  writer->error = error;
  lonejson_error_init(&json_error);
  while (count != 0U && rc == LC_OK) {
    take = count > LC_COLLECTION_JSON_CHUNK_BYTES
               ? LC_COLLECTION_JSON_CHUNK_BYTES
               : count;
    if (!lc_utf8_push(&writer->utf8, cursor, take)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection JSON contains invalid UTF-8", NULL, NULL,
                        NULL);
      break;
    }
    if (!writer->root_seen) {
      for (i = 0U; i < take; ++i) {
        if (cursor[i] != ' ' && cursor[i] != '\n' && cursor[i] != '\r' &&
            cursor[i] != '\t') {
          writer->root_seen = 1;
          if (cursor[i] != '{')
            rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                              "collection append requires one JSON object",
                              NULL, NULL, NULL);
          break;
        }
      }
    }
    if (rc == LC_OK) {
      status = lonejson_writer_value_stream_push(&writer->value, cursor, take,
                                                 &json_error);
      rc = lc_cj_status(writer, status, &json_error, error);
      writer->error = error;
    }
    cursor += take;
    count -= take;
  }
  if (rc == LC_OK && lc_cj_flush(writer) != LONEJSON_STATUS_OK)
    rc = writer->failed;
  writer->failed = rc;
  writer->error = NULL;
  return rc;
}

int lc_collection_json_writer_finish(lc_collection_json_writer *writer,
                                     lc_u64 *written, lc_error *error) {
  lonejson_error json_error;
  lonejson_status status;
  int rc;
  if (written != NULL)
    *written = writer != NULL ? writer->written : 0U;
  if (writer == NULL || written == NULL || writer->finished || writer->failed)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection JSON writer cannot finish", NULL, NULL,
                        NULL);
  writer->finished = 1;
  if (writer->utf8.remaining != 0) {
    writer->failed = LC_ERR_INVALID;
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection JSON has incomplete UTF-8", NULL, NULL,
                        NULL);
  }
  lonejson_error_init(&json_error);
  writer->error = error;
  status = lonejson_writer_value_stream_close(&writer->value, &json_error);
  if (status == LONEJSON_STATUS_OK)
    status = lonejson_writer_finish(&writer->writer, &json_error);
  if (status == LONEJSON_STATUS_OK)
    status = lc_cj_flush(writer);
  rc = lc_cj_status(writer, status, &json_error, error);
  *written = writer->written;
  return rc;
}

int lc_collection_json_copy(lc_source *src, lc_sink *dst,
                            const lc_allocator *allocator, lc_u64 *written,
                            lc_error *error) {
  lc_collection_json_writer *writer = NULL;
  unsigned char *input = NULL;
  size_t count;
  int rc;
  if (written != NULL)
    *written = 0U;
  if (src == NULL || src->read == NULL || written == NULL)
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection JSON requires source and output", NULL,
                        NULL, NULL);
  rc = lc_collection_json_writer_open(dst, allocator, &writer, error);
  if (rc == LC_OK) {
    input = lc_alloc_with_allocator(allocator, LC_COLLECTION_JSON_CHUNK_BYTES);
    if (input == NULL)
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate collection JSON input chunk", NULL,
                        NULL, NULL);
  }
  while (rc == LC_OK) {
    count = src->read(src, input, LC_COLLECTION_JSON_CHUNK_BYTES, error);
    if (error != NULL && error->code != LC_OK) {
      rc = error->code;
      break;
    }
    if (count > LC_COLLECTION_JSON_CHUNK_BYTES) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection source exceeded requested chunk size", NULL,
                        NULL, NULL);
      break;
    }
    if (count == 0U) {
      rc = lc_collection_json_writer_finish(writer, written, error);
      break;
    }
    rc = lc_collection_json_writer_write(writer, input, count, error);
  }
  if (writer != NULL)
    *written = writer->written;
  lc_collection_json_writer_close(writer);
  lc_free_with_allocator(allocator, input);
  return rc;
}

#include "lc_collection_json.h"
#include "lc_api_internal.h"
#include "lc_utf8.h"

#include <lonejson.h>

typedef struct lc_collection_json_output {
  lc_sink *sink;
  lc_error *error;
  lc_u64 written;
  int failed;
} lc_collection_json_output;

static void *lc_cj_alloc(void *context, size_t size) {
  return lc_alloc_with_allocator((const lc_allocator *)context, size);
}

static void *lc_cj_realloc(void *context, void *ptr, size_t size) {
  return lc_realloc_with_allocator((const lc_allocator *)context, ptr, size);
}

static void lc_cj_free(void *context, void *ptr) {
  lc_free_with_allocator((const lc_allocator *)context, ptr);
}

static lonejson_status lc_cj_write(void *user, const void *data, size_t length,
                                   lonejson_error *json_error) {
  lc_collection_json_output *out;
  (void)json_error;
  out = (lc_collection_json_output *)user;
  if ((lc_u64)length > LC_U64_MAX - out->written) {
    out->failed =
        lc_error_set(out->error, LC_ERR_INVALID, 0L,
                     "collection JSON byte count overflow", NULL, NULL, NULL);
    return LONEJSON_STATUS_OVERFLOW;
  }
  if (length != 0U && !out->sink->write(out->sink, data, length, out->error)) {
    out->failed = out->error != NULL && out->error->code != LC_OK
                      ? out->error->code
                      : LC_ERR_TRANSPORT;
    if (out->error != NULL && out->error->code == LC_OK) {
      lc_error_set(out->error, out->failed, 0L,
                   "collection JSON destination rejected bytes", NULL, NULL,
                   NULL);
    }
    return LONEJSON_STATUS_CALLBACK_FAILED;
  }
  out->written += (lc_u64)length;
  return LONEJSON_STATUS_OK;
}

int lc_collection_json_copy(lc_source *src, lc_sink *dst,
                            const lc_allocator *allocator, lc_u64 *written,
                            lc_error *error) {
  lonejson_allocator json_allocator;
  lonejson_config config;
  lonejson_error json_error;
  lonejson_status status;
  lonejson *runtime;
  lonejson_writer writer;
  lonejson_writer_value_stream value;
  lc_collection_json_output output;
  lc_utf8_state utf8;
  unsigned char *input;
  size_t count;
  size_t i;
  int root_seen;
  int rc;

  if (written != NULL) {
    *written = 0U;
  }
  if (src == NULL || src->read == NULL || dst == NULL || dst->write == NULL ||
      written == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection JSON requires source, sink, and output",
                        NULL, NULL, NULL);
  }
  memset(&json_allocator, 0, sizeof(json_allocator));
  json_allocator.malloc_fn = lc_cj_alloc;
  json_allocator.realloc_fn = lc_cj_realloc;
  json_allocator.free_fn = lc_cj_free;
  json_allocator.ctx = (void *)allocator;
  config = lonejson_default_config();
  config.allocator = &json_allocator;
  config.max_alloc_bytes = 1024U * 1024U;
  config.max_depth = LC_COLLECTION_JSON_MAX_DEPTH;
  config.json_value_max_depth = LC_COLLECTION_JSON_MAX_DEPTH;
  config.json_value_max_total_bytes = 0U;
  config.json_value_max_string_bytes = 0U;
  config.json_value_max_key_bytes = LC_COLLECTION_JSON_MAX_KEY_BYTES;
  config.json_value_max_number_bytes = LC_COLLECTION_JSON_MAX_NUMBER_BYTES;
  config.write_pretty = 0;
  lonejson_error_init(&json_error);
  runtime = lonejson_new(&config, &json_error);
  if (runtime == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize collection JSON validator",
                        json_error.message, NULL, NULL);
  }
  input = (unsigned char *)lc_alloc_with_allocator(
      allocator, LC_COLLECTION_JSON_CHUNK_BYTES);
  if (input == NULL) {
    lonejson_free(runtime);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate collection JSON input chunk", NULL,
                        NULL, NULL);
  }
  memset(&writer, 0, sizeof(writer));
  memset(&output, 0, sizeof(output));
  output.sink = dst;
  output.error = error;
  lonejson_writer_value_stream_init(&value);
  status = lonejson_writer_init_sink(runtime, &writer, lc_cj_write, &output,
                                     &json_error);
  if (status == LONEJSON_STATUS_OK) {
    status = lonejson_writer_value_stream_open(&value, &writer, &json_error);
  }
  rc = LC_OK;
  root_seen = 0;
  memset(&utf8, 0, sizeof(utf8));
  while (status == LONEJSON_STATUS_OK) {
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
      if (utf8.remaining != 0) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "collection JSON has incomplete UTF-8", NULL, NULL,
                          NULL);
        break;
      }
      status = lonejson_writer_value_stream_close(&value, &json_error);
      if (status == LONEJSON_STATUS_OK) {
        status = lonejson_writer_finish(&writer, &json_error);
      }
      break;
    }
    if (!lc_utf8_push(&utf8, input, count)) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "collection JSON contains invalid UTF-8", NULL, NULL,
                        NULL);
      break;
    }
    if (!root_seen) {
      for (i = 0U; i < count; ++i) {
        if (input[i] != ' ' && input[i] != '\n' && input[i] != '\r' &&
            input[i] != '\t') {
          root_seen = 1;
          if (input[i] != '{') {
            rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                              "collection append requires one JSON object",
                              NULL, NULL, NULL);
          }
          break;
        }
      }
      if (rc != LC_OK) {
        break;
      }
    }
    status =
        lonejson_writer_value_stream_push(&value, input, count, &json_error);
  }
  if (rc == LC_OK && output.failed != LC_OK) {
    rc = output.failed;
  }
  if (rc == LC_OK && status != LONEJSON_STATUS_OK) {
    rc = lc_error_set(error,
                      status == LONEJSON_STATUS_ALLOCATION_FAILED
                          ? LC_ERR_NOMEM
                          : LC_ERR_INVALID,
                      0L, "collection JSON validation failed",
                      json_error.message, NULL, NULL);
  }
  *written = output.written;
  lonejson_writer_value_stream_cleanup(&value);
  lonejson_writer_cleanup(&writer);
  lonejson_free(runtime);
  lc_free_with_allocator(allocator, input);
  return rc;
}

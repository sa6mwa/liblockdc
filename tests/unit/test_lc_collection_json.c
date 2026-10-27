#include <cmocka.h>
#include <fcntl.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../support/lc_test_tmp.h"
#include "lc_api_internal.h"
#include "lc_collection_json.h"
#include "lc_pouch_crypto.h"

#define CODEC_PREFIX LC_COLLECTION_TMP_PARENT "/collection-codec-"

typedef struct input {
  lc_source pub;
  const char *text;
  size_t length;
  size_t offset;
  size_t chunk;
  size_t fail_at;
} input;

static size_t input_read(lc_source *self, void *bytes, size_t capacity,
                         lc_error *error) {
  input *in = (input *)self;
  size_t length = in->length - in->offset;
  if (in->fail_at && in->offset >= in->fail_at) {
    lc_error_set(error, LC_ERR_TIMEOUT, 0, "source timed out", NULL, NULL,
                 NULL);
    return 0;
  }
  if (length > capacity)
    length = capacity;
  if (in->chunk && length > in->chunk)
    length = in->chunk;
  if (length)
    memcpy(bytes, in->text + in->offset, length);
  in->offset += length;
  return length;
}

static void input_init(input *in, const char *text, size_t chunk) {
  memset(in, 0, sizeof(*in));
  in->pub.read = input_read;
  in->text = text;
  in->length = strlen(text);
  in->chunk = chunk;
}

static void test_compacts_across_every_input_boundary(void **state) {
  const char *json = " \n { \"z\" : [1, true, null, {\"s\":\"white space\"}], "
                     "\"a\": -1.25e2 }\r\n";
  const char *expected =
      "{\"z\":[1,true,null,{\"s\":\"white space\"}],\"a\":-1.25e2}";
  (void)state;
  for (size_t chunk = 1; chunk <= strlen(json); ++chunk) {
    input in;
    lc_sink *sink = NULL;
    lc_error error;
    lc_u64 written;
    const void *bytes;
    size_t length;
    lc_error_init(&error);
    input_init(&in, json, chunk);
    assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
    assert_int_equal(
        lc_collection_json_copy(&in.pub, sink, NULL, &written, &error), LC_OK);
    assert_int_equal(lc_sink_memory_bytes(sink, &bytes, &length, &error),
                     LC_OK);
    assert_true(written == length);
    assert_int_equal(length, strlen(expected));
    assert_memory_equal(bytes, expected, length);
    lc_sink_close(sink);
    lc_error_cleanup(&error);
  }
}

static void test_rejects_invalid_and_nonobject_inputs(void **state) {
  const char *invalid[] = {"",
                           " ",
                           "[]",
                           "null",
                           "42",
                           "\"object\"",
                           "{} {}",
                           "{\"a\":}",
                           "{",
                           "{\"a\":\"unterminated",
                           "{\"a\":01}",
                           "{\"a\":true} trailing",
                           "{\"a\":\"\xff\"}",
                           "{\"a\":\"\xc0\xaf\"}",
                           "{\"a\":\"\xed\xa0\x80\"}",
                           "{\"a\":\"\xf4\x90\x80\x80\"}",
                           "{\"a\":\"\xe2\x82",
                           "{\"a\":\"\\uD800\"}",
                           "{\"a\":\"\\uDC00\"}"};
  (void)state;
  for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); ++i) {
    input in;
    lc_sink *sink = NULL;
    lc_error error;
    lc_u64 written;
    int rc;
    lc_error_init(&error);
    input_init(&in, invalid[i], 1);
    assert_int_equal(lc_sink_to_discard(&sink, &error), LC_OK);
    rc = lc_collection_json_copy(&in.pub, sink, NULL, &written, &error);
    lc_sink_close(sink);
    lc_error_cleanup(&error);
    if (rc != LC_ERR_INVALID)
      print_error("invalid input index %zu\n", i);
    assert_int_equal(rc, LC_ERR_INVALID);
  }
}

static int failing_write(lc_sink *sink, const void *bytes, size_t length,
                         lc_error *error) {
  (void)sink;
  (void)bytes;
  (void)length;
  lc_error_set(error, LC_ERR_TIMEOUT, 0, "destination timed out", NULL, NULL,
               NULL);
  return 0;
}

static void test_source_and_sink_failures_are_preserved(void **state) {
  input in;
  lc_sink sink;
  lc_sink *discard = NULL;
  lc_error error;
  lc_u64 written;
  (void)state;
  lc_error_init(&error);
  input_init(&in, "{\"x\":42}", 1);
  in.fail_at = 4;
  assert_int_equal(lc_sink_to_discard(&discard, &error), LC_OK);
  assert_int_equal(
      lc_collection_json_copy(&in.pub, discard, NULL, &written, &error),
      LC_ERR_TIMEOUT);
  assert_string_equal(error.message, "source timed out");
  lc_sink_close(discard);
  lc_error_cleanup(&error);
  lc_error_init(&error);
  memset(&sink, 0, sizeof(sink));
  sink.write = failing_write;
  input_init(&in, "{}", 1);
  assert_int_equal(
      lc_collection_json_copy(&in.pub, &sink, NULL, &written, &error),
      LC_ERR_TIMEOUT);
  assert_string_equal(error.message, "destination timed out");
  lc_error_cleanup(&error);
}

typedef union allocation_header {
  long double alignment;
  void *pointer_alignment;
  size_t size;
} allocation_header;

typedef struct allocation_stats {
  size_t live;
  size_t peak;
  size_t calls;
  size_t fail_at;
} allocation_stats;

static void *tracked_allocate(void *context, size_t size) {
  allocation_stats *stats = context;
  allocation_header *block;
  if (++stats->calls == stats->fail_at)
    return NULL;
  block = malloc(sizeof(*block) + size);
  if (!block)
    return NULL;
  block->size = size;
  stats->live += size;
  if (stats->live > stats->peak)
    stats->peak = stats->live;
  return block + 1;
}

static void tracked_free(void *context, void *ptr) {
  allocation_stats *stats = context;
  allocation_header *block;
  if (!ptr)
    return;
  block = (allocation_header *)ptr - 1;
  stats->live -= block->size;
  free(block);
}

static void *tracked_reallocate(void *context, void *ptr, size_t size) {
  allocation_stats *stats = context;
  allocation_header *block;
  size_t previous;
  if (!ptr)
    return tracked_allocate(context, size);
  if (!size) {
    tracked_free(context, ptr);
    return NULL;
  }
  if (++stats->calls == stats->fail_at)
    return NULL;
  block = (allocation_header *)ptr - 1;
  previous = block->size;
  block = realloc(block, sizeof(*block) + size);
  if (!block)
    return NULL;
  block->size = size;
  stats->live = stats->live - previous + size;
  if (stats->live > stats->peak)
    stats->peak = stats->live;
  return block + 1;
}

typedef struct generated {
  lc_source pub;
  lc_sink sink;
  size_t key_bytes;
  size_t value_bytes;
  size_t read;
  size_t written;
  int before_eof;
  int random_values;
} generated;

static unsigned char generated_byte(const generated *g, size_t i) {
  if (i == 0)
    return '{';
  if (i == 1)
    return '"';
  if (i < g->key_bytes + 2)
    return 'k';
  if (i == g->key_bytes + 2)
    return '"';
  if (i == g->key_bytes + 3)
    return ':';
  if (i == g->key_bytes + 4)
    return '"';
  if (i < g->key_bytes + g->value_bytes + 5) {
    if (g->random_values) {
      static const char alphabet[] =
          "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-";
      uint32_t value = (uint32_t)i;
      value ^= value >> 16;
      value *= (uint32_t)0x7feb352dUL;
      value ^= value >> 15;
      value *= (uint32_t)0x846ca68bUL;
      value ^= value >> 16;
      return (unsigned char)alphabet[value & 63U];
    }
    return 'v';
  }
  if (i == g->key_bytes + g->value_bytes + 5)
    return '"';
  return '}';
}

static size_t generated_read(lc_source *self, void *bytes, size_t capacity,
                             lc_error *error) {
  generated *g = (generated *)self;
  size_t length = g->key_bytes + g->value_bytes + 7 - g->read;
  unsigned char *out = bytes;
  (void)error;
  if (length > capacity)
    length = capacity;
  for (size_t i = 0; i < length; ++i)
    out[i] = generated_byte(g, g->read++);
  return length;
}

static int generated_write(lc_sink *self, const void *bytes, size_t length,
                           lc_error *error) {
  generated *g = self->impl;
  const unsigned char *in = bytes;
  (void)error;
  if (g->read < g->key_bytes + g->value_bytes + 7)
    g->before_eof = 1;
  for (size_t i = 0; i < length; ++i) {
    assert_true(g->written < g->key_bytes + g->value_bytes + 7);
    assert_int_equal(in[i], generated_byte(g, g->written++));
  }
  return 1;
}

static void test_large_values_and_keys_stream_with_bounded_heap(void **state) {
  generated g;
  allocation_stats stats;
  lc_allocator allocator;
  lc_error error;
  lc_u64 written;
  size_t baseline = 0;
  (void)state;
  allocator.malloc_fn = tracked_allocate;
  allocator.realloc_fn = tracked_reallocate;
  allocator.free_fn = tracked_free;
  allocator.context = &stats;
  for (size_t size = 2 * 1024 * 1024; size <= 8 * 1024 * 1024; size *= 4) {
    memset(&g, 0, sizeof(g));
    memset(&stats, 0, sizeof(stats));
    g.pub.read = generated_read;
    g.sink.write = generated_write;
    g.sink.impl = &g;
    g.key_bytes = LC_COLLECTION_JSON_CHUNK_BYTES * 2;
    g.value_bytes = size;
    lc_error_init(&error);
    assert_int_equal(
        lc_collection_json_copy(&g.pub, &g.sink, &allocator, &written, &error),
        LC_OK);
    assert_true(written == g.key_bytes + g.value_bytes + 7);
    assert_true(g.written == written);
    assert_true(g.before_eof);
    assert_true(stats.peak < 1024 * 1024);
    assert_int_equal(stats.live, 0);
    if (!baseline)
      baseline = stats.peak;
    else
      assert_true(stats.peak == baseline);
    lc_error_cleanup(&error);
  }
}

static void test_allocation_failures_unwind_without_leaks(void **state) {
  lc_allocator allocator;
  allocation_stats stats;
  lc_sink *sink = NULL;
  lc_error error;
  lc_u64 written;
  input in;
  size_t calls;
  (void)state;
  memset(&stats, 0, sizeof(stats));
  allocator.malloc_fn = tracked_allocate;
  allocator.realloc_fn = tracked_reallocate;
  allocator.free_fn = tracked_free;
  allocator.context = &stats;
  lc_error_init(&error);
  assert_int_equal(lc_sink_to_discard(&sink, &error), LC_OK);
  input_init(&in, "{\"object\":{\"array\":[1,2,3]},\"text\":\"hello\"}", 1);
  assert_int_equal(
      lc_collection_json_copy(&in.pub, sink, &allocator, &written, &error),
      LC_OK);
  calls = stats.calls;
  assert_int_equal(stats.live, 0);
  for (size_t fail = 1; fail <= calls; ++fail) {
    int rc;
    memset(&stats, 0, sizeof(stats));
    stats.fail_at = fail;
    in.offset = 0;
    lc_error_cleanup(&error);
    lc_error_init(&error);
    rc = lc_collection_json_copy(&in.pub, sink, &allocator, &written, &error);
    assert_true(rc != LC_OK);
    assert_int_equal(stats.live, 0);
  }
  lc_sink_close(sink);
  lc_error_cleanup(&error);
}

static int produce_json(void *context, lc_sink *sink, lc_error *error) {
  lc_u64 written;
  return lc_collection_json_copy(context, sink, NULL, &written, error);
}

static void test_push_codec_roundtrips_streamed_large_objects(void **state) {
  const char *context = "collection-body-binding";
  char *key = NULL;
  lc_error error;
  (void)state;
  lc_error_init(&error);
  assert_int_equal(lc_pouch_crypto_generate_key_string(&key, &error), LC_OK);
  for (int mode = 0; mode < 4; ++mode) {
    for (int allow_compression = 0; allow_compression <= 1;
         ++allow_compression) {
      lc_pouch_crypto_open_options opts;
      lc_pouch_crypto *crypto = NULL;
      lc_source *decoded = NULL;
      char *descriptor = NULL;
      char path[] = CODEC_PREFIX "XXXXXX";
      uint64_t plain_bytes, cipher_bytes;
      unsigned long crc;
      unsigned char read_buffer[32768];
      generated g;
      int fd;
      struct stat st;
      memset(&opts, 0, sizeof(opts));
      opts.key_string = mode & 1 ? key : NULL;
      opts.compression_enabled = (mode & 2) != 0;
      assert_int_equal(lc_pouch_crypto_open(NULL, &opts, &crypto, NULL, &error),
                       LC_OK);
      fd = lc_test_tmp_mkstemp(path, CODEC_PREFIX);
      assert_true(fd >= 0);
      memset(&g, 0, sizeof(g));
      g.pub.read = generated_read;
      g.sink.write = generated_write;
      g.sink.impl = &g;
      g.key_bytes = 20000;
      g.value_bytes = 2 * 1024 * 1024;
      g.random_values = opts.compression_enabled;
      assert_int_equal(lc_pouch_crypto_produce_to_fd(
                           crypto, context, fd, produce_json, &g.pub,
                           &plain_bytes, &cipher_bytes, &crc, &descriptor,
                           allow_compression, &error),
                       LC_OK);
      assert_true(plain_bytes == g.key_bytes + g.value_bytes + 7);
      assert_int_equal(fstat(fd, &st), 0);
      assert_true(cipher_bytes == (uint64_t)st.st_size);
      assert_int_equal(close(fd), 0);
      assert_int_equal(lc_pouch_crypto_source_from_file_span(
                           crypto, context, context, path, 0, cipher_bytes,
                           descriptor, &decoded, &error),
                       LC_OK);
      for (;;) {
        size_t got =
            decoded->read(decoded, read_buffer, sizeof(read_buffer), &error);
        assert_int_equal(error.code, LC_OK);
        if (!got)
          break;
        assert_true(g.sink.write(&g.sink, read_buffer, got, &error));
      }
      assert_true(g.written == plain_bytes);
      lc_source_close(decoded);
      lc_free_with_allocator(NULL, descriptor);
      lc_pouch_crypto_close(crypto);
      lc_test_tmp_cleanup_path(path, CODEC_PREFIX);
    }
  }
  lc_pouch_crypto_key_string_free(key);
  lc_error_cleanup(&error);
}

static int producer_ignores_sink_error(void *context, lc_sink *sink,
                                       lc_error *error) {
  unsigned char bytes[65536];
  (void)context;
  memset(bytes, 'v', sizeof(bytes));
  (void)sink->write(sink, bytes, sizeof(bytes), error);
  return LC_OK;
}

static void test_push_codec_does_not_complete_failed_producers(void **state) {
  lc_pouch_crypto_open_options opts;
  lc_pouch_crypto *crypto = NULL;
  char *key = NULL;
  lc_error error;
  (void)state;
  lc_error_init(&error);
  assert_int_equal(lc_pouch_crypto_generate_key_string(&key, &error), LC_OK);
  for (int mode = 0; mode < 4; ++mode) {
    char *descriptor = NULL;
    char path[] = CODEC_PREFIX "XXXXXX";
    uint64_t plain_bytes = 42, cipher_bytes = 42;
    unsigned long crc;
    input in;
    int fd;
    memset(&opts, 0, sizeof(opts));
    opts.key_string = mode & 1 ? key : NULL;
    opts.compression_enabled = (mode & 2) != 0;
    assert_int_equal(lc_pouch_crypto_open(NULL, &opts, &crypto, NULL, &error),
                     LC_OK);
    fd = lc_test_tmp_mkstemp(path, CODEC_PREFIX);
    assert_true(fd >= 0);
    input_init(&in, "{\"value\":\"almost done\"} trailing", 1);
    assert_int_equal(
        lc_pouch_crypto_produce_to_fd(crypto, "binding", fd, produce_json,
                                      &in.pub, &plain_bytes, &cipher_bytes,
                                      &crc, &descriptor, 1, &error),
        LC_ERR_INVALID);
    assert_null(descriptor);
    assert_true(plain_bytes == 0 && cipher_bytes == 0);
    lc_error_cleanup(&error);
    lc_error_init(&error);
    assert_int_equal(close(fd), 0);
    lc_pouch_crypto_close(crypto);
    lc_test_tmp_cleanup_path(path, CODEC_PREFIX);
  }
  {
    uint64_t plain_bytes, cipher_bytes;
    char *descriptor = NULL;
    int fd = open("/dev/null", O_RDONLY);
    assert_true(fd >= 0);
    assert_int_equal(lc_pouch_crypto_produce_to_fd(
                         NULL, NULL, fd, producer_ignores_sink_error, NULL,
                         &plain_bytes, &cipher_bytes, NULL, &descriptor, 0,
                         &error),
                     LC_ERR_TRANSPORT);
    assert_null(descriptor);
    assert_int_equal(close(fd), 0);
  }
  lc_pouch_crypto_key_string_free(key);
  lc_error_cleanup(&error);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_compacts_across_every_input_boundary),
      cmocka_unit_test(test_rejects_invalid_and_nonobject_inputs),
      cmocka_unit_test(test_source_and_sink_failures_are_preserved),
      cmocka_unit_test(test_large_values_and_keys_stream_with_bounded_heap),
      cmocka_unit_test(test_allocation_failures_unwind_without_leaks),
      cmocka_unit_test(test_push_codec_roundtrips_streamed_large_objects),
      cmocka_unit_test(test_push_codec_does_not_complete_failed_producers),
  };
  if (lc_allocator_tracking_configure(1, NULL) != LC_OK)
    return 1;
  return cmocka_run_group_tests(tests, NULL, NULL);
}

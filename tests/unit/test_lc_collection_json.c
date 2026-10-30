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
#include <zlib.h>

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
  size_t reject_above;
} allocation_stats;

static void *tracked_allocate(void *context, size_t size) {
  allocation_stats *stats = context;
  allocation_header *block;
  if (++stats->calls == stats->fail_at ||
      (stats->reject_above != 0 && size > stats->reject_above))
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
  if (++stats->calls == stats->fail_at ||
      (stats->reject_above != 0 && size > stats->reject_above))
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

static void
test_incremental_json_finish_abort_and_terminal_states(void **state) {
  const char *text = " {\"key\":\"\\u20ac \xe2\x82\xac\",\"n\":123} ";
  lc_collection_json_writer *writer = NULL;
  lc_sink *sink = NULL;
  lc_error error;
  lc_u64 written;
  const void *bytes;
  size_t length;
  (void)state;
  lc_error_init(&error);
  assert_int_equal(lc_sink_to_memory(&sink, &error), LC_OK);
  assert_int_equal(lc_collection_json_writer_open(sink, NULL, &writer, &error),
                   LC_OK);
  assert_int_equal(lc_collection_json_writer_write(writer, NULL, 0, &error),
                   LC_OK);
  for (size_t i = 0; i < strlen(text); ++i)
    assert_int_equal(
        lc_collection_json_writer_write(writer, text + i, 1, &error), LC_OK);
  assert_int_equal(lc_collection_json_writer_finish(writer, &written, &error),
                   LC_OK);
  assert_int_equal(lc_sink_memory_bytes(sink, &bytes, &length, &error), LC_OK);
  assert_true(length == written && length != 0);
  assert_int_equal(lc_collection_json_writer_write(writer, " ", 1, &error),
                   LC_ERR_INVALID);
  lc_collection_json_writer_close(writer);
  writer = NULL;
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(lc_collection_json_writer_open(sink, NULL, &writer, &error),
                   LC_OK);
  assert_int_equal(lc_collection_json_writer_write(writer, "{", 1, &error),
                   LC_OK);
  assert_int_equal(lc_collection_json_writer_finish(writer, &written, &error),
                   LC_ERR_INVALID);
  lc_collection_json_writer_close(writer);
  writer = NULL;
  lc_error_cleanup(&error);
  lc_error_init(&error);
  assert_int_equal(lc_collection_json_writer_open(sink, NULL, &writer, &error),
                   LC_OK);
  assert_int_equal(
      lc_collection_json_writer_write(writer, "{\"x\":\"partial", 13, &error),
      LC_OK);
  lc_collection_json_writer_close(writer); /* Abort without inventing EOF. */
  lc_sink_close(sink);
  lc_error_cleanup(&error);
}

typedef struct chunk_output {
  lc_sink pub;
  size_t bytes;
  size_t calls;
  size_t largest;
  int fail;
} chunk_output;

static int chunk_write(lc_sink *self, const void *bytes, size_t count,
                       lc_error *error) {
  chunk_output *out = (chunk_output *)self;
  (void)bytes;
  if (out->fail) {
    lc_error_set(error, LC_ERR_TIMEOUT, 0, "chunk backpressure failure", NULL,
                 NULL, NULL);
    return 0;
  }
  out->bytes += count;
  ++out->calls;
  if (count > out->largest)
    out->largest = count;
  return 1;
}

static void
test_incremental_output_is_bounded_and_flushed_before_eof(void **state) {
  lc_collection_json_writer *writer = NULL;
  chunk_output out = {0};
  lc_error error;
  lc_u64 written;
  char fragment[16384];
  (void)state;
  memset(fragment, 'x', sizeof(fragment));
  out.pub.write = chunk_write;
  lc_error_init(&error);
  assert_int_equal(
      lc_collection_json_writer_open(&out.pub, NULL, &writer, &error), LC_OK);
  assert_int_equal(
      lc_collection_json_writer_write(writer, "{\"s\":\"", 6, &error), LC_OK);
  for (unsigned int i = 0; i < 4; ++i) {
    assert_int_equal(lc_collection_json_writer_write(writer, fragment,
                                                     sizeof(fragment), &error),
                     LC_OK);
    /* Backpressure reaches the sink before EOF, including a short tail. */
    assert_true(out.bytes >= (i + 1U) * sizeof(fragment));
  }
  assert_true(out.largest <= 4096 && out.calls < 100);
  out.fail = 1;
  assert_int_equal(lc_collection_json_writer_write(writer, "\"}", 2, &error),
                   LC_ERR_TIMEOUT);
  assert_string_equal(error.message, "chunk backpressure failure");
  assert_int_equal(lc_collection_json_writer_finish(writer, &written, &error),
                   LC_ERR_INVALID);
  lc_collection_json_writer_close(writer);
  lc_error_cleanup(&error);
}

static void test_plain_codec_small_heap_and_frame_growth(void **state) {
  const struct {
    size_t bytes;
    size_t limit;
    int expected;
  } cases[] = {{0, 16384, LC_OK},    {1, 16384, LC_OK},
               {2048, 16384, LC_OK}, {2049, 0, LC_OK},
               {65536, 0, LC_OK},    {65537, 0, LC_OK},
               {131085, 0, LC_OK},   {2049, 16384, LC_ERR_NOMEM}};
  unsigned char bytes[2048], readback[2048];
  (void)state;
  for (size_t c = 0; c < sizeof(cases) / sizeof(cases[0]); ++c) {
    allocation_stats stats = {0};
    lc_allocator allocator = {tracked_allocate, tracked_reallocate,
                              tracked_free, &stats};
    lc_pouch_crypto_open_options options = {0};
    lc_pouch_crypto *crypto = NULL;
    lc_pouch_crypto_writer *writer = NULL;
    lc_error error;
    char path[] = CODEC_PREFIX "XXXXXX", *descriptor = NULL;
    uint64_t plain = 99, stored = 99;
    unsigned long crc = 0, expected_crc = crc32(0L, Z_NULL, 0);
    size_t offset = 0;
    int fd, rc = LC_OK;
    stats.reject_above = cases[c].limit;
    /* Keep an allocator-owning crypto object; this writer explicitly disables
     * its compression so the exercised output remains plain bytes. */
    options.compression_enabled = 1;
    lc_error_init(&error);
    fd = lc_test_tmp_mkstemp(path, CODEC_PREFIX);
    assert_true(fd >= 0);
    assert_int_equal(
        lc_pouch_crypto_open(&allocator, &options, &crypto, NULL, &error),
        LC_OK);
    assert_int_equal(
        lc_pouch_crypto_writer_open(crypto, NULL, fd, 0, &writer, &error),
        LC_OK);
    while (offset < cases[c].bytes) {
      size_t take = cases[c].bytes - offset;
      if (take > sizeof(bytes))
        take = sizeof(bytes);
      for (size_t i = 0; i < take; ++i)
        bytes[i] = (unsigned char)((offset + i) % 251);
      if (!lc_pouch_crypto_writer_sink(writer)->write(
              lc_pouch_crypto_writer_sink(writer), bytes, take, &error)) {
        rc = error.code;
        break;
      }
      expected_crc = crc32(expected_crc, bytes, (uInt)take);
      offset += take;
    }
    assert_int_equal(rc, cases[c].expected);
    if (rc != LC_OK)
      assert_false(lc_pouch_crypto_writer_sink(writer)->write(
          lc_pouch_crypto_writer_sink(writer), "x", 1, &error));
    assert_int_equal(lc_pouch_crypto_writer_finish(writer, &plain, &stored,
                                                   &crc, &descriptor, &error),
                     cases[c].expected);
    assert_null(descriptor);
    if (rc == LC_OK) {
      struct stat st;
      assert_true(plain == cases[c].bytes && stored == plain);
      assert_int_equal(crc, expected_crc);
      assert_int_equal(fstat(fd, &st), 0);
      assert_true((uint64_t)st.st_size == stored);
      for (offset = 0; offset < cases[c].bytes;) {
        size_t take = cases[c].bytes - offset;
        if (take > sizeof(readback))
          take = sizeof(readback);
        assert_int_equal(pread(fd, readback, take, (off_t)offset), take);
        for (size_t i = 0; i < take; ++i)
          assert_int_equal(readback[i], (unsigned char)((offset + i) % 251));
        offset += take;
      }
    } else {
      assert_true(plain == 0 && stored == 0);
    }
    lc_pouch_crypto_writer_close(writer);
    lc_pouch_crypto_close(crypto);
    assert_int_equal(stats.live, 0);
    assert_int_equal(close(fd), 0);
    lc_test_tmp_cleanup_path(path, CODEC_PREFIX);
    lc_error_cleanup(&error);
  }
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_plain_codec_small_heap_and_frame_growth),
      cmocka_unit_test(
          test_incremental_output_is_bounded_and_flushed_before_eof),
      cmocka_unit_test(test_incremental_json_finish_abort_and_terminal_states),
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

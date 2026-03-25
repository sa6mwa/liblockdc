#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <cmocka.h>

#include "lc/lc.h"
#include "lc_api_internal.h"

typedef struct fake_source {
  lc_source pub;
  size_t read_count;
  size_t reset_count;
  size_t close_count;
  int error_code;
} fake_source;

typedef struct fake_sink {
  lc_sink pub;
  size_t write_count;
  size_t close_count;
  int write_result;
  int error_code;
} fake_sink;

static size_t fake_source_read(lc_source *self, void *buffer, size_t capacity,
                               lc_error *error) {
  fake_source *source;

  (void)buffer;
  (void)capacity;
  source = (fake_source *)self;
  source->read_count += 1U;
  if (source->error_code != LC_OK) {
    lc_error_set(error, source->error_code, 0L, "fake source read failed", NULL,
                 NULL, NULL);
  }
  return 0U;
}

static int fake_source_reset(lc_source *self, lc_error *error) {
  fake_source *source;

  source = (fake_source *)self;
  source->reset_count += 1U;
  if (source->error_code != LC_OK) {
    return lc_error_set(error, source->error_code, 0L,
                        "fake source reset failed", NULL, NULL, NULL);
  }
  return LC_OK;
}

static void fake_source_close(lc_source *self) {
  fake_source *source;

  source = (fake_source *)self;
  source->close_count += 1U;
}

static int fake_sink_write(lc_sink *self, const void *bytes, size_t count,
                           lc_error *error) {
  fake_sink *sink;

  (void)bytes;
  (void)count;
  sink = (fake_sink *)self;
  sink->write_count += 1U;
  if (!sink->write_result && sink->error_code != LC_OK) {
    lc_error_set(error, sink->error_code, 0L, "fake sink write failed", NULL,
                 NULL, NULL);
  }
  return sink->write_result;
}

static void fake_sink_close(lc_sink *self) {
  fake_sink *sink;

  sink = (fake_sink *)self;
  sink->close_count += 1U;
}

static void test_copy_memory_source_to_memory_sink(void **state) {
  static const char payload[] = "hello stream";
  lc_source *source;
  lc_sink *sink;
  lc_error error;
  const void *bytes;
  size_t length;
  size_t written;
  int rc;

  (void)state;
  source = NULL;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  written = 0U;
  lc_error_init(&error);

  rc = lc_source_from_memory(payload, sizeof(payload) - 1U, &source, &error);
  assert_int_equal(rc, LC_OK);
  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_copy(source, sink, &written, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(written, sizeof(payload) - 1U);

  rc = lc_sink_memory_bytes(sink, &bytes, &length, &error);
  assert_int_equal(rc, LC_OK);
  assert_int_equal(length, sizeof(payload) - 1U);
  assert_memory_equal(bytes, payload, sizeof(payload) - 1U);

  lc_source_close(source);
  lc_sink_close(sink);
  lc_error_cleanup(&error);
}

static void test_copy_propagates_source_error_code(void **state) {
  fake_source source;
  fake_sink sink;
  lc_error error;
  int rc;

  (void)state;
  memset(&source, 0, sizeof(source));
  memset(&sink, 0, sizeof(sink));
  lc_error_init(&error);
  source.pub.read = fake_source_read;
  source.pub.reset = fake_source_reset;
  source.pub.close = fake_source_close;
  source.error_code = LC_ERR_PROTOCOL;
  sink.pub.write = fake_sink_write;
  sink.pub.close = fake_sink_close;
  sink.write_result = 1;

  rc = lc_copy(&source.pub, &sink.pub, NULL, &error);
  assert_int_equal(rc, LC_ERR_PROTOCOL);
  assert_int_equal(error.code, LC_ERR_PROTOCOL);
  assert_int_equal(source.read_count, 1U);
  assert_int_equal(sink.write_count, 0U);

  lc_error_cleanup(&error);
}

static void
test_copy_returns_transport_when_sink_write_fails_without_error(void **state) {
  static const char payload[] = "hello";
  lc_source *source;
  fake_sink sink;
  lc_error error;
  int rc;

  (void)state;
  source = NULL;
  memset(&sink, 0, sizeof(sink));
  lc_error_init(&error);
  rc = lc_source_from_memory(payload, sizeof(payload) - 1U, &source, &error);
  assert_int_equal(rc, LC_OK);
  sink.pub.write = fake_sink_write;
  sink.pub.close = fake_sink_close;
  sink.write_result = 0;

  rc = lc_copy(source, &sink.pub, NULL, &error);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_int_equal(sink.write_count, 1U);

  lc_source_close(source);
  lc_error_cleanup(&error);
}

static void test_json_from_string_can_reset_and_reread(void **state) {
  static const char json_text[] = "{\"kind\":\"example\",\"n\":42}";
  lc_json *json;
  lc_error error;
  char first[64];
  char second[64];
  size_t first_n;
  size_t second_n;
  size_t chunk;
  int rc;

  (void)state;
  json = NULL;
  first_n = 0U;
  second_n = 0U;
  lc_error_init(&error);

  rc = lc_json_from_string(json_text, &json, &error);
  assert_int_equal(rc, LC_OK);

  do {
    chunk = json->read(json, first + first_n, sizeof(first) - first_n, &error);
    first_n += chunk;
  } while (chunk > 0U);
  assert_int_equal(error.code, LC_OK);
  assert_int_equal(first_n, sizeof(json_text) - 1U);
  assert_memory_equal(first, json_text, sizeof(json_text) - 1U);

  rc = json->reset(json, &error);
  assert_int_equal(rc, LC_OK);

  do {
    chunk =
        json->read(json, second + second_n, sizeof(second) - second_n, &error);
    second_n += chunk;
  } while (chunk > 0U);
  assert_int_equal(error.code, LC_OK);
  assert_int_equal(second_n, sizeof(json_text) - 1U);
  assert_memory_equal(second, json_text, sizeof(json_text) - 1U);

  lc_json_close(json);
  lc_error_cleanup(&error);
}

static void
test_source_and_json_constructors_reject_invalid_arguments(void **state) {
  lc_error error;
  lc_source *source;
  lc_json *json;
  int rc;

  (void)state;
  source = NULL;
  json = NULL;
  lc_error_init(&error);

  rc = lc_source_from_memory("{}", 2U, NULL, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(error.code, LC_ERR_INVALID);

  lc_error_cleanup(&error);
  lc_error_init(&error);
  rc = lc_source_from_file(NULL, &source, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(error.code, LC_ERR_INVALID);

  lc_error_cleanup(&error);
  lc_error_init(&error);
  rc = lc_json_from_string(NULL, &json, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(error.code, LC_ERR_INVALID);

  lc_error_cleanup(&error);
  lc_error_init(&error);
  rc = lc_json_from_source(NULL, &json, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(error.code, LC_ERR_INVALID);

  lc_error_cleanup(&error);
}

static void test_file_constructors_report_transport_failures(void **state) {
  lc_source *source;
  lc_sink *sink;
  lc_error error;
  char template_dir[] = "/tmp/liblockdc-streams-XXXXXX";
  int rc;

  (void)state;
  source = NULL;
  sink = NULL;
  lc_error_init(&error);

  rc = lc_source_from_file("/definitely/missing/liblockdc.json", &source,
                           &error);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_int_equal(error.code, LC_ERR_TRANSPORT);
  assert_null(source);

  assert_non_null(mkdtemp(template_dir));
  lc_error_cleanup(&error);
  lc_error_init(&error);
  rc = lc_sink_to_file(template_dir, &sink, &error);
  assert_int_equal(rc, LC_ERR_TRANSPORT);
  assert_int_equal(error.code, LC_ERR_TRANSPORT);
  assert_null(sink);
  rmdir(template_dir);

  lc_error_cleanup(&error);
}

static void test_close_helpers_accept_null(void **state) {
  (void)state;
  lc_client_close(NULL);
  lc_lease_close(NULL);
  lc_message_close(NULL);
  lc_source_close(NULL);
  lc_sink_close(NULL);
  lc_json_close(NULL);
}

static void test_copy_rejects_null_endpoints(void **state) {
  lc_error error;
  int rc;

  (void)state;
  lc_error_init(&error);

  rc = lc_copy(NULL, NULL, NULL, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  assert_int_equal(error.code, LC_ERR_INVALID);

  lc_error_cleanup(&error);
}

static void test_sink_memory_bytes_rejects_invalid_arguments(void **state) {
  lc_sink *sink;
  lc_error error;
  const void *bytes;
  size_t length;
  int rc;

  (void)state;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  lc_error_init(&error);

  rc = lc_sink_to_memory(&sink, &error);
  assert_int_equal(rc, LC_OK);

  rc = lc_sink_memory_bytes(NULL, &bytes, &length, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  rc = lc_sink_memory_bytes(sink, NULL, &length, &error);
  assert_int_equal(rc, LC_ERR_INVALID);
  rc = lc_sink_memory_bytes(sink, &bytes, NULL, &error);
  assert_int_equal(rc, LC_ERR_INVALID);

  lc_sink_close(sink);
  lc_error_cleanup(&error);
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_copy_memory_source_to_memory_sink),
      cmocka_unit_test(test_copy_propagates_source_error_code),
      cmocka_unit_test(
          test_copy_returns_transport_when_sink_write_fails_without_error),
      cmocka_unit_test(test_json_from_string_can_reset_and_reread),
      cmocka_unit_test(
          test_source_and_json_constructors_reject_invalid_arguments),
      cmocka_unit_test(test_file_constructors_report_transport_failures),
      cmocka_unit_test(test_close_helpers_accept_null),
      cmocka_unit_test(test_copy_rejects_null_endpoints),
      cmocka_unit_test(test_sink_memory_bytes_rejects_invalid_arguments),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}

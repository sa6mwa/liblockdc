#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <cmocka.h>

#include "lc/lc.h"
#include "lc_intcompat.h"
#include "lc_mutate_stream.h"

typedef struct test_file_source {
  lc_source pub;
  const unsigned char *bytes;
  size_t length;
  size_t offset;
} test_file_source;

typedef struct test_file_store {
  const char *resolved_path;
  const unsigned char *bytes;
  size_t length;
  int opens;
} test_file_store;

typedef struct test_resolver_context {
  test_file_store *stores;
  size_t count;
  char last_path[512];
} test_resolver_context;

static size_t test_source_read(lc_source *self, void *buffer, size_t count,
                               lc_error *error) {
  test_file_source *source;
  size_t remaining;
  size_t nread;
  (void)error;

  source = (test_file_source *)self;
  remaining = source->length - source->offset;
  nread = count < remaining ? count : remaining;
  if (nread != 0U) {
    memcpy(buffer, source->bytes + source->offset, nread);
    source->offset += nread;
  }
  return nread;
}

static int test_source_reset(lc_source *self, lc_error *error) {
  test_file_source *source;
  (void)error;
  source = (test_file_source *)self;
  source->offset = 0U;
  return LC_OK;
}

static void test_source_close(lc_source *self) { free(self); }

static int test_resolver_open(void *context, const char *resolved_path,
                              lc_source **out, lc_error *error) {
  test_resolver_context *resolver;
  test_file_source *source;
  size_t i;

  resolver = (test_resolver_context *)context;
  snprintf(resolver->last_path, sizeof(resolver->last_path), "%s",
           resolved_path != NULL ? resolved_path : "");
  for (i = 0U; i < resolver->count; ++i) {
    if (strcmp(resolver->stores[i].resolved_path, resolved_path) == 0) {
      resolver->stores[i].opens += 1;
      source = (test_file_source *)calloc(1U, sizeof(*source));
      assert_non_null(source);
      source->pub.read = test_source_read;
      source->pub.reset = test_source_reset;
      source->pub.close = test_source_close;
      source->bytes = resolver->stores[i].bytes;
      source->length = resolver->stores[i].length;
      *out = &source->pub;
      return LC_OK;
    }
  }
  return lc_error_set(error, LC_ERR_INVALID, 0L, "unexpected resolver path",
                      resolved_path, NULL, NULL);
}

static FILE *tmp_with_text(const char *text) {
  FILE *fp;
  size_t length;

  fp = tmpfile();
  assert_non_null(fp);
  length = strlen(text);
  assert_int_equal(fwrite(text, 1U, length, fp), (int)length);
  assert_int_equal(fflush(fp), 0);
  rewind(fp);
  return fp;
}

static char *slurp_file(FILE *fp) {
  long length;
  char *text;

  assert_int_equal(fseek(fp, 0L, SEEK_END), 0);
  length = ftell(fp);
  assert_true(length >= 0L);
  assert_int_equal(fseek(fp, 0L, SEEK_SET), 0);
  text = (char *)malloc((size_t)length + 1U);
  assert_non_null(text);
  if (length != 0L) {
    assert_int_equal(fread(text, 1U, (size_t)length, fp), (int)length);
  }
  text[length] = '\0';
  return text;
}

static void
test_mutation_plan_streams_textfile_into_missing_path(void **state) {
  const char *exprs[1];
  const unsigned char payload[] = "hello\n\"quoted\"";
  test_file_store store;
  test_resolver_context resolver;
  lc_file_value_resolver file_resolver;
  lc_mutation_parse_options options;
  lc_mutation_plan *plan;
  lc_error error;
  FILE *input;
  FILE *output;
  char *json_text;
  int rc;

  (void)state;
  exprs[0] = "textfile:/meta/blob=blob.txt";
  memset(&store, 0, sizeof(store));
  store.resolved_path = "/virtual/blob.txt";
  store.bytes = payload;
  store.length = sizeof(payload) - 1U;
  memset(&resolver, 0, sizeof(resolver));
  resolver.stores = &store;
  resolver.count = 1U;
  memset(&file_resolver, 0, sizeof(file_resolver));
  file_resolver.open = test_resolver_open;
  file_resolver.context = &resolver;
  memset(&options, 0, sizeof(options));
  options.file_value_base_dir = "/virtual";
  options.file_value_resolver = &file_resolver;
  plan = NULL;
  lc_error_init(&error);

  rc = lc_mutation_plan_build(exprs, 1U, &options, &plan, &error);
  assert_int_equal(rc, LC_OK);
  input = tmp_with_text("{\"id\":\"a\"}");
  output = NULL;
  rc = lc_mutation_plan_apply(plan, input, &output, &error);
  assert_int_equal(rc, LC_OK);
  json_text = slurp_file(output);
  assert_string_equal(
      json_text,
      "{\"id\":\"a\",\"meta\":{\"blob\":\"hello\\n\\\"quoted\\\"\"}}");
  assert_int_equal(store.opens, 1);

  free(json_text);
  fclose(output);
  fclose(input);
  lc_mutation_plan_close(plan);
  lc_error_cleanup(&error);
}

static void test_mutation_plan_normalizes_now_time_literal(void **state) {
  const char *exprs[1];
  lc_mutation_parse_options options;
  lc_mutation_plan *plan;
  lc_error error;
  FILE *input;
  FILE *output;
  char *json_text;
  int rc;

  (void)state;
  exprs[0] = "time:/ts=NOW";
  memset(&options, 0, sizeof(options));
  options.now.tv_sec = 1700000000;
  options.now.tv_nsec = 123456789L;
  options.has_now = 1;
  plan = NULL;
  lc_error_init(&error);

  rc = lc_mutation_plan_build(exprs, 1U, &options, &plan, &error);
  assert_int_equal(rc, LC_OK);
  input = tmp_with_text("{}");
  output = NULL;
  rc = lc_mutation_plan_apply(plan, input, &output, &error);
  assert_int_equal(rc, LC_OK);
  json_text = slurp_file(output);
  assert_string_equal(json_text, "{\"ts\":\"2023-11-14T22:13:20.123456789Z\"}");

  free(json_text);
  fclose(output);
  fclose(input);
  lc_mutation_plan_close(plan);
  lc_error_cleanup(&error);
}

static void test_mutation_plan_normalizes_rfc3339_offset(void **state) {
  const char *exprs[1];
  lc_mutation_parse_options options;
  lc_mutation_plan *plan;
  lc_error error;
  FILE *input;
  FILE *output;
  char *json_text;
  int rc;

  (void)state;
  exprs[0] = "time:/ts=2024-01-02T03:04:05+02:30";
  memset(&options, 0, sizeof(options));
  plan = NULL;
  lc_error_init(&error);

  rc = lc_mutation_plan_build(exprs, 1U, &options, &plan, &error);
  assert_int_equal(rc, LC_OK);
  input = tmp_with_text("{}");
  output = NULL;
  rc = lc_mutation_plan_apply(plan, input, &output, &error);
  assert_int_equal(rc, LC_OK);
  json_text = slurp_file(output);
  assert_string_equal(json_text, "{\"ts\":\"2024-01-02T00:34:05Z\"}");

  free(json_text);
  fclose(output);
  fclose(input);
  lc_mutation_plan_close(plan);
  lc_error_cleanup(&error);
}

static void test_mutation_plan_normalizes_rfc3339nano_literal(void **state) {
  const char *exprs[1];
  lc_mutation_parse_options options;
  lc_mutation_plan *plan;
  lc_error error;
  FILE *input;
  FILE *output;
  char *json_text;
  int rc;

  (void)state;
  exprs[0] = "time:/ts=2024-01-02T03:04:05.120300000-01:00";
  memset(&options, 0, sizeof(options));
  plan = NULL;
  lc_error_init(&error);

  rc = lc_mutation_plan_build(exprs, 1U, &options, &plan, &error);
  assert_int_equal(rc, LC_OK);
  input = tmp_with_text("{}");
  output = NULL;
  rc = lc_mutation_plan_apply(plan, input, &output, &error);
  assert_int_equal(rc, LC_OK);
  json_text = slurp_file(output);
  assert_string_equal(json_text, "{\"ts\":\"2024-01-02T04:04:05.1203Z\"}");

  free(json_text);
  fclose(output);
  fclose(input);
  lc_mutation_plan_close(plan);
  lc_error_cleanup(&error);
}

static void test_mutation_plan_rejects_invalid_time_literal(void **state) {
  const char *exprs[1];
  lc_mutation_parse_options options;
  lc_mutation_plan *plan;
  lc_error error;
  int rc;

  (void)state;
  exprs[0] = "time:/ts=2024-13-02T03:04:05Z";
  memset(&options, 0, sizeof(options));
  plan = NULL;
  lc_error_init(&error);

  rc = lc_mutation_plan_build(exprs, 1U, &options, &plan, &error);
  assert_int_not_equal(rc, LC_OK);
  assert_null(plan);
  assert_non_null(error.message);

  lc_error_cleanup(&error);
}

static void test_mutation_plan_increments_and_removes_fields(void **state) {
  const char *exprs[2];
  lc_mutation_parse_options options;
  lc_mutation_plan *plan;
  lc_error error;
  FILE *input;
  FILE *output;
  char *json_text;
  int rc;

  (void)state;
  exprs[0] = "/counter++";
  exprs[1] = "rm:/drop";
  memset(&options, 0, sizeof(options));
  plan = NULL;
  lc_error_init(&error);

  rc = lc_mutation_plan_build(exprs, 2U, &options, &plan, &error);
  assert_int_equal(rc, LC_OK);
  input = tmp_with_text("{\"counter\":1,\"drop\":2,\"keep\":3}");
  output = NULL;
  rc = lc_mutation_plan_apply(plan, input, &output, &error);
  assert_int_equal(rc, LC_OK);
  json_text = slurp_file(output);
  assert_string_equal(json_text, "{\"counter\":2,\"keep\":3}");

  free(json_text);
  fclose(output);
  fclose(input);
  lc_mutation_plan_close(plan);
  lc_error_cleanup(&error);
}

static void test_mutation_plan_rejects_missing_array_path(void **state) {
  const char *exprs[1];
  lc_mutation_parse_options options;
  lc_mutation_plan *plan;
  lc_error error;
  FILE *input;
  FILE *output;
  int rc;

  (void)state;
  exprs[0] = "/items/0=\"x\"";
  memset(&options, 0, sizeof(options));
  plan = NULL;
  lc_error_init(&error);

  rc = lc_mutation_plan_build(exprs, 1U, &options, &plan, &error);
  assert_int_equal(rc, LC_OK);
  input = tmp_with_text("{}");
  output = NULL;
  rc = lc_mutation_plan_apply(plan, input, &output, &error);
  assert_int_not_equal(rc, LC_OK);
  assert_non_null(error.message);
  assert_non_null(strstr(error.message, "missing array paths"));
  assert_null(output);

  fclose(input);
  lc_mutation_plan_close(plan);
  lc_error_cleanup(&error);
}

static void
test_mutation_plan_rejects_truncated_object_key_without_crashing(void **state) {
  const char *exprs[1];
  lc_mutation_parse_options options;
  lc_mutation_plan *plan;
  lc_error error;
  FILE *input;
  FILE *output;
  int rc;

  (void)state;
  exprs[0] = "time:/ts=NOW";
  memset(&options, 0, sizeof(options));
  options.now.tv_sec = 1700000000;
  options.now.tv_nsec = 123456789L;
  options.has_now = 1;
  plan = NULL;
  lc_error_init(&error);

  rc = lc_mutation_plan_build(exprs, 1U, &options, &plan, &error);
  assert_int_equal(rc, LC_OK);
  input = tmp_with_text("{\"broken");
  output = NULL;
  rc = lc_mutation_plan_apply(plan, input, &output, &error);
  assert_int_not_equal(rc, LC_OK);
  assert_null(output);
  assert_non_null(error.message);

  fclose(input);
  lc_mutation_plan_close(plan);
  lc_error_cleanup(&error);
}

static void test_mutation_plan_streams_base64file_value(void **state) {
  const char *exprs[1];
  const unsigned char payload[] = {0x00, 0x01, 0x02, 'a'};
  test_file_store store;
  test_resolver_context resolver;
  lc_file_value_resolver file_resolver;
  lc_mutation_parse_options options;
  lc_mutation_plan *plan;
  lc_error error;
  FILE *input;
  FILE *output;
  char *json_text;
  int rc;

  (void)state;
  exprs[0] = "base64file:/payload=blob.bin";
  memset(&store, 0, sizeof(store));
  store.resolved_path = "/virtual/blob.bin";
  store.bytes = payload;
  store.length = sizeof(payload);
  memset(&resolver, 0, sizeof(resolver));
  resolver.stores = &store;
  resolver.count = 1U;
  memset(&file_resolver, 0, sizeof(file_resolver));
  file_resolver.open = test_resolver_open;
  file_resolver.context = &resolver;
  memset(&options, 0, sizeof(options));
  options.file_value_base_dir = "/virtual";
  options.file_value_resolver = &file_resolver;
  plan = NULL;
  lc_error_init(&error);

  rc = lc_mutation_plan_build(exprs, 1U, &options, &plan, &error);
  assert_int_equal(rc, LC_OK);
  input = tmp_with_text("{\"payload\":\"old\"}");
  output = NULL;
  rc = lc_mutation_plan_apply(plan, input, &output, &error);
  assert_int_equal(rc, LC_OK);
  json_text = slurp_file(output);
  assert_string_equal(json_text, "{\"payload\":\"AAECYQ==\"}");
  assert_int_equal(store.opens, 1);

  free(json_text);
  fclose(output);
  fclose(input);
  lc_mutation_plan_close(plan);
  lc_error_cleanup(&error);
}

static void
test_mutation_plan_auto_file_mode_streams_in_one_pass(void **state) {
  const char *text_exprs[1];
  const char *bin_exprs[1];
  const unsigned char text_payload[] = "hello world";
  const unsigned char bin_payload[] = {0x00, 0xff, 0x01};
  test_file_store stores[2];
  test_resolver_context resolver;
  lc_file_value_resolver file_resolver;
  lc_mutation_parse_options options;
  lc_mutation_plan *plan;
  lc_error error;
  FILE *input;
  FILE *output;
  char *json_text;
  int rc;

  (void)state;
  text_exprs[0] = "file:/payload=text.txt";
  bin_exprs[0] = "file:/payload=blob.bin";
  memset(stores, 0, sizeof(stores));
  stores[0].resolved_path = "/virtual/text.txt";
  stores[0].bytes = text_payload;
  stores[0].length = sizeof(text_payload) - 1U;
  stores[1].resolved_path = "/virtual/blob.bin";
  stores[1].bytes = bin_payload;
  stores[1].length = sizeof(bin_payload);
  memset(&resolver, 0, sizeof(resolver));
  resolver.stores = stores;
  resolver.count = 2U;
  memset(&file_resolver, 0, sizeof(file_resolver));
  file_resolver.open = test_resolver_open;
  file_resolver.context = &resolver;
  memset(&options, 0, sizeof(options));
  options.file_value_base_dir = "/virtual";
  options.file_value_resolver = &file_resolver;
  lc_error_init(&error);

  plan = NULL;
  rc = lc_mutation_plan_build(text_exprs, 1U, &options, &plan, &error);
  assert_int_equal(rc, LC_OK);
  input = tmp_with_text("{\"payload\":\"old\"}");
  output = NULL;
  rc = lc_mutation_plan_apply(plan, input, &output, &error);
  assert_int_equal(rc, LC_OK);
  json_text = slurp_file(output);
  assert_string_equal(json_text, "{\"payload\":\"hello world\"}");
  free(json_text);
  fclose(output);
  fclose(input);
  lc_mutation_plan_close(plan);

  plan = NULL;
  rc = lc_mutation_plan_build(bin_exprs, 1U, &options, &plan, &error);
  assert_int_equal(rc, LC_OK);
  input = tmp_with_text("{\"payload\":\"old\"}");
  output = NULL;
  rc = lc_mutation_plan_apply(plan, input, &output, &error);
  assert_int_equal(rc, LC_OK);
  json_text = slurp_file(output);
  assert_string_equal(json_text, "{\"payload\":\"AP8B\"}");
  assert_int_equal(stores[0].opens, 1);
  assert_int_equal(stores[1].opens, 1);
  free(json_text);
  fclose(output);
  fclose(input);
  lc_mutation_plan_close(plan);
  lc_error_cleanup(&error);
}

static void test_mutation_plan_rejects_invalid_utf8_textfile(void **state) {
  const char *exprs[1];
  const unsigned char payload[] = {0xff, 0xfe, 'x'};
  test_file_store store;
  test_resolver_context resolver;
  lc_file_value_resolver file_resolver;
  lc_mutation_parse_options options;
  lc_mutation_plan *plan;
  lc_error error;
  FILE *input;
  FILE *output;
  int rc;

  (void)state;
  exprs[0] = "textfile:/payload=blob.bin";
  memset(&store, 0, sizeof(store));
  store.resolved_path = "/virtual/blob.bin";
  store.bytes = payload;
  store.length = sizeof(payload);
  memset(&resolver, 0, sizeof(resolver));
  resolver.stores = &store;
  resolver.count = 1U;
  memset(&file_resolver, 0, sizeof(file_resolver));
  file_resolver.open = test_resolver_open;
  file_resolver.context = &resolver;
  memset(&options, 0, sizeof(options));
  options.file_value_base_dir = "/virtual";
  options.file_value_resolver = &file_resolver;
  plan = NULL;
  lc_error_init(&error);

  rc = lc_mutation_plan_build(exprs, 1U, &options, &plan, &error);
  assert_int_equal(rc, LC_OK);
  input = tmp_with_text("{}");
  output = NULL;
  rc = lc_mutation_plan_apply(plan, input, &output, &error);
  assert_int_not_equal(rc, LC_OK);
  assert_non_null(error.message);
  assert_true(strstr(error.message, "invalid UTF-8") != NULL);
  assert_null(output);
  assert_int_equal(store.opens, 1);

  fclose(input);
  lc_mutation_plan_close(plan);
  lc_error_cleanup(&error);
}

static void test_mutation_plan_expands_home_prefix(void **state) {
  const char *exprs[1];
  const unsigned char payload[] = "homedir";
  test_file_store store;
  test_resolver_context resolver;
  lc_file_value_resolver file_resolver;
  lc_mutation_parse_options options;
  lc_mutation_plan *plan;
  lc_error error;
  FILE *input;
  FILE *output;
  char *json_text;
  char home_dir[256];
  char resolved_path[320];
  int rc;

  (void)state;
  assert_non_null(getcwd(home_dir, sizeof(home_dir)));
  snprintf(resolved_path, sizeof(resolved_path), "%s/blob.txt", home_dir);
  assert_int_equal(setenv("HOME", home_dir, 1), 0);
  exprs[0] = "textfile:/payload=~/blob.txt";
  memset(&store, 0, sizeof(store));
  store.resolved_path = resolved_path;
  store.bytes = payload;
  store.length = sizeof(payload) - 1U;
  memset(&resolver, 0, sizeof(resolver));
  resolver.stores = &store;
  resolver.count = 1U;
  memset(&file_resolver, 0, sizeof(file_resolver));
  file_resolver.open = test_resolver_open;
  file_resolver.context = &resolver;
  memset(&options, 0, sizeof(options));
  options.file_value_resolver = &file_resolver;
  lc_error_init(&error);
  plan = NULL;

  rc = lc_mutation_plan_build(exprs, 1U, &options, &plan, &error);
  assert_int_equal(rc, LC_OK);
  input = tmp_with_text("{}");
  output = NULL;
  rc = lc_mutation_plan_apply(plan, input, &output, &error);
  assert_int_equal(rc, LC_OK);
  json_text = slurp_file(output);
  assert_string_equal(json_text, "{\"payload\":\"homedir\"}");
  assert_string_equal(resolver.last_path, resolved_path);

  free(json_text);
  fclose(output);
  fclose(input);
  lc_mutation_plan_close(plan);
  lc_error_cleanup(&error);
}

static void test_intcompat_roundtrips_large_decimal_literal(void **state) {
  lc_i64 value;
  char text[64];
  int rc;

  (void)state;
  assert_true(lc_i64_parse_base10("9223372036854775807", &value));
  rc = lc_i64_format_base10(value, text, sizeof(text));
  assert_true(rc > 0);
  assert_string_equal(text, "9223372036854775807");
  assert_true(lc_i64_parse_base10("-9223372036854775808", &value));
  rc = lc_i64_format_base10(value, text, sizeof(text));
  assert_true(rc > 0);
  assert_string_equal(text, "-9223372036854775808");
}

static void test_intcompat_rejects_out_of_range_narrowing(void **state) {
  int ivalue;
  long lvalue;
  unsigned long uvalue;

  (void)state;
  assert_false(lc_parse_int_base10_checked("2147483648", &ivalue));
  assert_false(lc_parse_long_base10_checked("9223372036854775808", &lvalue));
  assert_false(lc_parse_ulong_base10_checked("-1", &uvalue));
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_mutation_plan_streams_textfile_into_missing_path),
      cmocka_unit_test(test_mutation_plan_normalizes_now_time_literal),
      cmocka_unit_test(test_mutation_plan_normalizes_rfc3339_offset),
      cmocka_unit_test(test_mutation_plan_normalizes_rfc3339nano_literal),
      cmocka_unit_test(test_mutation_plan_rejects_invalid_time_literal),
      cmocka_unit_test(test_mutation_plan_increments_and_removes_fields),
      cmocka_unit_test(test_mutation_plan_rejects_missing_array_path),
      cmocka_unit_test(
          test_mutation_plan_rejects_truncated_object_key_without_crashing),
      cmocka_unit_test(test_mutation_plan_streams_base64file_value),
      cmocka_unit_test(test_mutation_plan_auto_file_mode_streams_in_one_pass),
      cmocka_unit_test(test_mutation_plan_rejects_invalid_utf8_textfile),
      cmocka_unit_test(test_mutation_plan_expands_home_prefix),
      cmocka_unit_test(test_intcompat_roundtrips_large_decimal_literal),
      cmocka_unit_test(test_intcompat_rejects_out_of_range_narrowing),
  };

  return cmocka_run_group_tests(tests, NULL, NULL);
}

#include <cmocka.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "lc_api_internal.h"
#include "lc_pouch_query_terms.h"

typedef struct input {
  lc_source pub;
  const char *text;
  size_t offset, length, chunk, fail_at, requested;
  size_t repeated;
  int reset, closed, overread, many_fields;
} input;

static size_t input_read(lc_source *self, void *bytes, size_t capacity,
                         lc_error *error) {
  input *in = (input *)self;
  size_t count = in->length - in->offset;
  if (capacity > in->requested)
    in->requested = capacity;
  if (in->overread)
    return capacity + 1;
  if (in->fail_at && in->offset >= in->fail_at) {
    lc_error_set(error, LC_ERR_TIMEOUT, 0, "term source timed out", NULL, NULL,
                 NULL);
    return 0;
  }
  if (count > capacity)
    count = capacity;
  if (in->chunk && count > in->chunk)
    count = in->chunk;
  for (size_t i = 0; i < count; ++i) {
    size_t offset = in->offset + i;
    ((char *)bytes)[i] =
        in->many_fields ? (offset == 0 ? '{'
                           : offset < 1 + in->repeated * 9
                               ? "\"a\":true,"[(offset - 1) % 9]
                               : "\"z\":null}"[offset - 1 - in->repeated * 9])
        : !in->repeated ? in->text[offset]
        : offset < 6    ? "{\"s\":\""[offset]
        : offset < 6 + in->repeated ? 'a'
                                    : "\"}"[offset - 6 - in->repeated];
  }
  in->offset += count;
  return count;
}

static int input_reset(lc_source *self, lc_error *error) {
  (void)error;
  ++((input *)self)->reset;
  return LC_ERR_INVALID;
}
static void input_close(lc_source *self) { ++((input *)self)->closed; }
static void input_init(input *in, const char *json, size_t chunk) {
  memset(in, 0, sizeof(*in));
  in->pub.read = input_read;
  in->pub.reset = input_reset;
  in->pub.close = input_close;
  in->text = json;
  in->length = json ? strlen(json) : 0;
  in->chunk = chunk;
}

typedef struct expected {
  const char *field;
  const char *value;
  char type;
  int seen;
} expected;

typedef struct terms {
  expected *expected;
  size_t count, values, presence, calls, fail_at;
  int fail_without_error;
  char hash[25];
} terms;

static void decode(const char *hex, char *out, size_t capacity,
                   size_t *length) {
  size_t count = strcmp(hex, "-") == 0 ? 0 : strlen(hex) / 2;
  const char *digits = "0123456789abcdef";
  assert_true(count < capacity);
  for (size_t i = 0; i < count; ++i) {
    const char *hi = strchr(digits, hex[i * 2]);
    const char *lo = strchr(digits, hex[i * 2 + 1]);
    assert_non_null(hi);
    assert_non_null(lo);
    out[i] = (char)((hi - digits) * 16 + (lo - digits));
  }
  out[count] = 0;
  *length = count;
}

static int visit(void *context, const lc_pouch_index_term_key *term,
                 int presence, lc_error *error) {
  terms *out = context;
  char field[512], value[512];
  size_t field_len, value_len;
  ++out->calls;
  if (out->calls == out->fail_at)
    return out->fail_without_error
               ? LC_ERR_TIMEOUT
               : lc_error_set(error, LC_ERR_TIMEOUT, 0,
                              "term callback timed out", NULL, NULL, NULL);
  decode(term->field_hex, field, sizeof(field), &field_len);
  if (presence) {
    assert_int_equal(term->value_type, 's');
    assert_string_equal(term->value_hex, "-");
    ++out->presence;
    return LC_OK;
  }
  ++out->values;
  if (term->value_type == 'h') {
    assert_string_equal(field, "/s");
    assert_int_equal(strlen(term->value_hex), 24);
    strcpy(out->hash, term->value_hex);
    return LC_OK;
  }
  decode(term->value_hex, value, sizeof(value), &value_len);
  for (size_t i = 0; i < out->count; ++i) {
    expected *want = out->expected + i;
    if (strcmp(field, want->field) == 0 && term->value_type == want->type &&
        value_len == strlen(want->value) &&
        memcmp(value, want->value, value_len) == 0) {
      ++want->seen;
      return LC_OK;
    }
  }
  if (out->expected)
    fail_msg("unexpected term %s %c %s", field, term->value_type,
             term->value_hex);
  return LC_OK;
}

static void test_chunk_boundaries_typed_values_and_array_paths(void **state) {
  const char *json =
      "{\"s\":\"\",\"n\":1e1,\"zero\":-0,\"b\":true,\"z\":null,"
      "\"a\":[\"x\",false],\"o\":{\"0\":\"key\"},\"a/b~\":\"escape\"}";
  expected wanted[] = {{"/s", "", 's', 0},           {"/n", "10", 'n', 0},
                       {"/zero", "0", 'n', 0},       {"/b", "true", 'b', 0},
                       {"/z", "null", 'z', 0},       {"/a/0", "x", 's', 0},
                       {"/a/[]", "x", 's', 0},       {"/a/1", "false", 'b', 0},
                       {"/a/[]", "false", 'b', 0},   {"/o/0", "key", 's', 0},
                       {"/a~1b~0", "escape", 's', 0}};
  (void)state;
  for (size_t chunk = 1; chunk <= strlen(json); ++chunk) {
    input in;
    terms out = {0};
    lc_error error;
    lc_error_init(&error);
    input_init(&in, json, chunk);
    out.expected = wanted;
    out.count = sizeof(wanted) / sizeof(wanted[0]);
    for (size_t i = 0; i < out.count; ++i)
      wanted[i].seen = 0;
    assert_int_equal(
        lc_pouch_query_terms_visit(&in.pub, NULL, visit, &out, &error), LC_OK);
    assert_int_equal(out.values, out.count);
    for (size_t i = 0; i < out.count; ++i)
      assert_int_equal(wanted[i].seen, 1);
    assert_true(out.presence >= out.values && in.offset == in.length);
    assert_true(in.reset == 0 && in.closed == 0);
    lc_error_cleanup(&error);
  }
}

typedef union block {
  long double align;
  void *pointer;
  size_t size;
} block;
typedef struct allocation_stats {
  size_t live, peak, calls, fail_at;
} allocation_stats;
static void *allocate(void *context, size_t size) {
  allocation_stats *stats = context;
  block *p;
  if (++stats->calls == stats->fail_at)
    return NULL;
  p = malloc(sizeof(*p) + size);
  if (!p)
    return NULL;
  p->size = size;
  stats->live += size;
  if (stats->live > stats->peak)
    stats->peak = stats->live;
  return p + 1;
}
static void release(void *context, void *ptr) {
  allocation_stats *stats = context;
  block *p;
  if (!ptr)
    return;
  p = (block *)ptr - 1;
  stats->live -= p->size;
  free(p);
}
static void *resize(void *context, void *ptr, size_t size) {
  void *next = allocate(context, size);
  if (!next)
    return NULL;
  if (ptr) {
    size_t old = ((block *)ptr - 1)->size;
    memcpy(next, ptr, old < size ? old : size);
    release(context, ptr);
  }
  return next;
}
static lc_allocator allocator_for(allocation_stats *stats) {
  lc_allocator result = {0};
  result.context = stats;
  result.malloc_fn = allocate;
  result.realloc_fn = resize;
  result.free_fn = release;
  return result;
}

static void test_large_string_is_hashed_with_bounded_storage(void **state) {
  char hash[25] = "";
  size_t peak = 0;
  (void)state;
  for (size_t size = 1024; size <= 4 * 1024 * 1024; size *= 4) {
    input in;
    terms out = {0};
    allocation_stats stats = {0};
    lc_allocator allocator = allocator_for(&stats);
    lc_error error;
    lc_error_init(&error);
    input_init(&in, NULL, 4093);
    in.repeated = size;
    in.length = size + 8;
    assert_int_equal(
        lc_pouch_query_terms_visit(&in.pub, &allocator, visit, &out, &error),
        LC_OK);
    assert_int_equal(out.values, 1);
    assert_int_equal(stats.live, 0);
    assert_true(stats.peak < 8192 && in.requested <= 65536);
    if (!peak)
      peak = stats.peak;
    assert_int_equal(peak, stats.peak);
    assert_true(strcmp(hash, out.hash) != 0);
    strcpy(hash, out.hash);
    lc_error_cleanup(&error);
  }
}

static void test_many_terms_do_not_accumulate_in_extractor(void **state) {
  size_t peak = 0;
  (void)state;
  for (size_t count = 100; count <= 10000; count *= 10) {
    input in;
    terms out = {0};
    allocation_stats stats = {0};
    lc_allocator allocator = allocator_for(&stats);
    lc_error error;
    lc_error_init(&error);
    input_init(&in, NULL, 4093);
    in.many_fields = 1;
    in.repeated = count;
    in.length = 10 + count * 9;
    assert_int_equal(
        lc_pouch_query_terms_visit(&in.pub, &allocator, visit, &out, &error),
        LC_OK);
    assert_int_equal(out.values, count + 1);
    assert_int_equal(stats.live, 0);
    if (!peak)
      peak = stats.peak;
    assert_int_equal(peak, stats.peak);
    lc_error_cleanup(&error);
  }
}

static void
test_callback_and_source_failures_preserve_errors_and_ownership(void **state) {
  (void)state;
  for (int bare = 0; bare <= 1; ++bare) {
    input in;
    terms out = {0};
    lc_error error;
    lc_error_init(&error);
    input_init(&in, "{\"a\":1,\"b\":2}", 1);
    out.fail_at = 2;
    out.fail_without_error = bare;
    assert_int_equal(
        lc_pouch_query_terms_visit(&in.pub, NULL, visit, &out, &error),
        LC_ERR_TIMEOUT);
    assert_int_equal(error.code, LC_ERR_TIMEOUT);
    if (!bare)
      assert_string_equal(error.message, "term callback timed out");
    assert_true(in.offset < in.length && in.reset == 0 && in.closed == 0);
    lc_error_cleanup(&error);
    lc_error_init(&error);
    input_init(&in, "{\"a\":1}", 1);
    in.fail_at = 4;
    memset(&out, 0, sizeof(out));
    assert_int_equal(
        lc_pouch_query_terms_visit(&in.pub, NULL, visit, &out, &error),
        LC_ERR_TIMEOUT);
    assert_string_equal(error.message, "term source timed out");
    assert_true(in.reset == 0 && in.closed == 0);
    lc_error_cleanup(&error);
  }
}

static void
test_malformed_input_and_overreporting_source_are_errors(void **state) {
  const char *invalid[] = {"{", "{\"s\":\"unterminated", "{\"n\":01}",
                           "{} extra", "{} {}"};
  (void)state;
  for (size_t i = 0; i <= sizeof(invalid) / sizeof(invalid[0]); ++i) {
    input in;
    terms out = {0};
    lc_error error;
    lc_error_init(&error);
    input_init(&in, i == 5 ? "{}" : invalid[i], 1);
    in.overread = i == 5;
    assert_int_equal(
        lc_pouch_query_terms_visit(&in.pub, NULL, visit, &out, &error),
        in.overread ? LC_ERR_INVALID : LC_ERR_PROTOCOL);
    lc_error_cleanup(&error);
  }
}

static void
test_allocation_failure_cleans_every_extraction_boundary(void **state) {
  const char *json = "{\"a\":[\"short\",{\"b\":12}],\"z\":null}";
  size_t calls = 0;
  (void)state;
  for (size_t fail_at = 0; fail_at <= calls; ++fail_at) {
    input in;
    terms out = {0};
    allocation_stats stats = {0};
    lc_allocator allocator = allocator_for(&stats);
    lc_error error;
    lc_error_init(&error);
    input_init(&in, json, 1);
    stats.fail_at = fail_at;
    assert_int_equal(
        lc_pouch_query_terms_visit(&in.pub, &allocator, visit, &out, &error),
        fail_at ? LC_ERR_NOMEM : LC_OK);
    if (!fail_at)
      calls = stats.calls;
    assert_int_equal(stats.live, 0);
    lc_error_cleanup(&error);
  }
}

int main(void) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(test_chunk_boundaries_typed_values_and_array_paths),
      cmocka_unit_test(test_large_string_is_hashed_with_bounded_storage),
      cmocka_unit_test(test_many_terms_do_not_accumulate_in_extractor),
      cmocka_unit_test(
          test_callback_and_source_failures_preserve_errors_and_ownership),
      cmocka_unit_test(
          test_malformed_input_and_overreporting_source_are_errors),
      cmocka_unit_test(
          test_allocation_failure_cleans_every_extraction_boundary),
  };
  if (lc_allocator_tracking_configure(1, NULL) != LC_OK)
    return 1;
  return cmocka_run_group_tests(tests, NULL, NULL);
}

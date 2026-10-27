#include <cmocka.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "../support/lc_test_cases.h"

#include "../support/lc_test_tmp.h"
#include "lc_collection_internal.h"
#include "lc_collection_query.h"
#include "lc_pouch_crypto.h"
#include "lc_pouch_path.h"
#include "lc_pouch_record.h"

#define PREFIX LC_COLLECTION_TMP_PARENT "/collections-api-"

typedef struct fixture {
  char root[256];
  char endpoint[512];
  lc_client *client;
  lc_collection *a;
  lc_collection *b;
  lc_source *source;
  lc_sink *sink;
  lc_collection_read_res read;
  lc_collection_index query_index;
  lc_error error;
  int owned_fd;
  int index_queries;
  char *crypto_key;
  lc_pouch_unix_seconds now;
} fixture;

static void check(fixture *f, int rc) {
  if (rc != LC_OK)
    print_error("collection failure %d: %s [%s]\n", rc,
                f->error.message != NULL ? f->error.message : "",
                f->error.server_code != NULL ? f->error.server_code : "");
  assert_int_equal(rc, LC_OK);
}

static void open_client(fixture *f, const char *key, int compressed,
                        int durable) {
  const char *endpoints[] = {f->endpoint};
  lc_client_config config;
  lc_pouch_settings settings;
  lc_client_config_init(&config);
  lc_pouch_settings_init(&settings);
  settings.set_mask =
      LC_POUCH_SETTING_SINGLE_WRITER | LC_POUCH_SETTING_BACKGROUND_COMPACTION |
      LC_POUCH_SETTING_QUERY_INDEXING | LC_POUCH_SETTING_SEGMENT_TARGET_BYTES |
      LC_POUCH_SETTING_COMPRESSION | LC_POUCH_SETTING_DURABLE_SYNC;
  settings.single_writer = 0;
  settings.query_indexing_enabled = f->index_queries;
  settings.segment_target_bytes = 4096U;
  settings.compression = compressed ? "zlib" : "none";
  settings.durable_sync = durable != 0;
  if (key != NULL) {
    settings.set_mask |= LC_POUCH_SETTING_CRYPTO_KEY;
    settings.crypto_key = key;
  }
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "journal";
  config.pouch_settings = &settings;
  check(f, lc_client_open(&config, &f->client, &f->error));
}

static void close_handles(fixture *f) {
  lc_collection_close(f->a);
  f->a = NULL;
  lc_collection_close(f->b);
  f->b = NULL;
  lc_client_close(f->client);
  f->client = NULL;
}

static int setup(void **state) {
  char template_path[] = PREFIX "XXXXXX";
  fixture *f = calloc(1U, sizeof(*f));
  assert_non_null(f);
  lc_error_init(&f->error);
  f->owned_fd = -1;
  assert_true(
      lc_test_tmp_mkdtemp(template_path, f->root, sizeof(f->root), PREFIX));
  assert_true(
      snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s", f->root) > 0);
  *state = f;
  open_client(f, NULL, 0, 0);
  ((lc_client_handle *)f->client)->pouch->compaction_delete_grace_seconds = 0U;
  return 0;
}

static int teardown(void **state) {
  fixture *f = *state;
  lc_collection_compact_test_hook = NULL;
  lc_collection_compact_test_context = NULL;
  lc_collection_root_test_hook = NULL;
  lc_collection_root_test_context = NULL;
  lc_collection_query_test_hook = NULL;
  lc_collection_query_test_context = NULL;
  lc_pouch_test_queue_clock_gettime = NULL;
  lc_pouch_test_queue_time_context = NULL;
  if (f->owned_fd >= 0)
    close(f->owned_fd);
  lc_source_close(f->source);
  lc_sink_close(f->sink);
  lc_collection_read_res_cleanup(&f->read);
  lc_collection_index_cleanup(&f->query_index);
  close_handles(f);
  lc_pouch_crypto_key_string_free(f->crypto_key);
  lc_error_cleanup(&f->error);
  lc_test_tmp_cleanup_path(f->root, PREFIX);
  free(f);
  return 0;
}

static void bind_collection(fixture *f, const char *name, int create,
                            lc_collection **out) {
  lc_collection_config config;
  lc_collection_config_init(&config);
  config.name = name;
  check(f, create
               ? lc_client_create_collection(f->client, &config, out, &f->error)
               : lc_client_open_collection(f->client, &config, out, &f->error));
}

static int append_retention(fixture *f, lc_collection *collection,
                            const char *id, const char *json, int has_retention,
                            long seconds, lc_collection_append_res *out) {
  lc_collection_append_req req;
  int rc;
  lc_source_close(f->source);
  f->source = NULL;
  check(f, lc_source_from_memory(json, strlen(json), &f->source, &f->error));
  lc_collection_append_req_init(&req);
  req.append_id = id;
  req.has_retention_seconds = has_retention;
  req.retention_seconds = seconds;
  rc = lc_collection_append(collection, &req, f->source, out, &f->error);
  lc_source_close(f->source);
  f->source = NULL;
  return rc;
}

static int append_json(fixture *f, lc_collection *collection, const char *id,
                       const char *json, lc_collection_append_res *out) {
  return append_retention(f, collection, id, json, 0, 0, out);
}

static void append(fixture *f, const char *id, const char *json,
                   uint64_t position) {
  lc_collection_append_res out;
  check(f, append_json(f, f->a, id, json, &out));
  assert_true(out.position == position && !out.duplicate && !out.deleted);
}

static int read_into_memory(fixture *f, const lc_collection_read_opts *opts) {
  lc_sink_close(f->sink);
  f->sink = NULL;
  lc_collection_read_res_cleanup(&f->read);
  check(f, lc_sink_to_memory(&f->sink, &f->error));
  return lc_collection_read(f->a, opts, f->sink, &f->read, &f->error);
}

static void assert_output(fixture *f, const char *expected) {
  const void *bytes;
  size_t length;
  check(f, lc_sink_memory_bytes(f->sink, &bytes, &length, &f->error));
  assert_int_equal(length, strlen(expected));
  if (length != 0U)
    assert_memory_equal(bytes, expected, length);
}

static lc_pouch *root_pouch(fixture *f) {
  return ((lc_client_handle *)f->client)->pouch;
}

static int maintain(fixture *f, int force, int cleanup,
                    lc_pouch_maintenance_result *out) {
  lc_pouch_maintenance_options options = {0};
  lc_pouch_maintenance_result_cleanup(&root_pouch(f)->allocator, out);
  options.ns = "journal";
  options.force = force;
  options.cleanup_only = cleanup;
  return lc_pouch_maintenance_run(root_pouch(f), &options, out, &f->error);
}

static void seed_three(fixture *f);

static int build_query(fixture *f, lc_u64 ordinal) {
  lc_collection_call call;
  int rc = lc_collection_call_begin(f->a, 0, &call, &f->error);
  if (rc == LC_OK) {
    rc = lc_collection_query_build(&call, ordinal, &f->error);
    lc_collection_call_end(&call);
  }
  return rc;
}

static int load_query(fixture *f, const char *query_id, int *found) {
  lc_collection_handle *handle = f->a->impl;
  lc_collection_order_row rows[LC_COLLECTION_ORDER_PAGE_ROWS];
  size_t count;
  lc_collection_index_cleanup(&f->query_index);
  check(f, lc_collection_prepare(handle, &f->error));
  check(f, lc_collection_order_rows(&handle->order, 0, rows,
                                    LC_COLLECTION_ORDER_PAGE_ROWS, &count,
                                    &f->error));
  return lc_collection_query_load(handle,
                                  query_id ? query_id : handle->order.query_id,
                                  0, lc_collection_query_row_hash(rows, count),
                                  &f->query_index, found, &f->error);
}

static void
test_query_artifacts_roundtrip_compaction_and_corruption(void **state) {
  fixture *f = *state;
  char *key = NULL;
  check(f, lc_pouch_crypto_generate_key_string(&key, &f->error));
  for (int mode = 0; mode < 8; ++mode) {
    char root[384], path[640], query_id[LC_XID_STRING_SIZE];
    const char *selected_key = mode & 1 ? key : NULL;
    int found;
    lc_collection_handle *handle;
    lc_pouch_maintenance_result maintained = {0};
    close_handles(f);
    snprintf(root, sizeof(root), "%s/query-mode-%d", f->root, mode);
    snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s", root);
    open_client(f, selected_key, mode & 2, mode & 4);
    seed_three(f);
    assert_int_equal(build_query(f, 1), LC_ERR_INVALID);
    check(f, build_query(f, 0));
    check(f, load_query(f, NULL, &found));
    assert_true(found && f->query_index.rows == 3 &&
                f->query_index.covered == 7);
    handle = f->a->impl;
    strcpy(query_id, handle->order.query_id);
    root_pouch(f)->compaction_delete_grace_seconds = 0;
    check(f, maintain(f, 1, 0, &maintained));
    lc_pouch_maintenance_result_cleanup(NULL, &maintained);
    check(f, load_query(f, NULL, &found));
    assert_true(found);
    assert_string_equal(query_id, handle->order.query_id);
    close_handles(f);
    open_client(f, selected_key, mode & 2, mode & 4);
    bind_collection(f, "events", 0, &f->a);
    handle = f->a->impl;
    check(f, load_query(f, NULL, &found));
    assert_true(found);
    assert_string_equal(query_id, handle->order.query_id);
    snprintf(path, sizeof(path), "%s/query/%s/page-0", handle->directory,
             query_id);
    f->owned_fd = open(path, O_WRONLY);
    assert_true(f->owned_fd >= 0);
    assert_int_equal(pwrite(f->owned_fd, "bad", 3, 0), 3);
    close(f->owned_fd);
    f->owned_fd = -1;
    check(f, load_query(f, NULL, &found));
    assert_false(found);
    check(f, read_into_memory(f, NULL));
    assert_output(f, "{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n");
    check(f, build_query(f, 0));
    check(f, load_query(f, NULL, &found));
    assert_true(found);
  }
  lc_pouch_crypto_key_string_free(key);
}

typedef struct generated_body {
  lc_source pub;
  size_t value_bytes, offset, requested;
} generated_body;

static size_t generated_read(lc_source *self, void *bytes, size_t capacity,
                             lc_error *error) {
  generated_body *body = (generated_body *)self;
  size_t count = body->value_bytes + 8 - body->offset;
  (void)error;
  if (capacity > body->requested)
    body->requested = capacity;
  if (count > capacity)
    count = capacity;
  for (size_t i = 0; i < count; ++i) {
    size_t offset = body->offset + i;
    ((char *)bytes)[i] = offset < 6 ? "{\"s\":\""[offset]
                         : offset < body->value_bytes + 6
                             ? 'a'
                             : "\"}"[offset - body->value_bytes - 6];
  }
  body->offset += count;
  return count;
}

typedef struct counted_output {
  lc_sink pub;
  uint64_t bytes;
  char last;
} counted_output;
static int count_output(lc_sink *self, const void *bytes, size_t length,
                        lc_error *error) {
  counted_output *out = (counted_output *)self;
  (void)error;
  out->bytes += length;
  if (length)
    out->last = ((const char *)bytes)[length - 1];
  return 1;
}

static void test_query_build_consumes_large_states_incrementally(void **state) {
  fixture *f = *state;
  generated_body body = {0};
  counted_output output = {0};
  lc_collection_append_req request = {"large"};
  lc_collection_append_res appended;
  lc_collection_call call;
  int found;
  body.pub.read = generated_read;
  body.value_bytes = 3 * 1024 * 1024;
  output.pub.write = count_output;
  bind_collection(f, "events", 1, &f->a);
  check(f,
        lc_collection_append(f->a, &request, &body.pub, &appended, &f->error));
  assert_true(appended.bytes == body.value_bytes + 8 &&
              body.requested <= 16384);
  check(f, lc_collection_call_begin(f->a, 0, &call, NULL));
  int rc = lc_collection_query_build(&call, 0, NULL);
  lc_collection_call_end(&call);
  check(f, rc);
  check(f, load_query(f, NULL, &found));
  assert_true(found && f->query_index.covered == 1 &&
              f->query_index.exact.terms.count == 1);
  assert_int_equal(f->query_index.exact.terms.items[0].value_type, 'h');
  assert_int_equal(strlen(f->query_index.exact.terms.items[0].value_hex), 24);
  check(f, lc_collection_read(f->a, NULL, &output.pub, &f->read, &f->error));
  assert_true(f->read.complete && f->read.completed_records == 1 &&
              output.bytes == appended.bytes + 1 && output.last == '\n');
}

static void test_append_retry_delete_and_reopen(void **state) {
  fixture *f = *state;
  lc_collection_append_res out;
  lc_collection_append_status_res status;
  lc_collection_delete_res deleted;
  lc_collection_info info;
  bind_collection(f, "events", 1, &f->a);
  append(f, "request-1", " { \"n\" : 1, \"nested\": [true, null] } ", 1U);
  check(f, append_json(f, f->a, "request-1", "{\"n\":1,\"nested\":[true,null]}",
                       &out));
  assert_true(out.position == 1U && out.duplicate && !out.deleted);
  assert_int_equal(append_json(f, f->a, "request-1", "{\"n\":2}", &out),
                   LC_ERR_INVALID);
  assert_string_equal(f->error.server_code, "append_id_conflict");
  append(f, "request-2", "{\"n\":2}", 2U);
  lc_get_res ordinary = {0};
  check(f, lc_sink_to_discard(&f->sink, &f->error));
  check(f, lc_get(f->client, "request-2", NULL, f->sink, &ordinary, &f->error));
  assert_true(ordinary.no_content);
  lc_get_res_cleanup(&ordinary);
  lc_sink_close(f->sink);
  f->sink = NULL;
  check(f, lc_collection_delete_entry(f->a, 1U, &deleted, &f->error));
  assert_true(deleted.deleted && !deleted.already_deleted);
  check(f, lc_collection_delete_entry(f->a, 1U, &deleted, &f->error));
  assert_true(deleted.already_deleted);
  check(f, lc_collection_append_status(f->a, "request-1", &status, &f->error));
  assert_int_equal(status.state, LC_COLLECTION_APPEND_DELETED);
  check(f, append_json(f, f->a, "request-1", "{\"n\":1,\"nested\":[true,null]}",
                       &out));
  assert_true(out.position == 1U && out.duplicate && out.deleted);
  assert_int_equal(lc_collection_delete_entry(f->a, 99U, &deleted, &f->error),
                   LC_ERR_INVALID);
  check(f, lc_collection_describe(f->a, &info, &f->error));
  assert_true(info.upper_position == 2U && info.live_count == 1U);
  close_handles(f);
  open_client(f, NULL, 0, 0);
  bind_collection(f, "events", 0, &f->a);
  check(f, lc_collection_append_status(f->a, "request-1", &status, &f->error));
  assert_int_equal(status.state, LC_COLLECTION_APPEND_DELETED);
  append(f, "request-3", "{\"n\":3}", 3U);
  check(f, read_into_memory(f, NULL));
  assert_output(f, "{\"n\":2}\n{\"n\":3}\n");
  assert_true(f->read.complete && f->read.upper_position == 3U &&
              f->read.completed_records == 2U);
}

typedef struct source_fault {
  lc_source pub;
  fixture *f;
  size_t offset;
  int reset;
  int closed;
  int reenter;
} source_fault;

static size_t source_read(lc_source *self, void *bytes, size_t size,
                          lc_error *error) {
  source_fault *source = (source_fault *)self;
  if (source->offset != 0U) {
    if (source->reenter)
      return 0U;
    lc_error_set(error, LC_ERR_TIMEOUT, 0L, "injected upload failure", NULL,
                 NULL, NULL);
    return 0U;
  }
  if (source->reenter) {
    lc_collection_append_status_res status;
    lc_collection_append_res appended;
    lc_collection_delete_res deleted;
    lc_collection_config config = {NULL, "events"};
    lc_collection *fresh = NULL;
    lc_error nested;
    lc_error_init(&nested);
    assert_int_equal(
        lc_collection_append_status(source->f->b, "prior", &status, &nested),
        LC_OK);
    assert_int_equal(status.state, LC_COLLECTION_APPEND_COMMITTED);
    assert_int_equal(
        lc_client_open_collection(source->f->client, &config, &fresh, &nested),
        LC_OK);
    assert_int_equal(
        lc_collection_append_status(fresh, "prior", &status, &nested), LC_OK);
    lc_collection_close(fresh);
    assert_int_equal(
        append_json(source->f, source->f->b, "nested", "{}", &appended),
        LC_ERR_INVALID);
    assert_int_equal(
        lc_collection_delete_entry(source->f->b, 1, &deleted, &nested),
        LC_ERR_INVALID);
    assert_int_equal(lc_collection_drop(source->f->b, &nested), LC_ERR_INVALID);
    lc_error_cleanup(&nested);
    lc_error_cleanup(&source->f->error);
    lc_error_init(&source->f->error);
  }
  assert_true(size >= 2U);
  memcpy(bytes, "{}", 2U);
  source->offset = 2U;
  return 2U;
}

static int source_reset(lc_source *self, lc_error *error) {
  (void)error;
  ++((source_fault *)self)->reset;
  return 1;
}
static void source_close(lc_source *self) { ++((source_fault *)self)->closed; }

static void test_failed_upload_and_source_callback_reentry(void **state) {
  fixture *f = *state;
  lc_collection_append_req req = {"bad-source"};
  lc_collection_append_res out;
  lc_collection_append_status_res status;
  source_fault source = {0};
  bind_collection(f, "events", 1, &f->a);
  source.pub.read = source_read;
  source.pub.reset = source_reset;
  source.pub.close = source_close;
  source.f = f;
  assert_int_equal(
      lc_collection_append(f->a, &req, &source.pub, &out, &f->error),
      LC_ERR_TIMEOUT);
  check(f, lc_collection_append_status(f->a, "bad-source", &status, &f->error));
  assert_int_equal(status.state, LC_COLLECTION_APPEND_ABSENT);
  const char *invalid[] = {"[]", "{", "{} {}", "{\"x\":\"\377\"}", "null"};
  for (size_t i = 0U; i < sizeof(invalid) / sizeof(invalid[0]); ++i)
    assert_int_equal(append_json(f, f->a, "bad-json", invalid[i], &out),
                     LC_ERR_INVALID);
  append(f, "prior", "{}", 1U);
  bind_collection(f, "events", 0, &f->b);
  source.offset = 0U;
  source.reenter = 1;
  req.append_id = "outer";
  check(f, lc_collection_append(f->a, &req, &source.pub, &out, &f->error));
  assert_true(out.position == 2U && source.closed == 0 && source.reset == 0);
}

typedef struct metadata_context {
  fixture *f;
  unsigned int entries;
  unsigned int fail_at;
  int close_owner;
  int append_peer;
  char cursor[LC_COLLECTION_CURSOR_BYTES];
} metadata_context;

static int metadata(void *context, const lc_collection_read_metadata *item,
                    lc_error *error) {
  metadata_context *m = context;
  if (item->kind != LC_COLLECTION_READ_METADATA_ENTRY)
    return LC_OK;
  ++m->entries;
  if (m->entries == m->fail_at)
    return lc_error_set(error, LC_ERR_TIMEOUT, 0, "injected metadata rejection",
                        NULL, NULL, NULL);
  assert_true(strlen(item->resume_cursor) < sizeof(m->cursor));
  strcpy(m->cursor, item->resume_cursor);
  if (m->append_peer && m->entries == 1U) {
    lc_collection_append_res out;
    check(m->f, append_json(m->f, m->f->b, "later", "{\"n\":4}", &out));
    assert_true(out.position == 4U);
  }
  if (m->close_owner) {
    lc_collection_close(m->f->a);
    m->f->a = NULL;
    lc_client_close(m->f->client);
    m->f->client = NULL;
    m->close_owner = 0;
  }
  return LC_OK;
}

static void seed_three(fixture *f) {
  bind_collection(f, "events", 1, &f->a);
  append(f, "one", "{\"n\":1}", 1U);
  append(f, "two", "{\"n\":2}", 2U);
  append(f, "three", "{\"n\":3}", 3U);
}

static void
test_separate_metadata_resume_checkpoint_and_bounded_eod(void **state) {
  fixture *f = *state;
  lc_collection_read_opts opts = {0};
  metadata_context m = {0};
  char cursor[LC_COLLECTION_CURSOR_BYTES];
  seed_three(f);
  bind_collection(f, "events", 0, &f->b);
  m.f = f;
  m.fail_at = 2U;
  m.append_peer = 1;
  opts.on_metadata = metadata;
  opts.metadata_context = &m;
  assert_int_equal(read_into_memory(f, &opts), LC_ERR_TIMEOUT);
  assert_output(f, "{\"n\":1}\n{\"n\":2}\n");
  assert_true(!f->read.complete && f->read.completed_records == 1U &&
              f->read.last_examined_position == 1U);
  assert_string_equal(f->read.resume_cursor, m.cursor);
  strcpy(cursor, f->read.resume_cursor);
  opts.cursor = cursor;
  opts.on_metadata = NULL;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "{\"n\":2}\n{\"n\":3}\n");
  assert_true(f->read.complete && f->read.upper_position == 3U);
  strcpy(cursor, f->read.checkpoint_cursor);
  opts.cursor = cursor;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "{\"n\":4}\n");
  assert_true(f->read.upper_position == 4U);
}

typedef struct cancel_sink {
  lc_sink pub;
  lc_sink *inner;
  size_t written;
  int cancel;
} cancel_sink;

static int cancel_write(lc_sink *self, const void *bytes, size_t count,
                        lc_error *error) {
  cancel_sink *sink = (cancel_sink *)self;
  int rc = sink->inner->write(sink->inner, bytes, count, error);
  sink->written += count;
  sink->cancel = 1;
  return rc;
}
static int is_cancelled(void *context) {
  return ((cancel_sink *)context)->cancel;
}

static void test_cancellation_retains_initial_cursor_before_lf(void **state) {
  fixture *f = *state;
  lc_collection_read_opts opts = {0};
  cancel_sink sink = {0};
  char cursor[LC_COLLECTION_CURSOR_BYTES];
  seed_three(f);
  check(f, lc_sink_to_memory(&f->sink, &f->error));
  sink.pub.write = cancel_write;
  sink.inner = f->sink;
  opts.cancelled = is_cancelled;
  opts.cancel_context = &sink;
  check(f, lc_collection_read(f->a, &opts, &sink.pub, &f->read, &f->error));
  assert_output(f, "{\"n\":1}");
  assert_true(!f->read.complete && f->read.completed_records == 0U &&
              f->read.last_examined_position == 0U);
  assert_int_equal(f->read.stop_reason, LC_COLLECTION_READ_CANCELLED);
  strcpy(cursor, f->read.resume_cursor);
  opts.cursor = cursor;
  opts.cancelled = NULL;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n");
}

static void test_lql_filters_and_cursor_binding(void **state) {
  fixture *f = *state;
  lc_collection_read_opts opts = {0};
  char cursor[LC_COLLECTION_CURSOR_BYTES];
  seed_three(f);
  opts.selector_lql = "range{field=/n,gte=2}";
  check(f, read_into_memory(f, &opts));
  assert_output(f, "{\"n\":2}\n{\"n\":3}\n");
  assert_true(f->read.completed_records == 2U &&
              f->read.last_examined_position == 3U);
  strcpy(cursor, f->read.checkpoint_cursor);
  opts.cursor = cursor;
  opts.selector_lql = "range{field=/n,gte=3}";
  assert_int_equal(read_into_memory(f, &opts), LC_ERR_INVALID);
  assert_output(f, "");
  opts.cursor = NULL;
  opts.selector_lql = "eq{field=/n,value=999}";
  check(f, read_into_memory(f, &opts));
  assert_output(f, "");
  assert_true(f->read.complete && f->read.last_examined_position == 3U);
  opts.selector_lql = "this is not LQL";
  assert_int_equal(read_into_memory(f, &opts), LC_ERR_INVALID);
  assert_output(f, "");
}

static void indexed_mode(fixture *f, int enabled, int create) {
  close_handles(f);
  f->index_queries = enabled;
  open_client(f, NULL, 0, 0);
  bind_collection(f, "events", create, &f->a);
}

static void indexed_append(fixture *f, unsigned int position, int match) {
  char id[32], json[256];
  snprintf(id, sizeof(id), "entry-%u", position);
  snprintf(json, sizeof(json),
           "{\"kind\":\"%s\",\"n\":%u,\"flag\":%s,\"nested\":%s,\"a\":[{\"k\":"
           "\"%s\"}],\"nul\":\"a\\u0000b\"}",
           match ? "match" : "other", position,
           position % 2 ? "true" : "\"true\"",
           position % 5 ? "{\"v\":1}" : "{}", match ? "match" : "other");
  append(f, id, json, position);
}

static void indexed_seed(fixture *f, unsigned int count) {
  indexed_mode(f, 1, 1);
  for (unsigned int i = 1; i <= count; ++i)
    indexed_append(f, i, i % 3 == 2);
}

static void
test_indexed_filters_agree_with_scan_and_cover_unindexed_tail(void **state) {
  fixture *f = *state;
  const char *predicates[] = {"eq{field=/kind,value=\"match\"}",
                              "eq{field=/kind,value=\"missing\"}",
                              "eq{field=/kind,value=\"\"}",
                              "eq{field=/flag,value=true}",
                              "eq{field=/flag,value=\"true\"}",
                              "eq{field=/n,value=65}",
                              "exists{/nested}",
                              "exists{/nested/**}",
                              "exists{/missing}",
                              "range{field=/n,gte=60}",
                              "contains{field=/kind,value=at}",
                              "eq{field=/a/[]/k,value=\"match\"}",
                              "eq{field=/nul,value=\"a\\u0000b\"}",
                              "eq{field=/nul,value=\"a\"}"};
  char expected[16384];
  indexed_seed(f, 68);
  int found;
  check(f, load_query(f, NULL, &found));
  assert_true(found && f->query_index.rows == 63);
  for (size_t i = 0; i < sizeof(predicates) / sizeof(predicates[0]); ++i) {
    lc_collection_read_opts options = {0};
    const void *bytes;
    size_t length;
    uint64_t records;
    options.selector_lql = predicates[i];
    indexed_mode(f, 0, 0);
    check(f, read_into_memory(f, &options));
    check(f, lc_sink_memory_bytes(f->sink, &bytes, &length, &f->error));
    assert_true(length < sizeof(expected));
    if (length != 0U)
      memcpy(expected, bytes, length);
    expected[length] = 0;
    records = f->read.completed_records;
    indexed_mode(f, 1, 0);
    check(f, read_into_memory(f, &options));
    assert_output(f, expected);
    assert_true(f->read.complete && f->read.upper_position == 68 &&
                f->read.last_examined_position == 68 &&
                f->read.completed_records == records);
  }
}

static void first_body_byte(fixture *f, char byte) {
  lc_collection_handle *handle = f->a->impl;
  lc_collection_order_row row;
  lc_pouch_collection_record record;
  lc_u64 segment = 0, end = 0;
  char *path = NULL, leaf[64];
  int fd = -1, found;
  check(f,
        lc_collection_order_find(&handle->order, 1, &row, &found, &f->error));
  assert_true(found);
  check(f, lc_collection_record_ref(handle, row.segment, row.record_offset, &fd,
                                    &segment, &end, &record, &f->error));
  close(fd);
  check(f,
        lc_collection_record_path(handle, row.segment, &path, leaf, &f->error));
  f->owned_fd = open(path, O_WRONLY);
  lc_free_with_allocator(&handle->allocator, path);
  assert_true(f->owned_fd >= 0);
  assert_int_equal(pwrite(f->owned_fd, &byte, 1, (off_t)record.payload_offset),
                   1);
  close(f->owned_fd);
  f->owned_fd = -1;
}

static void test_indexed_negative_does_not_read_its_payload(void **state) {
  fixture *f = *state;
  lc_collection_read_opts options = {0};
  indexed_seed(f, 68);
  first_body_byte(f, '[');
  options.selector_lql = "eq{field=/kind,value=\"match\"}";
  check(f, read_into_memory(f, &options));
  assert_true(f->read.complete && f->read.completed_records == 23);
  options.selector_lql = "eq{field=/a/[]/k,value=\"match\"}";
  check(f, read_into_memory(f, &options));
  assert_true(f->read.complete && f->read.completed_records == 23);
  indexed_mode(f, 0, 0);
  assert_int_equal(read_into_memory(f, &options), LC_ERR_PROTOCOL);
  first_body_byte(f, '{');
  check(f, read_into_memory(f, &options));
  assert_true(f->read.complete && f->read.completed_records == 23);
}

static void
test_query_index_does_not_retry_valid_out_of_range_numbers(void **state) {
  fixture *f = *state;
  lc_collection_append_res entry;
  lc_pouch_maintenance_result result = {0};
  int found;
  indexed_seed(f, 62);
  check(f, append_json(f, f->a, "huge-number", "{\"huge\":1e99999}", &entry));
  assert_int_equal(entry.position, 63);
  check(f, load_query(f, NULL, &found));
  assert_true(found && f->query_index.rows == 63);
  assert_true((f->query_index.covered & ((lc_u64)1 << 62)) == 0);
  check(f, maintain(f, 0, 0, &result));
  check(f, read_into_memory(f, NULL));
  assert_int_equal(f->read.completed_records, 63);
  const void *bytes;
  size_t length;
  const char *last = "{\"huge\":1e99999}\n";
  check(f, lc_sink_memory_bytes(f->sink, &bytes, &length, &f->error));
  assert_true(length >= strlen(last));
  assert_memory_equal((const char *)bytes + length - strlen(last), last,
                      strlen(last));
  lc_pouch_maintenance_result_cleanup(NULL, &result);
}

static void test_query_pruning_does_not_read_retained_payloads(void **state) {
  fixture *f = *state;
  lc_collection_delete_res removed;
  lc_pouch_maintenance_result result = {0};
  int found;
  indexed_seed(f, 63);
  root_pouch(f)->compaction_min_reclaimable_bytes = LC_U64_MAX;
  first_body_byte(f, '[');
  check(f, lc_collection_delete_entry(f->a, 2, &removed, &f->error));
  check(f, maintain(f, 0, 0, &result));
  assert_int_equal(result.compacted_collection_count, 0);
  check(f, load_query(f, NULL, &found));
  assert_true(found && (f->query_index.covered & 2U) == 0U);
  first_body_byte(f, '{');
  check(f, read_into_memory(f, NULL));
  assert_int_equal(f->read.completed_records, 62);
  lc_pouch_maintenance_result_cleanup(NULL, &result);
}

typedef struct prune_context {
  fixture *f;
  int pruned;
  int saw_removed;
} prune_context;

static int prune_during_read(void *context,
                             const lc_collection_read_metadata *entry,
                             lc_error *error) {
  prune_context *work = context;
  lc_collection_delete_res removed;
  lc_pouch_maintenance_result result = {0};
  (void)error;
  if (entry->kind != LC_COLLECTION_READ_METADATA_ENTRY)
    return LC_OK;
  if (entry->position == 90)
    work->saw_removed = 1;
  if (!work->pruned) {
    work->pruned = 1;
    check(work->f, lc_collection_delete_entry(work->f->b, 90, &removed,
                                              &work->f->error));
    check(work->f, maintain(work->f, 0, 0, &result));
    lc_pouch_maintenance_result_cleanup(NULL, &result);
  }
  return LC_OK;
}

static void
test_query_pruning_preserves_an_active_visibility_view(void **state) {
  fixture *f = *state;
  prune_context work = {f, 0, 0};
  lc_collection_read_opts options = {0};
  indexed_seed(f, 189);
  bind_collection(f, "events", 0, &f->b);
  root_pouch(f)->compaction_min_reclaimable_bytes = LC_U64_MAX;
  options.selector_lql = "exists{/kind}";
  options.on_metadata = prune_during_read;
  options.metadata_context = &work;
  check(f, read_into_memory(f, &options));
  assert_true(work.pruned && work.saw_removed);
  assert_int_equal(f->read.completed_records, 189);
  options.on_metadata = NULL;
  check(f, read_into_memory(f, &options));
  assert_int_equal(f->read.completed_records, 188);
}

static void
test_indexed_resume_retains_horizon_and_accepted_nonmatches(void **state) {
  fixture *f = *state;
  lc_collection_read_opts options = {0};
  metadata_context context = {0};
  char cursor[LC_COLLECTION_CURSOR_BYTES];
  indexed_seed(f, 68);
  context.fail_at = 2;
  options.selector_lql = "eq{field=/kind,value=\"match\"}";
  options.on_metadata = metadata;
  options.metadata_context = &context;
  assert_int_equal(read_into_memory(f, &options), LC_ERR_TIMEOUT);
  assert_true(!f->read.complete && f->read.completed_records == 1 &&
              f->read.last_examined_position == 4);
  strcpy(cursor, f->read.resume_cursor);
  indexed_append(f, 69, 1);
  options.cursor = cursor;
  options.on_metadata = NULL;
  check(f, read_into_memory(f, &options));
  assert_true(f->read.complete && f->read.upper_position == 68 &&
              f->read.completed_records == 22);
  strcpy(cursor, f->read.checkpoint_cursor);
  check(f, read_into_memory(f, &options));
  assert_true(f->read.complete && f->read.upper_position == 69 &&
              f->read.completed_records == 1);
}

static void
test_index_build_failure_preserves_committed_append_and_maintenance_repairs(
    void **state) {
  fixture *f = *state;
  lc_pouch_maintenance_result maintained = {0};
  lc_collection_append_status_res status;
  lc_collection_delete_res removed;
  char path[640];
  int found;
  indexed_seed(f, 62);
  lc_collection_handle *handle = f->a->impl;
  snprintf(path, sizeof(path), "%s/query", handle->directory);
  f->owned_fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600);
  assert_true(f->owned_fd >= 0);
  close(f->owned_fd);
  f->owned_fd = -1;
  indexed_append(f, 63, 1);
  assert_int_equal(f->error.code, LC_OK);
  check(f, lc_collection_append_status(f->a, "entry-63", &status, &f->error));
  assert_int_equal(status.state, LC_COLLECTION_APPEND_COMMITTED);
  check(f, load_query(f, NULL, &found));
  assert_false(found);
  assert_int_equal(unlink(path), 0);
  check(f, maintain(f, 0, 0, &maintained));
  check(f, load_query(f, NULL, &found));
  assert_true(found);
  check(f, lc_collection_delete_entry(f->a, 2, &removed, &f->error));
  check(f, maintain(f, 0, 0, &maintained));
  check(f, load_query(f, NULL, &found));
  assert_true(found && (f->query_index.covered & 2) == 0);
  lc_pouch_maintenance_result_cleanup(NULL, &maintained);
}

typedef struct progress_context {
  fixture *f;
  unsigned int calls;
  int reject;
  int append_and_cancel;
  int cancel;
  uint64_t appended_position;
} progress_context;

static int progress_metadata(void *context,
                             const lc_collection_read_metadata *item,
                             lc_error *error) {
  progress_context *p = context;
  lc_collection_append_res out;
  assert_int_equal(item->kind, LC_COLLECTION_READ_METADATA_PROGRESS);
  ++p->calls;
  if (p->reject)
    return lc_error_set(error, LC_ERR_TIMEOUT, 0, "injected progress rejection",
                        NULL, NULL, NULL);
  if (p->append_and_cancel) {
    check(p->f, append_json(p->f, p->f->b, "after-capture", "{\"later\":true}",
                            &out));
    p->appended_position = out.position;
    p->cancel = 1;
  }
  return LC_OK;
}

static int progress_cancelled(void *context) {
  return ((progress_context *)context)->cancel;
}

static void seed_many(fixture *f, uint64_t count) {
  char id[32];
  bind_collection(f, "events", 1, &f->a);
  for (uint64_t i = 1; i <= count; ++i) {
    assert_true(snprintf(id, sizeof(id), "event-%lu", (unsigned long)i) > 0);
    append(f, id, "{\"n\":1}", i);
  }
}

static void
test_deleted_scan_cancels_before_eod_and_resumes_original_bound(void **state) {
  fixture *f = *state;
  const uint64_t total = LC_COLLECTION_ORDER_PAGE_ROWS * 4 + 7;
  lc_collection_read_opts opts = {0};
  lc_collection_delete_res deleted;
  progress_context progress = {0};
  char cursor[LC_COLLECTION_CURSOR_BYTES];
  seed_many(f, total);
  bind_collection(f, "events", 0, &f->b);
  for (uint64_t i = 1; i <= total; ++i)
    check(f, lc_collection_delete_entry(f->a, i, &deleted, &f->error));
  progress.f = f;
  progress.append_and_cancel = 1;
  opts.on_metadata = progress_metadata;
  opts.metadata_context = &progress;
  opts.cancelled = progress_cancelled;
  opts.cancel_context = &progress;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "");
  assert_int_equal(progress.calls, 1);
  assert_true(progress.appended_position == total + 1);
  assert_true(!f->read.complete && f->read.completed_records == 0);
  assert_int_equal(f->read.stop_reason, LC_COLLECTION_READ_CANCELLED);
  assert_true(f->read.upper_position == total &&
              f->read.last_examined_position > 0 &&
              f->read.last_examined_position < total);
  strcpy(cursor, f->read.resume_cursor);
  opts.cursor = cursor;
  opts.on_metadata = NULL;
  opts.cancelled = NULL;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "");
  assert_true(f->read.complete && f->read.upper_position == total);
  strcpy(cursor, f->read.checkpoint_cursor);
  check(f, read_into_memory(f, &opts));
  assert_output(f, "{\"later\":true}\n");
  assert_true(f->read.complete && f->read.upper_position == total + 1);
}

static void
test_filter_progress_is_coalesced_and_rejection_is_retryable(void **state) {
  fixture *f = *state;
  const uint64_t total = LC_COLLECTION_ORDER_PAGE_ROWS * 4 + 7;
  lc_collection_read_opts opts = {0};
  progress_context progress = {0};
  char cursor[LC_COLLECTION_CURSOR_BYTES];
  seed_many(f, total);
  progress.reject = 1;
  opts.selector_lql = "eq{field=/n,value=999}";
  opts.on_metadata = progress_metadata;
  opts.metadata_context = &progress;
  assert_int_equal(read_into_memory(f, &opts), LC_ERR_TIMEOUT);
  assert_output(f, "");
  assert_true(!f->read.complete && f->read.completed_records == 0 &&
              f->read.last_examined_position == 0);
  assert_int_equal(progress.calls, 1);
  strcpy(cursor, f->read.resume_cursor);
  progress.reject = 0;
  progress.calls = 0;
  opts.cursor = cursor;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "");
  assert_true(f->read.complete && f->read.last_examined_position == total);
  assert_true(progress.calls > 0 && progress.calls <= 2);
}

static int visit_names(void *context, const lc_collection_info *info,
                       lc_error *error) {
  char *names = context;
  (void)error;
  assert_true(strlen(names) + strlen(info->name) + 2U < 1024U);
  strcat(names, info->name);
  strcat(names, "\n");
  return LC_OK;
}

static void test_catalog_incarnations_names_and_stale_cursor(void **state) {
  fixture *f = *state;
  lc_collection_config config = {0};
  lc_collection_info info;
  lc_collection_capabilities capabilities;
  lc_collection_read_opts opts = {0};
  char names[1024] = "";
  char id[LC_XID_STRING_SIZE];
  char cursor[LC_COLLECTION_CURSOR_BYTES];
  char name[256];
  check(f,
        lc_client_collection_capabilities(f->client, &capabilities, &f->error));
  assert_true(capabilities.collections && capabilities.lql_filter &&
              capabilities.resume && capabilities.ranges &&
              capabilities.first_n && !capabilities.last_n);
  check(f, lc_client_list_collections(f->client, NULL, visit_names, names,
                                      &f->error));
  assert_string_equal(names, "");
  bind_collection(f, "z", 1, &f->a);
  bind_collection(f, "a", 1, &f->b);
  check(f, lc_client_list_collections(f->client, NULL, visit_names, names,
                                      &f->error));
  assert_string_equal(names, "a\nz\n");
  config.name = "z";
  lc_collection *duplicate = NULL;
  assert_int_equal(
      lc_client_create_collection(f->client, &config, &duplicate, &f->error),
      LC_ERR_INVALID);
  assert_null(duplicate);
  strcpy(id, f->a->collection_id);
  check(f, read_into_memory(f, NULL));
  strcpy(cursor, f->read.checkpoint_cursor);
  check(f, lc_collection_drop(f->a, &f->error));
  assert_int_equal(lc_collection_describe(f->a, &info, &f->error),
                   LC_ERR_INVALID);
  lc_collection_close(f->b);
  f->b = NULL;
  bind_collection(f, "z", 1, &f->b);
  assert_string_not_equal(id, f->b->collection_id);
  assert_int_equal(lc_collection_drop(f->a, &f->error), LC_ERR_INVALID);
  lc_collection_close(f->a);
  f->a = f->b;
  f->b = NULL;
  opts.cursor = cursor;
  assert_int_equal(read_into_memory(f, &opts), LC_ERR_INVALID);
  memset(name, 'x', 255U);
  name[255] = '\0';
  bind_collection(f, name, 1, &f->b);
  lc_collection_close(f->b);
  f->b = NULL;
  config.name = "bad\377";
  assert_int_equal(
      lc_client_create_collection(f->client, &config, &f->b, &f->error),
      LC_ERR_INVALID);
  config.name = "private";
  config.ns = ".lockd";
  assert_int_equal(
      lc_client_create_collection(f->client, &config, &f->b, &f->error),
      LC_ERR_INVALID);
  config.ns = ".xa/participant";
  assert_int_equal(
      lc_client_create_collection(f->client, &config, &f->b, &f->error),
      LC_ERR_INVALID);
}

static void test_deferred_close_from_metadata(void **state) {
  fixture *f = *state;
  lc_collection_read_opts opts = {0};
  metadata_context m = {0};
  seed_three(f);
  m.f = f;
  m.close_owner = 1;
  opts.on_metadata = metadata;
  opts.metadata_context = &m;
  check(f, read_into_memory(f, &opts));
  assert_true(f->read.complete && m.entries == 3U);
  assert_null(f->a);
  assert_null(f->client);
  assert_output(f, "{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n");
}

static void test_transforms_rotation_and_reopen(void **state) {
  fixture *f = *state;
  char *key = NULL;
  char json[20008];
  check(f, lc_pouch_crypto_generate_key_string(&key, &f->error));
  memset(json, 'x', sizeof(json));
  memcpy(json, "{\"s\":\"", 6U);
  memcpy(json + sizeof(json) - 3U, "\"}", 3U);
  for (int mode = 0; mode < 8; ++mode) {
    char root[384];
    const char *selected_key = mode & 1 ? key : NULL;
    close_handles(f);
    assert_true(snprintf(root, sizeof(root), "%s/mode-%d", f->root, mode) > 0);
    assert_true(snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s", root) >
                0);
    open_client(f, selected_key, mode & 2, mode & 4);
    bind_collection(f, "events", 1, &f->a);
    append(f, "large", json, 1U);
    append(f, "small", "{}", 2U);
    lc_pouch_maintenance_result maintained = {0};
    root_pouch(f)->compaction_delete_grace_seconds = 0U;
    check(f, maintain(f, 1, 0, &maintained));
    assert_int_equal(maintained.compacted_collection_count, 1U);
    lc_pouch_maintenance_result_cleanup(NULL, &maintained);
    close_handles(f);
    open_client(f, selected_key, mode & 2, mode & 4);
    bind_collection(f, "events", 0, &f->a);
    check(f, read_into_memory(f, NULL));
    const void *bytes;
    size_t length;
    check(f, lc_sink_memory_bytes(f->sink, &bytes, &length, &f->error));
    assert_int_equal(length, strlen(json) + 4U);
    assert_memory_equal(bytes, json, strlen(json));
    assert_memory_equal((const char *)bytes + strlen(json), "\n{}\n", 4U);
  }
  lc_pouch_crypto_key_string_free(key);
}

static void
test_crash_nosync_rebuild_discards_uncommitted_projection(void **state) {
  fixture *f = *state;
  lc_collection_info info;
  lc_collection_append_status_res status;
  int child_status;
  int channel[2], found;
  char prior_query_id[LC_XID_STRING_SIZE];
  assert_int_equal(pipe(channel), 0);
  close_handles(f);
  pid_t child = fork();
  assert_true(child >= 0);
  if (child == 0) {
    close(channel[0]);
    struct stat first;
    open_client(f, NULL, 0, 0);
    bind_collection(f, "events", 1, &f->a);
    append(f, "survives", "{}", 1U);
    lc_collection_handle *handle = f->a->impl;
    if (fstat(handle->active_fd, &first) != 0)
      _exit(11);
    append(f, "lost", "{\"k\":\"lost\"}", 2U);
    if (build_query(f, 0) != LC_OK ||
        write(channel[1], handle->order.query_id, LC_XID_STRING_SIZE) !=
            LC_XID_STRING_SIZE)
      _exit(13);
    close(channel[1]);
    if (ftruncate(handle->active_fd, first.st_size) != 0 ||
        fsync(handle->active_fd) != 0 || fsync(handle->order.fd) != 0 ||
        fsync(handle->ids.fd) != 0)
      _exit(12);
    _exit(0);
  }
  close(channel[1]);
  assert_int_equal(waitpid(child, &child_status, 0), child);
  assert_true(WIFEXITED(child_status) && WEXITSTATUS(child_status) == 0);
  assert_int_equal(read(channel[0], prior_query_id, sizeof(prior_query_id)),
                   sizeof(prior_query_id));
  close(channel[0]);
  open_client(f, NULL, 0, 0);
  bind_collection(f, "events", 0, &f->a);
  check(f, lc_collection_describe(f->a, &info, &f->error));
  assert_true(info.upper_position == 1U && info.live_count == 1U);
  check(f, lc_collection_append_status(f->a, "lost", &status, &f->error));
  assert_int_equal(status.state, LC_COLLECTION_APPEND_ABSENT);
  append(f, "next", "{\"k\":\"next\"}", 2U);
  assert_string_not_equal(((lc_collection_handle *)f->a->impl)->order.query_id,
                          prior_query_id);
  /* The rolled-back page has identical positions, publication generations and
   * body sizes. Its old index is still well formed, but belongs to lost
   * history. */
  check(f, load_query(f, prior_query_id, &found));
  assert_true(found);
  check(f, load_query(f, NULL, &found));
  assert_false(found);
  check(f, read_into_memory(f, NULL));
  assert_output(f, "{}\n{\"k\":\"next\"}\n");
}

static void test_projection_repair_and_body_corruption(void **state) {
  fixture *f = *state;
  char path[512];
  seed_three(f);
  lc_collection_handle *handle = f->a->impl;
  assert_true(strlen(handle->order_path) < sizeof(path));
  strcpy(path, handle->order_path);
  close_handles(f);
  int fd = open(path, O_WRONLY);
  assert_true(fd >= 0);
  assert_int_equal(pwrite(fd, "x", 1U, 0), 1);
  close(fd);
  open_client(f, NULL, 0, 0);
  bind_collection(f, "events", 0, &f->a);
  check(f, read_into_memory(f, NULL));
  assert_output(f, "{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n");
  handle = f->a->impl;
  lc_collection_order_row row;
  lc_pouch_collection_record record;
  int found;
  int record_fd = -1;
  lc_u64 segment = 0, end = 0;
  check(f,
        lc_collection_order_find(&handle->order, 2U, &row, &found, &f->error));
  assert_true(found);
  check(f, lc_collection_record_ref(handle, row.segment, row.record_offset,
                                    &record_fd, &segment, &end, &record,
                                    &f->error));
  close(record_fd);
  assert_int_equal(
      pwrite(handle->active_fd, "9", 1U, (off_t)record.payload_offset + 5), 1);
  assert_int_equal(build_query(f, 0), LC_ERR_PROTOCOL);
  check(f, load_query(f, NULL, &found));
  assert_false(found);
  assert_int_equal(
      pwrite(handle->active_fd, "2", 1U, (off_t)record.payload_offset + 5), 1);
  assert_int_equal(
      pwrite(handle->active_fd, "[", 1U, (off_t)record.payload_offset), 1);
  assert_int_equal(read_into_memory(f, NULL), LC_ERR_PROTOCOL);
  assert_true(!f->read.complete && f->read.completed_records == 1U &&
              f->read.last_examined_position == 1U);
  const void *bytes;
  size_t length;
  check(f, lc_sink_memory_bytes(f->sink, &bytes, &length, &f->error));
  assert_int_equal(length, strlen("{\"n\":1}\n[\"n\":2}"));
  assert_memory_equal(bytes, "{\"n\":1}\n[\"n\":2}", length);
}

typedef struct writer {
  lc_collection *collection;
  pthread_barrier_t *barrier;
  unsigned int created;
  unsigned int duplicates;
  int rc;
} writer;

static void *write_parallel(void *context) {
  writer *w = context;
  lc_error error;
  lc_error_init(&error);
  pthread_barrier_wait(w->barrier);
  for (unsigned int i = 0U; i < 32U; ++i) {
    lc_source *source = NULL;
    lc_collection_append_req req;
    lc_collection_append_res out;
    char id[32];
    snprintf(id, sizeof(id), "parallel-%u", i);
    lc_collection_append_req_init(&req);
    req.append_id = id;
    w->rc = lc_source_from_memory("{}", 2U, &source, &error);
    if (w->rc == LC_OK)
      w->rc = lc_collection_append(w->collection, &req, source, &out, &error);
    lc_source_close(source);
    if (w->rc != LC_OK)
      break;
    if (out.duplicate)
      ++w->duplicates;
    else
      ++w->created;
  }
  lc_error_cleanup(&error);
  return NULL;
}

static void test_concurrent_handles_share_append_authority(void **state) {
  fixture *f = *state;
  pthread_t threads[2];
  pthread_barrier_t barrier;
  lc_collection_info info;
  bind_collection(f, "events", 1, &f->a);
  bind_collection(f, "events", 0, &f->b);
  assert_int_equal(pthread_barrier_init(&barrier, NULL, 2U), 0);
  writer writers[2] = {{f->a, &barrier, 0U, 0U, LC_OK},
                       {f->b, &barrier, 0U, 0U, LC_OK}};
  for (size_t i = 0; i < 2; ++i)
    assert_int_equal(
        pthread_create(threads + i, NULL, write_parallel, writers + i), 0);
  for (size_t i = 0; i < 2; ++i)
    assert_int_equal(pthread_join(threads[i], NULL), 0);
  assert_int_equal(pthread_barrier_destroy(&barrier), 0);
  assert_int_equal(writers[0].rc, LC_OK);
  assert_int_equal(writers[1].rc, LC_OK);
  assert_int_equal(writers[0].created + writers[1].created, 32U);
  assert_int_equal(writers[0].duplicates + writers[1].duplicates, 32U);
  check(f, lc_collection_describe(f->a, &info, &f->error));
  assert_true(info.upper_position == 32U && info.live_count == 32U);
  check(f, read_into_memory(f, NULL));
  assert_true(f->read.completed_records == 32U);
}

static void test_pending_and_torn_active_tail_repair(void **state) {
  fixture *f = *state;
  for (int mode = 0; mode < 2; ++mode) {
    char name[32];
    snprintf(name, sizeof(name), "repair-%d", mode);
    bind_collection(f, name, 1, &f->a);
    append(f, "committed", "{}", 1U);
    lc_collection_handle *handle = f->a->impl;
    lc_pouch_record_header header = {0};
    unsigned char bytes[LC_POUCH_RECORD_HEADER_BYTES];
    header.type = 1U; /* Existing canonical state PUT tag. */
    header.flags = LC_POUCH_RECORD_FLAG_PENDING;
    lc_pouch_record_header_encode(&header, bytes);
    assert_true(lseek(handle->active_fd, 0, SEEK_END) >= 0);
    size_t size = mode == 0 ? 7U : sizeof(bytes);
    assert_int_equal(write(handle->active_fd, bytes, size), size);
    handle->trusted =
        0; /* Simulates an unclean session, not a clean checkpoint. */
    close_handles(f);
    open_client(f, NULL, 0, 0);
    bind_collection(f, name, 0, &f->a);
    append(f, "after-repair", "{}", 2U);
    check(f, read_into_memory(f, NULL));
    assert_output(f, "{}\n{}\n");
    lc_collection_close(f->a);
    f->a = NULL;
  }
}

static void test_peer_client_refresh_and_optional_error_output(void **state) {
  fixture *f = *state;
  lc_collection_append_status_res status;
  lc_collection_config config = {NULL, "events"};
  bind_collection(f, "events", 1, &f->a);
  lc_client *owner = f->client;
  f->client = NULL;
  open_client(f, NULL, 0, 0);
  check(f, lc_client_open_collection(f->client, &config, &f->b, NULL));
  for (unsigned int i = 0; i < 70U; ++i) {
    char id[32];
    snprintf(id, sizeof(id), "peer-%u", i);
    append(f, id, "{}", i + 1U);
  }
  assert_int_equal(lc_collection_append_status(f->b, "peer-69", &status, NULL),
                   LC_OK);
  assert_true(status.state == LC_COLLECTION_APPEND_COMMITTED &&
              status.entry.position == 70U);
  lc_client_close(owner);
  lc_collection_close(f->a);
  f->a = f->b;
  f->b = NULL;
  check(f, read_into_memory(f, NULL));
  assert_true(f->read.completed_records == 70U);
}

static void test_remote_unsupported_without_network_requests(void **state) {
  fixture *f = *state;
  lc_client_config config;
  const char *endpoints[] = {"http://127.0.0.1:1"};
  lc_collection_config collection = {NULL, "events"};
  lc_collection_capabilities capabilities;
  close_handles(f);
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.disable_mtls = 1;
  check(f, lc_client_open(&config, &f->client, &f->error));
  check(f,
        lc_client_collection_capabilities(f->client, &capabilities, &f->error));
  assert_true(!capabilities.collections && !capabilities.lql_filter &&
              !capabilities.resume);
  assert_int_equal(
      lc_client_create_collection(f->client, &collection, &f->a, &f->error),
      LC_ERR_UNSUPPORTED);
  assert_null(f->a);
  assert_string_equal(f->error.server_code, "collections_unsupported");
}

typedef struct compact_context {
  fixture *f;
  int phase;
  int crash;
  int fired;
} compact_context;

static int append_during_compaction(void *context, lc_collection *collection,
                                    int phase, lc_error *error) {
  fixture *f = context;
  lc_collection_append_res out;
  lc_collection_delete_res deleted;
  (void)collection;
  (void)error;
  if (phase == LC_COLLECTION_COMPACT_CAPTURED) {
    check(f, append_json(f, f->b, "four", "{\"n\":4}", &out));
    assert_true(out.position == 4U);
    check(f, lc_collection_delete_entry(f->b, 3U, &deleted, &f->error));
  } else if (phase == LC_COLLECTION_COMPACT_COPIED) {
    check(f, append_json(f, f->b, "five", "{\"n\":5}", &out));
    assert_true(out.position == 5U);
  }
  return LC_OK;
}

typedef struct recover_during_copy {
  fixture *f;
  char query_id[LC_XID_STRING_SIZE];
  int recovered;
} recover_during_copy;

static int recover_projection(void *context, lc_collection *collection,
                              int phase, lc_error *error) {
  recover_during_copy *work = context;
  lc_collection_handle *peer = work->f->b->impl;
  lc_collection_info info;
  (void)collection;
  if (phase != LC_COLLECTION_COMPACT_COPIED)
    return LC_OK;
  assert_int_equal(unlink(peer->order_path), 0);
  int rc = lc_collection_describe(work->f->b, &info, error);
  if (rc == LC_OK) {
    strcpy(work->query_id, peer->order.query_id);
    work->recovered = 1;
  }
  return rc;
}

static void
test_compaction_does_not_restore_a_retired_query_identity(void **state) {
  fixture *f = *state;
  lc_pouch_maintenance_result maintained = {0};
  recover_during_copy work = {0};
  char prior[LC_XID_STRING_SIZE];
  seed_three(f);
  bind_collection(f, "events", 0, &f->b);
  strcpy(prior, ((lc_collection_handle *)f->a->impl)->order.query_id);
  work.f = f;
  lc_collection_compact_test_hook = recover_projection;
  lc_collection_compact_test_context = &work;
  assert_int_equal(maintain(f, 1, 0, &maintained), LC_ERR_PROTOCOL);
  assert_true(work.recovered);
  assert_string_not_equal(prior, work.query_id);
  lc_collection_compact_test_hook = NULL;
  check(f, read_into_memory(f, NULL));
  assert_output(f, "{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n");
  assert_string_equal(((lc_collection_handle *)f->a->impl)->order.query_id,
                      work.query_id);
  append(f, "after-recovery", "{}", 4);
  check(f, maintain(f, 1, 0, &maintained));
  lc_pouch_maintenance_result_cleanup(NULL, &maintained);
  check(f, read_into_memory(f, NULL));
  assert_output(f, "{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n{}\n");
  assert_string_equal(((lc_collection_handle *)f->a->impl)->order.query_id,
                      work.query_id);
}

static void
test_compaction_merges_tail_and_preserves_retry_bindings(void **state) {
  fixture *f = *state;
  lc_pouch_maintenance_result maintained = {0};
  lc_collection_delete_res deleted;
  lc_collection_append_status_res status;
  lc_collection_append_res retry;
  lc_collection_info info;
  char old_path[512];
  struct stat projection;
  struct stat refreshed;
  seed_three(f);
  bind_collection(f, "events", 0, &f->b);
  check(f, lc_collection_delete_entry(f->a, 2U, &deleted, &f->error));
  lc_collection_handle *handle = f->a->impl;
  char *allocated = NULL, leaf[64];
  check(f, lc_collection_record_path(handle, 1U, &allocated, leaf, &f->error));
  assert_true(strlen(allocated) < sizeof(old_path));
  strcpy(old_path, allocated);
  lc_free_with_allocator(&handle->allocator, allocated);
  lc_collection_compact_test_hook = append_during_compaction;
  lc_collection_compact_test_context = f;
  check(f, maintain(f, 1, 0, &maintained));
  lc_collection_compact_test_hook = NULL;
  assert_int_equal(maintained.compacted_collection_count, 1U);
  assert_true(maintained.cleanup_deleted_count > 0U);
  assert_int_equal(stat(old_path, &projection), -1);
  assert_int_equal(errno, ENOENT);
  assert_int_equal(stat(handle->order_path, &projection), 0);
  check(f, lc_collection_describe(f->a, &info, &f->error));
  assert_true(info.upper_position == 5U && info.live_count == 3U);
  assert_int_equal(stat(handle->order_path, &refreshed), 0);
  assert_true(projection.st_dev == refreshed.st_dev &&
              projection.st_ino == refreshed.st_ino);
  check(f, lc_collection_append_status(f->a, "two", &status, &f->error));
  assert_int_equal(status.state, LC_COLLECTION_APPEND_DELETED);
  check(f, lc_collection_append_status(f->b, "three", &status, &f->error));
  assert_int_equal(status.state, LC_COLLECTION_APPEND_DELETED);
  check(f, append_json(f, f->a, "two", "{\"n\":2}", &retry));
  assert_true(retry.duplicate && retry.deleted && retry.position == 2U);
  check(f, read_into_memory(f, NULL));
  assert_output(f, "{\"n\":1}\n{\"n\":4}\n{\"n\":5}\n");
  lc_pouch_maintenance_result_cleanup(NULL, &maintained);
  close_handles(f);
  open_client(f, NULL, 0, 0);
  bind_collection(f, "events", 0, &f->a);
  check(f, read_into_memory(f, NULL));
  assert_output(f, "{\"n\":1}\n{\"n\":4}\n{\"n\":5}\n");
  append(f, "six", "{}", 6U);
}

static void
test_compaction_of_all_deleted_entries_preserves_highwater(void **state) {
  fixture *f = *state;
  lc_pouch_maintenance_result maintained = {0};
  lc_collection_delete_res deleted;
  lc_collection_append_status_res status;
  lc_collection_append_res retry;
  lc_collection_info info;
  seed_three(f);
  for (uint64_t position = 1U; position <= 3U; ++position)
    check(f, lc_collection_delete_entry(f->a, position, &deleted, &f->error));
  check(f, maintain(f, 1, 0, &maintained));
  check(f, lc_collection_describe(f->a, &info, &f->error));
  assert_true(info.upper_position == 3U && info.live_count == 0U);
  check(f, read_into_memory(f, NULL));
  assert_output(f, "");
  check(f, lc_collection_append_status(f->a, "one", &status, &f->error));
  assert_true(status.state == LC_COLLECTION_APPEND_DELETED &&
              status.entry.position == 1U);
  check(f, append_json(f, f->a, "two", "{\"n\":2}", &retry));
  assert_true(retry.deleted && retry.duplicate && retry.position == 2U);
  append(f, "four", "{}", 4U);
  check(f, maintain(f, 1, 0, &maintained));
  check(f, read_into_memory(f, NULL));
  assert_output(f, "{}\n");
  check(f, lc_collection_append_status(f->a, "one", &status, &f->error));
  assert_int_equal(status.state, LC_COLLECTION_APPEND_DELETED);
  lc_pouch_maintenance_result_cleanup(NULL, &maintained);
}

static int compact_from_reader(void *context,
                               const lc_collection_read_metadata *item,
                               lc_error *error) {
  compact_context *work = context;
  lc_pouch_maintenance_result out = {0};
  (void)error;
  if (item->kind == LC_COLLECTION_READ_METADATA_ENTRY && !work->fired) {
    work->fired = 1;
    check(work->f, maintain(work->f, 1, 0, &out));
    assert_true(out.compacted_collection_count == 1U &&
                out.cleanup_pending_count > 0U);
    lc_pouch_maintenance_result_cleanup(NULL, &out);
  }
  return LC_OK;
}

static void
test_compaction_pins_old_readers_without_pinning_new_readers(void **state) {
  fixture *f = *state;
  compact_context context = {f, 0, 0, 0};
  lc_collection_read_opts opts = {0};
  lc_collection_info info;
  lc_pouch_maintenance_result maintained = {0};
  char old_path[512], leaf[64];
  char *path = NULL;
  struct stat st;
  bind_collection(f, "events", 1, &f->a);
  for (unsigned int i = 0; i < 100U; ++i) {
    char id[32];
    snprintf(id, sizeof(id), "entry-%u", i);
    append(f, id, "{}", i + 1U);
  }
  lc_collection_handle *handle = f->a->impl;
  check(f, lc_collection_record_path(handle, 1U, &path, leaf, &f->error));
  assert_true(strlen(path) < sizeof(old_path));
  strcpy(old_path, path);
  lc_free_with_allocator(&handle->allocator, path);
  opts.on_metadata = compact_from_reader;
  opts.metadata_context = &context;
  check(f, read_into_memory(f, &opts));
  assert_true(context.fired && f->read.complete &&
              f->read.completed_records == 100U);
  assert_int_equal(stat(old_path, &st), 0);
  check(f, lc_collection_describe(f->a, &info, &f->error));
  check(f, lc_collection_metadata_enter(handle, 0, &f->error));
  int rc = lc_collection_read_pin(handle, handle->head.snapshot, &f->owned_fd,
                                  &f->error);
  lc_collection_metadata_leave(handle);
  check(f, rc);
  check(f, maintain(f, 0, 1, &maintained));
  assert_int_equal(stat(old_path, &st), -1);
  assert_int_equal(errno, ENOENT);
  close(f->owned_fd);
  f->owned_fd = -1;
  assert_true(maintained.cleanup_deleted_count > 0U);
  lc_pouch_maintenance_result_cleanup(NULL, &maintained);
}

static int fail_publication(void *context, lc_collection *collection, int phase,
                            lc_error *error) {
  compact_context *work = context;
  (void)collection;
  if (phase != work->phase)
    return LC_OK;
  work->fired = 1;
  if (work->crash)
    _exit(phase);
  return lc_error_set(error, LC_ERR_TIMEOUT, 0L,
                      "injected compaction publication failure", NULL, NULL,
                      NULL);
}

static void
test_compaction_recovers_partial_projection_publication(void **state) {
  fixture *f = *state;
  lc_collection_delete_res deleted;
  lc_collection_append_status_res status;
  lc_pouch_maintenance_result out = {0};
  for (int phase = LC_COLLECTION_COMPACT_HEAD_PUBLISHED;
       phase <= LC_COLLECTION_COMPACT_IDS_PUBLISHED; ++phase) {
    char name[32];
    snprintf(name, sizeof(name), "publication-%d", phase);
    bind_collection(f, name, 1, &f->a);
    append(f, "one", "{\"n\":1}", 1U);
    append(f, "two", "{\"n\":2}", 2U);
    check(f, lc_collection_delete_entry(f->a, 1U, &deleted, &f->error));
    compact_context work = {f, phase, 0, 0};
    lc_collection_compact_test_hook = fail_publication;
    lc_collection_compact_test_context = &work;
    assert_int_equal(lc_collection_compact(f->a, 1, 0, &out, &f->error),
                     LC_ERR_TIMEOUT);
    assert_true(work.fired);
    lc_collection_compact_test_hook = NULL;
    check(f, read_into_memory(f, NULL));
    assert_output(f, "{\"n\":2}\n");
    check(f, lc_collection_append_status(f->a, "one", &status, &f->error));
    assert_int_equal(status.state, LC_COLLECTION_APPEND_DELETED);
    append(f, "three", "{}", 3U);
    check(f, maintain(f, 0, 1, &out));
    lc_collection_close(f->a);
    f->a = NULL;
  }
  lc_pouch_maintenance_result_cleanup(NULL, &out);
}

static void
test_crash_compaction_recovers_at_every_publication_boundary(void **state) {
  fixture *f = *state;
  lc_pouch_maintenance_result out = {0};
  for (int phase = LC_COLLECTION_COMPACT_CAPTURED;
       phase <= LC_COLLECTION_COMPACT_IDS_PUBLISHED; ++phase) {
    char name[32];
    snprintf(name, sizeof(name), "crash-%d", phase);
    close_handles(f);
    pid_t child = fork();
    assert_true(child >= 0);
    if (child == 0) {
      lc_collection_delete_res deleted;
      open_client(f, NULL, 0, 0);
      bind_collection(f, name, 1, &f->a);
      append(f, "one", "{\"n\":1}", 1U);
      append(f, "two", "{\"n\":2}", 2U);
      check(f, lc_collection_delete_entry(f->a, 1U, &deleted, &f->error));
      compact_context work = {f, phase, 1, 0};
      lc_collection_compact_test_hook = fail_publication;
      lc_collection_compact_test_context = &work;
      (void)lc_collection_compact(f->a, 1, 0, &out, &f->error);
      _exit(90);
    }
    int status;
    assert_int_equal(waitpid(child, &status, 0), child);
    assert_true(WIFEXITED(status) && WEXITSTATUS(status) == phase);
    open_client(f, NULL, 0, 0);
    root_pouch(f)->compaction_delete_grace_seconds = 0U;
    bind_collection(f, name, 0, &f->a);
    check(f, read_into_memory(f, NULL));
    assert_output(f, "{\"n\":2}\n");
    lc_collection_append_status_res observed;
    check(f, lc_collection_append_status(f->a, "one", &observed, &f->error));
    assert_int_equal(observed.state, LC_COLLECTION_APPEND_DELETED);
    append(f, "three", "{}", 3U);
    check(f, maintain(f, 1, 0, &out));
    check(f, read_into_memory(f, NULL));
    assert_output(f, "{\"n\":2}\n{}\n");
    lc_pouch_maintenance_result_cleanup(NULL, &out);
  }
}

static void test_collection_drop_and_orphan_cleanup_do_not_touch_replacements(
    void **state) {
  fixture *f = *state;
  lc_pouch_maintenance_result out = {0};
  lc_collection_append_res appended;
  struct stat st;
  char old_path[512];
  seed_three(f);
  lc_collection_handle *handle = f->a->impl;
  assert_true(strlen(handle->directory) < sizeof(old_path));
  strcpy(old_path, handle->directory);
  check(f, lc_collection_drop(f->a, &f->error));
  check(f, maintain(f, 0, 1, &out));
  assert_true(out.cleanup_pending_count > 0U);
  assert_int_equal(stat(old_path, &st), 0);
  bind_collection(f, "events", 1, &f->b);
  check(f, append_json(f, f->b, "replacement", "{}", &appended));
  assert_true(appended.position == 1U);
  lc_collection_close(f->a);
  f->a = f->b;
  f->b = NULL;
  check(f, maintain(f, 0, 1, &out));
  assert_int_equal(stat(old_path, &st), -1);
  assert_int_equal(errno, ENOENT);
  check(f, read_into_memory(f, NULL));
  assert_output(f, "{}\n");
  handle = f->a->impl;
  char *data =
      lc_pouch_path_join(&handle->allocator, handle->catalog_base, "data");
  char id[LC_XID_STRING_SIZE];
  check(f, lc_xid_new(id, &f->error));
  char *orphan = lc_pouch_path_join(&handle->allocator, data, id);
  assert_non_null(data);
  assert_non_null(orphan);
  assert_int_equal(mkdir(orphan, 0700), 0);
  check(f, maintain(f, 0, 1, &out));
  assert_int_equal(stat(orphan, &st), -1);
  assert_int_equal(errno, ENOENT);
  char *identity =
      lc_pouch_path_join(&handle->allocator, handle->directory, "identity");
  assert_non_null(identity);
  assert_int_equal(unlink(identity), 0);
  check(f, maintain(f, 0, 1, &out));
  assert_int_equal(stat(identity, &st), 0);
  check(f, read_into_memory(f, NULL));
  assert_output(f, "{}\n");
  lc_free_with_allocator(&handle->allocator, data);
  lc_free_with_allocator(&handle->allocator, orphan);
  lc_free_with_allocator(&handle->allocator, identity);
  lc_pouch_maintenance_result_cleanup(NULL, &out);
}

static void
test_compaction_grace_does_not_restart_on_later_compactions(void **state) {
  fixture *f = *state;
  lc_pouch_maintenance_result out = {0};
  unsigned char before[16], after[16];
  char marker[600];
  struct stat st;
  seed_three(f);
  lc_collection_handle *handle = f->a->impl;
  root_pouch(f)->compaction_delete_grace_seconds = 3600U;
  assert_true(snprintf(marker, sizeof(marker),
                       "%s/.obsolete-seg-00000000000000000001.log",
                       handle->directory) > 0);
  check(f, maintain(f, 1, 0, &out));
  assert_true(out.cleanup_pending_count > 0U);
  int fd = open(marker, O_RDONLY);
  assert_true(fd >= 0);
  assert_int_equal(read(fd, before, sizeof(before)), sizeof(before));
  close(fd);
  append(f, "four", "{}", 4U);
  check(f, maintain(f, 1, 0, &out));
  fd = open(marker, O_RDONLY);
  assert_true(fd >= 0);
  assert_int_equal(read(fd, after, sizeof(after)), sizeof(after));
  close(fd);
  assert_memory_equal(before, after, sizeof(before));
  root_pouch(f)->compaction_delete_grace_seconds = 0U;
  check(f, maintain(f, 0, 1, &out));
  assert_int_equal(stat(marker, &st), -1);
  assert_int_equal(errno, ENOENT);
  check(f, read_into_memory(f, NULL));
  assert_true(f->read.completed_records == 4U);
  lc_pouch_maintenance_result_cleanup(NULL, &out);
}

static void test_compaction_policy_and_existing_background_pass(void **state) {
  fixture *f = *state;
  lc_pouch_maintenance_result out = {0};
  lc_collection_delete_res deleted;
  lc_collection_info info;
  seed_three(f);
  root_pouch(f)->compaction_min_reclaimable_bytes = 1U;
  check(f, maintain(f, 0, 0, &out));
  assert_int_equal(out.compacted_collection_count, 0U);
  check(f, lc_collection_delete_entry(f->a, 2U, &deleted, &f->error));
  root_pouch(f)->background_compaction_enabled = 1;
  lc_pouch_test_compaction_run_pass(root_pouch(f));
  root_pouch(f)->background_compaction_enabled = 0;
  check(f, lc_collection_describe(f->a, &info, &f->error));
  lc_collection_handle *handle = f->a->impl;
  assert_true(handle->head.snapshot != 0U && info.live_count == 2U);
  check(f, read_into_memory(f, NULL));
  assert_output(f, "{\"n\":1}\n{\"n\":3}\n");
  lc_pouch_maintenance_result_cleanup(NULL, &out);
}

static void
test_collection_discovery_is_durable_fair_and_retires_empty_namespaces(
    void **state) {
  fixture *f = *state;
  lc_collection_config config = {"aaa", "events"};
  lc_pouch_maintenance_options options = {0};
  lc_pouch_maintenance_result out = {0};
  char *ns = NULL, *cursor = NULL, *next = NULL;
  const char *expected[] = {"aaa", "journal", "z-last", "aaa"};
  bind_collection(f, "events", 1, &f->a);
  check(f, lc_client_create_collection(f->client, &config, &f->b, &f->error));
  lc_collection_close(f->b);
  f->b = NULL;
  config.ns = "z-last";
  check(f, lc_client_create_collection(f->client, &config, &f->b, &f->error));
  for (size_t i = 0U; i < sizeof(expected) / sizeof(expected[0]); ++i) {
    check(f, lc_collection_maintenance_next(root_pouch(f), cursor, &ns, &next,
                                            &f->error));
    assert_non_null(ns);
    assert_string_equal(ns, expected[i]);
    lc_free_with_allocator(NULL, ns);
    lc_free_with_allocator(NULL, cursor);
    ns = NULL;
    cursor = next;
    next = NULL;
  }
  lc_free_with_allocator(NULL, cursor);
  cursor = NULL;
  close_handles(f);
  open_client(f, NULL, 0, 0);
  check(f, lc_collection_maintenance_next(root_pouch(f), NULL, &ns, &cursor,
                                          &f->error));
  assert_string_equal(ns, "aaa");
  lc_free_with_allocator(NULL, ns);
  lc_free_with_allocator(NULL, cursor);
  ns = cursor = NULL;
  config.ns = "aaa";
  check(f, lc_client_open_collection(f->client, &config, &f->a, &f->error));
  check(f, lc_collection_drop(f->a, &f->error));
  lc_collection_close(f->a);
  f->a = NULL;
  options.ns = "aaa";
  options.cleanup_only = 1;
  check(f, lc_collection_maintenance_run(root_pouch(f), &options, &out,
                                         &f->error));
  assert_true(out.cleanup_deleted_count > 0U);
  check(f, lc_collection_maintenance_next(root_pouch(f), NULL, &ns, &cursor,
                                          &f->error));
  assert_string_equal(ns, "journal");
  lc_free_with_allocator(NULL, ns);
  lc_free_with_allocator(NULL, cursor);
  lc_pouch_maintenance_result_cleanup(NULL, &out);
}

typedef struct catalog_contention {
  lc_source pub;
  fixture *f;
  pthread_t owner;
  pthread_t contender;
  pthread_mutex_t mutex;
  pthread_cond_t condition;
  int signalled;
  int started;
  int sent;
  int result;
} catalog_contention;

static void catalog_before_root(void *context, lc_pouch *pouch) {
  catalog_contention *work = context;
  (void)pouch;
  if (pthread_equal(pthread_self(), work->owner))
    return;
  pthread_mutex_lock(&work->mutex);
  work->signalled = 1;
  pthread_cond_signal(&work->condition);
  pthread_mutex_unlock(&work->mutex);
}

static void *catalog_contender(void *context) {
  catalog_contention *work = context;
  lc_collection_config config = {NULL, "other-collection"};
  lc_collection *collection = NULL;
  lc_error error;
  lc_error_init(&error);
  work->result = lc_client_create_collection(work->f->client, &config,
                                             &collection, &error);
  lc_collection_close(collection);
  lc_error_cleanup(&error);
  return NULL;
}

static size_t catalog_source_read(lc_source *self, void *bytes, size_t capacity,
                                  lc_error *error) {
  catalog_contention *work = (catalog_contention *)self;
  lc_collection_handle *handle = work->f->a->impl;
  lc_collection_config config = {NULL, "events"};
  lc_collection *fresh = NULL;
  struct timespec deadline;
  char *path;
  int fd;
  int rc;
  if (work->sent)
    return 0U;
  rc = pthread_create(&work->contender, NULL, catalog_contender, work);
  if (rc != 0) {
    lc_error_set(error, LC_ERR_TRANSPORT, 0,
                 "failed to start catalog contender", strerror(rc), NULL, NULL);
    return 0U;
  }
  work->started = 1;
  clock_gettime(CLOCK_REALTIME, &deadline);
  deadline.tv_sec += 10;
  pthread_mutex_lock(&work->mutex);
  while (!work->signalled) {
    rc = pthread_cond_timedwait(&work->condition, &work->mutex, &deadline);
    if (rc != 0)
      break;
  }
  pthread_mutex_unlock(&work->mutex);
  if (rc != 0) {
    lc_error_set(error, LC_ERR_TIMEOUT, 0,
                 "catalog contender did not reach root authority", NULL, NULL,
                 NULL);
    return 0U;
  }
  /* If catalog were acquired before root, the contender would hold it while
   * waiting for this callback's root mutation guard. Probe without blocking. */
  path = lc_pouch_path_join(&handle->allocator, handle->catalog_base,
                            ".catalog.lock");
  if (path == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0, "failed to allocate catalog probe",
                 NULL, NULL, NULL);
    return 0U;
  }
  fd = open(path, O_RDWR | O_CLOEXEC);
  lc_free_with_allocator(&handle->allocator, path);
  if (fd < 0) {
    lc_error_set(error, LC_ERR_TRANSPORT, 0, "failed to open catalog probe",
                 NULL, NULL, NULL);
    return 0U;
  }
  rc = flock(fd, LOCK_EX | LOCK_NB);
  if (rc == 0)
    (void)flock(fd, LOCK_UN);
  close(fd);
  if (rc != 0) {
    lc_error_set(error, LC_ERR_TIMEOUT, 0,
                 "inverted collection catalog/root lock order", NULL, NULL,
                 NULL);
    return 0U;
  }
  rc = lc_client_open_collection(work->f->client, &config, &fresh, error);
  lc_collection_close(fresh);
  if (rc != LC_OK)
    return 0U;
  if (capacity < 2U) {
    lc_error_set(error, LC_ERR_PROTOCOL, 0, "unexpected source capacity", NULL,
                 NULL, NULL);
    return 0U;
  }
  memcpy(bytes, "{}", 2U);
  work->sent = 1;
  return 2U;
}

static void
test_source_callback_catalog_contention_preserves_lock_order(void **state) {
  fixture *f = *state;
  catalog_contention work = {0};
  lc_collection_append_req request = {"outer"};
  lc_collection_append_res out;
  bind_collection(f, "events", 1, &f->a);
  append(f, "prior", "{}", 1U);
  work.f = f;
  work.owner = pthread_self();
  work.pub.read = catalog_source_read;
  assert_int_equal(pthread_mutex_init(&work.mutex, NULL), 0);
  assert_int_equal(pthread_cond_init(&work.condition, NULL), 0);
  lc_collection_root_test_hook = catalog_before_root;
  lc_collection_root_test_context = &work;
  int rc = lc_collection_append(f->a, &request, &work.pub, &out, &f->error);
  if (work.started)
    assert_int_equal(pthread_join(work.contender, NULL), 0);
  lc_collection_root_test_hook = NULL;
  lc_collection_root_test_context = NULL;
  assert_int_equal(pthread_cond_destroy(&work.condition), 0);
  assert_int_equal(pthread_mutex_destroy(&work.mutex), 0);
  check(f, rc);
  assert_int_equal(work.result, LC_OK);
  assert_true(work.signalled && out.position == 2U);
}

static int collection_clock(clockid_t id, struct timespec *out, void *context) {
  fixture *f = context;
  (void)id;
  out->tv_sec = (time_t)f->now;
  out->tv_nsec = 0;
  return 0;
}

static void set_collection_clock(fixture *f, lc_pouch_unix_seconds now) {
  f->now = now;
  lc_pouch_test_queue_clock_gettime = collection_clock;
  lc_pouch_test_queue_time_context = f;
}

static void
test_retention_metadata_defaults_preservation_and_clearing(void **state) {
  fixture *f = *state;
  lc_collection_config config = {0};
  lc_collection_info info;
  lc_collection_append_res appended, changed;
  lc_collection_append_status_res status;
  lc_collection_delete_res deleted;
  lc_collection_metadata_req req;
  set_collection_clock(f, 1000);
  config.name = "events";
  config.has_retention_seconds = 1;
  config.retention_seconds = 120;
  check(f, lc_client_create_collection(f->client, &config, &f->a, &f->error));
  check(f, lc_collection_describe(f->a, &info, &f->error));
  assert_int_equal(info.retention_support, LC_STATE_RETENTION_SUPPORTED);
  assert_true(info.has_retention_expires_at_unix);
  assert_int_equal(info.retention_expires_at_unix, 1120);
  f->now = 1010;
  check(f, append_retention(f, f->a, "one", "{\"n\":1}", 1, 60, &appended));
  assert_int_equal(appended.retention_expires_at_unix, 1070);
  check(f, append_json(f, f->a, "two", "{\"n\":2}", &changed));
  assert_true(changed.has_retention_expires_at_unix);
  assert_int_equal(changed.retention_expires_at_unix, 0);
  f->now = 1050;
  check(f, append_retention(f, f->a, "one", "{ \"n\" : 1 }", 1, 999, &changed));
  assert_true(changed.duplicate);
  assert_int_equal(changed.retention_expires_at_unix, 1070);
  lc_collection_metadata_req_init(&req);
  req.retention_seconds = -1; /* absent values are ignored */
  check(f, lc_collection_metadata(f->a, &req, &info, &f->error));
  assert_int_equal(info.retention_expires_at_unix, 1120);
  check(f, lc_collection_entry_metadata(f->a, 1, &req, &changed, &f->error));
  assert_int_equal(changed.retention_expires_at_unix, 1070);
  req.has_retention_seconds = 1;
  assert_int_equal(
      lc_collection_entry_metadata(f->a, 1, &req, &changed, &f->error),
      LC_ERR_INVALID);
  assert_int_equal(lc_collection_metadata(f->a, &req, &info, &f->error),
                   LC_ERR_INVALID);
  /* Expiry eligibility has no foreground effect, including on metadata edits.
   */
  f->now = 1100;
  req.retention_seconds = 100;
  check(f, f->a->entry_metadata(f->a, 1, &req, &changed, &f->error));
  assert_int_equal(changed.position, appended.position);
  assert_int_equal(changed.bytes, appended.bytes);
  assert_string_equal(changed.etag, appended.etag);
  assert_int_equal(changed.retention_expires_at_unix, 1200);
  check(f, read_into_memory(f, NULL));
  assert_output(f, "{\"n\":1}\n{\"n\":2}\n");
  req.retention_seconds = 0;
  check(f, f->a->metadata(f->a, &req, &info, &f->error));
  assert_int_equal(info.retention_expires_at_unix, 0);
  check(f, lc_collection_entry_metadata(f->a, 1, &req, &changed, &f->error));
  assert_int_equal(changed.retention_expires_at_unix, 0);
  check(f, lc_collection_describe(f->a, &info, &f->error));
  assert_int_equal(info.upper_position, 2);
  assert_int_equal(info.live_count, 2);
  assert_int_equal(
      lc_collection_entry_metadata(f->a, 99, &req, &changed, &f->error),
      LC_ERR_INVALID);
  assert_string_equal(f->error.server_code, "entry_not_found");
  check(f, lc_collection_delete_entry(f->a, 1, &deleted, &f->error));
  assert_int_equal(
      lc_collection_entry_metadata(f->a, 1, &req, &changed, &f->error),
      LC_ERR_INVALID);
  assert_string_equal(f->error.server_code, "entry_not_found");
  check(f, append_retention(f, f->a, "one", "{\"n\":1}", 1, 500, &changed));
  assert_true(changed.duplicate && changed.deleted);
  assert_false(changed.has_retention_expires_at_unix);
  check(f, lc_collection_append_status(f->a, "one", &status, &f->error));
  assert_int_equal(status.state, LC_COLLECTION_APPEND_DELETED);
  assert_int_equal(status.entry.position, 1);
}

static void
test_collection_expiry_drops_empty_parent_and_all_children(void **state) {
  fixture *f = *state;
  lc_collection_config config = {0};
  lc_collection_metadata_req req = {1, 0};
  lc_collection *unexpected = NULL;
  lc_collection_info info;
  lc_collection_append_res entry;
  lc_pouch_maintenance_result result = {0};
  char prior[LC_XID_STRING_SIZE];
  set_collection_clock(f, 1000);
  config.name = "events";
  config.has_retention_seconds = 1;
  config.retention_seconds = 30;
  check(f, lc_client_create_collection(f->client, &config, &f->a, &f->error));
  strcpy(prior, f->a->collection_id);
  f->now = 1030;
  check(f, maintain(f, 0, 1, &result)); /* cleanup-only never expires objects */
  check(f, lc_collection_describe(f->a, &info, &f->error));
  assert_int_equal(info.live_count, 0);
  check(f, lc_collection_metadata(f->a, &req, &info, &f->error));
  check(f, maintain(f, 0, 0, &result));
  check(f, lc_collection_describe(f->a, &info, &f->error));
  assert_int_equal(info.retention_expires_at_unix, 0);
  req.retention_seconds = 10;
  check(f, lc_collection_metadata(f->a, &req, &info, &f->error));
  assert_int_equal(info.retention_expires_at_unix, 1040);
  f->now = 1040;
  check(f, maintain(f, 0, 0, &result));
  assert_int_equal(result.retention_expired_count, 1);
  assert_int_equal(lc_collection_describe(f->a, &info, &f->error),
                   LC_ERR_INVALID);
  assert_string_equal(f->error.server_code, "collection_stale");
  config.has_retention_seconds = 0;
  assert_int_equal(
      lc_client_open_collection(f->client, &config, &f->b, &f->error),
      LC_ERR_INVALID);
  assert_string_equal(f->error.server_code, "collection_not_found");
  config.has_retention_seconds = 1;
  config.retention_seconds = 10;
  check(f, lc_client_create_collection(f->client, &config, &f->b, &f->error));
  assert_string_not_equal(prior, f->b->collection_id);
  check(f, append_retention(f, f->b, "long-child", "{}", 1, 1000, &entry));
  check(f, append_json(f, f->b, "no-expiry-child", "{}", &entry));
  assert_int_equal(
      lc_client_open_collection(f->client, &config, &unexpected, &f->error),
      LC_ERR_INVALID);
  assert_null(unexpected);
  f->now = 1050;
  check(f, maintain(f, 0, 0, &result));
  assert_int_equal(lc_collection_describe(f->b, &info, &f->error),
                   LC_ERR_INVALID);
  assert_string_equal(f->error.server_code, "collection_stale");
  lc_pouch_maintenance_result_cleanup(NULL, &result);
}

static void
test_entry_expiry_preserves_gaps_retry_identity_and_empty_parent(void **state) {
  fixture *f = *state;
  lc_collection_append_res entry;
  lc_collection_append_status_res status;
  lc_collection_metadata_req req = {1, 5};
  lc_collection_info info;
  lc_pouch_maintenance_result result = {0};
  set_collection_clock(f, 1000);
  bind_collection(f, "events", 1, &f->a);
  check(f, append_retention(f, f->a, "one", "{\"n\":1}", 1, 10, &entry));
  check(f, append_retention(f, f->a, "two", "{\"n\":2}", 1, 20, &entry));
  append(f, "three", "{\"n\":3}", 3);
  f->now = 1010;
  check(f, read_into_memory(f, NULL));
  assert_output(f, "{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n");
  check(f, maintain(f, 0, 0, &result));
  check(f, read_into_memory(f, NULL));
  assert_output(f, "{\"n\":2}\n{\"n\":3}\n");
  check(f, lc_collection_append_status(f->a, "one", &status, &f->error));
  assert_int_equal(status.state, LC_COLLECTION_APPEND_DELETED);
  check(f, append_retention(f, f->a, "one", "{\"n\":1}", 1, 99, &entry));
  assert_true(entry.duplicate && entry.deleted && entry.position == 1);
  f->now = 1020;
  check(f, lc_collection_entry_metadata(f->a, 3, &req, &entry, &f->error));
  check(f, maintain(f, 1, 0, &result));
  check(f, read_into_memory(f, NULL));
  assert_output(f, "{\"n\":3}\n");
  f->now = 1025;
  check(f, maintain(f, 1, 0, &result));
  check(f, lc_collection_describe(f->a, &info, &f->error));
  assert_int_equal(info.live_count, 0);
  assert_int_equal(info.upper_position, 3);
  assert_int_equal(info.retention_expires_at_unix, 0);
  append(f, "four", "{}", 4);
  check(f, read_into_memory(f, NULL));
  assert_output(f, "{}\n");
  lc_pouch_maintenance_result_cleanup(NULL, &result);
}

typedef struct retention_read_context {
  fixture *f;
  int expire;
  int changed;
  lc_unix_seconds expected;
  unsigned int seen;
} retention_read_context;

static int retention_during_read(void *context,
                                 const lc_collection_read_metadata *item,
                                 lc_error *error) {
  retention_read_context *work = context;
  lc_collection_append_res entry;
  lc_collection_metadata_req req = {1, 200};
  lc_pouch_maintenance_result maintained = {0};
  (void)error;
  if (item->kind != LC_COLLECTION_READ_METADATA_ENTRY)
    return LC_OK;
  if (item->position == 90) {
    assert_true(item->has_retention_expires_at_unix);
    assert_int_equal(item->retention_expires_at_unix, work->expected);
    ++work->seen;
  }
  if (!work->changed) {
    work->changed = 1;
    if (work->expire)
      work->f->now = 1020;
    else {
      check(work->f, lc_collection_entry_metadata(work->f->b, 90, &req, &entry,
                                                  &work->f->error));
      req.retention_seconds = 300;
      check(work->f, lc_collection_entry_metadata(work->f->b, 90, &req, &entry,
                                                  &work->f->error));
    }
    check(work->f, maintain(work->f, 1, 0, &maintained));
    assert_int_equal(maintained.compacted_collection_count, 1);
    lc_pouch_maintenance_result_cleanup(NULL, &maintained);
  }
  return LC_OK;
}

static void
test_retention_updates_and_janitor_preserve_active_read_view(void **state) {
  fixture *f = *state;
  lc_collection_read_opts opts = {0};
  lc_collection_info info;
  lc_collection_append_res entry;
  for (int expire = 0; expire <= 1; ++expire) {
    retention_read_context work = {f, expire, 0, 1020, 0};
    char name[32];
    if (f->a != NULL)
      check(f, lc_collection_drop(f->a, &f->error));
    close_handles(f);
    open_client(f, NULL, 0, 0);
    root_pouch(f)->compaction_delete_grace_seconds = 0;
    set_collection_clock(f, 1000);
    snprintf(name, sizeof(name), "read-retention-%d", expire);
    bind_collection(f, name, 1, &f->a);
    bind_collection(f, name, 0, &f->b);
    for (unsigned int i = 1; i <= 100; ++i) {
      char id[32];
      snprintf(id, sizeof(id), "entry-%u", i);
      check(f, append_retention(f, f->a, id, "{}", 1, 20, &entry));
    }
    opts.on_metadata = retention_during_read;
    opts.metadata_context = &work;
    check(f, read_into_memory(f, &opts));
    assert_int_equal(f->read.completed_records, 100);
    assert_int_equal(work.seen, 1);
    check(f, lc_collection_describe(f->a, &info, &f->error));
    assert_int_equal(info.live_count, expire ? 0 : 100);
    if (!expire) {
      work.expected = 1300;
      work.seen = 0;
      check(f, read_into_memory(f, &opts));
      assert_int_equal(work.seen, 1);
    } else {
      check(f, read_into_memory(f, NULL));
      assert_int_equal(f->read.completed_records, 0);
      assert_output(f, "");
    }
  }
}

typedef struct retention_compact_context {
  fixture *f;
  int phase;
  int changed;
} retention_compact_context;

static int retention_during_compaction(void *context, lc_collection *self,
                                       int phase, lc_error *error) {
  retention_compact_context *work = context;
  lc_collection_metadata_req req = {1, 100};
  lc_collection_append_res entry;
  (void)self;
  if (phase != work->phase || work->changed)
    return LC_OK;
  work->changed = 1;
  return lc_collection_entry_metadata(work->f->b, 2, &req, &entry, error);
}

static void test_metadata_racing_compaction_defers_and_converges(void **state) {
  fixture *f = *state;
  lc_collection_append_status_res status;
  lc_pouch_maintenance_result result = {0};
  for (int phase = LC_COLLECTION_COMPACT_CAPTURED;
       phase <= LC_COLLECTION_COMPACT_COPIED; ++phase) {
    retention_compact_context work = {f, phase, 0};
    char name[32];
    if (f->a != NULL)
      check(f, lc_collection_drop(f->a, &f->error));
    close_handles(f);
    open_client(f, NULL, 0, 0);
    root_pouch(f)->compaction_delete_grace_seconds = 0;
    set_collection_clock(f, 1000);
    snprintf(name, sizeof(name), "compact-retention-%d", phase);
    bind_collection(f, name, 1, &f->a);
    bind_collection(f, name, 0, &f->b);
    append(f, "one", "{\"n\":1}", 1);
    append(f, "two", "{\"n\":2}", 2);
    lc_collection_compact_test_hook = retention_during_compaction;
    lc_collection_compact_test_context = &work;
    check(f, maintain(f, 1, 0, &result));
    assert_true(work.changed);
    assert_int_equal(result.compacted_collection_count, 0);
    assert_true(result.cleanup_pending_count > 0);
    lc_collection_compact_test_hook = NULL;
    lc_collection_compact_test_context = NULL;
    check(f, read_into_memory(f, NULL));
    assert_output(f, "{\"n\":1}\n{\"n\":2}\n");
    check(f, lc_collection_append_status(f->a, "two", &status, &f->error));
    assert_int_equal(status.entry.retention_expires_at_unix, 1100);
    check(f, maintain(f, 1, 0, &result));
    assert_int_equal(result.compacted_collection_count, 1);
    check(f, lc_collection_append_status(f->a, "two", &status, &f->error));
    assert_int_equal(status.entry.retention_expires_at_unix, 1100);
    close_handles(f);
    open_client(f, NULL, 0, 0);
    bind_collection(f, name, 0, &f->a);
    check(f, read_into_memory(f, NULL));
    assert_output(f, "{\"n\":1}\n{\"n\":2}\n");
    check(f, lc_collection_append_status(f->a, "two", &status, &f->error));
    assert_int_equal(status.entry.retention_expires_at_unix, 1100);
  }
  lc_pouch_maintenance_result_cleanup(NULL, &result);
}

static void test_expiry_rechecks_renewed_and_cleared_metadata(void **state) {
  fixture *f = *state;
  lc_collection_metadata_req req = {1, 10};
  lc_collection_append_res entry;
  lc_collection_delete_res deleted;
  lc_collection_info info;
  int removed;
  set_collection_clock(f, 1000);
  bind_collection(f, "events", 1, &f->a);
  check(f, lc_collection_metadata(f->a, &req, &info, &f->error));
  check(f, append_retention(f, f->a, "one", "{}", 1, 10, &entry));
  f->now = 1010;
  req.retention_seconds = 20;
  check(f, lc_collection_entry_metadata(f->a, 1, &req, &entry, &f->error));
  check(f, lc_collection_expire_entry(f->a, 1, f->now, &deleted, &f->error));
  assert_false(deleted.deleted);
  req.retention_seconds = 0;
  check(f, lc_collection_metadata(f->a, &req, &info, &f->error));
  check(f, lc_collection_expire(f->a, f->now, &removed, &f->error));
  assert_false(removed);
  f->now = 1030;
  check(f, lc_collection_entry_metadata(f->a, 1, &req, &entry, &f->error));
  check(f, lc_collection_expire_entry(f->a, 1, f->now, &deleted, &f->error));
  assert_false(deleted.deleted);
  check(f, read_into_memory(f, NULL));
  assert_output(f, "{}\n");
}

static void
test_retention_survives_rebuild_and_compaction_in_storage_modes(void **state) {
  fixture *f = *state;
  lc_collection_config config = {0};
  lc_collection_metadata_req req = {1, 300};
  lc_collection_append_res entry;
  lc_collection_append_status_res status;
  lc_collection_info info;
  lc_pouch_maintenance_result result = {0};
  char *key = NULL;
  char path[768], name[32];
  check(f, lc_pouch_crypto_generate_key_string(&f->crypto_key, &f->error));
  key = f->crypto_key;
  for (int mode = 0; mode < 4; ++mode) {
    for (int durable = 0; durable <= 1; ++durable) {
      if (f->a != NULL)
        check(f, lc_collection_drop(f->a, &f->error));
      close_handles(f);
      char root[384];
      assert_true(snprintf(root, sizeof(root), "%s/retention-mode-%d-%d",
                           f->root, mode, durable) > 0);
      assert_true(
          snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s", root) > 0);
      open_client(f, mode & 1 ? key : NULL, mode & 2, durable);
      set_collection_clock(f, 1000);
      snprintf(name, sizeof(name), "persist-%d-%d", mode, durable);
      config.name = name;
      config.has_retention_seconds = 1;
      config.retention_seconds = 800;
      check(f,
            lc_client_create_collection(f->client, &config, &f->a, &f->error));
      check(f, append_retention(f, f->a, "one", "{\"raw\":\"body\"}", 1, 60,
                                &entry));
      check(f, lc_collection_entry_metadata(f->a, 1, &req, &entry, &f->error));
      assert_int_equal(entry.retention_expires_at_unix, 1300);
      strcpy(path, ((lc_collection_handle *)f->a->impl)->order_path);
      close_handles(f);
      assert_int_equal(unlink(path), 0);
      open_client(f, mode & 1 ? key : NULL, mode & 2, durable);
      bind_collection(f, name, 0, &f->a);
      check(f, lc_collection_describe(f->a, &info, &f->error));
      assert_int_equal(info.retention_expires_at_unix, 1800);
      check(f, lc_collection_append_status(f->a, "one", &status, &f->error));
      assert_int_equal(status.entry.retention_expires_at_unix, 1300);
      check(f, lc_collection_entry_metadata(f->a, 1, &req, &entry, &f->error));
      check(f, maintain(f, 1, 0, &result));
      check(f, read_into_memory(f, NULL));
      assert_output(f, "{\"raw\":\"body\"}\n");
      close_handles(f);
      assert_int_equal(unlink(path), 0);
      open_client(f, mode & 1 ? key : NULL, mode & 2, durable);
      bind_collection(f, name, 0, &f->a);
      check(f, lc_collection_append_status(f->a, "one", &status, &f->error));
      assert_int_equal(status.entry.retention_expires_at_unix, 1300);
      /* A compacted PUT retains a later metadata generation than publication.
       * Recovery must preserve that watermark before the next metadata edit. */
      check(f, lc_collection_entry_metadata(f->a, 1, &req, &entry, &f->error));
      check(f, read_into_memory(f, NULL));
      assert_output(f, "{\"raw\":\"body\"}\n");
      append(f, "two", "{}", 2);
      lc_pouch_maintenance_result_cleanup(NULL, &result);
    }
  }
}

static void test_retention_discovery_skips_unconfigured_journals(void **state) {
  fixture *f = *state;
  lc_collection_metadata_req req = {1, 10};
  lc_collection_append_res entry;
  lc_collection_info info;
  lc_pouch_maintenance_result result = {0};
  set_collection_clock(f, 1000);
  seed_many(f, 100);
  check(f, maintain(f, 0, 0, &result));
  assert_int_equal(result.retention_scanned_count, 0);
  check(f, lc_collection_entry_metadata(f->a, 90, &req, &entry, &f->error));
  f->now = 1010;
  check(f, maintain(f, 1, 0, &result));
  assert_int_equal(result.retention_deleted_state_count, 1);
  check(f, lc_collection_describe(f->a, &info, &f->error));
  assert_int_equal(info.live_count, 99);
  check(f, maintain(f, 0, 0, &result));
  assert_int_equal(result.retention_scanned_count, 0);
  lc_pouch_maintenance_result_cleanup(NULL, &result);
}

static void
test_query_cleanup_retires_epochs_and_abandoned_temporaries(void **state) {
  fixture *f = *state;
  lc_collection_handle *handle;
  lc_pouch_maintenance_result result = {0};
  char id[LC_XID_STRING_SIZE], temporary[768], obsolete[768], page[800];
  int found;
  indexed_seed(f, 63);
  handle = f->a->impl;
  check(f, lc_xid_new(id, &f->error));
  snprintf(temporary, sizeof(temporary), "%s/query/%s/.page-%s.tmp",
           handle->directory, handle->order.query_id, id);
  f->owned_fd = open(temporary, O_CREAT | O_EXCL | O_RDWR, 0600);
  assert_true(f->owned_fd >= 0);
  close(f->owned_fd);
  f->owned_fd = -1;
  check(f, lc_xid_new(id, &f->error));
  snprintf(obsolete, sizeof(obsolete), "%s/query/%s", handle->directory, id);
  assert_int_equal(mkdir(obsolete, 0700), 0);
  snprintf(page, sizeof(page), "%s/page-0", obsolete);
  f->owned_fd = open(page, O_CREAT | O_EXCL | O_RDWR, 0600);
  assert_true(f->owned_fd >= 0);
  close(f->owned_fd);
  f->owned_fd = -1;
  indexed_mode(f, 0, 0);
  check(f, maintain(f, 0, 1, &result));
  assert_true(result.cleanup_deleted_count >= 2);
  struct stat st;
  assert_int_equal(lstat(temporary, &st), -1);
  assert_int_equal(errno, ENOENT);
  assert_int_equal(lstat(obsolete, &st), -1);
  assert_int_equal(errno, ENOENT);
  indexed_mode(f, 1, 0);
  check(f, load_query(f, NULL, &found));
  assert_true(found && f->query_index.rows == 63);
  lc_pouch_maintenance_result_cleanup(NULL, &result);
}

typedef struct query_publish_context {
  fixture *f;
  int mode;
  int fired;
  int temporary_survived;
  unsigned long pending;
} query_publish_context;

static int query_during_publication(void *context,
                                    lc_collection_handle *collection,
                                    const char *temporary, lc_error *error) {
  query_publish_context *work = context;
  lc_collection_handle *peer = work->f->b->impl;
  lc_collection_info info;
  lc_collection_append_res appended;
  lc_pouch_maintenance_result result = {0};
  struct stat st;
  int rc;
  (void)collection;
  if (work->fired)
    return LC_OK;
  work->fired = 1;
  if (work->mode == 2)
    return append_json(work->f, work->f->b, "entry-63", "{}", &appended);
  if (work->mode == 1) {
    if (unlink(peer->order_path) != 0)
      return lc_error_set(error, LC_ERR_TRANSPORT, 0,
                          "test could not retire ordered projection", NULL,
                          NULL, NULL);
    rc = lc_collection_describe(work->f->b, &info, error);
    if (rc != LC_OK)
      return rc;
  }
  rc = lc_collection_query_cleanup(work->f->b, &result, error);
  work->temporary_survived = lstat(temporary, &st) == 0;
  work->pending = result.cleanup_pending_count;
  lc_pouch_maintenance_result_cleanup(NULL, &result);
  return rc;
}

static void test_query_cleanup_preserves_active_publication(void **state) {
  fixture *f = *state;
  query_publish_context work = {f, 0, 0, 0, 0};
  int found;
  indexed_seed(f, 62);
  bind_collection(f, "events", 0, &f->b);
  lc_collection_query_test_hook = query_during_publication;
  lc_collection_query_test_context = &work;
  check(f, build_query(f, 0));
  lc_collection_query_test_hook = NULL;
  assert_true(work.fired && work.temporary_survived && work.pending > 0);
  check(f, load_query(f, NULL, &found));
  assert_true(found && f->query_index.rows == 62);
}

static void
test_late_query_publication_cannot_revive_a_retired_epoch(void **state) {
  fixture *f = *state;
  query_publish_context work = {f, 1, 0, 0, 0};
  char old[768];
  struct stat st;
  int found;
  indexed_seed(f, 63);
  bind_collection(f, "events", 0, &f->b);
  lc_collection_handle *handle = f->a->impl;
  snprintf(old, sizeof(old), "%s/query/%s", handle->directory,
           handle->order.query_id);
  lc_collection_query_test_hook = query_during_publication;
  lc_collection_query_test_context = &work;
  check(f, build_query(f, 0));
  lc_collection_query_test_hook = NULL;
  assert_true(work.fired && !work.temporary_survived);
  assert_int_equal(lstat(old, &st), -1);
  assert_int_equal(errno, ENOENT);
  check(f, load_query(f, NULL, &found));
  assert_false(found);
  check(f, read_into_memory(f, NULL));
  assert_int_equal(f->read.completed_records, 63);
  check(f, build_query(f, 0));
  check(f, load_query(f, NULL, &found));
  assert_true(found);
}

static void
test_late_partial_query_page_cannot_replace_a_complete_page(void **state) {
  fixture *f = *state;
  query_publish_context work = {f, 2, 0, 0, 0};
  int found;
  indexed_seed(f, 62);
  bind_collection(f, "events", 0, &f->b);
  lc_collection_query_test_hook = query_during_publication;
  lc_collection_query_test_context = &work;
  check(f, build_query(f, 0));
  lc_collection_query_test_hook = NULL;
  assert_true(work.fired);
  check(f, load_query(f, NULL, &found));
  assert_true(found && f->query_index.rows == 63);
}

static void test_selection_ranges_empty_gaps_and_uint64(void **state) {
  fixture *f = *state;
  lc_collection_read_opts opts = {0};
  lc_collection_delete_res deleted;
  char cursor[LC_COLLECTION_CURSOR_BYTES];
  seed_three(f);
  check(f, lc_collection_delete_entry(f->a, 2, &deleted, &f->error));
  opts.has_after = opts.has_through = 1;
  opts.after = 1;
  opts.through = 3;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "{\"n\":3}\n");
  assert_true(f->read.complete && f->read.upper_position == 3);
  opts.after = 0;
  opts.through = 2;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "{\"n\":1}\n");
  assert_int_equal(f->read.last_examined_position, 2);
  opts.after = 2;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "");
  assert_true(f->read.complete && f->read.last_examined_position == 2);
  opts.after = 3;
  assert_int_equal(read_into_memory(f, &opts), LC_ERR_INVALID);
  assert_output(f, "");
  assert_string_equal(f->error.server_code, "selection_invalid");
  opts.after = opts.through = LC_U64_MAX;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "");
  assert_true(f->read.complete && f->read.upper_position == 3 &&
              f->read.last_examined_position == LC_U64_MAX);
  strcpy(cursor, f->read.checkpoint_cursor);
  opts.cursor = cursor;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "");
  opts.cursor = NULL;
  opts.after = 0;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "{\"n\":1}\n{\"n\":3}\n");
  opts.through = 0;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "");
  assert_true(f->read.complete && f->read.upper_position == 0);
  opts.has_after = 2;
  assert_int_equal(read_into_memory(f, &opts), LC_ERR_INVALID);
  opts.has_after = 0;
  opts.has_through = 2;
  assert_int_equal(read_into_memory(f, &opts), LC_ERR_INVALID);
  opts.has_through = 0;
  opts.has_first_n = 2;
  assert_int_equal(read_into_memory(f, &opts), LC_ERR_INVALID);
  opts.has_first_n = 0;
  opts.after = opts.first_n = LC_U64_MAX; /* Unselected values are ignored. */
  check(f, read_into_memory(f, &opts));
  assert_output(f, "{\"n\":1}\n{\"n\":3}\n");
}

static void
test_selection_first_n_complete_windows_and_checkpoints(void **state) {
  fixture *f = *state;
  lc_collection_read_opts opts = {0};
  char cursor[LC_COLLECTION_CURSOR_BYTES];
  const char *expected[] = {"{\"n\":1}\n", "{\"n\":2}\n", "{\"n\":3}\n", ""};
  seed_three(f);
  opts.has_first_n = 1;
  opts.first_n = 1;
  for (size_t i = 0; i < 4; ++i) {
    check(f, read_into_memory(f, &opts));
    assert_output(f, expected[i]);
    assert_true(f->read.complete && f->read.upper_position == 3);
    assert_int_equal(f->read.completed_records, i < 3 ? 1 : 0);
    assert_int_equal(f->read.last_examined_position, i < 3 ? i + 1 : 3);
    assert_null(f->read.resume_cursor);
    strcpy(cursor, f->read.checkpoint_cursor);
    opts.cursor = cursor;
  }
  append(f, "four", "{\"n\":4}", 4);
  check(f, read_into_memory(f, &opts));
  assert_output(f, "{\"n\":4}\n");
  opts.cursor = NULL;
  opts.first_n = 0;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "");
  assert_true(f->read.complete && f->read.completed_records == 0 &&
              f->read.last_examined_position == 0 &&
              f->read.upper_position == 4);
  strcpy(cursor, f->read.checkpoint_cursor);
  opts.cursor = cursor;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "");
  assert_int_equal(f->read.last_examined_position, 0);
  opts.cursor = NULL;
  opts.first_n = LC_U64_MAX;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n{\"n\":4}\n");
  assert_true(f->read.complete && f->read.completed_records == 4);
}

static void
test_selection_first_n_resume_budget_binding_and_accepted_cursor(void **state) {
  fixture *f = *state;
  lc_collection_read_opts opts = {0};
  metadata_context m = {0};
  char cursor[LC_COLLECTION_CURSOR_BYTES];
  seed_three(f);
  bind_collection(f, "events", 0, &f->b);
  m.f = f;
  m.fail_at = 2;
  m.append_peer = 1;
  opts.has_first_n = 1;
  opts.first_n = 2;
  opts.on_metadata = metadata;
  opts.metadata_context = &m;
  assert_int_equal(read_into_memory(f, &opts), LC_ERR_TIMEOUT);
  assert_true(!f->read.complete && f->read.completed_records == 1);
  strcpy(cursor, f->read.resume_cursor);
  opts.cursor = cursor;
  opts.on_metadata = NULL;
  opts.first_n = 3;
  assert_int_equal(read_into_memory(f, &opts), LC_ERR_INVALID);
  assert_output(f, "");
  opts.first_n = 2;
  opts.has_after = 1;
  assert_int_equal(read_into_memory(f, &opts), LC_ERR_INVALID);
  opts.has_after = 0;
  opts.has_through = 1;
  opts.through = 3;
  assert_int_equal(read_into_memory(f, &opts), LC_ERR_INVALID);
  opts.has_through = 0;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "{\"n\":2}\n");
  assert_true(f->read.complete && f->read.completed_records == 1 &&
              f->read.upper_position == 3 &&
              f->read.last_examined_position == 2);
  strcpy(cursor, f->read.checkpoint_cursor);
  check(f, read_into_memory(f, &opts));
  assert_output(f, "{\"n\":3}\n{\"n\":4}\n");
  /* A final entry's accepted cursor contains zero remaining budget, even if
   * the caller loses the successful return/checkpoint that followed it. */
  opts.cursor = NULL;
  opts.on_metadata = metadata;
  m.fail_at = m.append_peer = m.entries = 0;
  check(f, read_into_memory(f, &opts));
  strcpy(cursor, m.cursor);
  opts.cursor = cursor;
  opts.on_metadata = NULL;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "");
  assert_true(f->read.complete && f->read.completed_records == 0 &&
              f->read.last_examined_position == 2);
}

static void
test_selection_first_n_cancellation_preserves_unconsumed_budget(void **state) {
  fixture *f = *state;
  lc_collection_read_opts opts = {0};
  cancel_sink sink = {0};
  char cursor[LC_COLLECTION_CURSOR_BYTES];
  seed_three(f);
  check(f, lc_sink_to_memory(&f->sink, &f->error));
  sink.pub.write = cancel_write;
  sink.inner = f->sink;
  opts.has_first_n = 1;
  opts.first_n = 2;
  opts.cancelled = is_cancelled;
  opts.cancel_context = &sink;
  check(f, lc_collection_read(f->a, &opts, &sink.pub, &f->read, &f->error));
  assert_true(!f->read.complete && f->read.completed_records == 0);
  strcpy(cursor, f->read.resume_cursor);
  opts.cursor = cursor;
  opts.cancelled = NULL;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "{\"n\":1}\n{\"n\":2}\n");
  assert_true(f->read.complete && f->read.completed_records == 2 &&
              f->read.last_examined_position == 2);
}

static void test_selection_does_not_open_excluded_payloads(void **state) {
  fixture *f = *state;
  lc_collection_read_opts opts = {0};
  lc_collection_handle *handle;
  lc_pouch_collection_record record;
  lc_collection_order_row row;
  int found;
  seed_three(f);
  handle = f->a->impl;
  check(f,
        lc_collection_order_find(&handle->order, 2, &row, &found, &f->error));
  assert_true(found);
  check(f, lc_pouch_collection_record_read(handle->active_fd, row.record_offset,
                                           handle->order.replay_offset,
                                           "seg-00000000000000000001.log",
                                           &record, &f->error));
  assert_int_equal(
      pwrite(handle->active_fd, "[", 1, (off_t)record.payload_offset), 1);
  opts.has_first_n = 1;
  opts.first_n = 1;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "{\"n\":1}\n");
  opts.selector_lql = "eq{field=/n,value=3}";
  check(f, read_into_memory(f, &opts));
  assert_output(f, "");
  assert_true(f->read.complete && f->read.last_examined_position == 1);
  opts.selector_lql = NULL;
  opts.has_first_n = 0;
  opts.has_after = 1;
  opts.after = 2;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "{\"n\":3}\n");
  opts.has_after = 0;
  opts.has_through = 1;
  opts.through = 1;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "{\"n\":1}\n");
  opts.has_through = 0;
  assert_int_equal(read_into_memory(f, &opts), LC_ERR_PROTOCOL);
}

static void test_selection_index_scan_parity_across_pages(void **state) {
  fixture *f = *state;
  lc_collection_read_opts opts = {0};
  char expected[1024], cursor[LC_COLLECTION_CURSOR_BYTES];
  indexed_seed(f, LC_COLLECTION_ORDER_PAGE_ROWS * 2 + 5);
  check(f, build_query(f, 0));
  check(f, build_query(f, LC_COLLECTION_ORDER_PAGE_ROWS));
  opts.has_after = opts.has_through = opts.has_first_n = 1;
  opts.after = 62;
  opts.through = 68;
  opts.first_n = 3;
  opts.selector_lql = "eq{field=/kind,value=match}";
  for (int indexed = 1; indexed >= 0; --indexed) {
    indexed_mode(f, indexed, 0);
    check(f, read_into_memory(f, &opts));
    assert_true(f->read.complete && f->read.upper_position == 68 &&
                f->read.completed_records == 1 &&
                f->read.last_examined_position == 65);
    const void *bytes;
    size_t length;
    check(f, lc_sink_memory_bytes(f->sink, &bytes, &length, &f->error));
    assert_true(length < sizeof(expected));
    if (indexed) {
      memcpy(expected, bytes, length);
      expected[length] = '\0';
    } else
      assert_output(f, expected);
    strcpy(cursor, f->read.checkpoint_cursor);
    opts.cursor = cursor;
    check(f, read_into_memory(f, &opts));
    assert_true(f->read.completed_records == 1 &&
                f->read.last_examined_position == 68);
    opts.cursor = NULL;
  }
  opts.selector_lql = "eq{field=/kind,value=missing}";
  check(f, read_into_memory(f, &opts));
  assert_output(f, "");
  assert_true(f->read.complete && f->read.last_examined_position == 65);
}

static void
test_selection_survives_transforms_compaction_and_restart(void **state) {
  fixture *f = *state;
  char *key = NULL;
  check(f, lc_pouch_crypto_generate_key_string(&key, &f->error));
  for (int mode = 0; mode < 8; ++mode) {
    lc_collection_read_opts opts = {0};
    lc_collection_delete_res deleted;
    lc_pouch_maintenance_result maintained = {0};
    char cursor[LC_COLLECTION_CURSOR_BYTES];
    close_handles(f);
    snprintf(f->endpoint, sizeof(f->endpoint), "pouch://%s/bounded-%d", f->root,
             mode);
    open_client(f, mode & 1 ? key : NULL, mode & 2, mode & 4);
    seed_three(f);
    opts.has_after = opts.has_through = opts.has_first_n = 1;
    opts.after = 1;
    opts.through = 3;
    opts.first_n = 1;
    check(f, read_into_memory(f, &opts));
    assert_output(f, "{\"n\":2}\n");
    strcpy(cursor, f->read.checkpoint_cursor);
    check(f, lc_collection_delete_entry(f->a, 1, &deleted, &f->error));
    check(f, maintain(f, 1, 0, &maintained));
    lc_pouch_maintenance_result_cleanup(NULL, &maintained);
    close_handles(f);
    open_client(f, mode & 1 ? key : NULL, mode & 2, mode & 4);
    bind_collection(f, "events", 0, &f->a);
    opts.cursor = cursor;
    check(f, read_into_memory(f, &opts));
    assert_output(f, "{\"n\":3}\n");
    assert_true(f->read.complete && f->read.completed_records == 1 &&
                f->read.last_examined_position == 3);
  }
  lc_pouch_crypto_key_string_free(key);
}

typedef struct bounded_lf_sink {
  lc_sink pub;
  lc_sink *inner;
  unsigned int lines;
} bounded_lf_sink;

static int reject_second_lf(lc_sink *self, const void *bytes, size_t count,
                            lc_error *error) {
  bounded_lf_sink *sink = (bounded_lf_sink *)self;
  if (count == 1 && *(const char *)bytes == '\n' && ++sink->lines == 2) {
    lc_error_set(error, LC_ERR_TIMEOUT, 0, "injected LF rejection", NULL, NULL,
                 NULL);
    return 0;
  }
  return sink->inner->write(sink->inner, bytes, count, error);
}

static void
test_selection_first_n_lf_failure_preserves_entry_budget(void **state) {
  fixture *f = *state;
  lc_collection_read_opts opts = {0};
  bounded_lf_sink sink = {0};
  char cursor[LC_COLLECTION_CURSOR_BYTES];
  seed_three(f);
  check(f, lc_sink_to_memory(&f->sink, &f->error));
  sink.pub.write = reject_second_lf;
  sink.inner = f->sink;
  opts.has_first_n = 1;
  opts.first_n = 2;
  assert_int_equal(
      lc_collection_read(f->a, &opts, &sink.pub, &f->read, &f->error),
      LC_ERR_TIMEOUT);
  assert_output(f, "{\"n\":1}\n{\"n\":2}");
  assert_true(!f->read.complete && f->read.completed_records == 1 &&
              f->read.last_examined_position == 1);
  strcpy(cursor, f->read.resume_cursor);
  opts.cursor = cursor;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "{\"n\":2}\n");
  assert_true(f->read.complete && f->read.completed_records == 1 &&
              f->read.last_examined_position == 2);
}

static int change_next_window(void *context,
                              const lc_collection_read_metadata *entry,
                              lc_error *error) {
  lc_collection_read_opts *next = context;
  (void)error;
  if (entry->kind == LC_COLLECTION_READ_METADATA_ENTRY) {
    next->first_n = 100;
    next->has_through = 1;
    next->through = 1;
  }
  return LC_OK;
}

static void
test_selection_captures_scalar_options_before_callbacks(void **state) {
  fixture *f = *state;
  lc_collection_read_opts opts = {0};
  seed_three(f);
  opts.has_first_n = 1;
  opts.first_n = 2;
  opts.on_metadata = change_next_window;
  opts.metadata_context = &opts;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "{\"n\":1}\n{\"n\":2}\n");
  assert_true(f->read.complete && f->read.completed_records == 2 &&
              f->read.upper_position == 3 &&
              f->read.last_examined_position == 2);
}

static void test_selection_filters_after_input_count(void **state) {
  fixture *f = *state;
  lc_collection_read_opts opts = {0};
  lc_collection_delete_res deleted;
  char cursor[LC_COLLECTION_CURSOR_BYTES];
  seed_three(f);
  opts.has_first_n = 1;
  opts.first_n = 2;
  opts.selector_lql = "range{field=/n,gte=2}";
  check(f, read_into_memory(f, &opts));
  assert_output(f, "{\"n\":2}\n");
  assert_true(f->read.complete && f->read.completed_records == 1 &&
              f->read.last_examined_position == 2);
  strcpy(cursor, f->read.checkpoint_cursor);
  opts.cursor = cursor;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "{\"n\":3}\n");
  opts.cursor = NULL;
  opts.selector_lql = "eq{field=/n,value=3}";
  check(f, read_into_memory(f, &opts));
  assert_output(f, "");
  assert_true(f->read.complete && f->read.completed_records == 0 &&
              f->read.last_examined_position == 2);
  strcpy(cursor, f->read.checkpoint_cursor);
  opts.cursor = cursor;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "{\"n\":3}\n");
  opts.cursor = NULL;
  check(f, lc_collection_delete_entry(f->a, 2, &deleted, &f->error));
  check(f, read_into_memory(f, &opts));
  assert_output(f, "{\"n\":3}\n");
  assert_int_equal(f->read.last_examined_position, 3);
}

static int reject_second_input_progress(void *context,
                                        const lc_collection_read_metadata *item,
                                        lc_error *error) {
  if (item->kind == LC_COLLECTION_READ_METADATA_PROGRESS &&
      item->last_examined_position == 2) {
    if (context == NULL)
      return lc_error_set(error, LC_ERR_TIMEOUT, 0,
                          "injected filtered boundary rejection", NULL, NULL,
                          NULL);
    strcpy((char *)context, item->resume_cursor);
  }
  return LC_OK;
}

static void test_selection_filtered_progress_rejection_preserves_input_budget(
    void **state) {
  fixture *f = *state;
  lc_collection_read_opts opts = {0};
  char cursor[LC_COLLECTION_CURSOR_BYTES];
  char accepted[LC_COLLECTION_CURSOR_BYTES] = {0};
  seed_three(f);
  opts.has_first_n = 1;
  opts.first_n = 2;
  opts.selector_lql = "range{field=/n,lt=2}";
  opts.on_metadata = reject_second_input_progress;
  assert_int_equal(read_into_memory(f, &opts), LC_ERR_TIMEOUT);
  assert_output(f, "{\"n\":1}\n");
  assert_true(!f->read.complete && f->read.completed_records == 1 &&
              f->read.last_examined_position == 1);
  strcpy(cursor, f->read.resume_cursor);
  opts.cursor = cursor;
  opts.metadata_context = accepted;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "");
  assert_true(f->read.complete && f->read.completed_records == 0 &&
              f->read.last_examined_position == 2);
  assert_true(accepted[0] != '\0');
  strcpy(cursor, accepted);
  opts.on_metadata = NULL;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "");
  assert_true(f->read.complete && f->read.completed_records == 0 &&
              f->read.last_examined_position == 2);
}

static void
test_selection_coalesced_nonmatches_preserve_input_budget(void **state) {
  fixture *f = *state;
  const uint64_t total = LC_COLLECTION_ORDER_PAGE_ROWS * 4 + 7;
  lc_collection_read_opts opts = {0};
  progress_context progress = {0};
  char cursor[LC_COLLECTION_CURSOR_BYTES];
  seed_many(f, total);
  bind_collection(f, "events", 0, &f->b);
  progress.f = f;
  progress.append_and_cancel = 1;
  opts.has_first_n = 1;
  opts.first_n = LC_COLLECTION_ORDER_PAGE_ROWS * 4 + 1;
  opts.selector_lql = "eq{field=/n,value=3}";
  opts.on_metadata = progress_metadata;
  opts.metadata_context = &progress;
  opts.cancelled = progress_cancelled;
  opts.cancel_context = &progress;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "");
  assert_int_equal(progress.calls, 1);
  assert_int_equal(progress.appended_position, total + 1);
  assert_true(!f->read.complete && f->read.completed_records == 0 &&
              f->read.last_examined_position ==
                  LC_COLLECTION_ORDER_PAGE_ROWS * 4);
  strcpy(cursor, f->read.resume_cursor);
  opts.cursor = cursor;
  opts.on_metadata = NULL;
  opts.cancelled = NULL;
  check(f, read_into_memory(f, &opts));
  assert_output(f, "");
  assert_true(f->read.complete && f->read.completed_records == 0 &&
              f->read.last_examined_position == opts.first_n &&
              f->read.upper_position == total);
}

int main(int argc, char **argv) {
  const struct CMUnitTest tests[] = {
      cmocka_unit_test_setup_teardown(
          test_selection_coalesced_nonmatches_preserve_input_budget, setup,
          teardown),
      cmocka_unit_test_setup_teardown(test_selection_filters_after_input_count,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_selection_filtered_progress_rejection_preserves_input_budget,
          setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_selection_first_n_lf_failure_preserves_entry_budget, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_selection_captures_scalar_options_before_callbacks, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_selection_ranges_empty_gaps_and_uint64, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_selection_first_n_complete_windows_and_checkpoints, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_selection_first_n_resume_budget_binding_and_accepted_cursor,
          setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_selection_first_n_cancellation_preserves_unconsumed_budget,
          setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_selection_does_not_open_excluded_payloads, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_selection_index_scan_parity_across_pages, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_selection_survives_transforms_compaction_and_restart, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_query_cleanup_retires_epochs_and_abandoned_temporaries, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_query_cleanup_preserves_active_publication, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_late_query_publication_cannot_revive_a_retired_epoch, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_late_partial_query_page_cannot_replace_a_complete_page, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_query_index_does_not_retry_valid_out_of_range_numbers, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_query_pruning_does_not_read_retained_payloads, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_query_pruning_preserves_an_active_visibility_view, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_retention_discovery_skips_unconfigured_journals, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_retention_updates_and_janitor_preserve_active_read_view, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_metadata_racing_compaction_defers_and_converges, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_expiry_rechecks_renewed_and_cleared_metadata, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_retention_survives_rebuild_and_compaction_in_storage_modes,
          setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_retention_metadata_defaults_preservation_and_clearing, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_collection_expiry_drops_empty_parent_and_all_children, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_entry_expiry_preserves_gaps_retry_identity_and_empty_parent,
          setup, teardown),
      cmocka_unit_test_setup_teardown(test_append_retry_delete_and_reopen,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_query_artifacts_roundtrip_compaction_and_corruption, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_query_build_consumes_large_states_incrementally, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_failed_upload_and_source_callback_reentry, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_separate_metadata_resume_checkpoint_and_bounded_eod, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_cancellation_retains_initial_cursor_before_lf, setup, teardown),
      cmocka_unit_test_setup_teardown(test_lql_filters_and_cursor_binding,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_indexed_filters_agree_with_scan_and_cover_unindexed_tail, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_indexed_negative_does_not_read_its_payload, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_indexed_resume_retains_horizon_and_accepted_nonmatches, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_index_build_failure_preserves_committed_append_and_maintenance_repairs,
          setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_deleted_scan_cancels_before_eod_and_resumes_original_bound,
          setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_filter_progress_is_coalesced_and_rejection_is_retryable, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_catalog_incarnations_names_and_stale_cursor, setup, teardown),
      cmocka_unit_test_setup_teardown(test_deferred_close_from_metadata, setup,
                                      teardown),
      cmocka_unit_test_setup_teardown(test_transforms_rotation_and_reopen,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_crash_nosync_rebuild_discards_uncommitted_projection, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_projection_repair_and_body_corruption, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_concurrent_handles_share_append_authority, setup, teardown),
      cmocka_unit_test_setup_teardown(test_pending_and_torn_active_tail_repair,
                                      setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_peer_client_refresh_and_optional_error_output, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_remote_unsupported_without_network_requests, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_compaction_does_not_restore_a_retired_query_identity, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_compaction_merges_tail_and_preserves_retry_bindings, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_compaction_of_all_deleted_entries_preserves_highwater, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_compaction_pins_old_readers_without_pinning_new_readers, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_compaction_recovers_partial_projection_publication, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_crash_compaction_recovers_at_every_publication_boundary, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_collection_drop_and_orphan_cleanup_do_not_touch_replacements,
          setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_compaction_grace_does_not_restart_on_later_compactions, setup,
          teardown),
      cmocka_unit_test_setup_teardown(
          test_compaction_policy_and_existing_background_pass, setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_collection_discovery_is_durable_fair_and_retires_empty_namespaces,
          setup, teardown),
      cmocka_unit_test_setup_teardown(
          test_source_callback_catalog_contention_preserves_lock_order, setup,
          teardown),
  };
  if (lc_allocator_tracking_configure(1, NULL) != LC_OK)
    return 1;
  return lc_test_run_cases(tests, sizeof(tests) / sizeof(tests[0]), argc, argv,
                           NULL, NULL);
}

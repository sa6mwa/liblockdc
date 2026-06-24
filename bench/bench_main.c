#include "lc/lc.h"
#include "lc_mutate_stream.h"
#include "lc_pouch_store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

typedef struct bench_case {
  const char *name;
  long default_iterations;
  int (*run)(long iterations);
} bench_case;

typedef struct bench_alloc_header {
  size_t size;
} bench_alloc_header;

typedef struct bench_alloc_metrics {
  unsigned long malloc_calls;
  unsigned long calloc_calls;
  unsigned long realloc_calls;
  unsigned long free_calls;
  size_t outstanding_bytes;
  size_t peak_outstanding_bytes;
} bench_alloc_metrics;

static bench_alloc_metrics g_bench_alloc_metrics;

static double bench_now_seconds(void) {
  struct timespec ts;

  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
    return 0.0;
  }
  return (double)ts.tv_sec + ((double)ts.tv_nsec / 1000000000.0);
}

static void bench_alloc_metrics_reset(void) {
  memset(&g_bench_alloc_metrics, 0, sizeof(g_bench_alloc_metrics));
}

static void bench_alloc_note_alloc(size_t size) {
  g_bench_alloc_metrics.outstanding_bytes += size;
  if (g_bench_alloc_metrics.outstanding_bytes >
      g_bench_alloc_metrics.peak_outstanding_bytes) {
    g_bench_alloc_metrics.peak_outstanding_bytes =
        g_bench_alloc_metrics.outstanding_bytes;
  }
}

static void bench_alloc_note_free(size_t size) {
  if (g_bench_alloc_metrics.outstanding_bytes >= size) {
    g_bench_alloc_metrics.outstanding_bytes -= size;
  } else {
    g_bench_alloc_metrics.outstanding_bytes = 0U;
  }
}

static void *bench_alloc_malloc(void *context, size_t size) {
  bench_alloc_header *header;

  (void)context;
  header = (bench_alloc_header *)malloc(sizeof(*header) + size);
  if (header == NULL) {
    return NULL;
  }
  header->size = size;
  g_bench_alloc_metrics.malloc_calls++;
  bench_alloc_note_alloc(size);
  return (void *)(header + 1);
}

static void *bench_alloc_calloc(void *context, size_t count, size_t size) {
  bench_alloc_header *header;
  size_t bytes;

  (void)context;
  if (count != 0U && size > ((size_t)-1) / count) {
    return NULL;
  }
  bytes = count * size;
  header = (bench_alloc_header *)calloc(1U, sizeof(*header) + bytes);
  if (header == NULL) {
    return NULL;
  }
  header->size = bytes;
  g_bench_alloc_metrics.calloc_calls++;
  bench_alloc_note_alloc(bytes);
  return (void *)(header + 1);
}

static void *bench_alloc_realloc(void *context, void *ptr, size_t size) {
  bench_alloc_header *old_header;
  bench_alloc_header *new_header;
  size_t old_size;

  (void)context;
  if (ptr == NULL) {
    g_bench_alloc_metrics.realloc_calls++;
    return bench_alloc_malloc(NULL, size);
  }
  old_header = ((bench_alloc_header *)ptr) - 1;
  old_size = old_header->size;
  new_header =
      (bench_alloc_header *)realloc(old_header, sizeof(*new_header) + size);
  if (new_header == NULL) {
    return NULL;
  }
  new_header->size = size;
  g_bench_alloc_metrics.realloc_calls++;
  bench_alloc_note_free(old_size);
  bench_alloc_note_alloc(size);
  return (void *)(new_header + 1);
}

static void bench_alloc_free(void *context, void *ptr) {
  bench_alloc_header *header;

  (void)context;
  if (ptr == NULL) {
    return;
  }
  header = ((bench_alloc_header *)ptr) - 1;
  g_bench_alloc_metrics.free_calls++;
  bench_alloc_note_free(header->size);
  free(header);
}

static void bench_pouch_allocator(lc_pouch_allocator *allocator) {
  memset(allocator, 0, sizeof(*allocator));
  allocator->malloc_fn = bench_alloc_malloc;
  allocator->calloc_fn = bench_alloc_calloc;
  allocator->realloc_fn = bench_alloc_realloc;
  allocator->free_fn = bench_alloc_free;
}

static int bench_mutate_open(void *context, const char *resolved_path,
                             lc_source **out, lc_error *error) {
  static const unsigned char binary_payload[] = {0x00, 0x01, 0x02, 0xff, 'a'};
  const void *bytes;
  size_t length;

  (void)context;
  bytes = (const void *)"stream-text";
  length = strlen((const char *)bytes);
  if (resolved_path != NULL && (strstr(resolved_path, ".bin") != NULL ||
                                strstr(resolved_path, "base64") != NULL)) {
    bytes = (const void *)binary_payload;
    length = sizeof(binary_payload);
  }
  return lc_source_from_memory(bytes, length, out, error);
}

static int bench_stream_copy(long iterations) {
  static const char payload[] =
      "{\"key\":\"orders/"
      "42\",\"state\":{\"items\":[1,2,3],\"owner\":\"bench\"}}";
  lc_error error;
  long i;

  lc_error_init(&error);
  for (i = 0; i < iterations; ++i) {
    lc_source *source;
    lc_sink *sink;
    size_t written;

    source = NULL;
    sink = NULL;
    written = 0U;
    if (lc_source_from_memory(payload, sizeof(payload) - 1U, &source, &error) !=
        LC_OK) {
      lc_error_cleanup(&error);
      return 1;
    }
    if (lc_sink_to_memory(&sink, &error) != LC_OK) {
      lc_source_close(source);
      lc_error_cleanup(&error);
      return 1;
    }
    if (lc_copy(source, sink, &written, &error) != LC_OK) {
      lc_sink_close(sink);
      lc_source_close(source);
      lc_error_cleanup(&error);
      return 1;
    }
    lc_sink_close(sink);
    lc_source_close(source);
  }
  lc_error_cleanup(&error);
  return 0;
}

static int bench_json_stream(long iterations) {
  static const char json_payload[] =
      "{\"items\":[{\"id\":1},{\"id\":2},{\"id\":3}],\"owner\":\"bench\"}";
  unsigned char scratch[257];
  lc_error error;
  long i;

  lc_error_init(&error);
  for (i = 0; i < iterations; ++i) {
    lc_source *source;
    size_t got;

    source = NULL;
    if (lc_source_from_memory(json_payload, sizeof(json_payload) - 1U, &source,
                              &error) != LC_OK) {
      lc_error_cleanup(&error);
      return 1;
    }
    got = source->read(source, scratch, sizeof(scratch) - 1U, &error);
    if (got >= sizeof(scratch)) {
      got = sizeof(scratch) - 1U;
    }
    scratch[got] = '\0';
    if (source->reset(source, &error) != LC_OK) {
      lc_source_close(source);
      lc_error_cleanup(&error);
      return 1;
    }
    (void)source->read(source, scratch, 64U, &error);
    lc_source_close(source);
  }
  lc_error_cleanup(&error);
  return 0;
}

static int bench_mutate_parse(long iterations) {
  static const char *exprs[] = {"/name=bench", "/counter=3",
                                "textfile:/nested/value=payload.txt",
                                "base64file:/blob=blob.bin"};
  lc_mutation_parse_options options;
  lc_file_value_resolver resolver;
  lc_error error;
  long i;

  memset(&options, 0, sizeof(options));
  memset(&resolver, 0, sizeof(resolver));
  resolver.open = bench_mutate_open;
  options.file_value_base_dir = "/virtual";
  options.file_value_resolver = &resolver;
  options.now.tv_sec = 1700000000;
  options.now.tv_nsec = 123456789L;
  options.has_now = 1;
  lc_error_init(&error);

  for (i = 0; i < iterations; ++i) {
    lc_mutation_plan *plan;

    plan = NULL;
    if (lc_mutation_plan_build(exprs, sizeof(exprs) / sizeof(exprs[0]),
                               &options, &plan, &error) != LC_OK) {
      lc_error_cleanup(&error);
      return 1;
    }
    lc_mutation_plan_close(plan);
  }

  lc_error_cleanup(&error);
  return 0;
}

static int bench_mutate_apply(long iterations) {
  static const char *exprs[] = {"/name=bench", "/counter=3",
                                "textfile:/nested/value=payload.txt"};
  static const char input_json[] =
      "{\"counter\":1,\"nested\":{\"old\":\"value\"},\"name\":\"before\"}";
  lc_mutation_parse_options options;
  lc_file_value_resolver resolver;
  lc_mutation_plan *plan;
  lc_error error;
  long i;

  memset(&options, 0, sizeof(options));
  memset(&resolver, 0, sizeof(resolver));
  resolver.open = bench_mutate_open;
  options.file_value_base_dir = "/virtual";
  options.file_value_resolver = &resolver;
  options.now.tv_sec = 1700000000;
  options.now.tv_nsec = 123456789L;
  options.has_now = 1;
  plan = NULL;
  lc_error_init(&error);

  if (lc_mutation_plan_build(exprs, sizeof(exprs) / sizeof(exprs[0]), &options,
                             &plan, &error) != LC_OK) {
    lc_error_cleanup(&error);
    return 1;
  }

  for (i = 0; i < iterations; ++i) {
    FILE *input;
    FILE *output;

    input = tmpfile();
    output = NULL;
    if (input == NULL) {
      lc_mutation_plan_close(plan);
      lc_error_cleanup(&error);
      return 1;
    }
    if (fwrite(input_json, 1U, sizeof(input_json) - 1U, input) !=
        sizeof(input_json) - 1U) {
      fclose(input);
      lc_mutation_plan_close(plan);
      lc_error_cleanup(&error);
      return 1;
    }
    fflush(input);
    rewind(input);
    if (lc_mutation_plan_apply(plan, input, &output, &error) != LC_OK) {
      if (output != NULL) {
        fclose(output);
      }
      fclose(input);
      lc_mutation_plan_close(plan);
      lc_error_cleanup(&error);
      return 1;
    }
    if (output != NULL) {
      fclose(output);
    }
    fclose(input);
  }

  lc_mutation_plan_close(plan);
  lc_error_cleanup(&error);
  return 0;
}

static void bench_pouch_root_path(char *buffer, size_t buffer_size,
                                  const char *suffix) {
  snprintf(buffer, buffer_size, "/tmp/liblockdc-pouch-bench-%ld-%s",
           (long)getpid(), suffix);
}

static void bench_pouch_cleanup_root(const char *root) {
  char path[512];

  snprintf(path, sizeof(path), "%s/store.compact.tmp", root);
  unlink(path);
  snprintf(path, sizeof(path), "%s/query.index.compact.tmp", root);
  unlink(path);
  snprintf(path, sizeof(path), "%s/store.log", root);
  unlink(path);
  snprintf(path, sizeof(path), "%s/query.index", root);
  unlink(path);
  snprintf(path, sizeof(path), "%s/writer.lock", root);
  unlink(path);
  snprintf(path, sizeof(path), "%s/locks/bench/hot-key", root);
  unlink(path);
  snprintf(path, sizeof(path), "%s/locks/bench", root);
  rmdir(path);
  snprintf(path, sizeof(path), "%s/locks", root);
  rmdir(path);
  rmdir(root);
}

static lc_source *bench_source_from_text(const char *text, lc_error *error) {
  lc_source *source;

  source = NULL;
  if (lc_source_from_memory(text, strlen(text), &source, error) != LC_OK) {
    return NULL;
  }
  return source;
}

static lc_source *bench_source_from_bytes(const void *bytes, size_t length,
                                          lc_error *error) {
  lc_source *source;

  source = NULL;
  if (lc_source_from_memory(bytes, length, &source, error) != LC_OK) {
    return NULL;
  }
  return source;
}

static int bench_pouch_read_and_check(lc_source *source, const char *expected,
                                      lc_error *error) {
  char buffer[128];
  size_t got;

  got = source->read(source, buffer, sizeof(buffer) - 1U, error);
  if (error->code != LC_OK) {
    return 1;
  }
  if (got >= sizeof(buffer)) {
    got = sizeof(buffer) - 1U;
  }
  buffer[got] = '\0';
  return strcmp(buffer, expected) == 0 ? 0 : 1;
}

static void bench_fill_payload(unsigned char *payload, size_t length) {
  size_t index;

  for (index = 0U; index < length; ++index) {
    payload[index] = (unsigned char)('a' + (index % 26U));
  }
}

static int bench_pouch_drain_and_count(lc_source *source, size_t expected,
                                       lc_error *error) {
  unsigned char buffer[8192];
  size_t total;
  size_t got;

  total = 0U;
  do {
    got = source->read(source, buffer, sizeof(buffer), error);
    if (error->code != LC_OK) {
      return 1;
    }
    total += got;
  } while (got != 0U);

  return total == expected ? 0 : 1;
}

static int bench_pouch_store_row(lc_pouch_store *store,
                                 const lc_pouch_allocator *allocator,
                                 const char *key, const char *json,
                                 lc_error *error) {
  lc_source *source;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res put_res;
  lc_pouch_meta meta;
  lc_pouch_store_meta_res meta_res;
  int rc;

  memset(&opts, 0, sizeof(opts));
  memset(&put_res, 0, sizeof(put_res));
  memset(&meta, 0, sizeof(meta));
  memset(&meta_res, 0, sizeof(meta_res));
  opts.content_type = "application/json";
  source = bench_source_from_text(json, error);
  if (source == NULL) {
    return 1;
  }
  rc = store->write_state(store, "bench", key, source, &opts, &put_res, error);
  lc_source_close(source);
  if (rc != LC_OK) {
    lc_pouch_put_state_res_cleanup(allocator, &put_res);
    return 1;
  }
  meta.owner = "bench-owner";
  meta.lease_id = "bench-lease";
  meta.txn_id = "bench-txn";
  meta.state_etag = put_res.new_state_etag;
  meta.version = put_res.new_version;
  meta.lease_expires_at_unix = 3600L;
  meta.fencing_token = put_res.new_version;
  rc = store->store_meta(store, "bench", key, &meta, NULL, &meta_res, error);
  lc_pouch_store_meta_res_cleanup(allocator, &meta_res);
  lc_pouch_put_state_res_cleanup(allocator, &put_res);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_seed_query_rows(const char *root, long rows,
                                       lc_error *error) {
  lc_pouch_store *store;
  char key[96];
  char json[96];
  long i;
  int rc;

  store = NULL;
  rc = lc_pouch_disk_open(root, NULL, &store, error);
  if (rc != LC_OK) {
    return 1;
  }
  for (i = 0; i < rows; ++i) {
    snprintf(key, sizeof(key), "bench/query/%08ld", i);
    snprintf(json, sizeof(json), "{\"value\":%ld}", i);
    if (bench_pouch_store_row(store, NULL, key, json, error) != 0) {
      store->close(store, error);
      return 1;
    }
  }
  rc = store->close(store, error);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_seed_query_rows_with_allocator(
    const char *root, long rows, const lc_pouch_allocator *allocator,
    lc_error *error) {
  lc_pouch_store *store;
  char key[96];
  char json[96];
  long i;
  int rc;

  store = NULL;
  rc = lc_pouch_disk_open(root, allocator, &store, error);
  if (rc != LC_OK) {
    return 1;
  }
  for (i = 0; i < rows; ++i) {
    snprintf(key, sizeof(key), "bench/query/%08ld", i);
    snprintf(json, sizeof(json), "{\"value\":%ld}", i);
    if (bench_pouch_store_row(store, allocator, key, json, error) != 0) {
      store->close(store, error);
      return 1;
    }
  }
  rc = store->close(store, error);
  return rc == LC_OK ? 0 : 1;
}

typedef struct bench_scan_count {
  long rows;
} bench_scan_count;

typedef struct bench_query_key_count {
  long rows;
  size_t bytes;
} bench_query_key_count;

static int bench_scan_count_visit(void *context,
                                  const lc_pouch_scan_meta_row *row,
                                  lc_error *error) {
  bench_scan_count *count;

  (void)row;
  (void)error;
  count = (bench_scan_count *)context;
  count->rows += 1L;
  return LC_OK;
}

static int bench_query_key_begin(void *context, lc_error *error) {
  (void)context;
  (void)error;
  return 1;
}

static int bench_query_key_chunk(void *context, const char *bytes, size_t len,
                                 lc_error *error) {
  bench_query_key_count *count;

  (void)bytes;
  (void)error;
  count = (bench_query_key_count *)context;
  count->bytes += len;
  return 1;
}

static int bench_query_key_end(void *context, lc_error *error) {
  bench_query_key_count *count;

  (void)error;
  count = (bench_query_key_count *)context;
  count->rows += 1L;
  return 1;
}

static int bench_key_count_visit(void *context, const char *key,
                                 lc_error *error) {
  bench_scan_count *count;

  (void)key;
  (void)error;
  count = (bench_scan_count *)context;
  count->rows += 1L;
  return LC_OK;
}

static int bench_pouch_state_roundtrip(long iterations) {
  char root[256];
  char key[80];
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res put_res;
  lc_pouch_state_info info;
  lc_error error;
  long i;
  int rc;

  bench_pouch_root_path(root, sizeof(root), "state");
  bench_pouch_cleanup_root(root);
  lc_error_init(&error);
  store = NULL;
  rc = lc_pouch_disk_open(root, NULL, &store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    return 1;
  }
  memset(&opts, 0, sizeof(opts));
  opts.content_type = "application/json";

  for (i = 0; i < iterations; ++i) {
    snprintf(key, sizeof(key), "bench/state/%ld", i);
    memset(&put_res, 0, sizeof(put_res));
    memset(&info, 0, sizeof(info));
    read_body = NULL;
    source = bench_source_from_text("{\"value\":1}", &error);
    if (source == NULL) {
      store->close(store, &error);
      lc_error_cleanup(&error);
      bench_pouch_cleanup_root(root);
      return 1;
    }
    rc = store->write_state(store, "bench", key, source, &opts, &put_res,
                            &error);
    lc_source_close(source);
    if (rc != LC_OK) {
      store->close(store, &error);
      lc_error_cleanup(&error);
      bench_pouch_cleanup_root(root);
      return 1;
    }
    rc = store->read_state(store, "bench", key, &read_body, &info, &error);
    if (rc != LC_OK || info.no_content || read_body == NULL ||
        bench_pouch_read_and_check(read_body, "{\"value\":1}", &error) != 0) {
      if (read_body != NULL) {
        lc_source_close(read_body);
      }
      lc_pouch_state_info_cleanup(NULL, &info);
      lc_pouch_put_state_res_cleanup(NULL, &put_res);
      store->close(store, &error);
      lc_error_cleanup(&error);
      bench_pouch_cleanup_root(root);
      return 1;
    }
    lc_source_close(read_body);
    lc_pouch_state_info_cleanup(NULL, &info);
    lc_pouch_put_state_res_cleanup(NULL, &put_res);
  }

  rc = store->close(store, &error);
  lc_error_cleanup(&error);
  bench_pouch_cleanup_root(root);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_state_write_payload(long iterations, size_t payload_size,
                                           const char *suffix) {
  char root[256];
  char key[80];
  unsigned char *payload;
  lc_pouch_allocator allocator;
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res put_res;
  lc_error error;
  long i;
  int rc;

  bench_pouch_root_path(root, sizeof(root), suffix);
  bench_pouch_cleanup_root(root);
  lc_error_init(&error);
  bench_pouch_allocator(&allocator);
  payload = (unsigned char *)malloc(payload_size);
  if (payload == NULL) {
    lc_error_cleanup(&error);
    return 1;
  }
  bench_fill_payload(payload, payload_size);

  store = NULL;
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  if (rc != LC_OK) {
    free(payload);
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }
  memset(&opts, 0, sizeof(opts));
  opts.content_type = "application/octet-stream";

  for (i = 0; i < iterations; ++i) {
    snprintf(key, sizeof(key), "bench/payload/%ld", i);
    memset(&put_res, 0, sizeof(put_res));
    source = bench_source_from_bytes(payload, payload_size, &error);
    if (source == NULL) {
      store->close(store, &error);
      free(payload);
      lc_error_cleanup(&error);
      bench_pouch_cleanup_root(root);
      return 1;
    }
    rc = store->write_state(store, "bench", key, source, &opts, &put_res,
                            &error);
    lc_source_close(source);
    lc_pouch_put_state_res_cleanup(&allocator, &put_res);
    if (rc != LC_OK) {
      store->close(store, &error);
      free(payload);
      lc_error_cleanup(&error);
      bench_pouch_cleanup_root(root);
      return 1;
    }
  }

  rc = store->close(store, &error);
  free(payload);
  lc_error_cleanup(&error);
  bench_pouch_cleanup_root(root);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_state_read_payload(long iterations, size_t payload_size,
                                          const char *suffix) {
  char root[256];
  char key[80];
  unsigned char *payload;
  lc_pouch_allocator allocator;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res put_res;
  lc_pouch_state_info info;
  lc_error error;
  long i;
  int rc;

  bench_pouch_root_path(root, sizeof(root), suffix);
  bench_pouch_cleanup_root(root);
  lc_error_init(&error);
  bench_pouch_allocator(&allocator);
  payload = (unsigned char *)malloc(payload_size);
  if (payload == NULL) {
    lc_error_cleanup(&error);
    return 1;
  }
  bench_fill_payload(payload, payload_size);

  store = NULL;
  rc = lc_pouch_disk_open(root, NULL, &store, &error);
  if (rc != LC_OK) {
    free(payload);
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }
  memset(&opts, 0, sizeof(opts));
  opts.content_type = "application/octet-stream";

  for (i = 0; i < iterations; ++i) {
    snprintf(key, sizeof(key), "bench/payload/%ld", i);
    memset(&put_res, 0, sizeof(put_res));
    source = bench_source_from_bytes(payload, payload_size, &error);
    if (source == NULL) {
      store->close(store, &error);
      free(payload);
      lc_error_cleanup(&error);
      bench_pouch_cleanup_root(root);
      return 1;
    }
    rc = store->write_state(store, "bench", key, source, &opts, &put_res,
                            &error);
    lc_source_close(source);
    lc_pouch_put_state_res_cleanup(NULL, &put_res);
    if (rc != LC_OK) {
      store->close(store, &error);
      free(payload);
      lc_error_cleanup(&error);
      bench_pouch_cleanup_root(root);
      return 1;
    }
  }
  rc = store->close(store, &error);
  if (rc != LC_OK) {
    free(payload);
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }

  bench_alloc_metrics_reset();
  store = NULL;
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  if (rc != LC_OK) {
    free(payload);
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }
  for (i = 0; i < iterations; ++i) {
    snprintf(key, sizeof(key), "bench/payload/%ld", i);
    memset(&info, 0, sizeof(info));
    read_body = NULL;
    rc = store->read_state(store, "bench", key, &read_body, &info, &error);
    if (rc != LC_OK || info.no_content || read_body == NULL ||
        bench_pouch_drain_and_count(read_body, payload_size, &error) != 0) {
      if (read_body != NULL) {
        lc_source_close(read_body);
      }
      lc_pouch_state_info_cleanup(&allocator, &info);
      store->close(store, &error);
      free(payload);
      lc_error_cleanup(&error);
      bench_pouch_cleanup_root(root);
      return 1;
    }
    lc_source_close(read_body);
    lc_pouch_state_info_cleanup(&allocator, &info);
  }

  rc = store->close(store, &error);
  free(payload);
  lc_error_cleanup(&error);
  bench_pouch_cleanup_root(root);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_state_write_1k(long iterations) {
  return bench_pouch_state_write_payload(iterations, 1024U, "state-write-1k");
}

static int bench_pouch_state_write_64k(long iterations) {
  return bench_pouch_state_write_payload(iterations, 64U * 1024U,
                                         "state-write-64k");
}

static int bench_pouch_state_write_1m(long iterations) {
  return bench_pouch_state_write_payload(iterations, 1024U * 1024U,
                                         "state-write-1m");
}

static int bench_pouch_state_write_16m(long iterations) {
  return bench_pouch_state_write_payload(iterations, 16U * 1024U * 1024U,
                                         "state-write-16m");
}

static int bench_pouch_state_read_1k(long iterations) {
  return bench_pouch_state_read_payload(iterations, 1024U, "state-read-1k");
}

static int bench_pouch_state_read_64k(long iterations) {
  return bench_pouch_state_read_payload(iterations, 64U * 1024U,
                                        "state-read-64k");
}

static int bench_pouch_state_read_1m(long iterations) {
  return bench_pouch_state_read_payload(iterations, 1024U * 1024U,
                                        "state-read-1m");
}

static int bench_pouch_state_read_16m(long iterations) {
  return bench_pouch_state_read_payload(iterations, 16U * 1024U * 1024U,
                                        "state-read-16m");
}

static int bench_pouch_staged_promote(long iterations) {
  char root[256];
  char key[80];
  char txn_id[80];
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res staged;
  lc_pouch_put_state_res promoted;
  lc_error error;
  long i;
  int rc;

  bench_pouch_root_path(root, sizeof(root), "staged");
  bench_pouch_cleanup_root(root);
  lc_error_init(&error);
  store = NULL;
  rc = lc_pouch_disk_open(root, NULL, &store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    return 1;
  }
  memset(&opts, 0, sizeof(opts));
  opts.content_type = "application/json";

  for (i = 0; i < iterations; ++i) {
    snprintf(key, sizeof(key), "bench/staged/%ld", i);
    snprintf(txn_id, sizeof(txn_id), "txn-%ld", i);
    memset(&staged, 0, sizeof(staged));
    memset(&promoted, 0, sizeof(promoted));
    source = bench_source_from_text("{\"staged\":true}", &error);
    if (source == NULL) {
      store->close(store, &error);
      lc_error_cleanup(&error);
      bench_pouch_cleanup_root(root);
      return 1;
    }
    rc = store->stage_state(store, "bench", key, txn_id, source, &opts,
                            &staged, &error);
    lc_source_close(source);
    if (rc == LC_OK) {
      rc = store->promote_staged_state(store, "bench", key, txn_id, NULL,
                                       &promoted, &error);
    }
    lc_pouch_put_state_res_cleanup(NULL, &staged);
    lc_pouch_put_state_res_cleanup(NULL, &promoted);
    if (rc != LC_OK) {
      store->close(store, &error);
      lc_error_cleanup(&error);
      bench_pouch_cleanup_root(root);
      return 1;
    }
  }

  rc = store->close(store, &error);
  lc_error_cleanup(&error);
  bench_pouch_cleanup_root(root);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_public_mutate(long iterations) {
  char root[256];
  char endpoint[320];
  lc_client_config config;
  const char *endpoints[1];
  lc_client *client;
  lc_lease *lease;
  lc_source *source;
  lc_acquire_req acquire;
  lc_update_opts update_opts;
  lc_mutate_req mutate_req;
  const char *mutations[1];
  lc_error error;
  long expected_version;
  long i;
  int rc;

  bench_pouch_root_path(root, sizeof(root), "public-mutate");
  bench_pouch_cleanup_root(root);
  snprintf(endpoint, sizeof(endpoint), "pouch://%s", root);
  endpoints[0] = endpoint;
  lc_error_init(&error);
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "bench";
  client = NULL;
  lease = NULL;
  source = NULL;

  rc = lc_client_open(&config, &client, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }

  lc_acquire_req_init(&acquire);
  acquire.key = "bench/public-mutate";
  acquire.owner = "bench";
  acquire.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire, &lease, &error);
  if (rc != LC_OK) {
    client->close(client);
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }

  source = bench_source_from_text("{\"counter\":0,\"owner\":\"bench\"}",
                                  &error);
  if (source == NULL) {
    lease->close(lease);
    client->close(client);
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }
  lc_update_opts_init(&update_opts);
  update_opts.content_type = "application/json";
  rc = lease->update(lease, source, &update_opts, &error);
  lc_source_close(source);
  source = NULL;
  if (rc != LC_OK) {
    lease->close(lease);
    client->close(client);
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }

  mutations[0] = "/counter++";
  lc_mutate_req_init(&mutate_req);
  mutate_req.mutations = mutations;
  mutate_req.mutation_count = 1U;
  for (i = 0; i < iterations; ++i) {
    rc = lease->mutate(lease, &mutate_req, &error);
    if (rc != LC_OK) {
      lease->close(lease);
      client->close(client);
      lc_error_cleanup(&error);
      bench_pouch_cleanup_root(root);
      return 1;
    }
  }

  expected_version = iterations + 1L;
  if (lease->version != expected_version) {
    fprintf(stderr,
            "pouch-public-mutate final version was %ld, expected %ld\n",
            lease->version, expected_version);
    rc = LC_ERR_PROTOCOL;
  }

  lease->close(lease);
  client->close(client);
  lc_error_cleanup(&error);
  bench_pouch_cleanup_root(root);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_object_roundtrip(long iterations) {
  char root[256];
  char key[80];
  char name[80];
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_put_object_opts opts;
  lc_pouch_object_selector selector;
  lc_pouch_object_info info;
  lc_error error;
  long i;
  int rc;

  bench_pouch_root_path(root, sizeof(root), "object");
  bench_pouch_cleanup_root(root);
  lc_error_init(&error);
  store = NULL;
  rc = lc_pouch_disk_open(root, NULL, &store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    return 1;
  }

  for (i = 0; i < iterations; ++i) {
    snprintf(key, sizeof(key), "bench/object/%ld", i);
    snprintf(name, sizeof(name), "payload-%ld.txt", i);
    memset(&opts, 0, sizeof(opts));
    memset(&selector, 0, sizeof(selector));
    memset(&info, 0, sizeof(info));
    opts.name = name;
    opts.content_type = "text/plain";
    selector.name = name;
    read_body = NULL;
    source = bench_source_from_text("object-payload", &error);
    if (source == NULL) {
      store->close(store, &error);
      lc_error_cleanup(&error);
      bench_pouch_cleanup_root(root);
      return 1;
    }
    rc = store->put_object(store, "bench", key, source, &opts, &info, &error);
    lc_source_close(source);
    lc_pouch_object_info_cleanup(NULL, &info);
    if (rc == LC_OK) {
      rc = store->get_object(store, "bench", key, &selector, &read_body, &info,
                             &error);
    }
    if (rc != LC_OK || read_body == NULL ||
        bench_pouch_read_and_check(read_body, "object-payload", &error) != 0) {
      if (read_body != NULL) {
        lc_source_close(read_body);
      }
      lc_pouch_object_info_cleanup(NULL, &info);
      store->close(store, &error);
      lc_error_cleanup(&error);
      bench_pouch_cleanup_root(root);
      return 1;
    }
    lc_source_close(read_body);
    lc_pouch_object_info_cleanup(NULL, &info);
  }

  rc = store->close(store, &error);
  lc_error_cleanup(&error);
  bench_pouch_cleanup_root(root);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_queue_roundtrip(long iterations) {
  char root[256];
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_enqueue_opts enqueue_opts;
  lc_pouch_dequeue_opts dequeue_opts;
  lc_pouch_queue_message_info enqueued;
  lc_pouch_queue_message_info dequeued;
  lc_pouch_queue_ref ref;
  lc_error error;
  long i;
  int acked;
  int rc;

  bench_pouch_root_path(root, sizeof(root), "queue");
  bench_pouch_cleanup_root(root);
  lc_error_init(&error);
  store = NULL;
  rc = lc_pouch_disk_open(root, NULL, &store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    return 1;
  }
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  memset(&dequeue_opts, 0, sizeof(dequeue_opts));
  enqueue_opts.content_type = "text/plain";
  enqueue_opts.visibility_timeout_seconds = 30L;
  enqueue_opts.ttl_seconds = 3600L;
  enqueue_opts.max_attempts = 3;
  dequeue_opts.owner = "bench-worker";
  dequeue_opts.visibility_timeout_seconds = 30L;

  for (i = 0; i < iterations; ++i) {
    memset(&enqueued, 0, sizeof(enqueued));
    memset(&dequeued, 0, sizeof(dequeued));
    memset(&ref, 0, sizeof(ref));
    read_body = NULL;
    source = bench_source_from_text("queue-payload", &error);
    if (source == NULL) {
      store->close(store, &error);
      lc_error_cleanup(&error);
      bench_pouch_cleanup_root(root);
      return 1;
    }
    rc = store->enqueue_message(store, "bench", "jobs", source, &enqueue_opts,
                                &enqueued, &error);
    lc_source_close(source);
    if (rc == LC_OK) {
      rc = store->dequeue_message(store, "bench", "jobs", &dequeue_opts,
                                  &read_body, &dequeued, &error);
    }
    if (rc != LC_OK || read_body == NULL ||
        bench_pouch_read_and_check(read_body, "queue-payload", &error) != 0) {
      fprintf(stderr,
              "pouch-queue failed during dequeue/read at iteration %ld "
              "rc=%d error=%d message=%s\n",
              i, rc, error.code,
              error.message != NULL ? error.message : "(none)");
      if (read_body != NULL) {
        lc_source_close(read_body);
      }
      lc_pouch_queue_message_info_cleanup(NULL, &enqueued);
      lc_pouch_queue_message_info_cleanup(NULL, &dequeued);
      store->close(store, &error);
      lc_error_cleanup(&error);
      bench_pouch_cleanup_root(root);
      return 1;
    }
    lc_source_close(read_body);
    ref.namespace_name = dequeued.namespace_name;
    ref.queue = dequeued.queue;
    ref.message_id = dequeued.message_id;
    ref.lease_id = dequeued.lease_id;
    ref.txn_id = dequeued.txn_id;
    ref.fencing_token = dequeued.fencing_token;
    ref.meta_etag = dequeued.meta_etag;
    acked = 0;
    rc = store->ack_message(store, &ref, &acked, &error);
    lc_pouch_queue_message_info_cleanup(NULL, &enqueued);
    lc_pouch_queue_message_info_cleanup(NULL, &dequeued);
    if (rc != LC_OK || !acked) {
      fprintf(stderr,
              "pouch-queue failed during ack at iteration %ld rc=%d acked=%d "
              "error=%d message=%s\n",
              i, rc, acked, error.code,
              error.message != NULL ? error.message : "(none)");
      store->close(store, &error);
      lc_error_cleanup(&error);
      bench_pouch_cleanup_root(root);
      return 1;
    }
  }

  rc = store->close(store, &error);
  lc_error_cleanup(&error);
  bench_pouch_cleanup_root(root);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_queue_txn_rollback(long iterations) {
  char root[256];
  char txn_id[80];
  lc_pouch_allocator allocator;
  lc_pouch_store *store;
  lc_source *source;
  lc_source *read_body;
  lc_pouch_enqueue_opts enqueue_opts;
  lc_pouch_dequeue_opts dequeue_opts;
  lc_pouch_queue_message_info enqueued;
  lc_pouch_queue_message_info dequeued;
  lc_pouch_queue_message_info redelivered;
  lc_pouch_queue_ref ref;
  lc_error error;
  long i;
  int acked;
  int rc;

  bench_pouch_root_path(root, sizeof(root), "queue-txn-rollback");
  bench_pouch_cleanup_root(root);
  lc_error_init(&error);
  bench_pouch_allocator(&allocator);
  store = NULL;
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    return 1;
  }
  memset(&enqueue_opts, 0, sizeof(enqueue_opts));
  enqueue_opts.content_type = "text/plain";
  enqueue_opts.visibility_timeout_seconds = 30L;
  enqueue_opts.ttl_seconds = 3600L;
  enqueue_opts.max_attempts = 3;

  for (i = 0; i < iterations; ++i) {
    memset(&dequeue_opts, 0, sizeof(dequeue_opts));
    memset(&enqueued, 0, sizeof(enqueued));
    memset(&dequeued, 0, sizeof(dequeued));
    memset(&redelivered, 0, sizeof(redelivered));
    memset(&ref, 0, sizeof(ref));
    read_body = NULL;
    snprintf(txn_id, sizeof(txn_id), "bench-queue-txn-%ld", i);

    source = bench_source_from_text("queue-transaction-payload", &error);
    if (source == NULL) {
      store->close(store, &error);
      lc_error_cleanup(&error);
      bench_pouch_cleanup_root(root);
      return 1;
    }
    rc = store->enqueue_message(store, "bench", "txn-jobs", source,
                                &enqueue_opts, &enqueued, &error);
    lc_source_close(source);

    dequeue_opts.owner = "bench-worker";
    dequeue_opts.txn_id = txn_id;
    dequeue_opts.visibility_timeout_seconds = 30L;
    if (rc == LC_OK) {
      rc = store->dequeue_message(store, "bench", "txn-jobs", &dequeue_opts,
                                  &read_body, &dequeued, &error);
    }
    if (rc != LC_OK || read_body == NULL ||
        bench_pouch_read_and_check(read_body, "queue-transaction-payload",
                                   &error) != 0) {
      fprintf(stderr,
              "pouch-queue-txn-rollback failed during transactional dequeue "
              "at iteration %ld rc=%d error=%d message=%s\n",
              i, rc, error.code,
              error.message != NULL ? error.message : "(none)");
      if (read_body != NULL) {
        lc_source_close(read_body);
      }
      lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);
      lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);
      store->close(store, &error);
      lc_error_cleanup(&error);
      bench_pouch_cleanup_root(root);
      return 1;
    }
    lc_source_close(read_body);
    read_body = NULL;

    ref.namespace_name = dequeued.namespace_name;
    ref.queue = dequeued.queue;
    ref.message_id = dequeued.message_id;
    ref.lease_id = dequeued.lease_id;
    ref.txn_id = dequeued.txn_id;
    ref.fencing_token = dequeued.fencing_token;
    ref.meta_etag = dequeued.meta_etag;
    acked = 0;
    rc = store->ack_message(store, &ref, &acked, &error);
    if (rc == LC_OK && !acked) {
      rc = LC_ERR_PROTOCOL;
    }
    if (rc == LC_OK) {
      rc = store->apply_queue_txn(store, txn_id, 0, &error);
    }

    memset(&dequeue_opts, 0, sizeof(dequeue_opts));
    dequeue_opts.owner = "bench-worker-redelivery";
    dequeue_opts.visibility_timeout_seconds = 30L;
    if (rc == LC_OK) {
      rc = store->dequeue_message(store, "bench", "txn-jobs", &dequeue_opts,
                                  &read_body, &redelivered, &error);
    }
    if (rc != LC_OK || read_body == NULL ||
        bench_pouch_read_and_check(read_body, "queue-transaction-payload",
                                   &error) != 0 ||
        redelivered.message_id == NULL ||
        strcmp(redelivered.message_id, enqueued.message_id) != 0) {
      fprintf(stderr,
              "pouch-queue-txn-rollback failed during redelivery at "
              "iteration %ld rc=%d error=%d message=%s\n",
              i, rc, error.code,
              error.message != NULL ? error.message : "(none)");
      if (read_body != NULL) {
        lc_source_close(read_body);
      }
      lc_pouch_queue_message_info_cleanup(&allocator, &redelivered);
      lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);
      lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);
      store->close(store, &error);
      lc_error_cleanup(&error);
      bench_pouch_cleanup_root(root);
      return 1;
    }
    lc_source_close(read_body);

    ref.namespace_name = redelivered.namespace_name;
    ref.queue = redelivered.queue;
    ref.message_id = redelivered.message_id;
    ref.lease_id = redelivered.lease_id;
    ref.txn_id = redelivered.txn_id;
    ref.fencing_token = redelivered.fencing_token;
    ref.meta_etag = redelivered.meta_etag;
    acked = 0;
    rc = store->ack_message(store, &ref, &acked, &error);
    lc_pouch_queue_message_info_cleanup(&allocator, &redelivered);
    lc_pouch_queue_message_info_cleanup(&allocator, &dequeued);
    lc_pouch_queue_message_info_cleanup(&allocator, &enqueued);
    if (rc != LC_OK || !acked) {
      fprintf(stderr,
              "pouch-queue-txn-rollback failed during final ack at "
              "iteration %ld rc=%d acked=%d error=%d message=%s\n",
              i, rc, acked, error.code,
              error.message != NULL ? error.message : "(none)");
      store->close(store, &error);
      lc_error_cleanup(&error);
      bench_pouch_cleanup_root(root);
      return 1;
    }
  }

  rc = store->close(store, &error);
  lc_error_cleanup(&error);
  bench_pouch_cleanup_root(root);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_compaction(long iterations) {
  char root[256];
  unsigned char payload[4096];
  lc_pouch_store *store;
  lc_source *source;
  lc_pouch_put_state_opts opts;
  lc_pouch_put_state_res put_res;
  lc_error error;
  long i;
  int rc;

  bench_pouch_root_path(root, sizeof(root), "compact");
  bench_pouch_cleanup_root(root);
  memset(payload, 'x', sizeof(payload));
  lc_error_init(&error);
  store = NULL;
  rc = lc_pouch_disk_open(root, NULL, &store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    return 1;
  }
  memset(&opts, 0, sizeof(opts));
  opts.content_type = "application/octet-stream";

  for (i = 0; i < iterations; ++i) {
    memset(&put_res, 0, sizeof(put_res));
    source = bench_source_from_bytes(payload, sizeof(payload), &error);
    if (source == NULL) {
      store->close(store, &error);
      lc_error_cleanup(&error);
      bench_pouch_cleanup_root(root);
      return 1;
    }
    rc = store->write_state(store, "bench", "compact-hot-key", source, &opts,
                            &put_res, &error);
    lc_source_close(source);
    lc_pouch_put_state_res_cleanup(NULL, &put_res);
    if (rc != LC_OK) {
      store->close(store, &error);
      lc_error_cleanup(&error);
      bench_pouch_cleanup_root(root);
      return 1;
    }
  }

  rc = store->close(store, &error);
  lc_error_cleanup(&error);
  bench_pouch_cleanup_root(root);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_retention_sweep(long iterations) {
  char root[256];
  char key[96];
  char json[96];
  lc_pouch_allocator allocator;
  lc_pouch_store *store;
  lc_pouch_retention_sweep_req req;
  lc_pouch_retention_sweep_res res;
  lc_error error;
  long i;
  int rc;

  bench_pouch_root_path(root, sizeof(root), "retention");
  bench_pouch_cleanup_root(root);
  lc_error_init(&error);
  store = NULL;
  rc = lc_pouch_disk_open(root, NULL, &store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    return 1;
  }

  for (i = 0; i < iterations; ++i) {
    snprintf(key, sizeof(key), "bench/retention/%08ld", i);
    snprintf(json, sizeof(json), "{\"retention\":%ld}", i);
    if (bench_pouch_store_row(store, NULL, key, json, &error) != 0) {
      store->close(store, &error);
      lc_error_cleanup(&error);
      bench_pouch_cleanup_root(root);
      return 1;
    }
  }

  store->close(store, &error);
  store = NULL;
  bench_alloc_metrics_reset();
  bench_pouch_allocator(&allocator);
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }

  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  req.updated_before_unix = (long)time(NULL) + 1L;
  rc = store->retention_sweep(store, &req, &res, &error);
  if (rc == LC_OK &&
      (res.scanned_metadata != (unsigned long)iterations ||
       res.expired_metadata != (unsigned long)iterations ||
       res.deleted_metadata != (unsigned long)iterations ||
       res.deleted_state != (unsigned long)iterations ||
       res.failed_keys != 0UL)) {
    fprintf(stderr,
            "pouch-retention unexpected sweep counts: scanned=%lu "
            "expired=%lu deleted_meta=%lu deleted_state=%lu failed=%lu\n",
            res.scanned_metadata, res.expired_metadata, res.deleted_metadata,
            res.deleted_state, res.failed_keys);
    rc = LC_ERR_PROTOCOL;
  }
  store->close(store, &error);
  lc_error_cleanup(&error);
  bench_pouch_cleanup_root(root);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_scan_meta(long iterations) {
  char root[256];
  lc_pouch_store *store;
  lc_pouch_scan_meta_req req;
  lc_pouch_scan_meta_res res;
  bench_scan_count count;
  lc_error error;
  int rc;

  bench_pouch_root_path(root, sizeof(root), "scan-meta");
  bench_pouch_cleanup_root(root);
  lc_error_init(&error);
  if (bench_pouch_seed_query_rows(root, iterations, &error) != 0) {
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }

  store = NULL;
  rc = lc_pouch_disk_open(root, NULL, &store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&count, 0, sizeof(count));
  req.namespace_name = "bench";
  req.limit = (size_t)iterations;
  rc = store->scan_meta(store, &req, bench_scan_count_visit, &count, &res,
                        &error);
  lc_pouch_scan_meta_res_cleanup(NULL, &res);
  store->close(store, &error);
  lc_error_cleanup(&error);
  bench_pouch_cleanup_root(root);
  return rc == LC_OK && count.rows == iterations ? 0 : 1;
}

static int bench_pouch_open_rebuild(long iterations) {
  char root[256];
  lc_pouch_allocator allocator;
  lc_pouch_store *store;
  lc_error error;
  int rc;

  bench_pouch_root_path(root, sizeof(root), "open-rebuild");
  bench_pouch_cleanup_root(root);
  lc_error_init(&error);
  if (bench_pouch_seed_query_rows(root, iterations, &error) != 0) {
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }

  bench_alloc_metrics_reset();
  bench_pouch_allocator(&allocator);
  store = NULL;
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  if (rc == LC_OK) {
    rc = store->close(store, &error);
  }
  lc_error_cleanup(&error);
  bench_pouch_cleanup_root(root);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_index_scan(long iterations) {
  char root[256];
  lc_pouch_allocator allocator;
  lc_pouch_store *store;
  lc_pouch_query_index_scan_req req;
  lc_pouch_query_index_scan_res res;
  bench_scan_count count;
  lc_error error;
  int rc;

  bench_pouch_root_path(root, sizeof(root), "index-scan");
  bench_pouch_cleanup_root(root);
  lc_error_init(&error);
  bench_pouch_allocator(&allocator);
  if (bench_pouch_seed_query_rows_with_allocator(root, iterations, &allocator,
                                                 &error) != 0) {
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }

  bench_alloc_metrics_reset();
  store = NULL;
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&count, 0, sizeof(count));
  req.namespace_name = "bench";
  req.limit = (size_t)iterations;
  rc = store->query_index_scan(store, &req, bench_scan_count_visit, &count,
                               &res, &error);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &res);
  store->close(store, &error);
  lc_error_cleanup(&error);
  bench_pouch_cleanup_root(root);
  return rc == LC_OK && count.rows == iterations ? 0 : 1;
}

static int bench_pouch_index_keys(long iterations) {
  char root[256];
  lc_pouch_allocator allocator;
  lc_pouch_store *store;
  lc_pouch_query_index_scan_req req;
  lc_pouch_query_index_scan_res res;
  bench_scan_count count;
  lc_error error;
  int rc;

  bench_pouch_root_path(root, sizeof(root), "index-keys");
  bench_pouch_cleanup_root(root);
  lc_error_init(&error);
  bench_pouch_allocator(&allocator);
  if (bench_pouch_seed_query_rows_with_allocator(root, iterations, &allocator,
                                                 &error) != 0) {
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }

  bench_alloc_metrics_reset();
  store = NULL;
  rc = lc_pouch_disk_open(root, &allocator, &store, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }
  memset(&req, 0, sizeof(req));
  memset(&res, 0, sizeof(res));
  memset(&count, 0, sizeof(count));
  req.namespace_name = "bench";
  req.limit = (size_t)iterations;
  rc = store->query_index_keys_scan(store, &req, bench_key_count_visit, &count,
                                    &res, &error);
  lc_pouch_query_index_scan_res_cleanup(&allocator, &res);
  store->close(store, &error);
  lc_error_cleanup(&error);
  bench_pouch_cleanup_root(root);
  return rc == LC_OK && count.rows == iterations ? 0 : 1;
}

static int bench_pouch_scan_query(long iterations) {
  char root[256];
  char endpoint[320];
  lc_client_config config;
  const char *endpoints[1];
  lc_client *client;
  lc_sink *sink;
  lc_query_req req;
  lc_query_res res;
  lc_error error;
  int rc;

  bench_pouch_root_path(root, sizeof(root), "scan-query");
  bench_pouch_cleanup_root(root);
  lc_error_init(&error);
  if (bench_pouch_seed_query_rows(root, iterations, &error) != 0) {
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }

  snprintf(endpoint, sizeof(endpoint), "pouch://%s", root);
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "bench";
  config.pouch_query_engine = "scan";
  client = NULL;
  rc = lc_client_open(&config, &client, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }
  sink = NULL;
  rc = lc_sink_to_file("/dev/null", &sink, &error);
  if (rc != LC_OK) {
    client->close(client);
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }
  lc_query_req_init(&req);
  memset(&res, 0, sizeof(res));
  req.selector_json = "{}";
  req.limit = iterations;
  rc = client->query(client, &req, sink, &res, &error);
  lc_query_res_cleanup(&res);
  lc_sink_close(sink);
  client->close(client);
  lc_error_cleanup(&error);
  bench_pouch_cleanup_root(root);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_index_query(long iterations) {
  char root[256];
  char endpoint[320];
  lc_client_config config;
  const char *endpoints[1];
  lc_client *client;
  lc_sink *sink;
  lc_query_req req;
  lc_query_res res;
  lc_error error;
  int rc;

  bench_pouch_root_path(root, sizeof(root), "index-query");
  bench_pouch_cleanup_root(root);
  lc_error_init(&error);
  if (bench_pouch_seed_query_rows(root, iterations, &error) != 0) {
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }

  snprintf(endpoint, sizeof(endpoint), "pouch://%s", root);
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "bench";
  client = NULL;
  rc = lc_client_open(&config, &client, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }
  sink = NULL;
  rc = lc_sink_to_file("/dev/null", &sink, &error);
  if (rc != LC_OK) {
    client->close(client);
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }
  lc_query_req_init(&req);
  memset(&res, 0, sizeof(res));
  req.selector_json = "{}";
  req.limit = iterations;
  rc = client->query(client, &req, sink, &res, &error);
  if (rc == LC_OK && res.index_seq == 0UL) {
    fprintf(stderr, "pouch-index-query did not report an index sequence\n");
    rc = LC_ERR_PROTOCOL;
  }
  lc_query_res_cleanup(&res);
  lc_sink_close(sink);
  client->close(client);
  lc_error_cleanup(&error);
  bench_pouch_cleanup_root(root);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_scan_query_keys(long iterations) {
  char root[256];
  char endpoint[320];
  lc_client_config config;
  const char *endpoints[1];
  lc_client *client;
  lc_query_req req;
  lc_query_res res;
  lc_query_key_handler handler;
  bench_query_key_count count;
  lc_error error;
  int rc;

  bench_pouch_root_path(root, sizeof(root), "scan-query-keys");
  bench_pouch_cleanup_root(root);
  lc_error_init(&error);
  if (bench_pouch_seed_query_rows(root, iterations, &error) != 0) {
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }

  snprintf(endpoint, sizeof(endpoint), "pouch://%s", root);
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "bench";
  config.pouch_query_engine = "scan";
  client = NULL;
  rc = lc_client_open(&config, &client, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }

  lc_query_req_init(&req);
  memset(&res, 0, sizeof(res));
  memset(&handler, 0, sizeof(handler));
  memset(&count, 0, sizeof(count));
  handler.begin = bench_query_key_begin;
  handler.chunk = bench_query_key_chunk;
  handler.end = bench_query_key_end;
  req.selector_json = "{}";
  req.limit = iterations;
  rc = client->query_keys(client, &req, &handler, &count, &res, &error);
  if (rc == LC_OK && count.rows != iterations) {
    fprintf(stderr,
            "pouch-scan-query-keys streamed %ld keys, expected %ld\n",
            count.rows, iterations);
    rc = LC_ERR_PROTOCOL;
  }
  if (rc == LC_OK &&
      (res.return_mode == NULL || strcmp(res.return_mode, "keys") != 0)) {
    fprintf(stderr, "pouch-scan-query-keys returned unexpected mode %s\n",
            res.return_mode != NULL ? res.return_mode : "(null)");
    rc = LC_ERR_PROTOCOL;
  }
  lc_query_res_cleanup(&res);
  client->close(client);
  lc_error_cleanup(&error);
  bench_pouch_cleanup_root(root);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_index_query_keys(long iterations) {
  char root[256];
  char endpoint[320];
  lc_client_config config;
  const char *endpoints[1];
  lc_client *client;
  lc_query_req req;
  lc_query_res res;
  lc_query_key_handler handler;
  bench_query_key_count count;
  lc_error error;
  int rc;

  bench_pouch_root_path(root, sizeof(root), "index-query-keys");
  bench_pouch_cleanup_root(root);
  lc_error_init(&error);
  if (bench_pouch_seed_query_rows(root, iterations, &error) != 0) {
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }

  snprintf(endpoint, sizeof(endpoint), "pouch://%s", root);
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "bench";
  client = NULL;
  rc = lc_client_open(&config, &client, &error);
  if (rc != LC_OK) {
    lc_error_cleanup(&error);
    bench_pouch_cleanup_root(root);
    return 1;
  }

  lc_query_req_init(&req);
  memset(&res, 0, sizeof(res));
  memset(&handler, 0, sizeof(handler));
  memset(&count, 0, sizeof(count));
  handler.begin = bench_query_key_begin;
  handler.chunk = bench_query_key_chunk;
  handler.end = bench_query_key_end;
  req.selector_json = "{}";
  req.limit = iterations;
  rc = client->query_keys(client, &req, &handler, &count, &res, &error);
  if (rc == LC_OK && count.rows != iterations) {
    fprintf(stderr,
            "pouch-index-query-keys streamed %ld keys, expected %ld\n",
            count.rows, iterations);
    rc = LC_ERR_PROTOCOL;
  }
  if (rc == LC_OK &&
      (res.return_mode == NULL || strcmp(res.return_mode, "keys") != 0)) {
    fprintf(stderr, "pouch-index-query-keys returned unexpected mode %s\n",
            res.return_mode != NULL ? res.return_mode : "(null)");
    rc = LC_ERR_PROTOCOL;
  }
  if (rc == LC_OK && res.index_seq == 0UL) {
    fprintf(stderr,
            "pouch-index-query-keys did not report an index sequence\n");
    rc = LC_ERR_PROTOCOL;
  }
  lc_query_res_cleanup(&res);
  client->close(client);
  lc_error_cleanup(&error);
  bench_pouch_cleanup_root(root);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_key_lock_contention(long iterations) {
  char root[256];
  lc_pouch_allocator allocator;
  lc_pouch_store *first;
  lc_pouch_store *second;
  lc_pouch_key_lock *first_lock;
  lc_pouch_key_lock *second_lock;
  lc_pouch_lock_status baseline;
  lc_pouch_lock_status status;
  lc_error error;
  unsigned long baseline_contentions;
  long i;
  int acquired;
  int rc;

  bench_pouch_root_path(root, sizeof(root), "key-lock-contention");
  bench_pouch_cleanup_root(root);
  lc_error_init(&error);
  bench_pouch_allocator(&allocator);
  first = NULL;
  second = NULL;
  first_lock = NULL;
  second_lock = NULL;
  memset(&baseline, 0, sizeof(baseline));
  memset(&status, 0, sizeof(status));
  baseline_contentions = 0UL;
  rc = LC_OK;

  rc = lc_pouch_disk_open(root, &allocator, &first, &error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  rc = lc_pouch_disk_open(root, &allocator, &second, &error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  if (first->try_lock_key == NULL || first->unlock_key == NULL ||
      second->try_lock_key == NULL || second->unlock_key == NULL ||
      second->lock_status == NULL) {
    fprintf(stderr, "pouch key lock benchmark requires lock methods\n");
    rc = LC_ERR_INVALID;
    goto cleanup;
  }

  rc = second->lock_status(second, &baseline, &error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  baseline_contentions = baseline.process_key_lock_contentions;
  lc_pouch_lock_status_cleanup(&allocator, &baseline);

  for (i = 0; i < iterations; ++i) {
    acquired = 0;
    rc = first->try_lock_key(first, "bench", "hot-key", &first_lock, &acquired,
                             &error);
    if (rc != LC_OK || !acquired || first_lock == NULL) {
      fprintf(stderr, "pouch key lock benchmark failed first acquisition\n");
      rc = rc == LC_OK ? LC_ERR_PROTOCOL : rc;
      goto cleanup;
    }

    acquired = 1;
    rc = second->try_lock_key(second, "bench", "hot-key", &second_lock,
                              &acquired, &error);
    if (rc != LC_OK || acquired || second_lock != NULL) {
      fprintf(stderr, "pouch key lock benchmark failed contention check\n");
      rc = rc == LC_OK ? LC_ERR_PROTOCOL : rc;
      goto cleanup;
    }

    rc = first->unlock_key(first, first_lock, &error);
    first_lock = NULL;
    if (rc != LC_OK) {
      goto cleanup;
    }

    acquired = 0;
    rc = second->try_lock_key(second, "bench", "hot-key", &second_lock,
                              &acquired, &error);
    if (rc != LC_OK || !acquired || second_lock == NULL) {
      fprintf(stderr, "pouch key lock benchmark failed second acquisition\n");
      rc = rc == LC_OK ? LC_ERR_PROTOCOL : rc;
      goto cleanup;
    }

    rc = second->unlock_key(second, second_lock, &error);
    second_lock = NULL;
    if (rc != LC_OK) {
      goto cleanup;
    }
  }

  rc = second->lock_status(second, &status, &error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  if (status.process_key_lock_contentions < baseline_contentions ||
      status.process_key_lock_contentions - baseline_contentions <
          (unsigned long)iterations) {
    fprintf(stderr,
            "pouch key lock benchmark observed %lu contentions, expected at "
            "least %ld\n",
            status.process_key_lock_contentions - baseline_contentions,
            iterations);
    rc = LC_ERR_PROTOCOL;
    goto cleanup;
  }
  if (status.process_active_key_locks != 0U) {
    fprintf(stderr, "pouch key lock benchmark leaked active locks\n");
    rc = LC_ERR_PROTOCOL;
    goto cleanup;
  }

cleanup:
  lc_pouch_lock_status_cleanup(&allocator, &baseline);
  lc_pouch_lock_status_cleanup(&allocator, &status);
  if (second_lock != NULL && second != NULL) {
    (void)second->unlock_key(second, second_lock, &error);
  }
  if (first_lock != NULL && first != NULL) {
    (void)first->unlock_key(first, first_lock, &error);
  }
  if (second != NULL) {
    (void)second->close(second, &error);
  }
  if (first != NULL) {
    (void)first->close(first, &error);
  }
  lc_error_cleanup(&error);
  bench_pouch_cleanup_root(root);
  return rc == LC_OK ? 0 : 1;
}

static int run_case(const bench_case *test_case, long iterations) {
  double start_seconds;
  double end_seconds;
  double elapsed_seconds;
  double ops_per_sec;
  double ns_per_op;
  long effective_iterations;

  effective_iterations =
      iterations > 0L ? iterations : test_case->default_iterations;
  bench_alloc_metrics_reset();
  start_seconds = bench_now_seconds();
  if (test_case->run(effective_iterations) != 0) {
    return 1;
  }
  end_seconds = bench_now_seconds();
  elapsed_seconds = end_seconds - start_seconds;
  if (elapsed_seconds <= 0.0) {
    elapsed_seconds = 0.000000001;
  }

  ops_per_sec = (double)effective_iterations / elapsed_seconds;
  ns_per_op =
      (elapsed_seconds * 1000000000.0) / (double)effective_iterations;
  printf("%-18s %12ld %14.2f %14.2f %12lu %12lu %12lu\n", test_case->name,
         effective_iterations, ops_per_sec, ns_per_op,
         g_bench_alloc_metrics.malloc_calls +
             g_bench_alloc_metrics.calloc_calls +
             g_bench_alloc_metrics.realloc_calls,
         g_bench_alloc_metrics.free_calls,
         (unsigned long)g_bench_alloc_metrics.peak_outstanding_bytes);
  return 0;
}

static void print_usage(const char *argv0) {
  fprintf(stderr, "usage: %s [iterations] [all|streams|json|", argv0);
  fprintf(stderr, "mutate-parse|mutate-apply|pouch-state|");
  fprintf(stderr, "pouch-state-write-1k|pouch-state-write-64k|");
  fprintf(stderr, "pouch-state-write-1m|pouch-state-write-16m|");
  fprintf(stderr, "pouch-state-read-1k|pouch-state-read-64k|");
  fprintf(stderr, "pouch-state-read-1m|pouch-state-read-16m|");
  fprintf(stderr, "pouch-staged|pouch-public-mutate|pouch-object|");
  fprintf(stderr, "pouch-queue|pouch-queue-txn-rollback|");
  fprintf(stderr, "pouch-compaction|pouch-retention|pouch-scan-meta|");
  fprintf(stderr, "pouch-open-rebuild|pouch-index-scan|");
  fprintf(stderr, "pouch-index-keys|pouch-scan-query|pouch-index-query|");
  fprintf(stderr, "pouch-scan-query-keys|pouch-index-query-keys|");
  fprintf(stderr, "pouch-key-lock-contention]\n");
}

int main(int argc, char **argv) {
  static const bench_case bench_cases[] = {
      {"streams", 200000L, bench_stream_copy},
      {"json", 200000L, bench_json_stream},
      {"mutate-parse", 200000L, bench_mutate_parse},
      {"mutate-apply", 200000L, bench_mutate_apply},
      {"pouch-state", 1000L, bench_pouch_state_roundtrip},
      {"pouch-state-write-1k", 1000L, bench_pouch_state_write_1k},
      {"pouch-state-write-64k", 300L, bench_pouch_state_write_64k},
      {"pouch-state-write-1m", 30L, bench_pouch_state_write_1m},
      {"pouch-state-write-16m", 2L, bench_pouch_state_write_16m},
      {"pouch-state-read-1k", 1000L, bench_pouch_state_read_1k},
      {"pouch-state-read-64k", 300L, bench_pouch_state_read_64k},
      {"pouch-state-read-1m", 30L, bench_pouch_state_read_1m},
      {"pouch-state-read-16m", 2L, bench_pouch_state_read_16m},
      {"pouch-staged", 1000L, bench_pouch_staged_promote},
      {"pouch-public-mutate", 1000L, bench_pouch_public_mutate},
      {"pouch-object", 1000L, bench_pouch_object_roundtrip},
      {"pouch-queue", 1000L, bench_pouch_queue_roundtrip},
      {"pouch-queue-txn-rollback", 1000L, bench_pouch_queue_txn_rollback},
      {"pouch-compaction", 120L, bench_pouch_compaction},
      {"pouch-retention", 1000L, bench_pouch_retention_sweep},
      {"pouch-scan-meta", 1000L, bench_pouch_scan_meta},
      {"pouch-open-rebuild", 1000L, bench_pouch_open_rebuild},
      {"pouch-index-scan", 1000L, bench_pouch_index_scan},
      {"pouch-index-keys", 1000L, bench_pouch_index_keys},
      {"pouch-scan-query", 1000L, bench_pouch_scan_query},
      {"pouch-index-query", 1000L, bench_pouch_index_query},
      {"pouch-scan-query-keys", 1000L, bench_pouch_scan_query_keys},
      {"pouch-index-query-keys", 1000L, bench_pouch_index_query_keys},
      {"pouch-key-lock-contention", 1000L,
       bench_pouch_key_lock_contention}};
  const char *scenario;
  long iterations;
  size_t i;
  int ran;

  iterations = 0L;
  scenario = "all";
  if (argc >= 2) {
    iterations = strtol(argv[1], NULL, 10);
  }
  if (argc >= 3) {
    scenario = argv[2];
  }
  if (iterations < 0L) {
    print_usage(argv[0]);
    return 2;
  }

  printf("%-18s %12s %14s %14s %12s %12s %12s\n", "benchmark",
         "iterations", "ops/sec", "ns/op", "allocs", "frees",
         "peak_bytes");

  ran = 0;
  for (i = 0U; i < sizeof(bench_cases) / sizeof(bench_cases[0]); ++i) {
    if (strcmp(scenario, "all") != 0 &&
        strcmp(scenario, bench_cases[i].name) != 0) {
      continue;
    }
    ran = 1;
    if (run_case(&bench_cases[i], iterations) != 0) {
      return 1;
    }
  }

  if (!ran) {
    print_usage(argv[0]);
    return 2;
  }

  return 0;
}

#include "lc/lc.h"
#include "lc_pouch.h"
#include "../tests/support/lc_test_tmp.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define BENCH_POUCH_TMP_PREFIX "/tmp/liblockdc-pouch-bench-"

typedef struct bench_case {
  const char *name;
  long default_iterations;
  int (*run)(long iterations);
} bench_case;

typedef struct bench_query_key_count {
  size_t rows;
} bench_query_key_count;

typedef struct bench_pouch_query_case {
  const char *selector_json;
  const char *selector_lql;
  const char *engine;
  int documents;
} bench_pouch_query_case;

static double bench_now_seconds(void) {
  struct timespec ts;

  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
    return 0.0;
  }
  return (double)ts.tv_sec + ((double)ts.tv_nsec / 1000000000.0);
}

static int bench_stream_copy(long iterations) {
  static const char payload[] =
      "{\"key\":\"orders/42\",\"state\":{\"items\":[1,2,3]}}";
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

static int bench_pouch_root_path(char *buffer, size_t buffer_size,
                                 const char *suffix) {
  char template_path[512];
  int written;

  written = snprintf(template_path, sizeof(template_path),
                     BENCH_POUCH_TMP_PREFIX "%s-XXXXXX", suffix);
  if (written < 0 || (size_t)written >= sizeof(template_path)) {
    return 1;
  }
  return lc_test_tmp_mkdtemp(template_path, buffer, buffer_size,
                            BENCH_POUCH_TMP_PREFIX)
             ? 0
             : 1;
}

static void bench_pouch_cleanup_root(const char *root) {
  lc_test_tmp_cleanup_path(root, BENCH_POUCH_TMP_PREFIX);
}

static int bench_query_key_begin(void *context, lc_error *error) {
  (void)context;
  (void)error;
  return 1;
}

static int bench_query_key_chunk(void *context, const char *bytes, size_t len,
                                 lc_error *error) {
  (void)context;
  (void)bytes;
  (void)len;
  (void)error;
  return 1;
}

static int bench_query_key_end(void *context, lc_error *error) {
  bench_query_key_count *count;

  (void)error;
  count = (bench_query_key_count *)context;
  count->rows++;
  return 1;
}

static int bench_pouch_client_open(const char *root, lc_client **out,
                                   lc_error *error) {
  lc_client_config config;
  const char *endpoints[1];
  char endpoint[640];

  snprintf(endpoint, sizeof(endpoint), "pouch://%s", root);
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "bench";
  return lc_client_open(&config, out, error);
}

static int bench_pouch_seed_query_docs(lc_client *client, long count,
                                       lc_error *error) {
  lc_update_req req;
  lc_update_res res;
  long i;

  for (i = 0; i < count; ++i) {
    lc_source *source;
    char key[64];
    const char *bucket;
    const char *group;
    const char *region;
    const char *tag0;
    const char *tag1;
    const char *created_at;
    const char *message;
    const char *flag;
    char json[512];
    int match;
    int written;
    int rc;

    match = (i % 2L) == 0L;
    bucket = i % 64L == 0L ? "needle" : "haystack";
    group = match ? "even" : "odd";
    if (i % 3L == 0L) {
      region = "us";
    } else if (i % 3L == 1L) {
      region = "eu";
    } else {
      region = "apac";
    }
    tag0 = match ? "planning" : "runtime";
    tag1 = i % 4L == 0L ? "finance" : "ops";
    if (i % 5L == 0L) {
      created_at = "2026-01-01T00:00:00Z";
    } else if (i % 5L == 1L) {
      created_at = "not-a-date";
    } else {
      created_at = "2024-01-01T00:00:00Z";
    }
    message = i % 8L == 0L ? "timeout" : "ordinary";
    flag = i % 7L == 0L ? "true" : "false";
    snprintf(key, sizeof(key), "doc/%08ld", i);
    written = snprintf(
        json, sizeof(json),
        "{\"n\":%ld,\"owner\":\"%s\",\"bucket\":\"%s\",\"group\":\"%s\","
        "\"region\":\"%s\",\"value\":%ld,\"tags\":[\"%s\",\"%s\"],"
        "\"created_at\":\"%s\",\"details\":{\"message\":\"%s benchmark "
        "document %ld\"},\"flag\":%s}",
        i, match ? "alpha" : "beta", bucket, group, region, i, tag0, tag1,
        created_at, message, i, flag);
    if (written <= 0 || (size_t)written >= sizeof(json)) {
      return 1;
    }
    source = NULL;
    rc = lc_source_from_memory(json, strlen(json), &source, error);
    if (rc != LC_OK) {
      return rc;
    }
    lc_update_req_init(&req);
    memset(&res, 0, sizeof(res));
    req.lease.namespace_name = "bench";
    req.lease.key = key;
    req.content_type = "application/json";
    rc = client->update(client, &req, source, &res, error);
    lc_source_close(source);
    lc_update_res_cleanup(&res);
    if (rc != LC_OK) {
      return rc;
    }
  }
  return LC_OK;
}

static int bench_pouch_query_once(lc_client *client,
                                  const bench_pouch_query_case *query_case,
                                  long count, lc_error *error) {
  lc_query_req req;
  lc_query_res res;
  int rc;

  memset(&res, 0, sizeof(res));
  lc_query_req_init(&req);
  req.namespace_name = "bench";
  req.selector_json = query_case->selector_json;
  req.selector_lql = query_case->selector_lql;
  req.engine = query_case->engine;
  req.limit = count > 0L ? count : 1L;
  if (query_case->documents) {
    lc_sink *sink;

    sink = NULL;
    rc = lc_sink_to_memory(&sink, error);
    if (rc == LC_OK) {
      rc = client->query(client, &req, sink, &res, error);
    }
    if (sink != NULL) {
      lc_sink_close(sink);
    }
  } else {
    lc_query_key_handler handler;
    bench_query_key_count key_count;

    memset(&handler, 0, sizeof(handler));
    memset(&key_count, 0, sizeof(key_count));
    handler.begin = bench_query_key_begin;
    handler.chunk = bench_query_key_chunk;
    handler.end = bench_query_key_end;
    rc = client->query_keys(client, &req, &handler, &key_count, &res, error);
  }
  lc_query_res_cleanup(&res);
  return rc;
}

static int bench_pouch_query_text(long iterations,
                                  const bench_pouch_query_case *query_case) {
  lc_client *client;
  lc_error error;
  char root[512];
  int rc;

  lc_error_init(&error);
  client = NULL;
  root[0] = '\0';
  rc = 0;
  if (iterations <= 0L) {
    iterations = 1L;
  }
  if (bench_pouch_root_path(root, sizeof(root), "query-text") != 0) {
    rc = 1;
    goto done;
  }
  if (bench_pouch_client_open(root, &client, &error) != LC_OK) {
    rc = 1;
    goto done;
  }
  if (bench_pouch_seed_query_docs(client, iterations, &error) != LC_OK) {
    rc = 1;
    goto done;
  }
  if (bench_pouch_query_once(client, query_case, iterations, &error) != LC_OK) {
    rc = 1;
    goto done;
  }

done:
  if (client != NULL) {
    client->close(client);
  }
  bench_pouch_cleanup_root(root);
  lc_error_cleanup(&error);
  return rc;
}

static int bench_pouch_open(long iterations) {
  lc_error error;
  char root[512];
  long i;
  int rc;

  lc_error_init(&error);
  root[0] = '\0';
  rc = 0;
  if (bench_pouch_root_path(root, sizeof(root), "open") != 0) {
    rc = 1;
    goto done;
  }
  for (i = 0; i < iterations; ++i) {
    lc_pouch *pouch;

    pouch = NULL;
    if (lc_pouch_open(root, NULL, NULL, &pouch, &error) != LC_OK) {
      rc = 1;
      goto done;
    }
    lc_pouch_close(pouch);
  }

done:
  bench_pouch_cleanup_root(root);
  lc_error_cleanup(&error);
  return rc;
}

static int bench_pouch_namespace(long iterations) {
  lc_error error;
  lc_pouch *pouch;
  char root[512];
  long i;
  int rc;

  lc_error_init(&error);
  pouch = NULL;
  root[0] = '\0';
  rc = 0;
  if (bench_pouch_root_path(root, sizeof(root), "namespace") != 0) {
    rc = 1;
    goto done;
  }
  if (lc_pouch_open(root, NULL, NULL, &pouch, &error) != LC_OK) {
    rc = 1;
    goto done;
  }
  for (i = 0; i < iterations; ++i) {
    char namespace_name[64];

    snprintf(namespace_name, sizeof(namespace_name), "bench/%ld", i);
    if (lc_pouch_ensure_namespace(pouch, namespace_name, &error) != LC_OK) {
      rc = 1;
      goto done;
    }
  }

done:
  if (pouch != NULL) {
    lc_pouch_close(pouch);
  }
  bench_pouch_cleanup_root(root);
  lc_error_cleanup(&error);
  return rc;
}

static int bench_pouch_query_iprefix_index_keys(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"iprefix\":{\"field\":\"/tags[]\",\"value\":\"FIN\"}}", NULL,
      "index", 0};

  return bench_pouch_query_text(iterations, &query_case);
}

static int bench_pouch_query_iprefix_scan_keys(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"iprefix\":{\"field\":\"/tags[]\",\"value\":\"FIN\"}}", NULL, "scan",
      0};

  return bench_pouch_query_text(iterations, &query_case);
}

static int bench_pouch_query_iprefix_index_documents(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"iprefix\":{\"field\":\"/tags[]\",\"value\":\"FIN\"}}", NULL,
      "index", 1};

  return bench_pouch_query_text(iterations, &query_case);
}

static int bench_pouch_query_iprefix_scan_documents(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"iprefix\":{\"field\":\"/tags[]\",\"value\":\"FIN\"}}", NULL, "scan",
      1};

  return bench_pouch_query_text(iterations, &query_case);
}

static int bench_pouch_query_icontains_index_keys(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"icontains\":{\"field\":\"/tags[]\",\"value\":\"NAN\"}}", NULL,
      "index", 0};

  return bench_pouch_query_text(iterations, &query_case);
}

static int bench_pouch_query_icontains_scan_keys(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"icontains\":{\"field\":\"/tags[]\",\"value\":\"NAN\"}}", NULL,
      "scan", 0};

  return bench_pouch_query_text(iterations, &query_case);
}

static int bench_pouch_query_icontains_index_documents(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"icontains\":{\"field\":\"/tags[]\",\"value\":\"NAN\"}}", NULL,
      "index", 1};

  return bench_pouch_query_text(iterations, &query_case);
}

static int bench_pouch_query_icontains_scan_documents(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"icontains\":{\"field\":\"/tags[]\",\"value\":\"NAN\"}}", NULL,
      "scan", 1};

  return bench_pouch_query_text(iterations, &query_case);
}

static int bench_pouch_query_recursive_exists_index_keys(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"exists\":\"/details/**\"}", NULL, "index", 0};

  return bench_pouch_query_text(iterations, &query_case);
}

static int bench_pouch_query_recursive_exists_scan_keys(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"exists\":\"/details/**\"}", NULL, "scan", 0};

  return bench_pouch_query_text(iterations, &query_case);
}

static int bench_pouch_query_recursive_exists_index_documents(
    long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"exists\":\"/details/**\"}", NULL, "index", 1};

  return bench_pouch_query_text(iterations, &query_case);
}

static int bench_pouch_query_recursive_exists_scan_documents(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"exists\":\"/details/**\"}", NULL, "scan", 1};

  return bench_pouch_query_text(iterations, &query_case);
}

#define BENCH_POUCH_LQL_FUNC(function_name, selector_text, engine_text,        \
                             documents_value)                                 \
  static int function_name(long iterations) {                                  \
    static const bench_pouch_query_case query_case = {                         \
        NULL, selector_text, engine_text, documents_value};                    \
                                                                               \
    return bench_pouch_query_text(iterations, &query_case);                    \
  }

BENCH_POUCH_LQL_FUNC(bench_pouch_query_eq_sparse_index_keys,
                     "eq{field=/bucket,value=needle}", "index", 0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_eq_sparse_scan_keys,
                     "eq{field=/bucket,value=needle}", "scan", 0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_eq_sparse_index_documents,
                     "eq{field=/bucket,value=needle}", "index", 1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_eq_sparse_scan_documents,
                     "eq{field=/bucket,value=needle}", "scan", 1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_eq_dense_index_keys,
                     "eq{field=/group,value=even}", "index", 0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_eq_dense_scan_keys,
                     "eq{field=/group,value=even}", "scan", 0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_eq_dense_index_documents,
                     "eq{field=/group,value=even}", "index", 1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_eq_dense_scan_documents,
                     "eq{field=/group,value=even}", "scan", 1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_range_half_index_keys,
                     "range{field=/value,gte=0}", "index", 0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_range_half_scan_keys,
                     "range{field=/value,gte=0}", "scan", 0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_range_half_index_documents,
                     "range{field=/value,gte=0}", "index", 1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_range_half_scan_documents,
                     "range{field=/value,gte=0}", "scan", 1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_in_region_single_index_keys,
                     "in{field=/region,any=us}", "index", 0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_in_region_single_scan_keys,
                     "in{field=/region,any=us}", "scan", 0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_in_region_single_index_documents,
                     "in{field=/region,any=us}", "index", 1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_in_region_single_scan_documents,
                     "in{field=/region,any=us}", "scan", 1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_in_tags_index_keys,
                     "in{field=/tags[],any=planning|finance}", "index", 0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_in_tags_scan_keys,
                     "in{field=/tags[],any=planning|finance}", "scan", 0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_in_tags_index_documents,
                     "in{field=/tags[],any=planning|finance}", "index", 1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_in_tags_scan_documents,
                     "in{field=/tags[],any=planning|finance}", "scan", 1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_contains_message_index_keys,
                     "contains{field=/details/message,value=timeout}",
                     "index", 0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_contains_message_scan_keys,
                     "contains{field=/details/message,value=timeout}", "scan",
                     0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_contains_message_index_documents,
                     "contains{field=/details/message,value=timeout}",
                     "index", 1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_contains_message_scan_documents,
                     "contains{field=/details/message,value=timeout}", "scan",
                     1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_date_after_index_keys,
                     "date{field=/created_at,after=2025-01-01T00:00:00Z}",
                     "index", 0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_date_after_scan_keys,
                     "date{field=/created_at,after=2025-01-01T00:00:00Z}",
                     "scan", 0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_date_after_index_documents,
                     "date{field=/created_at,after=2025-01-01T00:00:00Z}",
                     "index", 1)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_date_after_scan_documents,
                     "date{field=/created_at,after=2025-01-01T00:00:00Z}",
                     "scan", 1)
BENCH_POUCH_LQL_FUNC(
    bench_pouch_query_or_sparse_or_flag_scan_keys,
    "or.eq{field=/bucket,value=needle},or.eq{field=/flag,value=true}", "scan",
    0)
BENCH_POUCH_LQL_FUNC(
    bench_pouch_query_or_sparse_or_flag_scan_documents,
    "or.eq{field=/bucket,value=needle},or.eq{field=/flag,value=true}", "scan",
    1)

static const bench_case *bench_cases(void) {
  static const bench_case cases[] = {
      {"stream-copy", 1000L, bench_stream_copy},
      {"pouch-open", 1000L, bench_pouch_open},
      {"pouch-namespace", 1000L, bench_pouch_namespace},
      {"pouch-query-eq-sparse-index-keys", 1024L,
       bench_pouch_query_eq_sparse_index_keys},
      {"pouch-query-eq-sparse-scan-keys", 1024L,
       bench_pouch_query_eq_sparse_scan_keys},
      {"pouch-query-eq-sparse-index-documents", 1024L,
       bench_pouch_query_eq_sparse_index_documents},
      {"pouch-query-eq-sparse-scan-documents", 1024L,
       bench_pouch_query_eq_sparse_scan_documents},
      {"pouch-query-eq-dense-index-keys", 1024L,
       bench_pouch_query_eq_dense_index_keys},
      {"pouch-query-eq-dense-scan-keys", 1024L,
       bench_pouch_query_eq_dense_scan_keys},
      {"pouch-query-eq-dense-index-documents", 1024L,
       bench_pouch_query_eq_dense_index_documents},
      {"pouch-query-eq-dense-scan-documents", 1024L,
       bench_pouch_query_eq_dense_scan_documents},
      {"pouch-query-range-half-index-keys", 1024L,
       bench_pouch_query_range_half_index_keys},
      {"pouch-query-range-half-scan-keys", 1024L,
       bench_pouch_query_range_half_scan_keys},
      {"pouch-query-range-half-index-documents", 1024L,
       bench_pouch_query_range_half_index_documents},
      {"pouch-query-range-half-scan-documents", 1024L,
       bench_pouch_query_range_half_scan_documents},
      {"pouch-query-in-region-single-index-keys", 1024L,
       bench_pouch_query_in_region_single_index_keys},
      {"pouch-query-in-region-single-scan-keys", 1024L,
       bench_pouch_query_in_region_single_scan_keys},
      {"pouch-query-in-region-single-index-documents", 1024L,
       bench_pouch_query_in_region_single_index_documents},
      {"pouch-query-in-region-single-scan-documents", 1024L,
       bench_pouch_query_in_region_single_scan_documents},
      {"pouch-query-in-tags-index-keys", 1024L,
       bench_pouch_query_in_tags_index_keys},
      {"pouch-query-in-tags-scan-keys", 1024L,
       bench_pouch_query_in_tags_scan_keys},
      {"pouch-query-in-tags-index-documents", 1024L,
       bench_pouch_query_in_tags_index_documents},
      {"pouch-query-in-tags-scan-documents", 1024L,
       bench_pouch_query_in_tags_scan_documents},
      {"pouch-query-contains-message-index-keys", 1024L,
       bench_pouch_query_contains_message_index_keys},
      {"pouch-query-contains-message-scan-keys", 1024L,
       bench_pouch_query_contains_message_scan_keys},
      {"pouch-query-contains-message-index-documents", 1024L,
       bench_pouch_query_contains_message_index_documents},
      {"pouch-query-contains-message-scan-documents", 1024L,
       bench_pouch_query_contains_message_scan_documents},
      {"pouch-query-date-after-index-keys", 1024L,
       bench_pouch_query_date_after_index_keys},
      {"pouch-query-date-after-scan-keys", 1024L,
       bench_pouch_query_date_after_scan_keys},
      {"pouch-query-date-after-index-documents", 1024L,
       bench_pouch_query_date_after_index_documents},
      {"pouch-query-date-after-scan-documents", 1024L,
       bench_pouch_query_date_after_scan_documents},
      {"pouch-query-or-sparse-or-flag-scan-keys", 1024L,
       bench_pouch_query_or_sparse_or_flag_scan_keys},
      {"pouch-query-or-sparse-or-flag-scan-documents", 1024L,
       bench_pouch_query_or_sparse_or_flag_scan_documents},
      {"pouch-query-iprefix-index-keys", 1024L,
       bench_pouch_query_iprefix_index_keys},
      {"pouch-query-iprefix-scan-keys", 1024L,
       bench_pouch_query_iprefix_scan_keys},
      {"pouch-query-iprefix-index-documents", 1024L,
       bench_pouch_query_iprefix_index_documents},
      {"pouch-query-iprefix-scan-documents", 1024L,
       bench_pouch_query_iprefix_scan_documents},
      {"pouch-query-icontains-index-keys", 1024L,
       bench_pouch_query_icontains_index_keys},
      {"pouch-query-icontains-scan-keys", 1024L,
       bench_pouch_query_icontains_scan_keys},
      {"pouch-query-icontains-index-documents", 1024L,
       bench_pouch_query_icontains_index_documents},
      {"pouch-query-icontains-scan-documents", 1024L,
       bench_pouch_query_icontains_scan_documents},
      {"pouch-query-recursive-exists-index-keys", 1024L,
       bench_pouch_query_recursive_exists_index_keys},
      {"pouch-query-recursive-exists-scan-keys", 1024L,
       bench_pouch_query_recursive_exists_scan_keys},
      {"pouch-query-recursive-exists-index-documents", 1024L,
       bench_pouch_query_recursive_exists_index_documents},
      {"pouch-query-recursive-exists-scan-documents", 1024L,
       bench_pouch_query_recursive_exists_scan_documents},
      {NULL, 0L, NULL}};

  return cases;
}

static void bench_usage(const char *argv0) {
  const bench_case *bench;

  fprintf(stderr, "usage: %s [iterations] [all", argv0);
  for (bench = bench_cases(); bench->name != NULL; ++bench) {
    fprintf(stderr, "|%s", bench->name);
  }
  fprintf(stderr, "]\n");
}

static const bench_case *bench_find(const char *name) {
  const bench_case *bench;

  for (bench = bench_cases(); bench->name != NULL; ++bench) {
    if (strcmp(bench->name, name) == 0) {
      return bench;
    }
  }
  return NULL;
}

static int bench_run_one(const bench_case *bench, long iterations) {
  double start;
  double elapsed;
  int rc;

  start = bench_now_seconds();
  rc = bench->run(iterations);
  elapsed = bench_now_seconds() - start;
  printf("%s iterations=%ld seconds=%.6f per_op_us=%.3f rc=%d\n",
         bench->name, iterations, elapsed,
         iterations > 0L ? (elapsed * 1000000.0) / (double)iterations : 0.0,
         rc);
  return rc;
}

int main(int argc, char **argv) {
  const bench_case *bench;
  long iterations;
  const char *name;
  int failed;

  if (argc > 1 && (strcmp(argv[1], "--help") == 0 ||
                   strcmp(argv[1], "-h") == 0)) {
    bench_usage(argv[0]);
    return 0;
  }
  iterations = argc > 1 ? strtol(argv[1], NULL, 10) : 0L;
  name = argc > 2 ? argv[2] : "all";
  failed = 0;
  lc_test_tmp_cleanup_stale("/tmp", "liblockdc-pouch-bench-",
                            BENCH_POUCH_TMP_PREFIX);

  if (strcmp(name, "all") == 0) {
    for (bench = bench_cases(); bench->name != NULL; ++bench) {
      long selected_iterations;

      selected_iterations =
          iterations > 0L ? iterations : bench->default_iterations;
      if (bench_run_one(bench, selected_iterations) != 0) {
        failed = 1;
      }
    }
    return failed ? 1 : 0;
  }

  bench = bench_find(name);
  if (bench == NULL) {
    bench_usage(argv[0]);
    return 1;
  }
  return bench_run_one(bench, iterations > 0L ? iterations
                                              : bench->default_iterations);
}

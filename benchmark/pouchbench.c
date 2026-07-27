#define _XOPEN_SOURCE 700

#include "pouchbench.h"

#include "lc/lc.h"

#include <dirent.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define LOCKDC_POUCH_BENCH_TMP_PREFIX "/tmp/liblockdc-pouch-go-bench-"

typedef struct lockdc_bench_key_count {
  long rows;
} lockdc_bench_key_count;

static uint64_t lockdc_bench_now_ns(void) {
  struct timespec ts;

  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
    return 0U;
  }
  return ((uint64_t)ts.tv_sec * 1000000000ULL) + (uint64_t)ts.tv_nsec;
}

static int lockdc_bench_has_tmp_prefix(const char *path) {
  return path != NULL && strncmp(path, LOCKDC_POUCH_BENCH_TMP_PREFIX,
                                 strlen(LOCKDC_POUCH_BENCH_TMP_PREFIX)) == 0;
}

static void lockdc_bench_cleanup_root(const char *path) {
  DIR *dir;
  struct dirent *entry;

  if (!lockdc_bench_has_tmp_prefix(path)) {
    return;
  }
  dir = opendir(path);
  if (dir == NULL) {
    (void)unlink(path);
    return;
  }
  while ((entry = readdir(dir)) != NULL) {
    char child[1024];
    struct stat st;
    int written;

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    written = snprintf(child, sizeof(child), "%s/%s", path, entry->d_name);
    if (written <= 0 || (size_t)written >= sizeof(child)) {
      continue;
    }
    if (lstat(child, &st) == 0 && S_ISDIR(st.st_mode)) {
      lockdc_bench_cleanup_root(child);
    } else {
      (void)unlink(child);
    }
  }
  (void)closedir(dir);
  (void)rmdir(path);
}

static int lockdc_bench_open_client(const char *root, lc_client **out,
                                    lc_error *error) {
  lc_client_config config;
  const char *endpoints[1];
  char endpoint[768];
  int written;

  written = snprintf(endpoint, sizeof(endpoint), "pouch://%s", root);
  if (written <= 0 || (size_t)written >= sizeof(endpoint)) {
    return LC_ERR_INVALID;
  }
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "bench";
  return lc_client_open(&config, out, error);
}

static const char *lockdc_bench_selector(const char *scenario) {
  if (scenario == NULL || strcmp(scenario, "EqSparse") == 0) {
    return "{\"eq\":{\"field\":\"/bucket\",\"value\":\"needle\"}}";
  }
  if (strcmp(scenario, "EqDense") == 0) {
    return "{\"eq\":{\"field\":\"/group\",\"value\":\"even\"}}";
  }
  if (strcmp(scenario, "RangeHalf") == 0) {
    return "{\"range\":{\"field\":\"/value\",\"gte\":0}}";
  }
  if (strcmp(scenario, "InRegionSingle") == 0) {
    return "{\"in\":{\"field\":\"/region\",\"any\":[\"us\"]}}";
  }
  if (strcmp(scenario, "InTags") == 0) {
    return "{\"in\":{\"field\":\"/"
           "tags[]\",\"any\":[\"planning\",\"finance\"]}}";
  }
  if (strcmp(scenario, "ContainsMessage") == 0) {
    return "{\"contains\":{\"field\":\"/details/"
           "message\",\"value\":\"timeout\"}}";
  }
  if (strcmp(scenario, "DateAfter") == 0) {
    return "{\"date\":{\"field\":\"/"
           "created_at\",\"after\":\"2025-01-01T00:00:00Z\"}}";
  }
  if (strcmp(scenario, "RecursiveExists") == 0) {
    return "{\"exists\":\"/details/**\"}";
  }
  if (strcmp(scenario, "OrSparseOrFlag") == 0) {
    return "{\"or\":[{\"eq\":{\"field\":\"/bucket\",\"value\":\"needle\"}},"
           "{\"eq\":{\"field\":\"/flag\",\"value\":true}}]}";
  }
  return "{\"eq\":{\"field\":\"/bucket\",\"value\":\"needle\"}}";
}

static int lockdc_bench_key_begin(void *context, lc_error *error) {
  (void)context;
  (void)error;
  return 1;
}

static int lockdc_bench_key_chunk(void *context, const char *bytes, size_t len,
                                  lc_error *error) {
  (void)context;
  (void)bytes;
  (void)len;
  (void)error;
  return 1;
}

static int lockdc_bench_key_end(void *context, lc_error *error) {
  lockdc_bench_key_count *count;

  (void)error;
  count = (lockdc_bench_key_count *)context;
  ++count->rows;
  return 1;
}

static int lockdc_bench_seed(lc_client *client, long rows, lc_error *error) {
  long i;

  for (i = 0; i < rows; ++i) {
    lc_update_req req;
    lc_update_res res;
    lc_source *source;
    char key[64];
    char json[512];
    int written;
    int rc;

    written =
        snprintf(json, sizeof(json),
                 "{\"bucket\":\"%s\",\"group\":\"%s\",\"region\":\"%s\","
                 "\"value\":%ld,\"tags\":[\"%s\",\"%s\"],"
                 "\"created_at\":\"%s\","
                 "\"details\":{\"message\":\"%s benchmark document %ld\"},"
                 "\"flag\":%s}",
                 (i % 64L) == 0L ? "needle" : "haystack",
                 (i % 2L) == 0L ? "even" : "odd",
                 (i % 3L) == 0L   ? "us"
                 : (i % 3L) == 1L ? "eu"
                                  : "apac",
                 i, (i % 2L) == 0L ? "planning" : "runtime",
                 (i % 4L) == 0L ? "finance" : "ops",
                 (i % 5L) == 0L   ? "2026-01-01T00:00:00Z"
                 : (i % 5L) == 1L ? "not-a-date"
                                  : "2024-01-01T00:00:00Z",
                 (i % 8L) == 0L ? "timeout" : "ordinary", i,
                 (i % 7L) == 0L ? "true" : "false");
    if (written <= 0 || (size_t)written >= sizeof(json)) {
      return LC_ERR_INVALID;
    }
    snprintf(key, sizeof(key), "doc/%08ld", i);
    lc_update_req_init(&req);
    memset(&res, 0, sizeof(res));
    req.lease.key = key;
    req.content_type = "application/json";
    source = NULL;
    rc = lc_source_from_memory(json, strlen(json), &source, error);
    if (rc == LC_OK) {
      rc = client->update(client, &req, source, &res, error);
    }
    if (source != NULL) {
      lc_source_close(source);
    }
    lc_update_res_cleanup(&res);
    if (rc != LC_OK) {
      return rc;
    }
  }
  return LC_OK;
}

static int lockdc_bench_flush(lc_client *client, lc_error *error) {
  lc_index_flush_req req;
  lc_index_flush_res res;
  int rc;

  lc_index_flush_req_init(&req);
  memset(&res, 0, sizeof(res));
  req.namespace_name = "bench";
  req.mode = "wait";
  rc = client->flush_index(client, &req, &res, error);
  lc_index_flush_res_cleanup(&res);
  return rc;
}

static int lockdc_bench_query(lc_client *client, const char *scenario,
                              const char *engine, int documents, long limit,
                              long *rows, lc_error *error) {
  lc_query_req req;
  lc_query_res res;
  int rc;

  lc_query_req_init(&req);
  memset(&res, 0, sizeof(res));
  req.namespace_name = "bench";
  req.selector_json = lockdc_bench_selector(scenario);
  req.engine = engine;
  req.limit = limit > 0L ? limit : 1L;
  if (documents) {
    lc_sink *sink;

    sink = NULL;
    rc = lc_sink_to_memory(&sink, error);
    if (rc == LC_OK) {
      rc = client->query(client, &req, sink, &res, error);
    }
    if (sink != NULL) {
      lc_sink_close(sink);
    }
    if (rc == LC_OK && rows != NULL) {
      *rows = -1L;
    }
  } else {
    lc_query_key_handler handler;
    lockdc_bench_key_count count;

    memset(&handler, 0, sizeof(handler));
    memset(&count, 0, sizeof(count));
    handler.begin = lockdc_bench_key_begin;
    handler.chunk = lockdc_bench_key_chunk;
    handler.end = lockdc_bench_key_end;
    rc = client->query_keys(client, &req, &handler, &count, &res, error);
    if (rc == LC_OK && rows != NULL) {
      *rows = count.rows;
    }
  }
  lc_query_res_cleanup(&res);
  return rc;
}

int lockdc_pouch_bench_run(const char *scenario, long rows, const char *engine,
                           int documents, lockdc_pouch_bench_result *out) {
  char root_template[] = LOCKDC_POUCH_BENCH_TMP_PREFIX "XXXXXX";
  lc_client *client;
  lc_error error;
  uint64_t start;
  uint64_t end;
  long matched_rows;
  int rc;

  if (out == NULL) {
    return LC_ERR_INVALID;
  }
  memset(out, 0, sizeof(*out));
  if (rows <= 0L) {
    rows = 1L;
  }
  client = NULL;
  matched_rows = 0L;
  lc_error_init(&error);
  if (mkdtemp(root_template) == NULL) {
    out->rc = errno;
    lc_error_cleanup(&error);
    return out->rc;
  }
  rc = lockdc_bench_open_client(root_template, &client, &error);
  if (rc == LC_OK) {
    rc = lockdc_bench_seed(client, rows, &error);
  }
  if (rc == LC_OK) {
    rc = lockdc_bench_flush(client, &error);
  }
  start = lockdc_bench_now_ns();
  if (rc == LC_OK) {
    rc = lockdc_bench_query(client, scenario, engine, documents, rows,
                            &matched_rows, &error);
  }
  end = lockdc_bench_now_ns();
  if (client != NULL) {
    lc_client_close(client);
  }
  lockdc_bench_cleanup_root(root_template);
  lc_error_cleanup(&error);
  out->rc = rc;
  out->rows = matched_rows;
  out->c_ns = end >= start ? end - start : 0U;
  return rc;
}

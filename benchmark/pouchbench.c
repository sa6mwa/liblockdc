#include "pouchbench.h"

#include "../src/lc_api_internal.h"
#include "lc/lc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define BENCHMARK_QUERY_PAGE_LIMIT 1000U

typedef struct key_count {
  uint64_t documents;
  uint64_t bytes;
} key_count;

typedef struct query_scenario {
  const char *name;
} query_scenario;

static uint64_t now_ns(void) {
  struct timespec ts;

  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
    return 0U;
  }
  return ((uint64_t)ts.tv_sec * 1000000000ULL) + (uint64_t)ts.tv_nsec;
}

static char *bench_strdup(const char *value) {
  char *copy;
  size_t len;

  if (value == NULL) {
    return NULL;
  }
  len = strlen(value) + 1U;
  copy = (char *)malloc(len);
  if (copy == NULL) {
    return NULL;
  }
  memcpy(copy, value, len);
  return copy;
}

static int parse_query_candidates(const char *metadata, uint64_t *out) {
  const char needle[] = "\"query_candidates\":";
  const char *at;
  char *end;
  unsigned long long value;

  if (metadata == NULL || out == NULL) {
    return 0;
  }
  at = strstr(metadata, needle);
  if (at == NULL) {
    return 0;
  }
  at += sizeof(needle) - 1U;
  value = strtoull(at, &end, 10);
  if (end == at) {
    return 0;
  }
  *out = (uint64_t)value;
  return 1;
}

static void set_error(lockdc_pouch_bench_result *out, const char *where,
                      const lc_error *error, int rc) {
  const char *message;

  if (out == NULL) {
    return;
  }
  message = error != NULL && error->message != NULL ? error->message : "";
  (void)snprintf(out->error, sizeof(out->error), "%s failed rc=%d %s", where,
                 rc, message);
}

static void capture_result_cache_stats(lc_client *client,
                                       lockdc_pouch_bench_result *out) {
  lc_client_handle *handle;
  lc_pouch_query_result_cache_status status;
  lc_error error;

  if (client == NULL || out == NULL) {
    return;
  }
  handle = (lc_client_handle *)client;
  if (handle->pouch_store == NULL ||
      handle->pouch_store->query_result_cache_status == NULL) {
    return;
  }
  memset(&status, 0, sizeof(status));
  lc_error_init(&error);
  if (handle->pouch_store->query_result_cache_status(
          handle->pouch_store, &status, &error) == LC_OK) {
    out->result_cache_entries = (uint64_t)status.entries;
    out->result_cache_hits = (uint64_t)status.hits;
    out->result_cache_misses = (uint64_t)status.misses;
    out->result_cache_puts = (uint64_t)status.puts;
  }
  lc_error_cleanup(&error);
}

static uint64_t target_row(uint64_t rows) { return rows > 1U ? rows / 2U : 0U; }

static uint64_t count_matching(uint64_t rows,
                               int (*match)(uint64_t i, uint64_t rows)) {
  uint64_t count;
  uint64_t i;

  count = 0U;
  for (i = 0U; i < rows; ++i) {
    if (match(i, rows)) {
      count++;
    }
  }
  return count;
}

static int match_eq_sparse(uint64_t i, uint64_t rows) {
  return i == target_row(rows);
}

static int match_eq_dense(uint64_t i, uint64_t rows) {
  (void)rows;
  return (i % 2U) == 0U;
}

static int match_range_half(uint64_t i, uint64_t rows) {
  return i >= target_row(rows);
}

static int match_in_region(uint64_t i, uint64_t rows) {
  (void)rows;
  return (i % 3U) == 0U || (i % 3U) == 1U;
}

static int match_in_region_single(uint64_t i, uint64_t rows) {
  (void)rows;
  return (i % 3U) == 0U;
}

static int match_in_tags(uint64_t i, uint64_t rows) {
  (void)rows;
  return (i % 3U) == 0U || (i % 5U) == 0U;
}

static int match_exists_flag(uint64_t i, uint64_t rows) {
  (void)rows;
  return (i % 5U) == 0U;
}

static int match_prefix_owner(uint64_t i, uint64_t rows) {
  (void)rows;
  return (i % 10U) == 0U;
}

static int match_contains_message(uint64_t i, uint64_t rows) {
  (void)rows;
  return (i % 8U) == 0U;
}

static int match_and_even_range(uint64_t i, uint64_t rows) {
  return match_eq_dense(i, rows) && match_range_half(i, rows);
}

static int match_or_sparse_or_flag(uint64_t i, uint64_t rows) {
  return match_eq_sparse(i, rows) || match_exists_flag(i, rows);
}

static const query_scenario *find_scenario(const char *name) {
  static const query_scenario scenarios[] = {
      {"EqSparse"},     {"EqDense"},        {"RangeHalf"},
      {"InRegion"},     {"InRegionSingle"}, {"InTags"},
      {"ExistsFlag"},   {"PrefixOwner"},    {"ContainsMessage"},
      {"AndEvenRange"}, {"OrSparseOrFlag"},
  };
  size_t index;

  if (name == NULL || name[0] == '\0') {
    name = "EqSparse";
  }
  for (index = 0U; index < sizeof(scenarios) / sizeof(scenarios[0]); ++index) {
    if (strcmp(name, scenarios[index].name) == 0) {
      return &scenarios[index];
    }
  }
  return NULL;
}

static uint64_t scenario_expected_rows(const query_scenario *scenario,
                                       uint64_t rows) {
  if (scenario == NULL) {
    return 0U;
  }
  if (strcmp(scenario->name, "EqSparse") == 0) {
    return count_matching(rows, match_eq_sparse);
  }
  if (strcmp(scenario->name, "EqDense") == 0) {
    return count_matching(rows, match_eq_dense);
  }
  if (strcmp(scenario->name, "RangeHalf") == 0) {
    return count_matching(rows, match_range_half);
  }
  if (strcmp(scenario->name, "InRegion") == 0) {
    return count_matching(rows, match_in_region);
  }
  if (strcmp(scenario->name, "InRegionSingle") == 0) {
    return count_matching(rows, match_in_region_single);
  }
  if (strcmp(scenario->name, "InTags") == 0) {
    return count_matching(rows, match_in_tags);
  }
  if (strcmp(scenario->name, "ExistsFlag") == 0) {
    return count_matching(rows, match_exists_flag);
  }
  if (strcmp(scenario->name, "PrefixOwner") == 0) {
    return count_matching(rows, match_prefix_owner);
  }
  if (strcmp(scenario->name, "ContainsMessage") == 0) {
    return count_matching(rows, match_contains_message);
  }
  if (strcmp(scenario->name, "AndEvenRange") == 0) {
    return count_matching(rows, match_and_even_range);
  }
  if (strcmp(scenario->name, "OrSparseOrFlag") == 0) {
    return count_matching(rows, match_or_sparse_or_flag);
  }
  return 0U;
}

static int scenario_selector_json(const query_scenario *scenario, uint64_t rows,
                                  char *out, size_t out_len) {
  const char *name;
  uint64_t target;
  int written;

  if (scenario == NULL || out == NULL || out_len == 0U) {
    return 0;
  }
  name = scenario->name;
  target = target_row(rows);
  if (strcmp(name, "EqSparse") == 0) {
    written = snprintf(out, out_len,
                       "{\"eq\":{\"field\":\"/bucket\",\"value\":\"needle\"}}");
  } else if (strcmp(name, "EqDense") == 0) {
    written = snprintf(out, out_len,
                       "{\"eq\":{\"field\":\"/group\",\"value\":\"even\"}}");
  } else if (strcmp(name, "RangeHalf") == 0) {
    written = snprintf(out, out_len,
                       "{\"range\":{\"field\":\"/value\",\"gte\":%llu}}",
                       (unsigned long long)target);
  } else if (strcmp(name, "InRegion") == 0) {
    written =
        snprintf(out, out_len,
                 "{\"in\":{\"field\":\"/region\",\"any\":[\"us\",\"eu\"]}}");
  } else if (strcmp(name, "InRegionSingle") == 0) {
    written = snprintf(out, out_len,
                       "{\"in\":{\"field\":\"/region\",\"any\":[\"us\"]}}");
  } else if (strcmp(name, "InTags") == 0) {
    written = snprintf(out, out_len,
                       "{\"in\":{\"field\":\"/tags[]\","
                       "\"any\":[\"planning\",\"finance\"]}}");
  } else if (strcmp(name, "ExistsFlag") == 0) {
    written = snprintf(out, out_len, "{\"exists\":\"/flag\"}");
  } else if (strcmp(name, "PrefixOwner") == 0) {
    written = snprintf(
        out, out_len,
        "{\"prefix\":{\"field\":\"/owner\",\"value\":\"bench-owner-00\"}}");
  } else if (strcmp(name, "ContainsMessage") == 0) {
    written = snprintf(out, out_len,
                       "{\"contains\":{\"field\":\"/details/message\","
                       "\"value\":\"timeout\"}}");
  } else if (strcmp(name, "AndEvenRange") == 0) {
    written = snprintf(out, out_len,
                       "{\"and\":[{\"eq\":{\"field\":\"/group\","
                       "\"value\":\"even\"}},{\"range\":{\"field\":"
                       "\"/value\",\"gte\":%llu}}]}",
                       (unsigned long long)target);
  } else if (strcmp(name, "OrSparseOrFlag") == 0) {
    written = snprintf(out, out_len,
                       "{\"or\":[{\"eq\":{\"field\":\"/bucket\","
                       "\"value\":\"needle\"}},{\"exists\":\"/flag\"}]}");
  } else {
    return 0;
  }
  return written > 0 && (size_t)written < out_len;
}

static lc_source *source_from_text(const char *text, lc_error *error) {
  lc_source *source;

  source = NULL;
  if (lc_source_from_memory(text, strlen(text), &source, error) != LC_OK) {
    return NULL;
  }
  return source;
}

static int seed_field_rows(lc_client *client, uint64_t rows, lc_error *error) {
  char key[96];
  char owner[32];
  char json[512];
  char flag_json[32];
  lc_acquire_req acquire;
  lc_update_opts update_opts;
  lc_lease *lease;
  lc_source *source;
  uint64_t target;
  uint64_t i;
  int rc;

  if (client == NULL) {
    return LC_ERR_INVALID;
  }
  target = rows > 1U ? rows / 2U : 0U;
  lc_acquire_req_init(&acquire);
  lc_update_opts_init(&update_opts);
  acquire.ttl_seconds = 3600L;
  update_opts.content_type = "application/json";

  for (i = 0U; i < rows; ++i) {
    (void)snprintf(key, sizeof(key), "bench/query/%08llu",
                   (unsigned long long)i);
    (void)snprintf(owner, sizeof(owner), "bench-owner-%02llu",
                   (unsigned long long)(i % 10U));
    if ((i % 5U) == 0U) {
      (void)snprintf(flag_json, sizeof(flag_json), ",\"flag\":true");
    } else {
      flag_json[0] = '\0';
    }
    (void)snprintf(
        json, sizeof(json),
        "{\"bucket\":\"%s\",\"group\":\"%s\",\"region\":\"%s\","
        "\"owner\":\"%s\",\"value\":%llu,\"tags\":[\"%s\",\"%s\"],"
        "\"details\":{\"message\":\"%s event %llu\"}%s}",
        i == target ? "needle" : "haystack", (i % 2U) == 0U ? "even" : "odd",
        (i % 3U) == 0U ? "us" : ((i % 3U) == 1U ? "eu" : "apac"), owner,
        (unsigned long long)i, (i % 3U) == 0U ? "planning" : "ops",
        (i % 5U) == 0U ? "finance" : "runtime",
        (i % 8U) == 0U ? "timeout" : "normal", (unsigned long long)i,
        flag_json);
    lease = NULL;
    source = NULL;
    acquire.key = key;
    acquire.owner = owner;
    rc = client->acquire(client, &acquire, &lease, error);
    if (rc == LC_OK) {
      source = source_from_text(json, error);
      if (source == NULL) {
        rc = LC_ERR_NOMEM;
      }
    }
    if (rc == LC_OK) {
      rc = lease->update(lease, source, &update_opts, error);
    }
    if (source != NULL) {
      lc_source_close(source);
    }
    if (lease != NULL) {
      lease->close(lease);
    }
    if (rc != LC_OK) {
      return rc;
    }
  }

  return LC_OK;
}

static int open_pouch_client(const char *root, lc_client **out,
                             lc_error *error) {
  char endpoint[512];
  lc_client_config config;
  const char *endpoints[1];

  (void)snprintf(endpoint, sizeof(endpoint), "pouch://%s", root);
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "bench";
  return lc_client_open(&config, out, error);
}

static int acquire_update_release(lc_client *client, const char *key,
                                  const char *owner, const char *json,
                                  lc_error *error) {
  lc_acquire_req acquire;
  lc_update_opts update_opts;
  lc_release_req release_req;
  lc_lease *lease;
  lc_source *source;
  int rc;

  lc_acquire_req_init(&acquire);
  lc_update_opts_init(&update_opts);
  lc_release_req_init(&release_req);
  acquire.key = key;
  acquire.owner = owner;
  acquire.ttl_seconds = 3600L;
  acquire.block_seconds = 1L;
  update_opts.content_type = "application/json";
  lease = NULL;
  source = NULL;

  rc = client->acquire(client, &acquire, &lease, error);
  if (rc == LC_OK) {
    source = source_from_text(json, error);
    if (source == NULL) {
      rc = LC_ERR_NOMEM;
    }
  }
  if (rc == LC_OK) {
    rc = lease->update(lease, source, &update_opts, error);
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  if (rc == LC_OK) {
    rc = lease->release(lease, &release_req, error);
    lease = NULL;
  }
  if (lease != NULL) {
    lease->close(lease);
  }
  return rc;
}

static int key_begin(void *context, lc_error *error) {
  (void)context;
  (void)error;
  return 1;
}

static int key_chunk(void *context, const char *bytes, size_t len,
                     lc_error *error) {
  key_count *count;

  (void)bytes;
  (void)error;
  count = (key_count *)context;
  count->bytes += (uint64_t)len;
  return 1;
}

static int key_end(void *context, lc_error *error) {
  key_count *count;

  (void)error;
  count = (key_count *)context;
  count->documents += 1U;
  return 1;
}

static int run_lql_scenario(const char *root, const char *scenario_name,
                            const char *engine, uint64_t iterations,
                            uint64_t seeded_documents, int keys_only,
                            lockdc_pouch_bench_result *out) {
  char selector[512];
  char endpoint[512];
  lc_client_config config;
  const char *endpoints[1];
  const query_scenario *scenario;
  lc_client *client;
  lc_query_req req;
  lc_query_res res;
  lc_query_key_handler handler;
  key_count keys;
  lc_sink *sink;
  lc_error error;
  uint64_t start;
  uint64_t end;
  uint64_t i;
  uint64_t expected_documents;
  int rc;

  if (out == NULL) {
    return LC_ERR_INVALID;
  }
  memset(out, 0, sizeof(*out));
  out->iterations = iterations;
  out->operations = iterations;
  out->documents = seeded_documents;
  lc_error_init(&error);

  if (root == NULL || root[0] == '\0') {
    set_error(out, "root validation", &error, LC_ERR_INVALID);
    lc_error_cleanup(&error);
    return LC_ERR_INVALID;
  }
  if (seeded_documents == 0U) {
    seeded_documents = 1U;
  }
  if (iterations == 0U) {
    iterations = 1U;
  }
  scenario = find_scenario(scenario_name);
  if (scenario == NULL || !scenario_selector_json(scenario, seeded_documents,
                                                  selector, sizeof(selector))) {
    set_error(out, "scenario validation", &error, LC_ERR_INVALID);
    lc_error_cleanup(&error);
    return LC_ERR_INVALID;
  }
  expected_documents = scenario_expected_rows(scenario, seeded_documents);
  out->iterations = iterations;
  out->operations = iterations;
  out->documents = seeded_documents;
  if (engine == NULL || engine[0] == '\0') {
    engine = "index";
  }
  if (strcmp(engine, "index") != 0 && strcmp(engine, "scan") != 0) {
    set_error(out, "engine validation", &error, LC_ERR_INVALID);
    lc_error_cleanup(&error);
    return LC_ERR_INVALID;
  }
  (void)snprintf(endpoint, sizeof(endpoint), "pouch://%s", root);
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "bench";
  client = NULL;
  rc = lc_client_open(&config, &client, &error);
  if (rc != LC_OK) {
    set_error(out, "open indexed client", &error, rc);
    lc_error_cleanup(&error);
    return rc;
  }
  rc = seed_field_rows(client, seeded_documents, &error);
  if (rc != LC_OK) {
    set_error(out, "seed", &error, rc);
    client->close(client);
    lc_error_cleanup(&error);
    return rc;
  }

  start = now_ns();
  for (i = 0U; i < iterations; ++i) {
    char *cursor;
    uint64_t page_count;

    cursor = NULL;
    page_count = 0U;
    memset(&keys, 0, sizeof(keys));
    do {
      char *next_cursor;
      uint64_t page_start;
      uint64_t page_end;
      uint64_t query_candidates;

      lc_query_req_init(&req);
      memset(&res, 0, sizeof(res));
      req.selector_json = selector;
      req.engine = engine;
      req.limit = (long)BENCHMARK_QUERY_PAGE_LIMIT;
      req.cursor = cursor;
      if (keys_only) {
        key_count page_keys;

        memset(&handler, 0, sizeof(handler));
        memset(&page_keys, 0, sizeof(page_keys));
        handler.begin = key_begin;
        handler.chunk = key_chunk;
        handler.end = key_end;
        page_start = now_ns();
        rc = client->query_keys(client, &req, &handler, &page_keys, &res,
                                &error);
        page_end = now_ns();
        keys.documents += page_keys.documents;
        keys.bytes += page_keys.bytes;
        out->bytes += page_keys.bytes;
      } else {
        sink = NULL;
        rc = lc_sink_to_file("/dev/null", &sink, &error);
        if (rc == LC_OK) {
          page_start = now_ns();
          rc = client->query(client, &req, sink, &res, &error);
          page_end = now_ns();
          lc_sink_close(sink);
        } else {
          page_start = 0U;
          page_end = 0U;
        }
      }
      if (page_end >= page_start) {
        if (page_count == 0U) {
          out->first_page_elapsed_ns += page_end - page_start;
          out->first_page_count++;
        } else {
          out->next_page_elapsed_ns += page_end - page_start;
          out->next_page_count++;
        }
      }
      if (strcmp(engine, "index") == 0 && rc == LC_OK && res.index_seq == 0UL) {
        rc = LC_ERR_PROTOCOL;
      }
      if (res.index_seq > out->index_seq) {
        out->index_seq = res.index_seq;
      }
      if (rc == LC_OK &&
          parse_query_candidates(res.metadata_json, &query_candidates)) {
        out->query_candidates += query_candidates;
        out->query_candidate_pages++;
      }
      next_cursor = NULL;
      if (rc == LC_OK && res.cursor != NULL && res.cursor[0] != '\0') {
        next_cursor = bench_strdup(res.cursor);
        if (next_cursor == NULL) {
          (void)snprintf(out->error, sizeof(out->error),
                         "failed to copy query pagination cursor");
          rc = LC_ERR_NOMEM;
        }
      }
      lc_query_res_cleanup(&res);
      free(cursor);
      cursor = next_cursor;
      page_count++;
      out->pages++;
      if (rc == LC_OK && cursor != NULL &&
          page_count > (seeded_documents / BENCHMARK_QUERY_PAGE_LIMIT) + 2U) {
        (void)snprintf(out->error, sizeof(out->error),
                       "query pagination exceeded expected page count");
        rc = LC_ERR_PROTOCOL;
      }
    } while (rc == LC_OK && cursor != NULL);
    free(cursor);
    if (keys_only && rc == LC_OK && keys.documents != expected_documents) {
      (void)snprintf(out->error, sizeof(out->error),
                     "query keys matched %llu documents, expected %llu",
                     (unsigned long long)keys.documents,
                     (unsigned long long)expected_documents);
      rc = LC_ERR_PROTOCOL;
    }
    if (rc != LC_OK) {
      if (out->error[0] == '\0') {
        set_error(out, keys_only ? "query keys" : "query documents", &error,
                  rc);
      }
      client->close(client);
      lc_error_cleanup(&error);
      return rc;
    }
  }
  end = now_ns();

  out->c_elapsed_ns = end >= start ? end - start : 0U;
  capture_result_cache_stats(client, out);
  client->close(client);
  lc_error_cleanup(&error);
  return LC_OK;
}

int lockdc_pouch_bench_indexed_lql_documents(const char *root,
                                             uint64_t iterations,
                                             uint64_t seeded_documents,
                                             lockdc_pouch_bench_result *out) {
  return run_lql_scenario(root, "EqSparse", "index", iterations,
                          seeded_documents, 0, out);
}

int lockdc_pouch_bench_indexed_lql_keys(const char *root, uint64_t iterations,
                                        uint64_t seeded_documents,
                                        lockdc_pouch_bench_result *out) {
  return run_lql_scenario(root, "EqSparse", "index", iterations,
                          seeded_documents, 1, out);
}

int lockdc_pouch_bench_indexed_lql_scenario_documents(
    const char *root, const char *scenario, uint64_t iterations,
    uint64_t seeded_documents, lockdc_pouch_bench_result *out) {
  return run_lql_scenario(root, scenario, "index", iterations, seeded_documents,
                          0, out);
}

int lockdc_pouch_bench_indexed_lql_scenario_keys(
    const char *root, const char *scenario, uint64_t iterations,
    uint64_t seeded_documents, lockdc_pouch_bench_result *out) {
  return run_lql_scenario(root, scenario, "index", iterations, seeded_documents,
                          1, out);
}

int lockdc_pouch_bench_lql_scenario_documents(const char *root,
                                              const char *scenario,
                                              const char *engine,
                                              uint64_t iterations,
                                              uint64_t seeded_documents,
                                              lockdc_pouch_bench_result *out) {
  return run_lql_scenario(root, scenario, engine, iterations, seeded_documents,
                          0, out);
}

int lockdc_pouch_bench_lql_scenario_keys(const char *root, const char *scenario,
                                         const char *engine,
                                         uint64_t iterations,
                                         uint64_t seeded_documents,
                                         lockdc_pouch_bench_result *out) {
  return run_lql_scenario(root, scenario, engine, iterations, seeded_documents,
                          1, out);
}

int lockdc_pouch_bench_state_write(const char *root, uint64_t iterations,
                                   lockdc_pouch_bench_result *out) {
  char key[96];
  char owner[32];
  char json[128];
  lc_client *client;
  lc_error error;
  uint64_t start;
  uint64_t end;
  uint64_t i;
  int rc;

  if (out == NULL) {
    return LC_ERR_INVALID;
  }
  memset(out, 0, sizeof(*out));
  if (iterations == 0U) {
    iterations = 1U;
  }
  out->iterations = iterations;
  out->operations = iterations;
  lc_error_init(&error);
  client = NULL;
  rc = open_pouch_client(root, &client, &error);
  if (rc != LC_OK) {
    set_error(out, "open pouch client", &error, rc);
    lc_error_cleanup(&error);
    return rc;
  }

  start = now_ns();
  for (i = 0U; i < iterations; ++i) {
    (void)snprintf(key, sizeof(key), "bench/write/%08llu",
                   (unsigned long long)i);
    (void)snprintf(owner, sizeof(owner), "bench-writer-%02llu",
                   (unsigned long long)(i % 10U));
    (void)snprintf(json, sizeof(json), "{\"bucket\":\"write\",\"value\":%llu}",
                   (unsigned long long)i);
    rc = acquire_update_release(client, key, owner, json, &error);
    if (rc != LC_OK) {
      set_error(out, "write state", &error, rc);
      client->close(client);
      lc_error_cleanup(&error);
      return rc;
    }
    out->bytes += (uint64_t)strlen(json);
  }
  end = now_ns();

  out->c_elapsed_ns = end >= start ? end - start : 0U;
  client->close(client);
  lc_error_cleanup(&error);
  return LC_OK;
}

int lockdc_pouch_bench_state_read(const char *root, uint64_t iterations,
                                  lockdc_pouch_bench_result *out) {
  static const char key[] = "bench-read-hot";
  static const char json[] = "{\"bucket\":\"read\",\"value\":1}";
  lc_client *client;
  lc_get_opts get_opts;
  lc_get_res get_res;
  lc_sink *sink;
  lc_error error;
  uint64_t start;
  uint64_t end;
  uint64_t i;
  int rc;

  if (out == NULL) {
    return LC_ERR_INVALID;
  }
  memset(out, 0, sizeof(*out));
  if (iterations == 0U) {
    iterations = 1U;
  }
  out->iterations = iterations;
  out->operations = iterations;
  lc_error_init(&error);
  client = NULL;
  rc = open_pouch_client(root, &client, &error);
  if (rc != LC_OK) {
    set_error(out, "open pouch client", &error, rc);
    lc_error_cleanup(&error);
    return rc;
  }
  rc = acquire_update_release(client, key, "bench-reader", json, &error);
  if (rc != LC_OK) {
    set_error(out, "seed read state", &error, rc);
    client->close(client);
    lc_error_cleanup(&error);
    return rc;
  }

  start = now_ns();
  for (i = 0U; i < iterations; ++i) {
    lc_get_opts_init(&get_opts);
    memset(&get_res, 0, sizeof(get_res));
    sink = NULL;
    rc = lc_sink_to_file("/dev/null", &sink, &error);
    if (rc == LC_OK) {
      rc = client->get(client, key, &get_opts, sink, &get_res, &error);
      lc_sink_close(sink);
    }
    if (rc != LC_OK) {
      set_error(out, "read state", &error, rc);
      client->close(client);
      lc_error_cleanup(&error);
      return rc;
    }
    out->bytes += (uint64_t)strlen(json);
    lc_get_res_cleanup(&get_res);
  }
  end = now_ns();

  out->c_elapsed_ns = end >= start ? end - start : 0U;
  client->close(client);
  lc_error_cleanup(&error);
  return LC_OK;
}

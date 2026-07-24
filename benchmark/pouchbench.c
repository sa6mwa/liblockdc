#include "pouchbench.h"

#include "lc/lc.h"

#include <stdio.h>
#include <string.h>
#include <time.h>

typedef struct key_count {
  uint64_t rows;
  uint64_t bytes;
} key_count;

static uint64_t now_ns(void) {
  struct timespec ts;

  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
    return 0U;
  }
  return ((uint64_t)ts.tv_sec * 1000000000ULL) + (uint64_t)ts.tv_nsec;
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
  char json[128];
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
    (void)snprintf(json, sizeof(json), "{\"bucket\":\"%s\",\"value\":%llu}",
                   i == target ? "needle" : "haystack", (unsigned long long)i);
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
  count->rows += 1U;
  return 1;
}

static int run_indexed_lql(const char *root, uint64_t iterations,
                           uint64_t seeded_rows, int keys_only,
                           lockdc_pouch_bench_result *out) {
  static const char selector[] =
      "{\"eq\":{\"field\":\"/bucket\",\"value\":\"needle\"}}";
  char endpoint[512];
  lc_client_config config;
  const char *endpoints[1];
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
  int rc;

  if (out == NULL) {
    return LC_ERR_INVALID;
  }
  memset(out, 0, sizeof(*out));
  out->iterations = iterations;
  out->operations = iterations;
  out->rows = seeded_rows;
  lc_error_init(&error);

  if (root == NULL || root[0] == '\0') {
    set_error(out, "root validation", &error, LC_ERR_INVALID);
    lc_error_cleanup(&error);
    return LC_ERR_INVALID;
  }
  if (seeded_rows == 0U) {
    seeded_rows = 1U;
  }
  if (iterations == 0U) {
    iterations = 1U;
  }
  out->iterations = iterations;
  out->operations = iterations;
  out->rows = seeded_rows;
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
  rc = seed_field_rows(client, seeded_rows, &error);
  if (rc != LC_OK) {
    set_error(out, "seed", &error, rc);
    client->close(client);
    lc_error_cleanup(&error);
    return rc;
  }

  start = now_ns();
  for (i = 0U; i < iterations; ++i) {
    lc_query_req_init(&req);
    memset(&res, 0, sizeof(res));
    req.selector_json = selector;
    req.limit = (long)seeded_rows;
    if (keys_only) {
      memset(&handler, 0, sizeof(handler));
      memset(&keys, 0, sizeof(keys));
      handler.begin = key_begin;
      handler.chunk = key_chunk;
      handler.end = key_end;
      rc = client->query_keys(client, &req, &handler, &keys, &res, &error);
      out->bytes += keys.bytes;
      if (rc == LC_OK && keys.rows != 1U) {
        rc = LC_ERR_PROTOCOL;
      }
    } else {
      sink = NULL;
      rc = lc_sink_to_file("/dev/null", &sink, &error);
      if (rc == LC_OK) {
        rc = client->query(client, &req, sink, &res, &error);
        lc_sink_close(sink);
      }
    }
    if (rc == LC_OK && res.index_seq == 0UL) {
      rc = LC_ERR_PROTOCOL;
    }
    if (res.index_seq > out->index_seq) {
      out->index_seq = res.index_seq;
    }
    lc_query_res_cleanup(&res);
    if (rc != LC_OK) {
      set_error(out, keys_only ? "query keys" : "query rows", &error, rc);
      client->close(client);
      lc_error_cleanup(&error);
      return rc;
    }
  }
  end = now_ns();

  out->c_elapsed_ns = end >= start ? end - start : 0U;
  client->close(client);
  lc_error_cleanup(&error);
  return LC_OK;
}

int lockdc_pouch_bench_indexed_lql_rows(const char *root, uint64_t iterations,
                                        uint64_t seeded_rows,
                                        lockdc_pouch_bench_result *out) {
  return run_indexed_lql(root, iterations, seeded_rows, 0, out);
}

int lockdc_pouch_bench_indexed_lql_keys(const char *root, uint64_t iterations,
                                        uint64_t seeded_rows,
                                        lockdc_pouch_bench_result *out) {
  return run_indexed_lql(root, iterations, seeded_rows, 1, out);
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

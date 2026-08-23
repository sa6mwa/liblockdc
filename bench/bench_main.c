#include "../tests/support/lc_test_tmp.h"
#include "lc/lc.h"
#include "lc_pouch.h"

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

typedef struct bench_pouch_perf_fixture {
  lc_client *client;
  char *crypto_key;
  char root[512];
} bench_pouch_perf_fixture;

typedef struct bench_workflow_fixture {
  lc_client *client;
  char root[512];
  char namespace_name[128];
  char key_prefix[96];
  int is_pouch;
} bench_workflow_fixture;

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

static int bench_env_enabled(const char *name) {
  const char *value;

  value = getenv(name);
  return value != NULL && value[0] != '\0' && strcmp(value, "0") != 0 &&
         strcmp(value, "false") != 0 && strcmp(value, "FALSE") != 0;
}

static long bench_env_long(const char *name, long fallback, long minimum) {
  const char *value;
  char *end;
  long parsed;

  value = getenv(name);
  if (value == NULL || value[0] == '\0') {
    return fallback;
  }
  end = NULL;
  parsed = strtol(value, &end, 10);
  if (end == value || *end != '\0' || parsed < minimum) {
    return fallback;
  }
  return parsed;
}

static int bench_pouch_client_open_crypto(const char *root,
                                          const char *crypto_key,
                                          lc_client **out, lc_error *error) {
  lc_client_config config;
  const char *endpoints[1];
  char endpoint[640];

  snprintf(endpoint, sizeof(endpoint), "pouch://%s", root);
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "bench";
  config.pouch_crypto_key = crypto_key;
  return lc_client_open(&config, out, error);
}

static int bench_pouch_perf_fixture_open(bench_pouch_perf_fixture *fixture,
                                         lc_error *error) {
  int rc;

  if (fixture == NULL) {
    return 1;
  }
  memset(fixture, 0, sizeof(*fixture));
  if (bench_pouch_root_path(fixture->root, sizeof(fixture->root), "perf") !=
      0) {
    return 1;
  }
  if (bench_env_enabled("LOCKDC_POUCH_PERF_CRYPTO")) {
    rc = lc_pouch_crypto_generate_key_string(&fixture->crypto_key, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  return bench_pouch_client_open_crypto(fixture->root, fixture->crypto_key,
                                        &fixture->client, error);
}

static void bench_pouch_perf_fixture_close(bench_pouch_perf_fixture *fixture) {
  if (fixture == NULL) {
    return;
  }
  if (fixture->client != NULL) {
    fixture->client->close(fixture->client);
  }
  lc_pouch_crypto_key_string_free(fixture->crypto_key);
  bench_pouch_cleanup_root(fixture->root);
  memset(fixture, 0, sizeof(*fixture));
}

static int bench_workflow_remote_client_open(const char *endpoint,
                                             const char *failover_endpoint,
                                             const char *bundle_path,
                                             const char *namespace_name,
                                             lc_client **out, lc_error *error) {
  const char *endpoints[2];
  lc_client_config config;
  lc_source *bundle_source;
  int rc;

  bundle_source = NULL;
  rc = lc_source_from_file(bundle_path, &bundle_source, error);
  if (rc != LC_OK) {
    return rc;
  }
  endpoints[0] = endpoint;
  endpoints[1] = failover_endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = failover_endpoint == NULL ? 1U : 2U;
  config.default_namespace = namespace_name;
  config.timeout_ms = 30000L;
  config.client_bundle_source = bundle_source;
  config.insecure_skip_verify = 1;
  rc = lc_client_open(&config, out, error);
  lc_source_close(bundle_source);
  return rc;
}

static int bench_workflow_remote_probe(lc_client *client,
                                       const char *namespace_name,
                                       lc_error *error) {
  lc_index_flush_req request;
  lc_index_flush_res result;
  int rc;

  lc_index_flush_req_init(&request);
  memset(&result, 0, sizeof(result));
  request.namespace_name = namespace_name;
  request.mode = "wait";
  rc = lc_flush_index(client, &request, &result, error);
  lc_index_flush_res_cleanup(&result);
  return rc;
}

static int bench_workflow_fixture_open(bench_workflow_fixture *fixture,
                                       lc_error *error) {
  const char *endpoint;
  const char *failover_endpoint;
  const char *bundle_path;
  const char *namespace_name;
  const char *endpoints[1];
  char root_path[sizeof(fixture->root)];
  lc_client_config config;
  struct timespec now;
  const char *selected_endpoint;
  const char *selected_failover_endpoint;
  int attempt;
  int rc;

  if (fixture == NULL) {
    return 1;
  }
  memset(fixture, 0, sizeof(*fixture));
  endpoints[0] = NULL;
  if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
    return 1;
  }
  if (snprintf(fixture->key_prefix, sizeof(fixture->key_prefix),
               "__lockdc_io/v1/outbox/bench-%ld-%ld", (long)getpid(),
               (long)now.tv_nsec) < 0) {
    return 1;
  }
  endpoint = getenv("LOCKDC_WORKFLOW_BENCH_ENDPOINT");
  failover_endpoint = getenv("LOCKDC_WORKFLOW_BENCH_FAILOVER_ENDPOINT");
  namespace_name = getenv("LOCKDC_WORKFLOW_BENCH_NAMESPACE");
  lc_client_config_init(&config);
  config.timeout_ms = 30000L;
  if (endpoint == NULL || endpoint[0] == '\0') {
    if (snprintf(fixture->namespace_name, sizeof(fixture->namespace_name),
                 "workflow-bench-%ld-%ld", (long)getpid(),
                 (long)now.tv_nsec) < 0) {
      return 1;
    }
    if (bench_pouch_root_path(fixture->root, sizeof(fixture->root),
                              "workflow") != 0) {
      return 1;
    }
    memcpy(root_path, fixture->root, sizeof(root_path));
    if (snprintf(fixture->root, sizeof(fixture->root), "pouch://%s",
                 root_path) < 0) {
      return 1;
    }
    endpoints[0] = fixture->root;
    fixture->is_pouch = 1;
  } else {
    if (namespace_name == NULL || namespace_name[0] == '\0') {
      namespace_name = "default";
    }
    if (snprintf(fixture->namespace_name, sizeof(fixture->namespace_name), "%s",
                 namespace_name) < 0) {
      return 1;
    }
    bundle_path = getenv("LOCKDC_WORKFLOW_BENCH_CLIENT_BUNDLE");
    if (bundle_path == NULL || bundle_path[0] == '\0') {
      bundle_path = "./devenv/volumes/lockd-disk-a-config/client.pem";
    }
    selected_endpoint = NULL;
    selected_failover_endpoint = NULL;
    for (attempt = 0; attempt < 30; ++attempt) {
      rc = bench_workflow_remote_client_open(endpoint, NULL, bundle_path,
                                             fixture->namespace_name,
                                             &fixture->client, error);
      if (rc == LC_OK) {
        rc = bench_workflow_remote_probe(fixture->client,
                                         fixture->namespace_name, error);
        if (rc == LC_OK) {
          selected_endpoint = endpoint;
          selected_failover_endpoint = failover_endpoint;
          fixture->client->close(fixture->client);
          fixture->client = NULL;
          break;
        }
        fixture->client->close(fixture->client);
        fixture->client = NULL;
      }
      if (failover_endpoint != NULL && failover_endpoint[0] != '\0') {
        lc_error_cleanup(error);
        lc_error_init(error);
        rc = bench_workflow_remote_client_open(
            failover_endpoint, NULL, bundle_path, fixture->namespace_name,
            &fixture->client, error);
        if (rc == LC_OK) {
          rc = bench_workflow_remote_probe(fixture->client,
                                           fixture->namespace_name, error);
          if (rc == LC_OK) {
            selected_endpoint = failover_endpoint;
            selected_failover_endpoint = endpoint;
            fixture->client->close(fixture->client);
            fixture->client = NULL;
            break;
          }
          fixture->client->close(fixture->client);
          fixture->client = NULL;
        }
      }
      if (attempt + 1 < 30) {
        lc_error_cleanup(error);
        lc_error_init(error);
        sleep(1U);
      }
    }
    if (selected_endpoint == NULL) {
      return rc;
    }
    return bench_workflow_remote_client_open(
        selected_endpoint, selected_failover_endpoint, bundle_path,
        fixture->namespace_name, &fixture->client, error);
  }
  config.default_namespace = fixture->namespace_name;
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  return lc_client_open(&config, &fixture->client, error);
}

static void bench_workflow_fixture_close(bench_workflow_fixture *fixture) {
  if (fixture == NULL) {
    return;
  }
  if (fixture->client != NULL) {
    fixture->client->close(fixture->client);
  }
  if (fixture->is_pouch) {
    const char *root = fixture->root + strlen("pouch://");

    bench_pouch_cleanup_root(root);
  }
  memset(fixture, 0, sizeof(*fixture));
}

static int bench_workflow_seed_record(lc_client *client,
                                      const char *namespace_name,
                                      const char *key,
                                      const char *dispatch_state,
                                      long generation, const char *payload,
                                      size_t payload_length, lc_error *error) {
  char state[512];
  lc_acquire_req acquire;
  lc_attach_req attach;
  lc_attach_res attach_result;
  lc_lease *lease;
  lc_source *state_source;
  lc_source *payload_source;
  int rc;

  if (snprintf(state, sizeof(state),
               "{\"record_type\":\"lockdc.outbox.v1\","
               "\"operation_id\":\"workflow-bench-%s-%ld\","
               "\"effect_id\":\"workflow-bench-effect-%s-%ld\","
               "\"effect_key\":\"workflow-bench-key-%s-%ld\","
               "\"kind\":\"benchmark\","
               "\"destination\":\"benchmark://reconcile\","
               "\"content_type\":\"application/octet-stream\","
               "\"dispatch_state\":\"%s\",\"attempt_count\":0,"
               "\"not_before_unix\":0,\"benchmark_generation\":%ld}",
               dispatch_state, generation, dispatch_state, generation,
               dispatch_state, generation, dispatch_state, generation) < 0) {
    return 1;
  }
  lease = NULL;
  state_source = NULL;
  payload_source = NULL;
  memset(&attach_result, 0, sizeof(attach_result));
  lc_acquire_req_init(&acquire);
  acquire.namespace_name = namespace_name;
  acquire.key = key;
  acquire.owner = "workflow-benchmark-seed";
  acquire.ttl_seconds = 60L;
  rc = client->acquire(client, &acquire, &lease, error);
  if (rc == LC_OK) {
    rc = lc_source_from_memory(state, strlen(state), &state_source, error);
  }
  if (rc == LC_OK) {
    rc = lease->update(lease, state_source, NULL, error);
  }
  if (rc == LC_OK && payload != NULL && payload_length > 0U) {
    rc = lc_source_from_memory(payload, payload_length, &payload_source, error);
  }
  if (rc == LC_OK && payload_source != NULL) {
    lc_attach_req_init(&attach);
    attach.name = "payload";
    attach.content_type = "application/octet-stream";
    attach.prevent_overwrite = 1;
    rc = lease->attach(lease, &attach, payload_source, &attach_result, error);
  }
  if (rc == LC_OK) {
    rc = lease->release(lease, NULL, error);
    if (rc == LC_OK) {
      lease = NULL;
    }
  }
  lc_attach_res_cleanup(&attach_result);
  if (payload_source != NULL) {
    lc_source_close(payload_source);
  }
  if (state_source != NULL) {
    lc_source_close(state_source);
  }
  if (lease != NULL) {
    lease->close(lease);
  }
  return rc;
}

static int bench_workflow_reconcile(long iterations) {
  bench_workflow_fixture fixture;
  lc_workflow_config config;
  lc_workflow *workflow;
  lc_workflow_stats stats;
  lc_outbox_job *job;
  lc_error error;
  struct timespec started;
  double first_delivery_seconds;
  double drain_seconds;
  long rows;
  long terminal_rows;
  long churn_updates;
  long payload_bytes;
  long page_capacity;
  uint64_t expected_recovery_queries;
  char *payload;
  size_t index;
  int rc;

  rows = iterations > 0L ? iterations : 256L;
  terminal_rows =
      bench_env_long("LOCKDC_WORKFLOW_BENCH_TERMINAL_ROWS", rows * 4L, 0L);
  churn_updates = bench_env_long("LOCKDC_WORKFLOW_BENCH_CHURN_UPDATES", 4L, 0L);
  payload_bytes =
      bench_env_long("LOCKDC_WORKFLOW_BENCH_PAYLOAD_BYTES", 4096L, 0L);
  page_capacity =
      bench_env_long("LOCKDC_WORKFLOW_BENCH_PAGE_CAPACITY", 16L, 1L);
  expected_recovery_queries =
      ((uint64_t)rows + (uint64_t)page_capacity - 1U) / (uint64_t)page_capacity;
  payload = NULL;
  workflow = NULL;
  memset(&fixture, 0, sizeof(fixture));
  memset(&stats, 0, sizeof(stats));
  lc_error_init(&error);
  rc = bench_workflow_fixture_open(&fixture, &error);
  if (rc != LC_OK) {
    goto done;
  }
  if (payload_bytes > 0L) {
    payload = (char *)malloc((size_t)payload_bytes);
    if (payload == NULL) {
      rc = 1;
      goto done;
    }
    memset(payload, 'p', (size_t)payload_bytes);
  }
  for (index = 0U; rc == LC_OK && index < (size_t)terminal_rows; ++index) {
    char key[128];

    if (snprintf(key, sizeof(key), "%s/terminal-%08lu", fixture.key_prefix,
                 (unsigned long)index) < 0) {
      rc = 1;
      break;
    }
    rc = bench_workflow_seed_record(fixture.client, fixture.namespace_name, key,
                                    "completed", 0L, NULL, 0U, &error);
  }
  for (index = 0U; rc == LC_OK && index < (size_t)churn_updates; ++index) {
    size_t terminal_index;

    for (terminal_index = 0U; terminal_index < (size_t)terminal_rows;
         ++terminal_index) {
      char key[128];

      if (snprintf(key, sizeof(key), "%s/terminal-%08lu", fixture.key_prefix,
                   (unsigned long)terminal_index) < 0) {
        rc = 1;
        break;
      }
      rc = bench_workflow_seed_record(fixture.client, fixture.namespace_name,
                                      key, "completed", (long)index + 1L, NULL,
                                      0U, &error);
      if (rc != LC_OK) {
        break;
      }
    }
  }
  for (index = 0U; rc == LC_OK && index < (size_t)rows; ++index) {
    char key[128];

    if (snprintf(key, sizeof(key), "%s/pending-%08lu", fixture.key_prefix,
                 (unsigned long)index) < 0) {
      rc = 1;
      break;
    }
    rc = bench_workflow_seed_record(fixture.client, fixture.namespace_name, key,
                                    "pending", 0L, payload,
                                    (size_t)payload_bytes, &error);
  }
  if (rc != LC_OK) {
    goto done;
  }
  lc_workflow_config_init(&config);
  config.namespace_name = fixture.namespace_name;
  config.owner = "workflow-benchmark-dispatcher";
  config.notification_capacity = (size_t)page_capacity;
  config.recovery_interval_seconds = 0L;
  if (clock_gettime(CLOCK_MONOTONIC, &started) != 0) {
    rc = 1;
    goto done;
  }
  rc = lc_client_new_workflow(fixture.client, &config, &workflow, &error);
  first_delivery_seconds = 0.0;
  for (index = 0U; rc == LC_OK && index < (size_t)rows; ++index) {
    job = NULL;
    rc = workflow->next(workflow, 30000L, &job, &error);
    if (rc != LC_OK || job == NULL) {
      if (rc == LC_OK) {
        rc = 1;
      }
      break;
    }
    if (job->outbox_key == NULL || strncmp(job->outbox_key, fixture.key_prefix,
                                           strlen(fixture.key_prefix)) != 0) {
      job->close(job);
      rc = 1;
      break;
    }
    if (index == 0U) {
      first_delivery_seconds =
          bench_now_seconds() -
          ((double)started.tv_sec + (double)started.tv_nsec / 1000000000.0);
    }
    rc = job->complete(job, &error);
    job->close(job);
  }
  drain_seconds =
      bench_now_seconds() -
      ((double)started.tv_sec + (double)started.tv_nsec / 1000000000.0);
  if (rc == LC_OK) {
    rc = workflow->get_stats(workflow, &stats, &error);
  }
  if (rc == LC_OK && (stats.recovered_claims < (uint64_t)rows ||
                      stats.recovery_queries < expected_recovery_queries)) {
    rc = 1;
  }
  printf("metric=workflow-reconcile backend=%s pending_rows=%ld "
         "terminal_rows=%ld churn_updates=%ld payload_bytes=%ld "
         "page_capacity=%ld first_delivery_ms=%.3f drain_ms=%.3f "
         "throughput_rows_per_second=%.3f recovery_queries=%.0f "
         "minimum_recovery_queries=%.0f recovered_claims=%.0f "
         "candidate_surplus=%.0f rc=%d\n",
         fixture.is_pouch ? "pouch" : "remote", rows, terminal_rows,
         churn_updates, payload_bytes, page_capacity,
         first_delivery_seconds * 1000.0, drain_seconds * 1000.0,
         drain_seconds > 0.0 ? (double)rows / drain_seconds : 0.0,
         (double)stats.recovery_queries, (double)expected_recovery_queries,
         (double)stats.recovered_claims,
         stats.recovered_claims > (uint64_t)rows
             ? (double)(stats.recovered_claims - (uint64_t)rows)
             : 0.0,
         rc);

done:
  if (workflow != NULL) {
    workflow->close(workflow);
  }
  lc_workflow_stats_cleanup(&stats);
  free(payload);
  bench_workflow_fixture_close(&fixture);
  if (rc != LC_OK && error.message != NULL) {
    fprintf(stderr,
            "workflow-reconcile failed: code=%d http=%ld message=%s"
            " detail=%s server_code=%s\n",
            error.code, error.http_status, error.message,
            error.detail == NULL ? "" : error.detail,
            error.server_code == NULL ? "" : error.server_code);
  }
  lc_error_cleanup(&error);
  return rc == LC_OK ? 0 : 1;
}

static char *bench_pouch_perf_document(long row, long generation,
                                       long payload_bytes) {
  const char *summary;
  const char *description;
  const char *stage;
  const char *tier;
  const char *risk;
  size_t capacity;
  size_t used;
  char *json;
  int written;

  if (payload_bytes < 1024L) {
    payload_bytes = 1024L;
  }
  capacity = (size_t)payload_bytes + 2048U;
  json = (char *)malloc(capacity);
  if (json == NULL) {
    return NULL;
  }
  summary = row % 2L == 0L ? "remediation audit workflow timeout risk narrative"
                           : "ordinary audit workflow context narrative";
  description = row % 3L == 0L
                    ? "audit evidence remediation plan with timeout handling"
                    : "audit evidence review plan with normal handling";
  stage = row % 5L == 0L ? "escalated" : "review";
  tier = row % 4L == 0L ? "enterprise" : "standard";
  risk = row % 7L == 0L ? "timeout pressure" : "routine pressure";
  written =
      snprintf(json, capacity,
               "{\"tenant\":{\"id\":\"tenant-%03ld\",\"tier\":\"%s\"},"
               "\"workflow\":{\"id\":\"wf-%03ld\",\"stage\":\"%s\","
               "\"owner\":{\"team\":\"ops\",\"region\":\"%s\"}},"
               "\"metrics\":{\"amount_usd\":%ld,\"risk_score\":%ld},"
               "\"risk\":{\"summary\":\"%s\"},"
               "\"narrative\":{\"summary\":\"%s row %ld generation %ld\","
               "\"description\":\"%s row %ld generation %ld\"},"
               "\"tags\":[\"audit\",\"finance\",\"planning\"],"
               "\"created_at\":\"2026-01-01T00:00:00Z\",\"payload\":\"",
               row % 17L, tier, row % 23L, stage, row % 3L == 0L ? "us" : "eu",
               1000L + (row * 37L), row % 100L, risk, summary, row, generation,
               description, row, generation);
  if (written <= 0 || (size_t)written >= capacity) {
    free(json);
    return NULL;
  }
  used = (size_t)written;
  while (used + 96U < capacity && (long)used < payload_bytes) {
    written = snprintf(json + used, capacity - used,
                       " audit remediation evidence workflow row %ld gen %ld;",
                       row, generation);
    if (written <= 0 || (size_t)written >= capacity - used) {
      free(json);
      return NULL;
    }
    used += (size_t)written;
  }
  if (used + 3U >= capacity) {
    free(json);
    return NULL;
  }
  json[used++] = '"';
  json[used++] = '}';
  json[used] = '\0';
  return json;
}

static int bench_pouch_seed_update(lc_client *client, const char *key,
                                   lc_source *source, lc_error *error) {
  lc_acquire_req acquire_req;
  lc_lease *lease;
  lc_release_req release_req;
  lc_update_opts update_opts;
  int rc;

  if (client == NULL || key == NULL || source == NULL) {
    return 1;
  }
  lease = NULL;
  lc_acquire_req_init(&acquire_req);
  lc_release_req_init(&release_req);
  lc_update_opts_init(&update_opts);
  acquire_req.namespace_name = "bench";
  acquire_req.key = key;
  acquire_req.owner = "lockdc-bench";
  acquire_req.ttl_seconds = 30L;
  update_opts.content_type = "application/json";
  rc = client->acquire(client, &acquire_req, &lease, error);
  if (rc == LC_OK) {
    rc = lease->update(lease, source, &update_opts, error);
  }
  if (lease != NULL) {
    if (rc == LC_OK) {
      rc = lease->release(lease, &release_req, error);
      if (rc == LC_OK) {
        lease = NULL;
      }
    }
    if (lease != NULL) {
      lease->close(lease);
    }
  }
  return rc;
}

static int bench_pouch_seed_perf_docs(lc_client *client, long rows,
                                      long generation, long payload_bytes,
                                      lc_error *error) {
  long row;

  for (row = 0L; row < rows; ++row) {
    lc_source *source;
    char key[64];
    char *json;
    int rc;

    json = bench_pouch_perf_document(row, generation, payload_bytes);
    if (json == NULL) {
      return 1;
    }
    source = NULL;
    rc = lc_source_from_memory(json, strlen(json), &source, error);
    if (rc == LC_OK) {
      snprintf(key, sizeof(key), "doc/%08ld", row);
      rc = bench_pouch_seed_update(client, key, source, error);
    }
    if (source != NULL) {
      lc_source_close(source);
    }
    free(json);
    if (rc != LC_OK) {
      return rc;
    }
  }
  return LC_OK;
}

static int bench_pouch_perf_flush(lc_client *client, lc_error *error) {
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

static int bench_pouch_perf_query(lc_client *client, const char *selector_lql,
                                  const char *engine, int documents, long limit,
                                  lc_error *error) {
  lc_query_req req;
  lc_query_res res;
  int rc;

  lc_query_req_init(&req);
  memset(&res, 0, sizeof(res));
  req.namespace_name = "bench";
  req.selector_lql = selector_lql;
  req.engine = engine;
  req.limit = limit > 0L ? limit : 1L;
  if (documents) {
    lc_sink *sink;

    sink = NULL;
    rc = lc_sink_to_discard(&sink, error);
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

static long bench_pouch_perf_rows(long iterations) {
  return iterations > 0L ? iterations : 128L;
}

static long bench_pouch_perf_payload_bytes(void) {
  const char *value;
  long parsed;

  value = getenv("LOCKDC_POUCH_PERF_PAYLOAD_BYTES");
  if (value == NULL || value[0] == '\0') {
    return 4096L;
  }
  parsed = strtol(value, NULL, 10);
  return parsed > 0L ? parsed : 4096L;
}

static int bench_pouch_perf_prepare(bench_pouch_perf_fixture *fixture,
                                    long rows, long payload_bytes,
                                    lc_error *error) {
  int rc;

  rc = bench_pouch_perf_fixture_open(fixture, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = bench_pouch_seed_perf_docs(fixture->client, rows, 0L, payload_bytes,
                                  error);
  if (rc == LC_OK) {
    rc = bench_pouch_perf_flush(fixture->client, error);
  }
  return rc;
}

static int bench_pouch_perf_query_case(long iterations, const char *name,
                                       const char *selector_lql,
                                       const char *engine, int documents) {
  bench_pouch_perf_fixture fixture;
  lc_error error;
  double start;
  double elapsed;
  long rows;
  int rc;

  lc_error_init(&error);
  rows = bench_pouch_perf_rows(iterations);
  memset(&fixture, 0, sizeof(fixture));
  rc = bench_pouch_perf_prepare(&fixture, rows,
                                bench_pouch_perf_payload_bytes(), &error);
  if (rc == LC_OK) {
    rc = bench_pouch_perf_query(fixture.client, selector_lql, engine, documents,
                                rows, &error);
  }
  start = bench_now_seconds();
  if (rc == LC_OK) {
    rc = bench_pouch_perf_query(fixture.client, selector_lql, engine, documents,
                                rows, &error);
  }
  elapsed = bench_now_seconds() - start;
  printf("metric=%s rows=%ld crypto=%d seconds=%.6f per_row_us=%.3f rc=%d\n",
         name, rows, bench_env_enabled("LOCKDC_POUCH_PERF_CRYPTO"), elapsed,
         rows > 0L ? (elapsed * 1000000.0) / (double)rows : 0.0, rc);
  bench_pouch_perf_fixture_close(&fixture);
  lc_error_cleanup(&error);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_perf_index_docs(long iterations) {
  return bench_pouch_perf_query_case(
      iterations, "pouch-perf-index-docs",
      "icontains{field=/narrative/summary,value=remediation}", "index", 1);
}

static int bench_pouch_perf_full_text_keys(long iterations) {
  return bench_pouch_perf_query_case(iterations, "pouch-perf-full-text-keys",
                                     "icontains{field=/...,value=audit}",
                                     "index", 0);
}

static int bench_pouch_perf_full_text_reopen_keys(long iterations) {
  bench_pouch_perf_fixture fixture;
  lc_error error;
  double start;
  double elapsed;
  long rows;
  int rc;

  lc_error_init(&error);
  rows = bench_pouch_perf_rows(iterations);
  memset(&fixture, 0, sizeof(fixture));
  rc = bench_pouch_perf_prepare(&fixture, rows,
                                bench_pouch_perf_payload_bytes(), &error);
  if (fixture.client != NULL) {
    fixture.client->close(fixture.client);
    fixture.client = NULL;
  }
  if (rc == LC_OK) {
    rc = bench_pouch_client_open_crypto(fixture.root, fixture.crypto_key,
                                        &fixture.client, &error);
  }
  if (rc == LC_OK) {
    rc = bench_pouch_perf_flush(fixture.client, &error);
  }
  start = bench_now_seconds();
  if (rc == LC_OK) {
    rc = bench_pouch_perf_query(fixture.client,
                                "icontains{field=/...,value=audit}", "index", 0,
                                rows, &error);
  }
  elapsed = bench_now_seconds() - start;
  printf("metric=pouch-perf-full-text-reopen-keys rows=%ld crypto=%d "
         "seconds=%.6f per_row_us=%.3f rc=%d\n",
         rows, bench_env_enabled("LOCKDC_POUCH_PERF_CRYPTO"), elapsed,
         rows > 0L ? (elapsed * 1000000.0) / (double)rows : 0.0, rc);
  bench_pouch_perf_fixture_close(&fixture);
  lc_error_cleanup(&error);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_perf_scan_keys(long iterations) {
  return bench_pouch_perf_query_case(
      iterations, "pouch-perf-scan-keys",
      "in{field=/workflow/stage,any=review|escalated}", "scan", 0);
}

static int bench_pouch_perf_flush_intermediate(long iterations) {
  bench_pouch_perf_fixture fixture;
  lc_error error;
  double start;
  double elapsed;
  long rows;
  int rc;

  lc_error_init(&error);
  rows = bench_pouch_perf_rows(iterations);
  memset(&fixture, 0, sizeof(fixture));
  rc = bench_pouch_perf_prepare(&fixture, rows,
                                bench_pouch_perf_payload_bytes(), &error);
  if (rc == LC_OK) {
    rc = bench_pouch_seed_perf_docs(fixture.client, rows, 1L,
                                    bench_pouch_perf_payload_bytes(), &error);
  }
  start = bench_now_seconds();
  if (rc == LC_OK) {
    rc = bench_pouch_perf_flush(fixture.client, &error);
  }
  elapsed = bench_now_seconds() - start;
  printf("metric=pouch-perf-flush-intermediate rows=%ld crypto=%d "
         "seconds=%.6f per_row_us=%.3f rc=%d\n",
         rows, bench_env_enabled("LOCKDC_POUCH_PERF_CRYPTO"), elapsed,
         rows > 0L ? (elapsed * 1000000.0) / (double)rows : 0.0, rc);
  bench_pouch_perf_fixture_close(&fixture);
  lc_error_cleanup(&error);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_perf_flush_reopen(long iterations) {
  bench_pouch_perf_fixture fixture;
  lc_error error;
  double start;
  double elapsed;
  long rows;
  int rc;

  lc_error_init(&error);
  rows = bench_pouch_perf_rows(iterations);
  memset(&fixture, 0, sizeof(fixture));
  rc = bench_pouch_perf_prepare(&fixture, rows,
                                bench_pouch_perf_payload_bytes(), &error);
  if (fixture.client != NULL) {
    fixture.client->close(fixture.client);
    fixture.client = NULL;
  }
  if (rc == LC_OK) {
    rc = bench_pouch_client_open_crypto(fixture.root, fixture.crypto_key,
                                        &fixture.client, &error);
  }
  start = bench_now_seconds();
  if (rc == LC_OK) {
    rc = bench_pouch_perf_flush(fixture.client, &error);
  }
  elapsed = bench_now_seconds() - start;
  printf("metric=pouch-perf-flush-reopen rows=%ld crypto=%d seconds=%.6f "
         "per_row_us=%.3f rc=%d\n",
         rows, bench_env_enabled("LOCKDC_POUCH_PERF_CRYPTO"), elapsed,
         rows > 0L ? (elapsed * 1000000.0) / (double)rows : 0.0, rc);
  bench_pouch_perf_fixture_close(&fixture);
  lc_error_cleanup(&error);
  return rc == LC_OK ? 0 : 1;
}

static int bench_pouch_seed_query_docs(lc_client *client, long count,
                                       lc_error *error) {
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
    rc = bench_pouch_seed_update(client, key, source, error);
    lc_source_close(source);
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
      "{\"iprefix\":{\"field\":\"/tags[]\",\"value\":\"FIN\"}}", NULL, "index",
      0};

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
      "{\"iprefix\":{\"field\":\"/tags[]\",\"value\":\"FIN\"}}", NULL, "index",
      1};

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
      "{\"icontains\":{\"field\":\"/tags[]\",\"value\":\"INA\"}}", NULL,
      "index", 0};

  return bench_pouch_query_text(iterations, &query_case);
}

static int bench_pouch_query_icontains_scan_keys(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"icontains\":{\"field\":\"/tags[]\",\"value\":\"INA\"}}", NULL, "scan",
      0};

  return bench_pouch_query_text(iterations, &query_case);
}

static int bench_pouch_query_icontains_index_documents(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"icontains\":{\"field\":\"/tags[]\",\"value\":\"INA\"}}", NULL,
      "index", 1};

  return bench_pouch_query_text(iterations, &query_case);
}

static int bench_pouch_query_icontains_scan_documents(long iterations) {
  static const bench_pouch_query_case query_case = {
      "{\"icontains\":{\"field\":\"/tags[]\",\"value\":\"INA\"}}", NULL, "scan",
      1};

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

static int bench_pouch_query_recursive_exists_index_documents(long iterations) {
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
                             documents_value)                                  \
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
                     "contains{field=/details/message,value=timeout}", "index",
                     0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_contains_message_scan_keys,
                     "contains{field=/details/message,value=timeout}", "scan",
                     0)
BENCH_POUCH_LQL_FUNC(bench_pouch_query_contains_message_index_documents,
                     "contains{field=/details/message,value=timeout}", "index",
                     1)
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
    bench_pouch_query_or_sparse_or_flag_index_keys,
    "or.eq{field=/bucket,value=needle},or.eq{field=/flag,value=true}", "index",
    0)
BENCH_POUCH_LQL_FUNC(
    bench_pouch_query_or_sparse_or_flag_scan_keys,
    "or.eq{field=/bucket,value=needle},or.eq{field=/flag,value=true}", "scan",
    0)
BENCH_POUCH_LQL_FUNC(
    bench_pouch_query_or_sparse_or_flag_index_documents,
    "or.eq{field=/bucket,value=needle},or.eq{field=/flag,value=true}", "index",
    1)
BENCH_POUCH_LQL_FUNC(
    bench_pouch_query_or_sparse_or_flag_scan_documents,
    "or.eq{field=/bucket,value=needle},or.eq{field=/flag,value=true}", "scan",
    1)

static const bench_case *bench_cases(void) {
  static const bench_case cases[] = {
      {"stream-copy", 1000L, bench_stream_copy},
      {"pouch-open", 1000L, bench_pouch_open},
      {"pouch-namespace", 1000L, bench_pouch_namespace},
      {"pouch-perf-index-docs", 128L, bench_pouch_perf_index_docs},
      {"pouch-perf-full-text-keys", 128L, bench_pouch_perf_full_text_keys},
      {"pouch-perf-full-text-reopen-keys", 128L,
       bench_pouch_perf_full_text_reopen_keys},
      {"pouch-perf-scan-keys", 128L, bench_pouch_perf_scan_keys},
      {"pouch-perf-flush-intermediate", 128L,
       bench_pouch_perf_flush_intermediate},
      {"pouch-perf-flush-reopen", 128L, bench_pouch_perf_flush_reopen},
      {"workflow-reconcile", 256L, bench_workflow_reconcile},
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
      {"pouch-query-or-sparse-or-flag-index-keys", 1024L,
       bench_pouch_query_or_sparse_or_flag_index_keys},
      {"pouch-query-or-sparse-or-flag-scan-keys", 1024L,
       bench_pouch_query_or_sparse_or_flag_scan_keys},
      {"pouch-query-or-sparse-or-flag-index-documents", 1024L,
       bench_pouch_query_or_sparse_or_flag_index_documents},
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
  printf("%s iterations=%ld seconds=%.6f per_op_us=%.3f rc=%d\n", bench->name,
         iterations, elapsed,
         iterations > 0L ? (elapsed * 1000000.0) / (double)iterations : 0.0,
         rc);
  return rc;
}

int main(int argc, char **argv) {
  const bench_case *bench;
  long iterations;
  const char *name;
  int failed;

  if (argc > 1 &&
      (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-h") == 0)) {
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

#define _XOPEN_SOURCE 700

#include "pouchbench.h"

#include "lc/lc.h"
#include "lc_pouch.h"

#include <dirent.h>
#include <errno.h>
#include <limits.h>
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

struct lockdc_pouch_bench_fixture {
  char root[sizeof(LOCKDC_POUCH_BENCH_TMP_PREFIX "XXXXXX")];
  lc_client *client;
  long rows;
};

static char *lockdc_bench_document(long row, long generation,
                                   long payload_bytes, size_t *out_len);

static uint64_t lockdc_bench_now_ns(void) {
  struct timespec ts;

  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
    return 0U;
  }
  return ((uint64_t)ts.tv_sec * 1000000000ULL) + (uint64_t)ts.tv_nsec;
}

static void lockdc_bench_add_ns(uint64_t *dst, uint64_t start, uint64_t end) {
  if (dst == NULL || end < start) {
    return;
  }
  *dst += end - start;
}

static void lockdc_bench_add_phase_ns(uint64_t *phase, uint64_t *total,
                                      uint64_t start, uint64_t end) {
  uint64_t before;

  if (phase == NULL) {
    return;
  }
  before = *phase;
  lockdc_bench_add_ns(phase, start, end);
  if (total != NULL) {
    *total += *phase - before;
  }
}

static long lockdc_bench_query_matches_from_metadata(const char *metadata) {
  const char *field;
  const char *value;
  char *end;
  unsigned long rows;

  if (metadata == NULL) {
    return 0L;
  }
  field = strstr(metadata, "\"query_matches\":");
  if (field == NULL) {
    return 0L;
  }
  value = field + strlen("\"query_matches\":");
  rows = strtoul(value, &end, 10);
  if (end == value || rows > (unsigned long)LONG_MAX) {
    return 0L;
  }
  return (long)rows;
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

static int lockdc_bench_open_client(const char *root, const char *crypto_key,
                                    const char *compression, lc_client **out,
                                    lc_error *error) {
  lc_client_config config;
  const char *endpoints[1];
  char endpoint[1200];
  int written;
  int rc;

  written = snprintf(endpoint, sizeof(endpoint),
                     "pouch://%s?pouch_single_writer=true", root);
  if (written <= 0 || (size_t)written >= sizeof(endpoint)) {
    return LC_ERR_INVALID;
  }
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "bench";
  config.pouch_crypto_key = crypto_key;
  config.pouch_compression = compression;
  rc = lc_client_open(&config, out, error);
  return rc;
}

static const char *lockdc_bench_selector_lql(const char *scenario) {
  if (scenario == NULL || strcmp(scenario, "EqSparse") == 0) {
    return "eq{field=/bucket,value=needle}";
  }
  if (strcmp(scenario, "EqDense") == 0) {
    return "eq{field=/group,value=even}";
  }
  if (strcmp(scenario, "RangeHalf") == 0) {
    return "range{field=/value,gte=0}";
  }
  if (strcmp(scenario, "InRegionSingle") == 0) {
    return "in{field=/region,any=us}";
  }
  if (strcmp(scenario, "InTags") == 0) {
    return "in{field=/tags[],any=planning|finance}";
  }
  if (strcmp(scenario, "ContainsMessage") == 0) {
    return "contains{field=/details/message,value=timeout}";
  }
  if (strcmp(scenario, "IprefixTags") == 0) {
    return "iprefix{field=/tags[],value=FIN}";
  }
  if (strcmp(scenario, "IcontainsTags") == 0) {
    return "icontains{field=/tags[],value=INA}";
  }
  if (strcmp(scenario, "DateAfter") == 0) {
    return "date{field=/created_at,after=2025-01-01T00:00:00Z}";
  }
  if (strcmp(scenario, "RecursiveExists") == 0) {
    return "exists{/details/**}";
  }
  if (strcmp(scenario, "OrSparseOrFlag") == 0) {
    return "or.eq{field=/bucket,value=needle},or.eq{field=/flag,value=true}";
  }
  if (strcmp(scenario, "TenantEnterprise") == 0) {
    return "eq{field=/tenant/tier,value=enterprise}";
  }
  if (strcmp(scenario, "WorkflowEscalated") == 0) {
    return "in{field=/workflow/stage,any=review|escalated}";
  }
  if (strcmp(scenario, "AmountBand") == 0) {
    return "range{field=/metrics/amount_usd,gte=10000,lt=90000}";
  }
  if (strcmp(scenario, "RiskSignal") == 0) {
    return "icontains{field=/risk/summary,value=timeout}";
  }
  if (strcmp(scenario, "NarrativeSummary") == 0) {
    return "icontains{field=/narrative/summary,value=remediation}";
  }
  if (strcmp(scenario, "NarrativeDescription") == 0) {
    return "contains{field=/narrative/description,value=audit}";
  }
  if (strcmp(scenario, "FullTextAny") == 0) {
    return "icontains{field=/...,value=audit}";
  }
  return "eq{field=/bucket,value=needle}";
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
    char *json;
    size_t json_len;
    int rc;

    json = lockdc_bench_document(i, 0L, 4096L, &json_len);
    if (json == NULL) {
      return LC_ERR_NOMEM;
    }
    snprintf(key, sizeof(key), "doc/%08ld", i);
    lc_update_req_init(&req);
    memset(&res, 0, sizeof(res));
    req.lease.key = key;
    req.content_type = "application/json";
    source = NULL;
    rc = lc_source_from_memory(json, json_len, &source, error);
    if (rc == LC_OK) {
      rc = client->update(client, &req, source, &res, error);
    }
    if (source != NULL) {
      lc_source_close(source);
    }
    lc_update_res_cleanup(&res);
    free(json);
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

static char *lockdc_bench_copy_string(const char *value) {
  char *copy;
  size_t len;

  if (value == NULL) {
    return NULL;
  }
  len = strlen(value);
  copy = (char *)malloc(len + 1U);
  if (copy == NULL) {
    return NULL;
  }
  memcpy(copy, value, len + 1U);
  return copy;
}

static char *lockdc_bench_document(long row, long generation,
                                   long payload_bytes, size_t *out_len) {
  const char *bucket;
  const char *group;
  const char *region;
  const char *tag0;
  const char *tag1;
  const char *created_at;
  const char *message;
  const char *flag;
  const char *tenant_tier;
  const char *stage;
  const char *team;
  const char *priority;
  const char *source;
  const char *narrative_summary;
  const char *narrative_description;
  const char *operator_notes;
  char prefix[4096];
  size_t target;
  size_t prefix_len;
  size_t pad_len;
  char *json;
  int written;

  target = payload_bytes > 0L ? (size_t)payload_bytes : 1024U;
  bucket = (row % 64L) == 0L ? "needle" : "haystack";
  group = (row % 2L) == 0L ? "even" : "odd";
  region = (row % 3L) == 0L ? "us" : (row % 3L) == 1L ? "eu" : "apac";
  tag0 = (row % 2L) == 0L ? "planning" : "runtime";
  tag1 = (row % 4L) == 0L ? "finance" : "ops";
  created_at = (row % 5L) == 0L   ? "2026-01-01T00:00:00Z"
               : (row % 5L) == 1L ? "not-a-date"
                                  : "2024-01-01T00:00:00Z";
  message = (row % 8L) == 0L ? "timeout" : "ordinary";
  flag = (row % 7L) == 0L ? "true" : "false";
  tenant_tier = (row % 11L) == 0L  ? "enterprise"
                : (row % 3L) == 0L ? "business"
                                   : "standard";
  stage = (row % 6L) == 0L   ? "ingest"
          : (row % 6L) == 1L ? "review"
          : (row % 6L) == 2L ? "approve"
          : (row % 6L) == 3L ? "escalated"
          : (row % 6L) == 4L ? "settled"
                             : "archive";
  team = (row % 5L) == 0L ? "risk" : (row % 2L) == 0L ? "platform" : "ops";
  priority = (row % 13L) == 0L  ? "critical"
             : (row % 4L) == 0L ? "high"
                                : "normal";
  source = (row % 3L) == 0L ? "api" : (row % 3L) == 1L ? "batch" : "worker";
  narrative_summary =
      (row % 8L) == 0L
          ? "timeout remediation required for customer escalation with "
            "repeated "
            "queue delivery delays, partial worker retries, owner handoff "
            "notes, and operational impact across billing, provisioning, audit "
            "trail, and downstream reconciliation services"
          : "standard production summary with customer context, processing "
            "history, operator observations, reconciliation status, retry "
            "notes, audit trail references, and downstream service health "
            "annotations";
  narrative_description =
      "long production description capturing the full audit trail, workflow "
      "transitions, validation notes, customer-visible symptoms, previous "
      "remediation attempts, backoffice comments, service ownership history, "
      "deployment context, business priority, compliance review markers, and "
      "expected follow-up actions for operators and automated reconciliation "
      "jobs";
  operator_notes =
      (row % 13L) == 0L
          ? "operator notes include critical escalation context, manual "
            "override history, cross-team review comments, incident timeline, "
            "retry budget exhaustion notes, and final remediation checklist "
            "for "
            "the current production workflow"
          : "operator notes include routine triage comments, observed state "
            "transitions, queue consumer handoff details, attachment review "
            "status, replay expectations, and post-processing verification "
            "notes";
  written = snprintf(
      prefix, sizeof(prefix),
      "{\"bucket\":\"%s\",\"group\":\"%s\",\"region\":\"%s\","
      "\"value\":%ld,\"generation\":%ld,\"tags\":[\"%s\",\"%s\"],"
      "\"created_at\":\"%s\","
      "\"tenant\":{\"id\":\"tenant-%03ld\",\"tier\":\"%s\","
      "\"region\":\"%s\"},"
      "\"workflow\":{\"stage\":\"%s\",\"attempt\":%ld,"
      "\"owner\":{\"team\":\"%s\",\"user\":\"user-%05ld\"}},"
      "\"metrics\":{\"amount_usd\":%ld,\"latency_ms\":%ld,"
      "\"retries\":%ld},"
      "\"risk\":{\"score\":%ld,\"summary\":\"%s risk signal for "
      "production timeout workflow %ld\"},"
      "\"narrative\":{\"summary\":\"%s\",\"description\":\"%s\","
      "\"operator_notes\":\"%s\"},"
      "\"details\":{\"message\":\"%s production benchmark document "
      "%ld\",\"attributes\":{\"priority\":\"%s\",\"source\":\"%s\","
      "\"schema_version\":3}},"
      "\"line_items\":[{\"sku\":\"sku-%04ld\",\"qty\":%ld,"
      "\"price\":%ld},{\"sku\":\"sku-%04ld\",\"qty\":%ld,"
      "\"price\":%ld}],\"flag\":%s,\"storage_pressure\":null",
      bucket, group, region, row, generation, tag0, tag1, created_at, row % 47L,
      tenant_tier, region, stage, generation + 1L, team, row % 10000L,
      1000L + ((row * 7919L) % 120000L), 25L + (row % 250L), generation % 5L,
      (row * 37L) % 100L, message, row, narrative_summary,
      narrative_description, operator_notes, message, row, priority, source,
      row % 4096L, 1L + (row % 9L), 100L + (row % 500L), (row + 17L) % 4096L,
      1L + (row % 4L), 50L + (row % 300L), flag);
  if (written <= 0 || (size_t)written >= sizeof(prefix)) {
    return NULL;
  }
  prefix_len = (size_t)written;
  pad_len = target > prefix_len + 1U ? target - prefix_len - 1U : 32U;
  json = (char *)malloc(prefix_len + pad_len + 2U);
  if (json == NULL) {
    return NULL;
  }
  memcpy(json, prefix, prefix_len);
  memset(json + prefix_len, ' ', pad_len);
  json[prefix_len + pad_len] = '}';
  json[prefix_len + pad_len + 1U] = '\0';
  if (out_len != NULL) {
    *out_len = prefix_len + pad_len + 1U;
  }
  return json;
}

static long lockdc_bench_payload_for_generation(long generation,
                                                long updates_per_key,
                                                long payload_bytes) {
  const long current_payload_bytes = 2L * 1024L;

  if (updates_per_key <= 1L || generation < updates_per_key - 1L) {
    return payload_bytes;
  }
  return payload_bytes < current_payload_bytes ? payload_bytes
                                               : current_payload_bytes;
}

static int lockdc_bench_update_lease(lc_lease *lease, const char *json,
                                     size_t json_len, const char *if_etag,
                                     lc_error *error) {
  lc_update_opts opts;
  lc_source *source;
  int rc;

  source = NULL;
  rc = lc_source_from_memory(json, json_len, &source, error);
  if (rc != LC_OK) {
    return rc;
  }
  lc_update_opts_init(&opts);
  opts.content_type = "application/json";
  opts.if_state_etag = if_etag;
  rc = lease->update(lease, source, &opts, error);
  lc_source_close(source);
  return rc;
}

static int lockdc_bench_read_key(lc_client *client, const char *key,
                                 lc_error *error) {
  lc_get_opts opts;
  lc_get_res res;
  lc_sink *sink;
  int rc;

  memset(&opts, 0, sizeof(opts));
  memset(&res, 0, sizeof(res));
  opts.public_read = 1;
  sink = NULL;
  rc = lc_sink_to_memory(&sink, error);
  if (rc == LC_OK) {
    rc = client->get(client, key, &opts, sink, &res, error);
  }
  if (sink != NULL) {
    lc_sink_close(sink);
  }
  lc_get_res_cleanup(&res);
  return rc;
}

static int lockdc_bench_read_lease(lc_lease *lease, lc_error *error) {
  lc_get_opts opts;
  lc_get_res res;
  lc_sink *sink;
  int rc;

  if (lease == NULL) {
    return LC_ERR_INVALID;
  }
  lc_get_opts_init(&opts);
  memset(&res, 0, sizeof(res));
  sink = NULL;
  rc = lc_sink_to_memory(&sink, error);
  if (rc == LC_OK) {
    rc = lease->get(lease, sink, &opts, &res, error);
  }
  if (sink != NULL) {
    lc_sink_close(sink);
  }
  lc_get_res_cleanup(&res);
  return rc;
}

static int lockdc_bench_attach_and_read(lc_lease *lease, long row,
                                        lc_error *error) {
  char payload[4096];
  char name[64];
  lc_attach_req attach_req;
  lc_attach_res attach_res;
  lc_attachment_get_req get_req;
  lc_attachment_get_res get_res;
  lc_source *source;
  lc_sink *sink;
  int rc;

  memset(payload, (int)('A' + (row % 26L)), sizeof(payload));
  snprintf(name, sizeof(name), "blob-%08ld.bin", row);
  lc_attach_req_init(&attach_req);
  memset(&attach_res, 0, sizeof(attach_res));
  attach_req.name = name;
  attach_req.content_type = "application/octet-stream";
  source = NULL;
  rc = lc_source_from_memory(payload, sizeof(payload), &source, error);
  if (rc == LC_OK) {
    rc = lease->attach(lease, &attach_req, source, &attach_res, error);
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  lc_attach_res_cleanup(&attach_res);
  if (rc != LC_OK) {
    return rc;
  }

  lc_attachment_get_req_init(&get_req);
  memset(&get_res, 0, sizeof(get_res));
  get_req.selector.name = name;
  sink = NULL;
  rc = lc_sink_to_memory(&sink, error);
  if (rc == LC_OK) {
    rc = lease->get_attachment(lease, &get_req, sink, &get_res, error);
  }
  if (sink != NULL) {
    lc_sink_close(sink);
  }
  lc_attachment_get_res_cleanup(&get_res);
  return rc;
}

static int lockdc_bench_queue_roundtrip(lc_client *client, long messages,
                                        lockdc_pouch_bench_result *out,
                                        lc_error *error) {
  long i;

  for (i = 0; i < messages; ++i) {
    char payload[128];
    lc_enqueue_req enqueue_req;
    lc_enqueue_res enqueue_res;
    lc_source *source;
    int rc;
    int written;

    written = snprintf(payload, sizeof(payload),
                       "{\"message\":%ld,\"kind\":\"production\"}", i);
    if (written <= 0 || (size_t)written >= sizeof(payload)) {
      return LC_ERR_INVALID;
    }
    lc_enqueue_req_init(&enqueue_req);
    memset(&enqueue_res, 0, sizeof(enqueue_res));
    enqueue_req.namespace_name = "bench";
    enqueue_req.queue = "production";
    enqueue_req.visibility_timeout_seconds = 30L;
    enqueue_req.ttl_seconds = 3600L;
    enqueue_req.max_attempts = 3;
    enqueue_req.content_type = "application/json";
    source = NULL;
    rc = lc_source_from_memory(payload, strlen(payload), &source, error);
    if (rc == LC_OK) {
      rc = client->enqueue(client, &enqueue_req, source, &enqueue_res, error);
    }
    if (source != NULL) {
      lc_source_close(source);
    }
    lc_enqueue_res_cleanup(&enqueue_res);
    if (rc != LC_OK) {
      return rc;
    }
    out->queue_messages++;
  }

  for (;;) {
    lc_dequeue_req dequeue_req;
    lc_dequeue_batch_res batch;
    size_t index;
    int rc;

    lc_dequeue_req_init(&dequeue_req);
    memset(&batch, 0, sizeof(batch));
    dequeue_req.namespace_name = "bench";
    dequeue_req.queue = "production";
    dequeue_req.owner = "pouch-production-bench";
    dequeue_req.visibility_timeout_seconds = 30L;
    dequeue_req.wait_seconds = 0L;
    dequeue_req.page_size = 16;
    rc = client->dequeue_batch(client, &dequeue_req, &batch, error);
    if (rc != LC_OK) {
      lc_dequeue_batch_cleanup(&batch);
      return rc;
    }
    if (batch.count == 0U) {
      lc_dequeue_batch_cleanup(&batch);
      break;
    }
    for (index = 0U; index < batch.count; ++index) {
      if (batch.messages[index] != NULL &&
          batch.messages[index]->ack(batch.messages[index], error) != LC_OK) {
        lc_dequeue_batch_cleanup(&batch);
        return error != NULL && error->code != LC_OK ? error->code
                                                     : LC_ERR_INVALID;
      }
      batch.messages[index] = NULL;
    }
    lc_dequeue_batch_cleanup(&batch);
  }
  return LC_OK;
}

static long lockdc_bench_count_segments_in(const char *path) {
  DIR *dir;
  struct dirent *entry;
  long count;

  dir = opendir(path);
  if (dir == NULL) {
    return 0L;
  }
  count = 0L;
  while ((entry = readdir(dir)) != NULL) {
    char child[1024];
    struct stat st;
    int written;

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    if (strncmp(entry->d_name, "seg-", 4U) == 0 &&
        strstr(entry->d_name, ".log") != NULL) {
      ++count;
    }
    written = snprintf(child, sizeof(child), "%s/%s", path, entry->d_name);
    if (written <= 0 || (size_t)written >= sizeof(child)) {
      continue;
    }
    if (lstat(child, &st) == 0 && S_ISDIR(st.st_mode)) {
      count += lockdc_bench_count_segments_in(child);
    }
  }
  (void)closedir(dir);
  return count;
}

static long lockdc_bench_count_files_with_prefix_in(const char *path,
                                                    const char *prefix,
                                                    const char *suffix) {
  DIR *dir;
  struct dirent *entry;
  long count;
  size_t prefix_len;
  size_t suffix_len;

  if (path == NULL || prefix == NULL || suffix == NULL) {
    return 0L;
  }
  dir = opendir(path);
  if (dir == NULL) {
    return 0L;
  }
  count = 0L;
  prefix_len = strlen(prefix);
  suffix_len = strlen(suffix);
  while ((entry = readdir(dir)) != NULL) {
    char child[1024];
    struct stat st;
    size_t name_len;
    int written;

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    name_len = strlen(entry->d_name);
    if (name_len >= prefix_len + suffix_len &&
        strncmp(entry->d_name, prefix, prefix_len) == 0 &&
        strcmp(entry->d_name + name_len - suffix_len, suffix) == 0) {
      ++count;
    }
    written = snprintf(child, sizeof(child), "%s/%s", path, entry->d_name);
    if (written <= 0 || (size_t)written >= sizeof(child)) {
      continue;
    }
    if (lstat(child, &st) == 0 && S_ISDIR(st.st_mode)) {
      count += lockdc_bench_count_files_with_prefix_in(child, prefix, suffix);
    }
  }
  (void)closedir(dir);
  return count;
}

static int lockdc_bench_pouch_write_document(
    lc_pouch *pouch, long row, long generation, long updates_per_key,
    long payload_bytes, lockdc_pouch_bench_result *out, lc_error *error) {
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result result;
  lc_source *source;
  char *json;
  char key[64];
  size_t json_len;
  uint64_t phase_start;
  uint64_t phase_end;
  int rc;

  if (pouch == NULL || out == NULL) {
    return LC_ERR_INVALID;
  }
  snprintf(key, sizeof(key), "doc/%08ld", row);
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  options.content_type = "application/json";
  json = lockdc_bench_document(row, generation,
                               lockdc_bench_payload_for_generation(
                                   generation, updates_per_key, payload_bytes),
                               &json_len);
  if (json == NULL) {
    return LC_ERR_NOMEM;
  }
  source = NULL;
  rc = lc_source_from_memory(json, json_len, &source, error);
  if (rc == LC_OK) {
    phase_start = lockdc_bench_now_ns();
    rc = lc_pouch_state_write(pouch, "bench", key, source, &options, &result,
                              error);
    phase_end = lockdc_bench_now_ns();
    if (rc == LC_OK) {
      uint64_t elapsed;

      elapsed = phase_end >= phase_start ? phase_end - phase_start : 0U;
      out->update_ns += elapsed;
      if (elapsed > out->max_update_ns) {
        out->max_update_ns = elapsed;
      }
      ++out->writes;
      out->bytes += (long)json_len;
    }
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  lc_pouch_state_write_result_cleanup(NULL, &result);
  free(json);
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
  req.selector_lql = lockdc_bench_selector_lql(scenario);
  req.engine = engine;
  req.limit = limit > 0L ? limit : 1L;
  if (documents) {
    lc_sink *sink;

    sink = NULL;
    rc = lc_sink_to_discard(&sink, error);
    if (rc == LC_OK) {
      rc = client->query(client, &req, sink, &res, error);
    }
    if (rc == LC_OK && rows != NULL) {
      *rows = lockdc_bench_query_matches_from_metadata(res.metadata_json);
    }
    if (sink != NULL) {
      lc_sink_close(sink);
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

static void lockdc_bench_result_set_error(lockdc_pouch_bench_result *out,
                                          const lc_error *error) {
  if (out != NULL && error != NULL && error->message != NULL &&
      error->message[0] != '\0') {
    snprintf(out->error, sizeof(out->error), "%s", error->message);
  }
}

int lockdc_pouch_bench_fixture_open(long rows, lockdc_pouch_bench_fixture **out,
                                    lockdc_pouch_bench_result *result) {
  char root_template[] = LOCKDC_POUCH_BENCH_TMP_PREFIX "XXXXXX";
  lockdc_pouch_bench_fixture *fixture;
  lc_error error;
  int rc;

  if (out == NULL || result == NULL) {
    return LC_ERR_INVALID;
  }
  *out = NULL;
  memset(result, 0, sizeof(*result));
  if (rows <= 0L) {
    rows = 1L;
  }
  fixture = (lockdc_pouch_bench_fixture *)calloc(1U, sizeof(*fixture));
  if (fixture == NULL) {
    result->rc = LC_ERR_NOMEM;
    return result->rc;
  }
  lc_error_init(&error);
  if (mkdtemp(root_template) == NULL) {
    result->rc = errno;
    free(fixture);
    lc_error_cleanup(&error);
    return result->rc;
  }
  snprintf(fixture->root, sizeof(fixture->root), "%s", root_template);
  fixture->rows = rows;
  rc = lockdc_bench_open_client(fixture->root, NULL, NULL, &fixture->client,
                                &error);
  if (rc == LC_OK) {
    rc = lockdc_bench_seed(fixture->client, rows, &error);
  }
  if (rc == LC_OK) {
    rc = lockdc_bench_flush(fixture->client, &error);
  }
  if (rc != LC_OK) {
    result->rc = rc;
    lockdc_bench_result_set_error(result, &error);
    lockdc_pouch_bench_fixture_close(fixture);
    lc_error_cleanup(&error);
    return rc;
  }
  lc_error_cleanup(&error);
  *out = fixture;
  return LC_OK;
}

int lockdc_pouch_bench_fixture_query(lockdc_pouch_bench_fixture *fixture,
                                     const char *scenario, const char *engine,
                                     int documents,
                                     lockdc_pouch_bench_result *out) {
  lc_error error;
  uint64_t start;
  uint64_t end;
  long matched_rows;
  int rc;

  if (fixture == NULL || fixture->client == NULL || out == NULL) {
    return LC_ERR_INVALID;
  }
  memset(out, 0, sizeof(*out));
  matched_rows = 0L;
  lc_error_init(&error);
  start = lockdc_bench_now_ns();
  rc = lockdc_bench_query(fixture->client, scenario, engine, documents,
                          fixture->rows, &matched_rows, &error);
  end = lockdc_bench_now_ns();
  if (rc != LC_OK) {
    lockdc_bench_result_set_error(out, &error);
  }
  lc_error_cleanup(&error);
  out->rc = rc;
  out->rows = matched_rows;
  out->c_ns = end >= start ? end - start : 0U;
  return rc;
}

void lockdc_pouch_bench_fixture_close(lockdc_pouch_bench_fixture *fixture) {
  if (fixture == NULL) {
    return;
  }
  if (fixture->client != NULL) {
    lc_client_close(fixture->client);
  }
  lockdc_bench_cleanup_root(fixture->root);
  free(fixture);
}

int lockdc_pouch_bench_production_run(long rows, long updates_per_key,
                                      long payload_bytes, int crypto_enabled,
                                      int compression_enabled,
                                      lockdc_pouch_bench_result *out) {
  char root_template[] = LOCKDC_POUCH_BENCH_TMP_PREFIX "XXXXXX";
  char root[sizeof(LOCKDC_POUCH_BENCH_TMP_PREFIX "XXXXXX")];
  lc_client *client;
  lc_error error;
  uint64_t start;
  uint64_t end;
  uint64_t phase_start;
  char *crypto_key;
  long row;
  long matched_rows;
  long queue_messages;
  int rc;

  if (out == NULL) {
    return LC_ERR_INVALID;
  }
  memset(out, 0, sizeof(*out));
  if (rows <= 0L) {
    rows = 128L;
  }
  if (updates_per_key <= 0L) {
    updates_per_key = 3L;
  }
  if (payload_bytes <= 0L) {
    payload_bytes = 256L * 1024L;
  }
  matched_rows = 0L;
  client = NULL;
  crypto_key = NULL;
  root[0] = '\0';
  lc_error_init(&error);
  if (mkdtemp(root_template) == NULL) {
    out->rc = errno;
    return out->rc;
  }
  snprintf(root, sizeof(root), "%s", root_template);
  start = lockdc_bench_now_ns();
  if (crypto_enabled != 0) {
    rc = lc_pouch_crypto_generate_key_string(&crypto_key, &error);
    if (rc != LC_OK) {
      goto done;
    }
  }
  rc = lockdc_bench_open_client(root, crypto_key,
                                compression_enabled != 0 ? "zlib" : NULL,
                                &client, &error);
  if (rc != LC_OK) {
    goto done;
  }

  for (row = 0L; row < rows; ++row) {
    lc_acquire_req acquire_req;
    lc_release_req release_req;
    lc_lease *lease;
    char key[64];
    char *stale_etag;
    long generation;

    snprintf(key, sizeof(key), "doc/%08ld", row);
    lc_acquire_req_init(&acquire_req);
    acquire_req.namespace_name = "bench";
    acquire_req.key = key;
    acquire_req.owner = "pouch-production-bench";
    acquire_req.ttl_seconds = 120L;
    lease = NULL;
    phase_start = lockdc_bench_now_ns();
    rc = client->acquire(client, &acquire_req, &lease, &error);
    if (rc != LC_OK) {
      goto done;
    }
    lockdc_bench_add_ns(&out->acquire_ns, phase_start, lockdc_bench_now_ns());
    stale_etag = NULL;
    for (generation = 0L; generation < updates_per_key; ++generation) {
      char *json;
      size_t json_len;

      json =
          lockdc_bench_document(row, generation,
                                lockdc_bench_payload_for_generation(
                                    generation, updates_per_key, payload_bytes),
                                &json_len);
      if (json == NULL) {
        rc = LC_ERR_NOMEM;
        lease->close(lease);
        goto done;
      }
      phase_start = lockdc_bench_now_ns();
      rc = lockdc_bench_update_lease(lease, json, json_len, NULL, &error);
      free(json);
      if (rc != LC_OK) {
        lease->close(lease);
        goto done;
      }
      lockdc_bench_add_ns(&out->update_ns, phase_start, lockdc_bench_now_ns());
      out->writes++;
      out->bytes += (long)json_len;
      if ((out->writes % 128L) == 0L) {
        phase_start = lockdc_bench_now_ns();
        rc = lockdc_bench_flush(client, &error);
        if (rc != LC_OK) {
          lease->close(lease);
          goto done;
        }
        lockdc_bench_add_phase_ns(&out->flush_intermediate_ns, &out->flush_ns,
                                  phase_start, lockdc_bench_now_ns());
      }
      if (row == 0L && generation == 0L && lease->state_etag != NULL) {
        stale_etag = lockdc_bench_copy_string(lease->state_etag);
        if (stale_etag == NULL) {
          rc = LC_ERR_NOMEM;
          lease->close(lease);
          goto done;
        }
      }
    }
    if (row == 0L && stale_etag != NULL) {
      lc_error stale_error;
      char *json;
      size_t json_len;

      lc_error_init(&stale_error);
      json = lockdc_bench_document(
          row, updates_per_key + 1L,
          lockdc_bench_payload_for_generation(updates_per_key + 1L,
                                              updates_per_key, payload_bytes),
          &json_len);
      if (json == NULL) {
        free(stale_etag);
        rc = LC_ERR_NOMEM;
        lease->close(lease);
        lc_error_cleanup(&stale_error);
        goto done;
      }
      phase_start = lockdc_bench_now_ns();
      rc = lockdc_bench_update_lease(lease, json, json_len, stale_etag,
                                     &stale_error);
      free(json);
      lockdc_bench_add_ns(&out->stale_ns, phase_start, lockdc_bench_now_ns());
      if (rc == LC_OK) {
        free(stale_etag);
        rc = LC_ERR_INVALID;
        lc_error_cleanup(&error);
        lc_error_init(&error);
        lease->close(lease);
        lc_error_cleanup(&stale_error);
        goto done;
      }
      out->stale_failures++;
      rc = LC_OK;
      lc_error_cleanup(&stale_error);
    }
    free(stale_etag);
    if ((row % 16L) == 0L) {
      phase_start = lockdc_bench_now_ns();
      rc = lockdc_bench_attach_and_read(lease, row, &error);
      if (rc != LC_OK) {
        lease->close(lease);
        goto done;
      }
      lockdc_bench_add_ns(&out->attachment_ns, phase_start,
                          lockdc_bench_now_ns());
      out->attachments++;
      out->reads++;
    }
    if ((row % 32L) == 0L) {
      phase_start = lockdc_bench_now_ns();
      rc = lockdc_bench_read_lease(lease, &error);
      if (rc != LC_OK) {
        lease->close(lease);
        goto done;
      }
      lockdc_bench_add_ns(&out->get_lease_ns, phase_start,
                          lockdc_bench_now_ns());
      out->reads++;
    }
    lc_release_req_init(&release_req);
    phase_start = lockdc_bench_now_ns();
    rc = lease->release(lease, &release_req, &error);
    if (rc != LC_OK) {
      lease->close(lease);
      goto done;
    }
    lockdc_bench_add_ns(&out->release_ns, phase_start, lockdc_bench_now_ns());
  }

  queue_messages = rows / 8L;
  if (queue_messages <= 0L) {
    queue_messages = 1L;
  }
  phase_start = lockdc_bench_now_ns();
  rc = lockdc_bench_queue_roundtrip(client, queue_messages, out, &error);
  if (rc != LC_OK) {
    goto done;
  }
  lockdc_bench_add_ns(&out->queue_ns, phase_start, lockdc_bench_now_ns());
  phase_start = lockdc_bench_now_ns();
  rc = lockdc_bench_flush(client, &error);
  if (rc != LC_OK) {
    goto done;
  }
  lockdc_bench_add_phase_ns(&out->flush_final_ns, &out->flush_ns, phase_start,
                            lockdc_bench_now_ns());
  phase_start = lockdc_bench_now_ns();
  rc = lockdc_bench_flush(client, &error);
  if (rc != LC_OK) {
    goto done;
  }
  lockdc_bench_add_phase_ns(&out->flush_noop_ns, &out->flush_ns, phase_start,
                            lockdc_bench_now_ns());
  phase_start = lockdc_bench_now_ns();
  lc_client_close(client);
  client = NULL;
  rc = lockdc_bench_open_client(root, crypto_key,
                                compression_enabled != 0 ? "zlib" : NULL,
                                &client, &error);
  if (rc != LC_OK) {
    goto done;
  }
  lockdc_bench_add_ns(&out->reopen_ns, phase_start, lockdc_bench_now_ns());
  phase_start = lockdc_bench_now_ns();
  rc = lockdc_bench_flush(client, &error);
  if (rc != LC_OK) {
    goto done;
  }
  lockdc_bench_add_phase_ns(&out->flush_reopen_ns, &out->flush_ns, phase_start,
                            lockdc_bench_now_ns());
  phase_start = lockdc_bench_now_ns();
  rc = lockdc_bench_query(client, "RangeHalf", "index", 0, rows, &out->rows,
                          &error);
  if (rc != LC_OK) {
    goto done;
  }
  if (out->rows <= 0L) {
    rc = LC_ERR_INVALID;
    snprintf(out->error, sizeof(out->error),
             "pouch production RangeHalf index query matched no rows");
    lc_error_cleanup(&error);
    lc_error_init(&error);
    goto done;
  }
  lockdc_bench_add_ns(&out->index_query_keys_ns, phase_start,
                      lockdc_bench_now_ns());
  matched_rows = 0L;
  phase_start = lockdc_bench_now_ns();
  rc = lockdc_bench_query(client, "NarrativeSummary", "index", 1, rows,
                          &matched_rows, &error);
  if (rc != LC_OK) {
    goto done;
  }
  if (matched_rows <= 0L) {
    rc = LC_ERR_INVALID;
    snprintf(out->error, sizeof(out->error),
             "pouch production NarrativeSummary index query matched no rows");
    lc_error_cleanup(&error);
    lc_error_init(&error);
    goto done;
  }
  lockdc_bench_add_ns(&out->index_query_docs_ns, phase_start,
                      lockdc_bench_now_ns());
  matched_rows = 0L;
  phase_start = lockdc_bench_now_ns();
  rc = lockdc_bench_query(client, "WorkflowEscalated", "scan", 0, rows,
                          &matched_rows, &error);
  if (rc != LC_OK) {
    goto done;
  }
  if (matched_rows <= 0L) {
    rc = LC_ERR_INVALID;
    snprintf(out->error, sizeof(out->error),
             "pouch production WorkflowEscalated scan query matched no rows");
    lc_error_cleanup(&error);
    lc_error_init(&error);
    goto done;
  }
  lockdc_bench_add_ns(&out->scan_query_keys_ns, phase_start,
                      lockdc_bench_now_ns());
  matched_rows = 0L;
  phase_start = lockdc_bench_now_ns();
  rc = lockdc_bench_query(client, "NarrativeDescription", "scan", 1, rows,
                          &matched_rows, &error);
  if (rc != LC_OK) {
    goto done;
  }
  if (matched_rows <= 0L) {
    rc = LC_ERR_INVALID;
    snprintf(
        out->error, sizeof(out->error),
        "pouch production NarrativeDescription scan query matched no rows");
    lc_error_cleanup(&error);
    lc_error_init(&error);
    goto done;
  }
  lockdc_bench_add_ns(&out->scan_query_docs_ns, phase_start,
                      lockdc_bench_now_ns());
  matched_rows = 0L;
  phase_start = lockdc_bench_now_ns();
  rc = lockdc_bench_query(client, "FullTextAny", "index", 0, rows,
                          &matched_rows, &error);
  if (rc != LC_OK) {
    goto done;
  }
  if (matched_rows <= 0L) {
    rc = LC_ERR_INVALID;
    snprintf(out->error, sizeof(out->error),
             "pouch production FullTextAny index query matched no rows");
    lc_error_cleanup(&error);
    lc_error_init(&error);
    goto done;
  }
  lockdc_bench_add_ns(&out->full_text_index_keys_ns, phase_start,
                      lockdc_bench_now_ns());
  matched_rows = 0L;
  phase_start = lockdc_bench_now_ns();
  rc = lockdc_bench_query(client, "FullTextAny", "scan", 1, rows, &matched_rows,
                          &error);
  if (rc != LC_OK) {
    goto done;
  }
  if (matched_rows <= 0L) {
    rc = LC_ERR_INVALID;
    snprintf(out->error, sizeof(out->error),
             "pouch production FullTextAny scan query matched no rows");
    lc_error_cleanup(&error);
    lc_error_init(&error);
    goto done;
  }
  lockdc_bench_add_ns(&out->full_text_scan_docs_ns, phase_start,
                      lockdc_bench_now_ns());
  for (row = 0L; row < rows; row += rows > 8L ? rows / 8L : 1L) {
    char key[64];

    snprintf(key, sizeof(key), "doc/%08ld", row);
    phase_start = lockdc_bench_now_ns();
    rc = lockdc_bench_read_key(client, key, &error);
    if (rc != LC_OK) {
      goto done;
    }
    lockdc_bench_add_ns(&out->get_public_ns, phase_start,
                        lockdc_bench_now_ns());
    out->reads++;
    if (row == rows - 1L) {
      break;
    }
  }
  out->segments = lockdc_bench_count_segments_in(root);
  if (out->segments <= 1L) {
    rc = LC_ERR_INVALID;
    snprintf(out->error, sizeof(out->error),
             "production pouch benchmark produced %ld segment(s), expected "
             "multiple segments with default segment target",
             out->segments);
    goto done_without_error_message;
  }

done:
  if (rc != LC_OK) {
    lockdc_bench_result_set_error(out, &error);
  }
done_without_error_message:
  end = lockdc_bench_now_ns();
  if (client != NULL) {
    lc_client_close(client);
  }
  lc_pouch_crypto_key_string_free(crypto_key);
  lockdc_bench_cleanup_root(root);
  lc_error_cleanup(&error);
  out->rc = rc;
  out->c_ns = end >= start ? end - start : 0U;
  return rc;
}

int lockdc_pouch_bench_compaction_run(
    long rows, long updates_per_key, long payload_bytes,
    long segment_target_bytes, long compaction_min_segment_count,
    long compaction_min_reclaimable_bytes, int scheduled, int crypto_enabled,
    int compression_enabled, lockdc_pouch_bench_result *out) {
  char root_template[] = LOCKDC_POUCH_BENCH_TMP_PREFIX "XXXXXX";
  char root[sizeof(LOCKDC_POUCH_BENCH_TMP_PREFIX "XXXXXX")];
  lc_pouch *pouch;
  lc_pouch_open_options open_options;
  lc_pouch_maintenance_options maintenance_options;
  lc_pouch_maintenance_result maintenance_result;
  lc_error error;
  char *crypto_key;
  uint64_t start;
  uint64_t end;
  uint64_t phase_start;
  long row;
  long generation;
  int rc;

  if (out == NULL) {
    return LC_ERR_INVALID;
  }
  memset(out, 0, sizeof(*out));
  if (rows <= 0L) {
    rows = 512L;
  }
  if (updates_per_key <= 0L) {
    updates_per_key = 2L;
  }
  if (payload_bytes <= 0L) {
    payload_bytes = 16L * 1024L;
  }
  if (segment_target_bytes <= 0L) {
    segment_target_bytes = 256L * 1024L;
  }
  if (compaction_min_segment_count <= 0L) {
    compaction_min_segment_count = 2L;
  }
  if (compaction_min_reclaimable_bytes <= 0L) {
    compaction_min_reclaimable_bytes = 1L;
  }
  pouch = NULL;
  crypto_key = NULL;
  root[0] = '\0';
  lc_error_init(&error);
  if (mkdtemp(root_template) == NULL) {
    out->rc = errno;
    return out->rc;
  }
  snprintf(root, sizeof(root), "%s", root_template);
  start = lockdc_bench_now_ns();
  if (crypto_enabled != 0) {
    rc = lc_pouch_crypto_generate_key_string(&crypto_key, &error);
    if (rc != LC_OK) {
      goto done;
    }
  }
  memset(&open_options, 0, sizeof(open_options));
  open_options.segment_target_bytes = (unsigned long)segment_target_bytes;
  open_options.compaction_min_segment_count =
      (unsigned long)compaction_min_segment_count;
  open_options.compaction_min_reclaimable_bytes =
      (unsigned long)compaction_min_reclaimable_bytes;
  open_options.background_compaction_enabled = scheduled != 0 ? 1 : 0;
  open_options.single_writer = 1;
  open_options.query_engine = "index";
  open_options.crypto_key = crypto_key;
  open_options.compression = compression_enabled != 0 ? "zlib" : NULL;
  rc = lc_pouch_open(root, NULL, &open_options, &pouch, &error);
  if (rc != LC_OK) {
    goto done;
  }

  for (row = 0L; row < rows; ++row) {
    for (generation = 0L; generation < updates_per_key; ++generation) {
      rc = lockdc_bench_pouch_write_document(
          pouch, row, generation, updates_per_key, payload_bytes, out, &error);
      if (rc != LC_OK) {
        goto done;
      }
    }
  }
  out->rows = rows;
  out->segments = lockdc_bench_count_segments_in(root);
  out->snapshots =
      lockdc_bench_count_files_with_prefix_in(root, "snapshot-", ".log");

  if (scheduled == 0) {
    memset(&maintenance_options, 0, sizeof(maintenance_options));
    memset(&maintenance_result, 0, sizeof(maintenance_result));
    maintenance_options.namespace_name = "bench";
    maintenance_options.force = 1;
    phase_start = lockdc_bench_now_ns();
    rc = lc_pouch_maintenance_run(pouch, &maintenance_options,
                                  &maintenance_result, &error);
    lockdc_bench_add_ns(&out->compaction_ns, phase_start,
                        lockdc_bench_now_ns());
    if (rc != LC_OK) {
      lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
      goto done;
    }
    if (maintenance_result.compacted) {
      ++out->compactions;
    }
    out->candidate_segments = (long)maintenance_result.candidate_segment_count;
    out->candidate_bytes = (long)maintenance_result.candidate_bytes;
    lc_pouch_maintenance_result_cleanup(NULL, &maintenance_result);
  }

  out->segments = lockdc_bench_count_segments_in(root);
  out->snapshots =
      lockdc_bench_count_files_with_prefix_in(root, "snapshot-", ".log");
  if (scheduled != 0) {
    out->compactions = out->snapshots;
  }
  if (scheduled == 0 && (out->compactions <= 0L || out->snapshots <= 0L)) {
    rc = LC_ERR_INVALID;
    snprintf(out->error, sizeof(out->error),
             "forced compaction benchmark did not compact");
    goto done_without_error_message;
  }

done:
  if (rc != LC_OK) {
    lockdc_bench_result_set_error(out, &error);
  }
done_without_error_message:
  end = lockdc_bench_now_ns();
  if (pouch != NULL) {
    lc_pouch_close(pouch);
  }
  lc_pouch_crypto_key_string_free(crypto_key);
  lockdc_bench_cleanup_root(root);
  lc_error_cleanup(&error);
  out->rc = rc;
  out->c_ns = end >= start ? end - start : 0U;
  return rc;
}

int lockdc_pouch_bench_run(const char *scenario, long rows, const char *engine,
                           int documents, lockdc_pouch_bench_result *out) {
  lockdc_pouch_bench_fixture *fixture;
  int rc;

  if (out == NULL) {
    return LC_ERR_INVALID;
  }
  fixture = NULL;
  rc = lockdc_pouch_bench_fixture_open(rows, &fixture, out);
  if (rc == LC_OK) {
    rc = lockdc_pouch_bench_fixture_query(fixture, scenario, engine, documents,
                                          out);
  }
  lockdc_pouch_bench_fixture_close(fixture);
  return rc;
}

#define _XOPEN_SOURCE 700

#include "pouchbench.h"

#include "lc/lc.h"
#include "lc_pouch.h"

#include <dirent.h>
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define LOCKDC_POUCH_BENCH_TMP_PREFIX "/tmp/liblockdc-pouch-go-bench-"

static const char lockdc_bench_concurrency_payload_prefix[] = "{\"payload\":\"";
static const char lockdc_bench_concurrency_payload_suffix[] = "\"}";

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

static const char *lockdc_bench_tenant_tier(long row) {
  if ((row % 11L) == 0L) {
    return "enterprise";
  }
  if ((row % 3L) == 0L) {
    return "business";
  }
  return "standard";
}

static const char *lockdc_bench_region(long row) {
  return (row % 3L) == 0L ? "us" : (row % 3L) == 1L ? "eu" : "apac";
}

static const char *lockdc_bench_workflow_stage(long row) {
  return (row % 6L) == 0L   ? "ingest"
         : (row % 6L) == 1L ? "review"
         : (row % 6L) == 2L ? "approve"
         : (row % 6L) == 3L ? "escalated"
         : (row % 6L) == 4L ? "settled"
                            : "archive";
}

static const char *lockdc_bench_team(long row) {
  return (row % 5L) == 0L ? "risk" : (row % 2L) == 0L ? "platform" : "ops";
}

static const char *lockdc_bench_priority(long row) {
  return (row % 13L) == 0L ? "critical" : (row % 4L) == 0L ? "high" : "normal";
}

static const char *lockdc_bench_source(long row) {
  return (row % 3L) == 0L ? "api" : (row % 3L) == 1L ? "batch" : "worker";
}

static const char *lockdc_bench_message(long row) {
  return (row % 8L) == 0L ? "timeout" : "ordinary";
}

static const char *lockdc_bench_narrative_summary(long row) {
  return (row % 8L) == 0L
             ? "timeout remediation required for customer escalation with "
               "repeated queue delivery delays, partial worker retries, owner "
               "handoff notes, and operational impact across billing, "
               "provisioning, audit trail, and downstream reconciliation "
               "services"
             : "standard production summary with customer context, processing "
               "history, operator observations, reconciliation status, retry "
               "notes, audit trail references, and downstream service health "
               "annotations";
}

static const char *lockdc_bench_narrative_description(void) {
  return "long production description capturing the full audit trail, workflow "
         "transitions, validation notes, customer-visible symptoms, previous "
         "remediation attempts, backoffice comments, service ownership "
         "history, "
         "deployment context, business priority, compliance review markers, "
         "and "
         "expected follow-up actions for operators and automated "
         "reconciliation "
         "jobs";
}

static const char *lockdc_bench_operator_notes(long row) {
  return (row % 13L) == 0L
             ? "operator notes include critical escalation context, manual "
               "override history, cross-team review comments, incident "
               "timeline, retry budget exhaustion notes, and final remediation "
               "checklist for the current production workflow"
             : "operator notes include routine triage comments, observed state "
               "transitions, queue consumer handoff details, attachment review "
               "status, replay expectations, and post-processing verification "
               "notes";
}

static int lockdc_bench_bytes_contains(const void *bytes, size_t length,
                                       const char *needle) {
  const unsigned char *haystack;
  size_t needle_len;
  size_t index;

  if (bytes == NULL || needle == NULL) {
    return 0;
  }
  needle_len = strlen(needle);
  if (needle_len == 0U || needle_len > length) {
    return 0;
  }
  haystack = (const unsigned char *)bytes;
  for (index = 0U; index + needle_len <= length; ++index) {
    if (memcmp(haystack + index, needle, needle_len) == 0) {
      return 1;
    }
  }
  return 0;
}

static int lockdc_bench_expect_contains(const void *bytes, size_t length,
                                        const char *snippet, const char *label,
                                        lc_error *error) {
  (void)label;
  (void)error;
  if (lockdc_bench_bytes_contains(bytes, length, snippet)) {
    return LC_OK;
  }
  return LC_ERR_INVALID;
}

static int lockdc_bench_validate_document_fields(const void *bytes,
                                                 size_t length, long row,
                                                 long generation,
                                                 lc_error *error) {
  char snippet[2048];
  int written;
  int rc;

  if (bytes == NULL || length == 0U) {
    (void)error;
    return LC_ERR_INVALID;
  }
  written = snprintf(snippet, sizeof(snippet), "\"value\":%ld", row);
  if (written <= 0 || (size_t)written >= sizeof(snippet)) {
    return LC_ERR_INVALID;
  }
  rc = lockdc_bench_expect_contains(bytes, length, snippet, "value", error);
  if (rc != LC_OK) {
    return rc;
  }
  written =
      snprintf(snippet, sizeof(snippet), "\"generation\":%ld", generation);
  if (written <= 0 || (size_t)written >= sizeof(snippet)) {
    return LC_ERR_INVALID;
  }
  rc =
      lockdc_bench_expect_contains(bytes, length, snippet, "generation", error);
  if (rc != LC_OK) {
    return rc;
  }
  written = snprintf(snippet, sizeof(snippet),
                     "\"tenant\":{\"id\":\"tenant-%03ld\",\"tier\":\"%s\","
                     "\"region\":\"%s\"}",
                     row % 47L, lockdc_bench_tenant_tier(row),
                     lockdc_bench_region(row));
  if (written <= 0 || (size_t)written >= sizeof(snippet)) {
    return LC_ERR_INVALID;
  }
  rc = lockdc_bench_expect_contains(bytes, length, snippet, "tenant", error);
  if (rc != LC_OK) {
    return rc;
  }
  written = snprintf(snippet, sizeof(snippet),
                     "\"workflow\":{\"stage\":\"%s\",\"attempt\":%ld,"
                     "\"owner\":{\"team\":\"%s\",\"user\":\"user-%05ld\"}}",
                     lockdc_bench_workflow_stage(row), generation + 1L,
                     lockdc_bench_team(row), row % 10000L);
  if (written <= 0 || (size_t)written >= sizeof(snippet)) {
    return LC_ERR_INVALID;
  }
  rc = lockdc_bench_expect_contains(bytes, length, snippet, "workflow", error);
  if (rc != LC_OK) {
    return rc;
  }
  written = snprintf(
      snippet, sizeof(snippet),
      "\"risk\":{\"score\":%ld,\"summary\":\"%s risk signal for production "
      "timeout workflow %ld\"}",
      (row * 37L) % 100L, lockdc_bench_message(row), row);
  if (written <= 0 || (size_t)written >= sizeof(snippet)) {
    return LC_ERR_INVALID;
  }
  rc = lockdc_bench_expect_contains(bytes, length, snippet, "risk", error);
  if (rc != LC_OK) {
    return rc;
  }
  written = snprintf(snippet, sizeof(snippet),
                     "\"narrative\":{\"summary\":\"%s\",\"description\":\"%s\","
                     "\"operator_notes\":\"%s\"}",
                     lockdc_bench_narrative_summary(row),
                     lockdc_bench_narrative_description(),
                     lockdc_bench_operator_notes(row));
  if (written <= 0 || (size_t)written >= sizeof(snippet)) {
    return LC_ERR_INVALID;
  }
  rc = lockdc_bench_expect_contains(bytes, length, snippet, "narrative", error);
  if (rc != LC_OK) {
    return rc;
  }
  written = snprintf(
      snippet, sizeof(snippet),
      "\"details\":{\"message\":\"%s production benchmark document %ld\","
      "\"attributes\":{\"priority\":\"%s\",\"source\":\"%s\","
      "\"schema_version\":3}}",
      lockdc_bench_message(row), row, lockdc_bench_priority(row),
      lockdc_bench_source(row));
  if (written <= 0 || (size_t)written >= sizeof(snippet)) {
    return LC_ERR_INVALID;
  }
  rc = lockdc_bench_expect_contains(bytes, length, snippet, "details", error);
  if (rc != LC_OK) {
    return rc;
  }
  written =
      snprintf(snippet, sizeof(snippet),
               "\"payload\":\" audit remediation evidence workflow row %ld gen "
               "%ld;",
               row, generation);
  if (written <= 0 || (size_t)written >= sizeof(snippet)) {
    return LC_ERR_INVALID;
  }
  return lockdc_bench_expect_contains(bytes, length, snippet, "payload", error);
}

static long lockdc_bench_expected_query_matches(const char *scenario,
                                                long rows) {
  long row;
  long count;

  if (rows <= 0L) {
    return 0L;
  }
  if (scenario == NULL || strcmp(scenario, "RangeHalf") == 0 ||
      strcmp(scenario, "NarrativeDescription") == 0 ||
      strcmp(scenario, "FullTextAny") == 0) {
    return rows;
  }
  count = 0L;
  for (row = 0L; row < rows; ++row) {
    if (strcmp(scenario, "NarrativeSummary") == 0) {
      if ((row % 8L) == 0L) {
        ++count;
      }
    } else if (strcmp(scenario, "WorkflowEscalated") == 0) {
      if ((row % 6L) == 1L || (row % 6L) == 3L) {
        ++count;
      }
    }
  }
  return count;
}

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
                                    const char *compression,
                                    uint64_t segment_target_bytes,
                                    lc_client **out, lc_error *error) {
  lc_client_config config;
  const char *endpoints[1];
  char endpoint[1200];
  int written;
  int rc;

  if (segment_target_bytes != 0U) {
    written = snprintf(endpoint, sizeof(endpoint),
                       "pouch://%s?pouch_single_writer=true&"
                       "segment_target_bytes=%" PRIu64,
                       root, segment_target_bytes);
  } else {
    written = snprintf(endpoint, sizeof(endpoint),
                       "pouch://%s?pouch_single_writer=true", root);
  }
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

static int lockdc_bench_open_shared_client(const char *root,
                                           const char *crypto_key,
                                           lc_client **out, lc_error *error) {
  lc_client_config config;
  const char *endpoints[1];
  char endpoint[1200];
  int written;
  int rc;

  written = snprintf(endpoint, sizeof(endpoint),
                     "pouch://%s?pouch_single_writer=false", root);
  if (written <= 0 || (size_t)written >= sizeof(endpoint)) {
    return LC_ERR_INVALID;
  }
  endpoints[0] = endpoint;
  lc_client_config_init(&config);
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "bench";
  config.pouch_crypto_key = crypto_key;
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
  const char payload_prefix[] = ",\"payload\":\"";
  const char payload_suffix[] = "\"}";
  char prefix[4096];
  char chunk[128];
  size_t target;
  size_t prefix_len;
  size_t payload_len;
  size_t payload_end;
  size_t payload_prefix_len;
  size_t payload_suffix_len;
  size_t json_len;
  size_t chunk_len;
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
  payload_prefix_len = strlen(payload_prefix);
  payload_suffix_len = strlen(payload_suffix);
  if (target > prefix_len + payload_prefix_len + payload_suffix_len) {
    payload_len = target - prefix_len - payload_prefix_len - payload_suffix_len;
  } else {
    payload_len = 32U;
  }
  if (payload_len < 32U) {
    payload_len = 32U;
  }
  json = (char *)malloc(prefix_len + payload_prefix_len + payload_len +
                        payload_suffix_len + 1U);
  if (json == NULL) {
    return NULL;
  }
  json_len = 0U;
  memcpy(json + json_len, prefix, prefix_len);
  json_len += prefix_len;
  memcpy(json + json_len, payload_prefix, payload_prefix_len);
  json_len += payload_prefix_len;
  payload_end = json_len + payload_len;
  written = snprintf(chunk, sizeof(chunk),
                     " audit remediation evidence workflow row %ld gen %ld;",
                     row, generation);
  if (written <= 0 || (size_t)written >= sizeof(chunk)) {
    free(json);
    return NULL;
  }
  chunk_len = (size_t)written;
  while (json_len + chunk_len <= payload_end) {
    memcpy(json + json_len, chunk, chunk_len);
    json_len += chunk_len;
  }
  if (json_len < payload_end) {
    memset(json + json_len, ' ', payload_end - json_len);
    json_len = payload_end;
  }
  memcpy(json + json_len, payload_suffix, payload_suffix_len);
  json_len += payload_suffix_len;
  json[json_len] = '\0';
  if (out_len != NULL) {
    *out_len = json_len;
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

static int lockdc_bench_read_key(lc_client *client, const char *key, long row,
                                 long generation, lc_error *error) {
  lc_get_opts opts;
  lc_get_res res;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  int rc;

  memset(&opts, 0, sizeof(opts));
  memset(&res, 0, sizeof(res));
  opts.public_read = 1;
  sink = NULL;
  bytes = NULL;
  length = 0U;
  rc = lc_sink_to_memory(&sink, error);
  if (rc == LC_OK) {
    rc = client->get(client, key, &opts, sink, &res, error);
  }
  if (rc == LC_OK) {
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  }
  if (rc == LC_OK) {
    rc = lockdc_bench_validate_document_fields(bytes, length, row, generation,
                                               error);
  }
  if (sink != NULL) {
    lc_sink_close(sink);
  }
  lc_get_res_cleanup(&res);
  return rc;
}

static int lockdc_bench_read_lease(lc_lease *lease, long row, long generation,
                                   lc_error *error) {
  lc_get_opts opts;
  lc_get_res res;
  lc_sink *sink;
  const void *bytes;
  size_t length;
  int rc;

  if (lease == NULL) {
    return LC_ERR_INVALID;
  }
  lc_get_opts_init(&opts);
  memset(&res, 0, sizeof(res));
  sink = NULL;
  bytes = NULL;
  length = 0U;
  rc = lc_sink_to_memory(&sink, error);
  if (rc == LC_OK) {
    rc = lease->get(lease, sink, &opts, &res, error);
  }
  if (rc == LC_OK) {
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  }
  if (rc == LC_OK) {
    rc = lockdc_bench_validate_document_fields(bytes, length, row, generation,
                                               error);
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
  const void *bytes;
  size_t length;
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
  bytes = NULL;
  length = 0U;
  rc = lc_sink_to_memory(&sink, error);
  if (rc == LC_OK) {
    rc = lease->get_attachment(lease, &get_req, sink, &get_res, error);
  }
  if (rc == LC_OK) {
    rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
  }
  if (rc == LC_OK && (length != sizeof(payload) ||
                      memcmp(bytes, payload, sizeof(payload)) != 0)) {
    (void)error;
    rc = LC_ERR_INVALID;
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
  long dequeued;
  long i;

  dequeued = 0L;
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
    int saw_message;

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
    saw_message = 0;
    for (index = 0U; index < batch.count; ++index) {
      lc_message *message;
      char expected[128];
      lc_sink *sink;
      const void *bytes;
      size_t length;
      int written;

      message = batch.messages[index];
      if (message == NULL) {
        continue;
      }
      saw_message = 1;
      written = snprintf(expected, sizeof(expected),
                         "{\"message\":%ld,\"kind\":\"production\"}", dequeued);
      if (written <= 0 || (size_t)written >= sizeof(expected)) {
        lc_dequeue_batch_cleanup(&batch);
        return LC_ERR_INVALID;
      }
      sink = NULL;
      bytes = NULL;
      length = 0U;
      rc = lc_sink_to_memory(&sink, error);
      if (rc == LC_OK) {
        rc = message->write_payload(message, sink, NULL, error);
      }
      if (rc == LC_OK) {
        rc = lc_sink_memory_bytes(sink, &bytes, &length, error);
      }
      if (rc == LC_OK && (length != strlen(expected) ||
                          memcmp(bytes, expected, length) != 0)) {
        (void)error;
        rc = LC_ERR_INVALID;
      }
      if (sink != NULL) {
        lc_sink_close(sink);
      }
      if (rc != LC_OK) {
        lc_dequeue_batch_cleanup(&batch);
        return rc;
      }
      if (message->ack(message, error) != LC_OK) {
        lc_dequeue_batch_cleanup(&batch);
        return error != NULL && error->code != LC_OK ? error->code
                                                     : LC_ERR_INVALID;
      }
      ++dequeued;
      batch.messages[index] = NULL;
    }
    if (!saw_message) {
      lc_dequeue_batch_cleanup(&batch);
      (void)error;
      return LC_ERR_INVALID;
    }
    lc_dequeue_batch_cleanup(&batch);
  }
  if (dequeued != messages) {
    (void)error;
    return LC_ERR_INVALID;
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

static int lockdc_bench_warm_query(lc_client *client, const char *scenario,
                                   const char *engine, int documents, long rows,
                                   lockdc_pouch_bench_result *out,
                                   lc_error *error) {
  long matched_rows;
  long expected_rows;
  int rc;

  matched_rows = 0L;
  expected_rows = lockdc_bench_expected_query_matches(scenario, rows);
  rc = lockdc_bench_query(client, scenario, engine, documents, rows,
                          &matched_rows, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (matched_rows != expected_rows) {
    if (out != NULL) {
      snprintf(out->error, sizeof(out->error),
               "pouch production warm %s %s query matched %ld rows", scenario,
               engine, matched_rows);
    }
    if (error != NULL) {
      lc_error_cleanup(error);
      lc_error_init(error);
    }
    return LC_ERR_INVALID;
  }
  return LC_OK;
}

static void lockdc_bench_result_set_error(lockdc_pouch_bench_result *out,
                                          const lc_error *error) {
  if (out != NULL && error != NULL && error->message != NULL &&
      error->message[0] != '\0') {
    snprintf(out->error, sizeof(out->error), "%s", error->message);
  }
}

typedef struct lockdc_bench_concurrency_gate {
  pthread_mutex_t mutex;
  pthread_cond_t condition;
  int started;
} lockdc_bench_concurrency_gate;

typedef struct lockdc_bench_concurrency_worker {
  lc_client *client;
  lockdc_bench_concurrency_gate *gate;
  const unsigned char *payload;
  size_t payload_bytes;
  long writer_id;
  long writes_per_writer;
  int same_key;
  int rc;
  uint64_t update_ns;
  uint64_t max_update_ns;
  char error[256];
} lockdc_bench_concurrency_worker;

static void lockdc_bench_concurrency_worker_set_error(
    lockdc_bench_concurrency_worker *worker, const lc_error *error,
    const char *phase) {
  if (worker == NULL) {
    return;
  }
  if (error != NULL && error->message != NULL && error->message[0] != '\0') {
    if (phase != NULL && error->detail != NULL && error->detail[0] != '\0') {
      snprintf(worker->error, sizeof(worker->error), "%s: %s: %s", phase,
               error->message, error->detail);
    } else if (phase != NULL) {
      snprintf(worker->error, sizeof(worker->error), "%s: %s", phase,
               error->message);
    } else if (error->detail != NULL && error->detail[0] != '\0') {
      snprintf(worker->error, sizeof(worker->error), "%s: %s", error->message,
               error->detail);
    } else {
      snprintf(worker->error, sizeof(worker->error), "%s", error->message);
    }
  } else if (phase != NULL) {
    snprintf(worker->error, sizeof(worker->error), "%s", phase);
  }
}

static int
lockdc_bench_concurrency_write(lockdc_bench_concurrency_worker *worker,
                               const char *key, const char *owner,
                               const char **phase, lc_error *error) {
  lc_acquire_req acquire_req;
  lc_release_req release_req;
  lc_update_opts update_opts;
  lc_lease *lease;
  lc_source *source;
  int rc;

  if (worker == NULL || worker->client == NULL || key == NULL ||
      owner == NULL) {
    return LC_ERR_INVALID;
  }
  if (phase != NULL) {
    *phase = "acquire";
  }
  lease = NULL;
  source = NULL;
  lc_acquire_req_init(&acquire_req);
  acquire_req.namespace_name = "bench";
  acquire_req.key = key;
  acquire_req.owner = owner;
  acquire_req.ttl_seconds = 60L;
  /* Match Go disk's bounded blocking acquire under same-key contention. */
  acquire_req.block_seconds = 30L;
  rc = worker->client->acquire(worker->client, &acquire_req, &lease, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (phase != NULL) {
    *phase = "update";
  }
  rc = lc_source_from_memory(worker->payload, worker->payload_bytes, &source,
                             error);
  if (rc == LC_OK) {
    lc_update_opts_init(&update_opts);
    update_opts.content_type = "application/json";
    rc = lease->update(lease, source, &update_opts, error);
  }
  if (source != NULL) {
    lc_source_close(source);
  }
  if (rc == LC_OK) {
    if (phase != NULL) {
      *phase = "release";
    }
    lc_release_req_init(&release_req);
    rc = lease->release(lease, &release_req, error);
    if (rc == LC_OK) {
      /* release() closes a successful public lease. */
      lease = NULL;
    }
  }
  if (lease != NULL) {
    lease->close(lease);
  }
  return rc;
}

static void *lockdc_bench_concurrency_worker_main(void *context) {
  lockdc_bench_concurrency_worker *worker;
  lc_error error;
  long operation;

  worker = (lockdc_bench_concurrency_worker *)context;
  if (worker == NULL || worker->gate == NULL) {
    return NULL;
  }
  lc_error_init(&error);
  if (pthread_mutex_lock(&worker->gate->mutex) != 0) {
    worker->rc = LC_ERR_INVALID;
    snprintf(worker->error, sizeof(worker->error), "lock start gate");
    lc_error_cleanup(&error);
    return NULL;
  }
  while (!worker->gate->started) {
    if (pthread_cond_wait(&worker->gate->condition, &worker->gate->mutex) !=
        0) {
      (void)pthread_mutex_unlock(&worker->gate->mutex);
      worker->rc = LC_ERR_INVALID;
      snprintf(worker->error, sizeof(worker->error), "wait for start gate");
      lc_error_cleanup(&error);
      return NULL;
    }
  }
  (void)pthread_mutex_unlock(&worker->gate->mutex);

  for (operation = 0L; operation < worker->writes_per_writer; ++operation) {
    char key[128];
    char owner[64];
    const char *phase;
    uint64_t start;
    uint64_t end;
    int rc;

    if (worker->same_key != 0) {
      snprintf(key, sizeof(key), "concurrency/shared");
    } else {
      snprintf(key, sizeof(key), "concurrency/w%03ld/k%06ld", worker->writer_id,
               operation);
    }
    snprintf(owner, sizeof(owner), "pouch-concurrency-%03ld",
             worker->writer_id);
    phase = NULL;
    start = lockdc_bench_now_ns();
    rc = lockdc_bench_concurrency_write(worker, key, owner, &phase, &error);
    end = lockdc_bench_now_ns();
    if (rc != LC_OK) {
      worker->rc = rc;
      lockdc_bench_concurrency_worker_set_error(worker, &error, phase);
      lc_error_cleanup(&error);
      return NULL;
    }
    if (end >= start) {
      uint64_t elapsed = end - start;

      worker->update_ns += elapsed;
      if (elapsed > worker->max_update_ns) {
        worker->max_update_ns = elapsed;
      }
    }
  }
  worker->rc = LC_OK;
  lc_error_cleanup(&error);
  return NULL;
}

static int lockdc_bench_concurrency_read_version(lc_client *client,
                                                 const char *key,
                                                 lc_version *version,
                                                 lc_error *error) {
  lc_get_opts options;
  lc_get_res result;
  lc_sink *sink;
  int rc;

  if (client == NULL || key == NULL || version == NULL) {
    return LC_ERR_INVALID;
  }
  memset(&options, 0, sizeof(options));
  memset(&result, 0, sizeof(result));
  options.public_read = 1;
  sink = NULL;
  rc = lc_sink_to_discard(&sink, error);
  if (rc == LC_OK) {
    rc = client->get(client, key, &options, sink, &result, error);
  }
  if (rc == LC_OK && result.no_content) {
    rc = LC_ERR_INVALID;
  }
  if (rc == LC_OK) {
    *version = result.version;
  }
  if (sink != NULL) {
    lc_sink_close(sink);
  }
  lc_get_res_cleanup(&result);
  return rc;
}

int lockdc_pouch_bench_concurrency_run(long writers, long writes_per_writer,
                                       long payload_bytes, int same_key,
                                       int crypto_enabled,
                                       lockdc_pouch_bench_result *out) {
  char root_template[] = LOCKDC_POUCH_BENCH_TMP_PREFIX "XXXXXX";
  char root[sizeof(LOCKDC_POUCH_BENCH_TMP_PREFIX "XXXXXX")];
  lockdc_bench_concurrency_gate gate;
  lockdc_bench_concurrency_worker *workers;
  pthread_t *threads;
  lc_client **clients;
  unsigned char *payload;
  lc_error error;
  char *crypto_key;
  uint64_t start;
  uint64_t end;
  long total_writes;
  long index;
  long started_threads;
  int gate_mutex_initialized;
  int gate_condition_initialized;
  int rc;

  if (out == NULL) {
    return LC_ERR_INVALID;
  }
  memset(out, 0, sizeof(*out));
  if (writers <= 0L) {
    writers = 2L;
  }
  if (writes_per_writer <= 0L) {
    writes_per_writer = 32L;
  }
  if (payload_bytes <= 0L) {
    payload_bytes = 256L;
  }
  if (writers > 64L || writes_per_writer > LONG_MAX / writers ||
      payload_bytes <
          (long)((sizeof(lockdc_bench_concurrency_payload_prefix) - 1U) +
                 (sizeof(lockdc_bench_concurrency_payload_suffix) - 1U)) ||
      payload_bytes > 1024L * 1024L ||
      (uintmax_t)payload_bytes > (uintmax_t)SIZE_MAX) {
    out->rc = LC_ERR_INVALID;
    snprintf(out->error, sizeof(out->error),
             "invalid concurrency benchmark dimensions");
    return out->rc;
  }
  total_writes = writers * writes_per_writer;
  if (total_writes > LONG_MAX / payload_bytes) {
    out->rc = LC_ERR_INVALID;
    snprintf(out->error, sizeof(out->error),
             "concurrency benchmark byte count overflows long");
    return out->rc;
  }
  root[0] = '\0';
  workers = NULL;
  threads = NULL;
  clients = NULL;
  payload = NULL;
  crypto_key = NULL;
  started_threads = 0L;
  gate_mutex_initialized = 0;
  gate_condition_initialized = 0;
  rc = LC_OK;
  lc_error_init(&error);
  if (mkdtemp(root_template) == NULL) {
    out->rc = errno;
    lc_error_cleanup(&error);
    return out->rc;
  }
  snprintf(root, sizeof(root), "%s", root_template);
  payload = (unsigned char *)malloc((size_t)payload_bytes);
  workers = (lockdc_bench_concurrency_worker *)calloc((size_t)writers,
                                                      sizeof(*workers));
  threads = (pthread_t *)calloc((size_t)writers, sizeof(*threads));
  clients = (lc_client **)calloc((size_t)writers, sizeof(*clients));
  if (payload == NULL || workers == NULL || threads == NULL ||
      clients == NULL) {
    rc = LC_ERR_NOMEM;
    goto done;
  }
  memcpy(payload, lockdc_bench_concurrency_payload_prefix,
         sizeof(lockdc_bench_concurrency_payload_prefix) - 1U);
  memset(payload + sizeof(lockdc_bench_concurrency_payload_prefix) - 1U, 'x',
         (size_t)payload_bytes -
             ((sizeof(lockdc_bench_concurrency_payload_prefix) - 1U) +
              (sizeof(lockdc_bench_concurrency_payload_suffix) - 1U)));
  memcpy(payload + (size_t)payload_bytes -
             (sizeof(lockdc_bench_concurrency_payload_suffix) - 1U),
         lockdc_bench_concurrency_payload_suffix,
         sizeof(lockdc_bench_concurrency_payload_suffix) - 1U);
  if (crypto_enabled != 0) {
    rc = lc_pouch_crypto_generate_key_string(&crypto_key, &error);
    if (rc != LC_OK) {
      goto done;
    }
  }
  for (index = 0L; index < writers; ++index) {
    rc = lockdc_bench_open_shared_client(root, crypto_key, &clients[index],
                                         &error);
    if (rc != LC_OK) {
      goto done;
    }
  }
  memset(&gate, 0, sizeof(gate));
  if (pthread_mutex_init(&gate.mutex, NULL) != 0) {
    rc = LC_ERR_INVALID;
    snprintf(out->error, sizeof(out->error), "initialize start gate mutex");
    goto done;
  }
  gate_mutex_initialized = 1;
  if (pthread_cond_init(&gate.condition, NULL) != 0) {
    rc = LC_ERR_INVALID;
    snprintf(out->error, sizeof(out->error), "initialize start gate condition");
    goto done;
  }
  gate_condition_initialized = 1;
  for (index = 0L; index < writers; ++index) {
    workers[index].client = clients[index];
    workers[index].gate = &gate;
    workers[index].payload = payload;
    workers[index].payload_bytes = (size_t)payload_bytes;
    workers[index].writer_id = index;
    workers[index].writes_per_writer = writes_per_writer;
    workers[index].same_key = same_key != 0 ? 1 : 0;
    if (pthread_create(&threads[index], NULL,
                       lockdc_bench_concurrency_worker_main,
                       &workers[index]) != 0) {
      rc = LC_ERR_INVALID;
      snprintf(out->error, sizeof(out->error), "create concurrent writer");
      goto join_started_threads;
    }
    ++started_threads;
  }
  start = lockdc_bench_now_ns();
  if (pthread_mutex_lock(&gate.mutex) != 0) {
    rc = LC_ERR_INVALID;
    snprintf(out->error, sizeof(out->error), "unlock start gate");
    goto join_started_threads;
  }
  gate.started = 1;
  (void)pthread_cond_broadcast(&gate.condition);
  (void)pthread_mutex_unlock(&gate.mutex);

join_started_threads:
  if (!gate.started && gate_mutex_initialized && gate_condition_initialized) {
    if (pthread_mutex_lock(&gate.mutex) == 0) {
      gate.started = 1;
      (void)pthread_cond_broadcast(&gate.condition);
      (void)pthread_mutex_unlock(&gate.mutex);
    }
  }
  for (index = 0L; index < started_threads; ++index) {
    (void)pthread_join(threads[index], NULL);
  }
  end = lockdc_bench_now_ns();
  if (rc != LC_OK) {
    goto done;
  }
  out->c_ns = end >= start ? end - start : 0U;
  out->rows = writers;
  out->writes = total_writes;
  out->bytes = total_writes * payload_bytes;
  for (index = 0L; index < writers; ++index) {
    if (workers[index].rc != LC_OK) {
      rc = workers[index].rc;
      snprintf(out->error, sizeof(out->error), "writer %ld: %.200s", index,
               workers[index].error[0] != '\0' ? workers[index].error
                                               : "concurrent write failed");
      goto done;
    }
    out->update_ns += workers[index].update_ns;
    if (workers[index].max_update_ns > out->max_update_ns) {
      out->max_update_ns = workers[index].max_update_ns;
    }
  }
  if (same_key != 0) {
    lc_version version;

    rc = lockdc_bench_concurrency_read_version(clients[0], "concurrency/shared",
                                               &version, &error);
    if (rc != LC_OK || version != (lc_version)total_writes) {
      if (rc == LC_OK) {
        rc = LC_ERR_INVALID;
        snprintf(out->error, sizeof(out->error),
                 "shared key version %lld, expected %ld", (long long)version,
                 total_writes);
      }
      goto done;
    }
  } else {
    for (index = 0L; index < writers; ++index) {
      long operation;

      for (operation = 0L; operation < writes_per_writer; ++operation) {
        char key[128];
        lc_version version;

        snprintf(key, sizeof(key), "concurrency/w%03ld/k%06ld", index,
                 operation);
        rc = lockdc_bench_concurrency_read_version(clients[0], key, &version,
                                                   &error);
        if (rc != LC_OK || version != 1) {
          if (rc == LC_OK) {
            rc = LC_ERR_INVALID;
            snprintf(out->error, sizeof(out->error),
                     "independent key %s version %lld, expected 1", key,
                     (long long)version);
          }
          goto done;
        }
      }
    }
  }
  out->segments = lockdc_bench_count_segments_in(root);

done:
  if (rc != LC_OK && out->error[0] == '\0') {
    lockdc_bench_result_set_error(out, &error);
  }
  if (gate_condition_initialized) {
    (void)pthread_cond_destroy(&gate.condition);
  }
  if (gate_mutex_initialized) {
    (void)pthread_mutex_destroy(&gate.mutex);
  }
  if (clients != NULL) {
    for (index = 0L; index < writers; ++index) {
      if (clients[index] != NULL) {
        lc_client_close(clients[index]);
      }
    }
  }
  lc_pouch_crypto_key_string_free(crypto_key);
  free(payload);
  free(clients);
  free(threads);
  free(workers);
  lockdc_bench_cleanup_root(root);
  lc_error_cleanup(&error);
  out->rc = rc;
  return rc;
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
  rc = lockdc_bench_open_client(fixture->root, NULL, NULL, 0U, &fixture->client,
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
                                      long payload_bytes,
                                      uint64_t segment_target_bytes,
                                      int crypto_enabled,
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
  const char *phase;
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
  phase = "start";
  root[0] = '\0';
  lc_error_init(&error);
  if (mkdtemp(root_template) == NULL) {
    out->rc = errno;
    return out->rc;
  }
  snprintf(root, sizeof(root), "%s", root_template);
  start = lockdc_bench_now_ns();
  if (crypto_enabled != 0) {
    phase = "generate crypto key";
    rc = lc_pouch_crypto_generate_key_string(&crypto_key, &error);
    if (rc != LC_OK) {
      goto done;
    }
  }
  phase = "open client";
  rc = lockdc_bench_open_client(root, crypto_key,
                                compression_enabled != 0 ? "zlib" : NULL,
                                segment_target_bytes, &client, &error);
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
    phase = "acquire lease";
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
      phase = "update lease";
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
        phase = "flush intermediate";
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
      phase = "stale update";
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
      phase = "attachment roundtrip";
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
      phase = "get lease";
      phase_start = lockdc_bench_now_ns();
      rc = lockdc_bench_read_lease(lease, row, updates_per_key - 1L, &error);
      if (rc != LC_OK) {
        lease->close(lease);
        goto done;
      }
      lockdc_bench_add_ns(&out->get_lease_ns, phase_start,
                          lockdc_bench_now_ns());
      out->reads++;
    }
    lc_release_req_init(&release_req);
    phase = "release lease";
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
  phase = "queue roundtrip";
  rc = lockdc_bench_queue_roundtrip(client, queue_messages, out, &error);
  if (rc != LC_OK) {
    goto done;
  }
  lockdc_bench_add_ns(&out->queue_ns, phase_start, lockdc_bench_now_ns());
  phase_start = lockdc_bench_now_ns();
  phase = "flush final";
  rc = lockdc_bench_flush(client, &error);
  if (rc != LC_OK) {
    goto done;
  }
  lockdc_bench_add_phase_ns(&out->flush_final_ns, &out->flush_ns, phase_start,
                            lockdc_bench_now_ns());
  phase_start = lockdc_bench_now_ns();
  phase = "flush noop";
  rc = lockdc_bench_flush(client, &error);
  if (rc != LC_OK) {
    goto done;
  }
  lockdc_bench_add_phase_ns(&out->flush_noop_ns, &out->flush_ns, phase_start,
                            lockdc_bench_now_ns());
  phase_start = lockdc_bench_now_ns();
  lc_client_close(client);
  client = NULL;
  phase = "reopen client";
  rc = lockdc_bench_open_client(root, crypto_key,
                                compression_enabled != 0 ? "zlib" : NULL,
                                segment_target_bytes, &client, &error);
  if (rc != LC_OK) {
    goto done;
  }
  lockdc_bench_add_ns(&out->reopen_ns, phase_start, lockdc_bench_now_ns());
  phase_start = lockdc_bench_now_ns();
  phase = "flush reopen";
  rc = lockdc_bench_flush(client, &error);
  if (rc != LC_OK) {
    goto done;
  }
  lockdc_bench_add_phase_ns(&out->flush_reopen_ns, &out->flush_ns, phase_start,
                            lockdc_bench_now_ns());
  phase_start = lockdc_bench_now_ns();
  phase = "RangeHalf index keys";
  rc = lockdc_bench_query(client, "RangeHalf", "index", 0, rows, &out->rows,
                          &error);
  if (rc != LC_OK) {
    goto done;
  }
  if (out->rows != lockdc_bench_expected_query_matches("RangeHalf", rows)) {
    rc = LC_ERR_INVALID;
    snprintf(out->error, sizeof(out->error),
             "pouch production RangeHalf index query matched %ld rows",
             out->rows);
    lc_error_cleanup(&error);
    lc_error_init(&error);
    goto done;
  }
  lockdc_bench_add_ns(&out->index_query_keys_ns, phase_start,
                      lockdc_bench_now_ns());
  phase = "NarrativeSummary warm index docs";
  rc = lockdc_bench_warm_query(client, "NarrativeSummary", "index", 1, rows,
                               out, &error);
  if (rc != LC_OK) {
    goto done;
  }
  matched_rows = 0L;
  phase_start = lockdc_bench_now_ns();
  phase = "NarrativeSummary index docs";
  rc = lockdc_bench_query(client, "NarrativeSummary", "index", 1, rows,
                          &matched_rows, &error);
  if (rc != LC_OK) {
    goto done;
  }
  if (matched_rows !=
      lockdc_bench_expected_query_matches("NarrativeSummary", rows)) {
    rc = LC_ERR_INVALID;
    snprintf(out->error, sizeof(out->error),
             "pouch production NarrativeSummary index query matched %ld rows",
             matched_rows);
    lc_error_cleanup(&error);
    lc_error_init(&error);
    goto done;
  }
  lockdc_bench_add_ns(&out->index_query_docs_ns, phase_start,
                      lockdc_bench_now_ns());
  matched_rows = 0L;
  phase_start = lockdc_bench_now_ns();
  phase = "WorkflowEscalated scan keys";
  rc = lockdc_bench_query(client, "WorkflowEscalated", "scan", 0, rows,
                          &matched_rows, &error);
  if (rc != LC_OK) {
    goto done;
  }
  if (matched_rows !=
      lockdc_bench_expected_query_matches("WorkflowEscalated", rows)) {
    rc = LC_ERR_INVALID;
    snprintf(out->error, sizeof(out->error),
             "pouch production WorkflowEscalated scan query matched %ld rows",
             matched_rows);
    lc_error_cleanup(&error);
    lc_error_init(&error);
    goto done;
  }
  lockdc_bench_add_ns(&out->scan_query_keys_ns, phase_start,
                      lockdc_bench_now_ns());
  phase = "NarrativeDescription warm scan docs";
  rc = lockdc_bench_warm_query(client, "NarrativeDescription", "scan", 1, rows,
                               out, &error);
  if (rc != LC_OK) {
    goto done;
  }
  matched_rows = 0L;
  phase_start = lockdc_bench_now_ns();
  phase = "NarrativeDescription scan docs";
  rc = lockdc_bench_query(client, "NarrativeDescription", "scan", 1, rows,
                          &matched_rows, &error);
  if (rc != LC_OK) {
    goto done;
  }
  if (matched_rows !=
      lockdc_bench_expected_query_matches("NarrativeDescription", rows)) {
    rc = LC_ERR_INVALID;
    snprintf(
        out->error, sizeof(out->error),
        "pouch production NarrativeDescription scan query matched %ld rows",
        matched_rows);
    lc_error_cleanup(&error);
    lc_error_init(&error);
    goto done;
  }
  lockdc_bench_add_ns(&out->scan_query_docs_ns, phase_start,
                      lockdc_bench_now_ns());
  matched_rows = 0L;
  phase_start = lockdc_bench_now_ns();
  phase = "FullTextAny index keys";
  rc = lockdc_bench_query(client, "FullTextAny", "index", 0, rows,
                          &matched_rows, &error);
  if (rc != LC_OK) {
    goto done;
  }
  if (matched_rows !=
      lockdc_bench_expected_query_matches("FullTextAny", rows)) {
    rc = LC_ERR_INVALID;
    snprintf(out->error, sizeof(out->error),
             "pouch production FullTextAny index query matched %ld rows",
             matched_rows);
    lc_error_cleanup(&error);
    lc_error_init(&error);
    goto done;
  }
  lockdc_bench_add_ns(&out->full_text_index_keys_ns, phase_start,
                      lockdc_bench_now_ns());
  phase = "FullTextAny warm scan docs";
  rc = lockdc_bench_warm_query(client, "FullTextAny", "scan", 1, rows, out,
                               &error);
  if (rc != LC_OK) {
    goto done;
  }
  matched_rows = 0L;
  phase_start = lockdc_bench_now_ns();
  phase = "FullTextAny scan docs";
  rc = lockdc_bench_query(client, "FullTextAny", "scan", 1, rows, &matched_rows,
                          &error);
  if (rc != LC_OK) {
    goto done;
  }
  if (matched_rows !=
      lockdc_bench_expected_query_matches("FullTextAny", rows)) {
    rc = LC_ERR_INVALID;
    snprintf(out->error, sizeof(out->error),
             "pouch production FullTextAny scan query matched %ld rows",
             matched_rows);
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
    phase = "get public";
    rc = lockdc_bench_read_key(client, key, row, updates_per_key - 1L, &error);
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
             "multiple segments with %" PRIu64 "-byte segment target",
             out->segments,
             segment_target_bytes != 0U ? segment_target_bytes
                                        : (uint64_t)(64U * 1024U * 1024U));
    goto done_without_error_message;
  }

done:
  if (rc != LC_OK) {
    lockdc_bench_result_set_error(out, &error);
    if (out->error[0] == '\0') {
      snprintf(out->error, sizeof(out->error),
               "pouch production benchmark failed during %s", phase);
    }
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

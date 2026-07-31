#ifndef LOCKDC_BENCHMARK_POUCHBENCH_H
#define LOCKDC_BENCHMARK_POUCHBENCH_H

#include <stdint.h>

typedef struct lockdc_pouch_bench_result {
  int rc;
  long rows;
  long writes;
  long reads;
  long attachments;
  long queue_messages;
  long stale_failures;
  long segments;
  long snapshots;
  long candidate_segments;
  long compactions;
  long bytes;
  long candidate_bytes;
  uint64_t c_ns;
  uint64_t acquire_ns;
  uint64_t update_ns;
  uint64_t max_update_ns;
  uint64_t release_ns;
  uint64_t stale_ns;
  uint64_t attachment_ns;
  uint64_t queue_ns;
  uint64_t flush_ns;
  uint64_t flush_intermediate_ns;
  uint64_t flush_final_ns;
  uint64_t flush_noop_ns;
  uint64_t flush_reopen_ns;
  uint64_t compaction_ns;
  uint64_t reopen_ns;
  uint64_t restart_recovery_ns;
  uint64_t get_public_ns;
  uint64_t get_lease_ns;
  uint64_t index_query_keys_ns;
  uint64_t index_query_docs_ns;
  uint64_t scan_query_keys_ns;
  uint64_t scan_query_docs_ns;
  uint64_t full_text_index_keys_ns;
  uint64_t full_text_scan_docs_ns;
  char error[256];
} lockdc_pouch_bench_result;

typedef struct lockdc_pouch_bench_fixture lockdc_pouch_bench_fixture;

int lockdc_pouch_bench_fixture_open(long rows, lockdc_pouch_bench_fixture **out,
                                    lockdc_pouch_bench_result *result);
int lockdc_pouch_bench_fixture_query(lockdc_pouch_bench_fixture *fixture,
                                     const char *scenario, const char *engine,
                                     int documents,
                                     lockdc_pouch_bench_result *out);
void lockdc_pouch_bench_fixture_close(lockdc_pouch_bench_fixture *fixture);

int lockdc_pouch_bench_run(const char *scenario, long rows, const char *engine,
                           int documents, lockdc_pouch_bench_result *out);

int lockdc_pouch_bench_production_run(long rows, long updates_per_key,
                                      long payload_bytes,
                                      uint64_t segment_target_bytes,
                                      int crypto_enabled,
                                      int compression_enabled,
                                      lockdc_pouch_bench_result *out);

int lockdc_pouch_bench_compaction_run(
    long rows, long updates_per_key, long payload_bytes,
    long segment_target_bytes, long compaction_min_segment_count,
    long compaction_min_reclaimable_bytes, int scheduled, int crypto_enabled,
    int compression_enabled, lockdc_pouch_bench_result *out);

int lockdc_pouch_bench_concurrency_run(long writers, long writes_per_writer,
                                       long payload_bytes, int same_key,
                                       int crypto_enabled,
                                       lockdc_pouch_bench_result *out);

#endif

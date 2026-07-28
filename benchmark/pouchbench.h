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
  long bytes;
  uint64_t c_ns;
  uint64_t acquire_ns;
  uint64_t update_ns;
  uint64_t release_ns;
  uint64_t stale_ns;
  uint64_t attachment_ns;
  uint64_t queue_ns;
  uint64_t flush_ns;
  uint64_t reopen_ns;
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
                                      lockdc_pouch_bench_result *out);

#endif

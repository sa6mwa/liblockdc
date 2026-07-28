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

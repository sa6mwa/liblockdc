#ifndef LOCKDC_BENCHMARK_POUCHBENCH_H
#define LOCKDC_BENCHMARK_POUCHBENCH_H

#include <stdint.h>

typedef struct lockdc_pouch_bench_result {
  int rc;
  long rows;
  uint64_t c_ns;
  char error[256];
} lockdc_pouch_bench_result;

int lockdc_pouch_bench_run(const char *scenario, long rows, const char *engine,
                           int documents, lockdc_pouch_bench_result *out);

#endif

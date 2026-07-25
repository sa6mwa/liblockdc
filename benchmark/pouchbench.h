#ifndef LOCKDC_BENCHMARK_POUCHBENCH_H
#define LOCKDC_BENCHMARK_POUCHBENCH_H

#include <stdint.h>

typedef struct lockdc_pouch_bench_result {
  uint64_t iterations;
  uint64_t operations;
  uint64_t rows;
  uint64_t bytes;
  uint64_t pages;
  uint64_t c_elapsed_ns;
  uint64_t index_seq;
  char error[256];
} lockdc_pouch_bench_result;

int lockdc_pouch_bench_indexed_lql_rows(const char *root, uint64_t iterations,
                                        uint64_t seeded_rows,
                                        lockdc_pouch_bench_result *out);
int lockdc_pouch_bench_indexed_lql_keys(const char *root, uint64_t iterations,
                                        uint64_t seeded_rows,
                                        lockdc_pouch_bench_result *out);
int lockdc_pouch_bench_indexed_lql_scenario_rows(
    const char *root, const char *scenario, uint64_t iterations,
    uint64_t seeded_rows, lockdc_pouch_bench_result *out);
int lockdc_pouch_bench_indexed_lql_scenario_keys(
    const char *root, const char *scenario, uint64_t iterations,
    uint64_t seeded_rows, lockdc_pouch_bench_result *out);
int lockdc_pouch_bench_lql_scenario_rows(
    const char *root, const char *scenario, const char *engine,
    uint64_t iterations, uint64_t seeded_rows, lockdc_pouch_bench_result *out);
int lockdc_pouch_bench_lql_scenario_keys(
    const char *root, const char *scenario, const char *engine,
    uint64_t iterations, uint64_t seeded_rows, lockdc_pouch_bench_result *out);
int lockdc_pouch_bench_state_write(const char *root, uint64_t iterations,
                                   lockdc_pouch_bench_result *out);
int lockdc_pouch_bench_state_read(const char *root, uint64_t iterations,
                                  lockdc_pouch_bench_result *out);

#endif

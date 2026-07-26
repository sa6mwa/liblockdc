#ifndef LOCKDC_BENCHMARK_POUCHBENCH_H
#define LOCKDC_BENCHMARK_POUCHBENCH_H

#include <stdint.h>

typedef struct lockdc_pouch_bench_result {
  uint64_t iterations;
  uint64_t operations;
  uint64_t documents;
  uint64_t bytes;
  uint64_t pages;
  uint64_t first_page_elapsed_ns;
  uint64_t first_page_count;
  uint64_t next_page_elapsed_ns;
  uint64_t next_page_count;
  uint64_t query_candidates;
  uint64_t query_candidate_pages;
  uint64_t result_cache_entries;
  uint64_t result_cache_hits;
  uint64_t result_cache_misses;
  uint64_t result_cache_puts;
  uint64_t c_elapsed_ns;
  uint64_t index_seq;
  char error[256];
} lockdc_pouch_bench_result;

typedef struct lockdc_pouch_bench_env lockdc_pouch_bench_env;

int lockdc_pouch_bench_indexed_lql_documents(const char *root,
                                             uint64_t iterations,
                                             uint64_t seeded_documents,
                                             lockdc_pouch_bench_result *out);
int lockdc_pouch_bench_indexed_lql_keys(const char *root, uint64_t iterations,
                                        uint64_t seeded_documents,
                                        lockdc_pouch_bench_result *out);
int lockdc_pouch_bench_indexed_lql_scenario_documents(
    const char *root, const char *scenario, uint64_t iterations,
    uint64_t seeded_documents, lockdc_pouch_bench_result *out);
int lockdc_pouch_bench_indexed_lql_scenario_keys(
    const char *root, const char *scenario, uint64_t iterations,
    uint64_t seeded_documents, lockdc_pouch_bench_result *out);
int lockdc_pouch_bench_lql_scenario_documents(const char *root,
                                              const char *scenario,
                                              const char *engine,
                                              uint64_t iterations,
                                              uint64_t seeded_documents,
                                              lockdc_pouch_bench_result *out);
int lockdc_pouch_bench_lql_scenario_keys(const char *root, const char *scenario,
                                         const char *engine,
                                         uint64_t iterations,
                                         uint64_t seeded_documents,
                                         lockdc_pouch_bench_result *out);
int lockdc_pouch_bench_open_lql_env(const char *root, uint64_t seeded_documents,
                                    lockdc_pouch_bench_env **env_out,
                                    lockdc_pouch_bench_result *out);
void lockdc_pouch_bench_env_close(lockdc_pouch_bench_env *env);
int lockdc_pouch_bench_lql_env_scenario_documents(
    lockdc_pouch_bench_env *env, const char *scenario, const char *engine,
    uint64_t iterations, lockdc_pouch_bench_result *out);
int lockdc_pouch_bench_lql_env_scenario_keys(lockdc_pouch_bench_env *env,
                                             const char *scenario,
                                             const char *engine,
                                             uint64_t iterations,
                                             lockdc_pouch_bench_result *out);
int lockdc_pouch_bench_state_write(const char *root, uint64_t iterations,
                                   lockdc_pouch_bench_result *out);
int lockdc_pouch_bench_state_read(const char *root, uint64_t iterations,
                                  lockdc_pouch_bench_result *out);

#endif

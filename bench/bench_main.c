#include "lc/lc.h"
#include "lc_pouch.h"
#include "../tests/support/lc_test_tmp.h"

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

static const bench_case *bench_cases(void) {
  static const bench_case cases[] = {
      {"stream-copy", 1000L, bench_stream_copy},
      {"pouch-open", 1000L, bench_pouch_open},
      {"pouch-namespace", 1000L, bench_pouch_namespace},
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
  printf("%s iterations=%ld seconds=%.6f per_op_us=%.3f rc=%d\n",
         bench->name, iterations, elapsed,
         iterations > 0L ? (elapsed * 1000000.0) / (double)iterations : 0.0,
         rc);
  return rc;
}

int main(int argc, char **argv) {
  const bench_case *bench;
  long iterations;
  const char *name;
  int failed;

  if (argc > 1 && (strcmp(argv[1], "--help") == 0 ||
                   strcmp(argv[1], "-h") == 0)) {
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

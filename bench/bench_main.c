#include "lc/lc.h"
#include "lc_mutate_stream.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct bench_case {
  const char *name;
  int (*run)(long iterations);
} bench_case;

static double bench_now_seconds(void) {
  struct timespec ts;

  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
    return 0.0;
  }
  return (double)ts.tv_sec + ((double)ts.tv_nsec / 1000000000.0);
}

static int bench_mutate_open(void *context, const char *resolved_path,
                             lc_source **out, lc_error *error) {
  static const unsigned char binary_payload[] = {0x00, 0x01, 0x02, 0xff, 'a'};
  const void *bytes;
  size_t length;

  (void)context;
  bytes = (const void *)"stream-text";
  length = strlen((const char *)bytes);
  if (resolved_path != NULL && (strstr(resolved_path, ".bin") != NULL ||
                                strstr(resolved_path, "base64") != NULL)) {
    bytes = (const void *)binary_payload;
    length = sizeof(binary_payload);
  }
  return lc_source_from_memory(bytes, length, out, error);
}

static int bench_stream_copy(long iterations) {
  static const char payload[] =
      "{\"key\":\"orders/"
      "42\",\"state\":{\"items\":[1,2,3],\"owner\":\"bench\"}}";
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

static int bench_json_stream(long iterations) {
  static const char json_payload[] =
      "{\"items\":[{\"id\":1},{\"id\":2},{\"id\":3}],\"owner\":\"bench\"}";
  unsigned char scratch[257];
  lc_error error;
  long i;

  lc_error_init(&error);
  for (i = 0; i < iterations; ++i) {
    lc_source *source;
    lc_json *json;
    size_t got;

    source = NULL;
    json = NULL;
    if (lc_source_from_memory(json_payload, sizeof(json_payload) - 1U, &source,
                              &error) != LC_OK) {
      lc_error_cleanup(&error);
      return 1;
    }
    if (lc_json_from_source(source, &json, &error) != LC_OK) {
      lc_source_close(source);
      lc_error_cleanup(&error);
      return 1;
    }
    source = NULL;
    got = json->read(json, scratch, sizeof(scratch) - 1U, &error);
    if (got >= sizeof(scratch)) {
      got = sizeof(scratch) - 1U;
    }
    scratch[got] = '\0';
    if (json->reset(json, &error) != LC_OK) {
      lc_json_close(json);
      lc_error_cleanup(&error);
      return 1;
    }
    (void)json->read(json, scratch, 64U, &error);
    lc_json_close(json);
  }
  lc_error_cleanup(&error);
  return 0;
}

static int bench_mutate_parse(long iterations) {
  static const char *exprs[] = {"/name=bench", "/counter=3",
                                "textfile:/nested/value=payload.txt",
                                "base64file:/blob=blob.bin"};
  lc_mutation_parse_options options;
  lc_file_value_resolver resolver;
  lc_error error;
  long i;

  memset(&options, 0, sizeof(options));
  memset(&resolver, 0, sizeof(resolver));
  resolver.open = bench_mutate_open;
  options.file_value_base_dir = "/virtual";
  options.file_value_resolver = &resolver;
  options.now.tv_sec = 1700000000;
  options.now.tv_nsec = 123456789L;
  options.has_now = 1;
  lc_error_init(&error);

  for (i = 0; i < iterations; ++i) {
    lc_mutation_plan *plan;

    plan = NULL;
    if (lc_mutation_plan_build(exprs, sizeof(exprs) / sizeof(exprs[0]),
                               &options, &plan, &error) != LC_OK) {
      lc_error_cleanup(&error);
      return 1;
    }
    lc_mutation_plan_close(plan);
  }

  lc_error_cleanup(&error);
  return 0;
}

static int bench_mutate_apply(long iterations) {
  static const char *exprs[] = {"/name=bench", "/counter=3",
                                "textfile:/nested/value=payload.txt"};
  static const char input_json[] =
      "{\"counter\":1,\"nested\":{\"old\":\"value\"},\"name\":\"before\"}";
  lc_mutation_parse_options options;
  lc_file_value_resolver resolver;
  lc_mutation_plan *plan;
  lc_error error;
  long i;

  memset(&options, 0, sizeof(options));
  memset(&resolver, 0, sizeof(resolver));
  resolver.open = bench_mutate_open;
  options.file_value_base_dir = "/virtual";
  options.file_value_resolver = &resolver;
  options.now.tv_sec = 1700000000;
  options.now.tv_nsec = 123456789L;
  options.has_now = 1;
  plan = NULL;
  lc_error_init(&error);

  if (lc_mutation_plan_build(exprs, sizeof(exprs) / sizeof(exprs[0]), &options,
                             &plan, &error) != LC_OK) {
    lc_error_cleanup(&error);
    return 1;
  }

  for (i = 0; i < iterations; ++i) {
    FILE *input;
    FILE *output;

    input = tmpfile();
    output = NULL;
    if (input == NULL) {
      lc_mutation_plan_close(plan);
      lc_error_cleanup(&error);
      return 1;
    }
    if (fwrite(input_json, 1U, sizeof(input_json) - 1U, input) !=
        sizeof(input_json) - 1U) {
      fclose(input);
      lc_mutation_plan_close(plan);
      lc_error_cleanup(&error);
      return 1;
    }
    fflush(input);
    rewind(input);
    if (lc_mutation_plan_apply(plan, input, &output, &error) != LC_OK) {
      if (output != NULL) {
        fclose(output);
      }
      fclose(input);
      lc_mutation_plan_close(plan);
      lc_error_cleanup(&error);
      return 1;
    }
    if (output != NULL) {
      fclose(output);
    }
    fclose(input);
  }

  lc_mutation_plan_close(plan);
  lc_error_cleanup(&error);
  return 0;
}

static int run_case(const bench_case *test_case, long iterations) {
  double start_seconds;
  double end_seconds;
  double elapsed_seconds;
  double ops_per_sec;
  double ns_per_op;

  start_seconds = bench_now_seconds();
  if (test_case->run(iterations) != 0) {
    return 1;
  }
  end_seconds = bench_now_seconds();
  elapsed_seconds = end_seconds - start_seconds;
  if (elapsed_seconds <= 0.0) {
    elapsed_seconds = 0.000000001;
  }

  ops_per_sec = (double)iterations / elapsed_seconds;
  ns_per_op = (elapsed_seconds * 1000000000.0) / (double)iterations;
  printf("%-16s %12ld %14.2f %14.2f\n", test_case->name, iterations,
         ops_per_sec, ns_per_op);
  return 0;
}

static void print_usage(const char *argv0) {
  fprintf(
      stderr,
      "usage: %s [iterations] [all|streams|json|mutate-parse|mutate-apply]\n",
      argv0);
}

int main(int argc, char **argv) {
  static const bench_case bench_cases[] = {
      {"streams", bench_stream_copy},
      {"json", bench_json_stream},
      {"mutate-parse", bench_mutate_parse},
      {"mutate-apply", bench_mutate_apply}};
  const char *scenario;
  long iterations;
  size_t i;
  int ran;

  iterations = 200000L;
  scenario = "all";
  if (argc >= 2) {
    iterations = strtol(argv[1], NULL, 10);
  }
  if (argc >= 3) {
    scenario = argv[2];
  }
  if (iterations <= 0L) {
    print_usage(argv[0]);
    return 2;
  }

  printf("%-16s %12s %14s %14s\n", "benchmark", "iterations", "ops/sec",
         "ns/op");

  ran = 0;
  for (i = 0U; i < sizeof(bench_cases) / sizeof(bench_cases[0]); ++i) {
    if (strcmp(scenario, "all") != 0 &&
        strcmp(scenario, bench_cases[i].name) != 0) {
      continue;
    }
    ran = 1;
    if (run_case(&bench_cases[i], iterations) != 0) {
      return 1;
    }
  }

  if (!ran) {
    print_usage(argv[0]);
    return 2;
  }

  return 0;
}

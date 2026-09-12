#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <lc/lc.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static lc_error error;

static size_t dimension(const char *value) {
  char *end = NULL;
  unsigned long parsed;
  errno = 0;
  parsed = strtoul(value, &end, 10);
  if (errno != 0 || end == value || *end != '\0' || *value == '-' ||
      parsed == 0 || parsed > 1000000UL) {
    fprintf(stderr, "Fixture dimensions must be integers in [1, 1000000]\n");
    exit(2);
  }
  return (size_t)parsed;
}

static void check(int rc) {
  if (rc != LC_OK) {
    fprintf(stderr, "Pouch operation failed (%d): %s\n", rc,
            error.message != NULL ? error.message : "no detail");
    exit(1);
  }
}

static void sample(const char *phase) {
  struct timespec wall;
  struct timespec cpu;
  double bytes = 0, calls = 0;
  char line[128];
  FILE *fp = fopen("/proc/self/io", "r");
  if (fp == NULL) {
    perror("/proc/self/io");
    exit(1);
  }
  while (fgets(line, sizeof(line), fp) != NULL) {
    if (sscanf(line, "rchar: %lf", &bytes) == 1) {
      continue;
    }
    (void)sscanf(line, "syscr: %lf", &calls);
  }
  fclose(fp);
  if (clock_gettime(CLOCK_MONOTONIC, &wall) != 0 ||
      clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpu) != 0) {
    perror("clock_gettime");
    exit(1);
  }
  printf("%s %.9f %.9f %.0f %.0f\n", phase,
         (double)wall.tv_sec + (double)wall.tv_nsec / 1e9,
         (double)cpu.tv_sec + (double)cpu.tv_nsec / 1e9, bytes, calls);
  fflush(stdout);
}

int main(int argc, char **argv) {
  lc_client_config config;
  lc_client *client = NULL;
  const char *endpoint;
  size_t keys, updates, namespaces, i, total;
  int seed;
  if (argc != 8) {
    fprintf(stderr,
            "usage: %s seed|probe ENDPOINT KEYS UPDATES NAMESPACES "
            "KEYFILE clean|unclean|staged\n",
            argv[0]);
    return 2;
  }
  seed = strcmp(argv[1], "seed") == 0;
  keys = dimension(argv[3]);
  updates = dimension(argv[4]);
  namespaces = dimension(argv[5]);
  if ((!seed && strcmp(argv[1], "probe") != 0) ||
      (strcmp(argv[7], "clean") != 0 && strcmp(argv[7], "unclean") != 0 &&
       strcmp(argv[7], "staged") != 0) ||
      keys > (size_t)LONG_MAX / namespaces ||
      keys * namespaces > (size_t)LONG_MAX / updates) {
    return 2;
  }
  lc_error_init(&error);
  lc_client_config_init(&config);
  endpoint = argv[2];
  config.endpoints = &endpoint;
  config.endpoint_count = 1;
  if (strcmp(argv[6], "-") != 0) {
    config.pouch_crypto_key_file = argv[6];
    config.pouch_crypto_generate_key_file = seed;
  }
  sample("before");
  check(lc_client_open(&config, &client, &error));
  sample("open");
  total = keys * namespaces * (seed ? updates : 1);
  for (i = 0; i < total; ++i) {
    char key[64], ns[64], body[128];
    lc_acquire_req acquire;
    lc_release_req release;
    lc_lease *lease = NULL;
    size_t generation = seed ? i / (keys * namespaces) : updates - 1;
    snprintf(key, sizeof(key), "key-%lu", (unsigned long)(i % keys));
    snprintf(ns, sizeof(ns), "namespace-%lu",
             (unsigned long)((i / keys) % namespaces));
    snprintf(body, sizeof(body), "{\"key\":%lu,\"generation\":%lu}",
             (unsigned long)(i % keys), (unsigned long)generation);
    lc_acquire_req_init(&acquire);
    acquire.key = key;
    acquire.namespace_name = ns;
    acquire.owner = "replay-probe";
    acquire.ttl_seconds = 60;
    check(client->acquire(client, &acquire, &lease, &error));
    if (seed) {
      lc_source *source = NULL;
      check(lc_source_from_memory(body, strlen(body), &source, &error));
      check(lease->update(lease, source, NULL, &error));
      lc_source_close(source);
    } else {
      lc_sink *sink = NULL;
      lc_get_res result;
      const void *bytes = NULL;
      size_t length = 0;
      memset(&result, 0, sizeof(result));
      check(lc_sink_to_memory(&sink, &error));
      check(lease->get(lease, sink, NULL, &result, &error));
      check(lc_sink_memory_bytes(sink, &bytes, &length, &error));
      if (length != strlen(body) || memcmp(bytes, body, length) != 0 ||
          lease->version != (lc_version)updates) {
        fprintf(stderr, "Restored state/version mismatch: %s/%s\n", ns, key);
        return 1;
      }
      lc_get_res_cleanup(&result);
      lc_sink_close(sink);
    }
    lc_release_req_init(&release);
    check(lease->release(lease, &release, &error));
    if (i == 0) {
      sample("first");
    }
  }
  sample("operations");
  if (seed && strcmp(argv[7], "staged") == 0) {
    lc_acquire_req acquire;
    lc_lease *lease = NULL;
    lc_source *source = NULL;
    lc_acquire_req_init(&acquire);
    acquire.key = "pending";
    acquire.namespace_name = "namespace-0";
    acquire.owner = "interrupted-writer";
    acquire.ttl_seconds = 86400;
    check(client->acquire(client, &acquire, &lease, &error));
    check(lc_source_from_memory("{}", 2, &source, &error));
    check(lease->update(lease, source, NULL, &error));
    lc_source_close(source);
    /* Leave a live staged key: unlike completed-only interrupted writers,
     * this requires the conservative transaction recovery path. */
    _exit(0);
  }
  if (seed && strcmp(argv[7], "unclean") == 0) {
    /* Completed/released mutations, no client close or clean projection.
     * This is not a torn-write or power-loss simulation. */
    _exit(0);
  }
  client->close(client);
  sample("close");
  lc_error_cleanup(&error);
  return 0;
}

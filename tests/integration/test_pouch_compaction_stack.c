#include "../support/lc_test_tmp.h"
#include "lc/lc.h"
#include "lc_api_internal.h"
#include "lc_pouch.h"
#include "lc_pouch_namespace.h"

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define PAYLOAD_BYTES (256U * 1024U + 37U)
#define TMP_PREFIX "/tmp/liblockdc-compaction-stack-"

static lc_error error;
static const char *namespaces[] = {"alpha", "beta"};

#define CHECK(condition)                                                       \
  do {                                                                         \
    if (!(condition)) {                                                        \
      fprintf(stderr, "%s:%d: %s (%s)\n", __FILE__, __LINE__, #condition,      \
              error.message != NULL ? error.message : "no API error");         \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)

static void put(lc_pouch *pouch, const char *ns, const char *key,
                const unsigned char *bytes, size_t length) {
  lc_source *source;
  lc_pouch_state_write_result result;

  source = NULL;
  memset(&result, 0, sizeof(result));
  CHECK(lc_source_from_memory(bytes, length, &source, &error) == LC_OK);
  CHECK(lc_pouch_state_write(pouch, ns, key, source, NULL, &result, &error) ==
        LC_OK);
  lc_source_close(source);
  lc_pouch_state_write_result_cleanup(NULL, &result);
}

static void check_body(lc_pouch *pouch, const char *ns, const char *key,
                       const unsigned char *expected, size_t length) {
  lc_pouch_state_read_result result;
  unsigned char chunk[4096];
  size_t offset;
  size_t got;

  memset(&result, 0, sizeof(result));
  CHECK(lc_pouch_state_read(pouch, ns, key, &result, &error) == LC_OK);
  CHECK(result.found && result.body != NULL);
  offset = 0U;
  while ((got = result.body->read(result.body, chunk, sizeof(chunk), &error)) !=
         0U) {
    CHECK(got <= length - offset);
    CHECK(memcmp(chunk, expected + offset, got) == 0);
    offset += got;
  }
  CHECK(error.code == LC_OK && offset == length);
  lc_pouch_state_read_result_cleanup(NULL, &result);
}

/* Observe the published manifest, not merely an idle worker or a staged file.
 */
static void wait_snapshot(const char *root, const char *ns, char previous[96]) {
  char *namespace_path;
  char path[1024];
  char line[512];
  struct timespec start;
  struct timespec now;
  struct timespec pause;
  FILE *file;

  namespace_path = lc_pouch_namespace_path(NULL, root, ns);
  CHECK(namespace_path != NULL);
  CHECK(snprintf(path, sizeof(path), "%s/manifest", namespace_path) > 0);
  lc_free_with_allocator(NULL, namespace_path);
  CHECK(clock_gettime(CLOCK_MONOTONIC, &start) == 0);
  pause.tv_sec = 0;
  pause.tv_nsec = 10000000L;
  for (;;) {
    file = fopen(path, "r");
    CHECK(file != NULL);
    while (fgets(line, sizeof(line), file) != NULL) {
      if (strncmp(line, "snapshot=", 9U) == 0) {
        line[strcspn(line, "\r\n")] = '\0';
        if (line[9] != '\0' && strcmp(line + 9, previous) != 0) {
          CHECK(strlen(line + 9) < 96U);
          strcpy(previous, line + 9);
          CHECK(fclose(file) == 0);
          return;
        }
      }
    }
    CHECK(fclose(file) == 0);
    CHECK(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
    CHECK(now.tv_sec - start.tv_sec < 10);
    (void)nanosleep(&pause, NULL);
  }
}

typedef struct maintenance_job {
  lc_pouch *pouch;
  int rc;
} maintenance_job;

static void *compact_small_stack(void *context) {
  maintenance_job *job;
  lc_pouch_maintenance_options options;
  lc_pouch_maintenance_result result;
  size_t i;

  job = (maintenance_job *)context;
  job->rc = LC_OK;
  for (i = 0U; i < sizeof(namespaces) / sizeof(namespaces[0]); ++i) {
    memset(&options, 0, sizeof(options));
    memset(&result, 0, sizeof(result));
    options.ns = namespaces[i];
    options.force = 1;
    job->rc = lc_pouch_maintenance_run(job->pouch, &options, &result, &error);
    if (job->rc == LC_OK && !result.compacted) {
      job->rc = LC_ERR_PROTOCOL;
    }
    lc_pouch_maintenance_result_cleanup(NULL, &result);
    if (job->rc != LC_OK) {
      break;
    }
  }
  return NULL;
}

static void run_case(int background, int transform, const char *crypto_key,
                     size_t thread_stack_bytes) {
  lc_pouch_open_options options;
  lc_pouch *pouch;
  maintenance_job job;
  pthread_attr_t attr;
  pthread_t thread;
  unsigned char *payload;
  unsigned char small[37];
  uint32_t random;
  char root[512];
  char template_path[] = TMP_PREFIX "XXXXXX";
  char snapshots[2][96];
  size_t i;
  size_t ns;
  unsigned int round;

  memset(&options, 0, sizeof(options));
  memset(snapshots, 0, sizeof(snapshots));
  CHECK(lc_test_tmp_mkdtemp(template_path, root, sizeof(root), TMP_PREFIX));
  options.segment_target_bytes = 1U;
  options.compaction_min_segment_count = 1UL;
  options.compaction_min_reclaimable_bytes = 1U;
  options.compaction_interval_seconds = 1U;
  options.compaction_throttling_disabled = 1;
  options.background_compaction_enabled_set = 1;
  options.crypto_key = (transform & 1) != 0 ? crypto_key : NULL;
  options.compression = (transform & 2) != 0 ? "zlib" : "none";
  payload = (unsigned char *)malloc(PAYLOAD_BYTES);
  CHECK(payload != NULL);
  random = 0x6d2b79f5U;
  for (i = 0U; i < PAYLOAD_BYTES; ++i) {
    random = random * 1664525U + 1013904223U;
    payload[i] = (unsigned char)(random >> 24U);
  }
  memcpy(small, payload, sizeof(small));
  for (round = 0U; round < 2U; ++round) {
    small[0] ^= 0x40U;
    options.background_compaction_enabled = background;
    CHECK(lc_pouch_open(root, NULL, &options, &pouch, &error) == LC_OK);
    for (ns = 0U; ns < 2U; ++ns) {
      put(pouch, namespaces[ns], "small", (const unsigned char *)"obsolete",
          8U);
      put(pouch, namespaces[ns], "small", small, sizeof(small));
      /* The second snapshot must also copy a live span from the previous
       * snapshot, not only newly appended journal spans. */
      if (round == 0U) {
        put(pouch, namespaces[ns], "large", payload, PAYLOAD_BYTES);
      }
    }
    if (!background) {
      job.pouch = pouch;
      CHECK(pthread_attr_init(&attr) == 0);
      CHECK(pthread_attr_setstacksize(&attr, thread_stack_bytes) == 0);
      CHECK(pthread_create(&thread, &attr, compact_small_stack, &job) == 0);
      CHECK(pthread_attr_destroy(&attr) == 0);
      CHECK(pthread_join(thread, NULL) == 0);
      CHECK(job.rc == LC_OK);
    }
    for (ns = 0U; ns < 2U; ++ns) {
      wait_snapshot(root, namespaces[ns], snapshots[ns]);
    }
    lc_pouch_close(pouch);
    options.background_compaction_enabled = 0;
    CHECK(lc_pouch_open(root, NULL, &options, &pouch, &error) == LC_OK);
    for (ns = 0U; ns < 2U; ++ns) {
      check_body(pouch, namespaces[ns], "small", small, sizeof(small));
      check_body(pouch, namespaces[ns], "large", payload, PAYLOAD_BYTES);
    }
    lc_pouch_close(pouch);
  }
  free(payload);
  lc_test_tmp_cleanup_path(root, TMP_PREFIX);
  printf("%s transform=%d: two namespaces, repeated snapshots and readback "
         "passed\n",
         background ? "background" : "small-stack", transform);
}

int main(int argc, char **argv) {
  char *crypto_key;
  int transform;
  int background;
  long minimum_stack_bytes;
  size_t thread_stack_bytes;

  lc_error_init(&error);
  CHECK(argc == 2);
  CHECK(strcmp(argv[1], "background") == 0 ||
        strcmp(argv[1], "small-stack") == 0);
  background = strcmp(argv[1], "background") == 0;
  thread_stack_bytes = 0U;
  if (!background) {
    /* This is a thread-stack stress limit, independent of the production
     * compaction I/O buffer on the heap. Aarch64 glibc needs at least 128 KiB;
     * query the running libc instead of assuming every target accepts 64 KiB.
     */
    minimum_stack_bytes = sysconf(_SC_THREAD_STACK_MIN);
    CHECK(minimum_stack_bytes > 0L);
    thread_stack_bytes = 64U * 1024U;
    if ((size_t)minimum_stack_bytes > thread_stack_bytes) {
      thread_stack_bytes = (size_t)minimum_stack_bytes;
    }
    printf("Thread stack: %zu bytes; runtime minimum: %ld bytes\n",
           thread_stack_bytes, minimum_stack_bytes);
    fflush(stdout);
  }
  crypto_key = NULL;
  CHECK(lc_pouch_crypto_generate_key_string(&crypto_key, &error) == LC_OK);
  for (transform = 0; transform < 4; ++transform) {
    run_case(background, transform, crypto_key, thread_stack_bytes);
  }
  lc_pouch_crypto_key_string_free(crypto_key);
  lc_error_cleanup(&error);
  return 0;
}

#define _DEFAULT_SOURCE 1
#define _XOPEN_SOURCE 700
#include "lc/lc.h"

#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define CHUNK_BYTES (128U * 1024U)
#define MAX_RUNS 9
#define MAX_OUTPUT_BYTES ((uint64_t)1024U * 1024U * 1024U)

typedef struct delivery {
  int expected_fd;
  unsigned char *scratch;
  uint64_t bytes;
} delivery;

typedef struct sample {
  double seconds, cpu;
  uint64_t io_bytes;
} sample;

static volatile sig_atomic_t interrupted;
static unsigned char *cache_vector;
static size_t page_bytes;
static uint64_t resident_pages, checked_pages;

static void interrupt_run(int sig) { interrupted = sig; }

static int failure(const char *message) {
  fprintf(stderr, "collection read comparison: %s (%s)\n", message,
          strerror(errno));
  return -1;
}

static int path_join(char *out, size_t size, const char *parent,
                     const char *leaf) {
  size_t a = strlen(parent), b = strlen(leaf);
  if (a >= size || b >= size - a || size - a - b < 2U) {
    errno = ENAMETOOLONG;
    return failure("fixture path is too long");
  }
  memcpy(out, parent, a);
  out[a] = '/';
  memcpy(out + a + 1U, leaf, b + 1U);
  return 0;
}

static double now(void) {
  struct timespec ts;
  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
    return -1.0;
  return (double)ts.tv_sec + (double)ts.tv_nsec / 1000000000.0;
}

static double cpu_time(void) {
  struct rusage usage;
  if (getrusage(RUSAGE_SELF, &usage) != 0)
    return -1.0;
  return (double)usage.ru_utime.tv_sec + (double)usage.ru_stime.tv_sec +
         ((double)usage.ru_utime.tv_usec + (double)usage.ru_stime.tv_usec) /
             1000000.0;
}

static int storage_bytes(uint64_t *out) {
  FILE *fp;
  char line[128], *end;
  uint64_t value;
  int found = 0;
  fp = fopen("/proc/self/io", "r");
  if (fp == NULL)
    return failure("cannot read /proc/self/io");
  while (fgets(line, sizeof(line), fp) != NULL) {
    if (strncmp(line, "read_bytes:", 11U) != 0)
      continue;
    end = line + 11U;
    while (*end == ' ' || *end == '\t')
      ++end;
    value = 0U;
    while (*end >= '0' && *end <= '9') {
      if (value > ((uint64_t)-1 - (unsigned int)(*end - '0')) / 10U)
        break;
      value = value * 10U + (unsigned int)(*end++ - '0');
    }
    found = *end == '\n' || *end == '\0';
    *out = value;
    break;
  }
  (void)fclose(fp);
  if (!found)
    errno = EINVAL;
  return found ? 0 : failure("missing or overflowing storage read counter");
}

static int write_all(int fd, const void *bytes, size_t count) {
  size_t done = 0U;
  ssize_t n;
  while (done < count && !interrupted) {
    n = write(fd, (const char *)bytes + done, count - done);
    if (n < 0 && errno == EINTR)
      continue;
    if (n <= 0)
      return failure("fixture write failed");
    done += (size_t)n;
  }
  return interrupted ? -1 : 0;
}

/* Before timing, compare the entire collection output against raw JSONL using
 * bounded chunks. Timed runs use this same sink with expected_fd == -1 and do
 * only byte accounting; no consumer checksum obscures SDK integrity costs. */
static int consume(lc_sink *sink, const void *bytes, size_t count,
                   lc_error *error) {
  delivery *out = (delivery *)sink->impl;
  const unsigned char *p = (const unsigned char *)bytes;
  size_t done = 0U, length;
  ssize_t n;
  if (interrupted)
    return 0;
  while (out->expected_fd >= 0 && done < count) {
    length = count - done;
    if (length > CHUNK_BYTES)
      length = CHUNK_BYTES;
    do {
      n = read(out->expected_fd, out->scratch, length);
    } while (n < 0 && errno == EINTR);
    if (n <= 0 || memcmp(p + done, out->scratch, (size_t)n) != 0) {
      (void)lc_error_set(error, LC_ERR_PROTOCOL, 0,
                         "collection bytes differ from raw JSONL", NULL, NULL,
                         NULL);
      return 0;
    }
    done += (size_t)n;
  }
  out->bytes += count;
  return 1;
}

static int read_collection(lc_collection *collection, int expected_fd,
                           unsigned char *scratch, uint64_t rows,
                           uint64_t bytes, lc_error *error) {
  lc_collection_read_res result;
  lc_sink sink;
  delivery out;
  int rc;
  lc_collection_read_res_init(&result);
  memset(&sink, 0, sizeof(sink));
  out.expected_fd = expected_fd;
  out.scratch = scratch;
  out.bytes = 0U;
  sink.impl = &out;
  sink.write = consume;
  rc = lc_collection_read(collection, NULL, &sink, &result, error);
  if (rc == LC_OK && (!result.complete || result.completed_records != rows ||
                      out.bytes != bytes))
    rc = lc_error_set(error, LC_ERR_PROTOCOL, 0,
                      "collection read count mismatch", NULL, NULL, NULL);
  lc_collection_read_res_cleanup(&result);
  return rc;
}

static int read_raw(int fd, unsigned char *scratch, uint64_t expected) {
  uint64_t bytes = 0U;
  ssize_t n;
  while (!interrupted) {
    n = read(fd, scratch, CHUNK_BYTES);
    if (n < 0 && errno == EINTR)
      continue;
    if (n < 0)
      return failure("raw JSONL read failed");
    if (n == 0)
      break;
    bytes += (uint64_t)n;
  }
  if (bytes != expected || interrupted) {
    errno = EIO;
    return failure("raw JSONL byte count mismatch or interrupted run");
  }
  return 0;
}

static int cold_file(const char *path, const struct stat *st, int type,
                     struct FTW *walk) {
  off_t offset = 0, aligned;
  size_t window, length, pages, i;
  void *mapping;
  int fd, rc;
  (void)walk;
  if (interrupted)
    return -1;
  if (type == FTW_D)
    return 0;
  if (type != FTW_F || !S_ISREG(st->st_mode)) {
    errno = EINVAL;
    return failure("unexpected nonregular fixture entry");
  }
  fd = open(path, O_RDONLY | O_CLOEXEC);
  if (fd < 0)
    return failure("cannot open fixture for cache eviction");
  rc = fsync(fd);
  aligned = st->st_size +
            (off_t)((page_bytes - (size_t)(st->st_size % (off_t)page_bytes)) %
                    page_bytes);
  if (rc == 0 && aligned != 0) {
    rc = posix_fadvise(fd, 0, aligned, POSIX_FADV_DONTNEED);
    if (rc != 0)
      errno = rc;
  }
  if (rc != 0) {
    (void)close(fd);
    return failure("cannot synchronize/evict fixture file pages");
  }
  window = (4U * 1024U * 1024U / page_bytes) * page_bytes;
  while (offset < st->st_size) {
    length = (uint64_t)(st->st_size - offset) < window
                 ? (size_t)(st->st_size - offset)
                 : window;
    pages = (length + page_bytes - 1U) / page_bytes;
    mapping = mmap(NULL, length, PROT_NONE, MAP_SHARED, fd, offset);
    if (mapping == MAP_FAILED || mincore(mapping, length, cache_vector) != 0) {
      if (mapping != MAP_FAILED)
        (void)munmap(mapping, length);
      (void)close(fd);
      return failure("cannot verify file-page residency");
    }
    for (i = 0U; i < pages; ++i)
      resident_pages += (cache_vector[i] & 1U) != 0U;
    checked_pages += pages;
    (void)munmap(mapping, length);
    offset += (off_t)length;
  }
  return close(fd) == 0 ? 0 : failure("cannot close cache probe");
}

static int evict(const char *path) {
  resident_pages = checked_pages = 0U;
  if (nftw(path, cold_file, 16, FTW_PHYS) != 0)
    return -1;
  if (resident_pages != 0U || checked_pages == 0U) {
    fprintf(stderr,
            "cannot establish page-cache-cold input: %lu/%lu pages resident in "
            "%s\n",
            (unsigned long)resident_pages, (unsigned long)checked_pages, path);
    return -1;
  }
  return 0;
}

static int remove_owned(const char *path, const struct stat *st, int type,
                        struct FTW *walk) {
  (void)st;
  (void)walk;
  return type == FTW_DP ? rmdir(path) : unlink(path);
}

static double median(double *values, size_t count) {
  double copy[MAX_RUNS], value;
  size_t i, j;
  memcpy(copy, values, count * sizeof(*values));
  for (i = 1U; i < count; ++i) {
    value = copy[i];
    j = i;
    while (j > 0U && copy[j - 1U] > value) {
      copy[j] = copy[j - 1U];
      --j;
    }
    copy[j] = value;
  }
  return count % 2U ? copy[count / 2U]
                    : (copy[count / 2U - 1U] + copy[count / 2U]) / 2.0;
}

static int number(const char *text, long min, long max, long *out) {
  char *end;
  errno = 0;
  *out = strtol(text, &end, 10);
  return errno == 0 && end != text && *end == '\0' && *out >= min &&
         *out <= max;
}

static void usage(FILE *out, const char *program) {
  fprintf(out,
          "usage: %s [rows=8192] [JSON-bytes=4096] [runs=3] "
          "[existing-directory=build/collection-read-compare]\n"
          "rows: 1..1000000; JSON-bytes: 16..1048576; runs: 1..9; "
          "total output: at most 1 GiB\n",
          program);
}

int main(int argc, char **argv) {
  static const char prefix[] = "{\"value\":\"";
  char directory[PATH_MAX], pouch[PATH_MAX], raw[PATH_MAX], partition[PATH_MAX];
  char endpoint[PATH_MAX + 16], id[64];
  const char *parent, *endpoints[1];
  unsigned char *body, *scratch;
  lc_client_config config;
  lc_pouch_settings settings;
  lc_collection_config cc;
  lc_collection_append_req req;
  lc_collection_append_res appended;
  lc_collection_info info;
  lc_client *client = NULL;
  lc_collection *collection = NULL;
  lc_source *source = NULL;
  lc_error error;
  sample measurements[2];
  double raw_times[MAX_RUNS], collection_times[MAX_RUNS], ratios[MAX_RUNS];
  double start, cpu, ratio;
  uint64_t bytes, before, after;
  unsigned long random = 1U;
  long rows = 8192, body_bytes = 4096, runs = 3, row, run;
  size_t i;
  int fd = -1, owned = 0, rc = 1, phase, kind, result;
  directory[0] = '\0';
  body = scratch = NULL;
  lc_error_init(&error);
  parent = argc > 4 ? argv[4] : "build/collection-read-compare";
  if (argc == 2 && strcmp(argv[1], "--help") == 0) {
    usage(stdout, argv[0]);
    rc = 0;
    goto done;
  }
  if (argc > 5 || (argc > 1 && !number(argv[1], 1, 1000000, &rows)) ||
      (argc > 2 && !number(argv[2], 16, 1048576, &body_bytes)) ||
      (argc > 3 && !number(argv[3], 1, MAX_RUNS, &runs))) {
    usage(stderr, argv[0]);
    goto done;
  }
  bytes = (uint64_t)rows * ((uint64_t)body_bytes + 1U);
  if (bytes > MAX_OUTPUT_BYTES) {
    fprintf(stderr, "comparison output is limited to 1 GiB per fixture\n");
    goto done;
  }
  if (snprintf(directory, sizeof(directory), "%s/collection-read-XXXXXX",
               parent) < 0 ||
      strlen(parent) + 64U >= sizeof(directory)) {
    errno = ENAMETOOLONG;
    failure("fixture directory is too long");
    goto done;
  }
  if (mkdtemp(directory) == NULL) {
    failure("cannot create fixture; create the parent directory first");
    goto done;
  }
  owned = 1;
  if (realpath(directory, raw) == NULL) {
    failure("cannot resolve fixture path");
    goto done;
  }
  memcpy(directory, raw, strlen(raw) + 1U);
  if (path_join(raw, sizeof(raw), directory, "raw.jsonl") != 0 ||
      path_join(pouch, sizeof(pouch), directory, "pouch") != 0)
    goto done;
  snprintf(endpoint, sizeof(endpoint), "pouch://%s", pouch);
  body = (unsigned char *)malloc((size_t)body_bytes);
  scratch = (unsigned char *)malloc(CHUNK_BYTES);
  if (body == NULL || scratch == NULL) {
    failure("cannot allocate bounded fixture buffers");
    goto done;
  }
  cache_vector = scratch;
  page_bytes = (size_t)sysconf(_SC_PAGESIZE);
  if (page_bytes == 0U || page_bytes > 4U * 1024U * 1024U ||
      (4U * 1024U * 1024U / page_bytes) > CHUNK_BYTES) {
    errno = EINVAL;
    failure("unsupported page size");
    goto done;
  }
  (void)signal(SIGINT, interrupt_run);
  (void)signal(SIGTERM, interrupt_run);
  lc_client_config_init(&config);
  lc_pouch_settings_init(&settings);
  settings.set_mask =
      LC_POUCH_SETTING_SINGLE_WRITER | LC_POUCH_SETTING_DURABLE_SYNC |
      LC_POUCH_SETTING_QUERY_INDEXING | LC_POUCH_SETTING_BACKGROUND_COMPACTION |
      LC_POUCH_SETTING_COMPRESSION;
  settings.single_writer = 1;
  settings.compression = "none";
  config.pouch_settings = &settings;
  endpoints[0] = endpoint;
  config.endpoints = endpoints;
  config.endpoint_count = 1U;
  config.default_namespace = "compare";
  lc_collection_config_init(&cc);
  cc.name = "journal";
  result = lc_client_open(&config, &client, &error);
  if (result == LC_OK)
    result = lc_client_create_collection(client, &cc, &collection, &error);
  if (result != LC_OK)
    goto done;
  fd = open(raw, O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0) {
    failure("cannot create raw JSONL fixture");
    goto done;
  }
  lc_collection_append_req_init(&req);
  memcpy(body, prefix, sizeof(prefix) - 1U);
  body[body_bytes - 2] = '"';
  body[body_bytes - 1] = '}';
  printf("fixture rows=%ld JSON_bytes=%ld output_bytes=%lu "
         "cache=verified-file-page-cache-cold crypto=0 compression=none "
         "LQL=none single_writer=1 indexing=0 durable_sync=0\n",
         rows, body_bytes, (unsigned long)bytes);
  fflush(stdout);
  for (row = 0; row < rows && !interrupted; ++row) {
    for (i = sizeof(prefix) - 1U; i < (size_t)body_bytes - 2U; ++i) {
      random = (random * 1664525UL + 1013904223UL) & 0xffffffffUL;
      body[i] = (unsigned char)('a' + ((random >> 16U) % 26U));
    }
    snprintf(id, sizeof(id), "event-%ld", row);
    req.append_id = id;
    result = lc_source_from_memory(body, (size_t)body_bytes, &source, &error);
    if (result == LC_OK)
      result =
          lc_collection_append(collection, &req, source, &appended, &error);
    lc_source_close(source);
    source = NULL;
    if (result != LC_OK || appended.duplicate ||
        appended.position != (uint64_t)row + 1U ||
        appended.bytes != (uint64_t)body_bytes ||
        write_all(fd, body, (size_t)body_bytes) != 0 ||
        write_all(fd, "\n", 1U) != 0)
      goto done;
  }
  if (interrupted || lseek(fd, 0, SEEK_SET) < 0 ||
      read_collection(collection, fd, scratch, (uint64_t)rows, bytes, &error) !=
          LC_OK)
    goto done;
  result = lc_collection_describe(collection, &info, &error);
  if (result != LC_OK)
    goto done;
  if (path_join(partition, sizeof(partition), pouch,
                "namespaces/compare/collections/data") != 0 ||
      path_join(pouch, sizeof(pouch), partition, info.collection_id) != 0)
    goto done;
  memcpy(partition, pouch, strlen(pouch) + 1U);
  printf("validation=byte-identical consumer=discard raw_chunk_bytes=%u "
         "metadata-cache=warm device-cache=uncontrolled\n",
         CHUNK_BYTES);
  for (run = 0; run < runs && !interrupted; ++run) {
    for (phase = 0; phase < 2; ++phase) {
      kind = (int)((run + phase) % 2);
      if (evict(kind == 0 ? raw : partition) != 0 ||
          lseek(fd, 0, SEEK_SET) < 0 || storage_bytes(&before) != 0)
        goto done;
      cpu = cpu_time();
      start = now();
      result = kind == 0 ? read_raw(fd, scratch, bytes)
                         : read_collection(collection, -1, scratch,
                                           (uint64_t)rows, bytes, &error);
      measurements[kind].seconds = now() - start;
      measurements[kind].cpu = cpu_time() - cpu;
      if (result != 0 || storage_bytes(&after) != 0 || after < before ||
          start < 0.0 || cpu < 0.0)
        goto done;
      measurements[kind].io_bytes = after - before;
      if (measurements[kind].seconds <= 0.0 || measurements[kind].cpu < 0.0) {
        errno = EIO;
        failure("invalid timer interval");
        goto done;
      }
      if (measurements[kind].io_bytes == 0U) {
        fprintf(stderr,
                "no storage reads recorded for %s; cannot establish a "
                "disk-read comparison on this filesystem\n",
                kind == 0 ? "raw JSONL" : "collection");
        goto done;
      }
      printf("run=%ld path=%s resident_pages_before=0 seconds=%.6f "
             "cpu_seconds=%.6f MiB_per_second=%.3f storage_read_bytes=%lu\n",
             run + 1, kind == 0 ? "raw-jsonl" : "collection",
             measurements[kind].seconds, measurements[kind].cpu,
             (double)bytes / 1048576.0 / measurements[kind].seconds,
             (unsigned long)measurements[kind].io_bytes);
    }
    raw_times[run] = measurements[0].seconds;
    collection_times[run] = measurements[1].seconds;
    ratios[run] = collection_times[run] / raw_times[run];
  }
  if (interrupted)
    goto done;
  ratio = median(ratios, (size_t)runs);
  printf("summary raw_median_seconds=%.6f collection_median_seconds=%.6f "
         "median_time_ratio=%.3f extra_time_fraction=%.3f extra_time_pct=%.1f "
         "throughput_retained_pct=%.1f\n",
         median(raw_times, (size_t)runs),
         median(collection_times, (size_t)runs), ratio, ratio - 1.0,
         (ratio - 1.0) * 100.0, 100.0 / ratio);
  rc = 0;
done:
  if (error.message != NULL && rc != 0)
    fprintf(stderr, "SDK: %s\n", error.message);
  lc_source_close(source);
  lc_collection_close(collection);
  lc_client_close(client);
  lc_error_cleanup(&error);
  if (fd >= 0)
    (void)close(fd);
  free(body);
  free(scratch);
  if (owned && nftw(directory, remove_owned, 16, FTW_DEPTH | FTW_PHYS) != 0) {
    failure("cannot remove owned fixture");
    rc = 1;
  }
  return interrupted ? 128 + interrupted : rc;
}

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define POUCH_MUTATION_MAX_INPUT 65536U
#define POUCH_MUTATION_PATH_MAX 1024U
#define POUCH_MUTATION_TARGET_TIMEOUT_SECONDS 30L
#define POUCH_MUTATION_MAX_TARGET_TIMEOUT_SECONDS 300L
#define POUCH_MUTATION_WAIT_NANOSECONDS 10000000L

typedef struct pouch_mutation_corpus {
  char **paths;
  size_t count;
  size_t capacity;
} pouch_mutation_corpus;

static int pouch_mutation_path_compare(const void *left, const void *right) {
  const char *const *left_path;
  const char *const *right_path;

  left_path = (const char *const *)left;
  right_path = (const char *const *)right;
  return strcmp(*left_path, *right_path);
}

static void pouch_mutation_corpus_cleanup(pouch_mutation_corpus *corpus) {
  size_t index;

  if (corpus == NULL) {
    return;
  }
  for (index = 0U; index < corpus->count; ++index) {
    free(corpus->paths[index]);
  }
  free(corpus->paths);
  memset(corpus, 0, sizeof(*corpus));
}

static int pouch_mutation_corpus_append(pouch_mutation_corpus *corpus,
                                        const char *path) {
  char **grown;
  char *copy;
  size_t capacity;

  if (corpus == NULL || path == NULL) {
    return 0;
  }
  if (corpus->count == corpus->capacity) {
    capacity = corpus->capacity == 0U ? 16U : corpus->capacity * 2U;
    if (capacity <= corpus->capacity ||
        capacity > (size_t)-1 / sizeof(*corpus->paths)) {
      return 0;
    }
    grown = (char **)realloc(corpus->paths, capacity * sizeof(*grown));
    if (grown == NULL) {
      return 0;
    }
    corpus->paths = grown;
    corpus->capacity = capacity;
  }
  copy = strdup(path);
  if (copy == NULL) {
    return 0;
  }
  corpus->paths[corpus->count++] = copy;
  return 1;
}

static int pouch_mutation_corpus_load(pouch_mutation_corpus *corpus,
                                      const char *directory) {
  DIR *dir;
  struct dirent *entry;
  int ok;

  if (corpus == NULL || directory == NULL) {
    return 0;
  }
  memset(corpus, 0, sizeof(*corpus));
  dir = opendir(directory);
  if (dir == NULL) {
    return 0;
  }
  ok = 1;
  while (ok && (entry = readdir(dir)) != NULL) {
    char path[POUCH_MUTATION_PATH_MAX];
    struct stat st;
    int written;

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    written = snprintf(path, sizeof(path), "%s/%s", directory, entry->d_name);
    if (written < 0 || (size_t)written >= sizeof(path) ||
        stat(path, &st) != 0) {
      ok = 0;
    } else if (S_ISREG(st.st_mode)) {
      ok = pouch_mutation_corpus_append(corpus, path);
    }
  }
  if (closedir(dir) != 0) {
    ok = 0;
  }
  if (!ok || corpus->count == 0U) {
    pouch_mutation_corpus_cleanup(corpus);
    return 0;
  }
  qsort(corpus->paths, corpus->count, sizeof(*corpus->paths),
        pouch_mutation_path_compare);
  return 1;
}

static int pouch_mutation_read_file(const char *path, unsigned char **out,
                                    size_t *out_length) {
  FILE *fp;
  long length;
  unsigned char *bytes;

  fp = fopen(path, "rb");
  if (fp == NULL || fseek(fp, 0L, SEEK_END) != 0 || (length = ftell(fp)) < 0L ||
      (size_t)length > POUCH_MUTATION_MAX_INPUT ||
      fseek(fp, 0L, SEEK_SET) != 0) {
    if (fp != NULL) {
      fclose(fp);
    }
    return 0;
  }
  bytes = (unsigned char *)malloc((size_t)length + 1U);
  if (bytes == NULL ||
      (length > 0L && fread(bytes, 1U, (size_t)length, fp) != (size_t)length) ||
      fclose(fp) != 0) {
    free(bytes);
    return 0;
  }
  *out = bytes;
  *out_length = (size_t)length;
  return 1;
}

static uint32_t pouch_mutation_next(uint32_t *state) {
  uint32_t value;

  value = *state;
  value ^= value << 13;
  value ^= value >> 17;
  value ^= value << 5;
  *state = value;
  return value;
}

static int pouch_mutation_parse_count(const char *value,
                                      unsigned long *mutation_count) {
  char *end;
  unsigned long parsed;

  if (value == NULL || mutation_count == NULL || value[0] == '\0' ||
      value[0] == '-') {
    return 0;
  }
  errno = 0;
  end = NULL;
  parsed = strtoul(value, &end, 10);
  if (errno != 0 || end == value || *end != '\0' || parsed == ULONG_MAX) {
    return 0;
  }
  *mutation_count = parsed;
  return 1;
}

static size_t pouch_mutation_make(unsigned char *out, size_t capacity,
                                  const unsigned char *input,
                                  size_t input_length,
                                  unsigned long case_index) {
  uint32_t state;
  size_t length;
  size_t offset;
  size_t count;

  if (out == NULL || capacity == 0U) {
    return 0U;
  }
  length = input_length > capacity ? capacity : input_length;
  if (length > 0U) {
    memcpy(out, input, length);
  }
  state = 0x9E3779B9U ^ (uint32_t)case_index ^ (uint32_t)input_length;
  offset = length == 0U ? 0U : (size_t)(pouch_mutation_next(&state) % length);
  switch (case_index % 5UL) {
  case 0UL:
    if (length == 0U) {
      out[0] = (unsigned char)pouch_mutation_next(&state);
      return 1U;
    }
    out[offset] ^= (unsigned char)(1U << (pouch_mutation_next(&state) % 8U));
    break;
  case 1UL:
    if (length == 0U) {
      out[0] = 0U;
      return 1U;
    }
    out[offset] = (unsigned char)pouch_mutation_next(&state);
    break;
  case 2UL:
    if (length > 0U) {
      length = offset;
    }
    break;
  case 3UL:
    if (length < capacity) {
      memmove(out + offset + 1U, out + offset, length - offset);
      out[offset] = (unsigned char)pouch_mutation_next(&state);
      ++length;
    }
    break;
  default:
    if (length > 0U) {
      count = 1U + (size_t)(pouch_mutation_next(&state) % length);
      if (count > length - offset) {
        count = length - offset;
      }
      memset(out + offset, (int)(pouch_mutation_next(&state) & 0xFFU), count);
    }
    break;
  }
  return length;
}

static int pouch_mutation_write_file(const char *path,
                                     const unsigned char *bytes,
                                     size_t length) {
  int fd;
  size_t written;

  fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
  if (fd < 0) {
    return 0;
  }
  written = 0U;
  while (written < length) {
    ssize_t result = write(fd, bytes + written, length - written);

    if (result <= 0) {
      close(fd);
      return 0;
    }
    written += (size_t)result;
  }
  return close(fd) == 0;
}

static long pouch_mutation_target_timeout_seconds(void) {
  const char *value;
  char *end;
  long timeout;

  value = getenv("LOCKDC_POUCH_INTEGRATION_TARGET_TIMEOUT_SECONDS");
  if (value == NULL || value[0] == '\0') {
    return POUCH_MUTATION_TARGET_TIMEOUT_SECONDS;
  }
  errno = 0;
  end = NULL;
  timeout = strtol(value, &end, 10);
  if (errno != 0 || end == value || *end != '\0' || timeout <= 0L ||
      timeout > POUCH_MUTATION_MAX_TARGET_TIMEOUT_SECONDS) {
    return POUCH_MUTATION_TARGET_TIMEOUT_SECONDS;
  }
  return timeout;
}

static int pouch_mutation_reap_target(pid_t pid, int *status) {
  pid_t waited;

  do {
    waited = waitpid(pid, status, 0);
  } while (waited < 0 && errno == EINTR);
  return waited == pid;
}

static int pouch_mutation_run_target(const char *target, const char *input,
                                     int *timed_out) {
  pid_t pid;
  pid_t waited;
  int status;
  long timeout_seconds;
  time_t started;
  time_t now;
  struct timespec pause;

  if (timed_out == NULL) {
    return 0;
  }
  *timed_out = 0;
  timeout_seconds = pouch_mutation_target_timeout_seconds();

  pid = fork();
  if (pid < 0) {
    return 0;
  }
  if (pid == 0) {
    execl(target, target, input, (char *)NULL);
    _exit(127);
  }
  now = time(NULL);
  if (now == (time_t)-1) {
    (void)kill(pid, SIGKILL);
    (void)pouch_mutation_reap_target(pid, &status);
    *timed_out = 1;
    return 0;
  }
  started = now;
  pause.tv_sec = 0L;
  pause.tv_nsec = POUCH_MUTATION_WAIT_NANOSECONDS;
  for (;;) {
    waited = waitpid(pid, &status, WNOHANG);
    if (waited == pid) {
      return WIFEXITED(status) && WEXITSTATUS(status) == 0;
    }
    if (waited < 0 && errno != EINTR) {
      (void)kill(pid, SIGKILL);
      (void)pouch_mutation_reap_target(pid, &status);
      return 0;
    }
    now = time(NULL);
    if (now == (time_t)-1 ||
        difftime(now, started) >= (double)timeout_seconds) {
      (void)kill(pid, SIGKILL);
      (void)pouch_mutation_reap_target(pid, &status);
      *timed_out = 1;
      return 0;
    }
    while (nanosleep(&pause, &pause) != 0 && errno == EINTR) {
    }
    pause.tv_sec = 0L;
    pause.tv_nsec = POUCH_MUTATION_WAIT_NANOSECONDS;
  }
}

static int pouch_mutation_run_case(const char *target, const char *seed_path,
                                   const unsigned char *bytes, size_t length,
                                   const char *work_path,
                                   unsigned long case_index) {
  int timed_out;

  timed_out = 0;
  if (!pouch_mutation_write_file(work_path, bytes, length) ||
      !pouch_mutation_run_target(target, work_path, &timed_out)) {
    fprintf(
        stderr, "pouch integration mutation %s: seed=%s case=%lu artifact=%s\n",
        timed_out ? "timeout" : "failure", seed_path, case_index, work_path);
    return 0;
  }
  return 1;
}

int main(int argc, char **argv) {
  pouch_mutation_corpus corpus;
  char work_template[] = "/tmp/liblockdc-pouch-integration-mutation-XXXXXX";
  char work_path[POUCH_MUTATION_PATH_MAX];
  char *work_dir;
  unsigned long mutation_count;
  unsigned long case_index;
  size_t seed_index;
  int ok;

  if (argc != 4 || !pouch_mutation_parse_count(argv[3], &mutation_count)) {
    fprintf(stderr, "usage: %s target corpus-dir mutation-count\n", argv[0]);
    return 2;
  }
  if (!pouch_mutation_corpus_load(&corpus, argv[2])) {
    fprintf(stderr, "failed to load Pouch integration corpus: %s\n", argv[2]);
    return 2;
  }
  work_dir = mkdtemp(work_template);
  if (work_dir == NULL ||
      snprintf(work_path, sizeof(work_path), "%s/input.bin", work_dir) < 0) {
    pouch_mutation_corpus_cleanup(&corpus);
    return 2;
  }

  ok = 1;
  for (seed_index = 0U; ok && seed_index < corpus.count; ++seed_index) {
    unsigned char *bytes;
    size_t length;

    bytes = NULL;
    if (!pouch_mutation_read_file(corpus.paths[seed_index], &bytes, &length) ||
        !pouch_mutation_run_case(argv[1], corpus.paths[seed_index], bytes,
                                 length, work_path, 0UL)) {
      if (bytes == NULL) {
        fprintf(stderr, "failed to read Pouch integration corpus seed: %s\n",
                corpus.paths[seed_index]);
      }
      ok = 0;
    }
    free(bytes);
  }
  for (case_index = 1UL; ok && case_index <= mutation_count; ++case_index) {
    unsigned char *input;
    unsigned char mutated[POUCH_MUTATION_MAX_INPUT];
    size_t input_length;
    size_t mutation_length;

    seed_index = (size_t)((case_index - 1UL) % (unsigned long)corpus.count);
    if (!pouch_mutation_read_file(corpus.paths[seed_index], &input,
                                  &input_length)) {
      fprintf(stderr, "failed to read Pouch integration corpus seed: %s\n",
              corpus.paths[seed_index]);
      ok = 0;
      break;
    }
    mutation_length = pouch_mutation_make(mutated, sizeof(mutated), input,
                                          input_length, case_index);
    free(input);
    if (!pouch_mutation_run_case(argv[1], corpus.paths[seed_index], mutated,
                                 mutation_length, work_path, case_index)) {
      ok = 0;
    }
  }
  if (ok) {
    unlink(work_path);
    rmdir(work_dir);
  }
  pouch_mutation_corpus_cleanup(&corpus);
  return ok ? 0 : 1;
}

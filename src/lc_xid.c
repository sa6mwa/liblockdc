/*
 * rs/xid-compatible process-local identifier generator.
 *
 * The layout and wire encoding intentionally follow github.com/rs/xid (MIT):
 * four big-endian Unix-time bytes, three machine-id bytes, two process-id
 * bytes, and a three-byte counter, encoded as lowercase base32hex.
 */
#include "lc_api_internal.h"

#include <errno.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <zlib.h>

#define LC_XID_RAW_LENGTH 12U
#define LC_XID_MACHINE_ID_LENGTH 3U
#define LC_XID_COUNTER_MASK 0xFFFFFFUL

typedef struct lc_xid_generator {
  pthread_mutex_t mutex;
  unsigned char machine_id[LC_XID_MACHINE_ID_LENGTH];
  uint32_t pid;
  uint32_t counter;
  int initialized;
  int init_status;
} lc_xid_generator;

static pthread_once_t lc_xid_once = PTHREAD_ONCE_INIT;
static lc_xid_generator lc_xid_global = {
    PTHREAD_MUTEX_INITIALIZER, {0U, 0U, 0U}, 0U, 0U, 0, LC_OK};

static int lc_xid_read_file(const char *path, unsigned char *out,
                            size_t capacity, size_t *out_length) {
  FILE *file;
  int failed;
  size_t length;

  if (out_length != NULL) {
    *out_length = 0U;
  }
  file = fopen(path, "rb");
  if (file == NULL) {
    return 0;
  }
  length = fread(out, 1U, capacity, file);
  failed = ferror(file) || fclose(file) != 0;
  if (failed) {
    return 0;
  }
  if (out_length != NULL) {
    *out_length = length;
  }
  return length > 0U;
}

static int lc_xid_machine_id_from_environment(unsigned char *machine_id) {
  const char *value;
  char *end;
  unsigned long parsed;

  value = getenv("XID_MACHINE_ID");
  if (value == NULL || value[0] == '\0') {
    return 0;
  }
  errno = 0;
  end = NULL;
  parsed = strtoul(value, &end, 10);
  if (errno != 0 || end == value || *end != '\0' ||
      parsed > LC_XID_COUNTER_MASK) {
    return -1;
  }
  machine_id[0] = (unsigned char)(parsed >> 16);
  machine_id[1] = (unsigned char)(parsed >> 8);
  machine_id[2] = (unsigned char)parsed;
  return 1;
}

static int lc_xid_machine_id_from_host(unsigned char *machine_id) {
  unsigned char host_id[4096];
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int digest_length;
  size_t host_id_length;
  int override_status;

  override_status = lc_xid_machine_id_from_environment(machine_id);
  if (override_status != 0) {
    return override_status > 0;
  }
  host_id_length = 0U;
#if defined(__linux__)
  if (!lc_xid_read_file("/etc/machine-id", host_id, sizeof(host_id),
                        &host_id_length)) {
    (void)lc_xid_read_file("/sys/class/dmi/id/product_uuid", host_id,
                           sizeof(host_id), &host_id_length);
  }
#endif
  if (host_id_length == 0U) {
    if (gethostname((char *)host_id, sizeof(host_id) - 1U) == 0) {
      host_id[sizeof(host_id) - 1U] = '\0';
      host_id_length = strlen((const char *)host_id);
    }
  }
  if (host_id_length != 0U &&
      EVP_Digest(host_id, host_id_length, digest, &digest_length, EVP_sha256(),
                 NULL) == 1 &&
      digest_length >= LC_XID_MACHINE_ID_LENGTH) {
    memcpy(machine_id, digest, LC_XID_MACHINE_ID_LENGTH);
    return 1;
  }
  return RAND_bytes(machine_id, LC_XID_MACHINE_ID_LENGTH) == 1;
}

static void lc_xid_initialize(void) {
  unsigned char random_counter[LC_XID_MACHINE_ID_LENGTH];
  unsigned char cpuset[4096];
  size_t cpuset_length;

  if (!lc_xid_machine_id_from_host(lc_xid_global.machine_id) ||
      RAND_bytes(random_counter, sizeof(random_counter)) != 1) {
    lc_xid_global.init_status = LC_ERR_TRANSPORT;
    return;
  }
  lc_xid_global.counter = ((uint32_t)random_counter[0] << 16) |
                          ((uint32_t)random_counter[1] << 8) |
                          (uint32_t)random_counter[2];
  lc_xid_global.pid = (uint32_t)getpid();
  cpuset_length = 0U;
#if defined(__linux__)
  if (lc_xid_read_file("/proc/self/cpuset", cpuset, sizeof(cpuset),
                       &cpuset_length) &&
      cpuset_length > 1U) {
    lc_xid_global.pid ^= (uint32_t)crc32(0L, cpuset, (uInt)cpuset_length);
  }
#endif
  lc_xid_global.initialized = 1;
}

static void lc_xid_encode(const unsigned char raw[LC_XID_RAW_LENGTH],
                          char out[LC_XID_STRING_SIZE]) {
  static const char encoding[] = "0123456789abcdefghijklmnopqrstuv";

  out[19] = encoding[(raw[11] << 4) & 0x1fU];
  out[18] = encoding[(raw[11] >> 1) & 0x1fU];
  out[17] = encoding[(raw[11] >> 6) | ((raw[10] << 2) & 0x1fU)];
  out[16] = encoding[raw[10] >> 3];
  out[15] = encoding[raw[9] & 0x1fU];
  out[14] = encoding[(raw[9] >> 5) | ((raw[8] << 3) & 0x1fU)];
  out[13] = encoding[(raw[8] >> 2) & 0x1fU];
  out[12] = encoding[(raw[8] >> 7) | ((raw[7] << 1) & 0x1fU)];
  out[11] = encoding[(raw[7] >> 4) | ((raw[6] << 4) & 0x1fU)];
  out[10] = encoding[(raw[6] >> 1) & 0x1fU];
  out[9] = encoding[(raw[6] >> 6) | ((raw[5] << 2) & 0x1fU)];
  out[8] = encoding[raw[5] >> 3];
  out[7] = encoding[raw[4] & 0x1fU];
  out[6] = encoding[(raw[4] >> 5) | ((raw[3] << 3) & 0x1fU)];
  out[5] = encoding[(raw[3] >> 2) & 0x1fU];
  out[4] = encoding[(raw[3] >> 7) | ((raw[2] << 1) & 0x1fU)];
  out[3] = encoding[(raw[2] >> 4) | ((raw[1] << 4) & 0x1fU)];
  out[2] = encoding[(raw[1] >> 1) & 0x1fU];
  out[1] = encoding[(raw[1] >> 6) | ((raw[0] << 2) & 0x1fU)];
  out[0] = encoding[raw[0] >> 3];
  out[LC_XID_STRING_LENGTH] = '\0';
}

int lc_xid_new(char out[LC_XID_STRING_SIZE], lc_error *error) {
  time_t now;
  unsigned char raw[LC_XID_RAW_LENGTH];
  uint32_t counter;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "xid output buffer is required", NULL, NULL, NULL);
  }
  out[0] = '\0';
  (void)pthread_once(&lc_xid_once, lc_xid_initialize);
  if (!lc_xid_global.initialized) {
    return lc_error_set(error, lc_xid_global.init_status, 0L,
                        "failed to initialize XID generator", NULL, NULL, NULL);
  }
  now = time(NULL);
  if (now == (time_t)-1) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read time for XID", NULL, NULL, NULL);
  }
  pthread_mutex_lock(&lc_xid_global.mutex);
  lc_xid_global.counter = (lc_xid_global.counter + 1U) & LC_XID_COUNTER_MASK;
  counter = lc_xid_global.counter;
  pthread_mutex_unlock(&lc_xid_global.mutex);
  raw[0] = (unsigned char)((uint32_t)now >> 24);
  raw[1] = (unsigned char)((uint32_t)now >> 16);
  raw[2] = (unsigned char)((uint32_t)now >> 8);
  raw[3] = (unsigned char)now;
  memcpy(raw + 4U, lc_xid_global.machine_id, LC_XID_MACHINE_ID_LENGTH);
  raw[7] = (unsigned char)(lc_xid_global.pid >> 8);
  raw[8] = (unsigned char)lc_xid_global.pid;
  raw[9] = (unsigned char)(counter >> 16);
  raw[10] = (unsigned char)(counter >> 8);
  raw[11] = (unsigned char)counter;
  lc_xid_encode(raw, out);
  return LC_OK;
}

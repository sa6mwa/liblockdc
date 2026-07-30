#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "lc/lc.h"
#include "lc_pouch_record.h"

static unsigned long fuzz_u32(const uint8_t *data, size_t size, size_t offset) {
  unsigned long value;
  size_t i;

  value = 0UL;
  for (i = 0U; i < 4U && offset + i < size; ++i) {
    value |= ((unsigned long)data[offset + i]) << (i * 8U);
  }
  return value;
}

static uint64_t fuzz_u64(const uint8_t *data, size_t size, size_t offset) {
  uint64_t value;
  size_t i;

  value = 0U;
  for (i = 0U; i < 8U && offset + i < size; ++i) {
    value |= ((uint64_t)data[offset + i]) << (i * 8U);
  }
  return value;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  lc_pouch_record_header decoded;
  lc_pouch_record_header header;
  unsigned char encoded[LC_POUCH_RECORD_HEADER_BYTES];
  unsigned char mutated[LC_POUCH_RECORD_HEADER_BYTES];
  lc_error error;

  if (data == NULL) {
    return 0;
  }

  lc_error_init(&error);
  if (size >= LC_POUCH_RECORD_HEADER_BYTES) {
    (void)lc_pouch_record_header_decode(data, &decoded, &error);
  }
  lc_error_cleanup(&error);

  memset(&header, 0, sizeof(header));
  header.type = size > 0U ? data[0] : 0U;
  header.flags = size > 1U ? data[1] : 0UL;
  header.key_len = fuzz_u32(data, size, 2U);
  header.meta_len = fuzz_u32(data, size, 6U);
  header.payload_len = fuzz_u64(data, size, 10U);
  header.payload_crc = fuzz_u32(data, size, 18U);
  lc_pouch_record_header_encode(&header, encoded);

  lc_error_init(&error);
  if (lc_pouch_record_header_decode(encoded, &decoded, &error) == LC_OK &&
      size > 0U) {
    memcpy(mutated, encoded, sizeof(mutated));
    mutated[data[0] % LC_POUCH_RECORD_HEADER_BYTES] ^= 0x5AU;
    (void)lc_pouch_record_header_decode(mutated, &decoded, &error);
  }
  lc_error_cleanup(&error);
  return 0;
}

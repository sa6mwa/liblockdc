#include "lc_pouch_record.h"

#include "lc_api_internal.h"

#include <string.h>
#include <zlib.h>

static void lc_pouch_record_put16(unsigned char *out, unsigned long value) {
  out[0] = (unsigned char)(value & 0xFFUL);
  out[1] = (unsigned char)((value >> 8U) & 0xFFUL);
}

static void lc_pouch_record_put32(unsigned char *out, unsigned long value) {
  out[0] = (unsigned char)(value & 0xFFUL);
  out[1] = (unsigned char)((value >> 8U) & 0xFFUL);
  out[2] = (unsigned char)((value >> 16U) & 0xFFUL);
  out[3] = (unsigned char)((value >> 24U) & 0xFFUL);
}

static void lc_pouch_record_put64(unsigned char *out, uint64_t value) {
  size_t i;

  for (i = 0U; i < 8U; ++i) {
    out[i] = (unsigned char)((value >> (i * 8U)) & 0xFFU);
  }
}

static unsigned long lc_pouch_record_get16(const unsigned char *in) {
  return (unsigned long)in[0] | ((unsigned long)in[1] << 8U);
}

static unsigned long lc_pouch_record_get32(const unsigned char *in) {
  return (unsigned long)in[0] | ((unsigned long)in[1] << 8U) |
         ((unsigned long)in[2] << 16U) | ((unsigned long)in[3] << 24U);
}

static uint64_t lc_pouch_record_get64(const unsigned char *in) {
  uint64_t value;
  size_t i;

  value = 0U;
  for (i = 0U; i < 8U; ++i) {
    value |= ((uint64_t)in[i]) << (i * 8U);
  }
  return value;
}

void lc_pouch_record_header_encode(const lc_pouch_record_header *header,
                                   unsigned char out[32]) {
  unsigned long header_crc;

  memset(out, 0, 32U);
  if (header == NULL) {
    return;
  }
  lc_pouch_record_put32(out, LC_POUCH_RECORD_MAGIC);
  out[4] = LC_POUCH_RECORD_FORMAT;
  out[5] = header->type;
  lc_pouch_record_put16(out + 6, header->flags);
  lc_pouch_record_put32(out + 8, header->key_len);
  lc_pouch_record_put32(out + 12, header->meta_len);
  lc_pouch_record_put64(out + 16, header->payload_len);
  lc_pouch_record_put32(out + 24, header->payload_crc);
  header_crc = (unsigned long)crc32(0L, out, 28U);
  lc_pouch_record_put32(out + 28, header_crc);
}

int lc_pouch_record_header_decode(const unsigned char in[32],
                                  lc_pouch_record_header *header,
                                  lc_error *error) {
  unsigned long magic;
  unsigned long expected_crc;
  unsigned long actual_crc;

  if (in == NULL || header == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch record header decode requires inputs", NULL,
                        NULL, "pouch");
  }
  magic = lc_pouch_record_get32(in);
  if (magic != LC_POUCH_RECORD_MAGIC) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch record magic mismatch", NULL, NULL, "pouch");
  }
  if (in[4] != LC_POUCH_RECORD_FORMAT) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch record format mismatch", NULL, NULL, "pouch");
  }
  expected_crc = lc_pouch_record_get32(in + 28);
  actual_crc = (unsigned long)crc32(0L, in, 28U);
  if (expected_crc != actual_crc) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch record header CRC mismatch", NULL, NULL,
                        "pouch");
  }
  memset(header, 0, sizeof(*header));
  header->type = in[5];
  header->flags = lc_pouch_record_get16(in + 6);
  header->key_len = lc_pouch_record_get32(in + 8);
  header->meta_len = lc_pouch_record_get32(in + 12);
  header->payload_len = lc_pouch_record_get64(in + 16);
  header->payload_crc = lc_pouch_record_get32(in + 24);
  header->header_crc = expected_crc;
  return LC_OK;
}

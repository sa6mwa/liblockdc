#ifndef LC_POUCH_RECORD_H
#define LC_POUCH_RECORD_H

#include "lc_pouch.h"

#include <stdint.h>

#define LC_POUCH_RECORD_MAGIC 0x5043484cUL
#define LC_POUCH_RECORD_FORMAT 1U
#define LC_POUCH_RECORD_HEADER_BYTES 32U
#define LC_POUCH_RECORD_FLAG_PENDING 1UL

typedef struct lc_pouch_record_header {
  unsigned char type;
  unsigned long flags;
  unsigned long key_len;
  unsigned long meta_len;
  uint64_t payload_len;
  unsigned long payload_crc;
  unsigned long header_crc;
} lc_pouch_record_header;

void lc_pouch_record_header_encode(const lc_pouch_record_header *header,
                                   unsigned char out[32]);
int lc_pouch_record_header_decode(const unsigned char in[32],
                                  lc_pouch_record_header *header,
                                  lc_error *error);

#endif

#include "lc_utf8.h"

int lc_utf8_push(lc_utf8_state *state, const unsigned char *bytes,
                 size_t length) {
  size_t i;
  unsigned char byte;
  for (i = 0U; i < length; ++i) {
    byte = bytes[i];
    if (state->remaining == 0) {
      if (byte <= 0x7fU) {
        continue;
      }
      if (byte >= 0xc2U && byte <= 0xdfU) {
        state->remaining = 1;
        state->codepoint = byte & 0x1fU;
        state->min_codepoint = 0x80U;
      } else if (byte >= 0xe0U && byte <= 0xefU) {
        state->remaining = 2;
        state->codepoint = byte & 0x0fU;
        state->min_codepoint = 0x800U;
      } else if (byte >= 0xf0U && byte <= 0xf4U) {
        state->remaining = 3;
        state->codepoint = byte & 0x07U;
        state->min_codepoint = 0x10000U;
      } else {
        return 0;
      }
    } else {
      if ((byte & 0xc0U) != 0x80U) {
        return 0;
      }
      state->codepoint = (state->codepoint << 6U) | (byte & 0x3fU);
      --state->remaining;
      if (state->remaining == 0 &&
          (state->codepoint < state->min_codepoint ||
           state->codepoint > 0x10ffffU ||
           (state->codepoint >= 0xd800U && state->codepoint <= 0xdfffU))) {
        return 0;
      }
    }
  }
  return 1;
}

#ifndef LC_UTF8_H
#define LC_UTF8_H

#include <stddef.h>

/* Incremental encoding check, independent of JSON or textfile syntax. A zeroed
 * state starts a value; remaining must be zero at its end. Invalid state must
 * not be reused. No allocation or retained input. */
typedef struct lc_utf8_state {
  int remaining;
  unsigned int codepoint;
  unsigned int min_codepoint;
} lc_utf8_state;

int lc_utf8_push(lc_utf8_state *state, const unsigned char *bytes,
                 size_t length);

#endif

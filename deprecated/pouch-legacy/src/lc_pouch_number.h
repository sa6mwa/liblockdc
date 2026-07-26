#ifndef LC_POUCH_NUMBER_H
#define LC_POUCH_NUMBER_H

#include <stddef.h>

int lc_pouch_number_eq_key(const char *data, size_t len, char **out);
int lc_pouch_number_eq_key_compare(const char *left, const char *right,
                                   int *out);

#endif

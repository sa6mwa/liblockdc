#ifndef LC_INTCOMPAT_H
#define LC_INTCOMPAT_H

#include <stddef.h>

#if defined(_MSC_VER)
typedef __int64 lc_i64;
typedef unsigned __int64 lc_u64;
#define LC_I64_SUPPORTED 1
#elif defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wlong-long"
typedef long long lc_i64;
typedef unsigned long long lc_u64;
#pragma clang diagnostic pop
#define LC_I64_SUPPORTED 1
#elif defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wlong-long"
typedef long long lc_i64;
typedef unsigned long long lc_u64;
#pragma GCC diagnostic pop
#define LC_I64_SUPPORTED 1
#else
#define LC_I64_SUPPORTED 0
#endif

#if !LC_I64_SUPPORTED
#error                                                                         \
    "liblockdc requires compiler-specific 64-bit integer support for internal parsing"
#endif

int lc_i64_parse_base10(const char *text, lc_i64 *out_value);
int lc_i64_format_base10(lc_i64 value, char *buffer, size_t buffer_size);
int lc_u64_parse_base10(const char *text, lc_u64 *out_value);
int lc_i64_to_long_checked(lc_i64 value, long *out_value);
int lc_i64_to_int_checked(lc_i64 value, int *out_value);
int lc_u64_to_ulong_checked(lc_u64 value, unsigned long *out_value);
int lc_u64_to_size_checked(lc_u64 value, size_t *out_value);
int lc_parse_long_base10_checked(const char *text, long *out_value);
int lc_parse_int_base10_checked(const char *text, int *out_value);
int lc_parse_ulong_base10_checked(const char *text, unsigned long *out_value);
int lc_parse_size_base10_checked(const char *text, size_t *out_value);
int lc_parse_long_base10_range_checked(const char *text, size_t length,
                                       long *out_value);
int lc_parse_ulong_base10_range_checked(const char *text, size_t length,
                                        unsigned long *out_value);

#endif

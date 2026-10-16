#ifndef LC_ALLOCATION_TRACKING_H
#define LC_ALLOCATION_TRACKING_H

#include <stddef.h>

#if defined(__SANITIZE_ADDRESS__)
#define LC_ALLOCATION_ASAN 1
#elif defined(__clang__)
#if __has_feature(address_sanitizer)
#define LC_ALLOCATION_ASAN 1
#endif
#endif
#ifdef LC_ALLOCATION_ASAN
#include <sanitizer/asan_interface.h>
#endif

/* Selected once at startup. Off points directly to libc, with no tracking
 * branch, header, counter update, lock or lazy initialization on allocation. */
extern void *(*lc_builtin_malloc)(size_t size);
extern void *(*lc_builtin_realloc)(void *pointer, size_t size);
extern void (*lc_builtin_free)(void *pointer);

/* Size-only metadata supports custom malloc/free pairs without realloc.
 * This domain never participates in tracking, regardless of process settings.
 * Full custom hook sets are called directly without this metadata. */
void *lc_sized_malloc(void *(*allocate)(void *, size_t), void *context,
                      size_t size);
void *lc_sized_realloc(void *(*allocate)(void *, size_t),
                       void (*release)(void *, void *), void *context,
                       void *pointer, size_t size);
void lc_sized_free(void (*release)(void *, void *), void *context,
                   void *pointer);

#endif

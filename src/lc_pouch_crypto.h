#ifndef LC_POUCH_CRYPTO_H
#define LC_POUCH_CRYPTO_H

#include "lc/lc.h"

#include <stddef.h>
#include <stdint.h>

typedef struct lc_pouch_crypto lc_pouch_crypto;

/* The inline record path is intentionally bounded. Larger values remain on
 * the one-pass streaming path and are never materialized by Pouch. */
#define LC_POUCH_CRYPTO_MEMORY_TRANSFORM_MAX_BYTES (64U * 1024U)

typedef struct lc_pouch_crypto_open_options {
  const char *key_string;
  const char *key_file;
  int generate_key_file;
  int compression_enabled;
} lc_pouch_crypto_open_options;

int lc_pouch_crypto_generate_key_string(char **out, lc_error *error);
void lc_pouch_crypto_key_string_free(char *key_string);
int lc_pouch_crypto_default_key_file(char **out, lc_error *error);
int lc_pouch_crypto_generate_key_file(const char *path, int overwrite,
                                      char **key_string_out, lc_error *error);

int lc_pouch_crypto_open(const lc_allocator *allocator,
                         const lc_pouch_crypto_open_options *options,
                         lc_pouch_crypto **out, char **key_file_out,
                         lc_error *error);
void lc_pouch_crypto_close(lc_pouch_crypto *crypto);
int lc_pouch_crypto_enabled(const lc_pouch_crypto *crypto);
int lc_pouch_crypto_compression_enabled(const lc_pouch_crypto *crypto);
int lc_pouch_crypto_key_id(const lc_pouch_crypto *crypto, char **out,
                           lc_error *error);
int lc_pouch_crypto_stream_to_file(lc_pouch_crypto *crypto, const char *context,
                                   const char *path, lc_source *body,
                                   uint64_t *plain_bytes,
                                   uint64_t *cipher_bytes,
                                   char **descriptor_out, lc_error *error);
int lc_pouch_crypto_stream_to_file_relaxed(
    lc_pouch_crypto *crypto, const char *context, const char *path,
    lc_source *body, uint64_t *plain_bytes, uint64_t *cipher_bytes,
    char **descriptor_out, lc_error *error);
int lc_pouch_crypto_stream_to_file_relaxed_uncompressed(
    lc_pouch_crypto *crypto, const char *context, const char *path,
    lc_source *body, uint64_t *plain_bytes, uint64_t *cipher_bytes,
    char **descriptor_out, lc_error *error);
int lc_pouch_crypto_stream_to_fd(lc_pouch_crypto *crypto, const char *context,
                                 int fd, lc_source *body, uint64_t *plain_bytes,
                                 uint64_t *cipher_bytes, char **descriptor_out,
                                 lc_error *error);
int lc_pouch_crypto_stream_to_fd_crc(lc_pouch_crypto *crypto,
                                     const char *context, int fd,
                                     lc_source *body, uint64_t *plain_bytes,
                                     uint64_t *cipher_bytes,
                                     unsigned long *stored_crc,
                                     char **descriptor_out, lc_error *error);
int lc_pouch_crypto_stream_to_fd_crc_with_compression(
    lc_pouch_crypto *crypto, const char *context, int fd, lc_source *body,
    uint64_t *plain_bytes, uint64_t *cipher_bytes, unsigned long *stored_crc,
    char **descriptor_out, int allow_compression, lc_error *error);
/* Transforms a bounded, already-materialized payload without touching a
 * source. Callers retain ownership of `plain`; `stored_out` uses Pouch's
 * allocator and must be released with lc_free_with_allocator(). */
int lc_pouch_crypto_transform_memory(
    lc_pouch_crypto *crypto, const char *context, const unsigned char *plain,
    size_t plain_length, int allow_compression, unsigned char **stored_out,
    size_t *stored_length_out, unsigned long *stored_crc_out,
    char **descriptor_out, lc_error *error);
int lc_pouch_crypto_rebind_descriptor(lc_pouch_crypto *crypto,
                                      const char *descriptor,
                                      const char *old_binding_context,
                                      const char *new_binding_context,
                                      char **out, lc_error *error);
int lc_pouch_crypto_source_from_file(lc_pouch_crypto *crypto,
                                     const char *context,
                                     const char *binding_context,
                                     const char *path, const char *descriptor,
                                     lc_source **out, lc_error *error);
int lc_pouch_crypto_source_from_file_span(
    lc_pouch_crypto *crypto, const char *context, const char *binding_context,
    const char *path, uint64_t offset, uint64_t length, const char *descriptor,
    lc_source **out, lc_error *error);
int lc_pouch_crypto_source_from_fd_span(lc_pouch_crypto *crypto,
                                        const char *context,
                                        const char *binding_context, int fd,
                                        uint64_t offset, uint64_t length,
                                        const char *descriptor, lc_source **out,
                                        lc_error *error);

#ifdef LOCKDC_TEST_BUILD
int lc_pouch_crypto_test_check_byte_counter(uint64_t total, size_t delta,
                                            lc_error *error);
int lc_pouch_crypto_test_generate_key_file_status(const char *path,
                                                  int *already_exists,
                                                  lc_error *error);
void lc_pouch_crypto_test_set_random_material(const unsigned char *bytes,
                                              size_t length);
int lc_pouch_crypto_test_descriptor_compressed(const char *descriptor, int *out,
                                               lc_error *error);
int lc_pouch_crypto_test_descriptor_set_compressed(const char *descriptor,
                                                   int compressed, char **out,
                                                   lc_error *error);
#endif

#endif

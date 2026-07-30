#include "lc_pouch_crypto.h"

#include "lc_api_internal.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zlib.h>

#define LC_POUCH_KEY_PREFIX "lc-pouch-key-v1:"
#define LC_POUCH_DESC_PREFIX "lc-pouch-desc-v1:"
#define LC_POUCH_ROOT_KEY_BYTES 32U
#define LC_POUCH_NONCE_PREFIX_BYTES 8U
#define LC_POUCH_NONCE_BYTES 12U
#define LC_POUCH_DEK_BYTES 32U
#define LC_POUCH_GCM_TAG_BYTES 16U
#define LC_POUCH_FIRST_FRAME_PLAINTEXT_BYTES (4U * 1024U)
#define LC_POUCH_FRAME_PLAINTEXT_BYTES (64U * 1024U)
#define LC_POUCH_DESC_RAW_BYTES (1U + 4U + LC_POUCH_NONCE_PREFIX_BYTES)
#define LC_POUCH_DESC_FLAG_ENCRYPTED 0x01U
#define LC_POUCH_DESC_FLAG_COMPRESSED 0x02U
#define LC_POUCH_ZLIB_CHUNK_BYTES (64U * 1024U)

struct lc_pouch_crypto {
  lc_allocator allocator;
  int encryption_enabled;
  int compression_enabled;
  unsigned char root_key[LC_POUCH_ROOT_KEY_BYTES];
  unsigned char data_key[LC_POUCH_DEK_BYTES];
};

typedef struct lc_pouch_crypto_desc {
  int encrypted;
  int compressed;
  unsigned long frame_size;
  unsigned char nonce_prefix[LC_POUCH_NONCE_PREFIX_BYTES];
} lc_pouch_crypto_desc;

typedef struct lc_pouch_crypto_source {
  lc_source pub;
  lc_allocator allocator;
  FILE *fp;
  EVP_CIPHER_CTX *ctx;
  unsigned char key[LC_POUCH_DEK_BYTES];
  unsigned char nonce_prefix[LC_POUCH_NONCE_PREFIX_BYTES];
  char *context;
  size_t context_len;
  unsigned char plain[LC_POUCH_FRAME_PLAINTEXT_BYTES];
  unsigned char cipher[LC_POUCH_FRAME_PLAINTEXT_BYTES + LC_POUCH_GCM_TAG_BYTES];
  size_t plain_offset;
  size_t plain_length;
  unsigned long frame_size;
  unsigned long counter;
  unsigned long span_offset;
  unsigned long span_length;
  unsigned long span_remaining;
  int bounded;
  int done;
} lc_pouch_crypto_source;

typedef struct lc_pouch_plain_span_source {
  lc_source pub;
  lc_allocator allocator;
  FILE *fp;
  unsigned long offset;
  unsigned long length;
  unsigned long read_bytes;
} lc_pouch_plain_span_source;

typedef struct lc_pouch_zlib_source {
  lc_source pub;
  lc_allocator allocator;
  lc_source *inner;
  z_stream stream;
  unsigned char input[LC_POUCH_ZLIB_CHUNK_BYTES];
  unsigned long input_total;
  int deflate_mode;
  int close_inner;
  int input_done;
  int stream_done;
} lc_pouch_zlib_source;

static const char lc_pouch_b64url_alphabet[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

static int lc_pouch_crypto_random_bytes(unsigned char *out, size_t len) {
#ifdef FUZZING_BUILD_MODE_UNSAFE_FOR_PRODUCTION
  size_t i;

  if (out == NULL) {
    return 0;
  }
  for (i = 0U; i < len; ++i) {
    out[i] = (unsigned char)((i * 131U + len * 17U + 23U) & 0xFFU);
  }
  return 1;
#else
  return RAND_bytes(out, (int)len) == 1;
#endif
}

static char *lc_pouch_crypto_strdup(const lc_allocator *allocator,
                                    const char *text) {
  char *copy;
  size_t len;

  if (text == NULL) {
    return NULL;
  }
  len = strlen(text) + 1U;
  copy = (char *)lc_alloc_with_allocator(allocator, len);
  if (copy == NULL) {
    return NULL;
  }
  memcpy(copy, text, len);
  return copy;
}

static int
lc_pouch_crypto_derive_data_key(const lc_pouch_crypto *crypto,
                                unsigned char key[LC_POUCH_DEK_BYTES],
                                lc_error *error);

static int lc_pouch_crypto_write_all_fd(int fd, const void *bytes, size_t count,
                                        lc_error *error) {
  const unsigned char *cursor;

  cursor = (const unsigned char *)bytes;
  while (count > 0U) {
    ssize_t written;

    written = write(fd, cursor, count);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to write encrypted pouch payload",
                          strerror(errno), NULL, "pouch");
    }
    if (written == 0) {
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "short write while writing encrypted pouch payload",
                          NULL, NULL, "pouch");
    }
    cursor += (size_t)written;
    count -= (size_t)written;
  }
  return LC_OK;
}

static int lc_pouch_crypto_read_all_file(const lc_allocator *allocator,
                                         const char *path, char **out,
                                         lc_error *error) {
  FILE *fp;
  char *buffer;
  long size;
  size_t got;

  if (path == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch key file read requires path and out", NULL, NULL,
                        "pouch");
  }
  *out = NULL;
  fp = fopen(path, "rb");
  if (fp == NULL) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch crypto key file", strerror(errno),
                        NULL, "pouch");
  }
  if (fseek(fp, 0L, SEEK_END) != 0) {
    fclose(fp);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to seek pouch crypto key file", strerror(errno),
                        NULL, "pouch");
  }
  size = ftell(fp);
  if (size < 0L || size > 8192L) {
    fclose(fp);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto key file is invalid", NULL, NULL,
                        "pouch");
  }
  if (fseek(fp, 0L, SEEK_SET) != 0) {
    fclose(fp);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to rewind pouch crypto key file",
                        strerror(errno), NULL, "pouch");
  }
  buffer = (char *)lc_alloc_with_allocator(allocator, (size_t)size + 1U);
  if (buffer == NULL) {
    fclose(fp);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto key buffer", NULL,
                        NULL, "pouch");
  }
  got = fread(buffer, 1U, (size_t)size, fp);
  if (got != (size_t)size) {
    lc_free_with_allocator(allocator, buffer);
    fclose(fp);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch crypto key file", NULL, NULL,
                        "pouch");
  }
  fclose(fp);
  while (got > 0U && (buffer[got - 1U] == '\n' || buffer[got - 1U] == '\r' ||
                      buffer[got - 1U] == ' ' || buffer[got - 1U] == '\t')) {
    --got;
  }
  buffer[got] = '\0';
  *out = buffer;
  return LC_OK;
}

static int lc_pouch_crypto_mkdirs(const lc_allocator *allocator,
                                  const char *path, lc_error *error) {
  char *copy;
  char *cursor;

  if (path == NULL || path[0] == '\0') {
    return LC_OK;
  }
  copy = lc_pouch_crypto_strdup(allocator, path);
  if (copy == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto path", NULL, NULL,
                        "pouch");
  }
  cursor = copy;
  if (*cursor == '/') {
    ++cursor;
  }
  for (; *cursor != '\0'; ++cursor) {
    if (*cursor == '/') {
      *cursor = '\0';
      if (copy[0] != '\0' && mkdir(copy, 0700) != 0 && errno != EEXIST) {
        lc_free_with_allocator(allocator, copy);
        return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                            "failed to create pouch crypto config directory",
                            strerror(errno), NULL, "pouch");
      }
      *cursor = '/';
    }
  }
  if (mkdir(copy, 0700) != 0 && errno != EEXIST) {
    lc_free_with_allocator(allocator, copy);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create pouch crypto config directory",
                        strerror(errno), NULL, "pouch");
  }
  lc_free_with_allocator(allocator, copy);
  return LC_OK;
}

static char *lc_pouch_crypto_dirname(const lc_allocator *allocator,
                                     const char *path) {
  const char *slash;
  char *out;
  size_t len;

  if (path == NULL) {
    return NULL;
  }
  slash = strrchr(path, '/');
  if (slash == NULL) {
    return lc_pouch_crypto_strdup(allocator, ".");
  }
  if (slash == path) {
    return lc_pouch_crypto_strdup(allocator, "/");
  }
  len = (size_t)(slash - path);
  out = (char *)lc_alloc_with_allocator(allocator, len + 1U);
  if (out == NULL) {
    return NULL;
  }
  memcpy(out, path, len);
  out[len] = '\0';
  return out;
}

static char *lc_pouch_crypto_temp_path(const char *path, unsigned int attempt) {
  char suffix[64];
  char *out;
  size_t path_len;
  size_t suffix_len;

  if (path == NULL) {
    return NULL;
  }
  snprintf(suffix, sizeof(suffix), ".tmp.%ld.%u", (long)getpid(), attempt);
  path_len = strlen(path);
  suffix_len = strlen(suffix);
  out = (char *)lc_alloc_with_allocator(NULL, path_len + suffix_len + 1U);
  if (out == NULL) {
    return NULL;
  }
  memcpy(out, path, path_len);
  memcpy(out + path_len, suffix, suffix_len + 1U);
  return out;
}

static int lc_pouch_crypto_fsync_dirname(const char *path, lc_error *error) {
  char *dir;
  int fd;
  int flags;
  int rc;

  dir = lc_pouch_crypto_dirname(NULL, path);
  if (dir == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto key directory", NULL,
                        NULL, "pouch");
  }
  flags = O_RDONLY;
#ifdef O_DIRECTORY
  flags |= O_DIRECTORY;
#endif
  fd = open(dir, flags);
  lc_free_with_allocator(NULL, dir);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch crypto key directory",
                        strerror(errno), NULL, "pouch");
  }
  rc = LC_OK;
  if (fsync(fd) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to fsync pouch crypto key directory",
                      strerror(errno), NULL, "pouch");
  }
  if (close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch crypto key directory",
                      strerror(errno), NULL, "pouch");
  }
  return rc;
}

static int lc_pouch_b64url_index(char ch) {
  const char *found;

  found = strchr(lc_pouch_b64url_alphabet, ch);
  if (found == NULL) {
    return -1;
  }
  return (int)(found - lc_pouch_b64url_alphabet);
}

static char *lc_pouch_b64url_encode(const unsigned char *bytes, size_t length) {
  char *out;
  size_t out_len;
  size_t src;
  size_t dst;

  out_len = ((length + 2U) / 3U) * 4U;
  while (out_len > 0U && (out_len * 3U) / 4U > length) {
    --out_len;
  }
  out = (char *)lc_alloc_with_allocator(NULL, out_len + 1U);
  if (out == NULL) {
    return NULL;
  }
  src = 0U;
  dst = 0U;
  while (src + 3U <= length) {
    unsigned long value;

    value = ((unsigned long)bytes[src] << 16) |
            ((unsigned long)bytes[src + 1U] << 8) | bytes[src + 2U];
    out[dst++] = lc_pouch_b64url_alphabet[(value >> 18) & 0x3FUL];
    out[dst++] = lc_pouch_b64url_alphabet[(value >> 12) & 0x3FUL];
    out[dst++] = lc_pouch_b64url_alphabet[(value >> 6) & 0x3FUL];
    out[dst++] = lc_pouch_b64url_alphabet[value & 0x3FUL];
    src += 3U;
  }
  if (src < length) {
    unsigned long value;

    value = (unsigned long)bytes[src] << 16;
    if (src + 1U < length) {
      value |= (unsigned long)bytes[src + 1U] << 8;
    }
    out[dst++] = lc_pouch_b64url_alphabet[(value >> 18) & 0x3FUL];
    out[dst++] = lc_pouch_b64url_alphabet[(value >> 12) & 0x3FUL];
    if (src + 1U < length) {
      out[dst++] = lc_pouch_b64url_alphabet[(value >> 6) & 0x3FUL];
    }
  }
  out[dst] = '\0';
  return out;
}

static int lc_pouch_b64url_decode(const char *text, unsigned char **out,
                                  size_t *out_len, lc_error *error) {
  unsigned char *bytes;
  size_t text_len;
  size_t max_len;
  size_t src;
  size_t dst;

  if (text == NULL || out == NULL || out_len == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch base64url decode requires inputs", NULL, NULL,
                        "pouch");
  }
  *out = NULL;
  *out_len = 0U;
  text_len = strlen(text);
  if (text_len == 0U || text_len % 4U == 1U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch base64url value is invalid", NULL, NULL,
                        "pouch");
  }
  max_len = (text_len / 4U) * 3U + 3U;
  bytes = (unsigned char *)lc_alloc_with_allocator(NULL, max_len);
  if (bytes == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch base64url decode buffer",
                        NULL, NULL, "pouch");
  }
  src = 0U;
  dst = 0U;
  while (src < text_len) {
    int vals[4];
    size_t take;
    unsigned long value;
    size_t i;

    take = text_len - src;
    if (take > 4U) {
      take = 4U;
    }
    if (take < 2U) {
      lc_free_with_allocator(NULL, bytes);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch base64url value is truncated", NULL, NULL,
                          "pouch");
    }
    vals[2] = 0;
    vals[3] = 0;
    for (i = 0U; i < take; ++i) {
      vals[i] = lc_pouch_b64url_index(text[src + i]);
      if (vals[i] < 0) {
        lc_free_with_allocator(NULL, bytes);
        return lc_error_set(error, LC_ERR_INVALID, 0L,
                            "pouch base64url value contains invalid bytes",
                            NULL, NULL, "pouch");
      }
    }
    value = ((unsigned long)vals[0] << 18) | ((unsigned long)vals[1] << 12) |
            ((unsigned long)vals[2] << 6) | (unsigned long)vals[3];
    bytes[dst++] = (unsigned char)((value >> 16) & 0xFFU);
    if (take >= 3U) {
      bytes[dst++] = (unsigned char)((value >> 8) & 0xFFU);
    }
    if (take >= 4U) {
      bytes[dst++] = (unsigned char)(value & 0xFFU);
    }
    src += take;
  }
  *out = bytes;
  *out_len = dst;
  return LC_OK;
}

int lc_pouch_crypto_generate_key_string(char **out, lc_error *error) {
  unsigned char key[LC_POUCH_ROOT_KEY_BYTES];
  char *encoded;
  char *result;
  size_t prefix_len;
  size_t encoded_len;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_crypto_generate_key_string requires out",
                        NULL, NULL, "pouch");
  }
  *out = NULL;
  if (!lc_pouch_crypto_random_bytes(key, sizeof(key))) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to generate pouch crypto root key", NULL, NULL,
                        "pouch");
  }
  encoded = lc_pouch_b64url_encode(key, sizeof(key));
  OPENSSL_cleanse(key, sizeof(key));
  if (encoded == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to encode pouch crypto root key", NULL, NULL,
                        "pouch");
  }
  prefix_len = strlen(LC_POUCH_KEY_PREFIX);
  encoded_len = strlen(encoded);
  result = (char *)lc_alloc_with_allocator(NULL, prefix_len + encoded_len + 1U);
  if (result == NULL) {
    lc_pouch_crypto_key_string_free(encoded);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto root key string", NULL,
                        NULL, "pouch");
  }
  memcpy(result, LC_POUCH_KEY_PREFIX, prefix_len);
  memcpy(result + prefix_len, encoded, encoded_len + 1U);
  lc_pouch_crypto_key_string_free(encoded);
  *out = result;
  return LC_OK;
}

void lc_pouch_crypto_key_string_free(char *key_string) {
  if (key_string == NULL) {
    return;
  }
  OPENSSL_cleanse(key_string, strlen(key_string));
  lc_free_with_allocator(NULL, key_string);
}

int lc_pouch_crypto_default_key_file(char **out, lc_error *error) {
  const char *xdg;
  const char *home;
  const char *prefix;
  const char *suffix;
  char *path;
  size_t len;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_crypto_default_key_file requires out", NULL,
                        NULL, "pouch");
  }
  *out = NULL;
  xdg = getenv("XDG_CONFIG_HOME");
  suffix = "/liblockdc/pouch.key";
  if (xdg != NULL && xdg[0] != '\0') {
    prefix = xdg;
    len = strlen(prefix) + strlen(suffix) + 1U;
    path = (char *)lc_alloc_with_allocator(NULL, len);
    if (path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch crypto key path", NULL,
                          NULL, "pouch");
    }
    snprintf(path, len, "%s%s", prefix, suffix);
    *out = path;
    return LC_OK;
  }
  home = getenv("HOME");
  if (home == NULL || home[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "HOME or XDG_CONFIG_HOME is required for default pouch "
                        "crypto key file",
                        NULL, NULL, "pouch");
  }
  suffix = "/.config/liblockdc/pouch.key";
  len = strlen(home) + strlen(suffix) + 1U;
  path = (char *)lc_alloc_with_allocator(NULL, len);
  if (path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto key path", NULL, NULL,
                        "pouch");
  }
  snprintf(path, len, "%s%s", home, suffix);
  *out = path;
  return LC_OK;
}

static int lc_pouch_crypto_generate_key_file_status(const char *path,
                                                    int overwrite,
                                                    char **key_string_out,
                                                    int *already_exists,
                                                    lc_error *error) {
  char *key;
  char *dir;
  char *tmp_path;
  unsigned int attempt;
  int fd;
  size_t len;
  int rc;

  if (already_exists != NULL) {
    *already_exists = 0;
  }
  if (path == NULL || path[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_crypto_generate_key_file requires path", NULL,
                        NULL, "pouch");
  }
  if (key_string_out != NULL) {
    *key_string_out = NULL;
  }
  key = NULL;
  tmp_path = NULL;
  rc = lc_pouch_crypto_generate_key_string(&key, error);
  if (rc != LC_OK) {
    return rc;
  }
  dir = lc_pouch_crypto_dirname(NULL, path);
  if (dir == NULL) {
    lc_pouch_crypto_key_string_free(key);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto key directory", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_crypto_mkdirs(NULL, dir, error);
  lc_free_with_allocator(NULL, dir);
  if (rc != LC_OK) {
    lc_pouch_crypto_key_string_free(key);
    return rc;
  }
  if (!overwrite) {
    if (access(path, F_OK) == 0) {
      if (already_exists != NULL) {
        *already_exists = 1;
      }
      lc_pouch_crypto_key_string_free(key);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to create pouch crypto key file",
                          "file already exists", NULL, "pouch");
    }
    if (errno != ENOENT) {
      lc_pouch_crypto_key_string_free(key);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to check pouch crypto key file",
                          strerror(errno), NULL, "pouch");
    }
  }
  fd = -1;
  for (attempt = 0U; attempt < 100U; ++attempt) {
    tmp_path = lc_pouch_crypto_temp_path(path, attempt);
    if (tmp_path == NULL) {
      lc_pouch_crypto_key_string_free(key);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch crypto key temp path", NULL,
                          NULL, "pouch");
    }
    fd = open(tmp_path, O_CREAT | O_EXCL | O_WRONLY, 0600);
    if (fd >= 0 || errno != EEXIST) {
      break;
    }
    lc_free_with_allocator(NULL, tmp_path);
    tmp_path = NULL;
  }
  if (fd < 0) {
    lc_free_with_allocator(NULL, tmp_path);
    lc_pouch_crypto_key_string_free(key);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create pouch crypto key file",
                        strerror(errno), NULL, "pouch");
  }
  if (fchmod(fd, 0600) != 0) {
    close(fd);
    unlink(tmp_path);
    lc_free_with_allocator(NULL, tmp_path);
    lc_pouch_crypto_key_string_free(key);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to set pouch crypto key file permissions",
                        strerror(errno), NULL, "pouch");
  }
  len = strlen(key);
  rc = lc_pouch_crypto_write_all_fd(fd, key, len, error);
  if (rc == LC_OK) {
    rc = lc_pouch_crypto_write_all_fd(fd, "\n", 1U, error);
  }
  if (rc == LC_OK && fsync(fd) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to fsync pouch crypto key file", strerror(errno),
                      NULL, "pouch");
  }
  if (close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch crypto key file", strerror(errno),
                      NULL, "pouch");
  }
  if (rc == LC_OK) {
    if (overwrite) {
      if (rename(tmp_path, path) != 0) {
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to replace pouch crypto key file",
                          strerror(errno), NULL, "pouch");
      }
    } else {
      if (link(tmp_path, path) != 0) {
        int saved_errno;

        saved_errno = errno;
        if (saved_errno == EEXIST && already_exists != NULL) {
          *already_exists = 1;
        }
        rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to create pouch crypto key file",
                          strerror(saved_errno), NULL, "pouch");
      }
      unlink(tmp_path);
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_crypto_fsync_dirname(path, error);
  }
  if (rc != LC_OK) {
    unlink(tmp_path);
    lc_free_with_allocator(NULL, tmp_path);
    lc_pouch_crypto_key_string_free(key);
    return rc;
  }
  lc_free_with_allocator(NULL, tmp_path);
  if (key_string_out != NULL) {
    *key_string_out = key;
  } else {
    lc_pouch_crypto_key_string_free(key);
  }
  return LC_OK;
}

int lc_pouch_crypto_generate_key_file(const char *path, int overwrite,
                                      char **key_string_out, lc_error *error) {
  return lc_pouch_crypto_generate_key_file_status(path, overwrite,
                                                  key_string_out, NULL, error);
}

#ifdef LOCKDC_TEST_BUILD
int lc_pouch_crypto_test_generate_key_file_status(const char *path,
                                                  int *already_exists,
                                                  lc_error *error) {
  return lc_pouch_crypto_generate_key_file_status(path, 0, NULL, already_exists,
                                                  error);
}
#endif

static int lc_pouch_crypto_parse_key(const char *key_string,
                                     unsigned char root_key[32],
                                     lc_error *error) {
  const char *encoded;
  unsigned char *decoded;
  size_t decoded_len;
  int rc;

  if (key_string == NULL || strncmp(key_string, LC_POUCH_KEY_PREFIX,
                                    sizeof(LC_POUCH_KEY_PREFIX) - 1U) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto key must use lc-pouch-key-v1 format",
                        NULL, NULL, "pouch");
  }
  encoded = key_string + sizeof(LC_POUCH_KEY_PREFIX) - 1U;
  decoded = NULL;
  decoded_len = 0U;
  rc = lc_pouch_b64url_decode(encoded, &decoded, &decoded_len, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (decoded_len != LC_POUCH_ROOT_KEY_BYTES) {
    lc_free_with_allocator(NULL, decoded);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto root key must be 32 bytes", NULL, NULL,
                        "pouch");
  }
  memcpy(root_key, decoded, LC_POUCH_ROOT_KEY_BYTES);
  OPENSSL_cleanse(decoded, decoded_len);
  lc_free_with_allocator(NULL, decoded);
  return LC_OK;
}

int lc_pouch_crypto_open(const lc_allocator *allocator,
                         const lc_pouch_crypto_open_options *options,
                         lc_pouch_crypto **out, char **key_file_out,
                         lc_error *error) {
  lc_pouch_crypto *crypto;
  char *loaded_key;
  char *resolved_key_file;
  const char *key_string;
  int key_file_already_exists;
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_crypto_open requires out", NULL, NULL,
                        "pouch");
  }
  *out = NULL;
  if (key_file_out != NULL) {
    *key_file_out = NULL;
  }
  if (options == NULL ||
      ((options->key_string == NULL || options->key_string[0] == '\0') &&
       (options->key_file == NULL || options->key_file[0] == '\0') &&
       !options->generate_key_file && !options->compression_enabled)) {
    return LC_OK;
  }
  if ((options->key_string == NULL || options->key_string[0] == '\0') &&
      (options->key_file == NULL || options->key_file[0] == '\0') &&
      !options->generate_key_file) {
    crypto = (lc_pouch_crypto *)lc_calloc_with_allocator(allocator, 1U,
                                                         sizeof(*crypto));
    if (crypto == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch transform provider", NULL,
                          NULL, "pouch");
    }
    if (allocator != NULL) {
      crypto->allocator = *allocator;
    } else {
      lc_allocator_init(&crypto->allocator);
    }
    crypto->compression_enabled = options->compression_enabled ? 1 : 0;
    *out = crypto;
    return LC_OK;
  }
  loaded_key = NULL;
  resolved_key_file = NULL;
  key_string = options->key_string;
  if (key_string == NULL || key_string[0] == '\0') {
    if (options->key_file != NULL && options->key_file[0] != '\0') {
      resolved_key_file = lc_pouch_crypto_strdup(allocator, options->key_file);
      if (resolved_key_file == NULL) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch crypto key file path",
                            NULL, NULL, "pouch");
      }
    } else {
      char *default_key_file;

      default_key_file = NULL;
      rc = lc_pouch_crypto_default_key_file(&default_key_file, error);
      if (rc != LC_OK) {
        return rc;
      }
      resolved_key_file = lc_pouch_crypto_strdup(allocator, default_key_file);
      lc_pouch_crypto_key_string_free(default_key_file);
      if (resolved_key_file == NULL) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch crypto key file path",
                            NULL, NULL, "pouch");
      }
    }
    rc = lc_pouch_crypto_read_all_file(NULL, resolved_key_file, &loaded_key,
                                       error);
    if (rc != LC_OK && options->generate_key_file) {
      lc_error_cleanup(error);
      key_file_already_exists = 0;
      rc = lc_pouch_crypto_generate_key_file_status(
          resolved_key_file, 0, &loaded_key, &key_file_already_exists, error);
      if (rc != LC_OK && key_file_already_exists) {
        lc_error_cleanup(error);
        rc = lc_pouch_crypto_read_all_file(NULL, resolved_key_file, &loaded_key,
                                           error);
      }
    }
    if (rc != LC_OK) {
      lc_free_with_allocator(allocator, resolved_key_file);
      return rc;
    }
    key_string = loaded_key;
  }
  crypto = (lc_pouch_crypto *)lc_calloc_with_allocator(allocator, 1U,
                                                       sizeof(*crypto));
  if (crypto == NULL) {
    lc_pouch_crypto_key_string_free(loaded_key);
    lc_free_with_allocator(allocator, resolved_key_file);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto provider", NULL, NULL,
                        "pouch");
  }
  if (allocator != NULL) {
    crypto->allocator = *allocator;
  } else {
    lc_allocator_init(&crypto->allocator);
  }
  crypto->encryption_enabled = 1;
  crypto->compression_enabled = options->compression_enabled ? 1 : 0;
  rc = lc_pouch_crypto_parse_key(key_string, crypto->root_key, error);
  lc_pouch_crypto_key_string_free(loaded_key);
  if (rc != LC_OK) {
    lc_pouch_crypto_close(crypto);
    lc_free_with_allocator(allocator, resolved_key_file);
    return rc;
  }
  rc = lc_pouch_crypto_derive_data_key(crypto, crypto->data_key, error);
  if (rc != LC_OK) {
    lc_pouch_crypto_close(crypto);
    lc_free_with_allocator(allocator, resolved_key_file);
    return rc;
  }
  *out = crypto;
  if (key_file_out != NULL) {
    *key_file_out = resolved_key_file;
  } else {
    lc_free_with_allocator(allocator, resolved_key_file);
  }
  return LC_OK;
}

void lc_pouch_crypto_close(lc_pouch_crypto *crypto) {
  lc_allocator allocator;

  if (crypto == NULL) {
    return;
  }
  allocator = crypto->allocator;
  OPENSSL_cleanse(crypto->root_key, sizeof(crypto->root_key));
  OPENSSL_cleanse(crypto->data_key, sizeof(crypto->data_key));
  lc_free_with_allocator(&allocator, crypto);
}

int lc_pouch_crypto_enabled(const lc_pouch_crypto *crypto) {
  return crypto != NULL && crypto->encryption_enabled;
}

int lc_pouch_crypto_compression_enabled(const lc_pouch_crypto *crypto) {
  return crypto != NULL && crypto->compression_enabled;
}

int lc_pouch_crypto_key_id(const lc_pouch_crypto *crypto, char **out,
                           lc_error *error) {
  unsigned char digest[EVP_MAX_MD_SIZE];
  const char label[] = "liblockdc-pouch-root-key-id-v1";
  const char prefix[] = "hmac-sha256:";
  char *encoded;
  char *key_id;
  size_t digest_len;
  size_t encoded_len;
  size_t prefix_len;

  if (crypto == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto key id requires crypto and out", NULL,
                        NULL, "pouch");
  }
  *out = NULL;
  digest_len = 0U;
  if (EVP_Q_mac(NULL, "HMAC", NULL, "SHA256", NULL, crypto->root_key,
                sizeof(crypto->root_key), (const unsigned char *)label,
                sizeof(label) - 1U, digest, sizeof(digest),
                &digest_len) == NULL ||
      digest_len == 0U) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to derive pouch crypto key id", NULL, NULL,
                        "pouch");
  }
  encoded = lc_pouch_b64url_encode(digest, digest_len);
  OPENSSL_cleanse(digest, sizeof(digest));
  if (encoded == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto key id digest", NULL,
                        NULL, "pouch");
  }
  prefix_len = sizeof(prefix) - 1U;
  encoded_len = strlen(encoded);
  key_id = (char *)lc_alloc_with_allocator(NULL, prefix_len + encoded_len + 1U);
  if (key_id == NULL) {
    lc_free_with_allocator(NULL, encoded);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto key id", NULL, NULL,
                        "pouch");
  }
  memcpy(key_id, prefix, prefix_len);
  memcpy(key_id + prefix_len, encoded, encoded_len + 1U);
  lc_free_with_allocator(NULL, encoded);
  *out = key_id;
  return LC_OK;
}

static void lc_pouch_crypto_put32(unsigned char out[4], unsigned long value) {
  out[0] = (unsigned char)((value >> 24) & 0xFFU);
  out[1] = (unsigned char)((value >> 16) & 0xFFU);
  out[2] = (unsigned char)((value >> 8) & 0xFFU);
  out[3] = (unsigned char)(value & 0xFFU);
}

static unsigned long lc_pouch_crypto_get32(const unsigned char in[4]) {
  return ((unsigned long)in[0] << 24) | ((unsigned long)in[1] << 16) |
         ((unsigned long)in[2] << 8) | (unsigned long)in[3];
}

static int
lc_pouch_crypto_derive_data_key(const lc_pouch_crypto *crypto,
                                unsigned char key[LC_POUCH_DEK_BYTES],
                                lc_error *error) {
  const char label[] = "lc-pouch-aes-256-gcm-data-key";
  size_t out_len;

  if (crypto == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto data key derivation requires crypto",
                        NULL, NULL, "pouch");
  }
  out_len = 0U;
  if (EVP_Q_mac(NULL, "HMAC", NULL, "SHA256", NULL, crypto->root_key,
                sizeof(crypto->root_key), (const unsigned char *)label,
                sizeof(label) - 1U, key, LC_POUCH_DEK_BYTES,
                &out_len) == NULL ||
      out_len != LC_POUCH_DEK_BYTES) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to derive pouch crypto data key", NULL, NULL,
                        "pouch");
  }
  return LC_OK;
}

static int lc_pouch_crypto_descriptor_encode(const lc_allocator *allocator,
                                             const lc_pouch_crypto_desc *desc,
                                             char **out, lc_error *error) {
  unsigned char raw[LC_POUCH_DESC_RAW_BYTES];
  char *encoded;
  char *result;
  size_t prefix_len;
  size_t encoded_len;

  raw[0] = 0U;
  if (desc->encrypted) {
    raw[0] = (unsigned char)(raw[0] | LC_POUCH_DESC_FLAG_ENCRYPTED);
  }
  if (desc->compressed) {
    raw[0] = (unsigned char)(raw[0] | LC_POUCH_DESC_FLAG_COMPRESSED);
  }
  lc_pouch_crypto_put32(raw + 1U, desc->frame_size);
  memcpy(raw + 5U, desc->nonce_prefix, LC_POUCH_NONCE_PREFIX_BYTES);
  encoded = lc_pouch_b64url_encode(raw, sizeof(raw));
  OPENSSL_cleanse(raw, sizeof(raw));
  if (encoded == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to encode pouch crypto descriptor", NULL, NULL,
                        "pouch");
  }
  prefix_len = strlen(LC_POUCH_DESC_PREFIX);
  encoded_len = strlen(encoded);
  result =
      (char *)lc_alloc_with_allocator(allocator, prefix_len + encoded_len + 1U);
  if (result == NULL) {
    lc_free_with_allocator(NULL, encoded);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto descriptor", NULL,
                        NULL, "pouch");
  }
  memcpy(result, LC_POUCH_DESC_PREFIX, prefix_len);
  memcpy(result + prefix_len, encoded, encoded_len + 1U);
  lc_free_with_allocator(NULL, encoded);
  *out = result;
  return LC_OK;
}

static int lc_pouch_crypto_descriptor_decode(const char *descriptor,
                                             lc_pouch_crypto_desc *out,
                                             lc_error *error) {
  const char *encoded;
  unsigned char *raw;
  size_t raw_len;
  int rc;

  if (descriptor == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto descriptor is missing or invalid", NULL,
                        NULL, "pouch");
  }
  memset(out, 0, sizeof(*out));
  if (strncmp(descriptor, LC_POUCH_DESC_PREFIX,
              sizeof(LC_POUCH_DESC_PREFIX) - 1U) != 0) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto descriptor is missing or invalid", NULL,
                        NULL, "pouch");
  }
  encoded = descriptor + sizeof(LC_POUCH_DESC_PREFIX) - 1U;
  raw = NULL;
  raw_len = 0U;
  rc = lc_pouch_b64url_decode(encoded, &raw, &raw_len, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (raw_len != LC_POUCH_DESC_RAW_BYTES) {
    lc_free_with_allocator(NULL, raw);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto descriptor has invalid length", NULL,
                        NULL, "pouch");
  }
  if ((raw[0] &
       ~(LC_POUCH_DESC_FLAG_ENCRYPTED | LC_POUCH_DESC_FLAG_COMPRESSED)) != 0U ||
      (raw[0] &
       (LC_POUCH_DESC_FLAG_ENCRYPTED | LC_POUCH_DESC_FLAG_COMPRESSED)) == 0U) {
    lc_free_with_allocator(NULL, raw);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto descriptor has invalid flags", NULL, NULL,
                        "pouch");
  }
  out->encrypted = (raw[0] & LC_POUCH_DESC_FLAG_ENCRYPTED) != 0U ? 1 : 0;
  out->compressed = (raw[0] & LC_POUCH_DESC_FLAG_COMPRESSED) != 0U ? 1 : 0;
  out->frame_size = lc_pouch_crypto_get32(raw + 1U);
  if (out->encrypted && (out->frame_size == 0UL ||
                         out->frame_size > LC_POUCH_FRAME_PLAINTEXT_BYTES)) {
    lc_free_with_allocator(NULL, raw);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto descriptor has invalid frame size", NULL,
                        NULL, "pouch");
  }
  if (!out->encrypted && out->frame_size != 0UL) {
    lc_free_with_allocator(NULL, raw);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto descriptor has invalid plaintext frame",
                        NULL, NULL, "pouch");
  }
  memcpy(out->nonce_prefix, raw + 5U, LC_POUCH_NONCE_PREFIX_BYTES);
  OPENSSL_cleanse(raw, raw_len);
  lc_free_with_allocator(NULL, raw);
  return LC_OK;
}

static void lc_pouch_crypto_nonce(const unsigned char prefix[8],
                                  unsigned long counter,
                                  unsigned char nonce[12]) {
  memcpy(nonce, prefix, LC_POUCH_NONCE_PREFIX_BYTES);
  lc_pouch_crypto_put32(nonce + LC_POUCH_NONCE_PREFIX_BYTES, counter);
}

static int lc_pouch_crypto_check_byte_counter(unsigned long total, size_t delta,
                                              const char *message,
                                              lc_error *error) {
  if (delta > (size_t)ULONG_MAX || total > ULONG_MAX - (unsigned long)delta) {
    return lc_error_set(error, LC_ERR_INVALID, 0L, message, NULL, NULL,
                        "pouch");
  }
  return LC_OK;
}

#ifdef LOCKDC_TEST_BUILD
int lc_pouch_crypto_test_check_byte_counter(unsigned long total, size_t delta,
                                            lc_error *error) {
  return lc_pouch_crypto_check_byte_counter(
      total, delta, "pouch payload byte count exceeds platform limit", error);
}
#endif

static int lc_pouch_crypto_encrypt_frame(
    int fd, EVP_CIPHER_CTX *ctx, const unsigned char key[LC_POUCH_DEK_BYTES],
    const unsigned char nonce_prefix[LC_POUCH_NONCE_PREFIX_BYTES],
    const char *context, size_t context_len, unsigned long counter,
    const unsigned char *plain, size_t plain_len, unsigned long *cipher_total,
    unsigned long *stored_crc, lc_error *error) {
  unsigned char
      frame[8U + LC_POUCH_FRAME_PLAINTEXT_BYTES + LC_POUCH_GCM_TAG_BYTES];
  unsigned char *header;
  unsigned char *cipher;
  unsigned char *tag;
  unsigned char nonce[LC_POUCH_NONCE_BYTES];
  size_t cipher_len;
  size_t frame_len;
  int out_len;
  int final_len;
  int aad_len;
  int rc;

  header = frame;
  cipher = frame + 8U;
  lc_pouch_crypto_put32(header, counter);
  lc_pouch_crypto_put32(header + 4U, (unsigned long)plain_len);
  lc_pouch_crypto_nonce(nonce_prefix, counter, nonce);
  if (EVP_EncryptInit_ex(ctx, EVP_aes_256_gcm(), NULL, NULL, NULL) != 1 ||
      EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, sizeof(nonce), NULL) !=
          1 ||
      EVP_EncryptInit_ex(ctx, NULL, NULL, key, nonce) != 1 ||
      EVP_EncryptUpdate(ctx, NULL, &aad_len, header, 8) != 1 ||
      (context_len > 0U && EVP_EncryptUpdate(ctx, NULL, &aad_len,
                                             (const unsigned char *)context,
                                             (int)context_len) != 1
           ? 0
           : 1) == 0 ||
      EVP_EncryptUpdate(ctx, cipher, &out_len, plain, (int)plain_len) != 1 ||
      EVP_EncryptFinal_ex(ctx, cipher + out_len, &final_len) != 1) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to encrypt pouch payload frame", NULL, NULL,
                        "pouch");
  }
  cipher_len = (size_t)(out_len + final_len);
  tag = cipher + cipher_len;
  if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, LC_POUCH_GCM_TAG_BYTES,
                          tag) != 1) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to encrypt pouch payload frame", NULL, NULL,
                        "pouch");
  }
  frame_len = 8U + cipher_len + LC_POUCH_GCM_TAG_BYTES;
  if (cipher_total != NULL) {
    rc = lc_pouch_crypto_check_byte_counter(
        *cipher_total, frame_len,
        "pouch encrypted payload byte count exceeds platform limit", error);
    if (rc != LC_OK) {
      OPENSSL_cleanse(nonce, sizeof(nonce));
      return rc;
    }
  }
  if (stored_crc != NULL) {
    *stored_crc =
        (unsigned long)crc32((uLong)*stored_crc, frame, (uInt)frame_len);
  }
  rc = lc_pouch_crypto_write_all_fd(fd, frame, frame_len, error);
  if (rc == LC_OK && cipher_total != NULL) {
    *cipher_total += (unsigned long)frame_len;
  }
  OPENSSL_cleanse(nonce, sizeof(nonce));
  return rc;
}

static int lc_pouch_crypto_stream_plain_to_fd(int fd, lc_source *body,
                                              unsigned long *plain_bytes,
                                              unsigned long *cipher_bytes,
                                              unsigned long *stored_crc,
                                              lc_error *error) {
  unsigned char buffer[LC_POUCH_FRAME_PLAINTEXT_BYTES];
  unsigned long total;
  int rc;

  if (fd < 0 || body == NULL || plain_bytes == NULL || cipher_bytes == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch plaintext stream write requires fd, body, and "
                        "byte outputs",
                        NULL, NULL, "pouch");
  }
  total = 0UL;
  rc = LC_OK;
  for (;;) {
    size_t got;

    got = body->read(body, buffer, sizeof(buffer), error);
    if (got == 0U) {
      if (error != NULL && error->code != LC_OK) {
        rc = error->code;
      }
      break;
    }
    rc = lc_pouch_crypto_check_byte_counter(
        total, got, "pouch payload byte count exceeds platform limit", error);
    if (rc != LC_OK) {
      break;
    }
    rc = lc_pouch_crypto_write_all_fd(fd, buffer, got, error);
    if (rc != LC_OK) {
      break;
    }
    if (stored_crc != NULL) {
      *stored_crc = (unsigned long)crc32((uLong)*stored_crc, buffer, (uInt)got);
    }
    total += (unsigned long)got;
  }
  OPENSSL_cleanse(buffer, sizeof(buffer));
  if (rc == LC_OK) {
    *plain_bytes = total;
    *cipher_bytes = total;
  }
  return rc;
}

static size_t lc_pouch_zlib_source_read(lc_source *self, void *buffer,
                                        size_t count, lc_error *error) {
  lc_pouch_zlib_source *source;
  int flush;

  if (self == NULL || buffer == NULL || count == 0U) {
    return 0U;
  }
  source = (lc_pouch_zlib_source *)self->impl;
  if (source == NULL || source->stream_done) {
    return 0U;
  }
  source->stream.next_out = (unsigned char *)buffer;
  source->stream.avail_out = (uInt)count;
  while (source->stream.avail_out > 0U && !source->stream_done) {
    int zrc;

    if (source->stream.avail_in == 0U && !source->input_done) {
      size_t got;

      got = source->inner->read(source->inner, source->input,
                                sizeof(source->input), error);
      if (got == 0U) {
        if (error != NULL && error->code != LC_OK) {
          return count - source->stream.avail_out;
        }
        source->input_done = 1;
      } else {
        if (lc_pouch_crypto_check_byte_counter(
                source->input_total, got,
                "pouch compressed plaintext byte count exceeds platform limit",
                error) != LC_OK) {
          return count - source->stream.avail_out;
        }
        source->input_total += (unsigned long)got;
        source->stream.next_in = source->input;
        source->stream.avail_in = (uInt)got;
      }
    }
    flush = source->input_done ? Z_FINISH : Z_NO_FLUSH;
    if (source->deflate_mode) {
      zrc = deflate(&source->stream, flush);
      if (zrc == Z_STREAM_END) {
        source->stream_done = 1;
        break;
      }
      if (zrc != Z_OK) {
        (void)lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                           "failed to compress pouch payload", NULL, NULL,
                           "pouch");
        return count - source->stream.avail_out;
      }
    } else {
      zrc = inflate(&source->stream, Z_NO_FLUSH);
      if (zrc == Z_STREAM_END) {
        source->stream_done = 1;
        break;
      }
      if (zrc == Z_BUF_ERROR && source->input_done &&
          source->stream.avail_in == 0U) {
        (void)lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                           "compressed pouch payload is truncated", NULL, NULL,
                           "pouch");
        return count - source->stream.avail_out;
      }
      if (zrc != Z_OK && zrc != Z_BUF_ERROR) {
        (void)lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                           "failed to decompress pouch payload", NULL, NULL,
                           "pouch");
        return count - source->stream.avail_out;
      }
      if (zrc == Z_BUF_ERROR && source->stream.avail_out == (uInt)count) {
        break;
      }
    }
  }
  return count - source->stream.avail_out;
}

static int lc_pouch_zlib_source_reset(lc_source *self, lc_error *error) {
  lc_pouch_zlib_source *source;
  int zrc;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch zlib source reset requires source", NULL, NULL,
                        "pouch");
  }
  source = (lc_pouch_zlib_source *)self->impl;
  if (source == NULL || source->inner == NULL || source->inner->reset == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch zlib source is not resettable", NULL, NULL,
                        "pouch");
  }
  if (source->inner->reset(source->inner, error) != LC_OK) {
    return error != NULL && error->code != LC_OK ? error->code
                                                 : LC_ERR_TRANSPORT;
  }
  zrc = source->deflate_mode ? deflateReset(&source->stream)
                             : inflateReset(&source->stream);
  if (zrc != Z_OK) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "failed to reset pouch zlib stream", NULL, NULL,
                        "pouch");
  }
  source->stream.next_in = NULL;
  source->stream.avail_in = 0U;
  source->input_total = 0UL;
  source->input_done = 0;
  source->stream_done = 0;
  return LC_OK;
}

static void lc_pouch_zlib_source_close(lc_source *self) {
  lc_pouch_zlib_source *source;

  if (self == NULL) {
    return;
  }
  source = (lc_pouch_zlib_source *)self->impl;
  if (source == NULL) {
    return;
  }
  if (source->deflate_mode) {
    (void)deflateEnd(&source->stream);
  } else {
    (void)inflateEnd(&source->stream);
  }
  if (source->close_inner && source->inner != NULL &&
      source->inner->close != NULL) {
    source->inner->close(source->inner);
  }
  lc_free_with_allocator(&source->allocator, source);
}

static int lc_pouch_zlib_source_open(const lc_allocator *allocator,
                                     lc_source *inner, int deflate_mode,
                                     int close_inner,
                                     lc_pouch_zlib_source **out,
                                     lc_error *error) {
  lc_pouch_zlib_source *source;
  int zrc;

  if (inner == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch zlib source requires inner source and output",
                        NULL, NULL, "pouch");
  }
  *out = NULL;
  source = (lc_pouch_zlib_source *)lc_calloc_with_allocator(allocator, 1U,
                                                            sizeof(*source));
  if (source == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch zlib source", NULL, NULL,
                        "pouch");
  }
  if (allocator != NULL) {
    source->allocator = *allocator;
  } else {
    lc_allocator_init(&source->allocator);
  }
  source->inner = inner;
  source->deflate_mode = deflate_mode ? 1 : 0;
  source->close_inner = close_inner ? 1 : 0;
  zrc = source->deflate_mode
            ? deflateInit2(&source->stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                           MAX_WBITS, 8, Z_DEFAULT_STRATEGY)
            : inflateInit(&source->stream);
  if (zrc != Z_OK) {
    lc_free_with_allocator(&source->allocator, source);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to initialize pouch zlib stream", NULL, NULL,
                        "pouch");
  }
  source->pub.read = lc_pouch_zlib_source_read;
  source->pub.reset = lc_pouch_zlib_source_reset;
  source->pub.close = lc_pouch_zlib_source_close;
  source->pub.impl = source;
  *out = source;
  return LC_OK;
}

static int lc_pouch_crypto_stream_to_fd_crc_impl(
    lc_pouch_crypto *crypto, const char *context, int fd, lc_source *body,
    unsigned long *plain_bytes, unsigned long *cipher_bytes,
    unsigned long *stored_crc, char **descriptor_out, int allow_compression,
    lc_error *error);

static int lc_pouch_crypto_stream_to_file_impl(
    lc_pouch_crypto *crypto, const char *context, const char *path,
    lc_source *body, unsigned long *plain_bytes, unsigned long *cipher_bytes,
    char **descriptor_out, int sync_file, int allow_compression,
    lc_error *error) {
  int fd;
  int rc;

  if (path == NULL || body == NULL || plain_bytes == NULL ||
      cipher_bytes == NULL || descriptor_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto stream write requires path, body, byte "
                        "outputs, and descriptor output",
                        NULL, NULL, "pouch");
  }
  *descriptor_out = NULL;
  fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0666);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create pouch state payload", strerror(errno),
                        NULL, "pouch");
  }
  rc = lc_pouch_crypto_stream_to_fd_crc_impl(
      crypto, context, fd, body, plain_bytes, cipher_bytes, NULL,
      descriptor_out, allow_compression, error);
  if (rc == LC_OK && sync_file && fsync(fd) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to fsync pouch state payload", strerror(errno),
                      NULL, "pouch");
  }
  if (close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state payload", strerror(errno),
                      NULL, "pouch");
  }
  if (rc != LC_OK) {
    unlink(path);
    if (*descriptor_out != NULL) {
      lc_free_with_allocator(crypto != NULL ? &crypto->allocator : NULL,
                             *descriptor_out);
      *descriptor_out = NULL;
    }
    return rc;
  }
  return LC_OK;
}

int lc_pouch_crypto_stream_to_file(lc_pouch_crypto *crypto, const char *context,
                                   const char *path, lc_source *body,
                                   unsigned long *plain_bytes,
                                   unsigned long *cipher_bytes,
                                   char **descriptor_out, lc_error *error) {
  return lc_pouch_crypto_stream_to_file_impl(crypto, context, path, body,
                                             plain_bytes, cipher_bytes,
                                             descriptor_out, 1, 1, error);
}

int lc_pouch_crypto_stream_to_file_relaxed(
    lc_pouch_crypto *crypto, const char *context, const char *path,
    lc_source *body, unsigned long *plain_bytes, unsigned long *cipher_bytes,
    char **descriptor_out, lc_error *error) {
  return lc_pouch_crypto_stream_to_file_impl(crypto, context, path, body,
                                             plain_bytes, cipher_bytes,
                                             descriptor_out, 0, 1, error);
}

int lc_pouch_crypto_stream_to_file_relaxed_uncompressed(
    lc_pouch_crypto *crypto, const char *context, const char *path,
    lc_source *body, unsigned long *plain_bytes, unsigned long *cipher_bytes,
    char **descriptor_out, lc_error *error) {
  return lc_pouch_crypto_stream_to_file_impl(crypto, context, path, body,
                                             plain_bytes, cipher_bytes,
                                             descriptor_out, 0, 0, error);
}

int lc_pouch_crypto_stream_to_fd(lc_pouch_crypto *crypto, const char *context,
                                 int fd, lc_source *body,
                                 unsigned long *plain_bytes,
                                 unsigned long *cipher_bytes,
                                 char **descriptor_out, lc_error *error) {
  return lc_pouch_crypto_stream_to_fd_crc_impl(crypto, context, fd, body,
                                               plain_bytes, cipher_bytes, NULL,
                                               descriptor_out, 1, error);
}

static int lc_pouch_crypto_stream_to_fd_crc_impl(
    lc_pouch_crypto *crypto, const char *context, int fd, lc_source *body,
    unsigned long *plain_bytes, unsigned long *cipher_bytes,
    unsigned long *stored_crc, char **descriptor_out, int allow_compression,
    lc_error *error) {
  lc_pouch_crypto_desc desc;
  lc_pouch_zlib_source *deflater;
  lc_source *write_body;
  EVP_CIPHER_CTX *ctx;
  unsigned char buffer[LC_POUCH_FRAME_PLAINTEXT_BYTES];
  unsigned long plain_total;
  unsigned long cipher_total;
  unsigned long counter;
  char *descriptor;
  int compressed;
  int encrypted;
  int rc;

  if (fd < 0 || body == NULL || plain_bytes == NULL || cipher_bytes == NULL ||
      descriptor_out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto stream write requires fd, body, byte "
                        "outputs, and descriptor output",
                        NULL, NULL, "pouch");
  }
  *descriptor_out = NULL;
  if (stored_crc != NULL) {
    *stored_crc = (unsigned long)crc32(0L, Z_NULL, 0);
  }
  encrypted = crypto != NULL && crypto->encryption_enabled;
  compressed =
      allow_compression && crypto != NULL && crypto->compression_enabled;
  if (!encrypted && !compressed) {
    return lc_pouch_crypto_stream_plain_to_fd(fd, body, plain_bytes,
                                              cipher_bytes, stored_crc, error);
  }
  if (encrypted && (context == NULL || context[0] == '\0')) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto write requires context", NULL, NULL,
                        "pouch");
  }
  deflater = NULL;
  write_body = body;
  if (compressed) {
    rc = lc_pouch_zlib_source_open(&crypto->allocator, body, 1, 0, &deflater,
                                   error);
    if (rc != LC_OK) {
      return rc;
    }
    write_body = &deflater->pub;
  }
  memset(&desc, 0, sizeof(desc));
  desc.encrypted = encrypted;
  desc.compressed = compressed;
  desc.frame_size = encrypted ? LC_POUCH_FRAME_PLAINTEXT_BYTES : 0UL;
  if (encrypted && !lc_pouch_crypto_random_bytes(desc.nonce_prefix,
                                                 sizeof(desc.nonce_prefix))) {
    if (deflater != NULL) {
      deflater->pub.close(&deflater->pub);
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to generate pouch crypto descriptor material",
                        NULL, NULL, "pouch");
  }
  descriptor = NULL;
  rc = lc_pouch_crypto_descriptor_encode(&crypto->allocator, &desc, &descriptor,
                                         error);
  if (rc != LC_OK) {
    if (deflater != NULL) {
      deflater->pub.close(&deflater->pub);
    }
    return rc;
  }
  if (!encrypted) {
    unsigned long compressed_bytes;

    compressed_bytes = 0UL;
    rc = lc_pouch_crypto_stream_plain_to_fd(fd, write_body, &compressed_bytes,
                                            cipher_bytes, stored_crc, error);
    if (rc == LC_OK) {
      *plain_bytes =
          deflater != NULL ? deflater->input_total : compressed_bytes;
      *descriptor_out = descriptor;
    } else {
      lc_free_with_allocator(&crypto->allocator, descriptor);
    }
    if (deflater != NULL) {
      deflater->pub.close(&deflater->pub);
    }
    return rc;
  }
  ctx = EVP_CIPHER_CTX_new();
  if (ctx == NULL) {
    lc_free_with_allocator(&crypto->allocator, descriptor);
    if (deflater != NULL) {
      deflater->pub.close(&deflater->pub);
    }
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto cipher context", NULL,
                        NULL, "pouch");
  }
  plain_total = 0UL;
  cipher_total = 0UL;
  counter = 0UL;
  rc = LC_OK;
  for (;;) {
    size_t got;
    size_t target;

    target =
        counter == 0UL ? LC_POUCH_FIRST_FRAME_PLAINTEXT_BYTES : desc.frame_size;
    got = write_body->read(write_body, buffer, target, error);
    if (got == 0U) {
      if (error != NULL && error->code != LC_OK) {
        rc = error->code;
      }
      break;
    }
    rc = lc_pouch_crypto_check_byte_counter(
        plain_total, got,
        "pouch encrypted plaintext byte count exceeds platform limit", error);
    if (rc != LC_OK) {
      break;
    }
    rc = lc_pouch_crypto_encrypt_frame(
        fd, ctx, crypto->data_key, desc.nonce_prefix, context, strlen(context),
        counter, buffer, got, &cipher_total, stored_crc, error);
    if (rc != LC_OK) {
      break;
    }
    plain_total += (unsigned long)got;
    ++counter;
    if (counter == 0xFFFFFFFFUL) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch encrypted payload exceeds frame counter limit",
                        NULL, NULL, "pouch");
      break;
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_crypto_encrypt_frame(
        fd, ctx, crypto->data_key, desc.nonce_prefix, context, strlen(context),
        counter, buffer, 0U, &cipher_total, stored_crc, error);
  }
  EVP_CIPHER_CTX_free(ctx);
  OPENSSL_cleanse(buffer, sizeof(buffer));
  if (rc != LC_OK) {
    lc_free_with_allocator(&crypto->allocator, descriptor);
    if (deflater != NULL) {
      deflater->pub.close(&deflater->pub);
    }
    return rc;
  }
  *plain_bytes = deflater != NULL ? deflater->input_total : plain_total;
  *cipher_bytes = cipher_total;
  *descriptor_out = descriptor;
  if (deflater != NULL) {
    deflater->pub.close(&deflater->pub);
  }
  return LC_OK;
}

int lc_pouch_crypto_stream_to_fd_crc(
    lc_pouch_crypto *crypto, const char *context, int fd, lc_source *body,
    unsigned long *plain_bytes, unsigned long *cipher_bytes,
    unsigned long *stored_crc, char **descriptor_out, lc_error *error) {
  return lc_pouch_crypto_stream_to_fd_crc_impl(
      crypto, context, fd, body, plain_bytes, cipher_bytes, stored_crc,
      descriptor_out, 1, error);
}

static size_t lc_pouch_crypto_source_read(lc_source *self, void *buffer,
                                          size_t count, lc_error *error);
static int lc_pouch_crypto_source_reset(lc_source *self, lc_error *error);
static void lc_pouch_crypto_source_close(lc_source *self);
static size_t lc_pouch_plain_span_source_read(lc_source *self, void *buffer,
                                              size_t count, lc_error *error);
static int lc_pouch_plain_span_source_reset(lc_source *self, lc_error *error);
static void lc_pouch_plain_span_source_close(lc_source *self);

int lc_pouch_crypto_source_from_file(lc_pouch_crypto *crypto,
                                     const char *context, const char *path,
                                     const char *descriptor, lc_source **out,
                                     lc_error *error) {
  lc_pouch_crypto_source *source;
  lc_pouch_zlib_source *inflater;
  lc_source *base;
  lc_pouch_crypto_desc desc;
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto source requires out", NULL, NULL,
                        "pouch");
  }
  *out = NULL;
  if (descriptor == NULL || descriptor[0] == '\0') {
    if (crypto != NULL &&
        (crypto->encryption_enabled || crypto->compression_enabled)) {
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "transformed pouch payload is missing descriptor",
                          NULL, NULL, "pouch");
    }
    return lc_source_from_file(path, out, error);
  }
  rc = lc_pouch_crypto_descriptor_decode(descriptor, &desc, error);
  if (rc != LC_OK) {
    return rc;
  }
  if (!desc.encrypted) {
    rc = lc_source_from_file(path, &base, error);
    if (rc != LC_OK) {
      return rc;
    }
    if (!desc.compressed) {
      *out = base;
      return LC_OK;
    }
    inflater = NULL;
    rc = lc_pouch_zlib_source_open(crypto != NULL ? &crypto->allocator : NULL,
                                   base, 0, 1, &inflater, error);
    if (rc != LC_OK) {
      if (base->close != NULL) {
        base->close(base);
      }
      return rc;
    }
    *out = &inflater->pub;
    return LC_OK;
  }
  if (crypto == NULL || !crypto->encryption_enabled) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "encrypted pouch payload requires crypto key", NULL,
                        NULL, "pouch");
  }
  source = (lc_pouch_crypto_source *)lc_calloc_with_allocator(
      &crypto->allocator, 1U, sizeof(*source));
  if (source == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto source", NULL, NULL,
                        "pouch");
  }
  source->allocator = crypto->allocator;
  source->ctx = EVP_CIPHER_CTX_new();
  if (source->ctx == NULL) {
    lc_free_with_allocator(&crypto->allocator, source);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch decrypt context", NULL, NULL,
                        "pouch");
  }
  source->fp = fopen(path, "rb");
  if (source->fp == NULL) {
    EVP_CIPHER_CTX_free(source->ctx);
    lc_free_with_allocator(&crypto->allocator, source);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open encrypted pouch payload",
                        strerror(errno), NULL, "pouch");
  }
  if (context == NULL || context[0] == '\0') {
    fclose(source->fp);
    EVP_CIPHER_CTX_free(source->ctx);
    lc_free_with_allocator(&crypto->allocator, source);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto read requires context", NULL, NULL,
                        "pouch");
  }
  source->context = lc_pouch_crypto_strdup(&crypto->allocator, context);
  if (source->context == NULL) {
    fclose(source->fp);
    EVP_CIPHER_CTX_free(source->ctx);
    lc_free_with_allocator(&crypto->allocator, source);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto read context", NULL,
                        NULL, "pouch");
  }
  source->context_len = strlen(source->context);
  memcpy(source->key, crypto->data_key, sizeof(source->key));
  memcpy(source->nonce_prefix, desc.nonce_prefix, sizeof(source->nonce_prefix));
  source->frame_size = desc.frame_size;
  source->pub.read = lc_pouch_crypto_source_read;
  source->pub.reset = lc_pouch_crypto_source_reset;
  source->pub.close = lc_pouch_crypto_source_close;
  source->pub.impl = source;
  if (desc.compressed) {
    inflater = NULL;
    rc = lc_pouch_zlib_source_open(&crypto->allocator, &source->pub, 0, 1,
                                   &inflater, error);
    if (rc != LC_OK) {
      source->pub.close(&source->pub);
      return rc;
    }
    *out = &inflater->pub;
  } else {
    *out = &source->pub;
  }
  return LC_OK;
}

int lc_pouch_crypto_source_from_file_span(lc_pouch_crypto *crypto,
                                          const char *context, const char *path,
                                          unsigned long offset,
                                          unsigned long length,
                                          const char *descriptor,
                                          lc_source **out, lc_error *error) {
  int fd;

  if (path == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto span source requires path", NULL, NULL,
                        "pouch");
  }
  fd = open(path, O_RDONLY);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch payload span", strerror(errno),
                        NULL, "pouch");
  }
  return lc_pouch_crypto_source_from_fd_span(crypto, context, fd, offset,
                                             length, descriptor, out, error);
}

int lc_pouch_crypto_source_from_fd_span(lc_pouch_crypto *crypto,
                                        const char *context, int fd,
                                        unsigned long offset,
                                        unsigned long length,
                                        const char *descriptor, lc_source **out,
                                        lc_error *error) {
  lc_pouch_crypto_source *source;
  lc_pouch_plain_span_source *plain;
  lc_pouch_zlib_source *inflater;
  lc_source *base;
  lc_pouch_crypto_desc desc;
  int rc;

  if (fd < 0 || out == NULL) {
    if (fd >= 0) {
      close(fd);
    }
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto span source requires fd and output", NULL,
                        NULL, "pouch");
  }
  *out = NULL;
  if (descriptor == NULL || descriptor[0] == '\0') {
    if (crypto != NULL &&
        (crypto->encryption_enabled || crypto->compression_enabled)) {
      close(fd);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "transformed pouch payload is missing descriptor",
                          NULL, NULL, "pouch");
    }
    plain = (lc_pouch_plain_span_source *)lc_calloc_with_allocator(
        NULL, 1U, sizeof(*plain));
    if (plain == NULL) {
      close(fd);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch payload span source", NULL,
                          NULL, "pouch");
    }
    lc_allocator_init(&plain->allocator);
    plain->fp = fdopen(fd, "rb");
    if (plain->fp == NULL) {
      close(fd);
      lc_free_with_allocator(&plain->allocator, plain);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to open pouch payload span descriptor",
                          strerror(errno), NULL, "pouch");
    }
    if (fseek(plain->fp, (long)offset, SEEK_SET) != 0) {
      fclose(plain->fp);
      lc_free_with_allocator(&plain->allocator, plain);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to seek pouch payload span", strerror(errno),
                          NULL, "pouch");
    }
    plain->offset = offset;
    plain->length = length;
    plain->pub.read = lc_pouch_plain_span_source_read;
    plain->pub.reset = lc_pouch_plain_span_source_reset;
    plain->pub.close = lc_pouch_plain_span_source_close;
    plain->pub.impl = plain;
    *out = &plain->pub;
    return LC_OK;
  }
  rc = lc_pouch_crypto_descriptor_decode(descriptor, &desc, error);
  if (rc != LC_OK) {
    close(fd);
    return rc;
  }
  if (!desc.encrypted) {
    plain = (lc_pouch_plain_span_source *)lc_calloc_with_allocator(
        crypto != NULL ? &crypto->allocator : NULL, 1U, sizeof(*plain));
    if (plain == NULL) {
      close(fd);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch payload span source", NULL,
                          NULL, "pouch");
    }
    if (crypto != NULL) {
      plain->allocator = crypto->allocator;
    } else {
      lc_allocator_init(&plain->allocator);
    }
    plain->fp = fdopen(fd, "rb");
    if (plain->fp == NULL) {
      close(fd);
      lc_free_with_allocator(&plain->allocator, plain);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to open pouch payload span descriptor",
                          strerror(errno), NULL, "pouch");
    }
    if (fseek(plain->fp, (long)offset, SEEK_SET) != 0) {
      fclose(plain->fp);
      lc_free_with_allocator(&plain->allocator, plain);
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to seek pouch payload span", strerror(errno),
                          NULL, "pouch");
    }
    plain->offset = offset;
    plain->length = length;
    plain->pub.read = lc_pouch_plain_span_source_read;
    plain->pub.reset = lc_pouch_plain_span_source_reset;
    plain->pub.close = lc_pouch_plain_span_source_close;
    plain->pub.impl = plain;
    base = &plain->pub;
    if (!desc.compressed) {
      *out = base;
      return LC_OK;
    }
    inflater = NULL;
    rc = lc_pouch_zlib_source_open(crypto != NULL ? &crypto->allocator : NULL,
                                   base, 0, 1, &inflater, error);
    if (rc != LC_OK) {
      base->close(base);
      return rc;
    }
    *out = &inflater->pub;
    return LC_OK;
  }
  if (crypto == NULL || !crypto->encryption_enabled) {
    close(fd);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "encrypted pouch payload requires crypto key", NULL,
                        NULL, "pouch");
  }
  source = (lc_pouch_crypto_source *)lc_calloc_with_allocator(
      &crypto->allocator, 1U, sizeof(*source));
  if (source == NULL) {
    close(fd);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto span source", NULL,
                        NULL, "pouch");
  }
  source->allocator = crypto->allocator;
  source->ctx = EVP_CIPHER_CTX_new();
  if (source->ctx == NULL) {
    close(fd);
    lc_free_with_allocator(&crypto->allocator, source);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch decrypt context", NULL, NULL,
                        "pouch");
  }
  source->fp = fdopen(fd, "rb");
  if (source->fp == NULL) {
    close(fd);
    EVP_CIPHER_CTX_free(source->ctx);
    lc_free_with_allocator(&crypto->allocator, source);
    return lc_error_set(
        error, LC_ERR_TRANSPORT, 0L,
        "failed to open encrypted pouch payload span descriptor",
        strerror(errno), NULL, "pouch");
  }
  if (fseek(source->fp, (long)offset, SEEK_SET) != 0) {
    fclose(source->fp);
    EVP_CIPHER_CTX_free(source->ctx);
    lc_free_with_allocator(&crypto->allocator, source);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to seek encrypted pouch payload span",
                        strerror(errno), NULL, "pouch");
  }
  if (context == NULL || context[0] == '\0') {
    fclose(source->fp);
    EVP_CIPHER_CTX_free(source->ctx);
    lc_free_with_allocator(&crypto->allocator, source);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto read requires context", NULL, NULL,
                        "pouch");
  }
  source->context = lc_pouch_crypto_strdup(&crypto->allocator, context);
  if (source->context == NULL) {
    fclose(source->fp);
    EVP_CIPHER_CTX_free(source->ctx);
    lc_free_with_allocator(&crypto->allocator, source);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto read context", NULL,
                        NULL, "pouch");
  }
  source->context_len = strlen(source->context);
  memcpy(source->key, crypto->data_key, sizeof(source->key));
  memcpy(source->nonce_prefix, desc.nonce_prefix, sizeof(source->nonce_prefix));
  source->frame_size = desc.frame_size;
  source->span_offset = offset;
  source->span_length = length;
  source->span_remaining = length;
  source->bounded = 1;
  source->pub.read = lc_pouch_crypto_source_read;
  source->pub.reset = lc_pouch_crypto_source_reset;
  source->pub.close = lc_pouch_crypto_source_close;
  source->pub.impl = source;
  if (desc.compressed) {
    inflater = NULL;
    rc = lc_pouch_zlib_source_open(&crypto->allocator, &source->pub, 0, 1,
                                   &inflater, error);
    if (rc != LC_OK) {
      source->pub.close(&source->pub);
      return rc;
    }
    *out = &inflater->pub;
  } else {
    *out = &source->pub;
  }
  return LC_OK;
}

static int lc_pouch_crypto_read_frame(lc_pouch_crypto_source *source,
                                      lc_error *error) {
  unsigned char header[8];
  unsigned char nonce[LC_POUCH_NONCE_BYTES];
  unsigned char *tag;
  unsigned long counter;
  unsigned long plain_len;
  int out_len;
  int final_len;
  int aad_len;
  size_t got;
  int ok;

  if (source->bounded && source->span_remaining < sizeof(header)) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "encrypted pouch payload span is truncated", NULL, NULL,
                        "pouch");
  }
  got = fread(header, 1U, sizeof(header), source->fp);
  if (got != sizeof(header)) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "encrypted pouch payload is truncated", NULL, NULL,
                        "pouch");
  }
  counter = lc_pouch_crypto_get32(header);
  plain_len = lc_pouch_crypto_get32(header + 4U);
  if (counter != source->counter ||
      plain_len > LC_POUCH_FRAME_PLAINTEXT_BYTES) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "encrypted pouch payload frame header is invalid", NULL,
                        NULL, "pouch");
  }
  if (source->bounded) {
    unsigned long frame_len;

    frame_len = 8UL + plain_len + LC_POUCH_GCM_TAG_BYTES;
    if (source->span_remaining < frame_len) {
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "encrypted pouch payload span frame is truncated",
                          NULL, NULL, "pouch");
    }
    source->span_remaining -= frame_len;
  }
  if (plain_len > 0UL) {
    got = fread(source->cipher, 1U, (size_t)plain_len, source->fp);
    if (got != (size_t)plain_len) {
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "encrypted pouch payload frame is truncated", NULL,
                          NULL, "pouch");
    }
  }
  tag = source->cipher + plain_len;
  got = fread(tag, 1U, LC_POUCH_GCM_TAG_BYTES, source->fp);
  if (got != LC_POUCH_GCM_TAG_BYTES) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "encrypted pouch payload frame tag is truncated", NULL,
                        NULL, "pouch");
  }
  lc_pouch_crypto_nonce(source->nonce_prefix, counter, nonce);
  ok = EVP_CIPHER_CTX_reset(source->ctx) == 1 &&
       EVP_DecryptInit_ex(source->ctx, EVP_aes_256_gcm(), NULL, NULL, NULL) ==
           1 &&
       EVP_CIPHER_CTX_ctrl(source->ctx, EVP_CTRL_GCM_SET_IVLEN, sizeof(nonce),
                           NULL) == 1 &&
       EVP_DecryptInit_ex(source->ctx, NULL, NULL, source->key, nonce) == 1 &&
       EVP_DecryptUpdate(source->ctx, NULL, &aad_len, header, sizeof(header)) ==
           1 &&
       (source->context_len > 0U &&
                EVP_DecryptUpdate(source->ctx, NULL, &aad_len,
                                  (const unsigned char *)source->context,
                                  (int)source->context_len) != 1
            ? 0
            : 1) != 0 &&
       EVP_DecryptUpdate(source->ctx, source->plain, &out_len, source->cipher,
                         (int)plain_len) == 1 &&
       EVP_CIPHER_CTX_ctrl(source->ctx, EVP_CTRL_GCM_SET_TAG,
                           LC_POUCH_GCM_TAG_BYTES, tag) == 1 &&
       EVP_DecryptFinal_ex(source->ctx, source->plain + out_len, &final_len) ==
           1;
  OPENSSL_cleanse(nonce, sizeof(nonce));
  if (!ok || (unsigned long)(out_len + final_len) != plain_len) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "encrypted pouch payload authentication failed", NULL,
                        NULL, "pouch");
  }
  ++source->counter;
  source->plain_offset = 0U;
  source->plain_length = (size_t)plain_len;
  if (plain_len == 0UL) {
    if (source->bounded) {
      if (source->span_remaining != 0UL) {
        return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                            "encrypted pouch payload span has trailing bytes",
                            NULL, NULL, "pouch");
      }
    } else {
      int extra;

      extra = fgetc(source->fp);
      if (extra != EOF) {
        return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                            "encrypted pouch payload has trailing bytes", NULL,
                            NULL, "pouch");
      }
    }
    source->done = 1;
  }
  return LC_OK;
}

static size_t lc_pouch_crypto_source_read(lc_source *self, void *buffer,
                                          size_t count, lc_error *error) {
  lc_pouch_crypto_source *source;
  size_t copied;

  if (self == NULL || buffer == NULL || count == 0U) {
    return 0U;
  }
  source = (lc_pouch_crypto_source *)self->impl;
  copied = 0U;
  while (copied < count) {
    size_t available;
    size_t take;

    available = source->plain_length - source->plain_offset;
    if (available == 0U) {
      int rc;

      if (source->done) {
        break;
      }
      rc = lc_pouch_crypto_read_frame(source, error);
      if (rc != LC_OK) {
        return copied;
      }
      available = source->plain_length - source->plain_offset;
      if (available == 0U && source->done) {
        break;
      }
    }
    take = count - copied;
    if (take > available) {
      take = available;
    }
    memcpy((unsigned char *)buffer + copied,
           source->plain + source->plain_offset, take);
    source->plain_offset += take;
    copied += take;
  }
  return copied;
}

static int lc_pouch_crypto_source_reset(lc_source *self, lc_error *error) {
  lc_pouch_crypto_source *source;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto source reset requires source", NULL, NULL,
                        "pouch");
  }
  source = (lc_pouch_crypto_source *)self->impl;
  if (source->bounded) {
    if (fseek(source->fp, (long)source->span_offset, SEEK_SET) != 0) {
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "failed to reset encrypted pouch payload span",
                          strerror(errno), NULL, "pouch");
    }
    source->span_remaining = source->span_length;
    source->plain_offset = 0U;
    source->plain_length = 0U;
    source->counter = 0UL;
    source->done = 0;
    return LC_OK;
  }
  if (fseek(source->fp, 0L, SEEK_SET) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to reset encrypted pouch payload",
                        strerror(errno), NULL, "pouch");
  }
  source->plain_offset = 0U;
  source->plain_length = 0U;
  source->counter = 0UL;
  source->done = 0;
  return LC_OK;
}

static void lc_pouch_crypto_source_close(lc_source *self) {
  lc_pouch_crypto_source *source;

  if (self == NULL) {
    return;
  }
  source = (lc_pouch_crypto_source *)self->impl;
  if (source->fp != NULL) {
    fclose(source->fp);
  }
  EVP_CIPHER_CTX_free(source->ctx);
  OPENSSL_cleanse(source->key, sizeof(source->key));
  OPENSSL_cleanse(source->plain, sizeof(source->plain));
  lc_free_with_allocator(&source->allocator, source->context);
  lc_free_with_allocator(&source->allocator, source);
}

static size_t lc_pouch_plain_span_source_read(lc_source *self, void *buffer,
                                              size_t count, lc_error *error) {
  lc_pouch_plain_span_source *source;
  unsigned long remaining;
  size_t want;
  size_t got;

  if (self == NULL || buffer == NULL || count == 0U) {
    return 0U;
  }
  source = (lc_pouch_plain_span_source *)self->impl;
  if (source == NULL || source->read_bytes >= source->length) {
    return 0U;
  }
  remaining = source->length - source->read_bytes;
  want = (unsigned long)count < remaining ? count : (size_t)remaining;
  got = fread(buffer, 1U, want, source->fp);
  if (got == 0U && ferror(source->fp) && error != NULL) {
    (void)lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                       "failed to read pouch payload span", strerror(errno),
                       NULL, "pouch");
  }
  source->read_bytes += (unsigned long)got;
  return got;
}

static int lc_pouch_plain_span_source_reset(lc_source *self, lc_error *error) {
  lc_pouch_plain_span_source *source;

  if (self == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch payload span reset requires source", NULL, NULL,
                        "pouch");
  }
  source = (lc_pouch_plain_span_source *)self->impl;
  if (source == NULL || source->fp == NULL ||
      fseek(source->fp, (long)source->offset, SEEK_SET) != 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to reset pouch payload span", strerror(errno),
                        NULL, "pouch");
  }
  source->read_bytes = 0UL;
  return LC_OK;
}

static void lc_pouch_plain_span_source_close(lc_source *self) {
  lc_pouch_plain_span_source *source;

  if (self == NULL) {
    return;
  }
  source = (lc_pouch_plain_span_source *)self->impl;
  if (source == NULL) {
    return;
  }
  if (source->fp != NULL) {
    fclose(source->fp);
  }
  lc_free_with_allocator(&source->allocator, source);
}

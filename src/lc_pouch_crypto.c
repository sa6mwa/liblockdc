#include "lc_pouch_crypto.h"

#include "lc_api_internal.h"

#include <errno.h>
#include <fcntl.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define LC_POUCH_KEY_PREFIX "lc-pouch-key-v1:"
#define LC_POUCH_DESC_PREFIX "lc-pouch-desc-v1:"
#define LC_POUCH_ROOT_KEY_BYTES 32U
#define LC_POUCH_SALT_BYTES 16U
#define LC_POUCH_NONCE_PREFIX_BYTES 8U
#define LC_POUCH_NONCE_BYTES 12U
#define LC_POUCH_DEK_BYTES 32U
#define LC_POUCH_GCM_TAG_BYTES 16U
#define LC_POUCH_FRAME_PLAINTEXT_BYTES (64U * 1024U)
#define LC_POUCH_DESC_RAW_BYTES                                                \
  (4U + LC_POUCH_SALT_BYTES + LC_POUCH_NONCE_PREFIX_BYTES)

struct lc_pouch_crypto {
  lc_allocator allocator;
  unsigned char root_key[LC_POUCH_ROOT_KEY_BYTES];
};

typedef struct lc_pouch_crypto_desc {
  unsigned long frame_size;
  unsigned char salt[LC_POUCH_SALT_BYTES];
  unsigned char nonce_prefix[LC_POUCH_NONCE_PREFIX_BYTES];
} lc_pouch_crypto_desc;

typedef struct lc_pouch_crypto_source {
  lc_source pub;
  FILE *fp;
  EVP_CIPHER_CTX *ctx;
  unsigned char key[LC_POUCH_DEK_BYTES];
  unsigned char nonce_prefix[LC_POUCH_NONCE_PREFIX_BYTES];
  unsigned char plain[LC_POUCH_FRAME_PLAINTEXT_BYTES];
  unsigned char cipher[LC_POUCH_FRAME_PLAINTEXT_BYTES + LC_POUCH_GCM_TAG_BYTES];
  size_t plain_offset;
  size_t plain_length;
  unsigned long frame_size;
  unsigned long counter;
  int done;
} lc_pouch_crypto_source;

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

static char *lc_pouch_crypto_strdup(const char *text) {
  char *copy;
  size_t len;

  if (text == NULL) {
    return NULL;
  }
  len = strlen(text) + 1U;
  copy = (char *)malloc(len);
  if (copy == NULL) {
    return NULL;
  }
  memcpy(copy, text, len);
  return copy;
}

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

static int lc_pouch_crypto_read_all_file(const char *path, char **out,
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
  buffer = (char *)malloc((size_t)size + 1U);
  if (buffer == NULL) {
    fclose(fp);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto key buffer", NULL,
                        NULL, "pouch");
  }
  got = fread(buffer, 1U, (size_t)size, fp);
  if (got != (size_t)size) {
    free(buffer);
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

static int lc_pouch_crypto_mkdirs(const char *path, lc_error *error) {
  char *copy;
  char *cursor;

  if (path == NULL || path[0] == '\0') {
    return LC_OK;
  }
  copy = lc_pouch_crypto_strdup(path);
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
        free(copy);
        return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                            "failed to create pouch crypto config directory",
                            strerror(errno), NULL, "pouch");
      }
      *cursor = '/';
    }
  }
  if (mkdir(copy, 0700) != 0 && errno != EEXIST) {
    free(copy);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create pouch crypto config directory",
                        strerror(errno), NULL, "pouch");
  }
  free(copy);
  return LC_OK;
}

static char *lc_pouch_crypto_dirname(const char *path) {
  const char *slash;
  char *out;
  size_t len;

  if (path == NULL) {
    return NULL;
  }
  slash = strrchr(path, '/');
  if (slash == NULL) {
    return lc_pouch_crypto_strdup(".");
  }
  if (slash == path) {
    return lc_pouch_crypto_strdup("/");
  }
  len = (size_t)(slash - path);
  out = (char *)malloc(len + 1U);
  if (out == NULL) {
    return NULL;
  }
  memcpy(out, path, len);
  out[len] = '\0';
  return out;
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
  out = (char *)malloc(out_len + 1U);
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
  bytes = (unsigned char *)malloc(max_len);
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
      free(bytes);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch base64url value is truncated", NULL, NULL,
                          "pouch");
    }
    vals[2] = 0;
    vals[3] = 0;
    for (i = 0U; i < take; ++i) {
      vals[i] = lc_pouch_b64url_index(text[src + i]);
      if (vals[i] < 0) {
        free(bytes);
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
  memset(key, 0, sizeof(key));
  if (encoded == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to encode pouch crypto root key", NULL, NULL,
                        "pouch");
  }
  prefix_len = strlen(LC_POUCH_KEY_PREFIX);
  encoded_len = strlen(encoded);
  result = (char *)malloc(prefix_len + encoded_len + 1U);
  if (result == NULL) {
    free(encoded);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto root key string", NULL,
                        NULL, "pouch");
  }
  memcpy(result, LC_POUCH_KEY_PREFIX, prefix_len);
  memcpy(result + prefix_len, encoded, encoded_len + 1U);
  free(encoded);
  *out = result;
  return LC_OK;
}

void lc_pouch_crypto_key_string_free(char *key_string) { free(key_string); }

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
    path = (char *)malloc(len);
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
  path = (char *)malloc(len);
  if (path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto key path", NULL, NULL,
                        "pouch");
  }
  snprintf(path, len, "%s%s", home, suffix);
  *out = path;
  return LC_OK;
}

int lc_pouch_crypto_generate_key_file(const char *path, int overwrite,
                                      char **key_string_out, lc_error *error) {
  char *key;
  char *dir;
  int fd;
  int flags;
  size_t len;
  int rc;

  if (path == NULL || path[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_crypto_generate_key_file requires path", NULL,
                        NULL, "pouch");
  }
  if (key_string_out != NULL) {
    *key_string_out = NULL;
  }
  key = NULL;
  rc = lc_pouch_crypto_generate_key_string(&key, error);
  if (rc != LC_OK) {
    return rc;
  }
  dir = lc_pouch_crypto_dirname(path);
  if (dir == NULL) {
    lc_pouch_crypto_key_string_free(key);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto key directory", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_crypto_mkdirs(dir, error);
  free(dir);
  if (rc != LC_OK) {
    lc_pouch_crypto_key_string_free(key);
    return rc;
  }
  flags = O_CREAT | O_WRONLY | O_TRUNC;
  if (!overwrite) {
    flags |= O_EXCL;
  }
  fd = open(path, flags, 0600);
  if (fd < 0) {
    lc_pouch_crypto_key_string_free(key);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create pouch crypto key file",
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
  if (rc != LC_OK) {
    unlink(path);
    lc_pouch_crypto_key_string_free(key);
    return rc;
  }
  if (key_string_out != NULL) {
    *key_string_out = key;
  } else {
    lc_pouch_crypto_key_string_free(key);
  }
  return LC_OK;
}

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
    free(decoded);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto root key must be 32 bytes", NULL, NULL,
                        "pouch");
  }
  memcpy(root_key, decoded, LC_POUCH_ROOT_KEY_BYTES);
  memset(decoded, 0, decoded_len);
  free(decoded);
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
       !options->generate_key_file)) {
    return LC_OK;
  }
  loaded_key = NULL;
  resolved_key_file = NULL;
  key_string = options->key_string;
  if (key_string == NULL || key_string[0] == '\0') {
    if (options->key_file != NULL && options->key_file[0] != '\0') {
      resolved_key_file = lc_pouch_crypto_strdup(options->key_file);
      if (resolved_key_file == NULL) {
        return lc_error_set(error, LC_ERR_NOMEM, 0L,
                            "failed to allocate pouch crypto key file path",
                            NULL, NULL, "pouch");
      }
    } else {
      rc = lc_pouch_crypto_default_key_file(&resolved_key_file, error);
      if (rc != LC_OK) {
        return rc;
      }
    }
    rc = lc_pouch_crypto_read_all_file(resolved_key_file, &loaded_key, error);
    if (rc != LC_OK && options->generate_key_file) {
      lc_error_cleanup(error);
      rc = lc_pouch_crypto_generate_key_file(resolved_key_file, 0, &loaded_key,
                                             error);
    }
    if (rc != LC_OK) {
      free(resolved_key_file);
      return rc;
    }
    key_string = loaded_key;
  }
  crypto = (lc_pouch_crypto *)lc_calloc_with_allocator(allocator, 1U,
                                                       sizeof(*crypto));
  if (crypto == NULL) {
    free(loaded_key);
    free(resolved_key_file);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto provider", NULL, NULL,
                        "pouch");
  }
  if (allocator != NULL) {
    crypto->allocator = *allocator;
  } else {
    lc_allocator_init(&crypto->allocator);
  }
  rc = lc_pouch_crypto_parse_key(key_string, crypto->root_key, error);
  free(loaded_key);
  if (rc != LC_OK) {
    lc_pouch_crypto_close(crypto);
    free(resolved_key_file);
    return rc;
  }
  *out = crypto;
  if (key_file_out != NULL) {
    *key_file_out = resolved_key_file;
  } else {
    free(resolved_key_file);
  }
  return LC_OK;
}

void lc_pouch_crypto_close(lc_pouch_crypto *crypto) {
  lc_allocator allocator;

  if (crypto == NULL) {
    return;
  }
  allocator = crypto->allocator;
  memset(crypto->root_key, 0, sizeof(crypto->root_key));
  lc_free_with_allocator(&allocator, crypto);
}

int lc_pouch_crypto_enabled(const lc_pouch_crypto *crypto) {
  return crypto != NULL;
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

static int lc_pouch_crypto_derive(const lc_pouch_crypto *crypto,
                                  const lc_pouch_crypto_desc *desc,
                                  const char *context,
                                  unsigned char key[LC_POUCH_DEK_BYTES],
                                  lc_error *error) {
  unsigned char prk[EVP_MAX_MD_SIZE];
  unsigned char info[512];
  const char label[] = "lc-pouch-aes-256-gcm:";
  size_t context_len;
  size_t prk_len;
  size_t out_len;
  size_t label_len;
  int ok;

  if (crypto == NULL || desc == NULL || context == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto derive requires crypto, descriptor, and "
                        "context",
                        NULL, NULL, "pouch");
  }
  prk_len = 0U;
  if (EVP_Q_mac(NULL, "HMAC", NULL, "SHA256", NULL, desc->salt,
                sizeof(desc->salt), crypto->root_key, sizeof(crypto->root_key),
                prk, sizeof(prk), &prk_len) == NULL ||
      prk_len == 0U) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to derive pouch crypto PRK", NULL, NULL,
                        "pouch");
  }
  context_len = strlen(context);
  label_len = sizeof(label) - 1U;
  if (context_len > sizeof(info) - label_len - 1U) {
    memset(prk, 0, sizeof(prk));
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto context is too long", NULL, NULL,
                        "pouch");
  }
  memcpy(info, label, label_len);
  memcpy(info + label_len, context, context_len);
  info[label_len + context_len] = 1U;
  out_len = 0U;
  ok = EVP_Q_mac(NULL, "HMAC", NULL, "SHA256", NULL, prk, prk_len, info,
                 label_len + context_len + 1U, key, LC_POUCH_DEK_BYTES,
                 &out_len) != NULL &&
       out_len == LC_POUCH_DEK_BYTES;
  memset(prk, 0, sizeof(prk));
  memset(info, 0, sizeof(info));
  if (!ok) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to derive pouch crypto DEK", NULL, NULL,
                        "pouch");
  }
  return LC_OK;
}

static int lc_pouch_crypto_descriptor_encode(const lc_pouch_crypto_desc *desc,
                                             char **out, lc_error *error) {
  unsigned char raw[LC_POUCH_DESC_RAW_BYTES];
  char *encoded;
  char *result;
  size_t prefix_len;
  size_t encoded_len;

  lc_pouch_crypto_put32(raw, desc->frame_size);
  memcpy(raw + 4U, desc->salt, LC_POUCH_SALT_BYTES);
  memcpy(raw + 4U + LC_POUCH_SALT_BYTES, desc->nonce_prefix,
         LC_POUCH_NONCE_PREFIX_BYTES);
  encoded = lc_pouch_b64url_encode(raw, sizeof(raw));
  memset(raw, 0, sizeof(raw));
  if (encoded == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to encode pouch crypto descriptor", NULL, NULL,
                        "pouch");
  }
  prefix_len = strlen(LC_POUCH_DESC_PREFIX);
  encoded_len = strlen(encoded);
  result = (char *)malloc(prefix_len + encoded_len + 1U);
  if (result == NULL) {
    free(encoded);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto descriptor", NULL,
                        NULL, "pouch");
  }
  memcpy(result, LC_POUCH_DESC_PREFIX, prefix_len);
  memcpy(result + prefix_len, encoded, encoded_len + 1U);
  free(encoded);
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

  if (descriptor == NULL || strncmp(descriptor, LC_POUCH_DESC_PREFIX,
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
    free(raw);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto descriptor has invalid length", NULL,
                        NULL, "pouch");
  }
  memset(out, 0, sizeof(*out));
  out->frame_size = lc_pouch_crypto_get32(raw);
  if (out->frame_size == 0UL ||
      out->frame_size > LC_POUCH_FRAME_PLAINTEXT_BYTES) {
    free(raw);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto descriptor has invalid frame size", NULL,
                        NULL, "pouch");
  }
  memcpy(out->salt, raw + 4U, LC_POUCH_SALT_BYTES);
  memcpy(out->nonce_prefix, raw + 4U + LC_POUCH_SALT_BYTES,
         LC_POUCH_NONCE_PREFIX_BYTES);
  memset(raw, 0, raw_len);
  free(raw);
  return LC_OK;
}

static void lc_pouch_crypto_nonce(const unsigned char prefix[8],
                                  unsigned long counter,
                                  unsigned char nonce[12]) {
  memcpy(nonce, prefix, LC_POUCH_NONCE_PREFIX_BYTES);
  lc_pouch_crypto_put32(nonce + LC_POUCH_NONCE_PREFIX_BYTES, counter);
}

static int lc_pouch_crypto_encrypt_frame(
    int fd, EVP_CIPHER_CTX *ctx, const unsigned char key[LC_POUCH_DEK_BYTES],
    const unsigned char nonce_prefix[LC_POUCH_NONCE_PREFIX_BYTES],
    unsigned long counter, const unsigned char *plain, size_t plain_len,
    unsigned long *cipher_total, lc_error *error) {
  unsigned char
      frame[8U + LC_POUCH_FRAME_PLAINTEXT_BYTES + LC_POUCH_GCM_TAG_BYTES];
  unsigned char *header;
  unsigned char *cipher;
  unsigned char *tag;
  unsigned char nonce[LC_POUCH_NONCE_BYTES];
  size_t cipher_len;
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
  rc = lc_pouch_crypto_write_all_fd(
      fd, frame, 8U + cipher_len + LC_POUCH_GCM_TAG_BYTES, error);
  if (rc == LC_OK && cipher_total != NULL) {
    *cipher_total += 8UL + (unsigned long)cipher_len + LC_POUCH_GCM_TAG_BYTES;
  }
  memset(nonce, 0, sizeof(nonce));
  return rc;
}

static int lc_pouch_crypto_stream_plain_to_file(const char *path,
                                                lc_source *body,
                                                unsigned long *plain_bytes,
                                                unsigned long *cipher_bytes,
                                                lc_error *error) {
  unsigned char buffer[LC_POUCH_FRAME_PLAINTEXT_BYTES];
  int fd;
  unsigned long total;
  int rc;

  fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0666);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create pouch state payload", strerror(errno),
                        NULL, "pouch");
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
    rc = lc_pouch_crypto_write_all_fd(fd, buffer, got, error);
    if (rc != LC_OK) {
      break;
    }
    total += (unsigned long)got;
  }
  if (rc == LC_OK && fsync(fd) != 0) {
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
    return rc;
  }
  if (plain_bytes != NULL) {
    *plain_bytes = total;
  }
  if (cipher_bytes != NULL) {
    *cipher_bytes = total;
  }
  return LC_OK;
}

int lc_pouch_crypto_stream_to_file(lc_pouch_crypto *crypto, const char *context,
                                   const char *path, lc_source *body,
                                   unsigned long *plain_bytes,
                                   unsigned long *cipher_bytes,
                                   char **descriptor_out, lc_error *error) {
  lc_pouch_crypto_desc desc;
  EVP_CIPHER_CTX *ctx;
  unsigned char key[LC_POUCH_DEK_BYTES];
  unsigned char buffer[LC_POUCH_FRAME_PLAINTEXT_BYTES];
  unsigned long plain_total;
  unsigned long cipher_total;
  unsigned long counter;
  char *descriptor;
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
  if (crypto == NULL) {
    return lc_pouch_crypto_stream_plain_to_file(path, body, plain_bytes,
                                                cipher_bytes, error);
  }
  if (context == NULL || context[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto write requires context", NULL, NULL,
                        "pouch");
  }
  memset(&desc, 0, sizeof(desc));
  desc.frame_size = LC_POUCH_FRAME_PLAINTEXT_BYTES;
  if (!lc_pouch_crypto_random_bytes(desc.salt, sizeof(desc.salt)) ||
      !lc_pouch_crypto_random_bytes(desc.nonce_prefix,
                                    sizeof(desc.nonce_prefix))) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to generate pouch crypto descriptor material",
                        NULL, NULL, "pouch");
  }
  descriptor = NULL;
  rc = lc_pouch_crypto_descriptor_encode(&desc, &descriptor, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_crypto_derive(crypto, &desc, context, key, error);
  if (rc != LC_OK) {
    free(descriptor);
    return rc;
  }
  fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0666);
  if (fd < 0) {
    memset(key, 0, sizeof(key));
    free(descriptor);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create encrypted pouch state payload",
                        strerror(errno), NULL, "pouch");
  }
  ctx = EVP_CIPHER_CTX_new();
  if (ctx == NULL) {
    close(fd);
    unlink(path);
    memset(key, 0, sizeof(key));
    free(descriptor);
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

    got = body->read(body, buffer, desc.frame_size, error);
    if (got == 0U) {
      if (error != NULL && error->code != LC_OK) {
        rc = error->code;
      }
      break;
    }
    rc = lc_pouch_crypto_encrypt_frame(fd, ctx, key, desc.nonce_prefix, counter,
                                       buffer, got, &cipher_total, error);
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
    rc = lc_pouch_crypto_encrypt_frame(fd, ctx, key, desc.nonce_prefix, counter,
                                       buffer, 0U, &cipher_total, error);
  }
  if (rc == LC_OK && fsync(fd) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to fsync encrypted pouch state payload",
                      strerror(errno), NULL, "pouch");
  }
  if (close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close encrypted pouch state payload",
                      strerror(errno), NULL, "pouch");
  }
  EVP_CIPHER_CTX_free(ctx);
  memset(key, 0, sizeof(key));
  memset(buffer, 0, sizeof(buffer));
  if (rc != LC_OK) {
    unlink(path);
    free(descriptor);
    return rc;
  }
  *plain_bytes = plain_total;
  *cipher_bytes = cipher_total;
  *descriptor_out = descriptor;
  return LC_OK;
}

static size_t lc_pouch_crypto_source_read(lc_source *self, void *buffer,
                                          size_t count, lc_error *error);
static int lc_pouch_crypto_source_reset(lc_source *self, lc_error *error);
static void lc_pouch_crypto_source_close(lc_source *self);

int lc_pouch_crypto_source_from_file(lc_pouch_crypto *crypto,
                                     const char *context, const char *path,
                                     const char *descriptor, lc_source **out,
                                     lc_error *error) {
  lc_pouch_crypto_source *source;
  lc_pouch_crypto_desc desc;
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch crypto source requires out", NULL, NULL,
                        "pouch");
  }
  *out = NULL;
  if (crypto == NULL && descriptor == NULL) {
    return lc_source_from_file(path, out, error);
  }
  if (crypto == NULL && descriptor != NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "encrypted pouch payload requires crypto key", NULL,
                        NULL, "pouch");
  }
  if (descriptor == NULL || descriptor[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "encrypted pouch payload is missing descriptor", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_crypto_descriptor_decode(descriptor, &desc, error);
  if (rc != LC_OK) {
    return rc;
  }
  source = (lc_pouch_crypto_source *)calloc(1U, sizeof(*source));
  if (source == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch crypto source", NULL, NULL,
                        "pouch");
  }
  source->ctx = EVP_CIPHER_CTX_new();
  if (source->ctx == NULL) {
    free(source);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch decrypt context", NULL, NULL,
                        "pouch");
  }
  source->fp = fopen(path, "rb");
  if (source->fp == NULL) {
    EVP_CIPHER_CTX_free(source->ctx);
    free(source);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open encrypted pouch payload",
                        strerror(errno), NULL, "pouch");
  }
  rc = lc_pouch_crypto_derive(crypto, &desc, context, source->key, error);
  if (rc != LC_OK) {
    fclose(source->fp);
    EVP_CIPHER_CTX_free(source->ctx);
    free(source);
    return rc;
  }
  memcpy(source->nonce_prefix, desc.nonce_prefix, sizeof(source->nonce_prefix));
  source->frame_size = desc.frame_size;
  source->pub.read = lc_pouch_crypto_source_read;
  source->pub.reset = lc_pouch_crypto_source_reset;
  source->pub.close = lc_pouch_crypto_source_close;
  source->pub.impl = source;
  *out = &source->pub;
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
       EVP_DecryptUpdate(source->ctx, source->plain, &out_len, source->cipher,
                         (int)plain_len) == 1 &&
       EVP_CIPHER_CTX_ctrl(source->ctx, EVP_CTRL_GCM_SET_TAG,
                           LC_POUCH_GCM_TAG_BYTES, tag) == 1 &&
       EVP_DecryptFinal_ex(source->ctx, source->plain + out_len, &final_len) ==
           1;
  memset(nonce, 0, sizeof(nonce));
  if (!ok || (unsigned long)(out_len + final_len) != plain_len) {
    return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "encrypted pouch payload authentication failed", NULL,
                        NULL, "pouch");
  }
  ++source->counter;
  source->plain_offset = 0U;
  source->plain_length = (size_t)plain_len;
  if (plain_len == 0UL) {
    int extra;

    extra = fgetc(source->fp);
    if (extra != EOF) {
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "encrypted pouch payload has trailing bytes", NULL,
                          NULL, "pouch");
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
  memset(source->key, 0, sizeof(source->key));
  memset(source->plain, 0, sizeof(source->plain));
  free(source);
}

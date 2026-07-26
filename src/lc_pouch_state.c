#include "lc_pouch.h"

#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define LC_POUCH_STATE_COPY_CHUNK 16384U
#define LC_POUCH_STATE_LINE_LIMIT 8192U

typedef struct lc_pouch_state_entry {
  char *key;
  char *content_type;
  char *etag;
  char *payload_leaf;
  unsigned long version;
  unsigned long bytes;
  int seen;
  int found;
} lc_pouch_state_entry;

static void lc_pouch_state_entry_cleanup(const lc_allocator *allocator,
                                         lc_pouch_state_entry *entry) {
  if (entry == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, entry->key);
  lc_free_with_allocator(allocator, entry->content_type);
  lc_free_with_allocator(allocator, entry->etag);
  lc_free_with_allocator(allocator, entry->payload_leaf);
  memset(entry, 0, sizeof(*entry));
}

static int lc_pouch_state_hex_value(char ch) {
  if (ch >= '0' && ch <= '9') {
    return ch - '0';
  }
  if (ch >= 'a' && ch <= 'f') {
    return ch - 'a' + 10;
  }
  if (ch >= 'A' && ch <= 'F') {
    return ch - 'A' + 10;
  }
  return -1;
}

static char *lc_pouch_state_hex_encode(const lc_allocator *allocator,
                                       const char *value) {
  static const char hex[] = "0123456789abcdef";
  const unsigned char *src;
  char *encoded;
  char *dst;
  size_t len;

  len = value != NULL ? strlen(value) : 0U;
  encoded = (char *)lc_alloc_with_allocator(allocator, len * 2U + 1U);
  if (encoded == NULL) {
    return NULL;
  }
  src = (const unsigned char *)(value != NULL ? value : "");
  dst = encoded;
  while (*src != '\0') {
    *dst++ = hex[*src >> 4];
    *dst++ = hex[*src & 0x0fU];
    ++src;
  }
  *dst = '\0';
  return encoded;
}

static char *lc_pouch_state_hex_decode(const lc_allocator *allocator,
                                       const char *encoded) {
  char *decoded;
  char *dst;
  size_t len;
  size_t i;

  if (encoded == NULL) {
    return NULL;
  }
  len = strlen(encoded);
  if ((len % 2U) != 0U) {
    return NULL;
  }
  decoded = (char *)lc_alloc_with_allocator(allocator, len / 2U + 1U);
  if (decoded == NULL) {
    return NULL;
  }
  dst = decoded;
  for (i = 0U; i < len; i += 2U) {
    int hi;
    int lo;

    hi = lc_pouch_state_hex_value(encoded[i]);
    lo = lc_pouch_state_hex_value(encoded[i + 1U]);
    if (hi < 0 || lo < 0) {
      lc_free_with_allocator(allocator, decoded);
      return NULL;
    }
    *dst++ = (char)((hi << 4) | lo);
  }
  *dst = '\0';
  return decoded;
}

static char *lc_pouch_state_child_path(const lc_allocator *allocator,
                                       const char *namespace_path,
                                       const char *child,
                                       const char *leaf) {
  char *dir;
  char *path;

  dir = lc_pouch_path_join(allocator, namespace_path, child);
  path = dir != NULL ? lc_pouch_path_join(allocator, dir, leaf) : NULL;
  lc_free_with_allocator(allocator, dir);
  return path;
}

static int lc_pouch_state_touch_marker(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    lc_error *error) {
  unsigned long sequence;

  sequence = ++pouch->marker_sequence;
  return lc_pouch_namespace_touch_marker(&pouch->allocator,
                                         manifest->namespace_path, sequence,
                                         error);
}

static int lc_pouch_state_write_all(int fd, const void *bytes, size_t count,
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
                          "failed to write pouch state bytes", strerror(errno),
                          NULL, NULL);
    }
    if (written == 0) {
      return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                          "short write while writing pouch state bytes", NULL,
                          NULL, NULL);
    }
    cursor += written;
    count -= (size_t)written;
  }
  return LC_OK;
}

static int lc_pouch_state_stream_payload(const char *path, lc_source *body,
                                         unsigned long *bytes,
                                         lc_error *error) {
  unsigned char buffer[LC_POUCH_STATE_COPY_CHUNK];
  unsigned long total;
  int fd;
  int rc;

  fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create pouch state payload", strerror(errno),
                        NULL, NULL);
  }
  total = 0UL;
  rc = LC_OK;
  for (;;) {
    size_t nread;

    nread = body->read(body, buffer, sizeof(buffer), error);
    if (nread == 0U) {
      if (error != NULL && error->code != LC_OK) {
        rc = error->code;
      }
      break;
    }
    rc = lc_pouch_state_write_all(fd, buffer, nread, error);
    if (rc != LC_OK) {
      break;
    }
    total += (unsigned long)nread;
  }
  if (rc == LC_OK && fsync(fd) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to fsync pouch state payload", strerror(errno),
                      NULL, NULL);
  }
  if (close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state payload", strerror(errno),
                      NULL, NULL);
  }
  if (rc == LC_OK && bytes != NULL) {
    *bytes = total;
  }
  if (rc != LC_OK) {
    unlink(path);
  }
  return rc;
}

static int lc_pouch_state_read_line(FILE *fp, char *line, size_t line_size,
                                    int *truncated) {
  size_t len;

  if (fgets(line, (int)line_size, fp) == NULL) {
    return 0;
  }
  len = strlen(line);
  if (len > 0U && line[len - 1U] == '\n') {
    line[len - 1U] = '\0';
  } else if (!feof(fp)) {
    int ch;

    *truncated = 1;
    do {
      ch = fgetc(fp);
    } while (ch != EOF && ch != '\n');
  }
  return 1;
}

static int lc_pouch_state_file_size(const char *path, unsigned long *size,
                                    lc_error *error) {
  struct stat st;

  if (stat(path, &st) != 0) {
    if (errno == ENOENT) {
      *size = 0UL;
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to stat pouch state segment", strerror(errno),
                        NULL, NULL);
  }
  if (!S_ISREG(st.st_mode)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state segment path is not a file", NULL, NULL,
                        NULL);
  }
  *size = (unsigned long)st.st_size;
  return LC_OK;
}

static int lc_pouch_state_parse_record(const lc_allocator *allocator,
                                       const char *line,
                                       lc_pouch_state_entry *entry,
                                       lc_error *error) {
  char key_hex[LC_POUCH_STATE_LINE_LIMIT];
  char content_type_hex[LC_POUCH_STATE_LINE_LIMIT];
  char etag_hex[LC_POUCH_STATE_LINE_LIMIT];
  char payload_leaf[256];
  unsigned long version;
  unsigned long bytes;
  char tag;
  int matched;

  matched = sscanf(line, "%c %lu %lu %8191s %8191s %8191s %255s", &tag,
                   &version, &bytes, key_hex, content_type_hex, etag_hex,
                   payload_leaf);
  if (matched != 7 || (tag != 'S' && tag != 'L')) {
    matched = sscanf(line, "%c %lu %8191s %8191s", &tag, &version, key_hex,
                     etag_hex);
    if (matched != 4 || tag != 'D') {
      return LC_OK;
    }
    lc_pouch_state_entry_cleanup(allocator, entry);
    entry->key = lc_pouch_state_hex_decode(allocator, key_hex);
    entry->etag = lc_pouch_state_hex_decode(allocator, etag_hex);
    if (entry->key == NULL || entry->etag == NULL) {
      lc_pouch_state_entry_cleanup(allocator, entry);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to decode pouch state tombstone record",
                          NULL, NULL, NULL);
    }
    entry->version = version;
    entry->bytes = 0UL;
    entry->seen = 1;
    entry->found = 0;
    return LC_OK;
  }
  lc_pouch_state_entry_cleanup(allocator, entry);
  entry->key = lc_pouch_state_hex_decode(allocator, key_hex);
  entry->content_type =
      lc_pouch_state_hex_decode(allocator, content_type_hex);
  entry->etag = lc_pouch_state_hex_decode(allocator, etag_hex);
  entry->payload_leaf = lc_strdup_with_allocator(allocator, payload_leaf);
  if (entry->key == NULL || entry->content_type == NULL ||
      entry->etag == NULL || entry->payload_leaf == NULL) {
    lc_pouch_state_entry_cleanup(allocator, entry);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to decode pouch state segment record", NULL,
                        NULL, NULL);
  }
  entry->version = version;
  entry->bytes = bytes;
  entry->seen = 1;
  entry->found = 1;
  return LC_OK;
}

static int lc_pouch_state_scan_file(lc_pouch *pouch, const char *segment_path,
                                    const char *key,
                                    lc_pouch_state_entry *current,
                                    unsigned long *max_version,
                                    lc_error *error) {
  char line[LC_POUCH_STATE_LINE_LIMIT];
  lc_pouch_state_entry entry;
  FILE *fp;
  int truncated;
  int rc;

  fp = fopen(segment_path, "rb");
  if (fp == NULL) {
    if (errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch state segment", strerror(errno),
                        NULL, NULL);
  }

  memset(&entry, 0, sizeof(entry));
  truncated = 0;
  rc = LC_OK;
  while (lc_pouch_state_read_line(fp, line, sizeof(line), &truncated)) {
    if (truncated) {
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch state segment record exceeds line limit", NULL,
                        NULL, NULL);
      break;
    }
    rc = lc_pouch_state_parse_record(&pouch->allocator, line, &entry, error);
    if (rc != LC_OK) {
      break;
    }
    if (!entry.seen) {
      continue;
    }
    if (max_version != NULL && entry.version > *max_version) {
      *max_version = entry.version;
    }
    if (key != NULL && strcmp(entry.key, key) == 0) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, current);
      *current = entry;
      memset(&entry, 0, sizeof(entry));
    }
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &entry);
  if (fclose(fp) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state segment", strerror(errno),
                      NULL, NULL);
  }
  return rc;
}

static int lc_pouch_state_scan(lc_pouch *pouch,
                               const lc_pouch_namespace_manifest *manifest,
                               const char *key,
                               lc_pouch_state_entry *current,
                               unsigned long *max_version, lc_error *error) {
  unsigned long segment_id;
  int rc;

  memset(current, 0, sizeof(*current));
  if (max_version != NULL) {
    *max_version = 0UL;
  }
  rc = LC_OK;
  for (segment_id = 1UL; segment_id <= manifest->max_segment_id;
       ++segment_id) {
    char *segment_leaf;
    char *segment_path;

    segment_leaf =
        lc_pouch_namespace_segment_leaf(&pouch->allocator, segment_id);
    segment_path =
        segment_leaf != NULL
            ? lc_pouch_state_child_path(&pouch->allocator,
                                        manifest->namespace_path, "segments",
                                        segment_leaf)
            : NULL;
    lc_free_with_allocator(&pouch->allocator, segment_leaf);
    if (segment_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state segment path", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_state_scan_file(pouch, segment_path, key, current,
                                  max_version, error);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    if (rc != LC_OK) {
      break;
    }
  }
  return rc;
}

static int lc_pouch_state_append_tombstone(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_namespace_manifest *manifest, const char *key, const char *etag,
    unsigned long version, lc_error *error) {
  char *segment_path;
  char *key_hex;
  char *etag_hex;
  char record[LC_POUCH_STATE_LINE_LIMIT];
  int fd;
  int rc;
  int len;
  unsigned long segment_size;

  key_hex = lc_pouch_state_hex_encode(&pouch->allocator, key);
  etag_hex = lc_pouch_state_hex_encode(&pouch->allocator, etag);
  if (key_hex == NULL || etag_hex == NULL) {
    lc_free_with_allocator(&pouch->allocator, key_hex);
    lc_free_with_allocator(&pouch->allocator, etag_hex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to encode pouch state tombstone record", NULL,
                        NULL, NULL);
  }
  len = snprintf(record, sizeof(record), "D %lu %s %s\n", version, key_hex,
                 etag_hex);
  lc_free_with_allocator(&pouch->allocator, key_hex);
  lc_free_with_allocator(&pouch->allocator, etag_hex);
  if (len < 0 || (size_t)len >= sizeof(record)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state tombstone record exceeds line limit",
                        NULL, NULL, NULL);
  }

  segment_path =
      lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                "segments", manifest->active_segment);
  if (segment_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state segment path", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_state_file_size(segment_path, &segment_size, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, segment_path);
    return rc;
  }
  if (segment_size > 0UL &&
      segment_size + (unsigned long)len > pouch->segment_target_bytes) {
    lc_free_with_allocator(&pouch->allocator, segment_path);
    rc = lc_pouch_namespace_manifest_rotate(&pouch->allocator, namespace_name,
                                            manifest,
                                            manifest->active_segment_id + 1UL,
                                            error);
    if (rc != LC_OK) {
      return rc;
    }
    segment_path =
        lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                  "segments", manifest->active_segment);
    if (segment_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state segment path", NULL,
                          NULL, NULL);
    }
  }
  fd = open(segment_path, O_WRONLY | O_CREAT | O_APPEND, 0666);
  lc_free_with_allocator(&pouch->allocator, segment_path);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch state segment", strerror(errno),
                        NULL, NULL);
  }
  rc = lc_pouch_state_write_all(fd, record, (size_t)len, error);
  if (rc == LC_OK && fsync(fd) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to fsync pouch state segment", strerror(errno),
                      NULL, NULL);
  }
  if (close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state segment", strerror(errno),
                      NULL, NULL);
  }
  return rc;
}

static int lc_pouch_state_append_record(lc_pouch *pouch,
                                        const char *namespace_name,
                                        lc_pouch_namespace_manifest *manifest,
                                        char record_tag,
                                        const char *key,
                                        const char *content_type,
                                        const char *etag,
                                        const char *payload_leaf,
                                        unsigned long version,
                                        unsigned long bytes,
                                        lc_error *error) {
  char *segment_path;
  char *key_hex;
  char *content_type_hex;
  char *etag_hex;
  char record[LC_POUCH_STATE_LINE_LIMIT];
  int fd;
  int rc;
  int len;
  unsigned long segment_size;

  key_hex = lc_pouch_state_hex_encode(&pouch->allocator, key);
  content_type_hex = lc_pouch_state_hex_encode(&pouch->allocator,
                                               content_type);
  etag_hex = lc_pouch_state_hex_encode(&pouch->allocator, etag);
  if (key_hex == NULL || content_type_hex == NULL || etag_hex == NULL) {
    lc_free_with_allocator(&pouch->allocator, key_hex);
    lc_free_with_allocator(&pouch->allocator, content_type_hex);
    lc_free_with_allocator(&pouch->allocator, etag_hex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to encode pouch state segment record", NULL,
                        NULL, NULL);
  }
  len = snprintf(record, sizeof(record), "%c %lu %lu %s %s %s %s\n",
                 record_tag, version, bytes, key_hex, content_type_hex,
                 etag_hex, payload_leaf);
  lc_free_with_allocator(&pouch->allocator, key_hex);
  lc_free_with_allocator(&pouch->allocator, content_type_hex);
  lc_free_with_allocator(&pouch->allocator, etag_hex);
  if (len < 0 || (size_t)len >= sizeof(record)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state segment record exceeds line limit", NULL,
                        NULL, NULL);
  }

  segment_path =
      lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                "segments", manifest->active_segment);
  if (segment_path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state segment path", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_state_file_size(segment_path, &segment_size, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, segment_path);
    return rc;
  }
  if (segment_size > 0UL &&
      segment_size + (unsigned long)len > pouch->segment_target_bytes) {
    lc_free_with_allocator(&pouch->allocator, segment_path);
    rc = lc_pouch_namespace_manifest_rotate(&pouch->allocator, namespace_name,
                                            manifest,
                                            manifest->active_segment_id + 1UL,
                                            error);
    if (rc != LC_OK) {
      return rc;
    }
    segment_path =
        lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                  "segments", manifest->active_segment);
    if (segment_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state segment path", NULL,
                          NULL, NULL);
    }
  }
  fd = open(segment_path, O_WRONLY | O_CREAT | O_APPEND, 0666);
  lc_free_with_allocator(&pouch->allocator, segment_path);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch state segment", strerror(errno),
                        NULL, NULL);
  }
  rc = lc_pouch_state_write_all(fd, record, (size_t)len, error);
  if (rc == LC_OK && fsync(fd) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to fsync pouch state segment", strerror(errno),
                      NULL, NULL);
  }
  if (close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state segment", strerror(errno),
                      NULL, NULL);
  }
  return rc;
}

static char *lc_pouch_state_etag(const lc_allocator *allocator,
                                 unsigned long version) {
  char stack[64];

  snprintf(stack, sizeof(stack), "pouch-state-%lu", version);
  return lc_strdup_with_allocator(allocator, stack);
}

static char *lc_pouch_state_payload_leaf(const lc_allocator *allocator,
                                         unsigned long version) {
  char stack[96];

  snprintf(stack, sizeof(stack), "state-%020lu.bin", version);
  return lc_strdup_with_allocator(allocator, stack);
}

static char *lc_pouch_state_staged_key(const lc_allocator *allocator,
                                       const char *key, const char *txn_id) {
  size_t key_len;
  size_t txn_len;
  size_t suffix_len;
  char *staged;

  if (key == NULL || key[0] == '\0' || txn_id == NULL ||
      txn_id[0] == '\0') {
    return NULL;
  }
  key_len = strlen(key);
  txn_len = strlen(txn_id);
  suffix_len = sizeof("/.staging/") - 1U;
  staged = (char *)lc_alloc_with_allocator(allocator,
                                           key_len + suffix_len + txn_len + 1U);
  if (staged == NULL) {
    return NULL;
  }
  memcpy(staged, key, key_len);
  memcpy(staged + key_len, "/.staging/", suffix_len);
  memcpy(staged + key_len + suffix_len, txn_id, txn_len);
  staged[key_len + suffix_len + txn_len] = '\0';
  return staged;
}

int lc_pouch_state_write(lc_pouch *pouch, const char *namespace_name,
                         const char *key, lc_source *body,
                         const lc_pouch_state_write_options *options,
                         lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_state_entry current;
  lc_pouch_namespace_manifest manifest;
  const char *content_type;
  char *payload_leaf;
  char *payload_path;
  char *etag;
  unsigned long max_version;
  unsigned long version;
  unsigned long bytes;
  int record_appended;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || body == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_write requires pouch, namespace, key, "
                        "body and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  rc = lc_pouch_ensure_namespace(pouch, namespace_name, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&manifest, 0, sizeof(manifest));
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                        namespace_name, &manifest, error);
  if (rc != LC_OK) {
    return rc;
  }

  memset(&current, 0, sizeof(current));
  rc = lc_pouch_state_scan(pouch, &manifest, key, &current, &max_version,
                           error);
  if (rc != LC_OK) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  if (options != NULL && options->expected_etag != NULL) {
    if (!current.found || strcmp(current.etag, options->expected_etag) != 0) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch state etag precondition failed", NULL, NULL,
                          NULL);
    }
  }
  if (options != NULL && options->has_expected_version) {
    if (!current.found || current.version != options->expected_version) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch state version precondition failed", NULL,
                          NULL, NULL);
    }
  }
  version = max_version + 1UL;
  etag = lc_pouch_state_etag(&pouch->allocator, version);
  payload_leaf = lc_pouch_state_payload_leaf(&pouch->allocator, version);
  payload_path =
      payload_leaf != NULL
          ? lc_pouch_state_child_path(&pouch->allocator, manifest.namespace_path,
                                      "payloads", payload_leaf)
          : NULL;
  if (etag == NULL || payload_leaf == NULL || payload_path == NULL) {
    lc_free_with_allocator(&pouch->allocator, etag);
    lc_free_with_allocator(&pouch->allocator, payload_leaf);
    lc_free_with_allocator(&pouch->allocator, payload_path);
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state write paths", NULL,
                        NULL, NULL);
  }

  bytes = 0UL;
  record_appended = 0;
  rc = lc_pouch_state_stream_payload(payload_path, body, &bytes, error);
  content_type = options != NULL && options->content_type != NULL
                     ? options->content_type
                     : "application/octet-stream";
  if (rc == LC_OK) {
    rc = lc_pouch_state_append_record(pouch, namespace_name, &manifest, 'S',
                                      key, content_type, etag, payload_leaf,
                                      version, bytes, error);
    if (rc == LC_OK) {
      record_appended = 1;
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_touch_marker(pouch, &manifest, error);
  }
  if (rc == LC_OK) {
    out->etag = etag;
    out->version = version;
    out->bytes = bytes;
    etag = NULL;
  } else {
    if (!record_appended) {
      unlink(payload_path);
    }
  }
  lc_free_with_allocator(&pouch->allocator, etag);
  lc_free_with_allocator(&pouch->allocator, payload_leaf);
  lc_free_with_allocator(&pouch->allocator, payload_path);
  lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

void lc_pouch_state_write_result_cleanup(const lc_allocator *allocator,
                                         lc_pouch_state_write_result *result) {
  if (result == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, result->etag);
  memset(result, 0, sizeof(*result));
}

int lc_pouch_state_delete(lc_pouch *pouch, const char *namespace_name,
                          const char *key,
                          const lc_pouch_state_write_options *options,
                          lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_state_entry current;
  lc_pouch_namespace_manifest manifest;
  char *etag;
  unsigned long max_version;
  unsigned long version;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_delete requires pouch, namespace, key "
                        "and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  rc = lc_pouch_ensure_namespace(pouch, namespace_name, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&manifest, 0, sizeof(manifest));
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                        namespace_name, &manifest, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&current, 0, sizeof(current));
  rc = lc_pouch_state_scan(pouch, &manifest, key, &current, &max_version,
                           error);
  if (rc != LC_OK) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  if (options != NULL && options->expected_etag != NULL) {
    if (!current.found || strcmp(current.etag, options->expected_etag) != 0) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch state delete etag precondition failed", NULL,
                          NULL, NULL);
    }
  }
  if (options != NULL && options->has_expected_version) {
    if (!current.found || current.version != options->expected_version) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
      lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
      return lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch state delete version precondition failed",
                          NULL, NULL, NULL);
    }
  }
  if (!current.found) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return LC_OK;
  }
  version = max_version + 1UL;
  etag = lc_pouch_state_etag(&pouch->allocator, version);
  if (etag == NULL) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state tombstone etag", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_state_append_tombstone(pouch, namespace_name, &manifest, key,
                                       etag, version, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_touch_marker(pouch, &manifest, error);
  }
  if (rc == LC_OK) {
    out->etag = etag;
    out->version = version;
    out->bytes = 0UL;
    etag = NULL;
  }
  lc_free_with_allocator(&pouch->allocator, etag);
  lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

int lc_pouch_state_stage_write(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    const char *txn_id, lc_source *body,
    const lc_pouch_state_write_options *options,
    lc_pouch_state_write_result *out, lc_error *error) {
  char *staged_key;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || txn_id == NULL || txn_id[0] == '\0' ||
      body == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_stage_write requires pouch, "
                        "namespace, key, txn_id, body and out",
                        NULL, NULL, NULL);
  }
  staged_key = lc_pouch_state_staged_key(&pouch->allocator, key, txn_id);
  if (staged_key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch staged state key", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_state_write(pouch, namespace_name, staged_key, body, options,
                            out, error);
  lc_free_with_allocator(&pouch->allocator, staged_key);
  return rc;
}

int lc_pouch_state_promote_staged(lc_pouch *pouch, const char *namespace_name,
                                  const char *key, const char *txn_id,
                                  const char *expected_committed_etag,
                                  lc_pouch_state_write_result *out,
                                  lc_error *error) {
  lc_pouch_state_entry committed;
  lc_pouch_state_entry staged;
  lc_pouch_namespace_manifest manifest;
  char *staged_key;
  unsigned long committed_max_version;
  unsigned long staged_max_version;
  unsigned long version;
  unsigned long discard_version;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || txn_id == NULL || txn_id[0] == '\0' ||
      out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_promote_staged requires pouch, "
                        "namespace, key, txn_id and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  staged_key = lc_pouch_state_staged_key(&pouch->allocator, key, txn_id);
  if (staged_key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch staged state key", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_ensure_namespace(pouch, namespace_name, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, staged_key);
    return rc;
  }
  memset(&manifest, 0, sizeof(manifest));
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                        namespace_name, &manifest, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, staged_key);
    return rc;
  }
  memset(&committed, 0, sizeof(committed));
  memset(&staged, 0, sizeof(staged));
  rc = lc_pouch_state_scan(pouch, &manifest, key, &committed,
                           &committed_max_version, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_scan(pouch, &manifest, staged_key, &staged,
                             &staged_max_version, error);
  }
  if (rc != LC_OK) {
    goto cleanup;
  }
  if (expected_committed_etag != NULL) {
    if (!committed.found ||
        strcmp(committed.etag, expected_committed_etag) != 0) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch staged promotion committed etag precondition "
                        "failed",
                        NULL, NULL, NULL);
      goto cleanup;
    }
  } else if (committed.found) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch staged promotion committed state already exists",
                      NULL, NULL, NULL);
    goto cleanup;
  }
  if (!staged.found) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch staged promotion source is missing", NULL, NULL,
                      NULL);
    goto cleanup;
  }
  version = committed_max_version > staged_max_version ? committed_max_version
                                                       : staged_max_version;
  version++;
  rc = lc_pouch_state_append_record(
      pouch, namespace_name, &manifest, 'L', key, staged.content_type,
      staged.etag, staged.payload_leaf, version, staged.bytes, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  discard_version = version + 1UL;
  rc = lc_pouch_state_append_tombstone(pouch, namespace_name, &manifest,
                                       staged_key, staged.etag,
                                       discard_version, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  rc = lc_pouch_state_touch_marker(pouch, &manifest, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  out->etag = lc_strdup_with_allocator(&pouch->allocator, staged.etag);
  if (out->etag == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch staged promotion etag", NULL,
                      NULL, NULL);
    goto cleanup;
  }
  out->version = version;
  out->bytes = staged.bytes;

cleanup:
  lc_pouch_state_entry_cleanup(&pouch->allocator, &staged);
  lc_pouch_state_entry_cleanup(&pouch->allocator, &committed);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  lc_free_with_allocator(&pouch->allocator, staged_key);
  if (rc != LC_OK) {
    lc_pouch_state_write_result_cleanup(&pouch->allocator, out);
  }
  return rc;
}

int lc_pouch_state_discard_staged(lc_pouch *pouch, const char *namespace_name,
                                  const char *key, const char *txn_id,
                                  int *discarded, lc_error *error) {
  lc_pouch_state_entry staged;
  lc_pouch_namespace_manifest manifest;
  char *staged_key;
  char *etag;
  unsigned long max_version;
  int rc;

  if (discarded != NULL) {
    *discarded = 0;
  }
  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || txn_id == NULL || txn_id[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_discard_staged requires pouch, "
                        "namespace, key and txn_id",
                        NULL, NULL, NULL);
  }
  staged_key = lc_pouch_state_staged_key(&pouch->allocator, key, txn_id);
  if (staged_key == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch staged state key", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_ensure_namespace(pouch, namespace_name, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, staged_key);
    return rc;
  }
  memset(&manifest, 0, sizeof(manifest));
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                        namespace_name, &manifest, error);
  if (rc != LC_OK) {
    lc_free_with_allocator(&pouch->allocator, staged_key);
    return rc;
  }
  memset(&staged, 0, sizeof(staged));
  etag = NULL;
  rc = lc_pouch_state_scan(pouch, &manifest, staged_key, &staged,
                           &max_version, error);
  if (rc == LC_OK && staged.found) {
    etag = lc_pouch_state_etag(&pouch->allocator, max_version + 1UL);
    if (etag == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch staged discard etag", NULL,
                        NULL, NULL);
    } else {
      rc = lc_pouch_state_append_tombstone(pouch, namespace_name, &manifest,
                                           staged_key, etag, max_version + 1UL,
                                           error);
      if (rc == LC_OK) {
        rc = lc_pouch_state_touch_marker(pouch, &manifest, error);
      }
      if (rc == LC_OK && discarded != NULL) {
        *discarded = 1;
      }
    }
  }
  lc_free_with_allocator(&pouch->allocator, etag);
  lc_pouch_state_entry_cleanup(&pouch->allocator, &staged);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  lc_free_with_allocator(&pouch->allocator, staged_key);
  return rc;
}

int lc_pouch_state_read(lc_pouch *pouch, const char *namespace_name,
                        const char *key, lc_pouch_state_read_result *out,
                        lc_error *error) {
  lc_pouch_state_entry current;
  lc_pouch_namespace_manifest manifest;
  char *payload_path;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_read requires pouch, namespace, key "
                        "and out",
                        NULL, NULL, NULL);
  }
  memset(out, 0, sizeof(*out));
  rc = lc_pouch_ensure_namespace(pouch, namespace_name, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&manifest, 0, sizeof(manifest));
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                        namespace_name, &manifest, error);
  if (rc != LC_OK) {
    return rc;
  }
  memset(&current, 0, sizeof(current));
  rc = lc_pouch_state_scan(pouch, &manifest, key, &current, NULL, error);
  if (rc != LC_OK || !current.found) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  payload_path =
      lc_pouch_state_child_path(&pouch->allocator, manifest.namespace_path,
                                "payloads", current.payload_leaf);
  if (payload_path == NULL) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state payload path", NULL,
                        NULL, NULL);
  }
  rc = lc_source_from_file(payload_path, &out->body, error);
  lc_free_with_allocator(&pouch->allocator, payload_path);
  if (rc == LC_OK) {
    out->content_type = current.content_type;
    out->etag = current.etag;
    out->version = current.version;
    out->bytes = current.bytes;
    out->found = 1;
    current.content_type = NULL;
    current.etag = NULL;
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

void lc_pouch_state_read_result_cleanup(const lc_allocator *allocator,
                                        lc_pouch_state_read_result *result) {
  if (result == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, result->content_type);
  lc_free_with_allocator(allocator, result->etag);
  if (result->body != NULL) {
    result->body->close(result->body);
  }
  memset(result, 0, sizeof(*result));
}

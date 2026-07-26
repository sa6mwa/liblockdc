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
#define LC_POUCH_STATE_SHARED_FORCE_AFTER_SKIPS 64UL
#define LC_POUCH_STATE_DECISION_COMMITTED "committed"
#define LC_POUCH_STATE_DECISION_DISCARDED "discarded"

typedef struct lc_pouch_state_entry {
  char *key;
  char *content_type;
  char *etag;
  char *payload_leaf;
  char *decision;
  unsigned long version;
  unsigned long bytes;
  int has_query_hidden;
  int query_hidden;
  int seen;
  int found;
  int control;
} lc_pouch_state_entry;

typedef struct lc_pouch_state_decision {
  char *staged_key;
  char *etag;
  char *decision;
  unsigned long version;
  struct lc_pouch_state_decision *next;
} lc_pouch_state_decision;

typedef struct lc_pouch_state_cache_record {
  char *key;
  char *content_type;
  char *etag;
  char *payload_leaf;
  unsigned long version;
  unsigned long bytes;
  int has_query_hidden;
  int query_hidden;
  int found;
  struct lc_pouch_state_cache_record *next;
} lc_pouch_state_cache_record;

typedef struct lc_pouch_state_visit_snapshot {
  char *key;
  char *content_type;
  char *etag;
  unsigned long version;
  unsigned long bytes;
  int has_query_hidden;
  int query_hidden;
} lc_pouch_state_visit_snapshot;

struct lc_pouch_state_cache_namespace {
  char *namespace_name;
  unsigned long max_segment_id;
  unsigned long max_version;
  int initialized;
  int decision_recovery_checked;
  lc_pouch_namespace_marker_refresh_state marker_refresh;
  lc_pouch_state_cache_record *records;
  struct lc_pouch_state_cache_namespace *next;
};

static void lc_pouch_state_entry_cleanup(const lc_allocator *allocator,
                                         lc_pouch_state_entry *entry) {
  if (entry == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, entry->key);
  lc_free_with_allocator(allocator, entry->content_type);
  lc_free_with_allocator(allocator, entry->etag);
  lc_free_with_allocator(allocator, entry->payload_leaf);
  lc_free_with_allocator(allocator, entry->decision);
  memset(entry, 0, sizeof(*entry));
}

static void lc_pouch_state_decision_cleanup(
    const lc_allocator *allocator, lc_pouch_state_decision *decision) {
  if (decision == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, decision->staged_key);
  lc_free_with_allocator(allocator, decision->etag);
  lc_free_with_allocator(allocator, decision->decision);
  lc_free_with_allocator(allocator, decision);
}

static void lc_pouch_state_decisions_cleanup(
    const lc_allocator *allocator, lc_pouch_state_decision *decision) {
  while (decision != NULL) {
    lc_pouch_state_decision *next;

    next = decision->next;
    lc_pouch_state_decision_cleanup(allocator, decision);
    decision = next;
  }
}

static void lc_pouch_state_cache_record_cleanup(
    const lc_allocator *allocator, lc_pouch_state_cache_record *record) {
  if (record == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, record->key);
  lc_free_with_allocator(allocator, record->content_type);
  lc_free_with_allocator(allocator, record->etag);
  lc_free_with_allocator(allocator, record->payload_leaf);
  lc_free_with_allocator(allocator, record);
}

static void lc_pouch_state_cache_records_cleanup(
    const lc_allocator *allocator, lc_pouch_state_cache_record *record) {
  while (record != NULL) {
    lc_pouch_state_cache_record *next;

    next = record->next;
    lc_pouch_state_cache_record_cleanup(allocator, record);
    record = next;
  }
}

static void lc_pouch_state_visit_snapshot_cleanup(
    const lc_allocator *allocator, lc_pouch_state_visit_snapshot *snapshot) {
  if (snapshot == NULL) {
    return;
  }
  lc_free_with_allocator(allocator, snapshot->key);
  lc_free_with_allocator(allocator, snapshot->content_type);
  lc_free_with_allocator(allocator, snapshot->etag);
  memset(snapshot, 0, sizeof(*snapshot));
}

static void lc_pouch_state_visit_snapshots_cleanup(
    const lc_allocator *allocator, lc_pouch_state_visit_snapshot *snapshots,
    size_t count) {
  size_t i;

  if (snapshots == NULL) {
    return;
  }
  for (i = 0U; i < count; ++i) {
    lc_pouch_state_visit_snapshot_cleanup(allocator, &snapshots[i]);
  }
  lc_free_with_allocator(allocator, snapshots);
}

static int lc_pouch_state_visit_snapshot_append(
    const lc_allocator *allocator, lc_pouch_state_visit_snapshot **snapshots,
    size_t *count, size_t *capacity,
    const lc_pouch_state_cache_record *record, lc_error *error) {
  lc_pouch_state_visit_snapshot *next;
  lc_pouch_state_visit_snapshot *snapshot;
  size_t next_capacity;

  if (*count == *capacity) {
    next_capacity = *capacity == 0U ? 16U : *capacity * 2U;
    next = (lc_pouch_state_visit_snapshot *)lc_calloc_with_allocator(
        allocator, next_capacity, sizeof(**snapshots));
    if (next == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state visit snapshot",
                          NULL, NULL, NULL);
    }
    if (*snapshots != NULL) {
      memcpy(next, *snapshots, *count * sizeof(**snapshots));
      lc_free_with_allocator(allocator, *snapshots);
    }
    *snapshots = next;
    *capacity = next_capacity;
  }
  snapshot = &(*snapshots)[*count];
  snapshot->key = lc_strdup_with_allocator(allocator, record->key);
  snapshot->content_type =
      record->content_type != NULL
          ? lc_strdup_with_allocator(allocator, record->content_type)
          : NULL;
  snapshot->etag = record->etag != NULL
                       ? lc_strdup_with_allocator(allocator, record->etag)
                       : NULL;
  if (snapshot->key == NULL ||
      (record->content_type != NULL && snapshot->content_type == NULL) ||
      (record->etag != NULL && snapshot->etag == NULL)) {
    lc_pouch_state_visit_snapshot_cleanup(allocator, snapshot);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state visit entry", NULL,
                        NULL, NULL);
  }
  snapshot->version = record->version;
  snapshot->bytes = record->bytes;
  snapshot->has_query_hidden = record->has_query_hidden;
  snapshot->query_hidden = record->query_hidden;
  ++*count;
  return LC_OK;
}

void lc_pouch_state_cache_cleanup(lc_pouch *pouch) {
  lc_pouch_state_cache_namespace *ns;

  if (pouch == NULL) {
    return;
  }
  ns = pouch->state_cache_namespaces;
  while (ns != NULL) {
    lc_pouch_state_cache_namespace *next;

    next = ns->next;
    lc_free_with_allocator(&pouch->allocator, ns->namespace_name);
    lc_pouch_state_cache_records_cleanup(&pouch->allocator, ns->records);
    lc_pouch_namespace_marker_refresh_state_cleanup(&pouch->allocator,
                                                    &ns->marker_refresh);
    lc_free_with_allocator(&pouch->allocator, ns);
    ns = next;
  }
  pouch->state_cache_namespaces = NULL;
}

static lc_pouch_state_cache_namespace *lc_pouch_state_cache_namespace_find(
    lc_pouch *pouch, const char *namespace_name, int create, lc_error *error) {
  lc_pouch_state_cache_namespace *ns;

  for (ns = pouch->state_cache_namespaces; ns != NULL; ns = ns->next) {
    if (strcmp(ns->namespace_name, namespace_name) == 0) {
      return ns;
    }
  }
  if (!create) {
    return NULL;
  }
  ns = (lc_pouch_state_cache_namespace *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*ns));
  if (ns == NULL) {
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch state cache namespace", NULL, NULL,
                 NULL);
    return NULL;
  }
  ns->namespace_name =
      lc_strdup_with_allocator(&pouch->allocator, namespace_name);
  if (ns->namespace_name == NULL) {
    lc_free_with_allocator(&pouch->allocator, ns);
    lc_error_set(error, LC_ERR_NOMEM, 0L,
                 "failed to allocate pouch state cache namespace name", NULL,
                 NULL, NULL);
    return NULL;
  }
  ns->next = pouch->state_cache_namespaces;
  pouch->state_cache_namespaces = ns;
  return ns;
}

static lc_pouch_state_cache_record *lc_pouch_state_cache_record_find(
    lc_pouch_state_cache_namespace *ns, const char *key) {
  lc_pouch_state_cache_record *record;

  for (record = ns->records; record != NULL; record = record->next) {
    if (strcmp(record->key, key) == 0) {
      return record;
    }
  }
  return NULL;
}

static int lc_pouch_state_cache_apply_entry(
    lc_pouch *pouch, lc_pouch_state_cache_namespace *ns,
    const lc_pouch_state_entry *entry, lc_error *error) {
  lc_pouch_state_cache_record *record;
  char *key;
  char *content_type;
  char *etag;
  char *payload_leaf;

  if (entry == NULL || !entry->seen || entry->key == NULL) {
    return LC_OK;
  }
  if (entry->control) {
    if (entry->version > ns->max_version) {
      ns->max_version = entry->version;
    }
    return LC_OK;
  }
  record = lc_pouch_state_cache_record_find(ns, entry->key);
  if (record == NULL) {
    record = (lc_pouch_state_cache_record *)lc_calloc_with_allocator(
        &pouch->allocator, 1U, sizeof(*record));
    if (record == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state cache record", NULL,
                          NULL, NULL);
    }
    record->key = lc_strdup_with_allocator(&pouch->allocator, entry->key);
    if (record->key == NULL) {
      lc_pouch_state_cache_record_cleanup(&pouch->allocator, record);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state cache key", NULL,
                          NULL, NULL);
    }
    record->next = ns->records;
    ns->records = record;
  }
  key = record->key;
  content_type =
      entry->content_type != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, entry->content_type)
          : NULL;
  etag = entry->etag != NULL
             ? lc_strdup_with_allocator(&pouch->allocator, entry->etag)
             : NULL;
  payload_leaf =
      entry->payload_leaf != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, entry->payload_leaf)
          : NULL;
  if ((entry->content_type != NULL && content_type == NULL) ||
      (entry->etag != NULL && etag == NULL) ||
      (entry->payload_leaf != NULL && payload_leaf == NULL)) {
    lc_free_with_allocator(&pouch->allocator, content_type);
    lc_free_with_allocator(&pouch->allocator, etag);
    lc_free_with_allocator(&pouch->allocator, payload_leaf);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch state cache record", NULL, NULL,
                        NULL);
  }
  lc_free_with_allocator(&pouch->allocator, record->content_type);
  lc_free_with_allocator(&pouch->allocator, record->etag);
  lc_free_with_allocator(&pouch->allocator, record->payload_leaf);
  record->key = key;
  record->content_type = content_type;
  record->etag = etag;
  record->payload_leaf = payload_leaf;
  record->version = entry->version;
  record->bytes = entry->bytes;
  record->has_query_hidden = entry->has_query_hidden;
  record->query_hidden = entry->query_hidden;
  record->found = entry->found;
  if (entry->version > ns->max_version) {
    ns->max_version = entry->version;
  }
  return LC_OK;
}

static int lc_pouch_state_entry_from_cache_record(
    lc_pouch *pouch, const lc_pouch_state_cache_record *record,
    lc_pouch_state_entry *out, lc_error *error) {
  memset(out, 0, sizeof(*out));
  if (record == NULL) {
    return LC_OK;
  }
  out->key = lc_strdup_with_allocator(&pouch->allocator, record->key);
  out->content_type =
      record->content_type != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, record->content_type)
          : NULL;
  out->etag = record->etag != NULL
                  ? lc_strdup_with_allocator(&pouch->allocator, record->etag)
                  : NULL;
  out->payload_leaf =
      record->payload_leaf != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, record->payload_leaf)
          : NULL;
  if (out->key == NULL ||
      (record->content_type != NULL && out->content_type == NULL) ||
      (record->etag != NULL && out->etag == NULL) ||
      (record->payload_leaf != NULL && out->payload_leaf == NULL)) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, out);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch state cache entry", NULL, NULL,
                        NULL);
  }
  out->version = record->version;
  out->bytes = record->bytes;
  out->has_query_hidden = record->has_query_hidden;
  out->query_hidden = record->query_hidden;
  out->seen = 1;
  out->found = record->found;
  return LC_OK;
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
                                         manifest->namespace_path,
                                         pouch->writer_marker_leaf, sequence,
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
  char decision_hex[LC_POUCH_STATE_LINE_LIMIT];
  char payload_leaf[256];
  unsigned long version;
  unsigned long bytes;
  int has_query_hidden;
  int query_hidden;
  char tag;
  int matched;

  matched = sscanf(line, "%c %lu %8191s %8191s %8191s", &tag, &version,
                   key_hex, etag_hex, decision_hex);
  if (matched == 5 && tag == 'T') {
    lc_pouch_state_entry_cleanup(allocator, entry);
    entry->key = lc_pouch_state_hex_decode(allocator, key_hex);
    entry->etag = lc_pouch_state_hex_decode(allocator, etag_hex);
    entry->decision = lc_pouch_state_hex_decode(allocator, decision_hex);
    if (entry->key == NULL || entry->etag == NULL ||
        entry->decision == NULL) {
      lc_pouch_state_entry_cleanup(allocator, entry);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to decode pouch state decision record",
                          NULL, NULL, NULL);
    }
    if (strcmp(entry->decision, LC_POUCH_STATE_DECISION_COMMITTED) != 0 &&
        strcmp(entry->decision, LC_POUCH_STATE_DECISION_DISCARDED) != 0) {
      lc_pouch_state_entry_cleanup(allocator, entry);
      return lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                          "pouch state decision record has unknown decision",
                          NULL, NULL, NULL);
    }
    entry->version = version;
    entry->bytes = 0UL;
    entry->seen = 1;
    entry->found = 0;
    entry->control = 1;
    return LC_OK;
  }

  has_query_hidden = 0;
  query_hidden = 0;
  matched = sscanf(line, "%c %lu %lu %8191s %8191s %8191s %255s %d %d",
                   &tag, &version, &bytes, key_hex, content_type_hex,
                   etag_hex, payload_leaf, &has_query_hidden, &query_hidden);
  if (matched != 9 || (tag != 'S' && tag != 'L' && tag != 'M')) {
    matched = sscanf(line, "%c %lu %lu %8191s %8191s %8191s %255s", &tag,
                     &version, &bytes, key_hex, content_type_hex, etag_hex,
                     payload_leaf);
    if (matched == 7 && (tag == 'S' || tag == 'L' || tag == 'M')) {
      has_query_hidden = 0;
      query_hidden = 0;
    }
  }
  if (matched != 7 && matched != 9) {
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
  if (tag != 'S' && tag != 'L' && tag != 'M') {
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
  entry->has_query_hidden = has_query_hidden != 0;
  entry->query_hidden = query_hidden != 0;
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
    if (!entry.control && key != NULL && strcmp(entry.key, key) == 0) {
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
  if (manifest->latest_snapshot != NULL) {
    char *snapshot_path;

    snapshot_path =
        lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                  "snapshots", manifest->latest_snapshot);
    if (snapshot_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state snapshot path", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_state_scan_file(pouch, snapshot_path, key, current,
                                  max_version, error);
    lc_free_with_allocator(&pouch->allocator, snapshot_path);
    if (rc != LC_OK) {
      return rc;
    }
  }
  for (segment_id = 1UL; segment_id <= manifest->max_segment_id;
       ++segment_id) {
    char *segment_leaf;
    char *segment_path;

    if (segment_id <= manifest->latest_snapshot_segment_id) {
      continue;
    }
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

static int lc_pouch_state_decision_apply(
    lc_pouch *pouch, lc_pouch_state_decision **decisions,
    const lc_pouch_state_entry *entry, lc_error *error) {
  lc_pouch_state_decision *decision;
  char *staged_key;
  char *etag;
  char *decision_value;

  if (entry == NULL || !entry->control || entry->key == NULL ||
      entry->etag == NULL || entry->decision == NULL) {
    return LC_OK;
  }
  for (decision = *decisions; decision != NULL; decision = decision->next) {
    if (strcmp(decision->staged_key, entry->key) != 0) {
      continue;
    }
    if (entry->version < decision->version) {
      return LC_OK;
    }
    staged_key = lc_strdup_with_allocator(&pouch->allocator, entry->key);
    etag = lc_strdup_with_allocator(&pouch->allocator, entry->etag);
    decision_value =
        lc_strdup_with_allocator(&pouch->allocator, entry->decision);
    if (staged_key == NULL || etag == NULL || decision_value == NULL) {
      lc_free_with_allocator(&pouch->allocator, staged_key);
      lc_free_with_allocator(&pouch->allocator, etag);
      lc_free_with_allocator(&pouch->allocator, decision_value);
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to copy pouch state decision", NULL, NULL,
                          NULL);
    }
    lc_free_with_allocator(&pouch->allocator, decision->staged_key);
    lc_free_with_allocator(&pouch->allocator, decision->etag);
    lc_free_with_allocator(&pouch->allocator, decision->decision);
    decision->staged_key = staged_key;
    decision->etag = etag;
    decision->decision = decision_value;
    decision->version = entry->version;
    return LC_OK;
  }

  decision = (lc_pouch_state_decision *)lc_calloc_with_allocator(
      &pouch->allocator, 1U, sizeof(*decision));
  if (decision == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state decision", NULL, NULL,
                        NULL);
  }
  decision->staged_key =
      lc_strdup_with_allocator(&pouch->allocator, entry->key);
  decision->etag = lc_strdup_with_allocator(&pouch->allocator, entry->etag);
  decision->decision =
      lc_strdup_with_allocator(&pouch->allocator, entry->decision);
  if (decision->staged_key == NULL || decision->etag == NULL ||
      decision->decision == NULL) {
    lc_pouch_state_decision_cleanup(&pouch->allocator, decision);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to copy pouch state decision", NULL, NULL,
                        NULL);
  }
  decision->version = entry->version;
  decision->next = *decisions;
  *decisions = decision;
  return LC_OK;
}

static int lc_pouch_state_collect_decisions_file(
    lc_pouch *pouch, const char *segment_path,
    lc_pouch_state_decision **decisions, lc_error *error) {
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
    rc = lc_pouch_state_decision_apply(pouch, decisions, &entry, error);
    if (rc != LC_OK) {
      break;
    }
    lc_pouch_state_entry_cleanup(&pouch->allocator, &entry);
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &entry);
  if (fclose(fp) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state segment", strerror(errno),
                      NULL, NULL);
  }
  return rc;
}

static int lc_pouch_state_collect_decisions(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    lc_pouch_state_decision **decisions, lc_error *error) {
  unsigned long segment_id;
  int rc;

  *decisions = NULL;
  rc = LC_OK;
  for (segment_id = manifest->latest_snapshot_segment_id + 1UL;
       segment_id <= manifest->max_segment_id; ++segment_id) {
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
    rc = lc_pouch_state_collect_decisions_file(pouch, segment_path, decisions,
                                               error);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    if (rc != LC_OK) {
      break;
    }
  }
  return rc;
}

static int lc_pouch_state_cache_replay_file(
    lc_pouch *pouch, lc_pouch_state_cache_namespace *cache,
    const char *segment_path, lc_error *error) {
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
    rc = lc_pouch_state_cache_apply_entry(pouch, cache, &entry, error);
    if (rc != LC_OK) {
      break;
    }
    lc_pouch_state_entry_cleanup(&pouch->allocator, &entry);
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &entry);
  if (fclose(fp) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state segment", strerror(errno),
                      NULL, NULL);
  }
  return rc;
}

static int lc_pouch_state_cache_refresh(
    lc_pouch *pouch, lc_pouch_state_cache_namespace *cache,
    const lc_pouch_namespace_manifest *manifest, int force, lc_error *error) {
  unsigned long segment_id;
  int rc;

  if (!force && cache->initialized &&
      cache->max_segment_id == manifest->max_segment_id) {
    return LC_OK;
  }
  lc_pouch_state_cache_records_cleanup(&pouch->allocator, cache->records);
  cache->records = NULL;
  cache->max_version = 0UL;
  cache->max_segment_id = 0UL;
  rc = LC_OK;
  if (manifest->latest_snapshot != NULL) {
    char *snapshot_path;

    snapshot_path =
        lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                  "snapshots", manifest->latest_snapshot);
    if (snapshot_path == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch state snapshot path", NULL,
                          NULL, NULL);
    }
    rc = lc_pouch_state_cache_replay_file(pouch, cache, snapshot_path, error);
    lc_free_with_allocator(&pouch->allocator, snapshot_path);
    if (rc != LC_OK) {
      return rc;
    }
  }
  for (segment_id = 1UL; segment_id <= manifest->max_segment_id;
       ++segment_id) {
    char *segment_leaf;
    char *segment_path;

    if (segment_id <= manifest->latest_snapshot_segment_id) {
      continue;
    }
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
    rc = lc_pouch_state_cache_replay_file(pouch, cache, segment_path, error);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    if (rc != LC_OK) {
      break;
    }
  }
  if (rc == LC_OK) {
    cache->max_segment_id = manifest->max_segment_id;
    cache->initialized = 1;
  }
  return rc;
}

static int lc_pouch_state_cache_lookup(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_namespace_manifest *manifest, const char *key,
    lc_pouch_state_entry *out, lc_error *error) {
  lc_pouch_state_cache_namespace *cache;
  lc_pouch_state_cache_record *record;
  int force_refresh;
  int rc;

  cache = lc_pouch_state_cache_namespace_find(pouch, namespace_name, 1, error);
  if (cache == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  force_refresh = 0;
  if (!pouch->single_writer) {
    rc = lc_pouch_namespace_marker_refresh_should_scan(
        &pouch->allocator, manifest->namespace_path, pouch->writer_marker_leaf,
        &cache->marker_refresh, LC_POUCH_STATE_SHARED_FORCE_AFTER_SKIPS,
        &force_refresh, error);
    if (rc != LC_OK) {
      return rc;
    }
  }
  rc = lc_pouch_state_cache_refresh(pouch, cache, manifest, force_refresh,
                                    error);
  if (rc != LC_OK) {
    return rc;
  }
  record = lc_pouch_state_cache_record_find(cache, key);
  return lc_pouch_state_entry_from_cache_record(pouch, record, out, error);
}

static int lc_pouch_state_snapshot_write_record(
    lc_pouch *pouch, int fd, const lc_pouch_state_cache_record *record,
    lc_error *error) {
  char *key_hex;
  char *content_type_hex;
  char *etag_hex;
  char line[LC_POUCH_STATE_LINE_LIMIT];
  int len;
  int rc;

  key_hex = lc_pouch_state_hex_encode(&pouch->allocator, record->key);
  etag_hex = lc_pouch_state_hex_encode(&pouch->allocator, record->etag);
  content_type_hex =
      record->found
          ? lc_pouch_state_hex_encode(&pouch->allocator, record->content_type)
          : NULL;
  if (key_hex == NULL || etag_hex == NULL ||
      (record->found && content_type_hex == NULL)) {
    lc_free_with_allocator(&pouch->allocator, key_hex);
    lc_free_with_allocator(&pouch->allocator, etag_hex);
    lc_free_with_allocator(&pouch->allocator, content_type_hex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to encode pouch state snapshot record", NULL,
                        NULL, NULL);
  }
  if (record->found) {
    len = snprintf(line, sizeof(line), "S %lu %lu %s %s %s %s %d %d\n",
                   record->version, record->bytes, key_hex, content_type_hex,
                   etag_hex, record->payload_leaf,
                   record->has_query_hidden ? 1 : 0,
                   record->query_hidden ? 1 : 0);
  } else {
    len = snprintf(line, sizeof(line), "D %lu %s %s\n", record->version,
                   key_hex, etag_hex);
  }
  lc_free_with_allocator(&pouch->allocator, key_hex);
  lc_free_with_allocator(&pouch->allocator, etag_hex);
  lc_free_with_allocator(&pouch->allocator, content_type_hex);
  if (len < 0 || (size_t)len >= sizeof(line)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state snapshot record exceeds line limit", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_state_write_all(fd, line, (size_t)len, error);
  return rc;
}

static int lc_pouch_state_write_snapshot(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    const char *snapshot_leaf, lc_pouch_state_cache_namespace *cache,
    lc_error *error) {
  lc_pouch_state_cache_record *record;
  char tmp_leaf[128];
  char *snapshot_path;
  char *tmp_path;
  int fd;
  int rc;
  int len;

  len = snprintf(tmp_leaf, sizeof(tmp_leaf), "%s.tmp", snapshot_leaf);
  if (len < 0 || (size_t)len >= sizeof(tmp_leaf)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state snapshot temp path exceeds limit", NULL,
                        NULL, NULL);
  }
  snapshot_path =
      lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                "snapshots", snapshot_leaf);
  tmp_path = snapshot_path != NULL
                 ? lc_pouch_state_child_path(&pouch->allocator,
                                             manifest->namespace_path,
                                             "snapshots", tmp_leaf)
                 : NULL;
  if (snapshot_path == NULL || tmp_path == NULL) {
    lc_free_with_allocator(&pouch->allocator, snapshot_path);
    lc_free_with_allocator(&pouch->allocator, tmp_path);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state snapshot path", NULL,
                        NULL, NULL);
  }
  fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if (fd < 0) {
    lc_free_with_allocator(&pouch->allocator, snapshot_path);
    lc_free_with_allocator(&pouch->allocator, tmp_path);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to create pouch state snapshot",
                        strerror(errno), NULL, NULL);
  }
  rc = LC_OK;
  for (record = cache->records; record != NULL; record = record->next) {
    rc = lc_pouch_state_snapshot_write_record(pouch, fd, record, error);
    if (rc != LC_OK) {
      break;
    }
  }
  if (rc == LC_OK && fsync(fd) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to fsync pouch state snapshot", strerror(errno),
                      NULL, NULL);
  }
  if (close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch state snapshot", strerror(errno),
                      NULL, NULL);
  }
  if (rc == LC_OK && rename(tmp_path, snapshot_path) != 0) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to install pouch state snapshot", strerror(errno),
                      NULL, NULL);
  }
  if (rc != LC_OK) {
    unlink(tmp_path);
  }
  lc_free_with_allocator(&pouch->allocator, snapshot_path);
  lc_free_with_allocator(&pouch->allocator, tmp_path);
  return rc;
}

static int lc_pouch_state_compaction_candidate_bytes(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    unsigned long *bytes, lc_error *error) {
  unsigned long segment_id;

  *bytes = 0UL;
  for (segment_id = manifest->latest_snapshot_segment_id + 1UL;
       segment_id <= manifest->max_segment_id; ++segment_id) {
    char *segment_leaf;
    char *segment_path;
    unsigned long size;
    int rc;

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
    rc = lc_pouch_state_file_size(segment_path, &size, error);
    lc_free_with_allocator(&pouch->allocator, segment_path);
    if (rc != LC_OK) {
      return rc;
    }
    *bytes += size;
  }
  return LC_OK;
}

static void lc_pouch_state_delete_compacted_files(
    lc_pouch *pouch, const lc_pouch_namespace_manifest *manifest,
    unsigned long compacted_segment_id, const char *old_snapshot) {
  unsigned long segment_id;

  for (segment_id = 1UL; segment_id <= compacted_segment_id; ++segment_id) {
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
    if (segment_path != NULL) {
      (void)unlink(segment_path);
      lc_free_with_allocator(&pouch->allocator, segment_path);
    }
  }
  if (old_snapshot != NULL &&
      (manifest->latest_snapshot == NULL ||
       strcmp(old_snapshot, manifest->latest_snapshot) != 0)) {
    char *snapshot_path;

    snapshot_path =
        lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                  "snapshots", old_snapshot);
    if (snapshot_path != NULL) {
      (void)unlink(snapshot_path);
      lc_free_with_allocator(&pouch->allocator, snapshot_path);
    }
  }
}

static int lc_pouch_state_compact_namespace(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_namespace_manifest *manifest, lc_error *error) {
  lc_pouch_state_cache_namespace snapshot_cache;
  char *snapshot_leaf;
  char *old_snapshot;
  unsigned long compacted_segment_id;
  int rc;

  if (manifest->max_segment_id <= manifest->latest_snapshot_segment_id) {
    return LC_OK;
  }
  memset(&snapshot_cache, 0, sizeof(snapshot_cache));
  rc = lc_pouch_state_cache_refresh(pouch, &snapshot_cache, manifest, 1,
                                    error);
  if (rc != LC_OK) {
    lc_pouch_state_cache_records_cleanup(&pouch->allocator,
                                         snapshot_cache.records);
    return rc;
  }
  compacted_segment_id = manifest->max_segment_id;
  snapshot_leaf =
      lc_pouch_namespace_snapshot_leaf(&pouch->allocator, compacted_segment_id);
  old_snapshot =
      manifest->latest_snapshot != NULL
          ? lc_strdup_with_allocator(&pouch->allocator, manifest->latest_snapshot)
          : NULL;
  if (snapshot_leaf == NULL ||
      (manifest->latest_snapshot != NULL && old_snapshot == NULL)) {
    lc_free_with_allocator(&pouch->allocator, snapshot_leaf);
    lc_free_with_allocator(&pouch->allocator, old_snapshot);
    lc_pouch_state_cache_records_cleanup(&pouch->allocator,
                                         snapshot_cache.records);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch state snapshot name", NULL,
                        NULL, NULL);
  }
  rc = lc_pouch_state_write_snapshot(pouch, manifest, snapshot_leaf,
                                     &snapshot_cache, error);
  if (rc == LC_OK) {
    rc = lc_pouch_namespace_manifest_install_snapshot(
        &pouch->allocator, namespace_name, manifest, snapshot_leaf,
        compacted_segment_id, compacted_segment_id + 1UL, error);
  }
  if (rc == LC_OK) {
    lc_pouch_state_delete_compacted_files(pouch, manifest,
                                          compacted_segment_id, old_snapshot);
    (void)lc_pouch_state_touch_marker(pouch, manifest, error);
  } else {
    char *snapshot_path;

    snapshot_path =
        lc_pouch_state_child_path(&pouch->allocator, manifest->namespace_path,
                                  "snapshots", snapshot_leaf);
    if (snapshot_path != NULL) {
      (void)unlink(snapshot_path);
      lc_free_with_allocator(&pouch->allocator, snapshot_path);
    }
  }
  lc_free_with_allocator(&pouch->allocator, snapshot_leaf);
  lc_free_with_allocator(&pouch->allocator, old_snapshot);
  lc_pouch_state_cache_records_cleanup(&pouch->allocator,
                                       snapshot_cache.records);
  return rc;
}

static void lc_pouch_state_maybe_compact(lc_pouch *pouch,
                                         const char *namespace_name,
                                         lc_pouch_namespace_manifest *manifest) {
  lc_error ignored;
  unsigned long candidate_count;
  unsigned long candidate_bytes;
  int rc;

  if (!pouch->background_compaction_enabled) {
    return;
  }
  if (manifest->max_segment_id <= manifest->latest_snapshot_segment_id) {
    return;
  }
  candidate_count =
      manifest->max_segment_id - manifest->latest_snapshot_segment_id;
  if (candidate_count < pouch->compaction_min_segment_count) {
    return;
  }
  lc_error_init(&ignored);
  rc = lc_pouch_state_compaction_candidate_bytes(pouch, manifest,
                                                 &candidate_bytes, &ignored);
  if (rc == LC_OK &&
      candidate_bytes >= pouch->compaction_min_reclaimable_bytes) {
    (void)lc_pouch_state_compact_namespace(pouch, namespace_name, manifest,
                                           &ignored);
  }
  lc_error_cleanup(&ignored);
}

static int lc_pouch_state_cache_apply_write(
    lc_pouch *pouch, const char *namespace_name,
    const lc_pouch_namespace_manifest *manifest, const char *key,
    const char *content_type, const char *etag, const char *payload_leaf,
    unsigned long version, unsigned long bytes, int has_query_hidden,
    int query_hidden, int found) {
  lc_pouch_state_cache_namespace *cache;
  lc_pouch_state_entry entry;
  lc_error ignored;
  int rc;

  cache = lc_pouch_state_cache_namespace_find(pouch, namespace_name, 0, NULL);
  if (cache == NULL || !cache->initialized) {
    return LC_OK;
  }
  memset(&entry, 0, sizeof(entry));
  entry.key = (char *)key;
  entry.content_type = (char *)content_type;
  entry.etag = (char *)etag;
  entry.payload_leaf = (char *)payload_leaf;
  entry.version = version;
  entry.bytes = bytes;
  entry.has_query_hidden = has_query_hidden;
  entry.query_hidden = query_hidden;
  entry.seen = 1;
  entry.found = found;
  lc_error_init(&ignored);
  rc = lc_pouch_state_cache_apply_entry(pouch, cache, &entry, &ignored);
  lc_error_cleanup(&ignored);
  if (rc != LC_OK) {
    lc_pouch_state_cache_records_cleanup(&pouch->allocator, cache->records);
    cache->records = NULL;
    cache->initialized = 0;
    cache->max_segment_id = 0UL;
    cache->max_version = 0UL;
    return LC_OK;
  }
  if (rc == LC_OK && manifest->max_segment_id > cache->max_segment_id) {
    cache->max_segment_id = manifest->max_segment_id;
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

static int lc_pouch_state_append_decision(
    lc_pouch *pouch, const char *namespace_name,
    lc_pouch_namespace_manifest *manifest, const char *staged_key,
    const char *etag, const char *decision, unsigned long version,
    lc_error *error) {
  char *segment_path;
  char *key_hex;
  char *etag_hex;
  char *decision_hex;
  char record[LC_POUCH_STATE_LINE_LIMIT];
  int fd;
  int rc;
  int len;
  unsigned long segment_size;

  key_hex = lc_pouch_state_hex_encode(&pouch->allocator, staged_key);
  etag_hex = lc_pouch_state_hex_encode(&pouch->allocator, etag);
  decision_hex = lc_pouch_state_hex_encode(&pouch->allocator, decision);
  if (key_hex == NULL || etag_hex == NULL || decision_hex == NULL) {
    lc_free_with_allocator(&pouch->allocator, key_hex);
    lc_free_with_allocator(&pouch->allocator, etag_hex);
    lc_free_with_allocator(&pouch->allocator, decision_hex);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to encode pouch state decision record", NULL,
                        NULL, NULL);
  }
  len = snprintf(record, sizeof(record), "T %lu %s %s %s\n", version, key_hex,
                 etag_hex, decision_hex);
  lc_free_with_allocator(&pouch->allocator, key_hex);
  lc_free_with_allocator(&pouch->allocator, etag_hex);
  lc_free_with_allocator(&pouch->allocator, decision_hex);
  if (len < 0 || (size_t)len >= sizeof(record)) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch state decision record exceeds line limit",
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
                                        int has_query_hidden,
                                        int query_hidden,
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
  len = snprintf(record, sizeof(record), "%c %lu %lu %s %s %s %s %d %d\n",
                 record_tag, version, bytes, key_hex, content_type_hex,
                 etag_hex, payload_leaf, has_query_hidden ? 1 : 0,
                 query_hidden ? 1 : 0);
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

int lc_pouch_state_recover_staged_decisions(lc_pouch *pouch,
                                            const char *namespace_name,
                                            lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  lc_pouch_state_cache_namespace *cache;
  lc_pouch_state_decision *decisions;
  lc_pouch_state_decision *decision;
  int recovered;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0') {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_recover_staged_decisions requires "
                        "pouch and namespace",
                        NULL, NULL, NULL);
  }
  cache = lc_pouch_state_cache_namespace_find(pouch, namespace_name, 1, error);
  if (cache == NULL) {
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  if (cache->decision_recovery_checked) {
    return LC_OK;
  }
  memset(&manifest, 0, sizeof(manifest));
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                        namespace_name, &manifest, error);
  if (rc != LC_OK) {
    return rc;
  }
  decisions = NULL;
  recovered = 0;
  rc = lc_pouch_state_collect_decisions(pouch, &manifest, &decisions, error);
  for (decision = decisions; rc == LC_OK && decision != NULL;
       decision = decision->next) {
    lc_pouch_state_entry staged;
    unsigned long max_version;
    unsigned long tombstone_version;

    memset(&staged, 0, sizeof(staged));
    rc = lc_pouch_state_scan(pouch, &manifest, decision->staged_key, &staged,
                             &max_version, error);
    if (rc != LC_OK) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &staged);
      break;
    }
    if (!staged.found) {
      lc_pouch_state_entry_cleanup(&pouch->allocator, &staged);
      continue;
    }
    tombstone_version = max_version + 1UL;
    rc = lc_pouch_state_append_tombstone(pouch, namespace_name, &manifest,
                                         decision->staged_key, decision->etag,
                                         tombstone_version, error);
    if (rc == LC_OK) {
      (void)lc_pouch_state_cache_apply_write(
          pouch, namespace_name, &manifest, decision->staged_key, NULL,
          decision->etag, NULL, tombstone_version, 0UL, 0, 0, 0);
      recovered = 1;
    }
    lc_pouch_state_entry_cleanup(&pouch->allocator, &staged);
  }
  if (rc == LC_OK && recovered) {
    rc = lc_pouch_state_touch_marker(pouch, &manifest, error);
  }
  if (rc == LC_OK && recovered) {
    lc_pouch_state_maybe_compact(pouch, namespace_name, &manifest);
  }
  if (rc == LC_OK) {
    cache->decision_recovery_checked = 1;
  }
  lc_pouch_state_decisions_cleanup(&pouch->allocator, decisions);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
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
  int has_query_hidden;
  int query_hidden;
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
  has_query_hidden = current.has_query_hidden;
  query_hidden = current.query_hidden;
  if (options != NULL && options->has_query_hidden) {
    has_query_hidden = 1;
    query_hidden = options->query_hidden;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_append_record(pouch, namespace_name, &manifest, 'S',
                                      key, content_type, etag, payload_leaf,
                                      version, bytes, has_query_hidden,
                                      query_hidden, error);
    if (rc == LC_OK) {
      record_appended = 1;
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_touch_marker(pouch, &manifest, error);
  }
  if (rc == LC_OK) {
    (void)lc_pouch_state_cache_apply_write(
        pouch, namespace_name, &manifest, key, content_type, etag,
        payload_leaf, version, bytes, has_query_hidden, query_hidden, 1);
    lc_pouch_state_maybe_compact(pouch, namespace_name, &manifest);
  }
  if (rc == LC_OK) {
    out->etag = etag;
    out->version = version;
    out->bytes = bytes;
    out->has_query_hidden = has_query_hidden;
    out->query_hidden = query_hidden;
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

int lc_pouch_state_update_metadata(
    lc_pouch *pouch, const char *namespace_name, const char *key,
    const lc_pouch_state_write_options *options,
    lc_pouch_state_write_result *out, lc_error *error) {
  lc_pouch_state_entry current;
  lc_pouch_namespace_manifest manifest;
  unsigned long max_version;
  unsigned long version;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || options == NULL || out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_update_metadata requires pouch, "
                        "namespace, key, options, and out",
                        NULL, NULL, NULL);
  }
  if (!options->has_query_hidden) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata update requires query_hidden", NULL,
                        NULL, NULL);
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
  if (!current.found) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata update requires existing state", NULL,
                        NULL, NULL);
  }
  if (options->has_expected_version &&
      current.version != options->expected_version) {
    lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch metadata version precondition failed", NULL,
                        NULL, NULL);
  }
  version = max_version + 1UL;
  rc = lc_pouch_state_append_record(
      pouch, namespace_name, &manifest, 'M', key, current.content_type,
      current.etag, current.payload_leaf, version, current.bytes, 1,
      options->query_hidden, error);
  if (rc == LC_OK) {
    rc = lc_pouch_state_touch_marker(pouch, &manifest, error);
  }
  if (rc == LC_OK) {
    (void)lc_pouch_state_cache_apply_write(
        pouch, namespace_name, &manifest, key, current.content_type,
        current.etag, current.payload_leaf, version, current.bytes, 1,
        options->query_hidden, 1);
    lc_pouch_state_maybe_compact(pouch, namespace_name, &manifest);
  }
  if (rc == LC_OK) {
    out->etag = lc_strdup_with_allocator(&pouch->allocator, current.etag);
    if (out->etag == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch metadata state etag", NULL,
                        NULL, NULL);
    }
  }
  if (rc == LC_OK) {
    out->version = version;
    out->bytes = current.bytes;
    out->has_query_hidden = 1;
    out->query_hidden = options->query_hidden;
  } else {
    lc_pouch_state_write_result_cleanup(&pouch->allocator, out);
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
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
    (void)lc_pouch_state_cache_apply_write(
        pouch, namespace_name, &manifest, key, NULL, etag, NULL, version, 0UL,
        0, 0, 0);
    lc_pouch_state_maybe_compact(pouch, namespace_name, &manifest);
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
  unsigned long decision_version;
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
      staged.etag, staged.payload_leaf, version, staged.bytes,
      staged.has_query_hidden, staged.query_hidden, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  decision_version = version + 1UL;
  rc = lc_pouch_state_append_decision(
      pouch, namespace_name, &manifest, staged_key, staged.etag,
      LC_POUCH_STATE_DECISION_COMMITTED, decision_version, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  discard_version = decision_version + 1UL;
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
  (void)lc_pouch_state_cache_apply_write(
      pouch, namespace_name, &manifest, key, staged.content_type, staged.etag,
      staged.payload_leaf, version, staged.bytes, staged.has_query_hidden,
      staged.query_hidden, 1);
  (void)lc_pouch_state_cache_apply_write(
      pouch, namespace_name, &manifest, staged_key, NULL, staged.etag, NULL,
      discard_version, 0UL, 0, 0, 0);
  out->etag = lc_strdup_with_allocator(&pouch->allocator, staged.etag);
  if (out->etag == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch staged promotion etag", NULL,
                      NULL, NULL);
    goto cleanup;
  }
  out->version = version;
  out->bytes = staged.bytes;
  lc_pouch_state_maybe_compact(pouch, namespace_name, &manifest);

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

int lc_pouch_state_commit_staged(lc_pouch *pouch, const char *namespace_name,
                                 const char *key, const char *txn_id,
                                 lc_pouch_state_write_result *out,
                                 lc_error *error) {
  lc_pouch_state_entry committed;
  lc_pouch_state_entry staged;
  lc_pouch_namespace_manifest manifest;
  char *staged_key;
  unsigned long committed_max_version;
  unsigned long staged_max_version;
  unsigned long version;
  unsigned long decision_version;
  unsigned long discard_version;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      key == NULL || key[0] == '\0' || txn_id == NULL || txn_id[0] == '\0' ||
      out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_commit_staged requires pouch, "
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
  if (!staged.found) {
    goto cleanup;
  }

  version = committed_max_version > staged_max_version ? committed_max_version
                                                       : staged_max_version;
  version++;
  rc = lc_pouch_state_append_record(
      pouch, namespace_name, &manifest, 'L', key, staged.content_type,
      staged.etag, staged.payload_leaf, version, staged.bytes,
      staged.has_query_hidden, staged.query_hidden, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  decision_version = version + 1UL;
  rc = lc_pouch_state_append_decision(
      pouch, namespace_name, &manifest, staged_key, staged.etag,
      LC_POUCH_STATE_DECISION_COMMITTED, decision_version, error);
  if (rc != LC_OK) {
    goto cleanup;
  }
  discard_version = decision_version + 1UL;
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
  (void)lc_pouch_state_cache_apply_write(
      pouch, namespace_name, &manifest, key, staged.content_type, staged.etag,
      staged.payload_leaf, version, staged.bytes, staged.has_query_hidden,
      staged.query_hidden, 1);
  (void)lc_pouch_state_cache_apply_write(
      pouch, namespace_name, &manifest, staged_key, NULL, staged.etag, NULL,
      discard_version, 0UL, 0, 0, 0);
  out->etag = lc_strdup_with_allocator(&pouch->allocator, staged.etag);
  if (out->etag == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch staged commit etag", NULL,
                      NULL, NULL);
    goto cleanup;
  }
  out->version = version;
  out->bytes = staged.bytes;
  out->has_query_hidden = staged.has_query_hidden;
  out->query_hidden = staged.query_hidden;
  lc_pouch_state_maybe_compact(pouch, namespace_name, &manifest);

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
  unsigned long decision_version;
  unsigned long tombstone_version;
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
    decision_version = max_version + 1UL;
    tombstone_version = decision_version + 1UL;
    etag = lc_pouch_state_etag(&pouch->allocator, tombstone_version);
    if (etag == NULL) {
      rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch staged discard etag", NULL,
                        NULL, NULL);
    } else {
      rc = lc_pouch_state_append_decision(
          pouch, namespace_name, &manifest, staged_key, etag,
          LC_POUCH_STATE_DECISION_DISCARDED, decision_version, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_state_append_tombstone(pouch, namespace_name, &manifest,
                                           staged_key, etag, tombstone_version,
                                           error);
      if (rc == LC_OK) {
        rc = lc_pouch_state_touch_marker(pouch, &manifest, error);
      }
      if (rc == LC_OK) {
        (void)lc_pouch_state_cache_apply_write(
            pouch, namespace_name, &manifest, staged_key, NULL, etag, NULL,
            tombstone_version, 0UL, 0, 0, 0);
        lc_pouch_state_maybe_compact(pouch, namespace_name, &manifest);
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
  rc = lc_pouch_state_cache_lookup(pouch, namespace_name, &manifest, key,
                                   &current, error);
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
    out->has_query_hidden = current.has_query_hidden;
    out->query_hidden = current.query_hidden;
    out->found = 1;
    current.content_type = NULL;
    current.etag = NULL;
  }
  lc_pouch_state_entry_cleanup(&pouch->allocator, &current);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

int lc_pouch_state_visit(lc_pouch *pouch, const char *namespace_name,
                         lc_pouch_state_visit_fn visitor, void *context,
                         lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  lc_pouch_state_cache_namespace *cache;
  lc_pouch_state_cache_record *record;
  lc_pouch_state_visit_snapshot *snapshots;
  size_t snapshot_count;
  size_t snapshot_capacity;
  size_t i;
  int force_refresh;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      visitor == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_visit requires pouch, namespace, and "
                        "visitor",
                        NULL, NULL, NULL);
  }
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
  snapshots = NULL;
  snapshot_count = 0U;
  snapshot_capacity = 0U;
  cache = lc_pouch_state_cache_namespace_find(pouch, namespace_name, 1, error);
  if (cache == NULL) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  force_refresh = 0;
  if (!pouch->single_writer) {
    rc = lc_pouch_namespace_marker_refresh_should_scan(
        &pouch->allocator, manifest.namespace_path, pouch->writer_marker_leaf,
        &cache->marker_refresh, LC_POUCH_STATE_SHARED_FORCE_AFTER_SKIPS,
        &force_refresh, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_cache_refresh(pouch, cache, &manifest, force_refresh,
                                      error);
  }
  for (record = rc == LC_OK ? cache->records : NULL; record != NULL;
       record = record->next) {
    if (!record->found) {
      continue;
    }
    rc = lc_pouch_state_visit_snapshot_append(
        &pouch->allocator, &snapshots, &snapshot_count, &snapshot_capacity,
        record, error);
    if (rc != LC_OK) {
      break;
    }
  }
  for (i = 0U; rc == LC_OK && i < snapshot_count; ++i) {
    lc_pouch_state_visit_entry entry;

    memset(&entry, 0, sizeof(entry));
    entry.key = snapshots[i].key;
    entry.content_type = snapshots[i].content_type;
    entry.etag = snapshots[i].etag;
    entry.version = snapshots[i].version;
    entry.bytes = snapshots[i].bytes;
    entry.has_query_hidden = snapshots[i].has_query_hidden;
    entry.query_hidden = snapshots[i].query_hidden;
    rc = visitor(&entry, context, error);
    if (rc != LC_OK) {
      break;
    }
  }
  lc_pouch_state_visit_snapshots_cleanup(&pouch->allocator, snapshots,
                                         snapshot_count);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

int lc_pouch_state_index_seq(lc_pouch *pouch, const char *namespace_name,
                             unsigned long *out, lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  lc_pouch_state_cache_namespace *cache;
  int force_refresh;
  int rc;

  if (pouch == NULL || namespace_name == NULL || namespace_name[0] == '\0' ||
      out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "lc_pouch_state_index_seq requires pouch, namespace, "
                        "and out",
                        NULL, NULL, NULL);
  }
  *out = 0UL;
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
  cache = lc_pouch_state_cache_namespace_find(pouch, namespace_name, 1, error);
  if (cache == NULL) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return error != NULL && error->code != LC_OK ? error->code : LC_ERR_NOMEM;
  }
  force_refresh = 0;
  if (!pouch->single_writer) {
    rc = lc_pouch_namespace_marker_refresh_should_scan(
        &pouch->allocator, manifest.namespace_path, pouch->writer_marker_leaf,
        &cache->marker_refresh, LC_POUCH_STATE_SHARED_FORCE_AFTER_SKIPS,
        &force_refresh, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_state_cache_refresh(pouch, cache, &manifest, force_refresh,
                                      error);
  }
  if (rc == LC_OK) {
    *out = cache->max_version;
  }
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

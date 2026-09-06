#include "lc_pouch.h"

#include "lc_api_internal.h"
#include "lc_pouch_internal.h"
#include "lc_pouch_namespace.h"
#include "lc_pouch_path.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define LC_POUCH_MIGRATION_TXN_NAMESPACE ".txns"
#define LC_POUCH_MIGRATION_LEASE_MAGIC_V1 "LPL1"
#define LC_POUCH_MIGRATION_LEASE_MAGIC_CURRENT "LPL2"
#define LC_POUCH_MIGRATION_TXN_MAGIC_V1 "LPT1"
#define LC_POUCH_MIGRATION_TXN_MAGIC_V2 "LPT2"
#define LC_POUCH_MIGRATION_TXN_MAGIC_CURRENT "LPT3"
#define LC_POUCH_MIGRATION_LEASE_CONTENT_TYPE "application/x-lockdc-pouch-lease"
#define LC_POUCH_MIGRATION_TXN_CONTENT_TYPE "application/x-lockdc-pouch-txn"
#define LC_POUCH_MIGRATION_MARKER_LEAF ".lockdc-control-migration-v1"
#define LC_POUCH_MIGRATION_MARKER_TEXT "control-record-layout=v1\n"

typedef struct lc_pouch_migration_buffer {
  unsigned char *bytes;
  size_t length;
  size_t capacity;
} lc_pouch_migration_buffer;

typedef struct lc_pouch_migration_cursor {
  const unsigned char *bytes;
  size_t length;
  size_t offset;
} lc_pouch_migration_cursor;

typedef struct lc_pouch_migration_lease {
  char *namespace_name;
  char *key;
  char *owner;
  char *lease_id;
  char *txn_id;
  long fencing_token;
  lc_pouch_unix_seconds expires_at_unix;
  int has_state_fields;
  lc_pouch_generation state_version;
  int txn_explicit;
} lc_pouch_migration_lease;

typedef struct lc_pouch_migration_participant {
  char *namespace_name;
  char *key;
  char *backend_hash;
  unsigned char vote;
} lc_pouch_migration_participant;

typedef struct lc_pouch_migration_txn {
  char *state;
  lc_pouch_unix_seconds expires_at_unix;
  uint64_t tc_term;
  char *target_backend_hash;
  lc_pouch_migration_participant *participants;
  size_t participant_count;
  size_t participant_capacity;
} lc_pouch_migration_txn;

typedef struct lc_pouch_migration_namespace {
  lc_pouch *pouch;
  const char *namespace_name;
  int legacy_control_error;
  int incomplete;
} lc_pouch_migration_namespace;

static void
lc_pouch_migration_buffer_cleanup(lc_pouch_migration_buffer *buffer) {
  if (buffer == NULL) {
    return;
  }
  lc_free_with_allocator(NULL, buffer->bytes);
  memset(buffer, 0, sizeof(*buffer));
}

static int lc_pouch_migration_buffer_append(lc_pouch_migration_buffer *buffer,
                                            const void *bytes, size_t length,
                                            lc_error *error) {
  unsigned char *next;
  size_t capacity;

  if (buffer == NULL || (bytes == NULL && length > 0U) ||
      length > SIZE_MAX - buffer->length) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch control migration record exceeds bounds", NULL,
                        NULL, "pouch");
  }
  if (buffer->length + length > buffer->capacity) {
    capacity = buffer->capacity == 0U ? 128U : buffer->capacity;
    while (capacity < buffer->length + length) {
      if (capacity > SIZE_MAX / 2U) {
        capacity = buffer->length + length;
        break;
      }
      capacity *= 2U;
    }
    next = (unsigned char *)lc_realloc_with_allocator(NULL, buffer->bytes,
                                                      capacity);
    if (next == NULL) {
      return lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to allocate pouch control migration record",
                          NULL, NULL, "pouch");
    }
    buffer->bytes = next;
    buffer->capacity = capacity;
  }
  if (length > 0U) {
    memcpy(buffer->bytes + buffer->length, bytes, length);
    buffer->length += length;
  }
  return LC_OK;
}

static int lc_pouch_migration_buffer_u16(lc_pouch_migration_buffer *buffer,
                                         uint16_t value, lc_error *error) {
  unsigned char bytes[2];

  bytes[0] = (unsigned char)(value & 0xffU);
  bytes[1] = (unsigned char)((value >> 8U) & 0xffU);
  return lc_pouch_migration_buffer_append(buffer, bytes, sizeof(bytes), error);
}

static int lc_pouch_migration_buffer_u64(lc_pouch_migration_buffer *buffer,
                                         uint64_t value, lc_error *error) {
  unsigned char bytes[8];
  size_t i;

  for (i = 0U; i < sizeof(bytes); ++i) {
    bytes[i] = (unsigned char)((value >> (i * 8U)) & 0xffU);
  }
  return lc_pouch_migration_buffer_append(buffer, bytes, sizeof(bytes), error);
}

static int lc_pouch_migration_buffer_string(lc_pouch_migration_buffer *buffer,
                                            const char *value,
                                            lc_error *error) {
  size_t length;
  int rc;

  if (value == NULL) {
    value = "";
  }
  length = strlen(value);
  if (length > 65535U) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch control migration string exceeds bounds", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_migration_buffer_u16(buffer, (uint16_t)length, error);
  if (rc == LC_OK) {
    rc = lc_pouch_migration_buffer_append(buffer, value, length, error);
  }
  return rc;
}

static int lc_pouch_migration_cursor_bytes(lc_pouch_migration_cursor *cursor,
                                           void *out, size_t length,
                                           lc_error *error) {
  if (cursor == NULL || out == NULL ||
      length > cursor->length - cursor->offset) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch control migration record is truncated", NULL,
                        NULL, "pouch");
  }
  memcpy(out, cursor->bytes + cursor->offset, length);
  cursor->offset += length;
  return LC_OK;
}

static int lc_pouch_migration_cursor_u16(lc_pouch_migration_cursor *cursor,
                                         uint16_t *out, lc_error *error) {
  unsigned char bytes[2];
  int rc;

  rc = lc_pouch_migration_cursor_bytes(cursor, bytes, sizeof(bytes), error);
  if (rc == LC_OK) {
    *out = (uint16_t)bytes[0] | ((uint16_t)bytes[1] << 8U);
  }
  return rc;
}

static int lc_pouch_migration_cursor_u64(lc_pouch_migration_cursor *cursor,
                                         uint64_t *out, lc_error *error) {
  unsigned char bytes[8];
  size_t i;
  int rc;

  rc = lc_pouch_migration_cursor_bytes(cursor, bytes, sizeof(bytes), error);
  if (rc == LC_OK) {
    *out = 0U;
    for (i = 0U; i < sizeof(bytes); ++i) {
      *out |= (uint64_t)bytes[i] << (i * 8U);
    }
  }
  return rc;
}

static int lc_pouch_migration_cursor_string(lc_pouch_migration_cursor *cursor,
                                            char **out, lc_error *error) {
  uint16_t length;
  char *value;
  int rc;

  if (out == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch control migration string requires output", NULL,
                        NULL, "pouch");
  }
  *out = NULL;
  rc = lc_pouch_migration_cursor_u16(cursor, &length, error);
  if (rc != LC_OK) {
    return rc;
  }
  if ((size_t)length > cursor->length - cursor->offset) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch control migration string is truncated", NULL,
                        NULL, "pouch");
  }
  value = (char *)lc_alloc_with_allocator(NULL, (size_t)length + 1U);
  if (value == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch control migration string",
                        NULL, NULL, "pouch");
  }
  memcpy(value, cursor->bytes + cursor->offset, length);
  value[length] = '\0';
  cursor->offset += length;
  *out = value;
  return LC_OK;
}

static void lc_pouch_migration_lease_cleanup(lc_pouch_migration_lease *lease) {
  if (lease == NULL) {
    return;
  }
  lc_free_with_allocator(NULL, lease->namespace_name);
  lc_free_with_allocator(NULL, lease->key);
  lc_free_with_allocator(NULL, lease->owner);
  lc_free_with_allocator(NULL, lease->lease_id);
  lc_free_with_allocator(NULL, lease->txn_id);
  memset(lease, 0, sizeof(*lease));
}

static int lc_pouch_migration_parse_lease_v1(const unsigned char *bytes,
                                             size_t length,
                                             lc_pouch_migration_lease *lease,
                                             lc_error *error) {
  lc_pouch_migration_cursor cursor;
  char magic[4];
  uint64_t value;
  int rc;

  if (bytes == NULL || lease == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch control migration lease is incomplete", NULL,
                        NULL, "pouch");
  }
  memset(lease, 0, sizeof(*lease));
  memset(&cursor, 0, sizeof(cursor));
  cursor.bytes = bytes;
  cursor.length = length;
  rc = lc_pouch_migration_cursor_bytes(&cursor, magic, sizeof(magic), error);
  if (rc == LC_OK &&
      memcmp(magic, LC_POUCH_MIGRATION_LEASE_MAGIC_V1, sizeof(magic)) != 0) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch control migration lease magic is invalid", NULL,
                      NULL, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_cursor_string(&cursor, &lease->namespace_name,
                                          error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_cursor_string(&cursor, &lease->key, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_cursor_string(&cursor, &lease->owner, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_cursor_string(&cursor, &lease->lease_id, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_cursor_string(&cursor, &lease->txn_id, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_cursor_u64(&cursor, &value, error);
  }
  if (rc == LC_OK) {
    lease->fencing_token = (long)(int64_t)value;
    if ((int64_t)lease->fencing_token != (int64_t)value) {
      rc =
          lc_error_set(error, LC_ERR_INVALID, 0L,
                       "pouch control migration lease fencing token is invalid",
                       NULL, NULL, "pouch");
    }
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_cursor_u64(&cursor, &value, error);
  }
  if (rc == LC_OK) {
    lease->expires_at_unix = (lc_pouch_unix_seconds)(int64_t)value;
  }
  /* Historical LPL1 writers used both the short and extended layouts. */
  if (rc == LC_OK && cursor.offset != cursor.length) {
    rc = lc_pouch_migration_cursor_u64(&cursor, &value, error);
    if (rc == LC_OK) {
      lease->state_version = (lc_pouch_generation)value;
      if (cursor.offset >= cursor.length || cursor.bytes[cursor.offset] > 1U) {
        rc = lc_error_set(
            error, LC_ERR_INVALID, 0L,
            "pouch control migration lease explicit flag is invalid", NULL,
            NULL, "pouch");
      } else {
        lease->txn_explicit = cursor.bytes[cursor.offset++];
        lease->has_state_fields = 1;
      }
    }
  }
  if (rc == LC_OK && cursor.offset != cursor.length) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch control migration lease has trailing bytes", NULL,
                      NULL, "pouch");
  }
  if (rc == LC_OK && (lease->namespace_name == NULL || lease->key == NULL ||
                      lease->owner == NULL || lease->lease_id == NULL ||
                      lease->txn_id == NULL)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch control migration lease is incomplete", NULL, NULL,
                      "pouch");
  }
  if (rc != LC_OK) {
    lc_pouch_migration_lease_cleanup(lease);
  }
  return rc;
}

static int lc_pouch_migration_build_lease_current(
    const lc_pouch_migration_lease *lease, lc_pouch_generation state_version,
    int txn_explicit, lc_pouch_migration_buffer *buffer, lc_error *error) {
  unsigned char explicit_flag;
  int rc;

  if (lease == NULL || buffer == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch control migration lease output is required",
                        NULL, NULL, "pouch");
  }
  explicit_flag = txn_explicit ? 1U : 0U;
  rc = lc_pouch_migration_buffer_append(
      buffer, LC_POUCH_MIGRATION_LEASE_MAGIC_CURRENT, 4U, error);
  if (rc == LC_OK) {
    rc = lc_pouch_migration_buffer_string(buffer, lease->namespace_name, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_buffer_string(buffer, lease->key, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_buffer_string(buffer, lease->owner, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_buffer_string(buffer, lease->lease_id, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_buffer_string(buffer, lease->txn_id, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_buffer_u64(
        buffer, (uint64_t)(int64_t)lease->fencing_token, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_buffer_u64(
        buffer, (uint64_t)(int64_t)lease->expires_at_unix, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_buffer_u64(buffer, state_version, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_buffer_append(buffer, &explicit_flag,
                                          sizeof(explicit_flag), error);
  }
  return rc;
}

static void lc_pouch_migration_txn_cleanup(lc_pouch_migration_txn *txn) {
  size_t i;

  if (txn == NULL) {
    return;
  }
  lc_free_with_allocator(NULL, txn->state);
  lc_free_with_allocator(NULL, txn->target_backend_hash);
  for (i = 0U; i < txn->participant_count; ++i) {
    lc_free_with_allocator(NULL, txn->participants[i].namespace_name);
    lc_free_with_allocator(NULL, txn->participants[i].key);
    lc_free_with_allocator(NULL, txn->participants[i].backend_hash);
  }
  lc_free_with_allocator(NULL, txn->participants);
  memset(txn, 0, sizeof(*txn));
}

static int lc_pouch_migration_txn_append_participant(
    lc_pouch_migration_txn *txn, char *namespace_name, char *key,
    char *backend_hash, unsigned char vote, lc_error *error) {
  lc_pouch_migration_participant *next;
  size_t capacity;

  if (txn->participant_count == txn->participant_capacity) {
    if (txn->participant_capacity > SIZE_MAX / 2U) {
      return lc_error_set(
          error, LC_ERR_INVALID, 0L,
          "pouch control migration participant count is invalid", NULL, NULL,
          "pouch");
    }
    capacity =
        txn->participant_capacity == 0U ? 4U : txn->participant_capacity * 2U;
    if (capacity > SIZE_MAX / sizeof(*next)) {
      return lc_error_set(
          error, LC_ERR_INVALID, 0L,
          "pouch control migration participant count is invalid", NULL, NULL,
          "pouch");
    }
    next = (lc_pouch_migration_participant *)lc_realloc_with_allocator(
        NULL, txn->participants, capacity * sizeof(*next));
    if (next == NULL) {
      return lc_error_set(
          error, LC_ERR_NOMEM, 0L,
          "failed to allocate pouch control migration participants", NULL, NULL,
          "pouch");
    }
    txn->participants = next;
    txn->participant_capacity = capacity;
  }
  txn->participants[txn->participant_count].namespace_name = namespace_name;
  txn->participants[txn->participant_count].key = key;
  txn->participants[txn->participant_count].backend_hash = backend_hash;
  txn->participants[txn->participant_count].vote = vote;
  txn->participant_count += 1U;
  return LC_OK;
}

static int lc_pouch_migration_parse_txn_legacy(const unsigned char *bytes,
                                               size_t length, int has_votes,
                                               lc_pouch_migration_txn *txn,
                                               lc_error *error) {
  lc_pouch_migration_cursor cursor;
  char magic[4];
  uint64_t count;
  uint64_t value;
  int rc;

  if (bytes == NULL || txn == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch control migration transaction is incomplete",
                        NULL, NULL, "pouch");
  }
  memset(txn, 0, sizeof(*txn));
  memset(&cursor, 0, sizeof(cursor));
  cursor.bytes = bytes;
  cursor.length = length;
  rc = lc_pouch_migration_cursor_bytes(&cursor, magic, sizeof(magic), error);
  if (rc == LC_OK && memcmp(magic,
                            has_votes ? LC_POUCH_MIGRATION_TXN_MAGIC_V2
                                      : LC_POUCH_MIGRATION_TXN_MAGIC_V1,
                            sizeof(magic)) != 0) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch control migration transaction magic is invalid",
                      NULL, NULL, "pouch");
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_cursor_string(&cursor, &txn->state, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_cursor_u64(&cursor, &value, error);
  }
  if (rc == LC_OK) {
    txn->expires_at_unix = (lc_pouch_unix_seconds)(int64_t)value;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_cursor_u64(&cursor, &txn->tc_term, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_cursor_string(&cursor, &txn->target_backend_hash,
                                          error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_cursor_u64(&cursor, &count, error);
  }
  if (rc == LC_OK) {
    if (count >
        (uint64_t)((cursor.length - cursor.offset) / (has_votes ? 7U : 6U))) {
      rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch control migration participant count is invalid",
                        NULL, NULL, "pouch");
    }
  }
  while (rc == LC_OK && count > 0U) {
    char *namespace_name = NULL;
    char *key = NULL;
    char *backend_hash = NULL;
    unsigned char vote = 0U;

    rc = lc_pouch_migration_cursor_string(&cursor, &namespace_name, error);
    if (rc == LC_OK) {
      rc = lc_pouch_migration_cursor_string(&cursor, &key, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_migration_cursor_string(&cursor, &backend_hash, error);
    }
    if (rc == LC_OK && has_votes) {
      rc = lc_pouch_migration_cursor_bytes(&cursor, &vote, sizeof(vote), error);
      if (rc == LC_OK && vote > 2U) {
        rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                          "pouch control migration transaction vote is invalid",
                          NULL, NULL, "pouch");
      }
    }
    if (rc == LC_OK) {
      rc = lc_pouch_migration_txn_append_participant(txn, namespace_name, key,
                                                     backend_hash, vote, error);
      if (rc == LC_OK) {
        namespace_name = NULL;
        key = NULL;
        backend_hash = NULL;
      }
    }
    lc_free_with_allocator(NULL, namespace_name);
    lc_free_with_allocator(NULL, key);
    lc_free_with_allocator(NULL, backend_hash);
    count -= 1U;
  }
  if (rc == LC_OK && cursor.offset != cursor.length) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch control migration transaction has trailing bytes",
                      NULL, NULL, "pouch");
  }
  if (rc == LC_OK && txn->state == NULL) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch control migration transaction is incomplete", NULL,
                      NULL, "pouch");
  }
  if (rc != LC_OK) {
    lc_pouch_migration_txn_cleanup(txn);
  }
  return rc;
}

static unsigned char lc_pouch_migration_v1_vote(const char *state) {
  if (state != NULL && strcmp(state, "commit") == 0) {
    return 1U;
  }
  if (state != NULL && strcmp(state, "rollback") == 0) {
    return 2U;
  }
  return 0U;
}

static int lc_pouch_migration_build_txn_current(
    const lc_pouch_migration_txn *txn, int source_has_votes,
    lc_pouch_migration_buffer *buffer, lc_error *error) {
  size_t i;
  int rc;

  if (txn == NULL || buffer == NULL || txn->state == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch control migration transaction output is incomplete", NULL, NULL,
        "pouch");
  }
  rc = lc_pouch_migration_buffer_append(
      buffer, LC_POUCH_MIGRATION_TXN_MAGIC_CURRENT, 4U, error);
  if (rc == LC_OK) {
    rc = lc_pouch_migration_buffer_string(buffer, txn->state, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_buffer_u64(
        buffer, (uint64_t)(int64_t)txn->expires_at_unix, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_buffer_u64(buffer, txn->tc_term, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_buffer_string(buffer, txn->target_backend_hash,
                                          error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_buffer_u64(buffer, txn->participant_count, error);
  }
  for (i = 0U; rc == LC_OK && i < txn->participant_count; ++i) {
    unsigned char vote = source_has_votes
                             ? txn->participants[i].vote
                             : lc_pouch_migration_v1_vote(txn->state);

    rc = lc_pouch_migration_buffer_string(
        buffer, txn->participants[i].namespace_name, error);
    if (rc == LC_OK) {
      rc = lc_pouch_migration_buffer_string(buffer, txn->participants[i].key,
                                            error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_migration_buffer_string(
          buffer, txn->participants[i].backend_hash, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_migration_buffer_append(buffer, &vote, sizeof(vote), error);
    }
  }
  return rc;
}

static int lc_pouch_migration_read_prefix(lc_source *body,
                                          unsigned char prefix[4],
                                          size_t *prefix_length,
                                          lc_error *error) {
  lc_error read_error;
  size_t read_count;
  int rc;

  if (body == NULL || prefix == NULL || prefix_length == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch control migration transaction has no body", NULL,
                        NULL, "pouch");
  }
  *prefix_length = 0U;
  lc_error_init(&read_error);
  rc = LC_OK;
  while (rc == LC_OK && *prefix_length < 4U) {
    read_count = body->read(body, prefix + *prefix_length, 4U - *prefix_length,
                            &read_error);
    if (read_count > 4U - *prefix_length) {
      rc = lc_error_set(error, LC_ERR_PROTOCOL, 0L,
                        "pouch control migration source exceeded read limit",
                        NULL, NULL, "pouch");
      break;
    }
    *prefix_length += read_count;
    if (read_error.code != LC_OK) {
      rc = read_error.code;
      if (error != NULL) {
        lc_error_cleanup(error);
        *error = read_error;
        lc_error_init(&read_error);
      }
      break;
    }
    if (read_count == 0U) {
      break;
    }
  }
  lc_error_cleanup(&read_error);
  return rc;
}

static int lc_pouch_migration_read_body(lc_source *body,
                                        lc_pouch_migration_buffer *buffer,
                                        lc_error *error) {
  char chunk[1024];
  lc_error read_error;
  size_t read_count;
  int rc;

  if (body == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch control migration transaction has no body", NULL,
                        NULL, "pouch");
  }
  lc_error_init(&read_error);
  rc = LC_OK;
  while (rc == LC_OK) {
    read_count = body->read(body, chunk, sizeof(chunk), &read_error);
    if (read_error.code != LC_OK) {
      rc = read_error.code;
      if (error != NULL) {
        lc_error_cleanup(error);
        *error = read_error;
        lc_error_init(&read_error);
      }
      break;
    }
    if (read_count == 0U) {
      break;
    }
    rc = lc_pouch_migration_buffer_append(buffer, chunk, read_count, error);
  }
  lc_error_cleanup(&read_error);
  return rc;
}

static int lc_pouch_migration_txn_visit(const lc_pouch_state_visit_entry *entry,
                                        void *context, lc_error *error) {
  lc_pouch_migration_namespace *migration;
  lc_pouch_state_read_result result;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result write_result;
  lc_pouch_migration_buffer input;
  lc_pouch_migration_buffer output;
  lc_pouch_migration_txn txn;
  lc_source *source;
  unsigned char magic[4];
  int legacy_candidate;
  size_t magic_length;
  int source_has_votes;
  int rc;

  migration = (lc_pouch_migration_namespace *)context;
  if (migration == NULL || entry == NULL || entry->key == NULL ||
      !entry->has_payload) {
    return LC_OK;
  }
  memset(&result, 0, sizeof(result));
  memset(&options, 0, sizeof(options));
  memset(&write_result, 0, sizeof(write_result));
  memset(&input, 0, sizeof(input));
  memset(&output, 0, sizeof(output));
  memset(&txn, 0, sizeof(txn));
  memset(magic, 0, sizeof(magic));
  source = NULL;
  legacy_candidate = 0;
  magic_length = 0U;
  source_has_votes = 0;
  rc = lc_pouch_state_read(migration->pouch, migration->namespace_name,
                           entry->key, &result, error);
  if (rc == LC_OK && !result.found) {
    rc = LC_OK;
  }
  if (rc == LC_OK && result.found) {
    /* Current LPT3 records are already runtime-readable; do not materialize
     * their potentially large bodies during the initialization-only scan. */
    rc = lc_pouch_migration_read_prefix(result.body, magic, &magic_length,
                                        error);
  }
  if (magic_length == 4U &&
      memcmp(magic, LC_POUCH_MIGRATION_TXN_MAGIC_V1, 4U) == 0) {
    legacy_candidate = 1;
    if (rc == LC_OK) {
      rc =
          lc_pouch_migration_buffer_append(&input, magic, sizeof(magic), error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_migration_read_body(result.body, &input, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_migration_parse_txn_legacy(input.bytes, input.length, 0,
                                               &txn, error);
    }
  } else if (magic_length == 4U &&
             memcmp(magic, LC_POUCH_MIGRATION_TXN_MAGIC_V2, 4U) == 0) {
    legacy_candidate = 1;
    source_has_votes = 1;
    if (rc == LC_OK) {
      rc =
          lc_pouch_migration_buffer_append(&input, magic, sizeof(magic), error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_migration_read_body(result.body, &input, error);
    }
    if (rc == LC_OK) {
      rc = lc_pouch_migration_parse_txn_legacy(input.bytes, input.length, 1,
                                               &txn, error);
    }
  } else {
    goto cleanup;
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_build_txn_current(&txn, source_has_votes, &output,
                                              error);
  }
  if (rc == LC_OK) {
    rc = lc_source_from_memory(output.bytes, output.length, &source, error);
  }
  if (rc == LC_OK) {
    options.content_type = result.content_type != NULL
                               ? result.content_type
                               : LC_POUCH_MIGRATION_TXN_CONTENT_TYPE;
    options.object_record = 1;
    options.suppress_query_index = 1;
    options.has_expected_version = 1;
    options.expected_version = result.version;
    rc = lc_pouch_state_write(migration->pouch, migration->namespace_name,
                              entry->key, source, &options, &write_result,
                              error);
  }

cleanup:
  if (legacy_candidate && rc != LC_OK) {
    migration->legacy_control_error = 1;
  }
  lc_source_close(source);
  lc_pouch_migration_txn_cleanup(&txn);
  lc_pouch_migration_buffer_cleanup(&input);
  lc_pouch_migration_buffer_cleanup(&output);
  lc_pouch_state_read_result_cleanup(&migration->pouch->allocator, &result);
  lc_pouch_state_write_result_cleanup(&migration->pouch->allocator,
                                      &write_result);
  return rc;
}

static int lc_pouch_migration_txn_namespace_exists(lc_pouch *pouch, int *exists,
                                                   lc_error *error) {
  char *path;
  struct stat st;

  if (pouch == NULL || exists == NULL) {
    return lc_error_set(
        error, LC_ERR_INVALID, 0L,
        "pouch control migration transaction namespace requires output", NULL,
        NULL, "pouch");
  }
  *exists = 0;
  path = lc_pouch_namespace_path(&pouch->allocator, pouch->root_path,
                                 LC_POUCH_MIGRATION_TXN_NAMESPACE);
  if (path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch transaction namespace path",
                        NULL, NULL, "pouch");
  }
  if (stat(path, &st) == 0) {
    *exists = S_ISDIR(st.st_mode) ? 1 : 0;
  } else if (errno != ENOENT) {
    lc_free_with_allocator(&pouch->allocator, path);
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to inspect pouch transaction namespace",
                        strerror(errno), NULL, "pouch");
  }
  lc_free_with_allocator(&pouch->allocator, path);
  return LC_OK;
}

static int lc_pouch_migration_legacy_lease_is_explicit(lc_pouch *pouch,
                                                       const char *txn_id,
                                                       int *explicit_txn,
                                                       lc_error *error) {
  lc_pouch_state_read_result result;
  int txn_namespace_exists = 0;
  int rc;

  if (explicit_txn == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch control migration lease requires output", NULL,
                        NULL, "pouch");
  }
  *explicit_txn = 0;
  if (txn_id == NULL || txn_id[0] == '\0') {
    return LC_OK;
  }
  rc = lc_pouch_migration_txn_namespace_exists(pouch, &txn_namespace_exists,
                                               error);
  if (rc != LC_OK || !txn_namespace_exists) {
    return rc;
  }
  memset(&result, 0, sizeof(result));
  rc = lc_pouch_state_read(pouch, LC_POUCH_MIGRATION_TXN_NAMESPACE, txn_id,
                           &result, error);
  if (rc == LC_OK) {
    *explicit_txn = result.found;
  }
  lc_pouch_state_read_result_cleanup(&pouch->allocator, &result);
  return rc;
}

static int
lc_pouch_migration_lease_visit(const lc_pouch_state_visit_entry *entry,
                               void *context, lc_error *error) {
  lc_pouch_migration_namespace *migration;
  lc_pouch_migration_lease lease;
  lc_pouch_migration_buffer output;
  lc_pouch_state_write_options options;
  lc_pouch_state_write_result write_result;
  int txn_explicit;
  int rc;

  migration = (lc_pouch_migration_namespace *)context;
  if (migration == NULL || entry == NULL || entry->key == NULL ||
      entry->metadata_length < 4U || entry->metadata == NULL ||
      memcmp(entry->metadata, LC_POUCH_MIGRATION_LEASE_MAGIC_V1, 4U) != 0) {
    return LC_OK;
  }
  memset(&lease, 0, sizeof(lease));
  memset(&output, 0, sizeof(output));
  memset(&options, 0, sizeof(options));
  memset(&write_result, 0, sizeof(write_result));
  txn_explicit = 0;
  rc = lc_pouch_migration_parse_lease_v1(entry->metadata,
                                         entry->metadata_length, &lease, error);
  if (rc == LC_OK &&
      (strcmp(lease.namespace_name, migration->namespace_name) != 0 ||
       strcmp(lease.key, entry->key) != 0)) {
    rc = lc_error_set(error, LC_ERR_INVALID, 0L,
                      "pouch control migration lease identity is invalid", NULL,
                      NULL, "pouch");
  }
  if (rc == LC_OK && !lease.has_state_fields) {
    rc = lc_pouch_migration_legacy_lease_is_explicit(
        migration->pouch, lease.txn_id, &txn_explicit, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_build_lease_current(
        &lease,
        lease.has_state_fields ? lease.state_version
                               : (entry->has_payload ? entry->version : 0UL),
        lease.has_state_fields ? lease.txn_explicit : txn_explicit, &output,
        error);
  }
  if (rc == LC_OK) {
    options.content_type = entry->content_type != NULL
                               ? entry->content_type
                               : LC_POUCH_MIGRATION_LEASE_CONTENT_TYPE;
    options.object_record = entry->object_record;
    options.suppress_query_index = 1;
    options.has_expected_version = 1;
    options.expected_version = entry->version;
    options.has_metadata = 1;
    options.metadata = output.bytes;
    options.metadata_length = output.length;
    options.has_query_hidden = 1;
    options.query_hidden = entry->query_hidden;
    rc = lc_pouch_state_update_metadata(migration->pouch,
                                        migration->namespace_name, entry->key,
                                        &options, &write_result, error);
  }
  if (rc != LC_OK) {
    migration->legacy_control_error = 1;
  }
  lc_pouch_migration_lease_cleanup(&lease);
  lc_pouch_migration_buffer_cleanup(&output);
  lc_pouch_state_write_result_cleanup(&migration->pouch->allocator,
                                      &write_result);
  return rc;
}

static int lc_pouch_migration_defer_unrelated_protocol_error(
    lc_pouch_migration_namespace *migration, int rc, lc_error *error) {
  if (rc != LC_ERR_PROTOCOL || migration->legacy_control_error) {
    return rc;
  }
  migration->incomplete = 1;
  if (error != NULL) {
    lc_error_cleanup(error);
    lc_error_init(error);
  }
  return LC_OK;
}

static int lc_pouch_migration_sync_file(lc_pouch *pouch, const char *directory,
                                        const char *leaf, lc_error *error) {
  char *path;
  int fd;
  int rc;

  path = lc_pouch_path_join(&pouch->allocator, directory, leaf);
  if (path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch migration sync path", NULL,
                        NULL, "pouch");
  }
  fd = open(path, O_RDWR);
  lc_free_with_allocator(&pouch->allocator, path);
  if (fd < 0) {
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to open pouch migration sync file",
                        strerror(errno), NULL, "pouch");
  }
  rc = lc_pouch_fsync_commit(pouch, fd, error);
  if (close(fd) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch migration sync file",
                      strerror(errno), NULL, "pouch");
  }
  return rc;
}

static int lc_pouch_migration_sync_namespace(lc_pouch *pouch,
                                             const char *namespace_name,
                                             lc_error *error) {
  lc_pouch_namespace_manifest manifest;
  char *segments;
  char *snapshots;
  unsigned long i;
  int rc;

  memset(&manifest, 0, sizeof(manifest));
  rc = lc_pouch_namespace_manifest_open(&pouch->allocator, pouch->root_path,
                                        namespace_name, &manifest, NULL, NULL,
                                        error);
  if (rc != LC_OK) {
    lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
    return rc;
  }
  segments = lc_pouch_path_join(&pouch->allocator, manifest.namespace_path,
                                "segments");
  snapshots = lc_pouch_path_join(&pouch->allocator, manifest.namespace_path,
                                 "snapshots");
  if (segments == NULL || snapshots == NULL) {
    rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                      "failed to allocate pouch migration sync directories",
                      NULL, NULL, "pouch");
  }
  /* A failed earlier append can already decode as current in the page cache.
   * Sync every live replay file, even when this scan rewrote no records. */
  for (i = 0UL; rc == LC_OK && i < manifest.segment_count; ++i) {
    rc = lc_pouch_migration_sync_file(pouch, segments,
                                      manifest.segment_leaves[i], error);
  }
  if (rc == LC_OK && manifest.latest_snapshot != NULL) {
    rc = lc_pouch_migration_sync_file(pouch, snapshots,
                                      manifest.latest_snapshot, error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_path_fsync_directory(
        segments, "failed to sync migration segments", error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_path_fsync_directory(
        snapshots, "failed to sync migration snapshots", error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_migration_sync_file(pouch, manifest.namespace_path,
                                      "manifest", error);
  }
  if (rc == LC_OK) {
    rc = lc_pouch_path_fsync_directory(
        manifest.namespace_path, "failed to sync migration namespace", error);
  }
  lc_free_with_allocator(&pouch->allocator, segments);
  lc_free_with_allocator(&pouch->allocator, snapshots);
  lc_pouch_namespace_manifest_cleanup(&pouch->allocator, &manifest);
  return rc;
}

static int lc_pouch_migration_scan_namespace(lc_pouch *pouch,
                                             const char *namespace_name,
                                             int *incomplete, lc_error *error) {
  lc_pouch_migration_namespace migration;
  int rc;

  memset(&migration, 0, sizeof(migration));
  migration.pouch = pouch;
  migration.namespace_name = namespace_name;
  if (strcmp(namespace_name, LC_POUCH_MIGRATION_TXN_NAMESPACE) == 0) {
    rc = lc_pouch_state_visit(pouch, namespace_name,
                              lc_pouch_migration_txn_visit, &migration, error);
    rc = lc_pouch_migration_defer_unrelated_protocol_error(&migration, rc,
                                                           error);
  } else {
    rc = LC_OK;
  }
  if (rc == LC_OK) {
    rc =
        lc_pouch_state_visit(pouch, namespace_name,
                             lc_pouch_migration_lease_visit, &migration, error);
    rc = lc_pouch_migration_defer_unrelated_protocol_error(&migration, rc,
                                                           error);
  }
  *incomplete |= migration.incomplete;
  if (rc == LC_OK && !migration.incomplete) {
    rc = lc_pouch_migration_sync_namespace(pouch, namespace_name, error);
  }
  return rc;
}

static int lc_pouch_migration_marker_complete(lc_pouch *pouch, int *complete,
                                              lc_error *error) {
  char text[sizeof(LC_POUCH_MIGRATION_MARKER_TEXT)];
  char *path;
  FILE *file;
  size_t length;
  size_t read_count;
  int rc;

  if (pouch == NULL || complete == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch control migration marker requires output", NULL,
                        NULL, "pouch");
  }
  *complete = 0;
  path = lc_pouch_path_join(&pouch->allocator, pouch->root_path,
                            LC_POUCH_MIGRATION_MARKER_LEAF);
  if (path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch migration marker path", NULL,
                        NULL, "pouch");
  }
  file = fopen(path, "rb");
  if (file == NULL) {
    int saved_errno;

    saved_errno = errno;
    lc_free_with_allocator(&pouch->allocator, path);
    if (saved_errno == ENOENT) {
      return LC_OK;
    }
    return lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to read pouch migration marker",
                        strerror(saved_errno), NULL, "pouch");
  }
  length = strlen(LC_POUCH_MIGRATION_MARKER_TEXT);
  read_count = fread(text, 1U, sizeof(text), file);
  rc = LC_OK;
  if (ferror(file)) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to read pouch migration marker", strerror(errno),
                      NULL, "pouch");
  }
  if (fclose(file) != 0 && rc == LC_OK) {
    rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                      "failed to close pouch migration marker", strerror(errno),
                      NULL, "pouch");
  }
  lc_free_with_allocator(&pouch->allocator, path);
  if (rc == LC_OK && read_count == length &&
      memcmp(text, LC_POUCH_MIGRATION_MARKER_TEXT, length) == 0) {
    *complete = 1;
  }
  return rc;
}

static int lc_pouch_migration_marker_write(lc_pouch *pouch, lc_error *error) {
  char *path;
  int rc;

  path = lc_pouch_path_join(&pouch->allocator, pouch->root_path,
                            LC_POUCH_MIGRATION_MARKER_LEAF);
  if (path == NULL) {
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch migration marker path", NULL,
                        NULL, "pouch");
  }
  rc = lc_pouch_path_write_text_file(path, LC_POUCH_MIGRATION_MARKER_TEXT,
                                     error);
  lc_free_with_allocator(&pouch->allocator, path);
  return rc;
}

int lc_pouch_control_migration_run(lc_pouch *pouch, lc_error *error) {
  char *namespaces_path;
  DIR *directory;
  struct dirent *entry;
  lc_pouch_state_shared_mutation_guard *guard;
  int complete;
  int incomplete = 0;
  int durable_sync;
  int rc;

  if (pouch == NULL) {
    return lc_error_set(error, LC_ERR_INVALID, 0L,
                        "pouch control migration requires pouch", NULL, NULL,
                        "pouch");
  }
  rc = lc_pouch_migration_marker_complete(pouch, &complete, error);
  if (rc != LC_OK || complete) {
    return rc;
  }
  guard = NULL;
  rc = lc_pouch_state_shared_mutation_enter(pouch, &guard, error);
  if (rc != LC_OK) {
    return rc;
  }
  rc = lc_pouch_migration_marker_complete(pouch, &complete, error);
  if (rc != LC_OK || complete) {
    lc_pouch_state_shared_mutation_leave(&guard);
    return rc;
  }
  namespaces_path =
      lc_pouch_path_join(&pouch->allocator, pouch->root_path, "namespaces");
  if (namespaces_path == NULL) {
    lc_pouch_state_shared_mutation_leave(&guard);
    return lc_error_set(error, LC_ERR_NOMEM, 0L,
                        "failed to allocate pouch migration namespace path",
                        NULL, NULL, "pouch");
  }
  /* The handle is not exposed yet. Migration commits must reach disk even
   * when ordinary runtime writes use the asynchronous durability policy. */
  durable_sync = pouch->durable_sync;
  if (!durable_sync) {
    pouch->durable_sync = 1;
    rc = lc_pouch_fsync_batcher_init(pouch, error);
    if (rc != LC_OK) {
      pouch->durable_sync = durable_sync;
      lc_free_with_allocator(&pouch->allocator, namespaces_path);
      lc_pouch_state_shared_mutation_leave(&guard);
      return rc;
    }
  }
  directory = opendir(namespaces_path);
  lc_free_with_allocator(&pouch->allocator, namespaces_path);
  if (directory == NULL) {
    rc = errno == ENOENT
             ? LC_OK
             : lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                            "failed to scan pouch migration namespaces",
                            strerror(errno), NULL, "pouch");
  } else {
    rc = LC_OK;
    while (rc == LC_OK) {
      char *namespace_name;

      errno = 0;
      entry = readdir(directory);
      if (entry == NULL) {
        if (errno != 0) {
          rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                            "failed to scan pouch migration namespaces",
                            strerror(errno), NULL, "pouch");
        }
        break;
      }
      if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
        continue;
      }
      namespace_name =
          lc_pouch_path_unescape_name(&pouch->allocator, entry->d_name);
      if (namespace_name == NULL) {
        rc = lc_error_set(error, LC_ERR_NOMEM, 0L,
                          "failed to decode pouch migration namespace", NULL,
                          NULL, "pouch");
        break;
      }
      rc = lc_pouch_migration_scan_namespace(pouch, namespace_name, &incomplete,
                                             error);
      lc_free_with_allocator(&pouch->allocator, namespace_name);
    }
    if (closedir(directory) != 0 && rc == LC_OK) {
      rc = lc_error_set(error, LC_ERR_TRANSPORT, 0L,
                        "failed to close pouch migration namespaces",
                        strerror(errno), NULL, "pouch");
    }
  }
  if (rc == LC_OK && !incomplete) {
    rc = lc_pouch_migration_marker_write(pouch, error);
  }
  if (!durable_sync) {
    lc_pouch_fsync_batcher_close(pouch);
    pouch->durable_sync = durable_sync;
  }
  lc_pouch_state_shared_mutation_leave(&guard);
  return rc;
}

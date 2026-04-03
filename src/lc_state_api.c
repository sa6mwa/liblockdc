#include "lc_api_internal.h"
#include "lc_internal.h"

#include <stdlib.h>
#include <string.h>

static int
lc_engine_build_acquire_body(lc_engine_client *client,
                             const lc_engine_acquire_request *request,
                             lc_engine_buffer *body);
static int
lc_engine_build_keepalive_body(lc_engine_client *client,
                               const lc_engine_keepalive_request *request,
                               lc_engine_buffer *body);
static int
lc_engine_build_release_body(lc_engine_client *client,
                             const lc_engine_release_request *request,
                             lc_engine_buffer *body);
static int lc_engine_build_query_body(lc_engine_client *client,
                                      const lc_engine_query_request *request,
                                      lc_engine_buffer *body);
static int lc_engine_build_get_path(lc_engine_client *client,
                                    const lc_engine_get_request *request,
                                    lc_engine_buffer *path);
static int lc_engine_build_update_path(lc_engine_client *client,
                                       const lc_engine_update_request *request,
                                       lc_engine_buffer *path);
static int lc_engine_build_mutate_path(lc_engine_client *client,
                                       const lc_engine_mutate_request *request,
                                       lc_engine_buffer *path);
static int
lc_engine_build_metadata_path(lc_engine_client *client,
                              const lc_engine_metadata_request *request,
                              lc_engine_buffer *path);
static int lc_engine_build_remove_path(lc_engine_client *client,
                                       const lc_engine_remove_request *request,
                                       lc_engine_buffer *path);
static int
lc_engine_build_describe_path(lc_engine_client *client,
                              const lc_engine_describe_request *request,
                              lc_engine_buffer *path);
static int lc_engine_build_mutate_body(lc_engine_client *client,
                                       const lc_engine_mutate_request *request,
                                       lc_engine_buffer *body);
static int
lc_engine_build_metadata_body(lc_engine_client *client,
                              const lc_engine_metadata_request *request,
                              lc_engine_buffer *body);
typedef struct lc_engine_body_capture_state {
  lc_engine_buffer buffer;
} lc_engine_body_capture_state;
typedef struct lc_engine_query_response_json {
  char *cursor;
  lonejson_int64 index_seq;
} lc_engine_query_response_json;
static const lonejson_field lc_engine_query_response_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_query_response_json, cursor,
                                "cursor"),
    LONEJSON_FIELD_I64(lc_engine_query_response_json, index_seq,
                       "index_seq")};
LONEJSON_MAP_DEFINE(lc_engine_query_response_map,
                    lc_engine_query_response_json,
                    lc_engine_query_response_fields);
typedef struct lc_engine_dequeue_capture_state {
  lc_allocator allocator;
  lc_engine_client *client;
  lc_engine_dequeue_response *response;
  lc_source *payload;
  lc_stream_pipe *pipe;
  size_t payload_length;
} lc_engine_dequeue_capture_state;
typedef struct lc_engine_query_hidden_json {
  int query_hidden;
} lc_engine_query_hidden_json;
typedef struct lc_engine_acquire_response_json {
  char *namespace_name;
  char *key;
  char *owner;
  char *lease_id;
  char *txn_id;
  long lease_expires_at_unix;
  long version;
  char *state_etag;
  long fencing_token;
} lc_engine_acquire_response_json;
typedef struct lc_engine_keepalive_response_json {
  long lease_expires_at_unix;
  long version;
  char *state_etag;
} lc_engine_keepalive_response_json;
typedef struct lc_engine_release_response_json {
  int released;
} lc_engine_release_response_json;
typedef struct lc_engine_update_response_json {
  long new_version;
  char *new_state_etag;
  long bytes;
} lc_engine_update_response_json;
typedef struct lc_engine_metadata_response_json {
  char *namespace_name;
  char *key;
  long version;
  lc_engine_query_hidden_json metadata;
} lc_engine_metadata_response_json;
typedef struct lc_engine_remove_response_json {
  int removed;
  long new_version;
} lc_engine_remove_response_json;
typedef struct lc_engine_describe_response_json {
  char *namespace_name;
  char *key;
  char *owner;
  char *lease_id;
  long expires_at_unix;
  long version;
  char *state_etag;
  long updated_at_unix;
  lc_engine_query_hidden_json metadata;
} lc_engine_describe_response_json;
typedef struct lc_engine_enqueue_response_json {
  char *namespace_name;
  char *queue;
  char *message_id;
  long attempts;
  long max_attempts;
  long failure_attempts;
  long not_visible_until_unix;
  long visibility_timeout_seconds;
  long payload_bytes;
} lc_engine_enqueue_response_json;
typedef struct lc_engine_queue_stats_response_json {
  char *namespace_name;
  char *queue;
  long waiting_consumers;
  long pending_candidates;
  long total_consumers;
  int has_active_watcher;
  int available;
  char *head_message_id;
  long head_enqueued_at_unix;
  long head_not_visible_until_unix;
  long head_age_seconds;
} lc_engine_queue_stats_response_json;
typedef struct lc_engine_queue_ack_response_json {
  int acked;
} lc_engine_queue_ack_response_json;
typedef struct lc_engine_queue_nack_response_json {
  int requeued;
  char *meta_etag;
} lc_engine_queue_nack_response_json;
typedef struct lc_engine_queue_extend_response_json {
  long lease_expires_at_unix;
  long visibility_timeout_seconds;
  char *meta_etag;
  long state_lease_expires_at_unix;
} lc_engine_queue_extend_response_json;
static const lonejson_map lc_engine_query_hidden_map;
static const lonejson_field lc_engine_acquire_response_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_response_json,
                                namespace_name, "namespace"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_response_json, key, "key"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_response_json, owner,
                                "owner"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_response_json, lease_id,
                                "lease_id"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_response_json, txn_id,
                                "txn_id"),
    LONEJSON_FIELD_I64(lc_engine_acquire_response_json,
                       lease_expires_at_unix, "expires_at_unix"),
    LONEJSON_FIELD_I64(lc_engine_acquire_response_json, version, "version"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_response_json, state_etag,
                                "state_etag"),
    LONEJSON_FIELD_I64(lc_engine_acquire_response_json, fencing_token,
                       "fencing_token")};
static const lonejson_field lc_engine_keepalive_response_fields[] = {
    LONEJSON_FIELD_I64(lc_engine_keepalive_response_json,
                       lease_expires_at_unix, "expires_at_unix"),
    LONEJSON_FIELD_I64(lc_engine_keepalive_response_json, version, "version"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_keepalive_response_json, state_etag,
                                "state_etag")};
static const lonejson_field lc_engine_release_response_fields[] = {
    LONEJSON_FIELD_BOOL(lc_engine_release_response_json, released,
                        "released")};
static const lonejson_field lc_engine_update_response_fields[] = {
    LONEJSON_FIELD_I64(lc_engine_update_response_json, new_version,
                       "new_version"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_update_response_json, new_state_etag,
                                "new_state_etag"),
    LONEJSON_FIELD_I64(lc_engine_update_response_json, bytes, "bytes")};
static const lonejson_field lc_engine_metadata_response_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_metadata_response_json,
                                namespace_name, "namespace"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_metadata_response_json, key, "key"),
    LONEJSON_FIELD_I64(lc_engine_metadata_response_json, version, "version"),
    LONEJSON_FIELD_OBJECT(lc_engine_metadata_response_json, metadata,
                          "metadata", &lc_engine_query_hidden_map)};
static const lonejson_field lc_engine_remove_response_fields[] = {
    LONEJSON_FIELD_BOOL(lc_engine_remove_response_json, removed, "removed"),
    LONEJSON_FIELD_I64(lc_engine_remove_response_json, new_version,
                       "new_version")};
static const lonejson_field lc_engine_describe_response_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_describe_response_json, namespace_name,
                                "namespace"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_describe_response_json, key, "key"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_describe_response_json, owner,
                                "owner"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_describe_response_json, lease_id,
                                "lease_id"),
    LONEJSON_FIELD_I64(lc_engine_describe_response_json, expires_at_unix,
                       "expires_at_unix"),
    LONEJSON_FIELD_I64(lc_engine_describe_response_json, version, "version"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_describe_response_json, state_etag,
                                "state_etag"),
    LONEJSON_FIELD_I64(lc_engine_describe_response_json, updated_at_unix,
                       "updated_at_unix"),
    LONEJSON_FIELD_OBJECT(lc_engine_describe_response_json, metadata,
                          "metadata", &lc_engine_query_hidden_map)};
static const lonejson_field lc_engine_query_hidden_fields[] = {
    LONEJSON_FIELD_BOOL(lc_engine_query_hidden_json, query_hidden,
                        "query_hidden")};
LONEJSON_MAP_DEFINE(lc_engine_query_hidden_map, lc_engine_query_hidden_json,
                    lc_engine_query_hidden_fields);
static const lonejson_field lc_engine_enqueue_response_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_enqueue_response_json, namespace_name,
                                "namespace"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_enqueue_response_json, queue, "queue"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_enqueue_response_json, message_id,
                                "message_id"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_response_json, attempts, "attempts"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_response_json, max_attempts,
                       "max_attempts"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_response_json, failure_attempts,
                       "failure_attempts"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_response_json,
                       not_visible_until_unix, "not_visible_until_unix"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_response_json,
                       visibility_timeout_seconds,
                       "visibility_timeout_seconds"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_response_json, payload_bytes,
                       "payload_bytes")};
LONEJSON_MAP_DEFINE(lc_engine_acquire_response_map, lc_engine_acquire_response_json,
                    lc_engine_acquire_response_fields);
LONEJSON_MAP_DEFINE(lc_engine_keepalive_response_map,
                    lc_engine_keepalive_response_json,
                    lc_engine_keepalive_response_fields);
LONEJSON_MAP_DEFINE(lc_engine_release_response_map,
                    lc_engine_release_response_json,
                    lc_engine_release_response_fields);
LONEJSON_MAP_DEFINE(lc_engine_update_response_map, lc_engine_update_response_json,
                    lc_engine_update_response_fields);
LONEJSON_MAP_DEFINE(lc_engine_metadata_response_map,
                    lc_engine_metadata_response_json,
                    lc_engine_metadata_response_fields);
LONEJSON_MAP_DEFINE(lc_engine_remove_response_map, lc_engine_remove_response_json,
                    lc_engine_remove_response_fields);
LONEJSON_MAP_DEFINE(lc_engine_describe_response_map,
                    lc_engine_describe_response_json,
                    lc_engine_describe_response_fields);
LONEJSON_MAP_DEFINE(lc_engine_enqueue_response_map,
                    lc_engine_enqueue_response_json,
                    lc_engine_enqueue_response_fields);
static int lc_engine_copy_bytes(unsigned char **out_bytes, size_t *out_length,
                                const char *bytes, size_t count);
static int lc_engine_capture_body_writer(void *context, const void *bytes,
                                         size_t count,
                                         lc_engine_error *error);
static int lc_engine_copy_dequeue_response(
    lc_engine_dequeue_response *dst, const lc_engine_dequeue_response *src);
static int lc_engine_dequeue_capture_begin(
    void *context, const lc_engine_dequeue_response *delivery,
    lc_engine_error *error);
static int lc_engine_dequeue_capture_chunk(void *context, const void *bytes,
                                           size_t count,
                                           lc_engine_error *error);
static int lc_engine_dequeue_capture_end(
    void *context, const lc_engine_dequeue_response *delivery,
    lc_engine_error *error);
static int lc_engine_set_fencing_header(long fencing_token,
                                        lc_engine_buffer *buffer,
                                        lc_engine_header_pair *header);
static int lc_engine_apply_common_mutation_headers(
    const char *content_type, const char *txn_id, const char *if_state_etag,
    lc_engine_buffer *fence_value, lc_engine_header_pair *headers,
    size_t *header_count, long fencing_token);
static int lc_engine_buffer_append_long_decimal(lc_engine_buffer *buffer,
                                                long value);
static int lc_engine_buffer_append_json_string_literal(lc_engine_buffer *buffer,
                                                       const char *value);

static int lc_engine_buffer_append_long_decimal(lc_engine_buffer *buffer,
                                                long value) {
  char digits[32];
  unsigned long magnitude;
  size_t index;
  int negative;

  negative = value < 0L ? 1 : 0;
  if (negative) {
    magnitude = (unsigned long)(-(value + 1L)) + 1UL;
  } else {
    magnitude = (unsigned long)value;
  }

  index = 0U;
  do {
    digits[index++] = (char)('0' + (magnitude % 10UL));
    magnitude /= 10UL;
  } while (magnitude > 0UL && index < sizeof(digits));

  if (negative) {
    if (lc_engine_buffer_append_cstr(buffer, "-") != LC_ENGINE_OK) {
      return LC_ENGINE_ERROR_NO_MEMORY;
    }
  }

  while (index > 0U) {
    if (lc_engine_buffer_append(buffer, &digits[index - 1U], 1U) !=
        LC_ENGINE_OK) {
      return LC_ENGINE_ERROR_NO_MEMORY;
    }
    --index;
  }

  return LC_ENGINE_OK;
}

static int lc_engine_buffer_append_json_string_literal(lc_engine_buffer *buffer,
                                                       const char *value) {
  const char *cursor;
  int rc;

  if (value == NULL) {
    value = "";
  }
  rc = lc_engine_buffer_append_cstr(buffer, "\"");
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  cursor = value;
  while (*cursor != '\0') {
    switch (*cursor) {
    case '\\':
      rc = lc_engine_buffer_append_cstr(buffer, "\\\\");
      break;
    case '"':
      rc = lc_engine_buffer_append_cstr(buffer, "\\\"");
      break;
    case '\n':
      rc = lc_engine_buffer_append_cstr(buffer, "\\n");
      break;
    case '\r':
      rc = lc_engine_buffer_append_cstr(buffer, "\\r");
      break;
    case '\t':
      rc = lc_engine_buffer_append_cstr(buffer, "\\t");
      break;
    default:
      rc = lc_engine_buffer_append(buffer, cursor, 1U);
      break;
    }
    if (rc != LC_ENGINE_OK) {
      return rc;
    }
    ++cursor;
  }
  return lc_engine_buffer_append_cstr(buffer, "\"");
}

static int
lc_engine_build_acquire_body(lc_engine_client *client,
                             const lc_engine_acquire_request *request,
                             lc_engine_buffer *body) {
  int first_field;
  int rc;

  if (request == NULL || request->key == NULL || request->key[0] == '\0' ||
      request->owner == NULL || request->owner[0] == '\0' ||
      request->ttl_seconds <= 0L) {
    return LC_ENGINE_ERROR_INVALID_ARGUMENT;
  }

  lc_engine_buffer_init(body);
  rc = lc_engine_json_begin_object(body);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  first_field = 1;
  rc = lc_engine_json_add_string_field(
      body, &first_field, "namespace",
      lc_engine_effective_namespace(client, request->namespace_name));
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_add_string_field(body, &first_field, "key",
                                         request->key);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_add_long_field(body, &first_field, "ttl_seconds",
                                       request->ttl_seconds);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_add_string_field(body, &first_field, "owner",
                                         request->owner);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_add_long_field(body, &first_field, "block_seconds",
                                       request->block_seconds);
  if (rc == LC_ENGINE_OK && request->if_not_exists)
    rc = lc_engine_json_add_bool_field(body, &first_field, "if_not_exists", 1);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_add_string_field(body, &first_field, "txn_id",
                                         request->txn_id);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_end_object(body);
  return rc;
}

static int
lc_engine_build_keepalive_body(lc_engine_client *client,
                               const lc_engine_keepalive_request *request,
                               lc_engine_buffer *body) {
  int first_field;
  int rc;

  if (request == NULL || request->key == NULL || request->key[0] == '\0' ||
      request->lease_id == NULL || request->lease_id[0] == '\0' ||
      request->ttl_seconds <= 0L || request->fencing_token <= 0L) {
    return LC_ENGINE_ERROR_INVALID_ARGUMENT;
  }

  lc_engine_buffer_init(body);
  rc = lc_engine_json_begin_object(body);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  first_field = 1;
  rc = lc_engine_json_add_string_field(
      body, &first_field, "namespace",
      lc_engine_effective_namespace(client, request->namespace_name));
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_add_string_field(body, &first_field, "key",
                                         request->key);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_add_string_field(body, &first_field, "lease_id",
                                         request->lease_id);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_add_long_field(body, &first_field, "ttl_seconds",
                                       request->ttl_seconds);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_add_string_field(body, &first_field, "txn_id",
                                         request->txn_id);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_end_object(body);
  return rc;
}

static int
lc_engine_build_release_body(lc_engine_client *client,
                             const lc_engine_release_request *request,
                             lc_engine_buffer *body) {
  int first_field;
  int rc;

  if (request == NULL || request->key == NULL || request->key[0] == '\0' ||
      request->lease_id == NULL || request->lease_id[0] == '\0' ||
      request->txn_id == NULL || request->txn_id[0] == '\0' ||
      request->fencing_token <= 0L) {
    return LC_ENGINE_ERROR_INVALID_ARGUMENT;
  }

  lc_engine_buffer_init(body);
  rc = lc_engine_json_begin_object(body);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  first_field = 1;
  rc = lc_engine_json_add_string_field(
      body, &first_field, "namespace",
      lc_engine_effective_namespace(client, request->namespace_name));
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_add_string_field(body, &first_field, "key",
                                         request->key);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_add_string_field(body, &first_field, "lease_id",
                                         request->lease_id);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_add_string_field(body, &first_field, "txn_id",
                                         request->txn_id);
  if (rc == LC_ENGINE_OK && request->rollback)
    rc = lc_engine_json_add_bool_field(body, &first_field, "rollback", 1);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_end_object(body);
  return rc;
}

static int lc_engine_build_query_body(lc_engine_client *client,
                                      const lc_engine_query_request *request,
                                      lc_engine_buffer *body) {
  int first_field;
  int rc;

  if (request == NULL || request->selector_json == NULL ||
      request->selector_json[0] == '\0') {
    return LC_ENGINE_ERROR_INVALID_ARGUMENT;
  }

  lc_engine_buffer_init(body);
  rc = lc_engine_json_begin_object(body);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  first_field = 1;
  rc = lc_engine_json_add_string_field(
      body, &first_field, "namespace",
      lc_engine_effective_namespace(client, request->namespace_name));
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_add_raw_field(body, &first_field, "selector",
                                      request->selector_json);
  if (rc == LC_ENGINE_OK && request->limit > 0L)
    rc = lc_engine_json_add_long_field(body, &first_field, "limit",
                                       request->limit);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_add_string_field(body, &first_field, "cursor",
                                         request->cursor);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_add_raw_field(body, &first_field, "fields",
                                      request->fields_json);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_add_string_field(body, &first_field, "return",
                                         request->return_mode);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_end_object(body);
  return rc;
}

static int lc_engine_build_get_path(lc_engine_client *client,
                                    const lc_engine_get_request *request,
                                    lc_engine_buffer *path) {
  char *namespace_name;
  char *key;
  int rc;

  namespace_name = lc_engine_url_encode(
      lc_engine_effective_namespace(client, request->namespace_name));
  key = lc_engine_url_encode(request->key);
  if (namespace_name == NULL || key == NULL) {
    free(namespace_name);
    free(key);
    return LC_ENGINE_ERROR_NO_MEMORY;
  }

  lc_engine_buffer_init(path);
  rc = lc_engine_buffer_append_cstr(path, "/v1/get?key=");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, key);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, "&namespace=");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, namespace_name);
  if (rc == LC_ENGINE_OK && request->public_read)
    rc = lc_engine_buffer_append_cstr(path, "&public=1");

  free(namespace_name);
  free(key);
  return rc;
}

static int lc_engine_build_update_path(lc_engine_client *client,
                                       const lc_engine_update_request *request,
                                       lc_engine_buffer *path) {
  char *namespace_name;
  char *key;
  int rc;

  namespace_name = lc_engine_url_encode(
      lc_engine_effective_namespace(client, request->namespace_name));
  key = lc_engine_url_encode(request->key);
  if (namespace_name == NULL || key == NULL) {
    free(namespace_name);
    free(key);
    return LC_ENGINE_ERROR_NO_MEMORY;
  }

  lc_engine_buffer_init(path);
  rc = lc_engine_buffer_append_cstr(path, "/v1/update?key=");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, key);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, "&namespace=");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, namespace_name);

  free(namespace_name);
  free(key);
  return rc;
}

static int lc_engine_build_mutate_path(lc_engine_client *client,
                                       const lc_engine_mutate_request *request,
                                       lc_engine_buffer *path) {
  char *namespace_name;
  char *key;
  int rc;

  namespace_name = lc_engine_url_encode(
      lc_engine_effective_namespace(client, request->namespace_name));
  key = lc_engine_url_encode(request->key);
  if (namespace_name == NULL || key == NULL) {
    free(namespace_name);
    free(key);
    return LC_ENGINE_ERROR_NO_MEMORY;
  }

  lc_engine_buffer_init(path);
  rc = lc_engine_buffer_append_cstr(path, "/v1/mutate?key=");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, key);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, "&namespace=");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, namespace_name);

  free(namespace_name);
  free(key);
  return rc;
}

static int
lc_engine_build_metadata_path(lc_engine_client *client,
                              const lc_engine_metadata_request *request,
                              lc_engine_buffer *path) {
  char *namespace_name;
  char *key;
  int rc;

  namespace_name = lc_engine_url_encode(
      lc_engine_effective_namespace(client, request->namespace_name));
  key = lc_engine_url_encode(request->key);
  if (namespace_name == NULL || key == NULL) {
    free(namespace_name);
    free(key);
    return LC_ENGINE_ERROR_NO_MEMORY;
  }

  lc_engine_buffer_init(path);
  rc = lc_engine_buffer_append_cstr(path, "/v1/metadata?key=");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, key);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, "&namespace=");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, namespace_name);

  free(namespace_name);
  free(key);
  return rc;
}

static int lc_engine_build_remove_path(lc_engine_client *client,
                                       const lc_engine_remove_request *request,
                                       lc_engine_buffer *path) {
  char *namespace_name;
  char *key;
  int rc;

  namespace_name = lc_engine_url_encode(
      lc_engine_effective_namespace(client, request->namespace_name));
  key = lc_engine_url_encode(request->key);
  if (namespace_name == NULL || key == NULL) {
    free(namespace_name);
    free(key);
    return LC_ENGINE_ERROR_NO_MEMORY;
  }

  lc_engine_buffer_init(path);
  rc = lc_engine_buffer_append_cstr(path, "/v1/remove?key=");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, key);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, "&namespace=");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, namespace_name);

  free(namespace_name);
  free(key);
  return rc;
}

static int
lc_engine_build_describe_path(lc_engine_client *client,
                              const lc_engine_describe_request *request,
                              lc_engine_buffer *path) {
  char *namespace_name;
  char *key;
  int rc;

  namespace_name = lc_engine_url_encode(
      lc_engine_effective_namespace(client, request->namespace_name));
  key = lc_engine_url_encode(request->key);
  if (namespace_name == NULL || key == NULL) {
    free(namespace_name);
    free(key);
    return LC_ENGINE_ERROR_NO_MEMORY;
  }

  lc_engine_buffer_init(path);
  rc = lc_engine_buffer_append_cstr(path, "/v1/describe?key=");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, key);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, "&namespace=");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(path, namespace_name);

  free(namespace_name);
  free(key);
  return rc;
}

static int lc_engine_build_mutate_body(lc_engine_client *client,
                                       const lc_engine_mutate_request *request,
                                       lc_engine_buffer *body) {
  int first_field;
  int rc;
  size_t index;

  if (request == NULL || request->key == NULL || request->key[0] == '\0' ||
      request->txn_id == NULL || request->txn_id[0] == '\0' ||
      request->fencing_token <= 0L || request->mutation_count == 0U ||
      request->mutations == NULL) {
    return LC_ENGINE_ERROR_INVALID_ARGUMENT;
  }

  lc_engine_buffer_init(body);
  rc = lc_engine_json_begin_object(body);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  first_field = 1;
  rc = lc_engine_json_add_string_field(
      body, &first_field, "namespace",
      lc_engine_effective_namespace(client, request->namespace_name));
  if (rc == LC_ENGINE_OK) {
    rc = lc_engine_buffer_append_cstr(body, ",\"mutations\":[");
  }
  for (index = 0U; rc == LC_ENGINE_OK && index < request->mutation_count;
       ++index) {
    if (index > 0U) {
      rc = lc_engine_buffer_append_cstr(body, ",");
    }
    if (rc == LC_ENGINE_OK) {
      rc = lc_engine_buffer_append_json_string_literal(
          body, request->mutations[index]);
    }
  }
  if (rc == LC_ENGINE_OK) {
    rc = lc_engine_buffer_append_cstr(body, "]");
  }
  if (rc == LC_ENGINE_OK) {
    rc = lc_engine_json_end_object(body);
  }
  return rc;
}

static int
lc_engine_build_metadata_body(lc_engine_client *client,
                              const lc_engine_metadata_request *request,
                              lc_engine_buffer *body) {
  int first_field;
  int rc;

  if (request == NULL || request->key == NULL || request->key[0] == '\0' ||
      request->txn_id == NULL || request->txn_id[0] == '\0' ||
      request->fencing_token <= 0L) {
    return LC_ENGINE_ERROR_INVALID_ARGUMENT;
  }

  lc_engine_buffer_init(body);
  rc = lc_engine_json_begin_object(body);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  first_field = 1;
  rc = lc_engine_json_add_string_field(
      body, &first_field, "namespace",
      lc_engine_effective_namespace(client, request->namespace_name));
  if (rc == LC_ENGINE_OK && request->has_query_hidden) {
    rc = lc_engine_json_add_bool_field(body, &first_field, "query_hidden",
                                       request->query_hidden);
  }
  if (rc == LC_ENGINE_OK) {
    rc = lc_engine_json_end_object(body);
  }
  return rc;
}

static int lc_engine_copy_bytes(unsigned char **out_bytes, size_t *out_length,
                                const char *bytes, size_t count) {
  unsigned char *copy;

  *out_bytes = NULL;
  *out_length = 0U;
  if (count == 0U) {
    return LC_ENGINE_OK;
  }
  copy = (unsigned char *)malloc(count);
  if (copy == NULL) {
    return LC_ENGINE_ERROR_NO_MEMORY;
  }
  memcpy(copy, bytes, count);
  *out_bytes = copy;
  *out_length = count;
  return LC_ENGINE_OK;
}

static int lc_engine_capture_body_writer(void *context, const void *bytes,
                                         size_t count,
                                         lc_engine_error *error) {
  lc_engine_body_capture_state *state;

  state = (lc_engine_body_capture_state *)context;
  if (state == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "missing body capture state") ==
           LC_ENGINE_OK;
  }
  if (count == 0U) {
    return 1;
  }
  if (lc_engine_buffer_append(&state->buffer, (const char *)bytes, count) !=
      LC_ENGINE_OK) {
    lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                               "failed to capture response body");
    return 0;
  }
  return 1;
}

static int lc_engine_copy_dequeue_response(
    lc_engine_dequeue_response *dst, const lc_engine_dequeue_response *src) {
  if (dst == NULL || src == NULL) {
    return LC_ENGINE_ERROR_INVALID_ARGUMENT;
  }

  dst->namespace_name = lc_engine_strdup_local(src->namespace_name);
  dst->queue = lc_engine_strdup_local(src->queue);
  dst->message_id = lc_engine_strdup_local(src->message_id);
  dst->attempts = src->attempts;
  dst->max_attempts = src->max_attempts;
  dst->failure_attempts = src->failure_attempts;
  dst->not_visible_until_unix = src->not_visible_until_unix;
  dst->visibility_timeout_seconds = src->visibility_timeout_seconds;
  dst->payload_content_type = lc_engine_strdup_local(src->payload_content_type);
  dst->payload = NULL;
  dst->payload_length = src->payload_length;
  dst->correlation_id = lc_engine_strdup_local(src->correlation_id);
  dst->lease_id = lc_engine_strdup_local(src->lease_id);
  dst->lease_expires_at_unix = src->lease_expires_at_unix;
  dst->fencing_token = src->fencing_token;
  dst->txn_id = lc_engine_strdup_local(src->txn_id);
  dst->meta_etag = lc_engine_strdup_local(src->meta_etag);
  dst->state_etag = lc_engine_strdup_local(src->state_etag);
  dst->state_lease_id = lc_engine_strdup_local(src->state_lease_id);
  dst->state_lease_expires_at_unix = src->state_lease_expires_at_unix;
  dst->state_fencing_token = src->state_fencing_token;
  dst->state_txn_id = lc_engine_strdup_local(src->state_txn_id);
  dst->next_cursor = lc_engine_strdup_local(src->next_cursor);

  if ((src->namespace_name != NULL && dst->namespace_name == NULL) ||
      (src->queue != NULL && dst->queue == NULL) ||
      (src->message_id != NULL && dst->message_id == NULL) ||
      (src->payload_content_type != NULL && dst->payload_content_type == NULL) ||
      (src->correlation_id != NULL && dst->correlation_id == NULL) ||
      (src->lease_id != NULL && dst->lease_id == NULL) ||
      (src->txn_id != NULL && dst->txn_id == NULL) ||
      (src->meta_etag != NULL && dst->meta_etag == NULL) ||
      (src->state_etag != NULL && dst->state_etag == NULL) ||
      (src->state_lease_id != NULL && dst->state_lease_id == NULL) ||
      (src->state_txn_id != NULL && dst->state_txn_id == NULL) ||
      (src->next_cursor != NULL && dst->next_cursor == NULL)) {
    lc_engine_dequeue_response_cleanup(dst);
    return LC_ENGINE_ERROR_NO_MEMORY;
  }

  return LC_ENGINE_OK;
}

static int lc_engine_dequeue_capture_begin(
    void *context, const lc_engine_dequeue_response *delivery,
    lc_engine_error *error) {
  lc_engine_dequeue_capture_state *state;
  int rc;
  lc_error pipe_error;

  state = (lc_engine_dequeue_capture_state *)context;
  if (state == NULL || state->response == NULL || delivery == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "missing dequeue capture state") ==
           LC_ENGINE_OK;
  }
  lc_engine_dequeue_response_cleanup(state->response);
  rc = lc_engine_copy_dequeue_response(state->response, delivery);
  if (rc != LC_ENGINE_OK) {
    lc_engine_set_client_error(error, rc, "failed to capture dequeue metadata");
    return 0;
  }
  memset(&pipe_error, 0, sizeof(pipe_error));
  rc = lc_stream_pipe_open(65536U, &state->allocator, &state->payload,
                           &state->pipe, &pipe_error);
  if (rc != LC_OK) {
    lc_engine_set_transport_error(
        error, pipe_error.message != NULL ? pipe_error.message
                                          : "failed to create dequeue payload stream");
    lc_error_cleanup(&pipe_error);
    lc_engine_dequeue_response_cleanup(state->response);
    return 0;
  }
  lc_error_cleanup(&pipe_error);
  state->payload_length = 0U;
  state->response->payload = state->payload;
  state->response->payload_length = 0U;
  return 1;
}

static int lc_engine_dequeue_capture_chunk(void *context, const void *bytes,
                                           size_t count,
                                           lc_engine_error *error) {
  lc_engine_dequeue_capture_state *state;
  int rc;

  state = (lc_engine_dequeue_capture_state *)context;
  if (state == NULL || state->response == NULL || state->pipe == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "missing dequeue capture state") ==
           LC_ENGINE_OK;
  }
  if (count == 0U) {
    return 1;
  }
  {
    lc_error pipe_error;

    memset(&pipe_error, 0, sizeof(pipe_error));
    rc = lc_stream_pipe_write(state->pipe, bytes, count, &pipe_error);
    if (rc != LC_OK) {
      lc_engine_set_transport_error(
          error, pipe_error.message != NULL ? pipe_error.message
                                            : "failed to stream dequeue payload");
      lc_error_cleanup(&pipe_error);
      return 0;
    }
    lc_error_cleanup(&pipe_error);
  }
  if (state->payload_length > ((size_t)-1) - count) {
    lc_engine_set_protocol_error(error, "dequeue payload is too large");
    return 0;
  }
  state->payload_length += count;
  return 1;
}

static int lc_engine_dequeue_capture_end(
    void *context, const lc_engine_dequeue_response *delivery,
    lc_engine_error *error) {
  lc_engine_dequeue_capture_state *state;

  (void)delivery;
  state = (lc_engine_dequeue_capture_state *)context;
  if (state == NULL || state->pipe == NULL || state->response == NULL ||
      state->payload == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "missing dequeue capture state") ==
           LC_ENGINE_OK;
  }
  lc_stream_pipe_finish(state->pipe);
  state->pipe = NULL;
  state->response->payload = state->payload;
  state->response->payload_length = state->payload_length;
  state->payload = NULL;
  return 1;
}

static int lc_engine_set_fencing_header(long fencing_token,
                                        lc_engine_buffer *buffer,
                                        lc_engine_header_pair *header) {
  lc_engine_buffer_init(buffer);
  if (lc_engine_buffer_append_long_decimal(buffer, fencing_token) !=
      LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(buffer);
    return LC_ENGINE_ERROR_NO_MEMORY;
  }
  header->name = "X-Fencing-Token";
  header->value = buffer->data;
  return LC_ENGINE_OK;
}

static int lc_engine_apply_common_mutation_headers(
    const char *content_type, const char *txn_id, const char *if_state_etag,
    lc_engine_buffer *fence_value, lc_engine_header_pair *headers,
    size_t *header_count, long fencing_token) {
  int rc;

  *header_count = 0U;
  headers[*header_count].name = "Content-Type";
  headers[*header_count].value = content_type;
  *header_count += 1U;
  rc = lc_engine_set_fencing_header(fencing_token, fence_value,
                                    &headers[*header_count]);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  *header_count += 1U;
  headers[*header_count].name = "X-Txn-ID";
  headers[*header_count].value = txn_id;
  *header_count += 1U;
  if (if_state_etag != NULL && if_state_etag[0] != '\0') {
    headers[*header_count].name = "X-If-State-ETag";
    headers[*header_count].value = if_state_etag;
    *header_count += 1U;
  }
  return LC_ENGINE_OK;
}

int lc_engine_client_acquire(lc_engine_client *client,
                             const lc_engine_acquire_request *request,
                             lc_engine_acquire_response *response,
                             lc_engine_error *error) {
  lc_engine_buffer body;
  lc_engine_http_result result;
  lc_engine_header_pair headers[1];
  lc_engine_acquire_response_json response_json;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }

  lc_engine_acquire_response_cleanup(response);
  rc = lc_engine_build_acquire_body(client, request, &body);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, rc, "invalid acquire request");
  }

  headers[0].name = "Content-Type";
  headers[0].value = "application/json";
  memset(&result, 0, sizeof(result));
  rc = lc_engine_http_json_request(client, "POST", "/v1/acquire", body.data,
                                   body.length, headers, 1U,
                                   &lc_engine_acquire_response_map,
                                   &response_json, &result, error);
  lc_engine_buffer_cleanup(&body);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status != 200L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }

  response->namespace_name = response_json.namespace_name;
  response_json.namespace_name = NULL;
  response->key = response_json.key;
  response_json.key = NULL;
  response->owner = response_json.owner;
  response_json.owner = NULL;
  response->lease_id = response_json.lease_id;
  response_json.lease_id = NULL;
  response->txn_id = response_json.txn_id;
  response_json.txn_id = NULL;
  response->lease_expires_at_unix = response_json.lease_expires_at_unix;
  response->version = response_json.version;
  response->state_etag = response_json.state_etag;
  response_json.state_etag = NULL;
  response->fencing_token = response_json.fencing_token;
  if (result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_acquire_response_cleanup(response);
    lc_engine_free_string(&response_json.namespace_name);
    lc_engine_free_string(&response_json.key);
    lc_engine_free_string(&response_json.owner);
    lc_engine_free_string(&response_json.lease_id);
    lc_engine_free_string(&response_json.txn_id);
    lc_engine_free_string(&response_json.state_etag);
    lc_engine_http_result_cleanup(&result);
    return lc_engine_set_protocol_error(error,
                                        "failed to decode acquire response");
  }

  lc_engine_http_result_cleanup(&result);
  return LC_ENGINE_OK;
}

int lc_engine_client_get(lc_engine_client *client,
                         const lc_engine_get_request *request,
                         lc_engine_get_response *response,
                         lc_engine_error *error) {
  lc_engine_buffer path;
  lc_engine_buffer fence_value;
  lc_engine_body_capture_state body_state;
  lc_engine_get_stream_response stream_response;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }
  if (request->key == NULL || request->key[0] == '\0') {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "get.key is required");
  }
  if (!request->public_read &&
      (request->lease_id == NULL || request->lease_id[0] == '\0')) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "get requires lease_id unless public_read is enabled");
  }

  lc_engine_get_response_cleanup(response);
  rc = lc_engine_build_get_path(client, request, &path);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&path);
    return lc_engine_set_client_error(error, rc, "failed to build get path");
  }

  lc_engine_buffer_init(&fence_value);
  if (!request->public_read) {
    lc_engine_header_pair empty_header = {0};

    rc = lc_engine_set_fencing_header(request->fencing_token, &fence_value,
                                      &empty_header);
    if (rc != LC_ENGINE_OK) {
      lc_engine_buffer_cleanup(&fence_value);
      lc_engine_buffer_cleanup(&path);
      return rc;
    }
  }

  lc_engine_buffer_init(&body_state.buffer);
  memset(&stream_response, 0, sizeof(stream_response));
  rc = lc_engine_client_get_into(client, request, lc_engine_capture_body_writer,
                                 &body_state, &stream_response, error);
  lc_engine_buffer_cleanup(&fence_value);
  lc_engine_buffer_cleanup(&path);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body_state.buffer);
    return rc;
  }

  if (!stream_response.no_content) {
    rc = lc_engine_copy_bytes(&response->body, &response->body_length,
                              body_state.buffer.data, body_state.buffer.length);
    if (rc != LC_ENGINE_OK) {
      lc_engine_get_response_cleanup(response);
      lc_engine_buffer_cleanup(&body_state.buffer);
      lc_engine_get_stream_response_cleanup(&stream_response);
      return lc_engine_set_client_error(error, rc, "failed to copy get body");
    }
  }
  response->content_type = lc_engine_strdup_local(
      stream_response.content_type != NULL ? stream_response.content_type
                                           : "application/octet-stream");
  if (response->content_type == NULL) {
    rc = LC_ENGINE_ERROR_NO_MEMORY;
  }
  if (rc == LC_ENGINE_OK && stream_response.etag != NULL) {
    response->etag = lc_engine_strdup_local(stream_response.etag);
    if (response->etag == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc == LC_ENGINE_OK) {
    response->version = stream_response.version;
    response->fencing_token = stream_response.fencing_token;
  }
  if (rc == LC_ENGINE_OK && stream_response.correlation_id != NULL) {
    response->correlation_id =
        lc_engine_strdup_local(stream_response.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_get_response_cleanup(response);
    lc_engine_buffer_cleanup(&body_state.buffer);
    lc_engine_get_stream_response_cleanup(&stream_response);
    return lc_engine_set_client_error(error, rc, "failed to copy get response");
  }

  lc_engine_buffer_cleanup(&body_state.buffer);
  lc_engine_get_stream_response_cleanup(&stream_response);
  return LC_ENGINE_OK;
}

int lc_engine_client_keepalive(lc_engine_client *client,
                               const lc_engine_keepalive_request *request,
                               lc_engine_keepalive_response *response,
                               lc_engine_error *error) {
  lc_engine_buffer body;
  lc_engine_buffer fence_value;
  lc_engine_header_pair headers[2];
  lc_engine_http_result result;
  lc_engine_keepalive_response_json response_json;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }

  lc_engine_keepalive_response_cleanup(response);
  rc = lc_engine_build_keepalive_body(client, request, &body);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, rc, "invalid keepalive request");
  }

  rc = lc_engine_set_fencing_header(request->fencing_token, &fence_value,
                                    &headers[1]);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, rc,
                                      "failed to prepare keepalive headers");
  }
  headers[0].name = "Content-Type";
  headers[0].value = "application/json";

  memset(&result, 0, sizeof(result));
  rc = lc_engine_http_json_request(client, "POST", "/v1/keepalive", body.data,
                                   body.length, headers, 2U,
                                   &lc_engine_keepalive_response_map,
                                   &response_json, &result, error);
  lc_engine_buffer_cleanup(&body);
  lc_engine_buffer_cleanup(&fence_value);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status != 200L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }

  response->lease_expires_at_unix = response_json.lease_expires_at_unix;
  response->version = response_json.version;
  response->state_etag = response_json.state_etag;
  response_json.state_etag = NULL;
  if (result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_keepalive_response_cleanup(response);
    lc_engine_free_string(&response_json.state_etag);
    lc_engine_http_result_cleanup(&result);
    return lc_engine_set_protocol_error(error,
                                        "failed to decode keepalive response");
  }

  lc_engine_http_result_cleanup(&result);
  return LC_ENGINE_OK;
}

int lc_engine_client_release(lc_engine_client *client,
                             const lc_engine_release_request *request,
                             lc_engine_release_response *response,
                             lc_engine_error *error) {
  lc_engine_buffer body;
  lc_engine_buffer fence_value;
  lc_engine_header_pair headers[2];
  lc_engine_http_result result;
  lc_engine_release_response_json response_json;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }

  lc_engine_release_response_cleanup(response);
  rc = lc_engine_build_release_body(client, request, &body);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, rc, "invalid release request");
  }

  rc = lc_engine_set_fencing_header(request->fencing_token, &fence_value,
                                    &headers[1]);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, rc,
                                      "failed to prepare release headers");
  }
  headers[0].name = "Content-Type";
  headers[0].value = "application/json";

  memset(&result, 0, sizeof(result));
  rc = lc_engine_http_json_request(client, "POST", "/v1/release", body.data,
                                   body.length, headers, 2U,
                                   &lc_engine_release_response_map,
                                   &response_json, &result, error);
  lc_engine_buffer_cleanup(&body);
  lc_engine_buffer_cleanup(&fence_value);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status != 200L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }

  response->released = response_json.released;
  if (result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_release_response_cleanup(response);
    lc_engine_http_result_cleanup(&result);
    return lc_engine_set_protocol_error(error,
                                        "failed to decode release response");
  }

  lc_engine_http_result_cleanup(&result);
  return LC_ENGINE_OK;
}

void lc_engine_update_response_cleanup(lc_engine_update_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->new_state_etag);
  lc_engine_free_string(&response->correlation_id);
  memset(response, 0, sizeof(*response));
}

int lc_engine_client_update(lc_engine_client *client,
                            const lc_engine_update_request *request,
                            lc_engine_update_response *response,
                            lc_engine_error *error) {
  lc_engine_buffer path;
  lc_engine_buffer fence_value;
  lc_engine_buffer version_value;
  lc_engine_header_pair headers[5];
  lc_engine_http_result result;
  lc_engine_update_response_json response_json;
  size_t header_count;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }
  if (request->key == NULL || request->key[0] == '\0' ||
      request->txn_id == NULL || request->txn_id[0] == '\0' ||
      request->fencing_token <= 0L) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "update requires key, txn_id, and fencing_token");
  }

  lc_engine_update_response_cleanup(response);
  rc = lc_engine_build_update_path(client, request, &path);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&path);
    return lc_engine_set_client_error(error, rc, "failed to build update path");
  }

  rc = lc_engine_apply_common_mutation_headers(
      request->content_type != NULL ? request->content_type
                                    : "application/octet-stream",
      request->txn_id, request->if_state_etag, &fence_value, headers,
      &header_count, request->fencing_token);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&path);
    return lc_engine_set_client_error(error, rc,
                                      "failed to prepare update headers");
  }

  lc_engine_buffer_init(&version_value);
  if (request->has_if_version) {
    if (lc_engine_buffer_append_long_decimal(
            &version_value, request->if_version) != LC_ENGINE_OK) {
      lc_engine_buffer_cleanup(&path);
      lc_engine_buffer_cleanup(&fence_value);
      lc_engine_buffer_cleanup(&version_value);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to prepare update version header");
    }
    headers[header_count].name = "X-If-Version";
    headers[header_count].value = version_value.data;
    ++header_count;
  }

  memset(&result, 0, sizeof(result));
  rc = lc_engine_http_json_request(client, "POST", path.data, request->body,
                                   request->body_length, headers, header_count,
                                   &lc_engine_update_response_map,
                                   &response_json, &result, error);
  lc_engine_buffer_cleanup(&path);
  lc_engine_buffer_cleanup(&fence_value);
  lc_engine_buffer_cleanup(&version_value);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status != 200L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }

  response->new_version = response_json.new_version;
  response->new_state_etag = response_json.new_state_etag;
  response_json.new_state_etag = NULL;
  response->bytes = response_json.bytes;
  if (result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_update_response_cleanup(response);
    lc_engine_free_string(&response_json.new_state_etag);
    lc_engine_http_result_cleanup(&result);
    return lc_engine_set_protocol_error(error,
                                        "failed to decode update response");
  }

  lc_engine_http_result_cleanup(&result);
  return LC_ENGINE_OK;
}

void lc_engine_mutate_response_cleanup(lc_engine_mutate_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->new_state_etag);
  lc_engine_free_string(&response->correlation_id);
  memset(response, 0, sizeof(*response));
}

int lc_engine_client_mutate(lc_engine_client *client,
                            const lc_engine_mutate_request *request,
                            lc_engine_mutate_response *response,
                            lc_engine_error *error) {
  lc_engine_buffer path;
  lc_engine_buffer body;
  lc_engine_buffer fence_value;
  lc_engine_buffer version_value;
  lc_engine_header_pair headers[5];
  lc_engine_http_result result;
  lc_engine_update_response_json response_json;
  size_t header_count;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }

  lc_engine_mutate_response_cleanup(response);
  rc = lc_engine_build_mutate_path(client, request, &path);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&path);
    return lc_engine_set_client_error(error, rc, "failed to build mutate path");
  }
  rc = lc_engine_build_mutate_body(client, request, &body);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&path);
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, rc, "failed to build mutate body");
  }
  rc = lc_engine_apply_common_mutation_headers(
      "application/json", request->txn_id, request->if_state_etag, &fence_value,
      headers, &header_count, request->fencing_token);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&path);
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, rc,
                                      "failed to prepare mutate headers");
  }
  lc_engine_buffer_init(&version_value);
  if (request->has_if_version) {
    if (lc_engine_buffer_append_long_decimal(
            &version_value, request->if_version) != LC_ENGINE_OK) {
      lc_engine_buffer_cleanup(&path);
      lc_engine_buffer_cleanup(&body);
      lc_engine_buffer_cleanup(&fence_value);
      lc_engine_buffer_cleanup(&version_value);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to prepare mutate version header");
    }
    headers[header_count].name = "X-If-Version";
    headers[header_count].value = version_value.data;
    ++header_count;
  }

  memset(&result, 0, sizeof(result));
  rc = lc_engine_http_json_request(client, "POST", path.data, body.data,
                                   body.length, headers, header_count,
                                   &lc_engine_update_response_map,
                                   &response_json, &result, error);
  lc_engine_buffer_cleanup(&path);
  lc_engine_buffer_cleanup(&body);
  lc_engine_buffer_cleanup(&fence_value);
  lc_engine_buffer_cleanup(&version_value);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status != 200L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }

  response->new_version = response_json.new_version;
  response->new_state_etag = response_json.new_state_etag;
  response_json.new_state_etag = NULL;
  response->bytes = response_json.bytes;
  if (result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_mutate_response_cleanup(response);
    lc_engine_free_string(&response_json.new_state_etag);
    lc_engine_http_result_cleanup(&result);
    return lc_engine_set_protocol_error(error,
                                        "failed to decode mutate response");
  }

  lc_engine_http_result_cleanup(&result);
  return LC_ENGINE_OK;
}

void lc_engine_metadata_response_cleanup(
    lc_engine_metadata_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->namespace_name);
  lc_engine_free_string(&response->key);
  lc_engine_free_string(&response->correlation_id);
  memset(response, 0, sizeof(*response));
}

int lc_engine_client_update_metadata(lc_engine_client *client,
                                     const lc_engine_metadata_request *request,
                                     lc_engine_metadata_response *response,
                                     lc_engine_error *error) {
  lc_engine_buffer path;
  lc_engine_buffer body;
  lc_engine_buffer fence_value;
  lc_engine_buffer version_value;
  lc_engine_header_pair headers[4];
  lc_engine_http_result result;
  lc_engine_metadata_response_json response_json;
  size_t header_count;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }

  lc_engine_metadata_response_cleanup(response);
  rc = lc_engine_build_metadata_path(client, request, &path);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&path);
    return lc_engine_set_client_error(error, rc,
                                      "failed to build metadata path");
  }
  rc = lc_engine_build_metadata_body(client, request, &body);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&path);
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, rc,
                                      "failed to build metadata body");
  }
  header_count = 0U;
  headers[header_count].name = "Content-Type";
  headers[header_count].value = "application/json";
  ++header_count;
  rc = lc_engine_set_fencing_header(request->fencing_token, &fence_value,
                                    &headers[header_count]);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&path);
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, rc,
                                      "failed to prepare metadata headers");
  }
  ++header_count;
  headers[header_count].name = "X-Txn-ID";
  headers[header_count].value = request->txn_id;
  ++header_count;
  lc_engine_buffer_init(&version_value);
  if (request->has_if_version) {
    if (lc_engine_buffer_append_long_decimal(
            &version_value, request->if_version) != LC_ENGINE_OK) {
      lc_engine_buffer_cleanup(&path);
      lc_engine_buffer_cleanup(&body);
      lc_engine_buffer_cleanup(&fence_value);
      lc_engine_buffer_cleanup(&version_value);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to prepare metadata version header");
    }
    headers[header_count].name = "X-If-Version";
    headers[header_count].value = version_value.data;
    ++header_count;
  }

  memset(&result, 0, sizeof(result));
  rc = lc_engine_http_json_request(client, "POST", path.data, body.data,
                                   body.length, headers, header_count,
                                   &lc_engine_metadata_response_map,
                                   &response_json, &result, error);
  lc_engine_buffer_cleanup(&path);
  lc_engine_buffer_cleanup(&body);
  lc_engine_buffer_cleanup(&fence_value);
  lc_engine_buffer_cleanup(&version_value);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status != 200L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }

  response->namespace_name = response_json.namespace_name;
  response_json.namespace_name = NULL;
  response->key = response_json.key;
  response_json.key = NULL;
  response->version = response_json.version;
  response->has_query_hidden = 1;
  response->query_hidden = response_json.metadata.query_hidden;
  if (result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_metadata_response_cleanup(response);
    lc_engine_free_string(&response_json.namespace_name);
    lc_engine_free_string(&response_json.key);
    lc_engine_http_result_cleanup(&result);
    return lc_engine_set_protocol_error(error,
                                        "failed to decode metadata response");
  }

  lc_engine_http_result_cleanup(&result);
  return LC_ENGINE_OK;
}

void lc_engine_remove_response_cleanup(lc_engine_remove_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->correlation_id);
  memset(response, 0, sizeof(*response));
}

int lc_engine_client_remove(lc_engine_client *client,
                            const lc_engine_remove_request *request,
                            lc_engine_remove_response *response,
                            lc_engine_error *error) {
  lc_engine_buffer path;
  lc_engine_buffer fence_value;
  lc_engine_buffer version_value;
  lc_engine_header_pair headers[5];
  lc_engine_http_result result;
  lc_engine_remove_response_json response_json;
  size_t header_count;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }
  if (request->key == NULL || request->key[0] == '\0' ||
      request->txn_id == NULL || request->txn_id[0] == '\0' ||
      request->fencing_token <= 0L) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "remove requires key, txn_id, and fencing_token");
  }

  lc_engine_remove_response_cleanup(response);
  rc = lc_engine_build_remove_path(client, request, &path);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&path);
    return lc_engine_set_client_error(error, rc, "failed to build remove path");
  }
  header_count = 0U;
  headers[header_count].name = "Content-Type";
  headers[header_count].value = "application/json";
  ++header_count;
  rc = lc_engine_set_fencing_header(request->fencing_token, &fence_value,
                                    &headers[header_count]);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&path);
    return lc_engine_set_client_error(error, rc,
                                      "failed to prepare remove headers");
  }
  ++header_count;
  headers[header_count].name = "X-Txn-ID";
  headers[header_count].value = request->txn_id;
  ++header_count;
  if (request->if_state_etag != NULL && request->if_state_etag[0] != '\0') {
    headers[header_count].name = "X-If-State-ETag";
    headers[header_count].value = request->if_state_etag;
    ++header_count;
  }
  lc_engine_buffer_init(&version_value);
  if (request->has_if_version) {
    if (lc_engine_buffer_append_long_decimal(
            &version_value, request->if_version) != LC_ENGINE_OK) {
      lc_engine_buffer_cleanup(&path);
      lc_engine_buffer_cleanup(&fence_value);
      lc_engine_buffer_cleanup(&version_value);
      return lc_engine_set_client_error(
          error, LC_ENGINE_ERROR_NO_MEMORY,
          "failed to prepare remove version header");
    }
    headers[header_count].name = "X-If-Version";
    headers[header_count].value = version_value.data;
    ++header_count;
  }

  memset(&result, 0, sizeof(result));
  rc = lc_engine_http_json_request(client, "POST", path.data, NULL, 0U,
                                   headers, header_count,
                                   &lc_engine_remove_response_map,
                                   &response_json, &result, error);
  lc_engine_buffer_cleanup(&path);
  lc_engine_buffer_cleanup(&fence_value);
  lc_engine_buffer_cleanup(&version_value);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status != 200L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }

  response->removed = response_json.removed;
  response->new_version = response_json.new_version;
  if (result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_remove_response_cleanup(response);
    lc_engine_http_result_cleanup(&result);
    return lc_engine_set_protocol_error(error,
                                        "failed to decode remove response");
  }

  lc_engine_http_result_cleanup(&result);
  return LC_ENGINE_OK;
}

void lc_engine_describe_response_cleanup(
    lc_engine_describe_response *response) {
  if (response == NULL) {
    return;
  }
  lc_engine_free_string(&response->namespace_name);
  lc_engine_free_string(&response->key);
  lc_engine_free_string(&response->owner);
  lc_engine_free_string(&response->lease_id);
  lc_engine_free_string(&response->state_etag);
  lc_engine_free_string(&response->correlation_id);
  memset(response, 0, sizeof(*response));
}

int lc_engine_client_describe(lc_engine_client *client,
                              const lc_engine_describe_request *request,
                              lc_engine_describe_response *response,
                              lc_engine_error *error) {
  lc_engine_buffer path;
  lc_engine_http_result result;
  lc_engine_describe_response_json response_json;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }
  if (request->key == NULL || request->key[0] == '\0') {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "describe.key is required");
  }

  lc_engine_describe_response_cleanup(response);
  rc = lc_engine_build_describe_path(client, request, &path);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&path);
    return lc_engine_set_client_error(error, rc,
                                      "failed to build describe path");
  }

  memset(&result, 0, sizeof(result));
  rc = lc_engine_http_json_request(client, "GET", path.data, NULL, 0U, NULL,
                                   0U, &lc_engine_describe_response_map,
                                   &response_json, &result, error);
  lc_engine_buffer_cleanup(&path);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status != 200L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }

  response->namespace_name = response_json.namespace_name;
  response_json.namespace_name = NULL;
  response->key = response_json.key;
  response_json.key = NULL;
  response->owner = response_json.owner;
  response_json.owner = NULL;
  response->lease_id = response_json.lease_id;
  response_json.lease_id = NULL;
  response->expires_at_unix = response_json.expires_at_unix;
  response->version = response_json.version;
  response->state_etag = response_json.state_etag;
  response_json.state_etag = NULL;
  response->updated_at_unix = response_json.updated_at_unix;
  response->has_query_hidden = 1;
  response->query_hidden = response_json.metadata.query_hidden;
  if (result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_describe_response_cleanup(response);
    lc_engine_free_string(&response_json.namespace_name);
    lc_engine_free_string(&response_json.key);
    lc_engine_free_string(&response_json.owner);
    lc_engine_free_string(&response_json.lease_id);
    lc_engine_free_string(&response_json.state_etag);
    lc_engine_http_result_cleanup(&result);
    return lc_engine_set_protocol_error(error,
                                        "failed to decode describe response");
  }

  lc_engine_http_result_cleanup(&result);
  return LC_ENGINE_OK;
}

int lc_engine_client_query(lc_engine_client *client,
                           const lc_engine_query_request *request,
                           lc_engine_query_response *response,
                           lc_engine_error *error) {
  lc_engine_buffer body;
  lc_engine_http_result result;
  lc_engine_header_pair headers[1];
  lc_engine_query_response_json parsed;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }

  lc_engine_query_response_cleanup(response);
  rc = lc_engine_build_query_body(client, request, &body);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, rc, "invalid query request");
  }

  headers[0].name = "Content-Type";
  headers[0].value = "application/json";
  memset(&result, 0, sizeof(result));
  memset(&parsed, 0, sizeof(parsed));
  rc = lc_engine_http_json_request(
      client, "POST", "/v1/query", body.data, body.length, headers, 1U,
      &lc_engine_query_response_map, &parsed, &result, error);
  lc_engine_buffer_cleanup(&body);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status != 200L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lonejson_cleanup(&lc_engine_query_response_map, &parsed);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }
  response->cursor = parsed.cursor;
  parsed.cursor = NULL;
  response->index_seq = (unsigned long)parsed.index_seq;
  if (result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_query_response_cleanup(response);
    lonejson_cleanup(&lc_engine_query_response_map, &parsed);
    lc_engine_http_result_cleanup(&result);
    return lc_engine_set_protocol_error(error,
                                        "failed to decode query response");
  }

  lonejson_cleanup(&lc_engine_query_response_map, &parsed);
  lc_engine_http_result_cleanup(&result);
  return LC_ENGINE_OK;
}

int lc_engine_client_enqueue(lc_engine_client *client,
                             const lc_engine_enqueue_request *request,
                             lc_engine_enqueue_response *response,
                             lc_engine_error *error) {
  lc_engine_buffer meta;
  lc_engine_buffer body;
  lc_engine_buffer content_type;
  lc_engine_http_result result;
  lc_engine_header_pair headers[1];
  lc_engine_enqueue_response_json response_json;
  const char *boundary;
  int first_field;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }
  if (request->queue == NULL || request->queue[0] == '\0') {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "enqueue.queue is required");
  }

  boundary = "lockdc-boundary-7e4dbe2f";
  lc_engine_enqueue_response_cleanup(response);

  lc_engine_buffer_init(&meta);
  rc = lc_engine_json_begin_object(&meta);
  first_field = 1;
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_add_string_field(
        &meta, &first_field, "namespace",
        lc_engine_effective_namespace(client, request->namespace_name));
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_add_string_field(&meta, &first_field, "queue",
                                         request->queue);
  if (rc == LC_ENGINE_OK && request->delay_seconds > 0L)
    rc = lc_engine_json_add_long_field(&meta, &first_field, "delay_seconds",
                                       request->delay_seconds);
  if (rc == LC_ENGINE_OK && request->visibility_timeout_seconds > 0L)
    rc = lc_engine_json_add_long_field(&meta, &first_field,
                                       "visibility_timeout_seconds",
                                       request->visibility_timeout_seconds);
  if (rc == LC_ENGINE_OK && request->ttl_seconds > 0L)
    rc = lc_engine_json_add_long_field(&meta, &first_field, "ttl_seconds",
                                       request->ttl_seconds);
  if (rc == LC_ENGINE_OK && request->max_attempts > 0)
    rc = lc_engine_json_add_long_field(&meta, &first_field, "max_attempts",
                                       (long)request->max_attempts);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_add_string_field(
        &meta, &first_field, "payload_content_type",
        request->payload_content_type != NULL ? request->payload_content_type
                                              : "application/octet-stream");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_end_object(&meta);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&meta);
    return lc_engine_set_client_error(error, rc,
                                      "failed to build enqueue metadata");
  }

  lc_engine_buffer_init(&body);
  rc = lc_engine_buffer_append_cstr(&body, "--");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(&body, boundary);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(
        &body, "\r\nContent-Type: application/json\r\nContent-Disposition: "
               "form-data; name=\"meta\"\r\n\r\n");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append(&body, meta.data, meta.length);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(&body, "\r\n--");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(&body, boundary);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(&body,
                                      "\r\nContent-Disposition: form-data; "
                                      "name=\"payload\"\r\nContent-Type: ");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(&body,
                                      request->payload_content_type != NULL
                                          ? request->payload_content_type
                                          : "application/octet-stream");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(&body, "\r\n\r\n");
  if (rc == LC_ENGINE_OK && request->payload != NULL &&
      request->payload_length > 0U)
    rc = lc_engine_buffer_append(&body, (const char *)request->payload,
                                 request->payload_length);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(&body, "\r\n--");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(&body, boundary);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(&body, "--\r\n");
  lc_engine_buffer_cleanup(&meta);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, rc,
                                      "failed to build enqueue body");
  }

  lc_engine_buffer_init(&content_type);
  rc = lc_engine_buffer_append_cstr(&content_type,
                                    "multipart/related; boundary=");
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_buffer_append_cstr(&content_type, boundary);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body);
    lc_engine_buffer_cleanup(&content_type);
    return lc_engine_set_client_error(error, rc,
                                      "failed to build enqueue content-type");
  }
  headers[0].name = "Content-Type";
  headers[0].value = content_type.data;

  memset(&result, 0, sizeof(result));
  rc = lc_engine_http_json_request(client, "POST", "/v1/queue/enqueue",
                                   body.data, body.length, headers, 1U,
                                   &lc_engine_enqueue_response_map,
                                   &response_json, &result, error);
  lc_engine_buffer_cleanup(&body);
  lc_engine_buffer_cleanup(&content_type);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status != 200L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }

  response->namespace_name = response_json.namespace_name;
  response_json.namespace_name = NULL;
  response->queue = response_json.queue;
  response_json.queue = NULL;
  response->message_id = response_json.message_id;
  response_json.message_id = NULL;
  response->attempts = (int)response_json.attempts;
  response->max_attempts = (int)response_json.max_attempts;
  response->failure_attempts = (int)response_json.failure_attempts;
  response->not_visible_until_unix = response_json.not_visible_until_unix;
  response->visibility_timeout_seconds =
      response_json.visibility_timeout_seconds;
  response->payload_bytes = response_json.payload_bytes;
  if (result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_enqueue_response_cleanup(response);
    lc_engine_free_string(&response_json.namespace_name);
    lc_engine_free_string(&response_json.queue);
    lc_engine_free_string(&response_json.message_id);
    lc_engine_http_result_cleanup(&result);
    return lc_engine_set_protocol_error(error,
                                        "failed to decode enqueue response");
  }

  lc_engine_http_result_cleanup(&result);
  return LC_ENGINE_OK;
}

static int lc_engine_dequeue_internal(lc_engine_client *client,
                                      const lc_engine_dequeue_request *request,
                                      int with_state,
                                      lc_engine_dequeue_response *response,
                                      lc_engine_error *error) {
  lc_engine_queue_stream_handler handler;
  lc_engine_dequeue_capture_state capture;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }
  if (request->queue == NULL || request->queue[0] == '\0') {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
                                      "dequeue.queue is required");
  }

  lc_engine_dequeue_response_cleanup(response);
  memset(&handler, 0, sizeof(handler));
  handler.begin = lc_engine_dequeue_capture_begin;
  handler.chunk = lc_engine_dequeue_capture_chunk;
  handler.end = lc_engine_dequeue_capture_end;
  memset(&capture, 0, sizeof(capture));
  capture.allocator.malloc_fn = client->allocator.malloc_fn;
  capture.allocator.realloc_fn = client->allocator.realloc_fn;
  capture.allocator.free_fn = client->allocator.free_fn;
  capture.allocator.context = client->allocator.context;
  capture.client = client;
  capture.response = response;
  rc = with_state ? lc_engine_client_dequeue_with_state_into(
                       client, request, &handler, &capture, error)
                  : lc_engine_client_dequeue_into(client, request, &handler,
                                                  &capture, error);
  if (rc != LC_ENGINE_OK) {
    if (capture.payload != NULL) {
      capture.payload->close(capture.payload);
      capture.payload = NULL;
    }
    lc_engine_dequeue_response_cleanup(response);
    return rc;
  }
  return LC_ENGINE_OK;
}

int lc_engine_client_dequeue(lc_engine_client *client,
                             const lc_engine_dequeue_request *request,
                             lc_engine_dequeue_response *response,
                             lc_engine_error *error) {
  return lc_engine_dequeue_internal(client, request, 0, response, error);
}

int lc_engine_client_dequeue_with_state(
    lc_engine_client *client, const lc_engine_dequeue_request *request,
    lc_engine_dequeue_response *response, lc_engine_error *error) {
  return lc_engine_dequeue_internal(client, request, 1, response, error);
}

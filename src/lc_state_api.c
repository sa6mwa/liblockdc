#include "lc_api_internal.h"
#include "lc_internal.h"

#include <stdlib.h>
#include <string.h>

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
typedef struct lc_engine_body_capture_state {
  lc_engine_buffer buffer;
} lc_engine_body_capture_state;
typedef struct lc_engine_query_response_json {
  char *cursor;
  lonejson_int64 index_seq;
} lc_engine_query_response_json;
typedef struct lc_engine_query_body_json {
  char *namespace_name;
  lonejson_json_value selector;
  lonejson_int64 limit;
  char *cursor;
  lonejson_json_value fields;
  char *return_mode;
} lc_engine_query_body_json;
typedef struct lc_engine_mutate_body_json {
  char *namespace_name;
  lonejson_string_array mutations;
} lc_engine_mutate_body_json;
static const lonejson_field lc_engine_acquire_body_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_request, namespace_name,
                                "namespace"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_request, key, "key"),
    LONEJSON_FIELD_I64(lc_engine_acquire_request, ttl_seconds, "ttl_seconds"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_request, owner, "owner"),
    LONEJSON_FIELD_I64(lc_engine_acquire_request, block_seconds,
                       "block_seconds"),
    LONEJSON_FIELD_BOOL(lc_engine_acquire_request, if_not_exists,
                        "if_not_exists"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_request, txn_id, "txn_id")};
static const lonejson_field lc_engine_keepalive_body_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_keepalive_request, namespace_name,
                                "namespace"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_keepalive_request, key, "key"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_keepalive_request, lease_id,
                                "lease_id"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_keepalive_request, txn_id, "txn_id"),
    LONEJSON_FIELD_I64(lc_engine_keepalive_request, ttl_seconds, "ttl_seconds"),
    LONEJSON_FIELD_I64(lc_engine_keepalive_request, fencing_token,
                       "fencing_token")};
static const lonejson_field lc_engine_release_body_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_release_request, namespace_name,
                                "namespace"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_release_request, key, "key"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_release_request, lease_id,
                                "lease_id"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_release_request, txn_id, "txn_id"),
    LONEJSON_FIELD_I64(lc_engine_release_request, fencing_token,
                       "fencing_token"),
    LONEJSON_FIELD_BOOL(lc_engine_release_request, rollback, "rollback")};
static const lonejson_field lc_engine_metadata_body_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_metadata_request, namespace_name,
                                "namespace"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_metadata_request, key, "key"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_metadata_request, lease_id,
                                "lease_id"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_metadata_request, txn_id, "txn_id"),
    LONEJSON_FIELD_I64(lc_engine_metadata_request, fencing_token,
                       "fencing_token"),
    LONEJSON_FIELD_I64(lc_engine_metadata_request, if_version, "if_version"),
    LONEJSON_FIELD_BOOL(lc_engine_metadata_request, has_query_hidden,
                        "has_query_hidden"),
    LONEJSON_FIELD_BOOL(lc_engine_metadata_request, query_hidden,
                        "query_hidden")};
static const lonejson_field lc_engine_query_body_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_query_body_json, namespace_name,
                                "namespace"),
    LONEJSON_FIELD_JSON_VALUE_REQ(lc_engine_query_body_json, selector,
                                  "selector"),
    LONEJSON_FIELD_I64(lc_engine_query_body_json, limit, "limit"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_query_body_json, cursor, "cursor"),
    LONEJSON_FIELD_JSON_VALUE(lc_engine_query_body_json, fields, "fields"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_query_body_json, return_mode,
                                "return")};
static const lonejson_field lc_engine_mutate_body_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_mutate_body_json, namespace_name,
                                "namespace"),
    LONEJSON_FIELD_STRING_ARRAY(lc_engine_mutate_body_json, mutations,
                                "mutations", LONEJSON_OVERFLOW_FAIL)};
static const lonejson_field lc_engine_query_response_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_query_response_json, cursor,
                                "cursor"),
    LONEJSON_FIELD_I64(lc_engine_query_response_json, index_seq, "index_seq")};
LONEJSON_MAP_DEFINE(lc_engine_query_response_map, lc_engine_query_response_json,
                    lc_engine_query_response_fields);
typedef struct lc_engine_dequeue_capture_state {
  lc_allocator allocator;
  lc_engine_client *client;
  lc_engine_dequeue_response *response;
  lc_source *payload;
  lc_stream_pipe *pipe;
  size_t payload_length;
} lc_engine_dequeue_capture_state;
typedef struct lc_engine_enqueue_meta_json {
  char *namespace_name;
  char *queue;
  lonejson_int64 delay_seconds;
  lonejson_int64 visibility_timeout_seconds;
  lonejson_int64 ttl_seconds;
  lonejson_int64 max_attempts;
  char *payload_content_type;
} lc_engine_enqueue_meta_json;
static const lonejson_field lc_engine_enqueue_meta_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_enqueue_meta_json, namespace_name,
                                "namespace"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_enqueue_meta_json, queue, "queue"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_meta_json, delay_seconds,
                       "delay_seconds"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_meta_json, visibility_timeout_seconds,
                       "visibility_timeout_seconds"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_meta_json, ttl_seconds, "ttl_seconds"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_meta_json, max_attempts,
                       "max_attempts"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_enqueue_meta_json,
                                payload_content_type, "payload_content_type")};
typedef struct lc_engine_query_hidden_json {
  int query_hidden;
} lc_engine_query_hidden_json;
typedef struct lc_engine_acquire_response_json {
  char *namespace_name;
  char *key;
  char *owner;
  char *lease_id;
  char *txn_id;
  lonejson_int64 lease_expires_at_unix;
  lonejson_int64 version;
  char *state_etag;
  lonejson_int64 fencing_token;
} lc_engine_acquire_response_json;
typedef struct lc_engine_keepalive_response_json {
  lonejson_int64 lease_expires_at_unix;
  lonejson_int64 version;
  char *state_etag;
} lc_engine_keepalive_response_json;
typedef struct lc_engine_release_response_json {
  int released;
} lc_engine_release_response_json;
typedef struct lc_engine_update_response_json {
  lonejson_int64 new_version;
  char *new_state_etag;
  lonejson_int64 bytes;
} lc_engine_update_response_json;
typedef struct lc_engine_metadata_response_json {
  char *namespace_name;
  char *key;
  lonejson_int64 version;
  lc_engine_query_hidden_json metadata;
} lc_engine_metadata_response_json;
typedef struct lc_engine_remove_response_json {
  int removed;
  lonejson_int64 new_version;
} lc_engine_remove_response_json;
typedef struct lc_engine_describe_response_json {
  char *namespace_name;
  char *key;
  char *owner;
  char *lease_id;
  lonejson_int64 expires_at_unix;
  lonejson_int64 version;
  char *state_etag;
  lonejson_int64 updated_at_unix;
  lc_engine_query_hidden_json metadata;
} lc_engine_describe_response_json;
typedef struct lc_engine_enqueue_response_json {
  char *namespace_name;
  char *queue;
  char *message_id;
  lonejson_int64 attempts;
  lonejson_int64 max_attempts;
  lonejson_int64 failure_attempts;
  lonejson_int64 not_visible_until_unix;
  lonejson_int64 visibility_timeout_seconds;
  lonejson_int64 payload_bytes;
} lc_engine_enqueue_response_json;
typedef struct lc_engine_queue_stats_response_json {
  char *namespace_name;
  char *queue;
  lonejson_int64 waiting_consumers;
  lonejson_int64 pending_candidates;
  lonejson_int64 total_consumers;
  int has_active_watcher;
  int available;
  char *head_message_id;
  lonejson_int64 head_enqueued_at_unix;
  lonejson_int64 head_not_visible_until_unix;
  lonejson_int64 head_age_seconds;
} lc_engine_queue_stats_response_json;
typedef struct lc_engine_queue_ack_response_json {
  int acked;
} lc_engine_queue_ack_response_json;
typedef struct lc_engine_queue_nack_response_json {
  int requeued;
  char *meta_etag;
} lc_engine_queue_nack_response_json;
typedef struct lc_engine_queue_extend_response_json {
  lonejson_int64 lease_expires_at_unix;
  lonejson_int64 visibility_timeout_seconds;
  char *meta_etag;
  lonejson_int64 state_lease_expires_at_unix;
} lc_engine_queue_extend_response_json;
static const lonejson_map lc_engine_query_hidden_map;
static const lonejson_field lc_engine_acquire_response_fields[] = {
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_response_json, namespace_name,
                                "namespace"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_response_json, key, "key"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_response_json, owner,
                                "owner"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_response_json, lease_id,
                                "lease_id"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_response_json, txn_id,
                                "txn_id"),
    LONEJSON_FIELD_I64(lc_engine_acquire_response_json, lease_expires_at_unix,
                       "expires_at_unix"),
    LONEJSON_FIELD_I64(lc_engine_acquire_response_json, version, "version"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_acquire_response_json, state_etag,
                                "state_etag"),
    LONEJSON_FIELD_I64(lc_engine_acquire_response_json, fencing_token,
                       "fencing_token")};
static const lonejson_field lc_engine_keepalive_response_fields[] = {
    LONEJSON_FIELD_I64(lc_engine_keepalive_response_json, lease_expires_at_unix,
                       "expires_at_unix"),
    LONEJSON_FIELD_I64(lc_engine_keepalive_response_json, version, "version"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_keepalive_response_json, state_etag,
                                "state_etag")};
static const lonejson_field lc_engine_release_response_fields[] = {
    LONEJSON_FIELD_BOOL(lc_engine_release_response_json, released, "released")};
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
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_describe_response_json,
                                namespace_name, "namespace"),
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
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_enqueue_response_json, queue,
                                "queue"),
    LONEJSON_FIELD_STRING_ALLOC(lc_engine_enqueue_response_json, message_id,
                                "message_id"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_response_json, attempts, "attempts"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_response_json, max_attempts,
                       "max_attempts"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_response_json, failure_attempts,
                       "failure_attempts"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_response_json, not_visible_until_unix,
                       "not_visible_until_unix"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_response_json,
                       visibility_timeout_seconds,
                       "visibility_timeout_seconds"),
    LONEJSON_FIELD_I64(lc_engine_enqueue_response_json, payload_bytes,
                       "payload_bytes")};
LONEJSON_MAP_DEFINE(lc_engine_acquire_response_map,
                    lc_engine_acquire_response_json,
                    lc_engine_acquire_response_fields);
LONEJSON_MAP_DEFINE(lc_engine_keepalive_response_map,
                    lc_engine_keepalive_response_json,
                    lc_engine_keepalive_response_fields);
LONEJSON_MAP_DEFINE(lc_engine_release_response_map,
                    lc_engine_release_response_json,
                    lc_engine_release_response_fields);
LONEJSON_MAP_DEFINE(lc_engine_update_response_map,
                    lc_engine_update_response_json,
                    lc_engine_update_response_fields);
LONEJSON_MAP_DEFINE(lc_engine_metadata_response_map,
                    lc_engine_metadata_response_json,
                    lc_engine_metadata_response_fields);
LONEJSON_MAP_DEFINE(lc_engine_remove_response_map,
                    lc_engine_remove_response_json,
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
                                         size_t count, lc_engine_error *error);
static int
lc_engine_copy_dequeue_response(lc_engine_dequeue_response *dst,
                                const lc_engine_dequeue_response *src);
static int
lc_engine_dequeue_capture_begin(void *context,
                                const lc_engine_dequeue_response *delivery,
                                lc_engine_error *error);
static int lc_engine_dequeue_capture_chunk(void *context, const void *bytes,
                                           size_t count,
                                           lc_engine_error *error);
static int
lc_engine_dequeue_capture_end(void *context,
                              const lc_engine_dequeue_response *delivery,
                              lc_engine_error *error);
static int lc_engine_set_fencing_header(lonejson_int64 fencing_token,
                                        lc_engine_buffer *buffer,
                                        lc_engine_header_pair *header);
static int lc_engine_apply_common_mutation_headers(
    const char *content_type, const char *lease_id, const char *txn_id,
    const char *if_state_etag, lc_engine_buffer *fence_value,
    lc_engine_header_pair *headers, size_t *header_count,
    lonejson_int64 fencing_token);
static int lc_engine_buffer_append_long_decimal(lc_engine_buffer *buffer,
                                                lonejson_int64 value);
static int lc_engine_buffer_append_json_string_literal(lc_engine_buffer *buffer,
                                                       const char *value);

static void lc_engine_lonejson_map_init(lonejson_map *map, const char *name,
                                        const lonejson_field *fields,
                                        size_t field_count,
                                        size_t struct_size) {
  if (map == NULL) {
    return;
  }
  map->name = name;
  map->struct_size = struct_size;
  map->fields = fields;
  map->field_count = field_count;
}

static int lc_engine_buffer_append_long_decimal(lc_engine_buffer *buffer,
                                                lonejson_int64 value) {
  char digits[32];
  unsigned long long magnitude;
  size_t index;
  int negative;

  negative = value < 0 ? 1 : 0;
  if (negative) {
    magnitude = (unsigned long long)(-(value + 1LL)) + 1ULL;
  } else {
    magnitude = (unsigned long long)value;
  }

  index = 0U;
  do {
    digits[index++] = (char)('0' + (magnitude % 10ULL));
    magnitude /= 10ULL;
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
                                         size_t count, lc_engine_error *error) {
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

static int
lc_engine_copy_dequeue_response(lc_engine_dequeue_response *dst,
                                const lc_engine_dequeue_response *src) {
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
      (src->payload_content_type != NULL &&
       dst->payload_content_type == NULL) ||
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

static int
lc_engine_dequeue_capture_begin(void *context,
                                const lc_engine_dequeue_response *delivery,
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
        error, pipe_error.message != NULL
                   ? pipe_error.message
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
      lc_engine_set_transport_error(error,
                                    pipe_error.message != NULL
                                        ? pipe_error.message
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

static int
lc_engine_dequeue_capture_end(void *context,
                              const lc_engine_dequeue_response *delivery,
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

static int lc_engine_set_fencing_header(lonejson_int64 fencing_token,
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
    const char *content_type, const char *lease_id, const char *txn_id,
    const char *if_state_etag, lc_engine_buffer *fence_value,
    lc_engine_header_pair *headers, size_t *header_count,
    lonejson_int64 fencing_token) {
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
  if (lease_id != NULL && lease_id[0] != '\0') {
    headers[*header_count].name = "X-Lease-ID";
    headers[*header_count].value = lease_id;
    *header_count += 1U;
  }
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
  lc_engine_http_result result;
  lc_engine_acquire_request body_src;
  lonejson_field body_fields[7];
  lonejson_map body_map;
  lc_engine_header_pair headers[1];
  lc_engine_acquire_response_json response_json;
  size_t body_field_count;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }

  lc_engine_acquire_response_cleanup(response);
  body_src = *request;
  body_src.namespace_name =
      (char *)lc_engine_effective_namespace(client, request->namespace_name);
  body_field_count = 0U;
  body_fields[body_field_count++] = lc_engine_acquire_body_fields[0];
  body_fields[body_field_count++] = lc_engine_acquire_body_fields[1];
  body_fields[body_field_count++] = lc_engine_acquire_body_fields[2];
  body_fields[body_field_count++] = lc_engine_acquire_body_fields[3];
  if (request->block_seconds > 0L) {
    body_fields[body_field_count++] = lc_engine_acquire_body_fields[4];
  }
  if (request->if_not_exists) {
    body_fields[body_field_count++] = lc_engine_acquire_body_fields[5];
  }
  if (request->txn_id != NULL && request->txn_id[0] != '\0') {
    body_fields[body_field_count++] = lc_engine_acquire_body_fields[6];
  }
  lc_engine_lonejson_map_init(&body_map, "lc_engine_acquire_request",
                              body_fields, body_field_count,
                              sizeof(body_src));
  headers[0].name = "Content-Type";
  headers[0].value = "application/json";
  memset(&result, 0, sizeof(result));
  rc = lc_engine_http_json_request_stream(
      client, "POST", "/v1/acquire", &body_map, &body_src, NULL, headers, 1U,
      &lc_engine_acquire_response_map, &response_json, &result, error);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status != 200L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }

  response->namespace_name =
      lc_engine_strdup_local(response_json.namespace_name);
  response->key = lc_engine_strdup_local(response_json.key);
  response->owner = lc_engine_strdup_local(response_json.owner);
  response->lease_id = lc_engine_strdup_local(response_json.lease_id);
  response->txn_id = lc_engine_strdup_local(response_json.txn_id);
  response->lease_expires_at_unix = response_json.lease_expires_at_unix;
  response->version = response_json.version;
  response->state_etag = lc_engine_strdup_local(response_json.state_etag);
  response->fencing_token = response_json.fencing_token;
  if ((response_json.namespace_name != NULL &&
       response->namespace_name == NULL) ||
      (response_json.key != NULL && response->key == NULL) ||
      (response_json.owner != NULL && response->owner == NULL) ||
      (response_json.lease_id != NULL && response->lease_id == NULL) ||
      (response_json.txn_id != NULL && response->txn_id == NULL) ||
      (response_json.state_etag != NULL && response->state_etag == NULL)) {
    rc = LC_ENGINE_ERROR_NO_MEMORY;
  }
  if (result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_acquire_response_cleanup(response);
    lc_engine_http_result_cleanup(&result);
    return lc_engine_set_protocol_error(error,
                                        "failed to decode acquire response");
  }

  lonejson_cleanup(&lc_engine_acquire_response_map, &response_json);
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
  lc_engine_buffer fence_value;
  lc_engine_keepalive_request body_src;
  lonejson_field body_fields[6];
  lonejson_map body_map;
  lc_engine_header_pair headers[2];
  lc_engine_http_result result;
  lc_engine_keepalive_response_json response_json;
  size_t body_field_count;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }

  lc_engine_keepalive_response_cleanup(response);
  body_src = *request;
  body_src.namespace_name =
      (char *)lc_engine_effective_namespace(client, request->namespace_name);
  body_field_count = 0U;
  body_fields[body_field_count++] = lc_engine_keepalive_body_fields[0];
  body_fields[body_field_count++] = lc_engine_keepalive_body_fields[1];
  body_fields[body_field_count++] = lc_engine_keepalive_body_fields[2];
  body_fields[body_field_count++] = lc_engine_keepalive_body_fields[4];
  body_fields[body_field_count++] = lc_engine_keepalive_body_fields[5];
  if (request->txn_id != NULL && request->txn_id[0] != '\0') {
    body_fields[body_field_count++] = lc_engine_keepalive_body_fields[3];
  }
  lc_engine_lonejson_map_init(&body_map, "lc_engine_keepalive_request",
                              body_fields, body_field_count,
                              sizeof(body_src));
  rc = lc_engine_set_fencing_header(request->fencing_token, &fence_value,
                                    &headers[1]);
  if (rc != LC_ENGINE_OK) {
    return lc_engine_set_client_error(error, rc,
                                      "failed to prepare keepalive headers");
  }
  headers[0].name = "Content-Type";
  headers[0].value = "application/json";

  memset(&result, 0, sizeof(result));
  rc = lc_engine_http_json_request_stream(
      client, "POST", "/v1/keepalive", &body_map, &body_src, NULL, headers,
      2U, &lc_engine_keepalive_response_map, &response_json, &result, error);
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
  response->state_etag = lc_engine_strdup_local(response_json.state_etag);
  if (response_json.state_etag != NULL && response->state_etag == NULL) {
    rc = LC_ENGINE_ERROR_NO_MEMORY;
  }
  if (result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_keepalive_response_cleanup(response);
    lc_engine_http_result_cleanup(&result);
    return lc_engine_set_protocol_error(error,
                                        "failed to decode keepalive response");
  }

  lonejson_cleanup(&lc_engine_keepalive_response_map, &response_json);
  lc_engine_http_result_cleanup(&result);
  return LC_ENGINE_OK;
}

int lc_engine_client_release(lc_engine_client *client,
                             const lc_engine_release_request *request,
                             lc_engine_release_response *response,
                             lc_engine_error *error) {
  lc_engine_buffer fence_value;
  lc_engine_release_request body_src;
  lonejson_field body_fields[6];
  lonejson_map body_map;
  lc_engine_header_pair headers[2];
  lc_engine_http_result result;
  lc_engine_release_response_json response_json;
  size_t body_field_count;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }

  lc_engine_release_response_cleanup(response);
  body_src = *request;
  body_src.namespace_name =
      (char *)lc_engine_effective_namespace(client, request->namespace_name);
  body_field_count = 0U;
  body_fields[body_field_count++] = lc_engine_release_body_fields[0];
  body_fields[body_field_count++] = lc_engine_release_body_fields[1];
  body_fields[body_field_count++] = lc_engine_release_body_fields[2];
  if (request->txn_id != NULL && request->txn_id[0] != '\0') {
    body_fields[body_field_count++] = lc_engine_release_body_fields[3];
  }
  if (request->rollback) {
    body_fields[body_field_count++] = lc_engine_release_body_fields[5];
  }
  lc_engine_lonejson_map_init(&body_map, "lc_engine_release_request",
                              body_fields, body_field_count,
                              sizeof(body_src));
  rc = lc_engine_set_fencing_header(request->fencing_token, &fence_value,
                                    &headers[1]);
  if (rc != LC_ENGINE_OK) {
    return lc_engine_set_client_error(error, rc,
                                      "failed to prepare release headers");
  }
  headers[0].name = "Content-Type";
  headers[0].value = "application/json";

  memset(&result, 0, sizeof(result));
  rc = lc_engine_http_json_request_stream(
      client, "POST", "/v1/release", &body_map, &body_src, NULL, headers, 2U,
      &lc_engine_release_response_map, &response_json, &result, error);
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
  lc_engine_header_pair headers[6];
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
      request->lease_id, request->txn_id, request->if_state_etag,
      &fence_value, headers, &header_count, request->fencing_token);
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
  response->new_state_etag =
      lc_engine_strdup_local(response_json.new_state_etag);
  if (response_json.new_state_etag != NULL &&
      response->new_state_etag == NULL) {
    rc = LC_ENGINE_ERROR_NO_MEMORY;
  }
  response->bytes = response_json.bytes;
  if (result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_update_response_cleanup(response);
    lc_engine_http_result_cleanup(&result);
    return lc_engine_set_protocol_error(error,
                                        "failed to decode update response");
  }

  lonejson_cleanup(&lc_engine_update_response_map, &response_json);
  lc_engine_http_result_cleanup(&result);
  return LC_ENGINE_OK;
}

int lc_engine_client_update_stream(lc_engine_client *client,
                                   const lc_engine_update_request *request,
                                   const lonejson_map *body_map,
                                   const void *body_src,
                                   const lonejson_write_options *body_options,
                                   lc_engine_update_response *response,
                                   lc_engine_error *error) {
  lc_engine_buffer path;
  lc_engine_buffer fence_value;
  lc_engine_buffer version_value;
  lc_engine_header_pair headers[6];
  lc_engine_http_result result;
  lc_engine_update_response_json response_json;
  size_t header_count;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }
  if (body_map == NULL || body_src == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "update_stream requires body map and source");
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
      request->lease_id, request->txn_id, request->if_state_etag,
      &fence_value, headers, &header_count, request->fencing_token);
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
  rc = lc_engine_http_json_request_stream(
      client, "POST", path.data, body_map, body_src, body_options, headers,
      header_count, &lc_engine_update_response_map, &response_json, &result,
      error);
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
  response->new_state_etag =
      lc_engine_strdup_local(response_json.new_state_etag);
  if (response_json.new_state_etag != NULL &&
      response->new_state_etag == NULL) {
    rc = LC_ENGINE_ERROR_NO_MEMORY;
  }
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

  lonejson_cleanup(&lc_engine_update_response_map, &response_json);
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
  lc_engine_mutate_body_json body_src;
  lonejson_field body_fields[2];
  lonejson_map body_map;
  lc_engine_buffer fence_value;
  lc_engine_buffer version_value;
  lc_engine_header_pair headers[6];
  lc_engine_http_result result;
  lc_engine_update_response_json response_json;
  size_t body_field_count;
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
  body_src.namespace_name =
      (char *)lc_engine_effective_namespace(client, request->namespace_name);
  body_src.mutations.items = (char **)request->mutations;
  body_src.mutations.count = request->mutation_count;
  body_src.mutations.capacity = request->mutation_count;
  body_src.mutations.flags = LONEJSON_ARRAY_FIXED_CAPACITY;
  body_field_count = 0U;
  if (body_src.namespace_name != NULL && body_src.namespace_name[0] != '\0') {
    body_fields[body_field_count++] = lc_engine_mutate_body_fields[0];
  }
  body_fields[body_field_count++] = lc_engine_mutate_body_fields[1];
  lc_engine_lonejson_map_init(&body_map, "lc_engine_mutate_body_json",
                              body_fields, body_field_count,
                              sizeof(body_src));
  rc = lc_engine_apply_common_mutation_headers(
      "application/json", request->lease_id, request->txn_id,
      request->if_state_etag, &fence_value, headers, &header_count,
      request->fencing_token);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&path);
    return lc_engine_set_client_error(error, rc,
                                      "failed to prepare mutate headers");
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
          "failed to prepare mutate version header");
    }
    headers[header_count].name = "X-If-Version";
    headers[header_count].value = version_value.data;
    ++header_count;
  }

  memset(&result, 0, sizeof(result));
  rc = lc_engine_http_json_request_stream(
      client, "POST", path.data, &body_map, &body_src, NULL, headers,
      header_count, &lc_engine_update_response_map, &response_json, &result,
      error);
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
  response->new_state_etag =
      lc_engine_strdup_local(response_json.new_state_etag);
  if (response_json.new_state_etag != NULL &&
      response->new_state_etag == NULL) {
    rc = LC_ENGINE_ERROR_NO_MEMORY;
  }
  response->bytes = response_json.bytes;
  if (result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_mutate_response_cleanup(response);
    lc_engine_http_result_cleanup(&result);
    return lc_engine_set_protocol_error(error,
                                        "failed to decode mutate response");
  }

  lonejson_cleanup(&lc_engine_update_response_map, &response_json);
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
  lonejson_field body_fields[8];
  lonejson_map body_map;
  lc_engine_buffer fence_value;
  lc_engine_buffer version_value;
  lc_engine_header_pair headers[5];
  lc_engine_http_result result;
  lc_engine_metadata_response_json response_json;
  size_t body_field_count;
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
  header_count = 0U;
  headers[header_count].name = "Content-Type";
  headers[header_count].value = "application/json";
  ++header_count;
  rc = lc_engine_set_fencing_header(request->fencing_token, &fence_value,
                                    &headers[header_count]);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&path);
    return lc_engine_set_client_error(error, rc,
                                      "failed to prepare metadata headers");
  }
  ++header_count;
  if (request->lease_id != NULL && request->lease_id[0] != '\0') {
    headers[header_count].name = "X-Lease-ID";
    headers[header_count].value = request->lease_id;
    ++header_count;
  }
  headers[header_count].name = "X-Txn-ID";
  headers[header_count].value = request->txn_id;
  ++header_count;
  lc_engine_buffer_init(&version_value);
  if (request->has_if_version) {
    if (lc_engine_buffer_append_long_decimal(
            &version_value, request->if_version) != LC_ENGINE_OK) {
      lc_engine_buffer_cleanup(&path);
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

  body_field_count = 0U;
  if (request->namespace_name != NULL &&
      request->namespace_name[0] != '\0') {
    body_fields[body_field_count++] = lc_engine_metadata_body_fields[0];
  }
  body_fields[body_field_count++] = lc_engine_metadata_body_fields[1];
  body_fields[body_field_count++] = lc_engine_metadata_body_fields[2];
  if (request->has_if_version) {
    body_fields[body_field_count++] = lc_engine_metadata_body_fields[5];
  }
  if (request->has_query_hidden) {
    body_fields[body_field_count++] = lc_engine_metadata_body_fields[7];
  }
  lc_engine_lonejson_map_init(&body_map, "lc_engine_metadata_request",
                              body_fields, body_field_count,
                              sizeof(*request));
  memset(&result, 0, sizeof(result));
  rc = lc_engine_http_json_request_stream(
      client, "POST", path.data, &body_map, request, NULL, headers,
      header_count, &lc_engine_metadata_response_map, &response_json, &result,
      error);
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

  response->namespace_name =
      lc_engine_strdup_local(response_json.namespace_name);
  response->key = lc_engine_strdup_local(response_json.key);
  if ((response_json.namespace_name != NULL &&
       response->namespace_name == NULL) ||
      (response_json.key != NULL && response->key == NULL)) {
    rc = LC_ENGINE_ERROR_NO_MEMORY;
  }
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

  lonejson_cleanup(&lc_engine_metadata_response_map, &response_json);
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
  lc_engine_header_pair headers[6];
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
  if (request->lease_id != NULL && request->lease_id[0] != '\0') {
    headers[header_count].name = "X-Lease-ID";
    headers[header_count].value = request->lease_id;
    ++header_count;
  }
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
  rc = lc_engine_http_json_request(client, "POST", path.data, NULL, 0U, headers,
                                   header_count, &lc_engine_remove_response_map,
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
  rc = lc_engine_http_json_request(client, "GET", path.data, NULL, 0U, NULL, 0U,
                                   &lc_engine_describe_response_map,
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

  response->namespace_name =
      lc_engine_strdup_local(response_json.namespace_name);
  response->key = lc_engine_strdup_local(response_json.key);
  response->owner = lc_engine_strdup_local(response_json.owner);
  response->lease_id = lc_engine_strdup_local(response_json.lease_id);
  if ((response_json.namespace_name != NULL &&
       response->namespace_name == NULL) ||
      (response_json.key != NULL && response->key == NULL) ||
      (response_json.owner != NULL && response->owner == NULL) ||
      (response_json.lease_id != NULL && response->lease_id == NULL)) {
    rc = LC_ENGINE_ERROR_NO_MEMORY;
  }
  response->expires_at_unix = response_json.expires_at_unix;
  response->version = response_json.version;
  response->state_etag = lc_engine_strdup_local(response_json.state_etag);
  if (response_json.state_etag != NULL && response->state_etag == NULL) {
    rc = LC_ENGINE_ERROR_NO_MEMORY;
  }
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

  lonejson_cleanup(&lc_engine_describe_response_map, &response_json);
  lc_engine_http_result_cleanup(&result);
  return LC_ENGINE_OK;
}

int lc_engine_client_query(lc_engine_client *client,
                           const lc_engine_query_request *request,
                           lc_engine_query_response *response,
                           lc_engine_error *error) {
  lc_engine_http_result result;
  lc_engine_header_pair headers[1];
  lc_engine_query_response_json parsed;
  lc_engine_query_body_json body_src;
  lonejson_field body_fields[6];
  lonejson_map body_map;
  lc_engine_json_reader_source selector_source;
  lc_engine_json_reader_source fields_source;
  lonejson_error lj_error;
  size_t body_field_count;
  int rc;

  if (client == NULL || request == NULL || response == NULL) {
    return lc_engine_set_client_error(
        error, LC_ENGINE_ERROR_INVALID_ARGUMENT,
        "client, request, and response are required");
  }

  lc_engine_query_response_cleanup(response);
  memset(&body_src, 0, sizeof(body_src));
  body_src.namespace_name =
      (char *)lc_engine_effective_namespace(client, request->namespace_name);
  body_src.limit = request->limit;
  body_src.cursor = (char *)request->cursor;
  body_src.return_mode = (char *)request->return_mode;
  lonejson_json_value_init(&body_src.selector);
  lonejson_json_value_init(&body_src.fields);
  lonejson_error_init(&lj_error);
  selector_source.cursor = (const unsigned char *)"";
  selector_source.remaining = 0U;
  if (request->selector_json != NULL && request->selector_json[0] != '\0') {
    selector_source.cursor = (const unsigned char *)request->selector_json;
    selector_source.remaining = strlen(request->selector_json);
    rc = lonejson_json_value_set_reader(&body_src.selector,
                                        lc_engine_json_memory_reader,
                                        &selector_source, &lj_error);
    if (rc != LONEJSON_STATUS_OK) {
      rc = lc_engine_lonejson_error_from_status(
          error, rc, &lj_error, "failed to configure query selector");
      return rc;
    }
  }
  fields_source.cursor = (const unsigned char *)"";
  fields_source.remaining = 0U;
  if (request->fields_json != NULL && request->fields_json[0] != '\0') {
    fields_source.cursor = (const unsigned char *)request->fields_json;
    fields_source.remaining = strlen(request->fields_json);
    rc = lonejson_json_value_set_reader(&body_src.fields,
                                        lc_engine_json_memory_reader,
                                        &fields_source, &lj_error);
    if (rc != LONEJSON_STATUS_OK) {
      lonejson_json_value_cleanup(&body_src.selector);
      rc = lc_engine_lonejson_error_from_status(
          error, rc, &lj_error, "failed to configure query fields");
      return rc;
    }
  }
  body_field_count = 0U;
  if (body_src.namespace_name != NULL && body_src.namespace_name[0] != '\0') {
    body_fields[body_field_count++] = lc_engine_query_body_fields[0];
  }
  body_fields[body_field_count++] = lc_engine_query_body_fields[1];
  if (request->limit > 0L) {
    body_fields[body_field_count++] = lc_engine_query_body_fields[2];
  }
  if (body_src.cursor != NULL && body_src.cursor[0] != '\0') {
    body_fields[body_field_count++] = lc_engine_query_body_fields[3];
  }
  if (request->fields_json != NULL && request->fields_json[0] != '\0') {
    body_fields[body_field_count++] = lc_engine_query_body_fields[4];
  }
  if (body_src.return_mode != NULL && body_src.return_mode[0] != '\0') {
    body_fields[body_field_count++] = lc_engine_query_body_fields[5];
  }
  lc_engine_lonejson_map_init(&body_map, "lc_engine_query_body_json",
                              body_fields, body_field_count,
                              sizeof(body_src));

  headers[0].name = "Content-Type";
  headers[0].value = "application/json";
  memset(&result, 0, sizeof(result));
  memset(&parsed, 0, sizeof(parsed));
  rc = lc_engine_http_json_request_stream(
      client, "POST", "/v1/query", &body_map, &body_src, NULL, headers, 1U,
      &lc_engine_query_response_map, &parsed, &result, error);
  lonejson_json_value_cleanup(&body_src.selector);
  lonejson_json_value_cleanup(&body_src.fields);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status != 200L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lonejson_cleanup(&lc_engine_query_response_map, &parsed);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }
  response->cursor = lc_engine_strdup_local(parsed.cursor);
  if (parsed.cursor != NULL && response->cursor == NULL) {
    rc = LC_ENGINE_ERROR_NO_MEMORY;
  }
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

int lc_engine_dequeue_internal(lc_engine_client *client,
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

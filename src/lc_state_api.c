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
static const char *lc_engine_find_bytes(const char *haystack,
                                        size_t haystack_length,
                                        const char *needle,
                                        size_t needle_length);
static int
lc_engine_parse_dequeue_response(const lc_engine_http_result *result,
                                 lc_engine_dequeue_response *response,
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

static const char *lc_engine_find_bytes(const char *haystack,
                                        size_t haystack_length,
                                        const char *needle,
                                        size_t needle_length) {
  size_t index;

  if (needle_length == 0U || haystack_length < needle_length) {
    return NULL;
  }
  for (index = 0U; index + needle_length <= haystack_length; ++index) {
    if (memcmp(haystack + index, needle, needle_length) == 0) {
      return haystack + index;
    }
  }
  return NULL;
}

static int
lc_engine_parse_dequeue_response(const lc_engine_http_result *result,
                                 lc_engine_dequeue_response *response,
                                 lc_engine_error *error) {
  const char *boundary_key;
  const char *boundary_value;
  const char *boundary_end;
  char *boundary;
  lc_engine_buffer marker;
  lc_engine_buffer next_marker;
  const char *body;
  size_t body_length;
  const char *cursor;
  const char *header_end;
  const char *meta_start;
  const char *meta_end;
  const char *second_boundary;
  const char *payload_header_end;
  const char *payload_start;
  const char *payload_end;
  char *meta_json;
  long value;
  int rc;

  if (result->content_type == NULL) {
    return lc_engine_set_protocol_error(
        error, "dequeue response missing Content-Type");
  }
  boundary_key = strstr(result->content_type, "boundary=");
  if (boundary_key == NULL) {
    return lc_engine_set_protocol_error(
        error, "dequeue response missing multipart boundary");
  }
  boundary_value = boundary_key + strlen("boundary=");
  if (*boundary_value == '"') {
    boundary_end = strchr(boundary_value + 1, '"');
    if (boundary_end == NULL) {
      return lc_engine_set_protocol_error(error, "invalid multipart boundary");
    }
    boundary = lc_engine_strdup_range(boundary_value + 1, boundary_end);
  } else {
    boundary_end = boundary_value;
    while (*boundary_end != '\0' && *boundary_end != ';' &&
           *boundary_end != ' ' && *boundary_end != '\t') {
      ++boundary_end;
    }
    boundary = lc_engine_strdup_range(boundary_value, boundary_end);
  }
  if (boundary == NULL) {
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to copy multipart boundary");
  }

  lc_engine_buffer_init(&marker);
  lc_engine_buffer_init(&next_marker);
  lc_engine_buffer_append_cstr(&marker, "--");
  lc_engine_buffer_append_cstr(&marker, boundary);
  lc_engine_buffer_append_cstr(&next_marker, "\r\n--");
  lc_engine_buffer_append_cstr(&next_marker, boundary);
  free(boundary);

  body = result->body.data;
  body_length = result->body.length;
  cursor = lc_engine_find_bytes(body, body_length, marker.data, marker.length);
  if (cursor == NULL) {
    lc_engine_buffer_cleanup(&marker);
    lc_engine_buffer_cleanup(&next_marker);
    return lc_engine_set_protocol_error(
        error, "multipart body missing first boundary");
  }
  cursor += marker.length;
  if (cursor + 2 <= body + body_length && cursor[0] == '\r' &&
      cursor[1] == '\n') {
    cursor += 2;
  }
  header_end = lc_engine_find_bytes(
      cursor, body_length - (size_t)(cursor - body), "\r\n\r\n", 4U);
  if (header_end == NULL) {
    lc_engine_buffer_cleanup(&marker);
    lc_engine_buffer_cleanup(&next_marker);
    return lc_engine_set_protocol_error(
        error, "multipart meta part missing header terminator");
  }
  meta_start = header_end + 4;
  meta_end = lc_engine_find_bytes(meta_start,
                                  body_length - (size_t)(meta_start - body),
                                  next_marker.data, next_marker.length);
  if (meta_end == NULL) {
    lc_engine_buffer_cleanup(&marker);
    lc_engine_buffer_cleanup(&next_marker);
    return lc_engine_set_protocol_error(
        error, "multipart meta part missing next boundary");
  }

  meta_json = lc_engine_strdup_range(meta_start, meta_end);
  if (meta_json == NULL) {
    lc_engine_buffer_cleanup(&marker);
    lc_engine_buffer_cleanup(&next_marker);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to copy dequeue metadata");
  }
  while (strlen(meta_json) > 0U &&
         (meta_json[strlen(meta_json) - 1U] == '\r' ||
          meta_json[strlen(meta_json) - 1U] == '\n')) {
    meta_json[strlen(meta_json) - 1U] = '\0';
  }

  rc = lc_engine_json_get_string(meta_json, "namespace",
                                 &response->namespace_name);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_get_string(meta_json, "queue", &response->queue);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_get_string(meta_json, "message_id",
                                   &response->message_id);
  if (rc == LC_ENGINE_OK) {
    value = 0L;
    rc = lc_engine_json_get_long(meta_json, "attempts", &value);
    response->attempts = (int)value;
  }
  if (rc == LC_ENGINE_OK) {
    value = 0L;
    rc = lc_engine_json_get_long(meta_json, "max_attempts", &value);
    response->max_attempts = (int)value;
  }
  if (rc == LC_ENGINE_OK) {
    value = 0L;
    rc = lc_engine_json_get_long(meta_json, "failure_attempts", &value);
    response->failure_attempts = (int)value;
  }
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_get_long(meta_json, "not_visible_until_unix",
                                 &response->not_visible_until_unix);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_get_long(meta_json, "visibility_timeout_seconds",
                                 &response->visibility_timeout_seconds);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_get_string(meta_json, "payload_content_type",
                                   &response->payload_content_type);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_get_string(meta_json, "correlation_id",
                                   &response->correlation_id);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_get_string(meta_json, "lease_id", &response->lease_id);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_get_long(meta_json, "lease_expires_at_unix",
                                 &response->lease_expires_at_unix);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_get_long(meta_json, "fencing_token",
                                 &response->fencing_token);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_get_string(meta_json, "txn_id", &response->txn_id);
  if (rc == LC_ENGINE_OK)
    rc =
        lc_engine_json_get_string(meta_json, "meta_etag", &response->meta_etag);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_get_string(meta_json, "state_etag",
                                   &response->state_etag);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_get_string(meta_json, "state_lease_id",
                                   &response->state_lease_id);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_get_long(meta_json, "state_lease_expires_at_unix",
                                 &response->state_lease_expires_at_unix);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_get_long(meta_json, "state_fencing_token",
                                 &response->state_fencing_token);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_get_string(meta_json, "state_txn_id",
                                   &response->state_txn_id);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_get_string(meta_json, "next_cursor",
                                   &response->next_cursor);
  free(meta_json);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&marker);
    lc_engine_buffer_cleanup(&next_marker);
    return lc_engine_set_protocol_error(error,
                                        "failed to decode dequeue metadata");
  }

  second_boundary = meta_end + 2;
  second_boundary += marker.length;
  if (second_boundary + 2 <= body + body_length && second_boundary[0] == '-' &&
      second_boundary[1] == '-') {
    lc_engine_buffer_cleanup(&marker);
    lc_engine_buffer_cleanup(&next_marker);
    return LC_ENGINE_OK;
  }
  if (second_boundary + 2 > body + body_length || second_boundary[0] != '\r' ||
      second_boundary[1] != '\n') {
    lc_engine_buffer_cleanup(&marker);
    lc_engine_buffer_cleanup(&next_marker);
    return lc_engine_set_protocol_error(error,
                                        "multipart payload boundary malformed");
  }
  second_boundary += 2;
  payload_header_end = lc_engine_find_bytes(
      second_boundary, body_length - (size_t)(second_boundary - body),
      "\r\n\r\n", 4U);
  if (payload_header_end == NULL) {
    lc_engine_buffer_cleanup(&marker);
    lc_engine_buffer_cleanup(&next_marker);
    return lc_engine_set_protocol_error(
        error, "multipart payload part missing header terminator");
  }
  payload_start = payload_header_end + 4;
  payload_end = lc_engine_find_bytes(
      payload_start, body_length - (size_t)(payload_start - body),
      next_marker.data, next_marker.length);
  if (payload_end == NULL) {
    lc_engine_buffer_cleanup(&marker);
    lc_engine_buffer_cleanup(&next_marker);
    return lc_engine_set_protocol_error(
        error, "multipart payload part missing closing boundary");
  }
  rc = lc_engine_copy_bytes(&response->payload, &response->payload_length,
                            payload_start,
                            (size_t)(payload_end - payload_start));
  lc_engine_buffer_cleanup(&marker);
  lc_engine_buffer_cleanup(&next_marker);
  if (rc != LC_ENGINE_OK) {
    return lc_engine_set_client_error(error, rc,
                                      "failed to copy dequeue payload");
  }
  if (response->payload_length >= 2U &&
      response->payload[response->payload_length - 2U] == '\r' &&
      response->payload[response->payload_length - 1U] == '\n') {
    response->payload_length -= 2U;
  }
  return LC_ENGINE_OK;
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
  lc_engine_header_pair headers[2];
  lc_engine_buffer fence_value;
  size_t header_count;
  lc_engine_http_result result;
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

  header_count = 0U;
  lc_engine_buffer_init(&fence_value);
  if (!request->public_read) {
    headers[header_count].name = "X-Lease-ID";
    headers[header_count].value = request->lease_id;
    ++header_count;
    if (request->fencing_token > 0L) {
      rc = lc_engine_set_fencing_header(request->fencing_token, &fence_value,
                                        &headers[header_count]);
      if (rc != LC_ENGINE_OK) {
        lc_engine_buffer_cleanup(&fence_value);
        lc_engine_buffer_cleanup(&path);
        return rc;
      }
      ++header_count;
    }
  }

  memset(&result, 0, sizeof(result));
  rc = lc_engine_http_request(client, "GET", path.data, NULL, 0U, headers,
                              header_count, &result, error);
  lc_engine_buffer_cleanup(&fence_value);
  lc_engine_buffer_cleanup(&path);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status != 200L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }

  rc = lc_engine_copy_bytes(&response->body, &response->body_length,
                            result.body.data, result.body.length);
  if (rc == LC_ENGINE_OK) {
    response->content_type = lc_engine_strdup_local(
        result.content_type != NULL ? result.content_type
                                    : "application/octet-stream");
    if (response->content_type == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc == LC_ENGINE_OK && result.etag != NULL) {
    response->etag = lc_engine_strdup_local(result.etag);
    if (response->etag == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc == LC_ENGINE_OK) {
    response->version = result.key_version;
    response->fencing_token = result.fencing_token;
  }
  if (rc == LC_ENGINE_OK && result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_get_response_cleanup(response);
    lc_engine_http_result_cleanup(&result);
    return lc_engine_set_client_error(error, rc, "failed to copy get response");
  }

  lc_engine_http_result_cleanup(&result);
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
  long value;
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
  rc = lc_engine_http_request(client, "POST", "/v1/query", body.data,
                              body.length, headers, 1U, &result, error);
  lc_engine_buffer_cleanup(&body);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status != 200L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }

  response->raw_json = lc_engine_strdup_range(
      result.body.data, result.body.data + result.body.length);
  if (response->raw_json == NULL) {
    lc_engine_http_result_cleanup(&result);
    return lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to copy query response");
  }
  rc = lc_engine_json_get_string(result.body.data, "cursor", &response->cursor);
  if (rc == LC_ENGINE_OK) {
    value = 0L;
    rc = lc_engine_json_get_long(result.body.data, "index_seq", &value);
    response->index_seq = (unsigned long)value;
  }
  if (rc == LC_ENGINE_OK && result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      rc = LC_ENGINE_ERROR_NO_MEMORY;
    }
  }
  if (rc != LC_ENGINE_OK) {
    lc_engine_query_response_cleanup(response);
    lc_engine_http_result_cleanup(&result);
    return lc_engine_set_protocol_error(error,
                                        "failed to decode query response");
  }

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
  lc_engine_buffer body;
  lc_engine_http_result result;
  lc_engine_header_pair headers[2];
  int first_field;
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
  lc_engine_buffer_init(&body);
  rc = lc_engine_json_begin_object(&body);
  first_field = 1;
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_add_string_field(
        &body, &first_field, "namespace",
        lc_engine_effective_namespace(client, request->namespace_name));
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_add_string_field(&body, &first_field, "queue",
                                         request->queue);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_add_string_field(&body, &first_field, "owner",
                                         request->owner);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_add_string_field(&body, &first_field, "txn_id",
                                         request->txn_id);
  if (rc == LC_ENGINE_OK && request->visibility_timeout_seconds > 0L)
    rc = lc_engine_json_add_long_field(&body, &first_field,
                                       "visibility_timeout_seconds",
                                       request->visibility_timeout_seconds);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_add_long_field(&body, &first_field, "wait_seconds",
                                       request->wait_seconds);
  if (rc == LC_ENGINE_OK && request->page_size > 0)
    rc = lc_engine_json_add_long_field(&body, &first_field, "page_size",
                                       (long)request->page_size);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_add_string_field(&body, &first_field, "start_after",
                                         request->start_after);
  if (rc == LC_ENGINE_OK)
    rc = lc_engine_json_end_object(&body);
  if (rc != LC_ENGINE_OK) {
    lc_engine_buffer_cleanup(&body);
    return lc_engine_set_client_error(error, rc,
                                      "failed to build dequeue request");
  }

  headers[0].name = "Content-Type";
  headers[0].value = "application/json";
  headers[1].name = "Accept";
  headers[1].value = "multipart/related";

  memset(&result, 0, sizeof(result));
  rc = lc_engine_http_request(
      client, "POST",
      with_state ? "/v1/queue/dequeueWithState" : "/v1/queue/dequeue",
      body.data, body.length, headers, 2U, &result, error);
  lc_engine_buffer_cleanup(&body);
  if (rc != LC_ENGINE_OK) {
    return rc;
  }
  if (result.http_status != 200L) {
    rc = lc_engine_set_server_error_from_result(error, &result);
    lc_engine_http_result_cleanup(&result);
    return rc;
  }

  rc = lc_engine_parse_dequeue_response(&result, response, error);
  if (rc == LC_ENGINE_OK && response->correlation_id == NULL &&
      result.correlation_id != NULL) {
    response->correlation_id = lc_engine_strdup_local(result.correlation_id);
    if (response->correlation_id == NULL) {
      rc = lc_engine_set_client_error(error, LC_ENGINE_ERROR_NO_MEMORY,
                                      "failed to copy dequeue correlation id");
    }
  }
  lc_engine_http_result_cleanup(&result);
  return rc;
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
